package com.totango.realtime

import com.totango.model.Service
import com.totango.model.ServiceOp
import com.totango.model.ServiceRecord
import com.totango.model.ServiceType
import com.totango.realtime.model.SdrEvent
import com.totango.realtime.serialization.json.JsonSerdes
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Duration


//https://github.com/confluentinc/demo-scene/blob/master/streams-movie-demo/streams-kotlin/src/main/kotlin/io/confluent/developer/dsl/KafkaStreamsDslDemo.kt

fun StreamsBuilder.buildWordCount(inputTopic: String, storeName: String): KStream<String, Long> {
    val options: Consumed<Void?, String> = Consumed.with(Serdes.Void(), Serdes.String())
    val word = { _: Void?, word: String -> word }
    return stream(inputTopic, options)
        .flatMapValues { line -> line.lowercase().split("\\W+".toRegex()).toList() }
        .groupBy(word, Grouped.with(Serdes.String(), Serdes.String()))
        .count(Materialized.`as`(storeName))
        .toStream()
}

fun StreamsBuilder.buildActivitiesStreamFromJson(): KStream<String, SdrEvent> {
    val sdrConsumeOptions: Consumed<Void?, SdrEvent> = Consumed.with(Serdes.Void(), JsonSerdes.create<SdrEvent>())
        .withTimestampExtractor(SdrTimestampExtractor())
    return stream("sdr_in", sdrConsumeOptions)
        .filter(::isValidSdr)
        .selectKey(::account)
}

fun isValidSdr(@Suppress("UNUSED_PARAMETER") key: Void?, sdr: SdrEvent) =
    sdr.user != null && sdr.activity != null && sdr.accountId != null && sdr.serviceId != null && sdr.moduleId != null


fun StreamsBuilder.buildActivityStreamFromAvro(serviceRecordSerde: Serde<ServiceRecord>): KStream<String, ServiceRecord> {
    val sdrConsumeOptions: Consumed<Void?, ServiceRecord> = Consumed.with(Serdes.Void(), serviceRecordSerde)
        .withTimestampExtractor(ServiceRecordTimestampExtractor())
    return stream("sdr_in", sdrConsumeOptions)
        .filter(::isValidService)
        .selectKey(::account)
}

fun isValidService(@Suppress("UNUSED_PARAMETER") key: Void?, sdr: ServiceRecord) =
    sdr.user != null && sdr.activity != null && sdr.accountId != null && sdr.serviceId != null && sdr.module != null

fun StreamsBuilder.buildUserActivitySessionsFromAvro(serviceRecordSerde: Serde<ServiceRecord>): KTable<Windowed<String>, Long> {
    return buildActivityStreamFromAvro(serviceRecordSerde)
        .groupBy(::userActivity, Grouped.with(Serdes.String(), serviceRecordSerde))
        .windowedBy(SessionWindows.with(Duration.ofMinutes(15)))
        .count(
            Materialized.`as`("sessions")
        )
}

fun userActivity(@Suppress("UNUSED_PARAMETER") key: String, sdr: ServiceRecord) =
    "${sdr.serviceId}:${sdr.accountId}:${sdr.activity}:${sdr.user}"


fun StreamsBuilder.buildUserActivitySessionsFromJson(): KTable<Windowed<String>, Long> {
    return buildActivitiesStreamFromJson()
        .groupBy(::userActivity, Grouped.with(Serdes.String(), JsonSerdes.create<SdrEvent>()))
        .windowedBy(SessionWindows.with(Duration.ofMinutes(15)))
        .count(
            Materialized.`as`("sessions")
        )
}

fun accountActivity(@Suppress("UNUSED_PARAMETER") key: String, sdr: SdrEvent) =
    "${sdr.serviceId}:${sdr.accountId}:${sdr.activity}"

fun StreamsBuilder.buildActivityByAccountAggregation(): KTable<String, Long> {
    // compute activity-by-account-table a KTable<String, Long>
    return buildActivitiesStreamFromJson().groupBy(
        ::accountActivity,
        Grouped.with(Serdes.String(), JsonSerdes.create<SdrEvent>())
    ).count(
        Materialized.`as`<String, Long, KeyValueStore<Bytes, ByteArray>>("activity-per-account")
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Long())
            .withRetention(Duration.ofDays(2))
    )
}

fun userActivity(@Suppress("UNUSED_PARAMETER") key: String, sdr: SdrEvent) =
    "${sdr.serviceId}:${sdr.accountId}:${sdr.activity}:${sdr.user}"


fun account(@Suppress("UNUSED_PARAMETER") key: Void?, sdr: ServiceRecord): String = sdr.serviceId

fun account(@Suppress("UNUSED_PARAMETER") key: Void?, sdr: SdrEvent): String =
    sdr.serviceId ?: throw IllegalArgumentException("serviceId is missing")

fun StreamsBuilder.buildServiceOpsProcessing(
    schemaRegistryUrl: String,
    servicesTopic: String,
    opsTopics: String
): StreamsBuilder {

    val opsSerde = specificAvroSerde<ServiceOp>(schemaRegistryUrl)
    val servicesTable =
        table(servicesTopic, Consumed.with(Serdes.String(), specificAvroSerde<Service>(schemaRegistryUrl)))
    val opsStream = stream(opsTopics, Consumed.with(Serdes.String(), opsSerde))

    // join ops with service to get the service type for each service op, this will help us to decide how to dispatch it.
    val joinParams = Joined.with(
        Serdes.String(),
        specificAvroSerde<ServiceOp>(schemaRegistryUrl),
        specificAvroSerde<Service>(schemaRegistryUrl)
    )
    val valueJoiner: ValueJoiner<ServiceOp, Service, ServiceOp> = ValueJoiner { op: ServiceOp, service: Service ->
        op.serviceType = service.serviceType
        op
    }
    val opsWithServiceTypeStream = opsStream.leftJoin(servicesTable, valueJoiner, joinParams)
        .peek{k, v -> println("processing key $k, value $v")}

    // branch ops according to service type

    fun isOfType(type: ServiceType) : (key: String, service:ServiceOp) -> Boolean = { _, op ->
        op.serviceType == type
    }
    val branches = opsWithServiceTypeStream.branch(
        isOfType(ServiceType.FREE),
        isOfType(ServiceType.PAYING),
        isOfType(ServiceType.EXCLUSIVE),
    )

    branches[0].to("self-services-ops")
    branches[1].to("paying-services-ops")

    // branch the exclusive each one to its own topic
    branches[2].to(
        { serviceId, _, _ -> "exclusive-service-$serviceId" },
        Produced.with(Serdes.String(), opsSerde))

    return this
}

fun <T : SpecificRecord> specificAvroSerde(schemaRegistryUrl: String): SpecificAvroSerde<T> {
    val avroSerde = SpecificAvroSerde<T>()
    avroSerde.configure(mapOf(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryUrl), false)
    return avroSerde
}



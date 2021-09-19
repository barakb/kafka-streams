package com.totango.realtime

import com.totango.realtime.model.SdrEvent
import com.totango.realtime.serialization.json.JsonSerdes
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import java.time.Duration

fun StreamsBuilder.buildWordCount(inputTopic: String, storeName: String): KStream<String, Long> {
    val options: Consumed<Void?, String> = Consumed.with(Serdes.Void(), Serdes.String())
    val word = { _: Void? , word: String -> word }
    return stream(inputTopic, options)
        .flatMapValues{ line -> line.lowercase().split("\\W+".toRegex()).toList() }
        .groupBy(word, Grouped.with(Serdes.String(), Serdes.String()))
        .count(Materialized.`as`(storeName))
        .toStream()
}

fun StreamsBuilder.buildActivitiesStream(): KStream<String, SdrEvent> {
    val sdrConsumeOptions: Consumed<Void?, SdrEvent> = Consumed.with(Serdes.Void(), JsonSerdes.create<SdrEvent>())
        .withTimestampExtractor(SdrTimestampExtractor())
    return stream("sdr_in", sdrConsumeOptions)
        .filter(::isValidSdr)
        .selectKey(::account)
}
fun isValidSdr(@Suppress("UNUSED_PARAMETER") key: Void?, sdr: SdrEvent) =
    sdr.user != null && sdr.activity != null && sdr.accountId != null && sdr.serviceId != null && sdr.moduleId != null


fun StreamsBuilder.buildUserActivitySessions(): KTable<Windowed<String>, Long> {
    return buildActivitiesStream()
        .groupBy(::userActivity, Grouped.with(Serdes.String(), JsonSerdes.create<SdrEvent>()))
        .windowedBy(SessionWindows.with(Duration.ofMinutes(15)))
        .count(
            Materialized.`as`("sessions"))
}

fun StreamsBuilder.buildActivityByAccountAggregation(): KTable<String, Long> {
    // compute activity-by-account-table a KTable<String, Long>
    return buildActivitiesStream().groupBy(
        ::accountActivity,
        Grouped.with(Serdes.String(), JsonSerdes.create<SdrEvent>())
    ).count(
        Materialized.`as`<String, Long, KeyValueStore<Bytes, ByteArray>>("activity-per-account")
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Long())
            .withRetention(Duration.ofDays(2))
    )
}
fun accountActivity(@Suppress("UNUSED_PARAMETER") key: String, sdr: SdrEvent) =
    "${sdr.serviceId}:${sdr.accountId}:${sdr.activity}"



fun account(@Suppress("UNUSED_PARAMETER") key: Void?, sdr: SdrEvent) = sdr.serviceId
fun userActivity(@Suppress("UNUSED_PARAMETER") key: String, sdr: SdrEvent) =
    "${sdr.serviceId}:${sdr.accountId}:${sdr.activity}:${sdr.user}"




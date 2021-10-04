package com.totango.realtime

import com.totango.model.Service
import com.totango.model.ServiceOp
import com.totango.model.ServiceRecord
import com.totango.model.ServiceType
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.time.Instant
import java.util.*


@SpringBootTest
class ServiceOpsProcessingTests {

    private lateinit var testDriver: TopologyTestDriver
    private lateinit var servicesTopic: TestInputTopic<String, Service>
    private lateinit var opsTopic: TestInputTopic<String, ServiceOp>
    private lateinit var selfServiceTopic: TestOutputTopic<String, ServiceOp>
    private lateinit var payingServiceTopic: TestOutputTopic<String, ServiceOp>
    private lateinit var exclusiveServiceTopic: TestOutputTopic<String, ServiceOp>

    private val schemaRegistryURL = "mock://${UUID.randomUUID()}"

    @BeforeEach
    fun setup() {
        val serviceRecordSerde: Serde<ServiceRecord> = SpecificAvroSerde()
        val config: Map<String, String> = mapOf(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to schemaRegistryURL
        )
        serviceRecordSerde.configure(config, false)

        val topology = StreamsBuilder()
            .buildServiceOpsProcessing(schemaRegistryURL, "services", "ops")
            .build()

        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "test"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:1234"
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = SpecificAvroSerde::class.java
        props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = schemaRegistryURL
        testDriver = TopologyTestDriver(topology, props)


        servicesTopic = testDriver.createInputTopic(
            "services", Serdes.String().serializer(), specificAvroSerde<Service>(schemaRegistryURL).serializer()
        )
        opsTopic = testDriver.createInputTopic(
            "ops", Serdes.String().serializer(), specificAvroSerde<ServiceOp>(schemaRegistryURL).serializer()
        )

        selfServiceTopic = testDriver.createOutputTopic(
            "self-services-ops",
            Serdes.String().deserializer(),
            specificAvroSerde<ServiceOp>(schemaRegistryURL).deserializer()
        )
        payingServiceTopic = testDriver.createOutputTopic(
            "paying-services-ops",
            Serdes.String().deserializer(),
            specificAvroSerde<ServiceOp>(schemaRegistryURL).deserializer()
        )
        exclusiveServiceTopic = testDriver.createOutputTopic(
            "exclusive-service-1",
            Serdes.String().deserializer(),
            specificAvroSerde<ServiceOp>(schemaRegistryURL).deserializer()
        )

    }

    @AfterEach
    fun teardown() {
        testDriver.close()
        MockSchemaRegistry.dropScope(schemaRegistryURL.substring("mock://".length))
    }

    @Test
    fun testProcessOps() {
        servicesTopic.pipeInput(
            "1",
            Service(
                Instant.parse("2015-06-02T21:34:33.616Z").toEpochMilli(),
                ServiceType.EXCLUSIVE, "1", "a very exclusive service"
            )
        )
        servicesTopic.pipeInput(
            "2",
            Service(
                Instant.parse("2015-06-02T22:34:33.616Z").toEpochMilli(),
                ServiceType.PAYING, "2", "a regular paying service"
            )
        )
        servicesTopic.pipeInput(
            "3",
            Service(
                Instant.parse("2015-06-02T23:34:33.616Z").toEpochMilli(),
                ServiceType.FREE, "3", "free as a bird"
            )
        )

        val selfServiceOp = ServiceOp(Instant.now().toEpochMilli(), null, "update", "3")
        opsTopic.pipeInput(selfServiceOp.serviceId, selfServiceOp)
        val payingServiceOp = ServiceOp(Instant.now().toEpochMilli(),null, "update", "2")
        opsTopic.pipeInput(payingServiceOp.serviceId, payingServiceOp)
        val exclusiveServiceOp = ServiceOp(Instant.now().toEpochMilli(), null, "update", "1")
        opsTopic.pipeInput(exclusiveServiceOp.serviceId, exclusiveServiceOp)

        var records = selfServiceTopic.readRecordsToList()
        assertThat(records.size).isEqualTo(1)
        assertThat(records[0].value.serviceId).isEqualTo(selfServiceOp.serviceId)

        records = payingServiceTopic.readRecordsToList()
        assertThat(records.size).isEqualTo(1)
        assertThat(records[0].value.serviceId).isEqualTo(payingServiceOp.serviceId)

        records = exclusiveServiceTopic.readRecordsToList()
        assertThat(records.size).isEqualTo(1)
        assertThat(records[0].value.serviceId).isEqualTo(exclusiveServiceOp.serviceId)
    }
}

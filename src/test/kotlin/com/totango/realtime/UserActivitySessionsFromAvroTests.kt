package com.totango.realtime

import com.totango.model.ServiceRecord
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.state.ReadOnlySessionStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.time.Instant
import java.util.*

// https://blog.jdriven.com/2019/12/kafka-streams-topologytestdriver-with-avro/
// https://github.com/avro-kotlin/avro4k
// https://github.com/thake/avro4k-kafka-serializer

@SpringBootTest
class UserActivitySessionsFromAvroTests {

    private lateinit var testDriver: TopologyTestDriver
    private lateinit var inputTopic: TestInputTopic<Void, ServiceRecord>
    private lateinit var store: ReadOnlySessionStore<String, Long>
    private val SCHEMA_REGISTRY_SCOPE: String = UserActivitySessionsFromAvroTests::class.java.name
    private val MOCK_SCHEMA_REGISTRY_URL = "mock://$SCHEMA_REGISTRY_SCOPE"

    @BeforeEach
    fun setup() {
        val serviceRecordSerde: Serde<ServiceRecord> = SpecificAvroSerde()
        val config: Map<String, String> = mapOf(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG to MOCK_SCHEMA_REGISTRY_URL
        )
        serviceRecordSerde.configure(config, false)

        val builder = StreamsBuilder()
        builder.buildUserActivitySessionsFromAvro(serviceRecordSerde)
        val topology: Topology = builder.build()

        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "test"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:1234"
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = SpecificAvroSerde::class.java
        props[AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = MOCK_SCHEMA_REGISTRY_URL
        testDriver = TopologyTestDriver(topology, props)



        inputTopic = testDriver.createInputTopic(
            "sdr_in", Serdes.Void().serializer(),serviceRecordSerde.serializer()
        )

        store = testDriver.getSessionStore("sessions")
    }

    @AfterEach
    fun teardown() {
        testDriver.close()
        MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE)
    }

    @Test
    fun testUserSession() {
        inputTopic.pipeInput(ServiceRecord(Instant.parse("2015-06-02T21:34:33.616Z").toEpochMilli(), "1", "account1", "module1", "activity1", "user1"))
        inputTopic.pipeInput(ServiceRecord(Instant.parse("2015-06-02T21:35:33.616Z").toEpochMilli(), "1", "account1", "module1", "activity1", "user1"))
        inputTopic.pipeInput(ServiceRecord(Instant.parse("2015-06-02T21:36:33.616Z").toEpochMilli(), "1", "account1", "module1", "activity1", "user1"))
        inputTopic.pipeInput(ServiceRecord(Instant.parse("2015-06-02T22:35:33.616Z").toEpochMilli(), "1", "account1", "module1", "activity1", "user1"))

        val key = "1:account1:activity1:user1"
        store.fetch(key).use {
            it.forEach { keyValue ->
                println("key: ${keyValue.key.key()}, ${keyValue.key.window()}, value: ${keyValue.value}")
            }
        }
        store.fetch(key).use { keyValueIterator ->
            assertThat(keyValueIterator).toIterable()
                .anySatisfy { kv -> assertThat(kv.value).isEqualTo(3L) }
                .anySatisfy { kv -> assertThat(kv.value).isEqualTo(1) }
        }
    }
}
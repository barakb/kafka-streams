package com.totango.realtime

import com.totango.realtime.model.SdrEvent
import com.totango.realtime.serialization.json.JsonSerdes
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.state.ReadOnlySessionStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.util.*

// https://kafka.apache.org/documentation/streams/developer-guide/testing.html

@SpringBootTest
class UserActivitySessionsTests {

    private lateinit var testDriver: TopologyTestDriver
    private lateinit var inputTopic: TestInputTopic<Void, SdrEvent>
    private lateinit var store: ReadOnlySessionStore<String, Long>

    @BeforeEach
    fun setup() {

        val builder = StreamsBuilder()
        builder.buildUserActivitySessions()
        val topology: Topology = builder.build()

        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "test"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:1234"
        testDriver = TopologyTestDriver(topology, props)

        inputTopic = testDriver.createInputTopic(
            "sdr_in", Serdes.Void().serializer(), JsonSerdes.create<SdrEvent>().serializer()
        )

        store = testDriver.getSessionStore("sessions")
    }

    @AfterEach
    fun teardown() {
        testDriver.close()
    }

    @Test
    fun testUserSession() {
        inputTopic.pipeInput(SdrEvent("2015-06-02T21:34:33.616Z", "1", "account1", "module1", "activity1", "user1"))
        inputTopic.pipeInput(SdrEvent("2015-06-02T21:35:33.616Z", "1", "account1", "module1", "activity1", "user1"))
        inputTopic.pipeInput(SdrEvent("2015-06-02T21:36:33.616Z", "1", "account1", "module1", "activity1", "user1"))
        inputTopic.pipeInput(SdrEvent("2015-06-02T22:35:33.616Z", "1", "account1", "module1", "activity1", "user1"))

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
package com.totango.realtime

import com.totango.realtime.model.SdrEvent
import com.totango.realtime.serialization.json.JsonSerdes
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.util.*


@SpringBootTest
class ActivityByAccountAggregationTests {
    private lateinit var testDriver: TopologyTestDriver
    private lateinit var inputTopic: TestInputTopic<Void, SdrEvent>
    private lateinit var store: ReadOnlyKeyValueStore<String, Long>

    @BeforeEach
    fun setup() {
        val builder = StreamsBuilder()
        builder.buildActivityByAccountAggregation()
            .toStream().to("activity-by-account-stream", Produced.with(Serdes.String(), Serdes.Long()))
        val topology: Topology = builder.build()

        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "test"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:1234"
        testDriver = TopologyTestDriver(topology, props)

        inputTopic = testDriver.createInputTopic(
            "sdr_in", Serdes.Void().serializer(), JsonSerdes.create<SdrEvent>().serializer()
        )

        store = testDriver.getKeyValueStore("activity-per-account")
    }

    @AfterEach
    fun teardown() {
        testDriver.close()
    }


    @Test
    @DisplayName("events that are not activity events should be filtered")
    fun testFilteringNonActivityEvents() {

        // pipe the test record to our Kafka topic
        inputTopic.pipeInput(SdrEvent("2015-06-02T21:34:33.616Z", "1", "account1"))

        // no user
        inputTopic.pipeInput(
            SdrEvent(
                "2015-06-02T21:34:33.616Z", "1", "account1",
                "activity1"
            )
        )

        // no activity
        inputTopic.pipeInput(
            SdrEvent(
                "2015-06-02T21:34:33.616Z", "1", "account1",
                null, "user1"
            )
        )

        // valid sdr
        val activity = SdrEvent(
            "2015-06-02T21:34:33.616Z", "1", "account1",
            "module1", "activity1", "user1"
        )

        inputTopic.pipeInput(activity)

        assertThat(store.get("${activity.serviceId}:${activity.accountId}:${activity.activity}")).isEqualTo(1L)
        store.all().use { iter ->
            assertThat(iter).toIterable().hasSize(1)
        }

    }
/*
    @ParameterizedTest(name = "greetings topic should contain greeting for {0}? expected = {1}")
    @CsvSource("Izzy, true", "Mitch, true", "Randy, false", "Elyse, true")
    fun testUsersGreeted(value: String, shouldProduceGreeting: Boolean) {
        // pipe the test record to our Kafka topic
        inputTopic.pipeInput(value)

        // save each record that appeared in the output topic to a list
        val outRecords = outputTopic.readRecordsToList()
        if (shouldProduceGreeting) {
            // if we expected a greeting, ensure the output topic only generated one
            // output record for a given input
            assertThat(outRecords).hasSize(1)
            // ensure the generated greeting is the expected value
            val greeting = outRecords[0].value
            assertThat(greeting).isEqualTo("Hello $value")
        } else {
            // if we did not expect a greeting, assert that no records appeared in
            // the output topic
            assertThat(outRecords).isEmpty()
        }
    }
*/

}

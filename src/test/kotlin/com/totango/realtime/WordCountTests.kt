package com.totango.realtime

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import java.util.*


@SpringBootTest
class WordCountTests {
    private lateinit var testDriver: TopologyTestDriver
    private lateinit var inputTopic: TestInputTopic<Void?, String>
    private lateinit var outputTopic: TestOutputTopic<String, Long>

    @BeforeEach
    fun setup() {
        val builder = StreamsBuilder()
        builder.buildWordCount("words", "words-count-store").to("word-count")
        val topology: Topology = builder.build()

        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "test"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:1234"
        testDriver = TopologyTestDriver(topology, props)

        inputTopic = testDriver.createInputTopic(
            "words", Serdes.Void().serializer(), Serdes.String().serializer()
        )
        outputTopic = testDriver.createOutputTopic(
            "word-count", Serdes.String().deserializer(), Serdes.Long().deserializer()
        )
    }

    @AfterEach
    fun teardown() {
        testDriver.close()
    }


    @Test
    @DisplayName("events that are not activity events should be filtered")
    fun testFilteringNonActivityEvents() {

        // pipe the test record to our Kafka topic
        inputTopic.pipeInput("Hi There , how are you ?")
        inputTopic.pipeInput("I am fine , thanks, how are you ?")

        val outRecords = outputTopic.readRecordsToList()
        assertThat(outRecords).anyMatch { it.key == "you" && it.value == 2L }

        val wordsCountStore = testDriver.getKeyValueStore<String, Long>("words-count-store")
        assertThat(wordsCountStore.get("you")).isEqualTo(2L)

    }
}

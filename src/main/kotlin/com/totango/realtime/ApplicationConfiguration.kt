package com.totango.realtime

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*


@ConfigurationProperties
class KafkaProperties {
    var applicationId: String = "kafka-streams-demo-app"
    var bootstrapServers: String = "localhost:29092"
    var numStreamThreads: Int = 1
    var inputTopic: String = "words"
    var outputTopic: String = "words-count"
    var storeName: String = "words-count-store"
}

@Configuration
@EnableConfigurationProperties(KafkaProperties::class)
class ApplicationConfiguration(private val kafkaProperties: KafkaProperties) {
    @Bean
    fun kafkaStreams(): KafkaStreams {
        val props = Properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = kafkaProperties.applicationId
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        props[StreamsConfig.NUM_STREAM_THREADS_CONFIG] = kafkaProperties.numStreamThreads
        props[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest"
//        props[StreamsConfig.APPLICATION_SERVER_CONFIG] = endpoint
        val builder = StreamsBuilder()
        builder.buildWordCount(kafkaProperties.inputTopic, kafkaProperties.storeName)
            .peek{ word, count ->
                println("word: $word, count: $count")
            }
            .to(kafkaProperties.outputTopic)
        val topology: Topology = builder.build()
        print("topology: ${topology.describe()}")
        return KafkaStreams(topology, props)
    }

}
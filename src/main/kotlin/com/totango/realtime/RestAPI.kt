package com.totango.realtime

import com.totango.model.ServiceRecord
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toFlux

@RestController
@RequestMapping("/api")
class RestAPI(val streams: KafkaStreams, val kafkaProperties: KafkaProperties) :
    ApplicationListener<ApplicationReadyEvent> {

    var store: ReadOnlyKeyValueStore<String, Long>? = null

    @GetMapping("/all")
    fun all(): Flux<KeyValue<String, Long>> {
        val iterator = store?.all()
        return iterator?.toFlux()?.doAfterTerminate { iterator.close() } ?: Flux.empty()
    }


    override fun onApplicationEvent(event: ApplicationReadyEvent) {
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
        streams.cleanUp()
        streams.start()
        store =
            streams.store(
                StoreQueryParameters.fromNameAndType(
                    kafkaProperties.storeName,
                    QueryableStoreTypes.keyValueStore()
                )
            )

    }
}
package com.totango.realtime.serialization.json

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes


@Suppress("SpellCheckingInspection")
object JsonSerdes {
    inline fun <reified T> create(): Serde<T> {
        val serializer: JsonSerializer<T> = JsonSerializer()
        val deserializer: JsonDeserializer<T> = JsonDeserializer(T::class.java)
        return Serdes.serdeFrom(serializer, deserializer)
    }
}
package com.totango.realtime.serialization.json

import com.google.gson.FieldNamingPolicy
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Serializer
import java.nio.charset.StandardCharsets


class JsonSerializer<T> : Serializer<T> {
    private val gson = GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create()

    override fun configure(props: Map<String?, *>?, isKey: Boolean) {}

    override fun serialize(topic: String, type: T): ByteArray {
        return gson.toJson(type).toByteArray(StandardCharsets.UTF_8)
    }

    override fun close() {}
}

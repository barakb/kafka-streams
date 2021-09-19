package com.totango.realtime.serialization.json

import com.google.gson.FieldNamingPolicy
import com.google.gson.GsonBuilder
import org.apache.kafka.common.serialization.Deserializer
import java.lang.reflect.Type


class JsonDeserializer<T>(private val cls: Class<T>) : Deserializer<T> {
    private val gson = GsonBuilder()
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
        .create()


    override fun configure(props: Map<String?, *>?, isKey: Boolean) {}
    override fun deserialize(topic: String?, bytes: ByteArray?): T? {
        if (bytes == null) {
            return null
        }
        return gson.fromJson(String(bytes), cls)
    }

    override fun close() {}
}
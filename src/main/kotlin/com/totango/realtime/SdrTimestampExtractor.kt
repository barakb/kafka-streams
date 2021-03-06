package com.totango.realtime

import com.totango.realtime.model.SdrEvent
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import java.time.Instant




class SdrTimestampExtractor : TimestampExtractor {

    override fun extract(record: ConsumerRecord<Any, Any>, partitionTime: Long): Long {
        val sdr: SdrEvent? = record.value() as SdrEvent?
        if (sdr != null) {
            val timestamp: String = sdr.timestamp
            return Instant.parse(timestamp).toEpochMilli()
        }
        return partitionTime

    }
}
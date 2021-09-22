package com.totango.realtime

import com.totango.model.ServiceRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import java.time.Instant




class ServiceRecordTimestampExtractor : TimestampExtractor {

    override fun extract(record: ConsumerRecord<Any, Any>, partitionTime: Long): Long {
        val serviceRecord: ServiceRecord? = record.value() as ServiceRecord?
        if (serviceRecord != null) {
            return serviceRecord.createdAt
        }
        return partitionTime

    }
}
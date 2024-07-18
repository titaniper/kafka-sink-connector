package com.example

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import java.sql.Struct
import java.util.*

class MySinkTask : SinkTask() {

    private lateinit var destTopic: String
    private lateinit var producer: KafkaProducer<String, String>
    private val objectMapper = ObjectMapper()

    override fun start(props: Map<String, String>) {
        // Task 초기화 작업
        destTopic = props["dest.topic"] ?: throw ConfigException("Destination topic must be set")

        val producerProps = Properties()
        producerProps["bootstrap.servers"] = props["bootstrap.servers"]
        producerProps["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        producerProps["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        producer = KafkaProducer(producerProps)
    }

    /**
     * 주기적으로 호출되어 데이터를 처리하고 다른 시스템으로 전송합니다.
     * - sink 는 source 가 자동으로 topic 이라 컨슈머가 필요가 없다.
     */
    override fun put(records: Collection<SinkRecord>) {
        // 데이터를 처리하여 다른 Kafka 토픽으로 전송
        for (record in records) {
            val key = convertToJsonString(record.key())
            val value = convertToJsonString(record.value())
            producer.send(ProducerRecord(destTopic, key, value))
        }
    }

    private fun convertToJsonString(data: Any?): String {
        return when (data) {
            is Struct -> objectMapper.writeValueAsString(data)
            is String -> data
            else -> data?.toString() ?: ""
        }
    }

    override fun stop() {
        // Task 종료 작업
        producer.close()
    }

    override fun version(): String {
        return "1.0"
    }
}

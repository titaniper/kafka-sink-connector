package com.example

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.*

class MySinkTask : SinkTask() {

    private lateinit var destTopic: String
    private lateinit var producer: KafkaProducer<String, String>
    private val objectMapper = ObjectMapper()
    private val logger: Logger = LogManager.getLogger(MySinkTask::class.java)

    override fun start(props: Map<String, String>) {
        // Task 초기화 작업
        destTopic = props["dest.topic"] ?: throw ConfigException("Destination topic must be set")

        val producerProps = Properties()
        producerProps["bootstrap.servers"] = props["bootstrap.servers"]
        producerProps["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        producerProps["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        producerProps[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = "1111"
        producer = KafkaProducer(producerProps)
        producer.initTransactions()
    }

    override fun put(records: Collection<SinkRecord>) {

        producer.beginTransaction()
        try {
            // 데이터를 처리하여 다른 Kafka 토픽으로 전송
            for (record in records) {
                logger.info("Received record:\n" +
                        "  topic: ${record.topic()}\n" +
                        "  partition: ${record.kafkaPartition()}\n" +
                        "  offset: ${record.kafkaOffset()}\n" +
                        "  key: ${record.key()}\n" +
                        "  value: ${convertStructToJson(record.value())}\n" +
                        "  timestamp: ${record.timestamp()}\n"
                )

                val structValue = record.value() as? Struct
                val afterStruct = structValue?.getStruct("after")
                if (afterStruct != null) {
                    val eventType = afterStruct.getString("type")
                    if (eventType == "KillEvent") {
                        throw RuntimeException("Encountered a KillEvent")
                    }
                }

                val key = convertStructToJson(record.key() ?: "")
                val value = convertStructToJson(record.value() ?: "")

                producer.send(ProducerRecord(destTopic, key, value))
                logger.info("Record sent to topic $destTopic: key=$key, value=$value")
            }
            producer.commitTransaction()
        } catch (e: ProducerFencedException) {
            producer.close()
            throw ConnectException(e)
        } catch (e: OutOfOrderSequenceException) {
            producer.close()
            throw ConnectException(e)
        } catch (e: AuthorizationException) {
            producer.close()
            throw ConnectException(e)
        } catch (e: KafkaException) {
            producer.abortTransaction()
        }
    }

    private fun convertStructToJson(data: Any?): String {
        return when (data) {
            is Struct -> objectMapper.writeValueAsString(structToMap(data))
            is String -> data
            else -> data?.toString() ?: ""
        }
    }

    private fun structToMap(struct: Struct): Map<String, Any?> {
        val map = mutableMapOf<String, Any?>()
        val schema = struct.schema()
        for (field in schema.fields()) {
            val fieldValue = struct.get(field)
            map[field.name()] = if (fieldValue is Struct) structToMap(fieldValue) else fieldValue
        }
        return map
    }

    override fun stop() {
        // Task 종료 작업
        producer.close()
    }

    override fun version(): String {
        return "1.0"
    }
}

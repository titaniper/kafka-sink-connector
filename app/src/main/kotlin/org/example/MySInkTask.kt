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


        /**
         * NOTE: Exactly once 를 위한 설정
         * - ENABLE_IDEMPOTENCE_CONFIG 설정은 Kafka 프로듀서가 멱등성을 보장하도록 활성화합니다.
         * - 멱등성은 동일한 메시지를 여러 번 전송하더라도 결과가 한 번 전송한 것과 동일하도록 보장하는 특성입니다.
         * - 이를 통해 중복 메시지 전송을 방지합니다.
         * - 기본적으로 이 설정은 중복 방지 메커니즘을 제공하여 프로듀서가 네트워크 오류나 브로커 장애로 인해 동일한 메시지를 여러 번 전송하더라도 중복 없이 처리되도록 합니다.
         * ENABLE_IDEMPOTENCE_CONFIG를 true로 설정하면 Kafka 프로듀서에서 메시지의 중복 전송과 순서 보장을 위해 특정한 내부 동작이 활성화됩니다. 이 설정은 Kafka 0.11.0 버전부터 도입된 기능으로, 프로듀서가 같은 메시지를 여러 번 보내더라도 중복된 메시지를 소비자가 받지 않도록 보장합니다.
         *
         * 이 설정을 통해 활성화되는 주요 내부 동작은 다음과 같습니다:
         * 프로듀서 ID 할당: 프로듀서는 Kafka 브로커로부터 고유한 프로듀서 ID (PID)를 할당받습니다. 이 PID는 메시지의 중복을 탐지하고 방지하는 데 사용됩니다.
         * 시퀀스 넘버 관리: 각 메시지에는 시퀀스 넘버가 붙습니다. 이 시퀀스 넘버는 프로듀서가 메시지를 보낸 순서를 나타내며, 브로커는 이 정보를 사용하여 중복 메시지를 식별하고 올바른 순서로 메시지를 저장합니다.
         * 일관성 보장: 브로커는 각 파티션별로 프로듀서의 마지막 시퀀스 넘버를 추적합니다. 만약 중복된 시퀀스 넘버의 메시지가 도착하면 브로커는 이를 무시합니다. 이는 메시지의 중복 전송이 발생하더라도 소비자가 중복된 메시지를 받지 않도록 합니다.
         * Ack 설정: acks=all 설정이 자동으로 적용됩니다. 이는 프로듀서가 메시지가 모든 복제본에 안전하게 기록될 때까지 기다리도록 합니다. 이렇게 하면 단일 브로커 장애에도 메시지의 손실 없이 안정적으로 전송할 수 있습니다.
         * Max In-Flight Requests: 동시에 비동기적으로 전송될 수 있는 요청의 수가 제한됩니다. 기본값은 5이지만, enable.idempotence=true로 설정하면 이 값이 자동으로 5 이하로 조정됩니다. 이는 메시지의 순서 보장을 위해 필요합니다.
         * 이 기능들을 통해 Kafka는 메시지의 중복 전송 방지와 순서 보장을 제공하여, 프로듀서가 동일한 메시지를 여러 번 보내는 상황에서도 소비자는 중복되지 않은 메시지를 올바른 순서대로 받을 수 있게 됩니다.
         */
//        producerProps[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = "true" // 멱등성을 활성화하여 중복 메시지 전송을 방지합니다.
//        producerProps[ProducerConfig.ACKS_CONFIG] = "all" // (all): 모든 복제 브로커가 메시지를 확인할 때까지 기다립니다.
//        producerProps[ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION] = "5" //  하나의 연결당 최대 요청 수를 제한합니다.
//        producerProps[ProducerConfig.RETRIES_CONFIG] = "5" // 실패한 요청을 재시도할 횟수를 설정합니다.

        /**
         * TRANSACTIONAL_ID_CONFIG는 Kafka 프로듀서가 트랜잭션을 사용할 수 있도록 합니다.
         * - 트랜잭션은 프로듀서가 여러 메시지를 묶어서 하나의 단위로 보내고,
         * - 이 단위가 모두 성공하거나 모두 실패하도록 보장합니다.
         * - 이를 통해 프로듀서가 정확히 한 번 전송을 보장할 수 있습니다.
         * - 트랜잭션 ID를 설정하여 트랜잭션을 활성화합니다
         */
        producerProps[ProducerConfig.TRANSACTIONAL_ID_CONFIG] = "TRANSACTIONAL_ID_CONFIG"

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
//                if (afterStruct != null) {
//                    val eventType = afterStruct.getString("type")
//                    if (eventType == "KillEvent") {
//                        throw RuntimeException("Encountered a KillEvent")
//                    }
//                }

                val key = convertStructToJson(record.key() ?: "")
                val value = convertStructToJson(record.value() ?: "")

                producer.send(ProducerRecord(destTopic, key, value))
                logger.info("Record sent to topic $destTopic: key=$key, value=$value")
            }
            producer.commitTransaction()
        } catch (e: Exception) {
            producer.abortTransaction()
            producer.close()
            throw e;
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

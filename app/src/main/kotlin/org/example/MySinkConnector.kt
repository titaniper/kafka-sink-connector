package com.example

import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.apache.kafka.common.config.ConfigDef
import java.util.*

class MySinkConnector : SinkConnector() {

    private lateinit var configProps: Map<String, String>

    /**
     * 커넥터 초기화 작업
     */
    override fun start(props: Map<String, String>) {
        configProps = props
    }

    /**
     * 커넥터와 연결된 Task 클래스 타입을 반환합니다.
     */
    override fun taskClass(): Class<out Task> {
        return MySinkTask::class.java
    }

    /**
     * 각 Task에 전달될 설정을 생성합니다.
     */
    override fun taskConfigs(maxTasks: Int): List<Map<String, String>> {
        // 각 Task에 전달할 설정 생성
        return List(maxTasks) { configProps }
    }

    /**
     * 커넥터를 중지합니다.
     */
    override fun stop() {
        // 커넥터 종료 작업
    }

    /**
     * 커넥터 설정을 정의합니다.
     */
    override fun config(): ConfigDef {
        // 커넥터 설정 정의
        return ConfigDef()
                .define("source.topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Source Kafka Topic")
                .define("dest.topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Destination Kafka Topic")
                .define("bootstrap.servers", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka Bootstrap Servers")
    }

    override fun version(): String {
        return "1.0"
    }
}
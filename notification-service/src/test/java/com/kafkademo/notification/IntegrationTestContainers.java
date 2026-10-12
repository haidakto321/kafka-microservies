package com.kafkademo.notification;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** A real Kafka for the integration tests (no database in this service). Same image as docker-compose.yml. */
@TestConfiguration(proxyBeanMethods = false)
public class IntegrationTestContainers {

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        // Same as docker-compose.yml: topics are only created on purpose (NewTopic beans).
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.3.1"))
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }
}

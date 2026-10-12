package com.kafkademo.payment;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Real containers for integration tests. Same images as docker-compose.yml, so nothing new is downloaded. */
@TestConfiguration(proxyBeanMethods = false)
public class IntegrationTestContainers {

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgres() {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:17.4"));
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        // Same as docker-compose.yml: topics are only created on purpose (NewTopic beans). With auto-create on,
        // a missing NewTopic bean - e.g. for a dead-letter topic - would go unnoticed in the tests.
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.3.1"))
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }
}

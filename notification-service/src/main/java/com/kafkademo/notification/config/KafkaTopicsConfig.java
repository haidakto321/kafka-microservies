package com.kafkademo.notification.config;

import com.kafkademo.contracts.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Creates the topics this service reads and writes, if they don't exist yet (broker auto-create is off). */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicsConfig {

    @Bean
    NewTopic orderEventsTopic() {
        return TopicBuilder.name(Topics.ORDER_EVENTS).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic orderEventsDeadLetterTopic() {
        // Same partition count: a failed record goes to the same partition number in the DLT.
        return TopicBuilder.name(Topics.ORDER_EVENTS_DLT).partitions(Topics.PARTITIONS).replicas(1).build();
    }
}

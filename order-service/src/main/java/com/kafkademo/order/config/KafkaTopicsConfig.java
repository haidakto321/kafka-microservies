package com.kafkademo.order.config;

import com.kafkademo.contracts.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Creates the topics this service reads and writes, if they don't exist yet (broker auto-create is off). */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicsConfig {

    @Bean
    NewTopic inventoryCommandsTopic() {
        return TopicBuilder.name(Topics.INVENTORY_COMMANDS).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic sagaRepliesTopic() {
        return TopicBuilder.name(Topics.SAGA_REPLIES).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic sagaRepliesDeadLetterTopic() {
        // Same partition count: a failed record goes to the same partition number in the DLT.
        return TopicBuilder.name(Topics.SAGA_REPLIES_DLT).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    // The outbox also writes to these two. With auto-create off, a missing topic would block every row behind it.
    @Bean
    NewTopic paymentCommandsTopic() {
        return TopicBuilder.name(Topics.PAYMENT_COMMANDS).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic orderEventsTopic() {
        return TopicBuilder.name(Topics.ORDER_EVENTS).partitions(Topics.PARTITIONS).replicas(1).build();
    }
}

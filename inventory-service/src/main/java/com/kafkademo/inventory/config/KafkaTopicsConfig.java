package com.kafkademo.inventory.config;

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
}

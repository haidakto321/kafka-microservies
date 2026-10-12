package com.kafkademo.notification.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.contracts.EnvelopeCodec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class AppConfig {

    @Bean
    EnvelopeCodec envelopeCodec(ObjectMapper objectMapper) {
        return new EnvelopeCodec(objectMapper);
    }
}

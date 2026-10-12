package com.kafkademo.payment.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.contracts.EnvelopeCodec;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class AppConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    EnvelopeCodec envelopeCodec(ObjectMapper objectMapper) {
        return new EnvelopeCodec(objectMapper);
    }
}

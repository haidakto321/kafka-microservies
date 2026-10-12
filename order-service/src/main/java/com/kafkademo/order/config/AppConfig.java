package com.kafkademo.order.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.contracts.EnvelopeCodec;
import java.time.Clock;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.ClientHttpRequestFactorySettings;
import org.springframework.boot.web.client.RestClientCustomizer;
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

    /** Spec 7.2: the price call gives up after 2 s (connect and read). A hanging inventory must not hang orders. */
    @Bean
    RestClientCustomizer restClientTimeouts(@Value("${inventory.timeout:2s}") Duration timeout) {
        ClientHttpRequestFactorySettings settings = ClientHttpRequestFactorySettings.defaults()
                .withConnectTimeout(timeout)
                .withReadTimeout(timeout);
        return builder -> builder.requestFactory(ClientHttpRequestFactoryBuilder.detect().build(settings));
    }
}

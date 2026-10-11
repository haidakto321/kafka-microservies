package com.kafkademo.inventory.config;

import com.kafkademo.contracts.InvalidMessageException;
import com.kafkademo.contracts.Topics;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration(proxyBeanMethods = false)
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    /**
     * What happens when a listener throws (spec 6.4). Spring Boot plugs this bean into every @KafkaListener.
     * - InvalidMessageException (not JSON, missing fields, wrong payload) can never succeed: it goes straight to
     *   "<topic>.DLT", no retries.
     * - Anything else (e.g. the database is down): 3 retries after 1 s, 2 s and 4 s, then "<topic>.DLT".
     * While a record is retried, the records behind it on the same partition wait. That keeps the messages of one
     * order in order ("blocking" retries). @RetryableTopic would not block, but would break that order.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka) {
        // Same partition number in "<topic>.DLT" (spec 4). Set explicitly: Spring Kafka 3.3's default is "<topic>-dlt".
        DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(kafka,
                (record, exception) -> new TopicPartition(record.topic() + Topics.DLT_SUFFIX, record.partition()));
        ConsumerRecordRecoverer logThenDeadLetter = (record, exception) -> {
            log.error("Giving up on {}-{} offset {} (key {}) - sending it to {}.DLT. Cause: {}",
                    record.topic(), record.partition(), record.offset(), record.key(), record.topic(),
                    rootCause(exception));
            deadLetters.accept(record, exception);
        };
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1_000);
        backOff.setMultiplier(2.0);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(logThenDeadLetter, backOff);
        errorHandler.addNotRetryableExceptions(InvalidMessageException.class);
        return errorHandler;
    }

    private static String rootCause(Throwable exception) {
        Throwable cause = exception;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}

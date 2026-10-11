package com.kafkademo.inventory.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.InvalidMessageException;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.inventory.IntegrationTestContainers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** The error handler's rules (spec 6.4), with the handler replaced by a mock that fails on purpose. */
@SpringBootTest
@Import(IntegrationTestContainers.class)
class KafkaErrorHandlingTest {

    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    ConsumerFactory<String, String> consumerFactory;
    @Autowired
    EnvelopeCodec codec;

    @MockitoBean
    InventoryCommandHandler handler;

    @Test
    void aFailingCommandIsRetriedThreeTimesThenSentToTheDeadLetterTopic() throws Exception {
        UUID orderId = UUID.randomUUID();
        doThrow(new IllegalStateException("database is down (test)")).when(handler).handle(argThat(forOrder(orderId)));

        send(orderId);

        // 1 try + 3 retries after 1 s, 2 s and 4 s.
        verify(handler, timeout(20_000).times(4)).handle(argThat(forOrder(orderId)));
        ConsumerRecord<String, String> dead = awaitRecord(Topics.INVENTORY_COMMANDS_DLT, orderId.toString());
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isEqualTo(IllegalStateException.class.getName());
    }

    @Test
    void anInvalidCommandIsNotRetried() throws Exception {
        UUID orderId = UUID.randomUUID();
        doThrow(new InvalidMessageException("bad payload (test)")).when(handler).handle(argThat(forOrder(orderId)));

        send(orderId);

        awaitRecord(Topics.INVENTORY_COMMANDS_DLT, orderId.toString());
        // A retry would come after 1 s. Wait longer than that and check there was only the one try.
        verify(handler, after(2_000).times(1)).handle(argThat(forOrder(orderId)));
    }

    private static ArgumentMatcher<MessageEnvelope> forOrder(UUID orderId) {
        return envelope -> envelope != null && orderId.equals(envelope.orderId());
    }

    private void send(UUID orderId) throws Exception {
        String json = codec.toJson(codec.wrap(MessageTypes.RESERVE_STOCK, orderId, new ReserveStock(1L, 1)));
        kafka.send(Topics.INVENTORY_COMMANDS, orderId.toString(), json).get(10, TimeUnit.SECONDS);
    }

    private ConsumerRecord<String, String> awaitRecord(String topic, String key) {
        try (Consumer<String, String> consumer = consumerFactory.createConsumer("test-" + UUID.randomUUID(), null)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No record on " + topic + " with key " + key);
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}

package com.kafkademo.notification;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.OrderCancelled;
import com.kafkademo.contracts.payload.OrderConfirmed;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest
@Import(IntegrationTestContainers.class)
@ExtendWith(OutputCaptureExtension.class)
class OrderEventListenerTest {

    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    ConsumerFactory<String, String> consumerFactory;
    @Autowired
    EnvelopeCodec codec;

    @Test
    void aConfirmedOrderIsAnnounced(CapturedOutput output) throws Exception {
        UUID orderId = UUID.randomUUID();

        send(orderId, codec.toJson(codec.wrap(MessageTypes.ORDER_CONFIRMED, orderId,
                new OrderConfirmed(1L, 2, new BigDecimal("100.00")))));

        awaitOutput(output, "NOTIFICATION: order " + orderId + " confirmed - 2 x product 1, total 100.00");
    }

    @Test
    void aCancelledOrderIsAnnouncedWithTheReason(CapturedOutput output) throws Exception {
        UUID orderId = UUID.randomUUID();

        send(orderId, codec.toJson(codec.wrap(MessageTypes.ORDER_CANCELLED, orderId, new OrderCancelled("PAYMENT_FAILED"))));

        awaitOutput(output, "NOTIFICATION: order " + orderId + " cancelled (PAYMENT_FAILED)");
    }

    @Test
    void aBrokenEventGoesToTheDeadLetterTopic() throws Exception {
        UUID key = UUID.randomUUID();

        send(key, "this is not json");

        assertThat(awaitRecord(Topics.ORDER_EVENTS_DLT, key.toString()).value()).isEqualTo("this is not json");
    }

    private void send(UUID key, String value) throws Exception {
        kafka.send(Topics.ORDER_EVENTS, key.toString(), value).get(10, TimeUnit.SECONDS);
    }

    private static void awaitOutput(CapturedOutput output, String expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (output.getOut().contains(expected)) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Never logged: " + expected);
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
}

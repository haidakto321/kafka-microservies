package com.kafkademo.payment.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.InvalidMessageException;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ChargePayment;
import com.kafkademo.contracts.payload.PaymentFailed;
import com.kafkademo.contracts.payload.PaymentSucceeded;
import com.kafkademo.payment.IntegrationTestContainers;
import com.kafkademo.payment.payment.PaymentRepository;
import com.kafkademo.payment.payment.PaymentStatus;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class PaymentCommandListenerTest {

    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    ConsumerFactory<String, String> consumerFactory;
    @Autowired
    EnvelopeCodec codec;
    @Autowired
    PaymentRepository payments;
    @Autowired
    JdbcTemplate jdbc;

    @Test
    void aChargeUnderTheLimitRepliesPaymentSucceeded() throws Exception {
        UUID orderId = UUID.randomUUID();

        send(codec.wrap(MessageTypes.CHARGE_PAYMENT, orderId, new ChargePayment(new BigDecimal("50.00"))));

        MessageEnvelope reply = awaitReply(orderId);
        assertThat(reply.type()).isEqualTo(MessageTypes.PAYMENT_SUCCEEDED);
        assertThat(codec.payload(reply, PaymentSucceeded.class).amount()).isEqualByComparingTo("50.00");
        assertThat(payments.findById(orderId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
    }

    @Test
    void aChargeOverTheLimitRepliesPaymentFailed() throws Exception {
        UUID orderId = UUID.randomUUID();

        send(codec.wrap(MessageTypes.CHARGE_PAYMENT, orderId, new ChargePayment(new BigDecimal("1500.00"))));

        MessageEnvelope reply = awaitReply(orderId);
        assertThat(reply.type()).isEqualTo(MessageTypes.PAYMENT_FAILED);
        assertThat(codec.payload(reply, PaymentFailed.class).reason()).isEqualTo("AMOUNT_OVER_LIMIT");
        assertThat(payments.findById(orderId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void aDuplicateChargeIsProcessedOnce() throws Exception {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope charge = codec.wrap(MessageTypes.CHARGE_PAYMENT, orderId, new ChargePayment(new BigDecimal("50.00")));

        send(charge);
        send(charge);
        MessageEnvelope marker = codec.wrap("Marker", orderId, new ChargePayment(BigDecimal.ONE));
        send(marker); // same key = same partition: handled after both copies

        awaitProcessed(marker.messageId());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id = ?", Long.class, orderId))
                .isEqualTo(1);
    }

    @Test
    void aChargeWithoutAnAmountGoesToTheDeadLetterTopic() throws Exception {
        UUID orderId = UUID.randomUUID();

        send(codec.wrap(MessageTypes.CHARGE_PAYMENT, orderId, Map.of())); // payload {}: no amount

        ConsumerRecord<String, String> dead = awaitRecord(Topics.PAYMENT_COMMANDS_DLT, orderId.toString());
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isEqualTo(InvalidMessageException.class.getName());
        assertThat(payments.findById(orderId)).isEmpty();
    }

    private void send(MessageEnvelope envelope) throws Exception {
        kafka.send(Topics.PAYMENT_COMMANDS, envelope.orderId().toString(), codec.toJson(envelope)).get(10, TimeUnit.SECONDS);
    }

    private MessageEnvelope awaitReply(UUID orderId) {
        return codec.fromJson(awaitRecord(Topics.SAGA_REPLIES, orderId.toString()).value());
    }

    private void awaitProcessed(UUID messageId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Long count = jdbc.queryForObject(
                    "SELECT count(*) FROM processed_message WHERE message_id = ?", Long.class, messageId);
            if (count != null && count == 1) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Message " + messageId + " was never processed");
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

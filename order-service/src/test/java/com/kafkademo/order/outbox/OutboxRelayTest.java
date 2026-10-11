package com.kafkademo.order.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.order.OrderService;
import com.kafkademo.order.product.ProductClient;
import com.kafkademo.order.product.ProductInfo;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** The relay with a fake Kafka that is up or down as each test says. */
@SpringBootTest
@Import(IntegrationTestContainers.class)
class OutboxRelayTest {

    @Autowired
    OrderService orderService;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    EnvelopeCodec codec;

    /** Replaces Spring Boot's KafkaTemplate bean, so the relay talks to this fake instead of a broker. */
    @MockitoBean
    KafkaTemplate<String, String> kafka;
    @MockitoBean
    ProductClient productClient;

    private final AtomicBoolean kafkaUp = new AtomicBoolean(true);
    private final Set<String> failingKeys = ConcurrentHashMap.newKeySet();

    @BeforeEach
    void fakes() {
        when(productClient.getProduct(anyLong())).thenReturn(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));
        when(kafka.send(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
            String key = invocation.getArgument(1);
            if (!kafkaUp.get() || failingKeys.contains(key)) {
                return CompletableFuture.failedFuture(new IllegalStateException("Kafka is down (test)"));
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    /** E4 in a test: Kafka is down, the order is still accepted, and its message goes out once Kafka is back. */
    @Test
    void aMessageWaitsWhileKafkaIsDownAndIsSentWhenItIsBack() throws Exception {
        kafkaUp.set(false);
        UUID orderId = orderService.placeOrder(1L, 2).getId(); // no exception: placing an order does not touch Kafka

        Thread.sleep(1_500); // about 3 relay runs, all failing
        assertThat(publishedAt(orderId)).isNull();

        kafkaUp.set(true);
        awaitPublished(orderId);

        ArgumentCaptor<String> sent = ArgumentCaptor.forClass(String.class);
        verify(kafka, atLeast(2)).send(eq(Topics.INVENTORY_COMMANDS), eq(orderId.toString()), sent.capture());
        // Every attempt sent the same message - the same messageId - so a consumer can spot a resend.
        assertThat(sent.getAllValues()).extracting(json -> codec.fromJson(json).messageId())
                .containsOnly(outboxId(orderId));
        assertThat(codec.fromJson(sent.getValue()).type()).isEqualTo(MessageTypes.RESERVE_STOCK);
    }

    /** The relay stops at the first failure, so a newer message never overtakes an older one. */
    @Test
    void aFailingMessageHoldsBackTheMessagesBehindIt() throws Exception {
        kafkaUp.set(false);
        UUID first = orderService.placeOrder(1L, 1).getId();
        UUID second = orderService.placeOrder(1L, 1).getId();
        failingKeys.add(first.toString());
        kafkaUp.set(true);

        Thread.sleep(1_500);
        assertThat(publishedAt(first)).isNull();
        assertThat(publishedAt(second)).isNull();
        verify(kafka, never()).send(anyString(), eq(second.toString()), anyString());

        failingKeys.clear();
        awaitPublished(second);
        assertThat(publishedAt(first)).isNotNull();
        InOrder sendOrder = inOrder(kafka);
        sendOrder.verify(kafka, atLeastOnce()).send(anyString(), eq(first.toString()), anyString());
        sendOrder.verify(kafka).send(anyString(), eq(second.toString()), anyString());
    }

    private Timestamp publishedAt(UUID orderId) {
        return jdbc.queryForObject("SELECT published_at FROM outbox_event WHERE aggregate_id = ?", Timestamp.class, orderId);
    }

    private UUID outboxId(UUID orderId) {
        return jdbc.queryForObject("SELECT id FROM outbox_event WHERE aggregate_id = ?", UUID.class, orderId);
    }

    private void awaitPublished(UUID orderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (publishedAt(orderId) != null) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Outbox row for order " + orderId + " was never sent");
    }
}

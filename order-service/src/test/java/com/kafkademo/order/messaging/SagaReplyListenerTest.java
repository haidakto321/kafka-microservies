package com.kafkademo.order.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.StockReservationFailed;
import com.kafkademo.contracts.payload.StockReserved;
import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.order.CancelReason;
import com.kafkademo.order.order.Order;
import com.kafkademo.order.order.OrderRepository;
import com.kafkademo.order.order.OrderService;
import com.kafkademo.order.order.OrderStatus;
import com.kafkademo.order.product.ProductClient;
import com.kafkademo.order.product.ProductInfo;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class SagaReplyListenerTest {

    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    EnvelopeCodec codec;
    @Autowired
    OrderService orderService;
    @Autowired
    OrderRepository orders;
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @MockitoBean
    ProductClient productClient;

    @BeforeEach
    void stubPrice() {
        when(productClient.getProduct(anyLong())).thenReturn(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));
    }

    @Test
    void aStockReservedReplyOnKafkaUpdatesTheOrder() throws Exception {
        UUID orderId = orderService.placeOrder(1L, 2).getId();

        sendReply(orderId, MessageTypes.STOCK_RESERVED, new StockReserved(1L, 2));

        awaitStatus(orderId, OrderStatus.STOCK_RESERVED);
    }

    @Test
    void aStockReservationFailedReplyOnKafkaCancelsTheOrder() throws Exception {
        UUID orderId = orderService.placeOrder(1L, 2).getId();

        sendReply(orderId, MessageTypes.STOCK_RESERVATION_FAILED, new StockReservationFailed(1L, 2, "OUT_OF_STOCK"));

        awaitStatus(orderId, OrderStatus.CANCELLED);
        assertThat(orders.findById(orderId).orElseThrow().getCancelReason()).isEqualTo(CancelReason.OUT_OF_STOCK);
    }

    @Test
    void anUnknownReplyTypeAndABrokenMessageDoNotStopTheListener() throws Exception {
        UUID orderId = orderService.placeOrder(1L, 1).getId();

        // Same key = same partition: the valid reply waits behind both bad messages.
        sendReply(orderId, "SomethingElse", new StockReserved(1L, 1));
        kafka.send(Topics.SAGA_REPLIES, orderId.toString(), "this is not json").get(10, TimeUnit.SECONDS);
        sendReply(orderId, MessageTypes.STOCK_RESERVED, new StockReserved(1L, 1));

        awaitStatus(orderId, OrderStatus.STOCK_RESERVED);
    }

    @Test
    void aReplyIsRecordedAsProcessed() throws Exception {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        MessageEnvelope reply = codec.wrap(MessageTypes.STOCK_RESERVED, orderId, new StockReserved(1L, 1));

        send(reply);

        awaitStatus(orderId, OrderStatus.STOCK_RESERVED);
        assertThat(processedCount(reply.messageId())).isEqualTo(1);
    }

    @Test
    void aReplyThatWasAlreadyProcessedIsSkipped() throws Exception {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        MessageEnvelope reply = codec.wrap(MessageTypes.STOCK_RESERVED, orderId, new StockReserved(1L, 1));
        // As if this exact message had been handled before: committed, but the app crashed before the offset commit.
        jdbc.update("INSERT INTO processed_message (message_id, processed_at) VALUES (?, now())", reply.messageId());

        send(reply);
        UUID markerId = sendMarker(orderId); // same key = same partition: handled after the reply

        awaitProcessed(markerId);
        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.PENDING);
    }

    @Test
    void aBrokenReplyGoesToTheDeadLetterTopic() throws Exception {
        String key = UUID.randomUUID().toString();

        kafka.send(Topics.SAGA_REPLIES, key, "this is not json").get(10, TimeUnit.SECONDS);

        assertThat(awaitRecord(Topics.SAGA_REPLIES_DLT, key).value()).isEqualTo("this is not json");
    }

    private void send(MessageEnvelope envelope) throws Exception {
        kafka.send(Topics.SAGA_REPLIES, envelope.orderId().toString(), codec.toJson(envelope)).get(10, TimeUnit.SECONDS);
    }

    /** A message of an unknown type: skipped, but recorded as processed - so we can tell when it was reached. */
    private UUID sendMarker(UUID orderId) throws Exception {
        MessageEnvelope marker = codec.wrap("Marker", orderId, new StockReserved(0L, 0));
        send(marker);
        return marker.messageId();
    }

    private long processedCount(UUID messageId) {
        return jdbc.queryForObject("SELECT count(*) FROM processed_message WHERE message_id = ?", Long.class, messageId);
    }

    private void awaitProcessed(UUID messageId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (processedCount(messageId) == 1) {
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

    private void sendReply(UUID orderId, String type, Object payload) throws Exception {
        String json = codec.toJson(codec.wrap(type, orderId, payload));
        kafka.send(Topics.SAGA_REPLIES, orderId.toString(), json).get(10, TimeUnit.SECONDS);
    }

    private void awaitStatus(UUID orderId, OrderStatus expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Order order = orders.findById(orderId).orElseThrow();
            if (order.getStatus() == expected) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Order " + orderId + " never reached " + expected);
    }
}

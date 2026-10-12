package com.kafkademo.inventory.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.InvalidMessageException;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReleaseStock;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.contracts.payload.StockReservationFailed;
import com.kafkademo.contracts.payload.StockReserved;
import com.kafkademo.inventory.IntegrationTestContainers;
import com.kafkademo.inventory.product.Product;
import com.kafkademo.inventory.product.ProductRepository;
import com.kafkademo.inventory.reservation.ReservationStatus;
import com.kafkademo.inventory.reservation.StockReservationRepository;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
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
class InventoryCommandListenerTest {

    @Autowired
    KafkaTemplate<String, String> kafka;
    @Autowired
    ConsumerFactory<String, String> consumerFactory;
    @Autowired
    EnvelopeCodec codec;
    @Autowired
    ProductRepository products;
    @Autowired
    StockReservationRepository reservations;
    @Autowired
    JdbcTemplate jdbc;

    @Test
    void reservesStockAndRepliesStockReserved() throws Exception {
        long productId = newProduct(3);
        UUID orderId = UUID.randomUUID();

        sendCommand(orderId, MessageTypes.RESERVE_STOCK, new ReserveStock(productId, 2));

        MessageEnvelope reply = awaitReply(orderId);
        assertThat(reply.type()).isEqualTo(MessageTypes.STOCK_RESERVED);
        assertThat(codec.payload(reply, StockReserved.class)).isEqualTo(new StockReserved(productId, 2));
        assertThat(products.findById(productId).orElseThrow().getStock()).isEqualTo(1);
        assertThat(reservations.findById(orderId).orElseThrow().getStatus()).isEqualTo(ReservationStatus.RESERVED);
    }

    @Test
    void notEnoughStockRepliesOutOfStockAndKeepsTheStock() throws Exception {
        long productId = newProduct(1);
        UUID orderId = UUID.randomUUID();

        sendCommand(orderId, MessageTypes.RESERVE_STOCK, new ReserveStock(productId, 2));

        MessageEnvelope reply = awaitReply(orderId);
        assertThat(reply.type()).isEqualTo(MessageTypes.STOCK_RESERVATION_FAILED);
        assertThat(codec.payload(reply, StockReservationFailed.class).reason()).isEqualTo("OUT_OF_STOCK");
        assertThat(products.findById(productId).orElseThrow().getStock()).isEqualTo(1);
        assertThat(reservations.findById(orderId)).isEmpty();
    }

    @Test
    void anUnknownProductRepliesUnknownProduct() throws Exception {
        UUID orderId = UUID.randomUUID();

        sendCommand(orderId, MessageTypes.RESERVE_STOCK, new ReserveStock(987_654_321L, 1));

        MessageEnvelope reply = awaitReply(orderId);
        assertThat(reply.type()).isEqualTo(MessageTypes.STOCK_RESERVATION_FAILED);
        assertThat(codec.payload(reply, StockReservationFailed.class).reason()).isEqualTo("UNKNOWN_PRODUCT");
    }

    @Test
    void anUnknownCommandTypeIsSkipped() throws Exception {
        long productId = newProduct(5);
        UUID orderId = UUID.randomUUID();

        sendCommand(orderId, "Bogus", new ReserveStock(productId, 1));
        sendCommand(orderId, MessageTypes.RESERVE_STOCK, new ReserveStock(productId, 1));

        assertThat(awaitReply(orderId).type()).isEqualTo(MessageTypes.STOCK_RESERVED);
        assertThat(products.findById(productId).orElseThrow().getStock()).isEqualTo(4);
    }

    @Test
    void aBrokenMessageDoesNotBlockTheMessagesBehindIt() throws Exception {
        long productId = newProduct(5);
        UUID orderId = UUID.randomUUID();

        // Same key = same partition: the valid message waits right behind the broken one.
        sendRaw(orderId.toString(), "this is not json");
        sendCommand(orderId, MessageTypes.RESERVE_STOCK, new ReserveStock(productId, 1));

        assertThat(awaitReply(orderId).type()).isEqualTo(MessageTypes.STOCK_RESERVED);
    }

    /** E3 in a test: the same message delivered twice changes the stock once and produces one reply. */
    @Test
    void aDuplicateMessageIsProcessedOnce() throws Exception {
        long productId = newProduct(5);
        UUID orderId = UUID.randomUUID();
        String json = codec.toJson(codec.wrap(MessageTypes.RESERVE_STOCK, orderId, new ReserveStock(productId, 2)));

        sendRaw(orderId.toString(), json);
        sendRaw(orderId.toString(), json);
        UUID markerId = sendMarker(orderId); // same key = same partition: handled after both copies

        awaitProcessed(markerId);
        assertThat(products.findById(productId).orElseThrow().getStock()).isEqualTo(3);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event WHERE aggregate_id = ?", Long.class, orderId))
                .isEqualTo(1);
    }

    @Test
    void aReleaseStockCommandPutsTheStockBack() throws Exception {
        long productId = newProduct(5);
        UUID orderId = UUID.randomUUID();
        sendCommand(orderId, MessageTypes.RESERVE_STOCK, new ReserveStock(productId, 2));
        assertThat(awaitReply(orderId).type()).isEqualTo(MessageTypes.STOCK_RESERVED);

        sendCommand(orderId, MessageTypes.RELEASE_STOCK, new ReleaseStock("PAYMENT_FAILED"));
        UUID markerId = sendMarker(orderId); // same key: handled after the ReleaseStock

        awaitProcessed(markerId);
        assertThat(products.findById(productId).orElseThrow().getStock()).isEqualTo(5);
        assertThat(reservations.findById(orderId).orElseThrow().getStatus()).isEqualTo(ReservationStatus.RELEASED);
    }

    @Test
    void aBrokenMessageGoesToTheDeadLetterTopic() throws Exception {
        String key = UUID.randomUUID().toString();

        sendRaw(key, "this is not json");

        ConsumerRecord<String, String> dead = awaitRecord(Topics.INVENTORY_COMMANDS_DLT, key);
        assertThat(dead.value()).isEqualTo("this is not json");
        assertThat(header(dead, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(Topics.INVENTORY_COMMANDS);
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isEqualTo(InvalidMessageException.class.getName());
    }

    private long newProduct(int stock) {
        long id = ThreadLocalRandom.current().nextLong(10_000, 1_000_000_000L); // away from the seed ids 1-4
        products.save(new Product(id, "Test product " + id, new BigDecimal("5.00"), stock));
        return id;
    }

    private void sendCommand(UUID orderId, String type, Object payload) throws Exception {
        String json = codec.toJson(codec.wrap(type, orderId, payload));
        kafka.send(Topics.INVENTORY_COMMANDS, orderId.toString(), json).get(10, TimeUnit.SECONDS);
    }

    private void sendRaw(String key, String value) throws Exception {
        kafka.send(Topics.INVENTORY_COMMANDS, key, value).get(10, TimeUnit.SECONDS);
    }

    /** A message of an unknown type: skipped, but recorded as processed - so we can tell when it was reached. */
    private UUID sendMarker(UUID orderId) throws Exception {
        MessageEnvelope marker = codec.wrap("Marker", orderId, new ReserveStock(0L, 0));
        sendRaw(orderId.toString(), codec.toJson(marker));
        return marker.messageId();
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

    private MessageEnvelope awaitReply(UUID orderId) {
        return codec.fromJson(awaitRecord(Topics.SAGA_REPLIES, orderId.toString()).value());
    }

    /** Reads a topic from the beginning until a record with this key shows up. */
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

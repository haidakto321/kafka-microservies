# M2 - Reliability: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the order -> inventory -> order message flow reliable: every message goes through a transactional outbox, every consumer skips duplicates, failing messages are retried and then parked in a dead-letter topic - and experiments E1, E2, E3, E4, E6 are run and written up for a Kafka beginner.

**Architecture:** Each service owns an `outbox_event` and a `processed_message` table (database per service, so the small outbox / dedupe classes are copied per service on purpose). A business change and its outgoing message are saved in one database transaction; a scheduled `OutboxRelay` sends unsent rows to Kafka and marks them. Each listener hands the message to a `@Transactional` handler that checks `processed_message`, does the work, writes any reply to the outbox and records the `messageId` - all in one commit. A shared Spring Kafka `DefaultErrorHandler` bean retries 3 times (1 s, 2 s, 4 s) and then sends the record to `<topic>.DLT`; `InvalidMessageException` skips the retries.

**Tech Stack:** Java 21, Spring Boot 3.5.6, Spring Kafka (`DefaultErrorHandler`, `DeadLetterPublishingRecoverer`), Spring Data JPA + Hibernate 6 (`@JdbcTypeCode(SqlTypes.JSON)` for `jsonb`), Flyway, PostgreSQL 17.4 (`FOR UPDATE SKIP LOCKED`, partial index), `apache/kafka-native:4.3.1`, Testcontainers.

**Spec:** `D:\project\kafka-microservices-demo\docs\superpowers\specs\2026-10-11-kafka-microservices-demo-design.md` (sections 6.1-6.5, 8, 9, 10, 11, 12 M2)

**Inputs from M1:** `docs/implementation-notes.md` "Notes for the M2 plan" (from the M1 final review) - all five are handled here (see the rulings below).

## Roadmap (one plan per milestone)

| Milestone | Plan | Main tasks |
|---|---|---|
| M1 Kafka basics | `2026-10-11-m1-kafka-basics.md` (done) | build, compose, contracts, catalog, reservation, order placement, E5/E7 |
| **M2 Reliability** | **this file** | contracts hardening, outbox + relay, idempotent consumer, retries + DLT, demo delay, E1-E4, E6 |
| M3 Full saga | written after M2 | payment-service, `ChargePayment` / compensation `ReleaseStock`, notification-service |
| M4 Patterns | written after M3 | api-gateway, Resilience4j, Jaeger + Prometheus + Grafana, outbox trace context (adds `trace_parent`), outbox gauge, E8/E9 |
| M5 Finish | written after M4 | README, LEARNINGS Q&A, end-to-end check |

## Decisions this plan makes (not spelled out in the spec)

1. **Outbox / dedupe code is copied per service**, not put in a shared module. The spec's repo layout keeps `messaging-contracts` "records only", and database-per-service means each service owns its tables. Cost: about 5 small classes per service (M3 adds a third copy). LEARNINGS explains the trade-off.
2. **No `trace_parent` column yet.** The spec's data model lists it, but nothing fills it until M4 (tracing). M4 adds it with its own migration.
3. **No `ErrorHandlingDeserializer`.** Values are read with `StringDeserializer`, which cannot fail; the JSON is parsed in our code and a parse error is an `InvalidMessageException`, marked not retryable. (Same as the M1 ruling.) LEARNINGS explains when you do need it.
4. **Optimistic-lock conflicts are retried inside the listener** (3 immediate attempts) before the error handler sees them, so a valid command does not use up the 3 slow retries and land in the DLT under load (M1 review note).
5. **`StockReservation` and `ProcessedMessage` are insert-only** (`Persistable.isNew()`), so a second row for the same key fails on the primary key instead of silently updating (M1 review I1).
6. **The relay stops at the first failed send** in a batch and commits the rows already sent. Kafka down = one failure per run, no duplicates, and a newer row never overtakes an older one. Cost: one bad row holds back the rows behind it (head-of-line blocking) - acceptable, a send only fails when Kafka is unreachable.
7. **`demo.processing-delay-ms` pauses after the DB commit and before the offset commit.** That is the window where a crash causes a redelivery, so E1 shows the idempotent consumer at work ("Duplicate message ... skipped"), not just a rollback.
8. **Test Kafka has topic auto-create off** (as in compose), so a missing DLT `NewTopic` bean fails the tests instead of going unnoticed (M1 review minor M7).
9. **Producer `max.block.ms: 5000`.** With Kafka down, `send()` waits for metadata for up to 60 s by default; the relay would hold its transaction that long. 5 s is enough.

## Global Constraints

- Java 21 (`<java.version>21</java.version>`), Spring Boot **3.5.6**, Maven Wrapper, Maven multi-module.
- **Every Maven command runs with JDK 21**: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"` (bash) or `. .\scripts\use-jdk21.ps1` (PowerShell). The default JDK 25 breaks Mockito inline mocks.
- Libraries: only those in spec section 13. **Any other library needs the owner's approval first.** This plan adds none (`JdbcTemplate` comes with `spring-boot-starter-data-jpa`; `@JdbcTypeCode` is Hibernate).
- No new Docker images (M2 uses `apache/kafka-native:4.3.1`, `redpandadata/console:v3.12.0`, `postgres:17.4`).
- No Lombok. Records for messages and DTOs. Constructor injection. Explicit transaction boundaries. Imports at the top, no inline fully-qualified names. Code comments in English.
- Kafka: every message key = `orderId`; topics have **3 partitions**, replication factor 1; topics (including DLTs) created by `NewTopic` beans; broker auto-create **off**.
- Surefire only runs classes named `*Test` / `*Tests` - never name a test `*IT`. Testcontainers needs `api.version=1.44` (already in the parent POM).
- The services run on the host, never as Docker images. Demo PostgreSQL on host port **5433**.
- Docs: simple English; use "-", never the long dash character.
- **Commits: allowed in this repo, but few** (owner, 2026-10-11): C1 = plan + Task 1 + Task 2, C2 = Task 3, C3 = Task 4. Personal git identity, branch `master`, no AI co-author trailer, short messages. **Do not push** unless the owner asks.
- The GitHub repo is **public**: no names from the owner's work projects (product, client or folder names) in any file.

## Review Focus

1. **Kafka is down while orders are placed (E4)** - `POST /orders` still answers 202 quickly, the `ReserveStock` row waits in the outbox, and it is sent (with the same `messageId`) once Kafka is back. Pinned by `aMessageWaitsWhileKafkaIsDownAndIsSentWhenItIsBack` (Task 3) and experiment E4.
2. **The app crashes after the DB commit but before the offset commit (E1)** - Kafka delivers the command again; stock is reserved once and one reply is sent. Pinned by `aDuplicateMessageIsProcessedOnce` (Task 2), `aReplyThatWasAlreadyProcessedIsSkipped` (Task 3) and experiment E1.
3. **A broken, null (tombstone) or `null`-literal message** - goes to `<topic>.DLT` without retries, and the messages behind it keep flowing. Pinned by `rejectsANullValue`, `rejectsTheJsonLiteralNull` (Task 1), `aBrokenMessageGoesToTheDeadLetterTopic` (Task 2), `aBrokenReplyGoesToTheDeadLetterTopic` (Task 3), `anInvalidCommandIsNotRetried` (Task 2).
4. **Two instances change the same product at once** - the lock conflict is retried right away and never sent to the DLT on its first few losses. Pinned by `aLockConflictIsTriedAgainRightAway` and `afterThreeLockConflictsTheErrorHandlerTakesOver` (Task 2).
5. **One outbox row cannot be sent** - it is retried on the next run and the rows behind it wait, so a newer message never overtakes an older one. Pinned by `aFailingMessageHoldsBackTheMessagesBehindIt` (Task 3).

---

### Task 1: Harden the messaging contracts

**Files:**
- Modify: `messaging-contracts/src/main/java/com/kafkademo/contracts/EnvelopeCodec.java`
- Modify: `messaging-contracts/src/main/java/com/kafkademo/contracts/Topics.java`
- Test: `messaging-contracts/src/test/java/com/kafkademo/contracts/EnvelopeCodecTest.java`

**Interfaces:**
- Consumes: M1 `EnvelopeCodec`, `MessageEnvelope`, `InvalidMessageException`, `Topics`.
- Produces:
  - `EnvelopeCodec.fromJson(String)` now throws `InvalidMessageException` for `null` and for the JSON text `null` (was `IllegalArgumentException` / NPE).
  - `MessageEnvelope EnvelopeCodec.envelope(UUID messageId, String type, UUID orderId, Instant occurredAt, String payloadJson)` - rebuilds an envelope from stored parts (the outbox relay uses it).
  - `Topics.INVENTORY_COMMANDS_DLT = "inventory-commands.DLT"`, `Topics.SAGA_REPLIES_DLT = "saga-replies.DLT"`.

- [ ] **Step 1: Write the failing tests** - add to `EnvelopeCodecTest` (keep the 7 existing tests):

```java
    @Test
    void rejectsANullValue() {
        // A Kafka record can have a null value (a "tombstone").
        assertThatThrownBy(() -> codec.fromJson(null))
                .isInstanceOf(InvalidMessageException.class);
    }

    @Test
    void rejectsTheJsonLiteralNull() {
        assertThatThrownBy(() -> codec.fromJson("null"))
                .isInstanceOf(InvalidMessageException.class);
    }

    @Test
    void rebuildsAnEnvelopeFromStoredParts() {
        UUID messageId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        // PostgreSQL jsonb may store the keys in another order - that must not matter.
        String storedPayload = "{\"quantity\":2,\"productId\":1}";

        MessageEnvelope rebuilt = codec.envelope(messageId, MessageTypes.RESERVE_STOCK, orderId, NOW, storedPayload);
        MessageEnvelope received = codec.fromJson(codec.toJson(rebuilt));

        assertThat(received.messageId()).isEqualTo(messageId);
        assertThat(received.type()).isEqualTo(MessageTypes.RESERVE_STOCK);
        assertThat(received.orderId()).isEqualTo(orderId);
        assertThat(received.occurredAt()).isEqualTo(NOW);
        assertThat(codec.payload(received, ReserveStock.class)).isEqualTo(new ReserveStock(1L, 2));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl messaging-contracts test`
Expected: COMPILATION ERROR - `cannot find symbol ... method envelope(...)`. (After Step 3 adds only the null handling, the two null tests would fail with `IllegalArgumentException` / `NullPointerException` instead of `InvalidMessageException` - that is the behavior being fixed.)

- [ ] **Step 3: Implement** - in `EnvelopeCodec`, replace `fromJson` and add `envelope`:

```java
    /**
     * Reads an envelope.
     *
     * @throws InvalidMessageException if the value is null, is not JSON, or misses messageId, type or orderId
     */
    public MessageEnvelope fromJson(String json) {
        if (json == null) {
            // A record with a null value (a "tombstone") has nothing to read.
            throw new InvalidMessageException("Empty message (null value)");
        }
        MessageEnvelope envelope;
        try {
            envelope = mapper.readValue(json, MessageEnvelope.class);
        } catch (JsonProcessingException e) {
            throw new InvalidMessageException("Not a valid message envelope: " + json, e);
        }
        // The JSON text "null" parses to null.
        if (envelope == null || envelope.messageId() == null || envelope.type() == null || envelope.orderId() == null) {
            throw new InvalidMessageException("Envelope is missing messageId, type or orderId: " + json);
        }
        return envelope;
    }

    /**
     * Rebuilds an envelope from stored parts. The outbox relay uses it to send a row exactly as it was written -
     * with the same messageId every time it is sent, so consumers can spot a resend.
     */
    public MessageEnvelope envelope(UUID messageId, String type, UUID orderId, Instant occurredAt, String payloadJson) {
        try {
            return new MessageEnvelope(messageId, type, orderId, occurredAt, mapper.readTree(payloadJson));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Stored payload of message " + messageId + " is not JSON", e);
        }
    }
```

In `Topics`, after `ORDER_EVENTS`:

```java
    /** Dead-letter topics: Spring Kafka's DeadLetterPublishingRecoverer sends a record that failed to "<topic>.DLT". */
    public static final String INVENTORY_COMMANDS_DLT = INVENTORY_COMMANDS + ".DLT";
    public static final String SAGA_REPLIES_DLT = SAGA_REPLIES + ".DLT";
```

- [ ] **Step 4: Run the tests and install the module for the services**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl messaging-contracts -am install`
Expected: `Tests run: 10, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 5: Checkpoint (no commit yet - C1 is after Task 2)**

---

### Task 2: inventory-service - outbox, idempotent consumer, retries + DLT, demo delay

**Files:**
- Create: `inventory-service/src/main/resources/db/migration/V3__outbox_and_processed_message.sql`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/outbox/OutboxEvent.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/outbox/OutboxEventRepository.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/outbox/OutboxWriter.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/outbox/OutboxRelay.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/messaging/ProcessedMessage.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/messaging/ProcessedMessageRepository.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/messaging/InventoryCommandHandler.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/config/SchedulingConfig.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/config/KafkaErrorHandlingConfig.java`
- Modify: `inventory-service/src/main/java/com/kafkademo/inventory/messaging/InventoryCommandListener.java` (hands off to the handler; lock-conflict retry; demo delay)
- Modify: `inventory-service/src/main/java/com/kafkademo/inventory/reservation/StockReservation.java` (insert-only)
- Modify: `inventory-service/src/main/java/com/kafkademo/inventory/config/KafkaTopicsConfig.java` (DLT topic)
- Modify: `inventory-service/src/main/resources/application.yml`
- Delete: `inventory-service/src/main/java/com/kafkademo/inventory/messaging/ReplyPublisher.java`
- Modify test: `inventory-service/src/test/java/com/kafkademo/inventory/IntegrationTestContainers.java`
- Modify test: `inventory-service/src/test/java/com/kafkademo/inventory/messaging/InventoryCommandListenerTest.java`
- Create test: `inventory-service/src/test/java/com/kafkademo/inventory/reservation/StockServiceTest.java`
- Create test: `inventory-service/src/test/java/com/kafkademo/inventory/messaging/KafkaErrorHandlingTest.java`
- Create test: `inventory-service/src/test/java/com/kafkademo/inventory/messaging/InventoryCommandListenerRetryTest.java`

**Interfaces:**
- Consumes (Task 1): `EnvelopeCodec.envelope(...)`, `fromJson` null handling, `Topics.INVENTORY_COMMANDS_DLT`; M1: `StockService.reserve(UUID, long, int) -> ReservationResult`, `Product`, `ProductRepository`.
- Produces:
  - Tables `outbox_event`, `processed_message` in `inventory_db` (V3).
  - `OutboxWriter.add(String topic, UUID orderId, String type, Object payload)` - `Propagation.MANDATORY`.
  - `OutboxRelay.publishPending()` - `@Scheduled(fixedDelay = 500)`.
  - `InventoryCommandHandler.handle(MessageEnvelope)` - `@Transactional`.
  - `InventoryCommandListener(EnvelopeCodec, InventoryCommandHandler, @Value("${demo.processing-delay-ms:0}") long)`.
  - Property `demo.processing-delay-ms` (Task 4 sets it from the command line).
  - Bean `DefaultErrorHandler kafkaErrorHandler` and topic `inventory-commands.DLT`.

#### Cycle A - outbox + idempotent consumer

- [ ] **Step 1: Turn topic auto-create off in the test broker** - `IntegrationTestContainers.kafka()`:

```java
    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        // Same as docker-compose.yml: topics are only created on purpose (NewTopic beans). With auto-create on,
        // a missing NewTopic bean - e.g. for a dead-letter topic - would go unnoticed in the tests.
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.3.1"))
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }
```

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl inventory-service test`
Expected: `Tests run: 13, Failures: 0, Errors: 0` - M1 does not depend on auto-create.

- [ ] **Step 2: Write the failing tests**

In `InventoryCommandListenerTest`: add the field and imports, replace `awaitReply` with the generic `awaitRecord` below, and add the two tests and helpers.

```java
import com.kafkademo.contracts.InvalidMessageException;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.KafkaHeaders;
```

```java
    @Autowired
    JdbcTemplate jdbc;

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
    void aBrokenMessageGoesToTheDeadLetterTopic() throws Exception {
        String key = UUID.randomUUID().toString();

        sendRaw(key, "this is not json");

        ConsumerRecord<String, String> dead = awaitRecord(Topics.INVENTORY_COMMANDS_DLT, key);
        assertThat(dead.value()).isEqualTo("this is not json");
        assertThat(header(dead, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(Topics.INVENTORY_COMMANDS);
        assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).isEqualTo(InvalidMessageException.class.getName());
    }
```

Helpers (replace the old `awaitReply` method):

```java
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
```

Create `reservation/StockServiceTest.java`:

```java
package com.kafkademo.inventory.reservation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkademo.inventory.IntegrationTestContainers;
import com.kafkademo.inventory.product.Product;
import com.kafkademo.inventory.product.ProductRepository;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class StockServiceTest {

    @Autowired
    StockService stockService;
    @Autowired
    ProductRepository products;

    /** The second guard behind the processed-message check: one reservation per order, enforced by the primary key. */
    @Test
    void aSecondReservationForTheSameOrderIsRejected() {
        long productId = ThreadLocalRandom.current().nextLong(10_000, 1_000_000_000L);
        products.save(new Product(productId, "Guarded", new BigDecimal("1.00"), 5));
        UUID orderId = UUID.randomUUID();
        stockService.reserve(orderId, productId, 1);

        assertThatThrownBy(() -> stockService.reserve(orderId, productId, 1))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(products.findById(productId).orElseThrow().getStock()).isEqualTo(4);
    }
}
```

- [ ] **Step 3: Run them to verify they fail**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl inventory-service test -Dtest='InventoryCommandListenerTest#aDuplicateMessageIsProcessedOnce+aBrokenMessageGoesToTheDeadLetterTopic,StockServiceTest'`
Expected: 3 tests not passing -
- `aDuplicateMessageIsProcessedOnce`: error `relation "processed_message" does not exist` (the feature is missing).
- `aBrokenMessageGoesToTheDeadLetterTopic`: `AssertionError: No record on inventory-commands.DLT ...` after 30 s (no DLT yet - Cycle B).
- `aSecondReservationForTheSameOrderIsRejected`: `Expecting code to raise a throwable` (the second save silently updated the row).

- [ ] **Step 4: Write the migration** `db/migration/V3__outbox_and_processed_message.sql`:

```sql
-- Outbox (spec 6.1): a message is first written here, in the same transaction as the business change.
-- OutboxRelay sends it to Kafka afterwards and sets published_at.
CREATE TABLE outbox_event (
    id           UUID         PRIMARY KEY,   -- = the messageId of the Kafka message
    aggregate_id UUID         NOT NULL,      -- the orderId; also the Kafka key
    topic        VARCHAR(100) NOT NULL,
    type         VARCHAR(50)  NOT NULL,
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ                 -- NULL = not sent yet
);

-- Partial index: only unsent rows are in it, so the relay's query stays fast however many sent rows pile up.
CREATE INDEX outbox_event_unpublished ON outbox_event (created_at) WHERE published_at IS NULL;

-- Idempotent consumer (spec 6.2): the messageId of every message already handled.
CREATE TABLE processed_message (
    message_id   UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
```

- [ ] **Step 5: Write the outbox classes**

`outbox/OutboxEvent.java`:

```java
package com.kafkademo.inventory.outbox;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/** A message waiting to be sent to Kafka - or already sent, when publishedAt is set. */
@Entity
@Table(name = "outbox_event")
public class OutboxEvent {

    /** The messageId of the Kafka message. It stays the same if the row is sent twice, so consumers spot the resend. */
    @Id
    private UUID id;

    /** The orderId: the Kafka key, and the order this message belongs to. */
    private UUID aggregateId;

    private String topic;

    private String type;

    /** The payload as JSON text, stored in a jsonb column. */
    @JdbcTypeCode(SqlTypes.JSON)
    private String payload;

    private Instant createdAt;

    /** null = not sent yet. */
    private Instant publishedAt;

    protected OutboxEvent() {
        // for JPA
    }

    public OutboxEvent(UUID id, UUID aggregateId, String topic, String type, String payload, Instant createdAt) {
        this.id = id;
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.type = type;
        this.payload = payload;
        this.createdAt = createdAt;
    }

    public void markPublished(Instant now) {
        publishedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public UUID getAggregateId() {
        return aggregateId;
    }

    public String getTopic() {
        return topic;
    }

    public String getType() {
        return type;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
```

`outbox/OutboxEventRepository.java`:

```java
package com.kafkademo.inventory.outbox;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Locks up to {@code limit} unsent rows, oldest first, until the transaction ends.
     * SKIP LOCKED: rows already locked by another relay instance are skipped instead of waited for,
     * so several instances can run the relay without sending the same row.
     */
    @Query(value = """
            SELECT * FROM outbox_event
            WHERE published_at IS NULL
            ORDER BY created_at
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> lockUnpublished(@Param("limit") int limit);
}
```

`outbox/OutboxWriter.java`:

```java
package com.kafkademo.inventory.outbox;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxWriter {

    private final OutboxEventRepository outbox;
    private final EnvelopeCodec codec;

    public OutboxWriter(OutboxEventRepository outbox, EnvelopeCodec codec) {
        this.outbox = outbox;
        this.codec = codec;
    }

    /**
     * Adds a message to the outbox. MANDATORY: it must run inside the caller's transaction, so the message is
     * saved together with the business change - or not at all. Called without a transaction, it throws.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void add(String topic, UUID orderId, String type, Object payload) {
        MessageEnvelope envelope = codec.wrap(type, orderId, payload);
        outbox.save(new OutboxEvent(envelope.messageId(), orderId, topic, type,
                envelope.payload().toString(), envelope.occurredAt()));
    }
}
```

`outbox/OutboxRelay.java`:

```java
package com.kafkademo.inventory.outbox;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Sends outbox rows to Kafka (spec 6.1).
 * Every 500 ms: lock up to 100 unsent rows, send them oldest first, waiting for the broker to confirm each one,
 * and mark the sent ones. It stops at the first failure (e.g. Kafka is down): the rest wait for the next run,
 * so a newer message never overtakes an older one.
 * Trade-off: the DB transaction stays open while talking to Kafka. Simple, and fine for a demo (see LEARNINGS).
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 100;

    private final OutboxEventRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final EnvelopeCodec codec;
    private final Clock clock;

    public OutboxRelay(OutboxEventRepository outbox, KafkaTemplate<String, String> kafka, EnvelopeCodec codec,
                       Clock clock) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.codec = codec;
        this.clock = clock;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void publishPending() {
        List<OutboxEvent> events = outbox.lockUnpublished(BATCH_SIZE);
        int sent = 0;
        for (OutboxEvent event : events) {
            if (!send(event)) {
                break;
            }
            event.markPublished(clock.instant());
            sent++;
        }
        if (sent > 0) {
            log.info("Outbox relay: sent {} of {} waiting message(s)", sent, events.size());
        }
    }

    private boolean send(OutboxEvent event) {
        MessageEnvelope envelope = codec.envelope(event.getId(), event.getType(), event.getAggregateId(),
                event.getCreatedAt(), event.getPayload());
        try {
            // .get(...) waits for the broker to confirm the write (acks=all).
            kafka.send(event.getTopic(), event.getAggregateId().toString(), codec.toJson(envelope))
                    .get(10, TimeUnit.SECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            // RuntimeException: the producer can also throw directly instead of failing the future.
            log.warn("Outbox relay: could not send {} {} for order {} - will try again: {}",
                    event.getType(), event.getId(), event.getAggregateId(), e.getMessage());
            return false;
        }
    }
}
```

`config/SchedulingConfig.java`:

```java
package com.kafkademo.inventory.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Turns on @Scheduled - used by the outbox relay. */
@Configuration(proxyBeanMethods = false)
@EnableScheduling
public class SchedulingConfig {
}
```

- [ ] **Step 6: Write the idempotent consumer classes**

`messaging/ProcessedMessage.java`:

```java
package com.kafkademo.inventory.messaging;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Persistable;

/** "This message was already handled." Saved in the same transaction as the work the message caused. */
@Entity
@Table(name = "processed_message")
public class ProcessedMessage implements Persistable<UUID> {

    @Id
    private UUID messageId;

    private Instant processedAt;

    /** See StockReservation.isNew(): makes save() an INSERT, so two copies racing each other cannot both win. */
    @Transient
    private boolean isNew = true;

    protected ProcessedMessage() {
        // for JPA
    }

    public ProcessedMessage(UUID messageId, Instant processedAt) {
        this.messageId = messageId;
        this.processedAt = processedAt;
    }

    @Override
    public UUID getId() {
        return messageId;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        isNew = false;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
```

`messaging/ProcessedMessageRepository.java`:

```java
package com.kafkademo.inventory.messaging;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, UUID> {
}
```

`messaging/InventoryCommandHandler.java`:

```java
package com.kafkademo.inventory.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.contracts.payload.StockReservationFailed;
import com.kafkademo.contracts.payload.StockReserved;
import com.kafkademo.inventory.outbox.OutboxWriter;
import com.kafkademo.inventory.reservation.ReservationResult;
import com.kafkademo.inventory.reservation.StockService;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryCommandHandler {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandHandler.class);

    private final ProcessedMessageRepository processedMessages;
    private final StockService stockService;
    private final OutboxWriter outbox;
    private final EnvelopeCodec codec;
    private final Clock clock;

    public InventoryCommandHandler(ProcessedMessageRepository processedMessages, StockService stockService,
                                   OutboxWriter outbox, EnvelopeCodec codec, Clock clock) {
        this.processedMessages = processedMessages;
        this.stockService = stockService;
        this.outbox = outbox;
        this.codec = codec;
        this.clock = clock;
    }

    /**
     * Handles one command in ONE transaction: the duplicate check, the stock change and the reply (an outbox row)
     * are committed together, or not at all. A copy delivered again later finds its messageId and is skipped.
     */
    @Transactional
    public void handle(MessageEnvelope envelope) {
        if (processedMessages.existsById(envelope.messageId())) {
            log.info("Duplicate message {} ({}) for order {} - already processed, skipped",
                    envelope.messageId(), envelope.type(), envelope.orderId());
            return;
        }
        processedMessages.save(new ProcessedMessage(envelope.messageId(), clock.instant()));
        switch (envelope.type()) {
            case MessageTypes.RESERVE_STOCK -> reserveStock(envelope);
            default -> log.warn("Ignoring unknown command type '{}' (messageId={})", envelope.type(), envelope.messageId());
        }
    }

    private void reserveStock(MessageEnvelope envelope) {
        ReserveStock command = codec.payload(envelope, ReserveStock.class);
        ReservationResult result = stockService.reserve(envelope.orderId(), command.productId(), command.quantity());
        if (result.reserved()) {
            outbox.add(Topics.SAGA_REPLIES, envelope.orderId(), MessageTypes.STOCK_RESERVED,
                    new StockReserved(command.productId(), command.quantity()));
        } else {
            outbox.add(Topics.SAGA_REPLIES, envelope.orderId(), MessageTypes.STOCK_RESERVATION_FAILED,
                    new StockReservationFailed(command.productId(), command.quantity(), result.reason()));
        }
    }
}
```

`messaging/InventoryCommandListener.java` (whole file; the lock-conflict retry and the delay come in Cycle C):

```java
package com.kafkademo.inventory.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka glue only: reads the record and hands it to the transactional handler. */
@Component
public class InventoryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);

    private final EnvelopeCodec codec;
    private final InventoryCommandHandler handler;

    public InventoryCommandListener(EnvelopeCodec codec, InventoryCommandHandler handler) {
        this.codec = codec;
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
    public void onCommand(ConsumerRecord<String, String> record) {
        // partition + offset in the log make the experiments visible.
        log.info("Command received: partition={} offset={} key={}", record.partition(), record.offset(), record.key());
        MessageEnvelope envelope = codec.fromJson(record.value());
        handler.handle(envelope);
    }
}
```

Delete `messaging/ReplyPublisher.java` (`git rm`).

`reservation/StockReservation.java` - make it insert-only. Add `implements Persistable<UUID>`, these imports and members (keep the existing fields, factory and getters):

```java
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;
```

```java
/** Stock held for one order. One reservation per order - enforced by the primary key (see isNew). */
@Entity
@Table(name = "stock_reservation")
public class StockReservation implements Persistable<UUID> {
```

```java
    /** true until the row is saved or loaded. */
    @Transient
    private boolean isNew = true;
```

```java
    @Override
    public UUID getId() {
        return orderId;
    }

    /**
     * Spring Data's save() asks this to choose INSERT or UPDATE. We set the id ourselves (the orderId), so by
     * default Spring would guess "existing row", load it and UPDATE it - a second reservation for the same order
     * would silently overwrite the first. Returning true makes it an INSERT, and the primary key rejects it.
     */
    @Override
    public boolean isNew() {
        return isNew;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        isNew = false;
    }
```

`application.yml` - under `spring.kafka.producer.properties`, add:

```yaml
        max.block.ms: 5000            # Kafka down: send() gives up after 5 s (default 60 s); the relay tries again later
```

- [ ] **Step 7: Run the Cycle A tests and the whole module**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl inventory-service test -Dtest='InventoryCommandListenerTest,StockServiceTest,StockServiceConcurrencyTest,Product*Test'`
Expected: all pass except `aBrokenMessageGoesToTheDeadLetterTopic` (still no DLT - Cycle B): `Tests run: 16, Failures: 1`. The M1 listener tests pass unchanged: the replies now reach `saga-replies` through the outbox relay.

#### Cycle B - retries + dead-letter topic

- [ ] **Step 8: Write the failing error-handling test** `messaging/KafkaErrorHandlingTest.java`:

```java
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
```

- [ ] **Step 9: Run it to verify it fails**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl inventory-service test -Dtest='KafkaErrorHandlingTest'`
Expected: 2 failures. The M1 default handler tries 10 times with no wait and then drops the record, and there is no DLT topic: `No record on inventory-commands.DLT` (or a Mockito `Wanted 1 time ... But was 10 times`).

- [ ] **Step 10: Implement the error handler and the DLT topic**

`config/KafkaErrorHandlingConfig.java`:

```java
package com.kafkademo.inventory.config;

import com.kafkademo.contracts.InvalidMessageException;
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
        DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(kafka);
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
```

`config/KafkaTopicsConfig.java` - add:

```java
    @Bean
    NewTopic inventoryCommandsDeadLetterTopic() {
        // Same partition count: a failed record goes to the same partition number in the DLT.
        return TopicBuilder.name(Topics.INVENTORY_COMMANDS_DLT).partitions(Topics.PARTITIONS).replicas(1).build();
    }
```

- [ ] **Step 11: Run the error-handling and DLT tests**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl inventory-service test -Dtest='KafkaErrorHandlingTest,InventoryCommandListenerTest'`
Expected: `Tests run: 9, Failures: 0, Errors: 0`.

#### Cycle C - lock-conflict retry + demo delay

- [ ] **Step 12: Write the failing unit test** `messaging/InventoryCommandListenerRetryTest.java`:

```java
package com.kafkademo.inventory.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.inventory.product.Product;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

/** Plain unit test: no Spring, no containers. */
class InventoryCommandListenerRetryTest {

    private final EnvelopeCodec codec = new EnvelopeCodec(new ObjectMapper().findAndRegisterModules());
    private final InventoryCommandHandler handler = mock(InventoryCommandHandler.class);

    @Test
    void aLockConflictIsTriedAgainRightAway() throws Exception {
        doThrow(lockConflict()).doThrow(lockConflict()).doNothing().when(handler).handle(any());

        listener(0).onCommand(aRecord());

        verify(handler, times(3)).handle(any());
    }

    @Test
    void afterThreeLockConflictsTheErrorHandlerTakesOver() {
        doThrow(lockConflict()).when(handler).handle(any());

        assertThatThrownBy(() -> listener(0).onCommand(aRecord()))
                .isInstanceOf(OptimisticLockingFailureException.class);
        verify(handler, times(3)).handle(any());
    }

    @Test
    void theDemoDelayPausesAfterTheCommandIsHandled() throws Exception {
        long start = System.nanoTime();

        listener(300).onCommand(aRecord());

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(300));
        verify(handler).handle(any());
    }

    private InventoryCommandListener listener(long delayMs) {
        return new InventoryCommandListener(codec, handler, delayMs);
    }

    private ConsumerRecord<String, String> aRecord() {
        UUID orderId = UUID.randomUUID();
        String json = codec.toJson(codec.wrap(MessageTypes.RESERVE_STOCK, orderId, new ReserveStock(1L, 1)));
        return new ConsumerRecord<>(Topics.INVENTORY_COMMANDS, 0, 0L, orderId.toString(), json);
    }

    private static ObjectOptimisticLockingFailureException lockConflict() {
        return new ObjectOptimisticLockingFailureException(Product.class, 1L);
    }
}
```

- [ ] **Step 13: Run it to verify it fails**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl inventory-service test -Dtest='InventoryCommandListenerRetryTest'`
Expected: COMPILATION ERROR - `constructor InventoryCommandListener in class InventoryCommandListener cannot be applied to given types` (no delay parameter, no retry yet).

- [ ] **Step 14: Implement** - `messaging/InventoryCommandListener.java` (whole file):

```java
package com.kafkademo.inventory.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka glue only: reads the record and hands it to the transactional handler.
 * Must NOT be @Transactional itself: the lock-conflict retry below needs each attempt to be its own transaction.
 */
@Component
public class InventoryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);
    private static final int LOCK_CONFLICT_ATTEMPTS = 3;

    private final EnvelopeCodec codec;
    private final InventoryCommandHandler handler;
    private final long processingDelayMs;

    public InventoryCommandListener(EnvelopeCodec codec, InventoryCommandHandler handler,
                                    @Value("${demo.processing-delay-ms:0}") long processingDelayMs) {
        this.codec = codec;
        this.handler = handler;
        this.processingDelayMs = processingDelayMs;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
    public void onCommand(ConsumerRecord<String, String> record) throws InterruptedException {
        // partition + offset in the log make the experiments visible.
        log.info("Command received: partition={} offset={} key={}", record.partition(), record.offset(), record.key());
        MessageEnvelope envelope = codec.fromJson(record.value());
        handleWithLockConflictRetry(envelope);
        if (processingDelayMs > 0) {
            // Demo only (experiments E1 and E6). The DB transaction is committed, but the Kafka offset is not yet:
            // a crash during this pause makes Kafka deliver the message again.
            log.info("Demo delay: waiting {} ms before the offset is committed", processingDelayMs);
            Thread.sleep(processingDelayMs);
        }
    }

    /**
     * Two instances can change the same product at the same moment; the loser's commit fails (optimistic lock).
     * That is normal under load, so try again right away a few times. Leaving it to the error handler would use
     * up its 3 slow retries, and a perfectly valid command could end up in the dead-letter topic.
     */
    private void handleWithLockConflictRetry(MessageEnvelope envelope) {
        for (int attempt = 1; ; attempt++) {
            try {
                handler.handle(envelope);
                return;
            } catch (OptimisticLockingFailureException e) {
                if (attempt == LOCK_CONFLICT_ATTEMPTS) {
                    throw e;
                }
                log.info("Order {}: lock conflict on attempt {}, trying again", envelope.orderId(), attempt);
            }
        }
    }
}
```

`application.yml` - add at the end:

```yaml
demo:
  processing-delay-ms: 0              # experiments E1/E6: pause after each command, before the offset commit
```

- [ ] **Step 15: Run the whole inventory module**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl inventory-service test`
Expected: `Tests run: 21, Failures: 0, Errors: 0` (13 from M1 + 2 listener + 1 StockServiceTest + 2 error handling + 3 retry unit tests), `BUILD SUCCESS`.

- [ ] **Step 16: Commit C1** (plan + Task 1 + Task 2)

```bash
git add docs/superpowers/plans/2026-10-11-m2-reliability.md messaging-contracts inventory-service
git commit -m "feat(inventory): outbox relay, idempotent consumer and dead-letter topic

Contracts: null-safe envelope parsing, rebuild from stored parts, DLT topic names.
Lock conflicts are retried in the listener; demo delay setting for E1/E6."
```

(`git add inventory-service` also stages the `ReplyPublisher.java` deletion. Check `git status --short` first: only these paths may be staged.)

---

### Task 3: order-service - outbox, idempotent reply handling, retries + DLT

**Files:**
- Create: `order-service/src/main/resources/db/migration/V2__outbox_and_processed_message.sql`
- Create: `order-service/src/main/java/com/kafkademo/order/outbox/OutboxEvent.java`
- Create: `order-service/src/main/java/com/kafkademo/order/outbox/OutboxEventRepository.java`
- Create: `order-service/src/main/java/com/kafkademo/order/outbox/OutboxWriter.java`
- Create: `order-service/src/main/java/com/kafkademo/order/outbox/OutboxRelay.java`
- Create: `order-service/src/main/java/com/kafkademo/order/messaging/ProcessedMessage.java`
- Create: `order-service/src/main/java/com/kafkademo/order/messaging/ProcessedMessageRepository.java`
- Create: `order-service/src/main/java/com/kafkademo/order/messaging/SagaReplyHandler.java`
- Create: `order-service/src/main/java/com/kafkademo/order/config/SchedulingConfig.java`
- Create: `order-service/src/main/java/com/kafkademo/order/config/KafkaErrorHandlingConfig.java`
- Modify: `order-service/src/main/java/com/kafkademo/order/order/OrderService.java` (outbox instead of `CommandPublisher`)
- Modify: `order-service/src/main/java/com/kafkademo/order/messaging/SagaReplyListener.java` (hands off to the handler)
- Modify: `order-service/src/main/java/com/kafkademo/order/config/KafkaTopicsConfig.java` (DLT topic)
- Modify: `order-service/src/main/resources/application.yml`
- Delete: `order-service/src/main/java/com/kafkademo/order/messaging/CommandPublisher.java`
- Modify test: `order-service/src/test/java/com/kafkademo/order/IntegrationTestContainers.java`
- Modify test: `order-service/src/test/java/com/kafkademo/order/messaging/SagaReplyListenerTest.java`
- Modify test: `order-service/src/test/java/com/kafkademo/order/web/OrderControllerTest.java` (M1 review minor: "no command sent")
- Create test: `order-service/src/test/java/com/kafkademo/order/outbox/OutboxRelayTest.java`
- Create test: `order-service/src/test/java/com/kafkademo/order/outbox/OutboxWriterTest.java`

**Interfaces:**
- Consumes (Task 1): `EnvelopeCodec.envelope(...)`, `Topics.SAGA_REPLIES_DLT`; M1: `Order`, `OrderRepository`, `OrderSagaService.onStockReserved(UUID)`, `onStockReservationFailed(UUID)`, `ProductClient`.
- Produces: tables `outbox_event`, `processed_message` in `order_db` (V2); `OutboxWriter.add(...)` (MANDATORY); `OutboxRelay.publishPending()`; `SagaReplyHandler.handle(MessageEnvelope)` (`@Transactional`); topic `saga-replies.DLT`. M3 adds `ChargePayment` / `ReleaseStock` outbox writes inside `OrderSagaService`.

#### Cycle A - outbox

- [ ] **Step 1: Turn topic auto-create off in the test broker** - same change as Task 2 Step 1, in `order-service/.../IntegrationTestContainers.java`:

```java
    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        // Same as docker-compose.yml: topics are only created on purpose (NewTopic beans). With auto-create on,
        // a missing NewTopic bean - e.g. for a dead-letter topic - would go unnoticed in the tests.
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.3.1"))
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    }
```

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl order-service test`
Expected: `Tests run: 24, Failures: 0, Errors: 0`.

- [ ] **Step 2: Write the failing tests**

`outbox/OutboxRelayTest.java`:

```java
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

    /** Replaces Spring Boot's "kafkaTemplate" bean by name (its declared type is KafkaTemplate<?, ?>). */
    @MockitoBean("kafkaTemplate")
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
```

`outbox/OutboxWriterTest.java`:

```java
package com.kafkademo.order.outbox;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.product.ProductClient;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.IllegalTransactionStateException;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class OutboxWriterTest {

    @Autowired
    OutboxWriter outbox;

    /** Same mocks as the other order tests, so Spring reuses their context (and containers). */
    @MockitoBean
    ProductClient productClient;

    /** An outbox row without a transaction around it could be saved without its business change. */
    @Test
    void addingAMessageOutsideATransactionFails() {
        assertThatThrownBy(() -> outbox.add(Topics.INVENTORY_COMMANDS, UUID.randomUUID(),
                MessageTypes.RESERVE_STOCK, new ReserveStock(1L, 1)))
                .isInstanceOf(IllegalTransactionStateException.class);
    }
}
```

- [ ] **Step 3: Run them to verify they fail**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl order-service test -Dtest='OutboxRelayTest,OutboxWriterTest'`
Expected: COMPILATION ERROR - `cannot find symbol: class OutboxWriter` (the outbox does not exist yet).

- [ ] **Step 4: Write the migration** `db/migration/V2__outbox_and_processed_message.sql` - the same SQL as Task 2 Step 4:

```sql
-- Outbox (spec 6.1): a message is first written here, in the same transaction as the business change.
-- OutboxRelay sends it to Kafka afterwards and sets published_at.
CREATE TABLE outbox_event (
    id           UUID         PRIMARY KEY,   -- = the messageId of the Kafka message
    aggregate_id UUID         NOT NULL,      -- the orderId; also the Kafka key
    topic        VARCHAR(100) NOT NULL,
    type         VARCHAR(50)  NOT NULL,
    payload      JSONB        NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ                 -- NULL = not sent yet
);

-- Partial index: only unsent rows are in it, so the relay's query stays fast however many sent rows pile up.
CREATE INDEX outbox_event_unpublished ON outbox_event (created_at) WHERE published_at IS NULL;

-- Idempotent consumer (spec 6.2): the messageId of every message already handled.
CREATE TABLE processed_message (
    message_id   UUID        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL
);
```

- [ ] **Step 5: Write the outbox classes** - copy the four classes from Task 2 Step 5 into `order-service/src/main/java/com/kafkademo/order/outbox/`, changing only the package line to `package com.kafkademo.order.outbox;`: `OutboxEvent.java`, `OutboxEventRepository.java`, `OutboxWriter.java`, `OutboxRelay.java`. The class bodies are identical (database per service: each service owns its copy). Copy `config/SchedulingConfig.java` the same way, with `package com.kafkademo.order.config;`.

- [ ] **Step 6: Send `ReserveStock` through the outbox** - `order/OrderService.java` (whole file):

```java
package com.kafkademo.order.order;

import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.order.outbox.OutboxWriter;
import com.kafkademo.order.product.ProductClient;
import com.kafkademo.order.product.ProductInfo;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final ProductClient productClient;
    private final OrderRepository orders;
    private final TransactionTemplate transactionTemplate;
    private final OutboxWriter outbox;
    private final Clock clock;

    public OrderService(ProductClient productClient, OrderRepository orders, TransactionTemplate transactionTemplate,
                        OutboxWriter outbox, Clock clock) {
        this.productClient = productClient;
        this.orders = orders;
        this.transactionTemplate = transactionTemplate;
        this.outbox = outbox;
        this.clock = clock;
    }

    /**
     * 1. Ask inventory for the price - OUTSIDE any DB transaction (never hold a DB connection during an HTTP call).
     * 2. In ONE transaction: save the order as PENDING and its ReserveStock message (an outbox row).
     *    Both are saved, or neither. The outbox relay sends the message afterwards, so Kafka can be down
     *    and the order is still accepted.
     */
    public Order placeOrder(long productId, int quantity) {
        ProductInfo product = productClient.getProduct(productId);
        Order order = Order.place(UUID.randomUUID(), productId, quantity, product.price(), clock.instant());

        Order saved = transactionTemplate.execute(status -> {
            Order created = orders.save(order);
            outbox.add(Topics.INVENTORY_COMMANDS, created.getId(), MessageTypes.RESERVE_STOCK,
                    new ReserveStock(productId, quantity));
            return created;
        });
        log.info("Order {} placed: {} x product {}, total {}", saved.getId(), quantity, productId, saved.getTotal());
        return saved;
    }

    @Transactional(readOnly = true)
    public Optional<Order> find(UUID id) {
        return orders.findById(id);
    }
}
```

Delete `messaging/CommandPublisher.java` (`git rm`).

`application.yml` - under `spring.kafka.producer.properties`, add:

```yaml
        max.block.ms: 5000            # Kafka down: send() gives up after 5 s (default 60 s); the relay tries again later
```

In `OrderControllerTest.anUnknownProductIs400AndSavesNothing`, also check that no message was queued (add `@Autowired JdbcTemplate jdbc;` and the import `org.springframework.jdbc.core.JdbcTemplate`):

```java
    @Test
    void anUnknownProductIs400AndSavesNothing() throws Exception {
        when(productClient.getProduct(99L)).thenThrow(new ProductNotFoundException(99L));
        long ordersBefore = orders.count();
        long messagesBefore = jdbc.queryForObject("SELECT count(*) FROM outbox_event", Long.class);

        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content("{\"productId\":99,\"quantity\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Product 99 not found"));

        assertThat(orders.count()).isEqualTo(ordersBefore);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event", Long.class)).isEqualTo(messagesBefore);
    }
```

- [ ] **Step 7: Run the Cycle A tests and the existing order tests**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl order-service test`
Expected: `Tests run: 27, Failures: 0, Errors: 0` (24 from M1 + 2 relay + 1 writer). `placingAnOrderSavesItPendingAndSendsReserveStock` still passes: the command now reaches Kafka through the relay.

#### Cycle B - idempotent reply handling + DLT

- [ ] **Step 8: Write the failing tests** - in `SagaReplyListenerTest` add the field, imports, three tests and helpers (keep the 3 existing tests and their helpers):

```java
import com.kafkademo.contracts.MessageEnvelope;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
```

```java
    @Autowired
    JdbcTemplate jdbc;
    @Autowired
    ConsumerFactory<String, String> consumerFactory;

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
```

- [ ] **Step 9: Run them to verify they fail**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl order-service test -Dtest='SagaReplyListenerTest'`
Expected: 3 failures - `aReplyIsRecordedAsProcessed`: `expected: 1L but was: 0L`; `aReplyThatWasAlreadyProcessedIsSkipped`: `Message ... was never processed` (nothing records messages yet); `aBrokenReplyGoesToTheDeadLetterTopic`: `No record on saga-replies.DLT`. The 3 M1 tests still pass.

- [ ] **Step 10: Implement**

Copy `messaging/ProcessedMessage.java` and `messaging/ProcessedMessageRepository.java` from Task 2 Step 6 into `order-service/src/main/java/com/kafkademo/order/messaging/`, changing only the package line to `package com.kafkademo.order.messaging;`. In the copied `ProcessedMessage`, the `isNew` field comment refers to StockReservation, which order-service does not have - use this comment instead:

```java
    /**
     * true until the row is saved or loaded. Spring Data's save() asks isNew() to choose INSERT or UPDATE; the id
     * is set by us, so by default it would guess UPDATE. INSERT means two copies racing each other cannot both win.
     */
```

Copy `config/KafkaErrorHandlingConfig.java` from Task 2 Step 10 with `package com.kafkademo.order.config;` (body identical).

`messaging/SagaReplyHandler.java`:

```java
package com.kafkademo.order.messaging;

import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.order.order.OrderSagaService;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SagaReplyHandler {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyHandler.class);

    private final ProcessedMessageRepository processedMessages;
    private final OrderSagaService saga;
    private final Clock clock;

    public SagaReplyHandler(ProcessedMessageRepository processedMessages, OrderSagaService saga, Clock clock) {
        this.processedMessages = processedMessages;
        this.saga = saga;
        this.clock = clock;
    }

    /**
     * Handles one reply in ONE transaction: the duplicate check and the order change are committed together,
     * or not at all. A copy delivered again later finds its messageId and is skipped.
     */
    @Transactional
    public void handle(MessageEnvelope envelope) {
        if (processedMessages.existsById(envelope.messageId())) {
            log.info("Duplicate message {} ({}) for order {} - already processed, skipped",
                    envelope.messageId(), envelope.type(), envelope.orderId());
            return;
        }
        processedMessages.save(new ProcessedMessage(envelope.messageId(), clock.instant()));
        switch (envelope.type()) {
            case MessageTypes.STOCK_RESERVED -> saga.onStockReserved(envelope.orderId());
            case MessageTypes.STOCK_RESERVATION_FAILED -> saga.onStockReservationFailed(envelope.orderId());
            default -> log.warn("Ignoring unknown reply type '{}' (messageId={})", envelope.type(), envelope.messageId());
        }
    }
}
```

`messaging/SagaReplyListener.java` (whole file):

```java
package com.kafkademo.order.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka glue only: reads the record and hands it to the transactional handler. */
@Component
public class SagaReplyListener {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyListener.class);

    private final EnvelopeCodec codec;
    private final SagaReplyHandler handler;

    public SagaReplyListener(EnvelopeCodec codec, SagaReplyHandler handler) {
        this.codec = codec;
        this.handler = handler;
    }

    @KafkaListener(topics = Topics.SAGA_REPLIES)
    public void onReply(ConsumerRecord<String, String> record) {
        log.info("Reply received: partition={} offset={} key={}", record.partition(), record.offset(), record.key());
        MessageEnvelope envelope = codec.fromJson(record.value());
        handler.handle(envelope);
    }
}
```

`config/KafkaTopicsConfig.java` - add:

```java
    @Bean
    NewTopic sagaRepliesDeadLetterTopic() {
        // Same partition count: a failed record goes to the same partition number in the DLT.
        return TopicBuilder.name(Topics.SAGA_REPLIES_DLT).partitions(Topics.PARTITIONS).replicas(1).build();
    }
```

- [ ] **Step 11: Run the whole order module**

Run: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot" ./mvnw -pl order-service test`
Expected: `Tests run: 30, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 12: Commit C2**

```bash
git add order-service
git commit -m "feat(order): send commands through the outbox, dedupe replies, DLT

POST /orders no longer touches Kafka: the order and its ReserveStock are saved
in one transaction and the relay sends the message."
```

(`git add order-service` also stages the `CommandPublisher.java` deletion. Check `git status --short` first.)

---

### Task 4: Scripts, experiments E1-E4 + E6, LEARNINGS, README

**Files:**
- Modify: `scripts/start-instance.ps1` (`-ExtraArgs`)
- Create: `scripts/outbox-status.ps1`
- Modify: `docs/LEARNINGS.md`, `docs/implementation-notes.md`, `README.md`

**Interfaces:**
- Consumes: Task 2 `demo.processing-delay-ms`; the `outbox_event` / `processed_message` tables of Tasks 2 and 3; M1 scripts.
- Produces: the experiment write-ups in LEARNINGS.

- [ ] **Step 1: `scripts/start-instance.ps1`** - add an `-ExtraArgs` parameter (whole file):

```powershell
# Starts ONE instance of a service in the background, on the given port.
# Example:  .\scripts\start-instance.ps1 -Service inventory-service -Port 8092
# Extra Spring Boot settings (experiments E1, E6):
#           .\scripts\start-instance.ps1 -Service inventory-service -Port 8082 -ExtraArgs '--demo.processing-delay-ms=15000'
param(
    [Parameter(Mandatory)][string]$Service,
    [Parameter(Mandatory)][int]$Port,
    [string[]]$ExtraArgs = @()
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$jar = Get-ChildItem "$root\$Service\target\$Service-*.jar" | Where-Object { $_.Name -notlike '*.original' } | Select-Object -First 1
if (-not $jar) { throw "No jar for $Service - run .\scripts\start-services.ps1 first (it builds)" }

$java = 'C:\Program Files\Eclipse Adoptium\jdk-21.0.4.7-hotspot\bin\java.exe'
New-Item -ItemType Directory -Force -Path "$root\logs" | Out-Null
$log = "$root\logs\$Service-$Port.log"

# --server.port (not -Dserver.port): PowerShell can split -D arguments at the dot.
$arguments = @('-jar', "`"$($jar.FullName)`"", "--server.port=$Port") + $ExtraArgs
$process = Start-Process -FilePath $java -ArgumentList $arguments `
    -RedirectStandardOutput $log -RedirectStandardError "$log.err" `
    -WindowStyle Hidden -PassThru
Set-Content -Path "$root\logs\$Service-$Port.pid" -Value $process.Id
Write-Host "Started $Service on port $Port (pid $($process.Id)) - log: logs\$Service-$Port.log"
```

- [ ] **Step 2: `scripts/outbox-status.ps1`**:

```powershell
# Shows how many outbox rows are waiting (not sent to Kafka yet) in each service's database.
# Example:  .\scripts\outbox-status.ps1
foreach ($db in 'order_db', 'inventory_db') {
    Write-Host "== $db"
    docker exec kafka-demo-postgres psql -U demo -d $db -c `
        "SELECT count(*) FILTER (WHERE published_at IS NULL) AS waiting, count(*) AS total FROM outbox_event"
}
```

- [ ] **Step 3: Build and start** (infra must be up: `.\scripts\start-infra.ps1`)

```powershell
.\scripts\stop-services.ps1
.\scripts\start-services.ps1
```

Expected: both services start; Flyway applies inventory `V3` and order `V2` (`Successfully applied 1 migration` in each log); `.\scripts\outbox-status.ps1` shows `waiting 0` in both databases.

- [ ] **Step 4: Experiment E4 - Kafka down, orders still accepted**

```powershell
docker compose stop kafka
Measure-Command { .\scripts\send-orders.ps1 -ProductId 1 -Quantity 1 -Count 3 }
.\scripts\outbox-status.ps1
docker compose start kafka
```

Expected: each POST answers `202 PENDING` in well under a second (no Kafka call in the request); `order_db` shows `waiting 3`; the order-service log shows `Outbox relay: could not send ... will try again` warnings; after `start kafka`, within about 10-20 s: `Outbox relay: sent 3 of 3`, the orders reach `STOCK_RESERVED` (`Invoke-RestMethod http://localhost:8081/orders/<id>`), `waiting 0` in both databases. Record: request time, the number of waiting rows, how long after the restart the rows were sent, any surprise (e.g. consumer reconnect time).

- [ ] **Step 5: Experiment E1 - kill inventory while it processes**

```powershell
.\scripts\stop-services.ps1
.\scripts\start-instance.ps1 -Service order-service -Port 8081
.\scripts\start-instance.ps1 -Service inventory-service -Port 8082 -ExtraArgs '--demo.processing-delay-ms=15000'
# wait ~20 s for both to start
.\scripts\send-orders.ps1 -ProductId 1 -Quantity 1 -Count 1
# within 15 s, as soon as "Demo delay" shows in logs\inventory-service-8082.log:
Stop-Process -Id (Get-Content logs\inventory-service-8082.pid) -Force
Remove-Item logs\inventory-service-8082.pid
.\scripts\start-instance.ps1 -Service inventory-service -Port 8082
```

Expected: before the kill the log shows `Command received ... offset=N`, `reserved 1 of product 1, X left`, `Demo delay`; the reply is already sent (the relay is a separate thread), so the order is `STOCK_RESERVED`. After the restart (and the ~45 s session timeout of the killed member), the log shows `Command received ... offset=N` again (same offset) and `Duplicate message ... already processed, skipped`. Check the stock went down by exactly 1: `Invoke-RestMethod http://localhost:8082/products/1` before and after. Record the offsets, times and stock numbers.

- [ ] **Step 6: Experiment E3 - the same message twice (Redpanda Console)**

Get one real `ReserveStock` envelope from the order database:

```powershell
docker exec kafka-demo-postgres psql -U demo -d order_db -At -c "SELECT json_build_object('messageId', id, 'type', type, 'orderId', aggregate_id, 'occurredAt', to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), 'payload', payload) FROM outbox_event ORDER BY created_at DESC LIMIT 1"
```

In Redpanda Console (http://localhost:8090) -> Topics -> `inventory-commands` -> Produce Record: key = the `orderId`, value = that JSON, as Text. Produce it twice.

Expected: the inventory log shows two `Command received` lines and two `Duplicate message ... skipped` lines (it was already processed when the order was first placed); stock unchanged; no new rows in `inventory_db.outbox_event`. Record what you saw.

- [ ] **Step 7: Experiment E2 - a broken message (Redpanda Console)**

Produce to `inventory-commands`: key `e2-broken`, value `hello` (Text). Then send one valid order: `.\scripts\send-orders.ps1 -ProductId 1 -Quantity 1`.

Expected: inventory log `Command received ... key=e2-broken`, then at once (no retries - not retryable) `Giving up on inventory-commands-P offset N (key e2-broken) - sending it to inventory-commands.DLT. Cause: JsonParseException ...`; the valid order reaches `STOCK_RESERVED`. In Redpanda Console, topic `inventory-commands.DLT` holds the record with value `hello` and headers such as `kafka_dlt-original-topic`, `kafka_dlt-original-offset`, `kafka_dlt-exception-message`. Record the headers.

- [ ] **Step 8: Experiment E6 - consumer lag**

```powershell
.\scripts\stop-services.ps1
.\scripts\start-instance.ps1 -Service order-service -Port 8081
.\scripts\start-instance.ps1 -Service inventory-service -Port 8082 -ExtraArgs '--demo.processing-delay-ms=200'
# wait ~20 s
Measure-Command { .\scripts\send-orders.ps1 -ProductId 4 -Quantity 1 -Count 500 } # Sold-out mug: no stock changes
```

While it runs and afterwards, open Redpanda Console -> Consumer Groups -> `inventory-service` and note the lag every ~15 s until it is 0.

Expected: the lag grows to a few hundred while orders arrive faster than 5 per second, then drains at about 5 messages per second (200 ms each, one consumer thread for 3 partitions); all 500 orders end `CANCELLED / OUT_OF_STOCK`. Record the peak lag and the drain time. (Grafana's lag panel comes in M4.) Then restart normally: `.\scripts\stop-services.ps1; .\scripts\start-services.ps1 -SkipBuild`.

- [ ] **Step 9: Update `docs/LEARNINGS.md`**

Keep sections 1-6 and the format rules. Changes:

1. Section 1 "How it works here": replace the last bullet with:

```markdown
- The consumer commits **after** processing each message, so a crash means the
  message is read again, not lost - "at-least-once" (section 7). Since M2 every
  consumer skips a message it has already handled (section 9), and a message that
  keeps failing goes to a dead-letter topic instead of being dropped (section 10).
```

2. Section 4 (dual write): add at the end of "How it works here":

```markdown
**Fixed in M2:** `OrderService` and `InventoryCommandHandler` no longer send to
Kafka. They write an outbox row in the same transaction (section 8), and every
consumer remembers the messages it handled (section 9).
```

3. Section 5 (broken message): add a last paragraph `**Since M2:** see section 10 and experiment E2 - the broken message goes to inventory-commands.DLT at once.`

4. Add these sections before the glossary (fill every "What I saw" with the real numbers from Steps 4-8):

````markdown
## 7. Delivery guarantees

### What it is

How many times a message is processed when something fails:

| Guarantee | Meaning | How you get it |
|---|---|---|
| **At-most-once** | 0 or 1 times - can be **lost** | Commit the offset **before** processing |
| **At-least-once** | 1 or more times - can be **duplicated** | Commit the offset **after** processing (this project) |
| **Exactly-once** | exactly 1 time | Kafka transactions - but only for Kafka-to-Kafka work |

### Why it matters

Networks and apps fail. You must choose between "may lose" and "may repeat".
Losing an order is worse than seeing it twice, so most systems pick
at-least-once and make the consumer **idempotent** (doing it twice = doing it
once). People call the result "effectively once".

### How it works here

- Producer: `acks=all` + idempotent producer - a producer retry never writes a
  duplicate **into Kafka**.
- Consumer: `ack-mode: record` - the offset is committed after the listener
  returns.
- The database work is not part of any Kafka transaction, so Kafka's
  exactly-once cannot cover it. The outbox (section 8) + the idempotent consumer
  (section 9) give "effectively once".

### Interview answer

> "Kafka gives at-least-once if you commit after processing. Exactly-once in
> Kafka only covers reading from and writing to Kafka. As soon as a database is
> involved, I use at-least-once with an idempotent consumer, and the outbox
> pattern on the sending side."

### Common follow-up questions

- *"Why not use Kafka transactions?"* - They make "read from Kafka, write to
  Kafka" atomic. Our write goes to PostgreSQL, which is not part of them.
- *"Where can duplicates come from?"* - A crash after the work but before the
  offset commit (E1), a rebalance, the outbox relay sending a row again, or
  someone producing the same message twice (E3).

## 8. The outbox pattern

### What it is

Instead of sending to Kafka directly, the service writes the message into an
`outbox_event` table **in the same database transaction** as the business
change. A separate job, the **relay**, sends the rows to Kafka later.

### Why it exists

It solves the dual-write problem (section 4): the business change and the
message are saved together or not at all. If Kafka is down, the rows just wait.

### How it works here

```
POST /orders
   |
   v
[ one DB transaction:  INSERT orders  +  INSERT outbox_event ]   -> 202 Accepted
                                              |
          OutboxRelay, every 500 ms:          v
          SELECT ... FOR UPDATE SKIP LOCKED (up to 100 unsent rows)
          send each to Kafka, wait for the broker's "OK"
          UPDATE published_at
```

- Code: `outbox/OutboxWriter`, `outbox/OutboxRelay` (one copy per service -
  each service owns its database).
- `OutboxWriter.add` is `Propagation.MANDATORY`: called without a transaction,
  it throws. That makes "same transaction" impossible to forget.
- The row id **is** the `messageId`. If the relay sends a row twice (crash after
  sending, before marking), the consumer sees the same `messageId` and skips it.
- `FOR UPDATE SKIP LOCKED`: several relay instances can run; each skips the rows
  another one has locked.
- A **partial index** (`WHERE published_at IS NULL`) keeps the query fast however
  many sent rows pile up.
- The relay stops at the first failed send, so a newer message never overtakes an
  older one.
- Trade-off: the relay keeps its DB transaction open while it talks to Kafka. Fine
  for a demo; at high volume you would send first and mark in a short separate
  transaction, or use CDC.
- Coming in M4: the outbox **breaks the trace** (the message is sent later, by
  another thread). The fix is to store the trace id in the row.

### What I saw (E4)

- What I did: stopped the Kafka container, placed 3 orders, started Kafka again.
- What I saw: (request time, waiting rows, the relay warnings, how long until the
  rows were sent and the orders moved on)
- Why: (explain in one or two sentences)

### Interview answer

> "I write the event into an outbox table in the same transaction as the
> business data, and a relay publishes it afterwards. Nothing is lost if Kafka is
> down, and nothing is sent for a rolled-back transaction. The relay can send a
> row twice, so consumers must be idempotent - the row id is the message id."

### Common follow-up questions

- *"What about CDC / Debezium?"* - Debezium reads the database log and publishes
  outbox rows with no polling and no relay code. More infrastructure; same idea.
- *"Several relay instances - what about order?"* - `SKIP LOCKED` stops double
  sends, but two instances can send rows of the same order out of order. Fixes:
  one relay instance, or partition the outbox by key.
- *"The outbox grows forever?"* - Delete or archive sent rows on a schedule.

## 9. The idempotent consumer

### What it is

A consumer that gives the same result whether a message arrives once or several
times.

### Why it exists

With at-least-once delivery, duplicates **will** happen (section 7). Without
protection, a duplicate `ReserveStock` takes the stock twice.

### How it works here

```
[ one DB transaction:
    messageId in processed_message?  -> yes: skip, done
    INSERT processed_message (messageId)
    do the work (reserve stock / change the order)
    INSERT outbox_event (the reply)                     ]  -> commit
```

- Code: `InventoryCommandHandler`, `SagaReplyHandler`.
- Everything is one commit: if the work fails, the `processed_message` row is
  rolled back too, so a retry is not wrongly skipped.
- Two copies at the same moment: both pass the check, but the second INSERT hits
  the primary key and rolls back; its retry then skips.
- **Natural idempotency** as a second guard: one reservation per order. The
  reservation is insert-only (`Persistable.isNew()`), so a second one fails on the
  primary key instead of overwriting the first (the M1 lesson in section 4).
- Not every consumer needs this: the notification service (M3) only writes a log
  line - a duplicate log line is harmless, a duplicate charge is not.

### What I saw (E1 and E3)

- E1 - What I did: inventory with a 15 s delay after the commit, killed it during
  the delay, started it again. What I saw: (offsets, the "Duplicate message"
  line, the stock before and after, the session-timeout wait). Why: (...)
- E3 - What I did: produced the same `ReserveStock` twice from Redpanda Console.
  What I saw: (...). Why: (...)

### Interview answer

> "I store the message id in a processed-messages table in the same transaction
> as the business change. A redelivered message finds its id and is skipped. I
> also use natural keys - one reservation per order - as a second guard."

### Common follow-up questions

- *"Why not check first, in its own transaction?"* - Then a crash between the
  check and the work could lose the message or do it twice. Same transaction or
  nothing.
- *"The table grows forever?"* - Delete ids older than the longest redelivery
  window (days).

## 10. Retries, poison messages and the dead-letter topic

### What it is

- **Retry**: try a failed message again, maybe after a wait (**back-off**).
- **Poison message**: a message that can never succeed (bad JSON). Retrying it is
  pointless.
- **Dead-letter topic (DLT)**: where a message goes after it has failed for
  good, so it can be inspected and replayed later.

### Why it exists

M1 had neither: a broken message was tried 10 times with no wait and then
**dropped** (section 5). Now nothing is dropped silently, and errors that can
never succeed are not retried.

### How it works here

- `config/KafkaErrorHandlingConfig` (one bean per service; Spring Boot plugs it
  into every `@KafkaListener`):
  - `InvalidMessageException` (bad JSON, missing fields, wrong payload) -> **not
    retryable** -> straight to `<topic>.DLT`.
  - anything else -> 3 retries after 1 s, 2 s, 4 s -> `<topic>.DLT`.
- The DLT gets the record **unchanged**, plus headers: `kafka_dlt-original-topic`,
  `-original-partition`, `-original-offset`, `-exception-fqcn`,
  `-exception-message`...
- The DLT topics are created by `NewTopic` beans (auto-create is off). Without
  them the error handler could not publish and would retry forever.
- These retries **block the partition**: the messages behind the failing one
  wait. That keeps the messages of one order in order. `@RetryableTopic` (retry
  topics) does not block, but a later message of the same order can overtake the
  retried one - wrong for a saga.
- Optimistic-lock conflicts (E5 in M1) are retried right away inside the listener,
  3 times, so a valid command does not end up in the DLT just because another
  instance was faster.
- No `ErrorHandlingDeserializer` here: values are read as plain strings, which
  cannot fail. You need it when Kafka's own deserializer parses JSON or Avro -
  otherwise a broken record fails **before** your code runs and can block the
  partition.

### What I saw (E2)

- What I did: produced `hello` to `inventory-commands`, then placed a valid order.
- What I saw: (the "Giving up" log line, no retries, the DLT record and its
  headers in Redpanda Console, the valid order going through)
- Why: (...)

### Interview answer

> "I use Spring Kafka's DefaultErrorHandler with exponential back-off and a
> dead-letter topic. Errors that can never succeed, like bad JSON, are marked not
> retryable and go to the DLT at once. Retries block the partition, which keeps
> per-key ordering; non-blocking retry topics would break it."

### Common follow-up questions

- *"What do you do with the DLT?"* - Alert on it, look at the headers, fix the
  cause, and replay the records to the original topic.
- *"Blocking retries stop the whole partition - is that bad?"* - For a few
  seconds, yes; that is the price of ordering. Keep the retry count and back-off
  small.

## 11. Consumer lag (experiment E6)

### What it is

**Lag** = how many messages in a partition the group has not processed yet
(newest offset minus the group's committed offset).

### Why it matters

Growing lag means the consumers are slower than the producers. It is the main
health metric of a Kafka consumer.

### How it works here

`demo.processing-delay-ms=200` slows inventory to about 5 messages per second.
Redpanda Console -> Consumer Groups shows the lag per partition. (Grafana in M4.)

### What I saw (E6)

- What I did: 500 orders for the sold-out mug against a 200 ms consumer.
- What I saw: (peak lag, drain time, messages per second)
- Why: (one thread handles all 3 partitions; more instances - up to 3 - would
  drain faster)

### Interview answer

> "Lag is the gap between the newest offset and the group's committed offset. I
> watch it per partition; if it keeps growing I add consumers, up to the number
> of partitions, or make processing faster."

## 12. Flyway and database per service

- **Flyway** runs the SQL files in `src/main/resources/db/migration` (`V1__...`,
  `V2__...`) in order at start-up and records them in `flyway_schema_history`. A
  file that already ran must never change - a change is a new `V<n>` file. M2
  added `V3` to inventory and `V2` to order.
- **Database per service**: order, inventory (and payment in M3) each own a
  database and never read each other's tables. They share data only through
  Kafka messages and the REST price call. That is why each service has its own
  `outbox_event` and `processed_message` tables, and its own copy of the small
  outbox classes.
````

5. Glossary: add rows - At-least-once, Idempotent consumer, Outbox, Relay, `SKIP LOCKED`, Partial index, Back-off, Poison message, Dead-letter topic (DLT), Consumer lag, Flyway.

6. "Coming in the next milestones": remove the M2 line.

- [ ] **Step 10: `docs/implementation-notes.md`** - add a `## M2` section with: the 9 decisions at the top of this plan (one line each), anything that differed from this plan during the build, and the experiment surprises. In the M1 "Notes for the M2 plan" list, add `(done in M2)` to each item.

- [ ] **Step 11: `README.md`** - Status: `M2 (reliability) done: outbox, idempotent consumers, retries and dead-letter topics.` Add under the run block:

````markdown
Experiments (see `docs/LEARNINGS.md` for what each one shows):

```powershell
.\scripts\outbox-status.ps1      # outbox rows waiting to be sent, per database
.\scripts\start-instance.ps1 -Service inventory-service -Port 8082 -ExtraArgs '--demo.processing-delay-ms=15000'
```
````

- [ ] **Step 12: Stop and final check**

```powershell
.\scripts\stop-services.ps1
. .\scripts\use-jdk21.ps1
.\mvnw.cmd -q test
```

Expected: all green - contracts 10, inventory 21, order 30. Grep the repo for the owner's work-project names (his own list) before committing: nothing may match.

- [ ] **Step 13: Commit C3**

```bash
git add scripts/start-instance.ps1 scripts/outbox-status.ps1 docs/LEARNINGS.md docs/implementation-notes.md README.md
git commit -m "docs: M2 experiments E1-E4 and E6, LEARNINGS and scripts"
```

# M1 - Kafka basics: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Infrastructure in Docker plus two Spring Boot services (order, inventory) that exchange `ReserveStock` / `StockReserved` / `StockReservationFailed` through Kafka, with experiments E5 and E7 run and written up for a Kafka beginner.

**Architecture:** Maven multi-module repo. `messaging-contracts` holds the topic names, message types and a JSON envelope codec. `order-service` saves an order and sends `ReserveStock` directly with `KafkaTemplate` (no outbox yet - that is M2). `inventory-service` reserves stock and replies on `saga-replies`. Every message's key is the `orderId`. Kafka, Redpanda Console and PostgreSQL run in Docker Compose; the services run on the host as normal Java apps.

**Tech Stack:** Java 21, Spring Boot 3.5.6, Spring Kafka, Spring Data JPA, Flyway, PostgreSQL 17.4, `apache/kafka-native:4.3.1` (KRaft), Redpanda Console v3.12.0, Testcontainers.

**Spec:** `D:\project\kafka-microservices-demo\docs\superpowers\specs\2026-10-11-kafka-microservices-demo-design.md`

## Roadmap (one plan per milestone)

| Milestone | Plan | Main tasks |
|---|---|---|
| **M1 Kafka basics** | **this file** | build, compose, contracts, inventory catalog + reservation, order placement + replies, scripts, E5/E7, LEARNINGS |
| M2 Reliability | written after M1 | outbox + relay in both services, idempotent consumer, retries + DLT (non-retryable classification), demo delay setting, E1-E4, E6 |
| M3 Full saga | written after M2 | payment-service, `ChargePayment` / compensation `ReleaseStock`, notification-service fan-out on `order-events` |
| M4 Patterns | written after M3 | api-gateway + filter, Resilience4j on the price call, Jaeger + Prometheus + Grafana, outbox trace context, E8/E9 |
| M5 Finish | written after M4 | README, LEARNINGS interview Q&A, end-to-end check, GitHub |

Later plans use what M1 teaches (whether `kafka-native` works with Testcontainers, whether Redpanda Console can produce messages, real versions).

## Global Constraints

- Java 21 (`<java.version>21</java.version>`), Spring Boot **3.5.6**, Maven Wrapper (`./mvnw`), Maven multi-module.
- **Every Maven command runs with JDK 21**: `JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"` (bash) or `. .\scripts\use-jdk21.ps1` (PowerShell). The default JDK 25 breaks Mockito inline mocks.
- Libraries: only those in spec section 13. **Any other library needs the owner's approval first** (e.g. no Awaitility - tests use a small own wait loop).
- No Lombok. Records for messages and DTOs. Constructor injection in application code. Explicit transaction boundaries. Imports at the top, no inline fully-qualified names. Code comments in English.
- Docker images pinned: `apache/kafka-native:4.3.1`, `redpandadata/console:v3.12.0`, `postgres:17.4` (already on the PC - reuse). The demo's PostgreSQL is on **host port 5433** (5432 is used by another local PostgreSQL container).
- Kafka: every message key = `orderId`; topics have **3 partitions**, replication factor 1; topics created by `NewTopic` beans; broker auto-create **off**.
- Surefire only runs classes named `*Test` / `*Tests` - never name a test `*IT`.
- Testcontainers needs `api.version=1.44` as a Surefire system property (Docker Engine 29, API 1.54).
- The 5 services run on the host, never as Docker images.
- Docs: simple English; use "-", never the long dash character.
- **No `git add` / `git commit` unless the owner asks.** Checkpoint steps run tests instead of committing.

## Review Focus

1. **Two orders reserve the last items at the same time** (two consumer instances) - stock must never go negative and only one reservation may succeed. Pinned by `StockServiceConcurrencyTest` (Task 4).
2. **A broken (non-JSON) message on a topic** - must not block the messages behind it on the same partition. Pinned by `aBrokenMessageDoesNotBlockTheMessagesBehindIt` (Task 4).
3. **An unknown message type** - skipped with a warning; the consumer keeps working. Pinned by `anUnknownCommandTypeIsSkipped` (Task 4) and `aReplyForAnUnknownOrderIsIgnored` (Task 6).
4. **An invalid order request or an unknown product** (quantity 0 or negative, missing productId, product not found) - 400, no order saved, no command sent. Pinned by `invalidRequestsAre400AndSaveNothing` and `anUnknownProductIs400AndSavesNothing` (Task 5).
5. **A late or duplicate reply** (e.g. `StockReserved` after the order was cancelled, or the same reply twice) - ignored, state unchanged. Pinned by `aLateStockReservedDoesNotReopenACancelledOrder` and `aDuplicateStockReservedChangesNothing` (Task 6).

---

### Task 1: Parent build and messaging contracts

**Files:**
- Create: `pom.xml`, `.gitignore`, `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties` (copied), `scripts/use-jdk21.ps1`, `docs/implementation-notes.md`
- Create: `messaging-contracts/pom.xml`
- Create: `messaging-contracts/src/main/java/com/kafkademo/contracts/Topics.java`
- Create: `messaging-contracts/src/main/java/com/kafkademo/contracts/MessageTypes.java`
- Create: `messaging-contracts/src/main/java/com/kafkademo/contracts/MessageEnvelope.java`
- Create: `messaging-contracts/src/main/java/com/kafkademo/contracts/InvalidMessageException.java`
- Create: `messaging-contracts/src/main/java/com/kafkademo/contracts/EnvelopeCodec.java`
- Create: `messaging-contracts/src/main/java/com/kafkademo/contracts/payload/ReserveStock.java`
- Create: `messaging-contracts/src/main/java/com/kafkademo/contracts/payload/StockReserved.java`
- Create: `messaging-contracts/src/main/java/com/kafkademo/contracts/payload/StockReservationFailed.java`
- Test: `messaging-contracts/src/test/java/com/kafkademo/contracts/EnvelopeCodecTest.java`

**Interfaces:**
- Produces: `Topics.INVENTORY_COMMANDS = "inventory-commands"`, `Topics.PAYMENT_COMMANDS`, `Topics.SAGA_REPLIES = "saga-replies"`, `Topics.ORDER_EVENTS`, `Topics.PARTITIONS = 3`
- Produces: `MessageTypes.RESERVE_STOCK = "ReserveStock"`, `MessageTypes.STOCK_RESERVED = "StockReserved"`, `MessageTypes.STOCK_RESERVATION_FAILED = "StockReservationFailed"`
- Produces: `record MessageEnvelope(UUID messageId, String type, UUID orderId, Instant occurredAt, JsonNode payload)`
- Produces: `EnvelopeCodec(ObjectMapper)`, `EnvelopeCodec(ObjectMapper, Clock)`, `MessageEnvelope wrap(String type, UUID orderId, Object payload)`, `String toJson(MessageEnvelope)`, `MessageEnvelope fromJson(String)` (throws `InvalidMessageException`), `<T> T payload(MessageEnvelope, Class<T>)` (throws `InvalidMessageException`)
- Produces: `record ReserveStock(long productId, int quantity)`, `record StockReserved(long productId, int quantity)`, `record StockReservationFailed(long productId, int quantity, String reason)`

- [ ] **Step 1: Copy the Maven Wrapper from the springboot-rag project**

```bash
cd /d/project/kafka-microservices-demo
cp /path/to/another-spring-boot-project/mvnw /path/to/another-spring-boot-project/mvnw.cmd .
mkdir -p .mvn/wrapper
cp /path/to/another-spring-boot-project/.mvn/wrapper/maven-wrapper.properties .mvn/wrapper/
cat .mvn/wrapper/maven-wrapper.properties
```

Expected: the file shows `distributionUrl=...apache-maven-3.9.16-bin.zip`.

- [ ] **Step 2: Create the parent `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.5.6</version>
        <relativePath/>
    </parent>

    <groupId>com.kafkademo</groupId>
    <artifactId>kafka-microservices-demo</artifactId>
    <version>0.1.0-SNAPSHOT</version>
    <packaging>pom</packaging>
    <name>kafka-microservices-demo</name>

    <properties>
        <java.version>21</java.version>
    </properties>

    <modules>
        <module>messaging-contracts</module>
    </modules>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>com.kafkademo</groupId>
                <artifactId>messaging-contracts</artifactId>
                <version>${project.version}</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <configuration>
                        <systemPropertyVariables>
                            <!-- docker-java (used by Testcontainers) defaults to an API version that
                                 Docker Engine 29.x / API 1.54 rejects with HTTP 400. docker-java reads
                                 the `api.version` system property, so pin it here. -->
                            <api.version>1.44</api.version>
                        </systemPropertyVariables>
                    </configuration>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

- [ ] **Step 3: Create `.gitignore`, `scripts/use-jdk21.ps1` and `docs/implementation-notes.md`**

`.gitignore`:
```
target/
logs/
.idea/
*.iml
.vscode/
```

`scripts/use-jdk21.ps1`:
```powershell
# Use JDK 21 in this PowerShell window. Dot-source it:  . .\scripts\use-jdk21.ps1
# The PC's default JDK is 25, which breaks Mockito inline mocks in the tests.
$env:JAVA_HOME = 'C:\Program Files\Eclipse Adoptium\jdk-21.0.4.7-hotspot'
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
```

`docs/implementation-notes.md`:
```markdown
# Implementation notes

Decisions made during the build that were not in the spec, changes, and
trade-offs. Newest at the bottom of each milestone.

## M1

- **JDK 21 for builds and runs.** The PC's default JDK is 25; JDK 25 breaks
  Mockito inline mocks. `scripts/use-jdk21.ps1` and the start scripts point
  `JAVA_HOME` at `jdk-21.0.4.7-hotspot`.
- **Messages are JSON strings with our own envelope** (`EnvelopeCodec`), sent
  with Kafka's `StringSerializer`, instead of Spring's `JsonSerializer` +
  type headers. Why: one wire format everywhere, readable in Redpanda Console,
  and the M2 outbox can store and send the exact same string. Effect on spec
  6.4: a broken message fails inside our listener (`InvalidMessageException`),
  not in the deserializer, so `ErrorHandlingDeserializer` is not needed - M2
  marks `InvalidMessageException` as not retryable instead.
- **`GET /orders/{id}` is built in M1**, not M3, so results can be checked
  without reading the database.
- **The price lookup (order -> inventory REST) is built in M1 as a plain
  `RestClient` call.** The order total needs the price before payment exists
  (M3). Resilience4j is added to this same call in M4.
- **Broker auto-create is off** (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`), so
  every topic comes from a `NewTopic` bean - a typo in a topic name fails
  loudly instead of silently creating a new topic.
- **No Docker health check on Kafka.** `apache/kafka-native` has no shell
  tools to run a check with, so `scripts/start-infra.ps1` waits for port 9092
  instead.
- **No Awaitility** (not in the approved library list). Tests use a small
  polling loop.
```

- [ ] **Step 4: Create `messaging-contracts/pom.xml` and add the module**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.kafkademo</groupId>
        <artifactId>kafka-microservices-demo</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>messaging-contracts</artifactId>
    <name>messaging-contracts</name>

    <dependencies>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.datatype</groupId>
            <artifactId>jackson-datatype-jsr310</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

(The parent already lists `<module>messaging-contracts</module>`.)

- [ ] **Step 5: Write the failing test** `EnvelopeCodecTest.java`

```java
package com.kafkademo.contracts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.kafkademo.contracts.payload.ReserveStock;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EnvelopeCodecTest {

    private static final Instant NOW = Instant.parse("2026-10-11T10:00:00Z");

    private final ObjectMapper mapper = new ObjectMapper()
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final EnvelopeCodec codec = new EnvelopeCodec(mapper, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void wrapsAndReadsBackAPayload() {
        UUID orderId = UUID.randomUUID();
        MessageEnvelope sent = codec.wrap(MessageTypes.RESERVE_STOCK, orderId, new ReserveStock(1L, 2));

        MessageEnvelope received = codec.fromJson(codec.toJson(sent));

        assertThat(received.messageId()).isEqualTo(sent.messageId());
        assertThat(received.type()).isEqualTo("ReserveStock");
        assertThat(received.orderId()).isEqualTo(orderId);
        assertThat(received.occurredAt()).isEqualTo(NOW);
        assertThat(codec.payload(received, ReserveStock.class)).isEqualTo(new ReserveStock(1L, 2));
    }

    @Test
    void writesReadableJson() {
        String json = codec.toJson(codec.wrap(MessageTypes.RESERVE_STOCK, UUID.randomUUID(), new ReserveStock(7L, 3)));

        assertThat(json)
                .contains("\"type\":\"ReserveStock\"")
                .contains("\"occurredAt\":\"2026-10-11T10:00:00Z\"")
                .contains("\"payload\":{\"productId\":7,\"quantity\":3}");
    }

    @Test
    void everyEnvelopeGetsItsOwnMessageId() {
        UUID orderId = UUID.randomUUID();

        MessageEnvelope first = codec.wrap(MessageTypes.RESERVE_STOCK, orderId, new ReserveStock(1L, 1));
        MessageEnvelope second = codec.wrap(MessageTypes.RESERVE_STOCK, orderId, new ReserveStock(1L, 1));

        assertThat(first.messageId()).isNotEqualTo(second.messageId());
    }

    @Test
    void rejectsTextThatIsNotJson() {
        assertThatThrownBy(() -> codec.fromJson("not json"))
                .isInstanceOf(InvalidMessageException.class);
    }

    @Test
    void rejectsAnEnvelopeWithoutOrderId() {
        String json = "{\"messageId\":\"" + UUID.randomUUID() + "\",\"type\":\"ReserveStock\",\"payload\":{}}";

        assertThatThrownBy(() -> codec.fromJson(json))
                .isInstanceOf(InvalidMessageException.class)
                .hasMessageContaining("missing");
    }

    @Test
    void rejectsAPayloadOfTheWrongShape() {
        String json = "{\"messageId\":\"" + UUID.randomUUID() + "\",\"type\":\"ReserveStock\",\"orderId\":\""
                + UUID.randomUUID() + "\",\"payload\":{\"productId\":\"abc\"}}";
        MessageEnvelope envelope = codec.fromJson(json);

        assertThatThrownBy(() -> codec.payload(envelope, ReserveStock.class))
                .isInstanceOf(InvalidMessageException.class);
    }

    @Test
    void rejectsAnEnvelopeWithoutPayload() {
        String json = "{\"messageId\":\"" + UUID.randomUUID() + "\",\"type\":\"ReserveStock\",\"orderId\":\""
                + UUID.randomUUID() + "\"}";
        MessageEnvelope envelope = codec.fromJson(json);

        assertThatThrownBy(() -> codec.payload(envelope, ReserveStock.class))
                .isInstanceOf(InvalidMessageException.class)
                .hasMessageContaining("no payload");
    }
}
```

- [ ] **Step 6: Run the test to verify it fails**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl messaging-contracts test
```

Expected: compilation FAILS - `EnvelopeCodec`, `MessageEnvelope`, `MessageTypes`, `InvalidMessageException`, `ReserveStock` not found.

- [ ] **Step 7: Write the contracts**

`Topics.java`:
```java
package com.kafkademo.contracts;

/** Kafka topic names shared by all services. */
public final class Topics {

    /** order-service -> inventory-service: ReserveStock, ReleaseStock. */
    public static final String INVENTORY_COMMANDS = "inventory-commands";
    /** order-service -> payment-service: ChargePayment. */
    public static final String PAYMENT_COMMANDS = "payment-commands";
    /** inventory and payment -> order-service: the answers to the commands. */
    public static final String SAGA_REPLIES = "saga-replies";
    /** order-service -> anyone listening: OrderConfirmed, OrderCancelled. */
    public static final String ORDER_EVENTS = "order-events";

    /** Every topic has 3 partitions, so up to 3 consumers per group can work in parallel. */
    public static final int PARTITIONS = 3;

    private Topics() {
    }
}
```

`MessageTypes.java`:
```java
package com.kafkademo.contracts;

/** Values of {@link MessageEnvelope#type()}. Constants so they can be used in switch statements. */
public final class MessageTypes {

    public static final String RESERVE_STOCK = "ReserveStock";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_RESERVATION_FAILED = "StockReservationFailed";

    private MessageTypes() {
    }
}
```

`MessageEnvelope.java`:
```java
package com.kafkademo.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/**
 * The JSON shape of every Kafka message in this project.
 *
 * @param messageId unique per message - used later to spot duplicates
 * @param type      which message this is, e.g. "ReserveStock" (see {@link MessageTypes})
 * @param orderId   the order this message belongs to; also the Kafka key
 * @param occurredAt when the message was created
 * @param payload   the message body; read it with {@link EnvelopeCodec#payload}
 */
public record MessageEnvelope(UUID messageId, String type, UUID orderId, Instant occurredAt, JsonNode payload) {
}
```

`InvalidMessageException.java`:
```java
package com.kafkademo.contracts;

/** A Kafka message that can never be processed: not JSON, missing fields, or the wrong payload shape. */
public class InvalidMessageException extends RuntimeException {

    public InvalidMessageException(String message) {
        super(message);
    }

    public InvalidMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

`EnvelopeCodec.java`:
```java
package com.kafkademo.contracts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/** Turns payloads into the JSON envelope that travels on Kafka, and back. */
public class EnvelopeCodec {

    private final ObjectMapper mapper;
    private final Clock clock;

    public EnvelopeCodec(ObjectMapper mapper) {
        this(mapper, Clock.systemUTC());
    }

    public EnvelopeCodec(ObjectMapper mapper, Clock clock) {
        this.mapper = mapper;
        this.clock = clock;
    }

    /** Wraps a payload in a new envelope with a fresh messageId. */
    public MessageEnvelope wrap(String type, UUID orderId, Object payload) {
        JsonNode body = mapper.valueToTree(payload);
        return new MessageEnvelope(UUID.randomUUID(), type, orderId, Instant.now(clock), body);
    }

    public String toJson(MessageEnvelope envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not write envelope " + envelope.messageId(), e);
        }
    }

    /**
     * Reads an envelope.
     *
     * @throws InvalidMessageException if the text is not JSON or misses messageId, type or orderId
     */
    public MessageEnvelope fromJson(String json) {
        MessageEnvelope envelope;
        try {
            envelope = mapper.readValue(json, MessageEnvelope.class);
        } catch (JsonProcessingException e) {
            throw new InvalidMessageException("Not a valid message envelope: " + json, e);
        }
        if (envelope.messageId() == null || envelope.type() == null || envelope.orderId() == null) {
            throw new InvalidMessageException("Envelope is missing messageId, type or orderId: " + json);
        }
        return envelope;
    }

    /**
     * Reads the payload as the given record type.
     *
     * @throws InvalidMessageException if there is no payload or it does not match the type
     */
    public <T> T payload(MessageEnvelope envelope, Class<T> type) {
        if (envelope.payload() == null || envelope.payload().isNull()) {
            throw new InvalidMessageException("Message " + envelope.messageId() + " has no payload");
        }
        try {
            return mapper.treeToValue(envelope.payload(), type);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new InvalidMessageException(
                    "Payload does not match " + type.getSimpleName() + ": " + envelope.payload(), e);
        }
    }
}
```

`payload/ReserveStock.java`:
```java
package com.kafkademo.contracts.payload;

/** Command: reserve this many items of this product for the order in the envelope. */
public record ReserveStock(long productId, int quantity) {
}
```

`payload/StockReserved.java`:
```java
package com.kafkademo.contracts.payload;

/** Reply: the stock was reserved. */
public record StockReserved(long productId, int quantity) {
}
```

`payload/StockReservationFailed.java`:
```java
package com.kafkademo.contracts.payload;

/** Reply: the stock could not be reserved. {@code reason} is OUT_OF_STOCK or UNKNOWN_PRODUCT. */
public record StockReservationFailed(long productId, int quantity, String reason) {
}
```

- [ ] **Step 8: Run the test to verify it passes**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl messaging-contracts install
```

Expected: `BUILD SUCCESS`, 7 tests pass. (`install` puts the jar in the local Maven repo so the services can depend on it.)

- [ ] **Step 9: Checkpoint (no commit)**

```bash
git init -b main
git status --short
```

Expected: the new files are listed as untracked. Do **not** commit unless the owner asks.

---

### Task 2: Infrastructure with Docker Compose

**Files:**
- Create: `docker-compose.yml`
- Create: `docker/postgres/init.sql`
- Create: `scripts/start-infra.ps1`
- Modify: `docs/implementation-notes.md` (append disk numbers)

**Interfaces:**
- Produces: Kafka at `localhost:9092` (host) and `kafka:29092` (inside Docker); PostgreSQL at `localhost:5433`, user `demo` / password `demo`, databases `order_db`, `inventory_db`, `payment_db`; Redpanda Console at `http://localhost:8090`.

- [ ] **Step 1: Record Docker disk usage before**

```bash
docker system df
```

Expected: a table of image / container / volume sizes. Copy the numbers for Step 6.

- [ ] **Step 2: Create `docker-compose.yml`**

```yaml
name: kafka-demo

services:
  kafka:
    image: apache/kafka-native:4.3.1
    container_name: kafka-demo-kafka
    ports:
      - "9092:9092"
    environment:
      # One node is both broker and controller (KRaft mode - no ZooKeeper).
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # PLAINTEXT is for the services on the host, DOCKER is for containers (Redpanda Console).
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,DOCKER://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,DOCKER://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,DOCKER:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: DOCKER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      # Single broker: internal topics can only have 1 copy.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      # Topics must be created on purpose (NewTopic beans), never by accident.
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_NUM_PARTITIONS: 3

  console:
    image: redpandadata/console:v3.12.0
    container_name: kafka-demo-console
    ports:
      - "8090:8080"
    environment:
      KAFKA_BROKERS: kafka:29092
    depends_on:
      - kafka

  postgres:
    image: postgres:17.4
    container_name: kafka-demo-postgres
    ports:
      - "5433:5432"
    environment:
      POSTGRES_USER: demo
      POSTGRES_PASSWORD: demo
      POSTGRES_DB: postgres
    volumes:
      - ./docker/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U demo"]
      interval: 5s
      timeout: 3s
      retries: 20

volumes:
  pgdata:
```

- [ ] **Step 3: Create `docker/postgres/init.sql`**

```sql
-- One database per service ("database per service"), all in one PostgreSQL container.
CREATE DATABASE order_db;
CREATE DATABASE inventory_db;
CREATE DATABASE payment_db;
```

- [ ] **Step 4: Create `scripts/start-infra.ps1`**

```powershell
# Starts Kafka, Redpanda Console and PostgreSQL, and waits until they are ready.
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)

docker compose up -d kafka console postgres
if ($LASTEXITCODE -ne 0) { throw 'docker compose up failed - is Docker Desktop running?' }

Write-Host 'Waiting for PostgreSQL...'
$state = ''
for ($i = 0; $i -lt 60; $i++) {
    $state = docker inspect -f '{{.State.Health.Status}}' kafka-demo-postgres
    if ($state -eq 'healthy') { break }
    Start-Sleep -Seconds 2
}
if ($state -ne 'healthy') { throw 'PostgreSQL did not become healthy' }

# kafka-native has no shell tools for a Docker health check, so we wait for its port.
Write-Host 'Waiting for Kafka on localhost:9092...'
$open = $false
for ($i = 0; $i -lt 60; $i++) {
    $open = (Test-NetConnection -ComputerName localhost -Port 9092 -WarningAction SilentlyContinue).TcpTestSucceeded
    if ($open) { break }
    Start-Sleep -Seconds 2
}
if (-not $open) { throw 'Kafka did not open port 9092' }

Write-Host 'Infrastructure is up. Redpanda Console: http://localhost:8090'
```

- [ ] **Step 5: Start it and check every piece**

```bash
cd /d/project/kafka-microservices-demo
docker compose up -d kafka console postgres
sleep 15
docker compose ps
docker exec kafka-demo-postgres psql -U demo -d postgres -c "\l" | grep -E "order_db|inventory_db|payment_db"
curl -s -o /dev/null -w "console http %{http_code}\n" http://localhost:8090
docker logs kafka-demo-kafka 2>&1 | grep -iE "Kafka Server started|started \(kafka.server" | head -3
```

Expected: 3 containers `running` (postgres `healthy`); the 3 databases listed; `console http 200`; Kafka log shows the server started. If Kafka exits, run `docker logs kafka-demo-kafka` and fix the environment variables before going on.

- [ ] **Step 6: Record disk usage after, in `docs/implementation-notes.md`**

```bash
docker system df
```

Append under `## M1`:
```markdown
- **Disk cost of the M1 infrastructure** (`docker system df`): images before
  <copy the Images SIZE from Step 1>, after <copy the Images SIZE now>.
  New images: `apache/kafka-native:4.3.1`, `redpandadata/console:v3.12.0`;
  `postgres:17.4` reused.
```
(Replace the two `<...>` with the real numbers.)

- [ ] **Step 7: Checkpoint (no commit)** - `docker compose ps` shows all 3 running. Leave the containers running for the next tasks.

---

### Task 3: inventory-service - product catalog

**Files:**
- Modify: `pom.xml` (add `<module>inventory-service</module>`)
- Create: `inventory-service/pom.xml`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/InventoryApplication.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/product/Product.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/product/ProductRepository.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/product/ProductView.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/product/ProductController.java`
- Create: `inventory-service/src/main/resources/application.yml`
- Create: `inventory-service/src/main/resources/db/migration/V1__create_tables.sql`
- Create: `inventory-service/src/main/resources/db/migration/V2__seed_products.sql`
- Test: `inventory-service/src/test/java/com/kafkademo/inventory/product/ProductTest.java`
- Test: `inventory-service/src/test/java/com/kafkademo/inventory/IntegrationTestContainers.java`
- Test: `inventory-service/src/test/java/com/kafkademo/inventory/product/ProductControllerTest.java`

**Interfaces:**
- Produces: `GET /products/{id}` -> `200 {"id":1,"name":"Keyboard","price":50.00,"stock":100}`, `404` if missing (port 8082)
- Produces: `Product(Long id, String name, BigDecimal price, int stock)`, `boolean hasStock(int quantity)`, `void decreaseStock(int quantity)` (throws `IllegalStateException`), getters `getId()`, `getName()`, `getPrice()`, `getStock()`
- Produces: `interface ProductRepository extends JpaRepository<Product, Long>`
- Produces: test config `IntegrationTestContainers` (PostgreSQL bean; Task 4 adds Kafka)

- [ ] **Step 1: Write the failing unit test** `ProductTest.java`

```java
package com.kafkademo.inventory.product;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class ProductTest {

    private Product productWithStock(int stock) {
        return new Product(1L, "Keyboard", new BigDecimal("50.00"), stock);
    }

    @Test
    void hasStockWhenEnoughOrExactlyEnough() {
        assertThat(productWithStock(5).hasStock(4)).isTrue();
        assertThat(productWithStock(5).hasStock(5)).isTrue();
    }

    @Test
    void hasNoStockWhenTooFew() {
        assertThat(productWithStock(5).hasStock(6)).isFalse();
        assertThat(productWithStock(0).hasStock(1)).isFalse();
    }

    @Test
    void zeroOrNegativeQuantityIsNeverAvailable() {
        assertThat(productWithStock(5).hasStock(0)).isFalse();
        assertThat(productWithStock(5).hasStock(-1)).isFalse();
    }

    @Test
    void decreaseStockTakesItemsAway() {
        Product product = productWithStock(5);

        product.decreaseStock(5);

        assertThat(product.getStock()).isZero();
    }

    @Test
    void decreaseStockRefusesToGoBelowZero() {
        Product product = productWithStock(2);

        assertThatThrownBy(() -> product.decreaseStock(3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Not enough stock");
        assertThat(product.getStock()).isEqualTo(2);
    }
}
```

- [ ] **Step 2: Create `inventory-service/pom.xml` and register the module**

Add `<module>inventory-service</module>` after `messaging-contracts` in the parent `pom.xml` `<modules>`.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.kafkademo</groupId>
        <artifactId>kafka-microservices-demo</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>inventory-service</artifactId>
    <name>inventory-service</name>

    <dependencies>
        <dependency>
            <groupId>com.kafkademo</groupId>
            <artifactId>messaging-contracts</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 3: Run the unit test to verify it fails**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl inventory-service test -Dtest=ProductTest
```

Expected: compilation FAILS - `Product` not found.

- [ ] **Step 4: Write the application, entity, repository, view and controller**

`InventoryApplication.java`:
```java
package com.kafkademo.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class InventoryApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryApplication.class, args);
    }
}
```

`product/Product.java`:
```java
package com.kafkademo.inventory.product;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;

@Entity
@Table(name = "product")
public class Product {

    @Id
    private Long id;

    private String name;

    private BigDecimal price;

    private int stock;

    /** Optimistic locking: two transactions changing the same product at once - the second one fails. */
    @Version
    private long version;

    protected Product() {
        // for JPA
    }

    public Product(Long id, String name, BigDecimal price, int stock) {
        this.id = id;
        this.name = name;
        this.price = price;
        this.stock = stock;
    }

    public boolean hasStock(int quantity) {
        return quantity > 0 && stock >= quantity;
    }

    public void decreaseStock(int quantity) {
        if (!hasStock(quantity)) {
            throw new IllegalStateException(
                    "Not enough stock for product " + id + ": have " + stock + ", need " + quantity);
        }
        stock -= quantity;
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public int getStock() {
        return stock;
    }
}
```

`product/ProductRepository.java`:
```java
package com.kafkademo.inventory.product;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, Long> {
}
```

`product/ProductView.java`:
```java
package com.kafkademo.inventory.product;

import java.math.BigDecimal;

/** What GET /products/{id} returns. */
public record ProductView(long id, String name, BigDecimal price, int stock) {

    static ProductView from(Product product) {
        return new ProductView(product.getId(), product.getName(), product.getPrice(), product.getStock());
    }
}
```

`product/ProductController.java`:
```java
package com.kafkademo.inventory.product;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/products")
public class ProductController {

    private final ProductRepository products;

    public ProductController(ProductRepository products) {
        this.products = products;
    }

    @GetMapping("/{id}")
    public ProductView get(@PathVariable long id) {
        return products.findById(id)
                .map(ProductView::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Product " + id + " not found"));
    }
}
```

- [ ] **Step 5: Run the unit test to verify it passes**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl inventory-service test -Dtest=ProductTest
```

Expected: PASS, 5 tests.

- [ ] **Step 6: Write the migrations and `application.yml`**

`db/migration/V1__create_tables.sql`:
```sql
CREATE TABLE product (
    id      BIGINT        PRIMARY KEY,
    name    VARCHAR(100)  NOT NULL,
    price   NUMERIC(12,2) NOT NULL,
    stock   INT           NOT NULL CHECK (stock >= 0),
    version BIGINT        NOT NULL DEFAULT 0
);

CREATE TABLE stock_reservation (
    order_id   UUID        PRIMARY KEY,
    product_id BIGINT      NOT NULL REFERENCES product (id),
    quantity   INT         NOT NULL,
    status     VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
```

`db/migration/V2__seed_products.sql`:
```sql
-- Seed data from the spec (section 5).
INSERT INTO product (id, name, price, stock) VALUES
    (1, 'Keyboard',       50.00, 100),  -- happy path
    (2, 'Monitor',       300.00,   5),  -- run out of stock by ordering many
    (3, 'Laptop',       1500.00,  10),  -- always fails payment (M3)
    (4, 'Sold-out mug',   10.00,   0);  -- always out of stock
```

`application.yml`:
```yaml
server:
  port: 8082

spring:
  application:
    name: inventory-service
  datasource:
    url: jdbc:postgresql://localhost:5433/inventory_db
    username: demo
    password: demo
  jpa:
    open-in-view: false
    hibernate:
      ddl-auto: validate
```

- [ ] **Step 7: Write the integration test config and the failing controller test**

`IntegrationTestContainers.java` (test):
```java
package com.kafkademo.inventory;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** Real containers for integration tests. Same images as docker-compose.yml, so nothing new is downloaded. */
@TestConfiguration(proxyBeanMethods = false)
public class IntegrationTestContainers {

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgres() {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:17.4"));
    }
}
```

`product/ProductControllerTest.java`:
```java
package com.kafkademo.inventory.product;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.kafkademo.inventory.IntegrationTestContainers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@Import(IntegrationTestContainers.class)
class ProductControllerTest {

    @Autowired
    MockMvc mvc;

    @Test
    void returnsASeededProduct() throws Exception {
        mvc.perform(get("/products/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("Keyboard"))
                .andExpect(jsonPath("$.price").value(50.0));
    }

    @Test
    void unknownProductIs404() throws Exception {
        mvc.perform(get("/products/999999"))
                .andExpect(status().isNotFound());
    }
}
```

- [ ] **Step 8: Run all inventory tests**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl inventory-service test
```

Expected: PASS, 7 tests. This also proves Testcontainers works with Docker Engine 29. If it fails with `client version ... is too old` / HTTP 400, check that the Surefire `api.version` setting from the parent is applied.

- [ ] **Step 9: Run it against the compose database**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl inventory-service spring-boot:run &
sleep 25
curl -s http://localhost:8082/products/2; echo
curl -s -o /dev/null -w "missing product http %{http_code}\n" http://localhost:8082/products/77
kill %1
```

Expected: `{"id":2,"name":"Monitor","price":300.00,"stock":5}` and `missing product http 404`.

- [ ] **Step 10: Checkpoint (no commit)** - `./mvnw -q -pl inventory-service test` green.

---

### Task 4: inventory-service - reserve stock from Kafka

**Files:**
- Modify: `inventory-service/pom.xml` (add `spring-kafka`, test dependency `org.testcontainers:kafka`)
- Modify: `inventory-service/src/main/resources/application.yml` (Kafka settings)
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/config/AppConfig.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/config/KafkaTopicsConfig.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/reservation/ReservationStatus.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/reservation/StockReservation.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/reservation/StockReservationRepository.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/reservation/ReservationResult.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/reservation/StockService.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/messaging/ReplyPublisher.java`
- Create: `inventory-service/src/main/java/com/kafkademo/inventory/messaging/InventoryCommandListener.java`
- Modify: `inventory-service/src/test/java/com/kafkademo/inventory/IntegrationTestContainers.java` (add Kafka)
- Test: `inventory-service/src/test/java/com/kafkademo/inventory/messaging/InventoryCommandListenerTest.java`
- Test: `inventory-service/src/test/java/com/kafkademo/inventory/reservation/StockServiceConcurrencyTest.java`

**Interfaces:**
- Consumes: `Product.hasStock/decreaseStock`, `ProductRepository` (Task 3); `EnvelopeCodec`, `Topics`, `MessageTypes`, payload records (Task 1)
- Produces: `StockService.reserve(UUID orderId, long productId, int quantity) -> ReservationResult` (`@Transactional`)
- Produces: `record ReservationResult(boolean reserved, String reason)` - reason `OUT_OF_STOCK` or `UNKNOWN_PRODUCT`
- Produces: consumer group `inventory-service` on `inventory-commands`; replies on `saga-replies` keyed by `orderId`

- [ ] **Step 1: Add the dependencies**

In `inventory-service/pom.xml`, add to `<dependencies>` (after flyway):
```xml
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
```
and with the test dependencies:
```xml
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 2: Add Kafka to the test containers config**

Replace `IntegrationTestContainers.java` with:
```java
package com.kafkademo.inventory;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Real containers for integration tests. Same images as docker-compose.yml, so nothing new is downloaded. */
@TestConfiguration(proxyBeanMethods = false)
public class IntegrationTestContainers {

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgres() {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:17.4"));
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.3.1"));
    }
}
```

- [ ] **Step 3: Write the failing listener test** `messaging/InventoryCommandListenerTest.java`

```java
package com.kafkademo.inventory.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.contracts.payload.StockReservationFailed;
import com.kafkademo.contracts.payload.StockReserved;
import com.kafkademo.inventory.IntegrationTestContainers;
import com.kafkademo.inventory.product.Product;
import com.kafkademo.inventory.product.ProductRepository;
import com.kafkademo.inventory.reservation.ReservationStatus;
import com.kafkademo.inventory.reservation.StockReservationRepository;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

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

    /** Reads saga-replies from the beginning until a reply for this order shows up. */
    private MessageEnvelope awaitReply(UUID orderId) {
        try (Consumer<String, String> consumer = consumerFactory.createConsumer("test-" + UUID.randomUUID(), null)) {
            consumer.subscribe(List.of(Topics.SAGA_REPLIES));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (orderId.toString().equals(record.key())) {
                        return codec.fromJson(record.value());
                    }
                }
            }
        }
        throw new AssertionError("No reply on " + Topics.SAGA_REPLIES + " for order " + orderId);
    }
}
```

- [ ] **Step 4: Write the failing concurrency test** `reservation/StockServiceConcurrencyTest.java`

```java
package com.kafkademo.inventory.reservation;

import static org.assertj.core.api.Assertions.assertThat;

import com.kafkademo.inventory.IntegrationTestContainers;
import com.kafkademo.inventory.product.Product;
import com.kafkademo.inventory.product.ProductRepository;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class StockServiceConcurrencyTest {

    @Autowired
    StockService stockService;
    @Autowired
    ProductRepository products;
    @Autowired
    StockReservationRepository reservations;

    /** Two consumer instances may handle two orders for the same product at the same moment. */
    @Test
    void twoOrdersForTheLastItemNeverOversell() throws Exception {
        long productId = ThreadLocalRandom.current().nextLong(10_000, 1_000_000_000L);
        products.save(new Product(productId, "Last one", new BigDecimal("9.99"), 1));

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    try {
                        return stockService.reserve(UUID.randomUUID(), productId, 1).reserved();
                    } catch (ObjectOptimisticLockingFailureException e) {
                        // The losing transaction. In the real flow the Kafka error handler retries it,
                        // and the retry then sees no stock left.
                        return false;
                    }
                }));
            }
            start.countDown();

            long successes = 0;
            for (Future<Boolean> result : results) {
                if (result.get(30, TimeUnit.SECONDS)) {
                    successes++;
                }
            }

            assertThat(successes).isEqualTo(1);
            assertThat(products.findById(productId).orElseThrow().getStock()).isZero();
            assertThat(reservations.findAll()).filteredOn(r -> r.getProductId() == productId).hasSize(1);
        } finally {
            pool.shutdownNow();
        }
    }
}
```

- [ ] **Step 5: Run the tests to verify they fail**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl inventory-service test
```

Expected: compilation FAILS - `StockService`, `StockReservationRepository`, `ReservationStatus` not found.

- [ ] **Step 6: Add the Kafka settings to `application.yml`**

Append under `spring:` (same indentation as `jpa:`):
```yaml
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all                       # the broker confirms only when the write is safe
      properties:
        enable.idempotence: true      # retries by the producer never create duplicates in Kafka
    consumer:
      group-id: inventory-service     # all instances of this service share the work
      auto-offset-reset: earliest     # a new group starts from the oldest message
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    listener:
      ack-mode: record                # commit the offset after each message is processed
```
and at the end of the file:
```yaml
logging:
  level:
    org.apache.kafka: WARN
```

- [ ] **Step 7: Write the configuration classes**

`config/AppConfig.java`:
```java
package com.kafkademo.inventory.config;

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
```

`config/KafkaTopicsConfig.java`:
```java
package com.kafkademo.inventory.config;

import com.kafkademo.contracts.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Creates the topics this service reads and writes, if they don't exist yet (broker auto-create is off). */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicsConfig {

    @Bean
    NewTopic inventoryCommandsTopic() {
        return TopicBuilder.name(Topics.INVENTORY_COMMANDS).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic sagaRepliesTopic() {
        return TopicBuilder.name(Topics.SAGA_REPLIES).partitions(Topics.PARTITIONS).replicas(1).build();
    }
}
```

- [ ] **Step 8: Write the reservation classes**

`reservation/ReservationStatus.java`:
```java
package com.kafkademo.inventory.reservation;

public enum ReservationStatus {
    RESERVED,
    RELEASED
}
```

`reservation/StockReservation.java`:
```java
package com.kafkademo.inventory.reservation;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/** Stock held for one order. One reservation per order. */
@Entity
@Table(name = "stock_reservation")
public class StockReservation {

    @Id
    private UUID orderId;

    private long productId;

    private int quantity;

    @Enumerated(EnumType.STRING)
    private ReservationStatus status;

    private Instant createdAt;

    protected StockReservation() {
        // for JPA
    }

    public static StockReservation reserved(UUID orderId, long productId, int quantity, Instant now) {
        StockReservation reservation = new StockReservation();
        reservation.orderId = orderId;
        reservation.productId = productId;
        reservation.quantity = quantity;
        reservation.status = ReservationStatus.RESERVED;
        reservation.createdAt = now;
        return reservation;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public long getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
```

`reservation/StockReservationRepository.java`:
```java
package com.kafkademo.inventory.reservation;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StockReservationRepository extends JpaRepository<StockReservation, UUID> {
}
```

`reservation/ReservationResult.java`:
```java
package com.kafkademo.inventory.reservation;

/** The answer to "reserve this stock": either reserved, or failed with a reason. */
public record ReservationResult(boolean reserved, String reason) {

    public static final String OUT_OF_STOCK = "OUT_OF_STOCK";
    public static final String UNKNOWN_PRODUCT = "UNKNOWN_PRODUCT";

    static ReservationResult success() {
        return new ReservationResult(true, null);
    }

    static ReservationResult failed(String reason) {
        return new ReservationResult(false, reason);
    }
}
```

`reservation/StockService.java`:
```java
package com.kafkademo.inventory.reservation;

import com.kafkademo.inventory.product.Product;
import com.kafkademo.inventory.product.ProductRepository;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class StockService {

    private static final Logger log = LoggerFactory.getLogger(StockService.class);

    private final ProductRepository products;
    private final StockReservationRepository reservations;
    private final Clock clock;

    public StockService(ProductRepository products, StockReservationRepository reservations, Clock clock) {
        this.products = products;
        this.reservations = reservations;
        this.clock = clock;
    }

    /**
     * Takes the items out of stock and records the reservation, in one transaction.
     * M1 has no duplicate protection: the same command twice reserves twice (fixed in M2).
     */
    @Transactional
    public ReservationResult reserve(UUID orderId, long productId, int quantity) {
        Optional<Product> found = products.findById(productId);
        if (found.isEmpty()) {
            log.info("Order {}: product {} does not exist", orderId, productId);
            return ReservationResult.failed(ReservationResult.UNKNOWN_PRODUCT);
        }
        Product product = found.get();
        if (!product.hasStock(quantity)) {
            log.info("Order {}: only {} of product {} left, {} wanted", orderId, product.getStock(), productId, quantity);
            return ReservationResult.failed(ReservationResult.OUT_OF_STOCK);
        }
        product.decreaseStock(quantity);
        reservations.save(StockReservation.reserved(orderId, productId, quantity, clock.instant()));
        log.info("Order {}: reserved {} of product {}, {} left", orderId, quantity, productId, product.getStock());
        return ReservationResult.success();
    }
}
```

- [ ] **Step 9: Write the publisher and the listener**

`messaging/ReplyPublisher.java`:
```java
package com.kafkademo.inventory.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.Topics;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Sends replies to order-service on saga-replies. The key is the orderId, so replies stay in order per order. */
@Component
public class ReplyPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final EnvelopeCodec codec;

    public ReplyPublisher(KafkaTemplate<String, String> kafka, EnvelopeCodec codec) {
        this.kafka = kafka;
        this.codec = codec;
    }

    public void send(UUID orderId, String type, Object payload) {
        MessageEnvelope envelope = codec.wrap(type, orderId, payload);
        try {
            // .get(...) waits for the broker to confirm the write (acks=all).
            kafka.send(Topics.SAGA_REPLIES, orderId.toString(), codec.toJson(envelope)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending " + type + " for order " + orderId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Could not send " + type + " for order " + orderId, e);
        }
    }
}
```

`messaging/InventoryCommandListener.java`:
```java
package com.kafkademo.inventory.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.contracts.payload.StockReservationFailed;
import com.kafkademo.contracts.payload.StockReserved;
import com.kafkademo.inventory.reservation.ReservationResult;
import com.kafkademo.inventory.reservation.StockService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);

    private final EnvelopeCodec codec;
    private final StockService stockService;
    private final ReplyPublisher replies;

    public InventoryCommandListener(EnvelopeCodec codec, StockService stockService, ReplyPublisher replies) {
        this.codec = codec;
        this.stockService = stockService;
        this.replies = replies;
    }

    @KafkaListener(topics = Topics.INVENTORY_COMMANDS)
    public void onCommand(ConsumerRecord<String, String> record) {
        // partition + offset in the log make experiments E5 and E7 visible.
        log.info("Command received: partition={} offset={} key={}", record.partition(), record.offset(), record.key());
        MessageEnvelope envelope = codec.fromJson(record.value());
        switch (envelope.type()) {
            case MessageTypes.RESERVE_STOCK -> handleReserveStock(envelope);
            default -> log.warn("Ignoring unknown command type '{}' (messageId={})", envelope.type(), envelope.messageId());
        }
    }

    private void handleReserveStock(MessageEnvelope envelope) {
        ReserveStock command = codec.payload(envelope, ReserveStock.class);
        ReservationResult result = stockService.reserve(envelope.orderId(), command.productId(), command.quantity());
        // M1: the reply is sent AFTER the database commit, not together with it.
        // If the app crashes in between, the stock is reserved but no reply is ever sent. M2 fixes this (outbox).
        if (result.reserved()) {
            replies.send(envelope.orderId(), MessageTypes.STOCK_RESERVED,
                    new StockReserved(command.productId(), command.quantity()));
        } else {
            replies.send(envelope.orderId(), MessageTypes.STOCK_RESERVATION_FAILED,
                    new StockReservationFailed(command.productId(), command.quantity(), result.reason()));
        }
    }
}
```

- [ ] **Step 10: Run all inventory tests**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl inventory-service test
```

Expected: PASS, 13 tests. This proves `apache/kafka-native` works with Testcontainers (spec risk, section 15).

If the context fails with **"no ConnectionDetails found for KafkaContainer"** (Spring Boot cannot auto-connect this container type), the container itself is fine: remove `@ServiceConnection` from the `kafka()` bean and add this bean to `IntegrationTestContainers` instead (`org.springframework.test.context.DynamicPropertyRegistrar`):

```java
    @Bean
    DynamicPropertyRegistrar kafkaProperties(KafkaContainer kafka) {
        return registry -> registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }
```

Then apply the same change to order-service's `IntegrationTestContainers` in Task 5, and note it in `docs/implementation-notes.md`.

**If the Kafka container itself fails to start** in tests, do not switch images on your own: write the error into `docs/implementation-notes.md` and ask the owner whether to use `apache/kafka:4.3.1` (+ ~239 MB) for tests.

- [ ] **Step 11: Checkpoint (no commit)** - inventory tests green; add a line to `docs/implementation-notes.md` under `## M1`:
```markdown
- **`apache/kafka-native:4.3.1` works with Testcontainers' `KafkaContainer`**
  and Spring Boot's `@ServiceConnection` (checked in Task 4). Tests reuse the
  same images as Docker Compose.
```

---

### Task 5: order-service - place an order

**Files:**
- Modify: `pom.xml` (add `<module>order-service</module>`)
- Create: `order-service/pom.xml`
- Create: `order-service/src/main/java/com/kafkademo/order/OrderApplication.java`
- Create: `order-service/src/main/java/com/kafkademo/order/config/AppConfig.java`
- Create: `order-service/src/main/java/com/kafkademo/order/config/KafkaTopicsConfig.java`
- Create: `order-service/src/main/java/com/kafkademo/order/order/OrderStatus.java`
- Create: `order-service/src/main/java/com/kafkademo/order/order/CancelReason.java`
- Create: `order-service/src/main/java/com/kafkademo/order/order/Order.java`
- Create: `order-service/src/main/java/com/kafkademo/order/order/OrderRepository.java`
- Create: `order-service/src/main/java/com/kafkademo/order/order/OrderService.java`
- Create: `order-service/src/main/java/com/kafkademo/order/web/PlaceOrderRequest.java`
- Create: `order-service/src/main/java/com/kafkademo/order/web/OrderView.java`
- Create: `order-service/src/main/java/com/kafkademo/order/web/OrderController.java`
- Create: `order-service/src/main/java/com/kafkademo/order/web/ApiExceptionHandler.java`
- Create: `order-service/src/main/java/com/kafkademo/order/product/ProductInfo.java`
- Create: `order-service/src/main/java/com/kafkademo/order/product/ProductNotFoundException.java`
- Create: `order-service/src/main/java/com/kafkademo/order/product/ProductClient.java`
- Create: `order-service/src/main/java/com/kafkademo/order/messaging/CommandPublisher.java`
- Create: `order-service/src/main/resources/application.yml`
- Create: `order-service/src/main/resources/db/migration/V1__create_orders.sql`
- Test: `order-service/src/test/java/com/kafkademo/order/order/OrderTest.java`
- Test: `order-service/src/test/java/com/kafkademo/order/product/ProductClientTest.java`
- Test: `order-service/src/test/java/com/kafkademo/order/IntegrationTestContainers.java`
- Test: `order-service/src/test/java/com/kafkademo/order/web/OrderControllerTest.java`

**Interfaces:**
- Consumes: `GET {inventory.base-url}/products/{id}` (Task 3); contracts (Task 1)
- Produces: `POST /orders {"productId":1,"quantity":2}` -> `202 {"id":"...","productId":1,"quantity":2,"unitPrice":50.00,"total":100.00,"status":"PENDING","cancelReason":null}`; `GET /orders/{id}` -> `200` same shape, `404` if missing (port 8081)
- Produces: `Order.place(UUID id, long productId, int quantity, BigDecimal unitPrice, Instant now)`, `boolean markStockReserved(Instant now)`, `boolean cancel(CancelReason reason, Instant now)`, getters `getId()`, `getProductId()`, `getQuantity()`, `getUnitPrice()`, `getTotal()`, `getStatus()`, `getCancelReason()`
- Produces: `enum OrderStatus { PENDING, STOCK_RESERVED, CONFIRMED, CANCELLED }`, `enum CancelReason { OUT_OF_STOCK, PAYMENT_FAILED }`
- Produces: `OrderService.placeOrder(long productId, int quantity) -> Order`, `OrderService.find(UUID id) -> Optional<Order>`
- Produces: `interface OrderRepository extends JpaRepository<Order, UUID>`
- Produces: `ProductClient.getProduct(long productId) -> ProductInfo` (throws `ProductNotFoundException`), `record ProductInfo(long id, String name, BigDecimal price, int stock)`
- Produces: `CommandPublisher.reserveStock(UUID orderId, long productId, int quantity)` -> `inventory-commands`, key `orderId`

- [ ] **Step 1: Write the failing unit test** `order/OrderTest.java`

```java
package com.kafkademo.order.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class OrderTest {

    private static final Instant NOW = Instant.parse("2026-10-11T10:00:00Z");

    private Order newOrder() {
        return Order.place(UUID.randomUUID(), 1L, 3, new BigDecimal("50.00"), NOW);
    }

    @Test
    void aNewOrderIsPendingWithTheTotalCalculated() {
        Order order = newOrder();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(order.getTotal()).isEqualByComparingTo("150.00");
        assertThat(order.getCancelReason()).isNull();
    }

    @Test
    void stockReservedMovesPendingToStockReserved() {
        Order order = newOrder();

        assertThat(order.markStockReserved(NOW)).isTrue();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.STOCK_RESERVED);
    }

    @Test
    void stockReservedTwiceIsIgnoredTheSecondTime() {
        Order order = newOrder();
        order.markStockReserved(NOW);

        assertThat(order.markStockReserved(NOW)).isFalse();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.STOCK_RESERVED);
    }

    @Test
    void cancelSetsTheReason() {
        Order order = newOrder();

        assertThat(order.cancel(CancelReason.OUT_OF_STOCK, NOW)).isTrue();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancelReason()).isEqualTo(CancelReason.OUT_OF_STOCK);
    }

    @Test
    void aCancelledOrderCannotBeReservedOrCancelledAgain() {
        Order order = newOrder();
        order.cancel(CancelReason.OUT_OF_STOCK, NOW);

        assertThat(order.markStockReserved(NOW)).isFalse();
        assertThat(order.cancel(CancelReason.PAYMENT_FAILED, NOW)).isFalse();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancelReason()).isEqualTo(CancelReason.OUT_OF_STOCK);
    }
}
```

- [ ] **Step 2: Write the failing unit test** `product/ProductClientTest.java`

```java
package com.kafkademo.order.product;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class ProductClientTest {

    @Test
    void readsAProduct() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo("http://inventory/products/1"))
                .andRespond(withSuccess("{\"id\":1,\"name\":\"Keyboard\",\"price\":50.00,\"stock\":100}",
                        MediaType.APPLICATION_JSON));
        ProductClient client = new ProductClient(builder, "http://inventory");

        assertThat(client.getProduct(1L)).isEqualTo(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));
        server.verify();
    }

    @Test
    void a404BecomesProductNotFound() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        server.expect(requestTo("http://inventory/products/99")).andRespond(withStatus(HttpStatus.NOT_FOUND));
        ProductClient client = new ProductClient(builder, "http://inventory");

        assertThatThrownBy(() -> client.getProduct(99L))
                .isInstanceOf(ProductNotFoundException.class)
                .hasMessage("Product 99 not found");
    }
}
```

- [ ] **Step 3: Create `order-service/pom.xml` and register the module**

Add `<module>order-service</module>` after `inventory-service` in the parent `<modules>`.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.kafkademo</groupId>
        <artifactId>kafka-microservices-demo</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>order-service</artifactId>
    <name>order-service</name>

    <dependencies>
        <dependency>
            <groupId>com.kafkademo</groupId>
            <artifactId>messaging-contracts</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-validation</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.flywaydb</groupId>
            <artifactId>flyway-database-postgresql</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 4: Run the unit tests to verify they fail**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl order-service test -Dtest='OrderTest,ProductClientTest'
```

Expected: compilation FAILS - `Order`, `ProductClient` not found.

- [ ] **Step 5: Write the domain classes**

`OrderApplication.java`:
```java
package com.kafkademo.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
```

`order/OrderStatus.java`:
```java
package com.kafkademo.order.order;

public enum OrderStatus {
    PENDING,
    STOCK_RESERVED,
    CONFIRMED,
    CANCELLED
}
```

`order/CancelReason.java`:
```java
package com.kafkademo.order.order;

public enum CancelReason {
    OUT_OF_STOCK,
    PAYMENT_FAILED
}
```

`order/Order.java`:
```java
package com.kafkademo.order.order;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * An order and its saga state. The state rules live here:
 * PENDING -> STOCK_RESERVED -> CONFIRMED, or -> CANCELLED with a reason.
 */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;

    private long productId;

    private int quantity;

    private BigDecimal unitPrice;

    private BigDecimal total;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    @Enumerated(EnumType.STRING)
    private CancelReason cancelReason;

    private Instant createdAt;

    private Instant updatedAt;

    @Version
    private long version;

    protected Order() {
        // for JPA
    }

    public static Order place(UUID id, long productId, int quantity, BigDecimal unitPrice, Instant now) {
        Order order = new Order();
        order.id = id;
        order.productId = productId;
        order.quantity = quantity;
        order.unitPrice = unitPrice;
        order.total = unitPrice.multiply(BigDecimal.valueOf(quantity));
        order.status = OrderStatus.PENDING;
        order.createdAt = now;
        order.updatedAt = now;
        return order;
    }

    /** @return false if the order is not PENDING - the reply is late or a duplicate and is ignored */
    public boolean markStockReserved(Instant now) {
        if (status != OrderStatus.PENDING) {
            return false;
        }
        status = OrderStatus.STOCK_RESERVED;
        updatedAt = now;
        return true;
    }

    /** @return false if the order is already CONFIRMED or CANCELLED */
    public boolean cancel(CancelReason reason, Instant now) {
        if (status != OrderStatus.PENDING && status != OrderStatus.STOCK_RESERVED) {
            return false;
        }
        status = OrderStatus.CANCELLED;
        cancelReason = reason;
        updatedAt = now;
        return true;
    }

    public UUID getId() {
        return id;
    }

    public long getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public BigDecimal getTotal() {
        return total;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public CancelReason getCancelReason() {
        return cancelReason;
    }
}
```

`order/OrderRepository.java`:
```java
package com.kafkademo.order.order;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, UUID> {
}
```

`product/ProductInfo.java`:
```java
package com.kafkademo.order.product;

import java.math.BigDecimal;

/** What inventory-service returns for GET /products/{id}. */
public record ProductInfo(long id, String name, BigDecimal price, int stock) {
}
```

`product/ProductNotFoundException.java`:
```java
package com.kafkademo.order.product;

public class ProductNotFoundException extends RuntimeException {

    public ProductNotFoundException(long productId) {
        super("Product " + productId + " not found");
    }
}
```

`product/ProductClient.java`:
```java
package com.kafkademo.order.product;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** Synchronous REST call to inventory-service for the product price. M4 adds a circuit breaker here. */
@Component
public class ProductClient {

    private final RestClient restClient;

    public ProductClient(RestClient.Builder builder, @Value("${inventory.base-url}") String baseUrl) {
        this.restClient = builder.baseUrl(baseUrl).build();
    }

    public ProductInfo getProduct(long productId) {
        return restClient.get()
                .uri("/products/{id}", productId)
                .retrieve()
                .onStatus(status -> status.isSameCodeAs(HttpStatus.NOT_FOUND), (request, response) -> {
                    throw new ProductNotFoundException(productId);
                })
                .body(ProductInfo.class);
    }
}
```

- [ ] **Step 6: Run the unit tests to verify they pass**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl order-service test -Dtest='OrderTest,ProductClientTest'
```

Expected: PASS, 7 tests.

- [ ] **Step 7: Write the migration, `application.yml` and the test containers config**

`db/migration/V1__create_orders.sql`:
```sql
CREATE TABLE orders (
    id            UUID          PRIMARY KEY,
    product_id    BIGINT        NOT NULL,
    quantity      INT           NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12,2) NOT NULL,
    total         NUMERIC(12,2) NOT NULL,
    status        VARCHAR(30)   NOT NULL,
    cancel_reason VARCHAR(30),
    created_at    TIMESTAMPTZ   NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL,
    version       BIGINT        NOT NULL DEFAULT 0
);
```

`application.yml`:
```yaml
server:
  port: 8081

spring:
  application:
    name: order-service
  datasource:
    url: jdbc:postgresql://localhost:5433/order_db
    username: demo
    password: demo
  jpa:
    open-in-view: false
    hibernate:
      ddl-auto: validate
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      properties:
        enable.idempotence: true
    consumer:
      group-id: order-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    listener:
      ack-mode: record

inventory:
  base-url: http://localhost:8082

logging:
  level:
    org.apache.kafka: WARN
```

`IntegrationTestContainers.java` (test, package `com.kafkademo.order`):
```java
package com.kafkademo.order;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Real containers for integration tests. Same images as docker-compose.yml, so nothing new is downloaded. */
@TestConfiguration(proxyBeanMethods = false)
public class IntegrationTestContainers {

    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgres() {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:17.4"));
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafka() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:4.3.1"));
    }
}
```

- [ ] **Step 8: Write the failing controller test** `web/OrderControllerTest.java`

```java
package com.kafkademo.order.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.order.OrderRepository;
import com.kafkademo.order.product.ProductClient;
import com.kafkademo.order.product.ProductInfo;
import com.kafkademo.order.product.ProductNotFoundException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest
@AutoConfigureMockMvc
@Import(IntegrationTestContainers.class)
class OrderControllerTest {

    @Autowired
    MockMvc mvc;
    @Autowired
    OrderRepository orders;
    @Autowired
    EnvelopeCodec codec;
    @Autowired
    ConsumerFactory<String, String> consumerFactory;

    @MockitoBean
    ProductClient productClient;

    @Test
    void placingAnOrderSavesItPendingAndSendsReserveStock() throws Exception {
        when(productClient.getProduct(1L)).thenReturn(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));

        UUID orderId = placeOrder("{\"productId\":1,\"quantity\":2}");

        assertThat(orders.findById(orderId)).isPresent();
        MessageEnvelope command = awaitRecord(Topics.INVENTORY_COMMANDS, orderId);
        assertThat(command.type()).isEqualTo(MessageTypes.RESERVE_STOCK);
        assertThat(codec.payload(command, ReserveStock.class)).isEqualTo(new ReserveStock(1L, 2));
    }

    @Test
    void getReturnsThePlacedOrder() throws Exception {
        when(productClient.getProduct(1L)).thenReturn(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));
        UUID orderId = placeOrder("{\"productId\":1,\"quantity\":3}");

        mvc.perform(get("/orders/{id}", orderId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.total").value(150.0));
    }

    @Test
    void anUnknownOrderIs404() throws Exception {
        mvc.perform(get("/orders/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"productId\":1,\"quantity\":0}",
            "{\"productId\":1,\"quantity\":-3}",
            "{\"quantity\":2}",
            "{\"productId\":0,\"quantity\":1}"
    })
    void invalidRequestsAre400AndSaveNothing(String body) throws Exception {
        long before = orders.count();

        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());

        assertThat(orders.count()).isEqualTo(before);
        verifyNoInteractions(productClient);
    }

    @Test
    void anUnknownProductIs400AndSavesNothing() throws Exception {
        when(productClient.getProduct(99L)).thenThrow(new ProductNotFoundException(99L));
        long before = orders.count();

        mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content("{\"productId\":99,\"quantity\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Product 99 not found"));

        assertThat(orders.count()).isEqualTo(before);
    }

    private UUID placeOrder(String body) throws Exception {
        MvcResult result = mvc.perform(post("/orders").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn();
        String id = JsonPath.read(result.getResponse().getContentAsString(), "$.id");
        return UUID.fromString(id);
    }

    /** Reads a topic from the beginning until a record with this order's key shows up. */
    private MessageEnvelope awaitRecord(String topic, UUID orderId) {
        try (Consumer<String, String> consumer = consumerFactory.createConsumer("test-" + UUID.randomUUID(), null)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (orderId.toString().equals(record.key())) {
                        return codec.fromJson(record.value());
                    }
                }
            }
        }
        throw new AssertionError("No record on " + topic + " for order " + orderId);
    }
}
```

- [ ] **Step 9: Run it to verify it fails**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl order-service test -Dtest=OrderControllerTest
```

Expected: compilation FAILS - `EnvelopeCodec` bean / `OrderService` / web classes missing.

- [ ] **Step 10: Write config, service, publisher and web layer**

`config/AppConfig.java`:
```java
package com.kafkademo.order.config;

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
```

`config/KafkaTopicsConfig.java`:
```java
package com.kafkademo.order.config;

import com.kafkademo.contracts.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Creates the topics this service reads and writes, if they don't exist yet (broker auto-create is off). */
@Configuration(proxyBeanMethods = false)
public class KafkaTopicsConfig {

    @Bean
    NewTopic inventoryCommandsTopic() {
        return TopicBuilder.name(Topics.INVENTORY_COMMANDS).partitions(Topics.PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic sagaRepliesTopic() {
        return TopicBuilder.name(Topics.SAGA_REPLIES).partitions(Topics.PARTITIONS).replicas(1).build();
    }
}
```

`messaging/CommandPublisher.java`:
```java
package com.kafkademo.order.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.contracts.payload.ReserveStock;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Sends saga commands. The key is always the orderId, so every message of one order lands in one partition. */
@Component
public class CommandPublisher {

    private final KafkaTemplate<String, String> kafka;
    private final EnvelopeCodec codec;

    public CommandPublisher(KafkaTemplate<String, String> kafka, EnvelopeCodec codec) {
        this.kafka = kafka;
        this.codec = codec;
    }

    public void reserveStock(UUID orderId, long productId, int quantity) {
        send(Topics.INVENTORY_COMMANDS, orderId, MessageTypes.RESERVE_STOCK, new ReserveStock(productId, quantity));
    }

    private void send(String topic, UUID orderId, String type, Object payload) {
        MessageEnvelope envelope = codec.wrap(type, orderId, payload);
        try {
            kafka.send(topic, orderId.toString(), codec.toJson(envelope)).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while sending " + type + " for order " + orderId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Could not send " + type + " for order " + orderId, e);
        }
    }
}
```

`order/OrderService.java`:
```java
package com.kafkademo.order.order;

import com.kafkademo.order.messaging.CommandPublisher;
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
    private final CommandPublisher commands;
    private final Clock clock;

    public OrderService(ProductClient productClient, OrderRepository orders, TransactionTemplate transactionTemplate,
                        CommandPublisher commands, Clock clock) {
        this.productClient = productClient;
        this.orders = orders;
        this.transactionTemplate = transactionTemplate;
        this.commands = commands;
        this.clock = clock;
    }

    /**
     * 1. Ask inventory for the price - OUTSIDE any DB transaction (never hold a DB connection during an HTTP call).
     * 2. Save the order as PENDING - an explicit transaction.
     * 3. Send ReserveStock.
     */
    public Order placeOrder(long productId, int quantity) {
        ProductInfo product = productClient.getProduct(productId);
        Order order = Order.place(UUID.randomUUID(), productId, quantity, product.price(), clock.instant());

        Order saved = transactionTemplate.execute(status -> orders.save(order));

        // M1: sent AFTER the commit, not together with it - the "dual write" problem.
        // If the app crashes right here, the order stays PENDING forever. M2 fixes this with the outbox.
        commands.reserveStock(saved.getId(), productId, quantity);
        log.info("Order {} placed: {} x product {}, total {}", saved.getId(), quantity, productId, saved.getTotal());
        return saved;
    }

    @Transactional(readOnly = true)
    public Optional<Order> find(UUID id) {
        return orders.findById(id);
    }
}
```

`web/PlaceOrderRequest.java`:
```java
package com.kafkademo.order.web;

import jakarta.validation.constraints.Positive;

/** Body of POST /orders. A missing field arrives as 0 and fails @Positive. */
public record PlaceOrderRequest(@Positive long productId, @Positive int quantity) {
}
```

`web/OrderView.java`:
```java
package com.kafkademo.order.web;

import com.kafkademo.order.order.CancelReason;
import com.kafkademo.order.order.Order;
import com.kafkademo.order.order.OrderStatus;
import java.math.BigDecimal;
import java.util.UUID;

/** What the order endpoints return. */
public record OrderView(UUID id, long productId, int quantity, BigDecimal unitPrice, BigDecimal total,
                        OrderStatus status, CancelReason cancelReason) {

    static OrderView from(Order order) {
        return new OrderView(order.getId(), order.getProductId(), order.getQuantity(), order.getUnitPrice(),
                order.getTotal(), order.getStatus(), order.getCancelReason());
    }
}
```

`web/OrderController.java`:
```java
package com.kafkademo.order.web;

import com.kafkademo.order.order.Order;
import com.kafkademo.order.order.OrderService;
import jakarta.validation.Valid;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /** 202 Accepted: the order is saved, but the saga runs in the background. Check GET /orders/{id} later. */
    @PostMapping
    public ResponseEntity<OrderView> place(@Valid @RequestBody PlaceOrderRequest request) {
        Order order = orderService.placeOrder(request.productId(), request.quantity());
        return ResponseEntity.accepted().body(OrderView.from(order));
    }

    @GetMapping("/{id}")
    public OrderView get(@PathVariable UUID id) {
        return orderService.find(id)
                .map(OrderView::from)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order " + id + " not found"));
    }
}
```

`web/ApiExceptionHandler.java`:
```java
package com.kafkademo.order.web;

import com.kafkademo.order.product.ProductNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    /** Ordering a product that doesn't exist is a bad request, not a server error. */
    @ExceptionHandler(ProductNotFoundException.class)
    ProblemDetail productNotFound(ProductNotFoundException e) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
    }
}
```

- [ ] **Step 11: Run all order tests**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl order-service test
```

Expected: PASS, 15 tests (5 `OrderTest` + 2 `ProductClientTest` + 8 `OrderControllerTest`, counting the 4 parameterized cases).

- [ ] **Step 12: Checkpoint (no commit)** - `./mvnw -q -pl order-service test` green.

---

### Task 6: order-service - handle the replies

**Files:**
- Create: `order-service/src/main/java/com/kafkademo/order/order/OrderSagaService.java`
- Create: `order-service/src/main/java/com/kafkademo/order/messaging/SagaReplyListener.java`
- Test: `order-service/src/test/java/com/kafkademo/order/order/OrderSagaServiceTest.java`
- Test: `order-service/src/test/java/com/kafkademo/order/messaging/SagaReplyListenerTest.java`

**Interfaces:**
- Consumes: `Order.markStockReserved/cancel`, `OrderRepository`, `OrderService.placeOrder` (Task 5); `MessageTypes.STOCK_RESERVED`, `MessageTypes.STOCK_RESERVATION_FAILED` (Task 1)
- Produces: `OrderSagaService.onStockReserved(UUID orderId)`, `OrderSagaService.onStockReservationFailed(UUID orderId)` (both `@Transactional`; M3 extends this class)
- Produces: consumer group `order-service` on `saga-replies`

- [ ] **Step 1: Write the failing service test** `order/OrderSagaServiceTest.java`

```java
package com.kafkademo.order.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.kafkademo.order.IntegrationTestContainers;
import com.kafkademo.order.product.ProductClient;
import com.kafkademo.order.product.ProductInfo;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class OrderSagaServiceTest {

    @Autowired
    OrderSagaService saga;
    @Autowired
    OrderService orderService;
    @Autowired
    OrderRepository orders;

    @MockitoBean
    ProductClient productClient;

    @BeforeEach
    void stubPrice() {
        when(productClient.getProduct(anyLong())).thenReturn(new ProductInfo(1L, "Keyboard", new BigDecimal("50.00"), 100));
    }

    @Test
    void stockReservedMovesTheOrderToStockReserved() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();

        saga.onStockReserved(orderId);

        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.STOCK_RESERVED);
    }

    @Test
    void stockReservationFailedCancelsTheOrder() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();

        saga.onStockReservationFailed(orderId);

        Order order = orders.findById(orderId).orElseThrow();
        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancelReason()).isEqualTo(CancelReason.OUT_OF_STOCK);
    }

    @Test
    void aLateStockReservedDoesNotReopenACancelledOrder() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        saga.onStockReservationFailed(orderId);

        saga.onStockReserved(orderId);

        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.CANCELLED);
    }

    @Test
    void aDuplicateStockReservedChangesNothing() {
        UUID orderId = orderService.placeOrder(1L, 1).getId();
        saga.onStockReserved(orderId);

        saga.onStockReserved(orderId);

        assertThat(orders.findById(orderId).orElseThrow().getStatus()).isEqualTo(OrderStatus.STOCK_RESERVED);
    }

    @Test
    void aReplyForAnUnknownOrderIsIgnored() {
        UUID unknown = UUID.randomUUID();

        saga.onStockReserved(unknown);
        saga.onStockReservationFailed(unknown);

        assertThat(orders.findById(unknown)).isEmpty();
    }
}
```

- [ ] **Step 2: Write the failing listener test** `messaging/SagaReplyListenerTest.java`

```java
package com.kafkademo.order.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

import com.kafkademo.contracts.EnvelopeCodec;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
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
```

- [ ] **Step 3: Run the tests to verify they fail**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q -pl order-service test -Dtest='OrderSagaServiceTest,SagaReplyListenerTest'
```

Expected: compilation FAILS - `OrderSagaService` not found.

- [ ] **Step 4: Write the saga service and the listener**

`order/OrderSagaService.java`:
```java
package com.kafkademo.order.order;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The saga "conductor": reacts to replies and decides the next step.
 * M1: stock replies only. M3 adds payment and compensation here.
 */
@Service
public class OrderSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaService.class);

    private final OrderRepository orders;
    private final Clock clock;

    public OrderSagaService(OrderRepository orders, Clock clock) {
        this.orders = orders;
        this.clock = clock;
    }

    @Transactional
    public void onStockReserved(UUID orderId) {
        Optional<Order> found = orders.findById(orderId);
        if (found.isEmpty()) {
            log.warn("StockReserved for unknown order {} - ignored", orderId);
            return;
        }
        Order order = found.get();
        if (order.markStockReserved(clock.instant())) {
            log.info("Order {} -> STOCK_RESERVED", orderId);
        } else {
            log.warn("StockReserved ignored: order {} is already {}", orderId, order.getStatus());
        }
    }

    @Transactional
    public void onStockReservationFailed(UUID orderId) {
        Optional<Order> found = orders.findById(orderId);
        if (found.isEmpty()) {
            log.warn("StockReservationFailed for unknown order {} - ignored", orderId);
            return;
        }
        Order order = found.get();
        if (order.cancel(CancelReason.OUT_OF_STOCK, clock.instant())) {
            log.info("Order {} -> CANCELLED (OUT_OF_STOCK)", orderId);
        } else {
            log.warn("StockReservationFailed ignored: order {} is already {}", orderId, order.getStatus());
        }
    }
}
```

`messaging/SagaReplyListener.java`:
```java
package com.kafkademo.order.messaging;

import com.kafkademo.contracts.EnvelopeCodec;
import com.kafkademo.contracts.MessageEnvelope;
import com.kafkademo.contracts.MessageTypes;
import com.kafkademo.contracts.Topics;
import com.kafkademo.order.order.OrderSagaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SagaReplyListener {

    private static final Logger log = LoggerFactory.getLogger(SagaReplyListener.class);

    private final EnvelopeCodec codec;
    private final OrderSagaService saga;

    public SagaReplyListener(EnvelopeCodec codec, OrderSagaService saga) {
        this.codec = codec;
        this.saga = saga;
    }

    @KafkaListener(topics = Topics.SAGA_REPLIES)
    public void onReply(ConsumerRecord<String, String> record) {
        log.info("Reply received: partition={} offset={} key={}", record.partition(), record.offset(), record.key());
        MessageEnvelope envelope = codec.fromJson(record.value());
        switch (envelope.type()) {
            case MessageTypes.STOCK_RESERVED -> saga.onStockReserved(envelope.orderId());
            case MessageTypes.STOCK_RESERVATION_FAILED -> saga.onStockReservationFailed(envelope.orderId());
            default -> log.warn("Ignoring unknown reply type '{}' (messageId={})", envelope.type(), envelope.messageId());
        }
    }
}
```

- [ ] **Step 5: Run all tests in the repo**

```bash
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-21.0.4.7-hotspot"
./mvnw -q test
```

Expected: PASS - contracts 7, inventory 13, order 23 (15 from Task 5 + 5 `OrderSagaServiceTest` + 3 `SagaReplyListenerTest`).

- [ ] **Step 6: Checkpoint (no commit)** - whole repo green.

---

### Task 7: Run scripts, end-to-end check, experiments E5 + E7, LEARNINGS

**Files:**
- Create: `scripts/start-instance.ps1`, `scripts/start-services.ps1`, `scripts/stop-services.ps1`, `scripts/send-orders.ps1`
- Create: `docs/LEARNINGS.md`
- Create: `README.md`
- Modify: `docs/implementation-notes.md` (Redpanda Console produce check)

**Interfaces:**
- Consumes: the jars from Tasks 3-6; `docker-compose.yml` and `scripts/start-infra.ps1` (Task 2)
- Produces: `scripts/start-instance.ps1 -Service <name> -Port <port>` (used again in M2 for E1/E6 and later milestones)

- [ ] **Step 1: Create the scripts**

`scripts/start-instance.ps1`:
```powershell
# Starts ONE instance of a service in the background, on the given port.
# Example:  .\scripts\start-instance.ps1 -Service inventory-service -Port 8092
param(
    [Parameter(Mandatory)][string]$Service,
    [Parameter(Mandatory)][int]$Port
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$jar = Get-ChildItem "$root\$Service\target\$Service-*.jar" | Where-Object { $_.Name -notlike '*.original' } | Select-Object -First 1
if (-not $jar) { throw "No jar for $Service - run .\scripts\start-services.ps1 first (it builds)" }

$java = 'C:\Program Files\Eclipse Adoptium\jdk-21.0.4.7-hotspot\bin\java.exe'
New-Item -ItemType Directory -Force -Path "$root\logs" | Out-Null
$log = "$root\logs\$Service-$Port.log"

# --server.port (not -Dserver.port): PowerShell can split -D arguments at the dot.
$process = Start-Process -FilePath $java `
    -ArgumentList @('-jar', "`"$($jar.FullName)`"", "--server.port=$Port") `
    -RedirectStandardOutput $log -RedirectStandardError "$log.err" `
    -WindowStyle Hidden -PassThru
Set-Content -Path "$root\logs\$Service-$Port.pid" -Value $process.Id
Write-Host "Started $Service on port $Port (pid $($process.Id)) - log: logs\$Service-$Port.log"
```

`scripts/start-services.ps1`:
```powershell
# Builds the services (unless -SkipBuild) and starts one instance of each.
# Run .\scripts\start-infra.ps1 first.
param([switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
. "$PSScriptRoot\use-jdk21.ps1"

if (-not $SkipBuild) {
    & .\mvnw.cmd -q -DskipTests package
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
}

& "$PSScriptRoot\start-instance.ps1" -Service inventory-service -Port 8082
& "$PSScriptRoot\start-instance.ps1" -Service order-service -Port 8081
Write-Host 'Give them ~20 seconds to start. Stop everything with .\scripts\stop-services.ps1'
```

`scripts/stop-services.ps1`:
```powershell
# Stops every service instance started by start-instance.ps1.
$root = Split-Path $PSScriptRoot -Parent
Get-ChildItem "$root\logs\*.pid" -ErrorAction SilentlyContinue | ForEach-Object {
    $procId = Get-Content $_
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    Remove-Item $_
    Write-Host "Stopped $($_.BaseName) (pid $procId)"
}
```

`scripts/send-orders.ps1`:
```powershell
# Places orders through order-service.
# Example:  .\scripts\send-orders.ps1 -ProductId 1 -Quantity 2 -Count 10
param(
    [long]$ProductId = 1,
    [int]$Quantity = 1,
    [int]$Count = 1,
    [string]$BaseUrl = 'http://localhost:8081'
)
for ($i = 1; $i -le $Count; $i++) {
    $body = @{ productId = $ProductId; quantity = $Quantity } | ConvertTo-Json
    $order = Invoke-RestMethod -Method Post -Uri "$BaseUrl/orders" -ContentType 'application/json' -Body $body
    Write-Host "$($order.id)  $($order.status)  total=$($order.total)"
}
```

- [ ] **Step 2: Start everything and run the end-to-end check**

In PowerShell, from the repo root:
```powershell
.\scripts\start-infra.ps1
.\scripts\start-services.ps1
Start-Sleep -Seconds 25
.\scripts\send-orders.ps1 -ProductId 1 -Quantity 2          # Keyboard
.\scripts\send-orders.ps1 -ProductId 4 -Quantity 1          # Sold-out mug
.\scripts\send-orders.ps1 -ProductId 2 -Quantity 6          # Monitor: only 5 in stock
Start-Sleep -Seconds 3
# For each id printed above:
Invoke-RestMethod http://localhost:8081/orders/<id>
```

Expected: the Keyboard order ends `STOCK_RESERVED`; the mug and the Monitor orders end `CANCELLED` with `cancelReason` `OUT_OF_STOCK`. `Invoke-RestMethod http://localhost:8082/products/1` shows Keyboard stock 98.

- [ ] **Step 3: Check that Redpanda Console can send a message**

Open `http://localhost:8090` -> Topics -> `inventory-commands` -> look for a "Produce" / "Produce Record" action. Send a record with key `test` and value `hello`. Then in `logs\inventory-service-8082.log` look for the error about an invalid envelope and the "Command received ... key=test" line.

Append to `docs/implementation-notes.md` under `## M1` one of:
```markdown
- **Redpanda Console can produce messages** (checked in Task 7): Topics ->
  inventory-commands -> Produce Record. M2 experiments E2 and E3 use it.
```
or, if the action does not exist:
```markdown
- **Redpanda Console v3.12.0 cannot produce messages** on this setup. The M2
  plan must add a small dev-only endpoint for sending raw test messages.
```

- [ ] **Step 4: Experiment E5 - scaling consumers**

```powershell
.\scripts\start-instance.ps1 -Service inventory-service -Port 8092
.\scripts\start-instance.ps1 -Service inventory-service -Port 8093
Start-Sleep -Seconds 25
Select-String -Path logs\inventory-service-*.log -Pattern 'partitions assigned'
.\scripts\send-orders.ps1 -ProductId 1 -Quantity 1 -Count 30
Start-Sleep -Seconds 3
Select-String -Path logs\inventory-service-*.log -Pattern 'Command received' | Group-Object Filename | Select-Object Name, Count
.\scripts\start-instance.ps1 -Service inventory-service -Port 8094
Start-Sleep -Seconds 25
Select-String -Path logs\inventory-service-8094.log -Pattern 'partitions assigned'
```

Expected: with 3 instances, each log shows **one** partition assigned (e.g. `[inventory-commands-1]`); the 30 orders are spread over the 3 logs; the 4th instance gets **no** partition (`partitions assigned: []`) and stays idle. Also open Redpanda Console -> Consumer Groups -> `inventory-service` and note the members and their partitions. Write down the real lines and counts for Step 6. Stop the extra instances afterwards:

```powershell
Stop-Process -Id (Get-Content logs\inventory-service-8092.pid), (Get-Content logs\inventory-service-8093.pid), (Get-Content logs\inventory-service-8094.pid)
Remove-Item logs\inventory-service-809?.pid
```

- [ ] **Step 5: Experiment E7 - keys and partitions**

```powershell
.\scripts\send-orders.ps1 -ProductId 1 -Quantity 1 -Count 5
```

In Redpanda Console, open `inventory-commands` and `saga-replies` and, for 2-3 of the order ids, note: the key, the partition in `inventory-commands`, and the partition of its reply in `saga-replies`.

Expected: the command and the reply of the same order have the **same key** and land in the **same partition number** in both topics (same key + same partition count = same partition), while different orders spread across partitions 0-2.

- [ ] **Step 6: Write `docs/LEARNINGS.md`**

Create the file with this content, then fill in the two "What I saw" parts with the real lines and numbers from Steps 4 and 5:

````markdown
# Learnings - Kafka and microservices

My learning notes for this demo. Written for a beginner, in the same format for
every topic:

1. **What it is** 2. **Why it exists** 3. **How it works here**
4. **What I saw** (from my experiments) 5. **Interview answer**
6. **Common follow-up questions**

The "Interview answer" parts are collected again at the end (added in M5).

---

## 1. Kafka basics

### What it is

Kafka is a **distributed log**: an append-only list of messages that is stored on
disk and can be read by many programs. Programs that write are **producers**;
programs that read are **consumers**. Messages are not deleted when they are read.

### Why it exists

Services that call each other directly (REST) are tightly coupled: if the
receiver is down or slow, the sender fails or waits. With Kafka, the sender writes
a message and moves on; the receiver reads it when it is ready. Kafka also lets
many different services read the same messages.

### The words you need

| Word | Meaning | In this project |
|---|---|---|
| **Broker** | A Kafka server. Real systems run 3 or more. | One broker in Docker (`kafka-demo-kafka`) |
| **Topic** | A named stream of messages, like a table name | `inventory-commands`, `saga-replies` |
| **Partition** | A topic is split into partitions. Each partition is an **ordered** log. Partitions are how Kafka works in parallel. | 3 partitions per topic |
| **Offset** | A message's position number inside its partition (0, 1, 2...) | Printed in the logs: `offset=12` |
| **Key** | Decides the partition: same key -> same partition | The `orderId` |
| **Producer** | Writes messages | `CommandPublisher`, `ReplyPublisher` |
| **Consumer** | Reads messages | `InventoryCommandListener`, `SagaReplyListener` |
| **Consumer group** | Consumers that **share** the work of a topic. Each partition is read by exactly one consumer of the group. | Groups `inventory-service`, `order-service` |
| **Rebalance** | When a consumer joins or leaves a group, partitions are handed out again | Seen in E5 |
| **Retention** | How long Kafka keeps messages (by time or size), whether read or not | Default (7 days) |
| **Commit** | A consumer saves "I processed up to offset N" | After every message (`ack-mode: record`) |
| **KRaft** | Kafka manages its own cluster metadata - no ZooKeeper needed (since Kafka 3.3; ZooKeeper removed in 4.0) | Our broker is also the controller |
| **Replication factor** | How many brokers keep a copy of each partition | 1 here (one broker). Production: 3, with `min.insync.replicas=2` |

### How it works here

```
order-service --ReserveStock--> [inventory-commands: P0 | P1 | P2] --> inventory-service
      ^                                                                      |
      |                                                                      |
      +-------- [saga-replies: P0 | P1 | P2] <--StockReserved / Failed--------+
```

- Messages are JSON strings in an "envelope" (`messaging-contracts/.../EnvelopeCodec`):
  `messageId`, `type`, `orderId`, `occurredAt`, `payload`.
- `acks=all` + idempotent producer: the broker confirms a write only when it is
  safe, and producer retries never write duplicates into Kafka.
- The consumer commits **after** processing each message, so a crash means the
  message is read again, never lost (more in M2).

### Interview answer

> "Kafka is a distributed, durable log. Producers write messages to topics;
> topics are split into partitions, which are ordered and let consumers work in
> parallel. Consumers in the same group share the partitions, so each message is
> handled by one consumer of the group, and different groups each get every
> message. Messages stay for the retention period, so they can be read again."

### Common follow-up questions

- *"How is Kafka different from a normal queue?"* - A queue usually deletes a
  message once it is read. Kafka keeps it, so several groups can read it and a
  group can re-read it by moving its offset.
- *"What is an offset?"* - A message's position in its partition. A consumer
  group remembers its offset per partition, so it continues where it stopped.

---

## 2. Ordering and keys (experiment E7)

### What it is

Kafka keeps order **only inside one partition**. The **key** chooses the
partition: Kafka hashes the key, so the same key always goes to the same
partition (as long as the number of partitions doesn't change).

### Why it matters

For one order, `ReserveStock` must be handled before a later `ReleaseStock`. If
they went to different partitions, two consumers could handle them in the wrong
order. Using the `orderId` as the key keeps all messages of one order in one
partition, in order.

### How it works here

`CommandPublisher` and `ReplyPublisher` always send with `key = orderId`.

### What I saw (E7)

- What I did: sent 5 orders, looked at the messages in Redpanda Console.
- What I saw: (write the real key / partition pairs here, e.g. "order 3f2a...:
  command in inventory-commands P1, reply in saga-replies P1")
- Why: same key + same partition count -> same partition number, in both topics.

### Interview answer

> "Kafka only guarantees order within a partition. So I give related messages
> the same key - here the order id - and they all go to the same partition and
> are processed in order. Different orders can still be processed in parallel."

### Common follow-up questions

- *"What if you add partitions later?"* - Keys are re-mapped, so the same key
  can move to another partition. Plan the partition count up front.
- *"What if you need order across all messages?"* - One partition, which means
  no parallelism. Usually you only need order per entity, not globally.

---

## 3. Scaling consumers with consumer groups (experiment E5)

### What it is

All instances of a service use the same `group-id`. Kafka gives each partition
to exactly **one** consumer in the group. Add consumers -> partitions are
re-assigned (a **rebalance**).

### Why it matters

It is how you scale message processing: run more instances. But the limit is
the number of partitions: **with 3 partitions, a 4th consumer has nothing to
do**.

### How it works here

`inventory-service` instances on ports 8082, 8092, 8093 (and 8094) - all in group
`inventory-service`.

### What I saw (E5)

- What I did: started 3 inventory instances, sent 30 orders, then started a 4th.
- What I saw: (write the real "partitions assigned" lines per instance, the
  number of commands each instance handled, and what the 4th instance got)
- Why: one partition per consumer; a 4th consumer gets no partition.

### Interview answer

> "Consumers with the same group id share a topic's partitions - one partition
> per consumer. To scale, you add instances, up to the number of partitions;
> extra consumers stay idle. When a consumer joins or leaves, Kafka rebalances
> the partitions, and processing pauses briefly."

### Common follow-up questions

- *"How many partitions should a topic have?"* - At least the number of
  consumers you expect to run in parallel, with some room to grow.
- *"What happens during a rebalance?"* - Consumers stop, partitions are
  re-assigned, and they continue from the committed offsets. Messages processed
  but not yet committed are processed again (M2: why consumers must be
  idempotent).

---

## 4. The dual-write problem (why M2 needs the outbox)

### What it is

In M1, `order-service` saves the order in the database, **then** sends
`ReserveStock` to Kafka. These are two separate writes. If the app crashes
between them, the order is saved but the message is never sent - the order stays
`PENDING` forever. The same happens in `inventory-service` between reserving the
stock and sending the reply.

### Why it matters

You cannot put a database write and a Kafka write in one normal transaction.
This is one of the most common microservice interview questions.

### How it works here

Look for the comment `M1: sent AFTER the commit` in `OrderService` and
`InventoryCommandListener`. M2 replaces this with the **outbox pattern**.

### Interview answer

> "Saving to the database and publishing to Kafka are two writes, and a crash
> between them leaves the system inconsistent. The outbox pattern fixes it: write
> the business change and the message into the same database transaction, and a
> separate process publishes the message afterwards."

---

## 5. Testcontainers and kafka-native

- Integration tests start **real** PostgreSQL and Kafka in Docker
  (`IntegrationTestContainers`), using the same images as `docker-compose.yml`, so
  tests download nothing new.
- `apache/kafka-native` is Kafka compiled to a native program with GraalVM:
  starts in about a second and uses little memory. It is meant for local
  development and tests, not production, and it has no Kafka command-line tools
  inside.
- Docker Engine 29 rejects the API version Testcontainers uses by default; the
  parent `pom.xml` sets `api.version=1.44` for Surefire.

---

## Glossary

| Term | Short meaning |
|---|---|
| Broker | A Kafka server |
| Topic | A named stream of messages |
| Partition | An ordered piece of a topic; unit of parallelism |
| Offset | Position of a message in a partition |
| Key | Chooses the partition; same key -> same partition |
| Consumer group | Consumers sharing a topic, one partition each |
| Rebalance | Partitions handed out again when consumers join/leave |
| Commit | Saving the processed offset |
| Retention | How long messages are kept |
| KRaft | Kafka's built-in cluster management (no ZooKeeper) |
| Replication factor | Number of copies of each partition |
| acks=all | Producer waits until the write is safe on all in-sync copies |
| Idempotent producer | Producer retries never create duplicates in Kafka |
| Dual write | Two separate writes (DB + Kafka) that can get out of sync |

---

## Coming in the next milestones

- M2: delivery guarantees, outbox, idempotent consumer, retries and the
  dead-letter topic, experiments E1-E4 and E6.
- M3: the saga - orchestration vs choreography, compensation vs rollback, async
  API (202 + polling).
- M4: Spring Cloud Gateway, Resilience4j, Micrometer Tracing + OpenTelemetry,
  Jaeger, Prometheus, Grafana, experiments E8 and E9.
- M5: interview Q&A collected from every section, and next steps.
````

- [ ] **Step 7: Write `README.md`**

````markdown
# Kafka microservices demo

A learning project: Kafka and microservice patterns with Spring Boot.
Design: `docs/superpowers/specs/2026-10-11-kafka-microservices-demo-design.md`.
What I learned: `docs/LEARNINGS.md`.

## Status

M1 (Kafka basics) done: order-service and inventory-service talk through Kafka.

## Run it (Windows, PowerShell)

Requirements: Docker Desktop running, JDK 21 installed at
`C:\Program Files\Eclipse Adoptium\jdk-21.0.4.7-hotspot`.

```powershell
.\scripts\start-infra.ps1        # Kafka, Redpanda Console, PostgreSQL
.\scripts\start-services.ps1     # builds and starts order + inventory
.\scripts\send-orders.ps1 -ProductId 1 -Quantity 2
Invoke-RestMethod http://localhost:8081/orders/<id>
.\scripts\stop-services.ps1
docker compose stop
```

| What | Where |
|---|---|
| order-service | http://localhost:8081 |
| inventory-service | http://localhost:8082 |
| Redpanda Console (Kafka UI) | http://localhost:8090 |
| PostgreSQL | localhost:5433, user `demo`, password `demo` |

## Tests

```powershell
. .\scripts\use-jdk21.ps1
.\mvnw.cmd test
```
````

- [ ] **Step 8: Stop and final check**

```powershell
.\scripts\stop-services.ps1
. .\scripts\use-jdk21.ps1
.\mvnw.cmd -q test
```

Expected: all services stopped; all tests green (contracts 7, inventory 13, order 23). Leave the Docker containers running or stop them with `docker compose stop` (keeps the data).

- [ ] **Step 9: Checkpoint (no commit)** - tell the owner M1 is done, with the E5/E7 results, the disk numbers, and the Redpanda Console finding. Ask whether to commit (personal GitHub repo, personal git identity) before starting the M2 plan.

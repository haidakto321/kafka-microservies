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
  `messageId`, `type`, `orderId`, `occurredAt`, `payload`. Readable in Redpanda
  Console (http://localhost:8090).
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
partition: Kafka hashes the key (murmur2), so the same key always goes to the
same partition (as long as the number of partitions doesn't change).

### Why it matters

For one order, `ReserveStock` must be handled before a later `ReleaseStock`. If
they went to different partitions, two consumers could handle them in the wrong
order. Using the `orderId` as the key keeps all messages of one order in one
partition, in order.

### How it works here

`CommandPublisher` and `ReplyPublisher` always send with `key = orderId`.

### What I saw (E7)

- What I did: sent 5 orders, then looked up each order's command (in the
  inventory log) and its reply (in the order log).
- What I saw:

  | Order (= key) | `inventory-commands` | `saga-replies` |
  |---|---|---|
  | `ac9874ee...` | partition 0 | partition 0 |
  | `515a1458...` | partition 0 | partition 0 |
  | `e91dc526...` | partition 1 | partition 1 |
  | `f61a9194...` | partition 1 | partition 1 |
  | `fa06af38...` | partition 1 | partition 1 |

- Why: same key + same number of partitions -> same partition number, in both
  topics. (Only 5 orders, so none happened to land on partition 2 - with 30
  orders in E5 all three partitions were used.)

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
`inventory-service`, started with `scripts/start-instance.ps1`.

### What I saw (E5)

- **3 instances -> one partition each:**
  `8082: [inventory-commands-0]`, `8093: [inventory-commands-1]`,
  `8092: [inventory-commands-2]`.
- **30 orders were spread over the 3 instances by partition:** 7 orders on
  partition 0, 6 on partition 1, 17 on partition 2. Not equal - the key hash
  decides, and 30 random keys don't split evenly.
- **4th instance -> one consumer gets nothing.** After 8094 joined:
  `8082: [P1]`, `8093: [P2]`, `8094: [P0]`, `8092: []`. Surprise: the idle one
  was **8092, an old instance, not the newcomer**, and the others swapped
  partitions. The default assignment strategy ("range", eager rebalance)
  takes **all** partitions away and hands them out again. A "cooperative
  sticky" strategy would move as few partitions as possible.

### Two more things E5 taught me (not planned!)

**1. A conflict, a silent retry, and a log line that lied.** All 30 orders were
for the same product, so 3 instances updated the **same database row** at the
same time. Some messages were processed **twice** - same partition, same offset:

```
19:49:36.641 Command received: partition=2 offset=19 ...  -> "reserved 1 of product 1, 74 left"
19:49:37.124 Command received: partition=2 offset=19 ...  -> "reserved 1 of product 1, 69 left"
```

But the final stock was exactly right (98 - 30 = 68) and there was one
reservation per order. What happened: the "reserved" log line is written
**inside** the transaction, before the commit. The commit then failed because
another instance had changed the same row first (optimistic locking with
`@Version`), the transaction rolled back, and Spring Kafka's error handler
retried the message - **silently**, because it only logs when all retries are
used up. The retry succeeded.
- `@Version` prevented overselling under real concurrency.
- A log line written inside a transaction can describe work that was rolled
  back. Log "success" after the commit, or remember this when debugging.

**2. A killed consumer keeps its partitions for 45 seconds.** I stopped the
extra instances with a force kill, so they never told Kafka they were leaving.
Kafka waited for the **session timeout** (`session.timeout.ms`, 45 s by default)
before giving their partitions to the remaining instance. Two orders sent in that
window waited **23 seconds** on partition 0 - not lost, just late:

```
19:51:58  orders placed (commands on partition 0 - its consumer is dead)
19:52:21  rebalance: 8082 gets [P0, P1, P2]
19:52:21  the two waiting commands are processed
```
A normal shutdown (`SIGTERM`, Ctrl+C) leaves the group at once, so the rebalance
happens immediately. This is why consumer lag spikes when an instance crashes.

### Interview answer

> "Consumers with the same group id share a topic's partitions - one partition
> per consumer. To scale, you add instances, up to the number of partitions;
> extra consumers stay idle. When a consumer joins or leaves, Kafka rebalances
> the partitions, and processing pauses briefly. If a consumer crashes instead
> of shutting down cleanly, its partitions wait for the session timeout before
> they're reassigned."

### Common follow-up questions

- *"How many partitions should a topic have?"* - At least the number of
  consumers you expect to run in parallel, with some room to grow.
- *"What happens during a rebalance?"* - Consumers stop, partitions are
  re-assigned, and they continue from the committed offsets. Messages processed
  but not yet committed are processed again (M2: why consumers must be
  idempotent).
- *"What if two consumers update the same row?"* - Use locking. Here optimistic
  locking (`@Version`) made the loser fail and retry; I saw it happen in E5.

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

## 5. A broken message in M1 (preview of M2's dead-letter topic)

### What I saw

I sent a record with key `test` and value `hello` (not valid JSON) from
Redpanda Console. inventory-service received it **10 times in a row** (Spring
Kafka's default: 1 try + 9 retries, no wait), logged `InvalidMessageException:
Not a valid message envelope`, and then **skipped it**. The messages behind it
continued normally (also checked by the test
`aBrokenMessageDoesNotBlockTheMessagesBehindIt`).

### Why it matters

Skipping keeps the partition moving - but the broken message is **gone**, with
only a log line left. Retrying it was also pointless: bad JSON will never become
valid. M2 fixes both: errors that can never succeed are **not retried**, and
failed messages go to a **dead-letter topic** where they can be inspected and
replayed.

---

## 6. Testcontainers and kafka-native

- Integration tests start **real** PostgreSQL and Kafka in Docker
  (`IntegrationTestContainers`), using the same images as `docker-compose.yml`, so
  tests download nothing new.
- `apache/kafka-native` is Kafka compiled to a native program with GraalVM:
  starts in about a second and uses little memory. It is meant for local
  development and tests, not production, and it has no Kafka command-line tools
  inside. It works with Testcontainers' `KafkaContainer` and Spring Boot's
  `@ServiceConnection`.
- Docker Engine 29 rejects the API version Testcontainers uses by default; the
  parent `pom.xml` sets `api.version=1.44` for Surefire.
- On the office network, Docker Hub downloads fail; the images came from
  Google's mirror `mirror.gcr.io` (same content, digest checked) - see
  `docs/implementation-notes.md`.

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
| Session timeout | How long Kafka waits for a silent consumer before rebalancing (45 s default) |
| Assignment strategy | How partitions are divided in a rebalance (range, round-robin, cooperative sticky) |
| Commit | Saving the processed offset |
| Retention | How long messages are kept |
| KRaft | Kafka's built-in cluster management (no ZooKeeper) |
| Replication factor | Number of copies of each partition |
| acks=all | Producer waits until the write is safe on all in-sync copies |
| Idempotent producer | Producer retries never create duplicates in Kafka |
| Optimistic locking | A version number on a row; a conflicting update fails instead of overwriting |
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

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
  message is read again, not lost - "at-least-once" (section 7). Since M2 every
  consumer skips a message it has already handled (section 9), and a message that
  keeps failing goes to a dead-letter topic instead of being dropped (section 10).

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

**1. A conflict, a quiet retry, and a log line that lied.** All 30 orders were
for the same product, so 3 instances updated the **same database row** at the
same time. Some messages were processed **twice** - same partition, same offset:

```
19:49:36.641 Command received: partition=2 offset=19 ...  -> "reserved 1 of product 1, 74 left"
19:49:37.124 Command received: partition=2 offset=19 ...  -> "reserved 1 of product 1, 69 left"
```

But the final stock was exactly right (98 - 30 = 68). What happened: the
"reserved" log line is written **inside** the transaction, before the commit. The
commit then failed because another instance had changed the same row first
(optimistic locking with `@Version`), the transaction rolled back, and Spring
Kafka's error handler retried the message. The retry was **quiet**: after each
failed try Spring Kafka logs only one INFO line, without the cause:

```
INFO ... o.s.k.l.KafkaMessageListenerContainer : Record in retry and not yet recovered
```

(The 3 instance logs had 20 of these lines.) The full ERROR with the stack trace
comes only when all tries fail. Here the next try succeeded.
- `@Version` prevented overselling under real concurrency.
- A log line written inside a transaction can describe work that was rolled
  back. Log "success" after the commit, or remember this when debugging.
- To find these retries, grep for `Record in retry`.
- **The retry has a limit:** 10 tries, no wait between them. Under heavy
  contention a valid `ReserveStock` could lose 10 times in a row. It would then
  be skipped (ERROR log), no reply would be sent, and the order would stay
  `PENDING` forever. M2 must not treat a lock conflict like a broken message:
  retry the conflict inside the service, don't send it to the dead-letter topic.
- Only the stock count proves that no order was reserved twice. The reservation
  table can't prove it: saving a reservation for an order that already has one
  updates the row instead of failing (section 4).

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
  The default retry gives up after 10 tries, so under heavy contention you
  retry the conflict in the service code, or use a pessimistic lock
  (`SELECT ... FOR UPDATE`).

---

## 4. The dual-write problem (why M2 needs the outbox)

### What it is

In M1, `order-service` saves the order in the database, **then** sends
`ReserveStock` to Kafka. These are two separate writes. If the app crashes
between them, the order is saved but the message is never sent - the order stays
`PENDING` forever.

In `inventory-service` the same crash has a **different** result. The stock is
reserved and committed, but the reply is not sent - and the Kafka offset is not
committed either (it is committed only after the listener returns). After the
restart Kafka delivers the same `ReserveStock` again, so inventory **reserves the
stock a second time**, then sends a reply. The result is a **duplicate**, not a
lost reply.

- The same happens if sending the reply times out (`.get(10, SECONDS)` in
  `ReplyPublisher`). A timeout does not mean "not sent": the producer keeps
  trying in the background (up to `delivery.timeout.ms`, 2 minutes), so the first
  reply can still arrive. Then order-service gets two replies.
- The primary key on `stock_reservation.order_id` does not stop the duplicate.
  We set the id ourselves, so Spring Data's `save()` cannot tell the entity is
  new. It does a merge (SELECT, then UPDATE the existing row) instead of an
  INSERT that would fail.

So: on the **sending** side the dual write **loses** messages; on the
**receiving** side, with at-least-once delivery, it **repeats** side effects.
M2 needs both fixes: the outbox (nothing lost) and an idempotent consumer
(nothing done twice). One alone is not enough: an idempotent consumer without
the outbox turns this duplicate into a lost reply (on redelivery it sees "already
done" and skips, so the reply is never sent).

### Why it matters

You cannot put a database write and a Kafka write in one normal transaction.
This is one of the most common microservice interview questions.

### How it works here

In M1, `OrderService` and `InventoryCommandListener` sent to Kafka right after
the database commit.

**Fixed in M2:** `OrderService` and `InventoryCommandHandler` no longer send to
Kafka. They write an outbox row in the same transaction (section 8), and every
consumer remembers the messages it handled (section 9).

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

**Since M2:** see section 10 and experiment E2 - the same `hello` now goes to
`inventory-commands.DLT` at once, with no retries.

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

---

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
- `max.block.ms: 5000`: with Kafka down, a send gives up after 5 s instead of the
  default 60 s, so the relay does not hold its transaction for a minute.
- Trade-off: the relay keeps its DB transaction open while it talks to Kafka. Fine
  for a demo; at high volume you would send first and mark in a short separate
  transaction, or use CDC.
- Small surprise: PostgreSQL `jsonb` stores the payload keys in its own order
  (`{"quantity": 1, "productId": 1}`). Harmless - consumers read fields by name.
- Coming in M4: the outbox **breaks the trace** (the message is sent later, by
  another thread). The fix is to store the trace id in the row.

### What I saw (E4)

- What I did: stopped the Kafka container, placed 3 orders, started Kafka again.
- What I saw:
  - Each `POST /orders` answered `202 PENDING` in **20-24 ms** (the first one
    489 ms - JVM warm-up). The request never touches Kafka now.
  - `outbox-status.ps1`: order_db **3 waiting**.
  - order-service logged `Outbox relay: could not send ReserveStock ... will try
    again: Send failed` about every 5.5 s (5 s `max.block.ms` + 500 ms delay).
  - Kafka started at 23:28:32.8; the relay logged `sent 3 of 3` at **23:28:33.3**
    (0.5 s later). Inventory reserved the stock at 33.7, its own relay sent the 3
    replies at 34.1, and all 3 orders were `STOCK_RESERVED`. 0 rows waiting.
- Why: the order and its message were one commit; the message just waited in the
  table until the relay could send it. Compare M1: a send failure there turned
  into a 500 for an order that was already saved.

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

---

## 9. The idempotent consumer

### What it is

A consumer that gives the same result whether a message arrives once or several
times.

### Why it exists

With at-least-once delivery, duplicates **will** happen (section 7). Without
protection, a duplicate `ReserveStock` takes the stock twice (the M1 lesson in
section 4).

### How it works here

```
[ one DB transaction:
    messageId in processed_message?  -> yes: skip, done
    INSERT processed_message (messageId)
    do the work (reserve stock / change the order)
    INSERT outbox_event (the reply)                     ]  -> commit
```

- Code: `InventoryCommandHandler`, `SagaReplyHandler`. The listeners only read
  the record and call the handler.
- Everything is one commit: if the work fails, the `processed_message` row is
  rolled back too, so a retry is not wrongly skipped.
- Two copies at the same moment: both pass the check, but the second INSERT hits
  the primary key and rolls back; its retry then skips.
- **Natural idempotency** as a second guard: one reservation per order. The
  reservation is insert-only (`Persistable.isNew()`), so a second one fails on the
  primary key instead of overwriting the first.
- Not every consumer needs this: the notification service (M3) only writes a log
  line - a duplicate log line is harmless, a duplicate charge is not.

### What I saw (E1 and E3)

- **E1** - What I did: started inventory with `--demo.processing-delay-ms=15000`
  (a 15 s pause **after** the DB commit, **before** the offset commit), placed one
  order, force-killed inventory during the pause, started it again.
  - Before the kill: `Command received partition=1 offset=10` at 23:29:51.163,
    `reserved 1 of product 1, 59 left`, `Demo delay: waiting 15000 ms`. The reply
    was sent anyway at 51.895 - the relay is a separate thread - so the order was
    already `STOCK_RESERVED`.
  - After the restart: the partitions came back only at 23:30:40.7, about **47 s
    after the kill** (the killed consumer's session timeout, as in E5). Then:
    `Command received partition=1 offset=10` - **the same offset again** - and
    `Duplicate message 8026b58d... (ReserveStock) ... already processed, skipped`.
  - Stock stayed **59**, still **one** reply row.
  - Why: the offset was never committed, so Kafka delivered the command again;
    the `processed_message` row (committed with the stock change) said "done".
- **E3** - What I did: rebuilt that same `ReserveStock` envelope from the order
  database and produced it twice from Redpanda Console, key = orderId.
  - Both copies went to partition 1 (the Console's Murmur2 partitioner hashes
    the key the same way as the Java producer): offsets 11 and 12.
  - Both: `Duplicate message ... skipped` within 8 ms. Stock 59, one reply row.

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

---

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
- The DLT gets the record **unchanged**, plus 10 headers (see E2).
- The DLT name is set **explicitly** (`<topic>.DLT`). Spring Kafka 3.3's own
  default is `<topic>-dlt` - found the hard way, see the build note below.
- The DLT topics are created by `NewTopic` beans (auto-create is off).
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

**Build note - what happens when the DLT is missing.** My first version used
Spring Kafka's default DLT name, `inventory-commands-dlt`, which did not exist
(auto-create off). Publishing to it failed, the error handler treated the record
as "not recovered", went back to it - and tried again and again. The partition
was **stuck**: the valid messages behind it were never processed. The tests
caught it only because the test broker also has auto-create off. Lesson: a
dead-letter topic must exist, and check what name the framework really uses.

### What I saw (E2)

- What I did: produced key `e2-broken`, value `hello` to `inventory-commands`
  from Redpanda Console, then placed a valid order.
- What I saw:
  - `Command received: partition=1 offset=13 key=e2-broken` at 23:34:15.224, then
    **5 ms later**, no retries: `Giving up on inventory-commands-1 offset 13 (key
    e2-broken) - sending it to inventory-commands.DLT. Cause: JsonParseException:
    Unrecognized token 'hello'`.
  - The valid order right after it: `STOCK_RESERVED`.
  - `inventory-commands.DLT` in Redpanda Console: 1 message, key `e2-broken`,
    value `hello` (14 bytes) - but the topic is **6.41 KiB**, because of the 10
    headers: `kafka_dlt-original-topic`, `-original-partition`,
    `-original-offset`, `-original-timestamp`, `-original-timestamp-type`,
    `-original-consumer-group`, `-exception-fqcn`
    (`ListenerExecutionFailedException`), `-exception-cause-fqcn`
    (`InvalidMessageException`), `-exception-message`, `-exception-stacktrace`.
- Why: `InvalidMessageException` is marked not retryable, so the error handler
  went straight to the recoverer. The stack trace header is the biggest part.

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
- *"What if publishing to the DLT fails?"* - The record is not recovered, so the
  consumer goes back to it: the partition stops. Make sure the DLT exists.

---

## 11. Consumer lag (experiment E6)

### What it is

**Lag** = how many messages in a partition the group has not processed yet
(newest offset minus the group's committed offset).

### Why it matters

Growing lag means the consumers are slower than the producers. It is the main
health metric of a Kafka consumer.

### How it works here

`demo.processing-delay-ms=200` slows inventory down. Redpanda Console ->
Consumer Groups shows the lag per partition (and its API,
`/api/consumer-groups/inventory-service`, gives the numbers). Grafana comes in M4.

### What I saw (E6)

- What I did: one inventory instance with a 200 ms delay; sent 500 orders for
  the sold-out mug (no stock changes) - they arrived in **8 s** (about 62 per
  second).
- What I saw:

  | Time | Lag (sum of 3 partitions) |
  |---|---|
  | 23:36:50 (still sending) | 332 |
  | 23:37:54 | 167 |
  | 23:38:04 | 120 |
  | 23:38:14 | 74 |
  | 23:38:24 | 29 |
  | 23:38:34 | 0 |

  - The 500 commands were processed from 23:36:40.6 to 23:38:30.3: **110 s, 4.6
    per second** (200 ms pause + about 20 ms of work). The peak was not sampled;
    computed, it was about 460 when the sending stopped.
  - The partitions drained unevenly: partition 0 was empty first, partition 1
    last.
  - All 500 orders ended `CANCELLED / OUT_OF_STOCK`.
- Why: one consumer thread handles all 3 partitions one record at a time. Up to
  3 instances (one per partition) would drain about 3 times faster; a 4th would
  sit idle (E5).

### Interview answer

> "Lag is the gap between the newest offset and the group's committed offset. I
> watch it per partition; if it keeps growing I add consumers, up to the number
> of partitions, or make processing faster."

---

## 12. Flyway and database per service

- **Flyway** runs the SQL files in `src/main/resources/db/migration` (`V1__...`,
  `V2__...`) in order at start-up and records them in `flyway_schema_history`. A
  file that already ran must never change - a change is a new `V<n>` file. M2
  added `V3` to inventory and `V2` to order (seen in the logs: `Successfully
  applied 1 migration ... now at version v3`).
- **Database per service**: order, inventory (and payment in M3) each own a
  database and never read each other's tables. They share data only through
  Kafka messages and the REST price call. That is why each service has its own
  `outbox_event` and `processed_message` tables, and its own copy of the small
  outbox classes.

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
| At-least-once | A message is processed one or more times - never lost, may repeat |
| Idempotent consumer | A consumer that gives the same result for one copy or several |
| Outbox | A table where messages wait, written in the same transaction as the business change |
| Relay | The job that sends outbox rows to Kafka and marks them sent |
| SKIP LOCKED | SQL: skip rows another transaction has locked instead of waiting |
| Partial index | An index on only some rows (here: the unsent ones) |
| Back-off | The wait between retries (here 1 s, 2 s, 4 s) |
| Poison message | A message that can never be processed, e.g. bad JSON |
| Dead-letter topic (DLT) | Where a message goes after it failed for good |
| Consumer lag | Messages in a partition not processed yet by the group |
| Flyway | Runs numbered SQL migration files once, in order |

---

## Coming in the next milestones

- M3: the saga - orchestration vs choreography, compensation vs rollback, async
  API (202 + polling).
- M4: Spring Cloud Gateway, Resilience4j, Micrometer Tracing + OpenTelemetry,
  Jaeger, Prometheus, Grafana, experiments E8 and E9.
- M5: interview Q&A collected from every section, and next steps.

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
| **Producer** | Writes messages | `OutboxRelay` (M1: `CommandPublisher`, `ReplyPublisher`) |
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

Every message is sent with `key = orderId`. In M1 `CommandPublisher` and
`ReplyPublisher` did it; since M2 `OutboxRelay` sends every message, with the
row's `aggregate_id` (the orderId) as the key.

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
  **Since M2:** the listener tries a lock conflict 3 times right away, then the
  error handler retries after 1 s, 2 s and 4 s, then the record goes to the DLT
  (section 10).
- Only the stock count proves that no order was reserved twice. In M1 the
  reservation table couldn't prove it: saving a reservation for an order that
  already had one updated the row instead of failing (section 4). **Since M2** the
  reservation is insert-only, so a second one fails on the primary key
  (section 9).

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
  Spring Kafka's default retry gives up after 10 tries, so under heavy
  contention you retry the conflict in the service code (this project, since
  M2), or use a pessimistic lock (`SELECT ... FOR UPDATE`).

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
  offset commit (E1), a rebalance, the outbox relay sending a row again
  (including copies left in the producer's buffer while Kafka was down - E4b in
  section 8), or someone producing the same message twice (E3).

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
  older one (with one relay instance - see the follow-up questions).
- Kafka down - two different waits (E4 and E4b below):
  - `max.block.ms: 5000` only limits how long `send()` waits for **topic
    metadata** (default 60 s). It applies when the producer has not talked to
    that topic yet.
  - If the producer already has the metadata, `send()` returns at once and puts
    the record in its **buffer**; the relay's `.get(10, SECONDS)` then times out.
    The buffered copy is **not** cancelled: the producer keeps trying for up to
    `delivery.timeout.ms` (120 s). So every failed relay run leaves one more copy,
    and they are all delivered when Kafka comes back. The consumers skip them as
    duplicates - "a timeout does not mean not sent" again (section 4).
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
    again: ... Send failed` about every 5.5 s (5 s `max.block.ms` + 500 ms
    delay). This order-service had not sent anything since it started, so the
    producer had no metadata yet.
  - Kafka started at 23:28:32.8; the relay logged `sent 3 of 3` at **23:28:33.3**
    (0.5 s later). Inventory reserved the stock at 33.7, its own relay sent the 3
    replies at 34.1, and all 3 orders were `STOCK_RESERVED`. 0 rows waiting.
- Why: the order and its message were one commit; the message just waited in the
  table until the relay could send it. Compare M1: a send failure there turned
  into a 500 for an order that was already saved.

### What I saw (E4b - Kafka down after the producer already has metadata)

Found in the final review of M2, then checked for real:

- What I did: started the services, placed one order (so the producer knew the
  topic), stopped Kafka, placed one more order, waited 40 s, started Kafka.
- What I saw:
  - The relay warned every **10.5 s** (the 10 s `.get` timeout + 500 ms), not
    every 5.5 s - `max.block.ms` did not apply. (The first version even logged
    `will try again: null`, because a `TimeoutException` has no message; it now
    logs the exception name.)
  - When Kafka came back, inventory received the command **4 times**: processed
    once, then `Duplicate message ... skipped` 3 times - one copy for each failed
    relay run, delivered from the producer's buffer.
  - Stock was taken once; the order reached `STOCK_RESERVED`.
- Why: `.get()` timing out does not cancel the send. The idempotent consumer is
  what keeps this correct. (A cleaner relay would set `delivery.timeout.ms`
  below the `.get` timeout, so a failed run leaves nothing behind.)

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

## 13. The saga (orchestration) and compensation

### What it is

A **saga** is one business action spread over several services, as a chain of
local transactions - each service commits its own part and sends a message. If a
later step fails, earlier steps are undone by **compensation**: a new command
that reverses the effect (put the stock back), not a database rollback.

### Why it exists

Each service has its own database (section 12), so there is no single
transaction across them. Two-phase commit (XA) would lock all of them and is
rarely available with Kafka. A saga accepts that the system is briefly in
between ("eventually consistent") and fixes failures with compensation.

### How it works here

```
order-service (the conductor)              other services
-----------------------------              --------------
PENDING      --ReserveStock-------------->  inventory: reserve
             <--StockReserved / Failed----
STOCK_RESERVED --ChargePayment----------->  payment: total <= 1,000 ?
             <--PaymentSucceeded / Failed-
CONFIRMED    --OrderConfirmed------------>  notification (order-events)
CANCELLED    --ReleaseStock-------------->  inventory: put the stock back   (only after PaymentFailed)
             --OrderCancelled------------>  notification
```

- Code: `OrderSagaService` - one method per reply. Each moves the order one state
  and writes the next message to the outbox **in the same transaction**.
- A reply that does not fit the state (late or duplicate) is ignored and sends
  nothing - e.g. `PaymentFailed` for a `CONFIRMED` order.
- Compensation (`ReleaseStock`) is fire-and-forget, as in the spec: no
  `StockReleased` reply. The outbox and the retries make sure it arrives; the
  release is naturally idempotent (only a `RESERVED` reservation is released).
- **Orchestration vs choreography:** here one service (order) tells the others
  what to do and decides the next step - the flow is in one class, easy to follow
  and debug. In **choreography** there is no conductor: each service reacts to
  the others' events (inventory hears `OrderPlaced`, payment hears
  `StockReserved`...). Fewer central parts, but the flow is spread over every
  service and harder to see.

### What I saw (end-to-end check)

- What I did: `.\scripts\e2e-check.ps1` twice - a Keyboard (50), a Laptop (1,500)
  and a sold-out mug.
- What I saw: all 7 checks passed both times.

  | Order | Final state | Time (1st run / 2nd run) |
  |---|---|---|
  | Keyboard | `CONFIRMED` | 3,088 ms / 1,372 ms |
  | Laptop | `CANCELLED / PAYMENT_FAILED`, stock 10 -> 10 | 999 ms / 996 ms |
  | Sold-out mug | `CANCELLED / OUT_OF_STOCK` | 492 ms / 374 ms |

  The Laptop order, followed through all four logs (partition 0 in every topic -
  same key):

  ```
  16.155 order         placed, total 1500.00
  16.274 inventory     reserved 1 of product 3, 9 left
  16.533 order         STOCK_RESERVED, ChargePayment 1500.00 sent
  16.797 payment       payment of 1500.00 FAILED
  17.139 order         CANCELLED (PAYMENT_FAILED), ReleaseStock sent
  17.321 notification  NOTIFICATION: order ... cancelled (PAYMENT_FAILED)
  17.327 inventory     released 1 of product 3, 10 left
  ```
- Why:
  - Each hop takes about 250 ms: the outbox relay runs every 500 ms, so a message
    waits 250 ms on average before it is sent. 4 hops for the Laptop = about 1 s.
  - Keyboard and Laptop both need **4 hops** to their final state (ReserveStock,
    StockReserved, ChargePayment, PaymentSucceeded/Failed). `OrderConfirmed` /
    `OrderCancelled` go out **after** the order is final, so the client sees the
    result before the notification fan-out happens. The warm-run gap (1.37 s vs
    1.0 s) is luck: where each hop lands in the relay's 500 ms cycle (0-500 ms
    wait per hop). The cold 3.1 s is JVM warm-up. The mug needs only 2 hops, so
    about half.
  - The notification and the stock release happen **in parallel**: they are
    different topics and different consumer groups.
  - The order says `CANCELLED` about 0.2 s **before** the stock is back -
    eventual consistency, accepted by the design.

### Interview answer

> "A saga is a chain of local transactions, one per service, connected by
> messages. When a step fails, the earlier steps are undone by compensating
> commands - here, payment fails and the order service sends ReleaseStock. I used
> orchestration: the order service is the conductor, so the whole flow is in one
> place. Every step goes through an outbox, and every consumer that changes data
> is idempotent - the notification log, on purpose, is not."

### Common follow-up questions

- *"Compensation vs rollback?"* - A rollback undoes an uncommitted transaction
  in one database. Compensation is a new business action that reverses a step
  that is **already committed** in another service.
- *"What if the compensation fails?"* - It is retried (outbox + error handler)
  and lands in the DLT if it never works - then someone must look. That is why
  compensations must be idempotent and simple.
- *"When choreography?"* - Few steps, teams that own their services and want no
  central coordinator. When the flow grows, orchestration is easier to change.
- *"How do you make it faster?"* - Here the relay's 500 ms poll is most of the
  time. Poll more often, or publish right after the commit and keep the poll as
  a safety net, or use CDC.

---

## 14. The asynchronous API (202 + polling)

### What it is

`POST /orders` answers **202 Accepted** with `status: PENDING` at once; the client
polls `GET /orders/{id}` until the status is final.

### Why it exists

The saga takes several messages and services. Holding the HTTP request open for
all of it would tie up threads and fail when any service is slow. 202 says "I
accepted your request, the work is still running".

### How it works here

`OrderController` returns 202 right after the order and its `ReserveStock` are
committed (section 8). `e2e-check.ps1` polls every 100 ms.

### What I saw

`POST` answers in about 20 ms (E4); the final state comes 0.4-1.4 s later (warm):
about 0.4 s for a 2-hop order (the mug), about 1-1.4 s for a 4-hop order
(section 13).

### Interview answer

> "For a long, multi-service action the API returns 202 with an id and the
> client polls a status endpoint - or gets a callback or push. The request is
> fast and does not depend on every service being up."

### Common follow-up questions

- *"Polling wastes requests?"* - Poll with back-off, or push the result
  (WebSocket, server-sent events, a webhook callback).

---

## 15. Fan-out and when idempotency is not needed

- **Fan-out**: `order-events` is read by notification-service in its **own
  consumer group**. Every group gets every message, so adding another reader (an
  analytics service, an e-mail service) needs no change in order-service.
- notification-service is **not idempotent** on purpose: a duplicate
  `OrderConfirmed` gives a duplicate log line - harmless. A duplicate charge is
  not harmless, so payment-service is idempotent twice over (processed-message
  table + one payment per order). Choose per consumer.

---

## 16. Shared message contracts - the trade-off

All services use the `messaging-contracts` module (topic names, message types,
payload records). Simple for one repo and one developer. In real systems a shared
module couples teams: a change forces everyone to rebuild and release together.
Teams often keep their own copy of the message classes, or use a **schema
registry** (Avro / JSON Schema / Protobuf) that checks every change stays
compatible.

**A contract bug I hit in M3:** money read back from a message lost its scale -
`100.00` came out as `100.0`, because Jackson reads a decimal inside a `JsonNode`
as a `double`. Doubles can also lose precision for money. The fix lives in one
place, the shared `EnvelopeCodec`: read decimals as `BigDecimal` and keep their
trailing zeros. A shared module made the fix easy; with copied classes every
team would have to find and fix it.

---

## 17. The API gateway

### What it is

One entry point in front of all services. The client calls only
`http://localhost:8080/api/...`; the gateway sends each request to the right
service (**routing**) and can do things every request needs (**filters**: logs,
auth, rate limits, CORS).

### Why it exists

Without it, clients must know every service's address and port, and every
service repeats the same cross-cutting code. The gateway hides the inside and
does that code once.

### How it works here

```
client --> api-gateway :8080 --/api/orders/**   (StripPrefix=1) --> order-service :8081 /orders/**
                             --/api/products/** (StripPrefix=1) --> inventory     :8082 /products/**
```

- Spring Cloud Gateway is **its own Spring Boot app** (`api-gateway`), not a
  library inside the services. It runs on **Netty** and is **reactive**
  (WebFlux): a few threads serve many requests, because no thread waits for a
  slow backend.
- Routes are configuration (`spring.cloud.gateway.server.webflux.routes` in
  `application.yml`) - no Java code.
- `AccessLogFilter` is a `GlobalFilter` (runs for every route): one line per
  request - method, path, status, duration. Reactive detail: `chain.filter(...)`
  returns at once; the line is written in `doFinally`, when the response is done.

### What I saw

- `POST /api/orders -> 202 in 296 ms` (first call, warm-up), then
  `GET /api/orders/<id> -> 200 in 17 ms`. The end-to-end check now goes through
  the gateway and passes.
- An unknown path (`/api/nothing`) is answered 404 but is **not** in the access
  log: global filters run only when a route matches.
- The gateway's log lines have an **empty trace id**: in reactive code the trace
  is carried in the Reactor context, not in the thread's logging context, so a
  plain `log.info` in `doFinally` cannot see it. (The trace itself is fine - the
  gateway spans are in Jaeger.)
- In Prometheus the gateway labels proxied requests `uri="UNKNOWN"` - it has no
  URI template like a controller has.

### Interview answer

> "An API gateway is the single entry point: it routes requests to the services
> and handles cross-cutting concerns like logging, auth and rate limiting in one
> place. I used Spring Cloud Gateway - a separate reactive Spring Boot app on
> Netty - with configured routes and one custom global filter for access logs."

### Common follow-up questions

- *"Gateway vs load balancer?"* - A load balancer spreads traffic over several
  instances of the same service (HAProxy, NGINX, AWS ALB). A gateway routes by
  path/headers to **different** services and runs filters. A gateway can also
  balance load (with service discovery); in production you often run several
  gateways **behind** a load balancer.
- *"Isn't the gateway a single point of failure?"* - Yes, so run more than one
  instance and keep it thin - no business logic.

---

## 18. Timeout, retry and circuit breaker (Resilience4j)

### What it is

- **Timeout**: stop waiting after a fixed time (here 2 s).
- **Retry**: try a failed call again (here up to 3 attempts, 200 ms apart).
- **Circuit breaker**: after too many failures, stop calling the sick service
  for a while and fail at once. States: **CLOSED** (normal) -> **OPEN** (fail
  fast) -> **HALF_OPEN** (let a few test calls through) -> CLOSED again.
- **Fallback**: what to do when the call fails - here a fast **503 "Product
  service unavailable, try again"**. It never guesses a price.

### Why it exists

order-service calls inventory over REST to get the price. If inventory hangs,
every order request hangs too, threads pile up and order-service dies as well
(a **cascading failure**). The breaker stops that and gives inventory time to
recover.

### How it works here

`ProductClient.getProduct` has `@Retry(name = "inventory", fallbackMethod =
"unavailable")` and `@CircuitBreaker(name = "inventory")`; the numbers are in
`application.yml`:

| Setting | Value |
|---|---|
| Timeout (connect + read) | 2 s (`inventory.timeout`, set on the RestClient) |
| Retry | 3 attempts, 200 ms wait, only on connection errors, timeouts, 5xx |
| Breaker window | last 10 calls, at least 10 calls, opens at 50% failures |
| Open for | 10 s, then HALF_OPEN by itself, 3 test calls |

- **Aspect order matters.** Resilience4j's default is `Retry( CircuitBreaker(
  call ) )`: the retry is **outside**, so the breaker counts **every attempt** -
  3 per request, so it opens after about 4 failed requests. The other style puts
  the breaker around the whole retry, `CircuitBreaker( Retry( call ) )`: one
  request counts once (opens later), but when it is open it also stops the
  retries at once. Know both.
- The fallback sits on `@Retry` (the outer one). On the inner `@CircuitBreaker`
  it would turn the error into a 503 **before** Retry sees it, and nothing would
  ever be retried.
- Only outages count: a **404** is an answer ("no such product"), so it is not
  retried, not counted by the breaker, and the client gets 400 "Product 99 not
  found".
- The state is on `/actuator/circuitbreakers`, and in Grafana.
- Build note: the Spring Cloud BOM also manages Resilience4j (older version).
  Mixed versions failed at start-up (`NoClassDefFoundError`); importing
  `resilience4j-bom` **before** the Spring Cloud BOM fixed it - the first BOM
  import wins.

### What I saw (E8)

- What I did: killed inventory, sent 8 orders through the gateway, started
  inventory again, sent 4 more.
- What I saw:

  | Request | Time | Answer | Breaker after |
  |---|---|---|---|
  | 1 | 609 ms | 503 | CLOSED |
  | 2 | 454 ms | 503 | CLOSED |
  | 3 | 244 ms | 503 | **OPEN** |
  | 4-8 | 31-40 ms | 503 | OPEN |
  | (inventory back, 10 s later) | | | HALF_OPEN |
  | 1-2 | 227, 48 ms | 202 | HALF_OPEN |
  | 3-4 | 47, 46 ms | 202 | **CLOSED** |

- Why:
  - Requests 1-2 tried 3 times each (connection refused is fast, so ~0.5 s
    including the 200 ms waits).
  - It opened during request 3, earlier than "about 4": the window of the last
    10 calls still held **successful** calls from the end-to-end check, so 50%
    was reached sooner.
  - Open = inventory is not called at all; 30-40 ms is just the gateway and
    order-service.
  - Prometheus showed `state="open"` = 1 at 08:23:15-20 and `half_open` at
    08:23:25-35 (one sample every 5 s).

### Interview answer

> "I protect the synchronous call with a 2-second timeout, a retry with only 3
> attempts for real outages, and a circuit breaker. When the breaker is open the
> call fails fast with a 503 instead of hanging - that stops a slow service from
> taking the caller down with it. In Resilience4j the default order is retry
> outside the breaker, so every attempt counts."

### Common follow-up questions

- *"Can retries make things worse?"* - Yes: a struggling service gets 3x the
  traffic (a **retry storm**). That is why retries need a small limit, a wait
  between them, and a breaker.
- *"How long should the timeout be?"* - Shorter than the caller's own timeout,
  and long enough for the normal slow case. Measure p99 latency first.
- *"What does the fallback return?"* - Something honest: an error the client can
  act on, a cached value, or a default. Never invent data like a price.

---

## 19. Tracing - Micrometer Tracing, OpenTelemetry, Jaeger

### What it is

- A **trace** is the story of one request across all services. It is a tree of
  **spans**; a span is one timed step (an HTTP call, a Kafka send, ...). All
  spans of a trace share one **trace id**.
- **Micrometer Tracing** is the Spring API for tracing; the **OpenTelemetry
  bridge** is the implementation under it; **OTLP** is the format used to send
  spans; **Jaeger** stores the spans and shows them on a timeline.

### Why it exists

One order touches 5 services and 4 topics. When it is slow or fails, logs of 5
apps are hard to join. A trace shows the whole path, with times, in one picture.

### How it works here

- Every service: `micrometer-tracing-bridge-otel` + `opentelemetry-exporter-otlp`,
  sampling 100%, spans sent to `http://localhost:4318/v1/traces` (Jaeger).
- **HTTP**: the trace id travels in the `traceparent` header - automatic for the
  gateway and for the RestClient call to inventory.
- **Kafka**: with `spring.kafka.template.observation-enabled` and
  `listener.observation-enabled`, the producer writes `traceparent` into the
  record headers and the consumer continues the trace.
- **Across the outbox** (the hard part): the outbox row is sent later, by the
  relay's own thread, so the trace would stop at the database. `OutboxTracing`
  stores the current `traceparent` in `outbox_event.trace_parent` when the row is
  written, and the relay starts an "outbox publish ..." span **from that value**
  when it sends the row. The Kafka producer span becomes its child and the trace
  goes on.
- Every log line has `[traceId-spanId]`, so a log line leads to its trace.
- Noise found and removed: each relay poll (every 500 ms in 3 services) created
  its own trace and buried the real ones in Jaeger's search. Fixed with
  `management.observations.enable.tasks.scheduled.execution: false`. (Each
  Prometheus scrape still creates a small `/actuator/prometheus` trace - filter
  it out in the Jaeger search.)

### What I saw (E9)

- What I did: placed one Keyboard order through the gateway, took the trace id
  from the order-service log line, opened it in Jaeger
  (`/api/v3/traces/<id>` - Jaeger 2.x's query API).
- What I saw: **one trace, 20 spans, all 5 services, 3.4 s**:

  ```
      0 ms  api-gateway          http post                      297 ms
      8     order-service        http post /orders               287
     98     order-service          http get (RestClient)          77
    147     inventory-service        http get /products/{id}       7
    641     order-service        outbox publish ReserveStock     73
    648     order-service          inventory-commands send       67
    731     inventory-service        inventory-commands receive  340
   1514     inventory-service    outbox publish StockReserved    75
   1593     order-service          saga-replies receive         317
   2275     order-service        outbox publish ChargePayment     6
   2295     payment-service          payment-commands receive   368
   2868     payment-service      outbox publish PaymentSucceeded 66
   2931     order-service          saga-replies receive          15
   3315     order-service        outbox publish OrderConfirmed    6
   3336     notification-service     order-events receive         48
  ```
- Why:
  - The trace crosses every outbox: each "outbox publish" span continues the
    trace stored in the row.
  - The **gaps** before each "outbox publish" (e.g. 1071 -> 1514 ms) are the
    time the row waited for the relay's next poll (every 500 ms) - exactly the
    latency explained in section 13, now visible.
  - The first receives are slow (300+ ms) because this was the first order after
    start (JVM warm-up); later orders are faster.

### How to read a Jaeger timeline

Each row is a span; indentation = parent/child; the bar's length = time; empty
space between bars = waiting. Click a span to see its tags (topic, partition,
HTTP status, error).

### Interview answer

> "I use Micrometer Tracing with the OpenTelemetry bridge and send spans over
> OTLP to Jaeger. The trace id travels in the traceparent header over HTTP and in
> Kafka record headers. The outbox breaks the trace because the relay sends the
> message later, so I store the traceparent in the outbox row and continue it
> when the relay publishes."

### Common follow-up questions

- *"Sampling in production?"* - Keep only a few percent of traces (cost), or use
  tail sampling to keep the slow and failed ones.
- *"Traces vs logs?"* - See section 21.

---

## 20. Metrics - Prometheus and Grafana

### What it is

- A **metric** is a number over time (requests per second, rows waiting).
  **Labels** split it (`job="order-service"`, `status="503"`).
- **Prometheus** collects and stores metrics. It **pulls** ("scrapes"): every 5 s
  it calls each service's `/actuator/prometheus`.
- **Grafana** draws dashboards. It stores no metrics: a **data source**
  (Prometheus) answers its queries, and each **panel** shows one query.

### Why it exists

Metrics answer "is something wrong right now?" cheaply - and they can trigger
alerts. A dashboard shows the health of all services on one screen.

### How it works here

- Every service: `micrometer-registry-prometheus`; `prometheus/prometheus.yml`
  lists the 5 services as jobs (`host.docker.internal:808x`, because Prometheus
  runs in Docker and the services on the host).
- Grafana is **provisioned from files** in `grafana/` (data source + dashboard
  JSON) - no clicking. Dashboard: `http://localhost:3000/d/kafka-demo`.
- Metric types: **counter** (only goes up - requests), **gauge** (up and down -
  rows waiting), **histogram** (buckets of durations - for percentiles).
- Custom gauge `outbox_unpublished` (`OutboxMetrics`): rows not sent yet, read on
  every scrape. Near 0 = healthy; growing = Kafka unreachable.
- Kafka consumer lag comes from the Kafka client metrics
  (`kafka_consumer_fetch_manager_records_lag`), circuit breaker state from
  Resilience4j (`resilience4j_circuitbreaker_state`).
- The six panels and their PromQL:

  | Panel | Query | Meaning |
  |---|---|---|
  | Requests/s | `sum by (job) (rate(http_server_requests_seconds_count[1m]))` | per-second rate of the counter over the last minute, one line per service |
  | p95 latency | `histogram_quantile(0.95, sum by (le, job) (rate(http_server_requests_seconds_bucket[1m])))` | 95% of requests were faster than this |
  | 5xx/s | same as requests, with `{status=~"5.."}` | errors per second |
  | Consumer lag | `sum by (job, topic) (kafka_consumer_fetch_manager_records_lag)` | messages waiting per consumer group and topic |
  | Breaker state | `resilience4j_circuitbreaker_state{name="inventory"}` | the current state's line is 1 |
  | Outbox | `outbox_unpublished` | rows waiting per service |

### What I saw

- All 5 targets `up`; `outbox_unpublished` = 0 in order, inventory and payment.
- During E8 the breaker panel showed OPEN, then HALF_OPEN, then CLOSED.
- **Gotcha:** the 5xx panel stayed flat during E8, although order-service
  counted 8 responses with status 503. The `status="503"` series did not exist
  before the outage, so its first value (8) is never seen as an increase -
  `rate()` and `increase()` need two samples of the **same** series. Systems that
  care pre-create such series at 0, or alert on the total instead.

### Interview answer

> "Each service exposes Micrometer metrics on /actuator/prometheus; Prometheus
> scrapes them every few seconds; Grafana shows dashboards with PromQL queries -
> request rate, p95 latency, errors, Kafka consumer lag, circuit breaker state,
> and a custom gauge for outbox rows waiting."

---

## 21. Traces vs metrics vs logs

| | Answers | Cost | Here |
|---|---|---|---|
| **Metrics** | "Is something wrong? How much?" | cheap - just numbers | Prometheus + Grafana, alerts |
| **Traces** | "Where is it slow or failing, for this request?" | medium - sampled | Jaeger |
| **Logs** | "What exactly happened?" | expensive - text | log files, joined to a trace by the trace id |

Usual flow: a metric or alert says something is wrong -> a trace shows which
service and step -> the logs of that step (found by trace id) show the details.
Central log storage (ELK, Loki) is out of scope here.

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
| Saga | One business action as a chain of local transactions, one per service |
| Orchestration | One service (the conductor) tells the others what to do next |
| Choreography | No conductor: each service reacts to the others' events |
| Compensation | A new command that undoes an already-committed step (here ReleaseStock) |
| Fan-out | One topic read by several consumer groups; each gets every message |
| 202 Accepted | "Request accepted, work still running" - the client polls for the result |
| API gateway | Single entry point that routes requests to services and runs shared filters |
| Circuit breaker | Stops calling a failing service for a while and fails fast (CLOSED / OPEN / HALF_OPEN) |
| Half-open | Breaker state that lets a few test calls through to see if the service is back |
| Retry | Trying a failed call again, a limited number of times |
| Timeout | The longest time to wait for an answer |
| Fallback | What to return or throw when the protected call fails |
| Trace | The path of one request across services, made of spans |
| Span | One timed step of a trace |
| traceparent | The W3C header that carries the trace id and parent span id |
| OpenTelemetry (OTel) | The standard (and libraries) for traces, metrics and logs |
| OTLP | OpenTelemetry's protocol for sending telemetry (here spans to Jaeger) |
| Jaeger | Stores traces and shows them on a timeline |
| Prometheus | Scrapes and stores metrics; queried with PromQL |
| Scrape | Prometheus pulling metrics from an endpoint |
| Metric / label | A number over time / a key=value that splits it |
| PromQL | Prometheus' query language (rate, sum by, histogram_quantile) |
| Gauge | A metric that goes up and down (here outbox_unpublished) |
| Grafana | Dashboards over data sources like Prometheus |
| Panel | One chart on a Grafana dashboard |

---

## Coming in the next milestones

- M5: interview Q&A collected from every section, and next steps.

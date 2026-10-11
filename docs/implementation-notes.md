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
- **`.gitattributes` keeps `mvnw` with LF line endings.** Git on this PC has
  `core.autocrlf=true`, which would otherwise check the bash script out with
  CRLF and break it.
- **Disk cost of the M1 infrastructure** (`docker system df`): images before
  2.515 GB, after 2.956 GB (+441 MB unpacked). New images:
  `apache/kafka-native:4.3.1`, `redpandadata/console:v3.12.0`;
  `postgres:17.4` reused.
- **Docker Hub pulls fail on the office network.** Layer downloads from
  `production.cloudfront.docker.com` are cut with `EOF` on every retry (the
  network's direct-egress filter). Workaround: pull from Google's public Docker
  Hub mirror and tag with the normal name - the content digest is identical,
  so `docker-compose.yml` does not change:
  `docker pull mirror.gcr.io/apache/kafka-native:4.3.1`
  `docker tag mirror.gcr.io/apache/kafka-native:4.3.1 apache/kafka-native:4.3.1`
  Checked: both digests match Docker Hub exactly. M4's images (Jaeger,
  Prometheus, Grafana) will need the same trick on this network.
- Redpanda Console starts faster than Kafka, logs "connection refused" once,
  and connects one second later - harmless.
- **`apache/kafka-native:4.3.1` works with Testcontainers' `KafkaContainer`**
  and Spring Boot's `@ServiceConnection` (checked in Task 4). Tests reuse the
  same images as Docker Compose.
- **Errors use RFC 9457 Problem Details** (`spring.mvc.problemdetails.enabled`).
  Found while running the controller test without the controller: a plain 404
  test also passes when the endpoint does not exist at all, so the tests now
  check the error's `detail` message too.
- **Redpanda Console can produce messages** (checked in Task 7): Topics ->
  inventory-commands -> Produce Record (key and value as Text). M2 experiments
  E2 and E3 use it.
- **M1 has no dead-letter topic yet.** A broken record (key `test`, value
  `hello`) was received 10 times in a row (Spring Kafka's default: 1 try + 9
  retries, no wait), logged as `InvalidMessageException`, then skipped - so it
  is lost, with only an ERROR log line left. M2 marks `InvalidMessageException` as not retryable and
  sends it to `inventory-commands.DLT` instead.
- **Final review changed one thing the plan prescribed.** A
  `StockReservationFailed` that arrives after `StockReserved` used to cancel the
  order (the plan's `cancel` accepts `STOCK_RESERVED`). Spec section 5 says a reply
  that does not fit the state is ignored, so the saga now calls
  `Order.markStockReservationFailed`, which only acts on a `PENDING` order.
  `cancel` keeps accepting `STOCK_RESERVED`, because M3 needs it for
  `PAYMENT_FAILED`. Test: `aLateStockReservationFailedDoesNotCancelAReservedOrder`.
- **The consumer-side dual write duplicates, it does not lose.** The first
  LEARNINGS draft said a crash between the stock commit and the reply loses the
  reply. Wrong: the offset is not committed, the command is redelivered, and the
  stock is reserved twice (the `stock_reservation` primary key does not stop it,
  because `save()` with an assigned id merges). LEARNINGS section 4 and the
  listener comment are corrected.
- **Notes for the M2 plan, from the final review:**
  - Make the reservation insert-only (`Persistable.isNew()` or a `findById`
    check), as a second guard next to the processed-message table.
  - An optimistic-lock conflict must not use up the retry budget and land in the
    DLT: retry it inside `StockService`, or classify it apart from broken input.
  - `EnvelopeCodec.fromJson` throws `IllegalArgumentException` / NPE (not
    `InvalidMessageException`) for a null value (tombstone) or the text `null`;
    fix it before marking `InvalidMessageException` as not retryable.
  - E4 "before" picture: with Kafka down, `POST /orders` blocks (10 s `get`, up
    to 60 s `max.block.ms`), returns 500, but the order is already saved as
    `PENDING` and the record may still be delivered later (reviewer reading - check in E4).
  - `scripts/stop-services.ps1` always force-kills (Windows `Stop-Process`), so a
    stopped instance holds its partitions for up to 45 s. Ask before adding a
    graceful stop (for example the actuator shutdown endpoint).

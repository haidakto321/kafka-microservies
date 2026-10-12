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
- **Notes for the M2 plan, from the final review** (done in M2 except the graceful stop - see the M2 section):
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

## M2

Decisions made in the M2 plan (`docs/superpowers/plans/2026-10-11-m2-reliability.md`):

- **Outbox / dedupe classes are copied per service**, not shared: `messaging-contracts` stays "records only", and each service owns its tables. About 5 small classes per service.
- **No `trace_parent` column yet** - nothing fills it until M4 (tracing); M4 adds it.
- **No `ErrorHandlingDeserializer`** - values are plain strings; a JSON error is our `InvalidMessageException`, marked not retryable.
- **Lock conflicts are retried in the listener** (3 immediate attempts) before the error handler sees them, so a valid command does not reach the DLT under load.
- **`StockReservation` and `ProcessedMessage` are insert-only** (`Persistable.isNew()`): a second row for the same key fails on the primary key.
- **The relay stops at the first failed send** and commits what it sent. Cost: one unsendable row holds back the rows behind it.
  The plan said "Kafka down = one failure per run, no duplicates" - only half true (see E4b below).
- **The demo delay pauses after the DB commit, before the offset commit** - so E1 shows the duplicate skip, not just a rollback.
- **Test Kafka has topic auto-create off**, like compose.
- **Producer `max.block.ms: 5000`** so a Kafka outage does not hold the relay's transaction for 60 s.

Changed during the build:

- **The DLT name is set explicitly.** The plan assumed Spring Kafka's `DeadLetterPublishingRecoverer` writes to `<topic>.DLT`; in Spring Kafka 3.3.10 the default is `<topic>-dlt`. With auto-create off, publishing failed and the partition was stuck re-reading the record. Fixed with an explicit destination resolver (`record.topic() + Topics.DLT_SUFFIX`), keeping the spec's name. Caught only because the test broker has auto-create off.
- **`@MockitoBean` by type, not by name**, for `KafkaTemplate` in `OutboxRelayTest`: replacing the bean by name fails because Spring Boot disables bean-definition overriding.
- `OrderControllerTest.anUnknownProductIs400AndSavesNothing` now also checks that no outbox row was written (M1 review minor M3).

Experiment notes (details in LEARNINGS sections 8-11):

- E4: orders accepted in 20-24 ms with Kafka down; the relay sent the 3 waiting rows 0.5 s after Kafka came back.
- E1: after the kill, the partitions came back only after ~47 s (session timeout of the killed member), then the same offset was redelivered and skipped as a duplicate.
- E3: Redpanda Console's Murmur2 partitioner put the copies on the same partition as the Java producer.
- E2: the DLT record is 14 bytes but carries ~6 KiB of headers (mostly the stack trace).
- E6: one consumer thread with a 200 ms delay drained 500 messages at 4.6 per second (110 s).
- PostgreSQL `jsonb` reorders the payload keys; harmless.

Final review (fresh reviewer on the whole M2 branch):

- **E4b - Kafka down after the producer has metadata.** `max.block.ms` only bounds the metadata wait. With metadata cached, `send()` buffers the record, `.get(10 s)` times out, and the buffered copy stays alive for `delivery.timeout.ms` (120 s). Checked for real: 3 failed relay runs -> 4 copies delivered when Kafka came back, 3 skipped as duplicates. Data correct (idempotent consumer). LEARNINGS section 8 now explains both cases. Not changed in code: setting `delivery.timeout.ms` below the `.get` timeout would stop the extra copies - left as a note.
- The relay logged `will try again: null` for a timeout (a `TimeoutException` has no message). It now logs `e.toString()`; pinned by `OutboxRelayWarningTest`.
- LEARNINGS sections 2 and 3 still described M1 classes and rules as current; now marked "Since M2".
- Still open from the M1 notes: a graceful stop script (owner's decision), so E1 and every scripted stop still wait ~45 s for the session timeout.

## M3

Decisions made in the M3 plan (`docs/superpowers/plans/2026-10-12-m3-saga.md`):

- **Compensation is fire-and-forget** (no `StockReleased` reply, as in the spec's topic table). On `PaymentFailed` the order is cancelled and `ReleaseStock` + `OrderCancelled` are written in one transaction. The order shows `CANCELLED` ~0.2 s before the stock is back.
- **Every service declares a `NewTopic` for every topic its outbox writes to** (order-service now also creates payment-commands and order-events) - one missing topic would block the whole outbox (M2 review).
- **payment-service copies the M2 classes** (outbox, processed message, error handler, scheduling) - database per service. No lock-conflict retry: no shared rows.
- **Payment rule in `Payment.charge`**: `amount <= 1000.00` succeeds, more fails with `AMOUNT_OVER_LIMIT`. One payment per order, insert-only.
- **A `ChargePayment` without an amount is invalid data** -> `InvalidMessageException` -> straight to `payment-commands.DLT`.
- **notification-service has no database and is not idempotent** (spec 6.2), but has the shared error handler and `order-events.DLT`.
- **`OrderSagaService` keeps one method per reply type**, each joining the handler's transaction.
- **`scripts/e2e-check.ps1`** is the spec's end-to-end check.

Changed during the build:

- **Money lost its scale in messages.** `EnvelopeCodec` read decimals inside the `JsonNode` payload as `double`, so `100.00` arrived as `100.0` (and a double can lose precision for money). Found by the notification test, which expected `total 100.00`. Fixed in `EnvelopeCodec`: a copy of the mapper with `USE_BIG_DECIMAL_FOR_FLOATS` on and `STRIP_TRAILING_BIGDECIMAL_ZEROES` off. Pinned by `aMoneyAmountKeepsItsExactValueAndScale` (contracts now 11 tests).

End-to-end results (details in LEARNINGS section 13):

- Both runs: all 7 checks passed. Warm run: Keyboard `CONFIRMED` in 1.37 s, Laptop `CANCELLED / PAYMENT_FAILED` in 1.0 s with stock 10 -> 10, mug `CANCELLED / OUT_OF_STOCK` in 0.37 s. Keyboard and Laptop both take 4 hops to their final state; the gap is where each hop lands in the 500 ms relay cycle (final review corrected an earlier "more hops" explanation).
- Each saga hop costs ~250 ms, mostly the relay's 500 ms poll.

## M4

Decisions made in the M4 plan (`docs/superpowers/plans/2026-10-12-m4-patterns.md`):

- **Versions:** Spring Cloud 2025.0.3 (Gateway 4.3.5), Resilience4j 2.4.0; images `jaegertracing/jaeger:2.21.0`, `prom/prometheus:v3.14.0`, `grafana/grafana:12.4.11`, pulled through `mirror.gcr.io` (Docker Hub still blocked here), digests equal to Docker Hub. Images on disk: 2.99 GB -> 4.69 GB.
- **Gateway artifact `spring-cloud-starter-gateway-server-webflux`**, routes under `spring.cloud.gateway.server.webflux.routes`.
- **Aspect order = Resilience4j default** (Retry outside the circuit breaker); the fallback is on `@Retry`.
- **Retry and breaker count only outages** (connection errors, timeouts, 5xx); a 404 stays a 400.
- **`minimum-number-of-calls: 10`** (the default 100 would never open in a demo).
- **Timeout on the RestClient** via a `RestClientCustomizer` (2 s), so the `MockRestServiceServer` unit test still works.
- **Outbox trace context in `OutboxTracing`** (one per service): stores `traceparent` with the row, the relay continues it.
- **OTLP to `localhost:4318`, sampling 100%**; tests keep tracing off except `OutboxTracingTest`.
- **The outbox gauge is unit-tested** with a mocked repository; its export was checked live.
- **Prometheus labels services by `job`**; HTTP histograms on for the p95 panel.
- **`e2e-check.ps1` goes through the gateway.**

Changed during the build:

- **Resilience4j versions were mixed.** The Spring Cloud BOM manages Resilience4j modules at 2.2.0; pinning only `resilience4j-spring-boot3` 2.4.0 gave `NoClassDefFoundError` at start-up. Fixed by importing `resilience4j-bom` 2.4.0 before the Spring Cloud BOM (first import wins).
- **Jaeger 2.21's query API is `/api/v3/...`** (`/api/services` is 404).
- **Trace noise:** every outbox relay poll (500 ms, 3 services) became its own trace. Turned off with `management.observations.enable.tasks.scheduled.execution: false` in order, inventory and payment. Scrape traces (`/actuator/prometheus`) remain - filter them in the Jaeger search.
- Two test fixes (not product bugs): the JDK `HttpServer` fakes need a thread pool (one thread queues slow requests into the next test); the gateway log test waits for the line (it is written in `doFinally`, after the response).

Experiment findings (details in LEARNINGS sections 17-20):

- E9: one trace, 20 spans, 5 services, ~3.3 s, crossing every outbox; the gaps before each "outbox publish" span are the relay's poll wait.
- E8: breaker opened during the 3rd failed request (the window still held earlier successes), then 503 in ~35 ms; HALF_OPEN after 10 s, CLOSED after 3 good calls.
- PromQL gotcha: the 503 series was born during the outage, so `increase()` showed 0 although the counter was 8 - the 5xx panel stayed flat.
- Gateway: unknown paths (no route) are not access-logged; reactive log lines carry no trace id; proxied requests have `uri="UNKNOWN"` in metrics.
- `OutboxTracingTest` exports a "test request" trace to Jaeger when Jaeger is running during the build - harmless.

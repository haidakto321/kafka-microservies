# Kafka microservices demo - design spec

Date: 2026-10-11
Status: draft, waiting for review
Location: `D:\project\kafka-microservices-demo`

## 1. Goal

A personal learning project, built in about 1-2 weeks, to learn **Kafka** and
common **microservice patterns** with Spring Boot - for Java backend job
interviews (Kafka / RabbitMQ is "nice to have" in the target job description;
"Java microservices" is required).

The owner has **never used Kafka in production**. He has used Redis queues and
pub/sub, REST between services, Resilience4j and Docker. Everything here should
be learnable by doing, and explained in simple English.

### Success criteria

1. `docker compose up` starts the infrastructure (Kafka, PostgreSQL, UIs,
   monitoring), and one start script runs the 5 services as normal Java apps -
   all on one Windows PC.
2. An order goes to `CONFIRMED` (happy path), to `CANCELLED / OUT_OF_STOCK`, and
   to `CANCELLED / PAYMENT_FAILED` with the stock put back.
3. All experiments E1-E9 (section 9) have been run, and what was seen is
   written in `docs/LEARNINGS.md`.
4. `./mvnw test` is green (unit tests + Testcontainers integration tests).
5. `README.md` explains how to run it and shows the architecture.
6. `docs/LEARNINGS.md` lets the owner prepare for an interview **without
   opening the code** (section 10).
7. The owner can describe the project in about 2 minutes in English.

### Not included (deliberately)

- Production quality, high availability, security hardening.
- Login / JWT / Keycloak (the owner already knows it from work).
- Service discovery (Eureka), config server, Kubernetes, AWS deployment.
- RabbitMQ (Kafka only).
- A front end. Clients are curl / Postman / PowerShell scripts.
- Debezium / Kafka Connect (the outbox is written by hand - see 6.1).

These go to a "Next steps" list in `docs/LEARNINGS.md`.

## 2. Decisions made during brainstorming

| Topic | Decision | Why |
|---|---|---|
| Size | 1-2 weeks: Kafka core + microservice patterns | Covers most interview questions |
| Business domain | E-commerce: order -> stock -> payment | Interviewers know it; lots of help online |
| Broker | Kafka only | The JD names Kafka; one broker learned deeply |
| Where it runs | Only on the owner's PC. Infrastructure in Docker Compose; the 5 services run as normal Java apps (IDE or start script), **not** as Docker images | Free; saves disk (no service images or build cache) and restarts fast; owner already has real AWS experience to talk about |
| Saga style | **Orchestration** (order-service is the conductor) | Flow visible in one place; easiest to debug and explain |
| Outbox | Own implementation with a scheduled relay | Teaches the mechanism; no extra infrastructure |
| Monitoring | **Jaeger** (traces) + **Prometheus** (metrics) + **Grafana** (dashboard) | Common in job descriptions; about half the size of the all-in-one Grafana image |
| Images | Light images only: `apache/kafka-native`, Redpanda Console; **reuse the `postgres:17.4` image already on the PC** (own container, host port 5433) | The PC has ~29 GB free on C: (92% used). New Docker downloads about 620 MB |
| Gateway | Spring Cloud Gateway (reactive) | Java/Spring, custom filters in Java |

## 3. Architecture

```
                       Client (curl / Postman / scripts)
                               |
                     api-gateway  :8080
                      /                \
            order-service           inventory-service
             :8081  (saga)  --REST-->  :8082   (GET /products/{id})
                 \                  /
                  \     KAFKA      /         payment-service :8083
                   +--------------+------------  /
                          |                     /
                   notification-service :8084

   The 5 services run on the host as normal Java apps.

   In Docker Compose:
   PostgreSQL :5433 - own container (reusing the postgres:17.4 image already on
                      the PC), 3 databases: order_db, inventory_db, payment_db
   Kafka :9092 (one broker, KRaft, apache/kafka-native)
   Redpanda Console :8090 (Kafka web UI)
   Jaeger UI :16686   Prometheus :9090   Grafana :3000
```

### Services

| Service | Responsibility | Database |
|---|---|---|
| `api-gateway` | Single entry point; routes `/api/orders/**` -> order-service and `/api/products/**` -> inventory-service; one custom access-log filter | none |
| `order-service` | `POST /orders`, `GET /orders/{id}`; runs the saga; owns the order state | `order_db` |
| `inventory-service` | Products and stock; `GET /products/{id}`; reserves / releases stock on command | `inventory_db` |
| `payment-service` | Fake payment; fails on purpose when the total is over 1,000 | `payment_db` |
| `notification-service` | Listens to final order events and logs a "notification" | none |

### Repo layout

One repo, Maven multi-module, Maven Wrapper (`./mvnw`):

```
kafka-microservices-demo/
  pom.xml                     parent: versions, plugins, modules
  messaging-contracts/        records only: commands, events, envelope, topic names
  api-gateway/
  order-service/
  inventory-service/
  payment-service/
  notification-service/
  docker-compose.yml          infrastructure only
  prometheus/prometheus.yml   which services Prometheus scrapes
  grafana/                    provisioned data source + dashboard JSON
  scripts/                    PowerShell: start the services, run the experiments
  docs/
    LEARNINGS.md              the learning notes (section 10)
    implementation-notes.md   decisions and deviations during the build
    superpowers/specs/, superpowers/plans/
  README.md
```

`messaging-contracts` is shared on purpose to keep the demo simple. Trade-off
(write it in LEARNINGS): in real systems a shared module couples teams; they
often keep their own copies of message classes or use a schema registry.

## 4. Messaging design

### Topics

All topics: **3 partitions**, replication factor 1 (single broker - LEARNINGS
must explain that production uses 3 replicas and `min.insync.replicas=2`).
Topics are created by `NewTopic` beans at start-up.

| Topic | Producer -> consumer | Messages |
|---|---|---|
| `inventory-commands` | order -> inventory | `ReserveStock`, `ReleaseStock` |
| `payment-commands` | order -> payment | `ChargePayment` |
| `saga-replies` | inventory, payment -> order | `StockReserved`, `StockReservationFailed`, `PaymentSucceeded`, `PaymentFailed` |
| `order-events` | order -> any listener | `OrderConfirmed`, `OrderCancelled` |
| `<topic>.DLT` | error handler | messages that failed all retries |

### Key

Every message's **Kafka key is the `orderId`**, so all messages for one order go
to the same partition and are processed in order.

### Message envelope (JSON)

```json
{
  "messageId": "uuid",
  "type": "ReserveStock",
  "orderId": "uuid",
  "occurredAt": "2026-10-11T10:00:00Z",
  "payload": { "productId": 1, "quantity": 2 }
}
```

`messageId` is used for duplicate detection. Trace context travels in Kafka
headers (W3C `traceparent`), added by the tracing setup.

## 5. Order flow

### Happy path

1. Client `POST /api/orders {productId, quantity}` -> gateway -> order-service.
2. order-service calls inventory `GET /products/{id}` over REST (with circuit
   breaker, 7.2) to get the price and check the product exists.
3. In **one transaction**: save the order as `PENDING` + an outbox row
   `ReserveStock`. Answer **202 Accepted** `{orderId, status: "PENDING"}`. The
   API does not wait for the saga.
4. The outbox relay publishes `ReserveStock` -> inventory reserves the stock and
   replies `StockReserved`.
5. order-service: `STOCK_RESERVED`, sends `ChargePayment`.
6. payment-service: total <= 1,000 -> replies `PaymentSucceeded`.
7. order-service: `CONFIRMED`, publishes `OrderConfirmed`.
8. notification-service logs "Order X confirmed".
9. Client `GET /api/orders/{id}` -> `CONFIRMED`.

### Failure 1 - not enough stock

inventory replies `StockReservationFailed` -> order `CANCELLED`, reason
`OUT_OF_STOCK` -> `OrderCancelled`. Nothing to undo.

### Failure 2 - payment fails (compensation)

payment replies `PaymentFailed` -> order-service sends `ReleaseStock` ->
inventory puts the stock back -> order `CANCELLED`, reason `PAYMENT_FAILED` ->
`OrderCancelled`.

### Order states

`PENDING -> STOCK_RESERVED -> CONFIRMED`, or `-> CANCELLED` with a reason
(`OUT_OF_STOCK`, `PAYMENT_FAILED`). A reply that doesn't fit the current state
(e.g. a late duplicate) is logged and ignored.

### Seed data (inventory)

| Product | Price | Stock | Purpose |
|---|---|---|---|
| Keyboard | 50 | 100 | Happy path |
| Monitor | 300 | 5 | Run out of stock by ordering many |
| Laptop | 1,500 | 10 | Always fails payment -> compensation |
| Sold-out mug | 10 | 0 | Always out of stock |

## 6. Reliability

### 6.1 Outbox

- Table `outbox_event` in order, inventory and payment databases.
- The business change and the outbox row are written in **one explicit
  `@Transactional`** method.
- A relay (`@Scheduled`, fixed delay ~500 ms) selects up to 100 unpublished rows
  with `SELECT ... FOR UPDATE SKIP LOCKED`, sends each to Kafka, **waits for the
  broker's confirmation**, then sets `published_at`.
- Kafka down: rows wait and are sent later. Crash after send but before marking:
  the row is sent **again** - accepted, because consumers are idempotent.
- `SKIP LOCKED` lets several instances run the relay without picking the same
  rows.
- A partial index on unpublished rows keeps the relay query fast.
- Trade-off to note in LEARNINGS: the relay holds a DB transaction while sending
  to Kafka; fine for the demo, worth discussing in an interview.

### 6.2 Idempotent consumer

- Table `processed_message (message_id PK, processed_at)` in inventory, payment
  and order.
- In the **same transaction** as the business change, insert the `messageId`;
  if it already exists, skip the message.
- Natural idempotency where possible too (e.g. `ReleaseStock` only releases a
  reservation that is still `RESERVED`).
- notification-service is **not** idempotent on purpose - a duplicate log line
  is harmless. LEARNINGS point: not every consumer needs idempotency; a
  duplicate charge is a problem, a duplicate log line is not.

### 6.3 Kafka client settings

- Producer: `acks=all`, idempotent producer enabled, key = `orderId`.
- Consumer: one consumer group per service; offsets committed **after**
  processing (at-least-once); ack mode per record.

### 6.4 Retries and dead-letter topic

- Spring Kafka `DefaultErrorHandler` with exponential back-off: 3 retries
  (1 s, 2 s, 4 s), then `DeadLetterPublishingRecoverer` -> `<topic>.DLT`.
- Errors that can never succeed (bad JSON, invalid data) are marked
  **not retryable** and go straight to the DLT.
- `ErrorHandlingDeserializer` wraps the JSON deserializer so one broken message
  cannot block a partition forever.
- Trade-off (LEARNINGS): these retries **block the partition** while retrying,
  which keeps per-order ordering. `@RetryableTopic` does not block but breaks
  ordering - wrong choice for a saga.

### 6.5 Demo setting

`demo.processing-delay-ms` on inventory-service (default 0) adds a pause while
processing, so a consumer can be killed mid-message (E1) or slowed down (E6).

## 7. Microservice patterns

### 7.1 API gateway

- Spring Cloud Gateway (reactive, runs on Netty), its own Spring Boot app.
- Routes defined in `application.yml`.
- One custom `GlobalFilter` written by hand: logs method, path, status and
  duration of every request.

### 7.2 REST call with Resilience4j

- order-service -> inventory `GET /products/{id}` using Spring's `RestClient`.
- Client timeout: 2 s.
- Retry: up to 3 attempts (1 call + 2 retries), only for connection errors,
  timeouts and 5xx.
- Circuit breaker: sliding window of 10 calls, opens at 50% failures, stays open
  10 s, then half-open with 3 test calls.
- Fallback: fail **fast** with 503 "Product service unavailable, try again". It
  never guesses a price.
- LEARNINGS note: Resilience4j's default annotation order puts **Retry outside**
  the circuit breaker (each retry is counted by the breaker). At the owner's
  current job the breaker wraps the whole retry instead (a retried call counts
  as one outcome). Explain both.
- State visible on `/actuator/circuitbreakers` and in Grafana.

### 7.3 Tracing and metrics

- **Tracing:** Micrometer Tracing with the OpenTelemetry bridge, exported over
  OTLP to **Jaeger** (web UI on :16686). Sampling 100% (demo).
- Trace context flows through HTTP automatically and through Kafka with
  `spring.kafka.template.observation-enabled` and
  `spring.kafka.listener.observation-enabled`.
- **Outbox breaks the trace** (sent later, from another thread). Fix: store the
  W3C `traceparent` in the outbox row when writing it, and continue that trace
  when publishing. Must be shown in E9 and explained in LEARNINGS.
- **Metrics:** Micrometer with the Prometheus registry; each service exposes
  `/actuator/prometheus`. **Prometheus** (in Docker) scrapes the services on the
  host through `host.docker.internal`, every 5 s.
- **Grafana** with a provisioned Prometheus data source and dashboard
  (`grafana/`): request rate / latency / errors per service, Kafka consumer
  lag, circuit breaker state, and a **custom gauge**: the number of unpublished
  outbox rows.
- **Logs:** every line includes `traceId` and `spanId`, so a log line can be
  matched to its trace in Jaeger. Central log storage is out of scope.

### 7.4 Health

Actuator health (liveness / readiness). Docker Compose health checks on Kafka
and PostgreSQL; the start script waits until they are healthy before starting
the services.

## 8. Data model

**order_db**
- `orders` (id uuid PK, product_id, quantity, unit_price, total, status,
  cancel_reason, created_at, updated_at, version) - `version` for optimistic
  locking.
- `outbox_event` (id uuid PK = messageId, aggregate_id, topic, type,
  payload jsonb, trace_parent, created_at, published_at)
- `processed_message` (message_id PK, processed_at)

**inventory_db**
- `product` (id PK, name, price, stock, version)
- `stock_reservation` (order_id PK, product_id, quantity, status
  RESERVED/RELEASED, created_at)
- `outbox_event`, `processed_message`

**payment_db**
- `payment` (order_id PK, amount, status SUCCEEDED/FAILED, reason, created_at)
- `outbox_event`, `processed_message`

Schema changes through Flyway migrations, one set per service.

## 9. Experiments

Each experiment has a script or clear steps in `scripts/`, and its result is
written in LEARNINGS (section 10 format).

| # | Do this | Expected |
|---|---|---|
| E1 | Kill inventory while it processes (delay setting on) | Message delivered again after restart; stock reserved only once |
| E2 | Send a broken message from Redpanda Console | Retries in logs, then the DLT; other orders keep flowing |
| E3 | Send the same message twice (Redpanda Console) | Processed once |
| E4 | Stop the Kafka container, create orders, start it again | Orders still 202; all complete when Kafka is back (outbox) |
| E5 | Start 3 inventory instances (script, different ports), then a 4th | One partition each; the 4th is idle; rebalance in logs |
| E6 | Slow the consumer, send 500 orders | Consumer lag grows in Redpanda Console and Grafana, then drains |
| E7 | Look at one order's messages in Redpanda Console | All in the same partition (key = orderId) |
| E8 | Stop inventory, keep creating orders | Timeouts -> breaker opens -> fast 503s -> start inventory -> half-open -> closed; visible in Grafana |
| E9 | Create an order, open Jaeger | One trace across gateway, order, REST, Kafka, inventory, payment, notification - including across the outbox |

## 10. Learning notes - `docs/LEARNINGS.md`

**The most important deliverable after the code.** The owner is new to Kafka
and to several tools here; LEARNINGS must let him study and prepare for
interviews without reading the code.

### Rules

- Simple English, short sentences, a diagram where it helps.
- Written for a beginner: no term used before it is explained.
- Updated at the end of every milestone, not only at the end.

### Format for each topic

1. **What it is** - one or two sentences.
2. **Why it exists** - the problem it solves.
3. **How it works here** - with a small diagram and the file / class that does
   it.
4. **What I saw** - from the experiments, with numbers where possible.
5. **Interview answer** - a short answer to say out loud.
6. **Common follow-up questions** - with short answers.

### Topics that must be covered

- Kafka basics: topic, partition, offset, key, producer, consumer, consumer
  group, rebalance, retention, KRaft, replication factor - plus a **glossary**.
- Delivery guarantees: at-most-once, at-least-once, exactly-once.
- Outbox pattern - including the trace-context lesson.
- Idempotent consumer - and when it is not needed.
- Retries, poison messages, dead-letter topic; blocking vs `@RetryableTopic`.
- Ordering and keys.
- Saga: orchestration vs choreography; compensation vs rollback.
- Async API (202 + polling).
- Spring Cloud Gateway: what it is, how it runs, gateway vs load balancer.
- Circuit breaker, retry, timeout with Resilience4j; aspect order.
- **Micrometer Tracing + OpenTelemetry**: what each part is, how a trace id
  travels over HTTP and Kafka, what a span is, how it reaches Jaeger.
- **Jaeger**: what it is, how to read a trace timeline in its UI.
- **Prometheus**: the pull model (scraping), what a metric and a label are,
  basic queries (PromQL) used by the dashboard.
- **Grafana**: data source, dashboard, panel - and how it reads Prometheus.
- Metrics with Micrometer; Kafka consumer lag; the custom outbox gauge.
- Traces vs metrics vs logs - when to use which.
- Flyway; Testcontainers (including the Docker 29 note below).
- Database per service; shared contracts trade-off.
- Every experiment E1-E9 in the experiment format: what I did, what I saw, why.
- **Interview Q&A** section collecting all the "interview answer" parts.
- **Next steps**: Debezium/CDC, choreography version, RabbitMQ comparison, login
  at the gateway, AWS deployment, Kubernetes.

`docs/implementation-notes.md` is kept during the build for decisions that were
not in this spec, changes, and trade-offs.

## 11. Testing

- **Unit tests** (plain Java, fast): saga state changes, payment rule, stock
  reservation and release logic.
- **Integration tests** (Testcontainers, real PostgreSQL + real Kafka) per
  service: e.g. `ReserveStock` reserves stock and writes `StockReserved` to the
  outbox; a duplicate message is processed once; a broken message lands in the
  DLT; the relay publishes outbox rows.
- **End-to-end check:** happy path to `CONFIRMED`; Laptop order to `CANCELLED /
  PAYMENT_FAILED` with stock restored.
- Known environment note (from the owner's earlier project): Testcontainers on
  Docker Engine 29 needed docker-java `api.version=1.44` set through Surefire
  system properties. Apply it if the same error appears.
- Work is done test-first where practical.

## 12. Milestones

Each milestone ends runnable, with tests green and LEARNINGS updated.

| # | Days | Scope | Experiments |
|---|---|---|---|
| M1 | 1-2 | Parent build, Docker Compose (Kafka native KRaft, Redpanda Console, PostgreSQL), service start script, messaging-contracts, order + inventory exchanging `ReserveStock` / `StockReserved` directly through `KafkaTemplate` (no outbox yet), topics + keys; check Redpanda Console can produce messages and Testcontainers runs with `kafka-native` | E5, E7 |
| M4 note | | Jaeger, Prometheus and Grafana containers are added in M4, not M1 - nothing is downloaded before it is needed | |
| M2 | 3-4 | Outbox + relay, idempotent consumer, retries + DLT, demo delay setting | E1, E2, E3, E4, E6 |
| M3 | 5-6 | payment-service, full orchestrated saga with compensation, notification fan-out, `GET /orders/{id}` | happy path + both failures |
| M4 | 7-8 | api-gateway + custom filter, `RestClient` + Resilience4j, tracing + metrics + Grafana dashboard, outbox trace context | E8, E9 |
| M5 | 9-10 | README, LEARNINGS complete with interview Q&A, end-to-end check, push to GitHub | - |

## 13. Tech stack and dependencies - for approval

Exact versions are pinned in the implementation plan after checking they work
together.

| For | Library / tool |
|---|---|
| Base | Java 21, Spring Boot 3.5.6, Spring Cloud 2025.0, Maven Wrapper |
| Services | `spring-boot-starter-web`, `spring-boot-starter-actuator`, `spring-boot-starter-validation`, `spring-boot-starter-data-jpa`, PostgreSQL JDBC driver |
| Migrations | Flyway (`flyway-core`, `flyway-database-postgresql`) |
| Kafka | `spring-kafka` |
| Gateway | `spring-cloud-starter-gateway` |
| Resilience | `resilience4j-spring-boot3`, `spring-boot-starter-aop` |
| Tracing | `micrometer-tracing-bridge-otel`, `opentelemetry-exporter-otlp` |
| Metrics | `micrometer-registry-prometheus` |
| Tests | `spring-boot-starter-test`, `spring-boot-testcontainers`, Testcontainers (JUnit 5, PostgreSQL, Kafka) |
| Not used | Lombok (use records and plain classes) |

Docker images, each pinned to an exact version (download sizes measured on
Docker Hub, 2026-10-11):

| Image | Download | Note |
|---|---|---|
| `apache/kafka-native` | ~59 MB | Official native-image Kafka broker, meant for local dev / tests |
| `redpandadata/console` | ~61 MB | Kafka web UI; works with any Kafka |
| `postgres:17.4` | 0 MB | **Already on the PC** - reused |
| `jaegertracing/jaeger` | ~55 MB | Traces |
| `prom/prometheus` | ~115 MB | Metrics |
| `grafana/grafana` | ~350 MB | Dashboards (exact size depends on the tag chosen) |

New downloads: about 620 MB (roughly 1.5-2 GB on disk once unpacked). Tests use
the same Kafka and PostgreSQL images, so they add nothing.

Any library not in this table needs the owner's approval before it is added.

## 14. Conventions

- Constructor injection; records for messages and DTOs; no Lombok.
- Explicit `@Transactional` boundaries on the methods that write business data
  + outbox / processed-message rows.
- Imports at the top of files, no inline fully-qualified class names.
- Code comments in English.
- New GitHub repo on the owner's **personal** account; personal git identity
  (as in the GraalVM study project). **Nothing is committed unless the owner
  asks.**

## 15. Risks and notes

- **Disk:** C: has ~29 GB free (92% used). Use only the images in section 13;
  no service images; reuse `postgres:17.4`. `docker system df` before and after
  M1 to record the real cost in implementation-notes.
- RAM: Kafka + PostgreSQL + monitoring in Docker, plus 5 JVMs on the host -
  about 4-5 GB. Docker Desktop is not auto-started on this PC - start it first.
- Port clash: another local PostgreSQL container also uses 5432; the
  demo's PostgreSQL is mapped to host port **5433** so both can run.
- `apache/kafka-native` is **experimental** (fine for local dev and tests, not
  production) and has **no Kafka command-line tools** inside. Test messages are
  sent from Redpanda Console; check in M1 that its "produce message" feature
  works, otherwise add a small dev-only endpoint. Also check in M1 that
  Testcontainers' `KafkaContainer` runs with this image; if not, tests fall
  back to `apache/kafka` (+ ~239 MB) - ask the owner first.
- Prometheus runs in Docker but the services run on the host: scrape targets
  use `host.docker.internal`.
- Image tags: pin exact versions (an earlier project could not pull `:latest`).
- The trace-across-outbox fix (7.3) is the least standard part; if it takes too
  long, document the broken trace as the finding and move on.

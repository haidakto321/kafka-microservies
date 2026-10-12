# Kafka microservices demo

A learning project: Kafka and microservice patterns with Spring Boot.
Design: `docs/superpowers/specs/2026-10-11-kafka-microservices-demo-design.md`.
What I learned: `docs/LEARNINGS.md`.

## Status

M1 (Kafka basics) done: order-service and inventory-service talk through Kafka.
M2 (reliability) done: outbox, idempotent consumers, retries and dead-letter topics.
M3 (full saga) done: payment, compensation and notifications.
M4 (patterns + observability) done: gateway, Resilience4j, Jaeger, Prometheus, Grafana.

## Run it (Windows, PowerShell)

Requirements: Docker Desktop running, JDK 21 installed at
`C:\Program Files\Eclipse Adoptium\jdk-21.0.4.7-hotspot`.

```powershell
.\scripts\start-infra.ps1        # Kafka, Redpanda Console, PostgreSQL, Jaeger, Prometheus, Grafana
.\scripts\start-services.ps1     # builds and starts the 5 services (gateway on 8080)
.\scripts\e2e-check.ps1          # happy path + both failures, end to end
.\scripts\send-orders.ps1 -ProductId 1 -Quantity 2
Invoke-RestMethod http://localhost:8080/api/orders/<id>   # clients go through the gateway
.\scripts\stop-services.ps1
docker compose stop
```

Experiments (see `docs/LEARNINGS.md` for what each one shows):

```powershell
.\scripts\outbox-status.ps1      # outbox rows waiting to be sent, per database
.\scripts\start-instance.ps1 -Service inventory-service -Port 8082 -ExtraArgs '--demo.processing-delay-ms=15000'
```

| What | Where |
|---|---|
| api-gateway (use this) | http://localhost:8080/api/orders, /api/products |
| order-service | http://localhost:8081 |
| inventory-service | http://localhost:8082 |
| payment-service | http://localhost:8083 |
| notification-service | http://localhost:8084 (logs only) |
| Redpanda Console (Kafka UI) | http://localhost:8090 |
| PostgreSQL | localhost:5433, user `demo`, password `demo` |
| Jaeger (traces) | http://localhost:16686 |
| Prometheus | http://localhost:9090 |
| Grafana dashboard | http://localhost:3000/d/kafka-demo |

## Tests

```powershell
. .\scripts\use-jdk21.ps1
.\mvnw.cmd test
```

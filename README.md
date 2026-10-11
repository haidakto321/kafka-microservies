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

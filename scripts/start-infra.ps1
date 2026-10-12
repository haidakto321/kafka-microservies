# Starts Kafka, Redpanda Console, PostgreSQL, Jaeger, Prometheus and Grafana, and waits until Kafka and PostgreSQL are ready.
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path $PSScriptRoot -Parent)

docker compose up -d kafka console postgres jaeger prometheus grafana
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

Write-Host 'Infrastructure is up. Redpanda Console http://localhost:8090 | Jaeger http://localhost:16686 | Prometheus http://localhost:9090 | Grafana http://localhost:3000'

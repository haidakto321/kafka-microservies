# Builds the services (unless -SkipBuild) and starts one instance of each of the 5 services.
# Run .\scripts\start-infra.ps1 first.
param([switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
. "$PSScriptRoot\use-jdk21.ps1"

# Start order: the gateway last, order-service after the services it calls.
$services = [ordered]@{
    'inventory-service'    = 8082
    'payment-service'      = 8083
    'notification-service' = 8084
    'order-service'        = 8081
    'api-gateway'          = 8080
}

# A taken port (usually an old instance) makes the new instance die while the old one still
# answers UP below - so refuse before building or starting anything.
$busy = @($services.Values | Where-Object { Get-NetTCPConnection -State Listen -LocalPort $_ -ErrorAction SilentlyContinue })
if ($busy.Count -gt 0) { throw "Port(s) $($busy -join ', ') already in use - run .\scripts\stop-services.ps1 first" }

if (-not $SkipBuild) {
    & .\mvnw.cmd -q -DskipTests package
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
}

foreach ($name in $services.Keys) {
    & "$PSScriptRoot\start-instance.ps1" -Service $name -Port $services[$name]
}
# Readiness probe (spec 7.4): UP means the service has started and is ready for traffic.
# Note: after a force-kill, a ready service can still wait up to ~45 s for its Kafka partitions
# (the killed consumer's session timeout - LEARNINGS section 3).
$deadline = (Get-Date).AddSeconds(120)
$waiting = @($services.Keys)
while ($waiting.Count -gt 0 -and (Get-Date) -lt $deadline) {
    foreach ($name in $waiting) {
        # A process that exited will never become ready - stop waiting at once.
        $procId = Get-Content "$root\logs\$name-$($services[$name]).pid"
        if (-not (Get-Process -Id $procId -ErrorAction SilentlyContinue)) {
            throw "$name stopped during startup - see logs\$name-$($services[$name]).log (and .err); then run .\scripts\stop-services.ps1"
        }
    }
    $waiting = @($waiting | Where-Object {
        try { (Invoke-RestMethod "http://localhost:$($services[$_])/actuator/health/readiness" -TimeoutSec 2).status -ne 'UP' } catch { $true }
    })
    if ($waiting.Count -gt 0) { Start-Sleep -Seconds 2 }
}
if ($waiting.Count -gt 0) { throw "Not ready after 120 s: $($waiting -join ', ') - see logs\" }
Write-Host 'All 5 services are ready. Stop everything with .\scripts\stop-services.ps1'

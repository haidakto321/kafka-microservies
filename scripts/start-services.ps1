# Builds the services (unless -SkipBuild) and starts one instance of each of the 5 services.
# Run .\scripts\start-infra.ps1 first.
param([switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root
. "$PSScriptRoot\use-jdk21.ps1"

if (-not $SkipBuild) {
    & .\mvnw.cmd -q -DskipTests package
    if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
}

& "$PSScriptRoot\start-instance.ps1" -Service inventory-service -Port 8082
& "$PSScriptRoot\start-instance.ps1" -Service payment-service -Port 8083
& "$PSScriptRoot\start-instance.ps1" -Service notification-service -Port 8084
& "$PSScriptRoot\start-instance.ps1" -Service order-service -Port 8081
& "$PSScriptRoot\start-instance.ps1" -Service api-gateway -Port 8080
# Readiness probe (spec 7.4): UP means the service has started and is ready for traffic.
# Note: after a force-kill, a ready service can still wait up to ~45 s for its Kafka partitions
# (the killed consumer's session timeout - LEARNINGS section 3).
$ports = 8080, 8081, 8082, 8083, 8084
$deadline = (Get-Date).AddSeconds(120)
$waiting = $ports
while ($waiting.Count -gt 0 -and (Get-Date) -lt $deadline) {
    $waiting = @($waiting | Where-Object {
        try { (Invoke-RestMethod "http://localhost:$_/actuator/health/readiness" -TimeoutSec 2).status -ne 'UP' } catch { $true }
    })
    if ($waiting.Count -gt 0) { Start-Sleep -Seconds 2 }
}
if ($waiting.Count -gt 0) { throw "Not ready after 120 s: ports $($waiting -join ', ') - see logs\" }
Write-Host 'All 5 services are ready. Stop everything with .\scripts\stop-services.ps1'

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
Write-Host 'Give them ~20 seconds to start. Stop everything with .\scripts\stop-services.ps1'

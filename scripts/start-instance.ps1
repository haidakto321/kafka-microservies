# Starts ONE instance of a service in the background, on the given port.
# Example:  .\scripts\start-instance.ps1 -Service inventory-service -Port 8092
# Extra Spring Boot settings (experiments E1, E6):
#           .\scripts\start-instance.ps1 -Service inventory-service -Port 8082 -ExtraArgs '--demo.processing-delay-ms=15000'
param(
    [Parameter(Mandatory)][string]$Service,
    [Parameter(Mandatory)][int]$Port,
    [string[]]$ExtraArgs = @()
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$jar = Get-ChildItem "$root\$Service\target\$Service-*.jar" | Where-Object { $_.Name -notlike '*.original' } | Select-Object -First 1
if (-not $jar) { throw "No jar for $Service - run .\scripts\start-services.ps1 first (it builds)" }

$java = 'C:\Program Files\Eclipse Adoptium\jdk-21.0.4.7-hotspot\bin\java.exe'
New-Item -ItemType Directory -Force -Path "$root\logs" | Out-Null
$log = "$root\logs\$Service-$Port.log"

# --server.port (not -Dserver.port): PowerShell can split -D arguments at the dot.
$arguments = @('-jar', "`"$($jar.FullName)`"", "--server.port=$Port") + $ExtraArgs
$process = Start-Process -FilePath $java -ArgumentList $arguments `
    -RedirectStandardOutput $log -RedirectStandardError "$log.err" `
    -WindowStyle Hidden -PassThru
Set-Content -Path "$root\logs\$Service-$Port.pid" -Value $process.Id
Write-Host "Started $Service on port $Port (pid $($process.Id)) - log: logs\$Service-$Port.log"

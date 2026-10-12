# End-to-end check (spec 11): the happy path and both failure paths, through all 4 services.
# Run .\scripts\start-infra.ps1 and .\scripts\start-services.ps1 first.
param(
    [string]$OrderUrl = 'http://localhost:8081',
    [string]$InventoryUrl = 'http://localhost:8082'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

function Wait-FinalState([string]$id) {
    $watch = [Diagnostics.Stopwatch]::StartNew()
    while ($watch.Elapsed.TotalSeconds -lt 30) {
        $order = Invoke-RestMethod "$OrderUrl/orders/$id"
        if ($order.status -eq 'CONFIRMED' -or $order.status -eq 'CANCELLED') {
            return [pscustomobject]@{ Order = $order; Ms = $watch.ElapsedMilliseconds }
        }
        Start-Sleep -Milliseconds 100
    }
    throw "Order $id did not reach a final state in 30 s"
}

$cases = @(
    @{ Name = 'Keyboard x1 - happy path';              Product = 1; Status = 'CONFIRMED'; Reason = $null },
    @{ Name = 'Laptop x1 - payment fails, stock back'; Product = 3; Status = 'CANCELLED'; Reason = 'PAYMENT_FAILED' },
    @{ Name = 'Sold-out mug x1 - out of stock';        Product = 4; Status = 'CANCELLED'; Reason = 'OUT_OF_STOCK' }
)
$laptopBefore = (Invoke-RestMethod "$InventoryUrl/products/3").stock
$failed = 0
$ids = @()
foreach ($case in $cases) {
    $body = @{ productId = $case.Product; quantity = 1 } | ConvertTo-Json
    $id = (Invoke-RestMethod -Method Post -Uri "$OrderUrl/orders" -ContentType 'application/json' -Body $body).id
    $ids += $id
    $result = Wait-FinalState $id
    $ok = ($result.Order.status -eq $case.Status) -and ($result.Order.cancelReason -eq $case.Reason)
    if (-not $ok) { $failed++ }
    $verdict = if ($ok) { 'PASS' } else { 'FAIL' }
    Write-Output ("{0}  {1}: {2} {3} after {4} ms ({5})" -f $verdict, $case.Name, $result.Order.status, $result.Order.cancelReason, $result.Ms, $id)
}

# ReleaseStock goes out after the order is already CANCELLED, so give it a moment.
$laptopAfter = $null
for ($i = 0; $i -lt 50; $i++) {
    $laptopAfter = (Invoke-RestMethod "$InventoryUrl/products/3").stock
    if ($laptopAfter -eq $laptopBefore) { break }
    Start-Sleep -Milliseconds 200
}
$restored = $laptopAfter -eq $laptopBefore
if (-not $restored) { $failed++ }
$verdict = if ($restored) { 'PASS' } else { 'FAIL' }
Write-Output ("{0}  Laptop stock put back: before {1}, after {2}" -f $verdict, $laptopBefore, $laptopAfter)

$log = "$root\logs\notification-service-8084.log"
if (Test-Path $log) {
    Start-Sleep -Seconds 1
    foreach ($id in $ids) {
        $line = Select-String -Path $log -Pattern "NOTIFICATION: order $id" | Select-Object -Last 1
        $verdict = if ($line) { 'PASS' } else { 'FAIL' }
        if (-not $line) { $failed++ }
        $text = if ($line) { $line.Line.Substring($line.Line.IndexOf('NOTIFICATION')) } else { "no notification for $id" }
        Write-Output "$verdict  $text"
    }
}
if ($failed -gt 0) { throw "$failed check(s) failed" }
Write-Output 'All checks passed.'

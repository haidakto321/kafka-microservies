# Places orders through order-service.
# Example:  .\scripts\send-orders.ps1 -ProductId 1 -Quantity 2 -Count 10
param(
    [long]$ProductId = 1,
    [int]$Quantity = 1,
    [int]$Count = 1,
    [string]$BaseUrl = 'http://localhost:8081'
)
for ($i = 1; $i -le $Count; $i++) {
    $body = @{ productId = $ProductId; quantity = $Quantity } | ConvertTo-Json
    $order = Invoke-RestMethod -Method Post -Uri "$BaseUrl/orders" -ContentType 'application/json' -Body $body
    Write-Host "$($order.id)  $($order.status)  total=$($order.total)"
}

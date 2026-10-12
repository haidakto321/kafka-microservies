# Shows how many outbox rows are waiting (not sent to Kafka yet) in each service's database.
# Example:  .\scripts\outbox-status.ps1
foreach ($db in 'order_db', 'inventory_db', 'payment_db') {
    Write-Host "== $db"
    docker exec kafka-demo-postgres psql -U demo -d $db -c `
        "SELECT count(*) FILTER (WHERE published_at IS NULL) AS waiting, count(*) AS total FROM outbox_event"
}

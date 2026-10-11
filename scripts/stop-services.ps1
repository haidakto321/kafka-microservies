# Stops every service instance started by start-instance.ps1.
$root = Split-Path $PSScriptRoot -Parent
Get-ChildItem "$root\logs\*.pid" -ErrorAction SilentlyContinue | ForEach-Object {
    $procId = Get-Content $_
    Stop-Process -Id $procId -Force -ErrorAction SilentlyContinue
    Remove-Item $_
    Write-Host "Stopped $($_.BaseName) (pid $procId)"
}

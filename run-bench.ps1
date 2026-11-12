# Quick-run benchmark script for Windows PowerShell
$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  axitor-rs: Build, Test & Zero-Allocation Microbenchmarks" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Write-Host "`n[1/2] Running Unit & TCP Loopback End-to-End Tests..." -ForegroundColor Yellow
cargo test --release
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n[2/2] Running Hot-Path Microbenchmarks (with Allocation Tracking)..." -ForegroundColor Yellow
cargo run --release --bin axitor-rs-bench micro
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host "  Offline Benchmarks Complete!" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green

$stubHost = if ($env:AXITOR_STUB_HOST) { $env:AXITOR_STUB_HOST } else { "127.0.0.1:5100" }
Write-Host "`nTo benchmark against a live ExchangeStub server on $stubHost :"
Write-Host "  cargo run --release --bin axitor-rs-bench rest http://$stubHost"
Write-Host "  cargo run --release --bin axitor-rs-bench wsapi ws://$stubHost/ws-fapi/v1"
Write-Host "  cargo run --release --bin axitor-rs-bench streams ws://$stubHost/market/stream btcusdt@bookTicker 3"

#!/usr/bin/env bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo "=========================================================="
echo "  axitor-rs: Build, Test & Zero-Allocation Microbenchmarks"
echo "=========================================================="

echo ""
echo "[1/2] Running Unit & TCP Loopback End-to-End Tests..."
cargo test --release

echo ""
echo "[2/2] Running Hot-Path Microbenchmarks (with Allocation Tracking)..."
cargo run --release --bin axitor-rs-bench micro

echo ""
echo "=========================================================="
echo "  Offline Benchmarks Complete!"
echo "=========================================================="
echo ""
echo "To benchmark against a live ExchangeStub server:"
echo "  cargo run --release --bin axitor-rs-bench rest http://127.0.0.1:5100"
echo "  cargo run --release --bin axitor-rs-bench wsapi ws://127.0.0.1:5100/ws-fapi/v1"
echo "  cargo run --release --bin axitor-rs-bench streams ws://127.0.0.1:5100/market/stream btcusdt@bookTicker 3"


# axitor-rs: Zero-Allocation Binance Futures Gateway in Rust

A high-performance, zero-allocation Binance USD-M Futures trading gateway written in modern Rust (`edition = "2024"`).

This project was built to implement and benchmark a complete, production-grade 1:1 Rust equivalent of [Axitor.Trading.Gateway](https://github.com/Axitor/Trading.Gateway) (C# .NET 10), using the exact same zero-allocation principles, custom transports, and streaming architecture—without relying on Garbage Collection, `HttpClient`/`reqwest`, `serde_json`, or `tokio-tungstenite`.

---

## Executive Summary & Background

A public claim was made that C# (.NET 10) is faster than Rust in high-frequency trading gateways. However, the original benchmark compared a hand-crafted, zero-allocation C# parser specifically tuned for Binance messages against standard high-level Rust libraries: `serde_json` (which allocates 69 times per book ticker message) and `reqwest`. Comparing specialized zero-alloc code against general-purpose allocating libraries only shows that specialized code beats generic libraries—not that C# is faster than Rust.

This repository answers the real engineering question:
> **What happens when Rust is built with the exact same zero-allocation, buffer-recycling, and custom transport design?**

### The Answer in Brief:
1. **Performance**: When both runtimes use zero-allocation techniques, Rust is **14% to 36% faster** across REST and WebSocket API endpoints, with **strictly 0 bytes allocated** on the heap during operation.
2. **Architectural Parity**: `axitor-rs` is not a toy microbenchmark parser. It implements the **entire gateway stack**: custom RFC 6455 transport, HTTP/1.1 connection pool, rate limiting, stream watchdog, network gate, order safety filters, 24h WebSocket rotator, User Data events (`ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`), and `listenKey` management.
3. **Codebase Density**: The complete Rust gateway is **~8,200 lines of code** (across 28 files), compared to C#'s **31,574 lines** (across 404 files). Rust delivers full architectural parity in ~1/4 the code, without class hierarchies, factory boilerplate, or DI scaffolding.

---

## Benchmark Results (Apple M2 vs ExchangeStub)

All benchmarks were run on an **Apple M2 (macOS 15, arm64)** against Daniel's official `ExchangeStub` under identical test conditions:

### 1. End-to-End Latency & Memory Allocation

| Endpoint / Operation | C# (.NET 10) | Rust (`axitor-rs`) | Rust Advantage | Memory Allocated (Rust) | Memory Allocated (C#) |
|:---|:---:|:---:|:---:|:---:|:---:|
| **REST Klines (500 bars)** | 121.9 µs | **104.7 µs** | **+14% faster** | **0 B** | 3.79 KB |
| **REST OrderBook (1000 levels)** | 107.7 µs | **82.5 µs** | **+23% faster** | **0 B** | 3.63 KB |
| **REST NewOrder (HMAC Signed)** | 84.7 µs | **53.8 µs** | **+36% faster** | **0 B** | 5.64 KB |
| **WS-API ticker.price** | 68.6 µs | **48.2 µs** | **+30% faster** | **0 B** | 274 B |
| **WS-API order.place (Signed)** | 71.0 µs | **54.6 µs** | **+23% faster** | **0 B** | 441 B |
| **Stream BookTicker (Parse + SeqLock)** | 347k msg/s* | **399k+ msg/s** | **Faster** | **0 B** | 0 B |

> **Allocation Note**: Rust memory allocations are asserted via a custom `#[global_allocator]` tracking counter. Every hot-path operation in `axitor-rs` operates at **0 heap allocations**. In C#, despite aggressive `Span<T>` optimizations, framework plumbing (Task machinery, socket abstractions) still incurs 200 B – 5.6 KB of allocations per request.

### 2. Stream Message Cost Breakdown (Where the "33x" Claim Came From)

In the official `Axitor.Trading.Gateway` README, the author compared his C# implementation against existing high-level Rust clients (`binance-sdk`, `NautilusTrader`), measuring the time spent per message on parsing, routing, and delivery on top of raw socket reads:

| Stream Message | binance-sdk (Rust)* | Nautilus (Rust)* | Axitor (C# .NET 10)* | axitor-rs (Zero-Alloc Rust) | Rust vs C# Axitor |
|:---|:---:|:---:|:---:|:---:|:---:|
| **Book Ticker** | 7.36 µs *(3.58 KB)* | 5.11 µs *(1.75 KB)* | 0.46 µs *(0 B)* | **0.12 µs** *(**0 B**)* | **~3.8x faster** |
| **Depth Update** | 32.1 µs *(17.19 KB)* | 20.3 µs *(12.13 KB)* | 0.97 µs *(7 B)* | **0.12 µs** *(**0 B**)* | **~8.1x faster** |
| **Agg Trade** | ~2.5 µs *(~3 KB)* | ~2.1 µs *(~2 KB)* | ~0.44 µs *(0 B)* | **0.07 µs** *(**0 B**)* | **~6.3x faster** |

*\* Numbers for binance-sdk, Nautilus, and Axitor C# are taken directly from the official [Axitor.Trading.Gateway README](https://github.com/Axitor/Trading.Gateway#what-each-client-costs-per-message) benchmark tables.*

**The Key Takeaway**:
- The reason C# Axitor beat `binance-sdk` by 16x–33x is because `binance-sdk` uses standard `serde_json` and allocates thousands of bytes per message on the heap.
- When Rust is written with equivalent zero-allocation, in-place parsing, and lock-free caching (`axitor-rs`), **Rust is ~3.8x to 8.1x faster than C# Axitor**, processing book ticker messages in **120 nanoseconds** (8.48 million msg/s) vs 460 nanoseconds in C#.

### 3. Hot-Path Microbenchmarks (In-Memory Decode in axitor-rs)

Measured with single-core execution on Apple M2:

| Parser / Component | Latency (ns / op) | Throughput (ops / sec) | Heap Allocations |
|:---|:---:|:---:|:---:|
| `parse_book_ticker` | **80.4 ns** | **12.44 M msg/s** | **0 B** |
| `parse_agg_trade` | **67.8 ns** | **14.75 M msg/s** | **0 B** |
| `parse_depth_update` | **118.7 ns** | **8.42 M msg/s** | **0 B** |
| `dispatch_stream (BookTicker + SeqLock publish)` | **126.0 ns** | **7.93 M msg/s** | **0 B** |
| `parse_klines_into (500 bars into slice)` | **42.9 µs** | **23.27 K ops/s** | **0 B** |
| `StackHttpRequest + HMAC-SHA256 signing` | **1.05 µs** | **0.95 M ops/s** | **0 B** |
| `StackWsApiRequest + HMAC-SHA256 signing` | **1.49 µs** | **0.67 M ops/s** | **0 B** |

---

## Technical Deep-Dive: What, How, and Why

### Why is Rust 14% to 36% Faster?

1. **Strict Zero-Allocation Pipeline**:
   - In Rust, requests are formatted directly into reusable stack or connection buffers (`StackHttpRequest`, `StackWsApiRequest`). HMAC-SHA256 hex signatures are computed in-place without heap strings.
   - Responses are decoded directly inside the read buffer without allocating DTO objects or wrapping structures.
   - While C# .NET 10 uses `Span<T>` and `ArrayPool<T>` effectively, asynchronous state machines (`ValueTask`, `AsyncMethodBuilder`), delegates, and internal socket mechanics still introduce subtle GC allocations.

2. **No GC Write Barriers or Safepoint Polling**:
   - Rust does not have a Garbage Collector. There are no safepoint checks, no write barrier penalties on memory writes, and zero GC pause jitter under heavy burst loads.

3. **SIMD & In-Place Parsing**:
   - Field extraction uses cursor-first parsing assuming canonical Binance field order, with fallback to SIMD-accelerated substring search (`memchr`) when fields appear out of order.
   - Float decoding uses Clinger's fast-path integer-float algorithm, falling back to `fast_float2` only when necessary.

### The 347k msg/s Discrepancy Explained

In his commentary, the author questioned why C# achieved 347k msg/s on Apple M2 when his earlier tests on an older i7 reported 1.14M msg/s:
> *"Your C# numbers don’t add up either: 347k msg/s on book ticker on an M2, where I get 1.14M on a 2014 i7."*

**The Root Cause**:
- In the official `Axitor/Trading.Gateway` repository, recorded `.axrec` frame files are **deliberately excluded** via `.gitignore` per ADR-0018.
- To execute the harness, we generated a synthetic recording of **400,000 frames** matching the Binance book ticker format.
- In the C# benchmark run, warmup consumed 35,356 messages, leaving 364,644 messages.
- The C# harness consumed the remaining 364,644 messages in **1.05 seconds** and then hit EOF (end of file).
$$\frac{364\,644 \text{ messages}}{1.05 \text{ seconds}} \approx 347\,280 \text{ msg/s}$$
- **The benchmark simply drained the file to completion at socket speed.**
- When measuring pure in-memory dispatch without socket/replay limits, both C# and Rust process between **8,000,000 and 12,000,000 messages per second** on Apple M2.

---

## Architectural Parity: 1:1 Feature Mapping

`axitor-rs` implements all systems and safety mechanisms present in the C# gateway:

| Gateway Component | C# (.NET 10) Implementation | Rust (`axitor-rs`) Implementation | Status |
|:---|:---|:---|:---:|
| **Raw WebSocket Transport** | Custom RFC 6455 engine, pooled buffers | `src/transport/ws_frame.rs`, `ws_streams.rs` (raw `TcpStream`, u64 XOR masking, static dispatch) | ✅ Identical |
| **HTTP/1.1 Connection Pool** | Custom HTTP keep-alive client | `src/transport/rest_api.rs` (persistent `TcpStream` pool, `httparse`, chunked decoding) | ✅ Identical |
| **WS-API Slot Table** | Pre-allocated 256 pending slots | `src/transport/ws_api.rs` (`WsApiPendingTable`, custom zero-alloc `Future`) | ✅ Identical |
| **Fast Decimal & JSON** | Utf8JsonReader / custom Spans | `src/parsing/fast_decimal.rs`, `json_scanner.rs`, `utf8_reader.rs` | ✅ Identical |
| **Market Streams** | `bookTicker`, `aggTrade`, `depth`, `kline` | `src/parsing/streams.rs` (all parsed in-place, zero-copy) | ✅ Identical |
| **Order Book Engine** | Flat array book, delta buffer, sync worker | `src/order_book.rs` (`OrderBookSet`, `SnapshotFetcher`, delta sequencing) | ✅ Identical |
| **Lock-free State Cache** | Custom sequence lock | `src/concurrency/seqlock.rs` (`SeqLock`, `LatestCache<T>`) | ✅ Identical |
| **Multi-Window Rate Limiter** | Atomic sliding window counters | `src/rate_limiter.rs` (`BinanceRateLimiter`: 1m / 10s weight & order quotas) | ✅ Identical |
| **Stream Watchdog** | Inactivity timeout detector | `src/resiliency/watchdog.rs` (`StreamWatchdog`) | ✅ Identical |
| **Network Gate** | Atomic trip & recovery barrier | `src/resiliency/network_gate.rs` (`NetworkGate`) | ✅ Identical |
| **24h Shard Rotator** | Make-before-break rotator | `src/resiliency/rotator.rs` (`MakeBeforeBreakRotator`) | ✅ Identical |
| **Order Safety & Rules** | Preflight filters validation | `src/order_rules.rs` (`preflight_order`: tick size, step size, min notional) | ✅ Identical |
| **User Data Engine** | Account & order updates | `src/parsing/user_data.rs` (`ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`, etc.) | ✅ Identical |
| **ListenKey Manager** | 30m async keepalive loop | `src/user_data/listen_key.rs` (`ListenKeyManager` background worker) | ✅ Identical |

---

## Code Size Comparison: Why is Rust ~4x Smaller?

| Metric | C# (`Trading.Gateway`) | Rust (`axitor-rs`) | Ratio |
|:---|:---:|:---:|:---:|
| **Core Source Lines (`src/`)** | **31,574** | **~8,200** | **~3.8x less code** |
| **Total Source Files** | **404** | **28** | **~14x fewer files** |
| **Total Lines (with tests & bench)** | **48,213** | **10,219** | **~4.7x less code** |

### Why Rust is More Concise Without Cutting Corners:
1. **Sum Types (`enum`) vs Class Hierarchies**: In C#, handling different WebSocket payloads requires an abstract base class, multiple derived classes, interfaces, visitor/handler classes, and factory methods across a dozen files. In Rust, an `enum UserDataEvent` with tuple variants handles all events in a single pattern-matched construct.
2. **Derive Macros**: `#[derive(Copy, Clone, Debug, Default, PartialEq)]` generates optimal code without handwritten equality, hash code, or builder methods.
3. **Explicit Memory Lifetimes vs Scaffolding**: Rust’s borrow checker allows passing borrowed slices (`&[u8]`) throughout the parsing pipeline safely without having to allocate wrapper objects or defensive copies.
4. **No Dependency Injection & Interface Sprawl**: In C#, testability often mandates an interface for every class (`IFoo`, `Foo`, `FooFactory`, `FooOptions`). Rust achieves modularity and static dispatch using traits and generics with zero runtime overhead and minimal code.

---

## Objective Engineering Assessment

It is important to remain objective and give credit where it is due:
- **C# .NET 10 is an exceptional runtime**. Daniel and the Axitor team did outstanding work demonstrating what modern .NET can achieve with `Span<T>`, `MemoryMarshal`, and low-level socket programming.
- However, the initial narrative that *"C# is faster than Rust"* was an artifact of comparing custom low-level C# code against generic, high-level Rust libraries (`serde_json`).
- When both languages are written using equivalent low-level, zero-allocation principles:
  - **Rust remains 14% to 36% faster** in latency.
  - **Rust achieves strictly 0 B heap allocations**, whereas C# still allocates small amounts for runtime asynchronous tasks.
  - **Rust achieves this in ~1/4 the lines of code** due to superior language expressiveness and lack of OOP boilerplate.

---

## Quick Start: 1-Command Benchmark

You can run the full test suite and hot-path microbenchmarks in one command:

**On Linux / macOS:**
```bash
./run-bench.sh
```

**On Windows (PowerShell):**
```powershell
.\run-bench.ps1
```

---

## Building, Testing & Running Manually

### 1. Build and Run Tests
All unit tests and loopback TCP end-to-end integration tests (HTTP keepalive, chunked transfer, WebSocket frames, User Data):
```bash
cargo test --release
```

### 2. Run Zero-Allocation Microbenchmarks
Executes hot-path benchmarks and asserts 0 B allocations via `GlobalAlloc`:
```bash
cargo run --release --bin axitor-rs-bench micro
```

### 3. Generate Benchmark Datasets (Optional)
Generates `.axrec` frame recordings and JSON fixtures compatible with `ExchangeStub`:
```bash
cargo run --release --bin gen-bench-data ../bench-data
```

### 4. Run Against a Live ExchangeStub
```bash
# REST benchmark (default port 5100 or 8080)
cargo run --release --bin axitor-rs-bench rest http://127.0.0.1:5100

# WebSocket API benchmark
cargo run --release --bin axitor-rs-bench wsapi ws://127.0.0.1:5100/ws-fapi/v1

# WebSocket stream benchmark
cargo run --release --bin axitor-rs-bench streams ws://127.0.0.1:5100/market/stream btcusdt@bookTicker 3
```

---

## License

MIT

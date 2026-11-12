use axitor_rs::concurrency::seqlock::LatestCache;
use axitor_rs::order_book::OrderBookSet;
use axitor_rs::parsing::depth::read_order_book_into;
use axitor_rs::parsing::rest::parse_klines_into;
use axitor_rs::parsing::streams::{parse_agg_trade, parse_book_ticker, parse_depth_update};
use axitor_rs::transport::rest_api::StackHttpRequest;
use axitor_rs::transport::ws_api::StackWsApiRequest;
use axitor_rs::transport::ws_streams::{
    dispatch_stream_frame, RouteSnapshot, StreamHandler, StreamKind, WsStreamRegistry,
};
use axitor_rs::{
    AggTradeModel, BinanceClient, BinanceClientOptions, BinanceInterval, BinanceLimits,
    BookTickerModel, ClientOrderId, DepthLevel, DepthUpdateHeader, Kline, KlineUpdateModel,
    NewOrderRequest, OrderSide, OrderType, PartialDepthModel, Symbol, TimeInForce,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct CountingAllocator;

static ALLOC_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[inline]
fn snapshot_allocs() -> (u64, u64) {
    (
        ALLOC_COUNT.load(Ordering::Relaxed),
        ALLOC_BYTES.load(Ordering::Relaxed),
    )
}

fn summarize_ns(samples: &mut [u64]) -> (u64, u64, u64, f64) {
    if samples.is_empty() {
        return (0, 0, 0, 0.0);
    }
    samples.sort_unstable();
    let n = samples.len();
    let p50 = samples[n * 50 / 100];
    let p95 = samples[(n * 95 / 100).min(n - 1)];
    let p99 = samples[(n * 99 / 100).min(n - 1)];
    let mean = samples.iter().copied().sum::<u64>() as f64 / n as f64;
    (p50, p95, p99, mean)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("micro");
    let default_stub = std::env::var("AXITOR_STUB_HOST").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

    match cmd {
        "micro" => run_micro(),
        "rest" => {
            let default_url = format!("http://{}", default_stub);
            let url = args
                .get(2)
                .map(String::as_str)
                .unwrap_or(&default_url);
            let iters = args
                .get(3)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(5_000);
            run_rest(url, iters).await;
        }
        "wsapi" => {
            let default_url = format!("ws://{}/ws-fapi/v1", default_stub);
            let url = args
                .get(2)
                .map(String::as_str)
                .unwrap_or(&default_url);
            let iters = args
                .get(3)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(5_000);
            run_wsapi(url, iters).await;
        }
        "streams" => {
            let default_url = format!("ws://{}/market/stream", default_stub);
            let url = args
                .get(2)
                .map(String::as_str)
                .unwrap_or(&default_url);
            let stream_name = args
                .get(3)
                .map(String::as_str)
                .unwrap_or("btcusdt@bookTicker");
            let secs = args
                .get(4)
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(3);
            run_streams(url, stream_name, secs).await;
        }
        _ => {
            eprintln!("Usage: axitor-rs-bench [micro | rest <url> [iters] | wsapi <url> [iters] | streams <url> <stream> [secs]]");
            std::process::exit(2);
        }
    }
}

struct NoopHandler {
    cache: Arc<LatestCache<BookTickerModel>>,
    count: u64,
}

impl StreamHandler for NoopHandler {
    #[inline(always)]
    fn on_book_ticker(&mut self, sub_id: u32, model: &BookTickerModel, _ts: i64) {
        self.cache.update(sub_id as usize, *model);
        self.count += 1;
    }

    #[inline(always)]
    fn on_agg_trade(&mut self, _sub_id: u32, _model: &AggTradeModel, _ts: i64) {
        self.count += 1;
    }

    #[inline(always)]
    fn on_partial_depth(&mut self, _sub_id: u32, _model: &PartialDepthModel, _ts: i64) {
        self.count += 1;
    }

    #[inline(always)]
    fn on_kline(&mut self, _sub_id: u32, _model: &KlineUpdateModel, _ts: i64) {
        self.count += 1;
    }

    #[inline(always)]
    fn on_depth_update(
        &mut self,
        _sub_id: u32,
        _header: &DepthUpdateHeader,
        _bids: &[DepthLevel],
        _asks: &[DepthLevel],
    ) {
        self.count += 1;
    }
}

fn run_micro() {
    println!("=== axitor-rs Hot-Path Microbenchmarks (zero-alloc target) ===");

    let book_ticker_frame = br#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":400900217,"s":"BTCUSDT","b":"64250.10","B":"1.250","a":"64250.20","A":"0.850","T":1700000000100,"E":1700000000101}}"#;
    let book_ticker_data = br#"{"e":"bookTicker","u":400900217,"s":"BTCUSDT","b":"64250.10","B":"1.250","a":"64250.20","A":"0.850","T":1700000000100,"E":1700000000101}"#;
    let agg_trade_data = br#"{"e":"aggTrade","E":1700000000100,"a":5933014,"s":"BTCUSDT","p":"64250.50","q":"0.150","f":100,"l":105,"T":1700000000100,"m":true}"#;
    let depth_data = br#"{"e":"depthUpdate","E":1700000000100,"T":1700000000100,"s":"BTCUSDT","U":1001,"u":1005,"pu":1000,"b":[["64250.10","2.500"],["64250.00","1.100"]],"a":[["64250.20","1.800"],["64250.30","3.200"]]}"#;

    let reg = WsStreamRegistry::new();
    reg.register("btcusdt@bookTicker", StreamKind::BookTicker)
        .unwrap();
    let snap: Arc<RouteSnapshot> = reg.snapshot().clone();

    let cache = Arc::new(LatestCache::new(16));
    let mut handler = NoopHandler {
        cache: Arc::clone(&cache),
        count: 0,
    };
    let mut bids_scratch = Vec::with_capacity(64);
    let mut asks_scratch = Vec::with_capacity(64);

    // Warmup
    for _ in 0..10_000 {
        std::hint::black_box(dispatch_stream_frame(
            book_ticker_frame,
            &snap,
            &mut handler,
            &mut bids_scratch,
            &mut asks_scratch,
        ));
    }

    let n = 1_000_000u64;
    let (a0, b0) = snapshot_allocs();
    let t0 = Instant::now();
    for _ in 0..n {
        let ok = dispatch_stream_frame(
            std::hint::black_box(book_ticker_frame),
            &snap,
            &mut handler,
            &mut bids_scratch,
            &mut asks_scratch,
        );
        std::hint::black_box(ok);
    }
    let dt = t0.elapsed();
    let (a1, b1) = snapshot_allocs();
    println!(
        "dispatch_stream(bookTicker+SeqLock): {:.2} ns/op | {:.2} M ops/s | allocs: {} ({} B)",
        dt.as_nanos() as f64 / n as f64,
        n as f64 / dt.as_secs_f64() / 1e6,
        a1 - a0,
        b1 - b0
    );

    let (a0, b0) = snapshot_allocs();
    let t0 = Instant::now();
    for _ in 0..n {
        std::hint::black_box(parse_book_ticker(std::hint::black_box(book_ticker_data)));
    }
    let dt = t0.elapsed();
    let (a1, b1) = snapshot_allocs();
    println!(
        "parse_book_ticker                  : {:.2} ns/op | {:.2} M ops/s | allocs: {} ({} B)",
        dt.as_nanos() as f64 / n as f64,
        n as f64 / dt.as_secs_f64() / 1e6,
        a1 - a0,
        b1 - b0
    );

    let (a0, b0) = snapshot_allocs();
    let t0 = Instant::now();
    for _ in 0..n {
        std::hint::black_box(parse_agg_trade(std::hint::black_box(agg_trade_data)));
    }
    let dt = t0.elapsed();
    let (a1, b1) = snapshot_allocs();
    println!(
        "parse_agg_trade                    : {:.2} ns/op | {:.2} M ops/s | allocs: {} ({} B)",
        dt.as_nanos() as f64 / n as f64,
        n as f64 / dt.as_secs_f64() / 1e6,
        a1 - a0,
        b1 - b0
    );

    let (a0, b0) = snapshot_allocs();
    let t0 = Instant::now();
    for _ in 0..n {
        std::hint::black_box(parse_depth_update(
            std::hint::black_box(depth_data),
            &mut bids_scratch,
            &mut asks_scratch,
        ));
    }
    let dt = t0.elapsed();
    let (a1, b1) = snapshot_allocs();
    println!(
        "parse_depth_update                 : {:.2} ns/op | {:.2} M ops/s | allocs: {} ({} B)",
        dt.as_nanos() as f64 / n as f64,
        n as f64 / dt.as_secs_f64() / 1e6,
        a1 - a0,
        b1 - b0
    );

    // Klines 500 bars into caller buffer
    let mut klines_json = String::from("[");
    for i in 0..500 {
        if i > 0 {
            klines_json.push(',');
        }
        klines_json.push_str(r#"[1700000000000,"64000.1","64100.5","63950.0","64080.2","123.45",1700000059999,"7900000.1",420,"60.1","3850000.0","0"]"#);
    }
    klines_json.push(']');
    let mut k_out = [Kline::default(); 500];
    let kn = 10_000u64;
    let (a0, b0) = snapshot_allocs();
    let t0 = Instant::now();
    for _ in 0..kn {
        std::hint::black_box(parse_klines_into(klines_json.as_bytes(), &mut k_out));
    }
    let dt = t0.elapsed();
    let (a1, b1) = snapshot_allocs();
    println!(
        "parse_klines_into(500)             : {:.2} us/op | {:.2} K ops/s | allocs: {} ({} B)",
        dt.as_nanos() as f64 / kn as f64 / 1000.0,
        kn as f64 / dt.as_secs_f64() / 1000.0,
        a1 - a0,
        b1 - b0
    );

    // Signed REST + WS-API request builder on stack
    let (a0, b0) = snapshot_allocs();
    let t0 = Instant::now();
    for i in 0..200_000u64 {
        let mut r = StackHttpRequest::new(b"POST", b"", b"/fapi/v1/order");
        r.add_str(b"symbol", b"BTCUSDT");
        r.add_str(b"side", b"BUY");
        r.add_str(b"type", b"LIMIT");
        r.add_str(b"timeInForce", b"GTC");
        r.add_f64(b"quantity", 0.01);
        r.add_f64(b"price", 64250.1);
        r.add_i64(b"timestamp", 1_700_000_000_000 + (i as i64));
        let wire = r.finish(
            b"127.0.0.1:8080",
            Some(b"test_api_key"),
            Some(b"test_api_secret"),
            true,
        );
        std::hint::black_box(wire.len());
    }
    let dt = t0.elapsed();
    let (a1, b1) = snapshot_allocs();
    println!(
        "StackHttpRequest+HMAC-SHA256       : {:.2} ns/op | {:.2} M ops/s | allocs: {} ({} B)",
        dt.as_nanos() as f64 / 200_000.0,
        200_000.0 / dt.as_secs_f64() / 1e6,
        a1 - a0,
        b1 - b0
    );

    let (a0, b0) = snapshot_allocs();
    let t0 = Instant::now();
    for i in 0..200_000u64 {
        let mut r = StackWsApiRequest::new(i + 1, b"order.place");
        r.add_str_param(b"apiKey", b"test_api_key", true);
        r.add_f64_param(b"price", 64250.1, true);
        r.add_f64_param(b"quantity", 0.01, true);
        r.add_str_param(b"side", b"BUY", true);
        r.add_str_param(b"symbol", b"BTCUSDT", true);
        r.add_str_param(b"timeInForce", b"GTC", true);
        r.add_i64_param(b"timestamp", 1_700_000_000_000 + (i as i64), true);
        r.add_str_param(b"type", b"LIMIT", true);
        let wire = r.finish_signed(b"test_api_secret");
        std::hint::black_box(wire.len());
    }
    let dt = t0.elapsed();
    let (a1, b1) = snapshot_allocs();
    println!(
        "StackWsApiRequest+HMAC-SHA256      : {:.2} ns/op | {:.2} M ops/s | allocs: {} ({} B)",
        dt.as_nanos() as f64 / 200_000.0,
        200_000.0 / dt.as_secs_f64() / 1e6,
        a1 - a0,
        b1 - b0
    );

    let _ = (read_order_book_into as fn(&[u8], bool, &mut [DepthLevel], &mut [DepthLevel]) -> _, OrderBookSet::new);
}

async fn run_rest(base_url: &str, iters: usize) {
    let client = BinanceClient::new(BinanceClientOptions {
        rest_base_url: base_url.to_string(),
        api_key: "bench_key".to_string(),
        api_secret: "bench_secret".to_string(),
        limits: BinanceLimits::unlimited(),
        ..Default::default()
    })
    .unwrap();
    client.warmup_rest(2).await.expect("REST warmup failed");

    let sym = Symbol::parse("BTCUSDT").unwrap();
    let mut klines = [Kline::default(); 500];
    let mut bids = [DepthLevel::default(); 1000];
    let mut asks = [DepthLevel::default(); 1000];

    // Warmup
    for _ in 0..50 {
        client
            .klines_into(sym, BinanceInterval::OneMinute, None, None, Some(500), &mut klines)
            .await
            .expect("klines warmup");
    }

    let mut lats = Vec::with_capacity(iters);
    let (a0, b0) = snapshot_allocs();
    for _ in 0..iters {
        let t = Instant::now();
        let n = client
            .klines_into(sym, BinanceInterval::OneMinute, None, None, Some(500), &mut klines)
            .await
            .expect("klines_into");
        std::hint::black_box(n);
        lats.push(t.elapsed().as_nanos() as u64);
    }
    let (a1, b1) = snapshot_allocs();
    let (p50, p95, p99, mean) = summarize_ns(&mut lats);
    println!(
        "REST klines_into(500) x{iters}: p50={:.2}us p95={:.2}us p99={:.2}us mean={:.2}us | total allocs={} ({} B)",
        p50 as f64 / 1000.0, p95 as f64 / 1000.0, p99 as f64 / 1000.0, mean / 1000.0, a1 - a0, b1 - b0
    );

    // order_book_into(1000)
    for _ in 0..20 {
        let _ = client.order_book_into(sym, 1000, &mut bids, &mut asks).await;
    }
    lats.clear();
    let (a0, b0) = snapshot_allocs();
    for _ in 0..iters {
        let t = Instant::now();
        let snap = client
            .order_book_into(sym, 1000, &mut bids, &mut asks)
            .await
            .expect("order_book_into");
        std::hint::black_box(snap);
        lats.push(t.elapsed().as_nanos() as u64);
    }
    let (a1, b1) = snapshot_allocs();
    let (p50, p95, p99, mean) = summarize_ns(&mut lats);
    println!(
        "REST order_book_into(1000) x{iters}: p50={:.2}us p95={:.2}us p99={:.2}us mean={:.2}us | total allocs={} ({} B)",
        p50 as f64 / 1000.0, p95 as f64 / 1000.0, p99 as f64 / 1000.0, mean / 1000.0, a1 - a0, b1 - b0
    );

    // new_order
    let mut order = NewOrderRequest {
        symbol: sym,
        side: OrderSide::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::Gtc,
        quantity: 0.01,
        price: 64250.10,
        client_order_id: ClientOrderId::parse("axitor_bench_001").unwrap(),
        ..Default::default()
    };
    for _ in 0..20 {
        let _ = client.new_order(&mut order).await;
    }
    lats.clear();
    let (a0, b0) = snapshot_allocs();
    for _ in 0..iters {
        let t = Instant::now();
        let res = client.new_order(&mut order).await.expect("new_order");
        std::hint::black_box(res);
        lats.push(t.elapsed().as_nanos() as u64);
    }
    let (a1, b1) = snapshot_allocs();
    let (p50, p95, p99, mean) = summarize_ns(&mut lats);
    println!(
        "REST new_order x{iters}: p50={:.2}us p95={:.2}us p99={:.2}us mean={:.2}us | total allocs={} ({} B)",
        p50 as f64 / 1000.0, p95 as f64 / 1000.0, p99 as f64 / 1000.0, mean / 1000.0, a1 - a0, b1 - b0
    );
}

async fn run_wsapi(ws_api_url: &str, iters: usize) {
    let client = BinanceClient::new(BinanceClientOptions {
        ws_api_url: ws_api_url.to_string(),
        api_key: "bench_key".to_string(),
        api_secret: "bench_secret".to_string(),
        limits: BinanceLimits::unlimited(),
        ws_api_lanes: 1,
        ..Default::default()
    })
    .unwrap();
    client.connect_ws_api().await.expect("connect_ws_api failed");

    let sym = Symbol::parse("BTCUSDT").unwrap();

    for _ in 0..50 {
        let _ = client.ws_ticker_price(sym).await.expect("ticker warmup");
    }

    let mut lats = Vec::with_capacity(iters);
    let (a0, b0) = snapshot_allocs();
    for _ in 0..iters {
        let t = Instant::now();
        let p = client.ws_ticker_price(sym).await.expect("ws_ticker_price");
        std::hint::black_box(p);
        lats.push(t.elapsed().as_nanos() as u64);
    }
    let (a1, b1) = snapshot_allocs();
    let (p50, p95, p99, mean) = summarize_ns(&mut lats);
    println!(
        "WS-API ticker.price x{iters}: p50={:.2}us p95={:.2}us p99={:.2}us mean={:.2}us | total allocs={} ({} B)",
        p50 as f64 / 1000.0, p95 as f64 / 1000.0, p99 as f64 / 1000.0, mean / 1000.0, a1 - a0, b1 - b0
    );

    let mut order = NewOrderRequest {
        symbol: sym,
        side: OrderSide::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::Gtc,
        quantity: 0.01,
        price: 64250.10,
        client_order_id: ClientOrderId::parse("axitor_bench_001").unwrap(),
        ..Default::default()
    };
    for _ in 0..50 {
        let _ = client.ws_new_order(&mut order).await.expect("ws_new_order warmup");
    }
    lats.clear();
    let (a0, b0) = snapshot_allocs();
    for _ in 0..iters {
        let t = Instant::now();
        let r = client.ws_new_order(&mut order).await.expect("ws_new_order");
        std::hint::black_box(r);
        lats.push(t.elapsed().as_nanos() as u64);
    }
    let (a1, b1) = snapshot_allocs();
    let (p50, p95, p99, mean) = summarize_ns(&mut lats);
    println!(
        "WS-API order.place x{iters}: p50={:.2}us p95={:.2}us p99={:.2}us mean={:.2}us | total allocs={} ({} B)",
        p50 as f64 / 1000.0, p95 as f64 / 1000.0, p99 as f64 / 1000.0, mean / 1000.0, a1 - a0, b1 - b0
    );
}

async fn run_streams(ws_url: &str, stream_name: &str, secs: u64) {
    let client = BinanceClient::new(BinanceClientOptions {
        ws_streams_url: ws_url.to_string(),
        limits: BinanceLimits::unlimited(),
        ..Default::default()
    })
    .unwrap();
    let kind = StreamKind::classify(stream_name.as_bytes());
    let sub_id = client
        .register_stream(stream_name, kind)
        .expect("register stream");
    let shard = client
        .connect_market_streams()
        .await
        .expect("connect streams");

    shard
        .subscribe_batch(&[stream_name.to_string()])
        .await
        .expect("subscribe batch");

    // Warmup 20k frames (or 300ms max) before starting measurement window
    let warmup_deadline = Instant::now() + Duration::from_millis(500);
    while shard.frames_received.load(Ordering::Relaxed) < 20_000 && Instant::now() < warmup_deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let start_frames = shard.frames_received.load(Ordering::Relaxed);
    let t0 = Instant::now();
    let (a0, b0) = snapshot_allocs();
    tokio::time::sleep(Duration::from_secs(secs)).await;
    let elapsed = t0.elapsed();
    let end_frames = shard.frames_received.load(Ordering::Relaxed);
    let (a1, b1) = snapshot_allocs();
    let frames = end_frames.saturating_sub(start_frames);

    let latest = client.try_get_book_ticker(sub_id);
    println!(
        "Streams {stream_name} ({:.2}s): frames={frames} ({:.0} msg/s) | window allocs={} ({} B) | latest_bt={}",
        elapsed.as_secs_f64(),
        frames as f64 / elapsed.as_secs_f64(),
        a1 - a0,
        b1 - b0,
        latest.is_some()
    );
    shard.close().await;
}

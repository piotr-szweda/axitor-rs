use crate::order_rules::{preflight_order, OrderRuleViolation};
use crate::parsing::fast_decimal::{format_i64, format_plain_decimal, format_u64};
use crate::parsing::rest::{parse_account_info_v2, parse_order};
use crate::parsing::ws_api::{
    extract_ws_api_id, parse_account_balance, parse_ticker_price, parse_ws_api_envelope,
};
use crate::primitives::{
    AccountBalanceModel, AccountInfoV2Model, ChangeLeverageResponse, ChangeMarginTypeResponse,
    ClientOrderId, MarginType, ModifyOrderRequest, NewOrderRequest, OrderModel, OrderResponseType,
    OrderSide, OrderType, PositionSide, PriceMatch, SelfTradePreventionMode, Symbol, TimeInForce,
    TickerPriceModel,
};
use crate::rate_limiter::{bucket, BinanceRateLimiter};
use crate::resiliency::network_gate::NetworkGate;
use crate::transport::ws_frame::{
    connect_and_handshake, encode_masked_frame, write_masked_frame, DecodedFrame, FastMaskRng,
    ParsedWsUrl, WsOpcode, WsReadBuffer,
};
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use sha2::Sha256;
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tokio::io::{AsyncWriteExt, WriteHalf};
use tokio::net::TcpStream;

type HmacSha256 = Hmac<Sha256>;

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";

/// Zero-allocation stack HMAC-SHA256 hex digest writer (`out` must be 64 bytes).
#[inline]
pub fn sign_hmac_sha256_hex(secret: &[u8], payload: &[u8], out: &mut [u8; 64]) {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(payload);
    let digest = mac.finalize().into_bytes();
    for (i, &b) in digest.iter().enumerate() {
        out[i * 2] = HEX_LOWER[(b >> 4) as usize];
        out[i * 2 + 1] = HEX_LOWER[(b & 0x0F) as usize];
    }
}

#[derive(Debug, Clone)]
pub enum WsApiError {
    Transport(String),
    Exchange { status: i32, code: i32 },
    Parse(&'static str),
    RuleViolation(OrderRuleViolation),
    RateLimited { retry_after_ms: i64 },
    CapacityExhausted,
}

impl std::fmt::Display for WsApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(s) => write!(f, "ws-api transport error: {s}"),
            Self::Exchange { status, code } => {
                write!(f, "ws-api exchange error status={status} code={code}")
            }
            Self::Parse(msg) => write!(f, "ws-api parse error: {msg}"),
            Self::RuleViolation(v) => write!(f, "order rule violation: {v:?}"),
            Self::RateLimited { retry_after_ms } => {
                write!(f, "rate limited, retry after {retry_after_ms}ms")
            }
            Self::CapacityExhausted => write!(f, "ws-api pending slot table exhausted"),
        }
    }
}

impl std::error::Error for WsApiError {}

const SLOT_FREE: u8 = 0;
const SLOT_ARMED: u8 = 1;
const SLOT_DONE: u8 = 2;
const SLOT_FAILED: u8 = 3;

const TABLE_SIZE: usize = 256;
const TABLE_MASK: usize = TABLE_SIZE - 1;

struct PendingSlot {
    state: AtomicU8,
    req_id: AtomicU64,
    waker: Mutex<Option<Waker>>,
    payload: UnsafeCell<Vec<u8>>,
    err: UnsafeCell<Option<String>>,
}

unsafe impl Sync for PendingSlot {}
unsafe impl Send for PendingSlot {}

impl PendingSlot {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(SLOT_FREE),
            req_id: AtomicU64::new(0),
            waker: Mutex::new(None),
            payload: UnsafeCell::new(Vec::with_capacity(8192)),
            err: UnsafeCell::new(None),
        }
    }
}

/// Fixed-capacity lock-free-index correlation table matching Axitor's `WsApiPendingSlot` design:
/// 0 heap allocations per RPC call on steady-state responses.
pub struct WsApiPendingTable {
    slots: Vec<PendingSlot>,
    next_id: AtomicU64,
}

impl WsApiPendingTable {
    pub fn new() -> Self {
        let mut slots = Vec::with_capacity(TABLE_SIZE);
        for _ in 0..TABLE_SIZE {
            slots.push(PendingSlot::new());
        }
        Self {
            slots,
            next_id: AtomicU64::new(1),
        }
    }

    /// Reserves a slot and returns `(request_id, slot_index)`.
    pub fn arm(&self) -> Option<(u64, usize)> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let start = (id as usize) & TABLE_MASK;
        for probe in 0..TABLE_SIZE {
            let idx = (start + probe) & TABLE_MASK;
            let slot = &self.slots[idx];
            if slot
                .state
                .compare_exchange(SLOT_FREE, SLOT_ARMED, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                slot.req_id.store(id, Ordering::Release);
                unsafe {
                    (*slot.payload.get()).clear();
                    *slot.err.get() = None;
                }
                return Some((id, idx));
            }
        }
        None
    }

    pub fn complete(&self, req_id: u64, data: &[u8]) -> bool {
        let start = (req_id as usize) & TABLE_MASK;
        for probe in 0..TABLE_SIZE {
            let idx = (start + probe) & TABLE_MASK;
            let slot = &self.slots[idx];
            let st = slot.state.load(Ordering::Acquire);
            if st == SLOT_ARMED && slot.req_id.load(Ordering::Acquire) == req_id {
                unsafe {
                    let dst = &mut *slot.payload.get();
                    dst.clear();
                    dst.extend_from_slice(data);
                }
                slot.state.store(SLOT_DONE, Ordering::Release);
                let w = slot.waker.lock().take();
                if let Some(waker) = w {
                    waker.wake();
                }
                return true;
            }
        }
        false
    }

    pub fn fail_all(&self, msg: &str) {
        for slot in &self.slots {
            if slot.state.load(Ordering::Acquire) == SLOT_ARMED {
                unsafe {
                    *slot.err.get() = Some(msg.to_string());
                }
                slot.state.store(SLOT_FAILED, Ordering::Release);
                let w = slot.waker.lock().take();
                if let Some(waker) = w {
                    waker.wake();
                }
            }
        }
    }

    #[inline]
    pub fn release(&self, idx: usize) {
        let slot = &self.slots[idx];
        *slot.waker.lock() = None;
        slot.state.store(SLOT_FREE, Ordering::Release);
    }

    /// Reads the response payload in-place once `SlotAwait` completes cleanly, before `Drop` frees the slot.
    #[inline]
    pub unsafe fn payload_slice(&self, idx: usize) -> &[u8] {
        unsafe { (&*self.slots[idx].payload.get()).as_slice() }
    }
}

impl Default for WsApiPendingTable {
    fn default() -> Self {
        Self::new()
    }
}

struct SlotGuard<'a> {
    table: &'a WsApiPendingTable,
    slot_idx: usize,
}

impl<'a> Drop for SlotGuard<'a> {
    #[inline]
    fn drop(&mut self) {
        self.table.release(self.slot_idx);
    }
}

struct SlotWaiter<'a> {
    table: &'a WsApiPendingTable,
    slot_idx: usize,
    req_id: u64,
}

impl<'a> Future for SlotWaiter<'a> {
    type Output = Result<(), WsApiError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let slot = &self.table.slots[self.slot_idx];
        let st = slot.state.load(Ordering::Acquire);
        match st {
            SLOT_DONE => Poll::Ready(Ok(())),
            SLOT_FAILED => {
                let err = unsafe { (*slot.err.get()).take() }
                    .unwrap_or_else(|| "ws-api connection failed".to_string());
                Poll::Ready(Err(WsApiError::Transport(err)))
            }
            SLOT_ARMED => {
                {
                    let mut guard = slot.waker.lock();
                    // Re-check after acquiring waker lock to avoid lost wakeups
                    let st2 = slot.state.load(Ordering::Acquire);
                    if st2 == SLOT_DONE {
                        return Poll::Ready(Ok(()));
                    }
                    if st2 == SLOT_FAILED {
                        let err = unsafe { (*slot.err.get()).take() }
                            .unwrap_or_else(|| "ws-api connection failed".to_string());
                        return Poll::Ready(Err(WsApiError::Transport(err)));
                    }
                    *guard = Some(cx.waker().clone());
                }
                Poll::Pending
            }
            _ => {
                let _ = self.req_id;
                Poll::Ready(Err(WsApiError::Transport("unexpected slot state".to_string())))
            }
        }
    }
}

/// Zero-allocation builder that emits canonical Binance WebSocket API JSON requests
/// onto a stack buffer (plus simultaneous HMAC-SHA256 query string construction).
pub struct StackWsApiRequest {
    json: [u8; 4096],
    json_len: usize,
    query: [u8; 2048],
    query_len: usize,
}

impl StackWsApiRequest {
    #[inline]
    pub fn new(id: u64, method: &[u8]) -> Self {
        let mut s = Self {
            json: [0u8; 4096],
            json_len: 0,
            query: [0u8; 2048],
            query_len: 0,
        };
        s.push_json(br#"{"id":"#);
        let mut ibuf = [0u8; 24];
        let n = format_u64(id, &mut ibuf);
        s.push_json(&ibuf[..n]);
        s.push_json(br#","method":""#);
        s.push_json(method);
        s.push_json(br#"","params":{"#);
        s
    }

    #[inline]
    fn push_json(&mut self, bytes: &[u8]) {
        let end = self.json_len + bytes.len();
        debug_assert!(end <= self.json.len());
        self.json[self.json_len..end].copy_from_slice(bytes);
        self.json_len = end;
    }

    #[inline]
    fn push_query(&mut self, bytes: &[u8]) {
        let end = self.query_len + bytes.len();
        debug_assert!(end <= self.query.len());
        self.query[self.query_len..end].copy_from_slice(bytes);
        self.query_len = end;
    }

    #[inline]
    fn start_param(&mut self, key: &[u8], signed: bool) {
        if !self.json[..self.json_len].ends_with(b"{") {
            self.push_json(b",");
        }
        self.push_json(b"\"");
        self.push_json(key);
        self.push_json(b"\":");

        if signed {
            if self.query_len > 0 {
                self.push_query(b"&");
            }
            self.push_query(key);
            self.push_query(b"=");
        }
    }

    #[inline]
    pub fn add_str_param(&mut self, key: &[u8], val: &[u8], signed: bool) {
        self.start_param(key, signed);
        self.push_json(b"\"");
        self.push_json(val);
        self.push_json(b"\"");
        if signed {
            self.push_query(val);
        }
    }

    #[inline]
    pub fn add_i64_param(&mut self, key: &[u8], val: i64, signed: bool) {
        self.start_param(key, signed);
        let mut buf = [0u8; 24];
        let n = format_i64(val, &mut buf);
        self.push_json(&buf[..n]);
        if signed {
            self.push_query(&buf[..n]);
        }
    }

    #[inline]
    pub fn add_f64_param(&mut self, key: &[u8], val: f64, signed: bool) {
        self.start_param(key, signed);
        let mut buf = [0u8; 48];
        let n = format_plain_decimal(val, &mut buf).unwrap_or_else(|| {
            buf[0] = b'0';
            1
        });
        // Binance WS API encodes decimal order fields as plain numbers or strings; Axitor sends stringified/plain
        self.push_json(b"\"");
        self.push_json(&buf[..n]);
        self.push_json(b"\"");
        if signed {
            self.push_query(&buf[..n]);
        }
    }

    #[inline]
    pub fn finish_unsigned(&mut self) -> &[u8] {
        self.push_json(b"}}");
        &self.json[..self.json_len]
    }

    #[inline]
    pub fn finish_signed(&mut self, api_secret: &[u8]) -> &[u8] {
        let mut sig = [0u8; 64];
        sign_hmac_sha256_hex(api_secret, &self.query[..self.query_len], &mut sig);
        self.add_str_param(b"signature", &sig, false);
        self.push_json(b"}}");
        &self.json[..self.json_len]
    }
}

struct LaneWriter {
    writer: WriteHalf<TcpStream>,
    mask_rng: FastMaskRng,
    frame_buf: Vec<u8>,
}

struct WsApiLane {
    writer: Arc<tokio::sync::Mutex<LaneWriter>>,
    pending: Arc<WsApiPendingTable>,
    alive: Arc<AtomicBool>,
    reader_task: Option<tokio::task::JoinHandle<()>>,
}

impl WsApiLane {
    async fn connect(url: &str, seed: u64) -> Result<Self, WsApiError> {
        let parsed = ParsedWsUrl::parse(url, "").map_err(WsApiError::Transport)?;
        let mut read_buf = WsReadBuffer::new(128 * 1024);
        let stream = connect_and_handshake(&parsed, &mut read_buf)
            .await
            .map_err(WsApiError::Transport)?;

        let (reader, writer) = tokio::io::split(stream);
        let pending = Arc::new(WsApiPendingTable::new());
        let alive = Arc::new(AtomicBool::new(true));
        let writer = Arc::new(tokio::sync::Mutex::new(LaneWriter {
            writer,
            mask_rng: FastMaskRng::new(seed),
            frame_buf: vec![0u8; 8192],
        }));

        let writer_bg = Arc::clone(&writer);
        let pending_bg = Arc::clone(&pending);
        let alive_bg = Arc::clone(&alive);

        let reader_task = tokio::spawn(async move {
            run_ws_api_reader(reader, read_buf, writer_bg, pending_bg, alive_bg).await;
        });

        Ok(Self {
            writer,
            pending,
            alive,
            reader_task: Some(reader_task),
        })
    }

    #[inline]
    async fn send_payload(&self, payload: &[u8]) -> Result<(), WsApiError> {
        let mut w = self.writer.lock().await;
        let needed = payload.len() + 14;
        if w.frame_buf.len() < needed {
            w.frame_buf.resize(needed, 0);
        }
        let mask = w.mask_rng.next_mask();
        let LaneWriter {
            writer,
            frame_buf,
            ..
        } = &mut *w;
        let n = encode_masked_frame(WsOpcode::Text, payload, mask, frame_buf);
        writer
            .write_all(&frame_buf[..n])
            .await
            .map_err(|e| WsApiError::Transport(format!("ws-api write failed: {e}")))
    }
}

async fn run_ws_api_reader(
    mut reader: tokio::io::ReadHalf<TcpStream>,
    mut read_buf: WsReadBuffer,
    writer: Arc<tokio::sync::Mutex<LaneWriter>>,
    pending: Arc<WsApiPendingTable>,
    alive: Arc<AtomicBool>,
) {
    let mut pong_scratch = [0u8; 125];
    'outer: loop {
        loop {
            let ping_len: Option<usize>;
            match read_buf.next_frame() {
                Ok(Some(DecodedFrame::Message { payload, .. })) => {
                    if let Some(id) = extract_ws_api_id(payload) {
                        if id >= 0 {
                            pending.complete(id as u64, payload);
                        }
                    }
                    continue;
                }
                Ok(Some(DecodedFrame::Ping(p))) => {
                    let n = p.len().min(125);
                    pong_scratch[..n].copy_from_slice(&p[..n]);
                    ping_len = Some(n);
                }
                Ok(Some(DecodedFrame::Pong(_))) => continue,
                Ok(Some(DecodedFrame::Close(_))) | Err(_) => break 'outer,
                Ok(None) => break,
            }

            if let Some(n) = ping_len {
                let mut w = writer.lock().await;
                let LaneWriter {
                    writer: wr,
                    mask_rng,
                    frame_buf,
                } = &mut *w;
                if write_masked_frame(wr, WsOpcode::Pong, &pong_scratch[..n], mask_rng, frame_buf)
                    .await
                    .is_err()
                {
                    break 'outer;
                }
            }
        }

        match read_buf.read_more(&mut reader).await {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
    }

    alive.store(false, Ordering::Release);
    pending.fail_all("ws-api connection closed");
}

impl Drop for WsApiLane {
    fn drop(&mut self) {
        if let Some(h) = self.reader_task.take() {
            h.abort();
        }
    }
}

/// Zero-allocation pipelined Binance WebSocket API transport supporting multiple striped lanes.
pub struct WsApiTransport {
    lanes: Vec<WsApiLane>,
    rr: AtomicUsize,
    api_key: String,
    api_secret: Vec<u8>,
    rate_limiter: Arc<BinanceRateLimiter>,
    network_gate: Arc<NetworkGate>,
    recv_window_ms: i64,
    request_timeout: Duration,
}

impl WsApiTransport {
    pub async fn connect(
        url: &str,
        lane_count: usize,
        api_key: String,
        api_secret: String,
        rate_limiter: Arc<BinanceRateLimiter>,
        network_gate: Arc<NetworkGate>,
        recv_window_ms: i64,
        request_timeout: Duration,
    ) -> Result<Self, WsApiError> {
        let count = lane_count.max(1);
        let mut lanes = Vec::with_capacity(count);
        for i in 0..count {
            let lane = WsApiLane::connect(url, 0xCAFE_1234_0000_0001u64 + (i as u64)).await?;
            lanes.push(lane);
        }
        Ok(Self {
            lanes,
            rr: AtomicUsize::new(0),
            api_key,
            api_secret: api_secret.into_bytes(),
            rate_limiter,
            network_gate,
            recv_window_ms,
            request_timeout,
        })
    }

    #[inline]
    fn pick_lane(&self) -> &WsApiLane {
        let n = self.lanes.len();
        if n == 1 {
            return &self.lanes[0];
        }
        let idx = self.rr.fetch_add(1, Ordering::Relaxed) % n;
        for offset in 0..n {
            let candidate = &self.lanes[(idx + offset) % n];
            if candidate.alive.load(Ordering::Acquire) {
                return candidate;
            }
        }
        &self.lanes[idx]
    }

    #[inline]
    fn check_rate_limit(&self, buckets: u32, weight: i32) -> Result<(), WsApiError> {
        self.rate_limiter
            .try_reserve(buckets, weight)
            .map_err(|retry_after_ms| WsApiError::RateLimited { retry_after_ms })
    }

    #[inline]
    async fn execute_rpc<R>(
        &self,
        build: impl FnOnce(u64, &mut StackWsApiRequest) -> usize,
        method: &[u8],
        parse: impl FnOnce(&[u8]) -> Option<R>,
    ) -> Result<R, WsApiError> {
        // Wait until network gate is open (halts during bans or disconnects)
        self.network_gate.wait_until_open().await;

        let lane = self.pick_lane();
        let (req_id, slot_idx) = lane
            .pending
            .arm()
            .ok_or(WsApiError::CapacityExhausted)?;
        let _guard = SlotGuard {
            table: &lane.pending,
            slot_idx,
        };

        let send_len = {
            let mut req = StackWsApiRequest::new(req_id, method);
            let len = build(req_id, &mut req);
            let payload = &req.json[..len];
            lane.send_payload(payload).await?;
            len
        };
        let _ = send_len;

        let waiter = SlotWaiter {
            table: &lane.pending,
            slot_idx,
            req_id,
        };

        match tokio::time::timeout(self.request_timeout, waiter).await {
            Ok(res) => res?,
            Err(_) => {
                return Err(WsApiError::Transport(
                    "ws-api request timed out while awaiting response: outcome unknown / maybe sent".into(),
                ));
            }
        }

        let raw = unsafe { lane.pending.payload_slice(slot_idx) };
        let env = parse_ws_api_envelope(raw).ok_or(WsApiError::Parse("envelope"))?;

        if env.status == 418 || env.status == 429 {
            self.network_gate.trip();
        }

        if env.status != 200 {
            return Err(WsApiError::Exchange {
                status: env.status,
                code: env.exchange_code,
            });
        }
        parse(env.result).ok_or(WsApiError::Parse("result payload"))
    }

    /// `ticker.price` — zero-alloc unsigned market price lookup.
    pub async fn ticker_price(&self, symbol: Symbol) -> Result<TickerPriceModel, WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            if symbol.is_empty() { 2 } else { 1 },
        )?;
        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);

        self.execute_rpc(
            |_, req| {
                if sym_len > 0 {
                    req.add_str_param(b"symbol", &sym_buf[..sym_len], false);
                }
                req.finish_unsigned().len()
            },
            b"ticker.price",
            parse_ticker_price,
        )
        .await
    }

    /// `v2/account.balance` — zero-alloc signed account balance query.
    pub async fn account_balance(&self) -> Result<AccountBalanceModel, WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            5,
        )?;
        let ts = BinanceRateLimiter::utc_millis();
        let recv = self.recv_window_ms;

        self.execute_rpc(
            |_, req| {
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), true);
                if recv > 0 {
                    req.add_i64_param(b"recvWindow", recv, true);
                }
                req.add_i64_param(b"timestamp", ts, true);
                req.finish_signed(&self.api_secret).len()
            },
            b"v2/account.balance",
            parse_account_balance,
        )
        .await
    }

    /// `account.status` / `v2/account.status`
    pub async fn account_status(&self) -> Result<AccountInfoV2Model, WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            5,
        )?;
        let ts = BinanceRateLimiter::utc_millis();
        let recv = self.recv_window_ms;

        self.execute_rpc(
            |_, req| {
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), true);
                if recv > 0 {
                    req.add_i64_param(b"recvWindow", recv, true);
                }
                req.add_i64_param(b"timestamp", ts, true);
                req.finish_signed(&self.api_secret).len()
            },
            b"account.status",
            parse_account_info_v2,
        )
        .await
    }

    /// `order.place` — validates preflight rules, signs on stack, sends without heap allocation.
    pub async fn new_order(&self, req_order: &mut NewOrderRequest) -> Result<OrderModel, WsApiError> {
        let violation = preflight_order(req_order);
        if violation != OrderRuleViolation::None {
            return Err(WsApiError::RuleViolation(violation));
        }

        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT
                | bucket::FUTURES_WS_API_REQUEST_WEIGHT
                | bucket::FUTURES_ORDER_MINUTE
                | bucket::FUTURES_ORDER_10S,
            1,
        )?;

        let ts = BinanceRateLimiter::utc_millis();
        let recv = self.recv_window_ms;
        let mut sym_buf = [0u8; 16];
        let sym_len = req_order.symbol.write_to(&mut sym_buf);
        let order_snap = *req_order;

        self.execute_rpc(
            |_, req| {
                // Alphabetical key order for both JSON params & HMAC query string!
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), true);
                if order_snap.good_till_date > 0 {
                    req.add_i64_param(b"goodTillDate", order_snap.good_till_date, true);
                }
                if !order_snap.client_order_id.is_empty() {
                    req.add_str_param(
                        b"newClientOrderId",
                        order_snap.client_order_id.as_bytes(),
                        true,
                    );
                }
                if order_snap.response_type != OrderResponseType::Ack {
                    req.add_str_param(
                        b"newOrderRespType",
                        order_snap.response_type.as_bytes(),
                        true,
                    );
                }
                if order_snap.position_side != PositionSide::Unknown {
                    req.add_str_param(
                        b"positionSide",
                        order_snap.position_side.as_bytes(),
                        true,
                    );
                }
                if order_snap.price > 0.0 {
                    req.add_f64_param(b"price", order_snap.price, true);
                }
                if !matches!(order_snap.price_match, PriceMatch::Unknown | PriceMatch::None) {
                    req.add_str_param(
                        b"priceMatch",
                        order_snap.price_match.as_bytes(),
                        true,
                    );
                }
                if order_snap.quantity > 0.0 {
                    req.add_f64_param(b"quantity", order_snap.quantity, true);
                }
                if recv > 0 {
                    req.add_i64_param(b"recvWindow", recv, true);
                }
                if order_snap.is_reduce_only {
                    req.add_str_param(b"reduceOnly", b"true", true);
                }
                if !matches!(
                    order_snap.self_trade_prevention_mode,
                    SelfTradePreventionMode::Unknown | SelfTradePreventionMode::None
                ) {
                    req.add_str_param(
                        b"selfTradePreventionMode",
                        order_snap.self_trade_prevention_mode.as_bytes(),
                        true,
                    );
                }
                if order_snap.side != OrderSide::Unknown {
                    req.add_str_param(b"side", order_snap.side.as_bytes(), true);
                }
                req.add_str_param(b"symbol", &sym_buf[..sym_len], true);
                if order_snap.time_in_force != TimeInForce::Unknown {
                    req.add_str_param(
                        b"timeInForce",
                        order_snap.time_in_force.as_bytes(),
                        true,
                    );
                }
                req.add_i64_param(b"timestamp", ts, true);
                if order_snap.order_type != OrderType::Unknown {
                    req.add_str_param(
                        b"type",
                        order_snap.order_type.as_bytes(),
                        true,
                    );
                }
                req.finish_signed(&self.api_secret).len()
            },
            b"order.place",
            parse_order,
        )
        .await
    }

    /// `order.cancel`
    pub async fn cancel_order(
        &self,
        symbol: Symbol,
        order_id: Option<i64>,
        orig_client_order_id: Option<&ClientOrderId>,
    ) -> Result<OrderModel, WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            1,
        )?;
        let ts = BinanceRateLimiter::utc_millis();
        let recv = self.recv_window_ms;
        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);
        let orig_bytes: &[u8] = orig_client_order_id
            .filter(|c| !c.is_empty())
            .map(|c| c.as_bytes())
            .unwrap_or(&[]);

        self.execute_rpc(
            |_, req| {
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), true);
                if let Some(oid) = order_id {
                    req.add_i64_param(b"orderId", oid, true);
                }
                if !orig_bytes.is_empty() {
                    req.add_str_param(b"origClientOrderId", &orig_bytes, true);
                }
                if recv > 0 {
                    req.add_i64_param(b"recvWindow", recv, true);
                }
                req.add_str_param(b"symbol", &sym_buf[..sym_len], true);
                req.add_i64_param(b"timestamp", ts, true);
                req.finish_signed(&self.api_secret).len()
            },
            b"order.cancel",
            parse_order,
        )
        .await
    }

    /// `order.modify`
    pub async fn modify_order(&self, req_mod: &ModifyOrderRequest) -> Result<OrderModel, WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            1,
        )?;
        let ts = BinanceRateLimiter::utc_millis();
        let recv = self.recv_window_ms;
        let mut sym_buf = [0u8; 16];
        let sym_len = req_mod.symbol.write_to(&mut sym_buf);

        self.execute_rpc(
            |_, req| {
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), true);
                if let Some(oid) = req_mod.order_id {
                    req.add_i64_param(b"orderId", oid, true);
                }
                if let Some(cid) = req_mod.orig_client_order_id {
                    if !cid.is_empty() {
                        req.add_str_param(b"origClientOrderId", cid.as_bytes(), true);
                    }
                }
                if req_mod.price > 0.0 {
                    req.add_f64_param(b"price", req_mod.price, true);
                }
                if req_mod.quantity > 0.0 {
                    req.add_f64_param(b"quantity", req_mod.quantity, true);
                }
                if recv > 0 {
                    req.add_i64_param(b"recvWindow", recv, true);
                }
                if req_mod.side != OrderSide::Unknown {
                    req.add_str_param(b"side", req_mod.side.as_bytes(), true);
                }
                req.add_str_param(b"symbol", &sym_buf[..sym_len], true);
                req.add_i64_param(b"timestamp", ts, true);
                req.finish_signed(&self.api_secret).len()
            },
            b"order.modify",
            parse_order,
        )
        .await
    }

    /// `order.cancelAll`
    pub async fn cancel_all_open_orders(&self, symbol: Symbol) -> Result<bool, WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            1,
        )?;
        let ts = BinanceRateLimiter::utc_millis();
        let recv = self.recv_window_ms;
        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);

        self.execute_rpc(
            |_, req| {
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), true);
                if recv > 0 {
                    req.add_i64_param(b"recvWindow", recv, true);
                }
                req.add_str_param(b"symbol", &sym_buf[..sym_len], true);
                req.add_i64_param(b"timestamp", ts, true);
                req.finish_signed(&self.api_secret).len()
            },
            b"order.cancelAll",
            |result| Some(memchr::memmem::find(result, br#""code":200"#).is_some() || result.starts_with(b"{")),
        )
        .await
    }

    /// `account.setLeverage`
    pub async fn change_leverage(&self, symbol: Symbol, leverage: i32) -> Result<ChangeLeverageResponse, WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            1,
        )?;
        let ts = BinanceRateLimiter::utc_millis();
        let recv = self.recv_window_ms;
        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);

        self.execute_rpc(
            |_, req| {
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), true);
                req.add_i64_param(b"leverage", leverage as i64, true);
                if recv > 0 {
                    req.add_i64_param(b"recvWindow", recv, true);
                }
                req.add_str_param(b"symbol", &sym_buf[..sym_len], true);
                req.add_i64_param(b"timestamp", ts, true);
                req.finish_signed(&self.api_secret).len()
            },
            b"account.setLeverage",
            |result| {
                let mut cursor = 0;
                let lev = crate::parsing::utf8_reader::read_i64(result, b"\"leverage\":", &mut cursor)? as i32;
                let max_n = crate::parsing::utf8_reader::read_quoted_f64(result, b"\"maxNotionalValue\":", &mut cursor).unwrap_or(0.0);
                let s = crate::parsing::utf8_reader::read_symbol(result, b"\"symbol\":", &mut cursor).unwrap_or_default();
                Some(ChangeLeverageResponse {
                    leverage: lev,
                    max_notional_value: max_n,
                    symbol: s,
                })
            },
        )
        .await
    }

    /// `account.setMarginType`
    pub async fn change_margin_type(&self, symbol: Symbol, margin_type: MarginType) -> Result<ChangeMarginTypeResponse, WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            1,
        )?;
        let ts = BinanceRateLimiter::utc_millis();
        let recv = self.recv_window_ms;
        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);

        self.execute_rpc(
            |_, req| {
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), true);
                req.add_str_param(b"marginType", margin_type.as_bytes(), true);
                if recv > 0 {
                    req.add_i64_param(b"recvWindow", recv, true);
                }
                req.add_str_param(b"symbol", &sym_buf[..sym_len], true);
                req.add_i64_param(b"timestamp", ts, true);
                req.finish_signed(&self.api_secret).len()
            },
            b"account.setMarginType",
            |result| {
                let mut cursor = 0;
                let code = crate::parsing::utf8_reader::read_i64(result, b"\"code\":", &mut cursor).unwrap_or(200) as i32;
                let msg = crate::parsing::utf8_reader::read_symbol(result, b"\"msg\":", &mut cursor).unwrap_or_default();
                Some(ChangeMarginTypeResponse { code, msg })
            },
        )
        .await
    }

    /// `userDataStream.start`
    pub async fn create_listen_key(&self) -> Result<String, WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            1,
        )?;
        self.execute_rpc(
            |_, req| {
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), false);
                req.finish_unsigned().len()
            },
            b"userDataStream.start",
            |result| {
                let key = br#""listenKey":""#;
                let idx = memchr::memmem::find(result, key)? + key.len();
                let end = memchr::memchr(b'"', &result[idx..])? + idx;
                std::str::from_utf8(&result[idx..end]).ok().map(str::to_owned)
            },
        )
        .await
    }

    /// `userDataStream.ping`
    pub async fn keepalive_listen_key(&self, listen_key: &str) -> Result<(), WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            1,
        )?;
        self.execute_rpc(
            |_, req| {
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), false);
                req.add_str_param(b"listenKey", listen_key.as_bytes(), false);
                req.finish_unsigned().len()
            },
            b"userDataStream.ping",
            |_| Some(()),
        )
        .await
    }

    /// `userDataStream.stop`
    pub async fn close_listen_key(&self, listen_key: &str) -> Result<(), WsApiError> {
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_WS_API_REQUEST_WEIGHT,
            1,
        )?;
        self.execute_rpc(
            |_, req| {
                req.add_str_param(b"apiKey", self.api_key.as_bytes(), false);
                req.add_str_param(b"listenKey", listen_key.as_bytes(), false);
                req.finish_unsigned().len()
            },
            b"userDataStream.stop",
            |_| Some(()),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stack_ws_api_builder_signed() {
        let mut req = StackWsApiRequest::new(7, b"v2/account.balance");
        req.add_str_param(b"apiKey", b"mykey", true);
        req.add_i64_param(b"timestamp", 1_700_000_000_000, true);
        let bytes = req.finish_signed(b"mysecret");
        let s = std::str::from_utf8(bytes).unwrap();
        assert!(s.starts_with(r#"{"id":7,"method":"v2/account.balance","params":{"#));
        assert!(s.contains(r#""signature":""#));
        assert!(s.ends_with("}}"));
    }

    #[test]
    fn pending_table_arm_complete_release() {
        let tbl = WsApiPendingTable::new();
        let (id, slot) = tbl.arm().unwrap();
        assert!(tbl.complete(id, b"hello"));
        let slice = unsafe { tbl.payload_slice(slot) };
        assert_eq!(slice, b"hello");
        tbl.release(slot);
    }
}


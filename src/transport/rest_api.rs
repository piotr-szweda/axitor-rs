use crate::order_rules::{preflight_order, OrderRuleViolation};
use crate::parsing::depth::{read_order_book, read_order_book_into};
use crate::parsing::fast_decimal::{format_i64, format_plain_decimal, parse_i64_fast};
use crate::parsing::rest::{
    parse_account_info_v2, parse_exchange_info, parse_klines_into, parse_leverage_bracket,
    parse_order,
};
use crate::primitives::{
    AccountInfoV2Model, BinanceInterval, ChangeLeverageResponse, ChangeMarginTypeResponse,
    ClientOrderId, DepthLevel, ExchangeInfoModel, Kline, LeverageBracketModel, MarginType,
    ModifyOrderRequest, NewOrderRequest, OrderBookModel, OrderBookSnapshotModel, OrderModel,
    OrderResponseType, OrderSide, OrderType, PositionSide, PriceMatch, SelfTradePreventionMode,
    Symbol, TimeInForce,
};
use crate::rate_limiter::{bucket, BinanceRateLimiter};
use crate::resiliency::network_gate::NetworkGate;
use crate::transport::ws_api::sign_hmac_sha256_hex;
use memchr::memmem;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub enum RestError {
    Transport(String),
    HttpStatus { status: u16, body_preview: String },
    Parse(&'static str),
    RuleViolation(OrderRuleViolation),
    RateLimited { retry_after_ms: i64 },
    MaybeSent(String),
    IpBanned { status: u16, body_preview: String },
    NetworkGateTimeout,
}

impl std::fmt::Display for RestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(s) => write!(f, "rest transport error: {s}"),
            Self::HttpStatus { status, body_preview } => {
                write!(f, "rest HTTP {status}: {body_preview}")
            }
            Self::Parse(m) => write!(f, "rest parse error: {m}"),
            Self::RuleViolation(v) => write!(f, "order rule violation: {v:?}"),
            Self::RateLimited { retry_after_ms } => {
                write!(f, "rate limited, retry after {retry_after_ms}ms")
            }
            Self::MaybeSent(s) => {
                write!(f, "outcome unknown / maybe sent (ADR-0009 order safety): {s}")
            }
            Self::IpBanned { status, body_preview } => {
                write!(f, "IP banned / rate limit exceeded (HTTP {status}): {body_preview}")
            }
            Self::NetworkGateTimeout => {
                write!(f, "network gate timed out waiting for network repair")
            }
        }
    }
}

impl std::error::Error for RestError {}

#[derive(Debug, Clone)]
pub struct ParsedHttpUrl {
    pub host_port: String,
    pub host_header: String,
    pub base_path: String,
}

impl ParsedHttpUrl {
    pub fn parse(url: &str) -> Result<Self, RestError> {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| RestError::Transport(format!("unsupported REST scheme (expected http://): {url}")))?;
        let (authority, path) = match rest.find('/') {
            Some(idx) => (&rest[..idx], rest[idx..].trim_end_matches('/')),
            None => (rest, ""),
        };
        let host_port = if authority.contains(':') {
            authority.to_string()
        } else {
            format!("{authority}:80")
        };
        Ok(Self {
            host_port,
            host_header: authority.to_string(),
            base_path: path.to_string(),
        })
    }
}

struct KeepAliveConn {
    stream: TcpStream,
    read_buf: Vec<u8>,
    body_buf: Vec<u8>,
}

impl KeepAliveConn {
    async fn connect(host_port: &str) -> Result<Self, RestError> {
        let stream = TcpStream::connect(host_port)
            .await
            .map_err(|e| RestError::Transport(format!("tcp connect {host_port}: {e}")))?;
        let _ = stream.set_nodelay(true);
        Ok(Self {
            stream,
            read_buf: vec![0u8; 512 * 1024],
            body_buf: Vec::with_capacity(256 * 1024),
        })
    }
}

/// Zero-allocation stack HTTP/1.1 request builder supporting unsigned and HMAC-SHA256 signed queries.
pub struct StackHttpRequest {
    buf: [u8; 4096],
    len: usize,
    query_start: usize,
    has_query: bool,
}

impl StackHttpRequest {
    #[inline]
    pub fn new(method: &[u8], base_path: &[u8], endpoint: &[u8]) -> Self {
        let mut s = Self {
            buf: [0u8; 4096],
            len: 0,
            query_start: 0,
            has_query: false,
        };
        s.push(method);
        s.push(b" ");
        if !base_path.is_empty() {
            s.push(base_path);
        }
        s.push(endpoint);
        s.query_start = s.len;
        s
    }

    #[inline]
    fn push(&mut self, bytes: &[u8]) {
        let end = self.len + bytes.len();
        debug_assert!(end <= self.buf.len());
        self.buf[self.len..end].copy_from_slice(bytes);
        self.len = end;
    }

    #[inline]
    fn begin_param(&mut self, key: &[u8]) {
        if !self.has_query {
            self.push(b"?");
            self.query_start = self.len;
            self.has_query = true;
        } else {
            self.push(b"&");
        }
        self.push(key);
        self.push(b"=");
    }

    #[inline]
    pub fn add_str(&mut self, key: &[u8], val: &[u8]) {
        self.begin_param(key);
        self.push(val);
    }

    #[inline]
    pub fn add_i64(&mut self, key: &[u8], val: i64) {
        self.begin_param(key);
        let mut b = [0u8; 24];
        let n = format_i64(val, &mut b);
        self.push(&b[..n]);
    }

    #[inline]
    pub fn add_f64(&mut self, key: &[u8], val: f64) {
        self.begin_param(key);
        let mut b = [0u8; 48];
        let n = format_plain_decimal(val, &mut b).unwrap_or_else(|| {
            b[0] = b'0';
            1
        });
        self.push(&b[..n]);
    }

    #[inline]
    pub fn finish(&mut self, host: &[u8], api_key: Option<&[u8]>, secret: Option<&[u8]>, is_write: bool) -> &[u8] {
        if let Some(sec) = secret {
            let query_slice = if self.has_query {
                &self.buf[self.query_start..self.len]
            } else {
                &[]
            };
            let mut sig = [0u8; 64];
            sign_hmac_sha256_hex(sec, query_slice, &mut sig);
            self.add_str(b"signature", &sig);
        }

        self.push(b" HTTP/1.1\r\nHost: ");
        self.push(host);
        self.push(b"\r\nConnection: keep-alive\r\n");
        if let Some(key) = api_key {
            if !key.is_empty() {
                self.push(b"X-MBX-APIKEY: ");
                self.push(key);
                self.push(b"\r\n");
            }
        }
        if is_write {
            self.push(b"Content-Length: 0\r\n");
        }
        self.push(b"\r\n");
        &self.buf[..self.len]
    }
}

/// Persistent HTTP/1.1 keep-alive REST transport with stack request building & zero-copy response parsing.
pub struct RestApiTransport {
    url: ParsedHttpUrl,
    api_key: String,
    api_secret: Vec<u8>,
    pool: Mutex<Vec<KeepAliveConn>>,
    max_pool: usize,
    rate_limiter: Arc<BinanceRateLimiter>,
    network_gate: Arc<NetworkGate>,
    recv_window_ms: i64,
    request_timeout: Duration,
}

impl RestApiTransport {
    pub fn new(
        base_url: &str,
        api_key: String,
        api_secret: String,
        max_pool: usize,
        rate_limiter: Arc<BinanceRateLimiter>,
        network_gate: Arc<NetworkGate>,
        recv_window_ms: i64,
        request_timeout: Duration,
    ) -> Result<Self, RestError> {
        let url = ParsedHttpUrl::parse(base_url)?;
        Ok(Self {
            url,
            api_key,
            api_secret: api_secret.into_bytes(),
            pool: Mutex::new(Vec::with_capacity(max_pool.max(4))),
            max_pool: max_pool.max(1),
            rate_limiter,
            network_gate,
            recv_window_ms,
            request_timeout,
        })
    }

    pub fn host_port(&self) -> &str {
        &self.url.host_port
    }

    /// Pre-warms `count` persistent keep-alive connections so first hot requests avoid TCP handshake.
    pub async fn warmup(&self, count: usize) -> Result<(), RestError> {
        let n = count.min(self.max_pool);
        let mut created = Vec::with_capacity(n);
        for _ in 0..n {
            created.push(KeepAliveConn::connect(&self.url.host_port).await?);
        }
        let mut pool = self.pool.lock();
        for c in created {
            if pool.len() < self.max_pool {
                pool.push(c);
            }
        }
        Ok(())
    }

    async fn checkout(&self) -> Result<KeepAliveConn, RestError> {
        if let Some(c) = self.pool.lock().pop() {
            return Ok(c);
        }
        KeepAliveConn::connect(&self.url.host_port).await
    }

    fn checkin(&self, conn: KeepAliveConn) {
        let mut pool = self.pool.lock();
        if pool.len() < self.max_pool {
            pool.push(conn);
        }
    }

    #[inline]
    fn check_rate_limit(&self, buckets: u32, weight: i32) -> Result<(), RestError> {
        self.rate_limiter
            .try_reserve(buckets, weight)
            .map_err(|retry_after_ms| RestError::RateLimited { retry_after_ms })
    }

    /// Executes an HTTP/1.1 request and invokes `parse_body(body_bytes)` directly over the
    /// pooled connection's receive buffer with zero intermediate allocations.
    ///
    /// Double-Order Safety (ADR-0009):
    /// If writing to a pooled idle connection fails before sending any bytes, we retry once
    /// with a fresh connection. However, once bytes are written to the wire, ANY connection
    /// drop or timeout returns `RestError::MaybeSent` to strictly prevent duplicate orders.
    async fn execute<R>(
        &self,
        req_bytes: &[u8],
        parse_body: impl FnMut(&[u8]) -> Option<R>,
    ) -> Result<R, RestError> {
        self.execute_with_gate_timeout(req_bytes, self.request_timeout, parse_body)
            .await
    }

    async fn execute_with_gate_timeout<R>(
        &self,
        req_bytes: &[u8],
        gate_timeout: Duration,
        parse_body: impl FnMut(&[u8]) -> Option<R>,
    ) -> Result<R, RestError> {
        // 1. Wait until network gate is open (bounded by gate_timeout, never hangs indefinitely)
        self.network_gate
            .wait_until_open_timeout(gate_timeout)
            .await
            .map_err(|_| RestError::NetworkGateTimeout)?;

        // 2. Enforce request timeout
        match tokio::time::timeout(self.request_timeout, self.execute_inner(req_bytes, parse_body)).await {
            Ok(res) => res,
            Err(_) => Err(RestError::MaybeSent(
                "request timed out while awaiting exchange response".into(),
            )),
        }
    }

    async fn execute_inner<R>(
        &self,
        req_bytes: &[u8],
        mut parse_body: impl FnMut(&[u8]) -> Option<R>,
    ) -> Result<R, RestError> {
        let mut conn = self.checkout().await?;
        if conn.stream.write_all(req_bytes).await.is_err() {
            // Write failed on pooled idle socket before any bytes were received by peer.
            // Safe to retry once on a brand new TCP connection.
            conn = KeepAliveConn::connect(&self.url.host_port).await?;
            if let Err(e2) = conn.stream.write_all(req_bytes).await {
                return Err(RestError::Transport(format!("socket write failed: {e2}")));
            }
        }

        // Bytes have been sent! Any failure past this point cannot be automatically retried
        // because the exchange may have already processed or filled the order (ADR-0009).
        match read_http_response(&mut conn, &self.rate_limiter).await {
            Ok(HttpBodyView {
                status,
                from_chunked,
                start,
                end,
                close_after,
            }) => {
                let body = if from_chunked {
                    &conn.body_buf[..end]
                } else {
                    &conn.read_buf[start..end]
                };

                // Check for ban / rate limit
                if status == 418 || status == 429 {
                    self.network_gate.trip();
                    let preview = String::from_utf8_lossy(&body[..body.len().min(200)]).into_owned();
                    return Err(RestError::IpBanned {
                        status,
                        body_preview: preview,
                    });
                }

                let res = if (200..300).contains(&status) {
                    parse_body(body).ok_or(RestError::Parse("invalid response JSON"))
                } else {
                    let preview = String::from_utf8_lossy(&body[..body.len().min(200)]).into_owned();
                    Err(RestError::HttpStatus {
                        status,
                        body_preview: preview,
                    })
                };

                if !close_after {
                    self.checkin(conn);
                }
                res
            }
            Err(ReadErr::PeerClosedEarly) => {
                Err(RestError::MaybeSent(
                    "peer closed connection before sending response (outcome unknown)".into(),
                ))
            }
            Err(ReadErr::Fatal(msg)) => {
                Err(RestError::MaybeSent(format!(
                    "fatal transport error while awaiting response: {msg}"
                )))
            }
        }
    }

    /// `GET /fapi/v1/klines` directly into caller-provided `dst` slice (0 heap allocs).
    pub async fn klines_into(
        &self,
        symbol: Symbol,
        interval: BinanceInterval,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<i32>,
        dst: &mut [Kline],
    ) -> Result<usize, RestError> {
        let weight = match limit.unwrap_or(500) {
            1..=99 => 1,
            100..=499 => 2,
            500..=1000 => 5,
            _ => 10,
        };
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, weight)?;

        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);
        let mut req = StackHttpRequest::new(b"GET", self.url.base_path.as_bytes(), b"/fapi/v1/klines");
        req.add_str(b"symbol", &sym_buf[..sym_len]);
        req.add_str(b"interval", interval.as_bytes());
        if let Some(st) = start_time {
            req.add_i64(b"startTime", st);
        }
        if let Some(et) = end_time {
            req.add_i64(b"endTime", et);
        }
        if let Some(l) = limit {
            req.add_i64(b"limit", l as i64);
        }
        let wire = req.finish(self.url.host_header.as_bytes(), None, None, false);

        self.execute(wire, |body| parse_klines_into(body, dst)).await
    }

    /// `GET /fapi/v1/depth` directly into caller-provided `bids`/`asks` slices (0 heap allocs).
    pub async fn order_book_into(
        &self,
        symbol: Symbol,
        limit: i32,
        bids: &mut [DepthLevel],
        asks: &mut [DepthLevel],
    ) -> Result<OrderBookSnapshotModel, RestError> {
        let weight = match limit {
            5 | 10 | 20 | 50 => 2,
            100 => 5,
            500 => 10,
            _ => 20,
        };
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_DEPTH_SNAPSHOT_WEIGHT,
            weight,
        )?;

        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);
        let mut req = StackHttpRequest::new(b"GET", self.url.base_path.as_bytes(), b"/fapi/v1/depth");
        req.add_str(b"symbol", &sym_buf[..sym_len]);
        req.add_i64(b"limit", limit as i64);
        let wire = req.finish(self.url.host_header.as_bytes(), None, None, false);

        self.execute(wire, |body| read_order_book_into(body, true, bids, asks))
            .await
    }

    /// `GET /fapi/v1/depth` returning owned `OrderBookModel`.
    pub async fn order_book(&self, symbol: Symbol, limit: i32) -> Result<OrderBookModel, RestError> {
        let weight = match limit {
            5 | 10 | 20 | 50 => 2,
            100 => 5,
            500 => 10,
            _ => 20,
        };
        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_DEPTH_SNAPSHOT_WEIGHT,
            weight,
        )?;

        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);
        let mut req = StackHttpRequest::new(b"GET", self.url.base_path.as_bytes(), b"/fapi/v1/depth");
        req.add_str(b"symbol", &sym_buf[..sym_len]);
        req.add_i64(b"limit", limit as i64);
        let wire = req.finish(self.url.host_header.as_bytes(), None, None, false);

        self.execute(wire, |body| read_order_book(body, true)).await
    }

    /// `GET /fapi/v1/exchangeInfo`
    pub async fn exchange_info(&self) -> Result<ExchangeInfoModel, RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 1)?;
        let mut req = StackHttpRequest::new(
            b"GET",
            self.url.base_path.as_bytes(),
            b"/fapi/v1/exchangeInfo",
        );
        let wire = req.finish(self.url.host_header.as_bytes(), None, None, false);
        self.execute(wire, parse_exchange_info).await
    }

    /// `GET /fapi/v2/account` (signed)
    pub async fn account_info_v2(&self) -> Result<AccountInfoV2Model, RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 5)?;
        let ts = BinanceRateLimiter::utc_millis();
        let mut req = StackHttpRequest::new(b"GET", self.url.base_path.as_bytes(), b"/fapi/v2/account");
        if self.recv_window_ms > 0 {
            req.add_i64(b"recvWindow", self.recv_window_ms);
        }
        req.add_i64(b"timestamp", ts);
        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            Some(&self.api_secret),
            false,
        );
        self.execute(wire, parse_account_info_v2).await
    }

    /// `GET /fapi/v1/leverageBracket` (signed)
    pub async fn leverage_bracket(
        &self,
        symbol: Option<Symbol>,
    ) -> Result<LeverageBracketModel, RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 1)?;
        let ts = BinanceRateLimiter::utc_millis();
        let mut req = StackHttpRequest::new(
            b"GET",
            self.url.base_path.as_bytes(),
            b"/fapi/v1/leverageBracket",
        );
        let mut sym_buf = [0u8; 16];
        if let Some(sym) = symbol {
            if !sym.is_empty() {
                let n = sym.write_to(&mut sym_buf);
                req.add_str(b"symbol", &sym_buf[..n]);
            }
        }
        if self.recv_window_ms > 0 {
            req.add_i64(b"recvWindow", self.recv_window_ms);
        }
        req.add_i64(b"timestamp", ts);
        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            Some(&self.api_secret),
            false,
        );
        self.execute(wire, parse_leverage_bracket).await
    }

    /// `POST /fapi/v1/order` (signed, zero heap allocs on hot path)
    pub async fn new_order(&self, order: &mut NewOrderRequest) -> Result<OrderModel, RestError> {
        let violation = preflight_order(order);
        if violation != OrderRuleViolation::None {
            return Err(RestError::RuleViolation(violation));
        }

        self.check_rate_limit(
            bucket::FUTURES_REQUEST_WEIGHT | bucket::FUTURES_ORDER_MINUTE | bucket::FUTURES_ORDER_10S,
            1,
        )?;

        let ts = BinanceRateLimiter::utc_millis();
        let mut sym_buf = [0u8; 16];
        let sym_len = order.symbol.write_to(&mut sym_buf);
        let mut req = StackHttpRequest::new(b"POST", self.url.base_path.as_bytes(), b"/fapi/v1/order");
        req.add_str(b"symbol", &sym_buf[..sym_len]);
        if order.side != OrderSide::Unknown {
            req.add_str(b"side", order.side.as_bytes());
        }
        if order.position_side != PositionSide::Unknown {
            req.add_str(b"positionSide", order.position_side.as_bytes());
        }
        if order.order_type != OrderType::Unknown {
            req.add_str(b"type", order.order_type.as_bytes());
        }
        if order.time_in_force != TimeInForce::Unknown {
            req.add_str(b"timeInForce", order.time_in_force.as_bytes());
        }
        if order.quantity > 0.0 {
            req.add_f64(b"quantity", order.quantity);
        }
        if order.price > 0.0 {
            req.add_f64(b"price", order.price);
        }
        if !order.client_order_id.is_empty() {
            req.add_str(b"newClientOrderId", order.client_order_id.as_bytes());
        }
        if order.is_reduce_only {
            req.add_str(b"reduceOnly", b"true");
        }
        if !matches!(order.price_match, PriceMatch::Unknown | PriceMatch::None) {
            req.add_str(b"priceMatch", order.price_match.as_bytes());
        }
        if !matches!(
            order.self_trade_prevention_mode,
            SelfTradePreventionMode::Unknown | SelfTradePreventionMode::None
        ) {
            req.add_str(
                b"selfTradePreventionMode",
                order.self_trade_prevention_mode.as_bytes(),
            );
        }
        if order.response_type != OrderResponseType::Ack {
            req.add_str(b"newOrderRespType", order.response_type.as_bytes());
        }
        if order.good_till_date > 0 {
            req.add_i64(b"goodTillDate", order.good_till_date);
        }
        if self.recv_window_ms > 0 {
            req.add_i64(b"recvWindow", self.recv_window_ms);
        }
        req.add_i64(b"timestamp", ts);

        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            Some(&self.api_secret),
            true,
        );
        self.execute(wire, parse_order).await
    }

    /// `DELETE /fapi/v1/order` (signed, zero heap allocs on hot path)
    pub async fn cancel_order(
        &self,
        symbol: Symbol,
        order_id: Option<i64>,
        orig_client_order_id: Option<&ClientOrderId>,
    ) -> Result<OrderModel, RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 1)?;
        let ts = BinanceRateLimiter::utc_millis();
        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);
        let mut req = StackHttpRequest::new(b"DELETE", self.url.base_path.as_bytes(), b"/fapi/v1/order");
        req.add_str(b"symbol", &sym_buf[..sym_len]);
        if let Some(oid) = order_id {
            req.add_i64(b"orderId", oid);
        }
        if let Some(cid) = orig_client_order_id {
            if !cid.is_empty() {
                req.add_str(b"origClientOrderId", cid.as_bytes());
            }
        }
        if self.recv_window_ms > 0 {
            req.add_i64(b"recvWindow", self.recv_window_ms);
        }
        req.add_i64(b"timestamp", ts);

        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            Some(&self.api_secret),
            true,
        );
        // Emergency cancel: bound gate wait to max 500ms so cancels are never stuck behind dead streams
        let gate_timeout = self.request_timeout.min(Duration::from_millis(500));
        self.execute_with_gate_timeout(wire, gate_timeout, parse_order).await
    }

    /// `PUT /fapi/v1/order` (modify order price/quantity)
    pub async fn modify_order(&self, req_mod: &ModifyOrderRequest) -> Result<OrderModel, RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 1)?;
        let ts = BinanceRateLimiter::utc_millis();
        let mut sym_buf = [0u8; 16];
        let sym_len = req_mod.symbol.write_to(&mut sym_buf);

        let mut req = StackHttpRequest::new(b"PUT", self.url.base_path.as_bytes(), b"/fapi/v1/order");
        req.add_str(b"symbol", &sym_buf[..sym_len]);
        if let Some(oid) = req_mod.order_id {
            req.add_i64(b"orderId", oid);
        }
        if let Some(cid) = req_mod.orig_client_order_id {
            if !cid.is_empty() {
                req.add_str(b"origClientOrderId", cid.as_bytes());
            }
        }
        if req_mod.side != OrderSide::Unknown {
            req.add_str(b"side", req_mod.side.as_bytes());
        }
        if req_mod.quantity > 0.0 {
            req.add_f64(b"quantity", req_mod.quantity);
        }
        if req_mod.price > 0.0 {
            req.add_f64(b"price", req_mod.price);
        }
        if self.recv_window_ms > 0 {
            req.add_i64(b"recvWindow", self.recv_window_ms);
        }
        req.add_i64(b"timestamp", ts);

        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            Some(&self.api_secret),
            true,
        );
        // Emergency cancel: bound gate wait to max 500ms so cancels are never stuck behind dead streams
        let gate_timeout = self.request_timeout.min(Duration::from_millis(500));
        self.execute_with_gate_timeout(wire, gate_timeout, parse_order).await
    }

    /// `DELETE /fapi/v1/allOpenOrders`
    pub async fn cancel_all_open_orders(&self, symbol: Symbol) -> Result<bool, RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 1)?;
        let ts = BinanceRateLimiter::utc_millis();
        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);

        let mut req = StackHttpRequest::new(b"DELETE", self.url.base_path.as_bytes(), b"/fapi/v1/allOpenOrders");
        req.add_str(b"symbol", &sym_buf[..sym_len]);
        if self.recv_window_ms > 0 {
            req.add_i64(b"recvWindow", self.recv_window_ms);
        }
        req.add_i64(b"timestamp", ts);

        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            Some(&self.api_secret),
            true,
        );
        self.execute(wire, |body| {
            // Returns {"code":200,"msg":"The operation of cancel all open order is done."}
            Some(memmem::find(body, br#""code":200"#).is_some())
        }).await
    }

    /// `POST /fapi/v1/leverage`
    pub async fn change_leverage(&self, symbol: Symbol, leverage: i32) -> Result<ChangeLeverageResponse, RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 1)?;
        let ts = BinanceRateLimiter::utc_millis();
        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);

        let mut req = StackHttpRequest::new(b"POST", self.url.base_path.as_bytes(), b"/fapi/v1/leverage");
        req.add_str(b"symbol", &sym_buf[..sym_len]);
        req.add_i64(b"leverage", leverage as i64);
        if self.recv_window_ms > 0 {
            req.add_i64(b"recvWindow", self.recv_window_ms);
        }
        req.add_i64(b"timestamp", ts);

        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            Some(&self.api_secret),
            true,
        );
        self.execute(wire, |body| {
            // {"leverage":20,"maxNotionalValue":"1000000","symbol":"BTCUSDT"}
            let mut cursor = 0;
            let lev = crate::parsing::utf8_reader::read_i64(body, b"\"leverage\":", &mut cursor)? as i32;
            let max_n = crate::parsing::utf8_reader::read_quoted_f64(body, b"\"maxNotionalValue\":", &mut cursor).unwrap_or(0.0);
            let s = crate::parsing::utf8_reader::read_symbol(body, b"\"symbol\":", &mut cursor).unwrap_or_default();
            Some(ChangeLeverageResponse {
                leverage: lev,
                max_notional_value: max_n,
                symbol: s,
            })
        }).await
    }

    /// `POST /fapi/v1/marginType`
    pub async fn change_margin_type(&self, symbol: Symbol, margin_type: MarginType) -> Result<ChangeMarginTypeResponse, RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 1)?;
        let ts = BinanceRateLimiter::utc_millis();
        let mut sym_buf = [0u8; 16];
        let sym_len = symbol.write_to(&mut sym_buf);

        let mut req = StackHttpRequest::new(b"POST", self.url.base_path.as_bytes(), b"/fapi/v1/marginType");
        req.add_str(b"symbol", &sym_buf[..sym_len]);
        req.add_str(b"marginType", margin_type.as_bytes());
        if self.recv_window_ms > 0 {
            req.add_i64(b"recvWindow", self.recv_window_ms);
        }
        req.add_i64(b"timestamp", ts);

        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            Some(&self.api_secret),
            true,
        );
        self.execute(wire, |body| {
            // {"code":200,"msg":"success"}
            let mut cursor = 0;
            let code = crate::parsing::utf8_reader::read_i64(body, b"\"code\":", &mut cursor).unwrap_or(200) as i32;
            let msg = crate::parsing::utf8_reader::read_symbol(body, b"\"msg\":", &mut cursor).unwrap_or_default();
            Some(ChangeMarginTypeResponse { code, msg })
        }).await
    }

    /// `POST /fapi/v1/listenKey` — creates UserData stream listenKey.
    pub async fn create_listen_key(&self) -> Result<String, RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 1)?;
        let mut req = StackHttpRequest::new(b"POST", self.url.base_path.as_bytes(), b"/fapi/v1/listenKey");
        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            None,
            true,
        );
        self.execute(wire, |body| {
            // {"listenKey":"..."}
            let key = br#""listenKey":""#;
            let idx = memmem::find(body, key)? + key.len();
            let end = memchr::memchr(b'"', &body[idx..])? + idx;
            std::str::from_utf8(&body[idx..end]).ok().map(str::to_owned)
        }).await
    }

    /// `PUT /fapi/v1/listenKey` — extends listenKey validity for another 60 minutes.
    pub async fn keepalive_listen_key(&self) -> Result<(), RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 1)?;
        let mut req = StackHttpRequest::new(b"PUT", self.url.base_path.as_bytes(), b"/fapi/v1/listenKey");
        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            None,
            true,
        );
        self.execute(wire, |_| Some(())).await
    }

    /// `DELETE /fapi/v1/listenKey` — closes UserData stream listenKey.
    pub async fn close_listen_key(&self) -> Result<(), RestError> {
        self.check_rate_limit(bucket::FUTURES_REQUEST_WEIGHT, 1)?;
        let mut req = StackHttpRequest::new(b"DELETE", self.url.base_path.as_bytes(), b"/fapi/v1/listenKey");
        let wire = req.finish(
            self.url.host_header.as_bytes(),
            Some(self.api_key.as_bytes()),
            None,
            true,
        );
        self.execute(wire, |_| Some(())).await
    }
}

struct HttpBodyView {
    status: u16,
    from_chunked: bool,
    start: usize,
    end: usize,
    close_after: bool,
}

enum ReadErr {
    PeerClosedEarly,
    Fatal(String),
}

async fn read_http_response(
    conn: &mut KeepAliveConn,
    rate_limiter: &BinanceRateLimiter,
) -> Result<HttpBodyView, ReadErr> {
    let mut total = 0usize;
    let header_end = loop {
        if let Some(idx) = memmem::find(&conn.read_buf[..total], b"\r\n\r\n") {
            break idx + 4;
        }
        if total == conn.read_buf.len() {
            if conn.read_buf.len() >= 1024 * 1024 {
                return Err(ReadErr::Fatal("HTTP response header exceeded 1MB".into()));
            }
            conn.read_buf.resize(conn.read_buf.len() * 2, 0);
        }
        let n = conn
            .stream
            .read(&mut conn.read_buf[total..])
            .await
            .map_err(|e| ReadErr::Fatal(format!("HTTP read error: {e}")))?;
        if n == 0 {
            if total == 0 {
                return Err(ReadErr::PeerClosedEarly);
            }
            return Err(ReadErr::Fatal("EOF inside HTTP headers".into()));
        }
        total += n;
    };

    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut resp = httparse::Response::new(&mut headers);
    let status = match resp.parse(&conn.read_buf[..header_end]) {
        Ok(httparse::Status::Complete(_)) => resp.code.unwrap_or(0),
        Ok(httparse::Status::Partial) => {
            return Err(ReadErr::Fatal("incomplete HTTP header block".into()));
        }
        Err(e) => return Err(ReadErr::Fatal(format!("httparse error: {e}"))),
    };

    let mut content_length: Option<usize> = None;
    let mut is_chunked = false;
    let mut close_after = false;

    for h in resp.headers.iter() {
        if h.name.eq_ignore_ascii_case("content-length") {
            if let Some((v, _)) = parse_i64_fast(h.value) {
                content_length = Some(v.max(0) as usize);
            }
        } else if h.name.eq_ignore_ascii_case("transfer-encoding") {
            if memmem::find(h.value, b"chunked").is_some() {
                is_chunked = true;
            }
        } else if h.name.eq_ignore_ascii_case("connection") {
            if h.value.eq_ignore_ascii_case(b"close") {
                close_after = true;
            }
        } else if h.name.eq_ignore_ascii_case("x-mbx-used-weight-1m") {
            if let Some((w, _)) = parse_i64_fast(h.value) {
                rate_limiter.update_weight(bucket::FUTURES_REQUEST_WEIGHT, w as i32);
            }
        } else if h.name.eq_ignore_ascii_case("x-mbx-order-count-1m") {
            if let Some((w, _)) = parse_i64_fast(h.value) {
                rate_limiter.update_weight(bucket::FUTURES_ORDER_MINUTE, w as i32);
            }
        } else if h.name.eq_ignore_ascii_case("x-mbx-order-count-10s") {
            if let Some((w, _)) = parse_i64_fast(h.value) {
                rate_limiter.update_weight(bucket::FUTURES_ORDER_10S, w as i32);
            }
        }
    }

    if let Some(clen) = content_length {
        let need = header_end + clen;
        if conn.read_buf.len() < need {
            conn.read_buf.resize(need.next_power_of_two(), 0);
        }
        while total < need {
            let n = conn
                .stream
                .read(&mut conn.read_buf[total..])
                .await
                .map_err(|e| ReadErr::Fatal(format!("HTTP body read: {e}")))?;
            if n == 0 {
                return Err(ReadErr::Fatal("unexpected EOF reading Content-Length body".into()));
            }
            total += n;
        }
        return Ok(HttpBodyView {
            status,
            from_chunked: false,
            start: header_end,
            end: need,
            close_after,
        });
    }

    if is_chunked {
        conn.body_buf.clear();
        let mut cursor = header_end;
        loop {
            let line_end = loop {
                if let Some(rel) = memmem::find(&conn.read_buf[cursor..total], b"\r\n") {
                    break cursor + rel;
                }
                if total == conn.read_buf.len() {
                    conn.read_buf.resize(conn.read_buf.len() * 2, 0);
                }
                let n = conn
                    .stream
                    .read(&mut conn.read_buf[total..])
                    .await
                    .map_err(|e| ReadErr::Fatal(format!("HTTP chunk size read: {e}")))?;
                if n == 0 {
                    return Err(ReadErr::Fatal("unexpected EOF in chunk header".into()));
                }
                total += n;
            };

            let hex_slice = &conn.read_buf[cursor..line_end];
            let chunk_len = parse_hex_len(hex_slice)
                .ok_or_else(|| ReadErr::Fatal("invalid hex chunk size".into()))?;
            cursor = line_end + 2;

            if chunk_len == 0 {
                // Consume terminating \r\n (minimal trailers handled)
                while total < cursor + 2 {
                    let n = conn
                        .stream
                        .read(&mut conn.read_buf[total..])
                        .await
                        .map_err(|e| ReadErr::Fatal(format!("HTTP final CRLF read: {e}")))?;
                    if n == 0 {
                        break;
                    }
                    total += n;
                }
                return Ok(HttpBodyView {
                    status,
                    from_chunked: true,
                    start: 0,
                    end: conn.body_buf.len(),
                    close_after,
                });
            }

            let chunk_end = cursor + chunk_len + 2;
            if conn.read_buf.len() < chunk_end {
                conn.read_buf.resize(chunk_end.next_power_of_two(), 0);
            }
            while total < chunk_end {
                let n = conn
                    .stream
                    .read(&mut conn.read_buf[total..])
                    .await
                    .map_err(|e| ReadErr::Fatal(format!("HTTP chunk payload read: {e}")))?;
                if n == 0 {
                    return Err(ReadErr::Fatal("unexpected EOF inside chunk payload".into()));
                }
                total += n;
            }
            conn.body_buf
                .extend_from_slice(&conn.read_buf[cursor..cursor + chunk_len]);
            cursor = chunk_end;
        }
    }

    // Fallback: connection-close body
    loop {
        if total == conn.read_buf.len() {
            conn.read_buf.resize(conn.read_buf.len() * 2, 0);
        }
        let n = conn
            .stream
            .read(&mut conn.read_buf[total..])
            .await
            .map_err(|e| ReadErr::Fatal(format!("HTTP close-delimited read: {e}")))?;
        if n == 0 {
            break;
        }
        total += n;
    }

    Ok(HttpBodyView {
        status,
        from_chunked: false,
        start: header_end,
        end: total,
        close_after: true,
    })
}

#[inline]
fn parse_hex_len(slice: &[u8]) -> Option<usize> {
    let trimmed = match slice.iter().position(|&b| b == b';') {
        Some(idx) => &slice[..idx],
        None => slice,
    };
    let mut val = 0usize;
    let mut digits = 0usize;
    for &b in trimmed {
        let d = match b {
            b'0'..=b'9' => (b - b'0') as usize,
            b'a'..=b'f' => (b - b'a' + 10) as usize,
            b'A'..=b'F' => (b - b'A' + 10) as usize,
            b' ' | b'\t' => continue,
            _ => return None,
        };
        val = val.checked_mul(16)?.checked_add(d)?;
        digits += 1;
    }
    if digits == 0 {
        None
    } else {
        Some(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stack_http_request_formats_cleanly() {
        let mut req = StackHttpRequest::new(b"POST", b"", b"/fapi/v1/order");
        req.add_str(b"symbol", b"BTCUSDT");
        req.add_str(b"side", b"BUY");
        req.add_i64(b"timestamp", 1_700_000_000_000);
        let wire = req.finish(b"127.0.0.1:8080", Some(b"k123"), Some(b"s123"), true);
        let text = std::str::from_utf8(wire).unwrap();
        assert!(text.starts_with("POST /fapi/v1/order?symbol=BTCUSDT&side=BUY&timestamp=1700000000000&signature="));
        assert!(text.contains("Host: 127.0.0.1:8080\r\n"));
        assert!(text.contains("X-MBX-APIKEY: k123\r\n"));
        assert!(text.contains("Content-Length: 0\r\n\r\n"));
    }
}

use crate::concurrency::seqlock::LatestCache;
use crate::order_book::OrderBookSet;
use crate::parsing::streams::{
    parse_agg_trade, parse_book_ticker, parse_depth_update, parse_kline_update, parse_partial_depth,
};
use crate::primitives::{
    AggTradeModel, BookTickerModel, DepthLevel, DepthUpdateHeader, KlineUpdateModel,
    PartialDepthModel,
};
use crate::transport::ws_frame::{
    connect_and_handshake, write_masked_frame, DecodedFrame, FastMaskRng, ParsedWsUrl,
    WsOpcode, WsReadBuffer,
};
use arc_swap::ArcSwap;
use memchr::{memchr, memmem};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

pub const MAX_STREAM_NAME_LEN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamKind {
    BookTicker = 1,
    AggTrade = 2,
    DepthDiff = 3,
    PartialDepth = 4,
    Kline = 5,
    Unknown = 255,
}

impl StreamKind {
    #[inline]
    pub fn classify(stream: &[u8]) -> Self {
        if let Some(at) = memchr(b'@', stream) {
            let suffix = &stream[at + 1..];
            if suffix == b"bookTicker" {
                return Self::BookTicker;
            }
            if suffix == b"aggTrade" {
                return Self::AggTrade;
            }
            if suffix.starts_with(b"depth5")
                || suffix.starts_with(b"depth10")
                || suffix.starts_with(b"depth20")
            {
                return Self::PartialDepth;
            }
            if suffix.starts_with(b"depth") {
                return Self::DepthDiff;
            }
            if suffix.starts_with(b"kline") {
                return Self::Kline;
            }
        }
        Self::Unknown
    }
}

#[inline(always)]
pub fn fnv1a_lower_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        let lb = if b.is_ascii_uppercase() { b + 32 } else { b };
        h ^= lb as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct RouteSlot {
    pub hash: u64,
    pub sub_id: u32,
    pub kind: StreamKind,
    pub key_len: u8,
    pub occupied: bool,
    pub _pad: u8,
    pub key: [u8; MAX_STREAM_NAME_LEN],
}

impl Default for RouteSlot {
    fn default() -> Self {
        Self {
            hash: 0,
            sub_id: 0,
            kind: StreamKind::Unknown,
            key_len: 0,
            occupied: false,
            _pad: 0,
            key: [0u8; MAX_STREAM_NAME_LEN],
        }
    }
}

pub struct RouteSnapshot {
    slots: Vec<RouteSlot>,
    mask: usize,
    active_count: usize,
}

impl RouteSnapshot {
    fn build(entries: &[(Vec<u8>, u32, StreamKind)]) -> Self {
        let min_cap = (entries.len() * 2 + 8).next_power_of_two();
        let mut slots = vec![RouteSlot::default(); min_cap];
        let mask = min_cap - 1;

        for (key, sub_id, kind) in entries {
            if key.len() > MAX_STREAM_NAME_LEN {
                continue;
            }
            let hash = fnv1a_lower_64(key);
            let mut idx = (hash as usize) & mask;
            loop {
                if !slots[idx].occupied {
                    slots[idx].hash = hash;
                    slots[idx].sub_id = *sub_id;
                    slots[idx].kind = *kind;
                    slots[idx].key_len = key.len() as u8;
                    slots[idx].occupied = true;
                    for (i, &b) in key.iter().enumerate() {
                        slots[idx].key[i] = b.to_ascii_lowercase();
                    }
                    break;
                }
                idx = (idx + 1) & mask;
            }
        }

        Self {
            slots,
            mask,
            active_count: entries.len(),
        }
    }

    #[inline(always)]
    pub fn lookup(&self, stream: &[u8]) -> Option<(u32, StreamKind)> {
        if stream.is_empty() || stream.len() > MAX_STREAM_NAME_LEN || self.slots.is_empty() {
            return None;
        }
        let hash = fnv1a_lower_64(stream);
        let mut idx = (hash as usize) & self.mask;
        let len = stream.len() as u8;

        loop {
            let slot = unsafe { self.slots.get_unchecked(idx) };
            if !slot.occupied {
                return None;
            }
            if slot.hash == hash
                && slot.key_len == len
                && ascii_eq_ignore_case(&slot.key[..stream.len()], stream)
            {
                return Some((slot.sub_id, slot.kind));
            }
            idx = (idx + 1) & self.mask;
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.active_count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.active_count == 0
    }
}

#[inline(always)]
fn ascii_eq_ignore_case(lower_stored: &[u8], probe: &[u8]) -> bool {
    // Fast exact match first (stream names from Binance are already lowercase!)
    if lower_stored == probe {
        return true;
    }
    lower_stored.eq_ignore_ascii_case(probe)
}

/// Lock-free open-addressing route registry using copy-on-write snapshot swaps (`ArcSwap`).
/// Reads on the WebSocket receive thread perform zero locks and zero heap allocations.
pub struct WsStreamRegistry {
    snapshot: ArcSwap<RouteSnapshot>,
    write_lock: Mutex<RegistryInner>,
}

struct RegistryInner {
    by_name: HashMap<Vec<u8>, (u32, StreamKind)>,
    free_ids: Vec<u32>,
    next_id: u32,
}

impl WsStreamRegistry {
    pub fn new() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(RouteSnapshot::build(&[])),
            write_lock: Mutex::new(RegistryInner {
                by_name: HashMap::new(),
                free_ids: Vec::new(),
                next_id: 0,
            }),
        }
    }

    pub fn register(&self, stream_name: &str, kind: StreamKind) -> Result<u32, &'static str> {
        let bytes = stream_name.as_bytes();
        if bytes.is_empty() || bytes.len() > MAX_STREAM_NAME_LEN {
            return Err("invalid stream name length");
        }
        let lower: Vec<u8> = bytes.iter().map(|b| b.to_ascii_lowercase()).collect();

        let mut inner = self.write_lock.lock();
        if let Some(&(sub_id, _)) = inner.by_name.get(&lower) {
            return Ok(sub_id);
        }

        let sub_id = if let Some(reused) = inner.free_ids.pop() {
            reused
        } else {
            let id = inner.next_id;
            inner.next_id += 1;
            id
        };

        let resolved_kind = if kind == StreamKind::Unknown {
            StreamKind::classify(&lower)
        } else {
            kind
        };

        inner.by_name.insert(lower, (sub_id, resolved_kind));
        self.publish_locked(&inner);
        Ok(sub_id)
    }

    pub fn unregister(&self, stream_name: &str) -> Option<(u32, StreamKind)> {
        let lower: Vec<u8> = stream_name
            .as_bytes()
            .iter()
            .map(|b| b.to_ascii_lowercase())
            .collect();
        let mut inner = self.write_lock.lock();
        let removed = inner.by_name.remove(&lower)?;
        inner.free_ids.push(removed.0);
        self.publish_locked(&inner);
        Some(removed)
    }

    fn publish_locked(&self, inner: &RegistryInner) {
        let entries: Vec<(Vec<u8>, u32, StreamKind)> = inner
            .by_name
            .iter()
            .map(|(k, &(id, kind))| (k.clone(), id, kind))
            .collect();
        self.snapshot.store(Arc::new(RouteSnapshot::build(&entries)));
    }

    #[inline(always)]
    pub fn snapshot(&self) -> arc_swap::Guard<Arc<RouteSnapshot>> {
        self.snapshot.load()
    }

    pub fn active_stream_names(&self) -> Vec<String> {
        let inner = self.write_lock.lock();
        inner
            .by_name
            .keys()
            .filter_map(|k| std::str::from_utf8(k).ok().map(str::to_owned))
            .collect()
    }
}

impl Default for WsStreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Zero-copy combined-stream envelope parser (`{"stream":"...","data":{...}}`).
#[inline]
pub fn parse_stream_envelope(msg: &[u8]) -> Option<(&[u8], &[u8])> {
    // Fast path for exact Axitor / Binance wire layout: {"stream":"...","data":...}
    if msg.starts_with(br#"{"stream":""#) {
        let rest = &msg[11..];
        let q = memchr(b'"', rest)?;
        let stream = &rest[..q];
        let after = &rest[q + 1..];
        if after.starts_with(br#","data":"#) {
            let mut data = &after[8..];
            if data.ends_with(b"}") {
                data = &data[..data.len() - 1];
            }
            return Some((stream, data));
        }
    }

    // Fallback scan with memmem
    let stream_key = memmem::find(msg, br#""stream""#)?;
    let after_key = skip_ws_colon(&msg[stream_key + 8..])?;
    if *after_key.first()? != b'"' {
        return None;
    }
    let s_inner = &after_key[1..];
    let s_end = memchr(b'"', s_inner)?;
    let stream = &s_inner[..s_end];

    let data_key = memmem::find(msg, br#""data""#)?;
    let mut data = skip_ws_colon(&msg[data_key + 6..])?;
    if data.ends_with(b"}") {
        data = &data[..data.len() - 1];
    }
    Some((stream, data))
}

#[inline]
fn skip_ws_colon(mut s: &[u8]) -> Option<&[u8]> {
    while let Some(&b) = s.first() {
        if b == b' ' || b == b'\t' || b == b'\r' || b == b'\n' {
            s = &s[1..];
        } else {
            break;
        }
    }
    if *s.first()? != b':' {
        return None;
    }
    s = &s[1..];
    while let Some(&b) = s.first() {
        if b == b' ' || b == b'\t' || b == b'\r' || b == b'\n' {
            s = &s[1..];
        } else {
            break;
        }
    }
    Some(s)
}

/// Checks if a frame is a SUBSCRIBE/UNSUBSCRIBE JSON-RPC control response (`{"result":...,"id":N}`)
/// and returns its `id`.
#[inline]
pub fn parse_control_ack_id(msg: &[u8]) -> Option<i64> {
    if msg.len() < 8 {
        return None;
    }
    let id_pos = memmem::find(msg, br#""id""#)?;
    let val = skip_ws_colon(&msg[id_pos + 4..])?;
    crate::parsing::fast_decimal::parse_i64_fast(val).map(|(id, _)| id)
}

/// Monomorphized stream event sink invoked synchronously on the WebSocket receive thread.
pub trait StreamHandler: Send + 'static {
    fn on_book_ticker(&mut self, sub_id: u32, model: &BookTickerModel, event_time: i64);
    fn on_agg_trade(&mut self, sub_id: u32, model: &AggTradeModel, event_time: i64);
    fn on_partial_depth(&mut self, sub_id: u32, model: &PartialDepthModel, event_time: i64);
    fn on_kline(&mut self, sub_id: u32, model: &KlineUpdateModel, event_time: i64);
    fn on_depth_update(
        &mut self,
        sub_id: u32,
        header: &DepthUpdateHeader,
        bids: &[DepthLevel],
        asks: &[DepthLevel],
    );
    fn on_disconnected(&mut self, _sub_id: u32, _kind: StreamKind) {}
}

/// Default lock-free state store handler backed by `LatestCache<T>` + `OrderBookSet`.
pub struct MarketStateHandler {
    pub book_tickers: Arc<LatestCache<BookTickerModel>>,
    pub agg_trades: Arc<LatestCache<AggTradeModel>>,
    pub partial_depths: Arc<LatestCache<PartialDepthModel>>,
    pub klines: Arc<LatestCache<KlineUpdateModel>>,
    pub order_books: Arc<OrderBookSet>,
}

impl StreamHandler for MarketStateHandler {
    #[inline(always)]
    fn on_book_ticker(&mut self, sub_id: u32, model: &BookTickerModel, _event_time: i64) {
        self.book_tickers.update(sub_id as usize, *model);
    }

    #[inline(always)]
    fn on_agg_trade(&mut self, sub_id: u32, model: &AggTradeModel, _event_time: i64) {
        self.agg_trades.update(sub_id as usize, *model);
    }

    #[inline(always)]
    fn on_partial_depth(&mut self, sub_id: u32, model: &PartialDepthModel, _event_time: i64) {
        self.partial_depths.update(sub_id as usize, *model);
    }

    #[inline(always)]
    fn on_kline(&mut self, sub_id: u32, model: &KlineUpdateModel, _event_time: i64) {
        self.klines.update(sub_id as usize, *model);
    }

    #[inline(always)]
    fn on_depth_update(
        &mut self,
        sub_id: u32,
        header: &DepthUpdateHeader,
        bids: &[DepthLevel],
        asks: &[DepthLevel],
    ) {
        self.order_books
            .on_depth_update(sub_id as usize, header, bids, asks);
    }

    fn on_disconnected(&mut self, sub_id: u32, kind: StreamKind) {
        match kind {
            StreamKind::BookTicker => self.book_tickers.disconnect(sub_id as usize),
            StreamKind::AggTrade => self.agg_trades.disconnect(sub_id as usize),
            StreamKind::PartialDepth => self.partial_depths.disconnect(sub_id as usize),
            StreamKind::Kline => self.klines.disconnect(sub_id as usize),
            StreamKind::DepthDiff => self.order_books.on_disconnected(sub_id as usize),
            StreamKind::Unknown => {}
        }
    }
}

/// Processes a single combined-stream frame payload (`{"stream":"...","data":...}`) directly
/// with zero heap allocations on known stream types.
#[inline(always)]
pub fn dispatch_stream_frame<H: StreamHandler>(
    payload: &[u8],
    routes: &RouteSnapshot,
    handler: &mut H,
    bids_scratch: &mut Vec<DepthLevel>,
    asks_scratch: &mut Vec<DepthLevel>,
) -> bool {
    let Some((stream, data)) = parse_stream_envelope(payload) else {
        return false;
    };
    let Some((sub_id, kind)) = routes.lookup(stream) else {
        return false;
    };

    match kind {
        StreamKind::BookTicker => {
            if let Some((model, ts)) = parse_book_ticker(data) {
                handler.on_book_ticker(sub_id, &model, ts);
                return true;
            }
        }
        StreamKind::AggTrade => {
            if let Some((model, ts)) = parse_agg_trade(data) {
                handler.on_agg_trade(sub_id, &model, ts);
                return true;
            }
        }
        StreamKind::DepthDiff => {
            if let Some(header) = parse_depth_update(data, bids_scratch, asks_scratch) {
                handler.on_depth_update(sub_id, &header, bids_scratch, asks_scratch);
                return true;
            }
        }
        StreamKind::PartialDepth => {
            if let Some((model, ts)) = parse_partial_depth(data) {
                handler.on_partial_depth(sub_id, &model, ts);
                return true;
            }
        }
        StreamKind::Kline => {
            if let Some((model, ts)) = parse_kline_update(data) {
                handler.on_kline(sub_id, &model, ts);
                return true;
            }
        }
        StreamKind::Unknown => {}
    }
    false
}

enum ShardCommand {
    Subscribe {
        streams: Vec<String>,
        ack: oneshot::Sender<Result<(), String>>,
    },
    Unsubscribe {
        streams: Vec<String>,
        ack: oneshot::Sender<Result<(), String>>,
    },
    Close,
}

/// Asynchronous handle to a single WebSocket stream shard connection.
pub struct WsStreamShard {
    cmd_tx: mpsc::UnboundedSender<ShardCommand>,
    pub frames_received: Arc<AtomicU64>,
    pub disconnected: Arc<AtomicBool>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl WsStreamShard {
    pub async fn connect<H: StreamHandler>(
        url: &str,
        registry: Arc<WsStreamRegistry>,
        handler: H,
    ) -> Result<Self, String> {
        let parsed = ParsedWsUrl::parse(url, "")?;
        let mut read_buf = WsReadBuffer::new(256 * 1024);
        let stream = connect_and_handshake(&parsed, &mut read_buf).await?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let frames_received = Arc::new(AtomicU64::new(0));
        let disconnected = Arc::new(AtomicBool::new(false));

        let frames_c = Arc::clone(&frames_received);
        let disc_c = Arc::clone(&disconnected);

        let join = tokio::spawn(run_shard_loop(
            stream,
            read_buf,
            registry,
            handler,
            cmd_rx,
            frames_c,
            disc_c,
        ));

        Ok(Self {
            cmd_tx,
            frames_received,
            disconnected,
            join: Some(join),
        })
    }

    pub async fn subscribe_batch(&self, streams: &[String]) -> Result<(), String> {
        if streams.is_empty() {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ShardCommand::Subscribe {
                streams: streams.to_vec(),
                ack: tx,
            })
            .map_err(|_| "shard loop closed".to_string())?;
        rx.await.map_err(|_| "shard ack dropped".to_string())?
    }

    pub async fn unsubscribe_batch(&self, streams: &[String]) -> Result<(), String> {
        if streams.is_empty() {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ShardCommand::Unsubscribe {
                streams: streams.to_vec(),
                ack: tx,
            })
            .map_err(|_| "shard loop closed".to_string())?;
        rx.await.map_err(|_| "shard ack dropped".to_string())?
    }

    pub async fn close(mut self) {
        let _ = self.cmd_tx.send(ShardCommand::Close);
        if let Some(j) = self.join.take() {
            let _ = j.await;
        }
    }
}

async fn run_shard_loop<H: StreamHandler>(
    stream: TcpStream,
    mut read_buf: WsReadBuffer,
    registry: Arc<WsStreamRegistry>,
    mut handler: H,
    mut cmd_rx: mpsc::UnboundedReceiver<ShardCommand>,
    frames_received: Arc<AtomicU64>,
    disconnected: Arc<AtomicBool>,
) {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut write_scratch = Vec::<u8>::with_capacity(4096);
    let mut ctrl_payload = Vec::<u8>::with_capacity(1024);
    let mut mask_rng = FastMaskRng::new(0xA510_5EED_CAFE_BABEu64);
    let mut bids_scratch = Vec::<DepthLevel>::with_capacity(256);
    let mut asks_scratch = Vec::<DepthLevel>::with_capacity(256);

    let next_req_id = AtomicI64::new(1);
    let mut pending_acks: HashMap<i64, oneshot::Sender<Result<(), String>>> = HashMap::new();
    let mut pong_scratch = [0u8; 125];

    // Drain any frames that piggybacked on the HTTP 101 handshake packet
    let _ = process_buffered_frames(
        &mut read_buf,
        &registry,
        &mut handler,
        &mut bids_scratch,
        &mut asks_scratch,
        &frames_received,
        &mut pending_acks,
        &mut writer,
        &mut mask_rng,
        &mut write_scratch,
        &mut pong_scratch,
    )
    .await;

    loop {
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(ShardCommand::Subscribe { streams, ack }) => {
                        let id = next_req_id.fetch_add(1, Ordering::Relaxed);
                        build_control_json(b"SUBSCRIBE", &streams, id, &mut ctrl_payload);
                        if let Err(e) = write_masked_frame(
                            &mut writer,
                            WsOpcode::Text,
                            &ctrl_payload,
                            &mut mask_rng,
                            &mut write_scratch,
                        ).await {
                            let _ = ack.send(Err(format!("subscribe write failed: {e}")));
                            break;
                        }
                        pending_acks.insert(id, ack);
                    }
                    Some(ShardCommand::Unsubscribe { streams, ack }) => {
                        let id = next_req_id.fetch_add(1, Ordering::Relaxed);
                        build_control_json(b"UNSUBSCRIBE", &streams, id, &mut ctrl_payload);
                        if let Err(e) = write_masked_frame(
                            &mut writer,
                            WsOpcode::Text,
                            &ctrl_payload,
                            &mut mask_rng,
                            &mut write_scratch,
                        ).await {
                            let _ = ack.send(Err(format!("unsubscribe write failed: {e}")));
                            break;
                        }
                        pending_acks.insert(id, ack);
                    }
                    Some(ShardCommand::Close) | None => {
                        let _ = write_masked_frame(
                            &mut writer,
                            WsOpcode::Close,
                            &[0x03, 0xE8],
                            &mut mask_rng,
                            &mut write_scratch,
                        ).await;
                        break;
                    }
                }
            }
            res = read_buf.read_more(&mut reader) => {
                match res {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        let keep_going = process_buffered_frames(
                            &mut read_buf,
                            &registry,
                            &mut handler,
                            &mut bids_scratch,
                            &mut asks_scratch,
                            &frames_received,
                            &mut pending_acks,
                            &mut writer,
                            &mut mask_rng,
                            &mut write_scratch,
                            &mut pong_scratch,
                        ).await;
                        if !keep_going {
                            break;
                        }
                    }
                }
            }
        }
    }

    disconnected.store(true, Ordering::Release);
    for (_, ack) in pending_acks.drain() {
        let _ = ack.send(Err("stream shard closed before ack".to_string()));
    }
    let snap = registry.snapshot();
    for slot in &snap.slots {
        if slot.occupied {
            handler.on_disconnected(slot.sub_id, slot.kind);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_buffered_frames<H: StreamHandler>(
    read_buf: &mut WsReadBuffer,
    registry: &WsStreamRegistry,
    handler: &mut H,
    bids_scratch: &mut Vec<DepthLevel>,
    asks_scratch: &mut Vec<DepthLevel>,
    frames_received: &AtomicU64,
    pending_acks: &mut HashMap<i64, oneshot::Sender<Result<(), String>>>,
    writer: &mut WriteHalf<TcpStream>,
    mask_rng: &mut FastMaskRng,
    write_scratch: &mut Vec<u8>,
    pong_scratch: &mut [u8; 125],
) -> bool {
    let routes = registry.snapshot();
    let ping_len: Option<usize>;

    loop {
        match read_buf.next_frame() {
            Ok(Some(DecodedFrame::Message { payload, .. })) => {
                if !dispatch_stream_frame(
                    payload,
                    &routes,
                    handler,
                    bids_scratch,
                    asks_scratch,
                ) {
                    if let Some(id) = parse_control_ack_id(payload) {
                        if let Some(ack) = pending_acks.remove(&id) {
                            let _ = ack.send(Ok(()));
                        }
                    }
                } else {
                    frames_received.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(Some(DecodedFrame::Ping(p))) => {
                let n = p.len().min(125);
                pong_scratch[..n].copy_from_slice(&p[..n]);
                ping_len = Some(n);
                break;
            }
            Ok(Some(DecodedFrame::Pong(_))) => {}
            Ok(Some(DecodedFrame::Close(_))) | Err(_) => return false,
            Ok(None) => return true,
        }
    }

    if let Some(n) = ping_len {
        if write_masked_frame(
            writer,
            WsOpcode::Pong,
            &pong_scratch[..n],
            mask_rng,
            write_scratch,
        )
        .await
        .is_err()
        {
            return false;
        }
        // Drain any remaining frames after replying to Ping
        return Box::pin(process_buffered_frames(
            read_buf,
            registry,
            handler,
            bids_scratch,
            asks_scratch,
            frames_received,
            pending_acks,
            writer,
            mask_rng,
            write_scratch,
            pong_scratch,
        ))
        .await;
    }

    true
}

fn build_control_json(method: &[u8], streams: &[String], id: i64, dst: &mut Vec<u8>) {
    dst.clear();
    dst.extend_from_slice(br#"{"method":""#);
    dst.extend_from_slice(method);
    dst.extend_from_slice(br#"","params":["#);
    for (idx, s) in streams.iter().enumerate() {
        if idx > 0 {
            dst.push(b',');
        }
        dst.push(b'"');
        dst.extend_from_slice(s.as_bytes());
        dst.push(b'"');
    }
    dst.extend_from_slice(br#"],"id":"#);
    let mut ibuf = [0u8; 24];
    let n = crate::parsing::fast_decimal::format_i64(id, &mut ibuf);
    dst.extend_from_slice(&ibuf[..n]);
    dst.push(b'}');
}

/// Ensures `ReadHalf` stays referenced for clean type checks.
const _: fn(TcpStream) -> (ReadHalf<TcpStream>, WriteHalf<TcpStream>) = tokio::io::split;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_registry_lookup_and_envelope() {
        let reg = WsStreamRegistry::new();
        let id1 = reg.register("btcusdt@bookTicker", StreamKind::BookTicker).unwrap();
        let id2 = reg.register("ethusdt@depth@100ms", StreamKind::DepthDiff).unwrap();
        assert_ne!(id1, id2);

        let snap = reg.snapshot();
        assert_eq!(snap.lookup(b"BTCUSDT@bookTicker"), Some((id1, StreamKind::BookTicker)));
        assert_eq!(snap.lookup(b"ethusdt@depth@100ms"), Some((id2, StreamKind::DepthDiff)));
        assert_eq!(snap.lookup(b"solusdt@bookTicker"), None);

        let msg = br#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1,"s":"BTCUSDT","b":"50000.1","B":"1.5","a":"50000.2","A":"2.0","T":100,"E":101}}"#;
        let (stream, data) = parse_stream_envelope(msg).unwrap();
        assert_eq!(stream, b"btcusdt@bookTicker");
        assert!(data.starts_with(b"{"));
        assert!(data.ends_with(b"}"));
    }
}

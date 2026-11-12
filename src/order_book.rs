//! Local order book synced from REST snapshots + diff depth updates, readable from any thread via SeqLock.

use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, Ordering};
use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::concurrency::seqlock;
use crate::primitives::{DepthLevel, DepthUpdateHeader, Market, Symbol};

const STATE_IDLE: u8 = 0;
const STATE_BUFFERING: u8 = 1;
const STATE_SYNCED: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotKind {
    None,
    Resync,
    Refresh,
}

struct OrderBookSide {
    keys: Vec<f64>,
    quantities: Vec<f64>,
    capacity: usize,
    sign: f64,
    boundary: f64,
}

impl OrderBookSide {
    fn new(capacity: usize, is_ask: bool) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            quantities: Vec::with_capacity(capacity),
            capacity,
            sign: if is_ask { -1.0 } else { 1.0 },
            boundary: f64::NEG_INFINITY,
        }
    }

    #[inline(always)]
    fn is_blind(&self) -> bool {
        self.keys.is_empty() && self.boundary > f64::NEG_INFINITY
    }

    #[inline(always)]
    fn needs_refresh(&self, threshold: usize) -> bool {
        self.keys.len() < threshold && self.boundary > f64::NEG_INFINITY
    }

    fn clear(&mut self) {
        self.keys.clear();
        self.quantities.clear();
        self.boundary = f64::NEG_INFINITY;
    }

    fn load(&mut self, levels: &[DepthLevel], is_full: bool) {
        self.keys.clear();
        self.quantities.clear();
        let count = levels.len().min(self.capacity);
        for i in (0..count).rev() {
            self.keys.push(levels[i].price * self.sign);
            self.quantities.push(levels[i].quantity);
        }
        self.boundary = if is_full && count > 0 {
            self.keys[0]
        } else {
            f64::NEG_INFINITY
        };
    }

    #[inline]
    fn find(&self, key: f64) -> Result<usize, usize> {
        let mut low = 0usize;
        let mut high = self.keys.len();
        while low < high {
            let mid = low + ((high - low) >> 1);
            let cur = unsafe { *self.keys.get_unchecked(mid) };
            if cur < key {
                low = mid + 1;
            } else if cur > key {
                high = mid;
            } else {
                return Ok(mid);
            }
        }
        Err(low)
    }

    #[inline]
    fn apply(&mut self, price: f64, quantity: f64) {
        let key = price * self.sign;
        if key < self.boundary {
            return;
        }
        match self.find(key) {
            Ok(idx) => {
                if quantity == 0.0 {
                    self.keys.remove(idx);
                    self.quantities.remove(idx);
                } else {
                    self.quantities[idx] = quantity;
                }
            }
            Err(pos) => {
                if quantity == 0.0 {
                    return;
                }
                if self.keys.len() < self.capacity {
                    self.keys.insert(pos, key);
                    self.quantities.insert(pos, quantity);
                } else if pos == 0 {
                    self.boundary = self.keys[0];
                } else {
                    self.keys.copy_within(1..pos, 0);
                    self.quantities.copy_within(1..pos, 0);
                    self.keys[pos - 1] = key;
                    self.quantities[pos - 1] = quantity;
                    self.boundary = self.keys[0];
                }
            }
        }
    }

    #[inline]
    fn try_get_best(&self) -> Option<DepthLevel> {
        let n = self.keys.len();
        if n == 0 {
            return None;
        }
        Some(DepthLevel {
            price: self.keys[n - 1] * self.sign,
            quantity: self.quantities[n - 1],
        })
    }

    #[inline]
    fn copy_top(&self, dst: &mut [DepthLevel]) -> usize {
        let stored = self.keys.len();
        let count = stored.min(dst.len());
        for i in 0..count {
            let idx = stored - 1 - i;
            dst[i] = DepthLevel {
                price: self.keys[idx] * self.sign,
                quantity: self.quantities[idx],
            };
        }
        count
    }
}

struct BufferedUpdate {
    first_update_id: i64,
    final_update_id: i64,
    #[allow(dead_code)]
    previous_final_update_id: i64,
    bid_count: usize,
    ask_count: usize,
}

struct BookInner {
    bids: OrderBookSide,
    asks: OrderBookSide,
    last_update_id: i64,
    updates: Vec<BufferedUpdate>,
    levels: Vec<DepthLevel>,
    symbol: Symbol,
    ticket: i64,
    lifetime: i32,
    queued: SnapshotKind,
    awaiting_first: bool,
    refresh_pending: bool,
    attempt: i32,
}

pub struct OrderBook {
    sync: Mutex<()>,
    sequence: AtomicI64,
    state: AtomicU8,
    inner: UnsafeCell<BookInner>,
    refresh_threshold: usize,
    max_buffer_levels: usize,
    snapshot_offset: i64,
}

unsafe impl Send for OrderBook {}
unsafe impl Sync for OrderBook {}

impl OrderBook {
    pub fn new(
        side_capacity: usize,
        refresh_threshold: usize,
        max_buffer_bytes: usize,
        snapshot_offset: i64,
    ) -> Self {
        Self {
            sync: Mutex::new(()),
            sequence: AtomicI64::new(0),
            state: AtomicU8::new(STATE_IDLE),
            inner: UnsafeCell::new(BookInner {
                bids: OrderBookSide::new(side_capacity, false),
                asks: OrderBookSide::new(side_capacity, true),
                last_update_id: 0,
                updates: Vec::with_capacity(64),
                levels: Vec::with_capacity(1024),
                symbol: Symbol::EMPTY,
                ticket: 0,
                lifetime: 0,
                queued: SnapshotKind::None,
                awaiting_first: false,
                refresh_pending: false,
                attempt: 0,
            }),
            refresh_threshold,
            max_buffer_levels: (max_buffer_bytes / core::mem::size_of::<DepthLevel>()).max(1024),
            snapshot_offset,
        }
    }

    #[inline(always)]
    pub fn is_synced(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_SYNCED
    }

    #[inline(always)]
    pub fn version(&self) -> i64 {
        self.sequence.load(Ordering::Acquire) >> 1
    }

    #[inline]
    pub fn try_get_best(&self) -> Option<(DepthLevel, DepthLevel, i64)> {
        if !self.is_synced() {
            return None;
        }
        loop {
            let seq = seqlock::begin_read(&self.sequence);
            if !self.is_synced() {
                return None;
            }
            let inner = unsafe { &*self.inner.get() };
            let bid = inner.bids.try_get_best();
            let ask = inner.asks.try_get_best();
            let uid = inner.last_update_id;
            if seqlock::end_read(&self.sequence, seq) {
                return match (bid, ask) {
                    (Some(b), Some(a)) => Some((b, a, uid)),
                    _ => None,
                };
            }
        }
    }

    #[inline]
    pub fn try_copy_top(
        &self,
        bids_dst: &mut [DepthLevel],
        asks_dst: &mut [DepthLevel],
    ) -> Option<(usize, usize, i64)> {
        if !self.is_synced() {
            return None;
        }
        loop {
            let seq = seqlock::begin_read(&self.sequence);
            if !self.is_synced() {
                return None;
            }
            let inner = unsafe { &*self.inner.get() };
            let bc = inner.bids.copy_top(bids_dst);
            let ac = inner.asks.copy_top(asks_dst);
            let uid = inner.last_update_id;
            if seqlock::end_read(&self.sequence, seq) {
                return Some((bc, ac, uid));
            }
        }
    }

    pub fn update(
        &self,
        header: &DepthUpdateHeader,
        bids: &[DepthLevel],
        asks: &[DepthLevel],
    ) -> (SnapshotKind, i64, bool) {
        let mut lost_sync = false;
        let _guard = self.sync.lock();
        let inner = unsafe { &mut *self.inner.get() };
        let st = self.state.load(Ordering::Relaxed);

        match st {
            STATE_SYNCED => {
                if !self.try_apply_live(inner, header, bids, asks) {
                    lost_sync = true;
                    self.start_buffering(inner, header, bids, asks);
                }
            }
            STATE_BUFFERING => {
                self.buffer_update(inner, header, bids, asks);
            }
            _ => {
                inner.symbol = header.symbol;
                self.start_buffering(inner, header, bids, asks);
            }
        }

        let (kind, ticket) = self.request(inner);
        (kind, ticket, lost_sync)
    }

    fn try_apply_live(
        &self,
        inner: &mut BookInner,
        header: &DepthUpdateHeader,
        bids: &[DepthLevel],
        asks: &[DepthLevel],
    ) -> bool {
        let boundary = inner.last_update_id + self.snapshot_offset;
        if inner.awaiting_first {
            if header.final_update_id < boundary {
                return true;
            }
            if header.first_update_id > boundary {
                return false;
            }
            inner.awaiting_first = false;
        } else {
            let expected_prev = if self.snapshot_offset == 0 {
                header.previous_final_update_id
            } else {
                header.first_update_id - 1
            };
            if expected_prev != inner.last_update_id {
                return false;
            }
        }

        seqlock::begin_write(&self.sequence);
        for lvl in bids {
            inner.bids.apply(lvl.price, lvl.quantity);
        }
        for lvl in asks {
            inner.asks.apply(lvl.price, lvl.quantity);
        }
        inner.last_update_id = header.final_update_id;

        if inner.bids.is_blind() || inner.asks.is_blind() {
            self.state.store(STATE_IDLE, Ordering::Release);
            seqlock::end_write(&self.sequence);
            return false;
        }

        if !inner.refresh_pending
            && (inner.bids.needs_refresh(self.refresh_threshold)
                || inner.asks.needs_refresh(self.refresh_threshold))
        {
            inner.refresh_pending = true;
        }
        seqlock::end_write(&self.sequence);
        true
    }

    fn start_buffering(
        &self,
        inner: &mut BookInner,
        header: &DepthUpdateHeader,
        bids: &[DepthLevel],
        asks: &[DepthLevel],
    ) {
        seqlock::begin_write(&self.sequence);
        self.state.store(STATE_BUFFERING, Ordering::Release);
        seqlock::end_write(&self.sequence);

        inner.awaiting_first = false;
        inner.refresh_pending = false;
        inner.updates.clear();
        inner.levels.clear();
        self.push_buffered(inner, header, bids, asks);
    }

    fn buffer_update(
        &self,
        inner: &mut BookInner,
        header: &DepthUpdateHeader,
        bids: &[DepthLevel],
        asks: &[DepthLevel],
    ) {
        if let Some(last) = inner.updates.last() {
            let expected_prev = if self.snapshot_offset == 0 {
                header.previous_final_update_id
            } else {
                header.first_update_id - 1
            };
            if expected_prev != last.final_update_id
                || inner.levels.len() + bids.len() + asks.len() > self.max_buffer_levels
            {
                inner.updates.clear();
                inner.levels.clear();
            }
        }
        self.push_buffered(inner, header, bids, asks);
    }

    fn push_buffered(
        &self,
        inner: &mut BookInner,
        header: &DepthUpdateHeader,
        bids: &[DepthLevel],
        asks: &[DepthLevel],
    ) {
        inner.levels.extend_from_slice(bids);
        inner.levels.extend_from_slice(asks);
        inner.updates.push(BufferedUpdate {
            first_update_id: header.first_update_id,
            final_update_id: header.final_update_id,
            previous_final_update_id: header.previous_final_update_id,
            bid_count: bids.len(),
            ask_count: asks.len(),
        });
    }

    fn wanted(&self, inner: &BookInner) -> SnapshotKind {
        if self.state.load(Ordering::Relaxed) == STATE_BUFFERING {
            SnapshotKind::Resync
        } else if self.state.load(Ordering::Relaxed) == STATE_SYNCED && inner.refresh_pending {
            SnapshotKind::Refresh
        } else {
            SnapshotKind::None
        }
    }

    fn request(&self, inner: &mut BookInner) -> (SnapshotKind, i64) {
        let wanted = self.wanted(inner);
        if wanted == SnapshotKind::None || inner.queued == wanted {
            return (SnapshotKind::None, 0);
        }
        inner.ticket += 1;
        inner.queued = wanted;
        (wanted, inner.ticket)
    }

    pub fn try_begin_snapshot(&self, ticket: i64) -> Option<(Symbol, i32)> {
        let _guard = self.sync.lock();
        let inner = unsafe { &mut *self.inner.get() };
        if ticket != inner.ticket {
            return None;
        }
        if self.wanted(inner) == SnapshotKind::None {
            inner.queued = SnapshotKind::None;
            return None;
        }
        Some((inner.symbol, inner.lifetime))
    }

    pub fn apply_snapshot(
        &self,
        lifetime: i32,
        snapshot_id: i64,
        snapshot_bids: &[DepthLevel],
        snapshot_asks: &[DepthLevel],
        limit: usize,
    ) -> bool {
        let _guard = self.sync.lock();
        let inner = unsafe { &mut *self.inner.get() };
        if lifetime != inner.lifetime || self.wanted(inner) == SnapshotKind::None {
            return false;
        }

        let boundary = snapshot_id + self.snapshot_offset;
        let mut first = 0usize;
        let mut level_offset = 0usize;

        while first < inner.updates.len() && inner.updates[first].final_update_id < boundary {
            level_offset += inner.updates[first].bid_count + inner.updates[first].ask_count;
            first += 1;
        }

        if first < inner.updates.len() && boundary < inner.updates[first].first_update_id {
            return false;
        }

        seqlock::begin_write(&self.sequence);
        inner.bids.load(snapshot_bids, snapshot_bids.len() >= limit);
        inner.asks.load(snapshot_asks, snapshot_asks.len() >= limit);
        inner.last_update_id = snapshot_id;
        inner.awaiting_first = first == inner.updates.len();

        for i in first..inner.updates.len() {
            let bc = inner.updates[i].bid_count;
            let ac = inner.updates[i].ask_count;
            let fuid = inner.updates[i].final_update_id;
            for j in 0..bc {
                let lvl = inner.levels[level_offset + j];
                inner.bids.apply(lvl.price, lvl.quantity);
            }
            level_offset += bc;
            for j in 0..ac {
                let lvl = inner.levels[level_offset + j];
                inner.asks.apply(lvl.price, lvl.quantity);
            }
            level_offset += ac;
            inner.last_update_id = fuid;
        }

        let next_state = if inner.bids.is_blind() || inner.asks.is_blind() {
            STATE_IDLE
        } else {
            STATE_SYNCED
        };
        self.state.store(next_state, Ordering::Release);
        seqlock::end_write(&self.sequence);

        inner.refresh_pending = false;
        inner.attempt = 0;
        inner.updates.clear();
        inner.levels.clear();
        true
    }

    pub fn finish_snapshot(&self, ticket: i64, applied: bool) -> (SnapshotKind, i64, i32) {
        let _guard = self.sync.lock();
        let inner = unsafe { &mut *self.inner.get() };
        if ticket != inner.ticket {
            return (SnapshotKind::None, 0, 0);
        }
        inner.queued = SnapshotKind::None;
        if !applied {
            inner.attempt += 1;
        }
        let (kind, next_ticket) = self.request(inner);
        (kind, next_ticket, inner.attempt)
    }

    pub fn disconnect(&self) {
        let _guard = self.sync.lock();
        let inner = unsafe { &mut *self.inner.get() };
        seqlock::begin_write(&self.sequence);
        self.state.store(STATE_IDLE, Ordering::Release);
        seqlock::end_write(&self.sequence);
        inner.awaiting_first = false;
        inner.refresh_pending = false;
        inner.updates.clear();
        inner.levels.clear();
    }

    pub fn reset(&self) {
        let _guard = self.sync.lock();
        let inner = unsafe { &mut *self.inner.get() };
        seqlock::begin_write(&self.sequence);
        self.state.store(STATE_IDLE, Ordering::Release);
        inner.bids.clear();
        inner.asks.clear();
        inner.last_update_id = 0;
        seqlock::end_write(&self.sequence);

        inner.symbol = Symbol::EMPTY;
        inner.lifetime += 1;
        inner.ticket += 1;
        inner.queued = SnapshotKind::None;
        inner.attempt = 0;
        inner.awaiting_first = false;
        inner.refresh_pending = false;
        inner.updates.clear();
        inner.levels.clear();
    }
}

#[derive(Clone, Copy)]
struct SnapshotReq {
    subscription: usize,
    ticket: i64,
}

/// Manages local order books across a set of subscriptions with a background REST snapshot worker.
pub struct OrderBookSet {
    books: Box<[OrderBook]>,
    resync_q: Mutex<VecDeque<SnapshotReq>>,
    refresh_q: Mutex<VecDeque<SnapshotReq>>,
    wake: Notify,
    resync_count: AtomicI64,
    snapshot_limit: usize,
    is_fatal: AtomicBool,
}

pub trait SnapshotFetcher: Send + 'static {
    fn fetch_snapshot<'a>(
        &'a mut self,
        symbol: Symbol,
        limit: usize,
        bids: &'a mut [DepthLevel],
        asks: &'a mut [DepthLevel],
    ) -> impl core::future::Future<Output = Option<(i64, usize, usize)>> + Send + 'a;
}

impl OrderBookSet {
    pub fn new(capacity: usize, market: Market, snapshot_limit: usize) -> Arc<Self> {
        let snapshot_offset = if market == Market::Spot { 1 } else { 0 };
        let side_cap = snapshot_limit * 2;
        let refresh_threshold = (snapshot_limit / 4).max(1);
        let mut books = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            books.push(OrderBook::new(
                side_cap,
                refresh_threshold,
                1024 * 1024,
                snapshot_offset,
            ));
        }
        Arc::new(Self {
            books: books.into_boxed_slice(),
            resync_q: Mutex::new(VecDeque::with_capacity(64)),
            refresh_q: Mutex::new(VecDeque::with_capacity(64)),
            wake: Notify::new(),
            resync_count: AtomicI64::new(0),
            snapshot_limit,
            is_fatal: AtomicBool::new(false),
        })
    }

    #[inline(always)]
    pub fn is_synced(&self, subscription: usize) -> bool {
        self.books
            .get(subscription)
            .is_some_and(OrderBook::is_synced)
    }

    #[inline(always)]
    pub fn version(&self, subscription: usize) -> i64 {
        self.books
            .get(subscription)
            .map_or(0, OrderBook::version)
    }

    #[inline(always)]
    pub fn try_get_best(&self, subscription: usize) -> Option<(DepthLevel, DepthLevel, i64)> {
        self.books.get(subscription)?.try_get_best()
    }

    #[inline(always)]
    pub fn try_copy_top(
        &self,
        subscription: usize,
        bids: &mut [DepthLevel],
        asks: &mut [DepthLevel],
    ) -> Option<(usize, usize, i64)> {
        self.books.get(subscription)?.try_copy_top(bids, asks)
    }

    #[inline(always)]
    pub fn resync_count(&self) -> i64 {
        self.resync_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn on_depth_update(
        &self,
        subscription: usize,
        header: &DepthUpdateHeader,
        bids: &[DepthLevel],
        asks: &[DepthLevel],
    ) {
        if self.is_fatal.load(Ordering::Relaxed) {
            return;
        }
        let Some(book) = self.books.get(subscription) else {
            return;
        };
        let (kind, ticket, lost_sync) = book.update(header, bids, asks);
        if lost_sync {
            self.resync_count.fetch_add(1, Ordering::Relaxed);
        }
        if kind != SnapshotKind::None {
            let req = SnapshotReq {
                subscription,
                ticket,
            };
            match kind {
                SnapshotKind::Resync => self.resync_q.lock().push_back(req),
                SnapshotKind::Refresh => self.refresh_q.lock().push_back(req),
                SnapshotKind::None => {}
            }
            self.wake.notify_one();
        }
    }

    #[inline]
    pub fn on_disconnected(&self, subscription: usize) {
        if let Some(book) = self.books.get(subscription) {
            book.disconnect();
        }
    }

    #[inline]
    pub fn on_unsubscribed(&self, subscription: usize) {
        if let Some(book) = self.books.get(subscription) {
            book.reset();
        }
    }

    pub fn snapshot_limit(&self) -> usize {
        self.snapshot_limit
    }

    pub async fn run_snapshot_worker<F: SnapshotFetcher>(
        self: Arc<Self>,
        mut fetcher: F,
    ) {
        let mut snap_bids = vec![DepthLevel::default(); self.snapshot_limit];
        let mut snap_asks = vec![DepthLevel::default(); self.snapshot_limit];

        loop {
            let req = {
                let mut rq = self.resync_q.lock();
                if let Some(r) = rq.pop_front() {
                    Some(r)
                } else {
                    drop(rq);
                    self.refresh_q.lock().pop_front()
                }
            };

            let Some(req) = req else {
                self.wake.notified().await;
                continue;
            };

            let Some(book) = self.books.get(req.subscription) else {
                continue;
            };
            let Some((symbol, lifetime)) = book.try_begin_snapshot(req.ticket) else {
                continue;
            };

            let applied = if let Some((last_id, bc, ac)) = fetcher
                .fetch_snapshot(symbol, self.snapshot_limit, &mut snap_bids, &mut snap_asks)
                .await
            {
                book.apply_snapshot(
                    lifetime,
                    last_id,
                    &snap_bids[..bc],
                    &snap_asks[..ac],
                    self.snapshot_limit,
                )
            } else {
                false
            };

            let (next_kind, next_ticket, attempt) = book.finish_snapshot(req.ticket, applied);
            if next_kind != SnapshotKind::None {
                let delay_ms = (50u64 << attempt.min(6) as u32).min(2000);
                tokio::time::sleep(core::time::Duration::from_millis(delay_ms)).await;
                let next_req = SnapshotReq {
                    subscription: req.subscription,
                    ticket: next_ticket,
                };
                match next_kind {
                    SnapshotKind::Resync => self.resync_q.lock().push_back(next_req),
                    SnapshotKind::Refresh => self.refresh_q.lock().push_back(next_req),
                    SnapshotKind::None => {}
                }
            }
        }
    }
}

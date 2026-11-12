//! Atomic multi-window Binance rate limiter with exchange-header synchronization.

use core::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

#[derive(Clone, Copy, Debug)]
pub struct BinanceLimits {
    pub futures_request_weight_per_min: i32,
    pub futures_order_weight_per_min: i32,
    pub futures_order_weight_per_10s: i32,
    pub futures_ws_api_weight_per_min: i32,
    pub futures_ws_api_shares_rest_weight: bool,
    pub futures_depth_snapshot_weight_percent: i32,
    pub stream_messages_per_sec: i32,
}

impl Default for BinanceLimits {
    fn default() -> Self {
        Self {
            futures_request_weight_per_min: 2400,
            futures_order_weight_per_min: 1200,
            futures_order_weight_per_10s: 300,
            futures_ws_api_weight_per_min: 2400,
            futures_ws_api_shares_rest_weight: true,
            futures_depth_snapshot_weight_percent: 25,
            stream_messages_per_sec: 10,
        }
    }
}

impl BinanceLimits {
    pub fn unlimited() -> Self {
        Self {
            futures_request_weight_per_min: i32::MAX / 100,
            futures_order_weight_per_min: i32::MAX,
            futures_order_weight_per_10s: i32::MAX,
            futures_ws_api_weight_per_min: i32::MAX,
            futures_ws_api_shares_rest_weight: false,
            futures_depth_snapshot_weight_percent: 100,
            stream_messages_per_sec: 1000,
        }
    }
}

pub mod bucket {
    pub const NONE: u32 = 0;
    pub const FUTURES_REQUEST_WEIGHT: u32 = 1 << 0;
    pub const FUTURES_ORDER_MINUTE: u32 = 1 << 1;
    pub const FUTURES_ORDER_10S: u32 = 1 << 2;
    pub const FUTURES_WS_API_REQUEST_WEIGHT: u32 = 1 << 3;
    pub const FUTURES_DEPTH_SNAPSHOT_WEIGHT: u32 = 1 << 4;
}

struct Window {
    limit: i32,
    window_ms: i64,
    window_start_ms: AtomicI64,
    used: AtomicU32,
}

impl Window {
    fn new(limit: i32, window_ms: i64) -> Self {
        Self {
            limit,
            window_ms,
            window_start_ms: AtomicI64::new(0),
            used: AtomicU32::new(0),
        }
    }

    #[inline(always)]
    fn can_charge(&self, now_ms: i64, weight: i32) -> Result<(), i64> {
        let start = self.window_start_ms.load(Ordering::Relaxed);
        let used = if now_ms - start >= self.window_ms {
            0
        } else {
            self.used.load(Ordering::Relaxed) as i32
        };
        if used.saturating_add(weight) <= self.limit {
            Ok(())
        } else {
            Err((start + self.window_ms - now_ms).max(1))
        }
    }

    #[inline(always)]
    fn charge(&self, now_ms: i64, weight: i32) {
        let start = self.window_start_ms.load(Ordering::Relaxed);
        if now_ms - start >= self.window_ms {
            let aligned = now_ms - (now_ms % self.window_ms);
            self.window_start_ms.store(aligned, Ordering::Relaxed);
            self.used.store(weight as u32, Ordering::Relaxed);
        } else {
            self.used.fetch_add(weight as u32, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    fn needs_raise(&self, now_ms: i64, reported: i32) -> bool {
        let start = self.window_start_ms.load(Ordering::Relaxed);
        if now_ms - start >= self.window_ms {
            return false;
        }
        (self.used.load(Ordering::Relaxed) as i32) < reported
    }

    #[inline(always)]
    fn raise(&self, now_ms: i64, reported: i32) {
        let start = self.window_start_ms.load(Ordering::Relaxed);
        if now_ms - start >= self.window_ms {
            let aligned = now_ms - (now_ms % self.window_ms);
            self.window_start_ms.store(aligned, Ordering::Relaxed);
            self.used.store(reported.max(0) as u32, Ordering::Relaxed);
        } else {
            let cur = self.used.load(Ordering::Relaxed) as i32;
            if cur < reported {
                self.used.store(reported as u32, Ordering::Relaxed);
            }
        }
    }
}

pub struct BinanceRateLimiter {
    sync: Mutex<()>,
    futures_request_weight: Window,
    futures_order_min: Window,
    futures_order_10s: Window,
    futures_ws_api_weight: Window,
    futures_depth_snapshot_weight: Window,
    ws_api_shares_rest: bool,
}

impl BinanceRateLimiter {
    pub fn new(limits: &BinanceLimits) -> Self {
        let snap_limit = (limits.futures_request_weight_per_min as i64
            * limits.futures_depth_snapshot_weight_percent as i64
            / 100) as i32;
        Self {
            sync: Mutex::new(()),
            futures_request_weight: Window::new(limits.futures_request_weight_per_min, 60_000),
            futures_order_min: Window::new(limits.futures_order_weight_per_min, 60_000),
            futures_order_10s: Window::new(limits.futures_order_weight_per_10s, 10_000),
            futures_ws_api_weight: Window::new(limits.futures_ws_api_weight_per_min, 60_000),
            futures_depth_snapshot_weight: Window::new(snap_limit.max(1), 60_000),
            ws_api_shares_rest: limits.futures_ws_api_shares_rest_weight,
        }
    }

    #[inline(always)]
    pub fn utc_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    #[inline(always)]
    fn resolve(&self, mut b: u32) -> u32 {
        if self.ws_api_shares_rest && (b & bucket::FUTURES_WS_API_REQUEST_WEIGHT) != 0 {
            b = (b & !bucket::FUTURES_WS_API_REQUEST_WEIGHT) | bucket::FUTURES_REQUEST_WEIGHT;
        }
        b
    }

    #[inline]
    pub fn try_reserve(&self, bucket_raw: u32, weight: i32) -> Result<(), i64> {
        let b = self.resolve(bucket_raw);
        if b == 0 {
            return Ok(());
        }
        let now_ms = Self::utc_millis();
        let _guard = self.sync.lock();

        if (b & bucket::FUTURES_REQUEST_WEIGHT) != 0 {
            self.futures_request_weight.can_charge(now_ms, weight)?;
        }
        if (b & bucket::FUTURES_ORDER_MINUTE) != 0 {
            self.futures_order_min.can_charge(now_ms, weight)?;
        }
        if (b & bucket::FUTURES_ORDER_10S) != 0 {
            self.futures_order_10s.can_charge(now_ms, weight)?;
        }
        if (b & bucket::FUTURES_WS_API_REQUEST_WEIGHT) != 0 {
            self.futures_ws_api_weight.can_charge(now_ms, weight)?;
        }
        if (b & bucket::FUTURES_DEPTH_SNAPSHOT_WEIGHT) != 0 {
            self.futures_depth_snapshot_weight
                .can_charge(now_ms, weight)?;
        }

        if (b & bucket::FUTURES_REQUEST_WEIGHT) != 0 {
            self.futures_request_weight.charge(now_ms, weight);
        }
        if (b & bucket::FUTURES_ORDER_MINUTE) != 0 {
            self.futures_order_min.charge(now_ms, weight);
        }
        if (b & bucket::FUTURES_ORDER_10S) != 0 {
            self.futures_order_10s.charge(now_ms, weight);
        }
        if (b & bucket::FUTURES_WS_API_REQUEST_WEIGHT) != 0 {
            self.futures_ws_api_weight.charge(now_ms, weight);
        }
        if (b & bucket::FUTURES_DEPTH_SNAPSHOT_WEIGHT) != 0 {
            self.futures_depth_snapshot_weight.charge(now_ms, weight);
        }
        Ok(())
    }

    #[inline]
    pub fn update_weight(&self, bucket_raw: u32, weight: i32) {
        let b = self.resolve(bucket_raw);
        let now_ms = Self::utc_millis();
        let win = match b {
            bucket::FUTURES_REQUEST_WEIGHT => &self.futures_request_weight,
            bucket::FUTURES_ORDER_MINUTE => &self.futures_order_min,
            bucket::FUTURES_ORDER_10S => &self.futures_order_10s,
            bucket::FUTURES_WS_API_REQUEST_WEIGHT => &self.futures_ws_api_weight,
            _ => return,
        };
        if !win.needs_raise(now_ms, weight) {
            return;
        }
        let _guard = self.sync.lock();
        win.raise(now_ms, weight);
    }
}


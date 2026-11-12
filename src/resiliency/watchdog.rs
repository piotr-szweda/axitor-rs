use crate::resiliency::network_gate::NetworkGate;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// Watchdog that monitors a high-volume market data stream (e.g. `btcusdt@bookTicker`).
/// If no frame arrives within `timeout`, trips `NetworkGate` and notifies recovery workers.
pub struct StreamWatchdog {
    last_frame_millis: Arc<AtomicI64>,
    timeout: Duration,
    gate: Arc<NetworkGate>,
    reconnect_notify: Arc<Notify>,
    active: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl StreamWatchdog {
    pub fn new(
        timeout: Duration,
        gate: Arc<NetworkGate>,
        reconnect_notify: Arc<Notify>,
    ) -> Self {
        Self {
            last_frame_millis: Arc::new(AtomicI64::new(crate::rate_limiter::BinanceRateLimiter::utc_millis())),
            timeout,
            gate,
            reconnect_notify,
            active: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    #[inline(always)]
    pub fn on_frame_received(&self) {
        let now = crate::rate_limiter::BinanceRateLimiter::utc_millis();
        self.last_frame_millis.store(now, Ordering::Relaxed);
    }

    pub fn start(&mut self) {
        if self.active.swap(true, Ordering::SeqCst) {
            return;
        }

        let last_time = Arc::clone(&self.last_frame_millis);
        let timeout_ms = self.timeout.as_millis() as i64;
        let poll_interval = (self.timeout / 3).max(Duration::from_millis(50));
        let gate = Arc::clone(&self.gate);
        let notify = Arc::clone(&self.reconnect_notify);
        let active = Arc::clone(&self.active);

        let handle = tokio::spawn(async move {
            while active.load(Ordering::Relaxed) {
                tokio::time::sleep(poll_interval).await;
                if !active.load(Ordering::Relaxed) {
                    break;
                }

                let last = last_time.load(Ordering::Relaxed);
                let now = crate::rate_limiter::BinanceRateLimiter::utc_millis();
                let silence = now - last;

                if silence > timeout_ms && !gate.is_tripped() {
                    // Watchdog barks! Stream went silent during market hours
                    gate.trip();
                    notify.notify_waiters();
                }
            }
        });

        self.handle = Some(handle);
    }

    pub fn stop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

impl Drop for StreamWatchdog {
    fn drop(&mut self) {
        self.stop();
    }
}


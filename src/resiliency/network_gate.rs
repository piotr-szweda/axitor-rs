use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Global network gate for tripping on connection failure or silence.
/// When tripped, requests wait until a background probe or repair succeeds.
pub struct NetworkGate {
    tripped: AtomicBool,
    notify: Notify,
    last_repair: Mutex<Instant>,
}

impl NetworkGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tripped: AtomicBool::new(false),
            notify: Notify::new(),
            last_repair: Mutex::new(Instant::now()),
        })
    }

    #[inline(always)]
    pub fn is_tripped(&self) -> bool {
        self.tripped.load(Ordering::Acquire)
    }

    /// Trips the gate. Returns true if this call tripped it from open to closed.
    pub fn trip(&self) -> bool {
        let was_open = self
            .tripped
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok();
        was_open
    }

    /// Restores the gate and wakes all waiting tasks.
    pub fn open(&self) {
        if self
            .tripped
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            *self.last_repair.lock() = Instant::now();
            self.notify.notify_waiters();
        }
    }

    /// Waits until the gate is open before proceeding.
    pub async fn wait_until_open(&self) {
        while self.is_tripped() {
            self.notify.notified().await;
        }
    }

    pub fn last_repair_elapsed(&self) -> Duration {
        self.last_repair.lock().elapsed()
    }
}

impl Default for NetworkGate {
    fn default() -> Self {
        Self {
            tripped: AtomicBool::new(false),
            notify: Notify::new(),
            last_repair: Mutex::new(Instant::now()),
        }
    }
}


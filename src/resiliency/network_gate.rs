use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkGateTimeout;

impl std::fmt::Display for NetworkGateTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "network gate timed out waiting for network repair")
    }
}

impl std::error::Error for NetworkGateTimeout {}

/// Global network gate for tripping on connection failure, silence, or rate-limit bans (HTTP 418/429).
/// When tripped, requests wait until a background probe or repair succeeds (bounded by timeout).
pub struct NetworkGate {
    tripped: AtomicBool,
    notify: Arc<Notify>,
    repair_trigger: Arc<Notify>,
    last_repair: Mutex<Instant>,
}

impl NetworkGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            tripped: AtomicBool::new(false),
            notify: Arc::new(Notify::new()),
            repair_trigger: Arc::new(Notify::new()),
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
        if was_open {
            self.repair_trigger.notify_waiters();
        }
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

    /// Waits until the gate is open before proceeding (unbounded).
    pub async fn wait_until_open(&self) {
        while self.is_tripped() {
            self.notify.notified().await;
        }
    }

    /// Waits until the gate is open, bounded by `timeout`.
    /// Returns `Err(NetworkGateTimeout)` if the network is not repaired within `timeout`.
    pub async fn wait_until_open_timeout(&self, timeout: Duration) -> Result<(), NetworkGateTimeout> {
        if !self.is_tripped() {
            return Ok(());
        }
        tokio::time::timeout(timeout, async {
            while self.is_tripped() {
                self.notify.notified().await;
            }
        })
        .await
        .map_err(|_| NetworkGateTimeout)
    }

    pub fn repair_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.repair_trigger)
    }

    pub fn last_repair_elapsed(&self) -> Duration {
        self.last_repair.lock().elapsed()
    }
}

impl Default for NetworkGate {
    fn default() -> Self {
        Self {
            tripped: AtomicBool::new(false),
            notify: Arc::new(Notify::new()),
            repair_trigger: Arc::new(Notify::new()),
            last_repair: Mutex::new(Instant::now()),
        }
    }
}

/// Background worker that actively probes the exchange endpoint when the gate is tripped.
/// When the probe succeeds (exchange is reachable again), it restores the gate via `open()`.
pub struct NetworkRepairWorker {
    gate: Arc<NetworkGate>,
    probe_addr: String,
    poll_interval: Duration,
    running: Arc<AtomicBool>,
    handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl NetworkRepairWorker {
    pub fn new(gate: Arc<NetworkGate>, probe_addr: String, poll_interval: Duration) -> Self {
        Self {
            gate,
            probe_addr,
            poll_interval: poll_interval.max(Duration::from_millis(100)),
            running: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
        }
    }

    pub fn start(&self) {
        if self.running.swap(true, Ordering::SeqCst) {
            return;
        }

        let running = Arc::clone(&self.running);
        let gate = Arc::clone(&self.gate);
        let repair_notify = gate.repair_notify();
        let addr = self.probe_addr.clone();
        let interval = self.poll_interval;

        let handle = tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                // If gate is healthy, park and await next trip trigger
                if !gate.is_tripped() {
                    repair_notify.notified().await;
                }
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                // Gate is tripped! Probe the host/port in a loop until connection succeeds
                while gate.is_tripped() && running.load(Ordering::Relaxed) {
                    if let Ok(Ok(_)) = tokio::time::timeout(
                        Duration::from_millis(1000),
                        tokio::net::TcpStream::connect(&addr),
                    )
                    .await
                    {
                        // Exchange is back online! Heal the gate
                        gate.open();
                        break;
                    }
                    tokio::time::sleep(interval).await;
                }
            }
        });

        *self.handle.lock() = Some(handle);
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(h) = self.handle.lock().take() {
            h.abort();
        }
    }
}

impl Drop for NetworkRepairWorker {
    fn drop(&mut self) {
        self.stop();
    }
}


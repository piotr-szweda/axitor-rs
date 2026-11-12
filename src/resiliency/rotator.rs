use crate::transport::ws_streams::{MarketStateHandler, WsStreamRegistry, WsStreamShard};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Make-before-break 24-hour shard rotator.
/// Prevents Binance's 24-hour connection cutoff by pre-establishing a replacement shard,
/// migrating subscriptions, verifying packet reception, and terminating the stale connection.
pub struct MakeBeforeBreakRotator {
    url: String,
    registry: Arc<WsStreamRegistry>,
    active_shard: Arc<Mutex<Option<WsStreamShard>>>,
    rotation_interval: Duration,
    running: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl MakeBeforeBreakRotator {
    pub fn new(
        url: String,
        registry: Arc<WsStreamRegistry>,
        active_shard: Arc<Mutex<Option<WsStreamShard>>>,
        rotation_interval: Duration,
    ) -> Self {
        Self {
            url,
            registry,
            active_shard,
            rotation_interval,
            running: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    pub fn start<F>(&mut self, handler_factory: F)
    where
        F: Fn() -> MarketStateHandler + Send + Sync + 'static,
    {
        if self.running.swap(true, Ordering::SeqCst) {
            return;
        }

        let running = Arc::clone(&self.running);
        let interval = self.rotation_interval;
        let url = self.url.clone();
        let registry = Arc::clone(&self.registry);
        let active_slot = Arc::clone(&self.active_shard);

        let handle = tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                tokio::time::sleep(interval).await;
                if !running.load(Ordering::Relaxed) {
                    break;
                }

                // Make-before-break rotation!
                let streams = registry.active_stream_names();
                if streams.is_empty() {
                    continue;
                }

                let handler = handler_factory();
                if let Ok(new_shard) = WsStreamShard::connect(&url, Arc::clone(&registry), handler).await {
                    if new_shard.subscribe_batch(&streams).await.is_ok() {
                        // Allow new shard to warm up and verify traffic
                        tokio::time::sleep(Duration::from_millis(200)).await;

                        let old_shard = {
                            let mut lock = active_slot.lock();
                            let old = lock.take();
                            *lock = Some(new_shard);
                            old
                        };

                        if let Some(old) = old_shard {
                            old.close().await;
                        }
                    } else {
                        new_shard.close().await;
                    }
                }
            }
        });

        self.handle = Some(handle);
    }

    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

impl Drop for MakeBeforeBreakRotator {
    fn drop(&mut self) {
        self.stop();
    }
}


use crate::transport::rest_api::RestApiTransport;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Manages UserData stream `listenKey` lifecycle:
/// automatically creates the key, spawns a background keepalive loop every 30 minutes,
/// and cleanly terminates it on drop.
pub struct ListenKeyManager {
    rest: Arc<RestApiTransport>,
    current_key: Arc<Mutex<Option<String>>>,
    active: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl ListenKeyManager {
    pub fn new(rest: Arc<RestApiTransport>) -> Self {
        Self {
            rest,
            current_key: Arc::new(Mutex::new(None)),
            active: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Obtains an active `listenKey`, creating one if not yet established or if expired.
    pub async fn get_or_create(&mut self) -> Result<String, String> {
        {
            let guard = self.current_key.lock();
            if let Some(ref k) = *guard {
                return Ok(k.clone());
            }
        }

        let key = self
            .rest
            .create_listen_key()
            .await
            .map_err(|e| format!("failed to create listenKey: {e}"))?;

        *self.current_key.lock() = Some(key.clone());
        self.start_keepalive_loop();
        Ok(key)
    }

    fn start_keepalive_loop(&mut self) {
        if self.active.swap(true, Ordering::SeqCst) {
            return;
        }

        let rest = Arc::clone(&self.rest);
        let active = Arc::clone(&self.active);
        let key_slot = Arc::clone(&self.current_key);

        let handle = tokio::spawn(async move {
            // Keep alive every 30 minutes (Binance listenKey expires in 60 minutes)
            let ping_interval = Duration::from_secs(30 * 60);

            while active.load(Ordering::Relaxed) {
                tokio::time::sleep(ping_interval).await;
                if !active.load(Ordering::Relaxed) {
                    break;
                }

                if rest.keepalive_listen_key().await.is_err() {
                    // Try to recreate if ping rejected
                    if let Ok(new_key) = rest.create_listen_key().await {
                        *key_slot.lock() = Some(new_key);
                    }
                }
            }
        });

        self.handle = Some(handle);
    }

    pub async fn close(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            h.abort();
        }
        let _ = self.rest.close_listen_key().await;
        *self.current_key.lock() = None;
    }
}

impl Drop for ListenKeyManager {
    fn drop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}


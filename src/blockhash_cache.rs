use solana_client::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use std::sync::{Arc, RwLock};
use tokio::time::{interval, Duration};
use tracing::{debug, warn};

/// Single-writer / many-reader blockhash cache. The hot-path `get()` is
/// a tiny RwLock read (no contention with other readers); only the 1s
/// refresh task takes the write lock briefly.
pub struct BlockhashCache {
    inner: Arc<RwLock<Hash>>,
}

impl BlockhashCache {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        eprintln!("[rpc_fetch_reason] reason=blockhash count=1 pubkeys_sample=[]");
        let initial = rpc.get_latest_blockhash().unwrap_or_default();
        let inner = Arc::new(RwLock::new(initial));
        let shared = inner.clone();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(1));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let rpc_clone = rpc.clone();
                let result = tokio::task::spawn_blocking(move || {
                    eprintln!("[rpc_fetch_reason] reason=blockhash count=1 pubkeys_sample=[]");
                    rpc_clone.get_latest_blockhash()
                }).await;

                match result {
                    Ok(Ok(h)) => {
                        *shared.write().unwrap() = h;
                        debug!("blockhash refreshed");
                    }
                    Ok(Err(e)) => warn!(error = %e, "blockhash RPC failed"),
                    Err(e) => warn!(error = %e, "blockhash spawn_blocking panicked"),
                }
            }
        });

        Self { inner }
    }

    #[inline]
    pub fn get(&self) -> Hash {
        *self.inner.read().unwrap()
    }
}

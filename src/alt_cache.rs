use anyhow::{Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use solana_account::Account;
use solana_client::rpc_client::RpcClient;
use solana_sdk::{address_lookup_table::AddressLookupTableAccount, pubkey::Pubkey};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};
use tracing::{debug, warn};

use crate::transaction::deserialize_alt_addresses;

const ALT_CACHE_SCHEMA_VERSION: u32 = 1;

#[derive(Clone)]
pub struct AltCache {
    inner: Arc<RwLock<HashMap<Pubkey, AddressLookupTableAccount>>>,
    raw_records: Arc<RwLock<HashMap<Pubkey, CachedAltRecord>>>,
    persist_path: Arc<RwLock<Option<PathBuf>>>,
}

#[derive(Serialize, Deserialize)]
struct AltCacheFile {
    schema_version: u32,
    alts: Vec<CachedAltRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
struct CachedAltRecord {
    pubkey: String,
    owner: Option<String>,
    lamports: Option<u64>,
    executable: Option<bool>,
    rent_epoch: Option<u64>,
    data_base64: Option<String>,
    addresses: Vec<String>,
    fetched_slot: Option<u64>,
    status: String,
}

impl AltCache {
    pub fn new(_tip_pubkeys: Vec<Pubkey>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            raw_records: Arc::new(RwLock::new(HashMap::new())),
            persist_path: Arc::new(RwLock::new(None)),
        }
    }

    pub fn load_from_disk<P: AsRef<Path>>(&self, path: P) -> Result<usize> {
        let path = path.as_ref().to_path_buf();
        *self.persist_path.write().unwrap() = Some(path.clone());
        if !path.exists() {
            eprintln!(
                "[alt_cache] status=missing path={} loaded=0",
                path.display()
            );
            return Ok(0);
        }

        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("cannot read {}", path.display()))?;
        let file: AltCacheFile = serde_json::from_str(&content)
            .with_context(|| format!("invalid {}", path.display()))?;
        if file.schema_version != ALT_CACHE_SCHEMA_VERSION {
            eprintln!(
                "[alt_cache] status=stale_schema path={} cached_schema={} expected_schema={} loaded=0",
                path.display(),
                file.schema_version,
                ALT_CACHE_SCHEMA_VERSION
            );
            return Ok(0);
        }

        let mut loaded = 0usize;
        let mut inner = self.inner.write().unwrap();
        let mut raw = self.raw_records.write().unwrap();
        for record in file.alts {
            if record.status != "valid" {
                continue;
            }
            let Ok(pubkey) = Pubkey::try_from(record.pubkey.as_str()) else {
                continue;
            };
            let addresses = record
                .addresses
                .iter()
                .filter_map(|addr| Pubkey::try_from(addr.as_str()).ok())
                .collect::<Vec<_>>();
            if addresses.is_empty() {
                continue;
            }
            inner.insert(
                pubkey,
                AddressLookupTableAccount {
                    key: pubkey,
                    addresses,
                },
            );
            raw.insert(pubkey, record);
            loaded += 1;
        }

        eprintln!(
            "[alt_cache] status=loaded path={} loaded={}",
            path.display(),
            loaded
        );
        Ok(loaded)
    }

    pub async fn prefetch_missing_rate_limited(
        &self,
        pubkeys: &[Pubkey],
        rpc: Arc<RpcClient>,
        requests_per_second: u64,
    ) {
        let rate = requests_per_second.max(1).min(5);
        let delay = Duration::from_millis((1000 / rate).max(1));
        let mut keys = pubkeys.to_vec();
        keys.sort_unstable();
        keys.dedup();
        keys.retain(|pk| !self.contains(pk));

        if keys.is_empty() {
            eprintln!("[alt_cache] prefetch_missing=0");
            return;
        }

        eprintln!(
            "[alt_cache] prefetch_start missing={} rpc_rate={}/sec",
            keys.len(),
            rate
        );
        for (idx, pubkey) in keys.iter().copied().enumerate() {
            let cache = self.clone();
            let rpc = rpc.clone();
            let result = tokio::task::spawn_blocking(move || cache.get_or_fetch(&pubkey, &rpc)).await;
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    eprintln!("[alt_cache] prefetch_miss alt={} error={}", pubkey, e);
                }
                Err(e) => {
                    eprintln!("[alt_cache] prefetch_task_error alt={} error={:?}", pubkey, e);
                }
            }
            if (idx + 1) % 10 == 0 || idx + 1 == keys.len() {
                eprintln!(
                    "[alt_cache] prefetch_progress fetched={}/{}",
                    idx + 1,
                    keys.len()
                );
            }
            tokio::time::sleep(delay).await;
        }
        eprintln!("[alt_cache] prefetch_complete total={}", keys.len());
    }

    pub fn get_or_fetch(
        &self,
        pubkey: &Pubkey,
        rpc: &RpcClient,
    ) -> Result<AddressLookupTableAccount> {
        {
            let cache = self.inner.read().unwrap();
            if let Some(alt) = cache.get(pubkey) {
                debug!(alt = %pubkey, "ALT cache hit");
                return Ok(alt.clone());
            }
        }

        debug!(alt = %pubkey, "ALT cache miss -- fetching from RPC");
        eprintln!(
            "[rpc_fetch_reason] reason=alt_account count=1 pubkeys_sample=[\"{}\"]",
            pubkey
        );
        let account = rpc
            .get_account(pubkey)
            .map_err(|e| anyhow::anyhow!("failed to fetch ALT {}: {}", pubkey, e))?;

        let addresses = deserialize_alt_addresses(&account.data)?;

        let alt = AddressLookupTableAccount {
            key: *pubkey,
            addresses: addresses.clone(),
        };

        self.inner.write().unwrap().insert(*pubkey, alt.clone());
        self.raw_records.write().unwrap().insert(
            *pubkey,
            CachedAltRecord {
                pubkey: pubkey.to_string(),
                owner: Some(account.owner.to_string()),
                lamports: Some(account.lamports),
                executable: Some(account.executable),
                rent_epoch: Some(account.rent_epoch),
                data_base64: Some(base64::engine::general_purpose::STANDARD.encode(&account.data)),
                addresses: addresses.iter().map(ToString::to_string).collect(),
                fetched_slot: None,
                status: "valid".to_string(),
            },
        );
        self.save_to_disk_snapshot();
        Ok(alt)
    }

    pub fn update_from_account_data(
        &self,
        pubkey: Pubkey,
        account: &Account,
    ) -> Result<AddressLookupTableAccount> {
        let addresses = deserialize_alt_addresses(&account.data)?;
        let alt = AddressLookupTableAccount {
            key: pubkey,
            addresses: addresses.clone(),
        };

        self.inner.write().unwrap().insert(pubkey, alt.clone());
        self.raw_records.write().unwrap().insert(
            pubkey,
            CachedAltRecord {
                pubkey: pubkey.to_string(),
                owner: Some(account.owner.to_string()),
                lamports: Some(account.lamports),
                executable: Some(account.executable),
                rent_epoch: Some(account.rent_epoch),
                data_base64: Some(base64::engine::general_purpose::STANDARD.encode(&account.data)),
                addresses: addresses.iter().map(ToString::to_string).collect(),
                fetched_slot: None,
                status: "valid".to_string(),
            },
        );
        self.save_to_disk_snapshot();
        Ok(alt)
    }

    #[allow(dead_code)]
    pub fn clear(&self) {
        self.inner.write().unwrap().clear();
        self.raw_records.write().unwrap().clear();
        warn!("ALT cache cleared");
    }

    fn contains(&self, pubkey: &Pubkey) -> bool {
        self.inner.read().unwrap().contains_key(pubkey)
    }

    fn save_to_disk_snapshot(&self) {
        if let Err(e) = self.save_to_disk() {
            eprintln!("[alt_cache] save_error error={}", e);
        }
    }

    fn save_to_disk(&self) -> Result<()> {
        let Some(path) = self.persist_path.read().unwrap().clone() else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("cannot create {}", parent.display()))?;
        }

        let inner = self.inner.read().unwrap();
        let raw = self.raw_records.read().unwrap();
        let mut alts = Vec::new();
        for (pubkey, alt) in inner.iter() {
            if let Some(record) = raw.get(pubkey) {
                alts.push(record.clone());
            } else {
                alts.push(CachedAltRecord {
                    pubkey: pubkey.to_string(),
                    owner: None,
                    lamports: None,
                    executable: None,
                    rent_epoch: None,
                    data_base64: None,
                    addresses: alt.addresses.iter().map(ToString::to_string).collect(),
                    fetched_slot: None,
                    status: "valid".to_string(),
                });
            }
        }
        alts.sort_by(|a, b| a.pubkey.cmp(&b.pubkey));
        let file = AltCacheFile {
            schema_version: ALT_CACHE_SCHEMA_VERSION,
            alts,
        };
        std::fs::write(&path, serde_json::to_vec_pretty(&file)?)
            .with_context(|| format!("cannot write {}", path.display()))?;
        Ok(())
    }
}

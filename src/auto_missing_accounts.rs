//! Automatic missing-account learning system.
//!
//! When LiteSVM preflight fails with `preflight_missing_account`, the missing
//! pubkey is enqueued via a non-blocking channel. A background task flushes
//! the queue to `missing_sim_accounts_pending.json`, then fetches the accounts
//! from RPC and injects them into the live `AccountCache` so the next
//! simulation attempt for the same route will find them.

use anyhow::{Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use solana_account::Account;
use solana_address::Address;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

use crate::account_cache::AccountCache;

const PENDING_SCHEMA_VERSION: u32 = 1;
const CACHE_SCHEMA_VERSION: u32 = 1;

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const DEX_OWNER_IDS: &[&str] = &[
    "ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA",
    "SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EkAW7vAB",
    "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq",
    "E2uCGJ4TtYyKPGaK57UMfbs9sgaumwDEZF1aAY6fF3mS",
    "ZERor4xhbiBXfsSBzNe7aXPAFuSCmrAMXnRtJ6grr8E",
    "MFv2hWf31Z9kbCa1snEPdcgp168vLVQL5iryD51M3P2",
    "1DEXqTDCE7JqpSNFKHJxEsWGEYWFJqNjmE5deTiaTHU",
];

// ── Public types ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct MissingAccountEvent {
    pub pubkey: Pubkey,
    pub route_sig: u128,
    pub route_labels: String,
    pub programs: String,
    pub source: String,
    pub is_signer: bool,
    pub is_writable: bool,
    pub created_by_setup: bool,
}

/// Cheap handle passed to the hot simulation path. `record()` is non-blocking.
#[derive(Clone)]
pub struct AutoMissingAccountsHandle {
    sender: mpsc::Sender<MissingAccountEvent>,
}

impl AutoMissingAccountsHandle {
    #[inline]
    pub fn record(&self, event: MissingAccountEvent) {
        let _ = self.sender.try_send(event);
    }
}

// ── File schemas ──────────────────────────────────────────────────────────────

#[derive(Clone, Serialize, Deserialize)]
struct PendingRecord {
    pubkey: String,
    first_seen_unix: u64,
    last_seen_unix: u64,
    seen_count: u32,
    route_sig: String,
    route_labels: serde_json::Value,
    programs: serde_json::Value,
    source: String,
    is_signer: bool,
    is_writable: bool,
    created_by_setup: bool,
    /// pending_rpc_fetch | cached | failed
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    retry_count: u32,
}

#[derive(Serialize, Deserialize)]
struct PendingFile {
    schema_version: u32,
    accounts: Vec<PendingRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
struct CachedRecord {
    pubkey: String,
    owner: String,
    lamports: u64,
    executable: bool,
    rent_epoch: u64,
    data_base64: String,
    data_len: usize,
    fetched_slot: u64,
    source: String,
    classification: String,
    is_writable_seen: bool,
    last_seen_unix: u64,
    seen_count: u32,
    /// valid | stale
    status: String,
    needs_live_subscription: bool,
}

#[derive(Serialize, Deserialize)]
struct CacheFile {
    schema_version: u32,
    accounts: Vec<CachedRecord>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ErrorRecord {
    pubkey: String,
    error: String,
    retry_count: u32,
    last_error_unix: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct LiveRecord {
    pubkey: String,
    owner: String,
    classification: String,
    is_writable_seen: bool,
    first_seen_unix: u64,
}

// ── Service ───────────────────────────────────────────────────────────────────

struct AutoMissingAccountsService {
    pending_path: PathBuf,
    cache_path: PathBuf,
    errors_path: PathBuf,
    live_path: PathBuf,
    rpc: Arc<RpcClient>,
    account_cache: AccountCache,
    receiver: mpsc::Receiver<MissingAccountEvent>,
}

/// Spawn the background service and return a cheap handle for the hot path.
pub fn start(
    manual_accounts_root: &Path,
    rpc: Arc<RpcClient>,
    account_cache: AccountCache,
) -> AutoMissingAccountsHandle {
    let (sender, receiver) = mpsc::channel(8192);
    let service = AutoMissingAccountsService {
        pending_path: manual_accounts_root.join("missing_sim_accounts_pending.json"),
        cache_path: manual_accounts_root.join("missing_sim_account_cache.json"),
        errors_path: manual_accounts_root.join("missing_sim_account_errors.json"),
        live_path: manual_accounts_root.join("missing_sim_account_live.json"),
        rpc,
        account_cache,
        receiver,
    };
    tokio::spawn(service.run());
    AutoMissingAccountsHandle { sender }
}

/// Load previously-fetched accounts into `AccountCache` at startup.
pub fn load_cache_into_account_cache(manual_accounts_root: &Path, cache: &AccountCache) -> usize {
    let path = manual_accounts_root.join("missing_sim_account_cache.json");
    match load_and_inject_cache(&path, cache) {
        Ok(n) => {
            eprintln!(
                "[missing_account_cache] status=loaded path={} loaded={}",
                path.display(),
                n
            );
            n
        }
        Err(e) => {
            eprintln!(
                "[missing_account_cache] status=load_error path={} error={}",
                path.display(),
                e
            );
            0
        }
    }
}

fn load_and_inject_cache(path: &Path, cache: &AccountCache) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("read {}", path.display()))?;
    let file: CacheFile = serde_json::from_str(&content)
        .with_context(|| format!("parse {}", path.display()))?;
    if file.schema_version != CACHE_SCHEMA_VERSION {
        return Ok(0);
    }
    let mut loaded = 0usize;
    for rec in file.accounts {
        if rec.status != "valid" {
            continue;
        }
        let Ok(pk) = Pubkey::try_from(rec.pubkey.as_str()) else { continue };
        let Ok(owner) = Pubkey::try_from(rec.owner.as_str()) else { continue };
        let Ok(data) = base64::engine::general_purpose::STANDARD
            .decode(rec.data_base64.as_bytes()) else { continue };
        cache.insert_manual(
            pk,
            Account {
                lamports: rec.lamports,
                data,
                owner: Address::from(owner.to_bytes()),
                executable: rec.executable,
                rent_epoch: rec.rent_epoch,
            },
        );
        loaded += 1;
    }
    Ok(loaded)
}

// ── Background task ───────────────────────────────────────────────────────────

impl AutoMissingAccountsService {
    async fn run(mut self) {
        let mut buffer: Vec<MissingAccountEvent> = Vec::new();
        let mut flush_timer = tokio::time::interval(Duration::from_secs(10));
        let mut fetch_timer = tokio::time::interval(Duration::from_secs(30));
        flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        fetch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Drain the first tick so we don't immediately fetch on start
        flush_timer.tick().await;
        fetch_timer.tick().await;

        loop {
            tokio::select! {
                Some(ev) = self.receiver.recv() => {
                    buffer.push(ev);
                    if buffer.len() >= 256 {
                        self.flush_buffer(&mut buffer).await;
                    }
                }
                _ = flush_timer.tick() => {
                    if !buffer.is_empty() {
                        self.flush_buffer(&mut buffer).await;
                    }
                }
                _ = fetch_timer.tick() => {
                    self.background_fetch().await;
                }
            }
        }
    }

    async fn flush_buffer(&self, buffer: &mut Vec<MissingAccountEvent>) {
        let events = std::mem::take(buffer);
        let path = self.pending_path.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Err(e) = flush_events_to_pending(&path, &events) {
                eprintln!("[missing_account_flush] error={}", e);
            }
        })
        .await;
    }

    async fn background_fetch(&self) {
        let pending_path = self.pending_path.clone();
        let cache_path = self.cache_path.clone();
        let errors_path = self.errors_path.clone();
        let live_path = self.live_path.clone();
        let rpc = self.rpc.clone();
        let account_cache = self.account_cache.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Err(e) =
                fetch_pending(&pending_path, &cache_path, &errors_path, &live_path, &rpc, &account_cache)
            {
                eprintln!("[missing_account_fetch] error={}", e);
            }
        })
        .await;
    }
}

// ── Core logic (runs on blocking thread) ─────────────────────────────────────

fn flush_events_to_pending(path: &Path, events: &[MissingAccountEvent]) -> Result<()> {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let mut records = read_pending(path)?;
    let now = unix_now();
    for ev in events {
        let pk_str = ev.pubkey.to_string();
        let sig_str = format!("{:032x}", ev.route_sig);
        if let Some(r) = records.iter_mut().find(|r| r.pubkey == pk_str) {
            r.seen_count += 1;
            r.last_seen_unix = now;
            r.route_sig = sig_str;
            r.route_labels = parse_jsonish(&ev.route_labels);
            r.programs = parse_jsonish(&ev.programs);
            r.is_writable = r.is_writable || ev.is_writable;
        } else {
            eprintln!(
                "[missing_account_recorded] pubkey={} source={} is_writable={} status=pending_rpc_fetch",
                pk_str, ev.source, ev.is_writable
            );
            records.push(PendingRecord {
                pubkey: pk_str,
                first_seen_unix: now,
                last_seen_unix: now,
                seen_count: 1,
                route_sig: sig_str,
                route_labels: parse_jsonish(&ev.route_labels),
                programs: parse_jsonish(&ev.programs),
                source: ev.source.clone(),
                is_signer: ev.is_signer,
                is_writable: ev.is_writable,
                created_by_setup: ev.created_by_setup,
                status: "pending_rpc_fetch".to_string(),
                last_error: None,
                retry_count: 0,
            });
        }
    }
    write_pending(path, &records)
}

fn fetch_pending(
    pending_path: &Path,
    cache_path: &Path,
    errors_path: &Path,
    live_path: &Path,
    rpc: &RpcClient,
    account_cache: &AccountCache,
) -> Result<()> {
    let mut pending = read_pending(pending_path)?;
    let to_fetch: Vec<usize> = pending
        .iter()
        .enumerate()
        .filter(|(_, r)| {
            r.status == "pending_rpc_fetch" || (r.status == "failed" && r.retry_count < 5)
        })
        .map(|(i, _)| i)
        .collect();

    if to_fetch.is_empty() {
        return Ok(());
    }

    let mut cached = read_cache(cache_path)?;
    let mut errors = read_errors(errors_path)?;
    let mut live = read_live(live_path)?;
    let now = unix_now();

    // Process in chunks of 100
    for chunk_indices in to_fetch.chunks(100) {
        let pubkeys: Vec<Pubkey> = chunk_indices
            .iter()
            .filter_map(|&i| Pubkey::try_from(pending[i].pubkey.as_str()).ok())
            .collect();
        if pubkeys.is_empty() {
            continue;
        }

        // Rate-limit: 200ms between chunks
        std::thread::sleep(Duration::from_millis(200));

        match rpc.get_multiple_accounts(&pubkeys) {
            Ok(results) => {
                for (&idx, (pk, maybe_acct)) in
                    chunk_indices.iter().zip(pubkeys.iter().zip(results.iter()))
                {
                    let pk_str = pk.to_string();
                    match maybe_acct {
                        Some(acct) => {
                            let owner_str = acct.owner.to_string();
                            let (class, needs_live) =
                                classify(&owner_str, acct.data.len(), pending[idx].is_writable);

                            account_cache.insert_manual(
                                *pk,
                                Account {
                                    lamports: acct.lamports,
                                    data: acct.data.clone(),
                                    owner: Address::from(acct.owner.to_bytes()),
                                    executable: acct.executable,
                                    rent_epoch: acct.rent_epoch,
                                },
                            );

                            let data_b64 = base64::engine::general_purpose::STANDARD
                                .encode(&acct.data);
                            let seen_count = pending[idx].seen_count;

                            if let Some(c) = cached.iter_mut().find(|r| r.pubkey == pk_str) {
                                c.data_base64 = data_b64;
                                c.data_len = acct.data.len();
                                c.owner = owner_str.clone();
                                c.lamports = acct.lamports;
                                c.last_seen_unix = now;
                                c.seen_count = seen_count;
                                c.status = "valid".to_string();
                                c.classification = class.clone();
                                c.needs_live_subscription = needs_live;
                            } else {
                                cached.push(CachedRecord {
                                    pubkey: pk_str.clone(),
                                    owner: owner_str.clone(),
                                    lamports: acct.lamports,
                                    executable: acct.executable,
                                    rent_epoch: acct.rent_epoch,
                                    data_base64: data_b64,
                                    data_len: acct.data.len(),
                                    fetched_slot: 0,
                                    source: "auto_missing_account".to_string(),
                                    classification: class.clone(),
                                    is_writable_seen: pending[idx].is_writable,
                                    last_seen_unix: now,
                                    seen_count,
                                    status: "valid".to_string(),
                                    needs_live_subscription: needs_live,
                                });
                            }

                            if needs_live && !live.iter().any(|r| r.pubkey == pk_str) {
                                live.push(LiveRecord {
                                    pubkey: pk_str.clone(),
                                    owner: owner_str.clone(),
                                    classification: class.clone(),
                                    is_writable_seen: pending[idx].is_writable,
                                    first_seen_unix: now,
                                });
                            }

                            pending[idx].status = "cached".to_string();
                            pending[idx].last_error = None;

                            eprintln!(
                                "[missing_account_fetch_ok] pubkey={} owner={} data_len={} classification={} needs_live={}",
                                pk_str,
                                owner_str,
                                acct.data.len(),
                                class,
                                needs_live
                            );
                        }
                        None => {
                            pending[idx].retry_count += 1;
                            pending[idx].last_error = Some("rpc_null".to_string());
                            if pending[idx].retry_count >= 5 {
                                pending[idx].status = "failed".to_string();
                                eprintln!(
                                    "[missing_account_rpc_null] pubkey={} retry_count={} action=check_rpc_or_account_deleted",
                                    pk_str, pending[idx].retry_count
                                );
                            }
                            upsert_error(&mut errors, &pk_str, "rpc_null", now);
                        }
                    }
                }
            }
            Err(e) => {
                let msg = e.to_string();
                eprintln!("[missing_account_rpc_error] error={}", msg);
                for &idx in chunk_indices {
                    pending[idx].retry_count += 1;
                    pending[idx].last_error = Some(msg.clone());
                    upsert_error(&mut errors, &pending[idx].pubkey.clone(), &msg, now);
                }
            }
        }
    }

    // Warn about accounts that are cached but still appearing as missing
    for r in &pending {
        if r.status == "cached" && r.seen_count > 10 {
            eprintln!(
                "[important_missing_or_bad_account] pubkey={} seen_count={} status=cached_but_still_failing action=manual_review_required route_labels={} programs={}",
                r.pubkey, r.seen_count, r.route_labels, r.programs
            );
        }
    }

    write_pending(pending_path, &pending)?;
    write_cache(cache_path, &cached)?;
    write_errors(errors_path, &errors)?;
    write_live(live_path, &live)?;
    Ok(())
}

// ── Classification ────────────────────────────────────────────────────────────

fn classify(owner: &str, data_len: usize, is_writable: bool) -> (String, bool) {
    if (owner == TOKEN_PROGRAM_ID || owner == TOKEN_2022_PROGRAM_ID) && data_len == 165 {
        return ("token_account".to_string(), is_writable);
    }
    if (owner == TOKEN_PROGRAM_ID || owner == TOKEN_2022_PROGRAM_ID) && data_len == 82 {
        return ("mint".to_string(), false);
    }
    if DEX_OWNER_IDS.contains(&owner) {
        if is_writable {
            return ("dex_owned_live".to_string(), true);
        } else if data_len < 512 {
            return ("dex_owned_static_pda".to_string(), false);
        } else {
            return ("dex_owned_pool_config".to_string(), false);
        }
    }
    if owner == "11111111111111111111111111111111" {
        return ("system_account".to_string(), is_writable && data_len == 0);
    }
    if owner.starts_with("BPFLoader") {
        return ("program".to_string(), false);
    }
    ("unknown".to_string(), is_writable)
}

// ── File helpers ──────────────────────────────────────────────────────────────

fn read_pending(path: &Path) -> Result<Vec<PendingRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let s = std::fs::read_to_string(path)?;
    let f: PendingFile = serde_json::from_str(&s).unwrap_or(PendingFile {
        schema_version: PENDING_SCHEMA_VERSION,
        accounts: Vec::new(),
    });
    if f.schema_version != PENDING_SCHEMA_VERSION {
        return Ok(Vec::new());
    }
    Ok(f.accounts)
}

fn write_pending(path: &Path, records: &[PendingRecord]) -> Result<()> {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let f = PendingFile {
        schema_version: PENDING_SCHEMA_VERSION,
        accounts: records.to_vec(),
    };
    Ok(std::fs::write(path, serde_json::to_vec_pretty(&f)?)?)
}

fn read_cache(path: &Path) -> Result<Vec<CachedRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let s = std::fs::read_to_string(path)?;
    let f: CacheFile = serde_json::from_str(&s).unwrap_or(CacheFile {
        schema_version: CACHE_SCHEMA_VERSION,
        accounts: Vec::new(),
    });
    if f.schema_version != CACHE_SCHEMA_VERSION {
        return Ok(Vec::new());
    }
    Ok(f.accounts)
}

fn write_cache(path: &Path, records: &[CachedRecord]) -> Result<()> {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    let f = CacheFile {
        schema_version: CACHE_SCHEMA_VERSION,
        accounts: records.to_vec(),
    };
    Ok(std::fs::write(path, serde_json::to_vec_pretty(&f)?)?)
}

fn read_errors(path: &Path) -> Result<Vec<ErrorRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let s = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&s).unwrap_or_default())
}

fn write_errors(path: &Path, records: &[ErrorRecord]) -> Result<()> {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    Ok(std::fs::write(path, serde_json::to_vec_pretty(records)?)?)
}

fn read_live(path: &Path) -> Result<Vec<LiveRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let s = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&s).unwrap_or_default())
}

fn write_live(path: &Path, records: &[LiveRecord]) -> Result<()> {
    if let Some(p) = path.parent() {
        std::fs::create_dir_all(p)?;
    }
    Ok(std::fs::write(path, serde_json::to_vec_pretty(records)?)?)
}

fn upsert_error(records: &mut Vec<ErrorRecord>, pubkey: &str, error: &str, now: u64) {
    if let Some(r) = records.iter_mut().find(|r| r.pubkey == pubkey) {
        r.retry_count += 1;
        r.error = error.to_string();
        r.last_error_unix = now;
    } else {
        records.push(ErrorRecord {
            pubkey: pubkey.to_string(),
            error: error.to_string(),
            retry_count: 1,
            last_error_unix: now,
        });
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn parse_jsonish(v: &str) -> serde_json::Value {
    serde_json::from_str(v)
        .unwrap_or_else(|_| serde_json::Value::String(v.to_string()))
}

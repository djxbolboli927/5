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

use crate::account_cache::AccountCache;

const MANUAL_SIM_SCHEMA_VERSION: u32 = 1;
const MANUAL_CACHE_SCHEMA_VERSION: u32 = 1;
const DEFAULT_ALPHAQ_MISSING_ACCOUNT: &str = "2ny7eGyZCoeEVTkNLf5HcnJFBKkyA4p4gcrtb3b8y8ou";

#[derive(Clone, Debug)]
pub struct RuntimeMissingAccount {
    pub pubkey: Pubkey,
    pub route_sig: u128,
    pub route_labels: String,
    pub programs: String,
    pub source: String,
    pub is_signer: bool,
    pub is_writable: bool,
    pub created_by_setup: bool,
    pub from_cache: bool,
    pub reason: String,
}

#[derive(Serialize, Deserialize)]
struct ManualSimAccountsFile {
    schema_version: u32,
    accounts: Vec<ManualSimAccount>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ManualSimAccount {
    pubkey: String,
    role: String,
    source: String,
    mode: String,
    synthetic_allowed: bool,
    required: bool,
    notes: String,
}

#[derive(Serialize, Deserialize)]
struct ManualAccountCacheFile {
    schema_version: u32,
    accounts: Vec<ManualCachedAccount>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ManualCachedAccount {
    pubkey: String,
    owner: String,
    lamports: u64,
    executable: bool,
    rent_epoch: u64,
    data_base64: String,
    fetched_slot: Option<u64>,
    status: String,
}

#[derive(Serialize, Deserialize)]
struct ManualAccountError {
    pubkey: String,
    role: String,
    error: String,
    required: bool,
    action: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct MissingRuntimeRecord {
    pubkey: String,
    first_seen_unix: u64,
    route_sig: String,
    route_labels: serde_json::Value,
    programs: serde_json::Value,
    source: String,
    is_signer: bool,
    is_writable: bool,
    created_by_setup: bool,
    from_cache: bool,
    reason: String,
    suggested_action: String,
}

pub fn output_root_from_dex_dir(dex_dir: &str) -> PathBuf {
    let dex_path = Path::new(dex_dir);
    let parent = if dex_path.is_file() {
        dex_path.parent().unwrap_or_else(|| Path::new("."))
    } else {
        dex_path
    };

    if parent.file_name().and_then(|name| name.to_str()) == Some("1") {
        if let Some(metis_dir) = parent.parent() {
            if metis_dir.file_name().and_then(|name| name.to_str()) == Some("metis") {
                if let Some(root) = metis_dir.parent() {
                    return root.to_path_buf();
                }
            }
        }
    }

    parent.to_path_buf()
}

pub fn ensure_default_manual_sim_accounts(path: &Path) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create {}", parent.display()))?;
    }
    let file = ManualSimAccountsFile {
        schema_version: MANUAL_SIM_SCHEMA_VERSION,
        accounts: vec![ManualSimAccount {
            pubkey: DEFAULT_ALPHAQ_MISSING_ACCOUNT.to_string(),
            role: "alphaq_static_readonly".to_string(),
            source: "manual".to_string(),
            mode: "fetch_once_at_startup".to_string(),
            synthetic_allowed: false,
            required: true,
            notes: "Missing before LiteSVM execution in AlphaQ routes".to_string(),
        }],
    };
    std::fs::write(path, serde_json::to_vec_pretty(&file)?)
        .with_context(|| format!("cannot write {}", path.display()))?;
    eprintln!("[manual_accounts] created_default path={}", path.display());
    Ok(())
}

pub fn load_cached_accounts_into_cache(path: &Path, cache: &AccountCache) -> Result<usize> {
    if !path.exists() {
        eprintln!(
            "[manual_account_cache] status=missing path={} loaded=0",
            path.display()
        );
        return Ok(0);
    }

    let content =
        std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    let file: ManualAccountCacheFile = serde_json::from_str(&content)
        .with_context(|| format!("invalid {}", path.display()))?;
    if file.schema_version != MANUAL_CACHE_SCHEMA_VERSION {
        eprintln!(
            "[manual_account_cache] status=stale_schema path={} cached_schema={} expected_schema={} loaded=0",
            path.display(),
            file.schema_version,
            MANUAL_CACHE_SCHEMA_VERSION
        );
        return Ok(0);
    }

    let mut loaded = 0usize;
    for record in file.accounts {
        if record.status != "valid" {
            continue;
        }
        let Ok(pubkey) = Pubkey::try_from(record.pubkey.as_str()) else {
            continue;
        };
        let Ok(owner) = Pubkey::try_from(record.owner.as_str()) else {
            continue;
        };
        let Ok(data) = base64::engine::general_purpose::STANDARD.decode(record.data_base64.as_bytes())
        else {
            eprintln!(
                "[manual_account_cache] invalid_record pubkey={} reason=bad_base64",
                record.pubkey
            );
            continue;
        };
        cache.insert_manual(
            pubkey,
            Account {
                lamports: record.lamports,
                data,
                owner: Address::from(owner.to_bytes()),
                executable: record.executable,
                rent_epoch: record.rent_epoch,
            },
        );
        loaded += 1;
    }
    eprintln!(
        "[manual_account_cache] status=loaded path={} loaded={}",
        path.display(),
        loaded
    );
    Ok(loaded)
}

pub async fn fetch_and_cache_startup_accounts(
    manual_path: &Path,
    cache_path: &Path,
    errors_path: &Path,
    rpc: Arc<RpcClient>,
    cache: &AccountCache,
    requests_per_second: u64,
) -> Result<()> {
    ensure_default_manual_sim_accounts(manual_path)?;
    let manual = load_manual_sim_accounts(manual_path)?;
    let mut cached_records = load_cache_file(cache_path)?;

    let rate = requests_per_second.max(1).min(5);
    let delay = Duration::from_millis((1000 / rate).max(1));
    let mut errors = Vec::new();
    let mut fetched = 0usize;
    let mut skipped = 0usize;

    for account in manual.accounts {
        let Ok(pubkey) = Pubkey::try_from(account.pubkey.as_str()) else {
            errors.push(ManualAccountError {
                pubkey: account.pubkey,
                role: account.role,
                error: "bad_pubkey".to_string(),
                required: account.required,
                action: "fix_manual_sim_accounts".to_string(),
            });
            continue;
        };

        if account.mode == "ignore_builtin" || account.mode == "synthetic_system" {
            skipped += 1;
            continue;
        }
        if cache.get(&pubkey).is_some() {
            skipped += 1;
            continue;
        }

        eprintln!(
            "[rpc_fetch_reason] reason=manual_startup_account count=1 pubkeys_sample=[\"{}\"]",
            pubkey
        );
        let rpc_clone = rpc.clone();
        let result = tokio::task::spawn_blocking(move || rpc_clone.get_account(&pubkey)).await;
        match result {
            Ok(Ok(acct)) => {
                cache.insert_manual(
                    pubkey,
                    Account {
                        lamports: acct.lamports,
                        data: acct.data.clone(),
                        owner: Address::from(acct.owner.to_bytes()),
                        executable: acct.executable,
                        rent_epoch: acct.rent_epoch,
                    },
                );
                cached_records.push(ManualCachedAccount {
                    pubkey: pubkey.to_string(),
                    owner: acct.owner.to_string(),
                    lamports: acct.lamports,
                    executable: acct.executable,
                    rent_epoch: acct.rent_epoch,
                    data_base64: base64::engine::general_purpose::STANDARD.encode(&acct.data),
                    fetched_slot: None,
                    status: "valid".to_string(),
                });
                fetched += 1;
                write_cache_file(cache_path, &cached_records)?;
            }
            Ok(Err(e)) => {
                errors.push(ManualAccountError {
                    pubkey: pubkey.to_string(),
                    role: account.role,
                    error: classify_rpc_error(&e.to_string()).to_string(),
                    required: account.required,
                    action: if account.required {
                        "routes_requiring_this_account_will_drop".to_string()
                    } else {
                        "warning_only".to_string()
                    },
                });
            }
            Err(e) => {
                errors.push(ManualAccountError {
                    pubkey: pubkey.to_string(),
                    role: account.role,
                    error: format!("task_error:{e:?}"),
                    required: account.required,
                    action: if account.required {
                        "routes_requiring_this_account_will_drop".to_string()
                    } else {
                        "warning_only".to_string()
                    },
                });
            }
        }
        tokio::time::sleep(delay).await;
    }

    write_cache_file(cache_path, &cached_records)?;
    write_errors(errors_path, &errors)?;
    eprintln!(
        "[manual_accounts_ready] true manual={} cache={} fetched={} skipped={} errors={}",
        manual_path.display(),
        cache_path.display(),
        fetched,
        skipped,
        errors.len()
    );
    Ok(())
}

pub fn append_missing_runtime_account(root: &Path, missing: RuntimeMissingAccount) {
    if let Err(e) = append_missing_runtime_account_inner(root, missing) {
        eprintln!("[missing_from_mix_runtime] write_error error={}", e);
    }
}

fn append_missing_runtime_account_inner(
    root: &Path,
    missing: RuntimeMissingAccount,
) -> Result<()> {
    std::fs::create_dir_all(root).with_context(|| format!("cannot create {}", root.display()))?;
    let path = root.join("missing_from_mix_runtime.json");
    let mut records = if path.exists() {
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("cannot read {}", path.display()))?;
        serde_json::from_str::<Vec<MissingRuntimeRecord>>(&content).unwrap_or_default()
    } else {
        Vec::new()
    };

    let route_sig = format!("{:032x}", missing.route_sig);
    if records
        .iter()
        .any(|record| record.pubkey == missing.pubkey.to_string() && record.route_sig == route_sig)
    {
        return Ok(());
    }

    records.push(MissingRuntimeRecord {
        pubkey: missing.pubkey.to_string(),
        first_seen_unix: unix_now(),
        route_sig,
        route_labels: parse_jsonish_array(&missing.route_labels),
        programs: parse_jsonish_array(&missing.programs),
        source: missing.source,
        is_signer: missing.is_signer,
        is_writable: missing.is_writable,
        created_by_setup: missing.created_by_setup,
        from_cache: missing.from_cache,
        reason: missing.reason,
        suggested_action: "add_to_manual_sim_accounts".to_string(),
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&records)?)
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(())
}

fn load_manual_sim_accounts(path: &Path) -> Result<ManualSimAccountsFile> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    let file: ManualSimAccountsFile =
        serde_json::from_str(&content).with_context(|| format!("invalid {}", path.display()))?;
    if file.schema_version != MANUAL_SIM_SCHEMA_VERSION {
        anyhow::bail!(
            "manual_sim_accounts schema mismatch: got {} expected {}",
            file.schema_version,
            MANUAL_SIM_SCHEMA_VERSION
        );
    }
    Ok(file)
}

fn load_cache_file(path: &Path) -> Result<Vec<ManualCachedAccount>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content =
        std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))?;
    let file: ManualAccountCacheFile =
        serde_json::from_str(&content).with_context(|| format!("invalid {}", path.display()))?;
    if file.schema_version != MANUAL_CACHE_SCHEMA_VERSION {
        return Ok(Vec::new());
    }
    Ok(file.accounts)
}

fn write_cache_file(path: &Path, accounts: &[ManualCachedAccount]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create {}", parent.display()))?;
    }
    let mut accounts = accounts.to_vec();
    accounts.sort_by(|a, b| a.pubkey.cmp(&b.pubkey));
    accounts.dedup_by(|a, b| a.pubkey == b.pubkey);
    let file = ManualAccountCacheFile {
        schema_version: MANUAL_CACHE_SCHEMA_VERSION,
        accounts,
    };
    std::fs::write(path, serde_json::to_vec_pretty(&file)?)
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(())
}

fn write_errors(path: &Path, errors: &[ManualAccountError]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("cannot create {}", parent.display()))?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(errors)?)
        .with_context(|| format!("cannot write {}", path.display()))?;
    Ok(())
}

fn parse_jsonish_array(value: &str) -> serde_json::Value {
    serde_json::from_str(value)
        .unwrap_or_else(|_| serde_json::Value::Array(vec![serde_json::Value::String(value.into())]))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn classify_rpc_error(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("not found") {
        "not_found"
    } else if lower.contains("429") || lower.contains("too many requests") {
        "rate_limited"
    } else if lower.contains("timeout") {
        "timeout"
    } else {
        "rpc_error"
    }
}

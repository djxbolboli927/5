//! Pool account registry.
//!
//! Reads `<dex_dir>/<DEX_NAME>/<pool>.toml` files at startup and returns:
//!   - `all_accounts` — every static pubkey to pre-fetch via RPC so the sim
//!     cache is fully populated before the first trade
//!   - `subscribe_accounts` — vault accounts that change on every swap and
//!     must be subscribed individually on Yellowstone for live updates
//!     (the Yellowstone owner-filter already covers accounts owned by DEX
//!     programs; vaults are owned by SPL Token and need a direct subscription)
//!
//! Directory layout:
//!   dex_dir/
//!     Goonfi_V2/
//!       hype_usdc.toml
//!       wsol_usdc.toml
//!     SomeOtherDex/
//!       ...
//!
//! Files starting with `_` (e.g. `_template.toml`) are skipped.

use serde::Deserialize;
use solana_sdk::pubkey::Pubkey;
use std::path::Path;
use tracing::{info, warn};

#[derive(Deserialize)]
struct PoolFile {
    name: Option<String>,
    market: Option<String>,
    vault_a: Option<String>,
    vault_b: Option<String>,
    mint_a: Option<String>,
    mint_b: Option<String>,
    #[serde(default)]
    extra: Vec<String>,
}

pub struct DexPools {
    /// Every static account to pre-fetch via RPC at startup.
    pub all_accounts: Vec<Pubkey>,
    /// Vault accounts to subscribe for live Yellowstone updates.
    /// These change on every swap and must be kept fresh.
    pub subscribe_accounts: Vec<Pubkey>,
    /// Account groups fetched during startup. For mix.json each group should
    /// correspond to one pool so warm-up can obey a pools/sec rate limit.
    pub prefetch_groups: Vec<Vec<Pubkey>>,
}

/// Load all pool files from `dex_dir/<DEX>/<pool>.toml`.
/// Returns an empty `DexPools` if the directory does not exist.
pub fn load(dex_dir: &str) -> DexPools {
    let dex_path = Path::new(dex_dir);
    if !dex_path.exists() {
        return DexPools {
            all_accounts: vec![],
            subscribe_accounts: vec![],
            prefetch_groups: vec![],
        };
    }

    let mix_path = if dex_path.is_file()
        && dex_path.file_name().and_then(|n| n.to_str()) == Some("mix.json")
    {
        Some(dex_path.to_path_buf())
    } else {
        let candidate = dex_path.join("mix.json");
        candidate.exists().then_some(candidate)
    };
    if let Some(path) = mix_path {
        return load_mix_json(&path);
    }

    let mut all: Vec<Pubkey> = Vec::new();
    let mut subs: Vec<Pubkey> = Vec::new();
    let mut pool_count = 0usize;

    let dex_entries = match std::fs::read_dir(dex_path) {
        Ok(e) => e,
        Err(e) => {
            warn!(dex_dir, error = %e, "cannot read dex_dir");
            return DexPools {
                all_accounts: all,
                subscribe_accounts: subs,
                prefetch_groups: vec![],
            };
        }
    };

    let mut prefetch_groups: Vec<Vec<Pubkey>> = Vec::new();
    for dex_entry in dex_entries.flatten() {
        if !dex_entry.file_type().map_or(false, |t| t.is_dir()) {
            continue;
        }
        let dex_name = dex_entry.file_name();
        let pool_entries = match std::fs::read_dir(dex_entry.path()) {
            Ok(e) => e,
            Err(_) => continue,
        };

        for pool_entry in pool_entries.flatten() {
            let path = pool_entry.path();
            // Skip template files and non-TOML files
            let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if fname.starts_with('_') || path.extension().and_then(|e| e.to_str()) != Some("toml") {
                continue;
            }

            let content = match std::fs::read_to_string(&path) {
                Ok(s) => s,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "cannot read pool file");
                    continue;
                }
            };

            let pool: PoolFile = match toml::from_str(&content) {
                Ok(p) => p,
                Err(e) => {
                    warn!(path = %path.display(), error = %e, "invalid pool TOML");
                    continue;
                }
            };

            let pool_name = pool.name.as_deref().unwrap_or(fname);
            let mut parsed = 0usize;
            let mut group: Vec<Pubkey> = Vec::new();

            let mut push = |s: Option<&str>, is_vault: bool| {
                let s = match s { Some(s) => s, None => return };
                match Pubkey::try_from(s) {
                    Ok(pk) => {
                        all.push(pk);
                        group.push(pk);
                        if is_vault { subs.push(pk); }
                        parsed += 1;
                    }
                    Err(_) => warn!(
                        dex = ?dex_name, pool = pool_name, addr = s,
                        "invalid pubkey in pool file — skipped"
                    ),
                }
            };

            push(pool.market.as_deref(), false);
            push(pool.vault_a.as_deref(), true);   // vault → live subscription
            push(pool.vault_b.as_deref(), true);   // vault → live subscription
            push(pool.mint_a.as_deref(), false);
            push(pool.mint_b.as_deref(), false);
            for addr in &pool.extra {
                push(Some(addr.as_str()), false);
            }

            info!(
                dex = ?dex_name, pool = pool_name,
                accounts = parsed,
                "pool loaded"
            );
            group.sort_unstable();
            group.dedup();
            if !group.is_empty() {
                prefetch_groups.push(group);
            }
            pool_count += 1;
        }
    }

    // Deduplicate — mints and global protocol accounts appear across pools
    all.sort_unstable();
    all.dedup();
    subs.sort_unstable();
    subs.dedup();

    info!(
        pools = pool_count,
        prefetch = all.len(),
        live_subs = subs.len(),
        groups = prefetch_groups.len(),
        "dex pool registry loaded"
    );

    DexPools {
        all_accounts: all,
        subscribe_accounts: subs,
        prefetch_groups,
    }
}

fn load_mix_json(path: &Path) -> DexPools {
    let content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "cannot read mix.json");
            return DexPools {
                all_accounts: vec![],
                subscribe_accounts: vec![],
                prefetch_groups: vec![],
            };
        }
    };

    let json: serde_json::Value = match serde_json::from_str(&content) {
        Ok(json) => json,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "invalid mix.json");
            return DexPools {
                all_accounts: vec![],
                subscribe_accounts: vec![],
                prefetch_groups: vec![],
            };
        }
    };

    let mut all = Vec::new();
    collect_pubkeys_from_json(&json, &mut all);
    all.sort_unstable();
    all.dedup();
    let mut groups = collect_prefetch_groups_from_mix(&json);
    if groups.is_empty() {
        groups = all.chunks(100).map(|chunk| chunk.to_vec()).collect();
    }

    info!(
        mix = %path.display(),
        accounts_total = all.len(),
        groups = groups.len(),
        "sim account mix loaded"
    );

    DexPools {
        all_accounts: all.clone(),
        subscribe_accounts: all,
        prefetch_groups: groups,
    }
}

fn collect_pubkeys_from_json(value: &serde_json::Value, out: &mut Vec<Pubkey>) {
    match value {
        serde_json::Value::String(s) => {
            if let Ok(pk) = Pubkey::try_from(s.as_str()) {
                out.push(pk);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_pubkeys_from_json(item, out);
            }
        }
        serde_json::Value::Object(map) => {
            for value in map.values() {
                collect_pubkeys_from_json(value, out);
            }
        }
        _ => {}
    }
}

fn collect_prefetch_groups_from_mix(value: &serde_json::Value) -> Vec<Vec<Pubkey>> {
    let root = value
        .get("pools")
        .or_else(|| value.get("Pools"))
        .unwrap_or(value);
    collect_pool_like_groups(root)
}

fn collect_pool_like_groups(value: &serde_json::Value) -> Vec<Vec<Pubkey>> {
    let mut own = Vec::new();
    collect_pubkeys_from_json(value, &mut own);
    own.sort_unstable();
    own.dedup();

    let children: Vec<&serde_json::Value> = match value {
        serde_json::Value::Array(items) => items.iter().collect(),
        serde_json::Value::Object(map) => map.values().collect(),
        _ => vec![],
    };

    let mut nested = Vec::new();
    for child in children {
        if matches!(
            child,
            serde_json::Value::Array(_) | serde_json::Value::Object(_)
        ) {
            nested.extend(collect_pool_like_groups(child));
        }
    }

    if nested.len() > 4 {
        nested
    } else if !own.is_empty() {
        vec![own]
    } else {
        nested
    }
}

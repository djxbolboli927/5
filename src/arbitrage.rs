use anyhow::Result;
use futures::stream::{self, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::account_cache::AccountCache;
use crate::alt_cache::AltCache;
use crate::blockhash_cache::BlockhashCache;
use crate::config::{Config, TemplateCacheConfig};
use crate::jito::JitoClient;
use crate::jito_grpc::JitoGrpcClient;
use crate::litesvm_sim::SimulatorPool;
use crate::metis::{MetisClient, QuoteResponse, SwapInstructionsResponse};
use crate::metrics::Metrics;
use crate::program_registry::{FORBIDDEN_DEX_LABELS, FORBIDDEN_DEX_PROGRAM_IDS};
use crate::rate_limiter::RateLimiter;
use crate::template_cache::{self, TemplateStore};
use crate::token_metrics::TokenMetrics;
use crate::tokens::WSOL_MINT;
use crate::transaction;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
const JITO_TIP_LAMPORTS: u64 = 1_600;
const NETWORK_FEE_LAMPORTS: u64 = 5_000;
const ALPHAQ_PROGRAM_ID_STR: &str = "ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA";
const SOLFI_PROGRAM_ID_STR: &str = "SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe";
const SOLFI_V2_PROGRAM_ID_STR: &str = "SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF";
const TESSERA_PROGRAM_ID_STR: &str = "TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH";
const GOONFI_V2_PROGRAM_ID_STR: &str = "goonuddtQRrWqqn5nFyczVKaie28f3kDkHWkHtURSLE";
const ZEROFI_PROGRAM_ID_STR: &str = "ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY";
const PANCAKESWAP_PROGRAM_ID_STR: &str = "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq";
const BYREAL_CLMM_PROGRAM_ID_STR: &str = "REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2";
const WHIRLPOOL_PROGRAM_ID_STR: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";

const RATE_RETRY_BACKOFF_MS: u64 = 20;

// ─── Route helpers ───────────────────────────────────────────────────────────

fn route_uses_forbidden_dex(quote: &QuoteResponse) -> bool {
    let arr = match quote.route_plan.as_array() {
        Some(a) => a,
        None => return false,
    };
    for hop in arr {
        let swap_info = match hop.get("swapInfo") {
            Some(s) => s,
            None => continue,
        };
        if let Some(label) = swap_info.get("label").and_then(|v| v.as_str()) {
            for banned in FORBIDDEN_DEX_LABELS {
                if label.eq_ignore_ascii_case(banned)
                    || label.to_ascii_lowercase().contains(&banned.to_ascii_lowercase())
                {
                    return true;
                }
            }
        }
        if let Some(obj) = swap_info.as_object() {
            for v in obj.values() {
                if let Some(s) = v.as_str() {
                    if FORBIDDEN_DEX_PROGRAM_IDS.iter().any(|p| *p == s) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn route_plan_mentions_alphaq(route_plan: &serde_json::Value) -> bool {
    route_plan_contains_string(route_plan, ALPHAQ_PROGRAM_ID_STR)
        || route_plan
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|hop| hop.get("swapInfo"))
            .filter_map(|swap_info| swap_info.get("label"))
            .filter_map(|label| label.as_str())
            .any(|label| {
                let label = label.to_ascii_lowercase();
                label.contains("alphaq") || label.contains("alpha_q")
            })
}

fn route_plan_mentions_opaque_dex(route_plan: &serde_json::Value) -> bool {
    route_plan_contains_string(route_plan, SOLFI_V2_PROGRAM_ID_STR)
        || route_plan_contains_string(route_plan, SOLFI_PROGRAM_ID_STR)
        || route_plan_contains_string(route_plan, ALPHAQ_PROGRAM_ID_STR)
        || route_plan_contains_string(route_plan, TESSERA_PROGRAM_ID_STR)
        || route_plan_contains_string(route_plan, GOONFI_V2_PROGRAM_ID_STR)
        || route_plan_contains_string(route_plan, ZEROFI_PROGRAM_ID_STR)
        || route_plan_contains_string(route_plan, PANCAKESWAP_PROGRAM_ID_STR)
        || route_plan_contains_string(route_plan, BYREAL_CLMM_PROGRAM_ID_STR)
        || route_labels(route_plan)
            .iter()
            .any(|label| opaque_dex_label(label))
}

fn route_plan_contains_string(value: &serde_json::Value, needle: &str) -> bool {
    match value {
        serde_json::Value::String(s) => s == needle,
        serde_json::Value::Array(arr) => arr
            .iter()
            .any(|value| route_plan_contains_string(value, needle)),
        serde_json::Value::Object(obj) => obj
            .values()
            .any(|value| route_plan_contains_string(value, needle)),
        _ => false,
    }
}

fn route_labels_summary(route_plan: &serde_json::Value) -> String {
    let labels = route_labels(route_plan);
    serde_json::to_string(&labels).unwrap_or_else(|_| "[]".to_string())
}

fn route_group_key(
    route_plan: &serde_json::Value,
    route_sig: u128,
    hop_count: usize,
    only_direct: bool,
) -> String {
    let hops = route_plan
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|hop| hop.get("swapInfo").and_then(|swap_info| swap_info.as_object()))
        .map(|swap_info| {
            serde_json::json!({
                "label": swap_info.get("label").cloned().unwrap_or(serde_json::Value::Null),
                "ammKey": swap_info.get("ammKey").cloned().unwrap_or(serde_json::Value::Null),
                "inputMint": swap_info.get("inputMint").cloned().unwrap_or(serde_json::Value::Null),
                "outputMint": swap_info.get("outputMint").cloned().unwrap_or(serde_json::Value::Null),
                "direction": swap_info
                    .get("aToB")
                    .or_else(|| swap_info.get("a_to_b"))
                    .or_else(|| swap_info.get("direction"))
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "route_sig": format!("{route_sig:032x}"),
        "hop_count": hop_count,
        "only_direct": only_direct,
        "hops": hops,
    })
    .to_string()
}

fn route_labels(route_plan: &serde_json::Value) -> Vec<String> {
    route_plan
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|hop| hop.get("swapInfo"))
        .filter_map(|swap_info| swap_info.get("label"))
        .filter_map(|label| label.as_str())
        .map(str::to_string)
        .collect::<Vec<_>>()
}

fn route_programs_summary(route_plan: &serde_json::Value) -> String {
    let mut programs = crate::program_registry::PROGRAMS
        .iter()
        .filter_map(|(program_id, _)| {
            route_plan_contains_string(route_plan, program_id).then(|| (*program_id).to_string())
        })
        .collect::<Vec<_>>();

    for label in route_labels(route_plan) {
        for program in program_ids_from_route_label(&label) {
            if programs.iter().all(|existing| existing != program) {
                programs.push(program.to_string());
            }
        }
    }
    programs.sort();
    programs.dedup();

    serde_json::to_string(&programs).unwrap_or_else(|_| "[]".to_string())
}

fn program_ids_from_route_label(label: &str) -> &'static [&'static str] {
    let l = label.to_ascii_lowercase();
    if l.contains("solfi") && (l.contains("v2") || l.contains("v 2")) {
        return &[SOLFI_V2_PROGRAM_ID_STR];
    }
    if l.contains("solfi") {
        return &[SOLFI_PROGRAM_ID_STR];
    }
    if l.contains("alphaq") || l.contains("alpha_q") {
        return &[ALPHAQ_PROGRAM_ID_STR];
    }
    if l.contains("whirlpool") || l.contains("orca") {
        return &[WHIRLPOOL_PROGRAM_ID_STR];
    }
    if l.contains("tessera") {
        return &[TESSERA_PROGRAM_ID_STR];
    }
    if l.contains("goonfi") || l.contains("goon fi") {
        return &[GOONFI_V2_PROGRAM_ID_STR];
    }
    if l.contains("zerofi") || l.contains("zero fi") {
        return &[ZEROFI_PROGRAM_ID_STR];
    }
    if l.contains("pancake") {
        return &[PANCAKESWAP_PROGRAM_ID_STR];
    }
    if l.contains("byreal") {
        return &[BYREAL_CLMM_PROGRAM_ID_STR];
    }
    &[]
}

fn opaque_dex_label(label: &str) -> bool {
    !program_ids_from_route_label(label).is_empty()
        && {
            let l = label.to_ascii_lowercase();
            l.contains("solfi")
                || l.contains("alphaq")
                || l.contains("alpha_q")
                || l.contains("tessera")
                || l.contains("goonfi")
                || l.contains("goon fi")
                || l.contains("zerofi")
                || l.contains("zero fi")
                || l.contains("pancake")
                || l.contains("byreal")
        }
}

fn merge_program_json(existing_json: &str, extra_programs: &[String]) -> String {
    let mut programs = serde_json::from_str::<Vec<String>>(existing_json).unwrap_or_default();
    programs.extend(extra_programs.iter().cloned());
    programs.sort();
    programs.dedup();
    serde_json::to_string(&programs).unwrap_or_else(|_| existing_json.to_string())
}

/// Returns true if quote1 and quote2 share at least one pool (ammKey).
/// A round-trip through the same pool always loses money (pays fee twice).
fn routes_share_pool(q1: &QuoteResponse, q2: &QuoteResponse) -> bool {
    let keys1: Vec<&str> = q1
        .route_plan
        .as_array()
        .map(|hops| {
            hops.iter()
                .filter_map(|h| h.get("swapInfo")?.get("ammKey")?.as_str())
                .collect()
        })
        .unwrap_or_default();
    if keys1.is_empty() {
        return false;
    }
    let arr2 = match q2.route_plan.as_array() {
        Some(a) => a,
        None => return false,
    };
    for hop in arr2 {
        if let Some(key) = hop.get("swapInfo").and_then(|s| s.get("ammKey")).and_then(|v| v.as_str()) {
            if keys1.contains(&key) {
                return true;
            }
        }
    }
    false
}

fn lookup_cu_limit(hop_count: usize, cu_limits: &[u32]) -> u32 {
    if cu_limits.is_empty() {
        return 200_000;
    }
    let index = hop_count.saturating_sub(2);
    cu_limits[index.min(cu_limits.len() - 1)]
}

#[derive(Clone, Copy, Debug)]
enum InstructionSource {
    RouteTemplate,
    HopTemplate,
    FreshMetis,
}

impl PartialEq for InstructionSource {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for InstructionSource {}

impl InstructionSource {
    fn as_str(self) -> &'static str {
        match self {
            InstructionSource::RouteTemplate => "route_template",
            InstructionSource::HopTemplate => "hop_template",
            InstructionSource::FreshMetis => "fresh_metis",
        }
    }
}

// ─── Stage 1 output ──────────────────────────────────────────────────────────

struct QuotePair {
    token_mint: String,
    amount: u64,
    output_wsol: u64,
    net_profit: i64,
    quote1: QuoteResponse,
    quote2: QuoteResponse,
    hop_count: usize,
    only_direct: bool,
}

// ─── LIFO queue item ──────────────────────────────────────────────────────────

struct ReadyInstruction {
    swap_ixs: SwapInstructionsResponse,
    hop_count: usize,
    min_wsol_gain: u64,
    should_simulate: bool,
    ix_source: InstructionSource,
    route_sig: u128,
    route_labels: String,
    route_programs: String,
    route_has_alphaq: bool,
    arrived_at: Instant,
    waited_for_slot: bool,
}

#[derive(Clone)]
struct SwapCandidate {
    merged: QuoteResponse,
    amount: u64,
    output_wsol: u64,
    net_profit_estimate: i64,
    hop_count: usize,
    only_direct: bool,
    route_sig: u128,
    route_key: String,
    route_labels: String,
    route_programs: String,
    route_has_alphaq: bool,
    context_slot: Option<u64>,
    min_wsol_gain: u64,
    should_simulate: bool,
    save_new: bool,
    fresh_metis_reason: String,
    created_at: Instant,
}

struct InflightSwapIx {
    current_profit: i64,
    pending: Option<SwapCandidate>,
}

pub struct SwapIxState {
    inflight: Mutex<HashMap<u128, InflightSwapIx>>,
    budget: Arc<Semaphore>,
    max_inflight: usize,
}

enum SwapIxStart {
    Start(OwnedSemaphorePermit),
    DroppedInflight,
    DroppedBudget { inflight: usize },
}

enum SwapIxDispatchOutcome {
    Sent,
    DroppedInflight,
    DroppedBudget,
}

impl SwapIxState {
    pub fn new(max_inflight: usize) -> Self {
        let max_inflight = max_inflight.max(1);
        Self {
            inflight: Mutex::new(HashMap::new()),
            budget: Arc::new(Semaphore::new(max_inflight)),
            max_inflight,
        }
    }

    fn try_start(&self, candidate: &SwapCandidate) -> SwapIxStart {
        let mut inflight = self.inflight.lock().unwrap();
        if let Some(existing) = inflight.get_mut(&candidate.route_sig) {
            if candidate.net_profit_estimate > existing.current_profit {
                let replace_pending = existing
                    .pending
                    .as_ref()
                    .map(|pending| candidate.net_profit_estimate > pending.net_profit_estimate)
                    .unwrap_or(true);
                if replace_pending {
                    existing.pending = Some(candidate.clone());
                }
            }
            return SwapIxStart::DroppedInflight;
        }

        match self.budget.clone().try_acquire_owned() {
            Ok(permit) => {
                inflight.insert(
                    candidate.route_sig,
                    InflightSwapIx {
                        current_profit: candidate.net_profit_estimate,
                        pending: None,
                    },
                );
                SwapIxStart::Start(permit)
            }
            Err(_) => SwapIxStart::DroppedBudget {
                inflight: inflight.len(),
            },
        }
    }

    fn complete(&self, route_sig: u128) -> Option<SwapCandidate> {
        let mut inflight = self.inflight.lock().unwrap();
        let Some(mut existing) = inflight.remove(&route_sig) else {
            return None;
        };
        if let Some(pending) = existing.pending.take() {
            return Some(pending);
        }
        None
    }

    fn max_inflight(&self) -> usize {
        self.max_inflight
    }

    fn inflight_len(&self) -> usize {
        self.inflight.lock().unwrap().len()
    }
}

// ─── Shared context ───────────────────────────────────────────────────────────

pub struct CalcCtx {
    pub metis: Arc<MetisClient>,
    pub blockhash_cache: Arc<BlockhashCache>,
    pub trading_keypair: Arc<Keypair>,
    pub rpc_client: Arc<RpcClient>,
    pub alt_cache: AltCache,
    pub jito: Arc<JitoClient>,
    pub jito_grpc: Option<Arc<JitoGrpcClient>>,
    pub jito_limiter: Arc<Mutex<RateLimiter>>,
    pub jito_grpc_limiter: Option<Arc<Mutex<RateLimiter>>>,
    pub cu_limits: Vec<u32>,
    pub user_pubkey: String,
    #[allow(dead_code)]
    pub sim_cache: Option<Arc<AccountCache>>,
    #[allow(dead_code)]
    pub sim_pool: Option<Arc<SimulatorPool>>,
    pub mix_registry: Option<Arc<crate::mix_registry::VerifiedMixRegistry>>,
    pub template_store: Arc<TemplateStore>,
    pub template_config: TemplateCacheConfig,
    pub swap_ix_state: Arc<SwapIxState>,
}

// ─── Pipeline handle ──────────────────────────────────────────────────────────

pub struct Pipeline {
    lifo: Arc<Mutex<Vec<ReadyInstruction>>>,
    lifo_sem: Arc<tokio::sync::Semaphore>,
}

// ─── Stage 1: Quote scanner ───────────────────────────────────────────────────

/// One quote check for a single route mode (free or direct-only).
/// Each call makes 2 sequential HTTP requests (q1 then q2).
/// Free and direct entries are separate items in all_pairs so they never
/// block each other — a direct-route timeout does not delay the free-route
/// check for the same token.
///
/// Both free (only_direct=false) and direct (only_direct=true) routes are
/// scanned for every token.  Free routes may return multi-hop paths
/// (hop_count > 2) which are also sent to /swap-instructions when profitable.
/// Direct routes should always be 2-hop (1 hop per leg).
async fn quote_check(
    metis: &MetisClient,
    token_mint: &str,
    amount: u64,
    only_direct: bool,
    min_profit_lamports: u64,
    metrics: &Metrics,
    token_metrics: &TokenMetrics,
) -> Option<QuotePair> {
    metrics.metis_req_sent.fetch_add(2, Ordering::Relaxed);
    let ts = token_metrics.get(token_mint);
    if let Some(ts) = ts {
        ts.q_sent.fetch_add(2, Ordering::Relaxed);
    }

    let quote1 = match metis.get_quote(WSOL_MINT, token_mint, amount, only_direct).await {
        Ok(q) => q,
        Err(_) => {
            if let Some(ts) = ts {
                ts.route_fail.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
    };

    let token_amount: u64 = match quote1.out_amount.parse::<u64>().ok().filter(|&v| v > 0) {
        Some(v) => v,
        None => {
            if let Some(ts) = ts {
                ts.route_fail.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
    };

    let quote2 = match metis.get_quote(token_mint, WSOL_MINT, token_amount, only_direct).await {
        Ok(q) => q,
        Err(_) => {
            if let Some(ts) = ts {
                ts.route_fail.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
    };

    let output_wsol: u64 = quote2.out_amount.parse().unwrap_or(0);
    metrics.metis_resp_total.fetch_add(1, Ordering::Relaxed);
    if let Some(ts) = ts {
        ts.route_ok.fetch_add(1, Ordering::Relaxed);
    }

    // Stage-1 requires the configured profit plus Jito/network costs and the
    // Metis latency margin before a route can request /swap-instructions.
    let stage1_threshold = amount.saturating_add(min_profit_lamports);
    if output_wsol <= stage1_threshold {
        if let Some(ts) = ts {
            ts.not_profitable.fetch_add(1, Ordering::Relaxed);
        }
        return None;
    }

    if route_uses_forbidden_dex(&quote1) || route_uses_forbidden_dex(&quote2) {
        if let Some(ts) = ts {
            ts.not_profitable.fetch_add(1, Ordering::Relaxed);
        }
        return None;
    }

    // Same-pool guard: round-tripping through the same AMM pool pays the fee
    // twice and always loses. These look profitable only due to Metis's
    // optimistic slippage=0 simulation; they revert on-chain every time.
    if routes_share_pool(&quote1, &quote2) {
        metrics.dropped_same_pool.fetch_add(1, Ordering::Relaxed);
        if let Some(ts) = ts {
            ts.not_profitable.fetch_add(1, Ordering::Relaxed);
        }
        return None;
    }

    if let Some(ts) = ts {
        ts.profitable.fetch_add(1, Ordering::Relaxed);
    }

    let hop_count = {
        let n1 = quote1.route_plan.as_array().map(|a| a.len()).unwrap_or(1);
        let n2 = quote2.route_plan.as_array().map(|a| a.len()).unwrap_or(1);
        n1 + n2
    };

    metrics.metis_resp_ok.fetch_add(1, Ordering::Relaxed);

    let on_chain_floor = amount
        .saturating_add(JITO_TIP_LAMPORTS)
        .saturating_add(NETWORK_FEE_LAMPORTS);
    let net_profit = output_wsol as i64 - on_chain_floor as i64;
    Some(QuotePair {
        token_mint: token_mint.to_string(),
        amount,
        output_wsol,
        net_profit,
        quote1,
        quote2,
        hop_count,
        only_direct,
    })
}

// ─── Worker pool ──────────────────────────────────────────────────────────────

pub fn spawn_workers(
    ctx: Arc<CalcCtx>,
    metrics: Arc<Metrics>,
    worker_count: usize,
    queue_max_age_ms: u64,
) -> Pipeline {
    let lifo: Arc<Mutex<Vec<ReadyInstruction>>> = Arc::new(Mutex::new(Vec::new()));
    let lifo_sem = Arc::new(tokio::sync::Semaphore::new(0));

    for _ in 0..worker_count {
        let lifo_c = lifo.clone();
        let sem_c = lifo_sem.clone();
        let ctx_c = ctx.clone();
        let met_c = metrics.clone();
        tokio::spawn(async move {
            loop {
                sem_c.acquire().await.unwrap().forget();

                let item = lifo_c.lock().unwrap().pop();
                let mut item = match item {
                    Some(i) => {
                        met_c.queue_depth.fetch_sub(1, Ordering::Relaxed);
                        i
                    }
                    None => continue,
                };

                if item.arrived_at.elapsed().as_millis() as u64 > queue_max_age_ms {
                    met_c.dropped_stale.fetch_add(1, Ordering::Relaxed);
                    met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                let cu_limit = lookup_cu_limit(item.hop_count, &ctx_c.cu_limits);
                let recent_blockhash = ctx_c.blockhash_cache.get();
                let keypair = ctx_c.trading_keypair.clone();
                let alt = ctx_c.alt_cache.clone();
                let rpc = ctx_c.rpc_client.clone();
                let min_wsol_gain = item.min_wsol_gain;
                let should_simulate = item.should_simulate;
                let ix_source = item.ix_source;
                let route_sig = item.route_sig;
                let route_labels = item.route_labels.clone();
                let route_programs = item.route_programs.clone();
                let route_has_alphaq = item.route_has_alphaq;
                let alt_addresses = item.swap_ixs.address_lookup_table_addresses.clone();
                let swap_ixs = item.swap_ixs.clone();

                if ctx_c.template_config.force_fresh_metis_all
                    && ix_source != InstructionSource::FreshMetis
                {
                    eprintln!(
                        "[drop_non_metis_instruction] route_sig={:032x} source={} reason=force_fresh_metis_all",
                        route_sig,
                        ix_source.as_str()
                    );
                    met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                let tx = match tokio::task::spawn_blocking(move || {
                    transaction::build_arb_transaction(
                        &swap_ixs,
                        &keypair,
                        JITO_TIP_LAMPORTS,
                        cu_limit,
                        recent_blockhash,
                        &alt,
                        &rpc,
                    )
                })
                .await
                {
                    Ok(Ok(tx)) => tx,
                    _ => {
                        met_c.tx_build_failed.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };

                // Solana hard limit: a tx may lock at most 64 distinct accounts
                // (static keys + every ALT-loaded account). Multi-hop circular
                // routes routinely exceed this; the block engine rejects them
                // with "too many account locks". Drop them here so we don't
                // waste a Jito rate-limit slot on a guaranteed 400.
                if transaction::account_lock_count(&tx) > 64 {
                    met_c.dropped_account_locks.fetch_add(1, Ordering::Relaxed);
                    met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                match bincode::serialize(&tx) {
                    Ok(bytes) if bytes.len() > 1232 => {
                        met_c.tx_too_large.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Err(_) => {
                        met_c.tx_build_failed.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    Ok(_) => {}
                }

                if should_simulate {
                    eprintln!(
                        "[sim_candidate] source={} route_sig={:032x} hop_count={} route_labels={} programs={} alphaq={}",
                        ix_source.as_str(),
                        route_sig,
                        item.hop_count,
                        route_labels,
                        route_programs,
                        route_has_alphaq
                    );
                    let sim_cache = match &ctx_c.sim_cache {
                        Some(cache) => cache.clone(),
                        None => {
                            met_c.sim_failed.fetch_add(1, Ordering::Relaxed);
                            met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    let sim_pool = match &ctx_c.sim_pool {
                        Some(pool) => pool.clone(),
                        None => {
                            met_c.sim_failed.fetch_add(1, Ordering::Relaxed);
                            met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    };
                    let alt_cache = ctx_c.alt_cache.clone();
                    let rpc = ctx_c.rpc_client.clone();
                    let tx_for_sim = tx.clone();
                    let met_for_sim = met_c.clone();
                    let mix_registry = ctx_c.mix_registry.clone();
                    let route_labels_for_mix = route_labels.clone();
                    let mut route_programs_for_mix = route_programs.clone();
                    let route_sig_for_mix = route_sig;

                    let sim_result = tokio::task::spawn_blocking(move || {
                        let alts = crate::litesvm_sim::resolve_alts(
                            &alt_addresses,
                            &alt_cache,
                            &rpc,
                        )?;
                        let tx_programs = crate::litesvm_sim::registered_programs_mentioned_in_tx(
                            &tx_for_sim,
                            &alts,
                        );
                        route_programs_for_mix =
                            merge_program_json(&route_programs_for_mix, &tx_programs);
                        if let Some(registry) = &mix_registry {
                            let tx_accounts =
                                crate::litesvm_sim::tx_account_keys_for_mix_gate(&tx_for_sim, &alts);
                            let data_hash_only_accounts =
                                registry.data_hash_only_tx_accounts(&tx_accounts);
                            if !data_hash_only_accounts.is_empty() {
                                met_for_sim.sim_mix_gate_data_hash_only_drop.fetch_add(
                                    data_hash_only_accounts.len() as u64,
                                    Ordering::Relaxed,
                                );
                                eprintln!(
                                    "[mix_gate_data_hash_only_ignore] route_sig={:032x} labels={} programs={} accounts={} action=simulate reason=stale_state_not_invalid_pool",
                                    route_sig_for_mix,
                                    route_labels_for_mix,
                                    route_programs_for_mix,
                                    crate::mix_registry::pubkeys_json(&data_hash_only_accounts)
                                );
                            }
                            if let Some(report) = registry.check_tx_accounts(&tx_accounts) {
                                if report.is_unverified_only() {
                                    met_for_sim
                                        .dropped_unverified_mix_route
                                        .fetch_add(1, Ordering::Relaxed);
                                } else {
                                    met_for_sim
                                        .dropped_invalid_mix_route
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                eprintln!(
                                    "[drop_invalid_mix_route] route_sig={:032x} reason={} labels={} programs={} amm_keys={} invalid_pools={} unverified_pools={} invalid_accounts={} unverified_accounts={}",
                                    route_sig_for_mix,
                                    report.reason(),
                                    route_labels_for_mix,
                                    route_programs_for_mix,
                                    crate::mix_registry::pubkeys_json(&report.amm_keys),
                                    crate::mix_registry::pubkeys_json(&report.invalid_pools),
                                    crate::mix_registry::pubkeys_json(&report.unverified_pools),
                                    crate::mix_registry::pubkeys_json(&report.invalid_accounts),
                                    crate::mix_registry::pubkeys_json(&report.unverified_accounts)
                                );
                                let sim = sim_pool.acquire();
                                sim.retry_mix_gate_drop_with_rpc_snapshot(
                                    &sim_cache,
                                    &tx_for_sim,
                                    &alts,
                                    min_wsol_gain,
                                    met_for_sim.as_ref(),
                                    route_sig_for_mix,
                                    &route_labels_for_mix,
                                    &route_programs_for_mix,
                                    ix_source.as_str(),
                                    report.reason(),
                                );
                                anyhow::bail!("mix gate drop before simulation: {}", report.reason());
                            }
                        }
                        let sim = sim_pool.acquire();
                        sim.simulate(
                            &tx_for_sim,
                            &alts,
                            &alt_cache,
                            &sim_cache,
                            min_wsol_gain,
                            met_for_sim.as_ref(),
                            route_sig_for_mix,
                            &route_labels_for_mix,
                            &route_programs_for_mix,
                            ix_source.as_str(),
                        )
                    })
                    .await;

                    match sim_result {
                        Ok(Ok(outcome)) => {
                            met_c.sim_ok.fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                cu = outcome.compute_units,
                                wsol_before = outcome.wsol_before,
                                wsol_after = outcome.wsol_after,
                                "local simulation passed"
                            );
                        }
                        Ok(Err(e)) => {
                            met_c.sim_failed.fetch_add(1, Ordering::Relaxed);
                            met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                            eprintln!("[sim_failed] error={e}");
                            tracing::warn!(error = %e, "local simulation failed");
                            continue;
                        }
                        Err(e) => {
                            met_c.sim_failed.fetch_add(1, Ordering::Relaxed);
                            met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                            eprintln!("[sim_failed] task_error={e:?}");
                            tracing::warn!(error = ?e, "local simulation task failed");
                            continue;
                        }
                    }
                }

                met_c.calc_done.fetch_add(1, Ordering::Relaxed);

                let use_grpc = if ctx_c.jito_limiter.lock().unwrap().try_acquire() {
                    false
                } else if ctx_c
                    .jito_grpc_limiter
                    .as_ref()
                    .map(|gl| gl.lock().unwrap().try_acquire())
                    .unwrap_or(false)
                {
                    true
                } else {
                    if !item.waited_for_slot {
                        item.waited_for_slot = true;
                        met_c.rate_requeued.fetch_add(1, Ordering::Relaxed);
                    }
                    lifo_c.lock().unwrap().push(item);
                    met_c.queue_depth.fetch_add(1, Ordering::Relaxed);
                    sem_c.add_permits(1);
                    tokio::time::sleep(std::time::Duration::from_millis(RATE_RETRY_BACKOFF_MS))
                        .await;
                    continue;
                };

                let result = if use_grpc {
                    match &ctx_c.jito_grpc {
                        Some(grpc) => grpc.send_bundle(&tx).await,
                        None => ctx_c.jito.send_bundle(&tx).await,
                    }
                } else {
                    ctx_c.jito.send_bundle(&tx).await
                };

                match result {
                    Ok(_) => {
                        met_c.jito_sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        met_c.jito_send_failed.fetch_add(1, Ordering::Relaxed);
                        met_c.tx_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    Pipeline { lifo, lifo_sem }
}

// ─── Queue helper ─────────────────────────────────────────────────────────────

fn push_to_queue(
    swap_ixs: SwapInstructionsResponse,
    hop_count: usize,
    min_wsol_gain: u64,
    should_simulate: bool,
    ix_source: InstructionSource,
    route_sig: u128,
    route_labels: String,
    route_programs: String,
    route_has_alphaq: bool,
    pipeline: &Pipeline,
    metrics: &Metrics,
) {
    let item = ReadyInstruction {
        swap_ixs,
        hop_count,
        min_wsol_gain,
        should_simulate,
        ix_source,
        route_sig,
        route_labels: route_labels.clone(),
        route_programs: route_programs.clone(),
        route_has_alphaq,
        arrived_at: Instant::now(),
        waited_for_slot: false,
    };
    pipeline.lifo.lock().unwrap().push(item);
    metrics.queue_in.fetch_add(1, Ordering::Relaxed);
    let queue_depth = metrics.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
    eprintln!(
        "[queue_push] source={} route_sig={:032x} route_labels={} programs={} queue_depth={}",
        ix_source.as_str(),
        route_sig,
        route_labels,
        route_programs,
        queue_depth
    );
    pipeline.lifo_sem.add_permits(1);
}

fn flush_candidate_batch(
    candidates: &mut Vec<SwapCandidate>,
    config: &Config,
    ctx: &Arc<CalcCtx>,
    pipeline: &Pipeline,
    metrics: &Arc<Metrics>,
) {
    if candidates.is_empty() {
        return;
    }

    let window_ms = config.performance.candidate_batch_window_ms.max(1);
    let candidates_in = candidates.len();
    let top_per_route = config.performance.candidate_top_per_route.max(1);
    let global_top_n = config.performance.candidate_global_top_n.max(1);

    let mut groups: HashMap<String, Vec<SwapCandidate>> = HashMap::new();
    for candidate in candidates.drain(..) {
        groups
            .entry(candidate.route_key.clone())
            .or_default()
            .push(candidate);
    }
    let groups_len = groups.len();

    let mut selected = Vec::new();
    let mut dropped_by_rank = 0usize;
    let mut coalesced = 0usize;
    for (_route_key, mut group) in groups {
        group.sort_by(|a, b| b.net_profit_estimate.cmp(&a.net_profit_estimate));
        let group_len = group.len();
        let kept = group_len.min(top_per_route);
        let route_sig = group
            .first()
            .map(|candidate| candidate.route_sig)
            .unwrap_or(0);
        let best_profit = group
            .first()
            .map(|candidate| candidate.net_profit_estimate)
            .unwrap_or(0);
        let worst_dropped_profit = group
            .get(kept)
            .map(|candidate| candidate.net_profit_estimate)
            .unwrap_or(0);
        let labels = group
            .first()
            .map(|candidate| candidate.route_labels.as_str())
            .unwrap_or("[]");
        eprintln!(
            "[route_coalesce] route_sig={:032x} labels={} candidates={} kept={} best_profit={} worst_dropped_profit={}",
            route_sig, labels, group_len, kept, best_profit, worst_dropped_profit
        );
        if group_len > kept {
            let dropped = group_len - kept;
            coalesced += dropped;
            dropped_by_rank += dropped;
            group.truncate(kept);
        }
        selected.extend(group);
    }

    selected.sort_by(|a, b| b.net_profit_estimate.cmp(&a.net_profit_estimate));
    if selected.len() > global_top_n {
        dropped_by_rank += selected.len() - global_top_n;
        selected.truncate(global_top_n);
    }

    metrics
        .candidate_coalesced_total
        .fetch_add(coalesced as u64, Ordering::Relaxed);
    metrics
        .candidate_dropped_rank_total
        .fetch_add(dropped_by_rank as u64, Ordering::Relaxed);

    let timeout_ms = config.performance.swap_instructions_timeout_ms;
    let selected_global = selected.len();
    let mut sent_to_metis = 0usize;
    let mut dropped_by_inflight = 0usize;
    let mut dropped_by_budget = 0usize;
    for candidate in selected {
        match spawn_fresh_metis_candidate(
            candidate,
            ctx.clone(),
            pipeline.lifo.clone(),
            pipeline.lifo_sem.clone(),
            metrics.clone(),
            timeout_ms,
        ) {
            SwapIxDispatchOutcome::Sent => sent_to_metis += 1,
            SwapIxDispatchOutcome::DroppedInflight => dropped_by_inflight += 1,
            SwapIxDispatchOutcome::DroppedBudget => dropped_by_budget += 1,
        }
    }
    eprintln!(
        "[candidate_batch] window_ms={} candidates_in={} groups={} selected_per_route={} selected_global={} dropped_by_rank={} dropped_by_inflight={} dropped_by_budget={} sent_to_metis={}",
        window_ms,
        candidates_in,
        groups_len,
        candidates_in.saturating_sub(coalesced),
        selected_global,
        dropped_by_rank,
        dropped_by_inflight,
        dropped_by_budget,
        sent_to_metis
    );
}

fn spawn_fresh_metis_candidate(
    candidate: SwapCandidate,
    ctx: Arc<CalcCtx>,
    lifo: Arc<Mutex<Vec<ReadyInstruction>>>,
    sem: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    timeout_ms: u64,
) -> SwapIxDispatchOutcome {
    match ctx.swap_ix_state.try_start(&candidate) {
        SwapIxStart::Start(permit) => {
            metrics.metis_req_sent.fetch_add(1, Ordering::Relaxed);
            metrics.swap_ix_sent_total.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[fresh_metis_budget] inflight={} max_inflight={} queued=0 sent=1 dropped_low_rank=0",
                ctx.swap_ix_state.inflight_len(),
                ctx.swap_ix_state.max_inflight()
            );
            tokio::spawn(async move {
                let route_sig = candidate.route_sig;
                let candidate_age_ms = candidate.created_at.elapsed().as_millis();
                let t = Instant::now();
                eprintln!(
                    "[fresh_metis_start] route_sig={:032x} route_labels={} programs={} amount={} output_wsol={} net_profit_estimate={} only_direct={} candidate_age_ms={} reason={}",
                    route_sig,
                    candidate.route_labels,
                    candidate.route_programs,
                    candidate.amount,
                    candidate.output_wsol,
                    candidate.net_profit_estimate,
                    candidate.only_direct,
                    candidate_age_ms,
                    candidate.fresh_metis_reason
                );
                let result = ctx
                    .metis
                    .get_swap_instructions_with_timeout(
                        &ctx.user_pubkey,
                        &candidate.merged,
                        timeout_ms,
                    )
                    .await;
                let fetch_ms = t.elapsed().as_millis() as u64;

                let swap_ixs = match result {
                    Ok(s) => s,
                    Err(e) => {
                        let (kind, error) = match e {
                            crate::metis::SwapIxError::Timeout => {
                                ("timeout", "timeout".to_string())
                            }
                            crate::metis::SwapIxError::Http(status) => {
                                ("http", format!("http_status_{status}"))
                            }
                            crate::metis::SwapIxError::Network => {
                                ("network", "network".to_string())
                            }
                            crate::metis::SwapIxError::Parse => ("parse", "parse".to_string()),
                        };
                        eprintln!(
                            "[fresh_metis_err] route_sig={:032x} elapsed_ms={} kind={} error={}",
                            route_sig, fetch_ms, kind, error
                        );
                        metrics.swap_ix_failed.fetch_add(1, Ordering::Relaxed);
                        metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                        match e {
                            crate::metis::SwapIxError::Timeout => {
                                metrics.swap_ix_timeout.fetch_add(1, Ordering::Relaxed)
                            }
                            crate::metis::SwapIxError::Http(_) => {
                                metrics.swap_ix_http.fetch_add(1, Ordering::Relaxed)
                            }
                            crate::metis::SwapIxError::Network => {
                                metrics.swap_ix_network.fetch_add(1, Ordering::Relaxed)
                            }
                            crate::metis::SwapIxError::Parse => {
                                metrics.swap_ix_parse.fetch_add(1, Ordering::Relaxed)
                            }
                        };
                        drop(permit);
                        if let Some(pending) = ctx.swap_ix_state.complete(route_sig) {
                            spawn_fresh_metis_candidate(
                                pending,
                                ctx.clone(),
                                lifo.clone(),
                                sem.clone(),
                                metrics.clone(),
                                timeout_ms,
                            );
                        }
                        return;
                    }
                };

                eprintln!(
                    "[fresh_metis_ok] route_sig={:032x} elapsed_ms={} setup_ix={} swap_ix=1 cleanup_ix={} alts={} action=queue_push",
                    route_sig,
                    fetch_ms,
                    swap_ixs.setup_instructions.len(),
                    if swap_ixs.cleanup_instruction.is_some() { 1 } else { 0 },
                    swap_ixs.address_lookup_table_addresses.len()
                );
                metrics.metis_fetch_ms_total.fetch_add(fetch_ms, Ordering::Relaxed);
                metrics.metis_fetch_samples.fetch_add(1, Ordering::Relaxed);
                metrics.swap_ix_ok.fetch_add(1, Ordering::Relaxed);
                metrics.ix_from_metis.fetch_add(1, Ordering::Relaxed);

                if candidate.save_new {
                    ctx.template_store.insert_route(
                        route_sig,
                        swap_ixs.clone(),
                        candidate.amount,
                        candidate.amount.saturating_add(candidate.min_wsol_gain),
                        &candidate.merged.route_plan,
                    );
                    ctx.template_store
                        .record_hops(&candidate.merged.route_plan, candidate.context_slot);
                }

                let item = ReadyInstruction {
                    swap_ixs,
                    hop_count: candidate.hop_count,
                    min_wsol_gain: candidate.min_wsol_gain,
                    should_simulate: candidate.should_simulate,
                    ix_source: InstructionSource::FreshMetis,
                    route_sig,
                    route_labels: candidate.route_labels.clone(),
                    route_programs: candidate.route_programs.clone(),
                    route_has_alphaq: candidate.route_has_alphaq,
                    arrived_at: Instant::now(),
                    waited_for_slot: false,
                };
                lifo.lock().unwrap().push(item);
                metrics.queue_in.fetch_add(1, Ordering::Relaxed);
                let queue_depth = metrics.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
                eprintln!(
                    "[queue_push] source={} route_sig={:032x} route_labels={} programs={} queue_depth={}",
                    InstructionSource::FreshMetis.as_str(),
                    route_sig,
                    candidate.route_labels,
                    candidate.route_programs,
                    queue_depth
                );
                sem.add_permits(1);

                drop(permit);
                if let Some(pending) = ctx.swap_ix_state.complete(route_sig) {
                    spawn_fresh_metis_candidate(
                        pending,
                        ctx.clone(),
                        lifo.clone(),
                        sem.clone(),
                        metrics.clone(),
                        timeout_ms,
                    );
                }
            });
            SwapIxDispatchOutcome::Sent
        }
        SwapIxStart::DroppedInflight => {
            metrics
                .candidate_dropped_inflight_total
                .fetch_add(1, Ordering::Relaxed);
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[fresh_metis_budget] inflight={} max_inflight={} queued=0 sent=0 dropped_low_rank=0 reason=inflight route_sig={:032x}",
                ctx.swap_ix_state.inflight_len(),
                ctx.swap_ix_state.max_inflight(),
                candidate.route_sig
            );
            SwapIxDispatchOutcome::DroppedInflight
        }
        SwapIxStart::DroppedBudget { inflight } => {
            metrics
                .swap_ix_budget_dropped_total
                .fetch_add(1, Ordering::Relaxed);
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[fresh_metis_budget] inflight={} max_inflight={} queued=0 sent=0 dropped_low_rank=1 reason=budget_full route_sig={:032x}",
                inflight,
                ctx.swap_ix_state.max_inflight(),
                candidate.route_sig
            );
            SwapIxDispatchOutcome::DroppedBudget
        }
    }
}

// ─── Main scan entry ─────────────────────────────────────────────────────────

/// One scan cycle over all (token × amount × route_mode) triples.
///
/// For each profitable 2-hop pair the three-tier flow is:
///
///   Tier 1 — RouteTemplate hit:
///     Serve from RAM using a cached SwapInstructionsResponse.
///     If amounts differ from the template, patch in_amount and
///     quoted_out_amount in the Borsh data at pre-discovered byte offsets.
///     Skips /swap-instructions entirely on a hit.
///
///   Tier 2 — HopTemplate check (metrics only, no composer yet):
///     If all hops in the route have been seen before, record the metric.
///     Still falls through to Metis until a composer is implemented.
///
///   Tier 3 — Metis fallback:
///     Call /swap-instructions as before.
///     On success: save RouteTemplate + record hops (if save_new=true).
pub async fn scan_all_tokens(
    token_mints: &[String],
    config: &Config,
    ctx: &Arc<CalcCtx>,
    pipeline: &Pipeline,
    metrics: &Arc<Metrics>,
    token_metrics: &Arc<TokenMetrics>,
) -> Result<()> {
    let min_lamports = (config.trading.min_amount_sol * LAMPORTS_PER_SOL) as u64;
    let max_lamports = (config.trading.max_amount_sol * LAMPORTS_PER_SOL) as u64;
    let step_lamports = (config.trading.step_sol * LAMPORTS_PER_SOL) as u64;
    let min_profit_lamports = config
        .trading
        .min_profit_lamports
        .saturating_add(JITO_TIP_LAMPORTS)
        .saturating_add(NETWORK_FEE_LAMPORTS)
        .saturating_add(config.performance.metis_latency_margin_lamports);

    // Each (amount, token) generates two independent entries: free routes and
    // direct-only routes. They run concurrently so a slow/timing-out direct
    // request never blocks the free-route check for the same token.
    // Note: free routes (only_direct=false) often return multi-hop paths that
    // are filtered later by the hop_count==2 gate, but direct routes that
    // happen to share the same 2-hop structure ARE also included.
    let all_pairs: Vec<(u64, String, bool)> = {
        let mut pairs = Vec::new();
        let mut amount = min_lamports;
        while amount <= max_lamports {
            for token_mint in token_mints {
                pairs.push((amount, token_mint.clone(), false)); // free routes
                pairs.push((amount, token_mint.clone(), true));  // direct routes only
            }
            amount += step_lamports;
        }
        pairs
    };
    let max_concurrent = config
        .performance
        .max_concurrent_quotes
        .max(1)
        .min(all_pairs.len().max(1));

    let metis_ref: &MetisClient = &ctx.metis;
    let met_ref: &Metrics = metrics;
    let tok_met_ref: &TokenMetrics = token_metrics;
    let simulation_enabled = config.simulation.enabled;

    let mut opps = stream::iter(all_pairs)
        .map(move |(amt, tok, direct)| async move {
            quote_check(metis_ref, &tok, amt, direct, min_profit_lamports, met_ref, tok_met_ref)
                .await
        })
        .buffer_unordered(max_concurrent);

    let mut candidate_batch = Vec::new();
    let mut candidate_batch_started = Instant::now();
    let candidate_batch_window =
        Duration::from_millis(config.performance.candidate_batch_window_ms.max(1));

    while let Some(result) = opps.next().await {
        let pair = match result {
            Some(p) => p,
            None => continue,
        };

        let on_chain_floor = pair.amount + JITO_TIP_LAMPORTS + NETWORK_FEE_LAMPORTS;
        tracing::debug!(
            token = %pair.token_mint,
            amount = pair.amount,
            output = pair.output_wsol,
            quoted_edge = pair.output_wsol as i64 - pair.amount as i64,
            floor_edge = pair.net_profit,
            "send_candidate"
        );

        // Direct routes (only_direct=true) must be exactly 2-hop.
        // Free routes (only_direct=false) may be multi-hop — all are forwarded.
        if pair.only_direct && pair.hop_count != 2 {
            metrics.dropped_multi_hop.fetch_add(1, Ordering::Relaxed);
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let merged = match MetisClient::merge_quotes(&pair.quote1, &pair.quote2, on_chain_floor) {
            Ok(m) => m,
            Err(_) => {
                metrics.dropped_merge_fail.fetch_add(1, Ordering::Relaxed);
                metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        let hop_count = pair.hop_count;
        let amount = pair.amount;
        let sig = template_cache::route_sig(&merged.route_plan);
        let tc = &config.template_cache;
        let context_slot = merged.context_slot;
        let min_wsol_gain = on_chain_floor.saturating_sub(amount);
        let route_has_alphaq = route_plan_mentions_alphaq(&merged.route_plan);
        let route_labels = route_labels_summary(&merged.route_plan);
        let route_programs = route_programs_summary(&merged.route_plan);
        let route_mentions_opaque = route_plan_mentions_opaque_dex(&merged.route_plan);
        let force_fresh_metis = tc.force_fresh_metis_all || route_mentions_opaque;
        let fresh_metis_reason = if tc.force_fresh_metis_all {
            "force_fresh_metis_all"
        } else if route_mentions_opaque {
            "opaque_dex"
        } else {
            "template_miss_or_fallback"
        };
        if let Some(registry) = &ctx.mix_registry {
            if let Some(report) = registry.check_route_plan(&merged.route_plan) {
                if report.is_unverified_only() {
                    metrics
                        .dropped_unverified_mix_route
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    metrics
                        .dropped_invalid_mix_route
                        .fetch_add(1, Ordering::Relaxed);
                }
                metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "[drop_invalid_mix_route] route_sig={:032x} reason={} labels={} amm_keys={} invalid_pools={} unverified_pools={} invalid_accounts={} unverified_accounts={}",
                    sig,
                    report.reason(),
                    route_labels,
                    crate::mix_registry::pubkeys_json(&report.amm_keys),
                    crate::mix_registry::pubkeys_json(&report.invalid_pools),
                    crate::mix_registry::pubkeys_json(&report.unverified_pools),
                    crate::mix_registry::pubkeys_json(&report.invalid_accounts),
                    crate::mix_registry::pubkeys_json(&report.unverified_accounts)
                );
                continue;
            }
        }
        let should_simulate = simulation_enabled;
        if should_simulate {
            metrics.sim_required.fetch_add(1, Ordering::Relaxed);
        } else {
            metrics.sim_bypassed.fetch_add(1, Ordering::Relaxed);
        }
        let allow_template_serving = tc.serve_route && !force_fresh_metis;
        if force_fresh_metis {
            eprintln!(
                "[template_disable] route_sig={:032x} route_labels={} programs={} reason={} action=fresh_metis_only",
                sig,
                route_labels,
                route_programs,
                fresh_metis_reason
            );
        }

        // ── Tier 1: RouteTemplate hit ─────────────────────────────────────────
        // RouteTemplate is keyed by route_sig (NO amount). When the same route
        // structure is requested with a different amount, the Borsh instruction
        // data is patched at pre-discovered byte offsets.
        if allow_template_serving {
            if let Some(tmpl) = ctx.template_store.get_route(sig) {
                if let Some(patched) = template_cache::serve_route(&tmpl, amount, on_chain_floor) {
                    metrics.route_template_hit.fetch_add(1, Ordering::Relaxed);
                    metrics.ix_from_ram.fetch_add(1, Ordering::Relaxed);
                    metrics.swap_ix_ok.fetch_add(1, Ordering::Relaxed);
                    ctx.template_store.record_route_hit(sig);
                    push_to_queue(
                        patched,
                        hop_count,
                        min_wsol_gain,
                        should_simulate,
                        InstructionSource::RouteTemplate,
                        sig,
                        route_labels.clone(),
                        route_programs.clone(),
                        route_has_alphaq,
                        pipeline,
                        metrics,
                    );
                    continue;
                }
                // Patching failed (no offsets discovered): fall through to Tier-2.
            }
        }

        // ── Tier 2: HopTemplate check + hop-pair secondary route lookup ──────
        // When all hops are known, try to find a RouteTemplate via the
        // hop-pair index. This serves from RAM even when the primary route_sig
        // doesn't match exactly (e.g. an extra unstable field in swapInfo).
        if !force_fresh_metis {
            let (all_hit, missing) = ctx.template_store.check_hops(&merged.route_plan);
            if all_hit {
                metrics.hop_template_all_hit.fetch_add(1, Ordering::Relaxed);
                if allow_template_serving {
                    if let Some(tmpl) = ctx.template_store.get_route_for_hops(&merged.route_plan) {
                        if let Some(patched) =
                            template_cache::serve_route(&tmpl, amount, on_chain_floor)
                        {
                            metrics.route_template_hit.fetch_add(1, Ordering::Relaxed);
                            metrics.ix_from_ram.fetch_add(1, Ordering::Relaxed);
                            metrics.swap_ix_ok.fetch_add(1, Ordering::Relaxed);
                            ctx.template_store.record_route_hit(tmpl.route_signature);
                            push_to_queue(
                                patched,
                                hop_count,
                                min_wsol_gain,
                                should_simulate,
                                InstructionSource::HopTemplate,
                                sig,
                                route_labels.clone(),
                                route_programs.clone(),
                                route_has_alphaq,
                                pipeline,
                                metrics,
                            );
                            continue;
                        }
                    }
                }
            } else if missing > 0 {
                metrics
                    .hop_template_missing
                    .fetch_add(missing as u64, Ordering::Relaxed);
            }
        }

        // ── Tier 3: Metis fallback ────────────────────────────────────────────
        if !tc.serve_from_metis && !force_fresh_metis {
            metrics.dropped_no_serve.fetch_add(1, Ordering::Relaxed);
            metrics.tx_dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let save_new = tc.save_new && !force_fresh_metis;
        if candidate_batch.is_empty() {
            candidate_batch_started = Instant::now();
        }
        let route_key = route_group_key(&merged.route_plan, sig, hop_count, pair.only_direct);
        candidate_batch.push(SwapCandidate {
            merged,
            amount,
            output_wsol: pair.output_wsol,
            net_profit_estimate: pair.net_profit,
            hop_count,
            only_direct: pair.only_direct,
            route_sig: sig,
            route_key,
            route_labels: route_labels.clone(),
            route_programs: route_programs.clone(),
            route_has_alphaq,
            context_slot,
            min_wsol_gain,
            should_simulate,
            save_new,
            fresh_metis_reason: fresh_metis_reason.to_string(),
            created_at: Instant::now(),
        });
        metrics
            .candidate_profitable_total
            .fetch_add(1, Ordering::Relaxed);

        if candidate_batch_started.elapsed() >= candidate_batch_window {
            flush_candidate_batch(&mut candidate_batch, config, ctx, pipeline, metrics);
            candidate_batch_started = Instant::now();
        }
    }

    flush_candidate_batch(&mut candidate_batch, config, ctx, pipeline, metrics);

    Ok(())
}

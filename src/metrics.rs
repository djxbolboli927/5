use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const WINDOW_SECS: u64 = 30;

pub struct Metrics {
    // ── Stage 1: quoting ─────────────────────────────────────────────────────
    /// Total HTTP requests sent to Metis (quotes + swap_instructions)
    pub metis_req_sent: AtomicU64,
    /// Round-trips where both quote1+quote2 returned successfully
    pub metis_resp_total: AtomicU64,
    /// Quote pairs that passed the profitability check at quote time.
    pub metis_resp_ok: AtomicU64,
    /// Items that successfully entered the LIFO queue (template hit OR swap_ix success).
    pub swap_ix_ok: AtomicU64,

    // ── Stage 1.5: template lookup + swap_instructions ────────────────────────
    /// Instructions served from RouteTemplate in RAM (Tier-1 exact + Tier-2 hop-pair).
    /// No /swap-instructions call was made for these.
    pub ix_from_ram: AtomicU64,
    /// Instructions obtained from Metis /swap-instructions (Tier-3 fallback).
    pub ix_from_metis: AtomicU64,
    /// RouteTemplate hit: served from RAM with amount patching (no Metis call).
    pub route_template_hit: AtomicU64,
    /// All hops in the route had a HopTemplate (metrics only; no composer yet).
    pub hop_template_all_hit: AtomicU64,
    /// At least one hop in the route was missing a HopTemplate.
    pub hop_template_missing: AtomicU64,
    /// /swap-instructions returned an error. Sum of four below.
    pub swap_ix_failed: AtomicU64,
    /// Breakdown: request exceeded quote_timeout_ms (Metis too slow).
    pub swap_ix_timeout: AtomicU64,
    /// Breakdown: Metis returned non-2xx (no route / rejected merged quote).
    pub swap_ix_http: AtomicU64,
    /// Breakdown: connection-level failure (TCP reset, pool exhausted, etc.).
    pub swap_ix_network: AtomicU64,
    /// Breakdown: 2xx body could not be parsed as SwapInstructionsResponse.
    pub swap_ix_parse: AtomicU64,
    /// Profitable candidates admitted to the pre-/swap-instructions selector.
    pub candidate_profitable_total: AtomicU64,
    /// Candidates collapsed by per-route coalescing.
    pub candidate_coalesced_total: AtomicU64,
    /// Candidates dropped by per-route/global ranking before Metis.
    pub candidate_dropped_rank_total: AtomicU64,
    /// Candidates dropped because an equal/better route is already in-flight.
    pub candidate_dropped_inflight_total: AtomicU64,
    /// Candidates dropped because the swap-instructions concurrency budget is full.
    pub swap_ix_budget_dropped_total: AtomicU64,
    /// Actual /swap-instructions requests sent after coalescing and budget checks.
    pub swap_ix_sent_total: AtomicU64,
    /// Profitable opp dropped: direct route had hop_count > 2 (should be 0).
    pub dropped_multi_hop: AtomicU64,
    /// Profitable opp dropped: merge_quotes failed (incompatible route formats).
    pub dropped_merge_fail: AtomicU64,
    /// Profitable opp dropped: no template AND serve_from_metis=false.
    pub dropped_no_serve: AtomicU64,
    /// Both legs of this "profitable" quote share an AMM pool; round-trip
    /// on the same pool always loses. Filtered before /swap-instructions.
    pub dropped_same_pool: AtomicU64,
    /// Items pushed into the LIFO queue.
    pub queue_in: AtomicU64,
    /// Current LIFO queue depth (gauge).
    pub queue_depth: AtomicI64,

    // ── Stage 2: worker processing ────────────────────────────────────────────
    pub dropped_stale: AtomicU64,
    pub tx_build_failed: AtomicU64,
    pub tx_too_large: AtomicU64,
    /// Built tx exceeded Solana's 64 distinct-account-lock limit (guaranteed
    /// block-engine reject). Dropped locally instead of burning a Jito slot.
    pub dropped_account_locks: AtomicU64,
    /// Route must pass local LiteSVM simulation before Jito send.
    pub sim_required: AtomicU64,
    /// Route skipped simulation because simulation is disabled.
    pub sim_bypassed: AtomicU64,
    /// Local LiteSVM simulation succeeded and met the WSOL profit floor.
    pub sim_ok: AtomicU64,
    /// Local LiteSVM simulation reverted, errored, or missed the profit floor.
    pub sim_failed: AtomicU64,
    /// Account required by a transaction was absent from the live sim cache.
    pub sim_missing_account: AtomicU64,
    /// Mix accounts that failed startup verification with account-not-found.
    pub invalid_mix_accounts: AtomicU64,
    /// Mix pools blocked because at least one static account is invalid/unverified.
    pub invalid_mix_pools: AtomicU64,
    /// Runtime routes dropped because they touched a permanently invalid mix entry.
    pub dropped_invalid_mix_route: AtomicU64,
    /// Runtime routes dropped because they touched an unverified mix entry.
    pub dropped_unverified_mix_route: AtomicU64,
    /// AlphaQ failed in LiteSVM with InvalidAccountOwner.
    pub sim_alphaq_invalid_owner: AtomicU64,
    /// RPC simulateTransaction succeeded for a tx that LiteSVM rejected.
    pub sim_rpc_compare_ok: AtomicU64,
    /// RPC simulateTransaction rejected with an error too.
    pub sim_rpc_compare_same_fail: AtomicU64,
    /// RPC simulateTransaction comparison itself failed.
    pub sim_rpc_compare_error: AtomicU64,
    /// Cache/RPC account-state mismatches observed while debugging a failed sim.
    pub sim_state_mismatch_total: AtomicU64,
    /// Cache/RPC mismatches for writable transaction accounts.
    pub sim_state_mismatch_writable: AtomicU64,
    /// Cache/RPC mismatches for readonly transaction accounts.
    pub sim_state_mismatch_readonly: AtomicU64,
    /// One-shot LiteSVM retry with a fresh RPC account snapshot succeeded.
    pub sim_retry_rpc_snapshot_ok: AtomicU64,
    /// One-shot LiteSVM retry with a fresh RPC account snapshot failed.
    pub sim_retry_rpc_snapshot_fail: AtomicU64,
    /// Routes that would have been dropped only because of data-hash drift.
    pub sim_mix_gate_data_hash_only_drop: AtomicU64,
    pub calc_done: AtomicU64,

    // ── Stage 3: Jito send ────────────────────────────────────────────────────
    pub rate_requeued: AtomicU64,
    pub jito_send_failed: AtomicU64,
    pub jito_sent: AtomicU64,

    // ── Legacy aggregates (drained each window, not shown) ────────────────────
    pub dropped_busy: AtomicU64,
    pub tx_dropped: AtomicU64,

    // ── swap_instructions latency ─────────────────────────────────────────────
    pub metis_fetch_ms_total: AtomicU64,
    pub metis_fetch_samples: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            metis_req_sent: AtomicU64::new(0),
            metis_resp_total: AtomicU64::new(0),
            metis_resp_ok: AtomicU64::new(0),
            swap_ix_ok: AtomicU64::new(0),
            ix_from_ram: AtomicU64::new(0),
            ix_from_metis: AtomicU64::new(0),
            route_template_hit: AtomicU64::new(0),
            hop_template_all_hit: AtomicU64::new(0),
            hop_template_missing: AtomicU64::new(0),
            swap_ix_failed: AtomicU64::new(0),
            swap_ix_timeout: AtomicU64::new(0),
            swap_ix_http: AtomicU64::new(0),
            swap_ix_network: AtomicU64::new(0),
            swap_ix_parse: AtomicU64::new(0),
            candidate_profitable_total: AtomicU64::new(0),
            candidate_coalesced_total: AtomicU64::new(0),
            candidate_dropped_rank_total: AtomicU64::new(0),
            candidate_dropped_inflight_total: AtomicU64::new(0),
            swap_ix_budget_dropped_total: AtomicU64::new(0),
            swap_ix_sent_total: AtomicU64::new(0),
            queue_in: AtomicU64::new(0),
            queue_depth: AtomicI64::new(0),
            dropped_stale: AtomicU64::new(0),
            tx_build_failed: AtomicU64::new(0),
            tx_too_large: AtomicU64::new(0),
            dropped_account_locks: AtomicU64::new(0),
            sim_required: AtomicU64::new(0),
            sim_bypassed: AtomicU64::new(0),
            sim_ok: AtomicU64::new(0),
            sim_failed: AtomicU64::new(0),
            sim_missing_account: AtomicU64::new(0),
            invalid_mix_accounts: AtomicU64::new(0),
            invalid_mix_pools: AtomicU64::new(0),
            dropped_invalid_mix_route: AtomicU64::new(0),
            dropped_unverified_mix_route: AtomicU64::new(0),
            sim_alphaq_invalid_owner: AtomicU64::new(0),
            sim_rpc_compare_ok: AtomicU64::new(0),
            sim_rpc_compare_same_fail: AtomicU64::new(0),
            sim_rpc_compare_error: AtomicU64::new(0),
            sim_state_mismatch_total: AtomicU64::new(0),
            sim_state_mismatch_writable: AtomicU64::new(0),
            sim_state_mismatch_readonly: AtomicU64::new(0),
            sim_retry_rpc_snapshot_ok: AtomicU64::new(0),
            sim_retry_rpc_snapshot_fail: AtomicU64::new(0),
            sim_mix_gate_data_hash_only_drop: AtomicU64::new(0),
            calc_done: AtomicU64::new(0),
            rate_requeued: AtomicU64::new(0),
            jito_send_failed: AtomicU64::new(0),
            jito_sent: AtomicU64::new(0),
            dropped_busy: AtomicU64::new(0),
            tx_dropped: AtomicU64::new(0),
            metis_fetch_ms_total: AtomicU64::new(0),
            metis_fetch_samples: AtomicU64::new(0),
            dropped_multi_hop: AtomicU64::new(0),
            dropped_merge_fail: AtomicU64::new(0),
            dropped_no_serve: AtomicU64::new(0),
            dropped_same_pool: AtomicU64::new(0),
        })
    }

    pub fn spawn_reporter(
        self: &Arc<Self>,
        queue_max_age_ms: u64,
        store: Arc<crate::template_cache::TemplateStore>,
    ) {
        let m = self.clone();
        let ttl_secs = queue_max_age_ms as f64 / 1000.0;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(WINDOW_SECS));
            interval.tick().await;

            loop {
                interval.tick().await;

                let sent      = m.metis_req_sent.swap(0, Ordering::Relaxed);
                let routes    = m.metis_resp_total.swap(0, Ordering::Relaxed);
                let profit    = m.metis_resp_ok.swap(0, Ordering::Relaxed);
                let sw_ok     = m.swap_ix_ok.swap(0, Ordering::Relaxed);

                let from_ram   = m.ix_from_ram.swap(0, Ordering::Relaxed);
                let from_metis = m.ix_from_metis.swap(0, Ordering::Relaxed);
                let rt_hit    = m.route_template_hit.swap(0, Ordering::Relaxed);
                let ht_all    = m.hop_template_all_hit.swap(0, Ordering::Relaxed);
                let ht_miss   = m.hop_template_missing.swap(0, Ordering::Relaxed);

                let swap_fail = m.swap_ix_failed.swap(0, Ordering::Relaxed);
                let sf_to     = m.swap_ix_timeout.swap(0, Ordering::Relaxed);
                let sf_http   = m.swap_ix_http.swap(0, Ordering::Relaxed);
                let sf_net    = m.swap_ix_network.swap(0, Ordering::Relaxed);
                let sf_parse  = m.swap_ix_parse.swap(0, Ordering::Relaxed);
                let cand_total = m.candidate_profitable_total.swap(0, Ordering::Relaxed);
                let cand_coalesced = m.candidate_coalesced_total.swap(0, Ordering::Relaxed);
                let cand_rank_drop = m.candidate_dropped_rank_total.swap(0, Ordering::Relaxed);
                let cand_inflight_drop = m.candidate_dropped_inflight_total.swap(0, Ordering::Relaxed);
                let swap_budget_drop = m.swap_ix_budget_dropped_total.swap(0, Ordering::Relaxed);
                let swap_sent_total = m.swap_ix_sent_total.swap(0, Ordering::Relaxed);
                let q_in      = m.queue_in.swap(0, Ordering::Relaxed);

                let stale     = m.dropped_stale.swap(0, Ordering::Relaxed);
                let build     = m.tx_build_failed.swap(0, Ordering::Relaxed);
                let too_big   = m.tx_too_large.swap(0, Ordering::Relaxed);
                let too_locks = m.dropped_account_locks.swap(0, Ordering::Relaxed);
                let sim_req   = m.sim_required.swap(0, Ordering::Relaxed);
                let sim_byp   = m.sim_bypassed.swap(0, Ordering::Relaxed);
                let sim_ok    = m.sim_ok.swap(0, Ordering::Relaxed);
                let sim_fail  = m.sim_failed.swap(0, Ordering::Relaxed);
                let sim_miss  = m.sim_missing_account.swap(0, Ordering::Relaxed);
                let mix_invalid_accounts = m.invalid_mix_accounts.swap(0, Ordering::Relaxed);
                let mix_invalid_pools    = m.invalid_mix_pools.swap(0, Ordering::Relaxed);
                let mix_drop_invalid     = m.dropped_invalid_mix_route.swap(0, Ordering::Relaxed);
                let mix_drop_unverified  = m.dropped_unverified_mix_route.swap(0, Ordering::Relaxed);
                let sim_alphaq_owner = m.sim_alphaq_invalid_owner.swap(0, Ordering::Relaxed);
                let rpc_cmp_ok       = m.sim_rpc_compare_ok.swap(0, Ordering::Relaxed);
                let rpc_cmp_fail     = m.sim_rpc_compare_same_fail.swap(0, Ordering::Relaxed);
                let rpc_cmp_err      = m.sim_rpc_compare_error.swap(0, Ordering::Relaxed);
                let state_mismatch_total = m.sim_state_mismatch_total.swap(0, Ordering::Relaxed);
                let state_mismatch_writable = m.sim_state_mismatch_writable.swap(0, Ordering::Relaxed);
                let state_mismatch_readonly = m.sim_state_mismatch_readonly.swap(0, Ordering::Relaxed);
                let retry_rpc_snapshot_ok = m.sim_retry_rpc_snapshot_ok.swap(0, Ordering::Relaxed);
                let retry_rpc_snapshot_fail = m.sim_retry_rpc_snapshot_fail.swap(0, Ordering::Relaxed);
                let mix_data_hash_only_drop = m.sim_mix_gate_data_hash_only_drop.swap(0, Ordering::Relaxed);
                let calc      = m.calc_done.swap(0, Ordering::Relaxed);
                let requeued  = m.rate_requeued.swap(0, Ordering::Relaxed);
                let jfail     = m.jito_send_failed.swap(0, Ordering::Relaxed);
                let jito      = m.jito_sent.swap(0, Ordering::Relaxed);

                let ms_ms     = m.metis_fetch_ms_total.swap(0, Ordering::Relaxed);
                let ms_n      = m.metis_fetch_samples.swap(0, Ordering::Relaxed);

                let depth     = m.queue_depth.load(Ordering::Relaxed);
                let _         = m.tx_dropped.swap(0, Ordering::Relaxed);
                let _         = m.dropped_busy.swap(0, Ordering::Relaxed);

                let drop_hop    = m.dropped_multi_hop.swap(0, Ordering::Relaxed);
                let drop_merge  = m.dropped_merge_fail.swap(0, Ordering::Relaxed);
                let drop_no_srv = m.dropped_no_serve.swap(0, Ordering::Relaxed);
                let drop_pool   = m.dropped_same_pool.swap(0, Ordering::Relaxed);

                let avg_ms    = if ms_n > 0 { ms_ms / ms_n } else { 0 };
                let n_routes  = store.route_count();
                let n_patch   = store.patchable_route_count();
                let n_hops    = store.hop_count();

                let ram_pct = if sw_ok > 0 { from_ram * 100 / sw_ok } else { 0 };

                eprintln!(
                    "[{WINDOW_SECS}s] \
metis_sent={sent} routes={routes} quoted_profitable={profit}\n  \
TEMPLATE  : route_hit={rt_hit}  hop_all_hit={ht_all}  hop_miss={ht_miss}  routes={n_routes}(patchable={n_patch})  hops={n_hops}\n  \
IX-SOURCE : from_ram={from_ram}  from_metis={from_metis}  ram_pct={ram_pct}%\n  \
  FUNNEL    : profitable={profit}  drop_same_pool={drop_pool}  drop_multi_hop={drop_hop}  drop_merge={drop_merge}  drop_no_serve={drop_no_srv}  -> swap_ix_ok={sw_ok}\n  \
  CANDIDATE : admitted={cand_total}  coalesced={cand_coalesced}  drop_rank={cand_rank_drop}  drop_inflight={cand_inflight_drop}  swap_budget_drop={swap_budget_drop}  swap_sent={swap_sent_total}\n  \
PRE-QUEUE : swap_ix_ok={sw_ok}  swap_ix_fail={swap_fail} [timeout={sf_to} http={sf_http} net={sf_net} parse={sf_parse}] -> queue_in={q_in}  (depth_now={depth})\n  \
  IN-QUEUE  : stale={stale} (waited >{ttl_secs}s)\n  \
  TX-BUILD  : build_fail={build}  too_large={too_big}  too_many_locks={too_locks}  calc_ok={calc}\n  \
  MIX       : invalid_accounts={mix_invalid_accounts}  invalid_pools={mix_invalid_pools}  drop_invalid_route={mix_drop_invalid}  drop_unverified_route={mix_drop_unverified}  data_hash_only_drop={mix_data_hash_only_drop}\n  \
  SIM       : required={sim_req}  bypassed={sim_byp}  ok={sim_ok}  fail={sim_fail}  missing_account={sim_miss}  alphaq_invalid_owner={sim_alphaq_owner}\n  \
  SIM-CMP   : rpc_ok={rpc_cmp_ok}  rpc_same_fail={rpc_cmp_fail}  rpc_compare_error={rpc_cmp_err}  state_mismatch={state_mismatch_total} [writable={state_mismatch_writable} readonly={state_mismatch_readonly}]  retry_rpc_snapshot_ok={retry_rpc_snapshot_ok}  retry_rpc_snapshot_fail={retry_rpc_snapshot_fail}\n  \
  JITO      : sent={jito}  send_fail={jfail}  waited_for_slot={requeued}\n  \
SWAP-IX   : avg_metis={avg_ms}ms"
                );
                if sim_req > 0 && sim_ok == 0 && sim_fail > 0 {
                    eprintln!(
                        "[sim_status] all simulations failed in this window; no tx sent past the simulation gate because fail_closed=true"
                    );
                }
            }
        });
    }
}

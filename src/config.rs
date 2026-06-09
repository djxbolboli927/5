use anyhow::Result;
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub metis: MetisConfig,
    pub trading: TradingConfig,
    pub jito: JitoConfig,
    pub rpc: RpcConfig,
    pub yellowstone_grpc: YellowstoneGrpcConfig,
    pub performance: PerformanceConfig,
    #[serde(default)]
    pub simulation: SimulationConfig,
    #[serde(default)]
    pub jito_grpc: JitoGrpcConfig,
    #[serde(default)]
    pub template_cache: TemplateCacheConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetisConfig {
    pub url: String,
    #[allow(dead_code)]
    pub binary_key: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TradingConfig {
    pub min_amount_sol: f64,
    pub max_amount_sol: f64,
    pub step_sol: f64,
    pub min_profit_lamports: u64,
    /// Standard Solana transaction fee in lamports (5000 = one signature fee).
    #[allow(dead_code)]
    pub base_fee_lamports: u64,
    pub tokens_file: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct JitoConfig {
    /// Multiple Jito block engine URLs -- bundles are sent to ALL concurrently.
    pub urls: Vec<String>,
    pub uuid: String,
    pub trading_keypair: String,
    #[allow(dead_code)]
    pub tip_min_lamports: u64,
    #[allow(dead_code)]
    pub tip_max_lamports: u64,
    #[allow(dead_code)]
    pub tip_profit_percent: f64,
    pub max_bundles_per_second: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RpcConfig {
    pub url: String,
    /// Commitment level for all RPC reads (account fetches, sim-compare,
    /// retry snapshots). Must match the Yellowstone stream commitment
    /// ("processed") so the cache and the RPC baseline see the same slot.
    /// Using the default "finalized" makes every actively-traded pool look
    /// stale because finalized lags the processed stream by ~30+ slots.
    #[serde(default = "default_rpc_commitment")]
    pub commitment: String,
}

fn default_rpc_commitment() -> String {
    "processed".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct YellowstoneGrpcConfig {
    pub endpoint: String,
    pub x_token: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SimulationConfig {
    /// If false, bot sends every profitable tx without any local sim gate
    /// (pre-LiteSVM behaviour). Default: disabled so legacy configs keep
    /// working until the operator opts in.
    #[serde(default)]
    pub enabled: bool,
    /// Directory containing the DEX .so binaries listed in `program_registry`.
    #[serde(default = "default_so_dir")]
    pub so_dir: String,
    /// Directory containing per-pool account files (`dex/<DEX>/<pool>.toml`).
    /// These are pre-fetched at startup and the vault accounts within are
    /// subscribed for live Yellowstone updates.
    #[serde(default = "default_dex_dir")]
    pub dex_dir: String,
    /// Canonical DEX keys kept for operator config compatibility and reporting.
    /// With `simulation.enabled=true`, all routes are simulated regardless of
    /// this list; missing programs/accounts fail closed instead of bypassing.
    #[serde(default = "default_simulation_dexes", alias = "enabled_exchanges")]
    pub enabled_dexes: Vec<String>,
    /// When sim reverts or errors, `fail_closed=true` drops the send (safest);
    /// `false` logs and forwards to Jito anyway (useful during rollout).
    #[serde(default = "default_true")]
    pub fail_closed: bool,
    /// Number of INDEPENDENT Simulator instances to spin up. Each Simulator
    /// owns its own `Mutex<LiteSVM>`, so N workers = N sims in parallel.
    /// Sizing guidance: in steady state each sim takes ~2-5ms of CPU, so
    /// `workers` should roughly equal the peak number of profitable
    /// opportunities that arrive per 5ms window. In production, 8 is a
    /// sensible default (handles ~1600 sims/sec with headroom).
    #[serde(default = "default_workers")]
    pub workers: usize,
    /// Startup RPC warm-up rate. For mix.json this is interpreted as pools
    /// per second because each pool group is fetched with getMultipleAccounts.
    #[serde(default = "default_prefetch_pools_per_second")]
    pub prefetch_pools_per_second: u64,
    /// Production should keep this false: simulation may use cache/gRPC state,
    /// but it must not wait on RPC in the hot path.
    #[serde(default)]
    pub allow_hot_path_rpc_fetch: bool,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            so_dir: default_so_dir(),
            dex_dir: default_dex_dir(),
            enabled_dexes: default_simulation_dexes(),
            fail_closed: true,
            workers: default_workers(),
            prefetch_pools_per_second: default_prefetch_pools_per_second(),
            allow_hot_path_rpc_fetch: false,
        }
    }
}

fn default_so_dir() -> String {
    "/home/soluser/m/so".to_string()
}

fn default_dex_dir() -> String {
    "vendor/litesvm/dex".to_string()
}

fn default_simulation_dexes() -> Vec<String> {
    vec![
        "meteora_damm_v2".to_string(),
        "meteora_dlmm".to_string(),
        "raydium_amm_v4".to_string(),
        "raydium_clmm".to_string(),
        "raydium_cpmm".to_string(),
        "whirlpool".to_string(),
    ]
}

fn default_true() -> bool {
    true
}

fn default_workers() -> usize {
    8
}

fn default_prefetch_pools_per_second() -> u64 {
    5
}

/// Second Jito submission path via SearcherService gRPC.
///
/// Runs alongside the REST UUID client in `jito.rs`. Each path has its
/// own rate limiter, so the effective Jito throughput is
/// `jito.max_bundles_per_second + jito_grpc.max_bundles_per_second`.
///
/// Like the REST client, gRPC fans out to every regional Block Engine
/// endpoint concurrently — first regional success wins. Per-region auth
/// is attempted using the whitelisted keypair, which gives 5 req/s per
/// region. Regions whose auth fails downgrade to no-auth mode (1 req/s).
#[derive(Debug, Deserialize, Clone)]
pub struct JitoGrpcConfig {
    /// If false, only the REST UUID path is used (pre-gRPC behaviour).
    #[serde(default)]
    pub enabled: bool,
    /// All Jito Block Engine gRPC endpoints. Bundles are broadcast to ALL
    /// of these per send call, mirroring the REST multi-region fan-out.
    #[serde(default = "default_jito_grpc_endpoints")]
    pub endpoints: Vec<String>,
    /// Path to the Solana keypair JSON whose pubkey Jito has whitelisted
    /// for gRPC auth. This wallet holds no funds — it is an identity only.
    /// If empty or auth fails, regions fall back to no-auth (1 req/s).
    #[serde(default)]
    pub auth_keypair: String,
    /// Per-second rate limit applied *before* the gRPC SendBundle call.
    /// REST and gRPC limiters operate independently.
    #[serde(default = "default_grpc_rate")]
    pub max_bundles_per_second: u32,
}

impl Default for JitoGrpcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoints: default_jito_grpc_endpoints(),
            auth_keypair: String::new(),
            max_bundles_per_second: default_grpc_rate(),
        }
    }
}

fn default_jito_grpc_endpoints() -> Vec<String> {
    vec![
        "https://amsterdam.mainnet.block-engine.jito.wtf".to_string(),
        "https://dublin.mainnet.block-engine.jito.wtf".to_string(),
        "https://frankfurt.mainnet.block-engine.jito.wtf".to_string(),
        "https://london.mainnet.block-engine.jito.wtf".to_string(),
        "https://ny.mainnet.block-engine.jito.wtf".to_string(),
        "https://slc.mainnet.block-engine.jito.wtf".to_string(),
        "https://singapore.mainnet.block-engine.jito.wtf".to_string(),
        "https://tokyo.mainnet.block-engine.jito.wtf".to_string(),
    ]
}

fn default_grpc_rate() -> u32 {
    5
}

#[derive(Debug, Deserialize, Clone)]
pub struct PerformanceConfig {
    /// Number of tokio worker threads (multi-thread runtime).
    pub threads: usize,
    pub quote_timeout_ms: u64,
    /// CU limits per hop count: index 0 = 2 hops, index 1 = 3 hops, etc.
    /// If hops exceed the array, the last value is used.
    pub cu_limits: Vec<u32>,
    /// Maximum in-flight Metis quote requests per scan chunk.
    /// Keeps the HTTP connection pool from being overwhelmed.
    #[serde(default = "default_max_concurrent_quotes")]
    pub max_concurrent_quotes: usize,
    /// Micro-batch window before firing /swap-instructions. Candidates inside
    /// this short window are ranked and coalesced by route before Metis.
    #[serde(default = "default_candidate_batch_window_ms")]
    pub candidate_batch_window_ms: u64,
    /// Keep at most this many profitable candidates per route signature.
    #[serde(default = "default_candidate_top_per_route")]
    pub candidate_top_per_route: usize,
    /// Keep at most this many candidates globally per micro-batch.
    #[serde(default = "default_candidate_global_top_n")]
    pub candidate_global_top_n: usize,
    /// Hard cap for concurrent /swap-instructions requests.
    #[serde(default = "default_max_concurrent_swap_instructions")]
    pub max_concurrent_swap_instructions: usize,
    /// Timeout for /swap-instructions, separate from quote timeout.
    #[serde(default = "default_swap_instructions_timeout_ms")]
    pub swap_instructions_timeout_ms: u64,
    /// Extra quote-stage margin before a candidate is allowed to request
    /// /swap-instructions. This absorbs latency and reduces Metis bursts.
    #[serde(default = "default_metis_latency_margin_lamports")]
    pub metis_latency_margin_lamports: u64,
    /// Maximum concurrent Stage-2 calc workers (merge quotes + fire
    /// swap_instructions). With fire-and-forget each worker holds its slot
    /// only for microseconds, so this can be set high to rule out the calc
    /// stage as a bottleneck. Default 6 (legacy value).
    #[serde(default = "default_calc_workers")]
    pub calc_workers: usize,
    /// Max time (ms) a swap_instructions result may wait in the LIFO queue
    /// before being dropped by a calc worker. Tune higher to tolerate slower
    /// Metis responses; lower to discard stale opportunities faster.
    #[serde(default = "default_queue_max_age_ms")]
    pub queue_max_age_ms: u64,
    #[serde(default)]
    pub bot_cpu_cores: Vec<usize>,
}

fn default_max_concurrent_quotes() -> usize {
    512
}

fn default_candidate_batch_window_ms() -> u64 {
    30
}

fn default_candidate_top_per_route() -> usize {
    1
}

fn default_candidate_global_top_n() -> usize {
    40
}

fn default_max_concurrent_swap_instructions() -> usize {
    32
}

fn default_swap_instructions_timeout_ms() -> u64 {
    800
}

fn default_metis_latency_margin_lamports() -> u64 {
    5_000
}

fn default_calc_workers() -> usize {
    6
}

fn default_queue_max_age_ms() -> u64 {
    5000
}

/// Template cache configuration.
///
/// Rollout order:
///   1. save_new=true       — extract and store route/hop templates from Metis
///                            responses. No behaviour change yet.
///   2. serve_route=true    — serve from RouteTemplate on hit, patching
///                            in_amount / quoted_out_amount in the Borsh data.
///                            Falls back to Metis when patching is not possible.
///   3. serve_from_metis=false — RAM-only (miss = drop, no Metis call).
#[derive(Debug, Deserialize, Clone)]
pub struct TemplateCacheConfig {
    /// Hard test switch: never serve or save templates; every instruction must
    /// come from a fresh Metis /swap-instructions response.
    #[serde(default)]
    pub force_fresh_metis_all: bool,
    /// Extract and save route/hop templates from every Metis response.
    #[serde(default)]
    pub save_new: bool,
    /// Serve from RouteTemplate when available (patches amounts if needed).
    #[serde(default)]
    pub serve_route: bool,
    /// Call Metis for instructions when no route template hits.
    #[serde(default = "default_true_tc")]
    pub serve_from_metis: bool,
}

impl Default for TemplateCacheConfig {
    fn default() -> Self {
        Self {
            force_fresh_metis_all: false,
            save_new: false,
            serve_route: false,
            serve_from_metis: true,
        }
    }
}

fn default_true_tc() -> bool {
    true
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}

//! Mapping between Metis DEX_PROGRAM_IDS and the .so binaries the user has
//! placed on disk. These .so files are loaded into LiteSVM at startup so the
//! simulator can execute real on-chain bytecode, not a mathematical model.
//!
//! Every entry is (on-chain program id, filename inside `simulation.so_dir`).
//! If a filename is empty or the file is missing, the program is not loaded.
//! With simulation enabled, a tx touching that missing executable fails closed
//! instead of being sent through an implicit DEX-level bypass.
//!
//! Nothing is blocked. Every DEX registered here is eligible for arbitrage.

pub const PROGRAMS: &[(&str, &str)] = &[
    // Aggregator (top-level program the swap_instruction targets).
    // Metis is a Jupiter fork: its swap_instruction carries Jupiter v6's
    // program_id. Without this loaded as executable, LiteSVM rejects the tx
    // with "Program account JUP6Lkb... is not executable".
    (
        "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
        "Jupiter_Aggregator_v6.so",
    ),
    // AMM / CLMM / orderbook DEXes.
    ("cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG", "Meteora_DAMM_v2.so"),
    ("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", "Raydium_AMM_v4.so"),
    (
        "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
        "Raydium_Concentrated_Liquidity.so",
    ),
    ("MNFSTqtC93rEfYHB6hF82sKdZpUDFWkViLByLd1k1Ms", "Manifest.so"),
    ("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "Whirlpools_Program.so"),
    ("BSwp6bEBihVLdqJRKGgzjcGLHkcTuzmSo1TQkHepzH8p", "BonkSwap.so"),
    ("fUSioN9YKKSa3CUC2YUc4tPkHJ5Y6XW1yz8y6F7qWz9", "Fusion_AMM.so"),
    ("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", "Meteora_DLMM_Program.so"),
    ("DEXYosS6oEGvk8uCDayvwEZz4qEyDJRf9nFgYCaqPMTm", "1Dex_Program.so"),
    (
        "MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky",
        "Mercurial_Stable_Swap.so",
    ),
    ("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C", "Raydium_CPMM.so"),
    ("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP", "Orca_Token_Swap_V2.so"),
    (
        "24Uqj9JCLxUeoC3hGfh5W3s9FM9uCHDS2SG3LYwBpyTi",
        "Meteora_Vault_Program.so",
    ),
    (
        "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",
        "Meteora_Pools_Program.so",
    ),
    ("HyaB3W9q6XdA5xwpU4XnSZV94htfmbmqJXZcEbRaJutt", "Invariant_Swap.so"),
    // Previously forbidden; now allowed at user's request.
    ("ALPHAQmeA7bjrVuccPsYPiCvsi428SNwte66Srvs4pHA", "AlphaQ.so"),
    ("AQU1FRd7papthgdrwPTTq5JacJh8YtwEXaBfKU3bTz45", "Aquifer.so"),
    (
        "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq",
        "PancakeSwap.so",
    ),
    ("REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2", "Byreal_CLMM.so"),
    // PMM-style DEXes are simulated like any other registered DEX.
    ("TessVdML9pBGgG9yGks7o4HewRaXVAMuoVj4x83GLQH", "Tessera_V.so"),
    ("SoLFiHG9TfgtdUXUjWAxi3LtvYuFyDLVhBWxdMZxyCe", "SolFi.so"),
    ("SV2EYYJyRz2YhfXwXnhNAevDEui5Q6yrfyo13WtupPF", "SolFi_V2.so"),
    ("ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY", "ZeroFi.so"),
    // GoonFi V2: uses sysvar_instructions whitelist because we send real
    // Jupiter txs. Token vault accounts are lazily RPC-fetched on first sim.
    ("goonuddtQRrWqqn5nFyczVKaie28f3kDkHWkHtURSLE", "GoonFi_V2.so"),
];

pub const KNOWN_PROGRAM_FILES: &[&str] = &[
    "1Dex_Program.so",
    "AlphaQ.so",
    "Aquifer.so",
    "associated_token_account.so",
    "BonkSwap.so",
    "Byreal_CLMM.so",
    "Fusion_AMM.so",
    "GoonFi_V2.so",
    "Invariant_Swap.so",
    "Jupiter_Aggregator_v6.so",
    "Manifest.so",
    "memo.so",
    "Mercurial_Stable_Swap.so",
    "Meteora_DAMM_v2.so",
    "Meteora_DLMM_Program.so",
    "Meteora_Pools_Program.so",
    "Meteora_Vault_Program.so",
    "Orca_Token_Swap_V2.so",
    "PancakeSwap.so",
    "Raydium_AMM_v4.so",
    "Raydium_CLMM.so",
    "Raydium_Concentrated_Liquidity.so",
    "Raydium_CPMM.so",
    "SolFi.so",
    "SolFi_V2.so",
    "spl_token.so",
    "Tessera_V.so",
    "token_2022.so",
    "Whirlpools_Program.so",
    "ZeroFi.so",
];

/// Canonical simulation keys exposed in config.toml.
pub const SIMULATION_DEX_PROGRAM_IDS: &[(&str, &str)] = &[
    ("meteora_damm_v2", "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"),
    ("meteora_dlmm", "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"),
    ("raydium_amm_v4", "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"),
    ("raydium_clmm", "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"),
    ("raydium_cpmm", "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"),
    ("whirlpool", "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"),
];

pub fn normalize_dex_key(raw: &str) -> String {
    raw.trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_")
}

pub fn dex_key_from_program_id(raw: &str) -> Option<&'static str> {
    let raw = raw.trim();
    SIMULATION_DEX_PROGRAM_IDS
        .iter()
        .find_map(|(key, pid)| (*pid == raw).then_some(*key))
}

pub fn dex_key_from_label(label: &str) -> Option<&'static str> {
    let l = label.to_ascii_lowercase();

    if l.contains("meteora")
        && (l.contains("damm v2")
            || l.contains("damm_v2")
            || l.contains("dynamic amm v2")
            || l.contains("cpamm")
            || l.contains("cp amm"))
    {
        return Some("meteora_damm_v2");
    }
    if l.contains("meteora") && l.contains("dlmm") {
        return Some("meteora_dlmm");
    }
    if l.contains("raydium") && l.contains("clmm") {
        return Some("raydium_clmm");
    }
    if l.contains("raydium")
        && (l.contains("cpmm")
            || l.contains("cp-swap")
            || l.contains("cp swap")
            || l.contains("cp_swap"))
    {
        return Some("raydium_cpmm");
    }
    if l.contains("raydium") {
        return Some("raydium_amm_v4");
    }
    if l.contains("whirlpool") || l.contains("orca") {
        return Some("whirlpool");
    }

    None
}

/// Program ids the bot refuses to route through. Currently empty; every DEX in
/// `PROGRAMS` is allowed.
pub const FORBIDDEN_DEX_PROGRAM_IDS: &[&str] = &[];

/// Jupiter/Metis DEX label substrings we ask the server to exclude up-front.
/// Currently empty; every DEX label is allowed.
pub const FORBIDDEN_DEX_LABELS: &[&str] = &[];

/// Program ids that the simulator should subscribe to on Yellowstone gRPC so
/// their pool accounts end up in the hot cache. This is the superset of
/// `PROGRAMS` as strings.
pub fn all_program_ids() -> Vec<String> {
    PROGRAMS.iter().map(|(id, _)| (*id).to_string()).collect()
}

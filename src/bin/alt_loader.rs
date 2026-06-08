//! Standalone ALT pre-loader utility.
//!
//! This is a separate binary that fetches all ALT addresses used by Jupiter/Metis
//! and can be run independently from the main bot. Useful for:
//!   1. Testing ALT connectivity without running the full bot
//!   2. Pre-populating ALT cache before starting simulation
//!   3. Debugging AddressLookupTableNotFound errors
//!
//! Usage:
//!   cargo run --bin alt_loader -- <RPC_URL>
//!
//! Example:
//!   cargo run --bin alt_loader -- https://api.mainnet-beta.solana.com

use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{address_lookup_table::AddressLookupTableAccount, pubkey::Pubkey};
use std::str::FromStr;

/// Common ALTs used by Jupiter/Metis aggregator
/// These are well-known lookup tables that Jupiter uses for various token lists
const JUPITER_ALTS: &[&str] = &[
    // Jupiter Main ALT (most commonly used)
    "3AL7kDjdMADqmDc1piGtDuWzQfbFqXzV8YxTJ8G6qpump",
    // Additional Jupiter ALTs
    "GbPYrQEDYV9Bn4Kp9oT3h5sQSYFpwLyhpD7PJE8xgRHX",
    "CRaGFDCkqdPB7FnJpJfzvvRoEeT9pqxFXFoipPxZ7Jst",
    "DRpbCBLoTZy9XyZKJxMcmFA3AJR45mNqRhC5SkraShyc",
    "FTpcGfsvXwGRcqjSaBNoxNYFSYeKgQVCuKtHkRvxXtpu",
    // Add more as discovered from Metis swap-instructions responses
];

fn deserialize_alt_addresses(data: &[u8]) -> Result<Vec<Pubkey>> {
    const HEADER_SIZE: usize = 56;
    if data.len() < HEADER_SIZE {
        anyhow::bail!("ALT account data too short: {} bytes", data.len());
    }
    let addresses_data = &data[HEADER_SIZE..];
    if addresses_data.len() % 32 != 0 {
        anyhow::bail!(
            "ALT addresses data has invalid length: {} (not a multiple of 32)",
            addresses_data.len()
        );
    }
    let addresses: Vec<Pubkey> = addresses_data
        .chunks_exact(32)
        .map(|chunk| Pubkey::new_from_array(chunk.try_into().unwrap()))
        .collect();
    Ok(addresses)
}

fn load_alt(rpc: &RpcClient, alt_pubkey: &str) -> Result<AddressLookupTableAccount> {
    let pk = Pubkey::from_str(alt_pubkey)
        .with_context(|| format!("Invalid ALT pubkey: {}", alt_pubkey))?;
    
    println!("[{}] Fetching ALT account...", alt_pubkey);
    
    let account = rpc
        .get_account(&pk)
        .with_context(|| format!("Failed to fetch ALT {}", alt_pubkey))?;
    
    if !account.executable {
        println!("       WARNING: Account is not executable (might not be an ALT)");
    }
    
    let addresses = deserialize_alt_addresses(&account.data)?;
    println!("       OK - {} addresses in table", addresses.len());
    
    Ok(AddressLookupTableAccount {
        key: pk,
        addresses,
    })
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::new(
                "info,solana=warn,reqwest=warn",
            ),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <RPC_URL>", args[0]);
        eprintln!("Example: {} https://api.mainnet-beta.solana.com", args[0]);
        std::process::exit(1);
    }

    let rpc_url = &args[1];
    println!("=== ALT Loader Utility ===");
    println!("RPC: {}", rpc_url);
    println!();

    let rpc = RpcClient::new(rpc_url.to_string());

    let mut success = 0;
    let mut failed = 0;

    for alt_addr in JUPITER_ALTS {
        match load_alt(&rpc, alt_addr) {
            Ok(alt) => {
                println!("       Key: {}", alt.key);
                if alt.addresses.len() > 0 && alt.addresses.len() <= 5 {
                    println!("       First few addresses:");
                    for addr in alt.addresses.iter().take(5) {
                        println!("         - {}", addr);
                    }
                }
                success += 1;
            }
            Err(e) => {
                println!("       FAILED: {:?}", e);
                failed += 1;
            }
        }
        println!();
    }

    println!("=== Summary ===");
    println!("Success: {}, Failed: {}, Total: {}", success, failed, JUPITER_ALTS.len());
    println!();
    println!("Note: If you see AddressLookupTableNotFound errors in simulation,");
    println!("ensure these ALTs are being fetched and cached BEFORE simulation starts.");
    println!("The ALTs must be loaded into LiteSVM's account state via set_account().");

    Ok(())
}

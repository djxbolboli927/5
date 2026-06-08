use anyhow::{Context, Result};
use base64::Engine;
use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use solana_sdk::transaction::VersionedTransaction;
use tracing::{debug, info, warn};

/// Jito JSON-RPC client for bundle submission.
/// Sends bundles to MULTIPLE block engine endpoints concurrently.
pub struct JitoClient {
    http: Client,
    bundle_urls: Vec<String>,
}

#[derive(Serialize)]
struct SendBundleRpcRequest {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: (Vec<String>, SendBundleConfig),
}

#[derive(Serialize)]
struct SendBundleConfig {
    encoding: &'static str,
}

#[derive(Deserialize, Debug)]
struct RpcResponse {
    result: Option<String>,
    error: Option<RpcError>,
}

#[derive(Deserialize, Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl JitoClient {
    /// Create a Jito client that sends bundles to multiple endpoints concurrently.
    pub fn new(base_urls: &[String], uuid: &str) -> Self {
        let bundle_urls: Vec<String> = base_urls
            .iter()
            .map(|url| {
                format!(
                    "{}/api/v1/bundles?uuid={}",
                    url.trim_end_matches('/'),
                    uuid
                )
            })
            .collect();

        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(1))
            // 8 regional endpoints × concurrent bundle sends — keep 16
            // idle connections per region so consecutive sends reuse the
            // warm TLS session instead of paying ~30ms handshake cost.
            .pool_max_idle_per_host(16)
            .tcp_nodelay(true)
            .build()
            .expect("failed to build http client");

        info!(
            endpoints = bundle_urls.len(),
            "Jito multi-region client initialized"
        );

        Self { http, bundle_urls }
    }

    /// Send a single-transaction bundle to ALL Jito endpoints concurrently.
    /// Returns the first successful bundle ID.
    pub async fn send_bundle(&self, tx: &VersionedTransaction) -> Result<String> {
        let tx_bytes = bincode::serialize(tx).context("failed to serialize transaction")?;
        let tx_base64 = base64::engine::general_purpose::STANDARD.encode(&tx_bytes);

        // Send to all endpoints concurrently, but release the worker as soon as
        // any region accepts. Dropping the remaining futures cancels slow tails.
        let mut futures = self
            .bundle_urls
            .iter()
            .map(|url| self.send_to_endpoint(url, &tx_base64))
            .collect::<FuturesUnordered<_>>();
        let mut last_err = None;

        while let Some(result) = futures.next().await {
            match result {
                Ok(bundle_id) => return Ok(bundle_id),
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no Jito endpoints configured")))
    }

    /// Send bundle to a single endpoint.
    async fn send_to_endpoint(&self, url: &str, tx_base64: &str) -> Result<String> {
        let request = SendBundleRpcRequest {
            jsonrpc: "2.0",
            id: 1,
            method: "sendBundle",
            params: (
                vec![tx_base64.to_string()],
                SendBundleConfig { encoding: "base64" },
            ),
        };

        let resp = self
            .http
            .post(url)
            .json(&request)
            .send()
            .await
            .with_context(|| format!("Jito sendBundle to {} failed", url))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            warn!(endpoint = url, http_status = %status, body = %body, "Jito HTTP error");
            anyhow::bail!("Jito HTTP error at {}: {} -- {}", url, status, body);
        }

        let rpc_resp: RpcResponse = resp
            .json()
            .await
            .context("failed to parse Jito response")?;

        if let Some(err) = rpc_resp.error {
            warn!(endpoint = url, code = err.code, message = %err.message, "Jito RPC error");
            anyhow::bail!(
                "Jito RPC error at {}: code={}, message={}",
                url,
                err.code,
                err.message
            );
        }

        let bundle_id = rpc_resp
            .result
            .ok_or_else(|| anyhow::anyhow!("Jito returned no result and no error"))?;

        debug!(endpoint = url, bundle_id = %bundle_id, "bundle accepted");
        Ok(bundle_id)
    }
}

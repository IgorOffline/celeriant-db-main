//! Protocol dictionary ceiling against a real server.
//!
//! Contract: the ceiling is 1 MiB. A server whose pre-staged dictionary is
//! larger refuses to start, naming the ceiling; exactly 1 MiB starts. The
//! client reads the Identify handshake under that protocol ceiling, not under
//! the pool's `max_response_size`, so a pool with a tiny response cap still
//! connects to a server holding a large dictionary.
//!
//! A custom dictionary is pre-staged by writing `dictionary.zstd_dict` into an
//! empty data root: with no `server_meta.toml`, first boot adopts the file.

use std::collections::HashSet;
use std::time::Duration;

use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_client_tokio::pool::{CeleriantPool, PoolOptions};
use celeriant_client_tokio::watch_connection::WatchOptions;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{ReadRequest, WatchRequest};
use tempfile::TempDir;

use crate::common::{R, event, port_for, unique_key};
use crate::{ServerConfig, ServerConfigExt, TestServer};

const CEILING: usize = 1_048_576;
const TINY_RESPONSE_CAP: u64 = 4096;

/// Deterministic noise. Content-only zstd dictionaries accept any bytes that do
/// not open with the zstd dictionary magic (0xEC30A437, little-endian 37 A4 30 EC).
fn dictionary_bytes(len: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut out: Vec<u8> = (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    out[0] = b'D';
    out
}

fn data_root_with_dictionary(len: usize) -> Result<TempDir, Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    std::fs::write(dir.path().join("dictionary.zstd_dict"), dictionary_bytes(len))?;
    Ok(dir)
}

fn test_config() -> ServerConfig {
    ServerConfig { num_shards: Some(1), log_level: "warn".to_string(), standalone: true, ..Default::default() }
}

/// The server binary on `root`, for tests that watch it exit instead of serving.
fn server_command(root: &std::path::Path, port: u16) -> tokio::process::Command {
    let mut config = test_config();
    config.data_root = root.to_path_buf();
    config.client_port = port;
    config.replication_port = port + 1;
    config.metrics_port = port + 2;
    let mut cmd = tokio::process::Command::new(TestServer::server_binary_path());
    cmd.args(config.to_cli_args()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped()).kill_on_drop(true);
    cmd
}

async fn start_with_dictionary(seed: &str, len: usize) -> Result<TestServer, Box<dyn std::error::Error>> {
    let dir = data_root_with_dictionary(len)?;
    TestServer::start_with_existing_dir(port_for(seed), test_config(), seed.to_string(), dir).await
}

/// Connect, write, read back and open a watch through a pool with response cap
/// `cap`. Every data frame here is small.
async fn pool_round_trip(server: &TestServer, seed: &str, cap: u64) -> R {
    let pool = CeleriantPool::new(
        PoolOptions::new(server.address().to_string()).with_max_response_size(cap),
    );
    let key = unique_key(seed);
    pool.write_events_with(
        key.clone(),
        vec![event(1, 100, 1001, "{\"dict\":1}")],
        0,
        WriteEventsOptions { allow_create: true, ..Default::default() },
    )
    .await
    .map_err(|e| format!("write through a {cap}-byte-cap pool must succeed: {e:?}"))?;

    let resp = pool
        .read(ReadRequest { correlation_id: None, aggregate_key: key.clone(), filters: ReadFilters::new(1) })
        .await
        .map_err(|e| format!("read through a {cap}-byte-cap pool must succeed: {e:?}"))?;
    let events: usize = resp.event_batches.iter().map(|b| b.events.len()).sum();
    if events != 1 {
        return Err(format!("expected the 1 written event back, got {events}").into());
    }

    let watch = WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: Some(HashSet::from([key.org_id])),
        aggregate_types: None,
        aggregates: None,
        operation_types: None,
    };
    pool.watch(watch, WatchOptions::default())
        .await
        .map_err(|e| format!("watch handshake through a {cap}-byte-cap pool must succeed: {e:?}"))?;
    Ok(())
}

/// A custom dictionary just under the ceiling and a pool
/// capped at 4096 bytes. The handshake is not data and must not hit the cap.
pub async fn tiny_cap_pool_against_large_dictionary() -> R {
    let seed = "dictionary_ceiling_tiny_cap_pool";
    let server = start_with_dictionary(seed, 1_000_000).await?;
    pool_round_trip(&server, seed, TINY_RESPONSE_CAP).await
}

/// Exactly 1 MiB starts, and the handshake carrying it fits the
/// client's protocol limit even through a tiny-cap pool.
pub async fn exact_ceiling_dictionary_starts() -> R {
    let seed = "dictionary_ceiling_exact";
    let server = start_with_dictionary(seed, CEILING)
        .await
        .map_err(|e| format!("a dictionary of exactly {CEILING} bytes must be allowed to start: {e}"))?;
    pool_round_trip(&server, seed, TINY_RESPONSE_CAP).await
}

/// One byte past the ceiling. The server must exit non-zero on its own and
/// name the ceiling in its output.
pub async fn oversize_dictionary_refuses_start() -> R {
    let dir = data_root_with_dictionary(CEILING + 1)?;
    let child = server_command(dir.path(), port_for("dictionary_ceiling_oversize")).spawn()?;

    let output = match tokio::time::timeout(Duration::from_secs(20), child.wait_with_output()).await {
        Ok(out) => out?,
        Err(_) => {
            return Err(format!(
                "server with a {}-byte dictionary was still running after 20s; it must refuse to start",
                CEILING + 1
            )
            .into());
        }
    };

    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if output.status.success() {
        return Err(format!("server exited 0 with an oversize dictionary; output:\n{text}").into());
    }
    let lower = text.to_lowercase();
    let names_ceiling = lower.contains("1048576") || lower.contains("1 mib") || lower.contains("1mib");
    if !names_ceiling {
        return Err(format!(
            "server exited {} but its output does not name the {CEILING}-byte ceiling:\n{text}",
            output.status
        )
        .into());
    }
    println!("  refused as expected ({}): {}", output.status, text.lines().last().unwrap_or(""));
    Ok(())
}

/// Control: with the default response cap, a random-byte
/// dictionary of 1,000,000 bytes and of exactly 1 MiB serve a full round trip.
/// Proves the pre-staged noise is a usable zstd dictionary, so the tiny-cap
/// cases fail on the cap and nothing else.
pub async fn control_default_cap_round_trip() -> R {
    for (seed, len) in [("dictionary_ceiling_control_1m", 1_000_000), ("dictionary_ceiling_control_mib", CEILING)] {
        let server = start_with_dictionary(seed, len).await?;
        pool_round_trip(&server, seed, 64 * 1024 * 1024).await.map_err(|e| format!("[{len} bytes] {e}"))?;
    }
    Ok(())
}

/// A data root from before the ceiling: `server_meta.toml` already pins an oversize
/// dictionary's sha. First boot never gets that far, so only the restart load can refuse it.
pub async fn restart_with_pinned_oversize_refused() -> R {
    let dir = TempDir::new()?;
    let dict_path = dir.path().join("dictionary.zstd_dict");
    let meta_path = dir.path().join("server_meta.toml");

    // First boot with a legal dictionary writes server_meta.toml.
    let legal = dictionary_bytes(CEILING);
    std::fs::write(&dict_path, &legal)?;
    let mut first = server_command(dir.path(), port_for("dictionary_ceiling_restart_a")).spawn()?;
    for _ in 0..100 {
        if meta_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    first.kill().await?;
    let meta = std::fs::read_to_string(&meta_path).map_err(|e| format!("first boot wrote no server_meta.toml: {e}"))?;

    // Rewrite the data root as an older build would have left it: an oversize dict whose sha the meta pins.
    let oversize = dictionary_bytes(CEILING + 1);
    let pinned = meta.replace(&celeriant_crypto::sha256_hex(&legal), &celeriant_crypto::sha256_hex(&oversize));
    if pinned == meta {
        return Err(format!("server_meta.toml does not hold the legal dict's sha:\n{meta}").into());
    }
    std::fs::write(&meta_path, pinned)?;
    std::fs::write(&dict_path, &oversize)?;

    let child = server_command(dir.path(), port_for("dictionary_ceiling_restart_b")).spawn()?;
    let output = match tokio::time::timeout(Duration::from_secs(20), child.wait_with_output()).await {
        Ok(out) => out?,
        Err(_) => {
            return Err(format!(
                "restart with server_meta.toml pinning a {}-byte dictionary was still running after 20s",
                CEILING + 1
            )
            .into());
        }
    };
    let text = format!("{}{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
    if output.status.success() || !text.contains("1048576") {
        return Err(format!("expected a non-zero exit naming 1048576, got {}:\n{text}", output.status).into());
    }
    println!("  refused as expected ({}): {}", output.status, text.lines().last().unwrap_or(""));
    Ok(())
}

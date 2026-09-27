//! Internode dictionary agreement against a real two-node cluster.
//!
//! Contract:
//! - Leader and follower on the same dictionary replicate, and every
//!   `ReplicationBatch` frame on the wire is `ZstdDict`.
//! - Leader and follower on different dictionaries is a configuration
//!   error. It must be refused at connect or on the first batch, with an error
//!   naming both shas. No write may be acked that the follower cannot hold, and
//!   a follower read must never return bytes other than those written.
//! - A node restarted with a different dictionary file in its existing data
//!   root refuses to start (`server_meta` sha check).
//!
//! The follower's advertised replication address points at a sniffing proxy
//! that parses leader-to-follower frames, so the tests see each frame's
//! compression byte and how often the leader reconnects.
//!
//! A custom dictionary is pre-staged by writing `dictionary.zstd_dict` into an
//! empty data root: with no `server_meta.toml`, first boot adopts the file.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wire::network::wire_header::WIRE_HEADER_SIZE;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::common::{R, event, flatten, port_for, read_all, unique_key};
use crate::{MinioContainer, ServerConfig, TestServer, s3_cluster_config, wait_for_leader};

const DICT_FILE: &str = "dictionary.zstd_dict";
const CUSTOM_DICT_LEN: usize = 64 * 1024;
const REPLICATION_BATCH_TYPE: u32 = 100;
const ZSTD_DICT_BYTE: u8 = 1;
const EVENTS: u64 = 5;
const WRITE_TIMEOUT: Duration = Duration::from_secs(20);
const OBSERVE_WINDOW: Duration = Duration::from_secs(20);

#[derive(Clone, Copy, Debug)]
pub(crate) enum Dict {
    BuiltIn,
    CustomA,
    CustomB,
}

/// Payloads share text with custom dictionary A, so compressed frames carry
/// back-references into it and a wrong dictionary cannot decode them by luck.
fn payload(i: u64) -> String {
    format!(
        r#"{{"tenant":"acme-orbital","ledger":"custodial-reserve","entry":{i},"memo":"quarterly settlement of the orbital custody ledger"}}"#
    )
}

/// Content-only dictionaries of equal length. Neither opens with the zstd
/// dictionary magic, so neither carries a dictionary id in its frames.
fn custom_dictionary(dict: Dict) -> Vec<u8> {
    let mut out = Vec::with_capacity(CUSTOM_DICT_LEN + 256);
    let mut i = 0u64;
    while out.len() < CUSTOM_DICT_LEN {
        let line = match dict {
            Dict::CustomB => format!(
                r#"{{"lighthouse":"northern-cape","keeper":"relief-rota","watch":{i},"note":"fog signal tested and lamp lens polished at dusk"}}"#
            ),
            _ => payload(i),
        };
        out.extend_from_slice(line.as_bytes());
        i += 1;
    }
    out.truncate(CUSTOM_DICT_LEN);
    out
}

fn data_root(dict: Dict) -> Result<TempDir, Box<dyn std::error::Error>> {
    let dir = TempDir::new()?;
    if !matches!(dict, Dict::BuiltIn) {
        std::fs::write(dir.path().join(DICT_FILE), custom_dictionary(dict))?;
    }
    Ok(dir)
}

/// The sha a node pinned in `server_meta.toml` at first boot.
pub(crate) fn pinned_sha(server: &TestServer) -> Result<String, Box<dyn std::error::Error>> {
    let meta = std::fs::read_to_string(server.config().data_root.join("server_meta.toml"))?;
    meta.lines()
        .find_map(|l| l.trim().strip_prefix("dictionary_sha256 = "))
        .map(|v| v.trim_matches('"').to_string())
        .ok_or_else(|| "server_meta.toml has no dictionary_sha256".into())
}

#[derive(Default, Clone, Copy, Debug)]
struct SniffStats {
    connections: u64,
    replication_zstd_dict: u64,
    replication_other: u64,
    fixed_frames: u64,
}

/// Forwards to `target`, parsing leader-to-follower frames (17-byte header,
/// then `compressed_length` body bytes) and counting them by type and
/// compression byte. Follower-to-leader bytes pass through untouched.
struct FrameSniffer {
    address: String,
    stats: Arc<Mutex<SniffStats>>,
    accept: JoinHandle<()>,
}

impl FrameSniffer {
    async fn start(target: String) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let stats: Arc<Mutex<SniffStats>> = Default::default();
        let shared = stats.clone();
        let accept = tokio::spawn(async move {
            while let Ok((inbound, _)) = listener.accept().await {
                let Ok(outbound) = TcpStream::connect(&target).await else { continue };
                if let Ok(mut s) = shared.lock() {
                    s.connections += 1;
                }
                let _ = inbound.set_nodelay(true);
                let _ = outbound.set_nodelay(true);
                let (mut in_r, mut in_w) = inbound.into_split();
                let (mut out_r, mut out_w) = outbound.into_split();
                tokio::spawn(async move {
                    let _ = tokio::io::copy(&mut out_r, &mut in_w).await;
                });
                let stats = shared.clone();
                tokio::spawn(async move {
                    let mut header = [0u8; WIRE_HEADER_SIZE];
                    let mut body = Vec::new();
                    loop {
                        if in_r.read_exact(&mut header).await.is_err() {
                            break;
                        }
                        let message_type = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
                        let len = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
                        body.resize(len, 0);
                        if in_r.read_exact(&mut body).await.is_err() {
                            break;
                        }
                        if let Ok(mut s) = stats.lock() {
                            match (message_type, header[16]) {
                                (REPLICATION_BATCH_TYPE, ZSTD_DICT_BYTE) => s.replication_zstd_dict += 1,
                                (REPLICATION_BATCH_TYPE, _) => s.replication_other += 1,
                                _ => s.fixed_frames += 1,
                            }
                        }
                        if out_w.write_all(&header).await.is_err() || out_w.write_all(&body).await.is_err() {
                            break;
                        }
                    }
                    let _ = out_w.shutdown().await;
                });
            }
        });
        Ok(Self { address, stats, accept })
    }

    fn stats(&self) -> SniffStats {
        self.stats.lock().map(|s| *s).unwrap_or_default()
    }
}

impl Drop for FrameSniffer {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

pub(crate) struct Cluster {
    pub(crate) leader: TestServer,
    pub(crate) follower: TestServer,
    sniffer: FrameSniffer,
    _minio: MinioContainer,
}

fn cluster_config(minio: &MinioContainer) -> ServerConfig {
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    let mut config = s3_cluster_config(1, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);
    config.heartbeat_lease_duration_ms = 30_000;
    config.s3_lease_duration_ms = 30_000;
    config
}

/// Leader first (so it wins the lease), then the follower behind the sniffer.
pub(crate) async fn start_cluster(leader_dict: Dict, follower_dict: Dict) -> Result<Cluster, Box<dyn std::error::Error>> {
    let (leader_port, follower_port, minio_port) =
        (port_for("internode_leader"), port_for("internode_follower"), port_for("internode_minio"));
    let minio = MinioContainer::start(minio_port).await?;
    let config = cluster_config(&minio);

    let leader = TestServer::start_with_existing_dir(leader_port, config.clone(), "leader".into(), data_root(leader_dict)?).await?;
    wait_for_leader(leader.address(), Duration::from_secs(30)).await?;

    let sniffer = FrameSniffer::start(format!("127.0.0.1:{}", follower_port + 1)).await?;
    let mut follower_config = config;
    follower_config.advertised_replication_address = Some(sniffer.address.clone());
    let follower =
        TestServer::start_with_existing_dir(follower_port, follower_config, "follower".into(), data_root(follower_dict)?).await?;
    tokio::time::sleep(Duration::from_secs(8)).await;
    Ok(Cluster { leader, follower, sniffer, _minio: minio })
}

async fn metrics_text(server: &TestServer) -> String {
    let url = format!("http://127.0.0.1:{}/metrics", server.config().metrics_port);
    match reqwest::Client::builder().timeout(Duration::from_secs(2)).build() {
        Ok(c) => match c.get(&url).send().await {
            Ok(r) => r.text().await.unwrap_or_default(),
            Err(e) => format!("scrape failed: {e}"),
        },
        Err(e) => format!("scrape failed: {e}"),
    }
}

/// Non-zero replication, catchup and fallback samples, for the record.
fn replication_metrics(text: &str) -> Vec<String> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .filter(|l| {
            let name = l.split(['{', ' ']).next().unwrap_or("");
            ["replicat", "catchup", "fallback", "fence"].iter().any(|n| name.contains(n))
        })
        .filter(|l| l.rsplit(' ').next().is_some_and(|v| v.parse::<f64>().is_ok_and(|v| v != 0.0)))
        .map(str::to_string)
        .collect()
}

fn log_matches(server: &TestServer, needles: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for line in server.log_tail(400) {
        let lower = line.to_lowercase();
        if needles.iter().any(|n| lower.contains(n)) && !out.contains(&line) {
            out.push(line);
        }
    }
    out
}

const LOG_NEEDLES: &[&str] = &["warn", "error", "dict", "decompress", "sha"];

struct Observation {
    acked: Vec<u64>,
    write_errors: Vec<String>,
    follower_events: Vec<Vec<u8>>,
    follower_read_error: Option<String>,
}

/// Write `EVENTS` events through the leader, then poll the follower until it
/// holds them all or `OBSERVE_WINDOW` passes.
async fn write_and_observe(cluster: &Cluster, seed: &str) -> Result<Observation, Box<dyn std::error::Error>> {
    let key = unique_key(seed);
    let mut leader = CeleriantClient::connect(cluster.leader.address()).await?;
    let mut acked = Vec::new();
    let mut write_errors = Vec::new();
    for i in 1..=EVENTS {
        let started = Instant::now();
        let write = leader.write_events_with(key.clone(), vec![event(i, 100, 1000 + i, &payload(i))], 999, WriteEventsOptions::default());
        match tokio::time::timeout(WRITE_TIMEOUT, write).await {
            Ok(Ok(_)) => {
                println!("  write {i} acked in {:?}", started.elapsed());
                acked.push(i);
            }
            Ok(Err(e)) => write_errors.push(format!("write {i} after {:?}: {e:?}", started.elapsed())),
            Err(_) => write_errors.push(format!("write {i}: no response in {WRITE_TIMEOUT:?}")),
        }
    }

    let started = Instant::now();
    let mut follower_events = Vec::new();
    let mut follower_read_error = None;
    while started.elapsed() < OBSERVE_WINDOW {
        match CeleriantClient::connect(cluster.follower.address()).await {
            Ok(mut c) => match read_all(&mut c, &key).await {
                Ok(batches) => {
                    follower_events = flatten(&batches).iter().map(|e| e.event_value.to_vec()).collect();
                    follower_read_error = None;
                }
                Err(e) => follower_read_error = Some(e.to_string()),
            },
            Err(e) => follower_read_error = Some(format!("connect: {e:?}")),
        }
        if follower_events.len() >= acked.len() && !acked.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    println!("  follower holds {} events after {:?}", follower_events.len(), started.elapsed());
    Ok(Observation { acked, write_errors, follower_events, follower_read_error })
}

async fn report(cluster: &mut Cluster, obs: &Observation) {
    println!("--- observation ---");
    println!("  acked writes: {:?}", obs.acked);
    for e in &obs.write_errors {
        println!("  write error: {e}");
    }
    println!("  follower events: {}", obs.follower_events.len());
    if let Some(e) = &obs.follower_read_error {
        println!("  follower read error: {e}");
    }
    println!("  sniffer: {:?}", cluster.sniffer.stats());
    println!("  leader alive: {:?}", cluster.leader.check_alive());
    println!("  follower alive: {:?}", cluster.follower.check_alive());
    for (name, server) in [("leader", &cluster.leader), ("follower", &cluster.follower)] {
        for m in replication_metrics(&metrics_text(server).await) {
            println!("  [{name} metric] {m}");
        }
        let lines = log_matches(server, LOG_NEEDLES);
        println!("  [{name} log] {} matching lines, last 15:", lines.len());
        for l in lines.iter().skip(lines.len().saturating_sub(15)) {
            println!("    {l}");
        }
    }
}

/// Every event the follower returns must be one the client wrote, byte for byte.
fn check_no_garbage(values: &[Vec<u8>]) -> R {
    for (idx, value) in values.iter().enumerate() {
        let expected = payload(idx as u64 + 1);
        if value.as_slice() != expected.as_bytes() {
            return Err(format!(
                "follower returned event {} as {:?}, written as {expected:?}",
                idx + 1,
                String::from_utf8_lossy(value)
            )
            .into());
        }
    }
    Ok(())
}

pub(crate) async fn follower_values(address: &str, key: &celeriant_wal::aggregate_key::AggregateKey) -> Result<Vec<Vec<u8>>, String> {
    let mut c = CeleriantClient::connect(address).await.map_err(|e| format!("connect: {e:?}"))?;
    let batches = read_all(&mut c, key).await.map_err(|e| e.to_string())?;
    Ok(flatten(&batches).iter().map(|e| e.event_value.to_vec()).collect())
}

/// One mismatched pairing. Expects a refusal naming both shas, no ack the follower
/// cannot hold, no garbage. After observing, the leader is killed to see
/// whether the acked writes survive promotion of the mismatched follower.
/// Every violation is collected so one run records them all.
async fn mismatched(leader_dict: Dict, follower_dict: Dict, seed: &str) -> R {
    let mut cluster = start_cluster(leader_dict, follower_dict).await?;
    let leader_sha = pinned_sha(&cluster.leader)?;
    let follower_sha = pinned_sha(&cluster.follower)?;
    println!("  leader {leader_dict:?} sha {leader_sha}\n  follower {follower_dict:?} sha {follower_sha}");
    if leader_sha == follower_sha {
        return Err("harness: both nodes pinned the same sha".into());
    }
    println!("  before writes, sniffer: {:?}", cluster.sniffer.stats());

    let obs = write_and_observe(&cluster, seed).await?;
    report(&mut cluster, &obs).await;

    let mut violations: Vec<String> = Vec::new();
    if let Err(e) = check_no_garbage(&obs.follower_events) {
        violations.push(format!("before failover: {e}"));
    }
    let named = |s: &TestServer| s.log_tail(400).iter().any(|l| l.contains(&leader_sha) && l.contains(&follower_sha));
    if !named(&cluster.leader) && !named(&cluster.follower) {
        violations.push(format!(
            "no log line on either node names both shas ({leader_sha}, {follower_sha}); a dictionary mismatch must be refused as a config error"
        ));
    }

    // These 194-byte payloads stay uncompressed; the inline ZstdDict case is in dictionary_s3_divergence.rs.
    println!("--- failover: killing the leader ---");
    cluster.leader.stop();
    match wait_for_leader(cluster.follower.address(), Duration::from_secs(60)).await {
        Err(e) => violations.push(format!("follower never promoted after the leader died: {e}")),
        Ok(()) => match follower_values(cluster.follower.address(), &unique_key(seed)).await {
            Err(e) => violations.push(format!("promoted follower read failed: {e}")),
            Ok(values) => {
                println!("  promoted follower holds {} events", values.len());
                if let Err(e) = check_no_garbage(&values) {
                    violations.push(format!("after failover: {e}"));
                }
                if values.len() < obs.acked.len() {
                    violations.push(format!(
                        "after failover the new leader holds {} of {} acked writes",
                        values.len(),
                        obs.acked.len()
                    ));
                }
            }
        },
    }
    for (name, server) in [("follower", &cluster.follower)] {
        for l in log_matches(server, LOG_NEEDLES).iter().rev().take(10).rev() {
            println!("  [{name} log after failover] {l}");
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(format!("acked={:?}\n  - {}", obs.acked, violations.join("\n  - ")).into())
    }
}

/// Leader on the built-in dictionary, follower on a pre-staged custom one.
pub async fn builtin_leader_custom_follower() -> R {
    mismatched(Dict::BuiltIn, Dict::CustomA, "internode_builtin_leader").await
}

/// Leader on a custom dictionary, follower on the built-in.
pub async fn custom_leader_builtin_follower() -> R {
    mismatched(Dict::CustomA, Dict::BuiltIn, "internode_custom_leader").await
}

/// Two content-only dictionaries of equal length: no dictionary id in any
/// frame, so zstd itself has nothing to reject on.
pub async fn two_custom_dictionaries() -> R {
    mismatched(Dict::CustomA, Dict::CustomB, "internode_two_custom").await
}

/// The same custom dictionary on both nodes. The follower holds every
/// write byte for byte, and every ReplicationBatch frame the sniffer saw was
/// ZstdDict.
pub async fn same_dictionary_replicates_zstd_dict() -> R {
    let mut cluster = start_cluster(Dict::CustomA, Dict::CustomA).await?;
    let obs = write_and_observe(&cluster, "internode_same").await?;
    report(&mut cluster, &obs).await;

    if !obs.write_errors.is_empty() || obs.follower_events.len() != EVENTS as usize {
        return Err(format!(
            "same dictionary must replicate all {EVENTS} writes; errors {:?}, follower holds {}",
            obs.write_errors,
            obs.follower_events.len()
        )
        .into());
    }
    check_no_garbage(&obs.follower_events)?;
    let stats = cluster.sniffer.stats();
    if stats.replication_zstd_dict == 0 || stats.replication_other != 0 {
        return Err(format!("every ReplicationBatch must be ZstdDict on the wire, saw {stats:?}").into());
    }
    Ok(())
}

/// A follower that replicated on the built-in dictionary, restarted with a
/// custom dictionary file in the same data root, refuses to start and names
/// both shas.
pub async fn restart_with_swapped_dictionary_refused() -> R {
    let mut cluster = start_cluster(Dict::BuiltIn, Dict::BuiltIn).await?;
    let obs = write_and_observe(&cluster, "internode_restart").await?;
    if obs.follower_events.len() != EVENTS as usize {
        return Err(format!("harness: follower holds {} of {EVENTS} before the swap", obs.follower_events.len()).into());
    }
    let pinned = pinned_sha(&cluster.follower)?;

    cluster.follower.stop();
    std::fs::write(cluster.follower.config().data_root.join(DICT_FILE), custom_dictionary(Dict::CustomA))?;

    match cluster.follower.restart().await {
        Ok(()) => Err("follower started with a swapped dictionary file; server_meta must refuse it".into()),
        Err(e) => {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let tail = cluster.follower.log_tail(20);
            println!("  restart refused: {e}");
            for l in &tail {
                println!("    {l}");
            }
            let names_both = tail.iter().any(|l| l.contains(&pinned) && other_sha(l, &pinned));
            if names_both {
                Ok(())
            } else {
                Err(format!("follower refused to start but no line names the pinned sha {pinned} and the on-disk sha").into())
            }
        }
    }
}

/// True when `line` carries a 64-hex-digit token other than `known`.
fn other_sha(line: &str, known: &str) -> bool {
    line.split(|c: char| !c.is_ascii_hexdigit())
        .any(|t| t.len() == 64 && t != known)
}

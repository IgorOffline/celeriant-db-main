//! Known-red tests for the dictionary at-rest divergence over S3 (celeriant-notes
//! `db/dictionary-at-rest-divergence.md`), plus same-dictionary controls.
//!
//! The hello stops `ZstdDict` frames crossing a mismatched pair, but a datablock
//! that compresses into the metablock's inline slot is stored as leader-dictionary
//! bytes inside the metablock. Replication and S3 catchup both copy the metablock
//! verbatim (`queue_replicated_entries` re-serialises only `Block` datablocks), so the
//! S3 fallback path still carries the leader's dictionary to a follower that can't
//! decode it. Payloads here serialise above the 512-byte inline threshold and
//! compress below it.

use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;

use tempfile::TempDir;

use crate::common::{R, event, port_for, unique_key};
use crate::internode_dictionary::{Dict, follower_values, pinned_sha, start_cluster};
use crate::{MinioContainer, TestServer, s3_cluster_config, wait_for_leader};

const EVENTS: u64 = 3;

fn payload(i: u64) -> String {
    let line = format!(
        r#"{{"tenant":"acme-orbital","ledger":"custodial-reserve","entry":{i},"memo":"quarterly settlement of the orbital custody ledger"}}"#
    );
    format!("[{}]", vec![line; 6].join(","))
}

/// Serialises and compresses far past the 512-byte inline slot, so it is stored as a
/// `Block` datablock; varied enough that zstd levels 1 and 19 give different sizes.
fn block_payload(i: u64) -> String {
    let rows: Vec<String> = (0..120u64)
        .map(|r| {
            let n = (i * 7919 + r * 104_729) % 1_000_003;
            format!(r#"{{"row":{r},"acct":"AC{n:07}","amt":{},"ccy":"{}","ref":"{:x}"}}"#, n % 99_991, ["AUD", "USD", "EUR", "JPY"][(n % 4) as usize], n * 2_654_435_761)
        })
        .collect();
    format!("[{}]", rows.join(","))
}

async fn write_acked(leader: &TestServer, key: &celeriant_wal::aggregate_key::AggregateKey, payload: fn(u64) -> String) -> R {
    let mut client = CeleriantClient::connect(leader.address()).await?;
    for i in 1..=EVENTS {
        let write = client.write_events_with(key.clone(), vec![event(i, 100, 1000 + i, &payload(i))], 999, WriteEventsOptions::default());
        tokio::time::timeout(Duration::from_secs(20), write).await?.map_err(|e| format!("write {i}: {e:?}"))?;
    }
    println!("  {EVENTS} writes acked ({} bytes each)", payload(1).len());
    Ok(())
}

/// Every acked write, byte for byte, from `node`.
async fn check_holds_acked(node: &TestServer, key: &celeriant_wal::aggregate_key::AggregateKey, payload: fn(u64) -> String, when: &str) -> R {
    let values = follower_values(node.address(), key).await.map_err(|e| format!("{when}: read failed: {e}"))?;
    for (idx, value) in values.iter().enumerate() {
        let expected = payload(idx as u64 + 1);
        if value.as_slice() != expected.as_bytes() {
            return Err(format!("{when}: acked event {} returned as {:?}", idx + 1, String::from_utf8_lossy(&value[..value.len().min(120)])).into());
        }
    }
    if values.len() != EVENTS as usize {
        return Err(format!("{when}: holds {} of {EVENTS} acked writes", values.len()).into());
    }
    Ok(())
}

async fn acked_writes_survive_failover(leader_dict: Dict, follower_dict: Dict, seed: &str) -> R {
    acked_survive_failover_with(leader_dict, follower_dict, seed, payload).await
}

async fn acked_survive_failover_with(leader_dict: Dict, follower_dict: Dict, seed: &str, payload: fn(u64) -> String) -> R {
    let mut cluster = start_cluster(leader_dict, follower_dict).await?;
    println!("  leader sha {}\n  follower sha {}", pinned_sha(&cluster.leader)?, pinned_sha(&cluster.follower)?);
    let key = unique_key(seed);
    write_acked(&cluster.leader, &key, payload).await?;
    cluster.leader.stop();
    wait_for_leader(cluster.follower.address(), Duration::from_secs(60)).await?;
    check_holds_acked(&cluster.follower, &key, payload, "promoted follower").await?;
    reread_cold(&mut cluster.follower, &key, payload).await
}

/// Applied entries are served from the write cache, so a Block's on-disk range is only
/// read once the cache is gone: restart the node and read again.
async fn reread_cold(node: &mut TestServer, key: &celeriant_wal::aggregate_key::AggregateKey, payload: fn(u64) -> String) -> R {
    node.stop();
    node.restart().await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    check_holds_acked(node, key, payload, "after restart (cold cache)").await
}

/// Control: same dictionary, the harness sees every acked write after failover.
pub async fn control_same_dictionary() -> R {
    acked_writes_survive_failover(Dict::CustomA, Dict::CustomA, "dictionary_s3_inline_control").await
}

pub async fn custom_leader_builtin_follower() -> R {
    acked_writes_survive_failover(Dict::CustomA, Dict::BuiltIn, "dictionary_s3_inline_custom_leader").await
}

pub async fn two_custom_dictionaries() -> R {
    acked_writes_survive_failover(Dict::CustomA, Dict::CustomB, "dictionary_s3_inline_two_custom").await
}

/// The Block path: the follower re-compresses a Block with its own dictionary
/// but keeps the leader's `metablock.compressed_size`.
pub async fn block_custom_leader_builtin_follower() -> R {
    acked_survive_failover_with(Dict::CustomA, Dict::BuiltIn, "dictionary_s3_block_custom_leader", block_payload).await
}

/// Same built-in dictionary, leader at zstd level 1, follower at 19. The hello agrees,
/// so live TCP replication runs; `route_to_s3` points the follower's advertised address
/// at a closed port so every write is acked through S3 fallback instead.
async fn level_mismatch(seed: &str, route_to_s3: bool) -> R {
    let (leader_port, follower_port, minio_port) = (port_for("dictionary_s3_level_leader"), port_for("dictionary_s3_level_follower"), port_for("dictionary_s3_level_minio"));
    let minio = MinioContainer::start(minio_port).await?;
    let (region, bucket, access_key, secret_key, endpoint, allow_http) = minio.s3_config_fields();
    let mut config = s3_cluster_config(1, &region, &bucket, &access_key, &secret_key, &endpoint, allow_http);
    config.heartbeat_lease_duration_ms = 30_000;
    config.s3_lease_duration_ms = 30_000;

    let mut leader_config = config.clone();
    leader_config.wal_compression_level = 1;
    let mut leader = TestServer::start_with_existing_dir(leader_port, leader_config, "leader".into(), TempDir::new()?).await?;
    wait_for_leader(leader.address(), Duration::from_secs(30)).await?;

    let mut follower_config = config;
    follower_config.wal_compression_level = 19;
    if route_to_s3 {
        let closed = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
        follower_config.advertised_replication_address = Some(closed.to_string());
    }
    let follower = TestServer::start_with_existing_dir(follower_port, follower_config, "follower".into(), TempDir::new()?).await?;
    tokio::time::sleep(Duration::from_secs(8)).await;

    let key = unique_key(seed);
    write_acked(&leader, &key, block_payload).await?;
    if !route_to_s3 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        check_holds_acked(&follower, &key, block_payload, "follower before failover").await?;
    }
    leader.stop();
    wait_for_leader(follower.address(), Duration::from_secs(60)).await?;
    let mut follower = follower;
    let result = match check_holds_acked(&follower, &key, block_payload, "promoted follower").await {
        Ok(()) => reread_cold(&mut follower, &key, block_payload).await,
        Err(e) => Err(e),
    };
    drop(minio);
    result
}

pub async fn block_level_mismatch_s3() -> R {
    level_mismatch("dictionary_s3_block_level_s3", true).await
}

pub async fn block_level_mismatch_tcp() -> R {
    level_mismatch("dictionary_s3_block_level_tcp", false).await
}

/// Control for the Block tests: same dictionary and level, acked Blocks survive failover.
pub async fn block_control_same_dictionary() -> R {
    acked_survive_failover_with(Dict::CustomA, Dict::CustomA, "dictionary_s3_block_control", block_payload).await
}

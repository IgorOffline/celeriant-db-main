//! A leader keeps accepting writes, and replicating them over TCP, while S3 is
//! unreachable for longer than its S3 lease.

use std::time::Duration;

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wal::aggregate_key::AggregateKey;
use crate::{
    is_leader, poll_event_count, read_all_batches, s3_cluster_config,
    MinioContainer, TcpProxy, TestServer,
};

use crate::common::{event, port_for, R};

const TYPE: u64 = 100;
/// Longer than `s3_cluster_config`'s 10s S3 lease.
const S3_OUTAGE: Duration = Duration::from_secs(20);

/// A subfolder unique to this test AND this process run (stale S3 fallback
/// objects from a prior run can interfere with catchup).
fn run_subfolder(test: &str) -> String {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("{test}-{}-{nonce}", std::process::id())
}

/// Find which of the given addresses currently accepts writes (the leader),
/// polling up to `timeout`. Returns the leader address or None on timeout.
async fn find_leader(addrs: &[&str], timeout: Duration) -> Option<String> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for a in addrs {
            if is_leader(a).await.unwrap_or(false) {
                // Let the leader<->follower replication TCP connection establish
                // before the caller's first write. Without this settle, the very
                // first write races the link-up and gets shipped to the S3
                // fallback path before the follower ever connected, which then
                // makes the follower's S3 catchup loop on a not-yet-visible
                // object.
                tokio::time::sleep(Duration::from_secs(12)).await;
                return Some(a.to_string());
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    None
}

/// Write client_seq i..=hi to `key` on `addr`, conditional on each prior version
/// (allow_create on the first). Returns Err on the first rejected write.
async fn write_seq(
    addr: &str,
    key: &AggregateKey,
    lo: u64,
    hi: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut c = CeleriantClient::connect(addr).await?;
    for i in lo..=hi {
        c.write_events_with(
            key.clone(),
            vec![event(i, TYPE, 1000 + i, &format!("{{\"n\":{i}}}"))],
            0, WriteEventsOptions { allow_create: i == 1, expected_version: Some(i - 1), ..Default::default() },
        )
        .await
        .map_err(|e| format!("write seq {i}: {e:?}"))?;
    }
    Ok(())
}

/// S3 unreachable with both nodes healthy: the leader keeps serving (writes
/// still acked via the live follower link), and acknowledged data survives the
/// outage. (durability-and-safety: "The current leader keeps serving;
/// replication to S3 backs off, replication to the follower continues";
/// leader-election-s3: "A long S3 outage stalls failover ... It does not
/// endanger acknowledged data".)
///
/// Front MinIO with a proxy and point both nodes at it. After election, block
/// the proxy: S3 is gone but leader<->follower stays up. Writes run for longer
/// than the S3 lease, so the leader must keep serving without renewing it.
pub async fn s3_outage_leader_keeps_serving() -> R {
    let pa = port_for("s3_outage_a");
    let pb = port_for("s3_outage_b");
    let pm = port_for("s3_outage_minio");
    let sub = run_subfolder("s3out");

    let _minio = MinioContainer::start_with_bucket(pm, "celeriant-test").await?;
    let s3_proxy = TcpProxy::start(pa + 6, format!("127.0.0.1:{pm}")).await?;
    let proxy_endpoint = format!("http://{}", s3_proxy.address());

    let mk = |repl_port: u16| {
        let mut cfg = s3_cluster_config(1, "us-east-1", "celeriant-test", "minioadmin", "minioadmin", &proxy_endpoint, true);
        cfg.s3_subfolder = Some(sub.clone());
        cfg.advertised_replication_address = Some(format!("127.0.0.1:{repl_port}"));
        cfg
    };
    let a = TestServer::start_with_config_labeled(pa, mk(pa + 1), "node-a".into()).await?;
    let b = TestServer::start_with_config_labeled(pb, mk(pb + 1), "node-b".into()).await?;

    let leader = find_leader(&[a.address(), b.address()], Duration::from_secs(40)).await
        .ok_or("no leader elected within 40s")?;
    let follower = if leader == a.address() { b.address().to_string() } else { a.address().to_string() };

    let key = AggregateKey::new(1, 1, 8002);
    write_seq(&leader, &key, 1, 4).await?;
    poll_event_count(&follower, &key, 4, Duration::from_secs(30)).await;

    s3_proxy.block();

    let outage = std::time::Instant::now();
    let mut acked = 4u64;
    while outage.elapsed() < S3_OUTAGE {
        acked += 1;
        write_seq(&leader, &key, acked, acked).await
            .map_err(|e| format!("leader stopped serving {:?} into the S3 outage: {e}", outage.elapsed()))?;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    if !is_leader(&leader).await? {
        return Err("leader lost leadership during the S3 outage".into());
    }
    if is_leader(&follower).await? {
        return Err("follower accepts writes during the S3 outage".into());
    }

    let mut c = CeleriantClient::connect(&leader).await?;
    let seqs: Vec<u64> = read_all_batches(&mut c, &key).await?
        .iter()
        .flat_map(|b| b.events.iter().map(|e| e.client_seq))
        .collect();
    if seqs != (1..=acked).collect::<Vec<_>>() {
        return Err(format!("during S3 outage leader holds client_seqs {seqs:?}, expected 1..={acked}").into());
    }
    poll_event_count(&follower, &key, acked as usize, Duration::from_secs(30)).await;

    s3_proxy.unblock();
    let deadline = std::time::Instant::now() + Duration::from_secs(40);
    let mut resumed = false;
    while std::time::Instant::now() < deadline {
        if write_seq(&leader, &key, acked + 1, acked + 1).await.is_ok() { resumed = true; break; }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if !resumed {
        return Err("writes did not resume after S3 recovered".into());
    }
    Ok(())
}

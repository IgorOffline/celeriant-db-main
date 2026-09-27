use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_client_tokio::client_operations::WriteEventsOptions;
use celeriant_wal::aggregate_key::AggregateKey;
use crate::{ServerConfig, TestServer};

use crate::common::{event, port_for, read_all, R};

const TYPE: u64 = 100;

/// Concurrent writers across many shards: each aggregate reads back strictly
/// ordered and gap-free, carrying only its own payloads. There is no cross-shard
/// ordering promise, so only per-aggregate order is asserted.
pub async fn per_aggregate_order_across_shards() -> R {
    let config = ServerConfig { num_shards: Some(8), standalone: true, ..Default::default() };
    let server = TestServer::start_with_config(port_for("shard_order"), config).await?;
    let addr = server.address().to_string();

    // aggregate_id routing (default), 8 shards: ids 0..n_aggs spread across shards.
    let n_aggs = 24u64;
    let k = 10u64; // batches per aggregate
    let base = 700_000u128;

    let mut handles = Vec::new();
    for a in 0..n_aggs {
        let addr = addr.clone();
        let key = AggregateKey::new(1, 1, base + a as u128);
        handles.push(tokio::spawn(async move {
            let mut c = CeleriantClient::connect(&addr).await.map_err(|e| format!("connect: {e:?}"))?;
            for i in 1..=k {
                // Each event tagged with its aggregate and its sequence, so a
                // cross-aggregate bleed or reordering is detectable in the payload.
                let payload = format!("{{\"agg\":{a},\"seq\":{i}}}");
                c.write_events_with(
                    key.clone(),
                    vec![event(i, TYPE, 1000 + i, &payload)],
                    0, WriteEventsOptions { allow_create: i == 1, expected_version: Some(i - 1), ..Default::default() },
                )
                .await
                .map_err(|e| format!("agg {a} seq {i}: {e:?}"))?;
            }
            Ok::<(), String>(())
        }));
    }
    for h in handles {
        h.await.map_err(|e| format!("join: {e}"))??;
    }

    // Read each aggregate back and assert strict per-aggregate order + isolation.
    let mut c = CeleriantClient::connect(&addr).await?;
    for a in 0..n_aggs {
        let key = AggregateKey::new(1, 1, base + a as u128);
        let batches = read_all(&mut c, &key).await?;
        if batches.len() != k as usize {
            return Err(format!("agg {a}: {} batches, expected {k}", batches.len()).into());
        }
        for (idx, b) in batches.iter().enumerate() {
            let want = idx as u64 + 1;
            if b.aggregate_version != want {
                return Err(format!("agg {a}: batch {idx} version {} != {want} (order broken)", b.aggregate_version).into());
            }
            let payload = String::from_utf8_lossy(&b.events[0].event_value);
            let expect = format!("{{\"agg\":{a},\"seq\":{want}}}");
            if payload != expect {
                return Err(format!("agg {a}: batch {idx} payload {payload:?} != {expect:?} (bleed or reorder)").into());
            }
        }
    }
    Ok(())
}

//! Reads of an aggregate larger than one response follow the cursor across pages.

use celeriant_client_tokio::celeriant_client::CeleriantClient;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;
use crate::{ServerConfig, TestServer};

use crate::common::{port_for, unique_key, R};

const TYPE: u64 = 100;

/// Following next_aggregate_version returns every batch exactly once and in
/// order. A 1 MiB response cap forces 40 x 64 KiB batches across several pages.
pub async fn pagination_streams_whole_aggregate() -> R {
    let config = ServerConfig {
        num_shards: Some(1),
        standalone: true,
        max_response_size: 1024 * 1024,
        ..Default::default()
    };
    let server = TestServer::start_with_config(port_for("pagination_streams_whole_aggregate"), config).await?;
    let mut c = CeleriantClient::connect(server.address()).await?;
    let key = unique_key("pagination_streams_whole_aggregate");

    let n = 40u64;
    for i in 1..=n {
        let mut blob = vec![0u8; 64 * 1024];
        crate::fill_incompressible(&mut blob, i);
        let ev = celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent {
            client_seq: i,
            event_seq: 0,
            event_id: None,
            event_timestamp: 1000 + i,
            event_type_major: TYPE,
            event_type_minor: 0,
            event_value: std::sync::Arc::new(blob),
            iv: None,
        };
        c.write_events(key.clone(), vec![ev], 0).await?;
    }

    let mut seen = Vec::new();
    let mut pages = 0u32;
    let mut from = 1u64;
    loop {
        let resp = c
            .read(ReadRequest { correlation_id: None, aggregate_key: key.clone(), filters: ReadFilters::new(from) })
            .await?;
        pages += 1;
        for b in &resp.event_batches {
            seen.push(b.aggregate_version);
        }
        match resp.next_aggregate_version {
            Some(next) => {
                if next <= from {
                    return Err(format!("cursor did not advance: from={from} next={next}").into());
                }
                from = next;
            }
            None => break,
        }
    }
    if pages < 2 {
        return Err(format!("{n} x 64 KiB batches fit in {pages} page under a 1 MiB response budget").into());
    }
    let expected: Vec<u64> = (1..=n).collect();
    if seen != expected {
        return Err(format!("paged versions {seen:?} != contiguous 1..={n}").into());
    }
    Ok(())
}

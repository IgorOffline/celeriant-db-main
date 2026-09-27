//! Leader-walk regressions. Each test pins routing behaviour that an earlier
//! build got wrong.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use celeriant_client_tokio::{CeleriantPool, ClientIdentityConfig, PoolOptions};
use celeriant_msg::process_identify::read_identify_request;
use celeriant_wire::network::wire_header::WireHeader;
use common::{Answer, dead_address, spawn, write_request};
use tokio_util::compat::TokioAsyncReadCompatExt;

/// Re-entering a hinted address costs one slot of `max_leader_retries` per
/// hinting follower. With four addresses the walk burned its whole budget
/// re-dialling the dead primary and never reached the live leader.
#[tokio::test]
async fn a_stale_hint_must_not_starve_an_untried_seed() {
    let primary = dead_address();
    let s1 = spawn(Answer::NotLeader(primary.clone()));
    let s2 = spawn(Answer::NotLeader(primary.clone()));
    let s3 = spawn(Answer::Ok);

    let pool = CeleriantPool::new(
        PoolOptions::new(&primary)
            .with_seed_addresses(vec![
                s1.addr.to_string(),
                s2.addr.to_string(),
                s3.addr.to_string(),
            ])
            .with_connection_timeout(Duration::from_millis(300))
            .with_request_timeout(Duration::from_secs(5)),
    );

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        pool.write(write_request(1, vec![7])),
    )
    .await
    .expect("the walk must not hang");

    assert_eq!(s3.requests(), 1, "the live leader must be tried; it never was");
    result.expect("a live leader in the seed list must serve the write");
}

/// An address that answered `NotLeader` is never re-entered in the same walk.
/// The hint is compared against `answered` *before* the current candidate is
/// pushed, so a node whose hint names itself (a stale local view during an
/// election) used to be dialled twice.
#[tokio::test]
async fn a_node_that_hints_at_itself_must_not_be_re_entered() {
    let node = common::bind();
    let addr = node.local_addr().unwrap().to_string();
    let node = common::serve(node, Answer::NotLeader(addr.clone()));

    let pool = CeleriantPool::new(
        PoolOptions::new(&addr)
            .with_connection_timeout(Duration::from_millis(300))
            .with_request_timeout(Duration::from_secs(5)),
    );

    tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(2, vec![7])))
        .await
        .expect("the walk must not hang")
        .expect_err("a node that redirects to itself cannot serve the write");

    assert_eq!(node.requests(), 1, "a definitive NotLeader must retire the address");
}

#[tokio::test]
async fn a_handshake_timeout_or_eof_is_a_pre_send_failure() {
    for (eof, explicit_identity) in [(false, false), (false, true), (true, false), (true, true)] {
        let listener = tokio::net::TcpListener::from_std(common::bind()).unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let identifies = Arc::new(AtomicUsize::new(0));
        let counted = identifies.clone();
        // Reads each Identify, then closes (EOF) or holds the socket unanswered (timeout).
        let primary = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                let mut stream = socket.compat();
                let header = WireHeader::from_reader(&mut stream, 1024 * 1024).await.unwrap();
                read_identify_request(header, &mut stream).await.unwrap();
                counted.fetch_add(1, Ordering::SeqCst);
                if !eof {
                    held.push(stream);
                }
            }
        });
        let seed = spawn(Answer::Ok);
        let mut options = PoolOptions::new(&addr)
            .with_seed_addresses(vec![seed.addr.to_string()])
            .with_connection_timeout(Duration::from_secs(5))
            .with_request_timeout(Duration::from_millis(300));
        if explicit_identity {
            options = options.with_identity(ClientIdentityConfig::from_api_key("k"));
        }
        let pool = CeleriantPool::new(options);

        tokio::time::timeout(Duration::from_secs(10), pool.write(write_request(5, vec![7])))
            .await
            .expect("the handshake failure must not hang")
            .unwrap_or_else(|error| {
                panic!("eof={eof} explicit_identity={explicit_identity}: a pre-send handshake failure must allow the healthy seed to serve the write: {error:?}")
            });
        assert_eq!(seed.requests(), 1, "the healthy seed receives the mutation exactly once");
        assert!(identifies.load(Ordering::SeqCst) > 0, "the primary must receive an Identify");
        primary.abort();
    }
}

//! Black-box contract for re-sending reads. A read changes nothing on the
//! server, so a read whose reply was lost after it was sent is safe to send
//! again, once. Writes keep the opposite rule (see `post_send_contract.rs`).

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use celeriant_client_tokio::{CeleriantPool, ClientError, PoolOptions, PooledReadAllIterator};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::ReadRequest;
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_msg::response::responses::ReadResponse;
use celeriant_wal::aggregate_key::AggregateKey;
use common::{Answer, Fake, spawn};

const PATIENCE: Duration = Duration::from_secs(10);

/// Every request a fake saw, as `node@cursor`. A non-read shows as `node@?`.
type Served = Arc<Mutex<Vec<(&'static str, Option<u64>)>>>;

fn key() -> AggregateKey {
    AggregateKey::new(1, 1, 1)
}

fn read_from(cursor: u64) -> ReadRequest {
    ReadRequest { correlation_id: None, aggregate_key: key(), filters: ReadFilters::new(cursor) }
}

fn cursor(request: &ClientRequest) -> Option<u64> {
    match request {
        ClientRequest::Read(r) => Some(r.filters.from_aggregate_version),
        _ => None,
    }
}

fn page(request: &ClientRequest, versions: &[u64], next: Option<u64>) -> Option<ClientResponse> {
    Some(ClientResponse::Read(ReadResponse {
        correlation_id: request.correlation_id(),
        event_batches: versions
            .iter()
            .map(|&aggregate_version| AggregateEventBatch {
                aggregate_version,
                client_id: 1,
                user_id: None,
                server_timestamp: 1,
                events: vec![],
            })
            .collect(),
        next_aggregate_version: next,
    }))
}

/// Serves a two-page stream: [1,2] then [3,4]. The first request for page 3,
/// across every node sharing `lost`, is read and dropped without an answer.
fn two_page_node(name: &'static str, served: Served, lost: Arc<AtomicUsize>) -> Fake {
    spawn(Answer::Script(Arc::new(move |request| {
        let from = cursor(request);
        served.lock().unwrap().push((name, from));
        match from {
            Some(1) => page(request, &[1, 2], Some(3)),
            Some(3) if lost.fetch_add(1, Ordering::SeqCst) == 0 => None,
            Some(3) => page(request, &[3, 4], None),
            _ => None,
        }
    })))
}

fn show(served: &Served) -> String {
    let served = served.lock().unwrap();
    let shown: Vec<String> = served
        .iter()
        .map(|(node, from)| format!("{node}@{}", from.map_or("?".to_string(), |c| c.to_string())))
        .collect();
    shown.join(", ")
}

fn options(address: impl Into<String>) -> PoolOptions {
    PoolOptions::new(address)
        .with_connection_timeout(Duration::from_millis(500))
        .with_request_timeout(Duration::from_secs(2))
}

/// Drains the stream, keeping the versions that arrived before any error.
async fn drain(mut stream: PooledReadAllIterator) -> (Vec<u64>, Option<ClientError>) {
    let mut versions = Vec::new();
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => versions.push(batch.aggregate_version),
            Err(err) => return (versions, Some(err)),
        }
    }
    (versions, None)
}

#[tokio::test]
async fn a_read_lost_after_it_was_sent_is_sent_again_and_the_caller_gets_the_answer() {
    let reads = Arc::new(AtomicUsize::new(0));
    let counted = reads.clone();
    let node = spawn(Answer::Script(Arc::new(move |request| {
        cursor(request)?;
        match counted.fetch_add(1, Ordering::SeqCst) {
            0 => None,
            _ => page(request, &[1], None),
        }
    })));
    let pool = CeleriantPool::new(options(node.addr.to_string()));

    let result = tokio::time::timeout(PATIENCE, pool.read(read_from(1))).await.expect("the read must not hang");

    let reads = reads.load(Ordering::SeqCst);
    let response = result.unwrap_or_else(|err| {
        panic!("the lost read must be re-sent and answered; node received it {reads} times, caller got: {err:?}")
    });
    let versions: Vec<u64> = response.event_batches.iter().map(|b| b.aggregate_version).collect();
    assert_eq!(versions, [1], "the caller must get the second copy's answer");
    assert_eq!(reads, 2, "the node received the read {reads} times; expected the lost copy plus one re-send");
    assert_eq!(node.requests(), 2, "the node saw {} requests; only the two reads were expected", node.requests());
}

#[tokio::test]
async fn a_read_that_keeps_losing_its_reply_is_sent_twice_then_reported_as_lost_after_send() {
    let node = spawn(Answer::Script(Arc::new(|_| None)));
    let pool = CeleriantPool::new(options(node.addr.to_string()));

    let err = tokio::time::timeout(PATIENCE, pool.read(read_from(1)))
        .await
        .expect("a node that loses every reply must not wedge the caller")
        .expect_err("the node never answers");

    let reads = node.requests();
    assert!(
        matches!(err, ClientError::ConnectionLostAfterSend(_)),
        "the node read every request before closing, so the loss must be typed as post-send; \
         got {err:?} after {reads} reads"
    );
    assert_eq!(reads, 2, "the node received the read {reads} times; the re-send is bounded to exactly 2");
}

#[tokio::test]
async fn a_streaming_read_that_loses_a_page_resumes_from_that_page_with_no_batch_repeated_or_skipped() {
    let served = Served::default();
    let node = two_page_node("node", served.clone(), Arc::default());
    let pool = CeleriantPool::new(options(node.addr.to_string()));

    let (versions, err) = tokio::time::timeout(PATIENCE, async {
        drain(pool.read_all(key(), Some(ReadFilters::new(1))).await.expect("the stream opens")).await
    })
    .await
    .expect("the stream must not hang");

    let seen = show(&served);
    assert!(err.is_none(), "the lost page must be re-sent; stream failed after {versions:?} with {err:?}, served: {seen}");
    assert_eq!(versions, [1, 2, 3, 4], "every batch exactly once, in order; served: {seen}");
    let cursors: Vec<Option<u64>> = served.lock().unwrap().iter().map(|(_, from)| *from).collect();
    assert_eq!(cursors, [Some(1), Some(3), Some(3)], "page 3 is asked for twice and nothing else; served: {seen}");
}

/// The page re-send is bounded like a single read's: a page that keeps losing
/// its reply is asked for twice, then the stream fails.
#[tokio::test]
async fn a_streaming_read_whose_page_keeps_losing_its_reply_asks_twice_then_fails() {
    let served = Served::default();
    let log = served.clone();
    let node = spawn(Answer::Script(Arc::new(move |request| {
        let from = cursor(request);
        log.lock().unwrap().push(("node", from));
        match from {
            Some(1) => page(request, &[1, 2], Some(3)),
            _ => None,
        }
    })));
    let pool = CeleriantPool::new(options(node.addr.to_string()));

    let (versions, err) = tokio::time::timeout(PATIENCE, async {
        drain(pool.read_all(key(), Some(ReadFilters::new(1))).await.expect("the stream opens")).await
    })
    .await
    .expect("a page that is always lost must not wedge the stream");

    let seen = show(&served);
    assert_eq!(versions, [1, 2], "only the first page arrives; served: {seen}");
    assert!(matches!(err, Some(ClientError::ConnectionLostAfterSend(_))), "expected a post-send loss, got {err:?}; served: {seen}");
    let cursors: Vec<Option<u64>> = served.lock().unwrap().iter().map(|(_, from)| *from).collect();
    assert_eq!(cursors, [Some(1), Some(3), Some(3)], "the lost page is asked for exactly twice; served: {seen}");
}

/// A cursor sent to a replica further behind would end the stream early, so
/// the re-sent page must go to the node that served the first one. Three nodes
/// because the primary is the presumed leader: two followers are needed for
/// the pool to have anything to rotate across.
#[tokio::test]
async fn a_streaming_read_that_loses_a_page_resends_it_to_the_same_follower() {
    let served = Served::default();
    let lost = Arc::new(AtomicUsize::new(0));
    let leader = two_page_node("leader", served.clone(), lost.clone());
    let b = two_page_node("b", served.clone(), lost.clone());
    let c = two_page_node("c", served.clone(), lost.clone());
    let pool = CeleriantPool::new(
        options(leader.addr.to_string())
            .with_seed_addresses(vec![b.addr.to_string(), c.addr.to_string()])
            .with_route_reads_to_followers(true)
            .with_max_connections(1),
    );

    tokio::time::timeout(PATIENCE, async {
        pool.read(read_from(1)).await.expect("first plain read");
        pool.read(read_from(1)).await.expect("second plain read");
    })
    .await
    .expect("plain reads must not hang");
    let nodes: Vec<&str> = served.lock().unwrap().iter().map(|(node, _)| *node).collect();
    assert!(
        nodes.len() == 2 && nodes[0] != nodes[1],
        "two plain reads must rotate across followers or this test proves nothing; served: {}",
        show(&served)
    );
    served.lock().unwrap().clear();

    let (versions, err) = tokio::time::timeout(PATIENCE, async {
        drain(pool.read_all(key(), Some(ReadFilters::new(1))).await.expect("the stream opens")).await
    })
    .await
    .expect("the stream must not hang");

    let seen = show(&served);
    assert!(err.is_none(), "the lost page must be re-sent; stream failed after {versions:?} with {err:?}, served: {seen}");
    assert_eq!(versions, [1, 2, 3, 4], "every batch exactly once, in order; served: {seen}");
    let served = served.lock().unwrap();
    assert!(
        served.len() == 3 && served.iter().all(|(node, _)| *node == served[0].0),
        "one stream must be served by one node, three requests: {seen}"
    );
}

/// Candidates before the last move on without a re-send; only the last gets
/// one. Leader-pinned, two nodes, both always lose the reply.
#[tokio::test]
async fn only_the_last_candidate_gets_the_re_send() {
    let first = spawn(Answer::Script(Arc::new(|_| None)));
    let last = spawn(Answer::Script(Arc::new(|_| None)));
    let pool = CeleriantPool::new(options(first.addr.to_string()).with_seed_addresses(vec![last.addr.to_string()]));

    let err = tokio::time::timeout(PATIENCE, pool.read(read_from(1)))
        .await
        .expect("nodes that lose every reply must not wedge the caller")
        .expect_err("nothing answers");

    assert!(matches!(err, ClientError::ConnectionLostAfterSend(_)), "expected a post-send loss, got {err:?}");
    assert_eq!(
        (first.requests(), last.requests()),
        (1, 2),
        "the first candidate is tried once and the last twice"
    );
}

/// A caller that bounds `next()` with a timeout cancels it. Cancelled while the
/// lost page waits for another connection, the stream reports an error on the
/// next call instead of panicking. The wait is real: a read queued for the
/// node's only slot takes it the moment the lost connection is released.
#[tokio::test]
async fn a_next_cancelled_while_waiting_to_re_send_a_page_ends_the_stream_with_an_error() {
    let lost = Arc::new(AtomicUsize::new(0));
    let node = spawn(Answer::Script(Arc::new(move |request| match cursor(request) {
        Some(1) => page(request, &[1, 2], Some(3)),
        Some(3) if lost.fetch_add(1, Ordering::SeqCst) == 0 => None,
        Some(3) => page(request, &[3, 4], None),
        _ => {
            std::thread::sleep(Duration::from_millis(500));
            page(request, &[9], None)
        }
    })));
    let pool = Arc::new(CeleriantPool::new(
        PoolOptions::new(node.addr.to_string())
            .with_max_connections(1)
            .with_connection_timeout(Duration::from_secs(5))
            .with_request_timeout(Duration::from_secs(5)),
    ));
    let mut stream = pool.read_all(key(), Some(ReadFilters::new(1))).await.expect("the stream opens");
    assert_eq!(stream.next().await.unwrap().unwrap().aggregate_version, 1);
    assert_eq!(stream.next().await.unwrap().unwrap().aggregate_version, 2);

    let other = Arc::clone(&pool);
    let queued = tokio::spawn(async move { other.read(read_from(99)).await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let cancelled = tokio::time::timeout(Duration::from_millis(200), stream.next()).await;
    assert!(cancelled.is_err(), "the re-send must still be waiting for a connection when cancelled: {cancelled:?}");

    let after = tokio::spawn(async move { stream.next().await.map(|r| r.map(|b| b.aggregate_version)) }).await;
    let _ = queued.await;
    let after = after.unwrap_or_else(|e| panic!("next() after a cancelled next() must not panic: {e:?}"));
    assert!(
        matches!(after, Some(Err(ClientError::ConnectionLostAfterSend(_)))),
        "the stream must end with the lost page's error, not silently: {after:?}"
    );
}
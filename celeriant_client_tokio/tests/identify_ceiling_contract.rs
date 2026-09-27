//! Identify handshake ceiling. The client reads the
//! IdentifyResponse under a fixed protocol limit (the 1 MiB dictionary ceiling
//! plus a small allowance), whatever the caller's `max_response_size`. A peer
//! advertising a larger IdentifyResponse must be refused from the header alone:
//! promptly, with a size error, without waiting for or allocating the body.
//!
//! Each fake answers Identify with a scripted reply, usually only a header
//! advertising more bytes than it sends, then holds the socket open.

use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use celeriant_client_tokio::watch_connection::{WatchConnection, WatchOptions};
use celeriant_client_tokio::{CeleriantPool, ClientError, PoolOptions};
use celeriant_msg::process_identify::{IDENTIFY_RESPONSE_MAX_BYTES, IDENTIFY_RESPONSE_TYPE_ID, read_identify_request};
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{ReadRequest, WatchRequest};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wire::network::wire_header::{PROTOCOL_VERSION_V4, WIRE_HEADER_SIZE, WireHeader};
use futures_lite::AsyncWriteExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

const MIB: u32 = 1024 * 1024;
const PROMPT: Duration = Duration::from_secs(3);

/// (row name, advertised IdentifyResponse length, caller's max_response_size).
/// Every advertised length is past ceiling + any small allowance. The last two
/// rows set a caller cap above the advertisement, so only the protocol limit
/// can refuse them.
const ROWS: &[(&str, u32, u64)] = &[
    ("64 MiB, default caller cap", 64 * MIB, 64 * 1024 * 1024),
    ("2 MiB, default caller cap", 2 * MIB, 64 * 1024 * 1024),
    ("64 MiB, caller cap 1 GiB", 64 * MIB, 1024 * 1024 * 1024),
    ("u32::MAX, caller cap u64::MAX", u32::MAX, u64::MAX),
];

/// Answers every Identify with `reply` verbatim, then holds the socket open.
fn scripted_identify_server(reply: Vec<u8>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        while let Ok((socket, _)) = listener.accept().await {
            let reply = reply.clone();
            tokio::spawn(async move {
                let mut stream = socket.compat();
                let Ok(header) = WireHeader::from_reader(&mut stream, u64::MAX).await else {
                    return;
                };
                if read_identify_request(header, &mut stream).await.is_err() {
                    return;
                }
                let _ = stream.write_all(&reply).await;
                let _ = stream.flush().await;
                std::future::pending::<()>().await
            });
        }
    });

    addr
}

/// An uncompressed IdentifyResponse header advertising `len` body bytes.
fn identify_header(len: u32) -> Vec<u8> {
    let mut raw = vec![0u8; WIRE_HEADER_SIZE];
    raw[0..4].copy_from_slice(&PROTOCOL_VERSION_V4.to_le_bytes());
    raw[4..8].copy_from_slice(&IDENTIFY_RESPONSE_TYPE_ID.to_le_bytes());
    raw[8..12].copy_from_slice(&len.to_le_bytes());
    raw[12..16].copy_from_slice(&len.to_le_bytes());
    raw
}

fn oversize_identify_server(advertised: u32) -> SocketAddr {
    scripted_identify_server(identify_header(advertised))
}

fn read_request() -> ReadRequest {
    ReadRequest { correlation_id: None, aggregate_key: AggregateKey::new(1, 1, 1), filters: ReadFilters::new(1) }
}

fn watch_request() -> WatchRequest {
    WatchRequest {
        correlation_id: None,
        requested_latency_ms: Some(100),
        shard_id: None,
        orgs: None,
        aggregate_types: None,
        aggregates: None,
        operation_types: None,
    }
}

fn is_size_error(err: &ClientError) -> bool {
    format!("{err:?}").contains("MessageTooLarge")
}

fn check(row: &str, outcome: Result<Result<(), ClientError>, tokio::time::error::Elapsed>) -> Option<String> {
    match outcome {
        Err(_) => Some(format!("[{row}] no answer within {PROMPT:?}: the client is waiting for the advertised body")),
        Ok(Ok(())) => Some(format!("[{row}] succeeded against a fake that never sent a body")),
        Ok(Err(e)) if is_size_error(&e) => None,
        Ok(Err(e)) => Some(format!("[{row}] expected a MessageTooLarge size error, got {e:?}")),
    }
}

/// The pool's request path: the first request dials and identifies. Request and connection timeouts sit well past
/// `PROMPT`, so a hang is not rescued into a timeout error before we look.
#[tokio::test]
async fn pool_refuses_an_identify_response_past_the_ceiling() {
    let mut failures = Vec::new();
    for &(row, advertised, cap) in ROWS {
        let addr = oversize_identify_server(advertised);
        let pool = CeleriantPool::new(
            PoolOptions::new(addr.to_string())
                .with_max_response_size(cap)
                .with_connection_timeout(Duration::from_secs(30))
                .with_request_timeout(Duration::from_secs(30)),
        );
        let outcome = tokio::time::timeout(PROMPT, async { pool.read(read_request()).await.map(drop) }).await;
        failures.extend(check(row, outcome));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The standalone watch path, which runs its own Identify.
#[tokio::test]
async fn watch_connection_refuses_an_identify_response_past_the_ceiling() {
    let mut failures = Vec::new();
    for &(row, advertised, cap) in ROWS {
        let addr = oversize_identify_server(advertised);
        let options = WatchOptions {
            timeout: Some(Duration::from_secs(30)),
            max_response_size: cap,
            ..Default::default()
        };
        let outcome = tokio::time::timeout(PROMPT, async {
            WatchConnection::connect(&addr.to_string(), watch_request(), options).await.map(drop)
        })
        .await;
        failures.extend(check(row, outcome));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Control: an advertisement past both
/// the caller cap and the ceiling is refused on both paths. Proves the fake and
/// the size-error match work, so the rows above fail for the right reason.
#[tokio::test]
async fn control_refused_when_the_caller_cap_is_also_exceeded() {
    let addr = oversize_identify_server(64 * MIB);
    let pool = CeleriantPool::new(
        PoolOptions::new(addr.to_string())
            .with_max_response_size(4096)
            .with_connection_timeout(Duration::from_secs(30))
            .with_request_timeout(Duration::from_secs(30)),
    );
    let pooled = tokio::time::timeout(PROMPT, async { pool.read(read_request()).await.map(drop) }).await;

    let options = WatchOptions { timeout: Some(Duration::from_secs(30)), max_response_size: 4096, ..Default::default() };
    let watched = tokio::time::timeout(PROMPT, async {
        WatchConnection::connect(&addr.to_string(), watch_request(), options).await.map(drop)
    })
    .await;

    let failures: Vec<String> = check("control pool", pooled).into_iter().chain(check("control watch", watched)).collect();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Runs the handshake through the pool and the standalone watch, both under a 4096-byte cap.
async fn both_paths(addr: SocketAddr) -> [(&'static str, Option<Result<(), ClientError>>); 2] {
    let pool = CeleriantPool::new(
        PoolOptions::new(addr.to_string())
            .with_max_response_size(4096)
            .with_connection_timeout(Duration::from_secs(30))
            .with_request_timeout(Duration::from_secs(30)),
    );
    let pooled = tokio::time::timeout(PROMPT, async { pool.read(read_request()).await.map(drop) }).await.ok();
    let options = WatchOptions { timeout: Some(Duration::from_secs(30)), max_response_size: 4096, ..Default::default() };
    let watched = tokio::time::timeout(PROMPT, async {
        WatchConnection::connect(&addr.to_string(), watch_request(), options).await.map(drop)
    })
    .await
    .ok();
    [("pool", pooled), ("watch", watched)]
}

/// Exactly `IDENTIFY_RESPONSE_MAX_BYTES` must pass the size check even under a 4096 cap; one more byte must not.
/// The body at the limit is zeros, so the client either decodes it or fails on something other than size.
#[tokio::test]
async fn identify_bound_is_exact() {
    let limit = IDENTIFY_RESPONSE_MAX_BYTES as u32;
    let mut at_limit = identify_header(limit);
    at_limit.resize(WIRE_HEADER_SIZE + limit as usize, 0);

    let mut failures = Vec::new();
    for (path, outcome) in both_paths(scripted_identify_server(at_limit)).await {
        match outcome {
            Some(Err(e)) if is_size_error(&e) => failures.push(format!("[{path}, {limit} bytes] refused on size: {e:?}")),
            _ => {}
        }
    }
    for (path, outcome) in both_paths(oversize_identify_server(limit + 1)).await {
        match outcome {
            Some(Err(e)) if is_size_error(&e) => {}
            other => failures.push(format!("[{path}, {} bytes] expected MessageTooLarge, got {other:?}", limit + 1)),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A non-Identify reply to Identify is an ordinary ClientResponse, not the dictionary handshake, so it stays under
/// the caller's cap. A variable-size type such as Read must not borrow the Identify limit.
#[tokio::test]
async fn non_identify_reply_honours_the_caller_cap() {
    const READ_RESPONSE_TYPE_ID: u32 = 2;
    const ADVERTISED: u32 = 8 * 1024;
    let mut reply = identify_header(ADVERTISED);
    reply[4..8].copy_from_slice(&READ_RESPONSE_TYPE_ID.to_le_bytes());
    reply.resize(WIRE_HEADER_SIZE + ADVERTISED as usize, 0);

    let mut failures = Vec::new();
    for (path, outcome) in both_paths(scripted_identify_server(reply)).await {
        match outcome {
            Some(Err(e)) if is_size_error(&e) => {}
            other => failures.push(format!("[{path}] {ADVERTISED}-byte Read reply to Identify under a 4096-byte cap: expected MessageTooLarge, got {other:?}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

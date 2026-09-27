//! Public TCP contract. Run: cargo test -p celeriant_client_tokio --test occ_guard_contract
use std::sync::{Arc, Mutex};
use std::time::Duration;

use celeriant_client_tokio::{
    WatchConnection, WatchOptions, CeleriantClient, CeleriantPool, ClientError, ClientIdentityConfig, DeleteError, PoolOptions, ServerError, WriteError, WriteEventsOptions,
};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::process_identify::{IDENTIFY_REQUEST_TYPE_ID, read_identify_request, write_identify_response};
use celeriant_msg::request::requests::{WatchRequest, DeleteRequest, SingleAggregateDelete, SingleAggregateWrite, WriteRequest};
use celeriant_msg::response::responses::{
    ErrorResponse, AggregateConflict, DeleteConflictResponse, DeleteResponse, IdentifyResponse, WriteConflictResponse, WriteResponse,
};
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wire::codec::compression::DictCodec;
use celeriant_wire::network::wire_header::{WireHeader, PROTOCOL_VERSION_V4, wire_header_write_fixed_size};
use futures_util::AsyncWriteExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

const MAX: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
enum Reply {
    Success,
    Conflict { delete: bool, mismatch: bool },
}

struct Fake {
    address: String,
    seen: Arc<Mutex<Vec<(usize, u32, Option<ClientRequest>)>>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Fake {
    fn start(handshake_versions: Vec<u32>, response_version: u32, reply: Reply) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            tokio::task::LocalSet::new().block_on(&rt, async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                tokio::pin!(stopped);
                let mut connection = 0;
                loop {
                    tokio::select! {
                        _ = &mut stopped => break,
                        accepted = listener.accept() => {
                            let (socket, _) = accepted.unwrap();
                            let id = connection;
                            connection += 1;
                            let handshake_version = handshake_versions.get(id).copied().unwrap_or(4);
                            let seen = captured.clone();
                            tokio::task::spawn_local(async move {
                                let mut stream = socket.compat();
                                let codec = DictCodec::new(BUILTIN_DICT_BYTES, 3).unwrap();
                                let mut mutation = 0;
                                loop {
                                    let Ok(header) = WireHeader::from_reader(&mut stream, MAX).await else { break };
                                    let version = header.version;
                                    if header.message_type == IDENTIFY_REQUEST_TYPE_ID {
                                        let request = read_identify_request(header, &mut stream).await.unwrap();
                                        seen.lock().unwrap().push((id, version, None));
                                        let response = IdentifyResponse {
                                            correlation_id: request.correlation_id, client_id: None, access_level: None,
                                            compression_dict_sha256: None, compression_dict_bytes: None,
                                        };
                                        if write_identify_response(&mut stream, &response, handshake_version).await.is_err() { break; }
                                        continue;
                                    }
                                    let Ok(request) = ClientRequest::read_from_header(header, &mut stream, &codec).await else { break };
                                    let correlation_id = request.correlation_id();
                                    seen.lock().unwrap().push((id, version, Some(request.clone())));
                                    let (response, version) = {
                                        mutation += 1;
                                        let response = match if mutation == 1 { reply } else { Reply::Success } {
                                            Reply::Success => match request {
                                                ClientRequest::Delete(_) => ClientResponse::Delete(DeleteResponse { correlation_id }),
                                                _ => ClientResponse::Write(WriteResponse { correlation_id, max_aggregate_version: Some(8) }),
                                            },
                                            Reply::Conflict { delete, mismatch } => {
                                                let correlation_id = if mismatch { Some(correlation_id.unwrap_or(0).wrapping_add(1)) } else { correlation_id };
                                                if delete { ClientResponse::DeleteConflict(DeleteConflictResponse { correlation_id, conflicts: conflicts() }) }
                                                else { ClientResponse::WriteConflict(WriteConflictResponse { correlation_id, conflicts: conflicts() }) }
                                            }
                                        };
                                        (response, response_version)
                                    };
                                    if ClientResponse::write_response(&mut stream, &response, false, &codec, MAX, version).await.is_err() { break; }
                                }
                            });
                        }
                    }
                }
            });
        });
        Self {
            address,
            seen,
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn mutations(&self) -> usize {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, _, r)| matches!(r, Some(ClientRequest::Write(_) | ClientRequest::Delete(_))))
            .count()
    }

    async fn client(&self) -> CeleriantClient {
        CeleriantClient::connect(&self.address)
            .await
            .unwrap()
            .with_timeout(Duration::from_secs(2))
    }

    fn pool(&self) -> CeleriantPool {
        CeleriantPool::new(
            PoolOptions::new(&self.address)
                .with_max_connections(1)
                .with_request_timeout(Duration::from_secs(2)),
        )
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.thread.take().unwrap().join().unwrap();
    }
}

fn conflicts() -> Vec<AggregateConflict> {
    vec![
        AggregateConflict {
            key: AggregateKey::new(1, 2, 3),
            expected: 4,
            current: 7,
        },
        AggregateConflict {
            key: AggregateKey::new(u128::MAX, u128::MAX, u128::MAX),
            expected: u64::MAX,
            current: u64::MAX - 1,
        },
    ]
}

fn event() -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: 0,
        event_seq: 0,
        event_id: None,
        event_timestamp: 1,
        event_type_major: 1,
        event_type_minor: 0,
        event_value: Arc::new(vec![1]),
        iv: None,
    }
}

fn write() -> WriteRequest {
    WriteRequest {
        correlation_id: Some(77),
        client_id: 42,
        user_id: None,
        writes: conflicts()
            .into_iter()
            .map(|c| {
                (
                    c.key,
                    SingleAggregateWrite {
                        events: vec![event()],
                        allow_create: false,
                        expected_version: Some(c.expected),
                        enforce_client_idempotency: false,
                    },
                )
            })
            .collect(),
    }
}

fn delete() -> DeleteRequest {
    DeleteRequest {
        correlation_id: Some(77),
        client_id: 42,
        user_id: None,
        deletes: conflicts()
            .into_iter()
            .map(|c| {
                (
                    c.key,
                    SingleAggregateDelete {
                        allow_recreate: false,
                        allow_sequence_continuation: false,
                        expected_version: Some(c.expected),
                    },
                )
            })
            .collect(),
    }
}

#[tokio::test]
async fn mutations_follow_a_v4_identify_that_pins_the_response_version() {
    for (explicit, handshake, response_version) in [(false, 4, 4), (false, 5, 4), (true, 5, 4), (true, 4, 5)] {
        let fake = Fake::start(vec![handshake], response_version, Reply::Success);
        let mut client = fake.client().await;
        let identified = !explicit || client.identify(&ClientIdentityConfig::from_api_key("test-api-key")).await.is_ok();
        assert_eq!(identified, !explicit || handshake == 4, "mismatched Identify version must fail");
        if identified {
            let written = client.write(write()).await;
            assert_eq!(
                written.is_ok(),
                handshake == 4 && response_version == 4,
                "explicit={explicit} handshake={handshake} response={response_version}: {written:?}"
            );
        }
        assert_eq!(fake.mutations(), usize::from(handshake == 4));
        let seen = fake.seen.lock().unwrap();
        assert!(seen[0].2.is_none(), "first request must be Identify: {:?}", seen[0]);
        assert!(seen.iter().all(|(_, version, _)| *version == 4));
    }
}

#[tokio::test]
async fn complete_write_and_delete_conflicts_are_typed_and_consumed_without_replay() {
    for is_delete in [false, true] {
        let fake = Fake::start(
            vec![4],
            4,
            Reply::Conflict {
                delete: is_delete,
                mismatch: false,
            },
        );
        let pool = fake.pool();
        let error = if is_delete {
            pool.delete(delete()).await.unwrap_err()
        } else {
            pool.write(write()).await.unwrap_err()
        };
        match error {
            ClientError::Server(ServerError::Write {
                kind:
                    WriteError::OptimisticConcurrencyViolation {
                        conflicts: actual,
                        expected_version,
                        current_aggregate_version,
                    },
                ..
            }) if !is_delete => {
                assert_eq!(actual, conflicts());
                assert_eq!(expected_version, Some(4));
                assert_eq!(current_aggregate_version, Some(7));
            }
            ClientError::Server(ServerError::Delete {
                kind:
                    DeleteError::OptimisticConcurrencyViolation {
                        conflicts: actual,
                        expected_version,
                        current_aggregate_version,
                    },
                ..
            }) if is_delete => {
                assert_eq!(actual, conflicts());
                assert_eq!(expected_version, Some(4));
                assert_eq!(current_aggregate_version, Some(7));
            }
            error => panic!("typed OCC expected, delete={is_delete}: {error:?}"),
        }
        pool.write(write()).await.expect("fully consumed conflict leaves connection reusable");
        assert_eq!(fake.mutations(), 2, "OCC is never replayed");
        assert!(
            fake.seen.lock().unwrap().iter().all(|(connection, _, _)| *connection == 0),
            "valid OCC must reuse the same connection"
        );
    }
}

#[tokio::test]
async fn correlation_and_operation_mismatches_are_protocol_failures_without_replay() {
    for (is_delete, wrong_operation, mismatch) in [(false, false, true), (true, false, true), (false, true, false), (true, true, false)] {
        let fake = Fake::start(
            vec![4],
            4,
            Reply::Conflict {
                delete: is_delete ^ wrong_operation,
                mismatch,
            },
        );
        let pool = fake.pool();
        let error = if is_delete {
            pool.delete(delete()).await.unwrap_err()
        } else {
            pool.write(write()).await.unwrap_err()
        };
        if mismatch {
            assert!(
                matches!(
                    error,
                    ClientError::CorrelationMismatch {
                        sent: Some(77),
                        received: Some(78)
                    }
                ),
                "{error:?}"
            );
        } else {
            assert!(matches!(error, ClientError::ProtocolError), "{error:?}");
        }
        assert_eq!(fake.mutations(), 1);
    }
}

#[tokio::test]
async fn receive_capacity_is_checked_before_write_and_delete_are_sent() {
    macro_rules! submit {
        ($target:expr, $is_delete:expr, $guarded:expr) => {
            if $is_delete {
                $target.delete(delete()).await.map(|_| ())
            } else if $guarded {
                $target
                    .write_batch(42)
                    .append(AggregateKey::new(1, 2, 3), vec![event()])
                    .guard(AggregateKey::new(1, 2, 4), 0)
                    .send()
                    .await
                    .map(|_| ())
            } else {
                $target.write(write()).await.map(|_| ())
            }
        };
    }
    for pooled in [false, true] {
        for (is_delete, guarded, fits) in [(false, false, 173), (true, false, 173), (false, true, 99)] {
            for capacity in [fits - 1, fits] {
                let fake = Fake::start(vec![4], 4, Reply::Success);
                let result = if pooled {
                    let pool = CeleriantPool::new(
                        PoolOptions::new(&fake.address)
                            .with_max_response_size(capacity)
                            .with_request_timeout(Duration::from_secs(2)),
                    );
                    submit!(pool, is_delete, guarded)
                } else {
                    let mut client = fake.client().await.with_max_response_size(capacity);
                    submit!(client, is_delete, guarded)
                };
                assert_eq!(
                    result.is_ok(),
                    capacity == fits,
                    "pool={pooled}, delete={is_delete}, guarded={guarded}, capacity={capacity}, result={result:?}"
                );
                assert_eq!(fake.mutations(), usize::from(capacity == fits));
            }
        }
    }
}

#[tokio::test]
async fn replacement_pool_connections_repeat_the_handshake() {
    let fake = Fake::start(vec![4, 5], 4, Reply::Success);
    let pool = fake.pool();
    pool.write(write()).await.unwrap();
    let mut connection = pool.get_connection().await.unwrap();
    connection.mark_broken();
    drop(connection);
    assert!(pool.write(write()).await.is_err(), "replacement mismatched peer must fail before mutation");
    assert_eq!(fake.mutations(), 1);
    let seen = fake.seen.lock().unwrap();
    assert!(
        seen.iter()
            .any(|(connection, version, request)| *connection == 1 && *version == 4 && request.is_none())
    );
}

#[tokio::test]
async fn client_and_pool_builders_encode_guards_and_preserve_append_options() {
    for (pooled, guard_only) in [(false, false), (true, false), (false, true)] {
        let fake = Fake::start(vec![4], 4, Reply::Success);
        let appended = AggregateKey::new(10, 20, 30);
        let pinned = AggregateKey::new(10, 20, 31);
        let guarded = AggregateKey::new(10, 20, 32);
        let options = WriteEventsOptions {
            allow_create: false,
            expected_version: Some(5),
            enforce_client_idempotency: true,
        };
        let (pool, mut client);
        let batch = if pooled {
            pool = fake.pool();
            pool.write_batch(42)
        } else {
            client = fake.client().await;
            client.write_batch(42)
        };
        let batch = if guard_only {
            batch
        } else {
            batch.append(appended.clone(), vec![event()]).append_with(pinned.clone(), vec![event()], options)
        };
        let pin = if guard_only { 0 } else { 7 };
        batch.guard(guarded.clone(), pin).send().await.unwrap();
        let seen = fake.seen.lock().unwrap();
        let request = seen
            .iter()
            .find_map(|(_, _, r)| match r {
                Some(ClientRequest::Write(request)) => Some(request),
                _ => None,
            })
            .expect("builder submits one write");
        assert_eq!(request.client_id, 42);
        if guard_only {
            assert_eq!(request.writes.len(), 1);
        } else {
            assert_eq!(request.writes.len(), 3);
            assert_eq!(request.writes[&appended].events.len(), 1);
            let pinned = &request.writes[&pinned];
            assert_eq!(pinned.events.len(), 1);
            assert_eq!(pinned.expected_version, Some(5));
            assert!(!pinned.allow_create);
            assert!(pinned.enforce_client_idempotency);
        }
        let guard = &request.writes[&guarded];
        assert!(guard.events.is_empty());
        assert_eq!(guard.expected_version, Some(pin));
        assert!(!guard.allow_create);
        assert!(!guard.enforce_client_idempotency);
    }
}

fn unsigned_identity() -> ClientIdentityConfig {
    ClientIdentityConfig { public_key: None, private_key: None, api_key: None }
}

#[tokio::test]
async fn identify_error_correlation_is_checked_before_mapping() {
    let mut failures = Vec::new();
    for mode in ["unsigned mutation", "explicit identity", "watch"] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = socket.compat();
            let header = WireHeader::from_reader(&mut stream, MAX).await.unwrap();
            let request = read_identify_request(header, &mut stream).await.unwrap();
            assert_eq!(request.correlation_id, None);
            let error = ErrorResponse {
                correlation_id: Some(777),
                error_code: celeriant_msg::error_codes::IDENTIFY_REQUIRED,
                error_message: "different handshake".into(),
            };
            wire_header_write_fixed_size(&mut stream, &error, 7, PROTOCOL_VERSION_V4).await.unwrap();
            stream.flush().await.unwrap();
        });
        let error = if mode == "watch" {
            let request = WatchRequest {
                correlation_id: None, requested_latency_ms: None, shard_id: None,
                orgs: None, aggregate_types: None, aggregates: None, operation_types: None,
            };
            let options = WatchOptions { timeout: Some(Duration::from_secs(2)), ..Default::default() };
            match WatchConnection::connect(&address, request, options).await {
                Err(error) => error,
                Ok(_) => panic!("watch unexpectedly connected"),
            }
        } else {
            let mut client = CeleriantClient::connect(&address).await.unwrap().with_timeout(Duration::from_secs(2));
            if mode == "explicit identity" {
                client.identify(&unsigned_identity()).await.unwrap_err()
            } else {
                client.write(write()).await.unwrap_err()
            }
        };
        server.await.unwrap();
        if !matches!(error, ClientError::CorrelationMismatch { sent: None, received: Some(777) }) {
            failures.push(format!("{mode}: mismatched handshake error must not be mapped: {error:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

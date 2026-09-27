use super::*;
use crate::sharded::lease_singleflight_contract_tests::{internal_config, scratch_dir, shard_config};
use celeriant_distributed::lease_store::{LeaseStoreError, LeaseWithEtag, MembershipWithEtag};
use celeriant_msg::request::requests::{AggregateDetailsRequest, DeleteRequest, SingleAggregateDelete, SingleAggregateWrite, WriteRequest};
use celeriant_shard::{replication_client::StubReplicationClient, s3_downloader::StubS3Downloader};
use celeriant_wal::{
    aggregate_key::AggregateKey,
    datablocks::datablock_aggregate_event::DatablockAggregateEvent,
    s3::{lease::Lease, membership::Membership},
};
use futures_lite::AsyncWriteExt;
use glommio::{
    channels::channel_mesh::{Full, MeshBuilder},
    net::TcpListener,
};

struct NoLeaseStore;
impl LeaseStore for NoLeaseStore {
    async fn get_lease(&self) -> Result<Option<LeaseWithEtag>, LeaseStoreError> {
        Ok(None)
    }
    async fn put_lease_create_only(&self, _: &Lease) -> Result<String, LeaseStoreError> {
        Err(LeaseStoreError::AlreadyExists)
    }
    async fn put_lease_conditional(&self, _: &Lease, _: &str) -> Result<String, LeaseStoreError> {
        Err(LeaseStoreError::PreconditionFailed)
    }
    async fn get_membership(&self) -> Result<Option<MembershipWithEtag>, LeaseStoreError> {
        Ok(None)
    }
    async fn put_membership(&self, _: &Membership, _: Option<&str>) -> Result<(), LeaseStoreError> {
        Ok(())
    }
}
type TestContext = ConnectionContext<StubReplicationClient, StubS3Downloader, NoLeaseStore>;

async fn open_context(dir: &std::path::Path) -> TestContext {
    let mesh = MeshBuilder::<IntrashardMessages, Full>::full(1, 16);
    let (sender, _receivers) = mesh.join().await.unwrap();
    let shard_wal = ShardWal::open(
        internal_config(dir),
        ValidatedNodeStatus::create_standalone(),
        StubReplicationClient,
        StubS3Downloader,
    )
    .await
    .unwrap();
    let mut config = shard_config(dir);
    config.require_client_identity = false;
    config.replication_config = None;
    config.slow_client_timeout = Duration::from_secs(2);
    TestContext {
        config: Rc::new(config),
        current_shard_id: 0,
        intrashard_sender: Rc::new(sender),
        shutdown_requested: Rc::new(Cell::new(false)),
        shard_wal: Rc::new(shard_wal),
        catchup_completion_tx: None,
        schema_registration_pending: None,
        lease_manager: None,
        dict_codec: Rc::new(DictCodec::new(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES, 3).unwrap()),
        extension_redirect_sink: None,
        connection_gauges: Rc::new(ConnectionGauges::new(0)),
    }
}

async fn connect(ctx: TestContext, trailing: Vec<u8>) -> TcpStream {
    connect_on(ctx, trailing, PortType::Client).await
}

async fn connect_on(ctx: TestContext, trailing: Vec<u8>, port_type: PortType) -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (client, server) = futures_lite::future::zip(TcpStream::connect(address), listener.accept()).await;
    handle_new_connection(server.unwrap(), trailing, ctx, port_type);
    client.unwrap()
}
fn mutation() -> ClientRequest {
    ClientRequest::Write(WriteRequest {
        correlation_id: Some(123),
        client_id: 7,
        user_id: None,
        writes: HashMap::from([(
            AggregateKey::new(1, 1, 1),
            SingleAggregateWrite {
                events: vec![DatablockAggregateEvent {
                    client_seq: 1,
                    event_type_major: 1,
                    ..Default::default()
                }],
                allow_create: true,
                expected_version: Some(0),
                enforce_client_idempotency: false,
            },
        )]),
    })
}
async fn mutation_frame(version: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    ClientRequest::write_request(&mut bytes, &mutation(), 1024 * 1024, version).await.unwrap();
    bytes
}
async fn receive(stream: &mut TcpStream) -> Result<WireHeader, WireError> {
    glommio::timer::timeout(Duration::from_secs(3), async {
        Ok::<_, glommio::GlommioError<()>>(WireHeader::from_reader(stream, 1024 * 1024).await)
    })
    .await
    .expect("server must respond or close promptly")
}
fn identify() -> IdentifyRequest {
    IdentifyRequest {
        correlation_id: Some(124),
        public_key: None,
        nonce: None,
        signature: None,
        api_key: None,
        known_dict_sha256: None,
    }
}

/// Sends Identify as the first frame, split so `prefix_bytes` arrive as kTLS trailing bytes.
async fn open_identified(ctx: TestContext, version: u32, prefix_bytes: usize) -> TcpStream {
    let mut opening = Vec::new();
    celeriant_msg::process_identify::write_identify_request(&mut opening, &identify(), version).await.unwrap();
    let split = prefix_bytes.min(opening.len());
    let mut client = connect(ctx, opening[..split].to_vec()).await;
    client.write_all(&opening[split..]).await.unwrap();
    let header = receive(&mut client).await.unwrap();
    assert_eq!(header.version, version);
    celeriant_msg::process_identify::read_identify_response(header, &mut client).await.unwrap();
    client
}

#[test]
fn non_identify_first_frame_gets_protocol_error_then_close_and_is_not_served() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        for version in [4, 5] {
            for prefix_bytes in [0, 9, usize::MAX] {
                let dir = scratch_dir("identify-first-frame");
                let ctx = open_context(&dir).await;
                let opening = mutation_frame(version).await;
                let split = prefix_bytes.min(opening.len());
                let mut client = connect(ctx.clone(), opening[..split].to_vec()).await;
                client.write_all(&opening[split..]).await.unwrap();

                let row = format!("version {version}, prefix {prefix_bytes}");
                let header = receive(&mut client).await.unwrap_or_else(|e| panic!("{row}: {e:?}"));
                assert_eq!(header.version, version, "{row}");
                let reply = ClientResponse::read_from_header(header, &mut client).await.unwrap();
                assert!(matches!(reply, ClientResponse::ProtocolError(_)), "{row}: {reply:?}");
                assert!(receive(&mut client).await.is_err(), "{row}: connection left open");

                let details = ClientRequest::AggregateDetails(AggregateDetailsRequest { correlation_id: Some(1), aggregate_key: AggregateKey::new(1, 1, 1) });
                let state = ctx.shard_wal.process_client_request(details).await;
                assert!(!matches!(state, Ok(ClientResponse::AggregateDetails(_))), "{row}: rejected write executed: {state:?}");
                ctx.shard_wal.close().await;
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }).unwrap().join().unwrap();
}

#[test]
fn protocol_supported_connections_reuse_after_typed_conflict_and_reject_later_switch() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        for version in [4, 5] {
            for prefix_bytes in [0, 9, usize::MAX] {
                let dir = scratch_dir("supported-client-protocol");
                let ctx = open_context(&dir).await;
                let mut client = open_identified(ctx.clone(), version, prefix_bytes).await;
                client.write_all(&mutation_frame(version).await).await.unwrap();
                let header = receive(&mut client).await.unwrap();
                assert_eq!(header.version, version);
                let accepted = ClientResponse::read_from_header(header, &mut client).await.unwrap();
                assert!(matches!(accepted, ClientResponse::Write(ref reply) if reply.correlation_id == Some(123) && reply.max_aggregate_version == Some(1)), "{accepted:?}");

                client.write_all(&mutation_frame(version).await).await.unwrap();
                let header = receive(&mut client).await.unwrap();
                assert_eq!(header.message_type, 13);
                assert_eq!(header.version, version);
                let conflict = ClientResponse::read_from_header(header, &mut client).await.unwrap();
                match conflict {
                    ClientResponse::WriteConflict(reply) => {
                        assert_eq!(reply.correlation_id, Some(123));
                        assert_eq!(reply.conflicts.len(), 1);
                        assert_eq!(reply.conflicts[0].key, AggregateKey::new(1, 1, 1));
                        assert_eq!((reply.conflicts[0].expected, reply.conflicts[0].current), (0, 1));
                    }
                    other => panic!("expected typed conflict, got {other:?}"),
                }
                let delete = ClientRequest::Delete(DeleteRequest {
                    correlation_id: Some(126), client_id: 7, user_id: None,
                    deletes: HashMap::from([(AggregateKey::new(1, 1, 1), SingleAggregateDelete {
                        expected_version: Some(0), allow_recreate: false, allow_sequence_continuation: true,
                    })]),
                });
                ClientRequest::write_request(&mut client, &delete, 1024 * 1024, version).await.unwrap();
                let header = receive(&mut client).await.unwrap();
                assert_eq!(header.message_type, 14);
                assert_eq!(header.version, version);
                match ClientResponse::read_from_header(header, &mut client).await.unwrap() {
                    ClientResponse::DeleteConflict(reply) => {
                        assert_eq!(reply.correlation_id, Some(126));
                        assert_eq!(reply.conflicts.len(), 1);
                        assert_eq!(reply.conflicts[0].key, AggregateKey::new(1, 1, 1));
                        assert_eq!((reply.conflicts[0].expected, reply.conflicts[0].current), (0, 1));
                    }
                    other => panic!("expected typed delete conflict, got {other:?}"),
                }
                let details = ClientRequest::AggregateDetails(AggregateDetailsRequest { correlation_id: Some(125), aggregate_key: AggregateKey::new(1, 1, 1) });
                ClientRequest::write_request(&mut client, &details, 1024 * 1024, version).await.unwrap();
                let header = receive(&mut client).await.unwrap();
                let reused = ClientResponse::read_from_header(header, &mut client).await.unwrap();
                assert!(matches!(reused, ClientResponse::AggregateDetails(ref reply) if reply.correlation_id == Some(125) && reply.max_aggregate_version == 1), "{reused:?}");

                let mut moved = mutation();
                if let ClientRequest::Write(request) = &mut moved {
                    request.writes.values_mut().next().unwrap().expected_version = Some(1);
                }
                ClientRequest::write_request(&mut client, &moved, 1024 * 1024, if version == 4 { 5 } else { 4 }).await.unwrap();
                assert!(receive(&mut client).await.is_err(), "mid-connection version change accepted");
                let final_state = ctx.shard_wal.process_client_request(details).await.unwrap();
                assert!(matches!(final_state, ClientResponse::AggregateDetails(ref reply) if reply.max_aggregate_version == 1), "switched mutation executed: {final_state:?}");
                ctx.shard_wal.close().await;
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }).unwrap().join().unwrap();
}

async fn identify_frame(version: u32) -> Vec<u8> {
    let mut bytes = Vec::new();
    celeriant_msg::process_identify::write_identify_request(&mut bytes, &identify(), version).await.unwrap();
    bytes
}

async fn write_executed(ctx: &TestContext) -> bool {
    let details = ClientRequest::AggregateDetails(AggregateDetailsRequest { correlation_id: Some(1), aggregate_key: AggregateKey::new(1, 1, 1) });
    matches!(ctx.shard_wal.process_client_request(details).await, Ok(ClientResponse::AggregateDetails(ref r)) if r.max_aggregate_version >= 1)
}

/// A Write pipelined in the same segment behind an Identify that the server rejects
/// must never execute, whichever control rejected it and however the bytes split
/// between the kTLS prefix and the socket.
#[test]
fn pipelined_write_behind_rejected_identify_is_not_executed() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        // (row, flag, keys file, expected code)
        let cases = [("flag", true, false, error_codes::IDENTIFY_REQUIRED), ("keys file", false, true, error_codes::AUTH_REQUIRED)];
        for (name, flag, keys_file, code) in cases {
            for version in [4, 5] {
                let identify_len = identify_frame(version).await.len();
                for prefix_bytes in [0, identify_len, usize::MAX] {
                    let dir = scratch_dir("pipelined-rejected");
                    let mut ctx = open_context(&dir).await;
                    Rc::get_mut(&mut ctx.config).unwrap().require_client_identity = flag;
                    if keys_file {
                        let h = celeriant_crypto::hash_api_key(&[7u8; 32]);
                        *ctx.config.api_key_hashes.borrow_mut() = Some(std::sync::Arc::new(super::super::shard_config::ApiKeyHashes { read_write: [h; 2], read_only: [h; 2] }));
                    }
                    let mut opening = identify_frame(version).await;
                    opening.extend(mutation_frame(version).await);
                    let split = prefix_bytes.min(opening.len());
                    let mut client = connect(ctx.clone(), opening[..split].to_vec()).await;
                    client.write_all(&opening[split..]).await.unwrap();

                    let row = format!("{name}, version {version}, prefix {prefix_bytes}");
                    let header = receive(&mut client).await.unwrap_or_else(|e| panic!("{row}: {e:?}"));
                    match ClientResponse::read_from_header(header, &mut client).await.unwrap() {
                        ClientResponse::GenericError(e) => assert_eq!(e.error_code, code, "{row}"),
                        other => panic!("{row}: {other:?}"),
                    }
                    assert!(receive(&mut client).await.is_err(), "{row}: connection left open");
                    assert!(!write_executed(&ctx).await, "{row}: pipelined write executed");
                    ctx.shard_wal.close().await;
                    std::fs::remove_dir_all(dir).unwrap();
                }
            }
        }
    }).unwrap().join().unwrap();
}

/// TLS 1.3 lets the client send application data right behind its Finished, so the
/// Identify and a pipelined request can both land in the kTLS trailing buffer. Both
/// must be served.
#[test]
#[ignore = "kTLS prefix drops pipelined requests, see celeriant-notes/db/ktls-prefix-drops-pipelined-requests.md"]
fn identify_and_pipelined_request_in_ktls_prefix_are_both_served() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        for version in [4, 5] {
            let identify_len = identify_frame(version).await.len();
            // Whole Identify plus 5 bytes of the Write, and Identify plus the whole Write.
            for prefix_bytes in [identify_len + 5, usize::MAX] {
                let dir = scratch_dir("ktls-pipelined");
                let ctx = open_context(&dir).await;
                let mut opening = identify_frame(version).await;
                opening.extend(mutation_frame(version).await);
                let split = prefix_bytes.min(opening.len());
                let mut client = connect(ctx.clone(), opening[..split].to_vec()).await;
                client.write_all(&opening[split..]).await.unwrap();

                let row = format!("version {version}, prefix {prefix_bytes}");
                let header = receive(&mut client).await.unwrap_or_else(|e| panic!("{row}: {e:?}"));
                celeriant_msg::process_identify::read_identify_response(header, &mut client).await.unwrap();
                let header = receive(&mut client).await.unwrap_or_else(|e| panic!("{row}: pipelined request not served: {e:?}"));
                let reply = ClientResponse::read_from_header(header, &mut client).await.unwrap();
                assert!(matches!(reply, ClientResponse::Write(_)), "{row}: {reply:?}");
                ctx.shard_wal.close().await;
                std::fs::remove_dir_all(dir).unwrap();
            }
        }
    }).unwrap().join().unwrap();
}

/// Any non-Identify first frame gets the protocol error, including a message type
/// the server does not recognise.
#[test]
fn unknown_message_type_first_frame_gets_protocol_error() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        for version in [4u32, 5] {
            let dir = scratch_dir("unknown-first-frame");
            let ctx = open_context(&dir).await;
            let mut frame = Vec::new();
            frame.extend(version.to_le_bytes());
            frame.extend(99u32.to_le_bytes());
            frame.extend(4u32.to_le_bytes());
            frame.extend(4u32.to_le_bytes());
            frame.push(0);
            frame.extend([0u8; 4]);
            let mut client = connect(ctx.clone(), Vec::new()).await;
            client.write_all(&frame).await.unwrap();
            let header = receive(&mut client).await.unwrap_or_else(|e| panic!("version {version}: closed with no reply: {e:?}"));
            let reply = ClientResponse::read_from_header(header, &mut client).await.unwrap();
            assert!(matches!(reply, ClientResponse::ProtocolError(_)), "version {version}: {reply:?}");
            ctx.shard_wal.close().await;
            std::fs::remove_dir_all(dir).unwrap();
        }
    }).unwrap().join().unwrap();
}

/// A second Identify after the handshake is not a request: the connection closes and
/// nothing behind it is served.
#[test]
fn second_identify_closes_and_serves_nothing_behind_it() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        for version in [4, 5] {
            let dir = scratch_dir("second-identify");
            let ctx = open_context(&dir).await;
            let mut client = open_identified(ctx.clone(), version, 0).await;
            let mut again = identify_frame(version).await;
            again.extend(mutation_frame(version).await);
            client.write_all(&again).await.unwrap();
            assert!(receive(&mut client).await.is_err(), "version {version}: second Identify answered");
            assert!(!write_executed(&ctx).await, "version {version}: write behind second Identify executed");
            ctx.shard_wal.close().await;
            std::fs::remove_dir_all(dir).unwrap();
        }
    }).unwrap().join().unwrap();
}

/// When the kTLS prefix ends on a frame boundary after Identify, a dropped prefix is
/// not a close: the whole frames in it vanish and the next frame on the socket is
/// answered in their place.
#[test]
#[ignore = "kTLS prefix drops pipelined requests, see celeriant-notes/db/ktls-prefix-drops-pipelined-requests.md"]
fn ktls_prefix_request_after_identify_is_not_silently_skipped() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        for version in [4, 5] {
            let dir = scratch_dir("ktls-skipped");
            let ctx = open_context(&dir).await;
            let mut prefix = identify_frame(version).await;
            let details = ClientRequest::AggregateDetails(AggregateDetailsRequest { correlation_id: Some(1), aggregate_key: AggregateKey::new(9, 9, 9) });
            ClientRequest::write_request(&mut prefix, &details, 1024 * 1024, version).await.unwrap();
            let mut client = connect(ctx.clone(), prefix).await;
            let header = receive(&mut client).await.unwrap();
            celeriant_msg::process_identify::read_identify_response(header, &mut client).await.unwrap();
            client.write_all(&mutation_frame(version).await).await.unwrap();

            let header = receive(&mut client).await.unwrap();
            let reply = ClientResponse::read_from_header(header, &mut client).await.unwrap();
            assert_eq!(reply.correlation_id(), Some(1), "version {version}: first reply answers a later request: {reply:?}");
            ctx.shard_wal.close().await;
            std::fs::remove_dir_all(dir).unwrap();
        }
    }).unwrap().join().unwrap();
}

async fn hello_frame(dict_sha256: [u8; 32]) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_dict_hello(&mut bytes, &DictHello { dict_sha256 }, DICT_HELLO_REQUEST_TYPE_ID).await.unwrap();
    bytes
}

async fn heartbeat_frame(codec: &DictCodec) -> Vec<u8> {
    let heartbeat = ClusterRequest::Heartbeat(HeartbeatRequest { correlation_id: Some(77), shard_id: 0, leader_timestamp_ms: 1, lease_epoch: 1 });
    let mut bytes = Vec::new();
    ClusterRequest::write_request(&mut bytes, &heartbeat, celeriant_wal::compression_type::CompressionType::None, 1024 * 1024, 4, codec).await.unwrap();
    bytes
}

async fn served_heartbeat(client: &mut TcpStream, codec: &DictCodec) -> bool {
    if client.write_all(&heartbeat_frame(codec).await).await.is_err() {
        return false;
    }
    match receive(client).await {
        Ok(header) => matches!(ClusterResponse::read_from_header(header, client).await, Ok(ClusterResponse::Heartbeat(_))),
        Err(_) => false,
    }
}

fn mismatch_count(rendered: &str, role: &str) -> f64 {
    rendered
        .lines()
        .find(|l| l.starts_with("celeriant_internode_dictionary_mismatch_total") && l.contains(&format!("role=\"{role}\"")))
        .and_then(|l| l.rsplit(' ').next()?.parse().ok())
        .unwrap_or(0.0)
}

#[test]
fn replication_hello_with_the_same_sha_is_answered_and_the_connection_served() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        for prefix_bytes in [0, 9, usize::MAX] {
            let dir = scratch_dir("dict-hello-agree");
            let ctx = open_context(&dir).await;
            let local = *ctx.dict_codec.sha256();
            let opening = hello_frame(local).await;
            let split = prefix_bytes.min(opening.len());
            let mut client = connect_on(ctx.clone(), opening[..split].to_vec(), PortType::Replication).await;
            client.write_all(&opening[split..]).await.unwrap();

            let answer = read_dict_hello(&mut client, DICT_HELLO_RESPONSE_TYPE_ID).await.unwrap();
            assert_eq!(answer.dict_sha256, local, "prefix {prefix_bytes}");
            assert!(served_heartbeat(&mut client, &ctx.dict_codec).await, "prefix {prefix_bytes}: agreed connection not served");
            ctx.shard_wal.close().await;
            std::fs::remove_dir_all(dir).unwrap();
        }
    }).unwrap().join().unwrap();
}

#[test]
fn replication_hello_with_another_sha_is_answered_with_ours_counted_and_closed() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let _recording = metrics::set_default_local_recorder(&recorder);
        let dir = scratch_dir("dict-hello-mismatch");
        let ctx = open_context(&dir).await;
        let local = *ctx.dict_codec.sha256();
        let mut client = connect_on(ctx.clone(), hello_frame([0xee; 32]).await, PortType::Replication).await;

        let answer = read_dict_hello(&mut client, DICT_HELLO_RESPONSE_TYPE_ID).await.unwrap();
        assert_eq!(answer.dict_sha256, local, "the refusal carries the acceptor's sha");
        assert!(!served_heartbeat(&mut client, &ctx.dict_codec).await, "a refused connection was served");
        assert_eq!(mismatch_count(&recorder.handle().render(), "acceptor"), 1.0);
        ctx.shard_wal.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }).unwrap().join().unwrap();
}

/// A dialer that skips the hello (an old node) opens with a cluster request. It's never
/// decompressed or served.
#[test]
fn replication_connection_without_a_hello_is_closed_unserved() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        let dir = scratch_dir("dict-hello-missing");
        let ctx = open_context(&dir).await;
        let mut client = connect_on(ctx.clone(), Vec::new(), PortType::Replication).await;
        assert!(!served_heartbeat(&mut client, &ctx.dict_codec).await);
        ctx.shard_wal.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }).unwrap().join().unwrap();
}

/// Bytes after the hello in the kTLS prefix were sent before the answer, so the dialer
/// didn't wait for agreement.
#[test]
fn replication_frames_pipelined_behind_the_hello_are_refused() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        let dir = scratch_dir("dict-hello-pipelined");
        let ctx = open_context(&dir).await;
        let mut prefix = hello_frame(*ctx.dict_codec.sha256()).await;
        prefix.extend(heartbeat_frame(&ctx.dict_codec).await);
        let mut client = connect_on(ctx.clone(), prefix, PortType::Replication).await;
        assert!(receive(&mut client).await.is_err(), "no answer may follow a pipelined hello");
        ctx.shard_wal.close().await;
        std::fs::remove_dir_all(dir).unwrap();
    }).unwrap().join().unwrap();
}

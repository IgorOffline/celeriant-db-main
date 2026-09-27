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
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (client, server) = futures_lite::future::zip(TcpStream::connect(address), listener.accept()).await;
    handle_new_connection(server.unwrap(), trailing, ctx, PortType::Client);
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

#[test]
fn protocol_supported_connections_reuse_after_typed_conflict_and_reject_later_switch() {
    glommio::LocalExecutorBuilder::default().spawn(|| async {
        for version in [4, 5] {
            for identify_first in [false, true] {
                for prefix_bytes in [0, 9, usize::MAX] {
                    let dir = scratch_dir("supported-client-protocol");
                    let ctx = open_context(&dir).await;
                    let mut opening = Vec::new();
                    if identify_first {
                        celeriant_msg::process_identify::write_identify_request(&mut opening, &identify(), version).await.unwrap();
                    } else {
                        opening = mutation_frame(version).await;
                    }
                    let split = prefix_bytes.min(opening.len());
                    let mut client = connect(ctx.clone(), opening[..split].to_vec()).await;
                    client.write_all(&opening[split..]).await.unwrap();
                    if identify_first {
                        let header = receive(&mut client).await.unwrap();
                        assert_eq!(header.version, version);
                        celeriant_msg::process_identify::read_identify_response(header, &mut client).await.unwrap();
                        client.write_all(&mutation_frame(version).await).await.unwrap();
                    }
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
        }
    }).unwrap().join().unwrap();
}

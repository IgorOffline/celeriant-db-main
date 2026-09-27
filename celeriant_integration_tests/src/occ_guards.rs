//! Atomic guard and complete-conflict contracts against real server processes.
use std::error::Error;
use std::time::{Duration, Instant};

use celeriant_client_tokio::{CeleriantClient, ClientError, DeleteError, ReadError, ServerError, WriteError, WriteEventsOptions};
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::read_filters::ReadFilters;
use celeriant_msg::request::requests::{
    AggregateDetailsRequest, DeleteRequest, ReadRequest, SingleAggregateDelete, SingleAggregateWrite, WriteRequest,
};
use celeriant_msg::response::responses::AggregateConflict;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wire::network::wire_header::WireHeader;
use tokio::net::TcpStream;
use tokio_util::compat::Compat;

use crate::common::{R, event, port_for, raw_identified, read_all_from};
use crate::{MinioContainer, ServerConfig, TestServer, is_leader, s3_cluster_config};

const CLIENT: u128 = 7001;
const MAX: u64 = 64 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct Stream {
    key: AggregateKey,
    from: u64,
    events: Vec<DatablockAggregateEvent>,
}

struct History {
    streams: Vec<Stream>,
    absent: Vec<AggregateKey>,
}

fn key(id: u128) -> AggregateKey {
    AggregateKey::new(71, 23, id)
}

fn payload(label: &str, sequence: u64) -> DatablockAggregateEvent {
    let mut result = event(
        sequence,
        100,
        1_700_000_000_000 + sequence,
        &format!(
            "{{\"label\":\"{label}\",\"sequence\":{sequence},\"data\":\"{}\"}}",
            "occ-guard-payload/".repeat(100)
        ),
    );
    result.event_id = Some(0x1234_0000 + u128::from(sequence));
    result.event_type_minor = 1;
    result
}

fn append(events: Vec<DatablockAggregateEvent>, expected: Option<u64>) -> SingleAggregateWrite {
    SingleAggregateWrite {
        events,
        allow_create: true,
        expected_version: expected,
        enforce_client_idempotency: false,
    }
}

fn guard(expected: u64) -> SingleAggregateWrite {
    SingleAggregateWrite {
        events: Vec::new(),
        allow_create: false,
        expected_version: Some(expected),
        enforce_client_idempotency: false,
    }
}

fn write(entries: impl IntoIterator<Item = (AggregateKey, SingleAggregateWrite)>) -> WriteRequest {
    WriteRequest {
        correlation_id: Some(77),
        client_id: CLIENT,
        user_id: None,
        writes: entries.into_iter().collect(),
    }
}

fn delete(entries: impl IntoIterator<Item = (AggregateKey, u64)>, allow_recreate: bool) -> DeleteRequest {
    DeleteRequest {
        correlation_id: Some(78),
        client_id: CLIENT,
        user_id: None,
        deletes: entries
            .into_iter()
            .map(|(key, version)| {
                (
                    key,
                    SingleAggregateDelete {
                        allow_recreate,
                        allow_sequence_continuation: true,
                        expected_version: Some(version),
                    },
                )
            })
            .collect(),
    }
}

fn assert_write_conflicts(error: ClientError, expected: &[AggregateConflict]) {
    match error {
        ClientError::Server(ServerError::Write {
            kind:
                WriteError::OptimisticConcurrencyViolation {
                    conflicts,
                    expected_version,
                    current_aggregate_version,
                },
            ..
        }) => {
            assert_eq!(conflicts, expected);
            assert_eq!(expected_version, Some(expected[0].expected));
            assert_eq!(current_aggregate_version, Some(expected[0].current));
        }
        other => panic!("expected complete write conflicts, got {other:?}"),
    }
}

fn assert_delete_conflicts(error: ClientError, expected: &[AggregateConflict]) {
    match error {
        ClientError::Server(ServerError::Delete {
            kind: DeleteError::OptimisticConcurrencyViolation { conflicts, .. },
            ..
        }) => assert_eq!(conflicts, expected),
        other => panic!("expected complete delete conflicts, got {other:?}"),
    }
}

async fn client(address: &str) -> Result<CeleriantClient, Box<dyn Error>> {
    Ok(CeleriantClient::connect(address).await?.with_timeout(DEADLINE))
}

async fn create(c: &mut CeleriantClient, history: &mut History, key: AggregateKey, count: u64, label: &str) -> R {
    let mut events = Vec::new();
    for sequence in 1..=count {
        let event = payload(label, sequence);
        let response = c.write(write([(key.clone(), append(vec![event.clone()], Some(sequence - 1)))])).await?;
        assert_eq!(response.max_aggregate_version, Some(sequence));
        events.push(event);
    }
    history.streams.push(Stream { key, from: 1, events });
    Ok(())
}

fn equal<T: PartialEq + std::fmt::Debug>(actual: &T, expected: &T, context: &str) -> R {
    if actual != expected {
        return Err(format!("readback mismatch: {context}: actual={actual:?}, expected={expected:?}").into());
    }
    Ok(())
}

async fn capture(address: &str, history: &History) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    let mut c = client(address).await?;
    let mut result = Vec::new();
    for stream in &history.streams {
        let batches = read_all_from(&mut c, &stream.key, stream.from)
            .await
            .map_err(|error| format!("readback {:?}: {error}", stream.key))?;
        let context = format!("{:?} on {address}", stream.key);
        equal(&batches.len(), &stream.events.len(), &format!("batch count {context}"))?;
        for (index, (batch, expected)) in batches.iter().zip(&stream.events).enumerate() {
            equal(&batch.aggregate_version, &(stream.from + index as u64), &format!("version {context}"))?;
            equal(&batch.client_id, &CLIENT, &format!("client ID {context}"))?;
            equal(&batch.user_id, &None, &format!("user ID {context}"))?;
            equal(&batch.events.len(), &1, &format!("events per batch {context}"))?;
            let actual = &batch.events[0];
            equal(&actual.event_value, &expected.event_value, &format!("full payload {context}"))?;
            equal(&actual.client_seq, &expected.client_seq, &format!("client sequence {context}"))?;
            equal(&actual.event_id, &expected.event_id, &format!("event ID {context}"))?;
            equal(&actual.event_timestamp, &expected.event_timestamp, &format!("timestamp {context}"))?;
            equal(&actual.event_type_major, &expected.event_type_major, &format!("major type {context}"))?;
            equal(&actual.event_type_minor, &expected.event_type_minor, &format!("minor type {context}"))?;
            equal(&actual.iv, &expected.iv, &format!("IV {context}"))?;
        }
        result.push(bincode::encode_to_vec(&batches, bincode::config::standard())?);
    }
    for key in &history.absent {
        let response = c
            .read(ReadRequest {
                correlation_id: None,
                aggregate_key: key.clone(),
                filters: ReadFilters::new(1),
            })
            .await;
        if !matches!(
            response,
            Err(ClientError::Server(ServerError::Read {
                kind: ReadError::AggregateNotExists,
                ..
            }))
        ) {
            return Err(format!("guard/rejected append created {key:?}: {response:?}").into());
        }
    }
    Ok(result)
}

async fn wait_snapshot(address: &str, history: &History, expected: &[Vec<u8>]) -> R {
    let deadline = Instant::now() + DEADLINE;
    let mut previous = String::new();
    loop {
        let last = match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), capture(address, history)).await {
            Ok(Ok(actual)) if actual == expected => return Ok(()),
            Ok(Ok(_)) => "complete payload/metadata differs from accepted history".to_string(),
            Ok(Err(error)) => error.to_string(),
            Err(_) => format!("readback deadline expired; prior observation: {previous}"),
        };
        if last != previous {
            println!("Waiting for {address} full readback: {last}");
            previous = last.clone();
        }
        if Instant::now() >= deadline {
            return Err(format!("{address} did not converge: {last}").into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn exchange(stream: &mut Compat<TcpStream>, request: ClientRequest, version: u32) -> Result<ClientResponse, Box<dyn Error>> {
    ClientRequest::write_request(stream, &request, MAX, version)
        .await
        .map_err(|e| format!("wire: {e:?}"))?;
    let header = tokio::time::timeout(DEADLINE, WireHeader::from_reader(stream, MAX))
        .await?
        .map_err(|e| format!("header: {e:?}"))?;
    assert_eq!(header.version, version);
    let response = ClientResponse::read_from_header(header, stream)
        .await
        .map_err(|e| format!("wire: {e:?}"))?;
    assert_eq!(response.correlation_id(), request.correlation_id());
    Ok(response)
}

async fn wire_contract(address: &str, history: &mut History, conflicts: &[AggregateConflict]) -> R {
    for version in [4, 5] {
        let mut stream = raw_identified(address, version).await?;
        let entries = conflicts.iter().map(|c| (c.key.clone(), guard(c.expected)));
        match exchange(&mut stream, ClientRequest::Write(write(entries)), version).await? {
            ClientResponse::WriteConflict(response) => assert_eq!(response.conflicts, conflicts),
            response => panic!("v{version} write conflict: {response:?}"),
        }
        let entries = conflicts.iter().map(|c| (c.key.clone(), c.expected));
        match exchange(&mut stream, ClientRequest::Delete(delete(entries, false)), version).await? {
            ClientResponse::DeleteConflict(response) => assert_eq!(response.conflicts, conflicts),
            response => panic!("v{version} delete conflict: {response:?}"),
        }
        let key = key(900 + u128::from(version));
        let event = payload(&format!("wire-v{version}"), 1);
        match exchange(
            &mut stream,
            ClientRequest::Write(write([(key.clone(), append(vec![event.clone()], Some(0)))])),
            version,
        )
        .await?
        {
            ClientResponse::Write(response) => assert_eq!(response.max_aggregate_version, Some(1)),
            response => panic!("v{version} connection reuse: {response:?}"),
        }
        history.streams.push(Stream {
            key,
            from: 1,
            events: vec![event],
        });
    }
    let mut stream = raw_identified(address, 4).await?;
    let key = key(906);
    let request = ClientRequest::Write(write([(key.clone(), append(vec![payload("changed-version-must-not-land", 1)], None))]));
    ClientRequest::write_request(&mut stream, &request, MAX, 5)
        .await
        .map_err(|e| format!("wire: {e:?}"))?;
    let response = tokio::time::timeout(DEADLINE, ClientResponse::read_response(&mut stream, MAX)).await?;
    assert!(
        !matches!(response, Ok(ClientResponse::Write(_))),
        "mid-connection version change accepted"
    );
    history.absent.push(key);
    println!("PASS wire: v4/v5 complete write/delete conflicts and reuse; version changes reject before mutation");
    Ok(())
}

async fn exercise(address: &str) -> Result<History, Box<dyn Error>> {
    let mut c = client(address).await?;
    let mut history = History {
        streams: Vec::new(),
        absent: Vec::new(),
    };
    let fence = key(1);
    let target = key(2);
    let missing = key(3);
    create(&mut c, &mut history, fence.clone(), 1, "fence").await?;
    create(&mut c, &mut history, target.clone(), 1, "target").await?;
    let initial = capture(address, &history).await?;
    assert_eq!(
        c.write_batch(CLIENT)
            .guard(missing.clone(), 0)
            .guard(fence.clone(), 1)
            .send()
            .await?
            .max_aggregate_version,
        None
    );
    history.absent.push(missing.clone());
    assert_eq!(capture(address, &history).await?, initial, "guard-only request changes no history");

    let malformed = SingleAggregateWrite {
        events: Vec::new(),
        allow_create: true,
        enforce_client_idempotency: false,
        expected_version: Some(1),
    };
    let error = c
        .write(write([
            (fence.clone(), malformed),
            (target.clone(), append(vec![payload("malformed-reject", 2)], Some(1))),
        ]))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            ClientError::Server(ServerError::Write {
                kind: WriteError::MalformedGuardEntry,
                ..
            })
        ),
        "{error:?}"
    );
    assert_eq!(capture(address, &history).await?, initial, "malformed request partially appended");
    let next = payload("target-guarded", 2);
    let response = c
        .write_batch(CLIENT)
        .append_with(
            target.clone(),
            vec![next.clone()],
            WriteEventsOptions {
                allow_create: false,
                expected_version: Some(1),
                enforce_client_idempotency: false,
            },
        )
        .guard(fence.clone(), 1)
        .guard(missing.clone(), 0)
        .send()
        .await?;
    assert_eq!(
        response.max_aggregate_version,
        Some(2),
        "guards must not hide one append's response version"
    );
    history.streams[1].events.push(next);
    println!("PASS guards: guard-only, malformed guard, mixed response version");

    let conflicting_keys = [
        AggregateKey::new(1, u128::MAX, 9),
        AggregateKey::new(2, 0, 1),
        AggregateKey::new(u128::MAX, 0, 0),
    ];
    for (index, key) in conflicting_keys.iter().enumerate() {
        create(&mut c, &mut history, key.clone(), index as u64 + 1, "conflict-participant").await?;
    }
    let expected: Vec<_> = conflicting_keys
        .iter()
        .enumerate()
        .map(|(index, key)| AggregateConflict {
            key: key.clone(),
            expected: 0,
            current: index as u64 + 1,
        })
        .collect();
    let unchanged = capture(address, &history).await?;
    let entries = conflicting_keys.iter().enumerate().map(|(index, key)| {
        (
            key.clone(),
            if index == 1 {
                guard(0)
            } else {
                append(vec![payload("rejected-conflict", 9)], Some(0))
            },
        )
    });
    let entries = entries.chain([(target.clone(), append(vec![payload("must-be-atomic", 3)], Some(2)))]);
    assert_write_conflicts(c.write(write(entries)).await.unwrap_err(), &expected);
    let entries = conflicting_keys.iter().cloned().map(|key| (key, 0)).chain([(target.clone(), 2)]);
    assert_delete_conflicts(c.delete(delete(entries, true)).await.unwrap_err(), &expected);
    assert_eq!(capture(address, &history).await?, unchanged, "partial write/delete");
    println!("PASS conflicts: complete sorted write/delete conflicts leave no partial state");
    wire_contract(address, &mut history, &expected).await?;
    capture(address, &history).await?;
    Ok(history)
}

async fn restrictive_capacity() -> R {
    let server = TestServer::start_with_config_labeled(
        port_for("occ-capacity"),
        ServerConfig {
            num_shards: Some(1),
            standalone: true,
            max_response_size: 172,
            ..Default::default()
        },
        "occ-capacity".into(),
    )
    .await?;
    let mut c = client(server.address()).await?;
    let a = key(400);
    let b = key(401);
    for key in [&a, &b] {
        c.write(write([(key.clone(), append(vec![event(1, 100, 1, "{}")], None))])).await?;
    }
    let error = c
        .write(write([(a.clone(), append(vec![event(2, 100, 2, "{}")], Some(1))), (b.clone(), guard(1))]))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            ClientError::Server(ServerError::Write {
                kind: WriteError::ConflictResponseCapacityExceeded,
                ..
            })
        ),
        "{error:?}"
    );
    let error = c.delete(delete([(a.clone(), 1), (b.clone(), 1)], false)).await.unwrap_err();
    assert!(
        matches!(
            error,
            ClientError::Server(ServerError::Delete {
                kind: DeleteError::ConflictResponseCapacityExceeded,
                ..
            })
        ),
        "{error:?}"
    );
    for key in [a, b] {
        let details = c
            .aggregate_details(AggregateDetailsRequest {
                correlation_id: None,
                aggregate_key: key,
            })
            .await?;
        assert_eq!(details.max_aggregate_version, 1);
        assert!(!details.is_deleted);
    }
    println!("PASS server capacity: max_response_size rejects matching write/delete before mutation");
    Ok(())
}

pub async fn standalone() -> R {
    let server = TestServer::start_with_config_labeled(
        port_for("occ-standalone"),
        ServerConfig {
            num_shards: Some(1),
            standalone: true,
            ..Default::default()
        },
        "occ-standalone".into(),
    )
    .await?;
    exercise(server.address()).await?;
    drop(server);
    restrictive_capacity().await?;
    println!("PASS occ_guards_standalone");
    Ok(())
}

async fn elected(a: &TestServer, b: &TestServer) -> Result<String, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        for server in [a, b] {
            if is_leader(server.address()).await.unwrap_or(false) {
                return Ok(server.address().to_string());
            }
        }
        if Instant::now() >= deadline {
            return Err("no leader elected in 60s".into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub async fn distributed() -> R {
    let minio = MinioContainer::start_with_bucket(port_for("occ-minio"), "occ-guards").await?;
    let (region, bucket, access, secret, endpoint, http) = minio.s3_config_fields();
    let mut config = s3_cluster_config(1, &region, &bucket, &access, &secret, &endpoint, http);
    config.s3_subfolder = Some(format!("occ-guards-{}", std::process::id()));
    config.log_level = "warn".into();
    let pa = port_for("occ-node-a");
    let pb = port_for("occ-node-b");
    config.advertised_replication_address = Some(format!("127.0.0.1:{}", pa + 1));
    let mut a = TestServer::start_with_config_labeled(pa, config.clone(), "occ-node-a".into()).await?;
    config.advertised_replication_address = Some(format!("127.0.0.1:{}", pb + 1));
    let mut b = TestServer::start_with_config_labeled(pb, config, "occ-node-b".into()).await?;
    let leader = elected(&a, &b).await?;
    let history = exercise(&leader).await?;
    let before = capture(&leader, &history).await?;
    for server in [&a, &b] {
        wait_snapshot(server.address(), &history, &before).await?;
        assert_eq!(capture(server.address(), &history).await?, before, "replica payload/metadata divergence");
    }
    a.stop();
    b.stop();
    a.restart().await?;
    b.restart().await?;
    elected(&a, &b).await?;
    for server in [&a, &b] {
        wait_snapshot(server.address(), &history, &before).await?;
        assert_eq!(
            capture(server.address(), &history).await?,
            before,
            "post-restart replica payload/metadata divergence"
        );
    }
    println!(
        "PASS distributed recovery: {} streams, {} absent keys equal on both nodes before and after dual restart",
        history.streams.len(),
        history.absent.len()
    );
    drop(a);
    drop(b);
    drop(minio);
    println!("PASS occ_guards_distributed");
    Ok(())
}

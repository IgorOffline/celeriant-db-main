//! Public request contracts over a real standalone shard and its durable WAL.
use crate::error::shard_delete_error::ShardDeleteError;
use crate::error::shard_error::ShardError;
use crate::error::shard_write_error::ShardWriteError;
use crate::internal_shard_config::InternalShardConfig;
use crate::replication_client::{ReplicationClient, StubReplicationClient};
use crate::s3_downloader::StubS3Downloader;
use crate::shard_wal::ShardWal;
use crate::timestamp_config::TimestampConfig;
use celeriant_distributed::validated_node_status::ValidatedNodeStatus;
use celeriant_msg::process_client_requests::ClientRequest;
use celeriant_msg::process_client_responses::ClientResponse;
use celeriant_msg::request::requests::*;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;
use celeriant_wal::schema_key::SchemaKey;
use glommio::{LocalExecutorBuilder, Placement};
use std::sync::Arc;
use std::time::Duration;

type TestShard = ShardWal<StubReplicationClient, StubS3Downloader>;
macro_rules! shard_test {
    ($body:expr) => {
        LocalExecutorBuilder::new(Placement::Unbound)
            .spawn(|| async move { $body })
            .unwrap()
            .join()
            .unwrap()
    };
}
const STRICT_SCHEMA: &str = r#"{"type":"object","required":["ok"],"properties":{"ok":{"const":true}}}"#;

fn test_config(dir: &std::path::Path, cache_bytes: u64) -> InternalShardConfig {
    InternalShardConfig {
        wal_join_data_meta_writes: true,
        node_id: 1,
        shard_id: 1,
        max_open_files: 4,
        shard_log_preallocate_bytes: 4 * 1024 * 1024,
        fsync_delay: Duration::ZERO,
        replication_delay: Duration::ZERO,
        s3_replication_delay: Duration::from_millis(500),
        replication_rollback_cooldown: Duration::ZERO,
        heartbeat_starve_threshold: Duration::ZERO,
        recent_write_cache_bytes: 64 * 1024 * 1024,
        shard_dir: dir.to_path_buf(),
        max_response_size: 16 * 1024 * 1024,
        max_request_size: 16 * 1024 * 1024,
        internode_max_request_size: 64 * 1024 * 1024,
        aggregate_snapshots_cache_bytes: cache_bytes,
        aggregate_client_snapshots_cache_bytes: cache_bytes,
        negative_lookup_cache_bytes: 2 * 1024 * 1024,
        read_max_chunk_size: 32 * 1024,
        chain_read_window_bytes: 1024,
        timestamp_config: TimestampConfig::default(),
        list_page_size: 100,
        list_max_concurrent: 16,
        list_max_duration: Duration::from_secs(2),
        schema_cache_bytes: cache_bytes,
        max_schema_size_bytes: 16384,
        max_catchup_gap_bytes: Some(100 * 1024 * 1024),
        max_promotion_batch_bytes: None,
        compaction_check_interval: Duration::from_secs(600),
        compaction_min_reclaimable_ratio: 0.20,
        compaction_temp_dir: std::path::PathBuf::from("/tmp/test_compaction"),
        max_clock_drift_ms: 500,
        read_max_concurrent: 64,
        cache_warmup_max_duration: Duration::MAX,
        wal_compression_level: 3,
        dict_bytes: Arc::from(celeriant_wal::builtin_dict::BUILTIN_DICT_BYTES),
        s3_lease_duration_ms: 0,
    }
}

fn key(id: u128) -> AggregateKey {
    AggregateKey::new(1, 1, id)
}
fn append(seq: u64, expected: Option<u64>) -> SingleAggregateWrite {
    SingleAggregateWrite {
        events: vec![DatablockAggregateEvent {
            client_seq: seq,
            event_type_major: 1,
            event_value: Arc::new(br#"{"ok":true}"#.to_vec()),
            ..Default::default()
        }],
        allow_create: true,
        expected_version: expected,
        enforce_client_idempotency: true,
    }
}
fn guard(expected: u64) -> SingleAggregateWrite {
    SingleAggregateWrite {
        events: vec![],
        allow_create: false,
        expected_version: Some(expected),
        enforce_client_idempotency: false,
    }
}
fn write(entries: impl IntoIterator<Item = (AggregateKey, SingleAggregateWrite)>) -> ClientRequest {
    ClientRequest::Write(WriteRequest {
        correlation_id: Some(17),
        client_id: 7,
        user_id: None,
        writes: entries.into_iter().collect(),
    })
}
fn delete(entries: impl IntoIterator<Item = (AggregateKey, u64)>, allow_recreate: bool) -> ClientRequest {
    ClientRequest::Delete(DeleteRequest {
        correlation_id: Some(23),
        client_id: 7,
        user_id: None,
        deletes: entries
            .into_iter()
            .map(|(k, expected)| {
                (
                    k,
                    SingleAggregateDelete {
                        expected_version: Some(expected),
                        allow_recreate,
                        allow_sequence_continuation: true,
                    },
                )
            })
            .collect(),
    })
}
fn leader_status() -> ValidatedNodeStatus {
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    ValidatedNodeStatus::create_custom_status(
        celeriant_distributed::node_status::NodeStatus::Leader { lease_epoch: 0 },
        500,
        now + 60_000,
    )
}
async fn open_with<R: ReplicationClient + 'static>(
    config: InternalShardConfig,
    status: ValidatedNodeStatus,
    replication: R,
) -> ShardWal<R, StubS3Downloader> {
    ShardWal::open(config, status, replication, StubS3Downloader).await.unwrap()
}
async fn open(dir: &std::path::Path, capacity: u64) -> TestShard {
    open_with(test_config(dir, capacity), ValidatedNodeStatus::create_standalone(), StubReplicationClient).await
}
async fn register_schema(shard: &TestShard, major: u64, schema: &str) {
    shard
        .process_client_request(ClientRequest::RegisterSchema(RegisterSchemaRequest {
            correlation_id: None,
            client_id: 7,
            user_id: None,
            schema_key: SchemaKey::new(1, 1, major, 0),
            schema_type: 0,
            schema: schema.into(),
        }))
        .await
        .unwrap();
}
async fn version(shard: &TestShard, aggregate_key: AggregateKey) -> u64 {
    match shard
        .process_client_request(ClientRequest::AggregateDetails(AggregateDetailsRequest {
            correlation_id: None,
            aggregate_key,
        }))
        .await
    {
        Ok(ClientResponse::AggregateDetails(d)) => d.max_aggregate_version,
        other => panic!("details must succeed: {other:?}"),
    }
}
async fn event_seqs(shard: &TestShard, aggregate_key: AggregateKey, from: u64) -> Vec<u64> {
    match shard
        .process_client_request(ClientRequest::Read(ReadRequest {
            correlation_id: None,
            aggregate_key,
            filters: celeriant_msg::request::read_filters::ReadFilters::new(from),
        }))
        .await
    {
        Ok(ClientResponse::Read(read)) => read
            .event_batches
            .into_iter()
            .flat_map(|batch| batch.events.into_iter().map(|event| event.client_seq))
            .collect(),
        other => panic!("history must be readable: {other:?}"),
    }
}
fn assert_write(result: Result<ClientResponse, ShardError>, expected: Option<u64>) {
    match result {
        Ok(ClientResponse::Write(w)) => assert_eq!(w.max_aggregate_version, expected),
        other => panic!("expected accepted write at {expected:?}, got {other:?}"),
    }
}
fn assert_write_rejected(result: Result<ClientResponse, ShardError>, code: &str) {
    match result {
        Err(ShardError::Write(error)) => assert_eq!(error.error_code(), code),
        other => panic!("expected {code}, got {other:?}"),
    }
}
fn conflict_records(result: Result<ClientResponse, ShardError>) -> Vec<(u128, u128, u128, u64, u64)> {
    let conflicts = match result {
        Err(ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation { conflicts, .. })) => conflicts,
        Err(ShardError::Delete(ShardDeleteError::OptimisticConcurrencyViolation { conflicts, .. })) => conflicts,
        other => panic!("expected a typed complete OCC rejection: {other:?}"),
    };
    conflicts
        .into_iter()
        .map(|conflict| {
            (
                conflict.key.org_id,
                conflict.key.aggregate_type_id,
                conflict.key.aggregate_id,
                conflict.expected,
                conflict.current,
            )
        })
        .collect()
}

#[test]
fn live_and_absent_guards_do_not_change_the_guarded_head() {
    shard_test!({
        for (id, seeded, expected) in [(1, false, 0), (2, true, 1)] {
            let tmp = tempfile::tempdir().unwrap();
            let shard = open(tmp.path(), 1024).await;
            if seeded {
                assert_write(shard.process_client_request(write([(key(id), append(1, None))])).await, Some(1));
            }
            for _ in 0..3 {
                assert_write(shard.process_client_request(write([(key(id), guard(expected))])).await, None);
            }
            assert_write(
                shard
                    .process_client_request(write([(key(id), guard(expected)), (key(99), append(1, Some(0)))]))
                    .await,
                Some(1),
            );
            if seeded {
                assert_eq!(version(&shard, key(id)).await, 1);
                assert_eq!(event_seqs(&shard, key(id), 1).await, vec![1]);
                assert_write(shard.process_client_request(write([(key(id), append(2, Some(1)))])).await, Some(2));
                assert_eq!(event_seqs(&shard, key(id), 1).await, vec![1, 2]);
            } else {
                let mut no_create = append(1, Some(0));
                no_create.allow_create = false;
                assert!(
                    matches!(
                        shard.process_client_request(write([(key(id), no_create)])).await,
                        Err(ShardError::Write(ShardWriteError::AggregateNotExists))
                    ),
                    "zero guard must not create an aggregate"
                );
            }
            shard.close().await;
        }
    });
}

#[test]
fn missing_and_deleted_targets_reject_guards_with_typed_errors() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let shard = open(tmp.path(), 1024 * 1024).await;
        let missing = shard.process_client_request(write([(key(99), guard(1))])).await;
        assert!(
            matches!(missing, Err(ShardError::Write(ShardWriteError::AggregateNotExists))),
            "{missing:?}"
        );
        for allow_recreate in [false, true] {
            let id = 1 + u128::from(allow_recreate);
            assert_write(shard.process_client_request(write([(key(id), append(1, None))])).await, Some(1));
            shard.process_client_request(delete([(key(id), 1)], allow_recreate)).await.unwrap();
            for pin in [0, 1] {
                let deleted = shard.process_client_request(write([(key(id), guard(pin))])).await;
                assert!(
                    matches!(deleted, Err(ShardError::Write(ShardWriteError::AggregateRecreateNotAllowed))),
                    "deleted target allow_recreate={allow_recreate}, pin={pin}: {deleted:?}"
                );
            }
        }
        shard.close().await;
    });
}

#[test]
fn every_schema_remains_enforced_when_a_request_exceeds_schema_cache() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let shard = open(tmp.path(), 1024).await;
        for major in 1..=32 {
            register_schema(&shard, major, STRICT_SCHEMA).await;
        }
        let mut valid = append(1, Some(0));
        valid.events = (1..=32)
            .map(|major| DatablockAggregateEvent {
                client_seq: major,
                event_type_major: major,
                event_value: Arc::new(br#"{"ok":true}"#.to_vec()),
                ..Default::default()
            })
            .collect();
        assert_write(shard.process_client_request(write([(key(1), valid.clone())])).await, Some(1));
        for invalid_index in [0, 15, 31] {
            let mut invalid = valid.clone();
            invalid.expected_version = Some(1);
            for event in &mut invalid.events {
                event.client_seq += 32;
            }
            invalid.events[invalid_index].event_value = Arc::new(br#"{"ok":false}"#.to_vec());
            let result = shard.process_client_request(write([(key(1), invalid)])).await;
            assert!(
                matches!(result, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))),
                "schema index {invalid_index} must still reject: {result:?}"
            );
            assert_eq!(version(&shard, key(1)).await, 1);
        }
        shard.close().await;
    });
}

#[test]
fn cache_pressure_never_loses_client_idempotency_history() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let shard = open(tmp.path(), 1024).await;
        for id in 1..=32 {
            assert_write(shard.process_client_request(write([(key(id), append(3, None))])).await, Some(1));
        }
        for id in [1, 16, 32] {
            let duplicate = shard
                .process_client_request(write([(key(id), append(3, None)), (key(99), append(1, None))]))
                .await;
            assert!(
                matches!(duplicate, Err(ShardError::Write(ShardWriteError::ClientIdempotencyViolation { .. }))),
                "durable duplicate aggregate {id}: {duplicate:?}"
            );
            assert_eq!(version(&shard, key(id)).await, 1);
        }
        let mut no_create = append(1, None);
        no_create.allow_create = false;
        let absent = shard.process_client_request(write([(key(99), no_create)])).await;
        assert!(
            matches!(absent, Err(ShardError::Write(ShardWriteError::AggregateNotExists))),
            "rejected mixed requests must never create second participant: {absent:?}"
        );
        shard.close().await;
    });
}

#[test]
fn malformed_guard_flags_have_a_distinct_rejection() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let shard = open(tmp.path(), 1024 * 1024).await;
        for (allow_create, enforce_client_idempotency) in [(true, false), (false, true), (true, true)] {
            let mut malformed = guard(0);
            malformed.allow_create = allow_create;
            malformed.enforce_client_idempotency = enforce_client_idempotency;
            match shard.process_client_request(write([(key(1), malformed)])).await {
                Err(ShardError::Write(error)) => assert_eq!(
                    error.error_code(),
                    "malformed_guard_entry",
                    "flags create={allow_create} idempotency={enforce_client_idempotency}"
                ),
                other => panic!("malformed guard accepted: {other:?}"),
            }
        }
        let mut unpinned = guard(0);
        unpinned.expected_version = None;
        assert!(matches!(
            shard.process_client_request(write([(key(1), unpinned)])).await,
            Err(ShardError::Write(ShardWriteError::EmptyEventsList))
        ));
        shard.close().await;
    });
}

#[test]
fn commits_after_warmup_under_cache_pressure_are_revalidated() {
    shard_test!({
        for (tombstone, attempts, code) in [
            (false, vec![append(3, Some(1))], "optimistic_concurrency_violation"),
            (false, vec![append(2, None)], "client_idempotency_violation"),
            (true, vec![append(2, Some(1)), append(2, None)], "aggregate_recreate_not_allowed"),
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let shard = open(tmp.path(), 1024).await;
            assert_write(shard.process_client_request(write([(key(1), append(1, None))])).await, Some(1));
            shard.set_validation_warmup_hook_for_test(move |shard| {
                Box::pin(async move {
                    if tombstone {
                        shard.process_client_request(delete([(key(1), 1)], false)).await.unwrap();
                    } else {
                        assert_write(shard.process_client_request(write([(key(1), append(2, None))])).await, Some(2));
                    }
                    for id in 2..=33 {
                        assert_write(shard.process_client_request(write([(key(id), append(1, None))])).await, Some(1));
                    }
                })
            });
            for attempt in attempts {
                assert_write_rejected(shard.process_client_request(write([(key(1), attempt)])).await, code);
            }
            if !tombstone {
                assert_eq!(version(&shard, key(1)).await, 2);
            }
            shard.close().await;
        }
    });
}

#[test]
fn schema_registered_before_or_during_warmup_is_enforced_under_cache_pressure() {
    shard_test!({
        for during_warmup in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let shard = open(tmp.path(), 1024).await;
            if !during_warmup {
                register_schema(&shard, 1, STRICT_SCHEMA).await;
            }
            assert_write(shard.process_client_request(write([(key(1), append(1, None))])).await, Some(1));
            shard.set_validation_warmup_hook_for_test(move |shard| {
                Box::pin(async move {
                    if during_warmup {
                        register_schema(shard, 1, STRICT_SCHEMA).await;
                    }
                    for major in 2..=33 {
                        register_schema(shard, major, r#"{"type":"object"}"#).await;
                    }
                })
            });
            let mut invalid = append(2, Some(1));
            invalid.events[0].event_value = Arc::new(br#"{"ok":false}"#.to_vec());
            let rejected = shard.process_client_request(write([(key(1), invalid)])).await;
            assert!(
                matches!(rejected, Err(ShardError::Write(ShardWriteError::SchemaValidationFailed { .. }))),
                "during_warmup={during_warmup}: {rejected:?}"
            );
            assert_eq!(version(&shard, key(1)).await, 1);
            shard.close().await;
        }
    });
}

#[test]
fn trim_floor_does_not_turn_guard_validation_into_a_read_error() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let shard = open(tmp.path(), 1024).await;
        for seq in 1..=3 {
            assert_write(shard.process_client_request(write([(key(1), append(seq, None))])).await, Some(seq));
        }
        shard
            .process_client_request(ClientRequest::TrimStart(TrimStartRequest {
                correlation_id: None,
                client_id: 7,
                user_id: None,
                aggregate_key: key(1),
                keep_from_aggregate_version: 3,
            }))
            .await
            .unwrap();
        assert_write(shard.process_client_request(write([(key(1), guard(3))])).await, None);
        let rejected = shard.process_client_request(write([(key(1), guard(1))])).await;
        assert!(
            matches!(rejected, Err(ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation { .. }))),
            "pin below trim floor is OCC: {rejected:?}"
        );
        assert_eq!(event_seqs(&shard, key(1), 3).await, vec![3]);
        shard.close().await;
    });
}

#[test]
fn independent_writes_can_share_one_unchanged_fence() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let shard = std::rc::Rc::new(open(tmp.path(), 1024).await);
        assert_write(shard.process_client_request(write([(key(1), append(1, None))])).await, Some(1));
        let mut tasks = Vec::new();
        for id in 2..=9 {
            let shard = shard.clone();
            tasks.push(glommio::spawn_local(async move {
                shard
                    .process_client_request(write([(key(1), guard(1)), (key(id), append(1, Some(0)))]))
                    .await
            }));
        }
        for task in tasks {
            assert_write(task.await, Some(1));
        }
        assert_eq!(event_seqs(&shard, key(1), 1).await, vec![1]);
        assert_write(shard.process_client_request(write([(key(1), append(2, Some(1)))])).await, Some(2));
        for id in 2..=9 {
            let rejected = shard
                .process_client_request(write([(key(1), guard(1)), (key(id), append(2, Some(1)))]))
                .await;
            assert!(
                matches!(rejected, Err(ShardError::Write(ShardWriteError::OptimisticConcurrencyViolation { .. }))),
                "advanced fence: {rejected:?}"
            );
            assert_eq!(event_seqs(&shard, key(id), 1).await, vec![1]);
        }
        shard.close().await;
    });
}

#[test]
fn non_occ_rejections_mask_conflicts_without_partial_mutations() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let shard = open(tmp.path(), 1024).await;
        for id in 1..=3 {
            assert_write(shard.process_client_request(write([(key(id), append(1, None))])).await, Some(1));
        }
        for _ in 0..8 {
            let mut zero_type = append(2, Some(1));
            zero_type.events[0].event_type_major = 0;
            let rejected = shard
                .process_client_request(write([(key(1), append(2, Some(0))), (key(2), zero_type), (key(3), append(2, Some(1)))]))
                .await;
            assert!(
                matches!(rejected, Err(ShardError::Write(ShardWriteError::ZeroEventType { .. }))),
                "non-OCC error must mask earlier OCC: {rejected:?}"
            );
        }
        for id in 1..=3 {
            assert_eq!(event_seqs(&shard, key(id), 1).await, vec![1]);
        }
        shard.close().await;
    });
}

#[test]
fn conflict_response_capacity_is_reserved_for_every_pin() {
    shard_test!({
        for max_response_size in [172, 173] {
            let tmp = tempfile::tempdir().unwrap();
            let mut config = test_config(tmp.path(), 1024);
            config.max_response_size = max_response_size;
            let shard = open_with(config, ValidatedNodeStatus::create_standalone(), StubReplicationClient).await;
            for id in 1..=6 {
                assert_write(shard.process_client_request(write([(key(id), append(1, None))])).await, Some(1));
            }
            let fits = max_response_size == 173;
            for guard_count in 0..=2u128 {
                let result = shard
                    .process_client_request(write((1..=2).map(|i| {
                        (key(2 * guard_count + i), if i <= guard_count { guard(1) } else { append(2, Some(1)) })
                    })))
                    .await;
                if fits {
                    assert!(matches!(result, Ok(ClientResponse::Write(_))), "guard_count={guard_count}: {result:?}");
                } else {
                    assert!(
                        matches!(
                            result,
                            Err(ShardError::Write(ShardWriteError::ConflictResponseTooLarge {
                                required: Some(173),
                                max_size: 172
                            }))
                        ),
                        "two pinned records reserve 25 + 74*2 = 173 bytes even when matching; guard_count={guard_count}: {result:?}"
                    );
                }
            }
            let result = shard.process_client_request(delete([(key(5), 1), (key(6), 1)], false)).await;
            if fits {
                assert!(matches!(result, Ok(ClientResponse::Delete(_))), "{result:?}");
            } else {
                assert!(
                    matches!(
                        result,
                        Err(ShardError::Delete(ShardDeleteError::ConflictResponseTooLarge {
                            required: Some(173),
                            max_size: 172
                        }))
                    ),
                    "matching delete also reserves the complete conflict body: {result:?}"
                );
                for id in 1..=6 {
                    assert_eq!(event_seqs(&shard, key(id), 1).await, vec![1]);
                }
            }
            shard.close().await;
        }
    });
}

#[test]
fn conflicts_are_complete_and_sorted_by_unsigned_key_tuple() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let shard = open(tmp.path(), 1024).await;
        let keys = [
            AggregateKey::new(u128::MAX, 1, 1),
            AggregateKey::new(1, 256, 1),
            AggregateKey::new(1, 1, 256),
            AggregateKey::new(1, 1, 1),
            AggregateKey::new(1, 1, u128::MAX),
        ];
        for k in keys.iter().chain([&key(99)]) {
            assert_write(shard.process_client_request(write([(k.clone(), append(1, None))])).await, Some(1));
        }
        for deleting in [false, true] {
            let pin = |i: usize| if deleting || i % 2 == 0 { 0 } else { 9 };
            let mut expected: Vec<_> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| (k.org_id, k.aggregate_type_id, k.aggregate_id, pin(i), 1))
                .collect();
            expected.sort();
            for _ in 0..10 {
                let request = if deleting {
                    delete(keys.iter().map(|k| (k.clone(), 0)).chain([(key(99), 1)]), false)
                } else {
                    write(
                        keys.iter()
                            .enumerate()
                            .map(|(i, k)| (k.clone(), if pin(i) == 0 { guard(0) } else { append(2, Some(9)) }))
                            .chain([(key(99), append(2, Some(1)))]),
                    )
                };
                assert_eq!(conflict_records(shard.process_client_request(request).await), expected, "deleting={deleting}");
            }
            for k in keys.iter().chain([&key(99)]) {
                assert_eq!(event_seqs(&shard, k.clone(), 1).await, vec![1]);
            }
        }
        shard.close().await;
    });
}

#[test]
fn conflicts_that_appear_after_warmup_include_every_moved_head() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let shard = open(tmp.path(), 1024).await;
        for id in 1..=3 {
            assert_write(shard.process_client_request(write([(key(id), append(1, None))])).await, Some(1));
        }
        shard.set_validation_warmup_hook_for_test(|shard| {
            Box::pin(async move {
                for id in [1, 2] {
                    assert_write(shard.process_client_request(write([(key(id), append(2, None))])).await, Some(2));
                }
            })
        });
        let rejected = shard
            .process_client_request(write([(key(1), guard(1)), (key(2), append(3, Some(1))), (key(3), append(2, Some(1)))]))
            .await;
        assert_eq!(conflict_records(rejected), vec![(1, 1, 1, 1, 2), (1, 1, 2, 1, 2)]);
        assert_eq!(event_seqs(&shard, key(1), 1).await, vec![1, 2]);
        assert_eq!(event_seqs(&shard, key(2), 1).await, vec![1, 2]);
        assert_eq!(event_seqs(&shard, key(3), 1).await, vec![1]);
        shard.close().await;
    });
}

#[test]
fn visibility_retry_replaces_the_entire_conflict_decision() {
    shard_test!({
        for deleting in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let shard = open(tmp.path(), 128).await;
            for id in 1..=3 {
                assert_write(shard.process_client_request(write([(key(id), append(1, None))])).await, Some(1));
            }
            let attempts = std::rc::Rc::new(std::cell::Cell::new(0));
            let seen = attempts.clone();
            shard.set_validation_warmup_hook_for_test(move |shard| {
                Box::pin(async move {
                    seen.set(seen.get() + 1);
                    shard.set_validation_warmup_hook_for_test(move |shard| {
                        Box::pin(async move {
                            seen.set(seen.get() + 1);
                            for seq in 2..=3 {
                                assert_write(shard.process_client_request(write([(key(1), append(seq, None))])).await, Some(seq));
                            }
                            assert_write(shard.process_client_request(write([(key(2), append(2, None))])).await, Some(2));
                        })
                    });
                })
            });
            let request = if deleting {
                delete([(key(1), 3), (key(2), 3), (key(3), 1)], true)
            } else {
                write([(key(1), guard(3)), (key(2), guard(3)), (key(3), guard(1))])
            };
            let rejected = shard.process_client_request(request).await;
            assert_eq!(attempts.get(), 2, "exactly one whole-request visibility retry, deleting={deleting}");
            assert_eq!(conflict_records(rejected), vec![(1, 1, 2, 3, 2)]);
            for (id, seqs) in [(1, vec![1, 2, 3]), (2, vec![1, 2]), (3, vec![1])] {
                assert_eq!(event_seqs(&shard, key(id), 1).await, seqs);
            }
            shard.close().await;
            let reopened = open(tmp.path(), 128).await;
            for (id, expected) in [(1, 3), (2, 2), (3, 1)] {
                assert_eq!(version(&reopened, key(id)).await, expected, "rejected request must leave durable aggregate {id} live");
            }
            reopened.close().await;
        }
    });
}

#[test]
fn guard_only_success_cannot_cross_either_durability_rollback_boundary() {
    shard_test!({
        for after_replication in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let shard = open(tmp.path(), 1024).await;
            assert_write(shard.process_client_request(write([(key(1), append(1, None))])).await, Some(1));
            let triggered = std::rc::Rc::new(std::cell::Cell::new(false));
            let observed = triggered.clone();
            if after_replication {
                shard.set_after_replication_hook_for_test(move |shard| {
                    Box::pin(async move {
                        observed.set(true);
                        shard.validation_cache_for_test().borrow_mut().execute_replication_rollback();
                    })
                });
            } else {
                shard.set_after_fsync_hook_for_test(move |shard| {
                    Box::pin(async move {
                        observed.set(true);
                        shard.validation_cache_for_test().borrow_mut().execute_replication_rollback();
                    })
                });
            }
            let rejected = shard.process_client_request(write([(key(1), guard(1))])).await;
            assert!(triggered.get(), "durability hook must run, after_replication={after_replication}");
            assert!(
                matches!(
                    rejected,
                    Err(ShardError::Write(ShardWriteError::ReplicationError(
                        crate::error::replication_error::ReplicationError::RollbackInProgress
                    )))
                ),
                "rollback crossing must reject, after_replication={after_replication}: {rejected:?}"
            );
            assert_eq!(event_seqs(&shard, key(1), 1).await, vec![1]);
            shard.close().await;
        }
    });
}

#[test]
fn pending_deleted_target_rejects_zero_and_preserved_head_guards() {
    shard_test!({
        for allow_recreate in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let shard = open_with(test_config(tmp.path(), 1024), leader_status(), StubReplicationClient).await;
            assert_write(shard.process_client_request(write([(key(1), append(1, None))])).await, Some(1));
            let triggered = std::rc::Rc::new(std::cell::Cell::new(false));
            let observed = triggered.clone();
            shard.set_after_fsync_hook_for_test(move |shard| {
                Box::pin(async move {
                    observed.set(true);
                    for pin in [0, 1] {
                        let rejected = shard.process_client_request(write([(key(1), guard(pin))])).await;
                        assert!(
                            matches!(rejected, Err(ShardError::Write(ShardWriteError::AggregateRecreateNotAllowed))),
                            "pending deleted target allow_recreate={allow_recreate} pin={pin}: {rejected:?}"
                        );
                    }
                })
            });
            shard.process_client_request(delete([(key(1), 1)], allow_recreate)).await.unwrap();
            assert!(triggered.get(), "pending-delete hook must execute");
            shard.close().await;
        }
    });
}

struct RecordingReplicationClient(std::rc::Rc<std::cell::Cell<usize>>);
impl ReplicationClient for RecordingReplicationClient {
    fn set_follower_address(&self, _: Option<String>) {}
    fn set_follower_reachable(&self, _: bool) {}
    fn is_follower_reachable(&self) -> bool {
        true
    }
    fn current_heartbeat_started_at_unix_ms(&self) -> Option<u64> {
        None
    }
    fn set_heartbeat_in_flight(&self, _: Option<u64>) {}
    fn reset_heartbeat_state(&self) {}
    async fn replicate_to_follower(
        &self,
        items: Vec<ReplicationBatchItem>,
        _: u64,
        _: u64,
    ) -> Result<(), crate::error::replication_to_follower_error::ReplicateToFollowerError> {
        self.0.set(self.0.get() + items.len());
        Ok(())
    }
    async fn replicate_to_s3(&self, items: Vec<ReplicationBatchItem>) -> Result<(), crate::error::replication_to_s3_error::ReplicateToS3Error> {
        self.0.set(self.0.get() + items.len());
        Ok(())
    }
    async fn send_heartbeat(
        &self,
        now: u64,
        _: u64,
    ) -> Result<celeriant_msg::response::responses::HeartbeatResult, crate::error::send_heartbeat_error::SendHeartbeatError> {
        Ok(celeriant_msg::response::responses::HeartbeatResult::Ack {
            follower_timestamp_ms: now,
            follower_can_accept_tcp_replication: true,
        })
    }
    async fn send_kick(&self) -> Result<bool, crate::error::send_heartbeat_error::SendHeartbeatError> {
        Ok(true)
    }
}

#[test]
fn accepted_guards_create_no_replication_items() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let items = std::rc::Rc::new(std::cell::Cell::new(0));
        let shard = open_with(test_config(tmp.path(), 1024), leader_status(), RecordingReplicationClient(items.clone())).await;
        assert_write(shard.process_client_request(write([(key(1), append(1, None))])).await, Some(1));
        assert_eq!(items.get(), 1, "fixture must observe the seeded WAL append");
        for _ in 0..3 {
            assert_write(shard.process_client_request(write([(key(1), guard(1)), (key(3), guard(0))])).await, None);
        }
        assert_eq!(items.get(), 1, "guard-only observations produce no replication item");
        assert_write(
            shard
                .process_client_request(write([(key(1), guard(1)), (key(2), append(1, Some(0))), (key(3), guard(0))]))
                .await,
            Some(1),
        );
        assert_eq!(items.get(), 2, "only the writing participant emits a WAL replication item");
        shard.close().await;
    });
}

#[test]
fn accepted_guards_publish_no_watch_notifications() {
    shard_test!({
        let tmp = tempfile::tempdir().unwrap();
        let shard = open(tmp.path(), 1024).await;
        for id in 1..=2 {
            assert_write(shard.process_client_request(write([(key(id), append(1, None))])).await, Some(1));
        }
        let (subscription, subscriber) = shard.watched_aggregates().add_subscriber(WatchRequest {
            correlation_id: None,
            requested_latency_ms: None,
            shard_id: None,
            orgs: None,
            aggregate_types: None,
            aggregates: None,
            operation_types: Some(std::collections::HashSet::from([
                celeriant_watch::aggregate_watch_event::AggregateWatchEvent::WRITE,
            ])),
        });
        let receiver = subscriber.borrow().receiver.clone();
        assert_write(shard.process_client_request(write([(key(1), guard(1))])).await, None);
        assert_write(
            shard
                .process_client_request(write([(key(1), guard(1)), (key(2), append(2, Some(1)))]))
                .await,
            Some(2),
        );
        let observed = glommio::timer::timeout(Duration::from_secs(1), async { Ok(receiver.recv().await) })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.aggregate_key, key(2), "only the writing participant publishes a watch event");
        assert!(matches!(
            observed.operation,
            celeriant_watch::aggregate_watch_event::AggregateWatchEventOperation::Write {
                from_aggregate_version: 2,
                to_aggregate_version: 2
            }
        ));
        assert!(
            futures_lite::future::poll_once(receiver.recv()).await.flatten().is_none(),
            "guard-only and mixed guard publish no additional notification"
        );
        shard.watched_aggregates().remove_subscriber(subscription);
        shard.close().await;
    });
}

#[test]
fn matching_batches_larger_than_caches_keep_every_head_with_and_without_rollback() {
    shard_test!({
        for (deleting, rollback) in [(false, false), (false, true), (true, false), (true, true)] {
            let tmp = tempfile::tempdir().unwrap();
            let shard = open(tmp.path(), 1024).await;
            for id in 1..=32 {
                assert_write(shard.process_client_request(write([(key(id), append(1, None))])).await, Some(1));
            }
            if rollback {
                shard.set_validation_warmup_hook_for_test(|shard| {
                    Box::pin(async move {
                        shard.validation_cache_for_test().borrow_mut().execute_replication_rollback();
                    })
                });
            }
            if deleting {
                let result = shard.process_client_request(delete((1..=32).map(|id| (key(id), 1)), false)).await;
                assert!(matches!(result, Ok(ClientResponse::Delete(_))), "rollback={rollback}: {result:?}");
                for id in 1..=32 {
                    let result = shard.process_client_request(write([(key(id), append(2, None))])).await;
                    assert!(
                        matches!(result, Err(ShardError::Write(ShardWriteError::AggregateRecreateNotAllowed))),
                        "rollback={rollback} aggregate {id}: {result:?}"
                    );
                }
            } else {
                assert_write(shard.process_client_request(write((1..=32).map(|id| (key(id), append(2, Some(1)))))).await, None);
                for id in 1..=32 {
                    assert_eq!(version(&shard, key(id)).await, 2, "rollback={rollback} aggregate {id}");
                }
            }
            shard.close().await;
        }
    });
}

#[test]
fn deletion_appearing_after_warmup_masks_occ_without_creating_other_participants() {
    shard_test!({
        for deleted_guard_pin in [0, 1] {
            let tmp = tempfile::tempdir().unwrap();
            let shard = open(tmp.path(), 128).await;
            for id in 1..=2 {
                assert_write(shard.process_client_request(write([(key(id), append(1, None))])).await, Some(1));
            }
            shard.set_validation_warmup_hook_for_test(|shard| {
                Box::pin(async move {
                    shard.process_client_request(delete([(key(1), 1)], true)).await.unwrap();
                })
            });
            let result = shard
                .process_client_request(write([(key(1), guard(deleted_guard_pin)), (key(2), guard(0)), (key(3), append(1, Some(0)))]))
                .await;
            assert!(matches!(result, Err(ShardError::Write(ShardWriteError::AggregateRecreateNotAllowed))), "{result:?}");
            assert_eq!(version(&shard, key(2)).await, 1);
            let mut must_exist = append(1, None);
            must_exist.allow_create = false;
            let absent = shard.process_client_request(write([(key(3), must_exist)])).await;
            assert!(matches!(absent, Err(ShardError::Write(ShardWriteError::AggregateNotExists))), "rejected request created aggregate: {absent:?}");
            shard.close().await;
        }
    });
}

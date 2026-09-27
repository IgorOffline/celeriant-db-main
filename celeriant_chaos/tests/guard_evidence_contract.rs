use celeriant_bench::guard_fence::{
    anchor_key, fence_keys, guard_key, new_history, GuardHistory, GuardOperation,
    GuardOutcome, GuardParticipant, GuardPhase,
};
use celeriant_bench::DatablockAggregateEvent;
use celeriant_chaos::guard_fence::{completed_rollback_shards, fault_traffic, readiness_observation};
use celeriant_chaos::sample::NodeSample;
use celeriant_msg::response::responses::AggregateConflict;

fn sample(host: &str, time: u64, leader: bool) -> NodeSample {
    NodeSample {
        host: host.into(), t_ms: time, ok: true, node_role: if leader { 1.0 } else { 0.0 },
        wal_seq_by_shard: [(0, 10)].into(), read_wal_seq_by_shard: [(0, 10)].into(),
        ..Default::default()
    }
}

#[test]
fn readiness_requires_fresh_evidence_from_each_relevant_node() {
    let cases = [
        ("fresh convergence", 9000, 9500, None, true),
        ("stale first node", 7000, 9500, None, false),
        ("stale second node", 9500, 7000, None, false),
        ("future first node", 10001, 9500, None, false),
        ("required leader stale", 7000, 9500, Some("a"), false),
        ("fresh required leader, unavailable peer", 9500, 7000, Some("a"), true),
        ("unknown required node", 9500, 9500, Some("missing"), false),
        ("required node is follower", 9500, 9500, Some("b"), false),
        ("freshness boundary", 8000, 9500, None, false),
    ];
    for (name, a_time, b_time, required, expected) in cases {
        let result = readiness_observation(&sample("a", a_time, true), &sample("b", b_time, false), 10000, required);
        assert_eq!(result.is_some(), expected, "{name}: {result:?}");
    }
    for (name, mutate) in [
        ("both leaders", (|b: &mut NodeSample| b.node_role = 1.0) as fn(&mut NodeSample)),
        ("unreadable peer", |b| b.ok = false),
        ("peer write diverges", |b| { b.wal_seq_by_shard.insert(0, 11); }),
        ("peer read lags", |b| { b.read_wal_seq_by_shard.insert(0, 9); }),
        ("peer lacks cursor evidence", |b| b.wal_seq_by_shard.clear()),
    ] {
        let mut peer = sample("b", 9500, false);
        mutate(&mut peer);
        assert!(readiness_observation(&sample("a", 9500, true), &peer, 10000, None).is_none(), "{name}");
    }
}

fn fault_history() -> GuardHistory {
    let mut history = new_history(7);
    for (attempt, conflict) in [(1, false), (2, true)] {
        let mut participants: Vec<_> = fence_keys(7).into_iter().map(|key| GuardParticipant {
            key, expected_version: if conflict { 0 } else { 1 }, allow_create: false,
            enforce_client_idempotency: false, events: vec![],
        }).collect();
        for slot in 0..2 {
            participants.push(GuardParticipant {
                key: guard_key(7, 100 + attempt as u128 * 2 + slot), expected_version: 0,
                allow_create: true, enforce_client_idempotency: false,
                events: vec![DatablockAggregateEvent::default()],
            });
        }
        if conflict {
            participants.push(GuardParticipant {
                key: anchor_key(7), expected_version: 0, allow_create: false,
                enforce_client_idempotency: false, events: vec![DatablockAggregateEvent::default()],
            });
        }
        history.operations.push(GuardOperation {
            attempt_id: attempt, phase: GuardPhase::Load, client_id: attempt as u128,
            correlation_id: attempt, start_ns: 120, end_ns: 180, participants,
            outcome: if conflict {
                GuardOutcome::Conflict { conflicts: fence_keys(7).into_iter().chain([anchor_key(7)]).map(|key|
                    AggregateConflict { key, expected: 0, current: 1 }).collect() }
            } else { GuardOutcome::Accepted { max_aggregate_version: None } },
        });
    }
    history
}

#[test]
fn fault_coverage_needs_a_paired_ack_and_complete_conflict_inside_the_installed_window() {
    assert!(fault_traffic(&fault_history(), 100, 200));
    let cases: &[(&str, fn(&mut GuardHistory))] = &[
        ("no activity", |h| h.operations.clear()),
        ("only accepted operations", |h| { h.operations.pop(); }),
        ("only conflicting operations", |h| { h.operations.remove(0); }),
        ("ack before installation", |h| h.operations[0].start_ns = 99),
        ("ack after healing", |h| h.operations[0].end_ns = 201),
        ("conflict before installation", |h| h.operations[1].start_ns = 99),
        ("conflict after healing", |h| h.operations[1].end_ns = 201),
        ("missing guards", |h| h.operations[0].participants.retain(|p| !p.events.is_empty())),
        ("guard-only ack", |h| h.operations[0].participants.retain(|p| p.events.is_empty())),
        ("single-target ack", |h| { h.operations[0].participants.pop(); }),
        ("incomplete conflict", |h| { if let GuardOutcome::Conflict { conflicts } = &mut h.operations[1].outcome { conflicts.pop(); } }),
        ("ambiguous pair", |h| h.operations[0].outcome = GuardOutcome::Ambiguous { error: "response lost".into() }),
        ("recovery traffic", |h| h.operations[0].phase = GuardPhase::Recovered),
    ];
    for (name, mutate) in cases {
        let mut history = fault_history();
        mutate(&mut history);
        assert!(!fault_traffic(&history, 100, 200), "{name}");
    }
    assert!(!fault_traffic(&fault_history(), 200, 100), "reversed window");
}

#[test]
fn rollback_evidence_requires_the_same_shard_to_finish_a_strict_rewind() {
    let rewind = "reconcile_durable_tail: demotion rewinding write to read shard_id=1 write_wal_seq=12 read_wal_seq=10";
    let cull = "cull_tail: cleared OCC/idempotency LRU + drained pending_replication + dropped parked commits";
    let cases = [
        ("completed strict rewind", format!("{rewind}\n{cull} shard_id=1"), vec![1]),
        ("different shard culled", format!("{rewind}\n{cull} shard_id=2"), vec![]),
        ("unattributed cull", format!("{rewind}\n{cull}"), vec![]),
        ("no rewind", format!("{cull} shard_id=1"), vec![]),
        ("attempted rewind only", rewind.to_string(), vec![]),
        ("cull before rewind", format!("{cull} shard_id=1\n{rewind}"), vec![]),
        ("equal cursors", format!("{}\n{cull} shard_id=1", rewind.replace("write_wal_seq=12", "write_wal_seq=10")), vec![]),
        ("deferred only", "replication_rollback_deferred_total=4".into(), vec![]),
        ("ack barrier rewind", format!("reconcile_durable_tail: demotion rewind to ack barrier shard_id=1 read_wal_seq=12 barrier=10\n{cull} shard_id=1"), vec![1]),
        ("another shard only began its rewind", format!("{rewind}\n{}\n{cull} shard_id=1", rewind.replace("shard_id=1", "shard_id=2")), vec![1]),
    ];
    let mut failures = Vec::new();
    for (name, journal, expected) in cases {
        if completed_rollback_shards(&journal).into_iter().collect::<Vec<_>>() != expected { failures.push(name); }
    }
    assert!(failures.is_empty(), "rollback proof misclassified: {failures:?}");
}

#[test]
fn seed_attribution_requires_a_unique_fresh_post_ack_wal_shard() {
    use celeriant_chaos::guard_fence::seeded_workload_shard;
    let cases: &[(&str, fn(&mut NodeSample), Option<u32>)] = &[
        ("sole seeded shard", |_| {}, Some(2)),
        ("zero cursors", |s| { s.wal_seq_by_shard.insert(2, 0); }, None),
        ("missing cursors", |s| s.wal_seq_by_shard.clear(), None),
        ("multiple active shards", |s| { s.wal_seq_by_shard.insert(1, 1); }, None),
        ("failed scrape", |s| s.ok = false, None),
        ("sample before ACK", |s| s.t_ms = 7999, None),
        ("sample at ACK", |s| s.t_ms = 8000, None),
        ("stale after ACK", |s| s.t_ms = 8500, None),
        ("future sample", |s| s.t_ms = 11001, None),
        ("reserved coordinator not inferred as data shard", |s| {
            s.wal_seq_by_shard.insert(2, 0);
            s.wal_seq_by_shard.insert(3, 3);
        }, Some(3)),
    ];
    for (name, mutate, expected) in cases {
        let mut observed = sample("actual leader", 10500, true);
        observed.wal_seq_by_shard = [(0, 0), (1, 0), (2, 3), (3, 0)].into();
        mutate(&mut observed);
        assert_eq!(seeded_workload_shard(&observed, 8000, 11000), *expected, "{name}");
    }
}

use std::collections::HashMap;
use std::sync::Arc;

use celeriant_bench::guard_fence::{
    anchor_key, check_guard_history, fence_keys, guard_key, GuardHistory, GuardOperation, GuardOutcome, GuardParticipant, GuardPhase, GuardReadState,
    GuardReadback, ReadbackStage, HISTORY_VERSION,
};
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_msg::response::responses::AggregateConflict;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

const SEED: u64 = 24092401;
const LOAD_OK: u64 = 2;
const LOAD_CONFLICT: u64 = 4;
const REJECTED: u64 = 7;
const AMBIGUOUS_PRESENT: u64 = 8;
const AMBIGUOUS_ABSENT: u64 = 9;
const ADVANCE: u64 = 12;
const STALE: u64 = 13;

fn nodes() -> Vec<String> {
    vec!["cs1".into(), "cs2".into()]
}

fn event(id: u64) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: id,
        event_id: Some(u128::from(id)),
        event_timestamp: id * 1000,
        event_type_major: 1,
        event_type_minor: 2,
        event_value: Arc::new(format!("guard-history-payload-{id}").into_bytes()),
        ..Default::default()
    }
}

fn append(key: AggregateKey, expected_version: u64, id: u64) -> GuardParticipant {
    GuardParticipant {
        key,
        expected_version,
        allow_create: expected_version == 0,
        enforce_client_idempotency: false,
        events: vec![event(id)],
    }
}

fn guard(key: AggregateKey, expected_version: u64) -> GuardParticipant {
    GuardParticipant {
        key,
        expected_version,
        allow_create: false,
        enforce_client_idempotency: false,
        events: vec![],
    }
}

fn targets(attempt: u64) -> [AggregateKey; 2] {
    [guard_key(SEED, u128::from(attempt) * 100), guard_key(SEED, u128::from(attempt) * 100 + 1)]
}

fn pair(attempt: u64, pin: u64) -> Vec<GuardParticipant> {
    let [f, g] = fence_keys(SEED);
    let [t, u] = targets(attempt);
    vec![
        guard(g.clone(), pin),
        append(t, 0, attempt * 10),
        guard(f.clone(), pin),
        append(u, 0, attempt * 10 + 1),
    ]
}

fn conflicts(pin: u64, current: u64, anchor: bool) -> GuardOutcome {
    let [f, g] = fence_keys(SEED);
    let mut keys = vec![f, g];
    if anchor {
        keys.push(anchor_key(SEED));
    }
    GuardOutcome::Conflict {
        conflicts: keys.into_iter().map(|key| AggregateConflict { key, expected: pin, current }).collect(),
    }
}

fn operation(attempt_id: u64, phase: GuardPhase, participants: Vec<GuardParticipant>, outcome: GuardOutcome) -> GuardOperation {
    GuardOperation {
        attempt_id,
        phase,
        client_id: 1000 + u128::from(attempt_id),
        correlation_id: attempt_id,
        start_ns: attempt_id * 100,
        end_ns: attempt_id * 100 + 50,
        participants,
        outcome,
    }
}

fn accepted() -> GuardOutcome {
    GuardOutcome::Accepted { max_aggregate_version: None }
}

fn rejected_round(attempt: u64, phase: GuardPhase) -> GuardOperation {
    let mut participants = pair(attempt, 0);
    participants.insert(1, append(anchor_key(SEED), 0, attempt * 10 + 2));
    operation(attempt, phase, participants, conflicts(0, 1, true))
}

fn complete_history() -> GuardHistory {
    let [f, g] = fence_keys(SEED);
    let a = anchor_key(SEED);
    let mut single = pair(5, 1);
    single.pop();
    let operations = vec![
        operation(
            1,
            GuardPhase::Seed,
            vec![append(f.clone(), 0, 10), append(g.clone(), 0, 11), append(a, 0, 12)],
            accepted(),
        ),
        operation(2, GuardPhase::Load, pair(2, 1), accepted()),
        operation(3, GuardPhase::Load, pair(3, 1), accepted()),
        rejected_round(4, GuardPhase::Load),
        operation(
            5,
            GuardPhase::Load,
            single,
            GuardOutcome::Accepted {
                max_aggregate_version: Some(1),
            },
        ),
        operation(6, GuardPhase::Load, vec![guard(f.clone(), 1), guard(g.clone(), 1)], accepted()),
        operation(
            7,
            GuardPhase::Load,
            pair(7, 1),
            GuardOutcome::Rejected {
                error: "definitive pre-mutation rejection".into(),
            },
        ),
        operation(
            8,
            GuardPhase::Load,
            pair(8, 1),
            GuardOutcome::Ambiguous {
                error: "response lost after commit".into(),
            },
        ),
        operation(
            9,
            GuardPhase::Load,
            pair(9, 1),
            GuardOutcome::Ambiguous {
                error: "connection lost before decision".into(),
            },
        ),
        operation(10, GuardPhase::Recovered, pair(10, 1), accepted()),
        rejected_round(11, GuardPhase::Recovered),
        operation(
            12,
            GuardPhase::Advance,
            vec![append(g.clone(), 1, 121), append(f.clone(), 1, 120)],
            accepted(),
        ),
        operation(13, GuardPhase::Stale, pair(13, 1), conflicts(1, 2, false)),
        operation(14, GuardPhase::Current, pair(14, 2), accepted()),
        operation(
            15,
            GuardPhase::Stale,
            vec![guard(g.clone(), 1), guard(f.clone(), 1)],
            conflicts(1, 2, false),
        ),
    ];
    // These are the decisions made by this fabricated execution, including one lost ACK.
    let committed = [1, 2, 3, 5, 10, 12, 14, AMBIGUOUS_PRESENT];
    let mut contents = HashMap::<AggregateKey, Vec<AggregateEventBatch>>::new();
    for op in &operations {
        for participant in &op.participants {
            let batches = contents.entry(participant.key.clone()).or_default();
            if committed.contains(&op.attempt_id) && !participant.events.is_empty() {
                let version = batches.len() as u64 + 1;
                let mut events = participant.events.clone();
                for (i, event) in events.iter_mut().enumerate() {
                    event.event_seq = version + i as u64;
                }
                batches.push(AggregateEventBatch {
                    aggregate_version: version,
                    client_id: op.client_id,
                    user_id: None,
                    server_timestamp: op.end_ns,
                    events,
                });
            }
        }
    }
    let mut readbacks = Vec::new();
    for stage in [ReadbackStage::Recovered, ReadbackStage::Restarted] {
        for node in nodes() {
            for (key, batches) in &contents {
                readbacks.push(GuardReadback {
                    stage,
                    node: node.clone(),
                    key: key.clone(),
                    state: if batches.is_empty() {
                        GuardReadState::Absent
                    } else {
                        GuardReadState::Present { batches: batches.clone() }
                    },
                });
            }
        }
    }
    GuardHistory {
        format_version: HISTORY_VERSION,
        seed: SEED,
        operations,
        readbacks,
    }
}

fn op_mut(history: &mut GuardHistory, attempt: u64) -> &mut GuardOperation {
    history.operations.iter_mut().find(|op| op.attempt_id == attempt).unwrap()
}

fn conflicts_mut(history: &mut GuardHistory) -> &mut Vec<AggregateConflict> {
    let GuardOutcome::Conflict { conflicts } = &mut op_mut(history, LOAD_CONFLICT).outcome else {
        panic!("fixture conflict")
    };
    conflicts
}

fn read_mut(history: &mut GuardHistory, key: AggregateKey) -> &mut GuardReadState {
    &mut history
        .readbacks
        .iter_mut()
        .find(|read| read.key == key && read.node == "cs1" && read.stage == ReadbackStage::Restarted)
        .unwrap()
        .state
}

fn batches_mut(history: &mut GuardHistory, key: AggregateKey) -> &mut Vec<AggregateEventBatch> {
    let GuardReadState::Present { batches } = read_mut(history, key) else {
        panic!("fixture present")
    };
    batches
}

fn land_attempt(history: &mut GuardHistory, attempt: u64) {
    let op = op_mut(history, attempt).clone();
    for participant in op.participants.iter().filter(|p| targets(attempt).contains(&p.key)) {
        let mut events = participant.events.clone();
        events[0].event_seq = 1;
        *read_mut(history, participant.key.clone()) = GuardReadState::Present {
            batches: vec![AggregateEventBatch {
                aggregate_version: 1,
                client_id: op.client_id,
                user_id: None,
                server_timestamp: op.end_ns,
                events,
            }],
        };
    }
}

fn omit_attempt(history: &mut GuardHistory, attempt: u64) {
    history.operations.retain(|op| op.attempt_id != attempt);
    history.readbacks.retain(|read| !targets(attempt).contains(&read.key));
}

fn copy_restart_to_every_read(history: &mut GuardHistory, key: AggregateKey) {
    let state = read_mut(history, key.clone()).clone();
    for read in history.readbacks.iter_mut().filter(|read| read.key == key) {
        read.state = state.clone();
    }
}

#[test]
fn complete_nonvacuous_history_accepts_committed_and_absent_ambiguous_pairs() {
    let checks = check_guard_history(&complete_history(), &nodes());
    assert!(!checks.is_empty(), "a checker must report its verdict");
    assert!(checks.iter().all(|check| check.passed), "valid history rejected: {checks:#?}");
}

#[test]
fn observable_atomicity_payload_conflict_and_evidence_defects_fail() {
    let cases: &[(&str, fn(&mut GuardHistory))] = &[
        ("accepted pair partly absent everywhere", |h| {
            *read_mut(h, targets(LOAD_OK)[0].clone()) = GuardReadState::Absent;
            copy_restart_to_every_read(h, targets(LOAD_OK)[0].clone());
        }),
        ("accepted pair entirely lost", |h| {
            for read in &mut h.readbacks {
                if targets(LOAD_OK).contains(&read.key) {
                    read.state = GuardReadState::Absent;
                }
            }
        }),
        ("typed OCC rejection left target appends everywhere", |h| {
            land_attempt(h, LOAD_CONFLICT);
            for key in targets(LOAD_CONFLICT) {
                copy_restart_to_every_read(h, key);
            }
        }),
        ("non-OCC definitive rejection left appends everywhere", |h| {
            land_attempt(h, REJECTED);
            for key in targets(REJECTED) {
                copy_restart_to_every_read(h, key);
            }
        }),
        ("rejected anchor append survived", |h| {
            let mut extra = batches_mut(h, anchor_key(SEED))[0].clone();
            extra.aggregate_version = 2;
            extra.events = vec![event(42)];
            batches_mut(h, anchor_key(SEED)).push(extra);
        }),
        ("guard added fence event", |h| {
            let batches = batches_mut(h, fence_keys(SEED)[0].clone());
            let mut extra = batches[0].clone();
            extra.aggregate_version = 3;
            extra.events = vec![event(999)];
            batches.push(extra);
        }),
        ("missing conflict", |h| {
            conflicts_mut(h).pop();
        }),
        ("empty conflicts", |h| conflicts_mut(h).clear()),
        ("unsorted conflicts", |h| conflicts_mut(h).swap(0, 2)),
        ("duplicate conflict", |h| {
            let first = conflicts_mut(h)[0].clone();
            conflicts_mut(h).push(first);
        }),
        ("wrong conflict key", |h| conflicts_mut(h)[0].key = targets(LOAD_CONFLICT)[0].clone()),
        ("wrong conflict expectation", |h| conflicts_mut(h)[0].expected = 1),
        ("wrong conflict current head", |h| conflicts_mut(h)[0].current = 2),
        ("guard-only conflict omits participant", |h| {
            let GuardOutcome::Conflict { conflicts } = &mut op_mut(h, 15).outcome else {
                panic!("fixture conflict")
            };
            conflicts.pop();
        }),
        ("ambiguous pair partly present everywhere", |h| {
            *read_mut(h, targets(AMBIGUOUS_PRESENT)[0].clone()) = GuardReadState::Absent;
            copy_restart_to_every_read(h, targets(AMBIGUOUS_PRESENT)[0].clone());
        }),
        ("ambiguous pair diverged between nodes", |h| {
            for read in &mut h.readbacks {
                if read.node == "cs1" && targets(AMBIGUOUS_PRESENT).contains(&read.key) {
                    read.state = GuardReadState::Absent;
                }
            }
        }),
        ("ambiguous pair diverged after restart", |h| {
            for read in &mut h.readbacks {
                if read.stage == ReadbackStage::Restarted && targets(AMBIGUOUS_PRESENT).contains(&read.key) {
                    read.state = GuardReadState::Absent;
                }
            }
        }),
        ("absent ambiguous pair appeared on one node", |h| land_attempt(h, AMBIGUOUS_ABSENT)),
        ("ambiguous request with a stale guard committed", |h| {
            for participant in &mut op_mut(h, AMBIGUOUS_PRESENT).participants {
                if participant.events.is_empty() {
                    participant.expected_version = 0;
                }
            }
        }),
        ("acknowledged payload changed everywhere", |h| {
            batches_mut(h, targets(LOAD_OK)[0].clone())[0].events[0].event_value = Arc::new(b"corrupt".to_vec());
            copy_restart_to_every_read(h, targets(LOAD_OK)[0].clone());
        }),
        ("acknowledged event identity changed", |h| {
            batches_mut(h, targets(LOAD_OK)[0].clone())[0].events[0].event_id = Some(999)
        }),
        ("acknowledged batch version changed", |h| {
            batches_mut(h, targets(LOAD_OK)[0].clone())[0].aggregate_version = 2
        }),
        ("acknowledged client identity changed", |h| {
            batches_mut(h, targets(LOAD_OK)[0].clone())[0].client_id = 999
        }),
        ("acknowledged event sequence changed", |h| {
            batches_mut(h, targets(LOAD_OK)[0].clone())[0].events[0].event_seq = 2
        }),
        ("ambiguous payload corrupted", |h| {
            batches_mut(h, targets(AMBIGUOUS_PRESENT)[0].clone())[0].events[0].event_value = Arc::new(b"corrupt".to_vec())
        }),
        ("missing one readback", |h| {
            h.readbacks.pop();
        }),
        ("missing rejected target evidence", |h| {
            h.readbacks.retain(|r| r.key != targets(LOAD_CONFLICT)[0])
        }),
        ("missing node", |h| h.readbacks.retain(|r| r.node != "cs2")),
        ("missing recovery stage", |h| h.readbacks.retain(|r| r.stage != ReadbackStage::Recovered)),
        ("missing restart stage", |h| h.readbacks.retain(|r| r.stage != ReadbackStage::Restarted)),
        ("readback error", |h| {
            *read_mut(h, targets(LOAD_OK)[0].clone()) = GuardReadState::Error {
                error: "read timed out".into(),
            }
        }),
        ("duplicate readback masks evidence loss", |h| {
            h.readbacks[1] = h.readbacks[0].clone();
        }),
        ("no activity", |h| {
            h.operations.clear();
            h.readbacks.clear();
        }),
        ("missing successful current-head round", |h| omit_attempt(h, 14)),
        ("missing successful recovered round", |h| omit_attempt(h, 10)),
        ("missing recovered definitive conflict", |h| omit_attempt(h, 11)),
        ("missing load definitive conflict", |h| omit_attempt(h, LOAD_CONFLICT)),
        ("unsupported history format", |h| h.format_version = HISTORY_VERSION + 1),
        ("duplicate attempt identity", |h| op_mut(h, 3).attempt_id = LOAD_OK),
        ("duplicate correlation identity", |h| op_mut(h, 3).correlation_id = LOAD_OK),
        ("missing complete request participants", |h| {
            op_mut(h, LOAD_OK).participants.clear();
        }),
        ("one append response has no version", |h| op_mut(h, 5).outcome = accepted()),
        ("guard-only response has a version", |h| {
            op_mut(h, 6).outcome = GuardOutcome::Accepted {
                max_aggregate_version: Some(1),
            }
        }),
        ("paired append response has a version", |h| {
            op_mut(h, LOAD_OK).outcome = GuardOutcome::Accepted {
                max_aggregate_version: Some(1),
            }
        }),
        ("stale probe starts before advance ACK", |h| {
            let end = op_mut(h, ADVANCE).end_ns;
            op_mut(h, STALE).start_ns = end - 1;
        }),
    ];
    let mut escaped = Vec::new();
    for (name, mutate) in cases {
        let mut history = complete_history();
        mutate(&mut history);
        let checks = check_guard_history(&history, &nodes());
        let failures: Vec<_> = checks.iter().filter(|check| !check.passed).collect();
        if failures.is_empty() {
            escaped.push(*name);
        } else {
            assert!(
                failures.iter().all(|check| !check.detail.is_empty()),
                "{name}: missing failure explanation: {checks:#?}"
            );
        }
    }
    assert!(escaped.is_empty(), "invalid histories passed: {escaped:#?}");
}

#[test]
fn missing_expected_node_manifest_cannot_pass_vacuously() {
    let checks = check_guard_history(&complete_history(), &[]);
    assert!(checks.iter().any(|check| !check.passed), "empty node manifest passed: {checks:#?}");
}

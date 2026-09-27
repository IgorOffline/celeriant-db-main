//! Bounded multi-aggregate guard workload and lossless binary recovery evidence.

use bincode::{Decode, Encode};
use celeriant_msg::response::aggregate_event_batch::AggregateEventBatch;
use celeriant_msg::response::responses::AggregateConflict;
use celeriant_wal::aggregate_key::AggregateKey;
use celeriant_wal::datablocks::datablock_aggregate_event::DatablockAggregateEvent;

pub const HISTORY_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum GuardPhase {
    Seed,
    Load,
    Recovered,
    Advance,
    Stale,
    Current,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct GuardParticipant {
    pub key: AggregateKey,
    pub expected_version: u64,
    pub allow_create: bool,
    pub enforce_client_idempotency: bool,
    pub events: Vec<DatablockAggregateEvent>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum GuardOutcome {
    Accepted { max_aggregate_version: Option<u64> },
    Conflict { conflicts: Vec<AggregateConflict> },
    Rejected { error: String },
    Ambiguous { error: String },
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct GuardOperation {
    pub attempt_id: u64,
    pub phase: GuardPhase,
    pub client_id: u128,
    pub correlation_id: u64,
    pub start_ns: u64,
    pub end_ns: u64,
    pub participants: Vec<GuardParticipant>,
    pub outcome: GuardOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum ReadbackStage {
    Recovered,
    Restarted,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum GuardReadState {
    Present { batches: Vec<AggregateEventBatch> },
    Absent,
    Error { error: String },
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct GuardReadback {
    pub stage: ReadbackStage,
    pub node: String,
    pub key: AggregateKey,
    pub state: GuardReadState,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct GuardHistory {
    pub format_version: u32,
    pub seed: u64,
    pub operations: Vec<GuardOperation>,
    pub readbacks: Vec<GuardReadback>,
}

#[derive(Debug, Clone)]
pub struct GuardCheck {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

/// All keys share an aggregate id, so default AggregateId routing keeps the
/// transaction together independently of the number of data shards.
pub fn guard_key(seed: u64, type_id: u128) -> AggregateKey {
    AggregateKey::new((u128::from(seed) << 64) | 0x4755415244, type_id, 1)
}

pub fn fence_keys(seed: u64) -> [AggregateKey; 2] {
    [guard_key(seed, 10), guard_key(seed, 20)]
}

pub fn anchor_key(seed: u64) -> AggregateKey {
    guard_key(seed, 30)
}

/// Check complete payload evidence on every named node at both recovery stages.
pub fn check_guard_history(history: &GuardHistory, expected_nodes: &[String]) -> Vec<GuardCheck> {
    check_history(history, expected_nodes, &[ReadbackStage::Recovered, ReadbackStage::Restarted])
}

use crate::Pool;
use celeriant_client_tokio::server_error::ReadError;
use celeriant_client_tokio::{ClientError, ServerError, WriteError};
use celeriant_msg::request::requests::{SingleAggregateWrite, WriteRequest};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn new_history(seed: u64) -> GuardHistory {
    GuardHistory {
        format_version: HISTORY_VERSION,
        seed,
        operations: Vec::new(),
        readbacks: Vec::new(),
    }
}

pub fn write_history(path: &Path, history: &GuardHistory) -> Result<(), String> {
    let bytes = bincode::encode_to_vec(history, bincode::config::standard()).map_err(|e| e.to_string())?;
    let temporary = path.with_extension("bin.tmp");
    std::fs::write(&temporary, bytes).map_err(|e| e.to_string())?;
    std::fs::rename(temporary, path).map_err(|e| e.to_string())
}

pub fn read_history(path: &Path) -> Result<GuardHistory, String> {
    let bytes = std::fs::read(path).map_err(|e| e.to_string())?;
    let (history, consumed) = bincode::decode_from_slice(&bytes, bincode::config::standard().with_limit::<67108864>()).map_err(|e| e.to_string())?;
    if consumed != bytes.len() {
        return Err("trailing binary history data".into());
    }
    Ok(history)
}

fn event(seed: u64, attempt: u64, slot: u64) -> DatablockAggregateEvent {
    DatablockAggregateEvent {
        client_seq: attempt,
        event_id: Some((u128::from(attempt) << 64) | u128::from(slot)),
        event_type_major: 1,
        event_value: Arc::new(format!("guard/{seed}/{attempt}/{slot}").into_bytes()),
        ..Default::default()
    }
}

fn append(seed: u64, attempt: u64, key: AggregateKey, expected: u64, slot: u64) -> GuardParticipant {
    GuardParticipant {
        key,
        expected_version: expected,
        allow_create: expected == 0,
        enforce_client_idempotency: false,
        events: vec![event(seed, attempt, slot)],
    }
}

fn guard(key: AggregateKey, expected: u64) -> GuardParticipant {
    GuardParticipant {
        key,
        expected_version: expected,
        allow_create: false,
        enforce_client_idempotency: false,
        events: Vec::new(),
    }
}

async fn submit(pool: &Pool, seed: u64, attempt: u64, phase: GuardPhase, mut participants: Vec<GuardParticipant>, epoch: Instant) -> GuardOperation {
    // Vary insertion order independently from the response's numeric key order.
    let rotation = (seed.wrapping_add(attempt) as usize) % participants.len();
    participants.rotate_left(rotation);
    if attempt % 2 == 0 {
        participants.reverse();
    }
    let client_id = (u128::from(seed) << 64) | u128::from(attempt);
    let writes = participants
        .iter()
        .map(|p| {
            (
                p.key.clone(),
                SingleAggregateWrite {
                    events: p.events.clone(),
                    allow_create: p.allow_create,
                    expected_version: Some(p.expected_version),
                    enforce_client_idempotency: p.enforce_client_idempotency,
                },
            )
        })
        .collect();
    let start_ns = epoch.elapsed().as_nanos() as u64;
    let response = pool
        .write(WriteRequest {
            correlation_id: Some(u128::from(attempt)),
            client_id,
            user_id: None,
            writes,
        })
        .await;
    let end_ns = epoch.elapsed().as_nanos() as u64;
    let outcome = match response {
        Ok(r) => GuardOutcome::Accepted {
            max_aggregate_version: r.max_aggregate_version,
        },
        Err(ClientError::Server(ServerError::Write {
            kind: WriteError::OptimisticConcurrencyViolation { conflicts, .. },
            ..
        })) => GuardOutcome::Conflict { conflicts },
        Err(error) => match crate::history::classify_error(&error).0 {
            crate::history::OpOutcome::Fail => GuardOutcome::Rejected { error: format!("{error:?}") },
            _ => GuardOutcome::Ambiguous { error: format!("{error:?}") },
        },
    };
    GuardOperation {
        attempt_id: attempt,
        phase,
        client_id,
        correlation_id: attempt,
        start_ns,
        end_ns,
        participants,
        outcome,
    }
}

pub async fn seed_guards(pool: &Pool, seed: u64, attempt: u64, epoch: Instant) -> GuardOperation {
    let keys = [fence_keys(seed)[0].clone(), fence_keys(seed)[1].clone(), anchor_key(seed)];
    let participants = keys.into_iter().enumerate().map(|(i, k)| append(seed, attempt, k, 0, i as u64)).collect();
    submit(pool, seed, attempt, GuardPhase::Seed, participants, epoch).await
}

pub async fn advance_guards(pool: &Pool, seed: u64, attempt: u64, epoch: Instant) -> GuardOperation {
    let participants = fence_keys(seed)
        .into_iter()
        .enumerate()
        .map(|(i, k)| append(seed, attempt, k, 1, i as u64))
        .collect();
    submit(pool, seed, attempt, GuardPhase::Advance, participants, epoch).await
}

/// Each worker submits a pair, a complete three-conflict rejection, a guard-only
/// observation, and a single append. Attempts are never replayed, even on timeout.
/// Stale rounds pin the advanced fences at 1 and expect every request to reject.
/// Returns the next unused attempt id and every completed operation.
pub async fn run_guard_round(
    pool: &Arc<Pool>,
    seed: u64,
    first_attempt: u64,
    phase: GuardPhase,
    writers: usize,
    epoch: Instant,
) -> Result<(u64, Vec<GuardOperation>), String> {
    if !(2..=16).contains(&writers) {
        return Err("guard writers must be 2..=16".into());
    }
    let mut tasks = tokio::task::JoinSet::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(writers));
    for writer in 0..writers {
        let pool = Arc::clone(pool);
        let barrier = Arc::clone(&barrier);
        tasks.spawn(async move {
            barrier.wait().await;
            let mut records = Vec::with_capacity(4);
            for kind in 0..4u64 {
                let attempt = first_attempt + writer as u64 * 4 + kind;
                let head = if phase == GuardPhase::Current { 2 } else { 1 };
                let pin = if kind == 1 { 0 } else { head };
                let mut participants: Vec<_> = fence_keys(seed).into_iter().map(|key| guard(key, pin)).collect();
                let targets = match kind {
                    0 | 1 => 2,
                    2 => 0,
                    _ => 1,
                };
                for slot in 0..targets {
                    participants.push(append(
                        seed,
                        attempt,
                        guard_key(seed, 1000 + u128::from(attempt) * 2 + slot),
                        0,
                        slot as u64,
                    ));
                }
                if kind == 1 {
                    let mut anchor = append(seed, attempt, anchor_key(seed), 0, 2);
                    anchor.allow_create = false;
                    participants.push(anchor);
                }
                records.push(submit(&pool, seed, attempt, phase, participants, epoch).await);
            }
            records
        });
    }
    let mut records = Vec::with_capacity(writers * 4);
    while let Some(result) = tasks.join_next().await {
        records.extend(result.map_err(|e| format!("guard worker: {e}"))?);
    }
    records.sort_by_key(|r| r.attempt_id);
    Ok((first_attempt + writers as u64 * 4, records))
}

pub fn history_keys(history: &GuardHistory) -> Vec<AggregateKey> {
    let mut keys: Vec<_> = history
        .operations
        .iter()
        .flat_map(|op| op.participants.iter().map(|p| p.key.clone()))
        .collect();
    keys.sort_by_key(key_tuple);
    keys.dedup();
    keys
}

pub async fn read_guard_state(pool: &Pool, key: &AggregateKey) -> GuardReadState {
    let read = async {
        let iter = pool.read_all(key.clone(), Some(crate::ReadFilters::new(1))).await?;
        iter.collect().await
    }
    .await;
    match read {
        Ok(batches) if batches.is_empty() => GuardReadState::Absent,
        Ok(batches) => GuardReadState::Present { batches },
        Err(ClientError::Server(ServerError::Read {
            kind: ReadError::AggregateNotExists,
            ..
        })) => GuardReadState::Absent,
        Err(error) => GuardReadState::Error { error: format!("{error:?}") },
    }
}

pub async fn read_guard_history(pool: &Arc<Pool>, node: &str, stage: ReadbackStage, keys: &[AggregateKey]) -> Result<Vec<GuardReadback>, String> {
    let mut output = Vec::with_capacity(keys.len());
    // Small batches bound both connections and recovery load on the shared hosts.
    for chunk in keys.chunks(8) {
        let mut tasks = tokio::task::JoinSet::new();
        for key in chunk {
            let (pool, key, node) = (Arc::clone(pool), key.clone(), node.to_string());
            tasks.spawn(async move {
                let mut state = read_guard_state(&pool, &key).await;
                for _ in 0..2 {
                    if !matches!(state, GuardReadState::Error { .. }) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    state = read_guard_state(&pool, &key).await;
                }
                GuardReadback { stage, node, key, state }
            });
        }
        while let Some(result) = tasks.join_next().await {
            output.push(result.map_err(|e| format!("guard read worker: {e}"))?);
        }
    }
    output.sort_by_key(|r| key_tuple(&r.key));
    Ok(output)
}

fn key_tuple(key: &AggregateKey) -> (u128, u128, u128) {
    (key.org_id, key.aggregate_type_id, key.aggregate_id)
}

pub fn check_guard_stage(history: &GuardHistory, nodes: &[String], stage: ReadbackStage) -> Vec<GuardCheck> {
    check_history(history, nodes, &[stage])
}

fn check_history(history: &GuardHistory, nodes: &[String], stages: &[ReadbackStage]) -> Vec<GuardCheck> {
    let mut faults: HashMap<&'static str, Vec<String>> = HashMap::new();
    let mut fail = |name, detail: String| {
        faults.entry(name).or_default().push(detail);
    };
    let fences = fence_keys(history.seed);
    let anchor = anchor_key(history.seed);
    let mut seeds = HashSet::new();
    let mut ids = HashSet::new();
    let mut correlations = HashSet::new();
    let mut event_ids = HashSet::new();
    let mut fresh_targets = HashSet::new();
    let mut must_reject = HashSet::new();
    let mut seed_end = 0;
    let advances: Vec<_> = history.operations.iter().filter(|o| o.phase == GuardPhase::Advance).collect();
    if history.format_version != HISTORY_VERSION
        || history.operations.is_empty()
        || nodes.is_empty()
        || nodes.iter().collect::<HashSet<_>>().len() != nodes.len()
    {
        fail("GuardHistoryComplete", "invalid version, empty operations/nodes or duplicate node".into());
    }
    if advances.len() != 1 || advances.first().is_some_and(|o| !matches!(o.outcome, GuardOutcome::Accepted { .. })) {
        fail("GuardFenceAdvance", "requires exactly one acknowledged fence advance".into());
    }
    let advance_end = advances.first().map(|o| o.end_ns).unwrap_or(u64::MAX);
    let advance_start = advances.first().map(|o| o.start_ns).unwrap_or(0);
    let mut load_pairs = 0;
    let mut recovered_ok = 0;
    let mut recovered_conflict = 0;
    let mut load_conflict = 0;
    let mut stale_conflict = 0;
    let mut current_ok = 0;
    let mut only_guard_ok = 0;
    let mut single_ok = 0;
    for op in &history.operations {
        if !ids.insert(op.attempt_id) || !correlations.insert(op.correlation_id) || op.start_ns > op.end_ns || op.participants.is_empty() {
            fail("GuardHistoryComplete", format!("invalid/duplicate operation {}", op.attempt_id));
        }
        let mut keys = HashSet::new();
        let mut expected_conflicts = Vec::new();
        let mut write_count = 0;
        let mut guarded = HashSet::new();
        for p in &op.participants {
            if !keys.insert(p.key.clone()) {
                fail("GuardHistoryComplete", format!("duplicate participant in {}", op.attempt_id));
            }
            if p.events.is_empty() {
                guarded.insert(p.key.clone());
                if p.allow_create || p.enforce_client_idempotency || !fences.contains(&p.key) {
                    fail("GuardHistoryComplete", format!("malformed or unknown guard in {}", op.attempt_id));
                }
            } else {
                write_count += 1;
                for event in &p.events {
                    if event.event_id.is_none() || !event_ids.insert(event.event_id) {
                        fail("GuardHistoryComplete", format!("missing/duplicate event identity in {}", op.attempt_id));
                    }
                }
            }
            let head = if op.phase == GuardPhase::Seed {
                0
            } else if fences.contains(&p.key) {
                if matches!(op.phase, GuardPhase::Stale | GuardPhase::Current) {
                    2
                } else {
                    1
                }
            } else if p.key == anchor {
                1
            } else {
                0
            };
            if p.expected_version != head {
                expected_conflicts.push(AggregateConflict {
                    key: p.key.clone(),
                    expected: p.expected_version,
                    current: head,
                });
            }
            if op.phase == GuardPhase::Seed {
                if ![fences[0].clone(), fences[1].clone(), anchor.clone()].contains(&p.key)
                    || p.events.len() != 1
                    || !p.allow_create
                    || p.expected_version != 0
                    || !seeds.insert(p.key.clone())
                {
                    fail("GuardHistoryComplete", "seed must initialize F/G/A exactly once".into());
                }
                seed_end = seed_end.max(op.end_ns);
                if !matches!(op.outcome, GuardOutcome::Accepted { .. }) {
                    fail("GuardHistoryComplete", "seed was not acknowledged".into());
                }
            } else if op.phase == GuardPhase::Advance {
                if !fences.contains(&p.key) || p.expected_version != 1 || p.events.len() != 1 {
                    fail("GuardFenceAdvance", "advance must append only F/G pinned at 1".into());
                }
            } else if !fences.contains(&p.key) && p.key != anchor {
                if p.events.is_empty() || p.expected_version != 0 || !p.allow_create || !fresh_targets.insert(p.key.clone()) {
                    fail("GuardHistoryComplete", format!("target reused/not fresh in {}", op.attempt_id));
                }
            }
        }
        if op.phase == GuardPhase::Advance && (keys.len() != 2 || write_count != 2) {
            fail("GuardFenceAdvance", "advance must contain both fences".into());
        }
        if !matches!(op.phase, GuardPhase::Seed | GuardPhase::Advance) && (guarded.len() != 2 || !fences.iter().all(|f| guarded.contains(f))) {
            fail("GuardHistoryComplete", format!("{} does not guard both fences", op.attempt_id));
        }
        if matches!(op.phase, GuardPhase::Stale | GuardPhase::Current) && op.start_ns < advance_end {
            fail("GuardFenceAdvance", "post-advance operation started before advance ACK".into());
        }
        if matches!(op.phase, GuardPhase::Load | GuardPhase::Recovered) && op.end_ns > advance_start {
            fail("GuardFenceAdvance", "load/recovery operation overlaps advance".into());
        }
        expected_conflicts.sort_by_key(|c| key_tuple(&c.key));
        if !expected_conflicts.is_empty() {
            must_reject.insert(op.attempt_id);
        }
        match &op.outcome {
            GuardOutcome::Accepted { max_aggregate_version } => {
                let expected_version = if write_count == 1 {
                    op.participants.iter().find(|p| !p.events.is_empty()).map(|p| p.expected_version + 1)
                } else {
                    None
                };
                if !expected_conflicts.is_empty() || *max_aggregate_version != expected_version {
                    fail("GuardResponses", format!("invalid accepted decision/version for {}", op.attempt_id));
                }
                match op.phase {
                    GuardPhase::Load if write_count == 2 => load_pairs += 1,
                    GuardPhase::Recovered => recovered_ok += 1,
                    GuardPhase::Current => current_ok += 1,
                    _ => {}
                }
                if write_count == 0 {
                    only_guard_ok += 1;
                }
                if write_count == 1 && !guarded.is_empty() {
                    single_ok += 1;
                }
            }
            GuardOutcome::Conflict { conflicts } => {
                if conflicts.is_empty() || *conflicts != expected_conflicts {
                    fail(
                        "GuardConflictsComplete",
                        format!("wrong/incomplete/unsorted conflicts for {}", op.attempt_id),
                    );
                }
                match op.phase {
                    GuardPhase::Load if conflicts.len() >= 3 => load_conflict += 1,
                    GuardPhase::Recovered if conflicts.len() >= 3 => recovered_conflict += 1,
                    GuardPhase::Stale => stale_conflict += 1,
                    _ => {}
                }
            }
            GuardOutcome::Rejected { error } | GuardOutcome::Ambiguous { error } if error.is_empty() => {
                fail("GuardHistoryComplete", format!("missing error for {}", op.attempt_id));
            }
            _ => {}
        }
    }
    if seeds.len() != 3 || history.operations.iter().any(|o| o.phase != GuardPhase::Seed && o.start_ns < seed_end) {
        fail("GuardHistoryComplete", "missing seeds or operation before seed ACK".into());
    }
    if load_pairs < 2
        || load_conflict == 0
        || recovered_ok == 0
        || recovered_conflict == 0
        || stale_conflict == 0
        || current_ok == 0
        || only_guard_ok == 0
        || single_ok == 0
    {
        fail(
            "GuardWorkloadExercised",
            format!(
                "load pairs={load_pairs}, load conflicts={load_conflict}, recovered={recovered_ok}/{recovered_conflict}, stale={stale_conflict}, current={current_ok}, guard-only={only_guard_ok}, single={single_ok}"
            ),
        );
    }
    let keys = history_keys(history);
    let mut reads = HashMap::new();
    for read in &history.readbacks {
        let index = (read.stage as u8, read.node.clone(), read.key.clone());
        if reads.insert(index, read).is_some() || !nodes.contains(&read.node) || !keys.contains(&read.key) {
            fail("GuardReadbackComplete", "duplicate or unexpected readback".into());
        }
    }
    for key in &keys {
        let candidates: Vec<_> = history
            .operations
            .iter()
            .flat_map(|op| {
                op.participants
                    .iter()
                    .filter(move |p| p.key == *key && !p.events.is_empty())
                    .map(move |p| (op, p))
            })
            .collect();
        let mut canonical = None;
        for &stage in stages {
            for node in nodes {
                let Some(read) = reads.get(&(stage as u8, node.clone(), key.clone())) else {
                    fail("GuardReadbackComplete", format!("missing {stage:?} read on {node} for {key:?}"));
                    continue;
                };
                let batches: &[AggregateEventBatch] = match &read.state {
                    GuardReadState::Present { batches } if !batches.is_empty() => batches,
                    GuardReadState::Absent => &[],
                    _ => {
                        fail("GuardReadbackComplete", format!("unreadable/empty-present {key:?} on {node}"));
                        continue;
                    }
                };
                let encoded = bincode::encode_to_vec(batches, bincode::config::standard()).expect("batch encoding");
                if let Some(first) = &canonical {
                    if *first != encoded {
                        fail("GuardReplicaParity", format!("readback differs for {key:?} on {node}/{stage:?}"));
                    }
                } else {
                    canonical = Some(encoded);
                }
                let mut seen = HashSet::new();
                let mut event_seq = 0;
                for (index, batch) in batches.iter().enumerate() {
                    if batch.aggregate_version != index as u64 + 1 {
                        fail("GuardPayloads", format!("batch version gap for {key:?}"));
                    }
                    let matched = candidates.iter().find(|(op, p)| batch_matches(batch, op, p));
                    let Some((op, _p)) = matched else {
                        fail("GuardPayloads", format!("unknown/altered/partial payload for {key:?} on {node}"));
                        continue;
                    };
                    if !seen.insert(op.attempt_id)
                        || must_reject.contains(&op.attempt_id)
                        || matches!(op.outcome, GuardOutcome::Conflict { .. } | GuardOutcome::Rejected { .. })
                    {
                        fail(
                            "GuardAtomicity",
                            format!("duplicate or rejected append {} present on {node}", op.attempt_id),
                        );
                    }
                    for event in &batch.events {
                        event_seq += 1;
                        if event.event_seq != event_seq {
                            fail("GuardPayloads", format!("event sequence mismatch for {key:?}"));
                        }
                    }
                }
                for (op, _) in &candidates {
                    if matches!(op.outcome, GuardOutcome::Accepted { .. }) && !seen.contains(&op.attempt_id) {
                        fail(
                            "GuardDurability",
                            format!("acknowledged append {} absent on {node}/{stage:?}", op.attempt_id),
                        );
                    }
                }
            }
        }
    }
    for op in &history.operations {
        for &stage in stages {
            for node in nodes {
                let states: Vec<_> = op
                    .participants
                    .iter()
                    .filter(|p| !p.events.is_empty())
                    .filter_map(|p| {
                        let read = reads.get(&(stage as u8, node.clone(), p.key.clone()))?;
                        match &read.state {
                            GuardReadState::Present { batches } => Some(batches.iter().any(|b| batch_matches(b, op, p))),
                            GuardReadState::Absent => Some(false),
                            GuardReadState::Error { .. } => None,
                        }
                    })
                    .collect();
                if states.iter().any(|s| *s) && states.iter().any(|s| !s) {
                    fail("GuardAtomicity", format!("partial attempt {} on {node}/{stage:?}", op.attempt_id));
                }
            }
        }
    }
    [
        "GuardHistoryComplete",
        "GuardFenceAdvance",
        "GuardResponses",
        "GuardConflictsComplete",
        "GuardWorkloadExercised",
        "GuardReadbackComplete",
        "GuardReplicaParity",
        "GuardPayloads",
        "GuardAtomicity",
        "GuardDurability",
    ]
    .into_iter()
    .map(|name| {
        let failures = faults.remove(name).unwrap_or_default();
        GuardCheck {
            name,
            passed: failures.is_empty(),
            detail: if failures.is_empty() {
                "complete evidence passed".into()
            } else {
                format!(
                    "{} violation(s): {}",
                    failures.len(),
                    failures.into_iter().take(5).collect::<Vec<_>>().join("; ")
                )
            },
        }
    })
    .collect()
}

fn batch_matches(batch: &AggregateEventBatch, op: &GuardOperation, p: &GuardParticipant) -> bool {
    batch.aggregate_version == p.expected_version + 1
        && batch.client_id == op.client_id
        && batch.user_id.is_none()
        && batch.events.len() == p.events.len()
        && batch.events.iter().zip(&p.events).all(|(a, b)| {
            a.event_id == b.event_id
                && a.client_seq == b.client_seq
                && a.event_timestamp == b.event_timestamp
                && a.event_type_major == b.event_type_major
                && a.event_type_minor == b.event_type_minor
                && a.event_value == b.event_value
                && a.iv == b.iv
        })
}

//! Guard-bearing fault scenarios restricted to the shared Pi test namespace.

use crate::actions::{Action, run_make_in};
use crate::config::ClusterConfig;
use crate::invariants::{CheckResult, ScenarioExpectations};
use crate::scenario::{ClusterUp, ScenarioParams, ScenarioReport, bring_up_cluster, build_bench_pool, tear_down_and_evaluate_with_audit};
use celeriant_bench::guard_fence::{self as workload, GuardHistory, GuardOutcome, GuardPhase, ReadbackStage};
use celeriant_bench::{BenchmarkResult, Pool, PoolBuilder};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardFault {
    Contention,
    LeaderKill,
    PartitionHeal,
    Rollback,
}

impl GuardFault {
    pub fn name(self) -> &'static str {
        match self {
            Self::Contention => "guard_fence_contention",
            Self::LeaderKill => "guard_fence_leader_kill",
            Self::PartitionHeal => "guard_fence_partition_heal",
            Self::Rollback => "guard_fence_rollback",
        }
    }
}

async fn action(cfg: &ClusterConfig, action: Action) -> Result<(), String> {
    let deploy_dir = cfg.deploy_dir.clone();
    tokio::task::spawn_blocking(move || run_make_in(&deploy_dir, action.make_target(), &action.make_vars()))
        .await
        .map_err(|e| format!("action task: {e}"))?
}

async fn target(cfg: &ClusterConfig, target: &'static str) -> Result<(), String> {
    let deploy_dir = cfg.deploy_dir.clone();
    tokio::task::spawn_blocking(move || run_make_in(&deploy_dir, target, &[]))
        .await
        .map_err(|e| format!("make task: {e}"))?
}

fn test_namespace(cfg: &ClusterConfig) -> Result<(), String> {
    if cfg.leader_host != "192.168.88.214"
        || cfg.follower_host != "192.168.88.213"
        || cfg.infra_host.as_deref() != Some("192.168.88.78")
        || (cfg.client_port, cfg.replication_port, cfg.metrics_port, cfg.s3_port) != (10000, 10001, 9090, 9000)
    {
        return Err("guard Pi scenarios require the verified cs1/cs2 test namespace and cs3:9000".into());
    }
    Ok(())
}

fn partitions(cfg: &ClusterConfig, leader: &str, fault: GuardFault) -> Vec<Action> {
    let mut rules = Vec::new();
    if matches!(fault, GuardFault::PartitionHeal | GuardFault::Rollback) {
        rules.push(Action::Partition {
            src: cfg.leader_host.clone(),
            dst: cfg.follower_host.clone(),
            port: cfg.replication_port,
        });
        rules.push(Action::Partition {
            src: cfg.follower_host.clone(),
            dst: cfg.leader_host.clone(),
            port: cfg.replication_port,
        });
    }
    if fault == GuardFault::Rollback {
        rules.push(Action::Partition {
            src: leader.to_string(),
            dst: cfg.infra_host.clone().expect("verified infra"),
            port: cfg.s3_port,
        });
    }
    rules
}

async fn heal(cfg: &ClusterConfig, rules: &[Action]) -> Result<(), String> {
    let mut errors = Vec::new();
    for rule in rules.iter().rev() {
        if let Action::Partition { src, dst, port } = rule {
            if let Err(e) = action(
                cfg,
                Action::Heal {
                    src: src.clone(),
                    dst: dst.clone(),
                    port: *port,
                },
            )
            .await
            {
                errors.push(e);
            }
        }
    }
    if errors.is_empty() { Ok(()) } else { Err(errors.join("; ")) }
}

/// A readiness observation identifies the fresh sample(s) it used, allowing the
/// caller to count distinct observations rather than repeatedly reading one tick.
pub fn readiness_observation(
    a: &crate::sample::NodeSample,
    b: &crate::sample::NodeSample,
    now_ms: u64,
    required_leader: Option<&str>,
) -> Option<(u64, u64)> {
    let fresh = |sample: &crate::sample::NodeSample| sample.t_ms <= now_ms && now_ms - sample.t_ms < 2000;
    if let Some(host) = required_leader {
        let expected = if a.host == host {
            a
        } else if b.host == host {
            b
        } else {
            return None;
        };
        return (fresh(expected) && expected.ok && expected.node_role > 0.5).then_some((expected.t_ms, expected.t_ms));
    }
    (fresh(a)
        && fresh(b)
        && a.ok
        && b.ok
        && ((a.node_role > 0.5) != (b.node_role > 0.5))
        && !a.wal_seq_by_shard.is_empty()
        && a.wal_seq_by_shard == b.wal_seq_by_shard
        && a.read_wal_seq_by_shard == a.wal_seq_by_shard
        && b.read_wal_seq_by_shard == b.wal_seq_by_shard)
        .then_some((a.t_ms, b.t_ms))
}

async fn wait_ready(cfg: &ClusterConfig, up: &ClusterUp, required_leader: Option<&str>, budget: Duration) -> Result<(), String> {
    let deadline = Instant::now() + budget;
    let mut consecutive = 0;
    let mut previous = None;
    while Instant::now() < deadline {
        let samples = up.scraper.store().snapshot().await;
        let a = samples.iter().rev().find(|s| s.host == cfg.leader_host);
        let b = samples.iter().rev().find(|s| s.host == cfg.follower_host);
        let observation = match (a, b) {
            (Some(a), Some(b)) => readiness_observation(a, b, up.elapsed_ms(), required_leader),
            _ => None,
        };
        match observation {
            Some(tick) if previous.is_none_or(|prior: (u64, u64)| tick.0 > prior.0 && tick.1 > prior.1) => {
                previous = Some(tick);
                consecutive += 1;
            }
            None => {
                previous = None;
                consecutive = 0;
            }
            _ => {}
        }
        if consecutive >= 3 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(format!("guard recovery did not stabilize within {}s", budget.as_secs()))
}

struct FaultWindow {
    check: CheckResult,
    start_ns: u64,
    end_ns: u64,
}

pub fn fault_traffic(history: &GuardHistory, start_ns: u64, end_ns: u64) -> bool {
    let during: Vec<_> = history
        .operations
        .iter()
        .filter(|op| {
            op.phase == GuardPhase::Load
                && op.start_ns >= start_ns
                && op.end_ns <= end_ns
                && op.participants.iter().filter(|p| p.events.is_empty()).count() == 2
        })
        .collect();
    during
        .iter()
        .any(|op| matches!(op.outcome, GuardOutcome::Accepted { .. }) && op.participants.iter().filter(|p| !p.events.is_empty()).count() == 2)
        && during
            .iter()
            .any(|op| matches!(&op.outcome, GuardOutcome::Conflict { conflicts } if conflicts.len() == 3))
}

async fn fault_window(
    cfg: &ClusterConfig,
    up: &ClusterUp,
    fault: GuardFault,
    leader: &str,
    rules: &[Action],
    run_dir: &PathBuf,
    epoch: Instant,
) -> Result<FaultWindow, String> {
    if fault == GuardFault::Contention {
        return Ok(FaultWindow {
            check: CheckResult::pass_with_detail("GuardFaultExercised", "healthy shared-fence contention"),
            start_ns: 0,
            end_ns: u64::MAX,
        });
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let begin = up.elapsed_ms();
    println!("[{}] inject {:?} at {begin}ms; actual leader {leader}", fault.name(), fault);
    let mut start_ns = epoch.elapsed().as_nanos() as u64;
    let result = async {
        for rule in rules {
            action(cfg, rule.clone()).await?;
        }
        if fault == GuardFault::LeaderKill {
            target(
                cfg,
                if leader == cfg.leader_host {
                    "kill-cs1-no-restart"
                } else {
                    "kill-cs2-no-restart"
                },
            )
            .await?;
        }
        start_ns = epoch.elapsed().as_nanos() as u64;
        let hold = if fault == GuardFault::PartitionHeal { 20 } else { 40 };
        tokio::time::sleep(Duration::from_secs(hold)).await;
        if matches!(fault, GuardFault::LeaderKill | GuardFault::Rollback) {
            let survivor = if leader == cfg.leader_host {
                &cfg.follower_host
            } else {
                &cfg.leader_host
            };
            wait_ready(cfg, up, Some(survivor), Duration::from_secs(20)).await?;
        }
        Ok::<_, String>(())
    }
    .await;
    let end_ns = epoch.elapsed().as_nanos() as u64;
    // Cleanup is unconditional even if installation or promotion failed.
    let cleanup = heal(cfg, rules).await;
    let restart = if fault == GuardFault::LeaderKill {
        action(cfg, if leader == cfg.leader_host { Action::StartCs1 } else { Action::StartCs2 }).await
    } else {
        Ok(())
    };
    let finish = up.elapsed_ms();
    let evidence = format!(
        "scenario={}\nleader={leader}\nstart_ms={begin}\nend_ms={finish}\ninstalled_epoch_ns={start_ns}\nheal_epoch_ns={end_ns}\nactions={rules:?}\nresult={result:?}\nheal={cleanup:?}\nrestart={restart:?}\n",
        fault.name()
    );
    std::fs::write(run_dir.join(format!("{}-fault.txt", fault.name())), evidence).map_err(|e| e.to_string())?;
    result?;
    cleanup?;
    restart?;
    Ok(FaultWindow {
        check: CheckResult::pass_with_detail(
            "GuardFaultExercised",
            format!("{fault:?} applied at {begin}ms, healed at {finish}ms; required role checks passed"),
        ),
        start_ns,
        end_ns,
    })
}

async fn run_load(
    pool: &Arc<Pool>,
    history: &mut GuardHistory,
    path: &PathBuf,
    epoch: Instant,
    duration: u64,
    writers: usize,
    next: &mut u64,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(duration);
    let mut rounds = 0;
    while Instant::now() < deadline && rounds < 90 {
        let (new_next, records) = workload::run_guard_round(pool, history.seed, *next, GuardPhase::Load, writers, epoch).await?;
        *next = new_next;
        history.operations.extend(records);
        workload::write_history(path, history)?;
        rounds += 1;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    println!(
        "guard workload: {rounds} completed rounds, {} recorded operations, requested {duration}s, actual {:.1}s",
        history.operations.len(),
        epoch.elapsed().as_secs_f64()
    );
    Ok(())
}

async fn pinned_pool(cfg: &ClusterConfig, host: &str) -> Result<Arc<Pool>, String> {
    let address = format!("{host}:{}", cfg.client_port);
    PoolBuilder {
        address1: &address,
        address2: &address,
        server_name: Some(host),
        ca_cert: cfg.ca_cert.to_str().ok_or("non-UTF8 CA path")?,
        client_cert: cfg.client_cert.to_str().ok_or("non-UTF8 cert path")?,
        client_key: cfg.client_key.to_str().ok_or("non-UTF8 key path")?,
        plaintext: false,
        max_connections: 8,
    }
    .build_with_request_timeout(Duration::from_secs(5))
    .await
    .map_err(|e| e.to_string())
}

async fn collect_stage(cfg: &ClusterConfig, history: &mut GuardHistory, path: &PathBuf, stage: ReadbackStage) -> Result<(), String> {
    let nodes = vec![cfg.leader_host.clone(), cfg.follower_host.clone()];
    let keys = workload::history_keys(history);
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        history.readbacks.retain(|r| r.stage != stage);
        for node in &nodes {
            let pool = pinned_pool(cfg, node).await?;
            history.readbacks.extend(workload::read_guard_history(&pool, node, stage, &keys).await?);
        }
        workload::write_history(path, history)?;
        let checks = workload::check_guard_stage(history, &nodes, stage);
        if checks.iter().all(|c| c.passed) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{stage:?} readback oracle: {}",
                checks
                    .iter()
                    .filter(|c| !c.passed)
                    .map(|c| format!("{}: {}", c.name, c.detail))
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
        }
        println!(
            "guard {stage:?} readback not yet complete/converged; retrying {} keys on both nodes",
            keys.len()
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

fn field(line: &str, name: &str) -> Option<u64> {
    let rest = line.split_once(&format!("{name}="))?.1;
    rest.chars().take_while(|c| c.is_ascii_digit()).collect::<String>().parse().ok()
}

/// Shards whose journal shows a strict cursor rewind followed by the cull path
/// clearing speculative state. A deferred replication metric is not rollback evidence.
pub fn completed_rollback_shards(text: &str) -> std::collections::BTreeSet<u64> {
    let mut rewinding = std::collections::BTreeSet::new();
    let mut completed = std::collections::BTreeSet::new();
    for line in text.lines() {
        let Some(shard) = field(line, "shard_id") else { continue };
        if line.contains("reconcile_durable_tail: demotion rewinding write to read") {
            if matches!((field(line,"write_wal_seq"),field(line,"read_wal_seq")), (Some(a),Some(b)) if a > b) {
                rewinding.insert(shard);
            }
        }
        if line.contains("reconcile_durable_tail: demotion rewind to ack barrier") {
            if matches!((field(line,"read_wal_seq"),field(line,"barrier")), (Some(a),Some(b)) if a > b) {
                rewinding.insert(shard);
            }
        }
        if line.contains("cull_tail: cleared OCC/idempotency LRU + drained pending_replication + dropped parked commits") && rewinding.remove(&shard) {
            completed.insert(shard);
        }
    }
    completed
}

pub fn seeded_workload_shard(sample: &crate::sample::NodeSample, after_ms: u64, now_ms: u64) -> Option<u32> {
    if !sample.ok || sample.t_ms <= after_ms || sample.t_ms > now_ms || now_ms - sample.t_ms >= 2000 {
        return None;
    }
    let mut active = sample.wal_seq_by_shard.iter().filter(|(_, seq)| **seq > 0);
    let (&shard, _) = active.next()?;
    active.next().is_none().then_some(shard)
}

async fn observe_seed_shard(up: &ClusterUp, leader: &str, run_dir: &PathBuf, fault: GuardFault) -> Result<u32, String> {
    let after_ms = up.elapsed_ms();
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let samples = up.scraper.store().snapshot().await;
        if let Some(sample) = samples.iter().rev().find(|sample| sample.host == leader) {
            if let Some(shard) = seeded_workload_shard(sample, after_ms, up.elapsed_ms()) {
                let evidence = serde_json::to_vec_pretty(sample).map_err(|e| e.to_string())?;
                std::fs::write(run_dir.join(format!("{}-seed-shard.json", fault.name())), evidence).map_err(|e| e.to_string())?;
                return Ok(shard);
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err("sole seed write did not identify exactly one fresh nonzero WAL shard".into())
}

async fn rollback_check(cfg: &ClusterConfig, fault: GuardFault, guard_shard: u32, started: SystemTime, run_dir: &PathBuf) -> Result<CheckResult, String> {
    if fault != GuardFault::Rollback {
        return Ok(CheckResult::pass_with_detail("GuardRollbackEvidence", "not required by this scenario"));
    }
    let mut observed = false;
    for host in [&cfg.leader_host, &cfg.follower_host] {
        let (host, path) = (host.clone(), run_dir.join(format!("{}-{host}-rollback.log", fault.name())));
        let text = tokio::task::spawn_blocking(move || {
            crate::logs::fetch_journal(&host, started, SystemTime::now(), Duration::ZERO, &path)?;
            std::fs::read_to_string(path).map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())??;
        observed |= completed_rollback_shards(&text).contains(&u64::from(guard_shard));
    }
    Ok(if observed {
        CheckResult::pass_with_detail(
            "GuardRollbackEvidence",
            format!("strict WAL cursor rewind and subsequent speculative-state cull recorded on guarded shard {guard_shard}"),
        )
    } else {
        CheckResult::inconclusive(
            "GuardRollbackEvidence",
            "no actual cursor rewind plus cull recorded; deferred replication and a fault label do not close the rollback gate",
        )
    })
}

pub async fn run(cfg: &ClusterConfig, mut params: ScenarioParams, run_dir: &PathBuf, fault: GuardFault) -> Result<ScenarioReport, String> {
    test_namespace(cfg)?;
    if !(2..=16).contains(&params.tasks) {
        return Err("guard scenarios require --tasks 2..16 (recommended 8)".into());
    }
    let minimum = if fault == GuardFault::Contention { 10 } else { 70 };
    if !(minimum..=180).contains(&params.duration_secs) {
        return Err(format!("{} needs --duration {minimum}..180", fault.name()));
    }
    params.max_connections = Some(params.tasks);
    params.throughput_floor = 0.01;
    let started = SystemTime::now();
    let up = bring_up_cluster(cfg, fault.name(), run_dir).await?;
    let leader = up.bench_primary.split(':').next().ok_or("missing leader address")?.to_string();
    let rules = partitions(cfg, &leader, fault);
    let history_path = run_dir.join(format!("{}.history.bin", fault.name()));
    let mut history = workload::new_history(params.seed);
    let epoch = Instant::now();
    let bench_start = up.elapsed_ms();
    let body = async {
        let pool = build_bench_pool(cfg, &up, params).await?;
        history.operations.push(workload::seed_guards(&pool, params.seed, 1, epoch).await);
        workload::write_history(&history_path, &history)?;
        if !matches!(history.operations[0].outcome, GuardOutcome::Accepted { .. }) {
            return Err("guard setup was not acknowledged".into());
        }
        let guard_shard = observe_seed_shard(&up, &leader, run_dir, fault).await?;
        let mut next = 2;
        let (load, injection) = tokio::join!(
            run_load(&pool, &mut history, &history_path, epoch, params.duration_secs, params.tasks, &mut next),
            fault_window(cfg, &up, fault, &leader, &rules, run_dir, epoch)
        );
        load?;
        let injection = injection?;
        wait_ready(cfg, &up, None, Duration::from_secs(90)).await?;
        let (n, records) = workload::run_guard_round(&pool, params.seed, next, GuardPhase::Recovered, params.tasks, epoch).await?;
        next = n;
        history.operations.extend(records);
        let advance = workload::advance_guards(&pool, params.seed, next, epoch).await;
        next += 1;
        let accepted = matches!(advance.outcome, GuardOutcome::Accepted { .. });
        history.operations.push(advance);
        workload::write_history(&history_path, &history)?;
        if !accepted {
            return Err("fence advance was not acknowledged; retained evidence, no ambiguous replay".into());
        }
        for phase in [GuardPhase::Stale, GuardPhase::Current] {
            let (n, records) = workload::run_guard_round(&pool, params.seed, next, phase, params.tasks, epoch).await?;
            next = n;
            history.operations.extend(records);
        }
        workload::write_history(&history_path, &history)?;
        drop(pool);
        collect_stage(cfg, &mut history, &history_path, ReadbackStage::Recovered).await?;
        let rollback = rollback_check(cfg, fault, guard_shard, started, run_dir).await?;
        // Availability checks cover traffic and recovery. The deliberate dual
        // shutdown is verified separately by readiness and full durable readback.
        let bench_end = up.elapsed_ms();
        action(cfg, Action::StopAll).await?;
        action(cfg, Action::StartCs1).await?;
        tokio::time::sleep(Duration::from_secs(5)).await;
        action(cfg, Action::StartCs2).await?;
        wait_ready(cfg, &up, None, Duration::from_secs(90)).await?;
        collect_stage(cfg, &mut history, &history_path, ReadbackStage::Restarted).await?;
        let traffic = if fault_traffic(&history, injection.start_ns, injection.end_ns) {
            CheckResult::pass_with_detail(
                "GuardFaultTraffic",
                "paired guarded ACK and complete three-key rejection observed wholly inside load/fault interval",
            )
        } else {
            CheckResult::inconclusive(
                "GuardFaultTraffic",
                "no paired guarded ACK plus complete three-key OCC rejection wholly inside the fault interval",
            )
        };
        let mut checks = vec![injection.check, traffic, rollback];
        for check in workload::check_guard_history(&history, &[cfg.leader_host.clone(), cfg.follower_host.clone()]) {
            checks.push(if check.passed {
                CheckResult::pass_with_detail(check.name, check.detail)
            } else {
                CheckResult::fail(check.name, check.detail)
            });
        }
        Ok::<_, String>((checks, bench_end, guard_shard))
    }
    .await;
    let cleanup = heal(cfg, &rules).await;
    let persist = workload::write_history(&history_path, &history);
    let (checks, bench_end, guard_shard) = match (body, cleanup, persist) {
        (Ok(result), Ok(()), Ok(())) => result,
        (body, cleanup, persist) => {
            let stopped = action(cfg, Action::StopAll).await;
            up.scraper.stop().await;
            return Err(format!(
                "{}: body={body:?}, heal={cleanup:?}, history={persist:?}, stop={stopped:?}",
                fault.name()
            ));
        }
    };
    let accepted = history
        .operations
        .iter()
        .filter(|o| matches!(o.outcome, GuardOutcome::Accepted { .. }))
        .count() as u64;
    let errors = history
        .operations
        .iter()
        .filter(|o| matches!(o.outcome, GuardOutcome::Rejected { .. } | GuardOutcome::Ambiguous { .. }))
        .count() as u64;
    let benchmark = BenchmarkResult {
        num_tasks: params.tasks,
        total_requests: accepted,
        errors,
        throughput: accepted as f64 / epoch.elapsed().as_secs_f64().max(0.001),
        avg_latency_ms: 0.,
        p50_ms: 0,
        p95_ms: 0,
        p99_ms: 0,
        p999_ms: 0,
        min_ms: 0,
        max_ms: 0,
        warmup_p50_ms: 0,
        errors_by_kind: Vec::new(),
        pool_stats: None,
    };
    let expectations = ScenarioExpectations {
        effective_leader_shard: Some(guard_shard),
        max_leader_elections: 20,
        max_s3_fallbacks: 10000,
        max_heartbeat_failures: 250,
        max_node_starts: 4,
        max_role_flips: 20,
        max_bench_errors: 10000,
        max_bench_error_ratio: Some(0.9),
        require_distinct_leader_hosts: if matches!(fault, GuardFault::LeaderKill | GuardFault::Rollback) {
            Some(2)
        } else {
            None
        },
        assert_eventual_progress: true,
        assert_read_converged_at_quiesce: true,
        ..Default::default()
    };
    tear_down_and_evaluate_with_audit(
        fault.name(),
        cfg,
        up,
        benchmark,
        bench_start,
        bench_end,
        expectations,
        params,
        checks,
        None,
        None,
        None,
        run_dir,
    )
    .await
}

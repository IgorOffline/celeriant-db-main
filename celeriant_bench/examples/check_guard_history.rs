use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use celeriant_bench::guard_fence::{check_guard_history, history_keys, read_history, GuardOutcome, GuardPhase, ReadbackStage};
use clap::Parser;

#[derive(Parser)]
struct Args {
    history: PathBuf,
    #[arg(long = "node", required = true)]
    nodes: Vec<String>,
    #[arg(long)]
    fault_evidence: Option<PathBuf>,
}

fn run(args: Args) -> Result<bool, String> {
    let history = read_history(&args.history)?;
    println!(
        "history={} version={} seed={} operations={} keys={} readbacks={}",
        args.history.display(),
        history.format_version,
        history.seed,
        history.operations.len(),
        history_keys(&history).len(),
        history.readbacks.len()
    );
    let mut counts = BTreeMap::new();
    for op in &history.operations {
        let phase = match op.phase {
            GuardPhase::Seed => "seed",
            GuardPhase::Load => "load",
            GuardPhase::Recovered => "recovered",
            GuardPhase::Advance => "advance",
            GuardPhase::Stale => "stale",
            GuardPhase::Current => "current",
        };
        let outcome = match &op.outcome {
            GuardOutcome::Accepted { .. } => "accepted",
            GuardOutcome::Conflict { .. } => "conflict",
            GuardOutcome::Rejected { .. } => "rejected",
            GuardOutcome::Ambiguous { .. } => "ambiguous",
        };
        *counts.entry((phase, outcome)).or_insert(0_usize) += 1;
    }
    for ((phase, outcome), count) in counts {
        println!("phase={phase} outcome={outcome} count={count}");
    }
    for node in &args.nodes {
        for stage in [ReadbackStage::Recovered, ReadbackStage::Restarted] {
            println!(
                "node={node} stage={stage:?} readbacks={}",
                history.readbacks.iter().filter(|r| r.node == *node && r.stage == stage).count()
            );
        }
    }
    if let Some(path) = args.fault_evidence {
        let evidence = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let timestamp = |name: &str| -> Result<u64, String> {
            evidence
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .ok_or_else(|| format!("missing {name}"))?
                .parse()
                .map_err(|e| format!("invalid {name}: {e}"))
        };
        let start = timestamp("installed_epoch_ns=")?;
        let end = timestamp("heal_epoch_ns=")?;
        if start >= end {
            return Err("empty fault window".into());
        }
        let inside = history.operations.iter().filter(|op| op.start_ns >= start && op.end_ns <= end);
        let mut accepted_pairs = 0;
        let mut complete_conflicts = 0;
        let mut ambiguous = 0;
        for op in inside {
            let appends = op.participants.iter().filter(|p| !p.events.is_empty()).count();
            let guards = op.participants.len() - appends;
            match &op.outcome {
                GuardOutcome::Accepted { .. } if appends == 2 && guards == 2 => accepted_pairs += 1,
                GuardOutcome::Conflict { conflicts } if conflicts.len() == 3 => complete_conflicts += 1,
                GuardOutcome::Ambiguous { .. } => ambiguous += 1,
                _ => (),
            }
        }
        println!("fault_window_ns={start}..{end} accepted_pairs={accepted_pairs} three_key_conflicts={complete_conflicts} ambiguous={ambiguous}");
        if accepted_pairs == 0 || complete_conflicts == 0 {
            return Err("missing traffic wholly within fault window".into());
        }
    }
    let checks = check_guard_history(&history, &args.nodes);
    for check in &checks {
        println!("{} {}: {}", if check.passed { "PASS" } else { "FAIL" }, check.name, check.detail);
    }
    Ok(!checks.is_empty() && checks.iter().all(|check| check.passed))
}

fn main() -> ExitCode {
    match run(Args::parse()) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("history audit failed: {error}");
            ExitCode::FAILURE
        }
    }
}

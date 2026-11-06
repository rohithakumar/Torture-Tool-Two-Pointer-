//! Phase 2 Execution Orchestrator.
//!
//! Integrates process supervision, concurrent writers, random crash killer,
//! and output drainer into a cohesive single-iteration run, recording full evidence.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::runner::drainer::{DrainerSummary, OutputDrainer};
use crate::runner::killer::{KillObservation, ProcessActivityTracker, RandomCrashKiller};
use crate::runner::supervisor::ChildSupervisor;
use crate::runner::writers::{ConcurrentWriterPool, SentOpRecord};

/// Comprehensive run evidence captured for crash-recovery verification.
#[derive(Debug, Clone)]
pub struct Phase2RunEvidence {
    pub seed: u64,
    pub iteration: usize,
    pub num_writers: u32,
    pub ops_sent: Vec<SentOpRecord>,
    pub complete_acks: Vec<u64>,
    pub complete_errs: Vec<(u64, String)>,
    pub kill_observation: KillObservation,
    pub drainer_summary: DrainerSummary,
    pub stderr_output: String,
}

/// Executes a single supervised crash run with concurrent writers and output draining.
pub fn execute_supervised_crash_iteration(
    executable: &Path,
    args: &[String],
    master_seed: u64,
    iteration: usize,
    num_writers: u32,
    min_delay: Duration,
    max_delay: Duration,
    payload_size: usize,
) -> std::io::Result<Phase2RunEvidence> {
    // 1. Spawn child process
    let (supervisor, stdout) = ChildSupervisor::spawn(executable, args)?;

    // 2. Start continuous output drainer
    let drainer = Arc::new(OutputDrainer::start(stdout));

    // 3. Activity tracker & Supervisor reference
    let activity = Arc::new(ProcessActivityTracker::new());
    let mut sup_arc = Arc::new(supervisor);

    // 4. Start concurrent writer pool
    let pool = ConcurrentWriterPool::start(
        num_writers,
        master_seed,
        payload_size,
        Arc::clone(&sup_arc),
        Arc::clone(&drainer),
        Arc::clone(&activity),
    );

    // 5. Initialize random crash killer
    let mut killer = RandomCrashKiller::new(master_seed, min_delay, max_delay);

    // 6. Wait scheduled duration and execute unconditional hard kill
    let kill_observation = killer.wait_and_kill(&sup_arc, &activity)?;

    // 7. Stop and join writer threads
    let ops_sent = pool.stop_and_join();

    // 8. Drain remaining buffered stdout until EOF and collect drainer summary
    let drainer_summary = drainer.join_and_summarize();

    // 9. Reaping stderr output
    let stderr_output = if let Some(sup) = Arc::get_mut(&mut sup_arc) {
        sup.collect_stderr()
    } else {
        String::new()
    };

    Ok(Phase2RunEvidence {
        seed: master_seed,
        iteration,
        num_writers,
        ops_sent,
        complete_acks: drainer_summary.complete_acks.clone(),
        complete_errs: drainer_summary.complete_errs.clone(),
        kill_observation,
        drainer_summary,
        stderr_output,
    })
}

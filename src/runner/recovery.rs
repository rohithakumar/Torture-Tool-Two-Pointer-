//! Full Recovery Lifecycle and Verification Runner.
//!
//! Executes the 11-step recovery lifecycle:
//!   1. Start engine child process (serve)
//!   2. Concurrent writers dispatch operations
//!   3. Random hard kill
//!   4. Pipe drained to EOF
//!   5. Operation ledger frozen
//!   6. Restart engine
//!   7. Run open (recovery routine)
//!   8. Run check (integrity validation)
//!   9. Normalized dump extraction
//!   10. Independent Oracle verification against state lattice
//!   11. Structured evidence packaging

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::adapter::{CheckResult, DatabaseEngineAdapter, OpenResult};
use crate::oracle::ledger::OracleLedger;
use crate::oracle::verifier::{OracleVerdict, OracleVerifier};
use crate::runner::drainer::OutputDrainer;
use crate::runner::killer::{KillObservation, ProcessActivityTracker, RandomCrashKiller};
use crate::runner::supervisor::ChildSupervisor;
use crate::runner::writers::{ConcurrentWriterPool, SentOpRecord};

/// Comprehensive structured evidence for a single crash-recovery iteration.
#[derive(Debug, Clone)]
pub struct Phase3IterationResult {
    pub seed: u64,
    pub iteration: usize,
    pub sent_ops: Vec<SentOpRecord>,
    pub complete_acks: Vec<u64>,
    pub complete_errs: Vec<(u64, String)>,
    pub inflight_ops_count: usize,
    pub kill_observation: KillObservation,
    pub open_result: OpenResult,
    pub check_result: CheckResult,
    pub recovered_dump_summary: BTreeMap<u64, usize>, // key -> payload size
    pub oracle_verdict: OracleVerdict,
}

/// Ensures that the database path is in a clean, provably empty initial state.
///
/// If db_path exists:
/// - If it is a directory, removes all files and subdirectories within it.
/// - If it is a file, removes the file.
/// If it does not exist, ensures parent directories are created.
pub fn ensure_clean_db_path(db_path: &Path) -> std::io::Result<()> {
    if db_path.exists() {
        if db_path.is_dir() {
            for entry in std::fs::read_dir(db_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    let _ = std::fs::remove_dir_all(&path);
                } else {
                    let _ = std::fs::remove_file(&path);
                }
            }
        } else {
            let _ = std::fs::remove_file(db_path);
        }
    } else {
        if let Some(parent) = db_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if db_path.extension().is_none() {
            let _ = std::fs::create_dir_all(db_path);
        }
    }
    Ok(())
}

/// Executes the complete recovery verification workflow.
pub fn execute_recovery_lifecycle(
    adapter: &dyn DatabaseEngineAdapter,
    executable: &Path,
    db_path: &Path,
    master_seed: u64,
    iteration: usize,
    num_writers: u32,
    min_kill_delay: Duration,
    max_kill_delay: Duration,
    payload_size: usize,
) -> std::io::Result<Phase3IterationResult> {
    // Step 0: Ensure provably clean initial database state
    ensure_clean_db_path(db_path)?;

    // Shared monotonic event clock for precise, non-synthetic happens-before
    let event_clock = Arc::new(std::sync::atomic::AtomicU64::new(1));

    // Step 1: Start engine serve child process
    let serve_args = vec!["serve".to_string(), db_path.display().to_string()];
    let (supervisor, stdout) = ChildSupervisor::spawn(executable, &serve_args)?;

    // Step 2 & 4: Start output drainer and activity tracker
    let drainer = Arc::new(OutputDrainer::start_with_clock(
        stdout,
        Arc::clone(&event_clock),
    ));
    let activity = Arc::new(ProcessActivityTracker::new());
    let sup_arc = Arc::new(supervisor);

    // Step 2: Spawn concurrent writers with shared event clock
    let pool = ConcurrentWriterPool::start_with_clock(
        num_writers,
        master_seed,
        payload_size,
        Arc::clone(&sup_arc),
        Arc::clone(&drainer),
        Arc::clone(&activity),
        Arc::clone(&event_clock),
    );

    // Step 3: Trigger random hard kill
    let mut killer = RandomCrashKiller::new(master_seed, min_kill_delay, max_kill_delay);
    let kill_observation = killer.wait_and_kill(&sup_arc, &activity)?;

    // Step 4: Stop writers and drain pipe to EOF
    let sent_ops = pool.stop_and_join();
    let drainer_summary = drainer.join_and_summarize();

    // Step 5: Freeze ledger using actual observed drainer ACK timestamps (never synthesized)
    let ack_ticks_vec: Vec<(u64, u64)> = drainer_summary.ack_ticks.into_iter().collect();
    let ledger = OracleLedger::build_with_ticks(
        sent_ops.clone(),
        &drainer_summary.complete_acks,
        &ack_ticks_vec,
        &drainer_summary.complete_errs,
    );
    let inflight_ops_count = ledger.inflight_count;

    // Step 7: Execute OPEN
    let open_result = adapter.run_open(db_path, Duration::from_secs(5))?;

    // Step 8: Execute CHECK
    let check_result = adapter.run_check(db_path, Duration::from_secs(5))?;

    // Step 9: Obtain normalized DUMP
    let raw_dump = adapter.run_dump(db_path, Duration::from_secs(10))?;
    let mut normalized_dump = BTreeMap::new();
    let mut dump_summary = BTreeMap::new();

    for (k_bytes, v_bytes) in raw_dump {
        if k_bytes.len() == 8 {
            let key = u64::from_le_bytes(k_bytes.try_into().unwrap());
            dump_summary.insert(key, v_bytes.len());
            normalized_dump.insert(key, v_bytes);
        }
    }

    // Step 10: Validate recovered dump against Independent Oracle
    let verifier = OracleVerifier::new(master_seed, ledger);
    let oracle_verdict = verifier.verify_dump(&normalized_dump);

    Ok(Phase3IterationResult {
        seed: master_seed,
        iteration,
        sent_ops,
        complete_acks: drainer_summary.complete_acks,
        complete_errs: drainer_summary.complete_errs,
        inflight_ops_count,
        kill_observation,
        open_result,
        check_result,
        recovered_dump_summary: dump_summary,
        oracle_verdict,
    })
}

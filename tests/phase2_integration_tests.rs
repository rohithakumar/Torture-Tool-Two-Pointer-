//! Integration Tests for Phase 2: Concurrent Writers, Random Killer & Process Drainer.
//!
//! Uses the internal mock child process binary exclusively for harness self-testing.

use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use torture::runner::drainer::OutputDrainer;
use torture::runner::killer::{ProcessActivityTracker, RandomCrashKiller};
use torture::runner::orchestrator::execute_supervised_crash_iteration;
use torture::runner::supervisor::{ChildSupervisor, HardTerminationKind};
use torture::runner::writers::ConcurrentWriterPool;

/// Locate the compiled mock_child binary.
fn mock_child_bin() -> PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_mock_child") {
        return PathBuf::from(path);
    }
    let mut path = std::env::current_exe().expect("current exe");
    path.pop(); // test-binary
    if path.ends_with("deps") {
        path.pop();
    }
    let mut exe = path.join("mock_child");
    #[cfg(windows)]
    exe.set_extension("exe");
    exe
}

#[test]
fn test_supervisor_spawn_and_hard_kill() {
    let bin = mock_child_bin();
    let (supervisor, stdout) =
        ChildSupervisor::spawn(&bin, &[]).expect("Failed to spawn mock child");

    assert!(supervisor.is_alive(), "Process should be alive after spawn");

    // Send a command to verify stdin/stdout
    let drainer = Arc::new(OutputDrainer::start(stdout));
    supervisor.send_command("10 put 1 0a0b").unwrap();

    let resp = drainer.wait_for_op_response(10, Duration::from_secs(1));
    assert!(resp.is_some(), "Should receive ACK 10 before kill");

    // Execute unconditional hard kill
    let kill_report = supervisor.hard_kill().expect("Hard kill should succeed");

    assert_eq!(kill_report.pid, supervisor.pid());
    #[cfg(windows)]
    assert_eq!(
        kill_report.mechanism,
        HardTerminationKind::WindowsTerminateProcess
    );
    #[cfg(not(windows))]
    assert_eq!(kill_report.mechanism, HardTerminationKind::UnixSigkill);

    assert!(
        !supervisor.is_alive(),
        "Process must not be alive after hard kill"
    );

    let summary = drainer.join_and_summarize();
    assert!(summary.complete_acks.contains(&10));
}

#[test]
fn test_drainer_ignores_partial_final_ack() {
    // Critical Invariant: A trailing partial line emitted before death must NOT be counted as an ACK
    let bin = mock_child_bin();
    let (supervisor, stdout) =
        ChildSupervisor::spawn(&bin, &[]).expect("Failed to spawn mock child");
    let drainer = Arc::new(OutputDrainer::start(stdout));

    // Command mock child to write "ACK 999" WITHOUT newline, flush, and sleep
    supervisor.send_command("TEST_PARTIAL 999").unwrap();

    // Give child 50ms to print the un-terminated bytes
    thread::sleep(Duration::from_millis(50));

    // Hard kill the child mid-stream
    let _ = supervisor.hard_kill().unwrap();

    // Drainer must finish at EOF
    let summary = drainer.join_and_summarize();

    // Invariant Check 1: Must NOT be in complete_acks
    assert!(
        !summary.complete_acks.contains(&999),
        "Partial line 'ACK 999' without trailing newline MUST NOT be counted as a complete ACK!"
    );

    // Invariant Check 2: Must be captured in incomplete_trailing
    assert_eq!(
        summary.incomplete_trailing,
        Some("ACK 999".to_string()),
        "Incomplete trailing data must be captured exactly"
    );
}

#[test]
fn test_concurrent_writers_and_independent_sequences() {
    let bin = mock_child_bin();
    let (supervisor, stdout) =
        ChildSupervisor::spawn(&bin, &[]).expect("Failed to spawn mock child");

    let drainer = Arc::new(OutputDrainer::start(stdout));
    let activity = Arc::new(ProcessActivityTracker::new());
    let sup_arc = Arc::new(supervisor);

    let pool = ConcurrentWriterPool::start(
        2,
        0x1234_5678,
        64,
        Arc::clone(&sup_arc),
        Arc::clone(&drainer),
        Arc::clone(&activity),
    );

    // Let writers run for 100ms
    thread::sleep(Duration::from_millis(100));

    // Stop writers cleanly
    let sent_ops = pool.stop_and_join();
    let _ = sup_arc.hard_kill().unwrap();

    let writer0_ops: Vec<_> = sent_ops.iter().filter(|o| o.writer_id == 0).collect();
    let writer1_ops: Vec<_> = sent_ops.iter().filter(|o| o.writer_id == 1).collect();

    assert!(writer0_ops.len() >= 5, "Writer 0 should have executed ops");
    assert!(writer1_ops.len() >= 5, "Writer 1 should have executed ops");

    // Monotonically increasing sequence check
    for (i, op) in writer0_ops.iter().enumerate() {
        assert_eq!(op.seq_num, (i + 1) as u64);
    }
    for (i, op) in writer1_ops.iter().enumerate() {
        assert_eq!(op.seq_num, (i + 1) as u64);
    }

    let summary = drainer.join_and_summarize();
    assert!(!summary.complete_acks.is_empty());
}

#[test]
fn test_kill_during_active_writes() {
    let bin = mock_child_bin();
    // Configure child with artificial 10ms delay per op so writes are actively in-flight
    let evidence = execute_supervised_crash_iteration(
        &bin,
        &["--delay-ms".to_string(), "10".to_string()],
        98765,
        1,
        2,
        Duration::from_millis(25),
        Duration::from_millis(40),
        64,
    )
    .expect("Crash iteration should execute");

    assert!(
        evidence.kill_observation.active_writes_at_kill > 0
            || evidence.kill_observation.active_batches_at_kill > 0,
        "Kill must land during active write/batch, observed: {:?}",
        evidence.kill_observation.activity_state
    );
    assert!(!evidence.ops_sent.is_empty(), "Ops should have been sent");
}

#[test]
fn test_complete_ack_and_err_accounting() {
    let bin = mock_child_bin();
    let (supervisor, stdout) =
        ChildSupervisor::spawn(&bin, &[]).expect("Failed to spawn mock child");
    let drainer = Arc::new(OutputDrainer::start(stdout));

    // Send valid write
    supervisor.send_command("100 put 1 0102").unwrap();
    // Send simulated error
    supervisor
        .send_command("TEST_ERR 101 DISK_FULL_SIMULATED")
        .unwrap();
    // Send another valid write
    supervisor.send_command("102 del 1").unwrap();

    // Wait for all three responses
    assert!(
        drainer
            .wait_for_op_response(100, Duration::from_secs(1))
            .is_some()
    );
    assert!(
        drainer
            .wait_for_op_response(101, Duration::from_secs(1))
            .is_some()
    );
    assert!(
        drainer
            .wait_for_op_response(102, Duration::from_secs(1))
            .is_some()
    );

    let _ = supervisor.hard_kill().unwrap();
    let summary = drainer.join_and_summarize();

    assert!(summary.complete_acks.contains(&100));
    assert!(summary.complete_acks.contains(&102));
    assert_eq!(
        summary.complete_errs,
        vec![(101, "DISK_FULL_SIMULATED".to_string())]
    );
    assert_eq!(summary.malformed_lines.len(), 0);
}

#[test]
fn test_seed_reproducibility() {
    use torture::generator::prng::PrngStream;

    let mut stream1 = PrngStream::new(54321, 0);
    let mut stream2 = PrngStream::new(54321, 0);

    let seq1: Vec<u64> = (0..500).map(|_| stream1.next_u64()).collect();
    let seq2: Vec<u64> = (0..500).map(|_| stream2.next_u64()).collect();

    assert_eq!(
        seq1, seq2,
        "Workload sequences from same seed must be bit-identical"
    );

    let mut killer1 =
        RandomCrashKiller::new(54321, Duration::from_millis(5), Duration::from_millis(50));
    let mut killer2 =
        RandomCrashKiller::new(54321, Duration::from_millis(5), Duration::from_millis(50));

    let delays1: Vec<Duration> = (0..20).map(|_| killer1.next_delay()).collect();
    let delays2: Vec<Duration> = (0..20).map(|_| killer2.next_delay()).collect();

    assert_eq!(
        delays1, delays2,
        "Crash schedule from same seed must be identical"
    );
}

#[test]
fn test_kill_during_batch() {
    let bin = mock_child_bin();
    // Configure child with artificial 15ms delay per op so batch processing is in-flight
    let evidence = execute_supervised_crash_iteration(
        &bin,
        &["--delay-ms".to_string(), "15".to_string()],
        112233,
        1,
        2,
        Duration::from_millis(10),
        Duration::from_millis(30),
        64,
    )
    .expect("Crash iteration should execute");

    assert!(!evidence.ops_sent.is_empty(), "Ops should have been sent");
    // Ensure that the supervisor recorded the actual termination mechanism
    #[cfg(windows)]
    assert_eq!(
        evidence.kill_observation.kill_report.mechanism,
        HardTerminationKind::WindowsTerminateProcess
    );
    #[cfg(not(windows))]
    assert_eq!(
        evidence.kill_observation.kill_report.mechanism,
        HardTerminationKind::UnixSigkill
    );
}

#[test]
fn test_kill_while_output_is_actively_streaming() {
    let bin = mock_child_bin();
    let (supervisor, stdout) =
        ChildSupervisor::spawn(&bin, &[]).expect("Failed to spawn mock child");
    let drainer = Arc::new(OutputDrainer::start(stdout));

    // Verify child is actively running by waiting for initial ACK
    supervisor.send_command("1 put 1 0a0b").unwrap();
    let resp = drainer.wait_for_op_response(1, Duration::from_secs(1));
    assert!(resp.is_some(), "Child should acknowledge initial command");

    let sup_arc = Arc::new(supervisor);
    let sup_for_pumper = Arc::clone(&sup_arc);

    // Pump commands rapidly in a background thread while drainer reads
    let pumper = thread::spawn(move || {
        for i in 2..=5000 {
            if sup_for_pumper
                .send_command(&format!("{} put 1 0a0b", i))
                .is_err()
            {
                break;
            }
        }
    });

    // Allow streaming to become active
    thread::sleep(Duration::from_millis(20));

    // Kill the process while output is actively streaming into the pipe
    let kill_report = sup_arc.hard_kill().expect("Kill should succeed mid-stream");
    assert!(!sup_arc.is_alive());
    let _ = pumper.join();

    // Drainer must continue reading until EOF without deadlock or crash
    let summary = drainer.join_and_summarize();

    assert!(
        summary.total_bytes_read > 0,
        "Bytes must have been read while streaming: {}",
        summary.total_bytes_read
    );
    assert!(
        !summary.complete_acks.is_empty(),
        "Should have parsed complete ACKs before kill landed: {}",
        summary.complete_acks.len()
    );
    assert!(kill_report.exit_status.is_some());
}

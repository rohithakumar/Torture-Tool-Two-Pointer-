//! Deterministic Random Crash Killer.
//!
//! Uses a seeded PRNG stream to schedule crash delays, executes hard termination
//! on the supervised child process, and measures actual vs. scheduled timing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::generator::prng::PrngStream;
use crate::runner::supervisor::{ChildSupervisor, HardKillReport};

/// Observable process activity status when the kill lands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineActivityState {
    /// Kill landed while at least one batch was actively processing
    ActiveBatch,
    /// Kill landed while at least one standard put/del write was in-flight
    ActiveWrite,
    /// Kill landed while zero writer threads had an outstanding request
    Idle,
}

/// Tracks concurrent activity across writers to classify kill timing.
#[derive(Default)]
pub struct ProcessActivityTracker {
    pub active_writes: AtomicUsize,
    pub active_batches: AtomicUsize,
}

impl ProcessActivityTracker {
    pub fn new() -> Self {
        Self {
            active_writes: AtomicUsize::new(0),
            active_batches: AtomicUsize::new(0),
        }
    }

    pub fn enter_write(&self) {
        self.active_writes.fetch_add(1, Ordering::SeqCst);
    }

    pub fn exit_write(&self) {
        self.active_writes.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn enter_batch(&self) {
        self.active_batches.fetch_add(1, Ordering::SeqCst);
    }

    pub fn exit_batch(&self) {
        self.active_batches.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn current_state(&self) -> (EngineActivityState, usize, usize) {
        let batches = self.active_batches.load(Ordering::SeqCst);
        let writes = self.active_writes.load(Ordering::SeqCst);
        let state = if batches > 0 {
            EngineActivityState::ActiveBatch
        } else if writes > 0 {
            EngineActivityState::ActiveWrite
        } else {
            EngineActivityState::Idle
        };
        (state, writes, batches)
    }
}

/// Measured evidence captured during the kill execution.
#[derive(Debug, Clone)]
pub struct KillObservation {
    pub scheduled_delay: Duration,
    pub actual_delay: Duration,
    pub activity_state: EngineActivityState,
    pub active_writes_at_kill: usize,
    pub active_batches_at_kill: usize,
    pub kill_report: HardKillReport,
}

/// Deterministic random crash killer.
pub struct RandomCrashKiller {
    stream: PrngStream,
    min_delay: Duration,
    max_delay: Duration,
}

impl RandomCrashKiller {
    /// Stream ID 9999 is reserved for the crash scheduler stream.
    pub const KILLER_STREAM_ID: u32 = 9999;

    pub fn new(master_seed: u64, min_delay: Duration, max_delay: Duration) -> Self {
        assert!(min_delay <= max_delay, "min_delay must be <= max_delay");
        Self {
            stream: PrngStream::new(master_seed, Self::KILLER_STREAM_ID),
            min_delay,
            max_delay,
        }
    }

    /// Calculate next deterministic kill delay.
    pub fn next_delay(&mut self) -> Duration {
        let min_micros = self.min_delay.as_micros() as u64;
        let max_micros = self.max_delay.as_micros() as u64;
        if min_micros == max_micros {
            return self.min_delay;
        }
        let chosen_micros = self.stream.next_range_u64(min_micros, max_micros + 1);
        Duration::from_micros(chosen_micros)
    }

    /// Wait for the calculated delay, record active process state, and execute hard kill.
    ///
    /// NOTE on OS Timer Limits:
    /// Operating system thread scheduling on Windows typically has a timer resolution
    /// between 1ms and 15ms. Sub-millisecond sleep requests may round up to the next
    /// OS scheduler quantum. We measure and record both the requested target and actual elapsed time.
    pub fn wait_and_kill(
        &mut self,
        supervisor: &ChildSupervisor,
        activity: &ProcessActivityTracker,
    ) -> std::io::Result<KillObservation> {
        let scheduled_delay = self.next_delay();
        let start = Instant::now();

        // High-precision sleep / spin hybrid where platform permits
        if scheduled_delay > Duration::from_millis(5) {
            thread::sleep(scheduled_delay - Duration::from_millis(2));
        }
        while start.elapsed() < scheduled_delay {
            std::hint::spin_loop();
        }
        let actual_delay = start.elapsed();

        // Sample observable engine activity immediately before hard termination
        let (activity_state, active_writes, active_batches) = activity.current_state();

        // Execute unconditional hard kill
        let kill_report = supervisor.hard_kill()?;

        Ok(KillObservation {
            scheduled_delay,
            actual_delay,
            activity_state,
            active_writes_at_kill: active_writes,
            active_batches_at_kill: active_batches,
            kill_report,
        })
    }
}

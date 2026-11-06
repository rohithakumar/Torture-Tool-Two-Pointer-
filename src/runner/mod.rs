//! Runner Subsystem: Supervision, Concurrent Writers, Killer & Drainer.

pub mod drainer;
pub mod killer;
pub mod orchestrator;
pub mod recovery;
pub mod supervisor;
pub mod writers;

pub use drainer::{DrainerSummary, OutputDrainer, ResponseStatus};
pub use killer::{EngineActivityState, KillObservation, ProcessActivityTracker, RandomCrashKiller};
pub use orchestrator::{Phase2RunEvidence, execute_supervised_crash_iteration};
pub use recovery::{Phase3IterationResult, ensure_clean_db_path, execute_recovery_lifecycle};
pub use supervisor::{ChildSupervisor, HardKillReport, HardTerminationKind};
pub use writers::{BatchSubOp, ConcurrentWriterPool, OpType, SentOpRecord, WriterThread};

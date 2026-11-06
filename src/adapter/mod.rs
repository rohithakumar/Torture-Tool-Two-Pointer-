use std::io;
use std::path::Path;
use std::time::Duration;

pub mod test_adapter;
pub use test_adapter::MockChildAdapter;

/// Placeholder response type received from the engine's serve interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineResponsePlaceholder {
    Ack { op_id: u64 },
    Err { op_id: u64, reason: String },
    Val { op_id: u64, value: Vec<u8> },
    NotFound { op_id: u64 },
    Other(String),
}

/// Abstract session representing an active `serve` child process.
pub trait EngineProcess: Send {
    /// Return the OS child process ID (PID)
    fn pid(&self) -> u32;

    /// Sends raw operation line / command to the engine's serve standard input.
    fn send_raw_op(&mut self, op: &str) -> io::Result<()>;

    /// Reads or polls for the next response line from standard output.
    fn poll_next_response(&mut self, timeout: Duration) -> io::Result<Option<String>>;

    /// Immediately terminates the process using hard kill (SIGKILL on POSIX, TerminateProcess on Windows).
    fn hard_kill(&mut self) -> io::Result<()>;

    /// Drains any remaining complete lines from the pipe that the child emitted before death.
    fn drain_stdout(&mut self) -> Vec<String>;
}

/// Result of running the engine's `open` command after a crash.
#[derive(Debug, Clone)]
pub struct OpenResult {
    pub exit_status_ok: bool,
    pub recovery_duration: Duration,
    pub recovered_keys_count: Option<usize>,
    pub raw_output: String,
}

/// Result of running the engine's `check` command.
#[derive(Debug, Clone)]
pub struct CheckResult {
    pub is_ok: bool,
    pub raw_output: String,
}

/// High-level adapter trait decoupling the torture runner from the concrete CLI commands.
pub trait DatabaseEngineAdapter: Send + Sync {
    /// Starts the engine child process in `serve` mode on the target DB path.
    fn start_serve(&self, db_path: &Path) -> io::Result<Box<dyn EngineProcess>>;

    /// Invokes the `open` recovery routine on the target DB path.
    fn run_open(&self, db_path: &Path, timeout: Duration) -> io::Result<OpenResult>;

    /// Invokes the `check` integrity validation on the target DB path.
    fn run_check(&self, db_path: &Path, timeout: Duration) -> io::Result<CheckResult>;

    /// Invokes the `dump` command to inspect key-value state for oracle comparison.
    fn run_dump(&self, db_path: &Path, timeout: Duration) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Optional compaction trigger (in-band or CLI).
    fn run_compaction(&self, db_path: &Path, timeout: Duration) -> io::Result<()>;
}

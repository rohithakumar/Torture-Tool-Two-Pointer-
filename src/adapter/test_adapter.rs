//! Test-Only Engine Adapter for `mock_child`.
//!
//! Exclusively implements the `DatabaseEngineAdapter` trait to test
//! the recovery lifecycle against the internal mock process across restarts.

use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crate::adapter::{CheckResult, DatabaseEngineAdapter, EngineProcess, OpenResult};
use crate::generator::payload::from_hex;
use crate::runner::drainer::OutputDrainer;
use crate::runner::supervisor::ChildSupervisor;

/// Active process session for `mock_child serve`.
pub struct MockServeSession {
    supervisor: ChildSupervisor,
    drainer: OutputDrainer,
}

impl EngineProcess for MockServeSession {
    fn pid(&self) -> u32 {
        self.supervisor.pid()
    }

    fn send_raw_op(&mut self, op: &str) -> io::Result<()> {
        self.supervisor.send_command(op)
    }

    fn poll_next_response(&mut self, _timeout: Duration) -> io::Result<Option<String>> {
        std::thread::sleep(Duration::from_millis(5));
        Ok(None)
    }

    fn hard_kill(&mut self) -> io::Result<()> {
        self.supervisor.hard_kill().map(|_| ())
    }

    fn drain_stdout(&mut self) -> Vec<String> {
        let summary = self.drainer.join_and_summarize();
        summary
            .complete_acks
            .into_iter()
            .map(|id| format!("ACK {}", id))
            .collect()
    }
}

/// Test-only adapter driving the internal `mock_child` binary.
#[derive(Debug, Clone)]
pub struct MockChildAdapter {
    bin_path: PathBuf,
    injected_fault: Option<String>,
}

impl MockChildAdapter {
    pub fn new(bin_path: PathBuf) -> Self {
        Self {
            bin_path,
            injected_fault: None,
        }
    }

    pub fn with_fault(mut self, fault: &str) -> Self {
        self.injected_fault = Some(fault.to_string());
        self
    }
}

impl DatabaseEngineAdapter for MockChildAdapter {
    fn start_serve(&self, db_path: &Path) -> io::Result<Box<dyn EngineProcess>> {
        let mut args = vec!["serve".to_string(), db_path.display().to_string()];
        if let Some(ref f) = self.injected_fault {
            args.push("--fault".to_string());
            args.push(f.clone());
        }

        let (supervisor, stdout) = ChildSupervisor::spawn(&self.bin_path, &args)?;
        let drainer = OutputDrainer::start(stdout);

        Ok(Box::new(MockServeSession {
            supervisor,
            drainer,
        }))
    }

    fn run_open(&self, db_path: &Path, _timeout: Duration) -> io::Result<OpenResult> {
        let start = Instant::now();
        let output = Command::new(&self.bin_path)
            .arg("open")
            .arg(db_path)
            .output()?;

        let raw = String::from_utf8_lossy(&output.stdout).to_string();
        let is_ok = output.status.success() && raw.contains("OPEN OK");

        Ok(OpenResult {
            exit_status_ok: is_ok,
            recovery_duration: start.elapsed(),
            recovered_keys_count: None,
            raw_output: raw,
        })
    }

    fn run_check(&self, db_path: &Path, _timeout: Duration) -> io::Result<CheckResult> {
        let output = Command::new(&self.bin_path)
            .arg("check")
            .arg(db_path)
            .output()?;

        let raw = String::from_utf8_lossy(&output.stdout).to_string();
        let is_ok = output.status.success() && raw.trim() == "OK";

        Ok(CheckResult {
            is_ok,
            raw_output: raw,
        })
    }

    fn run_dump(&self, db_path: &Path, _timeout: Duration) -> io::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut cmd = Command::new(&self.bin_path);
        cmd.arg("dump").arg(db_path);

        if let Some(ref f) = self.injected_fault {
            cmd.arg("--fault").arg(f);
        }

        let output = cmd.output()?;
        let raw = String::from_utf8_lossy(&output.stdout);
        let mut kv_pairs = Vec::new();

        for line in raw.lines() {
            let parts: Vec<&str> = line.trim().split_whitespace().collect();
            if parts.len() >= 2 {
                if let Ok(key_u64) = parts[0].parse::<u64>() {
                    if let Ok(val_bytes) = from_hex(parts[1]) {
                        kv_pairs.push((key_u64.to_le_bytes().to_vec(), val_bytes));
                    }
                }
            }
        }

        Ok(kv_pairs)
    }

    fn run_compaction(&self, _db_path: &Path, _timeout: Duration) -> io::Result<()> {
        Ok(())
    }
}

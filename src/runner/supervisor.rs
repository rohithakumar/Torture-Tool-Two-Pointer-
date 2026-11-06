//! Process Supervision and Hard Termination.
//!
//! Manages child process lifecycles, non-blocking standard I/O pipes,
//! process state tracking, and genuine hard termination (TerminateProcess on Windows,
//! SIGKILL on POSIX).

use std::io::{self, BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Specific OS hard-kill mechanism executed to terminate the child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardTerminationKind {
    /// Win32 TerminateProcess API
    WindowsTerminateProcess,
    /// POSIX SIGKILL (signal 9)
    UnixSigkill,
}

impl HardTerminationKind {
    /// Return the OS-appropriate hard-kill mechanism.
    pub fn current_platform() -> Self {
        #[cfg(windows)]
        {
            HardTerminationKind::WindowsTerminateProcess
        }
        #[cfg(not(windows))]
        {
            HardTerminationKind::UnixSigkill
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WindowsTerminateProcess => {
                "Windows TerminateProcess (Win32 unconditional hard kill)"
            }
            Self::UnixSigkill => "POSIX SIGKILL (Signal 9 unconditional process termination)",
        }
    }
}

/// Detailed report on child process termination.
#[derive(Debug, Clone)]
pub struct HardKillReport {
    pub pid: u32,
    pub mechanism: HardTerminationKind,
    pub kill_duration: Duration,
    pub exit_status: Option<String>,
}

/// Supervisor for a database or mock child process.
pub struct ChildSupervisor {
    pid: u32,
    child: Arc<Mutex<Option<Child>>>,
    stdin_writer: Arc<Mutex<Option<ChildStdin>>>,
    stderr_drain_handle: Option<JoinHandle<String>>,
}

impl ChildSupervisor {
    /// Spawns the target process with piped stdin, stdout, and stderr.
    ///
    /// Returns the supervisor along with the raw `ChildStdout` stream for continuous draining.
    pub fn spawn(
        executable: &std::path::Path,
        args: &[String],
    ) -> io::Result<(Self, std::process::ChildStdout)> {
        let mut cmd = Command::new(executable);
        cmd.args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn()?;
        let pid = child.id();

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to capture child stdin"))?;

        let stdout = child.stdout.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Failed to capture child stdout")
        })?;

        let stderr = child.stderr.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Failed to capture child stderr")
        })?;

        // Drain stderr asynchronously in background thread to avoid pipe buffer deadlocks
        let stderr_handle = thread::Builder::new()
            .name(format!("stderr-drainer-{}", pid))
            .spawn(move || {
                let mut reader = BufReader::new(stderr);
                let mut accumulated = String::new();
                let mut line = String::new();
                while let Ok(n) = reader.read_line(&mut line) {
                    if n == 0 {
                        break;
                    }
                    accumulated.push_str(&line);
                    line.clear();
                }
                accumulated
            })?;

        Ok((
            Self {
                pid,
                child: Arc::new(Mutex::new(Some(child))),
                stdin_writer: Arc::new(Mutex::new(Some(stdin))),
                stderr_drain_handle: Some(stderr_handle),
            },
            stdout,
        ))
    }

    /// Return the OS process ID.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Thread-safe send of a raw command string to the child's stdin.
    pub fn send_command(&self, cmd: &str) -> io::Result<()> {
        let mut guard = self.stdin_writer.lock().unwrap();
        if let Some(stdin) = guard.as_mut() {
            stdin.write_all(cmd.as_bytes())?;
            if !cmd.ends_with('\n') {
                stdin.write_all(b"\n")?;
            }
            stdin.flush()?;
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Child stdin already closed or terminated",
            ))
        }
    }

    /// Thread-safe send of a command string to the child's stdin with conservative interval timestamps.
    ///
    /// Records `t_sched` immediately after acquiring the mutex (strictly before writing to stdin),
    /// and `t_sub` immediately after `flush()` completes (for diagnostic telemetry only).
    pub fn send_command_interval(&self, cmd: &str, clock: &AtomicU64) -> io::Result<(u64, u64)> {
        let mut guard = self.stdin_writer.lock().unwrap();
        if let Some(stdin) = guard.as_mut() {
            let t_sched = clock.fetch_add(1, Ordering::SeqCst);
            stdin.write_all(cmd.as_bytes())?;
            if !cmd.ends_with('\n') {
                stdin.write_all(b"\n")?;
            }
            stdin.flush()?;
            let t_sub = clock.fetch_add(1, Ordering::SeqCst);
            Ok((t_sched, t_sub))
        } else {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Child stdin already closed or terminated",
            ))
        }
    }

    /// Close standard input to signal EOF to the child process.
    pub fn close_stdin(&self) {
        let mut guard = self.stdin_writer.lock().unwrap();
        *guard = None;
    }

    /// Check if the child process is still actively running.
    pub fn is_alive(&self) -> bool {
        let mut guard = self.child.lock().unwrap();
        if let Some(child) = guard.as_mut() {
            match child.try_wait() {
                Ok(None) => true,
                Ok(Some(_)) | Err(_) => false,
            }
        } else {
            false
        }
    }

    /// Execute genuine hard process termination.
    ///
    /// On Windows: calls Win32 TerminateProcess (never graceful).
    /// On POSIX: sends SIGKILL signal 9 (never graceful).
    pub fn hard_kill(&self) -> io::Result<HardKillReport> {
        let start = Instant::now();
        let mechanism = HardTerminationKind::current_platform();

        // Drop stdin handle first to sever input pipe
        {
            let mut stdin_guard = self.stdin_writer.lock().unwrap();
            *stdin_guard = None;
        }

        let mut child_guard = self.child.lock().unwrap();
        if let Some(mut child) = child_guard.take() {
            // child.kill() performs TerminateProcess on Windows, and SIGKILL on Unix.
            let kill_res = child.kill();
            let exit_status = child.wait().ok().map(|s| s.to_string());
            let kill_duration = start.elapsed();

            kill_res.map(|_| HardKillReport {
                pid: self.pid,
                mechanism,
                kill_duration,
                exit_status,
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Process was already terminated",
            ))
        }
    }

    /// Reaps the background stderr thread and returns captured stderr output.
    pub fn collect_stderr(&mut self) -> String {
        if let Some(handle) = self.stderr_drain_handle.take() {
            handle
                .join()
                .unwrap_or_else(|_| "<stderr thread panicked>".to_string())
        } else {
            String::new()
        }
    }
}

impl Drop for ChildSupervisor {
    fn drop(&mut self) {
        // Ensure child process is never orphaned on abnormal return or panic
        let mut guard = self.child.lock().unwrap();
        if let Some(mut child) = guard.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

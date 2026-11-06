//! Dedicated Output Drainer and Protocol Line Accounting.
//!
//! Drains stdout continuously in the background to prevent pipe buffer deadlocks.
//! Guarantees that only complete protocol lines with trailing newlines are counted as ACKs.
//! Any incomplete trailing bytes left when the child process dies are recorded separately
//! and strictly excluded from acknowledged writes.

use std::collections::HashMap;
use std::io::{self, Read};
use std::process::ChildStdout;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Status of an operation response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseStatus {
    Ack,
    Err(String),
}

/// Shared state between the background drainer thread and concurrent writers.
struct DrainerSharedState {
    complete_acks: Vec<u64>,
    complete_errs: Vec<(u64, String)>,
    ack_ticks: HashMap<u64, u64>,
    err_ticks: HashMap<u64, u64>,
    malformed_lines: Vec<String>,
    other_lines: Vec<String>,
    responses: HashMap<u64, ResponseStatus>,
    is_eof: bool,
    total_bytes_read: usize,
    incomplete_trailing: Option<String>,
}

/// Detailed accounting metrics produced by the drainer.
#[derive(Debug, Clone)]
pub struct DrainerSummary {
    pub total_bytes_read: usize,
    pub complete_acks: Vec<u64>,
    pub complete_errs: Vec<(u64, String)>,
    pub ack_ticks: HashMap<u64, u64>,
    pub err_ticks: HashMap<u64, u64>,
    pub malformed_lines: Vec<String>,
    pub other_lines: Vec<String>,
    pub incomplete_trailing: Option<String>,
}

/// Active drainer handle supervising standard output.
pub struct OutputDrainer {
    shared: Arc<Mutex<DrainerSharedState>>,
    condvar: Arc<Condvar>,
    thread_handle: Mutex<Option<JoinHandle<()>>>,
}

impl OutputDrainer {
    /// Start continuous background draining of the child stdout stream with an implicit clock.
    pub fn start(stdout: ChildStdout) -> Self {
        Self::start_with_clock(stdout, Arc::new(AtomicU64::new(1)))
    }

    /// Start continuous background draining of the child stdout stream with a shared event clock.
    pub fn start_with_clock(stdout: ChildStdout, clock: Arc<AtomicU64>) -> Self {
        let shared = Arc::new(Mutex::new(DrainerSharedState {
            complete_acks: Vec::new(),
            complete_errs: Vec::new(),
            ack_ticks: HashMap::new(),
            err_ticks: HashMap::new(),
            malformed_lines: Vec::new(),
            other_lines: Vec::new(),
            responses: HashMap::new(),
            is_eof: false,
            total_bytes_read: 0,
            incomplete_trailing: None,
        }));
        let condvar = Arc::new(Condvar::new());

        let shared_clone = Arc::clone(&shared);
        let condvar_clone = Arc::clone(&condvar);

        let thread_handle = thread::Builder::new()
            .name("stdout-drainer".to_string())
            .spawn(move || {
                Self::run_drain_loop(stdout, shared_clone, condvar_clone, clock);
            })
            .expect("Failed to spawn stdout drainer thread");

        Self {
            shared,
            condvar,
            thread_handle: Mutex::new(Some(thread_handle)),
        }
    }

    /// Internal loop continuously reading bytes and tokenizing complete lines.
    fn run_drain_loop(
        mut stdout: ChildStdout,
        shared: Arc<Mutex<DrainerSharedState>>,
        condvar: Arc<Condvar>,
        clock: Arc<AtomicU64>,
    ) {
        let mut buffer = [0u8; 8192];
        let mut line_accumulator: Vec<u8> = Vec::with_capacity(1024);

        loop {
            match stdout.read(&mut buffer) {
                Ok(0) => {
                    // EOF reached (pipe closed by child exit or termination)
                    break;
                }
                Ok(n) => {
                    let mut lock = shared.lock().unwrap();
                    lock.total_bytes_read += n;
                    drop(lock);

                    let slice = &buffer[..n];
                    for &byte in slice {
                        if byte == b'\n' {
                            // Complete line encountered
                            let mut complete_line = line_accumulator.clone();
                            line_accumulator.clear();

                            // Strip optional carriage return
                            if complete_line.ends_with(b"\r") {
                                complete_line.pop();
                            }

                            if let Ok(line_str) = String::from_utf8(complete_line) {
                                Self::process_complete_line(&line_str, &shared, &condvar, &clock);
                            } else {
                                let mut lock = shared.lock().unwrap();
                                lock.malformed_lines.push("<non-utf8 line>".to_string());
                            }
                        } else {
                            line_accumulator.push(byte);
                        }
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    // Read error (e.g. pipe broken on process kill)
                    break;
                }
            }
        }

        // EOF reached: check for incomplete trailing data
        let mut lock = shared.lock().unwrap();
        lock.is_eof = true;
        if !line_accumulator.is_empty() {
            // Incomplete trailing data detected: NEVER count as an ACK!
            let trailing_str = String::from_utf8_lossy(&line_accumulator).to_string();
            lock.incomplete_trailing = Some(trailing_str);
        }
        condvar.notify_all();
    }

    /// Classify and record a complete protocol line.
    fn process_complete_line(
        line: &str,
        shared: &Arc<Mutex<DrainerSharedState>>,
        condvar: &Arc<Condvar>,
        clock: &Arc<AtomicU64>,
    ) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return;
        }

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        let mut lock = shared.lock().unwrap();

        if parts.len() >= 2 && parts[0] == "ACK" {
            if let Ok(op_id) = parts[1].parse::<u64>() {
                let tick = clock.fetch_add(1, Ordering::SeqCst);
                lock.complete_acks.push(op_id);
                lock.ack_ticks.insert(op_id, tick);
                lock.responses.insert(op_id, ResponseStatus::Ack);
                condvar.notify_all();
                return;
            }
        }

        if parts.len() >= 2 && parts[0] == "ERR" {
            if let Ok(op_id) = parts[1].parse::<u64>() {
                let tick = clock.fetch_add(1, Ordering::SeqCst);
                let reason = parts[2..].join(" ");
                lock.complete_errs.push((op_id, reason.clone()));
                lock.err_ticks.insert(op_id, tick);
                lock.responses.insert(op_id, ResponseStatus::Err(reason));
                condvar.notify_all();
                return;
            }
        }

        // Neither ACK nor ERR
        lock.other_lines.push(line.to_string());
    }

    /// Writers call this to await the ACK or ERR response for their single outstanding operation.
    ///
    /// Returns `None` if EOF is reached or timeout expires before a complete response arrives.
    pub fn wait_for_op_response(&self, op_id: u64, timeout: Duration) -> Option<ResponseStatus> {
        let start = Instant::now();
        let mut lock = self.shared.lock().unwrap();

        while !lock.responses.contains_key(&op_id) {
            if lock.is_eof {
                return None;
            }
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return None;
            }
            let remaining = timeout - elapsed;
            let result = self.condvar.wait_timeout(lock, remaining).unwrap();
            lock = result.0;
        }

        lock.responses.get(&op_id).cloned()
    }

    /// Wait for the background drainer thread to finish reading EOF after process kill,
    /// and collect final drain metrics.
    pub fn join_and_summarize(&self) -> DrainerSummary {
        let handle = self.thread_handle.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.join();
        }

        let lock = self.shared.lock().unwrap();
        DrainerSummary {
            total_bytes_read: lock.total_bytes_read,
            complete_acks: lock.complete_acks.clone(),
            complete_errs: lock.complete_errs.clone(),
            ack_ticks: lock.ack_ticks.clone(),
            err_ticks: lock.err_ticks.clone(),
            malformed_lines: lock.malformed_lines.clone(),
            other_lines: lock.other_lines.clone(),
            incomplete_trailing: lock.incomplete_trailing.clone(),
        }
    }
}

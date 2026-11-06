//! Concurrent Writer System.
//!
//! Spawns two or more independent writer threads. Each writer possesses its own
//! PRNG stream, monotonically increasing sequence number, and key partition
//! (supporting both writer-owned and shared hot keys).
//! Operates with zero artificial think-time and enforces exactly one outstanding
//! operation per writer.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::generator::payload::{PayloadGenerator, to_hex};
use crate::generator::prng::PrngStream;
use crate::runner::drainer::OutputDrainer;
use crate::runner::killer::ProcessActivityTracker;
use crate::runner::supervisor::ChildSupervisor;

/// Sub-operation within an atomic batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchSubOp {
    Put { key: u64, payload: Vec<u8> },
    Del { key: u64 },
}

/// Type of workload operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpType {
    Put,
    Del,
    Batch { count: usize },
}

/// Ledger record of every operation sent by any writer.
#[derive(Debug, Clone)]
pub struct SentOpRecord {
    pub writer_id: u32,
    pub seq_num: u64,
    pub op_id: u64,
    pub key: u64,
    pub op_type: OpType,
    pub payload_len: usize,
    pub payload_crc: u32,
    pub payload: Option<Vec<u8>>,
    pub batch_ops: Vec<BatchSubOp>,
    pub dispatch_order: u64,
    pub t_sched: u64,
    pub t_sub: u64,
}

/// Writer configuration and state.
pub struct WriterThread {
    pub writer_id: u32,
    pub master_seed: u64,
    pub num_writers: u32,
    pub payload_size: usize,
}

impl WriterThread {
    /// Hot keys shared across all writers.
    pub const HOT_KEY_START: u64 = 90000;
    pub const HOT_KEY_COUNT: u64 = 100;

    /// Calculate key range owned exclusively by this writer.
    pub fn owned_key_range(&self) -> (u64, u64) {
        let base = (self.writer_id as u64) * 1000;
        (base, base + 1000)
    }

    /// Select a key: either from writer-owned space or shared hot-key space.
    pub fn select_key(&self, stream: &mut PrngStream) -> u64 {
        let is_hot = stream.next_range_u64(0, 100) < 40; // 40% hot keys, 60% owned keys
        if is_hot {
            Self::HOT_KEY_START + stream.next_range_u64(0, Self::HOT_KEY_COUNT)
        } else {
            let (min_key, max_key) = self.owned_key_range();
            stream.next_range_u64(min_key, max_key)
        }
    }

    /// Run the writer loop until child terminates or stop flag is raised.
    pub fn run_loop(
        self,
        supervisor: Arc<ChildSupervisor>,
        drainer: Arc<OutputDrainer>,
        activity: Arc<ProcessActivityTracker>,
        stop_flag: Arc<AtomicBool>,
        sent_ops_log: Arc<Mutex<Vec<SentOpRecord>>>,
        dispatch_counter: Arc<AtomicU64>,
    ) {
        let mut stream = PrngStream::new(self.master_seed, self.writer_id);
        let generator = PayloadGenerator::new(self.master_seed);
        let mut seq_num = 1u64;

        while !stop_flag.load(Ordering::SeqCst) {
            // Unique, deterministic Op ID: (seq * 1000) + writer_id
            let op_id = (seq_num * 1000) + (self.writer_id as u64);
            let op_choice = stream.next_range_u64(0, 100);

            if op_choice < 70 {
                // Single Put
                let key = self.select_key(&mut stream);
                let payload = generator.generate(self.writer_id, seq_num, key, self.payload_size);
                let crc = crate::generator::payload::crc32(&payload[..payload.len() - 4]);
                let cmd = format!("{} put {} {}", op_id, key, to_hex(&payload));

                activity.enter_write();
                match supervisor.send_command_interval(&cmd, &dispatch_counter) {
                    Ok((t_sched, t_sub)) => {
                        let record = SentOpRecord {
                            writer_id: self.writer_id,
                            seq_num,
                            op_id,
                            key,
                            op_type: OpType::Put,
                            payload_len: payload.len(),
                            payload_crc: crc,
                            payload: Some(payload),
                            batch_ops: Vec::new(),
                            dispatch_order: t_sched,
                            t_sched,
                            t_sub,
                        };
                        sent_ops_log.lock().unwrap().push(record);
                    }
                    Err(_) => {
                        activity.exit_write();
                        break;
                    }
                }

                // Wait for response (strictly one outstanding op per writer)
                let resp = drainer.wait_for_op_response(op_id, Duration::from_millis(500));
                activity.exit_write();

                if resp.is_none() {
                    break; // EOF reached on child death
                }
            } else if op_choice < 90 {
                // Single Del
                let key = self.select_key(&mut stream);
                let cmd = format!("{} del {}", op_id, key);

                activity.enter_write();
                match supervisor.send_command_interval(&cmd, &dispatch_counter) {
                    Ok((t_sched, t_sub)) => {
                        let record = SentOpRecord {
                            writer_id: self.writer_id,
                            seq_num,
                            op_id,
                            key,
                            op_type: OpType::Del,
                            payload_len: 0,
                            payload_crc: 0,
                            payload: None,
                            batch_ops: Vec::new(),
                            dispatch_order: t_sched,
                            t_sched,
                            t_sub,
                        };
                        sent_ops_log.lock().unwrap().push(record);
                    }
                    Err(_) => {
                        activity.exit_write();
                        break;
                    }
                }

                let resp = drainer.wait_for_op_response(op_id, Duration::from_millis(500));
                activity.exit_write();

                if resp.is_none() {
                    break;
                }
            } else {
                // Atomic Batch (2 to 5 operations)
                let batch_count = stream.next_range_usize(2, 6);
                let key = self.select_key(&mut stream);

                let mut batch_cmds = format!("{} batch {}\n", op_id, batch_count);
                let mut batch_members = Vec::with_capacity(batch_count);

                for i in 0..batch_count {
                    let b_key = self.select_key(&mut stream);
                    let is_b_del = stream.next_range_u64(0, 100) < 20;

                    if is_b_del {
                        batch_cmds.push_str(&format!("del {}\n", b_key));
                        batch_members.push(BatchSubOp::Del { key: b_key });
                    } else {
                        let payload = generator.generate(
                            self.writer_id,
                            seq_num + i as u64,
                            b_key,
                            self.payload_size,
                        );
                        batch_cmds.push_str(&format!("put {} {}\n", b_key, to_hex(&payload)));
                        batch_members.push(BatchSubOp::Put {
                            key: b_key,
                            payload,
                        });
                    }
                }

                activity.enter_batch();
                match supervisor.send_command_interval(&batch_cmds, &dispatch_counter) {
                    Ok((t_sched, t_sub)) => {
                        let record = SentOpRecord {
                            writer_id: self.writer_id,
                            seq_num,
                            op_id,
                            key,
                            op_type: OpType::Batch { count: batch_count },
                            payload_len: 0,
                            payload_crc: 0,
                            payload: None,
                            batch_ops: batch_members,
                            dispatch_order: t_sched,
                            t_sched,
                            t_sub,
                        };
                        sent_ops_log.lock().unwrap().push(record);
                    }
                    Err(_) => {
                        activity.exit_batch();
                        break;
                    }
                }

                let resp = drainer.wait_for_op_response(op_id, Duration::from_millis(1000));
                activity.exit_batch();

                if resp.is_none() {
                    break;
                }
            }

            seq_num += 1;
        }
    }
}

/// Spawns N concurrent writers and manages their execution handles.
pub struct ConcurrentWriterPool {
    handles: Vec<JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
    sent_ops_log: Arc<Mutex<Vec<SentOpRecord>>>,
    #[allow(dead_code)]
    pub dispatch_counter: Arc<AtomicU64>,
}

impl ConcurrentWriterPool {
    pub fn start(
        num_writers: u32,
        master_seed: u64,
        payload_size: usize,
        supervisor: Arc<ChildSupervisor>,
        drainer: Arc<OutputDrainer>,
        activity: Arc<ProcessActivityTracker>,
    ) -> Self {
        Self::start_with_clock(
            num_writers,
            master_seed,
            payload_size,
            supervisor,
            drainer,
            activity,
            Arc::new(AtomicU64::new(1)),
        )
    }

    pub fn start_with_clock(
        num_writers: u32,
        master_seed: u64,
        payload_size: usize,
        supervisor: Arc<ChildSupervisor>,
        drainer: Arc<OutputDrainer>,
        activity: Arc<ProcessActivityTracker>,
        clock: Arc<AtomicU64>,
    ) -> Self {
        assert!(
            num_writers >= 2,
            "Must configure at least 2 concurrent writers"
        );
        let stop_flag = Arc::new(AtomicBool::new(false));
        let sent_ops_log = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::with_capacity(num_writers as usize);

        for writer_id in 0..num_writers {
            let writer = WriterThread {
                writer_id,
                master_seed,
                num_writers,
                payload_size,
            };

            let sup = Arc::clone(&supervisor);
            let drn = Arc::clone(&drainer);
            let act = Arc::clone(&activity);
            let stop = Arc::clone(&stop_flag);
            let log = Arc::clone(&sent_ops_log);
            let clk = Arc::clone(&clock);

            let handle = thread::Builder::new()
                .name(format!("writer-{}", writer_id))
                .spawn(move || {
                    writer.run_loop(sup, drn, act, stop, log, clk);
                })
                .expect("Failed to spawn writer thread");

            handles.push(handle);
        }

        Self {
            handles,
            stop_flag,
            sent_ops_log,
            dispatch_counter: clock,
        }
    }

    /// Signals all writers to stop and reaps thread handles.
    pub fn stop_and_join(self) -> Vec<SentOpRecord> {
        self.stop_flag.store(true, Ordering::SeqCst);
        for handle in self.handles {
            let _ = handle.join();
        }
        let lock = self.sent_ops_log.lock().unwrap();
        lock.clone()
    }
}

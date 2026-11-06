//! Mock database engine processes for torture tool testing.
//!
//! IMPORTANT: These mocks exist EXCLUSIVELY to test the torture harness itself.
//! They simulate protocol interaction in memory without implementing real database internals,
//! storage, indexing, write-ahead logging, compaction, or persistence.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::time::Duration;

use crate::adapter::EngineProcess;

/// Simulated fault behavior injected into a mock engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MockBehavior {
    /// Fully compliant mock engine: processes commands and updates state accurately.
    Compliant,
    /// Simulates losing an acknowledged write (violates Rule 1: Durability).
    LostAckWrite { dropped_op_id: u64 },
    /// Simulates partial batch application (violates Rule 2: Batch Atomicity).
    PartialBatch { max_applied_ops: usize },
    /// Simulates a torn or corrupted payload on disk (violates Rule 4: Value Integrity).
    CorruptedValue { corrupted_key: u64 },
    /// Simulates resurrecting a deleted key after ACK (violates Rule 1 & 3).
    ResurrectedDelete { resurrected_key: u64 },
    /// Simulates unexpected error for a valid write (violates error contract).
    SpuriousError { failing_op_id: u64 },
}

/// In-memory mock engine session implementing `EngineProcess`.
pub struct MockEngine {
    behavior: MockBehavior,
    state: BTreeMap<u64, Vec<u8>>,
    output_queue: VecDeque<String>,
    is_killed: bool,
    pending_batch: Option<(u64, usize, Vec<(u64, u64, Option<Vec<u8>>)>)>, // (batch_id, target_count, ops)
}

impl MockEngine {
    /// Create a new mock engine configured with specific behavior.
    pub fn new(behavior: MockBehavior) -> Self {
        Self {
            behavior,
            state: BTreeMap::new(),
            output_queue: VecDeque::new(),
            is_killed: false,
            pending_batch: None,
        }
    }

    /// Return the current in-memory simulated state (with injected corruptions applied on dump).
    pub fn dump_state(&self) -> BTreeMap<u64, Vec<u8>> {
        let mut dumped = self.state.clone();

        // If configured for CorruptedValue, corrupt the target key in the dump
        if let MockBehavior::CorruptedValue { corrupted_key } = &self.behavior {
            if let Some(val) = dumped.get_mut(corrupted_key) {
                if !val.is_empty() {
                    // Invert a byte to guarantee CRC32 and body mismatch
                    let mid = val.len() / 2;
                    val[mid] ^= 0xAA;
                }
            }
        }

        dumped
    }

    /// Helper to process a command line.
    fn handle_command(&mut self, line: &str) {
        if self.is_killed {
            return;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            return;
        }

        // If inside a multi-line batch definition
        if let Some((batch_id, count, mut ops)) = self.pending_batch.take() {
            let parts: Vec<&str> = trimmed.split_whitespace().collect();
            if parts.len() >= 2 {
                let op_type = parts[0];
                let key_result = parts[1].parse::<u64>();

                if let Ok(key) = key_result {
                    if op_type == "put" && parts.len() >= 3 {
                        let val_bytes = crate::generator::payload::from_hex(parts[2])
                            .unwrap_or_else(|_| parts[2].as_bytes().to_vec());
                        ops.push((batch_id, key, Some(val_bytes)));
                    } else if op_type == "del" {
                        ops.push((batch_id, key, None));
                    }
                }
            }

            if ops.len() >= count {
                // Batch definition complete, commit according to behavior
                self.commit_batch(batch_id, ops);
            } else {
                self.pending_batch = Some((batch_id, count, ops));
            }
            return;
        }

        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.is_empty() {
            return;
        }

        // Check for batch command: "<id> batch <count>"
        if parts.len() >= 3 && parts[1] == "batch" {
            if let (Ok(op_id), Ok(count)) = (parts[0].parse::<u64>(), parts[2].parse::<usize>()) {
                if count == 0 {
                    self.output_queue.push_back(format!("ACK {}", op_id));
                } else {
                    self.pending_batch = Some((op_id, count, Vec::with_capacity(count)));
                }
            }
            return;
        }

        // Single put: "<id> put <key> <val>"
        if parts.len() >= 4 && parts[1] == "put" {
            if let (Ok(op_id), Ok(key)) = (parts[0].parse::<u64>(), parts[2].parse::<u64>()) {
                if let MockBehavior::SpuriousError { failing_op_id } = self.behavior {
                    if failing_op_id == op_id {
                        self.output_queue
                            .push_back(format!("ERR {} SPURIOUS_MOCK_ERROR", op_id));
                        return;
                    }
                }

                let val_bytes = crate::generator::payload::from_hex(parts[3])
                    .unwrap_or_else(|_| parts[3].as_bytes().to_vec());

                // If LostAckWrite matches this op_id, emit ACK but omit from state
                if let MockBehavior::LostAckWrite { dropped_op_id } = self.behavior {
                    if dropped_op_id == op_id {
                        self.output_queue.push_back(format!("ACK {}", op_id));
                        return;
                    }
                }

                self.state.insert(key, val_bytes);
                self.output_queue.push_back(format!("ACK {}", op_id));
                return;
            }
        }

        // Single del: "<id> del <key>"
        if parts.len() >= 3 && parts[1] == "del" {
            if let (Ok(op_id), Ok(key)) = (parts[0].parse::<u64>(), parts[2].parse::<u64>()) {
                // If ResurrectedDelete matches, keep key in state despite emitting ACK
                if let MockBehavior::ResurrectedDelete { resurrected_key } = self.behavior {
                    if resurrected_key == key {
                        self.output_queue.push_back(format!("ACK {}", op_id));
                        return;
                    }
                }

                self.state.remove(&key);
                self.output_queue.push_back(format!("ACK {}", op_id));
                return;
            }
        }

        // Fallback for ping or unhandled line
        if let Ok(op_id) = parts[0].parse::<u64>() {
            self.output_queue.push_back(format!("ACK {}", op_id));
        }
    }

    fn commit_batch(&mut self, batch_id: u64, ops: Vec<(u64, u64, Option<Vec<u8>>)>) {
        let max_ops = match self.behavior {
            MockBehavior::PartialBatch { max_applied_ops } => max_applied_ops.min(ops.len()),
            _ => ops.len(),
        };

        for (_, key, val_opt) in ops.into_iter().take(max_ops) {
            match val_opt {
                Some(val) => {
                    self.state.insert(key, val);
                }
                None => {
                    self.state.remove(&key);
                }
            }
        }

        // Mock returns ACK for the batch
        self.output_queue.push_back(format!("ACK {}", batch_id));
    }
}

impl EngineProcess for MockEngine {
    fn pid(&self) -> u32 {
        99999 // Simulated mock PID
    }

    fn send_raw_op(&mut self, op: &str) -> io::Result<()> {
        if self.is_killed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Cannot write to terminated mock process",
            ));
        }
        self.handle_command(op);
        Ok(())
    }

    fn poll_next_response(&mut self, _timeout: Duration) -> io::Result<Option<String>> {
        Ok(self.output_queue.pop_front())
    }

    fn hard_kill(&mut self) -> io::Result<()> {
        self.is_killed = true;
        Ok(())
    }

    fn drain_stdout(&mut self) -> Vec<String> {
        self.output_queue.drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compliant_mock_basic_lifecycle() {
        let mut mock = MockEngine::new(MockBehavior::Compliant);

        // Put op 1
        mock.send_raw_op("1 put 100 01020304").unwrap();
        let resp = mock.poll_next_response(Duration::from_millis(10)).unwrap();
        assert_eq!(resp, Some("ACK 1".to_string()));

        let dumped = mock.dump_state();
        assert_eq!(dumped.get(&100), Some(&vec![1, 2, 3, 4]));

        // Del op 2
        mock.send_raw_op("2 del 100").unwrap();
        let resp = mock.poll_next_response(Duration::from_millis(10)).unwrap();
        assert_eq!(resp, Some("ACK 2".to_string()));

        let dumped = mock.dump_state();
        assert_eq!(dumped.get(&100), None);
    }

    #[test]
    fn test_lost_ack_mock_behavior() {
        let mut mock = MockEngine::new(MockBehavior::LostAckWrite { dropped_op_id: 10 });

        mock.send_raw_op("10 put 50 0102").unwrap();
        // Returns ACK, but write was dropped
        let resp = mock.poll_next_response(Duration::from_millis(10)).unwrap();
        assert_eq!(resp, Some("ACK 10".to_string()));

        let dumped = mock.dump_state();
        assert_eq!(
            dumped.get(&50),
            None,
            "LostAckWrite must drop the write from dumped state"
        );
    }

    #[test]
    fn test_partial_batch_mock_behavior() {
        let mut mock = MockEngine::new(MockBehavior::PartialBatch { max_applied_ops: 1 });

        // Start batch of 2 ops
        mock.send_raw_op("100 batch 2").unwrap();
        mock.send_raw_op("put 1 0a0b").unwrap();
        mock.send_raw_op("put 2 0c0d").unwrap();

        let resp = mock.poll_next_response(Duration::from_millis(10)).unwrap();
        assert_eq!(resp, Some("ACK 100".to_string()));

        let dumped = mock.dump_state();
        assert!(
            dumped.contains_key(&1),
            "First op in batch should be applied"
        );
        assert!(
            !dumped.contains_key(&2),
            "Second op in batch should be dropped by partial mock"
        );
    }
}

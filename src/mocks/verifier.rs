//! Verification logic for Mutation Self-Tests.
//!
//! Validates dumped state against acknowledged operations to detect whether
//! simulated faults in broken mock engines are reliably caught by the test infrastructure.

use std::collections::BTreeMap;

use crate::generator::payload::{PayloadError, PayloadGenerator};

/// Invariant violations detected by the mutation verification engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationViolation {
    /// Rule 1 Violation: An operation was ACKed, but its key/value is missing from dumped state.
    LostAckWrite { op_id: u64, key: u64 },
    /// Rule 2 Violation: An ACKed batch was only partially applied (some keys present, others missing).
    PartialBatch {
        batch_id: u64,
        applied_keys: Vec<u64>,
        missing_keys: Vec<u64>,
    },
    /// Rule 3 Violation: An ACKed delete operation failed to remove the key, and it reappeared.
    ResurrectedDelete { op_id: u64, key: u64 },
    /// Rule 4 Violation: A value in the dumped state failed checksum, length, or body integrity.
    CorruptedValue { key: u64, reason: String },
    /// Error contract violation: A valid operation was rejected with an ERR response.
    SpuriousError { op_id: u64, error_msg: String },
}

impl std::fmt::Display for MutationViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LostAckWrite { op_id, key } => {
                write!(
                    f,
                    "RULE 1 VIOLATION: Lost ACKed write (Op ID {}, Key {})",
                    op_id, key
                )
            }
            Self::PartialBatch {
                batch_id,
                applied_keys,
                missing_keys,
            } => {
                write!(
                    f,
                    "RULE 2 VIOLATION: Partial batch atomicity failure (Batch ID {}, Applied {:?}, Missing {:?})",
                    batch_id, applied_keys, missing_keys
                )
            }
            Self::ResurrectedDelete { op_id, key } => {
                write!(
                    f,
                    "RULE 1/3 VIOLATION: Resurrected delete (Op ID {}, Key {})",
                    op_id, key
                )
            }
            Self::CorruptedValue { key, reason } => {
                write!(
                    f,
                    "RULE 4 VIOLATION: Value corruption detected on Key {} ({})",
                    key, reason
                )
            }
            Self::SpuriousError { op_id, error_msg } => {
                write!(
                    f,
                    "PROTOCOL VIOLATION: Spurious ERR on Op ID {} ({})",
                    op_id, error_msg
                )
            }
        }
    }
}

/// An operation tracked by the mutation verification harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackedOp {
    Put {
        op_id: u64,
        key: u64,
        payload: Vec<u8>,
    },
    Del {
        op_id: u64,
        key: u64,
    },
    Batch {
        batch_id: u64,
        ops: Vec<(u64, Option<Vec<u8>>)>, // (key, Some(payload) for put, None for del)
    },
}

/// Verifies whether the dumped state from a mock matches the tracked operations.
pub fn verify_mock_state(
    run_seed: u64,
    tracked_ops: &[TrackedOp],
    acked_ops: &[u64],
    errored_ops: &[(u64, String)],
    dump: &BTreeMap<u64, Vec<u8>>,
) -> Result<(), MutationViolation> {
    // 1. Check for spurious errors
    if let Some((err_id, reason)) = errored_ops.first() {
        return Err(MutationViolation::SpuriousError {
            op_id: *err_id,
            error_msg: reason.clone(),
        });
    }

    // 2. First validate the structural integrity of EVERY value present in the dump (Rule 4)
    for (&key, val) in dump {
        if let Err(err) = PayloadGenerator::validate(val, Some(key), Some(run_seed)) {
            let reason = match err {
                PayloadError::ChecksumMismatch { stored, computed } => {
                    format!(
                        "CRC32 mismatch (stored {:#010X}, computed {:#010X})",
                        stored, computed
                    )
                }
                PayloadError::CorruptedBody { offset } => {
                    format!("Deterministic body corrupted at byte {}", offset)
                }
                other => format!("{:?}", other),
            };
            return Err(MutationViolation::CorruptedValue { key, reason });
        }
    }

    // 3. Track effective expected state from ACKed operations in chronological order
    let mut expected_state: BTreeMap<u64, (u64, Vec<u8>)> = BTreeMap::new(); // key -> (op_id, payload)
    let mut deleted_keys: BTreeMap<u64, u64> = BTreeMap::new(); // key -> op_id

    for op in tracked_ops {
        match op {
            TrackedOp::Put {
                op_id,
                key,
                payload,
            } => {
                if acked_ops.contains(op_id) {
                    expected_state.insert(*key, (*op_id, payload.clone()));
                    deleted_keys.remove(key);
                }
            }
            TrackedOp::Del { op_id, key } => {
                if acked_ops.contains(op_id) {
                    expected_state.remove(key);
                    deleted_keys.insert(*key, *op_id);
                }
            }
            TrackedOp::Batch { batch_id, ops } => {
                if acked_ops.contains(batch_id) {
                    // Check batch atomicity in the dump
                    let mut applied = Vec::new();
                    let mut missing = Vec::new();

                    for (key, val_opt) in ops {
                        match val_opt {
                            Some(expected_val) => {
                                if let Some(dumped_val) = dump.get(key) {
                                    if dumped_val == expected_val {
                                        applied.push(*key);
                                    } else {
                                        missing.push(*key);
                                    }
                                } else {
                                    missing.push(*key);
                                }
                            }
                            None => {
                                if dump.contains_key(key) {
                                    missing.push(*key);
                                } else {
                                    applied.push(*key);
                                }
                            }
                        }
                    }

                    // If some applied and some missing, it's a partial batch violation
                    if !applied.is_empty() && !missing.is_empty() {
                        return Err(MutationViolation::PartialBatch {
                            batch_id: *batch_id,
                            applied_keys: applied,
                            missing_keys: missing,
                        });
                    }

                    // Apply to expected state
                    for (key, val_opt) in ops {
                        if let Some(val) = val_opt {
                            expected_state.insert(*key, (*batch_id, val.clone()));
                            deleted_keys.remove(key);
                        } else {
                            expected_state.remove(key);
                            deleted_keys.insert(*key, *batch_id);
                        }
                    }
                }
            }
        }
    }

    // 4. Verify that all expected ACKed writes exist in dump (Rule 1)
    for (key, (op_id, expected_val)) in &expected_state {
        match dump.get(key) {
            Some(actual_val) => {
                if actual_val != expected_val {
                    return Err(MutationViolation::LostAckWrite {
                        op_id: *op_id,
                        key: *key,
                    });
                }
            }
            None => {
                return Err(MutationViolation::LostAckWrite {
                    op_id: *op_id,
                    key: *key,
                });
            }
        }
    }

    // 5. Verify that deleted keys do not exist in dump (Rule 1/3)
    for (key, del_op_id) in &deleted_keys {
        if dump.contains_key(key) {
            return Err(MutationViolation::ResurrectedDelete {
                op_id: *del_op_id,
                key: *key,
            });
        }
    }

    Ok(())
}

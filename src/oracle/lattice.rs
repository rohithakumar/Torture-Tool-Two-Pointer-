//! State Lattice and Concurrency Semantics.
//!
//! Models uncertain crash states, concurrent happens-before relationships,
//! in-flight ambiguous outcomes, and multi-key batch atomicity.

use std::collections::{BTreeMap, HashMap};

use crate::oracle::ledger::{OpStatus, OracleLedger};
use crate::runner::writers::BatchSubOp;

/// An individual effect applied to a specific key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyEffectKind {
    Put(Vec<u8>),
    Delete,
}

/// A discrete key mutation event derived from the ledger.
#[derive(Debug, Clone)]
pub struct KeyMutation {
    pub op_id: u64,
    pub writer_id: u32,
    pub seq_num: u64,
    pub key: u64,
    pub dispatch_order: u64,
    pub ack_order: Option<u64>,
    pub status: OpStatus,
    pub kind: KeyEffectKind,
    pub batch_id: Option<u64>,
}

/// A legally permitted outcome for a specific key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegalCandidate {
    /// Expected payload bytes (None if deleted or absent)
    pub expected_value: Option<Vec<u8>>,
    /// Operation ID that produced this candidate (None if initial absent state)
    pub source_op_id: Option<u64>,
    /// Associated in-flight batch requirement: batch_id -> must be applied (true) or not applied (false)
    pub batch_conditions: HashMap<u64, bool>,
}

/// Invariant violations detected by the oracle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvariantViolation {
    /// Rule 1: Acknowledged write lost (key missing or older value present without superseding write)
    LostAckWrite {
        op_id: u64,
        key: u64,
        details: String,
    },
    /// Rule 1/3: Acknowledged delete resurrected
    ResurrectedDelete { op_id: u64, key: u64 },
    /// Rule 2: An in-flight operation was illegally evaluated
    IllegalInFlightState { key: u64, details: String },
    /// Rule 3: An operation that returned ERR appeared in recovered state
    RejectedOpVisible {
        op_id: u64,
        key: u64,
        reason: String,
    },
    /// Rule 4: Atomic batch partially applied
    PartialBatch {
        batch_id: u64,
        applied_keys: Vec<u64>,
        unapplied_keys: Vec<u64>,
    },
    /// Rule 5: Value integrity, checksum, or format violation
    CorruptedValue { key: u64, error: String },
    /// Rule 5: Foreign value or value written under wrong key/seed
    ForeignValue { key: u64, details: String },
}

impl std::fmt::Display for InvariantViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LostAckWrite {
                op_id,
                key,
                details,
            } => {
                write!(
                    f,
                    "RULE 1 VIOLATION (Lost ACK): Op {} on Key {} lost: {}",
                    op_id, key, details
                )
            }
            Self::ResurrectedDelete { op_id, key } => {
                write!(
                    f,
                    "RULE 1/3 VIOLATION (Resurrected Delete): Key {} was ACK-deleted by Op {} but is present",
                    key, op_id
                )
            }
            Self::IllegalInFlightState { key, details } => {
                write!(
                    f,
                    "RULE 2 VIOLATION (Illegal In-Flight State): Key {}: {}",
                    key, details
                )
            }
            Self::RejectedOpVisible { op_id, key, reason } => {
                write!(
                    f,
                    "RULE 3 VIOLATION (Rejected Op Visible): Op {} on Key {} failed with ERR ({}) but effect is on disk",
                    op_id, key, reason
                )
            }
            Self::PartialBatch {
                batch_id,
                applied_keys,
                unapplied_keys,
            } => {
                write!(
                    f,
                    "RULE 4 VIOLATION (Partial Batch): Batch {} broke atomicity: applied {:?}, unapplied {:?}",
                    batch_id, applied_keys, unapplied_keys
                )
            }
            Self::CorruptedValue { key, error } => {
                write!(
                    f,
                    "RULE 5 VIOLATION (Corrupted Value): Key {}: {}",
                    key, error
                )
            }
            Self::ForeignValue { key, details } => {
                write!(
                    f,
                    "RULE 5 VIOLATION (Foreign Value): Key {}: {}",
                    key, details
                )
            }
        }
    }
}

/// State lattice modeling all legally possible outcomes across all keys.
pub struct StateLattice {
    /// All mutations grouped by key in chronological order
    mutations_by_key: BTreeMap<u64, Vec<KeyMutation>>,
    /// All batches tracked: batch_id -> list of touched keys
    batches: HashMap<u64, (OpStatus, Vec<u64>)>,
}

impl StateLattice {
    /// Builds the state lattice from the frozen ledger.
    pub fn from_ledger(ledger: &OracleLedger) -> Self {
        let mut mutations_by_key: BTreeMap<u64, Vec<KeyMutation>> = BTreeMap::new();
        let mut batches = HashMap::new();

        for entry in &ledger.entries {
            match &entry.op_type {
                crate::runner::writers::OpType::Put => {
                    if let Some(payload) = &entry.payload {
                        mutations_by_key
                            .entry(entry.key)
                            .or_default()
                            .push(KeyMutation {
                                op_id: entry.op_id,
                                writer_id: entry.writer_id,
                                seq_num: entry.seq_num,
                                key: entry.key,
                                dispatch_order: entry.dispatch_order,
                                ack_order: entry.ack_order,
                                status: entry.status.clone(),
                                kind: KeyEffectKind::Put(payload.clone()),
                                batch_id: None,
                            });
                    }
                }
                crate::runner::writers::OpType::Del => {
                    mutations_by_key
                        .entry(entry.key)
                        .or_default()
                        .push(KeyMutation {
                            op_id: entry.op_id,
                            writer_id: entry.writer_id,
                            seq_num: entry.seq_num,
                            key: entry.key,
                            dispatch_order: entry.dispatch_order,
                            ack_order: entry.ack_order,
                            status: entry.status.clone(),
                            kind: KeyEffectKind::Delete,
                            batch_id: None,
                        });
                }
                crate::runner::writers::OpType::Batch { .. } => {
                    let mut touched_keys = Vec::new();
                    for sub_op in &entry.batch_ops {
                        match sub_op {
                            BatchSubOp::Put { key, payload } => {
                                touched_keys.push(*key);
                                mutations_by_key.entry(*key).or_default().push(KeyMutation {
                                    op_id: entry.op_id,
                                    writer_id: entry.writer_id,
                                    seq_num: entry.seq_num,
                                    key: *key,
                                    dispatch_order: entry.dispatch_order,
                                    ack_order: entry.ack_order,
                                    status: entry.status.clone(),
                                    kind: KeyEffectKind::Put(payload.clone()),
                                    batch_id: Some(entry.op_id),
                                });
                            }
                            BatchSubOp::Del { key } => {
                                touched_keys.push(*key);
                                mutations_by_key.entry(*key).or_default().push(KeyMutation {
                                    op_id: entry.op_id,
                                    writer_id: entry.writer_id,
                                    seq_num: entry.seq_num,
                                    key: *key,
                                    dispatch_order: entry.dispatch_order,
                                    ack_order: entry.ack_order,
                                    status: entry.status.clone(),
                                    kind: KeyEffectKind::Delete,
                                    batch_id: Some(entry.op_id),
                                });
                            }
                        }
                    }
                    batches.insert(entry.op_id, (entry.status.clone(), touched_keys));
                }
            }
        }

        Self {
            mutations_by_key,
            batches,
        }
    }

    /// Determines if mutation A strictly happens-before mutation B (A < B).
    pub fn happens_before(a: &KeyMutation, b: &KeyMutation) -> bool {
        // Per-writer ordering: same writer with smaller sequence number
        if a.writer_id == b.writer_id && a.seq_num < b.seq_num {
            return true;
        }

        // Cross-writer happens-before: A was acknowledged before B was dispatched
        if let Some(ack_tick) = a.ack_order {
            if ack_tick < b.dispatch_order {
                return true;
            }
        }

        false
    }

    /// Computes all legally possible values for a specific key under concurrency and crash rules.
    pub fn legal_candidates_for_key(&self, key: u64) -> Vec<LegalCandidate> {
        let mutations = match self.mutations_by_key.get(&key) {
            Some(m) if !m.is_empty() => m,
            _ => {
                // Key was never touched; must be absent
                return vec![LegalCandidate {
                    expected_value: None,
                    source_op_id: None,
                    batch_conditions: HashMap::new(),
                }];
            }
        };

        // Filter out rejected mutations (Rule 3: ERR has zero effect)
        let valid_mutations: Vec<&KeyMutation> = mutations
            .iter()
            .filter(|m| !matches!(m.status, OpStatus::Rejected { .. }))
            .collect();

        if valid_mutations.is_empty() {
            return vec![LegalCandidate {
                expected_value: None,
                source_op_id: None,
                batch_conditions: HashMap::new(),
            }];
        }

        let mut candidates = Vec::new();

        // 1. Find all acknowledged mutations that are NOT superseded by a later acknowledged mutation
        let mut acked_non_superseded: Vec<&KeyMutation> = Vec::new();
        for m1 in &valid_mutations {
            if let OpStatus::Acknowledged { .. } = m1.status {
                let superseded = valid_mutations.iter().any(|m2| {
                    m1.op_id != m2.op_id
                        && matches!(m2.status, OpStatus::Acknowledged { .. })
                        && Self::happens_before(m1, m2)
                });
                if !superseded {
                    acked_non_superseded.push(m1);
                }
            }
        }

        // Add acknowledged candidates
        for m in &acked_non_superseded {
            let mut conds = HashMap::new();
            if let Some(bid) = m.batch_id {
                conds.insert(bid, true);
            }
            candidates.push(LegalCandidate {
                expected_value: match &m.kind {
                    KeyEffectKind::Put(v) => Some(v.clone()),
                    KeyEffectKind::Delete => None,
                },
                source_op_id: Some(m.op_id),
                batch_conditions: conds,
            });
        }

        // 2. In-flight mutations (Rule 2: may or may not have been applied)
        for m in &valid_mutations {
            if let OpStatus::InFlight = m.status {
                // In-flight op applied
                let mut conds_applied = HashMap::new();
                if let Some(bid) = m.batch_id {
                    conds_applied.insert(bid, true);
                }
                candidates.push(LegalCandidate {
                    expected_value: match &m.kind {
                        KeyEffectKind::Put(v) => Some(v.clone()),
                        KeyEffectKind::Delete => None,
                    },
                    source_op_id: Some(m.op_id),
                    batch_conditions: conds_applied,
                });

                // In-flight batch unapplied: also permit the prior state before this batch
                if let Some(bid) = m.batch_id {
                    let mut conds_unapplied = HashMap::new();
                    conds_unapplied.insert(bid, false);

                    // Find state prior to this batch
                    let prior = valid_mutations
                        .iter()
                        .take_while(|other| other.op_id != m.op_id)
                        .last();

                    candidates.push(LegalCandidate {
                        expected_value: prior.and_then(|p| match &p.kind {
                            KeyEffectKind::Put(v) => Some(v.clone()),
                            KeyEffectKind::Delete => None,
                        }),
                        source_op_id: prior.map(|p| p.op_id),
                        batch_conditions: conds_unapplied,
                    });
                }
            }
        }

        // If no acknowledged mutation ever existed and all were in-flight, absent is also legal
        if acked_non_superseded.is_empty() {
            candidates.push(LegalCandidate {
                expected_value: None,
                source_op_id: None,
                batch_conditions: HashMap::new(),
            });
        }

        candidates
    }

    /// Verifies batch atomicity across all touched keys.
    pub fn verify_batch_atomicity(
        &self,
        dump: &BTreeMap<u64, Vec<u8>>,
    ) -> Result<(), InvariantViolation> {
        for (&batch_id, (status, keys)) in &self.batches {
            if keys.is_empty() {
                continue;
            }

            let mut applied_keys = Vec::new();
            let mut unapplied_keys = Vec::new();

            for &key in keys {
                let mutations = self.mutations_by_key.get(&key).unwrap();
                let batch_mutation = mutations.iter().find(|m| m.batch_id == Some(batch_id));

                if let Some(bm) = batch_mutation {
                    // Check if key was superseded by a later acknowledged mutation
                    let is_superseded = mutations.iter().any(|other| {
                        matches!(other.status, OpStatus::Acknowledged { .. })
                            && other.op_id != batch_id
                            && Self::happens_before(bm, other)
                    });

                    if is_superseded {
                        // Key's value was legitimately overwritten by a subsequent durable operation
                        applied_keys.push(key);
                        continue;
                    }

                    match &bm.kind {
                        KeyEffectKind::Put(expected_bytes) => {
                            if dump.get(&key) == Some(expected_bytes) {
                                applied_keys.push(key);
                            } else {
                                unapplied_keys.push(key);
                            }
                        }
                        KeyEffectKind::Delete => {
                            if !dump.contains_key(&key) {
                                applied_keys.push(key);
                            } else {
                                unapplied_keys.push(key);
                            }
                        }
                    }
                }
            }

            // Rule 4: If an acknowledged batch is not 100% applied, or an in-flight batch is partially applied
            if !applied_keys.is_empty() && !unapplied_keys.is_empty() {
                return Err(InvariantViolation::PartialBatch {
                    batch_id,
                    applied_keys,
                    unapplied_keys,
                });
            }

            // An acknowledged batch MUST be completely applied
            if let OpStatus::Acknowledged { .. } = status {
                if !unapplied_keys.is_empty() {
                    return Err(InvariantViolation::LostAckWrite {
                        op_id: batch_id,
                        key: unapplied_keys[0],
                        details: format!("Acknowledged batch {} was not fully applied", batch_id),
                    });
                }
            }

            // A rejected batch MUST be completely unapplied
            if let OpStatus::Rejected { reason, .. } = status {
                if !applied_keys.is_empty() {
                    return Err(InvariantViolation::RejectedOpVisible {
                        op_id: batch_id,
                        key: applied_keys[0],
                        reason: reason.clone(),
                    });
                }
            }
        }

        Ok(())
    }
}

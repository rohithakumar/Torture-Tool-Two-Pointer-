//! Independent Operation Ledger and Event Accounting.
//!
//! Exclusively captures operations sent by the torture harness, complete ACKs received,
//! complete ERRs received, and in-flight operations at the time of the crash.
//! NEVER queries or trusts the database engine to build its ledger.

use std::collections::{HashMap, HashSet};

use crate::runner::writers::{BatchSubOp, OpType, SentOpRecord};

/// Execution and confirmation status of an operation at crash time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpStatus {
    /// Received a complete ACK line before the crash
    Acknowledged { ack_order: u64 },
    /// Received an explicit ERR response before the crash
    Rejected { err_order: u64, reason: String },
    /// Dispatched by a writer, but no complete ACK or ERR was received before SIGKILL
    InFlight,
}

/// A discrete key mutation event derived from the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyMutationRecord {
    pub op_id: u64,
    pub sub_op_idx: Option<usize>,
    pub writer_id: u32,
    pub seq_num: u64,
    pub key: u64,
    pub t_sched: u64,
    pub t_sub: u64,
    pub ack_order: Option<u64>,
    pub status: OpStatus,
    pub is_delete: bool,
    pub payload: Option<Vec<u8>>,
}

/// A fully tracked operation event in the independent oracle.
#[derive(Debug, Clone)]
pub struct LedgerEntry {
    pub op_id: u64,
    pub writer_id: u32,
    pub seq_num: u64,
    pub key: u64,
    pub op_type: OpType,
    pub payload: Option<Vec<u8>>,
    pub batch_ops: Vec<BatchSubOp>,
    pub t_sched: u64,
    pub t_sub: u64,
    pub dispatch_order: u64,
    pub ack_order: Option<u64>,
    pub status: OpStatus,
}

impl LedgerEntry {
    /// Conservative happens-before relation between two operations.
    ///
    /// Returns true if and only if:
    /// 1. Both ops share the same writer and self.seq_num < other.seq_num (program order), OR
    /// 2. self was completely ACKed before other began dispatch scheduling (ack_order < other.t_sched).
    pub fn happens_before(&self, other: &LedgerEntry) -> bool {
        // Per-writer program order
        if self.writer_id == other.writer_id && self.seq_num < other.seq_num {
            return true;
        }

        // Cross-writer conservative durability boundary
        if let Some(ack_tick) = self.ack_order {
            if ack_tick < other.t_sched {
                return true;
            }
        }

        false
    }

    /// Conservative happens-before relation between this ledger entry and a key mutation.
    ///
    /// Returns true if and only if:
    /// 1. Both share the same writer and self.seq_num < m.seq_num (program order), OR
    /// 2. self was completely ACKed before m began dispatch scheduling (ack_order < m.t_sched).
    pub fn happens_before_mutation(&self, m: &KeyMutationRecord) -> bool {
        // Per-writer program order
        if self.writer_id == m.writer_id && self.seq_num < m.seq_num {
            return true;
        }

        // Cross-writer conservative durability boundary
        if let Some(ack_tick) = self.ack_order {
            if ack_tick < m.t_sched {
                return true;
            }
        }

        false
    }
}

impl KeyMutationRecord {
    /// Conservative happens-before relation between two key mutations.
    ///
    /// Returns true if and only if:
    /// 1. Both share the same writer and self.seq_num < other.seq_num (program order), OR
    /// 2. self was completely ACKed before other began dispatch scheduling (ack_order < other.t_sched).
    pub fn happens_before(&self, other: &KeyMutationRecord) -> bool {
        // Per-writer program order
        if self.writer_id == other.writer_id && self.seq_num < other.seq_num {
            return true;
        }

        // Cross-writer conservative durability boundary
        if let Some(ack_tick) = self.ack_order {
            if ack_tick < other.t_sched {
                return true;
            }
        }

        false
    }
}

/// Central oracle ledger.
#[derive(Debug, Clone, Default)]
pub struct OracleLedger {
    pub entries: Vec<LedgerEntry>,
    pub mutations_by_key: HashMap<u64, Vec<KeyMutationRecord>>,
    pub acked_count: usize,
    pub rejected_count: usize,
    pub inflight_count: usize,
}

impl OracleLedger {
    /// Builds the frozen oracle ledger by correlating sent operations with ACK and ERR streams.
    pub fn build(
        sent_ops: Vec<SentOpRecord>,
        complete_acks: &[u64],
        complete_errs: &[(u64, String)],
    ) -> Self {
        Self::build_with_ticks(sent_ops, complete_acks, &[], complete_errs)
    }

    /// Builds the frozen oracle ledger with explicit monotonic event ticks for precise happens-before.
    ///
    /// Never synthesizes artificial ACK timestamps. If no tick is provided for an ACK,
    /// it is treated without a relative tick to prevent manufacturing false happens-before.
    pub fn build_with_ticks(
        sent_ops: Vec<SentOpRecord>,
        complete_acks: &[u64],
        ack_ticks: &[(u64, u64)],
        complete_errs: &[(u64, String)],
    ) -> Self {
        let acked_set: HashSet<u64> = complete_acks.iter().cloned().collect();

        // Map of op_id -> ack tick (from actual drainer timestamps)
        let mut ack_map = HashMap::new();
        for &(op_id, tick) in ack_ticks {
            ack_map.insert(op_id, tick);
        }

        // Map of op_id -> err order and reason
        let mut err_map = HashMap::new();
        for (idx, (err_id, reason)) in complete_errs.iter().enumerate() {
            err_map.insert(*err_id, (idx as u64, reason.clone()));
        }

        let mut entries = Vec::with_capacity(sent_ops.len());
        let mut mutations_by_key: HashMap<u64, Vec<KeyMutationRecord>> = HashMap::new();
        let mut acked_count = 0;
        let mut rejected_count = 0;
        let mut inflight_count = 0;

        for op in sent_ops {
            let t_sched = if op.t_sched != 0 {
                op.t_sched
            } else {
                op.dispatch_order
            };
            let t_sub = if op.t_sub != 0 {
                op.t_sub
            } else {
                op.dispatch_order
            };

            let (status, ack_order) = if let Some(&ack_tick) = ack_map.get(&op.op_id) {
                acked_count += 1;
                (
                    OpStatus::Acknowledged {
                        ack_order: ack_tick,
                    },
                    Some(ack_tick),
                )
            } else if acked_set.contains(&op.op_id) {
                acked_count += 1;
                (OpStatus::Acknowledged { ack_order: 0 }, None)
            } else if let Some((err_order, reason)) = err_map.get(&op.op_id) {
                rejected_count += 1;
                (
                    OpStatus::Rejected {
                        err_order: *err_order,
                        reason: reason.clone(),
                    },
                    None,
                )
            } else {
                inflight_count += 1;
                (OpStatus::InFlight, None)
            };

            let entry = LedgerEntry {
                op_id: op.op_id,
                writer_id: op.writer_id,
                seq_num: op.seq_num,
                key: op.key,
                op_type: op.op_type,
                payload: op.payload,
                batch_ops: op.batch_ops,
                t_sched,
                t_sub,
                dispatch_order: t_sched,
                ack_order,
                status: status.clone(),
            };

            // Index all mutations including secondary batch keys
            match &entry.op_type {
                OpType::Put => {
                    mutations_by_key
                        .entry(entry.key)
                        .or_default()
                        .push(KeyMutationRecord {
                            op_id: entry.op_id,
                            sub_op_idx: None,
                            writer_id: entry.writer_id,
                            seq_num: entry.seq_num,
                            key: entry.key,
                            t_sched,
                            t_sub,
                            ack_order,
                            status: status.clone(),
                            is_delete: false,
                            payload: entry.payload.clone(),
                        });
                }
                OpType::Del => {
                    mutations_by_key
                        .entry(entry.key)
                        .or_default()
                        .push(KeyMutationRecord {
                            op_id: entry.op_id,
                            sub_op_idx: None,
                            writer_id: entry.writer_id,
                            seq_num: entry.seq_num,
                            key: entry.key,
                            t_sched,
                            t_sub,
                            ack_order,
                            status: status.clone(),
                            is_delete: true,
                            payload: None,
                        });
                }
                OpType::Batch { .. } => {
                    for (idx, sub_op) in entry.batch_ops.iter().enumerate() {
                        match sub_op {
                            BatchSubOp::Put { key, payload } => {
                                mutations_by_key
                                    .entry(*key)
                                    .or_default()
                                    .push(KeyMutationRecord {
                                        op_id: entry.op_id,
                                        sub_op_idx: Some(idx),
                                        writer_id: entry.writer_id,
                                        seq_num: entry.seq_num,
                                        key: *key,
                                        t_sched,
                                        t_sub,
                                        ack_order,
                                        status: status.clone(),
                                        is_delete: false,
                                        payload: Some(payload.clone()),
                                    });
                            }
                            BatchSubOp::Del { key } => {
                                mutations_by_key
                                    .entry(*key)
                                    .or_default()
                                    .push(KeyMutationRecord {
                                        op_id: entry.op_id,
                                        sub_op_idx: Some(idx),
                                        writer_id: entry.writer_id,
                                        seq_num: entry.seq_num,
                                        key: *key,
                                        t_sched,
                                        t_sub,
                                        ack_order,
                                        status: status.clone(),
                                        is_delete: true,
                                        payload: None,
                                    });
                            }
                        }
                    }
                }
            }

            entries.push(entry);
        }

        Self {
            entries,
            mutations_by_key,
            acked_count,
            rejected_count,
            inflight_count,
        }
    }

    /// Return all keys touched by any operation (single op or batch sub-op).
    pub fn touched_keys(&self) -> HashSet<u64> {
        self.mutations_by_key.keys().cloned().collect()
    }

    /// Return all keys touched by an acknowledged operation (single op or batch sub-op).
    pub fn acked_keys(&self) -> HashSet<u64> {
        self.mutations_by_key
            .iter()
            .filter(|(_, muts)| {
                muts.iter()
                    .any(|m| matches!(m.status, OpStatus::Acknowledged { .. }))
            })
            .map(|(&k, _)| k)
            .collect()
    }

    /// Return all mutations recorded for a specific key.
    pub fn mutations_for_key(&self, key: u64) -> &[KeyMutationRecord] {
        self.mutations_by_key
            .get(&key)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Return all operations that were in-flight when the crash occurred.
    pub fn inflight_ops(&self) -> Vec<&LedgerEntry> {
        self.entries
            .iter()
            .filter(|e| matches!(e.status, OpStatus::InFlight))
            .collect()
    }

    /// Return all operations that received an explicit ACK.
    pub fn acked_ops(&self) -> Vec<&LedgerEntry> {
        self.entries
            .iter()
            .filter(|e| matches!(e.status, OpStatus::Acknowledged { .. }))
            .collect()
    }

    /// Return all operations that were explicitly rejected.
    pub fn rejected_ops(&self) -> Vec<&LedgerEntry> {
        self.entries
            .iter()
            .filter(|e| matches!(e.status, OpStatus::Rejected { .. }))
            .collect()
    }
}

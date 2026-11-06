//! Oracle Verifier: Enforces all 5 crash-safety invariants on recovered state.
//!
//! Uses a sound forward-search global linearization solver with exact canonical memoization,
//! multi-key batch atomicity, and conservative causal happens-before relations.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::generator::payload::{PayloadError, PayloadGenerator};
use crate::oracle::lattice::{InvariantViolation, StateLattice};
use crate::oracle::ledger::{LedgerEntry, OpStatus, OracleLedger};
use crate::runner::writers::{BatchSubOp, OpType};

/// Final verdict of the independent oracle on a recovered dump.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleVerdict {
    /// Recovered state satisfies all 5 crash-safety rules and belongs to the legal lattice
    Valid {
        keys_inspected: usize,
        acknowledged_ops: usize,
        inflight_ops: usize,
    },
    /// Inconclusive verdict: Search budget reached under highly concurrent ambiguous history
    Inconclusive {
        reason: String,
        component_keys: Vec<u64>,
    },
    /// Invariant violation detected
    Violation(InvariantViolation),
}

/// Explicit lifecycle decision state for in-flight operations during search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InflightDecision {
    Undecided,
    Included,
    Excluded,
}

/// Exact, collision-proof canonical memoization state.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CanonicalMemoState {
    /// Exact set of applied operation IDs in canonical sorted order.
    pub applied_ops: BTreeSet<u64>,
    /// Exact decision status for every in-flight operation in this component.
    pub inflight_decisions: BTreeMap<u64, InflightDecision>,
    /// Exact canonical key-value store (deep equality on keys and byte payloads).
    pub canonical_store: BTreeMap<u64, Vec<u8>>,
}

/// Cloned search node guaranteeing complete branch isolation.
#[derive(Clone)]
pub struct SearchNode {
    pub applied_ops: BTreeSet<u64>,
    pub inflight_decisions: BTreeMap<u64, InflightDecision>,
    pub current_store: BTreeMap<u64, Vec<u8>>,
    pub linearization: Vec<u64>,
}

/// Execution step budget to ensure solver terminates safely.
pub struct StepBudget {
    remaining: usize,
}

impl StepBudget {
    pub fn new(limit: usize) -> Self {
        Self { remaining: limit }
    }

    pub fn is_exhausted(&self) -> bool {
        self.remaining == 0
    }

    pub fn consume(&mut self) {
        if self.remaining > 0 {
            self.remaining -= 1;
        }
    }
}

/// Result of an individual component search.
pub enum SolverResult {
    Success(Vec<u64>),
    Failure,
    Inconclusive(String),
}

/// Central verifier that evaluates a normalized dump against the independent oracle.
pub struct OracleVerifier {
    run_seed: u64,
    lattice: StateLattice,
    ledger: OracleLedger,
    step_budget_limit: usize,
}

impl OracleVerifier {
    /// Initialize with the run seed and frozen ledger.
    pub fn new(run_seed: u64, ledger: OracleLedger) -> Self {
        let lattice = StateLattice::from_ledger(&ledger);
        Self {
            run_seed,
            lattice,
            ledger,
            step_budget_limit: 100_000,
        }
    }

    /// Set an explicit step budget limit for the solver.
    pub fn with_step_budget(mut self, limit: usize) -> Self {
        self.step_budget_limit = limit;
        self
    }

    /// Access the underlying lattice.
    pub fn lattice(&self) -> &StateLattice {
        &self.lattice
    }

    /// Access the underlying ledger.
    pub fn ledger(&self) -> &OracleLedger {
        &self.ledger
    }

    /// Validates the recovered dump against the independent state lattice and global solver.
    pub fn verify_dump(&self, dump: &BTreeMap<u64, Vec<u8>>) -> OracleVerdict {
        // 1. Rule 5 (Integrity & Payload Verification): Validate every value in the dump
        for (&key, val) in dump {
            if let Err(err) = PayloadGenerator::validate(val, Some(key), Some(self.run_seed)) {
                let violation = match err {
                    PayloadError::ChecksumMismatch { stored, computed } => {
                        InvariantViolation::CorruptedValue {
                            key,
                            error: format!(
                                "CRC32 mismatch (stored {:#010X}, computed {:#010X})",
                                stored, computed
                            ),
                        }
                    }
                    PayloadError::CorruptedBody { offset } => InvariantViolation::CorruptedValue {
                        key,
                        error: format!("Deterministic payload body corrupted at offset {}", offset),
                    },
                    PayloadError::SeedMismatch { expected, found } => {
                        InvariantViolation::ForeignValue {
                            key,
                            details: format!(
                                "Foreign run seed detected: expected {}, found {}",
                                expected, found
                            ),
                        }
                    }
                    PayloadError::KeyMismatch { expected, found } => {
                        InvariantViolation::ForeignValue {
                            key,
                            details: format!(
                                "Value stored under wrong key: expected {}, header says {}",
                                expected, found
                            ),
                        }
                    }
                    other => InvariantViolation::CorruptedValue {
                        key,
                        error: format!("{:?}", other),
                    },
                };
                return OracleVerdict::Violation(violation);
            }
        }

        // 2. Reject foreign keys never written by any operation
        let touched_keys = self.ledger.touched_keys();
        for &k in dump.keys() {
            if !touched_keys.contains(&k) {
                return OracleVerdict::Violation(InvariantViolation::ForeignValue {
                    key: k,
                    details: "Key present in dump was never touched by any operation in ledger"
                        .to_string(),
                });
            }
        }

        // 3. Decompose into truly independent components
        let components = self.build_independent_components();

        // 4. Solve each component with the forward-search global linearization solver
        for comp_indices in &components {
            let comp_entries: Vec<&LedgerEntry> = comp_indices
                .iter()
                .map(|&idx| &self.ledger.entries[idx])
                .collect();

            let comp_keys: HashSet<u64> = comp_entries
                .iter()
                .flat_map(|e| Self::keys_touched_by_entry(e))
                .collect();

            let comp_dump: BTreeMap<u64, Vec<u8>> = dump
                .iter()
                .filter(|(k, _)| comp_keys.contains(k))
                .map(|(k, v)| (*k, v.clone()))
                .collect();

            let mut budget = StepBudget::new(self.step_budget_limit);
            let mut memo = HashSet::new();

            let mut initial_decisions = BTreeMap::new();
            for &op in &comp_entries {
                if matches!(op.status, OpStatus::InFlight) {
                    initial_decisions.insert(op.op_id, InflightDecision::Undecided);
                }
            }

            let initial_node = SearchNode {
                applied_ops: BTreeSet::new(),
                inflight_decisions: initial_decisions,
                current_store: BTreeMap::new(),
                linearization: Vec::new(),
            };

            match Self::forward_search(
                initial_node,
                &comp_entries,
                &comp_dump,
                &mut memo,
                &mut budget,
            ) {
                SolverResult::Success(_) => {
                    // Component successfully explained by at least one global linearization
                }
                SolverResult::Inconclusive(reason) => {
                    return OracleVerdict::Inconclusive {
                        reason,
                        component_keys: comp_keys.into_iter().collect(),
                    };
                }
                SolverResult::Failure => {
                    // Diagnose exact violation in this component
                    let violation = self.diagnose_component_failure(&comp_entries, &comp_dump);
                    return OracleVerdict::Violation(violation);
                }
            }
        }

        let all_keys: HashSet<u64> = dump.keys().cloned().chain(touched_keys).collect();
        OracleVerdict::Valid {
            keys_inspected: all_keys.len(),
            acknowledged_ops: self.ledger.acked_count,
            inflight_ops: self.ledger.inflight_count,
        }
    }

    /// Helper to collect all distinct keys touched by an operation (Put, Del, or Batch sub-ops).
    pub fn keys_touched_by_entry(entry: &LedgerEntry) -> HashSet<u64> {
        let mut keys = HashSet::new();
        match &entry.op_type {
            OpType::Put | OpType::Del => {
                keys.insert(entry.key);
            }
            OpType::Batch { .. } => {
                for sub in &entry.batch_ops {
                    match sub {
                        BatchSubOp::Put { key, .. } => {
                            keys.insert(*key);
                        }
                        BatchSubOp::Del { key } => {
                            keys.insert(*key);
                        }
                    }
                }
            }
        }
        keys
    }

    /// Construct truly independent components where operations share zero keys AND zero causal edges.
    pub fn build_independent_components(&self) -> Vec<Vec<usize>> {
        let n = self.ledger.entries.len();
        if n == 0 {
            return Vec::new();
        }

        // Adjacency list
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
        let keys_per_entry: Vec<HashSet<u64>> = self
            .ledger
            .entries
            .iter()
            .map(Self::keys_touched_by_entry)
            .collect();

        for i in 0..n {
            for j in (i + 1)..n {
                let e1 = &self.ledger.entries[i];
                let e2 = &self.ledger.entries[j];

                let share_keys = keys_per_entry[i]
                    .intersection(&keys_per_entry[j])
                    .next()
                    .is_some();
                let causal_link = e1.happens_before(e2) || e2.happens_before(e1);

                if share_keys || causal_link {
                    adj[i].push(j);
                    adj[j].push(i);
                }
            }
        }

        // Connected components via BFS
        let mut visited = vec![false; n];
        let mut components = Vec::new();

        for i in 0..n {
            if !visited[i] {
                let mut comp = Vec::new();
                let mut queue = std::collections::VecDeque::new();
                queue.push_back(i);
                visited[i] = true;

                while let Some(curr) = queue.pop_front() {
                    comp.push(curr);
                    for &neighbor in &adj[curr] {
                        if !visited[neighbor] {
                            visited[neighbor] = true;
                            queue.push_back(neighbor);
                        }
                    }
                }

                components.push(comp);
            }
        }

        components
    }

    /// Forward-search global linearization solver on an independent component.
    fn forward_search(
        node: SearchNode,
        all_ops: &[&LedgerEntry],
        dump: &BTreeMap<u64, Vec<u8>>,
        memo: &mut HashSet<CanonicalMemoState>,
        budget: &mut StepBudget,
    ) -> SolverResult {
        if budget.is_exhausted() {
            return SolverResult::Inconclusive(
                "Search step budget exhausted under ambiguous concurrent history".to_string(),
            );
        }
        budget.consume();

        // 1. Exact Canonical Memoization Check
        let memo_key = CanonicalMemoState {
            applied_ops: node.applied_ops.clone(),
            inflight_decisions: node.inflight_decisions.clone(),
            canonical_store: node.current_store.clone(),
        };
        if !memo.insert(memo_key) {
            return SolverResult::Failure;
        }

        // 2. Check Completion Condition
        let required_active_ops: Vec<u64> = all_ops
            .iter()
            .filter(|op| match op.status {
                OpStatus::Acknowledged { .. } => true,
                OpStatus::InFlight => {
                    node.inflight_decisions.get(&op.op_id) == Some(&InflightDecision::Included)
                }
                OpStatus::Rejected { .. } => false,
            })
            .map(|op| op.op_id)
            .collect();

        let all_required_applied = required_active_ops
            .iter()
            .all(|op_id| node.applied_ops.contains(op_id));

        let no_undecided = !all_ops.iter().any(|op| {
            matches!(op.status, OpStatus::InFlight)
                && node.inflight_decisions.get(&op.op_id) == Some(&InflightDecision::Undecided)
        });

        if all_required_applied && no_undecided {
            if node.current_store == *dump {
                return SolverResult::Success(node.linearization);
            } else {
                return SolverResult::Failure;
            }
        }

        // 3. Find Eligible Next Operations
        let mut eligible_ops = Vec::new();
        for &op in all_ops {
            if node.applied_ops.contains(&op.op_id) {
                continue;
            }
            if matches!(op.status, OpStatus::Rejected { .. }) {
                continue;
            }
            if matches!(op.status, OpStatus::InFlight)
                && node.inflight_decisions.get(&op.op_id) == Some(&InflightDecision::Excluded)
            {
                continue;
            }

            // Check if all causal predecessors that must execute have already executed
            let predecessors_satisfied = all_ops.iter().filter(|p| p.happens_before(op)).all(|p| {
                match p.status {
                    OpStatus::Acknowledged { .. } => node.applied_ops.contains(&p.op_id),
                    OpStatus::InFlight => {
                        match node.inflight_decisions.get(&p.op_id) {
                            Some(&InflightDecision::Included) => {
                                node.applied_ops.contains(&p.op_id)
                            }
                            Some(&InflightDecision::Excluded) => true,
                            _ => false, // Undecided: cannot proceed past undecided predecessor
                        }
                    }
                    OpStatus::Rejected { .. } => true,
                }
            });

            if predecessors_satisfied {
                eligible_ops.push(op);
            }
        }

        // 4. Branch on Undecided In-Flight Operations if no eligible op can progress
        if eligible_ops.is_empty() {
            let undecided_op = all_ops.iter().find(|op| {
                matches!(op.status, OpStatus::InFlight)
                    && node.inflight_decisions.get(&op.op_id) == Some(&InflightDecision::Undecided)
            });

            if let Some(u) = undecided_op {
                // Branch A: Exclude in-flight op (lost in crash)
                let mut branch_exclude = node.clone();
                branch_exclude
                    .inflight_decisions
                    .insert(u.op_id, InflightDecision::Excluded);
                match Self::forward_search(branch_exclude, all_ops, dump, memo, budget) {
                    SolverResult::Success(lin) => return SolverResult::Success(lin),
                    SolverResult::Inconclusive(reason) => {
                        return SolverResult::Inconclusive(reason);
                    }
                    SolverResult::Failure => {}
                }

                // Branch B: Include in-flight op (persisted before crash)
                let mut branch_include = node.clone();
                branch_include
                    .inflight_decisions
                    .insert(u.op_id, InflightDecision::Included);
                match Self::forward_search(branch_include, all_ops, dump, memo, budget) {
                    SolverResult::Success(lin) => return SolverResult::Success(lin),
                    SolverResult::Inconclusive(reason) => {
                        return SolverResult::Inconclusive(reason);
                    }
                    SolverResult::Failure => {}
                }

                return SolverResult::Failure;
            }
        }

        // 5. Branch Over Eligible Operations (Cloned Search State Per Branch)
        for next_op in eligible_ops {
            // If next_op is in-flight and undecided, explore excluding it first
            if matches!(next_op.status, OpStatus::InFlight)
                && node.inflight_decisions.get(&next_op.op_id) == Some(&InflightDecision::Undecided)
            {
                let mut branch_exclude = node.clone();
                branch_exclude
                    .inflight_decisions
                    .insert(next_op.op_id, InflightDecision::Excluded);
                match Self::forward_search(branch_exclude, all_ops, dump, memo, budget) {
                    SolverResult::Success(lin) => return SolverResult::Success(lin),
                    SolverResult::Inconclusive(reason) => {
                        return SolverResult::Inconclusive(reason);
                    }
                    SolverResult::Failure => {}
                }
            }

            let mut child_node = node.clone();
            if matches!(next_op.status, OpStatus::InFlight) {
                child_node
                    .inflight_decisions
                    .insert(next_op.op_id, InflightDecision::Included);
            }

            // Apply operation atomically (preserving intra-batch duplicate key sequence)
            Self::apply_operation_atomically(&mut child_node.current_store, next_op);
            child_node.applied_ops.insert(next_op.op_id);
            child_node.linearization.push(next_op.op_id);

            match Self::forward_search(child_node, all_ops, dump, memo, budget) {
                SolverResult::Success(lin) => return SolverResult::Success(lin),
                SolverResult::Inconclusive(reason) => return SolverResult::Inconclusive(reason),
                SolverResult::Failure => {}
            }
        }

        SolverResult::Failure
    }

    /// Atomically applies an operation to the store.
    ///
    /// Preserves the original sub-operation sequence and handles intra-batch duplicate keys.
    pub fn apply_operation_atomically(store: &mut BTreeMap<u64, Vec<u8>>, op: &LedgerEntry) {
        match &op.op_type {
            OpType::Put => {
                if let Some(payload) = &op.payload {
                    store.insert(op.key, payload.clone());
                }
            }
            OpType::Del => {
                store.remove(&op.key);
            }
            OpType::Batch { .. } => {
                for sub_op in &op.batch_ops {
                    match sub_op {
                        BatchSubOp::Put { key, payload } => {
                            store.insert(*key, payload.clone());
                        }
                        BatchSubOp::Del { key } => {
                            store.remove(key);
                        }
                    }
                }
            }
        }
    }

    /// Diagnoses why a component failed to linearize, pinpointing the exact invariant violation.
    fn diagnose_component_failure(
        &self,
        comp_entries: &[&LedgerEntry],
        comp_dump: &BTreeMap<u64, Vec<u8>>,
    ) -> InvariantViolation {
        // 1. Rule 3: Rejected operation effect visible
        for &op in comp_entries {
            if let OpStatus::Rejected { reason, .. } = &op.status {
                match &op.op_type {
                    OpType::Put => {
                        if comp_dump.get(&op.key) == op.payload.as_ref() {
                            return InvariantViolation::RejectedOpVisible {
                                op_id: op.op_id,
                                key: op.key,
                                reason: reason.clone(),
                            };
                        }
                    }
                    OpType::Batch { .. } => {
                        for sub in &op.batch_ops {
                            if let BatchSubOp::Put { key, payload } = sub {
                                if comp_dump.get(key) == Some(payload) {
                                    return InvariantViolation::RejectedOpVisible {
                                        op_id: op.op_id,
                                        key: *key,
                                        reason: reason.clone(),
                                    };
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // 2. Rule 4: Partial Batch violation (including secondary keys)
        for &op in comp_entries {
            if matches!(op.op_type, OpType::Batch { .. }) {
                // Compute net effect of this batch per key (preserving duplicate keys in batch)
                let mut net_effects: BTreeMap<u64, Option<&Vec<u8>>> = BTreeMap::new();
                for sub in &op.batch_ops {
                    match sub {
                        BatchSubOp::Put { key, payload } => {
                            net_effects.insert(*key, Some(payload));
                        }
                        BatchSubOp::Del { key } => {
                            net_effects.insert(*key, None);
                        }
                    }
                }

                let mut applied = Vec::new();
                let mut unapplied = Vec::new();

                for (&key, expected_val) in &net_effects {
                    let actual_val = comp_dump.get(&key);
                    if actual_val == *expected_val {
                        applied.push(key);
                    } else {
                        // Check if key was legitimately superseded by an acknowledged later operation
                        let superseded = self.ledger.mutations_for_key(key).iter().any(|m| {
                            matches!(m.status, OpStatus::Acknowledged { .. })
                                && m.op_id != op.op_id
                                && op.happens_before_mutation(m)
                        });

                        if !superseded {
                            unapplied.push(key);
                        }
                    }
                }

                if !applied.is_empty() && !unapplied.is_empty() {
                    return InvariantViolation::PartialBatch {
                        batch_id: op.op_id,
                        applied_keys: applied,
                        unapplied_keys: unapplied,
                    };
                }
            }
        }

        // 3. Rule 1: Lost Acknowledged Write (including secondary keys inside batches)
        for &op in comp_entries {
            if matches!(op.status, OpStatus::Acknowledged { .. }) {
                match &op.op_type {
                    OpType::Put => {
                        let key = op.key;
                        if !comp_dump.contains_key(&key) {
                            let has_subsequent_delete =
                                self.ledger.mutations_for_key(key).iter().any(|m| {
                                    m.is_delete
                                        && m.op_id != op.op_id
                                        && matches!(m.status, OpStatus::Acknowledged { .. })
                                        && op.happens_before_mutation(m)
                                });
                            if !has_subsequent_delete {
                                return InvariantViolation::LostAckWrite {
                                    op_id: op.op_id,
                                    key,
                                    details: format!(
                                        "Acknowledged write (op {}) on key {} missing from dump",
                                        op.op_id, key
                                    ),
                                };
                            }
                        }
                    }
                    OpType::Batch { .. } => {
                        for sub in &op.batch_ops {
                            if let BatchSubOp::Put { key, .. } = sub {
                                if !comp_dump.contains_key(key) {
                                    let has_subsequent_delete =
                                        self.ledger.mutations_for_key(*key).iter().any(|m| {
                                            m.is_delete
                                                && m.op_id != op.op_id
                                                && matches!(m.status, OpStatus::Acknowledged { .. })
                                                && op.happens_before_mutation(m)
                                        });
                                    if !has_subsequent_delete {
                                        return InvariantViolation::LostAckWrite {
                                            op_id: op.op_id,
                                            key: *key,
                                            details: format!(
                                                "Acknowledged batch write (op {}) on secondary key {} missing from dump",
                                                op.op_id, key
                                            ),
                                        };
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        // 4. Rule 1/3: Resurrected Delete
        for &key in comp_dump.keys() {
            let muts = self.ledger.mutations_for_key(key);
            if let Some(last_acked) = muts
                .iter()
                .filter(|m| matches!(m.status, OpStatus::Acknowledged { .. }))
                .last()
            {
                if last_acked.is_delete {
                    let has_later_put = muts
                        .iter()
                        .any(|m| !m.is_delete && last_acked.happens_before(m));
                    if !has_later_put {
                        return InvariantViolation::ResurrectedDelete {
                            op_id: last_acked.op_id,
                            key,
                        };
                    }
                }
            }
        }

        // Default: Foreign or broken state
        InvariantViolation::ForeignValue {
            key: comp_dump.keys().next().copied().unwrap_or(0),
            details: "Recovered state cannot be produced by any legal global linearization of active operations".to_string(),
        }
    }
}

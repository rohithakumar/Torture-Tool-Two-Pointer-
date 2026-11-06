# Architecture & Correctness Specification: Crash Torture Testing Tool

This document outlines the architectural design, formal concurrency model, invariant verification engine, and safety guarantees of Person 3's independent crash-safety torture tool.

---

## 1. Event Ordering & Happens-Before Semantics

The torture tool evaluates database durability and linearizability strictly from observed process I/O boundaries. It never relies on internal engine clocks or assumed commit protocols.

### Interval Event Clock
Command dispatch across OS pipes is an interval event rather than an instantaneous point:
- **$t_{\text{sched}}(O)$**: Assigned immediately after a writer thread acquires the exclusive `stdin_writer` mutex, strictly *before* writing command bytes into the OS pipe.
- **$t_{\text{sub}}(O)$**: Assigned immediately after `stdin.flush()` returns. This timestamp is retained strictly for diagnostic telemetry and logging. It is *not* used to assert happens-before because child processing and ACK observation by the drainer can interleave before writer thread rescheduling.
- **$t_{\text{ack}}(O)$**: Assigned by the continuous background `OutputDrainer` thread immediately upon reading a complete, `\n`-delimited `ACK <op_id>` line from `stdout`.

### Conservative Happens-Before ($\prec$)
The causal partial order $(\mathcal{O}, \prec)$ over operations is defined strictly:

1. **Intra-Writer Program Order**:
   Because each writer thread maintains at most one outstanding operation and increments its sequence number monotonically:
   $$\text{writer}(A) = \text{writer}(B) \quad \land \quad \text{seq}(A) < \text{seq}(B) \implies A \prec B$$

2. **Cross-Writer Conservative Durability Boundary**:
   For operations $A$ and $B$ dispatched by different writers:
   $$A \prec B \iff t_{\text{ack}}(A) < t_{\text{sched}}(B)$$
   **Soundness Guarantee**: Operation $A$ was confirmed durable on `stdout` *before* writer $B$ even began acquiring the `stdin` mutex to schedule command $B$.

3. **Ambiguous / Overlapping Events ($A \parallel B$)**:
   If $t_{\text{ack}}(A) \ge t_{\text{sched}}(B)$, or if $A$ has no recorded ACK tick:
   $$A \not\prec B \quad \land \quad B \not\prec A \implies A \parallel B$$
   Overlapping operations are treated as **strictly concurrent**. The oracle never synthesizes artificial ACK timestamps and never manufactures unproven happens-before relationships.

---

## 2. Flattened Batch Mutation Indexing

To prevent missed invariant violations on secondary keys inside atomic batches, `OracleLedger` maintains a flattened index `mutations_by_key: HashMap<u64, Vec<KeyMutationRecord>>`:

- Every single `Put` is indexed under its target key.
- Every single `Del` is indexed under its target key.
- Every sub-operation inside an atomic `Batch` is indexed under its respective key with its sub-operation index (`sub_op_idx`), preserving intra-batch ordering.
- Duplicate keys within batches are fully preserved in sequential index order.
- Missing secondary batch keys are recognized as potential `LostAckWrite` violations rather than misclassified as `ForeignValue`.

---

## 3. Forward-Search Global Linearization Solver

Instead of naive per-key heuristic checks that can accept mutually incompatible executions (e.g., torn multi-key states like the Apple/Mango counterexample), the verifier implements a forward-search global linearization solver.

### Linearization Invariants
A recovered database dump $D$ is verified by discovering a valid topological sort (linearization) $L = (o_1, o_2, \dots, o_k)$ of an active operation subset $\mathcal{S} \subseteq \mathcal{O}$ satisfying:
1. **Durability**: All acknowledged operations are included ($\mathcal{O}_{\text{ACK}} \subseteq \mathcal{S}$).
2. **Rejection**: No explicitly rejected operation is included ($\mathcal{O}_{\text{ERR}} \cap \mathcal{S} = \emptyset$).
3. **Atomic Batches**: Every batch is all-or-nothing: either all sub-operations execute atomically in intra-batch sequential order, or none execute.
4. **Causal Ordering**: For all $A, B \in \mathcal{S}$, if $A \prec B$, then $A$ appears before $B$ in $L$.
5. **State Equivalence**: Sequentially applying $L$ from an empty initial store reconstructs the exact dump $D$.

### Search Node & Branch Isolation
To prevent cross-branch mutation, every recursive branch receives an owned clone of `SearchNode`:
```rust
#[derive(Clone)]
pub struct SearchNode {
    pub applied_ops: BTreeSet<u64>,
    pub inflight_decisions: BTreeMap<u64, InflightDecision>,
    pub current_store: BTreeMap<u64, Vec<u8>>,
    pub linearization: Vec<u64>,
}
```

### Exact Canonical Memoization
Memoization collapses redundant paths reaching identical states. To prevent false collisions:
```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CanonicalMemoState {
    pub applied_ops: BTreeSet<u64>,
    pub inflight_decisions: BTreeMap<u64, InflightDecision>,
    pub canonical_store: BTreeMap<u64, Vec<u8>>,
}
```
Equality compares:
- Canonical sorted set of applied operations.
- Exact ternary decision vector for in-flight operations (`Undecided`, `Included`, `Excluded`).
- Exact key-value store (deep comparison on all keys and byte payloads).

### Sound Component Decomposition
The operation set is decomposed into independent components:
- Graph $G = (\mathcal{O}, E)$ connects operations that share keys OR have causal happens-before edges ($\prec$).
- Connected components $\{C_1, \dots, C_p\}$ partition operations into disjoint key sets with zero cross-component causal edges.
- **Safety Theorem**: Because operations in different components share zero keys and zero happens-before edges, independent component linearizations concatenate into a globally valid topological sort.

### Step Budget & Inconclusive Verdict
To protect against exponential exploration under highly concurrent ambiguous histories:
- The solver enforces an explicit step budget limit (default: 100,000 steps).
- If the budget is exhausted without finding a valid linearization or proving a violation, the verifier emits:
  `OracleVerdict::Inconclusive { reason, component_keys }`.
- An inconclusive outcome is never reported as a database failure, preserving invariant integrity.

---

## 4. Initial Database State Guarantee

The oracle derives its expected state assuming an initially empty database.
- `src/runner/recovery.rs` implements `ensure_clean_db_path(db_path: &Path) -> std::io::Result<()>`.
- Before Step 1 (`serve` child process spawn), `ensure_clean_db_path` wipes existing files or directories at `db_path`.
- This ensures that pre-existing leftover data from previous test runs cannot contaminate recovery verification.

---

## 5. Scope & Boundary Disclaimers

- **Target Engine Independence**: The torture tool connects to child processes via the clean `DatabaseEngineAdapter` trait. No real database engine code, protocol assumptions, or storage implementations are embedded in this tool.
- **Mock Engines**: In-memory and child mock engines are test fixtures strictly used to test and verify the torture tool itself. They do not claim compatibility with any real database engine.
- **Performance**: Theoretical worst-case complexity is exponential in the number of concurrent in-flight operations ($O(2^m \cdot e(P) \cdot k)$). Pruning and memoization optimize common execution paths, bounded by the explicit search budget.


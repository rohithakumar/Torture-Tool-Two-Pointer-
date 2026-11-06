//! Phase 3.1 Hardening & Regression Verification Suite.
//!
//! Validates:
//! - T1: Event ordering and conservative happens-before tracking (no synthetic timestamps)
//! - T2: Batch supersession by later acknowledged operations
//! - T3: Global atomicity and rejection of mixed/torn multi-key state (Apple/Mango)
//! - T4: Intra-batch duplicate keys with sequential overwrites
//! - T5: Missing secondary keys in acknowledged batches detected as LostAckWrite
//! - T6: Atomic batches containing deletes and mixed operations
//! - T7: In-flight operations (PUT, DELETE, BATCH) all-or-nothing
//! - T8: Solver branch isolation (cloned SearchNodes)
//! - T9: Exact canonical memoization (different in-flight decisions & store equality)
//! - T10: Initial database state cleanliness guarantee

use std::collections::BTreeMap;

use torture::generator::payload::PayloadGenerator;
use torture::oracle::lattice::InvariantViolation;
use torture::oracle::ledger::{LedgerEntry, OpStatus, OracleLedger};
use torture::oracle::verifier::{
    CanonicalMemoState, InflightDecision, OracleVerdict, OracleVerifier,
};
use torture::runner::recovery::ensure_clean_db_path;
use torture::runner::writers::{BatchSubOp, OpType, SentOpRecord};

const TEST_SEED: u64 = 0xABCD_EF01_2345;

fn test_payload(key: u64, seq_num: u64) -> Vec<u8> {
    let generator = PayloadGenerator::new(TEST_SEED);
    generator.generate(0, seq_num, key, 48)
}

// ---------------------------------------------------------------------------
// T1: Event Ordering & Conservative Happens-Before Tracking
// ---------------------------------------------------------------------------

#[test]
fn test_t1_event_ordering_establishes_happens_before_only_when_proven() {
    // Op 1 from Writer 0: scheduled at tick 10, ACKed at tick 20
    let e1 = LedgerEntry {
        op_id: 1,
        writer_id: 0,
        seq_num: 1,
        key: 10,
        op_type: OpType::Put,
        payload: Some(test_payload(10, 1)),
        batch_ops: Vec::new(),
        t_sched: 10,
        t_sub: 12,
        dispatch_order: 10,
        ack_order: Some(20),
        status: OpStatus::Acknowledged { ack_order: 20 },
    };

    // Op 2 from Writer 1: scheduled at tick 25 (strictly after e1 was ACKed)
    let e2 = LedgerEntry {
        op_id: 2,
        writer_id: 1,
        seq_num: 1,
        key: 10,
        op_type: OpType::Put,
        payload: Some(test_payload(10, 2)),
        batch_ops: Vec::new(),
        t_sched: 25,
        t_sub: 27,
        dispatch_order: 25,
        ack_order: Some(30),
        status: OpStatus::Acknowledged { ack_order: 30 },
    };

    // e1 ACK (20) < e2 t_sched (25) -> e1 ≺ e2 is strictly established!
    assert!(
        e1.happens_before(&e2),
        "ACK before schedule must establish happens-before"
    );
    assert!(!e2.happens_before(&e1), "Causality must be asymmetric");
}

#[test]
fn test_t1_ambiguous_ordering_remains_strictly_concurrent() {
    // Op 1 from Writer 0: scheduled at tick 10, ACKed at tick 30
    let e1 = LedgerEntry {
        op_id: 1,
        writer_id: 0,
        seq_num: 1,
        key: 10,
        op_type: OpType::Put,
        payload: Some(test_payload(10, 1)),
        batch_ops: Vec::new(),
        t_sched: 10,
        t_sub: 12,
        dispatch_order: 10,
        ack_order: Some(30),
        status: OpStatus::Acknowledged { ack_order: 30 },
    };

    // Op 2 from Writer 1: scheduled at tick 20 (before e1's ACK at tick 30 arrived!)
    let e2 = LedgerEntry {
        op_id: 2,
        writer_id: 1,
        seq_num: 1,
        key: 10,
        op_type: OpType::Put,
        payload: Some(test_payload(10, 2)),
        batch_ops: Vec::new(),
        t_sched: 20,
        t_sub: 22,
        dispatch_order: 20,
        ack_order: Some(35),
        status: OpStatus::Acknowledged { ack_order: 35 },
    };

    // Overlapping intervals: neither precedes the other
    assert!(
        !e1.happens_before(&e2),
        "Overlapping dispatch must NOT establish happens-before"
    );
    assert!(
        !e2.happens_before(&e1),
        "Neither operation can happen-before the other"
    );
}

#[test]
fn test_t1_no_synthetic_ack_timestamps() {
    let sent_ops = vec![SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 100,
        key: 10,
        op_type: OpType::Put,
        payload_len: 48,
        payload_crc: 0,
        payload: Some(test_payload(10, 1)),
        batch_ops: Vec::new(),
        dispatch_order: 10,
        t_sched: 10,
        t_sub: 12,
    }];

    // Build with NO ack ticks
    let ledger = OracleLedger::build(sent_ops, &[100], &[]);
    let entry = &ledger.entries[0];

    // ack_order must be None when no explicit tick was observed
    assert_eq!(
        entry.ack_order, None,
        "Ledger must NEVER synthesize artificial ACK timestamps"
    );
}

// ---------------------------------------------------------------------------
// T2: Batch Supersession by Later Acknowledged Operations
// ---------------------------------------------------------------------------

#[test]
fn test_t2_later_acknowledged_put_supersedes_batch_key() {
    let p_batch_k1 = test_payload(101, 10);
    let p_batch_k2 = test_payload(102, 10);
    let p_later_k2 = test_payload(102, 20);

    // Batch 1 on {101, 102} ACKed at tick 10
    let sent_batch = SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 1,
        key: 101,
        op_type: OpType::Batch { count: 2 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: vec![
            BatchSubOp::Put {
                key: 101,
                payload: p_batch_k1.clone(),
            },
            BatchSubOp::Put {
                key: 102,
                payload: p_batch_k2.clone(),
            },
        ],
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 2,
    };

    // Later Op 2 from Writer 1: scheduled at tick 15 (after Batch 1 ACK at 10)
    let sent_put = SentOpRecord {
        writer_id: 1,
        seq_num: 1,
        op_id: 2,
        key: 102,
        op_type: OpType::Put,
        payload_len: 48,
        payload_crc: 0,
        payload: Some(p_later_k2.clone()),
        batch_ops: Vec::new(),
        dispatch_order: 15,
        t_sched: 15,
        t_sub: 16,
    };

    let ledger = OracleLedger::build_with_ticks(
        vec![sent_batch, sent_put],
        &[1, 2],
        &[(1, 10), (2, 25)],
        &[],
    );
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Dump has Batch 1's value for 101, and Op 2's value for 102
    let mut dump = BTreeMap::new();
    dump.insert(101, p_batch_k1);
    dump.insert(102, p_later_k2);

    let verdict = verifier.verify_dump(&dump);
    assert!(
        matches!(verdict, OracleVerdict::Valid { .. }),
        "Later acknowledged PUT on K2 must supersede Batch's K2 without PartialBatch error: {:?}",
        verdict
    );
}

#[test]
fn test_t2_later_acknowledged_delete_supersedes_batch_key() {
    let p_batch_k1 = test_payload(201, 10);
    let p_batch_k2 = test_payload(202, 10);

    let sent_batch = SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 1,
        key: 201,
        op_type: OpType::Batch { count: 2 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: vec![
            BatchSubOp::Put {
                key: 201,
                payload: p_batch_k1.clone(),
            },
            BatchSubOp::Put {
                key: 202,
                payload: p_batch_k2.clone(),
            },
        ],
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 2,
    };

    let sent_del = SentOpRecord {
        writer_id: 1,
        seq_num: 1,
        op_id: 2,
        key: 202,
        op_type: OpType::Del,
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: Vec::new(),
        dispatch_order: 15,
        t_sched: 15,
        t_sub: 16,
    };

    let ledger = OracleLedger::build_with_ticks(
        vec![sent_batch, sent_del],
        &[1, 2],
        &[(1, 10), (2, 25)],
        &[],
    );
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Dump has K2 deleted, K1 present
    let mut dump = BTreeMap::new();
    dump.insert(201, p_batch_k1);

    let verdict = verifier.verify_dump(&dump);
    assert!(
        matches!(verdict, OracleVerdict::Valid { .. }),
        "Later acknowledged DELETE on K2 must legally leave K2 absent: {:?}",
        verdict
    );
}

#[test]
fn test_t2_concurrent_batch_and_operation_not_causally_superseded() {
    let p_batch_k1 = test_payload(301, 10);
    let p_batch_k2 = test_payload(302, 10);
    let p_op_b_k2 = test_payload(302, 20);

    // 1. Batch A is submitted at t_sched = 1 by Writer 0
    let batch_a = SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 1,
        key: 301,
        op_type: OpType::Batch { count: 2 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: vec![
            BatchSubOp::Put {
                key: 301,
                payload: p_batch_k1.clone(),
            },
            BatchSubOp::Put {
                key: 302,
                payload: p_batch_k2.clone(),
            },
        ],
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 2,
    };

    // 2. Operation B is submitted at t_sched = 5 by Writer 1 (BEFORE A's ACK at tick 20 is observed!)
    let op_b = SentOpRecord {
        writer_id: 1,
        seq_num: 1,
        op_id: 2,
        key: 302,
        op_type: OpType::Put,
        payload_len: 48,
        payload_crc: 0,
        payload: Some(p_op_b_k2.clone()),
        batch_ops: Vec::new(),
        dispatch_order: 5,
        t_sched: 5,
        t_sub: 6,
    };

    // 3. B receives ACK at tick 10
    // 4. A receives ACK later at tick 20
    let ledger = OracleLedger::build_with_ticks(
        vec![batch_a, op_b],
        &[1, 2],
        &[(2, 10), (1, 20)], // B ACKed at 10, A ACKed at 20
        &[],
    );

    // Causal condition verification:
    // A was ACKed at tick 20, but B was submitted at t_sched = 5.
    // batch ACK (20) < later op submission (5) is FALSE!
    let entry_a = &ledger.entries[0];
    let entry_b = &ledger.entries[1];
    assert!(
        !entry_a.happens_before(entry_b),
        "Batch A does NOT happen-before Op B because B was submitted before A was ACKed"
    );
    assert!(
        !entry_b.happens_before(entry_a),
        "Op B does NOT happen-before Batch A because A was submitted before B was ACKed"
    );

    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // 5. Verify that B is NOT automatically treated as causally superseding A.
    // Case A: Recovered dump shows torn state: Key 301 is applied, but Key 302 is missing!
    // If B were falsely treated as superseding A, Key 302's absence would be excused.
    // Under causal verification, this MUST be detected as PartialBatch!
    let mut torn_dump = BTreeMap::new();
    torn_dump.insert(301, p_batch_k1.clone());

    let verdict_torn = verifier.verify_dump(&torn_dump);
    assert!(
        matches!(
            verdict_torn,
            OracleVerdict::Violation(InvariantViolation::PartialBatch { batch_id: 1, .. })
        ),
        "Expected PartialBatch violation when Batch A is torn; B cannot excuse A's missing key: {:?}",
        verdict_torn
    );

    // Case B: Recovered dump shows only B's effect, completely missing acknowledged Batch A's key 301!
    let mut b_only_dump = BTreeMap::new();
    b_only_dump.insert(302, p_op_b_k2.clone());

    let verdict_b_only = verifier.verify_dump(&b_only_dump);
    assert!(
        matches!(
            verdict_b_only,
            OracleVerdict::Violation(InvariantViolation::LostAckWrite {
                op_id: 1,
                key: 301,
                ..
            })
        ),
        "Expected LostAckWrite violation when Batch A's key 301 is lost: {:?}",
        verdict_b_only
    );
}

#[test]
fn test_t2_sequential_acked_batch_then_op_establishes_causal_supersession() {
    let p_batch_k1 = test_payload(401, 10);
    let p_batch_k2 = test_payload(402, 10);
    let p_op_b_k2 = test_payload(402, 20);

    // Batch A on {401, 402} submitted at tick 1, complete ACK observed at tick 10
    let batch_a = SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 1,
        key: 401,
        op_type: OpType::Batch { count: 2 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: vec![
            BatchSubOp::Put {
                key: 401,
                payload: p_batch_k1.clone(),
            },
            BatchSubOp::Put {
                key: 402,
                payload: p_batch_k2.clone(),
            },
        ],
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 2,
    };

    // Op B on 402 submitted at tick 15 (STRICTLY AFTER Batch A's ACK at tick 10 was observed!)
    let op_b = SentOpRecord {
        writer_id: 1,
        seq_num: 1,
        op_id: 2,
        key: 402,
        op_type: OpType::Put,
        payload_len: 48,
        payload_crc: 0,
        payload: Some(p_op_b_k2.clone()),
        batch_ops: Vec::new(),
        dispatch_order: 15,
        t_sched: 15,
        t_sub: 16,
    };

    // Both operations completely ACKed
    let ledger = OracleLedger::build_with_ticks(
        vec![batch_a, op_b],
        &[1, 2],
        &[(1, 10), (2, 25)], // A ACKed at 10, B ACKed at 25
        &[],
    );

    // batch ACK (10) < later op submission (15) is TRUE!
    let entry_a = &ledger.entries[0];
    let entry_b = &ledger.entries[1];
    assert!(
        entry_a.happens_before(entry_b),
        "Batch A MUST happen-before Op B: A ACK (10) < B t_sched (15)"
    );

    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Dump shows Batch A's Key 401 and Op B's superseding Key 402
    let mut dump = BTreeMap::new();
    dump.insert(401, p_batch_k1);
    dump.insert(402, p_op_b_k2);

    let verdict = verifier.verify_dump(&dump);
    assert!(
        matches!(verdict, OracleVerdict::Valid { .. }),
        "Valid causal supersession must be accepted as Valid: {:?}",
        verdict
    );
}

// ---------------------------------------------------------------------------
// T3: Global Atomicity & Rejection of Mixed States (Apple/Mango)
// ---------------------------------------------------------------------------

#[test]
fn test_t3_concurrent_batches_apple_mango_rejected() {
    let apple_k1 = test_payload(1, 10);
    let apple_k2 = test_payload(2, 10);
    let mango_k1 = test_payload(1, 20);
    let mango_k2 = test_payload(2, 20);

    // Batch A: K1 = Apple, K2 = Apple (ACKed at tick 10)
    let batch_a = SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 10,
        key: 1,
        op_type: OpType::Batch { count: 2 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: vec![
            BatchSubOp::Put {
                key: 1,
                payload: apple_k1.clone(),
            },
            BatchSubOp::Put {
                key: 2,
                payload: apple_k2.clone(),
            },
        ],
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 2,
    };

    // Batch B: K1 = Mango, K2 = Mango (ACKed at tick 12, concurrent with A)
    let batch_b = SentOpRecord {
        writer_id: 1,
        seq_num: 1,
        op_id: 20,
        key: 1,
        op_type: OpType::Batch { count: 2 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: vec![
            BatchSubOp::Put {
                key: 1,
                payload: mango_k1.clone(),
            },
            BatchSubOp::Put {
                key: 2,
                payload: mango_k2.clone(),
            },
        ],
        dispatch_order: 3,
        t_sched: 3,
        t_sub: 4,
    };

    let ledger = OracleLedger::build_with_ticks(
        vec![batch_a, batch_b],
        &[10, 20],
        &[(10, 10), (20, 12)],
        &[],
    );
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Legal state 1: Apple/Apple (B then A)
    let mut dump_apple = BTreeMap::new();
    dump_apple.insert(1, apple_k1.clone());
    dump_apple.insert(2, apple_k2.clone());
    assert!(
        matches!(
            verifier.verify_dump(&dump_apple),
            OracleVerdict::Valid { .. }
        ),
        "Apple/Apple is a legal global linearization"
    );

    // Legal state 2: Mango/Mango (A then B)
    let mut dump_mango = BTreeMap::new();
    dump_mango.insert(1, mango_k1.clone());
    dump_mango.insert(2, mango_k2.clone());
    assert!(
        matches!(
            verifier.verify_dump(&dump_mango),
            OracleVerdict::Valid { .. }
        ),
        "Mango/Mango is a legal global linearization"
    );

    // ILLEGAL STATE: K1 = Apple, K2 = Mango (Torn cross-key batch state!)
    let mut dump_mixed = BTreeMap::new();
    dump_mixed.insert(1, apple_k1);
    dump_mixed.insert(2, mango_k2);

    let verdict = verifier.verify_dump(&dump_mixed);
    assert!(
        matches!(verdict, OracleVerdict::Violation(_)),
        "Solver MUST reject mixed Apple/Mango state because no global linearization can produce it: got {:?}",
        verdict
    );
}

// ---------------------------------------------------------------------------
// T4: Duplicate Keys Within Batches
// ---------------------------------------------------------------------------

#[test]
fn test_t4_intra_batch_duplicate_keys_sequential_overwrite() {
    let apple = test_payload(50, 10);
    let mango = test_payload(50, 20);
    let orange = test_payload(60, 30);

    // Batch: PUT K50=Apple, PUT K50=Mango, PUT K60=Orange
    let batch = SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 500,
        key: 50,
        op_type: OpType::Batch { count: 3 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: vec![
            BatchSubOp::Put {
                key: 50,
                payload: apple.clone(),
            },
            BatchSubOp::Put {
                key: 50,
                payload: mango.clone(),
            },
            BatchSubOp::Put {
                key: 60,
                payload: orange.clone(),
            },
        ],
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 2,
    };

    let ledger = OracleLedger::build(vec![batch], &[500], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Applied dump must have K50=Mango (overwritten) and K60=Orange
    let mut valid_dump = BTreeMap::new();
    valid_dump.insert(50, mango);
    valid_dump.insert(60, orange.clone());

    assert!(
        matches!(
            verifier.verify_dump(&valid_dump),
            OracleVerdict::Valid { .. }
        ),
        "Final net effect of batch on K50 must be Mango"
    );

    // Stale intra-batch state K50=Apple must be rejected!
    let mut stale_dump = BTreeMap::new();
    stale_dump.insert(50, apple);
    stale_dump.insert(60, orange);

    let verdict = verifier.verify_dump(&stale_dump);
    assert!(
        matches!(verdict, OracleVerdict::Violation(_)),
        "Intermediate intra-batch state Apple must be rejected: got {:?}",
        verdict
    );
}

// ---------------------------------------------------------------------------
// T5: Secondary Batch Keys Missing Detected as LostAckWrite
// ---------------------------------------------------------------------------

#[test]
fn test_t5_missing_secondary_batch_key_detected_as_lost_ack_write() {
    let p_k1 = test_payload(11, 1);
    let p_k2 = test_payload(22, 2);
    let p_k3 = test_payload(33, 3);

    let batch = SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 888,
        key: 11,
        op_type: OpType::Batch { count: 3 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: vec![
            BatchSubOp::Put {
                key: 11,
                payload: p_k1.clone(),
            },
            BatchSubOp::Put {
                key: 22,
                payload: p_k2.clone(),
            },
            BatchSubOp::Put {
                key: 33,
                payload: p_k3.clone(),
            },
        ],
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 2,
    };

    let ledger = OracleLedger::build(vec![batch], &[888], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Dump has K11 and K33, but secondary key K22 is missing!
    let mut torn_dump = BTreeMap::new();
    torn_dump.insert(11, p_k1);
    torn_dump.insert(33, p_k3);

    match verifier.verify_dump(&torn_dump) {
        OracleVerdict::Violation(InvariantViolation::PartialBatch {
            batch_id,
            unapplied_keys,
            ..
        }) => {
            assert_eq!(batch_id, 888);
            assert!(unapplied_keys.contains(&22));
        }
        OracleVerdict::Violation(InvariantViolation::LostAckWrite { op_id, key, .. }) => {
            assert_eq!(op_id, 888);
            assert_eq!(key, 22);
        }
        other => panic!(
            "Expected PartialBatch or LostAckWrite on K22, got {:?}",
            other
        ),
    }
}

// ---------------------------------------------------------------------------
// T6: Atomic Batches with Deletes and Mixed Operations
// ---------------------------------------------------------------------------

#[test]
fn test_t6_batch_mixed_put_and_delete_atomicity() {
    let p_initial = test_payload(70, 99);
    let p_new = test_payload(71, 100);

    // 1. Initial Put on K70 (ACKed)
    let init_op = SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 1,
        key: 70,
        op_type: OpType::Put,
        payload_len: 48,
        payload_crc: 0,
        payload: Some(p_initial),
        batch_ops: Vec::new(),
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 2,
    };

    // 2. Batch: DEL K70, PUT K71 (ACKed)
    let batch_op = SentOpRecord {
        writer_id: 0,
        seq_num: 2,
        op_id: 2,
        key: 70,
        op_type: OpType::Batch { count: 2 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: vec![
            BatchSubOp::Del { key: 70 },
            BatchSubOp::Put {
                key: 71,
                payload: p_new.clone(),
            },
        ],
        dispatch_order: 10,
        t_sched: 10,
        t_sub: 11,
    };

    let ledger = OracleLedger::build(vec![init_op, batch_op], &[1, 2], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Valid state: K70 absent, K71 present
    let mut valid_dump = BTreeMap::new();
    valid_dump.insert(71, p_new);

    assert!(
        matches!(
            verifier.verify_dump(&valid_dump),
            OracleVerdict::Valid { .. }
        ),
        "Mixed DEL/PUT batch must succeed"
    );
}

// ---------------------------------------------------------------------------
// T7: In-Flight Operations All-or-Nothing
// ---------------------------------------------------------------------------

#[test]
fn test_t7_inflight_batch_all_or_nothing() {
    let p1 = test_payload(81, 1);
    let p2 = test_payload(82, 2);

    // In-flight batch (never received ACK before crash)
    let inflight_batch = SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 999,
        key: 81,
        op_type: OpType::Batch { count: 2 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: vec![
            BatchSubOp::Put {
                key: 81,
                payload: p1.clone(),
            },
            BatchSubOp::Put {
                key: 82,
                payload: p2.clone(),
            },
        ],
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 2,
    };

    let ledger = OracleLedger::build(vec![inflight_batch], &[], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Outcome A: Both applied -> Legal
    let mut dump_both = BTreeMap::new();
    dump_both.insert(81, p1.clone());
    dump_both.insert(82, p2);
    assert!(
        matches!(
            verifier.verify_dump(&dump_both),
            OracleVerdict::Valid { .. }
        ),
        "In-flight batch fully applied is legal"
    );

    // Outcome B: Neither applied -> Legal
    let dump_none = BTreeMap::new();
    assert!(
        matches!(
            verifier.verify_dump(&dump_none),
            OracleVerdict::Valid { .. }
        ),
        "In-flight batch completely dropped is legal"
    );

    // Outcome C: Torn (K81 applied, K82 missing) -> ILLEGAL!
    let mut dump_torn = BTreeMap::new();
    dump_torn.insert(81, p1);
    let verdict = verifier.verify_dump(&dump_torn);
    assert!(
        matches!(verdict, OracleVerdict::Violation(_)),
        "Torn in-flight batch must be rejected: got {:?}",
        verdict
    );
}

// ---------------------------------------------------------------------------
// T8: Solver Branch Isolation
// ---------------------------------------------------------------------------

#[test]
fn test_t8_solver_branch_isolation_independent_cloned_nodes() {
    let p1 = test_payload(91, 1);
    let p2 = test_payload(92, 2);

    let op1 = SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 1,
        key: 91,
        op_type: OpType::Put,
        payload_len: 48,
        payload_crc: 0,
        payload: Some(p1.clone()),
        batch_ops: Vec::new(),
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 2,
    };

    let op2 = SentOpRecord {
        writer_id: 1,
        seq_num: 1,
        op_id: 2,
        key: 92,
        op_type: OpType::Put,
        payload_len: 48,
        payload_crc: 0,
        payload: Some(p2.clone()),
        batch_ops: Vec::new(),
        dispatch_order: 3,
        t_sched: 3,
        t_sub: 4,
    };

    let ledger = OracleLedger::build(vec![op1, op2], &[], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Explore valid dump with only op1 applied
    let mut dump1 = BTreeMap::new();
    dump1.insert(91, p1);
    assert!(matches!(
        verifier.verify_dump(&dump1),
        OracleVerdict::Valid { .. }
    ));

    // Explore valid dump with only op2 applied
    let mut dump2 = BTreeMap::new();
    dump2.insert(92, p2);
    assert!(matches!(
        verifier.verify_dump(&dump2),
        OracleVerdict::Valid { .. }
    ));
}

// ---------------------------------------------------------------------------
// T9: Exact Canonical Memoization
// ---------------------------------------------------------------------------

#[test]
fn test_t9_memoization_distinguishes_inflight_decisions_and_exact_stores() {
    let mut decisions1 = BTreeMap::new();
    decisions1.insert(100, InflightDecision::Undecided);

    let mut decisions2 = BTreeMap::new();
    decisions2.insert(100, InflightDecision::Excluded);

    let mut store = BTreeMap::new();
    store.insert(1, vec![1, 2, 3]);

    let state1 = CanonicalMemoState {
        applied_ops: std::collections::BTreeSet::new(),
        inflight_decisions: decisions1,
        canonical_store: store.clone(),
    };

    let state2 = CanonicalMemoState {
        applied_ops: std::collections::BTreeSet::new(),
        inflight_decisions: decisions2,
        canonical_store: store,
    };

    assert_ne!(
        state1, state2,
        "States with different in-flight decisions must NEVER collide in memoization"
    );
}

// ---------------------------------------------------------------------------
// T10: Initial Database State Guarantee
// ---------------------------------------------------------------------------

#[test]
fn test_t10_initial_database_state_guarantee() {
    let temp_dir = std::env::temp_dir().join(format!("torture_t10_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    // Create a dummy leftover file
    let leftover_file = temp_dir.join("leftover_data.bin");
    std::fs::write(&leftover_file, b"OLD_LEFTOVER_DATABASE_STATE").unwrap();
    assert!(leftover_file.exists());

    // Call ensure_clean_db_path
    ensure_clean_db_path(&temp_dir).expect("Must clean directory");

    // Directory must now be provably empty
    let entries_count = std::fs::read_dir(&temp_dir).unwrap().count();
    assert_eq!(
        entries_count, 0,
        "ensure_clean_db_path must leave the database directory completely empty"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

//! Comprehensive Oracle Mutation, Concurrency & Recovery Tests (Phase 3).
//!
//! Validates the State Lattice, Happens-Before Concurrency, In-Flight Ambiguity,
//! Batch Atomicity, and Persistent Mock Recovery.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use torture::adapter::test_adapter::MockChildAdapter;
use torture::generator::payload::PayloadGenerator;
use torture::oracle::lattice::InvariantViolation;
use torture::oracle::ledger::OracleLedger;
use torture::oracle::verifier::{OracleVerdict, OracleVerifier};
use torture::runner::recovery::execute_recovery_lifecycle;
use torture::runner::writers::{BatchSubOp, OpType, SentOpRecord};

const TEST_SEED: u64 = 0x5EED_9876_1234;

fn mock_child_bin() -> PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_mock_child") {
        return PathBuf::from(path);
    }
    let mut path = std::env::current_exe().expect("current exe");
    path.pop();
    if path.ends_with("deps") {
        path.pop();
    }
    let mut exe = path.join("mock_child");
    #[cfg(windows)]
    exe.set_extension("exe");
    exe
}

#[test]
fn test_oracle_accepts_compliant_state() {
    let generator = PayloadGenerator::new(TEST_SEED);
    let p1 = generator.generate(0, 1, 10, 64);
    let p2 = generator.generate(1, 1, 20, 64);

    let sent_ops = vec![
        SentOpRecord {
            writer_id: 0,
            seq_num: 1,
            op_id: 1001,
            key: 10,
            op_type: OpType::Put,
            payload_len: 64,
            payload_crc: 0,
            payload: Some(p1.clone()),
            batch_ops: Vec::new(),
            dispatch_order: 1,
            t_sched: 1,
            t_sub: 1,
        },
        SentOpRecord {
            writer_id: 1,
            seq_num: 1,
            op_id: 1002,
            key: 20,
            op_type: OpType::Put,
            payload_len: 64,
            payload_crc: 0,
            payload: Some(p2.clone()),
            batch_ops: Vec::new(),
            dispatch_order: 2,
            t_sched: 2,
            t_sub: 2,
        },
    ];

    let ledger = OracleLedger::build(sent_ops, &[1001, 1002], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    let mut dump = BTreeMap::new();
    dump.insert(10, p1);
    dump.insert(20, p2);

    let verdict = verifier.verify_dump(&dump);
    assert!(
        matches!(verdict, OracleVerdict::Valid { .. }),
        "Compliant state must be accepted by oracle, got: {:?}",
        verdict
    );
}

#[test]
fn test_oracle_rejects_lost_acknowledged_write() {
    let generator = PayloadGenerator::new(TEST_SEED);
    let p1 = generator.generate(0, 1, 100, 64);

    let sent_ops = vec![SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 55,
        key: 100,
        op_type: OpType::Put,
        payload_len: 64,
        payload_crc: 0,
        payload: Some(p1),
        batch_ops: Vec::new(),
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 1,
    }];

    // Op 55 was ACKed, but dump is empty!
    let ledger = OracleLedger::build(sent_ops, &[55], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    let dump = BTreeMap::new();
    let verdict = verifier.verify_dump(&dump);

    match verdict {
        OracleVerdict::Violation(InvariantViolation::LostAckWrite { op_id, key, .. }) => {
            assert_eq!(op_id, 55);
            assert_eq!(key, 100);
        }
        other => panic!("Expected LostAckWrite violation, got {:?}", other),
    }
}

#[test]
fn test_oracle_accepts_either_legal_outcome_of_inflight_operation() {
    let generator = PayloadGenerator::new(TEST_SEED);
    let p_inflight = generator.generate(0, 1, 55, 64);

    let sent_ops = vec![SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 900,
        key: 55,
        op_type: OpType::Put,
        payload_len: 64,
        payload_crc: 0,
        payload: Some(p_inflight.clone()),
        batch_ops: Vec::new(),
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 1,
    }];

    // Op 900 is in-flight (sent, but neither ACKed nor rejected)
    let ledger = OracleLedger::build(sent_ops, &[], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Legal Outcome A: in-flight write DID apply
    let mut dump_applied = BTreeMap::new();
    dump_applied.insert(55, p_inflight.clone());
    assert!(
        matches!(
            verifier.verify_dump(&dump_applied),
            OracleVerdict::Valid { .. }
        ),
        "Oracle must accept in-flight operation that applied"
    );

    // Legal Outcome B: in-flight write DID NOT apply (key is absent)
    let dump_unapplied = BTreeMap::new();
    assert!(
        matches!(
            verifier.verify_dump(&dump_unapplied),
            OracleVerdict::Valid { .. }
        ),
        "Oracle must accept in-flight operation that did NOT apply"
    );

    // Illegal Outcome C: key contains a corrupt value
    let mut dump_corrupt = BTreeMap::new();
    let mut corrupted = p_inflight;
    corrupted[50] ^= 0xFF;
    dump_corrupt.insert(55, corrupted);
    assert!(
        matches!(
            verifier.verify_dump(&dump_corrupt),
            OracleVerdict::Violation(InvariantViolation::CorruptedValue { .. })
        ),
        "Oracle must reject corrupted value even on in-flight key"
    );
}

#[test]
fn test_oracle_rejects_explicitly_rejected_operation() {
    let generator = PayloadGenerator::new(TEST_SEED);
    let p1 = generator.generate(0, 1, 70, 64);

    let sent_ops = vec![SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 70,
        key: 70,
        op_type: OpType::Put,
        payload_len: 64,
        payload_crc: 0,
        payload: Some(p1.clone()),
        batch_ops: Vec::new(),
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 1,
    }];

    // Op 70 explicitly failed with ERR
    let ledger = OracleLedger::build(sent_ops, &[], &[(70, "DISK_FULL_SIMULATED".to_string())]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    let mut dump = BTreeMap::new();
    dump.insert(70, p1);

    match verifier.verify_dump(&dump) {
        OracleVerdict::Violation(InvariantViolation::RejectedOpVisible { op_id, key, reason }) => {
            assert_eq!(op_id, 70);
            assert_eq!(key, 70);
            assert!(reason.contains("DISK_FULL"));
        }
        other => panic!("Expected RejectedOpVisible, got {:?}", other),
    }
}

#[test]
fn test_oracle_rejects_partially_applied_batch() {
    let generator = PayloadGenerator::new(TEST_SEED);
    let p1 = generator.generate(0, 1, 10, 64);
    let p2 = generator.generate(0, 2, 20, 64);

    let batch_members = vec![
        BatchSubOp::Put {
            key: 10,
            payload: p1.clone(),
        },
        BatchSubOp::Put {
            key: 20,
            payload: p2.clone(),
        },
    ];

    let sent_ops = vec![SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 777,
        key: 10,
        op_type: OpType::Batch { count: 2 },
        payload_len: 0,
        payload_crc: 0,
        payload: None,
        batch_ops: batch_members,
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 1,
    }];

    // Batch 777 was in-flight at crash time
    let ledger = OracleLedger::build(sent_ops, &[], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Violation: Key 10 is present, but Key 20 is missing!
    let mut partial_dump = BTreeMap::new();
    partial_dump.insert(10, p1.clone());

    match verifier.verify_dump(&partial_dump) {
        OracleVerdict::Violation(InvariantViolation::PartialBatch {
            batch_id,
            applied_keys,
            unapplied_keys,
        }) => {
            assert_eq!(batch_id, 777);
            assert_eq!(applied_keys, vec![10]);
            assert_eq!(unapplied_keys, vec![20]);
        }
        other => panic!("Expected PartialBatch violation, got {:?}", other),
    }

    // Atomicity Case 2: ALL applied -> Valid
    let mut all_applied_dump = BTreeMap::new();
    all_applied_dump.insert(10, p1);
    all_applied_dump.insert(20, p2);
    assert!(matches!(
        verifier.verify_dump(&all_applied_dump),
        OracleVerdict::Valid { .. }
    ));

    // Atomicity Case 3: NONE applied -> Valid
    let none_applied_dump = BTreeMap::new();
    assert!(matches!(
        verifier.verify_dump(&none_applied_dump),
        OracleVerdict::Valid { .. }
    ));
}

#[test]
fn test_oracle_rejects_resurrected_acknowledged_delete() {
    let generator = PayloadGenerator::new(TEST_SEED);
    let p1 = generator.generate(0, 1, 88, 64);

    let sent_ops = vec![
        SentOpRecord {
            writer_id: 0,
            seq_num: 1,
            op_id: 1,
            key: 88,
            op_type: OpType::Put,
            payload_len: 64,
            payload_crc: 0,
            payload: Some(p1.clone()),
            batch_ops: Vec::new(),
            dispatch_order: 1,
            t_sched: 1,
            t_sub: 1,
        },
        SentOpRecord {
            writer_id: 0,
            seq_num: 2,
            op_id: 2,
            key: 88,
            op_type: OpType::Del,
            payload_len: 0,
            payload_crc: 0,
            payload: None,
            batch_ops: Vec::new(),
            dispatch_order: 2,
            t_sched: 2,
            t_sub: 2,
        },
    ];

    // Both put and del were ACKed in order
    let ledger = OracleLedger::build(sent_ops, &[1, 2], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Key 88 erroneously reappears in dump!
    let mut dump = BTreeMap::new();
    dump.insert(88, p1);

    match verifier.verify_dump(&dump) {
        OracleVerdict::Violation(InvariantViolation::ResurrectedDelete { op_id, key }) => {
            assert_eq!(op_id, 2);
            assert_eq!(key, 88);
        }
        other => panic!("Expected ResurrectedDelete, got {:?}", other),
    }
}

#[test]
fn test_oracle_rejects_foreign_run_values() {
    let foreign_gen = PayloadGenerator::new(0x9999_9999); // Different run seed
    let foreign_payload = foreign_gen.generate(0, 1, 30, 64);

    let sent_ops = vec![SentOpRecord {
        writer_id: 0,
        seq_num: 1,
        op_id: 1,
        key: 30,
        op_type: OpType::Put,
        payload_len: 64,
        payload_crc: 0,
        payload: Some(foreign_payload.clone()),
        batch_ops: Vec::new(),
        dispatch_order: 1,
        t_sched: 1,
        t_sub: 1,
    }];

    let ledger = OracleLedger::build(sent_ops, &[1], &[]);
    // Target test seed is different from foreign_payload
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    let mut dump = BTreeMap::new();
    dump.insert(30, foreign_payload);

    match verifier.verify_dump(&dump) {
        OracleVerdict::Violation(InvariantViolation::ForeignValue { key, details }) => {
            assert_eq!(key, 30);
            assert!(details.contains("Foreign run seed detected"));
        }
        other => panic!("Expected ForeignValue violation, got {:?}", other),
    }
}

#[test]
fn test_oracle_handles_overlapping_hot_key_writes_correctly() {
    let generator = PayloadGenerator::new(TEST_SEED);
    let p_writer0 = generator.generate(0, 1, 90050, 64);
    let p_writer1 = generator.generate(1, 1, 90050, 64);

    // Concurrently overlapping writes to hot key 90050
    // Writer 0 dispatches Op 100
    // Writer 1 dispatches Op 200 before Op 100 is ACKed (overlapping concurrent dispatch)
    let sent_ops = vec![
        SentOpRecord {
            writer_id: 0,
            seq_num: 1,
            op_id: 100,
            key: 90050,
            op_type: OpType::Put,
            payload_len: 64,
            payload_crc: 0,
            payload: Some(p_writer0.clone()),
            batch_ops: Vec::new(),
            dispatch_order: 1,
            t_sched: 1,
            t_sub: 1,
        },
        SentOpRecord {
            writer_id: 1,
            seq_num: 1,
            op_id: 200,
            key: 90050,
            op_type: OpType::Put,
            payload_len: 64,
            payload_crc: 0,
            payload: Some(p_writer1.clone()),
            batch_ops: Vec::new(),
            dispatch_order: 2,
            t_sched: 2,
            t_sub: 2,
        },
    ];

    // Both ACKed concurrently (Op 100 dispatched at tick 1, Op 200 dispatched at tick 2, Op 100 ACKed at tick 3, Op 200 ACKed at tick 4)
    let ledger = OracleLedger::build_with_ticks(sent_ops, &[100, 200], &[(100, 3), (200, 4)], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Outcome A: Writer 0's value on disk -> Legal!
    let mut dump_w0 = BTreeMap::new();
    dump_w0.insert(90050, p_writer0);
    assert!(
        matches!(verifier.verify_dump(&dump_w0), OracleVerdict::Valid { .. }),
        "Oracle must accept Writer 0's concurrent write"
    );

    // Outcome B: Writer 1's value on disk -> Legal!
    let mut dump_w1 = BTreeMap::new();
    dump_w1.insert(90050, p_writer1);
    assert!(
        matches!(verifier.verify_dump(&dump_w1), OracleVerdict::Valid { .. }),
        "Oracle must accept Writer 1's concurrent write"
    );
}

#[test]
fn test_oracle_preserves_per_writer_ordering() {
    let generator = PayloadGenerator::new(TEST_SEED);
    let val_op1 = generator.generate(0, 1, 5, 64);
    let val_op2 = generator.generate(0, 2, 5, 64);

    // Same writer writes Op 1, then Op 2 to key 5
    let sent_ops = vec![
        SentOpRecord {
            writer_id: 0,
            seq_num: 1,
            op_id: 1,
            key: 5,
            op_type: OpType::Put,
            payload_len: 64,
            payload_crc: 0,
            payload: Some(val_op1.clone()),
            batch_ops: Vec::new(),
            dispatch_order: 1,
            t_sched: 1,
            t_sub: 1,
        },
        SentOpRecord {
            writer_id: 0,
            seq_num: 2,
            op_id: 2,
            key: 5,
            op_type: OpType::Put,
            payload_len: 64,
            payload_crc: 0,
            payload: Some(val_op2.clone()),
            batch_ops: Vec::new(),
            dispatch_order: 2,
            t_sched: 2,
            t_sub: 2,
        },
    ];

    let ledger = OracleLedger::build(sent_ops, &[1, 2], &[]);
    let verifier = OracleVerifier::new(TEST_SEED, ledger);

    // Op 2 superseded Op 1. If dump shows old val_op1 -> Violation!
    let mut stale_dump = BTreeMap::new();
    stale_dump.insert(5, val_op1);

    match verifier.verify_dump(&stale_dump) {
        OracleVerdict::Violation(InvariantViolation::ForeignValue { key, .. }) => {
            assert_eq!(key, 5);
        }
        other => panic!(
            "Expected violation for stale superseded write, got {:?}",
            other
        ),
    }

    // Dump shows latest val_op2 -> Valid!
    let mut latest_dump = BTreeMap::new();
    latest_dump.insert(5, val_op2);
    assert!(matches!(
        verifier.verify_dump(&latest_dump),
        OracleVerdict::Valid { .. }
    ));
}

#[test]
fn test_persistent_mock_recovery_lifecycle_end_to_end() {
    let bin = mock_child_bin();
    let temp_dir = std::env::temp_dir().join(format!("torture_phase3_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let adapter = MockChildAdapter::new(bin.clone());

    // Execute full crash-recovery iteration
    let result = execute_recovery_lifecycle(
        &adapter,
        &bin,
        &temp_dir,
        777888,
        1,
        2,
        Duration::from_millis(15),
        Duration::from_millis(35),
        64,
    )
    .expect("Recovery lifecycle must execute cleanly");

    // Invariant 1: OPEN must succeed
    assert!(result.open_result.exit_status_ok, "Open must report OK");

    // Invariant 2: CHECK must report OK
    assert!(result.check_result.is_ok, "Check must report OK");

    // Invariant 3: Oracle must accept recovered dump
    assert!(
        matches!(result.oracle_verdict, OracleVerdict::Valid { .. }),
        "Oracle verdict must be Valid for compliant persistent mock, got {:?}",
        result.oracle_verdict
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

//! Mutation Self-Tests proving that the torture tool detects all failure conditions.
//!
//! Evaluates the verification infrastructure against compliant and broken mock engines.

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::adapter::EngineProcess;
    use crate::generator::payload::{PayloadGenerator, to_hex};
    use crate::mocks::engine::{MockBehavior, MockEngine};
    use crate::mocks::verifier::{MutationViolation, TrackedOp, verify_mock_state};

    const TEST_SEED: u64 = 0xABCD_1234_5678_EEEE;

    #[test]
    fn test_compliant_mock_passes_verification() {
        let generator = PayloadGenerator::new(TEST_SEED);
        let mut mock = MockEngine::new(MockBehavior::Compliant);

        let mut tracked_ops = Vec::new();
        let mut acked_ops = Vec::new();
        let errored_ops = Vec::new();

        // 1. Put key 1
        let p1 = generator.generate(0, 1, 1, 64);
        tracked_ops.push(TrackedOp::Put {
            op_id: 1,
            key: 1,
            payload: p1.clone(),
        });
        mock.send_raw_op(&format!("1 put 1 {}", to_hex(&p1)))
            .unwrap();
        if let Some(resp) = mock.poll_next_response(Duration::from_millis(5)).unwrap() {
            if resp == "ACK 1" {
                acked_ops.push(1);
            }
        }

        // 2. Put key 2
        let p2 = generator.generate(0, 2, 2, 80);
        tracked_ops.push(TrackedOp::Put {
            op_id: 2,
            key: 2,
            payload: p2.clone(),
        });
        mock.send_raw_op(&format!("2 put 2 {}", to_hex(&p2)))
            .unwrap();
        if let Some(resp) = mock.poll_next_response(Duration::from_millis(5)).unwrap() {
            if resp == "ACK 2" {
                acked_ops.push(2);
            }
        }

        // 3. Del key 1
        tracked_ops.push(TrackedOp::Del { op_id: 3, key: 1 });
        mock.send_raw_op("3 del 1").unwrap();
        if let Some(resp) = mock.poll_next_response(Duration::from_millis(5)).unwrap() {
            if resp == "ACK 3" {
                acked_ops.push(3);
            }
        }

        let dump = mock.dump_state();
        let result = verify_mock_state(TEST_SEED, &tracked_ops, &acked_ops, &errored_ops, &dump);

        assert!(
            result.is_ok(),
            "Compliant mock must pass verification with zero violations"
        );
    }

    #[test]
    fn test_mutation_lost_ack_write_detected() {
        // Simulation: Op ID 42 was ACKed, but is missing from the database dump
        let generator = PayloadGenerator::new(TEST_SEED);
        let mut mock = MockEngine::new(MockBehavior::LostAckWrite { dropped_op_id: 42 });

        let p1 = generator.generate(0, 42, 100, 64);
        let tracked_ops = vec![TrackedOp::Put {
            op_id: 42,
            key: 100,
            payload: p1.clone(),
        }];

        mock.send_raw_op(&format!("42 put 100 {}", to_hex(&p1)))
            .unwrap();
        let resp = mock
            .poll_next_response(Duration::from_millis(5))
            .unwrap()
            .unwrap();
        assert_eq!(resp, "ACK 42");

        let acked_ops = vec![42];
        let dump = mock.dump_state();

        let result = verify_mock_state(TEST_SEED, &tracked_ops, &acked_ops, &[], &dump);
        assert_eq!(
            result,
            Err(MutationViolation::LostAckWrite {
                op_id: 42,
                key: 100
            }),
            "Infrastructure MUST detect lost ACKed write (Rule 1 violation)"
        );
    }

    #[test]
    fn test_mutation_partial_batch_detected() {
        // Simulation: Batch 888 has 2 keys, but the engine only committed the 1st key before crash
        let generator = PayloadGenerator::new(TEST_SEED);
        let mut mock = MockEngine::new(MockBehavior::PartialBatch { max_applied_ops: 1 });

        let p1 = generator.generate(0, 888, 10, 64);
        let p2 = generator.generate(0, 888, 20, 64);

        let tracked_ops = vec![TrackedOp::Batch {
            batch_id: 888,
            ops: vec![(10, Some(p1.clone())), (20, Some(p2.clone()))],
        }];

        mock.send_raw_op("888 batch 2").unwrap();
        mock.send_raw_op(&format!("put 10 {}", to_hex(&p1)))
            .unwrap();
        mock.send_raw_op(&format!("put 20 {}", to_hex(&p2)))
            .unwrap();

        let resp = mock
            .poll_next_response(Duration::from_millis(5))
            .unwrap()
            .unwrap();
        assert_eq!(resp, "ACK 888");

        let acked_ops = vec![888];
        let dump = mock.dump_state();

        let result = verify_mock_state(TEST_SEED, &tracked_ops, &acked_ops, &[], &dump);
        assert_eq!(
            result,
            Err(MutationViolation::PartialBatch {
                batch_id: 888,
                applied_keys: vec![10],
                missing_keys: vec![20]
            }),
            "Infrastructure MUST detect partial batch application (Rule 2 atomicity violation)"
        );
    }

    #[test]
    fn test_mutation_resurrected_delete_detected() {
        // Simulation: Key 50 is deleted and ACKed, but reappears in the dump
        let generator = PayloadGenerator::new(TEST_SEED);
        let mut mock = MockEngine::new(MockBehavior::ResurrectedDelete {
            resurrected_key: 50,
        });

        let p1 = generator.generate(0, 1, 50, 64);
        let tracked_ops = vec![
            TrackedOp::Put {
                op_id: 1,
                key: 50,
                payload: p1.clone(),
            },
            TrackedOp::Del { op_id: 2, key: 50 },
        ];

        // 1. Put
        mock.send_raw_op(&format!("1 put 50 {}", to_hex(&p1)))
            .unwrap();
        let _ = mock.poll_next_response(Duration::from_millis(5)).unwrap();

        // 2. Del (Mock responds ACK 2, but leaves key in state)
        mock.send_raw_op("2 del 50").unwrap();
        let resp2 = mock
            .poll_next_response(Duration::from_millis(5))
            .unwrap()
            .unwrap();
        assert_eq!(resp2, "ACK 2");

        let acked_ops = vec![1, 2];
        let dump = mock.dump_state();

        let result = verify_mock_state(TEST_SEED, &tracked_ops, &acked_ops, &[], &dump);
        assert_eq!(
            result,
            Err(MutationViolation::ResurrectedDelete { op_id: 2, key: 50 }),
            "Infrastructure MUST detect resurrected delete (Rule 1/3 violation)"
        );
    }

    #[test]
    fn test_mutation_corrupted_value_detected() {
        // Simulation: Data bitflip on disk in key 77 triggers CRC32 validation failure
        let generator = PayloadGenerator::new(TEST_SEED);
        let mut mock = MockEngine::new(MockBehavior::CorruptedValue { corrupted_key: 77 });

        let p1 = generator.generate(0, 5, 77, 64);
        let tracked_ops = vec![TrackedOp::Put {
            op_id: 5,
            key: 77,
            payload: p1.clone(),
        }];

        mock.send_raw_op(&format!("5 put 77 {}", to_hex(&p1)))
            .unwrap();
        let _ = mock.poll_next_response(Duration::from_millis(5)).unwrap();

        let acked_ops = vec![5];
        let dump = mock.dump_state();

        let result = verify_mock_state(TEST_SEED, &tracked_ops, &acked_ops, &[], &dump);
        match result {
            Err(MutationViolation::CorruptedValue { key, reason }) => {
                assert_eq!(key, 77);
                assert!(
                    reason.contains("CRC32 mismatch"),
                    "Reason should specify CRC32 mismatch: {}",
                    reason
                );
            }
            other => panic!("Expected CorruptedValue error, got {:?}", other),
        }
    }

    #[test]
    fn test_mutation_spurious_error_detected() {
        // Simulation: Database returns ERR for a valid operation
        let generator = PayloadGenerator::new(TEST_SEED);
        let mut mock = MockEngine::new(MockBehavior::SpuriousError { failing_op_id: 99 });

        let p1 = generator.generate(0, 99, 10, 64);
        mock.send_raw_op(&format!("99 put 10 {}", to_hex(&p1)))
            .unwrap();

        let resp = mock
            .poll_next_response(Duration::from_millis(5))
            .unwrap()
            .unwrap();
        assert_eq!(resp, "ERR 99 SPURIOUS_MOCK_ERROR");

        let errored_ops = vec![(99, "SPURIOUS_MOCK_ERROR".to_string())];
        let dump = mock.dump_state();

        let result = verify_mock_state(TEST_SEED, &[], &[], &errored_ops, &dump);
        assert_eq!(
            result,
            Err(MutationViolation::SpuriousError {
                op_id: 99,
                error_msg: "SPURIOUS_MOCK_ERROR".to_string()
            }),
            "Infrastructure MUST detect unexpected error responses"
        );
    }
}

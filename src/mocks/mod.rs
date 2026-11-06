//! Mock Engines and Mutation Self-Tests.
//!
//! IMPORTANT: Mock engines exist EXCLUSIVELY to test and prove the torture harness.
//! They simulate protocol interaction in-memory without implementing database internals,
//! storage, indexing, write-ahead logging, compaction, or persistence.
//!
//! These mocks must NEVER be presented as verification of the actual database.

pub mod engine;
pub mod mutation_tests;
pub mod verifier;

pub use engine::{MockBehavior, MockEngine};
pub use verifier::{MutationViolation, TrackedOp, verify_mock_state};

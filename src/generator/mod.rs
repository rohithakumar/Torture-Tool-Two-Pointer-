//! Deterministic Workload Generator.
//!
//! Provides:
//! - Reproducible seeded PRNG streams per writer thread.
//! - Self-describing payloads embedding identity, key, sequence, length, and CRC32.
//! - Independent validation for detected database records.

pub mod payload;
pub mod prng;

pub use payload::{
    MIN_PAYLOAD_SIZE, PayloadError, PayloadGenerator, PayloadHeader, from_hex, to_hex,
};
pub use prng::{PrngStream, SplitMix64};

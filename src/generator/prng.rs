//! Deterministic, seedable Pseudo-Random Number Generator (PRNG).
//!
//! Provides independent, reproducible streams for different writer threads without
//! relying on system randomness or external dependencies.

/// A pure-Rust 64-bit SplitMix PRNG core.
///
/// A deterministic 64-bit pseudo-random generator based on the SplitMix algorithm.
/// It is used for deterministic seeding and stream derivation without system randomness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Initialize with a 64-bit seed.
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Generate the next pseudo-random 64-bit integer.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Generate the next pseudo-random 32-bit integer.
    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }
}

/// An independent, reproducible PRNG stream for a specific writer or subsystem.
///
/// Each writer has a reproducible, separately initialized PRNG stream derived
/// deterministically from `(master_seed, stream_id)`. Generating numbers in
/// Stream A does not advance or modify Stream B's state, providing reproducible
/// stream isolation across writers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrngStream {
    master_seed: u64,
    stream_id: u32,
    core: SplitMix64,
    steps_taken: u64,
}

impl PrngStream {
    /// Create a new independent stream for the given `(master_seed, stream_id)`.
    pub fn new(master_seed: u64, stream_id: u32) -> Self {
        // Decorrelate master seed and stream ID using distinct Weyl multipliers
        let mut mixer =
            SplitMix64::new(master_seed ^ ((stream_id as u64).wrapping_mul(0x517C_C1B7_2722_0A95)));
        let derived_seed = mixer.next_u64();
        Self {
            master_seed,
            stream_id,
            core: SplitMix64::new(derived_seed),
            steps_taken: 0,
        }
    }

    /// Return the master seed that created this stream.
    pub fn master_seed(&self) -> u64 {
        self.master_seed
    }

    /// Return the unique stream ID (e.g. writer ID).
    pub fn stream_id(&self) -> u32 {
        self.stream_id
    }

    /// Return the count of values generated so far.
    pub fn steps_taken(&self) -> u64 {
        self.steps_taken
    }

    /// Generate the next 64-bit integer.
    pub fn next_u64(&mut self) -> u64 {
        self.steps_taken += 1;
        self.core.next_u64()
    }

    /// Generate the next 32-bit integer.
    pub fn next_u32(&mut self) -> u32 {
        self.steps_taken += 1;
        self.core.next_u32()
    }

    /// Generate a uniform random integer in the range `[min, max)`.
    /// Panics if `min >= max`.
    pub fn next_range_u64(&mut self, min: u64, max: u64) -> u64 {
        assert!(min < max, "min must be strictly less than max");
        let range = max - min;
        min + (self.next_u64() % range)
    }

    /// Generate a uniform random integer in the range `[min, max)`.
    pub fn next_range_usize(&mut self, min: usize, max: usize) -> usize {
        assert!(min < max, "min must be strictly less than max");
        let range = (max - min) as u64;
        min + (self.next_u64() % range) as usize
    }

    /// Fill a buffer with deterministic pseudo-random bytes.
    pub fn fill_bytes(&mut self, buf: &mut [u8]) {
        let mut chunks = buf.chunks_exact_mut(8);
        for chunk in chunks.by_ref() {
            let val = self.next_u64();
            chunk.copy_from_slice(&val.to_le_bytes());
        }
        let remainder = chunks.into_remainder();
        if !remainder.is_empty() {
            let val = self.next_u64();
            let bytes = val.to_le_bytes();
            remainder.copy_from_slice(&bytes[..remainder.len()]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prng_reproducibility() {
        let mut stream1 = PrngStream::new(0xDEAD_BEEF_CAFE_BABE, 0);
        let mut stream2 = PrngStream::new(0xDEAD_BEEF_CAFE_BABE, 0);

        let seq1: Vec<u64> = (0..1000).map(|_| stream1.next_u64()).collect();
        let seq2: Vec<u64> = (0..1000).map(|_| stream2.next_u64()).collect();

        assert_eq!(
            seq1, seq2,
            "Identical seed & stream ID must produce identical sequences"
        );
    }

    #[test]
    fn test_stream_independence() {
        // Writer 0 and Writer 1 initialized with same master seed
        let mut writer0 = PrngStream::new(12345, 0);
        let mut writer1 = PrngStream::new(12345, 1);

        // Advance writer 1 by 500 steps
        for _ in 0..500 {
            let _ = writer1.next_u64();
        }

        // Fresh writer 0 with no writer 1 activity
        let mut writer0_baseline = PrngStream::new(12345, 0);

        let seq0_interleaved: Vec<u64> = (0..200).map(|_| writer0.next_u64()).collect();
        let seq0_baseline: Vec<u64> = (0..200).map(|_| writer0_baseline.next_u64()).collect();

        assert_eq!(
            seq0_interleaved, seq0_baseline,
            "Activity on Writer 1 must NOT perturb Writer 0 sequence"
        );
    }

    #[test]
    fn test_different_seeds_produce_different_sequences() {
        let mut s1 = PrngStream::new(100, 0);
        let mut s2 = PrngStream::new(101, 0);

        let v1: Vec<u64> = (0..50).map(|_| s1.next_u64()).collect();
        let v2: Vec<u64> = (0..50).map(|_| s2.next_u64()).collect();

        assert_ne!(v1, v2, "Different seeds must produce different sequences");
    }

    #[test]
    fn test_different_streams_produce_different_sequences() {
        let mut s1 = PrngStream::new(42, 0);
        let mut s2 = PrngStream::new(42, 1);

        let v1: Vec<u64> = (0..50).map(|_| s1.next_u64()).collect();
        let v2: Vec<u64> = (0..50).map(|_| s2.next_u64()).collect();

        assert_ne!(
            v1, v2,
            "Different stream IDs must produce different sequences"
        );
    }

    #[test]
    fn test_range_bounds() {
        let mut stream = PrngStream::new(999, 0);
        for _ in 0..1000 {
            let val = stream.next_range_u64(10, 25);
            assert!(val >= 10 && val < 25);

            let idx = stream.next_range_usize(50, 100);
            assert!(idx >= 50 && idx < 100);
        }
    }

    #[test]
    fn test_fill_bytes_reproducibility() {
        let mut s1 = PrngStream::new(777, 3);
        let mut s2 = PrngStream::new(777, 3);

        let mut buf1 = [0u8; 127];
        let mut buf2 = [0u8; 127];

        s1.fill_bytes(&mut buf1);
        s2.fill_bytes(&mut buf2);

        assert_eq!(buf1, buf2);
        assert!(buf1.iter().any(|&b| b != 0));
    }
}

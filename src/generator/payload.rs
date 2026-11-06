//! Self-describing payload generator and validator.
//!
//! Generates deterministic binary values containing embedded identity, metadata,
//! deterministic body bytes, and a CRC32 checksum.
//! Enables independent validation of any value returned from the database without
//! relying on external state.

use crate::generator::prng::SplitMix64;

/// Magic bytes prefix: "TORT"
pub const PAYLOAD_MAGIC: [u8; 4] = [0x54, 0x4F, 0x52, 0x54];
/// Header version
pub const PAYLOAD_VERSION: u16 = 1;
/// Minimum payload size: 38-byte header + 4-byte CRC32 = 42 bytes
pub const MIN_PAYLOAD_SIZE: usize = 42;

// Compile-time IEEE 802.3 CRC32 lookup table (polynomial 0xEDB88320)
const fn make_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC_TABLE: [u32; 256] = make_crc_table();

/// Compute standard IEEE 802.3 CRC32 checksum.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        let idx = ((crc ^ (byte as u32)) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC_TABLE[idx];
    }
    !crc
}

/// Metadata extracted from a validated self-describing payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadHeader {
    pub version: u16,
    pub run_seed: u64,
    pub writer_id: u32,
    pub op_seq: u64,
    pub key_id: u64,
    pub total_len: u32,
    pub checksum: u32,
}

/// Errors detected during independent payload validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadError {
    TooShort { actual: usize, min_required: usize },
    InvalidMagic([u8; 4]),
    UnsupportedVersion(u16),
    LengthMismatch { header_len: u32, actual_len: usize },
    ChecksumMismatch { stored: u32, computed: u32 },
    KeyMismatch { expected: u64, found: u64 },
    SeedMismatch { expected: u64, found: u64 },
    CorruptedBody { offset: usize },
}

impl std::fmt::Display for PayloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort {
                actual,
                min_required,
            } => {
                write!(
                    f,
                    "Payload too short (actual {}, minimum {})",
                    actual, min_required
                )
            }
            Self::InvalidMagic(m) => write!(f, "Invalid magic bytes: {:?}", m),
            Self::UnsupportedVersion(v) => write!(f, "Unsupported payload version: {}", v),
            Self::LengthMismatch {
                header_len,
                actual_len,
            } => {
                write!(
                    f,
                    "Length mismatch: header declares {} bytes, actual is {}",
                    header_len, actual_len
                )
            }
            Self::ChecksumMismatch { stored, computed } => {
                write!(
                    f,
                    "CRC32 checksum mismatch: stored {:#010X}, computed {:#010X}",
                    stored, computed
                )
            }
            Self::KeyMismatch { expected, found } => {
                write!(
                    f,
                    "Key mismatch: expected key {}, found payload for key {}",
                    expected, found
                )
            }
            Self::SeedMismatch { expected, found } => {
                write!(
                    f,
                    "Seed mismatch: expected seed {}, found payload for seed {}",
                    expected, found
                )
            }
            Self::CorruptedBody { offset } => {
                write!(f, "Deterministic body corrupted at byte offset {}", offset)
            }
        }
    }
}

impl std::error::Error for PayloadError {}

/// Generator for self-describing values.
#[derive(Debug, Clone)]
pub struct PayloadGenerator {
    run_seed: u64,
}

impl PayloadGenerator {
    /// Create a generator bound to the run seed.
    pub fn new(run_seed: u64) -> Self {
        Self { run_seed }
    }

    /// Return the run seed.
    pub fn run_seed(&self) -> u64 {
        self.run_seed
    }

    /// Generate a self-describing payload.
    ///
    /// If `target_size` is less than `MIN_PAYLOAD_SIZE` (42 bytes), it is clamped to 42.
    pub fn generate(
        &self,
        writer_id: u32,
        op_seq: u64,
        key_id: u64,
        target_size: usize,
    ) -> Vec<u8> {
        let size = target_size.max(MIN_PAYLOAD_SIZE);
        let mut buf = vec![0u8; size];

        // 0..4: Magic
        buf[0..4].copy_from_slice(&PAYLOAD_MAGIC);
        // 4..6: Version
        buf[4..6].copy_from_slice(&PAYLOAD_VERSION.to_le_bytes());
        // 6..14: Run Seed
        buf[6..14].copy_from_slice(&self.run_seed.to_le_bytes());
        // 14..18: Writer ID
        buf[14..18].copy_from_slice(&writer_id.to_le_bytes());
        // 18..26: Op Sequence
        buf[18..26].copy_from_slice(&op_seq.to_le_bytes());
        // 26..34: Key ID
        buf[26..34].copy_from_slice(&key_id.to_le_bytes());
        // 34..38: Total Length
        buf[34..38].copy_from_slice(&(size as u32).to_le_bytes());

        // 38..size-4: Deterministic body fill
        let body_len = size - MIN_PAYLOAD_SIZE;
        if body_len > 0 {
            // Seed the body filler using a decorrelated mix of (seed, writer, op, key)
            let body_seed = self.run_seed
                ^ ((writer_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
                ^ (op_seq.wrapping_mul(0xBF58_476D_1CE4_E5B9))
                ^ (key_id.wrapping_mul(0x94D0_49BB_1331_11EB));

            let mut body_prng = SplitMix64::new(body_seed);
            let body_slice = &mut buf[38..size - 4];

            let mut chunks = body_slice.chunks_exact_mut(8);
            for chunk in chunks.by_ref() {
                let v = body_prng.next_u64();
                chunk.copy_from_slice(&v.to_le_bytes());
            }
            let rem = chunks.into_remainder();
            if !rem.is_empty() {
                let v = body_prng.next_u64();
                rem.copy_from_slice(&v.to_le_bytes()[..rem.len()]);
            }
        }

        // size-4..size: CRC32 checksum over [0..size-4]
        let checksum = crc32(&buf[..size - 4]);
        buf[size - 4..size].copy_from_slice(&checksum.to_le_bytes());

        buf
    }

    /// Decode and independently validate a raw payload slice.
    ///
    /// Checks:
    /// 1. Minimum size
    /// 2. "TORT" magic
    /// 3. Version
    /// 4. Declared length == actual length
    /// 5. CRC32 checksum over header + body
    /// 6. Optional key match check
    /// 7. Optional seed match check
    /// 8. Exact deterministic body byte verification
    pub fn validate(
        raw: &[u8],
        expected_key: Option<u64>,
        expected_seed: Option<u64>,
    ) -> Result<PayloadHeader, PayloadError> {
        if raw.len() < MIN_PAYLOAD_SIZE {
            return Err(PayloadError::TooShort {
                actual: raw.len(),
                min_required: MIN_PAYLOAD_SIZE,
            });
        }

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&raw[0..4]);
        if magic != PAYLOAD_MAGIC {
            return Err(PayloadError::InvalidMagic(magic));
        }

        let version = u16::from_le_bytes([raw[4], raw[5]]);
        if version != PAYLOAD_VERSION {
            return Err(PayloadError::UnsupportedVersion(version));
        }

        let run_seed = u64::from_le_bytes(raw[6..14].try_into().unwrap());
        let writer_id = u32::from_le_bytes(raw[14..18].try_into().unwrap());
        let op_seq = u64::from_le_bytes(raw[18..26].try_into().unwrap());
        let key_id = u64::from_le_bytes(raw[26..34].try_into().unwrap());
        let total_len = u32::from_le_bytes(raw[34..38].try_into().unwrap());

        if total_len as usize != raw.len() {
            return Err(PayloadError::LengthMismatch {
                header_len: total_len,
                actual_len: raw.len(),
            });
        }

        let stored_checksum = u32::from_le_bytes(raw[raw.len() - 4..].try_into().unwrap());
        let computed_checksum = crc32(&raw[..raw.len() - 4]);

        if stored_checksum != computed_checksum {
            return Err(PayloadError::ChecksumMismatch {
                stored: stored_checksum,
                computed: computed_checksum,
            });
        }

        if let Some(exp_key) = expected_key {
            if key_id != exp_key {
                return Err(PayloadError::KeyMismatch {
                    expected: exp_key,
                    found: key_id,
                });
            }
        }

        if let Some(exp_seed) = expected_seed {
            if run_seed != exp_seed {
                return Err(PayloadError::SeedMismatch {
                    expected: exp_seed,
                    found: run_seed,
                });
            }
        }

        // Verify deterministic body contents
        let body_len = raw.len() - MIN_PAYLOAD_SIZE;
        if body_len > 0 {
            let body_seed = run_seed
                ^ ((writer_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
                ^ (op_seq.wrapping_mul(0xBF58_476D_1CE4_E5B9))
                ^ (key_id.wrapping_mul(0x94D0_49BB_1331_11EB));

            let mut body_prng = SplitMix64::new(body_seed);
            let body_slice = &raw[38..raw.len() - 4];

            let mut offset = 0;
            let chunks = body_slice.chunks_exact(8);
            for chunk in chunks {
                let expected = body_prng.next_u64().to_le_bytes();
                if chunk != expected {
                    return Err(PayloadError::CorruptedBody {
                        offset: 38 + offset,
                    });
                }
                offset += 8;
            }
            let rem = &body_slice[offset..];
            if !rem.is_empty() {
                let expected = body_prng.next_u64().to_le_bytes();
                if rem != &expected[..rem.len()] {
                    return Err(PayloadError::CorruptedBody {
                        offset: 38 + offset,
                    });
                }
            }
        }

        Ok(PayloadHeader {
            version,
            run_seed,
            writer_id,
            op_seq,
            key_id,
            total_len,
            checksum: stored_checksum,
        })
    }
}

/// Converts raw bytes to lower-case hexadecimal string.
pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// Converts hexadecimal string to raw byte vector.
pub fn from_hex(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err("Hex string must have an even length".to_string());
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16)
            .map_err(|e| format!("Invalid hex digit at index {}: {}", i, e))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_generation_and_validation() {
        let generator = PayloadGenerator::new(0x1234_5678_9ABC_DEF0);
        let payload = generator.generate(1, 42, 100, 128);

        assert_eq!(payload.len(), 128);

        let header = PayloadGenerator::validate(&payload, Some(100), Some(0x1234_5678_9ABC_DEF0))
            .expect("Valid payload must pass validation");

        assert_eq!(header.version, 1);
        assert_eq!(header.run_seed, 0x1234_5678_9ABC_DEF0);
        assert_eq!(header.writer_id, 1);
        assert_eq!(header.op_seq, 42);
        assert_eq!(header.key_id, 100);
        assert_eq!(header.total_len, 128);
    }

    #[test]
    fn test_configurable_sizes() {
        let generator = PayloadGenerator::new(42);

        // Minimum clamp
        let p_min = generator.generate(0, 1, 10, 10);
        assert_eq!(p_min.len(), MIN_PAYLOAD_SIZE);
        assert!(PayloadGenerator::validate(&p_min, Some(10), Some(42)).is_ok());

        // 256 bytes
        let p_256 = generator.generate(0, 1, 10, 256);
        assert_eq!(p_256.len(), 256);
        assert!(PayloadGenerator::validate(&p_256, Some(10), Some(42)).is_ok());

        // 1 MB payload (mode large-values)
        let one_mb = 1024 * 1024;
        let p_1mb = generator.generate(2, 999, 50, one_mb);
        assert_eq!(p_1mb.len(), one_mb);
        assert!(PayloadGenerator::validate(&p_1mb, Some(50), Some(42)).is_ok());
    }

    #[test]
    fn test_payload_uniqueness() {
        let generator = PayloadGenerator::new(100);

        let p1 = generator.generate(0, 1, 10, 64);
        let p2 = generator.generate(0, 2, 10, 64); // Different op seq
        let p3 = generator.generate(1, 1, 10, 64); // Different writer
        let p4 = generator.generate(0, 1, 11, 64); // Different key

        assert_ne!(p1, p2);
        assert_ne!(p1, p3);
        assert_ne!(p1, p4);
    }

    #[test]
    fn test_corruption_detection_checksum() {
        let generator = PayloadGenerator::new(555);
        let mut payload = generator.generate(0, 1, 10, 100);

        // Corrupt a byte in the body
        payload[50] ^= 0xFF;

        let err = PayloadGenerator::validate(&payload, Some(10), Some(555)).unwrap_err();
        match err {
            PayloadError::ChecksumMismatch { .. } => {}
            other => panic!("Expected ChecksumMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_corruption_detection_magic() {
        let generator = PayloadGenerator::new(555);
        let mut payload = generator.generate(0, 1, 10, 100);

        // Corrupt magic
        payload[0] = b'X';

        let err = PayloadGenerator::validate(&payload, None, None).unwrap_err();
        assert!(matches!(err, PayloadError::InvalidMagic(_)));
    }

    #[test]
    fn test_key_mismatch_detection() {
        let generator = PayloadGenerator::new(555);
        let payload = generator.generate(0, 1, 10, 100);

        let err = PayloadGenerator::validate(&payload, Some(999), None).unwrap_err();
        assert_eq!(
            err,
            PayloadError::KeyMismatch {
                expected: 999,
                found: 10
            }
        );
    }

    #[test]
    fn test_hex_conversion_roundtrip() {
        let generator = PayloadGenerator::new(1234);
        let payload = generator.generate(1, 1, 5, 64);

        let hex = to_hex(&payload);
        let decoded = from_hex(&hex).expect("Hex must decode");

        assert_eq!(payload, decoded);
    }
}

//! Failure Bundles and Test Evidence Generation (Phase 1+)
//!
//! Responsible for:
//! - Packaging reproducible failure artifacts upon invariant violation:
//!   - `db-after-kill.bin`
//!   - `expected.txt` and `found.txt` (with offending op IDs)
//!   - `replay.txt` with exact CLI reproduction commands
//! - Recording regression seeds to `seeds/regressions.txt`.
//! - Emitting summary statistics and kill-landing metrics.

//! Shared helpers for the IronCondor parser fuzz targets (#52).
//!
//! Holds the tight [`ResourceLimits`] (one source of truth for every target)
//! and the bundle byte-framing (the inverse of the seed encoder in
//! `tests/seed_corpus.rs`) so the three `fuzz_targets/*` bins stay thin.

use ironcondor::ResourceLimits;

/// Tight ceilings so the fuzzer explores VALIDATION logic, not gigantic
/// allocations.
///
/// Every bound is MiB-/thousands-scale (vs the GiB / hundred-million defaults),
/// so a decompression bomb or a huge declared count is cut off fast — yet each
/// bound stays generous enough that the well-formed seeds (a 1–6 step condor
/// chain / a small result bundle) still parse `Ok`, which is what proves the
/// `Ok` path is reachable rather than every input erroring.
#[must_use]
pub fn tight_limits() -> ResourceLimits {
    ResourceLimits {
        max_steps: 64,
        max_contracts_per_snapshot: 64,
        max_total_bytes: 1 << 20,        // 1 MiB
        max_file_bytes: 1 << 20,         // 1 MiB
        max_decompressed_bytes: 4 << 20, // 4 MiB
        max_manifest_bytes: 1 << 18,     // 256 KiB
        max_string_len: 64 << 10,        // 64 KiB (contract ids are short)
        max_rows_per_table: 100_000,
    }
}

/// The number of files a result bundle carries (manifest + four tables).
pub const BUNDLE_SECTIONS: usize = 5;

/// The bundle file names, in framing order — the order the seed encoder and
/// this decoder agree on.
pub const BUNDLE_FILES: [&str; BUNDLE_SECTIONS] = [
    "manifest.json",
    "fills.parquet",
    "equity_curve.parquet",
    "positions.parquet",
    "greeks_attribution.parquet",
];

/// Split fuzzer bytes into the five bundle files via a length-prefixed framing:
/// five `[u32 LE length][length bytes]` sections in [`BUNDLE_FILES`] order.
///
/// The decode is TOTAL — a short or oversized length is clamped to the bytes
/// that remain and never panics — so a mutated input keeps exploring the reader
/// rather than bouncing off a strict frame, and the clamp also bounds the write
/// to at most `data.len()` (no framing-side OOM). A well-formed seed produced by
/// the exact inverse encoder (`frame_bundle` in `tests/seed_corpus.rs`) round-
/// trips to the original five files; keep the two in sync.
#[must_use]
pub fn split_bundle_sections(data: &[u8]) -> [Vec<u8>; BUNDLE_SECTIONS] {
    let mut out: [Vec<u8>; BUNDLE_SECTIONS] = std::array::from_fn(|_| Vec::new());
    let mut pos = 0usize;
    for slot in &mut out {
        let len = read_u32_le(data, pos) as usize;
        pos = pos.saturating_add(4).min(data.len());
        let end = pos.saturating_add(len).min(data.len());
        *slot = data.get(pos..end).unwrap_or_default().to_vec();
        pos = end;
    }
    out
}

/// Read a little-endian `u32` at `pos`, zero-padding past the end (total).
fn read_u32_le(data: &[u8], pos: usize) -> u32 {
    let mut bytes = [0u8; 4];
    for (i, byte) in bytes.iter_mut().enumerate() {
        *byte = data.get(pos.saturating_add(i)).copied().unwrap_or(0);
    }
    u32::from_le_bytes(bytes)
}

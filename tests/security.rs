//! Security suite (docs/TESTING.md §12.1, docs/07 §8): drive every committed
//! adversarial Parquet fixture through [`ParquetFeed::open`] and assert it
//! returns the **documented typed `BacktestError`** with a **bounded resource
//! ceiling — never a panic, hang, or OOM**.
//!
//! # What each assertion proves
//!
//! - **Typed error, matched to the fixture.** Each test pattern-matches the
//!   exact `BacktestError` kind the boundary documents for that malformed
//!   input (see the mapping in each test name + the fixture doc comment).
//! - **No panic.** A test that reaches its assertion has, by construction, not
//!   panicked — `ParquetFeed::open` returned an `Err`, it did not unwind.
//! - **No hang.** Each test completes; there is no unbounded read or loop (the
//!   feed's ceilings and the parser's bounded row-group handling guarantee it).
//! - **No OOM.** The `ResourceLimits` ceilings cut materialisation off before
//!   any unbounded allocation. A **small fixture + a low ceiling** proves the
//!   cut-off without gigabyte files (documented in each ceiling test).
//!
//! # Fixture provenance
//!
//! The fixtures are deterministic generators committed as Rust source in
//! `tests/fixtures/adversarial/mod.rs` (not opaque binary blobs), per the repo
//! "no committed binary blob" convention (golden §4, Cargo.toml dev-dep note).
//! Each is a permanent regression that must keep passing.
//!
//! The captured-log credential test (§12.3, `simulator` feature) is out of
//! scope for #21 and lands with the simulator-feed hardening.

#[path = "common/mod.rs"]
mod common;

#[path = "fixtures/adversarial/mod.rs"]
mod adversarial;

use ironcondor::{BacktestError, ParquetFeed, ResourceLimits};

/// Crossed quote → the specific typed `CrossedQuote` variant the conversion
/// core raises (§12.1 groups it under `Conversion`).
#[test]
fn test_security_crossed_quote_yields_crossed_quote() {
    let Ok((_dir, path)) = adversarial::crossed_quote() else {
        panic!("crossed-quote fixture must build");
    };
    assert!(matches!(
        ParquetFeed::open(&path, &ResourceLimits::default()),
        Err(BacktestError::CrossedQuote { .. })
    ));
}

/// Negative strike → `Conversion` (integer-cents sign rejection at the feed).
#[test]
fn test_security_negative_strike_yields_conversion() {
    let Ok((_dir, path)) = adversarial::negative_strike() else {
        panic!("negative-strike fixture must build");
    };
    assert!(matches!(
        ParquetFeed::open(&path, &ResourceLimits::default()),
        Err(BacktestError::Conversion(_))
    ));
}

/// NaN analytic → `Conversion` (non-finite analytic rejected by the core).
#[test]
fn test_security_nan_analytic_yields_conversion() {
    let Ok((_dir, path)) = adversarial::nan_analytic() else {
        panic!("nan-analytic fixture must build");
    };
    assert!(matches!(
        ParquetFeed::open(&path, &ResourceLimits::default()),
        Err(BacktestError::Conversion(_))
    ));
}

/// Reversed-timestamp tape → `DataOutOfOrder`.
#[test]
fn test_security_out_of_order_ts_yields_data_out_of_order() {
    let Ok((_dir, path)) = adversarial::out_of_order_ts() else {
        panic!("out-of-order fixture must build");
    };
    assert!(matches!(
        ParquetFeed::open(&path, &ResourceLimits::default()),
        Err(BacktestError::DataOutOfOrder { .. })
    ));
}

/// Duplicate-timestamp tape → `DataOutOfOrder` (`ts` must strictly increase).
#[test]
fn test_security_duplicate_ts_yields_data_out_of_order() {
    let Ok((_dir, path)) = adversarial::duplicate_ts() else {
        panic!("duplicate-ts fixture must build");
    };
    assert!(matches!(
        ParquetFeed::open(&path, &ResourceLimits::default()),
        Err(BacktestError::DataOutOfOrder { .. })
    ));
}

/// Oversized rows (3 steps) + low `max_steps` → `TapeTooLarge { max_steps }`.
/// A 3-step fixture with a cap of 2 proves the cut-off without a huge file.
#[test]
fn test_security_oversized_steps_yields_tape_too_large() {
    let Ok((_dir, path)) = adversarial::oversized_steps() else {
        panic!("oversized-steps fixture must build");
    };
    let limits = ResourceLimits {
        max_steps: 2,
        ..ResourceLimits::default()
    };
    assert!(matches!(
        ParquetFeed::open(&path, &limits),
        Err(BacktestError::TapeTooLarge {
            limit: "max_steps",
            ..
        })
    ));
}

/// One snapshot of 4 contracts + low `max_contracts_per_snapshot` →
/// `TapeTooLarge { max_contracts_per_snapshot }`, cut off before the quote Vec
/// grows past the cap.
#[test]
fn test_security_oversized_contracts_yields_tape_too_large() {
    let Ok((_dir, path)) = adversarial::oversized_contracts() else {
        panic!("oversized-contracts fixture must build");
    };
    let limits = ResourceLimits {
        max_contracts_per_snapshot: 2,
        ..ResourceLimits::default()
    };
    assert!(matches!(
        ParquetFeed::open(&path, &limits),
        Err(BacktestError::TapeTooLarge {
            limit: "max_contracts_per_snapshot",
            ..
        })
    ));
}

/// Decompression bomb + low `max_decompressed_bytes` →
/// `TapeTooLarge { max_decompressed_bytes }`. The declared uncompressed
/// row-group size (many identical rows) blows past a 64 KiB ceiling while the
/// compressed file stays tiny; the guard fires before any decode.
#[test]
fn test_security_decompression_bomb_yields_tape_too_large() {
    let Ok((_dir, path)) = adversarial::decompression_bomb() else {
        panic!("decompression-bomb fixture must build");
    };
    let limits = ResourceLimits {
        max_decompressed_bytes: 64 * 1024,
        ..ResourceLimits::default()
    };
    assert!(matches!(
        ParquetFeed::open(&path, &limits),
        Err(BacktestError::TapeTooLarge {
            limit: "max_decompressed_bytes",
            ..
        })
    ));
}

/// Truncated footer → `Data` (metadata read fails cleanly, no panic).
#[test]
fn test_security_truncated_footer_yields_data() {
    let Ok((_dir, path)) = adversarial::truncated_footer() else {
        panic!("truncated-footer fixture must build");
    };
    assert!(matches!(
        ParquetFeed::open(&path, &ResourceLimits::default()),
        Err(BacktestError::Data(_))
    ));
}

/// Corrupt row group → `Data` (row-group decode fails cleanly, no panic).
#[test]
fn test_security_corrupt_row_group_yields_data() {
    let Ok((_dir, path)) = adversarial::corrupt_row_group() else {
        panic!("corrupt-row-group fixture must build");
    };
    assert!(matches!(
        ParquetFeed::open(&path, &ResourceLimits::default()),
        Err(BacktestError::Data(_))
    ));
}

/// Valid tape + a 1-byte `max_total_bytes` → `TapeTooLarge { max_total_bytes }`.
/// The first snapshot's byte estimate already crosses the cap; the tape is cut
/// off before it grows (the #21 net-new ceiling).
#[test]
fn test_security_over_max_total_bytes_yields_tape_too_large() {
    let Ok((_dir, path)) = adversarial::well_formed() else {
        panic!("well-formed fixture must build");
    };
    let limits = ResourceLimits {
        max_total_bytes: 1,
        ..ResourceLimits::default()
    };
    assert!(matches!(
        ParquetFeed::open(&path, &limits),
        Err(BacktestError::TapeTooLarge {
            limit: "max_total_bytes",
            ..
        })
    ));
}

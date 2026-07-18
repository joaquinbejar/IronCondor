//! Materialise the fuzz seed corpus (#52) from the committed adversarial
//! generators — the corpus is DERIVED, never committed.
//!
//! # Why this lives in the test crate
//!
//! The permanent regressions are the deterministic generators in
//! `tests/fixtures/adversarial/mod.rs` (source-form, the repo "no committed
//! binary blob" convention — golden §4, docs/TESTING.md §12.1). The `fuzz/`
//! crate is a SEPARATE workspace and cannot import the test crate, so the bridge
//! is this single **ignored** test: it invokes the same generators and writes
//! their bytes into `fuzz/corpus/<target>/`, giving the fuzzer good starting
//! coverage without committing any binary seed. `fuzz/seed_corpus.sh` runs it
//! (`cargo test --test seed_corpus -- --ignored`); a normal `cargo test` skips
//! it (it is `#[ignore]`), so no other job writes the corpus.
//!
//! The bundle framing here (`frame_bundle`) is the exact inverse of
//! `ironcondor_fuzz::split_bundle_sections`; the two ~5-line codecs are kept in
//! sync by cross-reference (they cannot share a module across the workspace
//! boundary).

#[path = "common/mod.rs"]
mod common;

#[path = "fixtures/adversarial/mod.rs"]
mod adversarial;

use std::path::{Path, PathBuf};

use tempfile::TempDir;

/// An adversarial generator: builds a malformed (or well-formed) input, holding
/// it alive in a [`TempDir`] and returning the path the parser consumes.
type Generator = fn() -> Result<(TempDir, PathBuf), String>;

/// The five bundle files, in framing order — mirrors
/// [`ironcondor_fuzz::BUNDLE_FILES`] (kept in sync by cross-reference).
const BUNDLE_FILES: [&str; 5] = [
    "manifest.json",
    "fills.parquet",
    "equity_curve.parquet",
    "positions.parquet",
    "greeks_attribution.parquet",
];

/// Create (and clear) `fuzz/corpus/<target>/` under the crate root and return it.
fn corpus_dir(target: &str) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fuzz/corpus")
        .join(target);
    std::fs::create_dir_all(&root).expect("create corpus dir");
    root
}

/// Encode a bundle directory as one seed blob: five `[u32 LE len][bytes]`
/// sections in [`BUNDLE_FILES`] order — the exact inverse of
/// `ironcondor_fuzz::split_bundle_sections` (keep the two in sync).
fn frame_bundle(bundle: &Path) -> Vec<u8> {
    let mut out = Vec::new();
    for name in BUNDLE_FILES {
        let bytes = std::fs::read(bundle.join(name)).expect("read bundle file");
        let len = u32::try_from(bytes.len()).expect("bundle file fits u32");
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&bytes);
    }
    out
}

#[test]
#[ignore = "writes fuzz/corpus/*; run via fuzz/seed_corpus.sh (cargo test --test seed_corpus -- --ignored)"]
fn materialise_seed_corpus() {
    // fuzz_csv_feed — the CSV feed input is a directory of per-step files; each
    // file is a valid single-snapshot CSV seed on its own.
    let csv_gens: &[(&str, Generator)] = &[
        ("well_formed", adversarial::csv_well_formed),
        ("crossed_quote", adversarial::csv_crossed_quote),
        ("negative_strike", adversarial::csv_negative_strike),
        ("zero_strike", adversarial::csv_zero_strike),
        ("nan_analytic", adversarial::csv_nan_analytic),
        ("dollar_float_money", adversarial::csv_dollar_float_money),
        ("oversized_steps", adversarial::csv_oversized_steps),
    ];
    let out = corpus_dir("fuzz_csv_feed");
    let mut csv_seeds = 0usize;
    for (name, generate) in csv_gens {
        let (_dir, dir_path) = generate().expect("csv generator builds");
        for entry in std::fs::read_dir(&dir_path).expect("read csv dir") {
            let entry = entry.expect("csv dir entry");
            if entry.file_type().expect("csv entry type").is_file() {
                let bytes = std::fs::read(entry.path()).expect("read csv file");
                let seed = out.join(format!("{name}__{}", entry.file_name().to_string_lossy()));
                std::fs::write(seed, bytes).expect("write csv seed");
                csv_seeds += 1;
            }
        }
    }

    // fuzz_parquet_feed — each generator yields one `.parquet` file.
    let parquet_gens: &[(&str, Generator)] = &[
        ("well_formed", adversarial::well_formed),
        ("crossed_quote", adversarial::crossed_quote),
        ("negative_strike", adversarial::negative_strike),
        ("nan_analytic", adversarial::nan_analytic),
        ("out_of_order_ts", adversarial::out_of_order_ts),
        ("duplicate_ts", adversarial::duplicate_ts),
        ("oversized_steps", adversarial::oversized_steps),
        ("oversized_contracts", adversarial::oversized_contracts),
        ("decompression_bomb", adversarial::decompression_bomb),
        ("truncated_footer", adversarial::truncated_footer),
        ("corrupt_row_group", adversarial::corrupt_row_group),
        // The fuzzer-found, minimised parser-panic regressions (#52).
        (
            "malformed_arrow_schema",
            adversarial::parquet_malformed_arrow_schema,
        ),
        (
            "malformed_byte_range",
            adversarial::parquet_malformed_byte_range,
        ),
    ];
    let out = corpus_dir("fuzz_parquet_feed");
    for (name, generate) in parquet_gens {
        let (_dir, file_path) = generate().expect("parquet generator builds");
        let bytes = std::fs::read(&file_path).expect("read parquet file");
        std::fs::write(out.join(format!("{name}.parquet")), bytes).expect("write parquet seed");
    }

    // fuzz_bundle_readback — each generator yields a bundle DIRECTORY; frame its
    // five files into one seed blob the target decodes back.
    let bundle_gens: &[(&str, Generator)] = &[
        ("well_formed", adversarial::bundle_well_formed),
        ("wrong_schema_tag", adversarial::bundle_wrong_schema_tag),
        (
            "oversized_row_counts",
            adversarial::bundle_oversized_row_counts,
        ),
        (
            "truncated_parquet_footer",
            adversarial::bundle_truncated_parquet_footer,
        ),
        (
            "non_round_trippable_contract_id",
            adversarial::bundle_non_round_trippable_contract_id,
        ),
        (
            "missing_required_field",
            adversarial::bundle_missing_required_field,
        ),
        ("bad_input_hash", adversarial::bundle_bad_input_hash),
        // The fuzzer-found, minimised parser-panic regressions (#52).
        (
            "malformed_arrow_schema_table",
            adversarial::bundle_malformed_arrow_schema_table,
        ),
        (
            "malformed_byte_range_table",
            adversarial::bundle_malformed_byte_range_table,
        ),
    ];
    let out = corpus_dir("fuzz_bundle_readback");
    for (name, generate) in bundle_gens {
        let (_dir, bundle) = generate().expect("bundle generator builds");
        std::fs::write(out.join(name), frame_bundle(&bundle)).expect("write bundle seed");
    }

    println!(
        "seed corpus materialised: {csv_seeds} csv, {} parquet, {} bundle",
        parquet_gens.len(),
        bundle_gens.len()
    );
}

#![no_main]

use libfuzzer_sys::fuzz_target;

use ironcondor::{DataFeed, ParquetFeed};
use ironcondor_fuzz::tight_limits;

// The Parquet feed input is one columnar file. Each iteration writes the fuzzer
// bytes as a `.parquet` file into a fresh tempdir and drives the real public
// parser: `ParquetFeed::open` followed by a full drain. Same invariant-by-type
// and no-panic/hang/OOM rationale as `fuzz_csv_feed` (see fuzz/README.md); the
// tight `max_decompressed_bytes` bounds the decompression-bomb surface so the
// fuzzer probes the footer / schema / row-group decode paths, not huge allocs.
fuzz_target!(|data: &[u8]| {
    let limits = tight_limits();
    let Ok(dir) = tempfile::tempdir() else {
        return;
    };
    let path = dir.path().join("chain.parquet");
    if std::fs::write(&path, data).is_err() {
        return;
    }
    if let Ok(mut feed) = ParquetFeed::open(&path, &limits) {
        while let Ok(Some(_snapshot)) = feed.next() {}
    }
});

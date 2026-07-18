#![no_main]

use libfuzzer_sys::fuzz_target;

use ironcondor::{CsvFeed, DataFeed};
use ironcondor_fuzz::tight_limits;

// The CSV feed input is a DIRECTORY of per-step chain files. Each iteration
// interprets the fuzzer bytes as the content of ONE per-step file written into
// a fresh tempdir, then drives the real public parser: `CsvFeed::open` followed
// by a full drain. Writing one file reaches the actual byte-parser
// (`parse_csv_snapshot`: header + per-cell parse + the per-file ceilings); the
// per-iteration filesystem write is the accepted throughput cost of a
// path-based parser (see fuzz/README.md).
//
// The "only Ok(valid parse) or Err(BacktestError)" invariant is enforced BY THE
// TYPE — `open`/`next` return `Result<_, BacktestError>`, so no other outcome is
// representable. What is left to prove is the absence of a panic / hang / OOM,
// which libFuzzer + the tight `ResourceLimits` + the CI `-rss_limit_mb` /
// `-timeout` / `-malloc_limit_mb` bounds establish for any input.
fuzz_target!(|data: &[u8]| {
    let limits = tight_limits();
    let Ok(dir) = tempfile::tempdir() else {
        return;
    };
    if std::fs::write(dir.path().join("step_000.csv"), data).is_err() {
        return;
    }
    if let Ok(mut feed) = CsvFeed::open(dir.path(), &limits) {
        // Drain fully: `next` is a pure in-memory read, but draining exercises
        // the whole materialised tape.
        while let Ok(Some(_snapshot)) = feed.next() {}
    }
});

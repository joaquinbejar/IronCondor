#![no_main]

use libfuzzer_sys::fuzz_target;

use ironcondor::read_bundle;
use ironcondor_fuzz::{BUNDLE_FILES, split_bundle_sections, tight_limits};

// The bundle reader input is a DIRECTORY (manifest.json + four Parquet tables).
// Each iteration splits the fuzzer bytes into those five files via the
// length-prefixed framing (`ironcondor_fuzz::split_bundle_sections`) and writes
// them into a fresh bundle dir, so the fuzzer reaches PAST "manifest missing"
// into the real read-back gate: schema-tag → manifest-field → table-decode →
// row_counts → referenced-hash → contract-id round-trip. Then it drives the
// real public `read_bundle`.
//
// Same invariant-by-type and no-panic/hang/OOM rationale as the feed targets
// (see fuzz/README.md). The well-formed bundle seed decodes back to a valid
// `ironcondor.bundle.v1` and returns `Ok(ValidatedBundle)` (its referenced
// input is simply skipped as unreachable at replay), which is how the `Ok` path
// is confirmed reachable here.
fuzz_target!(|data: &[u8]| {
    let limits = tight_limits();
    let Ok(dir) = tempfile::tempdir() else {
        return;
    };
    let sections = split_bundle_sections(data);
    for (name, bytes) in BUNDLE_FILES.iter().zip(sections.iter()) {
        if std::fs::write(dir.path().join(name), bytes).is_err() {
            return;
        }
    }
    let _ = read_bundle(dir.path(), &limits);
});

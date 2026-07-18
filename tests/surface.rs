//! Public-surface freeze gates (#49) — the Rust re-export surface and the
//! runtime configuration env-var surface.
//!
//! # What this pins
//!
//! Two of the four frozen SemVer surfaces
//! ([docs/SEMVER.md §"What counts as a public surface"](../docs/SEMVER.md)):
//!
//! - **Rust public API** — every `pub mod` and every `pub use` leaf declared in
//!   [`src/lib.rs`](../src/lib.rs), annotated with its cfg feature (or
//!   `default`). This is the documented source of truth for the Rust surface:
//!   internal items not re-exported from `lib.rs` are not part of the contract.
//!   The extracted list is compared against the committed snapshot
//!   [`tests/surface/rust-public-api.txt`](surface/rust-public-api.txt). A
//!   re-export **add / remove / rename** changes the snapshot and **fails CI**
//!   unless the snapshot is regenerated in the same PR — the visible SemVer
//!   event.
//! - **Configuration env vars** — the set of runtime environment variables the
//!   crate reads (`env::var` / `env::var_os` under `src/`). Only `API_URL` (the
//!   OptionChain-Simulator base-URL override) is a behaviour-affecting runtime
//!   knob; a new `env::var` read in `src/` fails this gate until the pinned set
//!   is updated. Test/build-only vars (`BLESS`, `SIM_LIVE`, `PYO3_PYTHON`) live
//!   in `tests/` and `scripts/`, outside `src/`, and are excluded by
//!   construction.
//!
//! # Why a committed snapshot on the stable toolchain
//!
//! `cargo-public-api` and `cargo-semver-checks` both derive their diff from
//! **rustdoc JSON**, a nightly-only, unstable-format output; CI pins the stable
//! toolchain, and a quarter-long freeze gate must not red-CI on nightly format
//! churn. `cargo-semver-checks` is additionally **directional** and honours
//! Cargo's `0.x` "breaking allowed" semantics, so at the current `0.0.1` it
//! would pass a breaking change rather than gate it. The documented source of
//! truth is precisely the `src/lib.rs` re-exports, so a deterministic snapshot
//! extracted from that file on the pinned stable toolchain is both the most
//! robust and the most faithful gate ([docs/SEMVER.md](../docs/SEMVER.md)).
//!
//! # Regenerating the snapshot (BLESS)
//!
//! ```bash
//! BLESS=1 cargo test --test surface
//! ```
//!
//! Mirrors the golden convention ([`tests/golden.rs`](golden.rs)): `BLESS=1`
//! rewrites `tests/surface/rust-public-api.txt`; without it the test compares
//! and fails on drift.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

/// The crate root (the directory holding `Cargo.toml`).
fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Whether `BLESS=1` is set — regenerate the committed snapshot instead of
/// comparing (the repo-wide golden convention).
fn blessing() -> bool {
    std::env::var_os("BLESS").is_some_and(|value| value == "1")
}

/// The committed Rust-surface snapshot path.
fn snapshot_path() -> PathBuf {
    crate_root().join("tests/surface/rust-public-api.txt")
}

/// Extract the feature name from a `#[cfg(feature = "X")]` attribute line, if
/// present.
fn feature_of(line: &str) -> Option<String> {
    let start = line.find('"')?;
    let rest = line.get(start + 1..)?;
    let end = rest.find('"')?;
    rest.get(..end).map(str::to_string)
}

/// Resolve one re-exported leaf to its **exported** identifier, honouring a
/// `Source as Alias` rename (the exported name is `Alias`). Returns the module
/// path segment (may be empty) and the exported name.
fn split_leaf(leaf: &str) -> Option<(String, String)> {
    let leaf = leaf.trim();
    if leaf.is_empty() {
        return None;
    }
    if let Some((source, alias)) = leaf.split_once(" as ") {
        let exported = alias.trim().to_string();
        let module = source
            .trim()
            .rsplit_once("::")
            .map_or(String::new(), |(m, _)| m.to_string());
        Some((module, exported))
    } else {
        // A leaf inside a group is a bare identifier; a single import may carry
        // a `::`-qualified path handled by the caller. Here we return the leaf
        // as the exported name with no module (the caller supplies the prefix).
        Some((String::new(), leaf.to_string()))
    }
}

/// Parse a complete `pub use ...;` statement (already joined across lines) into
/// its exported `module::name` paths.
fn parse_use(stmt: &str) -> Vec<String> {
    let body = stmt
        .trim()
        .strip_prefix("pub use ")
        .unwrap_or(stmt)
        .trim()
        .trim_end_matches(';')
        .trim();
    if let Some((prefix_part, leaves_part)) = body.split_once('{') {
        let prefix = prefix_part.trim().trim_end_matches("::").trim();
        let leaves = leaves_part.trim().trim_end_matches('}');
        leaves
            .split(',')
            .filter_map(|leaf| split_leaf(leaf).map(|(_, name)| name))
            .filter(|name| !name.is_empty())
            .map(|name| format!("{prefix}::{name}"))
            .collect()
    } else {
        // Single import: `a::b::C` or `a::b::C as D`.
        if let Some((source, alias)) = body.split_once(" as ") {
            let exported = alias.trim();
            let module = source
                .trim()
                .rsplit_once("::")
                .map_or(String::new(), |(m, _)| m.to_string());
            if module.is_empty() {
                vec![exported.to_string()]
            } else {
                vec![format!("{module}::{exported}")]
            }
        } else {
            vec![body.to_string()]
        }
    }
}

/// Extract the public re-export surface declared in `src/lib.rs`: every
/// `pub mod` and every `pub use` leaf, each tagged with its cfg feature (or
/// `default`). Returns sorted `"<feature> <kind> <path>"` lines.
fn extract_surface(src: &str) -> Vec<String> {
    let mut out: BTreeSet<String> = BTreeSet::new();
    let mut pending_feature: Option<String> = None;
    let mut lines = src.lines();
    while let Some(raw) = lines.next() {
        let line = raw.trim();
        if line.starts_with("#[cfg(feature") {
            if let Some(feature) = feature_of(line) {
                pending_feature = Some(feature);
            }
            continue;
        }
        // Other attributes, doc comments, comments, blanks: keep any pending
        // feature (a `#[cfg(feature)]` attaches to the next item) and skip.
        if line.is_empty() || line.starts_with('#') || line.starts_with("//") {
            continue;
        }
        let feature = pending_feature
            .clone()
            .unwrap_or_else(|| "default".to_string());
        if let Some(rest) = line.strip_prefix("pub mod ") {
            let name = rest.trim_end_matches(';').trim();
            out.insert(format!("{feature} mod {name}"));
            pending_feature = None;
            continue;
        }
        if line.starts_with("pub use ") {
            // Accumulate a possibly multi-line group until the terminating `;`.
            let mut stmt = String::from(line);
            while !stmt.contains(';') {
                match lines.next() {
                    Some(next) => {
                        stmt.push(' ');
                        stmt.push_str(next.trim());
                    }
                    None => break,
                }
            }
            for path in parse_use(&stmt) {
                out.insert(format!("{feature} use {path}"));
            }
            pending_feature = None;
            continue;
        }
        // Any other item ends the reach of a pending feature.
        pending_feature = None;
    }
    out.into_iter().collect()
}

/// Recursively read every `.rs` file under `dir`, invoking `f` with its text.
fn visit_rs(dir: &Path, f: &mut impl FnMut(&str)) {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            visit_rs(&path, f);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs")
            && let Ok(content) = fs::read_to_string(&path)
        {
            f(&content);
        }
    }
}

/// The set of runtime env-var names read via `env::var(...)` / `env::var_os(...)`
/// anywhere under `src/`.
fn scan_runtime_env_vars(src_dir: &Path) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    visit_rs(src_dir, &mut |content| {
        for marker in ["env::var(", "env::var_os("] {
            let mut rest = content;
            while let Some(idx) = rest.find(marker) {
                let after = rest.get(idx + marker.len()..).unwrap_or("");
                if let Some(q1) = after.find('"') {
                    let tail = after.get(q1 + 1..).unwrap_or("");
                    if let Some(q2) = tail.find('"')
                        && let Some(name) = tail.get(..q2)
                    {
                        names.insert(name.to_string());
                    }
                }
                rest = after;
            }
        }
    });
    names
}

#[test]
fn test_rust_public_api_surface_matches_snapshot() {
    let lib = crate_root().join("src/lib.rs");
    let src = match fs::read_to_string(&lib) {
        Ok(src) => src,
        Err(err) => panic!("cannot read {}: {err}", lib.display()),
    };
    let current = extract_surface(&src);
    let rendered = format!("{}\n", current.join("\n"));
    let path = snapshot_path();

    if blessing() {
        if let Some(parent) = path.parent()
            && let Err(err) = fs::create_dir_all(parent)
        {
            panic!("BLESS must create {}: {err}", parent.display());
        }
        if let Err(err) = fs::write(&path, &rendered) {
            panic!("BLESS must write {}: {err}", path.display());
        }
        return;
    }

    let committed = match fs::read_to_string(&path) {
        Ok(committed) => committed,
        Err(err) => panic!(
            "missing Rust-surface snapshot {} ({err}); regenerate with \
             `BLESS=1 cargo test --test surface`",
            path.display()
        ),
    };

    if committed != rendered {
        let old: BTreeSet<&str> = committed.lines().collect();
        let new: BTreeSet<&str> = rendered.lines().collect();
        let removed: Vec<&str> = old.difference(&new).copied().collect();
        let added: Vec<&str> = new.difference(&old).copied().collect();
        panic!(
            "Rust public-API surface drift — this is a SemVer event.\n  \
             removed (no longer exported): {removed:?}\n  \
             added (new surface):          {added:?}\n\
             If intended, regenerate the snapshot with `BLESS=1 cargo test \
             --test surface`, record the SemVer bump, and make the diff visible \
             in the PR (docs/SEMVER.md §\"Surface-freeze gates\")."
        );
    }
}

#[test]
fn test_runtime_env_vars_are_pinned() {
    let expected: BTreeSet<String> = ["API_URL"].into_iter().map(str::to_string).collect();
    let found = scan_runtime_env_vars(&crate_root().join("src"));
    assert_eq!(
        found, expected,
        "runtime configuration env-var surface drift — this is a config-surface \
         SemVer event. Update the pinned set in tests/surface.rs and record the \
         bump (docs/SEMVER.md §\"v1.0 commitments\"). Only `API_URL` (the \
         simulator base-URL override) is a behaviour-affecting runtime env var."
    );
}

#[cfg(test)]
mod extractor_unit_tests {
    use super::{extract_surface, parse_use, scan_runtime_env_vars};
    use std::path::PathBuf;

    #[test]
    fn test_parse_use_single_qualified_path() {
        assert_eq!(
            parse_use("pub use error::BacktestError;"),
            ["error::BacktestError"]
        );
    }

    #[test]
    fn test_parse_use_group_expands_each_leaf() {
        assert_eq!(
            parse_use("pub use execution::{ExecutionModel, FillGroup, NaiveFill};"),
            [
                "execution::ExecutionModel",
                "execution::FillGroup",
                "execution::NaiveFill"
            ]
        );
    }

    #[test]
    fn test_parse_use_honours_rename_alias() {
        assert_eq!(
            parse_use("pub use error::BacktestError as Probe;"),
            ["error::Probe"]
        );
        assert_eq!(
            parse_use("pub use config::{TouchSize as Probe};"),
            ["config::Probe"]
        );
    }

    #[test]
    fn test_extract_surface_tags_cfg_feature_and_default() {
        let src = "\
pub mod engine;
#[cfg(feature = \"python\")]
pub mod python;
pub use error::BacktestError;
#[cfg(feature = \"orderbook\")]
pub use execution::RealisticFill;
";
        let out = extract_surface(src);
        assert!(out.contains(&"default mod engine".to_string()));
        assert!(out.contains(&"python mod python".to_string()));
        assert!(out.contains(&"default use error::BacktestError".to_string()));
        assert!(out.contains(&"orderbook use execution::RealisticFill".to_string()));
    }

    #[test]
    fn test_scan_runtime_env_vars_finds_api_url_only() {
        let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
        let found = scan_runtime_env_vars(&src);
        assert!(
            found.contains("API_URL"),
            "expected API_URL among {found:?}"
        );
    }
}

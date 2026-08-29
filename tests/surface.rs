//! Public-surface freeze gates (#49) — the Rust re-export surface and the
//! runtime configuration env-var surface.
//!
//! # What this pins
//!
//! Two of the four frozen SemVer surfaces
//! ([docs/SEMVER.md §"What counts as a public surface"](../docs/SEMVER.md)):
//!
//! - **Rust public API** — the whole surface reachable through the `pub mod`
//!   tree rooted at [`src/lib.rs`](../src/lib.rs): every `pub mod`, every
//!   `pub use` leaf, and every `pub` item (`fn` / `struct` / `enum` / `trait` /
//!   `type` / `const` / `static`) declared at module level in `lib.rs` **and in
//!   each module transitively reachable through a `pub mod`** (resolved from
//!   `src/X.rs` / `src/X/mod.rs`), each annotated with its effective cfg feature
//!   (or `default`). Because a `pub mod` re-exports every `pub` item beneath it
//!   under `ironcondor::<path>`, those items are part of the contract even when
//!   they are not re-exported at the crate root — so the snapshot tracks them
//!   too (that is the #44 gap the earlier lib.rs-only extractor missed).
//!   `pub(crate)` / `pub(super)` items are excluded (not reachable). The
//!   extracted list is compared against the committed snapshot
//!   [`tests/surface/rust-public-api.txt`](surface/rust-public-api.txt). An
//!   **add / remove / rename** anywhere in that surface changes the snapshot and
//!   **fails CI** unless the snapshot is regenerated in the same PR — the
//!   visible SemVer event.
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

/// A file-based `pub mod NAME;` child to recurse into: its exported name, the
/// path prefix its own items receive, and the cfg feature it inherits.
struct ChildMod {
    /// The module identifier (resolves to `<dir>/NAME.rs` or `<dir>/NAME/mod.rs`).
    name: String,
    /// The `path::` prefix prepended to every item declared inside the module.
    prefix: String,
    /// The inherited feature gate (`None` == `default`).
    feature: Option<String>,
}

/// Combine an inherited module feature with an item's own `#[cfg(feature)]`:
/// either alone when only one is present, the shared value when equal, else a
/// deterministic sorted `"a+b"` join (an item reachable only with BOTH gates).
fn combine_feature(base: Option<&str>, item: Option<&str>) -> String {
    match (base, item) {
        (None, None) => "default".to_string(),
        (Some(f), None) | (None, Some(f)) => f.to_string(),
        (Some(a), Some(b)) if a == b => a.to_string(),
        (Some(a), Some(b)) => {
            let mut parts = [a, b];
            parts.sort_unstable();
            format!("{}+{}", parts[0], parts[1])
        }
    }
}

/// Map a rendered `effective` feature back to the `Option` a child module
/// inherits (`"default"` == none).
fn feature_to_opt(effective: &str) -> Option<String> {
    (effective != "default").then(|| effective.to_string())
}

/// Determine the `(kind, name)` of a top-level `pub` item line, if it is one of
/// the tracked item kinds. The `const fn` / `async fn` forms are matched before
/// the bare `const` so the function name (not `"fn"`) is captured. Returns
/// `None` for any other line. `pub(crate)` / `pub(super)` never match — the
/// prefixes carry a trailing space, which `pub(` lacks.
fn item_kind_and_name(line: &str) -> Option<(&'static str, String)> {
    const RULES: [(&str, &str); 10] = [
        ("pub const fn ", "fn"),
        ("pub async fn ", "fn"),
        ("pub fn ", "fn"),
        ("pub struct ", "struct"),
        ("pub enum ", "enum"),
        ("pub trait ", "trait"),
        ("pub type ", "type"),
        ("pub const ", "const"),
        ("pub static ", "static"),
        ("pub union ", "union"),
    ];
    for (prefix, kind) in RULES {
        if let Some(rest) = line.strip_prefix(prefix) {
            let name = leading_ident(rest);
            if !name.is_empty() {
                return Some((kind, name));
            }
        }
    }
    None
}

/// The leading Rust identifier of `s` (stops at the first `(`, `<`, `:`, `=`,
/// `;`, `{`, or whitespace) — the item name after its keyword.
fn leading_ident(s: &str) -> String {
    s.chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// Remove up to one 4-space indentation level (or a single leading tab) from a
/// line — used to dedent an inline `pub mod { … }` body before re-extracting it.
fn dedent_one(line: &str) -> &str {
    line.strip_prefix("    ")
        .or_else(|| line.strip_prefix('\t'))
        .unwrap_or(line)
}

/// Capture the body of an inline `pub mod X { … }` opening at `lines[start]`: the
/// lines through the matching **column-0** closing brace, each dedented one
/// level, joined with newlines. rustfmt guarantees the module's own closing
/// brace is the first line that is exactly `}` at column 0 (nested `fn` / `impl`
/// braces close indented). Returns the body text and the index just past `}`.
fn capture_inline_body(lines: &[&str], start: usize) -> (String, usize) {
    let mut body = String::new();
    let mut i = start + 1;
    while i < lines.len() {
        if lines[i] == "}" {
            i += 1;
            break;
        }
        body.push_str(dedent_one(lines[i]));
        body.push('\n');
        i += 1;
    }
    (body, i)
}

/// Capture the body of a `pub enum` whose declaration opens at `lines[start]`,
/// returning the dedented body and the index just past its closing brace, or
/// `None` when the enum has **no** variants to scan.
///
/// Two shapes make this more than [`capture_inline_body`]:
///
/// - **The brace need not be on the declaration line.** rustfmt moves it to its
///   own line as soon as a generic parameter list or a `where` clause wraps
///   (`pub enum Wrapped<T>\nwhere\n    T: Clone,\n{`). Requiring `{` on the
///   `pub enum` line recorded **zero** variants for such an enum, silently — the
///   enum's own name still appeared, so nothing looked broken.
/// - **An empty enum closes on its own line** (`pub enum Never {}`). Scanning
///   from the next line for a column-0 `}` would run past it and swallow every
///   item up to the next one, silently deleting unrelated entries from the
///   snapshot.
///
/// The brace search is bounded to the declaration's own continuation lines —
/// generics, `where` clauses — and stops at anything that cannot be one, so a
/// malformed input yields `None` rather than consuming the rest of the file.
fn capture_enum_body(lines: &[&str], start: usize) -> Option<(String, usize)> {
    // Find the line carrying the opening brace: the declaration line itself, or
    // one of its continuations.
    let mut brace = start;
    while brace < lines.len() && !lines[brace].contains('{') {
        let line = lines[brace].trim();
        let continuation = brace == start
            || line.starts_with("where")
            || line.ends_with(',')
            || line.ends_with('>')
            || line.ends_with('+');
        if !continuation {
            return None;
        }
        brace += 1;
    }
    let opening = lines.get(brace)?;
    // `pub enum Never {}` — no variants, and the body must not be scanned.
    if opening.contains('}') {
        return Some((String::new(), brace + 1));
    }
    Some(capture_inline_body(lines, brace))
}

/// The variants of an enum body captured by [`capture_enum_body`] (one dedent
/// already applied, so a variant sits at column 0 and a struct-variant's own
/// fields stay indented), each paired with its **effective feature**.
///
/// A variant is a column-0 line whose first character is an ASCII uppercase
/// letter — which is what every Rust variant is, and what no attribute (`#[…]`),
/// doc comment (`///`), field, or closing brace can be. The leading identifier
/// stops at `(`, `{`, `=`, `,` or whitespace, so tuple, struct and
/// discriminant forms all yield the bare name.
///
/// A variant may carry its **own** `#[cfg(feature = "…")]` on top of the enum's
/// — `DataSourceSpec::Simulator` and `FeedKind::Simulator` both do — so the same
/// `pending_feature` rule [`extract_module`] applies to items applies here: a
/// cfg attaches to the next variant and combines with `base`. Without it the
/// snapshot would claim a gated variant exists by default and, in the direction
/// that actually matters, moving an existing variant BEHIND a `#[cfg]` (breaking
/// under default features) would not move the snapshot at all.
fn enum_variants(body: &str, base: &str) -> Vec<(String, String)> {
    let base_feature = feature_to_opt(base);
    let mut pending: Option<String> = None;
    let mut out = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#[cfg(feature") {
            if let Some(feature) = feature_of(trimmed) {
                pending = Some(feature);
            }
            continue;
        }
        // A struct-variant's fields are indented; blank, attribute and comment
        // lines keep a pending cfg, which attaches to the next variant.
        if line.starts_with(|c: char| c.is_whitespace())
            || trimmed.is_empty()
            || trimmed.starts_with('#')
            || trimmed.starts_with("//")
        {
            continue;
        }
        if !line.starts_with(|c: char| c.is_ascii_uppercase()) {
            // Punctuation (a closing brace) ends a pending cfg's reach.
            pending = None;
            continue;
        }
        let name = leading_ident(line);
        if !name.is_empty() {
            out.push((
                combine_feature(base_feature.as_deref(), pending.as_deref()),
                name,
            ));
        }
        pending = None;
    }
    out
}

/// Extract the public surface declared **directly** in one module's text.
///
/// `prefix` is the module's path prefix (`""` for `lib.rs`, `"engine::"` for
/// `src/engine/mod.rs`, …) and `base_feature` the cfg feature it inherits.
/// Records every `pub mod`, `pub use` leaf, and tracked `pub` item, each tagged
/// with its effective feature, **plus every public enum's variants** (an added
/// variant is a breaking change for a downstream exhaustive `match`, so it has
/// to be visible in the diff). Inline `pub mod X { … }` bodies are extracted in
/// place (recursively), while file-based `pub mod X;` declarations are returned
/// as [`ChildMod`]s for the driver to read and recurse into. Apart from those
/// variants, only column-0 (module-level) lines are items, so struct fields and
/// `impl` methods — always indented in rustfmt'd code — are excluded. Returns the sorted
/// `"<feature> <kind> <path>"` lines plus the file-based children.
fn extract_module(
    text: &str,
    prefix: &str,
    base_feature: Option<&str>,
) -> (BTreeSet<String>, Vec<ChildMod>) {
    let mut out: BTreeSet<String> = BTreeSet::new();
    let mut children: Vec<ChildMod> = Vec::new();
    let mut pending_feature: Option<String> = None;
    let lines: Vec<&str> = text.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let raw = lines[i];
        let line = raw.trim();

        // A `#[cfg(feature = "X")]` attaches to the NEXT item.
        if line.starts_with("#[cfg(feature") {
            if let Some(feature) = feature_of(line) {
                pending_feature = Some(feature);
            }
            i += 1;
            continue;
        }
        // Only column-0 lines are module-level items; anything indented is a
        // field / method / item body. Blank / attribute / comment lines keep any
        // pending feature (a cfg attaches to the next item) and are skipped.
        let indented = raw.starts_with(|c: char| c.is_whitespace());
        if indented || line.is_empty() || line.starts_with('#') || line.starts_with("//") {
            i += 1;
            continue;
        }

        let effective = combine_feature(base_feature, pending_feature.as_deref());

        // `pub mod` — inline (`{`) body extracted in place, or file (`;`) child.
        if let Some(rest) = line.strip_prefix("pub mod ") {
            let name = leading_ident(rest);
            if !name.is_empty() {
                out.insert(format!("{effective} mod {prefix}{name}"));
                let child_prefix = format!("{prefix}{name}::");
                let child_feature = feature_to_opt(&effective);
                if line.contains('{') {
                    let (body, next) = capture_inline_body(&lines, i);
                    let (child_lines, child_children) =
                        extract_module(&body, &child_prefix, child_feature.as_deref());
                    out.extend(child_lines);
                    children.extend(child_children);
                    i = next;
                } else {
                    children.push(ChildMod {
                        name,
                        prefix: child_prefix,
                        feature: child_feature,
                    });
                    i += 1;
                }
                pending_feature = None;
                continue;
            }
        }

        // `pub use` — accumulate a possibly multi-line group until `;`.
        if line.starts_with("pub use ") {
            let mut stmt = String::from(line);
            while !stmt.contains(';') && i + 1 < lines.len() {
                i += 1;
                stmt.push(' ');
                stmt.push_str(lines[i].trim());
            }
            for path in parse_use(&stmt) {
                out.insert(format!("{effective} use {prefix}{path}"));
            }
            pending_feature = None;
            i += 1;
            continue;
        }

        // Any other tracked `pub` item (fn / struct / enum / trait / type / …).
        if let Some((kind, name)) = item_kind_and_name(line) {
            out.insert(format!("{effective} {kind} {prefix}{name}"));
            // An enum's VARIANTS are part of the surface too: adding one is a
            // breaking change for a downstream exhaustive `match`, and tracking
            // only the enum's name made exactly that change invisible to this
            // gate (#117 added `StrategySpec::Legs` without moving the snapshot
            // by one line). Variants are indented, so the module-level scan
            // above skips them; capture the body and record them explicitly.
            if kind == "enum"
                && let Some((body, next)) = capture_enum_body(&lines, i)
            {
                for (variant_feature, variant) in enum_variants(&body, &effective) {
                    out.insert(format!(
                        "{variant_feature} variant {prefix}{name}::{variant}"
                    ));
                }
                pending_feature = None;
                i = next;
                continue;
            }
            pending_feature = None;
            i += 1;
            continue;
        }

        // A column-0 line starting with a letter (a non-pub item — `impl`, a
        // private `fn`, a macro invocation) ends the reach of a pending feature;
        // punctuation-led continuation lines (`)]`, `}`, …) leave it intact.
        if raw.starts_with(|c: char| c.is_ascii_alphabetic()) {
            pending_feature = None;
        }
        i += 1;
    }
    (out, children)
}

/// Extract the public surface declared **directly** in one module's text (no
/// recursion into file-based children). The extractor unit tests use this; the
/// freeze gate itself uses [`extract_tree`].
fn extract_surface(src: &str) -> Vec<String> {
    extract_module(src, "", None).0.into_iter().collect()
}

/// Resolve `pub mod name;` under `dir` to `(module file, submodule dir)`:
/// `dir/name.rs` or `dir/name/mod.rs`, with the module's own submodules resolved
/// against `dir/name`.
fn resolve_module(dir: &Path, name: &str) -> Option<(PathBuf, PathBuf)> {
    let sub_dir = dir.join(name);
    let as_file = dir.join(format!("{name}.rs"));
    if as_file.is_file() {
        return Some((as_file, sub_dir));
    }
    let as_mod = sub_dir.join("mod.rs");
    if as_mod.is_file() {
        return Some((as_mod, sub_dir));
    }
    None
}

/// Extract the crate's entire public surface: `src/lib.rs` plus every module
/// transitively reachable through a `pub mod`, each item tagged with its
/// effective cfg feature. Returns sorted `"<feature> <kind> <path>"` lines.
fn extract_tree(src_dir: &Path) -> Vec<String> {
    let lib = src_dir.join("lib.rs");
    let src = match fs::read_to_string(&lib) {
        Ok(src) => src,
        Err(err) => panic!("cannot read {}: {err}", lib.display()),
    };
    let mut out: BTreeSet<String> = BTreeSet::new();
    let (lines, children) = extract_module(&src, "", None);
    out.extend(lines);
    // Each stack entry pairs a child module with the directory its file resolves
    // against (children of `lib.rs` live directly under `src/`).
    let mut stack: Vec<(ChildMod, PathBuf)> = children
        .into_iter()
        .map(|child| (child, src_dir.to_path_buf()))
        .collect();
    while let Some((child, dir)) = stack.pop() {
        let Some((file, sub_dir)) = resolve_module(&dir, &child.name) else {
            // A `pub mod` with no on-disk file has no textual surface — skip it
            // rather than fail (every mod in this crate does resolve).
            continue;
        };
        let text = match fs::read_to_string(&file) {
            Ok(text) => text,
            Err(err) => panic!("cannot read {}: {err}", file.display()),
        };
        let (lines, grandchildren) = extract_module(&text, &child.prefix, child.feature.as_deref());
        out.extend(lines);
        for grandchild in grandchildren {
            stack.push((grandchild, sub_dir.clone()));
        }
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
    let current = extract_tree(&crate_root().join("src"));
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
    use super::{
        combine_feature, crate_root, extract_module, extract_surface, extract_tree,
        item_kind_and_name, parse_use, scan_runtime_env_vars,
    };
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
    fn test_item_kind_and_name_covers_each_kind() {
        let cases = [
            ("pub struct Foo {", Some(("struct", "Foo"))),
            ("pub struct Bar(u64);", Some(("struct", "Bar"))),
            ("pub struct Unit;", Some(("struct", "Unit"))),
            ("pub struct Gen<T> {", Some(("struct", "Gen"))),
            ("pub enum E {", Some(("enum", "E"))),
            ("pub trait T: Base {", Some(("trait", "T"))),
            ("pub fn f(x: u8) -> u8 {", Some(("fn", "f"))),
            ("pub type Alias<X> = Vec<X>;", Some(("type", "Alias"))),
            ("pub const K: u8 = 1;", Some(("const", "K"))),
            ("pub static S: u8 = 1;", Some(("static", "S"))),
            // `const fn` / `async fn` must capture the fn NAME, not "fn".
            ("pub const fn cf() -> u8 { 0 }", Some(("fn", "cf"))),
            ("pub async fn af() {}", Some(("fn", "af"))),
            // restricted visibility is not part of the public surface.
            ("pub(crate) fn hidden() {}", None),
            ("pub(super) struct Inner;", None),
            ("fn private() {}", None),
            ("impl Foo {", None),
        ];
        for (line, expected) in cases {
            let got = item_kind_and_name(line);
            let want = expected.map(|(k, n)| (k, n.to_string()));
            assert_eq!(got, want, "item_kind_and_name({line:?})");
        }
    }

    #[test]
    fn test_combine_feature_joins_two_gates() {
        assert_eq!(combine_feature(None, None), "default");
        assert_eq!(combine_feature(Some("python"), None), "python");
        assert_eq!(combine_feature(None, Some("simulator")), "simulator");
        assert_eq!(combine_feature(Some("python"), Some("python")), "python");
        // A deterministic sorted join when an item needs BOTH gates.
        assert_eq!(
            combine_feature(Some("python"), Some("orderbook")),
            "orderbook+python"
        );
        assert_eq!(
            combine_feature(Some("orderbook"), Some("python")),
            "orderbook+python"
        );
    }

    #[test]
    fn test_extract_module_records_every_variant_form() {
        // The gate exists because an added variant is breaking for a downstream
        // exhaustive `match` and was previously invisible here (#117 added
        // `StrategySpec::Legs` without moving this snapshot by one line). Every
        // variant shape must be recorded: unit, tuple, struct-like, and one
        // carrying an explicit discriminant.
        let src = "\
pub enum Shape {
    /// A unit variant.
    Unit,
    Tuple(u8, String),
    #[serde(rename = \"renamed\")]
    Struct {
        field: u8,
        other: u8,
    },
    Discriminant = 7,
}
";
        let (out, _children) = extract_module(src, "domain::", None);
        for variant in ["Unit", "Tuple", "Struct", "Discriminant"] {
            assert!(
                out.contains(&format!("default variant domain::Shape::{variant}")),
                "{variant} must be recorded: {out:?}"
            );
        }
        // A struct-like variant's own FIELDS are one level deeper, so they are
        // not variants and must not leak in.
        assert!(
            !out.iter()
                .any(|l| l.contains("field") || l.contains("other")),
            "variant fields are not surface items: {out:?}"
        );
        // The enum itself is still recorded alongside its variants.
        assert!(out.contains("default enum domain::Shape"));
    }

    #[test]
    fn test_extract_module_variants_inherit_the_enum_feature() {
        // A cfg-gated enum's variants carry the same feature tag, so a
        // feature-gated surface change is as visible as a default one.
        let src = "\
#[cfg(feature = \"simulator\")]
pub enum Gated {
    One,
    Two,
}
";
        let (out, _children) = extract_module(src, "data::", None);
        assert!(
            out.contains("simulator variant data::Gated::One"),
            "{out:?}"
        );
        assert!(
            out.contains("simulator variant data::Gated::Two"),
            "{out:?}"
        );
    }

    #[test]
    fn test_extract_module_variant_carries_its_own_cfg() {
        // A variant may be gated INDEPENDENTLY of its enum — `DataSourceSpec::Simulator`
        // and `FeedKind::Simulator` both are. Recording it as `default` would
        // claim it exists in the default surface; worse, the inverse (moving an
        // existing variant BEHIND a cfg, which is breaking under default
        // features) would not move the snapshot at all.
        let src = "\
pub enum Kind {
    Always,
    #[cfg(feature = \"simulator\")]
    Gated,
    AlsoAlways,
}
";
        let (out, _children) = extract_module(src, "data::", None);
        assert!(
            out.contains("default variant data::Kind::Always"),
            "{out:?}"
        );
        assert!(
            out.contains("simulator variant data::Kind::Gated"),
            "{out:?}"
        );
        assert!(
            !out.contains("default variant data::Kind::Gated"),
            "a gated variant must not be recorded as default: {out:?}"
        );
        // The cfg attaches to the NEXT variant only.
        assert!(
            out.contains("default variant data::Kind::AlsoAlways"),
            "a cfg must not leak onto the following variant: {out:?}"
        );
    }

    #[test]
    fn test_extract_module_records_variants_when_the_brace_wraps() {
        // rustfmt moves the brace to its own line as soon as generics or a
        // `where` clause wrap. Requiring `{` on the `pub enum` line recorded
        // ZERO variants for such an enum, silently — the enum's own name still
        // appeared, so nothing looked broken.
        let src = "\
pub enum Wrapped<T>
where
    T: Clone,
{
    Alpha(T),
    Beta,
}
pub fn after() {}
";
        let (out, _children) = extract_module(src, "engine::", None);
        assert!(
            out.contains("default variant engine::Wrapped::Alpha"),
            "{out:?}"
        );
        assert!(
            out.contains("default variant engine::Wrapped::Beta"),
            "{out:?}"
        );
        assert!(out.contains("default fn engine::after"), "{out:?}");
    }

    #[test]
    fn test_extract_module_empty_enum_does_not_swallow_later_items() {
        // `pub enum Never {}` closes on its own line. Scanning from the next
        // line for a column-0 `}` would run past it and swallow every item up to
        // the next one, silently DELETING unrelated entries from the snapshot.
        let src = "\
pub enum Never {}
pub struct Survivor {
    field: u8,
}
pub fn also_survives() {}
";
        let (out, _children) = extract_module(src, "domain::", None);
        assert!(out.contains("default enum domain::Never"), "{out:?}");
        assert!(
            !out.iter()
                .any(|l| l.starts_with("default variant domain::Never")),
            "an empty enum has no variants: {out:?}"
        );
        assert!(out.contains("default struct domain::Survivor"), "{out:?}");
        assert!(out.contains("default fn domain::also_survives"), "{out:?}");
    }

    #[test]
    fn test_extract_module_captures_pub_items_with_prefix() {
        let src = "\
pub struct Foo {
    field: u8,
}
pub enum Bar {
    A,
}
impl Foo {
    pub fn method(&self) {}
}
pub fn free() {}
pub const fn konst() -> u8 { 0 }
pub const K: u8 = 1;
pub type Alias = u8;
";
        let (out, _children) = extract_module(src, "engine::", None);
        assert!(out.contains("default struct engine::Foo"), "{out:?}");
        assert!(out.contains("default enum engine::Bar"));
        assert!(out.contains("default fn engine::free"));
        assert!(out.contains("default fn engine::konst"));
        assert!(out.contains("default const engine::K"));
        assert!(out.contains("default type engine::Alias"));
        // An `impl` method is indented, so it is NOT a module-level item.
        assert!(
            !out.contains("default fn engine::method"),
            "impl methods excluded: {out:?}"
        );
        // Struct fields are indented, so they never appear.
        assert!(!out.iter().any(|l| l.contains("field")));
        // An enum's variants ARE recorded (#121).
        assert!(out.contains("default variant engine::Bar::A"), "{out:?}");
    }

    #[test]
    fn test_extract_module_returns_file_children_with_feature() {
        let src = "\
pub mod backtest;
#[cfg(feature = \"simulator\")]
pub mod session;
";
        let (out, children) = extract_module(src, "engine::", None);
        assert!(out.contains("default mod engine::backtest"));
        assert!(out.contains("simulator mod engine::session"));
        let got: Vec<(&str, &str, Option<&str>)> = children
            .iter()
            .map(|c| (c.name.as_str(), c.prefix.as_str(), c.feature.as_deref()))
            .collect();
        assert!(
            got.contains(&("backtest", "engine::backtest::", None)),
            "{got:?}"
        );
        assert!(got.contains(&("session", "engine::session::", Some("simulator"))));
    }

    #[test]
    fn test_extract_module_inline_mod_prefixes_body_items() {
        let src = "\
pub mod sign {
    pub const fn side_sign() -> i64 {
        1
    }
    pub fn helper() {}
}
pub struct After;
";
        let (out, children) = extract_module(src, "domain::", None);
        assert!(out.contains("default mod domain::sign"));
        assert!(
            out.contains("default fn domain::sign::side_sign"),
            "{out:?}"
        );
        assert!(out.contains("default fn domain::sign::helper"));
        // Parsing resumes after the inline module's column-0 closing brace.
        assert!(out.contains("default struct domain::After"));
        // An inline module yields no file-based child.
        assert!(children.iter().all(|c| c.name != "sign"));
    }

    #[test]
    fn test_extract_module_propagates_base_feature() {
        let src = "\
pub fn plain() {}
#[cfg(feature = \"orderbook\")]
pub fn gated() {}
";
        let (out, _) = extract_module(src, "python::", Some("python"));
        assert!(out.contains("python fn python::plain"), "{out:?}");
        // An item's own cfg combines with the inherited module feature.
        assert!(out.contains("orderbook+python fn python::gated"));
    }

    #[test]
    fn test_extract_tree_is_a_superset_of_the_lib_surface() {
        use std::fs;
        let src_dir = crate_root().join("src");
        let lib = fs::read_to_string(src_dir.join("lib.rs")).expect("read lib.rs");
        let lib_surface = extract_surface(&lib);
        let tree = extract_tree(&src_dir);
        // Every lib.rs-level line survives the recursive extraction …
        for line in &lib_surface {
            assert!(tree.contains(line), "tree missing lib line {line:?}");
        }
        // … and recursion adds strictly more (the module internals, the #44 gap).
        assert!(
            tree.len() > lib_surface.len(),
            "recursion must add nested-module surface"
        );
        // A nested `pub mod` declaration is now tracked (e.g. engine::<sub>).
        assert!(
            tree.iter().any(|l| l.starts_with("default mod engine::")),
            "expected a nested engine submodule in the surface"
        );
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

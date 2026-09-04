# Security Policy — `ironcondor`

`ironcondor` is a Rust options-strategy backtester. It handles **no real
money** and routes **no orders**. It does, however, **parse untrusted input
files** (historical CSV / Parquet chain feeds and, on read-back, result
bundles) and is designed to run **inside firms' production research and CI
pipelines**. A parser that panics, hangs, or exhausts memory on a malformed
input is a denial-of-service vector in that CI, so its security posture is
treated like that of any internal service that parses external bytes.

> **Status:** Implementation has landed — the v0.1 core engine (deterministic
> replay loop and naive fill model) is in place, with the v0.x roadmap
> ongoing. This policy is in force; the threat model it summarises is a design
> commitment
> ([ADR-0007](docs/adr/0007-production-grade-performance-and-security.md)).

## Reporting a vulnerability

**Do not open a public issue for a security vulnerability.**

Report privately via **GitHub Security Advisories** —
[open a draft advisory](https://github.com/joaquinbejar/IronCondor/security/advisories/new)
on the repository — or by email to **Joaquin Bejar &lt;jb@taunais.com&gt;**
with `SECURITY` in the subject.

Please include:

- the affected version / commit,
- a description of the issue and its impact,
- a minimal reproduction (an input file or bundle that triggers it, where
  applicable),
- any suggested remediation.

## What to expect

- **Acknowledgement** within a few business days.
- A **fix or a mitigation plan** before public disclosure; because
  `ironcondor` sits inside CI infrastructure, parser and bundle-read-back
  vulnerabilities are fixed privately first, then disclosed.
- **Credit** in the advisory and `CHANGELOG.md`, unless you prefer to remain
  anonymous.

## Supported versions

Pre-1.0. During the `v0.x` series, security fixes land on the latest
released minor and are published to crates.io (and, from v0.4, PyPI). There
is no long-term-support branch before 1.0.

## Scope

**In scope**

- A malformed or adversarial **input file** (crafted CSV / Parquet, or a
  hostile result bundle handed back for read-back) that causes a panic,
  hang, unbounded memory growth, or a path escape.
- A **supply-chain** issue: a vulnerable, malicious, or typo-squatted
  dependency reaching a release.
- **Secret leakage**: a credential (e.g. an OptionChain-Simulator auth
  token) written to a bundle, a manifest, a log, or an error message.
- A **panic crossing the PyO3 boundary** into a host Python interpreter.

**Out of scope**

- Real-money / order-routing attacks — there is no live execution surface.
- A Byzantine OptionChain-Simulator server (treated as a same-trust data
  source; its responses are still validated at materialisation).
- OS-level isolation, multi-tenancy, or a hostile host — the deploying
  firm's responsibility.
- Timing / side-channel attacks — the crate holds no long-lived secrets on
  its hot paths.

## Security controls (design commitments)

- **Every untrusted input is validated once at its boundary**, with a
  resource ceiling and a typed error — never a panic. The immutable,
  hash-pinned, strictly-validated **tape** is the model
  ([docs/03 §6.1](docs/03-data-layer.md#61-materialised-tape--no-blocking-in-the-loop)).
- **`cargo audit` + `cargo deny` in CI from v0.1**; `Cargo.lock` committed
  and its `sha256` recorded in every bundle manifest as build provenance.
- **`#![forbid(unsafe_code)]`** in the crate itself (PyO3 macro glue is the
  only tolerated exception, never hand-written).
- **No panic crosses the PyO3 FFI boundary** — every binding maps a typed
  error to a Python exception.
- **Fuzz targets** for the CSV / Parquet / bundle-read-back parsers land
  before v1.0.
- **Secrets never reach a bundle, manifest, log, or error** — the sole
  credential channel is the optional simulator client, and a credential
  embedded as URL userinfo (`http://user:token@host`) is **redacted at
  serialisation** before any recorded copy (the manifest `data_source`, the
  nested `config.data_source`, or the `run_id` preimage) is written; the
  manifest records only the data-source **identity** (host URL + tape
  `sha256`). Proven by the captured-log credential test.

**v1.0 posture affirmation.** At the 1.0 cut the three security legs are in
place and verified fresh: untrusted-input hardening with fuzz targets over the
CSV / Parquet / bundle parsers (no panic, hang, or OOM on malformed bytes);
secrets non-leakage (the captured-log credential test above); and the
supply-chain / host-integrity controls — `#![forbid(unsafe_code)]` intact
in the crate (PyO3 macro glue the only tolerated exception), the
no-panic-across-FFI control asserted by the Python test suite, and
`cargo audit` + `cargo deny --all-features check` both green with an
**empty** ignore list in `.cargo/audit.toml` and `deny.toml`. The two
advisories the gates once had to excuse, `RUSTSEC-2024-0436` (`paste`,
unmaintained) and `RUSTSEC-2026-0235` (`rkyv 0.7`, reachable only through
`rust_decimal`'s optional feature), both left the resolved graph with the
2026-09-04 dependency refresh, so a genuinely new advisory anywhere now
fails CI without exception.

**Native-code surface (2026-09-04).** The same refresh grew the compiled
non-Rust surface behind the **optional** `simulator` feature: `reqwest`
0.13's TLS provider is `aws-lc-sys` (C + assembly, built through `cmake`)
in place of `ring` (also C + assembly), and server certificates are
verified against the operating-system trust store instead of a compiled-in
Mozilla root bundle. `forbid(unsafe_code)` is a crate-level lint that never
covered dependencies, so that guarantee is unchanged; the default build
links no TLS at all. Decision and consequences: the `reqwest` audit note in
`Cargo.toml` and the `0.6.0` CHANGELOG entry.

The full threat model, untrusted-input hardening table, and secrets-handling
rules live in
[docs/07-performance-and-security.md](docs/07-performance-and-security.md).

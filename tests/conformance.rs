//! The **shared conformance fixture** — a single reference bundle that asserts
//! the [docs/05 §12](../docs/05-analytics-and-reporting.md#12-producer-side-contract-matrix-ironcondorbundlev1)
//! producer-side contract matrix end to end, and is loaded **identically** by
//! [ChainView](https://github.com/joaquinbejar/ChainView)'s tests (#36,
//! [docs/05 §12.6](../docs/05-analytics-and-reporting.md#126-shared-conformance-fixture),
//! [docs/TESTING §5](../docs/TESTING.md#5-fixtures)).
//!
//! # The fixture
//!
//! A **four-leg iron condor** run through the **realistic** fill model over a
//! deliberately **thin** seeded book, committed under
//! [`tests/fixtures/conformance/`](../tests/fixtures/conformance) as
//! `manifest.json` + the four Parquet tables. It exercises the full trade
//! lifecycle the matrix pins:
//!
//! - **all four legs share one `trade_id`** — they are opened together in one
//!   step group ([docs/05 §12.4](../docs/05-analytics-and-reporting.md#124-identifier-grammars-producer-defined-delimiter-safe));
//! - **≥ 1 leg closed with a non-null `exit_reason`** — the short put is closed
//!   mid-run (a `ManualClose`), so its terminal `PositionRow` carries the only
//!   non-null `exit_reason` in the bundle
//!   ([docs/05 §9](../docs/05-analytics-and-reporting.md#9-positionsparquet));
//! - **≥ 1 leg left `open_at_end`** — the three surviving legs are still open
//!   when the feed exhausts ([docs/02 §3.3](../docs/02-engine-architecture.md#33-termination-feed-exhausted));
//! - **≥ 1 realistic multi-level fill** — each 5-lot marketable open walks the
//!   thin ladder (touch 3 + one deeper level), producing two `Fill`s per order
//!   at incrementing `fill_seq`, so the `(step, order_id, fill_seq)` **unique**
//!   key is exercised against a multi-row order
//!   ([docs/05 §7](../docs/05-analytics-and-reporting.md#7-fillsparquet)).
//!
//! # Two tests, two feature scopes
//!
//! - [`test_conformance_fixture_satisfies_the_contract_matrix`] (**default
//!   build**) reads the committed fixture through
//!   [`read_bundle`](ironcondor::read_bundle) — the *same* read a consumer
//!   (ChainView) does — and asserts every cell of the §12 matrix from the
//!   producer side. It is the portable, always-run guard.
//! - The [`producer`] module (**feature `orderbook`**) regenerates the fixture
//!   from the realistic run and asserts it equals the committed bytes under the
//!   single oracle (write → read → equal), and run-twice byte-identity. `BLESS=1`
//!   regenerates the committed fixture. Generation needs the matching engine, so
//!   it is feature-gated; the committed bytes it produces are what the default
//!   test (and ChainView) load.

use std::path::{Path, PathBuf};

use ironcondor::{ResourceLimits, ValidatedBundle, read_bundle};

mod common;
mod oracle;

/// Absolute path to the committed shared conformance fixture directory.
fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/conformance")
}

/// The four condor legs, `(strike_cents, style_str)` — the fixture's lifecycle.
const LEG_STRIKES: [(u64, &str); 4] = [
    (510_000, "call"), // short call
    (520_000, "call"), // long call
    (490_000, "put"),  // short put — closed mid-run
    (480_000, "put"),  // long put
];

/// The `trade_id` every leg of the single condor trade shares.
const CONDOR_TRADE_ID: u64 = 1;

/// The step at which the short put is closed (a `ManualClose`).
const CLOSE_STEP: u32 = 2;

/// Read the committed conformance fixture through the reader security gate.
fn read_committed_fixture() -> ValidatedBundle {
    let Ok(bundle) = read_bundle(fixture_dir(), &ResourceLimits::default()) else {
        panic!(
            "the committed conformance fixture must read back through read_bundle \
             (regenerate with: BLESS=1 cargo test --features orderbook --test conformance)"
        );
    };
    bundle
}

/// The default-build guard: read the committed fixture the same way ChainView
/// does and assert **every cell** of the [docs/05 §12](../docs/05-analytics-and-reporting.md#12-producer-side-contract-matrix-ironcondorbundlev1)
/// producer-side contract matrix.
#[test]
fn test_conformance_fixture_satisfies_the_contract_matrix() {
    let bundle = read_committed_fixture();
    assert_matrix(&bundle, &fixture_dir());
}

/// Assert the full §12 contract matrix over a decoded bundle in `bundle_dir`.
/// Called both by the default-build guard (over the committed fixture) and by
/// the producer regeneration test (over the freshly produced bundle in a
/// tempdir), so the raw-manifest checks read from the bundle's own directory.
fn assert_matrix(bundle: &ValidatedBundle, bundle_dir: &Path) {
    assert_manifest_matrix(bundle, bundle_dir);
    assert_table_shape_and_step_domain(bundle);
    assert_identifier_grammar(bundle);
    assert_lifecycle(bundle);
}

/// §12.1 / §12.3 / §12.5 — manifest fields, USD-scoping (no `currency` field),
/// and the `row_counts` cross-check (the reader already cross-checked lengths;
/// re-assert the counts are the four keyed fields and match the tables).
fn assert_manifest_matrix(bundle: &ValidatedBundle, bundle_dir: &Path) {
    let manifest = &bundle.manifest;
    assert_eq!(
        manifest.schema, "ironcondor.bundle.v1",
        "the schema tag is the frozen v1 wire contract"
    );

    // The raw manifest JSON: exactly the docs/05 §6 fields, and NO currency
    // field (USD is fixed by the tag, §12.3).
    let raw = oracle::read_manifest_json(bundle_dir);
    let Some(obj) = raw.as_object() else {
        panic!("the manifest.json must be a JSON object");
    };
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let mut expected = [
        "schema",
        "run_id",
        "created_utc",
        "code_version",
        "lockfile_sha256",
        "seed",
        "config",
        "strategy",
        "data_source",
        "metrics",
        "row_counts",
    ];
    expected.sort_unstable();
    assert_eq!(
        keys, expected,
        "manifest carries exactly the docs/05 §6 fields"
    );
    assert!(
        !obj.contains_key("currency"),
        "v1 has no currency field (USD by the tag)"
    );
    assert!(obj.contains_key("metrics"), "metrics is a required field");
    assert!(
        obj.contains_key("row_counts"),
        "row_counts is a required field"
    );

    // §12.5 — row_counts equals the decoded table lengths (the reader already
    // enforced this; assert it holds for the fixture too).
    let counts = manifest.row_counts;
    assert_eq!(
        counts.fills as usize,
        bundle.fills.len(),
        "row_counts.fills"
    );
    assert_eq!(
        counts.equity_curve as usize,
        bundle.equity_curve.len(),
        "row_counts.equity_curve"
    );
    assert_eq!(
        counts.positions as usize,
        bundle.positions.len(),
        "row_counts.positions"
    );
    assert_eq!(
        counts.greeks_attribution as usize,
        bundle.greeks_attribution.len(),
        "row_counts.greeks_attribution"
    );
}

/// §12.2 — table sort/unique keys, the single nullable column, and the
/// cross-table step-domain rules. (The reader already validated each table's
/// column set / physical type / nullability against the exact schema, so a wrong
/// column/type/nullability could not have decoded; here we assert the ordering
/// and step-domain invariants the reader does not enforce.)
fn assert_table_shape_and_step_domain(bundle: &ValidatedBundle) {
    // fills: (step, order_id, fill_seq) is UNIQUE and sorted.
    let mut fill_keys: Vec<(u32, u64, u32)> = bundle
        .fills
        .iter()
        .map(|f| (f.step, f.order_id, f.fill_seq))
        .collect();
    let sorted = {
        let mut s = fill_keys.clone();
        s.sort_unstable();
        s
    };
    assert_eq!(
        fill_keys, sorted,
        "fills are sorted by (step, order_id, fill_seq)"
    );
    fill_keys.dedup();
    assert_eq!(
        fill_keys.len(),
        bundle.fills.len(),
        "(step, order_id, fill_seq) is the UNIQUE fills key"
    );

    // positions: (step, position_id) is ≤ 1 row/step and sorted.
    let mut pos_keys: Vec<(u32, u64)> = bundle
        .positions
        .iter()
        .map(|p| (p.step, p.position_id))
        .collect();
    let pos_sorted = {
        let mut s = pos_keys.clone();
        s.sort_unstable();
        s
    };
    assert_eq!(
        pos_keys, pos_sorted,
        "positions are sorted by (step, position_id)"
    );
    pos_keys.dedup();
    assert_eq!(
        pos_keys.len(),
        bundle.positions.len(),
        "(step, position_id) is ≤ 1 row per step"
    );

    // equity_curve and greeks_attribution: exactly one row per step over a
    // contiguous 0..N, sorted by step.
    let n = bundle.equity_curve.len();
    assert!(n > 0, "the fixture has at least one step");
    for (i, point) in bundle.equity_curve.iter().enumerate() {
        assert_eq!(
            point.step as usize, i,
            "equity_curve is a contiguous 0..N by step"
        );
    }
    assert_eq!(
        bundle.greeks_attribution.len(),
        n,
        "greeks_attribution has one row per step, same N as equity_curve"
    );
    for (i, row) in bundle.greeks_attribution.iter().enumerate() {
        assert_eq!(
            row.step as usize, i,
            "greeks_attribution is a contiguous 0..N by step"
        );
    }

    // exit_reason is the ONLY nullable column: assert it is null while open and
    // non-null exactly on the closed leg's terminal row (checked in the
    // lifecycle assertions). Here: every fills/positions step is in 0..N.
    for f in &bundle.fills {
        assert!((f.step as usize) < n, "every fills step is within 0..N");
    }
    for p in &bundle.positions {
        assert!((p.step as usize) < n, "every positions step is within 0..N");
    }

    // Cross-table: all rows sharing a step carry the same ts_ns.
    let step_ts: std::collections::BTreeMap<u32, i64> = bundle
        .equity_curve
        .iter()
        .map(|p| (p.step, p.ts_ns))
        .collect();
    for f in &bundle.fills {
        assert_eq!(
            Some(&f.ts_ns),
            step_ts.get(&f.step),
            "fills ts_ns matches its step"
        );
    }
    for p in &bundle.positions {
        assert_eq!(
            Some(&p.ts_ns),
            step_ts.get(&p.step),
            "positions ts_ns matches its step"
        );
    }
    for g in &bundle.greeks_attribution {
        assert_eq!(
            Some(&g.ts_ns),
            step_ts.get(&g.step),
            "greeks ts_ns matches its step"
        );
    }
}

/// §12.4 — the `contract_id` grammar + round-trip (the reader enforced the
/// round-trip; re-assert the `v1:` prefix, the delimiter-safe `UNDERLYING`, and
/// that the id's parsed fields equal the row's own columns), and the money
/// scaling (§12.3): every strike/price is integer cents (a plain integer type on
/// the wire — implicit — so we assert positivity where the contract requires it).
fn assert_identifier_grammar(bundle: &ValidatedBundle) {
    for f in &bundle.fills {
        assert!(
            f.contract_id.starts_with("v1:"),
            "contract_id carries the v1: format prefix"
        );
        assert!(
            underlying_is_grammar_safe(&f.underlying),
            "UNDERLYING {:?} matches ^[A-Z0-9._]{{1,32}}$ and is colon-free",
            f.underlying
        );
        // The id round-trips to the row's own columns (a referential check).
        let expected = format!(
            "v1:{}:{}:{}:{}",
            f.underlying,
            f.expiration_ns,
            f.strike_cents,
            if f.style == "call" { "C" } else { "P" }
        );
        assert_eq!(
            f.contract_id, expected,
            "fills contract_id round-trips to its columns"
        );
        assert!(f.quantity > 0, "filled quantity is > 0");
        assert!(f.price_cents > 0, "executed premium is integer cents > 0");
        assert!(f.fees_cents >= 0, "fees are a non-negative cents magnitude");
    }
    for p in &bundle.positions {
        assert!(
            p.contract_id.starts_with("v1:"),
            "position contract_id carries v1:"
        );
        assert!(p.quantity > 0, "open/closed contracts are > 0");
    }
}

/// §12.6 — the trade lifecycle: one shared `trade_id`, four legs, a mid-run close
/// with a non-null `exit_reason`, a leg left `open_at_end`, and a realistic
/// multi-level fill exercising the `(step, order_id, fill_seq)` unique key.
fn assert_lifecycle(bundle: &ValidatedBundle) {
    // All four legs share ONE trade_id — on every fill and every position row.
    for f in &bundle.fills {
        assert_eq!(
            f.trade_id, CONDOR_TRADE_ID,
            "every fill shares the one condor trade_id"
        );
    }
    for p in &bundle.positions {
        assert_eq!(
            p.trade_id, CONDOR_TRADE_ID,
            "every position shares the one condor trade_id"
        );
    }

    // Exactly four legs (four distinct position_ids), matching the four condor
    // strikes.
    let mut leg_ids: Vec<u64> = bundle.positions.iter().map(|p| p.position_id).collect();
    leg_ids.sort_unstable();
    leg_ids.dedup();
    assert_eq!(
        leg_ids.len(),
        4,
        "the condor has four legs (four position_ids)"
    );
    let mut opened_strikes: Vec<(u64, &str)> = bundle
        .fills
        .iter()
        .map(|f| (f.strike_cents, f.style.as_str()))
        .collect();
    opened_strikes.sort_unstable();
    opened_strikes.dedup();
    let mut want: Vec<(u64, &str)> = LEG_STRIKES.to_vec();
    want.sort_unstable();
    assert_eq!(opened_strikes, want, "the four condor legs are opened");

    // ≥ 1 leg closed with a non-null exit_reason (the short put, mid-run) — and
    // exit_reason is null on EVERY other row (the only nullable column).
    let closed: Vec<&_> = bundle
        .positions
        .iter()
        .filter(|p| p.exit_reason.is_some())
        .collect();
    assert!(
        !closed.is_empty(),
        "at least one leg is closed with a non-null exit_reason"
    );
    for term in &closed {
        assert_eq!(term.step, CLOSE_STEP, "the mid-run close lands on its step");
        assert_eq!(
            term.exit_reason.as_deref(),
            Some("manual_close"),
            "a mid-run strategy close is a ManualClose"
        );
        assert!(!term.open_at_end, "a closed leg is not open_at_end");
        assert!(
            term.contract_id.contains(":490000:P"),
            "the closed leg is the short put (strike 490000, put)"
        );
    }

    // ≥ 1 leg left open_at_end (the surviving legs on the final step).
    let open_at_end: Vec<&_> = bundle.positions.iter().filter(|p| p.open_at_end).collect();
    assert!(
        !open_at_end.is_empty(),
        "at least one leg is left open_at_end at feed exhaustion"
    );
    for row in &open_at_end {
        assert!(
            row.exit_reason.is_none(),
            "an open_at_end leg has no exit_reason"
        );
    }

    // ≥ 1 realistic multi-level fill: some order_id has ≥ 2 fill_seq rows, and
    // the mode is realistic on every fill.
    let mut per_order: std::collections::BTreeMap<u64, Vec<u32>> =
        std::collections::BTreeMap::new();
    for f in &bundle.fills {
        assert_eq!(f.mode, "realistic", "the fixture is a realistic-mode run");
        per_order.entry(f.order_id).or_default().push(f.fill_seq);
    }
    let multi: Vec<(&u64, &Vec<u32>)> = per_order
        .iter()
        .filter(|(_, seqs)| seqs.len() >= 2)
        .collect();
    assert!(
        !multi.is_empty(),
        "at least one order walks ≥ 2 price levels (a multi-level fill)"
    );
    for (order_id, seqs) in &multi {
        let mut s = (*seqs).clone();
        s.sort_unstable();
        let contiguous: Vec<u32> = (0..u32::try_from(s.len()).unwrap_or(0)).collect();
        assert_eq!(
            s, contiguous,
            "order {order_id} has contiguous fill_seq 0..n across its levels"
        );
    }
}

/// A leg's underlying grammar: `^[A-Z0-9._]{1,32}$`, colon-free.
fn underlying_is_grammar_safe(underlying: &str) -> bool {
    !underlying.is_empty()
        && underlying.len() <= 32
        && !underlying.contains(':')
        && underlying
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '.' || c == '_')
}

// ---------------------------------------------------------------------------
// The producer (generation) side — feature `orderbook` only.
// ---------------------------------------------------------------------------

#[cfg(feature = "orderbook")]
mod producer {
    use std::path::{Path, PathBuf};

    use chrono::DateTime;
    use ironcondor::analytics::attribution::attribute;
    use ironcondor::analytics::metrics;
    use ironcondor::{
        BacktestConfig, BacktestEngine, BacktestRun, ChainContext, ContractKey, DataSourceSpec,
        ExecutionMode, FeeSchedule, IronCondorSpec, LiquidityProfile, OrderCommand, OrderIntent,
        PositionAction, PriceCents, Quantity, RealisticFill, ResourceLimits, SlippageModel,
        Strategy, StrategySpec, TimeInForce, TouchSize, Underlying, read_bundle, write_bundle,
    };
    use optionstratlib::ExpirationDate;
    use optionstratlib::{OptionStyle, Side};

    use super::{CLOSE_STEP, LEG_STRIKES, assert_matrix, fixture_dir, oracle};

    /// Steps in the conformance chain — enough for the open (step 0), the mid-run
    /// close (step 2), and the still-open legs at exhaustion (step 4).
    const CONFORMANCE_STEPS: u32 = 5;
    /// The per-leg contract count — large enough (> the thin touch of 3) that each
    /// marketable open walks a second price level (a multi-level fill).
    const LEG_LOTS: u32 = 5;
    /// Canonical data-source provenance for the fixture (pins the run_id/manifest,
    /// see `tests/bundle_golden.rs` for the same-shaped reasoning).
    const CANONICAL_DATA_PATH: &str = "conformance_condor.parquet";
    const CANONICAL_DATA_IDENTITY: &str = "conformance:iron_condor_realistic:ironcondor.bundle.v1";
    const CANONICAL_CREATED_UTC: &str = "1970-01-01T00:00:00+00:00";
    const CANONICAL_OUTPUT_DIR: &str = "conformance-out";
    const TABLE_FILES: [&str; 4] = [
        "fills.parquet",
        "equity_curve.parquet",
        "positions.parquet",
        "greeks_attribution.parquet",
    ];

    fn bless_enabled() -> bool {
        std::env::var_os("BLESS").is_some_and(|value| value == "1")
    }

    fn und() -> Underlying {
        let Ok(u) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        u
    }

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is a valid quantity");
        };
        q
    }

    /// A condor leg contract key at the chain's shared expiry.
    fn leg_key(strike: u64, style: OptionStyle) -> ContractKey {
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
                super::common::EXPIRY,
            )),
            strike: PriceCents::new(strike),
            style,
        }
    }

    /// The four condor legs as `(strike, style, side)`.
    fn legs() -> [(u64, OptionStyle, Side); 4] {
        [
            (510_000, OptionStyle::Call, Side::Short), // short call
            (520_000, OptionStyle::Call, Side::Long),  // long call
            (490_000, OptionStyle::Put, Side::Short),  // short put (closed mid-run)
            (480_000, OptionStyle::Put, Side::Long),   // long put
        ]
    }

    /// The conformance strategy: open the four legs together (one trade group ⇒
    /// one `trade_id`) as `LEG_LOTS`-lot marketable IOC orders that walk the thin
    /// book, close the short put mid-run (a `ManualClose`), and leave the other
    /// three legs open at feed exhaustion (`open_at_end`).
    struct ConformanceCondor {
        closed_put: bool,
    }

    impl Strategy for ConformanceCondor {
        fn on_start(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), ironcondor::BacktestError> {
            for (strike, style, side) in legs() {
                let key = leg_key(strike, style);
                let Some(quote) = ctx.snapshot.quotes.get(&key) else {
                    panic!("condor leg {strike} {style:?} must be quoted in the fixture chain");
                };
                out.push(OrderCommand::Submit(OrderIntent {
                    contract: key,
                    action: PositionAction::Open,
                    side,
                    quantity: qty(LEG_LOTS),
                    limit: None,
                    tif: TimeInForce::Ioc,
                    decision_mid: quote.mid,
                }));
            }
            Ok(())
        }

        fn on_snapshot(
            &mut self,
            ctx: &mut ChainContext,
            out: &mut Vec<OrderCommand>,
        ) -> Result<(), ironcondor::BacktestError> {
            if self.closed_put || ctx.step.value() != CLOSE_STEP {
                return Ok(());
            }
            let put_key = leg_key(490_000, OptionStyle::Put);
            let Some(leg) = ctx.open.iter().find(|p| p.contract == put_key) else {
                panic!("the short put must be open at the close step");
            };
            let decision_mid = ctx
                .snapshot
                .quotes
                .get(&leg.contract)
                .map_or(leg.entry_premium, |q| q.mid);
            // Buy back a short leg (flip the opened side) — a full close.
            let close_side = match leg.side {
                Side::Long => Side::Short,
                Side::Short => Side::Long,
            };
            out.push(OrderCommand::Submit(OrderIntent {
                contract: leg.contract.clone(),
                action: PositionAction::Close(leg.position_id),
                side: close_side,
                quantity: leg.quantity,
                limit: None,
                tif: TimeInForce::Ioc,
                decision_mid,
            }));
            self.closed_put = true;
            Ok(())
        }

        fn on_end(
            &mut self,
            _ctx: &mut ChainContext,
            _out: &mut Vec<OrderCommand>,
        ) -> Result<(), ironcondor::BacktestError> {
            // Close nothing: the three surviving legs are left open_at_end.
            Ok(())
        }
    }

    fn canonical_data_source() -> DataSourceSpec {
        DataSourceSpec::Parquet {
            path: CANONICAL_DATA_PATH.to_string(),
            sha256: String::new(),
        }
    }

    /// A qty-`LEG_LOTS` iron-condor spec — the manifest's documentary strategy
    /// (the fixture opens 5-lot legs to walk the thin book).
    fn conformance_spec() -> StrategySpec {
        StrategySpec::IronCondor(IronCondorSpec {
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            short_call_strike: PriceCents::new(510_000),
            short_put_strike: PriceCents::new(490_000),
            long_call_strike: PriceCents::new(520_000),
            long_put_strike: PriceCents::new(480_000),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
                super::common::EXPIRY,
            )),
            implied_volatility: rust_decimal::Decimal::new(20, 2),
            risk_free_rate: rust_decimal::Decimal::new(5, 2),
            dividend_yield: rust_decimal::Decimal::ZERO,
            quantity: qty(LEG_LOTS),
            premium_short_call: PriceCents::new(2_000),
            premium_short_put: PriceCents::new(1_800),
            premium_long_call: PriceCents::new(800),
            premium_long_put: PriceCents::new(700),
            open_fee: PriceCents::new(65),
            close_fee: PriceCents::new(65),
        })
    }

    fn conformance_config(output_dir: &Path) -> BacktestConfig {
        BacktestConfig {
            data_source: canonical_data_source(),
            mode: ExecutionMode::Realistic,
            seed: 7,
            initial_capital: 10_000_000,
            fees: FeeSchedule {
                per_contract_cents: 65,
                per_order_cents: 100,
            },
            slippage: SlippageModel::None,
            marketable_cap_ticks: 10,
            // A deliberately THIN book: a flat touch of 3 plus deeper levels one
            // tick apart, so a 5-lot marketable order walks a second level.
            liquidity_profile: LiquidityProfile {
                touch_size: TouchSize::Flat { contracts: 3 },
                depth_levels: 2,
                decay: rust_decimal::Decimal::ONE,
            },
            limits: ResourceLimits::default(),
            output_dir: output_dir.to_path_buf(),
            overwrite: false,
        }
    }

    /// Assemble and run the realistic conformance scenario, pinning the
    /// data-source identity to canonical constants (stable run_id/manifest).
    fn build_conformance_run(
        chain_dir: &Path,
        output_dir: &Path,
    ) -> (BacktestConfig, StrategySpec, BacktestRun) {
        let chain_path = chain_dir.join("conformance_condor.parquet");
        let rows = super::common::condor_rows(CONFORMANCE_STEPS, None);
        if super::common::write_parquet(&chain_path, &rows).is_err() {
            panic!("the conformance chain must write");
        }
        let Ok(feed) = ironcondor::ParquetFeed::open(&chain_path, &ResourceLimits::default())
        else {
            panic!("the conformance chain must open");
        };
        let config = conformance_config(output_dir);
        let execution = RealisticFill::with_liquidity_profile(
            config.fees,
            config.marketable_cap_ticks,
            config.seed,
            config.liquidity_profile,
        );
        let strategy = ConformanceCondor { closed_put: false };
        let Ok(mut run) = BacktestEngine::run(&config, feed, execution, strategy, "iron_condor")
        else {
            panic!("the conformance realistic run must succeed");
        };
        let Ok(initial) = i64::try_from(config.initial_capital) else {
            panic!("initial capital fits i64 cents");
        };
        if metrics::populate(
            &mut run.result,
            &run.equity_curve,
            initial,
            &run.trade_log,
            &run.open_at_end,
        )
        .is_err()
        {
            panic!("metrics must populate the conformance result");
        }
        let Ok(attribution) = attribute(&run.attribution_substrate) else {
            panic!("attribution must decompose the conformance run");
        };
        run.greeks_attribution = attribution;
        run.data_source = canonical_data_source();
        run.data_identity = CANONICAL_DATA_IDENTITY.to_string();
        (config, conformance_spec(), run)
    }

    fn bless_fixture(produced_dir: &Path) {
        let dir = fixture_dir();
        if std::fs::create_dir_all(&dir).is_err() {
            panic!("the conformance fixture directory must exist");
        }
        for name in TABLE_FILES {
            if std::fs::copy(produced_dir.join(name), dir.join(name)).is_err() {
                panic!("BLESS must copy {name} into the fixture directory");
            }
        }
        let mut manifest = oracle::read_manifest_json(produced_dir);
        if let Some(obj) = manifest.as_object_mut() {
            obj.insert(
                "created_utc".to_string(),
                serde_json::Value::String(CANONICAL_CREATED_UTC.to_string()),
            );
            if let Some(config) = obj
                .get_mut("config")
                .and_then(serde_json::Value::as_object_mut)
            {
                config.insert(
                    "output_dir".to_string(),
                    serde_json::Value::String(CANONICAL_OUTPUT_DIR.to_string()),
                );
            }
        }
        let Ok(mut text) = serde_json::to_string_pretty(&manifest) else {
            panic!("the normalised conformance manifest must serialise");
        };
        text.push('\n');
        if std::fs::write(dir.join("manifest.json"), text).is_err() {
            panic!("BLESS must write the conformance manifest.json");
        }
    }

    /// The producer-side freeze: regenerate the conformance bundle, assert it
    /// satisfies the §12 matrix, and (unless blessing) equals the committed
    /// fixture under the single oracle. `BLESS=1` regenerates the fixture.
    #[test]
    fn test_conformance_fixture_regenerates_to_committed_bytes() {
        let Ok(chain_dir) = tempfile::tempdir() else {
            panic!("a tempdir for the generated chain must create");
        };
        let Ok(out_dir) = tempfile::tempdir() else {
            panic!("a tempdir for the bundle output must create");
        };
        let (config, spec, run) = build_conformance_run(chain_dir.path(), out_dir.path());
        let Ok(produced_dir) = write_bundle(&run, &config, &spec) else {
            panic!("the conformance bundle must write");
        };

        // The freshly produced bundle must itself satisfy the whole matrix.
        let limits = ResourceLimits::default();
        let Ok(produced) = read_bundle(&produced_dir, &limits) else {
            panic!("the produced conformance bundle must read back");
        };
        assert_matrix(&produced, &produced_dir);

        if bless_enabled() {
            bless_fixture(&produced_dir);
            return;
        }

        // write → read → equal against the committed fixture, under the oracle.
        let Ok(expected) = read_bundle(fixture_dir(), &limits) else {
            panic!("the committed conformance fixture must read — regenerate with BLESS=1");
        };
        if let Err(diff) = oracle::compare_bundle_tables(&produced, &expected) {
            panic!(
                "conformance fixture table divergence: {diff} (regenerate with BLESS=1 if intended)"
            );
        }
        let produced_manifest = oracle::read_manifest_json(&produced_dir);
        let expected_manifest = oracle::read_manifest_json(&fixture_dir());
        if let Err(diff) = oracle::compare_manifest_json(&produced_manifest, &expected_manifest) {
            panic!(
                "conformance fixture manifest divergence: {diff} (regenerate with BLESS=1 if intended)"
            );
        }
    }

    /// Same-environment run-twice: the conformance bundle's four Parquet tables
    /// are byte-identical and its manifest identical after stripping created_utc.
    #[test]
    fn test_conformance_fixture_run_twice_byte_identical() {
        let write_once = || -> (PathBuf, tempfile::TempDir, tempfile::TempDir) {
            let Ok(chain_dir) = tempfile::tempdir() else {
                panic!("chain tempdir");
            };
            let Ok(out_dir) = tempfile::tempdir() else {
                panic!("output tempdir");
            };
            let (config, spec, run) = build_conformance_run(chain_dir.path(), out_dir.path());
            let Ok(dir) = write_bundle(&run, &config, &spec) else {
                panic!("the conformance bundle must write");
            };
            (dir, chain_dir, out_dir)
        };
        let (dir_a, _c_a, _o_a) = write_once();
        let (dir_b, _c_b, _o_b) = write_once();
        assert_eq!(dir_a.file_name(), dir_b.file_name(), "deterministic run_id");
        for name in TABLE_FILES {
            let (Ok(a), Ok(b)) = (
                std::fs::read(dir_a.join(name)),
                std::fs::read(dir_b.join(name)),
            ) else {
                panic!("{name} must be readable from both runs");
            };
            assert_eq!(a, b, "{name} must be byte-identical across two runs");
        }
        let a = oracle::canonical_manifest(&oracle::read_manifest_json(&dir_a));
        let b = oracle::canonical_manifest(&oracle::read_manifest_json(&dir_b));
        assert_eq!(
            a, b,
            "manifest identical after stripping created_utc + output_dir"
        );
    }

    /// Guard that the fixture strikes constant stays aligned with the legs the
    /// strategy opens — a cheap drift check on the shared `LEG_STRIKES` table.
    #[test]
    fn test_leg_table_matches_the_opened_legs() {
        let opened: Vec<(u64, &str)> = legs()
            .iter()
            .map(|(strike, style, _)| {
                (
                    *strike,
                    if matches!(style, OptionStyle::Call) {
                        "call"
                    } else {
                        "put"
                    },
                )
            })
            .collect();
        assert_eq!(opened, LEG_STRIKES.to_vec());
    }
}

//! Property tests for the canonical domain types.

use std::collections::BTreeMap;

use ironcondor::{
    BacktestError, Cents, ChainSnapshot, ContractKey, ExecutionMode, ExecutionModel, FeeSchedule,
    Fill, InstrumentSpec, Ledger, NaiveFill, OpenPosition, OrderCommand, OrderIntent,
    PositionAction, PositionId, PriceCents, Quantity, QuoteView, RawQuote, SimClock, SimTime,
    SlippageModel, SnapshotMeta, StepIndex, Ticks, TimeInForce, Underlying, raw_quotes_to_snapshot,
};
use optionstratlib::prelude::Positive;
use optionstratlib::{ExpirationDate, OptionStyle, Side};
use proptest::prelude::*;
use rust_decimal::Decimal;

use ironcondor::{
    BacktestEngine, BacktestRun, ChainContext, ParquetFeed, ResourceLimits, Strategy,
};
use rand_chacha::rand_core::RngCore;

mod common;

/// The JSON object keys of a serialised value, sorted — the field *shape* of
/// a bundle/record type, independent of its values.
fn json_object_keys(value: &serde_json::Value) -> Option<Vec<String>> {
    let mut keys: Vec<String> = value.as_object()?.keys().cloned().collect();
    keys.sort();
    Some(keys)
}

/// The tape anchor `ts_0` used by the conversion property tests.
const TS0: i64 = 1_750_291_200_000_000_000;
/// Nanoseconds in one calendar day of exactly 86 400 s (UTC).
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// Build a conversion meta with a 5-cent tick and a 100x multiplier anchored
/// at `TS0`.
fn conversion_meta() -> SnapshotMeta {
    let underlying = match Underlying::new("SPX") {
        Ok(u) => u,
        Err(e) => unreachable!("SPX is valid: {e}"),
    };
    SnapshotMeta {
        ts: SimTime::new(TS0),
        step: StepIndex::new(0),
        anchor_ts: SimTime::new(TS0),
        underlying,
        underlying_price: PriceCents::new(510_000),
        tick_size_cents: PriceCents::new(5),
        contract_multiplier: 100,
    }
}

/// A tick-aligned raw call quote at absolute expiry `TS0 + 30 days`.
fn abs_raw(strike: u64, bid: u64, ask: u64) -> RawQuote {
    let expiration = ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(
        TS0 + 30 * NANOS_PER_DAY,
    ));
    let size = match Quantity::new(1) {
        Ok(q) => q,
        Err(e) => unreachable!("1 is a valid quantity: {e}"),
    };
    RawQuote {
        expiration,
        strike: PriceCents::new(strike),
        style: OptionStyle::Call,
        bid: PriceCents::new(bid),
        ask: PriceCents::new(ask),
        bid_size: size,
        ask_size: size,
        implied_volatility: 0.2,
        delta: 0.5,
        gamma: 0.01,
        theta: -0.05,
        vega: 0.1,
    }
}

/// A resolved contract key for the naive-fill property tests.
fn naive_contract(strike: u64) -> Option<ContractKey> {
    let underlying = Underlying::new("SPX").ok()?;
    Some(ContractKey {
        underlying,
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(TS0)),
        strike: PriceCents::new(strike),
        style: OptionStyle::Call,
    })
}

/// A single-quote snapshot whose only contract has `mid == bid == ask` so a
/// `FixedCents` slippage is measured against a clean mid.
fn naive_snapshot(contract: &ContractKey, mid: u64) -> Option<ChainSnapshot> {
    let underlying = Underlying::new("SPX").ok()?;
    let spec = InstrumentSpec::new(PriceCents::new(5), 100).ok()?;
    let size = Quantity::new(1).ok()?;
    let quote = QuoteView {
        contract: contract.clone(),
        bid: PriceCents::new(mid),
        ask: PriceCents::new(mid),
        mid: PriceCents::new(mid),
        bid_size: size,
        ask_size: size,
        implied_volatility: Decimal::ZERO,
        delta: Decimal::ZERO,
        gamma: Decimal::ZERO,
        theta: Decimal::ZERO,
        vega: Decimal::ZERO,
    };
    let mut quotes = BTreeMap::new();
    quotes.insert(contract.clone(), quote);
    Some(ChainSnapshot {
        ts: SimTime::new(TS0),
        step: StepIndex::new(0),
        underlying,
        underlying_price: PriceCents::new(mid),
        spec,
        quotes,
    })
}

proptest! {
    /// Every money newtype serialises as its bare inner scalar and
    /// round-trips through JSON unchanged.
    #[test]
    fn money_newtype_roundtrip(
        cents in any::<i64>(),
        price in any::<u64>(),
        qty in 1u32..,
        ticks in any::<u128>(),
    ) {
        let c = Cents::new(cents);
        prop_assert_eq!(serde_json::to_string(&c).ok(), Some(cents.to_string()));
        let back: Result<Cents, _> = serde_json::from_str(&cents.to_string());
        prop_assert!(matches!(back, Ok(b) if b == c));

        let p = PriceCents::new(price);
        prop_assert_eq!(serde_json::to_string(&p).ok(), Some(price.to_string()));
        let back: Result<PriceCents, _> = serde_json::from_str(&price.to_string());
        prop_assert!(matches!(back, Ok(b) if b == p));

        let q = Quantity::new(qty);
        prop_assert!(matches!(q, Ok(q) if q.value() == qty));

        let t = Ticks::new(ticks);
        prop_assert_eq!(serde_json::to_string(&t).ok(), Some(ticks.to_string()));
        let back: Result<Ticks, _> = serde_json::from_str(&ticks.to_string());
        prop_assert!(matches!(back, Ok(b) if b == t));
    }

    /// Checked cents arithmetic either returns the exact mathematical
    /// result or `ArithmeticOverflow` — never a silent wrap.
    #[test]
    fn cents_arithmetic_no_silent_overflow(a in any::<i64>(), b in any::<i64>()) {
        let exact_sum = i128::from(a) + i128::from(b);
        match Cents::new(a).checked_add(Cents::new(b)) {
            Ok(sum) => prop_assert_eq!(i128::from(sum.value()), exact_sum),
            Err(BacktestError::ArithmeticOverflow) => {
                prop_assert!(exact_sum > i128::from(i64::MAX) || exact_sum < i128::from(i64::MIN));
            }
            Err(other) => prop_assert!(false, "unexpected error: {other}"),
        }

        let exact_diff = i128::from(a) - i128::from(b);
        match Cents::new(a).checked_sub(Cents::new(b)) {
            Ok(diff) => prop_assert_eq!(i128::from(diff.value()), exact_diff),
            Err(BacktestError::ArithmeticOverflow) => {
                prop_assert!(
                    exact_diff > i128::from(i64::MAX) || exact_diff < i128::from(i64::MIN)
                );
            }
            Err(other) => prop_assert!(false, "unexpected error: {other}"),
        }
    }

    /// `to_contract_id` → `from_contract_id` is the identity for any valid
    /// resolved key.
    #[test]
    fn contract_id_roundtrip_identity(
        ticker in "[A-Z0-9._]{1,32}",
        expiration_ns in any::<i64>(),
        strike in any::<u64>(),
        is_call in any::<bool>(),
    ) {
        let underlying = Underlying::new(ticker);
        prop_assert!(underlying.is_ok());
        let Ok(underlying) = underlying else { return Ok(()); };
        let key = ContractKey {
            underlying,
            expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(
                expiration_ns,
            )),
            strike: PriceCents::new(strike),
            style: if is_call { OptionStyle::Call } else { OptionStyle::Put },
        };
        let id = key.to_contract_id();
        prop_assert!(id.is_ok());
        let Ok(id) = id else { return Ok(()); };
        let back = ContractKey::from_contract_id(&id);
        prop_assert!(matches!(back, Ok(ref k) if *k == key));
    }

    /// `from_decimal_dollars` is deterministic: the same input always
    /// produces the same cents.
    #[test]
    fn from_decimal_dollars_deterministic(mantissa in 0i64..=i64::MAX, scale in 0u32..=10) {
        let d = rust_decimal::Decimal::new(mantissa, scale);
        let first = PriceCents::from_decimal_dollars(d);
        let second = PriceCents::from_decimal_dollars(d);
        match (first, second) {
            (Ok(a), Ok(b)) => prop_assert_eq!(a, b),
            (Err(_), Err(_)) => {}
            _ => prop_assert!(false, "non-deterministic conversion outcome"),
        }
    }

    /// `SimClock` is monotonic across a simulated run: driving a
    /// strictly-increasing `ts` sequence keeps `step` non-decreasing and `ts`
    /// strictly increasing, gaps are preserved, and any non-advancing `ts`
    /// (duplicate or reversed) is a `DataOutOfOrder` that leaves the clock
    /// state intact.
    #[test]
    fn clock_monotonic(
        start in -1_000_000_000_000i64..=1_000_000_000_000i64,
        gaps in prop::collection::vec(1i64..=1_000_000i64, 0..64),
    ) {
        let mut clock = SimClock::new();
        let mut prev_ts = clock.ts().value();
        let mut prev_step = clock.step().value();

        // First snapshot: the sentinel start guarantees the first advance
        // succeeds for any valid feed timestamp.
        let first = clock.advance_to(SimTime::new(start), StepIndex::new(0));
        prop_assert!(matches!(first, Ok(())));
        prop_assert!(clock.ts().value() > prev_ts);
        prop_assert!(clock.step().value() >= prev_step);
        prop_assert_eq!(clock.ts().value(), start);
        prev_ts = clock.ts().value();
        prev_step = clock.step().value();

        // Each gap advances ts strictly and step by one, gaps preserved exactly.
        let mut cur = start;
        let mut step: u32 = 0;
        for gap in &gaps {
            cur += *gap;
            step += 1;
            let outcome = clock.advance_to(SimTime::new(cur), StepIndex::new(step));
            prop_assert!(matches!(outcome, Ok(())));
            prop_assert!(clock.ts().value() > prev_ts);
            prop_assert!(clock.step().value() >= prev_step);
            prop_assert_eq!(clock.ts().value(), cur);
            prop_assert_eq!(clock.step().value(), step);
            prev_ts = clock.ts().value();
            prev_step = clock.step().value();
        }

        // A non-advancing ts errors and does not mutate the clock.
        let last_ts = clock.ts().value();
        let next_step = step + 1;
        let duplicate = clock.advance_to(SimTime::new(last_ts), StepIndex::new(next_step));
        let duplicate_is_ooo = matches!(duplicate, Err(BacktestError::DataOutOfOrder { .. }));
        prop_assert!(duplicate_is_ooo);
        let reversed = clock.advance_to(SimTime::new(last_ts - 1), StepIndex::new(next_step));
        let reversed_is_ooo = matches!(reversed, Err(BacktestError::DataOutOfOrder { .. }));
        prop_assert!(reversed_is_ooo);
        prop_assert_eq!(clock.ts().value(), last_ts);
        prop_assert_eq!(clock.step().value(), step);
    }

    /// Conversion preserves exactly the input strikes (tick-aligned, distinct),
    /// in sorted order, regardless of the feed's array order.
    #[test]
    fn chain_conversion_preserves_strikes(
        // Distinct tick multiples in [20, 20000) → strikes 100..100000 cents.
        multiples in prop::collection::hash_set(20u64..20_000, 1..40),
    ) {
        let mut strikes: Vec<u64> = multiples.iter().map(|m| m * 5).collect();
        strikes.sort_unstable();
        // Insert in reverse to prove the BTreeMap fixes the order.
        let quotes: Vec<RawQuote> = strikes
            .iter()
            .rev()
            .map(|&strike| abs_raw(strike, 100, 110))
            .collect();
        let snap = raw_quotes_to_snapshot(&conversion_meta(), &quotes);
        prop_assert!(snap.is_ok());
        let Ok(snap) = snap else { return Ok(()); };
        let got: Vec<u64> = snap.quotes.keys().map(|k| k.strike.value()).collect();
        prop_assert_eq!(got, strikes);
    }

    /// A bid/ask/strike that is not a multiple of the tick is rejected with
    /// `PriceNotTickAligned` — never silently rounded.
    #[test]
    fn price_rejected_when_not_tick_aligned(offset in 1u64..=4) {
        // ask = 110 + offset is not a multiple of the 5-cent tick.
        let quote = abs_raw(510_000, 100, 110 + offset);
        let result = raw_quotes_to_snapshot(&conversion_meta(), &[quote]);
        match result {
            Err(BacktestError::PriceNotTickAligned { price, tick }) => {
                prop_assert_eq!(price, 110 + offset);
                prop_assert_eq!(tick, 5);
            }
            other => prop_assert!(false, "expected PriceNotTickAligned, got {:?}", other.is_ok()),
        }
    }

    /// `Days(n)` resolves to exactly `ts_0 + n·86400·1e9` ns for integer `n`,
    /// anchored on `ts_0`, regardless of the snapshot's own timestamp — proven
    /// against an integer oracle.
    #[test]
    fn expiration_days_resolves_utc_calendar(
        n in 0u32..=3650,
        step_days in 0i64..=365,
    ) {
        let days = match Positive::new(f64::from(n)) {
            Ok(p) => p,
            Err(_) => return Ok(()),
        };
        let mut quote = abs_raw(510_000, 100, 110);
        quote.expiration = ExpirationDate::Days(days);
        // A meta whose snapshot ts is far from ts_0 must NOT change the anchor.
        let mut meta = conversion_meta();
        meta.ts = SimTime::new(TS0 + step_days * NANOS_PER_DAY);
        meta.step = StepIndex::new(1);
        let snap = raw_quotes_to_snapshot(&meta, &[quote]);
        prop_assert!(snap.is_ok());
        let Ok(snap) = snap else { return Ok(()); };
        let key = snap.quotes.keys().next();
        prop_assert!(key.is_some());
        let Some(key) = key else { return Ok(()); };
        // Integer oracle: pure ts_0 + n·86400e9, no calendar, no DST.
        let expected = TS0 + i64::from(n) * NANOS_PER_DAY;
        prop_assert!(matches!(key.expiration_ns(), Ok(ns) if ns == expected));
    }

    /// Scaffold for the v0.2 cross-mode parity guarantee: the shared `Fill`
    /// shape is mode-agnostic. For any field values, a fill built under
    /// `Naive` and one built under `Realistic` differ **only** in `mode` —
    /// the serialised field *shape* (its JSON key set) is identical, and
    /// re-stamping the mode makes the two fully equal, so analytics cannot
    /// tell which model produced a fill from its structure. The assembler
    /// that enforces this at the source is `pub(crate)` and unit-tested in
    /// `src/execution/mod.rs`; this scaffold pins the invariant at the public
    /// `Fill` boundary.
    #[test]
    fn fill_report_shape_mode_agnostic(
        ts in any::<i64>(),
        step in any::<u32>(),
        strike in any::<u64>(),
        is_call in any::<bool>(),
        is_long in any::<bool>(),
        quantity in 1u32..,
        price in any::<u64>(),
        fees in 0i64..,
        slippage in any::<i64>(),
    ) {
        let underlying = Underlying::new("SPX");
        prop_assert!(underlying.is_ok());
        let Ok(underlying) = underlying else { return Ok(()); };
        let quantity = Quantity::new(quantity);
        prop_assert!(quantity.is_ok());
        let Ok(quantity) = quantity else { return Ok(()); };
        let contract = ContractKey {
            underlying,
            expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(ts)),
            strike: PriceCents::new(strike),
            style: if is_call { OptionStyle::Call } else { OptionStyle::Put },
        };
        let side = if is_long { Side::Long } else { Side::Short };
        let fill = |mode| Fill {
            ts: SimTime::new(ts),
            step: StepIndex::new(step),
            contract: contract.clone(),
            side,
            quantity,
            price: PriceCents::new(price),
            fees: Cents::new(fees),
            slippage: Cents::new(slippage),
            mode,
        };
        let naive = fill(ExecutionMode::Naive);
        let realistic = fill(ExecutionMode::Realistic);

        // The serialised field shape is identical regardless of mode.
        let naive_json = serde_json::to_value(&naive);
        let realistic_json = serde_json::to_value(&realistic);
        prop_assert!(naive_json.is_ok() && realistic_json.is_ok());
        let (Ok(naive_json), Ok(realistic_json)) = (naive_json, realistic_json) else {
            return Ok(());
        };
        prop_assert_eq!(json_object_keys(&naive_json), json_object_keys(&realistic_json));

        // Only `mode` differs: re-stamping it makes the two fully equal.
        prop_assert_ne!(naive.mode, realistic.mode);
        let mut rebadged = naive;
        rebadged.mode = ExecutionMode::Realistic;
        prop_assert_eq!(rebadged, realistic);
    }

    /// The naive fill model, driven through the public seam, is honest about
    /// the two invariants analytics leans on: (a) a `FixedCents` slippage
    /// measured against `decision_mid == mid` is **always adverse**, i.e.
    /// `Fill.slippage ≥ 0` for both a buy filled above mid and a sell filled
    /// below mid (the §7.1 sign truth table, feeding
    /// `fill_and_step_sign_reconciliation`); and (b) the produced `Fill` has
    /// the mode-agnostic field shape and is stamped `Naive`. Zero configured
    /// slippage records exactly zero.
    #[test]
    fn naive_fill_slippage_is_adverse_and_shape_stable(
        strike in 100u64..1_000_000,
        mid in 1u64..1_000_000,
        cents in 0u64..500,
        is_long in any::<bool>(),
        quantity in 1u32..1_000,
    ) {
        let contract = naive_contract(strike);
        prop_assert!(contract.is_some());
        let Some(contract) = contract else { return Ok(()); };
        let snap = naive_snapshot(&contract, mid);
        prop_assert!(snap.is_some());
        let Some(snap) = snap else { return Ok(()); };
        let quantity = Quantity::new(quantity);
        prop_assert!(quantity.is_ok());
        let Ok(quantity) = quantity else { return Ok(()); };
        let side = if is_long { Side::Long } else { Side::Short };

        let mut model = NaiveFill::new(
            SlippageModel::FixedCents { cents },
            FeeSchedule { per_contract_cents: 0, per_order_cents: 0 },
        );
        let commands = [OrderCommand::Submit(OrderIntent {
            contract: contract.clone(),
            action: PositionAction::Open,
            side,
            quantity,
            limit: None,
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(mid), // decision_mid == mid
        })];
        let mut out = Vec::new();
        let result = model.fill(&commands, &snap, &mut out);
        prop_assert!(matches!(result, Ok(())));
        prop_assert_eq!(out.len(), 1); // always single-shot, always fills
        let Some(fill) = out.first() else { return Ok(()); };

        // (a) slippage is adverse (≥ 0); zero cents records exactly zero.
        prop_assert!(fill.slippage.value() >= 0);
        if cents == 0 {
            prop_assert_eq!(fill.slippage.value(), 0);
        }
        // filled the full intent, stamped Naive.
        prop_assert_eq!(fill.quantity.value(), quantity.value());
        prop_assert_eq!(fill.mode, ExecutionMode::Naive);

        // (b) the produced fill has the same field shape as a Realistic fill.
        let realistic = Fill { mode: ExecutionMode::Realistic, ..fill.clone() };
        let naive_json = serde_json::to_value(fill);
        let realistic_json = serde_json::to_value(&realistic);
        prop_assert!(naive_json.is_ok() && realistic_json.is_ok());
        let (Ok(naive_json), Ok(realistic_json)) = (naive_json, realistic_json) else {
            return Ok(());
        };
        prop_assert_eq!(json_object_keys(&naive_json), json_object_keys(&realistic_json));
    }
}

// --- engine replay-loop properties (issue #14) -----------------------------

/// A strategy that draws from `ctx.rng` every snapshot and opens the short call
/// once when a draw is even — exercises the RNG path so two same-seed runs must
/// produce byte-identical output.
struct RngProbe {
    opened: bool,
}

impl Strategy for RngProbe {
    fn on_start(
        &mut self,
        _ctx: &mut ChainContext,
        _out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        Ok(())
    }

    fn on_snapshot(
        &mut self,
        ctx: &mut ChainContext,
        out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        let draw = ctx.rng.next_u32();
        if self.opened || !draw.is_multiple_of(2) {
            return Ok(());
        }
        let target = ctx.snapshot.quotes.values().find(|q| {
            q.contract.strike == PriceCents::new(510_000) && q.contract.style == OptionStyle::Call
        });
        if let Some(quote) = target {
            let Ok(quantity) = Quantity::new(1) else {
                return Ok(());
            };
            out.push(OrderCommand::Submit(OrderIntent {
                contract: quote.contract.clone(),
                action: PositionAction::Open,
                side: Side::Long,
                quantity,
                limit: None,
                tif: TimeInForce::Ioc,
                decision_mid: quote.mid,
            }));
            self.opened = true;
        }
        Ok(())
    }

    fn on_end(
        &mut self,
        _ctx: &mut ChainContext,
        _out: &mut Vec<OrderCommand>,
    ) -> Result<(), BacktestError> {
        Ok(())
    }
}

/// Run the RNG-consuming strategy over a fixture at `path` with `seed`.
fn run_rng_probe(path: &std::path::Path, seed: u64) -> BacktestRun {
    let config = common::condor_config(path, seed);
    let Ok(feed) = ParquetFeed::open(path, &ResourceLimits::default()) else {
        panic!("the fixture opens");
    };
    let execution = NaiveFill::new(config.slippage.clone(), config.fees);
    let Ok(run) = BacktestEngine::run(&config, feed, execution, RngProbe { opened: false }, "rng")
    else {
        panic!("the rng run succeeds");
    };
    run
}

/// No look-ahead: perturbing a **future** snapshot leaves every earlier step's
/// equity point byte-identical, because the loop reads only `S_n` (and marks
/// from `S_n`'s mid) — it can never see `S_{n+1}`.
#[test]
fn test_no_look_ahead_future_perturbation_preserves_prefix() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir creates");
    };
    let base_path = dir.path().join("base.parquet");
    let perturbed_path = dir.path().join("perturbed.parquet");

    let base_rows = common::condor_rows(4, None);
    // Overwrite the short-call quote at step 2 (a FUTURE snapshot) with a
    // different tick-aligned mid; steps 0 and 1 must be unaffected.
    let perturb = common::Perturb {
        step: 2,
        strike: 510_000,
        style: "call",
        bid: 2_495,
        ask: 2_505,
    };
    let perturbed_rows = common::condor_rows(4, Some(perturb));

    if common::write_parquet(&base_path, &base_rows).is_err()
        || common::write_parquet(&perturbed_path, &perturbed_rows).is_err()
    {
        panic!("both fixtures must write");
    }

    let Ok(base) = common::run_condor(&base_path, 5) else {
        panic!("base run succeeds");
    };
    let Ok(perturbed) = common::run_condor(&perturbed_path, 5) else {
        panic!("perturbed run succeeds");
    };

    // Steps 0 and 1 (strictly before the perturbed step 2) are byte-identical.
    assert!(base.equity_curve.len() >= 3 && perturbed.equity_curve.len() >= 3);
    for step in 0..2usize {
        let (Some(a), Some(b)) = (
            base.equity_curve.get(step),
            perturbed.equity_curve.get(step),
        ) else {
            panic!("both curves have a point at step {step}");
        };
        assert_eq!(a, b, "future perturbation must not change step {step}");
    }
    // Sanity: the perturbation DID change the affected step, so the test is not
    // vacuous.
    let (Some(a2), Some(b2)) = (base.equity_curve.get(2), perturbed.equity_curve.get(2)) else {
        panic!("both curves have a point at step 2");
    };
    assert_ne!(
        a2.position_value_cents, b2.position_value_cents,
        "the perturbed step 2 mark must differ"
    );
}

/// Same seed + config + data ⇒ byte-identical output — for both a real
/// `IronCondor` (`from_spec`) and an RNG-consuming strategy that draws from
/// `ctx.rng`, so the randomness path is exercised.
#[test]
fn test_same_seed_same_result_iron_condor_and_rng_strategy() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir creates");
    };
    let path = dir.path().join("condor.parquet");
    let rows = common::condor_rows(5, None);
    if common::write_parquet(&path, &rows).is_err() {
        panic!("the fixture writes");
    }

    // Real IronCondor via from_spec.
    let (Ok(a), Ok(b)) = (
        common::run_condor(&path, 314),
        common::run_condor(&path, 314),
    ) else {
        panic!("both condor runs succeed");
    };
    assert_eq!(a.equity_curve, b.equity_curve);
    assert_eq!(a.open_at_end, b.open_at_end);

    // RNG-consuming strategy: same seed ⇒ identical draws ⇒ identical output.
    let c = run_rng_probe(&path, 271);
    let d = run_rng_probe(&path, 271);
    assert_eq!(c.equity_curve, d.equity_curve);
    assert_eq!(c.open_at_end, d.open_at_end);
}

// --- mark-to-market ledger properties (issue #15) --------------------------

/// The fixed contract multiplier of the ledger-property snapshots
/// ([`naive_snapshot`] builds a 100x spec).
const LEDGER_MULT: i128 = 100;

/// A resolved contract keyed at `strike`, expiring 365 days after `TS0` — far
/// beyond the `TS0` property snapshots, so a carry-forward is always pre-expiry
/// and the settlement-rejection path never trips these revaluation properties.
fn ledger_contract(strike: u64) -> Option<ContractKey> {
    let underlying = Underlying::new("SPX").ok()?;
    Some(ContractKey {
        underlying,
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(
            TS0 + 365 * NANOS_PER_DAY,
        )),
        strike: PriceCents::new(strike),
        style: OptionStyle::Call,
    })
}

/// One open leg on `contract`.
fn ledger_leg(contract: &ContractKey, side: Side, quantity: Quantity, entry: u64) -> OpenPosition {
    OpenPosition {
        position_id: PositionId::new(1),
        contract: contract.clone(),
        side,
        quantity,
        entry_premium: PriceCents::new(entry),
    }
}

/// A single naive fill on `contract` at `price` (zero slippage).
fn ledger_fill(
    contract: &ContractKey,
    side: Side,
    quantity: Quantity,
    price: u64,
    fees: i64,
) -> Fill {
    Fill {
        ts: SimTime::new(TS0),
        step: StepIndex::new(0),
        contract: contract.clone(),
        side,
        quantity,
        price: PriceCents::new(price),
        fees: Cents::new(fees),
        slippage: Cents::new(0),
        mode: ExecutionMode::Naive,
    }
}

proptest! {
    /// Cash-flow invariant: `cash` changes **only** through fills and fees.
    /// Revaluation-only settle steps (before and after a fill) never move cash;
    /// the single fill moves it by **exactly** `−side_sign·price·qty·mult −
    /// fees` — the two-invariants rule ([docs/02 §6](../../docs/02-engine-architecture.md)).
    #[test]
    fn cash_changes_only_by_fills_and_fees(
        initial in 1i64..=1_000_000_000_000,
        price in 1u64..=100_000,
        quantity in 1u32..=100,
        fees in 0i64..=1_000_000,
        is_long in any::<bool>(),
        pre_mids in prop::collection::vec(1u64..=100_000, 0..8),
        post_mids in prop::collection::vec(1u64..=100_000, 0..8),
    ) {
        let contract = ledger_contract(510_000);
        prop_assert!(contract.is_some());
        let Some(contract) = contract else { return Ok(()); };
        let quantity_q = Quantity::new(quantity);
        prop_assert!(quantity_q.is_ok());
        let Ok(quantity_q) = quantity_q else { return Ok(()); };
        let side = if is_long { Side::Long } else { Side::Short };
        let leg = ledger_leg(&contract, side, quantity_q, price);
        let mut ledger = Ledger::new(Cents::new(initial));

        // (1) revaluation-only steps BEFORE any fill leave cash == initial.
        for &mid in &pre_mids {
            let snap = naive_snapshot(&contract, mid);
            prop_assert!(snap.is_some());
            let Some(snap) = snap else { return Ok(()); };
            let point = ledger.settle(StepIndex::new(0), snap.ts, std::slice::from_ref(&leg), &snap);
            prop_assert!(point.is_ok());
            prop_assert_eq!(ledger.cash().value(), initial);
            if let Ok(point) = point {
                prop_assert_eq!(point.cash_cents, initial);
            }
        }

        // (2) one fill moves cash by EXACTLY -side_sign*price*qty*mult - fees.
        let side_sign: i128 = if is_long { 1 } else { -1 };
        let gross = i128::from(price) * i128::from(quantity) * LEDGER_MULT;
        let expected_cash = i128::from(initial) - side_sign * gross - i128::from(fees);
        prop_assert!(
            expected_cash >= i128::from(i64::MIN) && expected_cash <= i128::from(i64::MAX)
        );
        let applied = ledger.apply_fill(&ledger_fill(&contract, side, quantity_q, price, fees), 100);
        prop_assert!(applied.is_ok());
        prop_assert_eq!(i128::from(ledger.cash().value()), expected_cash);

        // (3) revaluation-only steps AFTER the fill leave cash at that value.
        let settled_cash = ledger.cash().value();
        for &mid in &post_mids {
            let snap = naive_snapshot(&contract, mid);
            prop_assert!(snap.is_some());
            let Some(snap) = snap else { return Ok(()); };
            let point = ledger.settle(StepIndex::new(0), snap.ts, std::slice::from_ref(&leg), &snap);
            prop_assert!(point.is_ok());
            prop_assert_eq!(ledger.cash().value(), settled_cash);
        }
    }

    /// Valuation invariant: every step `equity == cash + Σ(mark × qty × mult ×
    /// side_sign)`. Checked against an independent integer oracle for the leg
    /// value, and cash reads back unchanged by revaluation.
    #[test]
    fn equity_reconciles_cash_plus_position_value(
        initial in 1i64..=1_000_000_000_000,
        price in 1u64..=100_000,
        quantity in 1u32..=100,
        fees in 0i64..=1_000_000,
        is_long in any::<bool>(),
        mids in prop::collection::vec(1u64..=100_000, 1..8),
    ) {
        let contract = ledger_contract(510_000);
        prop_assert!(contract.is_some());
        let Some(contract) = contract else { return Ok(()); };
        let quantity_q = Quantity::new(quantity);
        prop_assert!(quantity_q.is_ok());
        let Ok(quantity_q) = quantity_q else { return Ok(()); };
        let side = if is_long { Side::Long } else { Side::Short };
        let leg = ledger_leg(&contract, side, quantity_q, price);
        let side_sign: i128 = if is_long { 1 } else { -1 };

        let mut ledger = Ledger::new(Cents::new(initial));
        let applied = ledger.apply_fill(&ledger_fill(&contract, side, quantity_q, price, fees), 100);
        prop_assert!(applied.is_ok());

        for &mid in &mids {
            let snap = naive_snapshot(&contract, mid);
            prop_assert!(snap.is_some());
            let Some(snap) = snap else { return Ok(()); };
            let point = ledger.settle(StepIndex::new(0), snap.ts, std::slice::from_ref(&leg), &snap);
            prop_assert!(point.is_ok());
            let Ok(point) = point else { return Ok(()); };
            // Independent oracle for the marked leg value.
            let expected_pv = side_sign * i128::from(mid) * i128::from(quantity) * LEDGER_MULT;
            prop_assert_eq!(i128::from(point.position_value_cents), expected_pv);
            // The valuation identity, both as emitted and against cash + oracle.
            prop_assert_eq!(point.equity_cents, point.cash_cents + point.position_value_cents);
            prop_assert_eq!(
                i128::from(point.equity_cents),
                i128::from(point.cash_cents) + expected_pv
            );
            // Revaluation did not move cash.
            prop_assert_eq!(point.cash_cents, ledger.cash().value());
        }
    }

    /// Drawdown is defined and never clamped across the whole range: `0` at a
    /// fresh peak, exactly `−1` at zero equity, and strictly below `−1` on
    /// negative equity (the [docs/01 §9](../../docs/01-domain-model.md) rule).
    #[test]
    fn drawdown_defined_at_zero_and_negative_equity(
        capital in 1i64..=1_000_000_000_000,
        mid in 1u64..=100_000,
    ) {
        let contract = ledger_contract(510_000);
        prop_assert!(contract.is_some());
        let Some(contract) = contract else { return Ok(()); };
        let one = Quantity::new(1);
        prop_assert!(one.is_ok());
        let Ok(one) = one else { return Ok(()); };

        // (a) fresh peak: an empty book → equity == capital == peak → 0.
        {
            let mut ledger = Ledger::new(Cents::new(capital));
            let snap = naive_snapshot(&contract, mid);
            prop_assert!(snap.is_some());
            let Some(snap) = snap else { return Ok(()); };
            let point = ledger.settle(StepIndex::new(0), snap.ts, &[], &snap);
            prop_assert!(point.is_ok());
            let Ok(point) = point else { return Ok(()); };
            prop_assert_eq!(point.equity_cents, capital);
            prop_assert!((point.drawdown - 0.0).abs() < f64::EPSILON);
        }

        // (b) zero equity: peak = mid*100, one short leg marked at mid →
        //     equity 0 → drawdown exactly -1 (not clamped away).
        {
            let peak = i64::try_from(i128::from(mid) * LEDGER_MULT).ok();
            prop_assert!(peak.is_some());
            let Some(peak) = peak else { return Ok(()); };
            let short = ledger_leg(&contract, Side::Short, one, mid);
            let mut ledger = Ledger::new(Cents::new(peak));
            let snap = naive_snapshot(&contract, mid);
            prop_assert!(snap.is_some());
            let Some(snap) = snap else { return Ok(()); };
            let point =
                ledger.settle(StepIndex::new(0), snap.ts, std::slice::from_ref(&short), &snap);
            prop_assert!(point.is_ok());
            let Ok(point) = point else { return Ok(()); };
            prop_assert_eq!(point.equity_cents, 0);
            prop_assert!((point.drawdown - (-1.0)).abs() < 1e-12);
        }

        // (c) negative equity: capital one cent below the liability → equity < 0
        //     → drawdown strictly below -1, reported as-is.
        {
            let liability = i128::from(mid) * LEDGER_MULT; // >= 100
            let cap = i64::try_from(liability - 1).ok();
            prop_assert!(cap.is_some());
            let Some(cap) = cap else { return Ok(()); };
            let short = ledger_leg(&contract, Side::Short, one, mid);
            let mut ledger = Ledger::new(Cents::new(cap));
            let snap = naive_snapshot(&contract, mid);
            prop_assert!(snap.is_some());
            let Some(snap) = snap else { return Ok(()); };
            let point =
                ledger.settle(StepIndex::new(0), snap.ts, std::slice::from_ref(&short), &snap);
            prop_assert!(point.is_ok());
            let Ok(point) = point else { return Ok(()); };
            prop_assert!(point.equity_cents < 0);
            prop_assert!(point.drawdown < -1.0);
        }
    }
}

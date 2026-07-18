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

use ironcondor::analytics::attribution::{
    RESIDUAL_WARN_FLOOR_CENTS, attribute, residual_is_notable,
};
use ironcondor::{
    AttributionSubstrate, BacktestEngine, BacktestRun, ChainContext, CsvFeed, DataFeed,
    LegAttributionSample, ParquetFeed, ResourceLimits, StepAttributionScalars, Strategy,
    UnitGreeks,
};
use ironcondor::{EquityPoint, FillRow, GreeksAttributionRow, PositionRow, RowCounts, RunId};
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

// --- result-bundle wire-shape properties (issue #33) -----------------------

proptest! {
    /// `bundle_serde_identity` — every bundle row type (and the manifest
    /// `RowCounts` / `RunId`) round-trips through JSON unchanged: serialise →
    /// deserialise → equal. This pins the wire shape of the four
    /// `ironcondor.bundle.v1` tables ([docs/01 §9](../../docs/01-domain-model.md#9-result-bundle-record-types)).
    #[test]
    #[allow(clippy::too_many_lines)]
    fn bundle_serde_identity(
        step in any::<u32>(),
        ts_ns in any::<i64>(),
        run_id in ".*",
        trade_id in any::<u64>(),
        position_id in any::<u64>(),
        order_id in any::<u64>(),
        fill_seq in any::<u32>(),
        underlying in ".*",
        expiration_ns in any::<i64>(),
        contract_id in ".*",
        strike_cents in any::<u64>(),
        style in ".*",
        side in ".*",
        quantity in any::<u32>(),
        price_cents in any::<u64>(),
        fees_cents in any::<i64>(),
        slippage_cents in any::<i64>(),
        mode in ".*",
        // finite drawdown ratio (≤ 0 by domain; NaN/Inf never reach a column).
        drawdown in -1_000.0f64..=0.0,
        cash_cents in any::<i64>(),
        position_value_cents in any::<i64>(),
        equity_cents in any::<i64>(),
        avg_price_cents in any::<u64>(),
        mark_cents in any::<u64>(),
        unrealized_cents in any::<i64>(),
        stale_mark in any::<bool>(),
        exit_reason in prop::option::of(".*"),
        open_at_end in any::<bool>(),
        theta in any::<i64>(),
        delta in any::<i64>(),
        vega in any::<i64>(),
        spread_capture in any::<i64>(),
        residual in any::<i64>(),
    ) {
        // fills.parquet
        let fill = FillRow {
            step,
            ts_ns,
            strategy_run_id: run_id.clone(),
            trade_id,
            position_id,
            order_id,
            fill_seq,
            underlying: underlying.clone(),
            expiration_ns,
            contract_id: contract_id.clone(),
            strike_cents,
            style: style.clone(),
            side: side.clone(),
            quantity,
            price_cents,
            fees_cents,
            slippage_cents,
            mode: mode.clone(),
        };
        let json = serde_json::to_string(&fill);
        prop_assert!(json.is_ok());
        let Ok(json) = json else { return Ok(()); };
        let back: Result<FillRow, _> = serde_json::from_str(&json);
        prop_assert!(matches!(back, Ok(ref r) if *r == fill));

        // equity_curve.parquet — integer columns exact; the one float column
        // (drawdown) within the oracle tolerance, since serde_json's default
        // float codec is not bit-exact and rule 11 forbids exact float equality.
        let point = EquityPoint::new(step, ts_ns, cash_cents, position_value_cents, equity_cents, drawdown);
        let json = serde_json::to_string(&point);
        prop_assert!(json.is_ok());
        let Ok(json) = json else { return Ok(()); };
        let back: Result<EquityPoint, _> = serde_json::from_str(&json);
        prop_assert!(back.is_ok());
        let Ok(back) = back else { return Ok(()); };
        prop_assert_eq!(back.step, point.step);
        prop_assert_eq!(back.ts_ns, point.ts_ns);
        prop_assert_eq!(back.cash_cents, point.cash_cents);
        prop_assert_eq!(back.position_value_cents, point.position_value_cents);
        prop_assert_eq!(back.equity_cents, point.equity_cents);
        let tol = f64::max(1e-9, 1e-6 * point.drawdown.abs().max(back.drawdown.abs()));
        prop_assert!((back.drawdown - point.drawdown).abs() <= tol);

        // positions.parquet — exit_reason is the only nullable column.
        let position = PositionRow {
            step,
            ts_ns,
            position_id,
            trade_id,
            contract_id,
            side,
            quantity,
            avg_price_cents,
            mark_cents,
            unrealized_cents,
            stale_mark,
            exit_reason,
            open_at_end,
        };
        let json = serde_json::to_string(&position);
        prop_assert!(json.is_ok());
        let Ok(json) = json else { return Ok(()); };
        let back: Result<PositionRow, _> = serde_json::from_str(&json);
        prop_assert!(matches!(back, Ok(ref r) if *r == position));

        // greeks_attribution.parquet
        let attribution = GreeksAttributionRow::new(step, ts_ns, theta, delta, vega, spread_capture, fees_cents, residual);
        let json = serde_json::to_string(&attribution);
        prop_assert!(json.is_ok());
        let Ok(json) = json else { return Ok(()); };
        let back: Result<GreeksAttributionRow, _> = serde_json::from_str(&json);
        prop_assert!(matches!(back, Ok(ref r) if *r == attribution));

        // manifest row_counts
        let counts = RowCounts::new(trade_id, position_id, order_id, u64::from(fill_seq));
        let json = serde_json::to_string(&counts);
        prop_assert!(json.is_ok());
        let Ok(json) = json else { return Ok(()); };
        let back: Result<RowCounts, _> = serde_json::from_str(&json);
        prop_assert!(matches!(back, Ok(ref r) if *r == counts));

        // run_id is transparent over its bare string.
        let id = RunId::from_hex(run_id.clone());
        let json = serde_json::to_string(&id);
        prop_assert!(json.is_ok());
        let Ok(json) = json else { return Ok(()); };
        let back: Result<RunId, _> = serde_json::from_str(&json);
        prop_assert!(matches!(back, Ok(ref r) if r.as_str() == run_id));
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

/// `same_seed_same_result` over a **`ShortStrangle`** run (#28): the v0.2 second
/// strategy, driven through the same generic adapter + engine, is byte-identical
/// for a fixed `(seed, config, data)`. Determinism is a property of the loop,
/// not of `IronCondor`.
#[test]
fn test_same_seed_same_result_short_strangle() {
    let Ok(dir) = tempfile::tempdir() else {
        panic!("tempdir creates");
    };
    let path = dir.path().join("strangle.parquet");
    let rows = common::strangle_rows(5);
    if common::write_parquet(&path, &rows).is_err() {
        panic!("the strangle fixture writes");
    }

    let (Ok(a), Ok(b)) = (
        common::run_strangle(&path, 314),
        common::run_strangle(&path, 314),
    ) else {
        panic!("both short strangle runs succeed");
    };
    assert_eq!(a.equity_curve, b.equity_curve);
    assert_eq!(a.open_at_end, b.open_at_end);
    assert_eq!(a.result.final_capital, b.result.final_capital);
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

// --- realistic-mode cents↔tick scaling (feature `orderbook`, #22) -------------

/// A resolved call contract at `TS0 + 30 days` for the realistic-scaling
/// properties.
#[cfg(feature = "orderbook")]
fn ob_contract() -> ContractKey {
    let underlying = match Underlying::new("SPX") {
        Ok(u) => u,
        Err(e) => unreachable!("SPX is valid: {e}"),
    };
    ContractKey {
        underlying,
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(
            TS0 + 30 * NANOS_PER_DAY,
        )),
        strike: PriceCents::new(510_000),
        style: OptionStyle::Call,
    }
}

/// A one-contract snapshot for [`ob_contract`] with the given tick and touch.
#[cfg(feature = "orderbook")]
fn ob_snapshot(tick: u64, bid: u64, ask: u64) -> Option<ChainSnapshot> {
    let underlying = Underlying::new("SPX").ok()?;
    let spec = InstrumentSpec::new(PriceCents::new(tick), 100).ok()?;
    let size = Quantity::new(10).ok()?;
    let quote = QuoteView {
        contract: ob_contract(),
        bid: PriceCents::new(bid),
        ask: PriceCents::new(ask),
        mid: PriceCents::new((bid + ask) / 2),
        bid_size: size,
        ask_size: size,
        implied_volatility: Decimal::ZERO,
        delta: Decimal::ZERO,
        gamma: Decimal::ZERO,
        theta: Decimal::ZERO,
        vega: Decimal::ZERO,
    };
    let mut quotes = BTreeMap::new();
    quotes.insert(ob_contract(), quote);
    Some(ChainSnapshot {
        ts: SimTime::new(TS0),
        step: StepIndex::new(0),
        underlying,
        underlying_price: PriceCents::new(510_000),
        spec,
        quotes,
    })
}

#[cfg(feature = "orderbook")]
proptest! {
    /// A tick-aligned executed price survives the cents→tick→cents round-trip:
    /// seed an ask at a tick-aligned `price`, cross it with a marketable buy,
    /// and the resulting `Fill.price` is exactly `price` — the scaling is
    /// lossless for any tick the book executes at.
    #[test]
    fn realistic_price_to_tick_roundtrip(tick_idx in 0usize..5, ticks_count in 1u64..1000) {
        let tick = [1u64, 5, 10, 25, 100][tick_idx];
        let price = ticks_count * tick; // tick-aligned by construction
        let one = match Quantity::new(1) { Ok(q) => q, Err(_) => return Ok(()) };
        let fees = FeeSchedule { per_contract_cents: 0, per_order_cents: 0 };
        let mut model = ironcondor::RealisticFill::new(fees, 10, 7);
        let seeded = model.seed_maker_limit(
            &ob_contract(), true, PriceCents::new(price), one, PriceCents::new(tick),
        );
        prop_assert!(seeded.is_ok());
        let Some(snap) = ob_snapshot(tick, price, price) else { return Ok(()); };
        let buy = OrderCommand::Submit(OrderIntent {
            contract: ob_contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: one,
            limit: None,
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(price),
        });
        let mut out: Vec<Fill> = Vec::new();
        prop_assert!(model.fill(&[buy], &snap, &mut out).is_ok());
        prop_assert_eq!(out.len(), 1);
        let Some(fill) = out.first() else { return Ok(()); };
        prop_assert_eq!(fill.price.value(), price);
    }

    /// A strategy limit off the tick grid is rejected with
    /// [`BacktestError::PriceNotTickAligned`], carrying the offending price and
    /// tick, before any book operation.
    #[test]
    fn realistic_price_rejected_when_not_tick_aligned(tick_idx in 0usize..4, base in 1u64..1000, off_raw in 0u64..1_000_000) {
        let tick = [5u64, 10, 25, 100][tick_idx];
        let off = 1 + (off_raw % (tick - 1)); // 1..=tick-1 ⇒ never a multiple
        let price = base * tick + off;
        let one = match Quantity::new(1) { Ok(q) => q, Err(_) => return Ok(()) };
        let fees = FeeSchedule { per_contract_cents: 0, per_order_cents: 0 };
        let mut model = ironcondor::RealisticFill::new(fees, 10, 7);
        let Some(snap) = ob_snapshot(tick, 0, price + tick) else { return Ok(()); };
        let submit = OrderCommand::Submit(OrderIntent {
            contract: ob_contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: one,
            limit: Some(PriceCents::new(price)),
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(price),
        });
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(&[submit], &snap, &mut out);
        // Bind the match to a bool first: `prop_assert!` stringifies its
        // condition into a format string, and `{ price: p }` braces would be
        // read as format placeholders.
        let rejected = match result {
            Err(BacktestError::PriceNotTickAligned { price: p, tick: t }) => p == price && t == tick,
            _ => false,
        };
        prop_assert!(rejected);
        prop_assert!(out.is_empty());
    }

    /// Realistic per-level slippage obeys the §7.1 sign truth table (feeding
    /// `fill_and_step_sign_reconciliation`): a marketable buy that walks a
    /// three-level ask ladder against a **fixed** decision-time mid (the touch)
    /// records, on every level, `slippage = (price − decision_mid) × qty`
    /// (side_sign +1 for a long) — **positive exactly when the fill is worse
    /// than the mid** and zero at the touch. The prices are non-decreasing along
    /// the walk (progressively deeper), so the impact is emergent, not
    /// configured, and never sign-flips twice.
    #[test]
    fn realistic_per_level_slippage_positive_when_adverse(
        tick_idx in 0usize..5,
        base in 1u64..500,
        s0 in 1u32..20,
        s1 in 1u32..20,
        s2 in 1u32..20,
    ) {
        let tick = [1u64, 5, 10, 25, 100][tick_idx];
        let touch = base * tick; // tick-aligned by construction
        let fees = FeeSchedule { per_contract_cents: 0, per_order_cents: 0 };
        let mut model = ironcondor::RealisticFill::new(fees, 10, 7);
        // Seed three ask levels one tick apart: touch, touch+tick, touch+2·tick.
        for (i, size) in [s0, s1, s2].into_iter().enumerate() {
            let level = u64::try_from(i).unwrap_or(0);
            let price = touch + level * tick;
            let q = match Quantity::new(size) { Ok(q) => q, Err(_) => return Ok(()) };
            let seeded = model.seed_maker_limit(
                &ob_contract(), true, PriceCents::new(price), q, PriceCents::new(tick),
            );
            prop_assert!(seeded.is_ok());
        }
        let total = s0 + s1 + s2;
        let qty = match Quantity::new(total) { Ok(q) => q, Err(_) => return Ok(()) };
        // decision_mid fixed at the touch (never re-read post-impact).
        let Some(snap) = ob_snapshot(tick, touch, touch) else { return Ok(()); };
        let buy = OrderCommand::Submit(OrderIntent {
            contract: ob_contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: qty,
            limit: None,
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(touch),
        });
        let mut out: Vec<Fill> = Vec::new();
        prop_assert!(model.fill(&[buy], &snap, &mut out).is_ok());
        prop_assert_eq!(out.len(), 3); // the buy walks all three seeded levels

        let mut prev_price = 0u64;
        for fill in &out {
            // prices are non-decreasing along the walk (deeper = worse).
            prop_assert!(fill.price.value() >= prev_price);
            prev_price = fill.price.value();
            // §7.1: slippage = (price − mid) × qty × (+1) for a long buy.
            let expected = i64::from(fill.quantity.value())
                * (i64::try_from(fill.price.value()).unwrap_or(i64::MAX)
                    - i64::try_from(touch).unwrap_or(i64::MAX));
            prop_assert_eq!(fill.slippage.value(), expected);
            // positive exactly when adverse (fill above the decision mid).
            if fill.price.value() > touch {
                prop_assert!(fill.slippage.value() > 0);
            } else {
                prop_assert_eq!(fill.slippage.value(), 0);
            }
        }
    }
}

// --- realistic between-snapshot refresh determinism (feature `orderbook`, #25)

/// A stepped, per-side-sized snapshot for [`ob_contract`] — the multi-snapshot
/// refresh determinism fixture (#025).
#[cfg(feature = "orderbook")]
fn ob_snapshot_full(
    step: u32,
    bid: u64,
    ask: u64,
    bid_size: u32,
    ask_size: u32,
) -> Option<ChainSnapshot> {
    let underlying = Underlying::new("SPX").ok()?;
    let spec = InstrumentSpec::new(PriceCents::new(5), 100).ok()?;
    let bq = Quantity::new(bid_size).ok()?;
    let aq = Quantity::new(ask_size).ok()?;
    let quote = QuoteView {
        contract: ob_contract(),
        bid: PriceCents::new(bid),
        ask: PriceCents::new(ask),
        mid: PriceCents::new((bid + ask) / 2),
        bid_size: bq,
        ask_size: aq,
        implied_volatility: Decimal::ZERO,
        delta: Decimal::ZERO,
        gamma: Decimal::ZERO,
        theta: Decimal::ZERO,
        vega: Decimal::ZERO,
    };
    let mut quotes = BTreeMap::new();
    quotes.insert(ob_contract(), quote);
    Some(ChainSnapshot {
        ts: SimTime::new(TS0 + i64::from(step)),
        step: StepIndex::new(step),
        underlying,
        underlying_price: PriceCents::new(510_000),
        spec,
        quotes,
    })
}

/// #025 determinism: `same_seed_same_result` over a **multi-snapshot realistic
/// run** exercising the between-snapshot refresh. Two runs over the same three
/// consecutive snapshots — a resting strategy limit the reseed crosses on the
/// final snapshot — produce byte-identical fill sequences (step, price, size,
/// fees, slippage). The refresh (cancel stale seed → reseed in fixed order →
/// capture) draws no wall clock and no unseeded RNG, so the tape fully pins the
/// output.
#[cfg(feature = "orderbook")]
#[test]
fn test_same_seed_same_result_realistic_multi_snapshot_refresh() {
    let run = || -> Vec<(u32, u64, u32, i64, i64)> {
        let profile = ironcondor::LiquidityProfile {
            touch_size: ironcondor::TouchSize::QuotedSize,
            depth_levels: 0,
            decay: Decimal::new(5, 1),
        };
        let fees = FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        };
        let mut model = ironcondor::RealisticFill::with_liquidity_profile(fees, 10, 7, profile);
        let one = match Quantity::new(1) {
            Ok(q) => q,
            Err(e) => panic!("1 is a valid quantity: {e}"),
        };
        // A GTC strategy buy at 500: rests at snap0, untouched at snap1, crossed
        // by the reseed at snap2 (a refresh-generated fill).
        let buy = OrderCommand::Submit(OrderIntent {
            contract: ob_contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: one,
            limit: Some(PriceCents::new(500)),
            tif: TimeInForce::Gtc,
            decision_mid: PriceCents::new(550),
        });
        let snaps = [
            ob_snapshot_full(0, 490, 600, 20, 20),
            ob_snapshot_full(1, 490, 540, 20, 20),
            ob_snapshot_full(2, 490, 500, 20, 20),
        ];
        let mut fills: Vec<(u32, u64, u32, i64, i64)> = Vec::new();
        let mut out: Vec<Fill> = Vec::new();
        for (i, snap) in snaps.iter().enumerate() {
            let Some(snap) = snap else {
                panic!("every refresh snapshot must build");
            };
            out.clear();
            let cmds: Vec<OrderCommand> = if i == 0 {
                vec![buy.clone()]
            } else {
                Vec::new()
            };
            match model.fill(&cmds, snap, &mut out) {
                Ok(()) => {}
                Err(e) => panic!("the multi-snapshot refresh run must succeed: {e}"),
            }
            for f in &out {
                fills.push((
                    f.step.value(),
                    f.price.value(),
                    f.quantity.value(),
                    f.fees.value(),
                    f.slippage.value(),
                ));
            }
        }
        fills
    };
    let a = run();
    let b = run();
    assert!(
        !a.is_empty(),
        "the resting buy fills on the crossing snapshot"
    );
    assert_eq!(
        a, b,
        "same (seed, config, data) ⇒ byte-identical refresh fills"
    );
}

proptest! {
    /// A non-tick-aligned ask sourced from a CSV file is rejected with
    /// `PriceNotTickAligned` — the same guarantee, now over the CSV feed path
    /// (#27). The value carried in the error is the offending cents price.
    #[test]
    fn csv_price_rejected_when_not_tick_aligned(offset in 1u64..=4) {
        let ask_cents = 2_000 + offset;
        let Ok(ask) = i64::try_from(ask_cents) else { return Ok(()); };
        let rows: Vec<common::Row> = vec![(0, TS0, 510_000, "call", 1_995, ask)];
        let Ok(dir) = tempfile::tempdir() else { return Ok(()); };
        let csv_dir = dir.path().join("csv");
        prop_assert!(common::write_csv_dir(&csv_dir, &rows).is_ok());
        match CsvFeed::open(&csv_dir, &ResourceLimits::default()) {
            Err(BacktestError::PriceNotTickAligned { price, tick }) => {
                prop_assert_eq!(price, ask_cents);
                prop_assert_eq!(tick, 5);
            }
            other => prop_assert!(false, "expected PriceNotTickAligned, got {:?}", other.is_ok()),
        }
    }

    /// Conversion over a CSV file preserves exactly the input strikes, in sorted
    /// order, regardless of the file's row order (#27).
    #[test]
    fn csv_chain_conversion_preserves_strikes(
        multiples in prop::collection::hash_set(20u64..20_000, 1..24),
    ) {
        let mut strikes: Vec<u64> = multiples.iter().map(|m| m * 5).collect();
        strikes.sort_unstable();
        // Rows written in reverse strike order to prove the BTreeMap fixes it.
        let mut rows: Vec<common::Row> = Vec::new();
        for &strike in strikes.iter().rev() {
            let Ok(strike_i) = i64::try_from(strike) else { return Ok(()); };
            rows.push((0, TS0, strike_i, "call", 100, 110));
        }
        let Ok(dir) = tempfile::tempdir() else { return Ok(()); };
        let csv_dir = dir.path().join("csv");
        prop_assert!(common::write_csv_dir(&csv_dir, &rows).is_ok());
        let Ok(mut feed) = CsvFeed::open(&csv_dir, &ResourceLimits::default()) else {
            return Err(TestCaseError::fail("csv feed must open"));
        };
        let Ok(Some(snap)) = feed.next() else {
            return Err(TestCaseError::fail("one snapshot must yield"));
        };
        let got: Vec<u64> = snap.quotes.keys().map(|k| k.strike.value()).collect();
        prop_assert_eq!(got, strikes);
    }
}

// --- P&L attribution by Greek (issue #31) ----------------------------------

/// The condor fixtures' initial capital ([`common::condor_config`]).
const ATTR_INITIAL_CAPITAL: i64 = 10_000_000;
/// Nanoseconds in one calendar day of exactly 86_400 s — the `Δt_days` base.
const ATTR_NANOS_PER_DAY: i64 = 86_400_000_000_000;
/// Dollars → cents: theta/vega are dollar-denominated, so their cents term
/// carries this factor (delta is a ratio and does NOT — see docs/05 §3).
const DOLLARS_TO_CENTS: i128 = 100;
/// Decimal IV → percentage points: a 0.01 decimal move is 1 pp.
const IV_DECIMAL_TO_PP: i128 = 100;

/// A `UnitGreeks` with the given per-contract unit sensitivities.
fn attr_greeks(delta: Decimal, theta: Decimal, vega: Decimal, iv: Decimal) -> UnitGreeks {
    UnitGreeks {
        delta,
        gamma: Decimal::ZERO,
        theta,
        vega,
        implied_volatility: iv,
    }
}

/// A one-step, single-leg substrate — the focused unit-of-analysis for the
/// mis-scaling guards. `prior_ts`/`prior_underlying` are the previous endpoints;
/// pass them equal to `ts`/`underlying` to zero a market delta.
#[allow(clippy::too_many_arguments)]
fn attr_single_leg(
    prior: Option<UnitGreeks>,
    current_iv: Decimal,
    quantity: u32,
    multiplier: u32,
    side: Side,
    ts: i64,
    prior_ts: i64,
    underlying: u64,
    prior_underlying: u64,
    spread_capture: i64,
    fees: i64,
    step_pnl: i64,
) -> AttributionSubstrate {
    let leg = LegAttributionSample {
        prior_greeks: prior,
        current_iv,
        quantity,
        contract_multiplier: multiplier,
        side,
    };
    let scalars = StepAttributionScalars {
        step: 0,
        ts_ns: ts,
        prior_ts_ns: prior_ts,
        underlying_cents: underlying,
        prior_underlying_cents: prior_underlying,
        spread_capture_cents: spread_capture,
        fees_cents: fees,
        step_pnl_cents: step_pnl,
        leg_count: 1,
    };
    AttributionSubstrate {
        steps: vec![scalars],
        legs: vec![leg],
    }
}

/// Banker's-rounded `value` to whole integer cents — the same policy the
/// production pass applies once per term (ADR-0003). The independent oracle.
fn attr_round_cents(value: Decimal) -> i64 {
    use rust_decimal::prelude::ToPrimitive;
    value
        .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::MidpointNearestEven)
        .to_i64()
        .unwrap_or(i64::MAX)
}

/// `side_sign` as an `i128` (`+1` long, `−1` short).
fn attr_side_sign(side: Side) -> i128 {
    match side {
        Side::Long => 1,
        Side::Short => -1,
    }
}

/// The full reconciliation identity for one row, in `i128` cents.
fn attr_identity_lhs(row: &ironcondor::GreeksAttributionRow) -> i128 {
    i128::from(row.theta_pnl_cents)
        + i128::from(row.delta_pnl_cents)
        + i128::from(row.vega_pnl_cents)
        + i128::from(row.spread_capture_cents)
        - i128::from(row.fees_cents)
        + i128::from(row.residual_cents)
}

/// Run the canonical condor over a freshly-written fixture and return its
/// attribution rows plus the equity curve, so the identity can be checked
/// against the INDEPENDENT `equity_n − equity_{n-1}`.
fn attr_run_condor(
    steps: u32,
    seed: u64,
) -> Option<(
    Vec<ironcondor::GreeksAttributionRow>,
    Vec<ironcondor::EquityPoint>,
)> {
    let dir = tempfile::tempdir().ok()?;
    let path = dir.path().join("condor.parquet");
    let rows = common::condor_rows(steps, None);
    common::write_parquet(&path, &rows).ok()?;
    let run = common::run_condor(&path, seed).ok()?;
    let attribution = attribute(&run.attribution_substrate).ok()?;
    Some((attribution, run.equity_curve))
}

proptest! {
    /// The golden reconciliation invariant: for EVERY step, the attribution
    /// terms sum **exactly** in integer cents to the ledger's mark-to-market
    /// `step_pnl = equity_n − equity_{n-1}` (step 0: `equity_0 − initial`).
    /// Checked against the INDEPENDENT equity curve, so it proves the whole
    /// engine-substrate → analytics chain reconciles, not just the residual's
    /// self-consistency.
    #[test]
    fn pnl_attribution_sums_to_step_pnl(steps in 2u32..=8, seed in any::<u64>()) {
        let Some((rows, curve)) = attr_run_condor(steps, seed) else {
            return Err(TestCaseError::fail("the condor run + attribution must succeed"));
        };
        prop_assert_eq!(rows.len(), curve.len());
        prop_assert_eq!(rows.len(), steps as usize);
        let mut prev_equity = ATTR_INITIAL_CAPITAL;
        for (row, point) in rows.iter().zip(curve.iter()) {
            // Independent step_pnl straight off the equity curve.
            let expected = i128::from(point.equity_cents) - i128::from(prev_equity);
            prev_equity = point.equity_cents;
            prop_assert_eq!(
                attr_identity_lhs(row),
                expected,
                "attribution must reconcile to equity delta at step {}",
                row.step
            );
        }
    }

    /// Step 0 reconciles the initial-capital baseline: all Greek terms are `0`
    /// (no `S_{-1}`), and the residual closes `step_pnl_0 = equity_0 − initial`
    /// exactly.
    #[test]
    fn step_zero_attribution_reconciles(steps in 1u32..=6, seed in any::<u64>()) {
        let Some((rows, curve)) = attr_run_condor(steps, seed) else {
            return Err(TestCaseError::fail("the condor run + attribution must succeed"));
        };
        let (Some(row0), Some(point0)) = (rows.first(), curve.first()) else {
            return Err(TestCaseError::fail("step 0 must exist"));
        };
        prop_assert_eq!(row0.step, 0);
        prop_assert_eq!(row0.theta_pnl_cents, 0, "no S_-1 ⇒ theta term 0 at step 0");
        prop_assert_eq!(row0.delta_pnl_cents, 0, "no S_-1 ⇒ delta term 0 at step 0");
        prop_assert_eq!(row0.vega_pnl_cents, 0, "no S_-1 ⇒ vega term 0 at step 0");
        // residual closes the initial-capital baseline.
        let expected = i128::from(point0.equity_cents) - i128::from(ATTR_INITIAL_CAPITAL);
        prop_assert_eq!(attr_identity_lhs(row0), expected);
        // The residual is exactly step_pnl minus the (spread_capture − fees).
        let expected_residual =
            expected - (i128::from(row0.spread_capture_cents) - i128::from(row0.fees_cents));
        prop_assert_eq!(i128::from(row0.residual_cents), expected_residual);
    }

    /// The theta term uses the **daily** greek against a **day** delta. The
    /// production term equals the daily oracle exactly; a ~365× annual mis-scale
    /// (theta annual, or Δt in years) would produce a DIFFERENT value, so this
    /// test discriminates it.
    #[test]
    fn theta_term_uses_daily_greek_and_day_delta(
        theta_milli in -500i64..=-1, // negative daily theta, in 1/1000 dollars
        day_count in 1i64..=30,
        quantity in 1u32..=10,
        is_long in any::<bool>(),
    ) {
        let theta = Decimal::new(theta_milli, 3);
        let side = if is_long { Side::Long } else { Side::Short };
        let mult = 100u32;
        let ts = TS0 + day_count * ATTR_NANOS_PER_DAY;
        let substrate = attr_single_leg(
            Some(attr_greeks(Decimal::ZERO, theta, Decimal::ZERO, Decimal::new(20, 2))),
            Decimal::new(20, 2), // iv unchanged ⇒ vega term 0
            quantity, mult, side,
            ts, TS0, 500_000, 500_000, // ΔS = 0
            0, 0, 0,
        );
        let Ok(rows) = attribute(&substrate) else {
            return Err(TestCaseError::fail("attribution must succeed"));
        };
        let Some(row) = rows.first() else {
            return Err(TestCaseError::fail("one row"));
        };
        let weight = i128::from(quantity) * i128::from(mult) * attr_side_sign(side);
        // Correct DAILY oracle: theta · Δt_days · 100 · weight.
        let dt_days = Decimal::from(day_count);
        let correct = attr_round_cents(
            theta * dt_days * Decimal::from(DOLLARS_TO_CENTS as i64)
                * Decimal::from(i64::try_from(weight).unwrap_or(i64::MAX)),
        );
        prop_assert_eq!(row.theta_pnl_cents, correct, "theta term uses the daily base");
        // ANNUAL mis-scale: Δt in years = day_count / 365 ⇒ ≈ 365× smaller.
        let dt_years = dt_days / Decimal::from(365);
        let mis = attr_round_cents(
            theta * dt_years * Decimal::from(DOLLARS_TO_CENTS as i64)
                * Decimal::from(i64::try_from(weight).unwrap_or(i64::MAX)),
        );
        // The discriminator only bites on a NON-degenerate term: a term that
        // rounds to 0 cents cannot distinguish the mis-scale (both round to 0),
        // so `correct == mis == 0` is a false failure, not a mis-scale escaping.
        // Guard it (never commit that degenerate seed); `correct != 0` ⇒
        // `|mis| ≈ |correct|/365` differs, so the guard still fails under the
        // ~365× annual mis-scale. The value equality above still runs for every
        // case, including the degenerate ones.
        prop_assume!(correct != 0);
        prop_assert_ne!(correct, mis, "a ~365× annual mis-scale would change the term");
    }

    /// The vega term uses the **per-percentage-point** greek against a **pp** IV
    /// delta. Production equals the pp oracle; a 100× decimal-IV mis-scale
    /// (using the raw 0.01 decimal move as ΔIV) would differ.
    #[test]
    fn vega_term_uses_per_point_greek_and_pp_delta(
        vega_milli in 1i64..=500,      // dollars/pp, in 1/1000
        iv_move_bp in 1i64..=500,      // IV move in 1/10000 (basis points of σ)
        quantity in 1u32..=10,
        is_long in any::<bool>(),
    ) {
        let vega = Decimal::new(vega_milli, 3);
        let prior_iv = Decimal::new(2000, 4); // 0.2000
        let current_iv = prior_iv + Decimal::new(iv_move_bp, 4);
        let side = if is_long { Side::Long } else { Side::Short };
        let mult = 100u32;
        let substrate = attr_single_leg(
            Some(attr_greeks(Decimal::ZERO, Decimal::ZERO, vega, prior_iv)),
            current_iv,
            quantity, mult, side,
            TS0, TS0, 500_000, 500_000, // Δt = 0, ΔS = 0
            0, 0, 0,
        );
        let Ok(rows) = attribute(&substrate) else {
            return Err(TestCaseError::fail("attribution must succeed"));
        };
        let Some(row) = rows.first() else {
            return Err(TestCaseError::fail("one row"));
        };
        let weight = i128::from(quantity) * i128::from(mult) * attr_side_sign(side);
        let weight_dec = Decimal::from(i64::try_from(weight).unwrap_or(i64::MAX));
        let iv_move_decimal = current_iv - prior_iv;
        // Correct: vega · (Δiv × 100 pp) · 100 (dollars→cents) · weight.
        let delta_iv_pp = iv_move_decimal * Decimal::from(IV_DECIMAL_TO_PP as i64);
        let correct = attr_round_cents(
            vega * delta_iv_pp * Decimal::from(DOLLARS_TO_CENTS as i64) * weight_dec,
        );
        prop_assert_eq!(row.vega_pnl_cents, correct, "vega term uses the per-pp base");
        // 100× decimal mis-scale: use the raw decimal move (no ×100 to pp).
        let mis = attr_round_cents(
            vega * iv_move_decimal * Decimal::from(DOLLARS_TO_CENTS as i64) * weight_dec,
        );
        // The discriminator only bites on a NON-degenerate term: a term that
        // rounds to 0 cents cannot distinguish the mis-scale (the 100×-smaller
        // `mis` also rounds to 0), so `correct == mis == 0` is a false failure,
        // not a mis-scale escaping. Guard it (never commit that degenerate seed);
        // `correct != 0` ⇒ `|mis| ≈ |correct|/100` differs, so the guard still
        // fails under the 100× mis-scale. The value equality above still runs for
        // every case, including the degenerate ones.
        prop_assume!(correct != 0);
        prop_assert_ne!(correct, mis, "a 100× decimal-IV mis-scale would change the term");
    }

    /// Each greek term weights `quantity × multiplier × side_sign` **exactly
    /// once**. Production equals the once-weighted oracle; an `N²`
    /// double-quantity (weighting a value that already folded in quantity) would
    /// differ for any quantity > 1.
    #[test]
    fn greek_terms_weight_quantity_once(
        delta_milli in -1000i64..=1000,
        ds_cents in -500i64..=500,
        quantity in 2u32..=10, // > 1 so N² is distinguishable
        is_long in any::<bool>(),
    ) {
        let delta = Decimal::new(delta_milli, 3);
        let side = if is_long { Side::Long } else { Side::Short };
        let mult = 100u32;
        let underlying = 500_000u64;
        let prior_underlying = i64::try_from(underlying)
            .ok()
            .and_then(|u| u.checked_sub(ds_cents))
            .and_then(|v| u64::try_from(v).ok());
        let Some(prior_underlying) = prior_underlying else {
            return Ok(());
        };
        let substrate = attr_single_leg(
            Some(attr_greeks(delta, Decimal::ZERO, Decimal::ZERO, Decimal::new(20, 2))),
            Decimal::new(20, 2),
            quantity, mult, side,
            TS0, TS0, underlying, prior_underlying, // Δt = 0
            0, 0, 0,
        );
        let Ok(rows) = attribute(&substrate) else {
            return Err(TestCaseError::fail("attribution must succeed"));
        };
        let Some(row) = rows.first() else {
            return Err(TestCaseError::fail("one row"));
        };
        let ds = i128::from(underlying) - i128::from(prior_underlying);
        let weight_once = i128::from(quantity) * i128::from(mult) * attr_side_sign(side);
        // delta · ΔS_cents · (weight once). Delta is a ratio ⇒ no ×100.
        let correct = attr_round_cents(
            delta
                * Decimal::from(i64::try_from(ds).unwrap_or(0))
                * Decimal::from(i64::try_from(weight_once).unwrap_or(i64::MAX)),
        );
        prop_assert_eq!(row.delta_pnl_cents, correct, "quantity weighted once");
        // N² mis-scale: weight = quantity² × mult × side_sign.
        let weight_squared =
            i128::from(quantity) * i128::from(quantity) * i128::from(mult) * attr_side_sign(side);
        let mis = attr_round_cents(
            delta
                * Decimal::from(i64::try_from(ds).unwrap_or(0))
                * Decimal::from(i64::try_from(weight_squared).unwrap_or(i64::MAX)),
        );
        // For a non-zero term, once-weighted and squared-weighted differ.
        if correct != 0 {
            prop_assert_ne!(correct, mis, "an N² double-quantity would change the term");
        }
    }

    /// Sign reconciliation at the attribution boundary: `spread_capture` is the
    /// single negation of the slippage sum, `fees ≥ 0` and subtracted, and the
    /// identity closes exactly for any generated frictions and target.
    #[test]
    fn fill_and_step_sign_reconciliation(
        slippage_sum in -100_000i64..=100_000,
        fees in 0i64..=100_000,
        step_pnl in -1_000_000i64..=1_000_000,
    ) {
        // spread_capture = −Σ slippage (the single sign flip).
        let spread_capture = slippage_sum.checked_neg();
        let Some(spread_capture) = spread_capture else { return Ok(()); };
        // A step with no Greek drivers isolates the frictions.
        let substrate = attr_single_leg(
            None, Decimal::new(20, 2), 1, 100, Side::Short,
            TS0, TS0, 500_000, 500_000, // all market deltas 0
            spread_capture, fees, step_pnl,
        );
        let Ok(rows) = attribute(&substrate) else {
            return Err(TestCaseError::fail("attribution must succeed"));
        };
        let Some(row) = rows.first() else {
            return Err(TestCaseError::fail("one row"));
        };
        prop_assert_eq!(row.spread_capture_cents, spread_capture, "single sign flip");
        prop_assert!(row.fees_cents >= 0, "fees are a non-negative magnitude");
        prop_assert_eq!(row.fees_cents, fees);
        // Greek terms 0 (no S_-1) ⇒ residual = step_pnl − (spread_capture − fees).
        prop_assert_eq!(row.theta_pnl_cents, 0);
        prop_assert_eq!(row.delta_pnl_cents, 0);
        prop_assert_eq!(row.vega_pnl_cents, 0);
        prop_assert_eq!(attr_identity_lhs(row), i128::from(step_pnl), "identity closes exactly");
    }

    /// The residual bound is **advisory**: whatever the residual's magnitude,
    /// `attribute` returns `Ok`, the row is present, and the identity still
    /// closes exactly. A residual above the floor and above `|step_pnl|` is
    /// flagged `notable` (an advisory warn) — but never fails the run.
    #[test]
    fn attribution_residual_is_bounded(
        delta_milli in -5000i64..=5000,
        ds_cents in -1000i64..=1000,
        step_pnl in -10_000_000i64..=10_000_000,
    ) {
        let delta = Decimal::new(delta_milli, 3);
        let underlying = 500_000u64;
        let prior_underlying = i64::try_from(underlying)
            .ok()
            .and_then(|u| u.checked_sub(ds_cents))
            .and_then(|v| u64::try_from(v).ok());
        let Some(prior_underlying) = prior_underlying else { return Ok(()); };
        let substrate = attr_single_leg(
            Some(attr_greeks(delta, Decimal::ZERO, Decimal::ZERO, Decimal::new(20, 2))),
            Decimal::new(20, 2),
            1, 100, Side::Long,
            TS0, TS0, underlying, prior_underlying,
            0, 0, step_pnl,
        );
        // Never errors, regardless of how large the residual grows.
        let Ok(rows) = attribute(&substrate) else {
            return Err(TestCaseError::fail("a large residual must never fail the pass"));
        };
        let Some(row) = rows.first() else {
            return Err(TestCaseError::fail("one row"));
        };
        // The identity always closes exactly in integer cents.
        prop_assert_eq!(attr_identity_lhs(row), i128::from(step_pnl));
        // The advisory predicate agrees with its definition — and it is only a
        // warning trigger, asserted here to be consistent, never a gate.
        let notable = residual_is_notable(row.residual_cents, step_pnl);
        let r = row.residual_cents.unsigned_abs();
        let expected_notable =
            r > RESIDUAL_WARN_FLOOR_CENTS.unsigned_abs() && r > step_pnl.unsigned_abs();
        prop_assert_eq!(notable, expected_notable);
    }
}

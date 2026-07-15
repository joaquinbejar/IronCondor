//! Property tests for the canonical domain types.

use ironcondor::{
    BacktestError, Cents, ContractKey, ExecutionMode, Fill, PriceCents, Quantity, RawQuote,
    SimClock, SimTime, SnapshotMeta, StepIndex, Ticks, Underlying, raw_quotes_to_snapshot,
};
use optionstratlib::prelude::Positive;
use optionstratlib::{ExpirationDate, OptionStyle, Side};
use proptest::prelude::*;

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
}

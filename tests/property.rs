//! Property tests for the canonical domain types.

use ironcondor::{BacktestError, Cents, ContractKey, PriceCents, Quantity, Ticks, Underlying};
use optionstratlib::{ExpirationDate, OptionStyle};
use proptest::prelude::*;

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
}

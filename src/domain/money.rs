//! Integer-cents money newtypes and the two `Decimal` boundary helpers.
//!
//! Money crosses every boundary as integer cents — `f64` is banned for
//! money, premiums, fees, equity, and P&L. Derived analytic values (Greeks,
//! implied volatility, ratios) are the only documented exception. Arithmetic
//! is checked: a silent wrap in the P&L ledger is a correctness bug, so
//! overflow is [`BacktestError::ArithmeticOverflow`], never a wrap and never
//! a `saturating_*` fallback.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};

use crate::error::BacktestError;

/// Signed ledger money in integer cents: P&L, fees, equity, slippage.
///
/// Serialises as its bare inner scalar (`-1550`), never a wrapped object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cents(i64);

impl Cents {
    /// Wrap a signed integer-cents amount.
    #[must_use]
    pub const fn new(cents: i64) -> Self {
        Self(cents)
    }

    /// The inner amount in integer cents.
    #[must_use]
    pub const fn value(self) -> i64 {
        self.0
    }

    /// Checked addition.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] if the sum exceeds the
    /// `i64` range.
    #[must_use = "checked arithmetic does nothing unless the result is used"]
    pub fn checked_add(self, rhs: Self) -> Result<Self, BacktestError> {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(BacktestError::ArithmeticOverflow)
    }

    /// Checked subtraction.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] if the difference
    /// exceeds the `i64` range.
    #[must_use = "checked arithmetic does nothing unless the result is used"]
    pub fn checked_sub(self, rhs: Self) -> Result<Self, BacktestError> {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or(BacktestError::ArithmeticOverflow)
    }

    /// Checked multiplication by a contract quantity.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] if the product exceeds
    /// the `i64` range.
    #[must_use = "checked arithmetic does nothing unless the result is used"]
    pub fn checked_mul_qty(self, qty: Quantity) -> Result<Self, BacktestError> {
        self.0
            .checked_mul(i64::from(qty.value()))
            .map(Self)
            .ok_or(BacktestError::ArithmeticOverflow)
    }
}

/// Quoted prices, premiums, and strikes in integer cents — non-negative.
///
/// Zero is a valid premium, so construction rejects nothing. Serialises as
/// its bare inner scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PriceCents(u64);

impl PriceCents {
    /// Wrap a non-negative integer-cents price.
    #[must_use]
    pub const fn new(cents: u64) -> Self {
        Self(cents)
    }

    /// The inner price in integer cents.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Round a `Decimal` dollar amount to whole cents with banker's
    /// rounding (half-to-even) — the single fixed policy on the
    /// `Decimal → cents` edge, so the same input always produces the same
    /// cents.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Conversion`] for a negative amount or one
    /// whose cents exceed the `u64` range, and
    /// [`BacktestError::ArithmeticOverflow`] if the dollars-to-cents scaling
    /// itself overflows `Decimal`.
    #[must_use = "the converted price must be used"]
    pub fn from_decimal_dollars(d: Decimal) -> Result<Self, BacktestError> {
        if d.is_sign_negative() && !d.is_zero() {
            return Err(BacktestError::Conversion(format!(
                "negative dollar amount {d} cannot be a price"
            )));
        }
        let cents = d
            .checked_mul(Decimal::ONE_HUNDRED)
            .ok_or(BacktestError::ArithmeticOverflow)?
            .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven);
        let cents = cents.to_u64().ok_or_else(|| {
            BacktestError::Conversion(format!("dollar amount {d} out of range for u64 cents"))
        })?;
        Ok(Self(cents))
    }

    /// The exact `Decimal` dollar value — cents are representable as
    /// `Decimal` without loss, so this is the lossless inverse of
    /// [`Self::from_decimal_dollars`].
    #[must_use]
    pub fn to_decimal_dollars(self) -> Decimal {
        Decimal::from_i128_with_scale(i128::from(self.0), 2)
    }
}

/// A contract count — strictly positive.
///
/// Serialises as its bare inner scalar and **deserialises through the `> 0`
/// invariant**: `into`/`try_from` route every decode of a `Quantity` (bare, or
/// nested in a strategy-spec / order-intent JSON) through [`Quantity::new`], so
/// a wire `0` is a typed [`BacktestError::InvalidQuantity`], never a silently
/// admitted zero-size leg (the derived `transparent` impl bypassed the check).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(into = "u32", try_from = "u32")]
pub struct Quantity(u32);

impl Quantity {
    /// Wrap a strictly positive contract count.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::InvalidQuantity`] for `0`.
    #[must_use = "the validated quantity must be used"]
    pub fn new(quantity: u32) -> Result<Self, BacktestError> {
        if quantity == 0 {
            return Err(BacktestError::InvalidQuantity(quantity));
        }
        Ok(Self(quantity))
    }

    /// The inner contract count (always `> 0`).
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

/// Validated deserialisation seam: every decoded `u32` is routed through the
/// `> 0` invariant, so a wire `0` fails loudly instead of admitting a zero-size
/// leg.
impl TryFrom<u32> for Quantity {
    type Error = BacktestError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// The transparent wire form: a `Quantity` serialises as its bare inner scalar.
impl From<Quantity> for u32 {
    fn from(quantity: Quantity) -> Self {
        quantity.0
    }
}

/// An orderbook price in venue ticks (realistic mode).
///
/// Serialises as its bare inner scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ticks(u128);

impl Ticks {
    /// Wrap a venue-tick price.
    #[must_use]
    pub const fn new(ticks: u128) -> Self {
        Self(ticks)
    }

    /// The inner price in venue ticks.
    #[must_use]
    pub const fn value(self) -> u128 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{Cents, PriceCents, Quantity, Ticks};
    use crate::error::BacktestError;

    #[test]
    fn test_quantity_new_zero_invalid() {
        assert!(matches!(
            Quantity::new(0),
            Err(BacktestError::InvalidQuantity(0))
        ));
    }

    #[test]
    fn test_quantity_new_nonzero_round_trips() {
        let qty = Quantity::new(4);
        assert!(matches!(qty, Ok(q) if q.value() == 4));
    }

    #[test]
    fn test_quantity_deserialize_rejects_zero() {
        // The `try_from = "u32"` seam routes a bare wire scalar through the
        // `> 0` invariant, so a strategy-spec / order-intent JSON carrying
        // `quantity: 0` fails to deserialise rather than admitting a zero leg.
        let zero: Result<Quantity, _> = serde_json::from_str("0");
        assert!(zero.is_err(), "quantity 0 must be rejected on deserialize");
        let ok: Result<Quantity, _> = serde_json::from_str("3");
        assert!(matches!(ok, Ok(q) if q.value() == 3));
    }

    #[test]
    fn test_cents_checked_add_overflow_at_i64_boundary() {
        let max = Cents::new(i64::MAX);
        assert!(matches!(
            max.checked_add(Cents::new(1)),
            Err(BacktestError::ArithmeticOverflow)
        ));
        assert!(matches!(
            Cents::new(1).checked_add(Cents::new(2)),
            Ok(c) if c.value() == 3
        ));
    }

    #[test]
    fn test_cents_checked_sub_overflow_at_i64_boundary() {
        let min = Cents::new(i64::MIN);
        assert!(matches!(
            min.checked_sub(Cents::new(1)),
            Err(BacktestError::ArithmeticOverflow)
        ));
        assert!(matches!(
            Cents::new(1).checked_sub(Cents::new(2)),
            Ok(c) if c.value() == -1
        ));
    }

    #[test]
    fn test_cents_checked_mul_qty_overflow() {
        let big = Cents::new(i64::MAX / 2);
        let Ok(qty) = Quantity::new(3) else {
            panic!("quantity 3 must be valid");
        };
        assert!(matches!(
            big.checked_mul_qty(qty),
            Err(BacktestError::ArithmeticOverflow)
        ));
    }

    #[test]
    fn test_from_decimal_dollars_rounds_half_to_even() {
        // 0.005 dollars = 0.5 cents → rounds to 0 (even).
        assert!(matches!(
            PriceCents::from_decimal_dollars(dec!(0.005)),
            Ok(p) if p.value() == 0
        ));
        // 0.015 dollars = 1.5 cents → rounds to 2 (even).
        assert!(matches!(
            PriceCents::from_decimal_dollars(dec!(0.015)),
            Ok(p) if p.value() == 2
        ));
        // 0.025 dollars = 2.5 cents → rounds to 2 (even).
        assert!(matches!(
            PriceCents::from_decimal_dollars(dec!(0.025)),
            Ok(p) if p.value() == 2
        ));
        // Plain case away from the midpoint.
        assert!(matches!(
            PriceCents::from_decimal_dollars(dec!(51.00)),
            Ok(p) if p.value() == 5100
        ));
    }

    #[test]
    fn test_from_decimal_dollars_rejects_negative_conversion_error() {
        assert!(matches!(
            PriceCents::from_decimal_dollars(dec!(-0.01)),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_to_decimal_dollars_lossless_inverse() {
        let price = PriceCents::new(5100);
        assert_eq!(price.to_decimal_dollars(), dec!(51.00));
        assert!(matches!(
            PriceCents::from_decimal_dollars(price.to_decimal_dollars()),
            Ok(p) if p == price
        ));
    }

    #[test]
    fn test_money_newtypes_serialize_as_bare_scalars() {
        assert!(matches!(
            serde_json::to_string(&Cents::new(-1550)).as_deref(),
            Ok("-1550")
        ));
        assert!(matches!(
            serde_json::to_string(&PriceCents::new(510_000)).as_deref(),
            Ok("510000")
        ));
        let qty = Quantity::new(2);
        assert!(matches!(qty, Ok(q) if serde_json::to_string(&q).unwrap_or_default() == "2"));
        assert!(matches!(
            serde_json::to_string(&Ticks::new(42)).as_deref(),
            Ok("42")
        ));
    }
}

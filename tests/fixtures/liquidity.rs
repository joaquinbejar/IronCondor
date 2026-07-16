//! Book-seeding fixture (docs/TESTING.md §5) — a canonical per-strike liquidity
//! profile plus a one-contract chain snapshot, generated as **Rust source**
//! (not a committed binary), following the repo convention. Consumed by the
//! realistic-mode ladder-walk integration tests (#023).
//!
//! The canonical profile is quoted-size touch, `L = 3`, `r = 0.5`. Against an
//! `ask_size = 8` quote it seeds the ask ladder `8 @ 500`, `4 @ 505`, `2 @ 510`,
//! `1 @ 515` — the depth a marketable buy walks level by level.

#![allow(dead_code)]

use std::collections::BTreeMap;

use ironcondor::{
    ChainSnapshot, ContractKey, InstrumentSpec, LiquidityProfile, PriceCents, Quantity, QuoteView,
    SimTime, StepIndex, TouchSize, Underlying,
};
use optionstratlib::{ExpirationDate, OptionStyle};

/// The tape anchor `ts_0` (ns since epoch, UTC), reused as the fixture expiry.
pub const TS0: i64 = 1_750_291_200_000_000_000;
/// The fixture tick size in cents.
pub const TICK: u64 = 5;

/// The canonical book-seeding profile: quoted-size touch, `L = 3`, `r = 0.5`.
#[must_use]
pub fn canonical_profile() -> LiquidityProfile {
    LiquidityProfile {
        touch_size: TouchSize::QuotedSize,
        depth_levels: 3,
        // `0.5` as mantissa 5 at scale 1 — no `f64`.
        decay: rust_decimal::Decimal::new(5, 1),
    }
}

/// The single SPX call the fixture snapshot quotes.
#[must_use]
pub fn contract() -> ContractKey {
    let underlying = match Underlying::new("SPX") {
        Ok(u) => u,
        Err(e) => panic!("SPX is a valid underlying: {e}"),
    };
    ContractKey {
        underlying,
        expiration: ExpirationDate::DateTime(chrono::DateTime::from_timestamp_nanos(TS0)),
        strike: PriceCents::new(510_000),
        style: OptionStyle::Call,
    }
}

/// A one-contract snapshot with the given touch and per-side sizes on a 5-cent
/// tick, 100x SPX chain — the shape the seeder builds ladders from.
#[must_use]
pub fn snapshot(bid: u64, ask: u64, bid_size: u32, ask_size: u32) -> ChainSnapshot {
    let (underlying, spec, bid_qty, ask_qty) = match (
        Underlying::new("SPX"),
        InstrumentSpec::new(PriceCents::new(TICK), 100),
        Quantity::new(bid_size),
        Quantity::new(ask_size),
    ) {
        (Ok(u), Ok(s), Ok(b), Ok(a)) => (u, s, b, a),
        _ => panic!("fixture underlying/spec/sizes must be valid"),
    };
    let quote = QuoteView {
        contract: contract(),
        bid: PriceCents::new(bid),
        ask: PriceCents::new(ask),
        mid: PriceCents::new((bid + ask) / 2),
        bid_size: bid_qty,
        ask_size: ask_qty,
        implied_volatility: rust_decimal::Decimal::ZERO,
        delta: rust_decimal::Decimal::ZERO,
        gamma: rust_decimal::Decimal::ZERO,
        theta: rust_decimal::Decimal::ZERO,
        vega: rust_decimal::Decimal::ZERO,
    };
    let mut quotes = BTreeMap::new();
    quotes.insert(contract(), quote);
    ChainSnapshot {
        ts: SimTime::new(TS0),
        step: StepIndex::new(0),
        underlying,
        underlying_price: PriceCents::new(510_000),
        spec,
        quotes,
    }
}

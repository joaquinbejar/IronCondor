//! Fill simulation: the [`ExecutionModel`] seam and the shared [`Fill`]
//! assembly both fill models emit through.
//!
//! Two fill models live under `src/execution/` — naive (mid/spread +
//! configured slippage, issue #13) and realistic (`option-chain-orderbook`
//! matching, feature `orderbook`, v0.2). This module owns the surface they
//! share so analytics cannot tell which mode produced a fill:
//!
//! - [`ExecutionModel`] — the command→fill seam the engine drives
//!   ([docs/04 §2](../../../docs/04-execution-models.md), [docs/02 §5](../../../docs/02-engine-architecture.md)).
//! - [`assemble_fill`] — the **single** place a [`Fill`] is stamped with its
//!   [`ExecutionMode`], its signed `slippage` (via
//!   [`crate::domain::execution::sign_convention::slippage_cents`], never
//!   reinvented), and its `fees`. Both modes call it, so the two are
//!   byte-shape identical and only their values differ.
//! - [`fee_for_fill`] — the shared fee rule
//!   ([docs/04 §10](../../../docs/04-execution-models.md)): `per_contract_cents ×
//!   quantity` on every fill, plus `per_order_cents` once on the order's first
//!   fill, integer cents, always `≥ 0`.
//!
//! # Realistic-mode-only mapping (v0.2, `src/execution/realistic.rs`)
//!
//! The `optionstratlib::Side` (Long/Short) → `option_chain_orderbook::Side`
//! (Buy/Sell) mapping and the `PriceCents` (u64 cents) → `u128` tick scaling
//! (via the snapshot's `InstrumentSpec.tick_size_cents`, the single tick
//! source) are **realistic-mode only** and land in
//! `src/execution/realistic.rs` when the `orderbook` feature ships (v0.2). The
//! naive path never touches the orderbook, and **no raw `f64` crosses into
//! `option-chain-orderbook`** — the whole seam is integer cents and `u128`
//! ticks. Nothing of that mapping lives here.

pub mod naive;

pub use naive::NaiveFill;

use crate::config::FeeSchedule;
use crate::domain::execution::sign_convention;
use crate::domain::{
    Cents, ChainSnapshot, ContractKey, ExecutionMode, Fill, OrderCommand, PriceCents, Quantity,
    SimTime, StepIndex,
};
use crate::error::BacktestError;
use optionstratlib::Side;

/// The command→fill seam. The engine selects one implementation from config
/// and drives it once per step; a strategy never sees which mode is active
/// ([docs/04 §2](../../../docs/04-execution-models.md)).
///
/// Both modes emit the identical [`Fill`] shape (assembled by
/// [`assemble_fill`]) so analytics is mode-agnostic — the invariant v0.2
/// cross-mode parity leans on.
pub trait ExecutionModel {
    /// Process the step's `commands` against `snap`, **appending** any
    /// resulting fills to the caller-owned `out_fills`.
    ///
    /// Contract (all deterministic — same `(seed, config, data)` ⇒
    /// byte-identical fills):
    ///
    /// - **Append, never allocate a return.** `fill` appends into
    ///   `out_fills`; it never returns an owned `Vec`. The engine clears
    ///   `out_fills` in place before the call so its capacity is reused — no
    ///   per-step `Vec<Fill>` allocation (PB-1,
    ///   [docs/07 §4](../../../docs/07-performance-and-security.md)).
    /// - **Ordered command processing.** `Cancel` and `Replace` are applied
    ///   **before** `Submit`, in a fixed order, so cancels free queue space
    ///   before new submits contend for it.
    /// - **Realistic refresh precedes command fills (v0.2).** In realistic
    ///   mode `fill` first refreshes the seeded book against `snap` and
    ///   appends any refresh-generated fills (a resting limit the market
    ///   moved to), then appends the command fills
    ///   ([docs/04 §6.1](../../../docs/04-execution-models.md)). Naive mode has
    ///   no book, so this step is a no-op. Only the contract is fixed here;
    ///   the realistic implementation lands in `src/execution/realistic.rs`.
    /// - **Synchronous.** No `.await` on this path — the replay loop drives it
    ///   directly ([rules/global_rules.md](../../../rules/global_rules.md)
    ///   "Concurrency").
    ///
    /// Naive mode always fills the full intent; realistic mode may fill
    /// partially or not at all.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError`] when a command is invalid (e.g. an oversized
    /// close) or when fill assembly overflows integer-cents arithmetic.
    fn fill(
        &mut self,
        commands: &[OrderCommand],
        snap: &ChainSnapshot,
        out_fills: &mut Vec<Fill>,
    ) -> Result<(), BacktestError>;

    /// Which fill model this is — `Naive` or `Realistic`.
    #[must_use]
    fn mode(&self) -> ExecutionMode;
}

/// Which fee components a given fill carries.
///
/// An order can produce several fills (levels walked,
/// [docs/04 §5.2](../../../docs/04-execution-models.md)), yet the once-per-order
/// fee must be charged exactly once. The domain [`Fill`] has **no** `fill_seq`
/// field — that ordinal lives on the bundle `FillRow` (v0.3), not here — so the
/// caller (the fill model, which knows the order's fill order) tells the
/// assembler whether this is the order's first fill through this enum. That is
/// how the per-order-first-fill split is expressed without a `fill_seq` on the
/// domain type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(crate) enum FeeCharge {
    /// The order's first fill: charge `per_contract_cents × quantity` **plus**
    /// the once-per-order `per_order_cents`.
    FirstFill = 0,
    /// A later fill of the same order: charge `per_contract_cents × quantity`
    /// only — the per-order fee already sat on the first fill. Naive mode is
    /// single-shot and never emits it; the first constructor is realistic
    /// mode's multi-level walk (v0.2).
    #[allow(
        dead_code,
        reason = "constructed by realistic mode's multi-level walk (v0.2); NaiveFill is single-shot"
    )]
    LaterFill = 1,
}

/// The raw execution facts a fill model produces, before the shared
/// [`assemble_fill`] stamps `mode`, computes signed `slippage`, and attaches
/// `fees`. Grouping them keeps the assembler to a small, testable signature and
/// guarantees both modes route through exactly the same construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FillDraft {
    /// The snapshot timestamp the fill executed against.
    pub ts: SimTime,
    /// The step the fill executed in.
    pub step: StepIndex,
    /// The filled contract identity.
    pub contract: ContractKey,
    /// Long or short — reused from `optionstratlib`.
    pub side: Side,
    /// Filled quantity (`> 0`); may be `<` the intent in realistic mode.
    pub quantity: Quantity,
    /// Executed premium per contract, integer cents.
    pub price: PriceCents,
    /// The mid at decision time — the slippage reference.
    pub decision_mid: PriceCents,
}

/// The fee for one fill in integer cents:
/// `per_contract_cents × quantity`, **plus** `per_order_cents` when `charge`
/// is [`FeeCharge::FirstFill`] ([docs/04 §10](../../../docs/04-execution-models.md)).
///
/// The result is always `≥ 0` (both schedule components and the quantity are
/// unsigned) and is subtracted in every P&L identity. Both products and the
/// sum are checked, so an overflow is a typed error rather than a wrap.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] when `per_contract_cents ×
/// quantity`, the added `per_order_cents`, or the narrowing to signed
/// [`Cents`] exceeds range.
#[must_use = "the computed fee must be recorded on the fill"]
pub(crate) fn fee_for_fill(
    schedule: &FeeSchedule,
    quantity: Quantity,
    charge: FeeCharge,
) -> Result<Cents, BacktestError> {
    let per_contract_total = schedule
        .per_contract_cents
        .checked_mul(u64::from(quantity.value()))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let total = match charge {
        FeeCharge::FirstFill => per_contract_total
            .checked_add(schedule.per_order_cents)
            .ok_or(BacktestError::ArithmeticOverflow)?,
        FeeCharge::LaterFill => per_contract_total,
    };
    let cents = i64::try_from(total).map_err(|_| BacktestError::ArithmeticOverflow)?;
    Ok(Cents::new(cents))
}

/// Assemble the shared [`Fill`] from a fill model's [`FillDraft`] — the single
/// place `mode` is stamped, `slippage` is signed against `decision_mid`, and
/// `fees` are attached, so a naive fill and a realistic fill of the same intent
/// are byte-shape identical and directly comparable
/// ([docs/04 §2](../../../docs/04-execution-models.md)).
///
/// `slippage` is delegated to
/// [`crate::domain::execution::sign_convention::slippage_cents`] (positive =
/// adverse); the sign math is never reinvented here. `fees` are delegated to
/// [`fee_for_fill`], with `charge` expressing whether this is the order's first
/// fill.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] when the slippage or fee
/// computation exceeds integer-cents range.
#[must_use = "the assembled fill must be recorded"]
pub(crate) fn assemble_fill(
    draft: FillDraft,
    mode: ExecutionMode,
    fee_schedule: &FeeSchedule,
    charge: FeeCharge,
) -> Result<Fill, BacktestError> {
    let slippage = sign_convention::slippage_cents(
        draft.price,
        draft.decision_mid,
        draft.quantity,
        draft.side,
    )?;
    let fees = fee_for_fill(fee_schedule, draft.quantity, charge)?;
    Ok(Fill {
        ts: draft.ts,
        step: draft.step,
        contract: draft.contract,
        side: draft.side,
        quantity: draft.quantity,
        price: draft.price,
        fees,
        slippage,
        mode,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};

    use super::{ExecutionModel, FeeCharge, FillDraft, assemble_fill, fee_for_fill};
    use crate::config::FeeSchedule;
    use crate::domain::{
        ChainSnapshot, ContractKey, ExecutionMode, Fill, InstrumentSpec, OrderCommand, OrderIntent,
        PositionAction, PriceCents, Quantity, SimTime, StepIndex, TimeInForce, Underlying,
    };
    use crate::error::BacktestError;

    const TS0: i64 = 1_750_291_200_000_000_000;

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is a valid quantity");
        };
        q
    }

    fn contract() -> ContractKey {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        ContractKey {
            underlying,
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(TS0)),
            strike: PriceCents::new(510_000),
            style: OptionStyle::Call,
        }
    }

    fn draft(side: Side, price: u64, decision_mid: u64, quantity: u32) -> FillDraft {
        FillDraft {
            ts: SimTime::new(TS0),
            step: StepIndex::new(0),
            contract: contract(),
            side,
            quantity: qty(quantity),
            price: PriceCents::new(price),
            decision_mid: PriceCents::new(decision_mid),
        }
    }

    fn fees() -> FeeSchedule {
        FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        }
    }

    fn empty_snapshot() -> ChainSnapshot {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(5), 100) else {
            panic!("5-cent tick and 100x multiplier are valid");
        };
        ChainSnapshot {
            ts: SimTime::new(TS0),
            step: StepIndex::new(0),
            underlying,
            underlying_price: PriceCents::new(510_000),
            spec,
            quotes: BTreeMap::new(),
        }
    }

    /// A trivial in-test execution model: it fills every `Submit` at its limit
    /// (or `decision_mid` when marketable) as a single-shot first fill and
    /// appends into the caller-owned buffer, ignoring `Cancel`/`Replace`. It
    /// exercises [`ExecutionModel::mode`] and the shared [`assemble_fill`];
    /// naive/realistic parity is out of scope for #12.
    struct EchoModel {
        mode: ExecutionMode,
        fees: FeeSchedule,
    }

    impl ExecutionModel for EchoModel {
        fn fill(
            &mut self,
            commands: &[OrderCommand],
            snap: &ChainSnapshot,
            out_fills: &mut Vec<Fill>,
        ) -> Result<(), BacktestError> {
            for command in commands {
                if let OrderCommand::Submit(intent) = command {
                    let price = intent.limit.unwrap_or(intent.decision_mid);
                    let d = FillDraft {
                        ts: snap.ts,
                        step: snap.step,
                        contract: intent.contract.clone(),
                        side: intent.side,
                        quantity: intent.quantity,
                        price,
                        decision_mid: intent.decision_mid,
                    };
                    out_fills.push(assemble_fill(
                        d,
                        self.mode,
                        &self.fees,
                        FeeCharge::FirstFill,
                    )?);
                }
            }
            Ok(())
        }

        fn mode(&self) -> ExecutionMode {
            self.mode
        }
    }

    fn submit(price: u64, decision_mid: u64) -> OrderCommand {
        OrderCommand::Submit(OrderIntent {
            contract: contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: qty(1),
            limit: Some(PriceCents::new(price)),
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(decision_mid),
        })
    }

    #[test]
    fn test_fill_report_shape_mode_agnostic() {
        // Same draft, same fees, same charge — only the mode differs; every
        // other field must be identical, proving the shape is mode-agnostic.
        let d = draft(Side::Long, 152, 150, 3);
        let naive = assemble_fill(
            d.clone(),
            ExecutionMode::Naive,
            &fees(),
            FeeCharge::FirstFill,
        );
        let realistic = assemble_fill(d, ExecutionMode::Realistic, &fees(), FeeCharge::FirstFill);
        let (Ok(naive), Ok(realistic)) = (naive, realistic) else {
            panic!("both assemblies must succeed");
        };
        assert_eq!(naive.mode, ExecutionMode::Naive);
        assert_eq!(realistic.mode, ExecutionMode::Realistic);
        assert_ne!(naive.mode, realistic.mode);
        // Re-stamping the mode makes them fully equal ⇒ all other fields match.
        let mut rebadged = naive;
        rebadged.mode = ExecutionMode::Realistic;
        assert_eq!(rebadged, realistic);
    }

    #[test]
    fn test_fees_per_order_charged_once_on_first_fill() {
        let schedule = fees(); // per_contract 65, per_order 100
        let first = fee_for_fill(&schedule, qty(3), FeeCharge::FirstFill);
        let later = fee_for_fill(&schedule, qty(3), FeeCharge::LaterFill);
        let (Ok(first), Ok(later)) = (first, later) else {
            panic!("fee computation must succeed");
        };
        // per_contract on every fill: 65 × 3 = 195.
        assert_eq!(later.value(), 195);
        // first fill additionally carries per_order once: 195 + 100 = 295.
        assert_eq!(first.value(), 295);
        // the once-per-order delta is exactly per_order_cents.
        assert_eq!(first.value() - later.value(), 100);
        // fees are always a non-negative magnitude.
        assert!(first.value() >= 0 && later.value() >= 0);
    }

    #[test]
    fn test_assemble_fill_slippage_positive_when_buy_adverse() {
        // Buying at 152 with mid 150 is adverse for a long: +2 per contract.
        let fill = assemble_fill(
            draft(Side::Long, 152, 150, 1),
            ExecutionMode::Naive,
            &fees(),
            FeeCharge::FirstFill,
        );
        assert!(matches!(fill, Ok(f) if f.slippage.value() == 2));
        // Buying below mid is favourable: negative.
        let fill = assemble_fill(
            draft(Side::Long, 149, 150, 1),
            ExecutionMode::Naive,
            &fees(),
            FeeCharge::FirstFill,
        );
        assert!(matches!(fill, Ok(f) if f.slippage.value() == -1));
    }

    #[test]
    fn test_assemble_fill_slippage_positive_when_sell_adverse() {
        // Selling at 148 with mid 150 is adverse for a short: +2 per contract.
        let fill = assemble_fill(
            draft(Side::Short, 148, 150, 1),
            ExecutionMode::Realistic,
            &fees(),
            FeeCharge::FirstFill,
        );
        assert!(matches!(fill, Ok(f) if f.slippage.value() == 2));
        // Selling above mid is favourable: negative.
        let fill = assemble_fill(
            draft(Side::Short, 153, 150, 1),
            ExecutionMode::Realistic,
            &fees(),
            FeeCharge::FirstFill,
        );
        assert!(matches!(fill, Ok(f) if f.slippage.value() == -3));
    }

    #[test]
    fn test_execution_model_mode_returns_correct_discriminant() {
        let naive = EchoModel {
            mode: ExecutionMode::Naive,
            fees: fees(),
        };
        let realistic = EchoModel {
            mode: ExecutionMode::Realistic,
            fees: fees(),
        };
        assert_eq!(naive.mode(), ExecutionMode::Naive);
        assert_eq!(realistic.mode(), ExecutionMode::Realistic);
    }

    #[test]
    fn test_execution_model_fill_appends_into_caller_buffer() {
        let mut model = EchoModel {
            mode: ExecutionMode::Naive,
            fees: fees(),
        };
        let snap = empty_snapshot();
        // A pre-existing entry proves `fill` appends and never clears.
        let mut out = Vec::new();
        let seed = assemble_fill(
            draft(Side::Long, 150, 150, 1),
            ExecutionMode::Naive,
            &fees(),
            FeeCharge::FirstFill,
        );
        let Ok(seed) = seed else {
            panic!("seed fill must assemble");
        };
        out.push(seed);
        let commands = [submit(152, 150), submit(151, 150)];
        let result = model.fill(&commands, &snap, &mut out);
        assert!(matches!(result, Ok(())));
        // one pre-existing + two appended, in submission order.
        assert_eq!(out.len(), 3);
        assert!(
            matches!(out.get(1), Some(f) if f.price.value() == 152 && f.mode == ExecutionMode::Naive)
        );
        assert!(matches!(out.get(2), Some(f) if f.price.value() == 151));
    }
}

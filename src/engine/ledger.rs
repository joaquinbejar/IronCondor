//! The mark-to-market ledger — minimal for v0.1, enriched by issues #15 and #30.
//!
//! [`Ledger`] holds the run's cash and equity peak and produces one
//! [`EquityPoint`] per step ([docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger)).
//! The replay loop ([`crate::engine::backtest`]) calls it in the single
//! ledger-mutation phase of each step: it applies the step's fills to cash
//! ([`Ledger::apply_fill`]), then re-marks the open legs at the snapshot's mid
//! and emits the step's one equity point ([`Ledger::settle`]).
//!
//! # The engine→analytics hand-off (issue #30)
//!
//! `EquityPoint` is the on-disk **wire** row (frozen for #033); the P&L
//! attribution (#031) also needs the per-step *valuation substrate* that never
//! reaches that row. The ledger surfaces it as an **in-memory hand-off**,
//! valid until the next [`Ledger::settle`], so attribution reads it directly
//! rather than recomputing or re-scanning the tape
//! ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)):
//!
//! - **Per-leg valuation** ([`Ledger::position_marks`], enriched
//!   [`PositionMark`]) — each open leg's `position_id`, `contract`, `side`,
//!   `quantity`, `contract_multiplier`, resolved `mark`, `stale_mark`, and the
//!   per-contract **unit** Greeks as of the **previous** snapshot `S_{n-1}`
//!   ([`PositionMark::prior_greeks`]). Quantity and multiplier are surfaced
//!   **separately** from the unit Greeks, so attribution weights exactly once
//!   and never squares the size (`N²`,
//!   [specs/optionstratlib §4.1](../../../docs/specs/optionstratlib.md#41-exact-units-and-position-weighting-pinned-contract)).
//! - **Step fill signals** — [`Ledger::spread_capture`] (`−Σ slippage`) and
//!   [`Ledger::fees`] (`Σ fees`, always `≥ 0`), accumulated across the step's
//!   [`Ledger::apply_fill`] calls and reset each [`Ledger::settle`].
//! - **[`Ledger::step_pnl`]** — `equity_n − equity_{n-1}` (step 0 uses the
//!   `initial_capital` baseline), computed once here; attribution never
//!   recomputes it.
//!
//! # Scope (the #14 minimal ledger, enriched by #15)
//!
//! This implementation computes `cash`, `position_value`, `equity`, and
//! `drawdown` correctly — cash moves **only** through fills and fees, the
//! valuation identity `equity = cash + position_value` holds each step, and
//! `drawdown` is never clamped. Issue #15 layered the **rigorous** rules on
//! top of the #14 baseline:
//!
//! - **Two invariants, kept distinct** — `cash` is touched *only* by
//!   [`Ledger::apply_fill`]; equity is re-marked *every step* by
//!   [`Ledger::settle`], even with no fill. Conflating them was the retired,
//!   false `equity_changes_only_by_fills_and_fees` property
//!   ([docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger)).
//! - **`stale_mark` exposed** — a held leg absent from a step's `quotes` keeps
//!   its last-known mid *and* is flagged stale for that step; the per-leg flags
//!   are surfaced through [`Ledger::position_marks`] (the v0.3 `PositionRow`
//!   consumes them, but the flag state is correct now).
//! - **Settlement-mark rejection** — carrying a mark forward is legal only
//!   *before* a contract's own expiry. A held leg absent **at or after its own
//!   expiry step** is [`BacktestError::DataOutOfOrder`] (a settlement mark is
//!   mandatory there); a merely sparse chain before expiry is tolerated.
//!
//! # Determinism
//!
//! The ledger reads no wall clock and draws no randomness. It keys its
//! last-known marks and its retained `S_{n-1}` unit Greeks in [`BTreeMap`]s
//! (ordered, never a `HashMap`), records the per-step per-leg valuation in a
//! scratch [`Vec`] allocated once and cleared in place, and iterates the open
//! legs in their deterministic inventory order. Both maps grow only when a new
//! held contract is first seen (warmup) and are updated **in place** thereafter
//! — never `clear()`ed and refilled — so a warm step body allocates nothing
//! (PB-1). Every money operation is checked integer-cents arithmetic — a silent
//! wrap in the P&L ledger would be a correctness bug
//! ([docs/02 §7](../../../docs/02-engine-architecture.md#7-determinism-and-reproducibility)).

use std::collections::BTreeMap;

use optionstratlib::Side;
use rust_decimal::Decimal;

use crate::domain::execution::sign_convention;
use crate::domain::{
    Cents, ChainSnapshot, ContractKey, EquityPoint, Fill, OpenPosition, PositionId, PriceCents,
    Quantity, SimTime, StepIndex,
};
use crate::error::BacktestError;

/// Per-contract **unit** (quantity = 1) Greeks in native `optionstratlib`
/// bases, as carried by a [`crate::domain::QuoteView`]
/// ([docs/01 §6](../../../docs/01-domain-model.md#6-market-data)): delta per $1
/// underlying, theta per **day**, vega per one **percentage point** of IV.
///
/// The ledger retains one of these per held contract as the **beginning-of-step
/// observation endpoint** `S_{n-1}` and surfaces it on each open leg's
/// [`PositionMark::prior_greeks`], so the P&L attribution (#031) reads the prior
/// snapshot's sensitivities without a look-back into the tape
/// ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
/// The values are **unit** sensitivities — attribution weights them by
/// `quantity × contract_multiplier` (surfaced separately on the
/// [`PositionMark`]) exactly once. `implied_volatility` is retained alongside
/// the Greeks because the vega term's `ΔIV_pp = (iv_n − iv_{n-1}) × 100` needs
/// the prior snapshot's IV. Analytic `Decimal`s — the documented exception to
/// the integer-cents rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitGreeks {
    /// Per-contract unit delta, per $1 of underlying.
    pub delta: Decimal,
    /// Per-contract unit gamma.
    pub gamma: Decimal,
    /// Per-contract unit theta, per day.
    pub theta: Decimal,
    /// Per-contract unit vega, per percentage point of IV.
    pub vega: Decimal,
    /// Implied volatility (decimal, `0.18` = 18%) — the `ΔIV` endpoint.
    pub implied_volatility: Decimal,
}

/// One open leg's per-step valuation substrate for the step the ledger just
/// settled — the enriched engine→analytics hand-off (#30).
///
/// The ledger rebuilds a `PositionMark` per open leg each [`Ledger::settle`]
/// and exposes the slice through [`Ledger::position_marks`], valid until the
/// next settle. It carries everything the P&L attribution (#031) needs for the
/// leg without recomputation: the `mark × quantity × contract_multiplier ×
/// side_sign` inputs to `position_value` (surfaced separately, never
/// pre-weighted), the `stale_mark` carry-forward flag, and the per-contract
/// unit Greeks as of the **previous** snapshot `S_{n-1}` ([`Self::prior_greeks`]).
/// The v0.3 analytics layer turns these into `positions.parquet` rows
/// ([docs/01 §9](../../../docs/01-domain-model.md#9-result-bundle-record-types));
/// the values are authoritative now, the wire row lands later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionMark {
    /// The engine-minted leg identity this mark belongs to.
    pub position_id: PositionId,
    /// The leg's contract identity (the join key into a snapshot quote and,
    /// later, `positions.parquet`).
    pub contract: ContractKey,
    /// The direction the leg is open in (`Long` `+`, `Short` `−`) — the
    /// `side_sign` factor of the leg's `position_value` contribution.
    pub side: Side,
    /// The open contract count (`> 0`) — one of the two weighting factors,
    /// surfaced separately from the unit Greeks so attribution weights once.
    pub quantity: Quantity,
    /// Contracts → underlying units for this snapshot — the other weighting
    /// factor, surfaced separately from the unit Greeks.
    pub contract_multiplier: u32,
    /// The mark (integer cents) the leg was valued at this step — the snapshot
    /// mid, or the last-known mid carried forward when `stale_mark`.
    pub mark: PriceCents,
    /// `true` iff the contract had no quote this step and its last-known mark
    /// (or entry premium) was carried forward
    /// ([docs/01 §6](../../../docs/01-domain-model.md#6-market-data)).
    pub stale_mark: bool,
    /// The per-contract unit Greeks as of the **previous** snapshot `S_{n-1}`
    /// — the fixed observation endpoint for step `n`'s Greek attribution
    /// ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
    /// `None` at step 0 (no `S_{-1}`) or before the leg's contract has ever been
    /// quoted; a leg absent this step carries its last-known Greeks forward,
    /// mirroring the `stale_mark` carry-forward of `mark`.
    pub prior_greeks: Option<UnitGreeks>,
}

/// The run's cash-and-mark ledger.
///
/// Construct with [`Ledger::new`] at startup (cash = the run's initial
/// capital); the loop then drives [`Ledger::apply_fill`] for each fill and one
/// [`Ledger::settle`] per step.
#[derive(Debug, Clone)]
pub struct Ledger {
    /// Account cash in integer cents — moves only through fills and fees.
    cash: Cents,
    /// The running equity peak in integer cents, `max(peak, equity)` each step;
    /// initialised to the run's initial capital (`> 0`), so it is never zero.
    running_peak: i64,
    /// Last-known mid per held contract, carried forward for a contract absent
    /// from a later snapshot (`stale_mark`,
    /// [docs/01 §6](../../../docs/01-domain-model.md#6-market-data)). Ordered by
    /// [`ContractKey`] for determinism; grows only when a new contract is first
    /// marked (warmup), so a warm step body does not allocate here.
    marks: BTreeMap<ContractKey, PriceCents>,
    /// Per-leg valuation for the step just settled, in open-inventory order —
    /// the enriched engine→analytics hand-off ([`Ledger::position_marks`]).
    /// Reused scratch: cleared in place at the start of every
    /// [`Ledger::settle`] and refilled, so it grows only during warmup and never
    /// allocates on a warm step (PB-1). Pushing a leg clones its
    /// [`ContractKey`], but the ticker is `Arc<str>` so that is a refcount bump,
    /// not a heap allocation.
    position_marks: Vec<PositionMark>,
    /// Last-known per-contract **unit** Greeks — the retained `S_{n-1}`
    /// observation endpoint the enrichment (#30) surfaces on each leg's
    /// [`PositionMark::prior_greeks`]. Updated **in place** at the *end* of
    /// every [`Ledger::settle`] (so a step reads the prior endpoint before it is
    /// overwritten with the current one) and, like [`Self::marks`], never
    /// `clear()`ed — it grows only when a new held contract is first quoted, so
    /// a warm step body does not allocate here.
    greeks: BTreeMap<ContractKey, UnitGreeks>,
    /// The previous step's equity in integer cents — `equity_{n-1}`. Seeded to
    /// the run's `initial_capital` so step 0's `step_pnl` uses the step-`−1`
    /// baseline `equity_{-1} = initial_capital`
    /// ([docs/02 §6](../../../docs/02-engine-architecture.md#6-mark-to-market-ledger)).
    /// Advanced to `equity_n` at the end of every [`Ledger::settle`].
    prev_equity: i64,
    /// `Σ slippage` over the current (not-yet-settled) step's fills, in `i128`
    /// cents — accumulated in [`Ledger::apply_fill`], captured and zeroed each
    /// [`Ledger::settle`]. `i128` gives headroom over any one step's fills; the
    /// exposed value is range-checked back into `i64` cents.
    step_slippage: i128,
    /// `Σ fees` (always `≥ 0`) over the current step's fills, in `i128` cents —
    /// accumulated in [`Ledger::apply_fill`], captured and zeroed each
    /// [`Ledger::settle`].
    step_fees: i128,
    /// `spread_capture = −Σ slippage` of the step just settled
    /// ([`Ledger::spread_capture`]); valid until the next settle.
    last_spread_capture: Cents,
    /// `Σ fees` of the step just settled ([`Ledger::fees`], always `≥ 0`);
    /// valid until the next settle.
    last_fees: Cents,
    /// `step_pnl = equity_n − equity_{n-1}` of the step just settled
    /// ([`Ledger::step_pnl`]); valid until the next settle.
    last_step_pnl: Cents,
}

impl Ledger {
    /// Create a ledger with `initial_capital` cash and the equity peak anchored
    /// at the same value.
    ///
    /// `initial_capital` must be `> 0` (the caller guarantees it, matching
    /// `config.validate`) so the drawdown denominator is never zero.
    #[must_use]
    pub fn new(initial_capital: Cents) -> Self {
        Self {
            cash: initial_capital,
            running_peak: initial_capital.value(),
            marks: BTreeMap::new(),
            position_marks: Vec::new(),
            greeks: BTreeMap::new(),
            prev_equity: initial_capital.value(),
            step_slippage: 0,
            step_fees: 0,
            last_spread_capture: Cents::new(0),
            last_fees: Cents::new(0),
            last_step_pnl: Cents::new(0),
        }
    }

    /// The current cash balance in integer cents.
    #[must_use]
    pub const fn cash(&self) -> Cents {
        self.cash
    }

    /// The per-leg valuation of the step most recently settled — the enriched
    /// engine→analytics hand-off (#30), in open-inventory order.
    ///
    /// Each [`PositionMark`] carries the leg's `position_id`, `contract`,
    /// `side`, `quantity`, `contract_multiplier`, resolved `mark`, `stale_mark`,
    /// and the unit Greeks as of the previous snapshot `S_{n-1}`
    /// ([`PositionMark::prior_greeks`]). Valid until the next [`Ledger::settle`],
    /// which clears and refills it.
    #[must_use]
    pub fn position_marks(&self) -> &[PositionMark] {
        &self.position_marks
    }

    /// The step just settled's `spread_capture = −Σ slippage` over its fills —
    /// the **single** sign flip from the slippage convention (positive =
    /// favourable execution,
    /// [docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)).
    ///
    /// Accumulated across the step's [`Ledger::apply_fill`] calls (which include
    /// the `on_start` fills for step 0) and reset each [`Ledger::settle`]; valid
    /// until the next settle.
    #[must_use]
    pub const fn spread_capture(&self) -> Cents {
        self.last_spread_capture
    }

    /// The step just settled's `Σ fees` over its fills — always `≥ 0`, and
    /// **subtracted** in the P&L attribution identity
    /// ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
    ///
    /// Accumulated and reset alongside [`Ledger::spread_capture`]; valid until
    /// the next [`Ledger::settle`].
    #[must_use]
    pub const fn fees(&self) -> Cents {
        self.last_fees
    }

    /// The step just settled's mark-to-market `step_pnl = equity_n −
    /// equity_{n-1}` in integer cents — the target the Greek attribution
    /// decomposes ([docs/05 §3](../../../docs/05-analytics-and-reporting.md#3-pl-attribution-by-greek)).
    ///
    /// Computed **once** here; step 0 uses the step-`−1` baseline
    /// `equity_{-1} = initial_capital`, so `step_pnl_0 = equity_0 −
    /// initial_capital`. Valid until the next [`Ledger::settle`].
    #[must_use]
    pub const fn step_pnl(&self) -> Cents {
        self.last_step_pnl
    }

    /// Apply one fill's cash flow: a **credit** for a sell (short side), a
    /// **debit** for a buy (long side), always **minus** the fee.
    ///
    /// Cash flow is `−side_sign × price × quantity × contract_multiplier`
    /// (selling a short leg credits `+`, buying a long leg debits `−`,
    /// [docs/01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table)),
    /// then the fee (always `≥ 0`) is subtracted. Cash moves **only** here,
    /// never in [`Ledger::settle`].
    ///
    /// It also accumulates this fill's `slippage` and `fees` into the current
    /// step's running totals, which [`Ledger::settle`] captures (as
    /// [`Ledger::spread_capture`] and [`Ledger::fees`]) and then zeroes for the
    /// next step. Cash and the fill-signal accumulators are the **only** state
    /// this method touches.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] if the scaled cash flow,
    /// the fee subtraction, the running cash total, or a fill-signal accumulator
    /// exceeds the integer-cents range.
    pub fn apply_fill(
        &mut self,
        fill: &Fill,
        contract_multiplier: u32,
    ) -> Result<(), BacktestError> {
        let gross = i128::from(fill.price.value())
            .checked_mul(i128::from(fill.quantity.value()))
            .and_then(|g| g.checked_mul(i128::from(contract_multiplier)))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        // -side_sign: sell (Short) credits (+), buy (Long) debits (-).
        let signed_flow = gross
            .checked_mul(-i128::from(sign_convention::side_sign(fill.side)))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let net = signed_flow
            .checked_sub(i128::from(fill.fees.value()))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        let net = i64::try_from(net).map_err(|_| BacktestError::ArithmeticOverflow)?;
        self.cash = self.cash.checked_add(Cents::new(net))?;
        // Accumulate the step's fill signals (raw slippage sum and fee sum); the
        // single sign flip to spread_capture happens once, at settle.
        self.step_slippage = self
            .step_slippage
            .checked_add(i128::from(fill.slippage.value()))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        self.step_fees = self
            .step_fees
            .checked_add(i128::from(fill.fees.value()))
            .ok_or(BacktestError::ArithmeticOverflow)?;
        Ok(())
    }

    /// Re-mark the open legs at `snapshot`'s mid and emit the step's single
    /// [`EquityPoint`].
    ///
    /// A held leg quoted this step marks at its snapshot mid (and refreshes the
    /// carried-forward value, flagged **not** stale); a leg whose contract is
    /// **absent** this step keeps its last-known mid — falling back to its entry
    /// premium if never quoted — and is flagged **stale** for the step (exposed
    /// through [`Ledger::position_marks`]). `position_value = Σ (mark ×
    /// quantity × contract_multiplier × side_sign)` (long `+`, short `−`),
    /// `equity = cash + position_value`, the peak is raised to `max(peak,
    /// equity)`, and `drawdown = (equity − peak) / peak` (never clamped). Cash
    /// is **not** touched here — only [`Ledger::apply_fill`] moves it.
    ///
    /// # Errors
    ///
    /// - [`BacktestError::DataOutOfOrder`] if a held leg is absent **at or after
    ///   its own expiry step**, where a settlement mark is mandatory and
    ///   carry-forward is illegal.
    /// - [`BacktestError::Conversion`] if an absent leg's expiration is still an
    ///   unresolved relative `Days(n)` (resolution happens once at tape
    ///   materialisation).
    /// - [`BacktestError::ArithmeticOverflow`] if a leg's marked value, the
    ///   position-value sum, or `cash + position_value` exceeds range.
    pub fn settle(
        &mut self,
        step: StepIndex,
        ts: SimTime,
        open: &[OpenPosition],
        snapshot: &ChainSnapshot,
    ) -> Result<EquityPoint, BacktestError> {
        let contract_multiplier = snapshot.spec.contract_multiplier;
        let multiplier = i128::from(contract_multiplier);
        // Reuse the scratch: clear (retains capacity, PB-1) then refill per leg.
        self.position_marks.clear();
        let mut position_value: i128 = 0;
        for leg in open {
            let (mark, stale) = self.resolve_mark(leg, snapshot)?;
            // The retained set still holds S_{n-1} here — read the prior endpoint
            // BEFORE the deferred update below overwrites it with S_n. Reading in
            // this loop (not the update loop) means two legs sharing one contract
            // both observe the same S_{n-1} Greeks.
            let prior_greeks = self.greeks.get(&leg.contract).copied();
            self.position_marks.push(PositionMark {
                position_id: leg.position_id,
                contract: leg.contract.clone(),
                side: leg.side,
                quantity: leg.quantity,
                contract_multiplier,
                mark,
                stale_mark: stale,
                prior_greeks,
            });
            let leg_value = i128::from(mark.value())
                .checked_mul(i128::from(leg.quantity.value()))
                .and_then(|v| v.checked_mul(multiplier))
                .and_then(|v| v.checked_mul(i128::from(sign_convention::side_sign(leg.side))))
                .ok_or(BacktestError::ArithmeticOverflow)?;
            position_value = position_value
                .checked_add(leg_value)
                .ok_or(BacktestError::ArithmeticOverflow)?;
        }
        let position_value_cents =
            i64::try_from(position_value).map_err(|_| BacktestError::ArithmeticOverflow)?;
        let equity_cents = self
            .cash
            .value()
            .checked_add(position_value_cents)
            .ok_or(BacktestError::ArithmeticOverflow)?;
        if equity_cents > self.running_peak {
            self.running_peak = equity_cents;
        }
        let drawdown = self.drawdown(equity_cents);

        // step_pnl = equity_n − equity_{n-1}; step 0's baseline is the initial
        // capital (prev_equity was seeded to it in `new`). Compute once here.
        self.last_step_pnl = Cents::new(
            equity_cents
                .checked_sub(self.prev_equity)
                .ok_or(BacktestError::ArithmeticOverflow)?,
        );
        self.prev_equity = equity_cents;

        // Capture the step's fill signals, then zero the accumulators for n+1.
        // spread_capture = −Σ slippage — the single sign flip.
        let spread_capture = self
            .step_slippage
            .checked_neg()
            .ok_or(BacktestError::ArithmeticOverflow)?;
        self.last_spread_capture = Cents::new(
            i64::try_from(spread_capture).map_err(|_| BacktestError::ArithmeticOverflow)?,
        );
        self.last_fees = Cents::new(
            i64::try_from(self.step_fees).map_err(|_| BacktestError::ArithmeticOverflow)?,
        );
        self.step_slippage = 0;
        self.step_fees = 0;

        // Deferred update of the retained observation endpoint to S_n: for each
        // held leg quoted this step, refresh its unit Greeks IN PLACE (a leg
        // absent this step carries its last-known Greeks forward, mirroring the
        // mark). Never `clear()`ed, so a warm step reuses the map's nodes (PB-1).
        for leg in open {
            if let Some(quote) = snapshot.quotes.get(&leg.contract) {
                let current = UnitGreeks {
                    delta: quote.delta,
                    gamma: quote.gamma,
                    theta: quote.theta,
                    vega: quote.vega,
                    implied_volatility: quote.implied_volatility,
                };
                match self.greeks.get_mut(&leg.contract) {
                    Some(slot) => *slot = current,
                    None => {
                        self.greeks.insert(leg.contract.clone(), current);
                    }
                }
            }
        }

        Ok(EquityPoint::new(
            step.value(),
            ts.value(),
            self.cash.value(),
            position_value_cents,
            equity_cents,
            drawdown,
        ))
    }

    /// Resolve a leg's mark this step and whether it is stale.
    ///
    /// The snapshot mid if the contract is quoted (refreshing the
    /// carried-forward value, `stale = false`); otherwise the last-known mid —
    /// or the entry premium if never quoted — carried forward with `stale =
    /// true`, **provided the step is before the contract's own expiry**.
    ///
    /// # Errors
    ///
    /// - [`BacktestError::DataOutOfOrder`] when the leg is absent **at or after
    ///   its own expiry instant** (`snapshot.ts ≥ expiration_ns`): a settlement
    ///   mark is mandatory there, so carrying a stale mark forward past a
    ///   contract's expiry is rejected rather than fabricated.
    /// - [`BacktestError::Conversion`] when the absent leg's expiration is an
    ///   unresolved relative `Days(n)` (see [`ContractKey::expiration_ns`]).
    fn resolve_mark(
        &mut self,
        leg: &OpenPosition,
        snapshot: &ChainSnapshot,
    ) -> Result<(PriceCents, bool), BacktestError> {
        if let Some(quote) = snapshot.quotes.get(&leg.contract) {
            // Update the carried-forward mid. `get_mut` avoids cloning the key
            // on the warm path; the clone only happens the first time a
            // contract is marked (warmup), keeping the step body allocation-free.
            match self.marks.get_mut(&leg.contract) {
                Some(slot) => *slot = quote.mid,
                None => {
                    self.marks.insert(leg.contract.clone(), quote.mid);
                }
            }
            return Ok((quote.mid, false));
        }
        // The contract is absent this step. Carry-forward is legal only BEFORE
        // its own expiry; at or after expiry a settlement mark is mandatory, so
        // reject the tape rather than fabricate a post-expiry price.
        let expiration_ns = leg.contract.expiration_ns()?;
        if snapshot.ts.value() >= expiration_ns {
            // Reuse the ordering error kind (docs/01 §6, docs/05 §2). Fields:
            // `ts` = the contract's expiry instant (reached/passed with no
            // settlement quote); `prev` = this snapshot's ts. expiry ≤ ts, so
            // the rendered "ts not strictly after previous" reads truthfully.
            return Err(BacktestError::DataOutOfOrder {
                step: snapshot.step.value(),
                ts: expiration_ns,
                prev: snapshot.ts.value(),
            });
        }
        let mark = self
            .marks
            .get(&leg.contract)
            .copied()
            .unwrap_or(leg.entry_premium);
        Ok((mark, true))
    }

    /// `(equity − running_peak) / running_peak`, never clamped; `0` at the
    /// degenerate `running_peak ≤ 0` (unreachable for a valid run).
    #[allow(
        clippy::cast_precision_loss,
        reason = "drawdown is the one documented analytic float; f64 is its wire type (docs/01 §9)"
    )]
    fn drawdown(&self, equity_cents: i64) -> f64 {
        if self.running_peak <= 0 {
            return 0.0;
        }
        (equity_cents as f64 - self.running_peak as f64) / self.running_peak as f64
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use rust_decimal_macros::dec;

    use super::{Ledger, UnitGreeks};
    use crate::domain::{
        Cents, ChainSnapshot, ContractKey, ExecutionMode, Fill, InstrumentSpec, OpenPosition,
        PositionId, PriceCents, Quantity, QuoteView, SimTime, StepIndex, Underlying,
    };
    use crate::error::BacktestError;

    const TS0: i64 = 1_750_291_200_000_000_000;
    /// Nanoseconds in one 86 400 s calendar day (UTC).
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    /// The fixtures' contract expiry — 30 days after `TS0`, so the per-step
    /// snapshots (at `TS0 + step`) are all well **before** expiry and a
    /// carry-forward is legal there.
    const EXPIRY: i64 = TS0 + 30 * NANOS_PER_DAY;

    fn und() -> Underlying {
        let Ok(u) = Underlying::new("SPX") else {
            panic!("SPX is valid");
        };
        u
    }

    fn qty(n: u32) -> Quantity {
        let Ok(q) = Quantity::new(n) else {
            panic!("{n} is a valid quantity");
        };
        q
    }

    fn key(strike: u64, style: OptionStyle) -> ContractKey {
        ContractKey {
            underlying: und(),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(EXPIRY)),
            strike: PriceCents::new(strike),
            style,
        }
    }

    fn quote(strike: u64, style: OptionStyle, mid: u64) -> QuoteView {
        // Fixtures always pass `mid >= 1`, so the bid stays non-negative without
        // `saturating_*` (banned by the rules, even in tests).
        debug_assert!(mid >= 1, "quote fixtures use a mid of at least 1c");
        QuoteView {
            contract: key(strike, style),
            bid: PriceCents::new(mid - 1),
            ask: PriceCents::new(mid + 1),
            mid: PriceCents::new(mid),
            bid_size: qty(10),
            ask_size: qty(10),
            implied_volatility: dec!(0.2),
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
        }
    }

    fn snapshot(step: u32, contract_mid: Option<(u64, OptionStyle, u64)>) -> ChainSnapshot {
        let mut quotes = BTreeMap::new();
        if let Some((strike, style, mid)) = contract_mid {
            let q = quote(strike, style, mid);
            quotes.insert(q.contract.clone(), q);
        }
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(1), 100) else {
            panic!("valid spec");
        };
        ChainSnapshot {
            ts: SimTime::new(TS0 + i64::from(step)),
            step: StepIndex::new(step),
            underlying: und(),
            underlying_price: PriceCents::new(500_000),
            spec,
            quotes,
        }
    }

    fn short_call(mid_entry: u64) -> OpenPosition {
        OpenPosition {
            position_id: PositionId::new(1),
            contract: key(510_000, OptionStyle::Call),
            side: Side::Short,
            quantity: qty(1),
            entry_premium: PriceCents::new(mid_entry),
        }
    }

    fn sell_fill(price: u64, fees: i64) -> Fill {
        Fill {
            ts: SimTime::new(TS0),
            step: StepIndex::new(0),
            contract: key(510_000, OptionStyle::Call),
            side: Side::Short,
            quantity: qty(1),
            price: PriceCents::new(price),
            fees: Cents::new(fees),
            slippage: Cents::new(0),
            mode: ExecutionMode::Naive,
        }
    }

    /// A short-call sell fill carrying explicit `slippage` and `fees` signals.
    fn sell_fill_signals(price: u64, fees: i64, slippage: i64) -> Fill {
        let mut fill = sell_fill(price, fees);
        fill.slippage = Cents::new(slippage);
        fill
    }

    /// A short-call leg of `n` contracts (entry premium `mid_entry`).
    fn short_call_qty(mid_entry: u64, n: u32) -> OpenPosition {
        OpenPosition {
            position_id: PositionId::new(1),
            contract: key(510_000, OptionStyle::Call),
            side: Side::Short,
            quantity: qty(n),
            entry_premium: PriceCents::new(mid_entry),
        }
    }

    /// The fixtures' default per-contract unit Greeks — the values [`quote`]
    /// stamps on every `QuoteView` (delta `0.5`, gamma `0.01`, theta `-0.05`,
    /// vega `0.1`, IV `0.2`).
    fn default_unit_greeks() -> UnitGreeks {
        UnitGreeks {
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
            implied_volatility: dec!(0.2),
        }
    }

    #[test]
    fn test_ledger_sell_credits_cash_minus_fees() {
        // Sell a short call at 200c, 1 contract, 100x multiplier, fee 65c.
        // cash delta = +200 * 1 * 100 - 65 = +19_935.
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let result = ledger.apply_fill(&sell_fill(200, 65), 100);
        assert!(matches!(result, Ok(())));
        assert_eq!(ledger.cash().value(), 10_000_000 + 20_000 - 65);
    }

    #[test]
    fn test_ledger_settle_short_leg_marks_as_liability() {
        // Sell short call for +20_000 cash (fee 0), then mark it at 200c mid:
        // position_value = 200 * 1 * 100 * side_sign(Short=-1) = -20_000.
        // equity = (10_000_000 + 20_000) + (-20_000) = 10_000_000. At a fresh
        // sell of a fairly-priced option, equity is unchanged (drawdown 0).
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let Ok(()) = ledger.apply_fill(&sell_fill(200, 0), 100) else {
            panic!("apply fill");
        };
        let snap = snapshot(0, Some((510_000, OptionStyle::Call, 200)));
        let point = ledger.settle(StepIndex::new(0), snap.ts, &[short_call(200)], &snap);
        let Ok(point) = point else {
            panic!("settle succeeds");
        };
        assert_eq!(point.cash_cents, 10_000_000 + 20_000);
        assert_eq!(point.position_value_cents, -20_000);
        assert_eq!(point.equity_cents, 10_000_000);
        assert!(
            (point.drawdown - 0.0).abs() < f64::EPSILON,
            "fresh peak → 0 drawdown"
        );
    }

    #[test]
    fn test_ledger_drawdown_is_negative_below_peak_never_clamped() {
        // No fills; peak = 10_000_000. Mark a short leg richer than entry so the
        // liability grows and equity falls below the peak → negative drawdown.
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        // Short leg with no cash applied: position_value = -(300*100) = -30_000.
        let snap = snapshot(1, Some((510_000, OptionStyle::Call, 300)));
        let point = ledger.settle(StepIndex::new(1), snap.ts, &[short_call(200)], &snap);
        let Ok(point) = point else {
            panic!("settle succeeds");
        };
        assert_eq!(point.equity_cents, 10_000_000 - 30_000);
        assert!(
            point.drawdown < 0.0,
            "equity below peak → negative drawdown"
        );
    }

    #[test]
    fn test_ledger_carries_last_known_mark_when_contract_absent() {
        // Step 0 marks the leg at 250c, step 1 has no quote for it → the
        // last-known mid (250c) is carried forward, not the entry premium (200c).
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let leg = short_call(200);
        let snap0 = snapshot(0, Some((510_000, OptionStyle::Call, 250)));
        let Ok(p0) = ledger.settle(
            StepIndex::new(0),
            snap0.ts,
            std::slice::from_ref(&leg),
            &snap0,
        ) else {
            panic!("settle 0");
        };
        assert_eq!(p0.position_value_cents, -25_000);
        let snap1 = snapshot(1, None); // the leg's contract is absent
        let Ok(p1) = ledger.settle(StepIndex::new(1), snap1.ts, &[leg], &snap1) else {
            panic!("settle 1");
        };
        // Carried forward 250c (not the 200c entry premium).
        assert_eq!(p1.position_value_cents, -25_000);
    }

    #[test]
    fn test_cash_unchanged_on_revaluation_only_step() {
        // Cash moves ONLY through fills. After one credit, repeated re-marking
        // at different mids leaves cash byte-identical while position_value —
        // and therefore equity — moves each step (the two-invariants rule).
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let Ok(()) = ledger.apply_fill(&sell_fill(200, 0), 100) else {
            panic!("apply fill");
        };
        let cash_after_fill = ledger.cash().value();
        assert_eq!(cash_after_fill, 10_000_000 + 20_000);

        let leg = short_call(200);
        let mut prev_position_value: Option<i64> = None;
        for (step, mid) in [(0u32, 200u64), (1, 300), (2, 150)] {
            let snap = snapshot(step, Some((510_000, OptionStyle::Call, mid)));
            let Ok(point) = ledger.settle(
                StepIndex::new(step),
                snap.ts,
                std::slice::from_ref(&leg),
                &snap,
            ) else {
                panic!("settle {step}");
            };
            // A revaluation-only step NEVER moves cash.
            assert_eq!(
                point.cash_cents, cash_after_fill,
                "cash unchanged by revaluation at step {step}"
            );
            assert_eq!(ledger.cash().value(), cash_after_fill);
            // But equity re-marks: consecutive position values differ.
            if let Some(prev) = prev_position_value {
                assert_ne!(
                    prev, point.position_value_cents,
                    "revaluation moved position_value at step {step}"
                );
            }
            prev_position_value = Some(point.position_value_cents);
        }
    }

    #[test]
    fn test_drawdown_below_minus_one_on_negative_equity() {
        // No fills: cash = peak = 10_000_000. A short leg marked so rich the
        // liability exceeds capital drives equity negative → drawdown < -1,
        // reported as-is (never clamped to the old [-1, 0] range).
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        // position_value = -(200_000 * 1 * 100) = -20_000_000.
        let snap = snapshot(1, Some((510_000, OptionStyle::Call, 200_000)));
        let Ok(point) = ledger.settle(StepIndex::new(1), snap.ts, &[short_call(200)], &snap) else {
            panic!("settle succeeds");
        };
        assert_eq!(point.equity_cents, 10_000_000 - 20_000_000);
        assert!(point.equity_cents < 0, "equity is negative");
        assert!(
            point.drawdown < -1.0,
            "negative equity → drawdown below -1, never clamped"
        );
    }

    #[test]
    fn test_stale_mark_carries_forward() {
        // Step 0 quotes the leg at 250c → fresh mark, not stale. Step 1 omits
        // it (before expiry) → the 250c mark carries forward AND the leg is
        // flagged stale for that step (exposed via position_marks).
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let leg = short_call(200);

        let snap0 = snapshot(0, Some((510_000, OptionStyle::Call, 250)));
        let settle0 = ledger.settle(
            StepIndex::new(0),
            snap0.ts,
            std::slice::from_ref(&leg),
            &snap0,
        );
        assert!(settle0.is_ok(), "settle 0 succeeds");
        let Some(mark0) = ledger.position_marks().first() else {
            panic!("one leg marked at step 0");
        };
        assert_eq!(mark0.position_id, PositionId::new(1));
        assert!(!mark0.stale_mark, "a quoted leg is not stale");
        assert_eq!(mark0.mark.value(), 250);

        let snap1 = snapshot(1, None); // the leg's contract is absent, pre-expiry
        let Ok(p1) = ledger.settle(
            StepIndex::new(1),
            snap1.ts,
            std::slice::from_ref(&leg),
            &snap1,
        ) else {
            panic!("settle 1");
        };
        let Some(mark1) = ledger.position_marks().first() else {
            panic!("one leg marked at step 1");
        };
        assert!(
            mark1.stale_mark,
            "an absent pre-expiry leg is flagged stale"
        );
        assert_eq!(
            mark1.mark.value(),
            250,
            "carries the last-known 250c, not the 200c entry"
        );
        assert_eq!(p1.position_value_cents, -25_000);
    }

    #[test]
    fn test_settle_enriched_leg_valuation_surfaces_all_inputs() {
        // The enriched hand-off (#30) surfaces, per open leg, every input the
        // attribution needs to reconstruct position_value and weight the Greeks:
        // position_id / contract / side / quantity / contract_multiplier / mark /
        // stale_mark, and prior_greeks = None at step 0 (no S_{-1}).
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let leg = short_call_qty(200, 3);
        let snap = snapshot(0, Some((510_000, OptionStyle::Call, 250)));
        let Ok(_point) = ledger.settle(
            StepIndex::new(0),
            snap.ts,
            std::slice::from_ref(&leg),
            &snap,
        ) else {
            panic!("settle 0 succeeds");
        };
        let Some(mark) = ledger.position_marks().first() else {
            panic!("one leg in the hand-off");
        };
        assert_eq!(mark.position_id, PositionId::new(1));
        assert_eq!(mark.contract, key(510_000, OptionStyle::Call));
        assert_eq!(mark.side, Side::Short);
        assert_eq!(
            mark.quantity.value(),
            3,
            "quantity surfaced, not pre-weighted"
        );
        assert_eq!(
            mark.contract_multiplier, 100,
            "multiplier surfaced separately from the unit Greeks"
        );
        assert_eq!(mark.mark.value(), 250, "resolved mark is the snapshot mid");
        assert!(!mark.stale_mark, "a quoted leg is not stale");
        assert!(
            mark.prior_greeks.is_none(),
            "step 0 has no S_-1, so the prior-Greek endpoint is absent"
        );
    }

    #[test]
    fn test_prior_greeks_are_the_previous_snapshot_endpoint() {
        // The retained unit Greeks are the S_{n-1} endpoint: step 1's hand-off
        // carries step 0's Greeks (delta 0.5), NOT step 1's (delta 0.3).
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let leg = short_call(200);

        let snap0 = snapshot(0, Some((510_000, OptionStyle::Call, 250)));
        let Ok(_p0) = ledger.settle(
            StepIndex::new(0),
            snap0.ts,
            std::slice::from_ref(&leg),
            &snap0,
        ) else {
            panic!("settle 0");
        };

        let mut snap1 = snapshot(1, Some((510_000, OptionStyle::Call, 260)));
        if let Some(q) = snap1.quotes.get_mut(&key(510_000, OptionStyle::Call)) {
            q.delta = dec!(0.3);
        }
        let Ok(_p1) = ledger.settle(
            StepIndex::new(1),
            snap1.ts,
            std::slice::from_ref(&leg),
            &snap1,
        ) else {
            panic!("settle 1");
        };
        let Some(mark1) = ledger.position_marks().first() else {
            panic!("one leg at step 1");
        };
        assert_eq!(
            mark1.prior_greeks,
            Some(default_unit_greeks()),
            "prior_greeks is S_0's unit Greeks (delta 0.5), never S_1's (0.3)"
        );
    }

    #[test]
    fn test_prior_greeks_carry_forward_through_a_stale_step() {
        // A leg absent this step carries its last-known unit Greeks forward, the
        // same way its mark carries forward. Step 0 quotes it (delta 0.5); step 1
        // omits it (stale); step 2 quotes it again (delta 0.3). Step 2's prior
        // endpoint is still S_0 (delta 0.5) — the stale step never overwrote it.
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let leg = short_call(200);

        let snap0 = snapshot(0, Some((510_000, OptionStyle::Call, 250)));
        let Ok(_p0) = ledger.settle(
            StepIndex::new(0),
            snap0.ts,
            std::slice::from_ref(&leg),
            &snap0,
        ) else {
            panic!("settle 0");
        };

        let snap1 = snapshot(1, None); // the leg's contract is absent (pre-expiry)
        let Ok(_p1) = ledger.settle(
            StepIndex::new(1),
            snap1.ts,
            std::slice::from_ref(&leg),
            &snap1,
        ) else {
            panic!("settle 1");
        };
        let Some(mark1) = ledger.position_marks().first() else {
            panic!("one leg at step 1");
        };
        assert_eq!(
            mark1.prior_greeks,
            Some(default_unit_greeks()),
            "the stale step still sees S_0's Greeks as its prior endpoint"
        );

        let mut snap2 = snapshot(2, Some((510_000, OptionStyle::Call, 270)));
        if let Some(q) = snap2.quotes.get_mut(&key(510_000, OptionStyle::Call)) {
            q.delta = dec!(0.3);
        }
        let Ok(_p2) = ledger.settle(
            StepIndex::new(2),
            snap2.ts,
            std::slice::from_ref(&leg),
            &snap2,
        ) else {
            panic!("settle 2");
        };
        let Some(mark2) = ledger.position_marks().first() else {
            panic!("one leg at step 2");
        };
        assert_eq!(
            mark2.prior_greeks,
            Some(default_unit_greeks()),
            "the absent step 1 did not overwrite the retained S_0 Greeks"
        );
    }

    #[test]
    fn test_step_pnl_uses_initial_capital_baseline_at_step_zero() {
        // step_pnl_0 = equity_0 − initial_capital (the step-−1 baseline); every
        // later step_pnl is equity_n − equity_{n-1}.
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let Ok(()) = ledger.apply_fill(&sell_fill(200, 0), 100) else {
            panic!("apply fill: cash = 10_020_000");
        };
        let leg = short_call(200);

        // Step 0: mark at 250c → position_value −25_000, equity 9_995_000.
        let snap0 = snapshot(0, Some((510_000, OptionStyle::Call, 250)));
        let Ok(p0) = ledger.settle(
            StepIndex::new(0),
            snap0.ts,
            std::slice::from_ref(&leg),
            &snap0,
        ) else {
            panic!("settle 0");
        };
        assert_eq!(p0.equity_cents, 9_995_000);
        assert_eq!(
            ledger.step_pnl().value(),
            9_995_000 - 10_000_000,
            "step 0 baseline is the initial capital"
        );

        // Step 1: mark at 300c → position_value −30_000, equity 9_990_000.
        let snap1 = snapshot(1, Some((510_000, OptionStyle::Call, 300)));
        let Ok(p1) = ledger.settle(
            StepIndex::new(1),
            snap1.ts,
            std::slice::from_ref(&leg),
            &snap1,
        ) else {
            panic!("settle 1");
        };
        assert_eq!(p1.equity_cents, 9_990_000);
        assert_eq!(
            ledger.step_pnl().value(),
            9_990_000 - 9_995_000,
            "step 1 is the mark-to-market delta vs step 0"
        );
    }

    #[test]
    fn test_settle_aggregates_and_resets_step_fill_signals() {
        // spread_capture = −Σ slippage and fees = Σ fees accumulate across the
        // step's fills, are exposed at settle, and zero for the next step.
        let mut ledger = Ledger::new(Cents::new(10_000_000));
        let leg = short_call(200);

        // Two fills in the step: slippage 20 and −5 → Σ = 15 → spread_capture
        // −15; fees 65 and 30 → Σ = 95.
        let Ok(()) = ledger.apply_fill(&sell_fill_signals(200, 65, 20), 100) else {
            panic!("apply fill 1");
        };
        let Ok(()) = ledger.apply_fill(&sell_fill_signals(200, 30, -5), 100) else {
            panic!("apply fill 2");
        };
        let snap0 = snapshot(0, Some((510_000, OptionStyle::Call, 200)));
        let Ok(_p0) = ledger.settle(
            StepIndex::new(0),
            snap0.ts,
            std::slice::from_ref(&leg),
            &snap0,
        ) else {
            panic!("settle 0");
        };
        assert_eq!(
            ledger.spread_capture().value(),
            -15,
            "spread_capture is −Σ slippage, the single sign flip"
        );
        assert_eq!(ledger.fees().value(), 95, "fees is Σ fees, always ≥ 0");

        // A revaluation-only step (no fills) exposes zeroed signals.
        let snap1 = snapshot(1, Some((510_000, OptionStyle::Call, 210)));
        let Ok(_p1) = ledger.settle(
            StepIndex::new(1),
            snap1.ts,
            std::slice::from_ref(&leg),
            &snap1,
        ) else {
            panic!("settle 1");
        };
        assert_eq!(
            ledger.spread_capture().value(),
            0,
            "no fills → spread_capture resets to 0"
        );
        assert_eq!(ledger.fees().value(), 0, "no fills → fees reset to 0");
    }

    #[test]
    fn test_held_leg_missing_at_expiry_rejected() {
        // A held leg absent AT its expiry step (ts == expiry) and AFTER it
        // (ts > expiry) is DataOutOfOrder — a settlement mark is mandatory, so
        // carrying a stale mark past a contract's own expiry is illegal.
        let leg = short_call(200); // expires at EXPIRY (via key())
        for offset in [0i64, 1, NANOS_PER_DAY] {
            let mut ledger = Ledger::new(Cents::new(10_000_000));
            let mut snap = snapshot(5, None); // no quote for the leg
            snap.ts = SimTime::new(EXPIRY + offset);
            let result = ledger.settle(snap.step, snap.ts, std::slice::from_ref(&leg), &snap);
            assert!(
                matches!(result, Err(BacktestError::DataOutOfOrder { .. })),
                "absent at/after expiry (offset {offset}) must be rejected"
            );
        }
    }
}

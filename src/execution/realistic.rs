//! The realistic fill model: the `option-chain-orderbook` adapter (v0.2,
//! feature `orderbook`, issue #22).
//!
//! [`RealisticFill`] routes each `Submit` intent through a real options
//! matching engine ([`option_chain_orderbook`] on top of `orderbook-rs`), so
//! queue position, per-strike depth, and market impact are **properties of the
//! matching**, not configured offsets
//! ([docs/04 §5](../../../docs/04-execution-models.md),
//! [ADR-0002](../../../docs/adr/0002-order-book-level-fill-simulation.md)). It
//! is the **only** seam where `option_chain_orderbook` newtypes appear and
//! **no raw `f64` crosses it** — everything is integer cents scaled to the
//! book's `u128` ticks.
//!
//! # What this issue (#22) builds
//!
//! The adapter foundation: leaf-book construction, seeded `OrderId` generation
//! from disjoint ranges, cents↔tick scaling, the side+action→Buy/Sell mapping,
//! marketable-limit conversion, submission + per-level fill capture, and the
//! `option_chain_orderbook::Error` → [`BacktestError`] mapping. Per-strike book
//! **seeding from a snapshot** (#023) and queue/impact goldens (#024) build on
//! top; the **between-snapshot refresh** (#025, [`RealisticFill::refresh_books`])
//! rebuilds the seeded liquidity every step and captures refresh-generated fills;
//! the mode switch (#026) is next. [`RealisticFill::seed_maker_limit`] and
//! [`RealisticFill::next_maker_order_id`] are the seeding primitives #023
//! consumes.
//!
//! # Both modes emit the identical [`Fill`] shape
//!
//! Every fill is stamped by the shared [`assemble_fill`] exactly as the naive
//! model's is, so a naive fill and a realistic fill of the same intent are
//! byte-shape identical and only their values differ. A marketable order that
//! walks several price levels yields **one [`Fill`] per level** (each at that
//! level's executed price and size), the first carrying the once-per-order fee
//! ([`FeeCharge::FirstFill`]) and every later level only per-contract fees
//! ([`FeeCharge::LaterFill`]) — this is `LaterFill`'s first production use.
//!
//! # Determinism
//!
//! `OrderId`s come from **seeded [`Id::Sequential`] counters**, never
//! `Id::new`/`new_uuid` (which are random —
//! [rules/global_rules.md](../../../rules/global_rules.md) "Determinism"). Leaf
//! books live in a `BTreeMap` (never a `HashMap`), submission order is fixed,
//! and the fill path is synchronous with **no `.await`** — the book is driven
//! in-process (`nats`/`sequencer` features off). Same `(seed, config, data)` ⇒
//! byte-identical fills.
//!
//! # DEVIATIONS from the v0.7.0-era spec (for architect review)
//!
//! The pinned [`docs/specs/option-chain-orderbook.md`] describes v0.7.0; the
//! resolved crate is **0.9.1**. Two deliberate deviations:
//!
//! 1. **Capture via `add_limit_order_full`, not
//!    `arm_trade_capture`/`last_trade_result`.** The `_full` methods (0.8.0+)
//!    return *this call's own* [`TradeResult`] directly, avoiding the
//!    single-slot last-write-wins footgun of the shared-capture API. The
//!    per-level fill data is identical (the `TradeResult` trade list); the
//!    `_full` path is simply race-free and needs no arm/poll dance.
//! 2. **Marketable submits use GTC + explicit `cancel_order`, not an IOC
//!    time-in-force.** This matches the spec's literal "cancel the unfilled
//!    remainder (IOC)" wording and is *forced* by upstream capture semantics:
//!    on an unfillable **IOC** remainder the `_full`/`_with_result` methods
//!    return a typed error and route the fills to the trade listener **only**,
//!    so a partial marketable walk submitted IOC would lose its captured fills.
//!    Submitting **GTC** fills up to the aggressive-limit cap, rests the
//!    remainder, and returns `Ok` with every fill in the call's own
//!    [`TradeResult`]; the adapter then discards the resting remainder with
//!    `cancel_order` when the intent is IOC. Deterministic, and no fill is lost.
//!
//! # optionstratlib version shim (for architect review)
//!
//! `OptionOrderBook::new(symbol, OptionStyle)` takes an
//! `optionstratlib::OptionStyle` **by value**, and the published crate (0.9.1)
//! pins optionstratlib `^0.17` while `ironcondor` is on 0.18. The resolver
//! keeps two optionstratlib copies; this module names the 0.17 `OptionStyle`
//! (aliased [`ObOptionStyle`]) **only** to construct leaf books, converting from
//! the crate's 0.18 [`OptionStyle`] with a trivial `Call`/`Put` match. Remove
//! the `optionstratlib_ob` shim once the matching crate republishes on
//! optionstratlib 0.18 (or re-exports `OptionStyle`).

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use option_chain_orderbook::{OptionOrderBook, OrderId as ObOrderId, Side as ObSide, TradeResult};
use optionstratlib::{OptionStyle, Side};
use optionstratlib_ob::OptionStyle as ObOptionStyle;

use crate::config::{FeeSchedule, LiquidityProfile};
use crate::domain::{
    ChainSnapshot, ContractKey, ExecutionMode, Fill, OrderCommand, OrderIntent, PriceCents,
    Quantity, QuoteView, TimeInForce,
};
use crate::error::BacktestError;

use super::{ExecutionModel, FeeCharge, FillDraft, FillGroup, assemble_fill, liquidity};

/// The first strategy `OrderId`. Strategy ids occupy the low range
/// `[STRATEGY_ID_BASE, MAKER_ID_BASE)`; a handle at or above [`MAKER_ID_BASE`]
/// is out of the strategy's range and rejected.
const STRATEGY_ID_BASE: u64 = 1;

/// The first seeded-maker `OrderId`. Seeded-maker ids occupy the high range
/// `[MAKER_ID_BASE, u64::MAX]`, **disjoint** from strategy ids, so #023's
/// liquidity never collides with strategy orders. `1 << 48` leaves ~2.8·10¹⁴
/// strategy ids below and ~2.1·10¹⁴ maker ids above.
pub(crate) const MAKER_ID_BASE: u64 = 1 << 48;

/// The realistic fill model: routes intents through per-contract leaf
/// [`OptionOrderBook`]s and reads fills back as the shared [`Fill`].
///
/// Holds the two config values it needs ([`FeeSchedule`], the marketable price
/// cap), the run `seed` (the reproducibility anchor), two **disjoint** seeded
/// `OrderId` counters, and a `BTreeMap` of leaf books keyed by [`ContractKey`]
/// (deterministic iteration; #023 seeds these). It does **not** derive
/// `Clone`/`PartialEq`: an [`OptionOrderBook`] carries live matching state that
/// must not be duplicated.
pub struct RealisticFill {
    /// The fee schedule stamped onto each fill.
    fees: FeeSchedule,
    /// Marketable price cap in ticks off the touch (`config.marketable_cap_ticks`).
    marketable_cap_ticks: u32,
    /// The run seed — the reproducibility anchor. #023's ladder seeding is a
    /// pure function of the profile, quotes, and tick and draws no RNG; the
    /// seed is retained for any later seeded-RNG liquidity model.
    seed: u64,
    /// Next strategy `OrderId` (low, disjoint range).
    next_strategy_id: u64,
    /// Next seeded-maker `OrderId` (high, disjoint range).
    next_maker_id: u64,
    /// The per-strike book-seeding profile (#023). `None` is the **raw
    /// adapter** — no auto-seeding or refresh, books are hand-built via
    /// [`Self::seed_maker_limit`]; `Some` reseeds from **every** snapshot
    /// [`Self::fill`] sees (#025). The config-driven engine path always supplies
    /// `Some(config.liquidity_profile)` (reproducible from the manifest).
    liquidity_profile: Option<LiquidityProfile>,
    /// The seeded-maker order ids still **resting** in each leaf book, in stable
    /// key order — the exact set the next snapshot's refresh cancels (#025). A
    /// snapshot is the only ground truth for depth, so every step cancels these
    /// and reseeds fresh. Reused across steps: each `Vec` is **cleared in place**
    /// (capacity retained) at cancel time, then refilled by the reseed, so this
    /// tracking state itself allocates nothing steady-state
    /// ([docs/07 §4](../../../docs/07-performance-and-security.md)). (The reseed's
    /// per-order [`OptionOrderBook::add_limit_order_full`] still returns an owned
    /// `TradeResult` whose `symbol` `String` allocates per call upstream — the one
    /// residual per-step allocation on this path, profiled and addressed in #029;
    /// the realistic path is not covered by the naive zero-alloc gate.) Only the
    /// **auto-reseed** path records here; hand-seeded depth
    /// ([`Self::seed_maker_limit`]) is untracked and never auto-cancelled.
    resting_seed_ids: BTreeMap<ContractKey, Vec<u64>>,
    /// Metadata for **strategy** limit orders currently resting in a leaf book,
    /// keyed by their book-`OrderId` sequence value (strategy-id range). Just
    /// enough to assemble a refresh-generated [`Fill`] when a reseed order
    /// crosses one (#025): its trade side, its decision-time mid (the slippage
    /// reference), how much is still resting, and whether its first fill has
    /// been charged the per-order fee. Never a `HashMap` — a `BTreeMap` keeps the
    /// refresh deterministic. An entry is removed once fully consumed **by a
    /// refresh cross** (the e1 path).
    ///
    /// Eviction is deliberately partial while strategy-order lifecycle is
    /// deferred: an entry a *taker intent* consumes (e2 `fill_submit`) or that a
    /// logical cancel removes is not evicted here, so this map is bounded only by
    /// the count of resting strategy limits until Cancel/Replace lands. This is
    /// unreachable today — every shipped strategy is marketable-IOC and never
    /// rests a GTC limit, so the map stays empty — but the eviction path and the
    /// e2-consumed-maker fill (its maker-side `Fill` is currently only emitted on
    /// the e1 path) must be closed together with that lifecycle work.
    resting_strategy: BTreeMap<u64, RestingStrategyOrder>,
    /// Reusable scratch for the per-step reseed plan — sized once, cleared in
    /// place by [`liquidity::plan_seed_into`] each refresh, so the refresh's own
    /// plan buffer never reallocates ([docs/07 §4](../../../docs/07-performance-and-security.md)).
    seed_plan: Vec<liquidity::SeedOrder>,
    /// The fill→order grouping for the most recent [`Self::fill`] call — one
    /// [`FillGroup`] per `Submit` (e2) that produced at least one fill, in
    /// command order (the fill→order correlation channel the engine reads via
    /// [`Self::fill_groups`]). A marketable order walking `n` price levels appends
    /// `n` fills and one group of `fill_count = n`, so the engine mints one
    /// order/position for it and assigns `fill_seq = 0..n`. **Reusable scratch:**
    /// cleared in place each `fill` (capacity retained), so it adds no
    /// steady-state allocation. Refresh-generated fills (e1) carry **no** group
    /// (they belong to a prior-step resting order, not a current command).
    fill_groups: Vec<FillGroup>,
    /// Per-contract leaf books, in stable key order (never a `HashMap`).
    books: BTreeMap<ContractKey, OptionOrderBook>,
}

/// A resting **strategy** limit order the refresh must be able to fill when a
/// later snapshot's reseed depth crosses it (#025).
///
/// The book owns the order's identity, price, and queue position; this carries
/// only what the shared [`assemble_fill`] needs that the book cannot re-derive —
/// the trade `side` and the original `decision_mid` — plus the `remaining`
/// resting size (to know when the order is exhausted) and `first_fill_done` (so
/// the once-per-order fee is charged exactly once, on the order's first fill,
/// whether that happened at submit or on a later refresh).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestingStrategyOrder {
    /// The order's trade side (`Long` = buy, `Short` = sell) — the slippage sign
    /// and the assembled fill's side.
    side: Side,
    /// The decision-time mid the order was submitted against — the slippage
    /// reference, invariant across refreshes.
    decision_mid: PriceCents,
    /// Contracts still resting (decremented as refresh fills consume it).
    remaining: u64,
    /// Whether the order's first fill has already carried the per-order fee.
    first_fill_done: bool,
}

impl std::fmt::Debug for RealisticFill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `OptionOrderBook` holds live matching state with no useful `Debug`;
        // report the book count, not the books.
        f.debug_struct("RealisticFill")
            .field("fees", &self.fees)
            .field("marketable_cap_ticks", &self.marketable_cap_ticks)
            .field("seed", &self.seed)
            .field("next_strategy_id", &self.next_strategy_id)
            .field("next_maker_id", &self.next_maker_id)
            .field("liquidity_profile", &self.liquidity_profile)
            .field("resting_seed_ids", &self.resting_seed_ids.len())
            .field("resting_strategy", &self.resting_strategy.len())
            .field("fill_groups", &self.fill_groups.len())
            .field("books", &self.books.len())
            .finish()
    }
}

impl RealisticFill {
    /// Build a **raw-adapter** realistic fill model — no automatic book
    /// seeding.
    ///
    /// The seeded `OrderId` counters start at their disjoint range bases; the
    /// leaf-book map starts empty. With no [`LiquidityProfile`], [`Self::fill`]
    /// routes against whatever depth the caller hand-builds via
    /// [`Self::seed_maker_limit`] — the constructor tests and the raw #022
    /// adapter path use. For the config-driven, snapshot-seeded model use
    /// [`Self::with_liquidity_profile`]. `marketable_cap_ticks` must be `> 0` —
    /// [`crate::config::BacktestConfig::validate`] guarantees it.
    #[must_use = "the constructed fill model must be used to produce fills"]
    pub fn new(fees: FeeSchedule, marketable_cap_ticks: u32, seed: u64) -> Self {
        Self {
            fees,
            marketable_cap_ticks,
            seed,
            next_strategy_id: STRATEGY_ID_BASE,
            next_maker_id: MAKER_ID_BASE,
            liquidity_profile: None,
            resting_seed_ids: BTreeMap::new(),
            resting_strategy: BTreeMap::new(),
            seed_plan: Vec::new(),
            fill_groups: Vec::new(),
            books: BTreeMap::new(),
        }
    }

    /// Build a realistic fill model that **reseeds** each strike's book from
    /// **every** snapshot per `profile` (#023 first-seed + #025 refresh).
    ///
    /// Identical to [`Self::new`] but carries the [`LiquidityProfile`] the
    /// engine reads from `config.liquidity_profile`, so [`Self::fill`] seeds the
    /// per-strike ladders (touch + `L` deeper levels, geometric decay) **before**
    /// routing the step's commands — a strategy order then queues behind the
    /// seeded depth at its entry step ([docs/04 §6](../../../docs/04-execution-models.md)).
    /// On every later snapshot the stale seed is cancelled and fresh depth is
    /// reseeded from that snapshot's quotes, so synthetic depth never accumulates
    /// ([docs/04 §6.1](../../../docs/04-execution-models.md)). The profile is
    /// recorded in the run config, so the seeded book is reproducible from the
    /// manifest.
    #[must_use = "the constructed fill model must be used to produce fills"]
    pub fn with_liquidity_profile(
        fees: FeeSchedule,
        marketable_cap_ticks: u32,
        seed: u64,
        profile: LiquidityProfile,
    ) -> Self {
        Self {
            fees,
            marketable_cap_ticks,
            seed,
            next_strategy_id: STRATEGY_ID_BASE,
            next_maker_id: MAKER_ID_BASE,
            liquidity_profile: Some(profile),
            resting_seed_ids: BTreeMap::new(),
            resting_strategy: BTreeMap::new(),
            seed_plan: Vec::new(),
            fill_groups: Vec::new(),
            books: BTreeMap::new(),
        }
    }

    /// The run seed — the reproducibility anchor. #023's ladder seeding draws
    /// no RNG; the seed is retained for a later seeded-RNG liquidity model.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }

    /// Mint the next **strategy** `OrderId` from the low seeded range.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] when the strategy range is
    /// exhausted (the counter reached [`MAKER_ID_BASE`]) — a strategy handle
    /// must never stray into the seeded-maker range.
    fn next_strategy_order_id(&mut self) -> Result<ObOrderId, BacktestError> {
        let id = self.next_strategy_id;
        if id >= MAKER_ID_BASE {
            return Err(BacktestError::Execution(format!(
                "strategy order id range exhausted at {id} (maker range begins at {MAKER_ID_BASE})"
            )));
        }
        // `id < MAKER_ID_BASE`, so `+ 1` cannot overflow `u64`.
        self.next_strategy_id = id + 1;
        Ok(ObOrderId::Sequential(id))
    }

    /// Mint the next **seeded-maker** `OrderId` from the high seeded range —
    /// the id source #023's liquidity seeding draws from.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] when the maker range is exhausted
    /// (the `u64` counter would wrap).
    pub(crate) fn next_maker_order_id(&mut self) -> Result<ObOrderId, BacktestError> {
        let id = self.next_maker_id;
        let next = id.checked_add(1).ok_or_else(|| {
            BacktestError::Execution("seeded-maker order id range exhausted".to_string())
        })?;
        self.next_maker_id = next;
        Ok(ObOrderId::Sequential(id))
    }

    /// Get (or lazily construct) the leaf [`OptionOrderBook`] for `contract`.
    ///
    /// The book is keyed by the contract's identity and constructed with its
    /// canonical `contract_id` symbol and 0.18→0.17 [`OptionStyle`] conversion.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Conversion`] when the contract's expiration is
    /// unresolved (cannot form a `contract_id`).
    fn leaf_book(&mut self, contract: &ContractKey) -> Result<&OptionOrderBook, BacktestError> {
        leaf_book_in(&mut self.books, contract)
    }

    /// Seed one resting maker limit into `contract`'s leaf book — the #023
    /// liquidity primitive, and how tests hand-build depth.
    ///
    /// `is_ask` rests a sell (ask) at `price`; `!is_ask` a bid (buy). The order
    /// carries a seeded-maker `OrderId` from the disjoint high range, rests
    /// (`TimeInForce::Gtc`), and — into an empty side — produces no fill.
    /// Prices are integer cents scaled to the book's `u128` ticks via
    /// `tick_size_cents`. (Refresh-generated fills from a seed crossing a
    /// resting strategy order are #025 scope, not this primitive's.)
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::PriceNotTickAligned`] when `price` is not a
    /// multiple of `tick_size_cents`, [`BacktestError::Execution`] when the
    /// maker id range is exhausted or the tick is zero,
    /// [`BacktestError::Conversion`] when the contract's expiration is
    /// unresolved, and [`BacktestError::OrderBook`] when the book rejects the
    /// order.
    pub fn seed_maker_limit(
        &mut self,
        contract: &ContractKey,
        is_ask: bool,
        price: PriceCents,
        quantity: Quantity,
        tick_size_cents: PriceCents,
    ) -> Result<(), BacktestError> {
        let tick = tick_size_cents.value();
        let price_ticks = cents_to_ticks(price, tick)?;
        let qty = u64::from(quantity.value());
        let side = if is_ask { ObSide::Sell } else { ObSide::Buy };
        let id = self.next_maker_order_id()?;
        let book = self.leaf_book(contract)?;
        book.add_limit_order(id, side, price_ticks, qty)?;
        Ok(())
    }

    /// The between-snapshot book refresh — phase **e1** of the step, run before
    /// the step's command fills (#025,
    /// [docs/04 §6.1](../../../docs/04-execution-models.md),
    /// [docs/02 §3.2](../../../docs/02-engine-architecture.md)).
    ///
    /// A snapshot is the only ground truth for depth, so when a
    /// [`LiquidityProfile`] is configured this **cancels every seeded-maker order
    /// still resting from the previous snapshot** and **reseeds fresh depth from
    /// `snap`**, in the fixed plan order (ascending [`ContractKey`], bid side
    /// before ask side). A reseed order that **crosses** a resting strategy limit
    /// matches it on add; each such match is captured from the reseed call's own
    /// `TradeResult` and appended to `out_fills` as a step-`n` refresh fill —
    /// this is how a resting order fills as the market moves onto it between
    /// steps. On the **first** snapshot the cancel step finds nothing resting, so
    /// it degenerates to the initial seed (#023). A no-op for the raw adapter
    /// (`liquidity_profile = None`): its hand-seeded depth is left untouched.
    ///
    /// **Strategy orders are never cancelled or reinserted** here — only their
    /// seeded-maker neighbours are — so a resting strategy order keeps its
    /// original `OrderId` and rest timestamp, and therefore its **aged price-time
    /// priority ahead of** the freshly reseeded depth ([docs/04 §6.1](../../../docs/04-execution-models.md) step 5).
    ///
    /// The ladder is a deterministic function of `(profile, quotes, tick)`; no
    /// RNG is drawn, so the run `seed` is not consumed here.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] when the tick is zero or a maker id
    /// range is exhausted, [`BacktestError::PriceNotTickAligned`] for a
    /// mis-aligned quote, [`BacktestError::ArithmeticOverflow`] on cents/size
    /// overflow, [`BacktestError::OrderBook`] when the book rejects an order, and
    /// [`BacktestError::Conversion`] for an unresolved expiration.
    fn refresh_books(
        &mut self,
        snap: &ChainSnapshot,
        out_fills: &mut Vec<Fill>,
    ) -> Result<(), BacktestError> {
        // Raw adapter: no auto-reseed, and no auto-tracked seed to cancel — the
        // hand-built book is left exactly as the caller left it.
        let Some(profile) = self.liquidity_profile else {
            return Ok(());
        };
        let tick = snap.spec.tick_size_cents.value();
        if tick == 0 {
            // Defence in depth: `InstrumentSpec` validates `tick > 0` at ingest.
            return Err(BacktestError::Execution(
                "instrument tick_size_cents is zero at the realistic refresh".to_string(),
            ));
        }
        // 1. Cancel every stale seeded-maker order (empty on the first snapshot
        //    ⇒ a plain initial seed). Strategy ids are never touched.
        self.cancel_stale_seed()?;
        // 2. Reseed fresh depth in the fixed plan order, capturing any
        //    refresh-generated fills of resting strategy limits the reseed
        //    crosses. The scratch plan buffer is moved out and back so the
        //    reseed can drive `&mut self` freely without a self-borrow clash;
        //    the buffer's capacity is retained across steps.
        let mut plan = std::mem::take(&mut self.seed_plan);
        let outcome = self.reseed(&mut plan, snap, &profile, tick, out_fills);
        self.seed_plan = plan;
        outcome
    }

    /// Cancel every seeded-maker order still resting from the previous snapshot,
    /// clearing each per-book id list **in place** (capacity retained) so the
    /// reseed can refill it without allocating.
    ///
    /// Only seeded-maker ids are cancelled (strategy ids never enter
    /// `resting_seed_ids`); an id the book already consumed cancels to
    /// `Ok(false)`, which is fine.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::OrderBook`] when a cancel is rejected by the book.
    fn cancel_stale_seed(&mut self) -> Result<(), BacktestError> {
        let Self {
            books,
            resting_seed_ids,
            ..
        } = self;
        for (contract, ids) in resting_seed_ids.iter_mut() {
            if let Some(book) = books.get(contract) {
                for &id in ids.iter() {
                    // `Ok(false)` = already gone (fully filled); `Err` propagates.
                    let _cancelled = book.cancel_order(ObOrderId::Sequential(id))?;
                }
            }
            ids.clear();
        }
        Ok(())
    }

    /// Reseed fresh depth from `snap` in the fixed plan order, recording each
    /// resting reseed order for the next snapshot's cancel and capturing any
    /// refresh-generated fills of resting strategy limits the reseed crosses.
    ///
    /// # Errors
    ///
    /// Propagates every [`liquidity::plan_seed_into`], scaling, book, and fill
    /// assembly error.
    fn reseed(
        &mut self,
        plan: &mut Vec<liquidity::SeedOrder>,
        snap: &ChainSnapshot,
        profile: &LiquidityProfile,
        tick: u64,
        out_fills: &mut Vec<Fill>,
    ) -> Result<(), BacktestError> {
        liquidity::plan_seed_into(snap, profile, plan)?;
        for order in plan.iter() {
            let price_ticks = cents_to_ticks(order.price, tick)?;
            let side = if order.is_ask {
                ObSide::Sell
            } else {
                ObSide::Buy
            };
            let qty = u64::from(order.size.value());
            // Mint the seeded-maker id BEFORE borrowing the book (disjoint borrows).
            let id = self.next_maker_order_id()?;
            let trade_result: TradeResult = {
                let book = leaf_book_in(&mut self.books, &order.contract)?;
                book.add_limit_order_full(id, side, price_ticks, qty)?
            };
            // A reseed order can only cross a RESTING STRATEGY limit (stale seed
            // was cancelled above, and same-side reseed levels never cross each
            // other), so each trade fills a strategy maker — capture it against
            // `snap` (one `Fill` per level).
            for trade in trade_result.match_result.trades().as_vec() {
                self.capture_refresh_fill(
                    trade.maker_order_id(),
                    trade.price().as_u128(),
                    trade.quantity().as_u64(),
                    &order.contract,
                    tick,
                    snap,
                    out_fills,
                )?;
            }
            // Record the reseed order as resting seed liquidity when any of it
            // rests, so the next snapshot's refresh cancels exactly it. A reseed
            // order fully consumed by a strategy cross rests nothing and is not
            // tracked (there is nothing to cancel).
            if trade_result.match_result.remaining_quantity().as_u64() > 0
                && let ObOrderId::Sequential(seq) = id
            {
                self.resting_seed_ids
                    .entry(order.contract.clone())
                    .or_default()
                    .push(seq);
            }
        }
        Ok(())
    }

    /// Assemble and append one refresh-generated [`Fill`] for the strategy limit
    /// a reseed order just crossed, identified by the trade's **maker** id.
    ///
    /// The trade executes at the resting strategy order's own price (price-time
    /// priority), so `trade_price_ticks` scales straight to the fill price and
    /// the stored `side` / `decision_mid` fix the slippage. The once-per-order
    /// fee rides the strategy order's **first** fill (whether at submit or here),
    /// tracked by `first_fill_done`; the entry is dropped once fully consumed.
    /// A trade whose maker is not a tracked strategy order (e.g. a seeded id) is
    /// ignored.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::ArithmeticOverflow`] on tick→cents or size
    /// narrowing overflow, [`BacktestError::Execution`] when a matched quantity
    /// exceeds the strategy order's tracked resting size (a book/tracking
    /// desync), and propagates fee/slippage errors from [`assemble_fill`].
    #[allow(clippy::too_many_arguments)]
    fn capture_refresh_fill(
        &mut self,
        maker_id: ObOrderId,
        trade_price_ticks: u128,
        trade_qty: u64,
        contract: &ContractKey,
        tick: u64,
        snap: &ChainSnapshot,
        out_fills: &mut Vec<Fill>,
    ) -> Result<(), BacktestError> {
        let ObOrderId::Sequential(maker_seq) = maker_id else {
            return Ok(());
        };
        if !self.resting_strategy.contains_key(&maker_seq) {
            // Not a tracked strategy order — nothing to record.
            return Ok(());
        }
        let exec_price = ticks_to_cents(trade_price_ticks, tick)?;
        let matched = u32::try_from(trade_qty).map_err(|_| BacktestError::ArithmeticOverflow)?;
        let quantity = Quantity::new(matched)?;
        // Read + update the strategy order's state in a scoped borrow so the fee
        // schedule (a disjoint field) is free for `assemble_fill` afterwards.
        let (side, decision_mid, charge, exhausted) = {
            let Some(meta) = self.resting_strategy.get_mut(&maker_seq) else {
                return Ok(());
            };
            let charge = if meta.first_fill_done {
                FeeCharge::LaterFill
            } else {
                FeeCharge::FirstFill
            };
            meta.first_fill_done = true;
            // `checked_sub`, not `saturating_sub`: a match larger than the resting
            // size is a book/tracking desync, and must surface as a typed error
            // rather than being silently clamped to 0 (global_rules.md — never
            // saturating on a counter). `trade_qty <= remaining` always holds while
            // the book and this mirror agree.
            meta.remaining = meta.remaining.checked_sub(trade_qty).ok_or_else(|| {
                BacktestError::Execution(
                    "refresh matched more than the strategy order's resting size \
                     (book/tracking desync)"
                        .to_string(),
                )
            })?;
            (meta.side, meta.decision_mid, charge, meta.remaining == 0)
        };
        let draft = FillDraft {
            ts: snap.ts,
            step: snap.step,
            contract: contract.clone(),
            side,
            quantity,
            price: exec_price,
            decision_mid,
        };
        out_fills.push(assemble_fill(
            draft,
            ExecutionMode::Realistic,
            &self.fees,
            charge,
        )?);
        if exhausted {
            self.resting_strategy.remove(&maker_seq);
        }
        Ok(())
    }

    /// Route one `Submit` intent through its leaf book and append one
    /// [`Fill`] per executed price level to `out_fills`.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Execution`] when a marketable intent's contract
    /// is not quoted (no touch to price off) or the tick is zero,
    /// [`BacktestError::PriceNotTickAligned`] when a limit price is off the tick
    /// grid, [`BacktestError::ArithmeticOverflow`] on cents/size overflow,
    /// [`BacktestError::OrderBook`] when the book rejects the order, and
    /// propagates fee/slippage errors from [`assemble_fill`].
    fn fill_submit(
        &mut self,
        intent: &OrderIntent,
        snap: &ChainSnapshot,
        out_fills: &mut Vec<Fill>,
    ) -> Result<(), BacktestError> {
        let tick = snap.spec.tick_size_cents.value();
        if tick == 0 {
            // Defence in depth: `InstrumentSpec` validates `tick > 0` at ingest.
            return Err(BacktestError::Execution(
                "instrument tick_size_cents is zero at the realistic seam".to_string(),
            ));
        }
        let ob_side = ob_side(intent.side);
        let qty = u64::from(intent.quantity.value());

        // Marketable (`limit = None`) → tick-aligned aggressive limit off the
        // CURRENT snapshot's touch, capped at `marketable_cap_ticks`.
        let limit_cents = match intent.limit {
            Some(limit) => limit,
            None => {
                let quote = snap.quotes.get(&intent.contract).ok_or_else(|| {
                    BacktestError::Execution(format!(
                        "realistic fill: marketable intent for strike {} not quoted at step {}",
                        intent.contract.strike.value(),
                        snap.step.value()
                    ))
                })?;
                marketable_limit_cents(intent.side, quote, tick, self.marketable_cap_ticks)?
            }
        };
        let price_ticks = cents_to_ticks(limit_cents, tick)?;

        // Mint the strategy id BEFORE borrowing the book (disjoint borrows).
        let order_id = self.next_strategy_order_id()?;
        // Submit **GTC** and capture this call's own `TradeResult`. GTC (never
        // IOC) at the seam is deliberate: on an unfillable IOC remainder the
        // upstream `_full`/`_with_result` methods return a typed error and route
        // the fills to the trade listener only, so a partial marketable walk
        // would lose its fills. GTC fills up to the aggressive-limit cap, rests
        // the remainder, and returns Ok with every fill — then IOC semantics are
        // applied explicitly below via `cancel_order` (docs/04 §5.2).
        let trade_result: TradeResult = {
            let book = self.leaf_book(&intent.contract)?;
            book.add_limit_order_full(order_id, ob_side, price_ticks, qty)?
        };
        let remaining = trade_result.match_result.remaining_quantity().as_u64();
        // IOC (marketable, or an explicit IOC limit): discard any resting
        // remainder past the cap — cancelled at end of step, never chased. GTC
        // strategy limits keep their resting remainder (a working order).
        if matches!(intent.tif, TimeInForce::Ioc) && remaining > 0 {
            let book = self.leaf_book(&intent.contract)?;
            // Ok(false) = nothing left to cancel (fully filled); Err propagates.
            let _cancelled = book.cancel_order(order_id)?;
        }

        // One `Fill` per executed price level, in queue-consumption order:
        // fill 0 carries the once-per-order fee, later fills only per-contract.
        for (level, trade) in trade_result
            .match_result
            .trades()
            .as_vec()
            .iter()
            .enumerate()
        {
            let exec_price = ticks_to_cents(trade.price().as_u128(), tick)?;
            let matched = u32::try_from(trade.quantity().as_u64())
                .map_err(|_| BacktestError::ArithmeticOverflow)?;
            let quantity = Quantity::new(matched)?;
            let charge = if level == 0 {
                FeeCharge::FirstFill
            } else {
                FeeCharge::LaterFill
            };
            let draft = FillDraft {
                ts: snap.ts,
                step: snap.step,
                contract: intent.contract.clone(),
                side: intent.side,
                quantity,
                price: exec_price,
                decision_mid: intent.decision_mid,
            };
            out_fills.push(assemble_fill(
                draft,
                ExecutionMode::Realistic,
                &self.fees,
                charge,
            )?);
        }

        // A GTC strategy limit with a resting remainder is a working order the
        // market can move onto between steps: track just enough to assemble its
        // refresh-generated fill (#025) when a later snapshot's reseed crosses it
        // — its trade side, decision mid, resting size, and whether its first
        // fill already carried the per-order fee (`remaining < qty` ⇒ it filled
        // some contracts on submit). IOC remainders were cancelled above and rest
        // nothing, so only GTC working orders are tracked.
        if matches!(intent.tif, TimeInForce::Gtc)
            && remaining > 0
            && let ObOrderId::Sequential(seq) = order_id
        {
            self.resting_strategy.insert(
                seq,
                RestingStrategyOrder {
                    side: intent.side,
                    decision_mid: intent.decision_mid,
                    remaining,
                    first_fill_done: remaining < qty,
                },
            );
        }
        Ok(())
    }
}

impl ExecutionModel for RealisticFill {
    /// Refresh the seeded book from `snap` (e1) and then route the step's
    /// commands (e2), appending every fill — refresh-generated first, then
    /// intent — to `out_fills`, all against `snap`
    /// ([docs/04 §6.1](../../../docs/04-execution-models.md),
    /// [docs/02 §3.2](../../../docs/02-engine-architecture.md)).
    ///
    /// **e1 (refresh) precedes e2 (intents).** The between-snapshot refresh
    /// (#025) cancels the stale seed, reseeds fresh depth from `snap`, and
    /// appends any refresh-generated fills of resting strategy limits the reseed
    /// crosses — **before** the step's own command fills — so a resting order the
    /// market moved onto fills exactly once, in this step, ahead of the new
    /// intents. See [`Self::refresh_books`].
    ///
    /// **`Cancel`/`Replace` remain deferred.** Cancelling or replacing a resting
    /// strategy order needs the domain-`OrderId` → book-`Id` map a later issue
    /// establishes; this routes `Submit` only. The trait's "process
    /// `Cancel`/`Replace` before `Submit`" ordering is honoured **vacuously** —
    /// their effect is empty, so the output is identical whatever the command
    /// order. The refresh never touches strategy orders, so a resting strategy
    /// limit keeps its price-time priority across steps regardless.
    ///
    /// # Errors
    ///
    /// Propagates every error from [`Self::refresh_books`] and
    /// [`Self::fill_submit`].
    fn fill(
        &mut self,
        commands: &[OrderCommand],
        snap: &ChainSnapshot,
        out_fills: &mut Vec<Fill>,
    ) -> Result<(), BacktestError> {
        // The fill→order grouping is rebuilt every call: clear in place (capacity
        // retained ⇒ no steady-state allocation), then record one group per
        // filling `Submit` (e2) below. e1 refresh fills carry no group.
        self.fill_groups.clear();
        // e1: refresh the per-strike books from `snap` (cancel the stale seed,
        // reseed fresh depth) and append any refresh-generated fills BEFORE the
        // intent fills. On the first snapshot this degenerates to the #023
        // initial seed; a no-op for the raw adapter (no profile).
        self.refresh_books(snap, out_fills)?;
        // e2: route the step's commands against the freshly reseeded book. For
        // each `Submit`, record how many fills it produced so the engine can
        // group them back to one order and assign each fill its `fill_seq`
        // (a marketable order walking `n` levels ⇒ one group of `fill_count = n`).
        for (command_index, command) in commands.iter().enumerate() {
            match command {
                OrderCommand::Submit(intent) => {
                    let before = out_fills.len();
                    self.fill_submit(intent, snap, out_fills)?;
                    // `fill_submit` only appends, so `len >= before` always holds;
                    // `checked_sub` (never `saturating_sub` on a counter, per the
                    // repo's Category-E rule) surfaces any future refactor that
                    // shrank the buffer as a typed error rather than a silent 0.
                    let produced = out_fills.len().checked_sub(before).ok_or_else(|| {
                        BacktestError::Execution(
                            "fill buffer shrank during fill_submit".to_string(),
                        )
                    })?;
                    if produced > 0 {
                        let fill_count = u32::try_from(produced)
                            .map_err(|_| BacktestError::ArithmeticOverflow)?;
                        self.fill_groups.push(FillGroup {
                            command_index,
                            fill_count,
                        });
                    }
                }
                // Resting-order cancel/replace lifecycle is deferred (see docs).
                OrderCommand::Cancel(_) | OrderCommand::Replace { .. } => {}
            }
        }
        Ok(())
    }

    #[inline]
    fn fill_groups(&self) -> &[FillGroup] {
        &self.fill_groups
    }

    #[inline]
    fn mode(&self) -> ExecutionMode {
        ExecutionMode::Realistic
    }
}

/// Get (or lazily construct) the leaf [`OptionOrderBook`] for `contract` inside
/// `books` — a free function so the refresh can drive it under a **disjoint
/// field borrow** of `self.books` (the reseed loop needs the rest of `self` at
/// the same time).
///
/// The book is keyed by the contract's identity and constructed with its
/// canonical `contract_id` symbol and 0.18→0.17 [`OptionStyle`] conversion.
///
/// # Errors
///
/// Returns [`BacktestError::Conversion`] when the contract's expiration is
/// unresolved (cannot form a `contract_id`).
fn leaf_book_in<'a>(
    books: &'a mut BTreeMap<ContractKey, OptionOrderBook>,
    contract: &ContractKey,
) -> Result<&'a OptionOrderBook, BacktestError> {
    match books.entry(contract.clone()) {
        Entry::Occupied(e) => Ok(&*e.into_mut()),
        Entry::Vacant(e) => {
            let symbol = contract.to_contract_id()?;
            let book = OptionOrderBook::new(symbol, ob_option_style(contract.style));
            Ok(&*e.insert(book))
        }
    }
}

/// Map an intent's **trade-direction** [`Side`] to the book's [`ObSide`] — the
/// **only** place this mapping lives.
///
/// `OrderIntent.side` is the **trade side**, not the position side: `Long` means
/// *buy*, `Short` means *sell*, for **both** opens and closes. The strategy's
/// `close_command` ([`crate::engine`], `strategy.rs`) already flips a leg's
/// position side to the trade side that flattens it — a long leg is closed by a
/// `Short` (sell) intent, a short leg by a `Long` (buy) intent — and the naive
/// model (the committed reference) interprets `intent.side` exactly this way
/// (`Long` fills up toward the ask, debits cash). So the book side follows
/// `side` alone; the intent's `action` must **not** re-flip it here. Re-flipping
/// on `Close` would **double-flip** a close and cross the wrong side of the book
/// (a buy-to-close crossing the bid), making the realised close price — and its
/// `Fill.slippage` sign ([01 §7.1](../../../docs/01-domain-model.md#71-sign-conventions-truth-table))
/// — dishonest.
///
/// | side (trade) | book side |
/// |--------------|-----------|
/// | `Long`  (buy)  | `Buy`   |
/// | `Short` (sell) | `Sell`  |
#[must_use]
const fn ob_side(side: Side) -> ObSide {
    match side {
        Side::Long => ObSide::Buy,
        Side::Short => ObSide::Sell,
    }
}

/// Convert the crate's 0.18 [`OptionStyle`] to the 0.17 [`ObOptionStyle`] the
/// leaf-book constructor takes (the version shim; see the module docs).
#[must_use]
fn ob_option_style(style: OptionStyle) -> ObOptionStyle {
    match style {
        OptionStyle::Call => ObOptionStyle::Call,
        OptionStyle::Put => ObOptionStyle::Put,
    }
}

/// Scale an integer-cents price to the book's `u128` tick grid:
/// `ticks = price_cents / tick_size_cents`, requiring an **exact** multiple.
///
/// # Errors
///
/// Returns [`BacktestError::PriceNotTickAligned`] when `price` is not a
/// multiple of `tick`, and [`BacktestError::Execution`] when `tick` is zero
/// (defence in depth — the tick is validated `> 0` at ingest).
#[must_use = "the scaled tick price must be submitted"]
fn cents_to_ticks(price: PriceCents, tick: u64) -> Result<u128, BacktestError> {
    let price = price.value();
    if tick == 0 {
        return Err(BacktestError::Execution(
            "tick_size_cents is zero in cents→tick scaling".to_string(),
        ));
    }
    if !price.is_multiple_of(tick) {
        return Err(BacktestError::PriceNotTickAligned { price, tick });
    }
    Ok(u128::from(price / tick))
}

/// Scale a book `u128` tick price back to integer cents:
/// `cents = ticks × tick_size_cents`. The lossless inverse of
/// [`cents_to_ticks`] for any tick the book actually executed at.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] when the product exceeds the
/// `u64` cents range.
#[must_use = "the scaled cents price must be recorded on the fill"]
fn ticks_to_cents(ticks: u128, tick: u64) -> Result<PriceCents, BacktestError> {
    let cents = ticks
        .checked_mul(u128::from(tick))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    let cents = u64::try_from(cents).map_err(|_| BacktestError::ArithmeticOverflow)?;
    Ok(PriceCents::new(cents))
}

/// The tick-aligned aggressive limit for a marketable intent, off the current
/// snapshot's touch, capped at `cap` ticks
/// ([docs/04 §5.2](../../../docs/04-execution-models.md)):
///
/// - **Buy:** `ask + cap × tick` — walks up to `cap` ticks through the touch.
/// - **Sell:** `bid − cap × tick`, **floored at `0`** (a premium cannot be
///   negative — an explicit floor, never a silent `saturating_sub`).
///
/// The touch is tick-aligned at ingest and `cap × tick` is a tick multiple, so
/// the result is tick-aligned by construction.
///
/// # Errors
///
/// Returns [`BacktestError::ArithmeticOverflow`] when `cap × tick` or the buy
/// price exceeds the `u64` cents range.
#[must_use = "the marketable limit price must be submitted"]
fn marketable_limit_cents(
    side: Side,
    quote: &QuoteView,
    tick: u64,
    cap: u32,
) -> Result<PriceCents, BacktestError> {
    let cap_offset = tick
        .checked_mul(u64::from(cap))
        .ok_or(BacktestError::ArithmeticOverflow)?;
    match ob_side(side) {
        ObSide::Buy => {
            let price = quote
                .ask
                .value()
                .checked_add(cap_offset)
                .ok_or(BacktestError::ArithmeticOverflow)?;
            Ok(PriceCents::new(price))
        }
        ObSide::Sell => {
            // Explicit floor at zero (a premium cannot be negative) via `i128`
            // `max(0)` — the repo idiom (see `naive::naive_fill_price`), never a
            // silent `saturating_sub` on money. `bid − cap_offset ∈ [−u64, u64]`
            // fits `i128`; the floored result is in `[0, bid]` and fits `u64`.
            let price = i128::from(quote.bid.value()) - i128::from(cap_offset);
            let price =
                u64::try_from(price.max(0)).map_err(|_| BacktestError::ArithmeticOverflow)?;
            Ok(PriceCents::new(price))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle, Side};
    use rust_decimal_macros::dec;

    use option_chain_orderbook::{OrderId as ObOrderId, Side as ObSide};

    use super::{
        MAKER_ID_BASE, RealisticFill, cents_to_ticks, marketable_limit_cents, ob_side,
        ticks_to_cents,
    };
    use crate::config::{FeeSchedule, LiquidityProfile, TouchSize};
    use crate::domain::{
        ChainSnapshot, ContractKey, ExecutionMode, Fill, InstrumentSpec, OrderCommand, OrderIntent,
        PositionAction, PositionId, PriceCents, Quantity, QuoteView, SimTime, StepIndex,
        TimeInForce, Underlying,
    };
    use crate::error::BacktestError;
    use crate::execution::ExecutionModel;

    const TS0: i64 = 1_750_291_200_000_000_000;
    const TICK: u64 = 5;

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

    fn fees() -> FeeSchedule {
        FeeSchedule {
            per_contract_cents: 65,
            per_order_cents: 100,
        }
    }

    fn quote(bid: u64, ask: u64) -> QuoteView {
        QuoteView {
            contract: contract(),
            bid: PriceCents::new(bid),
            ask: PriceCents::new(ask),
            mid: PriceCents::new((bid + ask) / 2),
            bid_size: qty(10),
            ask_size: qty(10),
            implied_volatility: dec!(0.2),
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
        }
    }

    fn snapshot(bid: u64, ask: u64) -> ChainSnapshot {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(TICK), 100) else {
            panic!("valid spec");
        };
        let mut quotes = BTreeMap::new();
        quotes.insert(contract(), quote(bid, ask));
        ChainSnapshot {
            ts: SimTime::new(TS0),
            step: StepIndex::new(0),
            underlying,
            underlying_price: PriceCents::new(510_000),
            spec,
            quotes,
        }
    }

    fn seq(id: ObOrderId) -> u64 {
        let ObOrderId::Sequential(n) = id else {
            panic!("adapter must mint Id::Sequential, never a random id");
        };
        n
    }

    /// A quote with explicit per-side sizes (the #025 refresh tests vary depth
    /// across snapshots to observe cancel/reseed).
    fn quote_full(bid: u64, ask: u64, bid_size: u32, ask_size: u32) -> QuoteView {
        QuoteView {
            contract: contract(),
            bid: PriceCents::new(bid),
            ask: PriceCents::new(ask),
            mid: PriceCents::new((bid + ask) / 2),
            bid_size: qty(bid_size),
            ask_size: qty(ask_size),
            implied_volatility: dec!(0.2),
            delta: dec!(0.5),
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.1),
        }
    }

    /// A one-contract snapshot at `step` with explicit per-side sizes — the
    /// consecutive-snapshot fixture the #025 refresh tests drive.
    fn snapshot_full(step: u32, bid: u64, ask: u64, bid_size: u32, ask_size: u32) -> ChainSnapshot {
        let Ok(underlying) = Underlying::new("SPX") else {
            panic!("SPX is a valid underlying");
        };
        let Ok(spec) = InstrumentSpec::new(PriceCents::new(TICK), 100) else {
            panic!("valid spec");
        };
        let mut quotes = BTreeMap::new();
        quotes.insert(contract(), quote_full(bid, ask, bid_size, ask_size));
        ChainSnapshot {
            ts: SimTime::new(TS0 + i64::from(step)),
            step: StepIndex::new(step),
            underlying,
            underlying_price: PriceCents::new(510_000),
            spec,
            quotes,
        }
    }

    /// A GTC strategy buy limit at `limit` for `quantity`, decision mid
    /// `decision_mid` — the resting working order the refresh may later cross.
    fn gtc_buy(limit: u64, quantity: u32, decision_mid: u64) -> OrderCommand {
        OrderCommand::Submit(OrderIntent {
            contract: contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: qty(quantity),
            limit: Some(PriceCents::new(limit)),
            tif: TimeInForce::Gtc,
            decision_mid: PriceCents::new(decision_mid),
        })
    }

    /// The canonical single-touch profile (QuotedSize, `L = 0`) the refresh
    /// tests seed with — one resting level per side at the quoted touch.
    fn touch_only_profile() -> LiquidityProfile {
        profile(TouchSize::QuotedSize, 0, dec!(0.5))
    }

    /// A marketable (`limit = None`) submit for `quantity` on `side`/`action`.
    fn marketable(
        side: Side,
        action: PositionAction,
        quantity: u32,
        decision_mid: u64,
    ) -> OrderCommand {
        OrderCommand::Submit(OrderIntent {
            contract: contract(),
            action,
            side,
            quantity: qty(quantity),
            limit: None,
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(decision_mid),
        })
    }

    // --- trade-side → Buy/Sell (both directions) -----------------------------
    //
    // `intent.side` is the TRADE side (Long = buy, Short = sell) for BOTH opens
    // and closes: the strategy's `close_command` flips a leg's position side to
    // the flattening trade side, so the book side follows `side` alone and
    // `action` never re-flips it (a double flip would cross the wrong side).

    #[test]
    fn test_ob_side_long_is_buy() {
        // A buy — open-long OR close-short (both arrive as Side::Long).
        assert_eq!(ob_side(Side::Long), ObSide::Buy);
    }

    #[test]
    fn test_ob_side_short_is_sell() {
        // A sell — open-short OR close-long (both arrive as Side::Short).
        assert_eq!(ob_side(Side::Short), ObSide::Sell);
    }

    // --- cents ↔ tick scaling ------------------------------------------------

    #[test]
    fn test_cents_to_ticks_exact_multiple_ok() {
        assert!(matches!(
            cents_to_ticks(PriceCents::new(500), TICK),
            Ok(100)
        ));
        // lossless round-trip back to cents.
        assert!(matches!(ticks_to_cents(100, TICK), Ok(p) if p.value() == 500));
    }

    #[test]
    fn test_cents_to_ticks_non_aligned_rejected() {
        assert!(matches!(
            cents_to_ticks(PriceCents::new(501), TICK),
            Err(BacktestError::PriceNotTickAligned {
                price: 501,
                tick: 5
            })
        ));
    }

    #[test]
    fn test_ticks_to_cents_overflow_is_typed_error() {
        assert!(matches!(
            ticks_to_cents(u128::MAX, TICK),
            Err(BacktestError::ArithmeticOverflow)
        ));
    }

    // --- seeded-id determinism + disjoint ranges -----------------------------

    #[test]
    fn test_seeded_strategy_ids_are_deterministic_across_same_seed() {
        let mut a = RealisticFill::new(fees(), 10, 42);
        let mut b = RealisticFill::new(fees(), 10, 42);
        let mut ids_a = Vec::new();
        let mut ids_b = Vec::new();
        for _ in 0..5 {
            let (Ok(ia), Ok(ib)) = (a.next_strategy_order_id(), b.next_strategy_order_id()) else {
                panic!("strategy ids must mint");
            };
            ids_a.push(seq(ia));
            ids_b.push(seq(ib));
        }
        // same seed ⇒ identical OrderId sequence.
        assert_eq!(ids_a, ids_b);
        // the sequence is the low range, contiguous from the base.
        assert_eq!(ids_a, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_strategy_and_maker_id_ranges_are_disjoint() {
        let mut model = RealisticFill::new(fees(), 10, 1);
        for _ in 0..8 {
            let (Ok(sid), Ok(mid)) = (model.next_strategy_order_id(), model.next_maker_order_id())
            else {
                panic!("both ranges must mint");
            };
            // strategy ids strictly below the maker base; maker ids at/above it.
            assert!(seq(sid) < MAKER_ID_BASE);
            assert!(seq(mid) >= MAKER_ID_BASE);
        }
    }

    // --- marketable-limit conversion + cap -----------------------------------

    #[test]
    fn test_marketable_limit_buy_is_ask_plus_cap_ticks() {
        // ask 500, cap 10, tick 5 → 500 + 50 = 550.
        let px = marketable_limit_cents(Side::Long, &quote(490, 500), TICK, 10);
        assert!(matches!(px, Ok(p) if p.value() == 550));
    }

    #[test]
    fn test_marketable_limit_sell_is_bid_minus_cap_ticks() {
        // bid 490, cap 10, tick 5 → 490 − 50 = 440.
        let px = marketable_limit_cents(Side::Short, &quote(490, 500), TICK, 10);
        assert!(matches!(px, Ok(p) if p.value() == 440));
    }

    #[test]
    fn test_marketable_limit_sell_floors_at_zero() {
        // bid 30, cap 10, tick 5 → 30 − 50 floors at 0 (premium ≥ 0).
        let px = marketable_limit_cents(Side::Short, &quote(30, 40), TICK, 10);
        assert!(matches!(px, Ok(p) if p.value() == 0));
    }

    // --- multi-level fill_seq (hand-seeded book) -----------------------------

    /// Route a marketable buy through a book hand-seeded with two ask levels
    /// and assert two fills at the two level prices, sharing the contract with
    /// the once-per-order fee only on the first (fill_seq 0), per-contract only
    /// on the second (fill_seq 1).
    #[test]
    fn test_marketable_buy_walks_two_levels_two_fills() {
        let mut model = RealisticFill::new(fees(), 10, 3);
        // ask ladder: 3 @ 500, 2 @ 505 (tick-aligned).
        let seeds = [(PriceCents::new(500), 3u32), (PriceCents::new(505), 2u32)];
        for (price, size) in seeds {
            let seeded =
                model.seed_maker_limit(&contract(), true, price, qty(size), PriceCents::new(TICK));
            assert!(matches!(seeded, Ok(())), "seed must rest");
        }
        // marketable buy for 5 (= 3 + 2) with mid 500; cap 10 reaches 505.
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 5, 500)],
            &snapshot(490, 500),
            &mut out,
        );
        assert!(matches!(result, Ok(())));
        assert_eq!(out.len(), 2, "two levels walked ⇒ two fills");

        let (Some(first), Some(second)) = (out.first(), out.get(1)) else {
            panic!("two fills expected");
        };
        // level 0: best ask 500, qty 3, once-per-order fee: 3×65 + 100 = 295.
        assert_eq!(first.price.value(), 500);
        assert_eq!(first.quantity.value(), 3);
        assert_eq!(first.fees.value(), 295);
        assert_eq!(first.mode, ExecutionMode::Realistic);
        // level 1: next ask 505, qty 2, per-contract only: 2×65 = 130.
        assert_eq!(second.price.value(), 505);
        assert_eq!(second.quantity.value(), 2);
        assert_eq!(second.fees.value(), 130);
        // both fills share the contract identity.
        assert_eq!(first.contract, second.contract);
        assert_eq!(first.side, Side::Long);
    }

    #[test]
    fn test_marketable_cap_stops_walk_and_discards_remainder() {
        let mut model = RealisticFill::new(fees(), 1, 9); // cap = 1 tick
        // three ask levels one tick apart: 500, 505, 510.
        for (price, size) in [(500u64, 3u32), (505, 3), (510, 3)] {
            let seeded = model.seed_maker_limit(
                &contract(),
                true,
                PriceCents::new(price),
                qty(size),
                PriceCents::new(TICK),
            );
            assert!(matches!(seeded, Ok(())));
        }
        // marketable buy for 9; cap 1 ⇒ limit = ask(500) + 1×5 = 505, so only
        // 500 and 505 are reachable, 510 is past the cap; the remainder (qty 3)
        // is discarded (IOC), never chased to 510.
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 9, 500)],
            &snapshot(490, 500),
            &mut out,
        );
        assert!(matches!(result, Ok(())));
        assert_eq!(out.len(), 2, "only two levels within the cap fill");
        assert!(out.iter().all(|f| f.price.value() <= 505));
    }

    // --- #024 honest close routing (trade side, no double flip) --------------

    /// A marketable **close of a short** (trade side `Long` = buy-to-close)
    /// crosses the **ask**, not the bid — the honest side. Under the old
    /// `action`-based double flip it would cross the bid and fill favourably
    /// (dishonest); crossing the ask makes the buy-back adverse, as it must be.
    #[test]
    fn test_close_of_short_crosses_ask_not_bid() {
        let mut model = RealisticFill::new(fees(), 10, 3);
        // asymmetric depth: bid @ 490, ask @ 510 (distinguishable by price).
        let bid = model.seed_maker_limit(
            &contract(),
            false,
            PriceCents::new(490),
            qty(5),
            PriceCents::new(TICK),
        );
        let ask = model.seed_maker_limit(
            &contract(),
            true,
            PriceCents::new(510),
            qty(5),
            PriceCents::new(TICK),
        );
        assert!(matches!((bid, ask), (Ok(()), Ok(()))));
        // Close a short: action = Close, trade side = Long (buy-to-close),
        // marketable, decision_mid = mid 500.
        let close = OrderCommand::Submit(OrderIntent {
            contract: contract(),
            action: PositionAction::Close(PositionId::new(1)),
            side: Side::Long,
            quantity: qty(1),
            limit: None,
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(500),
        });
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(&[close], &snapshot(490, 510), &mut out);
        assert!(matches!(result, Ok(())));
        let Some(fill) = out.first() else {
            panic!("the buy-to-close must fill against the ask");
        };
        // Crossed the ask (510), never the bid (490).
        assert_eq!(fill.price.value(), 510);
        assert_eq!(fill.side, Side::Long);
        // Buying back above mid is adverse: positive slippage.
        assert_eq!(fill.slippage.value(), 10);
    }

    /// A marketable **close of a long** (trade side `Short` = sell-to-close)
    /// crosses the **bid**, not the ask — selling below mid is adverse.
    #[test]
    fn test_close_of_long_crosses_bid_not_ask() {
        let mut model = RealisticFill::new(fees(), 10, 3);
        let bid = model.seed_maker_limit(
            &contract(),
            false,
            PriceCents::new(490),
            qty(5),
            PriceCents::new(TICK),
        );
        let ask = model.seed_maker_limit(
            &contract(),
            true,
            PriceCents::new(510),
            qty(5),
            PriceCents::new(TICK),
        );
        assert!(matches!((bid, ask), (Ok(()), Ok(()))));
        let close = OrderCommand::Submit(OrderIntent {
            contract: contract(),
            action: PositionAction::Close(PositionId::new(1)),
            side: Side::Short,
            quantity: qty(1),
            limit: None,
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(500),
        });
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(&[close], &snapshot(490, 510), &mut out);
        assert!(matches!(result, Ok(())));
        let Some(fill) = out.first() else {
            panic!("the sell-to-close must fill against the bid");
        };
        assert_eq!(fill.price.value(), 490);
        assert_eq!(fill.side, Side::Short);
        // Selling below mid is adverse: positive slippage.
        assert_eq!(fill.slippage.value(), 10);
    }

    // --- #024 queue position (strategy limit behind seeded depth) ------------

    /// A resting strategy limit at a **seeded price level** fills only after the
    /// depth ahead of it (added first) is consumed — same-price time priority,
    /// straight from the leaf book. Seed 3 @ 500 (ahead), rest a strategy sell
    /// of 2 @ 500 behind it, then a marketable buy for 5 walks the queue: the
    /// per-maker trades come back **seeded-3 first, strategy-2 second**, proving
    /// the strategy order queued behind the seeded depth at its level.
    #[test]
    fn test_queue_position_strategy_limit_fills_behind_seeded_depth() {
        let mut model = RealisticFill::new(fees(), 10, 5);
        // Seeded ask of 3 @ 500 — added first, so it is ahead in the queue.
        let seeded = model.seed_maker_limit(
            &contract(),
            true,
            PriceCents::new(500),
            qty(3),
            PriceCents::new(TICK),
        );
        assert!(matches!(seeded, Ok(())));
        // A strategy sell limit of 2 @ 500 rests BEHIND the seeded ask (same
        // price, later time), then a marketable buy for 5 consumes both.
        let rest_then_walk = [
            OrderCommand::Submit(OrderIntent {
                contract: contract(),
                action: PositionAction::Open,
                side: Side::Short, // sell → rests on the ask at 500, no cross
                quantity: qty(2),
                limit: Some(PriceCents::new(500)),
                tif: TimeInForce::Gtc,
                decision_mid: PriceCents::new(500),
            }),
            marketable(Side::Long, PositionAction::Open, 5, 500),
        ];
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(&rest_then_walk, &snapshot(490, 500), &mut out);
        assert!(matches!(result, Ok(())));
        // The resting sell produced no fill; the buy produced two per-maker
        // trades at 500: the seeded 3 first, the strategy 2 second.
        let levels: Vec<(u64, u32)> = out
            .iter()
            .map(|f| (f.price.value(), f.quantity.value()))
            .collect();
        assert_eq!(
            levels,
            vec![(500, 3), (500, 2)],
            "seeded depth (3) fills before the strategy limit (2) at the same price"
        );
    }

    // --- #024 partial / empty / deep fills -----------------------------------

    /// A thin strike fills **less than the intent**: a marketable buy for 5 into
    /// a book with only 2 seeded contracts fills 2 and the remainder is
    /// discarded (IOC) — realistic mode fills partially, unlike naive.
    #[test]
    fn test_thin_strike_partial_fill_matched_less_than_intent() {
        let mut model = RealisticFill::new(fees(), 10, 5);
        let seeded = model.seed_maker_limit(
            &contract(),
            true,
            PriceCents::new(500),
            qty(2),
            PriceCents::new(TICK),
        );
        assert!(matches!(seeded, Ok(())));
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 5, 500)],
            &snapshot(490, 500),
            &mut out,
        );
        assert!(matches!(result, Ok(())));
        assert_eq!(out.len(), 1, "only the seeded depth fills");
        let matched: u32 = out.iter().map(|f| f.quantity.value()).sum();
        assert_eq!(matched, 2, "partial: 2 of 5 filled, 3 discarded (IOC)");
    }

    /// An empty (unseeded) strike leaves a **zero** fill: nothing crosses, so no
    /// `Fill` is appended — realistic mode can decline to fill entirely.
    #[test]
    fn test_empty_strike_yields_zero_fills() {
        let mut model = RealisticFill::new(fees(), 10, 5);
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 5, 500)],
            &snapshot(490, 500),
            &mut out,
        );
        assert!(matches!(result, Ok(())));
        assert!(out.is_empty(), "no seeded depth ⇒ zero fills");
    }

    /// A deep strike fills the **full intent** in a single level: a marketable
    /// buy for 5 into 100 seeded contracts fills 5 at the touch, one fill.
    #[test]
    fn test_deep_strike_fills_full_intent_single_level() {
        let mut model = RealisticFill::new(fees(), 10, 5);
        let seeded = model.seed_maker_limit(
            &contract(),
            true,
            PriceCents::new(500),
            qty(100),
            PriceCents::new(TICK),
        );
        assert!(matches!(seeded, Ok(())));
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 5, 500)],
            &snapshot(490, 500),
            &mut out,
        );
        assert!(matches!(result, Ok(())));
        assert_eq!(out.len(), 1, "deep touch fills the whole intent at once");
        let Some(fill) = out.first() else {
            panic!("one fill expected");
        };
        assert_eq!((fill.price.value(), fill.quantity.value()), (500, 5));
    }

    // --- #024 slippage sign + fee parity -------------------------------------

    /// A marketable buy that walks the ladder records **progressively more
    /// adverse** (larger positive) per-level slippage against the fixed
    /// decision-time mid — the emergent market-impact signal, sign per §7.1.
    #[test]
    fn test_realistic_per_level_slippage_is_progressively_adverse() {
        let mut model = RealisticFill::new(fees(), 10, 5);
        for (price, size) in [(500u64, 2u32), (505, 2), (510, 2)] {
            let seeded = model.seed_maker_limit(
                &contract(),
                true,
                PriceCents::new(price),
                qty(size),
                PriceCents::new(TICK),
            );
            assert!(matches!(seeded, Ok(())));
        }
        // decision_mid fixed at 500 (never re-read post-impact); buy 6 walks all.
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 6, 500)],
            &snapshot(490, 500),
            &mut out,
        );
        assert!(matches!(result, Ok(())));
        let slippage: Vec<i64> = out.iter().map(|f| f.slippage.value()).collect();
        // (500−500)·2 = 0, (505−500)·2 = +10, (510−500)·2 = +20 — non-decreasing,
        // adverse (≥ 0) as each deeper level fills worse than the decision mid.
        assert_eq!(slippage, vec![0, 10, 20]);
        assert!(slippage.windows(2).all(|w| w[1] >= w[0]));
    }

    /// Fees are charged identically to naive for the same filled contracts:
    /// `per_contract` on the fill plus `per_order` once on the first fill.
    #[test]
    fn test_realistic_fees_match_naive_for_same_filled_contracts() {
        let mut model = RealisticFill::new(fees(), 10, 5);
        let seeded = model.seed_maker_limit(
            &contract(),
            true,
            PriceCents::new(500),
            qty(10),
            PriceCents::new(TICK),
        );
        assert!(matches!(seeded, Ok(())));
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 4, 500)],
            &snapshot(490, 500),
            &mut out,
        );
        assert!(matches!(result, Ok(())));
        let Some(fill) = out.first() else {
            panic!("one fill of 4 contracts expected");
        };
        // Same rule as naive: 4 × per_contract(65) + per_order(100) = 360.
        assert_eq!(fill.quantity.value(), 4);
        assert_eq!(fill.fees.value(), 4 * 65 + 100);
    }

    // --- #023 auto-seeding wiring --------------------------------------------

    fn profile(
        touch: TouchSize,
        depth_levels: u32,
        decay: rust_decimal::Decimal,
    ) -> LiquidityProfile {
        LiquidityProfile {
            touch_size: touch,
            depth_levels,
            decay,
        }
    }

    /// `with_liquidity_profile` seeds the ask ladder from the snapshot BEFORE
    /// routing, so a marketable buy walks the auto-seeded depth (no hand-seed).
    #[test]
    fn test_with_liquidity_profile_auto_seeds_ask_ladder_before_routing() {
        // QuotedSize, L=2, r=0.5, ask_size 10 → ask ladder 10@500, 5@505, 2@510.
        let mut model = RealisticFill::with_liquidity_profile(
            fees(),
            10,
            7,
            profile(TouchSize::QuotedSize, 2, dec!(0.5)),
        );
        let mut out: Vec<Fill> = Vec::new();
        // marketable buy for 12 walks 10@500 then 2@505 (cap 10 reaches 510).
        let result = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 12, 500)],
            &snapshot(490, 500),
            &mut out,
        );
        assert!(matches!(result, Ok(())));
        assert_eq!(out.len(), 2, "the buy walks two auto-seeded ask levels");
        let (Some(first), Some(second)) = (out.first(), out.get(1)) else {
            panic!("two fills expected");
        };
        assert_eq!((first.price.value(), first.quantity.value()), (500, 10));
        assert_eq!((second.price.value(), second.quantity.value()), (505, 2));
    }

    /// The raw-adapter constructor (`new`) never auto-seeds: a marketable buy
    /// into an unseeded book fills nothing.
    #[test]
    fn test_new_raw_adapter_does_not_auto_seed() {
        let mut model = RealisticFill::new(fees(), 10, 7);
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 5, 500)],
            &snapshot(490, 500),
            &mut out,
        );
        assert!(matches!(result, Ok(())));
        assert!(out.is_empty(), "no seeded depth ⇒ nothing to fill");
    }

    /// Auto-seeding draws its ids from the seeded-maker range, leaving the
    /// strategy range untouched — the #022 disjointness holds through seeding.
    #[test]
    fn test_auto_seed_consumes_only_maker_ids() {
        let mut model = RealisticFill::with_liquidity_profile(
            fees(),
            10,
            7,
            profile(TouchSize::QuotedSize, 2, dec!(0.5)),
        );
        let mut out: Vec<Fill> = Vec::new();
        // A passive (non-crossing) buy limit: seeding runs, no strategy fill,
        // and the strategy id counter advances by exactly one submit.
        let submit = OrderCommand::Submit(OrderIntent {
            contract: contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: qty(1),
            limit: Some(PriceCents::new(400)), // well below ask, rests, no cross
            tif: TimeInForce::Gtc,
            decision_mid: PriceCents::new(495),
        });
        let result = model.fill(&[submit], &snapshot(490, 500), &mut out);
        assert!(matches!(result, Ok(())));
        // Next maker id is in the high range (seeding consumed several);
        // next strategy id is in the low range and advanced past the base.
        let (Ok(next_maker), Ok(next_strategy)) =
            (model.next_maker_order_id(), model.next_strategy_order_id())
        else {
            panic!("both id ranges must still mint");
        };
        assert!(
            seq(next_maker) > MAKER_ID_BASE,
            "maker ids were consumed by seeding"
        );
        assert!(
            seq(next_strategy) < MAKER_ID_BASE,
            "strategy ids stay in the low range"
        );
    }

    // --- resting limit that does not cross yields no fill --------------------

    #[test]
    fn test_resting_gtc_limit_below_ask_appends_no_fill() {
        let mut model = RealisticFill::new(fees(), 10, 4);
        // an ask rests at 505; a passive buy limit at 500 (< 505) does not cross.
        let seeded = model.seed_maker_limit(
            &contract(),
            true,
            PriceCents::new(505),
            qty(2),
            PriceCents::new(TICK),
        );
        assert!(matches!(seeded, Ok(())));
        let submit = OrderCommand::Submit(OrderIntent {
            contract: contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: qty(2),
            limit: Some(PriceCents::new(500)),
            tif: TimeInForce::Gtc,
            decision_mid: PriceCents::new(502),
        });
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(&[submit], &snapshot(490, 505), &mut out);
        assert!(matches!(result, Ok(())));
        assert!(out.is_empty(), "a non-crossing resting limit does not fill");
    }

    // --- non-tick-aligned strategy limit rejected ----------------------------

    #[test]
    fn test_submit_non_tick_aligned_limit_rejected() {
        let mut model = RealisticFill::new(fees(), 10, 5);
        let submit = OrderCommand::Submit(OrderIntent {
            contract: contract(),
            action: PositionAction::Open,
            side: Side::Long,
            quantity: qty(1),
            limit: Some(PriceCents::new(501)), // not a multiple of tick 5
            tif: TimeInForce::Ioc,
            decision_mid: PriceCents::new(500),
        });
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(&[submit], &snapshot(490, 500), &mut out);
        assert!(matches!(
            result,
            Err(BacktestError::PriceNotTickAligned {
                price: 501,
                tick: 5
            })
        ));
        assert!(out.is_empty());
    }

    // --- marketable intent for an unquoted contract is an error --------------

    #[test]
    fn test_marketable_unquoted_contract_execution_error() {
        let mut model = RealisticFill::new(fees(), 10, 6);
        let mut snap = snapshot(490, 500);
        snap.quotes.clear();
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 1, 500)],
            &snap,
            &mut out,
        );
        assert!(matches!(result, Err(BacktestError::Execution(_))));
        assert!(out.is_empty());
    }

    // --- option_chain_orderbook::Error mapping -------------------------------

    #[test]
    fn test_orderbook_error_maps_to_backtest_orderbook() {
        let err = option_chain_orderbook::Error::NoDataAvailable {
            message: "seeded strike empty".to_string(),
        };
        let mapped = BacktestError::from(err);
        assert!(
            matches!(&mapped, BacktestError::OrderBook(msg) if msg.contains("no data available"))
        );
    }

    // --- mode + Cancel/Replace deferral --------------------------------------

    #[test]
    fn test_realistic_mode_returns_realistic() {
        let model = RealisticFill::new(fees(), 10, 1);
        assert_eq!(model.mode(), ExecutionMode::Realistic);
    }

    #[test]
    fn test_cancel_and_replace_append_no_fills_in_issue_22() {
        use crate::domain::OrderId;
        let mut model = RealisticFill::new(fees(), 10, 1);
        let commands = [
            OrderCommand::Cancel(OrderId::new(1)),
            OrderCommand::Replace {
                order_id: OrderId::new(2),
                replacement: OrderIntent {
                    contract: contract(),
                    action: PositionAction::Close(PositionId::new(9)),
                    side: Side::Short,
                    quantity: qty(1),
                    limit: Some(PriceCents::new(500)),
                    tif: TimeInForce::Gtc,
                    decision_mid: PriceCents::new(500),
                },
            },
        ];
        let mut out: Vec<Fill> = Vec::new();
        let result = model.fill(&commands, &snapshot(490, 500), &mut out);
        assert!(matches!(result, Ok(())));
        assert!(
            out.is_empty(),
            "cancel/replace remain deferred; they append no fills"
        );
    }

    // --- #025 between-snapshot book refresh ----------------------------------

    /// Seeded depth from `S_{n-1}` never leaks into `S_n`: the refresh cancels
    /// every stale seeded-maker order before reseeding. `snap1` seeds a DEEP ask
    /// at 505; `snap2` a THIN ask (3) at 500. A marketable buy for 10 at `snap2`
    /// (cap reaches 505) must fill ONLY the 3 @ 500 — were the stale 505 depth
    /// still resting it would fill 7 more there.
    #[test]
    fn test_refresh_cancels_stale_seed_no_leak_into_next_snapshot() {
        let mut model = RealisticFill::with_liquidity_profile(fees(), 10, 5, touch_only_profile());
        let mut out: Vec<Fill> = Vec::new();
        // snap1: ask 505, deep ask_size 100 — seeded, no order routed.
        let r1 = model.fill(&[], &snapshot_full(0, 495, 505, 100, 100), &mut out);
        assert!(matches!(r1, Ok(())));
        assert!(out.is_empty(), "seeding snap1 alone produces no fill");
        // snap2: ask 500, thin ask_size 3. A marketable buy for 10.
        out.clear();
        let r2 = model.fill(
            &[marketable(Side::Long, PositionAction::Open, 10, 500)],
            &snapshot_full(1, 490, 500, 3, 3),
            &mut out,
        );
        assert!(matches!(r2, Ok(())));
        let matched: u32 = out.iter().map(|f| f.quantity.value()).sum();
        assert_eq!(
            matched, 3,
            "only snap2's 3 @ 500 fills; every snap1 seeded order was cancelled"
        );
        assert_eq!(out.len(), 1, "one level — the stale 505 depth did not leak");
        let Some(fill) = out.first() else {
            panic!("one fill expected");
        };
        assert_eq!(fill.price.value(), 500);
    }

    /// A resting strategy limit fills **exactly** when a later snapshot's quotes
    /// cross it — via a refresh-generated fill on reseed — and not before. It
    /// fills at its own limit price (aged price-time priority), tagged to the
    /// crossing step, with the stored decision mid fixing the slippage sign.
    #[test]
    fn test_resting_strategy_limit_fills_exactly_when_later_snapshot_crosses() {
        let mut model = RealisticFill::with_liquidity_profile(fees(), 10, 5, touch_only_profile());
        // snap1: ask 600 (wide). A GTC strategy buy at 500 rests below it — no
        // cross, so no fill at snap1.
        let mut out: Vec<Fill> = Vec::new();
        let r1 = model.fill(
            &[gtc_buy(500, 1, 550)],
            &snapshot_full(0, 490, 600, 20, 20),
            &mut out,
        );
        assert!(matches!(r1, Ok(())));
        assert!(out.is_empty(), "the buy at 500 does not cross the 600 ask");
        // snap2: ask moves down to 500. The reseed ask touch at 500 crosses the
        // resting strategy buy — a refresh-generated fill at the buy's price.
        out.clear();
        let r2 = model.fill(&[], &snapshot_full(1, 490, 500, 20, 20), &mut out);
        assert!(matches!(r2, Ok(())));
        assert_eq!(
            out.len(),
            1,
            "exactly one refresh fill when the market crosses"
        );
        let Some(fill) = out.first() else {
            panic!("one refresh fill expected");
        };
        assert_eq!(fill.side, Side::Long);
        assert_eq!(
            fill.price.value(),
            500,
            "fills at the resting limit's price"
        );
        assert_eq!(fill.quantity.value(), 1);
        assert_eq!(fill.step.value(), 1, "a step-1 fill, against snap2");
        assert_eq!(fill.mode, ExecutionMode::Realistic);
        // decision_mid 550, buy at 500 (below mid) ⇒ favourable: negative slippage.
        assert_eq!(fill.slippage.value(), -50);
    }

    /// The refresh does **not** cancel or reinsert strategy orders: an uncrossed
    /// resting strategy limit survives a full cancel-seed → reseed cycle and
    /// still fills only when a still-later snapshot finally crosses it.
    #[test]
    fn test_refresh_leaves_uncrossed_strategy_order_resting() {
        let mut model = RealisticFill::with_liquidity_profile(fees(), 10, 5, touch_only_profile());
        let mut out: Vec<Fill> = Vec::new();
        // snap0: rest a strategy buy at 500 under a wide 600 ask.
        let r0 = model.fill(
            &[gtc_buy(500, 1, 560)],
            &snapshot_full(0, 490, 600, 20, 20),
            &mut out,
        );
        assert!(matches!(r0, Ok(())));
        assert!(out.is_empty());
        // snap1: ask still wide (580). The refresh cancels+reseeds the seed but
        // must NOT touch the strategy order, so it still does not fill.
        out.clear();
        let r1 = model.fill(&[], &snapshot_full(1, 490, 580, 20, 20), &mut out);
        assert!(matches!(r1, Ok(())));
        assert!(
            out.is_empty(),
            "the uncrossed strategy order survives the refresh unfilled"
        );
        // snap2: ask finally crosses at 500 — the surviving order fills now.
        out.clear();
        let r2 = model.fill(&[], &snapshot_full(2, 490, 500, 20, 20), &mut out);
        assert!(matches!(r2, Ok(())));
        assert_eq!(out.len(), 1);
        let Some(fill) = out.first() else {
            panic!("one fill expected");
        };
        assert_eq!(
            (fill.side, fill.price.value(), fill.step.value()),
            (Side::Long, 500, 2)
        );
    }

    /// Refresh-generated fills (e1) are appended **before** the step's intent
    /// fills (e2). At `snap2` the reseed crosses a resting buy (e1) and a
    /// marketable sell intent crosses the reseeded bid (e2); the refresh fill
    /// must come first in `out_fills`.
    #[test]
    fn test_refresh_fill_precedes_intent_fill_in_out_fills() {
        let mut model = RealisticFill::with_liquidity_profile(fees(), 10, 5, touch_only_profile());
        let mut out: Vec<Fill> = Vec::new();
        // snap1: rest a strategy buy at 500 under a wide 600 ask.
        let r1 = model.fill(
            &[gtc_buy(500, 1, 550)],
            &snapshot_full(0, 490, 600, 20, 20),
            &mut out,
        );
        assert!(matches!(r1, Ok(())));
        assert!(out.is_empty());
        // snap2: ask crosses the resting buy (e1) AND route a marketable sell (e2).
        out.clear();
        let r2 = model.fill(
            &[marketable(Side::Short, PositionAction::Open, 2, 500)],
            &snapshot_full(1, 490, 500, 20, 20),
            &mut out,
        );
        assert!(matches!(r2, Ok(())));
        assert_eq!(
            out.len(),
            2,
            "one refresh fill (e1) then one intent fill (e2)"
        );
        let (Some(first), Some(second)) = (out.first(), out.get(1)) else {
            panic!("two fills expected");
        };
        // e1: the refresh-generated fill of the resting buy, at its price 500.
        assert_eq!((first.side, first.price.value()), (Side::Long, 500));
        // e2: the marketable sell crosses snap2's reseeded bid at 490.
        assert_eq!((second.side, second.price.value()), (Side::Short, 490));
    }

    /// Two identical multi-snapshot refresh runs produce byte-identical fills —
    /// the refresh (cancel stale seed → reseed in fixed order → capture) is fully
    /// deterministic, seeded ids and all.
    #[test]
    fn test_refresh_is_deterministic_across_two_multi_snapshot_runs() {
        let run = || -> Vec<(u32, u64, u32, i64)> {
            let mut model =
                RealisticFill::with_liquidity_profile(fees(), 10, 5, touch_only_profile());
            let mut out: Vec<Fill> = Vec::new();
            let snaps = [
                snapshot_full(0, 490, 600, 20, 20),
                snapshot_full(1, 490, 540, 20, 20),
                snapshot_full(2, 490, 500, 20, 20),
            ];
            let cmds = [gtc_buy(500, 1, 550)];
            for (i, snap) in snaps.iter().enumerate() {
                out.clear();
                let step_cmds: &[OrderCommand] = if i == 0 { &cmds } else { &[] };
                match model.fill(step_cmds, snap, &mut out) {
                    Ok(()) => {}
                    Err(e) => panic!("the refresh run must succeed: {e}"),
                }
            }
            out.iter()
                .map(|f| {
                    (
                        f.step.value(),
                        f.price.value(),
                        f.quantity.value(),
                        f.slippage.value(),
                    )
                })
                .collect()
        };
        assert_eq!(
            run(),
            run(),
            "the same tape yields byte-identical refresh fills"
        );
    }
}

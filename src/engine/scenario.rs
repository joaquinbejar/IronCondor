//! Scenario data types (batch orchestration is deferred to v0.5).
//!
//! [`ScenarioType`] and [`ScenarioParams`] are migrated from OptionStratBacktest,
//! which shipped them as **data only** — no generator. This module keeps that
//! contract: the types describe a batch of independent single-threaded runs, but
//! the generator that fans them out (deterministic `child_seed(base_seed, i)`
//! derivation, per-run bundles, a parent index) is new work at v0.5, issue #46
//! ([docs/02 §9](../../../docs/02-engine-architecture.md#9-scenario-orchestration)).
//!
//! The field shape is reconciled to the pinned redesign in
//! [docs/02 §9](../../../docs/02-engine-architecture.md#9-scenario-orchestration)
//! (`kind` / `base_seed` / `count` / `sweep`), not the original
//! `num_scenarios` / `time_steps` / `stress_factor` / `historical_events` /
//! `custom_paths` shape, which is superseded.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::BacktestConfig;
use crate::error::BacktestError;

/// The kind of scenario batch to run.
///
/// A small, stable, boundary-exposed enum — `#[repr(u8)]` per the ruleset.
/// Migrated verbatim from OptionStratBacktest's `ScenarioType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
#[non_exhaustive]
pub enum ScenarioType {
    /// Standard Monte Carlo sweep over independently seeded runs.
    MonteCarlo = 0,
    /// A stress grid exercising extreme parameter moves.
    StressTest = 1,
    /// Runs replaying historical market episodes.
    Historical = 2,
    /// A user-defined batch.
    Custom = 3,
}

/// A single configuration override applied to the base config to build one run
/// of a sweep ([`expand`]).
///
/// One override describes a **sweep dimension**; the generator replicates the
/// base config once per override × `count` and applies the override to each
/// replicate ([docs/02 §9](../../../docs/02-engine-architecture.md#9-scenario-orchestration)).
/// The v0.5 generator applies:
///
/// - `seed` — an explicit engine-seed override. `None` means the run uses the
///   batch's derived [`child_seed`]`(base_seed, i)`; `Some(s)` pins `s`
///   instead. The **data** seed (simulator sources) is always
///   [`child_data_seed`]`(base_seed, i)` regardless of this field, so an engine
///   re-seed never perturbs which synthetic tape a run requests.
/// - `walk_volatility_factor` / `walk_drift_delta` — the
///   [`ScenarioType::StressTest`] parameter-shock knobs, applied **only** to a
///   [`crate::data::DataSourceSpec::Simulator`] source's walk (see
///   [`expand`]); ignored for file feeds, which carry fixed recorded data with
///   no walk to shift.
///
/// The shock knobs are `Decimal` (the repo money/param numeric type); the
/// generator converts them to the simulator wire's `f64` walk parameters at the
/// single conversion point ([`expand`]).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigOverride {
    /// Optional engine-seed override for this run. `None` means the run uses
    /// the batch's derived [`child_seed`]`(base_seed, i)`.
    pub seed: Option<u64>,
    /// [`ScenarioType::StressTest`] shock: a **multiplicative** factor applied
    /// to the simulator walk's `volatility` (and the session-level
    /// `volatility`). Must be finite and `≥ 0`. `None` leaves volatility
    /// unshocked. Ignored for non-simulator sources.
    #[serde(default)]
    pub walk_volatility_factor: Option<Decimal>,
    /// [`ScenarioType::StressTest`] shock: an **additive** delta applied to the
    /// simulator walk's `drift`. Must be finite. `None` leaves drift unshocked.
    /// Ignored for non-simulator sources.
    #[serde(default)]
    pub walk_drift_delta: Option<Decimal>,
}

/// Parameters describing a batch of independent backtest runs.
///
/// Reconciled to the pinned shape in
/// [docs/02 §9](../../../docs/02-engine-architecture.md#9-scenario-orchestration).
/// Data only — no run logic here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioParams {
    /// Which kind of batch this describes.
    pub kind: ScenarioType,
    /// The root seed; run `i` derives `child_seed(base_seed, i)` (v0.5), a
    /// fixed hash independent of thread-pool scheduling order.
    pub base_seed: u64,
    /// Number of runs in the batch.
    pub count: u32,
    /// Per-run config overrides (the sweep). Empty for a plain batch.
    #[serde(default)]
    pub sweep: Vec<ConfigOverride>,
}

// ---------------------------------------------------------------------------
// Deterministic child-seed derivation (a fixed hash, never scheduling-dependent)
// ---------------------------------------------------------------------------

/// Domain tag folded into every **engine** child-seed hash, so an engine seed
/// can never collide with a data seed (different tag) or with the tape hash
/// (a different tag again). The `v1` suffix pins the scheme: changing it would
/// change every derived seed and is a deliberate, versioned break — the
/// pinned-value tests (`test_child_seed_is_pinned_and_stable`) catch a silent
/// drift.
const ENGINE_SEED_TAG: &[u8] = b"ironcondor.scenario.child_seed.v1\0";

/// Domain tag folded into every **data** child-seed hash (the per-run simulator
/// walk seed). Distinct from [`ENGINE_SEED_TAG`], so the engine and data seeds
/// for the same `(base_seed, index)` differ yet are each stable.
const DATA_SEED_TAG: &[u8] = b"ironcondor.scenario.child_data_seed.v1\0";

/// Fold a domain tag plus `(base_seed, index)` into one `u64` via SHA-256 —
/// the shared, allocation-free core of [`child_seed`] and [`child_data_seed`].
///
/// A cryptographic hash is the fixed-hash choice: it needs **no** wrapping
/// arithmetic (unlike a splitmix mixer, which the repo arithmetic rule bans),
/// it can never silently drift (SHA-256 is frozen), and it matches the tape
/// identity's existing `sha2` layout. The seed is the first eight digest bytes
/// read little-endian — a pure function of `(tag, base_seed, index)`, so a
/// batch derives the identical per-run seeds regardless of thread-pool
/// scheduling order.
#[must_use]
fn seed_from(tag: &[u8], base_seed: u64, index: u32) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(tag);
    hasher.update(base_seed.to_le_bytes());
    hasher.update(index.to_le_bytes());
    let digest = hasher.finalize();
    // First eight digest bytes, little-endian. `zip` bounds the copy at eight
    // without any indexing, so this is total and panic-free.
    let mut bytes = [0_u8; 8];
    for (dst, src) in bytes.iter_mut().zip(digest.iter()) {
        *dst = *src;
    }
    u64::from_le_bytes(bytes)
}

/// The **engine** seed for run `index` of a batch rooted at `base_seed` — a
/// fixed hash, **never** a wall-clock or arrival-order counter, so a batch
/// reproduces the same per-run engine seeds regardless of how the thread pool
/// schedules it ([docs/02 §9](../../../docs/02-engine-architecture.md#9-scenario-orchestration)).
///
/// This is the value written into each run's `config.seed` when the run's
/// [`ConfigOverride::seed`] is `None`.
#[must_use]
pub fn child_seed(base_seed: u64, index: u32) -> u64 {
    seed_from(ENGINE_SEED_TAG, base_seed, index)
}

/// The **data** seed for run `index` of a batch rooted at `base_seed` — the
/// per-run simulator walk seed sent as `CreateSessionRequest::seed`.
///
/// Derived like [`child_seed`] but under a distinct domain tag, so the engine
/// and data seeds of one run differ while each stays a fixed, reproducible
/// function of `(base_seed, index)`. Recorded in the batch parent index for
/// provenance; the honest limit is that upstream v0.1.0 still wall-clock-stamps
/// each snapshot timestamp, so two same-`data_seed` synthetic runs materialise
/// **distinct** tapes and `run_id`s today
/// ([docs/03 §6](../../../docs/03-data-layer.md#6-synthetic-feed--optionchain-simulator)).
#[must_use]
pub fn child_data_seed(base_seed: u64, index: u32) -> u64 {
    seed_from(DATA_SEED_TAG, base_seed, index)
}

// ---------------------------------------------------------------------------
// Walk-type presets → CreateSessionRequest.method (the tagged wire enum)
// ---------------------------------------------------------------------------

/// A curated selection of the simulator's random-walk types, mirroring
/// `optionstratlib::simulation::WalkType`
/// ([docs/specs/optionstratlib.md §8](../../../docs/specs/optionstratlib.md#8-simulation-walks-and-exit-policies)).
///
/// `ironcondor` **never** reimplements a walk — the server evolves it. This
/// enum only *selects and parameterises* one, building the externally-tagged
/// JSON the simulator's `method` field carries
/// (`{"GeometricBrownian": {"dt": …, "drift": …, "volatility": …}}`), verified
/// field-for-field against the upstream `ApiWalkType` at v0.1.0 (commit
/// `63eac033efea`, read 2026-07-17). It is a convenience over hand-writing that
/// JSON; a caller may still supply a raw `method` `Value` directly. The set is
/// curated (the parameter-light, commonly-swept walks); the `Custom`,
/// `Telegraph`, `LogReturns`, and `Historical` variants are intentionally
/// omitted (a raw `method` covers them).
///
/// Parameters are `Decimal` (the repo numeric type); [`Self::to_method_json`]
/// converts each to the wire's `f64` at the single conversion point.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WalkPreset {
    /// Standard Brownian motion.
    Brownian {
        /// Time step (fraction of a year).
        dt: Decimal,
        /// Drift (expected return).
        drift: Decimal,
        /// Annualised volatility.
        volatility: Decimal,
    },
    /// Geometric Brownian motion (log-normal increments).
    GeometricBrownian {
        /// Time step (fraction of a year).
        dt: Decimal,
        /// Drift (expected return).
        drift: Decimal,
        /// Annualised volatility.
        volatility: Decimal,
    },
    /// Mean-reverting (Ornstein–Uhlenbeck) process.
    MeanReverting {
        /// Time step (fraction of a year).
        dt: Decimal,
        /// Annualised volatility.
        volatility: Decimal,
        /// Mean-reversion speed.
        speed: Decimal,
        /// Long-term equilibrium level.
        mean: Decimal,
    },
    /// Jump-diffusion (continuous diffusion plus Poisson jumps).
    JumpDiffusion {
        /// Time step (fraction of a year).
        dt: Decimal,
        /// Drift of the continuous part.
        drift: Decimal,
        /// Volatility of the continuous part.
        volatility: Decimal,
        /// Annual jump intensity.
        intensity: Decimal,
        /// Mean jump size.
        jump_mean: Decimal,
        /// Jump-size volatility.
        jump_volatility: Decimal,
    },
    /// GARCH (time-varying volatility).
    Garch {
        /// Time step (fraction of a year).
        dt: Decimal,
        /// Drift (expected return).
        drift: Decimal,
        /// Initial volatility level.
        volatility: Decimal,
        /// GARCH alpha (impact of past observations).
        alpha: Decimal,
        /// GARCH beta (volatility persistence).
        beta: Decimal,
    },
    /// Heston (stochastic volatility).
    Heston {
        /// Time step (fraction of a year).
        dt: Decimal,
        /// Drift (expected return).
        drift: Decimal,
        /// Initial volatility level.
        volatility: Decimal,
        /// Mean-reversion speed of variance.
        kappa: Decimal,
        /// Long-term variance.
        theta: Decimal,
        /// Volatility of variance.
        xi: Decimal,
        /// Price/vol correlation.
        rho: Decimal,
    },
}

impl WalkPreset {
    /// The upstream `ApiWalkType` variant name — the outer key of the
    /// externally-tagged `method` object.
    #[must_use]
    pub const fn variant_name(&self) -> &'static str {
        match self {
            Self::Brownian { .. } => "Brownian",
            Self::GeometricBrownian { .. } => "GeometricBrownian",
            Self::MeanReverting { .. } => "MeanReverting",
            Self::JumpDiffusion { .. } => "JumpDiffusion",
            Self::Garch { .. } => "Garch",
            Self::Heston { .. } => "Heston",
        }
    }

    /// Build the externally-tagged `method` JSON the simulator's
    /// `CreateSessionRequest.method` field carries, with every parameter as a
    /// finite `f64` number (the wire type), verified against the upstream
    /// `ApiWalkType` field names.
    ///
    /// # Errors
    ///
    /// [`BacktestError::Conversion`] if any `Decimal` parameter is not
    /// representable as a finite `f64` (e.g. an out-of-range magnitude).
    #[must_use = "the built walk method must be used"]
    pub fn to_method_json(&self) -> Result<serde_json::Value, BacktestError> {
        let params = match self {
            Self::Brownian {
                dt,
                drift,
                volatility,
            }
            | Self::GeometricBrownian {
                dt,
                drift,
                volatility,
            } => serde_json::json!({
                "dt": num(*dt)?,
                "drift": num(*drift)?,
                "volatility": num(*volatility)?,
            }),
            Self::MeanReverting {
                dt,
                volatility,
                speed,
                mean,
            } => serde_json::json!({
                "dt": num(*dt)?,
                "volatility": num(*volatility)?,
                "speed": num(*speed)?,
                "mean": num(*mean)?,
            }),
            Self::JumpDiffusion {
                dt,
                drift,
                volatility,
                intensity,
                jump_mean,
                jump_volatility,
            } => serde_json::json!({
                "dt": num(*dt)?,
                "drift": num(*drift)?,
                "volatility": num(*volatility)?,
                "intensity": num(*intensity)?,
                "jump_mean": num(*jump_mean)?,
                "jump_volatility": num(*jump_volatility)?,
            }),
            Self::Garch {
                dt,
                drift,
                volatility,
                alpha,
                beta,
            } => serde_json::json!({
                "dt": num(*dt)?,
                "drift": num(*drift)?,
                "volatility": num(*volatility)?,
                "alpha": num(*alpha)?,
                "beta": num(*beta)?,
            }),
            Self::Heston {
                dt,
                drift,
                volatility,
                kappa,
                theta,
                xi,
                rho,
            } => serde_json::json!({
                "dt": num(*dt)?,
                "drift": num(*drift)?,
                "volatility": num(*volatility)?,
                "kappa": num(*kappa)?,
                "theta": num(*theta)?,
                "xi": num(*xi)?,
                "rho": num(*rho)?,
            }),
        };
        let mut method = serde_json::Map::new();
        method.insert(self.variant_name().to_string(), params);
        Ok(serde_json::Value::Object(method))
    }
}

/// Convert a `Decimal` walk parameter to a finite `f64` JSON number — the one
/// place a walk parameter crosses into the simulator's `f64` wire domain.
fn num(value: Decimal) -> Result<serde_json::Value, BacktestError> {
    let as_f64 = value.to_f64().filter(|v| v.is_finite()).ok_or_else(|| {
        BacktestError::Conversion(format!("walk parameter {value} is not a finite f64"))
    })?;
    let number = serde_json::Number::from_f64(as_f64).ok_or_else(|| {
        BacktestError::Conversion(format!("walk parameter {value} is not a finite f64 number"))
    })?;
    Ok(serde_json::Value::Number(number))
}

// ---------------------------------------------------------------------------
// The generator: ScenarioParams × base config → N independent BacktestConfigs
// ---------------------------------------------------------------------------

/// The hard ceiling on the number of runs one [`expand`] may produce. A batch
/// crossing it is a typed [`BacktestError::Config`] **before** any allocation is
/// sized from the (untrusted) `count × sweep.len()` product — so a
/// `count = u32::MAX` sweep cannot abort the process on a giant `with_capacity`.
/// One million independent runs is already far beyond any realistic Monte-Carlo
/// or stress grid. Internal (not part of the public surface); the ceiling is
/// surfaced to callers through the error message, not as an API constant.
const MAX_RUNS: usize = 1_000_000;

/// Expand a [`ScenarioParams`] batch over `base` into `N` independent
/// [`BacktestConfig`]s — the generator behind the migrated scenario data types
/// ([docs/02 §9](../../../docs/02-engine-architecture.md#9-scenario-orchestration)).
///
/// **Cardinality.** With a non-empty `sweep`, `N = sweep.len() × count`: each
/// override is replicated `count` times (the outer loop is the sweep, the inner
/// loop the replicates), so a stress grid crossed with a Monte-Carlo replica
/// count is one flat batch. An **empty** `sweep` is treated as one implicit
/// no-op override, giving `N = count` plain Monte-Carlo replicas. The global run
/// index is positional (`override_idx × count + replicate`) — a pure function of
/// position, never of arrival order — so every derived seed is
/// scheduling-independent.
///
/// **Per-run derivation** for run `i` (config `out[i]`):
///
/// - `config.seed = override.seed.unwrap_or(child_seed(base_seed, i))` — the
///   deterministic engine seed, or the override's explicit pin.
/// - For a [`crate::data::DataSourceSpec::Simulator`] source (feature
///   `simulator`): the walk seed `session.seed` / `data_seed` is set to
///   [`child_data_seed`]`(base_seed, i)`, `tape_sha256` is cleared (re-pinned at
///   materialisation), and any `walk_volatility_factor` / `walk_drift_delta`
///   shock is applied to the walk (and session volatility). File-feed sources
///   ignore the shock knobs (no walk to shift).
/// - Operational fields (`output_dir`, `overwrite`) are inherited from `base`
///   unchanged — they are excluded from the `run_id` hash, so all runs share the
///   batch output directory and are disambiguated by their distinct `run_id`s.
///
/// The result is **data only** — no run executes here. The batch runner
/// ([`crate::batch::run_scenario_batch`]) is the composition root that opens a
/// feed per config and drives the run, keeping the engine layer analytics-free.
///
/// # Errors
///
/// - [`BacktestError::Config`] if the batch would expand to more than the
///   internal `MAX_RUNS` cap (1,000,000 runs), if a
///   non-[`ScenarioType::StressTest`] batch carries a walk shock
///   (`walk_volatility_factor` / `walk_drift_delta`), or if an explicit
///   [`ConfigOverride::seed`] is combined with `count > 1` (which would collide
///   run ids across replicates).
/// - [`BacktestError::ArithmeticOverflow`] if the run count overflows `u32`.
/// - [`BacktestError::Conversion`] if a stress shock is not a finite `f64` or a
///   volatility factor is negative.
#[must_use = "the expanded configs must be used"]
pub fn expand(
    params: &ScenarioParams,
    base: &BacktestConfig,
) -> Result<Vec<BacktestConfig>, BacktestError> {
    // An empty sweep is one implicit no-op override (plain Monte-Carlo replicas).
    let default_override = ConfigOverride::default();
    let overrides: &[ConfigOverride] = if params.sweep.is_empty() {
        std::slice::from_ref(&default_override)
    } else {
        &params.sweep
    };

    // The walk shocks are StressTest-only; a non-StressTest batch carrying them
    // is a misconfiguration, not a silent no-op (they would otherwise apply
    // regardless of `kind`).
    if params.kind != ScenarioType::StressTest
        && overrides
            .iter()
            .any(|over| over.walk_volatility_factor.is_some() || over.walk_drift_delta.is_some())
    {
        return Err(BacktestError::Config(format!(
            "walk_volatility_factor / walk_drift_delta are StressTest-only shocks, but the \
             scenario kind is {:?}",
            params.kind
        )));
    }

    // An explicit per-override seed with count > 1 makes every replicate of that
    // override share a run_id (identical config ⇒ identical run_id for file
    // feeds), so the replicates would race for the same bundle directory. Reject
    // it explicitly.
    if params.count > 1 && overrides.iter().any(|over| over.seed.is_some()) {
        return Err(BacktestError::Config(
            "an explicit ConfigOverride.seed with count > 1 makes every replicate share a \
             run_id (a concurrent bundle-directory race); use base_seed derivation \
             (seed: None) for replicated runs, or set count = 1"
                .to_string(),
        ));
    }

    let count = usize::try_from(params.count).map_err(|_| BacktestError::ArithmeticOverflow)?;
    let total = overrides
        .len()
        .checked_mul(count)
        .ok_or(BacktestError::ArithmeticOverflow)?;

    // Hard run-count ceiling BEFORE the allocation is sized from the (untrusted)
    // `count × sweep.len()` product, so a huge batch is a typed error rather than
    // an aborting `with_capacity`.
    if total > MAX_RUNS {
        return Err(BacktestError::Config(format!(
            "scenario batch expands to {total} runs, exceeding the hard cap of {MAX_RUNS}"
        )));
    }

    let mut out = Vec::with_capacity(total);

    let mut index: u32 = 0;
    for over in overrides {
        for _replicate in 0..count {
            out.push(derive_config(base, params.base_seed, index, over)?);
            index = index
                .checked_add(1)
                .ok_or(BacktestError::ArithmeticOverflow)?;
        }
    }
    Ok(out)
}

/// Build run `index`'s config from `base` + `over`: set the engine seed, and —
/// for a simulator source — the derived data seed plus any walk shock.
fn derive_config(
    base: &BacktestConfig,
    base_seed: u64,
    index: u32,
    over: &ConfigOverride,
) -> Result<BacktestConfig, BacktestError> {
    let mut config = base.clone();
    config.seed = over.seed.unwrap_or_else(|| child_seed(base_seed, index));

    #[cfg(feature = "simulator")]
    if let crate::data::DataSourceSpec::Simulator(spec) = &mut config.data_source {
        let data_seed = child_data_seed(base_seed, index);
        spec.data_seed = data_seed;
        spec.session.seed = Some(data_seed);
        // The tape sha is pinned at materialisation, per run — clear any
        // inherited value so the provenance is not carried across runs.
        spec.tape_sha256 = String::new();
        apply_walk_shocks(&mut spec.session, over)?;
    }
    // The shock knobs are inert for non-simulator sources (documented on
    // `ConfigOverride`); reference them so the field is not "unused" on a
    // non-simulator build.
    #[cfg(not(feature = "simulator"))]
    let _ = over;

    Ok(config)
}

/// Apply the [`ScenarioType::StressTest`] shock knobs to a simulator walk: a
/// multiplicative volatility factor (on both the session `volatility` and the
/// walk method's `volatility`) and an additive drift delta (on the walk
/// method's `drift`). Best-effort on the method `Value`: a walk variant lacking
/// a `volatility` / `drift` field is left unshocked, never an error.
#[cfg(feature = "simulator")]
fn apply_walk_shocks(
    session: &mut crate::data::simulator::CreateSessionRequest,
    over: &ConfigOverride,
) -> Result<(), BacktestError> {
    if let Some(factor) = over.walk_volatility_factor {
        let factor = shock_scalar(factor)?;
        if factor < 0.0 {
            return Err(BacktestError::Conversion(format!(
                "walk_volatility_factor must be >= 0, got {factor}"
            )));
        }
        session.volatility = finite(session.volatility * factor, "session volatility")?;
        shock_method_field(&mut session.method, "volatility", |v| v * factor)?;
    }
    if let Some(delta) = over.walk_drift_delta {
        let delta = shock_scalar(delta)?;
        shock_method_field(&mut session.method, "drift", |v| v + delta)?;
    }
    Ok(())
}

/// Convert a `Decimal` shock knob to a finite `f64`.
#[cfg(feature = "simulator")]
fn shock_scalar(value: Decimal) -> Result<f64, BacktestError> {
    value.to_f64().filter(|v| v.is_finite()).ok_or_else(|| {
        BacktestError::Conversion(format!("stress shock {value} is not a finite f64"))
    })
}

/// Guard a computed shock result finite before it re-enters the wire.
#[cfg(feature = "simulator")]
fn finite(value: f64, what: &str) -> Result<f64, BacktestError> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(BacktestError::Conversion(format!(
            "shocked {what} is not finite ({value})"
        )))
    }
}

/// Apply `f` to the numeric `field` inside an externally-tagged walk `method`
/// object, in place. No-op (never an error) if the method is not a single-key
/// object or the variant lacks that numeric field.
#[cfg(feature = "simulator")]
fn shock_method_field(
    method: &mut serde_json::Value,
    field: &str,
    f: impl Fn(f64) -> f64,
) -> Result<(), BacktestError> {
    let Some(object) = method.as_object_mut() else {
        return Ok(());
    };
    if object.len() != 1 {
        return Ok(());
    }
    let Some(variant_key) = object.keys().next().cloned() else {
        return Ok(());
    };
    let Some(params) = object.get_mut(&variant_key).and_then(|v| v.as_object_mut()) else {
        return Ok(());
    };
    if let Some(slot) = params.get_mut(field)
        && let Some(current) = slot.as_f64()
    {
        let shocked = finite(f(current), field)?;
        let number = serde_json::Number::from_f64(shocked).ok_or_else(|| {
            BacktestError::Conversion(format!("shocked walk {field} is not a finite f64 number"))
        })?;
        *slot = serde_json::Value::Number(number);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ConfigOverride, ScenarioParams, ScenarioType, WalkPreset, child_data_seed, child_seed,
        expand,
    };
    use crate::config::{BacktestConfig, FeeSchedule, ResourceLimits, SlippageModel};
    use crate::data::DataSourceSpec;
    use crate::domain::ExecutionMode;
    use crate::error::BacktestError;
    use rust_decimal::Decimal;

    fn base_config() -> BacktestConfig {
        BacktestConfig {
            data_source: DataSourceSpec::Parquet {
                path: "chains/spx.parquet".to_string(),
                sha256: String::new(),
            },
            mode: ExecutionMode::Naive,
            seed: 999,
            initial_capital: 10_000_000,
            fees: FeeSchedule {
                per_contract_cents: 65,
                per_order_cents: 100,
            },
            slippage: SlippageModel::None,
            marketable_cap_ticks: 10,
            liquidity_profile: crate::config::LiquidityProfile::default(),
            limits: ResourceLimits::default(),
            output_dir: "runs/out".into(),
            overwrite: false,
        }
    }

    #[test]
    fn test_scenario_params_serde_round_trip_preserves_fields() {
        let params = ScenarioParams {
            kind: ScenarioType::MonteCarlo,
            base_seed: 42,
            count: 128,
            sweep: vec![
                ConfigOverride {
                    seed: Some(7),
                    ..ConfigOverride::default()
                },
                ConfigOverride::default(),
            ],
        };
        let json = serde_json::to_string(&params).unwrap_or_default();
        let back: Result<ScenarioParams, _> = serde_json::from_str(&json);
        assert!(matches!(back, Ok(ref p) if *p == params));
    }

    #[test]
    fn test_scenario_type_variants_round_trip() {
        for kind in [
            ScenarioType::MonteCarlo,
            ScenarioType::StressTest,
            ScenarioType::Historical,
            ScenarioType::Custom,
        ] {
            let json = serde_json::to_string(&kind).unwrap_or_default();
            let back: Result<ScenarioType, _> = serde_json::from_str(&json);
            assert!(matches!(back, Ok(k) if k == kind));
        }
    }

    #[test]
    fn test_scenario_params_default_sweep_is_empty() {
        let json = r#"{"kind":"MonteCarlo","base_seed":1,"count":4}"#;
        let parsed: Result<ScenarioParams, _> = serde_json::from_str(json);
        assert!(matches!(parsed, Ok(ref p) if p.sweep.is_empty()));
    }

    #[test]
    fn test_config_override_backward_compatible_seed_only_json() {
        // A pre-v0.5 override carrying only `seed` still deserialises: the new
        // shock knobs default to `None`.
        let parsed: Result<ConfigOverride, _> = serde_json::from_str(r#"{"seed":7}"#);
        assert!(matches!(
            parsed,
            Ok(ref o) if o.seed == Some(7)
                && o.walk_volatility_factor.is_none()
                && o.walk_drift_delta.is_none()
        ));
    }

    #[test]
    fn test_child_seed_is_pinned_and_stable() {
        // Pinned expected values: the SHA-256 child-seed scheme is frozen. A
        // change here is a deliberate, versioned break — never a silent drift.
        assert_eq!(child_seed(0, 0), 0x0b6b_944a_18e1_6e29);
        assert_eq!(child_seed(42, 0), 0x5317_e52e_2b4f_cb1a);
        assert_eq!(child_seed(42, 1), 0xfb9f_52a6_4559_c464);
        // The data seed uses a distinct domain tag, so it differs from the
        // engine seed for the same (base_seed, index) yet is equally pinned.
        assert_eq!(child_data_seed(42, 0), 0x7d1c_7410_be51_cdb9);
        assert_ne!(child_seed(42, 0), child_data_seed(42, 0));
    }

    #[test]
    fn test_child_seed_is_a_pure_function_of_inputs() {
        // Independent of any call order or state: recomputing gives the same
        // value, and distinct indices give distinct seeds (no counter reuse).
        for base in [0_u64, 1, 42, u64::MAX] {
            for index in 0_u32..8 {
                assert_eq!(child_seed(base, index), child_seed(base, index));
            }
            let seeds: Vec<u64> = (0..8).map(|i| child_seed(base, i)).collect();
            let mut unique = seeds.clone();
            unique.sort_unstable();
            unique.dedup();
            assert_eq!(
                unique.len(),
                seeds.len(),
                "no index collisions for base {base}"
            );
        }
    }

    #[test]
    fn test_expand_empty_sweep_yields_count_replicas_with_derived_seeds() {
        let params = ScenarioParams {
            kind: ScenarioType::MonteCarlo,
            base_seed: 42,
            count: 4,
            sweep: Vec::new(),
        };
        let configs = match expand(&params, &base_config()) {
            Ok(c) => c,
            Err(e) => panic!("expand must succeed: {e}"),
        };
        assert_eq!(configs.len(), 4, "empty sweep => count replicas");
        for (index, config) in configs.iter().enumerate() {
            let i = u32::try_from(index).unwrap_or(u32::MAX);
            assert_eq!(config.seed, child_seed(42, i));
        }
    }

    #[test]
    fn test_expand_cardinality_is_sweep_times_count() {
        let params = ScenarioParams {
            kind: ScenarioType::StressTest,
            base_seed: 7,
            count: 3,
            sweep: vec![
                ConfigOverride::default(),
                ConfigOverride::default(),
                ConfigOverride::default(),
                ConfigOverride::default(),
            ],
        };
        let configs = match expand(&params, &base_config()) {
            Ok(c) => c,
            Err(e) => panic!("expand must succeed: {e}"),
        };
        assert_eq!(configs.len(), 4 * 3, "sweep.len() x count");
        // Positional global index => the i-th config carries child_seed(base, i).
        for (index, config) in configs.iter().enumerate() {
            let i = u32::try_from(index).unwrap_or(u32::MAX);
            assert_eq!(config.seed, child_seed(7, i));
        }
    }

    #[test]
    fn test_expand_rejects_run_count_over_max() {
        // count × sweep beyond MAX_RUNS is a typed Config error, raised before
        // the result Vec is sized from the untrusted product.
        let params = ScenarioParams {
            kind: ScenarioType::MonteCarlo,
            base_seed: 1,
            count: u32::MAX,
            sweep: Vec::new(),
        };
        assert!(matches!(
            expand(&params, &base_config()),
            Err(BacktestError::Config(_))
        ));
    }

    #[test]
    fn test_expand_rejects_explicit_seed_with_count_gt_one() {
        // An explicit override seed with count > 1 would collide run_ids across
        // replicates → rejected; count = 1 with the same seed is fine.
        let bad = ScenarioParams {
            kind: ScenarioType::MonteCarlo,
            base_seed: 1,
            count: 2,
            sweep: vec![ConfigOverride {
                seed: Some(7),
                ..ConfigOverride::default()
            }],
        };
        assert!(matches!(
            expand(&bad, &base_config()),
            Err(BacktestError::Config(_))
        ));

        let ok = ScenarioParams {
            kind: ScenarioType::MonteCarlo,
            base_seed: 1,
            count: 1,
            sweep: vec![ConfigOverride {
                seed: Some(7),
                ..ConfigOverride::default()
            }],
        };
        assert!(expand(&ok, &base_config()).is_ok());
    }

    #[test]
    fn test_expand_rejects_walk_shocks_outside_stress_test() {
        // A walk shock is StressTest-only; carrying it under any other kind is a
        // typed Config error, and StressTest accepts it.
        for kind in [
            ScenarioType::MonteCarlo,
            ScenarioType::Historical,
            ScenarioType::Custom,
        ] {
            let params = ScenarioParams {
                kind,
                base_seed: 1,
                count: 1,
                sweep: vec![ConfigOverride {
                    seed: None,
                    walk_volatility_factor: Some(Decimal::new(15, 1)),
                    walk_drift_delta: None,
                }],
            };
            assert!(
                matches!(
                    expand(&params, &base_config()),
                    Err(BacktestError::Config(_))
                ),
                "kind {kind:?} must reject a walk shock"
            );
        }
        let stress = ScenarioParams {
            kind: ScenarioType::StressTest,
            base_seed: 1,
            count: 1,
            sweep: vec![ConfigOverride {
                seed: None,
                walk_volatility_factor: Some(Decimal::new(15, 1)),
                walk_drift_delta: None,
            }],
        };
        assert!(expand(&stress, &base_config()).is_ok());
    }

    #[test]
    fn test_expand_is_scheduling_independent_pure_function() {
        // Regenerating from the same inputs yields byte-identical configs — the
        // expansion is a pure function of (params, base), never of order.
        // count = 1 keeps the explicit-seed override path in play while
        // respecting the "explicit seed + count > 1 is rejected" rule.
        let params = ScenarioParams {
            kind: ScenarioType::MonteCarlo,
            base_seed: 12_345,
            count: 1,
            sweep: vec![
                ConfigOverride {
                    seed: Some(111),
                    ..ConfigOverride::default()
                },
                ConfigOverride::default(),
            ],
        };
        let base = base_config();
        let first = expand(&params, &base);
        let second = expand(&params, &base);
        match (first, second) {
            (Ok(a), Ok(b)) => assert_eq!(a, b),
            other => panic!("expand must be deterministic: {other:?}"),
        }
    }

    #[test]
    fn test_expand_explicit_seed_override_pins_engine_seed() {
        let params = ScenarioParams {
            kind: ScenarioType::Custom,
            base_seed: 1,
            count: 1,
            sweep: vec![
                ConfigOverride {
                    seed: Some(4242),
                    ..ConfigOverride::default()
                },
                ConfigOverride::default(),
            ],
        };
        let configs = match expand(&params, &base_config()) {
            Ok(c) => c,
            Err(e) => panic!("expand must succeed: {e}"),
        };
        assert_eq!(configs.len(), 2);
        // Run 0's override pins the engine seed; run 1 falls back to child_seed.
        assert_eq!(configs[0].seed, 4242);
        assert_eq!(configs[1].seed, child_seed(1, 1));
    }

    #[test]
    fn test_walk_preset_geometric_brownian_maps_to_tagged_method_json() {
        let preset = WalkPreset::GeometricBrownian {
            dt: Decimal::new(4, 3),          // 0.004
            drift: Decimal::new(5, 2),       // 0.05
            volatility: Decimal::new(25, 2), // 0.25
        };
        let method = match preset.to_method_json() {
            Ok(m) => m,
            Err(e) => panic!("preset must build a method: {e}"),
        };
        let expected = serde_json::json!({
            "GeometricBrownian": {"dt": 0.004, "drift": 0.05, "volatility": 0.25}
        });
        assert_eq!(method, expected);
    }

    #[cfg(feature = "simulator")]
    #[test]
    fn test_expand_simulator_derives_data_seed_and_applies_stress_shocks() {
        use crate::data::SimulatorSourceSpec;
        use crate::data::simulator::CreateSessionRequest;

        let mut base = base_config();
        base.data_source = DataSourceSpec::Simulator(SimulatorSourceSpec {
            session: CreateSessionRequest {
                symbol: "SPX".to_string(),
                steps: 10,
                initial_price: 4300.0,
                days_to_expiration: 30.0,
                volatility: 0.2,
                risk_free_rate: 0.03,
                dividend_yield: 0.0,
                method: serde_json::json!({
                    "GeometricBrownian": {"dt": 0.004, "drift": 0.05, "volatility": 0.2}
                }),
                time_frame: "Day".to_string(),
                chain_size: Some(15),
                strike_interval: Some(5.0),
                skew_slope: None,
                smile_curve: None,
                spread: Some(0.02),
                seed: None,
            },
            base_url: "http://localhost:7070".to_string(),
            data_seed: 0,
            tape_sha256: "stale".to_string(),
            simulator_version: None,
        });

        let params = ScenarioParams {
            kind: ScenarioType::StressTest,
            base_seed: 7,
            count: 1,
            sweep: vec![
                // Baseline replica — no shock.
                ConfigOverride::default(),
                // A 1.5x volatility shock and a +0.01 drift shift.
                ConfigOverride {
                    seed: None,
                    walk_volatility_factor: Some(Decimal::new(15, 1)),
                    walk_drift_delta: Some(Decimal::new(1, 2)),
                },
            ],
        };
        let configs = match expand(&params, &base) {
            Ok(c) => c,
            Err(e) => panic!("simulator expand must succeed: {e}"),
        };
        assert_eq!(configs.len(), 2);

        // Run 0 (baseline): derived data seed, cleared tape sha, unshocked walk.
        let DataSourceSpec::Simulator(spec0) = &configs[0].data_source else {
            panic!("run 0 keeps a simulator source");
        };
        assert_eq!(spec0.data_seed, child_data_seed(7, 0));
        assert_eq!(spec0.session.seed, Some(child_data_seed(7, 0)));
        assert!(
            spec0.tape_sha256.is_empty(),
            "the stale tape sha is cleared"
        );
        assert_eq!(spec0.session.volatility, 0.2);

        // Run 1 (shocked): session vol ×1.5, walk vol ×1.5, walk drift +0.01.
        let DataSourceSpec::Simulator(spec1) = &configs[1].data_source else {
            panic!("run 1 keeps a simulator source");
        };
        assert_eq!(spec1.data_seed, child_data_seed(7, 1));
        assert!((spec1.session.volatility - 0.3).abs() < 1e-9);
        let method = &spec1.session.method;
        let vol = method
            .get("GeometricBrownian")
            .and_then(|m| m.get("volatility"))
            .and_then(serde_json::Value::as_f64);
        let drift = method
            .get("GeometricBrownian")
            .and_then(|m| m.get("drift"))
            .and_then(serde_json::Value::as_f64);
        assert!(
            matches!(vol, Some(v) if (v - 0.3).abs() < 1e-9),
            "walk vol shocked: {vol:?}"
        );
        assert!(
            matches!(drift, Some(d) if (d - 0.06).abs() < 1e-9),
            "walk drift shifted: {drift:?}"
        );
    }

    #[test]
    fn test_walk_preset_heston_maps_to_all_upstream_fields() {
        let preset = WalkPreset::Heston {
            dt: Decimal::new(4, 3),
            drift: Decimal::new(5, 2),
            volatility: Decimal::new(2, 1),
            kappa: Decimal::new(15, 1),
            theta: Decimal::new(4, 2),
            xi: Decimal::new(3, 1),
            rho: Decimal::new(-5, 1),
        };
        let method = match preset.to_method_json() {
            Ok(m) => m,
            Err(e) => panic!("preset must build a method: {e}"),
        };
        // Field names verified against the upstream ApiWalkType::Heston.
        let expected = serde_json::json!({
            "Heston": {
                "dt": 0.004, "drift": 0.05, "volatility": 0.2,
                "kappa": 1.5, "theta": 0.04, "xi": 0.3, "rho": -0.5
            }
        });
        assert_eq!(method, expected);
    }
}

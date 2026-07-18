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

use serde::{Deserialize, Serialize};

/// The kind of scenario batch to run.
///
/// A small, stable, boundary-exposed enum — `#[repr(u8)]` per the ruleset.
/// Migrated verbatim from OptionStratBacktest's `ScenarioType`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
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
/// of a sweep.
///
/// **Data only for v0.1** — a placeholder for the sweep dimension named in
/// [docs/02 §9](../../../docs/02-engine-architecture.md#9-scenario-orchestration)
/// (`sweep: Vec<ConfigOverride>`), which the doc references but does not yet
/// pin a shape for. The v0.1 shape carries an optional engine-seed override;
/// the generator that *applies* overrides — and any richer numeric override
/// dimensions — lands with the generator at v0.5 (issue #46). Kept minimal on
/// purpose so it does not fossilise an unspecified contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigOverride {
    /// Optional engine-seed override for this run. `None` means the run uses
    /// the batch's derived `child_seed(base_seed, i)` (v0.5).
    pub seed: Option<u64>,
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

#[cfg(test)]
mod tests {
    use super::{ConfigOverride, ScenarioParams, ScenarioType};

    #[test]
    fn test_scenario_params_serde_round_trip_preserves_fields() {
        let params = ScenarioParams {
            kind: ScenarioType::MonteCarlo,
            base_seed: 42,
            count: 128,
            sweep: vec![
                ConfigOverride { seed: Some(7) },
                ConfigOverride { seed: None },
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
}

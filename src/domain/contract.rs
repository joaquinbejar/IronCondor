//! Contract identity: [`Underlying`], [`ContractKey`], and the canonical
//! versioned `contract_id` string.
//!
//! `ContractKey` is the map key for per-contract state and the join key
//! between a chain-snapshot quote and a strategy leg. `ExpirationDate` and
//! `OptionStyle` are reused from `optionstratlib`, not redefined.
//!
//! # Exact identity semantics
//!
//! The upstream `ExpirationDate` implements `PartialEq`/`Hash` with an
//! epsilon tolerance routed through its day-count (and, for the `DateTime`
//! variant, a thread-local reference instant). That is unusable for a map
//! key in a deterministic replay, so `ContractKey` implements
//! `PartialEq`/`Eq`/`Hash` **by hand with exact semantics**: two keys are
//! equal iff every field is exactly equal (`Days` compares the exact
//! `Positive` value, `DateTime` the exact instant; the two variants are
//! never equal to each other). A contract's identity must be exact — a
//! different expiry is a different contract, never "close enough".

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use chrono::DateTime;
use optionstratlib::{ExpirationDate, OptionStyle};
use serde::{Deserialize, Serialize};

use crate::domain::money::PriceCents;
use crate::error::BacktestError;

/// The version prefix of the canonical contract identifier format.
const CONTRACT_ID_VERSION: &str = "v1";

/// A canonical uppercase ticker, e.g. `"SPX"`, `"BTC"`.
///
/// Enforces the colon-free grammar `^[A-Z0-9._]{1,32}$` so a `contract_id`
/// splits unambiguously on `:`. Serialises as a bare string; deserialisation
/// re-validates the grammar.
///
/// # Interning (`Arc<str>`)
///
/// The ticker is held as an [`Arc<str>`] rather than an owned `String`, so
/// cloning an `Underlying` — and therefore cloning a [`ContractKey`] and every
/// `Fill` / `FillDraft` / `OpenPosition` / `QuoteView` that owns one — is a
/// refcount bump, not a heap allocation. This removes the per-fill `String`
/// allocation from the warm replay-step body (PB-1,
/// [docs/07 §4](../../../docs/07-performance-and-security.md#4-allocation-discipline-on-the-replay-loop)):
/// the one allocation happens once, in [`Underlying::new`], when the validated
/// ticker is interned. `Arc<str>` derefs to `str`, so the derived
/// `Eq`/`Ord`/`Hash` compare and hash the ticker bytes exactly as the previous
/// `String` field did — the semantics are unchanged.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct Underlying(Arc<str>);

impl Underlying {
    /// Wrap a canonical uppercase ticker.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Conversion`] when the ticker violates the
    /// grammar `^[A-Z0-9._]{1,32}$`.
    #[must_use = "the validated underlying must be used"]
    pub fn new<S: Into<String>>(ticker: S) -> Result<Self, BacktestError> {
        let ticker = ticker.into();
        let valid_len = !ticker.is_empty() && ticker.len() <= 32;
        let valid_chars = ticker
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '.' || c == '_');
        if !valid_len || !valid_chars {
            return Err(BacktestError::Conversion(format!(
                "underlying {ticker:?} violates the grammar ^[A-Z0-9._]{{1,32}}$"
            )));
        }
        // Intern the validated ticker once — every later clone is a refcount
        // bump, not a heap allocation.
        Ok(Self(Arc::from(ticker)))
    }

    /// The inner canonical ticker.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for Underlying {
    type Error = BacktestError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<Underlying> for String {
    fn from(value: Underlying) -> Self {
        value.0.as_ref().to_owned()
    }
}

/// The identity of one option contract — the join key between a snapshot
/// quote and a strategy leg.
///
/// See the module docs for the exact (non-epsilon) equality and hashing
/// semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractKey {
    /// The canonical uppercase ticker.
    pub underlying: Underlying,
    /// The contract expiry — reused from `optionstratlib`. A relative
    /// `Days(n)` expiry is resolved to one absolute instant once, at tape
    /// materialisation (the data conversion boundary); only a resolved
    /// (`DateTime`) key can produce a `contract_id`.
    pub expiration: ExpirationDate,
    /// The strike in integer cents.
    pub strike: PriceCents,
    /// Call or put — reused from `optionstratlib`.
    pub style: OptionStyle,
}

/// Exact expiration equality: same variant, exactly the same value —
/// deliberately NOT the upstream epsilon-tolerant comparison.
fn expiration_exact_eq(a: &ExpirationDate, b: &ExpirationDate) -> bool {
    match (a, b) {
        (ExpirationDate::Days(da), ExpirationDate::Days(db)) => da.to_dec() == db.to_dec(),
        (ExpirationDate::DateTime(ta), ExpirationDate::DateTime(tb)) => ta == tb,
        _ => false,
    }
}

impl PartialEq for ContractKey {
    fn eq(&self, other: &Self) -> bool {
        self.underlying == other.underlying
            && self.strike == other.strike
            && self.style == other.style
            && expiration_exact_eq(&self.expiration, &other.expiration)
    }
}

impl Eq for ContractKey {}

/// Exact expiration ordering, consistent with [`expiration_exact_eq`]:
/// `Days` sorts before `DateTime`; within a variant the exact values
/// compare — deliberately NOT the upstream day-count comparison.
///
/// `pub(crate)` because it is the crate's **single** expiration ordering rule:
/// [`ContractKey`]'s `Ord` and the canonical leg order of
/// [`crate::domain::LegSetSpec`] (hashed into the `run_id`) must agree, so both
/// call this one function rather than re-deriving the same comparison.
pub(crate) fn expiration_exact_cmp(a: &ExpirationDate, b: &ExpirationDate) -> std::cmp::Ordering {
    match (a, b) {
        (ExpirationDate::Days(da), ExpirationDate::Days(db)) => da.to_dec().cmp(&db.to_dec()),
        (ExpirationDate::DateTime(ta), ExpirationDate::DateTime(tb)) => ta.cmp(tb),
        (ExpirationDate::Days(_), ExpirationDate::DateTime(_)) => std::cmp::Ordering::Less,
        (ExpirationDate::DateTime(_), ExpirationDate::Days(_)) => std::cmp::Ordering::Greater,
    }
}

impl PartialOrd for ContractKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ContractKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.underlying
            .cmp(&other.underlying)
            .then_with(|| expiration_exact_cmp(&self.expiration, &other.expiration))
            .then_with(|| self.strike.cmp(&other.strike))
            .then_with(|| self.style.cmp(&other.style))
    }
}

impl Hash for ContractKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.underlying.hash(state);
        match &self.expiration {
            ExpirationDate::Days(days) => {
                0u8.hash(state);
                // Normalised mantissa/scale so equal Decimals hash equally.
                let normalized = days.to_dec().normalize();
                normalized.mantissa().hash(state);
                normalized.scale().hash(state);
            }
            ExpirationDate::DateTime(instant) => {
                1u8.hash(state);
                instant.hash(state);
            }
        }
        self.strike.hash(state);
        self.style.hash(state);
    }
}

impl ContractKey {
    /// The resolved expiration as nanoseconds since the Unix epoch (UTC).
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Conversion`] when the expiration is still a
    /// relative `Days(n)` (resolution happens once, at the data conversion
    /// boundary) or when the instant is outside the nanosecond `i64` range.
    #[must_use = "the resolved expiration must be used"]
    pub fn expiration_ns(&self) -> Result<i64, BacktestError> {
        match &self.expiration {
            ExpirationDate::DateTime(instant) => instant.timestamp_nanos_opt().ok_or_else(|| {
                BacktestError::Conversion(format!(
                    "expiration {instant} outside the nanosecond i64 range"
                ))
            }),
            ExpirationDate::Days(days) => Err(BacktestError::Conversion(format!(
                "relative expiration Days({days}) is unresolved — resolved once at tape materialisation"
            ))),
        }
    }

    /// Build the canonical versioned contract identifier:
    /// `"v1:{UNDERLYING}:{expiration_ns}:{strike_cents}:{style}"`, e.g.
    /// `"v1:SPX:1750291200000000000:510000:C"`.
    ///
    /// A pure function of the key; [`Self::from_contract_id`] is its
    /// inverse.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Conversion`] when the expiration is not yet
    /// resolved to an absolute instant (see [`Self::expiration_ns`]).
    #[must_use = "the contract id must be used"]
    pub fn to_contract_id(&self) -> Result<String, BacktestError> {
        let expiration_ns = self.expiration_ns()?;
        let style = match self.style {
            OptionStyle::Call => "C",
            OptionStyle::Put => "P",
        };
        Ok(format!(
            "{CONTRACT_ID_VERSION}:{}:{expiration_ns}:{}:{style}",
            self.underlying.as_str(),
            self.strike.value()
        ))
    }

    /// Parse a canonical `contract_id` back into a key (the inverse of
    /// [`Self::to_contract_id`]); the reconstructed expiration is always the
    /// resolved `DateTime` form.
    ///
    /// # Errors
    ///
    /// Returns [`BacktestError::Conversion`] for a wrong version tag, a
    /// wrong segment count, an `UNDERLYING` violating the grammar, or a
    /// non-numeric expiration/strike segment.
    #[must_use = "the parsed contract key must be used"]
    pub fn from_contract_id(contract_id: &str) -> Result<Self, BacktestError> {
        let segments: Vec<&str> = contract_id.split(':').collect();
        let [version, underlying, expiration_ns, strike_cents, style] = segments.as_slice() else {
            return Err(BacktestError::Conversion(format!(
                "contract id {contract_id:?} must have exactly 5 ':'-separated segments"
            )));
        };
        if *version != CONTRACT_ID_VERSION {
            return Err(BacktestError::Conversion(format!(
                "contract id {contract_id:?} has unsupported version {version:?}, expected \"v1\""
            )));
        }
        let underlying = Underlying::new(*underlying)?;
        let expiration_ns: i64 = expiration_ns.parse().map_err(|_| {
            BacktestError::Conversion(format!(
                "contract id {contract_id:?} has non-numeric expiration_ns {expiration_ns:?}"
            ))
        })?;
        let strike: u64 = strike_cents.parse().map_err(|_| {
            BacktestError::Conversion(format!(
                "contract id {contract_id:?} has non-numeric strike_cents {strike_cents:?}"
            ))
        })?;
        let style = match *style {
            "C" => OptionStyle::Call,
            "P" => OptionStyle::Put,
            other => {
                return Err(BacktestError::Conversion(format!(
                    "contract id {contract_id:?} has unknown style {other:?}, expected \"C\" or \"P\""
                )));
            }
        };
        Ok(Self {
            underlying,
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(expiration_ns)),
            strike: PriceCents::new(strike),
            style,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use optionstratlib::{ExpirationDate, OptionStyle};
    use positive::Positive;

    fn days_30() -> Positive {
        let Ok(days) = Positive::new(30.0) else {
            panic!("30.0 is a valid positive value");
        };
        days
    }

    use super::{ContractKey, Underlying};
    use crate::domain::money::PriceCents;
    use crate::error::BacktestError;

    fn resolved_key() -> ContractKey {
        ContractKey {
            underlying: Underlying::new("SPX").unwrap_or_else(|_| unreachable!("SPX is valid")),
            expiration: ExpirationDate::DateTime(DateTime::from_timestamp_nanos(
                1_750_291_200_000_000_000,
            )),
            strike: PriceCents::new(510_000),
            style: OptionStyle::Call,
        }
    }

    #[test]
    fn test_contract_id_roundtrips() {
        let key = resolved_key();
        let id = key.to_contract_id();
        assert!(matches!(
            id.as_deref(),
            Ok("v1:SPX:1750291200000000000:510000:C")
        ));
        let id = id.unwrap_or_default();
        let back = ContractKey::from_contract_id(&id);
        assert!(matches!(back, Ok(ref k) if *k == key));
    }

    #[test]
    fn test_contract_id_rejects_wrong_version() {
        let err = ContractKey::from_contract_id("v2:SPX:0:100:C");
        assert!(matches!(err, Err(BacktestError::Conversion(_))));
    }

    #[test]
    fn test_contract_id_rejects_wrong_segment_count() {
        let err = ContractKey::from_contract_id("v1:SPX:0:100");
        assert!(matches!(err, Err(BacktestError::Conversion(_))));
    }

    #[test]
    fn test_contract_id_rejects_bad_underlying_grammar() {
        for bad in ["v1:spx:0:100:C", "v1::0:100:C", "v1:S-PX:0:100:C"] {
            assert!(
                matches!(
                    ContractKey::from_contract_id(bad),
                    Err(BacktestError::Conversion(_))
                ),
                "{bad} must be rejected"
            );
        }
    }

    #[test]
    fn test_contract_id_rejects_non_numeric_segments() {
        assert!(matches!(
            ContractKey::from_contract_id("v1:SPX:soon:100:C"),
            Err(BacktestError::Conversion(_))
        ));
        assert!(matches!(
            ContractKey::from_contract_id("v1:SPX:0:many:P"),
            Err(BacktestError::Conversion(_))
        ));
        assert!(matches!(
            ContractKey::from_contract_id("v1:SPX:0:100:X"),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_underlying_deserialisation_revalidates_grammar() {
        let bad: Result<Underlying, _> = serde_json::from_str("\"spx\"");
        assert!(bad.is_err(), "lowercase ticker must fail deserialisation");
        let good: Result<Underlying, _> = serde_json::from_str("\"SPX\"");
        assert!(matches!(good, Ok(ref u) if u.as_str() == "SPX"));
    }

    #[test]
    fn test_underlying_rejects_colon_and_lowercase_and_length() {
        assert!(Underlying::new("SPX").is_ok());
        assert!(Underlying::new("BRK.B").is_ok());
        assert!(Underlying::new("A_1").is_ok());
        assert!(Underlying::new("a").is_err());
        assert!(Underlying::new("S:PX").is_err());
        assert!(Underlying::new("").is_err());
        assert!(Underlying::new("X".repeat(33)).is_err());
        assert!(Underlying::new("X".repeat(32)).is_ok());
    }

    #[test]
    fn test_unresolved_days_expiration_cannot_build_contract_id() {
        let mut key = resolved_key();
        key.expiration = ExpirationDate::Days(days_30());
        assert!(matches!(
            key.to_contract_id(),
            Err(BacktestError::Conversion(_))
        ));
    }

    #[test]
    fn test_contract_key_equality_is_exact_across_variants() {
        let resolved = resolved_key();
        let mut relative = resolved.clone();
        relative.expiration = ExpirationDate::Days(days_30());
        // Days vs DateTime are never equal, whatever the upstream epsilon
        // comparison would say.
        assert_ne!(resolved, relative);
        assert_eq!(resolved, resolved.clone());
        assert_eq!(relative, relative.clone());
    }

    #[test]
    fn test_contract_key_works_as_exact_map_key() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(resolved_key(), 1u32);
        assert_eq!(map.get(&resolved_key()), Some(&1));
        let mut other = resolved_key();
        other.strike = PriceCents::new(520_000);
        assert_eq!(map.get(&other), None);
    }
}

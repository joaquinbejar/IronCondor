//! The typed error boundary of the crate.
//!
//! [`BacktestError`] is the single error type crossing every public boundary.
//! Upstream errors (`optionstratlib`, `option_chain_orderbook`, HTTP, Parquet
//! IO) convert into its kinds **here and nowhere else** — modules never do
//! ad-hoc mapping. `anyhow` is banned outside examples and binaries.
//!
//! At the PyO3 boundary each variant maps to a Python exception (v0.4); no
//! mapping code lives here and no panic crosses FFI.

/// The single typed error for every fallible operation in `ironcondor`.
///
/// The enum is flat and exhaustive; each kind carries the offending value in
/// a lowercase, human-readable message. Concrete conversions from upstream
/// error types land at their seams (engine migration, chain conversion,
/// Parquet feed) but always into one of these kinds.
#[derive(Debug, thiserror::Error)]
pub enum BacktestError {
    // domain / config
    /// A quantity of zero (or otherwise structurally invalid) was supplied.
    #[error("invalid quantity: {0}")]
    InvalidQuantity(u32),
    /// A quote whose bid exceeds its ask — rejected at the conversion
    /// boundary. Prices are integer cents.
    #[error("crossed quote: bid {bid} > ask {ask}")]
    CrossedQuote {
        /// Bid price in integer cents.
        bid: u64,
        /// Ask price in integer cents.
        ask: u64,
    },
    /// A price that is not representable on the venue tick grid. Prices and
    /// ticks are integer cents.
    #[error("price {price} not aligned to tick {tick}")]
    PriceNotTickAligned {
        /// The offending price in integer cents.
        price: u64,
        /// The venue tick size in integer cents.
        tick: u64,
    },
    /// A tape timestamp that is not strictly after its predecessor
    /// (duplicate or reversed).
    #[error("data out of order: step {step} ts {ts} not strictly after previous {prev}")]
    DataOutOfOrder {
        /// The zero-based step index at which the violation was detected.
        step: u32,
        /// The offending timestamp (nanoseconds since the Unix epoch).
        ts: i64,
        /// The previous timestamp it failed to strictly exceed.
        prev: i64,
    },
    /// A materialisation ceiling was crossed while collecting a tape — the
    /// run is cut off before an unbounded allocation.
    #[error("tape too large: {limit} = {value} exceeds cap {cap}")]
    TapeTooLarge {
        /// The name of the [`crate::config::ResourceLimits`] field crossed.
        limit: &'static str,
        /// The observed value.
        value: u64,
        /// The configured ceiling it exceeded.
        cap: u64,
    },
    /// An invalid configuration value, rejected by
    /// [`crate::config::BacktestConfig::validate`].
    #[error("config: {0}")]
    Config(String),

    // data / session
    /// An IO failure reading a data source.
    #[error("data source io: {0}")]
    DataIo(#[from] std::io::Error),
    /// A chain failed validation while converting into an
    /// `optionstratlib::chains::OptionChain`.
    #[error("chain conversion: {0}")]
    Conversion(String),
    /// A simulator session failure (HTTP transport or protocol).
    #[error("simulator session: {0}")]
    Session(String),

    // execution
    /// A fill-model failure (unknown position, oversized close, …).
    #[error("execution: {0}")]
    Execution(String),
    /// An order-book failure — wraps `option_chain_orderbook::Error` at the
    /// realistic-execution seam.
    #[error("orderbook: {0}")]
    OrderBook(String),

    // upstream + bundle io
    /// A strategy failure — wraps `optionstratlib` errors at the strategy
    /// adapter seam.
    #[error("strategy: {0}")]
    Strategy(String),
    /// A result-bundle write or read-back failure.
    #[error("bundle write: {0}")]
    Bundle(String),
    /// Checked integer arithmetic on cents or counters overflowed.
    #[error("arithmetic overflow")]
    ArithmeticOverflow,
}

#[cfg(test)]
mod tests {
    use super::BacktestError;

    #[test]
    fn test_error_messages_carry_offending_values() {
        assert_eq!(
            BacktestError::InvalidQuantity(0).to_string(),
            "invalid quantity: 0"
        );
        assert_eq!(
            BacktestError::CrossedQuote { bid: 105, ask: 100 }.to_string(),
            "crossed quote: bid 105 > ask 100"
        );
        assert_eq!(
            BacktestError::PriceNotTickAligned {
                price: 101,
                tick: 5
            }
            .to_string(),
            "price 101 not aligned to tick 5"
        );
        assert_eq!(
            BacktestError::DataOutOfOrder {
                step: 3,
                ts: 900,
                prev: 1_000
            }
            .to_string(),
            "data out of order: step 3 ts 900 not strictly after previous 1000"
        );
        assert_eq!(
            BacktestError::TapeTooLarge {
                limit: "max_steps",
                value: 200,
                cap: 100
            }
            .to_string(),
            "tape too large: max_steps = 200 exceeds cap 100"
        );
        assert_eq!(
            BacktestError::Config("initial capital must be positive, got 0".to_string())
                .to_string(),
            "config: initial capital must be positive, got 0"
        );
        assert_eq!(
            BacktestError::ArithmeticOverflow.to_string(),
            "arithmetic overflow"
        );
    }

    #[test]
    fn test_error_from_io_error_maps_to_data_io() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "missing.parquet");
        let err = BacktestError::from(io);
        assert!(matches!(err, BacktestError::DataIo(_)));
        assert!(err.to_string().starts_with("data source io: "));
    }
}

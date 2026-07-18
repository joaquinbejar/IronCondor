//! The result bundle.
//!
//! The portable run artifact — `manifest.json` plus the four Parquet tables —
//! written atomically and consumed by ChainView (roadmap issues #33–#36).
//!
//! [`schema`] lands the **typed shape** of the frozen `ironcondor.bundle.v1`
//! contract (issue #33): the schema tag [`BUNDLE_SCHEMA`], the run identity
//! [`RunId`], the [`Manifest`] (the single serialization source for
//! `manifest.json`), the per-table [`RowCounts`], and the pinned per-table sort
//! keys. The four Parquet **row** types it references live one layer down in
//! [`crate::domain::result`]. [`writer`] lands the Parquet encoding + atomic
//! directory publish (issue #34); read-back validation is #35 and the golden
//! freeze #36.

pub mod schema;
pub mod writer;

pub use schema::{
    BUNDLE_SCHEMA, EQUITY_CURVE_SORT_COLUMNS, FILLS_SORT_COLUMNS, GREEKS_ATTRIBUTION_SORT_COLUMNS,
    Manifest, POSITIONS_SORT_COLUMNS, RowCounts, RunId, equity_sort_key, fill_sort_key,
    greeks_sort_key, position_sort_key,
};
pub use writer::write_bundle;

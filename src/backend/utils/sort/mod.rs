//! Directory module: src/backend/utils/sort (the .c bodies for sort/store).
//!
//! Step 24 (M5 keystone) lands the in-memory sort + materialized tuple store +
//! logical-tape abstraction that back the ORDER BY / Agg / Unique / Material
//! executor nodes. The in-memory paths are complete; the external-merge spill is
//! structurally translated but stub-calls the still-hollow BufFile leaves
//! (rules.md s4).

pub mod logtape;
pub mod tuplesort;
pub mod tuplestore;

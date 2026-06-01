//! Block Range INdexes (postgres/src/backend/access/brin).
//!
//! So far: the built-in `minmax` opclass support functions (`brin_minmax`).

pub mod brin;
pub mod brin_inclusion;
pub mod brin_minmax_multi;
pub mod brin_xlog;
pub mod brin_internal;
pub mod brin_minmax;
pub mod brin_page;
pub mod brin_validate;
pub mod brin_revmap;
pub mod brin_tuple;
pub mod brin_bloom;
pub mod brin_pageops;

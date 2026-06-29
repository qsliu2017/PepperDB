//! Translated from PostgreSQL src/include/optimizer/placeholder.h
//!
//! C declarations; bodies live in `crate::backend::optimizer::util::placeholder`
//! and are re-exported here under the C names.

pub use crate::backend::optimizer::util::placeholder::{
    add_placeholders_to_base_rels, add_placeholders_to_joinrel,
    contain_placeholder_references_to, find_placeholder_info, find_placeholders_in_jointree,
    fix_placeholder_input_needed_levels, get_placeholder_nulling_relids, make_placeholder_expr,
    rebuild_placeholder_attr_needed, strip_noop_phvs,
};

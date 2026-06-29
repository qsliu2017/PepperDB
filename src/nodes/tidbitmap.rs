//! Header for the TID bitmap package (src/include/nodes/tidbitmap.h). The .c body
//! lives at `src/backend/nodes/tidbitmap.rs` (the plan-002 C-file-mapping
//! invariant: defs go under `src/backend/`); this header is rewired to re-export
//! the public types + entry points so existing `crate::nodes::tidbitmap::...`
//! references resolve to the implementation.

pub use crate::backend::nodes::tidbitmap::{
    tbm_add_page, tbm_add_tuples, tbm_begin_iterate, tbm_begin_private_iterate,
    tbm_calculate_entries, tbm_create, tbm_end_iterate, tbm_end_private_iterate, tbm_exhausted,
    tbm_free, tbm_intersect, tbm_is_empty, tbm_iterate, tbm_private_iterate, tbm_union,
    PagetableEntry, TBMIterateResult, TBMIterator, TBMPrivateIterator, TBMSharedIterator,
    TIDBitmap, TBM_MAX_TUPLES_PER_PAGE,
};

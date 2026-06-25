//! Translated from PostgreSQL src/include/nodes/miscnodes.h

use crate::utils::elog::ErrorData;

/// Function call context node for handling "soft" errors.
///
/// Initialize with all fields default except the tag; set `details_wanted` for
/// full `error_data`. After a call that might soft-error, check `error_occurred`.
pub struct ErrorSaveContext {
    pub error_occurred: bool,
    pub details_wanted: bool,
    pub error_data: Option<Box<ErrorData>>,
}

/// C: `SOFT_ERROR_OCCURRED(escontext)` -- a soft error was reported.
pub fn SOFT_ERROR_OCCURRED(escontext: Option<&ErrorSaveContext>) -> bool {
    escontext.is_some_and(|e| e.error_occurred)
}

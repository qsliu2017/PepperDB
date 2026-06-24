//! Translated from PostgreSQL src/include/nodes/miscnodes.h

/// Error details captured for a soft error.
///
/// Real definition lives in utils/elog.h; forward-declared here so
/// `ErrorSaveContext` can reference it.
// TODO(struct-forward): repoint to crate::utils::elog::ErrorData in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::elog::ErrorData in Phase 2")]
pub struct ErrorData;

/// Function call context node for handling "soft" errors.
///
/// Initialize with all fields default except the tag; set `details_wanted` for
/// full `error_data`. After a call that might soft-error, check `error_occurred`.
pub struct ErrorSaveContext {
    pub error_occurred: bool,
    pub details_wanted: bool,
    #[allow(deprecated)]
    pub error_data: Option<Box<ErrorData>>,
}

/// C: `SOFT_ERROR_OCCURRED(escontext)` -- a soft error was reported.
pub fn SOFT_ERROR_OCCURRED(escontext: Option<&ErrorSaveContext>) -> bool {
    // TODO(struct-forward): also assert IsA(escontext, ErrorSaveContext) once the
    // tag exists; the Option already encodes the NULL check.
    escontext.is_some_and(|e| e.error_occurred)
}

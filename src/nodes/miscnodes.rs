//! Translated from PostgreSQL src/include/nodes/miscnodes.h
//!
//! Definitions for hard-to-classify node types. `ErrorSaveContext` is the
//! "soft error" escape hatch: a caller that wants to trap a soft error (e.g. a
//! datatype input function reporting a conversion failure) initializes one of
//! these and passes it through `FunctionCallInfo.context`. After a call that
//! might report a soft error, it checks `error_occurred`; with `details_wanted`
//! set, `error_data` is populated (see `errsave`/`ereturn` in elog).

use crate::nodes::nodes::Node;
use crate::utils::elog::ErrorData;

/// Function call context node for handling of "soft" errors.
///
/// Initialize with all fields default (see [`ErrorSaveContext::new`]); optionally
/// set `details_wanted` for the full [`ErrorData`]. After a call that might
/// soft-error, check `error_occurred`.
#[derive(Debug, Clone, Default)]
pub struct ErrorSaveContext {
    /// set to true if we detect a soft error
    pub error_occurred: bool,
    /// does caller want more info than that?
    pub details_wanted: bool,
    /// details of error, if so
    pub error_data: Option<Box<ErrorData>>,
}

impl ErrorSaveContext {
    /// A fresh, empty context (C `{T_ErrorSaveContext}`: all zero except the tag).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

// ErrorData has no PartialEq (it carries backtraces etc.); the Node enum derives
// PartialEq, so compare only the observable flags. Two contexts are equal when
// they agree on whether an error occurred and whether details were wanted.
impl PartialEq for ErrorSaveContext {
    fn eq(&self, other: &Self) -> bool {
        self.error_occurred == other.error_occurred
            && self.details_wanted == other.details_wanted
    }
}

/// C: `SOFT_ERROR_OCCURRED(escontext)` -- a soft error was reported into an
/// `ErrorSaveContext`. `context` is the `FunctionCallInfo.context` node (or None).
#[must_use]
pub fn SOFT_ERROR_OCCURRED(context: Option<&Node>) -> bool {
    matches!(context, Some(Node::ErrorSaveContext(e)) if e.error_occurred)
}

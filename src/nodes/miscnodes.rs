//! nodes/miscnodes.h - definitions for hard-to-classify node types.
//!
//! Node types declared here are not part of parse trees, plan trees, or
//! execution state trees. We only assign them NodeTag values because IsA()
//! tests provide a convenient way to disambiguate what kind of structure is
//! being passed through assorted APIs, such as function "context" pointers.

use crate::nodes::nodes::NodeTag;
use crate::prelude::*; // brings in `bool` (and friends) from crate::c

// ErrorData is defined by utils/elog.h, which has not yet been translated as a
// struct (src/utils/elog.rs only carries a TODO). Use a minimal opaque stub.
// TODO: dedup when elog.h lands.
pub type ErrorData = c_void;

/// ErrorSaveContext -
///     function call context node for handling of "soft" errors
///
/// A caller wishing to trap soft errors must initialize a struct like this
/// with all fields zero/NULL except for the NodeTag. Optionally, set
/// `details_wanted = true` if more than the bare knowledge that a soft error
/// occurred is required. The struct is then passed to a SQL-callable function
/// via the FunctionCallInfo.context field; or below the level of SQL calls,
/// it could be passed to a subroutine directly.
///
/// After calling code that might report an error this way, check
/// `error_occurred` to see if an error happened. If so, and if `details_wanted`
/// is true, `error_data` has been filled with error details (stored in the
/// callee's memory context!). The ErrorData can be modified (e.g. downgraded
/// to a WARNING) and reported with ThrowErrorData(). FreeErrorData() can be
/// called to release `error_data`, although that step is typically not
/// necessary if the called code was run in a short-lived context.
#[repr(C)]
pub struct ErrorSaveContext {
    pub r#type: NodeTag,
    /// set to true if we detect a soft error
    pub error_occurred: bool,
    /// does caller want more info than that?
    pub details_wanted: bool,
    /// details of error, if so
    pub error_data: *mut ErrorData,
}

/// Often-useful macro for checking if a soft error was reported.
///
/// `SOFT_ERROR_OCCURRED(escontext)` in C. `escontext` may be NULL.
#[macro_export]
macro_rules! SOFT_ERROR_OCCURRED {
    ($escontext:expr) => {{
        let escontext = $escontext as *mut $crate::nodes::miscnodes::ErrorSaveContext;
        !escontext.is_null()
            && $crate::IsA!(escontext, T_ErrorSaveContext)
            && (*escontext).error_occurred
    }};
}

//! Translated from PostgreSQL src/include/nodes/subscripting.h

use crate::nodes::primnodes::SubscriptingRef;

// Forward references (parser/executor state) kept abstract here.
// TODO(struct-forward): repoint to the real ParseState/SubscriptingRefState/
// SubscriptExecSteps once those modules are translated.

/// Opaque parse state; real type lives in the parser.
#[deprecated(note = "TODO(struct-forward): repoint to crate::parser parse state in Phase 2")]
#[derive(Debug, Clone, PartialEq)]
pub struct ParseState;

/// Per-SubscriptingRef execution workspace (executor/execExpr.h).
/// `workspace` is a type-specific blob; modeled here as an opaque owned buffer.
// TODO(struct-forward): repoint to crate::executor::execExpr::SubscriptingRefState in Phase 2
#[deprecated(note = "TODO(struct-forward): repoint to crate::executor::execExpr in Phase 2")]
#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptingRefState {
    pub isassignment: bool,
    pub workspace: Option<Vec<u8>>,
    pub numupper: i32,
    pub upperprovided: Vec<bool>,
    pub upperindex: Vec<crate::postgres::Datum>,
    pub upperindexnull: Vec<bool>,
    pub numlower: i32,
    pub lowerprovided: Vec<bool>,
    pub lowerindex: Vec<crate::postgres::Datum>,
    pub lowerindexnull: Vec<bool>,
    pub replacevalue: crate::postgres::Datum,
    pub replacenull: bool,
    pub prevvalue: crate::postgres::Datum,
    pub prevnull: bool,
}

/// The execution-step callbacks a subscripting impl installs (the vtable filled
/// by `exec_setup`). Per routine-struct.md, the optional assignment-only
/// callbacks are split into a supertrait; the required step is `fetch`.
///
/// `sbs_check_subscripts` may be omitted (then `fetch` must subsume it), and
/// `sbs_assign`/`sbs_fetch_old` are only present for types supporting
/// assignment, hence the `Option` slots in the concrete vtable below.
pub trait SubscriptExec {
    /// sbs_fetch: perform a subscripting fetch.
    fn fetch(&self, state: &mut SubscriptingRefState);

    /// sbs_check_subscripts: validate/convert subscripts; false -> overall NULL.
    /// Default folds into `fetch` (the no-separate-check case).
    fn check_subscripts(&self, _state: &mut SubscriptingRefState) -> bool {
        true
    }
}

/// Assignment-capable subscripting (sbs_assign + sbs_fetch_old).
pub trait SubscriptAssign: SubscriptExec {
    /// sbs_assign: perform a subscripting assignment.
    fn assign(&self, state: &mut SubscriptingRefState);

    /// sbs_fetch_old: fetch the existing element/slice for nested assignment.
    fn fetch_old(&self, state: &mut SubscriptingRefState);
}

/// Concrete vtable form, mirroring C `SubscriptExecSteps` (filled by exec_setup).
/// NULL fn pointers -> `None`.
#[derive(Clone)]
pub struct SubscriptExecSteps {
    pub sbs_check_subscripts: Option<fn(&mut SubscriptingRefState) -> bool>,
    pub sbs_fetch: Option<fn(&mut SubscriptingRefState)>,
    pub sbs_assign: Option<fn(&mut SubscriptingRefState)>,
    pub sbs_fetch_old: Option<fn(&mut SubscriptingRefState)>,
}

/// transform method: parse-analyze a subscripting construct, filling `sbsref`.
/// `indirection` is the raw subscript list (List of A_Indices nodes).
pub type SubscriptTransform =
    fn(&mut SubscriptingRef, &[Box<crate::nodes::nodes::Node>], &mut ParseState, bool, bool);

/// exec_setup method: install the execution steps for a SubscriptingRef.
pub type SubscriptExecSetup =
    fn(&SubscriptingRef, &mut SubscriptingRefState, &mut SubscriptExecSteps);

/// Struct returned by the SQL-visible subscript handler function. Holds the two
/// required method pointers plus the leakproof/strict capability flags.
#[derive(Clone)]
pub struct SubscriptRoutines {
    pub transform: SubscriptTransform,
    pub exec_setup: SubscriptExecSetup,
    pub fetch_strict: bool,
    pub fetch_leakproof: bool,
    pub store_leakproof: bool,
}

//! Translated from PostgreSQL src/include/nodes/subscripting.h

use crate::executor::execExpr::SubscriptingRefState;
use crate::nodes::primnodes::SubscriptingRef;
use crate::parser::parse_node::ParseState;

/// The execution-step callbacks a subscripting impl installs (the vtable filled
/// by `exec_setup`). Per routine-struct.md, the optional assignment-only
/// callbacks are split into a supertrait; the required step is `fetch`.
///
/// `check_subscripts` may be omitted (then `fetch` must subsume it), and
/// `assign`/`fetch_old` are only present for types supporting
/// assignment, hence the `Option` slots in the concrete vtable below.
pub trait SubscriptExec {
    /// fetch: perform a subscripting fetch.
    fn fetch(&self, state: &mut SubscriptingRefState);

    /// check_subscripts: validate/convert subscripts; false -> overall NULL.
    /// Default folds into `fetch` (the no-separate-check case).
    fn check_subscripts(&self, _state: &mut SubscriptingRefState) -> bool {
        true
    }
}

/// Assignment-capable subscripting (assign + fetch_old).
pub trait SubscriptAssign: SubscriptExec {
    /// assign: perform a subscripting assignment.
    fn assign(&self, state: &mut SubscriptingRefState);

    /// fetch_old: fetch the existing element/slice for nested assignment.
    fn fetch_old(&self, state: &mut SubscriptingRefState);
}

/// Concrete vtable form, mirroring C `SubscriptExecSteps` (filled by exec_setup).
/// NULL fn pointers -> `None`.
#[derive(Clone)]
pub struct SubscriptExecSteps {
    pub check_subscripts: Option<fn(&mut SubscriptingRefState) -> bool>,
    pub fetch: Option<fn(&mut SubscriptingRefState)>,
    pub assign: Option<fn(&mut SubscriptingRefState)>,
    pub fetch_old: Option<fn(&mut SubscriptingRefState)>,
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

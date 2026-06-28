//! Translated from PostgreSQL src/include/nodes/nodeFuncs.h
//!
//! General-purpose node-tree manipulations. The translated bodies live in the
//! backend definition module (`crate::backend::nodes::nodeFuncs`) and are
//! re-exported below under their C names. Functions whose milestone has not
//! arrived remain `unimplemented!()` stubs here (they will move to the backend
//! module and be re-exported as each is translated; nodeFuncs.c is a `grow`
//! dispatcher).
//!
//! `QueryTreeWalkerFlags` is a type declaration and stays in this header. The C
//! callbacks (`tree_walker_callback`, `tree_mutator_callback`, ...) threaded a
//! `void *context`; a Rust closure captures that state, so the entry points take
//! `impl FnMut(&Node) -> bool` / `impl FnMut(Node) -> Node` and the context
//! parameter disappears.

#![allow(
    clippy::boxed_local,
    reason = "TODO(grow): hollow stubs mirror PG signatures 1:1; real impl consumes the Box"
)]
#![allow(
    clippy::needless_pass_by_value,
    reason = "TODO(grow): hollow stubs mirror PG signatures 1:1; real impl consumes the params"
)]

use bitflags::bitflags;

use crate::nodes::execnodes::PlanState;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::Query;
use crate::nodes::primnodes::{CoercionForm, OpExpr, ScalarArrayOpExpr};
use crate::postgres_ext::Oid;

bitflags! {
    /// Flag bits for query_tree_walker and query_tree_mutator. Composite
    /// `IGNORE_RC_SUBQUERIES` is the OR of the two RT/CTE subquery bits.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct QueryTreeWalkerFlags: i32 {
        /// Subqueries in rtable.
        const IGNORE_RT_SUBQUERIES = 0x01;
        /// Subqueries in cteList.
        const IGNORE_CTE_SUBQUERIES = 0x02;
        /// Both of the above.
        const IGNORE_RC_SUBQUERIES = 0x03;
        /// JOIN alias var lists.
        const IGNORE_JOINALIASES = 0x04;
        /// Skip rangetable entirely.
        const IGNORE_RANGE_TABLE = 0x08;
        /// Examine RTE nodes before their contents.
        const EXAMINE_RTES_BEFORE = 0x10;
        /// Examine RTE nodes after their contents.
        const EXAMINE_RTES_AFTER = 0x20;
        /// Do not copy top Query.
        const DONT_COPY_QUERY = 0x40;
        /// Include SortGroupClause lists.
        const EXAMINE_SORTGROUP = 0x80;
        /// GROUP expressions list.
        const IGNORE_GROUPEXPRS = 0x100;
    }
}

// Translated bodies (crate::backend::nodes::nodeFuncs), re-exported under C names
// (these are already C-cased) and snake_case names where PG uses them.
pub use crate::backend::nodes::nodeFuncs::{
    exprCollation, exprIsLengthCoercion, exprType, exprTypmod, expression_tree_mutator,
    expression_tree_walker, get_leftop, get_notclausearg, get_rightop, is_andclause,
    is_funcclause, is_notclause, is_opclause, is_orclause,
};

pub fn applyRelabelType(
    arg: Box<Node>,
    rtype: Oid,
    rtypmod: i32,
    rcollid: Oid,
    rformat: CoercionForm,
    rlocation: i32,
    overwrite_ok: bool,
) -> Box<Node> {
    let _ = (arg, rtype, rtypmod, rcollid, rformat, rlocation, overwrite_ok);
    unimplemented!()
}

pub fn relabel_to_typmod(expr: Box<Node>, typmod: i32) -> Box<Node> {
    let _ = (expr, typmod);
    unimplemented!()
}

pub fn strip_implicit_coercions(node: Box<Node>) -> Box<Node> {
    let _ = node;
    unimplemented!()
}

pub fn expression_returns_set(clause: &Node) -> bool {
    let _ = clause;
    unimplemented!()
}

pub fn exprInputCollation(expr: &Node) -> Oid {
    let _ = expr;
    unimplemented!()
}

pub fn exprSetCollation(expr: &mut Node, collation: Oid) {
    let _ = (expr, collation);
    unimplemented!()
}

pub fn exprSetInputCollation(expr: &mut Node, inputcollation: Oid) {
    let _ = (expr, inputcollation);
    unimplemented!()
}

pub fn exprLocation(expr: &Node) -> i32 {
    let _ = expr;
    unimplemented!()
}

pub fn fix_opfuncids(node: &mut Node) {
    let _ = node;
    unimplemented!()
}

pub fn set_opfuncid(opexpr: &mut OpExpr) {
    let _ = opexpr;
    unimplemented!()
}

pub fn set_sa_opfuncid(opexpr: &mut ScalarArrayOpExpr) {
    let _ = opexpr;
    unimplemented!()
}

pub fn check_functions_in_node(node: &Node, checker: impl FnMut(Oid) -> bool) -> bool {
    let _ = (node, checker);
    unimplemented!()
}

pub fn query_tree_walker(
    query: &Query,
    walker: impl FnMut(&Node) -> bool,
    flags: QueryTreeWalkerFlags,
) -> bool {
    let _ = (query, walker, flags);
    unimplemented!()
}

pub fn query_tree_mutator(
    query: Query,
    mutator: impl FnMut(Node) -> Node,
    flags: QueryTreeWalkerFlags,
) -> Box<Query> {
    let _ = (query, mutator, flags);
    unimplemented!()
}

pub fn range_table_walker(
    rtable: &[Box<Node>],
    walker: impl FnMut(&Node) -> bool,
    flags: QueryTreeWalkerFlags,
) -> bool {
    let _ = (rtable, walker, flags);
    unimplemented!()
}

pub fn range_table_mutator(
    rtable: Vec<Box<Node>>,
    mutator: impl FnMut(Node) -> Node,
    flags: QueryTreeWalkerFlags,
) -> Vec<Box<Node>> {
    let _ = (rtable, mutator, flags);
    unimplemented!()
}

pub fn range_table_entry_walker(
    rte: &crate::nodes::parsenodes::RangeTblEntry,
    walker: impl FnMut(&Node) -> bool,
    flags: QueryTreeWalkerFlags,
) -> bool {
    let _ = (rte, walker, flags);
    unimplemented!()
}

pub fn query_or_expression_tree_walker(
    node: &Node,
    walker: impl FnMut(&Node) -> bool,
    flags: QueryTreeWalkerFlags,
) -> bool {
    let _ = (node, walker, flags);
    unimplemented!()
}

pub fn query_or_expression_tree_mutator(
    node: Node,
    mutator: impl FnMut(Node) -> Node,
    flags: QueryTreeWalkerFlags,
) -> Node {
    let _ = (node, mutator, flags);
    unimplemented!()
}

pub fn raw_expression_tree_walker(node: &Node, walker: impl FnMut(&Node) -> bool) -> bool {
    let _ = (node, walker);
    unimplemented!()
}

// planstate_tree_walker operates over executor PlanState; the callback becomes
// `FnMut(&PlanState) -> bool`.
pub fn planstate_tree_walker(
    planstate: &PlanState,
    walker: impl FnMut(&PlanState) -> bool,
) -> bool {
    let _ = (planstate, walker);
    unimplemented!()
}

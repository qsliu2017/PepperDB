//! Translated from PostgreSQL src/include/nodes/nodeFuncs.h

use bitflags::bitflags;

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::Query;
use crate::nodes::primnodes::{BoolExprType, CoercionForm, OpExpr, ScalarArrayOpExpr};
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

// Callback types -> closures. C threaded a `void *context`; a Rust closure
// captures that state directly, so the context param disappears.
// - check_function_callback:        FnMut(Oid) -> bool
// - tree_walker_callback:           FnMut(&Node) -> bool
// - planstate_tree_walker_callback: FnMut(&PlanState) -> bool
// - tree_mutator_callback:          FnMut(Node) -> Node

pub fn exprType(expr: &Node) -> Oid {
    unimplemented!()
}

pub fn exprTypmod(expr: &Node) -> i32 {
    unimplemented!()
}

/// Returns whether the expr is a length coercion; the coerced typmod is the
/// payload when so.
pub fn exprIsLengthCoercion(expr: &Node) -> Option<i32> {
    unimplemented!()
}

pub fn applyRelabelType(
    arg: Box<Node>,
    rtype: Oid,
    rtypmod: i32,
    rcollid: Oid,
    rformat: CoercionForm,
    rlocation: i32,
    overwrite_ok: bool,
) -> Box<Node> {
    unimplemented!()
}

pub fn relabel_to_typmod(expr: Box<Node>, typmod: i32) -> Box<Node> {
    unimplemented!()
}

pub fn strip_implicit_coercions(node: Box<Node>) -> Box<Node> {
    unimplemented!()
}

pub fn expression_returns_set(clause: &Node) -> bool {
    unimplemented!()
}

pub fn exprCollation(expr: &Node) -> Oid {
    unimplemented!()
}

pub fn exprInputCollation(expr: &Node) -> Oid {
    unimplemented!()
}

pub fn exprSetCollation(expr: &mut Node, collation: Oid) {
    unimplemented!()
}

pub fn exprSetInputCollation(expr: &mut Node, inputcollation: Oid) {
    unimplemented!()
}

pub fn exprLocation(expr: &Node) -> i32 {
    unimplemented!()
}

pub fn fix_opfuncids(node: &mut Node) {
    unimplemented!()
}

pub fn set_opfuncid(opexpr: &mut OpExpr) {
    unimplemented!()
}

pub fn set_sa_opfuncid(opexpr: &mut ScalarArrayOpExpr) {
    unimplemented!()
}

/// Is clause a FuncExpr clause?
pub fn is_funcclause(clause: Option<&Node>) -> bool {
    matches!(clause, Some(Node::FuncExpr(_)))
}

/// Is clause an OpExpr clause?
pub fn is_opclause(clause: Option<&Node>) -> bool {
    matches!(clause, Some(Node::OpExpr(_)))
}

/// Extract left arg of a binary opclause, or only arg of a unary opclause.
pub fn get_leftop(clause: &Node) -> Option<&Node> {
    match clause {
        Node::OpExpr(e) => e.args.first().map(|a| a.as_ref()),
        _ => None,
    }
}

/// Extract right arg of a binary opclause (None if unary).
pub fn get_rightop(clause: &Node) -> Option<&Node> {
    match clause {
        Node::OpExpr(e) if e.args.len() >= 2 => e.args.get(1).map(|a| a.as_ref()),
        _ => None,
    }
}

/// Is clause an AND clause?
pub fn is_andclause(clause: Option<&Node>) -> bool {
    matches!(clause, Some(Node::BoolExpr(e)) if e.boolop == BoolExprType::AND_EXPR)
}

/// Is clause an OR clause?
pub fn is_orclause(clause: Option<&Node>) -> bool {
    matches!(clause, Some(Node::BoolExpr(e)) if e.boolop == BoolExprType::OR_EXPR)
}

/// Is clause a NOT clause?
pub fn is_notclause(clause: Option<&Node>) -> bool {
    matches!(clause, Some(Node::BoolExpr(e)) if e.boolop == BoolExprType::NOT_EXPR)
}

/// Extract argument from a clause known to be a NOT clause.
pub fn get_notclausearg(notclause: &Node) -> Option<&Node> {
    match notclause {
        Node::BoolExpr(e) => e.args.first().map(|a| a.as_ref()),
        _ => None,
    }
}

pub fn check_functions_in_node(node: &Node, checker: impl FnMut(Oid) -> bool) -> bool {
    unimplemented!()
}

pub fn expression_tree_walker(node: &Node, walker: impl FnMut(&Node) -> bool) -> bool {
    unimplemented!()
}

pub fn expression_tree_mutator(node: Node, mutator: impl FnMut(Node) -> Node) -> Node {
    unimplemented!()
}

pub fn query_tree_walker(
    query: &Query,
    walker: impl FnMut(&Node) -> bool,
    flags: QueryTreeWalkerFlags,
) -> bool {
    unimplemented!()
}

pub fn query_tree_mutator(
    query: Query,
    mutator: impl FnMut(Node) -> Node,
    flags: QueryTreeWalkerFlags,
) -> Box<Query> {
    unimplemented!()
}

pub fn range_table_walker(
    rtable: &[Box<Node>],
    walker: impl FnMut(&Node) -> bool,
    flags: QueryTreeWalkerFlags,
) -> bool {
    unimplemented!()
}

pub fn range_table_mutator(
    rtable: Vec<Box<Node>>,
    mutator: impl FnMut(Node) -> Node,
    flags: QueryTreeWalkerFlags,
) -> Vec<Box<Node>> {
    unimplemented!()
}

pub fn range_table_entry_walker(
    rte: &crate::nodes::parsenodes::RangeTblEntry,
    walker: impl FnMut(&Node) -> bool,
    flags: QueryTreeWalkerFlags,
) -> bool {
    unimplemented!()
}

pub fn query_or_expression_tree_walker(
    node: &Node,
    walker: impl FnMut(&Node) -> bool,
    flags: QueryTreeWalkerFlags,
) -> bool {
    unimplemented!()
}

pub fn query_or_expression_tree_mutator(
    node: Node,
    mutator: impl FnMut(Node) -> Node,
    flags: QueryTreeWalkerFlags,
) -> Node {
    unimplemented!()
}

pub fn raw_expression_tree_walker(node: &Node, walker: impl FnMut(&Node) -> bool) -> bool {
    unimplemented!()
}

// C forward-declares `struct PlanState` to avoid including execnodes.h. That
// header isn't translated yet, so mirror the forward decl locally.
// TODO(struct-forward): repoint to crate::nodes::execnodes::PlanState in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::execnodes::PlanState in Phase 2")]
#[derive(Debug)]
pub struct PlanState {
    _private: (),
}

// planstate_tree_walker operates over executor PlanState; the callback becomes
// `FnMut(&PlanState) -> bool`.
#[allow(deprecated)]
pub fn planstate_tree_walker(
    planstate: &PlanState,
    walker: impl FnMut(&PlanState) -> bool,
) -> bool {
    unimplemented!()
}

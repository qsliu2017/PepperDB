//! Var node manipulation routines. Translated from
//! backend/optimizer/util/var.c (disposition: leaf for the M7 inner-join path;
//! the join-alias / group-expr flattening grows later).
//!
//! Non-type-centric free functions; bodies here, re-exported from
//! `crate::optimizer::optimizer` (the C declarations live in optimizer.h) under
//! the C names.
//!
//! The recursive walkers mirror PG's `*_walker` functions: a context struct
//! threads the accumulator + `sublevels_up`, the `Var` / `CurrentOfExpr` /
//! `PlaceHolderVar` / `Query` tags are handled specially, and generic recursion
//! delegates to `expression_tree_walker` with a closure that re-enters the
//! walker (`expression_tree_walker(node, |n| walk(n, ctx))`).
//!
//! Staging (rules.md s4): a `Query` subnode would recurse through
//! `query_tree_walker`, which is not translated yet; for M7 inner-join clauses
//! (`OpExpr(Var, Var)`, `Const`, `BoolExpr`, `FuncExpr`, `RelabelType`) no Query
//! subnode is reachable, so that arm calls the (staged) `query_tree_walker`.
//! `flatten_join_alias_vars` / `flatten_group_exprs` need RTE_JOIN/RTE_GROUP
//! infra not present yet and remain stubbed in `optimizer.rs`.

#![allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: the by-value Option<Node>/Bitmapset args mirror the optimizer.h \
              declarations these bodies are re-exported under; the walkers only read the node"
)]

use crate::access::sysattr::FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER;
use crate::backend::nodes::nodeFuncs::expression_tree_walker;
use crate::nodes::bitmapset::{bms_add_member, bms_add_members, Bitmapset};
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{PlannerInfo, Relids};
use crate::nodes::primnodes::{Index, VarReturningType};
use crate::optimizer::optimizer::PullVarClauseFlags;

/// Panic for a var.c path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

// ---------------------------------------------------------------------------
// pull_varnos
// ---------------------------------------------------------------------------

struct PullVarnosContext<'a> {
    varnos: Relids,
    root: Option<&'a PlannerInfo>,
    sublevels_up: i32,
}

/// PG `pull_varnos`: the set of all distinct varnos present in a parsetree.
/// Only varnos that reference level-zero rtable entries are considered. The
/// result includes outer-join relids in `Var.varnullingrels` /
/// `PlaceHolderVar.phnullingrels`.
pub fn pull_varnos(root: &mut PlannerInfo, node: Option<Node>) -> Bitmapset {
    pull_varnos_of_level(root, node, 0)
}

/// PG `pull_varnos_of_level`: like `pull_varnos`, but only Vars of the
/// specified level are considered.
pub fn pull_varnos_of_level(root: &mut PlannerInfo, node: Option<Node>, levelsup: i32) -> Bitmapset {
    pull_varnos_impl(Some(root), node, levelsup)
}

/// Shared body. PG allows `root == NULL` when PHV processing isn't needed; the
/// `&mut PlannerInfo` header signature is only borrowed immutably here (the
/// walker reads `root.placeholder_array`), so re-borrow as `&PlannerInfo`.
fn pull_varnos_impl(root: Option<&PlannerInfo>, node: Option<Node>, levelsup: i32) -> Bitmapset {
    let mut context = PullVarnosContext {
        varnos: Bitmapset::default(),
        root,
        sublevels_up: levelsup,
    };
    if let Some(node) = node.as_ref() {
        // query_or_expression_tree_walker: a bare expression does not increment
        // sublevels_up; a top-level Query is handled inside the walker.
        pull_varnos_walker(node, &mut context);
    }
    context.varnos
}

fn pull_varnos_walker(node: &Node, context: &mut PullVarnosContext) -> bool {
    match node {
        Node::Var(var) => {
            if var.varlevelsup as i32 == context.sublevels_up {
                context.varnos = bms_add_member(std::mem::take(&mut context.varnos), var.varno);
                if let Some(nr) = var.varnullingrels.as_ref() {
                    context.varnos = bms_add_members(std::mem::take(&mut context.varnos), nr);
                }
            }
            false
        }
        Node::CurrentOfExpr(cexpr) => {
            if context.sublevels_up == 0 {
                context.varnos =
                    bms_add_member(std::mem::take(&mut context.varnos), cexpr.cvarno as i32);
            }
            false
        }
        Node::PlaceHolderVar(phv) => {
            // If a PHV is not of the target query level (or no root), recurse
            // into its expression to look for vars of the target level.
            if let Some(root) = context.root.filter(|_| phv.phlevelsup as i32 == context.sublevels_up)
            {
                // Ideally the PHV contributes its ph_eval_at set, but that may
                // not be computed yet; if there is no PlaceHolderInfo, fall back
                // to the syntactic phrels. (The ph_eval_at translation corner
                // cases of PG are deferred; the conservative phrels path is
                // correct for the M7 reachable shape.)
                let phinfo = if phv.phlevelsup == 0 {
                    root.placeholder_array.get(phv.phid).and_then(|p| p.as_ref())
                } else {
                    None
                };
                match phinfo {
                    None => {
                        if let Some(phrels) = phv.phrels.as_ref() {
                            context.varnos =
                                bms_add_members(std::mem::take(&mut context.varnos), phrels);
                        }
                    }
                    Some(phinfo) => {
                        if let Some(eval_at) = phinfo.ph_eval_at.as_ref() {
                            context.varnos =
                                bms_add_members(std::mem::take(&mut context.varnos), eval_at);
                        }
                    }
                }
                if let Some(phnr) = phv.phnullingrels.as_ref() {
                    context.varnos = bms_add_members(std::mem::take(&mut context.varnos), phnr);
                }
                return false; // don't recurse into expression
            }
            expression_tree_walker(node, |n| pull_varnos_walker(n, context))
        }
        Node::Query(_) => {
            // Recurse into RTE subquery / not-yet-planned sublink subquery.
            not_yet_reachable("pull_varnos_walker: Query recursion (query_tree_walker)");
        }
        _ => expression_tree_walker(node, |n| pull_varnos_walker(n, context)),
    }
}

// ---------------------------------------------------------------------------
// pull_varattnos
// ---------------------------------------------------------------------------

struct PullVarattnosContext {
    varattnos: Bitmapset,
    varno: Index,
}

/// PG `pull_varattnos`: the distinct attribute numbers of Vars of the given
/// `varno` (rtable level zero) present in `node`, added to the initial
/// `varattnos` (C out-param). Attribute numbers are offset by
/// `FirstLowInvalidHeapAttributeNumber` so system attributes fit in the bitmap.
pub fn pull_varattnos(node: Option<Node>, varno: Index, varattnos: Bitmapset) -> Bitmapset {
    let mut context = PullVarattnosContext { varattnos, varno };
    if let Some(node) = node.as_ref() {
        pull_varattnos_walker(node, &mut context);
    }
    context.varattnos
}

fn pull_varattnos_walker(node: &Node, context: &mut PullVarattnosContext) -> bool {
    match node {
        Node::Var(var) => {
            if var.varno as usize == context.varno && var.varlevelsup == 0 {
                let bit = i32::from(var.varattno) - i32::from(FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER);
                context.varattnos = bms_add_member(std::mem::take(&mut context.varattnos), bit);
            }
            false
        }
        Node::Query(_) => not_yet_reachable("pull_varattnos_walker: unexpected unplanned Query"),
        _ => expression_tree_walker(node, |n| pull_varattnos_walker(n, context)),
    }
}

// ---------------------------------------------------------------------------
// pull_vars_of_level
// ---------------------------------------------------------------------------

struct PullVarsContext {
    vars: Vec<Node>,
    sublevels_up: i32,
}

/// PG `pull_vars_of_level`: a list of all Vars (and PlaceHolderVars) referencing
/// the specified query level. The Vars are cloned into the list.
pub fn pull_vars_of_level(node: Option<Node>, levelsup: i32) -> Vec<Node> {
    let mut context = PullVarsContext { vars: Vec::new(), sublevels_up: levelsup };
    if let Some(node) = node.as_ref() {
        pull_vars_walker(node, &mut context);
    }
    context.vars
}

fn pull_vars_walker(node: &Node, context: &mut PullVarsContext) -> bool {
    match node {
        Node::Var(var) => {
            if var.varlevelsup as i32 == context.sublevels_up {
                context.vars.push(node.clone());
            }
            false
        }
        Node::PlaceHolderVar(phv) => {
            if phv.phlevelsup as i32 == context.sublevels_up {
                context.vars.push(node.clone());
            }
            // don't look into the contained expression
            false
        }
        Node::Query(_) => {
            not_yet_reachable("pull_vars_walker: Query recursion (query_tree_walker)");
        }
        _ => expression_tree_walker(node, |n| pull_vars_walker(n, context)),
    }
}

// ---------------------------------------------------------------------------
// contain_var_clause
// ---------------------------------------------------------------------------

/// PG `contain_var_clause`: whether `node` contains any Var of the current query
/// level. Does not examine subqueries (use only after sublink reduction).
pub fn contain_var_clause(node: Option<Node>) -> bool {
    node.as_ref().is_some_and(contain_var_clause_walker)
}

fn contain_var_clause_walker(node: &Node) -> bool {
    match node {
        Node::Var(var) => var.varlevelsup == 0,
        Node::CurrentOfExpr(_) => true,
        Node::PlaceHolderVar(phv) => {
            if phv.phlevelsup == 0 {
                return true;
            }
            // else fall through to check the contained expr
            expression_tree_walker(node, contain_var_clause_walker)
        }
        _ => expression_tree_walker(node, contain_var_clause_walker),
    }
}

// ---------------------------------------------------------------------------
// contain_vars_of_level
// ---------------------------------------------------------------------------

/// PG `contain_vars_of_level`: whether `node` contains any Var of the specified
/// query level. Recurses into sublinks; may be invoked directly on a Query.
pub fn contain_vars_of_level(node: Option<Node>, levelsup: i32) -> bool {
    let mut sublevels_up = levelsup;
    node.as_ref().is_some_and(|n| contain_vars_of_level_walker(n, &mut sublevels_up))
}

fn contain_vars_of_level_walker(node: &Node, sublevels_up: &mut i32) -> bool {
    match node {
        Node::Var(var) => var.varlevelsup as i32 == *sublevels_up,
        Node::CurrentOfExpr(_) => *sublevels_up == 0,
        Node::PlaceHolderVar(phv) => {
            if phv.phlevelsup as i32 == *sublevels_up {
                return true;
            }
            // else fall through to check the contained expr
            expression_tree_walker(node, |n| contain_vars_of_level_walker(n, sublevels_up))
        }
        Node::Query(_) => {
            not_yet_reachable("contain_vars_of_level_walker: Query recursion (query_tree_walker)");
        }
        _ => expression_tree_walker(node, |n| contain_vars_of_level_walker(n, sublevels_up)),
    }
}

// ---------------------------------------------------------------------------
// contain_vars_returning_old_or_new
// ---------------------------------------------------------------------------

/// PG `contain_vars_returning_old_or_new`: whether `node` contains any Var (of
/// the current level) whose varreturningtype is OLD or NEW, or any current-level
/// ReturningExpr. Does not examine subqueries.
pub fn contain_vars_returning_old_or_new(node: Option<Node>) -> bool {
    node.as_ref().is_some_and(contain_vars_returning_old_or_new_walker)
}

fn contain_vars_returning_old_or_new_walker(node: &Node) -> bool {
    match node {
        Node::Var(var) => {
            var.varlevelsup == 0 && var.varreturningtype != VarReturningType::DEFAULT
        }
        Node::ReturningExpr(re) => re.retlevelsup == 0,
        _ => expression_tree_walker(node, contain_vars_returning_old_or_new_walker),
    }
}

// ---------------------------------------------------------------------------
// locate_var_of_level
// ---------------------------------------------------------------------------

struct LocateVarOfLevelContext {
    var_location: i32,
    sublevels_up: i32,
}

/// PG `locate_var_of_level`: the parse location of any Var of the specified query
/// level, or -1 if none (or all have unknown location). Recurses into sublinks.
pub fn locate_var_of_level(node: Option<Node>, levelsup: i32) -> i32 {
    let mut context = LocateVarOfLevelContext { var_location: -1, sublevels_up: levelsup };
    if let Some(node) = node.as_ref() {
        locate_var_of_level_walker(node, &mut context);
    }
    context.var_location
}

fn locate_var_of_level_walker(node: &Node, context: &mut LocateVarOfLevelContext) -> bool {
    match node {
        Node::Var(var) => {
            if var.varlevelsup as i32 == context.sublevels_up && var.location >= 0 {
                context.var_location = var.location;
                return true; // abort traversal
            }
            false
        }
        // CurrentOfExpr doesn't carry location; nothing we can do.
        Node::CurrentOfExpr(_) => false,
        // No extra code for PlaceHolderVar; just look in contained expr.
        Node::Query(_) => {
            not_yet_reachable("locate_var_of_level_walker: Query recursion (query_tree_walker)");
        }
        _ => expression_tree_walker(node, |n| locate_var_of_level_walker(n, context)),
    }
}

// ---------------------------------------------------------------------------
// pull_var_clause
// ---------------------------------------------------------------------------

struct PullVarClauseContext {
    varlist: Vec<Node>,
    flags: PullVarClauseFlags,
}

/// PG `pull_var_clause`: all Var nodes from an expression clause. Aggrefs,
/// WindowFuncs and PlaceHolderVars are included/recursed-into/error per `flags`
/// (see `PullVarClauseFlags`). CurrentOfExpr is ignored. Does not examine
/// subqueries (use only after sublink reduction). The nodes are cloned.
pub fn pull_var_clause(node: Option<Node>, flags: PullVarClauseFlags) -> Vec<Node> {
    // Caller must not specify both INCLUDE and RECURSE for the same kind.
    crate::assert!(
        !flags.contains(PullVarClauseFlags::INCLUDE_AGGREGATES | PullVarClauseFlags::RECURSE_AGGREGATES)
    );
    crate::assert!(
        !flags.contains(PullVarClauseFlags::INCLUDE_WINDOWFUNCS | PullVarClauseFlags::RECURSE_WINDOWFUNCS)
    );
    crate::assert!(
        !flags.contains(PullVarClauseFlags::INCLUDE_PLACEHOLDERS | PullVarClauseFlags::RECURSE_PLACEHOLDERS)
    );

    let mut context = PullVarClauseContext { varlist: Vec::new(), flags };
    if let Some(node) = node.as_ref() {
        pull_var_clause_walker(node, &mut context);
    }
    context.varlist
}

fn pull_var_clause_walker(node: &Node, context: &mut PullVarClauseContext) -> bool {
    let flags = context.flags;
    match node {
        Node::Var(var) => {
            if var.varlevelsup != 0 {
                crate::elog!(
                    crate::utils::elog::ERROR,
                    "Upper-level Var found where not expected"
                );
            }
            context.varlist.push(node.clone());
            false
        }
        Node::Aggref(agg) => {
            if agg.agglevelsup != 0 {
                crate::elog!(
                    crate::utils::elog::ERROR,
                    "Upper-level Aggref found where not expected"
                );
            }
            if flags.contains(PullVarClauseFlags::INCLUDE_AGGREGATES) {
                context.varlist.push(node.clone());
                false
            } else if flags.contains(PullVarClauseFlags::RECURSE_AGGREGATES) {
                expression_tree_walker(node, |n| pull_var_clause_walker(n, context))
            } else {
                crate::elog!(crate::utils::elog::ERROR, "Aggref found where not expected");
                false
            }
        }
        Node::GroupingFunc(grp) => {
            if grp.agglevelsup != 0 {
                crate::elog!(
                    crate::utils::elog::ERROR,
                    "Upper-level GROUPING found where not expected"
                );
            }
            if flags.contains(PullVarClauseFlags::INCLUDE_AGGREGATES) {
                context.varlist.push(node.clone());
                false
            } else if flags.contains(PullVarClauseFlags::RECURSE_AGGREGATES) {
                expression_tree_walker(node, |n| pull_var_clause_walker(n, context))
            } else {
                crate::elog!(crate::utils::elog::ERROR, "GROUPING found where not expected");
                false
            }
        }
        Node::WindowFunc(_) => {
            // WindowFuncs have no levelsup field to check.
            if flags.contains(PullVarClauseFlags::INCLUDE_WINDOWFUNCS) {
                context.varlist.push(node.clone());
                false
            } else if flags.contains(PullVarClauseFlags::RECURSE_WINDOWFUNCS) {
                expression_tree_walker(node, |n| pull_var_clause_walker(n, context))
            } else {
                crate::elog!(crate::utils::elog::ERROR, "WindowFunc found where not expected");
                false
            }
        }
        Node::PlaceHolderVar(phv) => {
            if phv.phlevelsup != 0 {
                crate::elog!(
                    crate::utils::elog::ERROR,
                    "Upper-level PlaceHolderVar found where not expected"
                );
            }
            if flags.contains(PullVarClauseFlags::INCLUDE_PLACEHOLDERS) {
                context.varlist.push(node.clone());
                false
            } else if flags.contains(PullVarClauseFlags::RECURSE_PLACEHOLDERS) {
                expression_tree_walker(node, |n| pull_var_clause_walker(n, context))
            } else {
                crate::elog!(
                    crate::utils::elog::ERROR,
                    "PlaceHolderVar found where not expected"
                );
                false
            }
        }
        _ => expression_tree_walker(node, |n| pull_var_clause_walker(n, context)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::attnum::AttrNumber;
    use crate::catalog::genbki::{BOOLOID, INT4OID};
    use crate::nodes::bitmapset::bms_is_member;
    use crate::nodes::primnodes::{Const, OpExpr, Var};
    use crate::postgres::Datum;
    use crate::postgres_ext::InvalidOid;

    fn make_var(varno: i32, varattno: AttrNumber) -> Node {
        Node::Var(Box::new(Var {
            varno,
            varattno,
            vartype: INT4OID,
            vartypmod: -1,
            varcollid: InvalidOid,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: VarReturningType::DEFAULT,
            varnosyn: varno as usize,
            varattnosyn: varattno,
            location: -1,
        }))
    }

    fn int_const(v: usize) -> Node {
        Node::Const(Box::new(Const {
            consttype: INT4OID,
            consttypmod: -1,
            constcollid: InvalidOid,
            constlen: 4,
            constvalue: Datum(v),
            constisnull: false,
            constbyval: true,
            location: -1,
        }))
    }

    fn eq_opexpr(left: Node, right: Node) -> Node {
        Node::OpExpr(Box::new(OpExpr {
            opno: InvalidOid,
            opfuncid: InvalidOid,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args: vec![left, right],
            location: -1,
        }))
    }

    #[test]
    fn pull_varnos_two_rel_join_clause() {
        // a.x = b.y : OpExpr(Var{varno=1}, Var{varno=2}). No PHV -> root=None.
        let clause = eq_opexpr(make_var(1, 1), make_var(2, 1));
        let relids = pull_varnos_impl(None, Some(clause), 0);
        assert!(bms_is_member(1, &relids));
        assert!(bms_is_member(2, &relids));
        assert!(!bms_is_member(3, &relids));
    }

    #[test]
    fn pull_varnos_single_rel_clause() {
        // a.x = 5 : OpExpr(Var{varno=1}, Const). No PHV -> root=None.
        let clause = eq_opexpr(make_var(1, 1), int_const(5));
        let relids = pull_varnos_impl(None, Some(clause), 0);
        assert!(bms_is_member(1, &relids));
        assert!(!bms_is_member(2, &relids));
    }

    #[test]
    fn pull_varattnos_offsets_by_first_low_invalid() {
        // Var{varno=1, varattno=3} -> bit (3 - FirstLowInvalidHeapAttributeNumber)
        let var = make_var(1, 3);
        let attnos = pull_varattnos(Some(var), 1, Bitmapset::default());
        let expect_bit = 3 - i32::from(FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER);
        assert!(bms_is_member(expect_bit, &attnos));
    }

    #[test]
    fn pull_varattnos_ignores_other_varno() {
        let var = make_var(2, 3);
        let attnos = pull_varattnos(Some(var), 1, Bitmapset::default());
        assert!(attnos.is_empty());
    }

    #[test]
    fn contain_var_clause_detects_var() {
        let with_var = eq_opexpr(make_var(1, 1), int_const(5));
        assert!(contain_var_clause(Some(with_var)));

        let const_only = eq_opexpr(int_const(1), int_const(2));
        assert!(!contain_var_clause(Some(const_only)));
    }
}

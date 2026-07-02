//! General-purpose manipulations of node trees. Translated from
//! backend/nodes/nodeFuncs.c.
//!
//! These are non-type-centric free functions (`exprType`, `exprTypmod`,
//! `exprCollation`, the expression tree walker/mutator framework). Bodies live
//! here as snake_case `pub fn`s with the C symbol in the doc comment; the header
//! `crate::nodes::nodeFuncs` re-exports each under its C name.
//!
//! Disposition: `grow`. nodeFuncs.c is a `switch (nodeTag)` dispatcher. PG's
//! `Node` is a Rust enum here, and Rust matches must be exhaustive, so the
//! dispatchers handle every arm whose carried struct is already defined and whose
//! logic is self-contained, and route arms that reach a not-yet-translated
//! subsystem (array-type promotion, `Query`/subplan typing, ...) through a single
//! clearly-marked `not_yet_reachable` arm. Each arm is complete or absent; none is
//! half-written. The walker/mutator framework is translated for the M1-reachable
//! shape (Const leaves, TargetEntry recursion) and grows per milestone.

use crate::catalog::genbki::{BOOLOID, DEFAULT_COLLATION_OID, INT4OID, TEXTOID, XMLOID};
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{BoolExprType, XmlExprOp};
use crate::postgres_ext::{InvalidOid, Oid};

/// Panic for a node tag whose `exprType`/`exprTypmod`/`exprCollation` arm reaches
/// a subsystem not yet translated for the current milestone. Distinct from
/// `elog(ERROR, "unrecognized node type")`: that is for a genuinely bad tag,
/// whereas this marks a known-but-not-yet-reachable tag (staging per rules.md s4).
#[cold]
fn not_yet_reachable(what: &str, node: &Node) -> ! {
    unimplemented!("{what}: node tag not yet reachable for this milestone: {node:?}")
}

/// PG `exprType`: returns the OID of the type of the expression's result.
#[allow(
    clippy::match_same_arms,
    reason = "1:1 with PG's per-nodetag switch; merging same-valued arms loses the mapping"
)]
/// PG `exprType` for a SubLink: boolean for EXISTS/ANY/ALL/ROWCOMPARE; the single
/// output column's type for EXPR (read off the analyzed sub-Query).
fn sublink_expr_type(sl: &crate::nodes::primnodes::SubLink) -> Oid {
    use crate::nodes::primnodes::SubLinkType;
    match sl.subLinkType {
        SubLinkType::EXPR_SUBLINK => {
            let Some(Node::Query(q)) = sl.subselect.as_ref() else {
                return InvalidOid;
            };
            q.targetList
                .iter()
                .find_map(|n| match n {
                    Node::TargetEntry(t) if !t.resjunk => {
                        Some(t.expr.as_ref().map_or(InvalidOid, exprType))
                    }
                    _ => None,
                })
                .unwrap_or(InvalidOid)
        }
        _ => BOOLOID,
    }
}

#[allow(
    clippy::match_same_arms,
    reason = "1:1 PG exprType: several distinct node kinds independently yield BOOLOID; \
              merging them loses the per-node mapping"
)]
pub fn exprType(expr: &Node) -> Oid {
    match expr {
        Node::Var(v) => v.vartype,
        Node::Const(c) => c.consttype,
        Node::Param(p) => p.paramtype,
        Node::Aggref(a) => a.aggtype,
        Node::GroupingFunc(_) => INT4OID,
        Node::WindowFunc(w) => w.wintype,
        Node::MergeSupportFunc(m) => m.msftype,
        Node::SubscriptingRef(s) => s.refrestype,
        Node::FuncExpr(f) => f.funcresulttype,
        Node::NamedArgExpr(n) => n.arg.as_ref().map_or(InvalidOid, exprType),
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => o.opresulttype,
        Node::ScalarArrayOpExpr(_) | Node::BoolExpr(_) | Node::RowCompareExpr(_) => BOOLOID,
        Node::FieldSelect(f) => f.resulttype,
        Node::FieldStore(f) => f.resulttype,
        Node::RelabelType(r) => r.resulttype,
        Node::CoerceViaIO(c) => c.resulttype,
        Node::ArrayCoerceExpr(a) => a.resulttype,
        Node::ConvertRowtypeExpr(c) => c.resulttype,
        Node::CollateExpr(c) => c.arg.as_ref().map_or(InvalidOid, exprType),
        Node::CaseExpr(c) => c.casetype,
        Node::CaseTestExpr(c) => c.typeId,
        Node::ArrayExpr(a) => a.array_typeid,
        Node::RowExpr(r) => r.row_typeid,
        Node::CoalesceExpr(c) => c.coalescetype,
        Node::MinMaxExpr(m) => m.minmaxtype,
        Node::SQLValueFunction(s) => s.r#type,
        Node::XmlExpr(x) => match x.op {
            XmlExprOp::DOCUMENT => BOOLOID,
            XmlExprOp::XMLSERIALIZE => TEXTOID,
            _ => XMLOID,
        },
        Node::JsonValueExpr(j) => j.formatted_expr.as_ref().map_or(InvalidOid, exprType),
        Node::JsonConstructorExpr(j) => j.returning.as_ref().map_or(InvalidOid, |r| r.typid),
        Node::JsonIsPredicate(_) => BOOLOID,
        Node::JsonExpr(j) => j.returning.as_ref().map_or(InvalidOid, |r| r.typid),
        Node::JsonBehavior(b) => b.expr.as_ref().map_or(InvalidOid, exprType),
        Node::NullTest(_) | Node::BooleanTest(_) | Node::CurrentOfExpr(_) => BOOLOID,
        Node::CoerceToDomain(c) => c.resulttype,
        Node::CoerceToDomainValue(c) => c.typeId,
        Node::SetToDefault(s) => s.typeId,
        Node::NextValueExpr(n) => n.typeId,
        Node::InferenceElem(i) => i.expr.as_ref().map_or(InvalidOid, exprType),
        Node::ReturningExpr(r) => r.retexpr.as_ref().map_or(InvalidOid, exprType),
        Node::PlaceHolderVar(p) => exprType(&p.phexpr),
        // PG `exprType` SubLink arm (M12, step 44): EXISTS/ANY/ALL/ROWCOMPARE are
        // boolean; an EXPR sub-select's type is its single output column's type
        // (read off the analyzed sub-Query's targetlist). ARRAY/CTE/MULTIEXPR and the
        // SubPlan/AlternativeSubPlan typing (firstColType / record) grow later.
        Node::SubLink(sl) => sublink_expr_type(sl),
        Node::SubPlan(sp) => match sp.subLinkType {
            crate::nodes::primnodes::SubLinkType::EXPR_SUBLINK => sp.firstColType,
            _ => BOOLOID,
        },
        Node::AlternativeSubPlan(_) => not_yet_reachable("exprType", expr),
        other => {
            crate::elog!(
                crate::utils::elog::ERROR,
                format!("unrecognized node type: {other:?}")
            );
            InvalidOid // keep compiler quiet (elog(ERROR) does not return)
        }
    }
}

/// PG `exprTypmod`: returns the type-specific modifier of the expression's result
/// type, if it can be determined; otherwise -1.
pub fn exprTypmod(expr: &Node) -> i32 {
    match expr {
        Node::Var(v) => v.vartypmod,
        Node::Const(c) => c.consttypmod,
        Node::Param(p) => p.paramtypmod,
        Node::SubscriptingRef(s) => s.reftypmod,
        Node::NamedArgExpr(n) => n.arg.as_ref().map_or(-1, exprTypmod),
        Node::NullIfExpr(n) => n.args.first().map_or(-1, exprTypmod),
        Node::FieldSelect(f) => f.resulttypmod,
        Node::RelabelType(r) => r.resulttypmod,
        Node::ArrayCoerceExpr(a) => a.resulttypmod,
        Node::CollateExpr(c) => c.arg.as_ref().map_or(-1, exprTypmod),
        Node::CaseExpr(c) => case_expr_typmod(c),
        Node::CaseTestExpr(c) => c.typeMod,
        Node::ArrayExpr(a) => array_expr_typmod(a),
        Node::CoalesceExpr(c) => coalesce_expr_typmod(c),
        Node::MinMaxExpr(m) => min_max_expr_typmod(m),
        Node::SQLValueFunction(s) => s.typmod,
        Node::CoerceToDomain(c) => c.resulttypmod,
        Node::CoerceToDomainValue(c) => c.typeMod,
        Node::SetToDefault(s) => s.typeMod,
        Node::PlaceHolderVar(p) => exprTypmod(&p.phexpr),
        Node::ReturningExpr(r) => r.retexpr.as_ref().map_or(-1, exprTypmod),
        Node::FuncExpr(_) => {
            // Length-coercion functions report a coerced typmod; that path needs
            // exprIsLengthCoercion (syscache), not translated yet.
            exprIsLengthCoercion(expr).unwrap_or(-1)
        }
        // SubLink/SubPlan/AlternativeSubPlan need Query typing not translated yet.
        Node::SubLink(_) | Node::SubPlan(_) | Node::AlternativeSubPlan(_) => {
            not_yet_reachable("exprTypmod", expr)
        }
        // All other node types have an indeterminate typmod.
        _ => -1,
    }
}

fn case_expr_typmod(cexpr: &crate::nodes::primnodes::CaseExpr) -> i32 {
    // If all alternatives agree on type/typmod, return that typmod, else -1.
    let Some(defresult) = cexpr.defresult.as_ref() else {
        return -1;
    };
    let casetype = cexpr.casetype;
    if exprType(defresult) != casetype {
        return -1;
    }
    let typmod = exprTypmod(defresult);
    if typmod < 0 {
        return -1;
    }
    for w in &cexpr.args {
        let Node::CaseWhen(w) = w else {
            continue;
        };
        let Some(result) = w.result.as_ref() else {
            return -1;
        };
        if exprType(result) != casetype || exprTypmod(result) != typmod {
            return -1;
        }
    }
    typmod
}

fn array_expr_typmod(arrayexpr: &crate::nodes::primnodes::ArrayExpr) -> i32 {
    let Some(first) = arrayexpr.elements.first() else {
        return -1;
    };
    let typmod = exprTypmod(first);
    if typmod < 0 {
        return -1;
    }
    let commontype = if arrayexpr.multidims {
        arrayexpr.array_typeid
    } else {
        arrayexpr.element_typeid
    };
    for e in &arrayexpr.elements {
        if exprType(e) != commontype || exprTypmod(e) != typmod {
            return -1;
        }
    }
    typmod
}

fn coalesce_expr_typmod(cexpr: &crate::nodes::primnodes::CoalesceExpr) -> i32 {
    let coalescetype = cexpr.coalescetype;
    let Some(first) = cexpr.args.first() else {
        return -1;
    };
    let typmod = exprTypmod(first);
    if typmod < 0 {
        return -1;
    }
    for e in &cexpr.args {
        if exprType(e) != coalescetype || exprTypmod(e) != typmod {
            return -1;
        }
    }
    typmod
}

fn min_max_expr_typmod(mexpr: &crate::nodes::primnodes::MinMaxExpr) -> i32 {
    let minmaxtype = mexpr.minmaxtype;
    let Some(first) = mexpr.args.first() else {
        return -1;
    };
    let typmod = exprTypmod(first);
    if typmod < 0 {
        return -1;
    }
    for e in &mexpr.args {
        if exprType(e) != minmaxtype || exprTypmod(e) != typmod {
            return -1;
        }
    }
    typmod
}

/// PG `exprCollation`: returns the collation OID of the expression's result.
#[allow(
    clippy::match_same_arms,
    reason = "1:1 with PG's per-nodetag switch; merging same-valued arms loses the mapping"
)]
pub fn exprCollation(expr: &Node) -> Oid {
    match expr {
        Node::Var(v) => v.varcollid,
        Node::Const(c) => c.constcollid,
        Node::Param(p) => p.paramcollid,
        Node::Aggref(a) => a.aggcollid,
        Node::GroupingFunc(_) => InvalidOid,
        Node::WindowFunc(w) => w.wincollid,
        Node::MergeSupportFunc(m) => m.msfcollid,
        Node::SubscriptingRef(s) => s.refcollid,
        Node::FuncExpr(f) => f.funccollid,
        Node::NamedArgExpr(n) => n.arg.as_ref().map_or(InvalidOid, exprCollation),
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => o.opcollid,
        // boolean / record results have no collation
        Node::ScalarArrayOpExpr(_) | Node::BoolExpr(_) | Node::RowCompareExpr(_) => InvalidOid,
        Node::FieldSelect(f) => f.resultcollid,
        // FieldStore's result is composite -> no collation
        Node::FieldStore(_) => InvalidOid,
        Node::RelabelType(r) => r.resultcollid,
        Node::CoerceViaIO(c) => c.resultcollid,
        Node::ArrayCoerceExpr(a) => a.resultcollid,
        // ConvertRowtypeExpr's result is composite -> no collation
        Node::ConvertRowtypeExpr(_) => InvalidOid,
        Node::CollateExpr(c) => c.collOid,
        Node::CaseExpr(c) => c.casecollid,
        Node::CaseTestExpr(c) => c.collation,
        Node::ArrayExpr(a) => a.array_collid,
        // RowExpr's result is composite -> no collation
        Node::RowExpr(_) => InvalidOid,
        Node::CoalesceExpr(c) => c.coalescecollid,
        Node::MinMaxExpr(m) => m.minmaxcollid,
        Node::SQLValueFunction(_) | Node::CurrentOfExpr(_) | Node::NextValueExpr(_) => InvalidOid,
        Node::XmlExpr(x) => {
            // XMLSERIALIZE returns text (default collation); others have none.
            if x.op == XmlExprOp::XMLSERIALIZE {
                DEFAULT_COLLATION_OID
            } else {
                InvalidOid
            }
        }
        Node::JsonValueExpr(j) => j.formatted_expr.as_ref().map_or(InvalidOid, exprCollation),
        Node::JsonConstructorExpr(j) => j.coercion.as_ref().map_or(InvalidOid, exprCollation),
        Node::JsonIsPredicate(_) => InvalidOid,
        Node::JsonExpr(j) => j.collation,
        Node::JsonBehavior(b) => b.expr.as_ref().map_or(InvalidOid, exprCollation),
        Node::NullTest(_) | Node::BooleanTest(_) => InvalidOid,
        Node::CoerceToDomain(c) => c.resultcollid,
        Node::CoerceToDomainValue(c) => c.collation,
        Node::SetToDefault(s) => s.collation,
        Node::InferenceElem(i) => i.expr.as_ref().map_or(InvalidOid, exprCollation),
        Node::ReturningExpr(r) => r.retexpr.as_ref().map_or(InvalidOid, exprCollation),
        Node::PlaceHolderVar(p) => exprCollation(&p.phexpr),
        // SubLink/SubPlan/AlternativeSubPlan need Query typing not translated yet.
        Node::SubLink(_) | Node::SubPlan(_) | Node::AlternativeSubPlan(_) => {
            not_yet_reachable("exprCollation", expr)
        }
        other => {
            crate::elog!(
                crate::utils::elog::ERROR,
                format!("unrecognized node type: {other:?}")
            );
            InvalidOid // keep compiler quiet (elog(ERROR) does not return)
        }
    }
}

/// PG `exprIsLengthCoercion`: whether the expr is a length coercion; the coerced
/// typmod is the payload (`Some(typmod)`) when so, else `None`.
///
/// A scalar length coercion is a cast-context FuncExpr with 2-3 args whose second
/// arg is a non-NULL int4 Const (the typmod). M4's single-argument cast functions
/// are *type* coercions, not length coercions, so they return `None`. (The array
/// ArrayCoerceExpr arm grows with arrays.)
pub fn exprIsLengthCoercion(expr: &Node) -> Option<i32> {
    use crate::catalog::genbki::INT4OID;
    use crate::nodes::primnodes::CoercionForm;
    let Node::FuncExpr(func) = expr else {
        return None;
    };
    if !matches!(func.funcformat, CoercionForm::EXPLICIT_CAST | CoercionForm::IMPLICIT_CAST) {
        return None;
    }
    let nargs = func.args.len();
    if !(2..=3).contains(&nargs) {
        return None;
    }
    let Node::Const(second) = &func.args[1] else {
        return None;
    };
    if second.consttype != INT4OID || second.constisnull {
        return None;
    }
    Some(crate::postgres::DatumGetInt32(second.constvalue))
}

/// PG `exprSetCollation`: set the collation of an expression's result. M3 reaches
/// the nodes the operator/function transform produces (`OpExpr` / `FuncExpr` /
/// `BoolExpr`); for boolean-result nodes (BoolExpr, and comparison OpExprs whose
/// result is bool) PG asserts the collation is InvalidOid, which holds for the M3
/// int4/bool path. The remaining collation-bearing arms grow with their node types.
pub fn exprSetCollation(expr: &mut Node, collation: Oid) {
    match expr {
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => o.opcollid = collation,
        Node::FuncExpr(f) => f.funccollid = collation,
        // BoolExpr's result is boolean (uncollatable); nothing to set.
        Node::BoolExpr(_) => {}
        other => not_yet_reachable("exprSetCollation", other),
    }
}

/// PG `exprLocation`: the parse location of an expression (for error positioning).
/// M3 covers the nodes the operator/function transform produces and their leaves;
/// the remaining arms grow with their node types. `-1` means "unknown".
#[must_use]
pub fn exprLocation(expr: &Node) -> i32 {
    match expr {
        Node::Var(v) => v.location,
        Node::Const(c) => c.location,
        Node::Param(p) => p.location,
        Node::FuncExpr(f) => f.location,
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => o.location,
        Node::BoolExpr(b) => b.location,
        Node::A_Expr(a) => a.location,
        Node::A_Const(c) => c.location,
        Node::ColumnRef(c) => c.location,
        Node::FuncCall(fc) => fc.location,
        // Unknown / not-yet-covered nodes (incl. TargetEntry, which has no location)
        // report "unknown location" (-1), PG's safe default.
        _ => -1,
    }
}

/// PG `expression_returns_set` (+ `expression_returns_set_walker`): does the
/// expression contain a set-returning function call (`FuncExpr.funcretset` /
/// `OpExpr.opretset`) at any level? Aggref/WindowFunc arguments cannot contain
/// SRFs (parser-enforced in PG), so they short-circuit false, as in C. Uses its
/// own recursion (not `expression_tree_walker`, whose untranslated arms are grow
/// guards): a node kind without an arm here answers false, and a missed SRF is
/// backstopped by the executor's set-valued-function ereport.
#[must_use]
pub fn expression_returns_set(expr: &Node) -> bool {
    fn walker(node: &Node) -> bool {
        match node {
            Node::FuncExpr(f) if f.funcretset => true,
            Node::OpExpr(o) if o.opretset => true,
            Node::FuncExpr(f) => f.args.iter().any(walker),
            Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => {
                o.args.iter().any(walker)
            }
            Node::BoolExpr(b) => b.args.iter().any(walker),
            Node::TargetEntry(t) => t.expr.as_ref().is_some_and(walker),
            Node::RelabelType(r) => r.arg.as_ref().is_some_and(walker),
            Node::CoerceViaIO(c) => c.arg.as_ref().is_some_and(walker),
            Node::BooleanTest(b) => b.arg.as_ref().is_some_and(walker),
            Node::CaseExpr(c) => {
                c.arg.as_ref().is_some_and(walker)
                    || c.args.iter().any(walker)
                    || c.defresult.as_ref().is_some_and(walker)
            }
            Node::CaseWhen(w) => {
                w.expr.as_ref().is_some_and(walker) || w.result.as_ref().is_some_and(walker)
            }
            Node::CoalesceExpr(c) => c.args.iter().any(walker),
            Node::MinMaxExpr(m) => m.args.iter().any(walker),
            Node::RowExpr(r) => r.args.iter().any(walker),
            // Aggref / WindowFunc arguments cannot contain SRFs (C's explicit
            // early-outs); other leaves and untranslated containers answer false.
            _ => false,
        }
    }
    walker(expr)
}

// ---------------------------------------------------------------------------
// Expression tree walker / mutator framework.
//
// C threads a `void *context` and recurses via a function pointer; a Rust closure
// captures the context, so the public API takes `impl FnMut(&Node) -> bool`
// (walker) / `impl FnMut(Node) -> Node` (mutator). The internal
// `walk_node`/`mutate_node` recursors are generic over the same `F` and call
// themselves with `&mut *f`, so the closure type threads through unchanged.
// ---------------------------------------------------------------------------

/// PG `expression_tree_walker`: visit a node's sub-nodes, calling `walker` on each.
/// `walker` returns `true` to abort the walk early (propagated up).
pub fn expression_tree_walker(node: &Node, mut walker: impl FnMut(&Node) -> bool) -> bool {
    walk_node(node, &mut walker)
}

fn walk_node<F: FnMut(&Node) -> bool>(node: &Node, walker: &mut F) -> bool {
    // The walker has already visited `node`; recurse into its sub-nodes only.
    // Node lists (`Vec<Node>`) are iterated, calling the walker on each element.
    match node {
        // Primitive node types with no expression subnodes.
        Node::Var(_)
        | Node::Const(_)
        | Node::Param(_)
        | Node::CaseTestExpr(_)
        | Node::SQLValueFunction(_)
        | Node::CoerceToDomainValue(_)
        | Node::SetToDefault(_)
        | Node::CurrentOfExpr(_)
        | Node::NextValueExpr(_)
        | Node::RangeTblRef(_)
        | Node::SortGroupClause(_)
        | Node::MergeSupportFunc(_) => false,
        Node::TargetEntry(t) => t.expr.as_ref().is_some_and(&mut *walker),
        Node::FromExpr(f) => {
            f.fromlist.iter().any(&mut *walker) || f.quals.as_ref().is_some_and(&mut *walker)
        }
        Node::BoolExpr(b) => b.args.iter().any(&mut *walker),
        Node::OpExpr(o) | Node::DistinctExpr(o) | Node::NullIfExpr(o) => {
            o.args.iter().any(&mut *walker)
        }
        Node::FuncExpr(f) => f.args.iter().any(&mut *walker),
        Node::RelabelType(r) => r.arg.as_ref().is_some_and(&mut *walker),
        Node::BooleanTest(b) => b.arg.as_ref().is_some_and(&mut *walker),
        // PlaceHolderVar: recurse into the represented expression (var.c's
        // walkers visit a PHV's phexpr when not short-circuiting on it).
        Node::PlaceHolderVar(phv) => walker(&phv.phexpr),
        // Remaining node tags are not reachable for the current milestone; the
        // walker grows complete arms per milestone (rules.md s4 / README grow).
        other => not_yet_reachable("expression_tree_walker", other),
    }
}

/// PG `expression_tree_mutator`: rebuild a node by applying `mutator` to each
/// sub-node, returning a new node. Leaf nodes are returned (flat-copied).
pub fn expression_tree_mutator(node: Node, mut mutator: impl FnMut(Node) -> Node) -> Node {
    mutate_node(node, &mut mutator)
}

fn mutate_node<F: FnMut(Node) -> Node>(node: Node, mutator: &mut F) -> Node {
    match node {
        // Primitive node types: a flat copy (no sub-nodes to mutate).
        n @ (Node::Var(_)
        | Node::Const(_)
        | Node::Param(_)
        | Node::CaseTestExpr(_)
        | Node::SQLValueFunction(_)
        | Node::CoerceToDomainValue(_)
        | Node::SetToDefault(_)
        | Node::CurrentOfExpr(_)
        | Node::NextValueExpr(_)
        | Node::RangeTblRef(_)
        | Node::SortGroupClause(_)
        | Node::MergeSupportFunc(_)) => n,
        Node::TargetEntry(mut t) => {
            t.expr = t.expr.map(&mut *mutator);
            Node::TargetEntry(t)
        }
        Node::FromExpr(mut f) => {
            f.fromlist = mutate_list(f.fromlist, mutator);
            f.quals = f.quals.map(&mut *mutator);
            Node::FromExpr(f)
        }
        Node::BoolExpr(mut b) => {
            b.args = mutate_list(b.args, mutator);
            Node::BoolExpr(b)
        }
        Node::OpExpr(mut o) => {
            o.args = mutate_list(o.args, mutator);
            Node::OpExpr(o)
        }
        Node::FuncExpr(mut f) => {
            f.args = mutate_list(f.args, mutator);
            Node::FuncExpr(f)
        }
        Node::RelabelType(mut r) => {
            r.arg = r.arg.map(&mut *mutator);
            Node::RelabelType(r)
        }
        Node::BooleanTest(mut b) => {
            b.arg = b.arg.map(&mut *mutator);
            Node::BooleanTest(b)
        }
        other => not_yet_reachable("expression_tree_mutator", &other),
    }
}

/// Apply `mutator` to each element of a node list (`Vec<Node>`).
fn mutate_list<F: FnMut(Node) -> Node>(list: Vec<Node>, mutator: &mut F) -> Vec<Node> {
    list.into_iter().map(mutator).collect()
}

// ---------------------------------------------------------------------------
// Small clause-inspection helpers (PG: nodeFuncs.h inline functions). These were
// already implemented in the header; bodies live here and the header re-exports.
// ---------------------------------------------------------------------------

/// PG `is_funcclause`: is `clause` a `FuncExpr` clause?
pub fn is_funcclause(clause: Option<&Node>) -> bool {
    matches!(clause, Some(Node::FuncExpr(_)))
}

/// PG `is_opclause`: is `clause` an `OpExpr` clause?
pub fn is_opclause(clause: Option<&Node>) -> bool {
    matches!(clause, Some(Node::OpExpr(_)))
}

/// PG `get_leftop`: extract the left arg of a binary opclause (or only arg of a
/// unary one).
pub fn get_leftop(clause: &Node) -> Option<&Node> {
    match clause {
        Node::OpExpr(e) => e.args.first(),
        _ => None,
    }
}

/// PG `get_rightop`: extract the right arg of a binary opclause (None if unary).
pub fn get_rightop(clause: &Node) -> Option<&Node> {
    match clause {
        Node::OpExpr(e) if e.args.len() >= 2 => e.args.get(1),
        _ => None,
    }
}

/// PG `is_andclause`: is `clause` an AND clause?
pub fn is_andclause(clause: Option<&Node>) -> bool {
    matches!(clause, Some(Node::BoolExpr(e)) if e.boolop == BoolExprType::AND_EXPR)
}

/// PG `is_orclause`: is `clause` an OR clause?
pub fn is_orclause(clause: Option<&Node>) -> bool {
    matches!(clause, Some(Node::BoolExpr(e)) if e.boolop == BoolExprType::OR_EXPR)
}

/// PG `is_notclause`: is `clause` a NOT clause?
pub fn is_notclause(clause: Option<&Node>) -> bool {
    matches!(clause, Some(Node::BoolExpr(e)) if e.boolop == BoolExprType::NOT_EXPR)
}

/// PG `get_notclausearg`: extract the argument from a known NOT clause.
pub fn get_notclausearg(notclause: &Node) -> Option<&Node> {
    match notclause {
        Node::BoolExpr(e) => e.args.first(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::primnodes::{BoolExpr, Const, OpExpr, TargetEntry, Var, VarReturningType};
    use crate::postgres::Datum;

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

    fn a_var() -> Node {
        Node::Var(Box::new(Var {
            varno: 1,
            varattno: 1,
            vartype: BOOLOID,
            vartypmod: 7,
            varcollid: DEFAULT_COLLATION_OID,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: VarReturningType::DEFAULT,
            varnosyn: 1,
            varattnosyn: 1,
            location: -1,
        }))
    }

    #[test]
    fn expr_type_const_and_var() {
        assert_eq!(exprType(&int_const(1)), INT4OID);
        assert_eq!(exprType(&a_var()), BOOLOID);
    }

    #[test]
    fn expr_typmod_const_and_var() {
        assert_eq!(exprTypmod(&int_const(1)), -1);
        assert_eq!(exprTypmod(&a_var()), 7);
    }

    #[test]
    fn expr_collation_const_and_var() {
        assert_eq!(exprCollation(&int_const(1)), InvalidOid);
        assert_eq!(exprCollation(&a_var()), DEFAULT_COLLATION_OID);
    }

    #[test]
    fn expr_typmod_unknown_node_is_minus_one() {
        // A BoolExpr has no determinable typmod.
        let b = Node::BoolExpr(Box::new(BoolExpr {
            boolop: BoolExprType::AND_EXPR,
            args: Vec::new(),
            location: -1,
        }));
        assert_eq!(exprTypmod(&b), -1);
    }

    #[test]
    fn walker_visits_target_entry_expr() {
        let tle = Node::TargetEntry(Box::new(TargetEntry {
            expr: Some(int_const(9)),
            resno: 1,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: InvalidOid,
            resorigcol: 0,
            resjunk: false,
        }));
        let mut seen_const = false;
        let aborted = expression_tree_walker(&tle, |n| {
            if matches!(n, Node::Const(_)) {
                seen_const = true;
            }
            false
        });
        assert!(seen_const, "walker should have visited the Const child");
        assert!(!aborted);
    }

    #[test]
    fn walker_early_abort_propagates() {
        let tle = Node::TargetEntry(Box::new(TargetEntry {
            expr: Some(int_const(9)),
            resno: 1,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: InvalidOid,
            resorigcol: 0,
            resjunk: false,
        }));
        let aborted = expression_tree_walker(&tle, |_| true);
        assert!(aborted, "returning true from the walker aborts the walk");
    }

    #[test]
    fn walker_leaf_const_has_no_subnodes() {
        let mut calls = 0;
        let aborted = expression_tree_walker(&int_const(1), |_| {
            calls += 1;
            false
        });
        assert_eq!(calls, 0, "a Const leaf has no expression subnodes");
        assert!(!aborted);
    }

    #[test]
    fn mutator_rewrites_target_entry_child() {
        let tle = Node::TargetEntry(Box::new(TargetEntry {
            expr: Some(int_const(1)),
            resno: 1,
            resname: None,
            ressortgroupref: 0,
            resorigtbl: InvalidOid,
            resorigcol: 0,
            resjunk: false,
        }));
        // Replace every Const's value with 100.
        let out = expression_tree_mutator(tle, |n| match n {
            Node::Const(mut c) => {
                c.constvalue = Datum(100);
                Node::Const(c)
            }
            other => other,
        });
        let Node::TargetEntry(t) = out else {
            panic!("expected TargetEntry");
        };
        let Some(child) = t.expr else {
            panic!("expected expr");
        };
        let Node::Const(c) = child else {
            panic!("expected Const");
        };
        assert_eq!(c.constvalue, Datum(100));
    }

    #[test]
    fn clause_helpers() {
        let op = Node::OpExpr(Box::new(OpExpr {
            opno: InvalidOid,
            opfuncid: InvalidOid,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args: vec![int_const(1), int_const(2)],
            location: -1,
        }));
        assert!(is_opclause(Some(&op)));
        assert!(!is_funcclause(Some(&op)));
        assert!(get_leftop(&op).is_some());
        assert!(get_rightop(&op).is_some());

        let and = Node::BoolExpr(Box::new(BoolExpr {
            boolop: BoolExprType::AND_EXPR,
            args: vec![int_const(1)],
            location: -1,
        }));
        assert!(is_andclause(Some(&and)));
        assert!(!is_orclause(Some(&and)));
        assert!(!is_notclause(Some(&and)));
    }
}

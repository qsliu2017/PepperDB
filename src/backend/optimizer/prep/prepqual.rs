//! Routines for preprocessing qualification expressions. Translated from
//! backend/optimizer/prep/prepqual.c.
//!
//! `canonicalize_qual` is run on top-level WHERE / JOIN-ON clauses (and CHECK
//! constraints) before `deconstruct_jointree`. The input is assumed to have
//! already passed through `eval_const_expressions`, so it is AND/OR-flat; this
//! module preserves that flatness while pulling up redundant subclauses in
//! OR-of-AND trees and dropping NULL constants from the top-level structure.
//!
//! `negate_clause` (used by `eval_const_expressions` to fold a NOT node) is the
//! NOT-pushdown: it applies DeMorgan's laws and operator/null-test negation.
//!
//! FULL: `negate_clause`, `canonicalize_qual`, `pull_ands`, `pull_ors`,
//! `find_duplicate_ors`, `process_duplicate_ors` are all translated.
//!
//! Staged (`not_yet_reachable`, rules.md s4): the `equal`/`list_member` node
//! comparisons inside `process_duplicate_ors` reach the not-yet-translated
//! `nodes::equal`. They only fire for OR-of-AND quals that share a common
//! subclause (the inverse OR distributive law); the AND/OR flattening and
//! NOT-pushdown core works without them. Operator/SAOP negation reaches
//! `get_negator` (lsyscache stub); for those we fall back to a plain NOT node
//! (semantically correct, just unsimplified).

use crate::nodes::makefuncs::{makeBoolConst, make_andclause, make_notclause, make_orclause};
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{BoolExprType, NullTestType, BoolTestType};
use crate::postgres::DatumGetBool;

/// Panic for a prepqual path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `negate_clause`: negate a boolean expression, eliminating the NOT node by
/// logical simplification where possible (DeMorgan, operator/null-test/bool-test
/// negation). If we can't simplify, we tack on an explicit NOT node.
pub fn negate_clause(node: Node) -> Node {
    match node {
        Node::Const(ref c) => {
            // NOT NULL is still NULL; otherwise flip the boolean.
            if c.constisnull {
                makeBoolConst(false, true)
            } else {
                makeBoolConst(!DatumGetBool(c.constvalue), false)
            }
        }
        Node::OpExpr(_) | Node::ScalarArrayOpExpr(_) => {
            // Negate the operator if it has a negator: (NOT (< A B)) => (>= A B),
            // and x = ANY(list) => x <> ALL(list). get_negator reaches the
            // lsyscache stub, so fall through to an explicit NOT for now.
            // TODO(syscache): use get_negator to flip the operator in place.
            make_notclause(node)
        }
        Node::BoolExpr(expr) => match expr.boolop {
            // (NOT (AND A B)) => (OR (NOT A) (NOT B)); flatness preserved.
            BoolExprType::AND_EXPR => {
                let nargs = expr.args.into_iter().map(negate_clause).collect();
                make_orclause(nargs)
            }
            // (NOT (OR A B)) => (AND (NOT A) (NOT B)); flatness preserved.
            BoolExprType::OR_EXPR => {
                let nargs = expr.args.into_iter().map(negate_clause).collect();
                make_andclause(nargs)
            }
            // NOT underneath NOT: they cancel.
            BoolExprType::NOT_EXPR => expr
                .args
                .into_iter()
                .next()
                .unwrap_or_else(|| not_yet_reachable("negate_clause: empty NOT arg")),
        },
        Node::NullTest(ref expr) if !expr.argisrow => {
            // Scalar IS NULL <-> IS NOT NULL (rowtype case is not a logical inverse).
            let mut newexpr = expr.clone();
            newexpr.nulltesttype = if expr.nulltesttype == NullTestType::NULL {
                NullTestType::NOT_NULL
            } else {
                NullTestType::NULL
            };
            Node::NullTest(newexpr)
        }
        Node::BooleanTest(ref expr) => {
            let mut newexpr = expr.clone();
            newexpr.booltesttype = match expr.booltesttype {
                BoolTestType::TRUE => BoolTestType::NOT_TRUE,
                BoolTestType::NOT_TRUE => BoolTestType::TRUE,
                BoolTestType::FALSE => BoolTestType::NOT_FALSE,
                BoolTestType::NOT_FALSE => BoolTestType::FALSE,
                BoolTestType::UNKNOWN => BoolTestType::NOT_UNKNOWN,
                BoolTestType::NOT_UNKNOWN => BoolTestType::UNKNOWN,
            };
            Node::BooleanTest(newexpr)
        }
        // Else we don't know how to simplify; tack on an explicit NOT node.
        other => make_notclause(other),
    }
}

/// PG `canonicalize_qual`: convert a top-level WHERE/JOIN-ON (or CHECK if
/// `is_check`) qual to the most useful form. Assumes the input is already
/// AND/OR-flat (via `eval_const_expressions`). Pulls up redundant subclauses in
/// OR-of-AND trees and removes NULL constants in the top-level structure.
pub fn canonicalize_qual(qual: Option<Node>, is_check: bool) -> Option<Node> {
    let qual = qual?;
    // This should not be invoked on quals in implicit-AND (List) format; we have
    // no List node so there is nothing to assert.
    Some(find_duplicate_ors(qual, is_check))
}

/// PG `pull_ands`: recursively flatten nested AND clauses into a single
/// and-clause arglist. Input is the arglist of an AND clause.
fn pull_ands(andlist: Vec<Node>) -> Vec<Node> {
    let mut out_list = Vec::with_capacity(andlist.len());
    for subexpr in andlist {
        match subexpr {
            Node::BoolExpr(b) if b.boolop == BoolExprType::AND_EXPR => {
                out_list.extend(pull_ands(b.args));
            }
            other => out_list.push(other),
        }
    }
    out_list
}

/// PG `pull_ors`: recursively flatten nested OR clauses into a single or-clause
/// arglist. Input is the arglist of an OR clause.
fn pull_ors(orlist: Vec<Node>) -> Vec<Node> {
    let mut out_list = Vec::with_capacity(orlist.len());
    for subexpr in orlist {
        match subexpr {
            Node::BoolExpr(b) if b.boolop == BoolExprType::OR_EXPR => {
                out_list.extend(pull_ors(b.args));
            }
            other => out_list.push(other),
        }
    }
    out_list
}

/// Is `node` a constant whose boolean value (treating NULL per `is_check`)
/// means the clause should be dropped from / collapses its enclosing AND/OR?
/// Returns `Some(replacement)` if the AND/OR reduces to a constant, else `None`
/// after deciding whether to keep the arg. This is folded into the loops below.
enum ConstAction {
    /// Drop this arg from the list.
    Drop,
    /// The whole AND/OR reduces to this expression.
    Reduce(Node),
    /// Keep this arg in the list.
    Keep(Node),
}

/// Within an OR, decide what to do with a constant arg.
fn or_const_action(arg: Node, is_check: bool) -> ConstAction {
    let Node::Const(ref c) = arg else { return ConstAction::Keep(arg) };
    if is_check {
        // Within OR in CHECK, drop constant FALSE.
        if !c.constisnull && !DatumGetBool(c.constvalue) {
            return ConstAction::Drop;
        }
        // Constant TRUE or NULL, so OR reduces to TRUE.
        ConstAction::Reduce(makeBoolConst(true, false))
    } else {
        // Within OR in WHERE, drop constant FALSE or NULL.
        if c.constisnull || !DatumGetBool(c.constvalue) {
            return ConstAction::Drop;
        }
        // Constant TRUE, so OR reduces to TRUE.
        ConstAction::Reduce(arg)
    }
}

/// Within an AND, decide what to do with a constant arg.
fn and_const_action(arg: Node, is_check: bool) -> ConstAction {
    let Node::Const(ref c) = arg else { return ConstAction::Keep(arg) };
    if is_check {
        // Within AND in CHECK, drop constant TRUE or NULL.
        if c.constisnull || DatumGetBool(c.constvalue) {
            return ConstAction::Drop;
        }
        // Constant FALSE, so AND reduces to FALSE.
        ConstAction::Reduce(arg)
    } else {
        // Within AND in WHERE, drop constant TRUE.
        if !c.constisnull && DatumGetBool(c.constvalue) {
            return ConstAction::Drop;
        }
        // Constant FALSE or NULL, so AND reduces to FALSE.
        ConstAction::Reduce(makeBoolConst(false, false))
    }
}

/// PG `find_duplicate_ors`: search the top-level AND/OR structure for OR clauses
/// the inverse OR distributive law applies to, removing NULL constants on the
/// way. AND/OR flatness is preserved.
fn find_duplicate_ors(qual: Node, is_check: bool) -> Node {
    match qual {
        Node::BoolExpr(b) if b.boolop == BoolExprType::OR_EXPR => {
            let mut orlist = Vec::with_capacity(b.args.len());
            for arg in b.args {
                let arg = find_duplicate_ors(arg, is_check);
                match or_const_action(arg, is_check) {
                    ConstAction::Drop => {}
                    ConstAction::Reduce(node) => return node,
                    ConstAction::Keep(node) => orlist.push(node),
                }
            }
            // Flatten any ORs pulled up to just below here, then look for dups.
            let orlist = pull_ors(orlist);
            process_duplicate_ors(orlist)
        }
        Node::BoolExpr(b) if b.boolop == BoolExprType::AND_EXPR => {
            let mut andlist = Vec::with_capacity(b.args.len());
            for arg in b.args {
                let arg = find_duplicate_ors(arg, is_check);
                match and_const_action(arg, is_check) {
                    ConstAction::Drop => {}
                    ConstAction::Reduce(node) => return node,
                    ConstAction::Keep(node) => andlist.push(node),
                }
            }
            // Flatten any ANDs introduced just below here.
            let mut andlist = pull_ands(andlist);
            // AND of no inputs reduces to TRUE.
            if andlist.is_empty() {
                return makeBoolConst(true, false);
            }
            // Single-expression AND just reduces to that expression.
            if andlist.len() == 1 {
                return andlist.remove(0);
            }
            make_andclause(andlist)
        }
        other => other,
    }
}

/// PG `process_duplicate_ors`: given a list of OR'ed exprs, try to apply the
/// inverse OR distributive law `((A AND B) OR (A AND C)) => (A AND (B OR C))`.
/// Returns the resulting expression (AND clause, OR clause, or a single subexpr).
fn process_duplicate_ors(mut orlist: Vec<Node>) -> Node {
    // OR of no inputs reduces to FALSE.
    if orlist.is_empty() {
        return makeBoolConst(false, false);
    }
    // Single-expression OR just reduces to that expression.
    if orlist.len() == 1 {
        return orlist.remove(0);
    }

    // Choose the shortest AND clause as the reference list. A non-AND clause is
    // treated as a one-element AND, which necessarily wins as shortest.
    let mut reference: Option<Vec<Node>> = None;
    for clause in &orlist {
        match clause {
            Node::BoolExpr(b) if b.boolop == BoolExprType::AND_EXPR => {
                let nclauses = b.args.len();
                if reference.as_ref().is_none_or(|r| nclauses < r.len()) {
                    reference = Some(b.args.clone());
                }
            }
            other => {
                reference = Some(vec![other.clone()]);
                break;
            }
        }
    }

    // Just in case, eliminate any duplicates in the reference list. (list_union
    // uses node equality; staged.) For a non-AND single-element reference there
    // is nothing to dedup, so only the AND case can reach the stub.
    let reference = reference.unwrap_or_default();
    let reference = list_union_empty(reference);

    // Check each reference element to see if it's in all the OR clauses.
    let mut winners: Vec<Node> = Vec::new();
    for refclause in &reference {
        let mut win = true;
        for clause in &orlist {
            match clause {
                Node::BoolExpr(b) if b.boolop == BoolExprType::AND_EXPR => {
                    if !list_member(&b.args, refclause) {
                        win = false;
                        break;
                    }
                }
                other => {
                    if !node_equal(refclause, other) {
                        win = false;
                        break;
                    }
                }
            }
        }
        if win {
            winners.push(refclause.clone());
        }
    }

    // If no winners, we can't transform the OR.
    if winners.is_empty() {
        return make_orclause(orlist);
    }

    // Generate a new OR list of the remaining sub-clauses. A degenerate empty
    // clause means the whole OR collapses to the winners alone.
    let mut neworlist: Vec<Node> = Vec::new();
    let mut degenerate = false;
    for clause in orlist {
        match clause {
            Node::BoolExpr(b) if b.boolop == BoolExprType::AND_EXPR => {
                let mut subclauses = list_difference(b.args, &winners);
                if subclauses.is_empty() {
                    degenerate = true;
                    break;
                } else if subclauses.len() == 1 {
                    neworlist.push(subclauses.remove(0));
                } else {
                    neworlist.push(make_andclause(subclauses));
                }
            }
            other => {
                if list_member(&winners, &other) {
                    degenerate = true;
                    break;
                }
                neworlist.push(other);
            }
        }
    }
    if degenerate {
        neworlist.clear();
    }

    // Append the reduced OR to the winners list (if not degenerate), preserving
    // AND/OR flatness.
    if !neworlist.is_empty() {
        if neworlist.len() == 1 {
            winners.push(neworlist.remove(0));
        } else {
            winners.push(make_orclause(pull_ors(neworlist)));
        }
    }

    // Return the constructed AND clause, wary of a single element and flatness.
    if winners.len() == 1 {
        winners.remove(0)
    } else {
        make_andclause(pull_ands(winners))
    }
}

/// PG `list_union(NIL, list)`: the elements of `list` with duplicates removed by
/// node equality. Reaches the not-yet-translated `nodes::equal` only when the
/// list has 2+ elements that might be equal; a one-element list is returned as-is.
fn list_union_empty(list: Vec<Node>) -> Vec<Node> {
    let mut out: Vec<Node> = Vec::with_capacity(list.len());
    for item in list {
        if !list_member(&out, &item) {
            out.push(item);
        }
    }
    out
}

/// PG `list_member`: is `target` equal (by node equality) to some element of
/// `list`? Reaches the `nodes::equal` stub.
fn list_member(list: &[Node], target: &Node) -> bool {
    list.iter().any(|item| node_equal(item, target))
}

/// PG `list_difference(list, exclude)`: elements of `list` not in `exclude`.
fn list_difference(list: Vec<Node>, exclude: &[Node]) -> Vec<Node> {
    list.into_iter().filter(|item| !list_member(exclude, item)).collect()
}

/// PG `equal` over two nodes. The general node-equality routine is staged
/// (`nodes::equal` is `unimplemented!`). This only fires on OR-of-AND quals that
/// might share a common subclause; AND/OR flattening does not reach it.
fn node_equal(a: &Node, b: &Node) -> bool {
    crate::nodes::nodes::equal(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::primnodes::{BoolExpr, Var, VarReturningType};
    use crate::postgres_ext::{InvalidOid, Oid};

    fn var(varno: i32) -> Node {
        Node::Var(Box::new(Var {
            varno,
            varattno: 1,
            vartype: Oid(23),
            vartypmod: -1,
            varcollid: InvalidOid,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: VarReturningType::DEFAULT,
            varnosyn: varno as usize,
            varattnosyn: 1,
            location: -1,
        }))
    }

    fn boolexpr(boolop: BoolExprType, args: Vec<Node>) -> Node {
        Node::BoolExpr(Box::new(BoolExpr { boolop, args, location: -1 }))
    }

    fn is_and(n: &Node) -> bool {
        matches!(n, Node::BoolExpr(b) if b.boolop == BoolExprType::AND_EXPR)
    }
    fn is_or(n: &Node) -> bool {
        matches!(n, Node::BoolExpr(b) if b.boolop == BoolExprType::OR_EXPR)
    }

    #[test]
    fn negate_not_and_is_or_of_nots() {
        // NOT (a AND b) => (NOT a) OR (NOT b)
        let and = boolexpr(BoolExprType::AND_EXPR, vec![var(1), var(2)]);
        let neg = negate_clause(and);
        assert!(is_or(&neg), "expected OR, got {neg:?}");
        let Node::BoolExpr(b) = &neg else { unreachable!() };
        assert_eq!(b.args.len(), 2);
        // each arg is a NOT clause
        for arg in &b.args {
            assert!(matches!(arg, Node::BoolExpr(x) if x.boolop == BoolExprType::NOT_EXPR));
        }
    }

    #[test]
    fn negate_not_or_is_and_of_nots() {
        // NOT (a OR b) => (NOT a) AND (NOT b)
        let or = boolexpr(BoolExprType::OR_EXPR, vec![var(1), var(2)]);
        let neg = negate_clause(or);
        assert!(is_and(&neg));
        let Node::BoolExpr(b) = &neg else { unreachable!() };
        assert_eq!(b.args.len(), 2);
    }

    #[test]
    fn negate_not_not_cancels() {
        // negate(NOT x) where NOT x is a BoolExpr NOT_EXPR cancels to x.
        let notx = make_notclause(var(1));
        let neg = negate_clause(notx);
        // the inner arg was var(1)
        assert!(matches!(neg, Node::Var(_)));
    }

    #[test]
    fn negate_const_true_is_false() {
        let neg = negate_clause(makeBoolConst(true, false));
        let Node::Const(c) = &neg else { panic!("expected Const") };
        assert!(!c.constisnull);
        assert!(!DatumGetBool(c.constvalue));
    }

    #[test]
    fn canonicalize_flattens_nested_and() {
        // a AND (b AND c) -> a AND b AND c (3-arg flat AND).
        let inner = boolexpr(BoolExprType::AND_EXPR, vec![var(2), var(3)]);
        let outer = boolexpr(BoolExprType::AND_EXPR, vec![var(1), inner]);
        let out = canonicalize_qual(Some(outer), false).unwrap();
        assert!(is_and(&out));
        let Node::BoolExpr(b) = &out else { unreachable!() };
        assert_eq!(b.args.len(), 3, "nested AND should be flattened to 3 args");
        // none of the args is itself an AND
        assert!(b.args.iter().all(|a| !is_and(a)));
    }

    #[test]
    fn canonicalize_drops_const_true_in_and() {
        // a AND TRUE -> a (in WHERE context).
        let and = boolexpr(BoolExprType::AND_EXPR, vec![var(1), makeBoolConst(true, false)]);
        let out = canonicalize_qual(Some(and), false).unwrap();
        // single-element AND reduces to the bare expression
        assert!(matches!(out, Node::Var(_)));
    }

    #[test]
    fn canonicalize_const_false_collapses_and() {
        // a AND FALSE -> FALSE (in WHERE context).
        let and = boolexpr(BoolExprType::AND_EXPR, vec![var(1), makeBoolConst(false, false)]);
        let out = canonicalize_qual(Some(and), false).unwrap();
        let Node::Const(c) = &out else { panic!("expected Const FALSE") };
        assert!(!c.constisnull && !DatumGetBool(c.constvalue));
    }

    #[test]
    fn canonicalize_empty_is_none() {
        assert!(canonicalize_qual(None, false).is_none());
    }

    #[test]
    fn canonicalize_leaves_simple_var() {
        let out = canonicalize_qual(Some(var(1)), false).unwrap();
        assert!(matches!(out, Node::Var(_)));
    }
}

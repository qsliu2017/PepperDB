//! Planner preprocessing for subqueries and jointrees. Translated from
//! backend/optimizer/prep/prepjointree.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::prep` under the C names.
//!
//! Disposition: `grow`. M2's live entry is `replace_empty_jointree`: a FROM-less
//! SELECT (or INSERT ... VALUES with no SELECT source) gets a single `RTE_RESULT`
//! RTE plus a `RangeTblRef` in the jointree, matching PG so a `SELECT 1` plans
//! with a one-entry rangetable. The sublink-pullup / subquery-pullup / outer-join
//! reduction / function-RTE inlining passes are grow guards (rules.md s4).

use crate::nodes::makefuncs::makeAlias;
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::parsenodes::{Query, RTEKind, RangeTblEntry};
use crate::nodes::primnodes::RangeTblRef;
use crate::postgres_ext::InvalidOid;

/// PG `replace_empty_jointree`: if the query's jointree fromlist is empty (a
/// FROM-less SELECT), inject one `RTE_RESULT` RTE and a `RangeTblRef` to it. A
/// top-level setop tree is left alone.
pub fn replace_empty_jointree(parse: &mut Query) {
    // Nothing to do if the jointree is already nonempty.
    let from_empty = match parse.jointree.as_ref() {
        Some(Node::FromExpr(f)) => f.fromlist.is_empty(),
        // No jointree behaves like an empty FROM.
        None => true,
        Some(_) => return,
    };
    if !from_empty {
        return;
    }
    // We mustn't change it in the top level of a setop tree, either.
    if parse.setOperations.is_some() {
        return;
    }

    // Create the RTE_RESULT RTE and append it to the rangetable.
    let rte = make_result_rte();
    parse.rtable.push(Node::RangeTblEntry(Box::new(rte)));
    let rti = parse.rtable.len() as i32;

    // Jam a RangeTblRef into the jointree's fromlist.
    let rtr = Node::RangeTblRef(Box::new(RangeTblRef { rtindex: rti }));
    match parse.jointree.as_mut() {
        Some(Node::FromExpr(f)) => f.fromlist = vec![rtr],
        _ => {
            parse.jointree = Some(Node::FromExpr(Box::new(
                crate::nodes::makefuncs::makeFromExpr(vec![rtr], None),
            )));
        }
    }
}

/// `makeNode(RangeTblEntry)` for an `RTE_RESULT` (eref `*RESULT*`).
fn make_result_rte() -> RangeTblEntry {
    let eref = makeAlias("*RESULT*", Vec::new());
    RangeTblEntry {
        alias: None,
        eref: Some(Box::new(eref)),
        rtekind: RTEKind::RESULT,
        relid: InvalidOid,
        inh: false,
        relkind: 0,
        rellockmode: 0,
        perminfoindex: 0,
        tablesample: None,
        subquery: None,
        security_barrier: false,
        jointype: JoinType::INNER,
        joinmergedcols: 0,
        joinaliasvars: Vec::new(),
        joinleftcols: Vec::new(),
        joinrightcols: Vec::new(),
        join_using_alias: None,
        functions: Vec::new(),
        funcordinality: false,
        tablefunc: None,
        values_lists: Vec::new(),
        ctename: None,
        ctelevelsup: 0,
        self_reference: false,
        coltypes: Vec::new(),
        coltypmods: Vec::new(),
        colcollations: Vec::new(),
        enrname: None,
        enrtuples: 0.0,
        groupexprs: Vec::new(),
        lateral: false,
        inFromCl: false,
        securityQuals: Vec::new(),
    }
}

//! Target-list manipulation utilities. Translated from
//! backend/optimizer/util/tlist.c.
//!
//! Leaf helpers operating on `TargetEntry` lists and `PathTarget`s. Non-type-
//! centric free functions; bodies here as snake_case `pub fn`s, re-exported from
//! `crate::optimizer::tlist` under the C names.
//!
//! Disposition: `full` for the leaf tlist helpers reachable on the M1 const
//! path (`tlist_member`, `make_pathtarget_from_tlist`, `make_tlist_from_pathtarget`,
//! `apply_tlist_labeling`). Helpers whose dependencies (grouping ops, SRF
//! splitting, sortgroupref-keyed PathTarget editing) are not yet translated
//! call their existing `unimplemented!()` stubs per rules.md s4; they are not
//! reached by the const SELECT and grow with later milestones.

use crate::nodes::makefuncs::makeTargetEntry;
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{PathTarget, QualCost, VolatileFunctionStatus};
use crate::nodes::primnodes::TargetEntry;
use crate::utils::elog::OrElog;

/// PG `tlist_member`: find the (first) member of the given tlist whose
/// expression equals the given expression. Result is NULL if no such member.
pub fn tlist_member(node: &Node, targetlist: &[TargetEntry]) -> Option<TargetEntry> {
    targetlist
        .iter()
        .find(|tle| tle.expr.as_deref() == Some(node))
        .cloned()
}

/// PG `apply_tlist_labeling`: apply the resname/ressortgroupref/resorigtbl/
/// resorigcol/resjunk labeling of `src_tlist` onto the matching entries of
/// `dest_tlist` (which must be the same length and same resnos). Used to copy
/// the original column decoration onto a planner-built tlist.
pub fn apply_tlist_labeling(dest_tlist: &mut [TargetEntry], src_tlist: &[TargetEntry]) {
    crate::assert!(dest_tlist.len() == src_tlist.len());
    for (dest_tle, src_tle) in dest_tlist.iter_mut().zip(src_tlist.iter()) {
        crate::assert!(dest_tle.resno == src_tle.resno);
        dest_tle.resname.clone_from(&src_tle.resname);
        dest_tle.ressortgroupref = src_tle.ressortgroupref;
        dest_tle.resorigtbl = src_tle.resorigtbl;
        dest_tle.resorigcol = src_tle.resorigcol;
        dest_tle.resjunk = src_tle.resjunk;
    }
}

/// PG `make_pathtarget_from_tlist`: construct a `PathTarget` equivalent to the
/// given tlist (carrying its exprs and sortgrouprefs). Volatility is left
/// UNKNOWN; cost/width are not set here (`create_pathtarget` adds those).
pub fn make_pathtarget_from_tlist(tlist: &[TargetEntry]) -> PathTarget {
    let exprs = tlist
        .iter()
        .map(|tle| tle.expr.clone().unwrap_or_error_with(|| "tlist entry has no expr"))
        .collect();
    let sortgrouprefs = tlist.iter().map(|tle| tle.ressortgroupref).collect();
    PathTarget {
        exprs,
        sortgrouprefs,
        cost: QualCost { startup: 0.0, per_tuple: 0.0 },
        width: 0,
        has_volatile_expr: VolatileFunctionStatus::UNKNOWN,
    }
}

/// PG `make_tlist_from_pathtarget`: construct a flat tlist (with resnos 1..n and
/// the PathTarget's sortgrouprefs) from a `PathTarget`. The reverse of
/// `make_pathtarget_from_tlist` (resnames are not preserved by either).
pub fn make_tlist_from_pathtarget(target: &PathTarget) -> Vec<TargetEntry> {
    let has_sortgrouprefs = !target.sortgrouprefs.is_empty();
    target
        .exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| {
            let mut tle = makeTargetEntry(Some(expr.clone()), (i + 1) as i16, None, false);
            if has_sortgrouprefs {
                tle.ressortgroupref = target.sortgrouprefs[i];
            }
            tle
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::genbki::INT4OID;
    use crate::nodes::primnodes::Const;
    use crate::postgres::Int32GetDatum;
    use crate::postgres_ext::InvalidOid;

    /// Build an int4 Const Node of the given value.
    fn int4(v: i32) -> Node {
        Node::Const(Box::new(Const {
            consttype: INT4OID,
            consttypmod: -1,
            constcollid: InvalidOid,
            constlen: 4,
            constvalue: Int32GetDatum(v),
            constisnull: false,
            constbyval: true,
            location: -1,
        }))
    }

    /// Build a one-column tlist over the given expr with resno 1.
    fn tlist_of(expr: Node) -> Vec<TargetEntry> {
        vec![makeTargetEntry(Some(Box::new(expr)), 1, Some("c".to_owned()), false)]
    }

    #[test]
    fn tlist_member_finds_matching_expr() {
        let tlist = tlist_of(int4(7));
        let needle = int4(7);
        let found = tlist_member(&needle, &tlist).expect("member with equal expr is found");
        assert_eq!(found.resno, 1);
        assert_eq!(found.resname.as_deref(), Some("c"));
    }

    #[test]
    fn tlist_member_misses_different_expr() {
        let tlist = tlist_of(int4(7));
        let needle = int4(8);
        assert!(tlist_member(&needle, &tlist).is_none());
    }

    #[test]
    fn pathtarget_tlist_round_trips() {
        // make_pathtarget_from_tlist then make_tlist_from_pathtarget recovers the
        // exprs and resnos (resnames are intentionally not preserved).
        let original = vec![
            makeTargetEntry(Some(Box::new(int4(1))), 1, Some("a".to_owned()), false),
            makeTargetEntry(Some(Box::new(int4(2))), 2, Some("b".to_owned()), false),
        ];
        let target = make_pathtarget_from_tlist(&original);
        assert_eq!(target.exprs.len(), 2);

        let back = make_tlist_from_pathtarget(&target);
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].resno, 1);
        assert_eq!(back[1].resno, 2);
        assert_eq!(back[0].expr.as_deref(), original[0].expr.as_deref());
        assert_eq!(back[1].expr.as_deref(), original[1].expr.as_deref());
    }
}

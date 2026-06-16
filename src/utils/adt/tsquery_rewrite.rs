//! src/backend/utils/adt/tsquery_rewrite.c
//!
//! Utilities for reconstructing (rewriting/substituting) a tsquery.
//!
//! `#include` mapping:
//!   postgres.h           -> crate::prelude::*  (Datum, palloc0/pfree, elog!/ereport!/
//!                           errmsg!, Assert, c-types, null_mut).
//!   catalog/pg_type.h    -> TSQUERYOID (only used by the SPI-driven tsquery_rewrite,
//!                           which is STUBBED; not declared here).
//!   executor/spi.h       -> SPI engine.  NOT PORTED.  Only `tsquery_rewrite` (the
//!                           2-arg-form-over-an-SQL-text entry) needs it, and its body
//!                           is STUBBED with unimplemented!()+TODO.
//!   miscadmin.h          -> check_stack_depth() (crate::utils::misc::stack_depth) and
//!                           CHECK_FOR_INTERRUPTS (stubbed no-op, see below).
//!   tsearch/ts_utils.h   -> the QTNode machinery (QTNode, QTN_* flags, QT2QTN, QTN2QT,
//!                           QTNFree, QTNTernary, QTNSort, QTNBinary, QTNCopy,
//!                           QTNClearFlags, QTNodeCompare, QTNEq) + the tsquery node
//!                           structs (QueryItem/QueryOperand/QueryOperator, QI_VAL/QI_OPR,
//!                           OP_*, TSQuery, GETQUERY/GETOPERAND, HDRSIZETQ, SET_VARSIZE
//!                           target) -- ALL imported from crate::utils::adt::tsquery_util.
//!   utils/builtins.h     -> text_to_cstring (crate::utils::adt::varlena); only used by
//!                           the STUBBED tsquery_rewrite.
//!   varatt.h             -> SET_VARSIZE (crate::varatt).
//!
//! Status:
//!   findeq / dofindsubquery / findsubquery / tsquery_rewrite_query  -- FULLY REAL.
//!   tsquery_rewrite  -- STUBBED body: it is driven by executor/spi.h (SPI_connect,
//!     SPI_prepare, SPI_cursor_open/fetch, SPI_getbinval, ...), none of which is ported.
//!     Signature kept; body is unimplemented!()+TODO.  The QTNode-level substitution it
//!     would perform is identical to the REAL findsubquery used by tsquery_rewrite_query.
//!
//! NOTE on argument numbering: in PostgreSQL the SQL function `ts_rewrite(query, target,
//! sample)` is bound to the C function `tsquery_rewrite_query`, and `ts_rewrite(query,
//! 'select ...')` is bound to `tsquery_rewrite`.  So `tsquery_rewrite_query` takes three
//! TSQuery args (query, ex, subst) and `tsquery_rewrite` takes (query TSQuery, in TEXT).
//! This matches the C source exactly (do not be confused by the function names).

use crate::prelude::*;
use crate::varatt::SET_VARSIZE;

use crate::utils::adt::tsquery_util::{
    QTNBinary, QTNClearFlags, QTNCopy, QTNEq, QTNFree, QTNSort, QTNTernary, QTNodeCompare, QTNode,
    QueryOperand, QueryOperator, GETOPERAND, GETQUERY, HDRSIZETQ, OP_AND, OP_NOT, OP_OR, QI_OPR,
    QI_VAL, QT2QTN, QTN2QT, QTN_NOCHANGE, TSQuery,
};
use crate::utils::misc::stack_depth::check_stack_depth;

use crate::access::common::tupdesc::TupleDescData;
use crate::catalog::pg_type_d::TSQUERYOID;
use crate::executor::spi::{
    SPIPlanPtr, SPI_connect, SPI_cursor_close, SPI_cursor_fetch, SPI_cursor_open, SPI_finish,
    SPI_freeplan, SPI_freetuptable, SPI_getbinval, SPI_gettypeid, SPI_prepare, SPI_processed,
    SPI_tuptable, Portal,
};
use crate::utils::adt::ts_type::DatumGetTSQuery;
use crate::utils::adt::varlena::text_to_cstring;

use crate::{PG_FREE_IF_COPY, PG_GETARG_TEXT_PP, PG_GETARG_TSQUERY, PG_GETARG_TSQUERY_COPY,
    PG_RETURN_POINTER};

use core::ffi::{c_int, c_void};

/*
 * CHECK_FOR_INTERRUPTS (miscadmin.h): query-cancel/die check.  No signal machinery is
 * ported yet, so this is a no-op (same convention as other ported adt files).
 */
#[inline]
fn CHECK_FOR_INTERRUPTS() {}

/*
 * If "node" is equal to "ex", return a copy of "subs" instead.
 * If "ex" matches a subset of node's children, return a modified version
 * of "node" in which those children are replaced with a copy of "subs".
 * Otherwise return "node" unmodified.
 *
 * The QTN_NOCHANGE bit is set in successfully modified nodes, so that
 * we won't uselessly recurse into them.
 * Also, set *isfind true if we make a replacement.
 */
unsafe fn findeq(
    mut node: *mut QTNode,
    ex: *mut QTNode,
    subs: *mut QTNode,
    isfind: *mut bool,
) -> *mut QTNode {
    /* Can't match unless signature matches and node type matches. */
    if ((*node).sign & (*ex).sign) != (*ex).sign
        || (*(*node).valnode).type_() != (*(*ex).valnode).type_()
    {
        return node;
    }

    /* Ignore nodes marked NOCHANGE, too. */
    if (*node).flags & QTN_NOCHANGE != 0 {
        return node;
    }

    if (*(*node).valnode).type_() == QI_OPR {
        /* Must be same operator. */
        if (*(*node).valnode).qoperator.oper != (*(*ex).valnode).qoperator.oper {
            return node;
        }

        if (*node).nchild == (*ex).nchild {
            /*
             * Simple case: when same number of children, match if equal.
             * (This is reliable when the children were sorted earlier.)
             */
            if QTNEq(node, ex) {
                /* Match; delete node and return a copy of subs instead. */
                QTNFree(node);
                if !subs.is_null() {
                    node = QTNCopy(subs);
                    (*node).flags |= QTN_NOCHANGE;
                } else {
                    node = null_mut();
                }
                *isfind = true;
            }
        } else if (*node).nchild > (*ex).nchild && (*ex).nchild > 0 {
            /*
             * AND and OR are commutative/associative, so we should check if a
             * subset of the children match.  For example, if node is A|B|C,
             * and ex is B|C, we have a match after we notionally convert node
             * to A|(B|C).  This does not work for NOT or PHRASE nodes, but we
             * can't get here for those node types because they have a fixed
             * number of children.
             *
             * Because we expect that the children are sorted, it suffices to
             * make one pass through the two lists to find the matches.
             */

            /* Assert that the subset rule is OK */
            Assert!(
                (*(*node).valnode).qoperator.oper == OP_AND
                    || (*(*node).valnode).qoperator.oper == OP_OR
            );

            /* matched[] will record which children of node matched */
            let matched: *mut bool =
                palloc0((*node).nchild as usize * core::mem::size_of::<bool>()) as *mut bool;
            let mut nmatched: c_int = 0;
            let mut i: c_int = 0;
            let mut j: c_int = 0;
            while i < (*node).nchild && j < (*ex).nchild {
                let cmp = QTNodeCompare(
                    *(*node).child.add(i as usize),
                    *(*ex).child.add(j as usize),
                );

                if cmp == 0 {
                    /* match! */
                    *matched.add(i as usize) = true;
                    nmatched += 1;
                    i += 1;
                    j += 1;
                } else if cmp < 0 {
                    /* node->child[i] has no match, ignore it */
                    i += 1;
                } else {
                    /* ex->child[j] has no match; we can give up immediately */
                    break;
                }
            }

            if nmatched == (*ex).nchild {
                /* collapse out the matched children of node */
                j = 0;
                i = 0;
                while i < (*node).nchild {
                    if *matched.add(i as usize) {
                        QTNFree(*(*node).child.add(i as usize));
                    } else {
                        *(*node).child.add(j as usize) = *(*node).child.add(i as usize);
                        j += 1;
                    }
                    i += 1;
                }

                /* and instead insert a copy of subs */
                if !subs.is_null() {
                    let subs_copy = QTNCopy(subs);
                    (*subs_copy).flags |= QTN_NOCHANGE;
                    *(*node).child.add(j as usize) = subs_copy;
                    j += 1;
                }

                (*node).nchild = j;

                /*
                 * At this point we might have a node with zero or one child,
                 * which should be simplified.  But we leave it to our caller
                 * (dofindsubquery) to take care of that.
                 */

                /*
                 * Re-sort the node to put new child in the right place.  This
                 * is a bit bogus, because it won't matter for findsubquery's
                 * remaining processing, and it's insufficient to prepare the
                 * tree for another search (we would need to re-flatten as
                 * well, and we don't want to do that because we'd lose the
                 * QTN_NOCHANGE marking on the new child).  But it's needed to
                 * keep the results the same as the regression tests expect.
                 */
                QTNSort(node);

                *isfind = true;
            }

            pfree(matched as *mut c_void);
        }
    } else {
        Assert!((*(*node).valnode).type_() == QI_VAL);

        if (*(*node).valnode).qoperand.valcrc != (*(*ex).valnode).qoperand.valcrc {
            return node;
        } else if QTNEq(node, ex) {
            QTNFree(node);
            if !subs.is_null() {
                node = QTNCopy(subs);
                (*node).flags |= QTN_NOCHANGE;
            } else {
                node = null_mut();
            }
            *isfind = true;
        }
    }

    node
}

/*
 * Recursive guts of findsubquery(): attempt to replace "ex" with "subs"
 * at the root node, and if we failed to do so, recursively match against
 * child nodes.
 *
 * Delete any void subtrees resulting from the replacement.
 * In the following example '5' is replaced by empty operand:
 *
 *	  AND		->	  6
 *	 /	 \
 *	5	 OR
 *		/  \
 *	   6	5
 */
unsafe fn dofindsubquery(
    mut root: *mut QTNode,
    ex: *mut QTNode,
    subs: *mut QTNode,
    isfind: *mut bool,
) -> *mut QTNode {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    /* also, since it's a bit expensive, let's check for query cancel. */
    CHECK_FOR_INTERRUPTS();

    /* match at the node itself */
    root = findeq(root, ex, subs, isfind);

    /* unless we matched here, consider matches at child nodes */
    if !root.is_null()
        && (*root).flags & QTN_NOCHANGE == 0
        && (*(*root).valnode).type_() == QI_OPR
    {
        let mut j: c_int = 0;

        /*
         * Any subtrees that are replaced by NULL must be dropped from the
         * tree.
         */
        let mut i: c_int = 0;
        while i < (*root).nchild {
            *(*root).child.add(j as usize) =
                dofindsubquery(*(*root).child.add(i as usize), ex, subs, isfind);
            if !(*(*root).child.add(j as usize)).is_null() {
                j += 1;
            }
            i += 1;
        }

        (*root).nchild = j;

        /*
         * If we have just zero or one remaining child node, simplify out this
         * operator node.
         */
        if (*root).nchild == 0 {
            QTNFree(root);
            root = null_mut();
        } else if (*root).nchild == 1 && (*(*root).valnode).qoperator.oper != OP_NOT {
            let nroot: *mut QTNode = *(*root).child.add(0);

            pfree(root as *mut c_void);
            root = nroot;
        }
    }

    root
}

/*
 * Substitute "subs" for "ex" throughout the QTNode tree at root.
 *
 * If isfind isn't NULL, set *isfind to show whether we made any substitution.
 *
 * Both "root" and "ex" must have been through QTNTernary and QTNSort
 * to ensure reliable matching.
 */
pub unsafe fn findsubquery(
    root: *mut QTNode,
    ex: *mut QTNode,
    subs: *mut QTNode,
    isfind: *mut bool,
) -> *mut QTNode {
    let mut did_find: bool = false;

    let root = dofindsubquery(root, ex, subs, &mut did_find);

    if !isfind.is_null() {
        *isfind = did_find;
    }

    root
}

/*
 * ts_rewrite(query tsquery, in text) -- SPI-driven form.
 *
 * Runs the SQL text in "in" as a cursor returning (target tsquery, sample tsquery)
 * rows, and for each row substitutes "sample" for "target" throughout "query" via
 * findsubquery.  (This corresponds to the C function tsquery_rewrite_query; see the
 * note at the top of this file on the swapped C function-name <-> SQL-binding mapping.)
 */
pub unsafe fn tsquery_rewrite(fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
    let query: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 0);
    let in_: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut rewritten: TSQuery = query;
    let outercontext: MemoryContext = CurrentMemoryContext;
    let mut tree: *mut QTNode;
    let buf: *mut c_char;
    let plan: SPIPlanPtr;
    let portal: Portal;
    let mut isnull: bool = false;

    if (*query).size == 0 {
        PG_FREE_IF_COPY!(fcinfo, in_, 1);
        PG_RETURN_POINTER!(rewritten);
    }

    tree = QT2QTN(GETQUERY(query), GETOPERAND(query));
    QTNTernary(tree);
    QTNSort(tree);

    buf = text_to_cstring(in_);

    SPI_connect();

    plan = SPI_prepare(buf, 0, null_mut());
    if plan.is_null() {
        elog!(
            ERROR,
            "SPI_prepare(\"{}\") failed",
            std::ffi::CStr::from_ptr(buf).to_string_lossy()
        );
    }

    portal = SPI_cursor_open(null_mut(), plan, null_mut(), null_mut(), true);
    if portal.is_null() {
        elog!(
            ERROR,
            "SPI_cursor_open(\"{}\") failed",
            std::ffi::CStr::from_ptr(buf).to_string_lossy()
        );
    }

    SPI_cursor_fetch(portal, true, 100);

    if SPI_tuptable.is_null()
        || (*(*SPI_tuptable).tupdesc.cast::<TupleDescData>()).natts != 2
        || SPI_gettypeid((*SPI_tuptable).tupdesc, 1) != TSQUERYOID
        || SPI_gettypeid((*SPI_tuptable).tupdesc, 2) != TSQUERYOID
    {
        ereport!(
            ERROR,
            errmsg!("ts_rewrite query must return two tsquery columns")
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    while SPI_processed > 0 && !tree.is_null() {
        let mut i: u64 = 0;

        while i < SPI_processed && !tree.is_null() {
            let qdata: Datum = SPI_getbinval(
                *(*SPI_tuptable).vals.add(i as usize),
                (*SPI_tuptable).tupdesc,
                1,
                &mut isnull,
            );
            let sdata: Datum;

            if isnull {
                i += 1;
                continue;
            }

            sdata = SPI_getbinval(
                *(*SPI_tuptable).vals.add(i as usize),
                (*SPI_tuptable).tupdesc,
                2,
                &mut isnull,
            );

            if !isnull {
                let qtex: TSQuery = DatumGetTSQuery(qdata);
                let qtsubs: TSQuery = DatumGetTSQuery(sdata);
                let qex: *mut QTNode;
                let mut qsubs: *mut QTNode = null_mut();

                if (*qtex).size == 0 {
                    if qtex != DatumGetPointer(qdata) as TSQuery {
                        pfree(qtex as *mut c_void);
                    }
                    if qtsubs != DatumGetPointer(sdata) as TSQuery {
                        pfree(qtsubs as *mut c_void);
                    }
                    i += 1;
                    continue;
                }

                qex = QT2QTN(GETQUERY(qtex), GETOPERAND(qtex));

                QTNTernary(qex);
                QTNSort(qex);

                if (*qtsubs).size != 0 {
                    qsubs = QT2QTN(GETQUERY(qtsubs), GETOPERAND(qtsubs));
                }

                let oldcxt = MemoryContextSwitchTo(outercontext);
                tree = findsubquery(tree, qex, qsubs, null_mut());
                MemoryContextSwitchTo(oldcxt);

                QTNFree(qex);
                if qtex != DatumGetPointer(qdata) as TSQuery {
                    pfree(qtex as *mut c_void);
                }
                QTNFree(qsubs);
                if qtsubs != DatumGetPointer(sdata) as TSQuery {
                    pfree(qtsubs as *mut c_void);
                }

                if !tree.is_null() {
                    /* ready the tree for another pass */
                    QTNClearFlags(tree, QTN_NOCHANGE);
                    QTNTernary(tree);
                    QTNSort(tree);
                }
            }

            i += 1;
        }

        SPI_freetuptable(SPI_tuptable);
        SPI_cursor_fetch(portal, true, 100);
    }

    SPI_freetuptable(SPI_tuptable);
    SPI_cursor_close(portal);
    SPI_freeplan(plan);
    SPI_finish();

    if !tree.is_null() {
        QTNBinary(tree);
        rewritten = QTN2QT(tree);
        QTNFree(tree);
        PG_FREE_IF_COPY!(fcinfo, query, 0);
    } else {
        SET_VARSIZE(rewritten as *mut c_char, HDRSIZETQ() as i32);
        (*rewritten).size = 0;
    }

    pfree(buf as *mut c_void);
    PG_FREE_IF_COPY!(fcinfo, in_, 1);
    PG_RETURN_POINTER!(rewritten);
}

/*
 * ts_rewrite(query tsquery, target tsquery, sample tsquery)
 *
 * FULLY REAL: convert all three TSQuerys to QTNode trees, recursively substitute
 * `subst` (sample) for `ex` (target) in `query` via findsubquery, and flatten back.
 */
pub unsafe fn tsquery_rewrite_query(fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
    let query: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 0);
    let ex: TSQuery = PG_GETARG_TSQUERY!(fcinfo, 1);
    let subst: TSQuery = PG_GETARG_TSQUERY!(fcinfo, 2);
    let mut rewritten: TSQuery = query;

    if (*query).size == 0 || (*ex).size == 0 {
        PG_FREE_IF_COPY!(fcinfo, ex, 1);
        PG_FREE_IF_COPY!(fcinfo, subst, 2);
        PG_RETURN_POINTER!(rewritten);
    }

    let mut tree: *mut QTNode = QT2QTN(GETQUERY(query), GETOPERAND(query));
    QTNTernary(tree);
    QTNSort(tree);

    let qex: *mut QTNode = QT2QTN(GETQUERY(ex), GETOPERAND(ex));
    QTNTernary(qex);
    QTNSort(qex);

    let mut subs: *mut QTNode = null_mut();
    if (*subst).size != 0 {
        subs = QT2QTN(GETQUERY(subst), GETOPERAND(subst));
    }

    tree = findsubquery(tree, qex, subs, null_mut());

    QTNFree(qex);
    QTNFree(subs);

    if tree.is_null() {
        SET_VARSIZE(rewritten as *mut c_char, HDRSIZETQ() as i32);
        (*rewritten).size = 0;
        PG_FREE_IF_COPY!(fcinfo, ex, 1);
        PG_FREE_IF_COPY!(fcinfo, subst, 2);
        PG_RETURN_POINTER!(rewritten);
    } else {
        QTNBinary(tree);
        rewritten = QTN2QT(tree);
        QTNFree(tree);
    }

    PG_FREE_IF_COPY!(fcinfo, query, 0);
    PG_FREE_IF_COPY!(fcinfo, ex, 1);
    PG_FREE_IF_COPY!(fcinfo, subst, 2);
    PG_RETURN_POINTER!(rewritten);
}

// ================================================================
//   tests
// ================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::adt::tsquery_util::{
        QueryItem, QI_OPR as TQI_OPR, QI_VAL as TQI_VAL,
    };

    /*
     * Hand-build QTNode VAL leaves carrying a given crc/sign/word, with no flags
     * that would trigger frees of static data (QTN_WORDFREE/QTN_NEEDFREE unset).
     */
    unsafe fn make_val(item: *mut QueryItem, word: *mut c_char, crc: i32) -> *mut QTNode {
        let n = palloc0(core::mem::size_of::<QTNode>()) as *mut QTNode;
        (*item).qoperand.r#type = TQI_VAL;
        (*item).qoperand.valcrc = crc;
        /* length must equal strlen(word); QTNodeCompare reads it via tsCompareString */
        let len = {
            let mut l = 0usize;
            while *word.add(l) != 0 {
                l += 1;
            }
            l as u32
        };
        (*item).qoperand.lendist = len; /* distance=0, length=len */
        (*n).valnode = item;
        (*n).word = word;
        (*n).sign = 1u32 << ((crc as u32) % 32);
        (*n).nchild = 0;
        n
    }

    /*
     * dofindsubquery on a single VAL leaf: replacing the leaf's term with NULL
     * (subs == NULL) must delete the whole tree (return NULL).  Exercises the
     * VAL branch of findeq plus dofindsubquery's empty-tree handling.
     */
    #[test]
    fn findeq_val_replace_with_null_drops_tree() {
        unsafe {
            // QueryOperand has a private `_pad`; build via zeroed + field set.
            let mut it_a: QueryItem = core::mem::zeroed();
            it_a.qoperand.r#type = TQI_VAL;
            it_a.qoperand.weight = 0;
            it_a.qoperand.prefix = false;
            it_a.qoperand.valcrc = 0;
            it_a.qoperand.lendist = 0;
            let mut it_ex = it_a;
            let wa = b"cat\0".as_ptr() as *mut c_char;
            let wex = b"cat\0".as_ptr() as *mut c_char;

            let node = make_val(&mut it_a, wa, 42);
            let ex = make_val(&mut it_ex, wex, 42);

            let mut isfind = false;
            let res = dofindsubquery(node, ex, null_mut(), &mut isfind);

            assert!(isfind, "matching leaf should set isfind");
            assert!(res.is_null(), "leaf replaced by NULL should drop the tree");

            QTNFree(ex);
        }
    }

    /*
     * findeq with a non-matching crc must NOT touch the node and must not set isfind.
     */
    #[test]
    fn findeq_val_no_match_when_crc_differs() {
        unsafe {
            // QueryOperand has a private `_pad`; build via zeroed + field set.
            let mut it_a: QueryItem = core::mem::zeroed();
            it_a.qoperand.r#type = TQI_VAL;
            it_a.qoperand.weight = 0;
            it_a.qoperand.prefix = false;
            it_a.qoperand.valcrc = 0;
            it_a.qoperand.lendist = 0;
            let mut it_ex = it_a;
            let wa = b"cat\0".as_ptr() as *mut c_char;
            let wex = b"dog\0".as_ptr() as *mut c_char;

            let node = make_val(&mut it_a, wa, 1);
            let ex = make_val(&mut it_ex, wex, 2); /* different crc -> different sign too */

            let mut isfind = false;
            let res = findeq(node, ex, null_mut(), &mut isfind);

            assert!(!isfind, "different crc must not match");
            assert_eq!(res, node, "node returned unchanged");

            QTNFree(node);
            QTNFree(ex);
        }
    }

    /*
     * Subset match inside an AND/OR node: build (a & b & c), replace target (a)
     * with NULL.  Since OP_AND has >ex.nchild children with ex.nchild==1, findeq
     * does NOT take the subset path for a 1-child ex over a same-operator parent;
     * instead dofindsubquery recurses into the children and matches the VAL leaf
     * 'a', dropping it -> (b & c) survives with 2 children.
     */
    #[test]
    fn dofindsubquery_drops_one_and_child() {
        unsafe {
            /* three VAL leaves a,b,c under an OP_AND operator node */
            let mut its: [QueryItem; 4] = core::mem::zeroed();
            let wa = b"a\0".as_ptr() as *mut c_char;
            let wb = b"b\0".as_ptr() as *mut c_char;
            let wc = b"c\0".as_ptr() as *mut c_char;

            let la = make_val(&mut its[0], wa, 10);
            let lb = make_val(&mut its[1], wb, 20);
            let lc = make_val(&mut its[2], wc, 30);

            /* operator node */
            let opitem = &mut its[3] as *mut QueryItem;
            (*opitem).qoperator = QueryOperator {
                r#type: TQI_OPR,
                oper: OP_AND,
                distance: 0,
                left: 0,
            };
            let root = palloc0(core::mem::size_of::<QTNode>()) as *mut QTNode;
            (*root).valnode = opitem;
            (*root).nchild = 3;
            (*root).child =
                palloc0(core::mem::size_of::<*mut QTNode>() * 3) as *mut *mut QTNode;
            *(*root).child.add(0) = la;
            *(*root).child.add(1) = lb;
            *(*root).child.add(2) = lc;
            (*root).sign = (*la).sign | (*lb).sign | (*lc).sign;

            /* target ex = leaf 'a' */
            let mut it_ex: QueryItem = core::mem::zeroed();
            let wex = b"a\0".as_ptr() as *mut c_char;
            let ex = make_val(&mut it_ex, wex, 10);

            let mut isfind = false;
            let res = dofindsubquery(root, ex, null_mut(), &mut isfind);

            assert!(isfind, "leaf 'a' should have matched");
            assert!(!res.is_null(), "node still has b & c");
            assert_eq!((*res).nchild, 2, "should have dropped one child");

            QTNFree(res);
            QTNFree(ex);
        }
    }
}

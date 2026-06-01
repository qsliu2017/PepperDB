//! Translation of postgres/src/backend/utils/adt/tsquery_cleanup.c
//!
//! Cleanup of a parsed `tsquery` item tree: removes NOT subtrees
//! (`clean_NOT`), removes stopword-deleted (QI_VALSTOP) nodes and collapses the
//! degenerate operators they leave behind (`cleanup_tsquery_stopwords`), while
//! propagating phrase-distance adjustments.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   postgres.h          -> crate::prelude::*  (palloc/pfree/repalloc, ereport!/errmsg!,
//!                          Assert!, int32, null_mut).
//!   miscadmin.h         -> check_stack_depth() (crate::utils::misc::stack_depth, ported).
//!   tsearch/ts_utils.h  -> the tsquery node structs + QI_*/OP_* consts + GETQUERY/
//!                          GETOPERAND/COMPUTESIZE/HDRSIZETQ macros, all imported from the
//!                          sibling crate::utils::adt::tsquery_util (do NOT redefine).
//!   varatt.h            -> SET_VARSIZE (crate::varatt).
//!
//! Max() comes from crate::c.
//!
//! The local `NODE` tree struct (left/right/valnode) is private to this file in C
//! (it is a different, simpler structure than tsquery_util's QTNode), so it is
//! declared module-locally here, exactly as in the C source.
//!
//! TRANSLATED (everything in tsquery_cleanup.c): maketree, PLAINTREE, plainnode,
//! plaintree, freetree, clean_NOT_intree, clean_NOT (public), clean_stopword_intree,
//! calcstrlen, cleanup_tsquery_stopwords (public).
//!
//! Nothing is stubbed: every dependency (node structs, item-type/operator consts,
//! GETQUERY/GETOPERAND/COMPUTESIZE/HDRSIZETQ, check_stack_depth, SET_VARSIZE, Max)
//! is a real ported symbol.

use crate::prelude::*;

use crate::c::{int32, Max};
use crate::utils::adt::tsquery_util::{
    QueryOperand, QueryItem, COMPUTESIZE, GETOPERAND, GETQUERY, HDRSIZETQ, OP_AND, OP_NOT, OP_OR,
    OP_PHRASE, QI_OPR, QI_VAL, QI_VALSTOP, TSQuery,
};
use crate::utils::misc::stack_depth::check_stack_depth;
use crate::varatt::SET_VARSIZE;

use core::ffi::{c_int, c_void};

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/*
 * typedef struct NODE
 * {
 *     struct NODE *left;
 *     struct NODE *right;
 *     QueryItem  *valnode;
 * } NODE;
 */
#[repr(C)]
struct NODE {
    left: *mut NODE,
    right: *mut NODE,
    valnode: *mut QueryItem,
}

/*
 * make query tree from plain view of query
 */
unsafe fn maketree(in_: *mut QueryItem) -> *mut NODE {
    let node = palloc(core::mem::size_of::<NODE>()) as *mut NODE;

    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    (*node).valnode = in_;
    (*node).right = null_mut();
    (*node).left = null_mut();
    if (*in_).type_() == QI_OPR {
        (*node).right = maketree(in_.add(1));
        if (*in_).qoperator.oper != OP_NOT {
            (*node).left = maketree(in_.add((*in_).qoperator.left as usize));
        }
    }
    node
}

/*
 * Internal state for plaintree and plainnode
 */
struct PLAINTREE {
    ptr: *mut QueryItem,
    len: c_int, /* allocated size of ptr */
    cur: c_int, /* number of elements in ptr */
}

unsafe fn plainnode(state: *mut PLAINTREE, node: *mut NODE) {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (*state).cur == (*state).len {
        (*state).len *= 2;
        (*state).ptr = repalloc(
            (*state).ptr as *mut c_void,
            (*state).len as usize * core::mem::size_of::<QueryItem>(),
        ) as *mut QueryItem;
    }
    memcpy(
        (*state).ptr.add((*state).cur as usize) as *mut c_void,
        (*node).valnode as *const c_void,
        core::mem::size_of::<QueryItem>(),
    );
    if (*(*node).valnode).type_() == QI_VAL {
        (*state).cur += 1;
    } else if (*(*node).valnode).qoperator.oper == OP_NOT {
        (*(*state).ptr.add((*state).cur as usize)).qoperator.left = 1;
        (*state).cur += 1;
        plainnode(state, (*node).right);
    } else {
        let cur = (*state).cur;

        (*state).cur += 1;
        plainnode(state, (*node).right);
        (*(*state).ptr.add(cur as usize)).qoperator.left = ((*state).cur - cur) as u32;
        plainnode(state, (*node).left);
    }
    pfree(node as *mut c_void);
}

/*
 * make plain view of tree from a NODE-tree representation
 */
unsafe fn plaintree(root: *mut NODE, len: *mut c_int) -> *mut QueryItem {
    let mut pl = PLAINTREE {
        ptr: null_mut(),
        len: 16,
        cur: 0,
    };

    if !root.is_null()
        && ((*(*root).valnode).type_() == QI_VAL || (*(*root).valnode).type_() == QI_OPR)
    {
        pl.ptr = palloc(pl.len as usize * core::mem::size_of::<QueryItem>()) as *mut QueryItem;
        plainnode(&mut pl, root);
    } else {
        pl.ptr = null_mut();
    }
    *len = pl.cur;
    pl.ptr
}

unsafe fn freetree(node: *mut NODE) {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if node.is_null() {
        return;
    }
    if !(*node).left.is_null() {
        freetree((*node).left);
    }
    if !(*node).right.is_null() {
        freetree((*node).right);
    }
    pfree(node as *mut c_void);
}

/*
 * clean tree for ! operator.
 * It's useful for debug, but in
 * other case, such view is used with search in index.
 * Operator ! always return TRUE
 */
unsafe fn clean_NOT_intree(node: *mut NODE) -> *mut NODE {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (*(*node).valnode).type_() == QI_VAL {
        return node;
    }

    if (*(*node).valnode).qoperator.oper == OP_NOT {
        freetree(node);
        return null_mut();
    }

    /* operator & or | */
    if (*(*node).valnode).qoperator.oper == OP_OR {
        (*node).left = clean_NOT_intree((*node).left);
        if (*node).left.is_null() || {
            (*node).right = clean_NOT_intree((*node).right);
            (*node).right.is_null()
        } {
            freetree(node);
            return null_mut();
        }
    } else {
        let mut res = node;

        Assert!(
            (*(*node).valnode).qoperator.oper == OP_AND
                || (*(*node).valnode).qoperator.oper == OP_PHRASE
        );

        (*node).left = clean_NOT_intree((*node).left);
        (*node).right = clean_NOT_intree((*node).right);
        if (*node).left.is_null() && (*node).right.is_null() {
            pfree(node as *mut c_void);
            res = null_mut();
        } else if (*node).left.is_null() {
            res = (*node).right;
            pfree(node as *mut c_void);
        } else if (*node).right.is_null() {
            res = (*node).left;
            pfree(node as *mut c_void);
        }
        return res;
    }
    node
}

pub unsafe fn clean_NOT(ptr: *mut QueryItem, len: *mut c_int) -> *mut QueryItem {
    let root = maketree(ptr);

    plaintree(clean_NOT_intree(root), len)
}

/*
 * Remove QI_VALSTOP (stopword) nodes from query tree.
 *
 * Returns NULL if the query degenerates to nothing.  Input must not be NULL.
 *
 * When we remove a phrase operator due to removing one or both of its
 * arguments, we might need to adjust the distance of a parent phrase
 * operator.  For example, 'a' is a stopword, so:
 *      (b <-> a) <-> c  should become  b <2> c
 *      b <-> (a <-> c)  should become  b <2> c
 *      (b <-> (a <-> a)) <-> c  should become  b <3> c
 *      b <-> ((a <-> a) <-> c)  should become  b <3> c
 * To handle that, we define two output parameters:
 *      ladd: amount to add to a phrase distance to the left of this node
 *      radd: amount to add to a phrase distance to the right of this node
 * We need two outputs because we could need to bubble up adjustments to two
 * different parent phrase operators.  Consider
 *      w <-> (((a <-> x) <2> (y <3> a)) <-> z)
 * After we've removed the two a's and are considering the <2> node (which is
 * now just x <2> y), we have an ladd distance of 1 that needs to propagate
 * up to the topmost (leftmost) <->, and an radd distance of 3 that needs to
 * propagate to the rightmost <->, so that we'll end up with
 *      w <2> ((x <2> y) <4> z)
 * Near the bottom of the tree, we may have subtrees consisting only of
 * stopwords.  The distances of any phrase operators within such a subtree are
 * summed and propagated to both ladd and radd, since we don't know which side
 * of the lowest surviving phrase operator we are in.  The rule is that any
 * subtree that degenerates to NULL must return equal values of ladd and radd,
 * and the parent node dealing with it should incorporate only one of those.
 *
 * Currently, we only implement this adjustment for adjacent phrase operators.
 * Thus for example 'x <-> ((a <-> y) | z)' will become 'x <-> (y | z)', which
 * isn't ideal, but there is no way to represent the really desired semantics
 * without some redesign of the tsquery structure.  Certainly it would not be
 * any better to convert that to 'x <2> (y | z)'.  Since this is such a weird
 * corner case, let it go for now.  But we can fix it in cases where the
 * intervening non-phrase operator also gets removed, for example
 * '((x <-> a) | a) <-> y' will become 'x <2> y'.
 */
unsafe fn clean_stopword_intree(
    node: *mut NODE,
    ladd: *mut c_int,
    radd: *mut c_int,
) -> *mut NODE {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    /* default output parameters indicate no change in parent distance */
    *ladd = 0;
    *radd = 0;

    if (*(*node).valnode).type_() == QI_VAL {
        return node;
    } else if (*(*node).valnode).type_() == QI_VALSTOP {
        pfree(node as *mut c_void);
        return null_mut();
    }

    Assert!((*(*node).valnode).type_() == QI_OPR);

    if (*(*node).valnode).qoperator.oper == OP_NOT {
        /* NOT doesn't change pattern width, so just report child distances */
        (*node).right = clean_stopword_intree((*node).right, ladd, radd);
        if (*node).right.is_null() {
            freetree(node);
            return null_mut();
        }
    } else {
        let mut res = node;
        let isphrase: bool;
        let ndistance: c_int;
        let mut lladd: c_int = 0;
        let mut lradd: c_int = 0;
        let mut rladd: c_int = 0;
        let mut rradd: c_int = 0;

        /* First, recurse */
        (*node).left = clean_stopword_intree((*node).left, &mut lladd, &mut lradd);
        (*node).right = clean_stopword_intree((*node).right, &mut rladd, &mut rradd);

        /* Check if current node is OP_PHRASE, get its distance */
        isphrase = (*(*node).valnode).qoperator.oper == OP_PHRASE;
        ndistance = if isphrase {
            (*(*node).valnode).qoperator.distance as c_int
        } else {
            0
        };

        if (*node).left.is_null() && (*node).right.is_null() {
            /*
             * When we collapse out a phrase node entirely, propagate its own
             * distance into both *ladd and *radd; it is the responsibility of
             * the parent node to count it only once.  Also, for a phrase
             * node, distances coming from children are summed and propagated
             * up to parent (we assume lladd == lradd and rladd == rradd, else
             * rule was broken at a lower level).  But if this isn't a phrase
             * node, take the larger of the two child distances; that
             * corresponds to what TS_execute will do in non-stopword cases.
             */
            if isphrase {
                *ladd = lladd + ndistance + rladd;
                *radd = *ladd;
            } else {
                *ladd = Max(lladd, rladd);
                *radd = *ladd;
            }
            freetree(node);
            return null_mut();
        } else if (*node).left.is_null() {
            /* Removing this operator and left subnode */
            /* lladd and lradd are equal/redundant, don't count both */
            if isphrase {
                /* operator's own distance must propagate to left */
                *ladd = lladd + ndistance + rladd;
                *radd = rradd;
            } else {
                /* at non-phrase op, just forget the left subnode entirely */
                *ladd = rladd;
                *radd = rradd;
            }
            res = (*node).right;
            pfree(node as *mut c_void);
        } else if (*node).right.is_null() {
            /* Removing this operator and right subnode */
            /* rladd and rradd are equal/redundant, don't count both */
            if isphrase {
                /* operator's own distance must propagate to right */
                *ladd = lladd;
                *radd = lladd + ndistance + rradd;
            } else {
                /* at non-phrase op, just forget the right subnode entirely */
                *ladd = lladd;
                *radd = lladd;
            }
            res = (*node).left;
            pfree(node as *mut c_void);
        } else if isphrase {
            /* Absorb appropriate corrections at this level */
            (*(*node).valnode).qoperator.distance += (lradd + rladd) as i16;
            /* Propagate up any unaccounted-for corrections */
            *ladd = lladd;
            *radd = rradd;
        } else {
            /* We're keeping a non-phrase operator, so ladd/radd remain 0 */
        }

        return res;
    }
    node
}

/*
 * Number of elements in query tree
 */
unsafe fn calcstrlen(node: *mut NODE) -> int32 {
    let size: int32;

    if (*(*node).valnode).type_() == QI_VAL {
        size = (*(*node).valnode).qoperand.length() as int32 + 1;
    } else {
        Assert!((*(*node).valnode).type_() == QI_OPR);

        let mut s = calcstrlen((*node).right);
        if (*(*node).valnode).qoperator.oper != OP_NOT {
            s += calcstrlen((*node).left);
        }
        size = s;
    }

    size
}

/*
 * Remove QI_VALSTOP (stopword) nodes from TSQuery.
 */
pub unsafe fn cleanup_tsquery_stopwords(in_: TSQuery, noisy: bool) -> TSQuery {
    let mut len: c_int = 0;
    let lenstr: int32;
    let commonlen: int32;
    let root: *mut NODE;
    let mut ladd: c_int = 0;
    let mut radd: c_int = 0;
    let out: TSQuery;
    let mut items: *mut QueryItem;
    let mut operands: *mut core::ffi::c_char;

    if (*in_).size == 0 {
        return in_;
    }

    /* eliminate stop words */
    root = clean_stopword_intree(maketree(GETQUERY(in_)), &mut ladd, &mut radd);
    if root.is_null() {
        if noisy {
            ereport!(
                NOTICE,
                errmsg!(
                    "text-search query contains only stop words or doesn't contain lexemes, ignored"
                )
            );
        }
        let out = palloc(HDRSIZETQ()) as TSQuery;
        (*out).size = 0;
        SET_VARSIZE(out as *mut core::ffi::c_char, HDRSIZETQ() as int32);
        return out;
    }

    /*
     * Build TSQuery from plain view
     */

    lenstr = calcstrlen(root);
    items = plaintree(root, &mut len);
    commonlen = COMPUTESIZE(len, lenstr) as int32;

    out = palloc(commonlen as usize) as TSQuery;
    SET_VARSIZE(out as *mut core::ffi::c_char, commonlen);
    (*out).size = len;

    memcpy(
        GETQUERY(out) as *mut c_void,
        items as *const c_void,
        len as usize * core::mem::size_of::<QueryItem>(),
    );

    items = GETQUERY(out);
    operands = GETOPERAND(out);
    let mut i: int32 = 0;
    while i < (*out).size {
        let op: *mut QueryOperand = &mut (*items.add(i as usize)).qoperand;

        if (*op).r#type != QI_VAL {
            i += 1;
            continue;
        }

        memcpy(
            operands as *mut c_void,
            GETOPERAND(in_).add((*op).distance() as usize) as *const c_void,
            (*op).length() as usize,
        );
        *operands.add((*op).length() as usize) = 0; /* '\0' */
        (*op).set_distance((operands as isize - GETOPERAND(out) as isize) as u32);
        operands = operands.add((*op).length() as usize + 1);
        i += 1;
    }

    out
}

// ================================================================
//   tests
// ================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::adt::tsquery_util::QueryOperator;

    /* helper: build a leaf VAL node carrying a QI_VAL QueryItem */
    unsafe fn mk_val(valcrc: i32, length: u32) -> *mut NODE {
        // palloc0 so the private `_pad` byte is zeroed; set the public fields
        // individually (a struct literal cannot name the private `_pad`).
        let qi = palloc0(core::mem::size_of::<QueryItem>()) as *mut QueryItem;
        (*qi).qoperand.r#type = QI_VAL;
        (*qi).qoperand.weight = 0;
        (*qi).qoperand.prefix = false;
        (*qi).qoperand.valcrc = valcrc;
        (*qi).qoperand.lendist = length & ((1 << 12) - 1);
        mk_node(qi, null_mut(), null_mut())
    }

    /* helper: build a VALSTOP leaf node */
    unsafe fn mk_valstop() -> *mut NODE {
        let qi = palloc(core::mem::size_of::<QueryItem>()) as *mut QueryItem;
        (*qi).r#type = QI_VALSTOP;
        mk_node(qi, null_mut(), null_mut())
    }

    /* helper: build an operator node with given children */
    unsafe fn mk_opr(oper: i8, distance: i16, left: *mut NODE, right: *mut NODE) -> *mut NODE {
        let qi = palloc(core::mem::size_of::<QueryItem>()) as *mut QueryItem;
        (*qi).qoperator = QueryOperator {
            r#type: QI_OPR,
            oper,
            distance,
            left: 0,
        };
        mk_node(qi, left, right)
    }

    unsafe fn mk_node(valnode: *mut QueryItem, left: *mut NODE, right: *mut NODE) -> *mut NODE {
        let n = palloc(core::mem::size_of::<NODE>()) as *mut NODE;
        (*n).valnode = valnode;
        (*n).left = left;
        (*n).right = right;
        n
    }

    /*
     * (a AND <stopword>) : the stopword child is removed, the AND collapses to
     * its surviving operand `a`.
     */
    #[test]
    fn and_with_stopword_collapses_to_surviving_operand() {
        unsafe {
            let a = mk_val(11, 1);
            let stop = mk_valstop();
            /* C maketree convention: right child = item+1, left child = item+left.
             * Our hand-built tree just sets left/right directly. clean_stopword_intree
             * recurses on left and right independently; layout convention is irrelevant
             * to the collapse logic. Put the survivor on the left, stopword on right. */
            let and = mk_opr(OP_AND, 0, a, stop);

            let mut ladd = 0;
            let mut radd = 0;
            let res = clean_stopword_intree(and, &mut ladd, &mut radd);

            /* the AND node is freed; result is the surviving `a` leaf */
            assert_eq!(res, a);
            assert_eq!((*(*res).valnode).type_(), QI_VAL);
            assert_eq!((*(*res).valnode).qoperand.valcrc, 11);
            /* non-phrase op: no distance correction bubbles up */
            assert_eq!(ladd, 0);
            assert_eq!(radd, 0);

            freetree(res);
        }
    }

    /*
     * An all-stopword tree (stop AND stop) degenerates to NULL.
     */
    #[test]
    fn all_stopword_tree_returns_null() {
        unsafe {
            let s1 = mk_valstop();
            let s2 = mk_valstop();
            let and = mk_opr(OP_AND, 0, s1, s2);

            let mut ladd = 0;
            let mut radd = 0;
            let res = clean_stopword_intree(and, &mut ladd, &mut radd);

            assert!(res.is_null());
            /* non-phrase collapse: ladd == radd == Max(0,0) == 0 */
            assert_eq!(ladd, 0);
            assert_eq!(radd, 0);
        }
    }

    /*
     * Phrase distance propagation: (b <-> a) with `a` a stopword collapses to
     * `b`, and the phrase distance (1) bubbles up via ladd/radd so a parent
     * phrase operator can absorb it.  Here `a` is the right child (stopword).
     */
    #[test]
    fn phrase_with_stopword_propagates_distance() {
        unsafe {
            let b = mk_val(22, 1);
            let a = mk_valstop();
            /* phrase distance 1 */
            let phr = mk_opr(OP_PHRASE, 1, b, a);

            let mut ladd = 0;
            let mut radd = 0;
            let res = clean_stopword_intree(phr, &mut ladd, &mut radd);

            /* survivor is b (the left child) */
            assert_eq!(res, b);
            /* right subnode removed at a phrase op: radd gets own distance + child */
            /* lladd=0 (b leaf), rladd/rradd=0 (stopword leaf), ndistance=1 */
            assert_eq!(ladd, 0); /* *ladd = lladd */
            assert_eq!(radd, 1); /* *radd = lladd + ndistance + rradd = 0+1+0 */

            freetree(res);
        }
    }
}

//! Translation of postgres/src/backend/utils/adt/tsquery_op.c
//!
//! Various set/comparison operations on the `tsquery` datatype: numnode,
//! and/or/not/phrase constructors, the cmp/lt/le/eq/ne/ge/gt comparison
//! family, makeTSQuerySign, and the mcontains/mcontained term-set predicates.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   postgres.h            -> crate::prelude::*  (Datum, palloc/palloc0/pfree,
//!                            elog!/ereport!/errmsg!/errcode, Assert, null_mut).
//!   lib/qunique.h         -> crate::lib::qunique::qunique IF it exists; it is being
//!                            ported in this same batch.  Since it is not yet present,
//!                            a module-local adjacent-dedup helper `qunique_ptr` is
//!                            defined below with a TODO to switch over.
//!   tsearch/ts_utils.h    -> QTNode tree helpers (QT2QTN/QTN2QT/QTNFree/QTNodeCompare),
//!                            the QTN_* flags, TSQuerySign + TSQS_SIGLEN.  The QTNode
//!                            helpers and flags are imported from the sibling
//!                            crate::utils::adt::tsquery_util.  TSQuerySign /
//!                            TSQS_SIGLEN are declared module-locally here (ts_utils.h
//!                            is not otherwise ported) with a TODO to unify.
//!   utils/fmgrprotos.h    -> the fmgr V1 prototypes; provided by our fmgr macros.
//!   varatt.h              -> VARSIZE (crate::varatt).
//!
//! The TSQuery node types (TSQuery, QueryItem, QueryOperand, QueryOperator,
//! QI_VAL/QI_OPR, OP_AND/OP_OR/OP_NOT/OP_PHRASE, COMPUTESIZE/HDRSIZETQ/GETQUERY/
//! GETOPERAND) and the QTNode tree machinery are ALL imported from
//! crate::utils::adt::tsquery_util - they are NOT redefined here.
//!
//! MAXENTRYPOS comes from crate::utils::adt::tsvector (ts_type.h's WordEntryPos
//! limit), matching the C `#include "tsearch/ts_utils.h"` pull-in.
//!
//! TRANSLATED (everything in tsquery_op.c): tsquery_numnode, join_tsqueries,
//!   tsquery_and, tsquery_or, tsquery_phrase_distance, tsquery_phrase,
//!   tsquery_not, CompareTSQ, tsquery_cmp, the CMPFUNC family
//!   (tsquery_lt/le/eq/ge/gt/ne), makeTSQuerySign, collectTSQueryValues,
//!   cmp_string, tsq_mcontains, tsq_mcontained.
//!
//! No stubs were required: every callee (QT2QTN/QTN2QT/QTNFree/QTNodeCompare,
//! GETQUERY/GETOPERAND, VARSIZE/SET_VARSIZE) is available from tsquery_util /
//! varatt.  qunique is the only not-yet-present dependency and is emulated
//! locally (see qunique_ptr).

use crate::prelude::*;
use crate::varatt::VARSIZE;

use crate::utils::adt::tsquery_util::{
    QTNFree, QTNode, QTNodeCompare, QT2QTN, QTN2QT, GETOPERAND, GETQUERY, OP_AND, OP_NOT, OP_OR,
    OP_PHRASE, QI_OPR, QI_VAL, QTN_NEEDFREE, QueryItem, TSQuery,
};
use crate::utils::adt::tsvector::MAXENTRYPOS;

use crate::c::{int8, int32, uint16};
use core::ffi::{c_char, c_int, c_void};

use crate::utils::fmgr::FunctionCallInfo;
use crate::{
    PG_FREE_IF_COPY, PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_POINTER, PG_RETURN_BOOL,
    PG_RETURN_DATUM, PG_RETURN_INT32, PG_RETURN_POINTER,
};

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

// ================================================================
//   tsearch/ts_utils.h  (TSQuerySign)  --  declared locally
//   TODO(pg-port): unify once ts_utils.h's TSQuerySign is centrally ported.
// ================================================================

/* typedef uint64 TSQuerySign; */
pub type TSQuerySign = u64;

/* #define TSQS_SIGLEN (sizeof(TSQuerySign)*BITS_PER_BYTE) */
const TSQS_SIGLEN: u32 = (core::mem::size_of::<TSQuerySign>() as u32) * 8;

// ================================================================
//   lib/qunique.h emulation
//   TODO(pg-port): replace with crate::lib::qunique::qunique once ported.
// ================================================================

/*
 * Remove duplicates from a pre-sorted slice of `*mut c_char` pointers using the
 * given comparator, in place.  Returns the new length.  Mirrors the C `qunique`
 * (specialized to width == sizeof(char *)).
 */
unsafe fn qunique_ptr(
    array: *mut *mut c_char,
    elements: usize,
    compare: unsafe fn(*const c_void, *const c_void) -> c_int,
) -> usize {
    if elements <= 1 {
        return elements;
    }

    let mut j: usize = 0;
    let mut i: usize = 1;
    while i < elements {
        let pi = array.add(i) as *const c_void;
        let pj = array.add(j) as *const c_void;
        if compare(pi, pj) != 0 {
            j += 1;
            if j != i {
                *array.add(j) = *array.add(i);
            }
        }
        i += 1;
    }

    j + 1
}

// ================================================================
//   tsquery_op.c
// ================================================================

pub unsafe fn tsquery_numnode(fcinfo: FunctionCallInfo) -> Datum {
    let query: TSQuery = PG_GETARG_TSQUERY!(fcinfo, 0);
    let nnode: c_int = (*query).size;

    PG_FREE_IF_COPY!(fcinfo, query as *mut c_char, 0);
    PG_RETURN_INT32!(nnode)
}

/*
 * join_tsqueries: build a QTNode tree whose root is `operator` joining the
 * QTNode trees of `b` and `a` as children (note the C order: child[0] = b,
 * child[1] = a).
 */
unsafe fn join_tsqueries(a: TSQuery, b: TSQuery, operator: int8, distance: uint16) -> *mut QTNode {
    let res = palloc0(core::mem::size_of::<QTNode>()) as *mut QTNode;

    (*res).flags |= QTN_NEEDFREE;

    (*res).valnode = palloc0(core::mem::size_of::<QueryItem>()) as *mut QueryItem;
    (*(*res).valnode).r#type = QI_OPR;
    (*(*res).valnode).qoperator.oper = operator;
    if operator == OP_PHRASE {
        (*(*res).valnode).qoperator.distance = distance as i16;
    }

    (*res).child = palloc0(core::mem::size_of::<*mut QTNode>() * 2) as *mut *mut QTNode;
    *(*res).child.add(0) = QT2QTN(GETQUERY(b), GETOPERAND(b));
    *(*res).child.add(1) = QT2QTN(GETQUERY(a), GETOPERAND(a));
    (*res).nchild = 2;

    res
}

pub unsafe fn tsquery_and(fcinfo: FunctionCallInfo) -> Datum {
    let a: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 0);
    let b: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 1);

    if (*a).size == 0 {
        PG_FREE_IF_COPY!(fcinfo, a as *mut c_char, 1);
        PG_RETURN_POINTER!(b);
    } else if (*b).size == 0 {
        PG_FREE_IF_COPY!(fcinfo, b as *mut c_char, 1);
        PG_RETURN_POINTER!(a);
    }

    let res = join_tsqueries(a, b, OP_AND, 0);

    let query = QTN2QT(res);

    QTNFree(res);
    PG_FREE_IF_COPY!(fcinfo, a as *mut c_char, 0);
    PG_FREE_IF_COPY!(fcinfo, b as *mut c_char, 1);

    PG_RETURN_TSQUERY!(query)
}

pub unsafe fn tsquery_or(fcinfo: FunctionCallInfo) -> Datum {
    let a: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 0);
    let b: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 1);

    if (*a).size == 0 {
        PG_FREE_IF_COPY!(fcinfo, a as *mut c_char, 1);
        PG_RETURN_POINTER!(b);
    } else if (*b).size == 0 {
        PG_FREE_IF_COPY!(fcinfo, b as *mut c_char, 1);
        PG_RETURN_POINTER!(a);
    }

    let res = join_tsqueries(a, b, OP_OR, 0);

    let query = QTN2QT(res);

    QTNFree(res);
    PG_FREE_IF_COPY!(fcinfo, a as *mut c_char, 0);
    PG_FREE_IF_COPY!(fcinfo, b as *mut c_char, 1);

    PG_RETURN_TSQUERY!(query)
}

pub unsafe fn tsquery_phrase_distance(fcinfo: FunctionCallInfo) -> Datum {
    let a: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 0);
    let b: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 1);
    let distance: int32 = PG_GETARG_INT32!(fcinfo, 2);

    if distance < 0 || distance > MAXENTRYPOS {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!(
                "distance in phrase operator must be an integer value between zero and {} inclusive",
                MAXENTRYPOS
            )
        );
    }
    if (*a).size == 0 {
        PG_FREE_IF_COPY!(fcinfo, a as *mut c_char, 1);
        PG_RETURN_POINTER!(b);
    } else if (*b).size == 0 {
        PG_FREE_IF_COPY!(fcinfo, b as *mut c_char, 1);
        PG_RETURN_POINTER!(a);
    }

    let res = join_tsqueries(a, b, OP_PHRASE, distance as uint16);

    let query = QTN2QT(res);

    QTNFree(res);
    PG_FREE_IF_COPY!(fcinfo, a as *mut c_char, 0);
    PG_FREE_IF_COPY!(fcinfo, b as *mut c_char, 1);

    PG_RETURN_TSQUERY!(query)
}

pub unsafe fn tsquery_phrase(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall3!(
        tsquery_phrase_distance,
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1),
        Int32GetDatum(1)
    ))
}

pub unsafe fn tsquery_not(fcinfo: FunctionCallInfo) -> Datum {
    let a: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 0);

    if (*a).size == 0 {
        PG_RETURN_POINTER!(a);
    }

    let res = palloc0(core::mem::size_of::<QTNode>()) as *mut QTNode;

    (*res).flags |= QTN_NEEDFREE;

    (*res).valnode = palloc0(core::mem::size_of::<QueryItem>()) as *mut QueryItem;
    (*(*res).valnode).r#type = QI_OPR;
    (*(*res).valnode).qoperator.oper = OP_NOT;

    (*res).child = palloc0(core::mem::size_of::<*mut QTNode>()) as *mut *mut QTNode;
    *(*res).child.add(0) = QT2QTN(GETQUERY(a), GETOPERAND(a));
    (*res).nchild = 1;

    let query = QTN2QT(res);

    QTNFree(res);
    PG_FREE_IF_COPY!(fcinfo, a as *mut c_char, 0);

    PG_RETURN_POINTER!(query)
}

unsafe fn CompareTSQ(a: TSQuery, b: TSQuery) -> c_int {
    if (*a).size != (*b).size {
        if (*a).size < (*b).size {
            -1
        } else {
            1
        }
    } else if VARSIZE(a as *const c_char) != VARSIZE(b as *const c_char) {
        if VARSIZE(a as *const c_char) < VARSIZE(b as *const c_char) {
            -1
        } else {
            1
        }
    } else if (*a).size != 0 {
        let an = QT2QTN(GETQUERY(a), GETOPERAND(a));
        let bn = QT2QTN(GETQUERY(b), GETOPERAND(b));
        let res = QTNodeCompare(an, bn);

        QTNFree(an);
        QTNFree(bn);

        res
    } else {
        0
    }
}

pub unsafe fn tsquery_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let a: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 0);
    let b: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 1);
    let res = CompareTSQ(a, b);

    PG_FREE_IF_COPY!(fcinfo, a as *mut c_char, 0);
    PG_FREE_IF_COPY!(fcinfo, b as *mut c_char, 1);

    PG_RETURN_INT32!(res)
}

/*
 * CMPFUNC(NAME, CONDITION): the six comparison ops.  In C this is a macro; here
 * we expand it via a Rust macro_rules! producing fmgr V1 functions.
 */
macro_rules! CMPFUNC {
    ($name:ident, $cond:expr) => {
        pub unsafe fn $name(fcinfo: FunctionCallInfo) -> Datum {
            let a: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 0);
            let b: TSQuery = PG_GETARG_TSQUERY_COPY!(fcinfo, 1);
            let res = CompareTSQ(a, b);

            PG_FREE_IF_COPY!(fcinfo, a as *mut c_char, 0);
            PG_FREE_IF_COPY!(fcinfo, b as *mut c_char, 1);

            PG_RETURN_BOOL!($cond(res))
        }
    };
}

CMPFUNC!(tsquery_lt, |res: c_int| res < 0);
CMPFUNC!(tsquery_le, |res: c_int| res <= 0);
CMPFUNC!(tsquery_eq, |res: c_int| res == 0);
CMPFUNC!(tsquery_ge, |res: c_int| res >= 0);
CMPFUNC!(tsquery_gt, |res: c_int| res > 0);
CMPFUNC!(tsquery_ne, |res: c_int| res != 0);

pub unsafe fn makeTSQuerySign(a: TSQuery) -> TSQuerySign {
    let mut ptr: *mut QueryItem = GETQUERY(a);
    let mut sign: TSQuerySign = 0;

    let mut i: c_int = 0;
    while i < (*a).size {
        if (*ptr).type_() == QI_VAL {
            sign |= (1 as TSQuerySign)
                << (((*ptr).qoperand.valcrc as u32) % TSQS_SIGLEN);
        }
        ptr = ptr.add(1);
        i += 1;
    }

    sign
}

/*
 * Extract every QI_VAL operand of `a` into a freshly palloc'd, '\0'-terminated
 * array of C strings.  Returns the array; writes the count to *nvalues_p.
 */
unsafe fn collectTSQueryValues(a: TSQuery, nvalues_p: *mut c_int) -> *mut *mut c_char {
    let mut ptr: *mut QueryItem = GETQUERY(a);
    let operand: *mut c_char = GETOPERAND(a);

    let values = palloc(core::mem::size_of::<*mut c_char>() * (*a).size as usize) as *mut *mut c_char;
    let mut nvalues: c_int = 0;

    let mut i: c_int = 0;
    while i < (*a).size {
        if (*ptr).type_() == QI_VAL {
            let len = (*ptr).qoperand.length() as usize;

            let val = palloc(len + 1) as *mut c_char;
            memcpy(
                val as *mut c_void,
                operand.add((*ptr).qoperand.distance() as usize) as *const c_void,
                len,
            );
            *val.add(len) = 0;

            *values.add(nvalues as usize) = val;
            nvalues += 1;
        }
        ptr = ptr.add(1);
        i += 1;
    }

    *nvalues_p = nvalues;
    values
}

/* qsort/qunique comparator over `*mut c_char` strings. */
unsafe fn cmp_string(a: *const c_void, b: *const c_void) -> c_int {
    let sa = *(a as *const *mut c_char);
    let sb = *(b as *const *mut c_char);
    strcmp(sa, sb)
}

pub unsafe fn tsq_mcontains(fcinfo: FunctionCallInfo) -> Datum {
    let query: TSQuery = PG_GETARG_TSQUERY!(fcinfo, 0);
    let ex: TSQuery = PG_GETARG_TSQUERY!(fcinfo, 1);
    let mut result: bool = true;

    /* Extract the query terms into arrays */
    let mut query_nvalues: c_int = 0;
    let query_values = collectTSQueryValues(query, &mut query_nvalues);
    let mut ex_nvalues: c_int = 0;
    let ex_values = collectTSQueryValues(ex, &mut ex_nvalues);

    /* Sort and remove duplicates from both arrays */
    sort_string_ptrs(query_values, query_nvalues as usize);
    let mut query_nvalues = qunique_ptr(query_values, query_nvalues as usize, cmp_string) as c_int;
    sort_string_ptrs(ex_values, ex_nvalues as usize);
    let ex_nvalues = qunique_ptr(ex_values, ex_nvalues as usize, cmp_string) as c_int;

    /* suppress "value assigned is never read" for the parallel of C's reassign */
    let _ = &mut query_nvalues;

    if ex_nvalues > query_nvalues {
        result = false;
    } else {
        let mut j: c_int = 0;

        let mut i: c_int = 0;
        while i < ex_nvalues {
            while j < query_nvalues {
                if strcmp(*ex_values.add(i as usize), *query_values.add(j as usize)) == 0 {
                    break;
                }
                j += 1;
            }
            if j == query_nvalues {
                result = false;
                break;
            }
            i += 1;
        }
    }

    PG_RETURN_BOOL!(result)
}

pub unsafe fn tsq_mcontained(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall2!(
        tsq_mcontains,
        PG_GETARG_DATUM!(fcinfo, 1),
        PG_GETARG_DATUM!(fcinfo, 0)
    ))
}

/*
 * Helper standing in for C's `qsort(values, n, sizeof(char *), cmp_string)`.
 * Sorts an array of `*mut c_char` by strcmp order.
 */
unsafe fn sort_string_ptrs(values: *mut *mut c_char, n: usize) {
    if n <= 1 {
        return;
    }
    let sl = core::slice::from_raw_parts_mut(values, n);
    sl.sort_by(|x, y| {
        let r = strcmp(*x, *y);
        r.cmp(&0)
    });
}

// ================================================================
//   fmgr V1 entry-point macros for the TSQuery type
//   (ts_type.h: PG_GETARG_TSQUERY / PG_GETARG_TSQUERY_COPY / PG_RETURN_TSQUERY)
// ================================================================

#[macro_export]
macro_rules! PG_GETARG_TSQUERY {
    ($fcinfo:expr, $n:expr) => {
        $crate::postgres::DatumGetPointer($crate::PG_GETARG_DATUM!($fcinfo, $n))
            as $crate::utils::adt::tsquery_util::TSQuery
    };
}

#[macro_export]
macro_rules! PG_GETARG_TSQUERY_COPY {
    ($fcinfo:expr, $n:expr) => {
        $crate::PG_DETOAST_DATUM_COPY!($crate::PG_GETARG_DATUM!($fcinfo, $n))
            as $crate::utils::adt::tsquery_util::TSQuery
    };
}

#[macro_export]
macro_rules! PG_RETURN_TSQUERY {
    ($x:expr) => {
        return $crate::postgres::PointerGetDatum($x as *const core::ffi::c_void)
    };
}

pub(crate) use {PG_GETARG_TSQUERY, PG_GETARG_TSQUERY_COPY, PG_RETURN_TSQUERY};
use crate::{DirectFunctionCall2, DirectFunctionCall3, PG_DETOAST_DATUM_COPY};

// ================================================================
//   tests
// ================================================================
#[cfg(test)]
mod tests {
    use super::*;

    /*
     * Build a flat single-operand TSQuery by hand: header + 1 QueryItem (QI_VAL)
     * + the operand string + '\0'.  `crc` distinguishes operands.
     */
    unsafe fn make_single(word: &[u8], crc: int32) -> TSQuery {
        use crate::utils::adt::tsquery_util::{COMPUTESIZE, GETOPERAND, GETQUERY};
        use crate::varatt::SET_VARSIZE;

        let len = word.len();
        let total = COMPUTESIZE(1, (len + 1) as c_int);
        let q = palloc0(total) as TSQuery;
        SET_VARSIZE(q as *mut c_char, total as int32);
        (*q).size = 1;

        /* palloc0 already zeroed the buffer; set only the live fields (the
         * private `_pad` stays 0).  length = len, distance = 0. */
        let item = GETQUERY(q);
        (*item).qoperand.r#type = QI_VAL;
        (*item).qoperand.valcrc = crc;
        (*item).qoperand.lendist = len as u32;

        let op = GETOPERAND(q);
        for (i, &b) in word.iter().enumerate() {
            *op.add(i) = b as c_char;
        }
        *op.add(len) = 0;

        q
    }

    /*
     * Join two single-operand TSQuerys with OP_AND via join_tsqueries+QTN2QT and
     * confirm the resulting flat query has 3 nodes (2 operands + the AND op) and
     * the right root operator.
     */
    #[test]
    fn and_join_numnode_is_three() {
        unsafe {
            let a = make_single(b"alpha", 11);
            let b = make_single(b"beta", 22);

            let res = join_tsqueries(a, b, OP_AND, 0);
            let q = QTN2QT(res);
            QTNFree(res);

            assert_eq!((*q).size, 3);

            let items = GETQUERY(q);
            assert_eq!((*items.add(0)).type_(), QI_OPR);
            assert_eq!((*items.add(0)).qoperator.oper, OP_AND);

            pfree(q as *mut c_void);
            pfree(a as *mut c_void);
            pfree(b as *mut c_void);
        }
    }

    /* CompareTSQ of two structurally-equal queries must be 0; differing ones not. */
    #[test]
    fn comparetsq_equal_and_not() {
        unsafe {
            let a1 = make_single(b"same", 7);
            let a2 = make_single(b"same", 7);
            assert_eq!(CompareTSQ(a1, a2), 0);

            let b = make_single(b"different", 9);
            assert!(CompareTSQ(a1, b) != 0);

            pfree(a1 as *mut c_void);
            pfree(a2 as *mut c_void);
            pfree(b as *mut c_void);
        }
    }

    /* makeTSQuerySign sets exactly the bit for a single operand's valcrc. */
    #[test]
    fn sign_single_operand() {
        unsafe {
            let crc: int32 = 123;
            let a = make_single(b"w", crc);
            let sign = makeTSQuerySign(a);
            let expect = (1 as TSQuerySign) << ((crc as u32) % TSQS_SIGLEN);
            assert_eq!(sign, expect);
            pfree(a as *mut c_void);
        }
    }

    /* qunique_ptr collapses a sorted run of equal strings. */
    #[test]
    fn qunique_ptr_dedup() {
        unsafe {
            let s_a = b"a\0".as_ptr() as *mut c_char;
            let s_a2 = b"a\0".as_ptr() as *mut c_char;
            let s_b = b"b\0".as_ptr() as *mut c_char;
            let mut arr: [*mut c_char; 3] = [s_a, s_a2, s_b];
            let n = qunique_ptr(arr.as_mut_ptr(), 3, cmp_string);
            assert_eq!(n, 2);
            assert_eq!(strcmp(arr[0], b"a\0".as_ptr() as *const c_char), 0);
            assert_eq!(strcmp(arr[1], b"b\0".as_ptr() as *const c_char), 0);
        }
    }

    /*
     * collectTSQueryValues pulls the operand string out of a single-operand
     * query as a '\0'-terminated copy.
     */
    #[test]
    fn collect_values_single() {
        unsafe {
            let a = make_single(b"hello", 5);
            let mut n: c_int = 0;
            let vals = collectTSQueryValues(a, &mut n);
            assert_eq!(n, 1);
            assert_eq!(strcmp(*vals.add(0), b"hello\0".as_ptr() as *const c_char), 0);
            pfree(*vals.add(0) as *mut c_void);
            pfree(vals as *mut c_void);
            pfree(a as *mut c_void);
        }
    }
}

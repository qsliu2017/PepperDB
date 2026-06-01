//! Translation of postgres/src/backend/utils/adt/tsquery_util.c
//!
//! Utilities for the `tsquery` datatype: QTNode (query-tree node) helpers used
//! by tsquery rewrite/optimization (QT2QTN / QTN2QT / QTNFree / QTNSort /
//! QTNTernary / QTNBinary / QTNCopy / QTNClearFlags / QTNodeCompare / QTNEq).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   postgres.h            -> crate::prelude::*  (Datum, palloc/palloc0/pfree/repalloc,
//!                            elog!/ereport!/errmsg!, MaxAllocSize, Assert).
//!   miscadmin.h           -> check_stack_depth() (local no-op, mirrors json.rs/like.rs).
//!   tsearch/ts_utils.h    -> the QTNode struct + QTN_* flag consts (MERGED in below),
//!                            and tsCompareString (imported from the sibling
//!                            crate::utils::adt::tsvector, which translated it inline).
//!   varatt.h              -> SET_VARSIZE / VARHDRSZ (crate::varatt).
//!
//! ts_type.h node structs (QueryItemType, QueryOperand, QueryOperator, QueryItem,
//! TSQuery) are NOT yet provided by a sibling `tsquery` module (tsquery.c is not yet
//! ported), so they are RE-DECLARED module-locally here with a TODO to unify.  They
//! are `#[repr(C)]` with the exact C field layout because the code does raw
//! `memcpy(sizeof(QueryOperand))` / `memcpy(sizeof(QueryOperator))` between the union
//! and the flat tsquery buffer.
//!
//! TODO(pg-port): once src/utils/adt/tsquery.rs exists and exports QueryItemType /
//! QueryOperand / QueryOperator / QueryItem / TSQuery / the COMPUTESIZE etc. macros,
//! drop the module-local copies below and `use crate::utils::adt::tsquery::*;`.
//!
//! TRANSLATED (everything in tsquery_util.c):
//!   QT2QTN, QTNFree, QTNodeCompare, cmpQTN, QTNSort, QTNEq, QTNTernary, QTNBinary,
//!   cntsize, fillQT, QTN2QT, QTNCopy, QTNClearFlags.
//!
//! NOT in this file: makeTSQuerySign (lives in tsquery_gist.c, not tsquery_util.c),
//! findsubquery (tsquery_rewrite.c).  Nothing GiST-related is pulled in here, so no
//! signature stubs are required.

use crate::prelude::*;
use crate::varatt::*;

use crate::c::{int16, int32, uint16, uint32, Size};
use crate::utils::adt::tsvector::tsCompareString;

use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

/*
 * check_stack_depth (miscadmin.h): the recursion guard.  The real implementation
 * compares the current stack pointer against max_stack_depth; the port stubs it as
 * a no-op (same as nodes/nodeFuncs.rs, utils/adt/like.rs, utils/adt/json.rs).
 */
#[inline]
fn check_stack_depth() {}

// ================================================================
//   tsearch/ts_type.h  (tsquery node structs)  --  RE-DECLARED LOCALLY
//   TODO(pg-port): unify with crate::utils::adt::tsquery once that exists.
// ================================================================

/* typedef int8 QueryItemType; */
pub type QueryItemType = int8;

/* Valid values for QueryItemType: */
pub const QI_VAL: int8 = 1;
pub const QI_OPR: int8 = 2;
/* QI_VALSTOP is only used in an intermediate stack representation in parse_tsquery. */
pub const QI_VALSTOP: int8 = 3;

/*
 * QueryOperand is one node in tsquery - an operand.
 *
 * C layout (TSQuery is 4-byte aligned; no field needs 8-byte alignment):
 *   QueryItemType type;   // int8   @ 0
 *   uint8 weight;         //        @ 1
 *   bool  prefix;         //        @ 2  (1 byte)
 *                         //  pad   @ 3
 *   int32 valcrc;         //        @ 4
 *   uint32 length:12, distance:20;  // @ 8 (one uint32 word)
 * sizeof == 12, align == 4.
 *
 * The `length`/`distance` bitfield is modeled as a single u32 (`lendist`) with
 * accessors; the bit packing follows the platform ABI used by PostgreSQL on
 * x86_64/aarch64 (little-endian, fields packed from the least-significant bit
 * upward): length in bits 0..11, distance in bits 12..31.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QueryOperand {
    pub r#type: QueryItemType,
    pub weight: u8,
    pub prefix: bool,
    _pad: u8,
    pub valcrc: int32,
    /* uint32 length:12, distance:20; */
    pub lendist: uint32,
}

const QOPERAND_LENGTH_MASK: uint32 = (1 << 12) - 1; /* 12 bits */
const QOPERAND_DISTANCE_SHIFT: u32 = 12;

impl QueryOperand {
    #[inline]
    pub fn length(&self) -> uint32 {
        self.lendist & QOPERAND_LENGTH_MASK
    }
    #[inline]
    pub fn distance(&self) -> uint32 {
        self.lendist >> QOPERAND_DISTANCE_SHIFT
    }
    #[inline]
    pub fn set_distance(&mut self, v: uint32) {
        self.lendist = (self.lendist & QOPERAND_LENGTH_MASK) | (v << QOPERAND_DISTANCE_SHIFT);
    }
}

/*
 * Legal values for QueryOperator.oper.
 */
pub const OP_NOT: int8 = 1;
pub const OP_AND: int8 = 2;
pub const OP_OR: int8 = 3;
pub const OP_PHRASE: int8 = 4; /* highest code, tsquery_cleanup.c */
pub const OP_COUNT: usize = 4;

/*
 * QueryOperator is one node in tsquery - an operator.
 *
 * C layout:
 *   QueryItemType type;   // int8  @ 0
 *   int8  oper;           //       @ 1
 *   int16 distance;       //       @ 2
 *   uint32 left;          //       @ 4
 * sizeof == 8, align == 4.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QueryOperator {
    pub r#type: QueryItemType,
    pub oper: int8,
    pub distance: int16,
    pub left: uint32,
}

/*
 * QueryItem - a union of an operand and an operator, both sharing a leading
 * QueryItemType `type` field.  C:
 *   typedef union { QueryItemType type; QueryOperator qoperator; QueryOperand qoperand; }
 * Modeled as a #[repr(C)] union (sizeof == 12 to match QueryOperand).  The `type`
 * tag is read via the `type_()` accessor (it overlaps the leading byte of both
 * variants).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub union QueryItem {
    pub r#type: QueryItemType,
    pub qoperator: QueryOperator,
    pub qoperand: QueryOperand,
}

impl QueryItem {
    /* read the union's discriminant `type` field */
    #[inline]
    pub unsafe fn type_(&self) -> QueryItemType {
        self.r#type
    }
}

/*
 * TSQuery storage:
 *   (vl_len_)(size)(array of QueryItem)(operands as '\0'-terminated c-strings)
 */
// Unified to the canonical TSQueryData/TSQuery in ts_type (was a divergent local stub).
pub use crate::utils::adt::ts_type::{TSQuery, TSQueryData};

/* HDRSIZETQ = VARHDRSZ + sizeof(int32) */
#[inline]
pub fn HDRSIZETQ() -> usize {
    VARHDRSZ as usize + core::mem::size_of::<int32>()
}

/*
 * COMPUTESIZE(size, lenofoperand): size of header + all QueryItems + operands.
 * size is the number of QueryItems; lenofoperand is the total length of operands.
 */
#[inline]
pub fn COMPUTESIZE(size: c_int, lenofoperand: c_int) -> usize {
    HDRSIZETQ() + (size as usize) * core::mem::size_of::<QueryItem>() + (lenofoperand as usize)
}

/*
 * TSQUERY_TOO_BIG(size, lenofoperand):
 *   (size) > (MaxAllocSize - HDRSIZETQ - (lenofoperand)) / sizeof(QueryItem)
 */
#[inline]
pub fn TSQUERY_TOO_BIG(size: c_int, lenofoperand: c_int) -> bool {
    (size as Size)
        > (MaxAllocSize - HDRSIZETQ() as Size - lenofoperand as Size)
            / core::mem::size_of::<QueryItem>() as Size
}

/* GETQUERY(x): pointer to the first QueryItem in a TSQuery. */
#[inline]
pub unsafe fn GETQUERY(x: TSQuery) -> *mut QueryItem {
    (x as *mut c_char).add(HDRSIZETQ()) as *mut QueryItem
}

/* GETOPERAND(x): pointer to the beginning of operands in a TSQuery. */
#[inline]
pub unsafe fn GETOPERAND(x: TSQuery) -> *mut c_char {
    (GETQUERY(x) as *mut c_char).add((*x).size as usize * core::mem::size_of::<QueryItem>())
}

// ================================================================
//   tsearch/ts_utils.h  (QTNode + QTN_* flags)  --  MERGED IN
// ================================================================

/*
 * QTNode - one node in a query tree built from a flat QueryItem array.
 */
#[repr(C)]
pub struct QTNode {
    pub valnode: *mut QueryItem,
    pub flags: uint32,
    pub nchild: int32,
    pub word: *mut c_char,
    pub sign: uint32,
    pub child: *mut *mut QTNode,
}

/* bits in QTNode.flags */
pub const QTN_NEEDFREE: uint32 = 0x01;
pub const QTN_NOCHANGE: uint32 = 0x02;
pub const QTN_WORDFREE: uint32 = 0x04;

// ================================================================
//   tsquery_util.c
// ================================================================

/*
 * Build QTNode tree for a tsquery given in QueryItem array format.
 */
pub unsafe fn QT2QTN(in_: *mut QueryItem, operand: *mut c_char) -> *mut QTNode {
    let node = palloc0(core::mem::size_of::<QTNode>()) as *mut QTNode;

    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    (*node).valnode = in_;

    if (*in_).type_() == QI_OPR {
        (*node).child = palloc0(core::mem::size_of::<*mut QTNode>() * 2) as *mut *mut QTNode;
        *(*node).child.add(0) = QT2QTN(in_.add(1), operand);
        (*node).sign = (**(*node).child.add(0)).sign;
        if (*in_).qoperator.oper == OP_NOT {
            (*node).nchild = 1;
        } else {
            (*node).nchild = 2;
            *(*node).child.add(1) = QT2QTN(in_.add((*in_).qoperator.left as usize), operand);
            (*node).sign |= (**(*node).child.add(1)).sign;
        }
    } else if !operand.is_null() {
        (*node).word = operand.add((*in_).qoperand.distance() as usize);
        (*node).sign = (1u32) << (((*in_).qoperand.valcrc as u32) % 32);
    }

    node
}

/*
 * Free a QTNode tree.
 *
 * Referenced "word" and "valnode" items are freed if marked as transient
 * by flags.
 */
pub unsafe fn QTNFree(in_: *mut QTNode) {
    if in_.is_null() {
        return;
    }

    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (*(*in_).valnode).type_() == QI_VAL
        && !(*in_).word.is_null()
        && ((*in_).flags & QTN_WORDFREE) != 0
    {
        pfree((*in_).word as *mut c_void);
    }

    if (*(*in_).valnode).type_() == QI_OPR {
        let mut i: c_int = 0;
        while i < (*in_).nchild {
            QTNFree(*(*in_).child.add(i as usize));
            i += 1;
        }
    }
    if !(*in_).child.is_null() {
        pfree((*in_).child as *mut c_void);
    }

    if (*in_).flags & QTN_NEEDFREE != 0 {
        pfree((*in_).valnode as *mut c_void);
    }

    pfree(in_ as *mut c_void);
}

/*
 * Sort comparator for QTNodes.
 *
 * The sort order is somewhat arbitrary.
 */
pub unsafe fn QTNodeCompare(an: *mut QTNode, bn: *mut QTNode) -> c_int {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (*(*an).valnode).type_() != (*(*bn).valnode).type_() {
        return if (*(*an).valnode).type_() > (*(*bn).valnode).type_() {
            -1
        } else {
            1
        };
    }

    if (*(*an).valnode).type_() == QI_OPR {
        let ao: *mut QueryOperator = &mut (*(*an).valnode).qoperator;
        let bo: *mut QueryOperator = &mut (*(*bn).valnode).qoperator;

        if (*ao).oper != (*bo).oper {
            return if (*ao).oper > (*bo).oper { -1 } else { 1 };
        }

        if (*an).nchild != (*bn).nchild {
            return if (*an).nchild > (*bn).nchild { -1 } else { 1 };
        }

        {
            let mut i: c_int = 0;
            let mut res: c_int;
            while i < (*an).nchild {
                res = QTNodeCompare(
                    *(*an).child.add(i as usize),
                    *(*bn).child.add(i as usize),
                );
                if res != 0 {
                    return res;
                }
                i += 1;
            }
        }

        if (*ao).oper == OP_PHRASE && (*ao).distance != (*bo).distance {
            return if (*ao).distance > (*bo).distance {
                -1
            } else {
                1
            };
        }

        0
    } else if (*(*an).valnode).type_() == QI_VAL {
        let ao: *mut QueryOperand = &mut (*(*an).valnode).qoperand;
        let bo: *mut QueryOperand = &mut (*(*bn).valnode).qoperand;

        if (*ao).valcrc != (*bo).valcrc {
            return if (*ao).valcrc > (*bo).valcrc { -1 } else { 1 };
        }

        tsCompareString(
            (*an).word,
            (*ao).length() as c_int,
            (*bn).word,
            (*bo).length() as c_int,
            false,
        )
    } else {
        elog!(
            ERROR,
            "unrecognized QueryItem type: {}",
            (*(*an).valnode).type_()
        );
        0 /* keep compiler quiet */
    }
}

/*
 * qsort comparator for QTNode pointers.
 *
 * C: static int cmpQTN(const void *a, const void *b)
 *      { return QTNodeCompare(*(QTNode *const *) a, *(QTNode *const *) b); }
 */
unsafe fn cmpQTN(a: *const c_void, b: *const c_void) -> c_int {
    QTNodeCompare(
        *(a as *const *mut QTNode),
        *(b as *const *mut QTNode),
    )
}

/*
 * Canonicalize a QTNode tree by sorting the children of AND/OR nodes
 * into an arbitrary but well-defined order.
 */
pub unsafe fn QTNSort(in_: *mut QTNode) {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (*(*in_).valnode).type_() != QI_OPR {
        return;
    }

    let mut i: c_int = 0;
    while i < (*in_).nchild {
        QTNSort(*(*in_).child.add(i as usize));
        i += 1;
    }
    if (*in_).nchild > 1 && (*(*in_).valnode).qoperator.oper != OP_PHRASE {
        /* qsort(in->child, in->nchild, sizeof(QTNode *), cmpQTN); */
        let sl = core::slice::from_raw_parts_mut((*in_).child, (*in_).nchild as usize);
        sl.sort_by(|x, y| {
            let r = cmpQTN(
                x as *const *mut QTNode as *const c_void,
                y as *const *mut QTNode as *const c_void,
            );
            r.cmp(&0)
        });
    }
}

/*
 * Are two QTNode trees equal according to QTNodeCompare?
 */
pub unsafe fn QTNEq(a: *mut QTNode, b: *mut QTNode) -> bool {
    let sign: uint32 = (*a).sign & (*b).sign;

    if !(sign == (*a).sign && sign == (*b).sign) {
        return false;
    }

    QTNodeCompare(a, b) == 0
}

/*
 * Remove unnecessary intermediate nodes. For example:
 *
 *	    OR			OR
 *	a      OR	-> a b c
 *	     b	  c
 */
pub unsafe fn QTNTernary(in_: *mut QTNode) {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (*(*in_).valnode).type_() != QI_OPR {
        return;
    }

    let mut i: c_int = 0;
    while i < (*in_).nchild {
        QTNTernary(*(*in_).child.add(i as usize));
        i += 1;
    }

    /* Only AND and OR are associative, so don't flatten other node types */
    if (*(*in_).valnode).qoperator.oper != OP_AND && (*(*in_).valnode).qoperator.oper != OP_OR {
        return;
    }

    i = 0;
    while i < (*in_).nchild {
        let cc: *mut QTNode = *(*in_).child.add(i as usize);

        if (*(*cc).valnode).type_() == QI_OPR
            && (*(*in_).valnode).qoperator.oper == (*(*cc).valnode).qoperator.oper
        {
            let oldnchild: c_int = (*in_).nchild;

            (*in_).nchild += (*cc).nchild - 1;
            (*in_).child = repalloc(
                (*in_).child as *mut c_void,
                (*in_).nchild as usize * core::mem::size_of::<*mut QTNode>(),
            ) as *mut *mut QTNode;

            if i + 1 != oldnchild {
                memmove(
                    (*in_).child.add((i + (*cc).nchild) as usize) as *mut c_void,
                    (*in_).child.add((i + 1) as usize) as *const c_void,
                    (oldnchild - i - 1) as usize * core::mem::size_of::<*mut QTNode>(),
                );
            }

            memcpy(
                (*in_).child.add(i as usize) as *mut c_void,
                (*cc).child as *const c_void,
                (*cc).nchild as usize * core::mem::size_of::<*mut QTNode>(),
            );
            i += (*cc).nchild - 1;

            if (*cc).flags & QTN_NEEDFREE != 0 {
                pfree((*cc).valnode as *mut c_void);
            }
            pfree(cc as *mut c_void);
        }

        i += 1;
    }
}

/*
 * Convert a tree to binary tree by inserting intermediate nodes.
 * (Opposite of QTNTernary)
 */
pub unsafe fn QTNBinary(in_: *mut QTNode) {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (*(*in_).valnode).type_() != QI_OPR {
        return;
    }

    let mut i: c_int = 0;
    while i < (*in_).nchild {
        QTNBinary(*(*in_).child.add(i as usize));
        i += 1;
    }

    while (*in_).nchild > 2 {
        let nn = palloc0(core::mem::size_of::<QTNode>()) as *mut QTNode;

        (*nn).valnode = palloc0(core::mem::size_of::<QueryItem>()) as *mut QueryItem;
        (*nn).child = palloc0(core::mem::size_of::<*mut QTNode>() * 2) as *mut *mut QTNode;

        (*nn).nchild = 2;
        (*nn).flags = QTN_NEEDFREE;

        *(*nn).child.add(0) = *(*in_).child.add(0);
        *(*nn).child.add(1) = *(*in_).child.add(1);
        (*nn).sign = (**(*nn).child.add(0)).sign | (**(*nn).child.add(1)).sign;

        (*(*nn).valnode).r#type = (*(*in_).valnode).type_();
        (*(*nn).valnode).qoperator.oper = (*(*in_).valnode).qoperator.oper;

        *(*in_).child.add(0) = nn;
        *(*in_).child.add(1) = *(*in_).child.add(((*in_).nchild - 1) as usize);
        (*in_).nchild -= 1;
    }
}

/*
 * Count the total length of operand strings in tree (including '\0'-
 * terminators) and the total number of nodes.
 * Caller must initialize *sumlen and *nnode to zeroes.
 */
unsafe fn cntsize(in_: *mut QTNode, sumlen: *mut c_int, nnode: *mut c_int) {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    *nnode += 1;
    if (*(*in_).valnode).type_() == QI_OPR {
        let mut i: c_int = 0;
        while i < (*in_).nchild {
            cntsize(*(*in_).child.add(i as usize), sumlen, nnode);
            i += 1;
        }
    } else {
        *sumlen += (*(*in_).valnode).qoperand.length() as c_int + 1;
    }
}

struct QTN2QTState {
    curitem: *mut QueryItem,
    operand: *mut c_char,
    curoperand: *mut c_char,
}

/*
 * Recursively convert a QTNode tree into flat tsquery format.
 * Caller must have allocated arrays of the correct size.
 */
unsafe fn fillQT(state: *mut QTN2QTState, in_: *mut QTNode) {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    if (*(*in_).valnode).type_() == QI_VAL {
        memcpy(
            (*state).curitem as *mut c_void,
            (*in_).valnode as *const c_void,
            core::mem::size_of::<QueryOperand>(),
        );

        memcpy(
            (*state).curoperand as *mut c_void,
            (*in_).word as *const c_void,
            (*(*in_).valnode).qoperand.length() as usize,
        );
        let dist = (*state).curoperand as isize - (*state).operand as isize;
        (*(*state).curitem).qoperand.set_distance(dist as uint32);
        *(*state)
            .curoperand
            .add((*(*in_).valnode).qoperand.length() as usize) = 0; /* '\0' */
        (*state).curoperand = (*state)
            .curoperand
            .add((*(*in_).valnode).qoperand.length() as usize + 1);
        (*state).curitem = (*state).curitem.add(1);
    } else {
        let curitem: *mut QueryItem = (*state).curitem;

        Assert!((*(*in_).valnode).type_() == QI_OPR);

        memcpy(
            (*state).curitem as *mut c_void,
            (*in_).valnode as *const c_void,
            core::mem::size_of::<QueryOperator>(),
        );

        Assert!((*in_).nchild <= 2);
        (*state).curitem = (*state).curitem.add(1);

        fillQT(state, *(*in_).child.add(0));

        if (*in_).nchild == 2 {
            (*curitem).qoperator.left =
                ((*state).curitem as isize - curitem as isize) as uint32
                    / core::mem::size_of::<QueryItem>() as uint32;
            fillQT(state, *(*in_).child.add(1));
        }
    }
}

/*
 * Build flat tsquery from a QTNode tree.
 */
pub unsafe fn QTN2QT(in_: *mut QTNode) -> TSQuery {
    let out: TSQuery;
    let len: c_int;
    let mut sumlen: c_int = 0;
    let mut nnode: c_int = 0;
    let mut state = QTN2QTState {
        curitem: null_mut(),
        operand: null_mut(),
        curoperand: null_mut(),
    };

    cntsize(in_, &mut sumlen, &mut nnode);

    if TSQUERY_TOO_BIG(nnode, sumlen) {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(ERROR, errmsg!("tsquery is too large"));
    }
    len = COMPUTESIZE(nnode, sumlen) as c_int;

    out = palloc0(len as usize) as TSQuery;
    SET_VARSIZE(out as *mut c_char, len);
    (*out).size = nnode;

    state.curitem = GETQUERY(out);
    state.operand = GETOPERAND(out);
    state.curoperand = state.operand;

    fillQT(&mut state, in_);
    out
}

/*
 * Copy a QTNode tree.
 *
 * Modifiable copies of the words and valnodes are made, too.
 */
pub unsafe fn QTNCopy(in_: *mut QTNode) -> *mut QTNode {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    let out = palloc(core::mem::size_of::<QTNode>()) as *mut QTNode;

    *out = core::ptr::read(in_);
    (*out).valnode = palloc(core::mem::size_of::<QueryItem>()) as *mut QueryItem;
    *(*out).valnode = *(*in_).valnode;
    (*out).flags |= QTN_NEEDFREE;

    if (*(*in_).valnode).type_() == QI_VAL {
        (*out).word = palloc((*(*in_).valnode).qoperand.length() as usize + 1) as *mut c_char;
        memcpy(
            (*out).word as *mut c_void,
            (*in_).word as *const c_void,
            (*(*in_).valnode).qoperand.length() as usize,
        );
        *(*out)
            .word
            .add((*(*in_).valnode).qoperand.length() as usize) = 0; /* '\0' */
        (*out).flags |= QTN_WORDFREE;
    } else {
        (*out).child = palloc(core::mem::size_of::<*mut QTNode>() * (*in_).nchild as usize)
            as *mut *mut QTNode;

        let mut i: c_int = 0;
        while i < (*in_).nchild {
            *(*out).child.add(i as usize) = QTNCopy(*(*in_).child.add(i as usize));
            i += 1;
        }
    }

    out
}

/*
 * Clear the specified flag bit(s) in all nodes of a QTNode tree.
 */
pub unsafe fn QTNClearFlags(in_: *mut QTNode, flags: uint32) {
    /* since this function recurses, it could be driven to stack overflow. */
    check_stack_depth();

    (*in_).flags &= !flags;

    if (*(*in_).valnode).type_() != QI_VAL {
        let mut i: c_int = 0;
        while i < (*in_).nchild {
            QTNClearFlags(*(*in_).child.add(i as usize), flags);
            i += 1;
        }
    }
}

// ================================================================
//   tests
// ================================================================
#[cfg(test)]
mod tests {
    use super::*;

    /* Verify the re-declared node structs match the C ABI sizes/layout. */
    #[test]
    fn struct_sizes_match_c_abi() {
        assert_eq!(core::mem::size_of::<QueryOperand>(), 12);
        assert_eq!(core::mem::align_of::<QueryOperand>(), 4);
        assert_eq!(core::mem::size_of::<QueryOperator>(), 8);
        assert_eq!(core::mem::align_of::<QueryOperator>(), 4);
        /* union is sized to its largest member (QueryOperand). */
        assert_eq!(core::mem::size_of::<QueryItem>(), 12);
        assert_eq!(core::mem::align_of::<QueryItem>(), 4);
    }

    #[test]
    fn queryoperand_bitfield_accessors() {
        let mut op = QueryOperand {
            r#type: QI_VAL,
            weight: 0,
            prefix: false,
            _pad: 0,
            valcrc: 0,
            lendist: 0,
        };
        /* length is 12 bits, distance is 20 bits */
        op.lendist = 0;
        op.set_distance(0xABCDE);
        /* set_distance must not clobber length */
        op.lendist = (op.lendist & !QOPERAND_LENGTH_MASK) | 0x123;
        assert_eq!(op.length(), 0x123);
        assert_eq!(op.distance(), 0xABCDE);
    }

    #[test]
    fn union_type_tag_overlaps_both_variants() {
        unsafe {
            let mut qi = QueryItem {
                qoperator: QueryOperator {
                    r#type: QI_OPR,
                    oper: OP_AND,
                    distance: 0,
                    left: 7,
                },
            };
            assert_eq!(qi.type_(), QI_OPR);
            assert_eq!(qi.qoperator.oper, OP_AND);
            /* writing via the operand view's type updates the shared tag */
            qi.qoperand.r#type = QI_VAL;
            assert_eq!(qi.type_(), QI_VAL);
        }
    }

    /*
     * Build a tiny flat tsquery "a & b" by hand, convert to a QTNode tree with
     * QT2QTN, round-trip back to a TSQuery with QTN2QT, and confirm the tree
     * shape and sizes survive.  Exercises QT2QTN/cntsize/fillQT/QTN2QT/QTNFree.
     */
    #[test]
    fn qt2qtn_roundtrip_and_back() {
        unsafe {
            /* operand strings: "a\0b\0" */
            let operand: [c_char; 4] = [b'a' as c_char, 0, b'b' as c_char, 0];

            /*
             * Flat QueryItem layout for "a & b":
             *   [0] OP_AND, left=2  (left operand at item+1, right at item+left)
             *   [1] operand "a" (distance 0, length 1)
             *   [2] operand "b" (distance 2, length 1)
             */
            let mut items: [QueryItem; 3] = [
                QueryItem {
                    qoperator: QueryOperator {
                        r#type: QI_OPR,
                        oper: OP_AND,
                        distance: 0,
                        left: 2,
                    },
                },
                QueryItem {
                    qoperand: QueryOperand {
                        r#type: QI_VAL,
                        weight: 0,
                        prefix: false,
                        _pad: 0,
                        valcrc: 100,
                        lendist: 0,
                    },
                },
                QueryItem {
                    qoperand: QueryOperand {
                        r#type: QI_VAL,
                        weight: 0,
                        prefix: false,
                        _pad: 0,
                        valcrc: 200,
                        lendist: 0,
                    },
                },
            ];
            /* length=1, distance=0 for "a" */
            items[1].qoperand.lendist = 1;
            /* length=1, distance=2 for "b" */
            items[2].qoperand.lendist = 1 | (2u32 << QOPERAND_DISTANCE_SHIFT);

            let tree = QT2QTN(items.as_mut_ptr(), operand.as_ptr() as *mut c_char);
            assert!(!tree.is_null());
            assert_eq!((*(*tree).valnode).type_(), QI_OPR);
            assert_eq!((*tree).nchild, 2);
            /* root sign = OR of children's single-bit signs */
            let lsign = (1u32) << (100u32 % 32);
            let rsign = (1u32) << (200u32 % 32);
            assert_eq!((*tree).sign, lsign | rsign);

            /* left/right children are the two operands */
            let lc = *(*tree).child.add(0);
            let rc = *(*tree).child.add(1);
            assert_eq!((*(*lc).valnode).type_(), QI_VAL);
            assert_eq!((*(*rc).valnode).type_(), QI_VAL);
            assert_eq!(*(*lc).word, b'a' as c_char);
            assert_eq!(*(*rc).word, b'b' as c_char);

            /* round-trip back to a flat TSQuery */
            let q = QTN2QT(tree);
            assert!(!q.is_null());
            assert_eq!((*q).size, 3);
            let qitems = GETQUERY(q);
            assert_eq!((*qitems.add(0)).type_(), QI_OPR);
            assert_eq!((*qitems.add(0)).qoperator.oper, OP_AND);
            assert_eq!((*qitems.add(0)).qoperator.left, 2);
            assert_eq!((*qitems.add(1)).type_(), QI_VAL);
            assert_eq!((*qitems.add(2)).type_(), QI_VAL);

            /* operand strings preserved */
            let qop = GETOPERAND(q);
            assert_eq!(*qop.add((*qitems.add(1)).qoperand.distance() as usize), b'a' as c_char);
            assert_eq!(*qop.add((*qitems.add(2)).qoperand.distance() as usize), b'b' as c_char);

            QTNFree(tree);
            pfree(q as *mut c_void);
        }
    }

    /*
     * QTNCopy then QTNEq: a fresh copy must compare equal to its source, and
     * QTNClearFlags must clear the QTN_NEEDFREE/QTN_WORDFREE bits set by the copy.
     */
    #[test]
    fn qtncopy_eq_and_clearflags() {
        unsafe {
            let operand: [c_char; 2] = [b'x' as c_char, 0];
            let mut item = QueryItem {
                qoperand: QueryOperand {
                    r#type: QI_VAL,
                    weight: 0,
                    prefix: false,
                    _pad: 0,
                    valcrc: 42,
                    lendist: 1, /* length 1, distance 0 */
                },
            };
            let tree = QT2QTN(&mut item, operand.as_ptr() as *mut c_char);
            let copy = QTNCopy(tree);

            assert!(QTNEq(tree, copy));
            assert!((*copy).flags & QTN_NEEDFREE != 0);
            assert!((*copy).flags & QTN_WORDFREE != 0);

            QTNClearFlags(copy, QTN_NEEDFREE | QTN_WORDFREE);
            assert_eq!((*copy).flags & (QTN_NEEDFREE | QTN_WORDFREE), 0);

            /* tree's valnode points into the stack `item`, so only free word/child */
            QTNFree(tree);
            /* copy still owns its valnode (flag cleared); free its pieces manually */
            pfree((*copy).valnode as *mut c_void);
            pfree((*copy).word as *mut c_void);
            pfree(copy as *mut c_void);
        }
    }
}

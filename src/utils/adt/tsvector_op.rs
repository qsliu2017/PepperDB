//! Translation of postgres/src/backend/utils/adt/tsvector_op.c
//!
//! Operations over the `tsvector` type: comparison, strip, setweight, concat,
//! length, conversion to/from arrays, unnest, the tsquery match (@@) family,
//! ts_stat, and the tsvector update trigger.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   limits.h                 -> INT_MAX (core::i32::MAX) where TS_phrase_output needs it
//!   access/htup_details.h    -> heap_form_tuple / heap_modify_tuple_by_cols (executor) - STUB
//!   catalog/namespace.h      -> stringToQualifiedNameList / get_ts_config_oid       - STUB
//!   catalog/pg_type.h        -> TEXTOID / INT2OID / CHAROID / TSVECTOROID ... (oids)  - STUB
//!   commands/trigger.h       -> TriggerData / CALLED_AS_TRIGGER / TRIGGER_FIRED_*    - STUB
//!   common/int.h             -> crate::common::int (pg_cmp_s32)
//!   executor/spi.h           -> SPI_* (ts_stat_sql)                                   - STUB
//!   funcapi.h                -> set-returning-function (SRF) machinery                - STUB
//!   lib/qunique.h            -> qunique (used by array/prefix paths)                  - STUB
//!   mb/pg_wchar.h            -> pg_mblen (ts_stat_sql weight scan)                     - STUB
//!   miscadmin.h              -> check_stack_depth / CHECK_FOR_INTERRUPTS              - STUB
//!   parser/parse_coerce.h    -> IsBinaryCoercible                                     - STUB
//!   tsearch/ts_utils.h       -> TSQuery / QueryItem / QueryOperand / ExecPhraseData /
//!                               TSExecuteCallback / TSTernaryValue and the @@ engine.
//!                               tsquery.c is NOT yet ported, so the entire TSQuery
//!                               match family (TS_execute*, checkcondition_*,
//!                               TS_phrase_*, ts_match_*) is STUBBED.
//!   utils/array.h            -> ArrayType / construct_array_builtin /
//!                               deconstruct_array_builtin.  construct/deconstruct are
//!                               NOT yet ported, so every array-consuming/producing
//!                               function is STUBBED.
//!   utils/builtins.h         -> cstring_to_text_with_len (crate::utils::adt::varlena)
//!   utils/regproc.h, utils/rel.h -> regproc / Relation helpers (trigger)             - STUB
//!
//!   The TSVector type + its access macros and the lexeme/position helpers
//!   (WordEntry / WordEntryPos / WordEntryPosVector / ARRPTR / STRPTR / _POSVECPTR /
//!   POSDATALEN / POSDATAPTR / CALCDATASIZE / WEP_* / LIMITPOS / MAXNUMPOS /
//!   MAXENTRYPOS / MAXSTRPOS / DatumGetTSVector / TSVectorGetDatum / tsCompareString)
//!   are all imported from the already-ported sibling crate::utils::adt::tsvector.
//!
//! NOTE: tsCompareString physically lives in this C file, but the sibling
//! tsvector.rs already translated it inline (compareentry there depends on it).
//! To avoid a duplicate definition we IMPORT it here rather than re-defining it.
//!
//! TRANSLATED FULLY (self-contained over the ported TSVector):
//!   silly_cmp_tsvector, tsvector_lt/le/eq/ge/gt/ne/cmp (the TSVECTORCMPFUNC family),
//!   tsvector_strip, tsvector_length, tsvector_setweight, add_pos, tsvector_bsearch,
//!   tsvector_concat.
//!
//! STUBBED (deps not yet ported):
//!   - tsvector_setweight_by_filter, tsvector_to_array, array_to_tsvector,
//!     tsvector_filter, tsvector_delete_str/_arr, tsvector_delete_by_indices,
//!     compare_int, compare_text_lexemes  -> need utils/array.h construct/deconstruct.
//!   - tsvector_unnest, ts_stat1/2          -> need funcapi SRF (+ array / SPI).
//!   - the @@ family: checkclass_str, checkcondition_str, TS_phrase_output,
//!     TS_phrase_execute, TS_execute, TS_execute_ternary, TS_execute_recurse,
//!     TS_execute_locations(_recurse), tsquery_requires_match, ts_match_qv/vq/tt/tq
//!     -> need the TSQuery type (tsquery.c not yet ported).
//!   - ts_accum/insertStatEntry/... and tsvector_update_trigger* -> need SPI / executor
//!     / trigger manager / catalog.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;

use crate::{
    PG_GETARG_CHAR, PG_GETARG_DATUM, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_INT32,
    PG_RETURN_POINTER, PG_FREE_IF_COPY, PG_RETURN_DATUM, DirectFunctionCall2,
    list_make1, foreach, current_cell,
};
// pg_list support used by TS_execute_locations (the @@ location-list engine).
use crate::nodes::pg_list::{List, NIL, lappend, lfirst, list_concat};
use crate::c::{int32, uint16, uint32, Size, SHORTALIGN};
use crate::common::int::pg_cmp_s32;
use crate::utils::adt::tsvector::{
    TSVector, WordEntry, WordEntryPos, WordEntryPosVector, ARRPTR, CALCDATASIZE, DatumGetTSVector,
    LIMITPOS, MAXENTRYPOS, MAXNUMPOS, MAXSTRPOS, POSDATALEN, POSDATAPTR, STRPTR, TSVectorGetDatum,
    WEP_GETPOS, WEP_GETWEIGHT, WEP_SETPOS, WEP_SETWEIGHT, _POSVECPTR, compareWordEntryPos,
    tsCompareString,
};
use crate::utils::adt::tsquery_util::{
    QueryItem, QueryOperand, TSQuery, GETOPERAND, GETQUERY, OP_AND, OP_NOT, OP_OR, OP_PHRASE, QI_VAL,
};
use crate::utils::misc::stack_depth::check_stack_depth;
use crate::postgres::DatumGetBool;
use core::ffi::{c_char, c_int, c_void};

/* limits.h INT_MAX, used by TS_phrase_output. */
const INT_MAX: c_int = c_int::MAX;

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

// ================================================================
//   types local to tsvector_op.c
// ================================================================

/*
 * typedef struct { WordEntry *arrb; WordEntry *arre; char *values; char *operand; } CHKVAL;
 *
 * The opaque `arg` threaded through TS_execute/checkcondition_str: it describes
 * the tsvector being matched (entry array bounds + lexeme/operand storage).
 */
#[repr(C)]
struct CHKVAL {
    arrb: *mut WordEntry,
    arre: *mut WordEntry,
    values: *mut c_char,
    operand: *mut c_char,
}

// ----------------------------------------------------------------
//   tsearch/ts_utils.h: TSQuery execution support
// ----------------------------------------------------------------

/*
 * TS_execute requires ternary logic to handle NOT with phrase matches.
 *
 * typedef enum { TS_NO, TS_YES, TS_MAYBE } TSTernaryValue;
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TSTernaryValue {
    TS_NO = 0,    /* definitely no match */
    TS_YES = 1,   /* definitely does match */
    TS_MAYBE = 2, /* can't verify match for lack of pos data */
}
pub use TSTernaryValue::{TS_MAYBE, TS_NO, TS_YES};

/*
 * struct ExecPhraseData is passed to a TSExecuteCallback function if we need
 * lexeme position data (because of a phrase-match operator in the tsquery).
 * All fields are initially zeroed by the caller.
 */
#[repr(C)]
pub struct ExecPhraseData {
    pub npos: c_int,           /* number of positions reported */
    pub allocated: bool,       /* pos points to palloc'd data? */
    pub negate: bool,          /* positions are where query is NOT matched */
    pub pos: *mut WordEntryPos, /* ordered, non-duplicate lexeme positions */
    pub width: c_int,          /* width of match in lexemes, less 1 */
}

impl ExecPhraseData {
    /* memset(&x, 0, sizeof(x)) equivalent */
    #[inline]
    fn zeroed() -> ExecPhraseData {
        ExecPhraseData {
            npos: 0,
            allocated: false,
            negate: false,
            pos: null_mut(),
            width: 0,
        }
    }
}

/*
 * Signature for TSQuery lexeme check functions.
 *
 * C: typedef TSTernaryValue (*TSExecuteCallback)(void *arg, QueryOperand *val,
 *                                                ExecPhraseData *data);
 * Modeled as a plain unsafe fn pointer, matching the vtable/callback convention
 * used elsewhere in the crate (e.g. AppendState.choose_next_subplan).
 */
pub type TSExecuteCallback =
    unsafe fn(arg: *mut c_void, val: *mut QueryOperand, data: *mut ExecPhraseData) -> TSTernaryValue;

/*
 * Flag bits for TS_execute (ts_utils.h).
 */
pub const TS_EXEC_EMPTY: uint32 = 0x00;
/* NOT sub-expressions are automatically evaluated to be true. */
pub const TS_EXEC_SKIP_NOT: uint32 = 0x01;
/* allow OP_PHRASE to be executed lossily in the absence of position info. */
pub const TS_EXEC_PHRASE_NO_POS: uint32 = 0x02;

/*
 * typedef struct StatEntry { ... } StatEntry;  -- ts_stat support, stubbed.
 */
#[repr(C)]
#[allow(dead_code)]
struct StatEntry {
    ndoc: uint32, /* zero indicates that we were already here while walking the tree */
    nentry: uint32,
    left: *mut StatEntry,
    right: *mut StatEntry,
    lenlexeme: uint32,
    lexeme: [c_char; FLEXIBLE_ARRAY_MEMBER],
}

/* #define STATENTRYHDRSZ (offsetof(StatEntry, lexeme)) */
#[allow(dead_code)]
#[inline]
fn STATENTRYHDRSZ() -> usize {
    core::mem::offset_of!(StatEntry, lexeme)
}

#[repr(C)]
#[allow(dead_code)]
struct TSVectorStat {
    weight: int32,
    maxdepth: uint32,
    stack: *mut *mut StatEntry,
    stackpos: uint32,
    root: *mut StatEntry,
}

// ----------------------------------------------------------------
//   PG_GETARG_TSVECTOR(n): the C macro detoasts; with TOAST unported it is
//   the identity for in-line datums (mirrors tsvector.rs).
// ----------------------------------------------------------------
#[inline]
unsafe fn PG_GETARG_TSVECTOR(datum: Datum) -> TSVector {
    DatumGetTSVector(datum)
}

/*
 * Order: haspos, len, word, for all positions (pos, weight)
 */
unsafe fn silly_cmp_tsvector(a: TSVector, b: TSVector) -> c_int {
    if VARSIZE(a as *const c_char) < VARSIZE(b as *const c_char) {
        return -1;
    } else if VARSIZE(a as *const c_char) > VARSIZE(b as *const c_char) {
        return 1;
    } else if (*a).size < (*b).size {
        return -1;
    } else if (*a).size > (*b).size {
        return 1;
    } else {
        let mut aptr: *mut WordEntry = ARRPTR(a);
        let mut bptr: *mut WordEntry = ARRPTR(b);
        let mut i: c_int = 0;
        let mut res: c_int;

        while i < (*a).size {
            if (*aptr).haspos() != (*bptr).haspos() {
                return if (*aptr).haspos() > (*bptr).haspos() {
                    -1
                } else {
                    1
                };
            } else if {
                res = tsCompareString(
                    STRPTR(a).add((*aptr).pos() as usize),
                    (*aptr).len() as c_int,
                    STRPTR(b).add((*bptr).pos() as usize),
                    (*bptr).len() as c_int,
                    false,
                );
                res != 0
            } {
                return res;
            } else if (*aptr).haspos() != 0 {
                let mut ap: *mut WordEntryPos = POSDATAPTR(a, aptr);
                let mut bp: *mut WordEntryPos = POSDATAPTR(b, bptr);
                let mut j: c_int;

                if POSDATALEN(a, aptr) != POSDATALEN(b, bptr) {
                    return if POSDATALEN(a, aptr) > POSDATALEN(b, bptr) {
                        -1
                    } else {
                        1
                    };
                }

                j = 0;
                while j < POSDATALEN(a, aptr) {
                    if WEP_GETPOS(*ap) != WEP_GETPOS(*bp) {
                        return if WEP_GETPOS(*ap) > WEP_GETPOS(*bp) { -1 } else { 1 };
                    } else if WEP_GETWEIGHT(*ap) != WEP_GETWEIGHT(*bp) {
                        return if WEP_GETWEIGHT(*ap) > WEP_GETWEIGHT(*bp) {
                            -1
                        } else {
                            1
                        };
                    }
                    ap = ap.add(1);
                    bp = bp.add(1);
                    j += 1;
                }
            }

            aptr = aptr.add(1);
            bptr = bptr.add(1);
            i += 1;
        }
    }

    0
}

/*
 * #define TSVECTORCMPFUNC(type, action, ret) ...
 *
 * The C macro stamps out seven fmgr functions, each of which compares the two
 * argument tsvectors with silly_cmp_tsvector and returns `res action 0`.  We
 * expand them by hand here.
 */
macro_rules! TSVECTORCMPFUNC {
    ($name:ident, $cmp:tt, bool) => {
        pub unsafe fn $name(fcinfo: FunctionCallInfo) -> Datum {
            let a: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
            let b: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 1));
            let res: c_int = silly_cmp_tsvector(a, b);
            PG_FREE_IF_COPY!(fcinfo, a, 0);
            PG_FREE_IF_COPY!(fcinfo, b, 1);
            PG_RETURN_BOOL!(res $cmp 0)
        }
    };
    ($name:ident, $cmp:tt, int32) => {
        pub unsafe fn $name(fcinfo: FunctionCallInfo) -> Datum {
            let a: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
            let b: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 1));
            let res: c_int = silly_cmp_tsvector(a, b);
            PG_FREE_IF_COPY!(fcinfo, a, 0);
            PG_FREE_IF_COPY!(fcinfo, b, 1);
            // cmp variant: action is `+`, i.e. `res + 0`.
            PG_RETURN_INT32!(res $cmp 0)
        }
    };
}

TSVECTORCMPFUNC!(tsvector_lt, <, bool);
TSVECTORCMPFUNC!(tsvector_le, <=, bool);
TSVECTORCMPFUNC!(tsvector_eq, ==, bool);
TSVECTORCMPFUNC!(tsvector_ge, >=, bool);
TSVECTORCMPFUNC!(tsvector_gt, >, bool);
TSVECTORCMPFUNC!(tsvector_ne, !=, bool);
TSVECTORCMPFUNC!(tsvector_cmp, +, int32);

pub unsafe fn tsvector_strip(fcinfo: FunctionCallInfo) -> Datum {
    let in_: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let out: TSVector;
    let mut i: c_int;
    let mut len: c_int = 0;
    let arrin: *mut WordEntry = ARRPTR(in_);
    let arrout: *mut WordEntry;
    let mut cur: *mut c_char;

    i = 0;
    while i < (*in_).size {
        len += (*arrin.add(i as usize)).len() as c_int;
        i += 1;
    }

    let lenb = CALCDATASIZE((*in_).size, len) as c_int;
    out = palloc0(lenb as Size) as TSVector;
    SET_VARSIZE(out as *mut c_char, lenb);
    (*out).size = (*in_).size;
    arrout = ARRPTR(out);
    cur = STRPTR(out);
    i = 0;
    while i < (*in_).size {
        memcpy(
            cur as *mut c_void,
            STRPTR(in_).add((*arrin.add(i as usize)).pos() as usize) as *const c_void,
            (*arrin.add(i as usize)).len() as usize,
        );
        (*arrout.add(i as usize)).set_haspos(0);
        (*arrout.add(i as usize)).set_len((*arrin.add(i as usize)).len());
        (*arrout.add(i as usize)).set_pos((cur as isize - STRPTR(out) as isize) as u32);
        cur = cur.add((*arrout.add(i as usize)).len() as usize);
        i += 1;
    }

    PG_FREE_IF_COPY!(fcinfo, in_, 0);
    PG_RETURN_POINTER!(out)
}

pub unsafe fn tsvector_length(fcinfo: FunctionCallInfo) -> Datum {
    let in_: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let ret: int32 = (*in_).size;

    PG_FREE_IF_COPY!(fcinfo, in_, 0);
    PG_RETURN_INT32!(ret)
}

pub unsafe fn tsvector_setweight(fcinfo: FunctionCallInfo) -> Datum {
    let in_: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let cw: c_char = PG_GETARG_CHAR!(fcinfo, 1);
    let out: TSVector;
    let mut i: c_int;
    let mut j: c_int;
    let mut entry: *mut WordEntry;
    let mut p: *mut WordEntryPos;
    let mut w: c_int = 0;

    match cw as u8 {
        b'A' | b'a' => w = 3,
        b'B' | b'b' => w = 2,
        b'C' | b'c' => w = 1,
        b'D' | b'd' => w = 0,
        _ => {
            /* internal error */
            elog!(ERROR, "unrecognized weight: {}", cw as c_int);
        }
    }

    out = palloc(VARSIZE(in_ as *const c_char) as Size) as TSVector;
    memcpy(
        out as *mut c_void,
        in_ as *const c_void,
        VARSIZE(in_ as *const c_char) as usize,
    );
    entry = ARRPTR(out);
    i = (*out).size;
    while {
        let old = i;
        i -= 1;
        old != 0
    } {
        j = POSDATALEN(out, entry);
        if j != 0 {
            p = POSDATAPTR(out, entry);
            while {
                let old = j;
                j -= 1;
                old != 0
            } {
                WEP_SETWEIGHT(&mut *p, w);
                p = p.add(1);
            }
        }
        entry = entry.add(1);
    }

    PG_FREE_IF_COPY!(fcinfo, in_, 0);
    PG_RETURN_POINTER!(out)
}

/*
 * setweight(tsin tsvector, char_weight "char", lexemes "text"[])
 *
 * Assign weight w to elements of tsin that are listed in lexemes.
 *
 * TODO(pg-port): needs utils/array.h deconstruct_array_builtin (not yet ported).
 */
pub unsafe fn tsvector_setweight_by_filter(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    // C body deconstructs the lexeme text[] array, then for each lexeme does a
    // tsvector_bsearch + WEP_SETWEIGHT loop. Blocked on construct/deconstruct_array.
    unimplemented!("tsvector_setweight_by_filter: utils/array.h deconstruct_array_builtin not yet translated")
}

/*
 * #define compareEntry(pa, a, pb, b) \
 *     tsCompareString((pa) + (a)->pos, (a)->len, (pb) + (b)->pos, (b)->len, false)
 */
#[inline]
unsafe fn compareEntry(
    pa: *mut c_char,
    a: *const WordEntry,
    pb: *mut c_char,
    b: *const WordEntry,
) -> int32 {
    tsCompareString(
        pa.add((*a).pos() as usize),
        (*a).len() as c_int,
        pb.add((*b).pos() as usize),
        (*b).len() as c_int,
        false,
    )
}

/*
 * Add positions from src to dest after offsetting them by maxpos.
 * Return the number added (might be less than expected due to overflow)
 */
unsafe fn add_pos(
    src: TSVector,
    srcptr: *mut WordEntry,
    dest: TSVector,
    destptr: *mut WordEntry,
    maxpos: int32,
) -> int32 {
    let clen: *mut uint16 = &mut (*_POSVECPTR(dest, destptr)).npos;
    let mut i: c_int;
    let slen: uint16 = POSDATALEN(src, srcptr) as uint16;
    let startlen: uint16;
    let spos: *mut WordEntryPos = POSDATAPTR(src, srcptr);
    let dpos: *mut WordEntryPos = POSDATAPTR(dest, destptr);

    if (*destptr).haspos() == 0 {
        *clen = 0;
    }

    startlen = *clen;
    i = 0;
    while (i as uint16) < slen
        && *clen < MAXNUMPOS as uint16
        && (*clen == 0 || WEP_GETPOS(*dpos.add((*clen - 1) as usize)) != MAXENTRYPOS - 1)
    {
        WEP_SETWEIGHT(
            &mut *dpos.add(*clen as usize),
            WEP_GETWEIGHT(*spos.add(i as usize)),
        );
        WEP_SETPOS(
            &mut *dpos.add(*clen as usize),
            LIMITPOS(WEP_GETPOS(*spos.add(i as usize)) + maxpos),
        );
        *clen += 1;
        i += 1;
    }

    if *clen != startlen {
        (*destptr).set_haspos(1);
    }
    (*clen - startlen) as int32
}

/*
 * Perform binary search of given lexeme in TSVector.
 * Returns lexeme position in TSVector's entry array or -1 if lexeme wasn't
 * found.
 */
#[allow(dead_code)]
unsafe fn tsvector_bsearch(tsv: TSVector, lexeme: *mut c_char, lexeme_len: c_int) -> c_int {
    let arrin: *mut WordEntry = ARRPTR(tsv);
    let mut StopLow: c_int = 0;
    let mut StopHigh: c_int = (*tsv).size;
    let mut StopMiddle: c_int;
    let mut cmp: c_int;

    while StopLow < StopHigh {
        StopMiddle = (StopLow + StopHigh) / 2;

        cmp = tsCompareString(
            lexeme,
            lexeme_len,
            STRPTR(tsv).add((*arrin.add(StopMiddle as usize)).pos() as usize),
            (*arrin.add(StopMiddle as usize)).len() as c_int,
            false,
        );

        if cmp < 0 {
            StopHigh = StopMiddle;
        } else if cmp > 0 {
            StopLow = StopMiddle + 1;
        } else {
            /* found it */
            return StopMiddle;
        }
    }

    -1
}

/*
 * qsort comparator functions
 *
 * TODO(pg-port): compare_int / compare_text_lexemes are only used by the
 * array-consuming functions (tsvector_delete_by_indices, array_to_tsvector),
 * which are themselves stubbed pending utils/array.h.  Kept as stubs for parity.
 */
#[allow(dead_code)]
unsafe fn compare_int(va: *const c_void, vb: *const c_void) -> c_int {
    let a: c_int = *(va as *const c_int);
    let b: c_int = *(vb as *const c_int);
    pg_cmp_s32(a, b)
}

#[allow(dead_code)]
unsafe fn compare_text_lexemes(va: *const c_void, vb: *const c_void) -> c_int {
    // C: Datum a/b -> VARDATA_ANY / VARSIZE_ANY_EXHDR -> tsCompareString.
    let a: Datum = *(va as *const Datum);
    let b: Datum = *(vb as *const Datum);
    let alex: *mut c_char = VARDATA_ANY(DatumGetPointer(a) as *const c_char);
    let alex_len: c_int = VARSIZE_ANY_EXHDR(DatumGetPointer(a) as *const c_char) as c_int;
    let blex: *mut c_char = VARDATA_ANY(DatumGetPointer(b) as *const c_char);
    let blex_len: c_int = VARSIZE_ANY_EXHDR(DatumGetPointer(b) as *const c_char) as c_int;

    tsCompareString(alex, alex_len, blex, blex_len, false)
}

/*
 * Internal routine to delete lexemes from TSVector by array of offsets.
 *
 * TODO(pg-port): self-contained over TSVector, but only reached from
 * tsvector_delete_str/_arr, which are blocked on utils/array.h + lib/qunique.h.
 * Stubbed to keep the dependency surface small until those land.
 */
#[allow(dead_code)]
unsafe fn tsvector_delete_by_indices(
    tsv: TSVector,
    indices_to_delete: *mut c_int,
    indices_count: c_int,
) -> TSVector {
    let _ = (tsv, indices_to_delete, indices_count);
    unimplemented!("tsvector_delete_by_indices: lib/qunique.h (qunique) not yet translated")
}

/*
 * Delete given lexeme from tsvector.
 * Implementation of user-level ts_delete(tsvector, text).
 *
 * TODO(pg-port): needs tsvector_delete_by_indices (qunique), stubbed above.
 */
pub unsafe fn tsvector_delete_str(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("tsvector_delete_str: tsvector_delete_by_indices/qunique not yet translated")
}

/*
 * Delete given array of lexemes from tsvector.
 * Implementation of user-level ts_delete(tsvector, text[]).
 *
 * TODO(pg-port): needs utils/array.h deconstruct_array_builtin + qunique.
 */
pub unsafe fn tsvector_delete_arr(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("tsvector_delete_arr: utils/array.h deconstruct_array_builtin not yet translated")
}

/*
 * Expand tsvector as table with following columns:
 *     lexeme: lexeme text
 *     positions: integer array of lexeme positions
 *     weights: char array of weights corresponding to positions
 *
 * TODO(pg-port): set-returning function -> needs funcapi (SRF) + utils/array.h.
 */
pub unsafe fn tsvector_unnest(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("tsvector_unnest: funcapi SRF machinery + utils/array.h not yet translated")
}

/*
 * Convert tsvector to array of lexemes.
 *
 * TODO(pg-port): needs utils/array.h construct_array_builtin (not yet ported).
 */
pub unsafe fn tsvector_to_array(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("tsvector_to_array: utils/array.h construct_array_builtin not yet translated")
}

/*
 * Build tsvector from array of lexemes.
 *
 * TODO(pg-port): needs utils/array.h deconstruct_array_builtin + lib/qunique.h.
 */
pub unsafe fn array_to_tsvector(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("array_to_tsvector: utils/array.h deconstruct_array_builtin not yet translated")
}

/*
 * ts_filter(): keep only lexemes with given weights in tsvector.
 *
 * TODO(pg-port): needs utils/array.h deconstruct_array_builtin (not yet ported).
 */
pub unsafe fn tsvector_filter(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("tsvector_filter: utils/array.h deconstruct_array_builtin not yet translated")
}

pub unsafe fn tsvector_concat(fcinfo: FunctionCallInfo) -> Datum {
    let in1: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let in2: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 1));
    let out: TSVector;
    let mut ptr: *mut WordEntry;
    let mut ptr1: *mut WordEntry;
    let mut ptr2: *mut WordEntry;
    let mut p: *mut WordEntryPos;
    let mut maxpos: c_int = 0;
    let mut i: c_int;
    let mut j: c_int;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut dataoff: c_int;
    let mut output_bytes: c_int;
    let output_size: c_int;
    let data: *mut c_char;
    let data1: *mut c_char;
    let data2: *mut c_char;

    /* Get max position in in1; we'll need this to offset in2's positions */
    ptr = ARRPTR(in1);
    i = (*in1).size;
    while {
        let old = i;
        i -= 1;
        old != 0
    } {
        j = POSDATALEN(in1, ptr);
        if j != 0 {
            p = POSDATAPTR(in1, ptr);
            while {
                let old = j;
                j -= 1;
                old != 0
            } {
                if WEP_GETPOS(*p) > maxpos {
                    maxpos = WEP_GETPOS(*p);
                }
                p = p.add(1);
            }
        }
        ptr = ptr.add(1);
    }

    ptr1 = ARRPTR(in1);
    ptr2 = ARRPTR(in2);
    data1 = STRPTR(in1);
    data2 = STRPTR(in2);
    i1 = (*in1).size;
    i2 = (*in2).size;

    /*
     * Conservative estimate of space needed.  We might need all the data in
     * both inputs, and conceivably add a pad byte before position data for
     * each item where there was none before.
     */
    output_bytes =
        VARSIZE(in1 as *const c_char) as c_int + VARSIZE(in2 as *const c_char) as c_int + i1 + i2;

    out = palloc0(output_bytes as Size) as TSVector;
    SET_VARSIZE(out as *mut c_char, output_bytes);

    /*
     * We must make out->size valid so that STRPTR(out) is sensible.  We'll
     * collapse out any unused space at the end.
     */
    (*out).size = (*in1).size + (*in2).size;

    ptr = ARRPTR(out);
    data = STRPTR(out);
    dataoff = 0;
    while i1 != 0 && i2 != 0 {
        let cmp: c_int = compareEntry(data1, ptr1, data2, ptr2);

        if cmp < 0 {
            /* in1 first */
            (*ptr).set_haspos((*ptr1).haspos());
            (*ptr).set_len((*ptr1).len());
            memcpy(
                data.add(dataoff as usize) as *mut c_void,
                data1.add((*ptr1).pos() as usize) as *const c_void,
                (*ptr1).len() as usize,
            );
            (*ptr).set_pos(dataoff as u32);
            dataoff += (*ptr1).len() as c_int;
            if (*ptr).haspos() != 0 {
                dataoff = SHORTALIGN(dataoff as usize) as c_int;
                memcpy(
                    data.add(dataoff as usize) as *mut c_void,
                    _POSVECPTR(in1, ptr1) as *const c_void,
                    POSDATALEN(in1, ptr1) as usize * core::mem::size_of::<WordEntryPos>()
                        + core::mem::size_of::<uint16>(),
                );
                dataoff += POSDATALEN(in1, ptr1) * core::mem::size_of::<WordEntryPos>() as c_int
                    + core::mem::size_of::<uint16>() as c_int;
            }

            ptr = ptr.add(1);
            ptr1 = ptr1.add(1);
            i1 -= 1;
        } else if cmp > 0 {
            /* in2 first */
            (*ptr).set_haspos((*ptr2).haspos());
            (*ptr).set_len((*ptr2).len());
            memcpy(
                data.add(dataoff as usize) as *mut c_void,
                data2.add((*ptr2).pos() as usize) as *const c_void,
                (*ptr2).len() as usize,
            );
            (*ptr).set_pos(dataoff as u32);
            dataoff += (*ptr2).len() as c_int;
            if (*ptr).haspos() != 0 {
                let addlen: c_int = add_pos(in2, ptr2, out, ptr, maxpos);

                if addlen == 0 {
                    (*ptr).set_haspos(0);
                } else {
                    dataoff = SHORTALIGN(dataoff as usize) as c_int;
                    dataoff += addlen * core::mem::size_of::<WordEntryPos>() as c_int
                        + core::mem::size_of::<uint16>() as c_int;
                }
            }

            ptr = ptr.add(1);
            ptr2 = ptr2.add(1);
            i2 -= 1;
        } else {
            (*ptr).set_haspos((*ptr1).haspos() | (*ptr2).haspos());
            (*ptr).set_len((*ptr1).len());
            memcpy(
                data.add(dataoff as usize) as *mut c_void,
                data1.add((*ptr1).pos() as usize) as *const c_void,
                (*ptr1).len() as usize,
            );
            (*ptr).set_pos(dataoff as u32);
            dataoff += (*ptr1).len() as c_int;
            if (*ptr).haspos() != 0 {
                if (*ptr1).haspos() != 0 {
                    dataoff = SHORTALIGN(dataoff as usize) as c_int;
                    memcpy(
                        data.add(dataoff as usize) as *mut c_void,
                        _POSVECPTR(in1, ptr1) as *const c_void,
                        POSDATALEN(in1, ptr1) as usize * core::mem::size_of::<WordEntryPos>()
                            + core::mem::size_of::<uint16>(),
                    );
                    dataoff += POSDATALEN(in1, ptr1) * core::mem::size_of::<WordEntryPos>() as c_int
                        + core::mem::size_of::<uint16>() as c_int;
                    if (*ptr2).haspos() != 0 {
                        dataoff += add_pos(in2, ptr2, out, ptr, maxpos)
                            * core::mem::size_of::<WordEntryPos>() as c_int;
                    }
                } else {
                    /* must have ptr2->haspos */
                    let addlen: c_int = add_pos(in2, ptr2, out, ptr, maxpos);

                    if addlen == 0 {
                        (*ptr).set_haspos(0);
                    } else {
                        dataoff = SHORTALIGN(dataoff as usize) as c_int;
                        dataoff += addlen * core::mem::size_of::<WordEntryPos>() as c_int
                            + core::mem::size_of::<uint16>() as c_int;
                    }
                }
            }

            ptr = ptr.add(1);
            ptr1 = ptr1.add(1);
            ptr2 = ptr2.add(1);
            i1 -= 1;
            i2 -= 1;
        }
    }

    while i1 != 0 {
        (*ptr).set_haspos((*ptr1).haspos());
        (*ptr).set_len((*ptr1).len());
        memcpy(
            data.add(dataoff as usize) as *mut c_void,
            data1.add((*ptr1).pos() as usize) as *const c_void,
            (*ptr1).len() as usize,
        );
        (*ptr).set_pos(dataoff as u32);
        dataoff += (*ptr1).len() as c_int;
        if (*ptr).haspos() != 0 {
            dataoff = SHORTALIGN(dataoff as usize) as c_int;
            memcpy(
                data.add(dataoff as usize) as *mut c_void,
                _POSVECPTR(in1, ptr1) as *const c_void,
                POSDATALEN(in1, ptr1) as usize * core::mem::size_of::<WordEntryPos>()
                    + core::mem::size_of::<uint16>(),
            );
            dataoff += POSDATALEN(in1, ptr1) * core::mem::size_of::<WordEntryPos>() as c_int
                + core::mem::size_of::<uint16>() as c_int;
        }

        ptr = ptr.add(1);
        ptr1 = ptr1.add(1);
        i1 -= 1;
    }

    while i2 != 0 {
        (*ptr).set_haspos((*ptr2).haspos());
        (*ptr).set_len((*ptr2).len());
        memcpy(
            data.add(dataoff as usize) as *mut c_void,
            data2.add((*ptr2).pos() as usize) as *const c_void,
            (*ptr2).len() as usize,
        );
        (*ptr).set_pos(dataoff as u32);
        dataoff += (*ptr2).len() as c_int;
        if (*ptr).haspos() != 0 {
            let addlen: c_int = add_pos(in2, ptr2, out, ptr, maxpos);

            if addlen == 0 {
                (*ptr).set_haspos(0);
            } else {
                dataoff = SHORTALIGN(dataoff as usize) as c_int;
                dataoff += addlen * core::mem::size_of::<WordEntryPos>() as c_int
                    + core::mem::size_of::<uint16>() as c_int;
            }
        }

        ptr = ptr.add(1);
        ptr2 = ptr2.add(1);
        i2 -= 1;
    }

    /*
     * Instead of checking each offset individually, we check for overflow of
     * pos fields once at the end.
     */
    if dataoff > MAXSTRPOS {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!(
                "string is too long for tsvector ({} bytes, max {} bytes)",
                dataoff,
                MAXSTRPOS
            )
        );
    }

    /*
     * Adjust sizes (asserting that we didn't overrun the original estimates)
     * and collapse out any unused array entries.
     */
    output_size =
        ((ptr as isize - ARRPTR(out) as isize) / core::mem::size_of::<WordEntry>() as isize) as c_int;
    Assert!(output_size <= (*out).size);
    (*out).size = output_size;
    if data != STRPTR(out) {
        memmove(
            STRPTR(out) as *mut c_void,
            data as *const c_void,
            dataoff as usize,
        );
    }
    output_bytes = CALCDATASIZE((*out).size, dataoff) as c_int;
    Assert!(output_bytes <= VARSIZE(out as *const c_char) as c_int);
    SET_VARSIZE(out as *mut c_char, output_bytes);

    PG_FREE_IF_COPY!(fcinfo, in1, 0);
    PG_FREE_IF_COPY!(fcinfo, in2, 1);
    PG_RETURN_POINTER!(out)
}

// ================================================================
//   tsquery match (@@) engine
// ================================================================
//
// The TSQuery type (QueryItem / QueryOperand / GETQUERY / GETOPERAND / OP_*) is
// supplied by the sibling crate::utils::adt::tsquery_util (module-local copies
// pending tsquery.c).  ExecPhraseData / TSExecuteCallback / TSTernaryValue /
// the TS_EXEC_* flags are declared near the top of this file.

/* CHECK_FOR_INTERRUPTS(): no-op pending the signal/interrupt machinery. */
#[inline]
fn CHECK_FOR_INTERRUPTS() {}

/*
 * Check weight info or/and fill 'data' with the required positions.
 */
unsafe fn checkclass_str(
    chkval: *mut CHKVAL,
    entry: *mut WordEntry,
    val: *mut QueryOperand,
    data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let mut result: TSTernaryValue = TS_NO;

    Assert!(data.is_null() || (*data).npos == 0);

    if (*entry).haspos() != 0 {
        /*
         * We can't use the _POSVECPTR macro here because the pointer to the
         * tsvector's lexeme storage is already contained in chkval->values.
         */
        let posvec: *mut WordEntryPosVector = (*chkval)
            .values
            .add(SHORTALIGN(((*entry).pos() + (*entry).len()) as usize))
            as *mut WordEntryPosVector;
        let posvec_pos: *mut WordEntryPos = (*posvec).pos.as_mut_ptr();
        let posvec_npos: c_int = (*posvec).npos as c_int;

        if (*val).weight != 0 && !data.is_null() {
            let mut posvec_iter: *mut WordEntryPos = posvec_pos;
            let mut dptr: *mut WordEntryPos;

            /*
             * Filter position information by weights
             */
            (*data).pos =
                palloc(core::mem::size_of::<WordEntryPos>() * posvec_npos as usize) as *mut WordEntryPos;
            dptr = (*data).pos;
            (*data).allocated = true;

            /* Is there a position with a matching weight? */
            while posvec_iter < posvec_pos.add(posvec_npos as usize) {
                /* If true, append this position to the data->pos */
                if ((*val).weight & (1 << WEP_GETWEIGHT(*posvec_iter))) != 0 {
                    *dptr = WEP_GETPOS(*posvec_iter) as WordEntryPos;
                    dptr = dptr.add(1);
                }
                posvec_iter = posvec_iter.add(1);
            }

            (*data).npos = (dptr as isize - (*data).pos as isize) as c_int
                / core::mem::size_of::<WordEntryPos>() as c_int;

            if (*data).npos > 0 {
                result = TS_YES;
            } else {
                pfree((*data).pos as *mut c_void);
                (*data).pos = null_mut();
                (*data).allocated = false;
            }
        } else if (*val).weight != 0 {
            let mut posvec_iter: *mut WordEntryPos = posvec_pos;

            /* Is there a position with a matching weight? */
            while posvec_iter < posvec_pos.add(posvec_npos as usize) {
                if ((*val).weight & (1 << WEP_GETWEIGHT(*posvec_iter))) != 0 {
                    result = TS_YES;
                    break; /* no need to go further */
                }
                posvec_iter = posvec_iter.add(1);
            }
        } else if !data.is_null() {
            (*data).npos = posvec_npos;
            (*data).pos = posvec_pos;
            (*data).allocated = false;
            result = TS_YES;
        } else {
            /* simplest case: no weight check, positions not needed */
            result = TS_YES;
        }
    } else {
        /*
         * Position info is lacking, so if the caller requires it, we can only
         * say that maybe there is a match.
         */
        if !data.is_null() {
            result = TS_MAYBE;
        } else {
            result = TS_YES;
        }
    }

    result
}

/*
 * TS_execute callback for matching a tsquery operand to plain tsvector data.
 */
unsafe fn checkcondition_str(
    checkval: *mut c_void,
    val: *mut QueryOperand,
    data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let chkval: *mut CHKVAL = checkval as *mut CHKVAL;
    let mut StopLow: *mut WordEntry = (*chkval).arrb;
    let mut StopHigh: *mut WordEntry = (*chkval).arre;
    let mut StopMiddle: *mut WordEntry = StopHigh;
    let mut res: TSTernaryValue = TS_NO;

    /* Loop invariant: StopLow <= val < StopHigh */
    while StopLow < StopHigh {
        let difference: c_int;

        StopMiddle = StopLow.add(
            ((StopHigh as isize - StopLow as isize)
                / core::mem::size_of::<WordEntry>() as isize / 2) as usize,
        );
        difference = tsCompareString(
            (*chkval).operand.add((*val).distance() as usize),
            (*val).length() as c_int,
            (*chkval).values.add((*StopMiddle).pos() as usize),
            (*StopMiddle).len() as c_int,
            false,
        );

        if difference == 0 {
            /* Check weight info & fill 'data' with positions */
            res = checkclass_str(chkval, StopMiddle, val, data);
            break;
        } else if difference > 0 {
            StopLow = StopMiddle.add(1);
        } else {
            StopHigh = StopMiddle;
        }
    }

    /*
     * If it's a prefix search, we should also consider lexemes that the
     * search term is a prefix of.
     */
    if (*val).prefix && (res != TS_YES || !data.is_null()) {
        let mut allpos: *mut WordEntryPos = null_mut();
        let mut npos: c_int = 0;
        let mut totalpos: c_int = 0;

        /* adjust start position for corner case */
        if StopLow >= StopHigh {
            StopMiddle = StopHigh;
        }

        /* we don't try to re-use any data from the initial match */
        if !data.is_null() {
            if (*data).allocated {
                pfree((*data).pos as *mut c_void);
            }
            (*data).pos = null_mut();
            (*data).allocated = false;
            (*data).npos = 0;
        }
        res = TS_NO;

        while (res != TS_YES || !data.is_null())
            && StopMiddle < (*chkval).arre
            && tsCompareString(
                (*chkval).operand.add((*val).distance() as usize),
                (*val).length() as c_int,
                (*chkval).values.add((*StopMiddle).pos() as usize),
                (*StopMiddle).len() as c_int,
                true,
            ) == 0
        {
            let subres: TSTernaryValue = checkclass_str(chkval, StopMiddle, val, data);

            if subres != TS_NO {
                if !data.is_null() {
                    /*
                     * We need to join position information
                     */
                    if subres == TS_MAYBE {
                        res = TS_MAYBE;
                        npos = 0;
                        if !allpos.is_null() {
                            pfree(allpos as *mut c_void);
                        }
                        break;
                    }

                    while npos + (*data).npos > totalpos {
                        if totalpos == 0 {
                            totalpos = 256;
                            allpos = palloc(
                                core::mem::size_of::<WordEntryPos>() * totalpos as usize,
                            ) as *mut WordEntryPos;
                        } else {
                            totalpos *= 2;
                            allpos = repalloc(
                                allpos as *mut c_void,
                                core::mem::size_of::<WordEntryPos>() * totalpos as usize,
                            ) as *mut WordEntryPos;
                        }
                    }

                    memcpy(
                        allpos.add(npos as usize) as *mut c_void,
                        (*data).pos as *const c_void,
                        core::mem::size_of::<WordEntryPos>() * (*data).npos as usize,
                    );
                    npos += (*data).npos;

                    /* don't leak storage from individual matches */
                    if (*data).allocated {
                        pfree((*data).pos as *mut c_void);
                    }
                    (*data).pos = null_mut();
                    (*data).allocated = false;
                    /* it's important to reset data->npos before next loop */
                    (*data).npos = 0;
                } else {
                    /* Don't need positions, just handle YES/MAYBE */
                    if subres == TS_YES || res == TS_NO {
                        res = subres;
                    }
                }
            }

            StopMiddle = StopMiddle.add(1);
        }

        if !data.is_null() && npos > 0 {
            /* Sort and make unique array of found positions */
            (*data).pos = allpos;
            qsort_wep(allpos, npos);
            (*data).npos = qunique_wep(allpos, npos);
            (*data).allocated = true;
            res = TS_YES;
        }
    }

    res
}

/* qsort(pos, npos, sizeof(WordEntryPos), compareWordEntryPos) */
unsafe fn qsort_wep(pos: *mut WordEntryPos, npos: c_int) {
    let sl = core::slice::from_raw_parts_mut(pos, npos as usize);
    sl.sort_by(|x, y| {
        let r = compareWordEntryPos(
            x as *const WordEntryPos as *const c_void,
            y as *const WordEntryPos as *const c_void,
        );
        r.cmp(&0)
    });
}

/*
 * qunique(pos, npos, sizeof(WordEntryPos), compareWordEntryPos): remove adjacent
 * duplicates from a sorted array, returning the new length.  lib/qunique.h is not
 * yet ported, so the (tiny) algorithm is inlined here.
 */
unsafe fn qunique_wep(pos: *mut WordEntryPos, npos: c_int) -> c_int {
    if npos <= 1 {
        return npos;
    }
    let mut last: c_int = 0;
    let mut i: c_int = 1;
    while i < npos {
        if compareWordEntryPos(
            pos.add(i as usize) as *const c_void,
            pos.add(last as usize) as *const c_void,
        ) != 0
        {
            last += 1;
            if last != i {
                *pos.add(last as usize) = *pos.add(i as usize);
            }
        }
        i += 1;
    }
    last + 1
}

/*
 * Compute output position list for a tsquery operator in phrase mode.
 */
const TSPO_L_ONLY: c_int = 0x01; /* emit positions appearing only in L */
const TSPO_R_ONLY: c_int = 0x02; /* emit positions appearing only in R */
const TSPO_BOTH: c_int = 0x04; /* emit positions appearing in both L&R */

unsafe fn TS_phrase_output(
    data: *mut ExecPhraseData,
    Ldata: *mut ExecPhraseData,
    Rdata: *mut ExecPhraseData,
    emit: c_int,
    Loffset: c_int,
    Roffset: c_int,
    max_npos: c_int,
) -> TSTernaryValue {
    let mut Lindex: c_int;
    let mut Rindex: c_int;

    /* Loop until both inputs are exhausted */
    Lindex = 0;
    Rindex = 0;
    while Lindex < (*Ldata).npos || Rindex < (*Rdata).npos {
        let Lpos: c_int;
        let Rpos: c_int;
        let mut output_pos: c_int = 0;

        if Lindex < (*Ldata).npos {
            Lpos = WEP_GETPOS(*(*Ldata).pos.add(Lindex as usize)) + Loffset;
        } else {
            /* L array exhausted, so we're done if R_ONLY isn't set */
            if (emit & TSPO_R_ONLY) == 0 {
                break;
            }
            Lpos = INT_MAX;
        }
        if Rindex < (*Rdata).npos {
            Rpos = WEP_GETPOS(*(*Rdata).pos.add(Rindex as usize)) + Roffset;
        } else {
            /* R array exhausted, so we're done if L_ONLY isn't set */
            if (emit & TSPO_L_ONLY) == 0 {
                break;
            }
            Rpos = INT_MAX;
        }

        /* Merge-join the two input lists */
        if Lpos < Rpos {
            if (emit & TSPO_L_ONLY) != 0 {
                output_pos = Lpos;
            }
            Lindex += 1;
        } else if Lpos == Rpos {
            if (emit & TSPO_BOTH) != 0 {
                output_pos = Rpos;
            }
            Lindex += 1;
            Rindex += 1;
        } else {
            /* Lpos > Rpos */
            if (emit & TSPO_R_ONLY) != 0 {
                output_pos = Rpos;
            }
            Rindex += 1;
        }

        if output_pos > 0 {
            if !data.is_null() {
                /* Store position, first allocating output array if needed */
                if (*data).pos.is_null() {
                    (*data).pos = palloc(
                        max_npos as usize * core::mem::size_of::<WordEntryPos>(),
                    ) as *mut WordEntryPos;
                    (*data).allocated = true;
                }
                *(*data).pos.add((*data).npos as usize) = output_pos as WordEntryPos;
                (*data).npos += 1;
            } else {
                /* Exact positions not needed, return TS_YES at first hit. */
                return TS_YES;
            }
        }
    }

    if !data.is_null() && (*data).npos > 0 {
        Assert!((*data).npos <= max_npos);
        return TS_YES;
    }
    TS_NO
}

/*
 * Execute tsquery at or below an OP_PHRASE operator.
 */
unsafe fn TS_phrase_execute(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    flags: uint32,
    chkcond: TSExecuteCallback,
    data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let mut Ldata: ExecPhraseData;
    let mut Rdata: ExecPhraseData;
    let lmatch: TSTernaryValue;
    let rmatch: TSTernaryValue;
    let Loffset: c_int;
    let Roffset: c_int;
    let maxwidth: c_int;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /* ... and let's check for query cancel while we're at it */
    CHECK_FOR_INTERRUPTS();

    if (*curitem).type_() == QI_VAL {
        return chkcond(arg, curitem as *mut QueryOperand, data);
    }

    match (*curitem).qoperator.oper {
        OP_NOT => {
            if (flags & TS_EXEC_SKIP_NOT) != 0 {
                /* with SKIP_NOT, report NOT as "match everywhere" */
                Assert!((*data).npos == 0 && !(*data).negate);
                (*data).negate = true;
                return TS_YES;
            }
            match TS_phrase_execute(curitem.add(1), arg, flags, chkcond, data) {
                TS_NO => {
                    /* change "match nowhere" to "match everywhere" */
                    Assert!((*data).npos == 0 && !(*data).negate);
                    (*data).negate = true;
                    return TS_YES;
                }
                TS_YES => {
                    if (*data).npos > 0 {
                        /* we have some positions, invert negate flag */
                        (*data).negate = !(*data).negate;
                        return TS_YES;
                    } else if (*data).negate {
                        /* change "match everywhere" to "match nowhere" */
                        (*data).negate = false;
                        return TS_NO;
                    }
                    /* Should not get here if result was TS_YES */
                    Assert!(false);
                }
                TS_MAYBE => {
                    /* match positions are, and remain, uncertain */
                    return TS_MAYBE;
                }
            }
        }

        OP_PHRASE | OP_AND => {
            Ldata = ExecPhraseData::zeroed();
            Rdata = ExecPhraseData::zeroed();

            lmatch = TS_phrase_execute(
                curitem.add((*curitem).qoperator.left as usize),
                arg,
                flags,
                chkcond,
                &mut Ldata,
            );
            if lmatch == TS_NO {
                return TS_NO;
            }

            rmatch = TS_phrase_execute(curitem.add(1), arg, flags, chkcond, &mut Rdata);
            if rmatch == TS_NO {
                return TS_NO;
            }

            if lmatch == TS_MAYBE || rmatch == TS_MAYBE {
                return TS_MAYBE;
            }

            if (*curitem).qoperator.oper == OP_PHRASE {
                Loffset = (*curitem).qoperator.distance as c_int + Rdata.width;
                Roffset = 0;
                if !data.is_null() {
                    (*data).width =
                        (*curitem).qoperator.distance as c_int + Ldata.width + Rdata.width;
                }
            } else {
                maxwidth = core::cmp::max(Ldata.width, Rdata.width);
                Loffset = maxwidth - Ldata.width;
                Roffset = maxwidth - Rdata.width;
                if !data.is_null() {
                    (*data).width = maxwidth;
                }
            }

            if Ldata.negate && Rdata.negate {
                /* !L & !R: treat as !(L | R) */
                let _ = TS_phrase_output(
                    data,
                    &mut Ldata,
                    &mut Rdata,
                    TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                    Loffset,
                    Roffset,
                    Ldata.npos + Rdata.npos,
                );
                if !data.is_null() {
                    (*data).negate = true;
                }
                return TS_YES;
            } else if Ldata.negate {
                /* !L & R */
                return TS_phrase_output(
                    data, &mut Ldata, &mut Rdata, TSPO_R_ONLY, Loffset, Roffset, Rdata.npos,
                );
            } else if Rdata.negate {
                /* L & !R */
                return TS_phrase_output(
                    data, &mut Ldata, &mut Rdata, TSPO_L_ONLY, Loffset, Roffset, Ldata.npos,
                );
            } else {
                /* straight AND */
                return TS_phrase_output(
                    data,
                    &mut Ldata,
                    &mut Rdata,
                    TSPO_BOTH,
                    Loffset,
                    Roffset,
                    core::cmp::min(Ldata.npos, Rdata.npos),
                );
            }
        }

        OP_OR => {
            Ldata = ExecPhraseData::zeroed();
            Rdata = ExecPhraseData::zeroed();

            lmatch = TS_phrase_execute(
                curitem.add((*curitem).qoperator.left as usize),
                arg,
                flags,
                chkcond,
                &mut Ldata,
            );
            rmatch = TS_phrase_execute(curitem.add(1), arg, flags, chkcond, &mut Rdata);

            if lmatch == TS_NO && rmatch == TS_NO {
                return TS_NO;
            }

            if lmatch == TS_MAYBE || rmatch == TS_MAYBE {
                return TS_MAYBE;
            }

            /* Cope with undefined output width from failed submatch. */
            if lmatch == TS_NO {
                Ldata.width = 0;
            }
            if rmatch == TS_NO {
                Rdata.width = 0;
            }

            maxwidth = core::cmp::max(Ldata.width, Rdata.width);
            Loffset = maxwidth - Ldata.width;
            Roffset = maxwidth - Rdata.width;
            (*data).width = maxwidth;

            if Ldata.negate && Rdata.negate {
                /* !L | !R: treat as !(L & R) */
                let _ = TS_phrase_output(
                    data,
                    &mut Ldata,
                    &mut Rdata,
                    TSPO_BOTH,
                    Loffset,
                    Roffset,
                    core::cmp::min(Ldata.npos, Rdata.npos),
                );
                (*data).negate = true;
                return TS_YES;
            } else if Ldata.negate {
                /* !L | R: treat as !(L & !R) */
                let _ = TS_phrase_output(
                    data, &mut Ldata, &mut Rdata, TSPO_L_ONLY, Loffset, Roffset, Ldata.npos,
                );
                (*data).negate = true;
                return TS_YES;
            } else if Rdata.negate {
                /* L | !R: treat as !(!L & R) */
                let _ = TS_phrase_output(
                    data, &mut Ldata, &mut Rdata, TSPO_R_ONLY, Loffset, Roffset, Rdata.npos,
                );
                (*data).negate = true;
                return TS_YES;
            } else {
                /* straight OR */
                return TS_phrase_output(
                    data,
                    &mut Ldata,
                    &mut Rdata,
                    TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                    Loffset,
                    Roffset,
                    Ldata.npos + Rdata.npos,
                );
            }
        }

        other => {
            elog!(ERROR, "unrecognized operator: {}", other as c_int);
            unreachable!();
        }
    }

    /* not reachable, but keep compiler quiet */
    TS_NO
}

/*
 * Evaluate tsquery boolean expression.
 */
pub unsafe fn TS_execute(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    flags: uint32,
    chkcond: TSExecuteCallback,
) -> bool {
    /*
     * If we get TS_MAYBE from the recursion, return true.  We could only see
     * that result if the caller passed TS_EXEC_PHRASE_NO_POS.
     */
    TS_execute_recurse(curitem, arg, flags, chkcond) != TS_NO
}

/*
 * Evaluate tsquery boolean expression (TS_MAYBE returned as-is).
 */
pub unsafe fn TS_execute_ternary(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    flags: uint32,
    chkcond: TSExecuteCallback,
) -> TSTernaryValue {
    TS_execute_recurse(curitem, arg, flags, chkcond)
}

/*
 * TS_execute recursion for operators above any phrase operator.
 */
unsafe fn TS_execute_recurse(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    flags: uint32,
    chkcond: TSExecuteCallback,
) -> TSTernaryValue {
    let lmatch: TSTernaryValue;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /* ... and let's check for query cancel while we're at it */
    CHECK_FOR_INTERRUPTS();

    if (*curitem).type_() == QI_VAL {
        return chkcond(arg, curitem as *mut QueryOperand, null_mut() /* no pos info */);
    }

    match (*curitem).qoperator.oper {
        OP_NOT => {
            if (flags & TS_EXEC_SKIP_NOT) != 0 {
                return TS_YES;
            }
            match TS_execute_recurse(curitem.add(1), arg, flags, chkcond) {
                TS_NO => return TS_YES,
                TS_YES => return TS_NO,
                TS_MAYBE => return TS_MAYBE,
            }
        }

        OP_AND => {
            lmatch =
                TS_execute_recurse(curitem.add((*curitem).qoperator.left as usize), arg, flags, chkcond);
            if lmatch == TS_NO {
                return TS_NO;
            }
            match TS_execute_recurse(curitem.add(1), arg, flags, chkcond) {
                TS_NO => return TS_NO,
                TS_YES => return lmatch,
                TS_MAYBE => return TS_MAYBE,
            }
        }

        OP_OR => {
            lmatch =
                TS_execute_recurse(curitem.add((*curitem).qoperator.left as usize), arg, flags, chkcond);
            if lmatch == TS_YES {
                return TS_YES;
            }
            match TS_execute_recurse(curitem.add(1), arg, flags, chkcond) {
                TS_NO => return lmatch,
                TS_YES => return TS_YES,
                TS_MAYBE => return TS_MAYBE,
            }
        }

        OP_PHRASE => {
            match TS_phrase_execute(curitem, arg, flags, chkcond, null_mut()) {
                TS_NO => return TS_NO,
                TS_YES => return TS_YES,
                TS_MAYBE => {
                    return if (flags & TS_EXEC_PHRASE_NO_POS) != 0 {
                        TS_MAYBE
                    } else {
                        TS_NO
                    }
                }
            }
        }

        other => {
            elog!(ERROR, "unrecognized operator: {}", other as c_int);
            unreachable!();
        }
    }
}

/*
 * Evaluate tsquery and report locations of matching terms.
 *
 * On successful match, the result is a List of ExecPhraseData structs.
 */
pub unsafe fn TS_execute_locations(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    flags: uint32,
    chkcond: TSExecuteCallback,
) -> *mut crate::nodes::pg_list::List {
    let mut result: *mut crate::nodes::pg_list::List = NIL;

    /* No flags supported, as yet */
    Assert!(flags == TS_EXEC_EMPTY);
    if TS_execute_locations_recurse(curitem, arg, chkcond, &mut result) {
        return result;
    }
    NIL
}

/*
 * TS_execute_locations recursion for operators above any phrase operator.
 */
unsafe fn TS_execute_locations_recurse(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    chkcond: TSExecuteCallback,
    locations: *mut *mut crate::nodes::pg_list::List,
) -> bool {
    let lmatch: bool;
    let rmatch: bool;
    let mut llocations: *mut crate::nodes::pg_list::List = NIL;
    let mut rlocations: *mut crate::nodes::pg_list::List = NIL;
    let data: *mut ExecPhraseData;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /* ... and let's check for query cancel while we're at it */
    CHECK_FOR_INTERRUPTS();

    /* Default locations result is empty */
    *locations = NIL;

    if (*curitem).type_() == QI_VAL {
        let d = palloc0(core::mem::size_of::<ExecPhraseData>()) as *mut ExecPhraseData;
        if chkcond(arg, curitem as *mut QueryOperand, d) == TS_YES {
            *locations = list_make1!(d);
            return true;
        }
        pfree(d as *mut c_void);
        return false;
    }

    match (*curitem).qoperator.oper {
        OP_NOT => {
            if !TS_execute_locations_recurse(curitem.add(1), arg, chkcond, &mut llocations) {
                return true; /* we don't pass back any locations */
            }
            false
        }

        OP_AND => {
            if !TS_execute_locations_recurse(
                curitem.add((*curitem).qoperator.left as usize),
                arg,
                chkcond,
                &mut llocations,
            ) {
                return false;
            }
            if !TS_execute_locations_recurse(curitem.add(1), arg, chkcond, &mut rlocations) {
                return false;
            }
            *locations = list_concat(llocations, rlocations);
            true
        }

        OP_OR => {
            lmatch = TS_execute_locations_recurse(
                curitem.add((*curitem).qoperator.left as usize),
                arg,
                chkcond,
                &mut llocations,
            );
            rmatch = TS_execute_locations_recurse(curitem.add(1), arg, chkcond, &mut rlocations);
            if lmatch || rmatch {
                /*
                 * Generate an AND'able location struct from each combination of
                 * sub-matches (disjunctive law).
                 */
                if llocations == NIL {
                    *locations = rlocations;
                } else if rlocations == NIL {
                    *locations = llocations;
                } else {
                    foreach!(ll, llocations, {
                        let ldata = lfirst(current_cell!(ll)) as *mut ExecPhraseData;
                        foreach!(lr, rlocations, {
                            let rdata = lfirst(current_cell!(lr)) as *mut ExecPhraseData;
                            let d =
                                palloc0(core::mem::size_of::<ExecPhraseData>()) as *mut ExecPhraseData;
                            let _ = TS_phrase_output(
                                d,
                                ldata,
                                rdata,
                                TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                                0,
                                0,
                                (*ldata).npos + (*rdata).npos,
                            );
                            /* Report the larger width, as explained above. */
                            (*d).width = core::cmp::max((*ldata).width, (*rdata).width);
                            *locations = lappend(*locations, d as *mut c_void);
                        });
                    });
                }
                return true;
            }
            false
        }

        OP_PHRASE => {
            /* We can hand this off to TS_phrase_execute */
            data = palloc0(core::mem::size_of::<ExecPhraseData>()) as *mut ExecPhraseData;
            if TS_phrase_execute(curitem, arg, TS_EXEC_EMPTY, chkcond, data) == TS_YES {
                if !(*data).negate {
                    *locations = list_make1!(data);
                }
                return true;
            }
            pfree(data as *mut c_void);
            false
        }

        other => {
            elog!(ERROR, "unrecognized operator: {}", other as c_int);
            unreachable!();
        }
    }
}

/*
 * Detect whether a tsquery boolean expression requires any positive matches.
 */
pub unsafe fn tsquery_requires_match(curitem: *mut QueryItem) -> bool {
    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    if (*curitem).type_() == QI_VAL {
        return true;
    }

    match (*curitem).qoperator.oper {
        OP_NOT => {
            /* Assume there are no required matches underneath a NOT. */
            false
        }

        /* Treat OP_PHRASE as OP_AND here */
        OP_PHRASE | OP_AND => {
            /* If either side requires a match, we're good */
            if tsquery_requires_match(curitem.add((*curitem).qoperator.left as usize)) {
                true
            } else {
                tsquery_requires_match(curitem.add(1))
            }
        }

        OP_OR => {
            /* Both sides must require a match */
            if tsquery_requires_match(curitem.add((*curitem).qoperator.left as usize)) {
                tsquery_requires_match(curitem.add(1))
            } else {
                false
            }
        }

        other => {
            elog!(ERROR, "unrecognized operator: {}", other as c_int);
            unreachable!();
        }
    }
}

// ----------------------------------------------------------------
//   PG_GETARG_TSQUERY / DatumGetTSQuery / TSQueryGetDatum
//   (ts_type.h macros; the C PG_GETARG_TSQUERY detoasts, which is the identity
//   for in-line datums with TOAST unported -- mirrors PG_GETARG_TSVECTOR).
// ----------------------------------------------------------------
#[inline]
unsafe fn DatumGetTSQuery(x: Datum) -> TSQuery {
    crate::varatt::pg_detoast_datum_packed(DatumGetPointer(x) as *mut c_void) as TSQuery
}
#[inline]
unsafe fn PG_GETARG_TSQUERY(fcinfo: FunctionCallInfo, n: usize) -> TSQuery {
    DatumGetTSQuery(PG_GETARG_DATUM!(fcinfo, n))
}

/*
 * boolean operations
 */
pub unsafe fn ts_match_qv(fcinfo: FunctionCallInfo) -> Datum {
    /* PG_RETURN_DATUM(DirectFunctionCall2(ts_match_vq, ARG1, ARG0)); */
    PG_RETURN_DATUM!(DirectFunctionCall2!(
        ts_match_vq,
        PG_GETARG_DATUM!(fcinfo, 1),
        PG_GETARG_DATUM!(fcinfo, 0)
    ))
}

pub unsafe fn ts_match_vq(fcinfo: FunctionCallInfo) -> Datum {
    let val: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 1);
    let mut chkval: CHKVAL = CHKVAL {
        arrb: null_mut(),
        arre: null_mut(),
        values: null_mut(),
        operand: null_mut(),
    };
    let result: bool;

    /* empty query matches nothing */
    if (*query).size == 0 {
        PG_FREE_IF_COPY!(fcinfo, val, 0);
        PG_FREE_IF_COPY!(fcinfo, query, 1);
        PG_RETURN_BOOL!(false);
    }

    chkval.arrb = ARRPTR(val);
    chkval.arre = chkval.arrb.add((*val).size as usize);
    chkval.values = STRPTR(val);
    chkval.operand = GETOPERAND(query);
    result = TS_execute(
        GETQUERY(query),
        &mut chkval as *mut CHKVAL as *mut c_void,
        TS_EXEC_EMPTY,
        checkcondition_str,
    );

    PG_FREE_IF_COPY!(fcinfo, val, 0);
    PG_FREE_IF_COPY!(fcinfo, query, 1);
    PG_RETURN_BOOL!(result)
}

pub unsafe fn ts_match_tt(fcinfo: FunctionCallInfo) -> Datum {
    // C: to_tsvector(ARG0) @@ plainto_tsquery(ARG1), then ts_match_vq.
    // TODO(pg-port): to_tsvector / plainto_tsquery (tsvector parser, ts_parse.c)
    // are not yet ported, so this entry point remains stubbed.
    let _ = fcinfo;
    unimplemented!("ts_match_tt: to_tsvector / plainto_tsquery not yet translated")
}

pub unsafe fn ts_match_tq(fcinfo: FunctionCallInfo) -> Datum {
    // C: to_tsvector(ARG0) @@ ARG1::tsquery, then ts_match_vq.
    // TODO(pg-port): to_tsvector (ts_parse.c) is not yet ported, so this entry
    // point remains stubbed.
    let _ = fcinfo;
    unimplemented!("ts_match_tq: to_tsvector not yet translated")
}

// ================================================================
//   ts_stat statistic function support -- STUBBED (needs SPI / SRF)
// ================================================================
//
// TODO(pg-port): ts_accum / insertStatEntry / chooseNextStatEntry /
// walkStatEntryTree / ts_stat_sql build a balanced-tree statistic over rows
// fetched via SPI (executor/spi.h) and emit them through funcapi SRF. Neither
// SPI nor the SRF machinery is ported, so ts_stat1/ts_stat2 are stubs.

pub unsafe fn ts_stat1(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("ts_stat1: executor/spi.h (SPI) + funcapi SRF not yet translated")
}

pub unsafe fn ts_stat2(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    unimplemented!("ts_stat2: executor/spi.h (SPI) + funcapi SRF not yet translated")
}

// ================================================================
//   tsvector update trigger -- STUBBED (needs trigger manager / SPI / catalog)
// ================================================================
//
// TODO(pg-port): tsvector_update_trigger* require commands/trigger.h
// (TriggerData / CALLED_AS_TRIGGER / TRIGGER_FIRED_*), the SPI tuple helpers,
// catalog/namespace lookups (get_ts_config_oid), the text-search parser
// (parsetext / make_tsvector from tsvector.c's sibling units), and
// heap_modify_tuple_by_cols. None are ported yet.

pub unsafe fn tsvector_update_trigger_byid(fcinfo: FunctionCallInfo) -> Datum {
    // C: return tsvector_update_trigger(fcinfo, false);
    let _ = fcinfo;
    unimplemented!("tsvector_update_trigger_byid: trigger manager / SPI / catalog not yet translated")
}

pub unsafe fn tsvector_update_trigger_bycolumn(fcinfo: FunctionCallInfo) -> Datum {
    // C: return tsvector_update_trigger(fcinfo, true);
    let _ = fcinfo;
    unimplemented!("tsvector_update_trigger_bycolumn: trigger manager / SPI / catalog not yet translated")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::adt::tsvector::{TSVectorData, MAXSTRLEN};

    /*
     * tsvectorin's parser (tsvector_parser.c) is stubbed with unimplemented!(),
     * so we cannot build TSVectors through the I/O path at test time.  Instead we
     * assemble plain (no-position) TSVectors by hand here, matching the on-disk
     * layout that array_to_tsvector / tsvector_strip would produce: a sorted,
     * de-duplicated set of lexemes with haspos = 0.
     */
    unsafe fn make_plain_tsvector(lexemes: &[&[u8]]) -> TSVector {
        let n = lexemes.len() as c_int;
        let mut datalen: c_int = 0;
        for lex in lexemes {
            assert!((lex.len() as c_int) < MAXSTRLEN);
            datalen += lex.len() as c_int;
        }
        let total = CALCDATASIZE(n, datalen) as c_int;
        let v = palloc0(total as Size) as TSVector;
        SET_VARSIZE(v as *mut c_char, total);
        (*v).size = n;

        let arr = ARRPTR(v);
        let strbase = STRPTR(v);
        let mut off: c_int = 0;
        for (i, lex) in lexemes.iter().enumerate() {
            (*arr.add(i)).set_haspos(0);
            (*arr.add(i)).set_len(lex.len() as u32);
            (*arr.add(i)).set_pos(off as u32);
            memcpy(
                strbase.add(off as usize) as *mut c_void,
                lex.as_ptr() as *const c_void,
                lex.len(),
            );
            off += lex.len() as c_int;
        }
        v
    }

    /* Read back the lexemes of a (plain) tsvector for assertions. */
    unsafe fn lexemes_of(v: TSVector) -> Vec<Vec<u8>> {
        let arr = ARRPTR(v);
        let strbase = STRPTR(v);
        let mut out = Vec::new();
        for i in 0..(*v).size as usize {
            let e = arr.add(i);
            let p = strbase.add((*e).pos() as usize) as *const u8;
            let len = (*e).len() as usize;
            out.push(core::slice::from_raw_parts(p, len).to_vec());
        }
        out
    }

    /* Build a 1-arg fcinfo carrying a single TSVector datum. */
    macro_rules! call1 {
        ($func:expr, $arg:expr) => {{
            crate::LOCAL_FCINFO!(fcinfo, 1);
            crate::InitFunctionCallInfoData!(fcinfo, null_mut(), 1, 0, null_mut(), null_mut());
            (*(*fcinfo).args.as_mut_ptr().add(0)).value = TSVectorGetDatum($arg);
            (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
            $func(fcinfo)
        }};
    }

    /* Build a 2-arg fcinfo carrying two TSVector datums. */
    macro_rules! call2 {
        ($func:expr, $a:expr, $b:expr) => {{
            crate::LOCAL_FCINFO!(fcinfo, 2);
            crate::InitFunctionCallInfoData!(fcinfo, null_mut(), 2, 0, null_mut(), null_mut());
            (*(*fcinfo).args.as_mut_ptr().add(0)).value = TSVectorGetDatum($a);
            (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
            (*(*fcinfo).args.as_mut_ptr().add(1)).value = TSVectorGetDatum($b);
            (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
            $func(fcinfo)
        }};
    }

    #[test]
    fn cmp_eq_ne() {
        unsafe {
            let a = make_plain_tsvector(&[b"a", b"b", b"c"]);
            let a2 = make_plain_tsvector(&[b"a", b"b", b"c"]);
            let b = make_plain_tsvector(&[b"a", b"b", b"d"]);

            // a == a
            assert!(DatumGetBool(call2!(tsvector_eq, a, a2)));
            // a != b
            assert!(!DatumGetBool(call2!(tsvector_eq, a, b)));
            assert!(DatumGetBool(call2!(tsvector_ne, a, b)));

            // cmp(a,a) == 0
            assert_eq!(DatumGetInt32(call2!(tsvector_cmp, a, a2)), 0);
            // a and b have equal size/varsize; differ in last lexeme 'c' vs 'd'.
            // silly_cmp returns tsCompareString('c','d') < 0, so cmp(a,b) < 0.
            assert!(DatumGetInt32(call2!(tsvector_cmp, a, b)) < 0);
            assert!(DatumGetInt32(call2!(tsvector_cmp, b, a)) > 0);
            assert!(DatumGetBool(call2!(tsvector_lt, a, b)));
            assert!(DatumGetBool(call2!(tsvector_le, a, a2)));
            assert!(DatumGetBool(call2!(tsvector_gt, b, a)));
            assert!(DatumGetBool(call2!(tsvector_ge, a, a2)));
        }
    }

    #[test]
    fn length_is_lexeme_count() {
        unsafe {
            let a = make_plain_tsvector(&[b"a", b"b", b"c"]);
            assert_eq!(DatumGetInt32(call1!(tsvector_length, a)), 3);
        }
    }

    #[test]
    fn strip_keeps_lexemes_drops_positions() {
        unsafe {
            let a = make_plain_tsvector(&[b"a", b"b", b"c"]);
            let stripped = DatumGetPointer(call1!(tsvector_strip, a)) as TSVector;
            assert_eq!((*stripped).size, 3);
            let lex = lexemes_of(stripped);
            assert_eq!(lex, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
            // all entries have haspos cleared
            let arr = ARRPTR(stripped);
            for i in 0..3 {
                assert_eq!((*arr.add(i)).haspos(), 0);
            }
        }
    }

    #[test]
    fn concat_merges_sorted_lexemes() {
        unsafe {
            // "a b" || "c d" => "a b c d"
            let l = make_plain_tsvector(&[b"a", b"b"]);
            let r = make_plain_tsvector(&[b"c", b"d"]);
            let out = DatumGetPointer(call2!(tsvector_concat, l, r)) as TSVector;
            assert_eq!((*out).size, 4);
            assert_eq!(
                lexemes_of(out),
                vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
            );

            // overlapping lexeme should collapse: "a b" || "b c" => "a b c"
            let l2 = make_plain_tsvector(&[b"a", b"b"]);
            let r2 = make_plain_tsvector(&[b"b", b"c"]);
            let out2 = DatumGetPointer(call2!(tsvector_concat, l2, r2)) as TSVector;
            assert_eq!((*out2).size, 3);
            assert_eq!(
                lexemes_of(out2),
                vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
            );
        }
    }

    // Silence "field never read" on the layout-only structs.
    #[allow(dead_code)]
    fn _touch_layout() {
        let _ = core::mem::size_of::<CHKVAL>();
        let _ = core::mem::size_of::<TSVectorStat>();
        let _ = STATENTRYHDRSZ();
        let _ = core::mem::size_of::<TSVectorData>();
    }
}

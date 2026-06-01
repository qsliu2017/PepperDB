//! tsrank.rs - rank tsvector by tsquery
//!
//! 1:1 Rust port of src/backend/utils/adt/tsrank.c (PostgreSQL 18.3).
//!
//! Header mapping (mirrors tsrank.c #includes):
//!   "postgres.h"              -> crate::prelude::* / crate::c (int32, float4, MemSet)
//!   <limits.h>                -> INT_MAX const below
//!   <math.h>                  -> exp/log/sqrt bound via extern "C" libm (matches float.rs)
//!   "miscadmin.h"             -> check_stack_depth (crate::utils::misc::stack_depth)
//!   "tsearch/ts_utils.h"      -> TSVector / TSQuery / QueryItem / QueryOperand / WordEntry /
//!                                WordEntryPos(Vector) / the @@ engine TS_execute + callback
//!                                ExecPhraseData / TSTernaryValue / TS_EXEC_* (tsvector,
//!                                tsquery_util, tsvector_op)
//!   "utils/array.h"           -> ArrayType + ARR_* macros (crate::utils::array); ArrayGetNItems
//!                                (crate::utils::adt::arrayutils)
//!   "utils/fmgrprotos.h"      -> fmgr V1 dispatch macros (crate root)
//!
//! Notes on faithfulness:
//!   - array_contains_nulls() (arrayfuncs.c) is NOT yet ported; getWeights() inlines the
//!     equivalent null-bitmap scan (ARR_HASNULL + ARR_NULLBITMAP over ArrayGetNItems items).
//!   - qsort_arg(compareQueryOperand) -> crate::port::qsort::qsort_arg; qsort(compareDocR) ->
//!     crate::port::qsort::pg_qsort.
//!   - WordEntryPosVector has a flexible array member, so the on-stack POSNULL dummy uses
//!     WordEntryPosVector1 (1-element variant) cast to *mut WordEntryPosVector, matching the C.

use crate::prelude::*;
use crate::c::{float4, int16, int32, uint32, MemSet};
use crate::utils::fmgr::FunctionCallInfo;
use crate::{
    PG_GETARG_DATUM, PG_GETARG_INT32, PG_RETURN_FLOAT4, PG_FREE_IF_COPY,
    PG_DETOAST_DATUM,
};

use crate::utils::array::{ArrayType, ARR_DATA_PTR, ARR_DIMS, ARR_HASNULL, ARR_NDIM, ARR_NULLBITMAP};
use crate::utils::adt::arrayutils::ArrayGetNItems;
use crate::utils::misc::stack_depth::check_stack_depth;

use crate::utils::adt::tsvector::{
    TSVector, WordEntry, WordEntryPos, WordEntryPosVector, WordEntryPosVector1, ARRPTR,
    MAXENTRYPOS, POSDATALEN, POSDATAPTR, STRPTR, WEP_GETPOS, WEP_GETWEIGHT, WEP_SETPOS, _POSVECPTR,
    tsCompareString,
};
use crate::utils::adt::tsquery_util::{
    QueryItem, QueryOperand, TSQuery, GETOPERAND, GETQUERY, OP_AND, OP_PHRASE, QI_OPR, QI_VAL,
};
use crate::utils::adt::tsvector_op::{
    ExecPhraseData, TSTernaryValue, TS_NO, TS_YES, TS_EXEC_EMPTY, TS_execute,
};

use crate::port::qsort::{pg_qsort, qsort_arg};

use core::ffi::{c_int, c_void};
use core::ptr::null_mut;

/* <limits.h> INT_MAX */
const INT_MAX: c_int = c_int::MAX;

/*
 * <math.h> bindings (double precision), matching float.rs.  exp() also accepts a
 * float4 promoted to f64 in word_distance(), as in the C.
 */
extern "C" {
    fn exp(x: f64) -> f64;
    fn log(x: f64) -> f64;
    fn sqrt(x: f64) -> f64;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_ARRAY_SUBSCRIPT_ERROR: c_int = 0;
const ERRCODE_NULL_VALUE_NOT_ALLOWED: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

const NUM_WEIGHTS: usize = 4;
static default_weights: [float4; NUM_WEIGHTS] = [0.1, 0.2, 0.4, 1.0];

/* #define wpos(wep) ( w[ WEP_GETWEIGHT(wep) ] ) */
#[inline]
unsafe fn wpos(w: *const float4, wep: WordEntryPos) -> float4 {
    *w.add(WEP_GETWEIGHT(wep) as usize)
}

const RANK_NO_NORM: int32 = 0x00;
const RANK_NORM_LOGLENGTH: int32 = 0x01;
const RANK_NORM_LENGTH: int32 = 0x02;
#[allow(dead_code)]
const RANK_NORM_EXTDIST: int32 = 0x04;
const RANK_NORM_UNIQ: int32 = 0x08;
const RANK_NORM_LOGUNIQ: int32 = 0x10;
const RANK_NORM_RDIVRPLUS1: int32 = 0x20;
const DEF_NORM_METHOD: int32 = RANK_NO_NORM;

/*
 * Returns a weight of a word collocation
 */
fn word_distance(w: int32) -> float4 {
    if w > 100 {
        return 1e-30;
    }

    (1.0 / (1.005 + 0.05 * unsafe { exp(((w as float4) / 1.5 - 2.0) as f64) })) as float4
}

unsafe fn cnt_length(t: TSVector) -> c_int {
    let mut ptr: *mut WordEntry = ARRPTR(t);
    let end = STRPTR(t) as *mut WordEntry;
    let mut len: c_int = 0;

    while ptr < end {
        let clen = POSDATALEN(t, ptr);

        if clen == 0 {
            len += 1;
        } else {
            len += clen;
        }

        ptr = ptr.add(1);
    }

    len
}

/*
 * #define WordECompareQueryItem(e,q,p,i,m) \
 *     tsCompareString((q) + (i)->distance, (i)->length, \
 *                     (e) + (p)->pos, (p)->len, (m))
 */
#[inline]
unsafe fn WordECompareQueryItem(
    e: *mut c_char,
    q: *mut c_char,
    p: *const WordEntry,
    i: *const QueryOperand,
    m: bool,
) -> c_int {
    tsCompareString(
        q.add((*i).distance() as usize),
        (*i).length() as c_int,
        e.add((*p).pos() as usize),
        (*p).len() as c_int,
        m,
    )
}

/*
 * Returns a pointer to a WordEntry's array corresponding to 'item' from
 * tsvector 't'.  'q' is the TSQuery containing 'item'.  Returns NULL if not found.
 */
unsafe fn find_wordentry(
    t: TSVector,
    q: TSQuery,
    item: *mut QueryOperand,
    nitem: *mut int32,
) -> *mut WordEntry {
    let mut StopLow: *mut WordEntry = ARRPTR(t);
    let mut StopHigh: *mut WordEntry = STRPTR(t) as *mut WordEntry;
    let mut StopMiddle: *mut WordEntry = StopHigh;
    let mut difference: c_int;

    *nitem = 0;

    /* Loop invariant: StopLow <= item < StopHigh */
    while StopLow < StopHigh {
        StopMiddle = StopLow.add((StopHigh.offset_from(StopLow) as usize) / 2);
        difference = WordECompareQueryItem(STRPTR(t), GETOPERAND(q), StopMiddle, item, false);
        if difference == 0 {
            StopHigh = StopMiddle;
            *nitem = 1;
            break;
        } else if difference > 0 {
            StopLow = StopMiddle.add(1);
        } else {
            StopHigh = StopMiddle;
        }
    }

    if (*item).prefix {
        if StopLow >= StopHigh {
            StopMiddle = StopHigh;
        }

        *nitem = 0;

        while StopMiddle < (STRPTR(t) as *mut WordEntry)
            && WordECompareQueryItem(STRPTR(t), GETOPERAND(q), StopMiddle, item, true) == 0
        {
            *nitem += 1;
            StopMiddle = StopMiddle.add(1);
        }
    }

    if *nitem > 0 {
        StopHigh
    } else {
        null_mut()
    }
}

/*
 * sort QueryOperands by (length, word)
 */
unsafe fn compareQueryOperand(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int {
    let operand = arg as *mut c_char;
    let qa: *const QueryOperand = *(a as *const *const QueryOperand);
    let qb: *const QueryOperand = *(b as *const *const QueryOperand);

    tsCompareString(
        operand.add((*qa).distance() as usize),
        (*qa).length() as c_int,
        operand.add((*qb).distance() as usize),
        (*qb).length() as c_int,
        false,
    )
}

/*
 * Returns a sorted, de-duplicated array of QueryOperands in a query.
 * The returned QueryOperands are pointers to the original QueryOperands
 * in the query.  Length of the returned array is stored in *size.
 */
unsafe fn SortAndUniqItems(q: TSQuery, size: *mut c_int) -> *mut *mut QueryOperand {
    let operand = GETOPERAND(q);
    let mut item: *mut QueryItem = GETQUERY(q);
    let res: *mut *mut QueryOperand =
        palloc(core::mem::size_of::<*mut QueryOperand>() * (*size as usize)) as *mut *mut QueryOperand;
    let mut ptr: *mut *mut QueryOperand = res;

    /* Collect all operands from the tree to res */
    while {
        let old = *size;
        *size -= 1;
        old
    } != 0
    {
        if (*item).type_() == QI_VAL {
            *ptr = item as *mut QueryOperand;
            ptr = ptr.add(1);
        }
        item = item.add(1);
    }

    *size = ptr.offset_from(res) as c_int;
    if *size < 2 {
        return res;
    }

    qsort_arg(
        res as *mut c_void,
        *size as usize,
        core::mem::size_of::<*mut QueryOperand>(),
        compareQueryOperand,
        operand as *mut c_void,
    );

    ptr = res.add(1);
    let mut prevptr: *mut *mut QueryOperand = res;

    /* remove duplicates */
    while (ptr.offset_from(res) as c_int) < *size {
        if compareQueryOperand(
            ptr as *const c_void,
            prevptr as *const c_void,
            operand as *mut c_void,
        ) != 0
        {
            prevptr = prevptr.add(1);
            *prevptr = *ptr;
        }
        ptr = ptr.add(1);
    }

    *size = (prevptr.add(1)).offset_from(res) as c_int;
    res
}

fn calc_rank_and(w: *const float4, t: TSVector, q: TSQuery) -> float4 {
    unsafe {
        let pos: *mut *mut WordEntryPosVector;
        let mut posnull: WordEntryPosVector1 = WordEntryPosVector1 { npos: 0, pos: [0; 1] };
        let POSNULL: *mut WordEntryPosVector;
        let mut entry: *mut WordEntry;
        let mut firstentry: *mut WordEntry;
        let mut post: *mut WordEntryPos;
        let mut ct: *mut WordEntryPos;
        let mut dimt: int32;
        let mut lenct: int32;
        let mut dist: int32;
        let mut nitem: int32 = 0;
        let mut res: float4 = -1.0;
        let item: *mut *mut QueryOperand;
        let mut size: c_int = (*q).size;

        item = SortAndUniqItems(q, &mut size);
        if size < 2 {
            pfree(item as *mut c_void);
            return calc_rank_or(w, t, q);
        }
        pos = palloc0(
            core::mem::size_of::<*mut WordEntryPosVector>() * (*q).size as usize,
        ) as *mut *mut WordEntryPosVector;

        /* A dummy WordEntryPos array to use when haspos is false */
        posnull.npos = 1;
        posnull.pos[0] = 0;
        WEP_SETPOS(&mut posnull.pos[0], MAXENTRYPOS - 1);
        POSNULL = &mut posnull as *mut WordEntryPosVector1 as *mut WordEntryPosVector;

        let mut i: c_int = 0;
        while i < size {
            entry = find_wordentry(t, q, *item.add(i as usize), &mut nitem);
            firstentry = entry;
            if entry.is_null() {
                i += 1;
                continue;
            }

            while entry.offset_from(firstentry) < nitem as isize {
                if (*entry).haspos() != 0 {
                    *pos.add(i as usize) = _POSVECPTR(t, entry);
                } else {
                    *pos.add(i as usize) = POSNULL;
                }

                dimt = (**pos.add(i as usize)).npos as int32;
                post = (**pos.add(i as usize)).pos.as_mut_ptr();
                let mut k: c_int = 0;
                while k < i {
                    if (*pos.add(k as usize)).is_null() {
                        k += 1;
                        continue;
                    }
                    lenct = (**pos.add(k as usize)).npos as int32;
                    ct = (**pos.add(k as usize)).pos.as_mut_ptr();
                    let mut l: c_int = 0;
                    while l < dimt {
                        let mut p: c_int = 0;
                        while p < lenct {
                            dist = (WEP_GETPOS(*post.add(l as usize))
                                - WEP_GETPOS(*ct.add(p as usize)))
                            .abs();
                            if dist != 0
                                || (dist == 0
                                    && (*pos.add(i as usize) == POSNULL
                                        || *pos.add(k as usize) == POSNULL))
                            {
                                let curw: float4;

                                if dist == 0 {
                                    dist = MAXENTRYPOS;
                                }
                                curw = sqrt(
                                    (wpos(w, *post.add(l as usize))
                                        * wpos(w, *ct.add(p as usize))
                                        * word_distance(dist)) as f64,
                                ) as float4;
                                res = if res < 0.0 {
                                    curw
                                } else {
                                    1.0 - (1.0 - res) * (1.0 - curw)
                                };
                            }
                            p += 1;
                        }
                        l += 1;
                    }
                    k += 1;
                }

                entry = entry.add(1);
            }
            i += 1;
        }
        pfree(pos as *mut c_void);
        pfree(item as *mut c_void);
        res
    }
}

fn calc_rank_or(w: *const float4, t: TSVector, q: TSQuery) -> float4 {
    unsafe {
        let mut entry: *mut WordEntry;
        let mut firstentry: *mut WordEntry;
        let mut posnull: WordEntryPosVector1 = WordEntryPosVector1 { npos: 0, pos: [0; 1] };
        let mut post: *mut WordEntryPos;
        let mut dimt: int32;
        let mut nitem: int32 = 0;
        let mut res: float4 = 0.0;
        let item: *mut *mut QueryOperand;
        let mut size: c_int = (*q).size;

        /* A dummy WordEntryPos array to use when haspos is false */
        posnull.npos = 1;
        posnull.pos[0] = 0;

        item = SortAndUniqItems(q, &mut size);

        let mut i: int32 = 0;
        while i < size {
            let mut resj: float4;
            let mut wjm: float4;
            let mut jm: int32;

            entry = find_wordentry(t, q, *item.add(i as usize), &mut nitem);
            firstentry = entry;
            if entry.is_null() {
                i += 1;
                continue;
            }

            while entry.offset_from(firstentry) < nitem as isize {
                if (*entry).haspos() != 0 {
                    dimt = POSDATALEN(t, entry);
                    post = POSDATAPTR(t, entry);
                } else {
                    dimt = posnull.npos as int32;
                    post = posnull.pos.as_mut_ptr();
                }

                resj = 0.0;
                wjm = -1.0;
                jm = 0;
                let mut j: int32 = 0;
                while j < dimt {
                    resj = resj + wpos(w, *post.add(j as usize)) / ((j + 1) * (j + 1)) as float4;
                    if wpos(w, *post.add(j as usize)) > wjm {
                        wjm = wpos(w, *post.add(j as usize));
                        jm = j;
                    }
                    j += 1;
                }
                /*
                            limit (sum(1/i^2),i=1,inf) = pi^2/6
                            resj = sum(wi/i^2),i=1,noccurrence,
                            wi - should be sorted desc,
                            don't sort for now, just choose maximum weight. This should be corrected
                            Oleg Bartunov
                */
                res = res
                    + (wjm + resj - wjm / ((jm + 1) * (jm + 1)) as float4) / 1.64493406685;

                entry = entry.add(1);
            }
            i += 1;
        }
        if size > 0 {
            res = res / size as float4;
        }
        pfree(item as *mut c_void);
        res
    }
}

fn calc_rank(w: *const float4, t: TSVector, q: TSQuery, method: int32) -> float4 {
    unsafe {
        let item: *mut QueryItem = GETQUERY(q);
        let mut res: float4;
        let len: c_int;

        if (*t).size == 0 || (*q).size == 0 {
            return 0.0;
        }

        /* XXX: What about NOT? */
        res = if (*item).type_() == QI_OPR
            && ((*item).qoperator.oper == OP_AND || (*item).qoperator.oper == OP_PHRASE)
        {
            calc_rank_and(w, t, q)
        } else {
            calc_rank_or(w, t, q)
        };

        if res < 0.0 {
            res = 1e-20;
        }

        if (method & RANK_NORM_LOGLENGTH) != 0 && (*t).size > 0 {
            res /= (log((cnt_length(t) + 1) as f64) / log(2.0)) as float4;
        }

        if method & RANK_NORM_LENGTH != 0 {
            len = cnt_length(t);
            if len > 0 {
                res /= len as float4;
            }
        }

        /* RANK_NORM_EXTDIST not applicable */

        if (method & RANK_NORM_UNIQ) != 0 && (*t).size > 0 {
            res /= (*t).size as float4;
        }

        if (method & RANK_NORM_LOGUNIQ) != 0 && (*t).size > 0 {
            res /= (log(((*t).size + 1) as f64) / log(2.0)) as float4;
        }

        if method & RANK_NORM_RDIVRPLUS1 != 0 {
            res /= res + 1.0;
        }

        res
    }
}

/*
 * Extract weights from an array.  The weights are stored in *ws, which must
 * have space for NUM_WEIGHTS elements.
 */
unsafe fn getWeights(win: *mut ArrayType, ws: *mut float4) {
    let arrdata: *mut float4;

    /* Assert(win != NULL); */
    debug_assert!(!win.is_null());

    if ARR_NDIM(win) != 1 {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(ERROR, errmsg!("array of weight must be one-dimensional"));
    }

    if ArrayGetNItems(ARR_NDIM(win), ARR_DIMS(win)) < NUM_WEIGHTS as c_int {
        let _ = errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR);
        ereport!(ERROR, errmsg!("array of weight is too short"));
    }

    /*
     * array_contains_nulls(win): inlined null-bitmap scan (arrayfuncs.c not yet
     * ported).  ARR_HASNULL is a fast-path negative; otherwise scan the bitmap
     * over ArrayGetNItems() elements for any cleared bit.
     */
    if array_contains_nulls(win) {
        let _ = errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED);
        ereport!(ERROR, errmsg!("array of weight must not contain nulls"));
    }

    arrdata = ARR_DATA_PTR(win) as *mut float4;
    for i in 0..NUM_WEIGHTS {
        *ws.add(i) = if *arrdata.add(i) >= 0.0 {
            *arrdata.add(i)
        } else {
            default_weights[i]
        };
        if *ws.add(i) > 1.0 {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(ERROR, errmsg!("weight out of range"));
        }
    }
}

/*
 * array_contains_nulls (arrayfuncs.c) - inlined here.  Returns true if the array
 * has any NULL element.  Mirrors the C: fast-path on !ARR_HASNULL, else scan the
 * null bitmap for a cleared bit over the element count.
 */
unsafe fn array_contains_nulls(array: *mut ArrayType) -> bool {
    /* Easy answer if there's no null bitmap */
    if !ARR_HASNULL(array) {
        return false;
    }

    let bitmap: *mut u8 = ARR_NULLBITMAP(array);
    if bitmap.is_null() {
        return false;
    }

    let nelems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

    /* check whole bytes of the bitmap byte-at-a-time */
    let mut bitmask: c_int = 1;
    let mut bptr: *mut u8 = bitmap;
    let mut n = nelems;
    while n >= 8 {
        if *bptr != 0xff {
            return true;
        }
        bptr = bptr.add(1);
        n -= 8;
    }

    /* check last partial byte */
    let mut i = 0;
    while i < n {
        if (*bptr as c_int & bitmask) == 0 {
            return true;
        }
        bitmask <<= 1;
        i += 1;
    }

    false
}

pub unsafe fn ts_rank_wttf(fcinfo: FunctionCallInfo) -> Datum {
    let win = PG_DETOAST_DATUM!(PG_GETARG_DATUM!(fcinfo, 0)) as *mut ArrayType;
    let txt: TSVector = PG_GETARG_TSVECTOR(fcinfo, 1);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 2);
    let method: c_int = PG_GETARG_INT32!(fcinfo, 3);
    let mut weights: [float4; NUM_WEIGHTS] = [0.0; NUM_WEIGHTS];
    let res: float4;

    getWeights(win, weights.as_mut_ptr());
    res = calc_rank(weights.as_ptr(), txt, query, method);

    PG_FREE_IF_COPY!(fcinfo, win, 0);
    PG_FREE_IF_COPY!(fcinfo, txt, 1);
    PG_FREE_IF_COPY!(fcinfo, query, 2);
    PG_RETURN_FLOAT4!(res);
}

pub unsafe fn ts_rank_wtt(fcinfo: FunctionCallInfo) -> Datum {
    let win = PG_DETOAST_DATUM!(PG_GETARG_DATUM!(fcinfo, 0)) as *mut ArrayType;
    let txt: TSVector = PG_GETARG_TSVECTOR(fcinfo, 1);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 2);
    let mut weights: [float4; NUM_WEIGHTS] = [0.0; NUM_WEIGHTS];
    let res: float4;

    getWeights(win, weights.as_mut_ptr());
    res = calc_rank(weights.as_ptr(), txt, query, DEF_NORM_METHOD);

    PG_FREE_IF_COPY!(fcinfo, win, 0);
    PG_FREE_IF_COPY!(fcinfo, txt, 1);
    PG_FREE_IF_COPY!(fcinfo, query, 2);
    PG_RETURN_FLOAT4!(res);
}

pub unsafe fn ts_rank_ttf(fcinfo: FunctionCallInfo) -> Datum {
    let txt: TSVector = PG_GETARG_TSVECTOR(fcinfo, 0);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 1);
    let method: c_int = PG_GETARG_INT32!(fcinfo, 2);
    let res: float4;

    res = calc_rank(default_weights.as_ptr(), txt, query, method);

    PG_FREE_IF_COPY!(fcinfo, txt, 0);
    PG_FREE_IF_COPY!(fcinfo, query, 1);
    PG_RETURN_FLOAT4!(res);
}

pub unsafe fn ts_rank_tt(fcinfo: FunctionCallInfo) -> Datum {
    let txt: TSVector = PG_GETARG_TSVECTOR(fcinfo, 0);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 1);
    let res: float4;

    res = calc_rank(default_weights.as_ptr(), txt, query, DEF_NORM_METHOD);

    PG_FREE_IF_COPY!(fcinfo, txt, 0);
    PG_FREE_IF_COPY!(fcinfo, query, 1);
    PG_RETURN_FLOAT4!(res);
}

/*
 * typedef struct { union { struct { QueryItem **items; int16 nitem; } query;
 *                          struct { QueryItem *item; WordEntry *entry; } map; } data;
 *                  WordEntryPos pos; } DocRepresentation;
 *
 * The C union overlaps two same-sized two-pointer structs.  The `query` arm needs
 * a separate `nitem` field which would overflow the two-pointer footprint, but C
 * sizes the struct to hold it (pointer + int16, padded).  We model both arms as
 * explicit fields and read whichever the algorithm dictates; this is layout-
 * compatible for our purposes because the struct is only ever used internally
 * (never reinterpreted from foreign bytes) and is sized via size_of::<Self>().
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct DocRepQuery {
    items: *mut *mut QueryItem,
    nitem: int16,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DocRepMap {
    item: *mut QueryItem,
    entry: *mut WordEntry,
}

#[repr(C)]
#[derive(Clone, Copy)]
union DocRepData {
    query: DocRepQuery,
    map: DocRepMap,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DocRepresentation {
    data: DocRepData,
    pos: WordEntryPos,
}

unsafe fn compareDocR(va: *const c_void, vb: *const c_void) -> c_int {
    let a = va as *const DocRepresentation;
    let b = vb as *const DocRepresentation;

    if WEP_GETPOS((*a).pos) == WEP_GETPOS((*b).pos) {
        if WEP_GETWEIGHT((*a).pos) == WEP_GETWEIGHT((*b).pos) {
            if (*a).data.map.entry == (*b).data.map.entry {
                return 0;
            }

            return if (*a).data.map.entry > (*b).data.map.entry {
                1
            } else {
                -1
            };
        }

        return if WEP_GETWEIGHT((*a).pos) > WEP_GETWEIGHT((*b).pos) {
            1
        } else {
            -1
        };
    }

    if WEP_GETPOS((*a).pos) > WEP_GETPOS((*b).pos) {
        1
    } else {
        -1
    }
}

const MAXQROPOS: c_int = MAXENTRYPOS;

#[repr(C)]
struct QueryRepresentationOperand {
    operandexists: bool,
    /* indicates insert order, true means descending order */
    reverseinsert: bool,
    npos: uint32,
    pos: [WordEntryPos; MAXQROPOS as usize],
}

#[repr(C)]
struct QueryRepresentation {
    query: TSQuery,
    operandData: *mut QueryRepresentationOperand,
}

/*
 * #define QR_GET_OPERAND_DATA(q, v) \
 *   ( (q)->operandData + (((QueryItem*)(v)) - GETQUERY((q)->query)) )
 */
#[inline]
unsafe fn QR_GET_OPERAND_DATA(
    q: *mut QueryRepresentation,
    v: *mut QueryItem,
) -> *mut QueryRepresentationOperand {
    (*q)
        .operandData
        .offset(v.offset_from(GETQUERY((*q).query)))
}

/*
 * TS_execute callback for matching a tsquery operand to QueryRepresentation
 */
unsafe fn checkcondition_QueryOperand(
    checkval: *mut c_void,
    val: *mut QueryOperand,
    data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let qr = checkval as *mut QueryRepresentation;
    let opData = QR_GET_OPERAND_DATA(qr, val as *mut QueryItem);

    if !(*opData).operandexists {
        return TS_NO;
    }

    if !data.is_null() {
        (*data).npos = (*opData).npos as c_int;
        (*data).pos = (*opData).pos.as_mut_ptr();
        if (*opData).reverseinsert {
            (*data).pos = (*data).pos.add((MAXQROPOS as u32 - (*opData).npos) as usize);
        }
    }

    TS_YES
}

#[repr(C)]
struct CoverExt {
    pos: c_int,
    p: c_int,
    q: c_int,
    begin: *mut DocRepresentation,
    end: *mut DocRepresentation,
}

unsafe fn resetQueryRepresentation(qr: *mut QueryRepresentation, reverseinsert: bool) {
    let mut i: c_int = 0;
    while i < (*(*qr).query).size {
        (*(*qr).operandData.offset(i as isize)).operandexists = false;
        (*(*qr).operandData.offset(i as isize)).reverseinsert = reverseinsert;
        (*(*qr).operandData.offset(i as isize)).npos = 0;
        i += 1;
    }
}

unsafe fn fillQueryRepresentationData(qr: *mut QueryRepresentation, entry: *mut DocRepresentation) {
    let mut lastPos: c_int;

    let mut i: c_int = 0;
    while i < (*entry).data.query.nitem as c_int {
        let it = *(*entry).data.query.items.add(i as usize);
        if (*it).type_() != QI_VAL {
            i += 1;
            continue;
        }

        let opData = QR_GET_OPERAND_DATA(qr, it);

        (*opData).operandexists = true;

        if (*opData).npos == 0 {
            lastPos = if (*opData).reverseinsert {
                MAXQROPOS - 1
            } else {
                0
            };
            (*opData).pos[lastPos as usize] = (*entry).pos;
            (*opData).npos += 1;
            i += 1;
            continue;
        }

        lastPos = if (*opData).reverseinsert {
            MAXQROPOS - (*opData).npos as c_int
        } else {
            (*opData).npos as c_int - 1
        };

        if WEP_GETPOS((*opData).pos[lastPos as usize]) != WEP_GETPOS((*entry).pos) {
            lastPos = if (*opData).reverseinsert {
                MAXQROPOS - 1 - (*opData).npos as c_int
            } else {
                (*opData).npos as c_int
            };

            (*opData).pos[lastPos as usize] = (*entry).pos;
            (*opData).npos += 1;
        }
        i += 1;
    }
}

unsafe fn Cover(
    doc: *mut DocRepresentation,
    len: c_int,
    qr: *mut QueryRepresentation,
    ext: *mut CoverExt,
) -> bool {
    let mut ptr: *mut DocRepresentation;
    let mut lastpos: c_int = (*ext).pos;
    let mut found = false;

    /*
     * since this function recurses, it could be driven to stack overflow.
     * (though any decent compiler will optimize away the tail-recursion.)
     */
    check_stack_depth();

    resetQueryRepresentation(qr, false);

    (*ext).p = INT_MAX;
    (*ext).q = 0;
    ptr = doc.offset((*ext).pos as isize);

    /* find upper bound of cover from current position, move up */
    while ptr.offset_from(doc) < len as isize {
        fillQueryRepresentationData(qr, ptr);

        if TS_execute(
            GETQUERY((*qr).query),
            qr as *mut c_void,
            TS_EXEC_EMPTY,
            checkcondition_QueryOperand,
        ) {
            if WEP_GETPOS((*ptr).pos) > (*ext).q {
                (*ext).q = WEP_GETPOS((*ptr).pos);
                (*ext).end = ptr;
                lastpos = ptr.offset_from(doc) as c_int;
                found = true;
            }
            break;
        }
        ptr = ptr.add(1);
    }

    if !found {
        return false;
    }

    resetQueryRepresentation(qr, true);

    ptr = doc.offset(lastpos as isize);

    /* find lower bound of cover from found upper bound, move down */
    while ptr >= doc.offset((*ext).pos as isize) {
        /*
         * we scan doc from right to left, so pos info in reverse order!
         */
        fillQueryRepresentationData(qr, ptr);

        if TS_execute(
            GETQUERY((*qr).query),
            qr as *mut c_void,
            TS_EXEC_EMPTY,
            checkcondition_QueryOperand,
        ) {
            if WEP_GETPOS((*ptr).pos) < (*ext).p {
                (*ext).begin = ptr;
                (*ext).p = WEP_GETPOS((*ptr).pos);
            }
            break;
        }
        ptr = ptr.offset(-1);
    }

    if (*ext).p <= (*ext).q {
        /*
         * set position for next try to next lexeme after beginning of found cover
         */
        (*ext).pos = (ptr.offset_from(doc) as c_int) + 1;
        return true;
    }

    (*ext).pos += 1;
    Cover(doc, len, qr, ext)
}

unsafe fn get_docrep(
    txt: TSVector,
    qr: *mut QueryRepresentation,
    doclen: *mut c_int,
) -> *mut DocRepresentation {
    let item: *mut QueryItem = GETQUERY((*qr).query);
    let mut entry: *mut WordEntry;
    let mut firstentry: *mut WordEntry;
    let mut post: *mut WordEntryPos;
    let mut dimt: int32; /* number of 'post' items */
    let mut nitem: int32 = 0;
    let mut len: c_int = (*(*qr).query).size * 4;
    let mut cur: c_int = 0;
    let mut doc: *mut DocRepresentation;

    doc = palloc(core::mem::size_of::<DocRepresentation>() * len as usize) as *mut DocRepresentation;

    /*
     * Iterate through query to make DocRepresentation for words and it's
     * entries satisfied by query
     */
    let mut i: c_int = 0;
    while i < (*(*qr).query).size {
        let curoperand: *mut QueryOperand;

        if (*item.add(i as usize)).type_() != QI_VAL {
            i += 1;
            continue;
        }

        curoperand = &mut (*item.add(i as usize)).qoperand as *mut QueryOperand;

        entry = find_wordentry(txt, (*qr).query, curoperand, &mut nitem);
        firstentry = entry;
        if entry.is_null() {
            i += 1;
            continue;
        }

        /* iterations over entries in tsvector */
        while entry.offset_from(firstentry) < nitem as isize {
            if (*entry).haspos() != 0 {
                dimt = POSDATALEN(txt, entry);
                post = POSDATAPTR(txt, entry);
            } else {
                /* ignore words without positions */
                entry = entry.add(1);
                continue;
            }

            while cur + dimt >= len {
                len *= 2;
                doc = repalloc(
                    doc as *mut c_void,
                    core::mem::size_of::<DocRepresentation>() * len as usize,
                ) as *mut DocRepresentation;
            }

            /* iterations over entry's positions */
            let mut j: int32 = 0;
            while j < dimt {
                if (*curoperand).weight == 0
                    || ((*curoperand).weight as c_int
                        & (1 << WEP_GETWEIGHT(*post.add(j as usize))))
                        != 0
                {
                    (*doc.offset(cur as isize)).pos = *post.add(j as usize);
                    (*doc.offset(cur as isize)).data.map.entry = entry;
                    (*doc.offset(cur as isize)).data.map.item = curoperand as *mut QueryItem;
                    cur += 1;
                }
                j += 1;
            }

            entry = entry.add(1);
        }
        i += 1;
    }

    if cur > 0 {
        let mut rptr: *mut DocRepresentation = doc.offset(1);
        let mut wptr: *mut DocRepresentation = doc;
        let mut storage: DocRepresentation = core::mem::zeroed();

        /*
         * Sort representation in ascending order by pos and entry
         */
        pg_qsort(
            doc as *mut c_void,
            cur as usize,
            core::mem::size_of::<DocRepresentation>(),
            compareDocR,
        );

        /*
         * Join QueryItem per WordEntry and its position
         */
        storage.pos = (*doc).pos;
        storage.data.query.items = palloc(
            core::mem::size_of::<*mut QueryItem>() * (*(*qr).query).size as usize,
        ) as *mut *mut QueryItem;
        *storage.data.query.items.add(0) = (*doc).data.map.item;
        storage.data.query.nitem = 1;

        while rptr.offset_from(doc) < cur as isize {
            if (*rptr).pos == (*rptr.offset(-1)).pos
                && (*rptr).data.map.entry == (*rptr.offset(-1)).data.map.entry
            {
                *storage
                    .data
                    .query
                    .items
                    .add(storage.data.query.nitem as usize) = (*rptr).data.map.item;
                storage.data.query.nitem += 1;
            } else {
                *wptr = storage;
                wptr = wptr.add(1);
                storage.pos = (*rptr).pos;
                storage.data.query.items = palloc(
                    core::mem::size_of::<*mut QueryItem>() * (*(*qr).query).size as usize,
                ) as *mut *mut QueryItem;
                *storage.data.query.items.add(0) = (*rptr).data.map.item;
                storage.data.query.nitem = 1;
            }

            rptr = rptr.add(1);
        }

        *wptr = storage;
        wptr = wptr.add(1);

        *doclen = wptr.offset_from(doc) as c_int;
        return doc;
    }

    pfree(doc as *mut c_void);
    null_mut()
}

fn calc_rank_cd(arrdata: *const float4, txt: TSVector, query: TSQuery, method: c_int) -> float4 {
    unsafe {
        let doc: *mut DocRepresentation;
        let len: c_int;
        let mut doclen: c_int = 0;
        let mut ext: CoverExt = core::mem::zeroed();
        let mut Wdoc: f64 = 0.0;
        let mut invws: [f64; NUM_WEIGHTS] = [0.0; NUM_WEIGHTS];
        let mut SumDist: f64 = 0.0;
        let mut PrevExtPos: f64 = 0.0;
        let mut NExtent: c_int = 0;
        let mut qr: QueryRepresentation = QueryRepresentation {
            query: null_mut(),
            operandData: null_mut(),
        };

        for i in 0..NUM_WEIGHTS {
            invws[i] = if *arrdata.add(i) >= 0.0 {
                *arrdata.add(i)
            } else {
                default_weights[i]
            } as f64;
            if invws[i] > 1.0 {
                let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                ereport!(ERROR, errmsg!("weight out of range"));
            }
            invws[i] = 1.0 / invws[i];
        }

        qr.query = query;
        qr.operandData = palloc0(
            core::mem::size_of::<QueryRepresentationOperand>() * (*query).size as usize,
        ) as *mut QueryRepresentationOperand;

        doc = get_docrep(txt, &mut qr, &mut doclen);
        if doc.is_null() {
            pfree(qr.operandData as *mut c_void);
            return 0.0;
        }

        MemSet(
            &mut ext as *mut CoverExt as *mut c_void,
            0,
            core::mem::size_of::<CoverExt>(),
        );
        while Cover(doc, doclen, &mut qr, &mut ext) {
            let Cpos: f64;
            let mut InvSum: f64 = 0.0;
            let CurExtPos: f64;
            let mut nNoise: c_int;
            let mut ptr: *mut DocRepresentation = ext.begin;

            while ptr <= ext.end {
                InvSum += invws[WEP_GETWEIGHT((*ptr).pos) as usize];
                ptr = ptr.add(1);
            }

            Cpos = (ext.end.offset_from(ext.begin) as f64 + 1.0) / InvSum;

            /*
             * if doc are big enough then ext.q may be equal to ext.p due to limit
             * of positional information. In this case we approximate number of
             * noise word as half cover's length
             */
            nNoise = (ext.q - ext.p) - (ext.end.offset_from(ext.begin) as c_int);
            if nNoise < 0 {
                nNoise = (ext.end.offset_from(ext.begin) as c_int) / 2;
            }
            Wdoc += Cpos / ((1 + nNoise) as f64);

            CurExtPos = ((ext.q + ext.p) as f64) / 2.0;
            if NExtent > 0 && CurExtPos > PrevExtPos
            /* prevent division by zero in a case of multiple lexize */
            {
                SumDist += 1.0 / (CurExtPos - PrevExtPos);
            }

            PrevExtPos = CurExtPos;
            NExtent += 1;
        }

        if (method & RANK_NORM_LOGLENGTH) != 0 && (*txt).size > 0 {
            Wdoc /= log((cnt_length(txt) + 1) as f64);
        }

        if method & RANK_NORM_LENGTH != 0 {
            len = cnt_length(txt);
            if len > 0 {
                Wdoc /= len as f64;
            }
        }

        if (method & RANK_NORM_EXTDIST) != 0 && NExtent > 0 && SumDist > 0.0 {
            Wdoc /= (NExtent as f64) / SumDist;
        }

        if (method & RANK_NORM_UNIQ) != 0 && (*txt).size > 0 {
            Wdoc /= (*txt).size as f64;
        }

        if (method & RANK_NORM_LOGUNIQ) != 0 && (*txt).size > 0 {
            Wdoc /= log(((*txt).size + 1) as f64) / log(2.0);
        }

        if method & RANK_NORM_RDIVRPLUS1 != 0 {
            Wdoc /= Wdoc + 1.0;
        }

        pfree(doc as *mut c_void);

        pfree(qr.operandData as *mut c_void);

        let _ = len;
        Wdoc as float4
    }
}

pub unsafe fn ts_rankcd_wttf(fcinfo: FunctionCallInfo) -> Datum {
    let win = PG_DETOAST_DATUM!(PG_GETARG_DATUM!(fcinfo, 0)) as *mut ArrayType;
    let txt: TSVector = PG_GETARG_TSVECTOR(fcinfo, 1);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 2);
    let method: c_int = PG_GETARG_INT32!(fcinfo, 3);
    let mut weights: [float4; NUM_WEIGHTS] = [0.0; NUM_WEIGHTS];
    let res: float4;

    getWeights(win, weights.as_mut_ptr());
    res = calc_rank_cd(weights.as_ptr(), txt, query, method);

    PG_FREE_IF_COPY!(fcinfo, win, 0);
    PG_FREE_IF_COPY!(fcinfo, txt, 1);
    PG_FREE_IF_COPY!(fcinfo, query, 2);
    PG_RETURN_FLOAT4!(res);
}

pub unsafe fn ts_rankcd_wtt(fcinfo: FunctionCallInfo) -> Datum {
    let win = PG_DETOAST_DATUM!(PG_GETARG_DATUM!(fcinfo, 0)) as *mut ArrayType;
    let txt: TSVector = PG_GETARG_TSVECTOR(fcinfo, 1);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 2);
    let mut weights: [float4; NUM_WEIGHTS] = [0.0; NUM_WEIGHTS];
    let res: float4;

    getWeights(win, weights.as_mut_ptr());
    res = calc_rank_cd(weights.as_ptr(), txt, query, DEF_NORM_METHOD);

    PG_FREE_IF_COPY!(fcinfo, win, 0);
    PG_FREE_IF_COPY!(fcinfo, txt, 1);
    PG_FREE_IF_COPY!(fcinfo, query, 2);
    PG_RETURN_FLOAT4!(res);
}

pub unsafe fn ts_rankcd_ttf(fcinfo: FunctionCallInfo) -> Datum {
    let txt: TSVector = PG_GETARG_TSVECTOR(fcinfo, 0);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 1);
    let method: c_int = PG_GETARG_INT32!(fcinfo, 2);
    let res: float4;

    res = calc_rank_cd(default_weights.as_ptr(), txt, query, method);

    PG_FREE_IF_COPY!(fcinfo, txt, 0);
    PG_FREE_IF_COPY!(fcinfo, query, 1);
    PG_RETURN_FLOAT4!(res);
}

pub unsafe fn ts_rankcd_tt(fcinfo: FunctionCallInfo) -> Datum {
    let txt: TSVector = PG_GETARG_TSVECTOR(fcinfo, 0);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 1);
    let res: float4;

    res = calc_rank_cd(default_weights.as_ptr(), txt, query, DEF_NORM_METHOD);

    PG_FREE_IF_COPY!(fcinfo, txt, 0);
    PG_FREE_IF_COPY!(fcinfo, query, 1);
    PG_RETURN_FLOAT4!(res);
}

/*
 * PG_GETARG_TSVECTOR(n) / PG_GETARG_TSQUERY(n): spelled as fns here, mirroring
 * tsvector.rs's private PG_GETARG_TSVECTOR helper.  The C macros are
 *   DatumGetTSVector(PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))   and
 *   DatumGetTSQuery(PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))
 */
#[inline]
unsafe fn PG_GETARG_TSVECTOR(fcinfo: FunctionCallInfo, n: c_int) -> TSVector {
    crate::utils::adt::tsvector::DatumGetTSVector(crate::postgres::PointerGetDatum(
        PG_DETOAST_DATUM!(PG_GETARG_DATUM!(fcinfo, n)) as *const core::ffi::c_void,
    ))
}

/*
 * Mirrors tsquery_op.rs's PG_GETARG_TSQUERY! macro: DatumGetPointer(PG_GETARG_DATUM).
 * (The crate's TSQuery getter does not detoast; we follow that convention.)
 */
#[inline]
unsafe fn PG_GETARG_TSQUERY(fcinfo: FunctionCallInfo, n: c_int) -> TSQuery {
    crate::postgres::DatumGetPointer(PG_GETARG_DATUM!(fcinfo, n)) as TSQuery
}

#[cfg(test)]
mod tests {
    use super::*;

    /*
     * getWeights parses a 4-element float4[] weight array.  We build a minimal
     * 1-D ArrayType with no null bitmap, four float4 elements, and check it copies
     * them through (and clamps negatives to default_weights).
     */
    #[test]
    fn test_get_weights_basic() {
        unsafe {
            // 1-D array, 4 elems, no nulls, elemtype float4 (OID 700), data = [0.1,0.2,0.4,1.0]
            // Layout: ArrayType header (vl_len_, ndim, dataoffset, elemtype) + dims[1] + lbound[1] + data
            let ndim = 1usize;
            let hdr = core::mem::size_of::<ArrayType>();
            let dimsz = ndim * core::mem::size_of::<c_int>() * 2; // dims + lbound
            let datasz = NUM_WEIGHTS * core::mem::size_of::<float4>();
            let total = hdr + dimsz + datasz;
            let buf = palloc0(total) as *mut u8;
            let arr = buf as *mut ArrayType;
            // vl_len_ : set to total << 2 (4-byte varlena length header convention)
            let arrref = &mut *arr;
            arrref.vl_len_ = (total as i32) << 2;
            arrref.ndim = 1;
            arrref.dataoffset = 0; // no null bitmap
            arrref.elemtype = 700; // FLOAT4OID
            // dims[0] = 4, lbound[0] = 1
            *ARR_DIMS(arr).add(0) = NUM_WEIGHTS as c_int;
            *ARR_DIMS(arr).add(1) = 1; // lbound shares region right after dims
            let data = ARR_DATA_PTR(arr) as *mut float4;
            *data.add(0) = 0.1;
            *data.add(1) = 0.2;
            *data.add(2) = -1.0; // negative -> default_weights[2] = 0.4
            *data.add(3) = 1.0;

            let mut ws: [float4; NUM_WEIGHTS] = [0.0; NUM_WEIGHTS];
            getWeights(arr, ws.as_mut_ptr());

            assert!((ws[0] - 0.1).abs() < 1e-6);
            assert!((ws[1] - 0.2).abs() < 1e-6);
            assert!((ws[2] - 0.4).abs() < 1e-6); // clamped to default
            assert!((ws[3] - 1.0).abs() < 1e-6);

            pfree(buf as *mut c_void);
        }
    }

    /*
     * word_distance is a pure monotonic-decreasing function on [1,100], saturating
     * to 1e-30 beyond 100.  Verify the documented behaviour without fmgr dispatch.
     */
    #[test]
    fn test_word_distance_monotonic() {
        let d1 = word_distance(1);
        let d50 = word_distance(50);
        let d100 = word_distance(100);
        let d101 = word_distance(101);
        assert!(d1 > d50, "closer collocation should rank higher");
        assert!(d50 > d100);
        assert!((d101 - 1e-30).abs() < 1e-35);
        // all weights are positive and <= the w=1 maximum
        assert!(d1 > 0.0 && d1 <= 1.0);
    }
}

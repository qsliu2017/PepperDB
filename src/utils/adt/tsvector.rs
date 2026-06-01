//! Translation of postgres/src/backend/utils/adt/tsvector.c
//!
//! I/O functions for the `tsvector` type.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   common/int.h          -> crate::common::int (pg_cmp_s32)
//!   libpq/pqformat.h      -> crate::libpq::pqformat (pq_begintypsend/pq_sendint16/32/
//!                            pq_sendtext/pq_sendbyte/pq_endtypsend; pq_getmsgint/
//!                            pq_getmsgstring on the recv side)
//!   nodes/miscnodes.h     -> escontext soft-error path; the elog shim never raises a
//!                            soft error, so SOFT_ERROR_OCCURRED() is always false and
//!                            `ereturn` sites become hard ereport!(ERROR, ...).
//!   tsearch/ts_locale.h   -> t_iseq / TOUCHAR rendered inline; pg_mblen_range from
//!                            crate::mb::mbutils.
//!   tsearch/ts_utils.h    -> the tsvector parser API (init_tsvector_parser /
//!                            gettoken_tsvector / close_tsvector_parser) and
//!                            tsCompareString.  The parser unit (tsvector_parser.c) is
//!                            NOT yet ported, so its API is declared as a local stub
//!                            module `tsvector_parser` (opaque state + unimplemented!()
//!                            bodies) mirroring ts_utils.h exactly.  tsCompareString
//!                            lives in tsvector_op.c; it is short and self-contained, so
//!                            it is translated inline here (compareentry depends on it).
//!   utils/fmgrprotos.h    -> fmgr argument/return macros (crate::utils::fmgr).
//!   utils/memutils.h      -> MaxAllocSize (prelude).
//!   varatt.h              -> VAR* macros (crate::varatt).
//!   tsearch/ts_type.h     -> WordEntry / WordEntryPos / WordEntryPosVector / TSVector
//!                            and the access macros, MERGED in below.
//!
//! TRANSLATED (everything actually present in tsvector.c):
//!   compareWordEntryPos, uniquePos, compareentry, uniqueentry,
//!   tsvectorin, tsvectorout, tsvectorsend, tsvectorrecv.
//!
//! NOTE: silly_cmp_tsvector / tsvector_cmp/eq/ne/lt/le/gt/ge / tsvector_length /
//! tsvector_strip / tsvector_setweight / tsvector_concat referenced by the
//! file-specific task notes actually live in the sibling unit tsvector_op.c, not in
//! tsvector.c, so they are out of scope for this file and are not defined here.
//!
//! STUBBED (deps not yet ported):
//!   - tsvector_parser module: tsvector_parser.c not yet translated.
//!   - the runtime bodies of pg_mblen_range (mb/mbutils) and pq_sendtext /
//!     pq_getmsgstring (mb/mbutils via pqformat) are themselves unimplemented!() in
//!     their home modules; tsvectorout/send/recv compile against them and would only
//!     fail at runtime once exercised.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;

use crate::{
    PG_GETARG_CSTRING, PG_GETARG_DATUM, PG_GETARG_POINTER, PG_RETURN_BYTEA_P, PG_RETURN_CSTRING,
    PG_RETURN_NULL,
};
use crate::c::{int32, uint16, uint32, Size};
use crate::common::int::pg_cmp_s32;
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgint, pq_getmsgstring, pq_sendint16, pq_sendint32,
    pq_sendtext,
};
use crate::mb::mbutils::{pg_database_encoding_max_length, pg_mblen_range};
use crate::nodes::nodes::Node;
use crate::postgres::PointerGetDatum;
use core::ffi::{c_char, c_int, c_void};

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
    fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    // sprintf for the "%d" position formatting in tsvectorout (variadic).
    fn sprintf(buf: *mut c_char, fmt: *const c_char, ...) -> c_int;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

// ================================================================
//   tsearch/ts_type.h  --  MERGED IN
// ================================================================

/*
 * WordEntry - one per lexeme in a tsvector.
 *
 * C bitfield:
 *     uint32 haspos:1, len:11 (MAX 2Kb), pos:20 (MAX 1Mb);
 *
 * We model it as a single u32 with accessor fns.  The bit packing follows the
 * platform ABI used by PostgreSQL on x86_64/aarch64 (little-endian, fields packed
 * from the least-significant bit upward): haspos in bit 0, len in bits 1..11,
 * pos in bits 12..31.  tsvectorsend/recv rely on sizeof(WordEntry) == 4.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WordEntry {
    pub bits: uint32,
}

const WORDENTRY_HASPOS_SHIFT: u32 = 0;
const WORDENTRY_LEN_SHIFT: u32 = 1;
const WORDENTRY_POS_SHIFT: u32 = 12;
const WORDENTRY_LEN_MASK: uint32 = (1 << 11) - 1; /* 11 bits */
const WORDENTRY_POS_MASK: uint32 = (1 << 20) - 1; /* 20 bits */

impl WordEntry {
    #[inline]
    pub fn new() -> WordEntry {
        WordEntry { bits: 0 }
    }
    #[inline]
    pub fn haspos(&self) -> uint32 {
        (self.bits >> WORDENTRY_HASPOS_SHIFT) & 0x1
    }
    #[inline]
    pub fn len(&self) -> uint32 {
        (self.bits >> WORDENTRY_LEN_SHIFT) & WORDENTRY_LEN_MASK
    }
    #[inline]
    pub fn pos(&self) -> uint32 {
        (self.bits >> WORDENTRY_POS_SHIFT) & WORDENTRY_POS_MASK
    }
    #[inline]
    pub fn set_haspos(&mut self, v: uint32) {
        self.bits = (self.bits & !(0x1 << WORDENTRY_HASPOS_SHIFT))
            | ((v & 0x1) << WORDENTRY_HASPOS_SHIFT);
    }
    #[inline]
    pub fn set_len(&mut self, v: uint32) {
        self.bits = (self.bits & !(WORDENTRY_LEN_MASK << WORDENTRY_LEN_SHIFT))
            | ((v & WORDENTRY_LEN_MASK) << WORDENTRY_LEN_SHIFT);
    }
    #[inline]
    pub fn set_pos(&mut self, v: uint32) {
        self.bits = (self.bits & !(WORDENTRY_POS_MASK << WORDENTRY_POS_SHIFT))
            | ((v & WORDENTRY_POS_MASK) << WORDENTRY_POS_SHIFT);
    }
}

pub const MAXSTRLEN: c_int = (1 << 11) - 1;
pub const MAXSTRPOS: c_int = (1 << 20) - 1;

/*
 * Equivalent to
 * typedef struct { uint16 weight:2, pos:14; }
 */
pub type WordEntryPos = uint16;

#[repr(C)]
pub struct WordEntryPosVector {
    pub npos: uint16,
    pub pos: [WordEntryPos; FLEXIBLE_ARRAY_MEMBER],
}

/* WordEntryPosVector with exactly 1 entry */
#[repr(C)]
pub struct WordEntryPosVector1 {
    pub npos: uint16,
    pub pos: [WordEntryPos; 1],
}

#[inline]
pub fn WEP_GETWEIGHT(x: WordEntryPos) -> c_int {
    (x >> 14) as c_int
}
#[inline]
pub fn WEP_GETPOS(x: WordEntryPos) -> c_int {
    (x & 0x3fff) as c_int
}
#[inline]
pub fn WEP_SETWEIGHT(x: &mut WordEntryPos, v: c_int) {
    *x = (((v as uint16) << 14) | (*x & 0x3fff)) as WordEntryPos;
}
#[inline]
pub fn WEP_SETPOS(x: &mut WordEntryPos, v: c_int) {
    *x = ((*x & 0xc000) | ((v as uint16) & 0x3fff)) as WordEntryPos;
}

pub const MAXENTRYPOS: c_int = 1 << 14;
pub const MAXNUMPOS: c_int = 256;
#[inline]
pub fn LIMITPOS(x: c_int) -> c_int {
    if x >= MAXENTRYPOS {
        MAXENTRYPOS - 1
    } else {
        x
    }
}

/* This struct represents a complete tsvector datum */
#[repr(C)]
pub struct TSVectorData {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub size: int32,
    pub entries: [WordEntry; FLEXIBLE_ARRAY_MEMBER],
    /* lexemes follow the entries[] array */
}

pub type TSVector = *mut TSVectorData;

/* offsetof(TSVectorData, entries) */
#[inline]
pub fn DATAHDRSIZE() -> usize {
    core::mem::offset_of!(TSVectorData, entries)
}

#[inline]
pub fn CALCDATASIZE(nentries: c_int, lenstr: c_int) -> usize {
    DATAHDRSIZE() + (nentries as usize) * core::mem::size_of::<WordEntry>() + (lenstr as usize)
}

/* pointer to start of a tsvector's WordEntry array */
#[inline]
pub unsafe fn ARRPTR(x: TSVector) -> *mut WordEntry {
    (*x).entries.as_mut_ptr()
}

/* pointer to start of a tsvector's lexeme storage: (char *) &x->entries[x->size] */
#[inline]
pub unsafe fn STRPTR(x: TSVector) -> *mut c_char {
    (*x).entries.as_mut_ptr().add((*x).size as usize) as *mut c_char
}

/* (WordEntryPosVector *)(STRPTR(x) + SHORTALIGN(e->pos + e->len)) */
#[inline]
pub unsafe fn _POSVECPTR(x: TSVector, e: *const WordEntry) -> *mut WordEntryPosVector {
    STRPTR(x).add(SHORTALIGN(((*e).pos() + (*e).len()) as usize)) as *mut WordEntryPosVector
}
#[inline]
pub unsafe fn POSDATALEN(x: TSVector, e: *const WordEntry) -> c_int {
    if (*e).haspos() != 0 {
        (*_POSVECPTR(x, e)).npos as c_int
    } else {
        0
    }
}
#[inline]
pub unsafe fn POSDATAPTR(x: TSVector, e: *const WordEntry) -> *mut WordEntryPos {
    (*_POSVECPTR(x, e)).pos.as_mut_ptr()
}

/* PG_GETARG_TSVECTOR(n) == DatumGetTSVector(PG_GETARG_DATUM!(fcinfo, n)).
 * The C macro detoasts; with the TOAST path unported we treat the datum pointer
 * as a plain in-line TSVector (pg_detoast_datum_packed identity for plain datums). */
#[inline]
pub unsafe fn DatumGetTSVector(x: Datum) -> TSVector {
    crate::varatt::pg_detoast_datum_packed(DatumGetPointer(x) as *mut c_void) as TSVector
}

/* TSVectorGetDatum(X) == PointerGetDatum(X).  PG_RETURN_TSVECTOR(x) is the C
 * macro `return TSVectorGetDatum(x)`; call sites use `return TSVectorGetDatum(x)`. */
#[inline]
pub unsafe fn TSVectorGetDatum(x: TSVector) -> Datum {
    PointerGetDatum(x as *const c_void)
}

// ================================================================
//   tsvector_parser API stub (tsearch/ts_utils.h)
// ================================================================
//
// TODO(pg-port): tsvector_parser.c is not yet translated.  The opaque parser-state
// type and the three entry points used by tsvectorin are declared here mirroring
// ts_utils.h; bodies are unimplemented!().  When tsvector_parser.rs lands these
// should be replaced by `use crate::utils::adt::tsvector_parser::{...}`.
#[allow(dead_code)]
mod tsvector_parser {
    use super::{Node, WordEntryPos};
    use core::ffi::{c_char, c_int, c_void};

    /* struct TSVectorParseStateData; opaque in tsvector_parser.c */
    pub enum TSVectorParseStateData {}
    pub type TSVectorParseState = *mut TSVectorParseStateData;

    /* flag bits that can be passed to init_tsvector_parser */
    pub const P_TSV_OPR_IS_DELIM: c_int = 1 << 0;
    pub const P_TSV_IS_TSQUERY: c_int = 1 << 1;
    pub const P_TSV_IS_WEB: c_int = 1 << 2;

    pub unsafe fn init_tsvector_parser(
        input: *mut c_char,
        flags: c_int,
        escontext: *mut Node,
    ) -> TSVectorParseState {
        let _ = (input, flags, escontext);
        unimplemented!("init_tsvector_parser: tsvector_parser.c not yet translated")
    }

    pub unsafe fn reset_tsvector_parser(state: TSVectorParseState, input: *mut c_char) {
        let _ = (state, input);
        unimplemented!("reset_tsvector_parser: tsvector_parser.c not yet translated")
    }

    pub unsafe fn gettoken_tsvector(
        state: TSVectorParseState,
        strval: *mut *mut c_char,
        lenval: *mut c_int,
        pos_ptr: *mut *mut WordEntryPos,
        poslen: *mut c_int,
        endptr: *mut *mut c_char,
    ) -> bool {
        let _ = (state, strval, lenval, pos_ptr, poslen, endptr);
        unimplemented!("gettoken_tsvector: tsvector_parser.c not yet translated")
    }

    pub unsafe fn close_tsvector_parser(state: TSVectorParseState) {
        let _ = state;
        unimplemented!("close_tsvector_parser: tsvector_parser.c not yet translated")
    }

    /* convenience for callers that pass `_ as *mut c_void` */
    pub type _Unused = *mut c_void;
}

use tsvector_parser::{
    close_tsvector_parser, gettoken_tsvector, init_tsvector_parser, TSVectorParseState,
};

// ================================================================
//   tsCompareString  (from tsvector_op.c; translated inline)
// ================================================================

/*
 * Compare two strings by tsvector rules.
 *
 * if prefix = true then it returns zero value iff b has prefix a
 *
 * # Safety
 * `a`/`b` are readable for `lena`/`lenb` bytes respectively.
 */
pub unsafe fn tsCompareString(
    a: *mut c_char,
    lena: c_int,
    b: *mut c_char,
    lenb: c_int,
    prefix: bool,
) -> int32 {
    let cmp: c_int;

    if lena == 0 {
        if prefix {
            cmp = 0; /* empty string is prefix of anything */
        } else {
            cmp = if lenb > 0 { -1 } else { 0 };
        }
    } else if lenb == 0 {
        cmp = if lena > 0 { 1 } else { 0 };
    } else {
        let mut c = memcmp(
            a as *const c_void,
            b as *const c_void,
            core::cmp::min(lena as u32, lenb as u32) as usize,
        );

        if prefix {
            if c == 0 && lena > lenb {
                c = 1; /* a is longer, so not a prefix of b */
            }
        } else if c == 0 && lena != lenb {
            c = if lena < lenb { -1 } else { 1 };
        }
        cmp = c;
    }

    cmp
}

// ================================================================
//   tsvector.c
// ================================================================

/*
 * WordEntryIN
 */
#[repr(C)]
#[derive(Clone, Copy)]
struct WordEntryIN {
    entry: WordEntry, /* must be first, see compareentry */
    pos: *mut WordEntryPos,
    poslen: c_int, /* number of elements in pos */
}

/* Compare two WordEntryPos values for qsort */
pub unsafe fn compareWordEntryPos(a: *const c_void, b: *const c_void) -> c_int {
    let apos = WEP_GETPOS(*(a as *const WordEntryPos));
    let bpos = WEP_GETPOS(*(b as *const WordEntryPos));

    pg_cmp_s32(apos, bpos)
}

/*
 * Removes duplicate pos entries. If there's two entries with same pos but
 * different weight, the higher weight is retained, so we can't use
 * qunique here.
 *
 * Returns new length.
 */
unsafe fn uniquePos(a: *mut WordEntryPos, l: c_int) -> c_int {
    let mut ptr: *mut WordEntryPos;
    let mut res: *mut WordEntryPos;

    if l <= 1 {
        return l;
    }

    // qsort(a, l, sizeof(WordEntryPos), compareWordEntryPos);
    let sl = core::slice::from_raw_parts_mut(a, l as usize);
    sl.sort_by(|x, y| {
        let r = compareWordEntryPos(
            x as *const WordEntryPos as *const c_void,
            y as *const WordEntryPos as *const c_void,
        );
        r.cmp(&0)
    });

    res = a;
    ptr = a.add(1);
    while ((ptr as isize - a as isize) / core::mem::size_of::<WordEntryPos>() as isize)
        < l as isize
    {
        if WEP_GETPOS(*ptr) != WEP_GETPOS(*res) {
            res = res.add(1);
            *res = *ptr;
            if (res as isize - a as isize) / core::mem::size_of::<WordEntryPos>() as isize
                >= (MAXNUMPOS - 1) as isize
                || WEP_GETPOS(*res) == MAXENTRYPOS - 1
            {
                break;
            }
        } else if WEP_GETWEIGHT(*ptr) > WEP_GETWEIGHT(*res) {
            let w = WEP_GETWEIGHT(*ptr);
            WEP_SETWEIGHT(&mut *res, w);
        }
        ptr = ptr.add(1);
    }

    ((res as isize - a as isize) / core::mem::size_of::<WordEntryPos>() as isize) as c_int + 1
}

/*
 * Compare two WordEntry structs for qsort_arg.  This can also be used on
 * WordEntryIN structs, since those have WordEntry as their first field.
 *
 * `arg` is the BufferStr (char *).
 */
unsafe fn compareentry(va: *const c_void, vb: *const c_void, arg: *mut c_void) -> c_int {
    let a = va as *const WordEntry;
    let b = vb as *const WordEntry;
    let BufferStr = arg as *mut c_char;

    tsCompareString(
        BufferStr.add((*a).pos() as usize),
        (*a).len() as c_int,
        BufferStr.add((*b).pos() as usize),
        (*b).len() as c_int,
        false,
    )
}

/*
 * Sort an array of WordEntryIN, remove duplicates.
 * *outbuflen receives the amount of space needed for strings and positions.
 */
unsafe fn uniqueentry(
    a: *mut WordEntryIN,
    l: c_int,
    buf: *mut c_char,
    outbuflen: *mut c_int,
) -> c_int {
    let mut buflen: c_int;
    let mut ptr: *mut WordEntryIN;
    let mut res: *mut WordEntryIN;

    Assert!(l >= 1);

    if l > 1 {
        // qsort_arg(a, l, sizeof(WordEntryIN), compareentry, buf);
        let sl = core::slice::from_raw_parts_mut(a, l as usize);
        sl.sort_by(|x, y| {
            let r = compareentry(
                x as *const WordEntryIN as *const c_void,
                y as *const WordEntryIN as *const c_void,
                buf as *mut c_void,
            );
            r.cmp(&0)
        });
    }

    buflen = 0;
    res = a;
    ptr = a.add(1);
    while ((ptr as isize - a as isize) / core::mem::size_of::<WordEntryIN>() as isize) < l as isize {
        if !((*ptr).entry.len() == (*res).entry.len()
            && strncmp(
                buf.add((*ptr).entry.pos() as usize),
                buf.add((*res).entry.pos() as usize),
                (*res).entry.len() as usize,
            ) == 0)
        {
            /* done accumulating data into *res, count space needed */
            buflen += (*res).entry.len() as c_int;
            if (*res).entry.haspos() != 0 {
                (*res).poslen = uniquePos((*res).pos, (*res).poslen);
                buflen = SHORTALIGN(buflen as usize) as c_int;
                buflen += (*res).poslen * core::mem::size_of::<WordEntryPos>() as c_int
                    + core::mem::size_of::<uint16>() as c_int;
            }
            res = res.add(1);
            if res != ptr {
                memcpy(
                    res as *mut c_void,
                    ptr as *const c_void,
                    core::mem::size_of::<WordEntryIN>(),
                );
            }
        } else if (*ptr).entry.haspos() != 0 {
            if (*res).entry.haspos() != 0 {
                /* append ptr's positions to res's positions */
                let newlen = (*ptr).poslen + (*res).poslen;

                (*res).pos = repalloc(
                    (*res).pos as *mut c_void,
                    (newlen as usize) * core::mem::size_of::<WordEntryPos>(),
                ) as *mut WordEntryPos;
                memcpy(
                    (*res).pos.add((*res).poslen as usize) as *mut c_void,
                    (*ptr).pos as *const c_void,
                    (*ptr).poslen as usize * core::mem::size_of::<WordEntryPos>(),
                );
                (*res).poslen = newlen;
                pfree((*ptr).pos as *mut c_void);
            } else {
                /* just give ptr's positions to pos */
                (*res).entry.set_haspos(1);
                (*res).pos = (*ptr).pos;
                (*res).poslen = (*ptr).poslen;
            }
        }
        ptr = ptr.add(1);
    }

    /* count space needed for last item */
    buflen += (*res).entry.len() as c_int;
    if (*res).entry.haspos() != 0 {
        (*res).poslen = uniquePos((*res).pos, (*res).poslen);
        buflen = SHORTALIGN(buflen as usize) as c_int;
        buflen += (*res).poslen * core::mem::size_of::<WordEntryPos>() as c_int
            + core::mem::size_of::<uint16>() as c_int;
    }

    *outbuflen = buflen;
    ((res as isize - a as isize) / core::mem::size_of::<WordEntryIN>() as isize) as c_int + 1
}

pub unsafe fn tsvectorin(fcinfo: FunctionCallInfo) -> Datum {
    let buf: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let escontext: *mut Node = (*fcinfo).context;
    let state: TSVectorParseState;
    let mut arr: *mut WordEntryIN;
    let totallen: c_int;
    let mut arrlen: c_int; /* allocated size of arr */
    let inarr: *mut WordEntry;
    let mut len: c_int = 0;
    let in_: TSVector;
    let mut i: c_int;
    let mut token: *mut c_char = null_mut();
    let mut toklen: c_int = 0;
    let mut pos: *mut WordEntryPos = null_mut();
    let mut poslen: c_int = 0;
    let strbuf: *mut c_char;
    let mut stroff: c_int;

    /*
     * Tokens are appended to tmpbuf, cur is a pointer to the end of used
     * space in tmpbuf.
     */
    let mut tmpbuf: *mut c_char;
    let mut cur: *mut c_char;
    let mut buflen: c_int = 256; /* allocated size of tmpbuf */

    state = init_tsvector_parser(buf, 0, escontext);

    arrlen = 64;
    arr = palloc(core::mem::size_of::<WordEntryIN>() * arrlen as usize) as *mut WordEntryIN;
    tmpbuf = palloc(buflen as Size) as *mut c_char;
    cur = tmpbuf;

    while gettoken_tsvector(
        state,
        &mut token as *mut *mut c_char,
        &mut toklen as *mut c_int,
        &mut pos as *mut *mut WordEntryPos,
        &mut poslen as *mut c_int,
        null_mut(),
    ) {
        if toklen >= MAXSTRLEN {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            // ereturn(escontext, (Datum) 0, ...): soft errors unsupported -> hard ERROR.
            ereport!(
                ERROR,
                errmsg!(
                    "word is too long ({} bytes, max {} bytes)",
                    toklen as i64,
                    (MAXSTRLEN - 1) as i64
                )
            );
        }

        if (cur as isize - tmpbuf as isize) as c_int > MAXSTRPOS {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!(
                    "string is too long for tsvector ({} bytes, max {} bytes)",
                    (cur as isize - tmpbuf as isize) as i64,
                    MAXSTRPOS as i64
                )
            );
        }

        /*
         * Enlarge buffers if needed
         */
        if len >= arrlen {
            arrlen *= 2;
            arr = repalloc(
                arr as *mut c_void,
                core::mem::size_of::<WordEntryIN>() * arrlen as usize,
            ) as *mut WordEntryIN;
        }
        while ((cur as isize - tmpbuf as isize) as c_int) + toklen >= buflen {
            let dist = (cur as isize - tmpbuf as isize) as c_int;

            buflen *= 2;
            tmpbuf = repalloc(tmpbuf as *mut c_void, buflen as usize) as *mut c_char;
            cur = tmpbuf.add(dist as usize);
        }
        (*arr.add(len as usize)).entry.set_len(toklen as uint32);
        (*arr.add(len as usize))
            .entry
            .set_pos((cur as isize - tmpbuf as isize) as uint32);
        memcpy(cur as *mut c_void, token as *const c_void, toklen as usize);
        cur = cur.add(toklen as usize);

        if poslen != 0 {
            (*arr.add(len as usize)).entry.set_haspos(1);
            (*arr.add(len as usize)).pos = pos;
            (*arr.add(len as usize)).poslen = poslen;
        } else {
            (*arr.add(len as usize)).entry.set_haspos(0);
            (*arr.add(len as usize)).pos = null_mut();
            (*arr.add(len as usize)).poslen = 0;
        }
        len += 1;
    }

    close_tsvector_parser(state);

    /* Did gettoken_tsvector fail? */
    if soft_error_occurred(escontext) {
        PG_RETURN_NULL!(fcinfo);
    }

    if len > 0 {
        len = uniqueentry(arr, len, tmpbuf, &mut buflen as *mut c_int);
    } else {
        buflen = 0;
    }

    if buflen > MAXSTRPOS {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!(
                "string is too long for tsvector ({} bytes, max {} bytes)",
                buflen,
                MAXSTRPOS
            )
        );
    }

    totallen = CALCDATASIZE(len, buflen) as c_int;
    in_ = palloc0(totallen as Size) as TSVector;
    SET_VARSIZE(in_ as *mut c_char, totallen);
    (*in_).size = len;
    inarr = ARRPTR(in_);
    strbuf = STRPTR(in_);
    stroff = 0;
    i = 0;
    while i < len {
        let ai = arr.add(i as usize);
        memcpy(
            strbuf.add(stroff as usize) as *mut c_void,
            tmpbuf.add((*ai).entry.pos() as usize) as *const c_void,
            (*ai).entry.len() as usize,
        );
        (*ai).entry.set_pos(stroff as uint32);
        stroff += (*ai).entry.len() as c_int;
        if (*ai).entry.haspos() != 0 {
            /* This should be unreachable because of MAXNUMPOS restrictions */
            if (*ai).poslen > 0xFFFF {
                elog!(ERROR, "positions array too long");
            }

            /* Copy number of positions */
            stroff = SHORTALIGN(stroff as usize) as c_int;
            *(strbuf.add(stroff as usize) as *mut uint16) = (*ai).poslen as uint16;
            stroff += core::mem::size_of::<uint16>() as c_int;

            /* Copy positions */
            memcpy(
                strbuf.add(stroff as usize) as *mut c_void,
                (*ai).pos as *const c_void,
                (*ai).poslen as usize * core::mem::size_of::<WordEntryPos>(),
            );
            stroff += (*ai).poslen * core::mem::size_of::<WordEntryPos>() as c_int;

            pfree((*ai).pos as *mut c_void);
        }
        *inarr.add(i as usize) = (*ai).entry;
        i += 1;
    }

    Assert!(
        (strbuf.add(stroff as usize) as isize - in_ as isize) == totallen as isize
    );

    return TSVectorGetDatum(in_); /* PG_RETURN_TSVECTOR(in) */
}

pub unsafe fn tsvectorout(fcinfo: FunctionCallInfo) -> Datum {
    let out: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let outbuf: *mut c_char;
    let mut i: int32;
    let mut lenbuf: int32 = 0;
    let mut pp: int32;
    let mut ptr: *mut WordEntry = ARRPTR(out);
    let mut curin: *mut c_char;
    let mut curout: *mut c_char;
    let mut curend: *const c_char;

    lenbuf = (*out).size * 2 /* '' */ + (*out).size - 1 /* space */ + 2 /* \0 */;
    i = 0;
    while i < (*out).size {
        lenbuf +=
            (*ptr.add(i as usize)).len() as i32 * 2 * pg_database_encoding_max_length(); /* for escape */
        if (*ptr.add(i as usize)).haspos() != 0 {
            lenbuf += 1 /* : */ + 7 /* int2 + , + weight */ * POSDATALEN(out, ptr.add(i as usize));
        }
        i += 1;
    }

    outbuf = palloc(lenbuf as Size) as *mut c_char;
    curout = outbuf;
    i = 0;
    while i < (*out).size {
        curin = STRPTR(out).add((*ptr).pos() as usize);
        curend = curin.add((*ptr).len() as usize);
        if i != 0 {
            *curout = b' ' as c_char;
            curout = curout.add(1);
        }
        *curout = b'\'' as c_char;
        curout = curout.add(1);
        while (curin as *const c_char) < curend {
            let mut len = pg_mblen_range(curin, curend);

            if t_iseq(curin, b'\'') {
                *curout = b'\'' as c_char;
                curout = curout.add(1);
            } else if t_iseq(curin, b'\\') {
                *curout = b'\\' as c_char;
                curout = curout.add(1);
            }

            while len != 0 {
                *curout = *curin;
                curout = curout.add(1);
                curin = curin.add(1);
                len -= 1;
            }
        }

        *curout = b'\'' as c_char;
        curout = curout.add(1);
        pp = POSDATALEN(out, ptr);
        if pp != 0 {
            let mut wptr: *mut WordEntryPos;

            *curout = b':' as c_char;
            curout = curout.add(1);
            wptr = POSDATAPTR(out, ptr);
            while pp != 0 {
                // curout += sprintf(curout, "%d", WEP_GETPOS(*wptr));
                curout = curout.add(sprintf(
                    curout,
                    c"%d".as_ptr(),
                    WEP_GETPOS(*wptr),
                ) as usize);
                match WEP_GETWEIGHT(*wptr) {
                    3 => {
                        *curout = b'A' as c_char;
                        curout = curout.add(1);
                    }
                    2 => {
                        *curout = b'B' as c_char;
                        curout = curout.add(1);
                    }
                    1 => {
                        *curout = b'C' as c_char;
                        curout = curout.add(1);
                    }
                    _ => {
                        /* case 0 / default: nothing */
                    }
                }

                if pp > 1 {
                    *curout = b',' as c_char;
                    curout = curout.add(1);
                }
                pp -= 1;
                wptr = wptr.add(1);
            }
        }
        ptr = ptr.add(1);
        i += 1;
    }

    *curout = b'\0' as c_char;
    /* PG_FREE_IF_COPY(out, 0): no-op, detoast of an in-line datum is identity. */
    PG_RETURN_CSTRING!(outbuf)
}

/*
 * Binary Input / Output functions. The binary format is as follows:
 *
 * uint32	number of lexemes
 *
 * for each lexeme:
 *		lexeme text in client encoding, null-terminated
 *		uint16	number of positions
 *		for each position:
 *			uint16 WordEntryPos
 */

pub unsafe fn tsvectorsend(fcinfo: FunctionCallInfo) -> Datum {
    let vec: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let mut buf_data: StringInfoData = core::mem::zeroed();
    let buf: StringInfo = &mut buf_data as *mut StringInfoData;
    let mut i: c_int;
    let mut j: c_int;
    let mut weptr: *mut WordEntry = ARRPTR(vec);

    pq_begintypsend(buf);

    pq_sendint32(buf, (*vec).size as uint32);
    i = 0;
    while i < (*vec).size {
        let npos: uint16;

        /*
         * the strings in the TSVector array are not null-terminated, so we
         * have to send the null-terminator separately
         */
        pq_sendtext(
            buf,
            STRPTR(vec).add((*weptr).pos() as usize),
            (*weptr).len() as c_int,
        );
        pq_sendbyte(buf, b'\0' as c_int);

        npos = POSDATALEN(vec, weptr) as uint16;
        pq_sendint16(buf, npos);

        if npos > 0 {
            let wepptr: *mut WordEntryPos = POSDATAPTR(vec, weptr);

            j = 0;
            while j < npos as c_int {
                pq_sendint16(buf, *wepptr.add(j as usize));
                j += 1;
            }
        }
        weptr = weptr.add(1);
        i += 1;
    }

    PG_RETURN_BYTEA_P!(pq_endtypsend(buf))
}

pub unsafe fn tsvectorrecv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let mut vec: TSVector;
    let mut i: c_int;
    let nentries: int32;
    let mut datalen: c_int; /* number of bytes used in the variable size area */
    let hdrlen: Size;
    let mut len: Size; /* allocated size of vec */
    let mut needSort: bool = false;

    nentries = pq_getmsgint(buf, core::mem::size_of::<int32>() as c_int) as int32;
    if nentries < 0
        || nentries as usize > (MaxAllocSize as usize / core::mem::size_of::<WordEntry>())
    {
        elog!(ERROR, "invalid size of tsvector");
    }

    hdrlen = DATAHDRSIZE() + core::mem::size_of::<WordEntry>() * nentries as usize;

    len = hdrlen * 2; /* times two to make room for lexemes */
    vec = palloc0(len) as TSVector;
    (*vec).size = nentries;

    datalen = 0;
    i = 0;
    while i < nentries {
        let lexeme: *const c_char;
        let npos: uint16;
        let lex_len: usize;

        lexeme = pq_getmsgstring(buf);
        npos = pq_getmsgint(buf, core::mem::size_of::<uint16>() as c_int) as uint16;

        /* sanity checks */

        lex_len = strlen(lexeme);
        if lex_len > MAXSTRLEN as usize {
            elog!(ERROR, "invalid tsvector: lexeme too long");
        }

        if datalen > MAXSTRPOS {
            elog!(ERROR, "invalid tsvector: maximum total lexeme length exceeded");
        }

        if npos as c_int > MAXNUMPOS {
            elog!(ERROR, "unexpected number of tsvector positions");
        }

        /*
         * Looks valid. Fill the WordEntry struct, and copy lexeme.
         *
         * But make sure the buffer is large enough first.
         */
        while hdrlen
            + SHORTALIGN(datalen as usize + lex_len)
            + core::mem::size_of::<uint16>()
            + npos as usize * core::mem::size_of::<WordEntryPos>()
            >= len
        {
            len *= 2;
            vec = repalloc(vec as *mut c_void, len) as TSVector;
        }

        (*ARRPTR(vec).add(i as usize)).set_haspos(if npos > 0 { 1 } else { 0 });
        (*ARRPTR(vec).add(i as usize)).set_len(lex_len as uint32);
        (*ARRPTR(vec).add(i as usize)).set_pos(datalen as uint32);

        memcpy(
            STRPTR(vec).add(datalen as usize) as *mut c_void,
            lexeme as *const c_void,
            lex_len,
        );

        datalen += lex_len as c_int;

        if i > 0
            && compareentry(
                ARRPTR(vec).add(i as usize) as *const c_void,
                ARRPTR(vec).add((i - 1) as usize) as *const c_void,
                STRPTR(vec) as *mut c_void,
            ) <= 0
        {
            needSort = true;
        }

        /* Receive positions */
        if npos > 0 {
            let mut j: uint16;
            let wepptr: *mut WordEntryPos;

            /*
             * Pad to 2-byte alignment if necessary. Though we used palloc0
             * for the initial allocation, subsequent repalloc'd memory areas
             * are not initialized to zero.
             */
            if datalen != SHORTALIGN(datalen as usize) as c_int {
                *(STRPTR(vec).add(datalen as usize)) = b'\0' as c_char;
                datalen = SHORTALIGN(datalen as usize) as c_int;
            }

            memcpy(
                STRPTR(vec).add(datalen as usize) as *mut c_void,
                &npos as *const uint16 as *const c_void,
                core::mem::size_of::<uint16>(),
            );

            wepptr = POSDATAPTR(vec, ARRPTR(vec).add(i as usize));
            j = 0;
            while j < npos {
                *wepptr.add(j as usize) =
                    pq_getmsgint(buf, core::mem::size_of::<WordEntryPos>() as c_int)
                        as WordEntryPos;
                if j > 0
                    && WEP_GETPOS(*wepptr.add(j as usize))
                        <= WEP_GETPOS(*wepptr.add((j - 1) as usize))
                {
                    elog!(ERROR, "position information is misordered");
                }
                j += 1;
            }

            datalen += core::mem::size_of::<uint16>() as c_int
                + npos as c_int * core::mem::size_of::<WordEntryPos>() as c_int;
        }
        i += 1;
    }

    SET_VARSIZE(vec as *mut c_char, (hdrlen + datalen as usize) as int32);

    if needSort {
        // qsort_arg(ARRPTR(vec), vec->size, sizeof(WordEntry), compareentry, STRPTR(vec));
        let strp = STRPTR(vec) as *mut c_void;
        let sl = core::slice::from_raw_parts_mut(ARRPTR(vec), (*vec).size as usize);
        sl.sort_by(|x, y| {
            let r = compareentry(
                x as *const WordEntry as *const c_void,
                y as *const WordEntry as *const c_void,
                strp,
            );
            r.cmp(&0)
        });
    }

    return TSVectorGetDatum(vec); /* PG_RETURN_TSVECTOR(vec) */
}

// ----------------------------------------------------------------
//   local helpers
// ----------------------------------------------------------------

/* PG_GETARG_TSVECTOR(n) macro spelled as a fn here (see DatumGetTSVector above). */
#[inline]
unsafe fn PG_GETARG_TSVECTOR(datum: Datum) -> TSVector {
    DatumGetTSVector(datum)
}

/*
 * SOFT_ERROR_OCCURRED(escontext) from nodes/miscnodes.h:
 *   ((escontext) != NULL && IsA(escontext, ErrorSaveContext) &&
 *    ((ErrorSaveContext *) (escontext))->error_occurred)
 * The elog shim never produces soft errors (ereturn sites became hard ERRORs),
 * so this is always false.
 * TODO(pg-port): real ErrorSaveContext + IsA(ErrorSaveContext) check.
 */
#[inline]
unsafe fn soft_error_occurred(escontext: *mut Node) -> bool {
    let _ = escontext;
    false
}

/*
 * t_iseq(x, c) from ts_locale.h: TOUCHAR(x) == (unsigned char) c.
 * The second argument must be a plain ASCII character.
 */
#[inline]
unsafe fn t_iseq(x: *const c_char, c: u8) -> bool {
    (*(x as *const u8)) == c
}

/*
 * pq_sendbyte(buf, byt): pqformat.h convenience for pq_sendint8.  Not exported by
 * the Rust pqformat module under that name, so it is inlined here.
 */
#[inline]
unsafe fn pq_sendbyte(buf: StringInfo, byt: c_int) {
    crate::libpq::pqformat::pq_sendint8(buf, byt as crate::c::uint8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn word_entry_bitfield_roundtrip() {
        let mut e = WordEntry::new();
        e.set_haspos(1);
        e.set_len(1234);
        e.set_pos(0xABCDE);
        assert_eq!(e.haspos(), 1);
        assert_eq!(e.len(), 1234);
        assert_eq!(e.pos(), 0xABCDE);

        // overwrite individual fields without disturbing the others
        e.set_haspos(0);
        assert_eq!(e.haspos(), 0);
        assert_eq!(e.len(), 1234);
        assert_eq!(e.pos(), 0xABCDE);

        // WordEntry must be 4 bytes (tsvectorsend/recv rely on this)
        assert_eq!(core::mem::size_of::<WordEntry>(), 4);
    }

    #[test]
    fn wep_weight_pos_roundtrip() {
        let mut x: WordEntryPos = 0;
        WEP_SETPOS(&mut x, 1000);
        WEP_SETWEIGHT(&mut x, 3);
        assert_eq!(WEP_GETPOS(x), 1000);
        assert_eq!(WEP_GETWEIGHT(x), 3);

        // setpos must not clobber weight, and vice versa
        WEP_SETPOS(&mut x, 42);
        assert_eq!(WEP_GETWEIGHT(x), 3);
        assert_eq!(WEP_GETPOS(x), 42);
    }

    #[test]
    fn ts_compare_string_rules() {
        unsafe {
            let a = b"abc\0".as_ptr() as *mut c_char;
            let b = b"abd\0".as_ptr() as *mut c_char;
            assert!(tsCompareString(a, 3, b, 3, false) < 0);
            assert_eq!(tsCompareString(a, 3, a, 3, false), 0);
            // empty vs nonempty
            assert!(tsCompareString(a, 0, b, 3, false) < 0);
            assert_eq!(tsCompareString(a, 0, b, 3, true), 0); // empty is prefix
            // prefix: "ab" is a prefix of "abc"
            assert_eq!(tsCompareString(a, 2, a, 3, true), 0);
            // different lengths, equal common prefix, non-prefix mode
            assert!(tsCompareString(a, 2, a, 3, false) < 0);
        }
    }
}

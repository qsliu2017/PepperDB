//! tsearch/ts_type.h - Definitions for the tsvector and tsquery types.
//!
//! Canonical home of the tsvector/tsquery on-disk types. Faithful 1:1 port of
//! PostgreSQL 18.3 src/include/tsearch/ts_type.h.
//!
//! NOTE: minimal/duplicate copies of QueryItemType, QueryOperand, QueryOperator,
//! QueryItem, TSQuery/TSQueryData, and the COMPUTESIZE/TSQUERY_TOO_BIG/GETQUERY/
//! GETOPERAND/HDRSIZETQ macros also currently live in
//! src/utils/adt/tsquery_util.rs (module-local stubs, flagged there with a
//! TODO(pg-port) to dedup against this file once it exists). The main agent
//! should dedup: this module is intended to be the canonical source.

use crate::prelude::*;

// int8/int16/int32/uint16/uint32, Datum, Pointer, FLEXIBLE_ARRAY_MEMBER,
// VARHDRSZ, SHORTALIGN, PointerGetDatum, DatumGetPointer come in via the
// prelude (crate::c::*, crate::postgres::*).
use crate::utils::memutils::MaxAllocSize;

/*
 * TSVector type.
 *
 * Structure of tsvector datatype:
 * 1) standard varlena header
 * 2) int32  size - number of lexemes (WordEntry array entries)
 * 3) Array of WordEntry - one per lexeme; must be sorted according to
 *    tsCompareString() (ie, memcmp of lexeme strings). WordEntry->pos gives the
 *    number of bytes from end of WordEntry array to start of lexeme's string,
 *    which is of length len.
 * 4) Per-lexeme data storage:
 *    lexeme string (not null-terminated)
 *    if haspos is true:
 *      padding byte if necessary to make the position data 2-byte aligned
 *      uint16          number of positions that follow
 *      WordEntryPos[]  positions
 *
 * The positions for each lexeme must be sorted.
 *
 * Note, tsvectorsend/recv believe that sizeof(WordEntry) == 4
 */

/*
 * typedef struct {
 *   uint32  haspos:1, len:11, pos:20;
 * } WordEntry;
 *
 * One backing uint32 with accessor methods for the C bitfields.
 * C bitfield layout (little-endian): haspos = bit 0, len = bits 1..11 (11 bits),
 * pos = bits 12..31 (20 bits).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WordEntry {
    pub bits: uint32,
}

const WORDENTRY_HASPOS_SHIFT: u32 = 0;
const WORDENTRY_LEN_SHIFT: u32 = 1;
const WORDENTRY_POS_SHIFT: u32 = 12;
const WORDENTRY_HASPOS_MASK: uint32 = 0x1;
const WORDENTRY_LEN_MASK: uint32 = (1 << 11) - 1; /* 11 bits */
const WORDENTRY_POS_MASK: uint32 = (1 << 20) - 1; /* 20 bits */

impl WordEntry {
    #[inline]
    pub fn haspos(&self) -> uint32 {
        (self.bits >> WORDENTRY_HASPOS_SHIFT) & WORDENTRY_HASPOS_MASK
    }
    #[inline]
    pub fn set_haspos(&mut self, v: uint32) {
        self.bits = (self.bits & !(WORDENTRY_HASPOS_MASK << WORDENTRY_HASPOS_SHIFT))
            | ((v & WORDENTRY_HASPOS_MASK) << WORDENTRY_HASPOS_SHIFT);
    }
    #[inline]
    pub fn len(&self) -> uint32 {
        (self.bits >> WORDENTRY_LEN_SHIFT) & WORDENTRY_LEN_MASK
    }
    #[inline]
    pub fn set_len(&mut self, v: uint32) {
        self.bits = (self.bits & !(WORDENTRY_LEN_MASK << WORDENTRY_LEN_SHIFT))
            | ((v & WORDENTRY_LEN_MASK) << WORDENTRY_LEN_SHIFT);
    }
    #[inline]
    pub fn pos(&self) -> uint32 {
        (self.bits >> WORDENTRY_POS_SHIFT) & WORDENTRY_POS_MASK
    }
    #[inline]
    pub fn set_pos(&mut self, v: uint32) {
        self.bits = (self.bits & !(WORDENTRY_POS_MASK << WORDENTRY_POS_SHIFT))
            | ((v & WORDENTRY_POS_MASK) << WORDENTRY_POS_SHIFT);
    }
}

pub const MAXSTRLEN: uint32 = (1 << 11) - 1;
pub const MAXSTRPOS: uint32 = (1 << 20) - 1;

/* extern int compareWordEntryPos(const void *a, const void *b); */
pub unsafe fn compareWordEntryPos(a: *const c_void, b: *const c_void) -> c_int {
    let _ = (a, b);
    unimplemented!()
}

/*
 * Equivalent to
 * typedef struct {
 *     uint16 weight:2, pos:14;
 * }
 */
pub type WordEntryPos = uint16;

/*
 * typedef struct {
 *   uint16        npos;
 *   WordEntryPos  pos[FLEXIBLE_ARRAY_MEMBER];
 * } WordEntryPosVector;
 */
#[repr(C)]
pub struct WordEntryPosVector {
    pub npos: uint16,
    pub pos: [WordEntryPos; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * WordEntryPosVector with exactly 1 entry.
 * typedef struct {
 *   uint16        npos;
 *   WordEntryPos  pos[1];
 * } WordEntryPosVector1;
 */
#[repr(C)]
pub struct WordEntryPosVector1 {
    pub npos: uint16,
    pub pos: [WordEntryPos; 1],
}

/* #define WEP_GETWEIGHT(x)  ( (x) >> 14 ) */
#[inline]
pub fn WEP_GETWEIGHT(x: WordEntryPos) -> WordEntryPos {
    x >> 14
}

/* #define WEP_GETPOS(x)  ( (x) & 0x3fff ) */
#[inline]
pub fn WEP_GETPOS(x: WordEntryPos) -> WordEntryPos {
    x & 0x3fff
}

/* #define WEP_SETWEIGHT(x,v)  ( (x) = ( (v) << 14 ) | ( (x) & 0x3fff ) ) */
#[inline]
pub fn WEP_SETWEIGHT(x: &mut WordEntryPos, v: WordEntryPos) {
    *x = (v << 14) | (*x & 0x3fff);
}

/* #define WEP_SETPOS(x,v)  ( (x) = ( (x) & 0xc000 ) | ( (v) & 0x3fff ) ) */
#[inline]
pub fn WEP_SETPOS(x: &mut WordEntryPos, v: WordEntryPos) {
    *x = (*x & 0xc000) | (v & 0x3fff);
}

pub const MAXENTRYPOS: c_int = 1 << 14;
pub const MAXNUMPOS: c_int = 256;

/* #define LIMITPOS(x) ( ( (x) >= MAXENTRYPOS ) ? (MAXENTRYPOS-1) : (x) ) */
#[inline]
pub fn LIMITPOS(x: c_int) -> c_int {
    if x >= MAXENTRYPOS {
        MAXENTRYPOS - 1
    } else {
        x
    }
}

/* This struct represents a complete tsvector datum */
/*
 * typedef struct {
 *   int32      vl_len_;
 *   int32      size;
 *   WordEntry  entries[FLEXIBLE_ARRAY_MEMBER];
 * } TSVectorData;
 */
#[repr(C)]
pub struct TSVectorData {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub size: int32,
    pub entries: [WordEntry; FLEXIBLE_ARRAY_MEMBER],
    /* lexemes follow the entries[] array */
}

pub type TSVector = *mut TSVectorData;

/* #define DATAHDRSIZE (offsetof(TSVectorData, entries)) */
#[inline]
pub fn DATAHDRSIZE() -> usize {
    core::mem::offset_of!(TSVectorData, entries)
}

/* #define CALCDATASIZE(nentries, lenstr) (DATAHDRSIZE + (nentries)*sizeof(WordEntry) + (lenstr)) */
#[inline]
pub fn CALCDATASIZE(nentries: c_int, lenstr: c_int) -> usize {
    DATAHDRSIZE() + (nentries as usize) * core::mem::size_of::<WordEntry>() + (lenstr as usize)
}

/* pointer to start of a tsvector's WordEntry array. #define ARRPTR(x) ((x)->entries) */
#[inline]
pub unsafe fn ARRPTR(x: TSVector) -> *mut WordEntry {
    (*x).entries.as_mut_ptr()
}

/* pointer to start of a tsvector's lexeme storage.
 * #define STRPTR(x)  ( (char *) &(x)->entries[(x)->size] ) */
#[inline]
pub unsafe fn STRPTR(x: TSVector) -> *mut c_char {
    (*x).entries.as_mut_ptr().add((*x).size as usize) as *mut c_char
}

/* #define _POSVECPTR(x, e) ((WordEntryPosVector *)(STRPTR(x) + SHORTALIGN((e)->pos + (e)->len))) */
#[inline]
pub unsafe fn _POSVECPTR(x: TSVector, e: *const WordEntry) -> *mut WordEntryPosVector {
    STRPTR(x).add(SHORTALIGN(((*e).pos() + (*e).len()) as usize)) as *mut WordEntryPosVector
}

/* #define POSDATALEN(x,e) ( ( (e)->haspos ) ? (_POSVECPTR(x,e)->npos) : 0 ) */
#[inline]
pub unsafe fn POSDATALEN(x: TSVector, e: *const WordEntry) -> uint16 {
    if (*e).haspos() != 0 {
        (*_POSVECPTR(x, e)).npos
    } else {
        0
    }
}

/* #define POSDATAPTR(x,e) (_POSVECPTR(x,e)->pos) */
#[inline]
pub unsafe fn POSDATAPTR(x: TSVector, e: *const WordEntry) -> *mut WordEntryPos {
    (*_POSVECPTR(x, e)).pos.as_mut_ptr()
}

/*
 * fmgr interface functions
 */

/* static inline TSVector DatumGetTSVector(Datum X) */
#[inline]
pub unsafe fn DatumGetTSVector(X: Datum) -> TSVector {
    crate::PG_DETOAST_DATUM!(X) as TSVector
}

/* static inline TSVector DatumGetTSVectorCopy(Datum X) */
#[inline]
pub unsafe fn DatumGetTSVectorCopy(X: Datum) -> TSVector {
    crate::PG_DETOAST_DATUM_COPY!(X) as TSVector
}

/* static inline Datum TSVectorGetDatum(const TSVectorData *X) */
#[inline]
pub unsafe fn TSVectorGetDatum(X: *const TSVectorData) -> Datum {
    PointerGetDatum(X as *const c_void)
}

/* #define PG_GETARG_TSVECTOR(n)  DatumGetTSVector(PG_GETARG_DATUM(n)) */
#[macro_export]
macro_rules! PG_GETARG_TSVECTOR {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::adt::ts_type::DatumGetTSVector($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}

/* #define PG_GETARG_TSVECTOR_COPY(n)  DatumGetTSVectorCopy(PG_GETARG_DATUM(n)) */
#[macro_export]
macro_rules! PG_GETARG_TSVECTOR_COPY {
    ($fcinfo:expr, $n:expr) => {
        $crate::utils::adt::ts_type::DatumGetTSVectorCopy($crate::PG_GETARG_DATUM!($fcinfo, $n))
    };
}

/* #define PG_RETURN_TSVECTOR(x)  return TSVectorGetDatum(x) */
#[macro_export]
macro_rules! PG_RETURN_TSVECTOR {
    ($x:expr) => {
        return $crate::utils::adt::ts_type::TSVectorGetDatum($x)
    };
}

/*
 * TSQuery
 */

/* typedef int8 QueryItemType; */
pub type QueryItemType = int8;

/* Valid values for QueryItemType: */
pub const QI_VAL: int8 = 1;
pub const QI_OPR: int8 = 2;
/*
 * This is only used in an intermediate stack representation in parse_tsquery.
 * It's not a legal type elsewhere.
 */
pub const QI_VALSTOP: int8 = 3;

/*
 * QueryItem is one node in tsquery - operator or operand.
 *
 * typedef struct {
 *   QueryItemType type;
 *   uint8         weight;
 *   bool          prefix;
 *   int32         valcrc;
 *   uint32        length:12, distance:20;
 * } QueryOperand;
 *
 * The trailing length:12, distance:20 bitfields share one backing uint32
 * (lendist): length = low 12 bits, distance = high 20 bits.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QueryOperand {
    pub r#type: QueryItemType, /* operand or kind of operator (ts_tokentype) */
    pub weight: uint8, /* weights of operand to search; bitmask. 0 = any. A:1<<3 B:1<<2 C:1<<1 D:1<<0 */
    pub prefix: bool,  /* true if it's a prefix search */
    pub valcrc: int32, /* XXX: pg_crc32 would be more appropriate */
    /* pointer to text value of operand, must correlate with WordEntry */
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
    pub fn set_length(&mut self, v: uint32) {
        self.lendist = (self.lendist & !QOPERAND_LENGTH_MASK) | (v & QOPERAND_LENGTH_MASK);
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

/* extern PGDLLIMPORT const int tsearch_op_priority[OP_COUNT]; */
#[allow(improper_ctypes)]
extern "C" {
    pub static tsearch_op_priority: [c_int; OP_COUNT];
}

/* get operation priority by its code. #define OP_PRIORITY(x) ( tsearch_op_priority[(x)-1] ) */
#[inline]
pub unsafe fn OP_PRIORITY(x: int8) -> c_int {
    tsearch_op_priority[(x as usize) - 1]
}

/* get QueryOperator priority.
 * #define QO_PRIORITY(x)  OP_PRIORITY(((QueryOperator *)(x))->oper) */
#[inline]
pub unsafe fn QO_PRIORITY(x: *const QueryOperator) -> c_int {
    OP_PRIORITY((*x).oper)
}

/*
 * typedef struct {
 *   QueryItemType type;
 *   int8          oper;
 *   int16         distance;
 *   uint32        left;
 * } QueryOperator;
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QueryOperator {
    pub r#type: QueryItemType,
    pub oper: int8,      /* see above */
    pub distance: int16, /* distance between args for OP_PHRASE */
    pub left: uint32,    /* pointer to left operand. Right operand is item+1,
                          * left operand is placed item+item->left */
}

/*
 * Note: TSQuery is 4-bytes aligned, so make sure there's no fields inside
 * QueryItem requiring 8-byte alignment, like int64.
 *
 * typedef union {
 *   QueryItemType type;
 *   QueryOperator qoperator;
 *   QueryOperand  qoperand;
 * } QueryItem;
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub union QueryItem {
    pub r#type: QueryItemType,
    pub qoperator: QueryOperator,
    pub qoperand: QueryOperand,
}

/*
 * Storage:
 *   (len)(size)(array of QueryItem)(operands as '\0'-terminated c-strings)
 *
 * typedef struct {
 *   int32  vl_len_;
 *   int32  size;
 *   char   data[FLEXIBLE_ARRAY_MEMBER];
 * } TSQueryData;
 */
#[repr(C)]
pub struct TSQueryData {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub size: int32,    /* number of QueryItems */
    pub data: [c_char; FLEXIBLE_ARRAY_MEMBER], /* data starts here */
}

pub type TSQuery = *mut TSQueryData;

/* #define HDRSIZETQ  ( VARHDRSZ + sizeof(int32) ) */
#[inline]
pub fn HDRSIZETQ() -> usize {
    VARHDRSZ as usize + core::mem::size_of::<int32>()
}

/*
 * Computes the size of header and all QueryItems. size is the number of
 * QueryItems, and lenofoperand is the total length of all operands.
 * #define COMPUTESIZE(size, lenofoperand)
 *   ( HDRSIZETQ + (size)*sizeof(QueryItem) + (lenofoperand) )
 */
#[inline]
pub fn COMPUTESIZE(size: c_int, lenofoperand: c_int) -> usize {
    HDRSIZETQ() + (size as usize) * core::mem::size_of::<QueryItem>() + (lenofoperand as usize)
}

/*
 * #define TSQUERY_TOO_BIG(size, lenofoperand)
 *   ((size) > (MaxAllocSize - HDRSIZETQ - (lenofoperand)) / sizeof(QueryItem))
 */
#[inline]
pub fn TSQUERY_TOO_BIG(size: c_int, lenofoperand: c_int) -> bool {
    (size as Size)
        > (MaxAllocSize - HDRSIZETQ() as Size - lenofoperand as Size)
            / core::mem::size_of::<QueryItem>() as Size
}

/* Returns a pointer to the first QueryItem in a TSQuery.
 * #define GETQUERY(x)  ((QueryItem*)( (char*)(x)+HDRSIZETQ )) */
#[inline]
pub unsafe fn GETQUERY(x: TSQuery) -> *mut QueryItem {
    (x as *mut c_char).add(HDRSIZETQ()) as *mut QueryItem
}

/* Returns a pointer to the beginning of operands in a TSQuery.
 * #define GETOPERAND(x)  ( (char*)GETQUERY(x) + ((TSQuery)(x))->size * sizeof(QueryItem) ) */
#[inline]
pub unsafe fn GETOPERAND(x: TSQuery) -> *mut c_char {
    (GETQUERY(x) as *mut c_char).add((*x).size as usize * core::mem::size_of::<QueryItem>())
}

/*
 * fmgr interface functions
 * Note, TSQuery type marked as plain storage, so it can't be toasted but
 * PG_DETOAST_DATUM_COPY is used for simplicity.
 */

/* static inline TSQuery DatumGetTSQuery(Datum X) */
#[inline]
pub unsafe fn DatumGetTSQuery(X: Datum) -> TSQuery {
    DatumGetPointer(X) as TSQuery
}

/* static inline TSQuery DatumGetTSQueryCopy(Datum X) */
#[inline]
pub unsafe fn DatumGetTSQueryCopy(X: Datum) -> TSQuery {
    crate::PG_DETOAST_DATUM_COPY!(X) as TSQuery
}

/* static inline Datum TSQueryGetDatum(const TSQueryData *X) */
#[inline]
pub unsafe fn TSQueryGetDatum(X: *const TSQueryData) -> Datum {
    PointerGetDatum(X as *const c_void)
}

// PG_GETARG_TSQUERY / PG_GETARG_TSQUERY_COPY / PG_RETURN_TSQUERY are already
// #[macro_export]ed (crate-root) by the canonical tsquery module; not redefined
// here to avoid duplicate-macro-name errors. Use crate::PG_GETARG_TSQUERY! etc.

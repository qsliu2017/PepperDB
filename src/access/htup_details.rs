//! Translation of postgres/src/include/access/htup.h
//!                + postgres/src/include/access/htup_details.h  (merged)
//!
//! POSTGRES heap tuple header definitions: the in-memory/on-disk heap-tuple
//! header structs (HeapTupleHeaderData / MinimalTupleData / HeapTupleData), the
//! t_infomask / t_infomask2 flag constants documenting the on-disk format, and
//! the large family of bit-twiddling accessor "macros" (rendered as inline fns).
//!
//! Byte-level correctness matters here: the executor and the on-disk format both
//! depend on the exact layout/flag values.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` / dependency mapping:
//!   storage/itemptr.h     -> crate::storage::itemptr (ItemPointerData + accessors)
//!   access/transam.h      -> crate::access::transam (FrozenTransactionId,
//!                            InvalidTransactionId, TransactionIdEquals, ...)
//!   access/tupdesc.h      -> crate::access::common::tupdesc (TupleDesc,
//!                            TupleDescCompactAttr, CompactAttribute, ...)
//!   access/tupmacs.h      -> crate::access::tupmacs (att_isnull/fetch_att/...)
//!   storage/bufpage.h     -> STUB: SizeOfPageHeaderData/ItemIdData not yet ported;
//!                            MaxHeapTupleSize/MaxHeapTuplesPerPage use the known
//!                            upstream constant values (see comments).
//!   varatt.h              -> crate::varatt (VARSIZE / SET_VARSIZE)
//!   catalog/pg_attribute.h-> crate::catalog::pg_attribute (Form_pg_attribute)
//!   storage/block.h       -> crate::storage::block (BlockNumber)
//!   storage/off.h         -> crate::storage::off (OffsetNumber, MaxOffsetNumber)
//!   postgres.h / c.h      -> crate::prelude
//!
//! WHAT IS REAL vs STUBBED:
//!   REAL: all struct defs + the layout consts (SizeofHeapTupleHeader,
//!     MINIMAL_TUPLE_OFFSET/PADDING/DATA_OFFSET, HEAPTUPLESIZE), every t_infomask /
//!     t_infomask2 flag + derived mask, all the raw header accessors (Get/SetRawXmin,
//!     Get/SetRawXmax, GetRawCommandId/SetCmin/SetCmax/Get/SetXvac, Datum length/
//!     type/typmod, Natts, the xmin-committed/invalid/frozen helpers, HotUpdated/
//!     HeapOnly/Match, speculative-token + moved-partitions helpers, GETSTRUCT,
//!     the HeapTuple* nulls/varwidth/external helpers, HEAP_XMAX_IS_LOCKED_ONLY /
//!     HEAP_LOCKED_UPGRADED / the lock-test helpers, BITMAPLEN),
//!     HeapTupleHeaderGetXmin (uses XminFrozen), fastgetattr/heap_getattr
//!     (the cacheoff fast path + the att_isnull/null path; the slow walk delegates
//!     to nocachegetattr).
//!   STUBBED (one fn/branch each, signatures real):
//!     HeapTupleHeaderGetCmin/GetCmax/AdjustCmax        (utils/time/combocid.c)
//!     HeapTupleGetUpdateXid (the IS_MULTI branch of GetUpdateXid) (heapam.c/multixact.c)
//!     nocachegetattr, heap_getsysattr, getmissingattr   (common/heaptuple.c)
//!     + the rest of the common/heaptuple.c extern prototypes.
//!
//! MISSING crate symbols this module STUBS or works around:
//!   - crate::storage::itemptr lacks ItemPointerSetMovedPartitions /
//!     ItemPointerIndicatesMovedPartitions (only the *OffsetNumber / *BlockNumber
//!     consts exist); implemented inline here from those consts + ItemPointerSet /
//!     the NoCheck accessors (TODO(pg-port): move to itemptr.rs).
//!   - storage/bufpage.h (SizeOfPageHeaderData, ItemIdData) -> not ported; the two
//!     page-capacity consts use the upstream literal sizes.
//!   - common/heaptuple.c (nocachegetattr / heap_getsysattr / getmissingattr /
//!     heap_form_tuple / ...) -> not ported; declared as local unimplemented stubs.
//!   - utils/time/combocid.c, access/heapam.c (HeapTupleGetUpdateXid) -> stubbed.

use crate::prelude::*;

use crate::access::common::tupdesc::{
    CompactAttribute, TupleDesc, TupleDescCompactAttr,
};
use crate::access::transam::{FrozenTransactionId, InvalidTransactionId};
use crate::access::tupmacs::{att_isnull, fetch_att};
use crate::c::{bits8, int16, int32, uint8, uint16, uint32, CommandId, TransactionId};
use crate::pg_config::{BLCKSZ, MAXIMUM_ALIGNOF};
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::{
    ItemPointerData, ItemPointerGetBlockNumber, ItemPointerGetBlockNumberNoCheck,
    ItemPointerGetOffsetNumberNoCheck, ItemPointerSet, MovedPartitionsBlockNumber,
    MovedPartitionsOffsetNumber, SpecTokenOffsetNumber,
};
use crate::storage::off::MaxOffsetNumber;
use crate::varatt::{SET_VARSIZE, VARSIZE};

// c_char / c_int / c_void come in via crate::prelude::* (re-exported from core::ffi).
use core::mem::{offset_of, size_of};

// ============================================================================
//   htup_details.h: attribute-number limits
// ============================================================================

/*
 * MaxTupleAttributeNumber limits the number of (user) columns in a tuple.
 * The fixed overhead + null bitmap + MAXALIGN must fit into t_hoff (uint8).
 */
pub const MaxTupleAttributeNumber: c_int = 1664; /* 8 * 208 */

/*
 * MaxHeapAttributeNumber limits the number of (user) columns in a table.
 * Must be at least one less than MaxTupleAttributeNumber (UPDATE adds CTID).
 */
pub const MaxHeapAttributeNumber: c_int = 1600; /* 8 * 200 */

// ============================================================================
//   htup.h: HeapTupleData (in-memory pointer-to-tuple)
// ============================================================================

/*
 * HeapTupleData is an in-memory data structure that points to a tuple.  See the
 * extensive discussion in htup.h: t_data may point into a disk buffer, be NULL,
 * be part of a palloc'd chunk (t_data at offset HEAPTUPLESIZE), or point
 * MINIMAL_TUPLE_OFFSET bytes before a MinimalTuple.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HeapTupleData {
    /* length of *t_data */
    pub t_len: uint32,
    /* SelfItemPointer */
    pub t_self: ItemPointerData,
    /* table the tuple came from */
    pub t_tableOid: Oid,
    /* -> tuple header and data */
    pub t_data: HeapTupleHeader,
}

/* FIELDNO_HEAPTUPLEDATA_DATA 3 (t_data is the 4th field). */
pub const FIELDNO_HEAPTUPLEDATA_DATA: usize = 3;

pub type HeapTuple = *mut HeapTupleData;

/* #define HEAPTUPLESIZE MAXALIGN(sizeof(HeapTupleData)) */
pub const HEAPTUPLESIZE: usize = MAXALIGN(size_of::<HeapTupleData>());

/*
 * HeapTupleIsValid(tuple) - PointerIsValid(tuple)
 *
 * # Safety
 * `tuple` is null or references valid HeapTupleData.
 */
#[inline]
pub fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    PointerIsValid(tuple)
}

// ============================================================================
//   htup_details.h: the heap tuple header structs
// ============================================================================

/*
 * The three physical xmin/xmax/field3 fields shared with the Datum form.  t_field3
 * overlays the inserting/deleting CommandId (t_cid) with the old-style VACUUM FULL
 * xact id (t_xvac); both are uint32 so the union is 4 bytes.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub union HeapTupleFields_t_field3 {
    /* inserting or deleting command ID, or both */
    pub t_cid: CommandId,
    /* old-style VACUUM FULL xact ID */
    pub t_xvac: TransactionId,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct HeapTupleFields {
    /* inserting xact ID */
    pub t_xmin: TransactionId,
    /* deleting or locking xact ID */
    pub t_xmax: TransactionId,
    pub t_field3: HeapTupleFields_t_field3,
}

/*
 * DatumTupleFields - the Datum (composite/row value) overlay of the same first 12
 * bytes: a varlena length word, a typmod, and a composite type OID.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DatumTupleFields {
    /* varlena header (do not touch directly!) */
    pub datum_len_: int32,
    /* -1, or identifier of a record type */
    pub datum_typmod: int32,
    /* composite type OID, or RECORDOID */
    pub datum_typeid: Oid,
}

/*
 * t_choice: the heap-tuple xact fields vs. the Datum fields, overlaid.  Both
 * variants are 12 bytes (3 x uint32 / int32 / Oid).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub union HeapTupleHeaderData_t_choice {
    pub t_heap: HeapTupleFields,
    pub t_datum: DatumTupleFields,
}

/*
 * Heap tuple header.  Fields from t_infomask2 onward must match MinimalTupleData.
 * The fixed header is 23 bytes; the NULLs bitmap (t_bits) follows.
 */
#[repr(C)]
pub struct HeapTupleHeaderData {
    pub t_choice: HeapTupleHeaderData_t_choice,

    /*
     * current TID of this or newer tuple (or a speculative insertion token)
     */
    pub t_ctid: ItemPointerData,

    /* Fields below here must match MinimalTupleData! */

    /* number of attributes + various flags */
    pub t_infomask2: uint16,
    /* various flag bits, see below */
    pub t_infomask: uint16,
    /* sizeof header incl. bitmap, padding */
    pub t_hoff: uint8,

    /* ^ - 23 bytes - ^ */

    /* bitmap of NULLs -- MORE DATA FOLLOWS AT END OF STRUCT */
    pub t_bits: [bits8; FLEXIBLE_ARRAY_MEMBER],
}

pub type HeapTupleHeader = *mut HeapTupleHeaderData;

/* FIELDNO_HEAPTUPLEHEADERDATA_* (per the #defines interspersed in the C struct). */
pub const FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2: usize = 2;
pub const FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK: usize = 3;
pub const FIELDNO_HEAPTUPLEHEADERDATA_HOFF: usize = 4;
pub const FIELDNO_HEAPTUPLEHEADERDATA_BITS: usize = 5;

/* #define SizeofHeapTupleHeader offsetof(HeapTupleHeaderData, t_bits) */
pub const SizeofHeapTupleHeader: usize = offset_of!(HeapTupleHeaderData, t_bits);

// ----------------------------------------------------------------------------
//   information stored in t_infomask:
// ----------------------------------------------------------------------------

/* has null attribute(s) */
pub const HEAP_HASNULL: uint16 = 0x0001;
/* has variable-width attribute(s) */
pub const HEAP_HASVARWIDTH: uint16 = 0x0002;
/* has external stored attribute(s) */
pub const HEAP_HASEXTERNAL: uint16 = 0x0004;
/* has an object-id field */
pub const HEAP_HASOID_OLD: uint16 = 0x0008;
/* xmax is a key-shared locker */
pub const HEAP_XMAX_KEYSHR_LOCK: uint16 = 0x0010;
/* t_cid is a combo CID */
pub const HEAP_COMBOCID: uint16 = 0x0020;
/* xmax is exclusive locker */
pub const HEAP_XMAX_EXCL_LOCK: uint16 = 0x0040;
/* xmax, if valid, is only a locker */
pub const HEAP_XMAX_LOCK_ONLY: uint16 = 0x0080;

/* xmax is a shared locker */
pub const HEAP_XMAX_SHR_LOCK: uint16 = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;

pub const HEAP_LOCK_MASK: uint16 =
    HEAP_XMAX_SHR_LOCK | HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;

/* t_xmin committed */
pub const HEAP_XMIN_COMMITTED: uint16 = 0x0100;
/* t_xmin invalid/aborted */
pub const HEAP_XMIN_INVALID: uint16 = 0x0200;
pub const HEAP_XMIN_FROZEN: uint16 = HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID;
/* t_xmax committed */
pub const HEAP_XMAX_COMMITTED: uint16 = 0x0400;
/* t_xmax invalid/aborted */
pub const HEAP_XMAX_INVALID: uint16 = 0x0800;
/* t_xmax is a MultiXactId */
pub const HEAP_XMAX_IS_MULTI: uint16 = 0x1000;
/* this is UPDATEd version of row */
pub const HEAP_UPDATED: uint16 = 0x2000;
/* moved to another place by pre-9.0 VACUUM FULL; kept for binary upgrade support */
pub const HEAP_MOVED_OFF: uint16 = 0x4000;
/* moved from another place by pre-9.0 VACUUM FULL; kept for binary upgrade support */
pub const HEAP_MOVED_IN: uint16 = 0x8000;
pub const HEAP_MOVED: uint16 = HEAP_MOVED_OFF | HEAP_MOVED_IN;

/* visibility-related bits */
pub const HEAP_XACT_MASK: uint16 = 0xFFF0;

/*
 * A tuple is only locked (i.e. not updated by its Xmax) if the
 * HEAP_XMAX_LOCK_ONLY bit is set; or, for pg_upgrade's sake, if the Xmax is not a
 * multi and the EXCL_LOCK bit is set.
 */
#[inline]
pub fn HEAP_XMAX_IS_LOCKED_ONLY(infomask: uint16) -> bool {
    (infomask & HEAP_XMAX_LOCK_ONLY) != 0
        || (infomask & (HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK)) == HEAP_XMAX_EXCL_LOCK
}

/*
 * HEAP_LOCKED_UPGRADED - a tuple that has HEAP_XMAX_IS_MULTI and
 * HEAP_XMAX_LOCK_ONLY but neither EXCL_LOCK nor KEYSHR_LOCK must come from a tuple
 * share-locked in 9.2 or earlier and then pg_upgrade'd; such lockers are gone, so
 * the tuple is effectively not locked.
 */
#[inline]
pub fn HEAP_LOCKED_UPGRADED(infomask: uint16) -> bool {
    (infomask & HEAP_XMAX_IS_MULTI) != 0
        && (infomask & HEAP_XMAX_LOCK_ONLY) != 0
        && (infomask & (HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK)) == 0
}

/*
 * Use these to test whether a particular lock is applied to a tuple.  (The C
 * source declares the argument as int16; the bit values fit in 16 bits either
 * way, so we keep int16 here.)
 */
#[inline]
pub fn HEAP_XMAX_IS_SHR_LOCKED(infomask: int16) -> bool {
    (infomask & HEAP_LOCK_MASK as int16) == HEAP_XMAX_SHR_LOCK as int16
}
#[inline]
pub fn HEAP_XMAX_IS_EXCL_LOCKED(infomask: int16) -> bool {
    (infomask & HEAP_LOCK_MASK as int16) == HEAP_XMAX_EXCL_LOCK as int16
}
#[inline]
pub fn HEAP_XMAX_IS_KEYSHR_LOCKED(infomask: int16) -> bool {
    (infomask & HEAP_LOCK_MASK as int16) == HEAP_XMAX_KEYSHR_LOCK as int16
}

/* turn these all off when Xmax is to change */
pub const HEAP_XMAX_BITS: uint16 = HEAP_XMAX_COMMITTED
    | HEAP_XMAX_INVALID
    | HEAP_XMAX_IS_MULTI
    | HEAP_LOCK_MASK
    | HEAP_XMAX_LOCK_ONLY;

// ----------------------------------------------------------------------------
//   information stored in t_infomask2:
// ----------------------------------------------------------------------------

/* 11 bits for number of attributes */
pub const HEAP_NATTS_MASK: uint16 = 0x07FF;
/* bits 0x1800 are available */
/* tuple was updated and key cols modified, or tuple deleted */
pub const HEAP_KEYS_UPDATED: uint16 = 0x2000;
/* tuple was HOT-updated */
pub const HEAP_HOT_UPDATED: uint16 = 0x4000;
/* this is heap-only tuple */
pub const HEAP_ONLY_TUPLE: uint16 = 0x8000;

/* visibility-related bits */
pub const HEAP2_XACT_MASK: uint16 = 0xE000;

/*
 * HEAP_TUPLE_HAS_MATCH is a temporary flag used during hash joins, overlaid on a
 * visibility flag (HEAP_ONLY_TUPLE) in hash-table tuples.
 */
pub const HEAP_TUPLE_HAS_MATCH: uint16 = HEAP_ONLY_TUPLE; /* tuple has a join match */

// ============================================================================
//   HeapTupleHeader accessor functions
// ============================================================================

/*
 * HeapTupleHeaderGetRawXmin returns the "raw" xmin field - the xid originally used
 * to insert the tuple (the tuple might actually be frozen; see GetXmin).
 *
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetRawXmin(tup: *const HeapTupleHeaderData) -> TransactionId {
    (*tup).t_choice.t_heap.t_xmin
}

/*
 * HeapTupleHeaderGetXmin - the effective xmin (FrozenTransactionId if the tuple is
 * frozen via the XMIN_FROZEN infomask bits).
 *
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetXmin(tup: *const HeapTupleHeaderData) -> TransactionId {
    if HeapTupleHeaderXminFrozen(tup) {
        FrozenTransactionId
    } else {
        HeapTupleHeaderGetRawXmin(tup)
    }
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetXmin(tup: *mut HeapTupleHeaderData, xid: TransactionId) {
    (*tup).t_choice.t_heap.t_xmin = xid;
}

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderXminCommitted(tup: *const HeapTupleHeaderData) -> bool {
    ((*tup).t_infomask & HEAP_XMIN_COMMITTED) != 0
}

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderXminInvalid(tup: *const HeapTupleHeaderData) -> bool {
    ((*tup).t_infomask & (HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID)) == HEAP_XMIN_INVALID
}

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderXminFrozen(tup: *const HeapTupleHeaderData) -> bool {
    ((*tup).t_infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetXminCommitted(tup: *mut HeapTupleHeaderData) {
    Assert!(!HeapTupleHeaderXminInvalid(tup));
    (*tup).t_infomask |= HEAP_XMIN_COMMITTED;
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetXminInvalid(tup: *mut HeapTupleHeaderData) {
    Assert!(!HeapTupleHeaderXminCommitted(tup));
    (*tup).t_infomask |= HEAP_XMIN_INVALID;
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetXminFrozen(tup: *mut HeapTupleHeaderData) {
    Assert!(!HeapTupleHeaderXminInvalid(tup));
    (*tup).t_infomask |= HEAP_XMIN_FROZEN;
}

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetRawXmax(tup: *const HeapTupleHeaderData) -> TransactionId {
    (*tup).t_choice.t_heap.t_xmax
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetXmax(tup: *mut HeapTupleHeaderData, xid: TransactionId) {
    (*tup).t_choice.t_heap.t_xmax = xid;
}

/*
 * HeapTupleHeaderGetUpdateXid - the Xid that updated a tuple, resolving the
 * MultiXactId when the relevant bits are set (#ifndef FRONTEND in C).
 *
 * STUB: the IS_MULTI branch calls HeapTupleGetUpdateXid (heapam.c -> multixact.c),
 * not yet ported.  The common non-multi branch (just the raw Xmax) is real.
 *
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetUpdateXid(tup: *const HeapTupleHeaderData) -> TransactionId {
    if ((*tup).t_infomask & HEAP_XMAX_INVALID) == 0
        && ((*tup).t_infomask & HEAP_XMAX_IS_MULTI) != 0
        && ((*tup).t_infomask & HEAP_XMAX_LOCK_ONLY) == 0
    {
        HeapTupleGetUpdateXid(tup)
    } else {
        HeapTupleHeaderGetRawXmax(tup)
    }
}

/*
 * HeapTupleHeaderGetRawCommandId - what's in the t_cid header whether useful or
 * not.  Most code should use GetCmin/GetCmax instead.
 *
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetRawCommandId(tup: *const HeapTupleHeaderData) -> CommandId {
    (*tup).t_choice.t_heap.t_field3.t_cid
}

/* SetCmin is reasonably simple since we never need a combo CID.
 *
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetCmin(tup: *mut HeapTupleHeaderData, cid: CommandId) {
    Assert!(((*tup).t_infomask & HEAP_MOVED) == 0);
    (*tup).t_choice.t_heap.t_field3.t_cid = cid;
    (*tup).t_infomask &= !HEAP_COMBOCID;
}

/* SetCmax must be used after HeapTupleHeaderAdjustCmax; see combocid.c.
 *
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetCmax(tup: *mut HeapTupleHeaderData, cid: CommandId, iscombo: bool) {
    Assert!(((*tup).t_infomask & HEAP_MOVED) == 0);
    (*tup).t_choice.t_heap.t_field3.t_cid = cid;
    if iscombo {
        (*tup).t_infomask |= HEAP_COMBOCID;
    } else {
        (*tup).t_infomask &= !HEAP_COMBOCID;
    }
}

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetXvac(tup: *const HeapTupleHeaderData) -> TransactionId {
    if ((*tup).t_infomask & HEAP_MOVED) != 0 {
        (*tup).t_choice.t_heap.t_field3.t_xvac
    } else {
        InvalidTransactionId
    }
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetXvac(tup: *mut HeapTupleHeaderData, xid: TransactionId) {
    Assert!(((*tup).t_infomask & HEAP_MOVED) != 0);
    (*tup).t_choice.t_heap.t_field3.t_xvac = xid;
}

/* StaticAssertDecl(MaxOffsetNumber < SpecTokenOffsetNumber, ...) */
const _: () = assert!(MaxOffsetNumber < SpecTokenOffsetNumber, "invalid speculative token constant");

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderIsSpeculative(tup: *const HeapTupleHeaderData) -> bool {
    ItemPointerGetOffsetNumberNoCheck(&(*tup).t_ctid) == SpecTokenOffsetNumber
}

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData (and is speculative).
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetSpeculativeToken(tup: *const HeapTupleHeaderData) -> BlockNumber {
    Assert!(HeapTupleHeaderIsSpeculative(tup));
    ItemPointerGetBlockNumber(&(*tup).t_ctid)
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetSpeculativeToken(tup: *mut HeapTupleHeaderData, token: BlockNumber) {
    ItemPointerSet(&mut (*tup).t_ctid, token, SpecTokenOffsetNumber);
}

/*
 * HeapTupleHeaderIndicatesMovedPartitions / SetMovedPartitions.
 *
 * NOTE: storage/itemptr.rs only exports the MovedPartitions* consts, not the
 * ItemPointerIndicatesMovedPartitions / ItemPointerSetMovedPartitions inlines, so
 * they are expanded here from the consts.  TODO(pg-port): add those two helpers to
 * itemptr.rs and call them instead.
 *
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderIndicatesMovedPartitions(tup: *const HeapTupleHeaderData) -> bool {
    /* ItemPointerIndicatesMovedPartitions(&tup->t_ctid) */
    ItemPointerGetOffsetNumberNoCheck(&(*tup).t_ctid) == MovedPartitionsOffsetNumber
        && ItemPointerGetBlockNumberNoCheck(&(*tup).t_ctid) == MovedPartitionsBlockNumber
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetMovedPartitions(tup: *mut HeapTupleHeaderData) {
    /* ItemPointerSetMovedPartitions(&tup->t_ctid) */
    ItemPointerSet(
        &mut (*tup).t_ctid,
        MovedPartitionsBlockNumber,
        MovedPartitionsOffsetNumber,
    );
}

/*
 * Datum (composite-value) header accessors.  These overlay the xact fields.
 *
 * # Safety
 * `tup` references a valid HeapTupleHeaderData laid out as a Datum.
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetDatumLength(tup: *const HeapTupleHeaderData) -> uint32 {
    VARSIZE(tup as *const c_char)
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetDatumLength(tup: *mut HeapTupleHeaderData, len: uint32) {
    SET_VARSIZE(tup as *mut c_char, len as int32);
}

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData laid out as a Datum.
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetTypeId(tup: *const HeapTupleHeaderData) -> Oid {
    (*tup).t_choice.t_datum.datum_typeid
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetTypeId(tup: *mut HeapTupleHeaderData, datum_typeid: Oid) {
    (*tup).t_choice.t_datum.datum_typeid = datum_typeid;
}

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData laid out as a Datum.
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetTypMod(tup: *const HeapTupleHeaderData) -> int32 {
    (*tup).t_choice.t_datum.datum_typmod
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetTypMod(tup: *mut HeapTupleHeaderData, typmod: int32) {
    (*tup).t_choice.t_datum.datum_typmod = typmod;
}

/*
 * We stop considering a tuple HOT-updated as soon as it is known aborted or the
 * would-be updating transaction is known aborted.
 *
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderIsHotUpdated(tup: *const HeapTupleHeaderData) -> bool {
    ((*tup).t_infomask2 & HEAP_HOT_UPDATED) != 0
        && ((*tup).t_infomask & HEAP_XMAX_INVALID) == 0
        && !HeapTupleHeaderXminInvalid(tup)
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetHotUpdated(tup: *mut HeapTupleHeaderData) {
    (*tup).t_infomask2 |= HEAP_HOT_UPDATED;
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderClearHotUpdated(tup: *mut HeapTupleHeaderData) {
    (*tup).t_infomask2 &= !HEAP_HOT_UPDATED;
}

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderIsHeapOnly(tup: *const HeapTupleHeaderData) -> bool {
    ((*tup).t_infomask2 & HEAP_ONLY_TUPLE) != 0
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetHeapOnly(tup: *mut HeapTupleHeaderData) {
    (*tup).t_infomask2 |= HEAP_ONLY_TUPLE;
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderClearHeapOnly(tup: *mut HeapTupleHeaderData) {
    (*tup).t_infomask2 &= !HEAP_ONLY_TUPLE;
}

/*
 * These are used with both HeapTuple and MinimalTuple, so they are macros in C.
 * Rendered as inline fns over a raw header pointer (works for both, since the
 * t_infomask2 field is at the matching offset in both structs).
 *
 * # Safety
 * `tup` references a valid header (HeapTupleHeaderData or MinimalTupleData).
 */
#[inline]
pub unsafe fn HeapTupleHeaderGetNatts(tup: *const HeapTupleHeaderData) -> uint16 {
    (*tup).t_infomask2 & HEAP_NATTS_MASK
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetNatts(tup: *mut HeapTupleHeaderData, natts: uint16) {
    (*tup).t_infomask2 = ((*tup).t_infomask2 & !HEAP_NATTS_MASK) | natts;
}

/*
 * # Safety
 * `tup` references a valid HeapTupleHeaderData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderHasExternal(tup: *const HeapTupleHeaderData) -> bool {
    ((*tup).t_infomask & HEAP_HASEXTERNAL) != 0
}

/*
 * BITMAPLEN(NATTS) - size in bytes of the null bitmap for NATTS data columns.
 */
#[inline]
pub fn BITMAPLEN(NATTS: c_int) -> c_int {
    (NATTS + 7) / 8
}

// ----------------------------------------------------------------------------
//   Tuple-size / page-capacity limits
// ----------------------------------------------------------------------------

/*
 * MaxHeapTupleSize / MaxHeapTuplesPerPage / MinHeapTupleSize.
 *
 * STUB: storage/bufpage.h (SizeOfPageHeaderData) and storage/itemid.h (ItemIdData)
 * are not yet ported.  We inline the well-known upstream sizes:
 *   sizeof(ItemIdData)       == 4   (a line pointer is a packed 32-bit bitfield)
 *   SizeOfPageHeaderData     == 24  (offsetof(PageHeaderData, pd_linp) on 64-bit)
 * TODO(pg-port): replace with the real consts once bufpage.h / itemid.h land.
 */
const SIZEOF_ITEMIDDATA: usize = 4;
const SIZEOF_PAGEHEADERDATA: usize = 24;

/* #define MaxHeapTupleSize (BLCKSZ - MAXALIGN(SizeOfPageHeaderData + sizeof(ItemIdData))) */
pub const MaxHeapTupleSize: usize =
    BLCKSZ - MAXALIGN(SIZEOF_PAGEHEADERDATA + SIZEOF_ITEMIDDATA);
/* #define MinHeapTupleSize MAXALIGN(SizeofHeapTupleHeader) */
pub const MinHeapTupleSize: usize = MAXALIGN(SizeofHeapTupleHeader);

/*
 * #define MaxHeapTuplesPerPage \
 *   ((int) ((BLCKSZ - SizeOfPageHeaderData) /
 *           (MAXALIGN(SizeofHeapTupleHeader) + sizeof(ItemIdData))))
 */
pub const MaxHeapTuplesPerPage: c_int = ((BLCKSZ - SIZEOF_PAGEHEADERDATA)
    / (MAXALIGN(SizeofHeapTupleHeader) + SIZEOF_ITEMIDDATA)) as c_int;

/*
 * MaxAttrSize - arbitrary upper limit on declared sizes of char(n) and similar.
 */
pub const MaxAttrSize: usize = 10 * 1024 * 1024;

// ============================================================================
//   MinimalTupleData
// ============================================================================

/*
 * MINIMAL_TUPLE_OFFSET / PADDING / DATA_OFFSET, exactly per the C #defines.
 *
 *   MINIMAL_TUPLE_OFFSET =
 *     (offsetof(HeapTupleHeaderData, t_infomask2) - sizeof(uint32))
 *         / MAXIMUM_ALIGNOF * MAXIMUM_ALIGNOF
 *   MINIMAL_TUPLE_PADDING =
 *     (offsetof(HeapTupleHeaderData, t_infomask2) - sizeof(uint32)) % MAXIMUM_ALIGNOF
 */
pub const MINIMAL_TUPLE_OFFSET: usize =
    (offset_of!(HeapTupleHeaderData, t_infomask2) - size_of::<uint32>()) / MAXIMUM_ALIGNOF
        * MAXIMUM_ALIGNOF;
pub const MINIMAL_TUPLE_PADDING: usize =
    (offset_of!(HeapTupleHeaderData, t_infomask2) - size_of::<uint32>()) % MAXIMUM_ALIGNOF;

/*
 * MinimalTupleData: a length word, padding, and the fields matching
 * HeapTupleHeaderData from t_infomask2 onward.  Used for transient executor
 * tuples (no xact status / t_ctid).
 */
#[repr(C)]
pub struct MinimalTupleData {
    /* actual length of minimal tuple */
    pub t_len: uint32,

    pub mt_padding: [c_char; MINIMAL_TUPLE_PADDING],

    /* Fields below here must match HeapTupleHeaderData! */

    /* number of attributes + various flags */
    pub t_infomask2: uint16,
    /* various flag bits, see below */
    pub t_infomask: uint16,
    /* sizeof header incl. bitmap, padding */
    pub t_hoff: uint8,

    /* ^ - 23 bytes - ^ */

    /* bitmap of NULLs -- MORE DATA FOLLOWS AT END OF STRUCT */
    pub t_bits: [bits8; FLEXIBLE_ARRAY_MEMBER],
}

pub type MinimalTuple = *mut MinimalTupleData;

/* #define MINIMAL_TUPLE_DATA_OFFSET offsetof(MinimalTupleData, t_infomask2) */
pub const MINIMAL_TUPLE_DATA_OFFSET: usize = offset_of!(MinimalTupleData, t_infomask2);

/* #define SizeofMinimalTupleHeader offsetof(MinimalTupleData, t_bits) */
pub const SizeofMinimalTupleHeader: usize = offset_of!(MinimalTupleData, t_bits);

// ----------------------------------------------------------------------------
//   MinimalTuple accessor functions (hash-join match bit)
// ----------------------------------------------------------------------------

/*
 * # Safety
 * `tup` references a valid MinimalTupleData.
 */
#[inline]
pub unsafe fn HeapTupleHeaderHasMatch(tup: *const MinimalTupleData) -> bool {
    ((*tup).t_infomask2 & HEAP_TUPLE_HAS_MATCH) != 0
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderSetMatch(tup: *mut MinimalTupleData) {
    (*tup).t_infomask2 |= HEAP_TUPLE_HAS_MATCH;
}

/*
 * # Safety
 * `tup` is writable.
 */
#[inline]
pub unsafe fn HeapTupleHeaderClearMatch(tup: *mut MinimalTupleData) {
    (*tup).t_infomask2 &= !HEAP_TUPLE_HAS_MATCH;
}

// ============================================================================
//   Accessor functions to be used with HeapTuple pointers
// ============================================================================

/*
 * GETSTRUCT - given a HeapTuple pointer, return the address of the user data
 * (just past the header, at t_hoff).
 *
 * # Safety
 * `tuple` references a valid HeapTupleData with t_data set.
 */
#[inline]
pub unsafe fn GETSTRUCT(tuple: *const HeapTupleData) -> *mut c_void {
    ((*tuple).t_data as *mut c_char).add((*(*tuple).t_data).t_hoff as usize) as *mut c_void
}

/*
 * # Safety
 * `tuple` references a valid HeapTupleData with t_data set.
 */
#[inline]
pub unsafe fn HeapTupleHasNulls(tuple: *const HeapTupleData) -> bool {
    ((*(*tuple).t_data).t_infomask & HEAP_HASNULL) != 0
}

/*
 * # Safety
 * See [`HeapTupleHasNulls`].
 */
#[inline]
pub unsafe fn HeapTupleNoNulls(tuple: *const HeapTupleData) -> bool {
    !HeapTupleHasNulls(tuple)
}

/*
 * # Safety
 * See [`HeapTupleHasNulls`].
 */
#[inline]
pub unsafe fn HeapTupleHasVarWidth(tuple: *const HeapTupleData) -> bool {
    ((*(*tuple).t_data).t_infomask & HEAP_HASVARWIDTH) != 0
}

/*
 * # Safety
 * See [`HeapTupleHasNulls`].
 */
#[inline]
pub unsafe fn HeapTupleAllFixed(tuple: *const HeapTupleData) -> bool {
    !HeapTupleHasVarWidth(tuple)
}

/*
 * # Safety
 * See [`HeapTupleHasNulls`].
 */
#[inline]
pub unsafe fn HeapTupleHasExternal(tuple: *const HeapTupleData) -> bool {
    ((*(*tuple).t_data).t_infomask & HEAP_HASEXTERNAL) != 0
}

/*
 * # Safety
 * See [`HeapTupleHasNulls`].
 */
#[inline]
pub unsafe fn HeapTupleIsHotUpdated(tuple: *const HeapTupleData) -> bool {
    HeapTupleHeaderIsHotUpdated((*tuple).t_data)
}

/*
 * # Safety
 * `tuple` references a valid HeapTupleData with writable t_data.
 */
#[inline]
pub unsafe fn HeapTupleSetHotUpdated(tuple: *const HeapTupleData) {
    HeapTupleHeaderSetHotUpdated((*tuple).t_data);
}

/*
 * # Safety
 * `tuple` references a valid HeapTupleData with writable t_data.
 */
#[inline]
pub unsafe fn HeapTupleClearHotUpdated(tuple: *const HeapTupleData) {
    HeapTupleHeaderClearHotUpdated((*tuple).t_data);
}

/*
 * # Safety
 * See [`HeapTupleHasNulls`].
 */
#[inline]
pub unsafe fn HeapTupleIsHeapOnly(tuple: *const HeapTupleData) -> bool {
    HeapTupleHeaderIsHeapOnly((*tuple).t_data)
}

/*
 * # Safety
 * `tuple` references a valid HeapTupleData with writable t_data.
 */
#[inline]
pub unsafe fn HeapTupleSetHeapOnly(tuple: *const HeapTupleData) {
    HeapTupleHeaderSetHeapOnly((*tuple).t_data);
}

/*
 * # Safety
 * `tuple` references a valid HeapTupleData with writable t_data.
 */
#[inline]
pub unsafe fn HeapTupleClearHeapOnly(tuple: *const HeapTupleData) {
    HeapTupleHeaderClearHeapOnly((*tuple).t_data);
}

// ============================================================================
//   STUBS: prototypes for functions in common/heaptuple.c
// ============================================================================
//
// These are declared `extern` in htup_details.h and implemented in
// src/backend/access/common/heaptuple.c, which is not yet ported.  They are
// surfaced here as local unimplemented stubs so the real accessors below
// (fastgetattr/heap_getattr) can reference them with correct signatures.
// TODO(pg-port): translate common/heaptuple.c.

/*
 * nocachegetattr - the "slow" attribute walk used by fastgetattr when there is no
 * cached offset.  STUB.
 *
 * # Safety
 * Stub: never returns.
 */
pub unsafe fn nocachegetattr(
    tup: HeapTuple,
    attnum: c_int,
    tupleDesc: TupleDesc,
) -> Datum {
    // Real implementation lives in common/heaptuple.c (now ported); delegate.
    crate::access::common::heaptuple::nocachegetattr(tup, attnum, tupleDesc)
}

/*
 * heap_getsysattr - fetch a system column (ctid/xmin/xmax/cmin/cmax/tableoid).
 * STUB (calls combocid.c / GetCurrentCommandId for cmin/cmax).
 *
 * # Safety
 * Stub: never returns.
 */
pub unsafe fn heap_getsysattr(
    tup: HeapTuple,
    attnum: c_int,
    tupleDesc: TupleDesc,
    isnull: *mut bool,
) -> Datum {
    // Real implementation lives in common/heaptuple.c (now ported); delegate.
    crate::access::common::heaptuple::heap_getsysattr(tup, attnum, tupleDesc, isnull)
}

/*
 * getmissingattr - the value for an attribute beyond the tuple's natts (added by a
 * later ALTER TABLE ... ADD COLUMN with a default).  STUB.
 *
 * # Safety
 * Stub: never returns.
 */
pub unsafe fn getmissingattr(
    tupleDesc: TupleDesc,
    attnum: c_int,
    isnull: *mut bool,
) -> Datum {
    // Real implementation lives in common/heaptuple.c (now ported); delegate.
    crate::access::common::heaptuple::getmissingattr(tupleDesc, attnum, isnull)
}

/*
 * HeapTupleGetUpdateXid (access/heapam.c) - resolve a MultiXact Xmax to the actual
 * updating Xid.  STUB (needs multixact.c).
 *
 * # Safety
 * Stub: never returns.
 */
pub unsafe fn HeapTupleGetUpdateXid(_tup: *const HeapTupleHeaderData) -> TransactionId {
    // TODO(pg-port): access/heapam.c HeapTupleGetUpdateXid -> MultiXactIdGetUpdateXid.
    unimplemented!("HeapTupleGetUpdateXid: access/heapam.c (multixact) not yet translated")
}

// ----------------------------------------------------------------------------
//   HeapTupleHeader functions implemented in utils/time/combocid.c (htup.h)
// ----------------------------------------------------------------------------

/*
 * HeapTupleHeaderGetCmin - resolve t_cid to the real insert command id (resolving
 * a combo CID).  STUB (combocid.c).
 *
 * # Safety
 * Stub: never returns.
 */
pub unsafe fn HeapTupleHeaderGetCmin(_tup: *const HeapTupleHeaderData) -> CommandId {
    // TODO(pg-port): utils/time/combocid.c HeapTupleHeaderGetCmin.
    unimplemented!("HeapTupleHeaderGetCmin: utils/time/combocid.c not yet translated")
}

/*
 * HeapTupleHeaderGetCmax - resolve t_cid to the real delete command id.  STUB.
 *
 * # Safety
 * Stub: never returns.
 */
pub unsafe fn HeapTupleHeaderGetCmax(_tup: *const HeapTupleHeaderData) -> CommandId {
    // TODO(pg-port): utils/time/combocid.c HeapTupleHeaderGetCmax.
    unimplemented!("HeapTupleHeaderGetCmax: utils/time/combocid.c not yet translated")
}

/*
 * HeapTupleHeaderAdjustCmax - compute the cmax (and whether it is a combo CID) to
 * store for a delete/update.  STUB.
 *
 * # Safety
 * Stub: never returns.
 */
pub unsafe fn HeapTupleHeaderAdjustCmax(
    _tup: *const HeapTupleHeaderData,
    _cmax: *mut CommandId,
    _iscombo: *mut bool,
) {
    // TODO(pg-port): utils/time/combocid.c HeapTupleHeaderAdjustCmax.
    unimplemented!("HeapTupleHeaderAdjustCmax: utils/time/combocid.c not yet translated")
}

// ============================================================================
//   fastgetattr / heap_getattr  (#ifndef FRONTEND in C)
// ============================================================================

/*
 * fetchatt(att, T) macro: fetch_att(T, att->attbyval, att->attlen) for a
 * CompactAttribute.  (htup_details uses `fetchatt` which tupmacs.h #defines.)
 *
 * # Safety
 * `att` is a live CompactAttribute; `T` points to a properly-aligned field of at
 * least attlen readable bytes.
 */
#[inline]
unsafe fn fetchatt(att: *const CompactAttribute, T: *const c_char) -> Datum {
    fetch_att(T as *const c_void, (*att).attbyval, (*att).attlen as c_int)
}

/*
 * fastgetattr - fetch a user attribute's value as a Datum (a value, or a pointer
 * into the tuple data area).  Must NOT be used for system attributes; attnum MUST
 * be valid.  This is the deform workhorse: the cached-offset / no-nulls fast path
 * is real here, and the slow path delegates to nocachegetattr (STUB).
 *
 * # Safety
 * `tup` is a valid HeapTuple, `tupleDesc` matches it, `attnum` in 1..=natts,
 * `isnull` is writable.
 */
#[inline]
pub unsafe fn fastgetattr(
    tup: HeapTuple,
    attnum: c_int,
    tupleDesc: TupleDesc,
    isnull: *mut bool,
) -> Datum {
    Assert!(attnum > 0);

    *isnull = false;
    if HeapTupleNoNulls(tup) {
        let att: *mut CompactAttribute = TupleDescCompactAttr(tupleDesc, attnum - 1);
        if (*att).attcacheoff >= 0 {
            fetchatt(
                att,
                ((*tup).t_data as *mut c_char)
                    .add((*(*tup).t_data).t_hoff as usize)
                    .add((*att).attcacheoff as usize),
            )
        } else {
            nocachegetattr(tup, attnum, tupleDesc)
        }
    } else if att_isnull(attnum - 1, (*(*tup).t_data).t_bits.as_ptr()) {
        *isnull = true;
        Datum::default() /* (Datum) NULL */
    } else {
        nocachegetattr(tup, attnum, tupleDesc)
    }
}

/*
 * heap_getattr - extract an attribute of a heap tuple as a Datum, for either a
 * system or a user attribute; attnum is range-checked.  A NULL field yields a zero
 * Datum and *isnull == true.
 *
 * # Safety
 * `tup` is a valid HeapTuple, `tupleDesc` matches it, `isnull` is writable.
 */
#[inline]
pub unsafe fn heap_getattr(
    tup: HeapTuple,
    attnum: c_int,
    tupleDesc: TupleDesc,
    isnull: *mut bool,
) -> Datum {
    if attnum > 0 {
        if attnum > HeapTupleHeaderGetNatts((*tup).t_data) as c_int {
            getmissingattr(tupleDesc, attnum, isnull)
        } else {
            fastgetattr(tup, attnum, tupleDesc, isnull)
        }
    } else {
        heap_getsysattr(tup, attnum, tupleDesc, isnull)
    }
}

// ============================================================================
//   Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_layout() {
        // t_choice is 12 bytes (max(HeapTupleFields=12, DatumTupleFields=12)).
        assert_eq!(size_of::<HeapTupleFields>(), 12);
        assert_eq!(size_of::<DatumTupleFields>(), 12);
        assert_eq!(size_of::<HeapTupleHeaderData_t_choice>(), 12);

        // 12 (t_choice) + 6 (t_ctid ItemPointerData) = 18.
        assert_eq!(offset_of!(HeapTupleHeaderData, t_infomask2), 18);
        // + 2 (infomask2) + 2 (infomask) = 22 for t_hoff.
        assert_eq!(offset_of!(HeapTupleHeaderData, t_hoff), 22);
        // + 1 (t_hoff) = 23: the "23 byte" fixed header.
        assert_eq!(offset_of!(HeapTupleHeaderData, t_bits), 23);
        assert_eq!(SizeofHeapTupleHeader, 23);

        // FIELDNO sanity.
        assert_eq!(FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2, 2);

        // MinimalTuple: t_infomask2 offsets must match HeapTupleHeaderData modulo
        // MAXIMUM_ALIGNOF, and the data offset is the same 23-byte-minus-offset shape.
        assert_eq!(
            offset_of!(MinimalTupleData, t_infomask2) % MAXIMUM_ALIGNOF,
            offset_of!(HeapTupleHeaderData, t_infomask2) % MAXIMUM_ALIGNOF
        );
        assert_eq!(SizeofMinimalTupleHeader, MINIMAL_TUPLE_DATA_OFFSET + 5);
        // offsetof(t_infomask2) == 18, sizeof(uint32) == 4 -> (18-4)=14;
        // 14 / 8 * 8 = 8 ; 14 % 8 = 6.
        assert_eq!(MINIMAL_TUPLE_OFFSET, 8);
        assert_eq!(MINIMAL_TUPLE_PADDING, 6);
    }

    #[test]
    fn flag_values() {
        assert_eq!(HEAP_HASNULL, 0x0001);
        assert_eq!(HEAP_HASVARWIDTH, 0x0002);
        assert_eq!(HEAP_HASEXTERNAL, 0x0004);
        assert_eq!(HEAP_XMAX_SHR_LOCK, 0x0050);
        assert_eq!(HEAP_LOCK_MASK, 0x0050);
        assert_eq!(HEAP_XMIN_FROZEN, 0x0300);
        assert_eq!(HEAP_MOVED, 0xC000);
        assert_eq!(HEAP_XACT_MASK, 0xFFF0);
        assert_eq!(HEAP_NATTS_MASK, 0x07FF);
        assert_eq!(HEAP2_XACT_MASK, 0xE000);
        assert_eq!(HEAP_TUPLE_HAS_MATCH, HEAP_ONLY_TUPLE);
        assert_eq!(
            HEAP_XMAX_BITS,
            HEAP_XMAX_COMMITTED
                | HEAP_XMAX_INVALID
                | HEAP_XMAX_IS_MULTI
                | HEAP_LOCK_MASK
                | HEAP_XMAX_LOCK_ONLY
        );
        // BITMAPLEN: 1..8 cols -> 1 byte, 9 cols -> 2 bytes.
        assert_eq!(BITMAPLEN(1), 1);
        assert_eq!(BITMAPLEN(8), 1);
        assert_eq!(BITMAPLEN(9), 2);
    }

    #[test]
    fn lock_helpers() {
        // exclusive-only: EXCL_LOCK set, not multi -> locked-only.
        assert!(HEAP_XMAX_IS_LOCKED_ONLY(HEAP_XMAX_EXCL_LOCK));
        // explicit lock-only bit always counts.
        assert!(HEAP_XMAX_IS_LOCKED_ONLY(HEAP_XMAX_LOCK_ONLY));
        // a plain committed xmax (no lock bits) is not locked-only.
        assert!(!HEAP_XMAX_IS_LOCKED_ONLY(HEAP_XMAX_COMMITTED));

        assert!(HEAP_XMAX_IS_EXCL_LOCKED(HEAP_XMAX_EXCL_LOCK as int16));
        assert!(HEAP_XMAX_IS_KEYSHR_LOCKED(HEAP_XMAX_KEYSHR_LOCK as int16));
        assert!(HEAP_XMAX_IS_SHR_LOCKED(HEAP_XMAX_SHR_LOCK as int16));

        // pg_upgrade'd share lock: multi + lock-only, neither excl nor keyshr.
        assert!(HEAP_LOCKED_UPGRADED(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY));
        assert!(!HEAP_LOCKED_UPGRADED(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK));
    }

    #[test]
    fn accessor_roundtrips() {
        unsafe {
            // Hand-build a header on the stack (no trailing t_bits needed for these).
            let mut hdr: HeapTupleHeaderData = core::mem::zeroed();
            let p: *mut HeapTupleHeaderData = &mut hdr;

            // Natts: only the low 11 bits, preserving the high flag bits.
            (*p).t_infomask2 = HEAP_HOT_UPDATED; // a high bit that must survive
            HeapTupleHeaderSetNatts(p, 3);
            assert_eq!(HeapTupleHeaderGetNatts(p), 3);
            assert_ne!((*p).t_infomask2 & HEAP_HOT_UPDATED, 0);
            HeapTupleHeaderSetNatts(p, HEAP_NATTS_MASK); // max
            assert_eq!(HeapTupleHeaderGetNatts(p), HEAP_NATTS_MASK);

            // Xmin raw round-trip.
            HeapTupleHeaderSetXmin(p, 12345);
            assert_eq!(HeapTupleHeaderGetRawXmin(p), 12345);
            // Not frozen -> GetXmin == raw.
            assert_eq!(HeapTupleHeaderGetXmin(p), 12345);

            // Xmax raw round-trip.
            HeapTupleHeaderSetXmax(p, 67890);
            assert_eq!(HeapTupleHeaderGetRawXmax(p), 67890);

            // Datum length round-trip via the varlena header (overlays datum_len_).
            HeapTupleHeaderSetDatumLength(p, 0x1234);
            assert_eq!(HeapTupleHeaderGetDatumLength(p), 0x1234);

            // Datum typeid / typmod round-trip.
            HeapTupleHeaderSetTypeId(p, 0xABCD);
            assert_eq!(HeapTupleHeaderGetTypeId(p), 0xABCD);
            HeapTupleHeaderSetTypMod(p, -7);
            assert_eq!(HeapTupleHeaderGetTypMod(p), -7);

            // infomask flag set/clear.
            (*p).t_infomask = 0;
            HeapTupleHeaderSetHeapOnly(p);
            assert!(HeapTupleHeaderIsHeapOnly(p));
            HeapTupleHeaderClearHeapOnly(p);
            assert!(!HeapTupleHeaderIsHeapOnly(p));

            HeapTupleHeaderSetHotUpdated(p);
            // Need xmax-valid + xmin-not-invalid for IsHotUpdated; set those.
            (*p).t_infomask &= !HEAP_XMAX_INVALID;
            (*p).t_infomask &= !HEAP_XMIN_INVALID;
            assert!(HeapTupleHeaderIsHotUpdated(p));
            HeapTupleHeaderClearHotUpdated(p);
            assert!(!HeapTupleHeaderIsHotUpdated(p));

            // Xmin frozen path: set the frozen bits, GetXmin -> FrozenTransactionId.
            (*p).t_infomask = HEAP_XMIN_FROZEN;
            assert!(HeapTupleHeaderXminFrozen(p));
            assert_eq!(HeapTupleHeaderGetXmin(p), FrozenTransactionId);
        }
    }

    #[test]
    fn xvac_and_xmin_status() {
        unsafe {
            let mut hdr: HeapTupleHeaderData = core::mem::zeroed();
            let p: *mut HeapTupleHeaderData = &mut hdr;

            // Without HEAP_MOVED, GetXvac returns InvalidTransactionId.
            (*p).t_infomask = 0;
            assert_eq!(HeapTupleHeaderGetXvac(p), InvalidTransactionId);

            // With HEAP_MOVED set, Set/GetXvac round-trips through t_field3.
            (*p).t_infomask = HEAP_MOVED;
            HeapTupleHeaderSetXvac(p, 4242);
            assert_eq!(HeapTupleHeaderGetXvac(p), 4242);

            // xmin committed/invalid status helpers.
            (*p).t_infomask = HEAP_XMIN_COMMITTED;
            assert!(HeapTupleHeaderXminCommitted(p));
            assert!(!HeapTupleHeaderXminInvalid(p));

            (*p).t_infomask = HEAP_XMIN_INVALID;
            assert!(HeapTupleHeaderXminInvalid(p));
            assert!(!HeapTupleHeaderXminCommitted(p));
        }
    }

    #[test]
    fn speculative_and_moved_partitions() {
        unsafe {
            let mut hdr: HeapTupleHeaderData = core::mem::zeroed();
            let p: *mut HeapTupleHeaderData = &mut hdr;

            // Speculative token round-trip (token stored in the t_ctid block id).
            let token: BlockNumber = 0x00AB_CDEF;
            HeapTupleHeaderSetSpeculativeToken(p, token);
            assert!(HeapTupleHeaderIsSpeculative(p));
            assert_eq!(HeapTupleHeaderGetSpeculativeToken(p), token);

            // Moved-partitions marker.
            HeapTupleHeaderSetMovedPartitions(p);
            assert!(HeapTupleHeaderIndicatesMovedPartitions(p));
            // Once moved-partitions, it is not a speculative token.
            assert!(!HeapTupleHeaderIsSpeculative(p));
        }
    }

    #[test]
    fn sizes() {
        // HEAPTUPLESIZE is MAXALIGN of the in-memory HeapTupleData.
        assert_eq!(HEAPTUPLESIZE, MAXALIGN(size_of::<HeapTupleData>()));
        // MinHeapTupleSize is the maxaligned fixed header (23 -> 24).
        assert_eq!(MinHeapTupleSize, 24);
        assert_eq!(MaxAttrSize, 10 * 1024 * 1024);
        // Page-capacity sanity: a tuple plus its line pointer fits on a page.
        assert!(MaxHeapTupleSize < BLCKSZ);
        assert!(MaxHeapTuplesPerPage > 0);
    }
}

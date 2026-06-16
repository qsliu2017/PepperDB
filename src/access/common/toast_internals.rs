//! Translation of postgres/src/backend/access/common/toast_internals.c
//!
//! Functions for internal use by the TOAST system.
//!
//! Copyright (c) 2000-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/access/common/toast_internals.c
//!
//! `#include`s mapped:
//!   common/pg_lzcompress.h -> crate::common::pg_lzcompress (pglz_compress + PGLZ_MAX_OUTPUT)
//!   varatt.h               -> crate::varatt (VAR* macro layer; varattrib_4b va_compressed)
//!   access/toast_compression.h -> compression-method ids/chars defined module-locally below
//!     (pglz_compress_datum is inlined here from toast_compression.c; lz4_compress_datum is a STUB)
//!
//! TRANSLATED (self-contained, real): the compression path used by tuptoaster -
//!   toast_compress_datum, plus the inlined pglz_compress_datum helper and the
//!   TOAST_COMPRESS_* / toast_compress_header accessors.
//!
//! TRANSLATED (full 1:1 bodies, calling LOCAL TODO(pg-port) stubs for the
//!   heap / relcache / genam / catalog / snapmgr deps that are not yet ported
//!   with a compatible Relation type):
//!   toast_save_datum, toast_delete_datum, toastrel_valueid_exists,
//!   toastid_valueid_exists, toast_get_valid_index, toast_open_indexes,
//!   toast_close_indexes, get_toast_snapshot.  Their signatures use opaque
//!   local type aliases (Relation/LOCKMODE/Snapshot) so this file stays
//!   self-consistent until the access/heapam/genam/relcache/snapmgr modules
//!   expose a unified Relation/Snapshot.
//!
//! lz4 is not ported (USE_LZ4 off in this build): lz4_compress_datum ereports like
//! the upstream NO_LZ4_SUPPORT() path.
//!
//! libc memcpy is bound via extern "C" (used in the stubbed on-disk insert body
//! comments only; not actually reached from the compiled path).

use crate::prelude::*;
use crate::varatt::*;

use crate::common::pg_lzcompress::{pglz_compress, PGLZ_MAX_OUTPUT};

use core::ffi::{c_char, c_int, c_void};

use crate::c::Min;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::nodes::pg_list::{lfirst_oid, list_free, list_length, List, ListCell};
use crate::{current_cell, foreach};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ----------------------------------------------------------------------------
//   access/toast_compression.h (the bits this file needs)
// ----------------------------------------------------------------------------

/* ToastCompressionId - see toast_compression.h */
pub const TOAST_PGLZ_COMPRESSION_ID: u32 = 0;
pub const TOAST_LZ4_COMPRESSION_ID: u32 = 1;
pub const TOAST_INVALID_COMPRESSION_ID: u32 = 2;

/* Char codes stored in pg_attribute.attcompression / chosen at compress time. */
pub const TOAST_PGLZ_COMPRESSION: c_char = b'p' as c_char;
pub const TOAST_LZ4_COMPRESSION: c_char = b'l' as c_char;
pub const InvalidCompressionMethod: c_char = b'\0' as c_char;

#[inline]
pub fn CompressionMethodIsValid(cm: c_char) -> bool {
    cm != InvalidCompressionMethod
}

/*
 * default_toast_compression is an integer for purposes of the GUC machinery,
 * but the value is one of the char codes above (default: pglz).
 * Defined in toast_compression.c; mirrored here as the compiled default.
 */
pub static mut default_toast_compression: c_char = TOAST_PGLZ_COMPRESSION;

// ----------------------------------------------------------------------------
//   access/toast_internals.h - compressed toast header accessors
// ----------------------------------------------------------------------------

/*
 *	The information at the start of the compressed toast data.
 */
#[repr(C)]
pub struct toast_compress_header {
    /// varlena header (do not touch directly!)
    pub vl_len_: int32,
    /// 2 bits for compression method and 30 bits external size; see va_extinfo
    pub tcinfo: uint32,
}

/*
 * Utilities for manipulation of header information for compressed toast
 * entries.  These mirror the VARDATA_COMPRESSED_GET_* macros in varatt.rs but
 * operate via the toast_compress_header overlay (the C macros in
 * toast_internals.h).  VARLENA_EXTSIZE_BITS=30 / VARLENA_EXTSIZE_MASK are not
 * yet exported from varatt.rs, so define them locally (TODO(pg-port): move to
 * varatt.rs alongside the va_compressed accessors).
 */
const VARLENA_EXTSIZE_BITS: u32 = 30;
const VARLENA_EXTSIZE_MASK: u32 = (1u32 << VARLENA_EXTSIZE_BITS) - 1;

/* TOAST_COMPRESS_EXTSIZE(ptr) */
#[inline]
pub unsafe fn TOAST_COMPRESS_EXTSIZE(ptr: *const c_void) -> uint32 {
    (*(ptr as *const toast_compress_header)).tcinfo & VARLENA_EXTSIZE_MASK
}

/* TOAST_COMPRESS_METHOD(ptr) */
#[inline]
pub unsafe fn TOAST_COMPRESS_METHOD(ptr: *const c_void) -> uint32 {
    (*(ptr as *const toast_compress_header)).tcinfo >> VARLENA_EXTSIZE_BITS
}

/* TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(ptr, len, cm_method) */
#[inline]
pub unsafe fn TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(
    ptr: *mut c_void,
    len: int32,
    cm_method: uint32,
) {
    Assert!(len > 0 && (len as u32) <= VARLENA_EXTSIZE_MASK);
    Assert!(cm_method == TOAST_PGLZ_COMPRESSION_ID || cm_method == TOAST_LZ4_COMPRESSION_ID);
    (*(ptr as *mut toast_compress_header)).tcinfo =
        (len as uint32) | (cm_method << VARLENA_EXTSIZE_BITS);
}

/*
 * VARHDRSZ_COMPRESSED = offsetof(varattrib_4b, va_compressed.va_data).
 * va_compressed is { uint32 va_header; uint32 va_tcinfo; char va_data[] },
 * i.e. two 4-byte words before the data == 8.  This equals
 * size_of::<toast_compress_header>().
 */
const VARHDRSZ_COMPRESSED: int32 = core::mem::size_of::<toast_compress_header>() as int32;

// ----------------------------------------------------------------------------
//   Opaque type aliases + local TODO(pg-port) stubs for the heap / relcache /
//   genam / catalog / snapshot deps this file calls.  Kept standalone (opaque
//   `*mut c_void` relations etc.) so this file stays self-consistent while the
//   real access/heapam, access/genam, utils/rel and utils/snapmgr modules use
//   incompatible Relation types.  TODO(pg-port): import the real symbols once
//   those modules expose a unified Relation/Snapshot.
// ----------------------------------------------------------------------------
#[allow(non_camel_case_types)]
pub type Relation = *mut c_void;
#[allow(non_camel_case_types)]
pub type LOCKMODE = c_int;
#[allow(non_camel_case_types)]
pub type Snapshot = *mut c_void;

#[allow(non_camel_case_types)]
type Oid_ = Oid;
#[allow(non_camel_case_types)]
type CommandId = u32;
#[allow(non_camel_case_types)]
type AttrNumber = i16;
#[allow(non_camel_case_types)]
type HeapTuple = *mut c_void;
#[allow(non_camel_case_types)]
type TupleDesc = *mut c_void;
#[allow(non_camel_case_types)]
type SysScanDesc = *mut c_void;
#[allow(non_camel_case_types)]
type ItemPointer = *mut c_void;

/* storage/lockdefs.h lock modes used here */
const NoLock: LOCKMODE = 0;
const AccessShareLock: LOCKMODE = 1;
const RowExclusiveLock: LOCKMODE = 3;

/* access/stratnum.h + utils/fmgroids.h (PG 18.3 values). */
const BTEqualStrategyNumber: c_int = 3;
// TODO(pg-port): replace with the generated utils/fmgroids.h constant.
const F_OIDEQ: Oid = 184;

/* access/sdir.h */
const ForwardScanDirection: c_int = 1;

/* access/genam.h unique-check flags. */
#[allow(non_camel_case_types)]
type IndexUniqueCheck = c_int;
const UNIQUE_CHECK_NO: IndexUniqueCheck = 0;
const UNIQUE_CHECK_YES: IndexUniqueCheck = 1;

/* utils/snapmgr.h: SnapshotAny / the TOAST snapshot. */
const SnapshotAny: Snapshot = core::ptr::null_mut();
// TODO(pg-port): utils/snapmgr.h SnapshotData SnapshotToastData (opaque here).
static mut SnapshotToastData: u8 = 0;

/* heaptoast.h: TOAST_MAX_CHUNK_SIZE (PG 18.3 default page layout). */
const TOAST_MAX_CHUNK_SIZE: int32 = 1996;

/* varatt.h: TOAST_POINTER_SIZE = VARHDRSZ_EXTERNAL + sizeof(varatt_external). */
const TOAST_POINTER_SIZE: int32 =
    VARHDRSZ_EXTERNAL + core::mem::size_of::<varatt_external>() as int32;

/*
 * varatt.h: struct varatt_external -- a traditional out-of-line "TOAST pointer".
 * Stored UNALIGNED inside tuples, so always memcpy into a local before reading.
 */
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct varatt_external {
    pub va_rawsize: int32,
    pub va_extinfo: uint32,
    pub va_valueid: Oid,
    pub va_toastrelid: Oid,
}

/* varatt.h: VARATT_EXTERNAL_IS_COMPRESSED. */
#[inline]
fn VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer: varatt_external) -> bool {
    ((toast_pointer.va_extinfo & VARLENA_EXTSIZE_MASK) as int32)
        < (toast_pointer.va_rawsize - VARHDRSZ)
}

/* varatt.h: VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(toast_pointer, len, cm). */
#[inline]
fn VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(
    toast_pointer: &mut varatt_external,
    len: int32,
    cm_method: uint32,
) {
    Assert!((len as u32) <= VARLENA_EXTSIZE_MASK);
    Assert!(cm_method == TOAST_PGLZ_COMPRESSION_ID || cm_method == TOAST_LZ4_COMPRESSION_ID);
    toast_pointer.va_extinfo = (len as uint32) | (cm_method << VARLENA_EXTSIZE_BITS);
}

/* detoast.h: VARATT_IS_EXTERNAL_ONDISK(PTR). */
#[inline]
unsafe fn VARATT_IS_EXTERNAL_ONDISK(ptr: *const c_char) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_EXTERNAL(ptr) == VARTAG_ONDISK
}

/* varatt.h: VARTAG_EXTERNAL(PTR) == VARTAG_1B_E(PTR). */
#[inline]
unsafe fn VARTAG_EXTERNAL(ptr: *const c_char) -> uint8 {
    VARTAG_1B_E(ptr)
}

/* varatt.h: VARDATA_SHORT(PTR) == VARDATA_1B(PTR). */
#[inline]
unsafe fn VARDATA_SHORT(ptr: *const c_char) -> *mut c_char {
    VARDATA_1B(ptr)
}

/* varatt.h: VARSIZE_SHORT(PTR) == VARSIZE_1B(PTR). */
#[inline]
unsafe fn VARSIZE_SHORT(ptr: *const c_char) -> uint32 {
    VARSIZE_1B(ptr)
}

/*
 * varatt.h: VARDATA_COMPRESSED_GET_EXTSIZE / VARDATA_COMPRESSED_GET_COMPRESS_METHOD
 * on a compressed-in-line Datum (the tcinfo word right after the varlena header).
 */
#[inline]
unsafe fn VARDATA_COMPRESSED_GET_EXTSIZE(ptr: *const c_char) -> uint32 {
    (*(ptr as *const toast_compress_header)).tcinfo & VARLENA_EXTSIZE_MASK
}
#[inline]
unsafe fn VARDATA_COMPRESSED_GET_COMPRESS_METHOD(ptr: *const c_char) -> uint32 {
    (*(ptr as *const toast_compress_header)).tcinfo >> VARLENA_EXTSIZE_BITS
}

/* varatt.h: VARDATA_EXTERNAL(PTR) == VARDATA_1B_E(PTR). */
#[inline]
unsafe fn VARDATA_EXTERNAL(ptr: *const c_char) -> *mut c_char {
    VARDATA_1B_E(ptr)
}

/* varatt.h: SET_VARTAG_EXTERNAL(PTR, tag) == SET_VARTAG_1B_E(PTR, tag). */
#[inline]
unsafe fn SET_VARTAG_EXTERNAL(ptr: *mut c_char, tag: u8) {
    let p = ptr as *mut varattrib_1b_e;
    (*p).va_header = 0x01;
    (*p).va_tag = tag;
}

/*
 * detoast.h: VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr).
 * Copies the possibly-unaligned TOAST-pointer payload of an EXTERNAL datum.
 */
#[inline]
unsafe fn VARATT_EXTERNAL_GET_POINTER(attr: *const crate::c::varlena) -> varatt_external {
    let src = VARDATA_EXTERNAL(attr as *const c_char) as *const c_void;
    let mut out: core::mem::MaybeUninit<varatt_external> = core::mem::MaybeUninit::uninit();
    memcpy(
        out.as_mut_ptr() as *mut c_void,
        src,
        core::mem::size_of::<varatt_external>(),
    );
    out.assume_init()
}

/* ScanKeyData / ScanKeyInit - access/skey.h.  TODO(pg-port): import the real
 * ones once a unified scankey module lands. */
#[repr(C)]
#[allow(non_camel_case_types)]
struct ScanKeyData {
    sk_flags: c_int,
    sk_attno: AttrNumber,
    sk_strategy: c_int,
    sk_subtype: Oid,
    sk_func: Oid,
    sk_argument: Datum,
}

unsafe fn ScanKeyInit(
    entry: *mut ScanKeyData,
    attributeNumber: AttrNumber,
    strategy: c_int,
    procedure: Oid,
    argument: Datum,
) {
    (*entry).sk_flags = 0;
    (*entry).sk_attno = attributeNumber;
    (*entry).sk_strategy = strategy;
    (*entry).sk_subtype = InvalidOid;
    (*entry).sk_func = procedure;
    (*entry).sk_argument = argument;
}

/*
 * The C body reaches into a HeapTuple's t_self (ItemPointer) and a Relation's
 * rd_rel->reltoastrelid / rd_toastoid / rd_att / rd_index->{indisready,
 * indisunique,indisvalid}.  Those struct layouts are not available with the
 * opaque relation alias, so the helpers below front the accesses and remain
 * TODO(pg-port) until utils/rel + access/htup are wired in.
 */
// TODO(pg-port): (*rel).rd_rel->reltoastrelid
unsafe fn rel_reltoastrelid(_rel: Relation) -> Oid {
    unimplemented!("toast_internals: rel->rd_rel->reltoastrelid not yet accessible")
}
// TODO(pg-port): (*rel).rd_toastoid
unsafe fn rel_rd_toastoid(_rel: Relation) -> Oid {
    unimplemented!("toast_internals: rel->rd_toastoid not yet accessible")
}
// TODO(pg-port): (*toastrel).rd_att
unsafe fn rel_rd_att(_toastrel: Relation) -> TupleDesc {
    unimplemented!("toast_internals: toastrel->rd_att not yet accessible")
}
// TODO(pg-port): (*toastidx).rd_index->indisready
unsafe fn idx_indisready(_toastidx: Relation) -> bool {
    unimplemented!("toast_internals: toastidx->rd_index->indisready not yet accessible")
}
// TODO(pg-port): (*toastidx).rd_index->indisunique
unsafe fn idx_indisunique(_toastidx: Relation) -> bool {
    unimplemented!("toast_internals: toastidx->rd_index->indisunique not yet accessible")
}
// TODO(pg-port): (*toastidx).rd_index->indisvalid
unsafe fn idx_indisvalid(_toastidx: Relation) -> bool {
    unimplemented!("toast_internals: toastidx->rd_index->indisvalid not yet accessible")
}
// TODO(pg-port): &(toasttup->t_self)
unsafe fn htup_t_self(_toasttup: HeapTuple) -> ItemPointer {
    unimplemented!("toast_internals: heaptuple->t_self not yet accessible")
}

// ----------------------------------------------------------------------------
//   access/table, access/heapam, access/genam, catalog deps - local stubs.
// ----------------------------------------------------------------------------
// TODO(pg-port): access/table.h table_open / table_close.
unsafe fn table_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!("toast_internals: table_open not yet ported")
}
unsafe fn table_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!("toast_internals: table_close not yet ported")
}
// TODO(pg-port): access/genam.h index_open / index_close.
unsafe fn index_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!("toast_internals: index_open not yet ported")
}
unsafe fn index_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!("toast_internals: index_close not yet ported")
}
// TODO(pg-port): access/genam.h index_insert.
unsafe fn index_insert(
    _indexRelation: Relation,
    _values: *mut Datum,
    _isnull: *mut bool,
    _heap_t_ctid: ItemPointer,
    _heapRelation: Relation,
    _checkUnique: IndexUniqueCheck,
    _indexUnchanged: bool,
    _indexInfo: *mut c_void,
) -> bool {
    unimplemented!("toast_internals: index_insert not yet ported")
}
// TODO(pg-port): access/htup_details.h heap_form_tuple / heap_freetuple.
unsafe fn heap_form_tuple(
    _tupleDescriptor: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple {
    unimplemented!("toast_internals: heap_form_tuple not yet ported")
}
unsafe fn heap_freetuple(_htup: HeapTuple) {
    unimplemented!("toast_internals: heap_freetuple not yet ported")
}
// TODO(pg-port): access/heapam.h heap_insert.
unsafe fn heap_insert(
    _relation: Relation,
    _tup: HeapTuple,
    _cid: CommandId,
    _options: c_int,
    _bistate: *mut c_void,
) {
    unimplemented!("toast_internals: heap_insert not yet ported")
}
// TODO(pg-port): access/heapam.h simple_heap_delete / heap_abort_speculative.
unsafe fn simple_heap_delete(_relation: Relation, _tid: ItemPointer) {
    unimplemented!("toast_internals: simple_heap_delete not yet ported")
}
unsafe fn heap_abort_speculative(_relation: Relation, _tid: ItemPointer) {
    unimplemented!("toast_internals: heap_abort_speculative not yet ported")
}
// TODO(pg-port): access/xact.h GetCurrentCommandId.
unsafe fn GetCurrentCommandId(_used: bool) -> CommandId {
    unimplemented!("toast_internals: GetCurrentCommandId not yet ported")
}
// TODO(pg-port): catalog/catalog.h GetNewOidWithIndex.
unsafe fn GetNewOidWithIndex(_relation: Relation, _indexId: Oid, _oidcolumn: AttrNumber) -> Oid {
    unimplemented!("toast_internals: GetNewOidWithIndex not yet ported")
}
// TODO(pg-port): utils/rel.h RelationGetRelid / RelationGetIndexList.
unsafe fn RelationGetRelid(_relation: Relation) -> Oid {
    unimplemented!("toast_internals: RelationGetRelid not yet ported")
}
unsafe fn RelationGetIndexList(_relation: Relation) -> *mut List {
    unimplemented!("toast_internals: RelationGetIndexList not yet ported")
}
// TODO(pg-port): access/genam.h systable scan helpers.
unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: Snapshot,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc {
    unimplemented!("toast_internals: systable_beginscan not yet ported")
}
unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    unimplemented!("toast_internals: systable_getnext not yet ported")
}
unsafe fn systable_endscan(_sysscan: SysScanDesc) {
    unimplemented!("toast_internals: systable_endscan not yet ported")
}
unsafe fn systable_beginscan_ordered(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _snapshot: Snapshot,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc {
    unimplemented!("toast_internals: systable_beginscan_ordered not yet ported")
}
unsafe fn systable_getnext_ordered(_sysscan: SysScanDesc, _direction: c_int) -> HeapTuple {
    unimplemented!("toast_internals: systable_getnext_ordered not yet ported")
}
unsafe fn systable_endscan_ordered(_sysscan: SysScanDesc) {
    unimplemented!("toast_internals: systable_endscan_ordered not yet ported")
}
// TODO(pg-port): utils/snapmgr.h HaveRegisteredOrActiveSnapshot.
unsafe fn HaveRegisteredOrActiveSnapshot() -> bool {
    unimplemented!("toast_internals: HaveRegisteredOrActiveSnapshot not yet ported")
}

// ----------------------------------------------------------------------------
//   pglz_compress_datum (inlined from access/common/toast_compression.c)
// ----------------------------------------------------------------------------

/*
 * Compress a varlena using PGLZ.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 *
 * # Safety
 * `value` points to a valid in-line (non-external, non-compressed) varlena.
 */
unsafe fn pglz_compress_datum(value: *const crate::c::varlena) -> *mut crate::c::varlena {
    let valsize: int32;
    let len: int32;
    let tmp: *mut crate::c::varlena;

    valsize = VARSIZE_ANY_EXHDR(value as *const c_char) as int32;

    /*
     * No point in wasting a palloc cycle if value size is outside the allowed
     * range for compression.
     *
     * The C code reads PGLZ_strategy_default->min_input_size/max_input_size.
     * pglz_compress (below) re-validates against the strategy and returns -1
     * if out of range, so we let it decide rather than duplicating the bounds
     * here (the strategy struct is not re-exported).
     */

    /*
     * Figure out the maximum possible size of the pglz output, add the bytes
     * that will be needed for varlena overhead, and allocate that amount.
     */
    tmp = palloc((PGLZ_MAX_OUTPUT(valsize) + VARHDRSZ_COMPRESSED) as Size)
        as *mut crate::c::varlena;

    len = pglz_compress(
        VARDATA_ANY(value as *const c_char),
        valsize,
        (tmp as *mut c_char).add(VARHDRSZ_COMPRESSED as usize),
        core::ptr::null(),
    );
    if len < 0 {
        pfree(tmp as *mut c_void);
        return core::ptr::null_mut();
    }

    SET_VARSIZE_COMPRESSED(tmp as *mut c_char, len + VARHDRSZ_COMPRESSED);

    tmp
}

/*
 * Compress a varlena using LZ4.  Not built with USE_LZ4 in this port.
 *
 * # Safety
 * `value` points to a valid varlena.
 */
unsafe fn lz4_compress_datum(_value: *const crate::c::varlena) -> *mut crate::c::varlena {
    // TODO(pg-port): lz4 not ported (USE_LZ4 off). Upstream NO_LZ4_SUPPORT().
    ereport!(
        ERROR,
        errmsg!("compression method lz4 not supported")
    );
    #[allow(unreachable_code)]
    core::ptr::null_mut()
}

// ----------------------------------------------------------------------------
//   toast_compress_datum
// ----------------------------------------------------------------------------

/* ----------
 * toast_compress_datum -
 *
 *	Create a compressed version of a varlena datum
 *
 *	If we fail (ie, compressed result is actually bigger than original)
 *	then return NULL.  We must not use compressed data if it'd expand
 *	the tuple!
 *
 *	We use VAR{SIZE,DATA}_ANY so we can handle short varlenas here without
 *	copying them.  But we can't handle external or compressed datums.
 * ----------
 *
 * # Safety
 * `value` is a Datum referencing a valid in-line varlena (not external, not
 * compressed).
 */
pub unsafe fn toast_compress_datum(value: Datum, mut cmethod: c_char) -> Datum {
    let mut tmp: *mut crate::c::varlena = core::ptr::null_mut();
    let valsize: int32;
    let mut cmid: u32 = TOAST_INVALID_COMPRESSION_ID;

    Assert!(!VARATT_IS_EXTERNAL(DatumGetPointer(value) as *const c_char));
    Assert!(!VARATT_IS_COMPRESSED(DatumGetPointer(value) as *const c_char));

    valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value) as *const c_char) as int32;

    /* If the compression method is not valid, use the current default */
    if !CompressionMethodIsValid(cmethod) {
        cmethod = default_toast_compression;
    }

    /*
     * Call appropriate compression routine for the compression method.
     */
    if cmethod == TOAST_PGLZ_COMPRESSION {
        tmp = pglz_compress_datum(value as *const crate::c::varlena);
        cmid = TOAST_PGLZ_COMPRESSION_ID;
    } else if cmethod == TOAST_LZ4_COMPRESSION {
        tmp = lz4_compress_datum(value as *const crate::c::varlena);
        cmid = TOAST_LZ4_COMPRESSION_ID;
    } else {
        elog!(ERROR, "invalid compression method {}", cmethod as u8 as char);
    }

    if tmp.is_null() {
        return PointerGetDatum(core::ptr::null());
    }

    /*
     * We recheck the actual size even if compression reports success, because
     * it might be satisfied with having saved as little as one byte in the
     * compressed data --- which could turn into a net loss once you consider
     * header and alignment padding.  Worst case, the compressed format might
     * require three padding bytes (plus header, which is included in
     * VARSIZE(tmp)), whereas the uncompressed format would take only one
     * header byte and no padding if the value is short enough.  So we insist
     * on a savings of more than 2 bytes to ensure we have a gain.
     */
    if (VARSIZE(tmp as *const c_char) as int32) < valsize - 2 {
        /* successful compression */
        Assert!(cmid != TOAST_INVALID_COMPRESSION_ID);
        TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(tmp as *mut c_void, valsize, cmid);
        PointerGetDatum(tmp as *const c_void)
    } else {
        /* incompressible data */
        pfree(tmp as *mut c_void);
        PointerGetDatum(core::ptr::null())
    }
}

// ----------------------------------------------------------------------------
//   Stubbed heap / relcache / snapshot path
// ----------------------------------------------------------------------------

/* ----------
 * toast_save_datum -
 *
 *	Save one single datum into the secondary relation and return
 *	a Datum reference for it.
 *
 * rel: the main relation we're working with (not the toast rel!)
 * value: datum to be pushed to toast storage
 * oldexternal: if not NULL, toast pointer previously representing the datum
 * options: options to be passed to heap_insert() for toast rows
 * ----------
 *
 * # Safety
 * Stub: depends on heap/relcache/catalog machinery not yet ported.
 */
pub unsafe fn toast_save_datum(
    rel: Relation,
    value: Datum,
    oldexternal: *mut crate::c::varlena,
    options: c_int,
) -> Datum {
    let toastrel: Relation;
    let mut toastidxs: *mut Relation = core::ptr::null_mut();
    let mut toasttup: HeapTuple;
    let toasttupDesc: TupleDesc;
    let mut t_values: [Datum; 3] = [0; 3];
    let mut t_isnull: [bool; 3] = [false; 3];
    let mycid: CommandId = GetCurrentCommandId(true);
    let result: *mut crate::c::varlena;
    let mut toast_pointer: varatt_external = core::mem::zeroed();
    /*
     * union { struct varlena hdr; char data[TOAST_MAX_CHUNK_SIZE + VARHDRSZ];
     *         int32 align_it; } chunk_data = {0};
     */
    let mut chunk_data: [u8; (TOAST_MAX_CHUNK_SIZE + VARHDRSZ) as usize] =
        [0; (TOAST_MAX_CHUNK_SIZE + VARHDRSZ) as usize];
    let mut chunk_size: int32;
    let mut chunk_seq: int32 = 0;
    let mut data_p: *mut c_char;
    let mut data_todo: int32;
    let dval: Pointer = DatumGetPointer(value) as Pointer;
    let mut num_indexes: c_int = 0;
    let validIndex: c_int;

    Assert!(!VARATT_IS_EXTERNAL(value as *const c_char));

    /*
     * Open the toast relation and its indexes.  We can use the index to check
     * uniqueness of the OID we assign to the toasted item, even though it has
     * additional columns besides OID.
     */
    toastrel = table_open(rel_reltoastrelid(rel), RowExclusiveLock);
    toasttupDesc = rel_rd_att(toastrel);

    /* Open all the toast indexes and look for the valid one */
    validIndex = toast_open_indexes(
        toastrel,
        RowExclusiveLock,
        &mut toastidxs,
        &mut num_indexes,
    );

    /*
     * Get the data pointer and length, and compute va_rawsize and va_extinfo.
     *
     * va_rawsize is the size of the equivalent fully uncompressed datum, so
     * we have to adjust for short headers.
     *
     * va_extinfo stored the actual size of the data payload in the toast
     * records and the compression method in first 2 bits if data is
     * compressed.
     */
    if VARATT_IS_SHORT(dval) {
        data_p = VARDATA_SHORT(dval);
        data_todo = VARSIZE_SHORT(dval) as int32 - VARHDRSZ_SHORT;
        toast_pointer.va_rawsize = data_todo + VARHDRSZ; /* as if not short */
        toast_pointer.va_extinfo = data_todo as uint32;
    } else if VARATT_IS_COMPRESSED(dval) {
        data_p = VARDATA(dval);
        data_todo = VARSIZE(dval) as int32 - VARHDRSZ;
        /* rawsize in a compressed datum is just the size of the payload */
        toast_pointer.va_rawsize = VARDATA_COMPRESSED_GET_EXTSIZE(dval) as int32 + VARHDRSZ;

        /* set external size and compression method */
        VARATT_EXTERNAL_SET_SIZE_AND_COMPRESS_METHOD(
            &mut toast_pointer,
            data_todo,
            VARDATA_COMPRESSED_GET_COMPRESS_METHOD(dval),
        );
        /* Assert that the numbers look like it's compressed */
        Assert!(VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer));
    } else {
        data_p = VARDATA(dval);
        data_todo = VARSIZE(dval) as int32 - VARHDRSZ;
        toast_pointer.va_rawsize = VARSIZE(dval) as int32;
        toast_pointer.va_extinfo = data_todo as uint32;
    }

    /*
     * Insert the correct table OID into the result TOAST pointer.
     *
     * Normally this is the actual OID of the target toast table, but during
     * table-rewriting operations such as CLUSTER, we have to insert the OID
     * of the table's real permanent toast table instead.  rd_toastoid is set
     * if we have to substitute such an OID.
     */
    if OidIsValid(rel_rd_toastoid(rel)) {
        toast_pointer.va_toastrelid = rel_rd_toastoid(rel);
    } else {
        toast_pointer.va_toastrelid = RelationGetRelid(toastrel);
    }

    /*
     * Choose an OID to use as the value ID for this toast value.
     *
     * Normally we just choose an unused OID within the toast table.  But
     * during table-rewriting operations where we are preserving an existing
     * toast table OID, we want to preserve toast value OIDs too.  So, if
     * rd_toastoid is set and we had a prior external value from that same
     * toast table, re-use its value ID.  If we didn't have a prior external
     * value (which is a corner case, but possible if the table's attstorage
     * options have been changed), we have to pick a value ID that doesn't
     * conflict with either new or existing toast value OIDs.
     */
    if !OidIsValid(rel_rd_toastoid(rel)) {
        /* normal case: just choose an unused OID */
        toast_pointer.va_valueid = GetNewOidWithIndex(
            toastrel,
            RelationGetRelid(*toastidxs.add(validIndex as usize)),
            1 as AttrNumber,
        );
    } else {
        /* rewrite case: check to see if value was in old toast table */
        toast_pointer.va_valueid = InvalidOid;
        if !oldexternal.is_null() {
            Assert!(VARATT_IS_EXTERNAL_ONDISK(oldexternal as *const c_char));
            /* Must copy to access aligned fields */
            let old_toast_pointer: varatt_external = VARATT_EXTERNAL_GET_POINTER(oldexternal);
            if old_toast_pointer.va_toastrelid == rel_rd_toastoid(rel) {
                /* This value came from the old toast table; reuse its OID */
                toast_pointer.va_valueid = old_toast_pointer.va_valueid;

                /*
                 * There is a corner case here: the table rewrite might have
                 * to copy both live and recently-dead versions of a row, and
                 * those versions could easily reference the same toast value.
                 * When we copy the second or later version of such a row,
                 * reusing the OID will mean we select an OID that's already
                 * in the new toast table.  Check for that, and if so, just
                 * fall through without writing the data again.
                 *
                 * While annoying and ugly-looking, this is a good thing
                 * because it ensures that we wind up with only one copy of
                 * the toast value when there is only one copy in the old
                 * toast table.  Before we detected this case, we'd have made
                 * multiple copies, wasting space; and what's worse, the
                 * copies belonging to already-deleted heap tuples would not
                 * be reclaimed by VACUUM.
                 */
                if toastrel_valueid_exists(toastrel, toast_pointer.va_valueid) {
                    /* Match, so short-circuit the data storage loop below */
                    data_todo = 0;
                }
            }
        }
        if toast_pointer.va_valueid == InvalidOid {
            /*
             * new value; must choose an OID that doesn't conflict in either
             * old or new toast table
             */
            loop {
                toast_pointer.va_valueid = GetNewOidWithIndex(
                    toastrel,
                    RelationGetRelid(*toastidxs.add(validIndex as usize)),
                    1 as AttrNumber,
                );
                if !toastid_valueid_exists(rel_rd_toastoid(rel), toast_pointer.va_valueid) {
                    break;
                }
            }
        }
    }

    /*
     * Initialize constant parts of the tuple data
     */
    t_values[0] = ObjectIdGetDatum(toast_pointer.va_valueid);
    t_values[2] = PointerGetDatum(chunk_data.as_mut_ptr() as *const c_void);
    t_isnull[0] = false;
    t_isnull[1] = false;
    t_isnull[2] = false;

    /*
     * Split up the item into chunks
     */
    while data_todo > 0 {
        CHECK_FOR_INTERRUPTS();

        /*
         * Calculate the size of this chunk
         */
        chunk_size = Min(TOAST_MAX_CHUNK_SIZE, data_todo);

        /*
         * Build a tuple and store it
         */
        t_values[1] = Int32GetDatum(chunk_seq);
        chunk_seq += 1;
        SET_VARSIZE(chunk_data.as_mut_ptr() as *mut c_char, chunk_size + VARHDRSZ);
        memcpy(
            VARDATA(chunk_data.as_mut_ptr() as *const c_char) as *mut c_void,
            data_p as *const c_void,
            chunk_size as usize,
        );
        toasttup = heap_form_tuple(toasttupDesc, t_values.as_mut_ptr(), t_isnull.as_mut_ptr());

        heap_insert(toastrel, toasttup, mycid, options, core::ptr::null_mut());

        /*
         * Create the index entry.  We cheat a little here by not using
         * FormIndexDatum: this relies on the knowledge that the index columns
         * are the same as the initial columns of the table for all the
         * indexes.  We also cheat by not providing an IndexInfo: this is okay
         * for now because btree doesn't need one, but we might have to be
         * more honest someday.
         *
         * Note also that there had better not be any user-created index on
         * the TOAST table, since we don't bother to update anything else.
         */
        for i in 0..num_indexes {
            let toastidx = *toastidxs.add(i as usize);
            /* Only index relations marked as ready can be updated */
            if idx_indisready(toastidx) {
                index_insert(
                    toastidx,
                    t_values.as_mut_ptr(),
                    t_isnull.as_mut_ptr(),
                    htup_t_self(toasttup),
                    toastrel,
                    if idx_indisunique(toastidx) {
                        UNIQUE_CHECK_YES
                    } else {
                        UNIQUE_CHECK_NO
                    },
                    false,
                    core::ptr::null_mut(),
                );
            }
        }

        /*
         * Free memory
         */
        heap_freetuple(toasttup);

        /*
         * Move on to next chunk
         */
        data_todo -= chunk_size;
        data_p = data_p.add(chunk_size as usize);
    }

    /*
     * Done - close toast relation and its indexes but keep the lock until
     * commit, so as a concurrent reindex done directly on the toast relation
     * would be able to wait for this transaction.
     */
    toast_close_indexes(toastidxs, num_indexes, NoLock);
    table_close(toastrel, NoLock);

    /*
     * Create the TOAST pointer value that we'll return
     */
    result = palloc(TOAST_POINTER_SIZE as Size) as *mut crate::c::varlena;
    SET_VARTAG_EXTERNAL(result as *mut c_char, VARTAG_ONDISK);
    memcpy(
        VARDATA_EXTERNAL(result as *const c_char) as *mut c_void,
        &toast_pointer as *const varatt_external as *const c_void,
        core::mem::size_of::<varatt_external>(),
    );

    PointerGetDatum(result as *const c_void)
}

/* ----------
 * toast_delete_datum -
 *
 *	Delete a single external stored value.
 * ----------
 *
 * # Safety
 * Stub: depends on heap/relcache/snapshot machinery not yet ported.
 */
pub unsafe fn toast_delete_datum(_rel: Relation, value: Datum, is_speculative: bool) {
    let attr: *mut crate::c::varlena = DatumGetPointer(value) as *mut crate::c::varlena;
    let toast_pointer: varatt_external;
    let toastrel: Relation;
    let mut toastidxs: *mut Relation = core::ptr::null_mut();
    let mut toastkey: ScanKeyData = core::mem::zeroed();
    let toastscan: SysScanDesc;
    let mut toasttup: HeapTuple;
    let mut num_indexes: c_int = 0;
    let validIndex: c_int;

    if !VARATT_IS_EXTERNAL_ONDISK(attr as *const c_char) {
        return;
    }

    /* Must copy to access aligned fields */
    toast_pointer = VARATT_EXTERNAL_GET_POINTER(attr);

    /*
     * Open the toast relation and its indexes
     */
    toastrel = table_open(toast_pointer.va_toastrelid, RowExclusiveLock);

    /* Fetch valid relation used for process */
    validIndex = toast_open_indexes(
        toastrel,
        RowExclusiveLock,
        &mut toastidxs,
        &mut num_indexes,
    );

    /*
     * Setup a scan key to find chunks with matching va_valueid
     */
    ScanKeyInit(
        &mut toastkey,
        1 as AttrNumber,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(toast_pointer.va_valueid),
    );

    /*
     * Find all the chunks.  (We don't actually care whether we see them in
     * sequence or not, but since we've already locked the index we might as
     * well use systable_beginscan_ordered.)
     */
    toastscan = systable_beginscan_ordered(
        toastrel,
        *toastidxs.add(validIndex as usize),
        get_toast_snapshot(),
        1,
        &mut toastkey,
    );
    loop {
        toasttup = systable_getnext_ordered(toastscan, ForwardScanDirection);
        if toasttup.is_null() {
            break;
        }
        /*
         * Have a chunk, delete it
         */
        if is_speculative {
            heap_abort_speculative(toastrel, htup_t_self(toasttup));
        } else {
            simple_heap_delete(toastrel, htup_t_self(toasttup));
        }
    }

    /*
     * End scan and close relations but keep the lock until commit, so as a
     * concurrent reindex done directly on the toast relation would be able to
     * wait for this transaction.
     */
    systable_endscan_ordered(toastscan);
    toast_close_indexes(toastidxs, num_indexes, NoLock);
    table_close(toastrel, NoLock);
}

/* ----------
 * toastrel_valueid_exists -
 *
 *	Test whether a toast value with the given ID exists in the toast relation.
 *	For safety, we consider a value to exist if there are either live or dead
 *	toast rows with that ID; see notes for GetNewOidWithIndex().
 * ----------
 *
 * # Safety
 * Stub: depends on systable scan machinery not yet ported.
 */
#[allow(dead_code)]
unsafe fn toastrel_valueid_exists(toastrel: Relation, valueid: Oid) -> bool {
    let mut result: bool = false;
    let mut toastkey: ScanKeyData = core::mem::zeroed();
    let toastscan: SysScanDesc;
    let mut num_indexes: c_int = 0;
    let validIndex: c_int;
    let mut toastidxs: *mut Relation = core::ptr::null_mut();

    /* Fetch a valid index relation */
    validIndex = toast_open_indexes(
        toastrel,
        RowExclusiveLock,
        &mut toastidxs,
        &mut num_indexes,
    );

    /*
     * Setup a scan key to find chunks with matching va_valueid
     */
    ScanKeyInit(
        &mut toastkey,
        1 as AttrNumber,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(valueid),
    );

    /*
     * Is there any such chunk?
     */
    toastscan = systable_beginscan(
        toastrel,
        RelationGetRelid(*toastidxs.add(validIndex as usize)),
        true,
        SnapshotAny,
        1,
        &mut toastkey,
    );

    if !systable_getnext(toastscan).is_null() {
        result = true;
    }

    systable_endscan(toastscan);

    /* Clean up */
    toast_close_indexes(toastidxs, num_indexes, RowExclusiveLock);

    result
}

/* ----------
 * toastid_valueid_exists -
 *
 *	As above, but work from toast rel's OID not an open relation
 * ----------
 *
 * # Safety
 * Stub: depends on table_open/close not yet ported.
 */
#[allow(dead_code)]
unsafe fn toastid_valueid_exists(toastrelid: Oid, valueid: Oid) -> bool {
    let result: bool;
    let toastrel: Relation;

    toastrel = table_open(toastrelid, AccessShareLock);

    result = toastrel_valueid_exists(toastrel, valueid);

    table_close(toastrel, AccessShareLock);

    result
}

/* ----------
 * toast_get_valid_index
 *
 *	Get OID of valid index associated to given toast relation. A toast
 *	relation can have only one valid index at the same time.
 *
 * # Safety
 * Stub: depends on relcache/table_open machinery not yet ported.
 */
pub unsafe fn toast_get_valid_index(toastoid: Oid, lock: LOCKMODE) -> Oid {
    let mut num_indexes: c_int = 0;
    let validIndex: c_int;
    let validIndexOid: Oid;
    let mut toastidxs: *mut Relation = core::ptr::null_mut();
    let toastrel: Relation;

    /* Open the toast relation */
    toastrel = table_open(toastoid, lock);

    /* Look for the valid index of the toast relation */
    validIndex = toast_open_indexes(toastrel, lock, &mut toastidxs, &mut num_indexes);
    validIndexOid = RelationGetRelid(*toastidxs.add(validIndex as usize));

    /* Close the toast relation and all its indexes */
    toast_close_indexes(toastidxs, num_indexes, NoLock);
    table_close(toastrel, NoLock);

    validIndexOid
}

/* ----------
 * toast_open_indexes
 *
 *	Get an array of the indexes associated to the given toast relation
 *	and return as well the position of the valid index used by the toast
 *	relation in this array. It is the responsibility of the caller of this
 *	function to close the indexes as well as free them.
 *
 * # Safety
 * Stub: depends on relcache index-list machinery not yet ported.
 */
pub unsafe fn toast_open_indexes(
    toastrel: Relation,
    lock: LOCKMODE,
    toastidxs: *mut *mut Relation,
    num_indexes: *mut c_int,
) -> c_int {
    let mut i: c_int = 0;
    let mut res: c_int = 0;
    let mut found: bool = false;
    let indexlist: *mut List;

    /* Get index list of the toast relation */
    indexlist = RelationGetIndexList(toastrel);
    Assert!(indexlist != crate::nodes::pg_list::NIL);

    *num_indexes = list_length(indexlist);

    /* Open all the index relations */
    *toastidxs =
        palloc((*num_indexes as usize) * core::mem::size_of::<Relation>()) as *mut Relation;
    foreach!(lc, indexlist, {
        let cell = current_cell!(lc);
        *(*toastidxs).add(i as usize) = index_open(lfirst_oid(cell), lock);
        i += 1;
    });

    /* Fetch the first valid index in list */
    i = 0;
    while i < *num_indexes {
        let toastidx: Relation = *(*toastidxs).add(i as usize);

        if idx_indisvalid(toastidx) {
            res = i;
            found = true;
            break;
        }
        i += 1;
    }

    /*
     * Free index list, not necessary anymore as relations are opened and a
     * valid index has been found.
     */
    list_free(indexlist);

    /*
     * The toast relation should have one valid index, so something is going
     * wrong if there is nothing.
     */
    if !found {
        elog!(
            ERROR,
            "no valid index found for toast relation with Oid {}",
            RelationGetRelid(toastrel)
        );
    }

    res
}

/* ----------
 * toast_close_indexes
 *
 *	Close an array of indexes for a toast relation and free it. This should
 *	be called for a set of indexes opened previously with toast_open_indexes.
 *
 * # Safety
 * Stub: depends on index_close not yet ported.
 */
pub unsafe fn toast_close_indexes(toastidxs: *mut Relation, num_indexes: c_int, lock: LOCKMODE) {
    /* Close relations and clean up things */
    let mut i: c_int = 0;
    while i < num_indexes {
        index_close(*toastidxs.add(i as usize), lock);
        i += 1;
    }
    pfree(toastidxs as *mut c_void);
}

/* ----------
 * get_toast_snapshot
 *
 *	Return the TOAST snapshot. Detoasting *must* happen in the same
 *	transaction that originally fetched the toast pointer.
 *
 * # Safety
 * Stub: depends on snapmgr (HaveRegisteredOrActiveSnapshot / SnapshotToastData).
 */
pub unsafe fn get_toast_snapshot() -> Snapshot {
    /*
     * We cannot directly check that detoasting happens in the same
     * transaction that originally fetched the toast pointer, but at least
     * check that the session has some active snapshots. It might not if, for
     * example, a procedure fetches a toasted value into a local variable,
     * commits, and then tries to detoast the value. Such coding is unsafe,
     * because once we commit there is nothing to prevent the toast data from
     * being deleted. (This is not very much protection, because in many
     * scenarios the procedure would have already created a new transaction
     * snapshot, preventing us from detecting the problem. But it's better
     * than nothing.)
     */
    if !HaveRegisteredOrActiveSnapshot() {
        elog!(ERROR, "cannot fetch toast data without an active snapshot");
    }

    &raw mut SnapshotToastData as Snapshot
}

// ----------------------------------------------------------------------------
//   Tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::pg_lzcompress::pglz_decompress;

    /// Round-trip a highly compressible varlena through toast_compress_datum and
    /// then pglz_decompress, verifying the compressed header (size + method) and
    /// that the payload reconstructs.
    #[test]
    fn pglz_compress_datum_roundtrip() {
        unsafe {
            // Build a 4-byte-header datum with very compressible content.
            let raw: Vec<u8> = std::iter::repeat(b'A').take(2000).collect();
            let total = VARHDRSZ as usize + raw.len();
            let p = palloc(total) as *mut c_char;
            SET_VARSIZE(p, total as int32);
            let data = VARDATA(p);
            for (i, b) in raw.iter().enumerate() {
                *data.add(i) = *b as c_char;
            }

            let valsize = VARSIZE_ANY_EXHDR(p) as int32;

            let d = toast_compress_datum(PointerGetDatum(p as *const c_void), TOAST_PGLZ_COMPRESSION);
            let cptr = DatumGetPointer(d);
            assert!(!cptr.is_null(), "highly compressible data must compress");

            // It must be flagged as a compressed-in-line varlena.
            assert!(VARATT_IS_COMPRESSED(cptr as *const c_char));

            // Header: extsize == original payload size; method == PGLZ.
            assert_eq!(
                TOAST_COMPRESS_EXTSIZE(cptr as *const c_void) as int32,
                valsize
            );
            assert_eq!(
                TOAST_COMPRESS_METHOD(cptr as *const c_void),
                TOAST_PGLZ_COMPRESSION_ID
            );

            // Decompress the payload (starts after VARHDRSZ_COMPRESSED) and compare.
            let mut out = vec![0i8; raw.len()];
            let rawsize = pglz_decompress(
                (cptr as *const c_char).add(VARHDRSZ_COMPRESSED as usize),
                VARSIZE(cptr as *const c_char) as int32 - VARHDRSZ_COMPRESSED,
                out.as_mut_ptr() as *mut c_char,
                valsize,
                true,
            );
            assert_eq!(rawsize, raw.len() as int32);
            let got: Vec<u8> = out.iter().map(|&b| b as u8).collect();
            assert_eq!(got, raw);

            pfree(cptr as *mut c_void);
            pfree(p as *mut c_void);
        }
    }

    /// Incompressible (random-ish, short) data should return a NULL datum.
    #[test]
    fn incompressible_returns_null() {
        unsafe {
            // A short, non-repetitive payload won't beat the 2-byte gain threshold.
            let raw: [u8; 16] = [
                0x3f, 0xa1, 0x09, 0xce, 0x7b, 0x12, 0x44, 0x90, 0xde, 0x05, 0xbb, 0x6e, 0x21,
                0x88, 0xf3, 0x4c,
            ];
            let total = VARHDRSZ as usize + raw.len();
            let p = palloc(total) as *mut c_char;
            SET_VARSIZE(p, total as int32);
            let data = VARDATA(p);
            for (i, b) in raw.iter().enumerate() {
                *data.add(i) = *b as c_char;
            }

            let d = toast_compress_datum(PointerGetDatum(p as *const c_void), TOAST_PGLZ_COMPRESSION);
            assert!(DatumGetPointer(d).is_null());

            pfree(p as *mut c_void);
        }
    }
}

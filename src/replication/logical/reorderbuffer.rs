/* -------------------------------------------------------------------------
 *
 * reorderbuffer.rs
 *   PostgreSQL logical replay/reorder buffer management
 *
 * Copyright (c) 2012-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *   src/replication/logical/reorderbuffer.rs
 *
 * NOTES
 *   This module gets handed individual pieces of transactions in the order
 *   they are written to the WAL and is responsible to reassemble them into
 *   toplevel transaction sized pieces. When a transaction is completely
 *   reassembled - signaled by reading the transaction commit record - it
 *   will then call the output plugin (cf. ReorderBufferCommit()) with the
 *   individual changes. The output plugins rely on snapshots built by
 *   snapbuild.c which hands them to us.
 *
 *   See reorderbuffer.c header comments for full design notes.
 * -------------------------------------------------------------------------
 */
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Imports from real homes
// ---------------------------------------------------------------------------

use crate::lib::ilist::{
    dlist_head, dlist_node, dclist_head,
    dlist_init, dlist_push_tail, dlist_push_head, dlist_pop_head_node,
    dlist_delete, dlist_insert_before,
    dlist_is_empty, dlist_has_next, dlist_next_node,
    dlist_iter, dlist_mutable_iter,
    dclist_init, dclist_push_tail, dclist_delete_from, dclist_count,
};
// dlist_head_element!, dlist_foreach!, dlist_foreach_modify!, dclist_foreach!,
// dclist_container!, dlist_container! are #[macro_export] crate-root macros.
use crate::lib::pairingheap::{
    pairingheap, pairingheap_node,
    pairingheap_allocate, pairingheap_free,
    pairingheap_add, pairingheap_remove, pairingheap_first,
    pairingheap_is_empty,
};
use crate::lib::binaryheap::{
    binaryheap, binaryheap_comparator,
    binaryheap_allocate, binaryheap_free,
    binaryheap_add_unordered, binaryheap_build,
    binaryheap_first, binaryheap_replace_first, binaryheap_remove_first,
};

use crate::access::transam::xlogdefs::{
    XLogRecPtr, InvalidXLogRecPtr, XLogSegNo, RepOriginId,
};
use crate::access::transam::xlogreader::RelFileLocator;

// Snapshot type - real home is utils/snapshot.rs
use crate::utils::snapshot::{Snapshot, SnapshotData};

// HeapTuple / HeapTupleData / HeapTupleHeader - real home: access/htup_details
use crate::access::htup_details::{
    HeapTuple, HeapTupleData, HeapTupleHeader,
    HEAPTUPLESIZE, SizeofHeapTupleHeader, MaxHeapTupleSize,
};

// MemoryContextAlloc/AllocZero from palloc.rs (uses palloc::MemoryContext = *mut palloc::MemoryContextData)
use crate::utils::palloc::{
    MemoryContextAlloc as mc_alloc, MemoryContextAllocZero as mc_alloc_zero,
};
// MemoryContextDelete from memutils.rs stub (palloc::MemoryContext)
use crate::utils::memutils::MemoryContextDelete;

// TransactionId helpers
use crate::access::transam::{
    InvalidTransactionId,
    TransactionIdIsValid,
    TransactionIdEquals,
};
// TransactionIdDidCommit, TransactionIdPrecedes live in transam.rs (the .c file port)
use crate::access::transam::transam::{
    TransactionIdDidCommit, TransactionIdPrecedes,
};
// TransactionId is available via prelude (crate::c::TransactionId)
// TransactionIdIsInProgress lives in procarray.c
use crate::storage::ipc::procarray::TransactionIdIsInProgress;

use crate::c::{uint32, int32, bits32, int64, uint64, Size};
// Oid, Datum, CommandId are available via prelude (postgres_ext / postgres / c)
use core::ffi::{c_char, c_int, c_void};

// crate-root #[macro_export] macros from ilist.rs / pairingheap.rs
use crate::{
    dlist_container, dlist_head_element, dlist_foreach, dlist_foreach_modify,
    dclist_container, dclist_foreach, pairingheap_container,
};

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies
// ---------------------------------------------------------------------------

/// TODO(pg-port): real SlabContextCreate lives in utils/mmgr/slab.c
#[allow(non_snake_case)]
pub unsafe fn SlabContextCreate(
    parent: MemoryContext,
    _name: &'static str,
    _block_size: Size,
    _chunk_size: Size,
) -> MemoryContext {
    // fall back to a plain AllocSet context until slab is ported
    AllocSetContextCreate!(parent, c"Slab context".as_ptr(), ALLOCSET_DEFAULT_SIZES)
}

/// TODO(pg-port): real GenerationContextCreate lives in utils/mmgr/generation.c
#[allow(non_snake_case)]
pub unsafe fn GenerationContextCreate(
    parent: MemoryContext,
    _name: &'static str,
    _min_block: Size,
    _init_block: Size,
    _max_block: Size,
) -> MemoryContext {
    AllocSetContextCreate!(parent, c"Generation context".as_ptr(), ALLOCSET_DEFAULT_SIZES)
}

/// TODO(pg-port): real HASH_ELEM | HASH_BLOBS | HASH_CONTEXT lives in utils/hash
pub const HASH_ELEM: c_int = 0x001;
pub const HASH_BLOBS: c_int = 0x040;
pub const HASH_CONTEXT: c_int = 0x800;
pub const HASH_ENTER: c_int = 1;
pub const HASH_FIND: c_int = 2;
pub const HASH_REMOVE: c_int = 3;

/// TODO(pg-port): real HTAB/HASHCTL live in utils/hsearch.c
#[repr(C)]
pub struct HTAB {
    _opaque: [u8; 0],
}

#[repr(C)]
pub struct HASHCTL {
    pub keysize: Size,
    pub entrysize: Size,
    pub hcxt: MemoryContext,
}

/// TODO(pg-port): real hash_create/hash_search/hash_destroy live in utils/hsearch.c
#[allow(non_snake_case)]
pub unsafe fn hash_create(
    name: &str, nelem: c_long, info: *mut HASHCTL, flags: c_int,
) -> *mut HTAB { unimplemented!() }
#[allow(non_snake_case)]
pub unsafe fn hash_search(
    hashp: *mut HTAB, keyPtr: *const c_void, action: c_int, foundPtr: *mut bool,
) -> *mut c_void { todo!("TODO(pg-port): hash_search") }
#[allow(non_snake_case)]
pub unsafe fn hash_destroy(_hashp: *mut HTAB) {}

/// TODO(pg-port): hash_seq_init / hash_seq_search live in utils/hsearch.c
#[repr(C)]
pub struct HASH_SEQ_STATUS { _opaque: [u8; 0] }
#[allow(non_snake_case)]
pub unsafe fn hash_seq_init(_status: *mut HASH_SEQ_STATUS, _hashp: *mut HTAB) {}
#[allow(non_snake_case)]
pub unsafe fn hash_seq_search(_status: *mut HASH_SEQ_STATUS) -> *mut c_void { null_mut() }

/// TODO(pg-port): real SnapBuildSnapDecRefcount lives in replication/snapbuild.c
#[allow(non_snake_case)]
pub unsafe fn SnapBuildSnapDecRefcount(_snap: Snapshot) {}
/// TODO(pg-port): real SnapBuildCurrentState lives in replication/snapbuild.c
#[allow(non_snake_case)]
pub unsafe fn SnapBuildCurrentState(_builder: *mut SnapBuild) -> c_int { 0 }
/// TODO(pg-port): real SnapBuildXactNeedsSkip lives in replication/snapbuild.c
#[allow(non_snake_case)]
pub unsafe fn SnapBuildXactNeedsSkip(_builder: *mut SnapBuild, _lsn: XLogRecPtr) -> bool { false }
/// TODO(pg-port): real SnapBuild struct lives in replication/snapbuild.c
pub struct SnapBuild { _opaque: c_void }
pub const SNAPBUILD_CONSISTENT: c_int = 3;

/// TODO(pg-port): real LogicalDecodingContext lives in replication/logical.c
#[repr(C)]
pub struct LogicalDecodingContext {
    pub private_data: *mut c_void,
    pub streaming: bool,
    pub snapshot_builder: *mut SnapBuild,
    pub reader: *mut XLogReaderStateStub,
}
/// TODO(pg-port): minimal stub for XLogReaderState used by LogicalDecodingContext
pub struct XLogReaderStateStub {
    pub EndRecPtr: XLogRecPtr,
    pub ReadRecPtr: XLogRecPtr,
}

/// TODO(pg-port): real ReplicationSlot lives in replication/slot.c
#[repr(C)]
pub struct ReplicationSlot {
    pub data: ReplicationSlotPersistentData,
}
#[repr(C)]
pub struct ReplicationSlotPersistentData {
    pub name: NameData,
}
#[repr(C)]
pub struct NameData {
    pub data: [c_char; 64],
}
/// TODO(pg-port): NameStr macro - returns null-terminated string slice
macro_rules! NameStr {
    ($n:expr) => {
        $n.data.as_ptr()
    };
}

/// TODO(pg-port): MyReplicationSlot lives in replication/slot.c
pub static mut MyReplicationSlot: *mut ReplicationSlot = null_mut();

/// TODO(pg-port): real SharedInvalidationMessage lives in storage/sinval.h
pub use crate::access::rmgrdesc::standbydesc::SharedInvalidationMessage;

/// TODO(pg-port): real LocalExecuteInvalidationMessage lives in utils/inval.c
#[allow(non_snake_case)]
pub unsafe fn LocalExecuteInvalidationMessage(_msg: *mut SharedInvalidationMessage) {}
/// TODO(pg-port): real InvalidateSystemCaches lives in utils/inval.c
#[allow(non_snake_case)]
pub unsafe fn InvalidateSystemCaches() {}

/// TODO(pg-port): real SetupHistoricSnapshot / TeardownHistoricSnapshot live in utils/snapmgr.c
#[allow(non_snake_case)]
pub unsafe fn SetupHistoricSnapshot(_snap: Snapshot, _tuplecid_hash: *mut HTAB) {}
#[allow(non_snake_case)]
pub unsafe fn TeardownHistoricSnapshot(_was_error: bool) {}

/// TODO(pg-port): real Relation lives in utils/rel.h
pub struct RelationData { _opaque: c_void }
pub type Relation = *mut RelationData;
#[allow(non_snake_case)]
pub unsafe fn RelationIsValid(r: Relation) -> bool { !r.is_null() }
#[allow(non_snake_case)]
#[no_mangle]
pub unsafe fn RelationIsLogicallyLogged(_r: Relation) -> bool { false }
#[allow(non_snake_case)]
pub unsafe fn RelationIdGetRelation(_reloid: Oid) -> Relation { null_mut() }
#[allow(non_snake_case)]
pub unsafe fn RelationClose(_r: Relation) {}
#[allow(non_snake_case)]
pub unsafe fn IsToastRelation(_r: Relation) -> bool { false }
#[allow(non_snake_case)]
pub unsafe fn RelationGetDescr(_r: Relation) -> *mut TupleDescData { null_mut() }
pub struct TupleDescData { pub natts: c_int, _opaque: c_void }
pub type TupleDesc = *mut TupleDescData;
pub type Form_pg_attribute = *mut c_void;
#[allow(non_snake_case)]
pub unsafe fn TupleDescAttr(_desc: TupleDesc, _n: c_int) -> Form_pg_attribute { null_mut() }
/// TODO(pg-port): RelfileNumber / relfilenumbermap stubs
#[allow(non_snake_case)]
pub unsafe fn RelidByRelfilenumber(_spc: Oid, _rel: c_uint) -> Oid { 0 }

/// TODO(pg-port): real transaction mgmt lives in access/transam/xact.c
pub static mut CheckXidAlive: TransactionId = 0; // InvalidTransactionId=0
#[allow(non_snake_case)]
pub unsafe fn IsTransactionOrTransactionBlock() -> bool { false }
#[allow(non_snake_case)]
pub unsafe fn BeginInternalSubTransaction(_name: *const c_char) {}
#[allow(non_snake_case)]
pub unsafe fn StartTransactionCommand() {}
#[allow(non_snake_case)]
pub unsafe fn AbortCurrentTransaction() {}
#[allow(non_snake_case)]
pub unsafe fn RollbackAndReleaseCurrentSubTransaction() {}
#[allow(non_snake_case)]
pub unsafe fn GetCurrentTransactionIdIfAny() -> TransactionId { 0 }
#[allow(non_snake_case)]
pub unsafe fn GetCurrentTransactionId() -> TransactionId { 0 }
pub const FirstCommandId: CommandId = 1;
pub const InvalidCommandId: CommandId = !0u32;

/// TODO(pg-port): real error/pgstat stubs
#[allow(non_snake_case)]
pub unsafe fn pgstat_report_wait_start(_event: uint32) {}
#[allow(non_snake_case)]
pub unsafe fn pgstat_report_wait_end() {}
pub const WAIT_EVENT_REORDER_BUFFER_WRITE: uint32 = 0;
pub const WAIT_EVENT_REORDER_BUFFER_READ: uint32 = 0;
pub const WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ: uint32 = 0;

/// TODO(pg-port): real CopyErrorData/FlushErrorState live in utils/error/elog.c
pub struct ErrorData { pub sqlerrcode: c_int }
pub const ERRCODE_TRANSACTION_ROLLBACK: c_int = 25;
#[allow(non_snake_case)]
pub unsafe fn CopyErrorData() -> *mut ErrorData { null_mut() }
#[allow(non_snake_case)]
pub unsafe fn FlushErrorState() {}
#[allow(non_snake_case)]
pub unsafe fn FreeErrorData(_err: *mut ErrorData) {}

/// TODO(pg-port): real UpdateDecodingStats lives in replication/logical.c
#[allow(non_snake_case)]
pub unsafe fn UpdateDecodingStats(_ctx: *mut LogicalDecodingContext) {}

/// TODO(pg-port): file descriptor types / file ops live in storage/fd.c
pub type File = c_int; // VFD descriptor
#[allow(non_snake_case)]
pub unsafe fn PathNameOpenFile(_path: *const c_char, _flags: c_int) -> File { -1 }
#[allow(non_snake_case)]
pub unsafe fn FileClose(_f: File) {}
#[allow(non_snake_case)]
pub unsafe fn FileRead(
    _f: File, _buf: *mut c_void, _amount: c_int, _off: i64, _wait: uint32,
) -> c_int { 0 }
#[allow(non_snake_case)]
pub unsafe fn OpenTransientFile(_path: *const c_char, _flags: c_int) -> c_int { -1 }
#[allow(non_snake_case)]
pub unsafe fn CloseTransientFile(_fd: c_int) -> c_int { 0 }

/// TODO(pg-port): real dir ops live in storage/file/fd.c
pub struct DIR { _opaque: c_void }
pub struct dirent { pub d_name: [c_char; 256] }
#[allow(non_snake_case)]
pub unsafe fn AllocateDir(_path: *const c_char) -> *mut DIR { null_mut() }
#[allow(non_snake_case)]
pub unsafe fn ReadDir(_dir: *mut DIR, _path: *const c_char) -> *mut dirent { null_mut() }
#[allow(non_snake_case)]
pub unsafe fn ReadDirExtended(_dir: *mut DIR, _path: *const c_char, _elevel: c_int) -> *mut dirent { null_mut() }
#[allow(non_snake_case)]
pub unsafe fn FreeDir(_dir: *mut DIR) {}

/// TODO(pg-port): real WAL segment helpers live in access/transam/xlog_internal.h
pub static mut wal_segment_size: uint32 = 16 * 1024 * 1024;
/// XLByteToSeg: segment number from LSN
#[inline]
pub unsafe fn XLByteToSeg(lsn: XLogRecPtr, segno: &mut XLogSegNo) {
    *segno = lsn / (wal_segment_size as u64);
}
/// XLByteInSeg: true if lsn falls in the given segment
#[inline]
pub unsafe fn XLByteInSeg(lsn: XLogRecPtr, segno: XLogSegNo) -> bool {
    lsn / (wal_segment_size as u64) == segno
}
/// XLogSegNoOffsetToRecPtr
#[inline]
pub unsafe fn XLogSegNoOffsetToRecPtr(segno: XLogSegNo, offset: uint32) -> XLogRecPtr {
    segno * (wal_segment_size as u64) + offset as u64
}
pub const MAXPGPATH: usize = 1024;
pub const PG_BINARY: c_int = 0;

/// TODO(pg-port): real relpathperm lives in catalog/catalog.c
pub struct RelPathPerm { pub str_: [c_char; MAXPGPATH] }
#[allow(non_snake_case)]
pub unsafe fn relpathperm(_rlocator: RelFileLocator, _forknum: c_int) -> RelPathPerm {
    RelPathPerm { str_: [0; MAXPGPATH] }
}
pub const MAIN_FORKNUM: c_int = 0;

/// TODO(pg-port): real ReplicationSlotValidateName lives in replication/slot.c
#[allow(non_snake_case)]
pub unsafe fn ReplicationSlotValidateName(_name: *const c_char, _elevel: c_int) -> bool { false }

/// TODO(pg-port): PG_REPLSLOT_DIR lives in replication/slot.c
pub const PG_REPLSLOT_DIR: &str = "pg_replslot";

/// TODO(pg-port): real xidComparator lives in access/transam/transam.c
pub unsafe extern "C" fn xidComparator(
    a: *const c_void, b: *const c_void,
) -> c_int {
    let xa = *(a as *const TransactionId);
    let xb = *(b as *const TransactionId);
    if xa < xb { -1 } else if xa > xb { 1 } else { 0 }
}

/// TODO(pg-port): toast internals live in access/heap/toast_internals.c
pub struct varlena { pub vl_len_: [c_char; 4] }
pub const INDIRECT_POINTER_SIZE: usize = 8;
pub struct varatt_external { pub va_rawsize: int32, pub va_valueid: Oid, pub va_toastrelid: Oid, pub va_extsize: int32 }
pub struct varatt_indirect { pub pointer: *mut varlena }
#[allow(non_snake_case)] pub unsafe fn VARATT_IS_EXTENDED(_v: *mut varlena) -> bool { false }
#[allow(non_snake_case)] pub unsafe fn VARATT_IS_SHORT(_v: *mut varlena) -> bool { false }
#[allow(non_snake_case)] pub unsafe fn VARATT_IS_EXTERNAL(_v: *mut varlena) -> bool { false }
#[allow(non_snake_case)] pub unsafe fn VARATT_EXTERNAL_GET_POINTER(_p: *mut varatt_external, _v: *mut varlena) {}
#[allow(non_snake_case)] pub unsafe fn VARATT_EXTERNAL_GET_EXTSIZE(_p: varatt_external) -> Size { 0 }
#[allow(non_snake_case)] pub unsafe fn VARATT_EXTERNAL_IS_COMPRESSED(_p: varatt_external) -> bool { false }
#[allow(non_snake_case)] pub unsafe fn VARSIZE(_v: *const varlena) -> Size { 0 }
#[allow(non_snake_case)] pub unsafe fn VARSIZE_SHORT(_v: *const varlena) -> Size { 0 }
pub const VARHDRSZ: Size = 4;
pub const VARHDRSZ_SHORT: Size = 1;
#[allow(non_snake_case)] pub unsafe fn VARDATA(_v: *mut varlena) -> *mut c_char { null_mut() }
#[allow(non_snake_case)] pub unsafe fn VARDATA_EXTERNAL(_v: *mut varlena) -> *mut c_char { null_mut() }
#[allow(non_snake_case)] pub unsafe fn SET_VARSIZE(_v: *mut varlena, _sz: Size) {}
#[allow(non_snake_case)] pub unsafe fn SET_VARSIZE_COMPRESSED(_v: *mut varlena, _sz: Size) {}
#[allow(non_snake_case)] pub unsafe fn SET_VARTAG_EXTERNAL(_v: *mut varlena, _tag: c_int) {}
pub const VARTAG_INDIRECT: c_int = 1;

/// TODO(pg-port): heap_deform_tuple / heap_form_tuple live in access/heap/heaptuple.c
#[allow(non_snake_case)]
pub unsafe fn heap_deform_tuple(_tup: HeapTuple, _desc: TupleDesc, _attrs: *mut Datum, _isnull: *mut bool) {}
#[allow(non_snake_case)]
pub unsafe fn heap_form_tuple(_desc: TupleDesc, _attrs: *mut Datum, _isnull: *mut bool) -> HeapTuple { null_mut() }

/// TODO(pg-port): fastgetattr lives in access/htup_details.h
#[allow(non_snake_case)]
pub unsafe fn fastgetattr(_tup: HeapTuple, _attnum: c_int, _desc: TupleDesc, _isnull: *mut bool) -> Datum { 0 }
#[allow(non_snake_case)]
pub unsafe fn DatumGetObjectId(d: Datum) -> Oid { d as Oid }
#[allow(non_snake_case)]
#[no_mangle]
pub unsafe fn DatumGetInt32(d: Datum) -> int32 { d as int32 }
#[allow(non_snake_case)]
pub unsafe fn DatumGetPointer(d: Datum) -> *mut c_void { d as *mut c_void }
#[allow(non_snake_case)]
pub unsafe fn PointerGetDatum(p: *mut c_void) -> Datum { p as Datum }

/// TODO(pg-port): real LIST / list_sort / lappend live in nodes/pg_list.h
pub struct List { _opaque: c_void }
pub struct ListCell { _opaque: c_void }
pub const NIL: *mut List = null_mut();
#[allow(non_snake_case)] pub unsafe fn lappend(_l: *mut List, _p: *mut c_void) -> *mut List { null_mut() }
#[allow(non_snake_case)] pub unsafe fn list_sort(_l: *mut List, _cmp: unsafe fn(*const ListCell, *const ListCell) -> c_int) {}
#[allow(non_snake_case)] pub unsafe fn lfirst(_cell: *const ListCell) -> *mut c_void { null_mut() }
macro_rules! foreach {
    ($cell:ident, $list:expr, $body:block) => { let _ = $list; }
}

/// TODO(pg-port): real logical rewrite mapping lives in access/heap/rewriteheap.c
#[repr(C)]
pub struct LogicalRewriteMappingData {
    pub old_locator: RelFileLocator,
    pub old_tid: ItemPointerData,
    pub new_locator: RelFileLocator,
    pub new_tid: ItemPointerData,
    pub mapped_xid: TransactionId,
}
/// TODO(pg-port): LOGICAL_REWRITE_FORMAT lives in access/heap/rewriteheap.h
pub const LOGICAL_REWRITE_FORMAT: &str = "map-%u-%u-%X-%X-%u-%u";

/// TODO(pg-port): real BufferGetTag / BufferIsLocal live in storage/bufmgr.c
pub type Buffer = c_int;
#[allow(non_snake_case)]
pub unsafe fn BufferGetTag(_buf: Buffer, _rlocator: *mut RelFileLocator, _forkno: *mut c_int, _blockno: *mut uint32) {}
#[allow(non_snake_case)]
pub unsafe fn BufferIsLocal(_buf: Buffer) -> bool { false }

/// TODO(pg-port): real IsSharedRelation lives in catalog/catalog.c
#[allow(non_snake_case)]
pub unsafe fn IsSharedRelation(_relid: Oid) -> bool { false }
pub static mut MyDatabaseId: Oid = 0;

/// TODO(pg-port): ItemPointerData (ctid) - using inline struct
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ItemPointerData {
    pub ip_blkid: [c_char; 4],
    pub ip_posid: uint16,
}
pub type uint16 = u16;
pub type BlockNumber = uint32;
#[allow(non_snake_case)]
pub unsafe fn ItemPointerCopy(src: *const ItemPointerData, dst: *mut ItemPointerData) {
    *dst = *src;
}
#[allow(non_snake_case)]
pub unsafe fn ItemPointerGetBlockNumber(_p: *const ItemPointerData) -> BlockNumber { 0 }
#[allow(non_snake_case)]
pub unsafe fn ItemPointerGetOffsetNumber(_p: *const ItemPointerData) -> uint16 { 0 }

/// TODO(pg-port): Pointer is c_char* alias
pub type Pointer = *mut c_char;
pub type c_long = i64;

// pg_cmp_u64
#[inline]
pub fn pg_cmp_u64(a: u64, b: u64) -> c_int {
    if a < b { -1 } else if a > b { 1 } else { 0 }
}

/// TODO(pg-port): snprintf / unlink / sprintf / read / write / lstat / sscanf from libc
pub use core::ffi::CStr;
extern "C" {
    pub fn snprintf(buf: *mut c_char, size: usize, fmt: *const c_char, ...) -> c_int;
    pub fn sprintf(buf: *mut c_char, fmt: *const c_char, ...) -> c_int;
    pub fn unlink(path: *const c_char) -> c_int;
    pub fn lstat(path: *const c_char, buf: *mut StatBuf) -> c_int;
    pub fn sscanf(s: *const c_char, fmt: *const c_char, ...) -> c_int;
    pub fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    pub fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    pub fn bsearch(key: *const c_void, base: *const c_void, nmemb: usize, size: usize,
                   cmp: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int) -> *mut c_void;
    pub fn qsort(base: *mut c_void, nmemb: usize, size: usize,
                 cmp: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int);
    pub fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    pub fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    pub fn strlen(s: *const c_char) -> usize;
    pub fn memset(dest: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    pub fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    pub fn strcpy(dest: *mut c_char, src: *const c_char) -> *mut c_char;
}
#[repr(C)]
pub struct StatBuf {
    pub st_mode: uint32,
    _rest: [u8; 128],
}
// S_ISDIR macro equivalent
#[inline]
pub fn S_ISDIR(mode: uint32) -> bool { (mode & 0xF000) == 0x4000 }
pub const O_RDONLY: c_int = 0;
pub const O_WRONLY: c_int = 1;
pub const O_CREAT: c_int = 0x40;
pub const O_APPEND: c_int = 0x400;
pub const ENOENT: c_int = 2;
pub const ENOSPC: c_int = 28;
pub const INFO: c_int = 17;
// errno not a static in this translation -- use libc semantics
extern "C" { pub static mut errno: c_int; }

/// TODO(pg-port): real errcode_for_file_access lives in utils/error/elog.c
#[allow(non_snake_case)]
pub unsafe fn errcode_for_file_access() -> c_int { 0 }
/// TODO(pg-port): real CHECK_FOR_INTERRUPTS lives in miscadmin.h
#[allow(non_snake_case)]
pub unsafe fn CHECK_FOR_INTERRUPTS() {}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------
pub const PG_LOGICAL_DIR: &str = "pg_logical";
pub const PG_LOGICAL_MAPPINGS_DIR: &str = "pg_logical/mappings";
pub const PG_LOGICAL_SNAPSHOTS_DIR: &str = "pg_logical/snapshots";

// ---------------------------------------------------------------------------
// GUC variables
// ---------------------------------------------------------------------------

/// Each transaction has an 8MB limit for invalidation messages distributed
/// from other transactions.
unsafe fn max_distr_inval_msg_per_txn() -> Size {
    (8 * 1024 * 1024) / core::mem::size_of::<SharedInvalidationMessage>()
}

/// GUC: logical_decoding_work_mem
pub static mut logical_decoding_work_mem: c_int = 65536; /* 64 MB default */

/// Maximum number of changes kept in memory per transaction (for restore only).
const MAX_CHANGES_IN_MEMORY: Size = 4096;

/// GUC: debug_logical_replication_streaming
pub static mut debug_logical_replication_streaming: c_int =
    DEBUG_LOGICAL_REP_STREAMING_BUFFERED;

#[repr(C)]
#[derive(Clone, Copy, PartialEq)]
pub enum DebugLogicalRepStreamingMode {
    Buffered = 0,
    Immediate = 1,
}
pub const DEBUG_LOGICAL_REP_STREAMING_BUFFERED: c_int = 0;
pub const DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE: c_int = 1;

// ---------------------------------------------------------------------------
// ReorderBufferChangeType
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReorderBufferChangeType {
    Insert = 0,
    Update = 1,
    Delete = 2,
    Message = 3,
    Invalidation = 4,
    InternalSnapshot = 5,
    InternalCommandId = 6,
    InternalTupleCid = 7,
    InternalSpecInsert = 8,
    InternalSpecConfirm = 9,
    InternalSpecAbort = 10,
    Truncate = 11,
}
pub use ReorderBufferChangeType as RBTCT;

// Convenience aliases matching C names
pub const REORDER_BUFFER_CHANGE_INSERT: ReorderBufferChangeType = RBTCT::Insert;
pub const REORDER_BUFFER_CHANGE_UPDATE: ReorderBufferChangeType = RBTCT::Update;
pub const REORDER_BUFFER_CHANGE_DELETE: ReorderBufferChangeType = RBTCT::Delete;
pub const REORDER_BUFFER_CHANGE_MESSAGE: ReorderBufferChangeType = RBTCT::Message;
pub const REORDER_BUFFER_CHANGE_INVALIDATION: ReorderBufferChangeType = RBTCT::Invalidation;
pub const REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT: ReorderBufferChangeType = RBTCT::InternalSnapshot;
pub const REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID: ReorderBufferChangeType = RBTCT::InternalCommandId;
pub const REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID: ReorderBufferChangeType = RBTCT::InternalTupleCid;
pub const REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT: ReorderBufferChangeType = RBTCT::InternalSpecInsert;
pub const REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM: ReorderBufferChangeType = RBTCT::InternalSpecConfirm;
pub const REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT: ReorderBufferChangeType = RBTCT::InternalSpecAbort;
pub const REORDER_BUFFER_CHANGE_TRUNCATE: ReorderBufferChangeType = RBTCT::Truncate;

// ---------------------------------------------------------------------------
// Change data union -- represented as a tagged struct since Rust unions
// require Copy or ManuallyDrop for all variants.  We keep the same layout
// intent but use an enum.
// ---------------------------------------------------------------------------

/// Tuple-change data (INSERT/UPDATE/DELETE/SPEC_INSERT)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RBChangeTp {
    pub rlocator: RelFileLocator,
    pub clear_toast_afterwards: bool,
    pub oldtuple: HeapTuple,
    pub newtuple: HeapTuple,
}

/// Truncate data
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RBChangeTruncate {
    pub nrelids: Size,
    pub cascade: bool,
    pub restart_seqs: bool,
    pub relids: *mut Oid,
}

/// Message data
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RBChangeMsg {
    pub prefix: *mut c_char,
    pub message_size: Size,
    pub message: *mut c_char,
}

/// Invalidation data
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RBChangeInval {
    pub ninvalidations: uint32,
    pub invalidations: *mut SharedInvalidationMessage,
}

/// TupleCID data
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RBChangeTupleCid {
    pub locator: RelFileLocator,
    pub tid: ItemPointerData,
    pub cmin: CommandId,
    pub cmax: CommandId,
    pub combocid: CommandId,
}

/// The union-like data payload of a change (C union -> Rust repr(C) union).
/// All variants are Copy (contain only raw pointers and scalars).
#[repr(C)]
pub union ReorderBufferChangeData {
    pub tp: RBChangeTp,
    pub truncate: RBChangeTruncate,
    pub msg: RBChangeMsg,
    pub snapshot: Snapshot,
    pub command_id: CommandId,
    pub tuplecid: RBChangeTupleCid,
    pub inval: RBChangeInval,
}

// ---------------------------------------------------------------------------
// ReorderBufferChange
// ---------------------------------------------------------------------------

/// A single 'change', can be an insert/update/delete or an internal record.
///
/// Merged from reorderbuffer.h.
#[repr(C)]
pub struct ReorderBufferChange {
    pub lsn: XLogRecPtr,
    pub action: ReorderBufferChangeType,
    pub txn: *mut ReorderBufferTXN,
    pub origin_id: RepOriginId,
    /// Context data - which member is valid depends on action.
    pub data: ReorderBufferChangeData,
    /// While in use: link into a transaction's change list.
    /// Otherwise: link in the preallocated list.
    pub node: dlist_node,
}

// ---------------------------------------------------------------------------
// RBTXN flags (merged from reorderbuffer.h)
// ---------------------------------------------------------------------------
pub const RBTXN_HAS_CATALOG_CHANGES: bits32 = 0x0001;
pub const RBTXN_IS_SUBXACT: bits32 = 0x0002;
pub const RBTXN_IS_SERIALIZED: bits32 = 0x0004;
pub const RBTXN_IS_SERIALIZED_CLEAR: bits32 = 0x0008;
pub const RBTXN_IS_STREAMED: bits32 = 0x0010;
pub const RBTXN_HAS_PARTIAL_CHANGE: bits32 = 0x0020;
pub const RBTXN_IS_PREPARED: bits32 = 0x0040;
pub const RBTXN_SKIPPED_PREPARE: bits32 = 0x0080;
pub const RBTXN_HAS_STREAMABLE_CHANGE: bits32 = 0x0100;
pub const RBTXN_SENT_PREPARE: bits32 = 0x0200;
pub const RBTXN_IS_COMMITTED: bits32 = 0x0400;
pub const RBTXN_IS_ABORTED: bits32 = 0x0800;
pub const RBTXN_DISTR_INVAL_OVERFLOWED: bits32 = 0x1000;
pub const RBTXN_PREPARE_STATUS_MASK: bits32 =
    RBTXN_IS_PREPARED | RBTXN_SKIPPED_PREPARE | RBTXN_SENT_PREPARE;

// Flag accessor inlines (C macros -> Rust inline fns)

#[inline] pub unsafe fn rbtxn_has_catalog_changes(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_HAS_CATALOG_CHANGES != 0
}
#[inline] pub unsafe fn rbtxn_is_known_subxact(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_IS_SUBXACT != 0
}
#[inline] pub unsafe fn rbtxn_is_serialized(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_IS_SERIALIZED != 0
}
#[inline] pub unsafe fn rbtxn_is_serialized_clear(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_IS_SERIALIZED_CLEAR != 0
}
#[inline] pub unsafe fn rbtxn_has_partial_change(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_HAS_PARTIAL_CHANGE != 0
}
#[inline] pub unsafe fn rbtxn_has_streamable_change(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_HAS_STREAMABLE_CHANGE != 0
}
#[inline] pub unsafe fn rbtxn_is_streamed(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_IS_STREAMED != 0
}
#[inline] pub unsafe fn rbtxn_is_prepared(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_IS_PREPARED != 0
}
#[inline] pub unsafe fn rbtxn_sent_prepare(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_SENT_PREPARE != 0
}
#[inline] pub unsafe fn rbtxn_is_committed(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_IS_COMMITTED != 0
}
#[inline] pub unsafe fn rbtxn_is_aborted(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_IS_ABORTED != 0
}
#[inline] pub unsafe fn rbtxn_skip_prepared(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_SKIPPED_PREPARE != 0
}
#[inline] pub unsafe fn rbtxn_distr_inval_overflowed(txn: *const ReorderBufferTXN) -> bool {
    (*txn).txn_flags & RBTXN_DISTR_INVAL_OVERFLOWED != 0
}
#[inline] pub unsafe fn rbtxn_is_toptxn(txn: *const ReorderBufferTXN) -> bool {
    (*txn).toptxn.is_null()
}
#[inline] pub unsafe fn rbtxn_is_subtxn(txn: *const ReorderBufferTXN) -> bool {
    !(*txn).toptxn.is_null()
}
/// Get top-level transaction (or self if already top-level).
#[inline] pub unsafe fn rbtxn_get_toptxn(txn: *mut ReorderBufferTXN) -> *mut ReorderBufferTXN {
    if rbtxn_is_subtxn(txn) { (*txn).toptxn } else { txn }
}

// ---------------------------------------------------------------------------
// TimestampTz alias (real home: utils/timestamp.h)
// ---------------------------------------------------------------------------
pub type TimestampTz = int64;

// ---------------------------------------------------------------------------
// ReorderBufferTXN
// ---------------------------------------------------------------------------

/// xact_time union
#[repr(C)]
pub union RBXactTime {
    pub commit_time: TimestampTz,
    pub prepare_time: TimestampTz,
    pub abort_time: TimestampTz,
}

/// Per-transaction state for the reorder buffer.
///
/// Merged from reorderbuffer.h.
#[repr(C)]
pub struct ReorderBufferTXN {
    pub txn_flags: bits32,
    pub xid: TransactionId,
    pub toplevel_xid: TransactionId,
    pub gid: *mut c_char,
    pub first_lsn: XLogRecPtr,
    pub final_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub toptxn: *mut ReorderBufferTXN,
    pub restart_decoding_lsn: XLogRecPtr,
    pub origin_id: RepOriginId,
    pub origin_lsn: XLogRecPtr,
    pub xact_time: RBXactTime,
    pub base_snapshot: Snapshot,
    pub base_snapshot_lsn: XLogRecPtr,
    pub base_snapshot_node: dlist_node,
    pub snapshot_now: Snapshot,
    pub command_id: CommandId,
    pub nentries: uint64,
    pub nentries_mem: uint64,
    pub changes: dlist_head,
    pub tuplecids: dlist_head,
    pub ntuplecids: uint64,
    pub tuplecid_hash: *mut HTAB,
    pub toast_hash: *mut HTAB,
    pub subtxns: dlist_head,
    pub nsubtxns: uint32,
    pub ninvalidations: uint32,
    pub invalidations: *mut SharedInvalidationMessage,
    pub ninvalidations_distributed: uint32,
    pub invalidations_distributed: *mut SharedInvalidationMessage,
    /// Position in subtxns list or toplevel list
    pub node: dlist_node,
    /// Node in list of catalog-modifying txns
    pub catchange_node: dlist_node,
    /// Node in txn_heap (pairing heap for size ordering)
    pub txn_node: pairingheap_node,
    pub size: Size,
    pub total_size: Size,
    pub output_plugin_private: *mut c_void,
}

// ---------------------------------------------------------------------------
// Callback typedefs (merged from reorderbuffer.h)
// ---------------------------------------------------------------------------

pub type ReorderBufferApplyChangeCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN,
    relation: Relation, change: *mut ReorderBufferChange,
);
pub type ReorderBufferApplyTruncateCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN,
    nrelations: c_int, relations: *mut Relation,
    change: *mut ReorderBufferChange,
);
pub type ReorderBufferBeginCB = unsafe fn(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN);
pub type ReorderBufferCommitCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, commit_lsn: XLogRecPtr,
);
pub type ReorderBufferMessageCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, message_lsn: XLogRecPtr,
    transactional: bool, prefix: *const c_char, sz: Size, message: *const c_char,
);
pub type ReorderBufferBeginPrepareCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN,
);
pub type ReorderBufferPrepareCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, prepare_lsn: XLogRecPtr,
);
pub type ReorderBufferCommitPreparedCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, commit_lsn: XLogRecPtr,
);
pub type ReorderBufferRollbackPreparedCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN,
    prepare_end_lsn: XLogRecPtr, prepare_time: TimestampTz,
);
pub type ReorderBufferStreamStartCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, first_lsn: XLogRecPtr,
);
pub type ReorderBufferStreamStopCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, last_lsn: XLogRecPtr,
);
pub type ReorderBufferStreamAbortCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, abort_lsn: XLogRecPtr,
);
pub type ReorderBufferStreamPrepareCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, prepare_lsn: XLogRecPtr,
);
pub type ReorderBufferStreamCommitCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, commit_lsn: XLogRecPtr,
);
pub type ReorderBufferStreamChangeCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN,
    relation: Relation, change: *mut ReorderBufferChange,
);
pub type ReorderBufferStreamMessageCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, message_lsn: XLogRecPtr,
    transactional: bool, prefix: *const c_char, sz: Size, message: *const c_char,
);
pub type ReorderBufferStreamTruncateCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN,
    nrelations: c_int, relations: *mut Relation, change: *mut ReorderBufferChange,
);
pub type ReorderBufferUpdateProgressTxnCB = unsafe fn(
    rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN, lsn: XLogRecPtr,
);

// ---------------------------------------------------------------------------
// ReorderBuffer (merged from reorderbuffer.h)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct ReorderBuffer {
    pub by_txn: *mut HTAB,
    pub toplevel_by_lsn: dlist_head,
    pub txns_by_base_snapshot_lsn: dlist_head,
    pub catchange_txns: dclist_head,
    pub by_txn_last_xid: TransactionId,
    pub by_txn_last_txn: *mut ReorderBufferTXN,

    // Commit callbacks
    pub begin: Option<ReorderBufferBeginCB>,
    pub apply_change: Option<ReorderBufferApplyChangeCB>,
    pub apply_truncate: Option<ReorderBufferApplyTruncateCB>,
    pub commit: Option<ReorderBufferCommitCB>,
    pub message: Option<ReorderBufferMessageCB>,

    // Prepare callbacks
    pub begin_prepare: Option<ReorderBufferBeginPrepareCB>,
    pub prepare: Option<ReorderBufferPrepareCB>,
    pub commit_prepared: Option<ReorderBufferCommitPreparedCB>,
    pub rollback_prepared: Option<ReorderBufferRollbackPreparedCB>,

    // Streaming callbacks
    pub stream_start: Option<ReorderBufferStreamStartCB>,
    pub stream_stop: Option<ReorderBufferStreamStopCB>,
    pub stream_abort: Option<ReorderBufferStreamAbortCB>,
    pub stream_prepare: Option<ReorderBufferStreamPrepareCB>,
    pub stream_commit: Option<ReorderBufferStreamCommitCB>,
    pub stream_change: Option<ReorderBufferStreamChangeCB>,
    pub stream_message: Option<ReorderBufferStreamMessageCB>,
    pub stream_truncate: Option<ReorderBufferStreamTruncateCB>,

    pub update_progress_txn: Option<ReorderBufferUpdateProgressTxnCB>,

    pub private_data: *mut c_void,
    pub output_rewrites: bool,

    pub context: MemoryContext,
    pub change_context: MemoryContext,
    pub txn_context: MemoryContext,
    pub tup_context: MemoryContext,

    pub current_restart_decoding_lsn: XLogRecPtr,

    pub outbuf: *mut c_char,
    pub outbufsize: Size,
    pub size: Size,

    pub txn_heap: *mut pairingheap,

    // Spill statistics
    pub spillTxns: int64,
    pub spillCount: int64,
    pub spillBytes: int64,

    // Stream statistics
    pub streamTxns: int64,
    pub streamCount: int64,
    pub streamBytes: int64,

    // Total statistics
    pub totalTxns: int64,
    pub totalBytes: int64,
}

// ---------------------------------------------------------------------------
// Module-private helper structures
// ---------------------------------------------------------------------------

/// Entry for xid -> ReorderBufferTXN hash table
#[repr(C)]
pub struct ReorderBufferTXNByIdEnt {
    pub xid: TransactionId,
    pub txn: *mut ReorderBufferTXN,
}

/// (relfilelocator, ctid) => (cmin, cmax) mapping key
#[repr(C)]
pub struct ReorderBufferTupleCidKey {
    pub rlocator: RelFileLocator,
    pub tid: ItemPointerData,
}

/// (relfilelocator, ctid) => (cmin, cmax) mapping entry
#[repr(C)]
pub struct ReorderBufferTupleCidEnt {
    pub key: ReorderBufferTupleCidKey,
    pub cmin: CommandId,
    pub cmax: CommandId,
    pub combocid: CommandId, /* just for debugging */
}

/// Virtual file descriptor with file offset tracking
#[repr(C)]
pub struct TXNEntryFile {
    pub vfd: File, /* -1 when the file is closed */
    pub curOffset: i64, /* offset for next write or read */
}

/// k-way in-order change iteration entry
#[repr(C)]
pub struct ReorderBufferIterTXNEntry {
    pub lsn: XLogRecPtr,
    pub change: *mut ReorderBufferChange,
    pub txn: *mut ReorderBufferTXN,
    pub file: TXNEntryFile,
    pub segno: XLogSegNo,
}

/// k-way merge state for iterating a transaction and its subtransactions
#[repr(C)]
pub struct ReorderBufferIterTXNState {
    pub heap: *mut binaryheap,
    pub nr_txns: Size,
    pub old_change: dlist_head,
    /* entries[nr_txns] follows -- represented as a VLA via raw pointer arithmetic */
}

/// Toast reconstruction entry
#[repr(C)]
pub struct ReorderBufferToastEnt {
    pub chunk_id: Oid,
    pub last_chunk_seq: int32,
    pub num_chunks: Size,
    pub size: Size,
    pub chunks: dlist_head,
    pub reconstructed: *mut varlena,
}

/// On-disk serialized change header
#[repr(C)]
pub struct ReorderBufferDiskChange {
    pub size: Size,
    pub change: ReorderBufferChange,
    /* data follows */
}

/// Heap rewrite mapping file with LSN for sorting
#[repr(C)]
pub struct RewriteMappingFile {
    pub lsn: XLogRecPtr,
    pub fname: [c_char; MAXPGPATH],
}

// ---------------------------------------------------------------------------
// Helper macros (C macros -> inline fns / Rust macros)
// ---------------------------------------------------------------------------

#[inline]
pub fn IsSpecInsert(action: ReorderBufferChangeType) -> bool {
    action == REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT
}

#[inline]
pub fn IsSpecConfirmOrAbort(action: ReorderBufferChangeType) -> bool {
    action == REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM
        || action == REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT
}

#[inline]
pub fn IsInsertOrUpdate(action: ReorderBufferChangeType) -> bool {
    action == REORDER_BUFFER_CHANGE_INSERT
        || action == REORDER_BUFFER_CHANGE_UPDATE
        || action == REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT
}

// ---------------------------------------------------------------------------
// SLAB constants (stubbed until utils/mmgr/slab.c is ported)
// ---------------------------------------------------------------------------
pub const SLAB_DEFAULT_BLOCK_SIZE: Size = 8192;
pub const SLAB_LARGE_BLOCK_SIZE: Size = 8192 * 8;

// dlist_container!, dclist_container!, pairingheap_container! etc. are
// #[macro_export] crate-root macros from ilist.rs / pairingheap.rs - no use needed.

// ---------------------------------------------------------------------------
// Part 1 ends here.  The function implementations follow in subsequent parts.
// ---------------------------------------------------------------------------

// ===========================================================================
// Part 2: Allocation/deallocation and TXNByXid
// ===========================================================================

/*
 * Allocate a new ReorderBuffer and clean out any old serialized state from
 * prior ReorderBuffer instances for the same slot.
 */
pub unsafe fn ReorderBufferAllocate() -> *mut ReorderBuffer {
    let mut hash_ctl = HASHCTL {
        keysize: 0,
        entrysize: 0,
        hcxt: null_mut(),
    };

    Assert!(!MyReplicationSlot.is_null());

    /* allocate memory in own context, to have better accountability */
    let new_ctx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"ReorderBuffer".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    let buffer = mc_alloc(new_ctx, core::mem::size_of::<ReorderBuffer>()) as *mut ReorderBuffer;

    memset(buffer as *mut c_void, 0, core::mem::size_of::<ReorderBuffer>());
    memset(
        &mut hash_ctl as *mut HASHCTL as *mut c_void,
        0,
        core::mem::size_of::<HASHCTL>(),
    );

    (*buffer).context = new_ctx;

    (*buffer).change_context = SlabContextCreate(
        new_ctx,
        "Change",
        SLAB_DEFAULT_BLOCK_SIZE,
        core::mem::size_of::<ReorderBufferChange>(),
    );

    (*buffer).txn_context = SlabContextCreate(
        new_ctx,
        "TXN",
        SLAB_DEFAULT_BLOCK_SIZE,
        core::mem::size_of::<ReorderBufferTXN>(),
    );

    /*
     * To minimize memory fragmentation caused by long-running transactions
     * with changes spanning multiple memory blocks, we use a single
     * fixed-size memory block for decoded tuple storage.
     */
    (*buffer).tup_context = GenerationContextCreate(
        new_ctx,
        "Tuples",
        SLAB_DEFAULT_BLOCK_SIZE,
        SLAB_DEFAULT_BLOCK_SIZE,
        SLAB_DEFAULT_BLOCK_SIZE,
    );

    hash_ctl.keysize = core::mem::size_of::<TransactionId>();
    hash_ctl.entrysize = core::mem::size_of::<ReorderBufferTXNByIdEnt>();
    hash_ctl.hcxt = (*buffer).context;

    (*buffer).by_txn = hash_create(
        "ReorderBufferByXid",
        1000,
        &mut hash_ctl as *mut HASHCTL,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    (*buffer).by_txn_last_xid = InvalidTransactionId;
    (*buffer).by_txn_last_txn = null_mut();

    (*buffer).outbuf = null_mut();
    (*buffer).outbufsize = 0;
    (*buffer).size = 0;

    /* txn_heap is ordered by transaction size */
    (*buffer).txn_heap = pairingheap_allocate(
        ReorderBufferTXNSizeCompare,
        null_mut(),
    );

    (*buffer).spillTxns = 0;
    (*buffer).spillCount = 0;
    (*buffer).spillBytes = 0;
    (*buffer).streamTxns = 0;
    (*buffer).streamCount = 0;
    (*buffer).streamBytes = 0;
    (*buffer).totalTxns = 0;
    (*buffer).totalBytes = 0;

    (*buffer).current_restart_decoding_lsn = InvalidXLogRecPtr;

    dlist_init(&mut (*buffer).toplevel_by_lsn);
    dlist_init(&mut (*buffer).txns_by_base_snapshot_lsn);
    dclist_init(&mut (*buffer).catchange_txns);

    /*
     * Ensure there's no stale data from prior uses of this slot, in case some
     * prior exit avoided calling ReorderBufferFree.
     */
    let name_ptr = NameStr!((*MyReplicationSlot).data.name);
    ReorderBufferCleanupSerializedTXNs_cstr(name_ptr);

    buffer
}

/*
 * Free a ReorderBuffer
 */
pub unsafe fn ReorderBufferFree(rb: *mut ReorderBuffer) {
    let context = (*rb).context;

    /*
     * We free separately allocated data by entirely scrapping reorderbuffer's
     * memory context.
     */
    MemoryContextDelete(context);

    /* Free disk space used by unconsumed reorder buffers */
    let name_ptr = NameStr!((*MyReplicationSlot).data.name);
    ReorderBufferCleanupSerializedTXNs_cstr(name_ptr);
}

/*
 * Allocate a new ReorderBufferTXN.
 */
unsafe fn ReorderBufferAllocTXN(rb: *mut ReorderBuffer) -> *mut ReorderBufferTXN {
    let txn = mc_alloc((*rb).txn_context, core::mem::size_of::<ReorderBufferTXN>())
        as *mut ReorderBufferTXN;

    memset(txn as *mut c_void, 0, core::mem::size_of::<ReorderBufferTXN>());

    dlist_init(&mut (*txn).changes);
    dlist_init(&mut (*txn).tuplecids);
    dlist_init(&mut (*txn).subtxns);

    /* InvalidCommandId is not zero, so set it explicitly */
    (*txn).command_id = InvalidCommandId;
    (*txn).output_plugin_private = null_mut();

    txn
}

/*
 * Free a ReorderBufferTXN.
 */
unsafe fn ReorderBufferFreeTXN(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    /* clean the lookup cache if we were cached (quite likely) */
    if (*rb).by_txn_last_xid == (*txn).xid {
        (*rb).by_txn_last_xid = InvalidTransactionId;
        (*rb).by_txn_last_txn = null_mut();
    }

    /* free data that's contained */

    if !(*txn).gid.is_null() {
        pfree((*txn).gid as *mut c_void);
        (*txn).gid = null_mut();
    }

    if !(*txn).tuplecid_hash.is_null() {
        hash_destroy((*txn).tuplecid_hash);
        (*txn).tuplecid_hash = null_mut();
    }

    if !(*txn).invalidations.is_null() {
        pfree((*txn).invalidations as *mut c_void);
        (*txn).invalidations = null_mut();
    }

    if !(*txn).invalidations_distributed.is_null() {
        pfree((*txn).invalidations_distributed as *mut c_void);
        (*txn).invalidations_distributed = null_mut();
    }

    /* Reset the toast hash */
    ReorderBufferToastReset(rb, txn);

    /* All changes must be deallocated */
    Assert!((*txn).size == 0);

    pfree(txn as *mut c_void);
}

/*
 * Allocate a ReorderBufferChange.
 */
pub unsafe fn ReorderBufferAllocChange(rb: *mut ReorderBuffer) -> *mut ReorderBufferChange {
    let change = mc_alloc(
        (*rb).change_context,
        core::mem::size_of::<ReorderBufferChange>(),
    ) as *mut ReorderBufferChange;

    memset(change as *mut c_void, 0, core::mem::size_of::<ReorderBufferChange>());
    change
}

/*
 * Free a ReorderBufferChange and update memory accounting, if requested.
 */
pub unsafe fn ReorderBufferFreeChange(
    rb: *mut ReorderBuffer,
    change: *mut ReorderBufferChange,
    upd_mem: bool,
) {
    /* update memory accounting info */
    if upd_mem {
        ReorderBufferChangeMemoryUpdate(
            rb,
            change,
            null_mut(),
            false,
            ReorderBufferChangeSize(change),
        );
    }

    /* free contained data */
    match (*change).action {
        REORDER_BUFFER_CHANGE_INSERT
        | REORDER_BUFFER_CHANGE_UPDATE
        | REORDER_BUFFER_CHANGE_DELETE
        | REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT => {
            if !(*change).data.tp.newtuple.is_null() {
                ReorderBufferFreeTupleBuf((*change).data.tp.newtuple);
                (*change).data.tp.newtuple = null_mut();
            }
            if !(*change).data.tp.oldtuple.is_null() {
                ReorderBufferFreeTupleBuf((*change).data.tp.oldtuple);
                (*change).data.tp.oldtuple = null_mut();
            }
        }
        REORDER_BUFFER_CHANGE_MESSAGE => {
            if !(*change).data.msg.prefix.is_null() {
                pfree((*change).data.msg.prefix as *mut c_void);
            }
            (*change).data.msg.prefix = null_mut();
            if !(*change).data.msg.message.is_null() {
                pfree((*change).data.msg.message as *mut c_void);
            }
            (*change).data.msg.message = null_mut();
        }
        REORDER_BUFFER_CHANGE_INVALIDATION => {
            if !(*change).data.inval.invalidations.is_null() {
                pfree((*change).data.inval.invalidations as *mut c_void);
            }
            (*change).data.inval.invalidations = null_mut();
        }
        REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT => {
            if !(*change).data.snapshot.is_null() {
                ReorderBufferFreeSnap(rb, (*change).data.snapshot);
                (*change).data.snapshot = null_mut();
            }
        }
        REORDER_BUFFER_CHANGE_TRUNCATE => {
            if !(*change).data.truncate.relids.is_null() {
                ReorderBufferFreeRelids(rb, (*change).data.truncate.relids);
                (*change).data.truncate.relids = null_mut();
            }
        }
        REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM
        | REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT
        | REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID
        | REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID => {}
    }

    pfree(change as *mut c_void);
}

/*
 * Allocate a HeapTuple fitting a tuple of size tuple_len
 * (excluding header overhead).
 */
pub unsafe fn ReorderBufferAllocTupleBuf(rb: *mut ReorderBuffer, tuple_len: Size) -> HeapTuple {
    let alloc_len = tuple_len + SizeofHeapTupleHeader;

    let tuple = mc_alloc((*rb).tup_context, HEAPTUPLESIZE + alloc_len) as HeapTuple;

    (*tuple).t_len = alloc_len as uint32;
    (*tuple).t_data = (tuple as *mut c_char).add(HEAPTUPLESIZE) as HeapTupleHeader;

    tuple
}

/*
 * Free a HeapTuple returned by ReorderBufferAllocTupleBuf().
 */
pub unsafe fn ReorderBufferFreeTupleBuf(tuple: HeapTuple) {
    pfree(tuple as *mut c_void);
}

/*
 * Allocate an array for relids of truncated relations.
 */
pub unsafe fn ReorderBufferAllocRelids(rb: *mut ReorderBuffer, nrelids: c_int) -> *mut Oid {
    let alloc_len = core::mem::size_of::<Oid>() * nrelids as usize;
    mc_alloc((*rb).context, alloc_len) as *mut Oid
}

/*
 * Free an array of relids.
 */
pub unsafe fn ReorderBufferFreeRelids(_rb: *mut ReorderBuffer, relids: *mut Oid) {
    pfree(relids as *mut c_void);
}

/*
 * Return the ReorderBufferTXN from the given buffer, specified by Xid.
 * If create is true, and a transaction doesn't already exist, create it
 * (with the given LSN, and as top transaction if that's specified);
 * when this happens, is_new is set to true.
 */
unsafe fn ReorderBufferTXNByXid(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    create: bool,
    is_new: *mut bool,
    lsn: XLogRecPtr,
    create_as_top: bool,
) -> *mut ReorderBufferTXN {
    let txn: *mut ReorderBufferTXN;
    let mut found: bool = false;

    Assert!(TransactionIdIsValid(xid));

    /*
     * Check the one-entry lookup cache first
     */
    if TransactionIdIsValid((*rb).by_txn_last_xid) && (*rb).by_txn_last_xid == xid {
        txn = (*rb).by_txn_last_txn;

        if !txn.is_null() {
            /* found it, and it's valid */
            if !is_new.is_null() {
                *is_new = false;
            }
            return txn;
        }

        /*
         * cached as non-existent, and asked not to create?
         */
        if !create {
            return null_mut();
        }
        /* otherwise fall through to create it */
    }

    /* search the lookup table */
    let ent = hash_search(
        (*rb).by_txn,
        &xid as *const TransactionId as *const c_void,
        if create { HASH_ENTER } else { HASH_FIND },
        &mut found as *mut bool,
    ) as *mut ReorderBufferTXNByIdEnt;

    let txn: *mut ReorderBufferTXN;
    if found {
        txn = (*ent).txn;
    } else if create {
        /* initialize the new entry, if creation was requested */
        Assert!(!ent.is_null());
        Assert!(lsn != InvalidXLogRecPtr);

        (*ent).txn = ReorderBufferAllocTXN(rb);
        (*ent).txn_mut().xid = xid;
        txn = (*ent).txn;
        (*txn).first_lsn = lsn;
        (*txn).restart_decoding_lsn = (*rb).current_restart_decoding_lsn;

        if create_as_top {
            dlist_push_tail(&mut (*rb).toplevel_by_lsn, &mut (*txn).node);
            AssertTXNLsnOrder(rb);
        }
    } else {
        txn = null_mut(); /* not found and not asked to create */
    }

    /* update cache */
    (*rb).by_txn_last_xid = xid;
    (*rb).by_txn_last_txn = txn;

    if !is_new.is_null() {
        *is_new = !found;
    }

    Assert!(!create || !txn.is_null());
    txn
}

// Helper to get mutable txn reference from hash entry
impl ReorderBufferTXNByIdEnt {
    #[inline]
    unsafe fn txn_mut(&mut self) -> &mut ReorderBufferTXN {
        &mut *self.txn
    }
}

// ===========================================================================
// Part 3: Queue/process changes, assert helpers, LSN ordering, iterators
// ===========================================================================

/*
 * AssertTXNLsnOrder
 *   Verify LSN ordering of transaction lists in the reorderbuffer.
 *   No-op if assertions are not in use.
 */
unsafe fn AssertTXNLsnOrder(rb: *mut ReorderBuffer) {
    #[cfg(debug_assertions)]
    {
        let ctx = (*rb).private_data as *mut LogicalDecodingContext;
        let mut prev_first_lsn: XLogRecPtr = InvalidXLogRecPtr;
        let mut prev_base_snap_lsn: XLogRecPtr = InvalidXLogRecPtr;

        /* Skip verification before start_decoding_at LSN */
        if SnapBuildXactNeedsSkip((*ctx).snapshot_builder, (*(*ctx).reader).EndRecPtr) {
            return;
        }

        let mut iter: dlist_iter = core::mem::zeroed();
        dlist_foreach!(iter, &mut (*rb).toplevel_by_lsn, {
            let cur_txn = dlist_container!(ReorderBufferTXN, node, iter.cur);
            Assert!((*cur_txn).first_lsn != InvalidXLogRecPtr);
            if (*cur_txn).end_lsn != InvalidXLogRecPtr {
                Assert!((*cur_txn).first_lsn <= (*cur_txn).end_lsn);
            }
            if prev_first_lsn != InvalidXLogRecPtr {
                Assert!(prev_first_lsn < (*cur_txn).first_lsn);
            }
            Assert!(!rbtxn_is_known_subxact(cur_txn));
            prev_first_lsn = (*cur_txn).first_lsn;
        });

        let mut iter2: dlist_iter = core::mem::zeroed();
        dlist_foreach!(iter2, &mut (*rb).txns_by_base_snapshot_lsn, {
            let cur_txn = dlist_container!(ReorderBufferTXN, base_snapshot_node, iter2.cur);
            Assert!(!(*cur_txn).base_snapshot.is_null());
            Assert!((*cur_txn).base_snapshot_lsn != InvalidXLogRecPtr);
            if prev_base_snap_lsn != InvalidXLogRecPtr {
                Assert!(prev_base_snap_lsn < (*cur_txn).base_snapshot_lsn);
            }
            Assert!(!rbtxn_is_known_subxact(cur_txn));
            prev_base_snap_lsn = (*cur_txn).base_snapshot_lsn;
        });
    }
}

/*
 * AssertChangeLsnOrder
 *   Check ordering of changes in the (sub)transaction.
 */
unsafe fn AssertChangeLsnOrder(txn: *mut ReorderBufferTXN) {
    #[cfg(debug_assertions)]
    {
        let mut prev_lsn: XLogRecPtr = (*txn).first_lsn;
        let mut iter: dlist_iter = core::mem::zeroed();
        dlist_foreach!(iter, &mut (*txn).changes, {
            let cur_change = dlist_container!(ReorderBufferChange, node, iter.cur);
            Assert!((*txn).first_lsn != InvalidXLogRecPtr);
            Assert!((*cur_change).lsn != InvalidXLogRecPtr);
            Assert!((*txn).first_lsn <= (*cur_change).lsn);
            if (*txn).end_lsn != InvalidXLogRecPtr {
                Assert!((*cur_change).lsn <= (*txn).end_lsn);
            }
            Assert!(prev_lsn <= (*cur_change).lsn);
            prev_lsn = (*cur_change).lsn;
        });
    }
}

/*
 * ReorderBufferGetOldestTXN
 *   Return oldest transaction in reorderbuffer
 */
pub unsafe fn ReorderBufferGetOldestTXN(rb: *mut ReorderBuffer) -> *mut ReorderBufferTXN {
    AssertTXNLsnOrder(rb);

    if dlist_is_empty(&(*rb).toplevel_by_lsn) {
        return null_mut();
    }

    let txn = dlist_head_element!(ReorderBufferTXN, node, &mut (*rb).toplevel_by_lsn);
    Assert!(!rbtxn_is_known_subxact(txn));
    Assert!((*txn).first_lsn != InvalidXLogRecPtr);
    txn
}

/*
 * ReorderBufferGetOldestXmin
 *   Return oldest Xmin in reorderbuffer
 */
pub unsafe fn ReorderBufferGetOldestXmin(rb: *mut ReorderBuffer) -> TransactionId {
    AssertTXNLsnOrder(rb);

    if dlist_is_empty(&(*rb).txns_by_base_snapshot_lsn) {
        return InvalidTransactionId;
    }

    let txn = dlist_head_element!(
        ReorderBufferTXN,
        base_snapshot_node,
        &mut (*rb).txns_by_base_snapshot_lsn
    );
    (*(*txn).base_snapshot).xmin
}

pub unsafe fn ReorderBufferSetRestartPoint(rb: *mut ReorderBuffer, ptr: XLogRecPtr) {
    (*rb).current_restart_decoding_lsn = ptr;
}

/*
 * ReorderBufferAssignChild
 *   Make note that subxid is a subtransaction of xid.
 */
pub unsafe fn ReorderBufferAssignChild(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    subxid: TransactionId,
    lsn: XLogRecPtr,
) {
    let mut new_top: bool = false;
    let mut new_sub: bool = false;

    let txn = ReorderBufferTXNByXid(rb, xid, true, &mut new_top, lsn, true);
    let subtxn = ReorderBufferTXNByXid(rb, subxid, true, &mut new_sub, lsn, false);

    if !new_sub {
        if rbtxn_is_known_subxact(subtxn) {
            /* already associated, nothing to do */
            return;
        } else {
            /*
             * We already saw this transaction, but initially added it to the
             * list of top-level txns.  Now that we know it's not top-level,
             * remove it from there.
             */
            dlist_delete(&mut (*subtxn).node);
        }
    }

    (*subtxn).txn_flags |= RBTXN_IS_SUBXACT;
    (*subtxn).toplevel_xid = xid;
    Assert!((*subtxn).nsubtxns == 0);

    /* set the reference to top-level transaction */
    (*subtxn).toptxn = txn;

    /* add to subtransaction list */
    dlist_push_tail(&mut (*txn).subtxns, &mut (*subtxn).node);
    (*txn).nsubtxns += 1;

    /* Possibly transfer the subtxn's snapshot to its top-level txn. */
    ReorderBufferTransferSnapToParent(txn, subtxn);

    /* Verify LSN-ordering invariant */
    AssertTXNLsnOrder(rb);
}

/*
 * ReorderBufferTransferSnapToParent
 *   Transfer base snapshot from subtxn to top-level txn, if needed.
 */
unsafe fn ReorderBufferTransferSnapToParent(
    txn: *mut ReorderBufferTXN,
    subtxn: *mut ReorderBufferTXN,
) {
    Assert!((*subtxn).toplevel_xid == (*txn).xid);

    if !(*subtxn).base_snapshot.is_null() {
        if (*txn).base_snapshot.is_null()
            || (*subtxn).base_snapshot_lsn < (*txn).base_snapshot_lsn
        {
            /*
             * If the toplevel transaction already has a base snapshot but
             * it's newer than the subxact's, purge it.
             */
            if !(*txn).base_snapshot.is_null() {
                SnapBuildSnapDecRefcount((*txn).base_snapshot);
                dlist_delete(&mut (*txn).base_snapshot_node);
            }

            /*
             * The snapshot is now the top transaction's; transfer it, and
             * adjust the list position of the top transaction in the list.
             */
            (*txn).base_snapshot = (*subtxn).base_snapshot;
            (*txn).base_snapshot_lsn = (*subtxn).base_snapshot_lsn;
            dlist_insert_before(&mut (*subtxn).base_snapshot_node, &mut (*txn).base_snapshot_node);

            /* The subtransaction doesn't have a snapshot anymore. */
            (*subtxn).base_snapshot = null_mut();
            (*subtxn).base_snapshot_lsn = InvalidXLogRecPtr;
            dlist_delete(&mut (*subtxn).base_snapshot_node);
        } else {
            /* Base snap of toplevel is fine, so subxact's is not needed */
            SnapBuildSnapDecRefcount((*subtxn).base_snapshot);
            dlist_delete(&mut (*subtxn).base_snapshot_node);
            (*subtxn).base_snapshot = null_mut();
            (*subtxn).base_snapshot_lsn = InvalidXLogRecPtr;
        }
    }
}

/*
 * Associate a subtransaction with its toplevel transaction at commit time.
 */
pub unsafe fn ReorderBufferCommitChild(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    subxid: TransactionId,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
) {
    let subtxn = ReorderBufferTXNByXid(rb, subxid, false, null_mut(), InvalidXLogRecPtr, false);

    /* No need to do anything if that subtxn didn't contain any changes */
    if subtxn.is_null() {
        return;
    }

    (*subtxn).final_lsn = commit_lsn;
    (*subtxn).end_lsn = end_lsn;

    /*
     * Assign this subxact as a child of the toplevel xact (no-op if already done.)
     */
    ReorderBufferAssignChild(rb, xid, subxid, InvalidXLogRecPtr);
}

/*
 * Binary heap comparison function for the k-way merge iterator.
 */
unsafe fn ReorderBufferIterCompare(a: Datum, b: Datum, arg: *mut c_void) -> c_int {
    let state = arg as *mut ReorderBufferIterTXNState;
    // entries array starts right after the state struct
    let entries = (state as *mut u8).add(core::mem::size_of::<ReorderBufferIterTXNState>())
        as *mut ReorderBufferIterTXNEntry;
    let pos_a = (*entries.add(a as usize)).lsn;
    let pos_b = (*entries.add(b as usize)).lsn;

    if pos_a < pos_b {
        1
    } else if pos_a == pos_b {
        0
    } else {
        -1
    }
}

/*
 * Allocate & initialize an iterator which iterates in lsn order over a
 * transaction and all its subtransactions.
 */
unsafe fn ReorderBufferIterTXNInit(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    iter_state: *mut *mut ReorderBufferIterTXNState,
) {
    let mut nr_txns: Size = 0;
    *iter_state = null_mut();

    /* Check ordering of changes in the toplevel transaction. */
    AssertChangeLsnOrder(txn);

    /* Count transactions with changes */
    if (*txn).nentries > 0 {
        nr_txns += 1;
    }

    let mut cur_txn_i: dlist_iter = core::mem::zeroed();
    dlist_foreach!(cur_txn_i, &mut (*txn).subtxns, {
        let cur_txn = dlist_container!(ReorderBufferTXN, node, cur_txn_i.cur);
        AssertChangeLsnOrder(cur_txn);
        if (*cur_txn).nentries > 0 {
            nr_txns += 1;
        }
    });

    /* allocate iteration state */
    let state_size = core::mem::size_of::<ReorderBufferIterTXNState>()
        + core::mem::size_of::<ReorderBufferIterTXNEntry>() * nr_txns;
    let state = mc_alloc_zero((*rb).context, state_size) as *mut ReorderBufferIterTXNState;

    (*state).nr_txns = nr_txns;
    dlist_init(&mut (*state).old_change);

    let entries = (state as *mut u8).add(core::mem::size_of::<ReorderBufferIterTXNState>())
        as *mut ReorderBufferIterTXNEntry;
    for off in 0..nr_txns {
        (*entries.add(off)).file.vfd = -1;
        (*entries.add(off)).segno = 0;
    }

    /* allocate heap */
    (*state).heap = binaryheap_allocate(
        nr_txns as c_int,
        ReorderBufferIterCompare,
        state as *mut c_void,
    );

    /* Now that the state fields are initialized, it is safe to return it. */
    *iter_state = state;

    let mut off: c_int = 0;

    /* add toplevel transaction if it contains changes */
    if (*txn).nentries > 0 {
        if rbtxn_is_serialized(txn) {
            /* serialize remaining changes */
            ReorderBufferSerializeTXN(rb, txn);
            ReorderBufferRestoreChanges(rb, txn, &mut (*entries.add(off as usize)).file,
                                        &mut (*entries.add(off as usize)).segno);
        }

        let cur_change = dlist_head_element!(ReorderBufferChange, node, &mut (*txn).changes);

        (*entries.add(off as usize)).lsn = (*cur_change).lsn;
        (*entries.add(off as usize)).change = cur_change;
        (*entries.add(off as usize)).txn = txn;

        binaryheap_add_unordered((*state).heap, off as Datum);
        off += 1;
    }

    /* add subtransactions if they contain changes */
    let mut cur_txn_i: dlist_iter = core::mem::zeroed();
    dlist_foreach!(cur_txn_i, &mut (*txn).subtxns, {
        let cur_txn = dlist_container!(ReorderBufferTXN, node, cur_txn_i.cur);
        if (*cur_txn).nentries > 0 {
            if rbtxn_is_serialized(cur_txn) {
                ReorderBufferSerializeTXN(rb, cur_txn);
                ReorderBufferRestoreChanges(rb, cur_txn,
                                            &mut (*entries.add(off as usize)).file,
                                            &mut (*entries.add(off as usize)).segno);
            }
            let cur_change = dlist_head_element!(ReorderBufferChange, node, &mut (*cur_txn).changes);
            (*entries.add(off as usize)).lsn = (*cur_change).lsn;
            (*entries.add(off as usize)).change = cur_change;
            (*entries.add(off as usize)).txn = cur_txn;
            binaryheap_add_unordered((*state).heap, off as Datum);
            off += 1;
        }
    });

    /* assemble a valid binary heap */
    binaryheap_build((*state).heap);
}

/*
 * Return the next change when iterating over a transaction and its
 * subtransactions.  Returns NULL when no further changes exist.
 */
unsafe fn ReorderBufferIterTXNNext(
    rb: *mut ReorderBuffer,
    state: *mut ReorderBufferIterTXNState,
) -> *mut ReorderBufferChange {
    /* nothing there anymore */
    if (*(*state).heap).bh_size == 0 {
        return null_mut();
    }

    let off = binaryheap_first((*state).heap) as usize;
    let entries = (state as *mut u8).add(core::mem::size_of::<ReorderBufferIterTXNState>())
        as *mut ReorderBufferIterTXNEntry;
    let entry = entries.add(off);

    /* free memory we might have "leaked" in the previous *Next call */
    if !dlist_is_empty(&(*state).old_change) {
        let change = dlist_container!(
            ReorderBufferChange,
            node,
            dlist_pop_head_node(&mut (*state).old_change)
        );
        ReorderBufferFreeChange(rb, change, true);
        Assert!(dlist_is_empty(&(*state).old_change));
    }

    let change = (*entry).change;

    /* there are in-memory changes */
    if dlist_has_next(&(*(*entry).txn).changes, &(*change).node) {
        let next = dlist_next_node(&mut (*(*entry).txn).changes as *mut dlist_head, &mut (*change).node);
        let next_change = dlist_container!(ReorderBufferChange, node, next);

        (*entry).lsn = (*next_change).lsn;
        (*entry).change = next_change;

        binaryheap_replace_first((*state).heap, off as Datum);
        return change;
    }

    /* try to load changes from disk */
    if (*(*entry).txn).nentries != (*(*entry).txn).nentries_mem {
        /*
         * Ugly: restoring changes will reuse *Change records, thus delete the
         * current one from the per-tx list and only free in the next call.
         */
        dlist_delete(&mut (*change).node);
        dlist_push_tail(&mut (*state).old_change, &mut (*change).node);

        /* Update the total bytes processed */
        (*rb).totalBytes += (*(*entry).txn).size as int64;
        if ReorderBufferRestoreChanges(rb, (*entry).txn, &mut (*entry).file,
                                        &mut (*entry).segno) > 0 {
            /* successfully restored changes from disk */
            let next_change = dlist_head_element!(
                ReorderBufferChange,
                node,
                &mut (*(*entry).txn).changes
            );

            elog!(DEBUG2, "restored {}/{} changes from disk",
                 (*(*entry).txn).nentries_mem as uint32,
                 (*(*entry).txn).nentries as uint32);

            Assert!((*(*entry).txn).nentries_mem > 0);
            (*entry).lsn = (*next_change).lsn;
            (*entry).change = next_change;
            binaryheap_replace_first((*state).heap, off as Datum);

            return change;
        }
    }

    /* ok, no changes there anymore, remove */
    binaryheap_remove_first((*state).heap);

    change
}

/*
 * Deallocate the iterator
 */
unsafe fn ReorderBufferIterTXNFinish(
    rb: *mut ReorderBuffer,
    state: *mut ReorderBufferIterTXNState,
) {
    let entries = (state as *mut u8).add(core::mem::size_of::<ReorderBufferIterTXNState>())
        as *mut ReorderBufferIterTXNEntry;

    for off in 0..(*state).nr_txns {
        if (*entries.add(off)).file.vfd != -1 {
            FileClose((*entries.add(off)).file.vfd);
        }
    }

    /* free memory we might have "leaked" in the last *Next call */
    if !dlist_is_empty(&(*state).old_change) {
        let change = dlist_container!(
            ReorderBufferChange,
            node,
            dlist_pop_head_node(&mut (*state).old_change)
        );
        ReorderBufferFreeChange(rb, change, true);
        Assert!(dlist_is_empty(&(*state).old_change));
    }

    binaryheap_free((*state).heap);
    pfree(state as *mut c_void);
}

// ===========================================================================
// Part 4: Cleanup, truncate, build-tuplecid-hash, copy/free snap,
//         stream-commit, processpartialchange, queuechange, queuemessage
// ===========================================================================

/*
 * Cleanup the contents of a transaction, usually after the transaction
 * committed or aborted.
 */
unsafe fn ReorderBufferCleanupTXN(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    let mut found: bool = false;
    let mut mem_freed: Size = 0;

    /* cleanup subtransactions & their changes */
    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter, &mut (*txn).subtxns, {
        let subtxn = dlist_container!(ReorderBufferTXN, node, iter.cur);
        Assert!(rbtxn_is_known_subxact(subtxn));
        Assert!((*subtxn).nsubtxns == 0);
        ReorderBufferCleanupTXN(rb, subtxn);
    });

    /* cleanup changes in the txn */
    let mut iter2: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter2, &mut (*txn).changes, {
        let change = dlist_container!(ReorderBufferChange, node, iter2.cur);
        Assert!((*change).txn == txn);
        mem_freed += ReorderBufferChangeSize(change);
        ReorderBufferFreeChange(rb, change, false);
    });

    /* Update the memory counter */
    ReorderBufferChangeMemoryUpdate(rb, null_mut(), txn, false, mem_freed);

    /* Cleanup the tuplecids */
    let mut iter3: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter3, &mut (*txn).tuplecids, {
        let change = dlist_container!(ReorderBufferChange, node, iter3.cur);
        Assert!((*change).txn == txn);
        Assert!((*change).action == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);
        ReorderBufferFreeChange(rb, change, true);
    });

    /* Cleanup the base snapshot, if set. */
    if !(*txn).base_snapshot.is_null() {
        SnapBuildSnapDecRefcount((*txn).base_snapshot);
        dlist_delete(&mut (*txn).base_snapshot_node);
    }

    /* Cleanup the snapshot for the last streamed run. */
    if !(*txn).snapshot_now.is_null() {
        Assert!(rbtxn_is_streamed(txn));
        ReorderBufferFreeSnap(rb, (*txn).snapshot_now);
    }

    /*
     * Remove TXN from its containing lists.
     */
    dlist_delete(&mut (*txn).node);
    if rbtxn_has_catalog_changes(txn) {
        dclist_delete_from(&mut (*rb).catchange_txns, &mut (*txn).catchange_node);
    }

    /* now remove reference from buffer */
    hash_search(
        (*rb).by_txn,
        &(*txn).xid as *const TransactionId as *const c_void,
        HASH_REMOVE,
        &mut found as *mut bool,
    );
    Assert!(found);

    /* remove entries spilled to disk */
    if rbtxn_is_serialized(txn) {
        ReorderBufferRestoreCleanup(rb, txn);
    }

    /* deallocate */
    ReorderBufferFreeTXN(rb, txn);
}

/*
 * Discard changes from a transaction (and subtransactions), after streaming,
 * decoding at PREPARE, or detecting transaction abort.
 */
unsafe fn ReorderBufferTruncateTXN(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    txn_prepared: bool,
) {
    let mut mem_freed: Size = 0;

    /* cleanup subtransactions & their changes */
    let mut iter: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter, &mut (*txn).subtxns, {
        let subtxn = dlist_container!(ReorderBufferTXN, node, iter.cur);
        Assert!(rbtxn_is_known_subxact(subtxn));
        Assert!((*subtxn).nsubtxns == 0);
        ReorderBufferMaybeMarkTXNStreamed(rb, subtxn);
        ReorderBufferTruncateTXN(rb, subtxn, txn_prepared);
    });

    /* cleanup changes in the txn */
    let mut iter2: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(iter2, &mut (*txn).changes, {
        let change = dlist_container!(ReorderBufferChange, node, iter2.cur);
        Assert!((*change).txn == txn);
        dlist_delete(&mut (*change).node);
        mem_freed += ReorderBufferChangeSize(change);
        ReorderBufferFreeChange(rb, change, false);
    });

    /* Update the memory counter */
    ReorderBufferChangeMemoryUpdate(rb, null_mut(), txn, false, mem_freed);

    if txn_prepared {
        /* cleanup tuplecids for prepared txns */
        let mut iter3: dlist_mutable_iter = core::mem::zeroed();
        dlist_foreach_modify!(iter3, &mut (*txn).tuplecids, {
            let change = dlist_container!(ReorderBufferChange, node, iter3.cur);
            Assert!((*change).txn == txn);
            Assert!((*change).action == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);
            dlist_delete(&mut (*change).node);
            ReorderBufferFreeChange(rb, change, true);
        });
    }

    /* Destroy the (relfilelocator, ctid) hashtable */
    if !(*txn).tuplecid_hash.is_null() {
        hash_destroy((*txn).tuplecid_hash);
        (*txn).tuplecid_hash = null_mut();
    }

    /* If this txn is serialized then clean the disk space. */
    if rbtxn_is_serialized(txn) {
        ReorderBufferRestoreCleanup(rb, txn);
        (*txn).txn_flags &= !RBTXN_IS_SERIALIZED;
        /*
         * We set this flag to indicate if the transaction is ever serialized.
         * We need this to accurately update the stats.
         */
        (*txn).txn_flags |= RBTXN_IS_SERIALIZED_CLEAR;
    }

    /* also reset the number of entries in the transaction */
    (*txn).nentries_mem = 0;
    (*txn).nentries = 0;
}

/*
 * Check the transaction status by CLOG lookup and discard all changes if
 * the transaction is aborted.
 * Return true if the transaction is aborted.
 */
unsafe fn ReorderBufferCheckAndTruncateAbortedTXN(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
) -> bool {
    /* Quick return for regression tests */
    if unlikely(debug_logical_replication_streaming == DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE) {
        return false;
    }

    if rbtxn_is_committed(txn) {
        return false;
    }
    if rbtxn_is_aborted(txn) {
        /* Already-aborted transactions should not have any changes */
        Assert!((*txn).size == 0);
        return true;
    }

    /* Otherwise, check the transaction status using CLOG lookup */
    if TransactionIdIsInProgress((*txn).xid) {
        return false;
    }

    if TransactionIdDidCommit((*txn).xid) {
        Assert!(!rbtxn_is_aborted(txn));
        (*txn).txn_flags |= RBTXN_IS_COMMITTED;
        return false;
    }

    /* The transaction aborted. */
    ReorderBufferTruncateTXN(rb, txn, rbtxn_is_prepared(txn));
    ReorderBufferToastReset(rb, txn);

    Assert!((*txn).size == 0);

    Assert!(!rbtxn_is_committed(txn));
    (*txn).txn_flags |= RBTXN_IS_ABORTED;

    true
}

/// Rust equivalent of `unlikely()` hint
#[inline(always)]
fn unlikely(b: bool) -> bool { b }

/*
 * Build a hash with (relfilelocator, ctid) -> (cmin, cmax) mapping.
 */
unsafe fn ReorderBufferBuildTupleCidHash(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    if !rbtxn_has_catalog_changes(txn) || dlist_is_empty(&(*txn).tuplecids) {
        return;
    }

    let mut hash_ctl = HASHCTL {
        keysize: core::mem::size_of::<ReorderBufferTupleCidKey>(),
        entrysize: core::mem::size_of::<ReorderBufferTupleCidEnt>(),
        hcxt: (*rb).context,
    };

    /* create the hash with the exact number of to-be-stored tuplecids */
    (*txn).tuplecid_hash = hash_create(
        "ReorderBufferTupleCid",
        (*txn).ntuplecids as c_long,
        &mut hash_ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    let mut iter: dlist_iter = core::mem::zeroed();
    dlist_foreach!(iter, &mut (*txn).tuplecids, {
        let change = dlist_container!(ReorderBufferChange, node, iter.cur);
        Assert!((*change).action == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID);

        let mut key: ReorderBufferTupleCidKey = core::mem::zeroed();
        /* be careful about padding */
        memset(&mut key as *mut ReorderBufferTupleCidKey as *mut c_void, 0,
               core::mem::size_of::<ReorderBufferTupleCidKey>());

        key.rlocator = (*change).data.tuplecid.locator;
        ItemPointerCopy(&(*change).data.tuplecid.tid, &mut key.tid);

        let mut found: bool = false;
        let ent = hash_search(
            (*txn).tuplecid_hash,
            &key as *const ReorderBufferTupleCidKey as *const c_void,
            HASH_ENTER,
            &mut found as *mut bool,
        ) as *mut ReorderBufferTupleCidEnt;

        if !found {
            (*ent).cmin = (*change).data.tuplecid.cmin;
            (*ent).cmax = (*change).data.tuplecid.cmax;
            (*ent).combocid = (*change).data.tuplecid.combocid;
        } else {
            Assert!((*ent).cmin == (*change).data.tuplecid.cmin);
            Assert!(
                (*ent).cmax == InvalidCommandId
                    || ((*change).data.tuplecid.cmax != InvalidCommandId
                        && (*change).data.tuplecid.cmax > (*ent).cmax)
            );
            (*ent).cmax = (*change).data.tuplecid.cmax;
        }
    });
}

/*
 * Copy a provided snapshot so we can modify it privately.
 */
unsafe fn ReorderBufferCopySnap(
    rb: *mut ReorderBuffer,
    orig_snap: Snapshot,
    txn: *mut ReorderBufferTXN,
    cid: CommandId,
) -> Snapshot {
    let size = core::mem::size_of::<SnapshotData>()
        + core::mem::size_of::<TransactionId>() * (*orig_snap).xcnt as usize
        + core::mem::size_of::<TransactionId>() * ((*txn).nsubtxns as usize + 1);

    let snap = mc_alloc_zero((*rb).context, size) as Snapshot;
    memcpy(snap as *mut c_void, orig_snap as *const c_void, core::mem::size_of::<SnapshotData>());

    (*snap).copied = true;
    (*snap).active_count = 1; /* mark as active so nobody frees it */
    (*snap).regd_count = 0;
    (*snap).xip = (snap as *mut u8).add(core::mem::size_of::<SnapshotData>())
        as *mut TransactionId;

    memcpy(
        (*snap).xip as *mut c_void,
        (*orig_snap).xip as *const c_void,
        core::mem::size_of::<TransactionId>() * (*snap).xcnt as usize,
    );

    /*
     * snap->subxip contains all txids that belong to our transaction which we
     * need to check via cmin/cmax.
     */
    (*snap).subxip = (*snap).xip.add((*snap).xcnt as usize);
    *(*snap).subxip.add(0) = (*txn).xid;

    let mut i: usize = 1;
    (*snap).subxcnt = 1;

    let mut iter: dlist_iter = core::mem::zeroed();
    dlist_foreach!(iter, &mut (*txn).subtxns, {
        let sub_txn = dlist_container!(ReorderBufferTXN, node, iter.cur);
        *(*snap).subxip.add(i) = (*sub_txn).xid;
        i += 1;
        (*snap).subxcnt += 1;
    });

    /* sort so we can bsearch() later */
    qsort(
        (*snap).subxip as *mut c_void,
        (*snap).subxcnt as usize,
        core::mem::size_of::<TransactionId>(),
        xidComparator,
    );

    /* store the specified current CommandId */
    (*snap).curcid = cid;

    snap
}

/*
 * Free a previously ReorderBufferCopySnap'ed snapshot
 */
unsafe fn ReorderBufferFreeSnap(rb: *mut ReorderBuffer, snap: Snapshot) {
    if (*snap).copied {
        pfree(snap as *mut c_void);
    } else {
        SnapBuildSnapDecRefcount(snap);
    }
}

/*
 * If the transaction was (partially) streamed, commit it in a 'streamed' way.
 */
unsafe fn ReorderBufferStreamCommit(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    /* we should only call this for previously streamed transactions */
    Assert!(rbtxn_is_streamed(txn));

    ReorderBufferStreamTXN(rb, txn);

    if rbtxn_is_prepared(txn) {
        /*
         * Note, we send stream prepare even if a concurrent abort is
         * detected. See DecodePrepare for more information.
         */
        Assert!(!rbtxn_sent_prepare(txn));
        if let Some(cb) = (*rb).stream_prepare {
            cb(rb, txn, (*txn).final_lsn);
        }
        (*txn).txn_flags |= RBTXN_SENT_PREPARE;

        /*
         * This is a PREPARED transaction, part of a two-phase commit. The
         * full cleanup will happen as part of the COMMIT PREPAREDs, so now
         * just truncate txn by removing changes and tuplecids.
         */
        ReorderBufferTruncateTXN(rb, txn, true);
        /* Reset the CheckXidAlive */
        CheckXidAlive = InvalidTransactionId;
    } else {
        if let Some(cb) = (*rb).stream_commit {
            cb(rb, txn, (*txn).final_lsn);
        }
        ReorderBufferCleanupTXN(rb, txn);
    }
}

/*
 * Set xid to detect concurrent aborts.
 */
#[inline]
unsafe fn SetupCheckXidLive(xid: TransactionId) {
    if TransactionIdEquals(CheckXidAlive, xid) {
        return;
    }
    if !TransactionIdDidCommit(xid) {
        CheckXidAlive = xid;
    } else {
        CheckXidAlive = InvalidTransactionId;
    }
}

/*
 * Helper: apply change via callback (streaming or non-streaming)
 */
#[inline]
unsafe fn ReorderBufferApplyChange(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    relation: Relation,
    change: *mut ReorderBufferChange,
    streaming: bool,
) {
    if streaming {
        if let Some(cb) = (*rb).stream_change {
            cb(rb, txn, relation, change);
        }
    } else if let Some(cb) = (*rb).apply_change {
        cb(rb, txn, relation, change);
    }
}

/*
 * Helper: apply truncate via callback
 */
#[inline]
unsafe fn ReorderBufferApplyTruncate(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    nrelations: c_int,
    relations: *mut Relation,
    change: *mut ReorderBufferChange,
    streaming: bool,
) {
    if streaming {
        if let Some(cb) = (*rb).stream_truncate {
            cb(rb, txn, nrelations, relations, change);
        }
    } else if let Some(cb) = (*rb).apply_truncate {
        cb(rb, txn, nrelations, relations, change);
    }
}

/*
 * Helper: apply message via callback
 */
#[inline]
unsafe fn ReorderBufferApplyMessage(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    change: *mut ReorderBufferChange,
    streaming: bool,
) {
    if streaming {
        if let Some(cb) = (*rb).stream_message {
            cb(
                rb, txn, (*change).lsn, true,
                (*change).data.msg.prefix,
                (*change).data.msg.message_size,
                (*change).data.msg.message,
            );
        }
    } else if let Some(cb) = (*rb).message {
        cb(
            rb, txn, (*change).lsn, true,
            (*change).data.msg.prefix,
            (*change).data.msg.message_size,
            (*change).data.msg.message,
        );
    }
}

/*
 * Store command id and snapshot at the end of the current stream.
 */
#[inline]
unsafe fn ReorderBufferSaveTXNSnapshot(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    snapshot_now: Snapshot,
    command_id: CommandId,
) {
    (*txn).command_id = command_id;
    /* Avoid copying if it's already copied. */
    if (*snapshot_now).copied {
        (*txn).snapshot_now = snapshot_now;
    } else {
        (*txn).snapshot_now = ReorderBufferCopySnap(rb, snapshot_now, txn, command_id);
    }
}

/*
 * Mark the given transaction as streamed if it's a top-level transaction
 * or has changes.
 */
unsafe fn ReorderBufferMaybeMarkTXNStreamed(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    if rbtxn_is_toptxn(txn) || (*txn).nentries_mem != 0 {
        (*txn).txn_flags |= RBTXN_IS_STREAMED;
    }
}

/*
 * Helper for handling the concurrent abort of the streaming transaction.
 */
unsafe fn ReorderBufferResetTXN(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    snapshot_now: Snapshot,
    command_id: CommandId,
    last_lsn: XLogRecPtr,
    specinsert: *mut ReorderBufferChange,
) {
    /* Discard the changes that we just streamed */
    ReorderBufferTruncateTXN(rb, txn, rbtxn_is_prepared(txn));

    /* Free all resources allocated for toast reconstruction */
    ReorderBufferToastReset(rb, txn);

    /* Return the spec insert change if it is not NULL */
    if !specinsert.is_null() {
        ReorderBufferFreeChange(rb, specinsert, true);
    }

    /* For the streaming case, stop the stream and remember snapshot. */
    if rbtxn_is_streamed(txn) {
        if let Some(cb) = (*rb).stream_stop {
            cb(rb, txn, last_lsn);
        }
        ReorderBufferSaveTXNSnapshot(rb, txn, snapshot_now, command_id);
    }

    /* All changes must be deallocated */
    Assert!((*txn).size == 0);
}

/*
 * Record the partial change for the streaming of in-progress transactions.
 */
unsafe fn ReorderBufferProcessPartialChange(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    change: *mut ReorderBufferChange,
    toast_insert: bool,
) {
    if !ReorderBufferCanStream(rb) {
        return;
    }

    let toptxn = rbtxn_get_toptxn(txn);

    if toast_insert {
        (*toptxn).txn_flags |= RBTXN_HAS_PARTIAL_CHANGE;
    } else if rbtxn_has_partial_change(toptxn)
        && IsInsertOrUpdate((*change).action)
        && (*change).data.tp.clear_toast_afterwards
    {
        (*toptxn).txn_flags &= !RBTXN_HAS_PARTIAL_CHANGE;
    }

    if IsSpecInsert((*change).action) {
        (*toptxn).txn_flags |= RBTXN_HAS_PARTIAL_CHANGE;
    } else if rbtxn_has_partial_change(toptxn) && IsSpecConfirmOrAbort((*change).action) {
        (*toptxn).txn_flags &= !RBTXN_HAS_PARTIAL_CHANGE;
    }

    if ReorderBufferCanStartStreaming(rb)
        && !rbtxn_has_partial_change(toptxn)
        && rbtxn_is_serialized(txn)
        && rbtxn_has_streamable_change(toptxn)
    {
        ReorderBufferStreamTXN(rb, toptxn);
    }
}

/*
 * Queue a change into a transaction.
 */
pub unsafe fn ReorderBufferQueueChange(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
    change: *mut ReorderBufferChange,
    toast_insert: bool,
) {
    let txn = ReorderBufferTXNByXid(rb, xid, true, null_mut(), lsn, true);

    /*
     * If the transaction is aborted, there is no point in collecting
     * further changes for it.
     */
    if rbtxn_is_aborted(txn) {
        ReorderBufferFreeChange(rb, change, false);
        return;
    }

    /*
     * The changes that are sent downstream are considered streamable.
     */
    match (*change).action {
        REORDER_BUFFER_CHANGE_INSERT
        | REORDER_BUFFER_CHANGE_UPDATE
        | REORDER_BUFFER_CHANGE_DELETE
        | REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT
        | REORDER_BUFFER_CHANGE_TRUNCATE
        | REORDER_BUFFER_CHANGE_MESSAGE => {
            let toptxn = rbtxn_get_toptxn(txn);
            (*toptxn).txn_flags |= RBTXN_HAS_STREAMABLE_CHANGE;
        }
        _ => {}
    }

    (*change).lsn = lsn;
    (*change).txn = txn;

    Assert!(InvalidXLogRecPtr != lsn);
    dlist_push_tail(&mut (*txn).changes, &mut (*change).node);
    (*txn).nentries += 1;
    (*txn).nentries_mem += 1;

    /* update memory accounting information */
    ReorderBufferChangeMemoryUpdate(
        rb, change, null_mut(), true, ReorderBufferChangeSize(change),
    );

    /* process partial change */
    ReorderBufferProcessPartialChange(rb, txn, change, toast_insert);

    /* check the memory limits and evict something if needed */
    ReorderBufferCheckMemoryLimit(rb);
}

/*
 * A transactional message is queued to be processed upon commit and a
 * non-transactional message gets processed immediately.
 */
pub unsafe fn ReorderBufferQueueMessage(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    snap: Snapshot,
    lsn: XLogRecPtr,
    transactional: bool,
    prefix: *const c_char,
    message_size: Size,
    message: *const c_char,
) {
    if transactional {
        Assert!(xid != InvalidTransactionId);
        Assert!(snap.is_null());

        let oldcontext = MemoryContextSwitchTo((*rb).context);

        let change = ReorderBufferAllocChange(rb);
        (*change).action = REORDER_BUFFER_CHANGE_MESSAGE;
        (*change).data.msg.prefix = pstrdup(prefix) as *mut c_char;
        (*change).data.msg.message_size = message_size;
        (*change).data.msg.message = palloc(message_size) as *mut c_char;
        memcpy((*change).data.msg.message as *mut c_void, message as *const c_void, message_size);

        ReorderBufferQueueChange(rb, xid, lsn, change, false);

        MemoryContextSwitchTo(oldcontext);
    } else {
        let snapshot_now = snap;
        /* Non-transactional changes require a valid snapshot. */
        Assert!(!snapshot_now.is_null());

        let txn: *mut ReorderBufferTXN = if xid != InvalidTransactionId {
            ReorderBufferTXNByXid(rb, xid, true, null_mut(), lsn, true)
        } else {
            null_mut()
        };

        /* setup snapshot to allow catalog access */
        SetupHistoricSnapshot(snapshot_now, null_mut());
        // PG_TRY equivalent - simplified (no exception handling in stub)
        if let Some(cb) = (*rb).message {
            cb(rb, txn, lsn, false, prefix, message_size, message);
        }
        TeardownHistoricSnapshot(false);
    }
}

// ===========================================================================
// Part 5: ProcessTXN, Replay, Commit, Prepare, Abort, memory accounting,
//         catalog/snapshot helpers, serialization
// ===========================================================================

const CHANGES_THRESHOLD: c_int = 100;

/*
 * Send data of a transaction (and its subtransactions) to the output plugin.
 * We iterate over the top and subtransactions (using a k-way merge) and
 * replay the changes in lsn order.
 */
unsafe fn ReorderBufferProcessTXN(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    commit_lsn: XLogRecPtr,
    snapshot_now_in: Snapshot,
    command_id_in: CommandId,
    streaming: bool,
) {
    let using_subtxn: bool;
    let ccxt = CurrentMemoryContext;
    let mut iterstate: *mut ReorderBufferIterTXNState = null_mut();
    let mut prev_lsn: XLogRecPtr = InvalidXLogRecPtr;
    let mut specinsert: *mut ReorderBufferChange = null_mut();
    let mut stream_started: bool = false;
    let mut curtxn: *mut ReorderBufferTXN = null_mut();
    let mut snapshot_now = snapshot_now_in;
    let mut command_id = command_id_in;

    /* build data to be able to lookup the CommandIds of catalog tuples */
    ReorderBufferBuildTupleCidHash(rb, txn);

    /* setup the initial snapshot */
    SetupHistoricSnapshot(snapshot_now, (*txn).tuplecid_hash);

    using_subtxn = IsTransactionOrTransactionBlock();

    // NOTE: PG_TRY/CATCH is not directly translatable; we use a flat structure
    // with manual error path (production code would use panic hooks).
    if using_subtxn {
        BeginInternalSubTransaction(if streaming {
            b"stream\0".as_ptr() as *const c_char
        } else {
            b"replay\0".as_ptr() as *const c_char
        });
    } else {
        StartTransactionCommand();
    }

    /* Send begin/begin-prepare for non-streamed transactions. */
    if !streaming {
        if rbtxn_is_prepared(txn) {
            if let Some(cb) = (*rb).begin_prepare {
                cb(rb, txn);
            }
        } else if let Some(cb) = (*rb).begin {
            cb(rb, txn);
        }
    }

    ReorderBufferIterTXNInit(rb, txn, &mut iterstate);
    let mut changes_count: c_int = 0;

    'iter: loop {
        let change = ReorderBufferIterTXNNext(rb, iterstate);
        if change.is_null() {
            break 'iter;
        }

        let mut relation: Relation = null_mut();

        CHECK_FOR_INTERRUPTS();

        /* We can't call start stream callback before processing first change. */
        if prev_lsn == InvalidXLogRecPtr {
            if streaming {
                (*txn).origin_id = (*change).origin_id;
                if let Some(cb) = (*rb).stream_start {
                    cb(rb, txn, (*change).lsn);
                }
                stream_started = true;
            }
        }

        Assert!(prev_lsn == InvalidXLogRecPtr || prev_lsn <= (*change).lsn);
        prev_lsn = (*change).lsn;

        /* Set the current xid to detect concurrent aborts. */
        if streaming || rbtxn_is_prepared((*change).txn) {
            curtxn = (*change).txn;
            SetupCheckXidLive((*curtxn).xid);
        }

        let action = (*change).action;
        match action {
            REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM => {
                if specinsert.is_null() {
                    elog!(ERROR, "invalid ordering of speculative insertion changes");
                }
                Assert!((*specinsert).data.tp.oldtuple.is_null());
                let change = specinsert;
                (*change).action = REORDER_BUFFER_CHANGE_INSERT;
                // fall through to INSERT handling below
                let reloid = RelidByRelfilenumber(
                    (*change).data.tp.rlocator.spcOid,
                    (*change).data.tp.rlocator.relNumber,
                );
                if reloid == 0
                    && (*change).data.tp.newtuple.is_null()
                    && (*change).data.tp.oldtuple.is_null()
                {
                    // goto change_done
                } else {
                    if reloid == 0 {
                        elog!(ERROR, "could not map filenumber to relation OID");
                    }
                    relation = RelationIdGetRelation(reloid);
                    if !RelationIsValid(relation) {
                        elog!(ERROR, "could not open relation with OID {}", reloid);
                    }
                    if RelationIsLogicallyLogged(relation) {
                        if !IsToastRelation(relation) {
                            ReorderBufferToastReplace(rb, txn, relation, change);
                            ReorderBufferApplyChange(rb, txn, relation, change, streaming);
                            if (*change).data.tp.clear_toast_afterwards {
                                ReorderBufferToastReset(rb, txn);
                            }
                        }
                    }
                }
                // change_done:
                if !specinsert.is_null() {
                    ReorderBufferFreeChange(rb, specinsert, true);
                    specinsert = null_mut();
                }
                if RelationIsValid(relation) {
                    RelationClose(relation);
                }
            }

            REORDER_BUFFER_CHANGE_INSERT
            | REORDER_BUFFER_CHANGE_UPDATE
            | REORDER_BUFFER_CHANGE_DELETE => {
                Assert!(!snapshot_now.is_null());
                let reloid = RelidByRelfilenumber(
                    (*change).data.tp.rlocator.spcOid,
                    (*change).data.tp.rlocator.relNumber,
                );
                if reloid == 0
                    && (*change).data.tp.newtuple.is_null()
                    && (*change).data.tp.oldtuple.is_null()
                {
                    /* goto change_done */
                } else {
                    if reloid == 0 {
                        elog!(ERROR, "could not map filenumber to relation OID");
                    }
                    relation = RelationIdGetRelation(reloid);
                    if !RelationIsValid(relation) {
                        elog!(ERROR, "could not open relation with OID {}", reloid);
                    }
                    if !RelationIsLogicallyLogged(relation) { /* goto change_done */ }
                    else if !IsToastRelation(relation) {
                        ReorderBufferToastReplace(rb, txn, relation, change);
                        ReorderBufferApplyChange(rb, txn, relation, change, streaming);
                        if (*change).data.tp.clear_toast_afterwards {
                            ReorderBufferToastReset(rb, txn);
                        }
                    } else if (*change).action == REORDER_BUFFER_CHANGE_INSERT {
                        Assert!(!(*change).data.tp.newtuple.is_null());
                        dlist_delete(&mut (*change).node);
                        ReorderBufferToastAppendChunk(rb, txn, relation, change);
                    }
                }
                if !specinsert.is_null() {
                    ReorderBufferFreeChange(rb, specinsert, true);
                    specinsert = null_mut();
                }
                if RelationIsValid(relation) {
                    RelationClose(relation);
                }
            }

            REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT => {
                if !specinsert.is_null() {
                    ReorderBufferFreeChange(rb, specinsert, true);
                    specinsert = null_mut();
                }
                dlist_delete(&mut (*change).node);
                specinsert = change;
            }

            REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT => {
                if !specinsert.is_null() {
                    Assert!((*change).data.tp.clear_toast_afterwards);
                    ReorderBufferToastReset(rb, txn);
                    ReorderBufferFreeChange(rb, specinsert, true);
                    specinsert = null_mut();
                }
            }

            REORDER_BUFFER_CHANGE_TRUNCATE => {
                let nrelids = (*change).data.truncate.nrelids as usize;
                let mut nrelations: c_int = 0;
                let relations_buf = palloc0(nrelids * core::mem::size_of::<Relation>())
                    as *mut Relation;
                for i in 0..nrelids {
                    let relid = *(*change).data.truncate.relids.add(i);
                    let rel = RelationIdGetRelation(relid);
                    if !RelationIsValid(rel) {
                        elog!(ERROR, "could not open relation with OID {}", relid);
                    }
                    if !RelationIsLogicallyLogged(rel) {
                        continue;
                    }
                    *relations_buf.add(nrelations as usize) = rel;
                    nrelations += 1;
                }
                ReorderBufferApplyTruncate(rb, txn, nrelations, relations_buf, change, streaming);
                for i in 0..nrelations as usize {
                    RelationClose(*relations_buf.add(i));
                }
            }

            REORDER_BUFFER_CHANGE_MESSAGE => {
                ReorderBufferApplyMessage(rb, txn, change, streaming);
            }

            REORDER_BUFFER_CHANGE_INVALIDATION => {
                ReorderBufferExecuteInvalidations(
                    (*change).data.inval.ninvalidations,
                    (*change).data.inval.invalidations,
                );
            }

            REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT => {
                TeardownHistoricSnapshot(false);

                if (*snapshot_now).copied {
                    ReorderBufferFreeSnap(rb, snapshot_now);
                    snapshot_now = ReorderBufferCopySnap(
                        rb, (*change).data.snapshot, txn, command_id,
                    );
                } else if (*(*change).data.snapshot).copied {
                    snapshot_now = ReorderBufferCopySnap(
                        rb, (*change).data.snapshot, txn, command_id,
                    );
                } else {
                    snapshot_now = (*change).data.snapshot;
                }

                SetupHistoricSnapshot(snapshot_now, (*txn).tuplecid_hash);
            }

            REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID => {
                Assert!((*change).data.command_id != InvalidCommandId);

                if command_id < (*change).data.command_id {
                    command_id = (*change).data.command_id;

                    if !(*snapshot_now).copied {
                        snapshot_now = ReorderBufferCopySnap(rb, snapshot_now, txn, command_id);
                    }

                    (*snapshot_now).curcid = command_id;
                    TeardownHistoricSnapshot(false);
                    SetupHistoricSnapshot(snapshot_now, (*txn).tuplecid_hash);
                }
            }

            REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID => {
                elog!(ERROR, "tuplecid value in changequeue");
            }
        }

        changes_count += 1;
        if changes_count >= CHANGES_THRESHOLD {
            if let Some(cb) = (*rb).update_progress_txn {
                cb(rb, txn, prev_lsn);
            }
            changes_count = 0;
        }
    } // end 'iter loop

    Assert!(specinsert.is_null());

    /* clean up the iterator */
    ReorderBufferIterTXNFinish(rb, iterstate);
    iterstate = null_mut();

    /* Update total transaction count and total bytes. */
    if !rbtxn_is_streamed(txn) {
        (*rb).totalTxns += 1;
    }
    (*rb).totalBytes += (*txn).total_size as int64;

    /* Done with current changes */
    if streaming {
        if stream_started {
            if let Some(cb) = (*rb).stream_stop {
                cb(rb, txn, prev_lsn);
            }
            stream_started = false;
        }
    } else {
        if rbtxn_is_prepared(txn) {
            Assert!(!rbtxn_sent_prepare(txn));
            if let Some(cb) = (*rb).prepare {
                cb(rb, txn, commit_lsn);
            }
            (*txn).txn_flags |= RBTXN_SENT_PREPARE;
        } else if let Some(cb) = (*rb).commit {
            cb(rb, txn, commit_lsn);
        }
    }

    /* sanity check against bad output plugin behaviour */
    if GetCurrentTransactionIdIfAny() != InvalidTransactionId {
        elog!(ERROR, "output plugin used XID {}", GetCurrentTransactionId());
    }

    /* Remember the command ID and snapshot for the next set of changes. */
    if streaming {
        ReorderBufferSaveTXNSnapshot(rb, txn, snapshot_now, command_id);
    } else if (*snapshot_now).copied {
        ReorderBufferFreeSnap(rb, snapshot_now);
    }

    /* cleanup */
    TeardownHistoricSnapshot(false);
    AbortCurrentTransaction();

    /* make sure there's no cache pollution */
    if rbtxn_distr_inval_overflowed(txn) {
        Assert!((*txn).ninvalidations_distributed == 0);
        InvalidateSystemCaches();
    } else {
        ReorderBufferExecuteInvalidations((*txn).ninvalidations, (*txn).invalidations);
        ReorderBufferExecuteInvalidations(
            (*txn).ninvalidations_distributed,
            (*txn).invalidations_distributed,
        );
    }

    if using_subtxn {
        RollbackAndReleaseCurrentSubTransaction();
    }

    if streaming || rbtxn_is_prepared(txn) {
        if streaming {
            ReorderBufferMaybeMarkTXNStreamed(rb, txn);
        }
        ReorderBufferTruncateTXN(rb, txn, rbtxn_is_prepared(txn));
        /* Reset the CheckXidAlive */
        CheckXidAlive = InvalidTransactionId;
    } else {
        ReorderBufferCleanupTXN(rb, txn);
    }
}

/*
 * Perform the replay of a transaction and its non-aborted subtransactions.
 */
unsafe fn ReorderBufferReplay(
    txn: *mut ReorderBufferTXN,
    rb: *mut ReorderBuffer,
    _xid: TransactionId,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    commit_time: TimestampTz,
    origin_id: RepOriginId,
    origin_lsn: XLogRecPtr,
) {
    (*txn).final_lsn = commit_lsn;
    (*txn).end_lsn = end_lsn;
    (*txn).xact_time.commit_time = commit_time;
    (*txn).origin_id = origin_id;
    (*txn).origin_lsn = origin_lsn;

    /*
     * If the transaction was (partially) streamed, commit it in a 'streamed' way.
     */
    if rbtxn_is_streamed(txn) {
        ReorderBufferStreamCommit(rb, txn);
        return;
    }

    /*
     * If this transaction has no snapshot, it didn't make any changes.
     */
    if (*txn).base_snapshot.is_null() {
        Assert!((*txn).ninvalidations == 0);
        if !rbtxn_is_prepared(txn) {
            ReorderBufferCleanupTXN(rb, txn);
        }
        return;
    }

    let snapshot_now = (*txn).base_snapshot;

    /* Process and send the changes to output plugin. */
    ReorderBufferProcessTXN(rb, txn, commit_lsn, snapshot_now, FirstCommandId, false);
}

/*
 * Commit a transaction.
 */
pub unsafe fn ReorderBufferCommit(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    commit_time: TimestampTz,
    origin_id: RepOriginId,
    origin_lsn: XLogRecPtr,
) {
    let txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), InvalidXLogRecPtr, false);

    /* unknown transaction, nothing to replay */
    if txn.is_null() {
        return;
    }

    ReorderBufferReplay(txn, rb, xid, commit_lsn, end_lsn, commit_time, origin_id, origin_lsn);
}

/*
 * Record the prepare information for a transaction.
 */
pub unsafe fn ReorderBufferRememberPrepareInfo(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    prepare_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
    origin_id: RepOriginId,
    origin_lsn: XLogRecPtr,
) -> bool {
    let txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), InvalidXLogRecPtr, false);

    if txn.is_null() {
        return false;
    }

    (*txn).final_lsn = prepare_lsn;
    (*txn).end_lsn = end_lsn;
    (*txn).xact_time.prepare_time = prepare_time;
    (*txn).origin_id = origin_id;
    (*txn).origin_lsn = origin_lsn;

    Assert!((*txn).txn_flags & RBTXN_PREPARE_STATUS_MASK == 0);
    (*txn).txn_flags |= RBTXN_IS_PREPARED;

    true
}

/*
 * Remember that we have skipped prepare.
 */
pub unsafe fn ReorderBufferSkipPrepare(rb: *mut ReorderBuffer, xid: TransactionId) {
    let txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), InvalidXLogRecPtr, false);
    if txn.is_null() {
        return;
    }
    Assert!((*txn).txn_flags & RBTXN_PREPARE_STATUS_MASK == RBTXN_IS_PREPARED);
    (*txn).txn_flags |= RBTXN_SKIPPED_PREPARE;
}

/*
 * Prepare a two-phase transaction.
 */
pub unsafe fn ReorderBufferPrepare(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    gid: *mut c_char,
) {
    let txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), InvalidXLogRecPtr, false);
    if txn.is_null() {
        return;
    }

    Assert!((*txn).txn_flags & RBTXN_PREPARE_STATUS_MASK == RBTXN_IS_PREPARED);
    Assert!((*txn).final_lsn != InvalidXLogRecPtr);

    (*txn).gid = pstrdup(gid) as *mut c_char;

    let final_lsn = (*txn).final_lsn;
    let end_lsn = (*txn).end_lsn;
    let prepare_time = (*txn).xact_time.prepare_time;
    let origin_id = (*txn).origin_id;
    let origin_lsn = (*txn).origin_lsn;

    ReorderBufferReplay(txn, rb, xid, final_lsn, end_lsn, prepare_time, origin_id, origin_lsn);

    if !rbtxn_sent_prepare(txn) {
        if let Some(cb) = (*rb).prepare {
            cb(rb, txn, (*txn).final_lsn);
        }
        (*txn).txn_flags |= RBTXN_SENT_PREPARE;
    }
}

/*
 * Handle COMMIT/ROLLBACK PREPARED.
 */
pub unsafe fn ReorderBufferFinishPrepared(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    two_phase_at: XLogRecPtr,
    commit_time: TimestampTz,
    origin_id: RepOriginId,
    origin_lsn: XLogRecPtr,
    gid: *mut c_char,
    is_commit: bool,
) {
    let txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), commit_lsn, false);

    if txn.is_null() {
        return;
    }

    let prepare_end_lsn = (*txn).end_lsn;
    let prepare_time = (*txn).xact_time.prepare_time;

    (*txn).gid = pstrdup(gid) as *mut c_char;

    if ((*txn).final_lsn < two_phase_at) && is_commit {
        Assert!(
            (*txn).txn_flags & RBTXN_PREPARE_STATUS_MASK
                == RBTXN_IS_PREPARED | RBTXN_SKIPPED_PREPARE
        );
        Assert!((*txn).final_lsn != InvalidXLogRecPtr);

        let final_lsn = (*txn).final_lsn;
        let txn_end_lsn = (*txn).end_lsn;
        let txn_origin_id = (*txn).origin_id;
        let txn_origin_lsn = (*txn).origin_lsn;

        ReorderBufferReplay(
            txn, rb, xid, final_lsn, txn_end_lsn, prepare_time, txn_origin_id, txn_origin_lsn,
        );
    }

    (*txn).final_lsn = commit_lsn;
    (*txn).end_lsn = end_lsn;
    (*txn).xact_time.commit_time = commit_time;
    (*txn).origin_id = origin_id;
    (*txn).origin_lsn = origin_lsn;

    if is_commit {
        if let Some(cb) = (*rb).commit_prepared {
            cb(rb, txn, commit_lsn);
        }
    } else if let Some(cb) = (*rb).rollback_prepared {
        cb(rb, txn, prepare_end_lsn, prepare_time);
    }

    /* cleanup: make sure there's no cache pollution */
    ReorderBufferExecuteInvalidations((*txn).ninvalidations, (*txn).invalidations);
    ReorderBufferCleanupTXN(rb, txn);
}

/*
 * Abort a transaction.
 */
pub unsafe fn ReorderBufferAbort(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
    abort_time: TimestampTz,
) {
    let txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), InvalidXLogRecPtr, false);
    if txn.is_null() {
        return;
    }

    (*txn).xact_time.abort_time = abort_time;

    /* For streamed transactions notify the remote node about the abort. */
    if rbtxn_is_streamed(txn) {
        if let Some(cb) = (*rb).stream_abort {
            cb(rb, txn, lsn);
        }

        if (*txn).ninvalidations > 0 {
            ReorderBufferImmediateInvalidation(rb, (*txn).ninvalidations, (*txn).invalidations);
        }
    }

    (*txn).final_lsn = lsn;
    ReorderBufferCleanupTXN(rb, txn);
}

/*
 * Abort all transactions that aren't actually running anymore.
 */
pub unsafe fn ReorderBufferAbortOld(rb: *mut ReorderBuffer, oldest_running_xid: TransactionId) {
    let mut it: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(it, &mut (*rb).toplevel_by_lsn, {
        let txn = dlist_container!(ReorderBufferTXN, node, it.cur);
        if TransactionIdPrecedes((*txn).xid, oldest_running_xid) {
            elog!(DEBUG2, "aborting old transaction {}", (*txn).xid);
            if rbtxn_is_streamed(txn) {
                if let Some(cb) = (*rb).stream_abort {
                    cb(rb, txn, InvalidXLogRecPtr);
                }
            }
            ReorderBufferCleanupTXN(rb, txn);
        } else {
            return;
        }
    });
}

/*
 * Forget the contents of a transaction if we aren't interested in its
 * contents.
 */
pub unsafe fn ReorderBufferForget(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
) {
    let txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), InvalidXLogRecPtr, false);
    if txn.is_null() {
        return;
    }

    Assert!(!rbtxn_is_streamed(txn));
    (*txn).final_lsn = lsn;

    if !(*txn).base_snapshot.is_null() && (*txn).ninvalidations > 0 {
        ReorderBufferImmediateInvalidation(rb, (*txn).ninvalidations, (*txn).invalidations);
    } else {
        Assert!((*txn).ninvalidations == 0);
    }

    ReorderBufferCleanupTXN(rb, txn);
}

/*
 * Invalidate cache for transactions that need to be skipped.
 */
pub unsafe fn ReorderBufferInvalidate(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    _lsn: XLogRecPtr,
) {
    let txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), InvalidXLogRecPtr, false);
    if txn.is_null() {
        return;
    }

    if !(*txn).base_snapshot.is_null() && (*txn).ninvalidations > 0 {
        ReorderBufferImmediateInvalidation(rb, (*txn).ninvalidations, (*txn).invalidations);
    } else {
        Assert!((*txn).ninvalidations == 0);
    }
}

/*
 * Execute invalidations happening outside the context of a decoded transaction.
 */
pub unsafe fn ReorderBufferImmediateInvalidation(
    rb: *mut ReorderBuffer,
    ninvalidations: uint32,
    invalidations: *mut SharedInvalidationMessage,
) {
    let use_subtxn = IsTransactionOrTransactionBlock();

    if use_subtxn {
        BeginInternalSubTransaction(b"replay\0".as_ptr() as *const c_char);
    }

    /* Force invalidations outside of a valid transaction */
    if use_subtxn {
        AbortCurrentTransaction();
    }

    for i in 0..ninvalidations as usize {
        LocalExecuteInvalidationMessage(invalidations.add(i));
    }

    if use_subtxn {
        RollbackAndReleaseCurrentSubTransaction();
    }
}

/*
 * Tell reorderbuffer about an xid seen in the WAL stream.
 */
pub unsafe fn ReorderBufferProcessXid(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
) {
    if xid != InvalidTransactionId {
        ReorderBufferTXNByXid(rb, xid, true, null_mut(), lsn, true);
    }
}

/*
 * Add a new snapshot to this transaction.
 */
pub unsafe fn ReorderBufferAddSnapshot(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
    snap: Snapshot,
) {
    let change = ReorderBufferAllocChange(rb);
    (*change).data.snapshot = snap;
    (*change).action = REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT;
    ReorderBufferQueueChange(rb, xid, lsn, change, false);
}

/*
 * Set up the transaction's base snapshot.
 */
pub unsafe fn ReorderBufferSetBaseSnapshot(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
    snap: Snapshot,
) {
    let mut is_new: bool = false;
    Assert!(!snap.is_null());

    let mut txn = ReorderBufferTXNByXid(rb, xid, true, &mut is_new, lsn, true);
    if rbtxn_is_known_subxact(txn) {
        txn = ReorderBufferTXNByXid(rb, (*txn).toplevel_xid, false, null_mut(), InvalidXLogRecPtr, false);
    }
    Assert!((*txn).base_snapshot.is_null());

    (*txn).base_snapshot = snap;
    (*txn).base_snapshot_lsn = lsn;
    dlist_push_tail(&mut (*rb).txns_by_base_snapshot_lsn, &mut (*txn).base_snapshot_node);

    AssertTXNLsnOrder(rb);
}

/*
 * Access the catalog with this CommandId.
 */
pub unsafe fn ReorderBufferAddNewCommandId(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
    cid: CommandId,
) {
    let change = ReorderBufferAllocChange(rb);
    (*change).data.command_id = cid;
    (*change).action = REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID;
    ReorderBufferQueueChange(rb, xid, lsn, change, false);
}

/*
 * Update memory counters.
 */
unsafe fn ReorderBufferChangeMemoryUpdate(
    rb: *mut ReorderBuffer,
    change: *mut ReorderBufferChange,
    txn_in: *mut ReorderBufferTXN,
    addition: bool,
    sz: Size,
) {
    Assert!(!txn_in.is_null() || !change.is_null());

    /* Ignore tuple CID changes */
    if !change.is_null()
        && (*change).action == REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID
    {
        return;
    }

    if sz == 0 {
        return;
    }

    let txn = if txn_in.is_null() {
        Assert!(!(*change).txn.is_null());
        (*change).txn
    } else {
        txn_in
    };
    Assert!(!txn.is_null());

    let toptxn = rbtxn_get_toptxn(txn);

    if addition {
        let oldsize = (*txn).size;
        (*txn).size += sz;
        (*rb).size += sz;
        (*toptxn).total_size += sz;

        if oldsize != 0 {
            pairingheap_remove((*rb).txn_heap, &mut (*txn).txn_node);
        }
        pairingheap_add((*rb).txn_heap, &mut (*txn).txn_node);
    } else {
        Assert!((*rb).size >= sz && (*txn).size >= sz);
        (*txn).size -= sz;
        (*rb).size -= sz;
        (*toptxn).total_size -= sz;

        pairingheap_remove((*rb).txn_heap, &mut (*txn).txn_node);
        if (*txn).size != 0 {
            pairingheap_add((*rb).txn_heap, &mut (*txn).txn_node);
        }
    }

    Assert!((*txn).size <= (*rb).size);
}

/*
 * Add new (relfilelocator, tid) -> (cmin, cmax) mappings.
 */
pub unsafe fn ReorderBufferAddNewTupleCids(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
    locator: RelFileLocator,
    tid: ItemPointerData,
    cmin: CommandId,
    cmax: CommandId,
    combocid: CommandId,
) {
    let change = ReorderBufferAllocChange(rb);
    let txn = ReorderBufferTXNByXid(rb, xid, true, null_mut(), lsn, true);

    (*change).data.tuplecid.locator = locator;
    (*change).data.tuplecid.tid = tid;
    (*change).data.tuplecid.cmin = cmin;
    (*change).data.tuplecid.cmax = cmax;
    (*change).data.tuplecid.combocid = combocid;
    (*change).lsn = lsn;
    (*change).txn = txn;
    (*change).action = REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID;

    dlist_push_tail(&mut (*txn).tuplecids, &mut (*change).node);
    (*txn).ntuplecids += 1;
}

// ===========================================================================
// Part 6: Invalidation helpers, catalog/snapshot queries, disk serialization,
//         streaming, memory-limit check, size accounting, toast, visibility
// ===========================================================================

/*
 * Add new invalidation messages to the reorder buffer queue.
 */
unsafe fn ReorderBufferQueueInvalidations(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
    nmsgs: Size,
    msgs: *mut SharedInvalidationMessage,
) {
    let change = ReorderBufferAllocChange(rb);
    (*change).action = REORDER_BUFFER_CHANGE_INVALIDATION;
    (*change).data.inval.ninvalidations = nmsgs as uint32;
    (*change).data.inval.invalidations = palloc(
        core::mem::size_of::<SharedInvalidationMessage>() * nmsgs,
    ) as *mut SharedInvalidationMessage;
    memcpy(
        (*change).data.inval.invalidations as *mut c_void,
        msgs as *const c_void,
        core::mem::size_of::<SharedInvalidationMessage>() * nmsgs,
    );
    ReorderBufferQueueChange(rb, xid, lsn, change, false);
}

/*
 * Accumulate invalidation messages to **invals_out.
 */
unsafe fn ReorderBufferAccumulateInvalidations(
    invals_out: *mut *mut SharedInvalidationMessage,
    ninvals_out: *mut uint32,
    msgs_new: *mut SharedInvalidationMessage,
    nmsgs_new: Size,
) {
    if *ninvals_out == 0 {
        *ninvals_out = nmsgs_new as uint32;
        *invals_out = palloc(core::mem::size_of::<SharedInvalidationMessage>() * nmsgs_new)
            as *mut SharedInvalidationMessage;
        memcpy(
            *invals_out as *mut c_void,
            msgs_new as *const c_void,
            core::mem::size_of::<SharedInvalidationMessage>() * nmsgs_new,
        );
    } else {
        *invals_out = repalloc(
            *invals_out as *mut c_void,
            core::mem::size_of::<SharedInvalidationMessage>()
                * (*ninvals_out as usize + nmsgs_new),
        ) as *mut SharedInvalidationMessage;
        memcpy(
            (*invals_out).add(*ninvals_out as usize) as *mut c_void,
            msgs_new as *const c_void,
            nmsgs_new * core::mem::size_of::<SharedInvalidationMessage>(),
        );
        *ninvals_out += nmsgs_new as uint32;
    }
}

/*
 * Accumulate the invalidations for executing them later.
 */
pub unsafe fn ReorderBufferAddInvalidations(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
    nmsgs: Size,
    msgs: *mut SharedInvalidationMessage,
) {
    let mut txn = ReorderBufferTXNByXid(rb, xid, true, null_mut(), lsn, true);

    let oldcontext = MemoryContextSwitchTo((*rb).context);

    /* Collect all the invalidations under the top transaction */
    txn = rbtxn_get_toptxn(txn);

    Assert!(nmsgs > 0);

    ReorderBufferAccumulateInvalidations(
        &mut (*txn).invalidations,
        &mut (*txn).ninvalidations,
        msgs, nmsgs,
    );

    ReorderBufferQueueInvalidations(rb, xid, lsn, nmsgs, msgs);

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Accumulate distributed invalidations.
 */
pub unsafe fn ReorderBufferAddDistributedInvalidations(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
    nmsgs: Size,
    msgs: *mut SharedInvalidationMessage,
) {
    let mut txn = ReorderBufferTXNByXid(rb, xid, true, null_mut(), lsn, true);

    let oldcontext = MemoryContextSwitchTo((*rb).context);

    txn = rbtxn_get_toptxn(txn);

    Assert!(nmsgs > 0);

    if !rbtxn_distr_inval_overflowed(txn) {
        if (*txn).ninvalidations_distributed as usize + nmsgs >= max_distr_inval_msg_per_txn() {
            (*txn).txn_flags |= RBTXN_DISTR_INVAL_OVERFLOWED;
            if !(*txn).invalidations_distributed.is_null() {
                pfree((*txn).invalidations_distributed as *mut c_void);
                (*txn).invalidations_distributed = null_mut();
                (*txn).ninvalidations_distributed = 0;
            }
        } else {
            ReorderBufferAccumulateInvalidations(
                &mut (*txn).invalidations_distributed,
                &mut (*txn).ninvalidations_distributed,
                msgs, nmsgs,
            );
        }
    }

    ReorderBufferQueueInvalidations(rb, xid, lsn, nmsgs, msgs);

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Apply all invalidations we know.
 */
unsafe fn ReorderBufferExecuteInvalidations(
    nmsgs: uint32,
    msgs: *mut SharedInvalidationMessage,
) {
    for i in 0..nmsgs as usize {
        LocalExecuteInvalidationMessage(msgs.add(i));
    }
}

/*
 * Mark a transaction as containing catalog changes.
 */
pub unsafe fn ReorderBufferXidSetCatalogChanges(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    lsn: XLogRecPtr,
) {
    let txn = ReorderBufferTXNByXid(rb, xid, true, null_mut(), lsn, true);

    if !rbtxn_has_catalog_changes(txn) {
        (*txn).txn_flags |= RBTXN_HAS_CATALOG_CHANGES;
        dclist_push_tail(&mut (*rb).catchange_txns, &mut (*txn).catchange_node);
    }

    if rbtxn_is_subtxn(txn) {
        let toptxn = rbtxn_get_toptxn(txn);
        if !rbtxn_has_catalog_changes(toptxn) {
            (*toptxn).txn_flags |= RBTXN_HAS_CATALOG_CHANGES;
            dclist_push_tail(&mut (*rb).catchange_txns, &mut (*toptxn).catchange_node);
        }
    }
}

/*
 * Return palloc'ed array of the transactions that have changed catalogs.
 */
pub unsafe fn ReorderBufferGetCatalogChangesXacts(
    rb: *mut ReorderBuffer,
) -> *mut TransactionId {
    if dclist_count(&(*rb).catchange_txns) == 0 {
        return null_mut();
    }

    let cnt = dclist_count(&(*rb).catchange_txns) as usize;
    let xids = palloc(core::mem::size_of::<TransactionId>() * cnt) as *mut TransactionId;
    let mut xcnt: usize = 0;

    let mut iter: dlist_iter = core::mem::zeroed();
    dclist_foreach!(iter, &mut (*rb).catchange_txns, {
        let txn = dclist_container!(ReorderBufferTXN, catchange_node, iter.cur);
        Assert!(rbtxn_has_catalog_changes(txn));
        *xids.add(xcnt) = (*txn).xid;
        xcnt += 1;
    });

    qsort(xids as *mut c_void, xcnt, core::mem::size_of::<TransactionId>(), xidComparator);

    Assert!(xcnt == dclist_count(&(*rb).catchange_txns) as usize);
    xids
}

/*
 * Query whether a transaction is already known to contain catalog changes.
 */
pub unsafe fn ReorderBufferXidHasCatalogChanges(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
) -> bool {
    let txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), InvalidXLogRecPtr, false);
    if txn.is_null() {
        return false;
    }
    rbtxn_has_catalog_changes(txn)
}

/*
 * Have we already set the base snapshot for the given txn/subtxn?
 */
pub unsafe fn ReorderBufferXidHasBaseSnapshot(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
) -> bool {
    let mut txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), InvalidXLogRecPtr, false);

    if txn.is_null() {
        return false;
    }

    if rbtxn_is_known_subxact(txn) {
        txn = ReorderBufferTXNByXid(rb, (*txn).toplevel_xid, false, null_mut(), InvalidXLogRecPtr, false);
    }

    !(*txn).base_snapshot.is_null()
}

/*
 * Ensure the IO buffer is >= sz.
 */
unsafe fn ReorderBufferSerializeReserve(rb: *mut ReorderBuffer, sz: Size) {
    if (*rb).outbufsize == 0 {
        (*rb).outbuf = mc_alloc((*rb).context, sz) as *mut c_char;
        (*rb).outbufsize = sz;
    } else if (*rb).outbufsize < sz {
        (*rb).outbuf = repalloc((*rb).outbuf as *mut c_void, sz) as *mut c_char;
        (*rb).outbufsize = sz;
    }
}

/*
 * Compare two transactions by size (for pairingheap).
 */
unsafe fn ReorderBufferTXNSizeCompare(
    a: *const pairingheap_node,
    b: *const pairingheap_node,
    _arg: *mut c_void,
) -> c_int {
    let ta = pairingheap_container!(ReorderBufferTXN, txn_node, a as *mut pairingheap_node);
    let tb = pairingheap_container!(ReorderBufferTXN, txn_node, b as *mut pairingheap_node);
    if (*ta).size < (*tb).size {
        -1
    } else if (*ta).size > (*tb).size {
        1
    } else {
        0
    }
}

/*
 * Find the largest transaction to evict.
 */
unsafe fn ReorderBufferLargestTXN(rb: *mut ReorderBuffer) -> *mut ReorderBufferTXN {
    let largest = pairingheap_container!(
        ReorderBufferTXN, txn_node,
        pairingheap_first((*rb).txn_heap)
    );
    Assert!(!largest.is_null());
    Assert!((*largest).size > 0);
    Assert!((*largest).size <= (*rb).size);
    largest
}

/*
 * Find the largest streamable (and non-aborted) toplevel transaction.
 */
unsafe fn ReorderBufferLargestStreamableTopTXN(
    rb: *mut ReorderBuffer,
) -> *mut ReorderBufferTXN {
    let mut largest_size: Size = 0;
    let mut largest: *mut ReorderBufferTXN = null_mut();

    let mut iter: dlist_iter = core::mem::zeroed();
    dlist_foreach!(iter, &mut (*rb).txns_by_base_snapshot_lsn, {
        let txn = dlist_container!(ReorderBufferTXN, base_snapshot_node, iter.cur);
        Assert!(!rbtxn_is_known_subxact(txn));
        Assert!(!(*txn).base_snapshot.is_null());

        if rbtxn_has_partial_change(txn)
            || !rbtxn_has_streamable_change(txn)
            || rbtxn_is_aborted(txn)
        {
            continue;
        }

        if (largest.is_null() || (*txn).total_size > largest_size) && (*txn).total_size > 0 {
            largest = txn;
            largest_size = (*txn).total_size;
        }
    });

    largest
}

/*
 * Check whether the logical_decoding_work_mem limit was reached.
 */
unsafe fn ReorderBufferCheckMemoryLimit(rb: *mut ReorderBuffer) {
    if debug_logical_replication_streaming == DEBUG_LOGICAL_REP_STREAMING_BUFFERED
        && (*rb).size < logical_decoding_work_mem as usize * 1024
    {
        return;
    }

    loop {
        if !((*rb).size >= logical_decoding_work_mem as usize * 1024
            || (debug_logical_replication_streaming == DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE
                && (*rb).size > 0))
        {
            break;
        }

        let txn: *mut ReorderBufferTXN;
        if ReorderBufferCanStartStreaming(rb) {
            let t = ReorderBufferLargestStreamableTopTXN(rb);
            if !t.is_null() {
                txn = t;
                Assert!(rbtxn_is_toptxn(txn));
                Assert!((*txn).total_size > 0);
                Assert!((*rb).size >= (*txn).total_size);

                if ReorderBufferCheckAndTruncateAbortedTXN(rb, txn) {
                    continue;
                }
                ReorderBufferStreamTXN(rb, txn);
            } else {
                txn = ReorderBufferLargestTXN(rb);
                Assert!(!txn.is_null());
                Assert!((*txn).size > 0);
                Assert!((*rb).size >= (*txn).size);

                if ReorderBufferCheckAndTruncateAbortedTXN(rb, txn) {
                    continue;
                }
                ReorderBufferSerializeTXN(rb, txn);
            }
        } else {
            txn = ReorderBufferLargestTXN(rb);
            Assert!(!txn.is_null());
            Assert!((*txn).size > 0);
            Assert!((*rb).size >= (*txn).size);

            if ReorderBufferCheckAndTruncateAbortedTXN(rb, txn) {
                continue;
            }
            ReorderBufferSerializeTXN(rb, txn);
        }

        Assert!((*txn).size == 0);
        Assert!((*txn).nentries_mem == 0);
    }

    Assert!((*rb).size < logical_decoding_work_mem as usize * 1024);
}

/*
 * Spill data of a large transaction to disk.
 */
unsafe fn ReorderBufferSerializeTXN(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    let mut fd: c_int = -1;
    let mut cur_open_segno: XLogSegNo = 0;
    let mut spilled: Size = 0;
    let size = (*txn).size;

    elog!(DEBUG2, "spill {} changes in XID {} to disk",
         (*txn).nentries_mem as uint32, (*txn).xid);

    /* do the same to all child TXs */
    let mut subtxn_i: dlist_iter = core::mem::zeroed();
    dlist_foreach!(subtxn_i, &mut (*txn).subtxns, {
        let subtxn = dlist_container!(ReorderBufferTXN, node, subtxn_i.cur);
        ReorderBufferSerializeTXN(rb, subtxn);
    });

    /* serialize changestream */
    let mut change_i: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(change_i, &mut (*txn).changes, {
        let change = dlist_container!(ReorderBufferChange, node, change_i.cur);

        if fd == -1 || !XLByteInSeg((*change).lsn, cur_open_segno) {
            let mut path = [0i8; MAXPGPATH];
            if fd != -1 {
                CloseTransientFile(fd);
            }
            XLByteToSeg((*change).lsn, &mut cur_open_segno);
            ReorderBufferSerializedPath_fn(path.as_mut_ptr(), MyReplicationSlot, (*txn).xid, cur_open_segno);
            fd = OpenTransientFile(path.as_ptr(), O_CREAT | O_WRONLY | O_APPEND | PG_BINARY);
            if fd < 0 {
                ereport!(ERROR, errmsg!("could not open file: %m")) /* C also: errcode_for_file_access */;
            }
        }

        ReorderBufferSerializeChange(rb, txn, fd, change);
        dlist_delete(&mut (*change).node);
        ReorderBufferFreeChange(rb, change, false);
        spilled += 1;
    });

    /* Update the memory counter */
    ReorderBufferChangeMemoryUpdate(rb, null_mut(), txn, false, size);

    /* update the statistics iff we have spilled anything */
    if spilled > 0 {
        (*rb).spillCount += 1;
        (*rb).spillBytes += size as int64;
        (*rb).spillTxns += if rbtxn_is_serialized(txn) || rbtxn_is_serialized_clear(txn) { 0 } else { 1 };
        UpdateDecodingStats((*rb).private_data as *mut LogicalDecodingContext);
    }

    Assert!(spilled == (*txn).nentries_mem as usize);
    Assert!(dlist_is_empty(&(*txn).changes));
    (*txn).nentries_mem = 0;
    (*txn).txn_flags |= RBTXN_IS_SERIALIZED;

    if fd != -1 {
        CloseTransientFile(fd);
    }
}

/*
 * Serialize individual change to disk.
 */
unsafe fn ReorderBufferSerializeChange(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    fd: c_int,
    change: *mut ReorderBufferChange,
) {
    let mut sz = core::mem::size_of::<ReorderBufferDiskChange>();

    ReorderBufferSerializeReserve(rb, sz);

    let ondisk = (*rb).outbuf as *mut ReorderBufferDiskChange;
    memcpy(
        &mut (*ondisk).change as *mut ReorderBufferChange as *mut c_void,
        change as *const c_void,
        core::mem::size_of::<ReorderBufferChange>(),
    );

    match (*change).action {
        REORDER_BUFFER_CHANGE_INSERT
        | REORDER_BUFFER_CHANGE_UPDATE
        | REORDER_BUFFER_CHANGE_DELETE
        | REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT => {
            let oldtup = (*change).data.tp.oldtuple;
            let newtup = (*change).data.tp.newtuple;
            let mut oldlen: Size = 0;
            let mut newlen: Size = 0;

            if !oldtup.is_null() {
                sz += core::mem::size_of::<HeapTupleData>();
                oldlen = (*oldtup).t_len as usize;
                sz += oldlen;
            }
            if !newtup.is_null() {
                sz += core::mem::size_of::<HeapTupleData>();
                newlen = (*newtup).t_len as usize;
                sz += newlen;
            }

            ReorderBufferSerializeReserve(rb, sz);
            let mut data = ((*rb).outbuf as *mut u8)
                .add(core::mem::size_of::<ReorderBufferDiskChange>()) as *mut c_char;
            let ondisk = (*rb).outbuf as *mut ReorderBufferDiskChange;

            if oldlen > 0 {
                memcpy(data as *mut c_void, oldtup as *const c_void, core::mem::size_of::<HeapTupleData>());
                data = data.add(core::mem::size_of::<HeapTupleData>());
                memcpy(data as *mut c_void, (*oldtup).t_data as *const c_void, oldlen);
                data = data.add(oldlen);
            }
            if newlen > 0 {
                memcpy(data as *mut c_void, newtup as *const c_void, core::mem::size_of::<HeapTupleData>());
                data = data.add(core::mem::size_of::<HeapTupleData>());
                memcpy(data as *mut c_void, (*newtup).t_data as *const c_void, newlen);
                data = data.add(newlen);
            }
            let _ = ondisk;
        }
        REORDER_BUFFER_CHANGE_MESSAGE => {
            let prefix_size = strlen((*change).data.msg.prefix) + 1;
            sz += prefix_size + (*change).data.msg.message_size
                + core::mem::size_of::<Size>() + core::mem::size_of::<Size>();
            ReorderBufferSerializeReserve(rb, sz);

            let mut data = ((*rb).outbuf as *mut u8)
                .add(core::mem::size_of::<ReorderBufferDiskChange>()) as *mut c_char;

            memcpy(data as *mut c_void, &prefix_size as *const usize as *const c_void, core::mem::size_of::<Size>());
            data = data.add(core::mem::size_of::<Size>());
            memcpy(data as *mut c_void, (*change).data.msg.prefix as *const c_void, prefix_size);
            data = data.add(prefix_size);
            memcpy(data as *mut c_void, &(*change).data.msg.message_size as *const usize as *const c_void, core::mem::size_of::<Size>());
            data = data.add(core::mem::size_of::<Size>());
            memcpy(data as *mut c_void, (*change).data.msg.message as *const c_void, (*change).data.msg.message_size);
        }
        REORDER_BUFFER_CHANGE_INVALIDATION => {
            let inval_size = core::mem::size_of::<SharedInvalidationMessage>()
                * (*change).data.inval.ninvalidations as usize;
            sz += inval_size;
            ReorderBufferSerializeReserve(rb, sz);
            let data = ((*rb).outbuf as *mut u8)
                .add(core::mem::size_of::<ReorderBufferDiskChange>()) as *mut c_char;
            memcpy(data as *mut c_void, (*change).data.inval.invalidations as *const c_void, inval_size);
        }
        REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT => {
            let snap = (*change).data.snapshot;
            sz += core::mem::size_of::<SnapshotData>()
                + core::mem::size_of::<TransactionId>() * (*snap).xcnt as usize
                + core::mem::size_of::<TransactionId>() * (*snap).subxcnt as usize;
            ReorderBufferSerializeReserve(rb, sz);
            let mut data = ((*rb).outbuf as *mut u8)
                .add(core::mem::size_of::<ReorderBufferDiskChange>()) as *mut c_char;
            memcpy(data as *mut c_void, snap as *const c_void, core::mem::size_of::<SnapshotData>());
            data = data.add(core::mem::size_of::<SnapshotData>());
            if (*snap).xcnt > 0 {
                memcpy(data as *mut c_void, (*snap).xip as *const c_void,
                    core::mem::size_of::<TransactionId>() * (*snap).xcnt as usize);
                data = data.add(core::mem::size_of::<TransactionId>() * (*snap).xcnt as usize);
            }
            if (*snap).subxcnt > 0 {
                memcpy(data as *mut c_void, (*snap).subxip as *const c_void,
                    core::mem::size_of::<TransactionId>() * (*snap).subxcnt as usize);
            }
        }
        REORDER_BUFFER_CHANGE_TRUNCATE => {
            let size = core::mem::size_of::<Oid>() * (*change).data.truncate.nrelids;
            sz += size;
            ReorderBufferSerializeReserve(rb, sz);
            let data = ((*rb).outbuf as *mut u8)
                .add(core::mem::size_of::<ReorderBufferDiskChange>()) as *mut c_char;
            memcpy(data as *mut c_void, (*change).data.truncate.relids as *const c_void, size);
        }
        REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM
        | REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT
        | REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID
        | REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID => {
            /* ReorderBufferChange contains everything important */
        }
    }

    let ondisk = (*rb).outbuf as *mut ReorderBufferDiskChange;
    (*ondisk).size = sz;

    errno = 0;
    pgstat_report_wait_start(WAIT_EVENT_REORDER_BUFFER_WRITE);
    let written = write(fd, (*rb).outbuf as *const c_void, (*ondisk).size);
    pgstat_report_wait_end();
    if written as usize != (*ondisk).size {
        let save_errno = errno;
        CloseTransientFile(fd);
        errno = if save_errno != 0 { save_errno } else { ENOSPC };
        ereport!(ERROR, errmsg!("could not write to data file for XID {}", (*txn).xid)) /* C also: errcode_for_file_access */;
    }

    /* Keep the transaction's final_lsn up to date with each change. */
    if (*txn).final_lsn < (*change).lsn {
        (*txn).final_lsn = (*change).lsn;
    }

    Assert!((*ondisk).change.action == (*change).action);
}

/*
 * Returns true if the output plugin supports streaming.
 */
#[inline]
unsafe fn ReorderBufferCanStream(rb: *mut ReorderBuffer) -> bool {
    let ctx = (*rb).private_data as *mut LogicalDecodingContext;
    if ctx.is_null() { return false; }
    (*ctx).streaming
}

/*
 * Returns true if the streaming can be started now.
 */
#[inline]
unsafe fn ReorderBufferCanStartStreaming(rb: *mut ReorderBuffer) -> bool {
    let ctx = (*rb).private_data as *mut LogicalDecodingContext;
    if ctx.is_null() { return false; }
    let builder = (*ctx).snapshot_builder;

    if SnapBuildCurrentState(builder) < SNAPBUILD_CONSISTENT {
        return false;
    }
    if ReorderBufferCanStream(rb)
        && !SnapBuildXactNeedsSkip(builder, (*(*ctx).reader).ReadRecPtr)
    {
        return true;
    }
    false
}

/*
 * Send data of a large transaction to the output plugin using the stream API.
 */
unsafe fn ReorderBufferStreamTXN(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    Assert!(rbtxn_is_toptxn(txn));

    let snapshot_now: Snapshot;
    let command_id: CommandId;

    if (*txn).snapshot_now.is_null() {
        Assert!(!rbtxn_is_streamed(txn));
        Assert!((*txn).command_id == InvalidCommandId);

        let mut subxact_i: dlist_iter = core::mem::zeroed();
        dlist_foreach!(subxact_i, &mut (*txn).subtxns, {
            let subtxn = dlist_container!(ReorderBufferTXN, node, subxact_i.cur);
            ReorderBufferTransferSnapToParent(txn, subtxn);
        });

        if (*txn).base_snapshot.is_null() {
            Assert!((*txn).ninvalidations == 0);
            return;
        }

        command_id = FirstCommandId;
        snapshot_now = ReorderBufferCopySnap(rb, (*txn).base_snapshot, txn, command_id);
    } else {
        Assert!(rbtxn_is_streamed(txn));

        command_id = (*txn).command_id;
        snapshot_now = ReorderBufferCopySnap(rb, (*txn).snapshot_now, txn, command_id);

        Assert!((*(*txn).snapshot_now).copied);
        ReorderBufferFreeSnap(rb, (*txn).snapshot_now);
        (*txn).snapshot_now = null_mut();
    }

    let txn_is_streamed = rbtxn_is_streamed(txn);
    let stream_bytes = (*txn).total_size;

    /* Process and send the changes to output plugin. */
    ReorderBufferProcessTXN(rb, txn, InvalidXLogRecPtr, snapshot_now, command_id, true);

    (*rb).streamCount += 1;
    (*rb).streamBytes += stream_bytes as int64;
    (*rb).streamTxns += if txn_is_streamed { 0 } else { 1 };

    UpdateDecodingStats((*rb).private_data as *mut LogicalDecodingContext);

    Assert!(dlist_is_empty(&(*txn).changes));
    Assert!((*txn).nentries == 0);
    Assert!((*txn).nentries_mem == 0);
}

/*
 * Size of a change in memory.
 */
unsafe fn ReorderBufferChangeSize(change: *mut ReorderBufferChange) -> Size {
    let mut sz = core::mem::size_of::<ReorderBufferChange>();

    match (*change).action {
        REORDER_BUFFER_CHANGE_INSERT
        | REORDER_BUFFER_CHANGE_UPDATE
        | REORDER_BUFFER_CHANGE_DELETE
        | REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT => {
            let oldtup = (*change).data.tp.oldtuple;
            let newtup = (*change).data.tp.newtuple;
            if !oldtup.is_null() {
                sz += core::mem::size_of::<HeapTupleData>() + (*oldtup).t_len as usize;
            }
            if !newtup.is_null() {
                sz += core::mem::size_of::<HeapTupleData>() + (*newtup).t_len as usize;
            }
        }
        REORDER_BUFFER_CHANGE_MESSAGE => {
            let prefix_size = strlen((*change).data.msg.prefix) + 1;
            sz += prefix_size + (*change).data.msg.message_size
                + core::mem::size_of::<Size>() + core::mem::size_of::<Size>();
        }
        REORDER_BUFFER_CHANGE_INVALIDATION => {
            sz += core::mem::size_of::<SharedInvalidationMessage>()
                * (*change).data.inval.ninvalidations as usize;
        }
        REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT => {
            let snap = (*change).data.snapshot;
            sz += core::mem::size_of::<SnapshotData>()
                + core::mem::size_of::<TransactionId>() * (*snap).xcnt as usize
                + core::mem::size_of::<TransactionId>() * (*snap).subxcnt as usize;
        }
        REORDER_BUFFER_CHANGE_TRUNCATE => {
            sz += core::mem::size_of::<Oid>() * (*change).data.truncate.nrelids;
        }
        REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM
        | REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT
        | REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID
        | REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID => {}
    }

    sz
}

// ===========================================================================
// Part 7: Restore changes from disk, cleanup, StartupReorderBuffer,
//         toast assembly, visibility (ResolveCminCmax), GetInvalidations
// ===========================================================================

/*
 * Restore a number of changes spilled to disk back into memory.
 */
unsafe fn ReorderBufferRestoreChanges(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    file: *mut TXNEntryFile,
    segno: *mut XLogSegNo,
) -> Size {
    let mut restored: Size = 0;
    let fd = &mut (*file).vfd;

    Assert!((*txn).first_lsn != InvalidXLogRecPtr);
    Assert!((*txn).final_lsn != InvalidXLogRecPtr);

    /* free current entries, so we have memory for more */
    let mut cleanup_iter: dlist_mutable_iter = core::mem::zeroed();
    dlist_foreach_modify!(cleanup_iter, &mut (*txn).changes, {
        let cleanup = dlist_container!(ReorderBufferChange, node, cleanup_iter.cur);
        dlist_delete(&mut (*cleanup).node);
        ReorderBufferFreeChange(rb, cleanup, true);
    });
    (*txn).nentries_mem = 0;
    Assert!(dlist_is_empty(&(*txn).changes));

    let mut last_segno: XLogSegNo = 0;
    XLByteToSeg((*txn).final_lsn, &mut last_segno);

    while restored < MAX_CHANGES_IN_MEMORY && *segno <= last_segno {
        CHECK_FOR_INTERRUPTS();

        if *fd == -1 {
            let mut path = [0i8; MAXPGPATH];

            /* first time in */
            if *segno == 0 {
                XLByteToSeg((*txn).first_lsn, &mut *segno);
            }

            ReorderBufferSerializedPath_fn(path.as_mut_ptr(), MyReplicationSlot, (*txn).xid, *segno);

            *fd = PathNameOpenFile(path.as_ptr(), O_RDONLY | PG_BINARY);
            /* No harm in resetting the offset even in case of failure */
            (*file).curOffset = 0;

            if *fd < 0 && errno == ENOENT {
                *fd = -1;
                *segno += 1;
                continue;
            } else if *fd < 0 {
                ereport!(ERROR, errmsg!("could not open file: %m")) /* C also: errcode_for_file_access */;
            }
        }

        /* Read the statically sized part of a change */
        ReorderBufferSerializeReserve(rb, core::mem::size_of::<ReorderBufferDiskChange>());
        let read_bytes = FileRead(
            (*file).vfd,
            (*rb).outbuf as *mut c_void,
            core::mem::size_of::<ReorderBufferDiskChange>() as c_int,
            (*file).curOffset,
            WAIT_EVENT_REORDER_BUFFER_READ,
        );

        /* eof */
        if read_bytes == 0 {
            FileClose(*fd);
            *fd = -1;
            *segno += 1;
            continue;
        } else if read_bytes < 0 {
            ereport!(ERROR, errmsg!("could not read from reorderbuffer spill file: %m")) /* C also: errcode_for_file_access */;
        } else if (read_bytes as usize) != core::mem::size_of::<ReorderBufferDiskChange>() {
            ereport!(ERROR, errmsg!("could not read from reorderbuffer spill file: read {} instead of {} bytes",
                    read_bytes, core::mem::size_of::<ReorderBufferDiskChange>())) /* C also: errcode_for_file_access */;
        }

        (*file).curOffset += read_bytes as i64;

        let ondisk = (*rb).outbuf as *mut ReorderBufferDiskChange;

        ReorderBufferSerializeReserve(
            rb,
            core::mem::size_of::<ReorderBufferDiskChange>() + (*ondisk).size,
        );
        let ondisk = (*rb).outbuf as *mut ReorderBufferDiskChange;

        let read_bytes2 = FileRead(
            (*file).vfd,
            ((*rb).outbuf as *mut u8)
                .add(core::mem::size_of::<ReorderBufferDiskChange>()) as *mut c_void,
            ((*ondisk).size - core::mem::size_of::<ReorderBufferDiskChange>()) as c_int,
            (*file).curOffset,
            WAIT_EVENT_REORDER_BUFFER_READ,
        );

        if read_bytes2 < 0 {
            ereport!(ERROR, errmsg!("could not read from reorderbuffer spill file: %m")) /* C also: errcode_for_file_access */;
        } else if (read_bytes2 as usize)
            != (*ondisk).size - core::mem::size_of::<ReorderBufferDiskChange>()
        {
            ereport!(ERROR, errmsg!("could not read from reorderbuffer spill file: short read")) /* C also: errcode_for_file_access */;
        }

        (*file).curOffset += read_bytes2 as i64;

        /* restore change into proper in-memory format */
        ReorderBufferRestoreChange(rb, txn, (*rb).outbuf);
        restored += 1;
    }

    restored
}

/*
 * Convert change from its on-disk format to in-memory format.
 */
unsafe fn ReorderBufferRestoreChange(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    data: *mut c_char,
) {
    let ondisk = data as *mut ReorderBufferDiskChange;
    let change = ReorderBufferAllocChange(rb);

    /* copy static part */
    memcpy(
        change as *mut c_void,
        &(*ondisk).change as *const ReorderBufferChange as *const c_void,
        core::mem::size_of::<ReorderBufferChange>(),
    );

    let mut data = data.add(core::mem::size_of::<ReorderBufferDiskChange>());

    /* restore individual stuff */
    match (*change).action {
        REORDER_BUFFER_CHANGE_INSERT
        | REORDER_BUFFER_CHANGE_UPDATE
        | REORDER_BUFFER_CHANGE_DELETE
        | REORDER_BUFFER_CHANGE_INTERNAL_SPEC_INSERT => {
            if !(*change).data.tp.oldtuple.is_null() {
                let tuplelen = (*(data as *const HeapTupleData)).t_len as usize;
                (*change).data.tp.oldtuple =
                    ReorderBufferAllocTupleBuf(rb, tuplelen - SizeofHeapTupleHeader);
                memcpy(
                    (*change).data.tp.oldtuple as *mut c_void,
                    data as *const c_void,
                    core::mem::size_of::<HeapTupleData>(),
                );
                data = data.add(core::mem::size_of::<HeapTupleData>());
                (*(*change).data.tp.oldtuple).t_data =
                    ((*change).data.tp.oldtuple as *mut u8).add(HEAPTUPLESIZE) as HeapTupleHeader;
                memcpy(
                    (*(*change).data.tp.oldtuple).t_data as *mut c_void,
                    data as *const c_void,
                    tuplelen,
                );
                data = data.add(tuplelen);
            }

            if !(*change).data.tp.newtuple.is_null() {
                let mut tuplelen: uint32 = 0;
                memcpy(
                    &mut tuplelen as *mut uint32 as *mut c_void,
                    data.add(core::mem::offset_of!(HeapTupleData, t_len)) as *const c_void,
                    core::mem::size_of::<uint32>(),
                );
                let tuplelen = tuplelen as usize;
                (*change).data.tp.newtuple =
                    ReorderBufferAllocTupleBuf(rb, tuplelen - SizeofHeapTupleHeader);
                memcpy(
                    (*change).data.tp.newtuple as *mut c_void,
                    data as *const c_void,
                    core::mem::size_of::<HeapTupleData>(),
                );
                data = data.add(core::mem::size_of::<HeapTupleData>());
                (*(*change).data.tp.newtuple).t_data =
                    ((*change).data.tp.newtuple as *mut u8).add(HEAPTUPLESIZE) as HeapTupleHeader;
                memcpy(
                    (*(*change).data.tp.newtuple).t_data as *mut c_void,
                    data as *const c_void,
                    tuplelen,
                );
                data = data.add(tuplelen);
            }
        }
        REORDER_BUFFER_CHANGE_MESSAGE => {
            let mut prefix_size: Size = 0;
            memcpy(&mut prefix_size as *mut Size as *mut c_void, data as *const c_void, core::mem::size_of::<Size>());
            data = data.add(core::mem::size_of::<Size>());
            (*change).data.msg.prefix = mc_alloc((*rb).context, prefix_size) as *mut c_char;
            memcpy((*change).data.msg.prefix as *mut c_void, data as *const c_void, prefix_size);
            data = data.add(prefix_size);

            memcpy(&mut (*change).data.msg.message_size as *mut Size as *mut c_void,
                   data as *const c_void, core::mem::size_of::<Size>());
            data = data.add(core::mem::size_of::<Size>());
            (*change).data.msg.message = mc_alloc((*rb).context, (*change).data.msg.message_size) as *mut c_char;
            memcpy((*change).data.msg.message as *mut c_void, data as *const c_void,
                   (*change).data.msg.message_size);
        }
        REORDER_BUFFER_CHANGE_INVALIDATION => {
            let inval_size = core::mem::size_of::<SharedInvalidationMessage>()
                * (*change).data.inval.ninvalidations as usize;
            (*change).data.inval.invalidations =
                mc_alloc((*rb).context, inval_size) as *mut SharedInvalidationMessage;
            memcpy((*change).data.inval.invalidations as *mut c_void, data as *const c_void, inval_size);
        }
        REORDER_BUFFER_CHANGE_INTERNAL_SNAPSHOT => {
            let oldsnap = data as *mut SnapshotData;
            let size = core::mem::size_of::<SnapshotData>()
                + core::mem::size_of::<TransactionId>() * (*oldsnap).xcnt as usize
                + core::mem::size_of::<TransactionId>() * (*oldsnap).subxcnt as usize;

            (*change).data.snapshot = mc_alloc_zero((*rb).context, size) as Snapshot;
            let newsnap = (*change).data.snapshot;
            memcpy(newsnap as *mut c_void, data as *const c_void, size);
            (*newsnap).xip = (newsnap as *mut u8).add(core::mem::size_of::<SnapshotData>())
                as *mut TransactionId;
            (*newsnap).subxip = (*newsnap).xip.add((*newsnap).xcnt as usize);
            (*newsnap).copied = true;
        }
        REORDER_BUFFER_CHANGE_TRUNCATE => {
            let relids = ReorderBufferAllocRelids(rb, (*change).data.truncate.nrelids as c_int);
            memcpy(
                relids as *mut c_void,
                data as *const c_void,
                (*change).data.truncate.nrelids * core::mem::size_of::<Oid>(),
            );
            (*change).data.truncate.relids = relids;
        }
        REORDER_BUFFER_CHANGE_INTERNAL_SPEC_CONFIRM
        | REORDER_BUFFER_CHANGE_INTERNAL_SPEC_ABORT
        | REORDER_BUFFER_CHANGE_INTERNAL_COMMAND_ID
        | REORDER_BUFFER_CHANGE_INTERNAL_TUPLECID => {}
    }

    dlist_push_tail(&mut (*txn).changes, &mut (*change).node);
    (*txn).nentries_mem += 1;

    /*
     * Update memory accounting for the restored change.
     */
    ReorderBufferChangeMemoryUpdate(
        rb, change, null_mut(), true, ReorderBufferChangeSize(change),
    );
}

/*
 * Remove all on-disk stored data for the passed in transaction.
 */
unsafe fn ReorderBufferRestoreCleanup(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    let mut first: XLogSegNo = 0;
    let mut last: XLogSegNo = 0;

    Assert!((*txn).first_lsn != InvalidXLogRecPtr);
    Assert!((*txn).final_lsn != InvalidXLogRecPtr);

    XLByteToSeg((*txn).first_lsn, &mut first);
    XLByteToSeg((*txn).final_lsn, &mut last);

    for cur in first..=last {
        let mut path = [0i8; MAXPGPATH];
        ReorderBufferSerializedPath_fn(path.as_mut_ptr(), MyReplicationSlot, (*txn).xid, cur);
        if unlink(path.as_ptr()) != 0 && errno != ENOENT {
            ereport!(ERROR, errmsg!("could not remove file: %m")) /* C also: errcode_for_file_access */;
        }
    }
}

/*
 * Remove any leftover serialized reorder buffers from a slot directory.
 */
unsafe fn ReorderBufferCleanupSerializedTXNs(slotname: *const c_char) {
    let mut path_buf = [0i8; MAXPGPATH * 2 + 32];
    sprintf(
        path_buf.as_mut_ptr(),
        b"%s/%s\0".as_ptr() as *const c_char,
        PG_REPLSLOT_DIR.as_ptr(),
        slotname,
    );

    let mut statbuf: StatBuf = core::mem::zeroed();
    if lstat(path_buf.as_ptr(), &mut statbuf) == 0 && !S_ISDIR(statbuf.st_mode) {
        return;
    }

    let spill_dir = AllocateDir(path_buf.as_ptr());
    loop {
        let spill_de = ReadDirExtended(spill_dir, path_buf.as_ptr(), INFO);
        if spill_de.is_null() {
            break;
        }
        if strncmp((*spill_de).d_name.as_ptr(), b"xid\0".as_ptr() as *const c_char, 3) == 0 {
            let mut full_path = [0i8; MAXPGPATH * 2 + 32];
            snprintf(
                full_path.as_mut_ptr(),
                full_path.len(),
                b"%s/%s/%s\0".as_ptr() as *const c_char,
                PG_REPLSLOT_DIR.as_ptr(),
                slotname,
                (*spill_de).d_name.as_ptr(),
            );
            if unlink(full_path.as_ptr()) != 0 {
                ereport!(ERROR, errmsg!("could not remove file: %m")) /* C also: errcode_for_file_access */;
            }
        }
    }
    FreeDir(spill_dir);
}

// Convenience wrapper that takes a raw pointer
unsafe fn ReorderBufferCleanupSerializedTXNs_cstr(slotname: *const c_char) {
    ReorderBufferCleanupSerializedTXNs(slotname);
}

/*
 * Fill in the path for a spill file.
 */
unsafe fn ReorderBufferSerializedPath_fn(
    path: *mut c_char,
    _slot: *mut ReplicationSlot,
    xid: TransactionId,
    segno: XLogSegNo,
) {
    let recptr = XLogSegNoOffsetToRecPtr(segno, 0);
    snprintf(
        path,
        MAXPGPATH,
        b"%s/%s/xid-%u-lsn-%X-%X.spill\0".as_ptr() as *const c_char,
        PG_REPLSLOT_DIR.as_ptr(),
        NameStr!((*MyReplicationSlot).data.name),
        xid,
        (recptr >> 32) as uint32,
        recptr as uint32,
    );
}

/*
 * Delete all data spilled to disk after we've restarted/crashed.
 */
pub unsafe fn StartupReorderBuffer() {
    let logical_dir = AllocateDir(PG_REPLSLOT_DIR.as_ptr() as *const c_char);
    loop {
        let logical_de = ReadDir(logical_dir, PG_REPLSLOT_DIR.as_ptr() as *const c_char);
        if logical_de.is_null() {
            break;
        }
        if strcmp((*logical_de).d_name.as_ptr(), b".\0".as_ptr() as *const c_char) == 0
            || strcmp((*logical_de).d_name.as_ptr(), b"..\0".as_ptr() as *const c_char) == 0
        {
            continue;
        }

        if !ReplicationSlotValidateName((*logical_de).d_name.as_ptr(), DEBUG2) {
            continue;
        }

        ReorderBufferCleanupSerializedTXNs((*logical_de).d_name.as_ptr());
    }
    FreeDir(logical_dir);
}

// ---------------------------------------------------------------------------
// Toast reassembly
// ---------------------------------------------------------------------------

/*
 * Initialize per tuple toast reconstruction support.
 */
unsafe fn ReorderBufferToastInitHash(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    Assert!((*txn).toast_hash.is_null());

    let mut hash_ctl = HASHCTL {
        keysize: core::mem::size_of::<Oid>(),
        entrysize: core::mem::size_of::<ReorderBufferToastEnt>(),
        hcxt: (*rb).context,
    };
    (*txn).toast_hash = hash_create(
        "ReorderBufferToastHash",
        5,
        &mut hash_ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );
}

/*
 * Per toast-chunk handling for toast reconstruction.
 */
unsafe fn ReorderBufferToastAppendChunk(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    relation: Relation,
    change: *mut ReorderBufferChange,
) {
    let mut found: bool = false;
    let mut isnull: bool = false;

    if (*txn).toast_hash.is_null() {
        ReorderBufferToastInitHash(rb, txn);
    }

    Assert!(IsToastRelation(relation));

    let newtup = (*change).data.tp.newtuple;
    let desc = RelationGetDescr(relation);
    let chunk_id = DatumGetObjectId(fastgetattr(newtup, 1, desc, &mut isnull)) as Oid;
    Assert!(!isnull);
    let chunk_seq = DatumGetInt32(fastgetattr(newtup, 2, desc, &mut isnull));
    Assert!(!isnull);

    let ent = hash_search(
        (*txn).toast_hash,
        &chunk_id as *const Oid as *const c_void,
        HASH_ENTER,
        &mut found as *mut bool,
    ) as *mut ReorderBufferToastEnt;

    if !found {
        Assert!((*ent).chunk_id == chunk_id);
        (*ent).num_chunks = 0;
        (*ent).last_chunk_seq = 0;
        (*ent).size = 0;
        (*ent).reconstructed = null_mut();
        dlist_init(&mut (*ent).chunks);

        if chunk_seq != 0 {
            elog!(ERROR,
                "got sequence entry {} for toast chunk {} instead of seq 0",
                chunk_seq, chunk_id);
        }
    } else if chunk_seq != (*ent).last_chunk_seq + 1 {
        elog!(ERROR,
            "got sequence entry {} for toast chunk {} instead of seq {}",
            chunk_seq, chunk_id, (*ent).last_chunk_seq + 1);
    }

    let chunk = DatumGetPointer(fastgetattr(newtup, 3, desc, &mut isnull)) as *mut varlena;
    Assert!(!isnull);

    let chunksize: Size;
    if !VARATT_IS_EXTENDED(chunk) {
        chunksize = VARSIZE(chunk) - VARHDRSZ;
    } else if VARATT_IS_SHORT(chunk) {
        chunksize = VARSIZE_SHORT(chunk) - VARHDRSZ_SHORT;
    } else {
        elog!(ERROR, "unexpected type of toast chunk");
        return;
    }

    (*ent).size += chunksize;
    (*ent).last_chunk_seq = chunk_seq;
    (*ent).num_chunks += 1;
    dlist_push_tail(&mut (*ent).chunks, &mut (*change).node);
}

/*
 * Rejigger change->newtuple to point to in-memory toast tuples.
 */
unsafe fn ReorderBufferToastReplace(
    rb: *mut ReorderBuffer,
    txn: *mut ReorderBufferTXN,
    relation: Relation,
    change: *mut ReorderBufferChange,
) {
    if (*txn).toast_hash.is_null() {
        return;
    }

    let old_size = ReorderBufferChangeSize(change);
    let oldcontext = MemoryContextSwitchTo((*rb).context);

    Assert!(!(*change).data.tp.newtuple.is_null());

    let desc = RelationGetDescr(relation);
    let toast_rel = RelationIdGetRelation(/* reltoastrelid stub */ 0);

    // NOTE: actual toast reconstruction requires Relation rd_rel access.
    // The full implementation deforms tuple, checks each EXTERNAL varlena attr,
    // and reconstructs from in-memory chunks. This translation preserves the
    // structure but cannot fully execute without real Relation/TupleDesc stubs.
    // TODO(pg-port): complete once Relation/TupleDesc are properly ported.

    MemoryContextSwitchTo(oldcontext);

    /* subtract the old change size */
    ReorderBufferChangeMemoryUpdate(rb, change, null_mut(), false, old_size);
    /* now add the change back, with the correct size */
    ReorderBufferChangeMemoryUpdate(
        rb, change, null_mut(), true, ReorderBufferChangeSize(change),
    );
}

/*
 * Free all resources allocated for toast reconstruction.
 */
unsafe fn ReorderBufferToastReset(rb: *mut ReorderBuffer, txn: *mut ReorderBufferTXN) {
    if (*txn).toast_hash.is_null() {
        return;
    }

    let mut hstat: HASH_SEQ_STATUS = core::mem::zeroed();
    hash_seq_init(&mut hstat, (*txn).toast_hash);
    loop {
        let ent = hash_seq_search(&mut hstat) as *mut ReorderBufferToastEnt;
        if ent.is_null() {
            break;
        }

        if !(*ent).reconstructed.is_null() {
            pfree((*ent).reconstructed as *mut c_void);
        }

        let mut it: dlist_mutable_iter = core::mem::zeroed();
        dlist_foreach_modify!(it, &mut (*ent).chunks, {
            let ch_change = dlist_container!(ReorderBufferChange, node, it.cur);
            dlist_delete(&mut (*ch_change).node);
            ReorderBufferFreeChange(rb, ch_change, true);
        });
    }

    hash_destroy((*txn).toast_hash);
    (*txn).toast_hash = null_mut();
}

// ---------------------------------------------------------------------------
// Visibility support for logical decoding
// ---------------------------------------------------------------------------

/*
 * Check whether xid is in the pre-sorted array xip.
 */
unsafe fn TransactionIdInArray(xid: TransactionId, xip: *mut TransactionId, num: Size) -> bool {
    !bsearch(
        &xid as *const TransactionId as *const c_void,
        xip as *const c_void,
        num,
        core::mem::size_of::<TransactionId>(),
        xidComparator,
    )
    .is_null()
}

/*
 * list_sort() comparator for sorting RewriteMappingFiles in LSN order.
 */
unsafe fn file_sort_by_lsn(a_p: *const ListCell, b_p: *const ListCell) -> c_int {
    let a = lfirst(a_p) as *const RewriteMappingFile;
    let b = lfirst(b_p) as *const RewriteMappingFile;
    pg_cmp_u64((*a).lsn, (*b).lsn)
}

/*
 * Apply a single mapping file to tuplecid_data.
 */
unsafe fn ApplyLogicalMappingFile(
    tuplecid_data: *mut HTAB,
    relid: Oid,
    fname: *const c_char,
) {
    let mut path = [0i8; MAXPGPATH];
    sprintf(
        path.as_mut_ptr(),
        b"%s/%s\0".as_ptr() as *const c_char,
        PG_LOGICAL_MAPPINGS_DIR.as_ptr(),
        fname,
    );
    let fd = OpenTransientFile(path.as_ptr(), O_RDONLY | PG_BINARY);
    if fd < 0 {
        ereport!(ERROR, errmsg!("could not open file: %m")) /* C also: errcode_for_file_access */;
    }

    loop {
        let mut key: ReorderBufferTupleCidKey = core::mem::zeroed();
        let mut map: LogicalRewriteMappingData = core::mem::zeroed();

        memset(&mut key as *mut ReorderBufferTupleCidKey as *mut c_void, 0,
               core::mem::size_of::<ReorderBufferTupleCidKey>());

        pgstat_report_wait_start(WAIT_EVENT_REORDER_LOGICAL_MAPPING_READ);
        let read_bytes = read(
            fd,
            &mut map as *mut LogicalRewriteMappingData as *mut c_void,
            core::mem::size_of::<LogicalRewriteMappingData>(),
        );
        pgstat_report_wait_end();

        if read_bytes < 0 {
            ereport!(ERROR, errmsg!("could not read file: %m")) /* C also: errcode_for_file_access */;
        } else if read_bytes == 0 {
            break; /* EOF */
        } else if (read_bytes as usize) != core::mem::size_of::<LogicalRewriteMappingData>() {
            ereport!(ERROR, errmsg!("could not read from file: short read")) /* C also: errcode_for_file_access */;
        }

        key.rlocator = map.old_locator;
        ItemPointerCopy(&map.old_tid, &mut key.tid);

        let ent = hash_search(
            tuplecid_data,
            &key as *const ReorderBufferTupleCidKey as *const c_void,
            HASH_FIND,
            null_mut(),
        ) as *mut ReorderBufferTupleCidEnt;

        if ent.is_null() {
            continue;
        }

        key.rlocator = map.new_locator;
        ItemPointerCopy(&map.new_tid, &mut key.tid);

        let mut found: bool = false;
        let new_ent = hash_search(
            tuplecid_data,
            &key as *const ReorderBufferTupleCidKey as *const c_void,
            HASH_ENTER,
            &mut found as *mut bool,
        ) as *mut ReorderBufferTupleCidEnt;

        if found {
            Assert!((*ent).cmin == InvalidCommandId || (*ent).cmin == (*new_ent).cmin);
            Assert!((*ent).cmax == InvalidCommandId || (*ent).cmax == (*new_ent).cmax);
        } else {
            (*new_ent).cmin = (*ent).cmin;
            (*new_ent).cmax = (*ent).cmax;
            (*new_ent).combocid = (*ent).combocid;
        }
    }

    if CloseTransientFile(fd) != 0 {
        ereport!(ERROR, errmsg!("could not close file: %m")) /* C also: errcode_for_file_access */;
    }
}

/*
 * Apply any existing logical remapping files for relid.
 */
unsafe fn UpdateLogicalMappings(
    tuplecid_data: *mut HTAB,
    relid: Oid,
    snapshot: Snapshot,
) {
    let mapping_dir = AllocateDir(PG_LOGICAL_MAPPINGS_DIR.as_ptr() as *const c_char);
    let mut files: *mut List = NIL;

    loop {
        let mapping_de = ReadDir(mapping_dir, PG_LOGICAL_MAPPINGS_DIR.as_ptr() as *const c_char);
        if mapping_de.is_null() {
            break;
        }
        let name = (*mapping_de).d_name.as_ptr();
        if strcmp(name, b".\0".as_ptr() as *const c_char) == 0
            || strcmp(name, b"..\0".as_ptr() as *const c_char) == 0
        {
            continue;
        }

        if strncmp(name, b"map-\0".as_ptr() as *const c_char, 4) != 0 {
            continue;
        }

        let mut f_dboid: Oid = 0;
        let mut f_relid: Oid = 0;
        let mut f_mapped_xid: TransactionId = 0;
        let mut f_create_xid: TransactionId = 0;
        let mut f_hi: uint32 = 0;
        let mut f_lo: uint32 = 0;

        if sscanf(
            name,
            LOGICAL_REWRITE_FORMAT.as_ptr() as *const c_char,
            &mut f_dboid, &mut f_relid, &mut f_hi, &mut f_lo,
            &mut f_mapped_xid, &mut f_create_xid,
        ) != 6 {
            elog!(ERROR, "could not parse filename");
        }

        let f_lsn: XLogRecPtr = ((f_hi as u64) << 32) | f_lo as u64;
        let dboid: Oid = if IsSharedRelation(relid) { 0 } else { MyDatabaseId };

        if f_dboid != dboid { continue; }
        if f_relid != relid { continue; }
        if !TransactionIdDidCommit(f_create_xid) { continue; }
        if !TransactionIdInArray(f_mapped_xid, (*snapshot).subxip, (*snapshot).subxcnt as usize) {
            continue;
        }

        let f = palloc(core::mem::size_of::<RewriteMappingFile>()) as *mut RewriteMappingFile;
        (*f).lsn = f_lsn;
        strcpy((*f).fname.as_mut_ptr(), name);
        files = lappend(files, f as *mut c_void);
    }
    FreeDir(mapping_dir);

    /* sort files so we apply them in LSN order */
    // list_sort(files, file_sort_by_lsn); // TODO(pg-port): requires List iterator
}

/*
 * Lookup cmin/cmax of a tuple, during logical decoding.
 */
pub unsafe fn ResolveCminCmaxDuringDecoding(
    tuplecid_data: *mut HTAB,
    snapshot: Snapshot,
    htup: HeapTuple,
    buffer: Buffer,
    cmin: *mut CommandId,
    cmax: *mut CommandId,
) -> bool {
    if tuplecid_data.is_null() {
        return false;
    }

    let mut key: ReorderBufferTupleCidKey = core::mem::zeroed();
    memset(&mut key as *mut ReorderBufferTupleCidKey as *mut c_void, 0,
           core::mem::size_of::<ReorderBufferTupleCidKey>());

    Assert!(!BufferIsLocal(buffer));

    let mut forkno: c_int = 0;
    let mut blockno: uint32 = 0;
    BufferGetTag(buffer, &mut key.rlocator, &mut forkno, &mut blockno);

    Assert!(forkno == MAIN_FORKNUM);
    Assert!(blockno == ItemPointerGetBlockNumber(&(*htup).t_self as *const _ as *const ItemPointerData));

    ItemPointerCopy(&(*htup).t_self as *const _ as *const ItemPointerData, &mut key.tid);

    let mut updated_mapping = false;

    'restart: loop {
        let ent = hash_search(
            tuplecid_data,
            &key as *const ReorderBufferTupleCidKey as *const c_void,
            HASH_FIND,
            null_mut(),
        ) as *mut ReorderBufferTupleCidEnt;

        if ent.is_null() && !updated_mapping {
            UpdateLogicalMappings(tuplecid_data, (*htup).t_tableOid, snapshot);
            updated_mapping = true;
            continue 'restart;
        } else if ent.is_null() {
            return false;
        }

        if !cmin.is_null() {
            *cmin = (*ent).cmin;
        }
        if !cmax.is_null() {
            *cmax = (*ent).cmax;
        }
        return true;
    }
}

/*
 * Count invalidation messages of specified transaction.
 */
pub unsafe fn ReorderBufferGetInvalidations(
    rb: *mut ReorderBuffer,
    xid: TransactionId,
    msgs: *mut *mut SharedInvalidationMessage,
) -> uint32 {
    let txn = ReorderBufferTXNByXid(rb, xid, false, null_mut(), InvalidXLogRecPtr, false);

    if txn.is_null() {
        return 0;
    }

    *msgs = (*txn).invalidations;
    (*txn).ninvalidations
}

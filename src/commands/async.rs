//! async.rs -- Asynchronous notification: NOTIFY, LISTEN, UNLISTEN
//!
//! 1:1 translation of postgres/src/backend/commands/async.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/commands/async.c
//!
//! -------------------------------------------------------------------------
//!
//! STUB list (all marked TODO(pg-port)):
//!   - SimpleLruGetBankLock / SimpleLruZeroPage / SimpleLruReadPage /
//!     SimpleLruReadPage_ReadOnly / SimpleLruTruncate / SimpleLruInit /
//!     SimpleLruShmemSize / SlruScanDirectory / SlruScanDirCbDeleteAll
//!     (access/slru.c -- not yet ported)
//!   - LWLockAcquire / LWLockRelease (storage/lwlock.c)
//!   - LockSharedObject (storage/lmgr.c)
//!   - ShmemInitStruct / mul_size / add_size (storage/shmem.c)
//!   - before_shmem_exit (storage/ipc.c)
//!   - SendProcSignal / PROCSIG_NOTIFY_INTERRUPT (storage/procsignal.c)
//!   - SetLatch (storage/latch.c)
//!   - RegisterSnapshot / UnregisterSnapshot / GetLatestSnapshot (utils/snapmgr.c)
//!   - XidInMVCCSnapshot (utils/snapmgr.c)
//!   - TransactionIdDidCommit (access/transam/transam.c)
//!   - GetCurrentTransactionId / GetCurrentTransactionNestLevel /
//!     IsTransactionOrTransactionBlock (access/transam/xact.c)
//!   - StartTransactionCommand / CommitTransactionCommand (access/transam/xact.c)
//!   - pq_beginmessage / pq_sendint32 / pq_sendstring / pq_endmessage / pq_flush
//!     (libpq/pqformat.c)
//!   - set_ps_display (utils/ps_status.c)
//!   - GetCurrentTimestamp / TimestampDifferenceExceeds (utils/timestamp.c)
//!   - text_to_cstring / PG_GETARG_TEXT_PP (utils/builtins)
//!   - SRF_IS_FIRSTCALL / SRF_FIRSTCALL_INIT / SRF_PERCALL_SETUP /
//!     SRF_RETURN_NEXT / SRF_RETURN_DONE / FuncCallContext (funcapi.h)
//!   - hash_create / hash_search / hash_any (utils/hash)
//!   - check_slru_buffers (utils/guc_hooks.c)
//!   - IsParallelWorker / PreventCommandDuringRecovery (miscadmin)
//!   - list_concat / list_free_deep / foreach_delete_current (pg_list)
//!   - CStringGetTextDatum (builtins)

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};
use std::ptr;
use std::mem::size_of;

use crate::c::{int32, int64, uint16, uint32, Size, TransactionId};
use crate::nodes::pg_list::{lappend, list_length, list_nth, list_head, lnext,
                             lfirst, NIL, List, ListCell};
use crate::list_make1;
use crate::miscadmin::{MyDatabaseId, MaxBackends, notify_buffers, ExitOnAnyError,
                        MyLatch, InvalidPid};
use crate::access::transam::{InvalidTransactionId, FrozenTransactionId,
                               TransactionIdIsNormal};
use crate::access::transam::transam::TransactionIdPrecedes;
use crate::postgres::DatumGetUInt32;
use crate::utils::guc_hooks::GucSource;
use crate::pg_config::BLCKSZ;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_ARGISNULL, PG_GETARG_TEXT_PP, PG_RETURN_VOID, PG_RETURN_NULL,
            PG_RETURN_FLOAT8, foreach, current_cell, foreach_delete_current};
use crate::nodes::pg_list::{list_concat, list_free_deep};

// ---------------------------------------------------------------------------
// Stub dependencies not yet ported
// ---------------------------------------------------------------------------

#[allow(non_camel_case_types)]
pub type LWLock = c_void;
const LW_EXCLUSIVE: c_int = 1;
const LW_SHARED: c_int = 2;

#[allow(non_snake_case)]
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    // TODO(pg-port): storage/lwlock.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    // TODO(pg-port): storage/lwlock.c
    unimplemented!()
}

// NotifyQueueLock / NotifyQueueTailLock (storage/lwlocknames.h -- built-in LWLocks)
#[allow(non_upper_case_globals)]
static mut NotifyQueueLock: *mut LWLock = ptr::null_mut();
#[allow(non_upper_case_globals)]
static mut NotifyQueueTailLock: *mut LWLock = ptr::null_mut();

#[repr(C)]
pub struct SlruSharedData {
    pub page_buffer: *mut *mut c_char,
    pub page_dirty:  *mut bool,
}
pub type SlruShared = *mut SlruSharedData;

#[repr(C)]
pub struct SlruCtlData {
    pub shared:        SlruShared,
    pub PagePrecedes:  Option<unsafe fn(i64, i64) -> bool>,
}
pub type SlruCtl = *mut SlruCtlData;

#[allow(non_snake_case)]
unsafe fn SimpleLruGetBankLock(_ctl: SlruCtl, _pageno: i64) -> *mut LWLock {
    // TODO(pg-port): access/slru.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn SimpleLruZeroPage(_ctl: SlruCtl, _pageno: i64) -> c_int {
    // TODO(pg-port): access/slru.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn SimpleLruReadPage(_ctl: SlruCtl, _pageno: i64, _write_ok: bool,
                              _xid: TransactionId) -> c_int {
    // TODO(pg-port): access/slru.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn SimpleLruReadPage_ReadOnly(_ctl: SlruCtl, _pageno: i64,
                                      _xid: TransactionId) -> c_int {
    // TODO(pg-port): access/slru.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn SimpleLruTruncate(_ctl: SlruCtl, _cutoffPage: i64) {
    // TODO(pg-port): access/slru.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn SimpleLruInit(_ctl: SlruCtl, _name: *const c_char, _nslots: c_int,
                         _nlsns: c_int, _subdir: *const c_char,
                         _buffer_tranche_id: c_int, _slru_tranche_id: c_int,
                         _sync_handler: c_int, _long_segment_names: bool) {
    // TODO(pg-port): access/slru.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn SimpleLruShmemSize(_nslots: c_int, _nlsns: c_int) -> Size {
    // TODO(pg-port): access/slru.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn SlruScanDirectory(_ctl: SlruCtl,
                              _callback: Option<unsafe fn(SlruCtl, *const c_char, i64) -> bool>,
                              _data: *mut c_void) -> bool {
    // TODO(pg-port): access/slru.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn SlruScanDirCbDeleteAll(_ctl: SlruCtl, _fname: *const c_char,
                                  _segno: i64) -> bool {
    // TODO(pg-port): access/slru.c
    unimplemented!()
}

#[allow(non_snake_case)]
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size,
                           _foundPtr: *mut bool) -> *mut c_void {
    // TODO(pg-port): storage/shmem.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn mul_size(_s1: Size, _s2: Size) -> Size {
    // TODO(pg-port): storage/shmem.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn add_size(_s1: Size, _s2: Size) -> Size {
    // TODO(pg-port): storage/shmem.c
    unimplemented!()
}

// tranche ids (lwlock.h / lwlocknames.h)
const LWTRANCHE_NOTIFY_BUFFER: c_int = 0;
const LWTRANCHE_NOTIFY_SLRU:   c_int = 0;

// SyncRequestHandler (storage/sync.h)
const SYNC_HANDLER_NONE: c_int = -1;

const SLRU_PAGES_PER_SEGMENT: i64 = 32;

// catalog/pg_database.h
const DatabaseRelationId: Oid = 1262;

// storage/lockdefs.h
const AccessExclusiveLock: c_int = 8;

#[allow(non_snake_case)]
unsafe fn LockSharedObject(_classid: Oid, _objid: Oid, _objsubid: uint32,
                             _lockmode: c_int) {
    // TODO(pg-port): storage/lmgr.c
    unimplemented!()
}

#[allow(non_camel_case_types)]
pub type ProcNumber = c_int;
const INVALID_PROC_NUMBER: ProcNumber = -1;

extern "C" {
    static mut MyProcPid: c_int;
    static mut MyProcNumber: ProcNumber;
}

// storage/procsignal.h
const PROCSIG_NOTIFY_INTERRUPT: c_int = 4;

#[allow(non_snake_case)]
unsafe fn SendProcSignal(_pid: int32, _signal: c_int,
                          _procNumber: ProcNumber) -> c_int {
    // TODO(pg-port): storage/procsignal.c
    unimplemented!()
}

#[allow(non_snake_case)]
unsafe fn SetLatch(_latch: *mut c_void) {
    // TODO(pg-port): storage/latch.c
    unimplemented!()
}

#[allow(non_snake_case)]
unsafe fn before_shmem_exit(_function: unsafe fn(c_int, Datum),
                              _arg: Datum) {
    // TODO(pg-port): storage/ipc.c
    unimplemented!()
}

// Snapshot (utils/snapshot.h)
pub type Snapshot = *mut c_void;

#[allow(non_snake_case)]
unsafe fn RegisterSnapshot(_snap: Snapshot) -> Snapshot {
    // TODO(pg-port): utils/snapmgr.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn UnregisterSnapshot(_snap: Snapshot) {
    // TODO(pg-port): utils/snapmgr.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn GetLatestSnapshot() -> Snapshot {
    // TODO(pg-port): utils/snapmgr.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn XidInMVCCSnapshot(_xid: TransactionId, _snap: Snapshot) -> bool {
    // TODO(pg-port): utils/snapmgr.c
    unimplemented!()
}

#[allow(non_snake_case)]
unsafe fn TransactionIdDidCommit(_xid: TransactionId) -> bool {
    // TODO(pg-port): access/transam/transam.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn GetCurrentTransactionId() -> TransactionId {
    // TODO(pg-port): access/transam/xact.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn GetCurrentTransactionNestLevel() -> c_int {
    // TODO(pg-port): access/transam/xact.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn IsTransactionOrTransactionBlock() -> bool {
    // TODO(pg-port): access/transam/xact.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn StartTransactionCommand() {
    // TODO(pg-port): access/transam/xact.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn CommitTransactionCommand() {
    // TODO(pg-port): access/transam/xact.c
    unimplemented!()
}

// utils/timestamp.h
pub type TimestampTz = i64;
#[allow(non_snake_case)]
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    // TODO(pg-port): utils/timestamp.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn TimestampDifferenceExceeds(_t1: TimestampTz, _t2: TimestampTz,
                                      _msec: c_int) -> bool {
    // TODO(pg-port): utils/timestamp.c
    unimplemented!()
}

// libpq -- lib/stringinfo.h
#[repr(C)]
pub struct StringInfoData {
    pub data:    *mut c_char,
    pub len:     c_int,
    pub maxlen:  c_int,
    pub cursor:  c_int,
}
pub type StringInfo = *mut StringInfoData;
#[allow(non_snake_case)]
unsafe fn pq_beginmessage(_buf: *mut c_void, _msgtype: u8) {
    // TODO(pg-port): libpq/pqformat.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn pq_sendint32(_buf: *mut c_void, _i: int32) {
    // TODO(pg-port): libpq/pqformat.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn pq_sendstring(_buf: *mut c_void, _s: *const c_char) {
    // TODO(pg-port): libpq/pqformat.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn pq_endmessage(_buf: *mut c_void) {
    // TODO(pg-port): libpq/pqformat.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn pq_flush() {
    // TODO(pg-port): libpq/libpq.c
    unimplemented!()
}

// tcop/dest.h
pub type CommandDest = c_int;
const DestRemote: CommandDest = 2;
extern "C" {
    static mut whereToSendOutput: CommandDest;
}
const PqMsg_NotificationResponse: u8 = b'A';

// utils/ps_status.h
#[allow(non_snake_case)]
unsafe fn set_ps_display(_activity: *const c_char) {
    // TODO(pg-port): utils/ps_status.c
    unimplemented!()
}

// utils/hsearch.h -- minimal hash table stubs
#[repr(C)]
pub struct HASHCTL {
    pub keysize:   Size,
    pub entrysize: Size,
    pub hash:      Option<unsafe fn(*const c_void, Size) -> uint32>,
    pub match_fn:  Option<unsafe fn(*const c_void, *const c_void, Size) -> c_int>,
    pub hcxt:      MemoryContext,
}
pub type HTAB = c_void;
const HASH_ELEM:     c_int = 0x0008;
const HASH_FUNCTION: c_int = 0x0010;
const HASH_COMPARE:  c_int = 0x0020;
const HASH_CONTEXT:  c_int = 0x0080;
const HASH_FIND:     c_int = 0;
const HASH_ENTER:    c_int = 1;

#[allow(non_snake_case)]
unsafe fn hash_create(_tabname: *const c_char, _nelem: i64,
                       _info: *mut HASHCTL, _flags: c_int) -> *mut HTAB {
    // TODO(pg-port): utils/hash/dynahash.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn hash_search(_hashp: *mut HTAB, _keyPtr: *const c_void,
                       _action: c_int, _foundPtr: *mut bool) -> *mut c_void {
    // TODO(pg-port): utils/hash/dynahash.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn hash_any(_k: *const u8, _keylen: c_int) -> Datum {
    // TODO(pg-port): common/hashfn.c
    unimplemented!()
}

// utils/guc_hooks.h
#[allow(non_snake_case)]
unsafe fn check_slru_buffers(_name: *const c_char, _newval: *mut c_int) -> bool {
    // TODO(pg-port): utils/guc_hooks.c
    unimplemented!()
}

// funcapi.h -- set-returning function support
#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr:  uint32,
    pub user_fctx: *mut c_void,
}

// SRF_* set-returning-function macros: not yet ported to the crate root.
// Mirror funcapi.h by delegating to local stubs.
macro_rules! SRF_IS_FIRSTCALL {
    ($fcinfo:expr) => { srf_is_firstcall($fcinfo) };
}
macro_rules! SRF_FIRSTCALL_INIT {
    ($fcinfo:expr) => { srf_firstcall_init($fcinfo) };
}
macro_rules! SRF_PERCALL_SETUP {
    ($fcinfo:expr) => { srf_percall_setup($fcinfo) };
}
macro_rules! SRF_RETURN_NEXT {
    ($fctx:expr, $result:expr) => { return srf_return_next($fctx, $result) };
}
macro_rules! SRF_RETURN_DONE {
    ($fctx:expr) => { return srf_return_done($fctx) };
}

#[allow(non_snake_case)]
unsafe fn srf_is_firstcall(_fcinfo: FunctionCallInfo) -> bool {
    // TODO(pg-port): utils/fmgr/funcapi.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn srf_firstcall_init(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    // TODO(pg-port): utils/fmgr/funcapi.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn srf_percall_setup(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    // TODO(pg-port): utils/fmgr/funcapi.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn srf_return_next(_fctx: *mut FuncCallContext, _result: Datum) -> Datum {
    // TODO(pg-port): utils/fmgr/funcapi.c
    unimplemented!()
}
#[allow(non_snake_case)]
unsafe fn srf_return_done(_fctx: *mut FuncCallContext) -> Datum {
    // TODO(pg-port): utils/fmgr/funcapi.c
    unimplemented!()
}

// PreventCommandDuringRecovery (miscadmin / utility.c)
#[allow(non_snake_case)]
unsafe fn PreventCommandDuringRecovery(_cmdname: *const c_char) {
    // TODO(pg-port): access/transam/xact.c
    unimplemented!()
}

// IsParallelWorker macro (miscadmin.h)
#[allow(non_snake_case)]
unsafe fn IsParallelWorker() -> bool {
    // TODO(pg-port): miscadmin
    unimplemented!()
}

// text_to_cstring / PG_GETARG_TEXT_PP (builtins / fmgr.h)
pub type text = c_void;
#[allow(non_snake_case)]
unsafe fn text_to_cstring(_t: *const text) -> *mut c_char {
    // TODO(pg-port): utils/adt/varlena.c
    unimplemented!()
}

// CStringGetTextDatum (builtins)
#[allow(non_snake_case)]
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    // TODO(pg-port): utils/adt/varlena.c
    unimplemented!()
}

// MemoryContext types (re-use prelude definitions)
extern "C" {
    static mut CurTransactionContext: MemoryContext;
    static mut TopTransactionContext: MemoryContext;
}
#[allow(non_snake_case)]
unsafe fn MemoryContextAlloc(_cxt: MemoryContext, _size: Size) -> *mut c_void {
    // TODO(pg-port): utils/mmgr/mcxt.c
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum size of a NOTIFY payload, including terminating NULL.
const NOTIFY_PAYLOAD_MAX_LENGTH: usize = BLCKSZ - NAMEDATALEN - 128;

/// NAMEDATALEN from pg_config (64 bytes).
const NAMEDATALEN: usize = 64;

#[inline]
fn QUEUEALIGN(len: usize) -> usize {
    // INTALIGN: round up to multiple of 4
    (len + 3) & !3
}

/// Minimum possible queue entry size (channel="" payload="", no alignment padding).
fn AsyncQueueEntryEmptySize() -> usize {
    // offsetof(AsyncQueueEntry, data) + 2  (two null terminators)
    (size_of::<c_int>()   // length
     + size_of::<Oid>()   // dboid
     + size_of::<TransactionId>() // xid
     + size_of::<int32>() // srcPid
    ) + 2
}

const QUEUE_PAGESIZE: usize = BLCKSZ;

/// Warn at most once every 5s.
const QUEUE_FULL_WARN_INTERVAL: c_int = 5000;

/// Advance tail every this many pages.
const QUEUE_CLEANUP_DELAY: i64 = 4;

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

/// An entry in the global notify queue.
#[repr(C)]
pub struct AsyncQueueEntry {
    pub length: c_int,         // total allocated length of entry
    pub dboid:  Oid,           // sender's database OID
    pub xid:    TransactionId, // sender's XID
    pub srcPid: int32,         // sender's PID
    pub data:   [c_char; NAMEDATALEN + NOTIFY_PAYLOAD_MAX_LENGTH], // channel name + payload, null-terminated strings
}

/// Queue position (page + offset).
#[derive(Copy, Clone)]
#[repr(C)]
pub struct QueuePosition {
    pub page:   i64, // SLRU page number
    pub offset: c_int, // byte offset within page
}

#[inline] fn QUEUE_POS_PAGE(x: QueuePosition) -> i64   { x.page }
#[inline] fn QUEUE_POS_OFFSET(x: QueuePosition) -> c_int { x.offset }

#[inline]
fn SET_QUEUE_POS(x: &mut QueuePosition, y: i64, z: c_int) {
    x.page = y;
    x.offset = z;
}

#[inline]
fn QUEUE_POS_EQUAL(x: QueuePosition, y: QueuePosition) -> bool {
    x.page == y.page && x.offset == y.offset
}

#[inline]
fn QUEUE_POS_IS_ZERO(x: QueuePosition) -> bool {
    x.page == 0 && x.offset == 0
}

/// Choose logically smaller QueuePosition.
#[inline]
unsafe fn QUEUE_POS_MIN(x: QueuePosition, y: QueuePosition) -> QueuePosition {
    if asyncQueuePagePrecedes(x.page, y.page) { x }
    else if x.page != y.page { y }
    else if x.offset < y.offset { x }
    else { y }
}

/// Choose logically larger QueuePosition.
#[inline]
unsafe fn QUEUE_POS_MAX(x: QueuePosition, y: QueuePosition) -> QueuePosition {
    if asyncQueuePagePrecedes(x.page, y.page) { y }
    else if x.page != y.page { x }
    else if x.offset > y.offset { x }
    else { y }
}

/// Per-backend status entry in shared memory.
#[repr(C)]
pub struct QueueBackendStatus {
    pub pid:          int32,     // either a PID or InvalidPid
    pub dboid:        Oid,       // backend's database OID, or InvalidOid
    pub nextListener: ProcNumber, // id of next listener, or INVALID_PROC_NUMBER
    pub pos:          QueuePosition, // backend has read queue up to here
}

/// Shared memory state for LISTEN/NOTIFY.
#[repr(C)]
pub struct AsyncQueueControl {
    pub head:               QueuePosition,
    pub tail:               QueuePosition,
    pub stopPage:           i64,
    pub firstListener:      ProcNumber,
    pub lastQueueFillWarn:  TimestampTz,
    pub backend:            [QueueBackendStatus; 0], // FLEXIBLE_ARRAY_MEMBER
}

// global pointer to shared memory structure
static mut asyncQueueControl: *mut AsyncQueueControl = ptr::null_mut();

// Accessor macros implemented as inline unsafe fns
#[inline] unsafe fn QUEUE_HEAD()  -> QueuePosition { (*asyncQueueControl).head }
#[inline] unsafe fn QUEUE_HEAD_mut() -> &'static mut QueuePosition { &mut (*asyncQueueControl).head }
#[inline] unsafe fn QUEUE_TAIL()  -> QueuePosition { (*asyncQueueControl).tail }
#[inline] unsafe fn QUEUE_TAIL_mut() -> &'static mut QueuePosition { &mut (*asyncQueueControl).tail }
#[inline] unsafe fn QUEUE_STOP_PAGE() -> i64 { (*asyncQueueControl).stopPage }
#[inline] unsafe fn QUEUE_STOP_PAGE_mut() -> &'static mut i64 { &mut (*asyncQueueControl).stopPage }
#[inline] unsafe fn QUEUE_FIRST_LISTENER() -> ProcNumber { (*asyncQueueControl).firstListener }
#[inline] unsafe fn QUEUE_FIRST_LISTENER_mut() -> &'static mut ProcNumber {
    &mut (*asyncQueueControl).firstListener
}
#[inline] unsafe fn QUEUE_BACKEND_PID(i: ProcNumber) -> int32 {
    (*(*asyncQueueControl).backend.as_ptr().add(i as usize)).pid
}
#[inline] unsafe fn QUEUE_BACKEND_PID_set(i: ProcNumber, v: int32) {
    (*(*asyncQueueControl).backend.as_mut_ptr().add(i as usize)).pid = v;
}
#[inline] unsafe fn QUEUE_BACKEND_DBOID(i: ProcNumber) -> Oid {
    (*(*asyncQueueControl).backend.as_ptr().add(i as usize)).dboid
}
#[inline] unsafe fn QUEUE_BACKEND_DBOID_set(i: ProcNumber, v: Oid) {
    (*(*asyncQueueControl).backend.as_mut_ptr().add(i as usize)).dboid = v;
}
#[inline] unsafe fn QUEUE_NEXT_LISTENER(i: ProcNumber) -> ProcNumber {
    (*(*asyncQueueControl).backend.as_ptr().add(i as usize)).nextListener
}
#[inline] unsafe fn QUEUE_NEXT_LISTENER_set(i: ProcNumber, v: ProcNumber) {
    (*(*asyncQueueControl).backend.as_mut_ptr().add(i as usize)).nextListener = v;
}
#[inline] unsafe fn QUEUE_BACKEND_POS(i: ProcNumber) -> QueuePosition {
    (*(*asyncQueueControl).backend.as_ptr().add(i as usize)).pos
}
#[inline] unsafe fn QUEUE_BACKEND_POS_set(i: ProcNumber, v: QueuePosition) {
    (*(*asyncQueueControl).backend.as_mut_ptr().add(i as usize)).pos = v;
}

/// The SLRU buffer area through which we access the notification queue.
static mut NotifyCtlData: SlruCtlData = SlruCtlData {
    shared: ptr::null_mut(),
    PagePrecedes: None,
};
#[inline] fn NotifyCtl() -> SlruCtl { unsafe { &mut NotifyCtlData as SlruCtl } }

// ---------------------------------------------------------------------------
// Listen-action types
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum ListenActionKind {
    LISTEN_LISTEN,
    LISTEN_UNLISTEN,
    LISTEN_UNLISTEN_ALL,
}
use ListenActionKind::*;

#[repr(C)]
pub struct ListenAction {
    pub action:  ListenActionKind,
    pub channel: [c_char; 0], // nul-terminated string (FLEXIBLE_ARRAY_MEMBER)
}

#[repr(C)]
pub struct ActionList {
    pub nestingLevel: c_int,
    pub actions:      *mut List,
    pub upper:        *mut ActionList,
}

static mut pendingActions: *mut ActionList = ptr::null_mut();

// ---------------------------------------------------------------------------
// Notification types
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct Notification {
    pub channel_len: uint16,
    pub payload_len: uint16,
    pub data:        [c_char; 0], // null-terminated channel, then null-terminated payload
}

#[repr(C)]
pub struct NotificationList {
    pub nestingLevel: c_int,
    pub events:       *mut List,
    pub hashtab:      *mut HTAB,
    pub upper:        *mut NotificationList,
}

/// Threshold to build hash table.
const MIN_HASHABLE_NOTIFIES: c_int = 16;

#[repr(C)]
pub struct NotificationHash {
    pub event: *mut Notification, // => the actual Notification struct
}

static mut pendingNotifies: *mut NotificationList = ptr::null_mut();

// ---------------------------------------------------------------------------
// Module-level state
// ---------------------------------------------------------------------------

/*
 * Inbound notifications are initially processed by HandleNotifyInterrupt(),
 * called from inside a signal handler. That just sets the
 * notifyInterruptPending flag and sets the process
 * latch. ProcessNotifyInterrupt() will then be called whenever it's safe to
 * actually deal with the interrupt.
 */
#[no_mangle]
pub static mut notifyInterruptPending: bool = false;

/* True if we've registered an on_shmem_exit cleanup */
static mut unlistenExitRegistered: bool = false;

/* True if we're currently registered as a listener in asyncQueueControl */
static mut amRegisteredListener: bool = false;

/* have we advanced to a page that's a multiple of QUEUE_CLEANUP_DELAY? */
static mut tryAdvanceTail: bool = false;

/* GUC parameters */
#[no_mangle]
pub static mut Trace_notify: bool = false;

/* For 8 KB pages this gives 8 GB of disk space */
#[no_mangle]
pub static mut max_notify_queue_pages: c_int = 1048576;

/*
 * listenChannels identifies the channels we are actually listening to
 * (ie, have committed a LISTEN on).  It is a simple list of channel names,
 * allocated in TopMemoryContext.
 */
static mut listenChannels: *mut List = NIL;

// ---------------------------------------------------------------------------
// Inline helpers
// ---------------------------------------------------------------------------

/*
 * Compute the difference between two queue page numbers.
 * Previously this function accounted for a wraparound.
 */
#[inline]
unsafe fn asyncQueuePageDiff(p: i64, q: i64) -> i64 {
    p - q
}

/*
 * Determines whether p precedes q.
 * Previously this function accounted for a wraparound.
 */
#[inline]
unsafe fn asyncQueuePagePrecedes(p: i64, q: i64) -> bool {
    p < q
}

// ---------------------------------------------------------------------------
// Public API: size / init
// ---------------------------------------------------------------------------

/*
 * Report space needed for our shared memory area
 */
pub unsafe fn AsyncShmemSize() -> Size {
    let mut size: Size;

    /* This had better match AsyncShmemInit */
    size = mul_size(MaxBackends as Size, size_of::<QueueBackendStatus>());
    size = add_size(size, std::mem::offset_of!(AsyncQueueControl, backend));

    size = add_size(size, SimpleLruShmemSize(notify_buffers, 0));

    size
}

/*
 * Initialize our shared memory area
 */
pub unsafe fn AsyncShmemInit() {
    let mut found: bool = false;
    let size: Size;

    /*
     * Create or attach to the AsyncQueueControl structure.
     */
    let size1 = mul_size(MaxBackends as Size, size_of::<QueueBackendStatus>());
    let size2 = add_size(size1, std::mem::offset_of!(AsyncQueueControl, backend));

    asyncQueueControl = ShmemInitStruct(
        c"Async Queue Control".as_ptr(),
        size2,
        &mut found,
    ) as *mut AsyncQueueControl;

    if !found {
        /* First time through, so initialize it */
        SET_QUEUE_POS(&mut (*asyncQueueControl).head, 0, 0);
        SET_QUEUE_POS(&mut (*asyncQueueControl).tail, 0, 0);
        (*asyncQueueControl).stopPage = 0;
        (*asyncQueueControl).firstListener = INVALID_PROC_NUMBER;
        (*asyncQueueControl).lastQueueFillWarn = 0;
        for i in 0..MaxBackends {
            QUEUE_BACKEND_PID_set(i, InvalidPid);
            QUEUE_BACKEND_DBOID_set(i, InvalidOid);
            QUEUE_NEXT_LISTENER_set(i, INVALID_PROC_NUMBER);
            SET_QUEUE_POS(
                &mut (*(*asyncQueueControl).backend.as_mut_ptr().add(i as usize)).pos,
                0, 0,
            );
        }
    }

    /*
     * Set up SLRU management of the pg_notify data. Note that long segment
     * names are used in order to avoid wraparound.
     */
    NotifyCtlData.PagePrecedes = Some(asyncQueuePagePrecedes);
    SimpleLruInit(
        NotifyCtl(),
        c"notify".as_ptr(),
        notify_buffers,
        0,
        c"pg_notify".as_ptr(),
        LWTRANCHE_NOTIFY_BUFFER,
        LWTRANCHE_NOTIFY_SLRU,
        SYNC_HANDLER_NONE,
        true,
    );

    if !found {
        /*
         * During start or reboot, clean out the pg_notify directory.
         */
        SlruScanDirectory(NotifyCtl(), Some(SlruScanDirCbDeleteAll), ptr::null_mut());
    }
}

/*
 * pg_notify -
 *	  SQL function to send a notification event
 */
#[no_mangle]
pub unsafe extern "C" fn pg_notify(fcinfo: FunctionCallInfo) -> Datum {
    let channel: *const c_char;
    let payload: *const c_char;

    if PG_ARGISNULL!(fcinfo, 0) {
        channel = c"".as_ptr();
    } else {
        channel = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 0) as *const text);
    }

    if PG_ARGISNULL!(fcinfo, 1) {
        payload = c"".as_ptr();
    } else {
        payload = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 1) as *const text);
    }

    /* For NOTIFY as a statement, this is checked in ProcessUtility */
    PreventCommandDuringRecovery(c"NOTIFY".as_ptr());

    Async_Notify(channel, payload);

    PG_RETURN_VOID!()
}

/*
 * Async_Notify
 *
 *		This is executed by the SQL notify command.
 *
 *		Adds the message to the list of pending notifies.
 *		Actual notification happens during transaction commit.
 *		^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
 */
#[allow(non_snake_case)]
pub unsafe fn Async_Notify(channel: *const c_char, payload: *const c_char) {
    let my_level: c_int = GetCurrentTransactionNestLevel();
    let channel_len: usize;
    let payload_len: usize;
    let n: *mut Notification;
    let oldcontext: MemoryContext;

    if IsParallelWorker() {
        elog!(ERROR, "cannot send notifications from a parallel worker");
    }

    if Trace_notify {
        elog!(DEBUG1, "Async_Notify({})",
              std::ffi::CStr::from_ptr(channel).to_string_lossy());
    }

    channel_len = if !channel.is_null() { libc::strlen(channel) } else { 0 };
    payload_len = if !payload.is_null() { libc::strlen(payload) } else { 0 };

    /* a channel name must be specified */
    if channel_len == 0 {
        ereport!(ERROR, errmsg!("channel name cannot be empty"));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    /* enforce length limits */
    if channel_len >= NAMEDATALEN {
        ereport!(ERROR, errmsg!("channel name too long"));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    if payload_len >= NOTIFY_PAYLOAD_MAX_LENGTH {
        ereport!(ERROR, errmsg!("payload string too long"));
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
    }

    /*
     * We must construct the Notification entry, even if we end up not using
     * it, in order to compare it cheaply to existing list entries.
     *
     * The notification list needs to live until end of transaction, so store
     * it in the transaction context.
     */
    oldcontext = MemoryContextSwitchTo(CurTransactionContext);

    n = palloc(std::mem::offset_of!(Notification, data)
               + channel_len + payload_len + 2) as *mut Notification;
    (*n).channel_len = channel_len as uint16;
    (*n).payload_len = payload_len as uint16;
    libc::strcpy((*n).data.as_mut_ptr(), channel);
    if !payload.is_null() {
        libc::strcpy((*n).data.as_mut_ptr().add(channel_len + 1), payload);
    } else {
        *(*n).data.as_mut_ptr().add(channel_len + 1) = b'\0' as c_char;
    }

    if pendingNotifies.is_null() || my_level > (*pendingNotifies).nestingLevel {
        let notifies: *mut NotificationList;

        /*
         * First notify event in current (sub)xact. Note that we allocate the
         * NotificationList in TopTransactionContext; the nestingLevel might
         * get changed later by AtSubCommit_Notify.
         */
        notifies = MemoryContextAlloc(TopTransactionContext,
                                      size_of::<NotificationList>())
            as *mut NotificationList;
        (*notifies).nestingLevel = my_level;
        (*notifies).events = list_make1!(n as *mut c_void);
        /* We certainly don't need a hashtable yet */
        (*notifies).hashtab = ptr::null_mut();
        (*notifies).upper = pendingNotifies;
        pendingNotifies = notifies;
    } else {
        /* Now check for duplicates */
        if AsyncExistsPendingNotify(n) {
            /* It's a dup, so forget it */
            pfree(n as *mut c_void);
            MemoryContextSwitchTo(oldcontext);
            return;
        }

        /* Append more events to existing list */
        AddEventToPendingNotifies(n);
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * queue_listen
 *		Common code for listen, unlisten, unlisten all commands.
 *
 *		Adds the request to the list of pending actions.
 *		Actual update of the listenChannels list happens during transaction
 *		commit.
 */
#[allow(non_snake_case)]
unsafe fn queue_listen(action: ListenActionKind, channel: *const c_char) {
    let oldcontext: MemoryContext;
    let actrec: *mut ListenAction;
    let my_level: c_int = GetCurrentTransactionNestLevel();

    /*
     * Unlike Async_Notify, we don't try to collapse out duplicates. It would
     * be too complicated to ensure we get the right interactions of
     * conflicting LISTEN/UNLISTEN/UNLISTEN_ALL, and it's unlikely that there
     * would be any performance benefit anyway in sane applications.
     */
    oldcontext = MemoryContextSwitchTo(CurTransactionContext);

    /* space for terminating null is included in sizeof(ListenAction) */
    actrec = palloc(std::mem::offset_of!(ListenAction, channel)
                    + libc::strlen(channel) + 1) as *mut ListenAction;
    (*actrec).action = action;
    libc::strcpy((*actrec).channel.as_mut_ptr(), channel);

    if pendingActions.is_null() || my_level > (*pendingActions).nestingLevel {
        let actions: *mut ActionList;

        /*
         * First action in current sub(xact). Note that we allocate the
         * ActionList in TopTransactionContext; the nestingLevel might get
         * changed later by AtSubCommit_Notify.
         */
        actions = MemoryContextAlloc(TopTransactionContext, size_of::<ActionList>())
            as *mut ActionList;
        (*actions).nestingLevel = my_level;
        (*actions).actions = list_make1!(actrec as *mut c_void);
        (*actions).upper = pendingActions;
        pendingActions = actions;
    } else {
        (*pendingActions).actions = lappend((*pendingActions).actions,
                                            actrec as *mut c_void);
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Async_Listen
 *
 *		This is executed by the SQL listen command.
 */
#[allow(non_snake_case)]
pub unsafe fn Async_Listen(channel: *const c_char) {
    if Trace_notify {
        elog!(DEBUG1, "Async_Listen({},{})",
              std::ffi::CStr::from_ptr(channel).to_string_lossy(), MyProcPid);
    }

    queue_listen(LISTEN_LISTEN, channel);
}

/*
 * Async_Unlisten
 *
 *		This is executed by the SQL unlisten command.
 */
#[allow(non_snake_case)]
pub unsafe fn Async_Unlisten(channel: *const c_char) {
    if Trace_notify {
        elog!(DEBUG1, "Async_Unlisten({},{})",
              std::ffi::CStr::from_ptr(channel).to_string_lossy(), MyProcPid);
    }

    /* If we couldn't possibly be listening, no need to queue anything */
    if pendingActions.is_null() && !unlistenExitRegistered {
        return;
    }

    queue_listen(LISTEN_UNLISTEN, channel);
}

/*
 * Async_UnlistenAll
 *
 *		This is invoked by UNLISTEN * command, and also at backend exit.
 */
#[allow(non_snake_case)]
pub unsafe fn Async_UnlistenAll() {
    if Trace_notify {
        elog!(DEBUG1, "Async_UnlistenAll({})", MyProcPid);
    }

    /* If we couldn't possibly be listening, no need to queue anything */
    if pendingActions.is_null() && !unlistenExitRegistered {
        return;
    }

    queue_listen(LISTEN_UNLISTEN_ALL, c"".as_ptr());
}

/*
 * SQL function: return a set of the channel names this backend is actively
 * listening to.
 *
 * Note: this coding relies on the fact that the listenChannels list cannot
 * change within a transaction.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_listening_channels(fcinfo: FunctionCallInfo) -> Datum {
    let funcctx: *mut FuncCallContext;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL!(fcinfo) {
        /* create a function context for cross-call persistence */
        let _ = SRF_FIRSTCALL_INIT!(fcinfo);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP!(fcinfo);

    if ((*funcctx).call_cntr as c_int) < list_length(listenChannels) {
        let channel: *mut c_char = list_nth(listenChannels,
                                             (*funcctx).call_cntr as c_int)
            as *mut c_char;

        SRF_RETURN_NEXT!(funcctx, CStringGetTextDatum(channel));
    }

    SRF_RETURN_DONE!(funcctx)
}

/*
 * Async_UnlistenOnExit
 *
 * This is executed at backend exit if we have done any LISTENs in this
 * backend.  It might not be necessary anymore, if the user UNLISTENed
 * everything, but we don't try to detect that case.
 */
#[allow(non_snake_case)]
unsafe fn Async_UnlistenOnExit(_code: c_int, _arg: Datum) {
    Exec_UnlistenAllCommit();
    asyncQueueUnregister();
}

/*
 * AtPrepare_Notify
 *
 *		This is called at the prepare phase of a two-phase
 *		transaction.  Save the state for possible commit later.
 */
#[allow(non_snake_case)]
pub unsafe fn AtPrepare_Notify() {
    /* It's not allowed to have any pending LISTEN/UNLISTEN/NOTIFY actions */
    if !pendingActions.is_null() || !pendingNotifies.is_null() {
        ereport!(ERROR, errmsg!("cannot PREPARE a transaction that has executed LISTEN, UNLISTEN, or NOTIFY"));
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }
}

/*
 * PreCommit_Notify
 *
 *		This is called at transaction commit, before actually committing to
 *		clog.
 *
 *		If there are pending LISTEN actions, make sure we are listed in the
 *		shared-memory listener array.  This must happen before commit to
 *		ensure we don't miss any notifies from transactions that commit
 *		just after ours.
 *
 *		If there are outbound notify requests in the pendingNotifies list,
 *		add them to the global queue.  We do that before commit so that
 *		we can still throw error if we run out of queue space.
 */
#[allow(non_snake_case)]
pub unsafe fn PreCommit_Notify() {
    if pendingActions.is_null() && pendingNotifies.is_null() {
        return; /* no relevant statements in this xact */
    }

    if Trace_notify {
        elog!(DEBUG1, "PreCommit_Notify");
    }

    /* Preflight for any pending listen/unlisten actions */
    if !pendingActions.is_null() {
        foreach!(p, (*pendingActions).actions, {
            let actrec: *mut ListenAction = lfirst(current_cell!(p)) as *mut ListenAction;

            match (*actrec).action {
                ListenActionKind::LISTEN_LISTEN => {
                    Exec_ListenPreCommit();
                }
                ListenActionKind::LISTEN_UNLISTEN => {
                    /* there is no Exec_UnlistenPreCommit() */
                }
                ListenActionKind::LISTEN_UNLISTEN_ALL => {
                    /* there is no Exec_UnlistenAllPreCommit() */
                }
            }
        });
    }

    /* Queue any pending notifies (must happen after the above) */
    if !pendingNotifies.is_null() {
        let mut nextNotify: *mut ListCell;

        /*
         * Make sure that we have an XID assigned to the current transaction.
         * GetCurrentTransactionId is cheap if we already have an XID, but not
         * so cheap if we don't, and we'd prefer not to do that work while
         * holding NotifyQueueLock.
         */
        let _ = GetCurrentTransactionId();

        /*
         * Serialize writers by acquiring a special lock that we hold till
         * after commit.  This ensures that queue entries appear in commit
         * order, and in particular that there are never uncommitted queue
         * entries ahead of committed ones, so an uncommitted transaction
         * can't block delivery of deliverable notifications.
         *
         * We use a heavyweight lock so that it'll automatically be released
         * after either commit or abort.  This also allows deadlocks to be
         * detected, though really a deadlock shouldn't be possible here.
         *
         * The lock is on "database 0", which is pretty ugly but it doesn't
         * seem worth inventing a special locktag category just for this.
         * (Historical note: before PG 9.0, a similar lock on "database 0" was
         * used by the flatfiles mechanism.)
         */
        LockSharedObject(DatabaseRelationId, InvalidOid, 0,
                         AccessExclusiveLock);

        /* Now push the notifications into the queue */
        nextNotify = list_head((*pendingNotifies).events);
        while !nextNotify.is_null() {
            /*
             * Add the pending notifications to the queue.  We acquire and
             * release NotifyQueueLock once per page, which might be overkill
             * but it does allow readers to get in while we're doing this.
             *
             * A full queue is very uncommon and should really not happen,
             * given that we have so much space available in the SLRU pages.
             * Nevertheless we need to deal with this possibility. Note that
             * when we get here we are in the process of committing our
             * transaction, but we have not yet committed to clog, so at this
             * point in time we can still roll the transaction back.
             */
            LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
            asyncQueueFillWarning();
            if asyncQueueIsFull() {
                ereport!(ERROR, errmsg!("too many notifications in the NOTIFY queue"));
                /* C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED) */
            }
            nextNotify = asyncQueueAddEntries(nextNotify);
            LWLockRelease(NotifyQueueLock);
        }

        /* Note that we don't clear pendingNotifies; AtCommit_Notify will. */
    }
}

/*
 * AtCommit_Notify
 *
 *		This is called at transaction commit, after committing to clog.
 *
 *		Update listenChannels and clear transaction-local state.
 *
 *		If we issued any notifications in the transaction, send signals to
 *		listening backends (possibly including ourselves) to process them.
 *		Also, if we filled enough queue pages with new notifies, try to
 *		advance the queue tail pointer.
 */
#[allow(non_snake_case)]
pub unsafe fn AtCommit_Notify() {
    /*
     * Allow transactions that have not executed LISTEN/UNLISTEN/NOTIFY to
     * return as soon as possible
     */
    if pendingActions.is_null() && pendingNotifies.is_null() {
        return;
    }

    if Trace_notify {
        elog!(DEBUG1, "AtCommit_Notify");
    }

    /* Perform any pending listen/unlisten actions */
    if !pendingActions.is_null() {
        foreach!(p, (*pendingActions).actions, {
            let actrec: *mut ListenAction = lfirst(current_cell!(p)) as *mut ListenAction;

            match (*actrec).action {
                ListenActionKind::LISTEN_LISTEN => {
                    Exec_ListenCommit((*actrec).channel.as_ptr());
                }
                ListenActionKind::LISTEN_UNLISTEN => {
                    Exec_UnlistenCommit((*actrec).channel.as_ptr());
                }
                ListenActionKind::LISTEN_UNLISTEN_ALL => {
                    Exec_UnlistenAllCommit();
                }
            }
        });
    }

    /* If no longer listening to anything, get out of listener array */
    if amRegisteredListener && listenChannels == NIL {
        asyncQueueUnregister();
    }

    /*
     * Send signals to listening backends.  We need do this only if there are
     * pending notifies, which were previously added to the shared queue by
     * PreCommit_Notify().
     */
    if !pendingNotifies.is_null() {
        SignalBackends();
    }

    /*
     * If it's time to try to advance the global tail pointer, do that.
     *
     * (It might seem odd to do this in the sender, when more than likely the
     * listeners won't yet have read the messages we just sent.  However,
     * there's less contention if only the sender does it, and there is little
     * need for urgency in advancing the global tail.  So this typically will
     * be clearing out messages that were sent some time ago.)
     */
    if tryAdvanceTail {
        tryAdvanceTail = false;
        asyncQueueAdvanceTail();
    }

    /* And clean up */
    ClearPendingActionsAndNotifies();
}

/*
 * Exec_ListenPreCommit --- subroutine for PreCommit_Notify
 *
 * This function must make sure we are ready to catch any incoming messages.
 */
#[allow(non_snake_case)]
unsafe fn Exec_ListenPreCommit() {
    let head: QueuePosition;
    let mut max: QueuePosition;
    let mut prevListener: ProcNumber;

    /*
     * Nothing to do if we are already listening to something, nor if we
     * already ran this routine in this transaction.
     */
    if amRegisteredListener {
        return;
    }

    if Trace_notify {
        elog!(DEBUG1, "Exec_ListenPreCommit({})", MyProcPid);
    }

    /*
     * Before registering, make sure we will unlisten before dying. (Note:
     * this action does not get undone if we abort later.)
     */
    if !unlistenExitRegistered {
        before_shmem_exit(Async_UnlistenOnExit, 0);
        unlistenExitRegistered = true;
    }

    /*
     * This is our first LISTEN, so establish our pointer.
     *
     * We set our pointer to the global tail pointer and then move it forward
     * over already-committed notifications.  This ensures we cannot miss any
     * not-yet-committed notifications.  We might get a few more but that
     * doesn't hurt.
     *
     * In some scenarios there might be a lot of committed notifications that
     * have not yet been pruned away (because some backend is being lazy about
     * reading them).  To reduce our startup time, we can look at other
     * backends and adopt the maximum "pos" pointer of any backend that's in
     * our database; any notifications it's already advanced over are surely
     * committed and need not be re-examined by us.  (We must consider only
     * backends connected to our DB, because others will not have bothered to
     * check committed-ness of notifications in our DB.)
     *
     * We need exclusive lock here so we can look at other backends' entries
     * and manipulate the list links.
     */
    LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
    head = QUEUE_HEAD();
    max = QUEUE_TAIL();
    prevListener = INVALID_PROC_NUMBER;
    let mut i: ProcNumber = QUEUE_FIRST_LISTENER();
    while i != INVALID_PROC_NUMBER {
        if QUEUE_BACKEND_DBOID(i) == MyDatabaseId {
            max = QUEUE_POS_MAX(max, QUEUE_BACKEND_POS(i));
        }
        /* Also find last listening backend before this one */
        if i < MyProcNumber {
            prevListener = i;
        }
        i = QUEUE_NEXT_LISTENER(i);
    }
    QUEUE_BACKEND_POS_set(MyProcNumber, max);
    QUEUE_BACKEND_PID_set(MyProcNumber, MyProcPid);
    QUEUE_BACKEND_DBOID_set(MyProcNumber, MyDatabaseId);
    /* Insert backend into list of listeners at correct position */
    if prevListener != INVALID_PROC_NUMBER {
        QUEUE_NEXT_LISTENER_set(MyProcNumber, QUEUE_NEXT_LISTENER(prevListener));
        QUEUE_NEXT_LISTENER_set(prevListener, MyProcNumber);
    } else {
        QUEUE_NEXT_LISTENER_set(MyProcNumber, QUEUE_FIRST_LISTENER());
        *QUEUE_FIRST_LISTENER_mut() = MyProcNumber;
    }
    LWLockRelease(NotifyQueueLock);

    /* Now we are listed in the global array, so remember we're listening */
    amRegisteredListener = true;

    /*
     * Try to move our pointer forward as far as possible.  This will skip
     * over already-committed notifications, which we want to do because they
     * might be quite stale.  Note that we are not yet listening on anything,
     * so we won't deliver such notifications to our frontend.  Also, although
     * our transaction might have executed NOTIFY, those message(s) aren't
     * queued yet so we won't skip them here.
     */
    if !QUEUE_POS_EQUAL(max, head) {
        asyncQueueReadAllNotifications();
    }
}

/*
 * Exec_ListenCommit --- subroutine for AtCommit_Notify
 *
 * Add the channel to the list of channels we are listening on.
 */
#[allow(non_snake_case)]
unsafe fn Exec_ListenCommit(channel: *const c_char) {
    let oldcontext: MemoryContext;

    /* Do nothing if we are already listening on this channel */
    if IsListeningOn(channel) {
        return;
    }

    /*
     * Add the new channel name to listenChannels.
     *
     * XXX It is theoretically possible to get an out-of-memory failure here,
     * which would be bad because we already committed.  For the moment it
     * doesn't seem worth trying to guard against that, but maybe improve this
     * later.
     */
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    listenChannels = lappend(listenChannels, pstrdup(channel) as *mut c_void);
    MemoryContextSwitchTo(oldcontext);
}

/*
 * Exec_UnlistenCommit --- subroutine for AtCommit_Notify
 *
 * Remove the specified channel name from listenChannels.
 */
#[allow(non_snake_case)]
unsafe fn Exec_UnlistenCommit(channel: *const c_char) {
    if Trace_notify {
        elog!(DEBUG1, "Exec_UnlistenCommit({},{})",
              std::ffi::CStr::from_ptr(channel).to_string_lossy(), MyProcPid);
    }

    foreach!(q, listenChannels, {
        let lchan: *mut c_char = lfirst(current_cell!(q)) as *mut c_char;

        if libc::strcmp(lchan, channel) == 0 {
            listenChannels = foreach_delete_current!(listenChannels, q);
            pfree(lchan as *mut c_void);
            break;
        }
    });

    /*
     * We do not complain about unlistening something not being listened;
     * should we?
     */
}

/*
 * Exec_UnlistenAllCommit --- subroutine for AtCommit_Notify
 *
 *		Unlisten on all channels for this backend.
 */
#[allow(non_snake_case)]
unsafe fn Exec_UnlistenAllCommit() {
    if Trace_notify {
        elog!(DEBUG1, "Exec_UnlistenAllCommit({})", MyProcPid);
    }

    list_free_deep(listenChannels);
    listenChannels = NIL;
}

/*
 * Test whether we are actively listening on the given channel name.
 *
 * Note: this function is executed for every notification found in the queue.
 * Perhaps it is worth further optimization, eg convert the list to a sorted
 * array so we can binary-search it.  In practice the list is likely to be
 * fairly short, though.
 */
#[allow(non_snake_case)]
unsafe fn IsListeningOn(channel: *const c_char) -> bool {
    foreach!(p, listenChannels, {
        let lchan: *mut c_char = lfirst(current_cell!(p)) as *mut c_char;

        if libc::strcmp(lchan, channel) == 0 {
            return true;
        }
    });
    false
}

/*
 * Remove our entry from the listeners array when we are no longer listening
 * on any channel.  NB: must not fail if we're already not listening.
 */
#[allow(non_snake_case)]
unsafe fn asyncQueueUnregister() {
    Assert!(listenChannels == NIL); /* else caller error */

    if !amRegisteredListener {
        /* nothing to do */
        return;
    }

    /*
     * Need exclusive lock here to manipulate list links.
     */
    LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
    /* Mark our entry as invalid */
    QUEUE_BACKEND_PID_set(MyProcNumber, InvalidPid);
    QUEUE_BACKEND_DBOID_set(MyProcNumber, InvalidOid);
    /* and remove it from the list */
    if QUEUE_FIRST_LISTENER() == MyProcNumber {
        *QUEUE_FIRST_LISTENER_mut() = QUEUE_NEXT_LISTENER(MyProcNumber);
    } else {
        let mut i: ProcNumber = QUEUE_FIRST_LISTENER();
        while i != INVALID_PROC_NUMBER {
            if QUEUE_NEXT_LISTENER(i) == MyProcNumber {
                QUEUE_NEXT_LISTENER_set(i, QUEUE_NEXT_LISTENER(MyProcNumber));
                break;
            }
            i = QUEUE_NEXT_LISTENER(i);
        }
    }
    QUEUE_NEXT_LISTENER_set(MyProcNumber, INVALID_PROC_NUMBER);
    LWLockRelease(NotifyQueueLock);

    /* mark ourselves as no longer listed in the global array */
    amRegisteredListener = false;
}

/*
 * Test whether there is room to insert more notification messages.
 *
 * Caller must hold at least shared NotifyQueueLock.
 */
#[allow(non_snake_case)]
unsafe fn asyncQueueIsFull() -> bool {
    let headPage: int64 = QUEUE_POS_PAGE(QUEUE_HEAD());
    let tailPage: int64 = QUEUE_POS_PAGE(QUEUE_TAIL());
    let occupied: int64 = headPage - tailPage;

    occupied >= max_notify_queue_pages as int64
}

/*
 * Advance the QueuePosition to the next entry, assuming that the current
 * entry is of length entryLength.  If we jump to a new page the function
 * returns true, else false.
 */
#[allow(non_snake_case)]
unsafe fn asyncQueueAdvance(position: *mut QueuePosition, entryLength: c_int) -> bool {
    let mut pageno: int64 = QUEUE_POS_PAGE(*position);
    let mut offset: c_int = QUEUE_POS_OFFSET(*position);
    let mut pageJump: bool = false;

    /*
     * Move to the next writing position: First jump over what we have just
     * written or read.
     */
    offset += entryLength;
    Assert!(offset as usize <= QUEUE_PAGESIZE);

    /*
     * In a second step check if another entry can possibly be written to the
     * page. If so, stay here, we have reached the next position. If not, then
     * we need to move on to the next page.
     */
    if offset as usize + QUEUEALIGN(AsyncQueueEntryEmptySize()) > QUEUE_PAGESIZE {
        pageno += 1;
        offset = 0;
        pageJump = true;
    }

    SET_QUEUE_POS(&mut *position, pageno, offset);
    pageJump
}

/*
 * Fill the AsyncQueueEntry at *qe with an outbound notification message.
 */
#[allow(non_snake_case)]
unsafe fn asyncQueueNotificationToEntry(n: *mut Notification, qe: *mut AsyncQueueEntry) {
    let channellen: usize = (*n).channel_len as usize;
    let payloadlen: usize = (*n).payload_len as usize;
    let mut entryLength: c_int;

    Assert!(channellen < NAMEDATALEN);
    Assert!(payloadlen < NOTIFY_PAYLOAD_MAX_LENGTH);

    /* The terminators are already included in AsyncQueueEntryEmptySize */
    entryLength = (AsyncQueueEntryEmptySize() + payloadlen + channellen) as c_int;
    entryLength = QUEUEALIGN(entryLength as usize) as c_int;
    (*qe).length = entryLength;
    (*qe).dboid = MyDatabaseId;
    (*qe).xid = GetCurrentTransactionId();
    (*qe).srcPid = MyProcPid;
    libc::memcpy((*qe).data.as_mut_ptr() as *mut c_void,
                 (*n).data.as_ptr() as *const c_void,
                 channellen + payloadlen + 2);
}

/*
 * Add pending notifications to the queue.
 *
 * We go page by page here, i.e. we stop once we have to go to a new page but
 * we will be called again and then fill that next page. If an entry does not
 * fit into the current page, we write a dummy entry with an InvalidOid as the
 * database OID in order to fill the page. So every page is always used up to
 * the last byte which simplifies reading the page later.
 *
 * We are passed the list cell (in pendingNotifies->events) containing the next
 * notification to write and return the first still-unwritten cell back.
 * Eventually we will return NULL indicating all is done.
 *
 * We are holding NotifyQueueLock already from the caller and grab
 * page specific SLRU bank lock locally in this function.
 */
#[allow(non_snake_case)]
unsafe fn asyncQueueAddEntries(mut nextNotify: *mut ListCell) -> *mut ListCell {
    let mut qe: AsyncQueueEntry = core::mem::zeroed();
    let mut queue_head: QueuePosition;
    let mut pageno: int64;
    let mut offset: c_int;
    let mut slotno: c_int;
    let mut prevlock: *mut LWLock;

    /*
     * We work with a local copy of QUEUE_HEAD, which we write back to shared
     * memory upon exiting.  The reason for this is that if we have to advance
     * to a new page, SimpleLruZeroPage might fail (out of disk space, for
     * instance), and we must not advance QUEUE_HEAD if it does.  (Otherwise,
     * subsequent insertions would try to put entries into a page that slru.c
     * thinks doesn't exist yet.)  So, use a local position variable.  Note
     * that if we do fail, any already-inserted queue entries are forgotten;
     * this is okay, since they'd be useless anyway after our transaction
     * rolls back.
     */
    queue_head = QUEUE_HEAD();

    /*
     * If this is the first write since the postmaster started, we need to
     * initialize the first page of the async SLRU.  Otherwise, the current
     * page should be initialized already, so just fetch it.
     */
    pageno = QUEUE_POS_PAGE(queue_head);
    prevlock = SimpleLruGetBankLock(NotifyCtl(), pageno);

    /* We hold both NotifyQueueLock and SLRU bank lock during this operation */
    LWLockAcquire(prevlock, LW_EXCLUSIVE);

    if QUEUE_POS_IS_ZERO(queue_head) {
        slotno = SimpleLruZeroPage(NotifyCtl(), pageno);
    } else {
        slotno = SimpleLruReadPage(NotifyCtl(), pageno, true,
                                   InvalidTransactionId);
    }

    /* Note we mark the page dirty before writing in it */
    *(*(*NotifyCtl()).shared).page_dirty.add(slotno as usize) = true;

    while !nextNotify.is_null() {
        let n: *mut Notification = lfirst(nextNotify) as *mut Notification;

        /* Construct a valid queue entry in local variable qe */
        asyncQueueNotificationToEntry(n, &mut qe);

        offset = QUEUE_POS_OFFSET(queue_head);

        /* Check whether the entry really fits on the current page */
        if (offset + qe.length) as usize <= QUEUE_PAGESIZE {
            /* OK, so advance nextNotify past this item */
            nextNotify = lnext((*pendingNotifies).events, nextNotify);
        } else {
            /*
             * Write a dummy entry to fill up the page. Actually readers will
             * only check dboid and since it won't match any reader's database
             * OID, they will ignore this entry and move on.
             */
            qe.length = QUEUE_PAGESIZE as c_int - offset;
            qe.dboid = InvalidOid;
            qe.xid = InvalidTransactionId;
            qe.data[0] = b'\0' as c_char; /* empty channel */
            qe.data[1] = b'\0' as c_char; /* empty payload */
        }

        /* Now copy qe into the shared buffer page */
        libc::memcpy(
            (*(*(*NotifyCtl()).shared).page_buffer.add(slotno as usize))
                .add(offset as usize) as *mut c_void,
            &qe as *const AsyncQueueEntry as *const c_void,
            qe.length as usize);

        /* Advance queue_head appropriately, and detect if page is full */
        if asyncQueueAdvance(&mut queue_head, qe.length) {
            let lock: *mut LWLock;

            pageno = QUEUE_POS_PAGE(queue_head);
            lock = SimpleLruGetBankLock(NotifyCtl(), pageno);
            if lock != prevlock {
                LWLockRelease(prevlock);
                LWLockAcquire(lock, LW_EXCLUSIVE);
                prevlock = lock;
            }

            /*
             * Page is full, so we're done here, but first fill the next page
             * with zeroes.  The reason to do this is to ensure that slru.c's
             * idea of the head page is always the same as ours, which avoids
             * boundary problems in SimpleLruTruncate.  The test in
             * asyncQueueIsFull() ensured that there is room to create this
             * page without overrunning the queue.
             */
            slotno = SimpleLruZeroPage(NotifyCtl(), QUEUE_POS_PAGE(queue_head));

            /*
             * If the new page address is a multiple of QUEUE_CLEANUP_DELAY,
             * set flag to remember that we should try to advance the tail
             * pointer (we don't want to actually do that right here).
             */
            if QUEUE_POS_PAGE(queue_head) % QUEUE_CLEANUP_DELAY == 0 {
                tryAdvanceTail = true;
            }

            /* And exit the loop */
            break;
        }
    }

    /* Success, so update the global QUEUE_HEAD */
    *QUEUE_HEAD_mut() = queue_head;

    LWLockRelease(prevlock);

    nextNotify
}

/*
 * SQL function to return the fraction of the notification queue currently
 * occupied.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_notification_queue_usage(_fcinfo: FunctionCallInfo) -> Datum {
    let usage: f64;

    /* Advance the queue tail so we don't report a too-large result */
    asyncQueueAdvanceTail();

    LWLockAcquire(NotifyQueueLock, LW_SHARED);
    usage = asyncQueueUsage();
    LWLockRelease(NotifyQueueLock);

    PG_RETURN_FLOAT8!(usage)
}

/*
 * Return the fraction of the queue that is currently occupied.
 *
 * The caller must hold NotifyQueueLock in (at least) shared mode.
 *
 * Note: we measure the distance to the logical tail page, not the physical
 * tail page.  In some sense that's wrong, but the relative position of the
 * physical tail is affected by details such as SLRU segment boundaries,
 * so that a result based on that is unpleasantly unstable.
 */
#[allow(non_snake_case)]
unsafe fn asyncQueueUsage() -> f64 {
    let headPage: int64 = QUEUE_POS_PAGE(QUEUE_HEAD());
    let tailPage: int64 = QUEUE_POS_PAGE(QUEUE_TAIL());
    let occupied: int64 = headPage - tailPage;

    if occupied == 0 {
        return 0 as f64; /* fast exit for common case */
    }

    occupied as f64 / max_notify_queue_pages as f64
}

/*
 * Check whether the queue is at least half full, and emit a warning if so.
 *
 * This is unlikely given the size of the queue, but possible.
 * The warnings show up at most once every QUEUE_FULL_WARN_INTERVAL.
 *
 * Caller must hold exclusive NotifyQueueLock.
 */
#[allow(non_snake_case)]
unsafe fn asyncQueueFillWarning() {
    let fillDegree: f64;
    let t: TimestampTz;

    fillDegree = asyncQueueUsage();
    if fillDegree < 0.5 {
        return;
    }

    t = GetCurrentTimestamp();

    if TimestampDifferenceExceeds((*asyncQueueControl).lastQueueFillWarn,
                                  t, QUEUE_FULL_WARN_INTERVAL) {
        let mut min: QueuePosition = QUEUE_HEAD();
        let mut minPid: int32 = InvalidPid;

        let mut i: ProcNumber = QUEUE_FIRST_LISTENER();
        while i != INVALID_PROC_NUMBER {
            Assert!(QUEUE_BACKEND_PID(i) != InvalidPid);
            min = QUEUE_POS_MIN(min, QUEUE_BACKEND_POS(i));
            if QUEUE_POS_EQUAL(min, QUEUE_BACKEND_POS(i)) {
                minPid = QUEUE_BACKEND_PID(i);
            }
            i = QUEUE_NEXT_LISTENER(i);
        }

        ereport!(WARNING, errmsg!("NOTIFY queue is {:.0}% full", fillDegree * 100.0));
        /* C also: (minPid != InvalidPid ? */
        /*   errdetail("The server process with PID %d is among those with the oldest transactions.", minPid) : 0) */
        /* C also: (minPid != InvalidPid ? */
        /*   errhint("The NOTIFY queue cannot be emptied until that process ends its current transaction.") : 0) */
        let _ = minPid;

        (*asyncQueueControl).lastQueueFillWarn = t;
    }
}

/*
 * Send signals to listening backends.
 *
 * Normally we signal only backends in our own database, since only those
 * backends could be interested in notifies we send.  However, if there's
 * notify traffic in our database but no traffic in another database that
 * does have listener(s), those listeners will fall further and further
 * behind.  Waken them anyway if they're far enough behind, so that they'll
 * advance their queue position pointers, allowing the global tail to advance.
 *
 * Since we know the ProcNumber and the Pid the signaling is quite cheap.
 *
 * This is called during CommitTransaction(), so it's important for it
 * to have very low probability of failure.
 */
#[allow(non_snake_case)]
unsafe fn SignalBackends() {
    let pids: *mut int32;
    let procnos: *mut ProcNumber;
    let mut count: c_int;

    /*
     * Identify backends that we need to signal.  We don't want to send
     * signals while holding the NotifyQueueLock, so this loop just builds a
     * list of target PIDs.
     *
     * XXX in principle these pallocs could fail, which would be bad. Maybe
     * preallocate the arrays?  They're not that large, though.
     */
    pids = palloc(MaxBackends as usize * size_of::<int32>()) as *mut int32;
    procnos = palloc(MaxBackends as usize * size_of::<ProcNumber>()) as *mut ProcNumber;
    count = 0;

    LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
    let mut i: ProcNumber = QUEUE_FIRST_LISTENER();
    while i != INVALID_PROC_NUMBER {
        let pid: int32 = QUEUE_BACKEND_PID(i);
        let pos: QueuePosition;

        Assert!(pid != InvalidPid);
        pos = QUEUE_BACKEND_POS(i);
        if QUEUE_BACKEND_DBOID(i) == MyDatabaseId {
            /*
             * Always signal listeners in our own database, unless they're
             * already caught up (unlikely, but possible).
             */
            if QUEUE_POS_EQUAL(pos, QUEUE_HEAD()) {
                i = QUEUE_NEXT_LISTENER(i);
                continue;
            }
        } else {
            /*
             * Listeners in other databases should be signaled only if they
             * are far behind.
             */
            if asyncQueuePageDiff(QUEUE_POS_PAGE(QUEUE_HEAD()),
                                  QUEUE_POS_PAGE(pos)) < QUEUE_CLEANUP_DELAY {
                i = QUEUE_NEXT_LISTENER(i);
                continue;
            }
        }
        /* OK, need to signal this one */
        *pids.add(count as usize) = pid;
        *procnos.add(count as usize) = i;
        count += 1;
        i = QUEUE_NEXT_LISTENER(i);
    }
    LWLockRelease(NotifyQueueLock);

    /* Now send signals */
    for i in 0..count {
        let pid: int32 = *pids.add(i as usize);

        /*
         * If we are signaling our own process, no need to involve the kernel;
         * just set the flag directly.
         */
        if pid == MyProcPid {
            notifyInterruptPending = true;
            continue;
        }

        /*
         * Note: assuming things aren't broken, a signal failure here could
         * only occur if the target backend exited since we released
         * NotifyQueueLock; which is unlikely but certainly possible. So we
         * just log a low-level debug message if it happens.
         */
        if SendProcSignal(pid, PROCSIG_NOTIFY_INTERRUPT, *procnos.add(i as usize)) < 0 {
            elog!(DEBUG3, "could not signal backend with PID {}", pid);
            /* C also: %m */
        }
    }

    pfree(pids as *mut c_void);
    pfree(procnos as *mut c_void);
}

/*
 * AtAbort_Notify
 *
 *	This is called at transaction abort.
 *
 *	Gets rid of pending actions and outbound notifies that we would have
 *	executed if the transaction got committed.
 */
#[allow(non_snake_case)]
pub unsafe fn AtAbort_Notify() {
    /*
     * If we LISTEN but then roll back the transaction after PreCommit_Notify,
     * we have registered as a listener but have not made any entry in
     * listenChannels.  In that case, deregister again.
     */
    if amRegisteredListener && listenChannels == NIL {
        asyncQueueUnregister();
    }

    /* And clean up */
    ClearPendingActionsAndNotifies();
}

/*
 * AtSubCommit_Notify() --- Take care of subtransaction commit.
 *
 * Reassign all items in the pending lists to the parent transaction.
 */
#[allow(non_snake_case)]
pub unsafe fn AtSubCommit_Notify() {
    let my_level: c_int = GetCurrentTransactionNestLevel();

    /* If there are actions at our nesting level, we must reparent them. */
    if !pendingActions.is_null() &&
        (*pendingActions).nestingLevel >= my_level {
        if (*pendingActions).upper.is_null() ||
            (*(*pendingActions).upper).nestingLevel < my_level - 1 {
            /* nothing to merge; give the whole thing to the parent */
            (*pendingActions).nestingLevel -= 1;
        } else {
            let childPendingActions: *mut ActionList = pendingActions;

            pendingActions = (*pendingActions).upper;

            /*
             * Mustn't try to eliminate duplicates here --- see queue_listen()
             */
            (*pendingActions).actions =
                list_concat((*pendingActions).actions,
                            (*childPendingActions).actions);
            pfree(childPendingActions as *mut c_void);
        }
    }

    /* If there are notifies at our nesting level, we must reparent them. */
    if !pendingNotifies.is_null() &&
        (*pendingNotifies).nestingLevel >= my_level {
        Assert!((*pendingNotifies).nestingLevel == my_level);

        if (*pendingNotifies).upper.is_null() ||
            (*(*pendingNotifies).upper).nestingLevel < my_level - 1 {
            /* nothing to merge; give the whole thing to the parent */
            (*pendingNotifies).nestingLevel -= 1;
        } else {
            /*
             * Formerly, we didn't bother to eliminate duplicates here, but
             * now we must, else we fall foul of "Assert(!found)", either here
             * or during a later attempt to build the parent-level hashtable.
             */
            let childPendingNotifies: *mut NotificationList = pendingNotifies;

            pendingNotifies = (*pendingNotifies).upper;
            /* Insert all the subxact's events into parent, except for dups */
            foreach!(l, (*childPendingNotifies).events, {
                let childn: *mut Notification = lfirst(current_cell!(l)) as *mut Notification;

                if !AsyncExistsPendingNotify(childn) {
                    AddEventToPendingNotifies(childn);
                }
            });
            pfree(childPendingNotifies as *mut c_void);
        }
    }
}

/*
 * AtSubAbort_Notify() --- Take care of subtransaction abort.
 */
#[allow(non_snake_case)]
pub unsafe fn AtSubAbort_Notify() {
    let my_level: c_int = GetCurrentTransactionNestLevel();

    /*
     * All we have to do is pop the stack --- the actions/notifies made in
     * this subxact are no longer interesting, and the space will be freed
     * when CurTransactionContext is recycled. We still have to free the
     * ActionList and NotificationList objects themselves, though, because
     * those are allocated in TopTransactionContext.
     *
     * Note that there might be no entries at all, or no entries for the
     * current subtransaction level, either because none were ever created, or
     * because we reentered this routine due to trouble during subxact abort.
     */
    while !pendingActions.is_null() &&
        (*pendingActions).nestingLevel >= my_level {
        let childPendingActions: *mut ActionList = pendingActions;

        pendingActions = (*pendingActions).upper;
        pfree(childPendingActions as *mut c_void);
    }

    while !pendingNotifies.is_null() &&
        (*pendingNotifies).nestingLevel >= my_level {
        let childPendingNotifies: *mut NotificationList = pendingNotifies;

        pendingNotifies = (*pendingNotifies).upper;
        pfree(childPendingNotifies as *mut c_void);
    }
}

/*
 * HandleNotifyInterrupt
 *
 *		Signal handler portion of interrupt handling. Let the backend know
 *		that there's a pending notify interrupt. If we're currently reading
 *		from the client, this will interrupt the read and
 *		ProcessClientReadInterrupt() will call ProcessNotifyInterrupt().
 */
#[allow(non_snake_case)]
pub unsafe fn HandleNotifyInterrupt() {
    /*
     * Note: this is called by a SIGNAL HANDLER. You must be very wary what
     * you do here.
     */

    /* signal that work needs to be done */
    notifyInterruptPending = true;

    /* make sure the event is processed in due course */
    SetLatch(MyLatch);
}

/*
 * ProcessNotifyInterrupt
 *
 *		This is called if we see notifyInterruptPending set, just before
 *		transmitting ReadyForQuery at the end of a frontend command, and
 *		also if a notify signal occurs while reading from the frontend.
 *		HandleNotifyInterrupt() will cause the read to be interrupted
 *		via the process's latch, and this routine will get called.
 *		If we are truly idle (ie, *not* inside a transaction block),
 *		process the incoming notifies.
 *
 *		If "flush" is true, force any frontend messages out immediately.
 *		This can be false when being called at the end of a frontend command,
 *		since we'll flush after sending ReadyForQuery.
 */
#[allow(non_snake_case)]
pub unsafe fn ProcessNotifyInterrupt(flush: bool) {
    if IsTransactionOrTransactionBlock() {
        return; /* not really idle */
    }

    /* Loop in case another signal arrives while sending messages */
    while notifyInterruptPending {
        ProcessIncomingNotify(flush);
    }
}

/*
 * Read all pending notifications from the queue, and deliver appropriate
 * ones to my frontend.  Stop when we reach queue head or an uncommitted
 * notification.
 */
#[allow(non_snake_case)]
unsafe fn asyncQueueReadAllNotifications() {
    let mut pos: QueuePosition;
    let head: QueuePosition;
    let snapshot: Snapshot;

    /* Fetch current state */
    LWLockAcquire(NotifyQueueLock, LW_SHARED);
    /* Assert checks that we have a valid state entry */
    Assert!(MyProcPid == QUEUE_BACKEND_PID(MyProcNumber));
    pos = QUEUE_BACKEND_POS(MyProcNumber);
    head = QUEUE_HEAD();
    LWLockRelease(NotifyQueueLock);

    if QUEUE_POS_EQUAL(pos, head) {
        /* Nothing to do, we have read all notifications already. */
        return;
    }

    /*----------
     * Get snapshot we'll use to decide which xacts are still in progress.
     * This is trickier than it might seem, because of race conditions.
     * Consider the following example:
     *
     * Backend 1:					 Backend 2:
     *
     * transaction starts
     * UPDATE foo SET ...;
     * NOTIFY foo;
     * commit starts
     * queue the notify message
     *								 transaction starts
     *								 LISTEN foo;  -- first LISTEN in session
     *								 SELECT * FROM foo WHERE ...;
     * commit to clog
     *								 commit starts
     *								 add backend 2 to array of listeners
     *								 advance to queue head (this code)
     *								 commit to clog
     *
     * Transaction 2's SELECT has not seen the UPDATE's effects, since that
     * wasn't committed yet.  Ideally we'd ensure that client 2 would
     * eventually get transaction 1's notify message, but there's no way
     * to do that; until we're in the listener array, there's no guarantee
     * that the notify message doesn't get removed from the queue.
     *
     * Therefore the coding technique transaction 2 is using is unsafe:
     * applications must commit a LISTEN before inspecting database state,
     * if they want to ensure they will see notifications about subsequent
     * changes to that state.
     *
     * What we do guarantee is that we'll see all notifications from
     * transactions committing after the snapshot we take here.
     * Exec_ListenPreCommit has already added us to the listener array,
     * so no not-yet-committed messages can be removed from the queue
     * before we see them.
     *----------
     */
    snapshot = RegisterSnapshot(GetLatestSnapshot());

    /*
     * It is possible that we fail while trying to send a message to our
     * frontend (for example, because of encoding conversion failure).  If
     * that happens it is critical that we not try to send the same message
     * over and over again.  Therefore, we set ExitOnAnyError to upgrade any
     * ERRORs to FATAL, causing the client connection to be closed on error.
     *
     * We used to only skip over the offending message and try to soldier on,
     * but it was somewhat questionable to lose a notification and give the
     * client an ERROR instead.  A client application is not be prepared for
     * that and can't tell that a notification was missed.  It was also not
     * very useful in practice because notifications are often processed while
     * a connection is idle and reading a message from the client, and in that
     * state, any error is upgraded to FATAL anyway.  Closing the connection
     * is a clear signal to the application that it might have missed
     * notifications.
     */
    {
        let save_ExitOnAnyError: bool = ExitOnAnyError;
        let mut reachedStop: bool;

        ExitOnAnyError = true;

        loop {
            /*
             * Process messages up to the stop position, end of page, or an
             * uncommitted message.
             *
             * Our stop position is what we found to be the head's position
             * when we entered this function. It might have changed already.
             * But if it has, we will receive (or have already received and
             * queued) another signal and come here again.
             *
             * We are not holding NotifyQueueLock here! The queue can only
             * extend beyond the head pointer (see above) and we leave our
             * backend's pointer where it is so nobody will truncate or
             * rewrite pages under us. Especially we don't want to hold a lock
             * while sending the notifications to the frontend.
             */
            reachedStop = asyncQueueProcessPageEntries(&mut pos, head, snapshot);
            if reachedStop {
                break;
            }
        }

        /* Update shared state */
        LWLockAcquire(NotifyQueueLock, LW_SHARED);
        QUEUE_BACKEND_POS_set(MyProcNumber, pos);
        LWLockRelease(NotifyQueueLock);

        ExitOnAnyError = save_ExitOnAnyError;
    }

    /* Done with snapshot */
    UnregisterSnapshot(snapshot);
}

/*
 * Fetch notifications from the shared queue, beginning at position current,
 * and deliver relevant ones to my frontend.
 *
 * The function returns true once we have reached the stop position or an
 * uncommitted notification, and false if we have finished with the page.
 * In other words: once it returns true there is no need to look further.
 * The QueuePosition *current is advanced past all processed messages.
 */
#[allow(non_snake_case)]
unsafe fn asyncQueueProcessPageEntries(current: *mut QueuePosition,
                                       stop: QueuePosition,
                                       snapshot: Snapshot) -> bool {
    let curpage: int64 = QUEUE_POS_PAGE(*current);
    let slotno: c_int;
    let page_buffer: *mut c_char;
    let mut reachedStop: bool = false;
    let mut reachedEndOfPage: bool;

    /*
     * We copy the entries into a local buffer to avoid holding the SLRU lock
     * while we transmit them to our frontend.  The local buffer must be
     * adequately aligned, so use a union.
     */
    #[repr(C, align(4))]
    struct LocalBuf {
        buf: [c_char; QUEUE_PAGESIZE],
    }
    let mut local_buf: LocalBuf = core::mem::zeroed();
    let local_buf_start: *mut c_char = local_buf.buf.as_mut_ptr();
    let mut local_buf_end: *mut c_char = local_buf_start;

    slotno = SimpleLruReadPage_ReadOnly(NotifyCtl(), curpage,
                                        InvalidTransactionId);
    page_buffer = *(*(*NotifyCtl()).shared).page_buffer.add(slotno as usize);

    loop {
        let thisentry: QueuePosition = *current;
        let qe: *mut AsyncQueueEntry;

        if QUEUE_POS_EQUAL(thisentry, stop) {
            break;
        }

        qe = page_buffer.add(QUEUE_POS_OFFSET(thisentry) as usize) as *mut AsyncQueueEntry;

        /*
         * Advance *current over this message, possibly to the next page. As
         * noted in the comments for asyncQueueReadAllNotifications, we must
         * do this before possibly failing while processing the message.
         */
        reachedEndOfPage = asyncQueueAdvance(current, (*qe).length);

        /* Ignore messages destined for other databases */
        if (*qe).dboid == MyDatabaseId {
            if XidInMVCCSnapshot((*qe).xid, snapshot) {
                /*
                 * The source transaction is still in progress, so we can't
                 * process this message yet.  Break out of the loop, but first
                 * back up *current so we will reprocess the message next
                 * time.  (Note: it is unlikely but not impossible for
                 * TransactionIdDidCommit to fail, so we can't really avoid
                 * this advance-then-back-up behavior when dealing with an
                 * uncommitted message.)
                 *
                 * Note that we must test XidInMVCCSnapshot before we test
                 * TransactionIdDidCommit, else we might return a message from
                 * a transaction that is not yet visible to snapshots; compare
                 * the comments at the head of heapam_visibility.c.
                 *
                 * Also, while our own xact won't be listed in the snapshot,
                 * we need not check for TransactionIdIsCurrentTransactionId
                 * because our transaction cannot (yet) have queued any
                 * messages.
                 */
                *current = thisentry;
                reachedStop = true;
                break;
            }

            /*
             * Quick check for the case that we're not listening on any
             * channels, before calling TransactionIdDidCommit().  This makes
             * that case a little faster, but more importantly, it ensures
             * that if there's a bad entry in the queue for which
             * TransactionIdDidCommit() fails for some reason, we can skip
             * over it on the first LISTEN in a session, and not get stuck on
             * it indefinitely.
             */
            if listenChannels == NIL {
                if reachedEndOfPage {
                    break;
                }
                continue;
            }

            if TransactionIdDidCommit((*qe).xid) {
                libc::memcpy(local_buf_end as *mut c_void,
                             qe as *const c_void,
                             (*qe).length as usize);
                local_buf_end = local_buf_end.add((*qe).length as usize);
            } else {
                /*
                 * The source transaction aborted or crashed, so we just
                 * ignore its notifications.
                 */
            }
        }

        /* Loop back if we're not at end of page */
        if reachedEndOfPage {
            break;
        }
    }

    /* Release lock that we got from SimpleLruReadPage_ReadOnly() */
    LWLockRelease(SimpleLruGetBankLock(NotifyCtl(), curpage));

    /*
     * Now that we have let go of the SLRU bank lock, send the notifications
     * to our backend
     */
    Assert!(local_buf_end as usize - local_buf_start as usize <= BLCKSZ);
    let mut p: *mut c_char = local_buf_start;
    while p < local_buf_end {
        let qe: *mut AsyncQueueEntry = p as *mut AsyncQueueEntry;

        /* qe->data is the null-terminated channel name */
        let channel: *mut c_char = (*qe).data.as_mut_ptr();

        if IsListeningOn(channel) {
            /* payload follows channel name */
            let payload: *mut c_char = (*qe).data.as_mut_ptr().add(libc::strlen(channel) + 1);

            NotifyMyFrontEnd(channel, payload, (*qe).srcPid);
        }

        p = p.add((*qe).length as usize);
    }

    if QUEUE_POS_EQUAL(*current, stop) {
        reachedStop = true;
    }

    reachedStop
}

/*
 * Advance the shared queue tail variable to the minimum of all the
 * per-backend tail pointers.  Truncate pg_notify space if possible.
 *
 * This is (usually) called during CommitTransaction(), so it's important for
 * it to have very low probability of failure.
 */
#[allow(non_snake_case)]
unsafe fn asyncQueueAdvanceTail() {
    let mut min: QueuePosition;
    let oldtailpage: int64;
    let newtailpage: int64;
    let boundary: int64;

    /* Restrict task to one backend per cluster; see SimpleLruTruncate(). */
    LWLockAcquire(NotifyQueueTailLock, LW_EXCLUSIVE);

    /*
     * Compute the new tail.  Pre-v13, it's essential that QUEUE_TAIL be exact
     * (ie, exactly match at least one backend's queue position), so it must
     * be updated atomically with the actual computation.  Since v13, we could
     * get away with not doing it like that, but it seems prudent to keep it
     * so.
     *
     * Also, because incoming backends will scan forward from QUEUE_TAIL, that
     * must be advanced before we can truncate any data.  Thus, QUEUE_TAIL is
     * the logical tail, while QUEUE_STOP_PAGE is the physical tail, or oldest
     * un-truncated page.  When QUEUE_STOP_PAGE != QUEUE_POS_PAGE(QUEUE_TAIL),
     * there are pages we can truncate but haven't yet finished doing so.
     *
     * For concurrency's sake, we don't want to hold NotifyQueueLock while
     * performing SimpleLruTruncate.  This is OK because no backend will try
     * to access the pages we are in the midst of truncating.
     */
    LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
    min = QUEUE_HEAD();
    let mut i: ProcNumber = QUEUE_FIRST_LISTENER();
    while i != INVALID_PROC_NUMBER {
        Assert!(QUEUE_BACKEND_PID(i) != InvalidPid);
        min = QUEUE_POS_MIN(min, QUEUE_BACKEND_POS(i));
        i = QUEUE_NEXT_LISTENER(i);
    }
    *QUEUE_TAIL_mut() = min;
    oldtailpage = QUEUE_STOP_PAGE();
    LWLockRelease(NotifyQueueLock);

    /*
     * We can truncate something if the global tail advanced across an SLRU
     * segment boundary.
     *
     * XXX it might be better to truncate only once every several segments, to
     * reduce the number of directory scans.
     */
    newtailpage = QUEUE_POS_PAGE(min);
    boundary = newtailpage - (newtailpage % SLRU_PAGES_PER_SEGMENT);
    if asyncQueuePagePrecedes(oldtailpage, boundary) {
        /*
         * SimpleLruTruncate() will ask for SLRU bank locks but will also
         * release the lock again.
         */
        SimpleLruTruncate(NotifyCtl(), newtailpage);

        LWLockAcquire(NotifyQueueLock, LW_EXCLUSIVE);
        *QUEUE_STOP_PAGE_mut() = newtailpage;
        LWLockRelease(NotifyQueueLock);
    }

    LWLockRelease(NotifyQueueTailLock);
}

/*
 * AsyncNotifyFreezeXids
 *
 * Prepare the async notification queue for CLOG truncation by freezing
 * transaction IDs that are about to become inaccessible.
 *
 * This function is called by VACUUM before advancing datfrozenxid. It scans
 * the notification queue and replaces XIDs that would become inaccessible
 * after CLOG truncation with special markers:
 * - Committed transactions are set to FrozenTransactionId
 * - Aborted/crashed transactions are set to InvalidTransactionId
 *
 * Only XIDs < newFrozenXid are processed, as those are the ones whose CLOG
 * pages will be truncated. If XID < newFrozenXid, it cannot still be running
 * (or it would have held back newFrozenXid through ProcArray).
 * Therefore, if TransactionIdDidCommit returns false, we know the transaction
 * either aborted explicitly or crashed, and we can safely mark it invalid.
 */
#[allow(non_snake_case)]
pub unsafe fn AsyncNotifyFreezeXids(newFrozenXid: TransactionId) {
    let mut pos: QueuePosition;
    let head: QueuePosition;
    let mut curpage: int64 = -1;
    let mut slotno: c_int = -1;
    let mut page_buffer: *mut c_char = ptr::null_mut();
    let mut page_dirty: bool = false;

    /*
     * Acquire locks in the correct order to avoid deadlocks. As per the
     * locking protocol: NotifyQueueTailLock, then NotifyQueueLock, then SLRU
     * bank locks.
     *
     * We only need SHARED mode since we're just reading the head/tail
     * positions, not modifying them.
     */
    LWLockAcquire(NotifyQueueTailLock, LW_SHARED);
    LWLockAcquire(NotifyQueueLock, LW_SHARED);

    pos = QUEUE_TAIL();
    head = QUEUE_HEAD();

    /* Release NotifyQueueLock early, we only needed to read the positions */
    LWLockRelease(NotifyQueueLock);

    /*
     * Scan the queue from tail to head, freezing XIDs as needed. We hold
     * NotifyQueueTailLock throughout to ensure the tail doesn't move while
     * we're working.
     */
    while !QUEUE_POS_EQUAL(pos, head) {
        let qe: *mut AsyncQueueEntry;
        let xid: TransactionId;
        let pageno: int64 = QUEUE_POS_PAGE(pos);
        let offset: c_int = QUEUE_POS_OFFSET(pos);

        /* If we need a different page, release old lock and get new one */
        if pageno != curpage {
            let lock: *mut LWLock;

            /* Release previous page if any */
            if slotno >= 0 {
                if page_dirty {
                    *(*(*NotifyCtl()).shared).page_dirty.add(slotno as usize) = true;
                    page_dirty = false;
                }
                LWLockRelease(SimpleLruGetBankLock(NotifyCtl(), curpage));
            }

            lock = SimpleLruGetBankLock(NotifyCtl(), pageno);
            LWLockAcquire(lock, LW_EXCLUSIVE);
            slotno = SimpleLruReadPage(NotifyCtl(), pageno, true,
                                       InvalidTransactionId);
            page_buffer = *(*(*NotifyCtl()).shared).page_buffer.add(slotno as usize);
            curpage = pageno;
        }

        qe = page_buffer.add(offset as usize) as *mut AsyncQueueEntry;
        xid = (*qe).xid;

        if TransactionIdIsNormal(xid) &&
            TransactionIdPrecedes(xid, newFrozenXid) {
            if TransactionIdDidCommit(xid) {
                (*qe).xid = FrozenTransactionId;
                page_dirty = true;
            } else {
                (*qe).xid = InvalidTransactionId;
                page_dirty = true;
            }
        }

        /* Advance to next entry */
        asyncQueueAdvance(&mut pos, (*qe).length);
    }

    /* Release final page lock if we acquired one */
    if slotno >= 0 {
        if page_dirty {
            *(*(*NotifyCtl()).shared).page_dirty.add(slotno as usize) = true;
        }
        LWLockRelease(SimpleLruGetBankLock(NotifyCtl(), curpage));
    }

    LWLockRelease(NotifyQueueTailLock);
}

/*
 * ProcessIncomingNotify
 *
 *		Scan the queue for arriving notifications and report them to the front
 *		end.  The notifications might be from other sessions, or our own;
 *		there's no need to distinguish here.
 *
 *		If "flush" is true, force any frontend messages out immediately.
 *
 *		NOTE: since we are outside any transaction, we must create our own.
 */
#[allow(non_snake_case)]
unsafe fn ProcessIncomingNotify(flush: bool) {
    /* We *must* reset the flag */
    notifyInterruptPending = false;

    /* Do nothing else if we aren't actively listening */
    if listenChannels == NIL {
        return;
    }

    if Trace_notify {
        elog!(DEBUG1, "ProcessIncomingNotify");
    }

    set_ps_display(c"notify interrupt".as_ptr());

    /*
     * We must run asyncQueueReadAllNotifications inside a transaction, else
     * bad things happen if it gets an error.
     */
    StartTransactionCommand();

    asyncQueueReadAllNotifications();

    CommitTransactionCommand();

    /*
     * If this isn't an end-of-command case, we must flush the notify messages
     * to ensure frontend gets them promptly.
     */
    if flush {
        pq_flush();
    }

    set_ps_display(c"idle".as_ptr());

    if Trace_notify {
        elog!(DEBUG1, "ProcessIncomingNotify: done");
    }
}

/*
 * Send NOTIFY message to my front end.
 */
#[allow(non_snake_case)]
pub unsafe fn NotifyMyFrontEnd(channel: *const c_char, payload: *const c_char,
                               srcPid: int32) {
    if whereToSendOutput == DestRemote {
        let mut buf: StringInfoData = core::mem::zeroed();

        pq_beginmessage(&mut buf as *mut StringInfoData as *mut c_void,
                        PqMsg_NotificationResponse);
        pq_sendint32(&mut buf as *mut StringInfoData as *mut c_void, srcPid);
        pq_sendstring(&mut buf as *mut StringInfoData as *mut c_void, channel);
        pq_sendstring(&mut buf as *mut StringInfoData as *mut c_void, payload);
        pq_endmessage(&mut buf as *mut StringInfoData as *mut c_void);

        /*
         * NOTE: we do not do pq_flush() here.  Some level of caller will
         * handle it later, allowing this message to be combined into a packet
         * with other ones.
         */
    } else {
        elog!(INFO, "NOTIFY for \"{}\" payload \"{}\"",
              std::ffi::CStr::from_ptr(channel).to_string_lossy(),
              std::ffi::CStr::from_ptr(payload).to_string_lossy());
    }
}

/* Does pendingNotifies include a match for the given event? */
#[allow(non_snake_case)]
unsafe fn AsyncExistsPendingNotify(n: *mut Notification) -> bool {
    if pendingNotifies.is_null() {
        return false;
    }

    if !(*pendingNotifies).hashtab.is_null() {
        /* Use the hash table to probe for a match */
        if !hash_search((*pendingNotifies).hashtab,
                        &n as *const *mut Notification as *const c_void,
                        HASH_FIND,
                        ptr::null_mut()).is_null() {
            return true;
        }
    } else {
        /* Must scan the event list */
        foreach!(l, (*pendingNotifies).events, {
            let oldn: *mut Notification = lfirst(current_cell!(l)) as *mut Notification;

            if (*n).channel_len == (*oldn).channel_len &&
                (*n).payload_len == (*oldn).payload_len &&
                libc::memcmp((*n).data.as_ptr() as *const c_void,
                             (*oldn).data.as_ptr() as *const c_void,
                             ((*n).channel_len + (*n).payload_len + 2) as usize) == 0 {
                return true;
            }
        });
    }

    false
}

/*
 * Add a notification event to a pre-existing pendingNotifies list.
 *
 * Because pendingNotifies->events is already nonempty, this works
 * correctly no matter what CurrentMemoryContext is.
 */
#[allow(non_snake_case)]
unsafe fn AddEventToPendingNotifies(n: *mut Notification) {
    Assert!((*pendingNotifies).events != NIL);

    /* Create the hash table if it's time to */
    if list_length((*pendingNotifies).events) >= MIN_HASHABLE_NOTIFIES &&
        (*pendingNotifies).hashtab.is_null() {
        let mut hash_ctl: HASHCTL = core::mem::zeroed();

        /* Create the hash table */
        hash_ctl.keysize = size_of::<*mut Notification>();
        hash_ctl.entrysize = size_of::<NotificationHash>();
        hash_ctl.hash = Some(notification_hash);
        hash_ctl.match_fn = Some(notification_match);
        hash_ctl.hcxt = CurTransactionContext;
        (*pendingNotifies).hashtab =
            hash_create(c"Pending Notifies".as_ptr(),
                        256i64,
                        &mut hash_ctl,
                        HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

        /* Insert all the already-existing events */
        foreach!(l, (*pendingNotifies).events, {
            let oldn: *mut Notification = lfirst(current_cell!(l)) as *mut Notification;
            let mut found: bool = false;

            let _ = hash_search((*pendingNotifies).hashtab,
                                &oldn as *const *mut Notification as *const c_void,
                                HASH_ENTER,
                                &mut found);
            Assert!(!found);
        });
    }

    /* Add new event to the list, in order */
    (*pendingNotifies).events = lappend((*pendingNotifies).events, n as *mut c_void);

    /* Add event to the hash table if needed */
    if !(*pendingNotifies).hashtab.is_null() {
        let mut found: bool = false;

        let _ = hash_search((*pendingNotifies).hashtab,
                            &n as *const *mut Notification as *const c_void,
                            HASH_ENTER,
                            &mut found);
        Assert!(!found);
    }
}

/*
 * notification_hash: hash function for notification hash table
 *
 * The hash "keys" are pointers to Notification structs.
 */
#[allow(non_snake_case)]
unsafe fn notification_hash(key: *const c_void, keysize: Size) -> uint32 {
    let k: *const Notification = *(key as *const *const Notification);

    Assert!(keysize == size_of::<*mut Notification>());
    /* We don't bother to include the payload's trailing null in the hash */
    DatumGetUInt32(hash_any((*k).data.as_ptr() as *const u8,
                            ((*k).channel_len + (*k).payload_len + 1) as c_int))
}

/*
 * notification_match: match function to use with notification_hash
 */
#[allow(non_snake_case)]
unsafe fn notification_match(key1: *const c_void, key2: *const c_void,
                             keysize: Size) -> c_int {
    let k1: *const Notification = *(key1 as *const *const Notification);
    let k2: *const Notification = *(key2 as *const *const Notification);

    Assert!(keysize == size_of::<*mut Notification>());
    if (*k1).channel_len == (*k2).channel_len &&
        (*k1).payload_len == (*k2).payload_len &&
        libc::memcmp((*k1).data.as_ptr() as *const c_void,
                     (*k2).data.as_ptr() as *const c_void,
                     ((*k1).channel_len + (*k1).payload_len + 2) as usize) == 0 {
        return 0; /* equal */
    }
    1 /* not equal */
}

/* Clear the pendingActions and pendingNotifies lists. */
#[allow(non_snake_case)]
unsafe fn ClearPendingActionsAndNotifies() {
    /*
     * Everything's allocated in either TopTransactionContext or the context
     * for the subtransaction to which it corresponds.  So, there's nothing to
     * do here except reset the pointers; the space will be reclaimed when the
     * contexts are deleted.
     */
    pendingActions = ptr::null_mut();
    pendingNotifies = ptr::null_mut();
}

/*
 * GUC check_hook for notify_buffers
 */
#[allow(non_snake_case)]
pub unsafe fn check_notify_buffers(newval: *mut c_int, _extra: *mut *mut c_void,
                                   _source: GucSource) -> bool {
    check_slru_buffers(c"notify_buffers".as_ptr(), newval)
}

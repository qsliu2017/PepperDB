//! storage/lmgr/lwlock.c -- Lightweight lock manager
//!
//! Lightweight locks are intended primarily to provide mutual exclusion of
//! access to shared-memory data structures.  Therefore, they offer both
//! exclusive and shared lock modes (to support read/write and read-only
//! access to a shared object).  There are few other frammishes.  User-level
//! locking should be done with the full lock manager --- which depends on
//! LWLocks to protect its shared state.
//!
//! In addition to exclusive and shared modes, lightweight locks can be used to
//! wait until a variable changes value.  The variable is initially not set
//! when the lock is acquired with LWLockAcquire, i.e. it remains set to the
//! value it was set to when the lock was released last, and can be updated
//! without releasing the lock by calling LWLockUpdateVar.  LWLockWaitForVar
//! waits for the variable to be updated, or until the lock is free.  When
//! releasing the lock with LWLockReleaseClearVar() the value can be set to an
//! appropriate value for a free lock.  The meaning of the variable is up to
//! the caller, the lightweight lock code just assigns and compares it.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/lmgr/lwlock.c

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use crate::prelude::*;

use core::ffi::CStr;
use std::ffi::c_int;

// MAX_BACKENDS lives in storage/procnumber.h
use crate::storage::procnumber::{MyProcNumber, ProcNumber, MAX_BACKENDS};

// proclist types and operations (storage/proclist_types.h, storage/proclist.h)
use crate::storage::proclist_types::{proclist_head, proclist_mutable_iter};
use crate::storage::proclist::{proclist_init, proclist_is_empty, GetPGProcByNumber, PGPROC};
use crate::{
    proclist_delete, proclist_foreach_modify, proclist_push_head, proclist_push_tail,
};

// Spinlock API (storage/spin.h) and ShmemLock (storage/ipc/shmem.c)
use crate::storage::spin::{SpinLockAcquire, SpinLockRelease};
use crate::storage::ipc::shmem::ShmemLock;

// Spin-delay loop (storage/s_lock.h)
use crate::storage::lmgr::s_lock::{
    finish_spin_delay, perform_spin_delay, SpinDelayStatus, DEFAULT_SPINS_PER_DELAY,
};

// NUM_INDIVIDUAL_LWLOCKS (storage/lwlocknames.h, generated)
use crate::storage::lwlocklist::NUM_INDIVIDUAL_LWLOCKS;

// pg_atomic_uint64 (port/atomics.h)
use crate::port::atomics_backend::pg_atomic_uint64;

// PG_CACHE_LINE_SIZE / NAMEDATALEN
use crate::pg_config_manual::{NAMEDATALEN, PG_CACHE_LINE_SIZE};

// pg_nextpower2_32 (port/pg_bitutils.h)
use crate::port::pg_bitutils::pg_nextpower2_32;

// Interrupt holdoff (miscadmin.h)
use crate::miscadmin::{HOLD_INTERRUPTS, RESUME_INTERRUPTS};

// =============================================================================
// Dependency stubs --- definitions live in other .c files not yet ported.
// =============================================================================

// pg_atomic_uint32 and the u32/u64 atomic op API (port/atomics.h).
// TODO(pg-port): real defs live in port/atomics.h.
#[repr(C)]
pub struct pg_atomic_uint32 {
    pub value: u32,
}

unsafe fn pg_atomic_init_u32(ptr: *mut pg_atomic_uint32, val: u32) {
    crate::port::atomics::pg_atomic_init_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        val,
    )
}
unsafe fn pg_atomic_read_u32(ptr: *mut pg_atomic_uint32) -> u32 {
    crate::port::atomics::pg_atomic_read_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
    )
}
unsafe fn pg_atomic_compare_exchange_u32(
    ptr: *mut pg_atomic_uint32,
    expected: *mut u32,
    newval: u32,
) -> bool {
    crate::port::atomics::pg_atomic_compare_exchange_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        &mut *expected,
        newval,
    )
}
unsafe fn pg_atomic_fetch_or_u32(ptr: *mut pg_atomic_uint32, or_: u32) -> u32 {
    crate::port::atomics::generic::pg_atomic_fetch_or_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        or_,
    )
}
unsafe fn pg_atomic_fetch_and_u32(ptr: *mut pg_atomic_uint32, and_: u32) -> u32 {
    crate::port::atomics::generic::pg_atomic_fetch_and_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        and_,
    )
}
unsafe fn pg_atomic_fetch_add_u32(ptr: *mut pg_atomic_uint32, add_: u32) -> u32 {
    crate::port::atomics::pg_atomic_fetch_add_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        add_ as i32,
    )
}
unsafe fn pg_atomic_fetch_sub_u32(ptr: *mut pg_atomic_uint32, sub_: u32) -> u32 {
    crate::port::atomics::generic::pg_atomic_fetch_sub_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        sub_ as i32,
    )
}
unsafe fn pg_atomic_sub_fetch_u32(ptr: *mut pg_atomic_uint32, sub_: u32) -> u32 {
    crate::port::atomics::generic::pg_atomic_sub_fetch_u32_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint32),
        sub_ as i32,
    )
}
unsafe fn pg_atomic_read_u64(ptr: *mut pg_atomic_uint64) -> u64 {
    crate::port::atomics::generic::pg_atomic_read_u64_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint64),
    )
}
unsafe fn pg_atomic_exchange_u64(ptr: *mut pg_atomic_uint64, newval: u64) -> u64 {
    crate::port::atomics::generic::pg_atomic_exchange_u64_impl(
        &*(ptr as *const crate::port::atomics::pg_atomic_uint64),
        newval,
    )
}
unsafe fn pg_write_barrier() {
    // TODO(pg-port): real pg_write_barrier lives in port/atomics.h
}

// init_local_spin_delay (storage/s_lock.h)
// TODO(pg-port): real init_local_spin_delay macro lives in storage/s_lock.h.
unsafe fn init_local_spin_delay(status: *mut SpinDelayStatus) {
    (*status).spins = 0;
    (*status).delays = 0;
    (*status).cur_delay = 0;
    (*status).file = b"lwlock.c\0".as_ptr() as *const c_char;
    (*status).line = 0;
    (*status).func = b"\0".as_ptr() as *const c_char;
}

// IsUnderPostmaster / process_shmem_requests_in_progress (miscadmin.h)
static mut IsUnderPostmaster: bool = false; // TODO(pg-port): miscadmin.h
static mut process_shmem_requests_in_progress: bool = false; // TODO(pg-port): miscadmin.h
static mut MyProcPid: c_int = 0; // TODO(pg-port): miscadmin.h

// MyProc and PGSemaphore* (storage/proc.h, storage/pg_sema.h)
// TODO(pg-port): real MyProc lives in storage/proc.h.
extern "C" { pub static mut MyProc: *mut PGPROC; }
unsafe fn PGSemaphoreLock(sema: PGSemaphore) {
    crate::storage::pg_sema::PGSemaphoreLock(sema as _)
}
unsafe fn PGSemaphoreUnlock(sema: PGSemaphore) {
    crate::storage::pg_sema::PGSemaphoreUnlock(sema as _)
}
type PGSemaphore = *mut c_void; // TODO(pg-port): real PGSemaphore lives in storage/pg_sema.h

// PGPROC field accessors --- PGPROC is an opaque stub from proclist.rs.
// The real fields (lwWaiting, lwWaitMode, sem) live in storage/proc.h.
// TODO(pg-port): real PGPROC fields live in storage/proc.h.
unsafe fn pgproc_lwWaiting(p: *mut PGPROC) -> LWLockWaitState {
    match (*(p as *mut crate::storage::lmgr::proc::PGPROC)).lwWaiting {
        1 => LW_WS_WAITING,
        2 => LW_WS_PENDING_WAKEUP,
        _ => LW_WS_NOT_WAITING,
    }
}
unsafe fn pgproc_set_lwWaiting(p: *mut PGPROC, v: LWLockWaitState) {
    (*(p as *mut crate::storage::lmgr::proc::PGPROC)).lwWaiting = v as u8;
}
unsafe fn pgproc_lwWaitMode(p: *mut PGPROC) -> LWLockMode {
    match (*(p as *mut crate::storage::lmgr::proc::PGPROC)).lwWaitMode {
        1 => LW_SHARED,
        2 => LW_WAIT_UNTIL_FREE,
        _ => LW_EXCLUSIVE,
    }
}
unsafe fn pgproc_set_lwWaitMode(p: *mut PGPROC, v: LWLockMode) {
    (*(p as *mut crate::storage::lmgr::proc::PGPROC)).lwWaitMode = v as u8;
}
unsafe fn pgproc_sem(p: *mut PGPROC) -> PGSemaphore {
    (*(p as *mut crate::storage::lmgr::proc::PGPROC)).sem as PGSemaphore
}

// ShmemAlloc (storage/ipc/shmem.c)
unsafe fn ShmemAlloc(size: Size) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemAlloc(size)
}

// add_size / mul_size (storage/shmem.h)
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2 // TODO(pg-port): real add_size (with overflow check) lives in storage/ipc/shmem.c
}
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    s1 * s2 // TODO(pg-port): real mul_size (with overflow check) lives in storage/ipc/shmem.c
}

// MemoryContextAllocZero family (utils/mmgr/mcxt.c) - palloc.h provides some;
// repalloc0_array is a macro in utils/palloc.h.
// TODO(pg-port): real repalloc0_array lives in utils/palloc.h.
unsafe fn repalloc0_array_charptr(
    ptr: *mut *const c_char,
    oldcount: c_int,
    newcount: c_int,
) -> *mut *const c_char {
    let elemsize = core::mem::size_of::<*const c_char>();
    crate::utils::palloc::repalloc0(
        ptr as *mut c_void,
        elemsize * oldcount as usize,
        elemsize * newcount as usize,
    ) as *mut *const c_char
}

// pgstat wait-event reporting (utils/activity/wait_event.c, pgstat.h)
const PG_WAIT_LWLOCK: u32 = 0x0B00_0000; // TODO(pg-port): real value lives in utils/wait_event.h
unsafe fn pgstat_report_wait_start(_wait_event_info: u32) {
    // TODO(pg-port): real pgstat_report_wait_start lives in pgstat.h
}
unsafe fn pgstat_report_wait_end() {
    // TODO(pg-port): real pgstat_report_wait_end lives in pgstat.h
}

// DTrace tracepoints (pg_trace.h) - compile to no-ops when not enabled.
unsafe fn TRACE_POSTGRESQL_LWLOCK_WAIT_START_ENABLED() -> bool {
    false // TODO(pg-port): real tracepoint lives in pg_trace.h
}
unsafe fn TRACE_POSTGRESQL_LWLOCK_WAIT_START(_name: *const c_char, _mode: LWLockMode) {}
unsafe fn TRACE_POSTGRESQL_LWLOCK_WAIT_DONE_ENABLED() -> bool {
    false // TODO(pg-port): real tracepoint lives in pg_trace.h
}
unsafe fn TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(_name: *const c_char, _mode: LWLockMode) {}
unsafe fn TRACE_POSTGRESQL_LWLOCK_ACQUIRE_ENABLED() -> bool {
    false // TODO(pg-port): real tracepoint lives in pg_trace.h
}
unsafe fn TRACE_POSTGRESQL_LWLOCK_ACQUIRE(_name: *const c_char, _mode: LWLockMode) {}
unsafe fn TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_ENABLED() -> bool {
    false // TODO(pg-port): real tracepoint lives in pg_trace.h
}
unsafe fn TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE(_name: *const c_char, _mode: LWLockMode) {}
unsafe fn TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL_ENABLED() -> bool {
    false // TODO(pg-port): real tracepoint lives in pg_trace.h
}
unsafe fn TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL(_name: *const c_char, _mode: LWLockMode) {}
unsafe fn TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT_ENABLED() -> bool {
    false // TODO(pg-port): real tracepoint lives in pg_trace.h
}
unsafe fn TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT(_name: *const c_char, _mode: LWLockMode) {}
unsafe fn TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT_FAIL_ENABLED() -> bool {
    false // TODO(pg-port): real tracepoint lives in pg_trace.h
}
unsafe fn TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT_FAIL(_name: *const c_char, _mode: LWLockMode) {}
unsafe fn TRACE_POSTGRESQL_LWLOCK_RELEASE_ENABLED() -> bool {
    false // TODO(pg-port): real tracepoint lives in pg_trace.h
}
unsafe fn TRACE_POSTGRESQL_LWLOCK_RELEASE(_name: *const c_char) {}

// =============================================================================
// #ifdef LWLOCK_STATS
//
// Debugging-only statistics gathering, compiled in only when the
// "lwlock_stats" feature is enabled (matching the C LWLOCK_STATS macro).
// =============================================================================

#[cfg(feature = "lwlock_stats")]
#[repr(C)]
#[derive(Clone, Copy)]
struct lwlock_stats_key {
    tranche: c_int,
    instance: *mut c_void,
}

#[cfg(feature = "lwlock_stats")]
#[repr(C)]
#[derive(Clone, Copy)]
struct lwlock_stats {
    key: lwlock_stats_key,
    sh_acquire_count: c_int,
    ex_acquire_count: c_int,
    block_count: c_int,
    dequeue_self_count: c_int,
    spin_delay_count: c_int,
}

#[cfg(feature = "lwlock_stats")]
static mut lwlock_stats_htab: *mut crate::utils::hash::dynahash::HTAB = std::ptr::null_mut();
#[cfg(feature = "lwlock_stats")]
static mut lwlock_stats_dummy: lwlock_stats = lwlock_stats {
    key: lwlock_stats_key {
        tranche: 0,
        instance: std::ptr::null_mut(),
    },
    sh_acquire_count: 0,
    ex_acquire_count: 0,
    block_count: 0,
    dequeue_self_count: 0,
    spin_delay_count: 0,
};

#[cfg(feature = "lwlock_stats")]
unsafe fn init_lwlock_stats() {
    use crate::utils::hash::dynahash::{
        hash_create, HASHCTL, HASH_BLOBS, HASH_CONTEXT, HASH_ELEM,
    };
    use crate::utils::mmgr::aset::AllocSetContextCreate;
    use crate::utils::mmgr::mcxt::{MemoryContextAllowInCriticalSection, MemoryContextDelete};
    use crate::storage::ipc::ipc::on_shmem_exit;

    let mut ctl: HASHCTL = core::mem::zeroed();
    static mut lwlock_stats_cxt: crate::utils::mmgr::memnodes::MemoryContext = std::ptr::null_mut();
    static mut exit_registered: bool = false;

    if !lwlock_stats_cxt.is_null() {
        MemoryContextDelete(lwlock_stats_cxt);
    }

    /*
     * The LWLock stats will be updated within a critical section, which
     * requires allocating new hash entries. Allocations within a critical
     * section are normally not allowed because running out of memory would
     * lead to a PANIC, but LWLOCK_STATS is debugging code that's not normally
     * turned on in production, so that's an acceptable risk. The hash entries
     * are small, so the risk of running out of memory is minimal in practice.
     */
    lwlock_stats_cxt = AllocSetContextCreate(
        TopMemoryContext,
        c"LWLock stats".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );
    MemoryContextAllowInCriticalSection(lwlock_stats_cxt, true);

    ctl.keysize = core::mem::size_of::<lwlock_stats_key>() as Size;
    ctl.entrysize = core::mem::size_of::<lwlock_stats>() as Size;
    ctl.hcxt = lwlock_stats_cxt;
    lwlock_stats_htab = hash_create(
        c"lwlock stats".as_ptr(),
        16384,
        &ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );
    if !exit_registered {
        on_shmem_exit(print_lwlock_stats, 0);
        exit_registered = true;
    }
}

#[cfg(feature = "lwlock_stats")]
unsafe extern "C" fn print_lwlock_stats(_code: c_int, _arg: Datum) {
    use crate::utils::hash::dynahash::{hash_seq_init, hash_seq_search, HASH_SEQ_STATUS};

    let mut scan: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut lwstats: *mut lwlock_stats;

    hash_seq_init(&mut scan, lwlock_stats_htab);

    /* Grab an LWLock to keep different backends from mixing reports */
    LWLockAcquire(&raw mut *(*MainLWLockArray).lock as *mut LWLock, LW_EXCLUSIVE);

    loop {
        lwstats = hash_seq_search(&mut scan) as *mut lwlock_stats;
        if lwstats.is_null() {
            break;
        }
        eprintln!(
            "PID {} lwlock {} {:p}: shacq {} exacq {} blk {} spindelay {} dequeue self {}",
            MyProcPid,
            CStr::from_ptr(GetLWTrancheName((*lwstats).key.tranche as u16)).to_string_lossy(),
            (*lwstats).key.instance,
            (*lwstats).sh_acquire_count,
            (*lwstats).ex_acquire_count,
            (*lwstats).block_count,
            (*lwstats).spin_delay_count,
            (*lwstats).dequeue_self_count
        );
    }

    LWLockRelease(&raw mut *(*MainLWLockArray).lock as *mut LWLock);
}

#[cfg(feature = "lwlock_stats")]
unsafe fn get_lwlock_stats_entry(lock: *mut LWLock) -> *mut lwlock_stats {
    use crate::utils::hash::dynahash::{hash_search, HASH_ENTER};

    let mut key: lwlock_stats_key;
    let lwstats: *mut lwlock_stats;
    let mut found: bool = false;

    /*
     * During shared memory initialization, the hash table doesn't exist yet.
     * Stats of that phase aren't very interesting, so just collect operations
     * on all locks in a single dummy entry.
     */
    if lwlock_stats_htab.is_null() {
        return &raw mut lwlock_stats_dummy;
    }

    /* Fetch or create the entry. */
    key = core::mem::zeroed();
    key.tranche = (*lock).tranche as c_int;
    key.instance = lock as *mut c_void;
    lwstats = hash_search(
        lwlock_stats_htab,
        &key as *const lwlock_stats_key as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut lwlock_stats;
    if !found {
        (*lwstats).sh_acquire_count = 0;
        (*lwstats).ex_acquire_count = 0;
        (*lwstats).block_count = 0;
        (*lwstats).dequeue_self_count = 0;
        (*lwstats).spin_delay_count = 0;
    }
    lwstats
}

// =============================================================================
// Declarations from storage/lwlock.h
// =============================================================================

/// what state of the wait process is a backend in
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LWLockWaitState {
    LW_WS_NOT_WAITING,    // not currently waiting / woken up
    LW_WS_WAITING,        // currently waiting
    LW_WS_PENDING_WAKEUP, // removed from waitlist, but not yet signalled
}
use LWLockWaitState::*;

/// Code outside of lwlock.c should not manipulate the contents of this
/// structure directly, but we have to declare it here to allow LWLocks to be
/// incorporated into other data structures.
#[repr(C)]
pub struct LWLock {
    pub tranche: u16,             // tranche ID
    pub state: pg_atomic_uint32,  // state of exclusive/nonexclusive lockers
    pub waiters: proclist_head,   // list of waiting PGPROCs
    // #ifdef LOCK_DEBUG: nwaiters, owner --- omitted (LOCK_DEBUG off)
}

/// In most cases, it's desirable to force each tranche of LWLocks to be aligned
/// on a cache line boundary and make the array stride a power of 2.
pub const LWLOCK_PADDED_SIZE: usize = PG_CACHE_LINE_SIZE;

/// LWLock, padded to a full cache line size
#[repr(C)]
pub union LWLockPadded {
    pub lock: std::mem::ManuallyDrop<LWLock>,
    pub pad: [c_char; LWLOCK_PADDED_SIZE],
}

/// extern PGDLLIMPORT LWLockPadded *MainLWLockArray;
#[no_mangle]
pub static mut MainLWLockArray: *mut LWLockPadded = std::ptr::null_mut();

/// struct for storing named tranche information
#[repr(C)]
pub struct NamedLWLockTranche {
    pub trancheId: c_int,
    pub trancheName: *mut c_char,
}

pub static mut NamedLWLockTrancheArray: *mut NamedLWLockTranche = std::ptr::null_mut();
pub static mut NamedLWLockTrancheRequests: c_int = 0;

/* Number of partitions of the shared buffer mapping hashtable */
pub const NUM_BUFFER_PARTITIONS: c_int = 128;

/* Number of partitions the shared lock tables are divided into */
pub const LOG2_NUM_LOCK_PARTITIONS: c_int = 4;
pub const NUM_LOCK_PARTITIONS: c_int = 1 << LOG2_NUM_LOCK_PARTITIONS;

/* Number of partitions the shared predicate lock tables are divided into */
pub const LOG2_NUM_PREDICATELOCK_PARTITIONS: c_int = 4;
pub const NUM_PREDICATELOCK_PARTITIONS: c_int = 1 << LOG2_NUM_PREDICATELOCK_PARTITIONS;

/* Offsets for various chunks of preallocated lwlocks. */
pub const BUFFER_MAPPING_LWLOCK_OFFSET: c_int = NUM_INDIVIDUAL_LWLOCKS;
pub const LOCK_MANAGER_LWLOCK_OFFSET: c_int = BUFFER_MAPPING_LWLOCK_OFFSET + NUM_BUFFER_PARTITIONS;
pub const PREDICATELOCK_MANAGER_LWLOCK_OFFSET: c_int =
    LOCK_MANAGER_LWLOCK_OFFSET + NUM_LOCK_PARTITIONS;
pub const NUM_FIXED_LWLOCKS: c_int =
    PREDICATELOCK_MANAGER_LWLOCK_OFFSET + NUM_PREDICATELOCK_PARTITIONS;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum LWLockMode {
    LW_EXCLUSIVE,
    LW_SHARED,
    // A special mode used in PGPROC->lwWaitMode, when waiting for lock to
    // become free.  Not to be used as LWLockAcquire argument.
    LW_WAIT_UNTIL_FREE,
}
use LWLockMode::*;

/// Every tranche ID less than NUM_INDIVIDUAL_LWLOCKS is reserved; also,
/// we reserve additional tranche IDs for builtin tranches not included in
/// the set of individual LWLocks.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BuiltinTrancheIds {
    LWTRANCHE_XACT_BUFFER = NUM_INDIVIDUAL_LWLOCKS as isize,
    LWTRANCHE_COMMITTS_BUFFER,
    LWTRANCHE_SUBTRANS_BUFFER,
    LWTRANCHE_MULTIXACTOFFSET_BUFFER,
    LWTRANCHE_MULTIXACTMEMBER_BUFFER,
    LWTRANCHE_NOTIFY_BUFFER,
    LWTRANCHE_SERIAL_BUFFER,
    LWTRANCHE_WAL_INSERT,
    LWTRANCHE_BUFFER_CONTENT,
    LWTRANCHE_REPLICATION_ORIGIN_STATE,
    LWTRANCHE_REPLICATION_SLOT_IO,
    LWTRANCHE_LOCK_FASTPATH,
    LWTRANCHE_BUFFER_MAPPING,
    LWTRANCHE_LOCK_MANAGER,
    LWTRANCHE_PREDICATE_LOCK_MANAGER,
    LWTRANCHE_PARALLEL_HASH_JOIN,
    LWTRANCHE_PARALLEL_BTREE_SCAN,
    LWTRANCHE_PARALLEL_QUERY_DSA,
    LWTRANCHE_PER_SESSION_DSA,
    LWTRANCHE_PER_SESSION_RECORD_TYPE,
    LWTRANCHE_PER_SESSION_RECORD_TYPMOD,
    LWTRANCHE_SHARED_TUPLESTORE,
    LWTRANCHE_SHARED_TIDBITMAP,
    LWTRANCHE_PARALLEL_APPEND,
    LWTRANCHE_PER_XACT_PREDICATE_LIST,
    LWTRANCHE_PGSTATS_DSA,
    LWTRANCHE_PGSTATS_HASH,
    LWTRANCHE_PGSTATS_DATA,
    LWTRANCHE_LAUNCHER_DSA,
    LWTRANCHE_LAUNCHER_HASH,
    LWTRANCHE_DSM_REGISTRY_DSA,
    LWTRANCHE_DSM_REGISTRY_HASH,
    LWTRANCHE_COMMITTS_SLRU,
    LWTRANCHE_MULTIXACTMEMBER_SLRU,
    LWTRANCHE_MULTIXACTOFFSET_SLRU,
    LWTRANCHE_NOTIFY_SLRU,
    LWTRANCHE_SERIAL_SLRU,
    LWTRANCHE_SUBTRANS_SLRU,
    LWTRANCHE_XACT_SLRU,
    LWTRANCHE_PARALLEL_VACUUM_DSA,
    LWTRANCHE_AIO_URING_COMPLETION,
    LWTRANCHE_FIRST_USER_DEFINED,
}
use BuiltinTrancheIds::*;

// =============================================================================
// lwlock.c
// =============================================================================

pub const LW_FLAG_HAS_WAITERS: u32 = 1u32 << 31;
pub const LW_FLAG_RELEASE_OK: u32 = 1u32 << 30;
pub const LW_FLAG_LOCKED: u32 = 1u32 << 29;
pub const LW_FLAG_BITS: u32 = 3;
pub const LW_FLAG_MASK: u32 = ((1 << LW_FLAG_BITS) - 1) << (32 - LW_FLAG_BITS);

/* assumes MAX_BACKENDS is a (power of 2) - 1, checked below */
pub const LW_VAL_EXCLUSIVE: u32 = MAX_BACKENDS + 1;
pub const LW_VAL_SHARED: u32 = 1;

/* already (power of 2)-1, i.e. suitable for a mask */
pub const LW_SHARED_MASK: u32 = MAX_BACKENDS;
pub const LW_LOCK_MASK: u32 = MAX_BACKENDS | LW_VAL_EXCLUSIVE;

// StaticAssertDecl(((MAX_BACKENDS + 1) & MAX_BACKENDS) == 0, ...)
// StaticAssertDecl((MAX_BACKENDS & LW_FLAG_MASK) == 0, ...)
// StaticAssertDecl((LW_VAL_EXCLUSIVE & LW_FLAG_MASK) == 0, ...)
const _: () = {
    assert!(((MAX_BACKENDS + 1) & MAX_BACKENDS) == 0);
    assert!((MAX_BACKENDS & LW_FLAG_MASK) == 0);
    assert!((LW_VAL_EXCLUSIVE & LW_FLAG_MASK) == 0);
};

/// There are three sorts of LWLock "tranches" --- see the C comment.  These are
/// the names of the built-in tranches.  The array is indexed by tranche ID.
//
// C also: the individually-named locks come from "storage/lwlocklist.h" via the
// PG_LWLOCK(id, lockname) macro; those entries are elided here pending the
// generated lwlocknames table.
static BuiltinTrancheNames: &[(BuiltinTrancheIds, &str)] = &[
    (LWTRANCHE_XACT_BUFFER, "XactBuffer"),
    (LWTRANCHE_COMMITTS_BUFFER, "CommitTsBuffer"),
    (LWTRANCHE_SUBTRANS_BUFFER, "SubtransBuffer"),
    (LWTRANCHE_MULTIXACTOFFSET_BUFFER, "MultiXactOffsetBuffer"),
    (LWTRANCHE_MULTIXACTMEMBER_BUFFER, "MultiXactMemberBuffer"),
    (LWTRANCHE_NOTIFY_BUFFER, "NotifyBuffer"),
    (LWTRANCHE_SERIAL_BUFFER, "SerialBuffer"),
    (LWTRANCHE_WAL_INSERT, "WALInsert"),
    (LWTRANCHE_BUFFER_CONTENT, "BufferContent"),
    (LWTRANCHE_REPLICATION_ORIGIN_STATE, "ReplicationOriginState"),
    (LWTRANCHE_REPLICATION_SLOT_IO, "ReplicationSlotIO"),
    (LWTRANCHE_LOCK_FASTPATH, "LockFastPath"),
    (LWTRANCHE_BUFFER_MAPPING, "BufferMapping"),
    (LWTRANCHE_LOCK_MANAGER, "LockManager"),
    (LWTRANCHE_PREDICATE_LOCK_MANAGER, "PredicateLockManager"),
    (LWTRANCHE_PARALLEL_HASH_JOIN, "ParallelHashJoin"),
    (LWTRANCHE_PARALLEL_BTREE_SCAN, "ParallelBtreeScan"),
    (LWTRANCHE_PARALLEL_QUERY_DSA, "ParallelQueryDSA"),
    (LWTRANCHE_PER_SESSION_DSA, "PerSessionDSA"),
    (LWTRANCHE_PER_SESSION_RECORD_TYPE, "PerSessionRecordType"),
    (LWTRANCHE_PER_SESSION_RECORD_TYPMOD, "PerSessionRecordTypmod"),
    (LWTRANCHE_SHARED_TUPLESTORE, "SharedTupleStore"),
    (LWTRANCHE_SHARED_TIDBITMAP, "SharedTidBitmap"),
    (LWTRANCHE_PARALLEL_APPEND, "ParallelAppend"),
    (LWTRANCHE_PER_XACT_PREDICATE_LIST, "PerXactPredicateList"),
    (LWTRANCHE_PGSTATS_DSA, "PgStatsDSA"),
    (LWTRANCHE_PGSTATS_HASH, "PgStatsHash"),
    (LWTRANCHE_PGSTATS_DATA, "PgStatsData"),
    (LWTRANCHE_LAUNCHER_DSA, "LogicalRepLauncherDSA"),
    (LWTRANCHE_LAUNCHER_HASH, "LogicalRepLauncherHash"),
    (LWTRANCHE_DSM_REGISTRY_DSA, "DSMRegistryDSA"),
    (LWTRANCHE_DSM_REGISTRY_HASH, "DSMRegistryHash"),
    (LWTRANCHE_COMMITTS_SLRU, "CommitTsSLRU"),
    (LWTRANCHE_MULTIXACTOFFSET_SLRU, "MultiXactOffsetSLRU"),
    (LWTRANCHE_MULTIXACTMEMBER_SLRU, "MultiXactMemberSLRU"),
    (LWTRANCHE_NOTIFY_SLRU, "NotifySLRU"),
    (LWTRANCHE_SERIAL_SLRU, "SerialSLRU"),
    (LWTRANCHE_SUBTRANS_SLRU, "SubtransSLRU"),
    (LWTRANCHE_XACT_SLRU, "XactSLRU"),
    (LWTRANCHE_PARALLEL_VACUUM_DSA, "ParallelVacuumDSA"),
    (LWTRANCHE_AIO_URING_COMPLETION, "AioUringCompletion"),
];

/// This is indexed by tranche ID minus LWTRANCHE_FIRST_USER_DEFINED, and
/// stores the names of all dynamically-created tranches known to the current
/// process.  Any unused entries in the array will contain NULL.
static mut LWLockTrancheNames: *mut *const c_char = std::ptr::null_mut();
static mut LWLockTrancheNamesAllocated: c_int = 0;

/// We use this structure to keep track of locked LWLocks for release
/// during error recovery.  Normally, only a few will be held at once, but
/// occasionally the number can be much higher; for example, the pg_buffercache
/// extension locks all buffer partitions simultaneously.
const MAX_SIMUL_LWLOCKS: usize = 200;

/// struct representing the LWLocks we're holding
#[derive(Clone, Copy)]
struct LWLockHandle {
    lock: *mut LWLock,
    mode: LWLockMode,
}

static mut num_held_lwlocks: c_int = 0;
static mut held_lwlocks: [LWLockHandle; MAX_SIMUL_LWLOCKS] = [LWLockHandle {
    lock: std::ptr::null_mut(),
    mode: LW_EXCLUSIVE,
}; MAX_SIMUL_LWLOCKS];

/// struct representing the LWLock tranche request for named tranche
#[repr(C)]
#[derive(Clone, Copy)]
struct NamedLWLockTrancheRequest {
    tranche_name: [c_char; NAMEDATALEN],
    num_lwlocks: c_int,
}

static mut NamedLWLockTrancheRequestArray: *mut NamedLWLockTrancheRequest = std::ptr::null_mut();
static mut NamedLWLockTrancheRequestsAllocated: c_int = 0;

// T_NAME(lock) -> GetLWTrancheName((lock)->tranche)
unsafe fn T_NAME(lock: *mut LWLock) -> *const c_char {
    GetLWTrancheName((*lock).tranche)
}

// PRINT_LWDEBUG / LOG_LWDEBUG are no-ops when LOCK_DEBUG is not defined.
#[inline]
unsafe fn PRINT_LWDEBUG(_where_: &str, _lock: *mut LWLock, _mode: LWLockMode) {}
#[inline]
unsafe fn LOG_LWDEBUG(_where_: &str, _lock: *mut LWLock, _msg: &str) {}

/// Compute number of LWLocks required by named tranches.  These will be
/// allocated in the main array.
unsafe fn NumLWLocksForNamedTranches() -> c_int {
    let mut numLocks: c_int = 0;
    let mut i: c_int;

    i = 0;
    while i < NamedLWLockTrancheRequests {
        numLocks += (*NamedLWLockTrancheRequestArray.add(i as usize)).num_lwlocks;
        i += 1;
    }

    numLocks
}

/// Compute shmem space needed for LWLocks and named tranches.
pub unsafe fn LWLockShmemSize() -> Size {
    let mut size: Size;
    let mut i: c_int;
    let mut numLocks: c_int = NUM_FIXED_LWLOCKS;

    /* Calculate total number of locks needed in the main array. */
    numLocks += NumLWLocksForNamedTranches();

    /* Space for the LWLock array. */
    size = mul_size(numLocks as Size, core::mem::size_of::<LWLockPadded>());

    /* Space for dynamic allocation counter, plus room for alignment. */
    size = add_size(
        size,
        core::mem::size_of::<c_int>() + LWLOCK_PADDED_SIZE,
    );

    /* space for named tranches. */
    size = add_size(
        size,
        mul_size(
            NamedLWLockTrancheRequests as Size,
            core::mem::size_of::<NamedLWLockTranche>(),
        ),
    );

    /* space for name of each tranche. */
    i = 0;
    while i < NamedLWLockTrancheRequests {
        let name = (*NamedLWLockTrancheRequestArray.add(i as usize)).tranche_name.as_ptr();
        size = add_size(size, CStr::from_ptr(name).to_bytes().len() + 1);
        i += 1;
    }

    size
}

/// Point the builtin (individual) LWLock name globals at their slots in
/// `MainLWLockArray`.  In C each `<Name>Lock` is the macro
/// `(&MainLWLockArray[<Name>_LWLOCK_ID].lock)`, resolved at compile time.
/// Here the globals are runtime pointers that must be assigned once the array
/// exists, i.e. after `InitializeLWLocks` in the postmaster.  Only globals with
/// a single canonical (`#[no_mangle]`) definition are assigned; the rest are
/// still module-private stubs with no shared symbol to point here.
pub unsafe fn InitializeBuiltinLWLockPointers() {
    macro_rules! assign {
        ($global:ident, $id:ident) => {
            crate::backend_link_shims::$global = &raw mut (*MainLWLockArray
                .add(crate::storage::lwlocklist::$id as usize))
                .lock as *mut _ as *mut c_void;
        };
    }
    assign!(ShmemIndexLock, ShmemIndex_LWLOCK_ID);
    assign!(OidGenLock, OidGen_LWLOCK_ID);
    assign!(XidGenLock, XidGen_LWLOCK_ID);
    assign!(ProcArrayLock, ProcArray_LWLOCK_ID);
    assign!(SInvalReadLock, SInvalRead_LWLOCK_ID);
    assign!(SInvalWriteLock, SInvalWrite_LWLOCK_ID);
    assign!(WALBufMappingLock, WALBufMapping_LWLOCK_ID);
    assign!(WALWriteLock, WALWrite_LWLOCK_ID);
    assign!(ControlFileLock, ControlFile_LWLOCK_ID);
    assign!(MultiXactGenLock, MultiXactGen_LWLOCK_ID);
    assign!(RelCacheInitLock, RelCacheInit_LWLOCK_ID);
    assign!(CheckpointerCommLock, CheckpointerComm_LWLOCK_ID);
    assign!(TwoPhaseStateLock, TwoPhaseState_LWLOCK_ID);
    assign!(TablespaceCreateLock, TablespaceCreate_LWLOCK_ID);
    assign!(BtreeVacuumLock, BtreeVacuum_LWLOCK_ID);
    assign!(AddinShmemInitLock, AddinShmemInit_LWLOCK_ID);
    assign!(AutovacuumLock, Autovacuum_LWLOCK_ID);
    assign!(AutovacuumScheduleLock, AutovacuumSchedule_LWLOCK_ID);
    assign!(SyncScanLock, SyncScan_LWLOCK_ID);
    assign!(RelationMappingLock, RelationMapping_LWLOCK_ID);
    assign!(NotifyQueueLock, NotifyQueue_LWLOCK_ID);
    assign!(SerializableXactHashLock, SerializableXactHash_LWLOCK_ID);
    assign!(SerializableFinishedListLock, SerializableFinishedList_LWLOCK_ID);
    assign!(SerializablePredicateListLock, SerializablePredicateList_LWLOCK_ID);
    assign!(SyncRepLock, SyncRep_LWLOCK_ID);
    assign!(BackgroundWorkerLock, BackgroundWorker_LWLOCK_ID);
    assign!(DynamicSharedMemoryControlLock, DynamicSharedMemoryControl_LWLOCK_ID);
    assign!(AutoFileLock, AutoFile_LWLOCK_ID);
    assign!(ReplicationSlotAllocationLock, ReplicationSlotAllocation_LWLOCK_ID);
    assign!(ReplicationSlotControlLock, ReplicationSlotControl_LWLOCK_ID);
    assign!(CommitTsLock, CommitTs_LWLOCK_ID);
    assign!(ReplicationOriginLock, ReplicationOrigin_LWLOCK_ID);
    assign!(MultiXactTruncationLock, MultiXactTruncation_LWLOCK_ID);
    assign!(LogicalRepWorkerLock, LogicalRepWorker_LWLOCK_ID);
    assign!(XactTruncationLock, XactTruncation_LWLOCK_ID);
    assign!(WrapLimitsVacuumLock, WrapLimitsVacuum_LWLOCK_ID);
    assign!(NotifyQueueTailLock, NotifyQueueTail_LWLOCK_ID);
    assign!(WaitEventCustomLock, WaitEventCustom_LWLOCK_ID);
    assign!(WALSummarizerLock, WALSummarizer_LWLOCK_ID);
    assign!(DSMRegistryLock, DSMRegistry_LWLOCK_ID);
    assign!(InjectionPointLock, InjectionPoint_LWLOCK_ID);
    assign!(SerialControlLock, SerialControl_LWLOCK_ID);
    assign!(AioWorkerSubmissionQueueLock, AioWorkerSubmissionQueue_LWLOCK_ID);
}

/// Allocate shmem space for the main LWLock array and all tranches and
/// initialize it.  We also register extension LWLock tranches here.
pub unsafe fn CreateLWLocks() {
    if !IsUnderPostmaster {
        let spaceLocks: Size = LWLockShmemSize();
        let LWLockCounter: *mut c_int;
        let mut ptr: *mut c_char;

        /* Allocate space */
        ptr = ShmemAlloc(spaceLocks) as *mut c_char;

        /* Leave room for dynamic allocation of tranches */
        ptr = ptr.add(core::mem::size_of::<c_int>());

        /* Ensure desired alignment of LWLock array */
        ptr = ptr.add(LWLOCK_PADDED_SIZE - (ptr as usize) % LWLOCK_PADDED_SIZE);

        MainLWLockArray = ptr as *mut LWLockPadded;

        /*
         * Initialize the dynamic-allocation counter for tranches, which is
         * stored just before the first LWLock.
         */
        LWLockCounter = (MainLWLockArray as *mut c_char).sub(core::mem::size_of::<c_int>())
            as *mut c_int;
        *LWLockCounter = LWTRANCHE_FIRST_USER_DEFINED as c_int;

        /* Initialize all LWLocks */
        InitializeLWLocks();

        /* Point builtin LWLock name globals at their slots in the array */
        InitializeBuiltinLWLockPointers();
    }

    /* Register named extension LWLock tranches in the current process. */
    let mut i: c_int = 0;
    while i < NamedLWLockTrancheRequests {
        let tranche = &*NamedLWLockTrancheArray.add(i as usize);
        LWLockRegisterTranche(tranche.trancheId, tranche.trancheName);
        i += 1;
    }
}

/// Initialize LWLocks that are fixed and those belonging to named tranches.
unsafe fn InitializeLWLocks() {
    let numNamedLocks: c_int = NumLWLocksForNamedTranches();
    let mut id: c_int;
    let mut i: c_int;
    let mut j: c_int;
    let mut lock: *mut LWLockPadded;

    /* Initialize all individual LWLocks in main array */
    id = 0;
    lock = MainLWLockArray;
    while id < NUM_INDIVIDUAL_LWLOCKS {
        LWLockInitialize(&mut *(*lock).lock, id);
        id += 1;
        lock = lock.add(1);
    }

    /* Initialize buffer mapping LWLocks in main array */
    lock = MainLWLockArray.add(BUFFER_MAPPING_LWLOCK_OFFSET as usize);
    id = 0;
    while id < NUM_BUFFER_PARTITIONS {
        LWLockInitialize(&mut *(*lock).lock, LWTRANCHE_BUFFER_MAPPING as c_int);
        id += 1;
        lock = lock.add(1);
    }

    /* Initialize lmgrs' LWLocks in main array */
    lock = MainLWLockArray.add(LOCK_MANAGER_LWLOCK_OFFSET as usize);
    id = 0;
    while id < NUM_LOCK_PARTITIONS {
        LWLockInitialize(&mut *(*lock).lock, LWTRANCHE_LOCK_MANAGER as c_int);
        id += 1;
        lock = lock.add(1);
    }

    /* Initialize predicate lmgrs' LWLocks in main array */
    lock = MainLWLockArray.add(PREDICATELOCK_MANAGER_LWLOCK_OFFSET as usize);
    id = 0;
    while id < NUM_PREDICATELOCK_PARTITIONS {
        LWLockInitialize(&mut *(*lock).lock, LWTRANCHE_PREDICATE_LOCK_MANAGER as c_int);
        id += 1;
        lock = lock.add(1);
    }

    /*
     * Copy the info about any named tranches into shared memory (so that
     * other processes can see it), and initialize the requested LWLocks.
     */
    if NamedLWLockTrancheRequests > 0 {
        let mut trancheNames: *mut c_char;

        NamedLWLockTrancheArray = (&mut *MainLWLockArray
            .add((NUM_FIXED_LWLOCKS + numNamedLocks) as usize))
            as *mut LWLockPadded as *mut NamedLWLockTranche;

        trancheNames = (NamedLWLockTrancheArray as *mut c_char).add(
            (NamedLWLockTrancheRequests as usize) * core::mem::size_of::<NamedLWLockTranche>(),
        );
        lock = MainLWLockArray.add(NUM_FIXED_LWLOCKS as usize);

        i = 0;
        while i < NamedLWLockTrancheRequests {
            let request: *mut NamedLWLockTrancheRequest =
                &mut *NamedLWLockTrancheRequestArray.add(i as usize);
            let tranche: *mut NamedLWLockTranche =
                &mut *NamedLWLockTrancheArray.add(i as usize);
            let name: *mut c_char;

            name = trancheNames;
            trancheNames = trancheNames
                .add(CStr::from_ptr((*request).tranche_name.as_ptr()).to_bytes().len() + 1);
            libc::strcpy(name, (*request).tranche_name.as_ptr());
            (*tranche).trancheId = LWLockNewTrancheId();
            (*tranche).trancheName = name;

            j = 0;
            while j < (*request).num_lwlocks {
                LWLockInitialize(&mut *(*lock).lock, (*tranche).trancheId);
                j += 1;
                lock = lock.add(1);
            }
            i += 1;
        }
    }
}

/// InitLWLockAccess - initialize backend-local state needed to hold LWLocks
pub unsafe fn InitLWLockAccess() {
    // #ifdef LWLOCK_STATS: init_lwlock_stats() --- omitted (LWLOCK_STATS off)
}

/// GetNamedLWLockTranche - returns the base address of LWLock from the
///     specified tranche.
pub unsafe fn GetNamedLWLockTranche(tranche_name: *const c_char) -> *mut LWLockPadded {
    let mut lock_pos: c_int;
    let mut i: c_int;

    /*
     * Obtain the position of base address of LWLock belonging to requested
     * tranche_name in MainLWLockArray.  LWLocks for named tranches are placed
     * in MainLWLockArray after fixed locks.
     */
    lock_pos = NUM_FIXED_LWLOCKS;
    i = 0;
    while i < NamedLWLockTrancheRequests {
        if libc::strcmp(
            (*NamedLWLockTrancheRequestArray.add(i as usize)).tranche_name.as_ptr(),
            tranche_name,
        ) == 0
        {
            return MainLWLockArray.add(lock_pos as usize);
        }

        lock_pos += (*NamedLWLockTrancheRequestArray.add(i as usize)).num_lwlocks;
        i += 1;
    }

    elog!(ERROR, "requested tranche is not registered");

    /* just to keep compiler quiet */
    #[allow(unreachable_code)]
    std::ptr::null_mut()
}

/// Allocate a new tranche ID.
pub unsafe fn LWLockNewTrancheId() -> c_int {
    let result: c_int;
    let LWLockCounter: *mut c_int;

    LWLockCounter =
        (MainLWLockArray as *mut c_char).sub(core::mem::size_of::<c_int>()) as *mut c_int;
    /* We use the ShmemLock spinlock to protect LWLockCounter */
    SpinLockAcquire(ShmemLock);
    result = *LWLockCounter;
    *LWLockCounter += 1;
    SpinLockRelease(ShmemLock);

    result
}

/// Register a dynamic tranche name in the lookup table of the current process.
pub unsafe fn LWLockRegisterTranche(mut tranche_id: c_int, tranche_name: *const c_char) {
    /* This should only be called for user-defined tranches. */
    if tranche_id < LWTRANCHE_FIRST_USER_DEFINED as c_int {
        return;
    }

    /* Convert to array index. */
    tranche_id -= LWTRANCHE_FIRST_USER_DEFINED as c_int;

    /* If necessary, create or enlarge array. */
    if tranche_id >= LWLockTrancheNamesAllocated {
        let newalloc: c_int;

        newalloc = pg_nextpower2_32(Max(8, tranche_id + 1) as u32) as c_int;

        if LWLockTrancheNames.is_null() {
            LWLockTrancheNames = MemoryContextAllocZero(
                TopMemoryContext,
                (newalloc as usize) * core::mem::size_of::<*mut c_char>(),
            ) as *mut *const c_char;
        } else {
            LWLockTrancheNames = repalloc0_array_charptr(
                LWLockTrancheNames,
                LWLockTrancheNamesAllocated,
                newalloc,
            );
        }
        LWLockTrancheNamesAllocated = newalloc;
    }

    *LWLockTrancheNames.add(tranche_id as usize) = tranche_name;
}

/// Max(a, b) helper for the C Max() macro used above.
#[inline]
fn Max(a: c_int, b: c_int) -> c_int {
    if a > b {
        a
    } else {
        b
    }
}

/// RequestNamedLWLockTranche
///     Request that extra LWLocks be allocated during postmaster startup.
pub unsafe fn RequestNamedLWLockTranche(tranche_name: *const c_char, num_lwlocks: c_int) {
    let request: *mut NamedLWLockTrancheRequest;

    if !process_shmem_requests_in_progress {
        elog!(
            FATAL,
            "cannot request additional LWLocks outside shmem_request_hook"
        );
    }

    if NamedLWLockTrancheRequestArray.is_null() {
        NamedLWLockTrancheRequestsAllocated = 16;
        NamedLWLockTrancheRequestArray = MemoryContextAlloc(
            TopMemoryContext,
            (NamedLWLockTrancheRequestsAllocated as usize)
                * core::mem::size_of::<NamedLWLockTrancheRequest>(),
        ) as *mut NamedLWLockTrancheRequest;
    }

    if NamedLWLockTrancheRequests >= NamedLWLockTrancheRequestsAllocated {
        let i: c_int = pg_nextpower2_32((NamedLWLockTrancheRequests + 1) as u32) as c_int;

        NamedLWLockTrancheRequestArray = repalloc(
            NamedLWLockTrancheRequestArray as *mut c_void,
            (i as usize) * core::mem::size_of::<NamedLWLockTrancheRequest>(),
        ) as *mut NamedLWLockTrancheRequest;
        NamedLWLockTrancheRequestsAllocated = i;
    }

    request = &mut *NamedLWLockTrancheRequestArray.add(NamedLWLockTrancheRequests as usize);
    Assert!(CStr::from_ptr(tranche_name).to_bytes().len() + 1 <= NAMEDATALEN);
    strlcpy((*request).tranche_name.as_mut_ptr(), tranche_name, NAMEDATALEN);
    (*request).num_lwlocks = num_lwlocks;
    NamedLWLockTrancheRequests += 1;
}

/// LWLockInitialize - initialize a new lwlock; it's initially unlocked
#[no_mangle]
pub unsafe fn LWLockInitialize(lock: *mut LWLock, tranche_id: c_int) {
    pg_atomic_init_u32(&mut (*lock).state, LW_FLAG_RELEASE_OK);
    // #ifdef LOCK_DEBUG: pg_atomic_init_u32(&lock->nwaiters, 0) --- omitted
    (*lock).tranche = tranche_id as u16;
    proclist_init(&mut (*lock).waiters);
}

// strlcpy (port/strlcpy.c)
unsafe fn strlcpy(dst: *mut c_char, src: *const c_char, siz: usize) -> usize {
    crate::port::strlcpy::strlcpy(dst, src, siz)
}

/// Report start of wait event for light-weight locks.
#[inline]
unsafe fn LWLockReportWaitStart(lock: *mut LWLock) {
    pgstat_report_wait_start(PG_WAIT_LWLOCK | (*lock).tranche as u32);
}

/// Report end of wait event for light-weight locks.
#[inline]
unsafe fn LWLockReportWaitEnd() {
    pgstat_report_wait_end();
}

/// Return the name of an LWLock tranche.
unsafe fn GetLWTrancheName(mut trancheId: u16) -> *const c_char {
    /* Built-in tranche or individual LWLock? */
    if (trancheId as c_int) < LWTRANCHE_FIRST_USER_DEFINED as c_int {
        // C also: indexed lookup in BuiltinTrancheNames[trancheId].  The
        // individual-LWLock names (ids < LWTRANCHE_XACT_BUFFER) come from the
        // generated lwlocknames.h and are not yet available here.
        let first = LWTRANCHE_XACT_BUFFER as c_int;
        if (trancheId as c_int) >= first {
            let idx = (trancheId as c_int - first) as usize;
            if idx < BuiltinTrancheNames.len() {
                return BuiltinTrancheNames[idx].1.as_ptr() as *const c_char;
            }
        }
        return b"\0".as_ptr() as *const c_char;
    }

    /*
     * It's an extension tranche, so look in LWLockTrancheNames[].  However,
     * it's possible that the tranche has never been registered in the current
     * process, in which case give up and return "extension".
     */
    trancheId -= LWTRANCHE_FIRST_USER_DEFINED as u16;

    if (trancheId as c_int) >= LWLockTrancheNamesAllocated
        || (*LWLockTrancheNames.add(trancheId as usize)).is_null()
    {
        return b"extension\0".as_ptr() as *const c_char;
    }

    *LWLockTrancheNames.add(trancheId as usize)
}

/// Return an identifier for an LWLock based on the wait class and event.
pub unsafe fn GetLWLockIdentifier(classId: u32, eventId: u16) -> *const c_char {
    Assert!(classId == PG_WAIT_LWLOCK);
    /* The event IDs are just tranche numbers. */
    GetLWTrancheName(eventId)
}

/// Internal function that tries to atomically acquire the lwlock in the passed
/// in mode.
///
/// This function will not block waiting for a lock to become free - that's the
/// caller's job.
///
/// Returns true if the lock isn't free and we need to wait.
unsafe fn LWLockAttemptLock(lock: *mut LWLock, mode: LWLockMode) -> bool {
    let mut old_state: u32;

    Assert!(mode == LW_EXCLUSIVE || mode == LW_SHARED);

    /*
     * Read once outside the loop, later iterations will get the newer value
     * via compare & exchange.
     */
    old_state = pg_atomic_read_u32(&mut (*lock).state);

    /* loop until we've determined whether we could acquire the lock or not */
    loop {
        let mut desired_state: u32;
        let lock_free: bool;

        desired_state = old_state;

        if mode == LW_EXCLUSIVE {
            lock_free = (old_state & LW_LOCK_MASK) == 0;
            if lock_free {
                desired_state += LW_VAL_EXCLUSIVE;
            }
        } else {
            lock_free = (old_state & LW_VAL_EXCLUSIVE) == 0;
            if lock_free {
                desired_state += LW_VAL_SHARED;
            }
        }

        /*
         * Attempt to swap in the state we are expecting. If we didn't see
         * lock to be free, that's just the old value. If we saw it as free,
         * we'll attempt to mark it acquired. The reason that we always swap
         * in the value is that this doubles as a memory barrier. We could try
         * to be smarter and only swap in values if we saw the lock as free,
         * but benchmark haven't shown it as beneficial so far.
         *
         * Retry if the value changed since we last looked at it.
         */
        if pg_atomic_compare_exchange_u32(&mut (*lock).state, &mut old_state, desired_state) {
            if lock_free {
                /* Great! Got the lock. */
                // #ifdef LOCK_DEBUG: if mode == LW_EXCLUSIVE lock->owner = MyProc
                return false;
            } else {
                return true; /* somebody else has the lock */
            }
        }
    }
}

/// Lock the LWLock's wait list against concurrent activity.
///
/// NB: even though the wait list is locked, non-conflicting lock operations
/// may still happen concurrently.
///
/// Time spent holding mutex should be short!
unsafe fn LWLockWaitListLock(lock: *mut LWLock) {
    let mut old_state: u32;
    // #ifdef LWLOCK_STATS: lwstats/delays --- omitted (LWLOCK_STATS off)

    loop {
        /* always try once to acquire lock directly */
        old_state = pg_atomic_fetch_or_u32(&mut (*lock).state, LW_FLAG_LOCKED);
        if (old_state & LW_FLAG_LOCKED) == 0 {
            break; /* got lock */
        }

        /* and then spin without atomic operations until lock is released */
        {
            let mut delayStatus: SpinDelayStatus = std::mem::zeroed();

            init_local_spin_delay(&mut delayStatus);

            while old_state & LW_FLAG_LOCKED != 0 {
                perform_spin_delay(&mut delayStatus);
                old_state = pg_atomic_read_u32(&mut (*lock).state);
            }
            // #ifdef LWLOCK_STATS: delays += delayStatus.delays --- omitted
            finish_spin_delay(&mut delayStatus);
        }

        /*
         * Retry. The lock might obviously already be re-acquired by the time
         * we're attempting to get it again.
         */
    }

    // #ifdef LWLOCK_STATS: lwstats->spin_delay_count += delays --- omitted
}

/// Unlock the LWLock's wait list.
///
/// Note that it can be more efficient to manipulate flags and release the
/// locks in a single atomic operation.
unsafe fn LWLockWaitListUnlock(lock: *mut LWLock) {
    let old_state: u32; // PG_USED_FOR_ASSERTS_ONLY

    old_state = pg_atomic_fetch_and_u32(&mut (*lock).state, !LW_FLAG_LOCKED);

    Assert!(old_state & LW_FLAG_LOCKED != 0);
}

/// Wakeup all the lockers that currently have a chance to acquire the lock.
unsafe fn LWLockWakeup(lock: *mut LWLock) {
    let mut new_release_ok: bool;
    let mut wokeup_somebody: bool = false;
    let mut wakeup: proclist_head = std::mem::zeroed();
    let mut iter: proclist_mutable_iter = std::mem::zeroed();

    proclist_init(&mut wakeup);

    new_release_ok = true;

    /* lock wait list while collecting backends to wake up */
    LWLockWaitListLock(lock);

    proclist_foreach_modify!(iter, &mut (*lock).waiters, lwWaitLink, {
        let waiter: *mut PGPROC = GetPGProcByNumber(iter.cur);

        if wokeup_somebody && pgproc_lwWaitMode(waiter) == LW_EXCLUSIVE {
            continue;
        }

        proclist_delete!(&mut (*lock).waiters, iter.cur, lwWaitLink);
        proclist_push_tail!(&mut wakeup, iter.cur, lwWaitLink);

        if pgproc_lwWaitMode(waiter) != LW_WAIT_UNTIL_FREE {
            /*
             * Prevent additional wakeups until retryer gets to run. Backends
             * that are just waiting for the lock to become free don't retry
             * automatically.
             */
            new_release_ok = false;

            /*
             * Don't wakeup (further) exclusive locks.
             */
            wokeup_somebody = true;
        }

        /*
         * Signal that the process isn't on the wait list anymore. This allows
         * LWLockDequeueSelf() to remove itself of the waitlist with a
         * proclist_delete(), rather than having to check if it has been
         * removed from the list.
         */
        Assert!(pgproc_lwWaiting(waiter) == LW_WS_WAITING);
        pgproc_set_lwWaiting(waiter, LW_WS_PENDING_WAKEUP);

        /*
         * Once we've woken up an exclusive lock, there's no point in waking
         * up anybody else.
         */
        if pgproc_lwWaitMode(waiter) == LW_EXCLUSIVE {
            break;
        }
    });

    Assert!(
        proclist_is_empty(&wakeup)
            || pg_atomic_read_u32(&mut (*lock).state) & LW_FLAG_HAS_WAITERS != 0
    );

    /* unset required flags, and release lock, in one fell swoop */
    {
        let mut old_state: u32;
        let mut desired_state: u32;

        old_state = pg_atomic_read_u32(&mut (*lock).state);
        loop {
            desired_state = old_state;

            /* compute desired flags */

            if new_release_ok {
                desired_state |= LW_FLAG_RELEASE_OK;
            } else {
                desired_state &= !LW_FLAG_RELEASE_OK;
            }

            if proclist_is_empty(&(*lock).waiters) {
                desired_state &= !LW_FLAG_HAS_WAITERS;
            }

            desired_state &= !LW_FLAG_LOCKED; /* release lock */

            if pg_atomic_compare_exchange_u32(&mut (*lock).state, &mut old_state, desired_state) {
                break;
            }
        }
    }

    /* Awaken any waiters I removed from the queue. */
    proclist_foreach_modify!(iter, &mut wakeup, lwWaitLink, {
        let waiter: *mut PGPROC = GetPGProcByNumber(iter.cur);

        LOG_LWDEBUG("LWLockRelease", lock, "release waiter");
        proclist_delete!(&mut wakeup, iter.cur, lwWaitLink);

        /*
         * Guarantee that lwWaiting being unset only becomes visible once the
         * unlink from the link has completed. Otherwise the target backend
         * could be woken up for other reason and enqueue for a new lock - if
         * that happens before the list unlink happens, the list would end up
         * being corrupted.
         *
         * The barrier pairs with the LWLockWaitListLock() when enqueuing for
         * another lock.
         */
        pg_write_barrier();
        pgproc_set_lwWaiting(waiter, LW_WS_NOT_WAITING);
        PGSemaphoreUnlock(pgproc_sem(waiter));
    });
}

/// Add ourselves to the end of the queue.
///
/// NB: Mode can be LW_WAIT_UNTIL_FREE here!
unsafe fn LWLockQueueSelf(lock: *mut LWLock, mode: LWLockMode) {
    /*
     * If we don't have a PGPROC structure, there's no way to wait. This
     * should never occur, since MyProc should only be null during shared
     * memory initialization.
     */
    if MyProc.is_null() {
        elog!(PANIC, "cannot wait without a PGPROC structure");
    }

    if pgproc_lwWaiting(MyProc) != LW_WS_NOT_WAITING {
        elog!(PANIC, "queueing for lock while waiting on another one");
    }

    LWLockWaitListLock(lock);

    /* setting the flag is protected by the spinlock */
    pg_atomic_fetch_or_u32(&mut (*lock).state, LW_FLAG_HAS_WAITERS);

    pgproc_set_lwWaiting(MyProc, LW_WS_WAITING);
    pgproc_set_lwWaitMode(MyProc, mode);

    /* LW_WAIT_UNTIL_FREE waiters are always at the front of the queue */
    if mode == LW_WAIT_UNTIL_FREE {
        proclist_push_head!(&mut (*lock).waiters, MyProcNumber, lwWaitLink);
    } else {
        proclist_push_tail!(&mut (*lock).waiters, MyProcNumber, lwWaitLink);
    }

    /* Can release the mutex now */
    LWLockWaitListUnlock(lock);

    // #ifdef LOCK_DEBUG: pg_atomic_fetch_add_u32(&lock->nwaiters, 1) --- omitted
}

/// Remove ourselves from the waitlist.
///
/// This is used if we queued ourselves because we thought we needed to sleep
/// but, after further checking, we discovered that we don't actually need to
/// do so.
unsafe fn LWLockDequeueSelf(lock: *mut LWLock) {
    let on_waitlist: bool;

    // #ifdef LWLOCK_STATS: lwstats->dequeue_self_count++ --- omitted

    LWLockWaitListLock(lock);

    /*
     * Remove ourselves from the waitlist, unless we've already been removed.
     * The removal happens with the wait list lock held, so there's no race in
     * this check.
     */
    on_waitlist = pgproc_lwWaiting(MyProc) == LW_WS_WAITING;
    if on_waitlist {
        proclist_delete!(&mut (*lock).waiters, MyProcNumber, lwWaitLink);
    }

    if proclist_is_empty(&(*lock).waiters)
        && (pg_atomic_read_u32(&mut (*lock).state) & LW_FLAG_HAS_WAITERS) != 0
    {
        pg_atomic_fetch_and_u32(&mut (*lock).state, !LW_FLAG_HAS_WAITERS);
    }

    /* XXX: combine with fetch_and above? */
    LWLockWaitListUnlock(lock);

    /* clear waiting state again, nice for debugging */
    if on_waitlist {
        pgproc_set_lwWaiting(MyProc, LW_WS_NOT_WAITING);
    } else {
        let mut extraWaits: c_int = 0;

        /*
         * Somebody else dequeued us and has or will wake us up. Deal with the
         * superfluous absorption of a wakeup.
         */

        /*
         * Reset RELEASE_OK flag if somebody woke us before we removed
         * ourselves - they'll have set it to false.
         */
        pg_atomic_fetch_or_u32(&mut (*lock).state, LW_FLAG_RELEASE_OK);

        /*
         * Now wait for the scheduled wakeup, otherwise our ->lwWaiting would
         * get reset at some inconvenient point later. Most of the time this
         * will immediately return.
         */
        loop {
            PGSemaphoreLock(pgproc_sem(MyProc));
            if pgproc_lwWaiting(MyProc) == LW_WS_NOT_WAITING {
                break;
            }
            extraWaits += 1;
        }

        /*
         * Fix the process wait semaphore's count for any absorbed wakeups.
         */
        while extraWaits > 0 {
            PGSemaphoreUnlock(pgproc_sem(MyProc));
            extraWaits -= 1;
        }
    }

    // #ifdef LOCK_DEBUG: nwaiters fetch_sub assert --- omitted (LOCK_DEBUG off)
}

/// LWLockAcquire - acquire a lightweight lock in the specified mode
///
/// If the lock is not available, sleep until it is.  Returns true if the lock
/// was available immediately, false if we had to sleep.
///
/// Side effect: cancel/die interrupts are held off until lock release.
#[no_mangle]
pub unsafe fn LWLockAcquire(lock: *mut LWLock, mode: LWLockMode) -> bool {
    let proc_: *mut PGPROC = MyProc;
    let mut result: bool = true;
    let mut extraWaits: c_int = 0;
    // #ifdef LWLOCK_STATS: lwstats --- omitted (LWLOCK_STATS off)

    Assert!(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    PRINT_LWDEBUG("LWLockAcquire", lock, mode);

    // #ifdef LWLOCK_STATS: count lock acquisition attempts --- omitted

    /*
     * We can't wait if we haven't got a PGPROC.  This should only occur
     * during bootstrap or shared memory initialization.  Put an Assert here
     * to catch unsafe coding practices.
     */
    Assert!(!(proc_.is_null() && IsUnderPostmaster));

    /* Ensure we will have room to remember the lock */
    if num_held_lwlocks >= MAX_SIMUL_LWLOCKS as c_int {
        elog!(ERROR, "too many LWLocks taken");
    }

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS();

    /*
     * Loop here to try to acquire lock after each time we are signaled by
     * LWLockRelease.
     *
     * NOTE: it might seem better to have LWLockRelease actually grant us the
     * lock, rather than retrying and possibly having to go back to sleep. But
     * in practice that is no good because it means a process swap for every
     * lock acquisition when two or more processes are contending for the same
     * lock.  Since LWLocks are normally used to protect not-very-long
     * sections of computation, a process needs to be able to acquire and
     * release the same lock many times during a single CPU time slice, even
     * in the presence of contention.  The efficiency of being able to do that
     * outweighs the inefficiency of sometimes wasting a process dispatch
     * cycle because the lock is not free when a released waiter finally gets
     * to run.  See pgsql-hackers archives for 29-Dec-01.
     */
    loop {
        let mut mustwait: bool;

        /*
         * Try to grab the lock the first time, we're not in the waitqueue
         * yet/anymore.
         */
        mustwait = LWLockAttemptLock(lock, mode);

        if !mustwait {
            LOG_LWDEBUG("LWLockAcquire", lock, "immediately acquired lock");
            break; /* got the lock */
        }

        /*
         * Ok, at this point we couldn't grab the lock on the first try. We
         * cannot simply queue ourselves to the end of the list and wait to be
         * woken up because by now the lock could long have been released.
         * Instead add us to the queue and try to grab the lock again. If we
         * succeed we need to revert the queuing and be happy, otherwise we
         * recheck the lock. If we still couldn't grab it, we know that the
         * other locker will see our queue entries when releasing since they
         * existed before we checked for the lock.
         */

        /* add to the queue */
        LWLockQueueSelf(lock, mode);

        /* we're now guaranteed to be woken up if necessary */
        mustwait = LWLockAttemptLock(lock, mode);

        /* ok, grabbed the lock the second time round, need to undo queueing */
        if !mustwait {
            LOG_LWDEBUG("LWLockAcquire", lock, "acquired, undoing queue");

            LWLockDequeueSelf(lock);
            break;
        }

        /*
         * Wait until awakened.
         *
         * It is possible that we get awakened for a reason other than being
         * signaled by LWLockRelease.  If so, loop back and wait again.  Once
         * we've gotten the LWLock, re-increment the sema by the number of
         * additional signals received.
         */
        LOG_LWDEBUG("LWLockAcquire", lock, "waiting");

        // #ifdef LWLOCK_STATS: lwstats->block_count++ --- omitted

        LWLockReportWaitStart(lock);
        if TRACE_POSTGRESQL_LWLOCK_WAIT_START_ENABLED() {
            TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), mode);
        }

        loop {
            PGSemaphoreLock(pgproc_sem(proc_));
            if pgproc_lwWaiting(proc_) == LW_WS_NOT_WAITING {
                break;
            }
            extraWaits += 1;
        }

        /* Retrying, allow LWLockRelease to release waiters again. */
        pg_atomic_fetch_or_u32(&mut (*lock).state, LW_FLAG_RELEASE_OK);

        // #ifdef LOCK_DEBUG: nwaiters fetch_sub assert --- omitted

        if TRACE_POSTGRESQL_LWLOCK_WAIT_DONE_ENABLED() {
            TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), mode);
        }
        LWLockReportWaitEnd();

        LOG_LWDEBUG("LWLockAcquire", lock, "awakened");

        /* Now loop back and try to acquire lock again. */
        result = false;
    }

    if TRACE_POSTGRESQL_LWLOCK_ACQUIRE_ENABLED() {
        TRACE_POSTGRESQL_LWLOCK_ACQUIRE(T_NAME(lock), mode);
    }

    /* Add lock to list of locks held by this backend */
    held_lwlocks[num_held_lwlocks as usize].lock = lock;
    held_lwlocks[num_held_lwlocks as usize].mode = mode;
    num_held_lwlocks += 1;

    /*
     * Fix the process wait semaphore's count for any absorbed wakeups.
     */
    while extraWaits > 0 {
        PGSemaphoreUnlock(pgproc_sem(proc_));
        extraWaits -= 1;
    }

    result
}

/// LWLockConditionalAcquire - acquire a lightweight lock in the specified mode
///
/// If the lock is not available, return false with no side-effects.
///
/// If successful, cancel/die interrupts are held off until lock release.
pub unsafe fn LWLockConditionalAcquire(lock: *mut LWLock, mode: LWLockMode) -> bool {
    let mustwait: bool;

    Assert!(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    PRINT_LWDEBUG("LWLockConditionalAcquire", lock, mode);

    /* Ensure we will have room to remember the lock */
    if num_held_lwlocks >= MAX_SIMUL_LWLOCKS as c_int {
        elog!(ERROR, "too many LWLocks taken");
    }

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS();

    /* Check for the lock */
    mustwait = LWLockAttemptLock(lock, mode);

    if mustwait {
        /* Failed to get lock, so release interrupt holdoff */
        RESUME_INTERRUPTS();

        LOG_LWDEBUG("LWLockConditionalAcquire", lock, "failed");
        if TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL_ENABLED() {
            TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL(T_NAME(lock), mode);
        }
    } else {
        /* Add lock to list of locks held by this backend */
        held_lwlocks[num_held_lwlocks as usize].lock = lock;
        held_lwlocks[num_held_lwlocks as usize].mode = mode;
        num_held_lwlocks += 1;
        if TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_ENABLED() {
            TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE(T_NAME(lock), mode);
        }
    }
    !mustwait
}

/// LWLockAcquireOrWait - Acquire lock, or wait until it's free
///
/// The semantics of this function are a bit funky.  If the lock is currently
/// free, it is acquired in the given mode, and the function returns true.  If
/// the lock isn't immediately free, the function waits until it is released
/// and returns false, but does not acquire the lock.
///
/// This is currently used for WALWriteLock: when a backend flushes the WAL,
/// holding WALWriteLock, it can flush the commit records of many other
/// backends as a side-effect.  Those other backends need to wait until the
/// flush finishes, but don't need to acquire the lock anymore.  They can just
/// wake up, observe that their records have already been flushed, and return.
pub unsafe fn LWLockAcquireOrWait(lock: *mut LWLock, mode: LWLockMode) -> bool {
    let proc_: *mut PGPROC = MyProc;
    let mut mustwait: bool;
    let mut extraWaits: c_int = 0;
    // #ifdef LWLOCK_STATS: lwstats --- omitted (LWLOCK_STATS off)

    Assert!(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    PRINT_LWDEBUG("LWLockAcquireOrWait", lock, mode);

    /* Ensure we will have room to remember the lock */
    if num_held_lwlocks >= MAX_SIMUL_LWLOCKS as c_int {
        elog!(ERROR, "too many LWLocks taken");
    }

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS();

    /*
     * NB: We're using nearly the same twice-in-a-row lock acquisition
     * protocol as LWLockAcquire(). Check its comments for details.
     */
    mustwait = LWLockAttemptLock(lock, mode);

    if mustwait {
        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE);

        mustwait = LWLockAttemptLock(lock, mode);

        if mustwait {
            /*
             * Wait until awakened.  Like in LWLockAcquire, be prepared for
             * bogus wakeups.
             */
            LOG_LWDEBUG("LWLockAcquireOrWait", lock, "waiting");

            // #ifdef LWLOCK_STATS: lwstats->block_count++ --- omitted

            LWLockReportWaitStart(lock);
            if TRACE_POSTGRESQL_LWLOCK_WAIT_START_ENABLED() {
                TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), mode);
            }

            loop {
                PGSemaphoreLock(pgproc_sem(proc_));
                if pgproc_lwWaiting(proc_) == LW_WS_NOT_WAITING {
                    break;
                }
                extraWaits += 1;
            }

            // #ifdef LOCK_DEBUG: nwaiters fetch_sub assert --- omitted

            if TRACE_POSTGRESQL_LWLOCK_WAIT_DONE_ENABLED() {
                TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), mode);
            }
            LWLockReportWaitEnd();

            LOG_LWDEBUG("LWLockAcquireOrWait", lock, "awakened");
        } else {
            LOG_LWDEBUG("LWLockAcquireOrWait", lock, "acquired, undoing queue");

            /*
             * Got lock in the second attempt, undo queueing. We need to treat
             * this as having successfully acquired the lock, otherwise we'd
             * not necessarily wake up people we've prevented from acquiring
             * the lock.
             */
            LWLockDequeueSelf(lock);
        }
    }

    /*
     * Fix the process wait semaphore's count for any absorbed wakeups.
     */
    while extraWaits > 0 {
        PGSemaphoreUnlock(pgproc_sem(proc_));
        extraWaits -= 1;
    }

    if mustwait {
        /* Failed to get lock, so release interrupt holdoff */
        RESUME_INTERRUPTS();
        LOG_LWDEBUG("LWLockAcquireOrWait", lock, "failed");
        if TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT_FAIL_ENABLED() {
            TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT_FAIL(T_NAME(lock), mode);
        }
    } else {
        LOG_LWDEBUG("LWLockAcquireOrWait", lock, "succeeded");
        /* Add lock to list of locks held by this backend */
        held_lwlocks[num_held_lwlocks as usize].lock = lock;
        held_lwlocks[num_held_lwlocks as usize].mode = mode;
        num_held_lwlocks += 1;
        if TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT_ENABLED() {
            TRACE_POSTGRESQL_LWLOCK_ACQUIRE_OR_WAIT(T_NAME(lock), mode);
        }
    }

    !mustwait
}

/// Does the lwlock in its current state need to wait for the variable value to
/// change?
///
/// If we don't need to wait, and it's because the value of the variable has
/// changed, store the current value in newval.
///
/// *result is set to true if the lock was free, and false otherwise.
unsafe fn LWLockConflictsWithVar(
    lock: *mut LWLock,
    valptr: *mut pg_atomic_uint64,
    oldval: u64,
    newval: *mut u64,
    result: *mut bool,
) -> bool {
    let mut mustwait: bool;
    let value: u64;

    /*
     * Test first to see if it the slot is free right now.
     *
     * XXX: the unique caller of this routine, WaitXLogInsertionsToFinish()
     * via LWLockWaitForVar(), uses an implied barrier with a spinlock before
     * this, so we don't need a memory barrier here as far as the current
     * usage is concerned.  But that might not be safe in general.
     */
    mustwait = (pg_atomic_read_u32(&mut (*lock).state) & LW_VAL_EXCLUSIVE) != 0;

    if !mustwait {
        *result = true;
        return false;
    }

    *result = false;

    /*
     * Reading this value atomically is safe even on platforms where uint64
     * cannot be read without observing a torn value.
     */
    value = pg_atomic_read_u64(valptr);

    if value != oldval {
        mustwait = false;
        *newval = value;
    } else {
        mustwait = true;
    }

    mustwait
}

/// LWLockWaitForVar - Wait until lock is free, or a variable is updated.
///
/// If the lock is held and *valptr equals oldval, waits until the lock is
/// either freed, or the lock holder updates *valptr by calling
/// LWLockUpdateVar.  If the lock is free on exit (immediately or after
/// waiting), returns true.  If the lock is still held, but *valptr no longer
/// matches oldval, returns false and sets *newval to the current value in
/// *valptr.
///
/// Note: this function ignores shared lock holders; if the lock is held
/// in shared mode, returns 'true'.
///
/// Be aware that LWLockConflictsWithVar() does not include a memory barrier,
/// hence the caller of this function may want to rely on an explicit barrier or
/// an implied barrier via spinlock or LWLock to avoid memory ordering issues.
pub unsafe fn LWLockWaitForVar(
    lock: *mut LWLock,
    valptr: *mut pg_atomic_uint64,
    oldval: u64,
    newval: *mut u64,
) -> bool {
    let proc_: *mut PGPROC = MyProc;
    let mut extraWaits: c_int = 0;
    let mut result: bool = false;
    // #ifdef LWLOCK_STATS: lwstats --- omitted (LWLOCK_STATS off)

    PRINT_LWDEBUG("LWLockWaitForVar", lock, LW_WAIT_UNTIL_FREE);

    /*
     * Lock out cancel/die interrupts while we sleep on the lock.  There is no
     * cleanup mechanism to remove us from the wait queue if we got
     * interrupted.
     */
    HOLD_INTERRUPTS();

    /*
     * Loop here to check the lock's status after each time we are signaled.
     */
    loop {
        let mut mustwait: bool;

        mustwait = LWLockConflictsWithVar(lock, valptr, oldval, newval, &mut result);

        if !mustwait {
            break; /* the lock was free or value didn't match */
        }

        /*
         * Add myself to wait queue. Note that this is racy, somebody else
         * could wakeup before we're finished queuing. NB: We're using nearly
         * the same twice-in-a-row lock acquisition protocol as
         * LWLockAcquire(). Check its comments for details. The only
         * difference is that we also have to check the variable's values when
         * checking the state of the lock.
         */
        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE);

        /*
         * Set RELEASE_OK flag, to make sure we get woken up as soon as the
         * lock is released.
         */
        pg_atomic_fetch_or_u32(&mut (*lock).state, LW_FLAG_RELEASE_OK);

        /*
         * We're now guaranteed to be woken up if necessary. Recheck the lock
         * and variables state.
         */
        mustwait = LWLockConflictsWithVar(lock, valptr, oldval, newval, &mut result);

        /* Ok, no conflict after we queued ourselves. Undo queueing. */
        if !mustwait {
            LOG_LWDEBUG("LWLockWaitForVar", lock, "free, undoing queue");

            LWLockDequeueSelf(lock);
            break;
        }

        /*
         * Wait until awakened.
         *
         * It is possible that we get awakened for a reason other than being
         * signaled by LWLockRelease.  If so, loop back and wait again.  Once
         * we've gotten the LWLock, re-increment the sema by the number of
         * additional signals received.
         */
        LOG_LWDEBUG("LWLockWaitForVar", lock, "waiting");

        // #ifdef LWLOCK_STATS: lwstats->block_count++ --- omitted

        LWLockReportWaitStart(lock);
        if TRACE_POSTGRESQL_LWLOCK_WAIT_START_ENABLED() {
            TRACE_POSTGRESQL_LWLOCK_WAIT_START(T_NAME(lock), LW_EXCLUSIVE);
        }

        loop {
            PGSemaphoreLock(pgproc_sem(proc_));
            if pgproc_lwWaiting(proc_) == LW_WS_NOT_WAITING {
                break;
            }
            extraWaits += 1;
        }

        // #ifdef LOCK_DEBUG: nwaiters fetch_sub assert --- omitted

        if TRACE_POSTGRESQL_LWLOCK_WAIT_DONE_ENABLED() {
            TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(T_NAME(lock), LW_EXCLUSIVE);
        }
        LWLockReportWaitEnd();

        LOG_LWDEBUG("LWLockWaitForVar", lock, "awakened");

        /* Now loop back and check the status of the lock again. */
    }

    /*
     * Fix the process wait semaphore's count for any absorbed wakeups.
     */
    while extraWaits > 0 {
        PGSemaphoreUnlock(pgproc_sem(proc_));
        extraWaits -= 1;
    }

    /*
     * Now okay to allow cancel/die interrupts.
     */
    RESUME_INTERRUPTS();

    result
}

/// LWLockUpdateVar - Update a variable and wake up waiters atomically
///
/// Sets *valptr to 'val', and wakes up all processes waiting for us with
/// LWLockWaitForVar().  It first sets the value atomically and then wakes up
/// waiting processes so that any process calling LWLockWaitForVar() on the same
/// lock is guaranteed to see the new value, and act accordingly.
///
/// The caller must be holding the lock in exclusive mode.
pub unsafe fn LWLockUpdateVar(lock: *mut LWLock, valptr: *mut pg_atomic_uint64, val: u64) {
    let mut wakeup: proclist_head = std::mem::zeroed();
    let mut iter: proclist_mutable_iter = std::mem::zeroed();

    PRINT_LWDEBUG("LWLockUpdateVar", lock, LW_EXCLUSIVE);

    /*
     * Note that pg_atomic_exchange_u64 is a full barrier, so we're guaranteed
     * that the variable is updated before waking up waiters.
     */
    pg_atomic_exchange_u64(valptr, val);

    proclist_init(&mut wakeup);

    LWLockWaitListLock(lock);

    Assert!(pg_atomic_read_u32(&mut (*lock).state) & LW_VAL_EXCLUSIVE != 0);

    /*
     * See if there are any LW_WAIT_UNTIL_FREE waiters that need to be woken
     * up. They are always in the front of the queue.
     */
    proclist_foreach_modify!(iter, &mut (*lock).waiters, lwWaitLink, {
        let waiter: *mut PGPROC = GetPGProcByNumber(iter.cur);

        if pgproc_lwWaitMode(waiter) != LW_WAIT_UNTIL_FREE {
            break;
        }

        proclist_delete!(&mut (*lock).waiters, iter.cur, lwWaitLink);
        proclist_push_tail!(&mut wakeup, iter.cur, lwWaitLink);

        /* see LWLockWakeup() */
        Assert!(pgproc_lwWaiting(waiter) == LW_WS_WAITING);
        pgproc_set_lwWaiting(waiter, LW_WS_PENDING_WAKEUP);
    });

    /* We are done updating shared state of the lock itself. */
    LWLockWaitListUnlock(lock);

    /*
     * Awaken any waiters I removed from the queue.
     */
    proclist_foreach_modify!(iter, &mut wakeup, lwWaitLink, {
        let waiter: *mut PGPROC = GetPGProcByNumber(iter.cur);

        proclist_delete!(&mut wakeup, iter.cur, lwWaitLink);
        /* check comment in LWLockWakeup() about this barrier */
        pg_write_barrier();
        pgproc_set_lwWaiting(waiter, LW_WS_NOT_WAITING);
        PGSemaphoreUnlock(pgproc_sem(waiter));
    });
}

/// Stop treating lock as held by current backend.
///
/// This is the code that can be shared between actually releasing a lock
/// (LWLockRelease()) and just not tracking ownership of the lock anymore
/// without releasing the lock (LWLockDisown()).
///
/// Returns the mode in which the lock was held by the current backend.
///
/// NB: This does not call RESUME_INTERRUPTS(), but leaves that responsibility
/// of the caller.
///
/// NB: This will leave lock->owner pointing to the current backend (if
/// LOCK_DEBUG is set). This is somewhat intentional, as it makes it easier to
/// debug cases of missing wakeups during lock release.
#[inline]
unsafe fn LWLockDisownInternal(lock: *mut LWLock) -> LWLockMode {
    let mode: LWLockMode;
    let mut i: c_int;

    /*
     * Remove lock from list of locks held.  Usually, but not always, it will
     * be the latest-acquired lock; so search array backwards.
     */
    i = num_held_lwlocks;
    loop {
        i -= 1;
        if i < 0 {
            break;
        }
        if lock == held_lwlocks[i as usize].lock {
            break;
        }
    }

    if i < 0 {
        elog!(ERROR, "lock {} is not held", CStr::from_ptr(T_NAME(lock)).to_string_lossy());
    }

    mode = held_lwlocks[i as usize].mode;

    num_held_lwlocks -= 1;
    while i < num_held_lwlocks {
        held_lwlocks[i as usize] = held_lwlocks[(i + 1) as usize];
        i += 1;
    }

    mode
}

/// Helper function to release lock, shared between LWLockRelease() and
/// LWLockReleaseDisowned().
unsafe fn LWLockReleaseInternal(lock: *mut LWLock, mode: LWLockMode) {
    let oldstate: u32;
    let check_waiters: bool;

    /*
     * Release my hold on lock, after that it can immediately be acquired by
     * others, even if we still have to wakeup other waiters.
     */
    if mode == LW_EXCLUSIVE {
        oldstate = pg_atomic_sub_fetch_u32(&mut (*lock).state, LW_VAL_EXCLUSIVE);
    } else {
        oldstate = pg_atomic_sub_fetch_u32(&mut (*lock).state, LW_VAL_SHARED);
    }

    /* nobody else can have that kind of lock */
    Assert!(!(oldstate & LW_VAL_EXCLUSIVE != 0));

    if TRACE_POSTGRESQL_LWLOCK_RELEASE_ENABLED() {
        TRACE_POSTGRESQL_LWLOCK_RELEASE(T_NAME(lock));
    }

    /*
     * We're still waiting for backends to get scheduled, don't wake them up
     * again.
     */
    if (oldstate & (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK))
        == (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)
        && (oldstate & LW_LOCK_MASK) == 0
    {
        check_waiters = true;
    } else {
        check_waiters = false;
    }

    /*
     * As waking up waiters requires the spinlock to be acquired, only do so
     * if necessary.
     */
    if check_waiters {
        /* XXX: remove before commit? */
        LOG_LWDEBUG("LWLockRelease", lock, "releasing waiters");
        LWLockWakeup(lock);
    }
}

/// Stop treating lock as held by current backend.
///
/// After calling this function it's the callers responsibility to ensure that
/// the lock gets released (via LWLockReleaseDisowned()), even in case of an
/// error. This only is desirable if the lock is going to be released in a
/// different process than the process that acquired it.
pub unsafe fn LWLockDisown(lock: *mut LWLock) {
    LWLockDisownInternal(lock);

    RESUME_INTERRUPTS();
}

/// LWLockRelease - release a previously acquired lock
#[no_mangle]
pub unsafe fn LWLockRelease(lock: *mut LWLock) {
    let mode: LWLockMode;

    mode = LWLockDisownInternal(lock);

    PRINT_LWDEBUG("LWLockRelease", lock, mode);

    LWLockReleaseInternal(lock, mode);

    /*
     * Now okay to allow cancel/die interrupts.
     */
    RESUME_INTERRUPTS();
}

/// Release lock previously disowned with LWLockDisown().
pub unsafe fn LWLockReleaseDisowned(lock: *mut LWLock, mode: LWLockMode) {
    LWLockReleaseInternal(lock, mode);
}

/// LWLockReleaseClearVar - release a previously acquired lock, reset variable
pub unsafe fn LWLockReleaseClearVar(lock: *mut LWLock, valptr: *mut pg_atomic_uint64, val: u64) {
    /*
     * Note that pg_atomic_exchange_u64 is a full barrier, so we're guaranteed
     * that the variable is updated before releasing the lock.
     */
    pg_atomic_exchange_u64(valptr, val);

    LWLockRelease(lock);
}

/// LWLockReleaseAll - release all currently-held locks
///
/// Used to clean up after ereport(ERROR). An important difference between this
/// function and retail LWLockRelease calls is that InterruptHoldoffCount is
/// unchanged by this operation.  This is necessary since InterruptHoldoffCount
/// has been set to an appropriate level earlier in error recovery. We could
/// decrement it below zero if we allow it to drop for each released lock!
///
/// Note that this function must be safe to call even before the LWLock
/// subsystem has been initialized (e.g., during early startup failures).
/// In that case, num_held_lwlocks will be 0 and we do nothing.
pub unsafe fn LWLockReleaseAll() {
    while num_held_lwlocks > 0 {
        HOLD_INTERRUPTS(); /* match the upcoming RESUME_INTERRUPTS */

        LWLockRelease(held_lwlocks[(num_held_lwlocks - 1) as usize].lock);
    }

    Assert!(num_held_lwlocks == 0);
}

/// ForEachLWLockHeldByMe - run a callback for each held lock
///
/// This is meant as debug support only.
pub unsafe fn ForEachLWLockHeldByMe(
    callback: unsafe extern "C" fn(*mut LWLock, LWLockMode, *mut c_void),
    context: *mut c_void,
) {
    let mut i: c_int;

    i = 0;
    while i < num_held_lwlocks {
        callback(
            held_lwlocks[i as usize].lock,
            held_lwlocks[i as usize].mode,
            context,
        );
        i += 1;
    }
}

/// LWLockHeldByMe - test whether my process holds a lock in any mode
///
/// This is meant as debug support only.
pub unsafe fn LWLockHeldByMe(lock: *mut LWLock) -> bool {
    let mut i: c_int;

    i = 0;
    while i < num_held_lwlocks {
        if held_lwlocks[i as usize].lock == lock {
            return true;
        }
        i += 1;
    }
    false
}

/// LWLockAnyHeldByMe - test whether my process holds any of an array of locks
///
/// This is meant as debug support only.
pub unsafe fn LWLockAnyHeldByMe(lock: *mut LWLock, nlocks: c_int, stride: usize) -> bool {
    let begin: *mut c_char;
    let end: *mut c_char;
    let mut i: c_int;

    begin = lock as *mut c_char;
    end = begin.add(nlocks as usize * stride);
    i = 0;
    while i < num_held_lwlocks {
        let held_lock_addr: *mut c_char = held_lwlocks[i as usize].lock as *mut c_char;
        if held_lock_addr >= begin
            && held_lock_addr < end
            && (held_lock_addr as usize - begin as usize) % stride == 0
        {
            return true;
        }
        i += 1;
    }
    false
}

/// LWLockHeldByMeInMode - test whether my process holds a lock in given mode
///
/// This is meant as debug support only.
pub unsafe fn LWLockHeldByMeInMode(lock: *mut LWLock, mode: LWLockMode) -> bool {
    let mut i: c_int;

    i = 0;
    while i < num_held_lwlocks {
        if held_lwlocks[i as usize].lock == lock && held_lwlocks[i as usize].mode == mode {
            return true;
        }
        i += 1;
    }
    false
}

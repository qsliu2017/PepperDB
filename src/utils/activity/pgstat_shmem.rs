/* -------------------------------------------------------------------------
 *
 * pgstat_shmem.rs
 *   Storage of stats entries in shared memory
 *
 * Copyright (c) 2001-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *   src/backend/utils/activity/pgstat_shmem.c
 * -------------------------------------------------------------------------
 */

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use crate::prelude::*;

// Types from pgstat_internal (canonical full home for all stats types).
use crate::utils::activity::pgstat_internal::{
    pgStatLocal, pgstat_assert_is_up, pgstat_delete_pending_entry, pgstat_get_entry_data,
    pgstat_get_entry_len, pgstat_get_kind_info, PgStat_EntryRef, PgStat_HashKey,
    PgStat_ShmemControl, PgStatShared_Common, PgStatShared_HashEntry,
};
// TimestampTz is defined in pgstat and re-imported through pgstat_internal.
use crate::utils::activity::pgstat::TimestampTz;

// Kind constants and predicates.
use crate::utils::pgstat_kind::{
    pgstat_is_kind_builtin, PgStat_Kind, PGSTAT_KIND_CUSTOM_MIN, PGSTAT_KIND_DATABASE,
    PGSTAT_KIND_MAX, PGSTAT_KIND_MIN,
};

// LWLock API from pgstat (stub that matches the rest of the activity module).
use crate::utils::activity::pgstat::{
    LWLock, LWLockAcquire, LWLockInitialize, LWLockRelease, LW_EXCLUSIVE, LWTRANCHE_PGSTATS_DATA,
};

// Datum conversions -- live in crate::postgres via prelude.
use crate::postgres::{DatumGetInt32, DatumGetObjectId, Int32GetDatum, ObjectIdGetDatum};

// dshash / dsa types and functions.
use crate::lib::dshash::{
    dshash_create, dshash_delete_current, dshash_delete_entry, dshash_detach, dshash_find,
    dshash_find_or_insert, dshash_get_hash_table_handle, dshash_memcpy, dshash_parameters,
    dshash_release_lock, dshash_seq_init, dshash_seq_next, dshash_seq_status, dshash_seq_term,
    dshash_table, DSA_ALLOC_NO_OOM, DSA_ALLOC_ZERO,
    InvalidDsaPointer, LW_SHARED,
};
// dsa_pointer from dshash (the canonical ported location).
use crate::lib::dshash::{dsa_pointer, dshash_attach};

// ERRCODE_OUT_OF_MEMORY is defined in dshash (the only ported location so far).
// Only used in the ereport path; retained for documentation.
#[allow(unused_imports)]
use crate::lib::dshash::ERRCODE_OUT_OF_MEMORY;

// Atomic operations -- use the _impl suffix wrappers that are the translated
// equivalents of the C macros pg_atomic_*(). The types and wrappers all live
// in crate::port::atomics (mod.rs).
use crate::port::atomics::{
    pg_atomic_fetch_add_u32_impl as pg_atomic_fetch_add_u32,
    pg_atomic_init_u32_impl as pg_atomic_init_u32,
    pg_atomic_read_u32_impl as pg_atomic_read_u32,
    pg_atomic_uint32, pg_atomic_uint64,
};

// ---- stubs for DSA functions (not yet ported) --------------------------------
// TODO(pg-port): real dsa_area / dsa_pointer live in utils/dsa.h
//
// pgStatLocal.dsa is typed as *mut c_void (pgstat_internal uses `type dsa_area = c_void`),
// so all dsa_* function stubs here take/return *mut c_void to match that.

unsafe fn dsa_create_in_place(
    _place: *mut c_void,
    _size: Size,
    _tranche_id: c_int,
    _segment: *mut c_void,
) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real dsa_create_in_place lives in utils/dsa.c
}

unsafe fn dsa_attach_in_place(_place: *mut c_void, _segment: *mut c_void) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real dsa_attach_in_place lives in utils/dsa.c
}

unsafe fn dsa_pin(_area: *mut c_void) {
    unimplemented!() // TODO(pg-port): real dsa_pin lives in utils/dsa.c
}

unsafe fn dsa_pin_mapping(_area: *mut c_void) {
    unimplemented!() // TODO(pg-port): real dsa_pin_mapping lives in utils/dsa.c
}

unsafe fn dsa_detach(_area: *mut c_void) {
    unimplemented!() // TODO(pg-port): real dsa_detach lives in utils/dsa.c
}

unsafe fn dsa_release_in_place(_place: *mut c_void) {
    unimplemented!() // TODO(pg-port): real dsa_release_in_place lives in utils/dsa.c
}

unsafe fn dsa_minimum_size() -> Size {
    unimplemented!() // TODO(pg-port): real dsa_minimum_size lives in utils/dsa.c
}

unsafe fn dsa_set_size_limit(_area: *mut c_void, _limit: i64) {
    unimplemented!() // TODO(pg-port): real dsa_set_size_limit lives in utils/dsa.c
}

unsafe fn dsa_allocate_extended(_area: *mut c_void, _size: Size, _flags: c_int) -> dsa_pointer {
    unimplemented!() // TODO(pg-port): real dsa_allocate_extended lives in utils/dsa.c
}

unsafe fn dsa_get_address(_area: *mut c_void, _ptr: dsa_pointer) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real dsa_get_address lives in utils/dsa.c
}

unsafe fn dsa_free(_area: *mut c_void, _ptr: dsa_pointer) {
    unimplemented!() // TODO(pg-port): real dsa_free lives in utils/dsa.c
}

// ---- stubs for atomic u64 operations not yet in the public atomics API -------
// TODO(pg-port): real pg_atomic_init_u64 / pg_atomic_read_u64 / pg_atomic_fetch_add_u64
// live in port/atomics.h

unsafe fn pg_atomic_init_u64(_ptr: &pg_atomic_uint64, _val: u64) {
    unimplemented!() // TODO(pg-port): real pg_atomic_init_u64 lives in port/atomics.h
}

unsafe fn pg_atomic_read_u64(_ptr: &pg_atomic_uint64) -> u64 {
    unimplemented!() // TODO(pg-port): real pg_atomic_read_u64 lives in port/atomics.h
}

unsafe fn pg_atomic_fetch_add_u64(_ptr: &pg_atomic_uint64, _add: i64) -> u64 {
    unimplemented!() // TODO(pg-port): real pg_atomic_fetch_add_u64 lives in port/atomics.h
}

unsafe fn pg_atomic_fetch_sub_u32(_ptr: &pg_atomic_uint32, _sub: i32) -> u32 {
    unimplemented!() // TODO(pg-port): real pg_atomic_fetch_sub_u32 lives in port/atomics.h
}

// ---- stubs for ShmemInitStruct / ShmemAlloc (storage/shmem.h) ---------------
// TODO(pg-port): real implementations live in storage/ipc/shmem.c

unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _found: *mut bool) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real ShmemInitStruct lives in storage/ipc/shmem.c
}

unsafe fn ShmemAlloc(_size: Size) -> *mut c_void {
    unimplemented!() // TODO(pg-port): real ShmemAlloc lives in storage/ipc/shmem.c
}

unsafe fn add_size(_s1: Size, _s2: Size) -> Size {
    unimplemented!() // TODO(pg-port): real add_size lives in storage/ipc/shmem.c
}

// ---- stubs for IsUnderPostmaster (miscadmin.h) --------------------------------
// TODO(pg-port): real IsUnderPostmaster lives in utils/init/globals.c

static mut IsUnderPostmaster: bool = false; // TODO(pg-port): miscadmin.h

// ---- stub for LWTRANCHE_PGSTATS_DSA / LWTRANCHE_PGSTATS_HASH -----------------
// TODO(pg-port): real values live in storage/lwlock.h

const LWTRANCHE_PGSTATS_DSA: c_int = 0; // TODO(pg-port): storage/lwlock.h
const LWTRANCHE_PGSTATS_HASH: c_int = 0; // TODO(pg-port): storage/lwlock.h

// ---- stub for LWLockConditionalAcquire ----------------------------------------
// TODO(pg-port): real LWLockConditionalAcquire lives in storage/lwlock.c

unsafe fn LWLockConditionalAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    unimplemented!() // TODO(pg-port): real LWLockConditionalAcquire lives in storage/lwlock.c
}

// ---- OidIsValid (c.h) ---------------------------------------------------------
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

// ---- Type cast helpers -------------------------------------------------------
// pgstat_internal defines `type dshash_table = c_void` and `type dsa_area = c_void`
// (both stubs pointing to c_void), while crate::lib::dshash has the real
// struct definitions.  Use these inline casts at every call site that bridges
// the two module boundaries, matching the C implicit pointer casts.

#[inline]
unsafe fn shared_hash(p: *mut c_void) -> *mut dshash_table {
    p as *mut dshash_table
}

// ---------------------------------------------------------------------------
// Module-level constants
// ---------------------------------------------------------------------------

const PGSTAT_ENTRY_REF_HASH_SIZE: usize = 128;

// ---------------------------------------------------------------------------
// PgStat_EntryRefHashEntry
//
// Hash table entry for finding the PgStat_EntryRef for a key.  In C this is
// generated by the lib/simplehash.h macro set.  Here we translate it as a
// plain struct; the "simplehash" operations are implemented inline below as
// regular Rust functions, keeping the 1:1 structure of the C code.
// ---------------------------------------------------------------------------

struct PgStat_EntryRefHashEntry {
    key: PgStat_HashKey,
    status: u8,
    entry_ref: *mut PgStat_EntryRef,
}

// ---------------------------------------------------------------------------
// Simplehash table for local entry-ref cache.
//
// In C the SH_PREFIX/SH_DEFINE/SH_DECLARE macros expand to a set of
// functions whose names all start with "pgstat_entry_ref_hash_".  We
// translate them as a thin wrapper struct with the same method names.
// ---------------------------------------------------------------------------

struct pgstat_entry_ref_hash_hash {
    entries: Vec<Option<Box<PgStat_EntryRefHashEntry>>>,
    pub members: usize,
}

impl pgstat_entry_ref_hash_hash {
    fn new(initial_size: usize) -> Self {
        let cap = initial_size.next_power_of_two().max(8);
        let mut entries = Vec::with_capacity(cap);
        for _ in 0..cap {
            entries.push(None);
        }
        pgstat_entry_ref_hash_hash {
            entries,
            members: 0,
        }
    }

    /// Compute slot index for the given key.
    fn slot(&self, key: &PgStat_HashKey) -> usize {
        // Simple hash: xor the three fields together.
        let h = (key.kind as usize)
            ^ ((key.dboid as usize).wrapping_mul(0x9e3779b9))
            ^ ((key.objid as usize).wrapping_mul(0x517cc1b727220a95));
        h & (self.entries.len() - 1)
    }

    fn key_eq(a: &PgStat_HashKey, b: &PgStat_HashKey) -> bool {
        a.kind == b.kind && a.dboid == b.dboid && a.objid == b.objid
    }

    /// Insert (or find existing) entry for key.  Returns a raw pointer to the
    /// entry and sets *found accordingly.
    unsafe fn insert(
        &mut self,
        key: PgStat_HashKey,
        found: *mut bool,
    ) -> *mut PgStat_EntryRefHashEntry {
        // Linear probe.
        let len = self.entries.len();
        let start = self.slot(&key);
        let mut idx = start;
        loop {
            match &self.entries[idx] {
                None => {
                    // Empty slot -- insert here.
                    *found = false;
                    self.entries[idx] = Some(Box::new(PgStat_EntryRefHashEntry {
                        key,
                        status: 1,
                        entry_ref: null_mut(),
                    }));
                    self.members += 1;
                    return self.entries[idx].as_mut().unwrap().as_mut() as *mut _;
                }
                Some(e) if Self::key_eq(&e.key, &key) => {
                    *found = true;
                    return self.entries[idx].as_mut().unwrap().as_mut() as *mut _;
                }
                _ => {}
            }
            idx = (idx + 1) & (len - 1);
            if idx == start {
                panic!("pgstat_entry_ref_hash: table full");
            }
        }
    }

    /// Look up key; returns null if not found.
    unsafe fn lookup(&mut self, key: PgStat_HashKey) -> *mut PgStat_EntryRefHashEntry {
        let len = self.entries.len();
        let start = self.slot(&key);
        let mut idx = start;
        loop {
            match &self.entries[idx] {
                None => return null_mut(),
                Some(e) if Self::key_eq(&e.key, &key) => {
                    return self.entries[idx].as_mut().unwrap().as_mut() as *mut _;
                }
                _ => {}
            }
            idx = (idx + 1) & (len - 1);
            if idx == start {
                return null_mut();
            }
        }
    }

    /// Delete the entry for key.  Returns true if found.
    unsafe fn delete(&mut self, key: PgStat_HashKey) -> bool {
        let len = self.entries.len();
        let start = self.slot(&key);
        let mut idx = start;
        loop {
            match &self.entries[idx] {
                None => return false,
                Some(e) if Self::key_eq(&e.key, &key) => {
                    self.entries[idx] = None;
                    self.members -= 1;
                    return true;
                }
                _ => {}
            }
            idx = (idx + 1) & (len - 1);
            if idx == start {
                return false;
            }
        }
    }
}

// C typedef for the callback used by pgstat_release_matching_entry_refs().
type ReleaseMatchCB =
    Option<unsafe fn(ent: *mut PgStat_EntryRefHashEntry, data: Datum) -> bool>;

// ---------------------------------------------------------------------------
// Parameter table for the shared dshash table of stats entries.
// ---------------------------------------------------------------------------

/// Parameter for the shared hash (file-scope static in C).
///
/// NOTE: dshash_memcpy is used here as the copy function, matching the C code.
static dsh_params: dshash_parameters = dshash_parameters {
    key_size: core::mem::size_of::<PgStat_HashKey>(),
    entry_size: core::mem::size_of::<PgStatShared_HashEntry>(),
    compare_function: Some(pgstat_cmp_hash_key_extern),
    hash_function: Some(pgstat_hash_hash_key_extern),
    copy_function: Some(dshash_memcpy),
    tranche_id: LWTRANCHE_PGSTATS_HASH,
};

// Extern "C" wrappers so we can take function pointers of the inline helpers
// from pgstat_internal.
unsafe extern "C" fn pgstat_cmp_hash_key_extern(
    a: *const c_void,
    b: *const c_void,
    size: Size,
    arg: *mut c_void,
) -> c_int {
    crate::utils::activity::pgstat_internal::pgstat_cmp_hash_key(a, b, size, arg)
}

unsafe extern "C" fn pgstat_hash_hash_key_extern(
    d: *const c_void,
    size: Size,
    arg: *mut c_void,
) -> u32 {
    crate::utils::activity::pgstat_internal::pgstat_hash_hash_key(d, size, arg)
}

// ---------------------------------------------------------------------------
// File-scope (static) state
// ---------------------------------------------------------------------------

/*
 * Backend local references to shared stats entries.  If there are pending
 * updates to a stats entry, the PgStat_EntryRef is added to the pgStatPending
 * list.
 *
 * When a stats entry is dropped each backend needs to release its reference
 * to it before the memory can be released.  To trigger that
 * pgStatLocal.shmem->gc_request_count is incremented - which each backend
 * compares to their copy of pgStatSharedRefAge on a regular basis.
 */
static mut pgStatEntryRefHash: *mut pgstat_entry_ref_hash_hash = null_mut();
static mut pgStatSharedRefAge: u64 = 0; /* cache age of pgStatLocal.shmem */

/*
 * Memory contexts containing the pgStatEntryRefHash table and the
 * pgStatSharedRef entries respectively.  Kept separate to make it easier to
 * track / attribute memory usage.
 */
static mut pgStatSharedRefContext: MemoryContext = null_mut();
static mut pgStatEntryRefHashContext: MemoryContext = null_mut();

// ---------------------------------------------------------------------------
// Public functions called from postmaster
// ---------------------------------------------------------------------------

/*
 * The size of the shared memory allocation for stats stored in the shared
 * stats hash table.  This allocation will be done as part of the main shared
 * memory, rather than dynamic shared memory, allowing it to be initialized in
 * postmaster.
 */
unsafe fn pgstat_dsa_init_size() -> Size {
    let sz: Size;

    /*
     * The dshash header / initial buckets array needs to fit into "plain"
     * shared memory, but it's beneficial to not need dsm segments
     * immediately.  A size of 256kB seems to work well and is not
     * disproportional compared to other constant sized shared memory
     * allocations.  NB: To avoid DSMs further, the user can configure
     * min_dynamic_shared_memory.
     */
    sz = 256 * 1024;
    Assert!(dsa_minimum_size() <= sz);
    MAXALIGN(sz)
}

/*
 * Compute shared memory space needed for cumulative statistics
 */
pub unsafe fn StatsShmemSize() -> Size {
    let mut sz: Size;

    sz = MAXALIGN(core::mem::size_of::<PgStat_ShmemControl>());
    sz = add_size(sz, pgstat_dsa_init_size());

    /* Add shared memory for all the custom fixed-numbered statistics */
    let mut kind: PgStat_Kind = PGSTAT_KIND_CUSTOM_MIN;
    while kind <= PGSTAT_KIND_MAX {
        let kind_info = pgstat_get_kind_info(kind);

        if kind_info.is_null() {
            kind += 1;
            continue;
        }
        if !(*kind_info).fixed_amount() {
            kind += 1;
            continue;
        }

        Assert!((*kind_info).shared_size != 0);

        sz = sz.wrapping_add(MAXALIGN((*kind_info).shared_size as Size));
        kind += 1;
    }

    sz
}

/*
 * Initialize cumulative statistics system during startup
 */
pub unsafe fn StatsShmemInit() {
    let mut found: bool = false;
    let sz: Size;

    sz = StatsShmemSize();
    pgStatLocal.shmem = ShmemInitStruct(c"Shared Memory Stats".as_ptr(), sz, &mut found)
        as *mut PgStat_ShmemControl;

    if !IsUnderPostmaster {
        // dsa is *mut c_void here because our stub dsa_create_in_place returns *mut c_void.
        // Cast to the real dshash dsa_area type only at the dshash_create/dshash_attach call sites.
        let dsa: *mut c_void;
        let dsh: *mut dshash_table;
        let ctl: *mut PgStat_ShmemControl = pgStatLocal.shmem;
        let mut p: *mut c_char = ctl as *mut c_char;

        Assert!(!found);

        /* the allocation of pgStatLocal.shmem itself */
        p = p.add(MAXALIGN(core::mem::size_of::<PgStat_ShmemControl>()));

        /*
         * Create a small dsa allocation in plain shared memory.  This is
         * required because postmaster cannot use dsm segments.  It also
         * provides a small efficiency win.
         */
        (*ctl).raw_dsa_area = p as *mut c_void;
        p = p.add(MAXALIGN(pgstat_dsa_init_size()));
        dsa = dsa_create_in_place(
            (*ctl).raw_dsa_area,
            pgstat_dsa_init_size(),
            LWTRANCHE_PGSTATS_DSA,
            null_mut(),
        );
        dsa_pin(dsa);

        /*
         * To ensure dshash is created in "plain" shared memory, temporarily
         * limit size of dsa to the initial size of the dsa.
         */
        dsa_set_size_limit(dsa, pgstat_dsa_init_size() as i64);

        /*
         * With the limit in place, create the dshash table.  XXX: It'd be
         * nice if there were dshash_create_in_place().
         */
        dsh = dshash_create(dsa as *mut crate::lib::dshash::dsa_area, &dsh_params, null_mut());
        (*ctl).hash_handle = dshash_get_hash_table_handle(dsh);

        /* lift limit set above */
        dsa_set_size_limit(dsa, -1);

        /*
         * Postmaster will never access these again, thus free the local
         * dsa/dshash references.
         */
        dshash_detach(dsh);
        dsa_detach(dsa);

        pg_atomic_init_u64(&(*ctl).gc_request_count, 1);

        /* initialize fixed-numbered stats */
        let mut kind: PgStat_Kind = PGSTAT_KIND_MIN;
        while kind <= PGSTAT_KIND_MAX {
            let kind_info = pgstat_get_kind_info(kind);
            let ptr: *mut c_char;

            if kind_info.is_null() || !(*kind_info).fixed_amount() {
                kind += 1;
                continue;
            }

            if pgstat_is_kind_builtin(kind) {
                ptr = (ctl as *mut c_char).add((*kind_info).shared_ctl_off as usize);
            } else {
                let idx = (kind - PGSTAT_KIND_CUSTOM_MIN) as usize;

                Assert!((*kind_info).shared_size != 0);
                (*ctl).custom_data[idx] = ShmemAlloc((*kind_info).shared_size as Size);
                ptr = (*ctl).custom_data[idx] as *mut c_char;
            }

            ((*kind_info).init_shmem_cb.unwrap())(ptr as *mut c_void);
            kind += 1;
        }
    } else {
        Assert!(found);
    }
}

pub unsafe fn pgstat_attach_shmem() {
    let oldcontext: MemoryContext;

    Assert!(!pgStatLocal.dsa.is_null() == false);

    /* stats shared memory persists for the backend lifetime */
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    pgStatLocal.dsa = dsa_attach_in_place((*pgStatLocal.shmem).raw_dsa_area, null_mut());
    dsa_pin_mapping(pgStatLocal.dsa);

    pgStatLocal.shared_hash = dshash_attach(
        pgStatLocal.dsa as *mut crate::lib::dshash::dsa_area,
        &dsh_params,
        (*pgStatLocal.shmem).hash_handle,
        null_mut(),
    ) as *mut c_void;

    MemoryContextSwitchTo(oldcontext);
}

pub unsafe fn pgstat_detach_shmem() {
    Assert!(!pgStatLocal.dsa.is_null());

    /* we shouldn't leave references to shared stats */
    pgstat_release_all_entry_refs(false);

    dshash_detach(shared_hash(pgStatLocal.shared_hash));
    pgStatLocal.shared_hash = null_mut();

    dsa_detach(pgStatLocal.dsa);

    /*
     * dsa_detach() does not decrement the DSA reference count as no segment
     * was provided to dsa_attach_in_place(), causing no cleanup callbacks to
     * be registered.  Hence, release it manually now.
     */
    dsa_release_in_place((*pgStatLocal.shmem).raw_dsa_area);

    pgStatLocal.dsa = null_mut();
}

// ---------------------------------------------------------------------------
// Maintenance of shared memory stats entries
// ---------------------------------------------------------------------------

/*
 * Initialize entry newly-created.
 *
 * Returns NULL in the event of an allocation failure, so as callers can
 * take cleanup actions as the entry initialized is already inserted in the
 * shared hashtable.
 */
pub unsafe fn pgstat_init_entry(
    kind: PgStat_Kind,
    shhashent: *mut PgStatShared_HashEntry,
) -> *mut PgStatShared_Common {
    /* Create new stats entry. */
    let chunk: dsa_pointer;
    let shheader: *mut PgStatShared_Common;

    /*
     * Initialize refcount to 1, marking it as valid / not dropped.  The entry
     * can't be freed before the initialization because it can't be found as
     * long as we hold the dshash partition lock.  Caller needs to increase
     * further if a longer lived reference is needed.
     */
    pg_atomic_init_u32(&(*shhashent).refcount, 1);

    /*
     * Initialize "generation" to 0, as freshly created.
     */
    pg_atomic_init_u32(&(*shhashent).generation, 0);
    (*shhashent).dropped = false;

    chunk = dsa_allocate_extended(
        pgStatLocal.dsa,
        (*pgstat_get_kind_info(kind)).shared_size as Size,
        DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM,
    );
    if chunk == InvalidDsaPointer {
        return null_mut();
    }

    shheader = dsa_get_address(pgStatLocal.dsa, chunk) as *mut PgStatShared_Common;
    (*shheader).magic = 0xdeadbeef;

    /* Link the new entry from the hash entry. */
    (*shhashent).body = chunk;

    LWLockInitialize(&mut (*shheader).lock, LWTRANCHE_PGSTATS_DATA);

    shheader
}

unsafe fn pgstat_reinit_entry(
    kind: PgStat_Kind,
    shhashent: *mut PgStatShared_HashEntry,
) -> *mut PgStatShared_Common {
    let shheader: *mut PgStatShared_Common;

    shheader =
        dsa_get_address(pgStatLocal.dsa, (*shhashent).body) as *mut PgStatShared_Common;

    /* mark as not dropped anymore */
    pg_atomic_fetch_add_u32(&(*shhashent).refcount, 1);

    /*
     * Increment "generation", to let any backend with local references know
     * that what they point to is outdated.
     */
    pg_atomic_fetch_add_u32(&(*shhashent).generation, 1);
    (*shhashent).dropped = false;

    /* reinitialize content */
    Assert!((*shheader).magic == 0xdeadbeef);
    core::ptr::write_bytes(
        pgstat_get_entry_data(kind, shheader) as *mut u8,
        0,
        pgstat_get_entry_len(kind),
    );

    shheader
}

unsafe fn pgstat_setup_shared_refs() {
    if !pgStatEntryRefHash.is_null() {
        return;
    }

    pgStatEntryRefHash =
        Box::into_raw(Box::new(pgstat_entry_ref_hash_hash::new(PGSTAT_ENTRY_REF_HASH_SIZE)));
    pgStatSharedRefAge = pg_atomic_read_u64(&(*pgStatLocal.shmem).gc_request_count);
    Assert!(pgStatSharedRefAge != 0);
}

/*
 * Helper function for pgstat_get_entry_ref().
 */
unsafe fn pgstat_acquire_entry_ref(
    entry_ref: *mut PgStat_EntryRef,
    shhashent: *mut PgStatShared_HashEntry,
    shheader: *mut PgStatShared_Common,
) {
    Assert!((*shheader).magic == 0xdeadbeef);
    Assert!(pg_atomic_read_u32(&(*shhashent).refcount) > 0);

    pg_atomic_fetch_add_u32(&(*shhashent).refcount, 1);

    dshash_release_lock(shared_hash(pgStatLocal.shared_hash), shhashent as *mut c_void);

    (*entry_ref).shared_stats = shheader;
    (*entry_ref).shared_entry = shhashent;
    (*entry_ref).generation = pg_atomic_read_u32(&(*shhashent).generation);
}

/*
 * Helper function for pgstat_get_entry_ref().
 */
unsafe fn pgstat_get_entry_ref_cached(
    key: PgStat_HashKey,
    entry_ref_p: *mut *mut PgStat_EntryRef,
) -> bool {
    let mut found: bool = false;
    let cache_entry: *mut PgStat_EntryRefHashEntry;

    /*
     * We immediately insert a cache entry, because it avoids 1) multiple
     * hashtable lookups in case of a cache miss 2) having to deal with
     * out-of-memory errors after incrementing PgStatShared_Common->refcount.
     */

    cache_entry = (*pgStatEntryRefHash).insert(key, &mut found);

    if !found || (*cache_entry).entry_ref.is_null() {
        let entry_ref: *mut PgStat_EntryRef;

        entry_ref = MemoryContextAlloc(
            pgStatSharedRefContext,
            core::mem::size_of::<PgStat_EntryRef>(),
        ) as *mut PgStat_EntryRef;
        (*cache_entry).entry_ref = entry_ref;
        (*entry_ref).shared_stats = null_mut();
        (*entry_ref).shared_entry = null_mut();
        (*entry_ref).pending = null_mut();

        found = false;
    } else if (*(*cache_entry).entry_ref).shared_stats.is_null() {
        Assert!((*(*cache_entry).entry_ref).pending.is_null());
        found = false;
    } else {
        /* PG_USED_FOR_ASSERTS_ONLY in C -- only used in debug assertions */
        let entry_ref: *mut PgStat_EntryRef = (*cache_entry).entry_ref;
        Assert!(!(*entry_ref).shared_entry.is_null());
        Assert!(!(*entry_ref).shared_stats.is_null());

        Assert!((*(*entry_ref).shared_stats).magic == 0xdeadbeef);
        /* should have at least our reference */
        Assert!(pg_atomic_read_u32(&(*(*entry_ref).shared_entry).refcount) > 0);
    }

    *entry_ref_p = (*cache_entry).entry_ref;
    found
}

/*
 * Get a shared stats reference.  If create is true, the shared stats object
 * is created if it does not exist.
 *
 * When create is true, and created_entry is non-NULL, it'll be set to true
 * if the entry is newly created, false otherwise.
 */
pub unsafe fn pgstat_get_entry_ref(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
    create: bool,
    created_entry: *mut bool,
) -> *mut PgStat_EntryRef {
    let mut key: PgStat_HashKey = core::mem::zeroed();
    let shhashent: *mut PgStatShared_HashEntry;
    let mut shheader: *mut PgStatShared_Common = null_mut();
    let mut entry_ref: *mut PgStat_EntryRef = null_mut();

    /* clear padding */
    core::ptr::write_bytes(&mut key as *mut PgStat_HashKey as *mut u8, 0,
                           core::mem::size_of::<PgStat_HashKey>());

    key.kind = kind;
    key.dboid = dboid;
    key.objid = objid;

    /*
     * passing in created_entry only makes sense if we possibly could create
     * entry.
     */
    Assert!(create || created_entry.is_null());
    pgstat_assert_is_up();
    Assert!(!pgStatLocal.shared_hash.is_null());
    Assert!(!(*pgStatLocal.shmem).is_shutdown);

    pgstat_setup_memcxt();
    pgstat_setup_shared_refs();

    if !created_entry.is_null() {
        *created_entry = false;
    }

    /*
     * Check if other backends dropped stats that could not be deleted because
     * somebody held references to it.  If so, check this backend's references.
     * This is not expected to happen often.  The location of the check is a
     * bit random, but this is a relatively frequently called path, so better
     * than most.
     */
    if pgstat_need_entry_refs_gc() {
        pgstat_gc_entry_refs();
    }

    /*
     * First check the lookup cache hashtable in local memory.  If we find a
     * match here we can avoid taking locks / causing contention.
     */
    if pgstat_get_entry_ref_cached(key, &mut entry_ref) {
        return entry_ref;
    }

    Assert!(!entry_ref.is_null());

    /*
     * Do a lookup in the hash table first - it's quite likely that the entry
     * already exists, and that way we only need a shared lock.
     */
    let shhashent_raw = dshash_find(
        shared_hash(pgStatLocal.shared_hash),
        &key as *const PgStat_HashKey as *const c_void,
        false,
    );
    let mut shhashent: *mut PgStatShared_HashEntry = shhashent_raw as *mut PgStatShared_HashEntry;

    if create && shhashent.is_null() {
        let mut shfound: bool = false;

        /*
         * It's possible that somebody created the entry since the above
         * lookup.  If so, fall through to the same path as if it had already
         * been created before the dshash_find() call.
         */
        shhashent = dshash_find_or_insert(
            shared_hash(pgStatLocal.shared_hash),
            &key as *const PgStat_HashKey as *const c_void,
            &mut shfound,
        ) as *mut PgStatShared_HashEntry;
        if !shfound {
            shheader = pgstat_init_entry(kind, shhashent);
            if shheader.is_null() {
                /*
                 * Failed the allocation of a new entry, so clean up the
                 * shared hashtable before giving up.
                 */
                dshash_delete_entry(shared_hash(pgStatLocal.shared_hash), shhashent as *mut c_void);

                ereport!(
                    ERROR,
                    errmsg!(
                        "out of memory: failed while allocating stats entry {}/{}/{}.",
                        key.kind,
                        key.dboid,
                        key.objid
                    )
                );
            }
            pgstat_acquire_entry_ref(entry_ref, shhashent, shheader);

            if !created_entry.is_null() {
                *created_entry = true;
            }

            return entry_ref;
        }
    }

    if shhashent.is_null() {
        /*
         * If we're not creating, delete the reference again.  In all
         * likelihood it's just a stats lookup - no point wasting memory for a
         * shared ref to nothing...
         */
        pgstat_release_entry_ref(key, entry_ref, false);

        return null_mut();
    } else {
        /*
         * Can get here either because dshash_find() found a match, or if
         * dshash_find_or_insert() found a concurrently inserted entry.
         */

        if (*shhashent).dropped && create {
            /*
             * There are legitimate cases where the old stats entry might not
             * yet have been dropped by the time it's reused.  The most obvious
             * case are replication slot stats, where a new slot can be
             * created with the same index just after dropping.  But oid
             * wraparound can lead to other cases as well.  We just reset the
             * stats to their plain state, while incrementing its "generation"
             * in the shared entry for any remaining local references.
             */
            shheader = pgstat_reinit_entry(kind, shhashent);
            pgstat_acquire_entry_ref(entry_ref, shhashent, shheader);

            if !created_entry.is_null() {
                *created_entry = true;
            }

            return entry_ref;
        } else if (*shhashent).dropped {
            dshash_release_lock(shared_hash(pgStatLocal.shared_hash), shhashent as *mut c_void);
            pgstat_release_entry_ref(key, entry_ref, false);

            return null_mut();
        } else {
            shheader =
                dsa_get_address(pgStatLocal.dsa, (*shhashent).body) as *mut PgStatShared_Common;
            pgstat_acquire_entry_ref(entry_ref, shhashent, shheader);

            return entry_ref;
        }
    }
}

unsafe fn pgstat_release_entry_ref(
    key: PgStat_HashKey,
    entry_ref: *mut PgStat_EntryRef,
    discard_pending: bool,
) {
    if !entry_ref.is_null() && !(*entry_ref).pending.is_null() {
        if discard_pending {
            pgstat_delete_pending_entry(entry_ref);
        } else {
            elog!(ERROR, "releasing ref with pending data");
        }
    }

    if !entry_ref.is_null() && !(*entry_ref).shared_stats.is_null() {
        Assert!((*(*entry_ref).shared_stats).magic == 0xdeadbeef);
        Assert!((*entry_ref).pending.is_null());

        /*
         * This can't race with another backend looking up the stats entry
         * and increasing the refcount because it is not "legal" to create
         * additional references to dropped entries.
         */
        if pg_atomic_fetch_sub_u32(&(*(*entry_ref).shared_entry).refcount, 1) == 1 {
            let shent: *mut PgStatShared_HashEntry;

            /*
             * We're the last referrer to this entry, try to drop the shared
             * entry.
             */

            /* only dropped entries can reach a 0 refcount */
            Assert!((*(*entry_ref).shared_entry).dropped);

            shent = dshash_find(
                shared_hash(pgStatLocal.shared_hash),
                &(*(*entry_ref).shared_entry).key as *const PgStat_HashKey as *const c_void,
                true,
            ) as *mut PgStatShared_HashEntry;
            if shent.is_null() {
                elog!(ERROR, "could not find just referenced shared stats entry");
            }

            /*
             * This entry may have been reinitialized while trying to release
             * it, so double-check that it has not been reused while holding a
             * lock on its shared entry.
             */
            if pg_atomic_read_u32(&(*(*entry_ref).shared_entry).generation)
                == (*entry_ref).generation
            {
                /* Same "generation", so we're OK with the removal */
                Assert!(
                    pg_atomic_read_u32(&(*(*entry_ref).shared_entry).refcount) == 0
                );
                Assert!((*entry_ref).shared_entry == shent);
                pgstat_free_entry(shent, null_mut());
            } else {
                /*
                 * Shared stats entry has been reinitialized, so do not drop
                 * its shared entry, only release its lock.
                 */
                dshash_release_lock(shared_hash(pgStatLocal.shared_hash), shent as *mut c_void);
            }
        }
    }

    if !(*pgStatEntryRefHash).delete(key) {
        elog!(ERROR, "entry ref vanished before deletion");
    }

    if !entry_ref.is_null() {
        pfree(entry_ref as *mut c_void);
    }
}

/*
 * Acquire exclusive lock on the entry.
 *
 * If nowait is true, it's just a conditional acquire, and the result
 * *must* be checked to verify success.
 * If nowait is false, waits as necessary, always returning true.
 */
pub unsafe fn pgstat_lock_entry(entry_ref: *mut PgStat_EntryRef, nowait: bool) -> bool {
    let lock: *mut LWLock = &mut (*(*entry_ref).shared_stats).lock;

    if nowait {
        return LWLockConditionalAcquire(lock, LW_EXCLUSIVE);
    }

    LWLockAcquire(lock, LW_EXCLUSIVE);
    true
}

/*
 * Acquire shared lock on the entry.
 *
 * Separate from pgstat_lock_entry() as most callers will need to lock
 * exclusively.  The wait semantics are identical.
 */
pub unsafe fn pgstat_lock_entry_shared(entry_ref: *mut PgStat_EntryRef, nowait: bool) -> bool {
    let lock: *mut LWLock = &mut (*(*entry_ref).shared_stats).lock;

    if nowait {
        return LWLockConditionalAcquire(lock, LW_SHARED);
    }

    LWLockAcquire(lock, LW_SHARED);
    true
}

pub unsafe fn pgstat_unlock_entry(entry_ref: *mut PgStat_EntryRef) {
    LWLockRelease(&mut (*(*entry_ref).shared_stats).lock);
}

/*
 * Helper function to fetch and lock shared stats.
 */
pub unsafe fn pgstat_get_entry_ref_locked(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
    nowait: bool,
) -> *mut PgStat_EntryRef {
    let entry_ref: *mut PgStat_EntryRef;

    /* find shared table stats entry corresponding to the local entry */
    entry_ref = pgstat_get_entry_ref(kind, dboid, objid, true, null_mut());

    /* lock the shared entry to protect the content, skip if failed */
    if !pgstat_lock_entry(entry_ref, nowait) {
        return null_mut();
    }

    entry_ref
}

pub unsafe fn pgstat_request_entry_refs_gc() {
    pg_atomic_fetch_add_u64(&(*pgStatLocal.shmem).gc_request_count, 1);
}

unsafe fn pgstat_need_entry_refs_gc() -> bool {
    let curage: u64;

    if pgStatEntryRefHash.is_null() {
        return false;
    }

    /* should have been initialized when creating pgStatEntryRefHash */
    Assert!(pgStatSharedRefAge != 0);

    curage = pg_atomic_read_u64(&(*pgStatLocal.shmem).gc_request_count);

    pgStatSharedRefAge != curage
}

unsafe fn pgstat_gc_entry_refs() {
    let curage: u64;

    curage = pg_atomic_read_u64(&(*pgStatLocal.shmem).gc_request_count);
    Assert!(curage != 0);

    /*
     * Some entries have been dropped or reinitialized.  Invalidate cache
     * pointer to them.
     */
    let len = (*pgStatEntryRefHash).entries.len();
    let mut idx = 0;
    while idx < len {
        // Collect key + entry_ref without holding a borrow across the delete call.
        let (key, entry_ref_ptr) = match &(&(*pgStatEntryRefHash).entries)[idx] {
            Some(e) => (e.key, e.entry_ref),
            None => {
                idx += 1;
                continue;
            }
        };

        let entry_ref: *mut PgStat_EntryRef = entry_ref_ptr;

        Assert!(
            (*entry_ref).shared_stats.is_null()
                || (*(*entry_ref).shared_stats).magic == 0xdeadbeef
        );

        /*
         * "generation" checks for the case of entries being reinitialized,
         * and "dropped" for the case where these are..  dropped.
         */
        if !(*(*entry_ref).shared_entry).dropped
            && pg_atomic_read_u32(&(*(*entry_ref).shared_entry).generation)
                == (*entry_ref).generation
        {
            idx += 1;
            continue;
        }

        /* cannot gc shared ref that has pending data */
        if !(*entry_ref).pending.is_null() {
            idx += 1;
            continue;
        }

        pgstat_release_entry_ref(key, entry_ref, false);
        // Note: after release, the slot is now None; idx stays so we
        // re-examine the same index (no entries shift in a linear-probe table
        // on removal -- our simple delete leaves a None tombstone).
        idx += 1;
    }

    pgStatSharedRefAge = curage;
}

unsafe fn pgstat_release_matching_entry_refs(
    discard_pending: bool,
    match_cb: ReleaseMatchCB,
    match_data: Datum,
) {
    if pgStatEntryRefHash.is_null() {
        return;
    }

    let len = (*pgStatEntryRefHash).entries.len();
    let mut idx = 0;
    while idx < len {
        let (key, entry_ref_ptr) = match &(&(*pgStatEntryRefHash).entries)[idx] {
            Some(e) => (e.key, e.entry_ref),
            None => {
                idx += 1;
                continue;
            }
        };
        let cache_entry_ptr: *mut PgStat_EntryRefHashEntry =
            (&mut (*pgStatEntryRefHash).entries)[idx].as_mut().unwrap().as_mut() as *mut _;

        Assert!(!entry_ref_ptr.is_null());

        if let Some(m) = match_cb {
            if !m(cache_entry_ptr, match_data) {
                idx += 1;
                continue;
            }
        }

        pgstat_release_entry_ref(key, entry_ref_ptr, discard_pending);
        idx += 1;
    }
}

/*
 * Release all local references to shared stats entries.
 *
 * When a process exits it cannot do so while still holding references onto
 * stats entries, otherwise the shared stats entries could never be freed.
 */
unsafe fn pgstat_release_all_entry_refs(discard_pending: bool) {
    if pgStatEntryRefHash.is_null() {
        return;
    }

    pgstat_release_matching_entry_refs(discard_pending, None, 0);
    Assert!((*pgStatEntryRefHash).members == 0);
    drop(Box::from_raw(pgStatEntryRefHash));
    pgStatEntryRefHash = null_mut();
}

unsafe fn match_db(ent: *mut PgStat_EntryRefHashEntry, match_data: Datum) -> bool {
    let dboid: Oid = DatumGetObjectId(match_data);

    (*ent).key.dboid == dboid
}

unsafe fn pgstat_release_db_entry_refs(dboid: Oid) {
    pgstat_release_matching_entry_refs(
        /* discard pending = */ true,
        Some(match_db),
        ObjectIdGetDatum(dboid),
    );
}

// ---------------------------------------------------------------------------
// Dropping and resetting of stats entries
// ---------------------------------------------------------------------------

unsafe fn pgstat_free_entry(
    shent: *mut PgStatShared_HashEntry,
    hstat: *mut dshash_seq_status,
) {
    let pdsa: dsa_pointer;

    /*
     * Fetch dsa pointer before deleting entry - that way we can free the
     * memory after releasing the lock.
     */
    pdsa = (*shent).body;

    if hstat.is_null() {
        dshash_delete_entry(shared_hash(pgStatLocal.shared_hash), shent as *mut c_void);
    } else {
        dshash_delete_current(hstat);
    }

    dsa_free(pgStatLocal.dsa, pdsa);
}

/*
 * Helper for both pgstat_drop_database_and_contents() and
 * pgstat_drop_entry().  If hstat is non-null delete the shared entry using
 * dshash_delete_current(), otherwise use dshash_delete_entry().  In either
 * case the entry needs to be already locked.
 */
unsafe fn pgstat_drop_entry_internal(
    shent: *mut PgStatShared_HashEntry,
    hstat: *mut dshash_seq_status,
) -> bool {
    Assert!((*shent).body != InvalidDsaPointer);

    /* should already have released local reference */
    if !pgStatEntryRefHash.is_null() {
        Assert!((*pgStatEntryRefHash).lookup((*shent).key).is_null());
    }

    /*
     * Signal that the entry is dropped - this will eventually cause other
     * backends to release their references.
     */
    if (*shent).dropped {
        elog!(
            ERROR,
            "trying to drop stats entry already dropped: kind={} dboid={} objid={} refcount={} generation={}",
            (*pgstat_get_kind_info((*shent).key.kind)).name as usize, // name is *const c_char; format as ptr for now
            (*shent).key.dboid,
            (*shent).key.objid,
            pg_atomic_read_u32(&(*shent).refcount),
            pg_atomic_read_u32(&(*shent).generation)
        );
    }
    (*shent).dropped = true;

    /* release refcount marking entry as not dropped */
    if pg_atomic_sub_fetch_u32(&(*shent).refcount, 1) == 0 {
        pgstat_free_entry(shent, hstat);
        return true;
    } else {
        if hstat.is_null() {
            dshash_release_lock(shared_hash(pgStatLocal.shared_hash), shent as *mut c_void);
        }
        return false;
    }
}

/// pg_atomic_sub_fetch_u32: atomic subtract, returns new value.
/// TODO(pg-port): real pg_atomic_sub_fetch_u32 lives in port/atomics.h
unsafe fn pg_atomic_sub_fetch_u32(ptr: &pg_atomic_uint32, sub_: i32) -> u32 {
    let old = pg_atomic_fetch_sub_u32(ptr, sub_);
    old.wrapping_sub(sub_ as u32)
}

/*
 * Drop stats for the database and all the objects inside that database.
 */
unsafe fn pgstat_drop_database_and_contents(dboid: Oid) {
    let mut hstat: dshash_seq_status = core::mem::zeroed();
    let mut p: *mut PgStatShared_HashEntry;
    let mut not_freed_count: u64 = 0;

    Assert!(OidIsValid(dboid));

    Assert!(!pgStatLocal.shared_hash.is_null());

    /*
     * This backend might very well be the only backend holding a reference
     * to about-to-be-dropped entries.  Ensure that we're not preventing it
     * from being cleaned up till later.
     *
     * Doing this separately from the dshash iteration below avoids having to
     * do so while holding a partition lock on the shared hashtable.
     */
    pgstat_release_db_entry_refs(dboid);

    /* some of the dshash entries are to be removed, take exclusive lock. */
    dshash_seq_init(&mut hstat, shared_hash(pgStatLocal.shared_hash), true);
    loop {
        p = dshash_seq_next(&mut hstat) as *mut PgStatShared_HashEntry;
        if p.is_null() {
            break;
        }

        if (*p).dropped {
            continue;
        }

        if (*p).key.dboid != dboid {
            continue;
        }

        if !pgstat_drop_entry_internal(p, &mut hstat) {
            /*
             * Even statistics for a dropped database might currently be
             * accessed (consider e.g. database stats for pg_stat_database).
             */
            not_freed_count += 1;
        }
    }
    dshash_seq_term(&mut hstat);

    /*
     * If some of the stats data could not be freed, signal the reference
     * holders to run garbage collection of their cached pgStatLocal.shmem.
     */
    if not_freed_count > 0 {
        pgstat_request_entry_refs_gc();
    }
}

/*
 * Drop a single stats entry.
 *
 * This routine returns false if the stats entry of the dropped object could
 * not be freed, true otherwise.
 *
 * The callers of this function should call pgstat_request_entry_refs_gc()
 * if the stats entry could not be freed, to ensure that this entry's memory
 * can be reclaimed later by a different backend calling
 * pgstat_gc_entry_refs().
 */
pub unsafe fn pgstat_drop_entry(kind: PgStat_Kind, dboid: Oid, objid: u64) -> bool {
    let mut key: PgStat_HashKey = core::mem::zeroed();
    let shent: *mut PgStatShared_HashEntry;
    let mut freed: bool = true;

    /* clear padding */
    core::ptr::write_bytes(&mut key as *mut PgStat_HashKey as *mut u8, 0,
                           core::mem::size_of::<PgStat_HashKey>());

    key.kind = kind;
    key.dboid = dboid;
    key.objid = objid;

    /* delete local reference */
    if !pgStatEntryRefHash.is_null() {
        let lohashent = (*pgStatEntryRefHash).lookup(key);

        if !lohashent.is_null() {
            pgstat_release_entry_ref((*lohashent).key, (*lohashent).entry_ref, true);
        }
    }

    /* mark entry in shared hashtable as deleted, drop if possible */
    shent = dshash_find(
        shared_hash(pgStatLocal.shared_hash),
        &key as *const PgStat_HashKey as *const c_void,
        true,
    ) as *mut PgStatShared_HashEntry;
    if !shent.is_null() {
        freed = pgstat_drop_entry_internal(shent, null_mut());

        /*
         * Database stats contain other stats.  Drop those as well when
         * dropping the database.  XXX: Perhaps this should be done in a
         * slightly more principled way?  But not obvious what that'd look
         * like, and so far this is the only case...
         */
        if key.kind == PGSTAT_KIND_DATABASE {
            pgstat_drop_database_and_contents(key.dboid);
        }
    }

    freed
}

/*
 * Scan through the shared hashtable of stats, dropping statistics if
 * approved by the optional do_drop() function.
 */
pub unsafe fn pgstat_drop_matching_entries(
    do_drop: Option<unsafe extern "C" fn(*mut PgStatShared_HashEntry, Datum) -> bool>,
    match_data: Datum,
) {
    let mut hstat: dshash_seq_status = core::mem::zeroed();
    let mut ps: *mut PgStatShared_HashEntry;
    let mut not_freed_count: u64 = 0;

    /* entries are removed, take an exclusive lock */
    dshash_seq_init(&mut hstat, shared_hash(pgStatLocal.shared_hash), true);
    loop {
        ps = dshash_seq_next(&mut hstat) as *mut PgStatShared_HashEntry;
        if ps.is_null() {
            break;
        }

        if (*ps).dropped {
            continue;
        }

        if let Some(f) = do_drop {
            if !f(ps, match_data) {
                continue;
            }
        }

        /* delete local reference */
        if !pgStatEntryRefHash.is_null() {
            let lohashent = (*pgStatEntryRefHash).lookup((*ps).key);

            if !lohashent.is_null() {
                pgstat_release_entry_ref((*lohashent).key, (*lohashent).entry_ref, true);
            }
        }

        if !pgstat_drop_entry_internal(ps, &mut hstat) {
            not_freed_count += 1;
        }
    }
    dshash_seq_term(&mut hstat);

    if not_freed_count > 0 {
        pgstat_request_entry_refs_gc();
    }
}

/*
 * Scan through the shared hashtable of stats and drop all entries.
 */
pub unsafe fn pgstat_drop_all_entries() {
    pgstat_drop_matching_entries(None, 0);
}

unsafe fn shared_stat_reset_contents(
    kind: PgStat_Kind,
    header: *mut PgStatShared_Common,
    ts: TimestampTz,
) {
    let kind_info = pgstat_get_kind_info(kind);

    core::ptr::write_bytes(
        pgstat_get_entry_data(kind, header) as *mut u8,
        0,
        pgstat_get_entry_len(kind),
    );

    if let Some(cb) = (*kind_info).reset_timestamp_cb {
        cb(header, ts);
    }
}

/*
 * Reset one variable-numbered stats entry.
 */
pub unsafe fn pgstat_reset_entry(
    kind: PgStat_Kind,
    dboid: Oid,
    objid: u64,
    ts: TimestampTz,
) {
    let entry_ref: *mut PgStat_EntryRef;

    Assert!(!(*pgstat_get_kind_info(kind)).fixed_amount());

    entry_ref = pgstat_get_entry_ref(kind, dboid, objid, false, null_mut());
    if entry_ref.is_null() || (*(*entry_ref).shared_entry).dropped {
        return;
    }

    let _ = pgstat_lock_entry(entry_ref, false);
    shared_stat_reset_contents(kind, (*entry_ref).shared_stats, ts);
    pgstat_unlock_entry(entry_ref);
}

/*
 * Scan through the shared hashtable of stats, resetting statistics if
 * approved by the provided do_reset() function.
 */
pub unsafe fn pgstat_reset_matching_entries(
    do_reset: Option<unsafe extern "C" fn(*mut PgStatShared_HashEntry, Datum) -> bool>,
    match_data: Datum,
    ts: TimestampTz,
) {
    let mut hstat: dshash_seq_status = core::mem::zeroed();
    let mut p: *mut PgStatShared_HashEntry;

    /* dshash entry is not modified, take shared lock */
    dshash_seq_init(&mut hstat, shared_hash(pgStatLocal.shared_hash), false);
    loop {
        p = dshash_seq_next(&mut hstat) as *mut PgStatShared_HashEntry;
        if p.is_null() {
            break;
        }

        let header: *mut PgStatShared_Common;

        if (*p).dropped {
            continue;
        }

        if let Some(f) = do_reset {
            if !f(p, match_data) {
                continue;
            }
        } else {
            // NULL do_reset means reset everything (not used by this helper,
            // but match the C pattern: only skip if callback returns false).
        }

        header =
            dsa_get_address(pgStatLocal.dsa, (*p).body) as *mut PgStatShared_Common;

        LWLockAcquire(&mut (*header).lock, LW_EXCLUSIVE);

        shared_stat_reset_contents((*p).key.kind, header, ts);

        LWLockRelease(&mut (*header).lock);
    }
    dshash_seq_term(&mut hstat);
}

unsafe extern "C" fn match_kind(p: *mut PgStatShared_HashEntry, match_data: Datum) -> bool {
    (*p).key.kind == DatumGetInt32(match_data) as PgStat_Kind
}

pub unsafe fn pgstat_reset_entries_of_kind(kind: PgStat_Kind, ts: TimestampTz) {
    pgstat_reset_matching_entries(Some(match_kind), Int32GetDatum(kind as int32), ts);
}

unsafe fn pgstat_setup_memcxt() {
    if pgStatSharedRefContext.is_null() {
        pgStatSharedRefContext = AllocSetContextCreate!(
            TopMemoryContext,
            "PgStat Shared Ref",
            ALLOCSET_SMALL_SIZES
        );
    }
    if pgStatEntryRefHashContext.is_null() {
        pgStatEntryRefHashContext = AllocSetContextCreate!(
            TopMemoryContext,
            "PgStat Shared Ref Hash",
            ALLOCSET_SMALL_SIZES
        );
    }
}

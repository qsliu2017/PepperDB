//! src/backend/utils/misc/injection_point.c
//!
//! Routines to control and run injection points in the code.
//!
//! Injection points can be used to run arbitrary code by attaching callbacks
//! that would be executed in place of the named injection point.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/misc/injection_point.c

use crate::prelude::*;
use crate::pg_config_manual::MAXPGPATH;
use crate::miscadmin::pkglib_path;

use std::ffi::{c_char, c_int, c_void};

/*
 * Typedef for callback function launched by an injection point.
 *
 * src/include/utils/injection_point.h
 */
pub type InjectionPointCallback =
    Option<unsafe extern "C" fn(name: *const c_char, private_data: *const c_void, arg: *mut c_void)>;

/* Field sizes */
const INJ_NAME_MAXLEN: usize = 64;
const INJ_LIB_MAXLEN: usize = 128;
const INJ_FUNC_MAXLEN: usize = 128;
const INJ_PRIVATE_MAXLEN: usize = 1024;

/* Single injection point stored in shared memory */
#[repr(C)]
pub struct InjectionPointEntry {
    /*
     * Because injection points need to be usable without LWLocks, we use a
     * generation counter on each entry to allow safe, lock-free reading.
     *
     * To read an entry, first read the current 'generation' value.  If it's
     * even, then the slot is currently unused, and odd means it's in use.
     * When reading the other fields, beware that they may change while
     * reading them, if the entry is released and reused!  After reading the
     * other fields, read 'generation' again: if its value hasn't changed, you
     * can be certain that the other fields you read are valid.  Otherwise,
     * the slot was concurrently recycled, and you should ignore it.
     *
     * When adding an entry, you must store all the other fields first, and
     * then update the generation number, with an appropriate memory barrier
     * in between. In addition to that protocol, you must also hold
     * InjectionPointLock, to prevent two backends from modifying the array at
     * the same time.
     */
    pub generation: pg_atomic_uint64,

    pub name: [c_char; INJ_NAME_MAXLEN],         /* point name */
    pub library: [c_char; INJ_LIB_MAXLEN],       /* library */
    pub function: [c_char; INJ_FUNC_MAXLEN],     /* function */

    /*
     * Opaque data area that modules can use to pass some custom data to
     * callbacks, registered when attached.
     */
    pub private_data: [c_char; INJ_PRIVATE_MAXLEN],
}

const MAX_INJECTION_POINTS: usize = 128;

/*
 * Shared memory array of active injection points.
 *
 * 'max_inuse' is the highest index currently in use, plus one.  It's just an
 * optimization to avoid scanning through the whole entry, in the common case
 * that there are no injection points, or only a few.
 */
#[repr(C)]
pub struct InjectionPointsCtl {
    pub max_inuse: pg_atomic_uint32,
    pub entries: [InjectionPointEntry; MAX_INJECTION_POINTS],
}

#[allow(non_upper_case_globals)]
pub static mut ActiveInjectionPoints: *mut InjectionPointsCtl = std::ptr::null_mut();

/*
 * Backend local cache of injection callbacks already loaded, stored in
 * TopMemoryContext.
 */
#[repr(C)]
pub struct InjectionPointCacheEntry {
    pub name: [c_char; INJ_NAME_MAXLEN],
    pub private_data: [c_char; INJ_PRIVATE_MAXLEN],
    pub callback: InjectionPointCallback,

    /*
     * Shmem slot and copy of its generation number when this cache entry was
     * created.  They can be used to validate if the cached entry is still
     * valid.
     */
    pub slot_idx: c_int,
    pub generation: uint64,
}

static mut InjectionPointCache: *mut HTAB = std::ptr::null_mut();

/*
 * injection_point_cache_add
 *
 * Add an injection point to the local cache.
 */
unsafe fn injection_point_cache_add(
    name: *const c_char,
    slot_idx: c_int,
    generation: uint64,
    callback: InjectionPointCallback,
    private_data: *const c_void,
) -> *mut InjectionPointCacheEntry {
    let entry: *mut InjectionPointCacheEntry;
    let mut found: bool = false;

    /* If first time, initialize */
    if InjectionPointCache.is_null() {
        let mut hash_ctl: HASHCTL = std::mem::zeroed();

        hash_ctl.keysize = std::mem::size_of::<[c_char; INJ_NAME_MAXLEN]>();
        hash_ctl.entrysize = std::mem::size_of::<InjectionPointCacheEntry>();
        hash_ctl.hcxt = TopMemoryContext;

        InjectionPointCache = hash_create(
            c"InjectionPoint cache hash".as_ptr(),
            MAX_INJECTION_POINTS as c_long,
            &mut hash_ctl,
            HASH_ELEM | HASH_STRINGS | HASH_CONTEXT,
        );
    }

    entry = hash_search(
        InjectionPointCache,
        name as *const c_void,
        HASHACTION::HASH_ENTER,
        &mut found,
    ) as *mut InjectionPointCacheEntry;

    Assert(!found);
    strlcpy(
        (*entry).name.as_mut_ptr(),
        name,
        std::mem::size_of_val(&(*entry).name),
    );
    (*entry).slot_idx = slot_idx;
    (*entry).generation = generation;
    (*entry).callback = callback;
    libc_memcpy(
        (*entry).private_data.as_mut_ptr() as *mut c_void,
        private_data,
        INJ_PRIVATE_MAXLEN,
    );

    entry
}

/*
 * injection_point_cache_remove
 *
 * Remove entry from the local cache.  Note that this leaks a callback
 * loaded but removed later on, which should have no consequence from
 * a testing perspective.
 */
unsafe fn injection_point_cache_remove(name: *const c_char) {
    let mut found: bool = false;

    let _ = hash_search(
        InjectionPointCache,
        name as *const c_void,
        HASHACTION::HASH_REMOVE,
        &mut found,
    );
    Assert(found);
}

/*
 * injection_point_cache_load
 *
 * Load an injection point into the local cache.
 */
unsafe fn injection_point_cache_load(
    entry: *mut InjectionPointEntry,
    slot_idx: c_int,
    generation: uint64,
) -> *mut InjectionPointCacheEntry {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let injection_callback_local: *mut c_void;

    snprintf(
        path.as_mut_ptr(),
        MAXPGPATH,
        c"%s/%s%s".as_ptr(),
        pkglib_path.as_ptr(),
        (*entry).library.as_ptr(),
        DLSUFFIX.as_ptr(),
    );

    if !pg_file_exists(path.as_ptr()) {
        elog!(
            ERROR,
            "could not find library \"{}\" for injection point \"{}\"",
            cstr_to_string(path.as_ptr()),
            cstr_to_string((*entry).name.as_ptr())
        );
        unreachable!();
    }

    injection_callback_local = load_external_function(
        path.as_ptr(),
        (*entry).function.as_ptr(),
        false,
        std::ptr::null_mut(),
    ) as *mut c_void;

    if injection_callback_local.is_null() {
        elog!(
            ERROR,
            "could not find function \"{}\" in library \"{}\" for injection point \"{}\"",
            cstr_to_string((*entry).function.as_ptr()),
            cstr_to_string(path.as_ptr()),
            cstr_to_string((*entry).name.as_ptr())
        );
        unreachable!();
    }

    /* add it to the local cache */
    injection_point_cache_add(
        (*entry).name.as_ptr(),
        slot_idx,
        generation,
        std::mem::transmute::<*mut c_void, InjectionPointCallback>(injection_callback_local),
        (*entry).private_data.as_ptr() as *const c_void,
    )
}

/*
 * injection_point_cache_get
 *
 * Retrieve an injection point from the local cache, if any.
 */
unsafe fn injection_point_cache_get(name: *const c_char) -> *mut InjectionPointCacheEntry {
    let mut found: bool = false;
    let entry: *mut InjectionPointCacheEntry;

    /* no callback if no cache yet */
    if InjectionPointCache.is_null() {
        return std::ptr::null_mut();
    }

    entry = hash_search(
        InjectionPointCache,
        name as *const c_void,
        HASHACTION::HASH_FIND,
        &mut found,
    ) as *mut InjectionPointCacheEntry;

    if found {
        return entry;
    }

    std::ptr::null_mut()
}

/*
 * Return the space for dynamic shared hash table.
 */
pub unsafe fn InjectionPointShmemSize() -> Size {
    let mut sz: Size = 0;

    sz = add_size(sz, std::mem::size_of::<InjectionPointsCtl>());
    sz
}

/*
 * Allocate shmem space for dynamic shared hash.
 */
pub unsafe fn InjectionPointShmemInit() {
    let mut found: bool = false;

    ActiveInjectionPoints = ShmemInitStruct(
        c"InjectionPoint hash".as_ptr(),
        std::mem::size_of::<InjectionPointsCtl>(),
        &mut found,
    ) as *mut InjectionPointsCtl;
    if !IsUnderPostmaster {
        Assert(!found);
        pg_atomic_init_u32(&mut (*ActiveInjectionPoints).max_inuse, 0);
        for i in 0..MAX_INJECTION_POINTS {
            pg_atomic_init_u64(&mut (*ActiveInjectionPoints).entries[i].generation, 0);
        }
    } else {
        Assert(found);
    }
}

/*
 * Attach a new injection point.
 */
pub unsafe fn InjectionPointAttach(
    name: *const c_char,
    library: *const c_char,
    function: *const c_char,
    private_data: *const c_void,
    private_data_size: c_int,
) {
    let mut entry: *mut InjectionPointEntry;
    let mut generation: uint64;
    let max_inuse: uint32;
    let mut free_idx: c_int;

    if strlen(name) >= INJ_NAME_MAXLEN {
        elog!(
            ERROR,
            "injection point name {} too long (maximum of {} characters)",
            cstr_to_string(name),
            INJ_NAME_MAXLEN - 1
        );
        unreachable!();
    }
    if strlen(library) >= INJ_LIB_MAXLEN {
        elog!(
            ERROR,
            "injection point library {} too long (maximum of {} characters)",
            cstr_to_string(library),
            INJ_LIB_MAXLEN - 1
        );
        unreachable!();
    }
    if strlen(function) >= INJ_FUNC_MAXLEN {
        elog!(
            ERROR,
            "injection point function {} too long (maximum of {} characters)",
            cstr_to_string(function),
            INJ_FUNC_MAXLEN - 1
        );
        unreachable!();
    }
    if private_data_size as usize > INJ_PRIVATE_MAXLEN {
        elog!(
            ERROR,
            "injection point data too long (maximum of {} bytes)",
            INJ_PRIVATE_MAXLEN
        );
        unreachable!();
    }

    /*
     * Allocate and register a new injection point.  A new point should not
     * exist.  For testing purposes this should be fine.
     */
    LWLockAcquire(InjectionPointLock, LWLockMode::LW_EXCLUSIVE);
    max_inuse = pg_atomic_read_u32(&mut (*ActiveInjectionPoints).max_inuse);
    free_idx = -1;

    for idx in 0..max_inuse as c_int {
        entry = &mut (*ActiveInjectionPoints).entries[idx as usize];
        generation = pg_atomic_read_u64(&mut (*entry).generation);
        if generation % 2 == 0 {
            /*
             * Found a free slot where we can add the new entry, but keep
             * going so that we will find out if the entry already exists.
             */
            if free_idx == -1 {
                free_idx = idx;
            }
        } else if strcmp((*entry).name.as_ptr(), name) == 0 {
            elog!(
                ERROR,
                "injection point \"{}\" already defined",
                cstr_to_string(name)
            );
            unreachable!();
        }
    }
    if free_idx == -1 {
        if max_inuse as usize == MAX_INJECTION_POINTS {
            elog!(ERROR, "too many injection points");
            unreachable!();
        }
        free_idx = max_inuse as c_int;
    }
    entry = &mut (*ActiveInjectionPoints).entries[free_idx as usize];
    generation = pg_atomic_read_u64(&mut (*entry).generation);
    Assert(generation % 2 == 0);

    /* Save the entry */
    strlcpy(
        (*entry).name.as_mut_ptr(),
        name,
        std::mem::size_of_val(&(*entry).name),
    );
    (*entry).name[INJ_NAME_MAXLEN - 1] = b'\0' as c_char;
    strlcpy(
        (*entry).library.as_mut_ptr(),
        library,
        std::mem::size_of_val(&(*entry).library),
    );
    (*entry).library[INJ_LIB_MAXLEN - 1] = b'\0' as c_char;
    strlcpy(
        (*entry).function.as_mut_ptr(),
        function,
        std::mem::size_of_val(&(*entry).function),
    );
    (*entry).function[INJ_FUNC_MAXLEN - 1] = b'\0' as c_char;
    if !private_data.is_null() {
        libc_memcpy(
            (*entry).private_data.as_mut_ptr() as *mut c_void,
            private_data,
            private_data_size as usize,
        );
    }

    pg_write_barrier();
    pg_atomic_write_u64(&mut (*entry).generation, generation + 1);

    if free_idx + 1 > max_inuse as c_int {
        pg_atomic_write_u32(
            &mut (*ActiveInjectionPoints).max_inuse,
            (free_idx + 1) as uint32,
        );
    }

    LWLockRelease(InjectionPointLock);
}

/*
 * Detach an existing injection point.
 *
 * Returns true if the injection point was detached, false otherwise.
 */
pub unsafe fn InjectionPointDetach(name: *const c_char) -> bool {
    let mut found: bool = false;
    let mut idx: c_int;
    let max_inuse: c_int;

    LWLockAcquire(InjectionPointLock, LWLockMode::LW_EXCLUSIVE);

    /* Find it in the shmem array, and mark the slot as unused */
    max_inuse = pg_atomic_read_u32(&mut (*ActiveInjectionPoints).max_inuse) as c_int;
    idx = max_inuse - 1;
    while idx >= 0 {
        let entry: *mut InjectionPointEntry = &mut (*ActiveInjectionPoints).entries[idx as usize];
        let generation: uint64;

        generation = pg_atomic_read_u64(&mut (*entry).generation);
        if generation % 2 == 0 {
            idx -= 1;
            continue; /* empty slot */
        }

        if strcmp((*entry).name.as_ptr(), name) == 0 {
            Assert(!found);
            found = true;
            pg_atomic_write_u64(&mut (*entry).generation, generation + 1);
            break;
        }
        idx -= 1;
    }

    /* If we just removed the highest-numbered entry, update 'max_inuse' */
    if found && idx == max_inuse - 1 {
        while idx >= 0 {
            let entry: *mut InjectionPointEntry =
                &mut (*ActiveInjectionPoints).entries[idx as usize];
            let generation: uint64;

            generation = pg_atomic_read_u64(&mut (*entry).generation);
            if generation % 2 != 0 {
                break;
            }
            idx -= 1;
        }
        pg_atomic_write_u32(&mut (*ActiveInjectionPoints).max_inuse, (idx + 1) as uint32);
    }
    LWLockRelease(InjectionPointLock);

    found
}

/*
 * Common workhorse of InjectionPointRun() and InjectionPointLoad()
 *
 * Checks if an injection point exists in shared memory, and update
 * the local cache entry accordingly.
 */
unsafe fn InjectionPointCacheRefresh(name: *const c_char) -> *mut InjectionPointCacheEntry {
    let max_inuse: uint32;
    let namelen: c_int;
    let mut local_copy: InjectionPointEntry = std::mem::zeroed();
    let mut cached: *mut InjectionPointCacheEntry;

    /*
     * First read the number of in-use slots.  More entries can be added or
     * existing ones can be removed while we're reading them.  If the entry
     * we're looking for is concurrently added or removed, we might or might
     * not see it.  That's OK.
     */
    max_inuse = pg_atomic_read_u32(&mut (*ActiveInjectionPoints).max_inuse);
    if max_inuse == 0 {
        if !InjectionPointCache.is_null() {
            hash_destroy(InjectionPointCache);
            InjectionPointCache = std::ptr::null_mut();
        }
        return std::ptr::null_mut();
    }

    /*
     * If we have this entry in the local cache already, check if the cached
     * entry is still valid.
     */
    cached = injection_point_cache_get(name);
    if !cached.is_null() {
        let idx = (*cached).slot_idx;
        let entry: *mut InjectionPointEntry = &mut (*ActiveInjectionPoints).entries[idx as usize];

        if pg_atomic_read_u64(&mut (*entry).generation) == (*cached).generation {
            /* still good */
            return cached;
        }
        injection_point_cache_remove(name);
        cached = std::ptr::null_mut();
        let _ = cached;
    }

    /*
     * Search the shared memory array.
     *
     * It's possible that the entry we're looking for is concurrently detached
     * or attached.  Or detached *and* re-attached, to the same slot or a
     * different slot.  Detach and re-attach is not an atomic operation, so
     * it's OK for us to return the old value, NULL, or the new value in such
     * cases.
     */
    namelen = strlen(name) as c_int;
    for idx in 0..max_inuse as c_int {
        let entry: *mut InjectionPointEntry = &mut (*ActiveInjectionPoints).entries[idx as usize];
        let generation: uint64;

        /*
         * Read the generation number so that we can detect concurrent
         * modifications.  The read barrier ensures that the generation number
         * is loaded before any of the other fields.
         */
        generation = pg_atomic_read_u64(&mut (*entry).generation);
        if generation % 2 == 0 {
            continue; /* empty slot */
        }
        pg_read_barrier();

        /* Is this the injection point we're looking for? */
        if libc_memcmp(
            (*entry).name.as_ptr() as *const c_void,
            name as *const c_void,
            (namelen + 1) as usize,
        ) != 0
        {
            continue;
        }

        /*
         * The entry can change at any time, if the injection point is
         * concurrently detached.  Copy it to local memory, and re-check the
         * generation.  If the generation hasn't changed, we know our local
         * copy is coherent.
         */
        libc_memcpy(
            &mut local_copy as *mut InjectionPointEntry as *mut c_void,
            entry as *const c_void,
            std::mem::size_of::<InjectionPointEntry>(),
        );

        pg_read_barrier();
        if pg_atomic_read_u64(&mut (*entry).generation) != generation {
            /*
             * The entry was concurrently detached.
             *
             * Continue the search, because if the generation number changed,
             * we cannot trust the result of the name comparison we did above.
             * It's theoretically possible that it falsely matched a mixed-up
             * state of the old and new name, if the slot was recycled with a
             * different name.
             */
            continue;
        }

        /* Success! Load it into the cache and return it */
        return injection_point_cache_load(&mut local_copy, idx, generation);
    }
    std::ptr::null_mut()
}

/*
 * Load an injection point into the local cache.
 *
 * This is useful to be able to load an injection point before running it,
 * especially if the injection point is called in a code path where memory
 * allocations cannot happen, like critical sections.
 */
pub unsafe fn InjectionPointLoad(name: *const c_char) {
    InjectionPointCacheRefresh(name);
}

/*
 * Execute an injection point, if defined.
 */
pub unsafe fn InjectionPointRun(name: *const c_char, arg: *mut c_void) {
    let cache_entry: *mut InjectionPointCacheEntry;

    cache_entry = InjectionPointCacheRefresh(name);
    if !cache_entry.is_null() {
        ((*cache_entry).callback.unwrap())(
            name,
            (*cache_entry).private_data.as_ptr() as *const c_void,
            arg,
        );
    }
}

/*
 * Execute an injection point directly from the cache, if defined.
 */
pub unsafe fn InjectionPointCached(name: *const c_char, arg: *mut c_void) {
    let cache_entry: *mut InjectionPointCacheEntry;

    cache_entry = injection_point_cache_get(name);
    if !cache_entry.is_null() {
        ((*cache_entry).callback.unwrap())(
            name,
            (*cache_entry).private_data.as_ptr() as *const c_void,
            arg,
        );
    }
}

/*
 * Test if an injection point is defined.
 */
pub unsafe fn IsInjectionPointAttached(name: *const c_char) -> bool {
    !InjectionPointCacheRefresh(name).is_null()
}

/* ---- local stubs for unported helpers / externs ---- */

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

unsafe fn libc_memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void {
    std::ptr::copy_nonoverlapping(src as *const u8, dst as *mut u8, n);
    dst
}

unsafe fn libc_memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int {
    for i in 0..n {
        let av = *(a as *const u8).add(i);
        let bv = *(b as *const u8).add(i);
        if av != bv {
            return av as c_int - bv as c_int;
        }
    }
    0
}

unsafe fn cstr_to_string(s: *const c_char) -> std::string::String {
    if s.is_null() {
        return std::string::String::new();
    }
    std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

#[allow(non_camel_case_types)]
pub struct pg_atomic_uint32 {
    pub value: u32,
}
#[allow(non_camel_case_types)]
pub struct pg_atomic_uint64 {
    pub value: u64,
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct HTAB {
    _private: [u8; 0],
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub struct HASHCTL {
    pub keysize: usize,
    pub entrysize: usize,
    pub hcxt: MemoryContext,
}

#[allow(non_camel_case_types)]
#[repr(C)]
pub enum HASHACTION {
    HASH_FIND,
    HASH_ENTER,
    HASH_REMOVE,
    HASH_ENTER_NULL,
}

pub const HASH_ELEM: c_int = 0x0008;
pub const HASH_STRINGS: c_int = 0x4000;
pub const HASH_CONTEXT: c_int = 0x0040;

#[allow(non_camel_case_types)]
#[repr(C)]
pub enum LWLockMode {
    LW_EXCLUSIVE,
    LW_SHARED,
}

pub static mut InjectionPointLock: *mut c_void = std::ptr::null_mut();

#[allow(non_upper_case_globals)]
pub static IsUnderPostmaster: bool = false;

pub const DLSUFFIX: &std::ffi::CStr = c".so";

unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> *mut HTAB {
    unimplemented!() // TODO: utils/hash/dynahash.c
}

unsafe fn hash_search(
    _hashp: *mut HTAB,
    _key: *const c_void,
    _action: HASHACTION,
    _foundptr: *mut bool,
) -> *mut c_void {
    unimplemented!() // TODO: utils/hash/dynahash.c
}

unsafe fn hash_destroy(_hashp: *mut HTAB) {
    unimplemented!() // TODO: utils/hash/dynahash.c
}

unsafe fn strlcpy(_dst: *mut c_char, _src: *const c_char, _siz: usize) -> usize {
    unimplemented!() // TODO: src/port/strlcpy.c
}

unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2 // TODO: storage/ipc/shmem.c (faithful add w/ overflow check)
}

unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _foundPtr: *mut bool) -> *mut c_void {
    unimplemented!() // TODO: storage/ipc/shmem.c
}

unsafe fn pg_atomic_init_u32(ptr: *mut pg_atomic_uint32, val: u32) {
    (*ptr).value = val;
}

unsafe fn pg_atomic_init_u64(ptr: *mut pg_atomic_uint64, val: u64) {
    (*ptr).value = val;
}

unsafe fn pg_atomic_read_u32(ptr: *mut pg_atomic_uint32) -> u32 {
    (*ptr).value
}

unsafe fn pg_atomic_read_u64(ptr: *mut pg_atomic_uint64) -> u64 {
    (*ptr).value
}

unsafe fn pg_atomic_write_u32(ptr: *mut pg_atomic_uint32, val: u32) {
    (*ptr).value = val;
}

unsafe fn pg_atomic_write_u64(ptr: *mut pg_atomic_uint64, val: u64) {
    (*ptr).value = val;
}

unsafe fn pg_write_barrier() {
    std::sync::atomic::fence(std::sync::atomic::Ordering::Release);
}

unsafe fn pg_read_barrier() {
    std::sync::atomic::fence(std::sync::atomic::Ordering::Acquire);
}

unsafe fn LWLockAcquire(_lock: *mut c_void, _mode: LWLockMode) -> bool {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

unsafe fn LWLockRelease(_lock: *mut c_void) {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

unsafe fn pg_file_exists(_name: *const c_char) -> bool {
    unimplemented!() // TODO: storage/file/fd.c
}

unsafe fn load_external_function(
    _filename: *const c_char,
    _funcname: *const c_char,
    _signalNotFound: bool,
    _filehandle: *mut *mut c_void,
) -> *mut c_void {
    unimplemented!() // TODO: utils/fmgr/dfmgr.c
}

#[allow(non_snake_case)]
unsafe fn Assert(_cond: bool) {}

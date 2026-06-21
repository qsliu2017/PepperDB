//! storage/ipc/dsm_registry.c - dynamic shared memory registry.
//!
//! Provides a way for libraries to use shared memory without requesting it at
//! startup via a shmem_request_hook.  The registry stores DSM segment handles
//! keyed by a library-specified string.

use crate::prelude::*;

// -- Stubbed core types (canonical homes not yet ported) --

// utils/dsa.h
// TODO: utils/dsa.c
pub type dsa_area = c_void;
// TODO: utils/dsa.c - dsa_handle is a dsa_pointer (Size).
pub type dsa_handle = Size;
// TODO: utils/dsa.c
const DSA_HANDLE_INVALID: dsa_handle = 0;

// lib/dshash.h
// TODO: lib/dshash.c
pub type dshash_table = c_void;
// TODO: lib/dshash.c - dshash_table_handle is a dsa_pointer.
pub type dshash_table_handle = Size;
// TODO: lib/dshash.c
const DSHASH_HANDLE_INVALID: dshash_table_handle = 0;

// storage/dsm_impl.h: dsm_handle is uint32.
pub type dsm_segment = c_void;
pub type dsm_handle = uint32;
// TODO: storage/dsm.c - invalid handle sentinel.
const DSM_HANDLE_INVALID: dsm_handle = 0;

// storage/lwlock.h
pub type LWLock = c_void;
// TODO: storage/lwlock.c - LW_EXCLUSIVE mode.
const LW_EXCLUSIVE: c_int = 0;

// storage/lwlock.h: built-in tranche ids (BuiltinTrancheIds).
// TODO: storage/lwlock.c
const LWTRANCHE_DSM_REGISTRY_DSA: c_int = 0;
const LWTRANCHE_DSM_REGISTRY_HASH: c_int = 0;

// lib/dshash.h: hash table tuning/callback parameters.
#[repr(C)]
pub struct dshash_parameters {
    pub key_size: Size,
    pub entry_size: Size,
    pub compare_function: dshash_compare_function,
    pub hash_function: dshash_hash_function,
    pub copy_function: dshash_copy_function,
    pub tranche_id: c_int,
}

// lib/dshash.h: callback function pointer types.
pub type dshash_compare_function =
    unsafe fn(a: *const c_void, b: *const c_void, size: Size, arg: *mut c_void) -> c_int;
pub type dshash_hash_function =
    unsafe fn(v: *const c_void, size: Size, arg: *mut c_void) -> dshash_hash;
pub type dshash_copy_function =
    unsafe fn(dest: *mut c_void, src: *const c_void, size: Size, arg: *mut c_void);
// lib/dshash.h
pub type dshash_hash = uint32;

#[repr(C)]
struct DSMRegistryCtxStruct {
    dsah: dsa_handle,
    dshh: dshash_table_handle,
}

static mut DSMRegistryCtx: *mut DSMRegistryCtxStruct = null_mut();

#[repr(C)]
struct DSMRegistryEntry {
    name: [c_char; 64],
    handle: dsm_handle,
    size: Size,
}

// offsetof(DSMRegistryEntry, handle): the name field occupies the first 64 bytes,
// so the key_size is the byte offset of `handle`.
const DSMREGISTRYENTRY_HANDLE_OFFSET: Size = 64;

// static const dshash_parameters dsh_params
const dsh_params: dshash_parameters = dshash_parameters {
    key_size: DSMREGISTRYENTRY_HANDLE_OFFSET,
    entry_size: core::mem::size_of::<DSMRegistryEntry>() as Size,
    compare_function: dshash_strcmp,
    hash_function: dshash_strhash,
    copy_function: dshash_strcpy,
    tranche_id: LWTRANCHE_DSM_REGISTRY_HASH,
};

static mut dsm_registry_dsa: *mut dsa_area = null_mut();
static mut dsm_registry_table: *mut dshash_table = null_mut();

pub fn DSMRegistryShmemSize() -> Size {
    MAXALIGN(core::mem::size_of::<DSMRegistryCtxStruct>())
}

pub unsafe fn DSMRegistryShmemInit() {
    let mut found: bool = false;

    DSMRegistryCtx = ShmemInitStruct(
        c"DSM Registry Data".as_ptr(),
        DSMRegistryShmemSize(),
        &mut found,
    ) as *mut DSMRegistryCtxStruct;

    if !found {
        (*DSMRegistryCtx).dsah = DSA_HANDLE_INVALID;
        (*DSMRegistryCtx).dshh = DSHASH_HANDLE_INVALID;
    }
}

/*
 * Initialize or attach to the dynamic shared hash table that stores the DSM
 * registry entries, if not already done.  This must be called before accessing
 * the table.
 */
unsafe fn init_dsm_registry() {
    /* Quick exit if we already did this. */
    if !dsm_registry_table.is_null() {
        return;
    }

    /* Otherwise, use a lock to ensure only one process creates the table. */
    LWLockAcquire(DSMRegistryLock, LW_EXCLUSIVE);

    if (*DSMRegistryCtx).dshh == DSHASH_HANDLE_INVALID {
        /* Initialize dynamic shared hash table for registry. */
        dsm_registry_dsa = dsa_create(LWTRANCHE_DSM_REGISTRY_DSA);
        dsm_registry_table = dshash_create(dsm_registry_dsa, &dsh_params, null_mut());

        dsa_pin(dsm_registry_dsa);
        dsa_pin_mapping(dsm_registry_dsa);

        /* Store handles in shared memory for other backends to use. */
        (*DSMRegistryCtx).dsah = dsa_get_handle(dsm_registry_dsa);
        (*DSMRegistryCtx).dshh = dshash_get_hash_table_handle(dsm_registry_table);
    } else {
        /* Attach to existing dynamic shared hash table. */
        dsm_registry_dsa = dsa_attach((*DSMRegistryCtx).dsah);
        dsa_pin_mapping(dsm_registry_dsa);
        dsm_registry_table = dshash_attach(
            dsm_registry_dsa,
            &dsh_params,
            (*DSMRegistryCtx).dshh,
            null_mut(),
        );
    }

    LWLockRelease(DSMRegistryLock);
}

/*
 * Initialize or attach a named DSM segment.
 *
 * This routine returns the address of the segment.  init_callback is called to
 * initialize the segment when it is first created.
 */
pub unsafe fn GetNamedDSMSegment(
    name: *const c_char,
    size: Size,
    init_callback: Option<unsafe fn(ptr: *mut c_void)>,
    found: *mut bool,
) -> *mut c_void {
    let entry: *mut DSMRegistryEntry;
    let oldcontext: MemoryContext;
    let ret: *mut c_void;
    let mut seg: *mut dsm_segment;

    Assert!(!found.is_null());

    if name.is_null() || *name == 0 {
        ereport!(ERROR, "DSM segment name cannot be empty");
    }

    if strlen(name) >= DSMREGISTRYENTRY_HANDLE_OFFSET {
        ereport!(ERROR, "DSM segment name too long");
    }

    if size == 0 {
        ereport!(ERROR, "DSM segment size must be nonzero");
    }

    /* Be sure any local memory allocated by DSM/DSA routines is persistent. */
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    /* Connect to the registry. */
    init_dsm_registry();

    entry =
        dshash_find_or_insert(dsm_registry_table, name as *const c_void, found) as *mut DSMRegistryEntry;
    if !(*found) {
        (*entry).handle = DSM_HANDLE_INVALID;
        (*entry).size = size;
    } else if (*entry).size != size {
        ereport!(
            ERROR,
            "requested DSM segment size does not match size of existing segment"
        );
    }

    if (*entry).handle == DSM_HANDLE_INVALID {
        *found = false;

        /* Initialize the segment. */
        seg = dsm_create(size, 0);

        if let Some(cb) = init_callback {
            cb(dsm_segment_address(seg));
        }

        dsm_pin_segment(seg);
        dsm_pin_mapping(seg);
        (*entry).handle = dsm_segment_handle(seg);
    } else {
        /* If the existing segment is not already attached, attach it now. */
        seg = dsm_find_mapping((*entry).handle);
        if seg.is_null() {
            seg = dsm_attach((*entry).handle);
            if seg.is_null() {
                elog!(ERROR, "could not map dynamic shared memory segment");
            }

            dsm_pin_mapping(seg);
        }
    }

    ret = dsm_segment_address(seg);
    dshash_release_lock(dsm_registry_table, entry as *mut c_void);
    MemoryContextSwitchTo(oldcontext);

    ret
}

// -- Local stubs for not-yet-ported callees --

use crate::backend_link_shims::DSMRegistryLock;

// TODO: storage/ipc/shmem.c
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _found_ptr: *mut bool) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(_name, _size, _found_ptr)
}

// TODO: storage/lwlock.c
unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    crate::storage::lmgr::lwlock::LWLockAcquire(_lock as _, if _mode == 1 { crate::storage::lmgr::lwlock::LWLockMode::LW_SHARED } else { crate::storage::lmgr::lwlock::LWLockMode::LW_EXCLUSIVE })
}

// TODO: storage/lwlock.c
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    crate::storage::lmgr::lwlock::LWLockRelease(_lock as _)
}

// TODO: utils/dsa.c
unsafe fn dsa_create(_tranche_id: c_int) -> *mut dsa_area {
    unimplemented!()
}

// TODO: utils/dsa.c
unsafe fn dsa_attach(_handle: dsa_handle) -> *mut dsa_area {
    crate::utils::mmgr::dsa::dsa_attach(_handle as _) as _
}

// TODO: utils/dsa.c
unsafe fn dsa_pin(_area: *mut dsa_area) {
    crate::utils::mmgr::dsa::dsa_pin(_area as _)
}

// TODO: utils/dsa.c
unsafe fn dsa_pin_mapping(_area: *mut dsa_area) {
    crate::utils::mmgr::dsa::dsa_pin_mapping(_area as _)
}

// TODO: utils/dsa.c
unsafe fn dsa_get_handle(_area: *mut dsa_area) -> dsa_handle {
    crate::utils::mmgr::dsa::dsa_get_handle(_area as _) as _
}

// TODO: lib/dshash.c
unsafe fn dshash_create(
    _area: *mut dsa_area,
    _params: *const dshash_parameters,
    _arg: *mut c_void,
) -> *mut dshash_table {
    crate::lib::dshash::dshash_create(_area as _, _params as _, _arg) as _
}

// TODO: lib/dshash.c
unsafe fn dshash_attach(
    _area: *mut dsa_area,
    _params: *const dshash_parameters,
    _handle: dshash_table_handle,
    _arg: *mut c_void,
) -> *mut dshash_table {
    crate::lib::dshash::dshash_attach(_area as _, _params as _, _handle as _, _arg) as _
}

// TODO: lib/dshash.c
unsafe fn dshash_get_hash_table_handle(_hash_table: *mut dshash_table) -> dshash_table_handle {
    crate::lib::dshash::dshash_get_hash_table_handle(_hash_table as _) as _
}

// TODO: lib/dshash.c
unsafe fn dshash_find_or_insert(
    _hash_table: *mut dshash_table,
    _key: *const c_void,
    _found: *mut bool,
) -> *mut c_void {
    crate::lib::dshash::dshash_find_or_insert(_hash_table as _, _key, _found)
}

// TODO: lib/dshash.c
unsafe fn dshash_release_lock(_hash_table: *mut dshash_table, _entry: *mut c_void) {
    crate::lib::dshash::dshash_release_lock(_hash_table as _, _entry)
}

// TODO: lib/dshash.c
unsafe fn dshash_strcmp(_a: *const c_void, _b: *const c_void, _size: Size, _arg: *mut c_void) -> c_int {
    unimplemented!()
}

// TODO: lib/dshash.c
unsafe fn dshash_strhash(_v: *const c_void, _size: Size, _arg: *mut c_void) -> dshash_hash {
    unimplemented!()
}

// TODO: lib/dshash.c
unsafe fn dshash_strcpy(_dest: *mut c_void, _src: *const c_void, _size: Size, _arg: *mut c_void) {
    unimplemented!()
}

// TODO: storage/dsm.c
unsafe fn dsm_create(_size: Size, _flags: c_int) -> *mut dsm_segment {
    crate::storage::ipc::dsm::dsm_create(_size, _flags) as _
}

// TODO: storage/dsm.c
unsafe fn dsm_attach(_h: dsm_handle) -> *mut dsm_segment {
    crate::storage::ipc::dsm::dsm_attach(_h as _) as _
}

// TODO: storage/dsm.c
unsafe fn dsm_find_mapping(_h: dsm_handle) -> *mut dsm_segment {
    crate::storage::ipc::dsm::dsm_find_mapping(_h as _) as _
}

// TODO: storage/dsm.c
unsafe fn dsm_segment_handle(_seg: *mut dsm_segment) -> dsm_handle {
    crate::storage::ipc::dsm::dsm_segment_handle(_seg as _) as _
}

// TODO: storage/dsm.c
unsafe fn dsm_segment_address(_seg: *mut dsm_segment) -> *mut c_void {
    crate::storage::ipc::dsm::dsm_segment_address(_seg as _)
}

// TODO: storage/dsm.c
unsafe fn dsm_pin_segment(_seg: *mut dsm_segment) {
    crate::storage::ipc::dsm::dsm_pin_segment(_seg as _)
}

// TODO: storage/dsm.c
unsafe fn dsm_pin_mapping(_seg: *mut dsm_segment) {
    crate::storage::ipc::dsm::dsm_pin_mapping(_seg as _)
}

// string.h: bound directly via extern "C".
extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

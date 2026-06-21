//! access/common/session.c - Encapsulation of user session.
//!
//! Intended to contain data that needs to be shared between backends performing
//! work for a client session.  In particular such a session is shared between
//! the leader and worker processes for parallel queries.  Currently used to
//! share the typemod registry for ephemeral row-types (BlessTupleDesc etc).
//!
//! Merged header: postgres/src/include/access/session.h

use crate::prelude::*;

use crate::storage::ipc::shm_toc::{
    shm_toc, shm_toc_estimate, shm_toc_estimate_chunk, shm_toc_estimate_keys,
    shm_toc_estimator, shm_toc_initialize_estimator,
};

// --- Types from companion header access/session.h (and its dependencies) ---

/// storage/dsm.h: opaque dynamic shared memory segment handle.
pub type dsm_segment = c_void;
/// storage/dsm_impl.h: dsm_handle is uint32.
pub type dsm_handle = uint32;
/// utils/dsa.h: opaque dynamic shared area.
pub type dsa_area = c_void;
/// lib/dshash.h: opaque concurrent hash table.
pub type dshash_table = c_void;
/// utils/typcache.h (forward-declared in session.h): opaque shared record
/// typmod registry.
pub type SharedRecordTypmodRegistry = c_void;

/// A struct encapsulating some elements of a user's session.  For now this
/// manages state that applies to parallel query, but in principle it could
/// include other things that are currently global variables.
#[repr(C)]
pub struct Session {
    /// The session-scoped DSM segment.
    pub segment: *mut dsm_segment,
    /// The session-scoped DSA area.
    pub area: *mut dsa_area,

    /* State managed by typcache.c. */
    pub shared_typmod_registry: *mut SharedRecordTypmodRegistry,
    pub shared_record_table: *mut dshash_table,
    pub shared_typmod_table: *mut dshash_table,
}

/* Magic number for per-session DSM TOC. */
const SESSION_MAGIC: uint64 = 0xabb0fbc9;

/*
 * We want to create a DSA area to store shared state that has the same
 * lifetime as a session.  So far, it's only used to hold the shared record
 * type registry.  We don't want it to have to create any DSM segments just
 * yet in common cases, so we'll give it enough space to hold a very small
 * SharedRecordTypmodRegistry.
 */
const SESSION_DSA_SIZE: Size = 0x30000;

/*
 * Magic numbers for state sharing in the per-session DSM area.
 */
const SESSION_KEY_DSA: uint64 = UINT64CONST(0xFFFFFFFFFFFF0001);
const SESSION_KEY_RECORD_TYPMOD_REGISTRY: uint64 = UINT64CONST(0xFFFFFFFFFFFF0002);

/* This backend's current session. */
#[no_mangle]
pub static mut CurrentSession: *mut Session = null_mut();

/*
 * Set up CurrentSession to point to an empty Session object.
 */
#[no_mangle]
pub unsafe extern "C" fn InitializeSession() {
    CurrentSession =
        MemoryContextAllocZero(TopMemoryContext, size_of::<Session>()) as *mut Session;
}

/*
 * Initialize the per-session DSM segment if it isn't already initialized, and
 * return its handle so that worker processes can attach to it.
 *
 * Unlike the per-context DSM segment, this segment and its contents are
 * reused for future parallel queries.
 *
 * Return DSM_HANDLE_INVALID if a segment can't be allocated due to lack of
 * resources.
 */
#[no_mangle]
pub unsafe extern "C" fn GetSessionDsmHandle() -> dsm_handle {
    let mut estimator: shm_toc_estimator = std::mem::zeroed();
    let toc: *mut shm_toc;
    let seg: *mut dsm_segment;
    let typmod_registry_size: usize;
    let size: usize;
    let dsa_space: *mut c_void;
    let typmod_registry_space: *mut c_void;
    let dsa: *mut dsa_area;
    let old_context: MemoryContext;

    /*
     * If we have already created a session-scope DSM segment in this backend,
     * return its handle.  The same segment will be used for the rest of this
     * backend's lifetime.
     */
    if (*CurrentSession).segment != null_mut() {
        return dsm_segment_handle((*CurrentSession).segment);
    }

    /* Otherwise, prepare to set one up. */
    old_context = MemoryContextSwitchTo(TopMemoryContext);
    shm_toc_initialize_estimator(&mut estimator);

    /* Estimate space for the per-session DSA area. */
    shm_toc_estimate_keys(&mut estimator, 1);
    shm_toc_estimate_chunk(&mut estimator, SESSION_DSA_SIZE);

    /* Estimate space for the per-session record typmod registry. */
    typmod_registry_size = SharedRecordTypmodRegistryEstimate();
    shm_toc_estimate_keys(&mut estimator, 1);
    shm_toc_estimate_chunk(&mut estimator, typmod_registry_size);

    /* Set up segment and TOC. */
    size = shm_toc_estimate(&estimator);
    seg = dsm_create(size, DSM_CREATE_NULL_IF_MAXSEGMENTS);
    if seg == null_mut() {
        MemoryContextSwitchTo(old_context);

        return DSM_HANDLE_INVALID;
    }
    toc = shm_toc_create(SESSION_MAGIC, dsm_segment_address(seg), size);

    /* Create per-session DSA area. */
    dsa_space = shm_toc_allocate(toc, SESSION_DSA_SIZE);
    dsa = dsa_create_in_place(dsa_space, SESSION_DSA_SIZE, LWTRANCHE_PER_SESSION_DSA, seg);
    shm_toc_insert(toc, SESSION_KEY_DSA, dsa_space);

    /* Create session-scoped shared record typmod registry. */
    typmod_registry_space = shm_toc_allocate(toc, typmod_registry_size);
    SharedRecordTypmodRegistryInit(
        typmod_registry_space as *mut SharedRecordTypmodRegistry,
        seg,
        dsa,
    );
    shm_toc_insert(toc, SESSION_KEY_RECORD_TYPMOD_REGISTRY, typmod_registry_space);

    /*
     * If we got this far, we can pin the shared memory so it stays mapped for
     * the rest of this backend's life.  If we don't make it this far, cleanup
     * callbacks for anything we installed above (ie currently
     * SharedRecordTypmodRegistry) will run when the DSM segment is detached
     * by CurrentResourceOwner so we aren't left with a broken CurrentSession.
     */
    dsm_pin_mapping(seg);
    dsa_pin_mapping(dsa);

    /* Make segment and area available via CurrentSession. */
    (*CurrentSession).segment = seg;
    (*CurrentSession).area = dsa;

    MemoryContextSwitchTo(old_context);

    dsm_segment_handle(seg)
}

/*
 * Attach to a per-session DSM segment provided by a parallel leader.
 */
#[no_mangle]
pub unsafe extern "C" fn AttachSession(handle: dsm_handle) {
    let seg: *mut dsm_segment;
    let toc: *mut shm_toc;
    let dsa_space: *mut c_void;
    let typmod_registry_space: *mut c_void;
    let dsa: *mut dsa_area;
    let old_context: MemoryContext;

    old_context = MemoryContextSwitchTo(TopMemoryContext);

    /* Attach to the DSM segment. */
    seg = dsm_attach(handle);
    if seg == null_mut() {
        elog!(ERROR, "could not attach to per-session DSM segment");
    }
    toc = shm_toc_attach(SESSION_MAGIC, dsm_segment_address(seg));

    /* Attach to the DSA area. */
    dsa_space = shm_toc_lookup(toc, SESSION_KEY_DSA, false);
    dsa = dsa_attach_in_place(dsa_space, seg);

    /* Make them available via the current session. */
    (*CurrentSession).segment = seg;
    (*CurrentSession).area = dsa;

    /* Attach to the shared record typmod registry. */
    typmod_registry_space =
        shm_toc_lookup(toc, SESSION_KEY_RECORD_TYPMOD_REGISTRY, false);
    SharedRecordTypmodRegistryAttach(typmod_registry_space as *mut SharedRecordTypmodRegistry);

    /* Remain attached until end of backend or DetachSession(). */
    dsm_pin_mapping(seg);
    dsa_pin_mapping(dsa);

    MemoryContextSwitchTo(old_context);
}

/*
 * Detach from the current session DSM segment.  It's not strictly necessary
 * to do this explicitly since we'll detach automatically at backend exit, but
 * if we ever reuse parallel workers it will become important for workers to
 * detach from one session before attaching to another.  Note that this runs
 * detach hooks.
 */
#[no_mangle]
pub unsafe extern "C" fn DetachSession() {
    /* Runs detach hooks. */
    dsm_detach((*CurrentSession).segment);
    (*CurrentSession).segment = null_mut();
    dsa_detach((*CurrentSession).area);
    (*CurrentSession).area = null_mut();
}

// ---------------------------------------------------------------------------
// Local stubs for functions not yet ported.
// ---------------------------------------------------------------------------

/* storage/dsm_impl.h: invalid handle sentinel. */
// TODO: replace with real value from a ported storage/dsm_impl.rs.
const DSM_HANDLE_INVALID: dsm_handle = 0;

/* storage/dsm.h: dsm_create flag. */
// TODO: replace with real value from a ported storage/dsm.rs.
const DSM_CREATE_NULL_IF_MAXSEGMENTS: c_int = 0x0001;

/* storage/lwlock.h: built-in LWLock tranche id. */
// TODO: replace with real value from a ported storage/lwlock.rs (BuiltinTrancheIds).
const LWTRANCHE_PER_SESSION_DSA: c_int = 0;

// TODO: storage/dsm.c
unsafe fn dsm_create(_size: Size, _flags: c_int) -> *mut dsm_segment { unimplemented!() }

// TODO: storage/dsm.c
unsafe fn dsm_attach(_h: dsm_handle) -> *mut dsm_segment { unimplemented!() }

// TODO: storage/dsm.c
unsafe fn dsm_segment_handle(_seg: *mut dsm_segment) -> dsm_handle { unimplemented!() }

// TODO: storage/dsm.c
unsafe fn dsm_segment_address(_seg: *mut dsm_segment) -> *mut c_void { unimplemented!() }

// TODO: storage/dsm.c
unsafe fn dsm_detach(_seg: *mut dsm_segment) { unimplemented!() }

// TODO: storage/dsm.c
unsafe fn dsm_pin_mapping(_seg: *mut dsm_segment) { unimplemented!() }

// TODO: storage/ipc/shm_toc.c (not yet ported alongside the estimator helpers)
unsafe fn shm_toc_create(_magic: uint64, _address: *mut c_void, _nbytes: Size) -> *mut shm_toc { crate::storage::ipc::shm_toc::shm_toc_create(_magic, _address, _nbytes) }

// TODO: storage/ipc/shm_toc.c
unsafe fn shm_toc_attach(_magic: uint64, _address: *mut c_void) -> *mut shm_toc { crate::storage::ipc::shm_toc::shm_toc_attach(_magic, _address) }

// TODO: storage/ipc/shm_toc.c
unsafe fn shm_toc_allocate(_toc: *mut shm_toc, _nbytes: Size) -> *mut c_void { crate::storage::ipc::shm_toc::shm_toc_allocate(_toc, _nbytes) }

// TODO: storage/ipc/shm_toc.c
unsafe fn shm_toc_insert(_toc: *mut shm_toc, _key: uint64, _address: *mut c_void) { crate::storage::ipc::shm_toc::shm_toc_insert(_toc, _key, _address) }

// TODO: storage/ipc/shm_toc.c
unsafe fn shm_toc_lookup(_toc: *mut shm_toc, _key: uint64, _noError: bool) -> *mut c_void { crate::storage::ipc::shm_toc::shm_toc_lookup(_toc, _key, _noError) }

// TODO: utils/dsa.c
unsafe fn dsa_create_in_place(
    _place: *mut c_void,
    _size: Size,
    _tranche_id: c_int,
    _segment: *mut dsm_segment,
) -> *mut dsa_area {
    unimplemented!()
}

// TODO: utils/dsa.c
unsafe fn dsa_attach_in_place(_place: *mut c_void, _segment: *mut dsm_segment) -> *mut dsa_area { unimplemented!() }

// TODO: utils/dsa.c
unsafe fn dsa_pin_mapping(_area: *mut dsa_area) { unimplemented!() }

// TODO: utils/dsa.c
unsafe fn dsa_detach(_area: *mut dsa_area) { unimplemented!() }

// TODO: utils/cache/typcache.c
unsafe fn SharedRecordTypmodRegistryEstimate() -> usize { crate::utils::cache::typcache::SharedRecordTypmodRegistryEstimate() }

// TODO: utils/cache/typcache.c
unsafe fn SharedRecordTypmodRegistryInit(
    _registry: *mut SharedRecordTypmodRegistry,
    _segment: *mut dsm_segment,
    _area: *mut dsa_area,
) { unimplemented!() }

// TODO: utils/cache/typcache.c
unsafe fn SharedRecordTypmodRegistryAttach(_registry: *mut SharedRecordTypmodRegistry) { unimplemented!() }

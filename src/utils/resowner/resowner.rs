//! Translation of postgres/src/backend/utils/resowner/resowner.c
//!                + the public types from postgres/src/include/utils/resowner.h
//!
//! POSTGRES resource owner management. Query-lifespan resources (buffers,
//! files, locks, ...) are tracked by associating them with ResourceOwner
//! objects so they can be freed at the right time (txn / portal / subxact end).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! #include mapping:
//!   "common/hashfn.h"     -> crate::common::hashfn (hash_combine{,64}, murmurhash{32,64})
//!   "common/int.h"        -> crate::common::int    (pg_cmp_u32)
//!   "lib/ilist.h"         -> crate::lib::ilist     (dlist_*)
//!   "storage/aio.h"       -> STUB pgaio_io_release_resowner (aio handle release)
//!   "storage/ipc.h"       -> STUB on_shmem_exit (CreateAuxProcessResourceOwner)
//!   "storage/predicate.h" -> STUB ReleasePredicateLocks
//!   "storage/proc.h"      -> STUB ProcReleaseLocks
//!   "utils/memutils.h"    -> crate::prelude (MemoryContextAllocZero, TopMemoryContext, pfree)
//!   "utils/resowner.h"    -> merged below (public types + fn signatures)
//!
//! lock manager (LockReassignCurrentOwner / LockReleaseCurrentOwner) and the
//! LOCALLOCK type are STUBs (opaque `LOCALLOCK = c_void`); the local-lock cache
//! support and AIO/proc hooks call into subsystems not yet ported -- see the
//! `unimplemented!()`/TODO sites.

use crate::common::hashfn::{hash_combine, hash_combine64, murmurhash32, murmurhash64};
use crate::common::int::pg_cmp_u32;
use crate::lib::ilist::{
    dlist_delete_from, dlist_head, dlist_head_node, dlist_init, dlist_is_empty, dlist_node,
    dlist_push_tail,
};
use crate::prelude::*;

// ---------------------------------------------------------------------------
// STUBs for subsystems not yet ported.
// ---------------------------------------------------------------------------

/// STUB: storage/lock.h LOCALLOCK. Local lock entries are an opaque pointer
/// type here; the lock manager is not yet ported.
pub type LOCALLOCK = c_void;

/// STUB: storage/aio.h. Release an AIO handle owned by a resource owner.
/// TODO(pg-port): wire to the real pgaio_io_release_resowner once storage/aio
/// is translated. For now this would loop forever in ResourceOwnerReleaseInternal
/// if any aio handles were registered, so it is only reached when none are.
unsafe fn pgaio_io_release_resowner(_ioh_node: *mut dlist_node, _on_error: bool) {
    unimplemented!("storage/aio.h pgaio_io_release_resowner not ported");
}

/// STUB: storage/proc.h. Release all locks at top-of-recursion for a top xact.
/// TODO(pg-port): wire to the real lock manager.
unsafe fn ProcReleaseLocks(_is_commit: bool) {
    unimplemented!("storage/proc.h ProcReleaseLocks not ported");
}

/// STUB: storage/predicate.h ReleasePredicateLocks.
/// TODO(pg-port): wire to the real predicate lock manager.
unsafe fn ReleasePredicateLocks(_is_commit: bool, _is_read_only_safe: bool) {
    unimplemented!("storage/predicate.h ReleasePredicateLocks not ported");
}

/// STUB: storage/lock.h LockReassignCurrentOwner. Subxact-commit lock transfer.
/// TODO(pg-port): wire to the real lock manager.
unsafe fn LockReassignCurrentOwner(_locks: *mut *mut LOCALLOCK, _nlocks: c_int) {
    unimplemented!("storage/lock.h LockReassignCurrentOwner not ported");
}

/// STUB: storage/lock.h LockReleaseCurrentOwner. Subxact-abort lock release.
/// TODO(pg-port): wire to the real lock manager.
unsafe fn LockReleaseCurrentOwner(_locks: *mut *mut LOCALLOCK, _nlocks: c_int) {
    unimplemented!("storage/lock.h LockReleaseCurrentOwner not ported");
}

/// STUB: storage/ipc.h on_shmem_exit. Register a shmem-exit callback.
/// TODO(pg-port): wire to the real ipc shutdown callback registry.
unsafe fn on_shmem_exit(_function: unsafe fn(c_int, Datum), _arg: Datum) {
    unimplemented!("storage/ipc.h on_shmem_exit not ported");
}

// ---------------------------------------------------------------------------
// resowner.h public types (merged).
// ---------------------------------------------------------------------------

/// `ResourceOwner` is an opaque pointer to ResourceOwnerData (resowner.h).
pub type ResourceOwner = *mut ResourceOwnerData;

/// Resource releasing is done in three phases: pre-locks, locks, post-locks.
/// The pre-lock phase releases resources visible to other backends (pinned
/// buffers); the post-lock phase is for backend-internal cleanup.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(C)]
pub enum ResourceReleasePhase {
    RESOURCE_RELEASE_BEFORE_LOCKS = 1,
    RESOURCE_RELEASE_LOCKS = 2,
    RESOURCE_RELEASE_AFTER_LOCKS = 3,
}
pub use ResourceReleasePhase::*;

/// Within a phase, resources are released in priority order (just an integer).
pub type ResourceReleasePriority = uint32;

/* priorities of built-in BEFORE_LOCKS resources */
pub const RELEASE_PRIO_BUFFER_IOS: ResourceReleasePriority = 100;
pub const RELEASE_PRIO_BUFFER_PINS: ResourceReleasePriority = 200;
pub const RELEASE_PRIO_RELCACHE_REFS: ResourceReleasePriority = 300;
pub const RELEASE_PRIO_DSMS: ResourceReleasePriority = 400;
pub const RELEASE_PRIO_JIT_CONTEXTS: ResourceReleasePriority = 500;
pub const RELEASE_PRIO_CRYPTOHASH_CONTEXTS: ResourceReleasePriority = 600;
pub const RELEASE_PRIO_HMAC_CONTEXTS: ResourceReleasePriority = 700;

/* priorities of built-in AFTER_LOCKS resources */
pub const RELEASE_PRIO_CATCACHE_REFS: ResourceReleasePriority = 100;
pub const RELEASE_PRIO_CATCACHE_LIST_REFS: ResourceReleasePriority = 200;
pub const RELEASE_PRIO_PLANCACHE_REFS: ResourceReleasePriority = 300;
pub const RELEASE_PRIO_TUPDESC_REFS: ResourceReleasePriority = 400;
pub const RELEASE_PRIO_SNAPSHOT_REFS: ResourceReleasePriority = 500;
pub const RELEASE_PRIO_FILES: ResourceReleasePriority = 600;
pub const RELEASE_PRIO_WAITEVENTSETS: ResourceReleasePriority = 700;

/* 0 is considered invalid */
pub const RELEASE_PRIO_FIRST: ResourceReleasePriority = 1;
pub const RELEASE_PRIO_LAST: ResourceReleasePriority = u32::MAX;

/// In order to track an object, resowner.c needs a few callbacks for it. The
/// callbacks for resources of a specific kind are encapsulated in
/// ResourceOwnerDesc. Note the callbacks occur post-commit/post-abort, so they
/// can only do noncritical cleanup and must not fail.
#[repr(C)]
pub struct ResourceOwnerDesc {
    /// name for the object kind, for debugging.
    pub name: *const c_char,

    /// when are these objects released?
    pub release_phase: ResourceReleasePhase,
    pub release_priority: ResourceReleasePriority,

    /// Release resource. Called for each resource in the owner, in the order
    /// specified by release_phase/release_priority. The resource is implicitly
    /// removed from the owner; the callback need not call ResourceOwnerForget.
    pub ReleaseResource: unsafe fn(res: Datum),

    /// Format a string describing the resource, for debugging. May be None, in
    /// which case a generic "[resource name]: [ptr]" format is used.
    pub DebugPrint: Option<unsafe fn(res: Datum) -> *mut c_char>,
}

/// Dynamically loaded modules can get control during ResourceOwnerRelease by
/// providing a callback of this form.
pub type ResourceReleaseCallback =
    unsafe fn(phase: ResourceReleasePhase, isCommit: bool, isTopLevel: bool, arg: *mut c_void);

// ---------------------------------------------------------------------------
// resowner.c private types & constants.
// ---------------------------------------------------------------------------

/// ResourceElem represents a reference associated with a resource owner. All
/// objects managed here fit in a Datum (pointers or integers).
#[derive(Clone, Copy)]
#[repr(C)]
struct ResourceElem {
    item: Datum,
    /// null indicates a free hash table slot.
    kind: *const ResourceOwnerDesc,
}

/// Size of the fixed-size array to hold most-recently remembered resources.
const RESOWNER_ARRAY_SIZE: usize = 32;

/// Initially allocated size of a ResourceOwner's hash table. Must be a power of
/// two because we use (capacity - 1) as the mask for hashing.
const RESOWNER_HASH_INIT_SIZE: uint32 = 64;

/// How many items may be stored in a hash table of given capacity. When this
/// number is reached, we must resize. Must leave room to copy the fixed array
/// into the hash in ResourceOwnerSort; otherwise 0.75 is a reasonable fill.
#[inline]
fn RESOWNER_HASH_MAX_ITEMS(capacity: uint32) -> uint32 {
    core::cmp::min(
        capacity - RESOWNER_ARRAY_SIZE as uint32,
        capacity / 4 * 3,
    )
}

// StaticAssertDecl(RESOWNER_HASH_MAX_ITEMS(RESOWNER_HASH_INIT_SIZE) >= RESOWNER_ARRAY_SIZE)
const _: () = assert!(
    {
        let cap = RESOWNER_HASH_INIT_SIZE;
        let a = cap - RESOWNER_ARRAY_SIZE as uint32;
        let b = cap / 4 * 3;
        let m = if a < b { a } else { b };
        m >= RESOWNER_ARRAY_SIZE as uint32
    },
    "initial hash size too small compared to array size"
);

/// MAX_RESOWNER_LOCKS is the size of the per-resource-owner locks cache.
const MAX_RESOWNER_LOCKS: usize = 15;

/// ResourceOwner objects look like this (struct ResourceOwnerData).
#[repr(C)]
pub struct ResourceOwnerData {
    /// null if no parent (toplevel owner).
    parent: ResourceOwner,
    /// head of linked list of children.
    firstchild: ResourceOwner,
    /// next child of same parent.
    nextchild: ResourceOwner,
    /// name (just for debugging).
    name: *const c_char,

    /// When ResourceOwnerRelease is called, we sort 'hash'/'arr' by release
    /// priority. After that, no new resources can be remembered/forgotten in
    /// retail. Separate flags because ResourceOwnerReleaseAllOfKind temporarily
    /// sets 'releasing' without sorting.
    releasing: bool,
    /// are 'hash' and 'arr' sorted by priority?
    sorted: bool,

    /// number of owned locks.
    nlocks: uint8,
    /// how many items are stored in the array.
    narr: uint8,
    /// how many items are stored in the hash.
    nhash: uint32,

    /// The fixed-size array for recent resources. If 'sorted', contents are
    /// sorted by release priority.
    arr: [ResourceElem; RESOWNER_ARRAY_SIZE],

    /// The hash table (open-addressing). 'nhash' is the number of items;
    /// enlarged when it would exceed 'grow_at'. If 'sorted', contents are no
    /// longer hashed but sorted by release priority (first 'nhash' occupied).
    hash: *mut ResourceElem,
    /// allocated length of hash[].
    capacity: uint32,
    /// grow hash when reach this.
    grow_at: uint32,

    /// The local locks cache.
    locks: [*mut LOCALLOCK; MAX_RESOWNER_LOCKS],

    /// AIO handles need be registered in critical sections and therefore cannot
    /// use the normal ResourceElem mechanism.
    aio_handles: dlist_head,
}

// ---------------------------------------------------------------------------
// GLOBAL MEMORY
// ---------------------------------------------------------------------------

/// Globally known ResourceOwners. `CurrentResourceOwner` points at the owner
/// currently being released so release callbacks know who they belong to.
pub static mut CurrentResourceOwner: ResourceOwner = null_mut();
pub static mut CurTransactionResourceOwner: ResourceOwner = null_mut();
pub static mut TopTransactionResourceOwner: ResourceOwner = null_mut();
pub static mut AuxProcessResourceOwner: ResourceOwner = null_mut();

/// List of add-on callbacks for resource releasing.
#[repr(C)]
struct ResourceReleaseCallbackItem {
    next: *mut ResourceReleaseCallbackItem,
    callback: ResourceReleaseCallback,
    arg: *mut c_void,
}

static mut ResourceRelease_callbacks: *mut ResourceReleaseCallbackItem = null_mut();

// ---------------------------------------------------------------------------
// INTERNAL ROUTINES
// ---------------------------------------------------------------------------

/// Hash function for value+kind combination.
///
/// Most resource kinds store a pointer in 'value', unique on its own. But some
/// resources store plain integers (Files, Buffers), so we incorporate 'kind'
/// in the hash too via hash_combine, otherwise they collide a lot.
#[inline]
fn hash_resource_elem(value: Datum, kind: *const ResourceOwnerDesc) -> uint32 {
    // SIZEOF_DATUM == 8 on the platforms we target (Datum = usize = uintptr_t).
    if core::mem::size_of::<Datum>() == 8 {
        hash_combine64(murmurhash64(value as uint64), kind as uint64) as uint32
    } else {
        hash_combine(murmurhash32(value as uint32), kind as uint32)
    }
}

/// Adds 'value' of given 'kind' to the ResourceOwner's hash table.
unsafe fn ResourceOwnerAddToHash(
    owner: ResourceOwner,
    value: Datum,
    kind: *const ResourceOwnerDesc,
) {
    let mask = (*owner).capacity - 1;

    Assert!(!kind.is_null());

    // Insert into first free slot at or after hash location.
    let mut idx = hash_resource_elem(value, kind) & mask;
    loop {
        if (*(*owner).hash.add(idx as usize)).kind.is_null() {
            break; // found a free slot
        }
        idx = (idx + 1) & mask;
    }
    let slot = (*owner).hash.add(idx as usize);
    (*slot).item = value;
    (*slot).kind = kind;
    (*owner).nhash += 1;
}

/// Comparison ordering helper: sort by release phase and priority (reverse).
/// Mirrors the C resource_priority_cmp (returns <0/0/>0).
#[inline]
unsafe fn resource_priority_cmp(ra: &ResourceElem, rb: &ResourceElem) -> c_int {
    let ka = &*ra.kind;
    let kb = &*rb.kind;

    // Note: reverse order
    if ka.release_phase == kb.release_phase {
        pg_cmp_u32(kb.release_priority, ka.release_priority)
    } else if ka.release_phase > kb.release_phase {
        -1
    } else {
        1
    }
}

/// Sort resources in reverse release priority.
///
/// If the hash table is in use, all elements from the fixed-size array are
/// moved to the hash table, then the hash is sorted. Otherwise the fixed-size
/// array is sorted directly. Either way the result is one sorted array.
unsafe fn ResourceOwnerSort(owner: ResourceOwner) {
    let items: *mut ResourceElem;
    let nitems: usize;

    if (*owner).nhash == 0 {
        items = (*owner).arr.as_mut_ptr();
        nitems = (*owner).narr as usize;
    } else {
        // Compact the hash table so all elements are at the beginning with no
        // empty slots.
        let mut dst: uint32 = 0;

        for idx in 0..(*owner).capacity {
            if !(*(*owner).hash.add(idx as usize)).kind.is_null() {
                if dst != idx {
                    *(*owner).hash.add(dst as usize) = *(*owner).hash.add(idx as usize);
                }
                dst += 1;
            }
        }

        // Move all entries from the fixed-size array to 'hash'.
        // RESOWNER_HASH_MAX_ITEMS guarantees enough free space.
        Assert!(dst + (*owner).narr as uint32 <= (*owner).capacity);
        for idx in 0..(*owner).narr as usize {
            *(*owner).hash.add(dst as usize) = (*owner).arr[idx];
            dst += 1;
        }
        Assert!(dst == (*owner).nhash + (*owner).narr as uint32);
        (*owner).narr = 0;
        (*owner).nhash = dst;

        items = (*owner).hash;
        nitems = (*owner).nhash as usize;
    }

    let slice = core::slice::from_raw_parts_mut(items, nitems);
    slice.sort_by(|a, b| resource_priority_cmp(a, b).cmp(&0));
}

/// Call the ReleaseResource callback on entries with given 'phase'.
unsafe fn ResourceOwnerReleaseAll(
    owner: ResourceOwner,
    phase: ResourceReleasePhase,
    printLeakWarnings: bool,
) {
    let items: *mut ResourceElem;
    let mut nitems: uint32;

    // ResourceOwnerSort must've been called already.
    Assert!((*owner).releasing);
    Assert!((*owner).sorted);
    if (*owner).nhash == 0 {
        items = (*owner).arr.as_mut_ptr();
        nitems = (*owner).narr as uint32;
    } else {
        Assert!((*owner).narr == 0);
        items = (*owner).hash;
        nitems = (*owner).nhash;
    }

    // Resources are sorted in reverse priority order. Release from the end
    // until we hit the end of the phase being released; we continue from there
    // on the next phase.
    while nitems > 0 {
        let idx = nitems - 1;
        let value = (*items.add(idx as usize)).item;
        let kind = (*items.add(idx as usize)).kind;

        if (*kind).release_phase > phase {
            break;
        }
        Assert!((*kind).release_phase == phase);

        if printLeakWarnings {
            // DebugPrint or generic "[name] [ptr]" (psprintf not ported; emit
            // the equivalent via the elog formatting directly).
            // TODO(pg-port): use psprintf once ported, and pfree the result.
            match (*kind).DebugPrint {
                Some(dp) => {
                    let res_str = dp(value);
                    elog!(
                        WARNING,
                        "resource was not closed: {:?}",
                        res_str
                    );
                    pfree(res_str as *mut c_void);
                }
                None => {
                    elog!(
                        WARNING,
                        "resource was not closed: {:?} {:?}",
                        (*kind).name,
                        DatumGetPointer(value)
                    );
                }
            }
        }
        ((*kind).ReleaseResource)(value);
        nitems -= 1;
    }
    if (*owner).nhash == 0 {
        (*owner).narr = nitems as uint8;
    } else {
        (*owner).nhash = nitems;
    }
}

// ---------------------------------------------------------------------------
// EXPORTED ROUTINES
// ---------------------------------------------------------------------------

/// ResourceOwnerCreate: create an empty ResourceOwner.
///
/// All ResourceOwner objects are kept in TopMemoryContext, since they should
/// only be freed explicitly.
pub unsafe fn ResourceOwnerCreate(parent: ResourceOwner, name: *const c_char) -> ResourceOwner {
    let owner = MemoryContextAllocZero(
        TopMemoryContext,
        core::mem::size_of::<ResourceOwnerData>(),
    ) as ResourceOwner;
    (*owner).name = name;

    if !parent.is_null() {
        (*owner).parent = parent;
        (*owner).nextchild = (*parent).firstchild;
        (*parent).firstchild = owner;
    }

    dlist_init(&mut (*owner).aio_handles);

    owner
}

/// Make sure there is room for at least one more resource in an array.
///
/// Separate from inserting so that an out-of-memory failure happens *before*
/// acquiring the resource. NB: no unrelated ResourceOwnerRemember() calls may
/// happen between this and the Remember you reserved space for.
pub unsafe fn ResourceOwnerEnlarge(owner: ResourceOwner) {
    // Mustn't remember more resources after we have started releasing.
    if (*owner).releasing {
        elog!(ERROR, "ResourceOwnerEnlarge called after release started");
    }

    if (*owner).narr < RESOWNER_ARRAY_SIZE as uint8 {
        return; // no work needed
    }

    // Is there space in the hash? If not, enlarge it.
    if (*owner).narr as uint32 + (*owner).nhash >= (*owner).grow_at {
        let oldhash = (*owner).hash;
        let oldcap = (*owner).capacity;

        // Double the capacity (must stay a power of 2!).
        let newcap = if oldcap > 0 {
            oldcap * 2
        } else {
            RESOWNER_HASH_INIT_SIZE
        };
        let newhash = MemoryContextAllocZero(
            TopMemoryContext,
            newcap as Size * core::mem::size_of::<ResourceElem>(),
        ) as *mut ResourceElem;

        // We assume we can't fail below this point, so OK to scribble.
        (*owner).hash = newhash;
        (*owner).capacity = newcap;
        (*owner).grow_at = RESOWNER_HASH_MAX_ITEMS(newcap);
        (*owner).nhash = 0;

        if !oldhash.is_null() {
            // Transfer pre-existing entries into the new hash table; they don't
            // necessarily go where they were before.
            for i in 0..oldcap {
                let e = &*oldhash.add(i as usize);
                if !e.kind.is_null() {
                    ResourceOwnerAddToHash(owner, e.item, e.kind);
                }
            }

            // And release old hash table.
            pfree(oldhash as *mut c_void);
        }
    }

    // Move items from the array to the hash.
    for i in 0..(*owner).narr as usize {
        ResourceOwnerAddToHash(owner, (*owner).arr[i].item, (*owner).arr[i].kind);
    }
    (*owner).narr = 0;

    Assert!((*owner).nhash <= (*owner).grow_at);
}

/// Remember that an object is owned by a ResourceOwner.
///
/// Caller must have previously done ResourceOwnerEnlarge().
pub unsafe fn ResourceOwnerRemember(
    owner: ResourceOwner,
    value: Datum,
    kind: *const ResourceOwnerDesc,
) {
    // sanity check the ResourceOwnerDesc
    Assert!((*kind).release_phase as c_int != 0);
    Assert!((*kind).release_priority != 0);

    // Mustn't remember after release started (already checked in Enlarge).
    Assert!(!(*owner).releasing);
    Assert!(!(*owner).sorted);

    if (*owner).narr >= RESOWNER_ARRAY_SIZE as uint8 {
        // forgot to call ResourceOwnerEnlarge?
        elog!(ERROR, "ResourceOwnerRemember called but array was full");
    }

    // Append to the array.
    let idx = (*owner).narr as usize;
    (*owner).arr[idx].item = value;
    (*owner).arr[idx].kind = kind;
    (*owner).narr += 1;
}

/// Forget that an object is owned by a ResourceOwner.
///
/// Note: If the same resource ID is associated more than once, one instance is
/// removed. Forgetting does not guarantee room for a new remember, except that
/// forgetting the most-recently-remembered resource does make room (some
/// callers rely on that).
pub unsafe fn ResourceOwnerForget(
    owner: ResourceOwner,
    value: Datum,
    kind: *const ResourceOwnerDesc,
) {
    // Mustn't call this after release started.
    if (*owner).releasing {
        elog!(
            ERROR,
            "ResourceOwnerForget called for {:?} after release started",
            (*kind).name
        );
    }
    Assert!(!(*owner).sorted);

    // Search through all items in the array first.
    let mut i = (*owner).narr as isize - 1;
    while i >= 0 {
        let e = (*owner).arr[i as usize];
        if e.item == value && e.kind == kind {
            (*owner).arr[i as usize] = (*owner).arr[(*owner).narr as usize - 1];
            (*owner).narr -= 1;
            return;
        }
        i -= 1;
    }

    // Search hash
    if (*owner).nhash > 0 {
        let mask = (*owner).capacity - 1;
        let mut idx = hash_resource_elem(value, kind) & mask;
        for _ in 0..(*owner).capacity {
            let slot = (*owner).hash.add(idx as usize);
            if (*slot).item == value && (*slot).kind == kind {
                (*slot).item = 0 as Datum;
                (*slot).kind = null();
                (*owner).nhash -= 1;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    // %p in the C source: print the underlying pointer (a programmer error).
    elog!(
        ERROR,
        "{:?} {:?} is not owned by resource owner {:?}",
        (*kind).name,
        DatumGetPointer(value),
        (*owner).name
    );
}

/// ResourceOwnerRelease: release all resources owned by a ResourceOwner and its
/// descendants, but don't delete the owner objects themselves.
///
/// Executes just one phase; typically called three times. xact.c may have other
/// operations to do between phases.
///
/// After starting release, no new resources can be remembered, and you cannot
/// ResourceOwnerForget previously-remembered ones in retail.
pub unsafe fn ResourceOwnerRelease(
    owner: ResourceOwner,
    phase: ResourceReleasePhase,
    isCommit: bool,
    isTopLevel: bool,
) {
    // There's not currently any setup needed before recursing.
    ResourceOwnerReleaseInternal(owner, phase, isCommit, isTopLevel);
}

unsafe fn ResourceOwnerReleaseInternal(
    owner: ResourceOwner,
    phase: ResourceReleasePhase,
    isCommit: bool,
    isTopLevel: bool,
) {
    // Recurse to handle descendants.
    let mut child = (*owner).firstchild;
    while !child.is_null() {
        ResourceOwnerReleaseInternal(child, phase, isCommit, isTopLevel);
        child = (*child).nextchild;
    }

    // To release resources in the right order, sort them by phase and priority.
    // The ReleaseResource callbacks are not allowed to remember/forget any
    // other resources after this.
    if !(*owner).releasing {
        Assert!(phase == RESOURCE_RELEASE_BEFORE_LOCKS);
        Assert!(!(*owner).sorted);
        (*owner).releasing = true;
    } else {
        // Phase is normally > BEFORE_LOCKS if not the first call. But if an
        // error happens between phases, AbortTransaction may call us again for
        // the same owner.
    }
    if !(*owner).sorted {
        ResourceOwnerSort(owner);
        (*owner).sorted = true;
    }

    // Make CurrentResourceOwner point to me, so release callbacks know which
    // resource owner is being released.
    let save = CurrentResourceOwner;
    CurrentResourceOwner = owner;

    if phase == RESOURCE_RELEASE_BEFORE_LOCKS {
        // Release all resources that need to be released before the locks.
        // During a commit there shouldn't be any remaining (warn); in the abort
        // case clean up quietly.
        ResourceOwnerReleaseAll(owner, phase, isCommit);

        while !dlist_is_empty(&(*owner).aio_handles) {
            let node = dlist_head_node(&mut (*owner).aio_handles);
            pgaio_io_release_resowner(node, !isCommit);
        }
    } else if phase == RESOURCE_RELEASE_LOCKS {
        if isTopLevel {
            // For a top-level xact we release all (non-session) locks with a
            // single lmgr call at the top of recursion.
            if owner == TopTransactionResourceOwner {
                ProcReleaseLocks(isCommit);
                ReleasePredicateLocks(isCommit, false);
            }
        } else {
            // Release locks retail. If committing a subtransaction, transfer
            // locks to the parent rather than releasing.
            Assert!(!(*owner).parent.is_null());

            // Pass the list of locks to the lock manager, unless overflowed.
            let (locks, nlocks): (*mut *mut LOCALLOCK, c_int) =
                if (*owner).nlocks as usize > MAX_RESOWNER_LOCKS {
                    (null_mut(), 0)
                } else {
                    ((*owner).locks.as_mut_ptr(), (*owner).nlocks as c_int)
                };

            if isCommit {
                LockReassignCurrentOwner(locks, nlocks);
            } else {
                LockReleaseCurrentOwner(locks, nlocks);
            }
        }
    } else if phase == RESOURCE_RELEASE_AFTER_LOCKS {
        // Release all resources that need to be released after the locks.
        ResourceOwnerReleaseAll(owner, phase, isCommit);
    }

    // Let add-on modules get a chance too.
    let mut item = ResourceRelease_callbacks;
    while !item.is_null() {
        // allow callbacks to unregister themselves when called
        let next = (*item).next;
        ((*item).callback)(phase, isCommit, isTopLevel, (*item).arg);
        item = next;
    }

    CurrentResourceOwner = save;
}

/// ResourceOwnerReleaseAllOfKind: release all resources of a certain type held
/// by this owner.
pub unsafe fn ResourceOwnerReleaseAllOfKind(
    owner: ResourceOwner,
    kind: *const ResourceOwnerDesc,
) {
    // Mustn't call this after release started.
    if (*owner).releasing {
        elog!(
            ERROR,
            "ResourceOwnerForget called for {:?} after release started",
            (*kind).name
        );
    }
    Assert!(!(*owner).sorted);

    // Temporarily set 'releasing' to prevent ResourceOwnerRemember while
    // scanning the owner (enlarging the hash would lose our scan point).
    (*owner).releasing = true;

    // Array first
    let mut i: isize = 0;
    while i < (*owner).narr as isize {
        if (*owner).arr[i as usize].kind == kind {
            let value = (*owner).arr[i as usize].item;

            (*owner).arr[i as usize] = (*owner).arr[(*owner).narr as usize - 1];
            (*owner).narr -= 1;
            i -= 1;

            ((*kind).ReleaseResource)(value);
        }
        i += 1;
    }

    // Then hash
    for i in 0..(*owner).capacity {
        let slot = (*owner).hash.add(i as usize);
        if (*slot).kind == kind {
            let value = (*slot).item;

            (*slot).item = 0 as Datum;
            (*slot).kind = null();
            (*owner).nhash -= 1;

            ((*kind).ReleaseResource)(value);
        }
    }
    (*owner).releasing = false;
}

/// ResourceOwnerDelete: delete an owner object and its descendants.
///
/// The caller must have already released all resources in the object tree.
pub unsafe fn ResourceOwnerDelete(owner: ResourceOwner) {
    // We had better not be deleting CurrentResourceOwner ...
    Assert!(owner != CurrentResourceOwner);

    // And it better not own any resources, either.
    Assert!((*owner).narr == 0);
    Assert!((*owner).nhash == 0);
    Assert!((*owner).nlocks == 0 || (*owner).nlocks as usize == MAX_RESOWNER_LOCKS + 1);

    // Delete children. The recursive call delinks the child from me, so just
    // iterate as long as there is a child.
    while !(*owner).firstchild.is_null() {
        ResourceOwnerDelete((*owner).firstchild);
    }

    // Delink the owner from its parent before deleting it, so an error won't
    // leave deleted/busted owners attached to the tree. Better a leak than a
    // crash.
    ResourceOwnerNewParent(owner, null_mut());

    // And free the object.
    if !(*owner).hash.is_null() {
        pfree((*owner).hash as *mut c_void);
    }
    pfree(owner as *mut c_void);
}

/// Fetch parent of a ResourceOwner (returns null if top-level owner).
pub unsafe fn ResourceOwnerGetParent(owner: ResourceOwner) -> ResourceOwner {
    (*owner).parent
}

/// Reassign a ResourceOwner to have a new parent.
pub unsafe fn ResourceOwnerNewParent(owner: ResourceOwner, newparent: ResourceOwner) {
    let oldparent = (*owner).parent;

    if !oldparent.is_null() {
        if owner == (*oldparent).firstchild {
            (*oldparent).firstchild = (*owner).nextchild;
        } else {
            let mut child = (*oldparent).firstchild;
            while !child.is_null() {
                if owner == (*child).nextchild {
                    (*child).nextchild = (*owner).nextchild;
                    break;
                }
                child = (*child).nextchild;
            }
        }
    }

    if !newparent.is_null() {
        Assert!(owner != newparent);
        (*owner).parent = newparent;
        (*owner).nextchild = (*newparent).firstchild;
        (*newparent).firstchild = owner;
    } else {
        (*owner).parent = null_mut();
        (*owner).nextchild = null_mut();
    }
}

/// Register a callback function for resource cleanup. Used by dynamically
/// loaded modules. Nowadays defining a new ResourceOwnerDesc is easier.
pub unsafe fn RegisterResourceReleaseCallback(callback: ResourceReleaseCallback, arg: *mut c_void) {
    let item = MemoryContextAlloc(
        TopMemoryContext,
        core::mem::size_of::<ResourceReleaseCallbackItem>(),
    ) as *mut ResourceReleaseCallbackItem;
    (*item).callback = callback;
    (*item).arg = arg;
    (*item).next = ResourceRelease_callbacks;
    ResourceRelease_callbacks = item;
}

/// Deregister a previously-registered resource cleanup callback.
pub unsafe fn UnregisterResourceReleaseCallback(
    callback: ResourceReleaseCallback,
    arg: *mut c_void,
) {
    let mut prev: *mut ResourceReleaseCallbackItem = null_mut();
    let mut item = ResourceRelease_callbacks;
    while !item.is_null() {
        if core::ptr::fn_addr_eq((*item).callback, callback) && (*item).arg == arg {
            if !prev.is_null() {
                (*prev).next = (*item).next;
            } else {
                ResourceRelease_callbacks = (*item).next;
            }
            pfree(item as *mut c_void);
            break;
        }
        prev = item;
        item = (*item).next;
    }
}

/// Establish an AuxProcessResourceOwner for the current process.
pub unsafe fn CreateAuxProcessResourceOwner() {
    Assert!(AuxProcessResourceOwner.is_null());
    Assert!(CurrentResourceOwner.is_null());
    AuxProcessResourceOwner =
        ResourceOwnerCreate(null_mut(), c"AuxiliaryProcess".as_ptr());
    CurrentResourceOwner = AuxProcessResourceOwner;

    // Register a shmem-exit callback for cleanup of aux-process resource owner.
    on_shmem_exit(ReleaseAuxProcessResourcesCallback, 0 as Datum);
}

/// Convenience routine to release all resources tracked in
/// AuxProcessResourceOwner (the resowner itself is not destroyed here). Warn
/// about leaked resources if isCommit is true.
pub unsafe fn ReleaseAuxProcessResources(isCommit: bool) {
    // At this writing the only thing that could actually get released is buffer
    // pins; but we may as well do the full release protocol.
    ResourceOwnerRelease(
        AuxProcessResourceOwner,
        RESOURCE_RELEASE_BEFORE_LOCKS,
        isCommit,
        true,
    );
    ResourceOwnerRelease(
        AuxProcessResourceOwner,
        RESOURCE_RELEASE_LOCKS,
        isCommit,
        true,
    );
    ResourceOwnerRelease(
        AuxProcessResourceOwner,
        RESOURCE_RELEASE_AFTER_LOCKS,
        isCommit,
        true,
    );
    // allow it to be reused
    (*AuxProcessResourceOwner).releasing = false;
    (*AuxProcessResourceOwner).sorted = false;
}

/// Shmem-exit callback for the same. Warn about leaked resources if process
/// exit code is zero (ie normal).
unsafe fn ReleaseAuxProcessResourcesCallback(code: c_int, _arg: Datum) {
    let isCommit = code == 0;
    ReleaseAuxProcessResources(isCommit);
}

/// Remember that a Local Lock is owned by a ResourceOwner.
///
/// Unlike the generic ResourceOwnerRemember, the list of locks is a lossy cache
/// holding up to MAX_RESOWNER_LOCKS entries; when it overflows we stop tracking
/// locks (so ResourceOwnerForgetLock needn't scan a large array).
pub unsafe fn ResourceOwnerRememberLock(owner: ResourceOwner, locallock: *mut LOCALLOCK) {
    Assert!(!locallock.is_null());

    if (*owner).nlocks as usize > MAX_RESOWNER_LOCKS {
        return; // we have already overflowed
    }

    if ((*owner).nlocks as usize) < MAX_RESOWNER_LOCKS {
        (*owner).locks[(*owner).nlocks as usize] = locallock;
    } else {
        // overflowed
    }
    (*owner).nlocks += 1;
}

/// Forget that a Local Lock is owned by a ResourceOwner.
pub unsafe fn ResourceOwnerForgetLock(owner: ResourceOwner, locallock: *mut LOCALLOCK) {
    if (*owner).nlocks as usize > MAX_RESOWNER_LOCKS {
        return; // we have overflowed
    }

    Assert!((*owner).nlocks > 0);
    let mut i = (*owner).nlocks as isize - 1;
    while i >= 0 {
        if locallock == (*owner).locks[i as usize] {
            (*owner).locks[i as usize] = (*owner).locks[(*owner).nlocks as usize - 1];
            (*owner).nlocks -= 1;
            return;
        }
        i -= 1;
    }
    elog!(
        ERROR,
        "lock reference {:?} is not owned by resource owner {:?}",
        locallock,
        (*owner).name
    );
}

/// Register an AIO handle (dlist node) with a ResourceOwner.
pub unsafe fn ResourceOwnerRememberAioHandle(owner: ResourceOwner, ioh_node: *mut dlist_node) {
    dlist_push_tail(&mut (*owner).aio_handles, ioh_node);
}

/// Forget an AIO handle (dlist node) owned by a ResourceOwner.
pub unsafe fn ResourceOwnerForgetAioHandle(owner: ResourceOwner, ioh_node: *mut dlist_node) {
    dlist_delete_from(&mut (*owner).aio_handles, ioh_node);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Counts how many times the test ReleaseResource callback fires.
    static mut RELEASE_COUNT: u32 = 0;

    unsafe fn test_release(_res: Datum) {
        RELEASE_COUNT += 1;
    }

    // ResourceOwnerDesc holds a raw `*const c_char` name, so it isn't Sync;
    // wrap it for the `static` (single stable address, needed for kind-pointer
    // identity in Forget/ReleaseAllOfKind).
    struct SyncDesc(ResourceOwnerDesc);
    unsafe impl Sync for SyncDesc {}
    static TEST_KIND_W: SyncDesc = SyncDesc(ResourceOwnerDesc {
        name: c"test-resource".as_ptr(),
        release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
        release_priority: RELEASE_PRIO_FIRST,
        ReleaseResource: test_release,
        DebugPrint: None,
    });
    static TEST_KIND: &SyncDesc = &TEST_KIND_W;

    // RELEASE_COUNT is a process-global static mut shared by both tests;
    // serialize them (cargo runs tests in parallel threads).
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn remember_forget_release() {
        unsafe {
            let _g = TEST_LOCK.lock().unwrap();
            RELEASE_COUNT = 0;

            let owner = ResourceOwnerCreate(null_mut(), c"test-owner".as_ptr());
            assert!(!owner.is_null());
            assert!((*owner).parent.is_null());

            // Remember N items (small enough to stay in the fixed array).
            let n: usize = 10;
            for k in 1..=n {
                ResourceOwnerEnlarge(owner);
                ResourceOwnerRemember(owner, k as Datum, &TEST_KIND.0);
            }
            assert_eq!((*owner).narr as usize, n);
            assert_eq!((*owner).nhash, 0);

            // Forget one item.
            ResourceOwnerForget(owner, 5 as Datum, &TEST_KIND.0);
            assert_eq!((*owner).narr as usize, n - 1);

            // Release drives the remaining items through the kind callback.
            ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
            assert_eq!(RELEASE_COUNT, (n - 1) as u32);

            // After BEFORE_LOCKS phase, all BEFORE_LOCKS resources are gone.
            assert_eq!((*owner).narr, 0);
            // We stop after BEFORE_LOCKS: the LOCKS phase would call into the
            // (unported) lock manager for a non-top-level owner, and the
            // top-level path needs TopTransactionResourceOwner wiring.
        }
    }

    // Exercise the hash-table path: remember more than RESOWNER_ARRAY_SIZE so
    // entries spill into the open-addressing hash, then release them all.
    #[test]
    fn release_spills_to_hash() {
        unsafe {
            let _g = TEST_LOCK.lock().unwrap();
            RELEASE_COUNT = 0;

            let owner = ResourceOwnerCreate(null_mut(), c"hash-owner".as_ptr());

            let n: usize = RESOWNER_ARRAY_SIZE + 40; // forces hash use
            for k in 1..=n {
                ResourceOwnerEnlarge(owner);
                ResourceOwnerRemember(owner, k as Datum, &TEST_KIND.0);
            }
            assert!((*owner).nhash > 0);

            // Forget one from the hash.
            ResourceOwnerForget(owner, 1 as Datum, &TEST_KIND.0);

            ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
            assert_eq!(RELEASE_COUNT, (n - 1) as u32);
            assert_eq!((*owner).nhash, 0);
        }
    }
}

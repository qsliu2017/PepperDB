//! src/backend/lib/dshash.c
//! src/include/lib/dshash.h
//!
//! dshash.c
//!   Concurrent hash tables backed by dynamic shared memory areas.
//!
//! This is an open hashing hash table, with a linked list at each table
//! entry.  It supports dynamic resizing, as required to prevent the linked
//! lists from growing too long on average.  Currently, only growing is
//! supported: the hash table never becomes smaller.
//!
//! To deal with concurrency, it has a fixed size set of partitions, each of
//! which is independently locked.  Each bucket maps to a partition; so insert,
//! find and iterate operations normally only acquire one lock.  Therefore,
//! good concurrency is achieved whenever such operations don't collide at the
//! lock partition level.  However, when a resize operation begins, all
//! partition locks must be acquired simultaneously for a brief period.  This
//! is only expected to happen a small number of times until a stable size is
//! found, since growth is geometric.
//!
//! Future versions may support iterators and incremental resizing; for now
//! the implementation is minimalist.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_int, c_void};

// ----------------------------------------------------------------------------
// Types from dsa.h / lwlock.h (stubbed where not yet ported).
// ----------------------------------------------------------------------------

/// dsa_pointer: an address relative to a dynamic shared memory area.
pub type dsa_pointer = uint64;

/// Opaque backing dynamic shared memory area.
#[repr(C)]
pub struct dsa_area {
    pub _private: [u8; 0],
}

/// LWLock - stubbed type for lock partitions.
#[repr(C)]
pub struct LWLock {
    pub _private: [u8; 0],
}

/// LWLockMode values.
pub type LWLockMode = c_int;
pub const LW_EXCLUSIVE: LWLockMode = 0;
pub const LW_SHARED: LWLockMode = 1;

/// DSA allocation flags.
pub const DSA_ALLOC_NO_OOM: c_int = 0x02;
pub const DSA_ALLOC_ZERO: c_int = 0x04;
pub const DSA_ALLOC_HUGE: c_int = 0x01;

/// The invalid dsa_pointer value.
pub const InvalidDsaPointer: dsa_pointer = 0;

/// errcode for out-of-memory.
pub const ERRCODE_OUT_OF_MEMORY: c_int = 0;

#[inline]
fn DsaPointerIsValid(x: dsa_pointer) -> bool {
    x != InvalidDsaPointer
}

// ----------------------------------------------------------------------------
// dshash.h - public header, merged in.
// ----------------------------------------------------------------------------

/* A handle for a dshash_table which can be shared with other processes. */
pub type dshash_table_handle = dsa_pointer;

/* Sentinel value to use for invalid dshash_table handles. */
pub const DSHASH_HANDLE_INVALID: dshash_table_handle = InvalidDsaPointer;

/* The type for hash values. */
pub type dshash_hash = uint32;

/* A function type for comparing keys. */
pub type dshash_compare_function =
    Option<unsafe extern "C" fn(a: *const c_void, b: *const c_void, size: Size, arg: *mut c_void) -> c_int>;

/* A function type for computing hash values for keys. */
pub type dshash_hash_function =
    Option<unsafe extern "C" fn(v: *const c_void, size: Size, arg: *mut c_void) -> dshash_hash>;

/* A function type for copying keys. */
pub type dshash_copy_function =
    Option<unsafe extern "C" fn(dest: *mut c_void, src: *const c_void, size: Size, arg: *mut c_void)>;

/*
 * The set of parameters needed to create or attach to a hash table.  The
 * tranche_id member does not need to be initialized when attaching to an
 * existing hash table.
 *
 * Compare, hash, and copy functions must be supplied even when attaching,
 * because we can't safely share function pointers between backends in general.
 * The user data pointer supplied to the create and attach functions will be
 * passed to these functions.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct dshash_parameters {
    pub key_size: Size,                            /* Size of the key (initial bytes of entry) */
    pub entry_size: Size,                          /* Total size of entry */
    pub compare_function: dshash_compare_function, /* Compare function */
    pub hash_function: dshash_hash_function,       /* Hash function */
    pub copy_function: dshash_copy_function,       /* Copy function */
    pub tranche_id: c_int,                         /* The tranche ID to use for locks */
}

/*
 * Sequential scan state. The detail is exposed to let users know the storage
 * size but it should be considered as an opaque type by callers.
 */
#[repr(C)]
pub struct dshash_seq_status {
    pub hash_table: *mut dshash_table,  /* dshash table working on */
    pub curbucket: c_int,               /* bucket number we are at */
    pub nbuckets: c_int,                /* total number of buckets in the dshash */
    pub curitem: *mut dshash_table_item, /* item we are currently at */
    pub pnextitem: dsa_pointer,         /* dsa-pointer to the next item */
    pub curpartition: c_int,            /* partition number we are at */
    pub exclusive: bool,                /* locking mode */
}

// ----------------------------------------------------------------------------
// dshash.c - private types.
// ----------------------------------------------------------------------------

/*
 * An item in the hash table.  This wraps the user's entry object in an
 * envelop that holds a pointer back to the bucket and a pointer to the next
 * item in the bucket.
 */
#[repr(C)]
pub struct dshash_table_item {
    /* The next item in the same bucket. */
    pub next: dsa_pointer,
    /* The hashed key, to avoid having to recompute it. */
    pub hash: dshash_hash,
    /* The user's entry object follows here.  See ENTRY_FROM_ITEM(item). */
}

/*
 * The number of partitions for locking purposes.  This is set to match
 * NUM_BUFFER_PARTITIONS for now, on the basis that whatever's good enough for
 * the buffer pool must be good enough for any other purpose.  This could
 * become a runtime parameter in future.
 */
const DSHASH_NUM_PARTITIONS_LOG2: usize = 7;
const DSHASH_NUM_PARTITIONS: usize = 1 << DSHASH_NUM_PARTITIONS_LOG2;

/* A magic value used to identify our hash tables. */
const DSHASH_MAGIC: uint32 = 0x75ff6a20;

/*
 * Tracking information for each lock partition.  Initially, each partition
 * corresponds to one bucket, but each time the hash table grows, the buckets
 * covered by each partition split so the number of buckets covered doubles.
 *
 * We might want to add padding here so that each partition is on a different
 * cache line, but doing so would bloat this structure considerably.
 */
#[repr(C)]
pub struct dshash_partition {
    pub lock: LWLock,  /* Protects all buckets in this partition. */
    pub count: Size,   /* # of items in this partition's buckets */
}

/*
 * The head object for a hash table.  This will be stored in dynamic shared
 * memory.
 */
#[repr(C)]
pub struct dshash_table_control {
    pub handle: dshash_table_handle,
    pub magic: uint32,
    pub partitions: [dshash_partition; DSHASH_NUM_PARTITIONS],
    pub lwlock_tranche_id: c_int,

    /*
     * The following members are written to only when ALL partitions locks are
     * held.  They can be read when any one partition lock is held.
     */

    /* Number of buckets expressed as power of 2 (8 = 256 buckets). */
    pub size_log2: Size, /* log2(number of buckets) */
    pub buckets: dsa_pointer, /* current bucket array */
}

/*
 * Per-backend state for a dynamic hash table.
 */
#[repr(C)]
pub struct dshash_table {
    pub area: *mut dsa_area,             /* Backing dynamic shared memory area. */
    pub params: dshash_parameters,       /* Parameters. */
    pub arg: *mut c_void,                /* User-supplied data pointer. */
    pub control: *mut dshash_table_control, /* Control object in DSM. */
    pub buckets: *mut dsa_pointer,       /* Current bucket pointers in DSM. */
    pub size_log2: Size,                 /* log2(number of buckets) */
}

// ----------------------------------------------------------------------------
// Macros translated to helper functions / inline expressions.
// ----------------------------------------------------------------------------

/* Given a pointer to an item, find the entry (user data) it holds. */
#[inline]
unsafe fn ENTRY_FROM_ITEM(item: *mut dshash_table_item) -> *mut c_void {
    (item as *mut c_char).add(MAXALIGN(core::mem::size_of::<dshash_table_item>())) as *mut c_void
}

/* Given a pointer to an entry, find the item that holds it. */
#[inline]
unsafe fn ITEM_FROM_ENTRY(entry: *mut c_void) -> *mut dshash_table_item {
    (entry as *mut c_char).sub(MAXALIGN(core::mem::size_of::<dshash_table_item>()))
        as *mut dshash_table_item
}

/* How many resize operations (bucket splits) have there been? */
#[inline]
fn NUM_SPLITS(size_log2: Size) -> Size {
    size_log2 - DSHASH_NUM_PARTITIONS_LOG2
}

/* How many buckets are there in a given size? */
#[inline]
fn NUM_BUCKETS(size_log2: Size) -> Size {
    (1 as Size) << size_log2
}

/* How many buckets are there in each partition at a given size? */
#[inline]
fn BUCKETS_PER_PARTITION(size_log2: Size) -> Size {
    (1 as Size) << NUM_SPLITS(size_log2)
}

/* Max entries before we need to grow.  Half + quarter = 75% load factor. */
#[inline]
unsafe fn MAX_COUNT_PER_PARTITION(hash_table: *mut dshash_table) -> Size {
    BUCKETS_PER_PARTITION((*hash_table).size_log2) / 2
        + BUCKETS_PER_PARTITION((*hash_table).size_log2) / 4
}

/* Choose partition based on the highest order bits of the hash. */
#[inline]
fn PARTITION_FOR_HASH(hash: dshash_hash) -> Size {
    (hash >> ((core::mem::size_of::<dshash_hash>() * 8) - DSHASH_NUM_PARTITIONS_LOG2)) as Size
}

/*
 * Find the bucket index for a given hash and table size.  Each time the table
 * doubles in size, the appropriate bucket for a given hash value doubles and
 * possibly adds one, depending on the newly revealed bit, so that all buckets
 * are split.
 */
#[inline]
fn BUCKET_INDEX_FOR_HASH_AND_SIZE(hash: dshash_hash, size_log2: Size) -> Size {
    (hash >> ((core::mem::size_of::<dshash_hash>() * 8) - size_log2)) as Size
}

/* The index of the first bucket in a given partition. */
#[inline]
fn BUCKET_INDEX_FOR_PARTITION(partition: Size, size_log2: Size) -> Size {
    partition << NUM_SPLITS(size_log2)
}

/* Choose partition based on bucket index. */
#[inline]
fn PARTITION_FOR_BUCKET_INDEX(bucket_idx: Size, size_log2: Size) -> Size {
    bucket_idx >> NUM_SPLITS(size_log2)
}

/* The head of the active bucket for a given hash value (lvalue). */
#[inline]
unsafe fn BUCKET_FOR_HASH(hash_table: *mut dshash_table, hash: dshash_hash) -> *mut dsa_pointer {
    (*hash_table)
        .buckets
        .add(BUCKET_INDEX_FOR_HASH_AND_SIZE(hash, (*hash_table).size_log2))
}

/* Lock for the given partition index. */
#[inline]
unsafe fn PARTITION_LOCK(hash_table: *mut dshash_table, i: Size) -> *mut LWLock {
    &mut (*(*hash_table).control).partitions[i].lock
}

#[inline]
unsafe fn ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME(hash_table: *mut dshash_table) {
    Assert!(!LWLockAnyHeldByMe(
        &mut (*(*hash_table).control).partitions[0].lock,
        DSHASH_NUM_PARTITIONS as c_int,
        core::mem::size_of::<dshash_partition>(),
    ));
}

// ----------------------------------------------------------------------------
// Public functions.
// ----------------------------------------------------------------------------

/*
 * Create a new hash table backed by the given dynamic shared area, with the
 * given parameters.  The returned object is allocated in backend-local memory
 * using the current MemoryContext.  'arg' will be passed through to the
 * compare, hash, and copy functions.
 */
pub unsafe fn dshash_create(
    area: *mut dsa_area,
    params: *const dshash_parameters,
    arg: *mut c_void,
) -> *mut dshash_table {
    let hash_table: *mut dshash_table;
    let control: dsa_pointer;

    /* Allocate the backend-local object representing the hash table. */
    hash_table = palloc(core::mem::size_of::<dshash_table>()) as *mut dshash_table;

    /* Allocate the control object in shared memory. */
    control = dsa_allocate(area, core::mem::size_of::<dshash_table_control>());

    /* Set up the local and shared hash table structs. */
    (*hash_table).area = area;
    (*hash_table).params = *params;
    (*hash_table).arg = arg;
    (*hash_table).control = dsa_get_address(area, control) as *mut dshash_table_control;
    (*(*hash_table).control).handle = control;
    (*(*hash_table).control).magic = DSHASH_MAGIC;
    (*(*hash_table).control).lwlock_tranche_id = (*params).tranche_id;

    /* Set up the array of lock partitions. */
    {
        let partitions: *mut dshash_partition = (*(*hash_table).control).partitions.as_mut_ptr();
        let tranche_id: c_int = (*(*hash_table).control).lwlock_tranche_id;
        let mut i: c_int = 0;

        while i < DSHASH_NUM_PARTITIONS as c_int {
            LWLockInitialize(&mut (*partitions.add(i as usize)).lock, tranche_id);
            (*partitions.add(i as usize)).count = 0;
            i += 1;
        }
    }

    /*
     * Set up the initial array of buckets.  Our initial size is the same as
     * the number of partitions.
     */
    (*(*hash_table).control).size_log2 = DSHASH_NUM_PARTITIONS_LOG2;
    (*(*hash_table).control).buckets = dsa_allocate_extended(
        area,
        core::mem::size_of::<dsa_pointer>() * DSHASH_NUM_PARTITIONS,
        DSA_ALLOC_NO_OOM | DSA_ALLOC_ZERO,
    );
    if !DsaPointerIsValid((*(*hash_table).control).buckets) {
        dsa_free(area, control);
        ereport!(
            ERROR,
            "out of memory"
        );
        unreachable!();
    }
    (*hash_table).buckets =
        dsa_get_address(area, (*(*hash_table).control).buckets) as *mut dsa_pointer;
    (*hash_table).size_log2 = (*(*hash_table).control).size_log2;

    hash_table
}

/*
 * Attach to an existing hash table using a handle.  The returned object is
 * allocated in backend-local memory using the current MemoryContext.  'arg'
 * will be passed through to the compare and hash functions.
 */
pub unsafe fn dshash_attach(
    area: *mut dsa_area,
    params: *const dshash_parameters,
    handle: dshash_table_handle,
    arg: *mut c_void,
) -> *mut dshash_table {
    let hash_table: *mut dshash_table;
    let control: dsa_pointer;

    /* Allocate the backend-local object representing the hash table. */
    hash_table = palloc(core::mem::size_of::<dshash_table>()) as *mut dshash_table;

    /* Find the control object in shared memory. */
    control = handle;

    /* Set up the local hash table struct. */
    (*hash_table).area = area;
    (*hash_table).params = *params;
    (*hash_table).arg = arg;
    (*hash_table).control = dsa_get_address(area, control) as *mut dshash_table_control;
    Assert!((*(*hash_table).control).magic == DSHASH_MAGIC);

    /*
     * These will later be set to the correct values by
     * ensure_valid_bucket_pointers(), at which time we'll be holding a
     * partition lock for interlocking against concurrent resizing.
     */
    (*hash_table).buckets = core::ptr::null_mut();
    (*hash_table).size_log2 = 0;

    hash_table
}

/*
 * Detach from a hash table.  This frees backend-local resources associated
 * with the hash table, but the hash table will continue to exist until it is
 * either explicitly destroyed (by a backend that is still attached to it), or
 * the area that backs it is returned to the operating system.
 */
pub unsafe fn dshash_detach(hash_table: *mut dshash_table) {
    ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME(hash_table);

    /* The hash table may have been destroyed.  Just free local memory. */
    pfree(hash_table as *mut c_void);
}

/*
 * Destroy a hash table, returning all memory to the area.  The caller must be
 * certain that no other backend will attempt to access the hash table before
 * calling this function.  Other backend must explicitly call dshash_detach to
 * free up backend-local memory associated with the hash table.  The backend
 * that calls dshash_destroy must not call dshash_detach.
 */
pub unsafe fn dshash_destroy(hash_table: *mut dshash_table) {
    let size: Size;
    let mut i: Size;

    Assert!((*(*hash_table).control).magic == DSHASH_MAGIC);
    ensure_valid_bucket_pointers(hash_table);

    /* Free all the entries. */
    size = NUM_BUCKETS((*hash_table).size_log2);
    i = 0;
    while i < size {
        let mut item_pointer: dsa_pointer = *(*hash_table).buckets.add(i);

        while DsaPointerIsValid(item_pointer) {
            let item: *mut dshash_table_item;
            let next_item_pointer: dsa_pointer;

            item = dsa_get_address((*hash_table).area, item_pointer) as *mut dshash_table_item;
            next_item_pointer = (*item).next;
            dsa_free((*hash_table).area, item_pointer);
            item_pointer = next_item_pointer;
        }
        i += 1;
    }

    /*
     * Vandalize the control block to help catch programming errors where
     * other backends access the memory formerly occupied by this hash table.
     */
    (*(*hash_table).control).magic = 0;

    /* Free the active table and control object. */
    dsa_free((*hash_table).area, (*(*hash_table).control).buckets);
    dsa_free((*hash_table).area, (*(*hash_table).control).handle);

    pfree(hash_table as *mut c_void);
}

/*
 * Get a handle that can be used by other processes to attach to this hash
 * table.
 */
pub unsafe fn dshash_get_hash_table_handle(hash_table: *mut dshash_table) -> dshash_table_handle {
    Assert!((*(*hash_table).control).magic == DSHASH_MAGIC);

    (*(*hash_table).control).handle
}

/*
 * Look up an entry, given a key.  Returns a pointer to an entry if one can be
 * found with the given key.  Returns NULL if the key is not found.  If a
 * non-NULL value is returned, the entry is locked and must be released by
 * calling dshash_release_lock.  If an error is raised before
 * dshash_release_lock is called, the lock will be released automatically, but
 * the caller must take care to ensure that the entry is not left corrupted.
 * The lock mode is either shared or exclusive depending on 'exclusive'.
 *
 * The caller must not hold a lock already.
 *
 * Note that the lock held is in fact an LWLock, so interrupts will be held on
 * return from this function, and not resumed until dshash_release_lock is
 * called.  It is a very good idea for the caller to release the lock quickly.
 */
pub unsafe fn dshash_find(
    hash_table: *mut dshash_table,
    key: *const c_void,
    exclusive: bool,
) -> *mut c_void {
    let hash: dshash_hash;
    let partition: Size;
    let item: *mut dshash_table_item;

    hash = hash_key(hash_table, key);
    partition = PARTITION_FOR_HASH(hash);

    Assert!((*(*hash_table).control).magic == DSHASH_MAGIC);
    ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME(hash_table);

    LWLockAcquire(
        PARTITION_LOCK(hash_table, partition),
        if exclusive { LW_EXCLUSIVE } else { LW_SHARED },
    );
    ensure_valid_bucket_pointers(hash_table);

    /* Search the active bucket. */
    item = find_in_bucket(hash_table, key, *BUCKET_FOR_HASH(hash_table, hash));

    if item.is_null() {
        /* Not found. */
        LWLockRelease(PARTITION_LOCK(hash_table, partition));
        core::ptr::null_mut()
    } else {
        /* The caller will free the lock by calling dshash_release_lock. */
        ENTRY_FROM_ITEM(item)
    }
}

/*
 * Returns a pointer to an exclusively locked item which must be released with
 * dshash_release_lock.  If the key is found in the hash table, 'found' is set
 * to true and a pointer to the existing entry is returned.  If the key is not
 * found, 'found' is set to false, and a pointer to a newly created entry is
 * returned.
 *
 * Notes above dshash_find() regarding locking and error handling equally
 * apply here.
 */
pub unsafe fn dshash_find_or_insert(
    hash_table: *mut dshash_table,
    key: *const c_void,
    found: *mut bool,
) -> *mut c_void {
    let hash: dshash_hash;
    let partition_index: Size;
    let partition: *mut dshash_partition;
    let mut item: *mut dshash_table_item;

    hash = hash_key(hash_table, key);
    partition_index = PARTITION_FOR_HASH(hash);
    partition = &mut (*(*hash_table).control).partitions[partition_index];

    Assert!((*(*hash_table).control).magic == DSHASH_MAGIC);
    ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME(hash_table);

    'restart: loop {
        LWLockAcquire(PARTITION_LOCK(hash_table, partition_index), LW_EXCLUSIVE);
        ensure_valid_bucket_pointers(hash_table);

        /* Search the active bucket. */
        item = find_in_bucket(hash_table, key, *BUCKET_FOR_HASH(hash_table, hash));

        if !item.is_null() {
            *found = true;
        } else {
            *found = false;

            /* Check if we are getting too full. */
            if (*partition).count > MAX_COUNT_PER_PARTITION(hash_table) {
                /*
                 * The load factor (= keys / buckets) for all buckets protected by
                 * this partition is > 0.75.  Presumably the same applies
                 * generally across the whole hash table (though we don't attempt
                 * to track that directly to avoid contention on some kind of
                 * central counter; we just assume that this partition is
                 * representative).  This is a good time to resize.
                 *
                 * Give up our existing lock first, because resizing needs to
                 * reacquire all the locks in the right order to avoid deadlocks.
                 */
                LWLockRelease(PARTITION_LOCK(hash_table, partition_index));
                resize(hash_table, (*hash_table).size_log2 + 1);

                continue 'restart;
            }

            /* Finally we can try to insert the new item. */
            item = insert_into_bucket(hash_table, key, BUCKET_FOR_HASH(hash_table, hash));
            (*item).hash = hash;
            /* Adjust per-lock-partition counter for load factor knowledge. */
            (*partition).count += 1;
        }

        break;
    }

    /* The caller must release the lock with dshash_release_lock. */
    ENTRY_FROM_ITEM(item)
}

/*
 * Remove an entry by key.  Returns true if the key was found and the
 * corresponding entry was removed.
 *
 * To delete an entry that you already have a pointer to, see
 * dshash_delete_entry.
 */
pub unsafe fn dshash_delete_key(hash_table: *mut dshash_table, key: *const c_void) -> bool {
    let hash: dshash_hash;
    let partition: Size;
    let found: bool;

    Assert!((*(*hash_table).control).magic == DSHASH_MAGIC);
    ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME(hash_table);

    hash = hash_key(hash_table, key);
    partition = PARTITION_FOR_HASH(hash);

    LWLockAcquire(PARTITION_LOCK(hash_table, partition), LW_EXCLUSIVE);
    ensure_valid_bucket_pointers(hash_table);

    if delete_key_from_bucket(hash_table, key, BUCKET_FOR_HASH(hash_table, hash)) {
        Assert!((*(*hash_table).control).partitions[partition].count > 0);
        found = true;
        (*(*hash_table).control).partitions[partition].count -= 1;
    } else {
        found = false;
    }

    LWLockRelease(PARTITION_LOCK(hash_table, partition));

    found
}

/*
 * Remove an entry.  The entry must already be exclusively locked, and must
 * have been obtained by dshash_find or dshash_find_or_insert.  Note that this
 * function releases the lock just like dshash_release_lock.
 *
 * To delete an entry by key, see dshash_delete_key.
 */
pub unsafe fn dshash_delete_entry(hash_table: *mut dshash_table, entry: *mut c_void) {
    let item: *mut dshash_table_item = ITEM_FROM_ENTRY(entry);
    let partition: Size = PARTITION_FOR_HASH((*item).hash);

    Assert!((*(*hash_table).control).magic == DSHASH_MAGIC);
    Assert!(LWLockHeldByMeInMode(
        PARTITION_LOCK(hash_table, partition),
        LW_EXCLUSIVE
    ));

    delete_item(hash_table, item);
    LWLockRelease(PARTITION_LOCK(hash_table, partition));
}

/*
 * Unlock an entry which was locked by dshash_find or dshash_find_or_insert.
 */
pub unsafe fn dshash_release_lock(hash_table: *mut dshash_table, entry: *mut c_void) {
    let item: *mut dshash_table_item = ITEM_FROM_ENTRY(entry);
    let partition_index: Size = PARTITION_FOR_HASH((*item).hash);

    Assert!((*(*hash_table).control).magic == DSHASH_MAGIC);

    LWLockRelease(PARTITION_LOCK(hash_table, partition_index));
}

/*
 * A compare function that forwards to memcmp.
 */
pub unsafe extern "C" fn dshash_memcmp(
    a: *const c_void,
    b: *const c_void,
    size: Size,
    _arg: *mut c_void,
) -> c_int {
    memcmp(a, b, size)
}

/*
 * A hash function that forwards to tag_hash.
 */
pub unsafe extern "C" fn dshash_memhash(
    v: *const c_void,
    size: Size,
    _arg: *mut c_void,
) -> dshash_hash {
    tag_hash(v, size)
}

/*
 * A copy function that forwards to memcpy.
 */
pub unsafe extern "C" fn dshash_memcpy(
    dest: *mut c_void,
    src: *const c_void,
    size: Size,
    _arg: *mut c_void,
) {
    let _ = memcpy(dest, src, size);
}

/*
 * A compare function that forwards to strcmp.
 */
pub unsafe extern "C" fn dshash_strcmp(
    a: *const c_void,
    b: *const c_void,
    size: Size,
    _arg: *mut c_void,
) -> c_int {
    Assert!(strlen(a as *const c_char) < size);
    Assert!(strlen(b as *const c_char) < size);

    strcmp(a as *const c_char, b as *const c_char)
}

/*
 * A hash function that forwards to string_hash.
 */
pub unsafe extern "C" fn dshash_strhash(
    v: *const c_void,
    size: Size,
    _arg: *mut c_void,
) -> dshash_hash {
    Assert!(strlen(v as *const c_char) < size);

    string_hash(v as *const c_char, size)
}

/*
 * A copy function that forwards to strcpy.
 */
pub unsafe extern "C" fn dshash_strcpy(
    dest: *mut c_void,
    src: *const c_void,
    size: Size,
    _arg: *mut c_void,
) {
    Assert!(strlen(src as *const c_char) < size);

    let _ = strcpy(dest as *mut c_char, src as *const c_char);
}

/*
 * Sequentially scan through dshash table and return all the elements one by
 * one, return NULL when all elements have been returned.
 *
 * dshash_seq_term needs to be called when a scan finished.  The caller may
 * delete returned elements midst of a scan by using dshash_delete_current()
 * if exclusive = true.
 */
pub unsafe fn dshash_seq_init(
    status: *mut dshash_seq_status,
    hash_table: *mut dshash_table,
    exclusive: bool,
) {
    (*status).hash_table = hash_table;
    (*status).curbucket = 0;
    (*status).nbuckets = 0;
    (*status).curitem = core::ptr::null_mut();
    (*status).pnextitem = InvalidDsaPointer;
    (*status).curpartition = -1;
    (*status).exclusive = exclusive;
}

/*
 * Returns the next element.
 *
 * Returned elements are locked and the caller may not release the lock. It is
 * released by future calls to dshash_seq_next() or dshash_seq_term().
 */
pub unsafe fn dshash_seq_next(status: *mut dshash_seq_status) -> *mut c_void {
    let mut next_item_pointer: dsa_pointer;

    /*
     * Not yet holding any partition locks. Need to determine the size of the
     * hash table, it could have been resized since we were looking last.
     * Since we iterate in partition order, we can start by unconditionally
     * lock partition 0.
     *
     * Once we hold the lock, no resizing can happen until the scan ends. So
     * we don't need to repeatedly call ensure_valid_bucket_pointers().
     */
    if (*status).curpartition == -1 {
        Assert!((*status).curbucket == 0);
        ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME((*status).hash_table);

        (*status).curpartition = 0;

        LWLockAcquire(
            PARTITION_LOCK((*status).hash_table, (*status).curpartition as Size),
            if (*status).exclusive {
                LW_EXCLUSIVE
            } else {
                LW_SHARED
            },
        );

        ensure_valid_bucket_pointers((*status).hash_table);

        (*status).nbuckets =
            NUM_BUCKETS((*(*(*status).hash_table).control).size_log2) as c_int;
        next_item_pointer = *(*(*status).hash_table).buckets.add((*status).curbucket as usize);
    } else {
        next_item_pointer = (*status).pnextitem;
    }

    Assert!(LWLockHeldByMeInMode(
        PARTITION_LOCK((*status).hash_table, (*status).curpartition as Size),
        if (*status).exclusive {
            LW_EXCLUSIVE
        } else {
            LW_SHARED
        }
    ));

    /* Move to the next bucket if we finished the current bucket */
    while !DsaPointerIsValid(next_item_pointer) {
        let next_partition: c_int;

        (*status).curbucket += 1;
        if (*status).curbucket >= (*status).nbuckets {
            /* all buckets have been scanned. finish. */
            return core::ptr::null_mut();
        }

        /* Check if move to the next partition */
        next_partition = PARTITION_FOR_BUCKET_INDEX(
            (*status).curbucket as Size,
            (*(*status).hash_table).size_log2,
        ) as c_int;

        if (*status).curpartition != next_partition {
            /*
             * Move to the next partition. Lock the next partition then
             * release the current, not in the reverse order to avoid
             * concurrent resizing.  Avoid dead lock by taking lock in the
             * same order with resize().
             */
            LWLockAcquire(
                PARTITION_LOCK((*status).hash_table, next_partition as Size),
                if (*status).exclusive {
                    LW_EXCLUSIVE
                } else {
                    LW_SHARED
                },
            );
            LWLockRelease(PARTITION_LOCK(
                (*status).hash_table,
                (*status).curpartition as Size,
            ));
            (*status).curpartition = next_partition;
        }

        next_item_pointer = *(*(*status).hash_table).buckets.add((*status).curbucket as usize);
    }

    (*status).curitem =
        dsa_get_address((*(*status).hash_table).area, next_item_pointer) as *mut dshash_table_item;

    /*
     * The caller may delete the item. Store the next item in case of
     * deletion.
     */
    (*status).pnextitem = (*(*status).curitem).next;

    ENTRY_FROM_ITEM((*status).curitem)
}

/*
 * Terminates the seqscan and release all locks.
 *
 * Needs to be called after finishing or when exiting a seqscan.
 */
pub unsafe fn dshash_seq_term(status: *mut dshash_seq_status) {
    if (*status).curpartition >= 0 {
        LWLockRelease(PARTITION_LOCK(
            (*status).hash_table,
            (*status).curpartition as Size,
        ));
    }
}

/*
 * Remove the current entry of the seq scan.
 */
pub unsafe fn dshash_delete_current(status: *mut dshash_seq_status) {
    let hash_table: *mut dshash_table = (*status).hash_table;
    let item: *mut dshash_table_item = (*status).curitem;
    let partition: Size; /* PG_USED_FOR_ASSERTS_ONLY */

    partition = PARTITION_FOR_HASH((*item).hash);

    Assert!((*status).exclusive);
    Assert!((*(*hash_table).control).magic == DSHASH_MAGIC);
    Assert!(LWLockHeldByMeInMode(
        PARTITION_LOCK(hash_table, partition),
        LW_EXCLUSIVE
    ));

    delete_item(hash_table, item);
}

/*
 * Print debugging information about the internal state of the hash table to
 * stderr.  The caller must hold no partition locks.
 */
pub unsafe fn dshash_dump(hash_table: *mut dshash_table) {
    let mut i: Size;
    let mut j: Size;

    Assert!((*(*hash_table).control).magic == DSHASH_MAGIC);
    ASSERT_NO_PARTITION_LOCKS_HELD_BY_ME(hash_table);

    i = 0;
    while i < DSHASH_NUM_PARTITIONS {
        Assert!(!LWLockHeldByMe(PARTITION_LOCK(hash_table, i)));
        LWLockAcquire(PARTITION_LOCK(hash_table, i), LW_SHARED);
        i += 1;
    }

    ensure_valid_bucket_pointers(hash_table);

    eprint!(
        "hash table size = {}\n",
        (1 as Size) << (*hash_table).size_log2
    );
    i = 0;
    while i < DSHASH_NUM_PARTITIONS {
        let partition: *mut dshash_partition = &mut (*(*hash_table).control).partitions[i];
        let begin: Size = BUCKET_INDEX_FOR_PARTITION(i, (*hash_table).size_log2);
        let end: Size = BUCKET_INDEX_FOR_PARTITION(i + 1, (*hash_table).size_log2);

        eprint!("  partition {}\n", i);
        eprint!(
            "    active buckets (key count = {})\n",
            (*partition).count
        );

        j = begin;
        while j < end {
            let mut count: Size = 0;
            let mut bucket: dsa_pointer = *(*hash_table).buckets.add(j);

            while DsaPointerIsValid(bucket) {
                let item: *mut dshash_table_item;

                item = dsa_get_address((*hash_table).area, bucket) as *mut dshash_table_item;

                bucket = (*item).next;
                count += 1;
            }
            eprint!("      bucket {} (key count = {})\n", j, count);
            j += 1;
        }
        i += 1;
    }

    i = 0;
    while i < DSHASH_NUM_PARTITIONS {
        LWLockRelease(PARTITION_LOCK(hash_table, i));
        i += 1;
    }
}

// ----------------------------------------------------------------------------
// Static (private) functions.
// ----------------------------------------------------------------------------

/*
 * Delete a locked item to which we have a pointer.
 */
unsafe fn delete_item(hash_table: *mut dshash_table, item: *mut dshash_table_item) {
    let hash: Size = (*item).hash as Size;
    let partition: Size = PARTITION_FOR_HASH(hash as dshash_hash);

    Assert!(LWLockHeldByMe(PARTITION_LOCK(hash_table, partition)));

    if delete_item_from_bucket(
        hash_table,
        item,
        BUCKET_FOR_HASH(hash_table, hash as dshash_hash),
    ) {
        Assert!((*(*hash_table).control).partitions[partition].count > 0);
        (*(*hash_table).control).partitions[partition].count -= 1;
    } else {
        Assert!(false);
    }
}

/*
 * Grow the hash table if necessary to the requested number of buckets.  The
 * requested size must be double some previously observed size.
 *
 * Must be called without any partition lock held.
 */
unsafe fn resize(hash_table: *mut dshash_table, new_size_log2: Size) {
    let old_buckets: dsa_pointer;
    let new_buckets_shared: dsa_pointer;
    let new_buckets: *mut dsa_pointer;
    let size: Size;
    let new_size: Size = (1 as Size) << new_size_log2;
    let mut i: Size;

    /*
     * Acquire the locks for all lock partitions.  This is expensive, but we
     * shouldn't have to do it many times.
     */
    i = 0;
    while i < DSHASH_NUM_PARTITIONS {
        Assert!(!LWLockHeldByMe(PARTITION_LOCK(hash_table, i)));

        LWLockAcquire(PARTITION_LOCK(hash_table, i), LW_EXCLUSIVE);
        if i == 0 && (*(*hash_table).control).size_log2 >= new_size_log2 {
            /*
             * Another backend has already increased the size; we can avoid
             * obtaining all the locks and return early.
             */
            LWLockRelease(PARTITION_LOCK(hash_table, 0));
            return;
        }
        i += 1;
    }

    Assert!(new_size_log2 == (*(*hash_table).control).size_log2 + 1);

    /* Allocate the space for the new table. */
    new_buckets_shared = dsa_allocate_extended(
        (*hash_table).area,
        core::mem::size_of::<dsa_pointer>() * new_size,
        DSA_ALLOC_HUGE | DSA_ALLOC_ZERO,
    );
    new_buckets = dsa_get_address((*hash_table).area, new_buckets_shared) as *mut dsa_pointer;

    /*
     * We've allocated the new bucket array; all that remains to do now is to
     * reinsert all items, which amounts to adjusting all the pointers.
     */
    size = (1 as Size) << (*(*hash_table).control).size_log2;
    i = 0;
    while i < size {
        let mut item_pointer: dsa_pointer = *(*hash_table).buckets.add(i);

        while DsaPointerIsValid(item_pointer) {
            let item: *mut dshash_table_item;
            let next_item_pointer: dsa_pointer;

            item = dsa_get_address((*hash_table).area, item_pointer) as *mut dshash_table_item;
            next_item_pointer = (*item).next;
            insert_item_into_bucket(
                hash_table,
                item_pointer,
                item,
                new_buckets.add(BUCKET_INDEX_FOR_HASH_AND_SIZE((*item).hash, new_size_log2)),
            );
            item_pointer = next_item_pointer;
        }
        i += 1;
    }

    /* Swap the hash table into place and free the old one. */
    old_buckets = (*(*hash_table).control).buckets;
    (*(*hash_table).control).buckets = new_buckets_shared;
    (*(*hash_table).control).size_log2 = new_size_log2;
    (*hash_table).buckets = new_buckets;
    dsa_free((*hash_table).area, old_buckets);

    /* Release all the locks. */
    i = 0;
    while i < DSHASH_NUM_PARTITIONS {
        LWLockRelease(PARTITION_LOCK(hash_table, i));
        i += 1;
    }
}

/*
 * Make sure that our backend-local bucket pointers are up to date.  The
 * caller must have locked one lock partition, which prevents resize() from
 * running concurrently.
 */
#[inline]
unsafe fn ensure_valid_bucket_pointers(hash_table: *mut dshash_table) {
    if (*hash_table).size_log2 != (*(*hash_table).control).size_log2 {
        (*hash_table).buckets =
            dsa_get_address((*hash_table).area, (*(*hash_table).control).buckets)
                as *mut dsa_pointer;
        (*hash_table).size_log2 = (*(*hash_table).control).size_log2;
    }
}

/*
 * Scan a locked bucket for a match, using the provided compare function.
 */
#[inline]
unsafe fn find_in_bucket(
    hash_table: *mut dshash_table,
    key: *const c_void,
    mut item_pointer: dsa_pointer,
) -> *mut dshash_table_item {
    while DsaPointerIsValid(item_pointer) {
        let item: *mut dshash_table_item;

        item = dsa_get_address((*hash_table).area, item_pointer) as *mut dshash_table_item;
        if equal_keys(hash_table, key, ENTRY_FROM_ITEM(item)) {
            return item;
        }
        item_pointer = (*item).next;
    }
    core::ptr::null_mut()
}

/*
 * Insert an already-allocated item into a bucket.
 */
unsafe fn insert_item_into_bucket(
    _hash_table: *mut dshash_table,
    item_pointer: dsa_pointer,
    item: *mut dshash_table_item,
    bucket: *mut dsa_pointer,
) {
    Assert!(item == dsa_get_address((*_hash_table).area, item_pointer) as *mut dshash_table_item);

    (*item).next = *bucket;
    *bucket = item_pointer;
}

/*
 * Allocate space for an entry with the given key and insert it into the
 * provided bucket.
 */
unsafe fn insert_into_bucket(
    hash_table: *mut dshash_table,
    key: *const c_void,
    bucket: *mut dsa_pointer,
) -> *mut dshash_table_item {
    let item_pointer: dsa_pointer;
    let item: *mut dshash_table_item;

    item_pointer = dsa_allocate(
        (*hash_table).area,
        (*hash_table).params.entry_size + MAXALIGN(core::mem::size_of::<dshash_table_item>()),
    );
    item = dsa_get_address((*hash_table).area, item_pointer) as *mut dshash_table_item;
    copy_key(hash_table, ENTRY_FROM_ITEM(item), key);
    insert_item_into_bucket(hash_table, item_pointer, item, bucket);
    item
}

/*
 * Search a bucket for a matching key and delete it.
 */
unsafe fn delete_key_from_bucket(
    hash_table: *mut dshash_table,
    key: *const c_void,
    mut bucket_head: *mut dsa_pointer,
) -> bool {
    while DsaPointerIsValid(*bucket_head) {
        let item: *mut dshash_table_item;

        item = dsa_get_address((*hash_table).area, *bucket_head) as *mut dshash_table_item;

        if equal_keys(hash_table, key, ENTRY_FROM_ITEM(item)) {
            let next: dsa_pointer;

            next = (*item).next;
            dsa_free((*hash_table).area, *bucket_head);
            *bucket_head = next;

            return true;
        }
        bucket_head = &mut (*item).next;
    }
    false
}

/*
 * Delete the specified item from the bucket.
 */
unsafe fn delete_item_from_bucket(
    hash_table: *mut dshash_table,
    item: *mut dshash_table_item,
    mut bucket_head: *mut dsa_pointer,
) -> bool {
    while DsaPointerIsValid(*bucket_head) {
        let bucket_item: *mut dshash_table_item;

        bucket_item = dsa_get_address((*hash_table).area, *bucket_head) as *mut dshash_table_item;

        if bucket_item == item {
            let next: dsa_pointer;

            next = (*item).next;
            dsa_free((*hash_table).area, *bucket_head);
            *bucket_head = next;
            return true;
        }
        bucket_head = &mut (*bucket_item).next;
    }
    false
}

/*
 * Compute the hash value for a key.
 */
#[inline]
unsafe fn hash_key(hash_table: *mut dshash_table, key: *const c_void) -> dshash_hash {
    ((*hash_table).params.hash_function.unwrap())(
        key,
        (*hash_table).params.key_size,
        (*hash_table).arg,
    )
}

/*
 * Check whether two keys compare equal.
 */
#[inline]
unsafe fn equal_keys(hash_table: *mut dshash_table, a: *const c_void, b: *const c_void) -> bool {
    ((*hash_table).params.compare_function.unwrap())(
        a,
        b,
        (*hash_table).params.key_size,
        (*hash_table).arg,
    ) == 0
}

/*
 * Copy a key.
 */
#[inline]
unsafe fn copy_key(hash_table: *mut dshash_table, dest: *mut c_void, src: *const c_void) {
    ((*hash_table).params.copy_function.unwrap())(
        dest,
        src,
        (*hash_table).params.key_size,
        (*hash_table).arg,
    );
}

// ----------------------------------------------------------------------------
// Local stubs for unported helper functions / dependencies.
// ----------------------------------------------------------------------------

unsafe fn dsa_allocate(_area: *mut dsa_area, _size: Size) -> dsa_pointer {
    unimplemented!() // TODO: utils/dsa.c
}

unsafe fn dsa_allocate_extended(_area: *mut dsa_area, _size: Size, _flags: c_int) -> dsa_pointer {
    unimplemented!() // TODO: utils/dsa.c
}

unsafe fn dsa_free(_area: *mut dsa_area, _dp: dsa_pointer) {
    unimplemented!() // TODO: utils/dsa.c
}

unsafe fn dsa_get_address(_area: *mut dsa_area, _dp: dsa_pointer) -> *mut c_void {
    unimplemented!() // TODO: utils/dsa.c
}

unsafe fn LWLockInitialize(_lock: *mut LWLock, _tranche_id: c_int) {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

pub unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: LWLockMode) -> bool {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

pub unsafe fn LWLockRelease(_lock: *mut LWLock) {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

pub unsafe fn LWLockHeldByMe(_lock: *mut LWLock) -> bool {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

pub unsafe fn LWLockHeldByMeInMode(_lock: *mut LWLock, _mode: LWLockMode) -> bool {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

unsafe fn LWLockAnyHeldByMe(_lock: *mut LWLock, _nlocks: c_int, _stride: Size) -> bool {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

unsafe fn tag_hash(_key: *const c_void, _keysize: Size) -> uint32 {
    unimplemented!() // TODO: common/hashfn.c
}

unsafe fn string_hash(_key: *const c_char, _keysize: Size) -> uint32 {
    unimplemented!() // TODO: common/hashfn.c
}

extern "C" {
    fn memcmp(a: *const c_void, b: *const c_void, n: Size) -> c_int;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strcpy(dest: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strlen(s: *const c_char) -> Size;
}

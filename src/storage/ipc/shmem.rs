//! src/backend/storage/ipc/shmem.c
//!
//! create shared memory and initialize shared memory data structures.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/storage/ipc/shmem.c
//!
//! POSTGRES processes share one or more regions of shared memory.
//! The shared memory is created by a postmaster and is inherited
//! by each backend via fork() (or, in some ports, via other OS-specific
//! methods).  The routines in this file are used for allocating and
//! binding to shared memory data structures.
//!
//! NOTES:
//!		(a) There are three kinds of shared memory data structures
//!	available to POSTGRES: fixed-size structures, queues and hash
//!	tables.  Fixed-size structures contain things like global variables
//!	for a module and should never be allocated after the shared memory
//!	initialization phase.  Hash tables have a fixed maximum size, but
//!	their actual size can vary dynamically.  When entries are added
//!	to the table, more space is allocated.  Queues link data structures
//!	that have been allocated either within fixed-size structures or as hash
//!	buckets.  Each shared data structure has a string name to identify
//!	it (assigned in the module that declares it).
//!
//!		(b) During initialization, each module looks for its
//!	shared data structures in a hash table called the "Shmem Index".
//!	If the data structure is not present, the caller can allocate
//!	a new one and initialize it.  If the data structure is present,
//!	the caller "attaches" to the structure by initializing a pointer
//!	in the local address space.
//!		The shmem index has two purposes: first, it gives us
//!	a simple model of how the world looks when a backend process
//!	initializes.  If something is present in the shmem index,
//!	it is initialized.  If it is not, it is uninitialized.  Second,
//!	the shmem index allows us to allocate shared memory on demand
//!	instead of trying to preallocate structures and hard-wire the
//!	sizes and locations in header files.  If you are using a lot
//!	of shared memory in a lot of different places (and changing
//!	things during development), this is important.
//!
//!		(c) In standard Unix-ish environments, individual backends do not
//!	need to re-establish their local pointers into shared memory, because
//!	they inherit correct values of those variables via fork() from the
//!	postmaster.  However, this does not work in the EXEC_BACKEND case.
//!	In ports using EXEC_BACKEND, new backends have to set up their local
//!	pointers using the method described in (b) above.
//!
//!		(d) memory allocation model: shared memory can never be
//!	freed, once allocated.   Each hash table has its own free list,
//!	so hash buckets can be reused when an item is deleted.  However,
//!	if one hash table grows very large and then shrinks, its space
//!	cannot be redistributed to other tables.  We could build a simple
//!	hash bucket garbage collector if need be.  Right now, it seems
//!	unnecessary.

use crate::prelude::*;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;


use std::ffi::{c_char, c_int, c_void};

// ----------------------------------------------------------------
// shmem.h --- shared memory management structures
//
// Historical note:
// A long time ago, Postgres' shared memory region was allowed to be mapped
// at a different address in each process, and shared memory "pointers" were
// passed around as offsets relative to the start of the shared memory region.
// That is no longer the case: each process must map the shared memory region
// at the same address.  This means shared memory pointers can be passed
// around directly between different processes.
//
// src/include/storage/shmem.h
// ----------------------------------------------------------------

/* size constants for the shmem index table */
/* max size of data structure string name */
pub const SHMEM_INDEX_KEYSIZE: usize = 48;
/* estimated size of the shmem index table (not a hard limit) */
pub const SHMEM_INDEX_SIZE: c_long = 64;

use std::ffi::c_long;

/* this is a hash bucket in the shmem index table */
#[repr(C)]
pub struct ShmemIndexEnt {
    pub key: [c_char; SHMEM_INDEX_KEYSIZE], /* string name */
    pub location: *mut c_void,              /* location in shared mem */
    pub size: Size,                         /* # bytes requested for the structure */
    pub allocated_size: Size,               /* # bytes actually allocated */
}

// ----------------------------------------------------------------
// shmem.c
// ----------------------------------------------------------------

/* shared memory global variables */

static mut ShmemSegHdr: *mut PGShmemHeader = std::ptr::null_mut(); /* shared mem segment header */

static mut ShmemBase: *mut c_void = std::ptr::null_mut(); /* start address of shared memory */

static mut ShmemEnd: *mut c_void = std::ptr::null_mut(); /* end+1 address of shared memory */

/* spinlock for shared memory and LWLock allocation */
pub static mut ShmemLock: *mut slock_t = std::ptr::null_mut();

static mut ShmemIndex: *mut HTAB = std::ptr::null_mut(); /* primary index hashtable for shmem */

/* To get reliable results for NUMA inquiry we need to "touch pages" once */
static mut firstNumaTouch: bool = true;

/*
 *	InitShmemAccess() --- set up basic pointers to shared memory.
 */
pub unsafe fn InitShmemAccess(seghdr: *mut PGShmemHeader) {
    ShmemSegHdr = seghdr;
    ShmemBase = seghdr as *mut c_void;
    ShmemEnd = (ShmemBase as *mut c_char).add((*seghdr).totalsize) as *mut c_void;
}

/*
 *	InitShmemAllocation() --- set up shared-memory space allocation.
 *
 * This should be called only in the postmaster or a standalone backend.
 */
pub unsafe fn InitShmemAllocation() {
    let shmhdr: *mut PGShmemHeader = ShmemSegHdr;
    let aligned: *mut c_char;

    Assert!(!shmhdr.is_null());

    /*
     * Initialize the spinlock used by ShmemAlloc.  We must use
     * ShmemAllocUnlocked, since obviously ShmemAlloc can't be called yet.
     */
    ShmemLock = ShmemAllocUnlocked(std::mem::size_of::<slock_t>()) as *mut slock_t;

    SpinLockInit(ShmemLock);

    /*
     * Allocations after this point should go through ShmemAlloc, which
     * expects to allocate everything on cache line boundaries.  Make sure the
     * first allocation begins on a cache line boundary.
     */
    aligned = CACHELINEALIGN(
        (shmhdr as *mut c_char).add((*shmhdr).freeoffset) as usize
    ) as *mut c_char;
    (*shmhdr).freeoffset = (aligned as usize) - (shmhdr as *mut c_char as usize);

    /* ShmemIndex can't be set up yet (need LWLocks first) */
    (*shmhdr).index = std::ptr::null_mut();
    ShmemIndex = std::ptr::null_mut();
}

/*
 * ShmemAlloc -- allocate max-aligned chunk from shared memory
 *
 * Throws error if request cannot be satisfied.
 *
 * Assumes ShmemLock and ShmemSegHdr are initialized.
 */
pub unsafe fn ShmemAlloc(size: Size) -> *mut c_void {
    let newSpace: *mut c_void;
    let mut allocated_size: Size = 0;

    newSpace = ShmemAllocRaw(size, &mut allocated_size);
    if newSpace.is_null() {
        ereport!(ERROR, "out of shared memory");
    }
    newSpace
}

/*
 * ShmemAllocNoError -- allocate max-aligned chunk from shared memory
 *
 * As ShmemAlloc, but returns NULL if out of space, rather than erroring.
 */
pub unsafe extern "C" fn ShmemAllocNoError(size: Size) -> *mut c_void {
    let mut allocated_size: Size = 0;

    ShmemAllocRaw(size, &mut allocated_size)
}

/*
 * ShmemAllocRaw -- allocate align chunk and return allocated size
 *
 * Also sets *allocated_size to the number of bytes allocated, which will
 * be equal to the number requested plus any padding we choose to add.
 */
unsafe fn ShmemAllocRaw(mut size: Size, allocated_size: *mut Size) -> *mut c_void {
    let newStart: Size;
    let newFree: Size;
    let newSpace: *mut c_void;

    /*
     * Ensure all space is adequately aligned.  We used to only MAXALIGN this
     * space but experience has proved that on modern systems that is not good
     * enough.  Many parts of the system are very sensitive to critical data
     * structures getting split across cache line boundaries.  To avoid that,
     * attempt to align the beginning of the allocation to a cache line
     * boundary.  The calling code will still need to be careful about how it
     * uses the allocated space - e.g. by padding each element in an array of
     * structures out to a power-of-two size - but without this, even that
     * won't be sufficient.
     */
    size = CACHELINEALIGN(size);
    *allocated_size = size;

    Assert!(!ShmemSegHdr.is_null());

    SpinLockAcquire(ShmemLock);

    newStart = (*ShmemSegHdr).freeoffset;

    newFree = newStart + size;
    if newFree <= (*ShmemSegHdr).totalsize {
        newSpace = (ShmemBase as *mut c_char).add(newStart) as *mut c_void;
        (*ShmemSegHdr).freeoffset = newFree;
    } else {
        newSpace = std::ptr::null_mut();
    }

    SpinLockRelease(ShmemLock);

    /* note this assert is okay with newSpace == NULL */
    Assert!(newSpace as usize == CACHELINEALIGN(newSpace as usize));

    newSpace
}

/*
 * ShmemAllocUnlocked -- allocate max-aligned chunk from shared memory
 *
 * Allocate space without locking ShmemLock.  This should be used for,
 * and only for, allocations that must happen before ShmemLock is ready.
 *
 * We consider maxalign, rather than cachealign, sufficient here.
 */
pub unsafe fn ShmemAllocUnlocked(mut size: Size) -> *mut c_void {
    let newStart: Size;
    let newFree: Size;
    let newSpace: *mut c_void;

    /*
     * Ensure allocated space is adequately aligned.
     */
    size = MAXALIGN(size);

    Assert!(!ShmemSegHdr.is_null());

    newStart = (*ShmemSegHdr).freeoffset;

    newFree = newStart + size;
    if newFree > (*ShmemSegHdr).totalsize {
        ereport!(ERROR, "out of shared memory");
    }
    (*ShmemSegHdr).freeoffset = newFree;

    newSpace = (ShmemBase as *mut c_char).add(newStart) as *mut c_void;

    Assert!(newSpace as usize == MAXALIGN(newSpace as usize));

    newSpace
}

/*
 * ShmemAddrIsValid -- test if an address refers to shared memory
 *
 * Returns true if the pointer points within the shared memory segment.
 */
pub unsafe fn ShmemAddrIsValid(addr: *const c_void) -> bool {
    (addr >= ShmemBase as *const c_void) && (addr < ShmemEnd as *const c_void)
}

/*
 *	InitShmemIndex() --- set up or attach to shmem index table.
 */
pub unsafe fn InitShmemIndex() {
    let mut info: HASHCTL = std::mem::zeroed();

    /*
     * Create the shared memory shmem index.
     *
     * Since ShmemInitHash calls ShmemInitStruct, which expects the ShmemIndex
     * hashtable to exist already, we have a bit of a circularity problem in
     * initializing the ShmemIndex itself.  The special "ShmemIndex" hash
     * table name will tell ShmemInitStruct to fake it.
     */
    info.keysize = SHMEM_INDEX_KEYSIZE;
    info.entrysize = std::mem::size_of::<ShmemIndexEnt>();

    ShmemIndex = ShmemInitHash(
        c"ShmemIndex".as_ptr(),
        SHMEM_INDEX_SIZE,
        SHMEM_INDEX_SIZE,
        &mut info,
        (HASH_ELEM | HASH_STRINGS) as c_int,
    );
}

/*
 * ShmemInitHash -- Create and initialize, or attach to, a
 *		shared memory hash table.
 *
 * We assume caller is doing some kind of synchronization
 * so that two processes don't try to create/initialize the same
 * table at once.  (In practice, all creations are done in the postmaster
 * process; child processes should always be attaching to existing tables.)
 *
 * max_size is the estimated maximum number of hashtable entries.  This is
 * not a hard limit, but the access efficiency will degrade if it is
 * exceeded substantially (since it's used to compute directory size and
 * the hash table buckets will get overfull).
 *
 * init_size is the number of hashtable entries to preallocate.  For a table
 * whose maximum size is certain, this should be equal to max_size; that
 * ensures that no run-time out-of-shared-memory failures can occur.
 *
 * *infoP and hash_flags must specify at least the entry sizes and key
 * comparison semantics (see hash_create()).  Flag bits and values specific
 * to shared-memory hash tables are added here, except that callers may
 * choose to specify HASH_PARTITION and/or HASH_FIXED_SIZE.
 *
 * Note: before Postgres 9.0, this function returned NULL for some failure
 * cases.  Now, it always throws error instead, so callers need not check
 * for NULL.
 */
pub unsafe fn ShmemInitHash(
    name: *const c_char, /* table string name for shmem index */
    init_size: c_long,   /* initial table size */
    max_size: c_long,    /* max size of the table */
    infoP: *mut HASHCTL, /* info about key and bucket size */
    mut hash_flags: c_int, /* info about infoP */
) -> *mut HTAB {
    let found: bool;
    let location: *mut c_void;

    /*
     * Hash tables allocated in shared memory have a fixed directory; it can't
     * grow or other backends wouldn't be able to find it. So, make sure we
     * make it big enough to start with.
     *
     * The shared memory allocator must be specified too.
     */
    (*infoP).dsize = hash_select_dirsize(max_size);
    (*infoP).max_dsize = (*infoP).dsize;
    (*infoP).alloc = Some(ShmemAllocNoError);
    hash_flags |= (HASH_SHARED_MEM | HASH_ALLOC | HASH_DIRSIZE) as c_int;

    /* look it up in the shmem index */
    let mut found_local: bool = false;
    location = ShmemInitStruct(
        name,
        hash_get_shared_size(infoP, hash_flags),
        &mut found_local,
    );
    found = found_local;

    /*
     * if it already exists, attach to it rather than allocate and initialize
     * new space
     */
    if found {
        hash_flags |= HASH_ATTACH as c_int;
    }

    /* Pass location of hashtable header to hash_create */
    (*infoP).hctl = location as *mut HASHHDR;

    hash_create(name, init_size, infoP, hash_flags)
}

/*
 * ShmemInitStruct -- Create/attach to a structure in shared memory.
 *
 *		This is called during initialization to find or allocate
 *		a data structure in shared memory.  If no other process
 *		has created the structure, this routine allocates space
 *		for it.  If it exists already, a pointer to the existing
 *		structure is returned.
 *
 *	Returns: pointer to the object.  *foundPtr is set true if the object was
 *		already in the shmem index (hence, already initialized).
 *
 *	Note: before Postgres 9.0, this function returned NULL for some failure
 *	cases.  Now, it always throws error instead, so callers need not check
 *	for NULL.
 */
pub unsafe fn ShmemInitStruct(
    name: *const c_char,
    size: Size,
    foundPtr: *mut bool,
) -> *mut c_void {
    let result: *mut ShmemIndexEnt;
    let structPtr: *mut c_void;

    LWLockAcquire(ShmemIndexLock, LW_EXCLUSIVE);

    if ShmemIndex.is_null() {
        let shmemseghdr: *mut PGShmemHeader = ShmemSegHdr;

        /* Must be trying to create/attach to ShmemIndex itself */
        Assert!(strcmp(name, c"ShmemIndex".as_ptr()) == 0);

        if IsUnderPostmaster {
            /* Must be initializing a (non-standalone) backend */
            Assert!(!(*shmemseghdr).index.is_null());
            structPtr = (*shmemseghdr).index;
            *foundPtr = true;
        } else {
            /*
             * If the shmem index doesn't exist, we are bootstrapping: we must
             * be trying to init the shmem index itself.
             *
             * Notice that the ShmemIndexLock is released before the shmem
             * index has been initialized.  This should be OK because no other
             * process can be accessing shared memory yet.
             */
            Assert!((*shmemseghdr).index.is_null());
            structPtr = ShmemAlloc(size);
            (*shmemseghdr).index = structPtr;
            *foundPtr = false;
        }
        LWLockRelease(ShmemIndexLock);
        return structPtr;
    }

    /* look it up in the shmem index */
    result = hash_search(
        ShmemIndex,
        name as *const c_void,
        HASH_ENTER_NULL,
        foundPtr,
    ) as *mut ShmemIndexEnt;

    if result.is_null() {
        LWLockRelease(ShmemIndexLock);
        ereport!(
            ERROR,
            "could not create ShmemIndex entry for data structure"
        );
    }

    if *foundPtr {
        /*
         * Structure is in the shmem index so someone else has allocated it
         * already.  The size better be the same as the size we are trying to
         * initialize to, or there is a name conflict (or worse).
         */
        if (*result).size != size {
            LWLockRelease(ShmemIndexLock);
            ereport!(ERROR, "ShmemIndex entry size is wrong for data structure");
        }
        structPtr = (*result).location;
    } else {
        let mut allocated_size: Size = 0;

        /* It isn't in the table yet. allocate and initialize it */
        structPtr = ShmemAllocRaw(size, &mut allocated_size);
        if structPtr.is_null() {
            /* out of memory; remove the failed ShmemIndex entry */
            hash_search(
                ShmemIndex,
                name as *const c_void,
                HASH_REMOVE,
                std::ptr::null_mut(),
            );
            LWLockRelease(ShmemIndexLock);
            ereport!(
                ERROR,
                "not enough shared memory for data structure"
            );
        }
        (*result).size = size;
        (*result).allocated_size = allocated_size;
        (*result).location = structPtr;
    }

    LWLockRelease(ShmemIndexLock);

    Assert!(ShmemAddrIsValid(structPtr));

    Assert!(structPtr as usize == CACHELINEALIGN(structPtr as usize));

    structPtr
}

/*
 * Add two Size values, checking for overflow
 */
pub unsafe fn add_size(s1: Size, s2: Size) -> Size {
    let result: Size;

    result = s1.wrapping_add(s2);
    /* We are assuming Size is an unsigned type here... */
    if result < s1 || result < s2 {
        ereport!(ERROR, "requested shared memory size overflows size_t");
    }
    result
}

/*
 * Multiply two Size values, checking for overflow
 */
pub unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    let result: Size;

    if s1 == 0 || s2 == 0 {
        return 0;
    }
    result = s1.wrapping_mul(s2);
    /* We are assuming Size is an unsigned type here... */
    if result / s2 != s1 {
        ereport!(ERROR, "requested shared memory size overflows size_t");
    }
    result
}

/* SQL SRF showing allocated shared memory */
pub unsafe fn pg_get_shmem_allocations(fcinfo: FunctionCallInfo) -> Datum {
    const PG_GET_SHMEM_SIZES_COLS: usize = 4;
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let mut hstat: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut ent: *mut ShmemIndexEnt;
    let mut named_allocated: Size = 0;
    let mut values: [Datum; PG_GET_SHMEM_SIZES_COLS] = [0; PG_GET_SHMEM_SIZES_COLS];
    let mut nulls: [bool; PG_GET_SHMEM_SIZES_COLS] = [false; PG_GET_SHMEM_SIZES_COLS];

    InitMaterializedSRF(fcinfo, 0);

    LWLockAcquire(ShmemIndexLock, LW_SHARED);

    hash_seq_init(&mut hstat, ShmemIndex);

    /* output all allocated entries */
    nulls.iter_mut().for_each(|n| *n = false);
    loop {
        ent = hash_seq_search(&mut hstat) as *mut ShmemIndexEnt;
        if ent.is_null() {
            break;
        }
        values[0] = CStringGetTextDatum((*ent).key.as_ptr());
        values[1] = Int64GetDatum(
            ((*ent).location as *mut c_char as isize - ShmemSegHdr as *mut c_char as isize)
                as int64,
        );
        values[2] = Int64GetDatum((*ent).size as int64);
        values[3] = Int64GetDatum((*ent).allocated_size as int64);
        named_allocated += (*ent).allocated_size;

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    }

    /* output shared memory allocated but not counted via the shmem index */
    values[0] = CStringGetTextDatum(c"<anonymous>".as_ptr());
    nulls[1] = true;
    values[2] = Int64GetDatum(((*ShmemSegHdr).freeoffset - named_allocated) as int64);
    values[3] = values[2];
    tuplestore_putvalues(
        (*rsinfo).setResult,
        (*rsinfo).setDesc,
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
    );

    /* output as-of-yet unused shared memory */
    nulls[0] = true;
    values[1] = Int64GetDatum((*ShmemSegHdr).freeoffset as int64);
    nulls[1] = false;
    values[2] =
        Int64GetDatum(((*ShmemSegHdr).totalsize - (*ShmemSegHdr).freeoffset) as int64);
    values[3] = values[2];
    tuplestore_putvalues(
        (*rsinfo).setResult,
        (*rsinfo).setDesc,
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
    );

    LWLockRelease(ShmemIndexLock);

    0 as Datum
}

/*
 * SQL SRF showing NUMA memory nodes for allocated shared memory
 *
 * Compared to pg_get_shmem_allocations(), this function does not return
 * information about shared anonymous allocations and unused shared memory.
 */
pub unsafe fn pg_get_shmem_allocations_numa(fcinfo: FunctionCallInfo) -> Datum {
    const PG_GET_SHMEM_NUMA_SIZES_COLS: usize = 3;
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let mut hstat: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut ent: *mut ShmemIndexEnt;
    let mut values: [Datum; PG_GET_SHMEM_NUMA_SIZES_COLS] = [0; PG_GET_SHMEM_NUMA_SIZES_COLS];
    let mut nulls: [bool; PG_GET_SHMEM_NUMA_SIZES_COLS] = [false; PG_GET_SHMEM_NUMA_SIZES_COLS];
    let os_page_size: Size;
    let page_ptrs: *mut *mut c_void;
    let pages_status: *mut c_int;
    let shm_total_page_count: uint64;
    let mut shm_ent_page_count: uint64;
    let max_nodes: uint64;
    let nodes: *mut Size;

    if pg_numa_init() == -1 {
        elog!(
            ERROR,
            "libnuma initialization failed or NUMA is not supported on this platform"
        );
    }

    InitMaterializedSRF(fcinfo, 0);

    max_nodes = pg_numa_get_max_node() as uint64;
    nodes = palloc(std::mem::size_of::<Size>() * (max_nodes as usize + 2)) as *mut Size;

    /*
     * Shared memory allocations can vary in size and may not align with OS
     * memory page boundaries, while NUMA queries work on pages.
     *
     * To correctly map each allocation to NUMA nodes, we need to: 1.
     * Determine the OS memory page size. 2. Align each allocation's start/end
     * addresses to page boundaries. 3. Query NUMA node information for all
     * pages spanning the allocation.
     */
    os_page_size = pg_get_shmem_pagesize();

    /*
     * Allocate memory for page pointers and status based on total shared
     * memory size. This simplified approach allocates enough space for all
     * pages in shared memory rather than calculating the exact requirements
     * for each segment.
     *
     * Add 1, because we don't know how exactly the segments align to OS
     * pages, so the allocation might use one more memory page. In practice
     * this is not very likely, and moreover we have more entries, each of
     * them using only fraction of the total pages.
     */
    shm_total_page_count = ((*ShmemSegHdr).totalsize as uint64 / os_page_size as uint64) + 1;
    page_ptrs = palloc0(std::mem::size_of::<*mut c_void>() * shm_total_page_count as usize)
        as *mut *mut c_void;
    pages_status =
        palloc(std::mem::size_of::<c_int>() * shm_total_page_count as usize) as *mut c_int;

    if firstNumaTouch {
        elog!(
            DEBUG1,
            "NUMA: page-faulting shared memory segments for proper NUMA readouts"
        );
    }

    LWLockAcquire(ShmemIndexLock, LW_SHARED);

    hash_seq_init(&mut hstat, ShmemIndex);

    /* output all allocated entries */
    loop {
        ent = hash_seq_search(&mut hstat) as *mut ShmemIndexEnt;
        if ent.is_null() {
            break;
        }
        let mut i: c_int;
        let startptr: *mut c_char;
        let endptr: *mut c_char;
        let total_len: Size;

        /*
         * Calculate the range of OS pages used by this segment. The segment
         * may start / end half-way through a page, we want to count these
         * pages too. So we align the start/end pointers down/up, and then
         * calculate the number of pages from that.
         */
        startptr = TYPEALIGN_DOWN(os_page_size, (*ent).location as usize) as *mut c_char;
        endptr = TYPEALIGN(
            os_page_size,
            ((*ent).location as *mut c_char).add((*ent).allocated_size) as usize,
        ) as *mut c_char;
        total_len = (endptr as usize) - (startptr as usize);

        shm_ent_page_count = total_len as uint64 / os_page_size as uint64;

        /*
         * If we ever get 0xff (-1) back from kernel inquiry, then we probably
         * have a bug in mapping buffers to OS pages.
         */
        std::ptr::write_bytes(
            pages_status,
            0xff,
            std::mem::size_of::<c_int>() * shm_ent_page_count as usize / std::mem::size_of::<c_int>(),
        );

        /*
         * Setup page_ptrs[] with pointers to all OS pages for this segment,
         * and get the NUMA status using pg_numa_query_pages.
         *
         * In order to get reliable results we also need to touch memory
         * pages, so that inquiry about NUMA memory node doesn't return -2
         * (ENOENT, which indicates unmapped/unallocated pages).
         */
        i = 0;
        while (i as uint64) < shm_ent_page_count {
            *page_ptrs.add(i as usize) =
                startptr.add(i as usize * os_page_size) as *mut c_void;

            if firstNumaTouch {
                pg_numa_touch_mem_if_required(*page_ptrs.add(i as usize));
            }

            CHECK_FOR_INTERRUPTS();
            i += 1;
        }

        if pg_numa_query_pages(0, shm_ent_page_count as c_int, page_ptrs, pages_status) == -1 {
            elog!(ERROR, "failed NUMA pages inquiry status");
        }

        /* Count number of NUMA nodes used for this shared memory entry */
        std::ptr::write_bytes(nodes, 0, max_nodes as usize + 2);

        i = 0;
        while (i as uint64) < shm_ent_page_count {
            let s: c_int = *pages_status.add(i as usize);

            /* Ensure we are adding only valid index to the array */
            if s >= 0 && (s as uint64) <= max_nodes {
                /* valid NUMA node */
                *nodes.add(s as usize) += 1;
                i += 1;
                continue;
            } else if s == -2 {
                /* -2 means ENOENT (e.g. page was moved to swap) */
                *nodes.add(max_nodes as usize + 1) += 1;
                i += 1;
                continue;
            }

            elog!(
                ERROR,
                "invalid NUMA node id outside of allowed range [0, {}]: {}",
                max_nodes,
                s
            );
        }

        /* no NULLs for regular nodes */
        nulls.iter_mut().for_each(|n| *n = false);

        /*
         * Add one entry for each NUMA node, including those without allocated
         * memory for this segment.
         */
        i = 0;
        while (i as uint64) <= max_nodes {
            values[0] = CStringGetTextDatum((*ent).key.as_ptr());
            values[1] = i as Datum;
            values[2] = Int64GetDatum((*nodes.add(i as usize) * os_page_size) as int64);

            tuplestore_putvalues(
                (*rsinfo).setResult,
                (*rsinfo).setDesc,
                values.as_mut_ptr(),
                nulls.as_mut_ptr(),
            );
            i += 1;
        }

        /* The last entry is used for pages without a NUMA node. */
        nulls[1] = true;
        values[0] = CStringGetTextDatum((*ent).key.as_ptr());
        values[2] =
            Int64GetDatum((*nodes.add(max_nodes as usize + 1) * os_page_size) as int64);

        tuplestore_putvalues(
            (*rsinfo).setResult,
            (*rsinfo).setDesc,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
    }

    LWLockRelease(ShmemIndexLock);
    firstNumaTouch = false;

    0 as Datum
}

/*
 * Determine the memory page size used for the shared memory segment.
 *
 * If the shared segment was allocated using huge pages, returns the size of
 * a huge page. Otherwise returns the size of regular memory page.
 *
 * This should be used only after the server is started.
 */
pub unsafe fn pg_get_shmem_pagesize() -> Size {
    let mut os_page_size: Size;

    os_page_size = sysconf(_SC_PAGESIZE) as Size;

    Assert!(IsUnderPostmaster);
    Assert!(huge_pages_status != HUGE_PAGES_UNKNOWN);

    if huge_pages_status == HUGE_PAGES_ON {
        GetHugePageSize(&mut os_page_size, std::ptr::null_mut());
    }

    os_page_size
}

pub unsafe fn pg_numa_available(_fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL(pg_numa_init() != -1)
}

// ----------------------------------------------------------------
// Local stubs for unported dependencies
// ----------------------------------------------------------------

pub use crate::storage::pg_shmem::PGShmemHeader;

pub use crate::utils::hash::dynahash::{HTAB, HASHHDR, HASHCTL, HASH_SEQ_STATUS};

#[repr(C)]
pub struct ReturnSetInfo {
    pub setResult: *mut c_void,
    pub setDesc: *mut c_void,
}

pub type slock_t = c_int;
pub type FunctionCallInfo = *mut FunctionCallInfoBaseData;

#[repr(C)]
pub struct FunctionCallInfoBaseData {
    pub resultinfo: *mut c_void,
}

/* hash_create flags (canonical, from dynahash) */
pub use crate::utils::hash::dynahash::{HASH_ELEM, HASH_STRINGS, HASH_SHARED_MEM, HASH_ALLOC, HASH_DIRSIZE, HASH_ATTACH};

/* hash_search action */
pub const HASH_ENTER_NULL: c_int = 3;
pub const HASH_REMOVE: c_int = 2;

/* LWLock modes */
pub const LW_EXCLUSIVE: c_int = 0;
pub const LW_SHARED: c_int = 1;

/* huge pages status */
pub const HUGE_PAGES_UNKNOWN: c_int = 0;
pub const HUGE_PAGES_ON: c_int = 1;

extern "C" {
    pub static mut ShmemIndexLock: *mut c_void;
    pub static mut IsUnderPostmaster: bool;
    pub static mut huge_pages_status: c_int;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn sysconf(name: c_int) -> c_long;
}

const _SC_PAGESIZE: c_int = 29;

unsafe fn SpinLockInit(_lock: *mut slock_t) {
    // TODO: storage/spin.h
}
unsafe fn SpinLockAcquire(_lock: *mut slock_t) {
    // TODO: storage/spin.h
}
unsafe fn SpinLockRelease(_lock: *mut slock_t) {
    // TODO: storage/spin.h
}

unsafe fn LWLockAcquire(lock: *mut c_void, mode: c_int) -> bool {
    use crate::storage::lmgr::lwlock::LWLockMode;
    let m = if mode == LW_SHARED {
        LWLockMode::LW_SHARED
    } else {
        LWLockMode::LW_EXCLUSIVE
    };
    crate::storage::lmgr::lwlock::LWLockAcquire(lock as _, m)
}
unsafe fn LWLockRelease(lock: *mut c_void) {
    crate::storage::lmgr::lwlock::LWLockRelease(lock as _)
}

unsafe fn hash_select_dirsize(ntuples: c_long) -> c_long {
    crate::utils::hash::dynahash::hash_select_dirsize(ntuples)
}
unsafe fn hash_get_shared_size(info: *mut HASHCTL, flags: c_int) -> Size {
    crate::utils::hash::dynahash::hash_get_shared_size(info as _, flags)
}
unsafe fn hash_create(
    tabname: *const c_char,
    nelem: c_long,
    info: *mut HASHCTL,
    flags: c_int,
) -> *mut HTAB {
    crate::utils::hash::dynahash::hash_create(tabname, nelem, info as _, flags) as *mut HTAB
}
unsafe fn hash_search(
    hashp: *mut HTAB,
    keyPtr: *const c_void,
    action: c_int,
    foundPtr: *mut bool,
) -> *mut c_void {
    use crate::utils::hash::dynahash::HASHACTION;
    let a = match action {
        0 => HASHACTION::HASH_FIND,
        1 => HASHACTION::HASH_ENTER,
        2 => HASHACTION::HASH_REMOVE,
        _ => HASHACTION::HASH_ENTER_NULL,
    };
    crate::utils::hash::dynahash::hash_search(hashp as _, keyPtr, a, foundPtr)
}
unsafe fn hash_seq_init(status: *mut HASH_SEQ_STATUS, hashp: *mut HTAB) {
    crate::utils::hash::dynahash::hash_seq_init(status as _, hashp as _)
}
unsafe fn hash_seq_search(status: *mut HASH_SEQ_STATUS) -> *mut c_void {
    crate::utils::hash::dynahash::hash_seq_search(status as _)
}

unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn tuplestore_putvalues(
    _state: *mut c_void,
    _tdesc: *mut c_void,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: utils/fmgr/fmgr.c
}
unsafe fn Int64GetDatum(_val: int64) -> Datum {
    unimplemented!() // TODO: utils/adt/int8.c
}
unsafe fn PG_RETURN_BOOL(_b: bool) -> Datum {
    unimplemented!() // TODO: fmgr.h
}

unsafe fn pg_numa_init() -> c_int {
    unimplemented!() // TODO: port/pg_numa.c
}
unsafe fn pg_numa_get_max_node() -> c_int {
    unimplemented!() // TODO: port/pg_numa.c
}
unsafe fn pg_numa_query_pages(
    _pid: c_int,
    _count: c_int,
    _pages: *mut *mut c_void,
    _status: *mut c_int,
) -> c_int {
    unimplemented!() // TODO: port/pg_numa.c
}
unsafe fn pg_numa_touch_mem_if_required(_ptr: *mut c_void) {
    unimplemented!() // TODO: port/pg_numa.h
}

unsafe fn GetHugePageSize(_hugepagesize: *mut Size, _mmap_flags: *mut c_int) {
    unimplemented!() // TODO: port/sysv_shmem.c
}

/* CACHELINEALIGN/TYPEALIGN helpers - const-fn style */
const PG_CACHE_LINE_SIZE: usize = 128;

#[inline]
fn TYPEALIGN(alignval: Size, len: usize) -> usize {
    (len + (alignval - 1)) & !(alignval - 1)
}
#[inline]
fn TYPEALIGN_DOWN(alignval: Size, len: usize) -> usize {
    len & !(alignval - 1)
}
#[inline]
fn CACHELINEALIGN(len: usize) -> usize {
    TYPEALIGN(PG_CACHE_LINE_SIZE, len)
}

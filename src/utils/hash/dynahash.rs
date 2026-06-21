//! Translation of:
//!   - postgres/src/include/utils/hsearch.h         (HTAB/HASHCTL/HASHELEMENT/...
//!     public types, HASH_* flags, HASHACTION, and the dynahash.c prototypes)
//!   - postgres/src/backend/utils/hash/dynahash.c   (dynamic chained hash tables)
//!
//! dynahash.c supports both local-to-a-backend hash tables and hash tables in
//! shared memory.  For shared hash tables, it is the caller's responsibility
//! to provide appropriate access interlocking.  The simplest convention is
//! that a single LWLock protects the whole hash table.  Searches (HASH_FIND or
//! hash_seq_search) need only shared lock, but any update requires exclusive
//! lock.  For heavily-used shared tables, the single-lock approach creates a
//! concurrency bottleneck, so we also support "partitioned" locking wherein
//! there are multiple LWLocks guarding distinct subsets of the table.  To use
//! a hash table in partitioned mode, the HASH_PARTITION flag must be given
//! to hash_create.  This prevents any attempt to split buckets on-the-fly.
//! Therefore, each hash bucket chain operates independently, and no fields
//! of the hash header change after init except nentries and freeList.
//! (A partitioned table uses multiple copies of those fields, guarded by
//! spinlocks, for additional concurrency.)
//! This lets any subset of the hash buckets be treated as a separately
//! lockable partition.  We expect callers to use the low-order bits of a
//! lookup key's hash value as a partition number --- this will work because
//! of the way calc_bucket() maps hash values to bucket numbers.
//!
//! For hash tables in shared memory, the memory allocator function should
//! match malloc's semantics of returning NULL on failure.  For hash tables
//! in local memory, we typically use palloc() which will throw error on
//! failure.  The code in this file has to cope with both cases.
//!
//! dynahash.c provides support for these types of lookup keys:
//!
//! 1. Null-terminated C strings (truncated if necessary to fit in keysize),
//! compared as though by strcmp().  This is selected by specifying the
//! HASH_STRINGS flag to hash_create.
//!
//! 2. Arbitrary binary data of size keysize, compared as though by memcmp().
//! (Caller must ensure there are no undefined padding bits in the keys!)
//! This is selected by specifying the HASH_BLOBS flag to hash_create.
//!
//! 3. More complex key behavior can be selected by specifying user-supplied
//! hashing, comparison, and/or key-copying functions.  At least a hashing
//! function must be supplied; comparison defaults to memcmp() and key copying
//! to memcpy() when a user-defined hashing function is selected.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Original comments:
//!
//! Dynamic hashing, after CACM April 1988 pp 446-457, by Per-Ake Larson.
//! Coded into C, with minor code improvements, and with hsearch(3) interface,
//! by ejp@ausmelb.oz, Jul 26, 1988: 13:16;
//! also, hcreate/hdestroy routines added to simulate hsearch(3).
//!
//! These routines simulate hsearch(3) and family, with the important
//! difference that the hash table is dynamic - can grow indefinitely
//! beyond its original size (as supplied to hcreate()).

use crate::prelude::*;

use crate::common::hashfn::{string_hash, tag_hash, uint32_hash};
use crate::port::pg_bitutils::{pg_ceil_log2_32, pg_ceil_log2_64};
use crate::utils::palloc::MCXT_ALLOC_NO_OOM;

/* ==================================================================== */
/*  hsearch.h: exported definitions                                     */
/* ==================================================================== */

/*
 * Hash functions must have this signature.
 */
pub type HashValueFunc = Option<unsafe extern "C" fn(key: *const c_void, keysize: Size) -> uint32>;

/*
 * Key comparison functions must have this signature.  Comparison functions
 * return zero for match, nonzero for no match.  (The comparison function
 * definition is designed to allow memcmp() and strncmp() to be used directly
 * as key comparison functions.)
 */
pub type HashCompareFunc =
    Option<unsafe extern "C" fn(key1: *const c_void, key2: *const c_void, keysize: Size) -> c_int>;

/*
 * Key copying functions must have this signature.  The return value is not
 * used.  (The definition is set up to allow memcpy() and strlcpy() to be
 * used directly.)
 */
pub type HashCopyFunc =
    Option<unsafe extern "C" fn(dest: *mut c_void, src: *const c_void, keysize: Size) -> *mut c_void>;

/*
 * Space allocation function for a hashtable --- designed to match malloc().
 * Note: there is no free function API; can't destroy a hashtable unless you
 * use the default allocator.
 */
pub type HashAllocFunc = Option<unsafe extern "C" fn(request: Size) -> *mut c_void>;

/*
 * HASHELEMENT is the private part of a hashtable entry.  The caller's data
 * follows the HASHELEMENT structure (on a MAXALIGN'd boundary).  The hash key
 * is expected to be at the start of the caller's hash entry data structure.
 */
#[repr(C)]
pub struct HASHELEMENT {
    /// link to next entry in same bucket
    pub link: *mut HASHELEMENT,
    /// hash function result for this entry
    pub hashvalue: uint32,
}

/* Parameter data structure for hash_create */
/* Only those fields indicated by hash_flags need be set */
#[repr(C)]
pub struct HASHCTL {
    /* Used if HASH_PARTITION flag is set: */
    /// # partitions (must be power of 2)
    pub num_partitions: c_long,
    /* Used if HASH_SEGMENT flag is set: */
    /// segment size
    pub ssize: c_long,
    /* Used if HASH_DIRSIZE flag is set: */
    /// (initial) directory size
    pub dsize: c_long,
    /// limit to dsize if dir size is limited
    pub max_dsize: c_long,
    /* Used if HASH_ELEM flag is set (which is now required): */
    /// hash key length in bytes
    pub keysize: Size,
    /// total user element size in bytes
    pub entrysize: Size,
    /* Used if HASH_FUNCTION flag is set: */
    /// hash function
    pub hash: HashValueFunc,
    /* Used if HASH_COMPARE flag is set: */
    /// key comparison function
    pub r#match: HashCompareFunc,
    /* Used if HASH_KEYCOPY flag is set: */
    /// key copying function
    pub keycopy: HashCopyFunc,
    /* Used if HASH_ALLOC flag is set: */
    /// memory allocator
    pub alloc: HashAllocFunc,
    /* Used if HASH_CONTEXT flag is set: */
    /// memory context to use for allocations
    pub hcxt: MemoryContext,
    /* Used if HASH_SHARED_MEM flag is set: */
    /// location of header in shared mem
    pub hctl: *mut HASHHDR,
}

/* Flag bits for hash_create; most indicate which parameters are supplied */
/// Hashtable is used w/partitioned locking
pub const HASH_PARTITION: c_int = 0x0001;
/// Set segment size
pub const HASH_SEGMENT: c_int = 0x0002;
/// Set directory size (initial and max)
pub const HASH_DIRSIZE: c_int = 0x0004;
/// Set keysize and entrysize (now required!)
pub const HASH_ELEM: c_int = 0x0008;
/// Select support functions for string keys
pub const HASH_STRINGS: c_int = 0x0010;
/// Select support functions for binary keys
pub const HASH_BLOBS: c_int = 0x0020;
/// Set user defined hash function
pub const HASH_FUNCTION: c_int = 0x0040;
/// Set user defined comparison function
pub const HASH_COMPARE: c_int = 0x0080;
/// Set user defined key-copying function
pub const HASH_KEYCOPY: c_int = 0x0100;
/// Set memory allocator
pub const HASH_ALLOC: c_int = 0x0200;
/// Set memory allocation context
pub const HASH_CONTEXT: c_int = 0x0400;
/// Hashtable is in shared memory
pub const HASH_SHARED_MEM: c_int = 0x0800;
/// Do not initialize hctl
pub const HASH_ATTACH: c_int = 0x1000;
/// Initial size is a hard limit
pub const HASH_FIXED_SIZE: c_int = 0x2000;

/// max_dsize value to indicate expansible directory
pub const NO_MAX_DSIZE: c_long = -1;

/* hash_search operations */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum HASHACTION {
    HASH_FIND,
    HASH_ENTER,
    HASH_REMOVE,
    HASH_ENTER_NULL,
}
pub use HASHACTION::*;

/* hash_seq status (should be considered an opaque type by callers) */
#[repr(C)]
pub struct HASH_SEQ_STATUS {
    pub hashp: *mut HTAB,
    /// index of current bucket
    pub curBucket: uint32,
    /// current entry in bucket
    pub curEntry: *mut HASHELEMENT,
    /// true if hashvalue was provided
    pub hasHashvalue: bool,
    /// hashvalue to start seqscan over hash
    pub hashvalue: uint32,
}

/* ==================================================================== */
/*  Stubs for not-yet-translated subsystems                            */
/* ==================================================================== */

// TODO(pg-port): storage/spin.h -- single-process port, so spinlocks are no-ops.
// The freelist mutex field stays in the struct but lock ops do nothing; real
// locking arrives with the shared-memory/concurrency work.
type slock_t = c_int;

#[inline]
unsafe fn SpinLockInit(_lock: *mut slock_t) {}
#[inline]
unsafe fn SpinLockAcquire(_lock: *mut slock_t) {}
#[inline]
unsafe fn SpinLockRelease(_lock: *mut slock_t) {}

// TODO(pg-port): storage/shmem.h -- ShmemAlloc is only reached on the shared
// (HASH_SHARED_MEM) path, which this port does not yet support.
#[allow(dead_code)]
unsafe fn ShmemAlloc(_size: Size) -> *mut c_void {
    unimplemented!("TODO(pg-port): shared memory")
}
#[allow(dead_code)]
unsafe fn ShmemAllocNoError(_size: Size) -> *mut c_void { crate::storage::ipc::shmem::ShmemAllocNoError(_size) }

// TODO(pg-port): access/xact.h -- transaction nest level.  The seq-scan tracking
// records the current subtransaction nest level so AtEOSubXact_HashTables can
// clean up leaked scans.  Until xact.c is translated, treat everything as the
// top transaction level.
#[inline]
unsafe fn GetCurrentTransactionNestLevel() -> c_int {
    1
}

// TopMemoryContext is the permanent backend context (utils/memutils.h). It comes
// from the prelude (a non-NULL bootstrap sentinel); allocations land in the global
// allocator regardless of context.

// TODO(pg-port): storage/shmem.h size helpers add_size()/mul_size() detect
// overflow and ereport; here they reproduce the overflow check faithfully.
#[inline]
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    let result = s1.wrapping_add(s2);
    /* We are assuming Size is an unsigned type here... */
    if result < s1 || result < s2 {
        ereport!(ERROR, errmsg!("requested shared memory size overflows size_t"));
    }
    result
}

#[inline]
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    if s1 == 0 || s2 == 0 {
        return 0;
    }
    let result = s1.wrapping_mul(s2);
    /* We are assuming Size is an unsigned type here, so the test below works. */
    if result / s2 != s1 {
        ereport!(ERROR, errmsg!("requested shared memory size overflows size_t"));
    }
    result
}

/*
 * C-ABI wrappers around the libc / crate routines used as default callback
 * function pointers.  The C source stores bare function pointers (memcmp,
 * strncmp, strlcpy, memcpy, the hashfn.c hashes, DynaHashAlloc) and both calls
 * and *compares* them (e.g. `hashp->hash == string_hash`).  We reproduce that
 * by giving every default an `extern "C"` wrapper with the matching ABI and
 * comparing pointers via fn_eq() below.
 */
extern "C" {
    fn memcmp(s1: *const c_void, s2: *const c_void, n: Size) -> c_int;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: Size) -> c_int;
}

/* HASHELEMENT is a #[repr(C)] struct above; the hashfn.c hashes are plain Rust
 * fns, so wrap them with the C ABI so they share the HashValueFunc type. */
unsafe extern "C" fn string_hash_c(key: *const c_void, keysize: Size) -> uint32 {
    string_hash(key, keysize)
}
unsafe extern "C" fn tag_hash_c(key: *const c_void, keysize: Size) -> uint32 {
    tag_hash(key, keysize)
}
unsafe extern "C" fn uint32_hash_c(key: *const c_void, keysize: Size) -> uint32 {
    uint32_hash(key, keysize)
}

unsafe extern "C" fn memcmp_c(key1: *const c_void, key2: *const c_void, keysize: Size) -> c_int {
    memcmp(key1, key2, keysize)
}

unsafe extern "C" fn memcpy_c(dest: *mut c_void, src: *const c_void, keysize: Size) -> *mut c_void {
    memcpy(dest, src, keysize)
}

/*
 * The signature of keycopy is meant for memcpy(), which returns void*, but
 * strlcpy() returns size_t.  Since we never use the return value of keycopy,
 * this wrapper is the equivalent of the C cast to HashCopyFunc.
 */
unsafe extern "C" fn strlcpy_c(dest: *mut c_void, src: *const c_void, keysize: Size) -> *mut c_void {
    crate::port::strlcpy::strlcpy(dest as *mut c_char, src as *const c_char, keysize);
    dest
}

/* Compare two Option<fn ptr> by their raw addresses (C `==` on fn pointers).
 * A bare fn pointer is pointer-sized, so reading its bytes as a usize yields the
 * code address, which is exactly what the C `==` compares. */
#[inline]
fn fn_eq<T: Copy>(a: Option<T>, b: T) -> bool {
    debug_assert!(core::mem::size_of::<T>() == core::mem::size_of::<usize>());
    match a {
        Some(f) => {
            let pa = unsafe { *(&f as *const T as *const usize) };
            let pb = unsafe { *(&b as *const T as *const usize) };
            pa == pb
        }
        None => false,
    }
}

/* ==================================================================== */
/*  dynahash.c                                                          */
/* ==================================================================== */

/*
 * Constants
 *
 * A hash table has a top-level "directory", each of whose entries points
 * to a "segment" of ssize bucket headers.  The maximum number of hash
 * buckets is thus dsize * ssize (but dsize may be expansible).  Of course,
 * the number of records in the table can be larger, but we don't want a
 * whole lot of records per bucket or performance goes down.
 *
 * In a hash table allocated in shared memory, the directory cannot be
 * expanded because it must stay at a fixed address.  The directory size
 * should be selected using hash_select_dirsize (and you'd better have
 * a good idea of the maximum number of entries!).  For non-shared hash
 * tables, the initial directory size can be left at the default.
 */
const DEF_SEGSIZE: c_long = 256;
/// must be log2(DEF_SEGSIZE)
const DEF_SEGSIZE_SHIFT: c_int = 8;
const DEF_DIRSIZE: c_long = 256;

/// Number of freelists to be used for a partitioned hash table.
const NUM_FREELISTS: usize = 32;

/* A hash bucket is a linked list of HASHELEMENTs */
type HASHBUCKET = *mut HASHELEMENT;

/* A hash segment is an array of bucket headers */
type HASHSEGMENT = *mut HASHBUCKET;

/*
 * Per-freelist data.
 *
 * In a partitioned hash table, each freelist is associated with a specific
 * set of hashcodes, as determined by the FREELIST_IDX() macro below.
 * nentries tracks the number of live hashtable entries having those hashcodes
 * (NOT the number of entries in the freelist, as you might expect).
 *
 * The coverage of a freelist might be more or less than one partition, so it
 * needs its own lock rather than relying on caller locking.  Relying on that
 * wouldn't work even if the coverage was the same, because of the occasional
 * need to "borrow" entries from another freelist; see get_hash_entry().
 *
 * Using an array of FreeListData instead of separate arrays of mutexes,
 * nentries and freeLists helps to reduce sharing of cache lines between
 * different mutexes.
 */
#[repr(C)]
struct FreeListData {
    /// spinlock for this freelist
    mutex: slock_t,
    /// number of entries in associated buckets
    nentries: c_long,
    /// chain of free elements
    freeList: *mut HASHELEMENT,
}

/*
 * Header structure for a hash table --- contains all changeable info
 *
 * In a shared-memory hash table, the HASHHDR is in shared memory, while
 * each backend has a local HTAB struct.  For a non-shared table, there isn't
 * any functional difference between HASHHDR and HTAB, but we separate them
 * anyway to share code between shared and non-shared tables.
 */
#[repr(C)]
pub struct HASHHDR {
    /*
     * The freelist can become a point of contention in high-concurrency hash
     * tables, so we use an array of freelists, each with its own mutex and
     * nentries count, instead of just a single one.  Although the freelists
     * normally operate independently, we will scavenge entries from freelists
     * other than a hashcode's default freelist when necessary.
     *
     * If the hash table is not partitioned, only freeList[0] is used and its
     * spinlock is not used at all; callers' locking is assumed sufficient.
     */
    freeList: [FreeListData; NUM_FREELISTS],

    /* These fields can change, but not in a partitioned table */
    /* Also, dsize can't change in a shared table, even if unpartitioned */
    /// directory size
    dsize: c_long,
    /// number of allocated segments (<= dsize)
    nsegs: c_long,
    /// ID of maximum bucket in use
    max_bucket: uint32,
    /// mask to modulo into entire table
    high_mask: uint32,
    /// mask to modulo into lower half of table
    low_mask: uint32,

    /* These fields are fixed at hashtable creation */
    /// hash key length in bytes
    keysize: Size,
    /// total user element size in bytes
    entrysize: Size,
    /// # partitions (must be power of 2), or 0
    num_partitions: c_long,
    /// 'dsize' limit if directory is fixed size
    max_dsize: c_long,
    /// segment size --- must be power of 2
    ssize: c_long,
    /// segment shift = log2(ssize)
    sshift: c_int,
    /// number of entries to allocate at once
    nelem_alloc: c_int,
    // HASH_STATISTICS fields (accesses, collisions) are omitted: not compiled in.
}

#[inline]
unsafe fn IS_PARTITIONED(hctl: *const HASHHDR) -> bool {
    (*hctl).num_partitions != 0
}

#[inline]
unsafe fn FREELIST_IDX(hctl: *const HASHHDR, hashcode: uint32) -> c_int {
    if IS_PARTITIONED(hctl) {
        (hashcode as usize % NUM_FREELISTS) as c_int
    } else {
        0
    }
}

/*
 * Top control structure for a hashtable --- in a shared table, each backend
 * has its own copy (OK since no fields change at runtime)
 */
#[repr(C)]
pub struct HTAB {
    /// => shared control information
    hctl: *mut HASHHDR,
    /// directory of segment starts
    dir: *mut HASHSEGMENT,
    /// hash function
    hash: HashValueFunc,
    /// key comparison function
    r#match: HashCompareFunc,
    /// key copying function
    keycopy: HashCopyFunc,
    /// memory allocator
    alloc: HashAllocFunc,
    /// memory context if default allocator used
    hcxt: MemoryContext,
    /// table name (for error messages)
    tabname: *mut c_char,
    /// true if table is in shared memory
    isshared: bool,
    /// if true, don't enlarge
    isfixed: bool,

    /* freezing a shared table isn't allowed, so we can keep state here */
    /// true = no more inserts allowed
    frozen: bool,

    /* We keep local copies of these fixed values to reduce contention */
    /// hash key length in bytes
    keysize: Size,
    /// segment size --- must be power of 2
    ssize: c_long,
    /// segment shift = log2(ssize)
    sshift: c_int,
}

/*
 * Key (also entry) part of a HASHELEMENT
 */
#[inline]
unsafe fn ELEMENTKEY(helem: *mut HASHELEMENT) -> *mut c_void {
    (helem as *mut c_char).add(MAXALIGN(core::mem::size_of::<HASHELEMENT>())) as *mut c_void
}

/*
 * Obtain element pointer given pointer to key
 */
#[inline]
unsafe fn ELEMENT_FROM_KEY(key: *mut c_void) -> *mut HASHELEMENT {
    (key as *mut c_char).sub(MAXALIGN(core::mem::size_of::<HASHELEMENT>())) as *mut HASHELEMENT
}

/*
 * Fast MOD arithmetic, assuming that y is a power of 2 !
 */
#[inline]
fn MOD(x: c_long, y: c_long) -> c_long {
    x & (y - 1)
}

/*
 * memory allocation support
 */
static mut CurrentDynaHashCxt: MemoryContext = null_mut();

unsafe extern "C" fn DynaHashAlloc(size: Size) -> *mut c_void {
    Assert!(MemoryContextIsValid(CurrentDynaHashCxt));
    MemoryContextAllocExtended(CurrentDynaHashCxt, size, MCXT_ALLOC_NO_OOM)
}

/*
 * HashCompareFunc for string keys
 *
 * Because we copy keys with strlcpy(), they will be truncated at keysize-1
 * bytes, so we can only compare that many ... hence strncmp is almost but
 * not quite the right thing.
 */
unsafe extern "C" fn string_compare(key1: *const c_void, key2: *const c_void, keysize: Size) -> c_int {
    strncmp(key1 as *const c_char, key2 as *const c_char, keysize - 1)
}

/************************** CREATE ROUTINES **********************/

/*
 * hash_create -- create a new dynamic hash table
 *
 *	tabname: a name for the table (for debugging purposes)
 *	nelem: maximum number of elements expected
 *	*info: additional table parameters, as indicated by flags
 *	flags: bitmask indicating which parameters to take from *info
 *
 * The flags value *must* include HASH_ELEM.  (Formerly, this was nominally
 * optional, but the default keysize and entrysize values were useless.)
 * The flags value must also include exactly one of HASH_STRINGS, HASH_BLOBS,
 * or HASH_FUNCTION, to define the key hashing semantics (C strings,
 * binary blobs, or custom, respectively).  Callers specifying a custom
 * hash function will likely also want to use HASH_COMPARE, and perhaps
 * also HASH_KEYCOPY, to control key comparison and copying.
 * Another often-used flag is HASH_CONTEXT, to allocate the hash table
 * under info->hcxt rather than under TopMemoryContext; the default
 * behavior is only suitable for session-lifespan hash tables.
 *
 * Fields in *info are read only when the associated flags bit is set.
 * It is not necessary to initialize other fields of *info.
 * Neither tabname nor *info need persist after the hash_create() call.
 *
 * Note: It is deprecated for callers of hash_create() to explicitly specify
 * string_hash, tag_hash, uint32_hash, or oid_hash.  Just set HASH_STRINGS or
 * HASH_BLOBS.  Use HASH_FUNCTION only when you want something other than
 * one of these.
 *
 * Note: for a shared-memory hashtable, nelem needs to be a pretty good
 * estimate, since we can't expand the table on the fly.  But an unshared
 * hashtable can be expanded on-the-fly, so it's better for nelem to be
 * on the small side and let the table grow if it's exceeded.  An overly
 * large nelem will penalize hash_seq_search speed without buying much.
 */
#[no_mangle]
pub unsafe fn hash_create(
    tabname: *const c_char,
    nelem: c_long,
    info: *const HASHCTL,
    flags: c_int,
) -> *mut HTAB {
    let hashp: *mut HTAB;
    let hctl: *mut HASHHDR;

    /*
     * Hash tables now allocate space for key and data, but you have to say
     * how much space to allocate.
     */
    Assert!(flags & HASH_ELEM != 0);
    Assert!((*info).keysize > 0);
    Assert!((*info).entrysize >= (*info).keysize);

    /*
     * For shared hash tables, we have a local hash header (HTAB struct) that
     * we allocate in TopMemoryContext; all else is in shared memory.
     *
     * For non-shared hash tables, everything including the hash header is in
     * a memory context created specially for the hash table --- this makes
     * hash_destroy very simple.  The memory context is made a child of either
     * a context specified by the caller, or TopMemoryContext if nothing is
     * specified.
     */
    if flags & HASH_SHARED_MEM != 0 {
        /* Set up to allocate the hash header */
        CurrentDynaHashCxt = TopMemoryContext;
    } else {
        /* Create the hash table's private memory context */
        if flags & HASH_CONTEXT != 0 {
            CurrentDynaHashCxt = (*info).hcxt;
        } else {
            CurrentDynaHashCxt = TopMemoryContext;
        }
        CurrentDynaHashCxt =
            AllocSetContextCreate!(CurrentDynaHashCxt, c"dynahash".as_ptr(), ALLOCSET_DEFAULT_SIZES);
    }

    /* Initialize the hash header, plus a copy of the table name */
    hashp = MemoryContextAlloc(
        CurrentDynaHashCxt,
        core::mem::size_of::<HTAB>() + strlen(tabname) + 1,
    ) as *mut HTAB;
    MemSet(hashp as *mut c_void, 0, core::mem::size_of::<HTAB>());

    (*hashp).tabname = hashp.add(1) as *mut c_char;
    strcpy((*hashp).tabname, tabname);

    /* If we have a private context, label it with hashtable's name */
    if flags & HASH_SHARED_MEM == 0 {
        MemoryContextSetIdentifier(CurrentDynaHashCxt, (*hashp).tabname);
    }

    /*
     * Select the appropriate hash function (see comments at head of file).
     */
    if flags & HASH_FUNCTION != 0 {
        Assert!(flags & (HASH_BLOBS | HASH_STRINGS) == 0);
        (*hashp).hash = (*info).hash;
    } else if flags & HASH_BLOBS != 0 {
        Assert!(flags & HASH_STRINGS == 0);
        /* We can optimize hashing for common key sizes */
        if (*info).keysize == core::mem::size_of::<uint32>() {
            (*hashp).hash = Some(uint32_hash_c);
        } else {
            (*hashp).hash = Some(tag_hash_c);
        }
    } else {
        /*
         * string_hash used to be considered the default hash method, and in a
         * non-assert build it effectively still is.  But we now consider it
         * an assertion error to not say HASH_STRINGS explicitly.  To help
         * catch mistaken usage of HASH_STRINGS, we also insist on a
         * reasonably long string length: if the keysize is only 4 or 8 bytes,
         * it's almost certainly an integer or pointer not a string.
         */
        Assert!(flags & HASH_STRINGS != 0);
        Assert!((*info).keysize > 8);

        (*hashp).hash = Some(string_hash_c);
    }

    /*
     * If you don't specify a match function, it defaults to string_compare if
     * you used string_hash, and to memcmp otherwise.
     *
     * Note: explicitly specifying string_hash is deprecated, because this
     * might not work for callers in loadable modules on some platforms due to
     * referencing a trampoline instead of the string_hash function proper.
     * Specify HASH_STRINGS instead.
     */
    if flags & HASH_COMPARE != 0 {
        (*hashp).r#match = (*info).r#match;
    } else if fn_eq((*hashp).hash, string_hash_c) {
        (*hashp).r#match = Some(string_compare);
    } else {
        (*hashp).r#match = Some(memcmp_c);
    }

    /*
     * Similarly, the key-copying function defaults to strlcpy or memcpy.
     */
    if flags & HASH_KEYCOPY != 0 {
        (*hashp).keycopy = (*info).keycopy;
    } else if fn_eq((*hashp).hash, string_hash_c) {
        /*
         * The signature of keycopy is meant for memcpy(), which returns
         * void*, but strlcpy() returns size_t.  Since we never use the return
         * value of keycopy, and size_t is pretty much always the same size as
         * void *, this should be safe.  The extra cast in the middle is to
         * avoid warnings from -Wcast-function-type.
         */
        (*hashp).keycopy = Some(strlcpy_c);
    } else {
        (*hashp).keycopy = Some(memcpy_c);
    }

    /* And select the entry allocation function, too. */
    if flags & HASH_ALLOC != 0 {
        (*hashp).alloc = (*info).alloc;
    } else {
        (*hashp).alloc = Some(DynaHashAlloc);
    }

    if flags & HASH_SHARED_MEM != 0 {
        /*
         * ctl structure and directory are preallocated for shared memory
         * tables.  Note that HASH_DIRSIZE and HASH_ALLOC had better be set as
         * well.
         */
        (*hashp).hctl = (*info).hctl;
        (*hashp).dir = ((*info).hctl as *mut c_char).add(core::mem::size_of::<HASHHDR>())
            as *mut HASHSEGMENT;
        (*hashp).hcxt = null_mut();
        (*hashp).isshared = true;

        /* hash table already exists, we're just attaching to it */
        if flags & HASH_ATTACH != 0 {
            /* make local copies of some heavily-used values */
            let hctl = (*hashp).hctl;
            (*hashp).keysize = (*hctl).keysize;
            (*hashp).ssize = (*hctl).ssize;
            (*hashp).sshift = (*hctl).sshift;

            return hashp;
        }
    } else {
        /* setup hash table defaults */
        (*hashp).hctl = null_mut();
        (*hashp).dir = null_mut();
        (*hashp).hcxt = CurrentDynaHashCxt;
        (*hashp).isshared = false;
    }

    if (*hashp).hctl.is_null() {
        (*hashp).hctl =
            ((*hashp).alloc.unwrap())(core::mem::size_of::<HASHHDR>()) as *mut HASHHDR;
        if (*hashp).hctl.is_null() {
            ereport!(ERROR, errmsg!("out of memory"));
        }
    }

    (*hashp).frozen = false;

    hdefault(hashp);

    hctl = (*hashp).hctl;

    if flags & HASH_PARTITION != 0 {
        /* Doesn't make sense to partition a local hash table */
        Assert!(flags & HASH_SHARED_MEM != 0);

        /*
         * The number of partitions had better be a power of 2. Also, it must
         * be less than INT_MAX (see init_htab()), so call the int version of
         * next_pow2.
         */
        Assert!((*info).num_partitions == next_pow2_int((*info).num_partitions) as c_long);

        (*hctl).num_partitions = (*info).num_partitions;
    }

    if flags & HASH_SEGMENT != 0 {
        (*hctl).ssize = (*info).ssize;
        (*hctl).sshift = my_log2((*info).ssize);
        /* ssize had better be a power of 2 */
        Assert!((*hctl).ssize == (1_i64 << (*hctl).sshift));
    }

    /*
     * SHM hash tables have fixed directory size passed by the caller.
     */
    if flags & HASH_DIRSIZE != 0 {
        (*hctl).max_dsize = (*info).max_dsize;
        (*hctl).dsize = (*info).dsize;
    }

    /* remember the entry sizes, too */
    (*hctl).keysize = (*info).keysize;
    (*hctl).entrysize = (*info).entrysize;

    /* make local copies of heavily-used constant fields */
    (*hashp).keysize = (*hctl).keysize;
    (*hashp).ssize = (*hctl).ssize;
    (*hashp).sshift = (*hctl).sshift;

    /* Build the hash directory structure */
    if !init_htab(hashp, nelem) {
        elog!(ERROR, "failed to initialize hash table \"{}\"", cstr((*hashp).tabname));
    }

    /*
     * For a shared hash table, preallocate the requested number of elements.
     * This reduces problems with run-time out-of-shared-memory conditions.
     *
     * For a non-shared hash table, preallocate the requested number of
     * elements if it's less than our chosen nelem_alloc.  This avoids wasting
     * space if the caller correctly estimates a small table size.
     */
    if (flags & HASH_SHARED_MEM != 0) || nelem < (*hctl).nelem_alloc as c_long {
        let mut i: c_int;
        let freelist_partitions: c_int;
        let nelem_alloc: c_long;
        let nelem_alloc_first: c_long;

        /*
         * If hash table is partitioned, give each freelist an equal share of
         * the initial allocation.  Otherwise only freeList[0] is used.
         */
        if IS_PARTITIONED((*hashp).hctl) {
            freelist_partitions = NUM_FREELISTS as c_int;
        } else {
            freelist_partitions = 1;
        }

        nelem_alloc = {
            let na = nelem / freelist_partitions as c_long;
            if na <= 0 {
                1
            } else {
                na
            }
        };

        /*
         * Make sure we'll allocate all the requested elements; freeList[0]
         * gets the excess if the request isn't divisible by NUM_FREELISTS.
         */
        if nelem_alloc * (freelist_partitions as c_long) < nelem {
            nelem_alloc_first = nelem - nelem_alloc * (freelist_partitions as c_long - 1);
        } else {
            nelem_alloc_first = nelem_alloc;
        }

        i = 0;
        while i < freelist_partitions {
            let temp = if i == 0 { nelem_alloc_first } else { nelem_alloc };

            if !element_alloc(hashp, temp as c_int, i) {
                ereport!(ERROR, errmsg!("out of memory"));
            }
            i += 1;
        }
    }

    if flags & HASH_FIXED_SIZE != 0 {
        (*hashp).isfixed = true;
    }
    hashp
}

/*
 * Set default HASHHDR parameters.
 */
unsafe fn hdefault(hashp: *mut HTAB) {
    let hctl: *mut HASHHDR = (*hashp).hctl;

    MemSet(hctl as *mut c_void, 0, core::mem::size_of::<HASHHDR>());

    (*hctl).dsize = DEF_DIRSIZE;
    (*hctl).nsegs = 0;

    (*hctl).num_partitions = 0; /* not partitioned */

    /* table has no fixed maximum size */
    (*hctl).max_dsize = NO_MAX_DSIZE;

    (*hctl).ssize = DEF_SEGSIZE;
    (*hctl).sshift = DEF_SEGSIZE_SHIFT;
}

/*
 * Given the user-specified entry size, choose nelem_alloc, ie, how many
 * elements to add to the hash table when we need more.
 */
fn choose_nelem_alloc(entrysize: Size) -> c_int {
    let mut nelem_alloc: c_int;
    let elementSize: Size;
    let mut allocSize: Size;

    /* Each element has a HASHELEMENT header plus user data. */
    /* NB: this had better match element_alloc() */
    elementSize = MAXALIGN(core::mem::size_of::<HASHELEMENT>()) + MAXALIGN(entrysize);

    /*
     * The idea here is to choose nelem_alloc at least 32, but round up so
     * that the allocation request will be a power of 2 or just less. This
     * makes little difference for hash tables in shared memory, but for hash
     * tables managed by palloc, the allocation request will be rounded up to
     * a power of 2 anyway.  If we fail to take this into account, we'll waste
     * as much as half the allocated space.
     */
    allocSize = 32 * 4; /* assume elementSize at least 8 */
    loop {
        allocSize <<= 1;
        nelem_alloc = (allocSize / elementSize) as c_int;
        if nelem_alloc >= 32 {
            break;
        }
    }

    nelem_alloc
}

/*
 * Compute derived fields of hctl and build the initial directory/segment
 * arrays
 */
unsafe fn init_htab(hashp: *mut HTAB, nelem: c_long) -> bool {
    let hctl: *mut HASHHDR = (*hashp).hctl;
    let mut segp: *mut HASHSEGMENT;
    let mut nbuckets: c_int;
    let mut nsegs: c_int;
    let mut i: c_int;

    /*
     * initialize mutexes if it's a partitioned table
     */
    if IS_PARTITIONED(hctl) {
        i = 0;
        while (i as usize) < NUM_FREELISTS {
            SpinLockInit(&mut (*hctl).freeList[i as usize].mutex);
            i += 1;
        }
    }

    /*
     * Allocate space for the next greater power of two number of buckets,
     * assuming a desired maximum load factor of 1.
     */
    nbuckets = next_pow2_int(nelem);

    /*
     * In a partitioned table, nbuckets must be at least equal to
     * num_partitions; were it less, keys with apparently different partition
     * numbers would map to the same bucket, breaking partition independence.
     * (Normally nbuckets will be much bigger; this is just a safety check.)
     */
    while (nbuckets as c_long) < (*hctl).num_partitions {
        nbuckets = nbuckets.wrapping_shl(1);
    }

    (*hctl).max_bucket = (nbuckets - 1) as uint32;
    (*hctl).low_mask = (nbuckets - 1) as uint32;
    /* (nbuckets << 1) - 1 done in uint32: C computes in int and assigns to
     * uint32; the wrapping shl reproduces that without a debug overflow panic. */
    (*hctl).high_mask = (nbuckets as uint32).wrapping_shl(1).wrapping_sub(1);

    /*
     * Figure number of directory segments needed, round up to a power of 2
     */
    nsegs = ((nbuckets - 1) as c_long / (*hctl).ssize + 1) as c_int;
    nsegs = next_pow2_int(nsegs as c_long);

    /*
     * Make sure directory is big enough. If pre-allocated directory is too
     * small, choke (caller screwed up).
     */
    if nsegs as c_long > (*hctl).dsize {
        if (*hashp).dir.is_null() {
            (*hctl).dsize = nsegs as c_long;
        } else {
            return false;
        }
    }

    /* Allocate a directory */
    if (*hashp).dir.is_null() {
        CurrentDynaHashCxt = (*hashp).hcxt;
        (*hashp).dir = ((*hashp).alloc.unwrap())(
            (*hctl).dsize as Size * core::mem::size_of::<HASHSEGMENT>(),
        ) as *mut HASHSEGMENT;
        if (*hashp).dir.is_null() {
            return false;
        }
    }

    /* Allocate initial segments */
    segp = (*hashp).dir;
    while (*hctl).nsegs < nsegs as c_long {
        *segp = seg_alloc(hashp);
        if (*segp).is_null() {
            return false;
        }
        (*hctl).nsegs += 1;
        segp = segp.add(1);
    }

    /* Choose number of entries to allocate at a time */
    (*hctl).nelem_alloc = choose_nelem_alloc((*hctl).entrysize);

    true
}

/*
 * Estimate the space needed for a hashtable containing the given number
 * of entries of given size.
 * NOTE: this is used to estimate the footprint of hashtables in shared
 * memory; therefore it does not count HTAB which is in local memory.
 * NB: assumes that all hash structure parameters have default values!
 */
pub unsafe fn hash_estimate_size(num_entries: c_long, entrysize: Size) -> Size {
    let mut size: Size;
    let nBuckets: c_long;
    let nSegments: c_long;
    let mut nDirEntries: c_long;
    let nElementAllocs: c_long;
    let elementSize: c_long;
    let elementAllocCnt: c_long;

    /* estimate number of buckets wanted */
    nBuckets = next_pow2_long(num_entries);
    /* # of segments needed for nBuckets */
    nSegments = next_pow2_long((nBuckets - 1) / DEF_SEGSIZE + 1);
    /* directory entries */
    nDirEntries = DEF_DIRSIZE;
    while nDirEntries < nSegments {
        nDirEntries <<= 1; /* dir_alloc doubles dsize at each call */
    }

    /* fixed control info */
    size = MAXALIGN(core::mem::size_of::<HASHHDR>()); /* but not HTAB, per above */
    /* directory */
    size = add_size(
        size,
        mul_size(nDirEntries as Size, core::mem::size_of::<HASHSEGMENT>()),
    );
    /* segments */
    size = add_size(
        size,
        mul_size(
            nSegments as Size,
            MAXALIGN(DEF_SEGSIZE as Size * core::mem::size_of::<HASHBUCKET>()),
        ),
    );
    /* elements --- allocated in groups of choose_nelem_alloc() entries */
    elementAllocCnt = choose_nelem_alloc(entrysize) as c_long;
    nElementAllocs = (num_entries - 1) / elementAllocCnt + 1;
    elementSize =
        (MAXALIGN(core::mem::size_of::<HASHELEMENT>()) + MAXALIGN(entrysize)) as c_long;
    size = add_size(
        size,
        mul_size(
            nElementAllocs as Size,
            mul_size(elementAllocCnt as Size, elementSize as Size),
        ),
    );

    size
}

/*
 * Select an appropriate directory size for a hashtable with the given
 * maximum number of entries.
 * This is only needed for hashtables in shared memory, whose directories
 * cannot be expanded dynamically.
 * NB: assumes that all hash structure parameters have default values!
 *
 * XXX this had better agree with the behavior of init_htab()...
 */
pub fn hash_select_dirsize(num_entries: c_long) -> c_long {
    let nBuckets: c_long;
    let nSegments: c_long;
    let mut nDirEntries: c_long;

    /* estimate number of buckets wanted */
    nBuckets = next_pow2_long(num_entries);
    /* # of segments needed for nBuckets */
    nSegments = next_pow2_long((nBuckets - 1) / DEF_SEGSIZE + 1);
    /* directory entries */
    nDirEntries = DEF_DIRSIZE;
    while nDirEntries < nSegments {
        nDirEntries <<= 1; /* dir_alloc doubles dsize at each call */
    }

    nDirEntries
}

/*
 * Compute the required initial memory allocation for a shared-memory
 * hashtable with the given parameters.  We need space for the HASHHDR
 * and for the (non expansible) directory.
 */
pub unsafe fn hash_get_shared_size(info: *const HASHCTL, flags: c_int) -> Size {
    Assert!(flags & HASH_DIRSIZE != 0);
    Assert!((*info).dsize == (*info).max_dsize);
    core::mem::size_of::<HASHHDR>() + (*info).dsize as Size * core::mem::size_of::<HASHSEGMENT>()
}

/********************** DESTROY ROUTINES ************************/

pub unsafe fn hash_destroy(hashp: *mut HTAB) {
    if !hashp.is_null() {
        /* allocation method must be one we know how to free, too */
        Assert!(fn_eq((*hashp).alloc, DynaHashAlloc));
        /* so this hashtable must have its own context */
        Assert!(!(*hashp).hcxt.is_null());

        hash_stats(c"destroy".as_ptr(), hashp);

        /*
         * Free everything by destroying the hash table's memory context.
         */
        MemoryContextDelete((*hashp).hcxt);
    }
}

pub unsafe fn hash_stats(_where: *const c_char, _hashp: *mut HTAB) {
    /* HASH_STATISTICS not compiled in; nothing to report. */
}

/*******************************SEARCH ROUTINES *****************************/

/*
 * get_hash_value -- exported routine to calculate a key's hash value
 *
 * We export this because for partitioned tables, callers need to compute
 * the partition number (from the low-order bits of the hash value) before
 * searching.
 */
pub unsafe fn get_hash_value(hashp: *mut HTAB, keyPtr: *const c_void) -> uint32 {
    ((*hashp).hash.unwrap())(keyPtr, (*hashp).keysize)
}

/* Convert a hash value to a bucket number */
#[inline]
unsafe fn calc_bucket(hctl: *mut HASHHDR, hash_val: uint32) -> uint32 {
    let mut bucket: uint32;

    bucket = hash_val & (*hctl).high_mask;
    if bucket > (*hctl).max_bucket {
        bucket = bucket & (*hctl).low_mask;
    }

    bucket
}

/*
 * hash_search -- look up key in table and perform action
 * hash_search_with_hash_value -- same, with key's hash value already computed
 *
 * action is one of:
 *		HASH_FIND: look up key in table
 *		HASH_ENTER: look up key in table, creating entry if not present
 *		HASH_ENTER_NULL: same, but return NULL if out of memory
 *		HASH_REMOVE: look up key in table, remove entry if present
 *
 * Return value is a pointer to the element found/entered/removed if any,
 * or NULL if no match was found.  (NB: in the case of the REMOVE action,
 * the result is a dangling pointer that shouldn't be dereferenced!)
 *
 * HASH_ENTER will normally ereport a generic "out of memory" error if
 * it is unable to create a new entry.  The HASH_ENTER_NULL operation is
 * the same except it will return NULL if out of memory.
 *
 * If foundPtr isn't NULL, then *foundPtr is set true if we found an
 * existing entry in the table, false otherwise.  This is needed in the
 * HASH_ENTER case, but is redundant with the return value otherwise.
 *
 * For hash_search_with_hash_value, the hashvalue parameter must have been
 * calculated with get_hash_value().
 */
#[no_mangle]
pub unsafe fn hash_search(
    hashp: *mut HTAB,
    keyPtr: *const c_void,
    action: HASHACTION,
    foundPtr: *mut bool,
) -> *mut c_void {
    hash_search_with_hash_value(
        hashp,
        keyPtr,
        ((*hashp).hash.unwrap())(keyPtr, (*hashp).keysize),
        action,
        foundPtr,
    )
}

pub unsafe fn hash_search_with_hash_value(
    hashp: *mut HTAB,
    keyPtr: *const c_void,
    hashvalue: uint32,
    action: HASHACTION,
    foundPtr: *mut bool,
) -> *mut c_void {
    let hctl: *mut HASHHDR = (*hashp).hctl;
    let freelist_idx: c_int = FREELIST_IDX(hctl, hashvalue);
    let keysize: Size;
    let mut currBucket: HASHBUCKET;
    let mut prevBucketPtr: *mut HASHBUCKET = null_mut();
    let r#match: HashCompareFunc;

    /*
     * If inserting, check if it is time to split a bucket.
     *
     * NOTE: failure to expand table is not a fatal error, it just means we
     * have to run at higher fill factor than we wanted.  However, if we're
     * using the palloc allocator then it will throw error anyway on
     * out-of-memory, so we must do this before modifying the table.
     */
    if action == HASH_ENTER || action == HASH_ENTER_NULL {
        /*
         * Can't split if running in partitioned mode, nor if frozen, nor if
         * table is the subject of any active hash_seq_search scans.
         */
        if (*hctl).freeList[0].nentries > (*hctl).max_bucket as c_long
            && !IS_PARTITIONED(hctl)
            && !(*hashp).frozen
            && !has_seq_scans(hashp)
        {
            let _ = expand_table(hashp);
        }
    }

    /*
     * Do the initial lookup
     */
    let _ = hash_initial_lookup(hashp, hashvalue, &mut prevBucketPtr);
    currBucket = *prevBucketPtr;

    /*
     * Follow collision chain looking for matching key
     */
    r#match = (*hashp).r#match; /* save one fetch in inner loop */
    keysize = (*hashp).keysize; /* ditto */

    while !currBucket.is_null() {
        if (*currBucket).hashvalue == hashvalue
            && (r#match.unwrap())(ELEMENTKEY(currBucket), keyPtr, keysize) == 0
        {
            break;
        }
        prevBucketPtr = &mut (*currBucket).link;
        currBucket = *prevBucketPtr;
    }

    if !foundPtr.is_null() {
        *foundPtr = !currBucket.is_null();
    }

    /*
     * OK, now what?
     */
    match action {
        HASH_FIND => {
            if !currBucket.is_null() {
                return ELEMENTKEY(currBucket);
            }
            return null_mut();
        }

        HASH_REMOVE => {
            if !currBucket.is_null() {
                /* if partitioned, must lock to touch nentries and freeList */
                if IS_PARTITIONED(hctl) {
                    SpinLockAcquire(&mut (*hctl).freeList[freelist_idx as usize].mutex);
                }

                /* delete the record from the appropriate nentries counter. */
                Assert!((*hctl).freeList[freelist_idx as usize].nentries > 0);
                (*hctl).freeList[freelist_idx as usize].nentries -= 1;

                /* remove record from hash bucket's chain. */
                *prevBucketPtr = (*currBucket).link;

                /* add the record to the appropriate freelist. */
                (*currBucket).link = (*hctl).freeList[freelist_idx as usize].freeList;
                (*hctl).freeList[freelist_idx as usize].freeList = currBucket;

                if IS_PARTITIONED(hctl) {
                    SpinLockRelease(&mut (*hctl).freeList[freelist_idx as usize].mutex);
                }

                /*
                 * better hope the caller is synchronizing access to this
                 * element, because someone else is going to reuse it the next
                 * time something is added to the table
                 */
                return ELEMENTKEY(currBucket);
            }
            return null_mut();
        }

        HASH_ENTER | HASH_ENTER_NULL => {
            /* Return existing element if found, else create one */
            if !currBucket.is_null() {
                return ELEMENTKEY(currBucket);
            }

            /* disallow inserts if frozen */
            if (*hashp).frozen {
                elog!(
                    ERROR,
                    "cannot insert into frozen hashtable \"{}\"",
                    cstr((*hashp).tabname)
                );
            }

            currBucket = get_hash_entry(hashp, freelist_idx);
            if currBucket.is_null() {
                /* out of memory */
                if action == HASH_ENTER_NULL {
                    return null_mut();
                }
                /* report a generic message */
                if (*hashp).isshared {
                    ereport!(ERROR, errmsg!("out of shared memory"));
                } else {
                    ereport!(ERROR, errmsg!("out of memory"));
                }
            }

            /* link into hashbucket chain */
            *prevBucketPtr = currBucket;
            (*currBucket).link = null_mut();

            /* copy key into record */
            (*currBucket).hashvalue = hashvalue;
            ((*hashp).keycopy.unwrap())(ELEMENTKEY(currBucket), keyPtr, keysize);

            /*
             * Caller is expected to fill the data field on return.  DO NOT
             * insert any code that could possibly throw error here, as doing
             * so would leave the table entry incomplete and hence corrupt the
             * caller's data structure.
             */

            return ELEMENTKEY(currBucket);
        }
    }

    // C falls out of the switch here only for an unrecognized action; the Rust
    // match above is exhaustive over HASHACTION, so this is unreachable but kept
    // to mirror the source's trailing error.
    #[allow(unreachable_code)]
    {
        elog!(ERROR, "unrecognized hash action code: {}", action as c_int);
        null_mut() /* keep compiler quiet */
    }
}

/*
 * hash_update_hash_key -- change the hash key of an existing table entry
 *
 * This is equivalent to removing the entry, making a new entry, and copying
 * over its data, except that the entry never goes to the table's freelist.
 * Therefore this cannot suffer an out-of-memory failure, even if there are
 * other processes operating in other partitions of the hashtable.
 *
 * Returns true if successful, false if the requested new hash key is already
 * present.  Throws error if the specified entry pointer isn't actually a
 * table member.
 *
 * NB: currently, there is no special case for old and new hash keys being
 * identical, which means we'll report false for that situation.  This is
 * preferable for existing uses.
 *
 * NB: for a partitioned hashtable, caller must hold lock on both relevant
 * partitions, if the new hash key would belong to a different partition.
 */
pub unsafe fn hash_update_hash_key(
    hashp: *mut HTAB,
    existingEntry: *mut c_void,
    newKeyPtr: *const c_void,
) -> bool {
    let existingElement: *mut HASHELEMENT = ELEMENT_FROM_KEY(existingEntry);
    let newhashvalue: uint32;
    let keysize: Size;
    let bucket: uint32;
    let newbucket: uint32;
    let mut currBucket: HASHBUCKET;
    let mut prevBucketPtr: *mut HASHBUCKET = null_mut();
    let oldPrevPtr: *mut HASHBUCKET;
    let r#match: HashCompareFunc;

    /* disallow updates if frozen */
    if (*hashp).frozen {
        elog!(
            ERROR,
            "cannot update in frozen hashtable \"{}\"",
            cstr((*hashp).tabname)
        );
    }

    /*
     * Lookup the existing element using its saved hash value.  We need to do
     * this to be able to unlink it from its hash chain, but as a side benefit
     * we can verify the validity of the passed existingEntry pointer.
     */
    bucket = hash_initial_lookup(hashp, (*existingElement).hashvalue, &mut prevBucketPtr);
    currBucket = *prevBucketPtr;

    while !currBucket.is_null() {
        if currBucket == existingElement {
            break;
        }
        prevBucketPtr = &mut (*currBucket).link;
        currBucket = *prevBucketPtr;
    }

    if currBucket.is_null() {
        elog!(
            ERROR,
            "hash_update_hash_key argument is not in hashtable \"{}\"",
            cstr((*hashp).tabname)
        );
    }

    oldPrevPtr = prevBucketPtr;

    /*
     * Now perform the equivalent of a HASH_ENTER operation to locate the hash
     * chain we want to put the entry into.
     */
    newhashvalue = ((*hashp).hash.unwrap())(newKeyPtr, (*hashp).keysize);
    newbucket = hash_initial_lookup(hashp, newhashvalue, &mut prevBucketPtr);
    currBucket = *prevBucketPtr;

    /*
     * Follow collision chain looking for matching key
     */
    r#match = (*hashp).r#match; /* save one fetch in inner loop */
    keysize = (*hashp).keysize; /* ditto */

    while !currBucket.is_null() {
        if (*currBucket).hashvalue == newhashvalue
            && (r#match.unwrap())(ELEMENTKEY(currBucket), newKeyPtr, keysize) == 0
        {
            break;
        }
        prevBucketPtr = &mut (*currBucket).link;
        currBucket = *prevBucketPtr;
    }

    if !currBucket.is_null() {
        return false; /* collision with an existing entry */
    }

    currBucket = existingElement;

    /*
     * If old and new hash values belong to the same bucket, we need not
     * change any chain links, and indeed should not since this simplistic
     * update will corrupt the list if currBucket is the last element.  (We
     * cannot fall out earlier, however, since we need to scan the bucket to
     * check for duplicate keys.)
     */
    if bucket != newbucket {
        /* OK to remove record from old hash bucket's chain. */
        *oldPrevPtr = (*currBucket).link;

        /* link into new hashbucket chain */
        *prevBucketPtr = currBucket;
        (*currBucket).link = null_mut();
    }

    /* copy new key into record */
    (*currBucket).hashvalue = newhashvalue;
    ((*hashp).keycopy.unwrap())(ELEMENTKEY(currBucket), newKeyPtr, keysize);

    /* rest of record is untouched */

    true
}

/*
 * Allocate a new hashtable entry if possible; return NULL if out of memory.
 * (Or, if the underlying space allocator throws error for out-of-memory,
 * we won't return at all.)
 */
unsafe fn get_hash_entry(hashp: *mut HTAB, freelist_idx: c_int) -> HASHBUCKET {
    let hctl: *mut HASHHDR = (*hashp).hctl;
    let mut newElement: HASHBUCKET;

    loop {
        /* if partitioned, must lock to touch nentries and freeList */
        if IS_PARTITIONED(hctl) {
            SpinLockAcquire(&mut (*hctl).freeList[freelist_idx as usize].mutex);
        }

        /* try to get an entry from the freelist */
        newElement = (*hctl).freeList[freelist_idx as usize].freeList;

        if !newElement.is_null() {
            break;
        }

        if IS_PARTITIONED(hctl) {
            SpinLockRelease(&mut (*hctl).freeList[freelist_idx as usize].mutex);
        }

        /*
         * No free elements in this freelist.  In a partitioned table, there
         * might be entries in other freelists, but to reduce contention we
         * prefer to first try to get another chunk of buckets from the main
         * shmem allocator.  If that fails, though, we *MUST* root through all
         * the other freelists before giving up.  There are multiple callers
         * that assume that they can allocate every element in the initially
         * requested table size, or that deleting an element guarantees they
         * can insert a new element, even if shared memory is entirely full.
         * Failing because the needed element is in a different freelist is
         * not acceptable.
         */
        if !element_alloc(hashp, (*hctl).nelem_alloc, freelist_idx) {
            let mut borrow_from_idx: c_int;

            if !IS_PARTITIONED(hctl) {
                return null_mut(); /* out of memory */
            }

            /* try to borrow element from another freelist */
            borrow_from_idx = freelist_idx;
            loop {
                borrow_from_idx = (borrow_from_idx + 1) % NUM_FREELISTS as c_int;
                if borrow_from_idx == freelist_idx {
                    break; /* examined all freelists, fail */
                }

                SpinLockAcquire(&mut (*hctl).freeList[borrow_from_idx as usize].mutex);
                newElement = (*hctl).freeList[borrow_from_idx as usize].freeList;

                if !newElement.is_null() {
                    (*hctl).freeList[borrow_from_idx as usize].freeList = (*newElement).link;
                    SpinLockRelease(&mut (*hctl).freeList[borrow_from_idx as usize].mutex);

                    /* careful: count the new element in its proper freelist */
                    SpinLockAcquire(&mut (*hctl).freeList[freelist_idx as usize].mutex);
                    (*hctl).freeList[freelist_idx as usize].nentries += 1;
                    SpinLockRelease(&mut (*hctl).freeList[freelist_idx as usize].mutex);

                    return newElement;
                }

                SpinLockRelease(&mut (*hctl).freeList[borrow_from_idx as usize].mutex);
            }

            /* no elements available to borrow either, so out of memory */
            return null_mut();
        }
    }

    /* remove entry from freelist, bump nentries */
    (*hctl).freeList[freelist_idx as usize].freeList = (*newElement).link;
    (*hctl).freeList[freelist_idx as usize].nentries += 1;

    if IS_PARTITIONED(hctl) {
        SpinLockRelease(&mut (*hctl).freeList[freelist_idx as usize].mutex);
    }

    newElement
}

/*
 * hash_get_num_entries -- get the number of entries in a hashtable
 */
pub unsafe fn hash_get_num_entries(hashp: *mut HTAB) -> c_long {
    let mut i: c_int;
    let mut sum: c_long = (*(*hashp).hctl).freeList[0].nentries;

    /*
     * We currently don't bother with acquiring the mutexes; it's only
     * sensible to call this function if you've got lock on all partitions of
     * the table.
     */
    if IS_PARTITIONED((*hashp).hctl) {
        i = 1;
        while (i as usize) < NUM_FREELISTS {
            sum += (*(*hashp).hctl).freeList[i as usize].nentries;
            i += 1;
        }
    }

    sum
}

/*
 * hash_seq_init/_search/_term
 *			Sequentially search through hash table and return
 *			all the elements one by one, return NULL when no more.
 *
 * hash_seq_term should be called if and only if the scan is abandoned before
 * completion; if hash_seq_search returns NULL then it has already done the
 * end-of-scan cleanup.
 *
 * NOTE: caller may delete the returned element before continuing the scan.
 * However, deleting any other element while the scan is in progress is
 * UNDEFINED (it might be the one that curIndex is pointing at!).  Also,
 * if elements are added to the table while the scan is in progress, it is
 * unspecified whether they will be visited by the scan or not.
 *
 * NOTE: it is possible to use hash_seq_init/hash_seq_search without any
 * worry about hash_seq_term cleanup, if the hashtable is first locked against
 * further insertions by calling hash_freeze.
 *
 * NOTE: to use this with a partitioned hashtable, caller had better hold
 * at least shared lock on all partitions of the table throughout the scan!
 * We can cope with insertions or deletions by our own backend, but *not*
 * with concurrent insertions or deletions by another.
 */
pub unsafe fn hash_seq_init(status: *mut HASH_SEQ_STATUS, hashp: *mut HTAB) {
    (*status).hashp = hashp;
    (*status).curBucket = 0;
    (*status).curEntry = null_mut();
    (*status).hasHashvalue = false;
    if !(*hashp).frozen {
        register_seq_scan(hashp);
    }
}

/*
 * Same as above but scan by the given hash value.
 * See also hash_seq_search().
 *
 * NOTE: the default hash function doesn't match syscache hash function.
 * Thus, if you're going to use this function in syscache callback, make sure
 * you're using custom hash function.  See relatt_cache_syshash()
 * for example.
 */
pub unsafe fn hash_seq_init_with_hash_value(
    status: *mut HASH_SEQ_STATUS,
    hashp: *mut HTAB,
    hashvalue: uint32,
) {
    let mut bucketPtr: *mut HASHBUCKET = null_mut();

    hash_seq_init(status, hashp);

    (*status).hasHashvalue = true;
    (*status).hashvalue = hashvalue;

    (*status).curBucket = hash_initial_lookup(hashp, hashvalue, &mut bucketPtr);
    (*status).curEntry = *bucketPtr;
}

pub unsafe fn hash_seq_search(status: *mut HASH_SEQ_STATUS) -> *mut c_void {
    let hashp: *mut HTAB;
    let hctl: *mut HASHHDR;
    let max_bucket: uint32;
    let ssize: c_long;
    let mut segment_num: c_long;
    let mut segment_ndx: c_long;
    let mut segp: HASHSEGMENT;
    let mut curBucket: uint32;
    let mut curElem: *mut HASHELEMENT;

    if (*status).hasHashvalue {
        /*
         * Scan entries only in the current bucket because only this bucket
         * can contain entries with the given hash value.
         */
        loop {
            curElem = (*status).curEntry;
            if curElem.is_null() {
                break;
            }
            (*status).curEntry = (*curElem).link;
            if (*status).hashvalue != (*curElem).hashvalue {
                continue;
            }
            return ELEMENTKEY(curElem);
        }

        hash_seq_term(status);
        return null_mut();
    }

    curElem = (*status).curEntry;
    if !curElem.is_null() {
        /* Continuing scan of curBucket... */
        (*status).curEntry = (*curElem).link;
        if (*status).curEntry.is_null() {
            /* end of this bucket */
            (*status).curBucket += 1;
        }
        return ELEMENTKEY(curElem);
    }

    /*
     * Search for next nonempty bucket starting at curBucket.
     */
    curBucket = (*status).curBucket;
    hashp = (*status).hashp;
    hctl = (*hashp).hctl;
    ssize = (*hashp).ssize;
    max_bucket = (*hctl).max_bucket;

    if curBucket > max_bucket {
        hash_seq_term(status);
        return null_mut(); /* search is done */
    }

    /*
     * first find the right segment in the table directory.
     */
    segment_num = (curBucket >> (*hashp).sshift) as c_long;
    segment_ndx = MOD(curBucket as c_long, ssize);

    segp = *(*hashp).dir.add(segment_num as usize);

    /*
     * Pick up the first item in this bucket's chain.  If chain is not empty
     * we can begin searching it.  Otherwise we have to advance to find the
     * next nonempty bucket.  We try to optimize that case since searching a
     * near-empty hashtable has to iterate this loop a lot.
     */
    loop {
        curElem = *segp.add(segment_ndx as usize);
        if !curElem.is_null() {
            break;
        }
        /* empty bucket, advance to next */
        curBucket += 1;
        if curBucket > max_bucket {
            (*status).curBucket = curBucket;
            hash_seq_term(status);
            return null_mut(); /* search is done */
        }
        segment_ndx += 1;
        if segment_ndx >= ssize {
            segment_num += 1;
            segment_ndx = 0;
            segp = *(*hashp).dir.add(segment_num as usize);
        }
    }

    /* Begin scan of curBucket... */
    (*status).curEntry = (*curElem).link;
    if (*status).curEntry.is_null() {
        /* end of this bucket */
        curBucket += 1;
    }
    (*status).curBucket = curBucket;
    ELEMENTKEY(curElem)
}

pub unsafe fn hash_seq_term(status: *mut HASH_SEQ_STATUS) {
    if !(*(*status).hashp).frozen {
        deregister_seq_scan((*status).hashp);
    }
}

/*
 * hash_freeze
 *			Freeze a hashtable against future insertions (deletions are
 *			still allowed)
 *
 * The reason for doing this is that by preventing any more bucket splits,
 * we no longer need to worry about registering hash_seq_search scans,
 * and thus caller need not be careful about ensuring hash_seq_term gets
 * called at the right times.
 *
 * Multiple calls to hash_freeze() are allowed, but you can't freeze a table
 * with active scans (since hash_seq_term would then do the wrong thing).
 */
pub unsafe fn hash_freeze(hashp: *mut HTAB) {
    if (*hashp).isshared {
        elog!(ERROR, "cannot freeze shared hashtable \"{}\"", cstr((*hashp).tabname));
    }
    if !(*hashp).frozen && has_seq_scans(hashp) {
        elog!(
            ERROR,
            "cannot freeze hashtable \"{}\" because it has active scans",
            cstr((*hashp).tabname)
        );
    }
    (*hashp).frozen = true;
}

/********************************* UTILITIES ************************/

/*
 * Expand the table by adding one more hash bucket.
 */
unsafe fn expand_table(hashp: *mut HTAB) -> bool {
    let hctl: *mut HASHHDR = (*hashp).hctl;
    let old_seg: HASHSEGMENT;
    let new_seg: HASHSEGMENT;
    let old_bucket: c_long;
    let new_bucket: c_long;
    let new_segnum: c_long;
    let new_segndx: c_long;
    let old_segnum: c_long;
    let old_segndx: c_long;
    let mut oldlink: *mut HASHBUCKET;
    let mut newlink: *mut HASHBUCKET;
    let mut currElement: HASHBUCKET;
    let mut nextElement: HASHBUCKET;

    Assert!(!IS_PARTITIONED(hctl));

    new_bucket = (*hctl).max_bucket as c_long + 1;
    new_segnum = new_bucket >> (*hashp).sshift;
    new_segndx = MOD(new_bucket, (*hashp).ssize);

    if new_segnum >= (*hctl).nsegs {
        /* Allocate new segment if necessary -- could fail if dir full */
        if new_segnum >= (*hctl).dsize {
            if !dir_realloc(hashp) {
                return false;
            }
        }
        let seg = seg_alloc(hashp);
        *(*hashp).dir.add(new_segnum as usize) = seg;
        if seg.is_null() {
            return false;
        }
        (*hctl).nsegs += 1;
    }

    /* OK, we created a new bucket */
    (*hctl).max_bucket += 1;

    /*
     * *Before* changing masks, find old bucket corresponding to same hash
     * values; values in that bucket may need to be relocated to new bucket.
     * Note that new_bucket is certainly larger than low_mask at this point,
     * so we can skip the first step of the regular hash mask calc.
     */
    old_bucket = new_bucket & (*hctl).low_mask as c_long;

    /*
     * If we crossed a power of 2, readjust masks.
     */
    if new_bucket as uint32 > (*hctl).high_mask {
        (*hctl).low_mask = (*hctl).high_mask;
        (*hctl).high_mask = new_bucket as uint32 | (*hctl).low_mask;
    }

    /*
     * Relocate records to the new bucket.  NOTE: because of the way the hash
     * masking is done in calc_bucket, only one old bucket can need to be
     * split at this point.  With a different way of reducing the hash value,
     * that might not be true!
     */
    old_segnum = old_bucket >> (*hashp).sshift;
    old_segndx = MOD(old_bucket, (*hashp).ssize);

    old_seg = *(*hashp).dir.add(old_segnum as usize);
    new_seg = *(*hashp).dir.add(new_segnum as usize);

    oldlink = old_seg.add(old_segndx as usize);
    newlink = new_seg.add(new_segndx as usize);

    currElement = *oldlink;
    while !currElement.is_null() {
        nextElement = (*currElement).link;
        if calc_bucket(hctl, (*currElement).hashvalue) as c_long == old_bucket {
            *oldlink = currElement;
            oldlink = &mut (*currElement).link;
        } else {
            *newlink = currElement;
            newlink = &mut (*currElement).link;
        }
        currElement = nextElement;
    }
    /* don't forget to terminate the rebuilt hash chains... */
    *oldlink = null_mut();
    *newlink = null_mut();

    true
}

unsafe fn dir_realloc(hashp: *mut HTAB) -> bool {
    let p: *mut HASHSEGMENT;
    let old_p: *mut HASHSEGMENT;
    let new_dsize: c_long;
    let old_dirsize: c_long;
    let new_dirsize: c_long;

    if (*(*hashp).hctl).max_dsize != NO_MAX_DSIZE {
        return false;
    }

    /* Reallocate directory */
    new_dsize = (*(*hashp).hctl).dsize << 1;
    old_dirsize = (*(*hashp).hctl).dsize * core::mem::size_of::<HASHSEGMENT>() as c_long;
    new_dirsize = new_dsize * core::mem::size_of::<HASHSEGMENT>() as c_long;

    old_p = (*hashp).dir;
    CurrentDynaHashCxt = (*hashp).hcxt;
    p = ((*hashp).alloc.unwrap())(new_dirsize as Size) as *mut HASHSEGMENT;

    if !p.is_null() {
        memcpy(p as *mut c_void, old_p as *const c_void, old_dirsize as Size);
        MemSet(
            (p as *mut c_char).add(old_dirsize as usize) as *mut c_void,
            0,
            (new_dirsize - old_dirsize) as Size,
        );
        (*hashp).dir = p;
        (*(*hashp).hctl).dsize = new_dsize;

        /* XXX assume the allocator is palloc, so we know how to free */
        Assert!(fn_eq((*hashp).alloc, DynaHashAlloc));
        pfree(old_p as *mut c_void);

        return true;
    }

    false
}

unsafe fn seg_alloc(hashp: *mut HTAB) -> HASHSEGMENT {
    let segp: HASHSEGMENT;

    CurrentDynaHashCxt = (*hashp).hcxt;
    segp = ((*hashp).alloc.unwrap())(
        core::mem::size_of::<HASHBUCKET>() * (*hashp).ssize as Size,
    ) as HASHSEGMENT;

    if segp.is_null() {
        return null_mut();
    }

    MemSet(
        segp as *mut c_void,
        0,
        core::mem::size_of::<HASHBUCKET>() * (*hashp).ssize as Size,
    );

    segp
}

/*
 * allocate some new elements and link them into the indicated free list
 */
unsafe fn element_alloc(hashp: *mut HTAB, nelem: c_int, freelist_idx: c_int) -> bool {
    let hctl: *mut HASHHDR = (*hashp).hctl;
    let elementSize: Size;
    let firstElement: *mut HASHELEMENT;
    let mut tmpElement: *mut HASHELEMENT;
    let mut prevElement: *mut HASHELEMENT;
    let mut i: c_int;

    if (*hashp).isfixed {
        return false;
    }

    /* Each element has a HASHELEMENT header plus user data. */
    elementSize = MAXALIGN(core::mem::size_of::<HASHELEMENT>()) + MAXALIGN((*hctl).entrysize);

    CurrentDynaHashCxt = (*hashp).hcxt;
    firstElement = ((*hashp).alloc.unwrap())(nelem as Size * elementSize) as *mut HASHELEMENT;

    if firstElement.is_null() {
        return false;
    }

    /* prepare to link all the new entries into the freelist */
    prevElement = null_mut();
    tmpElement = firstElement;
    i = 0;
    while i < nelem {
        (*tmpElement).link = prevElement;
        prevElement = tmpElement;
        tmpElement = (tmpElement as *mut c_char).add(elementSize) as *mut HASHELEMENT;
        i += 1;
    }

    /* if partitioned, must lock to touch freeList */
    if IS_PARTITIONED(hctl) {
        SpinLockAcquire(&mut (*hctl).freeList[freelist_idx as usize].mutex);
    }

    /* freelist could be nonempty if two backends did this concurrently */
    (*firstElement).link = (*hctl).freeList[freelist_idx as usize].freeList;
    (*hctl).freeList[freelist_idx as usize].freeList = prevElement;

    if IS_PARTITIONED(hctl) {
        SpinLockRelease(&mut (*hctl).freeList[freelist_idx as usize].mutex);
    }

    true
}

/*
 * Do initial lookup of a bucket for the given hash value, retrieving its
 * bucket number and its hash bucket.
 */
#[inline]
unsafe fn hash_initial_lookup(
    hashp: *mut HTAB,
    hashvalue: uint32,
    bucketptr: *mut *mut HASHBUCKET,
) -> uint32 {
    let hctl: *mut HASHHDR = (*hashp).hctl;
    let segp: HASHSEGMENT;
    let segment_num: c_long;
    let segment_ndx: c_long;
    let bucket: uint32;

    bucket = calc_bucket(hctl, hashvalue);

    segment_num = (bucket >> (*hashp).sshift) as c_long;
    segment_ndx = MOD(bucket as c_long, (*hashp).ssize);

    segp = *(*hashp).dir.add(segment_num as usize);

    if segp.is_null() {
        hash_corrupted(hashp);
    }

    *bucketptr = segp.add(segment_ndx as usize);
    bucket
}

/* complain when we have detected a corrupted hashtable */
fn hash_corrupted(hashp: *mut HTAB) -> ! {
    /*
     * If the corruption is in a shared hashtable, we'd better force a
     * systemwide restart.  Otherwise, just shut down this one backend.
     */
    unsafe {
        if (*hashp).isshared {
            elog!(PANIC, "hash table \"{}\" corrupted", cstr((*hashp).tabname));
        } else {
            elog!(FATAL, "hash table \"{}\" corrupted", cstr((*hashp).tabname));
        }
    }
    // elog!(ERROR/FATAL/PANIC, ...) panics (never returns) in this port.
    unreachable!()
}

/* calculate ceil(log base 2) of num */
#[no_mangle]
pub fn my_log2(num: c_long) -> c_int {
    let mut num = num;
    /*
     * guard against too-large input, which would be invalid for
     * pg_ceil_log2_*()
     */
    if num > c_long::MAX / 2 {
        num = c_long::MAX / 2;
    }

    /* SIZEOF_LONG == 8 on the LP64 platforms this port targets. */
    pg_ceil_log2_64(num as uint64) as c_int
}

/* calculate first power of 2 >= num, bounded to what will fit in a long */
fn next_pow2_long(num: c_long) -> c_long {
    /* my_log2's internal range check is sufficient */
    1_i64 << my_log2(num)
}

/* calculate first power of 2 >= num, bounded to what will fit in an int */
fn next_pow2_int(num: c_long) -> c_int {
    let mut num = num;
    if num > (c_int::MAX / 2) as c_long {
        num = (c_int::MAX / 2) as c_long;
    }
    1 << my_log2(num)
}

/************************* SEQ SCAN TRACKING ************************/

/*
 * We track active hash_seq_search scans here.  The need for this mechanism
 * comes from the fact that a scan will get confused if a bucket split occurs
 * while it's in progress: it might visit entries twice, or even miss some
 * entirely (if it's partway through the same bucket that splits).  Hence
 * we want to inhibit bucket splits if there are any active scans on the
 * table being inserted into.  This is a fairly rare case in current usage,
 * so just postponing the split until the next insertion seems sufficient.
 *
 * Given present usages of the function, only a few scans are likely to be
 * open concurrently; so a finite-size stack of open scans seems sufficient,
 * and we don't worry that linear search is too slow.  Note that we do
 * allow multiple scans of the same hashtable to be open concurrently.
 *
 * This mechanism can support concurrent scan and insertion in a shared
 * hashtable if it's the same backend doing both.  It would fail otherwise,
 * but locking reasons seem to preclude any such scenario anyway, so we don't
 * worry.
 *
 * This arrangement is reasonably robust if a transient hashtable is deleted
 * without notifying us.  The absolute worst case is we might inhibit splits
 * in another table created later at exactly the same address.  We will give
 * a warning at transaction end for reference leaks, so any bugs leading to
 * lack of notification should be easy to catch.
 */

const MAX_SEQ_SCANS: usize = 100;

/// tables being scanned
static mut seq_scan_tables: [*mut HTAB; MAX_SEQ_SCANS] = [null_mut(); MAX_SEQ_SCANS];
/// subtransaction nest level
static mut seq_scan_level: [c_int; MAX_SEQ_SCANS] = [0; MAX_SEQ_SCANS];
static mut num_seq_scans: c_int = 0;

/* Register a table as having an active hash_seq_search scan */
unsafe fn register_seq_scan(hashp: *mut HTAB) {
    if num_seq_scans >= MAX_SEQ_SCANS as c_int {
        elog!(
            ERROR,
            "too many active hash_seq_search scans, cannot start one on \"{}\"",
            cstr((*hashp).tabname)
        );
    }
    seq_scan_tables[num_seq_scans as usize] = hashp;
    seq_scan_level[num_seq_scans as usize] = GetCurrentTransactionNestLevel();
    num_seq_scans += 1;
}

/* Deregister an active scan */
unsafe fn deregister_seq_scan(hashp: *mut HTAB) {
    let mut i: c_int;

    /* Search backward since it's most likely at the stack top */
    i = num_seq_scans - 1;
    while i >= 0 {
        if seq_scan_tables[i as usize] == hashp {
            seq_scan_tables[i as usize] = seq_scan_tables[(num_seq_scans - 1) as usize];
            seq_scan_level[i as usize] = seq_scan_level[(num_seq_scans - 1) as usize];
            num_seq_scans -= 1;
            return;
        }
        i -= 1;
    }
    elog!(ERROR, "no hash_seq_search scan for hash table \"{}\"", cstr((*hashp).tabname));
}

/* Check if a table has any active scan */
unsafe fn has_seq_scans(hashp: *mut HTAB) -> bool {
    let mut i: c_int;

    i = 0;
    while i < num_seq_scans {
        if seq_scan_tables[i as usize] == hashp {
            return true;
        }
        i += 1;
    }
    false
}

/* Clean up any open scans at end of transaction */
pub unsafe fn AtEOXact_HashTables(isCommit: bool) {
    /*
     * During abort cleanup, open scans are expected; just silently clean 'em
     * out.  An open scan at commit means someone forgot a hash_seq_term()
     * call, so complain.
     *
     * Note: it's tempting to try to print the tabname here, but refrain for
     * fear of touching deallocated memory.  This isn't a user-facing message
     * anyway, so it needn't be pretty.
     */
    if isCommit {
        let mut i: c_int;

        i = 0;
        while i < num_seq_scans {
            elog!(
                WARNING,
                "leaked hash_seq_search scan for hash table {:p}",
                seq_scan_tables[i as usize]
            );
            i += 1;
        }
    }
    num_seq_scans = 0;
}

/* Clean up any open scans at end of subtransaction */
pub unsafe fn AtEOSubXact_HashTables(isCommit: bool, nestDepth: c_int) {
    let mut i: c_int;

    /*
     * Search backward to make cleanup easy.  Note we must check all entries,
     * not only those at the end of the array, because deletion technique
     * doesn't keep them in order.
     */
    i = num_seq_scans - 1;
    while i >= 0 {
        if seq_scan_level[i as usize] >= nestDepth {
            if isCommit {
                elog!(
                    WARNING,
                    "leaked hash_seq_search scan for hash table {:p}",
                    seq_scan_tables[i as usize]
                );
            }
            seq_scan_tables[i as usize] = seq_scan_tables[(num_seq_scans - 1) as usize];
            seq_scan_level[i as usize] = seq_scan_level[(num_seq_scans - 1) as usize];
            num_seq_scans -= 1;
        }
        i -= 1;
    }
}

/* ==================================================================== */
/*  Small libc-style helpers used above                                 */
/* ==================================================================== */

/// `strlen` over a NUL-terminated C string (the prelude does not re-export libc
/// strlen; mirrors the helper used elsewhere in the port).
///
/// # Safety
/// `s` must point to a valid NUL-terminated C string.
#[inline]
unsafe fn strlen(s: *const c_char) -> Size {
    let mut n: Size = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/// `strcpy(dst, src)`: copy a NUL-terminated C string including its terminator.
///
/// # Safety
/// `dst` must have room for `strlen(src) + 1` bytes; `src` must be NUL-terminated.
#[inline]
unsafe fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char {
    let mut i: Size = 0;
    loop {
        let ch = *src.add(i);
        *dst.add(i) = ch;
        if ch == 0 {
            break;
        }
        i += 1;
    }
    dst
}

/// Render a NUL-terminated C string for use in a Rust format string (error
/// messages translated from C `%s` of `hashp->tabname`).
///
/// # Safety
/// `s` must point to a valid NUL-terminated C string.
unsafe fn cstr<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "";
    }
    let len = strlen(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    core::str::from_utf8(bytes).unwrap_or("<non-utf8>")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[repr(C)]
    struct Entry {
        key: u32, // key MUST be the first field (dynahash assumes key at offset 0)
        value: i32,
    }

    #[test]
    fn create_insert_lookup_remove_iterate() {
        unsafe {
            let mut ctl: HASHCTL = core::mem::zeroed();
            ctl.keysize = core::mem::size_of::<u32>();
            ctl.entrysize = core::mem::size_of::<Entry>();
            let htab = hash_create(c"test".as_ptr(), 16, &ctl, HASH_ELEM | HASH_BLOBS);
            assert!(!htab.is_null());

            // Insert several entries (forces at least one bucket chain collision).
            let keys: [u32; 6] = [1, 100, 12345, 7, 65536, 42];
            for &k in keys.iter() {
                let mut found = true;
                let e = hash_search(
                    htab,
                    &k as *const u32 as *const c_void,
                    HASH_ENTER,
                    &mut found,
                ) as *mut Entry;
                assert!(!e.is_null());
                assert!(!found, "key {} unexpectedly already present", k);
                (*e).value = (k as i32).wrapping_mul(2);
            }
            assert_eq!(hash_get_num_entries(htab), keys.len() as c_long);

            // Look up an existing key.
            let k = 12345u32;
            let mut found = false;
            let e = hash_search(htab, &k as *const u32 as *const c_void, HASH_FIND, &mut found)
                as *mut Entry;
            assert!(found && !e.is_null());
            assert_eq!((*e).value, 24690);

            // Missing key.
            let k = 999u32;
            let mut found = true;
            let e = hash_search(htab, &k as *const u32 as *const c_void, HASH_FIND, &mut found);
            assert!(e.is_null() && !found);

            // Remove one.
            let k = 7u32;
            let mut found = false;
            hash_search(htab, &k as *const u32 as *const c_void, HASH_REMOVE, &mut found);
            assert!(found);
            assert_eq!(hash_get_num_entries(htab), (keys.len() - 1) as c_long);

            // Sequential scan visits the remaining entries exactly once.
            let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
            hash_seq_init(&mut status, htab);
            let mut seen = std::collections::HashSet::new();
            loop {
                let e = hash_seq_search(&mut status) as *mut Entry;
                if e.is_null() {
                    break;
                }
                seen.insert((*e).key);
            }
            assert_eq!(seen.len(), keys.len() - 1);
            assert!(!seen.contains(&7u32));
            assert!(seen.contains(&12345u32));
        }
    }
}

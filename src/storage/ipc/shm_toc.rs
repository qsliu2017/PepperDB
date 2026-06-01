//! Shared memory segment table of contents.
//!
//! Source: postgres/src/backend/storage/ipc/shm_toc.c
//! Merged header: postgres/src/include/storage/shm_toc.h
//!
//! A `shm_toc` is a simple key->offset directory laid out in a caller-provided
//! memory buffer. It divides one physical (shared) memory segment into logical
//! chunks: TOC entries grow forward from the front of the segment, while
//! `shm_toc_allocate()` chunks are bump-allocated backwards from the end. All
//! pointers stored are *relative* (byte offsets from the TOC start) so the same
//! segment can be mapped at different addresses in different backends.
//!
//! This translation does the real pointer arithmetic. Two C dependencies are
//! modeled rather than ported:
//!   - storage/spin.h spinlock (`slock_t` + S_LOCK/SpinLockAcquire/Release):
//!     NOT ported. The mutex field is kept as a placeholder and the
//!     insert/allocate paths run WITHOUT real locking (see TODO below). The
//!     offset arithmetic they perform is unchanged.
//!   - port/atomics.h read/write barriers: modeled as compiler fences, which is
//!     sufficient for the single-process / no-real-concurrency model here.

use crate::prelude::*;
use core::ffi::c_void;
use core::sync::atomic::{compiler_fence, Ordering};

use crate::c::{BUFFERALIGN, PG_UINT32_MAX};
use crate::pg_config::ALIGNOF_BUFFER;

// ---------------------------------------------------------------------------
// Local helpers for dependencies not yet ported.
// ---------------------------------------------------------------------------

/// `BUFFERALIGN_DOWN(LEN)` from c.h - round LEN down to a buffer boundary.
/// (`c::TYPEALIGN_DOWN` exists but there is no BUFFERALIGN_DOWN wrapper yet.)
#[inline(always)]
const fn BUFFERALIGN_DOWN(len: Size) -> Size {
    crate::c::TYPEALIGN_DOWN(ALIGNOF_BUFFER, len)
}

/// storage/shmem.h `add_size()` - overflow-checked addition. shmem.c is not
/// yet ported, so reproduce the overflow check locally.
/// TODO(pg-port): replace with the real shmem.c add_size() once ported.
#[inline]
fn add_size(s1: Size, s2: Size) -> Size {
    let result = s1.wrapping_add(s2);
    /* We are assuming Size is an unsigned type here... */
    if result < s1 || result < s2 {
        ereport!(ERROR, errmsg!("requested shared memory size overflows size_t"));
    }
    result
}

/// storage/shmem.h `mul_size()` - overflow-checked multiplication.
/// TODO(pg-port): replace with the real shmem.c mul_size() once ported.
#[inline]
fn mul_size(s1: Size, s2: Size) -> Size {
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

/// Placeholder for storage/spin.h `slock_t`. The real spinlock is not ported;
/// this is a do-nothing stand-in so the struct layout still carries the field.
/// TODO(pg-port): port storage/spin.h and use the real slock_t + S_LOCK here.
pub type slock_t = c_int;

// ---------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------

/// A single key->offset directory entry. `offset` is in bytes from the TOC
/// start (a relative pointer). Matches C `struct shm_toc_entry`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct shm_toc_entry {
    /// Arbitrary identifier.
    pub key: uint64,
    /// Offset, in bytes, from TOC start.
    pub offset: Size,
}

/// The table of contents header. In C this is an opaque type whose trailing
/// `toc_entry[FLEXIBLE_ARRAY_MEMBER]` overlays the caller's buffer; the same
/// holds here - callers must place a `shm_toc` at the start of a buffer that is
/// large enough for the header plus all entries plus the bump-allocated chunks.
#[repr(C)]
pub struct shm_toc {
    /// Magic number identifying this TOC.
    pub toc_magic: uint64,
    /// Spinlock for mutual exclusion (placeholder; not a real lock here).
    pub toc_mutex: slock_t,
    /// Bytes managed by this TOC.
    pub toc_total_bytes: Size,
    /// Bytes allocated of those managed.
    pub toc_allocated_bytes: Size,
    /// Number of entries in TOC.
    pub toc_nentry: uint32,
    /// Entry directory (FLEXIBLE_ARRAY_MEMBER). Indexed past the declared length
    /// via raw pointer arithmetic; the backing buffer supplies the real space.
    pub toc_entry: [shm_toc_entry; FLEXIBLE_ARRAY_MEMBER],
}

impl shm_toc {
    /// `offsetof(shm_toc, toc_entry)` - header size before the entry array.
    #[inline(always)]
    fn offset_of_toc_entry() -> Size {
        core::mem::offset_of!(shm_toc, toc_entry)
    }

    /// Raw pointer to `toc_entry[i]`, computed by hand because the declared
    /// array length is zero (FLEXIBLE_ARRAY_MEMBER).
    #[inline(always)]
    unsafe fn entry_ptr(toc: *mut shm_toc, i: Size) -> *mut shm_toc_entry {
        let base = (toc as *mut u8).add(Self::offset_of_toc_entry());
        (base as *mut shm_toc_entry).add(i)
    }
}

/// storage/spin.h `SpinLockInit` placeholder. Real spinlocks are not ported.
/// TODO(pg-port): initialize the real slock_t once spin.h is translated.
#[inline(always)]
unsafe fn SpinLockInit(_mutex: *mut slock_t) {
    /* no-op placeholder */
}

// ---------------------------------------------------------------------------
// API
// ---------------------------------------------------------------------------

/// Initialize a region of (shared) memory with a table of contents.
pub unsafe fn shm_toc_create(magic: uint64, address: *mut c_void, nbytes: Size) -> *mut shm_toc {
    let toc = address as *mut shm_toc;

    Assert!(nbytes > shm_toc::offset_of_toc_entry());
    (*toc).toc_magic = magic;
    SpinLockInit(&mut (*toc).toc_mutex);

    /*
     * The alignment code in shm_toc_allocate() assumes that the starting value
     * is buffer-aligned.
     */
    (*toc).toc_total_bytes = BUFFERALIGN_DOWN(nbytes);
    (*toc).toc_allocated_bytes = 0;
    (*toc).toc_nentry = 0;

    toc
}

/// Attach to an existing table of contents. If the magic number found at the
/// target address doesn't match our expectations, return NULL.
pub unsafe fn shm_toc_attach(magic: uint64, address: *mut c_void) -> *mut shm_toc {
    let toc = address as *mut shm_toc;

    if (*toc).toc_magic != magic {
        return null_mut();
    }

    Assert!((*toc).toc_total_bytes >= (*toc).toc_allocated_bytes);
    Assert!((*toc).toc_total_bytes > shm_toc::offset_of_toc_entry());

    toc
}

/// Allocate (shared) memory from a segment managed by a table of contents.
///
/// This is not a full-blown allocator; there's no way to free memory. It's just
/// a way of dividing a single physical segment into logical chunks. We allocate
/// backwards from the end of the segment, so that the TOC entries can grow
/// forward from the start of the segment.
pub unsafe fn shm_toc_allocate(toc: *mut shm_toc, mut nbytes: Size) -> *mut c_void {
    /*
     * Make sure request is well-aligned. XXX: MAXALIGN is not enough, because
     * atomic ops might need a wider alignment. BUFFERALIGN ought to be enough.
     */
    nbytes = BUFFERALIGN(nbytes);

    // TODO(pg-port): SpinLockAcquire(&toc->toc_mutex) - spinlock not ported.

    let total_bytes = (*toc).toc_total_bytes;
    let allocated_bytes = (*toc).toc_allocated_bytes;
    let nentry = (*toc).toc_nentry as Size;
    let toc_bytes = shm_toc::offset_of_toc_entry()
        + nentry * core::mem::size_of::<shm_toc_entry>()
        + allocated_bytes;

    /* Check for memory exhaustion and overflow. */
    if toc_bytes + nbytes > total_bytes || toc_bytes + nbytes < toc_bytes {
        // TODO(pg-port): SpinLockRelease(&toc->toc_mutex) - spinlock not ported.
        ereport!(ERROR, errmsg!("out of shared memory"));
    }
    (*toc).toc_allocated_bytes += nbytes;

    // TODO(pg-port): SpinLockRelease(&toc->toc_mutex) - spinlock not ported.

    (toc as *mut u8).add(total_bytes - allocated_bytes - nbytes) as *mut c_void
}

/// Return the number of bytes that can still be allocated.
pub unsafe fn shm_toc_freespace(toc: *mut shm_toc) -> Size {
    // TODO(pg-port): SpinLockAcquire(&toc->toc_mutex) - spinlock not ported.
    let total_bytes = (*toc).toc_total_bytes;
    let allocated_bytes = (*toc).toc_allocated_bytes;
    let nentry = (*toc).toc_nentry as Size;
    // TODO(pg-port): SpinLockRelease(&toc->toc_mutex) - spinlock not ported.

    let toc_bytes =
        shm_toc::offset_of_toc_entry() + nentry * core::mem::size_of::<shm_toc_entry>();
    Assert!(allocated_bytes + BUFFERALIGN(toc_bytes) <= total_bytes);
    total_bytes - (allocated_bytes + BUFFERALIGN(toc_bytes))
}

/// Insert a TOC entry.
///
/// Registers `address` (a pointer within the segment) under `key`. Other
/// backends pass the same key to `shm_toc_lookup()` to recover the address.
/// Pointers are stored relative to the TOC start because the segment may be
/// mapped at different addresses in different backends.
pub unsafe fn shm_toc_insert(toc: *mut shm_toc, key: uint64, address: *mut c_void) {
    /* Relativize pointer. */
    Assert!((address as *const c_void) > (toc as *const c_void));
    let offset = (address as *mut u8).offset_from(toc as *mut u8) as Size;

    // TODO(pg-port): SpinLockAcquire(&toc->toc_mutex) - spinlock not ported.

    let total_bytes = (*toc).toc_total_bytes;
    let allocated_bytes = (*toc).toc_allocated_bytes;
    let nentry = (*toc).toc_nentry as Size;
    let toc_bytes = shm_toc::offset_of_toc_entry()
        + nentry * core::mem::size_of::<shm_toc_entry>()
        + allocated_bytes;

    /* Check for memory exhaustion and overflow. */
    if toc_bytes + core::mem::size_of::<shm_toc_entry>() > total_bytes
        || toc_bytes + core::mem::size_of::<shm_toc_entry>() < toc_bytes
        || nentry >= PG_UINT32_MAX as Size
    {
        // TODO(pg-port): SpinLockRelease(&toc->toc_mutex) - spinlock not ported.
        ereport!(ERROR, errmsg!("out of shared memory"));
    }

    Assert!(offset < total_bytes);
    let entry = shm_toc::entry_ptr(toc, nentry);
    (*entry).key = key;
    (*entry).offset = offset;

    /*
     * By placing a write barrier after filling in the entry and before updating
     * the number of entries, we make it safe to read the TOC unlocked.
     */
    // pg_write_barrier(): modeled as a compiler fence (atomics.h not ported).
    compiler_fence(Ordering::Release);

    (*toc).toc_nentry += 1;

    // TODO(pg-port): SpinLockRelease(&toc->toc_mutex) - spinlock not ported.
}

/// Look up a TOC entry.
///
/// If the key is not found, returns NULL if `no_error` is true, otherwise
/// throws `elog(ERROR)`. Unlike the other functions, this acquires no lock; it
/// uses only barriers.
pub unsafe fn shm_toc_lookup(toc: *mut shm_toc, key: uint64, no_error: bool) -> *mut c_void {
    /*
     * Read the number of entries before we examine any entry. We assume that
     * reading a uint32 is atomic.
     */
    let nentry = (*toc).toc_nentry;
    // pg_read_barrier(): modeled as a compiler fence (atomics.h not ported).
    compiler_fence(Ordering::Acquire);

    /* Now search for a matching entry. */
    let mut i: uint32 = 0;
    while i < nentry {
        let entry = shm_toc::entry_ptr(toc, i as Size);
        if (*entry).key == key {
            return (toc as *mut u8).add((*entry).offset) as *mut c_void;
        }
        i += 1;
    }

    /* No matching entry was found. */
    if !no_error {
        elog!(
            ERROR,
            "could not find key {} in shm TOC at {:p}",
            key,
            toc
        );
    }
    null_mut()
}

// ---------------------------------------------------------------------------
// Estimator (shm_toc.h macros + shm_toc_estimate())
// ---------------------------------------------------------------------------

/// Tools for estimating how large a chunk of (shared) memory will be needed to
/// store a TOC and its dependent objects. `number_of_keys` is a `Size` for
/// convenience even though large key counts are not really supported.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct shm_toc_estimator {
    pub space_for_chunks: Size,
    pub number_of_keys: Size,
}

/// `shm_toc_initialize_estimator(e)` macro.
#[inline(always)]
pub fn shm_toc_initialize_estimator(e: &mut shm_toc_estimator) {
    e.space_for_chunks = 0;
    e.number_of_keys = 0;
}

/// `shm_toc_estimate_chunk(e, sz)` macro - reserve `BUFFERALIGN(sz)` of chunk
/// space.
#[inline(always)]
pub fn shm_toc_estimate_chunk(e: &mut shm_toc_estimator, sz: Size) {
    e.space_for_chunks = add_size(e.space_for_chunks, BUFFERALIGN(sz));
}

/// `shm_toc_estimate_keys(e, cnt)` macro - reserve space for `cnt` more keys.
#[inline(always)]
pub fn shm_toc_estimate_keys(e: &mut shm_toc_estimator, cnt: Size) {
    e.number_of_keys = add_size(e.number_of_keys, cnt);
}

/// Estimate how much (shared) memory will be required to store a TOC and its
/// dependent data structures.
pub fn shm_toc_estimate(e: &shm_toc_estimator) -> Size {
    let mut sz: Size = shm_toc::offset_of_toc_entry();
    sz = add_size(
        sz,
        mul_size(e.number_of_keys, core::mem::size_of::<shm_toc_entry>()),
    );
    sz = add_size(sz, e.space_for_chunks);

    BUFFERALIGN(sz)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MAGIC: uint64 = 0x1234_5678_9abc_def0;

    #[test]
    fn create_allocate_insert_lookup() {
        unsafe {
            // A generously-sized, buffer-aligned-capacity backing buffer.
            let nbytes: Size = 8192;
            let mut buf = vec![0u8; nbytes];
            let address = buf.as_mut_ptr() as *mut c_void;

            let toc = shm_toc_create(TEST_MAGIC, address, nbytes);
            assert!(!toc.is_null());
            assert_eq!((*toc).toc_magic, TEST_MAGIC);
            assert_eq!((*toc).toc_nentry, 0);
            assert_eq!((*toc).toc_allocated_bytes, 0);
            assert_eq!((*toc).toc_total_bytes, BUFFERALIGN_DOWN(nbytes));

            // Attaching with the right magic returns the same address; wrong
            // magic returns NULL.
            assert_eq!(shm_toc_attach(TEST_MAGIC, address), toc);
            assert!(shm_toc_attach(TEST_MAGIC ^ 1, address).is_null());

            let free0 = shm_toc_freespace(toc);

            // Allocate two chunks from the end of the segment.
            let chunk_a = shm_toc_allocate(toc, 100);
            let chunk_b = shm_toc_allocate(toc, 200);
            assert!(!chunk_a.is_null());
            assert!(!chunk_b.is_null());

            // Both chunks must lie inside the buffer and not overlap each other.
            let base = toc as *mut u8;
            let off_a = (chunk_a as *mut u8).offset_from(base) as Size;
            let off_b = (chunk_b as *mut u8).offset_from(base) as Size;
            assert!(off_a < nbytes && off_b < nbytes);
            // Allocation grows backwards: the second chunk sits before the first.
            assert!(off_b < off_a);
            // BUFFERALIGN(100)=128, BUFFERALIGN(200)=224 -> 352 allocated.
            assert_eq!((*toc).toc_allocated_bytes, BUFFERALIGN(100) + BUFFERALIGN(200));

            // Freespace must have decreased after allocation.
            let free1 = shm_toc_freespace(toc);
            assert!(free1 < free0);
            assert_eq!(free0 - free1, BUFFERALIGN(100) + BUFFERALIGN(200));

            // Insert two keys pointing at the two chunks.
            shm_toc_insert(toc, 42, chunk_a);
            shm_toc_insert(toc, 99, chunk_b);
            assert_eq!((*toc).toc_nentry, 2);

            // Lookup returns the exact chunk addresses we inserted.
            assert_eq!(shm_toc_lookup(toc, 42, false), chunk_a);
            assert_eq!(shm_toc_lookup(toc, 99, false), chunk_b);

            // Missing key with no_error=true yields NULL.
            assert!(shm_toc_lookup(toc, 7, true).is_null());

            // Inserting entries also consumes freespace (one entry each).
            let free2 = shm_toc_freespace(toc);
            assert!(free2 < free1);
        }
    }

    #[test]
    fn estimator_matches_layout() {
        let mut e = shm_toc_estimator::default();
        shm_toc_initialize_estimator(&mut e);
        assert_eq!(e.space_for_chunks, 0);
        assert_eq!(e.number_of_keys, 0);

        shm_toc_estimate_chunk(&mut e, 100); // +BUFFERALIGN(100)
        shm_toc_estimate_chunk(&mut e, 200); // +BUFFERALIGN(200)
        shm_toc_estimate_keys(&mut e, 3);

        assert_eq!(e.space_for_chunks, BUFFERALIGN(100) + BUFFERALIGN(200));
        assert_eq!(e.number_of_keys, 3);

        let header = shm_toc::offset_of_toc_entry();
        let expected = BUFFERALIGN(
            header + 3 * core::mem::size_of::<shm_toc_entry>() + BUFFERALIGN(100) + BUFFERALIGN(200),
        );
        assert_eq!(shm_toc_estimate(&e), expected);
    }
}

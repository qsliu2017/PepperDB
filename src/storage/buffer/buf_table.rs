//! src/backend/storage/buffer/buf_table.c
//!   routines for mapping BufferTags to buffer indexes (the shared buffer
//!   lookup hash table mapping a disk page's BufferTag -> buffer id).
//!
//! Merged #includes:
//!   - storage/buf_internals.h : the BufferTag struct (RelFileLocator-flattened
//!     spcOid/dbOid/relNumber + forkNum + blockNum), InitBufferTag /
//!     BufferTagsEqual helpers, and the BufferLookupEnt hashtable entry that
//!     buf_table.c defines locally.
//!   - storage/lwlock.h : NUM_BUFFER_PARTITIONS (128) used by InitBufTable.
//!
//! STUBs:
//!   - ShmemInitHash (storage/shmem.h): the real shared-memory hash-table
//!     constructor.  Not yet ported; we substitute a plain backend-local
//!     dynahash hash_create.  See ShmemInitHash() below.
//!   - P_NEW (storage/bufmgr.h): the "extend relation" sentinel block number,
//!     defined locally as InvalidBlockNumber per bufmgr.h.
//!
//! Note: the routines here do no locking of their own; in real PostgreSQL the
//! caller holds the appropriate BufMappingLock partition lock.  This single-
//! process port omits that locking.

use crate::prelude::*;

// Canonical storage/catalog types from their already-ported home modules.
use crate::common::blkreftable::RelFileLocator; // storage/relfilelocator.h shape
use crate::common::relpath::{ForkNumber, InvalidForkNumber}; // common/relpath.h
use crate::storage::block::{BlockNumber, InvalidBlockNumber}; // storage/block.h

// RelFileNumber is `Oid` in PostgreSQL (common/relpath.h).  We re-derive the
// alias locally rather than `use`-ing one of the several existing definitions.
pub type RelFileNumber = Oid;
/// common/relpath.h: InvalidRelFileNumber is InvalidOid (== 0).
pub const InvalidRelFileNumber: RelFileNumber = InvalidOid;

// dynahash backend (utils/hash/dynahash.rs) -- the local stand-in for the
// shared-memory hash table.
use crate::utils::hash::dynahash::{
    get_hash_value, hash_create, hash_estimate_size, hash_search_with_hash_value, HASHCTL, HTAB,
    HASH_BLOBS, HASH_ELEM, HASH_ENTER, HASH_FIND, HASH_PARTITION, HASH_REMOVE,
};

/// storage/lwlock.h: NUM_BUFFER_PARTITIONS -- number of partitions of the
/// shared buffer mapping table (must be a power of 2).
pub const NUM_BUFFER_PARTITIONS: c_long = 128;

/// storage/bufmgr.h: P_NEW is the magic block number passed to ReadBuffer to
/// request relation extension.  It is defined as InvalidBlockNumber.
pub const P_NEW: BlockNumber = InvalidBlockNumber;

/*
 * Buffer tag identifies which disk block the buffer contains.  This is the
 * RelFileLocator (spcOid/dbOid/relNumber) flattened together with the fork
 * number and block number.  It is used directly as a hash key, so it must have
 * no pad bytes (all fields are 4 bytes here) -- InitBufferTag zeroes the whole
 * struct's worth of bytes via ClearBufferTag for the same reason.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BufferTag {
    /// tablespace oid
    pub spcOid: Oid,
    /// database oid
    pub dbOid: Oid,
    /// relation file number
    pub relNumber: RelFileNumber,
    /// fork number
    pub forkNum: ForkNumber,
    /// blknum relative to begin of reln
    pub blockNum: BlockNumber,
}

/* entry for buffer lookup hashtable (defined locally in buf_table.c) */
#[repr(C)]
pub struct BufferLookupEnt {
    /// Tag of a disk page (must be first: it is the hash key)
    pub key: BufferTag,
    /// Associated buffer ID
    pub id: c_int,
}

/*
 * BufferTag accessors / mutators (buf_internals.h, inline).
 */

#[inline]
pub fn BufTagGetRelNumber(tag: &BufferTag) -> RelFileNumber {
    tag.relNumber
}

#[inline]
pub fn BufTagGetForkNum(tag: &BufferTag) -> ForkNumber {
    tag.forkNum
}

#[inline]
pub fn BufTagSetRelForkDetails(tag: &mut BufferTag, relnumber: RelFileNumber, forknum: ForkNumber) {
    tag.relNumber = relnumber;
    tag.forkNum = forknum;
}

#[inline]
pub fn BufTagGetRelFileLocator(tag: &BufferTag) -> RelFileLocator {
    RelFileLocator {
        spcOid: tag.spcOid,
        dbOid: tag.dbOid,
        relNumber: BufTagGetRelNumber(tag),
    }
}

/// buf_internals.h: ClearBufferTag -- reset every field to its Invalid value.
#[inline]
pub fn ClearBufferTag(tag: &mut BufferTag) {
    tag.spcOid = InvalidOid;
    tag.dbOid = InvalidOid;
    BufTagSetRelForkDetails(tag, InvalidRelFileNumber, InvalidForkNumber);
    tag.blockNum = InvalidBlockNumber;
}

/// buf_internals.h: InitBufferTag (the INIT_BUFFERTAG helper) -- populate a tag
/// from a RelFileLocator + fork + block.
#[inline]
pub fn InitBufferTag(
    tag: &mut BufferTag,
    rlocator: &RelFileLocator,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
) {
    tag.spcOid = rlocator.spcOid;
    tag.dbOid = rlocator.dbOid;
    BufTagSetRelForkDetails(tag, rlocator.relNumber, forkNum);
    tag.blockNum = blockNum;
}

/// buf_internals.h: BufferTagsEqual (the BUFFERTAGS_EQUAL helper).
#[inline]
pub fn BufferTagsEqual(tag1: &BufferTag, tag2: &BufferTag) -> bool {
    tag1.spcOid == tag2.spcOid
        && tag1.dbOid == tag2.dbOid
        && tag1.relNumber == tag2.relNumber
        && tag1.blockNum == tag2.blockNum
        && tag1.forkNum == tag2.forkNum
}

/// buf_internals.h: BufTagMatchesRelFileLocator.
#[inline]
pub fn BufTagMatchesRelFileLocator(tag: &BufferTag, rlocator: &RelFileLocator) -> bool {
    tag.spcOid == rlocator.spcOid
        && tag.dbOid == rlocator.dbOid
        && BufTagGetRelNumber(tag) == rlocator.relNumber
}

/* ==================================================================== */
/*  The shared buffer lookup hash table                                 */
/* ==================================================================== */

/// buf_table.c: `static HTAB *SharedBufHash;`
///
/// In PostgreSQL this points into shared memory.  Here it is a backend-local
/// dynahash table installed by InitBufTable.
pub static mut SharedBufHash: *mut HTAB = null_mut();

// TODO(pg-port): storage/shmem.h -- ShmemInitHash creates (or attaches to) a
// hash table living in the shared-memory segment, sized between init_size and
// max_size, and registers it under `name` in the shmem index.  Shared memory is
// not yet ported, so this stand-in just calls dynahash::hash_create to build an
// ordinary backend-local table of `max_size` initial buckets.  When shmem.c is
// translated this must become the real shared (cross-process) constructor.
#[allow(non_snake_case)]
unsafe fn ShmemInitHash(
    name: *const c_char,
    init_size: c_long,
    max_size: c_long,
    infop: *const HASHCTL,
    hash_flags: c_int,
) -> *mut HTAB {
    crate::storage::ipc::shmem::ShmemInitHash(name, init_size, max_size, infop as *mut HASHCTL, hash_flags)
}

/*
 * Estimate space needed for mapping hashtable
 *		size is the desired hash table size (possibly more than NBuffers)
 */
pub unsafe fn BufTableShmemSize(size: c_int) -> Size {
    hash_estimate_size(size as c_long, std::mem::size_of::<BufferLookupEnt>())
}

/*
 * Initialize shmem hash table for mapping buffers
 *		size is the desired hash table size (possibly more than NBuffers)
 */
pub unsafe fn InitBufTable(size: c_int) {
    // assume no locking is needed yet

    // BufferTag maps to Buffer
    let mut info: HASHCTL = std::mem::zeroed();
    info.keysize = std::mem::size_of::<BufferTag>();
    info.entrysize = std::mem::size_of::<BufferLookupEnt>();
    info.num_partitions = NUM_BUFFER_PARTITIONS;

    SharedBufHash = ShmemInitHash(
        c"Shared Buffer Lookup Table".as_ptr(),
        size as c_long,
        size as c_long,
        &info,
        HASH_ELEM | HASH_BLOBS | HASH_PARTITION,
    );
}

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the tag.
 * Callers need the hash code to choose which buffer partition to lock and to
 * avoid recomputing the (slow) hash twice.
 */
pub unsafe fn BufTableHashCode(tagPtr: *mut BufferTag) -> uint32 {
    get_hash_value(SharedBufHash, tagPtr as *const c_void)
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition.
 */
pub unsafe fn BufTableLookup(tagPtr: *mut BufferTag, hashcode: uint32) -> c_int {
    let result = hash_search_with_hash_value(
        SharedBufHash,
        tagPtr as *const c_void,
        hashcode,
        HASH_FIND,
        null_mut(),
    ) as *mut BufferLookupEnt;

    if result.is_null() {
        return -1;
    }

    (*result).id
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists already,
 * returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition.
 */
pub unsafe fn BufTableInsert(tagPtr: *mut BufferTag, hashcode: uint32, buf_id: c_int) -> c_int {
    Assert!(buf_id >= 0); // -1 is reserved for not-in-table
    Assert!((*tagPtr).blockNum != P_NEW); // invalid tag

    let mut found: bool = false;
    let result = hash_search_with_hash_value(
        SharedBufHash,
        tagPtr as *const c_void,
        hashcode,
        HASH_ENTER,
        &mut found,
    ) as *mut BufferLookupEnt;

    if found {
        // found something already in the table
        return (*result).id;
    }

    (*result).id = buf_id;

    -1
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition.
 */
pub unsafe fn BufTableDelete(tagPtr: *mut BufferTag, hashcode: uint32) {
    let result = hash_search_with_hash_value(
        SharedBufHash,
        tagPtr as *const c_void,
        hashcode,
        HASH_REMOVE,
        null_mut(),
    ) as *mut BufferLookupEnt;

    if result.is_null() {
        // shouldn't happen
        elog!(ERROR, "shared buffer hash table corrupted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // SharedBufHash is a process-global `static mut`; serialize the tests that
    // build and mutate it.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn make_tag(rel: RelFileNumber, blk: BlockNumber) -> BufferTag {
        let loc = RelFileLocator {
            spcOid: 1663,
            dbOid: 5,
            relNumber: rel,
        };
        let mut tag: BufferTag = unsafe { std::mem::zeroed() };
        InitBufferTag(&mut tag, &loc, MAIN_FORKNUM, blk);
        tag
    }

    // MAIN_FORKNUM (== 0) from common/relpath.h.
    const MAIN_FORKNUM: ForkNumber = 0;

    #[test]
    fn init_insert_lookup_delete_roundtrip() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            InitBufTable(64);
            assert!(!SharedBufHash.is_null());

            let mut tag = make_tag(42, 7);
            let code = BufTableHashCode(&mut tag);

            // Not present yet.
            assert_eq!(BufTableLookup(&mut tag, code), -1);

            // Fresh insert returns -1, then the entry is found with its id.
            assert_eq!(BufTableInsert(&mut tag, code, 99), -1);
            assert_eq!(BufTableLookup(&mut tag, code), 99);

            // Re-inserting the same tag (with a different id) reports the
            // existing buffer id and does not overwrite it.
            assert_eq!(BufTableInsert(&mut tag, code, 123), 99);
            assert_eq!(BufTableLookup(&mut tag, code), 99);

            // Delete removes it; subsequent lookup misses.
            BufTableDelete(&mut tag, code);
            assert_eq!(BufTableLookup(&mut tag, code), -1);

            SharedBufHash = null_mut();
        }
    }

    #[test]
    fn distinct_tags_are_independent() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            InitBufTable(64);

            let mut a = make_tag(10, 0);
            let mut b = make_tag(10, 1); // same rel, different block
            let mut c = make_tag(11, 0); // different rel
            let ca = BufTableHashCode(&mut a);
            let cb = BufTableHashCode(&mut b);
            let cc = BufTableHashCode(&mut c);

            assert_eq!(BufTableInsert(&mut a, ca, 1), -1);
            assert_eq!(BufTableInsert(&mut b, cb, 2), -1);
            assert_eq!(BufTableInsert(&mut c, cc, 3), -1);

            assert_eq!(BufTableLookup(&mut a, ca), 1);
            assert_eq!(BufTableLookup(&mut b, cb), 2);
            assert_eq!(BufTableLookup(&mut c, cc), 3);

            // Deleting one leaves the others intact.
            BufTableDelete(&mut b, cb);
            assert_eq!(BufTableLookup(&mut a, ca), 1);
            assert_eq!(BufTableLookup(&mut b, cb), -1);
            assert_eq!(BufTableLookup(&mut c, cc), 3);

            BufTableDelete(&mut a, ca);
            BufTableDelete(&mut c, cc);
            SharedBufHash = null_mut();
        }
    }

    #[test]
    fn tag_helpers_roundtrip() {
        let loc = RelFileLocator {
            spcOid: 1663,
            dbOid: 5,
            relNumber: 77,
        };
        let mut tag: BufferTag = unsafe { std::mem::zeroed() };
        InitBufferTag(&mut tag, &loc, 2, 9);

        assert_eq!(BufTagGetRelNumber(&tag), 77);
        assert_eq!(BufTagGetForkNum(&tag), 2);
        assert!(BufTagMatchesRelFileLocator(&tag, &loc));

        let got = BufTagGetRelFileLocator(&tag);
        assert_eq!(got.spcOid, loc.spcOid);
        assert_eq!(got.dbOid, loc.dbOid);
        assert_eq!(got.relNumber, loc.relNumber);

        let same = {
            let mut t2: BufferTag = unsafe { std::mem::zeroed() };
            InitBufferTag(&mut t2, &loc, 2, 9);
            t2
        };
        assert!(BufferTagsEqual(&tag, &same));

        ClearBufferTag(&mut tag);
        assert_eq!(tag.spcOid, InvalidOid);
        assert_eq!(tag.dbOid, InvalidOid);
        assert_eq!(tag.relNumber, InvalidRelFileNumber);
        assert_eq!(tag.forkNum, InvalidForkNumber);
        assert_eq!(tag.blockNum, InvalidBlockNumber);
    }
}

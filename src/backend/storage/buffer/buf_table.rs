//! Translated from PostgreSQL src/backend/storage/buffer/buf_table.c
//!
//! Routines for mapping `BufferTag`s to buffer indexes.
//!
//! In C this is a single chained shmem hash (`SharedBufHash`) whose entries are
//! protected by `NUM_BUFFER_PARTITIONS` `BufMappingLock` partitions; callers
//! compute `BufTableHashCode()` once, then lock the partition before
//! lookup/insert/delete and hold it across a buffer-header adjustment.
//!
//! Under the single-process port the shmem hash + naked partition LWLocks become
//! a `BufTable`: an array of `NUM_BUFFER_PARTITIONS` shards, each a
//! `std::sync::RwLock<HashMap<BufferTag, i32>>` owning the data it protects (per
//! rules.md section 9: a lock wraps its data, not a naked lock). The partition is
//! chosen by `hashcode % NUM_BUFFER_PARTITIONS`, matching `BufTableHashPartition`.
//!
//! INVARIANT (rules.md section 5): a shard lock is a brief sync critical section;
//! it is NEVER held across an `.await`. Part B (`bufmgr`) takes the shard guard,
//! mutates the map + buffer header, and drops it before any suspension point.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use parking_lot::RwLock;

use crate::storage::buf_internals::BufferTag;

/// Number of buffer-mapping partitions. C: `NUM_BUFFER_PARTITIONS` in lwlock.h.
/// Must be a power of two (the partition index is `hashcode % this`).
pub const NUM_BUFFER_PARTITIONS: usize = 128;
const _: () = assert!(NUM_BUFFER_PARTITIONS.is_power_of_two());

/// The shared buffer-lookup table: `BufferTag -> buf_id` (0-based), sharded for
/// concurrency. Replaces C's `SharedBufHash` + the `BufMappingLock` partitions.
pub struct BufTable {
    shards: Box<[RwLock<HashMap<BufferTag, i32>>]>,
}

impl Default for BufTable {
    fn default() -> Self {
        Self::new()
    }
}

impl BufTable {
    /// C: `InitBufTable`. The C `size` (table sizing hint) is irrelevant for a
    /// `HashMap` that grows on demand, so it is dropped.
    pub fn new() -> Self {
        let shards = (0..NUM_BUFFER_PARTITIONS)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        Self { shards }
    }

    /// C: `BufTableHashCode`. Hash a `BufferTag` to the value used both as the
    /// map key hash and (via [`partition`](Self::partition)) the shard selector.
    pub fn hash_code(tag: &BufferTag) -> u32 {
        let mut h = DefaultHasher::new();
        tag.hash(&mut h);
        h.finish() as u32
    }

    /// C: `BufTableHashPartition`. The shard index for a precomputed hashcode.
    #[inline]
    pub fn partition(hashcode: u32) -> usize {
        hashcode as usize % NUM_BUFFER_PARTITIONS
    }

    /// C: `BufTableLookup`. Return the buffer id for `tag`, or `None` (C `-1`).
    ///
    /// The caller passes the precomputed `hashcode` to pick the shard, exactly
    /// as C passes it to choose the partition lock.
    pub fn lookup(&self, tag: &BufferTag, hashcode: u32) -> Option<i32> {
        let shard = &self.shards[Self::partition(hashcode)];
        let guard = shard.read();
        guard.get(tag).copied()
    }

    /// C: `BufTableInsert`. Insert `tag -> buf_id` unless an entry exists.
    /// Returns `None` on successful insertion (C returns `-1`); if a conflicting
    /// entry already exists, returns the existing buffer id (and does NOT
    /// overwrite it), matching C `HASH_ENTER` + `found`.
    pub fn insert(&self, tag: &BufferTag, hashcode: u32, buf_id: i32) -> Option<i32> {
        debug_assert!(buf_id >= 0); // -1 is reserved for not-in-table
        let shard = &self.shards[Self::partition(hashcode)];
        let mut guard = shard.write();
        if let Some(&existing) = guard.get(tag) { Some(existing) } else {
            guard.insert(*tag, buf_id);
            None
        }
    }

    /// C: `BufTableDelete`. Remove the entry for `tag` (which must exist).
    pub fn delete(&self, tag: &BufferTag, hashcode: u32) {
        let shard = &self.shards[Self::partition(hashcode)];
        let mut guard = shard.write();
        // C: elog(ERROR, "shared buffer hash table corrupted").
        // TODO(panic): migrate to Result + ?
        assert!(guard.remove(tag).is_some(), "shared buffer hash table corrupted");
    }

    /// Number of mapped buffers across all shards. For tests/assertions.
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::relpath::ForkNumber;
    use crate::postgres_ext::Oid;
    use crate::storage::relfilelocator::RelFileLocator;

    fn tag(block: u32) -> BufferTag {
        let loc = RelFileLocator { spcOid: Oid(1), dbOid: Oid(2), relNumber: Oid(3) };
        BufferTag::init(&loc, ForkNumber::MAIN_FORKNUM, block)
    }

    #[test]
    fn insert_lookup_delete_round_trip() {
        let t = BufTable::new();
        let tg = tag(10);
        let hc = BufTable::hash_code(&tg);

        assert_eq!(t.lookup(&tg, hc), None);
        assert_eq!(t.insert(&tg, hc, 7), None, "fresh insert returns None");
        assert_eq!(t.lookup(&tg, hc), Some(7));

        t.delete(&tg, hc);
        assert_eq!(t.lookup(&tg, hc), None);
        assert!(t.is_empty());
    }

    #[test]
    fn insert_conflict_returns_existing_and_does_not_overwrite() {
        let t = BufTable::new();
        let tg = tag(20);
        let hc = BufTable::hash_code(&tg);

        assert_eq!(t.insert(&tg, hc, 1), None);
        // A second insert of the same tag must report the existing id, unchanged.
        assert_eq!(t.insert(&tg, hc, 99), Some(1));
        assert_eq!(t.lookup(&tg, hc), Some(1));
    }

    #[test]
    fn distinct_tags_can_land_in_distinct_shards() {
        let t = BufTable::new();
        // Find two tags whose hashcodes select different partitions.
        let mut a = None;
        let mut b = None;
        for blk in 0..10_000u32 {
            let tg = tag(blk);
            let p = BufTable::partition(BufTable::hash_code(&tg));
            match (a, b) {
                (None, _) => a = Some((tg, p)),
                (Some((_, pa)), None) if p != pa => b = Some((tg, p)),
                _ => {}
            }
            if a.is_some() && b.is_some() {
                break;
            }
        }
        let (ta, pa) = a.expect("found tag a");
        let (tb, pb) = b.expect("found two tags in different shards");
        assert_ne!(pa, pb);

        t.insert(&ta, BufTable::hash_code(&ta), 100);
        t.insert(&tb, BufTable::hash_code(&tb), 200);
        assert_eq!(t.lookup(&ta, BufTable::hash_code(&ta)), Some(100));
        assert_eq!(t.lookup(&tb, BufTable::hash_code(&tb)), Some(200));
        assert_eq!(t.len(), 2);
    }
}

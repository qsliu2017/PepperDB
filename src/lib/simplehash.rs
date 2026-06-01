//! Translation of postgres/src/include/lib/simplehash.h
//!
//! simplehash.h is a C *macro template*: each `#include` with SH_PREFIX/
//! SH_ELEMENT_TYPE/... generates a specialized open-addressing (Robin Hood) hash
//! table.  Rust has generics, so we port it once as `SimpleHash<O>` parameterized
//! by an `ops` trait that supplies the per-instantiation pieces the C template
//! takes as macro arguments (element type, key type, status get/set, key hash,
//! key set/compare).  The algorithm - power-of-two sizing, Robin Hood insert with
//! forward-shift, backward-shift delete, and grow-on-fillfactor - is faithful to
//! the C.
//!
//! Macro-argument -> trait-method mapping:
//!   SH_ELEMENT_TYPE        -> O::Elem
//!   SH_KEY_TYPE            -> O::Key
//!   element `status` field -> O::status / O::set_status
//!   SH_HASH_KEY(tb, key)   -> O::hash_key(key)
//!   SH_KEY (entry's key)   -> O::set_key / O::entry_hash / O::keys_equal
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{uint32, uint64};
use core::marker::PhantomData;

/* SH_STATUS values: a slot is either empty or in use. */
pub const SH_STATUS_EMPTY: u8 = 0x00;
pub const SH_STATUS_IN_USE: u8 = 0x01;

/* Sizing/growth tuning, from simplehash.h. */
const SH_FILLFACTOR: f64 = 0.9;
const SH_MAX_FILLFACTOR: f64 = 0.98;
const SH_GROW_MAX_DIB: u32 = 25;
const SH_GROW_MAX_MOVE: i32 = 150;
const SH_GROW_MIN_FILLFACTOR: f64 = 0.1;
/* The C uses (uint32 max + 1) = 2^32; we cap at 2^31 to keep u32 arithmetic
 * (sizemask, distances) safe.  No real in-memory table approaches this. */
const SH_MAX_SIZE: u64 = 1 << 31;

/// Per-instantiation operations (the simplehash.h SH_* macro arguments).
pub trait SimpleHashOps {
    /// The element stored in the table (must be Copy: the table memmoves slots).
    type Elem: Copy;
    /// The key used for lookup/insert.
    type Key: Copy;

    /// A zeroed/empty element (status must read as SH_STATUS_EMPTY).
    fn empty_elem() -> Self::Elem;
    fn status(e: &Self::Elem) -> u8;
    fn set_status(e: &mut Self::Elem, s: u8);
    /// Hash of a bare key (SH_HASH_KEY).
    fn hash_key(key: Self::Key) -> u32;
    /// Hash of an in-table entry's key (SH_ENTRY_HASH; recomputed from the key
    /// here - the SH_STORE_HASH optimization is not modeled).
    fn entry_hash(e: &Self::Elem) -> u32;
    /// Store a key into an element (entry->SH_KEY = key).
    fn set_key(e: &mut Self::Elem, key: Self::Key);
    /// SH_EQUAL: does the entry's key equal `key`?
    fn keys_equal(e: &Self::Elem, key: Self::Key) -> bool;
}

/// The hash table (SH_TYPE).
pub struct SimpleHash<O: SimpleHashOps> {
    size: uint64,
    members: uint32,
    sizemask: uint32,
    grow_threshold: uint32,
    data: Vec<O::Elem>,
    _ops: PhantomData<O>,
}

/// Iterator state (SH_ITERATOR).
pub struct SimpleHashIterator {
    cur: uint32,
    end: uint32,
    done: bool,
}

#[inline]
fn compute_size(newsize: u64) -> u64 {
    let size = newsize.max(2);
    let size = size.next_power_of_two();
    debug_assert!(size <= SH_MAX_SIZE);
    size
}

impl<O: SimpleHashOps> SimpleHash<O> {
    #[inline]
    fn initial_bucket(&self, hash: uint32) -> uint32 {
        hash & self.sizemask
    }
    #[inline]
    fn next(&self, curelem: uint32) -> uint32 {
        (curelem + 1) & self.sizemask
    }
    #[inline]
    fn prev(&self, curelem: uint32) -> uint32 {
        curelem.wrapping_sub(1) & self.sizemask
    }
    #[inline]
    fn distance_from_optimal(&self, optimal: uint32, bucket: uint32) -> uint32 {
        if optimal <= bucket {
            bucket - optimal
        } else {
            (self.size as uint32 + bucket) - optimal
        }
    }

    fn update_parameters(&mut self, newsize: u64) {
        let size = compute_size(newsize);
        self.size = size;
        self.sizemask = (size - 1) as uint32;
        if self.size == SH_MAX_SIZE {
            self.grow_threshold = (self.size as f64 * SH_MAX_FILLFACTOR) as uint32;
        } else {
            self.grow_threshold = (self.size as f64 * SH_FILLFACTOR) as uint32;
        }
    }

    /// SH_CREATE: a table with room for `nelements` distinct members.
    pub fn create(nelements: uint32) -> Self {
        let mut tb = SimpleHash {
            size: 0,
            members: 0,
            sizemask: 0,
            grow_threshold: 0,
            data: Vec::new(),
            _ops: PhantomData,
        };
        let size = ((SH_MAX_SIZE as f64).min(nelements as f64 / SH_FILLFACTOR)) as u64;
        let size = compute_size(size);
        tb.data = (0..size).map(|_| O::empty_elem()).collect();
        tb.update_parameters(size);
        tb
    }

    /// SH_RESET: clear all contents.
    pub fn reset(&mut self) {
        for e in self.data.iter_mut() {
            *e = O::empty_elem();
        }
        self.members = 0;
    }

    pub fn members(&self) -> uint32 {
        self.members
    }

    /// SH_GROW: grow to at least `newsize` buckets, reinserting all entries.
    pub fn grow(&mut self, newsize: u64) {
        let oldsize = self.size;
        let olddata = core::mem::take(&mut self.data);
        let newsize = compute_size(newsize);

        let mut newdata: Vec<O::Elem> = (0..newsize).map(|_| O::empty_elem()).collect();
        self.update_parameters(newsize);

        // Find the first slot that's empty or at its optimal position, so we can
        // copy entries over without wraparound conflicts.
        let mut startelem: u32 = 0;
        for i in 0..oldsize as u32 {
            let oldentry = &olddata[i as usize];
            if O::status(oldentry) != SH_STATUS_IN_USE {
                startelem = i;
                break;
            }
            let optimal = self.initial_bucket(O::entry_hash(oldentry));
            if optimal == i {
                startelem = i;
                break;
            }
        }

        let mut copyelem = startelem;
        for _ in 0..oldsize as u32 {
            let oldentry = olddata[copyelem as usize];
            if O::status(&oldentry) == SH_STATUS_IN_USE {
                let start2 = self.initial_bucket(O::entry_hash(&oldentry));
                let mut curelem = start2;
                loop {
                    if O::status(&newdata[curelem as usize]) == SH_STATUS_EMPTY {
                        break;
                    }
                    curelem = self.next(curelem);
                }
                newdata[curelem as usize] = oldentry;
            }
            copyelem += 1;
            if copyelem >= oldsize as u32 {
                copyelem = 0;
            }
        }
        self.data = newdata;
    }

    fn insert_hash_internal(&mut self, key: O::Key, hash: uint32) -> (uint32, bool) {
        'restart: loop {
            let mut insertdist: u32 = 0;

            if self.members >= self.grow_threshold {
                debug_assert!(self.size != SH_MAX_SIZE, "hash table size exceeded");
                let dbl = self.size * 2;
                self.grow(dbl);
            }

            let startelem = self.initial_bucket(hash);
            let mut curelem = startelem;
            loop {
                if O::status(&self.data[curelem as usize]) == SH_STATUS_EMPTY {
                    self.members += 1;
                    let e = &mut self.data[curelem as usize];
                    O::set_key(e, key);
                    O::set_status(e, SH_STATUS_IN_USE);
                    return (curelem, false);
                }

                if O::keys_equal(&self.data[curelem as usize], key) {
                    return (curelem, true);
                }

                let curhash = O::entry_hash(&self.data[curelem as usize]);
                let curoptimal = self.initial_bucket(curhash);
                let curdist = self.distance_from_optimal(curoptimal, curelem);

                if insertdist > curdist {
                    // Robin Hood: shift the colliding run forward by one.
                    let mut emptyelem = curelem;
                    let mut emptydist: i32 = 0;
                    loop {
                        emptyelem = self.next(emptyelem);
                        if O::status(&self.data[emptyelem as usize]) == SH_STATUS_EMPTY {
                            break;
                        }
                        emptydist += 1;
                        if emptydist > SH_GROW_MAX_MOVE
                            && (self.members as f64 / self.size as f64) >= SH_GROW_MIN_FILLFACTOR
                        {
                            self.grow_threshold = 0;
                            continue 'restart;
                        }
                    }
                    // shift forward, starting at the last occupied element
                    let mut moveelem = emptyelem;
                    while moveelem != curelem {
                        let src = self.prev(moveelem);
                        self.data[moveelem as usize] = self.data[src as usize];
                        moveelem = src;
                    }
                    self.members += 1;
                    let e = &mut self.data[curelem as usize];
                    O::set_key(e, key);
                    O::set_status(e, SH_STATUS_IN_USE);
                    return (curelem, false);
                }

                curelem = self.next(curelem);
                insertdist += 1;

                if insertdist > SH_GROW_MAX_DIB
                    && (self.members as f64 / self.size as f64) >= SH_GROW_MIN_FILLFACTOR
                {
                    self.grow_threshold = 0;
                    continue 'restart;
                }
            }
        }
    }

    /// SH_INSERT: returns (bucket index, found). On !found the slot's key is set
    /// and status IN_USE; the caller fills in the value via `entry_mut`.
    pub fn insert(&mut self, key: O::Key) -> (uint32, bool) {
        let hash = O::hash_key(key);
        self.insert_hash_internal(key, hash)
    }

    pub fn insert_hash(&mut self, key: O::Key, hash: uint32) -> (uint32, bool) {
        self.insert_hash_internal(key, hash)
    }

    /// SH_LOOKUP: bucket index of `key`, or None if absent.
    pub fn lookup(&self, key: O::Key) -> Option<uint32> {
        let hash = O::hash_key(key);
        self.lookup_hash(key, hash)
    }

    pub fn lookup_hash(&self, key: O::Key, hash: uint32) -> Option<uint32> {
        let startelem = self.initial_bucket(hash);
        let mut curelem = startelem;
        loop {
            let entry = &self.data[curelem as usize];
            if O::status(entry) == SH_STATUS_EMPTY {
                return None;
            }
            if O::keys_equal(entry, key) {
                return Some(curelem);
            }
            curelem = self.next(curelem);
        }
    }

    /// Access an element by its bucket index (from insert/lookup).
    pub fn entry(&self, idx: uint32) -> &O::Elem {
        &self.data[idx as usize]
    }
    pub fn entry_mut(&mut self, idx: uint32) -> &mut O::Elem {
        &mut self.data[idx as usize]
    }

    /// SH_DELETE: remove `key`; returns whether it was present.
    pub fn delete(&mut self, key: O::Key) -> bool {
        let hash = O::hash_key(key);
        let startelem = self.initial_bucket(hash);
        let mut curelem = startelem;
        loop {
            if O::status(&self.data[curelem as usize]) == SH_STATUS_EMPTY {
                return false;
            }
            if O::status(&self.data[curelem as usize]) == SH_STATUS_IN_USE
                && O::keys_equal(&self.data[curelem as usize], key)
            {
                self.members -= 1;
                let mut lastelem = curelem;
                // backward-shift following elements until an empty / optimal one.
                loop {
                    curelem = self.next(curelem);
                    if O::status(&self.data[curelem as usize]) != SH_STATUS_IN_USE {
                        O::set_status(&mut self.data[lastelem as usize], SH_STATUS_EMPTY);
                        break;
                    }
                    let curhash = O::entry_hash(&self.data[curelem as usize]);
                    let curoptimal = self.initial_bucket(curhash);
                    if curoptimal == curelem {
                        O::set_status(&mut self.data[lastelem as usize], SH_STATUS_EMPTY);
                        break;
                    }
                    self.data[lastelem as usize] = self.data[curelem as usize];
                    lastelem = curelem;
                }
                return true;
            }
            curelem = self.next(curelem);
        }
    }

    /// SH_START_ITERATE: begin iteration at the first empty slot (so the current
    /// entry can be deleted mid-iteration despite backward shifts).
    pub fn start_iterate(&self) -> SimpleHashIterator {
        let mut startelem = u32::MAX;
        for i in 0..self.size as u32 {
            if O::status(&self.data[i as usize]) != SH_STATUS_IN_USE {
                startelem = i;
                break;
            }
        }
        debug_assert!((startelem as u64) < SH_MAX_SIZE);
        SimpleHashIterator {
            cur: startelem,
            end: startelem,
            done: false,
        }
    }

    /// SH_ITERATE: next occupied bucket index, or None when done. Iterates
    /// backwards from the start so deletes don't skip/duplicate entries.
    pub fn iterate(&self, iter: &mut SimpleHashIterator) -> Option<uint32> {
        while !iter.done {
            let elem = iter.cur;
            iter.cur = self.prev(iter.cur);
            if iter.cur == iter.end {
                iter.done = true;
            }
            if O::status(&self.data[elem as usize]) == SH_STATUS_IN_USE {
                return Some(elem);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    struct Ent {
        key: u32,
        val: u32,
        status: u8,
    }
    struct Ops;
    impl SimpleHashOps for Ops {
        type Elem = Ent;
        type Key = u32;
        fn empty_elem() -> Ent {
            Ent { key: 0, val: 0, status: SH_STATUS_EMPTY }
        }
        fn status(e: &Ent) -> u8 {
            e.status
        }
        fn set_status(e: &mut Ent, s: u8) {
            e.status = s;
        }
        fn hash_key(key: u32) -> u32 {
            // a simple integer scramble (Knuth)
            key.wrapping_mul(2654435761)
        }
        fn entry_hash(e: &Ent) -> u32 {
            Self::hash_key(e.key)
        }
        fn set_key(e: &mut Ent, key: u32) {
            e.key = key;
        }
        fn keys_equal(e: &Ent, key: u32) -> bool {
            e.key == key
        }
    }

    #[test]
    fn insert_lookup_delete_grow_iterate() {
        let mut tb: SimpleHash<Ops> = SimpleHash::create(4);

        // Insert 1000 keys (forces several grows).
        for k in 0..1000u32 {
            let (idx, found) = tb.insert(k);
            assert!(!found);
            tb.entry_mut(idx).val = k * 10;
        }
        assert_eq!(tb.members(), 1000);

        // Re-insert returns found=true with the existing value.
        let (idx, found) = tb.insert(42);
        assert!(found);
        assert_eq!(tb.entry(idx).val, 420);

        // Lookups.
        for k in 0..1000u32 {
            let i = tb.lookup(k).expect("present");
            assert_eq!(tb.entry(i).key, k);
            assert_eq!(tb.entry(i).val, k * 10);
        }
        assert!(tb.lookup(99999).is_none());

        // Iterate and sum all keys.
        let mut it = tb.start_iterate();
        let mut count = 0u32;
        let mut sum = 0u64;
        while let Some(i) = tb.iterate(&mut it) {
            count += 1;
            sum += tb.entry(i).key as u64;
        }
        assert_eq!(count, 1000);
        assert_eq!(sum, (0..1000u64).sum());

        // Delete the even keys; odd keys remain findable.
        for k in (0..1000u32).step_by(2) {
            assert!(tb.delete(k));
        }
        assert_eq!(tb.members(), 500);
        for k in 0..1000u32 {
            assert_eq!(tb.lookup(k).is_some(), k % 2 == 1);
        }
        assert!(!tb.delete(2)); // already gone
    }
}

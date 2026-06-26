//! Translated from PostgreSQL src/include/storage/procnumber.h

/// Uniquely identifies an active backend/auxiliary process; an index into the
/// proc array, starting from 0.
pub type ProcNumber = i32;

pub const INVALID_PROC_NUMBER: ProcNumber = -1;

pub const MAX_BACKENDS_BITS: u32 = 18;
pub const MAX_BACKENDS: u32 = (1u32 << MAX_BACKENDS_BITS) - 1;

/// Proc number of this backend.
pub static mut MY_PROC_NUMBER: ProcNumber = INVALID_PROC_NUMBER;

/// Proc number of our parallel session leader, or INVALID_PROC_NUMBER if none.
pub static mut PARALLEL_LEADER_PROC_NUMBER: ProcNumber = INVALID_PROC_NUMBER;

/// ProcNumber to use for our session's temp relations.
pub fn proc_number_for_temp_relations() -> ProcNumber {
    unsafe {
        if PARALLEL_LEADER_PROC_NUMBER == INVALID_PROC_NUMBER {
            MY_PROC_NUMBER
        } else {
            PARALLEL_LEADER_PROC_NUMBER
        }
    }
}

// Generational slab: the Rust-native replacement for PostgreSQL's fixed
// slot-index arrays (proc array, ProcSignal slots, sinval cursors, the
// supervisor child registry, bgworker handles). Each `Key` carries a
// generation alongside its index; when a slot is vacated and later reused the
// generation advances, so a stale key from the previous occupant fails lookup
// instead of silently aliasing the new one.
//
// This is a plain data structure with NO internal locking, per the rule that
// each shared structure owns its own locking. Consumers wrap it in whatever
// lock fits their access pattern.

use std::marker::PhantomData;
use std::num::NonZeroU32;

/// A generational handle into a [`GenSlab`]. `Copy` regardless of `T`: the
/// `PhantomData<fn() -> T>` makes the derived bounds unconditional while still
/// type-checking keys against the slab that minted them.
pub struct Key<T> {
    index: u32,
    generation: NonZeroU32,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for Key<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Key<T> {}

impl<T> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.generation == other.generation
    }
}

impl<T> Eq for Key<T> {}

impl<T> std::hash::Hash for Key<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index.hash(state);
        self.generation.hash(state);
    }
}

impl<T> std::fmt::Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Key")
            .field("index", &self.index)
            .field("generation", &self.generation)
            .finish()
    }
}

impl<T> Key<T> {
    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn generation(&self) -> NonZeroU32 {
        self.generation
    }

    /// View the key's index as a `ProcNumber`. Slabs are bounded well under
    /// `i32::MAX`, so the cast never loses or sign-flips the index.
    pub fn as_proc_number(&self) -> ProcNumber {
        self.index as ProcNumber
    }
}

struct Slot<T> {
    generation: u32,
    occupant: Option<T>,
}

/// A slab that hands out generational [`Key`]s. Removing an entry advances its
/// slot generation, so keys never alias across reuse.
pub struct GenSlab<T> {
    slots: Vec<Slot<T>>,
    free: Vec<u32>,
    len: usize,
}

impl<T> Default for GenSlab<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GenSlab<T> {
    pub fn new() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
            len: 0,
        }
    }

    pub fn with_capacity(n: usize) -> Self {
        Self {
            slots: Vec::with_capacity(n),
            free: Vec::with_capacity(n),
            len: 0,
        }
    }

    pub fn insert(&mut self, value: T) -> Key<T> {
        self.len += 1;
        if let Some(index) = self.free.pop() {
            let slot = &mut self.slots[index as usize];
            slot.occupant = Some(value);
            Key {
                index,
                generation: NonZeroU32::new(slot.generation).unwrap(),
                _marker: PhantomData,
            }
        } else {
            let index = self.slots.len() as u32;
            self.slots.push(Slot {
                generation: 1,
                occupant: Some(value),
            });
            Key {
                index,
                generation: NonZeroU32::new(1).unwrap(),
                _marker: PhantomData,
            }
        }
    }

    fn resolve(&self, key: Key<T>) -> Option<&Slot<T>> {
        self.slots
            .get(key.index as usize)
            .filter(|slot| slot.generation == key.generation.get() && slot.occupant.is_some())
    }

    pub fn get(&self, key: Key<T>) -> Option<&T> {
        self.resolve(key).and_then(|slot| slot.occupant.as_ref())
    }

    pub fn get_mut(&mut self, key: Key<T>) -> Option<&mut T> {
        self.slots
            .get_mut(key.index as usize)
            .filter(|slot| slot.generation == key.generation.get())
            .and_then(|slot| slot.occupant.as_mut())
    }

    pub fn remove(&mut self, key: Key<T>) -> Option<T> {
        let slot = self
            .slots
            .get_mut(key.index as usize)
            .filter(|slot| slot.generation == key.generation.get())?;
        let value = slot.occupant.take()?;
        // Advance the generation so the just-freed key can never match again;
        // skip 0 on wraparound to keep it a valid NonZero.
        slot.generation = match slot.generation.wrapping_add(1) {
            0 => 1,
            g => g,
        };
        self.free.push(key.index);
        self.len -= 1;
        Some(value)
    }

    pub fn contains(&self, key: Key<T>) -> bool {
        self.resolve(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Yield every live entry with the key that currently resolves to it.
    pub fn iter(&self) -> impl Iterator<Item = (Key<T>, &T)> {
        self.slots.iter().enumerate().filter_map(|(index, slot)| {
            slot.occupant.as_ref().map(|value| {
                let key = Key {
                    index: index as u32,
                    generation: NonZeroU32::new(slot.generation).unwrap(),
                    _marker: PhantomData,
                };
                (key, value)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_round_trip() {
        let mut slab = GenSlab::new();
        let k = slab.insert("hello".to_string());
        assert_eq!(slab.get(k).map(String::as_str), Some("hello"));

        *slab.get_mut(k).unwrap() = "world".to_string();
        assert_eq!(slab.get(k).map(String::as_str), Some("world"));
    }

    #[test]
    fn remove_invalidates_key() {
        let mut slab = GenSlab::new();
        let k = slab.insert(42);
        assert!(slab.contains(k));
        assert_eq!(slab.remove(k), Some(42));
        assert_eq!(slab.get(k), None);
        assert!(!slab.contains(k));
        assert_eq!(slab.remove(k), None);
    }

    #[test]
    fn reuse_does_not_confuse_stale_key() {
        let mut slab = GenSlab::new();
        let a = slab.insert("A");
        slab.remove(a);
        let b = slab.insert("B");

        assert_eq!(a.index(), b.index(), "B should reuse A's freed slot");
        assert_ne!(a.generation(), b.generation(), "generation must advance");

        assert_eq!(slab.get(a), None, "stale key must not resolve");
        assert_eq!(slab.get(b), Some(&"B"));
        assert!(!slab.contains(a));
        assert!(slab.contains(b));
    }

    #[test]
    fn len_and_is_empty_track() {
        let mut slab = GenSlab::new();
        assert!(slab.is_empty());
        let a = slab.insert(1);
        let b = slab.insert(2);
        assert_eq!(slab.len(), 2);
        slab.remove(a);
        assert_eq!(slab.len(), 1);
        assert!(!slab.is_empty());
        slab.remove(b);
        assert!(slab.is_empty());
    }

    #[test]
    fn iter_yields_live_entries_with_resolving_keys() {
        let mut slab = GenSlab::new();
        let a = slab.insert(10);
        let b = slab.insert(20);
        let c = slab.insert(30);
        slab.remove(b);

        let mut seen: Vec<(u32, i32)> = slab
            .iter()
            .map(|(key, &value)| {
                assert!(slab.contains(key), "iter key must resolve");
                (key.index(), value)
            })
            .collect();
        seen.sort_unstable();
        assert_eq!(seen, vec![(a.index(), 10), (c.index(), 30)]);
    }

    #[test]
    fn key_is_copy_for_non_clone_t() {
        struct NonClone(#[allow(dead_code)] i32);
        let mut slab: GenSlab<NonClone> = GenSlab::new();
        let k = slab.insert(NonClone(7));
        let k1 = k; // Copy
        let k2 = k; // Copy again -- proves Key: Copy independent of T
        assert_eq!(k1, k2);
        assert!(slab.contains(k));
    }
}

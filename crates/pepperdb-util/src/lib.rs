//! Small Rust-native utilities shared across the port (no C counterpart).

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::BuildHasher;

/// Containers constructible from a capacity hint. std has no such trait, so we
/// declare a tiny one and impl it for our targets.
pub trait WithCapacity {
    fn with_capacity(cap: usize) -> Self;
}
impl<T> WithCapacity for Vec<T> {
    fn with_capacity(c: usize) -> Self {
        Self::with_capacity(c)
    }
}
impl<T> WithCapacity for VecDeque<T> {
    fn with_capacity(c: usize) -> Self {
        Self::with_capacity(c)
    }
}
impl WithCapacity for String {
    fn with_capacity(c: usize) -> Self {
        Self::with_capacity(c)
    }
}
impl<K, V, S: BuildHasher + Default> WithCapacity for HashMap<K, V, S> {
    fn with_capacity(c: usize) -> Self {
        Self::with_capacity_and_hasher(c, S::default())
    }
}
impl<T, S: BuildHasher + Default> WithCapacity for HashSet<T, S> {
    fn with_capacity(c: usize) -> Self {
        Self::with_capacity_and_hasher(c, S::default())
    }
}

/// Collect an iterator while preallocating the destination, avoiding the
/// reallocations a plain `collect()` does after `filter` zeroes the lower
/// size_hint. Use when the result count is bounded and you want one allocation.
pub trait PreallocCollect: Iterator + Sized {
    /// Collect, preallocating from the iterator's *upper* size_hint (the
    /// "aggressive" bit -- `collect` uses the lower bound, which `filter`
    /// zeroes). Falls back to the lower bound, then 0. Over-allocates when a
    /// filter rejects most items, so prefer it where most items are kept or the
    /// upper bound is a tight, small constant (e.g. MaxBackends).
    fn prealloc_collect<C>(self) -> C
    where
        C: WithCapacity + Extend<Self::Item>,
    {
        let (lo, hi) = self.size_hint();
        let mut c = C::with_capacity(hi.unwrap_or(lo));
        c.extend(self);
        c
    }
    /// Same, with an explicit capacity -- for chains whose upper hint is `None`
    /// (`flat_map`, `flatten`), where only the caller can compute the bound.
    fn collect_with_capacity<C>(self, cap: usize) -> C
    where
        C: WithCapacity + Extend<Self::Item>,
    {
        let mut c = C::with_capacity(cap);
        c.extend(self);
        c
    }
}
impl<I: Iterator> PreallocCollect for I {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prealloc_collect_keeps_all() {
        let v: Vec<i32> = (0..5).prealloc_collect();
        assert_eq!(v, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn collect_with_capacity_for_flat_map() {
        let v: Vec<i32> = (0..3)
            .flat_map(|x| [x, x * 10])
            .collect_with_capacity(6);
        assert_eq!(v, vec![0, 0, 1, 10, 2, 20]);
        assert!(v.capacity() >= 6);
    }

    #[test]
    fn prealloc_collect_filtered() {
        // Upper hint = input len (filter can't tighten it); result is shorter.
        let v: Vec<i32> = (0..10).filter(|x| x % 2 == 0).prealloc_collect();
        assert_eq!(v, vec![0, 2, 4, 6, 8]);
    }
}

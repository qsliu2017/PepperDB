//! Translated from PostgreSQL src/include/lib/radixtree.h
//!
//! TEMPLATE header (RT_*, macro-generated per instantiation). This is NOT a
//! concrete Rust type. Each C instantiation (`RT_PREFIX foo` -> `foo_radix_tree`)
//! maps to a `BTreeMap<u64, V>` in the port: keys are unsigned 64-bit integers
//! and iteration is in ascending key order, matching the adaptive radix tree's
//! ordered semantics (see translation-rules.md container table). The adaptive
//! node kinds (node4/16/48/256), SIMD search, slab allocation, and DSA/shmem
//! support are internal performance machinery and are dropped: BTreeMap provides
//! the same ordered map contract. The shmem variant collapses under the
//! single-process model (Arc + lock if shared).
//!
//! Interface mapping (per instantiation, against BTreeMap<u64, V>):
//!   RT_CREATE/RT_FREE       -> BTreeMap::new / drop
//!   RT_FIND(key)            -> map.get(&key) -> Option<&V>
//!   RT_SET(key, val)        -> map.insert(key, val) (returns prior presence)
//!   RT_DELETE(key)          -> map.remove(&key).is_some()
//!   RT_BEGIN_ITERATE/NEXT   -> map.iter() (ascending)
//!   RT_MEMORY_USAGE         -> bespoke estimate (n/a for skeleton)

/// Node kinds (`RT_NODE_KIND_*`). Sequential ordinal, not a flag set
/// (bitflags-port.md appendix D), so a Rust enum. These describe internal node
/// layout in the C template and are not needed by the BTreeMap mapping; kept for
/// fidelity.
#[repr(u8)]
pub enum RtNodeKind {
    Node4 = 0x00,
    Node16 = 0x01,
    Node48 = 0x02,
    Node256 = 0x03,
}

/// `RT_NODE_KIND_COUNT` - number of node kinds (fixed at 4).
pub const RT_NODE_KIND_COUNT: usize = 4;

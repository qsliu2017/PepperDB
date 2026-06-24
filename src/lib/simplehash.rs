//! Translated from PostgreSQL src/include/lib/simplehash.h
//!
//! TEMPLATE header (SH_*, macro-generated per instantiation). This is NOT a
//! concrete Rust type. Each C instantiation (`SH_PREFIX foo` -> `foo_hash`) maps
//! to a `HashMap<K, V>` in the port (see translation-rules.md container table).
//! The open-addressing / robin-hood layout, the `status` slot tags, the
//! load-factor and grow machinery, and the custom allocator hooks are internal
//! performance details and are dropped: HashMap provides the same key/value map
//! contract.
//!
//! Interface mapping (per instantiation, against HashMap<K, V>):
//!   SH_CREATE/SH_DESTROY/SH_RESET -> HashMap::with_capacity / drop / clear
//!   SH_INSERT(key) -> (entry, found) -> map.entry(key) / insert
//!   SH_LOOKUP(key)                -> map.get(&key) -> Option<&V>
//!   SH_DELETE(key)                -> map.remove(&key).is_some()
//!   SH_START_ITERATE / SH_ITERATE -> map.iter() / iter_mut()
//!   SH_GROW / SH_STAT             -> n/a (HashMap handles resizing)

/// `SH_STATUS` - per-slot occupancy tag in the open-addressing table. With
/// HashMap this is implicit; kept for fidelity to the template.
#[repr(u8)]
pub enum ShStatus {
    Empty = 0x00,
    InUse = 0x01,
}

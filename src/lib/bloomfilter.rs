//! Translated from PostgreSQL src/include/lib/bloomfilter.h
//!
//! Space-efficient set membership testing. `bloom_filter` is opaque in C; its
//! real definition lives in the .c file. We keep it opaque here (fields private).

/// A Bloom filter (opaque; internal layout defined in the implementation).
pub struct bloom_filter {
    _private: (),
}

/// bloom_create: build a filter sized for `total_elems` within `bloom_work_mem` KB.
pub fn bloom_create(_total_elems: i64, _bloom_work_mem: i32, _seed: u64) -> bloom_filter {
    unimplemented!()
}

/// bloom_free: drop the filter (RAII; provided for parity).
#[allow(clippy::drop_non_drop, reason = "explicit drop mirrors PG free; no-op on non-Drop type")]
pub fn bloom_free(filter: bloom_filter) {
    drop(filter);
}

/// bloom_add_element: add an element.
pub fn bloom_add_element(_filter: &mut bloom_filter, _elem: &[u8]) {
    unimplemented!()
}

/// bloom_lacks_element: true if the element is definitely absent.
pub fn bloom_lacks_element(_filter: &bloom_filter, _elem: &[u8]) -> bool {
    unimplemented!()
}

/// bloom_prop_bits_set: fraction of bits set (for false-positive estimation).
pub fn bloom_prop_bits_set(_filter: &bloom_filter) -> f64 {
    unimplemented!()
}

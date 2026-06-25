//! Translated from PostgreSQL src/include/access/rmgrdesc_utils.h

use crate::lib::stringinfo::StringInfo;

/// Describe an array; per-element callback writes into `buf` (void *arg -> closure).
pub fn array_desc(
    _buf: &mut StringInfo,
    _array: &[u8],
    _elem_size: usize,
    _count: i32,
    _elem_desc: impl Fn(&mut StringInfo, &[u8]),
) {
    unimplemented!()
}

pub fn offset_elem_desc(_buf: &mut StringInfo, _offset: &[u8]) {
    unimplemented!()
}

pub fn redirect_elem_desc(_buf: &mut StringInfo, _offset: &[u8]) {
    unimplemented!()
}

pub fn oid_elem_desc(_buf: &mut StringInfo, _relid: &[u8]) {
    unimplemented!()
}

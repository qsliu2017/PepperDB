//! Translated from PostgreSQL src/include/access/rmgrdesc_utils.h

// TODO(struct-forward): StringInfoData lives in lib/stringinfo.h; repoint to
// crate::lib::stringinfo in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::lib::stringinfo::StringInfo in Phase 2")]
pub struct StringInfo {
    _opaque: [u8; 0],
}

/// Describe an array; per-element callback writes into `buf` (void *arg -> closure).
#[allow(deprecated)]
pub fn array_desc(
    _buf: &mut StringInfo,
    _array: &[u8],
    _elem_size: usize,
    _count: i32,
    _elem_desc: impl Fn(&mut StringInfo, &[u8]),
) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn offset_elem_desc(_buf: &mut StringInfo, _offset: &[u8]) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn redirect_elem_desc(_buf: &mut StringInfo, _offset: &[u8]) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn oid_elem_desc(_buf: &mut StringInfo, _relid: &[u8]) {
    unimplemented!()
}

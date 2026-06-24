//! Translated from PostgreSQL src/include/access/toast_internals.h

use crate::access::toast_compression::ToastCompressionId;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::lockdefs::LockMode;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::Snapshot;
use crate::varatt::{varlena, VARLENA_EXTSIZE_BITS, VARLENA_EXTSIZE_MASK};

/// Information at the start of compressed toast data (on-disk).
#[repr(C)]
pub struct toast_compress_header {
    pub vl_len_: i32,  // varlena header (do not touch directly!)
    pub tcinfo: u32,   // 2 bits compression method + 30 bits external size
}

/// External (uncompressed) size from a compressed toast header.
pub fn TOAST_COMPRESS_EXTSIZE(ptr: &toast_compress_header) -> u32 {
    ptr.tcinfo & VARLENA_EXTSIZE_MASK
}
/// Compression method id from a compressed toast header.
pub fn TOAST_COMPRESS_METHOD(ptr: &toast_compress_header) -> u32 {
    ptr.tcinfo >> VARLENA_EXTSIZE_BITS
}
/// Pack external size + compression method into the header's tcinfo word.
pub fn TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(
    ptr: &mut toast_compress_header,
    len: u32,
    cm_method: ToastCompressionId,
) {
    debug_assert!(len > 0 && len <= VARLENA_EXTSIZE_MASK);
    ptr.tcinfo = len | ((cm_method as u32) << VARLENA_EXTSIZE_BITS);
}

pub fn toast_compress_datum(_value: Datum, _cmethod: i8) -> Datum {
    unimplemented!()
}
pub fn toast_get_valid_index(_toastoid: Oid, _lock: LockMode) -> Oid {
    unimplemented!()
}
pub fn toast_delete_datum(_rel: Relation, _value: Datum, _is_speculative: bool) {
    unimplemented!()
}
pub fn toast_save_datum(
    _rel: Relation,
    _value: Datum,
    _oldexternal: &varlena,
    _options: i32,
) -> Datum {
    unimplemented!()
}
/// Returns the open toast index relations (out-params folded into the return).
pub fn toast_open_indexes(_toastrel: Relation, _lock: LockMode) -> Vec<Relation> {
    unimplemented!()
}
pub fn toast_close_indexes(_toastidxs: &[Relation], _lock: LockMode) {
    unimplemented!()
}
pub fn get_toast_snapshot() -> Snapshot<'static> {
    unimplemented!()
}

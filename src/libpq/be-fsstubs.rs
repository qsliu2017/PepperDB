//! Translated from PostgreSQL src/include/libpq/be-fsstubs.h

use crate::c::SubTransactionId;

/// C: `int lo_read(int fd, char *buf, int len)`. Returns bytes read.
pub fn lo_read(_fd: i32, _buf: &mut [u8]) -> i32 {
    unimplemented!()
}

/// C: `int lo_write(int fd, const char *buf, int len)`. Returns bytes written.
pub fn lo_write(_fd: i32, _buf: &[u8]) -> i32 {
    unimplemented!()
}

// Cleanup LOs at xact commit/abort.
pub fn at_eo_xact_large_object(_is_commit: bool) {
    unimplemented!()
}

pub fn at_eo_sub_xact_large_object(
    _is_commit: bool,
    _my_subid: SubTransactionId,
    _parent_subid: SubTransactionId,
) {
    unimplemented!()
}

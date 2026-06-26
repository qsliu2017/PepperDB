//! Translated from PostgreSQL src/include/storage/large_object.h
//!
//! Large objects (inversion). The descriptor is in-memory; the LO bytes live in
//! pg_largeobject as bytea chunks.
#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use bitflags::bitflags;

use crate::c::SubTransactionId;
use crate::pg_config::BLCKSZ;
use crate::postgres_ext::Oid;
use crate::utils::palloc::MemoryContext;
use crate::utils::snapshot::SnapshotData;

bitflags! {
    /// LargeObjectDesc.flags bits. As of v11 these also imply the permission has
    /// been checked.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct LoFlags: i32 {
        const RDLOCK = 1 << 0; // LO was opened for reading
        const WRLOCK = 1 << 1; // LO was opened for writing
    }
}

/// Data about a currently-open large object (in-memory).
pub struct LargeObjectDesc<'a> {
    pub id: Oid,                              // LO's identifier
    pub snapshot: Option<&'a mut SnapshotData>, // snapshot to use
    pub subid: SubTransactionId,             // owning subtransaction ID
    pub offset: u64,                         // current seek pointer
    pub flags: LoFlags,                      // see LoFlags
}

/// Each "page" (tuple) of a large object can hold this much data.
/// NB: changing LOBLKSIZE requires an initdb.
pub const LOBLKSIZE: u32 = BLCKSZ / 4;

/// Maximum length in bytes for a large object.
pub const MAX_LARGE_OBJECT_SIZE: i64 = (i32::MAX as i64) * (LOBLKSIZE as i64);

/// GUC: backwards-compatibility flag to suppress LO permission checks.
// TODO: GUC global; convert to session/exec-context state in a later pass.
pub static mut lo_compat_privileges: bool = false;

// inversion stuff in inv_api.c

pub fn close_lo_relation(_is_commit: bool) {
    unimplemented!()
}
pub fn inv_create(_lobj_id: Oid) -> Oid {
    unimplemented!()
}
pub fn inv_open<'a>(_lobj_id: Oid, _flags: i32, _mcxt: MemoryContext) -> Box<LargeObjectDesc<'a>> {
    unimplemented!()
}
pub fn inv_close(_obj_desc: Box<LargeObjectDesc>) {
    unimplemented!()
}
pub fn inv_drop(_lobj_id: Oid) -> i32 {
    unimplemented!()
}
pub fn inv_seek(_obj_desc: &mut LargeObjectDesc, _offset: i64, _whence: i32) -> i64 {
    unimplemented!()
}
pub fn inv_tell(_obj_desc: &mut LargeObjectDesc) -> i64 {
    unimplemented!()
}
pub fn inv_read(_obj_desc: &mut LargeObjectDesc, _buf: &mut [u8]) -> i32 {
    unimplemented!()
}
pub fn inv_write(_obj_desc: &mut LargeObjectDesc, _buf: &[u8]) -> i32 {
    unimplemented!()
}
pub fn inv_truncate(_obj_desc: &mut LargeObjectDesc, _len: i64) {
    unimplemented!()
}

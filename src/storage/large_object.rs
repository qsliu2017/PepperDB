//! large_object.h - Declarations for PostgreSQL large objects (inversion only).

use std::ffi::c_int;

use crate::c::{int64, uint64, SubTransactionId};
use crate::pg_config::BLCKSZ;
use crate::postgres_ext::Oid;

// Snapshot comes from utils/snapshot.h (not yet ported as snapshot.rs).
// Canonical def lives in nodes/execnodes.rs as `*mut SnapshotData`.
// TODO: dedup when snapshot.h lands.
pub type Snapshot = *mut c_void;
use std::ffi::c_void;

// MemoryContext comes from utils/palloc.h / nodes/memnodes.h.
// Canonical def: `*mut MemoryContextData` (utils/palloc.rs, utils/mmgr/memnodes.rs).
// TODO: dedup when fully wired.
pub type MemoryContext = *mut c_void;

/*----------
 * Data about a currently-open large object.
 *
 * id is the logical OID of the large object
 * snapshot is the snapshot to use for read/write operations
 * subid is the subtransaction that opened the desc (or currently owns it)
 * offset is the current seek offset within the LO
 * flags contains some flag bits
 *----------
 */
#[repr(C)]
pub struct LargeObjectDesc {
    pub id: Oid,                  // LO's identifier
    pub snapshot: Snapshot,       // snapshot to use
    pub subid: SubTransactionId,  // owning subtransaction ID
    pub offset: uint64,           // current seek pointer
    pub flags: c_int,             // see flag bits below
}

/* bits in flags: */
pub const IFS_RDLOCK: c_int = 1 << 0; // LO was opened for reading
pub const IFS_WRLOCK: c_int = 1 << 1; // LO was opened for writing

/*
 * Each "page" (tuple) of a large object can hold this much data.
 *
 * NB: Changing LOBLKSIZE requires an initdb.
 */
pub const LOBLKSIZE: usize = BLCKSZ / 4;

/*
 * Maximum length in bytes for a large object.
 */
pub const MAX_LARGE_OBJECT_SIZE: int64 = (i32::MAX as int64) * (LOBLKSIZE as int64);

/*
 * GUC: backwards-compatibility flag to suppress LO permission checks
 */
// extern PGDLLIMPORT bool lo_compat_privileges;
#[no_mangle]
pub static mut lo_compat_privileges: bool = false;

/*
 * Function definitions...
 */

/* inversion stuff in inv_api.c */
pub unsafe fn close_lo_relation(_isCommit: bool) {
    unimplemented!()
}

pub unsafe fn inv_create(_lobjId: Oid) -> Oid {
    unimplemented!()
}

pub unsafe fn inv_open(_lobjId: Oid, _flags: c_int, _mcxt: MemoryContext) -> *mut LargeObjectDesc {
    unimplemented!()
}

pub unsafe fn inv_close(_obj_desc: *mut LargeObjectDesc) {
    unimplemented!()
}

pub unsafe fn inv_drop(_lobjId: Oid) -> c_int {
    unimplemented!()
}

pub unsafe fn inv_seek(_obj_desc: *mut LargeObjectDesc, _offset: int64, _whence: c_int) -> int64 {
    unimplemented!()
}

pub unsafe fn inv_tell(_obj_desc: *mut LargeObjectDesc) -> int64 {
    unimplemented!()
}

pub unsafe fn inv_read(_obj_desc: *mut LargeObjectDesc, _buf: *mut c_char, _nbytes: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn inv_write(
    _obj_desc: *mut LargeObjectDesc,
    _buf: *const c_char,
    _nbytes: c_int,
) -> c_int {
    unimplemented!()
}

pub unsafe fn inv_truncate(_obj_desc: *mut LargeObjectDesc, _len: int64) {
    unimplemented!()
}

use std::ffi::c_char;

pub mod inv_api;

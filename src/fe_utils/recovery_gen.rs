//! fe_utils/recovery_gen.h - Generator for recovery configuration.

use std::ffi::{c_char, c_int, c_void};

// libpq / pqexpbuffer frontend types - stubbed locally (not yet ported).
// TODO: dedup
pub type PGconn = c_void;
// PQExpBuffer is a pointer to a PQExpBufferData; stub the buffer body as an
// opaque struct and the handle as a pointer to it.
// TODO: dedup
#[repr(C)]
pub struct PQExpBufferData {
    _private: [u8; 0],
}
// TODO: dedup
pub type PQExpBuffer = *mut PQExpBufferData;

/*
 * recovery configuration is part of postgresql.conf in version 12 and up, and
 * in recovery.conf before that.
 */
pub const MINIMUM_VERSION_FOR_RECOVERY_GUC: c_int = 120000;

// extern PQExpBuffer GenerateRecoveryConfig(PGconn *pgconn,
//                                           const char *replication_slot,
//                                           char *dbname);
pub unsafe fn GenerateRecoveryConfig(
    pgconn: *mut PGconn,
    replication_slot: *const c_char,
    dbname: *mut c_char,
) -> PQExpBuffer {
    unimplemented!()
}

// extern void WriteRecoveryConfig(PGconn *pgconn, const char *target_dir,
//                                 PQExpBuffer contents);
pub unsafe fn WriteRecoveryConfig(
    pgconn: *mut PGconn,
    target_dir: *const c_char,
    contents: PQExpBuffer,
) {
    unimplemented!()
}

// extern char *GetDbnameFromConnectionOptions(const char *connstr);
pub unsafe fn GetDbnameFromConnectionOptions(connstr: *const c_char) -> *mut c_char {
    unimplemented!()
}

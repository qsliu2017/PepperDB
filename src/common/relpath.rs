//! Translated from PostgreSQL src/include/common/relpath.h
//! Declarations for GetRelationPath() and friends.

use crate::catalog::catversion::CATALOG_VERSION_NO;
use crate::pg_config::PG_MAJORVERSION;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};

/// RelFileNumber identifies the specific relation file name.
pub type RelFileNumber = Oid;

pub const InvalidRelFileNumber: RelFileNumber = InvalidOid;

pub const fn rel_file_number_is_valid(relnumber: RelFileNumber) -> bool {
    relnumber.0 != InvalidRelFileNumber.0
}

/// Name of major-version-specific tablespace subdirectories.
pub fn tablespace_version_directory() -> String {
    format!("PG_{}_{}", PG_MAJORVERSION, CATALOG_VERSION_NO)
}

pub const PG_TBLSPC_DIR: &str = "pg_tblspc";
pub const PG_TBLSPC_DIR_SLASH: &str = "pg_tblspc/";

/// Characters to allow for an OID in a relation path (max chars printed by %u).
pub const OIDCHARS: usize = 10;

/// The physical storage of a relation consists of one or more forks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ForkNumber {
    InvalidForkNumber = -1,
    MAIN_FORKNUM = 0,
    FSM_FORKNUM = 1,
    VISIBILITYMAP_FORKNUM = 2,
    INIT_FORKNUM = 3,
}

pub const MAX_FORKNUM: ForkNumber = ForkNumber::INIT_FORKNUM;

/// Max chars for a fork name.
pub const FORKNAMECHARS: usize = 4;

// TODO(generated): forkNames array lives in src/common/relpath.c.
pub fn fork_names() -> &'static [&'static str] {
    unimplemented!()
}

/// Look up a fork number by name; None if unknown.
pub fn forkname_to_number(fork_name: &str) -> Option<ForkNumber> {
    let _ = fork_name;
    unimplemented!()
}

/// Count the leading chars of `str` that form a fork name; returns (chars, fork).
pub fn forkname_chars(s: &str) -> (i32, Option<ForkNumber>) {
    let _ = s;
    unimplemented!()
}

/// No easy way to derive this from MAX_BACKENDS (2^18-1). Crosschecked in tests.
pub const PROCNUMBER_CHARS: usize = 6;

/// Longest possible relation path length, excluding the trailing null byte.
pub const REL_PATH_STR_MAXLEN: usize = (PG_TBLSPC_DIR.len())
    + 1 // '/'
    + OIDCHARS // spcOid
    + 1 // '/'
    + (3 + 2 + 9) // TABLESPACE_VERSION_DIRECTORY: "PG_" + majorversion + "_" + catver
    + 1 // '/'
    + OIDCHARS // dbOid
    + 1 // '/'
    + 1 // 't' temporary table indicator
    + PROCNUMBER_CHARS
    + 1 // '_'
    + OIDCHARS // relNumber
    + 1 // '_'
    + FORKNAMECHARS;

/// String of the exact length required to represent a relation path.
pub struct RelPathStr {
    pub str: [u8; REL_PATH_STR_MAXLEN + 1],
}

/// Filesystem path for a database (relative to installation's $PGDATA).
pub fn get_database_path(db_oid: Oid, spc_oid: Oid) -> String {
    let _ = (db_oid, spc_oid);
    unimplemented!()
}

/// Filesystem path for a relation fork.
pub fn get_relation_path(
    db_oid: Oid,
    spc_oid: Oid,
    rel_number: RelFileNumber,
    proc_number: ProcNumber,
    fork_number: ForkNumber,
) -> RelPathStr {
    let _ = (db_oid, spc_oid, rel_number, proc_number, fork_number);
    unimplemented!()
}

// TODO(struct-forward): repoint to crate::storage::relfilelocator in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::storage::relfilelocator in Phase 2")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
}

// TODO(struct-forward): repoint to crate::storage::relfilelocator in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::storage::relfilelocator in Phase 2")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelFileLocatorBackend {
    #[allow(deprecated)]
    pub locator: RelFileLocator,
    pub backend: ProcNumber,
}

/// Wrapper for GetRelationPath; first argument is a RelFileLocator.
#[allow(deprecated)]
pub fn relpathbackend(
    rlocator: RelFileLocator,
    backend: ProcNumber,
    forknum: ForkNumber,
) -> RelPathStr {
    get_relation_path(rlocator.dbOid, rlocator.spcOid, rlocator.relNumber, backend, forknum)
}

/// Wrapper for GetRelationPath for a permanent (shared) relation.
#[allow(deprecated)]
pub fn relpathperm(rlocator: RelFileLocator, forknum: ForkNumber) -> RelPathStr {
    relpathbackend(rlocator, INVALID_PROC_NUMBER, forknum)
}

/// Wrapper for GetRelationPath; first argument is a RelFileLocatorBackend.
#[allow(deprecated)]
pub fn relpath(rlocator: RelFileLocatorBackend, forknum: ForkNumber) -> RelPathStr {
    relpathbackend(rlocator.locator, rlocator.backend, forknum)
}

//! Translated from PostgreSQL src/include/storage/fileset.h
//! Management of named temporary files.

use crate::postgres_ext::Oid;
use crate::storage::fd::File;

/// A set of temporary files. In-memory bookkeeping; not on-disk.
#[derive(Debug, Clone)]
pub struct FileSet {
    pub creator_pid: u32,   // PID of the creating process (pid_t)
    pub number: u32,        // per-PID identifier
    pub tablespaces: Vec<Oid>, // OIDs of tablespaces to use (C: fixed [8] + count)
}

pub fn FileSetInit(_fileset: &mut FileSet) {
    unimplemented!()
}

pub fn FileSetCreate(_fileset: &mut FileSet, _name: &str) -> File {
    unimplemented!()
}

pub fn FileSetOpen(_fileset: &mut FileSet, _name: &str, _mode: i32) -> File {
    unimplemented!()
}

/// Returns whether the file was deleted; `error_on_failure` panics on error.
pub fn FileSetDelete(_fileset: &mut FileSet, _name: &str, _error_on_failure: bool) -> bool {
    unimplemented!()
}

pub fn FileSetDeleteAll(_fileset: &mut FileSet) {
    unimplemented!()
}

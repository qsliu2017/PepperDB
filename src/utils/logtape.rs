//! Translated from PostgreSQL src/include/utils/logtape.h

// LogicalTapeSet and LogicalTape are opaque spill-to-disk handles whose details
// live in logtape.c. Modeled as opaque structs; API stubbed.

use crate::storage::sharedfileset::SharedFileSet;

/// Opaque handle for a set of logical tapes backed by a temporary file.
pub struct LogicalTapeSet {
    _private: (),
}

/// Opaque handle for a single logical tape within a tape set.
pub struct LogicalTape {
    _private: (),
}

/// TapeShare metadata exported when freezing a worker's materialized tape.
pub struct TapeShare {
    /// Location of the materialized tape's first block.
    pub firstblocknumber: i64,
}

pub fn LogicalTapeSetCreate(
    preallocate: bool,
    fileset: Option<&mut SharedFileSet>,
    worker: i32,
) -> Box<LogicalTapeSet> {
    unimplemented!()
}

pub fn LogicalTapeClose(lt: &mut LogicalTape) {
    unimplemented!()
}

pub fn LogicalTapeSetClose(lts: &mut LogicalTapeSet) {
    unimplemented!()
}

pub fn LogicalTapeCreate(lts: &mut LogicalTapeSet) -> Box<LogicalTape> {
    unimplemented!()
}

pub fn LogicalTapeImport(
    lts: &mut LogicalTapeSet,
    worker: i32,
    shared: &TapeShare,
) -> Box<LogicalTape> {
    unimplemented!()
}

pub fn LogicalTapeSetForgetFreeSpace(lts: &mut LogicalTapeSet) {
    unimplemented!()
}

pub fn LogicalTapeRead(lt: &mut LogicalTape, ptr: &mut [u8], size: usize) -> usize {
    unimplemented!()
}

pub fn LogicalTapeWrite(lt: &mut LogicalTape, ptr: &[u8], size: usize) {
    unimplemented!()
}

pub fn LogicalTapeRewindForRead(lt: &mut LogicalTape, buffer_size: usize) {
    unimplemented!()
}

pub fn LogicalTapeFreeze(lt: &mut LogicalTape, share: Option<&mut TapeShare>) {
    unimplemented!()
}

pub fn LogicalTapeBackspace(lt: &mut LogicalTape, size: usize) -> usize {
    unimplemented!()
}

pub fn LogicalTapeSeek(lt: &mut LogicalTape, blocknum: i64, offset: i32) {
    unimplemented!()
}

/// C out-params `(int64 *blocknum, int *offset)` -> tuple.
pub fn LogicalTapeTell(lt: &LogicalTape) -> (i64, i32) {
    unimplemented!()
}

pub fn LogicalTapeSetBlocks(lts: &LogicalTapeSet) -> i64 {
    unimplemented!()
}

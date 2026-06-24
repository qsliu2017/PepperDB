//! Translated from PostgreSQL src/include/storage/md.h
//!
//! STUB (foundation-rewrite: smgr). The magnetic-disk smgr backend behind
//! `crate::storage::smgr`. All bodies are `// TODO(smgr)`.

use crate::common::relpath::ForkNumber;
use crate::postgres_ext::Oid;
use crate::storage::aio_internal::PgAioHandle;
use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::{RelFileLocator, RelFileLocatorBackend};
use crate::storage::smgr::SmgrRelation;
use crate::storage::sync::FileTag;

// const PgAioHandleCallbacks aio_md_readv_cb -> deferred (Phase 2).

// md storage manager functionality
pub fn mdinit() {
    unimplemented!() // TODO(smgr)
}
pub fn mdopen(_reln: &mut SmgrRelation) {
    unimplemented!() // TODO(smgr)
}
pub fn mdclose(_reln: &mut SmgrRelation, _forknum: ForkNumber) {
    unimplemented!() // TODO(smgr)
}
pub fn mdcreate(_reln: &mut SmgrRelation, _forknum: ForkNumber, _is_redo: bool) {
    unimplemented!() // TODO(smgr)
}
pub fn mdexists(_reln: &mut SmgrRelation, _forknum: ForkNumber) -> bool {
    unimplemented!() // TODO(smgr)
}
pub fn mdunlink(_rlocator: RelFileLocatorBackend, _forknum: ForkNumber, _is_redo: bool) {
    unimplemented!() // TODO(smgr)
}
pub fn mdextend(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffer: &[u8],
    _skip_fsync: bool,
) {
    unimplemented!() // TODO(smgr)
}
pub fn mdzeroextend(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: i32,
    _skip_fsync: bool,
) {
    unimplemented!() // TODO(smgr)
}
pub fn mdprefetch(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: i32,
) -> bool {
    unimplemented!() // TODO(smgr)
}
pub fn mdmaxcombine(_reln: &mut SmgrRelation, _forknum: ForkNumber, _blocknum: BlockNumber) -> u32 {
    unimplemented!() // TODO(smgr)
}
pub fn mdreadv(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffers: &mut [&mut [u8]],
    _nblocks: BlockNumber,
) {
    unimplemented!() // TODO(smgr)
}
pub fn mdstartreadv(
    _ioh: &mut PgAioHandle,
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffers: &mut [&mut [u8]],
    _nblocks: BlockNumber,
) {
    unimplemented!() // TODO(smgr)
}
pub fn mdwritev(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffers: &[&[u8]],
    _nblocks: BlockNumber,
    _skip_fsync: bool,
) {
    unimplemented!() // TODO(smgr)
}
pub fn mdwriteback(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: BlockNumber,
) {
    unimplemented!() // TODO(smgr)
}
pub fn mdnblocks(_reln: &mut SmgrRelation, _forknum: ForkNumber) -> BlockNumber {
    unimplemented!() // TODO(smgr)
}
pub fn mdtruncate(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _curnblk: BlockNumber,
    _nblocks: BlockNumber,
) {
    unimplemented!() // TODO(smgr)
}
pub fn mdimmedsync(_reln: &mut SmgrRelation, _forknum: ForkNumber) {
    unimplemented!() // TODO(smgr)
}
pub fn mdregistersync(_reln: &mut SmgrRelation, _forknum: ForkNumber) {
    unimplemented!() // TODO(smgr)
}
// mdfd's `uint32 *off` out-param folded into the return tuple.
pub fn mdfd(_reln: &mut SmgrRelation, _forknum: ForkNumber, _blocknum: BlockNumber) -> (i32, u32) {
    unimplemented!() // TODO(smgr)
}

pub fn ForgetDatabaseSyncRequests(_dbid: Oid) {
    unimplemented!() // TODO(smgr)
}
pub fn DropRelationFiles(_delrels: &[RelFileLocator], _is_redo: bool) {
    unimplemented!() // TODO(smgr)
}

// md sync callbacks; the `char *path` out-param folds into the returned String.
pub fn mdsyncfiletag(_ftag: &FileTag) -> (i32, String) {
    unimplemented!() // TODO(smgr)
}
pub fn mdunlinkfiletag(_ftag: &FileTag) -> (i32, String) {
    unimplemented!() // TODO(smgr)
}
pub fn mdfiletagmatches(_ftag: &FileTag, _candidate: &FileTag) -> bool {
    unimplemented!() // TODO(smgr)
}

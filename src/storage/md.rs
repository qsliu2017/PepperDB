//! Translated from PostgreSQL src/include/storage/md.h
//!
//! Declarations for the magnetic-disk smgr backend. The bodies live in
//! `crate::backend::storage::smgr::md`; this header re-exports them so md.h call
//! sites resolve. The smgr switch (`crate::storage::smgr`) is the normal caller.
//!
//! Deleted vs md.h: `mdstartreadv` and the `aio_md_readv_cb` callbacks (the PG18
//! AIO read path is collapsed to a direct async read in mdreadv). `mdfd`
//! (FileGetRawDesc for cross-process AIO) is gone with the AIO machinery.
//! `DropRelationFiles` depends on the buffer manager / xlog and is deferred to
//! its consumers (TODO(bufmgr)).

pub use crate::backend::storage::smgr::md::{
    forget_database_sync_requests, mdclose, mdcreate, mdexists, mdextend, mdfiletagmatches,
    mdimmedsync, mdmaxcombine, mdnblocks, mdopen, mdprefetch, mdreadv, mdregistersync,
    mdsyncfiletag, mdtruncate, mdunlink, mdunlinkfiletag, mdwriteback, mdwritev, mdzeroextend,
    MdfdVec,
};

/// mdinit() -- no per-task init under the async model (was a MemoryContext).
pub fn mdinit() {}

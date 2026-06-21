//! smgr.rs
//!   public interface routines to storage manager switch
//! Translated 1:1 from postgres/src/backend/storage/smgr/smgr.c
//
//-------------------------------------------------------------------------
//
// smgr.c
//	  public interface routines to storage manager switch.
//
// All file system operations on relations dispatch through these routines.
// An SMgrRelation represents physical on-disk relation files that are open
// for reading and writing.
//
// When a relation is first accessed through the relation cache, the
// corresponding SMgrRelation entry is opened by calling smgropen(), and the
// reference is stored in the relation cache entry.
//
// Accesses that don't go through the relation cache open the SMgrRelation
// directly.  That includes flushing buffers from the buffer cache, as well as
// all accesses in auxiliary processes like the checkpointer or the WAL redo
// in the startup process.
//
// Operations like CREATE, DROP, ALTER TABLE also hold SMgrRelation references
// independent of the relation cache.  They need to prepare the physical files
// before updating the relation cache.
//
// There is a hash table that holds all the SMgrRelation entries in the
// backend.  If you call smgropen() twice for the same rel locator, you get a
// reference to the same SMgrRelation. The reference is valid until the end of
// transaction.  This makes repeated access to the same relation efficient,
// and allows caching things like the relation size in the SMgrRelation entry.
//
// At end of transaction, all SMgrRelation entries that haven't been pinned
// are removed.  An SMgrRelation can hold kernel file system descriptors for
// the underlying files, and we'd like to close those reasonably soon if the
// file gets deleted.  The SMgrRelations references held by the relcache are
// pinned to prevent them from being closed.
//
// There is another mechanism to close file descriptors early:
// PROCSIGNAL_BARRIER_SMGRRELEASE.  It is a request to immediately close all
// file descriptors.  Upon receiving that signal, the backend closes all file
// descriptors held open by SMgrRelations, but because it can happen in the
// middle of a transaction, we cannot destroy the SMgrRelation objects
// themselves, as there could pointers to them in active use.  See
// smgrrelease() and smgrreleaseall().
//
// NB: We need to hold interrupts across most of the functions in this file,
// as otherwise interrupt processing, e.g. due to a < ERROR elog/ereport, can
// trigger procsignal processing, which in turn can trigger
// smgrreleaseall(). Most of the relevant code is not reentrant.  It seems
// better to put the HOLD_INTERRUPTS()/RESUME_INTERRUPTS() here, instead of
// trying to push them down to md.c where possible: For one, every smgr
// implementation would be vulnerable, for another, a good bit of smgr.c code
// itself is affected too.  Eventually we might want a more targeted solution,
// allowing e.g. a networked smgr implementation to be interrupted, but many
// other, more complicated, problems would need to be fixed for that to be
// viable (e.g. smgr.c is often called with interrupts already held).
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
//
//
// IDENTIFICATION
//	  src/backend/storage/smgr/smgr.c
//
//-------------------------------------------------------------------------

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use crate::prelude::*;
use crate::{dlist_container, dlist_foreach_modify};

// #include "access/xlogutils.h"
// #include "lib/ilist.h"
// #include "miscadmin.h"
// #include "storage/aio.h"
// #include "storage/bufmgr.h"
// #include "storage/ipc.h"
// #include "storage/md.h"
// #include "storage/smgr.h"
// #include "utils/hsearch.h"
// #include "utils/inval.h"

use crate::lib::ilist::{
    dlist_delete, dlist_head, dlist_init, dlist_mutable_iter, dlist_node, dlist_push_tail,
};
use crate::miscadmin::{HOLD_INTERRUPTS, INTERRUPTS_CAN_BE_PROCESSED, RESUME_INTERRUPTS};
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::ipc::ipc::on_proc_exit;
use crate::storage::procnumber::{ProcNumber, MyProcNumber, INVALID_PROC_NUMBER};
use crate::storage::relfilelocator::{
    RelFileLocator, RelFileLocatorBackend, RelFileLocatorBackendIsTemp,
};
use crate::common::relpath::{ForkNumber, RelPathStr, RelFileNumber, MAX_FORKNUM};
use crate::utils::hash::dynahash::{
    hash_create, hash_search, hash_seq_init, hash_seq_search, HASHCTL, HASH_BLOBS, HASH_ELEM,
    HASH_ENTER, HASH_FIND, HASH_REMOVE, HASH_SEQ_STATUS, HTAB,
};

// ----------------------------------------------------------------------------
// TODO(pg-port) stubs: dependencies not yet ported into src/.
// ----------------------------------------------------------------------------

// TODO(pg-port): real RelFileNumberIsValid lives in
// postgres/src/include/storage/relfilelocator.h.  A RelFileNumber is valid if
// it is not InvalidRelFileNumber (0).
const InvalidRelFileNumber: RelFileNumber = 0;
#[inline]
unsafe fn RelFileNumberIsValid(relnumber: RelFileNumber) -> bool {
    relnumber != InvalidRelFileNumber
}

// TODO(pg-port): real InRecovery lives in access/xlog.c (xlog.h).  True while
// the startup process is replaying WAL.
static mut InRecovery: bool = false;

// TODO(pg-port): real psprintf lives in utils/mmgr/mcxt.c (psprintf.h); it
// formats into a palloc'd string.  The gettext _() marker lives in c.h.
// TODO(pg-port): real psprintf lives in utils/mmgr/mcxt.c.
unsafe fn psprintf(_fmt: *const c_char) -> *mut c_char {
    unimplemented!("psprintf not yet ported")
}

// TODO(pg-port): real pg_unreachable lives in c.h.
#[inline]
unsafe fn pg_unreachable() {
    std::hint::unreachable_unchecked()
}

// TODO(pg-port): real relpathbackend lives in common/relpath.c (relpath.h);
// it builds the on-disk path for a fork of a relation.
unsafe fn relpathbackend(
    _rlocator: RelFileLocator,
    _backend: ProcNumber,
    _forknum: ForkNumber,
) -> RelPathStr {
    unimplemented!("relpathbackend not yet ported")
}

// TODO(pg-port): real _MdfdVec lives in storage/md.c; SMgrRelationData holds
// per-fork arrays of pointers to it.  Opaque here.
#[repr(C)]
pub struct _MdfdVec {
    _private: [u8; 0],
}

// TODO(pg-port): real AIO types live in storage/aio_types.h and storage/aio.h.
#[repr(C)]
pub struct PgAioHandle {
    _private: [u8; 0],
}

// TODO(pg-port): real PgAioTargetData (a union) lives in storage/aio_types.h.
// The smgr arm holds the relfilelocator and IO descriptor for an smgr target.
#[repr(C)]
pub struct PgAioTargetDataSmgr {
    pub rlocator: RelFileLocator,
    pub blockNum: BlockNumber,
    pub nblocks: BlockNumber,
    pub forkNum: ForkNumber,
    pub is_temp: bool,
    pub skip_fsync: bool,
}
#[repr(C)]
pub struct PgAioTargetData {
    pub smgr: PgAioTargetDataSmgr,
}

// TODO(pg-port): real PgAioOpData (a union) lives in storage/aio_types.h.
#[repr(C)]
pub struct PgAioOpDataReadWrite {
    pub fd: c_int,
    pub iov_length: u16,
    pub total_size: u32,
    pub offset: u64,
}
#[repr(C)]
pub struct PgAioOpData {
    pub read: PgAioOpDataReadWrite,
    pub write: PgAioOpDataReadWrite,
}

// TODO(pg-port): real PgAioTargetInfo lives in storage/aio.h; it describes the
// callbacks for an AIO target type.
#[repr(C)]
pub struct PgAioTargetInfo {
    pub name: *const c_char,
    pub reopen: Option<unsafe fn(ioh: *mut PgAioHandle)>,
    pub describe_identity: Option<unsafe fn(sd: *const PgAioTargetData) -> *mut c_char>,
}
unsafe impl Sync for PgAioTargetInfo {}

// TODO(pg-port): real AIO op enum lives in storage/aio_types.h.
const PGAIO_OP_INVALID: c_int = 0;
const PGAIO_OP_READV: c_int = 1;
const PGAIO_OP_WRITEV: c_int = 2;

// TODO(pg-port): real AIO target id enum lives in storage/aio_types.h.
const PGAIO_TID_SMGR: c_int = 0;

// TODO(pg-port): real pgaio_io_* helpers live in storage/aio.c (storage/aio.h).
unsafe fn pgaio_io_get_target_data(_ioh: *mut PgAioHandle) -> *mut PgAioTargetData {
    crate::storage::aio::aio_target::pgaio_io_get_target_data(_ioh as _) as _
}
unsafe fn pgaio_io_get_op_data(_ioh: *mut PgAioHandle) -> *mut PgAioOpData {
    crate::storage::aio::aio_io::pgaio_io_get_op_data(_ioh as _) as _
}
unsafe fn pgaio_io_set_target(_ioh: *mut PgAioHandle, _targetid: c_int) {
    unimplemented!("pgaio_io_set_target not yet ported")
}
unsafe fn pgaio_io_get_owner(_ioh: *mut PgAioHandle) -> ProcNumber {
    crate::storage::aio::aio::pgaio_io_get_owner(_ioh as _) as _
}
unsafe fn pgaio_io_get_op(_ioh: *mut PgAioHandle) -> c_int {
    crate::storage::aio::aio_io::pgaio_io_get_op(_ioh as _) as _
}

// TODO(pg-port): real buffer-manager helpers live in storage/buffer/bufmgr.c
// (storage/bufmgr.h).
unsafe fn FlushRelationsAllBuffers(_smgrs: *mut SMgrRelation, _nrels: c_int) {
    crate::storage::buffer::bufmgr::FlushRelationsAllBuffers(_smgrs as _, _nrels)
}
unsafe fn DropRelationsAllBuffers(_smgr_reln: *mut SMgrRelation, _nlocators: c_int) {
    crate::storage::buffer::bufmgr::DropRelationsAllBuffers(_smgr_reln as _, _nlocators)
}
unsafe fn DropRelationBuffers(
    _smgr_reln: SMgrRelation,
    _forknum: *mut ForkNumber,
    _nforks: c_int,
    _firstdelblock: *mut BlockNumber,
) {
    crate::storage::buffer::bufmgr::DropRelationBuffers(_smgr_reln as _, _forknum as _, _nforks, _firstdelblock as _)
}

// TODO(pg-port): real CacheInvalidateSmgr lives in utils/cache/inval.c
// (utils/inval.h).
unsafe fn CacheInvalidateSmgr(_rlocator: RelFileLocatorBackend) {
    crate::utils::cache::inval::CacheInvalidateSmgr(_rlocator)
}

// TODO(pg-port): the smgrsw[] dispatch table references md.c functions.  md.c
// is not yet ported, so these are local stubs that unimplemented!().  Real
// implementations live in storage/smgr/md.c (storage/md.h).
unsafe fn mdinit() { crate::storage::smgr::md::mdinit() }
unsafe fn mdopen(_reln: SMgrRelation) { crate::storage::smgr::md::mdopen(_reln) }
unsafe fn mdclose(_reln: SMgrRelation, _forknum: ForkNumber) { crate::storage::smgr::md::mdclose(_reln, _forknum) }
unsafe fn mdcreate(_reln: SMgrRelation, _forknum: ForkNumber, _isRedo: bool) { crate::storage::smgr::md::mdcreate(_reln, _forknum, _isRedo) }
unsafe fn mdexists(_reln: SMgrRelation, _forknum: ForkNumber) -> bool { crate::storage::smgr::md::mdexists(_reln, _forknum) }
unsafe fn mdunlink(_rlocator: RelFileLocatorBackend, _forknum: ForkNumber, _isRedo: bool) { crate::storage::smgr::md::mdunlink(_rlocator, _forknum, _isRedo) }
unsafe fn mdextend(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffer: *const c_void,
    _skipFsync: bool,
) { crate::storage::smgr::md::mdextend(_reln, _forknum, _blocknum, _buffer, _skipFsync) }
unsafe fn mdzeroextend(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: c_int,
    _skipFsync: bool,
) { crate::storage::smgr::md::mdzeroextend(_reln, _forknum, _blocknum, _nblocks, _skipFsync) }
unsafe fn mdprefetch(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: c_int,
) -> bool { crate::storage::smgr::md::mdprefetch(_reln, _forknum, _blocknum, _nblocks) }
unsafe fn mdmaxcombine(_reln: SMgrRelation, _forknum: ForkNumber, _blocknum: BlockNumber) -> uint32 { crate::storage::smgr::md::mdmaxcombine(_reln, _forknum, _blocknum) }
unsafe fn mdreadv(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffers: *mut *mut c_void,
    _nblocks: BlockNumber,
) { crate::storage::smgr::md::mdreadv(_reln, _forknum, _blocknum, _buffers, _nblocks) }
unsafe fn mdstartreadv(
    _ioh: *mut PgAioHandle,
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffers: *mut *mut c_void,
    _nblocks: BlockNumber,
) { crate::storage::smgr::md::mdstartreadv(_ioh as _, _reln, _forknum, _blocknum, _buffers, _nblocks) }
unsafe fn mdwritev(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffers: *mut *const c_void,
    _nblocks: BlockNumber,
    _skipFsync: bool,
) { crate::storage::smgr::md::mdwritev(_reln, _forknum, _blocknum, _buffers, _nblocks, _skipFsync) }
unsafe fn mdwriteback(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: BlockNumber,
) { crate::storage::smgr::md::mdwriteback(_reln, _forknum, _blocknum, _nblocks) }
unsafe fn mdnblocks(_reln: SMgrRelation, _forknum: ForkNumber) -> BlockNumber { crate::storage::smgr::md::mdnblocks(_reln, _forknum) }
unsafe fn mdtruncate(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _old_blocks: BlockNumber,
    _nblocks: BlockNumber,
) { crate::storage::smgr::md::mdtruncate(_reln, _forknum, _old_blocks, _nblocks) }
unsafe fn mdimmedsync(_reln: SMgrRelation, _forknum: ForkNumber) { crate::storage::smgr::md::mdimmedsync(_reln, _forknum) }
unsafe fn mdregistersync(_reln: SMgrRelation, _forknum: ForkNumber) { crate::storage::smgr::md::mdregistersync(_reln, _forknum) }
unsafe fn mdfd(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _off: *mut uint32,
) -> c_int { crate::storage::smgr::md::mdfd(_reln, _forknum, _blocknum, _off) }

// ----------------------------------------------------------------------------
// smgr.h declarations (merged in, as Rust has no headers)
// ----------------------------------------------------------------------------

// smgr.c maintains a table of SMgrRelation objects, which are essentially
// cached file handles.  An SMgrRelation is created (if not already present)
// by smgropen(), and destroyed by smgrdestroy().  Note that neither of these
// operations imply I/O, they just create or destroy a hashtable entry.  (But
// smgrdestroy() may release associated resources, such as OS-level file
// descriptors.)
//
// An SMgrRelation may be "pinned", to prevent it from being destroyed while
// it's in use.  We use this to prevent pointers in relcache to smgr from being
// invalidated.  SMgrRelations that are not pinned are deleted at end of
// transaction.
#[repr(C)]
pub struct SMgrRelationData {
    /* rlocator is the hashtable lookup key, so it must be first! */
    pub smgr_rlocator: RelFileLocatorBackend, /* relation physical identifier */

    /*
     * The following fields are reset to InvalidBlockNumber upon a cache flush
     * event, and hold the last known size for each fork.  This information is
     * currently only reliable during recovery, since there is no cache
     * invalidation for fork extension.
     */
    pub smgr_targblock: BlockNumber, /* current insertion target block */
    pub smgr_cached_nblocks: [BlockNumber; (MAX_FORKNUM + 1) as usize], /* last known size */

    /* additional public fields may someday exist here */

    /*
     * Fields below here are intended to be private to smgr.c and its
     * submodules.  Do not touch them from elsewhere.
     */
    pub smgr_which: c_int, /* storage manager selector */

    /*
     * for md.c; per-fork arrays of the number of open segments
     * (md_num_open_segs) and the segments themselves (md_seg_fds).
     */
    pub md_num_open_segs: [c_int; (MAX_FORKNUM + 1) as usize],
    pub md_seg_fds: [*mut _MdfdVec; (MAX_FORKNUM + 1) as usize],

    /*
     * Pinning support.  If unpinned (ie. pincount == 0), 'node' is a list
     * link in list of all unpinned SMgrRelations.
     */
    pub pincount: c_int,
    pub node: dlist_node,
}

pub type SMgrRelation = *mut SMgrRelationData;

#[inline]
pub unsafe fn SmgrIsTemp(smgr: SMgrRelation) -> bool {
    RelFileLocatorBackendIsTemp(&(*smgr).smgr_rlocator)
}

// ----------------------------------------------------------------------------

// This struct of function pointers defines the API between smgr.c and
// any individual storage manager module.  Note that smgr subfunctions are
// generally expected to report problems via elog(ERROR).  An exception is
// that smgr_unlink should use elog(WARNING), rather than erroring out,
// because we normally unlink relations during post-commit/abort cleanup,
// and so it's too late to raise an error.  Also, various conditions that
// would normally be errors should be allowed during bootstrap and/or WAL
// recovery --- see comments in md.c for details.
#[repr(C)]
pub struct f_smgr {
    pub smgr_init: Option<unsafe fn()>,     /* may be NULL */
    pub smgr_shutdown: Option<unsafe fn()>, /* may be NULL */
    pub smgr_open: Option<unsafe fn(reln: SMgrRelation)>,
    pub smgr_close: Option<unsafe fn(reln: SMgrRelation, forknum: ForkNumber)>,
    pub smgr_create: Option<unsafe fn(reln: SMgrRelation, forknum: ForkNumber, isRedo: bool)>,
    pub smgr_exists: Option<unsafe fn(reln: SMgrRelation, forknum: ForkNumber) -> bool>,
    pub smgr_unlink:
        Option<unsafe fn(rlocator: RelFileLocatorBackend, forknum: ForkNumber, isRedo: bool)>,
    pub smgr_extend: Option<
        unsafe fn(
            reln: SMgrRelation,
            forknum: ForkNumber,
            blocknum: BlockNumber,
            buffer: *const c_void,
            skipFsync: bool,
        ),
    >,
    pub smgr_zeroextend: Option<
        unsafe fn(
            reln: SMgrRelation,
            forknum: ForkNumber,
            blocknum: BlockNumber,
            nblocks: c_int,
            skipFsync: bool,
        ),
    >,
    pub smgr_prefetch: Option<
        unsafe fn(
            reln: SMgrRelation,
            forknum: ForkNumber,
            blocknum: BlockNumber,
            nblocks: c_int,
        ) -> bool,
    >,
    pub smgr_maxcombine:
        Option<unsafe fn(reln: SMgrRelation, forknum: ForkNumber, blocknum: BlockNumber) -> uint32>,
    pub smgr_readv: Option<
        unsafe fn(
            reln: SMgrRelation,
            forknum: ForkNumber,
            blocknum: BlockNumber,
            buffers: *mut *mut c_void,
            nblocks: BlockNumber,
        ),
    >,
    pub smgr_startreadv: Option<
        unsafe fn(
            ioh: *mut PgAioHandle,
            reln: SMgrRelation,
            forknum: ForkNumber,
            blocknum: BlockNumber,
            buffers: *mut *mut c_void,
            nblocks: BlockNumber,
        ),
    >,
    pub smgr_writev: Option<
        unsafe fn(
            reln: SMgrRelation,
            forknum: ForkNumber,
            blocknum: BlockNumber,
            buffers: *mut *const c_void,
            nblocks: BlockNumber,
            skipFsync: bool,
        ),
    >,
    pub smgr_writeback: Option<
        unsafe fn(
            reln: SMgrRelation,
            forknum: ForkNumber,
            blocknum: BlockNumber,
            nblocks: BlockNumber,
        ),
    >,
    pub smgr_nblocks: Option<unsafe fn(reln: SMgrRelation, forknum: ForkNumber) -> BlockNumber>,
    pub smgr_truncate: Option<
        unsafe fn(
            reln: SMgrRelation,
            forknum: ForkNumber,
            old_blocks: BlockNumber,
            nblocks: BlockNumber,
        ),
    >,
    pub smgr_immedsync: Option<unsafe fn(reln: SMgrRelation, forknum: ForkNumber)>,
    pub smgr_registersync: Option<unsafe fn(reln: SMgrRelation, forknum: ForkNumber)>,
    pub smgr_fd: Option<
        unsafe fn(
            reln: SMgrRelation,
            forknum: ForkNumber,
            blocknum: BlockNumber,
            off: *mut uint32,
        ) -> c_int,
    >,
}

unsafe impl Sync for f_smgr {}

static smgrsw: [f_smgr; 1] = [
    /* magnetic disk */
    f_smgr {
        smgr_init: Some(mdinit),
        smgr_shutdown: None,
        smgr_open: Some(mdopen),
        smgr_close: Some(mdclose),
        smgr_create: Some(mdcreate),
        smgr_exists: Some(mdexists),
        smgr_unlink: Some(mdunlink),
        smgr_extend: Some(mdextend),
        smgr_zeroextend: Some(mdzeroextend),
        smgr_prefetch: Some(mdprefetch),
        smgr_maxcombine: Some(mdmaxcombine),
        smgr_readv: Some(mdreadv),
        smgr_startreadv: Some(mdstartreadv),
        smgr_writev: Some(mdwritev),
        smgr_writeback: Some(mdwriteback),
        smgr_nblocks: Some(mdnblocks),
        smgr_truncate: Some(mdtruncate),
        smgr_immedsync: Some(mdimmedsync),
        smgr_registersync: Some(mdregistersync),
        smgr_fd: Some(mdfd),
    },
];

static NSmgr: c_int = lengthof!(smgrsw) as c_int;

// Each backend has a hashtable that stores all extant SMgrRelation objects.
// In addition, "unpinned" SMgrRelation objects are chained together in a list.
static mut SMgrRelationHash: *mut HTAB = null_mut();

static mut unpinned_relns: dlist_head = dlist_head {
    head: dlist_node {
        prev: null_mut(),
        next: null_mut(),
    },
};

// local function prototypes
// (static smgrshutdown, smgrdestroy, smgr_aio_reopen, smgr_aio_describe_identity below)

pub static aio_smgr_target_info: PgAioTargetInfo = PgAioTargetInfo {
    name: c"smgr".as_ptr(),
    reopen: Some(smgr_aio_reopen),
    describe_identity: Some(smgr_aio_describe_identity),
};

// smgrinit(), smgrshutdown() -- Initialize or shut down storage
//								 managers.
//
// Note: smgrinit is called during backend startup (normal or standalone
// case), *not* during postmaster start.  Therefore, any resources created
// here or destroyed in smgrshutdown are backend-local.
pub unsafe fn smgrinit() {
    let mut i: c_int;

    HOLD_INTERRUPTS();

    i = 0;
    while i < NSmgr {
        if let Some(smgr_init) = smgrsw[i as usize].smgr_init {
            smgr_init();
        }
        i += 1;
    }

    RESUME_INTERRUPTS();

    /* register the shutdown proc */
    on_proc_exit(smgrshutdown, 0 as Datum);
}

// on_proc_exit hook for smgr cleanup during backend shutdown
unsafe extern "C" fn smgrshutdown(_code: c_int, _arg: Datum) {
    let mut i: c_int;

    HOLD_INTERRUPTS();

    i = 0;
    while i < NSmgr {
        if let Some(smgr_shutdown) = smgrsw[i as usize].smgr_shutdown {
            smgr_shutdown();
        }
        i += 1;
    }

    RESUME_INTERRUPTS();
}

// smgropen() -- Return an SMgrRelation object, creating it if need be.
//
// In versions of PostgreSQL prior to 17, this function returned an object
// with no defined lifetime.  Now, however, the object remains valid for the
// lifetime of the transaction, up to the point where AtEOXact_SMgr() is
// called, making it much easier for callers to know for how long they can
// hold on to a pointer to the returned object.  If this function is called
// outside of a transaction, the object remains valid until smgrdestroy() or
// smgrdestroyall() is called.  Background processes that use smgr but not
// transactions typically do this once per checkpoint cycle.
//
// This does not attempt to actually open the underlying files.
pub unsafe fn smgropen(rlocator: RelFileLocator, backend: ProcNumber) -> SMgrRelation {
    let mut brlocator: RelFileLocatorBackend = std::mem::zeroed();
    let reln: SMgrRelation;
    let mut found: bool = false;

    Assert!(RelFileNumberIsValid(rlocator.relNumber));

    HOLD_INTERRUPTS();

    if SMgrRelationHash.is_null() {
        /* First time through: initialize the hash table */
        let mut ctl: HASHCTL = std::mem::zeroed();

        ctl.keysize = std::mem::size_of::<RelFileLocatorBackend>();
        ctl.entrysize = std::mem::size_of::<SMgrRelationData>();
        SMgrRelationHash = hash_create(
            c"smgr relation table".as_ptr(),
            400,
            &ctl,
            HASH_ELEM | HASH_BLOBS,
        );
        dlist_init(&raw mut unpinned_relns);
    }

    /* Look up or create an entry */
    brlocator.locator = rlocator;
    brlocator.backend = backend;
    reln = hash_search(
        SMgrRelationHash,
        &brlocator as *const RelFileLocatorBackend as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as SMgrRelation;

    /* Initialize it if not present before */
    if !found {
        /* hash_search already filled in the lookup key */
        (*reln).smgr_targblock = InvalidBlockNumber;
        let mut i: c_int = 0;
        while i <= MAX_FORKNUM {
            (*reln).smgr_cached_nblocks[i as usize] = InvalidBlockNumber;
            i += 1;
        }
        (*reln).smgr_which = 0; /* we only have md.c at present */

        /* it is not pinned yet */
        (*reln).pincount = 0;
        dlist_push_tail(&raw mut unpinned_relns, &raw mut (*reln).node);

        /* implementation-specific initialization */
        (smgrsw[(*reln).smgr_which as usize].smgr_open.unwrap())(reln);
    }

    RESUME_INTERRUPTS();

    reln
}

// smgrpin() -- Prevent an SMgrRelation object from being destroyed at end of
//				transaction
pub unsafe fn smgrpin(reln: SMgrRelation) {
    if (*reln).pincount == 0 {
        dlist_delete(&raw mut (*reln).node);
    }
    (*reln).pincount += 1;
}

// smgrunpin() -- Allow an SMgrRelation object to be destroyed at end of
//				  transaction
//
// The object remains valid, but if there are no other pins on it, it is moved
// to the unpinned list where it will be destroyed by AtEOXact_SMgr().
pub unsafe fn smgrunpin(reln: SMgrRelation) {
    Assert!((*reln).pincount > 0);
    (*reln).pincount -= 1;
    if (*reln).pincount == 0 {
        dlist_push_tail(&raw mut unpinned_relns, &raw mut (*reln).node);
    }
}

// smgrdestroy() -- Delete an SMgrRelation object.
unsafe fn smgrdestroy(reln: SMgrRelation) {
    let mut forknum: ForkNumber;

    Assert!((*reln).pincount == 0);

    HOLD_INTERRUPTS();

    forknum = 0;
    while forknum <= MAX_FORKNUM {
        (smgrsw[(*reln).smgr_which as usize].smgr_close.unwrap())(reln, forknum);
        forknum += 1;
    }

    dlist_delete(&raw mut (*reln).node);

    if hash_search(
        SMgrRelationHash,
        &raw const (*reln).smgr_rlocator as *const c_void,
        HASH_REMOVE,
        null_mut(),
    )
    .is_null()
    {
        elog!(ERROR, "SMgrRelation hashtable corrupted");
    }

    RESUME_INTERRUPTS();
}

// smgrrelease() -- Release all resources used by this object.
//
// The object remains valid.
pub unsafe fn smgrrelease(reln: SMgrRelation) {
    HOLD_INTERRUPTS();

    let mut forknum: ForkNumber = 0;
    while forknum <= MAX_FORKNUM {
        (smgrsw[(*reln).smgr_which as usize].smgr_close.unwrap())(reln, forknum);
        (*reln).smgr_cached_nblocks[forknum as usize] = InvalidBlockNumber;
        forknum += 1;
    }
    (*reln).smgr_targblock = InvalidBlockNumber;

    RESUME_INTERRUPTS();
}

// smgrclose() -- Close an SMgrRelation object.
//
// The SMgrRelation reference should not be used after this call.  However,
// because we don't keep track of the references returned by smgropen(), we
// don't know if there are other references still pointing to the same object,
// so we cannot remove the SMgrRelation object yet.  Therefore, this is just a
// synonym for smgrrelease() at the moment.
pub unsafe fn smgrclose(reln: SMgrRelation) {
    smgrrelease(reln);
}

// smgrdestroyall() -- Release resources used by all unpinned objects.
//
// It must be known that there are no pointers to SMgrRelations, other than
// those pinned with smgrpin().
pub unsafe fn smgrdestroyall() {
    let mut iter: dlist_mutable_iter = std::mem::zeroed();

    /* seems unsafe to accept interrupts while in a dlist_foreach_modify() */
    HOLD_INTERRUPTS();

    /*
     * Zap all unpinned SMgrRelations.  We rely on smgrdestroy() to remove
     * each one from the list.
     */
    dlist_foreach_modify!(iter, &raw mut unpinned_relns, {
        let rel: SMgrRelation = dlist_container!(SMgrRelationData, node, iter.cur);

        smgrdestroy(rel);
    });

    RESUME_INTERRUPTS();
}

// smgrreleaseall() -- Release resources used by all objects.
pub unsafe fn smgrreleaseall() {
    let mut status: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut reln: SMgrRelation;

    /* Nothing to do if hashtable not set up */
    if SMgrRelationHash.is_null() {
        return;
    }

    /* seems unsafe to accept interrupts while iterating */
    HOLD_INTERRUPTS();

    hash_seq_init(&mut status, SMgrRelationHash);

    loop {
        reln = hash_seq_search(&mut status) as SMgrRelation;
        if reln.is_null() {
            break;
        }
        smgrrelease(reln);
    }

    RESUME_INTERRUPTS();
}

// smgrreleaserellocator() -- Release resources for given RelFileLocator, if
//							  it's open.
//
// This has the same effects as smgrrelease(smgropen(rlocator)), but avoids
// uselessly creating a hashtable entry only to drop it again when no
// such entry exists already.
pub unsafe fn smgrreleaserellocator(rlocator: RelFileLocatorBackend) {
    let reln: SMgrRelation;

    /* Nothing to do if hashtable not set up */
    if SMgrRelationHash.is_null() {
        return;
    }

    reln = hash_search(
        SMgrRelationHash,
        &rlocator as *const RelFileLocatorBackend as *const c_void,
        HASH_FIND,
        null_mut(),
    ) as SMgrRelation;
    if !reln.is_null() {
        smgrrelease(reln);
    }
}

// smgrexists() -- Does the underlying file for a fork exist?
pub unsafe fn smgrexists(reln: SMgrRelation, forknum: ForkNumber) -> bool {
    let ret: bool;

    HOLD_INTERRUPTS();
    ret = (smgrsw[(*reln).smgr_which as usize].smgr_exists.unwrap())(reln, forknum);
    RESUME_INTERRUPTS();

    ret
}

// smgrcreate() -- Create a new relation.
//
// Given an already-created (but presumably unused) SMgrRelation,
// cause the underlying disk file or other storage for the fork
// to be created.
pub unsafe fn smgrcreate(reln: SMgrRelation, forknum: ForkNumber, isRedo: bool) {
    HOLD_INTERRUPTS();
    (smgrsw[(*reln).smgr_which as usize].smgr_create.unwrap())(reln, forknum, isRedo);
    RESUME_INTERRUPTS();
}

// smgrdosyncall() -- Immediately sync all forks of all given relations
//
// All forks of all given relations are synced out to the store.
//
// This is equivalent to FlushRelationBuffers() for each smgr relation,
// then calling smgrimmedsync() for all forks of each relation, but it's
// significantly quicker so should be preferred when possible.
pub unsafe fn smgrdosyncall(rels: *mut SMgrRelation, nrels: c_int) {
    let mut i: c_int = 0;
    let mut forknum: ForkNumber;

    if nrels == 0 {
        return;
    }

    FlushRelationsAllBuffers(rels, nrels);

    HOLD_INTERRUPTS();

    /*
     * Sync the physical file(s).
     */
    i = 0;
    while i < nrels {
        let which: c_int = (**rels.add(i as usize)).smgr_which;

        forknum = 0;
        while forknum <= MAX_FORKNUM {
            if (smgrsw[which as usize].smgr_exists.unwrap())(*rels.add(i as usize), forknum) {
                (smgrsw[which as usize].smgr_immedsync.unwrap())(*rels.add(i as usize), forknum);
            }
            forknum += 1;
        }
        i += 1;
    }

    RESUME_INTERRUPTS();
}

// smgrdounlinkall() -- Immediately unlink all forks of all given relations
//
// All forks of all given relations are removed from the store.  This
// should not be used during transactional operations, since it can't be
// undone.
//
// If isRedo is true, it is okay for the underlying file(s) to be gone
// already.
pub unsafe fn smgrdounlinkall(rels: *mut SMgrRelation, nrels: c_int, isRedo: bool) {
    let mut i: c_int = 0;
    let rlocators: *mut RelFileLocatorBackend;
    let mut forknum: ForkNumber;

    if nrels == 0 {
        return;
    }

    /*
     * It would be unsafe to process interrupts between DropRelationBuffers()
     * and unlinking the underlying files. This probably should be a critical
     * section, but we're not there yet.
     */
    HOLD_INTERRUPTS();

    /*
     * Get rid of any remaining buffers for the relations.  bufmgr will just
     * drop them without bothering to write the contents.
     */
    DropRelationsAllBuffers(rels, nrels);

    /*
     * create an array which contains all relations to be dropped, and close
     * each relation's forks at the smgr level while at it
     */
    rlocators =
        palloc(std::mem::size_of::<RelFileLocatorBackend>() * nrels as usize) as *mut RelFileLocatorBackend;
    i = 0;
    while i < nrels {
        let rlocator: RelFileLocatorBackend = (**rels.add(i as usize)).smgr_rlocator;
        let which: c_int = (**rels.add(i as usize)).smgr_which;

        *rlocators.add(i as usize) = rlocator;

        /* Close the forks at smgr level */
        forknum = 0;
        while forknum <= MAX_FORKNUM {
            (smgrsw[which as usize].smgr_close.unwrap())(*rels.add(i as usize), forknum);
            forknum += 1;
        }
        i += 1;
    }

    /*
     * Send a shared-inval message to force other backends to close any
     * dangling smgr references they may have for these rels.  We should do
     * this before starting the actual unlinking, in case we fail partway
     * through that step.  Note that the sinval messages will eventually come
     * back to this backend, too, and thereby provide a backstop that we
     * closed our own smgr rel.
     */
    i = 0;
    while i < nrels {
        CacheInvalidateSmgr(*rlocators.add(i as usize));
        i += 1;
    }

    /*
     * Delete the physical file(s).
     *
     * Note: smgr_unlink must treat deletion failure as a WARNING, not an
     * ERROR, because we've already decided to commit or abort the current
     * xact.
     */

    i = 0;
    while i < nrels {
        let which: c_int = (**rels.add(i as usize)).smgr_which;

        forknum = 0;
        while forknum <= MAX_FORKNUM {
            (smgrsw[which as usize].smgr_unlink.unwrap())(*rlocators.add(i as usize), forknum, isRedo);
            forknum += 1;
        }
        i += 1;
    }

    pfree(rlocators as *mut c_void);

    RESUME_INTERRUPTS();
}

// smgrextend() -- Add a new block to a file.
//
// The semantics are nearly the same as smgrwrite(): write at the
// specified position.  However, this is to be used for the case of
// extending a relation (i.e., blocknum is at or beyond the current
// EOF).  Note that we assume writing a block beyond current EOF
// causes intervening file space to become filled with zeroes.
pub unsafe fn smgrextend(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: *const c_void,
    skipFsync: bool,
) {
    HOLD_INTERRUPTS();

    (smgrsw[(*reln).smgr_which as usize].smgr_extend.unwrap())(
        reln, forknum, blocknum, buffer, skipFsync,
    );

    /*
     * Normally we expect this to increase nblocks by one, but if the cached
     * value isn't as expected, just invalidate it so the next call asks the
     * kernel.
     */
    if (*reln).smgr_cached_nblocks[forknum as usize] == blocknum {
        (*reln).smgr_cached_nblocks[forknum as usize] = blocknum + 1;
    } else {
        (*reln).smgr_cached_nblocks[forknum as usize] = InvalidBlockNumber;
    }

    RESUME_INTERRUPTS();
}

// smgrzeroextend() -- Add new zeroed out blocks to a file.
//
// Similar to smgrextend(), except the relation can be extended by
// multiple blocks at once and the added blocks will be filled with
// zeroes.
pub unsafe fn smgrzeroextend(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: c_int,
    skipFsync: bool,
) {
    HOLD_INTERRUPTS();

    (smgrsw[(*reln).smgr_which as usize].smgr_zeroextend.unwrap())(
        reln, forknum, blocknum, nblocks, skipFsync,
    );

    /*
     * Normally we expect this to increase the fork size by nblocks, but if
     * the cached value isn't as expected, just invalidate it so the next call
     * asks the kernel.
     */
    if (*reln).smgr_cached_nblocks[forknum as usize] == blocknum {
        (*reln).smgr_cached_nblocks[forknum as usize] = blocknum + nblocks as BlockNumber;
    } else {
        (*reln).smgr_cached_nblocks[forknum as usize] = InvalidBlockNumber;
    }

    RESUME_INTERRUPTS();
}

// smgrprefetch() -- Initiate asynchronous read of the specified block of a relation.
//
// In recovery only, this can return false to indicate that a file
// doesn't exist (presumably it has been dropped by a later WAL
// record).
pub unsafe fn smgrprefetch(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: c_int,
) -> bool {
    let ret: bool;

    HOLD_INTERRUPTS();
    ret = (smgrsw[(*reln).smgr_which as usize].smgr_prefetch.unwrap())(
        reln, forknum, blocknum, nblocks,
    );
    RESUME_INTERRUPTS();

    ret
}

// smgrmaxcombine() - Return the maximum number of total blocks that can be
//				 combined with an IO starting at blocknum.
//
// The returned value includes the IO for blocknum itself.
pub unsafe fn smgrmaxcombine(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
) -> uint32 {
    let ret: uint32;

    HOLD_INTERRUPTS();
    ret = (smgrsw[(*reln).smgr_which as usize].smgr_maxcombine.unwrap())(reln, forknum, blocknum);
    RESUME_INTERRUPTS();

    ret
}

// smgrreadv() -- read a particular block range from a relation into the
//				 supplied buffers.
//
// This routine is called from the buffer manager in order to
// instantiate pages in the shared buffer cache.  All storage managers
// return pages in the format that POSTGRES expects.
//
// If more than one block is intended to be read, callers need to use
// smgrmaxcombine() to check how many blocks can be combined into one IO.
pub unsafe fn smgrreadv(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *mut *mut c_void,
    nblocks: BlockNumber,
) {
    HOLD_INTERRUPTS();
    (smgrsw[(*reln).smgr_which as usize].smgr_readv.unwrap())(
        reln, forknum, blocknum, buffers, nblocks,
    );
    RESUME_INTERRUPTS();
}

// smgrstartreadv() -- asynchronous version of smgrreadv()
//
// This starts an asynchronous readv IO using the IO handle `ioh`. Other than
// `ioh` all parameters are the same as smgrreadv().
//
// Completion callbacks above smgr will be passed the result as the number of
// successfully read blocks if the read [partially] succeeds (Buffers for
// blocks not successfully read might bear unspecified modifications, up to
// the full nblocks). This maintains the abstraction that smgr operates on the
// level of blocks, rather than bytes.
//
// Compared to smgrreadv(), more responsibilities fall on the caller:
// - Partial reads need to be handled by the caller re-issuing IO for the
//   unread blocks
// - smgr will ereport(LOG_SERVER_ONLY) some problems, but higher layers are
//   responsible for pgaio_result_report() to mirror that news to the user (if
//   the IO results in PGAIO_RS_WARNING) or abort the (sub)transaction (if
//   PGAIO_RS_ERROR).
// - Under Valgrind, the "buffers" memory may or may not change status to
//   DEFINED, depending on io_method and concurrent activity.
pub unsafe fn smgrstartreadv(
    ioh: *mut PgAioHandle,
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *mut *mut c_void,
    nblocks: BlockNumber,
) {
    HOLD_INTERRUPTS();
    (smgrsw[(*reln).smgr_which as usize].smgr_startreadv.unwrap())(
        ioh, reln, forknum, blocknum, buffers, nblocks,
    );
    RESUME_INTERRUPTS();
}

// smgrwritev() -- Write the supplied buffers out.
//
// This is to be used only for updating already-existing blocks of a
// relation (ie, those before the current EOF).  To extend a relation,
// use smgrextend().
//
// This is not a synchronous write -- the block is not necessarily
// on disk at return, only dumped out to the kernel.  However,
// provisions will be made to fsync the write before the next checkpoint.
//
// NB: The mechanism to ensure fsync at next checkpoint assumes that there is
// something that prevents a concurrent checkpoint from "racing ahead" of the
// write.  One way to prevent that is by holding a lock on the buffer; the
// buffer manager's writes are protected by that.  The bulk writer facility
// in bulk_write.c checks the redo pointer and calls smgrimmedsync() if a
// checkpoint happened; that relies on the fact that no other backend can be
// concurrently modifying the page.
//
// skipFsync indicates that the caller will make other provisions to
// fsync the relation, so we needn't bother.  Temporary relations also
// do not require fsync.
//
// If more than one block is intended to be read, callers need to use
// smgrmaxcombine() to check how many blocks can be combined into one IO.
pub unsafe fn smgrwritev(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *mut *const c_void,
    nblocks: BlockNumber,
    skipFsync: bool,
) {
    HOLD_INTERRUPTS();
    (smgrsw[(*reln).smgr_which as usize].smgr_writev.unwrap())(
        reln, forknum, blocknum, buffers, nblocks, skipFsync,
    );
    RESUME_INTERRUPTS();
}

// smgrwriteback() -- Trigger kernel writeback for the supplied range of
//					   blocks.
pub unsafe fn smgrwriteback(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: BlockNumber,
) {
    HOLD_INTERRUPTS();
    (smgrsw[(*reln).smgr_which as usize].smgr_writeback.unwrap())(reln, forknum, blocknum, nblocks);
    RESUME_INTERRUPTS();
}

// smgrnblocks() -- Calculate the number of blocks in the
//					supplied relation.
pub unsafe fn smgrnblocks(reln: SMgrRelation, forknum: ForkNumber) -> BlockNumber {
    let mut result: BlockNumber;

    /* Check and return if we get the cached value for the number of blocks. */
    result = smgrnblocks_cached(reln, forknum);
    if result != InvalidBlockNumber {
        return result;
    }

    HOLD_INTERRUPTS();

    result = (smgrsw[(*reln).smgr_which as usize].smgr_nblocks.unwrap())(reln, forknum);

    (*reln).smgr_cached_nblocks[forknum as usize] = result;

    RESUME_INTERRUPTS();

    result
}

// smgrnblocks_cached() -- Get the cached number of blocks in the supplied
//						   relation.
//
// Returns an InvalidBlockNumber when not in recovery and when the relation
// fork size is not cached.
pub unsafe fn smgrnblocks_cached(reln: SMgrRelation, forknum: ForkNumber) -> BlockNumber {
    /*
     * For now, this function uses cached values only in recovery due to lack
     * of a shared invalidation mechanism for changes in file size.  Code
     * elsewhere reads smgr_cached_nblocks and copes with stale data.
     */
    if InRecovery && (*reln).smgr_cached_nblocks[forknum as usize] != InvalidBlockNumber {
        return (*reln).smgr_cached_nblocks[forknum as usize];
    }

    InvalidBlockNumber
}

// smgrtruncate() -- Truncate the given forks of supplied relation to
//					 each specified numbers of blocks
//
// The truncation is done immediately, so this can't be rolled back.
//
// The caller must hold AccessExclusiveLock on the relation, to ensure that
// other backends receive the smgr invalidation event that this function sends
// before they access any forks of the relation again.  The current size of
// the forks should be provided in old_nblocks.  This function should normally
// be called in a critical section, but the current size must be checked
// outside the critical section, and no interrupts or smgr functions relating
// to this relation should be called in between.
pub unsafe fn smgrtruncate(
    reln: SMgrRelation,
    forknum: *mut ForkNumber,
    nforks: c_int,
    old_nblocks: *mut BlockNumber,
    nblocks: *mut BlockNumber,
) {
    let mut i: c_int;

    /*
     * Get rid of any buffers for the about-to-be-deleted blocks. bufmgr will
     * just drop them without bothering to write the contents.
     */
    DropRelationBuffers(reln, forknum, nforks, nblocks);

    /*
     * Send a shared-inval message to force other backends to close any smgr
     * references they may have for this rel.  This is useful because they
     * might have open file pointers to segments that got removed, and/or
     * smgr_targblock variables pointing past the new rel end.  (The inval
     * message will come back to our backend, too, causing a
     * probably-unnecessary local smgr flush.  But we don't expect that this
     * is a performance-critical path.)  As in the unlink code, we want to be
     * sure the message is sent before we start changing things on-disk.
     */
    CacheInvalidateSmgr((*reln).smgr_rlocator);

    /* Do the truncation */
    i = 0;
    while i < nforks {
        /* Make the cached size is invalid if we encounter an error. */
        (*reln).smgr_cached_nblocks[*forknum.add(i as usize) as usize] = InvalidBlockNumber;

        (smgrsw[(*reln).smgr_which as usize].smgr_truncate.unwrap())(
            reln,
            *forknum.add(i as usize),
            *old_nblocks.add(i as usize),
            *nblocks.add(i as usize),
        );

        /*
         * We might as well update the local smgr_cached_nblocks values. The
         * smgr cache inval message that this function sent will cause other
         * backends to invalidate their copies of smgr_cached_nblocks, and
         * these ones too at the next command boundary. But ensure they aren't
         * outright wrong until then.
         *
         * We can have nblocks > old_nblocks when a relation was truncated
         * multiple times, a replica applied all the truncations, and later
         * restarts from a restartpoint located before the truncations. The
         * relation on disk will be the size of the last truncate. When
         * replaying the first truncate, we will have nblocks > current size.
         * In such cases, smgr_truncate does nothing, so set the cached size
         * to the old size rather than the requested size.
         */
        (*reln).smgr_cached_nblocks[*forknum.add(i as usize) as usize] =
            if *nblocks.add(i as usize) > *old_nblocks.add(i as usize) {
                *old_nblocks.add(i as usize)
            } else {
                *nblocks.add(i as usize)
            };
        i += 1;
    }
}

// smgrregistersync() -- Request a relation to be sync'd at next checkpoint
//
// This can be used after calling smgrwrite() or smgrextend() with skipFsync =
// true, to register the fsyncs that were skipped earlier.
//
// Note: be mindful that a checkpoint could already have happened between the
// smgrwrite or smgrextend calls and this!  In that case, the checkpoint
// already missed fsyncing this relation, and you should use smgrimmedsync
// instead.  Most callers should use the bulk loading facility in bulk_write.c
// which handles all that.
pub unsafe fn smgrregistersync(reln: SMgrRelation, forknum: ForkNumber) {
    HOLD_INTERRUPTS();
    (smgrsw[(*reln).smgr_which as usize].smgr_registersync.unwrap())(reln, forknum);
    RESUME_INTERRUPTS();
}

// smgrimmedsync() -- Force the specified relation to stable storage.
//
// Synchronously force all previous writes to the specified relation
// down to disk.
//
// This is useful for building completely new relations (eg, new
// indexes).  Instead of incrementally WAL-logging the index build
// steps, we can just write completed index pages to disk with smgrwrite
// or smgrextend, and then fsync the completed index file before
// committing the transaction.  (This is sufficient for purposes of
// crash recovery, since it effectively duplicates forcing a checkpoint
// for the completed index.  But it is *not* sufficient if one wishes
// to use the WAL log for PITR or replication purposes: in that case
// we have to make WAL entries as well.)
//
// The preceding writes should specify skipFsync = true to avoid
// duplicative fsyncs.
//
// Note that you need to do FlushRelationBuffers() first if there is
// any possibility that there are dirty buffers for the relation;
// otherwise the sync is not very meaningful.
//
// Most callers should use the bulk loading facility in bulk_write.c
// instead of calling this directly.
pub unsafe fn smgrimmedsync(reln: SMgrRelation, forknum: ForkNumber) {
    HOLD_INTERRUPTS();
    (smgrsw[(*reln).smgr_which as usize].smgr_immedsync.unwrap())(reln, forknum);
    RESUME_INTERRUPTS();
}

// Return fd for the specified block number and update *off to the appropriate
// position.
//
// This is only to be used for when AIO needs to perform the IO in a different
// process than where it was issued (e.g. in an IO worker).
unsafe fn smgrfd(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    off: *mut uint32,
) -> c_int {
    let fd: c_int;

    /*
     * The caller needs to prevent interrupts from being processed, otherwise
     * the FD could be closed prematurely.
     */
    Assert!(!INTERRUPTS_CAN_BE_PROCESSED());

    fd = (smgrsw[(*reln).smgr_which as usize].smgr_fd.unwrap())(reln, forknum, blocknum, off);

    fd
}

// AtEOXact_SMgr
//
// This routine is called during transaction commit or abort (it doesn't
// particularly care which).  All unpinned SMgrRelation objects are destroyed.
//
// We do this as a compromise between wanting transient SMgrRelations to
// live awhile (to amortize the costs of blind writes of multiple blocks)
// and needing them to not live forever (since we're probably holding open
// a kernel file descriptor for the underlying file, and we need to ensure
// that gets closed reasonably soon if the file gets deleted).
pub unsafe fn AtEOXact_SMgr() {
    smgrdestroyall();
}

// This routine is called when we are ordered to release all open files by a
// ProcSignalBarrier.
pub unsafe fn ProcessBarrierSmgrRelease() -> bool {
    smgrreleaseall();
    true
}

// Set target of the IO handle to be smgr and initialize all the relevant
// pieces of data.
pub unsafe fn pgaio_io_set_target_smgr(
    ioh: *mut PgAioHandle,
    smgr: *mut SMgrRelationData,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: c_int,
    skip_fsync: bool,
) {
    let sd: *mut PgAioTargetData = pgaio_io_get_target_data(ioh);

    pgaio_io_set_target(ioh, PGAIO_TID_SMGR);

    /* backend is implied via IO owner */
    (*sd).smgr.rlocator = (*smgr).smgr_rlocator.locator;
    (*sd).smgr.forkNum = forknum;
    (*sd).smgr.blockNum = blocknum;
    (*sd).smgr.nblocks = nblocks as BlockNumber;
    (*sd).smgr.is_temp = SmgrIsTemp(smgr);
    /* Temp relations should never be fsync'd */
    (*sd).smgr.skip_fsync = skip_fsync && !SmgrIsTemp(smgr);
}

// Callback for the smgr AIO target, to reopen the file (e.g. because the IO
// is executed in a worker).
unsafe fn smgr_aio_reopen(ioh: *mut PgAioHandle) {
    let sd: *mut PgAioTargetData = pgaio_io_get_target_data(ioh);
    let od: *mut PgAioOpData = pgaio_io_get_op_data(ioh);
    let reln: SMgrRelation;
    let procno: ProcNumber;
    let mut off: uint32 = 0;

    /*
     * The caller needs to prevent interrupts from being processed, otherwise
     * the FD could be closed again before we get to executing the IO.
     */
    Assert!(!INTERRUPTS_CAN_BE_PROCESSED());

    if (*sd).smgr.is_temp {
        procno = pgaio_io_get_owner(ioh);
    } else {
        procno = INVALID_PROC_NUMBER;
    }

    reln = smgropen((*sd).smgr.rlocator, procno);
    match pgaio_io_get_op(ioh) {
        PGAIO_OP_INVALID => {
            pg_unreachable();
        }
        PGAIO_OP_READV => {
            (*od).read.fd = smgrfd(reln, (*sd).smgr.forkNum, (*sd).smgr.blockNum, &mut off);
            Assert!(off == (*od).read.offset as uint32);
        }
        PGAIO_OP_WRITEV => {
            (*od).write.fd = smgrfd(reln, (*sd).smgr.forkNum, (*sd).smgr.blockNum, &mut off);
            Assert!(off == (*od).write.offset as uint32);
        }
        _ => {}
    }
}

// Callback for the smgr AIO target, describing the target of the IO.
unsafe fn smgr_aio_describe_identity(sd: *const PgAioTargetData) -> *mut c_char {
    let path: RelPathStr;
    let desc: *mut c_char;

    path = relpathbackend(
        (*sd).smgr.rlocator,
        if (*sd).smgr.is_temp {
            MyProcNumber
        } else {
            INVALID_PROC_NUMBER
        },
        (*sd).smgr.forkNum,
    );

    if (*sd).smgr.nblocks == 0 {
        // psprintf(_("file \"%s\""), path.str)
        let _ = path.str.as_ptr();
        desc = psprintf(c"file \"%s\"".as_ptr());
    } else if (*sd).smgr.nblocks == 1 {
        // psprintf(_("block %u in file \"%s\""), sd->smgr.blockNum, path.str)
        let _ = path.str.as_ptr();
        desc = psprintf(c"block %u in file \"%s\"".as_ptr());
    } else {
        // psprintf(_("blocks %u..%u in file \"%s\""),
        //          sd->smgr.blockNum, sd->smgr.blockNum + sd->smgr.nblocks - 1, path.str)
        let _ = path.str.as_ptr();
        desc = psprintf(c"blocks %u..%u in file \"%s\"".as_ptr());
    }

    desc
}

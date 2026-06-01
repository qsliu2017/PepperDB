//! md.rs
//!   This code manages relations that reside on magnetic disk.
//! Translated 1:1 from postgres/src/backend/storage/smgr/md.c
//
//-------------------------------------------------------------------------
//
// md.c
//	  This code manages relations that reside on magnetic disk.
//
// Or at least, that was what the Berkeley folk had in mind when they named
// this file.  In reality, what this code provides is an interface from
// the smgr API to Unix-like filesystem APIs, so it will work with any type
// of device for which the operating system provides filesystem support.
// It doesn't matter whether the bits are on spinning rust or some other
// storage technology.
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
//
//
// IDENTIFICATION
//	  src/backend/storage/smgr/md.c
//
//-------------------------------------------------------------------------

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use crate::prelude::*;

// #include <unistd.h>
// #include <fcntl.h>
// #include <sys/file.h>
// #include "access/xlogutils.h"
// #include "commands/tablespace.h"
// #include "common/file_utils.h"
// #include "miscadmin.h"
// #include "pg_trace.h"
// #include "pgstat.h"
// #include "storage/aio.h"
// #include "storage/bufmgr.h"
// #include "storage/fd.h"
// #include "storage/md.h"
// #include "storage/relfilelocator.h"
// #include "storage/smgr.h"
// #include "storage/sync.h"
// #include "utils/memutils.h"

use core::ffi::CStr;

use crate::utils::elog::LOG_SERVER_ONLY;

// errmsg_internal() is a thin alias for errmsg() in this port; errcode/errdetail
// get folded into /* C also: */ comments.
macro_rules! errmsg_internal {
    ($fmt:literal $(, $arg:expr)*) => { errmsg!($fmt $(, $arg)*) };
}

use crate::access::transam::xlogutils::XLogDropRelation;
use crate::common::file_utils::compute_remaining_iovec;
use crate::common::relpath::{
    ForkNumber, GetRelationPath, RelPathStr, InvalidForkNumber, MAIN_FORKNUM, MAX_FORKNUM,
    REL_PATH_STR_MAXLEN,
};
use crate::miscadmin::IsBinaryUpgrade;
use crate::pg_config::{BLCKSZ, RELSEG_SIZE};
use crate::pg_config_manual::{MAXPGPATH, PG_IO_ALIGN_SIZE};
use crate::storage::aio::aio::{
    pgaio_io_register_callbacks, pgaio_io_set_flag, PGAIO_HF_BUFFERED,
};
use crate::storage::aio::aio_callback::{pgaio_result_report, PGAIO_HCB_MD_READV};
use crate::storage::aio::aio_io::pgaio_io_get_iovec;
use crate::storage::aio::aio_target::pgaio_io_get_target_data;
use crate::storage::aio_types::{
    PgAioHandleCallbacks, PgAioResult, PgAioTargetData, PGAIO_RS_ERROR, PGAIO_RS_PARTIAL,
};
use crate::storage::aio_internal::PgAioHandle;
use crate::storage::block::{BlockNumber, InvalidBlockNumber, MaxBlockNumber};
use crate::storage::file::fd::{
    data_sync_elevel, io_direct_flags, pg_truncate, File, FileClose, FileFallocate, FileGetRawDesc,
    FilePathName, FilePrefetch, FileReadV, FileSize, FileStartReadV, FileSync, FileTruncate,
    FileWriteV, FileWriteback, FileZero, PathNameOpenFile,
};
use crate::storage::procnumber::{MyProcNumber, INVALID_PROC_NUMBER};
use crate::storage::relfilelocator::{
    RelFileLocator, RelFileLocatorBackend, RelFileLocatorBackendIsTemp,
};
use crate::storage::smgr::smgr::{
    pgaio_io_set_target_smgr, smgrclose, smgrdounlinkall, smgropen, SmgrIsTemp, SMgrRelation,
};
use crate::storage::sync::sync::{
    FileTag, RegisterSyncRequest, SYNC_FORGET_REQUEST, SYNC_FILTER_REQUEST, SYNC_HANDLER_MD,
    SYNC_REQUEST, SYNC_UNLINK_REQUEST,
};
use crate::utils::activity::pgstat_io::{
    pgstat_count_io_op_time, pgstat_prepare_io_time, instr_time, IOCONTEXT_NORMAL, IOOBJECT_RELATION,
    IOOP_FSYNC,
};
use crate::utils::palloc::MCXT_ALLOC_ZERO;

use crate::port::pg_iovec::iovec;

// TODO(pg-port): real PG_IOV_MAX lives in port/pg_iovec.h (= Min(IOV_MAX, 128));
// the port crate exports it as a c_int, but array sizing needs usize.
const PG_IOV_MAX: usize = 128;

// TODO(pg-port): real OIDCHARS lives in common/relpath.h (max chars printed by %u).
const OIDCHARS: usize = 10;

// ----------------------------------------------------------------------------
// libc / errno
// ----------------------------------------------------------------------------
extern "C" {
    fn unlink(path: *const c_char) -> c_int;
    fn sprintf(s: *mut c_char, fmt: *const c_char, ...) -> c_int;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strlcpy(dst: *mut c_char, src: *const c_char, siz: usize) -> usize;
    // errno access (thread-local). macOS/Darwin uses __error().
    fn __error() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn set_errno(e: c_int) {
    *__error() = e;
}

// from errno.h
const ENOENT: c_int = 2;
const ENOSPC: c_int = 28;

// from fcntl.h
const O_RDWR: c_int = 0x0002;
const O_CREAT: c_int = 0x0200;
const O_EXCL: c_int = 0x0800;

// ----------------------------------------------------------------------------
// TODO(pg-port) stubs: dependencies not yet ported into src/.
// ----------------------------------------------------------------------------

// TODO(pg-port): real PG_BINARY lives in c.h (0 on non-Windows).
const PG_BINARY: c_int = 0;

// TODO(pg-port): real PG_O_DIRECT lives in storage/fd.h.  On Darwin it is a
// sentinel meaning "apply F_NOCACHE after open"; treated as 0 here for flags.
const PG_O_DIRECT: c_int = 0;

// TODO(pg-port): real IO_DIRECT_DATA lives in storage/fd.h.
const IO_DIRECT_DATA: c_int = 0x01;

// TODO(pg-port): real PG_IO_ALIGN_SIZE-related TYPEALIGN check; not needed at
// runtime here.

// TODO(pg-port): real file_extend_method GUC and enum live in storage/fd.h /
// storage/md.h.  POSIX_FALLOCATE is the platform default.
const FILE_EXTEND_METHOD_WRITE_ZEROS: c_int = 0;
const FILE_EXTEND_METHOD_POSIX_FALLOCATE: c_int = 1;
static mut file_extend_method: c_int = FILE_EXTEND_METHOD_POSIX_FALLOCATE;

// TODO(pg-port): real zero_damaged_pages GUC lives in storage/buffer/bufmgr.c.
static mut zero_damaged_pages: bool = false;

// TODO(pg-port): real track_io_timing GUC lives in utils/misc/guc_tables.c.
static mut track_io_timing: bool = false;

// TODO(pg-port): real InRecovery lives in access/xlog.c (xlog.h).  True while
// the startup process is replaying WAL.
static mut InRecovery: bool = false;

// TODO(pg-port): real FILE_POSSIBLY_DELETED lives in storage/fd.h.
#[inline]
fn FILE_POSSIBLY_DELETED(err: c_int) -> bool {
    err == ENOENT
}

// TODO(pg-port): real wait-event constants live in the generated
// utils/wait_event_types.h.  They are opaque u32 tags for tracing only.
const WAIT_EVENT_DATA_FILE_EXTEND: u32 = 0;
const WAIT_EVENT_DATA_FILE_PREFETCH: u32 = 0;
const WAIT_EVENT_DATA_FILE_READ: u32 = 0;
const WAIT_EVENT_DATA_FILE_WRITE: u32 = 0;
const WAIT_EVENT_DATA_FILE_FLUSH: u32 = 0;
const WAIT_EVENT_DATA_FILE_TRUNCATE: u32 = 0;
const WAIT_EVENT_DATA_FILE_SYNC: u32 = 0;
const WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC: u32 = 0;

// TODO(pg-port): real FileWrite (single-buffer write) lives in storage/fd.c;
// here implemented in terms of FileWriteV over a one-element iovec.
unsafe fn FileWrite(
    file: File,
    buffer: *const c_void,
    amount: c_int,
    offset: off_t,
    wait_event_info: u32,
) -> c_int {
    let iov = iovec {
        iov_base: buffer as *mut c_void,
        iov_len: amount as usize,
    };
    FileWriteV(file, (&iov as *const iovec).cast(), 1, offset, wait_event_info) as c_int
}

// TODO(pg-port): real TablespaceCreateDbspace lives in commands/tablespace.c.
unsafe fn TablespaceCreateDbspace(_spcOid: Oid, _dbOid: Oid, _isRedo: bool) {
    // TODO(pg-port): create the per-database subdirectory for the tablespace.
}

// relpath.h wrapper macros, expanded as functions (common/relpath.h):
//   relpathbackend(rlocator, backend, forknum) =
//       GetRelationPath(rlocator.dbOid, rlocator.spcOid, rlocator.relNumber,
//                       backend, forknum)
#[inline]
unsafe fn relpathbackend(
    rlocator: RelFileLocator,
    backend: c_int,
    forknum: ForkNumber,
) -> RelPathStr {
    GetRelationPath(rlocator.dbOid, rlocator.spcOid, rlocator.relNumber, backend, forknum)
}

// relpathperm(rlocator, forknum) = relpathbackend(rlocator, INVALID_PROC_NUMBER, forknum)
#[inline]
unsafe fn relpathperm(rlocator: RelFileLocator, forknum: ForkNumber) -> RelPathStr {
    relpathbackend(rlocator, INVALID_PROC_NUMBER, forknum)
}

// relpath(rlocator, forknum) = relpathbackend(rlocator.locator, rlocator.backend, forknum)
#[inline]
unsafe fn relpath(rlocator: RelFileLocatorBackend, forknum: ForkNumber) -> RelPathStr {
    relpathbackend(rlocator.locator, rlocator.backend, forknum)
}

// ----------------------------------------------------------------------------
// md.c body
// ----------------------------------------------------------------------------

// The magnetic disk storage manager keeps track of open file
// descriptors in its own descriptor pool.  This is done to make it
// easier to support relations that are larger than the operating
// system's file size limit (often 2GBytes).  In order to do that,
// we break relations up into "segment" files that are each shorter than
// the OS file size limit.  The segment size is set by the RELSEG_SIZE
// configuration constant in pg_config.h.
//
// File descriptors are stored in the per-fork md_seg_fds arrays inside
// SMgrRelation. The length of these arrays is stored in md_num_open_segs.
//
// The entire MdfdVec array is palloc'd in the MdCxt memory context.

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MdfdVec {
    pub mdfd_vfd: File,         /* fd number in fd.c's pool */
    pub mdfd_segno: BlockNumber, /* segment number, from 0 */
}

static mut MdCxt: MemoryContext = null_mut(); /* context for all MdfdVec objects */

// Populate a file tag describing an md.c segment file.
// #define INIT_MD_FILETAG(a,xx_rlocator,xx_forknum,xx_segno)
#[inline]
unsafe fn INIT_MD_FILETAG(
    a: &mut FileTag,
    xx_rlocator: RelFileLocator,
    xx_forknum: ForkNumber,
    xx_segno: BlockNumber,
) {
    *a = core::mem::zeroed();
    a.handler = SYNC_HANDLER_MD as int16;
    a.rlocator = xx_rlocator;
    a.forknum = xx_forknum as int16;
    a.segno = xx_segno as uint64;
}

/*** behavior for mdopen & _mdfd_getseg ***/
/* ereport if segment not present */
const EXTENSION_FAIL: c_int = 1 << 0;
/* return NULL if segment not present */
const EXTENSION_RETURN_NULL: c_int = 1 << 1;
/* create new segments as needed */
const EXTENSION_CREATE: c_int = 1 << 2;
/* create new segments if needed during recovery */
const EXTENSION_CREATE_RECOVERY: c_int = 1 << 3;
/* don't try to open a segment, if not already open */
const EXTENSION_DONT_OPEN: c_int = 1 << 5;

// Fixed-length string to represent paths to files that need to be built by
// md.c.
const SEGMENT_CHARS: usize = OIDCHARS;
const MD_PATH_STR_MAXLEN: usize = REL_PATH_STR_MAXLEN + core::mem::size_of::<c_char>() + SEGMENT_CHARS;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct MdPathStr {
    pub str: [c_char; MD_PATH_STR_MAXLEN + 1],
}

pub const aio_md_readv_cb: PgAioHandleCallbacks = PgAioHandleCallbacks {
    stage: None,
    complete_shared: Some(md_readv_complete),
    complete_local: None,
    report: Some(md_readv_report),
};

#[inline]
unsafe fn _mdfd_open_flags() -> c_int {
    let mut flags: c_int = O_RDWR | PG_BINARY;

    if io_direct_flags & IO_DIRECT_DATA != 0 {
        flags |= PG_O_DIRECT;
    }

    flags
}

// mdinit() -- Initialize private state for magnetic disk storage manager.
pub unsafe fn mdinit() {
    MdCxt = AllocSetContextCreate!(
        TopMemoryContext,
        c"MdSmgr".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
}

// mdexists() -- Does the physical file exist?
//
// Note: this will return true for lingering files, with pending deletions
pub unsafe fn mdexists(reln: SMgrRelation, forknum: ForkNumber) -> bool {
    // Close it first, to ensure that we notice if the fork has been unlinked
    // since we opened it.  As an optimization, we can skip that in recovery,
    // which already closes relations when dropping them.
    if !InRecovery {
        mdclose(reln, forknum);
    }

    !mdopenfork(reln, forknum, EXTENSION_RETURN_NULL).is_null()
}

// mdcreate() -- Create a new relation on magnetic disk.
//
// If isRedo is true, it's okay for the relation to exist already.
pub unsafe fn mdcreate(reln: SMgrRelation, forknum: ForkNumber, isRedo: bool) {
    let mdfd: *mut MdfdVec;
    let path: RelPathStr;
    let mut fd: File;

    if isRedo && (*reln).md_num_open_segs[forknum as usize] > 0 {
        return; /* created and opened already... */
    }

    Assert!((*reln).md_num_open_segs[forknum as usize] == 0);

    // We may be using the target table space for the first time in this
    // database, so create a per-database subdirectory if needed.
    //
    // XXX this is a fairly ugly violation of module layering, but this seems
    // to be the best place to put the check.  Maybe TablespaceCreateDbspace
    // should be here and not in commands/tablespace.c?  But that would imply
    // importing a lot of stuff that smgr.c oughtn't know, either.
    TablespaceCreateDbspace(
        (*reln).smgr_rlocator.locator.spcOid,
        (*reln).smgr_rlocator.locator.dbOid,
        isRedo,
    );

    path = relpath((*reln).smgr_rlocator, forknum);

    fd = PathNameOpenFile(path.str.as_ptr(), _mdfd_open_flags() | O_CREAT | O_EXCL);

    if fd < 0 {
        let save_errno = errno();

        if isRedo {
            fd = PathNameOpenFile(path.str.as_ptr(), _mdfd_open_flags());
        }
        if fd < 0 {
            /* be sure to report the error reported by create, not open */
            set_errno(save_errno);
            // C also: errcode_for_file_access()
            ereport!(
                ERROR,
                errmsg!(
                    "could not create file \"{}\": errno={}",
                    CStr::from_ptr(path.str.as_ptr()).to_string_lossy(),
                    errno()
                )
            );
        }
    }

    _fdvec_resize(reln, forknum, 1);
    mdfd = ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add(0);
    (*mdfd).mdfd_vfd = fd;
    (*mdfd).mdfd_segno = 0;

    if !SmgrIsTemp(reln) {
        register_dirty_segment(reln, forknum, mdfd);
    }
}

// mdunlink() -- Unlink a relation.
//
// Note that we're passed a RelFileLocatorBackend --- by the time this is called,
// there won't be an SMgrRelation hashtable entry anymore.
//
// forknum can be a fork number to delete a specific fork, or InvalidForkNumber
// to delete all forks.
//
// (See md.c header comment for the full rationale.)
//
// Note: any failure should be reported as WARNING not ERROR, because
// we are usually not in a transaction anymore when this is called.
pub unsafe fn mdunlink(rlocator: RelFileLocatorBackend, forknum: ForkNumber, isRedo: bool) {
    /* Now do the per-fork work */
    if forknum == InvalidForkNumber {
        let mut forknum: ForkNumber = 0;
        while forknum <= MAX_FORKNUM {
            mdunlinkfork(rlocator, forknum, isRedo);
            forknum += 1;
        }
    } else {
        mdunlinkfork(rlocator, forknum, isRedo);
    }
}

// Truncate a file to release disk space.
unsafe fn do_truncate(path: *const c_char) -> c_int {
    let save_errno: c_int;
    let ret: c_int;

    ret = pg_truncate(path, 0);

    /* Log a warning here to avoid repetition in callers. */
    if ret < 0 && errno() != ENOENT {
        save_errno = errno();
        // C also: errcode_for_file_access()
        ereport!(
            WARNING,
            errmsg!(
                "could not truncate file \"{}\": errno={}",
                CStr::from_ptr(path).to_string_lossy(),
                errno()
            )
        );
        set_errno(save_errno);
    }

    ret
}

unsafe fn mdunlinkfork(rlocator: RelFileLocatorBackend, forknum: ForkNumber, isRedo: bool) {
    let path: RelPathStr;
    let mut ret: c_int;
    let mut save_errno: c_int;

    path = relpath(rlocator, forknum);

    // Truncate and then unlink the first segment, or just register a request
    // to unlink it later, as described in the comments for mdunlink().
    if isRedo
        || IsBinaryUpgrade
        || forknum != MAIN_FORKNUM
        || RelFileLocatorBackendIsTemp(&rlocator)
    {
        if !RelFileLocatorBackendIsTemp(&rlocator) {
            /* Prevent other backends' fds from holding on to the disk space */
            ret = do_truncate(path.str.as_ptr());

            /* Forget any pending sync requests for the first segment */
            save_errno = errno();
            register_forget_request(rlocator, forknum, 0 /* first seg */);
            set_errno(save_errno);
        } else {
            ret = 0;
        }

        /* Next unlink the file, unless it was already found to be missing */
        if ret >= 0 || errno() != ENOENT {
            ret = unlink(path.str.as_ptr());
            if ret < 0 && errno() != ENOENT {
                save_errno = errno();
                // C also: errcode_for_file_access()
                ereport!(
                    WARNING,
                    errmsg!(
                        "could not remove file \"{}\": errno={}",
                        CStr::from_ptr(path.str.as_ptr()).to_string_lossy(),
                        errno()
                    )
                );
                set_errno(save_errno);
            }
        }
    } else {
        /* Prevent other backends' fds from holding on to the disk space */
        ret = do_truncate(path.str.as_ptr());

        /* Register request to unlink first segment later */
        save_errno = errno();
        register_unlink_segment(rlocator, forknum, 0 /* first seg */);
        set_errno(save_errno);
    }

    // Delete any additional segments.
    //
    // Note that because we loop until getting ENOENT, we will correctly
    // remove all inactive segments as well as active ones.  Ideally we'd
    // continue the loop until getting exactly that errno, but that risks an
    // infinite loop if the problem is directory-wide (for instance, if we
    // suddenly can't read the data directory itself).  We compromise by
    // continuing after a non-ENOENT truncate error, but stopping after any
    // unlink error.  If there is indeed a directory-wide problem, additional
    // unlink attempts wouldn't work anyway.
    if ret >= 0 || errno() != ENOENT {
        let mut segpath: MdPathStr = core::mem::zeroed();
        let mut segno: BlockNumber;

        segno = 1;
        loop {
            sprintf(
                segpath.str.as_mut_ptr(),
                c"%s.%u".as_ptr(),
                path.str.as_ptr(),
                segno as c_uint,
            );

            if !RelFileLocatorBackendIsTemp(&rlocator) {
                // Prevent other backends' fds from holding on to the disk
                // space.  We're done if we see ENOENT, though.
                if do_truncate(segpath.str.as_ptr()) < 0 && errno() == ENOENT {
                    break;
                }

                // Forget any pending sync requests for this segment before we
                // try to unlink.
                register_forget_request(rlocator, forknum, segno);
            }

            if unlink(segpath.str.as_ptr()) < 0 {
                /* ENOENT is expected after the last segment... */
                if errno() != ENOENT {
                    // C also: errcode_for_file_access()
                    ereport!(
                        WARNING,
                        errmsg!(
                            "could not remove file \"{}\": errno={}",
                            CStr::from_ptr(segpath.str.as_ptr()).to_string_lossy(),
                            errno()
                        )
                    );
                }
                break;
            }

            segno += 1;
        }
    }
}

// mdextend() -- Add a block to the specified relation.
//
// The semantics are nearly the same as mdwrite(): write at the
// specified position.  However, this is to be used for the case of
// extending a relation (i.e., blocknum is at or beyond the current
// EOF).  Note that we assume writing a block beyond current EOF
// causes intervening file space to become filled with zeroes.
pub unsafe fn mdextend(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: *const c_void,
    skipFsync: bool,
) {
    let seekpos: off_t;
    let nbytes: c_int;
    let v: *mut MdfdVec;

    /* If this build supports direct I/O, the buffer must be I/O aligned. */
    if PG_O_DIRECT != 0 && PG_IO_ALIGN_SIZE <= BLCKSZ {
        Assert!((buffer as usize) == crate::c::TYPEALIGN(PG_IO_ALIGN_SIZE, buffer as usize));
    }

    /* This assert is too expensive to have on normally ... */
    // #ifdef CHECK_WRITE_VS_EXTEND
    //     Assert(blocknum >= mdnblocks(reln, forknum));
    // #endif

    // If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
    // more --- we mustn't create a block whose number actually is
    // InvalidBlockNumber.  (Note that this failure should be unreachable
    // because of upstream checks in bufmgr.c.)
    if blocknum == InvalidBlockNumber {
        // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        ereport!(
            ERROR,
            errmsg!(
                "cannot extend file \"{}\" beyond {} blocks",
                CStr::from_ptr(relpath((*reln).smgr_rlocator, forknum).str.as_ptr())
                    .to_string_lossy(),
                InvalidBlockNumber
            )
        );
    }

    v = _mdfd_getseg(reln, forknum, blocknum, skipFsync, EXTENSION_CREATE);

    seekpos = (BLCKSZ as off_t) * (blocknum % (RELSEG_SIZE as BlockNumber)) as off_t;

    Assert!(seekpos < (BLCKSZ as off_t) * RELSEG_SIZE as off_t);

    nbytes = FileWrite(
        (*v).mdfd_vfd,
        buffer,
        BLCKSZ as c_int,
        seekpos,
        WAIT_EVENT_DATA_FILE_EXTEND,
    );
    if nbytes != BLCKSZ as c_int {
        if nbytes < 0 {
            // C also: errcode_for_file_access(), errhint("Check free disk space.")
            ereport!(
                ERROR,
                errmsg!(
                    "could not extend file \"{}\": errno={}",
                    CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                    errno()
                )
            );
        }
        /* short write: complain appropriately */
        // C also: errcode(ERRCODE_DISK_FULL), errhint("Check free disk space.")
        ereport!(
            ERROR,
            errmsg!(
                "could not extend file \"{}\": wrote only {} of {} bytes at block {}",
                CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                nbytes,
                BLCKSZ,
                blocknum
            )
        );
    }

    if !skipFsync && !SmgrIsTemp(reln) {
        register_dirty_segment(reln, forknum, v);
    }

    Assert!(_mdnblocks(reln, forknum, v) <= RELSEG_SIZE as BlockNumber);
}

// mdzeroextend() -- Add new zeroed out blocks to the specified relation.
//
// Similar to mdextend(), except the relation can be extended by multiple
// blocks at once and the added blocks will be filled with zeroes.
pub unsafe fn mdzeroextend(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: c_int,
    skipFsync: bool,
) {
    let mut v: *mut MdfdVec;
    let mut curblocknum: BlockNumber = blocknum;
    let mut remblocks: c_int = nblocks;

    Assert!(nblocks > 0);

    /* This assert is too expensive to have on normally ... */
    // #ifdef CHECK_WRITE_VS_EXTEND
    //     Assert(blocknum >= mdnblocks(reln, forknum));
    // #endif

    // If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
    // more --- we mustn't create a block whose number actually is
    // InvalidBlockNumber or larger.
    if (blocknum as u64) + nblocks as u64 >= InvalidBlockNumber as u64 {
        // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        ereport!(
            ERROR,
            errmsg!(
                "cannot extend file \"{}\" beyond {} blocks",
                CStr::from_ptr(relpath((*reln).smgr_rlocator, forknum).str.as_ptr())
                    .to_string_lossy(),
                InvalidBlockNumber
            )
        );
    }

    while remblocks > 0 {
        let segstartblock: BlockNumber = curblocknum % (RELSEG_SIZE as BlockNumber);
        let seekpos: off_t = (BLCKSZ as off_t) * segstartblock as off_t;
        let numblocks: c_int;

        if segstartblock as usize + remblocks as usize > RELSEG_SIZE {
            numblocks = RELSEG_SIZE as c_int - segstartblock as c_int;
        } else {
            numblocks = remblocks;
        }

        v = _mdfd_getseg(reln, forknum, curblocknum, skipFsync, EXTENSION_CREATE);

        Assert!((segstartblock as usize) < RELSEG_SIZE);
        Assert!(segstartblock as usize + numblocks as usize <= RELSEG_SIZE);

        // If available and useful, use posix_fallocate() (via
        // FileFallocate()) to extend the relation. That's often more
        // efficient than using write(), as it commonly won't cause the kernel
        // to allocate page cache space for the extended pages.
        //
        // However, we don't use FileFallocate() for small extensions, as it
        // defeats delayed allocation on some filesystems. Not clear where
        // that decision should be made though? For now just use a cutoff of
        // 8, anything between 4 and 8 worked OK in some local testing.
        if numblocks > 8 && file_extend_method != FILE_EXTEND_METHOD_WRITE_ZEROS {
            let ret: c_int;

            // #ifdef HAVE_POSIX_FALLOCATE
            if file_extend_method == FILE_EXTEND_METHOD_POSIX_FALLOCATE {
                ret = FileFallocate(
                    (*v).mdfd_vfd,
                    seekpos,
                    (BLCKSZ as off_t) * numblocks as off_t,
                    WAIT_EVENT_DATA_FILE_EXTEND,
                );
            } else
            // #endif
            {
                elog!(ERROR, "unsupported file_extend_method: {}", file_extend_method);
            }
            if ret != 0 {
                // C also: errcode_for_file_access(), errhint("Check free disk space.")
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not extend file \"{}\" with FileFallocate(): errno={}",
                        CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                        errno()
                    )
                );
            }
        } else {
            let ret: c_int;

            // Even if we don't want to use fallocate, we can still extend a
            // bit more efficiently than writing each 8kB block individually.
            // pg_pwrite_zeros() (via FileZero()) uses pg_pwritev_with_retry()
            // to avoid multiple writes or needing a zeroed buffer for the
            // whole length of the extension.
            ret = FileZero(
                (*v).mdfd_vfd,
                seekpos,
                (BLCKSZ as off_t) * numblocks as off_t,
                WAIT_EVENT_DATA_FILE_EXTEND,
            );
            if ret < 0 {
                // C also: errcode_for_file_access(), errhint("Check free disk space.")
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not extend file \"{}\": errno={}",
                        CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                        errno()
                    )
                );
            }
        }

        if !skipFsync && !SmgrIsTemp(reln) {
            register_dirty_segment(reln, forknum, v);
        }

        Assert!(_mdnblocks(reln, forknum, v) <= RELSEG_SIZE as BlockNumber);

        remblocks -= numblocks;
        curblocknum += numblocks as BlockNumber;
    }
}

// mdopenfork() -- Open one fork of the specified relation.
//
// Note we only open the first segment, when there are multiple segments.
//
// If first segment is not present, either ereport or return NULL according
// to "behavior".  We treat EXTENSION_CREATE the same as EXTENSION_FAIL;
// EXTENSION_CREATE means it's OK to extend an existing relation, not to
// invent one out of whole cloth.
unsafe fn mdopenfork(reln: SMgrRelation, forknum: ForkNumber, behavior: c_int) -> *mut MdfdVec {
    let mdfd: *mut MdfdVec;
    let path: RelPathStr;
    let fd: File;

    /* No work if already open */
    if (*reln).md_num_open_segs[forknum as usize] > 0 {
        return ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add(0);
    }

    path = relpath((*reln).smgr_rlocator, forknum);

    fd = PathNameOpenFile(path.str.as_ptr(), _mdfd_open_flags());

    if fd < 0 {
        if (behavior & EXTENSION_RETURN_NULL) != 0 && FILE_POSSIBLY_DELETED(errno()) {
            return null_mut();
        }
        // C also: errcode_for_file_access()
        ereport!(
            ERROR,
            errmsg!(
                "could not open file \"{}\": errno={}",
                CStr::from_ptr(path.str.as_ptr()).to_string_lossy(),
                errno()
            )
        );
    }

    _fdvec_resize(reln, forknum, 1);
    mdfd = ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add(0);
    (*mdfd).mdfd_vfd = fd;
    (*mdfd).mdfd_segno = 0;

    Assert!(_mdnblocks(reln, forknum, mdfd) <= RELSEG_SIZE as BlockNumber);

    mdfd
}

// mdopen() -- Initialize newly-opened relation.
pub unsafe fn mdopen(reln: SMgrRelation) {
    /* mark it not open */
    let mut forknum: c_int = 0;
    while forknum <= MAX_FORKNUM {
        (*reln).md_num_open_segs[forknum as usize] = 0;
        forknum += 1;
    }
}

// mdclose() -- Close the specified relation, if it isn't closed already.
pub unsafe fn mdclose(reln: SMgrRelation, forknum: ForkNumber) {
    let mut nopensegs: c_int = (*reln).md_num_open_segs[forknum as usize];

    /* No work if already closed */
    if nopensegs == 0 {
        return;
    }

    /* close segments starting from the end */
    while nopensegs > 0 {
        let v: *mut MdfdVec =
            ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add((nopensegs - 1) as usize)
                as *mut MdfdVec;

        FileClose((*v).mdfd_vfd);
        _fdvec_resize(reln, forknum, nopensegs - 1);
        nopensegs -= 1;
    }
}

// mdprefetch() -- Initiate asynchronous read of the specified blocks of a relation
pub unsafe fn mdprefetch(
    reln: SMgrRelation,
    forknum: ForkNumber,
    mut blocknum: BlockNumber,
    mut nblocks: c_int,
) -> bool {
    // #ifdef USE_PREFETCH
    Assert!((io_direct_flags & IO_DIRECT_DATA) == 0);

    if (blocknum as u64) + nblocks as u64 > MaxBlockNumber as u64 + 1 {
        return false;
    }

    while nblocks > 0 {
        let seekpos: off_t;
        let v: *mut MdfdVec;
        let nblocks_this_segment: c_int;

        v = _mdfd_getseg(
            reln,
            forknum,
            blocknum,
            false,
            if InRecovery {
                EXTENSION_RETURN_NULL
            } else {
                EXTENSION_FAIL
            },
        );
        if v.is_null() {
            return false;
        }

        seekpos = (BLCKSZ as off_t) * (blocknum % (RELSEG_SIZE as BlockNumber)) as off_t;

        Assert!(seekpos < (BLCKSZ as off_t) * RELSEG_SIZE as off_t);

        nblocks_this_segment = Min(
            nblocks,
            RELSEG_SIZE as c_int - (blocknum % (RELSEG_SIZE as BlockNumber)) as c_int,
        );

        let _ = FilePrefetch(
            (*v).mdfd_vfd,
            seekpos,
            (BLCKSZ as off_t) * nblocks_this_segment as off_t,
            WAIT_EVENT_DATA_FILE_PREFETCH,
        );

        blocknum += nblocks_this_segment as BlockNumber;
        nblocks -= nblocks_this_segment;
    }
    // #endif /* USE_PREFETCH */

    true
}

// Convert an array of buffer address into an array of iovec objects, and
// return the number that were required.  'iov' must have enough space for up
// to 'nblocks' elements, but the number used may be less depending on
// merging.  In the case of a run of fully contiguous buffers, a single iovec
// will be populated that can be handled as a plain non-vectored I/O.
unsafe fn buffers_to_iovec(iov: *mut iovec, buffers: *mut *mut c_void, nblocks: c_int) -> c_int {
    let mut iovp: *mut iovec;
    let mut iovcnt: c_int;

    Assert!(nblocks >= 1);

    /* If this build supports direct I/O, buffers must be I/O aligned. */
    for i in 0..nblocks {
        if PG_O_DIRECT != 0 && PG_IO_ALIGN_SIZE <= BLCKSZ {
            Assert!(
                (*buffers.add(i as usize) as usize)
                    == crate::c::TYPEALIGN(PG_IO_ALIGN_SIZE, *buffers.add(i as usize) as usize)
            );
        }
    }

    /* Start the first iovec off with the first buffer. */
    iovp = &mut *iov.add(0);
    (*iovp).iov_base = *buffers.add(0);
    (*iovp).iov_len = BLCKSZ;
    iovcnt = 1;

    /* Try to merge the rest. */
    for i in 1..nblocks {
        let buffer: *mut c_void = *buffers.add(i as usize);

        if ((*iovp).iov_base as *mut c_char).add((*iovp).iov_len) == buffer as *mut c_char {
            /* Contiguous with the last iovec. */
            (*iovp).iov_len += BLCKSZ;
        } else {
            /* Need a new iovec. */
            iovp = iovp.add(1);
            (*iovp).iov_base = buffer;
            (*iovp).iov_len = BLCKSZ;
            iovcnt += 1;
        }
    }

    iovcnt
}

// mdmaxcombine() -- Return the maximum number of total blocks that can be
//				 combined with an IO starting at blocknum.
pub unsafe fn mdmaxcombine(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    blocknum: BlockNumber,
) -> uint32 {
    let segoff: BlockNumber;

    segoff = blocknum % (RELSEG_SIZE as BlockNumber);

    RELSEG_SIZE as uint32 - segoff
}

// mdreadv() -- Read the specified blocks from a relation.
pub unsafe fn mdreadv(
    reln: SMgrRelation,
    forknum: ForkNumber,
    mut blocknum: BlockNumber,
    mut buffers: *mut *mut c_void,
    mut nblocks: BlockNumber,
) {
    while nblocks > 0 {
        let mut iov: [iovec; PG_IOV_MAX] = core::mem::zeroed();
        let mut iovcnt: c_int;
        let mut seekpos: off_t;
        let mut nbytes: c_int;
        let v: *mut MdfdVec;
        let mut nblocks_this_segment: BlockNumber;
        let mut transferred_this_segment: usize;
        let size_this_segment: usize;

        v = _mdfd_getseg(
            reln,
            forknum,
            blocknum,
            false,
            EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY,
        );

        seekpos = (BLCKSZ as off_t) * (blocknum % (RELSEG_SIZE as BlockNumber)) as off_t;

        Assert!(seekpos < (BLCKSZ as off_t) * RELSEG_SIZE as off_t);

        nblocks_this_segment = Min(
            nblocks,
            RELSEG_SIZE as BlockNumber - (blocknum % (RELSEG_SIZE as BlockNumber)),
        );
        nblocks_this_segment = Min(nblocks_this_segment, lengthof!(iov) as BlockNumber);

        if nblocks_this_segment != nblocks {
            elog!(ERROR, "read crosses segment boundary");
        }

        iovcnt = buffers_to_iovec(iov.as_mut_ptr(), buffers, nblocks_this_segment as c_int);
        size_this_segment = nblocks_this_segment as usize * BLCKSZ;
        transferred_this_segment = 0;

        // Inner loop to continue after a short read.  We'll keep going until
        // we hit EOF rather than assuming that a short read means we hit the
        // end.
        loop {
            // TRACE_POSTGRESQL_SMGR_MD_READ_START(...)
            nbytes = FileReadV(
                (*v).mdfd_vfd,
                iov.as_ptr().cast(),
                iovcnt,
                seekpos,
                WAIT_EVENT_DATA_FILE_READ,
            ) as c_int;
            // TRACE_POSTGRESQL_SMGR_MD_READ_DONE(...)

            // #ifdef SIMULATE_SHORT_READ
            //     nbytes = Min(nbytes, 4096);
            // #endif

            if nbytes < 0 {
                // C also: errcode_for_file_access()
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not read blocks {}..{} in file \"{}\": errno={}",
                        blocknum,
                        blocknum + nblocks_this_segment - 1,
                        CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                        errno()
                    )
                );
            }

            if nbytes == 0 {
                // We are at or past EOF, or we read a partial block at EOF.
                // Normally this is an error; upper levels should never try to
                // read a nonexistent block.  However, if zero_damaged_pages
                // is ON or we are InRecovery, we should instead return zeroes
                // without complaining.  This allows, for example, the case of
                // trying to update a block that was later truncated away.
                //
                // (See md.c for the full rationale; an Assert(false) marks
                // this codepath as believed-unreachable for PG 18.)
                if zero_damaged_pages || InRecovery {
                    Assert!(false); /* see comment above */

                    let mut i: BlockNumber = (transferred_this_segment / BLCKSZ) as BlockNumber;
                    while i < nblocks_this_segment {
                        std::ptr::write_bytes(*buffers.add(i as usize) as *mut u8, 0, BLCKSZ);
                        i += 1;
                    }
                    break;
                } else {
                    // C also: errcode(ERRCODE_DATA_CORRUPTED)
                    ereport!(
                        ERROR,
                        errmsg!(
                            "could not read blocks {}..{} in file \"{}\": read only {} of {} bytes",
                            blocknum,
                            blocknum + nblocks_this_segment - 1,
                            CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                            transferred_this_segment,
                            size_this_segment
                        )
                    );
                }
            }

            /* One loop should usually be enough. */
            transferred_this_segment += nbytes as usize;
            Assert!(transferred_this_segment <= size_this_segment);
            if transferred_this_segment == size_this_segment {
                break;
            }

            /* Adjust position and vectors after a short read. */
            seekpos += nbytes as off_t;
            iovcnt = compute_remaining_iovec(
                iov.as_mut_ptr().cast(),
                iov.as_ptr().cast(),
                iovcnt,
                nbytes as Size,
            );
        }

        nblocks -= nblocks_this_segment;
        buffers = buffers.add(nblocks_this_segment as usize);
        blocknum += nblocks_this_segment;
    }
}

// mdstartreadv() -- Asynchronous version of mdreadv().
pub unsafe fn mdstartreadv(
    ioh: *mut PgAioHandle,
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: *mut *mut c_void,
    nblocks: BlockNumber,
) {
    let seekpos: off_t;
    let v: *mut MdfdVec;
    let nblocks_this_segment: BlockNumber;
    let mut iov: *mut iovec = null_mut();
    let mut iovcnt: c_int;
    let ret: c_int;

    v = _mdfd_getseg(
        reln,
        forknum,
        blocknum,
        false,
        EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY,
    );

    seekpos = (BLCKSZ as off_t) * (blocknum % (RELSEG_SIZE as BlockNumber)) as off_t;

    Assert!(seekpos < (BLCKSZ as off_t) * RELSEG_SIZE as off_t);

    nblocks_this_segment = Min(
        nblocks,
        RELSEG_SIZE as BlockNumber - (blocknum % (RELSEG_SIZE as BlockNumber)),
    );

    if nblocks_this_segment != nblocks {
        elog!(ERROR, "read crossing segment boundary");
    }

    iovcnt = pgaio_io_get_iovec(ioh, &mut iov);

    Assert!(nblocks <= iovcnt as BlockNumber);

    iovcnt = buffers_to_iovec(iov, buffers, nblocks_this_segment as c_int);

    Assert!(iovcnt <= nblocks_this_segment as c_int);

    if (io_direct_flags & IO_DIRECT_DATA) == 0 {
        pgaio_io_set_flag(ioh, PGAIO_HF_BUFFERED);
    }

    pgaio_io_set_target_smgr(ioh, reln, forknum, blocknum, nblocks as c_int, false);
    pgaio_io_register_callbacks(ioh, PGAIO_HCB_MD_READV, 0);

    ret = FileStartReadV(
        ioh.cast(),
        (*v).mdfd_vfd,
        iovcnt,
        seekpos,
        WAIT_EVENT_DATA_FILE_READ,
    );
    if ret != 0 {
        // C also: errcode_for_file_access()
        ereport!(
            ERROR,
            errmsg!(
                "could not start reading blocks {}..{} in file \"{}\": errno={}",
                blocknum,
                blocknum + nblocks_this_segment - 1,
                CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                errno()
            )
        );
    }

    // The error checks corresponding to the post-read checks in mdreadv() are
    // in md_readv_complete().
    //
    // However we chose, at least for now, to not implement the
    // zero_damaged_pages logic present in mdreadv().
}

// mdwritev() -- Write the supplied blocks at the appropriate location.
//
// This is to be used only for updating already-existing blocks of a
// relation (ie, those before the current EOF).  To extend a relation,
// use mdextend().
pub unsafe fn mdwritev(
    reln: SMgrRelation,
    forknum: ForkNumber,
    mut blocknum: BlockNumber,
    mut buffers: *mut *const c_void,
    mut nblocks: BlockNumber,
    skipFsync: bool,
) {
    /* This assert is too expensive to have on normally ... */
    // #ifdef CHECK_WRITE_VS_EXTEND
    //     Assert((uint64) blocknum + (uint64) nblocks <= (uint64) mdnblocks(reln, forknum));
    // #endif

    while nblocks > 0 {
        let mut iov: [iovec; PG_IOV_MAX] = core::mem::zeroed();
        let mut iovcnt: c_int;
        let mut seekpos: off_t;
        let mut nbytes: c_int;
        let v: *mut MdfdVec;
        let mut nblocks_this_segment: BlockNumber;
        let mut transferred_this_segment: usize;
        let size_this_segment: usize;

        v = _mdfd_getseg(
            reln,
            forknum,
            blocknum,
            skipFsync,
            EXTENSION_FAIL | EXTENSION_CREATE_RECOVERY,
        );

        seekpos = (BLCKSZ as off_t) * (blocknum % (RELSEG_SIZE as BlockNumber)) as off_t;

        Assert!(seekpos < (BLCKSZ as off_t) * RELSEG_SIZE as off_t);

        nblocks_this_segment = Min(
            nblocks,
            RELSEG_SIZE as BlockNumber - (blocknum % (RELSEG_SIZE as BlockNumber)),
        );
        nblocks_this_segment = Min(nblocks_this_segment, lengthof!(iov) as BlockNumber);

        if nblocks_this_segment != nblocks {
            elog!(ERROR, "write crosses segment boundary");
        }

        iovcnt = buffers_to_iovec(
            iov.as_mut_ptr(),
            buffers as *mut *mut c_void,
            nblocks_this_segment as c_int,
        );
        size_this_segment = nblocks_this_segment as usize * BLCKSZ;
        transferred_this_segment = 0;

        // Inner loop to continue after a short write.  If the reason is that
        // we're out of disk space, a future attempt should get an ENOSPC
        // error from the kernel.
        loop {
            // TRACE_POSTGRESQL_SMGR_MD_WRITE_START(...)
            nbytes = FileWriteV(
                (*v).mdfd_vfd,
                iov.as_ptr().cast(),
                iovcnt,
                seekpos,
                WAIT_EVENT_DATA_FILE_WRITE,
            ) as c_int;
            // TRACE_POSTGRESQL_SMGR_MD_WRITE_DONE(...)

            // #ifdef SIMULATE_SHORT_WRITE
            //     nbytes = Min(nbytes, 4096);
            // #endif

            if nbytes < 0 {
                let _enospc: bool = errno() == ENOSPC;

                // C also: errcode_for_file_access(),
                //         enospc ? errhint("Check free disk space.") : 0
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not write blocks {}..{} in file \"{}\": errno={}",
                        blocknum,
                        blocknum + nblocks_this_segment - 1,
                        CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                        errno()
                    )
                );
            }

            /* One loop should usually be enough. */
            transferred_this_segment += nbytes as usize;
            Assert!(transferred_this_segment <= size_this_segment);
            if transferred_this_segment == size_this_segment {
                break;
            }

            /* Adjust position and iovecs after a short write. */
            seekpos += nbytes as off_t;
            iovcnt = compute_remaining_iovec(
                iov.as_mut_ptr().cast(),
                iov.as_ptr().cast(),
                iovcnt,
                nbytes as Size,
            );
        }

        if !skipFsync && !SmgrIsTemp(reln) {
            register_dirty_segment(reln, forknum, v);
        }

        nblocks -= nblocks_this_segment;
        buffers = buffers.add(nblocks_this_segment as usize);
        blocknum += nblocks_this_segment;
    }
}

// mdwriteback() -- Tell the kernel to write pages back to storage.
//
// This accepts a range of blocks because flushing several pages at once is
// considerably more efficient than doing so individually.
pub unsafe fn mdwriteback(
    reln: SMgrRelation,
    forknum: ForkNumber,
    mut blocknum: BlockNumber,
    mut nblocks: BlockNumber,
) {
    Assert!((io_direct_flags & IO_DIRECT_DATA) == 0);

    // Issue flush requests in as few requests as possible; have to split at
    // segment boundaries though, since those are actually separate files.
    while nblocks > 0 {
        let mut nflush: BlockNumber = nblocks;
        let seekpos: off_t;
        let v: *mut MdfdVec;
        let segnum_start: c_int;
        let segnum_end: c_int;

        v = _mdfd_getseg(
            reln,
            forknum,
            blocknum,
            true, /* not used */
            EXTENSION_DONT_OPEN,
        );

        // We might be flushing buffers of already removed relations, that's
        // ok, just ignore that case.  If the segment file wasn't open already
        // (ie from a recent mdwrite()), then we don't want to re-open it, to
        // avoid a race with PROCSIGNAL_BARRIER_SMGRRELEASE that might leave
        // us with a descriptor to a file that is about to be unlinked.
        if v.is_null() {
            return;
        }

        /* compute offset inside the current segment */
        segnum_start = (blocknum / RELSEG_SIZE as BlockNumber) as c_int;

        /* compute number of desired writes within the current segment */
        segnum_end = ((blocknum + nblocks - 1) / RELSEG_SIZE as BlockNumber) as c_int;
        if segnum_start != segnum_end {
            nflush = RELSEG_SIZE as BlockNumber - (blocknum % (RELSEG_SIZE as BlockNumber));
        }

        Assert!(nflush >= 1);
        Assert!(nflush <= nblocks);

        seekpos = (BLCKSZ as off_t) * (blocknum % (RELSEG_SIZE as BlockNumber)) as off_t;

        FileWriteback(
            (*v).mdfd_vfd,
            seekpos,
            (BLCKSZ as off_t) * nflush as off_t,
            WAIT_EVENT_DATA_FILE_FLUSH,
        );

        nblocks -= nflush;
        blocknum += nflush;
    }
}

// mdnblocks() -- Get the number of blocks stored in a relation.
//
// Important side effect: all active segments of the relation are opened
// and added to the md_seg_fds array.  If this routine has not been
// called, then only segments up to the last one actually touched
// are present in the array.
pub unsafe fn mdnblocks(reln: SMgrRelation, forknum: ForkNumber) -> BlockNumber {
    let mut v: *mut MdfdVec;
    let mut nblocks: BlockNumber;
    let mut segno: BlockNumber;

    mdopenfork(reln, forknum, EXTENSION_FAIL);

    /* mdopen has opened the first segment */
    Assert!((*reln).md_num_open_segs[forknum as usize] > 0);

    // Start from the last open segments, to avoid redundant seeks.  We have
    // previously verified that these segments are exactly RELSEG_SIZE long,
    // and it's useless to recheck that each time.
    //
    // NOTE: this assumption could only be wrong if another backend has
    // truncated the relation.  We rely on higher code levels to handle that
    // scenario by closing and re-opening the md fd, which is handled via
    // relcache flush.
    segno = ((*reln).md_num_open_segs[forknum as usize] - 1) as BlockNumber;
    v = ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add(segno as usize);

    loop {
        nblocks = _mdnblocks(reln, forknum, v);
        if nblocks > RELSEG_SIZE as BlockNumber {
            elog!(FATAL, "segment too big");
        }
        if nblocks < RELSEG_SIZE as BlockNumber {
            return (segno * RELSEG_SIZE as BlockNumber) + nblocks;
        }

        // If segment is exactly RELSEG_SIZE, advance to next one.
        segno += 1;

        // We used to pass O_CREAT here, but that has the disadvantage that it
        // might create a segment which has vanished through some operating
        // system misadventure.  In such a case, creating the segment here
        // undermines _mdfd_getseg's attempts to notice and report an error
        // upon access to a missing segment.
        v = _mdfd_openseg(reln, forknum, segno, 0);
        if v.is_null() {
            return segno * RELSEG_SIZE as BlockNumber;
        }
    }
}

// mdtruncate() -- Truncate relation to specified number of blocks.
//
// Guaranteed not to allocate memory, so it can be used in a critical section.
//
// If nblocks > curnblk, the request is ignored when we are InRecovery,
// otherwise, an error is raised.
pub unsafe fn mdtruncate(
    reln: SMgrRelation,
    forknum: ForkNumber,
    curnblk: BlockNumber,
    nblocks: BlockNumber,
) {
    let mut priorblocks: BlockNumber;
    let mut curopensegs: c_int;

    if nblocks > curnblk {
        /* Bogus request ... but no complaint if InRecovery */
        if InRecovery {
            return;
        }
        ereport!(
            ERROR,
            errmsg!(
                "could not truncate file \"{}\" to {} blocks: it's only {} blocks now",
                CStr::from_ptr(relpath((*reln).smgr_rlocator, forknum).str.as_ptr())
                    .to_string_lossy(),
                nblocks,
                curnblk
            )
        );
    }
    if nblocks == curnblk {
        return; /* no work */
    }

    // Truncate segments, starting at the last one. Starting at the end makes
    // managing the memory for the fd array easier, should there be errors.
    curopensegs = (*reln).md_num_open_segs[forknum as usize];
    while curopensegs > 0 {
        let v: *mut MdfdVec;

        priorblocks = (curopensegs - 1) as BlockNumber * RELSEG_SIZE as BlockNumber;

        v = ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add((curopensegs - 1) as usize)
            as *mut MdfdVec;

        if priorblocks > nblocks {
            // This segment is no longer active. We truncate the file, but do
            // not delete it, for reasons explained in the header comments.
            if FileTruncate((*v).mdfd_vfd, 0, WAIT_EVENT_DATA_FILE_TRUNCATE) < 0 {
                // C also: errcode_for_file_access()
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not truncate file \"{}\": errno={}",
                        CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                        errno()
                    )
                );
            }

            if !SmgrIsTemp(reln) {
                register_dirty_segment(reln, forknum, v);
            }

            /* we never drop the 1st segment */
            Assert!(
                v != ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add(0)
            );

            FileClose((*v).mdfd_vfd);
            _fdvec_resize(reln, forknum, curopensegs - 1);
        } else if priorblocks + RELSEG_SIZE as BlockNumber > nblocks {
            // This is the last segment we want to keep. Truncate the file to
            // the right length. NOTE: if nblocks is exactly a multiple K of
            // RELSEG_SIZE, we will truncate the K+1st segment to 0 length but
            // keep it. This adheres to the invariant given in the header
            // comments.
            let lastsegblocks: BlockNumber = nblocks - priorblocks;

            if FileTruncate(
                (*v).mdfd_vfd,
                lastsegblocks as off_t * BLCKSZ as off_t,
                WAIT_EVENT_DATA_FILE_TRUNCATE,
            ) < 0
            {
                // C also: errcode_for_file_access()
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not truncate file \"{}\" to {} blocks: errno={}",
                        CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                        nblocks,
                        errno()
                    )
                );
            }
            if !SmgrIsTemp(reln) {
                register_dirty_segment(reln, forknum, v);
            }
        } else {
            // We still need this segment, so nothing to do for this and any
            // earlier segment.
            break;
        }
        curopensegs -= 1;
    }
}

// mdregistersync() -- Mark whole relation as needing fsync
pub unsafe fn mdregistersync(reln: SMgrRelation, forknum: ForkNumber) {
    let mut segno: c_int;
    let min_inactive_seg: c_int;

    // NOTE: mdnblocks makes sure we have opened all active segments, so that
    // the loop below will get them all!
    mdnblocks(reln, forknum);

    segno = (*reln).md_num_open_segs[forknum as usize];
    min_inactive_seg = segno;

    // Temporarily open inactive segments, then close them after sync.  There
    // may be some inactive segments left opened after error, but that is
    // harmless.  We don't bother to clean them up and take a risk of further
    // trouble.  The next mdclose() will soon close them.
    while !_mdfd_openseg(reln, forknum, segno as BlockNumber, 0).is_null() {
        segno += 1;
    }

    while segno > 0 {
        let v: *mut MdfdVec =
            ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add((segno - 1) as usize)
                as *mut MdfdVec;

        register_dirty_segment(reln, forknum, v);

        /* Close inactive segments immediately */
        if segno > min_inactive_seg {
            FileClose((*v).mdfd_vfd);
            _fdvec_resize(reln, forknum, segno - 1);
        }

        segno -= 1;
    }
}

// mdimmedsync() -- Immediately sync a relation to stable storage.
//
// Note that only writes already issued are synced; this routine knows
// nothing of dirty buffers that may exist inside the buffer manager.  We
// sync active and inactive segments; smgrDoPendingSyncs() relies on this.
pub unsafe fn mdimmedsync(reln: SMgrRelation, forknum: ForkNumber) {
    let mut segno: c_int;
    let min_inactive_seg: c_int;

    // NOTE: mdnblocks makes sure we have opened all active segments, so that
    // the loop below will get them all!
    mdnblocks(reln, forknum);

    segno = (*reln).md_num_open_segs[forknum as usize];
    min_inactive_seg = segno;

    // Temporarily open inactive segments, then close them after sync.  There
    // may be some inactive segments left opened after fsync() error, but that
    // is harmless.  We don't bother to clean them up and take a risk of
    // further trouble.  The next mdclose() will soon close them.
    while !_mdfd_openseg(reln, forknum, segno as BlockNumber, 0).is_null() {
        segno += 1;
    }

    while segno > 0 {
        let v: *mut MdfdVec =
            ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add((segno - 1) as usize)
                as *mut MdfdVec;

        // fsyncs done through mdimmedsync() should be tracked in a separate
        // IOContext than those done through mdsyncfiletag() to differentiate
        // between unavoidable client backend fsyncs (e.g. those done during
        // index build) and those which ideally would have been done by the
        // checkpointer.
        if FileSync((*v).mdfd_vfd, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) < 0 {
            // C also: errcode_for_file_access()
            ereport!(
                data_sync_elevel(ERROR),
                errmsg!(
                    "could not fsync file \"{}\": errno={}",
                    CStr::from_ptr(FilePathName((*v).mdfd_vfd)).to_string_lossy(),
                    errno()
                )
            );
        }

        /* Close inactive segments immediately */
        if segno > min_inactive_seg {
            FileClose((*v).mdfd_vfd);
            _fdvec_resize(reln, forknum, segno - 1);
        }

        segno -= 1;
    }
}

pub unsafe fn mdfd(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    off: *mut uint32,
) -> c_int {
    let mut v: *mut MdfdVec = mdopenfork(reln, forknum, EXTENSION_FAIL);

    v = _mdfd_getseg(reln, forknum, blocknum, false, EXTENSION_FAIL);

    *off = ((BLCKSZ as off_t) * (blocknum % (RELSEG_SIZE as BlockNumber)) as off_t) as uint32;

    Assert!((*off as off_t) < (BLCKSZ as off_t) * RELSEG_SIZE as off_t);

    FileGetRawDesc((*v).mdfd_vfd)
}

// register_dirty_segment() -- Mark a relation segment as needing fsync
//
// If there is a local pending-ops table, just make an entry in it for
// ProcessSyncRequests to process later.  Otherwise, try to pass off the
// fsync request to the checkpointer process.  If that fails, just do the
// fsync locally before returning (we hope this will not happen often
// enough to be a performance problem).
unsafe fn register_dirty_segment(reln: SMgrRelation, forknum: ForkNumber, seg: *mut MdfdVec) {
    let mut tag: FileTag = core::mem::zeroed();

    INIT_MD_FILETAG(
        &mut tag,
        (*reln).smgr_rlocator.locator,
        forknum,
        (*seg).mdfd_segno,
    );

    /* Temp relations should never be fsync'd */
    Assert!(!SmgrIsTemp(reln));

    if !RegisterSyncRequest(&tag, SYNC_REQUEST, false /* retryOnError */) {
        let io_start: instr_time;

        ereport!(
            DEBUG1,
            errmsg_internal!("could not forward fsync request because request queue is full")
        );

        io_start = pgstat_prepare_io_time(track_io_timing);

        if FileSync((*seg).mdfd_vfd, WAIT_EVENT_DATA_FILE_SYNC) < 0 {
            // C also: errcode_for_file_access()
            ereport!(
                data_sync_elevel(ERROR),
                errmsg!(
                    "could not fsync file \"{}\": errno={}",
                    CStr::from_ptr(FilePathName((*seg).mdfd_vfd)).to_string_lossy(),
                    errno()
                )
            );
        }

        // We have no way of knowing if the current IOContext is
        // IOCONTEXT_NORMAL or IOCONTEXT_[BULKREAD, BULKWRITE, VACUUM] at this
        // point, so count the fsync as being in the IOCONTEXT_NORMAL
        // IOContext.
        pgstat_count_io_op_time(
            IOOBJECT_RELATION,
            IOCONTEXT_NORMAL,
            IOOP_FSYNC,
            io_start,
            1,
            0,
        );
    }
}

// register_unlink_segment() -- Schedule a file to be deleted after next checkpoint
unsafe fn register_unlink_segment(
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    segno: BlockNumber,
) {
    let mut tag: FileTag = core::mem::zeroed();

    INIT_MD_FILETAG(&mut tag, rlocator.locator, forknum, segno);

    /* Should never be used with temp relations */
    Assert!(!RelFileLocatorBackendIsTemp(&rlocator));

    RegisterSyncRequest(&tag, SYNC_UNLINK_REQUEST, true /* retryOnError */);
}

// register_forget_request() -- forget any fsyncs for a relation fork's segment
unsafe fn register_forget_request(
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    segno: BlockNumber,
) {
    let mut tag: FileTag = core::mem::zeroed();

    INIT_MD_FILETAG(&mut tag, rlocator.locator, forknum, segno);

    RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true /* retryOnError */);
}

// ForgetDatabaseSyncRequests -- forget any fsyncs and unlinks for a DB
pub unsafe fn ForgetDatabaseSyncRequests(dbid: Oid) {
    let mut tag: FileTag = core::mem::zeroed();
    let mut rlocator: RelFileLocator = core::mem::zeroed();

    rlocator.dbOid = dbid;
    rlocator.spcOid = 0;
    rlocator.relNumber = 0;

    INIT_MD_FILETAG(&mut tag, rlocator, InvalidForkNumber, InvalidBlockNumber);

    RegisterSyncRequest(&tag, SYNC_FILTER_REQUEST, true /* retryOnError */);
}

// DropRelationFiles -- drop files of all given relations
pub unsafe fn DropRelationFiles(delrels: *mut RelFileLocator, ndelrels: c_int, isRedo: bool) {
    let srels: *mut SMgrRelation;
    let mut i: c_int;

    srels =
        palloc(core::mem::size_of::<SMgrRelation>() * ndelrels as usize) as *mut SMgrRelation;
    i = 0;
    while i < ndelrels {
        let srel: SMgrRelation = smgropen(*delrels.add(i as usize), INVALID_PROC_NUMBER);

        if isRedo {
            let mut fork: ForkNumber = 0;
            while fork <= MAX_FORKNUM {
                XLogDropRelation(*delrels.add(i as usize), fork);
                fork += 1;
            }
        }
        *srels.add(i as usize) = srel;
        i += 1;
    }

    smgrdounlinkall(srels, ndelrels, isRedo);

    i = 0;
    while i < ndelrels {
        smgrclose(*srels.add(i as usize));
        i += 1;
    }
    pfree(srels as *mut c_void);
}

// _fdvec_resize() -- Resize the fork's open segments array
unsafe fn _fdvec_resize(reln: SMgrRelation, forknum: ForkNumber, nseg: c_int) {
    if nseg == 0 {
        if (*reln).md_num_open_segs[forknum as usize] > 0 {
            pfree((*reln).md_seg_fds[forknum as usize] as *mut c_void);
            (*reln).md_seg_fds[forknum as usize] = null_mut();
        }
    } else if (*reln).md_num_open_segs[forknum as usize] == 0 {
        (*reln).md_seg_fds[forknum as usize] = MemoryContextAlloc(
            MdCxt,
            core::mem::size_of::<MdfdVec>() * nseg as usize,
        ) as *mut _;
    } else if nseg > (*reln).md_num_open_segs[forknum as usize] {
        // It doesn't seem worthwhile complicating the code to amortize
        // repalloc() calls.  Those are far faster than PathNameOpenFile() or
        // FileClose(), and the memory context internally will sometimes avoid
        // doing an actual reallocation.
        (*reln).md_seg_fds[forknum as usize] = repalloc(
            (*reln).md_seg_fds[forknum as usize] as *mut c_void,
            core::mem::size_of::<MdfdVec>() * nseg as usize,
        ) as *mut _;
    } else {
        // We don't reallocate a smaller array, because we want mdtruncate()
        // to be able to promise that it won't allocate memory, so that it is
        // allowed in a critical section.  This means that a bit of space in
        // the array is now wasted, until the next time we add a segment and
        // reallocate.
    }

    (*reln).md_num_open_segs[forknum as usize] = nseg;
}

// Return the filename for the specified segment of the relation. The
// returned string is palloc'd.
unsafe fn _mdfd_segpath(reln: SMgrRelation, forknum: ForkNumber, segno: BlockNumber) -> MdPathStr {
    let path: RelPathStr;
    let mut fullpath: MdPathStr = core::mem::zeroed();

    path = relpath((*reln).smgr_rlocator, forknum);

    if segno > 0 {
        sprintf(
            fullpath.str.as_mut_ptr(),
            c"%s.%u".as_ptr(),
            path.str.as_ptr(),
            segno as c_uint,
        );
    } else {
        strcpy(fullpath.str.as_mut_ptr(), path.str.as_ptr());
    }

    fullpath
}

// Open the specified segment of the relation,
// and make a MdfdVec object for it.  Returns NULL on failure.
unsafe fn _mdfd_openseg(
    reln: SMgrRelation,
    forknum: ForkNumber,
    segno: BlockNumber,
    oflags: c_int,
) -> *mut MdfdVec {
    let v: *mut MdfdVec;
    let fd: File;
    let fullpath: MdPathStr;

    fullpath = _mdfd_segpath(reln, forknum, segno);

    /* open the file */
    fd = PathNameOpenFile(fullpath.str.as_ptr(), _mdfd_open_flags() | oflags);

    if fd < 0 {
        return null_mut();
    }

    // Segments are always opened in order from lowest to highest, so we must
    // be adding a new one at the end.
    Assert!(segno == (*reln).md_num_open_segs[forknum as usize] as BlockNumber);

    _fdvec_resize(reln, forknum, segno as c_int + 1);

    /* fill the entry */
    v = ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add(segno as usize);
    (*v).mdfd_vfd = fd;
    (*v).mdfd_segno = segno;

    Assert!(_mdnblocks(reln, forknum, v) <= RELSEG_SIZE as BlockNumber);

    /* all done */
    v
}

// _mdfd_getseg() -- Find the segment of the relation holding the
//					 specified block.
//
// If the segment doesn't exist, we ereport, return NULL, or create the
// segment, according to "behavior".  Note: skipFsync is only used in the
// EXTENSION_CREATE case.
unsafe fn _mdfd_getseg(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blkno: BlockNumber,
    skipFsync: bool,
    behavior: c_int,
) -> *mut MdfdVec {
    let mut v: *mut MdfdVec;
    let targetseg: BlockNumber;
    let mut nextsegno: BlockNumber;

    /* some way to handle non-existent segments needs to be specified */
    Assert!(
        behavior
            & (EXTENSION_FAIL | EXTENSION_CREATE | EXTENSION_RETURN_NULL | EXTENSION_DONT_OPEN)
            != 0
    );

    targetseg = blkno / (RELSEG_SIZE as BlockNumber);

    /* if an existing and opened segment, we're done */
    if targetseg < (*reln).md_num_open_segs[forknum as usize] as BlockNumber {
        v = ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec).add(targetseg as usize)
            as *mut MdfdVec;
        return v;
    }

    /* The caller only wants the segment if we already had it open. */
    if behavior & EXTENSION_DONT_OPEN != 0 {
        return null_mut();
    }

    // The target segment is not yet open. Iterate over all the segments
    // between the last opened and the target segment. This way missing
    // segments either raise an error, or get created (according to
    // 'behavior'). Start with either the last opened, or the first segment if
    // none was opened before.
    if (*reln).md_num_open_segs[forknum as usize] > 0 {
        v = ((*reln).md_seg_fds[forknum as usize] as *mut MdfdVec)
            .add(((*reln).md_num_open_segs[forknum as usize] - 1) as usize);
    } else {
        v = mdopenfork(reln, forknum, behavior);
        if v.is_null() {
            return null_mut(); /* if behavior & EXTENSION_RETURN_NULL */
        }
    }

    nextsegno = (*reln).md_num_open_segs[forknum as usize] as BlockNumber;
    while nextsegno <= targetseg {
        let nblocks: BlockNumber = _mdnblocks(reln, forknum, v);
        let mut flags: c_int = 0;

        Assert!(nextsegno == (*v).mdfd_segno + 1);

        if nblocks > RELSEG_SIZE as BlockNumber {
            elog!(FATAL, "segment too big");
        }

        if (behavior & EXTENSION_CREATE) != 0
            || (InRecovery && (behavior & EXTENSION_CREATE_RECOVERY) != 0)
        {
            // Normally we will create new segments only if authorized by the
            // caller (i.e., we are doing mdextend()).  But when doing WAL
            // recovery, create segments anyway; this allows cases such as
            // replaying WAL data that has a write into a high-numbered
            // segment of a relation that was later deleted.
            //
            // We have to maintain the invariant that segments before the last
            // active segment are of size RELSEG_SIZE; therefore, if
            // extending, pad them out with zeroes if needed.
            if nblocks < RELSEG_SIZE as BlockNumber {
                let zerobuf: *mut c_char =
                    palloc_aligned(BLCKSZ, PG_IO_ALIGN_SIZE, MCXT_ALLOC_ZERO) as *mut c_char;

                mdextend(
                    reln,
                    forknum,
                    nextsegno * RELSEG_SIZE as BlockNumber - 1,
                    zerobuf as *const c_void,
                    skipFsync,
                );
                pfree(zerobuf as *mut c_void);
            }
            flags = O_CREAT;
        } else if nblocks < RELSEG_SIZE as BlockNumber {
            // When not extending, only open the next segment if the current
            // one is exactly RELSEG_SIZE.  If not (this branch), either
            // return NULL or fail.
            if behavior & EXTENSION_RETURN_NULL != 0 {
                // Some callers discern between reasons for _mdfd_getseg()
                // returning NULL based on errno. As there's no failing
                // syscall involved in this case, explicitly set errno to
                // ENOENT, as that seems the closest interpretation.
                set_errno(ENOENT);
                return null_mut();
            }

            // C also: errcode_for_file_access()
            ereport!(
                ERROR,
                errmsg!(
                    "could not open file \"{}\" (target block {}): previous segment is only {} blocks",
                    CStr::from_ptr(_mdfd_segpath(reln, forknum, nextsegno).str.as_ptr())
                        .to_string_lossy(),
                    blkno,
                    nblocks
                )
            );
        }

        v = _mdfd_openseg(reln, forknum, nextsegno, flags);

        if v.is_null() {
            if (behavior & EXTENSION_RETURN_NULL) != 0 && FILE_POSSIBLY_DELETED(errno()) {
                return null_mut();
            }
            // C also: errcode_for_file_access()
            ereport!(
                ERROR,
                errmsg!(
                    "could not open file \"{}\" (target block {}): errno={}",
                    CStr::from_ptr(_mdfd_segpath(reln, forknum, nextsegno).str.as_ptr())
                        .to_string_lossy(),
                    blkno,
                    errno()
                )
            );
        }

        nextsegno += 1;
    }

    v
}

// Get number of blocks present in a single disk file
unsafe fn _mdnblocks(_reln: SMgrRelation, _forknum: ForkNumber, seg: *mut MdfdVec) -> BlockNumber {
    let len: off_t;

    len = FileSize((*seg).mdfd_vfd);
    if len < 0 {
        // C also: errcode_for_file_access()
        ereport!(
            ERROR,
            errmsg!(
                "could not seek to end of file \"{}\": errno={}",
                CStr::from_ptr(FilePathName((*seg).mdfd_vfd)).to_string_lossy(),
                errno()
            )
        );
    }
    /* note that this calculation will ignore any partial block at EOF */
    (len / BLCKSZ as off_t) as BlockNumber
}

// Sync a file to disk, given a file tag.  Write the path into an output
// buffer so the caller can use it in error messages.
//
// Return 0 on success, -1 on failure, with errno set.
pub unsafe fn mdsyncfiletag(ftag: *const FileTag, path: *mut c_char) -> c_int {
    let reln: SMgrRelation = smgropen((*ftag).rlocator, INVALID_PROC_NUMBER);
    let file: File;
    let io_start: instr_time;
    let need_to_close: bool;
    let result: c_int;
    let save_errno: c_int;

    /* See if we already have the file open, or need to open it. */
    if ((*ftag).segno as c_int) < (*reln).md_num_open_segs[(*ftag).forknum as usize] {
        file = (*((*reln).md_seg_fds[(*ftag).forknum as usize] as *mut MdfdVec)
            .add((*ftag).segno as usize))
        .mdfd_vfd;
        strlcpy(path, FilePathName(file), MAXPGPATH);
        need_to_close = false;
    } else {
        let p: MdPathStr;

        p = _mdfd_segpath(reln, (*ftag).forknum as ForkNumber, (*ftag).segno as BlockNumber);
        strlcpy(path, p.str.as_ptr(), MD_PATH_STR_MAXLEN);

        file = PathNameOpenFile(path, _mdfd_open_flags());
        if file < 0 {
            return -1;
        }
        need_to_close = true;
    }

    io_start = pgstat_prepare_io_time(track_io_timing);

    /* Sync the file. */
    result = FileSync(file, WAIT_EVENT_DATA_FILE_SYNC);
    save_errno = errno();

    if need_to_close {
        FileClose(file);
    }

    pgstat_count_io_op_time(
        IOOBJECT_RELATION,
        IOCONTEXT_NORMAL,
        IOOP_FSYNC,
        io_start,
        1,
        0,
    );

    set_errno(save_errno);
    result
}

// Unlink a file, given a file tag.  Write the path into an output
// buffer so the caller can use it in error messages.
//
// Return 0 on success, -1 on failure, with errno set.
pub unsafe fn mdunlinkfiletag(ftag: *const FileTag, path: *mut c_char) -> c_int {
    let p: RelPathStr;

    /* Compute the path. */
    p = relpathperm((*ftag).rlocator, MAIN_FORKNUM);
    strlcpy(path, p.str.as_ptr(), MAXPGPATH);

    /* Try to unlink the file. */
    unlink(path)
}

// Check if a given candidate request matches a given tag, when processing
// a SYNC_FILTER_REQUEST request.  This will be called for all pending
// requests to find out whether to forget them.
pub unsafe fn mdfiletagmatches(ftag: *const FileTag, candidate: *const FileTag) -> bool {
    // For now we only use filter requests as a way to drop all scheduled
    // callbacks relating to a given database, when dropping the database.
    // We'll return true for all candidates that have the same database OID as
    // the ftag from the SYNC_FILTER_REQUEST request, so they're forgotten.
    (*ftag).rlocator.dbOid == (*candidate).rlocator.dbOid
}

// AIO completion callback for mdstartreadv().
unsafe fn md_readv_complete(
    ioh: *mut PgAioHandle,
    prior_result: PgAioResult,
    _cb_data: uint8,
) -> PgAioResult {
    let td: *mut PgAioTargetData = pgaio_io_get_target_data(ioh);
    let mut result: PgAioResult = prior_result;

    if prior_result.result < 0 {
        result.set_status(PGAIO_RS_ERROR as uint32);
        result.set_id(PGAIO_HCB_MD_READV as uint32);
        /* For "hard" errors, track the error number in error_data */
        result.set_error_data((-prior_result.result) as uint32);
        result.result = 0;

        // Immediately log a message about the IO error, but only to the
        // server log.
        pgaio_result_report(result, td, LOG_SERVER_ONLY);

        return result;
    }

    // As explained above smgrstartreadv(), the smgr API operates on the level
    // of blocks, rather than bytes. Convert.
    result.result /= BLCKSZ as int32;

    Assert!(result.result <= (*td).smgr.nblocks as int32);

    if result.result == 0 {
        /* consider 0 blocks read a failure */
        result.set_status(PGAIO_RS_ERROR as uint32);
        result.set_id(PGAIO_HCB_MD_READV as uint32);
        result.set_error_data(0);

        /* see comment above the "hard error" case */
        pgaio_result_report(result, td, LOG_SERVER_ONLY);

        return result;
    }

    if result.status() != PGAIO_RS_ERROR as uint32
        && result.result < (*td).smgr.nblocks as int32
    {
        /* partial reads should be retried at upper level */
        result.set_status(PGAIO_RS_PARTIAL as uint32);
        result.set_id(PGAIO_HCB_MD_READV as uint32);
    }

    result
}

// AIO error reporting callback for mdstartreadv().
//
// Errors are encoded as follows:
// - PgAioResult.error_data != 0 encodes IO that failed with that errno
// - PgAioResult.error_data == 0 encodes IO that didn't read all data
unsafe fn md_readv_report(result: PgAioResult, td: *const PgAioTargetData, elevel: c_int) {
    let path: RelPathStr;

    path = relpathbackend(
        (*td).smgr.rlocator,
        if (*td).smgr.is_temp() {
            MyProcNumber
        } else {
            INVALID_PROC_NUMBER
        },
        (*td).smgr.forkNum(),
    );

    if result.error_data() != 0 {
        /* for errcode_for_file_access() and %m */
        set_errno(result.error_data() as c_int);

        // C also: errcode_for_file_access()
        ereport!(
            elevel,
            errmsg!(
                "could not read blocks {}..{} in file \"{}\": errno={}",
                (*td).smgr.blockNum,
                (*td).smgr.blockNum + (*td).smgr.nblocks - 1,
                CStr::from_ptr(path.str.as_ptr()).to_string_lossy(),
                errno()
            )
        );
    } else {
        // NB: This will typically only be output in debug messages, while
        // retrying a partial IO.
        // C also: errcode(ERRCODE_DATA_CORRUPTED)
        ereport!(
            elevel,
            errmsg!(
                "could not read blocks {}..{} in file \"{}\": read only {} of {} bytes",
                (*td).smgr.blockNum,
                (*td).smgr.blockNum + (*td).smgr.nblocks - 1,
                CStr::from_ptr(path.str.as_ptr()).to_string_lossy(),
                result.result as usize * BLCKSZ,
                (*td).smgr.nblocks as usize * BLCKSZ
            )
        );
    }
}

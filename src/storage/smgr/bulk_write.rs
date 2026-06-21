//! storage/smgr/bulk_write.c - efficiently and reliably populate a new relation.
//!
//! The assumption is that no other backends access the relation while we are
//! loading it, so we can take some shortcuts.  We bypass the buffer manager to
//! avoid the locking overhead, call smgrextend() directly, WAL-log batches of
//! pages, and register the relation for fsync at the next checkpoint ourselves.

use crate::prelude::*;

use crate::access::transam::xlogrecord::XLR_MAX_BLOCK_ID;
use crate::common::relpath::{ForkNumber, INIT_FORKNUM};
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{Page, PageSetChecksumInplace};
use crate::storage::buf_internals::SMgrRelation;
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::mmgr::mcxt::MemoryContextAllocAligned;
use crate::utils::rel::Relation;

use crate::pg_config::{BLCKSZ, PG_IO_ALIGN_SIZE};

// access/xlogdefs.h: XLogRecPtr.
// TODO: dedup (also defined in access/transam/xlogreader.rs).
type XLogRecPtr = uint64;

// #define MAX_PENDING_WRITES XLR_MAX_BLOCK_ID
const MAX_PENDING_WRITES: usize = XLR_MAX_BLOCK_ID as usize;

/*
 * storage/bufpage.h: PGIOAlignedBlock - a BLCKSZ-sized buffer aligned for
 * direct I/O.  BulkWriteBuffer is a pointer to one of these.
 */
#[repr(C, align(4096))]
pub struct PGIOAlignedBlock {
    pub data: [c_char; BLCKSZ],
}

pub type BulkWriteBuffer = *mut PGIOAlignedBlock;

// static const PGIOAlignedBlock zero_buffer = {0}; (worth BLCKSZ)
static zero_buffer: PGIOAlignedBlock = PGIOAlignedBlock {
    data: [0; BLCKSZ],
};

#[repr(C)]
struct PendingWrite {
    buf: BulkWriteBuffer,
    blkno: BlockNumber,
    page_std: bool,
}

/*
 * Bulk writer state for one relation fork.
 */
#[repr(C)]
pub struct BulkWriteState {
    /* Information about the target relation we're writing */
    smgr: SMgrRelation,
    forknum: ForkNumber,
    use_wal: bool,

    /* We keep several writes queued, and WAL-log them in batches */
    npending: c_int,
    pending_writes: [PendingWrite; MAX_PENDING_WRITES],

    /* Current size of the relation */
    relsize: BlockNumber,

    /* The RedoRecPtr at the time that the bulk operation started */
    start_RedoRecPtr: XLogRecPtr,

    memcxt: MemoryContext,
}

/*
 * Start a bulk write operation on a relation fork.
 */
#[no_mangle]
pub unsafe fn smgr_bulk_start_rel(rel: Relation, forknum: ForkNumber) -> *mut BulkWriteState {
    smgr_bulk_start_smgr(
        RelationGetSmgr(rel),
        forknum,
        RelationNeedsWAL(rel) || forknum == INIT_FORKNUM,
    )
}

/*
 * Start a bulk write operation on a relation fork.
 *
 * This is like smgr_bulk_start_rel, but can be used without a relcache entry.
 */
pub unsafe fn smgr_bulk_start_smgr(
    smgr: SMgrRelation,
    forknum: ForkNumber,
    use_wal: bool,
) -> *mut BulkWriteState {
    let state: *mut BulkWriteState;

    state = palloc(core::mem::size_of::<BulkWriteState>()) as *mut BulkWriteState;
    (*state).smgr = smgr;
    (*state).forknum = forknum;
    (*state).use_wal = use_wal;

    (*state).npending = 0;
    (*state).relsize = smgrnblocks(smgr, forknum);

    (*state).start_RedoRecPtr = GetRedoRecPtr();

    /*
     * Remember the memory context.  We will use it to allocate all the
     * buffers later.
     */
    (*state).memcxt = CurrentMemoryContext;

    state
}

/*
 * Finish bulk write operation.
 *
 * This WAL-logs and flushes any remaining pending writes to disk, and fsyncs
 * the relation if needed.
 */
#[no_mangle]
pub unsafe fn smgr_bulk_finish(bulkstate: *mut BulkWriteState) {
    /* WAL-log and flush any remaining pages */
    smgr_bulk_flush(bulkstate);

    /*
     * Fsync the relation, or register it for the next checkpoint, if
     * necessary.
     */
    if SmgrIsTemp((*bulkstate).smgr) {
        /* Temporary relations don't need to be fsync'd, ever */
    } else if !(*bulkstate).use_wal {
        /*----------
         * This is either an unlogged relation, or a permanent relation but we
         * skipped WAL-logging because wal_level=minimal.  See the original C
         * comment for the full reasoning; conservatively assume it's an
         * unlogged relation and register it for sync.
         */
        smgrregistersync((*bulkstate).smgr, (*bulkstate).forknum);
    } else {
        /*
         * Permanent relation, WAL-logged normally.
         *
         * We already WAL-logged all the pages, so they will be replayed from
         * WAL on crash.  However, when we wrote out the pages, we passed
         * skipFsync=true to avoid the overhead of registering all the writes
         * with the checkpointer.  Register the whole relation now.
         *
         * There is one hole in that idea: if a checkpoint occurred while we
         * were writing the pages, it already missed fsyncing the pages we had
         * written before the checkpoint started.  So if a checkpoint started
         * after the bulk write, fsync the files now.
         */

        /*
         * Prevent a checkpoint from starting between the GetRedoRecPtr() and
         * smgrregistersync() calls.
         */
        Assert!((((*MyProc).delayChkptFlags & DELAY_CHKPT_START) == 0));
        (*MyProc).delayChkptFlags |= DELAY_CHKPT_START;

        if (*bulkstate).start_RedoRecPtr != GetRedoRecPtr() {
            /*
             * A checkpoint occurred and it didn't know about our writes, so
             * fsync() the relation ourselves.
             */
            (*MyProc).delayChkptFlags &= !DELAY_CHKPT_START;
            smgrimmedsync((*bulkstate).smgr, (*bulkstate).forknum);
            elog!(
                DEBUG1,
                "flushed relation because a checkpoint occurred concurrently"
            );
        } else {
            smgrregistersync((*bulkstate).smgr, (*bulkstate).forknum);
            (*MyProc).delayChkptFlags &= !DELAY_CHKPT_START;
        }
    }
}

unsafe extern "C" fn buffer_cmp(a: *const c_void, b: *const c_void) -> c_int {
    let bufa = a as *const PendingWrite;
    let bufb = b as *const PendingWrite;

    /* We should not see duplicated writes for the same block */
    Assert!((*bufa).blkno != (*bufb).blkno);
    if (*bufa).blkno > (*bufb).blkno {
        1
    } else {
        -1
    }
}

/*
 * Finish all the pending writes.
 */
unsafe fn smgr_bulk_flush(bulkstate: *mut BulkWriteState) {
    let npending: c_int = (*bulkstate).npending;
    let pending_writes: *mut PendingWrite = (*bulkstate).pending_writes.as_mut_ptr();

    if npending == 0 {
        return;
    }

    if npending > 1 {
        qsort(
            pending_writes as *mut c_void,
            npending as Size,
            core::mem::size_of::<PendingWrite>() as Size,
            Some(buffer_cmp),
        );
    }

    if (*bulkstate).use_wal {
        let mut blknos: [BlockNumber; MAX_PENDING_WRITES] = [0; MAX_PENDING_WRITES];
        let mut pages: [Page; MAX_PENDING_WRITES] = [null_mut(); MAX_PENDING_WRITES];
        let mut page_std: bool = true;

        for i in 0..npending as usize {
            blknos[i] = (*pending_writes.add(i)).blkno;
            pages[i] = (*(*pending_writes.add(i)).buf).data.as_mut_ptr();

            /*
             * If any of the pages use !page_std, we log them all as such.
             * That's a bit wasteful, but in practice, a mix of standard and
             * non-standard page layout is rare.  None of the built-in AMs do
             * that.
             */
            if !(*pending_writes.add(i)).page_std {
                page_std = false;
            }
        }
        log_newpages(
            &mut (*((*bulkstate).smgr as *mut SMgrRelationData)).smgr_rlocator.locator,
            (*bulkstate).forknum,
            npending,
            blknos.as_mut_ptr(),
            pages.as_mut_ptr(),
            page_std,
        );
    }

    for i in 0..npending as usize {
        let blkno: BlockNumber = (*pending_writes.add(i)).blkno;
        let page: Page = (*(*pending_writes.add(i)).buf).data.as_mut_ptr();

        PageSetChecksumInplace(page, blkno);

        if blkno >= (*bulkstate).relsize {
            /*
             * If we have to write pages nonsequentially, fill in the space
             * with zeroes until we come back and overwrite.  This is not
             * logically necessary on standard Unix filesystems (unwritten
             * space will read as zeroes anyway), but it should help to avoid
             * fragmentation.  The dummy pages aren't WAL-logged though.
             */
            while blkno > (*bulkstate).relsize {
                /* don't set checksum for all-zero page */
                smgrextend(
                    (*bulkstate).smgr,
                    (*bulkstate).forknum,
                    (*bulkstate).relsize,
                    &zero_buffer as *const PGIOAlignedBlock as *const c_void,
                    true,
                );
                (*bulkstate).relsize += 1;
            }

            smgrextend(
                (*bulkstate).smgr,
                (*bulkstate).forknum,
                blkno,
                page as *const c_void,
                true,
            );
            (*bulkstate).relsize += 1;
        } else {
            smgrwrite((*bulkstate).smgr, (*bulkstate).forknum, blkno, page, true);
        }
        crate::utils::mmgr::mcxt::pfree(page as *mut c_void);
    }

    (*bulkstate).npending = 0;
}

/*
 * Queue write of 'buf'.
 *
 * NB: this takes ownership of 'buf'!
 *
 * You are only allowed to write a given block once as part of one bulk write
 * operation.
 */
#[no_mangle]
pub unsafe fn smgr_bulk_write(
    bulkstate: *mut BulkWriteState,
    blocknum: BlockNumber,
    buf: BulkWriteBuffer,
    page_std: bool,
) {
    let w: *mut PendingWrite;

    w = &mut (*bulkstate).pending_writes[(*bulkstate).npending as usize];
    (*bulkstate).npending += 1;
    (*w).buf = buf;
    (*w).blkno = blocknum;
    (*w).page_std = page_std;

    if (*bulkstate).npending == MAX_PENDING_WRITES as c_int {
        smgr_bulk_flush(bulkstate);
    }
}

/*
 * Allocate a new buffer which can later be written with smgr_bulk_write().
 *
 * There is no function to free the buffer.  When you pass it to
 * smgr_bulk_write(), it takes ownership and frees it when it's no longer
 * needed.
 *
 * This is currently implemented as a simple palloc, but could be implemented
 * using a ring buffer or larger chunks in the future, so don't rely on it.
 */
#[no_mangle]
pub unsafe fn smgr_bulk_get_buf(bulkstate: *mut BulkWriteState) -> BulkWriteBuffer {
    MemoryContextAllocAligned(
        (*bulkstate).memcxt as *mut _,
        BLCKSZ as Size,
        PG_IO_ALIGN_SIZE as Size,
        0,
    ) as BulkWriteBuffer
}

// ---------------------------------------------------------------------------
// Local stubs for functions not yet ported.
// ---------------------------------------------------------------------------

/// storage/smgr.h: SMgrRelationData with the smgr_rlocator field accessed here.
/// The real definition is opaque in storage/buf_internals.rs; we provide a
/// minimal #[repr(C)]-compatible layout so smgr_rlocator.locator can be read.
// TODO: dedup once storage/smgr.c lands.
#[repr(C)]
struct SMgrRelationData {
    smgr_rlocator: RelFileLocatorBackend,
    // ... remaining fields omitted; only the locator is touched here.
}

// storage/relfilelocator.h: RelFileLocatorBackend (locator + backend).
// TODO: dedup with storage/relfilelocator.rs (re-stated here to lay out the
// SMgrRelationData prefix without making the opaque type concrete elsewhere).
#[repr(C)]
struct RelFileLocatorBackend {
    locator: RelFileLocator,
    backend: c_int,
}

// access/xlog.h: GetRedoRecPtr().
// TODO: port access/transam/xlog.c.
unsafe fn GetRedoRecPtr() -> XLogRecPtr {
    crate::access::transam::xlog::GetRedoRecPtr()
}

// access/xloginsert.h: log_newpages().
// TODO: port access/transam/xloginsert.c.
unsafe fn log_newpages(
    _rlocator: *mut RelFileLocator,
    _forknum: ForkNumber,
    _num_pages: c_int,
    _blknos: *mut BlockNumber,
    _pages: *mut Page,
    _page_std: bool,
) {
    crate::access::transam::xloginsert::log_newpages(
        _rlocator as _, _forknum as _, _num_pages, _blknos, _pages as _, _page_std,
    )
}

// storage/smgr.h: smgrnblocks().
// TODO: port storage/smgr.c.
unsafe fn smgrnblocks(_reln: SMgrRelation, _forknum: ForkNumber) -> BlockNumber {
    crate::storage::smgr::smgr::smgrnblocks(_reln as _, _forknum as _)
}

// storage/smgr.h: smgrextend().
// TODO: port storage/smgr.c.
unsafe fn smgrextend(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffer: *const c_void,
    _skipFsync: bool,
) {
    crate::storage::smgr::smgr::smgrextend(_reln as _, _forknum as _, _blocknum, _buffer as _, _skipFsync)
}

// storage/smgr.h: smgrwrite() (inline wrapper over smgrwritev).
// TODO: port storage/smgr.c.
unsafe fn smgrwrite(
    _reln: SMgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _buffer: *const c_char,
    _skipFsync: bool,
) {
    let mut bufs: [*const c_void; 1] = [_buffer as *const c_void];
    crate::storage::smgr::smgr::smgrwritev(_reln as _, _forknum as _, _blocknum, bufs.as_mut_ptr(), 1, _skipFsync)
}

// storage/smgr.h: smgrregistersync().
// TODO: port storage/smgr.c.
unsafe fn smgrregistersync(_reln: SMgrRelation, _forknum: ForkNumber) {
    crate::storage::smgr::smgr::smgrregistersync(_reln as _, _forknum as _)
}

// storage/smgr.h: smgrimmedsync().
// TODO: port storage/smgr.c.
unsafe fn smgrimmedsync(_reln: SMgrRelation, _forknum: ForkNumber) {
    crate::storage::smgr::smgr::smgrimmedsync(_reln as _, _forknum as _)
}

// storage/smgr.h: SmgrIsTemp().
// TODO: port storage/smgr.c.
unsafe fn SmgrIsTemp(_reln: SMgrRelation) -> bool {
    crate::storage::smgr::smgr::SmgrIsTemp(_reln as _)
}

// utils/rel.h: RelationGetSmgr().
// TODO: port the inline accessor from utils/rel.h.
unsafe fn RelationGetSmgr(_rel: Relation) -> SMgrRelation {
    crate::storage::buffer::bufmgr::RelationGetSmgr(_rel as _) as _
}

// utils/rel.h: RelationNeedsWAL().
// TODO: port the macro from utils/rel.h.
unsafe fn RelationNeedsWAL(_rel: Relation) -> bool {
    (*(*_rel).rd_rel).relpersistence == b'p' as i8
}

// stdlib.h: qsort().
// TODO: route to the libc/port qsort once available.
unsafe fn qsort(
    base: *mut c_void,
    nmemb: Size,
    size: Size,
    compar: Option<unsafe extern "C" fn(*const c_void, *const c_void) -> c_int>,
) {
    extern "C" {
        #[link_name = "qsort"]
        fn c_qsort(
            base: *mut c_void,
            nmemb: usize,
            size: usize,
            compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
        );
    }
    if let Some(f) = compar {
        c_qsort(base, nmemb as usize, size as usize, f);
    }
}

// storage/proc.h: MyProc (PGPROC*), and the delayChkptFlags bits.
// TODO: port storage/proc.c / proc.h.
#[repr(C)]
struct PGPROC {
    delayChkptFlags: c_int,
}

extern "C" { pub static mut MyProc: *mut PGPROC; }
// proc.h: DELAY_CHKPT_START.
const DELAY_CHKPT_START: c_int = 1 << 0;

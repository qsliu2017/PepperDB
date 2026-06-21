//! storage/buffer/localbuf.c
//!
//! local buffer manager. Fast buffer manager for temporary tables,
//! which never need to be WAL-logged or checkpointed, etc.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994-5, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/buffer/localbuf.c

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(static_mut_refs)]

use crate::prelude::*;

// #include "access/parallel.h"
// #include "executor/instrument.h"
// #include "pgstat.h"
// #include "storage/aio.h"
// #include "storage/buf_internals.h"
// #include "storage/bufmgr.h"
// #include "storage/fd.h"
// #include "utils/guc_hooks.h"
// #include "utils/memdebug.h"
// #include "utils/memutils.h"
// #include "utils/resowner.h"

use crate::storage::block::{BlockNumber, InvalidBlockNumber, MaxBlockNumber};
use crate::storage::buf::{Buffer, BufferIsLocal, InvalidBuffer};
use crate::storage::relfilelocator::RelFileLocator;
use crate::common::relpath::ForkNumber;
use crate::storage::aio_types::PgAioWaitRef;
use crate::port::atomics::pg_atomic_uint32;
use crate::storage::procnumber::MyProcNumber;
use crate::utils::resowner::resowner::{ResourceOwnerEnlarge, CurrentResourceOwner};

use crate::storage::buf_internals::{
    BufferDesc, BufferTag, LocalBufferDescriptors,
    GetLocalBufferDescriptor, BufferDescriptorGetBuffer,
    InitBufferTag, BufferTagsEqual, ClearBufferTag,
    BufTagGetRelFileLocator, BufTagGetForkNum, BufTagMatchesRelFileLocator,
    PrefetchBufferResult, SMgrRelation,
    BUF_FLAG_MASK, BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE, BUF_REFCOUNT_ONE,
    BM_TAG_VALID, BM_DIRTY, BM_VALID, BM_IO_ERROR, BM_JUST_DIRTIED,
    BM_MAX_USAGE_COUNT, BUF_STATE_GET_USAGECOUNT, BUF_STATE_GET_REFCOUNT,
};

use crate::utils::hash::dynahash::{
    HTAB, HASHCTL, hash_create, hash_search,
    HASH_FIND, HASH_ENTER, HASH_REMOVE, HASH_ELEM, HASH_BLOBS,
};

use crate::pg_config::BLCKSZ;
use crate::pg_config_manual::PG_IO_ALIGN_SIZE;

// ==========================================================================
// Stubs for unported dependencies
// ==========================================================================

// storage/bufmgr.h: BufferManagerRelation.  The C code here only accesses the
// `smgr` field, so model that minimally.
// TODO: dedup (storage/bufmgr.h not ported yet)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BufferManagerRelation {
    pub rel: *mut c_void,
    pub smgr: SMgrRelation,
    pub relpersistence: c_char,
}

// utils/guc.h: GucSource. Unused by check_temp_buffers.
// TODO: dedup (utils/guc.h not ported yet)
pub type GucSource = c_int;
pub const PGC_S_TEST: GucSource = 16;

// executor/instrument.h: instr_time (matches pgstat_io's instr_time = int64).
use crate::utils::activity::pgstat_io::instr_time;

/// pgstat.h: BufferUsage (only fields used here).
// TODO: dedup (pgstat.h / instrument.h not ported yet)
#[repr(C)]
pub struct BufferUsage {
    pub local_blks_written: i64,
    pub local_blks_dirtied: i64,
}

// pgstat.h: global pgBufferUsage accumulator.
// TODO: dedup
pub static mut pgBufferUsage: BufferUsage = BufferUsage {
    local_blks_written: 0,
    local_blks_dirtied: 0,
};

// pgstat.h: IOObject / IOContext / IOOp.  Use the canonical pgstat_io types so
// the values match the Assert()s inside pgstat_count_io_op().
use crate::utils::activity::pgstat_io::{
    IOObject, IOContext, IOOp,
    IOOBJECT_TEMP_RELATION, IOCONTEXT_NORMAL,
    IOOP_WRITE, IOOP_EXTEND, IOOP_EVICT,
};

// miscadmin.h: track_io_timing GUC.
// TODO: dedup
pub static mut track_io_timing: bool = false;

// guc.c / globals.c: num_temp_buffers GUC.
// TODO: dedup
pub static mut num_temp_buffers: c_int = 1024;

/// GUC_check_errdetail(...) - stages a detail string onto the in-flight GUC
/// check error.  No-op shim until guc.c is ported.
macro_rules! GUC_check_errdetail {
    ($($arg:tt)*) => {{
        let _detail: String = format!($($arg)*);
        let _ = _detail;
    }};
}

#[inline]
unsafe fn IsParallelWorker() -> bool {
    false // TODO: access/parallel.c
}

// ---- atomics (port/atomics.h) --------------------------------------------

#[inline]
unsafe fn pg_atomic_read_u32(ptr: *mut pg_atomic_uint32) -> u32 {
    crate::port::atomics::pg_atomic_read_u32_impl(&*ptr)
}

#[inline]
unsafe fn pg_atomic_unlocked_write_u32(ptr: *mut pg_atomic_uint32, val: u32) {
    crate::port::atomics::generic::pg_atomic_unlocked_write_u32_impl(&*ptr, val);
}

// ---- AIO wait refs (storage/aio_types.h) ---------------------------------
//
// BufferDesc.io_wref is aio_types::PgAioWaitRef; the canonical wref helpers
// take aio::aio::PgAioWaitRef.  The two structs share an identical 3x u32
// layout, so cast the pointer across.

#[inline]
unsafe fn pgaio_wref_valid(iow: *mut PgAioWaitRef) -> bool {
    crate::storage::aio::aio::pgaio_wref_valid(iow as *mut crate::storage::aio::aio::PgAioWaitRef)
}

#[inline]
unsafe fn pgaio_wref_wait(iow: *mut PgAioWaitRef) {
    crate::storage::aio::aio::pgaio_wref_wait(iow as *mut crate::storage::aio::aio::PgAioWaitRef)
}

#[inline]
unsafe fn pgaio_wref_clear(iow: *mut PgAioWaitRef) {
    crate::storage::aio::aio::pgaio_wref_clear(iow as *mut crate::storage::aio::aio::PgAioWaitRef)
}

// ---- smgr (storage/smgr.h) -----------------------------------------------

// storage/smgr.h: SMgrRelationData with the smgr_rlocator field accessed here.
// Mirror bulk_write.rs: minimal #[repr(C)] layout exposing smgr_rlocator.locator.
#[repr(C)]
struct SMgrRelationDataLocal {
    smgr_rlocator: RelFileLocatorBackend,
}

// storage/relfilelocator.h: RelFileLocatorBackend (locator + backend).
#[repr(C)]
struct RelFileLocatorBackend {
    locator: RelFileLocator,
    backend: c_int,
}

#[inline]
unsafe fn smgr_locator(smgr: SMgrRelation) -> *mut RelFileLocator {
    &mut (*(smgr as *mut SMgrRelationDataLocal)).smgr_rlocator.locator
}

#[inline]
unsafe fn smgr_rlocator_backend(smgr: SMgrRelation) -> *mut RelFileLocatorBackend {
    &mut (*(smgr as *mut SMgrRelationDataLocal)).smgr_rlocator
}

#[inline]
unsafe fn smgropen(rlocator: RelFileLocator, backend: c_int) -> SMgrRelation {
    crate::storage::smgr::smgr::smgropen(rlocator, backend as _) as _
}

#[inline]
unsafe fn smgrprefetch(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: c_int,
) -> bool {
    crate::storage::smgr::smgr::smgrprefetch(reln as _, forknum as _, blocknum, nblocks)
}

#[inline]
unsafe fn smgrwrite(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: *const c_void,
    skipFsync: bool,
) {
    let buffers: [*const c_void; 1] = [buffer];
    crate::storage::smgr::smgr::smgrwritev(
        reln as _,
        forknum as _,
        blocknum,
        buffers.as_ptr() as *mut *const c_void,
        1,
        skipFsync,
    )
}

#[inline]
unsafe fn smgrnblocks(reln: SMgrRelation, forknum: ForkNumber) -> BlockNumber {
    crate::storage::smgr::smgr::smgrnblocks(reln as _, forknum as _)
}

#[inline]
unsafe fn smgrzeroextend(
    reln: SMgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: c_int,
    skipFsync: bool,
) {
    crate::storage::smgr::smgr::smgrzeroextend(reln as _, forknum as _, blocknum, nblocks, skipFsync)
}

// ---- bufpage (storage/bufpage.h) -----------------------------------------

type Page = *mut c_char;
type Block = *mut c_void;

#[inline]
unsafe fn PageSetChecksumInplace(page: Page, blkno: BlockNumber) {
    crate::storage::bufpage::PageSetChecksumInplace(page as _, blkno)
}

// ---- pgstat (pgstat.h / pgstat_io.c) -------------------------------------

#[inline]
unsafe fn pgstat_prepare_io_time(track: bool) -> instr_time {
    crate::utils::activity::pgstat_io::pgstat_prepare_io_time(track)
}

#[inline]
unsafe fn pgstat_count_io_op_time(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    start_time: instr_time,
    cnt: u32,
    bytes: u64,
) {
    crate::utils::activity::pgstat_io::pgstat_count_io_op_time(
        io_object, io_context, io_op, start_time, cnt, bytes,
    )
}

#[inline]
unsafe fn pgstat_count_io_op(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    cnt: u32,
    bytes: u64,
) {
    crate::utils::activity::pgstat_io::pgstat_count_io_op(io_object, io_context, io_op, cnt, bytes)
}

// ---- resowner (utils/resowner.h) -----------------------------------------

#[inline]
unsafe fn ResourceOwnerRememberBuffer(owner: *mut c_void, buffer: Buffer) {
    crate::storage::buf_internals::ResourceOwnerRememberBuffer(owner as _, buffer)
}

#[inline]
unsafe fn ResourceOwnerForgetBuffer(owner: *mut c_void, buffer: Buffer) {
    crate::storage::buf_internals::ResourceOwnerForgetBuffer(owner as _, buffer)
}

// ---- relpath (common/relpath.h) ------------------------------------------

use crate::common::relpath::RelPathStr;

#[inline]
unsafe fn relpath(rlocator: *mut RelFileLocatorBackend, forknum: ForkNumber) -> RelPathStr {
    let loc = (*rlocator).locator;
    crate::common::relpath::GetRelationPath(
        loc.dbOid,
        loc.spcOid,
        loc.relNumber,
        (*rlocator).backend,
        forknum,
    )
}

#[inline]
unsafe fn relpathbackend(
    rlocator: RelFileLocator,
    backend: c_int,
    forknum: ForkNumber,
) -> RelPathStr {
    crate::common::relpath::GetRelationPath(
        rlocator.dbOid,
        rlocator.spcOid,
        rlocator.relNumber,
        backend,
        forknum,
    )
}

// ---- bufmgr.h debug helper -----------------------------------------------

#[inline]
unsafe fn DebugPrintBufferRefcount(_buffer: Buffer) -> *mut c_char {
    // Only reached on a local-buffer leak (an assertion-failure path).  The
    // canonical helper logs via elog and returns void; here we keep the
    // C-shaped "returns a pfree-able string" API and hand back null.
    null_mut()
}

// ---- io_direct (storage/fd.h) --------------------------------------------

static mut io_direct_flags: c_int = 0;
const IO_DIRECT_DATA: c_int = 1 << 0;

// ---- memdebug (Valgrind) -- no-ops in non-Valgrind builds ----------------

#[inline]
unsafe fn VALGRIND_MAKE_MEM_DEFINED(_addr: *mut c_void, _len: usize) {}
#[inline]
unsafe fn VALGRIND_MAKE_MEM_NOACCESS(_addr: *mut c_void, _len: usize) {}

// ==========================================================================
// localbuf.c
// ==========================================================================

/*#define LBDEBUG*/

/* entry for buffer lookup hashtable */
#[repr(C)]
#[derive(Clone, Copy)]
struct LocalBufferLookupEnt {
    key: BufferTag, /* Tag of a disk page */
    id: c_int,      /* Associated local buffer's index */
}

/* Note: this macro only works on local buffers, not shared ones! */
// #define LocalBufHdrGetBlock(bufHdr) \
//     LocalBufferBlockPointers[-((bufHdr)->buf_id + 2)]
#[inline]
unsafe fn LocalBufHdrGetBlock(bufHdr: *const BufferDesc) -> Block {
    *LocalBufferBlockPointers.offset(-((*bufHdr).buf_id + 2) as isize)
}

#[inline]
unsafe fn LocalBufHdrSetBlock(bufHdr: *const BufferDesc, val: Block) {
    *LocalBufferBlockPointers.offset(-((*bufHdr).buf_id + 2) as isize) = val;
}

#[no_mangle]
pub static mut NLocBuffer: c_int = 0; /* until buffers are initialized */

// LocalBufferDescriptors is declared (extern) in buf_internals.rs.
#[no_mangle]
pub static mut LocalBufferBlockPointers: *mut Block = null_mut();
#[no_mangle]
pub static mut LocalRefCount: *mut int32 = null_mut();

static mut nextFreeLocalBufId: c_int = 0;

static mut LocalBufHash: *mut HTAB = null_mut();

/* number of local buffers pinned at least once */
static mut NLocalPinnedBuffers: c_int = 0;

/*
 * PrefetchLocalBuffer -
 *	  initiate asynchronous read of a block of a relation
 *
 * Do PrefetchBuffer's work for temporary relations.
 * No-op if prefetching isn't compiled in.
 */
pub unsafe fn PrefetchLocalBuffer(
    smgr: SMgrRelation,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
) -> PrefetchBufferResult {
    let mut result = PrefetchBufferResult {
        recent_buffer: InvalidBuffer,
        initiated_io: false,
    };
    let mut newTag: BufferTag = std::mem::zeroed(); /* identity of requested block */
    let hresult: *mut LocalBufferLookupEnt;

    InitBufferTag(&mut newTag, smgr_locator(smgr), forkNum, blockNum);

    /* Initialize local buffers if first request in this session */
    if LocalBufHash.is_null() {
        InitLocalBuffers();
    }

    /* See if the desired buffer already exists */
    hresult = hash_search(
        LocalBufHash,
        &newTag as *const BufferTag as *const c_void,
        HASH_FIND,
        null_mut(),
    ) as *mut LocalBufferLookupEnt;

    if !hresult.is_null() {
        /* Yes, so nothing to do */
        result.recent_buffer = -(*hresult).id - 1;
    } else {
        /* USE_PREFETCH */
        /* Not in buffers, so initiate prefetch */
        if (io_direct_flags & IO_DIRECT_DATA) == 0
            && smgrprefetch(smgr, forkNum, blockNum, 1)
        {
            result.initiated_io = true;
        }
    }

    result
}

/*
 * LocalBufferAlloc -
 *	  Find or create a local buffer for the given page of the given relation.
 *
 * API is similar to bufmgr.c's BufferAlloc, except that we do not need to do
 * any locking since this is all local.  We support only default access
 * strategy (hence, usage_count is always advanced).
 */
pub unsafe fn LocalBufferAlloc(
    smgr: SMgrRelation,
    forkNum: ForkNumber,
    blockNum: BlockNumber,
    foundPtr: *mut bool,
) -> *mut BufferDesc {
    let mut newTag: BufferTag = std::mem::zeroed(); /* identity of requested block */
    let mut hresult: *mut LocalBufferLookupEnt;
    let bufHdr: *mut BufferDesc;
    let victim_buffer: Buffer;
    let bufid: c_int;
    let mut found: bool = false;

    InitBufferTag(&mut newTag, smgr_locator(smgr), forkNum, blockNum);

    /* Initialize local buffers if first request in this session */
    if LocalBufHash.is_null() {
        InitLocalBuffers();
    }

    ResourceOwnerEnlarge(CurrentResourceOwner);

    /* See if the desired buffer already exists */
    hresult = hash_search(
        LocalBufHash,
        &newTag as *const BufferTag as *const c_void,
        HASH_FIND,
        null_mut(),
    ) as *mut LocalBufferLookupEnt;

    if !hresult.is_null() {
        bufid = (*hresult).id;
        bufHdr = GetLocalBufferDescriptor(bufid as uint32);
        Assert!(BufferTagsEqual(&(*bufHdr).tag, &newTag));

        *foundPtr = PinLocalBuffer(bufHdr, true);
    } else {
        let mut buf_state: uint32;

        victim_buffer = GetLocalVictimBuffer();
        bufid = -victim_buffer - 1;
        bufHdr = GetLocalBufferDescriptor(bufid as uint32);

        hresult = hash_search(
            LocalBufHash,
            &newTag as *const BufferTag as *const c_void,
            HASH_ENTER,
            &mut found,
        ) as *mut LocalBufferLookupEnt;
        if found {
            /* shouldn't happen */
            elog!(ERROR, "local buffer hash table corrupted");
        }
        (*hresult).id = bufid;

        /*
         * it's all ours now.
         */
        (*bufHdr).tag = newTag;

        buf_state = pg_atomic_read_u32(&mut (*bufHdr).state);
        buf_state &= !(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
        buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
        pg_atomic_unlocked_write_u32(&mut (*bufHdr).state, buf_state);

        *foundPtr = false;
    }

    bufHdr
}

/*
 * Like FlushBuffer(), just for local buffers.
 */
pub unsafe fn FlushLocalBuffer(bufHdr: *mut BufferDesc, mut reln: SMgrRelation) {
    let io_start: instr_time;
    let localpage: Page = LocalBufHdrGetBlock(bufHdr) as *mut c_char;

    Assert!(*LocalRefCount.offset((-BufferDescriptorGetBuffer(bufHdr) - 1) as isize) > 0);

    /*
     * Try to start an I/O operation.  There currently are no reasons for
     * StartLocalBufferIO to return false, so we raise an error in that case.
     */
    if !StartLocalBufferIO(bufHdr, false, false) {
        elog!(ERROR, "failed to start write IO on local buffer");
    }

    /* Find smgr relation for buffer */
    if reln.is_null() {
        reln = smgropen(
            BufTagGetRelFileLocator(&(*bufHdr).tag),
            MyProcNumber,
        );
    }

    PageSetChecksumInplace(localpage, (*bufHdr).tag.blockNum);

    io_start = pgstat_prepare_io_time(track_io_timing);

    /* And write... */
    smgrwrite(
        reln,
        BufTagGetForkNum(&(*bufHdr).tag),
        (*bufHdr).tag.blockNum,
        localpage as *const c_void,
        false,
    );

    /* Temporary table I/O does not use Buffer Access Strategies */
    pgstat_count_io_op_time(
        IOOBJECT_TEMP_RELATION,
        IOCONTEXT_NORMAL,
        IOOP_WRITE,
        io_start,
        1,
        BLCKSZ as u64,
    );

    /* Mark not-dirty */
    TerminateLocalBufferIO(bufHdr, true, 0, false);

    pgBufferUsage.local_blks_written += 1;
}

unsafe fn GetLocalVictimBuffer() -> Buffer {
    let mut victim_bufid: c_int;
    let mut trycounter: c_int;
    let bufHdr: *mut BufferDesc;

    ResourceOwnerEnlarge(CurrentResourceOwner);

    /*
     * Need to get a new buffer.  We use a clock sweep algorithm (essentially
     * the same as what freelist.c does now...)
     */
    trycounter = NLocBuffer;
    let final_bufHdr: *mut BufferDesc;
    loop {
        victim_bufid = nextFreeLocalBufId;

        nextFreeLocalBufId += 1;
        if nextFreeLocalBufId >= NLocBuffer {
            nextFreeLocalBufId = 0;
        }

        let cur = GetLocalBufferDescriptor(victim_bufid as uint32);

        if *LocalRefCount.offset(victim_bufid as isize) == 0 {
            let mut buf_state: uint32 = pg_atomic_read_u32(&mut (*cur).state);

            if BUF_STATE_GET_USAGECOUNT(buf_state) > 0 {
                buf_state -= BUF_USAGECOUNT_ONE;
                pg_atomic_unlocked_write_u32(&mut (*cur).state, buf_state);
                trycounter = NLocBuffer;
            } else if BUF_STATE_GET_REFCOUNT(buf_state) > 0 {
                /*
                 * This can be reached if the backend initiated AIO for this
                 * buffer and then errored out.
                 */
            } else {
                /* Found a usable buffer */
                PinLocalBuffer(cur, false);
                final_bufHdr = cur;
                break;
            }
        } else {
            trycounter -= 1;
            if trycounter == 0 {
                ereport!(
                    ERROR,
                    errmsg!("no empty local buffer available")
                );
            }
        }
    }
    bufHdr = final_bufHdr;

    /*
     * lazy memory allocation: allocate space on first use of a buffer.
     */
    if LocalBufHdrGetBlock(bufHdr).is_null() {
        /* Set pointer for use by BufferGetBlock() macro */
        LocalBufHdrSetBlock(bufHdr, GetLocalBufferStorage());
    }

    /*
     * this buffer is not referenced but it might still be dirty. if that's
     * the case, write it out before reusing it!
     */
    if pg_atomic_read_u32(&mut (*bufHdr).state) & BM_DIRTY != 0 {
        FlushLocalBuffer(bufHdr, null_mut());
    }

    /*
     * Remove the victim buffer from the hashtable and mark as invalid.
     */
    if pg_atomic_read_u32(&mut (*bufHdr).state) & BM_TAG_VALID != 0 {
        InvalidateLocalBuffer(bufHdr, false);

        pgstat_count_io_op(IOOBJECT_TEMP_RELATION, IOCONTEXT_NORMAL, IOOP_EVICT, 1, 0);
    }

    BufferDescriptorGetBuffer(bufHdr)
}

/* see GetPinLimit() */
pub unsafe fn GetLocalPinLimit() -> uint32 {
    /* Every backend has its own temporary buffers, and can pin them all. */
    num_temp_buffers as uint32
}

/* see GetAdditionalPinLimit() */
pub unsafe fn GetAdditionalLocalPinLimit() -> uint32 {
    Assert!(NLocalPinnedBuffers <= num_temp_buffers);
    (num_temp_buffers - NLocalPinnedBuffers) as uint32
}

/* see LimitAdditionalPins() */
pub unsafe fn LimitAdditionalLocalPins(additional_pins: *mut uint32) {
    let max_pins: uint32;

    if *additional_pins <= 1 {
        return;
    }

    /*
     * In contrast to LimitAdditionalPins() other backends don't play a role
     * here. We can allow up to NLocBuffer pins in total, but it might not be
     * initialized yet so read num_temp_buffers.
     */
    max_pins = (num_temp_buffers - NLocalPinnedBuffers) as uint32;

    if *additional_pins >= max_pins {
        *additional_pins = max_pins;
    }
}

/*
 * Implementation of ExtendBufferedRelBy() and ExtendBufferedRelTo() for
 * temporary buffers.
 */
pub unsafe fn ExtendBufferedRelLocal(
    bmr: BufferManagerRelation,
    fork: ForkNumber,
    _flags: uint32,
    mut extend_by: uint32,
    extend_upto: BlockNumber,
    buffers: *mut Buffer,
    extended_by: *mut uint32,
) -> BlockNumber {
    let first_block: BlockNumber;
    let io_start: instr_time;

    /* Initialize local buffers if first request in this session */
    if LocalBufHash.is_null() {
        InitLocalBuffers();
    }

    LimitAdditionalLocalPins(&mut extend_by);

    for i in 0..extend_by {
        let buf_hdr: *mut BufferDesc;
        let buf_block: Block;

        *buffers.offset(i as isize) = GetLocalVictimBuffer();
        buf_hdr = GetLocalBufferDescriptor((-*buffers.offset(i as isize) - 1) as uint32);
        buf_block = LocalBufHdrGetBlock(buf_hdr);

        /* new buffers are zero-filled */
        std::ptr::write_bytes(buf_block as *mut u8, 0, BLCKSZ);
    }

    first_block = smgrnblocks(bmr.smgr, fork);

    if extend_upto != InvalidBlockNumber {
        /*
         * In contrast to shared relations, nothing could change the relation
         * size concurrently. Thus we shouldn't end up finding that we don't
         * need to do anything.
         */
        Assert!(first_block <= extend_upto);

        Assert!((first_block as u64) + (extend_by as u64) <= extend_upto as u64);
    }

    /* Fail if relation is already at maximum possible length */
    if (first_block as u64) + (extend_by as u64) >= MaxBlockNumber as u64 {
        elog!(
            ERROR,
            "cannot extend relation {} beyond {} blocks",
            "<relpath>",
            MaxBlockNumber
        );
        // relpath(bmr.smgr->smgr_rlocator, fork).str -- relpath not ported
        let _ = relpath(smgr_rlocator_backend(bmr.smgr), fork);
    }

    for i in 0..extend_by {
        let victim_buf_id: c_int;
        let victim_buf_hdr: *mut BufferDesc;
        let mut tag: BufferTag = std::mem::zeroed();
        let hresult: *mut LocalBufferLookupEnt;
        let mut found: bool = false;

        victim_buf_id = -*buffers.offset(i as isize) - 1;
        victim_buf_hdr = GetLocalBufferDescriptor(victim_buf_id as uint32);

        /* in case we need to pin an existing buffer below */
        ResourceOwnerEnlarge(CurrentResourceOwner);

        InitBufferTag(
            &mut tag,
            smgr_locator(bmr.smgr),
            fork,
            first_block + i,
        );

        hresult = hash_search(
            LocalBufHash,
            &tag as *const BufferTag as *const c_void,
            HASH_ENTER,
            &mut found,
        ) as *mut LocalBufferLookupEnt;
        if found {
            let existing_hdr: *mut BufferDesc;
            let mut buf_state: uint32;

            UnpinLocalBuffer(BufferDescriptorGetBuffer(victim_buf_hdr));

            existing_hdr = GetLocalBufferDescriptor((*hresult).id as uint32);
            PinLocalBuffer(existing_hdr, false);
            *buffers.offset(i as isize) = BufferDescriptorGetBuffer(existing_hdr);

            /*
             * Clear the BM_VALID bit, do StartLocalBufferIO() and proceed.
             */
            buf_state = pg_atomic_read_u32(&mut (*existing_hdr).state);
            Assert!(buf_state & BM_TAG_VALID != 0);
            Assert!(buf_state & BM_DIRTY == 0);
            buf_state &= !BM_VALID;
            pg_atomic_unlocked_write_u32(&mut (*existing_hdr).state, buf_state);

            /* no need to loop for local buffers */
            StartLocalBufferIO(existing_hdr, true, false);
        } else {
            let mut buf_state: uint32 = pg_atomic_read_u32(&mut (*victim_buf_hdr).state);

            Assert!(buf_state & (BM_VALID | BM_TAG_VALID | BM_DIRTY | BM_JUST_DIRTIED) == 0);

            (*victim_buf_hdr).tag = tag;

            buf_state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;

            pg_atomic_unlocked_write_u32(&mut (*victim_buf_hdr).state, buf_state);

            (*hresult).id = victim_buf_id;

            StartLocalBufferIO(victim_buf_hdr, true, false);
        }
    }

    io_start = pgstat_prepare_io_time(track_io_timing);

    /* actually extend relation */
    smgrzeroextend(bmr.smgr, fork, first_block, extend_by as c_int, false);

    pgstat_count_io_op_time(
        IOOBJECT_TEMP_RELATION,
        IOCONTEXT_NORMAL,
        IOOP_EXTEND,
        io_start,
        1,
        (extend_by as u64) * (BLCKSZ as u64),
    );

    for i in 0..extend_by {
        let buf: Buffer = *buffers.offset(i as isize);
        let buf_hdr: *mut BufferDesc;
        let mut buf_state: uint32;

        buf_hdr = GetLocalBufferDescriptor((-buf - 1) as uint32);

        buf_state = pg_atomic_read_u32(&mut (*buf_hdr).state);
        buf_state |= BM_VALID;
        pg_atomic_unlocked_write_u32(&mut (*buf_hdr).state, buf_state);
    }

    *extended_by = extend_by;

    pgBufferUsage.local_blks_written += extend_by as i64;

    first_block
}

/*
 * MarkLocalBufferDirty -
 *	  mark a local buffer dirty
 */
pub unsafe fn MarkLocalBufferDirty(buffer: Buffer) {
    let bufid: c_int;
    let bufHdr: *mut BufferDesc;
    let mut buf_state: uint32;

    Assert!(BufferIsLocal(buffer));

    // #ifdef LBDEBUG
    // fprintf(stderr, "LB DIRTY %d\n", buffer);
    // #endif

    bufid = -buffer - 1;

    Assert!(*LocalRefCount.offset(bufid as isize) > 0);

    bufHdr = GetLocalBufferDescriptor(bufid as uint32);

    buf_state = pg_atomic_read_u32(&mut (*bufHdr).state);

    if buf_state & BM_DIRTY == 0 {
        pgBufferUsage.local_blks_dirtied += 1;
    }

    buf_state |= BM_DIRTY;

    pg_atomic_unlocked_write_u32(&mut (*bufHdr).state, buf_state);
}

/*
 * Like StartBufferIO, but for local buffers
 */
pub unsafe fn StartLocalBufferIO(bufHdr: *mut BufferDesc, forInput: bool, nowait: bool) -> bool {
    let buf_state: uint32;

    /*
     * With AIO the buffer could have IO in progress, e.g. when there are two
     * scans of the same relation. Either wait for the other IO or return
     * false.
     */
    if pgaio_wref_valid(&mut (*bufHdr).io_wref) {
        let mut iow: PgAioWaitRef = core::ptr::read(&(*bufHdr).io_wref);

        if nowait {
            return false;
        }

        pgaio_wref_wait(&mut iow);
    }

    /* Once we get here, there is definitely no I/O active on this buffer */

    /* Check if someone else already did the I/O */
    buf_state = pg_atomic_read_u32(&mut (*bufHdr).state);
    if if forInput {
        buf_state & BM_VALID != 0
    } else {
        buf_state & BM_DIRTY == 0
    } {
        return false;
    }

    /* BM_IO_IN_PROGRESS isn't currently used for local buffers */

    /* local buffers don't track IO using resowners */

    true
}

/*
 * Like TerminateBufferIO, but for local buffers
 */
pub unsafe fn TerminateLocalBufferIO(
    bufHdr: *mut BufferDesc,
    clear_dirty: bool,
    set_flag_bits: uint32,
    release_aio: bool,
) {
    /* Only need to adjust flags */
    let mut buf_state: uint32 = pg_atomic_read_u32(&mut (*bufHdr).state);

    /* BM_IO_IN_PROGRESS isn't currently used for local buffers */

    /* Clear earlier errors, if this IO failed, it'll be marked again */
    buf_state &= !BM_IO_ERROR;

    if clear_dirty {
        buf_state &= !BM_DIRTY;
    }

    if release_aio {
        /* release pin held by IO subsystem, see also buffer_stage_common() */
        Assert!(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
        buf_state -= BUF_REFCOUNT_ONE;
        pgaio_wref_clear(&mut (*bufHdr).io_wref);
    }

    buf_state |= set_flag_bits;
    pg_atomic_unlocked_write_u32(&mut (*bufHdr).state, buf_state);

    /* local buffers don't track IO using resowners */

    /* local buffers don't use the IO CV, as no other process can see buffer */

    /* local buffers don't use BM_PIN_COUNT_WAITER, so no need to wake */
}

/*
 * InvalidateLocalBuffer -- mark a local buffer invalid.
 *
 * If check_unreferenced is true, error out if the buffer is still
 * pinned. Passing false is appropriate when calling InvalidateLocalBuffer()
 * as part of changing the identity of a buffer, instead of just dropping the
 * buffer.
 *
 * See also InvalidateBuffer().
 */
pub unsafe fn InvalidateLocalBuffer(bufHdr: *mut BufferDesc, check_unreferenced: bool) {
    let buffer: Buffer = BufferDescriptorGetBuffer(bufHdr);
    let bufid: c_int = -buffer - 1;
    let mut buf_state: uint32;
    let hresult: *mut LocalBufferLookupEnt;

    /*
     * It's possible that we started IO on this buffer before e.g. aborting
     * the transaction that created a table. We need to wait for that IO to
     * complete before removing / reusing the buffer.
     */
    if pgaio_wref_valid(&mut (*bufHdr).io_wref) {
        let mut iow: PgAioWaitRef = core::ptr::read(&(*bufHdr).io_wref);

        pgaio_wref_wait(&mut iow);
        Assert!(!pgaio_wref_valid(&mut (*bufHdr).io_wref));
    }

    buf_state = pg_atomic_read_u32(&mut (*bufHdr).state);

    /*
     * We need to test not just LocalRefCount[bufid] but also the BufferDesc
     * itself, as the latter is used to represent a pin by the AIO subsystem.
     * This can happen if AIO is initiated and then the query errors out.
     */
    if check_unreferenced
        && (*LocalRefCount.offset(bufid as isize) != 0
            || BUF_STATE_GET_REFCOUNT(buf_state) != 0)
    {
        elog!(
            ERROR,
            "block {} of {} is still referenced (local {})",
            (*bufHdr).tag.blockNum,
            "<relpath>",
            *LocalRefCount.offset(bufid as isize)
        );
        let _ = relpathbackend(
            BufTagGetRelFileLocator(&(*bufHdr).tag),
            MyProcNumber,
            BufTagGetForkNum(&(*bufHdr).tag),
        );
    }

    /* Remove entry from hashtable */
    hresult = hash_search(
        LocalBufHash,
        &(*bufHdr).tag as *const BufferTag as *const c_void,
        HASH_REMOVE,
        null_mut(),
    ) as *mut LocalBufferLookupEnt;
    if hresult.is_null() {
        /* shouldn't happen */
        elog!(ERROR, "local buffer hash table corrupted");
    }
    /* Mark buffer invalid */
    ClearBufferTag(&mut (*bufHdr).tag);
    buf_state &= !BUF_FLAG_MASK;
    buf_state &= !BUF_USAGECOUNT_MASK;
    pg_atomic_unlocked_write_u32(&mut (*bufHdr).state, buf_state);
}

/*
 * DropRelationLocalBuffers
 *		This function removes from the buffer pool all the pages of the
 *		specified relation that have block numbers >= firstDelBlock.
 *		(In particular, with firstDelBlock = 0, all pages are removed.)
 *		Dirty pages are simply dropped, without bothering to write them
 *		out first.  Therefore, this is NOT rollback-able, and so should be
 *		used only with extreme caution!
 *
 *		See DropRelationBuffers in bufmgr.c for more notes.
 */
pub unsafe fn DropRelationLocalBuffers(
    rlocator: RelFileLocator,
    forkNum: ForkNumber,
    firstDelBlock: BlockNumber,
) {
    let mut i: c_int;

    i = 0;
    while i < NLocBuffer {
        let bufHdr: *mut BufferDesc = GetLocalBufferDescriptor(i as uint32);
        let buf_state: uint32;

        buf_state = pg_atomic_read_u32(&mut (*bufHdr).state);

        if (buf_state & BM_TAG_VALID != 0)
            && BufTagMatchesRelFileLocator(&(*bufHdr).tag, &rlocator)
            && BufTagGetForkNum(&(*bufHdr).tag) == forkNum
            && (*bufHdr).tag.blockNum >= firstDelBlock
        {
            InvalidateLocalBuffer(bufHdr, true);
        }

        i += 1;
    }
}

/*
 * DropRelationAllLocalBuffers
 *		This function removes from the buffer pool all pages of all forks
 *		of the specified relation.
 *
 *		See DropRelationsAllBuffers in bufmgr.c for more notes.
 */
pub unsafe fn DropRelationAllLocalBuffers(rlocator: RelFileLocator) {
    let mut i: c_int;

    i = 0;
    while i < NLocBuffer {
        let bufHdr: *mut BufferDesc = GetLocalBufferDescriptor(i as uint32);
        let buf_state: uint32;

        buf_state = pg_atomic_read_u32(&mut (*bufHdr).state);

        if (buf_state & BM_TAG_VALID != 0)
            && BufTagMatchesRelFileLocator(&(*bufHdr).tag, &rlocator)
        {
            InvalidateLocalBuffer(bufHdr, true);
        }

        i += 1;
    }
}

/*
 * InitLocalBuffers -
 *	  init the local buffer cache. Since most queries (esp. multi-user ones)
 *	  don't involve local buffers, we delay allocating actual memory for the
 *	  buffers until we need them; just make the buffer headers here.
 */
unsafe fn InitLocalBuffers() {
    let nbufs: c_int = num_temp_buffers;
    let mut info: HASHCTL = std::mem::zeroed();
    let mut i: c_int;

    /*
     * Parallel workers can't access data in temporary tables, because they
     * have no visibility into the local buffers of their leader.  This is a
     * convenient, low-cost place to provide a backstop check for that.  Note
     * that we don't wish to prevent a parallel worker from accessing catalog
     * metadata about a temp table, so checks at higher levels would be
     * inappropriate.
     */
    if IsParallelWorker() {
        ereport!(
            ERROR,
            errmsg!("cannot access temporary tables during a parallel operation")
        );
    }

    /* Allocate and zero buffer headers and auxiliary arrays */
    LocalBufferDescriptors =
        calloc(nbufs as usize, std::mem::size_of::<BufferDesc>()) as *mut BufferDesc;
    LocalBufferBlockPointers =
        calloc(nbufs as usize, std::mem::size_of::<Block>()) as *mut Block;
    LocalRefCount = calloc(nbufs as usize, std::mem::size_of::<int32>()) as *mut int32;
    if LocalBufferDescriptors.is_null()
        || LocalBufferBlockPointers.is_null()
        || LocalRefCount.is_null()
    {
        ereport!(FATAL, errmsg!("out of memory"));
    }

    nextFreeLocalBufId = 0;

    /* initialize fields that need to start off nonzero */
    i = 0;
    while i < nbufs {
        let buf: *mut BufferDesc = GetLocalBufferDescriptor(i as uint32);

        /*
         * negative to indicate local buffer. This is tricky: shared buffers
         * start with 0. We have to start with -2. (Note that the routine
         * BufferDescriptorGetBuffer adds 1 to buf_id so our first buffer id
         * is -1.)
         */
        (*buf).buf_id = -i - 2;

        pgaio_wref_clear(&mut (*buf).io_wref);

        /*
         * Intentionally do not initialize the buffer's atomic variable
         * (besides zeroing the underlying memory above). That way we get
         * errors on platforms without atomics, if somebody (re-)introduces
         * atomic operations for local buffers.
         */

        i += 1;
    }

    /* Create the lookup hash table */
    info.keysize = std::mem::size_of::<BufferTag>();
    info.entrysize = std::mem::size_of::<LocalBufferLookupEnt>();

    LocalBufHash = hash_create(
        c"Local Buffer Lookup Table".as_ptr(),
        nbufs as c_long,
        &info,
        HASH_ELEM | HASH_BLOBS,
    );

    if LocalBufHash.is_null() {
        elog!(ERROR, "could not initialize local buffer hash table");
    }

    /* Initialization done, mark buffers allocated */
    NLocBuffer = nbufs;
}

/*
 * XXX: We could have a slightly more efficient version of PinLocalBuffer()
 * that does not support adjusting the usagecount - but so far it does not
 * seem worth the trouble.
 *
 * Note that ResourceOwnerEnlarge() must have been done already.
 */
pub unsafe fn PinLocalBuffer(buf_hdr: *mut BufferDesc, adjust_usagecount: bool) -> bool {
    let mut buf_state: uint32;
    let buffer: Buffer = BufferDescriptorGetBuffer(buf_hdr);
    let bufid: c_int = -buffer - 1;

    buf_state = pg_atomic_read_u32(&mut (*buf_hdr).state);

    if *LocalRefCount.offset(bufid as isize) == 0 {
        NLocalPinnedBuffers += 1;
        buf_state += BUF_REFCOUNT_ONE;
        if adjust_usagecount && BUF_STATE_GET_USAGECOUNT(buf_state) < BM_MAX_USAGE_COUNT {
            buf_state += BUF_USAGECOUNT_ONE;
        }
        pg_atomic_unlocked_write_u32(&mut (*buf_hdr).state, buf_state);

        /*
         * See comment in PinBuffer().
         *
         * If the buffer isn't allocated yet, it'll be marked as defined in
         * GetLocalBufferStorage().
         */
        if !LocalBufHdrGetBlock(buf_hdr).is_null() {
            VALGRIND_MAKE_MEM_DEFINED(LocalBufHdrGetBlock(buf_hdr), BLCKSZ);
        }
    }
    *LocalRefCount.offset(bufid as isize) += 1;
    ResourceOwnerRememberBuffer(
        CurrentResourceOwner as *mut c_void,
        BufferDescriptorGetBuffer(buf_hdr),
    );

    buf_state & BM_VALID != 0
}

pub unsafe fn UnpinLocalBuffer(buffer: Buffer) {
    UnpinLocalBufferNoOwner(buffer);
    ResourceOwnerForgetBuffer(CurrentResourceOwner as *mut c_void, buffer);
}

pub unsafe fn UnpinLocalBufferNoOwner(buffer: Buffer) {
    let buffid: c_int = -buffer - 1;

    Assert!(BufferIsLocal(buffer));
    Assert!(*LocalRefCount.offset(buffid as isize) > 0);
    Assert!(NLocalPinnedBuffers > 0);

    *LocalRefCount.offset(buffid as isize) -= 1;
    if *LocalRefCount.offset(buffid as isize) == 0 {
        let buf_hdr: *mut BufferDesc = GetLocalBufferDescriptor(buffid as uint32);
        let mut buf_state: uint32;

        NLocalPinnedBuffers -= 1;

        buf_state = pg_atomic_read_u32(&mut (*buf_hdr).state);
        Assert!(BUF_STATE_GET_REFCOUNT(buf_state) > 0);
        buf_state -= BUF_REFCOUNT_ONE;
        pg_atomic_unlocked_write_u32(&mut (*buf_hdr).state, buf_state);

        /* see comment in UnpinBufferNoOwner */
        VALGRIND_MAKE_MEM_NOACCESS(LocalBufHdrGetBlock(buf_hdr), BLCKSZ);
    }
}

/*
 * GUC check_hook for temp_buffers
 */
pub unsafe fn check_temp_buffers(
    newval: *mut c_int,
    _extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    /*
     * Once local buffers have been initialized, it's too late to change this.
     * However, if this is only a test call, allow it.
     */
    if source != PGC_S_TEST && NLocBuffer != 0 && NLocBuffer != *newval {
        GUC_check_errdetail!(
            "\"temp_buffers\" cannot be changed after any temporary tables have been accessed in the session."
        );
        return false;
    }
    true
}

/*
 * GetLocalBufferStorage - allocate memory for a local buffer
 *
 * The idea of this function is to aggregate our requests for storage
 * so that the memory manager doesn't see a whole lot of relatively small
 * requests.  Since we'll never give back a local buffer once it's created
 * within a particular process, no point in burdening memmgr with separately
 * managed chunks.
 */
unsafe fn GetLocalBufferStorage() -> Block {
    static mut cur_block: *mut c_char = null_mut();
    static mut next_buf_in_block: c_int = 0;
    static mut num_bufs_in_block: c_int = 0;
    static mut total_bufs_allocated: c_int = 0;
    static mut LocalBufferContext: MemoryContext = null_mut();

    let this_buf: *mut c_char;

    Assert!(total_bufs_allocated < NLocBuffer);

    if next_buf_in_block >= num_bufs_in_block {
        /* Need to make a new request to memmgr */
        let mut num_bufs: c_int;

        /*
         * We allocate local buffers in a context of their own, so that the
         * space eaten for them is easily recognizable in MemoryContextStats
         * output.  Create the context on first use.
         */
        if LocalBufferContext.is_null() {
            LocalBufferContext = AllocSetContextCreate!(
                TopMemoryContext,
                c"LocalBufferContext".as_ptr(),
                ALLOCSET_DEFAULT_SIZES
            );
        }

        /* Start with a 16-buffer request; subsequent ones double each time */
        num_bufs = Max(num_bufs_in_block * 2, 16);
        /* But not more than what we need for all remaining local bufs */
        num_bufs = Min(num_bufs, NLocBuffer - total_bufs_allocated);
        /* And don't overflow MaxAllocSize, either */
        num_bufs = Min(num_bufs, (MaxAllocSize / BLCKSZ) as c_int);

        /* Buffers should be I/O aligned. */
        cur_block = TYPEALIGN(
            PG_IO_ALIGN_SIZE,
            MemoryContextAlloc(
                LocalBufferContext,
                (num_bufs as usize) * BLCKSZ + PG_IO_ALIGN_SIZE,
            ) as usize,
        ) as *mut c_char;
        next_buf_in_block = 0;
        num_bufs_in_block = num_bufs;
    }

    /* Allocate next buffer in current memory block */
    this_buf = cur_block.add((next_buf_in_block as usize) * BLCKSZ);
    next_buf_in_block += 1;
    total_bufs_allocated += 1;

    /*
     * Caller's PinLocalBuffer() was too early for Valgrind updates, so do it
     * here.  The block is actually undefined, but we want consistency with
     * the regular case of not needing to allocate memory.  This is
     * specifically needed when method_io_uring.c fills the block, because
     * Valgrind doesn't recognize io_uring reads causing undefined memory to
     * become defined.
     */
    VALGRIND_MAKE_MEM_DEFINED(this_buf as *mut c_void, BLCKSZ);

    this_buf as Block
}

/*
 * CheckForLocalBufferLeaks - ensure this backend holds no local buffer pins
 *
 * This is just like CheckForBufferLeaks(), but for local buffers.
 */
unsafe fn CheckForLocalBufferLeaks() {
    // #ifdef USE_ASSERT_CHECKING
    if !LocalRefCount.is_null() {
        let mut RefCountErrors: c_int = 0;
        let mut i: c_int;

        i = 0;
        while i < NLocBuffer {
            if *LocalRefCount.offset(i as isize) != 0 {
                let b: Buffer = -i - 1;
                let s: *mut c_char;

                s = DebugPrintBufferRefcount(b);
                elog!(WARNING, "local buffer refcount leak: {}", "<refcount>");
                pfree(s as *mut c_void);

                RefCountErrors += 1;
            }

            i += 1;
        }
        Assert!(RefCountErrors == 0);
    }
    // #endif
}

/*
 * AtEOXact_LocalBuffers - clean up at end of transaction.
 *
 * This is just like AtEOXact_Buffers, but for local buffers.
 */
pub unsafe fn AtEOXact_LocalBuffers(_isCommit: bool) {
    CheckForLocalBufferLeaks();
}

/*
 * AtProcExit_LocalBuffers - ensure we have dropped pins during backend exit.
 *
 * This is just like AtProcExit_Buffers, but for local buffers.
 */
pub unsafe fn AtProcExit_LocalBuffers() {
    /*
     * We shouldn't be holding any remaining pins; if we are, and assertions
     * aren't enabled, we'll fail later in DropRelationBuffers while trying to
     * drop the temp rels.
     */
    CheckForLocalBufferLeaks();
}

// libc calloc, used by InitLocalBuffers.
extern "C" {
    fn calloc(nmemb: usize, size: usize) -> *mut c_void;
}

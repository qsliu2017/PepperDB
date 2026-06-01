//! access/spgist/spginsert.c - Externally visible SP-GiST index creation/insertion routines.
//!
//! All the actual insertion logic is in spgdoinsert.c.

use crate::prelude::*;

use crate::access::index::amapi::{IndexBuildResult, IndexInfo, IndexUniqueCheck};
use crate::access::spgist::spgist_private::{
    SpGistState, SPGIST_LEAF, SPGIST_METAPAGE_BLKNO, SPGIST_NULL_BLKNO, SPGIST_NULLS,
    SPGIST_ROOT_BLKNO,
};
use crate::common::relpath::{ForkNumber, INIT_FORKNUM, MAIN_FORKNUM};
use crate::miscadmin::{END_CRIT_SECTION, START_CRIT_SECTION};
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::Page;
use crate::storage::itemptr::ItemPointer;
use crate::utils::rel::{Relation, RelationGetRelationName};

use std::mem::size_of;

#[repr(C)]
struct SpGistBuildState {
    spgstate: SpGistState,    /* SPGiST's working state */
    indtuples: int64,         /* total number of tuples indexed */
    tmpCtx: MemoryContext,    /* per-tuple temporary context */
}

/* Callback to process one heap tuple during table_index_build_scan */
unsafe extern "C" fn spgistBuildCallback(
    index: Relation,
    tid: ItemPointer,
    values: *mut Datum,
    isnull: *mut bool,
    _tupleIsAlive: bool,
    state: *mut c_void,
) {
    let buildstate = state as *mut SpGistBuildState;

    /* Work in temp context, and reset it after each tuple */
    let oldCtx = MemoryContextSwitchTo((*buildstate).tmpCtx);

    /*
     * Even though no concurrent insertions can be happening, we still might
     * get a buffer-locking failure due to bgwriter or checkpointer taking a
     * lock on some buffer.  So we need to be willing to retry.  We can flush
     * any temp data when retrying.
     */
    while !spgdoinsert(
        index,
        &mut (*buildstate).spgstate,
        tid,
        values,
        isnull,
    ) {
        MemoryContextReset((*buildstate).tmpCtx);
    }

    /* Update total tuple count */
    (*buildstate).indtuples += 1;

    MemoryContextSwitchTo(oldCtx);
    MemoryContextReset((*buildstate).tmpCtx);
}

/*
 * Build an SP-GiST index.
 */
pub unsafe fn spgbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult {
    if RelationGetNumberOfBlocks(index) != 0 {
        elog!(
            ERROR,
            "index already contains data"
        );
        let _ = RelationGetRelationName(index);
    }

    /*
     * Initialize the meta page and root pages
     */
    let metabuffer = SpGistNewBuffer(index);
    let rootbuffer = SpGistNewBuffer(index);
    let nullbuffer = SpGistNewBuffer(index);

    Assert!(BufferGetBlockNumber(metabuffer) == SPGIST_METAPAGE_BLKNO);
    Assert!(BufferGetBlockNumber(rootbuffer) == SPGIST_ROOT_BLKNO);
    Assert!(BufferGetBlockNumber(nullbuffer) == SPGIST_NULL_BLKNO);

    START_CRIT_SECTION();

    SpGistInitMetapage(BufferGetPage(metabuffer));
    MarkBufferDirty(metabuffer);
    SpGistInitBuffer(rootbuffer, SPGIST_LEAF as uint16);
    MarkBufferDirty(rootbuffer);
    SpGistInitBuffer(nullbuffer, (SPGIST_LEAF | SPGIST_NULLS) as uint16);
    MarkBufferDirty(nullbuffer);

    END_CRIT_SECTION();

    UnlockReleaseBuffer(metabuffer);
    UnlockReleaseBuffer(rootbuffer);
    UnlockReleaseBuffer(nullbuffer);

    /*
     * Now insert all the heap data into the index
     */
    let mut buildstate: SpGistBuildState = std::mem::zeroed();
    initSpGistState(&mut buildstate.spgstate, index);
    buildstate.spgstate.isBuild = true;
    buildstate.indtuples = 0;

    buildstate.tmpCtx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"SP-GiST build temporary context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    let reltuples = table_index_build_scan(
        heap,
        index,
        indexInfo,
        true,
        true,
        spgistBuildCallback,
        &mut buildstate as *mut SpGistBuildState as *mut c_void,
        null_mut(),
    );

    MemoryContextDelete(buildstate.tmpCtx);

    SpGistUpdateMetaPage(index);

    /*
     * We didn't write WAL records as we built the index, so if WAL-logging is
     * required, write all pages to the WAL now.
     */
    if RelationNeedsWAL(index) {
        log_newpage_range(
            index,
            MAIN_FORKNUM,
            0,
            RelationGetNumberOfBlocks(index),
            true,
        );
    }

    let result = palloc0(size_of::<IndexBuildResultData>()) as *mut IndexBuildResultData;
    (*result).heap_tuples = reltuples;
    (*result).index_tuples = buildstate.indtuples as f64;

    result as *mut IndexBuildResult
}

/*
 * Build an empty SPGiST index in the initialization fork
 */
pub unsafe fn spgbuildempty(index: Relation) {
    let bulkstate = smgr_bulk_start_rel(index, INIT_FORKNUM);

    /* Construct metapage. */
    let mut buf = smgr_bulk_get_buf(bulkstate);
    SpGistInitMetapage(buf as Page);
    smgr_bulk_write(bulkstate, SPGIST_METAPAGE_BLKNO, buf, true);

    /* Likewise for the root page. */
    buf = smgr_bulk_get_buf(bulkstate);
    SpGistInitPage(buf as Page, SPGIST_LEAF as uint16);
    smgr_bulk_write(bulkstate, SPGIST_ROOT_BLKNO, buf, true);

    /* Likewise for the null-tuples root page. */
    buf = smgr_bulk_get_buf(bulkstate);
    SpGistInitPage(buf as Page, (SPGIST_LEAF | SPGIST_NULLS) as uint16);
    smgr_bulk_write(bulkstate, SPGIST_NULL_BLKNO, buf, true);

    smgr_bulk_finish(bulkstate);
}

/*
 * Insert one new tuple into an SPGiST index.
 */
pub unsafe fn spginsert(
    index: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    ht_ctid: ItemPointer,
    _heapRel: Relation,
    _checkUnique: IndexUniqueCheck,
    _indexUnchanged: bool,
    _indexInfo: *mut IndexInfo,
) -> bool {
    let mut spgstate: SpGistState = std::mem::zeroed();

    let insertCtx = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"SP-GiST insert temporary context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
    let oldCtx = MemoryContextSwitchTo(insertCtx);

    initSpGistState(&mut spgstate, index);

    /*
     * We might have to repeat spgdoinsert() multiple times, if conflicts
     * occur with concurrent insertions.  If so, reset the insertCtx each time
     * to avoid cumulative memory consumption.  That means we also have to
     * redo initSpGistState(), but it's cheap enough not to matter.
     */
    while !spgdoinsert(index, &mut spgstate, ht_ctid, values, isnull) {
        MemoryContextReset(insertCtx);
        initSpGistState(&mut spgstate, index);
    }

    SpGistUpdateMetaPage(index);

    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx);

    /* return false since we've not done any unique check */
    false
}

// ---------------------------------------------------------------------------
// Concrete IndexBuildResult layout (genam.h). The amapi alias is opaque
// (c_void); we need the real fields here to populate the result.
// TODO(pg-port): unify with real `struct IndexBuildResult` in access/genam.h.
// ---------------------------------------------------------------------------
#[repr(C)]
struct IndexBuildResultData {
    heap_tuples: f64,  /* # of heap tuples */
    index_tuples: f64, /* # of index tuples */
}

// ---------------------------------------------------------------------------
// Opaque bulk-write types (storage/bulk_write.h). NOT ported.
// ---------------------------------------------------------------------------
type BulkWriteState = c_void;
type BulkWriteBuffer = *mut c_void;

// ---------------------------------------------------------------------------
// Local stubs for not-yet-ported callees. TODO(pg-port): replace with imports
// once these are translated.
// ---------------------------------------------------------------------------
unsafe fn spgdoinsert(
    _index: Relation,
    _state: *mut SpGistState,
    _heapPtr: ItemPointer,
    _datums: *mut Datum,
    _isnulls: *mut bool,
) -> bool {
    unimplemented!() // TODO: spgdoinsert.c
}

unsafe fn initSpGistState(_state: *mut SpGistState, _index: Relation) {
    unimplemented!() // TODO: spgutils.c
}

unsafe fn SpGistNewBuffer(_index: Relation) -> Buffer {
    unimplemented!() // TODO: spgutils.c
}

unsafe fn SpGistUpdateMetaPage(_index: Relation) {
    unimplemented!() // TODO: spgutils.c
}

unsafe fn SpGistInitPage(_page: Page, _f: uint16) {
    unimplemented!() // TODO: spgutils.c
}

unsafe fn SpGistInitBuffer(_b: Buffer, _f: uint16) {
    unimplemented!() // TODO: spgutils.c
}

unsafe fn SpGistInitMetapage(_page: Page) {
    unimplemented!() // TODO: spgutils.c
}

unsafe fn RelationGetNumberOfBlocks(_relation: Relation) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.h (RelationGetNumberOfBlocksInFork)
}

unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}

unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn UnlockReleaseBuffer(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn RelationNeedsWAL(_relation: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn log_newpage_range(
    _rel: Relation,
    _forknum: ForkNumber,
    _startblk: BlockNumber,
    _endblk: BlockNumber,
    _page_std: bool,
) {
    unimplemented!() // TODO: access/xloginsert.c
}

unsafe fn table_index_build_scan(
    _heapRelation: Relation,
    _indexRelation: Relation,
    _indexInfo: *mut IndexInfo,
    _allow_sync: bool,
    _progress: bool,
    _callback: unsafe extern "C" fn(Relation, ItemPointer, *mut Datum, *mut bool, bool, *mut c_void),
    _callback_state: *mut c_void,
    _scan: *mut c_void,
) -> f64 {
    unimplemented!() // TODO: access/tableam.h
}

unsafe fn smgr_bulk_start_rel(_rel: Relation, _forknum: ForkNumber) -> *mut BulkWriteState {
    unimplemented!() // TODO: storage/bulk_write.c
}

unsafe fn smgr_bulk_get_buf(_bulkstate: *mut BulkWriteState) -> BulkWriteBuffer {
    unimplemented!() // TODO: storage/bulk_write.c
}

unsafe fn smgr_bulk_write(
    _bulkstate: *mut BulkWriteState,
    _blocknum: BlockNumber,
    _buf: BulkWriteBuffer,
    _page_std: bool,
) {
    unimplemented!() // TODO: storage/bulk_write.c
}

unsafe fn smgr_bulk_finish(_bulkstate: *mut BulkWriteState) {
    unimplemented!() // TODO: storage/bulk_write.c
}

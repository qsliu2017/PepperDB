//! Heap access method: insert + forward sequential scan. Translated from
//! backend/access/heap/heapam.c.
//!
//! M2 scope (step 12): `heap_insert` (with `heap_prepare_insert` and the
//! `XLOG_HEAP_INSERT` WAL emit), `heap_beginscan`/`heap_endscan`/`heap_rescan`,
//! the page-at-a-time forward seqscan (`heapgettup_pagemode` + the
//! `heap_prepare_pagescan`/`page_collect_tuples` visibility pass), and
//! `heap_getnext`. Update/delete/lock/index-fetch and backward scan are grow
//! guards (M6/M8) -- clean `unimplemented!()`, never half-written.
//!
//! Async coloring (rules.md s5): `heap_insert` reaches the buffer pool, FSM, and
//! WAL leaves, so it is `async`; the scan page reads (`read_buffer_common`) are
//! async too, so `heap_getnext`/the page-walk are `async`. The buffer content
//! lock (a `parking_lot` guard) is NEVER held across an `.await`: insert takes
//! it, mutates, sets the LSN, drops it, then awaits nothing more under it; the
//! scan takes the SHARE lock only for the synchronous per-page visibility
//! collection, drops it, and keeps the pin while returning visible tuples (PG's
//! page-at-a-time contract). The C read-stream prefetch is replaced by a direct
//! block-by-block `read_buffer_common` walk (rewrite-to-design: the prefetch is a
//! perf optimization; the visible-tuple semantics are identical).
//!
//! Threading: the foundation buffer/FSM/WAL APIs take `&Arc<SharedState>`
//! explicitly, so the M2 heap entry points carry it as a leading parameter
//! (matching the established foundation convention) in addition to the C
//! `Arc<RelationData>`.

use std::sync::Arc;

use crate::access::heapam_xlog::{
    xl_heap_delete, xl_heap_header, xl_heap_insert, xl_heap_lock, xl_heap_update, SizeOfHeapDelete,
    SizeOfHeapHeader, SizeOfHeapInsert, SizeOfHeapLock, SizeOfHeapUpdate, XLHL_KEYS_UPDATED,
    XLHL_XMAX_EXCL_LOCK, XLHL_XMAX_IS_MULTI, XLHL_XMAX_KEYSHR_LOCK, XLHL_XMAX_LOCK_ONLY,
    XLH_UPDATE_CONTAINS_NEW_TUPLE, XLOG_HEAP_DELETE, XLOG_HEAP_INIT_PAGE, XLOG_HEAP_INSERT,
    XLOG_HEAP_LOCK, XLOG_HEAP_UPDATE,
};
use crate::access::htup::{HeapTuple, HeapTupleData};
use crate::access::htup_details::{
    HeapTupleHeaderData, SizeofHeapTupleHeader, HEAP2_XACT_MASK, HEAP_KEYS_UPDATED, HEAP_MOVED,
    HEAP_UPDATED, HEAP_XACT_MASK, HEAP_XMAX_BITS, HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_INVALID,
    HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY, HEAP_XMAX_SHR_LOCK,
    HEAP_XMAX_IS_LOCKED_ONLY,
};
use crate::access::tableam::{TM_FailureData, TM_Result, TU_UpdateIndexes};
use crate::backend::access::heap::heapam_visibility::HeapTupleSatisfiesUpdate;
use crate::backend::access::transam::xact::IsInParallelMode;
use crate::backend::utils::time::combocid::{HeapTupleHeaderAdjustCmax, HeapTupleHeaderGetCmax};
use crate::backend::access::transam::xact::TransactionIdIsCurrentTransactionId;
use crate::c::InvalidCommandId;
use crate::nodes::lockoptions::{LockTupleMode, LockWaitPolicy};
use crate::storage::bufpage::{maxalign, Page};
use crate::storage::itemptr::ItemPointerData;
use crate::utils::elog::ERROR;
use crate::access::relscan::{TableScanDescData, TableScanType};
use crate::access::rmgrlist::RmgrId;
use crate::access::sdir::{scan_direction_is_forward, ScanDirection};
use crate::access::tableam::ScanOptions;
use crate::access::xlog::XLogRecordFlags;
use crate::access::xloginsert::RegBuf;
use crate::backend::access::heap::heapam_visibility::heap_tuple_satisfies_mvcc;
use crate::backend::access::heap::hio::{relation_get_buffer_for_tuple, relation_put_heap_tuple};
use crate::backend::access::transam::xact::GetCurrentTransactionId;
use crate::backend::access::transam::xloginsert::{
    begin_insert, register_block, register_buf_data, register_data, set_record_flags, xlog_insert,
};
use crate::common::relpath::ForkNumber;
use crate::shared_state::SharedState;
use crate::catalog::pg_class::{RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_SEQUENCE};
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::buf::Buffer;
use crate::storage::bufmgr::InvalidBuffer;
use crate::storage::off::{OffsetNumber, FIRST_OFFSET_NUMBER};
use crate::utils::rel::RelationData;
use crate::utils::snapshot::{Snapshot, SnapshotData, SnapshotType};

pub use crate::access::heapam::{
    HeapScanDescData, HEAP_INSERT_FROZEN, HEAP_INSERT_NO_LOGICAL, HEAP_INSERT_SKIP_FSM,
    HEAP_INSERT_SPECULATIVE,
};
use crate::c::{CommandId, TransactionId};

// The descriptor is now auto-`Send`: it borrows the relation/snapshot
// (`&'rel RelationData` / `&'snap SnapshotData`, both Send now that RelationData
// is auto-Send+Sync) and `ctup` owns its body (an `Option<Box<[u64]>>`
// -- Send). No `unsafe impl Send` is needed; `SendPtr`/`SendTuple` are retired
// (relation-ownership-plan step 9, the last non-shmem `unsafe impl Send`).

/// `heap_prepare_insert`: fill in the tuple header's transaction fields, returning
/// the (mutably borrowed) tuple to actually store. M2: no toast (the tuple fits
/// inline), so the caller's tuple is returned unchanged after its header is stamped.
fn heap_prepare_insert<'t>(
    relation: &RelationData,
    tup: &'t mut HeapTupleData,
    xid: TransactionId,
    cid: CommandId,
    options: i32,
) -> &'t mut HeapTupleData {
    // SAFETY: live in-memory tuple header (owned body, built by heap_form_tuple).
    let data: &mut HeapTupleHeaderData = unsafe { &mut *tup.t_data_mut() };

    data.t_infomask &= !HEAP_XACT_MASK;
    data.t_infomask2 &= !HEAP2_XACT_MASK;
    data.t_infomask |= HEAP_XMAX_INVALID;
    data.set_xmin(xid);
    if (options & HEAP_INSERT_FROZEN) != 0 {
        data.set_xmin_frozen();
    }

    data.set_cmin(cid);
    data.set_xmax(TransactionId(0)); // for cleanliness
    tup.t_tableOid = relation.relid();

    // M2: only plain tables / matviews reach here; no out-of-line toasting (the
    // tuple is known to fit). The toast path (heap_toast_insert_or_update) is a
    // grow guard.
    let relkind = relation.form().relkind;
    crate::assert!(
        relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW || relkind == RELKIND_SEQUENCE,
        "heap_insert supports only RELKIND_RELATION/MATVIEW/SEQUENCE"
    );
    tup
}

/// `heap_insert`: insert `tup` into `relation`, emitting an `XLOG_HEAP_INSERT`
/// WAL record. Stamps the tuple's xmin/cmin, places it on a page with room
/// (extending the relation if needed), marks the buffer dirty, logs, and sets the
/// page LSN. `tup.t_self` is updated to the stored location.
///
/// `cid` is the inserting command id; `options` is the `HEAP_INSERT_*` bitset.
/// The caller must be inside a WAL insertion scope (`with_insertion`).
pub async fn heap_insert(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    tup: &mut HeapTupleData,
    cid: CommandId,
    options: i32,
) {
    let xid = GetCurrentTransactionId(shared).await;

    // Stamp the tuple header. The tuple (owned body) and `&RelationData`
    // (RelationData: Sync) are both Send, so they may cross the `.await`s below.
    heap_prepare_insert(relation, tup, xid, cid, options);
    let needs_wal = relation.needs_wal();
    let t_len = tup.t_len as usize;

    // Find a buffer with room for this tuple (pins it; not locked).
    let buffer = relation_get_buffer_for_tuple(shared, relation, t_len, options).await;
    let block = shared.buffers().buffer_get_block_number(buffer);

    // Place the tuple + (if WAL'd) stage the record, all under the exclusive
    // content lock with NO `.await` in between (the C critical section). The WAL
    // data is staged synchronously (begin/register_*); the awaiting `xlog_insert`
    // runs AFTER the content lock is dropped below.
    let (info, offnum, xlhdr, tuple_body): (u8, OffsetNumber, Option<xl_heap_header>, Vec<u8>) = {
        // heaptup == tup (no toast copy in M2).
        let pool = shared.buffers();
        let _g = pool.content_exclusive(buffer);
        let buf_id = buf_id_of(buffer);
        // SAFETY: exclusive content lock held -> sole writer to this slot.
        let page = unsafe { pool.block_mut(buf_id) };

        relation_put_heap_tuple(page, block, tup);
        pool.mark_buffer_dirty(buffer);

        let offnum = tup.t_self.offset_number();

        if needs_wal {
            // If this is the single, first tuple on the page, we can re-init the
            // page on replay instead of restoring the whole thing.
            let mut info = XLOG_HEAP_INSERT;
            if offnum == FIRST_OFFSET_NUMBER && page.get_max_offset_number() == FIRST_OFFSET_NUMBER {
                info |= XLOG_HEAP_INIT_PAGE;
            }

            // SAFETY: live tuple header; copy out the reduced WAL header + the
            // bytes after the heap-tuple header (bitmap + data).
            let (xlhdr, body) = unsafe {
                let d = &*tup.t_data();
                let xlhdr = xl_heap_header {
                    t_infomask2: d.t_infomask2,
                    t_infomask: d.t_infomask,
                    t_hoff: d.t_hoff,
                };
                let total = tup.t_len as usize;
                let src = (tup.t_data().cast::<u8>()).add(SizeofHeapTupleHeader);
                let body = core::slice::from_raw_parts(src, total - SizeofHeapTupleHeader).to_vec();
                (xlhdr, body)
            };
            (info, offnum, Some(xlhdr), body)
        } else {
            (XLOG_HEAP_INSERT, offnum, None, Vec::new())
        }
        // content lock dropped here
    };

    if let Some(xlhdr) = xlhdr {
        let recptr = emit_insert_wal(
            shared, relation, buffer, block, info, offnum, &xlhdr, &tuple_body, options,
        )
        .await;
        // Set the page LSN under the exclusive lock (sync, no await).
        let pool = shared.buffers();
        let _g = pool.content_exclusive(buffer);
        let buf_id = buf_id_of(buffer);
        // SAFETY: exclusive content lock held -> sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        page.set_lsn(recptr);
    }

    shared.buffers().release_buffer(buffer);

    // M2: cache invalidation, pgstat, and copying t_self back into the caller's
    // image (when heaptup is a private toast copy) are grow guards -- here heaptup
    // IS tup, so t_self is already updated in place.
}

/// Emit the `XLOG_HEAP_INSERT` record. Split out so the content lock is dropped
/// before the (awaiting) `xlog_insert`. The page image is registered (copied)
/// inside `register_block`, so the lock need not be held.
#[allow(clippy::too_many_arguments, reason = "mirrors the C XLOG block's locals")]
async fn emit_insert_wal(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    buffer: Buffer,
    block: BlockNumber,
    info: u8,
    offnum: OffsetNumber,
    xlhdr: &xl_heap_header,
    tuple_body: &[u8],
    _options: i32,
) -> crate::access::xlogdefs::XLogRecPtr {
    let locator = relation.rd_locator;

    let xlrec = xl_heap_insert { offnum, flags: 0 };

    begin_insert();
    register_data(as_bytes(&xlrec, SizeOfHeapInsert));

    // Mark the header as belonging to the buffer; if a full-page image is taken,
    // xl_heap_header need not be stored separately.
    let mut bufflags = RegBuf::STANDARD;
    if (info & XLOG_HEAP_INIT_PAGE) != 0 {
        bufflags |= RegBuf::WILL_INIT;
    }

    // Register the page image (copied here). Read the current page bytes under a
    // shared lock for the copy.
    {
        let pool = shared.buffers();
        let _g = pool.content_share(buffer);
        let page = pool.buffer_get_page(buffer);
        register_block(0, &locator, ForkNumber::MAIN_FORKNUM, block, page, bufflags);
    }
    register_buf_data(0, as_bytes(xlhdr, SizeOfHeapHeader));
    // PG73FORMAT: write bitmap [+ padding] [+ oid] + data.
    register_buf_data(0, tuple_body);

    // Filtering by origin on a row level is much more efficient.
    set_record_flags(XLogRecordFlags::INCLUDE_ORIGIN);

    xlog_insert(shared.xlog(), RmgrId::Heap as u8, info).await
}

/// Reinterpret a `#[repr(C)]` WAL fixed-part struct as its leading `size` bytes.
///
/// SAFETY: `T` is a `#[repr(C)]` POD WAL header; `size` is its `SizeOf*` (the
/// offset of the last field + its size), <= `size_of::<T>()`.
fn as_bytes<T>(v: &T, size: usize) -> &[u8] {
    debug_assert!(size <= core::mem::size_of::<T>());
    // SAFETY: see doc; reading `size` bytes of a live `repr(C)` value.
    unsafe { core::slice::from_raw_parts(std::ptr::from_ref(v).cast::<u8>(), size) }
}

// ---------------------------------------------------------------------------
// Sequential scan (page-at-a-time, forward; MVCC).
// ---------------------------------------------------------------------------

/// `heap_beginscan`: start a forward MVCC sequential scan of `relation`. Returns a
/// boxed `HeapScanDescData` (the C palloc'd descriptor). M2 supports seqscan with
/// an MVCC snapshot and page-at-a-time mode; other scan types are grow guards.
///
/// Borrow-based ownership (relation-ownership-plan step 1): the descriptor borrows
/// the relation (`&'rel RelationData`) and snapshot (`&'snap SnapshotData`). The
/// `Arc` owners live in the caller's stack frame and must outlive the descriptor
/// (and every `heap_getnext`/`heap_endscan` driven on it).
pub fn heap_beginscan<'rel, 'snap>(
    relation: &'rel RelationData,
    snapshot: &'snap SnapshotData,
    nkeys: i32,
    flags: ScanOptions,
) -> Box<HeapScanDescData<'rel, 'snap>> {
    crate::assert!(nkeys == 0, "M2 heap scan supports no scan keys");

    let mut flags = flags;
    // Disable page-at-a-time mode if the snapshot is not MVCC-safe.
    if snapshot.snapshot_type != SnapshotType::Mvcc {
        flags &= !ScanOptions::ALLOW_PAGEMODE;
    }

    let table_oid = relation.rd_id;

    let mut t_self = crate::storage::itemptr::ItemPointerData {
        blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
        posid: 0,
    };
    t_self.set_invalid();
    let ctup = HeapTupleData::null(t_self, table_oid);

    let base = TableScanDescData {
        rs_rd: relation,
        rs_snapshot: snapshot,
        rs_nkeys: nkeys,
        rs_key: Vec::new(),
        st: TableScanType::None,
        rs_flags: flags.bits(),
        rs_parallel: None,
    };

    let scan = HeapScanDescData {
        base,
        nblocks: 0,
        startblock: 0,
        numblocks: INVALID_BLOCK_NUMBER,
        inited: false,
        coffset: 0,
        cblock: INVALID_BLOCK_NUMBER,
        cbuf: InvalidBuffer,
        strategy: None,
        ctup,
        read_stream: None,
        dir: ScanDirection::Forward,
        prefetch_block: INVALID_BLOCK_NUMBER,
        parallelworkerdata: None,
        cindex: 0,
        ntuples: 0,
        vistuples: [0; crate::access::htup_details::MaxHeapTuplesPerPage as usize],
    };
    Box::new(scan)
}

/// `heap_endscan`: release scan resources -- unpin the current buffer. The
/// borrowed snapshot/relation `Arc`s are owned by the caller's frame (no reclaim
/// here). The boxed descriptor is dropped by the caller (it owns the `Box`).
pub fn heap_endscan(shared: &Arc<SharedState>, scan: &mut HeapScanDescData<'_, '_>) {
    if scan.cbuf != InvalidBuffer {
        shared.buffers().release_buffer(scan.cbuf);
        scan.cbuf = InvalidBuffer;
    }
}

/// `heap_rescan`: restart a scan from the beginning (M2: forward seqscan only).
pub fn heap_rescan(shared: &Arc<SharedState>, scan: &mut HeapScanDescData<'_, '_>) {
    if scan.cbuf != InvalidBuffer {
        shared.buffers().release_buffer(scan.cbuf);
        scan.cbuf = InvalidBuffer;
    }
    initscan(scan);
}

/// `initscan` (M2 subset): reset the scan cursor. The block count is read lazily
/// on the first `heap_getnext` (it needs an `.await`, which `initscan` avoids).
fn initscan(scan: &mut HeapScanDescData<'_, '_>) {
    scan.startblock = 0;
    scan.numblocks = INVALID_BLOCK_NUMBER;
    scan.inited = false;
    scan.ctup.body = None;
    scan.ctup.t_self.set_invalid();
    scan.cbuf = InvalidBuffer;
    scan.cblock = INVALID_BLOCK_NUMBER;
    scan.ntuples = 0;
    scan.cindex = 0;
    scan.dir = ScanDirection::Forward;
    scan.prefetch_block = INVALID_BLOCK_NUMBER;
}

/// `heap_getnext`: advance the scan and return the next visible tuple, or `None`
/// at end of scan. M2: forward, page-at-a-time, MVCC only. The returned
/// `HeapTuple` references `scan.ctup`, whose body is an owned copy of the page
/// item (no longer aliases the pinned page); valid until the next
/// `heap_getnext`/`heap_endscan`.
pub async fn heap_getnext(
    shared: &Arc<SharedState>,
    scan: &mut HeapScanDescData<'_, '_>,
    direction: ScanDirection,
) -> Option<HeapTuple> {
    crate::assert!(
        scan_direction_is_forward(direction),
        "M2 heap scan supports forward direction only"
    );

    heapgettup_pagemode(shared, scan, direction).await;

    if scan.ctup.t_data_is_null() {
        return None;
    }
    Some(std::ptr::from_mut(&mut scan.ctup))
}

/// `heapgettup_pagemode` (M2 forward subset): walk pages forward, returning the
/// next visible tuple. On each new page it runs `heap_prepare_pagescan` (the
/// per-page visibility collection) and then yields the page's visible tuples in
/// order. The current buffer stays pinned across calls (the page-at-a-time
/// contract); a fresh page is read via `read_buffer_common`.
async fn heapgettup_pagemode<'rel>(
    shared: &Arc<SharedState>,
    scan: &mut HeapScanDescData<'rel, '_>,
    dir: ScanDirection,
) {
    // Copy the relation borrow out (a `&'rel RelationData` is `Copy`) so the scan's
    // `&mut` fields stay mutable in the loop while we read the relation.
    let relation: &'rel RelationData = scan.base.rs_rd;

    // Continue from the previously returned page/tuple if the scan is inited.
    let mut lineindex = if scan.inited {
        scan.cindex + 1
    } else {
        0
    };

    loop {
        if scan.inited && lineindex < scan.ntuples {
            // Emit the next visible tuple from the current page.
            let lineoff = scan.vistuples[lineindex as usize];
            let pool = shared.buffers();
            let page = pool.buffer_get_page(scan.cbuf);
            let item_id = page.get_item_id(lineoff);
            let item = page.get_item(&item_id);
            // PG returns a tuple pointing into the pinned page; here `ctup` OWNS its
            // body, so copy the page item's bytes into a fresh owned body (a
            // per-tuple copy on the hot path -- the cost of a genuinely-Send
            // descriptor). The returned tuple stays valid for the scan's life,
            // independent of the pin. SAFETY: a normal item's bytes begin with a
            // HeapTupleHeaderData and `item` is `item.len()` readable page bytes.
            let len = item.len();
            let body = unsafe { crate::access::htup::tuple_body_from_raw(item.as_ptr(), len) };
            scan.ctup.body = Some(body);
            scan.ctup.t_len = len as u32;
            scan.ctup.t_self.set(scan.cblock, lineoff);
            scan.cindex = lineindex;
            return;
        }

        // Need the next page. Release the current buffer, advance the block.
        if scan.cbuf != InvalidBuffer {
            shared.buffers().release_buffer(scan.cbuf);
            scan.cbuf = InvalidBuffer;
        }

        // Read the relation block count once, on first entry.
        if scan.inited {
            scan.cblock = scan.cblock.wrapping_add(1);
        } else {
            let smgr_ptr = relation.smgr();
            // SAFETY: relcache-owned smgr handle, valid while the rel is open.
            let smgr = unsafe { &mut *smgr_ptr };
            scan.nblocks = smgr.nblocks(shared, ForkNumber::MAIN_FORKNUM).await;
            scan.cblock = scan.startblock;
            scan.inited = true;
        }

        if scan.cblock >= scan.nblocks {
            // End of scan.
            scan.ctup.body = None;
            scan.inited = false;
            scan.cblock = INVALID_BLOCK_NUMBER;
            return;
        }

        scan.cbuf = read_relation_block(shared, relation, scan.cblock).await;
        heap_prepare_pagescan(shared, scan).await;
        lineindex = 0;
        // Loop back: emit from the freshly collected page (or skip to the next
        // page if it had no visible tuples).
    }
}

/// Read a main-fork block of `relation` into a pinned buffer.
async fn read_relation_block(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    block: BlockNumber,
) -> Buffer {
    let relpersistence = relation.form().relpersistence;
    let smgr_ptr = relation.smgr();
    // SAFETY: relcache-owned smgr handle valid while rel is open.
    let smgr = unsafe { &mut *smgr_ptr };
    crate::backend::storage::buffer::bufmgr::read_buffer_common(
        shared,
        smgr,
        relpersistence,
        ForkNumber::MAIN_FORKNUM,
        block,
        crate::storage::bufmgr::ReadBufferMode::NORMAL,
        None,
    )
    .await
}

/// The global buffer-pool slot index for a pinned shared buffer.
fn buf_id_of(buffer: Buffer) -> i32 {
    #[allow(clippy::expect_used, reason = "heap pages are always shared (global) buffers")]
    let id = buffer.as_global().expect("shared buffer expected") as i32;
    id
}

/// A zeroed (invalid) item pointer.
fn zero_item_pointer() -> crate::storage::itemptr::ItemPointerData {
    let mut ip = crate::storage::itemptr::ItemPointerData {
        blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
        posid: 0,
    };
    ip.set_invalid();
    ip
}

/// `heap_prepare_pagescan` (M2): collect the visible-tuple offsets of the current
/// page into `scan.vistuples`. The per-tuple MVCC visibility test runs with the
/// SHARE content lock dropped (it `.await`s clog/subtrans); only the pin is held
/// during the test, which is sound because the page's line pointers and tuple
/// bytes cannot change under a snapshot that can't see concurrent writers (PG's
/// page-at-a-time guarantee).
async fn heap_prepare_pagescan(shared: &Arc<SharedState>, scan: &mut HeapScanDescData<'_, '_>) {
    // The snapshot is borrowed from the caller's frame (valid for the scan).
    let snapshot: &SnapshotData = scan.base.rs_snapshot;

    // Snapshot the page's line count + collect candidate (block, offset) under a
    // brief SHARE lock; then drop it before the awaiting visibility tests.
    let lines = {
        let pool = shared.buffers();
        let _g = pool.content_share(scan.cbuf);
        let page = pool.buffer_get_page(scan.cbuf);
        page.get_max_offset_number()
    };

    let mut ntup: u32 = 0;
    for lineoff in FIRST_OFFSET_NUMBER..=lines {
        // Read the candidate header bytes under a brief shared lock, copy the
        // header out, drop the lock, then test visibility (which awaits).
        let header_copy = {
            let pool = shared.buffers();
            let _g = pool.content_share(scan.cbuf);
            let page = pool.buffer_get_page(scan.cbuf);
            let item_id = page.get_item_id(lineoff);
            if item_id.is_normal() {
                let item = page.get_item(&item_id);
                // SAFETY: a normal heap item begins with a HeapTupleHeaderData;
                // copy the fixed header out (visibility needs only the header).
                let hdr = unsafe { read_header(item) };
                Some(hdr)
            } else {
                None
            }
        };

        let Some(hdr) = header_copy else { continue };

        if heap_tuple_satisfies_mvcc(shared, &hdr, snapshot).await {
            scan.vistuples[ntup as usize] = lineoff;
            ntup += 1;
        }
    }

    scan.ntuples = ntup;
}

/// Copy a heap tuple's fixed 23-byte header out of a page item's bytes.
///
/// SAFETY: `item` is the bytes of a normal heap line pointer, which begin with a
/// `HeapTupleHeaderData`; at least `SizeofHeapTupleHeader` bytes are present.
unsafe fn read_header(item: &[u8]) -> HeapTupleHeaderData {
    debug_assert!(item.len() >= SizeofHeapTupleHeader);
    // SAFETY: read the header by value via an unaligned read (the slice may not
    // be 8-aligned within our borrow, though on-page it is); copies POD bytes.
    unsafe { std::ptr::read_unaligned(item.as_ptr().cast::<HeapTupleHeaderData>()) }
}

// ---------------------------------------------------------------------------
// Grow guards: ops added at later milestones (M6 index fetch, M8 update/delete).
// ---------------------------------------------------------------------------

/// `heap_fetch`: fetch the tuple at a TID with a visibility test. Grow guard
/// (M6: index fetch / row-version fetch).
pub fn heap_fetch() {
    unimplemented!("heap_fetch: M6 (index fetch / row-version)")
}

/// Fetch the heap tuple at `tid` from `relation`, applying the MVCC visibility
/// test against `snapshot`, returning an OWNED copy (header+data) or `None` if the
/// line pointer is unused or the tuple is invisible. This is the table-AM index
/// fetch primitive (`table_index_fetch_tuple` -> heap) used by the index scan +
/// systable index path; it reads one heap block, tests visibility with the
/// content lock dropped (only the pin held, like the page-at-a-time scan), and
/// copies the bytes out so the caller need not hold a pin.
pub async fn heap_fetch_tid(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    tid: &crate::storage::itemptr::ItemPointerData,
    snapshot: &SnapshotData,
) -> Option<Box<HeapTupleData>> {
    let block = tid.block_number();
    let offnum = tid.offset_number();

    let buffer = read_relation_block(shared, relation, block).await;

    // Copy the candidate header out under a brief share lock, drop it, test
    // visibility (awaits), then copy the full tuple bytes under another brief lock.
    let header_copy = {
        let pool = shared.buffers();
        let _g = pool.content_share(buffer);
        let page = pool.buffer_get_page(buffer);
        if offnum == 0 || offnum > page.get_max_offset_number() {
            None
        } else {
            let item_id = page.get_item_id(offnum);
            if item_id.is_normal() {
                let item = page.get_item(&item_id);
                // SAFETY: a normal heap item begins with a HeapTupleHeaderData.
                Some(unsafe { read_header(item) })
            } else {
                None
            }
        }
    };

    let Some(hdr) = header_copy else {
        shared.buffers().release_buffer(buffer);
        return None;
    };

    let visible = heap_tuple_satisfies_mvcc(shared, &hdr, snapshot).await;
    if !visible {
        shared.buffers().release_buffer(buffer);
        return None;
    }

    // Copy the full tuple out under a brief share lock into an owned body so the
    // caller need not hold a pin.
    let result = {
        let pool = shared.buffers();
        let _g = pool.content_share(buffer);
        let page = pool.buffer_get_page(buffer);
        let item_id = page.get_item_id(offnum);
        let item = page.get_item(&item_id);
        // SAFETY: a normal heap item's bytes begin with a HeapTupleHeaderData; the
        // page stays pinned and content-locked for this borrow; `item` is
        // `item.len()` readable page bytes.
        let body = unsafe { crate::access::htup::tuple_body_from_raw(item.as_ptr(), item.len()) };
        let mut tuple = HeapTupleData {
            t_len: item.len() as u32,
            t_self: zero_item_pointer(),
            t_tableOid: relation.rd_id,
            body: Some(body),
        };
        tuple.t_self.set(block, offnum);
        Box::new(tuple)
    };

    shared.buffers().release_buffer(buffer);
    Some(result)
}

/// The max line-pointer offset of a heap block (for the bitmap heap scan's
/// lossy-page path, where the bitmap only records that a page must be visited and
/// the scan must examine every tuple offset on it). Reads the page line count under
/// a brief SHARE lock and releases the buffer. Returns 0 for an empty/new page.
pub async fn heap_block_max_offset(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    block: BlockNumber,
) -> OffsetNumber {
    let buffer = read_relation_block(shared, relation, block).await;
    let max = {
        let pool = shared.buffers();
        let _g = pool.content_share(buffer);
        pool.buffer_get_page(buffer).get_max_offset_number()
    };
    shared.buffers().release_buffer(buffer);
    max
}

// ---------------------------------------------------------------------------
// Update / delete / row-lock (M8). Translated from heap_delete / heap_update /
// heap_lock_tuple. See module docs + rules.md s4 for what is staged.
// ---------------------------------------------------------------------------

/// `compute_infobits`: the `infobits_set` byte stored in the delete/update/lock
/// WAL records, derived from the tuple's infomask/infomask2.
fn compute_infobits(infomask: u16, infomask2: u16) -> u8 {
    (if (infomask & HEAP_XMAX_IS_MULTI) != 0 { XLHL_XMAX_IS_MULTI } else { 0 })
        | (if (infomask & HEAP_XMAX_LOCK_ONLY) != 0 { XLHL_XMAX_LOCK_ONLY } else { 0 })
        | (if (infomask & HEAP_XMAX_EXCL_LOCK) != 0 { XLHL_XMAX_EXCL_LOCK } else { 0 })
        // note we ignore HEAP_XMAX_SHR_LOCK here
        | (if (infomask & HEAP_XMAX_KEYSHR_LOCK) != 0 { XLHL_XMAX_KEYSHR_LOCK } else { 0 })
        | (if (infomask2 & HEAP_KEYS_UPDATED) != 0 { XLHL_KEYS_UPDATED } else { 0 })
}

/// `compute_new_xmax_infomask` (single-locker subset): given the existing
/// xmax/infomask of a tuple and the current xid acquiring a lock/update of `mode`,
/// compute the new `(xmax, infomask, infomask2)` to store. Returns the triple (the
/// three C out-params).
///
/// Staged (multixact, rules.md s4): only the `HEAP_XMAX_INVALID` arm (no previous
/// locker -- the common case) is implemented. A live/committed/multixact previous
/// xmax requires `MultiXactIdExpand`/`MultiXactIdCreate`, which are not yet
/// reachable; those arms `unimplemented!()` with a clear message.
fn compute_new_xmax_infomask(
    old_xmax: TransactionId,
    old_infomask: u16,
    _old_infomask2: u16,
    add_to_xmax: TransactionId,
    mode: LockTupleMode,
    is_update: bool,
) -> (TransactionId, u16, u16) {
    debug_assert!(TransactionIdIsCurrentTransactionId(add_to_xmax));

    if (old_infomask & HEAP_XMAX_INVALID) != 0 {
        // No previous locker; we just insert our own TransactionId.
        let mut new_infomask: u16 = 0;
        let mut new_infomask2: u16 = 0;
        let new_xmax;
        if is_update {
            new_xmax = add_to_xmax;
            if mode == LockTupleMode::LockTupleExclusive {
                new_infomask2 |= HEAP_KEYS_UPDATED;
            }
        } else {
            new_infomask |= HEAP_XMAX_LOCK_ONLY;
            match mode {
                LockTupleMode::LockTupleKeyShare => {
                    new_xmax = add_to_xmax;
                    new_infomask |= HEAP_XMAX_KEYSHR_LOCK;
                }
                LockTupleMode::LockTupleShare => {
                    new_xmax = add_to_xmax;
                    new_infomask |= HEAP_XMAX_SHR_LOCK;
                }
                LockTupleMode::LockTupleNoKeyExclusive => {
                    new_xmax = add_to_xmax;
                    new_infomask |= HEAP_XMAX_EXCL_LOCK;
                }
                LockTupleMode::LockTupleExclusive => {
                    new_xmax = add_to_xmax;
                    new_infomask |= HEAP_XMAX_EXCL_LOCK;
                    new_infomask2 |= HEAP_KEYS_UPDATED;
                }
            }
        }
        (new_xmax, new_infomask, new_infomask2)
    } else {
        // A previous locker/updater (in-progress, committed, or a multixact)
        // must be folded into a MultiXactId. Not yet reachable.
        let _ = old_xmax;
        unimplemented!(
            "compute_new_xmax_infomask: previous live/committed/multi xmax -- staged with multixact (step 33)"
        )
    }
}

/// Cast the on-page tuple header (located by `lp_off`) to a mutable reference for
/// in-place mutation of the transaction fields.
///
/// SAFETY: an exclusive content lock is held (sole writer); `lp_off` addresses a
/// normal heap item whose bytes begin with a `HeapTupleHeaderData`, and the page
/// is 8-aligned with MAXALIGN'd item offsets so the overlay is soundly aligned.
unsafe fn page_tuple_header_mut(page: &mut Page, lp_off: usize) -> &mut HeapTupleHeaderData {
    #[allow(
        clippy::cast_ptr_alignment,
        reason = "sound overlay: page is 8-aligned and item offsets are MAXALIGN'd, so HeapTupleHeaderData's align divides the address"
    )]
    let hdr = page.as_mut_bytes()[lp_off..].as_mut_ptr().cast::<HeapTupleHeaderData>();
    unsafe { &mut *hdr }
}

/// `heap_delete`: delete the tuple at `tid` in `relation`. Runs the
/// `HeapTupleSatisfiesUpdate` visibility check; on `TM_Ok` it stamps the tuple's
/// xmax with the current xid (plus the lock/delete infomask bits, clearing
/// `HEAP_XMAX_INVALID`), self-points `t_ctid`, sets cmax, marks the buffer dirty,
/// emits an `XLOG_HEAP_DELETE` record (mirroring `heap_insert`'s WAL pattern), and
/// sets the page LSN. On a non-Ok result it fills `tmfd` and returns without
/// mutating the tuple.
///
/// Staged (rules.md s4): the `TM_BeingModified && wait` arm (concurrent-update
/// wait + retry loop, multixact lockers), serializable-conflict checks, toast
/// delete, replica-identity logging, cache invalidation, and the visibility-map /
/// all-visible bookkeeping. The simple single-version delete is complete.
#[allow(clippy::too_many_arguments, reason = "mirrors the C heap_delete signature")]
pub async fn heap_delete(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    tid: &ItemPointerData,
    cid: CommandId,
    _crosscheck: Option<&SnapshotData>,
    _wait: bool,
    _changing_part: bool,
) -> (TM_Result, TM_FailureData) {
    crate::assert!(tid.is_valid(), "heap_delete: invalid tid");
    crate::assert!(
        !IsInParallelMode(),
        "cannot delete tuples during a parallel operation"
    );

    let xid = GetCurrentTransactionId(shared).await;
    let block = tid.block_number();
    let offnum = tid.offset_number();
    let buffer = read_relation_block(shared, relation, block).await;

    // Visibility check: copy the header out under a brief exclusive lock, drop the
    // lock for the (awaiting) HeapTupleSatisfiesUpdate, then re-lock to mutate.
    let pool = shared.buffers();
    let buf_id = buf_id_of(buffer);

    let (hdr_copy, lp_off) = {
        let _g = pool.content_exclusive(buffer);
        // SAFETY: exclusive content lock -> sole reader/writer.
        let page = unsafe { pool.block_mut(buf_id) };
        let item_id = page.get_item_id(offnum);
        crate::assert!(item_id.is_normal(), "heap_delete: target line pointer not normal");
        let off = item_id.lp_off() as usize;
        // SAFETY: normal heap item; lock held.
        let hdr = unsafe { read_header(page.get_item(&item_id)) };
        (hdr, off)
    };

    let result = HeapTupleSatisfiesUpdate(shared, &hdr_copy, tid, cid).await;

    if result == TM_Result::Invisible {
        pool.release_buffer(buffer);
        crate::elog!(ERROR, "attempted to delete invisible tuple");
    }

    if result != TM_Result::Ok {
        // TM_BeingModified wait/retry is staged; surface the failure data the
        // caller (step 34 executor) needs to classify the conflict.
        let mut tmfd = empty_failure_data();
        tmfd.ctid = hdr_copy.ctid;
        tmfd.xmax = hdr_copy.get_update_xid();
        tmfd.cmax = if result == TM_Result::SelfModified {
            HeapTupleHeaderGetCmax(&hdr_copy)
        } else {
            InvalidCommandId
        };
        pool.release_buffer(buffer);
        return (result, tmfd);
    }

    // Replace cid with a combo CID if necessary.
    let (cmax, iscombo) = HeapTupleHeaderAdjustCmax(&hdr_copy, cid);

    let (new_xmax, new_infomask, new_infomask2) = compute_new_xmax_infomask(
        hdr_copy.get_raw_xmax(),
        hdr_copy.t_infomask,
        hdr_copy.t_infomask2,
        xid,
        LockTupleMode::LockTupleExclusive,
        true,
    );

    let needs_wal = relation.needs_wal();
    let (offnum, infobits): (OffsetNumber, u8) = {
        let _g = pool.content_exclusive(buffer);
        // SAFETY: exclusive content lock -> sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        // SAFETY: lp_off addresses the normal header; lock held.
        let data = unsafe { page_tuple_header_mut(page, lp_off) };

        // Store transaction information of xact deleting the tuple.
        data.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        data.t_infomask2 &= !HEAP_KEYS_UPDATED;
        data.t_infomask |= new_infomask;
        data.t_infomask2 |= new_infomask2;
        data.clear_hot_updated();
        data.set_xmax(new_xmax);
        data.set_cmax(cmax, iscombo);
        // Make sure there is no forward chain link in t_ctid (self-point).
        data.ctid = *tid;

        let infobits = compute_infobits(data.t_infomask, data.t_infomask2);
        pool.mark_buffer_dirty(buffer);
        (data.ctid.offset_number(), infobits)
        // content lock dropped here
    };

    if needs_wal {
        let recptr =
            emit_delete_wal(shared, relation, buffer, block, offnum, new_xmax, infobits).await;
        let _g = pool.content_exclusive(buffer);
        // SAFETY: exclusive content lock -> sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        page.set_lsn(recptr);
    }

    pool.release_buffer(buffer);
    (TM_Result::Ok, empty_failure_data())
}

/// Emit the `XLOG_HEAP_DELETE` record. Mirrors `emit_insert_wal`: the content lock
/// is dropped before the awaiting `xlog_insert`; the page image is registered
/// (copied) under a brief share lock.
#[allow(clippy::too_many_arguments, reason = "mirrors the C XLOG block's locals")]
async fn emit_delete_wal(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    buffer: Buffer,
    block: BlockNumber,
    offnum: OffsetNumber,
    xmax: TransactionId,
    infobits_set: u8,
) -> crate::access::xlogdefs::XLogRecPtr {
    let locator = relation.rd_locator;
    let xlrec = xl_heap_delete { xmax, offnum, infobits_set, flags: 0 };

    begin_insert();
    register_data(as_bytes(&xlrec, SizeOfHeapDelete));
    {
        let pool = shared.buffers();
        let _g = pool.content_share(buffer);
        let page = pool.buffer_get_page(buffer);
        register_block(0, &locator, ForkNumber::MAIN_FORKNUM, block, page, RegBuf::STANDARD);
    }
    set_record_flags(XLogRecordFlags::INCLUDE_ORIGIN);
    xlog_insert(shared.xlog(), RmgrId::Heap as u8, XLOG_HEAP_DELETE).await
}

/// `heap_update`: replace the tuple at `otid` with `newtup`. Runs the
/// `HeapTupleSatisfiesUpdate` visibility check; on `TM_Ok` it stamps the new
/// tuple's xmin (with `HEAP_UPDATED`), places it (same page if it fits, else a
/// fresh page via `relation_get_buffer_for_tuple`), then stamps the OLD tuple's
/// xmax + points its `t_ctid` at the new version, marks both buffers dirty, emits
/// an `XLOG_HEAP_UPDATE` record (mirroring `heap_insert`), and sets the page LSN.
/// Returns `(TM_Result, lockmode, update_indexes)` (the C out-params).
///
/// Staged (rules.md s4): HOT update detection (always a full update here, both
/// tuples marked not-HOT), the `TM_BeingModified && wait` retry loop, toast,
/// key-intact weaker-lock detection (always `LockTupleExclusive`), multixact
/// lockers on the old tuple, replica identity, cache invalidation, and VM
/// bookkeeping. The non-HOT single-version update (same-page or new-page) is
/// complete.
#[allow(clippy::too_many_arguments, reason = "mirrors the C heap_update signature")]
#[allow(
    clippy::too_many_lines,
    reason = "faithful translation of heap_update's same-page vs new-page paths + WAL emit"
)]
#[allow(clippy::similar_names, reason = "infomask_old_tuple/infomask2_old_tuple mirror PG identifiers")]
pub async fn heap_update(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    otid: &ItemPointerData,
    newtup: &mut HeapTupleData,
    cid: CommandId,
    _crosscheck: Option<&SnapshotData>,
    _wait: bool,
) -> (TM_Result, LockTupleMode, TU_UpdateIndexes) {
    crate::assert!(otid.is_valid(), "heap_update: invalid otid");
    crate::assert!(
        !IsInParallelMode(),
        "cannot update tuples during a parallel operation"
    );

    let xid = GetCurrentTransactionId(shared).await;
    // We don't yet compute modified key columns, so always take the strongest
    // lock (key-intact weaker-lock detection is staged).
    let lockmode = LockTupleMode::LockTupleExclusive;

    let block = otid.block_number();
    let offnum = otid.offset_number();
    let buffer = read_relation_block(shared, relation, block).await;
    let pool = shared.buffers();
    let buf_id = buf_id_of(buffer);

    let (old_hdr, old_lp_off) = {
        let _g = pool.content_exclusive(buffer);
        // SAFETY: exclusive content lock -> sole reader/writer.
        let page = unsafe { pool.block_mut(buf_id) };
        let item_id = page.get_item_id(offnum);
        crate::assert!(item_id.is_normal(), "heap_update: target line pointer not normal");
        let off = item_id.lp_off() as usize;
        // SAFETY: normal heap item; lock held.
        let hdr = unsafe { read_header(page.get_item(&item_id)) };
        (hdr, off)
    };

    let result = HeapTupleSatisfiesUpdate(shared, &old_hdr, otid, cid).await;

    if result == TM_Result::Invisible {
        pool.release_buffer(buffer);
        crate::elog!(ERROR, "attempted to update invisible tuple");
    }

    if result != TM_Result::Ok {
        let mut tmfd = empty_failure_data();
        tmfd.ctid = old_hdr.ctid;
        tmfd.xmax = old_hdr.get_update_xid();
        if result == TM_Result::SelfModified {
            tmfd.cmax = HeapTupleHeaderGetCmax(&old_hdr);
        }
        let _ = tmfd; // tmfd surfaced via the failure path at step 34
        pool.release_buffer(buffer);
        return (result, lockmode, TU_UpdateIndexes::None);
    }

    // Compute the old tuple's new xmax/infomask (an update lock by `xid`).
    let (xmax_old, infomask_old_tuple, infomask2_old_tuple) = compute_new_xmax_infomask(
        old_hdr.get_raw_xmax(),
        old_hdr.t_infomask,
        old_hdr.t_infomask2,
        xid,
        lockmode,
        true,
    );
    // With no surviving locker (HEAP_XMAX_INVALID on the old tuple), the new
    // version carries no xmax.
    let (infomask_new_tuple, infomask2_new_tuple) = (HEAP_XMAX_INVALID, 0u16);

    let (cmax, iscombo) = HeapTupleHeaderAdjustCmax(&old_hdr, cid);

    // Prepare the new tuple header: xmin = xid, cmin = cid, HEAP_UPDATED.
    {
        // SAFETY: live in-memory tuple header (owned body, built by heap_form_tuple).
        let nd: &mut HeapTupleHeaderData = unsafe { &mut *newtup.t_data_mut() };
        nd.t_infomask &= !HEAP_XACT_MASK;
        nd.t_infomask2 &= !HEAP2_XACT_MASK;
        nd.set_xmin(xid);
        nd.set_cmin(cid);
        nd.t_infomask |= HEAP_UPDATED | infomask_new_tuple;
        nd.t_infomask2 |= infomask2_new_tuple;
        nd.set_xmax(TransactionId(0));
        nd.clear_heap_only();
        nd.clear_hot_updated();
    }
    newtup.t_tableOid = relation.relid();

    let newtupsize = maxalign(newtup.t_len as usize);
    let pagefree = {
        let _g = pool.content_share(buffer);
        pool.buffer_get_page(buffer).get_heap_free_space()
    };

    let needs_wal = relation.needs_wal();
    let same_page = newtupsize <= pagefree;

    if same_page {
        // New tuple fits on the old page: insert it + stamp the old tuple, all
        // under one exclusive content lock (the C critical section).
        let (old_offnum, new_offnum, infobits, xlhdr, body): (
            OffsetNumber,
            OffsetNumber,
            u8,
            xl_heap_header,
            Vec<u8>,
        ) = {
            let _g = pool.content_exclusive(buffer);
            // SAFETY: exclusive content lock -> sole writer.
            let page = unsafe { pool.block_mut(buf_id) };
            relation_put_heap_tuple(page, block, newtup);
            let new_offnum = newtup.t_self.offset_number();

            // SAFETY: old_lp_off addresses the old header; lock held.
            let od = unsafe { page_tuple_header_mut(page, old_lp_off) };
            od.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
            od.t_infomask2 &= !HEAP_KEYS_UPDATED;
            od.set_xmax(xmax_old);
            od.t_infomask |= infomask_old_tuple;
            od.t_infomask2 |= infomask2_old_tuple;
            od.set_cmax(cmax, iscombo);
            // Record the address of the new tuple in t_ctid of the old one.
            od.ctid = newtup.t_self;
            let infobits = compute_infobits(od.t_infomask, od.t_infomask2);

            pool.mark_buffer_dirty(buffer);

            let (xlhdr, body) = new_tuple_wal_payload(newtup);
            (offnum, new_offnum, infobits, xlhdr, body)
        };

        if needs_wal {
            let recptr = emit_update_wal(
                shared, relation, buffer, block, buffer, block, old_offnum, xmax_old, infobits,
                new_offnum, &xlhdr, &body,
            )
            .await;
            let _g = pool.content_exclusive(buffer);
            // SAFETY: exclusive content lock -> sole writer.
            let page = unsafe { pool.block_mut(buf_id) };
            page.set_lsn(recptr);
        }
    } else {
        // New tuple needs a fresh page. Get a buffer with room (this may extend
        // the relation). Order matters in C to avoid deadlock; with one writer in
        // the foundation, a straight allocate-then-lock is sufficient.
        let newbuf = relation_get_buffer_for_tuple(shared, relation, newtup.t_len as usize, 0).await;
        let newblock = pool.buffer_get_block_number(newbuf);
        let newbuf_id = buf_id_of(newbuf);

        let new_offnum: OffsetNumber = {
            let _g = pool.content_exclusive(newbuf);
            // SAFETY: exclusive content lock on the new page -> sole writer.
            let page = unsafe { pool.block_mut(newbuf_id) };
            relation_put_heap_tuple(page, newblock, newtup);
            pool.mark_buffer_dirty(newbuf);
            newtup.t_self.offset_number()
        };

        let (old_offnum, infobits): (OffsetNumber, u8) = {
            let _g = pool.content_exclusive(buffer);
            // SAFETY: exclusive content lock on the old page -> sole writer.
            let page = unsafe { pool.block_mut(buf_id) };
            // SAFETY: old_lp_off addresses the old header; lock held.
            let od = unsafe { page_tuple_header_mut(page, old_lp_off) };
            od.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
            od.t_infomask2 &= !HEAP_KEYS_UPDATED;
            od.set_xmax(xmax_old);
            od.t_infomask |= infomask_old_tuple;
            od.t_infomask2 |= infomask2_old_tuple;
            od.set_cmax(cmax, iscombo);
            od.ctid = newtup.t_self;
            let infobits = compute_infobits(od.t_infomask, od.t_infomask2);
            pool.mark_buffer_dirty(buffer);
            (offnum, infobits)
        };

        if needs_wal {
            let (xlhdr, body) = {
                let _g = pool.content_share(newbuf);
                new_tuple_wal_payload(newtup)
            };
            let recptr = emit_update_wal(
                shared, relation, buffer, block, newbuf, newblock, old_offnum, xmax_old, infobits,
                new_offnum, &xlhdr, &body,
            )
            .await;
            {
                let _g = pool.content_exclusive(newbuf);
                // SAFETY: exclusive content lock -> sole writer.
                unsafe { pool.block_mut(newbuf_id) }.set_lsn(recptr);
            }
            {
                let _g = pool.content_exclusive(buffer);
                // SAFETY: exclusive content lock -> sole writer.
                unsafe { pool.block_mut(buf_id) }.set_lsn(recptr);
            }
        }

        pool.release_buffer(newbuf);
    }

    pool.release_buffer(buffer);
    (TM_Result::Ok, lockmode, TU_UpdateIndexes::All)
}

/// Build the `xl_heap_update` new-tuple WAL payload: the reduced header + the
/// bytes after the heap-tuple header (bitmap + data).
fn new_tuple_wal_payload(newtup: &HeapTupleData) -> (xl_heap_header, Vec<u8>) {
    // SAFETY: live tuple header; copy out the reduced WAL header + the body bytes.
    unsafe {
        let d = &*newtup.t_data();
        let xlhdr = xl_heap_header {
            t_infomask2: d.t_infomask2,
            t_infomask: d.t_infomask,
            t_hoff: d.t_hoff,
        };
        let total = newtup.t_len as usize;
        let src = newtup.t_data().cast::<u8>().add(SizeofHeapTupleHeader);
        let body = core::slice::from_raw_parts(src, total - SizeofHeapTupleHeader).to_vec();
        (xlhdr, body)
    }
}

/// Emit the `XLOG_HEAP_UPDATE` record. Mirrors `emit_insert_wal`: registers the
/// old page (block 0) and the new page (block 1, with the new-tuple payload),
/// after the content locks are dropped.
#[allow(clippy::too_many_arguments, reason = "mirrors the C log_heap_update locals")]
async fn emit_update_wal(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    buffer: Buffer,
    block: BlockNumber,
    newbuf: Buffer,
    newblock: BlockNumber,
    old_offnum: OffsetNumber,
    old_xmax: TransactionId,
    old_infobits_set: u8,
    new_offnum: OffsetNumber,
    xlhdr: &xl_heap_header,
    body: &[u8],
) -> crate::access::xlogdefs::XLogRecPtr {
    let locator = relation.rd_locator;
    let xlrec = xl_heap_update {
        old_xmax,
        old_offnum,
        old_infobits_set,
        flags: XLH_UPDATE_CONTAINS_NEW_TUPLE,
        new_xmax: TransactionId(0),
        new_offnum,
    };

    begin_insert();
    register_data(as_bytes(&xlrec, SizeOfHeapUpdate));

    let pool = shared.buffers();
    // Block 1: the new page image.
    {
        let _g = pool.content_share(newbuf);
        let page = pool.buffer_get_page(newbuf);
        register_block(1, &locator, ForkNumber::MAIN_FORKNUM, newblock, page, RegBuf::STANDARD);
    }
    // Block 0: the old page image.
    {
        let _g = pool.content_share(buffer);
        let page = pool.buffer_get_page(buffer);
        register_block(0, &locator, ForkNumber::MAIN_FORKNUM, block, page, RegBuf::STANDARD);
    }
    // New-tuple header + body go with block 1.
    register_buf_data(1, as_bytes(xlhdr, SizeOfHeapHeader));
    register_buf_data(1, body);

    set_record_flags(XLogRecordFlags::INCLUDE_ORIGIN);
    xlog_insert(shared.xlog(), RmgrId::Heap as u8, XLOG_HEAP_UPDATE).await
}

/// `heap_lock_tuple`: acquire a row lock (`SELECT FOR UPDATE/SHARE/...`) on the
/// tuple at `tuple.t_self`. Runs `HeapTupleSatisfiesUpdate`; on `TM_Ok` it sets
/// the xmax lock bits per `mode` (clearing `HEAP_XMAX_INVALID`), self-points
/// `t_ctid` for a lock-only xmax, marks the buffer dirty, emits an
/// `XLOG_HEAP_LOCK` record (mirroring `heap_insert`'s WAL), and sets the page LSN.
/// Returns `(TM_Result, Buffer)` -- the C `*buffer` out-param (the buffer is left
/// pinned, as in PG, for the caller to fetch the locked tuple).
///
/// Staged (rules.md s4): the `TM_BeingModified/Updated/Deleted` wait + update-chain
/// follow path, multixact lockers, `LockWaitSkip`/`LockWaitError` policies, and VM
/// all-frozen bookkeeping. The common single-locker `FOR UPDATE` case is complete.
#[allow(clippy::too_many_arguments, reason = "mirrors the C heap_lock_tuple signature")]
pub async fn heap_lock_tuple(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    tuple: &mut HeapTupleData,
    cid: CommandId,
    mode: LockTupleMode,
    _wait_policy: LockWaitPolicy,
    _follow_updates: bool,
) -> (TM_Result, TM_FailureData, Buffer) {
    let tid = tuple.t_self;
    let block = tid.block_number();
    let offnum = tid.offset_number();
    let buffer = read_relation_block(shared, relation, block).await;
    let pool = shared.buffers();
    let buf_id = buf_id_of(buffer);

    let (hdr_copy, lp_off, t_len) = {
        let _g = pool.content_exclusive(buffer);
        // SAFETY: exclusive content lock -> sole reader/writer.
        let page = unsafe { pool.block_mut(buf_id) };
        let item_id = page.get_item_id(offnum);
        crate::assert!(item_id.is_normal(), "heap_lock_tuple: target line pointer not normal");
        let off = item_id.lp_off() as usize;
        // SAFETY: normal heap item; lock held.
        let hdr = unsafe { read_header(page.get_item(&item_id)) };
        (hdr, off, item_id.lp_len())
    };
    tuple.t_len = u32::from(t_len);
    tuple.t_tableOid = relation.relid();

    let result = HeapTupleSatisfiesUpdate(shared, &hdr_copy, &tid, cid).await;

    if result == TM_Result::Invisible {
        // Possible only for ON CONFLICT UPDATE; return so the caller can throw a
        // more specific error (matches C).
        return (TM_Result::Invisible, empty_failure_data(), buffer);
    }

    if result != TM_Result::Ok {
        // The wait / update-chain-follow path is staged; surface failure data.
        let mut tmfd = empty_failure_data();
        tmfd.ctid = hdr_copy.ctid;
        tmfd.xmax = hdr_copy.get_update_xid();
        tmfd.cmax = if result == TM_Result::SelfModified {
            HeapTupleHeaderGetCmax(&hdr_copy)
        } else {
            InvalidCommandId
        };
        return (result, tmfd, buffer);
    }

    let xid = GetCurrentTransactionId(shared).await;
    let (lock_xid, new_infomask, new_infomask2) = compute_new_xmax_infomask(
        hdr_copy.get_raw_xmax(),
        hdr_copy.t_infomask,
        hdr_copy.t_infomask2,
        xid,
        mode,
        false,
    );

    let needs_wal = relation.needs_wal();
    let infobits: u8 = {
        let _g = pool.content_exclusive(buffer);
        // SAFETY: exclusive content lock -> sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        // SAFETY: lp_off addresses the normal header; lock held.
        let data = unsafe { page_tuple_header_mut(page, lp_off) };

        // Cmax is meaningless for a lock, so don't set it.
        data.t_infomask &= !HEAP_XMAX_BITS;
        data.t_infomask2 &= !HEAP_KEYS_UPDATED;
        data.t_infomask |= new_infomask;
        data.t_infomask2 |= new_infomask2;
        if HEAP_XMAX_IS_LOCKED_ONLY(new_infomask) {
            data.clear_hot_updated();
            // No forward chain link for a lock-only xmax.
            data.ctid = tid;
        }
        data.set_xmax(lock_xid);
        let infobits = compute_infobits(new_infomask, data.t_infomask2);
        pool.mark_buffer_dirty(buffer);
        infobits
    };

    if needs_wal {
        let recptr =
            emit_lock_wal(shared, relation, buffer, block, offnum, lock_xid, infobits).await;
        let _g = pool.content_exclusive(buffer);
        // SAFETY: exclusive content lock -> sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        page.set_lsn(recptr);
    }

    (TM_Result::Ok, empty_failure_data(), buffer)
}

/// Emit the `XLOG_HEAP_LOCK` record. Mirrors `emit_insert_wal`.
#[allow(clippy::too_many_arguments, reason = "mirrors the C XLOG block's locals")]
async fn emit_lock_wal(
    shared: &Arc<SharedState>,
    relation: &RelationData,
    buffer: Buffer,
    block: BlockNumber,
    offnum: OffsetNumber,
    xmax: TransactionId,
    infobits_set: u8,
) -> crate::access::xlogdefs::XLogRecPtr {
    let locator = relation.rd_locator;
    let xlrec = xl_heap_lock { xmax, offnum, infobits_set, flags: 0 };

    begin_insert();
    register_data(as_bytes(&xlrec, SizeOfHeapLock));
    {
        let pool = shared.buffers();
        let _g = pool.content_share(buffer);
        let page = pool.buffer_get_page(buffer);
        register_block(0, &locator, ForkNumber::MAIN_FORKNUM, block, page, RegBuf::STANDARD);
    }
    // We don't decode row locks, so no need to log the origin.
    xlog_insert(shared.xlog(), RmgrId::Heap as u8, XLOG_HEAP_LOCK).await
}

/// A zeroed `TM_FailureData` (the success / not-yet-filled case).
fn empty_failure_data() -> TM_FailureData {
    TM_FailureData {
        ctid: zero_item_pointer(),
        xmax: TransactionId(0),
        cmax: InvalidCommandId,
        traversed: false,
    }
}

#[cfg(test)]
mod tests;

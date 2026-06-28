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
//! `Relation`.

use std::sync::Arc;

use crate::access::heapam_xlog::{
    xl_heap_header, xl_heap_insert, SizeOfHeapHeader, SizeOfHeapInsert, XLOG_HEAP_INIT_PAGE,
    XLOG_HEAP_INSERT,
};
use crate::access::htup::{HeapTuple, HeapTupleData};
use crate::access::htup_details::{
    HeapTupleHeaderData, SizeofHeapTupleHeader, HEAP2_XACT_MASK, HEAP_XACT_MASK, HEAP_XMAX_INVALID,
};
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
use crate::catalog::pg_class::{RELKIND_MATVIEW, RELKIND_RELATION};
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::buf::Buffer;
use crate::storage::bufmgr::InvalidBuffer;
use crate::storage::off::{OffsetNumber, FIRST_OFFSET_NUMBER};
use crate::utils::rel::RelationData;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::{SnapshotData, SnapshotType};

pub use crate::access::heapam::{
    HeapScanDescData, HEAP_INSERT_FROZEN, HEAP_INSERT_NO_LOGICAL, HEAP_INSERT_SKIP_FSM,
    HEAP_INSERT_SPECULATIVE,
};
use crate::c::{CommandId, TransactionId};

/// A `*mut T` that is `Send`, so it may be held across an `.await` in a future
/// that runs on the multi-thread tokio runtime.
///
/// The heap entry points thread the relcache `Relation` and the in-memory
/// `HeapTuple` (both raw pointers, hence `!Send`) across buffer/FSM/WAL `.await`
/// leaves. In the single-process port these pointees are NOT shared between
/// tasks for the duration of one operation: the relation is held open by the
/// caller and the tuple lives in this task's local heap. A task may resume on a
/// different OS thread after an `.await`, but no other task accesses the pointee
/// concurrently, so moving the pointer with the task is sound.
///
/// SAFETY (`unsafe impl Send`): the pointee is task-confined for the operation's
/// duration; see above. This mirrors the foundation's `unsafe impl Send` for the
/// shared raw-pointer arenas (e.g. `ProcCell`).
pub struct SendPtr<T>(pub *mut T);

impl<T> Clone for SendPtr<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for SendPtr<T> {}

// SAFETY: see the type doc -- the pointee is task-confined per operation.
unsafe impl<T> Send for SendPtr<T> {}

impl<T> SendPtr<T> {
    /// The wrapped raw pointer.
    #[inline]
    pub fn get(self) -> *mut T {
        self.0
    }
}

/// A `Send` relcache `Relation` handle (for crossing `.await`).
pub type SendRelation = SendPtr<RelationData>;
/// A `Send` in-memory `HeapTuple` handle (for crossing `.await`).
pub type SendTuple = SendPtr<HeapTupleData>;

// SAFETY: the heap scan descriptor holds raw pointers (the relcache Relation, the
// leaked snapshot Arc, the current pinned buffer's tuple) but is task-confined: a
// scan is driven by one task, and its pointees (relation held open by the caller,
// the snapshot owned by the scan, the page kept resident by the scan's pin) are
// not concurrently accessed by other tasks. Marking the descriptor `Send` lets
// `heap_getnext` (which holds `&mut HeapScanDescData` across buffer-read `.await`s)
// run on the multi-thread runtime. Same justification as `SendPtr`.
#[allow(
    clippy::non_send_fields_in_send_ty,
    reason = "the raw-pointer fields are task-confined for the scan's duration; see the SAFETY note"
)]
unsafe impl Send for HeapScanDescData {}

/// `heap_prepare_insert`: fill in the tuple header's transaction fields, returning
/// the tuple to actually store. M2: no toast (the tuple fits inline), so the
/// caller's tuple is returned unchanged after its header is stamped.
///
/// SAFETY: `tup.t_data` is a live, writable heap-tuple header (built by
/// `heap_form_tuple`).
fn heap_prepare_insert(
    relation: &RelationData,
    tup: HeapTuple,
    xid: TransactionId,
    cid: CommandId,
    options: i32,
) -> HeapTuple {
    // SAFETY: live in-memory tuple header (see above).
    let data: &mut HeapTupleHeaderData = unsafe { &mut *(*tup).t_data };

    data.t_infomask &= !HEAP_XACT_MASK;
    data.t_infomask2 &= !HEAP2_XACT_MASK;
    data.t_infomask |= HEAP_XMAX_INVALID;
    data.set_xmin(xid);
    if (options & HEAP_INSERT_FROZEN) != 0 {
        data.set_xmin_frozen();
    }

    data.set_cmin(cid);
    data.set_xmax(TransactionId(0)); // for cleanliness
    // SAFETY: live in-memory tuple.
    unsafe {
        (*tup).t_tableOid = relation.relid();
    }

    // M2: only plain tables / matviews reach here; no out-of-line toasting (the
    // tuple is known to fit). The toast path (heap_toast_insert_or_update) is a
    // grow guard.
    let relkind = unsafe { (*relation.rd_rel).relkind };
    crate::assert!(
        relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW,
        "M2 heap_insert supports only RELKIND_RELATION/MATVIEW"
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
    relation: SendRelation,
    tup: SendTuple,
    cid: CommandId,
    options: i32,
) {
    let xid = GetCurrentTransactionId(shared).await;

    // Read the Send scalars off the relation under a short borrow (a
    // `&RelationData` is `!Send` -- it holds a `*mut SmgrRelation` -- so it must
    // not cross an `.await`). `heap_prepare_insert` stamps the tuple header here.
    let (needs_wal, t_len) = {
        // SAFETY: `relation` is a live, open relation held open by the caller.
        let rel: &RelationData = unsafe { &*relation.get() };
        let heaptup = heap_prepare_insert(rel, tup.get(), xid, cid, options);
        // SAFETY: live in-memory tuple.
        (rel.needs_wal(), unsafe { (*heaptup).t_len } as usize)
    };
    // Find a buffer with room for this tuple (pins it; not locked).
    let buffer = relation_get_buffer_for_tuple(shared, relation, t_len, options).await;
    let block = shared.buffers().buffer_get_block_number(buffer);

    // Place the tuple + (if WAL'd) stage the record, all under the exclusive
    // content lock with NO `.await` in between (the C critical section). The WAL
    // data is staged synchronously (begin/register_*); the awaiting `xlog_insert`
    // runs AFTER the content lock and the `!Send` `heaptup` are dropped below.
    let (info, offnum, xlhdr, tuple_body): (u8, OffsetNumber, Option<xl_heap_header>, Vec<u8>) = {
        // heaptup == tup (no toast copy in M2); scoped here so the `!Send` raw
        // pointer is dropped before the WAL `.await`.
        let mut heaptup: HeapTuple = tup.get();
        let pool = shared.buffers();
        let _g = pool.content_exclusive(buffer);
        let buf_id = buf_id_of(buffer);
        // SAFETY: exclusive content lock held -> sole writer to this slot.
        let page = unsafe { pool.block_mut(buf_id) };

        relation_put_heap_tuple(page, block, &mut heaptup);
        pool.mark_buffer_dirty(buffer);

        // SAFETY: live in-memory tuple; offset patched by relation_put_heap_tuple.
        let offnum = unsafe { (*heaptup).t_self.offset_number() };

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
                let d = &*(*heaptup).t_data;
                let xlhdr = xl_heap_header {
                    t_infomask2: d.t_infomask2,
                    t_infomask: d.t_infomask,
                    t_hoff: d.t_hoff,
                };
                let total = (*heaptup).t_len as usize;
                let src = ((*heaptup).t_data.cast::<u8>()).add(SizeofHeapTupleHeader);
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
    // IS tup, so t_self is already updated in place. heaptup (a `!Send` raw ptr) is
    // intentionally not referenced past the WAL `.await` so the future stays Send.
}

/// Emit the `XLOG_HEAP_INSERT` record. Split out so the content lock is dropped
/// before the (awaiting) `xlog_insert`. The page image is registered (copied)
/// inside `register_block`, so the lock need not be held.
#[allow(clippy::too_many_arguments, reason = "mirrors the C XLOG block's locals")]
async fn emit_insert_wal(
    shared: &Arc<SharedState>,
    relation: SendRelation,
    buffer: Buffer,
    block: BlockNumber,
    info: u8,
    offnum: OffsetNumber,
    xlhdr: &xl_heap_header,
    tuple_body: &[u8],
    _options: i32,
) -> crate::access::xlogdefs::XLogRecPtr {
    // SAFETY: live relation.
    let locator = unsafe { (*relation.get()).rd_locator };

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
/// The `Arc<SnapshotData>` is leaked into the descriptor's `rs_snapshot` raw
/// pointer for the scan's lifetime and reclaimed in `heap_endscan` (mirroring C's
/// snapshot ownership).
pub fn heap_beginscan(
    relation: SendRelation,
    snapshot: Arc<SnapshotData>,
    nkeys: i32,
    flags: ScanOptions,
) -> Box<HeapScanDescData> {
    let relation = relation.get();
    crate::assert!(nkeys == 0, "M2 heap scan supports no scan keys");

    let mut flags = flags;
    // Disable page-at-a-time mode if the snapshot is not MVCC-safe.
    if snapshot.snapshot_type != SnapshotType::Mvcc {
        flags &= !ScanOptions::ALLOW_PAGEMODE;
    }

    // SAFETY: live relation.
    let table_oid = unsafe { (*relation).rd_id };

    let snap_ptr = Arc::into_raw(snapshot).cast_mut();

    let mut t_self = crate::storage::itemptr::ItemPointerData {
        blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
        posid: 0,
    };
    t_self.set_invalid();
    let ctup = HeapTupleData {
        t_len: 0,
        t_self,
        t_tableOid: table_oid,
        t_data: core::ptr::null_mut(),
    };

    let base = TableScanDescData {
        rs_rd: relation,
        rs_snapshot: snap_ptr,
        rs_nkeys: nkeys,
        rs_key: core::ptr::null_mut(),
        st: TableScanType::None,
        rs_flags: flags.bits(),
        rs_parallel: core::ptr::null_mut(),
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
        read_stream: core::ptr::null_mut(),
        dir: ScanDirection::Forward,
        prefetch_block: INVALID_BLOCK_NUMBER,
        parallelworkerdata: core::ptr::null_mut(),
        cindex: 0,
        ntuples: 0,
        vistuples: [0; crate::access::htup_details::MaxHeapTuplesPerPage as usize],
    };
    Box::new(scan)
}

/// `heap_endscan`: release scan resources -- unpin the current buffer and reclaim
/// the leaked snapshot Arc. The boxed descriptor is dropped by the caller (it
/// owns the `Box`).
pub fn heap_endscan(shared: &Arc<SharedState>, scan: &mut HeapScanDescData) {
    if scan.cbuf != InvalidBuffer {
        shared.buffers().release_buffer(scan.cbuf);
        scan.cbuf = InvalidBuffer;
    }
    if !scan.base.rs_snapshot.is_null() {
        // SAFETY: rs_snapshot was produced by Arc::into_raw in heap_beginscan;
        // reclaim the refcount exactly once.
        unsafe {
            drop(Arc::from_raw(scan.base.rs_snapshot.cast_const()));
        }
        scan.base.rs_snapshot = core::ptr::null_mut();
    }
}

/// `heap_rescan`: restart a scan from the beginning (M2: forward seqscan only).
pub fn heap_rescan(shared: &Arc<SharedState>, scan: &mut HeapScanDescData) {
    if scan.cbuf != InvalidBuffer {
        shared.buffers().release_buffer(scan.cbuf);
        scan.cbuf = InvalidBuffer;
    }
    initscan(scan);
}

/// `initscan` (M2 subset): reset the scan cursor. The block count is read lazily
/// on the first `heap_getnext` (it needs an `.await`, which `initscan` avoids).
fn initscan(scan: &mut HeapScanDescData) {
    scan.startblock = 0;
    scan.numblocks = INVALID_BLOCK_NUMBER;
    scan.inited = false;
    scan.ctup.t_data = core::ptr::null_mut();
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
/// `HeapTuple` points into `scan.ctup` (whose `t_data` aliases the pinned page);
/// it is valid until the next `heap_getnext`/`heap_endscan`.
pub async fn heap_getnext(
    shared: &Arc<SharedState>,
    scan: &mut HeapScanDescData,
    direction: ScanDirection,
) -> Option<HeapTuple> {
    crate::assert!(
        scan_direction_is_forward(direction),
        "M2 heap scan supports forward direction only"
    );

    heapgettup_pagemode(shared, scan, direction).await;

    if scan.ctup.t_data.is_null() {
        return None;
    }
    Some(std::ptr::from_mut(&mut scan.ctup))
}

/// `heapgettup_pagemode` (M2 forward subset): walk pages forward, returning the
/// next visible tuple. On each new page it runs `heap_prepare_pagescan` (the
/// per-page visibility collection) and then yields the page's visible tuples in
/// order. The current buffer stays pinned across calls (the page-at-a-time
/// contract); a fresh page is read via `read_buffer_common`.
async fn heapgettup_pagemode(
    shared: &Arc<SharedState>,
    scan: &mut HeapScanDescData,
    dir: ScanDirection,
) {
    let relation = SendPtr(scan.base.rs_rd);

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
            // SAFETY: a normal item's bytes begin with a HeapTupleHeaderData; the
            // page stays pinned, so the pointer is valid until the next call.
            #[allow(
                clippy::cast_ptr_alignment,
                reason = "sound overlay: the page is 8-aligned and PageAddItem MAXALIGNs item offsets, so HeapTupleHeaderData's align (4) divides the address"
            )]
            let t_data = item.as_ptr().cast::<HeapTupleHeaderData>().cast_mut();
            scan.ctup.t_data = t_data;
            scan.ctup.t_len = item.len() as u32;
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
            // SAFETY: live relation.
            let rel: &mut RelationData = unsafe { &mut *relation.get() };
            let smgr_ptr = rel.smgr();
            // SAFETY: relcache-owned smgr handle, valid while the rel is open.
            let smgr = unsafe { &mut *smgr_ptr };
            scan.nblocks = smgr.nblocks(shared, ForkNumber::MAIN_FORKNUM).await;
            scan.cblock = scan.startblock;
            scan.inited = true;
        }

        if scan.cblock >= scan.nblocks {
            // End of scan.
            scan.ctup.t_data = core::ptr::null_mut();
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
    relation: SendRelation,
    block: BlockNumber,
) -> Buffer {
    // Pull the Send scalar + the (Send) smgr handle out under a short borrow so no
    // `&mut RelationData` (it is `!Send`) is held across the `.await`.
    let (relpersistence, smgr_ptr) = {
        // SAFETY: live relation.
        let rel: &mut RelationData = unsafe { &mut *relation.get() };
        (unsafe { (*rel.rd_rel).relpersistence }, SendPtr(rel.smgr()))
    };
    // SAFETY: relcache-owned smgr handle valid while rel is open; Send.
    let smgr = unsafe { &mut *smgr_ptr.get() };
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

/// `heap_prepare_pagescan` (M2): collect the visible-tuple offsets of the current
/// page into `scan.vistuples`. The per-tuple MVCC visibility test runs with the
/// SHARE content lock dropped (it `.await`s clog/subtrans); only the pin is held
/// during the test, which is sound because the page's line pointers and tuple
/// bytes cannot change under a snapshot that can't see concurrent writers (PG's
/// page-at-a-time guarantee).
async fn heap_prepare_pagescan(shared: &Arc<SharedState>, scan: &mut HeapScanDescData) {
    // SAFETY: rs_snapshot is the leaked Arc from beginscan, valid for the scan.
    let snapshot: &SnapshotData = unsafe { &*scan.base.rs_snapshot };

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

/// `heap_update`: grow guard (M8).
pub fn heap_update() {
    unimplemented!("heap_update: M8")
}

/// `heap_delete`: grow guard (M8).
pub fn heap_delete() {
    unimplemented!("heap_delete: M8")
}

/// `heap_lock_tuple`: grow guard (M8).
pub fn heap_lock_tuple() {
    unimplemented!("heap_lock_tuple: M8")
}

#[cfg(test)]
mod tests;

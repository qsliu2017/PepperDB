//! The btree access method entry points + the AM handler. Translated from the
//! M2-reachable parts of `src/backend/access/nbtree/nbtree.c`.
//!
//! M2 scope (step 13-rest): `btbuild` (scan the heap, sort the index tuples, pack
//! them bottom-up via nbtsort's [`bt_load`]) and `btbuildempty` (write a meta page
//! for an empty index). The scan/insert AM callbacks are exposed as the async
//! free functions in nbtsearch/nbtinsert/indexam (the C `IndexAmRoutine` vtable +
//! the planner-facing sync `btgettuple`/`btbeginscan` API is the executor's M-path
//! and stays as the header stubs).
//!
//! Async coloring (rules.md s5): `btbuild` scans the heap (buffer pool) and writes
//! index pages (buffer pool), so it is `async`.


use std::sync::Arc;

use crate::access::genam::IndexBuildResult;
use crate::access::sdir::ScanDirection;
use crate::access::tableam::ScanOptions;
use crate::backend::access::common::heaptuple::heap_getattr;
use crate::backend::access::heap::heapam::{
    heap_beginscan, heap_endscan, heap_getnext,
};
use crate::backend::access::nbtree::nbtpage::{bt_allocbuf, bt_initmetapage, bt_relbuf, bt_write_page};
use crate::backend::access::nbtree::nbtsort::{bt_load, BuildTuple};
use crate::backend::utils::time::snapmgr::GetActiveSnapshot;
use crate::nodes::execnodes::IndexInfo;
use crate::postgres::Datum;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::Page;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::rel::RelationData;

/// `btbuild`: build a btree index over `heap` for `index`. Scans the heap with the
/// active snapshot, extracts each live tuple's index key columns + heap TID, sorts
/// by the key columns, and packs the sorted tuples bottom-up into the index.
///
/// M2 form: the index key columns are a direct projection of heap columns
/// (`index_info.index_attr_numbers`); index expressions and partial-index
/// predicates are later scope. Returns the heap/index tuple counts.
pub async fn btbuild(
    shared: &Arc<SharedState>,
    heap: &RelationData,
    index: &RelationData,
    index_info: &IndexInfo,
) -> IndexBuildResult {
    let keycols: Vec<i32> = index_info
        .index_attr_numbers
        .iter()
        .map(|&a| i32::from(a))
        .collect();

    // For the M2 builtin opclasses all columns are equalimage.
    let allequalimage = true;

    let heap_td = heap.rd_att.clone()
        .unwrap_or_else(|| unreachable!("heap relation has a descriptor"));

    let snapshot = GetActiveSnapshot();
    let scan_snapshot = snapshot
        .clone()
        .unwrap_or_else(|| unreachable!("btbuild requires an active snapshot"));
    let mut hscan = heap_beginscan(heap, &scan_snapshot, 0, ScanOptions::ALLOW_PAGEMODE);

    let mut tuples: Vec<BuildTuple> = Vec::new();
    let mut heap_count: f64 = 0.0;

    while let Some(tup) = heap_getnext(shared, &mut hscan, ScanDirection::Forward).await {
        // SAFETY: tup points into the pinned scan buffer; valid until next getnext.
        let tref = unsafe { &*tup };
        heap_count += 1.0;

        let mut values = Vec::with_capacity(keycols.len());
        let mut isnull = Vec::with_capacity(keycols.len());
        for &attno in &keycols {
            // SAFETY: attno is a valid heap attribute number.
            let (v, n) = unsafe { heap_getattr(tref, attno, &heap_td) };
            values.push(v);
            isnull.push(n);
        }
        tuples.push(BuildTuple { values, isnull, heap_tid: tref.t_self });
    }

    heap_endscan(shared, &mut hscan);

    sort_build_tuples(index, &mut tuples);

    let index_tuples = bt_load(shared, index, &tuples, allequalimage).await;

    IndexBuildResult { heap_tuples: heap_count, index_tuples }
}

/// Sort the build tuples into index order using each key column's btree comparator
/// (resolved from the index's opclass support procs). Multi-column lexicographic;
/// NULLs ordered per the column's NULLS_FIRST option; DESC inverts.
fn sort_build_tuples(index: &RelationData, tuples: &mut [BuildTuple]) {
    use crate::access::nbtree::{BTORDER_PROC, SK_BT_DESC, SK_BT_INDOPTION_SHIFT, SK_BT_NULLS_FIRST};
    use crate::backend::access::index::indexam::index_getprocinfo;
    use crate::fmgr::FunctionCall2Coll;

    struct Col {
        proc: std::cell::RefCell<crate::fmgr::FmgrInfo>,
        flags: i32,
        collation: crate::postgres_ext::Oid,
    }

    let r: &RelationData = index;
    let nkeys = r.index_number_of_key_attributes() as usize;
    let cols: Vec<Col> = (0..nkeys)
        .map(|i| {
            let proc = std::cell::RefCell::new(index_getprocinfo(index, (i + 1) as i32, BTORDER_PROC));
            let indoption = r.rd_indoption[i];
            let collation = r.rd_indcollation[i];
            Col { proc, flags: i32::from(indoption) << SK_BT_INDOPTION_SHIFT, collation }
        })
        .collect();

    tuples.sort_by(|a, b| {
        for (i, col) in cols.iter().enumerate() {
            let (av, an) = (a.values[i], a.isnull[i]);
            let (bv, bn) = (b.values[i], b.isnull[i]);
            let nulls_first = col.flags & SK_BT_NULLS_FIRST != 0;
            let ord = match (an, bn) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => {
                    if nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    }
                }
                (false, true) => {
                    if nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    }
                }
                (false, false) => {
                    let raw = {
                        let mut f = col.proc.borrow_mut();
                        let d = FunctionCall2Coll(&mut f, col.collation, av, bv)
                            .unwrap_or_else(|| unreachable!("comparator returned NULL"));
                        crate::postgres::DatumGetInt32(d)
                    };
                    let raw = if col.flags & SK_BT_DESC != 0 { -raw } else { raw };
                    raw.cmp(&0)
                }
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// `btinsert`: the AM insert callback. Forms the leaf index tuple from `values`/
/// `isnull` with `heap_tid` as its TID, then inserts it via `bt_doinsert`
/// (descend + place + split). Returns true (M2: no deferred unique conflict).
pub async fn btinsert(
    shared: &Arc<SharedState>,
    index: &RelationData,
    values: &[Datum],
    isnull: &[bool],
    heap_tid: &ItemPointerData,
) -> bool {
    use crate::backend::access::common::indextuple::{index_form_tuple, pfree_index_tuple};
    let itupdesc = index.rd_att.clone()
        .unwrap_or_else(|| unreachable!("index relation has a descriptor"));
    let itup = index_form_tuple(&itupdesc, values, isnull);
    // SAFETY: itup is a freshly formed index tuple.
    unsafe {
        (*itup).tid = *heap_tid;
    }
    let size = unsafe { (*itup).size() };
    let bytes = unsafe { core::slice::from_raw_parts(itup.cast::<u8>(), size) }.to_vec();
    // SAFETY: itup came from index_form_tuple.
    unsafe { pfree_index_tuple(itup) };

    crate::backend::access::nbtree::nbtinsert::bt_doinsert(shared, index, &bytes).await
}

/// `btbulkdelete`: remove index entries pointing at dead heap tuples. Translated
/// from the M13-reachable core of `btbulkdelete` / `btvacuumscan` /
/// `btvacuumpage` in nbtree.c.
///
/// `dead` is the set of dead heap TIDs collected by the heap prune pass (VACUUM's
/// first pass). This does a physical block-order scan of the index (PG's
/// `btvacuumscan`, which walks blocks linearly rather than via the tree so it visits
/// every leaf), and on each leaf page deletes the line pointers whose index tuple
/// references a dead heap TID (`PageIndexMultiDelete`). Returns the count of index
/// tuples removed and the count remaining.
///
/// Async: reads/writes index pages via the buffer pool. Holds the exclusive content
/// lock only for the synchronous page rewrite (no `.await` inside); VACUUM's
/// ShareUpdateExclusive relation lock keeps concurrent writers out.
///
/// Staged (rules.md s4): page deletion / half-dead handling (empty leaf recycling),
/// the pending-FSM pages, `btvacuumcleanup`'s page-count bookkeeping, and the
/// `XLOG_BTREE_VACUUM` WAL record (the page image is rewritten, covered by the
/// buffer's next full-page flush). The M2 btree never splits below one leaf in the
/// tested sizes; the linear-scan delete is the correctness core.
#[allow(clippy::implicit_hasher, reason = "internal caller builds the set with the default hasher")]
pub async fn btbulkdelete(
    shared: &Arc<SharedState>,
    index: &RelationData,
    dead: &std::collections::HashSet<ItemPointerData>,
) -> (u64, u64) {
    use crate::access::nbtree::{BTPageGetOpaque, P_FIRSTDATAKEY, P_IGNORE, P_ISLEAF};
    use crate::backend::access::nbtree::nbtpage::{bt_read_buffer, bt_relbuf, bt_write_page};
    use crate::storage::off::OffsetNumber;

    let nblocks = index_nblocks(shared, index).await;
    let mut num_removed: u64 = 0;
    let mut num_remaining: u64 = 0;

    // Block 0 is the meta page; data pages start at block 1.
    for blkno in 1..nblocks {
        let buffer = bt_read_buffer(shared, index, blkno).await;

        // Read the leaf page's items under a brief share lock (a copy), decide the
        // deletions, then rewrite under the exclusive lock. `.await`s only around
        // the reads/writes, never under a held content lock.
        let (to_delete, remaining): (Vec<OffsetNumber>, u64) = {
            let pool = shared.buffers();
            let _g = pool.content_share(buffer);
            let page = pool.buffer_get_page(buffer);
            // SAFETY: a formatted btree page has a BTPageOpaqueData special area.
            let opaque = unsafe { &*BTPageGetOpaque(page) };
            if !P_ISLEAF(opaque) || P_IGNORE(opaque) {
                (Vec::new(), 0)
            } else {
                let firstoff = P_FIRSTDATAKEY(opaque);
                let maxoff = page.get_max_offset_number();
                let mut del = Vec::new();
                let mut kept: u64 = 0;
                let mut off = firstoff;
                while off <= maxoff {
                    let item_id = page.get_item_id(off);
                    if item_id.is_normal() {
                        let item = page.get_item(&item_id);
                        // SAFETY: a normal btree leaf item begins with IndexTupleData.
                        #[allow(
                            clippy::cast_ptr_alignment,
                            reason = "Page is align(8); items live at MAXALIGN offsets, so the IndexTupleData overlay is aligned (matches nbtsearch.rs)"
                        )]
                        let tid = unsafe {
                            (*item.as_ptr().cast::<crate::access::itup::IndexTupleData>()).tid
                        };
                        if dead.contains(&tid) {
                            del.push(off);
                        } else {
                            kept += 1;
                        }
                    }
                    off += 1;
                }
                (del, kept)
            }
        };

        num_remaining += remaining;

        if to_delete.is_empty() {
            bt_relbuf(shared, buffer);
            continue;
        }

        // Rewrite the page with the dead items removed.
        let mut page_copy = {
            let pool = shared.buffers();
            let _g = pool.content_share(buffer);
            let src = pool.buffer_get_page(buffer);
            let mut copy = crate::storage::bufpage::Page::boxed_zeroed();
            copy.as_mut_bytes().copy_from_slice(src.as_bytes());
            copy
        };
        page_copy.index_multi_delete(&to_delete);
        num_removed += to_delete.len() as u64;
        bt_write_page(shared, buffer, &page_copy, crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
        bt_relbuf(shared, buffer);
    }

    (num_removed, num_remaining)
}

/// The block count of an index's main fork.
async fn index_nblocks(shared: &Arc<SharedState>, index: &RelationData) -> BlockNumber {
    use crate::common::relpath::ForkNumber;
    let smgr_ptr = index.smgr();
    // SAFETY: relcache-owned smgr handle, valid while the index is open.
    let smgr = unsafe { &mut *smgr_ptr };
    smgr.nblocks(shared, ForkNumber::MAIN_FORKNUM).await
}

/// `btbuildempty`: write an empty btree (a meta page with no root). Used for
/// unlogged indexes; M2 writes it to the main fork via the buffer pool.
pub async fn btbuildempty(shared: &Arc<SharedState>, index: &RelationData) {
    let buf = bt_allocbuf(shared, index).await;
    let mut metapage = Page::boxed_zeroed();
    bt_initmetapage(&mut metapage, crate::access::nbtree::P_NONE, 0, true);
    bt_write_page(shared, buf, &metapage, crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
    bt_relbuf(shared, buf);
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

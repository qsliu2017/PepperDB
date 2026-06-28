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

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: holds per-backend raw Relation handles task-confined for the operation; futures never migrate the pointee between tasks"
)]
#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "btree AM entry points take raw Relation pointers per the C API; faithful to C"
)]

use std::sync::Arc;

use crate::access::genam::IndexBuildResult;
use crate::access::sdir::ScanDirection;
use crate::access::tableam::ScanOptions;
use crate::backend::access::common::heaptuple::heap_getattr;
use crate::backend::access::heap::heapam::{
    heap_beginscan, heap_endscan, heap_getnext, SendPtr,
};
use crate::backend::access::nbtree::nbtpage::{bt_allocbuf, bt_initmetapage, bt_relbuf, bt_write_page};
use crate::backend::access::nbtree::nbtsort::{bt_load, BuildTuple};
use crate::backend::utils::time::snapmgr::GetActiveSnapshot;
use crate::nodes::execnodes::IndexInfo;
use crate::postgres::Datum;
use crate::shared_state::SharedState;
use crate::storage::bufpage::Page;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::rel::RelationData;
use crate::utils::relcache::Relation;

/// `btbuild`: build a btree index over `heap` for `index`. Scans the heap with the
/// active snapshot, extracts each live tuple's index key columns + heap TID, sorts
/// by the key columns, and packs the sorted tuples bottom-up into the index.
///
/// M2 form: the index key columns are a direct projection of heap columns
/// (`index_info.index_attr_numbers`); index expressions and partial-index
/// predicates are later scope. Returns the heap/index tuple counts.
pub async fn btbuild(
    shared: &Arc<SharedState>,
    heap: Relation,
    index: Relation,
    index_info: &IndexInfo,
) -> IndexBuildResult {
    let keycols: Vec<i32> = index_info
        .index_attr_numbers
        .iter()
        .map(|&a| i32::from(a))
        .collect();

    // For the M2 builtin opclasses all columns are equalimage.
    let allequalimage = true;

    // SAFETY: live heap relation with a descriptor.
    let heap_td = unsafe { (*heap).rd_att.clone() }
        .unwrap_or_else(|| unreachable!("heap relation has a descriptor"));

    let snapshot = GetActiveSnapshot();
    let scan_snapshot = snapshot
        .clone()
        .unwrap_or_else(|| unreachable!("btbuild requires an active snapshot"));
    let mut hscan = heap_beginscan(SendPtr(heap), scan_snapshot, 0, ScanOptions::ALLOW_PAGEMODE);

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
fn sort_build_tuples(index: Relation, tuples: &mut [BuildTuple]) {
    use crate::access::nbtree::{BTORDER_PROC, SK_BT_DESC, SK_BT_INDOPTION_SHIFT, SK_BT_NULLS_FIRST};
    use crate::backend::access::index::indexam::index_getprocinfo;
    use crate::fmgr::FunctionCall2Coll;

    struct Col {
        proc: *mut crate::fmgr::FmgrInfo,
        flags: i32,
        collation: crate::postgres_ext::Oid,
    }

    // SAFETY: live index relation with access info initialized.
    let r: &RelationData = unsafe { &*index };
    let nkeys = r.index_number_of_key_attributes() as usize;
    let cols: Vec<Col> = (0..nkeys)
        .map(|i| {
            let proc = index_getprocinfo(index, (i + 1) as i32, BTORDER_PROC);
            // SAFETY: rd_indoption / rd_indcollation sized >= nkeys.
            let indoption = unsafe { *r.rd_indoption.add(i) };
            let collation = unsafe { *r.rd_indcollation.add(i) };
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
                    // SAFETY: proc is a live FmgrInfo (resolved above).
                    let raw = {
                        let f = unsafe { &mut *col.proc };
                        let d = FunctionCall2Coll(f, col.collation, av, bv)
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
    index: Relation,
    values: &[Datum],
    isnull: &[bool],
    heap_tid: &ItemPointerData,
) -> bool {
    use crate::backend::access::common::indextuple::{index_form_tuple, pfree_index_tuple};
    // SAFETY: live index relation with a descriptor.
    let itupdesc = unsafe { (*index).rd_att.clone() }
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

/// `btbuildempty`: write an empty btree (a meta page with no root). Used for
/// unlogged indexes; M2 writes it to the main fork via the buffer pool.
pub async fn btbuildempty(shared: &Arc<SharedState>, index: Relation) {
    let buf = bt_allocbuf(shared, SendPtr(index)).await;
    let mut metapage = Page::boxed_zeroed();
    bt_initmetapage(&mut metapage, crate::access::nbtree::P_NONE, 0, true);
    bt_write_page(shared, buf, &metapage, crate::access::xlogdefs::INVALID_XLOG_REC_PTR);
    bt_relbuf(shared, buf);
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

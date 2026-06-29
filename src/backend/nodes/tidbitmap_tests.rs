//! Unit tests for the owned TIDBitmap: add TIDs across pages and iterate in
//! ascending block order, union/intersect, and lossy-page promotion.

use super::*;

fn tid(block: BlockNumber, off: OffsetNumber) -> ItemPointerData {
    let mut ip = ItemPointerData {
        blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
        posid: 0,
    };
    ip.set(block, off);
    ip
}

/// Collect a bitmap's iteration as (blockno, lossy, offsets) in emission order.
fn drain(tbm: &mut TIDBitmap) -> Vec<(BlockNumber, bool, Vec<OffsetNumber>)> {
    let mut it = tbm_begin_iterate(tbm);
    let mut out = Vec::new();
    while let Some(r) = tbm_iterate(&mut it) {
        out.push((r.blockno, r.lossy, r.offsets));
    }
    tbm_end_iterate(&mut it);
    out
}

#[test]
fn add_and_iterate_in_block_order() {
    let mut tbm = tbm_create(1024 * 1024);
    // Insert out of order across pages; offsets within a page also out of order.
    tbm_add_tuples(&mut tbm, &[tid(5, 3), tid(2, 7), tid(5, 1), tid(2, 2)], false);
    let rows = drain(&mut tbm);
    assert_eq!(
        rows,
        vec![
            (2, false, vec![2, 7]),
            (5, false, vec![1, 3]),
        ]
    );
}

#[test]
fn empty_bitmap_iterates_to_nothing() {
    let mut tbm = tbm_create(1024 * 1024);
    assert!(tbm_is_empty(&tbm));
    assert!(drain(&mut tbm).is_empty());
}

#[test]
fn union_merges_pages_and_offsets() {
    let mut a = tbm_create(1024 * 1024);
    let mut b = tbm_create(1024 * 1024);
    tbm_add_tuples(&mut a, &[tid(1, 1), tid(3, 5)], false);
    tbm_add_tuples(&mut b, &[tid(1, 4), tid(2, 9)], false);
    tbm_union(&mut a, &b);
    let rows = drain(&mut a);
    assert_eq!(
        rows,
        vec![
            (1, false, vec![1, 4]),
            (2, false, vec![9]),
            (3, false, vec![5]),
        ]
    );
}

#[test]
fn intersect_keeps_common_tuples() {
    let mut a = tbm_create(1024 * 1024);
    let mut b = tbm_create(1024 * 1024);
    // a: page1{1,2,3}, page2{4}, page3{1}
    tbm_add_tuples(&mut a, &[tid(1, 1), tid(1, 2), tid(1, 3), tid(2, 4), tid(3, 1)], false);
    // b: page1{2,3}, page3{2}  -> intersection: page1{2,3}; page2 gone; page3 empty
    tbm_add_tuples(&mut b, &[tid(1, 2), tid(1, 3), tid(3, 2)], false);
    tbm_intersect(&mut a, &b);
    let rows = drain(&mut a);
    assert_eq!(rows, vec![(1, false, vec![2, 3])]);
}

#[test]
fn intersect_with_empty_clears() {
    // Intersecting against an empty b empties a: tbm_intersect short-circuits only
    // when *a* is empty; otherwise every page in a finds no match in b and is
    // dropped (matching PG's tbm_intersect, which deletes the unmatched pages).
    let mut a = tbm_create(1024 * 1024);
    let b = tbm_create(1024 * 1024);
    tbm_add_tuples(&mut a, &[tid(1, 1)], false);
    tbm_intersect(&mut a, &b);
    assert!(tbm_is_empty(&a));
    // Disjoint pages also intersect to empty.
    let mut a2 = tbm_create(1024 * 1024);
    let mut b2 = tbm_create(1024 * 1024);
    tbm_add_tuples(&mut a2, &[tid(1, 1)], false);
    tbm_add_tuples(&mut b2, &[tid(9, 1)], false);
    tbm_intersect(&mut a2, &b2);
    assert!(tbm_is_empty(&a2));
}

#[test]
fn lossy_promotion_under_tight_budget() {
    // A tiny budget forces lossify: many distinct pages exceed maxentries and get
    // folded into lossy chunks. The iteration still yields every touched page, now
    // flagged lossy/recheck.
    let mut tbm = tbm_create(16); // floored to 16 entries by tbm_calculate_entries
    let npages: BlockNumber = 100;
    for p in 1..=npages {
        tbm_add_tuples(&mut tbm, &[tid(p, 1)], false);
    }
    let rows = drain(&mut tbm);
    // Every page is still represented exactly once, in ascending order.
    let blocks: Vec<BlockNumber> = rows.iter().map(|r| r.0).collect();
    let expected: Vec<BlockNumber> = (1..=npages).collect();
    assert_eq!(blocks, expected);
    // At least some pages were promoted to lossy (recheck) under the tight budget.
    assert!(rows.iter().any(|r| r.1), "expected some lossy pages");
    for r in &rows {
        if r.1 {
            assert!(r.2.is_empty(), "lossy page carries no exact offsets");
        }
    }
}

#[test]
fn add_page_is_lossy() {
    let mut tbm = tbm_create(1024 * 1024);
    tbm_add_page(&mut tbm, 7);
    let rows = drain(&mut tbm);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 7);
    assert!(rows[0].1, "tbm_add_page marks the page lossy");
}

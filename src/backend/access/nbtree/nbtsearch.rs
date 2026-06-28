//! Search code for postgres btrees. Translated from
//! src/backend/access/nbtree/nbtsearch.c.
//!
//! The full `_bt_search`/`_bt_first`/`_bt_next` tree descent operates on a live
//! `Relation` relcache entry (for the opclass support procs, indoption, and the
//! buffer-by-relation read path) plus pinned/locked buffers. The relcache (step
//! 12) and the heap AM are not yet translated, so the `Relation`-typed entry
//! points remain the header `unimplemented!()` stubs for now (rules.md s4).
//!
//! What IS real and tested here is the algorithmic core those entry points are
//! built from, expressed at the page level so it can run over a real [`Page`] of
//! index tuples with an explicit tuple descriptor and per-column comparator:
//!
//!   * [`bt_compare_page_item`] -- the `_bt_compare` per-attribute comparison
//!     loop (NULL ordering + DESC handling + multi-column lexicographic order),
//!   * [`bt_binsrch_leaf`] -- the `_bt_binsrch` binary search that finds the
//!     first leaf offset whose key is >= the search key (generic over the number
//!     of scankey columns),
//!   * forward iteration over a sorted leaf page ([`bt_leaf_scan`]).
//!
//! A column comparator has the btree support-proc contract: it returns `< 0`,
//! `0`, `> 0` for `index_value <=> search_argument`. The relcache-backed wrapper
//! supplies one per key column from the opclass (the `bt*cmp` functions in
//! nbtcompare.rs / the adt files) via fmgr.

use crate::access::nbtree::{SK_BT_DESC, SK_BT_NULLS_FIRST};
use crate::access::tupdesc::TupleDescData;
use crate::backend::access::common::indextuple::index_getattr;
use crate::access::itup::IndexTuple;
use crate::postgres::Datum;
use crate::storage::bufpage::Page;
use crate::storage::itemid::LP_NORMAL;
use crate::storage::off::OffsetNumber;

/// Reinterpret a page item (the bytes a line pointer covers) as an index tuple.
///
/// `index_getattr` only reads through the handle, but its type is the C
/// `IndexTuple` (`*mut`), so the `*const u8` from `get_item` is cast to `*mut`.
/// Page is `#[repr(C, align(8))]` and items are stored at MAXALIGN'd offsets, so
/// the `IndexTupleData` overlay (2-byte alignment) is well-aligned.
#[allow(
    clippy::cast_ptr_alignment,
    reason = "Page is align(8); page items live at MAXALIGN offsets, so the IndexTupleData \
              overlay is well-aligned (matches itup.rs / bufpage.rs)"
)]
fn item_as_index_tuple(item: &[u8]) -> IndexTuple {
    item.as_ptr().cast::<crate::access::itup::IndexTupleData>().cast_mut()
}

/// One preprocessed comparison column, mirroring the parts of a `ScanKeyData`
/// that `_bt_compare` consults: which index attribute (1-based), the search
/// argument, whether the argument is NULL, the comparator, and the DESC /
/// NULLS_FIRST flags (the `SK_BT_*` bits remapped from `pg_index.indoption`).
pub struct BtCompareColumn<'a> {
    pub attno: i32,
    pub argument: Datum,
    pub argnull: bool,
    /// `index_value <=> search_argument` -> `<0 / 0 / >0`.
    pub cmp: &'a dyn Fn(Datum, Datum) -> i32,
    /// `SK_BT_DESC | SK_BT_NULLS_FIRST` flag bits for this column.
    pub flags: i32,
}

/// PG `_bt_compare` (page-item half): compare an insertion/search key (a slice of
/// [`BtCompareColumn`], the first k key attributes in order) against the index
/// tuple at line pointer `offnum` on `page`.
///
/// Returns `< 0` if the search key sorts before the item, `0` if equal across all
/// provided columns, `> 0` if after. This is the sign convention callers expect
/// ("scankey vs index item"): the raw comparator computes `index_value <=>
/// argument`, which is sign-flipped here (unless the column is DESC), exactly as
/// nbtsearch.c does.
///
/// SAFETY: `page` must hold a valid index tuple at `offnum`.
pub fn bt_compare_page_item(
    key: &[BtCompareColumn<'_>],
    itupdesc: &TupleDescData,
    page: &Page,
    offnum: OffsetNumber,
) -> i32 {
    let item_id = page.get_item_id(offnum);
    debug_assert_eq!(item_id.lp_flags(), LP_NORMAL);
    let item = page.get_item(&item_id);
    let itup = item_as_index_tuple(item);

    for col in key {
        // SAFETY: `itup` points at a valid index-tuple block (a live line pointer
        // on the page); `attno` is a 1-based key attribute.
        let (datum, isnull) = unsafe { index_getattr(itup, col.attno, itupdesc) };

        let result = if col.argnull {
            if isnull {
                0 // NULL "=" NULL
            } else if col.flags & SK_BT_NULLS_FIRST != 0 {
                -1 // NULL "<" NOT_NULL
            } else {
                1 // NULL ">" NOT_NULL
            }
        } else if isnull {
            if col.flags & SK_BT_NULLS_FIRST != 0 {
                1 // NOT_NULL ">" NULL
            } else {
                -1 // NOT_NULL "<" NULL
            }
        } else {
            // comparator computes index_value <=> argument; flip to express
            // "search key vs index item" unless the column is DESC.
            let mut r = (col.cmp)(datum, col.argument);
            if col.flags & SK_BT_DESC == 0 {
                r = -r;
            }
            r
        };

        if result != 0 {
            return result;
        }
    }

    0
}

/// PG `_bt_binsrch` (leaf half): binary search a leaf page for the first
/// non-pivot item whose key is `>=` the search key. Returns an offset in
/// `[firstkey, maxoff + 1]`; `maxoff + 1` means "all items sort before the key".
///
/// `firstkey` is the first data offset (`P_FIRSTDATAKEY`), `maxoff` the page's
/// last offset. Generic over the number of scankey columns via
/// [`bt_compare_page_item`].
pub fn bt_binsrch_leaf(
    key: &[BtCompareColumn<'_>],
    itupdesc: &TupleDescData,
    page: &Page,
    firstkey: OffsetNumber,
    maxoff: OffsetNumber,
) -> OffsetNumber {
    // Invariant: everything strictly left of `low` is < key; everything at or
    // right of `high` is >= key. (nbtsearch.c keeps `low`/`high` this way.)
    let mut low = firstkey;
    let mut high = maxoff;

    if high < low {
        return low; // empty data range
    }
    // Make high one past the last item so the loop's [low, high) is half-open.
    high += 1;

    while low < high {
        let mid = low + (high - low) / 2;
        let result = bt_compare_page_item(key, itupdesc, page, mid);
        if result > 0 {
            // search key sorts after item[mid]: item[mid] < key -> go right.
            low = mid + 1;
        } else {
            // item[mid] >= key.
            high = mid;
        }
    }

    low
}

/// One result of scanning a leaf page: the matched item's offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BtLeafMatch {
    pub offnum: OffsetNumber,
}

/// Forward-scan a sorted leaf page returning every item offset in index order
/// (the in-page half of `_bt_readpage` for a full scan; key filtering is the
/// caller's `_bt_checkkeys`). Offsets run `[firstkey, maxoff]`.
pub fn bt_leaf_scan(firstkey: OffsetNumber, maxoff: OffsetNumber) -> Vec<BtLeafMatch> {
    if maxoff < firstkey {
        return Vec::new();
    }
    (firstkey..=maxoff).map(|offnum| BtLeafMatch { offnum }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::nbtree::nbtpage::bt_init_root_leaf;
    use crate::backend::access::common::indextuple::index_form_tuple;
    use crate::catalog::pg_attribute::FormData_pg_attribute;
    use crate::catalog::pg_type::{TYPALIGN_INT, TYPSTORAGE_PLAIN};
    use crate::postgres::{DatumGetInt32, Int32GetDatum};
    use crate::storage::bufpage::Page;

    /// Build a single-column int4 index tuple descriptor (no syscache needed).
    fn int4_desc(ncols: usize) -> TupleDescData {
        let mut desc = TupleDescData::create_template(ncols as i32);
        for i in 0..ncols {
            let att: &mut FormData_pg_attribute = &mut desc.attrs[i];
            att.attnum = (i + 1) as i16;
            att.atttypid = crate::postgres_ext::Oid(23); // INT4OID
            att.attlen = 4;
            att.attbyval = true;
            att.attalign = TYPALIGN_INT;
            att.attstorage = TYPSTORAGE_PLAIN;
            att.attcollation = crate::postgres_ext::InvalidOid;
            desc.populate_compact_attribute(i);
        }
        desc
    }

    fn int4cmp(a: Datum, b: Datum) -> i32 {
        DatumGetInt32(a).cmp(&DatumGetInt32(b)) as i32
    }

    /// Form an int4 index tuple and append it to a leaf page. Returns nothing;
    /// callers insert keys already in sorted order to build a sorted leaf.
    fn append_int4(page: &mut Page, desc: &TupleDescData, v: i32) {
        let itup = index_form_tuple(desc, &[Int32GetDatum(v)], &[false]);
        // SAFETY: index_form_tuple returns a valid block sized by its t_info.
        let size = unsafe { (*itup).size() };
        let bytes = unsafe { core::slice::from_raw_parts(itup.cast::<u8>(), size) };
        let off = page.add_item(bytes, size, 0 /* InvalidOffsetNumber */, false, false);
        assert_ne!(off, 0, "page add_item failed");
        // SAFETY: itup came from index_form_tuple in this test.
        unsafe { crate::backend::access::common::indextuple::pfree_index_tuple(itup) };
    }

    fn col(v: i32, cmp: &dyn Fn(Datum, Datum) -> i32) -> BtCompareColumn<'_> {
        BtCompareColumn { attno: 1, argument: Int32GetDatum(v), argnull: false, cmp, flags: 0 }
    }

    #[test]
    fn compare_single_column() {
        let desc = int4_desc(1);
        let mut page = Page::boxed_zeroed();
        bt_init_root_leaf(&mut page);
        append_int4(&mut page, &desc, 10);
        append_int4(&mut page, &desc, 20);
        append_int4(&mut page, &desc, 30);

        let cmp = int4cmp;
        // search key 20 vs item at offset 2 (value 20) -> equal.
        assert_eq!(bt_compare_page_item(&[col(20, &cmp)], &desc, &page, 2), 0);
        // search key 25 vs item at offset 2 (value 20) -> key after item (>0).
        assert!(bt_compare_page_item(&[col(25, &cmp)], &desc, &page, 2) > 0);
        // search key 5 vs item at offset 1 (value 10) -> key before item (<0).
        assert!(bt_compare_page_item(&[col(5, &cmp)], &desc, &page, 1) < 0);
    }

    #[test]
    fn binsrch_finds_first_ge() {
        let desc = int4_desc(1);
        let mut page = Page::boxed_zeroed();
        bt_init_root_leaf(&mut page);
        for v in [10, 20, 30, 40, 50] {
            append_int4(&mut page, &desc, v);
        }
        let maxoff = page.get_max_offset_number();
        assert_eq!(maxoff, 5);
        let cmp = int4cmp;

        // present key 30 -> offset 3.
        assert_eq!(bt_binsrch_leaf(&[col(30, &cmp)], &desc, &page, 1, maxoff), 3);
        // absent key 25 -> first item >= 25 is 30 at offset 3.
        assert_eq!(bt_binsrch_leaf(&[col(25, &cmp)], &desc, &page, 1, maxoff), 3);
        // below all -> offset 1.
        assert_eq!(bt_binsrch_leaf(&[col(1, &cmp)], &desc, &page, 1, maxoff), 1);
        // above all -> maxoff + 1.
        assert_eq!(bt_binsrch_leaf(&[col(99, &cmp)], &desc, &page, 1, maxoff), maxoff + 1);
        // exact first / last.
        assert_eq!(bt_binsrch_leaf(&[col(10, &cmp)], &desc, &page, 1, maxoff), 1);
        assert_eq!(bt_binsrch_leaf(&[col(50, &cmp)], &desc, &page, 1, maxoff), 5);
    }

    #[test]
    fn multi_column_lexicographic() {
        let desc = int4_desc(2);
        let mut page = Page::boxed_zeroed();
        bt_init_root_leaf(&mut page);
        // sorted by (a, b): (1,5) (1,9) (2,1)
        for (a, b) in [(1, 5), (1, 9), (2, 1)] {
            let itup = index_form_tuple(&desc, &[Int32GetDatum(a), Int32GetDatum(b)], &[false, false]);
            let size = unsafe { (*itup).size() };
            let bytes = unsafe { core::slice::from_raw_parts(itup.cast::<u8>(), size) };
            page.add_item(bytes, size, 0, false, false);
            unsafe { crate::backend::access::common::indextuple::pfree_index_tuple(itup) };
        }
        let maxoff = page.get_max_offset_number();
        let cmp = int4cmp;

        let key = |a: i32, b: i32| {
            vec![
                BtCompareColumn { attno: 1, argument: Int32GetDatum(a), argnull: false, cmp: &cmp, flags: 0 },
                BtCompareColumn { attno: 2, argument: Int32GetDatum(b), argnull: false, cmp: &cmp, flags: 0 },
            ]
        };

        // (1,9) exact -> offset 2.
        assert_eq!(bt_binsrch_leaf(&key(1, 9), &desc, &page, 1, maxoff), 2);
        // (1,7) -> first >= is (1,9) at offset 2.
        assert_eq!(bt_binsrch_leaf(&key(1, 7), &desc, &page, 1, maxoff), 2);
        // (1,1) -> first >= is (1,5) at offset 1.
        assert_eq!(bt_binsrch_leaf(&key(1, 1), &desc, &page, 1, maxoff), 1);
        // (2,0) -> first >= is (2,1) at offset 3.
        assert_eq!(bt_binsrch_leaf(&key(2, 0), &desc, &page, 1, maxoff), 3);
    }

    #[test]
    fn desc_column_inverts_order() {
        // A DESC column: comparator sign is NOT flipped, so a physically
        // descending page (50,40,..,10) is "sorted" for the scankey.
        let desc = int4_desc(1);
        let mut page = Page::boxed_zeroed();
        bt_init_root_leaf(&mut page);
        for v in [50, 40, 30, 20, 10] {
            append_int4(&mut page, &desc, v);
        }
        let maxoff = page.get_max_offset_number();
        let cmp = int4cmp;
        let dcol = |v: i32| {
            vec![BtCompareColumn { attno: 1, argument: Int32GetDatum(v), argnull: false, cmp: &cmp, flags: SK_BT_DESC }]
        };
        // In DESC order, 30 is at offset 3.
        assert_eq!(bt_binsrch_leaf(&dcol(30), &desc, &page, 1, maxoff), 3);
        // 35 falls between 40 (off 2) and 30 (off 3); first item that is
        // "<= 35 in value" i.e. >= in DESC order is 30 at offset 3.
        assert_eq!(bt_binsrch_leaf(&dcol(35), &desc, &page, 1, maxoff), 3);
    }

    #[test]
    fn leaf_scan_returns_all_in_order() {
        let desc = int4_desc(1);
        let mut page = Page::boxed_zeroed();
        bt_init_root_leaf(&mut page);
        for v in [10, 20, 30] {
            append_int4(&mut page, &desc, v);
        }
        let maxoff = page.get_max_offset_number();
        let matches = bt_leaf_scan(1, maxoff);
        let cmp = int4cmp;
        let _ = &cmp;
        let vals: Vec<i32> = matches
            .iter()
            .map(|m| {
                let id = page.get_item_id(m.offnum);
                let item = page.get_item(&id);
                let itup = super::item_as_index_tuple(item);
                let (d, _n) = unsafe { index_getattr(itup, 1, &desc) };
                DatumGetInt32(d)
            })
            .collect();
        assert_eq!(vals, vec![10, 20, 30]);
    }
}

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


use std::sync::Arc;

use crate::access::nbtree::{
    BTPageGetOpaque, BTScanInsertData, BTreeTupleGetDownLink, P_FIRSTDATAKEY, P_HIKEY, P_ISLEAF,
    P_RIGHTMOST, SK_BT_DESC, SK_BT_NULLS_FIRST, P_NONE,
};
use crate::access::sdir::{scan_direction_is_forward, ScanDirection};
use crate::access::stratnum::{
    StrategyNumber, BT_EQUAL_STRATEGY_NUMBER, BT_GREATER_EQUAL_STRATEGY_NUMBER,
    BT_GREATER_STRATEGY_NUMBER, BT_LESS_EQUAL_STRATEGY_NUMBER, BT_LESS_STRATEGY_NUMBER,
};
use crate::access::tupdesc::TupleDescData;
use crate::backend::access::common::indextuple::index_getattr;
use crate::backend::access::nbtree::nbtpage::{
    bt_getroot_read, bt_metaversion, bt_read_buffer, bt_read_page_copy, bt_relbuf,
};
use crate::backend::access::nbtree::nbtutils::{
    bt_compare, bt_compare_col_value, bt_mkscankey, bt_scankey_set_search_args,
};
use crate::access::itup::IndexTuple;
use crate::postgres::Datum;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::Page;
use crate::storage::itemid::LP_NORMAL;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;
use crate::utils::rel::RelationData;

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

// ===========================================================================
// Relation-bound tree descent + index scan (the buffer-by-relation path).
//
// Async-lock discipline (rules.md s5/s8): the descent reads each page into an
// OWNED copy (`bt_read_page_copy`), drops the content lock, decides where to go,
// then awaits the next read. No `parking_lot` guard is ever held across `.await`.
// The scan keeps no buffer pin between `bt_next` calls: the current leaf page is
// owned, and the next leaf is read fresh by following the right-link. This is a
// faithful rewrite-to-design of the C pin-holding scan for the async world; for a
// single-process port with snapshot isolation the visible-tuple semantics match.
// ===========================================================================

/// Find the downlink offset on an internal page for the search key (the
/// `_bt_binsrch` internal-page variant: the last pivot whose key is <= the search
/// key). Returns an offset in `[P_FIRSTDATAKEY, maxoff]`.
fn bt_binsrch_internal(
    rel: &RelationData,
    key: &mut BTScanInsertData,
    page: &Page,
) -> OffsetNumber {
    // SAFETY: page is a btree internal page with a valid opaque area.
    let opaque = unsafe { &*BTPageGetOpaque(page) };
    let firstdatakey = P_FIRSTDATAKEY(opaque);
    let maxoff = page.get_max_offset_number();
    if maxoff < firstdatakey {
        return firstdatakey;
    }
    // Binary search: invariant low-1 has key <= search key. C uses
    // low in [firstdatakey, maxoff+1), high similar; result is low-1.
    let mut low = firstdatakey;
    let mut high = maxoff;
    // _bt_binsrch: find first offset with cmp(key, item) <= 0, then step back one
    // for the internal (downlink) case (the high key separates children).
    while low <= high {
        let mid = low + (high - low) / 2;
        let cmp = bt_compare(rel, key, page, mid);
        if cmp >= 0 {
            // key >= item[mid]: the downlink could be here or further right.
            low = mid + 1;
        } else {
            if mid == firstdatakey {
                break;
            }
            high = mid - 1;
        }
    }
    // low-1 is the last pivot with key >= it (the child to descend into). Clamp.
    if low > firstdatakey {
        low - 1
    } else {
        firstdatakey
    }
}

/// `_bt_moveright` (read path core): if the search key is greater than this page's
/// high key, the page has been split and the target moved right; follow the
/// right-link until the high key is >= the key (or we reach the rightmost page).
/// Returns the (block, owned page) to continue from.
async fn bt_moveright(
    shared: &Arc<SharedState>,
    rel: &RelationData,
    key: &mut BTScanInsertData,
    mut blkno: BlockNumber,
    mut page: Box<Page>,
) -> (BlockNumber, Box<Page>) {
    loop {
        // SAFETY: btree page opaque area.
        let opaque = unsafe { &*BTPageGetOpaque(&page) };
        if P_RIGHTMOST(opaque) {
            return (blkno, page);
        }
        let nextblk = opaque.next;
        // Compare key against the high key (offset P_HIKEY). cmp > 0 means the key
        // sorts after the high key -> the target is on a right sibling.
        let cmp = bt_compare(rel, key, &page, P_HIKEY);
        if cmp <= 0 {
            return (blkno, page);
        }
        // Step right.
        let buf = bt_read_buffer(shared, rel, nextblk).await;
        page = bt_read_page_copy(shared, buf);
        bt_relbuf(shared, buf);
        blkno = nextblk;
    }
}

/// `_bt_search` (read path): descend from the root to the leaf page that would
/// contain the search key. Returns `(leaf_block, owned_leaf_page)`, or `None` if
/// the index is empty.
async fn bt_search_read(
    shared: &Arc<SharedState>,
    rel: &RelationData,
    key: &mut BTScanInsertData,
) -> Option<(BlockNumber, Box<Page>)> {
    let (mut blkno, mut page) = bt_getroot_read(shared, rel).await?;

    loop {
        // Move right if the page split under a stale downlink.
        let (b, p) = bt_moveright(shared, rel, key, blkno, page).await;
        blkno = b;
        page = p;

        // SAFETY: btree page opaque.
        let opaque = unsafe { &*BTPageGetOpaque(&page) };
        if P_ISLEAF(opaque) {
            return Some((blkno, page));
        }

        // Internal page: pick the downlink and descend.
        let offnum = bt_binsrch_internal(rel, key, &page);
        let item_id = page.get_item_id(offnum);
        let item = page.get_item(&item_id);
        let itup = item_as_index_tuple(item);
        // SAFETY: itup is a live pivot tuple.
        let child = BTreeTupleGetDownLink(unsafe { &*itup });

        let buf = bt_read_buffer(shared, rel, child).await;
        page = bt_read_page_copy(shared, buf);
        bt_relbuf(shared, buf);
        blkno = child;
    }
}

/// `_bt_get_endpoint` (leftmost, read): descend from the root always taking the
/// first downlink, returning the leftmost leaf page + its block. `None` if empty.
async fn bt_get_endpoint_leftmost(
    shared: &Arc<SharedState>,
    rel: &RelationData,
) -> Option<(BlockNumber, Box<Page>)> {
    let (mut blkno, mut page) = bt_getroot_read(shared, rel).await?;
    loop {
        // SAFETY: btree page opaque.
        let opaque = unsafe { &*BTPageGetOpaque(&page) };
        if P_ISLEAF(opaque) {
            return Some((blkno, page));
        }
        let firstdatakey = P_FIRSTDATAKEY(opaque);
        let item_id = page.get_item_id(firstdatakey);
        let item = page.get_item(&item_id);
        let itup = item_as_index_tuple(item);
        // SAFETY: itup is a live pivot tuple.
        let child = BTreeTupleGetDownLink(unsafe { &*itup });
        let buf = bt_read_buffer(shared, rel, child).await;
        page = bt_read_page_copy(shared, buf);
        bt_relbuf(shared, buf);
        blkno = child;
    }
}

/// `_bt_binsrch` (leaf, relation form, public): first offset whose key is >= the
/// search key, in `[firstkey, maxoff+1]`. The insert path reuses this to find the
/// in-page placement offset.
#[must_use]
pub fn bt_binsrch_one(
    rel: &RelationData,
    key: &mut BTScanInsertData,
    page: &Page,
    firstkey: OffsetNumber,
    maxoff: OffsetNumber,
) -> OffsetNumber {
    bt_binsrch_leaf_rel(rel, key, page, firstkey, maxoff)
}

/// `_bt_search` (insert path): descend from the root to the target leaf, recording
/// the (block, child-offset) of each internal page descended through (the
/// `BTStack`). Returns `(leaf_block, owned_leaf_page, stack)`. The stack is
/// root-to-leaf order popped leaf-to-root by the split propagation. Assumes the
/// index has a root (the caller's `ensure_root` created one).
pub async fn bt_search_internal_path(
    shared: &Arc<SharedState>,
    rel: &RelationData,
    key: &mut BTScanInsertData,
) -> (BlockNumber, Box<Page>, Vec<(BlockNumber, OffsetNumber)>) {
    let mut stack: Vec<(BlockNumber, OffsetNumber)> = Vec::new();
    let (mut blkno, mut page) = bt_getroot_read(shared, rel)
        .await
        .unwrap_or_else(|| unreachable!("insert path requires an existing root"));

    loop {
        let (b, p) = bt_moveright(shared, rel, key, blkno, page).await;
        blkno = b;
        page = p;

        // SAFETY: btree page opaque.
        let opaque = unsafe { &*BTPageGetOpaque(&page) };
        if P_ISLEAF(opaque) {
            return (blkno, page, stack);
        }

        let offnum = bt_binsrch_internal(rel, key, &page);
        stack.push((blkno, offnum));
        let item_id = page.get_item_id(offnum);
        let item = page.get_item(&item_id);
        let itup = item_as_index_tuple(item);
        // SAFETY: itup is a live pivot tuple.
        let child = BTreeTupleGetDownLink(unsafe { &*itup });

        let buf = bt_read_buffer(shared, rel, child).await;
        page = bt_read_page_copy(shared, buf);
        bt_relbuf(shared, buf);
        blkno = child;
    }
}

/// The btree index-scan opaque state (`BTScanOpaqueData`, M2 owned form). Holds
/// the index relation, the insertion scankey built from the search arguments, the
/// current leaf page COPY + scan offset, and the next-leaf right-link block.
pub struct BtScan<'irel> {
    /// The INDEX relation this scan walks. Borrow (relation-ownership-plan step 2):
    /// the owner (the `IndexScanState`'s caller frame) holds the `Arc` above; the
    /// scan borrows it for its bounded, properly-nested lifetime.
    pub rel: &'irel RelationData,
    /// Search keys `(attno, strategy, datum)` for the leading columns. An equality
    /// scan uses `BT_EQUAL`; the executor's range scans (`<`/`<=`/`>`/`>=`) tag
    /// each key with its btree strategy number (M6 single-column int4).
    search_keys: Vec<(i32, StrategyNumber, Datum)>,
    /// The insertion scankey (comparators + args), built on first `bt_first`.
    key: Option<Box<BTScanInsertData>>,
    /// Current leaf page copy + its block number.
    cur_page: Option<Box<Page>>,
    cur_block: BlockNumber,
    /// Current offset within the leaf (next item to return).
    cur_off: OffsetNumber,
    /// Last offset on the current leaf.
    max_off: OffsetNumber,
    /// Whether the scan has been positioned (`bt_first` called).
    pub started: bool,
    /// Whether the scan is exhausted.
    done: bool,
}

impl<'irel> BtScan<'irel> {
    #[must_use]
    pub fn new(rel: &'irel RelationData) -> Self {
        Self {
            rel,
            search_keys: Vec::new(),
            key: None,
            cur_page: None,
            cur_block: P_NONE,
            cur_off: 0,
            max_off: 0,
            started: false,
            done: false,
        }
    }

    /// Set the equality search keys (the `index_rescan` keys). Resets position.
    pub fn set_search_keys(&mut self, keys: Vec<(i32, Datum)>) {
        self.search_keys = keys
            .into_iter()
            .map(|(a, d)| (a, BT_EQUAL_STRATEGY_NUMBER, d))
            .collect();
        self.key = None;
        self.cur_page = None;
        self.started = false;
        self.done = false;
    }

    /// Set strategy-tagged search keys (the executor's `index_rescan` keys). Each
    /// key is `(attno, strategy, argument)`; `=`/`<`/`<=`/`>`/`>=` are honored
    /// (M6 single-column int4). Resets position.
    pub fn set_strategy_keys(&mut self, keys: Vec<(i32, StrategyNumber, Datum)>) {
        self.search_keys = keys;
        self.key = None;
        self.cur_page = None;
        self.started = false;
        self.done = false;
    }

    /// Deform the index tuple last returned by `bt_first`/`bt_next` into
    /// `(values, isnull)` of length `itupdesc.natts` (PG `index_deform_tuple`, the
    /// index-only-scan data source). The current item is at `cur_off - 1` (the scan
    /// advances `cur_off` past it on return). Returns `None` if no item is current.
    #[must_use]
    pub fn current_index_values(&self) -> Option<(Vec<Datum>, Vec<bool>)> {
        let page = self.cur_page.as_ref()?;
        if self.cur_off == 0 {
            return None;
        }
        let off = self.cur_off - 1;
        if off < P_FIRSTDATAKEY_for(page) || off > self.max_off {
            return None;
        }
        let item_id = page.get_item_id(off);
        if item_id.lp_flags() != LP_NORMAL {
            return None;
        }
        let item = page.get_item(&item_id);
        let itup = item_as_index_tuple(item);
        let itupdesc = self.rel.descr();
        let natts = itupdesc.natts as usize;
        let mut values = vec![Datum(0); natts];
        let mut isnull = vec![false; natts];
        // SAFETY: itup is a live leaf index tuple; itupdesc is the index rowtype.
        unsafe {
            crate::backend::access::common::indextuple::index_deform_tuple(
                itup, &itupdesc, &mut values, &mut isnull,
            );
        }
        Some((values, isnull))
    }
}

/// The argument of the leading key with a lower-bound strategy (`=`/`>=`/`>`), if
/// any. Used to decide whether `bt_first` descends to a key or to the leftmost
/// leaf. Returns `(attno, strategy, datum)`.
fn lower_bound_key(scan: &BtScan<'_>) -> Option<(i32, StrategyNumber, Datum)> {
    scan.search_keys
        .iter()
        .copied()
        .find(|&(_, strat, _)| {
            strat == BT_EQUAL_STRATEGY_NUMBER
                || strat == BT_GREATER_EQUAL_STRATEGY_NUMBER
                || strat == BT_GREATER_STRATEGY_NUMBER
        })
}

/// Read the current item's heap TID from the scan's leaf page at `cur_off`.
fn scan_current_tid(scan: &BtScan<'_>) -> Option<ItemPointerData> {
    let page = scan.cur_page.as_ref()?;
    if scan.cur_off < P_FIRSTDATAKEY_for(page) || scan.cur_off > scan.max_off {
        return None;
    }
    let item_id = page.get_item_id(scan.cur_off);
    if item_id.lp_flags() != LP_NORMAL {
        return None;
    }
    let item = page.get_item(&item_id);
    let itup = item_as_index_tuple(item);
    // The heap TID is the index tuple's t_tid (non-pivot leaf tuple).
    // SAFETY: itup is a live leaf index tuple.
    Some(unsafe { (*itup).tid })
}

/// First data offset for a leaf page (P_HIKEY if rightmost else P_FIRSTKEY).
fn p_firstdatakey_page(page: &Page) -> OffsetNumber {
    // SAFETY: btree page opaque.
    let opaque = unsafe { &*BTPageGetOpaque(page) };
    P_FIRSTDATAKEY(opaque)
}

/// Alias used by [`scan_current_tid`].
fn P_FIRSTDATAKEY_for(page: &Page) -> OffsetNumber {
    p_firstdatakey_page(page)
}

/// `_bt_first`: position the scan at the first matching entry and return its heap
/// TID, or `None` if no match. Builds the insertion scankey from the search args,
/// descends to the leaf, and binsearches the start offset.
pub async fn bt_first(
    shared: &Arc<SharedState>,
    scan: &mut BtScan<'_>,
    dir: ScanDirection,
) -> Option<ItemPointerData> {
    crate::assert!(scan_direction_is_forward(dir), "M2 btree scan supports forward only");
    scan.started = true;

    // Build the insertion scankey (comparators from the opclass), set heapkeyspace
    // from the meta page, and install the lower-bound argument (if any). A scan
    // with only an upper bound (`<`/`<=`) or no key starts at the leftmost leaf.
    let mut key = bt_mkscankey(scan.rel, None);
    let (heapkeyspace, allequalimage) = bt_metaversion(shared, scan.rel).await;
    key.heapkeyspace = heapkeyspace;
    key.allequalimage = allequalimage;

    let lower = lower_bound_key(scan);
    if let Some((_attno, _strat, datum)) = lower {
        bt_scankey_set_search_args(&mut key, &[(datum, false)]);
    } else {
        key.keysz = 0;
    }

    // A scan with a lower bound descends to the leaf that would contain it;
    // otherwise (no lower bound) descend to the LEFTMOST leaf.
    let descended = if key.keysz == 0 {
        bt_get_endpoint_leftmost(shared, scan.rel).await
    } else {
        bt_search_read(shared, scan.rel, &mut key).await
    };
    let Some((blkno, page)) = descended else {
        scan.done = true;
        return None;
    };

    let firstdatakey = p_firstdatakey_page(&page);
    let maxoff = page.get_max_offset_number();

    // Binary search for the first leaf offset >= the lower bound. With no lower
    // bound, start at the first data key (left endpoint).
    let start = if key.keysz == 0 {
        firstdatakey
    } else {
        bt_binsrch_leaf_rel(scan.rel, &mut key, &page, firstdatakey, maxoff)
    };

    scan.cur_page = Some(page);
    scan.cur_block = blkno;
    scan.cur_off = start;
    scan.max_off = maxoff;
    scan.key = Some(key);
    scan.done = false;

    advance_to_match(shared, scan).await
}

/// `_bt_binsrch` (leaf, relation form): first offset whose key is >= the search
/// key. Mirrors the page-level [`bt_binsrch_leaf`] but uses [`bt_compare`].
fn bt_binsrch_leaf_rel(
    rel: &RelationData,
    key: &mut BTScanInsertData,
    page: &Page,
    firstkey: OffsetNumber,
    maxoff: OffsetNumber,
) -> OffsetNumber {
    let mut low = firstkey;
    if maxoff < low {
        return low;
    }
    let mut high = maxoff + 1;
    while low < high {
        let mid = low + (high - low) / 2;
        if bt_compare(rel, key, page, mid) > 0 {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

/// From the current position, return the TID if it matches the equality keys;
/// otherwise advance (and, at end of leaf, follow the right-link) until a match or
/// the equality range is exhausted.
async fn advance_to_match(
    shared: &Arc<SharedState>,
    scan: &mut BtScan<'_>,
) -> Option<ItemPointerData> {
    loop {
        if scan.done {
            return None;
        }
        // Past the end of the current leaf: step to the next leaf.
        if scan.cur_off > scan.max_off {
            if !bt_steppage(shared, scan).await {
                scan.done = true;
                return None;
            }
            continue;
        }

        let item_id = scan
            .cur_page
            .as_ref()
            .map(|p| p.get_item_id(scan.cur_off));
        let Some(item_id) = item_id else {
            scan.done = true;
            return None;
        };
        if item_id.lp_flags() != LP_NORMAL {
            scan.cur_off += 1;
            continue;
        }

        // Check the search keys (if any). With no keys, every tuple matches.
        if scan.search_keys.is_empty() {
            let tid = scan_current_tid(scan);
            scan.cur_off += 1;
            return tid;
        }

        let (matches, continuescan) = check_strategy_keys(scan);
        if matches {
            let tid = scan_current_tid(scan);
            scan.cur_off += 1;
            return tid;
        }
        if !continuescan {
            scan.done = true;
            return None;
        }
        scan.cur_off += 1;
    }
}

/// Evaluate every strategy search key against the current item, returning
/// `(matches, continuescan)`. `matches` is true when all keys are satisfied;
/// `continuescan` is false once an ascending upper bound (`<`/`<=`, or the upper
/// edge of `=`) is passed, so the scan can stop (mirrors `_bt_checkkeys`).
fn check_strategy_keys(scan: &mut BtScan<'_>) -> (bool, bool) {
    let keys = scan.search_keys.clone();
    let rel = scan.rel;
    let Some(page) = scan.cur_page.as_ref() else {
        return (false, false);
    };
    let off = scan.cur_off;
    let Some(key) = scan.key.as_mut() else {
        return (false, false);
    };

    let mut matches = true;
    let mut continuescan = true;
    for (attno, strat, arg) in keys {
        // cmp = index_value <=> arg.
        let cmp = bt_compare_col_value(rel, key, attno, arg, page, off);
        let (ok, cont) = match strat {
            BT_EQUAL_STRATEGY_NUMBER => (cmp == 0, cmp <= 0),
            BT_GREATER_EQUAL_STRATEGY_NUMBER => (cmp >= 0, true),
            BT_GREATER_STRATEGY_NUMBER => (cmp > 0, true),
            BT_LESS_EQUAL_STRATEGY_NUMBER => (cmp <= 0, cmp <= 0),
            BT_LESS_STRATEGY_NUMBER => (cmp < 0, cmp < 0),
            _ => unreachable!("M6 btree scan keys use only the five comparison strategies"),
        };
        if !ok {
            matches = false;
        }
        if !cont {
            continuescan = false;
        }
    }
    (matches, continuescan)
}

/// `_bt_steppage` (forward): advance to the right-sibling leaf, loading its page
/// copy. Returns false at the end of the rightmost leaf.
async fn bt_steppage(shared: &Arc<SharedState>, scan: &mut BtScan<'_>) -> bool {
    let next = {
        let Some(page) = scan.cur_page.as_ref() else { return false };
        // SAFETY: btree leaf page opaque.
        let opaque = unsafe { &*BTPageGetOpaque(page) };
        if P_RIGHTMOST(opaque) {
            P_NONE
        } else {
            opaque.next
        }
    };
    if next == P_NONE {
        scan.cur_page = None;
        return false;
    }
    let buf = bt_read_buffer(shared, scan.rel, next).await;
    let page = bt_read_page_copy(shared, buf);
    bt_relbuf(shared, buf);
    let firstdatakey = p_firstdatakey_page(&page);
    scan.max_off = page.get_max_offset_number();
    scan.cur_block = next;
    scan.cur_off = firstdatakey;
    scan.cur_page = Some(page);
    true
}

/// `_bt_next`: return the next matching heap TID, or `None` at end of scan.
pub async fn bt_next(
    shared: &Arc<SharedState>,
    scan: &mut BtScan<'_>,
    dir: ScanDirection,
) -> Option<ItemPointerData> {
    crate::assert!(scan_direction_is_forward(dir), "M2 btree scan supports forward only");
    if !scan.started {
        return bt_first(shared, scan, dir).await;
    }
    advance_to_match(shared, scan).await
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
            att.atttypid = crate::postgres_ext::Oid::new(23); // INT4OID
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

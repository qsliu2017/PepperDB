//! Utility code for Postgres btree implementation. Translated from the
//! M2-reachable parts of `src/backend/access/nbtree/nbtutils.c`.
//!
//! M2 scope (step 13-rest): `_bt_mkscankey` (build the insertion scankey that
//! descends the tree, with the per-column comparator resolved from the opclass via
//! `index_getprocinfo`), `_bt_compare` (compare an insertion scankey against an
//! index tuple on a page, calling the comparator through fmgr), and the simple
//! equality `_bt_checkkeys` the M2 search path uses. SAOP/skip arrays, posting
//! lists, kill-items, and the preprocess-keys planner machinery are later-scope
//! grow guards.
//!
//! The comparator contract: the btree `BTORDER_PROC` (`bt*cmp`) returns `< 0`,
//! `0`, `> 0` for `index_value <=> argument`. `_bt_compare` sign-flips it to the
//! "scankey vs index item" convention (unless the column is DESC), exactly as
//! nbtsearch.c's `_bt_compare` does.


use crate::access::itup::IndexTuple;
use crate::access::nbtree::{
    BTScanInsertData, BTreeTupleGetNAtts, SK_BT_DESC, SK_BT_INDOPTION_SHIFT, SK_BT_NULLS_FIRST,
    BTORDER_PROC,
};
use crate::access::skey::{ScanKeyData, ScanKeyFlags};
use crate::backend::access::common::indextuple::index_getattr;
use crate::backend::access::index::indexam::index_getprocinfo;
use crate::fmgr::{FmgrInfo, FunctionCall2Coll};
use crate::postgres::Datum;
use crate::storage::bufpage::Page;
use crate::storage::itemid::LP_NORMAL;
use crate::storage::off::OffsetNumber;
use crate::utils::rel::RelationData;

/// Reinterpret a page item's bytes as an index tuple handle.
///
/// `index_getattr` reads through the handle (typed `*mut IndexTupleData`); the page
/// is `align(8)` and items live at MAXALIGN offsets, so the 2-byte-aligned overlay
/// is sound (matches itup.rs / nbtsearch.rs).
#[allow(
    clippy::cast_ptr_alignment,
    reason = "Page is align(8); page items live at MAXALIGN offsets, so the IndexTupleData overlay is well-aligned (matches itup.rs)"
)]
fn item_as_index_tuple(item: &[u8]) -> IndexTuple {
    item.as_ptr().cast::<crate::access::itup::IndexTupleData>().cast_mut()
}

/// `_bt_mkscankey` (M2 form): build the insertion scankey used to descend the
/// tree. If `itup` is given, the key arguments are that index tuple's key
/// attributes (the btinsert / unique-check path); if `None`, the caller fills the
/// argument datums afterward (the search path -- see [`bt_scankey_set_search_args`]).
///
/// The per-column comparator (`BTORDER_PROC`) is resolved from the opclass via
/// [`index_getprocinfo`] and stored in each `ScanKeyData.func`. The DESC /
/// NULLS_FIRST flags come from `rd_indoption[i]` shifted into the SK_BT_* byte.
///
/// `heapkeyspace`/`allequalimage` are set by the caller (it needs the meta page,
/// which is async); this builds the column array and leaves those as the utility
/// defaults (`heapkeyspace=true`).
#[must_use]
pub fn bt_mkscankey(rel: &RelationData, itup: Option<IndexTuple>) -> Box<BTScanInsertData> {
    let r: &RelationData = rel;
    let itupdesc = r.descr();
    let indnkeyatts = r.index_number_of_key_attributes() as usize;
    // SAFETY: itup, when Some, is a live index tuple on a page.
    let tupnatts = itup.map_or(0, |t| BTreeTupleGetNAtts(unsafe { &*t }, rel) as usize);

    let keysz = indnkeyatts.min(if itup.is_some() { tupnatts } else { indnkeyatts });

    let mut scankeys: Vec<ScanKeyData> = Vec::with_capacity(indnkeyatts);
    let mut anynullkeys = false;

    for i in 0..indnkeyatts {
        // The support FmgrInfo for this key column's comparator (owned copy).
        let func = index_getprocinfo(rel, (i + 1) as i32, BTORDER_PROC);

        let (arg, isnull) = itup.map_or((Datum(0), false), |t| {
            if i < tupnatts {
                // SAFETY: t is a live index tuple; attno 1-based.
                unsafe { index_getattr(t, (i + 1) as i32, &itupdesc) }
            } else {
                (Datum(0), true)
            }
        });

        let indoption = r.rd_indoption[i];
        let collation = r.rd_indcollation[i];
        let mut flags = i32::from(indoption) << SK_BT_INDOPTION_SHIFT;
        if isnull {
            flags |= ScanKeyFlags::ISNULL.bits();
            anynullkeys = true;
        }

        scankeys.push(ScanKeyData {
            flags,
            attno: (i + 1) as i16,
            strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
            subtype: crate::postgres_ext::InvalidOid,
            collation,
            func,
            argument: arg,
        });
    }

    Box::new(BTScanInsertData {
        heapkeyspace: true,
        allequalimage: false,
        anynullkeys,
        nextkey: false,
        backward: false,
        scantid: None,
        keysz: keysz as i32,
        scankeys,
    })
}

/// Set the search-argument datums (and per-column null flags) on a scankey built
/// with `itup = None`. `args[i]` is the equality argument for key column `i+1`;
/// `keysz` is set to the number of provided columns (a partial-key prefix search).
pub fn bt_scankey_set_search_args(key: &mut BTScanInsertData, args: &[(Datum, bool)]) {
    for (i, &(arg, isnull)) in args.iter().enumerate() {
        if i >= key.scankeys.len() {
            break;
        }
        key.scankeys[i].argument = arg;
        if isnull {
            key.scankeys[i].flags |= ScanKeyFlags::ISNULL.bits();
        } else {
            key.scankeys[i].flags &= !ScanKeyFlags::ISNULL.bits();
        }
    }
    key.keysz = args.len().min(key.scankeys.len()) as i32;
}

/// `_bt_compare`: compare the insertion scankey `key` against the index tuple at
/// `offnum` on `page` of index `rel`. Returns `< 0` if the key sorts before the
/// item, `0` if equal across all `keysz` columns, `> 0` if after.
///
/// Calls each column's comparator (`key.scankeys[i].func`) via fmgr; the raw
/// result is `index_value <=> argument`, sign-flipped to "key vs item" unless the
/// column is DESC, with NULL ordering per the NULLS_FIRST flag (matches the
/// page-level [`super::nbtsearch::bt_compare_page_item`]).
///
/// SAFETY: `page` holds a valid index tuple at `offnum`; `rel` is a live index.
pub fn bt_compare(
    rel: &RelationData,
    key: &mut BTScanInsertData,
    page: &Page,
    offnum: OffsetNumber,
) -> i32 {
    let r: &RelationData = rel;
    let itupdesc = r.descr();

    let item_id = page.get_item_id(offnum);
    debug_assert_eq!(item_id.lp_flags(), LP_NORMAL);
    let item = page.get_item(&item_id);
    let itup = item_as_index_tuple(item);

    // The tuple may have fewer attributes than the key (pivot truncation); compare
    // only min(keysz, tuple natts).
    // SAFETY: itup is a live index tuple.
    let tupnatts = i32::from(BTreeTupleGetNAtts(unsafe { &*itup }, rel));
    let ncmp = key.keysz.min(tupnatts);

    for i in 0..ncmp as usize {
        let sk = &mut key.scankeys[i];
        // SAFETY: itup live; attno 1-based key attribute.
        let (datum, isnull) = unsafe { index_getattr(itup, i32::from(sk.attno), &itupdesc) };
        let argnull = sk.flags & ScanKeyFlags::ISNULL.bits() != 0;

        let result = if argnull {
            if isnull {
                0
            } else if sk.flags & SK_BT_NULLS_FIRST != 0 {
                -1
            } else {
                1
            }
        } else if isnull {
            if sk.flags & SK_BT_NULLS_FIRST != 0 {
                1
            } else {
                -1
            }
        } else {
            // comparator computes index_value <=> argument; flip to "key vs item"
            // unless DESC.
            let raw = call_cmp(&mut sk.func, sk.collation, datum, sk.argument);
            if sk.flags & SK_BT_DESC == 0 {
                -raw
            } else {
                raw
            }
        };

        if result != 0 {
            return result;
        }
    }

    // Tuple is either equal so far. If the key has more columns than the tuple
    // (truncated pivot), the pivot sorts low (-1 means key > pivot, i.e. continue
    // right); PG returns 1 (key sorts after a truncated pivot). For the M2 search
    // path all tuples are non-truncated leaf tuples, so ncmp == keysz and this is 0.
    i32::from(key.keysz > tupnatts)
}

/// Invoke a btree comparator (`bt*cmp`) returning its `i32` three-way result.
fn call_cmp(func: &mut FmgrInfo, collation: crate::postgres_ext::Oid, a: Datum, b: Datum) -> i32 {
    let d = FunctionCall2Coll(func, collation, a, b)
        .unwrap_or_else(|| unreachable!("btree comparator returned NULL"));
    // The comparator returns an int4 Datum.
    crate::postgres::DatumGetInt32(d)
}

/// `_bt_checkkeys` (M2 equality form): does the index tuple at the current scan
/// position satisfy every equality search key? Used by the scan to stop once the
/// key no longer matches (continuescan = false). Returns `(matches, continuescan)`.
///
/// For an equality scan, once `bt_compare` of the search key vs the tuple is `< 0`
/// (the tuple sorts after the key) the scan can stop. `== 0` matches; this M2 form
/// only supports the leading equality prefix the search/systable path uses.
#[must_use]
pub fn bt_checkkeys_eq(
    rel: &RelationData,
    key: &mut BTScanInsertData,
    page: &Page,
    offnum: OffsetNumber,
) -> (bool, bool) {
    let cmp = bt_compare(rel, key, page, offnum);
    // cmp < 0: key < tuple -> tuple is past the equality range -> stop.
    // cmp == 0: match.
    // cmp > 0: key > tuple -> shouldn't happen once positioned, but keep scanning.
    match cmp.cmp(&0) {
        std::cmp::Ordering::Equal => (true, true),
        std::cmp::Ordering::Less => (false, false),
        std::cmp::Ordering::Greater => (false, true),
    }
}

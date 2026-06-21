//! Translation of postgres/src/include/storage/itemptr.h
//!                (+ ItemPointerEquals/ItemPointerCompare from
//!                 postgres/src/backend/storage/page/itemptr.c)
//!
//! ItemPointer: a logical disk-tuple address (block number + offset), a.k.a. the
//! `ctid` system column / the SQL `tid` type's internal form.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::{int32, PointerIsValid, PG_UINT16_MAX};
use crate::Assert;
use crate::storage::block::{
    BlockIdData, BlockIdGetBlockNumber, BlockIdSet, BlockNumber, InvalidBlockNumber,
};
use crate::storage::off::{InvalidOffsetNumber, OffsetNumber};
use crate::postgres::{Datum, DatumGetPointer, PointerGetDatum};
use core::ffi::c_void;

/*
 * ItemPointer:
 *
 * This is a pointer to an item within a disk page of a known file (e.g., the
 * block number and item offset).  All fields are uint16 so the struct is 6 bytes
 * with 2-byte alignment (the C struct is pg_attribute_packed/aligned(2)).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ItemPointerData {
    pub ip_blkid: BlockIdData,
    pub ip_posid: OffsetNumber,
}
pub type ItemPointer = *mut ItemPointerData;

/* special ItemPointer offset values (itemptr.h) */
pub const SpecTokenOffsetNumber: OffsetNumber = 0xfffe;
pub const MovedPartitionsOffsetNumber: OffsetNumber = 0xfffd;
pub const MovedPartitionsBlockNumber: BlockNumber = InvalidBlockNumber;

// ---- support functions (itemptr.h static inlines) ----

/// ItemPointerIsValid - true iff the disk item pointer is not NULL (and posid != 0).
///
/// # Safety
/// `pointer` is null or references a valid ItemPointerData.
#[inline]
pub unsafe fn ItemPointerIsValid(pointer: *const ItemPointerData) -> bool {
    PointerIsValid(pointer) && (*pointer).ip_posid != 0
}

/// # Safety
/// `pointer` references a valid ItemPointerData.
#[inline]
pub unsafe fn ItemPointerGetBlockNumberNoCheck(pointer: *const ItemPointerData) -> BlockNumber {
    BlockIdGetBlockNumber(&(*pointer).ip_blkid)
}
/// # Safety
/// As above; asserts the pointer looks valid.
#[inline]
pub unsafe fn ItemPointerGetBlockNumber(pointer: *const ItemPointerData) -> BlockNumber {
    Assert!(ItemPointerIsValid(pointer));
    ItemPointerGetBlockNumberNoCheck(pointer)
}
/// # Safety
/// `pointer` references a valid ItemPointerData.
#[inline]
pub unsafe fn ItemPointerGetOffsetNumberNoCheck(pointer: *const ItemPointerData) -> OffsetNumber {
    (*pointer).ip_posid
}
/// # Safety
/// As above; asserts the pointer looks valid.
#[inline]
pub unsafe fn ItemPointerGetOffsetNumber(pointer: *const ItemPointerData) -> OffsetNumber {
    Assert!(ItemPointerIsValid(pointer));
    ItemPointerGetOffsetNumberNoCheck(pointer)
}

/// ItemPointerSet - set to the specified block and offset.
///
/// # Safety
/// `pointer` is writable.
#[inline]
pub unsafe fn ItemPointerSet(pointer: *mut ItemPointerData, blockNumber: BlockNumber, offNum: OffsetNumber) {
    Assert!(PointerIsValid(pointer));
    BlockIdSet(&mut (*pointer).ip_blkid, blockNumber);
    (*pointer).ip_posid = offNum;
}
/// # Safety
/// `pointer` is writable.
#[inline]
pub unsafe fn ItemPointerSetBlockNumber(pointer: *mut ItemPointerData, blockNumber: BlockNumber) {
    Assert!(PointerIsValid(pointer));
    BlockIdSet(&mut (*pointer).ip_blkid, blockNumber);
}
/// # Safety
/// `pointer` is writable.
#[inline]
pub unsafe fn ItemPointerSetOffsetNumber(pointer: *mut ItemPointerData, offsetNumber: OffsetNumber) {
    Assert!(PointerIsValid(pointer));
    (*pointer).ip_posid = offsetNumber;
}

/// ItemPointerCopy - copy one disk item pointer to another.
///
/// # Safety
/// Both pointers are valid; `toPointer` is writable.
#[inline]
pub unsafe fn ItemPointerCopy(fromPointer: *const ItemPointerData, toPointer: *mut ItemPointerData) {
    Assert!(PointerIsValid(toPointer));
    Assert!(PointerIsValid(fromPointer));
    *toPointer = *fromPointer;
}

/// ItemPointerSetInvalid.
///
/// # Safety
/// `pointer` is writable.
#[inline]
pub unsafe fn ItemPointerSetInvalid(pointer: *mut ItemPointerData) {
    Assert!(PointerIsValid(pointer));
    BlockIdSet(&mut (*pointer).ip_blkid, InvalidBlockNumber);
    (*pointer).ip_posid = InvalidOffsetNumber;
}

/*
 * ItemPointerEquals (itemptr.c) - block+offset equality.
 *
 * # Safety
 * Both pointers reference valid ItemPointerData.
 */
#[no_mangle]
pub unsafe fn ItemPointerEquals(pointer1: ItemPointer, pointer2: ItemPointer) -> bool {
    ItemPointerGetBlockNumber(pointer1) == ItemPointerGetBlockNumber(pointer2)
        && ItemPointerGetOffsetNumber(pointer1) == ItemPointerGetOffsetNumber(pointer2)
}

/*
 * ItemPointerCompare (itemptr.c) - total order by (block, offset), using the
 * NoCheck accessors (a user-supplied tid may legally have ip_posid == 0).
 *
 * # Safety
 * Both pointers reference valid ItemPointerData.
 */
pub unsafe fn ItemPointerCompare(arg1: ItemPointer, arg2: ItemPointer) -> int32 {
    let b1 = ItemPointerGetBlockNumberNoCheck(arg1);
    let b2 = ItemPointerGetBlockNumberNoCheck(arg2);

    if b1 < b2 {
        -1
    } else if b1 > b2 {
        1
    } else if ItemPointerGetOffsetNumberNoCheck(arg1) < ItemPointerGetOffsetNumberNoCheck(arg2) {
        -1
    } else if ItemPointerGetOffsetNumberNoCheck(arg1) > ItemPointerGetOffsetNumberNoCheck(arg2) {
        1
    } else {
        0
    }
}

/*
 * ItemPointerInc (itemptr.c)
 *		Increment 'pointer' by 1 only paying attention to the ItemPointer's
 *		type's range limits and not MaxOffsetNumber and FirstOffsetNumber.
 *		This may result in 'pointer' becoming !OffsetNumberIsValid.
 *
 * If the pointer is already the maximum possible values permitted by the
 * range of the ItemPointer's types, then do nothing.
 *
 * # Safety
 * `pointer` is writable.
 */
pub unsafe fn ItemPointerInc(pointer: ItemPointer) {
    let mut blk = ItemPointerGetBlockNumberNoCheck(pointer);
    let mut off = ItemPointerGetOffsetNumberNoCheck(pointer);

    if off == PG_UINT16_MAX {
        if blk != InvalidBlockNumber {
            off = 0;
            blk += 1;
        }
    } else {
        off += 1;
    }

    ItemPointerSet(pointer, blk, off);
}

/*
 * ItemPointerDec (itemptr.c)
 *		Decrement 'pointer' by 1 only paying attention to the ItemPointer's
 *		type's range limits and not MaxOffsetNumber and FirstOffsetNumber.
 *		This may result in 'pointer' becoming !OffsetNumberIsValid.
 *
 * If the pointer is already the minimum possible values permitted by the
 * range of the ItemPointer's types, then do nothing.  This does rely on
 * FirstOffsetNumber being 1 rather than 0.
 *
 * # Safety
 * `pointer` is writable.
 */
pub unsafe fn ItemPointerDec(pointer: ItemPointer) {
    let mut blk = ItemPointerGetBlockNumberNoCheck(pointer);
    let mut off = ItemPointerGetOffsetNumberNoCheck(pointer);

    if off == 0 {
        if blk != 0 {
            off = PG_UINT16_MAX;
            blk -= 1;
        }
    } else {
        off -= 1;
    }

    ItemPointerSet(pointer, blk, off);
}

// ---- fmgr interface (itemptr.h) ----
/// # Safety
/// `x` is a Datum holding an ItemPointer.
#[inline]
pub unsafe fn DatumGetItemPointer(x: Datum) -> ItemPointer {
    DatumGetPointer(x) as ItemPointer
}
#[inline]
pub unsafe fn ItemPointerGetDatum(x: ItemPointer) -> Datum {
    PointerGetDatum(x as *const c_void)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn itemptr_set_get_compare() {
        unsafe {
            let mut a = ItemPointerData { ip_blkid: BlockIdData { bi_hi: 0, bi_lo: 0 }, ip_posid: 0 };
            let mut b = a;
            ItemPointerSet(&mut a, 0x0001_2345, 7);
            assert_eq!(ItemPointerGetBlockNumber(&a), 0x0001_2345);
            assert_eq!(ItemPointerGetOffsetNumber(&a), 7);

            ItemPointerSet(&mut b, 0x0001_2345, 9);
            assert!(ItemPointerCompare(&mut a, &mut b) < 0);
            assert!(!ItemPointerEquals(&mut a, &mut b));
            ItemPointerCopy(&a, &mut b);
            assert!(ItemPointerEquals(&mut a, &mut b));
            assert_eq!(ItemPointerCompare(&mut a, &mut b), 0);

            // block number dominates the ordering
            ItemPointerSet(&mut b, 0x0001_2346, 1);
            assert!(ItemPointerCompare(&mut a, &mut b) < 0);

            ItemPointerSetInvalid(&mut a);
            assert!(!ItemPointerIsValid(&a)); // posid == 0
        }
    }
}

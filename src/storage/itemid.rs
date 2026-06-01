//! Translation of postgres/src/include/storage/itemid.h
//!
//! Line-pointer (ItemId) definitions.  An ItemIdData is a C bitfield
//!   { unsigned lp_off:15, lp_flags:2, lp_len:15; }  (exactly 4 bytes).
//! Rust has no bitfields, so the struct holds a single u32 and the accessor
//! macros are reproduced as inline fns doing the bit math.  On the little-endian
//! targets this crate supports, the C compiler lays the first field in the low
//! bits, so: lp_off = bits[0..15], lp_flags = bits[15..17], lp_len = bits[17..32].
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::PointerIsValid;
use core::ffi::c_void;

/// A line pointer on a buffer page.  Layout-compatible with the C bitfield.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct ItemIdData {
    bits: u32, /* lp_off:15 | lp_flags:2 | lp_len:15 */
}

/// Pointer to a line pointer (the on-page array element type).
pub type ItemId = *mut ItemIdData;

/* lp_flags has these possible states. */
pub const LP_UNUSED: u32 = 0; /* unused (should always have lp_len=0) */
pub const LP_NORMAL: u32 = 1; /* used (should always have lp_len>0) */
pub const LP_REDIRECT: u32 = 2; /* HOT redirect (should have lp_len=0) */
pub const LP_DEAD: u32 = 3; /* dead, may or may not have storage */

const OFF_MASK: u32 = 0x7FFF; /* 15 bits */
const FLAGS_MASK: u32 = 0x3; /* 2 bits */
const LEN_MASK: u32 = 0x7FFF; /* 15 bits */

#[inline]
unsafe fn lp_off(it: ItemId) -> u32 {
    (*it).bits & OFF_MASK
}
#[inline]
unsafe fn lp_flags(it: ItemId) -> u32 {
    ((*it).bits >> 15) & FLAGS_MASK
}
#[inline]
unsafe fn lp_len(it: ItemId) -> u32 {
    ((*it).bits >> 17) & LEN_MASK
}
#[inline]
unsafe fn set_fields(it: ItemId, off: u32, flags: u32, len: u32) {
    (*it).bits =
        (off & OFF_MASK) | ((flags & FLAGS_MASK) << 15) | ((len & LEN_MASK) << 17);
}

/* ItemIdGetLength - byte length of the tuple this points to. */
#[inline]
pub unsafe fn ItemIdGetLength(it: ItemId) -> u32 {
    lp_len(it)
}
/* ItemIdGetOffset - offset to the tuple (from start of page). */
#[inline]
pub unsafe fn ItemIdGetOffset(it: ItemId) -> u32 {
    lp_off(it)
}
/* ItemIdGetFlags - the line-pointer state. */
#[inline]
pub unsafe fn ItemIdGetFlags(it: ItemId) -> u32 {
    lp_flags(it)
}
/* ItemIdGetRedirect - the link of a redirect line pointer (kept in lp_off). */
#[inline]
pub unsafe fn ItemIdGetRedirect(it: ItemId) -> u32 {
    lp_off(it)
}

/* ItemIdIsValid - is the pointer itself non-NULL. */
#[inline]
pub unsafe fn ItemIdIsValid(it: ItemId) -> bool {
    PointerIsValid(it as *const c_void)
}
/* ItemIdIsUsed - line pointer is in use (any non-unused state). */
#[inline]
pub unsafe fn ItemIdIsUsed(it: ItemId) -> bool {
    lp_flags(it) != LP_UNUSED
}
#[inline]
pub unsafe fn ItemIdIsNormal(it: ItemId) -> bool {
    lp_flags(it) == LP_NORMAL
}
#[inline]
pub unsafe fn ItemIdIsRedirected(it: ItemId) -> bool {
    lp_flags(it) == LP_REDIRECT
}
#[inline]
pub unsafe fn ItemIdIsDead(it: ItemId) -> bool {
    lp_flags(it) == LP_DEAD
}
/* ItemIdHasStorage - the line pointer has associated storage. */
#[inline]
pub unsafe fn ItemIdHasStorage(it: ItemId) -> bool {
    lp_len(it) != 0
}

/* Set just lp_off, preserving flags+len (C code does `lp->lp_off = x;`). */
#[inline]
pub unsafe fn ItemIdSetOffset(it: ItemId, off: u32) {
    let flags = lp_flags(it);
    let len = lp_len(it);
    set_fields(it, off, flags, len);
}

/* ItemIdSetUnused - mark unused (off=0, len=0). */
#[inline]
pub unsafe fn ItemIdSetUnused(it: ItemId) {
    set_fields(it, 0, LP_UNUSED, 0);
}
/* ItemIdSetNormal - point to a normal tuple at (off, len). */
#[inline]
pub unsafe fn ItemIdSetNormal(it: ItemId, off: u32, len: u32) {
    set_fields(it, off, LP_NORMAL, len);
}
/* ItemIdSetRedirect - HOT redirect to another line pointer `link`. */
#[inline]
pub unsafe fn ItemIdSetRedirect(it: ItemId, link: u32) {
    set_fields(it, link, LP_REDIRECT, 0);
}
/* ItemIdSetDead - mark dead with no storage. */
#[inline]
pub unsafe fn ItemIdSetDead(it: ItemId) {
    set_fields(it, 0, LP_DEAD, 0);
}
/* ItemIdMarkDead - mark dead but keep any existing storage. */
#[inline]
pub unsafe fn ItemIdMarkDead(it: ItemId) {
    let off = lp_off(it);
    let len = lp_len(it);
    set_fields(it, off, LP_DEAD, len);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_and_accessors() {
        assert_eq!(core::mem::size_of::<ItemIdData>(), 4);
        unsafe {
            let mut id = ItemIdData { bits: 0 };
            let p = &mut id as ItemId;
            ItemIdSetNormal(p, 8128, 24);
            assert_eq!(ItemIdGetOffset(p), 8128);
            assert_eq!(ItemIdGetLength(p), 24);
            assert!(ItemIdIsNormal(p) && ItemIdIsUsed(p) && ItemIdHasStorage(p));
            // independent fields don't bleed into each other.
            assert_eq!(ItemIdGetFlags(p), LP_NORMAL);

            ItemIdMarkDead(p);
            assert!(ItemIdIsDead(p));
            assert_eq!(ItemIdGetLength(p), 24); // storage kept
            assert_eq!(ItemIdGetOffset(p), 8128);

            ItemIdSetRedirect(p, 17);
            assert!(ItemIdIsRedirected(p));
            assert_eq!(ItemIdGetRedirect(p), 17);
            assert_eq!(ItemIdGetLength(p), 0);

            ItemIdSetUnused(p);
            assert!(!ItemIdIsUsed(p));
            assert_eq!(id.bits, 0);
        }
    }
}

//! Translation of postgres/src/include/nodes/bitmapset.h
//!                + postgres/src/backend/nodes/bitmapset.c
//!
//! PostgreSQL generic bitmap set package.
//!
//! A bitmap set can represent any set of nonnegative integers, although it is
//! mainly intended for sets where the maximum value is not large, say at most a
//! few hundred.  By convention, we always represent a set with the minimum
//! possible number of words, i.e, there are never any trailing zero words.
//! Enforcing this requires that an empty set is represented as NULL.  Because an
//! empty Bitmapset is represented as NULL, a non-NULL Bitmapset always has at
//! least 1 Bitmapword.  We can exploit this fact to speed up various loops over
//! the Bitmapset's words array by using "do while" loops instead of "for" loops.
//!
//! Callers must ensure that the set returned by functions in this file which
//! adjust the members of an existing set is assigned to all pointers pointing to
//! that existing set.  No guarantees are made that we'll ever modify the
//! existing set in-place and return it.
//!
//! To help find bugs caused by callers failing to record the return value of the
//! function which manipulates an existing set, we support building with
//! REALLOCATE_BITMAPSETS.  This results in the set being reallocated each time
//! the set is altered and the existing being pfreed.  This is useful as if any
//! references still exist to the old set, we're more likely to notice as any
//! users of the old set will be accessing pfree'd memory.  This option is only
//! intended to be used for debugging.
//!
//! Portions Copyright (c) 2003-2025, PostgreSQL Global Development Group
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * `Bitmapset` is a `#[repr(C)]` struct with a trailing FLEXIBLE_ARRAY_MEMBER
//!   `words: [bitmapword; 0]`.  Allocation uses BITMAPSET_SIZE(nwords) =
//!   offsetof(Bitmapset, words) + nwords*sizeof(bitmapword), and the words array
//!   is accessed through the `words_ptr` helper rather than direct field access.
//!
//! * This is the SIZEOF_VOID_P >= 8 (64-bit) build: bitmapword = uint64,
//!   signedbitmapword = int64, BITS_PER_BITMAPWORD = 64, and the bmw_* helpers
//!   alias the *64 bit-twiddling functions.  The 32-bit word branch is noted with
//!   TODO(pg-port).
//!
//! * `bms_is_valid_set` (USE_ASSERT_CHECKING) and `bms_copy_and_free`
//!   (REALLOCATE_BITMAPSETS) are translated; the REALLOCATE_BITMAPSETS call sites
//!   are noted with TODO(pg-port) and compiled out, matching a default build.
//!
//! * Functions that dereference raw `*mut Bitmapset` pointers are `pub unsafe fn`.
//!   NULL is the canonical empty set: kept as `core::ptr::null_mut()` / `.is_null()`.

use crate::prelude::*;
use core::ffi::{c_int, c_void};

use crate::IsA;
use crate::common::hashfn::hash_any;
use crate::nodes::nodes::NodeTag::T_Bitmapset;
use crate::nodes::nodes::NodeTag;
use crate::nodes::pg_list::{lfirst_int, List, NIL};
use crate::port::pg_bitutils::{pg_leftmost_one_pos64, pg_popcount64, pg_rightmost_one_pos64};
use crate::{current_cell, foreach};

// ===========================================================================
//                       bitmapset.h: data representation
// ===========================================================================

/*
 * Data representation
 *
 * Larger bitmap word sizes generally give better performance, so long as
 * they're not wider than the processor can handle efficiently.  We use
 * 64-bit words if pointers are that large, else 32-bit words.
 */
// SIZEOF_VOID_P >= 8 branch (64-bit build).
pub const BITS_PER_BITMAPWORD: c_int = 64;
pub type bitmapword = uint64; /* must be an unsigned type */
pub type signedbitmapword = int64; /* must be the matching signed type */
// TODO(pg-port): the SIZEOF_VOID_P < 8 branch defines BITS_PER_BITMAPWORD = 32,
// bitmapword = uint32, signedbitmapword = int32.  Not translated; this build is
// 64-bit only.

/*
 * typedef struct Bitmapset
 * {
 *     pg_node_attr(custom_copy_equal, special_read_write, no_query_jumble)
 *     NodeTag     type;
 *     int         nwords;       // number of words in array
 *     bitmapword  words[FLEXIBLE_ARRAY_MEMBER];  // really [nwords]
 * } Bitmapset;
 *
 * The pg_node_attr(...) marker has no runtime meaning and is dropped.
 */
#[repr(C)]
pub struct Bitmapset {
    pub r#type: NodeTag,
    /// number of words in array
    pub nwords: c_int,
    /// really [nwords]
    pub words: [bitmapword; FLEXIBLE_ARRAY_MEMBER],
}

/* result of bms_subset_compare */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BMS_Comparison {
    BMS_EQUAL,     /* sets are equal */
    BMS_SUBSET1,   /* first set is a subset of the second */
    BMS_SUBSET2,   /* second set is a subset of the first */
    BMS_DIFFERENT, /* neither set is a subset of the other */
}
pub use BMS_Comparison::*;

/* result of bms_membership */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BMS_Membership {
    BMS_EMPTY_SET, /* 0 members */
    BMS_SINGLETON, /* 1 member */
    BMS_MULTIPLE,  /* >1 member */
}
pub use BMS_Membership::*;

/*
 * Select appropriate bit-twiddling functions for bitmap word size.
 *
 * BITS_PER_BITMAPWORD == 64 branch.
 * (The 32-bit branch would use pg_*_one_pos32 / pg_popcount32 -- TODO(pg-port).)
 */
#[inline]
fn bmw_leftmost_one_pos(w: bitmapword) -> c_int {
    pg_leftmost_one_pos64(w)
}
#[inline]
fn bmw_rightmost_one_pos(w: bitmapword) -> c_int {
    pg_rightmost_one_pos64(w)
}
#[inline]
fn bmw_popcount(w: bitmapword) -> c_int {
    pg_popcount64(w)
}

/// `bms_is_empty(a)` = `((a) == NULL)`.
///
/// NULL is now the only allowed representation of an empty bitmapset.
#[inline]
pub fn bms_is_empty(a: *const Bitmapset) -> bool {
    a.is_null()
}

// ===========================================================================
//                       bitmapset.c: implementation
// ===========================================================================

/* #define WORDNUM(x)	((x) / BITS_PER_BITMAPWORD) */
#[inline]
const fn WORDNUM(x: c_int) -> c_int {
    x / BITS_PER_BITMAPWORD
}

/* #define BITNUM(x)	((x) % BITS_PER_BITMAPWORD) */
#[inline]
const fn BITNUM(x: c_int) -> c_int {
    x % BITS_PER_BITMAPWORD
}

/*
 * #define BITMAPSET_SIZE(nwords)
 *     (offsetof(Bitmapset, words) + (nwords) * sizeof(bitmapword))
 */
#[inline]
const fn BITMAPSET_SIZE(nwords: c_int) -> Size {
    core::mem::offset_of!(Bitmapset, words) + (nwords as Size) * core::mem::size_of::<bitmapword>()
}

/// Helper: pointer to the flexible `words[]` array of a Bitmapset.
///
/// In C `a->words[i]` indexes the trailing flexible array; in Rust we compute the
/// base address via offsetof and index from there.
///
/// # Safety
/// `a` must point to a Bitmapset whose chunk has room for `nwords` words.
#[inline]
unsafe fn words_ptr(a: *const Bitmapset) -> *const bitmapword {
    (a as *const u8).add(core::mem::offset_of!(Bitmapset, words)) as *const bitmapword
}

/// Mutable variant of [`words_ptr`].
///
/// # Safety
/// See [`words_ptr`]; `a` must additionally be writable.
#[inline]
unsafe fn words_ptr_mut(a: *mut Bitmapset) -> *mut bitmapword {
    (a as *mut u8).add(core::mem::offset_of!(Bitmapset, words)) as *mut bitmapword
}

/*----------
 * This is a well-known cute trick for isolating the rightmost one-bit
 * in a word.  It assumes two's complement arithmetic.  Consider any
 * nonzero value, and focus attention on the rightmost one.  The value is
 * then something like
 *				xxxxxx10000
 * where x's are unspecified bits.  The two's complement negative is formed
 * by inverting all the bits and adding one.  Inversion gives
 *				yyyyyy01111
 * where each y is the inverse of the corresponding x.  Incrementing gives
 *				yyyyyy10000
 * and then ANDing with the original value gives
 *				00000010000
 * This works for all cases except original value = zero, where of course
 * we get zero.
 *----------
 */
/* #define RIGHTMOST_ONE(x) ((signedbitmapword) (x) & -((signedbitmapword) (x))) */
//
// The C `-((signedbitmapword)(x))` overflows (UB-adjacent) for the minimum
// signed value; Rust panics on overflow in debug builds, so use wrapping_neg().
#[inline]
fn RIGHTMOST_ONE(x: bitmapword) -> bitmapword {
    ((x as signedbitmapword) & (x as signedbitmapword).wrapping_neg()) as bitmapword
}

/* #define HAS_MULTIPLE_ONES(x)	((bitmapword) RIGHTMOST_ONE(x) != (x)) */
#[inline]
fn HAS_MULTIPLE_ONES(x: bitmapword) -> bool {
    RIGHTMOST_ONE(x) != x
}

/*
 * bms_is_valid_set - for cassert builds to check for valid sets
 *
 * In C this is compiled only under USE_ASSERT_CHECKING.  Here it is always
 * present but is only ever invoked from inside Assert!, which is a no-op in
 * release builds.
 *
 * # Safety
 * `a` must be NULL or point to a (possibly pfree'd) Bitmapset.
 */
unsafe fn bms_is_valid_set(a: *const Bitmapset) -> bool {
    /* NULL is the correct representation of an empty set */
    if a.is_null() {
        return true;
    }

    /* check the node tag is set correctly.  pfree'd pointer, maybe? */
    if !IsA!(a, T_Bitmapset) {
        return false;
    }

    /* trailing zero words are not allowed */
    if *words_ptr(a).add(((*a).nwords - 1) as usize) == 0 {
        return false;
    }

    true
}

// TODO(pg-port): bms_copy_and_free (REALLOCATE_BITMAPSETS-only) is not translated;
// this build does not define REALLOCATE_BITMAPSETS, so the helper and all of its
// call sites are compiled out exactly as in a default PostgreSQL build:
//
//   static Bitmapset *
//   bms_copy_and_free(Bitmapset *a)
//   {
//       Bitmapset *c = bms_copy(a);
//       bms_free(a);
//       return c;
//   }

/*
 * bms_copy - make a palloc'd copy of a bitmapset
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_copy(a: *const Bitmapset) -> *mut Bitmapset {
    let result: *mut Bitmapset;
    let size: usize;

    Assert!(bms_is_valid_set(a));

    if a.is_null() {
        return core::ptr::null_mut();
    }

    size = BITMAPSET_SIZE((*a).nwords);
    result = palloc(size) as *mut Bitmapset;
    core::ptr::copy_nonoverlapping(a as *const u8, result as *mut u8, size);
    result
}

/*
 * bms_equal - are two bitmapsets equal? or both NULL?
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_equal(a: *const Bitmapset, b: *const Bitmapset) -> bool {
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        if b.is_null() {
            return true;
        }
        return false;
    } else if b.is_null() {
        return false;
    }

    /* can't be equal if the word counts don't match */
    if (*a).nwords != (*b).nwords {
        return false;
    }

    /* check each word matches */
    let aw = words_ptr(a);
    let bw = words_ptr(b);
    i = 0;
    loop {
        if *aw.add(i as usize) != *bw.add(i as usize) {
            return false;
        }
        i += 1;
        if !(i < (*a).nwords) {
            break;
        }
    }

    true
}

/*
 * bms_compare - qsort-style comparator for bitmapsets
 *
 * This guarantees to report values as equal iff bms_equal would say they are
 * equal.  Otherwise, the highest-numbered bit that is set in one value but
 * not the other determines the result.  (This rule means that, for example,
 * {6} is greater than {5}, which seems plausible.)
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_compare(a: *const Bitmapset, b: *const Bitmapset) -> c_int {
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        return if b.is_null() { 0 } else { -1 };
    } else if b.is_null() {
        return 1;
    }

    /* the set with the most words must be greater */
    if (*a).nwords != (*b).nwords {
        return if (*a).nwords > (*b).nwords { 1 } else { -1 };
    }

    let awords = words_ptr(a);
    let bwords = words_ptr(b);
    i = (*a).nwords - 1;
    loop {
        let aw: bitmapword = *awords.add(i as usize);
        let bw: bitmapword = *bwords.add(i as usize);

        if aw != bw {
            return if aw > bw { 1 } else { -1 };
        }
        i -= 1;
        if !(i >= 0) {
            break;
        }
    }
    0
}

/*
 * bms_make_singleton - build a bitmapset containing a single member
 */
pub unsafe fn bms_make_singleton(x: c_int) -> *mut Bitmapset {
    let result: *mut Bitmapset;
    let wordnum: c_int;
    let bitnum: c_int;

    if x < 0 {
        elog!(ERROR, "negative bitmapset member not allowed");
    }
    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);
    result = palloc0(BITMAPSET_SIZE(wordnum + 1)) as *mut Bitmapset;
    (*result).r#type = T_Bitmapset;
    (*result).nwords = wordnum + 1;
    *words_ptr_mut(result).add(wordnum as usize) = (1 as bitmapword) << bitnum;
    result
}

/*
 * bms_free - free a bitmapset
 *
 * Same as pfree except for allowing NULL input
 *
 * # Safety
 * `a` must be NULL or a palloc'd Bitmapset.
 */
pub unsafe fn bms_free(a: *mut Bitmapset) {
    if !a.is_null() {
        pfree(a as *mut c_void);
    }
}

/*
 * bms_copy_and_free - copy a set and free the original.
 *
 * This is useful for cleaning up after a result has been copied to a longer-
 * lived context.
 */
pub unsafe fn bms_copy_and_free(a: *mut Bitmapset) -> *mut Bitmapset {
    let c: *mut Bitmapset = bms_copy(a);

    bms_free(a);
    c
}

/*
 * bms_union - create and return a new set containing all members from both
 * input sets.  Both inputs are left unmodified.
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_union(a: *const Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    let result: *mut Bitmapset;
    let other: *const Bitmapset;
    let otherlen: c_int;
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        return bms_copy(b);
    }
    if b.is_null() {
        return bms_copy(a);
    }
    /* Identify shorter and longer input; copy the longer one */
    if (*a).nwords <= (*b).nwords {
        result = bms_copy(b);
        other = a;
    } else {
        result = bms_copy(a);
        other = b;
    }
    /* And union the shorter input into the result */
    otherlen = (*other).nwords;
    let rwords = words_ptr_mut(result);
    let owords = words_ptr(other);
    i = 0;
    loop {
        *rwords.add(i as usize) |= *owords.add(i as usize);
        i += 1;
        if !(i < otherlen) {
            break;
        }
    }
    result
}

/*
 * bms_intersect - create and return a new set containing members which both
 * input sets have in common.  Both inputs are left unmodified.
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_intersect(a: *const Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    let result: *mut Bitmapset;
    let other: *const Bitmapset;
    let mut lastnonzero: c_int;
    let resultlen: c_int;
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() || b.is_null() {
        return core::ptr::null_mut();
    }

    /* Identify shorter and longer input; copy the shorter one */
    if (*a).nwords <= (*b).nwords {
        result = bms_copy(a);
        other = b;
    } else {
        result = bms_copy(b);
        other = a;
    }
    /* And intersect the longer input with the result */
    resultlen = (*result).nwords;
    let rwords = words_ptr_mut(result);
    let owords = words_ptr(other);
    lastnonzero = -1;
    i = 0;
    loop {
        *rwords.add(i as usize) &= *owords.add(i as usize);

        if *rwords.add(i as usize) != 0 {
            lastnonzero = i;
        }
        i += 1;
        if !(i < resultlen) {
            break;
        }
    }
    /* If we computed an empty result, we must return NULL */
    if lastnonzero == -1 {
        pfree(result as *mut c_void);
        return core::ptr::null_mut();
    }

    /* get rid of trailing zero words */
    (*result).nwords = lastnonzero + 1;
    result
}

/*
 * bms_difference - create and return a new set containing all the members of
 * 'a' without the members of 'b'.
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_difference(a: *const Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    let result: *mut Bitmapset;
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        return core::ptr::null_mut();
    }
    if b.is_null() {
        return bms_copy(a);
    }

    /*
     * In Postgres' usage, an empty result is a very common case, so it's
     * worth optimizing for that by testing bms_nonempty_difference().  This
     * saves us a palloc/pfree cycle compared to checking after-the-fact.
     */
    if !bms_nonempty_difference(a, b) {
        return core::ptr::null_mut();
    }

    /* Copy the left input */
    result = bms_copy(a);

    let rwords = words_ptr_mut(result);
    let bwords = words_ptr(b);

    /* And remove b's bits from result */
    if (*result).nwords > (*b).nwords {
        /*
         * We'll never need to remove trailing zero words when 'a' has more
         * words than 'b' as the additional words must be non-zero.
         */
        i = 0;
        loop {
            *rwords.add(i as usize) &= !*bwords.add(i as usize);
            i += 1;
            if !(i < (*b).nwords) {
                break;
            }
        }
    } else {
        let mut lastnonzero: c_int = -1;

        /* we may need to remove trailing zero words from the result. */
        i = 0;
        loop {
            *rwords.add(i as usize) &= !*bwords.add(i as usize);

            /* remember the last non-zero word */
            if *rwords.add(i as usize) != 0 {
                lastnonzero = i;
            }
            i += 1;
            if !(i < (*result).nwords) {
                break;
            }
        }

        /* trim off trailing zero words */
        (*result).nwords = lastnonzero + 1;
    }
    Assert!((*result).nwords != 0);

    /* Need not check for empty result, since we handled that case above */
    result
}

/*
 * bms_is_subset - is A a subset of B?
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_is_subset(a: *const Bitmapset, b: *const Bitmapset) -> bool {
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        return true; /* empty set is a subset of anything */
    }
    if b.is_null() {
        return false;
    }

    /* 'a' can't be a subset of 'b' if it contains more words */
    if (*a).nwords > (*b).nwords {
        return false;
    }

    /* Check all 'a' members are set in 'b' */
    let awords = words_ptr(a);
    let bwords = words_ptr(b);
    i = 0;
    loop {
        if (*awords.add(i as usize) & !*bwords.add(i as usize)) != 0 {
            return false;
        }
        i += 1;
        if !(i < (*a).nwords) {
            break;
        }
    }
    true
}

/*
 * bms_subset_compare - compare A and B for equality/subset relationships
 *
 * This is more efficient than testing bms_is_subset in both directions.
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_subset_compare(a: *const Bitmapset, b: *const Bitmapset) -> BMS_Comparison {
    let mut result: BMS_Comparison;
    let shortlen: c_int;
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        if b.is_null() {
            return BMS_EQUAL;
        }
        return BMS_SUBSET1;
    }
    if b.is_null() {
        return BMS_SUBSET2;
    }

    /* Check common words */
    result = BMS_EQUAL; /* status so far */
    shortlen = Min((*a).nwords, (*b).nwords);
    let awords = words_ptr(a);
    let bwords = words_ptr(b);
    i = 0;
    loop {
        let aword: bitmapword = *awords.add(i as usize);
        let bword: bitmapword = *bwords.add(i as usize);

        if (aword & !bword) != 0 {
            /* a is not a subset of b */
            if result == BMS_SUBSET1 {
                return BMS_DIFFERENT;
            }
            result = BMS_SUBSET2;
        }
        if (bword & !aword) != 0 {
            /* b is not a subset of a */
            if result == BMS_SUBSET2 {
                return BMS_DIFFERENT;
            }
            result = BMS_SUBSET1;
        }
        i += 1;
        if !(i < shortlen) {
            break;
        }
    }
    /* Check extra words */
    if (*a).nwords > (*b).nwords {
        /* if a has more words then a is not a subset of b */
        if result == BMS_SUBSET1 {
            return BMS_DIFFERENT;
        }
        return BMS_SUBSET2;
    } else if (*a).nwords < (*b).nwords {
        /* if b has more words then b is not a subset of a */
        if result == BMS_SUBSET2 {
            return BMS_DIFFERENT;
        }
        return BMS_SUBSET1;
    }
    result
}

/*
 * bms_is_member - is X a member of A?
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_is_member(x: c_int, a: *const Bitmapset) -> bool {
    let wordnum: c_int;
    let bitnum: c_int;

    Assert!(bms_is_valid_set(a));

    /* XXX better to just return false for x<0 ? */
    if x < 0 {
        elog!(ERROR, "negative bitmapset member not allowed");
    }
    if a.is_null() {
        return false;
    }

    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);
    if wordnum >= (*a).nwords {
        return false;
    }
    if (*words_ptr(a).add(wordnum as usize) & ((1 as bitmapword) << bitnum)) != 0 {
        return true;
    }
    false
}

/*
 * bms_member_index
 *		determine 0-based index of member x in the bitmap
 *
 * Returns (-1) when x is not a member.
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_member_index(a: *mut Bitmapset, x: c_int) -> c_int {
    let mut i: c_int;
    let bitnum: c_int;
    let wordnum: c_int;
    let mut result: c_int = 0;
    let mask: bitmapword;

    Assert!(bms_is_valid_set(a));

    /* return -1 if not a member of the bitmap */
    if !bms_is_member(x, a) {
        return -1;
    }

    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);

    let words = words_ptr(a);

    /* count bits in preceding words */
    i = 0;
    while i < wordnum {
        let w: bitmapword = *words.add(i as usize);

        /* No need to count the bits in a zero word */
        if w != 0 {
            result += bmw_popcount(w);
        }
        i += 1;
    }

    /*
     * Now add bits of the last word, but only those before the item. We can
     * do that by applying a mask and then using popcount again. To get
     * 0-based index, we want to count only preceding bits, not the item
     * itself, so we subtract 1.
     */
    mask = ((1 as bitmapword) << bitnum) - 1;
    result += bmw_popcount(*words.add(wordnum as usize) & mask);

    result
}

/*
 * bms_overlap - do sets overlap (ie, have a nonempty intersection)?
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_overlap(a: *const Bitmapset, b: *const Bitmapset) -> bool {
    let shortlen: c_int;
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() || b.is_null() {
        return false;
    }
    /* Check words in common */
    shortlen = Min((*a).nwords, (*b).nwords);
    let awords = words_ptr(a);
    let bwords = words_ptr(b);
    i = 0;
    loop {
        if (*awords.add(i as usize) & *bwords.add(i as usize)) != 0 {
            return true;
        }
        i += 1;
        if !(i < shortlen) {
            break;
        }
    }
    false
}

/*
 * bms_overlap_list - does a set overlap an integer list?
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset; `b` must be NIL or a valid IntList.
 */
pub unsafe fn bms_overlap_list(a: *const Bitmapset, b: *const List) -> bool {
    // C declares `int wordnum, bitnum;` at function scope; they are only ever
    // assigned inside the loop body, so we declare them with `let` per-iteration.

    Assert!(bms_is_valid_set(a));

    if a.is_null() || b == NIL {
        return false;
    }

    let words = words_ptr(a);

    foreach!(lc, b, {
        let x: c_int = lfirst_int(current_cell!(lc));

        if x < 0 {
            elog!(ERROR, "negative bitmapset member not allowed");
        }
        let wordnum = WORDNUM(x);
        let bitnum = BITNUM(x);
        if wordnum < (*a).nwords {
            if (*words.add(wordnum as usize) & ((1 as bitmapword) << bitnum)) != 0 {
                return true;
            }
        }
    });

    false
}

/*
 * bms_nonempty_difference - do sets have a nonempty difference?
 *
 * i.e., are any members set in 'a' that are not also set in 'b'.
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_nonempty_difference(a: *const Bitmapset, b: *const Bitmapset) -> bool {
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        return false;
    }
    if b.is_null() {
        return true;
    }
    /* if 'a' has more words then it must contain additional members */
    if (*a).nwords > (*b).nwords {
        return true;
    }
    /* Check all 'a' members are set in 'b' */
    let awords = words_ptr(a);
    let bwords = words_ptr(b);
    i = 0;
    loop {
        if (*awords.add(i as usize) & !*bwords.add(i as usize)) != 0 {
            return true;
        }
        i += 1;
        if !(i < (*a).nwords) {
            break;
        }
    }
    false
}

/*
 * bms_singleton_member - return the sole integer member of set
 *
 * Raises error if |a| is not 1.
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_singleton_member(a: *const Bitmapset) -> c_int {
    let mut result: c_int = -1;
    let nwords: c_int;
    let mut wordnum: c_int;

    Assert!(bms_is_valid_set(a));

    if a.is_null() {
        elog!(ERROR, "bitmapset is empty");
    }

    let words = words_ptr(a);
    nwords = (*a).nwords;
    wordnum = 0;
    loop {
        let w: bitmapword = *words.add(wordnum as usize);

        if w != 0 {
            if result >= 0 || HAS_MULTIPLE_ONES(w) {
                elog!(ERROR, "bitmapset has multiple members");
            }
            result = wordnum * BITS_PER_BITMAPWORD;
            result += bmw_rightmost_one_pos(w);
        }
        wordnum += 1;
        if !(wordnum < nwords) {
            break;
        }
    }

    /* we don't expect non-NULL sets to be empty */
    Assert!(result >= 0);
    result
}

/*
 * bms_get_singleton_member
 *
 * Test whether the given set is a singleton.
 * If so, set *member to the value of its sole member, and return true.
 * If not, return false, without changing *member.
 *
 * This is more convenient and faster than calling bms_membership() and then
 * bms_singleton_member(), if we don't care about distinguishing empty sets
 * from multiple-member sets.
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset; `member` must be a valid writable ptr.
 */
pub unsafe fn bms_get_singleton_member(a: *const Bitmapset, member: *mut c_int) -> bool {
    let mut result: c_int = -1;
    let nwords: c_int;
    let mut wordnum: c_int;

    Assert!(bms_is_valid_set(a));

    if a.is_null() {
        return false;
    }

    let words = words_ptr(a);
    nwords = (*a).nwords;
    wordnum = 0;
    loop {
        let w: bitmapword = *words.add(wordnum as usize);

        if w != 0 {
            if result >= 0 || HAS_MULTIPLE_ONES(w) {
                return false;
            }
            result = wordnum * BITS_PER_BITMAPWORD;
            result += bmw_rightmost_one_pos(w);
        }
        wordnum += 1;
        if !(wordnum < nwords) {
            break;
        }
    }

    /* we don't expect non-NULL sets to be empty */
    Assert!(result >= 0);
    *member = result;
    true
}

/*
 * bms_num_members - count members of set
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_num_members(a: *const Bitmapset) -> c_int {
    let mut result: c_int = 0;
    let nwords: c_int;
    let mut wordnum: c_int;

    Assert!(bms_is_valid_set(a));

    if a.is_null() {
        return 0;
    }

    let words = words_ptr(a);
    nwords = (*a).nwords;
    wordnum = 0;
    loop {
        let w: bitmapword = *words.add(wordnum as usize);

        /* No need to count the bits in a zero word */
        if w != 0 {
            result += bmw_popcount(w);
        }
        wordnum += 1;
        if !(wordnum < nwords) {
            break;
        }
    }
    result
}

/*
 * bms_membership - does a set have zero, one, or multiple members?
 *
 * This is faster than making an exact count with bms_num_members().
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_membership(a: *const Bitmapset) -> BMS_Membership {
    let mut result: BMS_Membership = BMS_EMPTY_SET;
    let nwords: c_int;
    let mut wordnum: c_int;

    Assert!(bms_is_valid_set(a));

    if a.is_null() {
        return BMS_EMPTY_SET;
    }

    let words = words_ptr(a);
    nwords = (*a).nwords;
    wordnum = 0;
    loop {
        let w: bitmapword = *words.add(wordnum as usize);

        if w != 0 {
            if result != BMS_EMPTY_SET || HAS_MULTIPLE_ONES(w) {
                return BMS_MULTIPLE;
            }
            result = BMS_SINGLETON;
        }
        wordnum += 1;
        if !(wordnum < nwords) {
            break;
        }
    }
    result
}

/*
 * bms_add_member - add a specified member to set
 *
 * 'a' is recycled when possible.
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_add_member(mut a: *mut Bitmapset, x: c_int) -> *mut Bitmapset {
    let wordnum: c_int;
    let bitnum: c_int;

    Assert!(bms_is_valid_set(a));

    if x < 0 {
        elog!(ERROR, "negative bitmapset member not allowed");
    }
    if a.is_null() {
        return bms_make_singleton(x);
    }

    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);

    /* enlarge the set if necessary */
    if wordnum >= (*a).nwords {
        let oldnwords: c_int = (*a).nwords;
        let mut i: c_int;

        a = repalloc(a as *mut c_void, BITMAPSET_SIZE(wordnum + 1)) as *mut Bitmapset;
        (*a).nwords = wordnum + 1;
        /* zero out the enlarged portion */
        let words = words_ptr_mut(a);
        i = oldnwords;
        loop {
            *words.add(i as usize) = 0;
            i += 1;
            if !(i < (*a).nwords) {
                break;
            }
        }
    }

    *words_ptr_mut(a).add(wordnum as usize) |= (1 as bitmapword) << bitnum;

    // TODO(pg-port): under REALLOCATE_BITMAPSETS, `a = bms_copy_and_free(a)` here.

    a
}

/*
 * bms_del_member - remove a specified member from set
 *
 * No error if x is not currently a member of set
 *
 * 'a' is recycled when possible.
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_del_member(a: *mut Bitmapset, x: c_int) -> *mut Bitmapset {
    let wordnum: c_int;
    let bitnum: c_int;

    Assert!(bms_is_valid_set(a));

    if x < 0 {
        elog!(ERROR, "negative bitmapset member not allowed");
    }
    if a.is_null() {
        return core::ptr::null_mut();
    }

    wordnum = WORDNUM(x);
    bitnum = BITNUM(x);

    // TODO(pg-port): under REALLOCATE_BITMAPSETS, `a = bms_copy_and_free(a)` here.

    /* member can't exist.  Return 'a' unmodified */
    if unlikely(wordnum >= (*a).nwords) {
        return a;
    }

    let words = words_ptr_mut(a);
    *words.add(wordnum as usize) &= !((1 as bitmapword) << bitnum);

    /* when last word becomes empty, trim off all trailing empty words */
    if *words.add(wordnum as usize) == 0 && wordnum == (*a).nwords - 1 {
        /* find the last non-empty word and make that the new final word */
        let mut i: c_int = wordnum - 1;
        while i >= 0 {
            if *words.add(i as usize) != 0 {
                (*a).nwords = i + 1;
                return a;
            }
            i -= 1;
        }

        /* the set is now empty */
        pfree(a as *mut c_void);
        return core::ptr::null_mut();
    }
    a
}

/*
 * bms_add_members - like bms_union, but left input is recycled when possible
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_add_members(a: *mut Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    let result: *mut Bitmapset;
    let other: *const Bitmapset;
    let otherlen: c_int;
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        return bms_copy(b);
    }
    if b.is_null() {
        // TODO(pg-port): under REALLOCATE_BITMAPSETS, `a = bms_copy_and_free(a)`.
        return a;
    }
    /* Identify shorter and longer input; copy the longer one if needed */
    if (*a).nwords < (*b).nwords {
        result = bms_copy(b);
        other = a;
    } else {
        result = a;
        other = b;
    }
    /* And union the shorter input into the result */
    otherlen = (*other).nwords;
    let rwords = words_ptr_mut(result);
    let owords = words_ptr(other);
    i = 0;
    loop {
        *rwords.add(i as usize) |= *owords.add(i as usize);
        i += 1;
        if !(i < otherlen) {
            break;
        }
    }
    if result != a {
        pfree(a as *mut c_void);
    }
    // TODO(pg-port): under REALLOCATE_BITMAPSETS, else `result = bms_copy_and_free(result)`.

    result
}

/*
 * bms_replace_members
 *		Remove all existing members from 'a' and repopulate the set with members
 *		from 'b', recycling 'a', when possible.
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_replace_members(mut a: *mut Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    if a.is_null() {
        return bms_copy(b);
    }
    if b.is_null() {
        pfree(a as *mut c_void);
        return core::ptr::null_mut();
    }

    if (*a).nwords < (*b).nwords {
        a = repalloc(a as *mut c_void, BITMAPSET_SIZE((*b).nwords)) as *mut Bitmapset;
    }

    let awords = words_ptr_mut(a);
    let bwords = words_ptr(b);
    i = 0;
    loop {
        *awords.add(i as usize) = *bwords.add(i as usize);
        i += 1;
        if !(i < (*b).nwords) {
            break;
        }
    }

    (*a).nwords = (*b).nwords;

    // TODO(pg-port): under REALLOCATE_BITMAPSETS, `a = bms_copy_and_free(a)` here.

    a
}

/*
 * bms_add_range
 *		Add members in the range of 'lower' to 'upper' to the set.
 *
 * Note this could also be done by calling bms_add_member in a loop, however,
 * using this function will be faster when the range is large as we work at
 * the bitmapword level rather than at bit level.
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_add_range(mut a: *mut Bitmapset, lower: c_int, upper: c_int) -> *mut Bitmapset {
    let lwordnum: c_int;
    let lbitnum: c_int;
    let uwordnum: c_int;
    let ushiftbits: c_int;
    let mut wordnum: c_int;

    Assert!(bms_is_valid_set(a));

    /* do nothing if nothing is called for, without further checking */
    if upper < lower {
        // TODO(pg-port): under REALLOCATE_BITMAPSETS, `a = bms_copy_and_free(a)`.
        return a;
    }

    if lower < 0 {
        elog!(ERROR, "negative bitmapset member not allowed");
    }
    uwordnum = WORDNUM(upper);

    if a.is_null() {
        a = palloc0(BITMAPSET_SIZE(uwordnum + 1)) as *mut Bitmapset;
        (*a).r#type = T_Bitmapset;
        (*a).nwords = uwordnum + 1;
    } else if uwordnum >= (*a).nwords {
        let oldnwords: c_int = (*a).nwords;
        let mut i: c_int;

        /* ensure we have enough words to store the upper bit */
        a = repalloc(a as *mut c_void, BITMAPSET_SIZE(uwordnum + 1)) as *mut Bitmapset;
        (*a).nwords = uwordnum + 1;
        /* zero out the enlarged portion */
        let words = words_ptr_mut(a);
        i = oldnwords;
        loop {
            *words.add(i as usize) = 0;
            i += 1;
            if !(i < (*a).nwords) {
                break;
            }
        }
    }

    lwordnum = WORDNUM(lower);
    wordnum = lwordnum;

    lbitnum = BITNUM(lower);
    ushiftbits = BITS_PER_BITMAPWORD - (BITNUM(upper) + 1);

    let words = words_ptr_mut(a);

    /*
     * Special case when lwordnum is the same as uwordnum we must perform the
     * upper and lower masking on the word.
     */
    if lwordnum == uwordnum {
        *words.add(lwordnum as usize) |= !((((1 as bitmapword) << lbitnum) - 1) as bitmapword)
            & ((!(0 as bitmapword)) >> ushiftbits);
    } else {
        /* turn on lbitnum and all bits left of it */
        *words.add(wordnum as usize) |= !((((1 as bitmapword) << lbitnum) - 1) as bitmapword);
        wordnum += 1;

        /* turn on all bits for any intermediate words */
        while wordnum < uwordnum {
            *words.add(wordnum as usize) = !(0 as bitmapword);
            wordnum += 1;
        }

        /* turn on upper's bit and all bits right of it. */
        *words.add(uwordnum as usize) |= (!(0 as bitmapword)) >> ushiftbits;
    }

    // TODO(pg-port): under REALLOCATE_BITMAPSETS, `a = bms_copy_and_free(a)` here.

    a
}

/*
 * bms_int_members - like bms_intersect, but left input is recycled when
 * possible
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_int_members(a: *mut Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    let mut lastnonzero: c_int;
    let shortlen: c_int;
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        return core::ptr::null_mut();
    }
    if b.is_null() {
        pfree(a as *mut c_void);
        return core::ptr::null_mut();
    }

    /* Intersect b into a; we need never copy */
    shortlen = Min((*a).nwords, (*b).nwords);
    let awords = words_ptr_mut(a);
    let bwords = words_ptr(b);
    lastnonzero = -1;
    i = 0;
    loop {
        *awords.add(i as usize) &= *bwords.add(i as usize);

        if *awords.add(i as usize) != 0 {
            lastnonzero = i;
        }
        i += 1;
        if !(i < shortlen) {
            break;
        }
    }

    /* If we computed an empty result, we must return NULL */
    if lastnonzero == -1 {
        pfree(a as *mut c_void);
        return core::ptr::null_mut();
    }

    /* get rid of trailing zero words */
    (*a).nwords = lastnonzero + 1;

    // TODO(pg-port): under REALLOCATE_BITMAPSETS, `a = bms_copy_and_free(a)` here.

    a
}

/*
 * bms_del_members - delete members in 'a' that are set in 'b'.  'a' is
 * recycled when possible.
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_del_members(a: *mut Bitmapset, b: *const Bitmapset) -> *mut Bitmapset {
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        return core::ptr::null_mut();
    }
    if b.is_null() {
        // TODO(pg-port): under REALLOCATE_BITMAPSETS, `a = bms_copy_and_free(a)`.
        return a;
    }

    let awords = words_ptr_mut(a);
    let bwords = words_ptr(b);

    /* Remove b's bits from a; we need never copy */
    if (*a).nwords > (*b).nwords {
        /*
         * We'll never need to remove trailing zero words when 'a' has more
         * words than 'b'.
         */
        i = 0;
        loop {
            *awords.add(i as usize) &= !*bwords.add(i as usize);
            i += 1;
            if !(i < (*b).nwords) {
                break;
            }
        }
    } else {
        let mut lastnonzero: c_int = -1;

        /* we may need to remove trailing zero words from the result. */
        i = 0;
        loop {
            *awords.add(i as usize) &= !*bwords.add(i as usize);

            /* remember the last non-zero word */
            if *awords.add(i as usize) != 0 {
                lastnonzero = i;
            }
            i += 1;
            if !(i < (*a).nwords) {
                break;
            }
        }

        /* check if 'a' has become empty */
        if lastnonzero == -1 {
            pfree(a as *mut c_void);
            return core::ptr::null_mut();
        }

        /* trim off any trailing zero words */
        (*a).nwords = lastnonzero + 1;
    }

    // TODO(pg-port): under REALLOCATE_BITMAPSETS, `a = bms_copy_and_free(a)` here.

    a
}

/*
 * bms_join - like bms_union, but *either* input *may* be recycled
 *
 * # Safety
 * `a` and `b` must each be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_join(a: *mut Bitmapset, b: *mut Bitmapset) -> *mut Bitmapset {
    let result: *mut Bitmapset;
    let other: *mut Bitmapset;
    let otherlen: c_int;
    let mut i: c_int;

    Assert!(bms_is_valid_set(a));
    Assert!(bms_is_valid_set(b));

    /* Handle cases where either input is NULL */
    if a.is_null() {
        // TODO(pg-port): under REALLOCATE_BITMAPSETS, `b = bms_copy_and_free(b)`.
        return b;
    }
    if b.is_null() {
        // TODO(pg-port): under REALLOCATE_BITMAPSETS, `a = bms_copy_and_free(a)`.
        return a;
    }

    /* Identify shorter and longer input; use longer one as result */
    if (*a).nwords < (*b).nwords {
        result = b;
        other = a;
    } else {
        result = a;
        other = b;
    }
    /* And union the shorter input into the result */
    otherlen = (*other).nwords;
    let rwords = words_ptr_mut(result);
    let owords = words_ptr(other);
    i = 0;
    loop {
        *rwords.add(i as usize) |= *owords.add(i as usize);
        i += 1;
        if !(i < otherlen) {
            break;
        }
    }
    if other != result {
        /* pure paranoia */
        pfree(other as *mut c_void);
    }

    // TODO(pg-port): under REALLOCATE_BITMAPSETS, `result = bms_copy_and_free(result)`.

    result
}

/*
 * bms_next_member - find next member of a set
 *
 * Returns smallest member greater than "prevbit", or -2 if there is none.
 * "prevbit" must NOT be less than -1, or the behavior is unpredictable.
 *
 * This is intended as support for iterating through the members of a set.
 * The typical pattern is
 *
 *			x = -1;
 *			while ((x = bms_next_member(inputset, x)) >= 0)
 *				process member x;
 *
 * Notice that when there are no more members, we return -2, not -1 as you
 * might expect.  The rationale for that is to allow distinguishing the
 * loop-not-started state (x == -1) from the loop-completed state (x == -2).
 * It makes no difference in simple loop usage, but complex iteration logic
 * might need such an ability.
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_next_member(a: *const Bitmapset, mut prevbit: c_int) -> c_int {
    let nwords: c_int;
    let mut wordnum: c_int;
    let mut mask: bitmapword;

    Assert!(bms_is_valid_set(a));

    if a.is_null() {
        return -2;
    }
    nwords = (*a).nwords;
    prevbit += 1;
    mask = (!(0 as bitmapword)) << BITNUM(prevbit);
    let words = words_ptr(a);
    wordnum = WORDNUM(prevbit);
    while wordnum < nwords {
        let mut w: bitmapword = *words.add(wordnum as usize);

        /* ignore bits before prevbit */
        w &= mask;

        if w != 0 {
            let mut result: c_int;

            result = wordnum * BITS_PER_BITMAPWORD;
            result += bmw_rightmost_one_pos(w);
            return result;
        }

        /* in subsequent words, consider all bits */
        mask = !(0 as bitmapword);
        wordnum += 1;
    }
    -2
}

/*
 * bms_prev_member - find prev member of a set
 *
 * Returns largest member less than "prevbit", or -2 if there is none.
 * "prevbit" must NOT be more than one above the highest possible bit that can
 * be set at the Bitmapset at its current size.
 *
 * To ease finding the highest set bit for the initial loop, the special
 * prevbit value of -1 can be passed to have the function find the highest
 * valued member in the set.
 *
 * This is intended as support for iterating through the members of a set in
 * reverse.  The typical pattern is
 *
 *			x = -1;
 *			while ((x = bms_prev_member(inputset, x)) >= 0)
 *				process member x;
 *
 * Notice that when there are no more members, we return -2, not -1 as you
 * might expect.  The rationale for that is to allow distinguishing the
 * loop-not-started state (x == -1) from the loop-completed state (x == -2).
 * It makes no difference in simple loop usage, but complex iteration logic
 * might need such an ability.
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_prev_member(a: *const Bitmapset, mut prevbit: c_int) -> c_int {
    let mut wordnum: c_int;
    let ushiftbits: c_int;
    let mut mask: bitmapword;

    Assert!(bms_is_valid_set(a));

    /*
     * If set is NULL or if there are no more bits to the right then we've
     * nothing to do.
     */
    if a.is_null() || prevbit == 0 {
        return -2;
    }

    /* transform -1 to the highest possible bit we could have set */
    if prevbit == -1 {
        prevbit = (*a).nwords * BITS_PER_BITMAPWORD - 1;
    } else {
        prevbit -= 1;
    }

    ushiftbits = BITS_PER_BITMAPWORD - (BITNUM(prevbit) + 1);
    mask = (!(0 as bitmapword)) >> ushiftbits;
    let words = words_ptr(a);
    wordnum = WORDNUM(prevbit);
    while wordnum >= 0 {
        let mut w: bitmapword = *words.add(wordnum as usize);

        /* mask out bits left of prevbit */
        w &= mask;

        if w != 0 {
            let mut result: c_int;

            result = wordnum * BITS_PER_BITMAPWORD;
            result += bmw_leftmost_one_pos(w);
            return result;
        }

        /* in subsequent words, consider all bits */
        mask = !(0 as bitmapword);
        wordnum -= 1;
    }
    -2
}

/*
 * bms_hash_value - compute a hash key for a Bitmapset
 *
 * # Safety
 * `a` must be NULL or a valid Bitmapset.
 */
pub unsafe fn bms_hash_value(a: *const Bitmapset) -> uint32 {
    Assert!(bms_is_valid_set(a));

    if a.is_null() {
        return 0; /* All empty sets hash to 0 */
    }
    DatumGetUInt32(hash_any(
        words_ptr(a) as *const core::ffi::c_uchar,
        (*a).nwords * core::mem::size_of::<bitmapword>() as c_int,
    ))
}

/*
 * bitmap_hash - hash function for keys that are (pointers to) Bitmapsets
 *
 * Note: don't forget to specify bitmap_match as the match function!
 *
 * # Safety
 * `key` must point to a `*const Bitmapset`.
 */
pub unsafe extern "C" fn bitmap_hash(key: *const c_void, keysize: Size) -> uint32 {
    Assert!(keysize == core::mem::size_of::<*const Bitmapset>());
    bms_hash_value(*(key as *const *const Bitmapset))
}

/*
 * bitmap_match - match function to use with bitmap_hash
 *
 * # Safety
 * `key1` and `key2` must each point to a `*const Bitmapset`.
 */
pub unsafe extern "C" fn bitmap_match(key1: *const c_void, key2: *const c_void, keysize: Size) -> c_int {
    Assert!(keysize == core::mem::size_of::<*const Bitmapset>());
    !bms_equal(
        *(key1 as *const *const Bitmapset),
        *(key2 as *const *const Bitmapset),
    ) as c_int
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_member_union_intersect_iterate() {
        unsafe {
            let mut a: *mut Bitmapset = core::ptr::null_mut(); // NULL == empty set
            a = bms_add_member(a, 3);
            a = bms_add_member(a, 70); // forces growth past one 64-bit word
            a = bms_add_member(a, 5);
            assert!(bms_is_member(3, a));
            assert!(bms_is_member(70, a));
            assert!(!bms_is_member(4, a));
            assert_eq!(bms_num_members(a), 3);

            // iterate in ascending order via the -1-seeded cursor
            let mut got = Vec::new();
            let mut bit = bms_next_member(a, -1);
            while bit >= 0 {
                got.push(bit);
                bit = bms_next_member(a, bit);
            }
            assert_eq!(got, vec![3, 5, 70]);

            let b = bms_make_singleton(70);
            let u = bms_union(a, b);
            assert_eq!(bms_num_members(u), 3); // 70 already present
            let i = bms_intersect(a, b);
            assert_eq!(bms_num_members(i), 1);
            assert!(bms_is_member(70, i));

            bms_free(a);
            bms_free(b);
            bms_free(u);
            bms_free(i);
        }
    }
}

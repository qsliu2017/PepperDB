//! Translated from PostgreSQL src/include/nodes/bitmapset.h

/// 64-bit words (target is 64-bit only). C: `bitmapword`.
pub type Bitmapword = u64;
pub type SignedBitmapword = i64;
pub const BITS_PER_BITMAPWORD: i32 = 64;

/// A set of nonnegative integers. C represents the empty set as a NULL pointer;
/// here an empty `Vec`/`Bitmapset::default()` is the empty set (`bms_is_empty`).
// In-memory container: newtype over Vec<u64> per the container table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Bitmapset {
    pub words: Vec<Bitmapword>,
}

/// C: result of `bms_subset_compare`.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BMS_Comparison {
    EQUAL = 0,
    SUBSET1,
    SUBSET2,
    DIFFERENT,
}

/// C: result of `bms_membership`.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BMS_Membership {
    EMPTY_SET = 0,
    SINGLETON,
    MULTIPLE,
}

impl Bitmapset {
    /// C: `bms_is_empty(a)` -- the empty set (C NULL).
    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|&w| w == 0)
    }
}

pub fn bms_copy(a: &Bitmapset) -> Bitmapset {
    a.clone()
}
pub fn bms_equal(a: &Bitmapset, b: &Bitmapset) -> bool {
    a == b
}
pub fn bms_compare(_a: &Bitmapset, _b: &Bitmapset) -> i32 {
    unimplemented!()
}
pub fn bms_make_singleton(_x: i32) -> Bitmapset {
    unimplemented!()
}
// Freeing is RAII (Drop); kept for parity.
pub fn bms_free(_a: Bitmapset) {}

pub fn bms_union(_a: &Bitmapset, _b: &Bitmapset) -> Bitmapset {
    unimplemented!()
}
pub fn bms_intersect(_a: &Bitmapset, _b: &Bitmapset) -> Bitmapset {
    unimplemented!()
}
pub fn bms_difference(_a: &Bitmapset, _b: &Bitmapset) -> Bitmapset {
    unimplemented!()
}
pub fn bms_is_subset(_a: &Bitmapset, _b: &Bitmapset) -> bool {
    unimplemented!()
}
pub fn bms_subset_compare(_a: &Bitmapset, _b: &Bitmapset) -> BMS_Comparison {
    unimplemented!()
}
pub fn bms_is_member(_x: i32, _a: &Bitmapset) -> bool {
    unimplemented!()
}
pub fn bms_member_index(_a: &Bitmapset, _x: i32) -> i32 {
    unimplemented!()
}
pub fn bms_overlap(_a: &Bitmapset, _b: &Bitmapset) -> bool {
    unimplemented!()
}
/// C: `bms_overlap_list(a, b)` where `b` is an IntList -> `Vec<i32>` (`&[i32]`).
pub fn bms_overlap_list(_a: &Bitmapset, _b: &[i32]) -> bool {
    unimplemented!()
}
pub fn bms_nonempty_difference(_a: &Bitmapset, _b: &Bitmapset) -> bool {
    unimplemented!()
}
pub fn bms_singleton_member(_a: &Bitmapset) -> i32 {
    unimplemented!()
}
/// C: `bms_get_singleton_member(a, *member)` -- bool + out-param -> `Option<i32>`.
pub fn bms_get_singleton_member(_a: &Bitmapset) -> Option<i32> {
    unimplemented!()
}
pub fn bms_num_members(_a: &Bitmapset) -> i32 {
    unimplemented!()
}
pub fn bms_membership(_a: &Bitmapset) -> BMS_Membership {
    unimplemented!()
}

/* these routines recycle (modify or free) their non-const inputs */

pub fn bms_add_member(_a: Bitmapset, _x: i32) -> Bitmapset {
    unimplemented!()
}
pub fn bms_del_member(_a: Bitmapset, _x: i32) -> Bitmapset {
    unimplemented!()
}
pub fn bms_add_members(_a: Bitmapset, _b: &Bitmapset) -> Bitmapset {
    unimplemented!()
}
pub fn bms_replace_members(_a: Bitmapset, _b: &Bitmapset) -> Bitmapset {
    unimplemented!()
}
pub fn bms_add_range(_a: Bitmapset, _lower: i32, _upper: i32) -> Bitmapset {
    unimplemented!()
}
pub fn bms_int_members(_a: Bitmapset, _b: &Bitmapset) -> Bitmapset {
    unimplemented!()
}
pub fn bms_del_members(_a: Bitmapset, _b: &Bitmapset) -> Bitmapset {
    unimplemented!()
}
pub fn bms_join(_a: Bitmapset, _b: Bitmapset) -> Bitmapset {
    unimplemented!()
}

/* iteration: C returns -2 when exhausted -> Option<i32> */

pub fn bms_next_member(_a: &Bitmapset, _prevbit: i32) -> Option<i32> {
    unimplemented!()
}
pub fn bms_prev_member(_a: &Bitmapset, _prevbit: i32) -> Option<i32> {
    unimplemented!()
}

/* hashtable support */

pub fn bms_hash_value(_a: &Bitmapset) -> u32 {
    unimplemented!()
}
pub fn bitmap_hash(_key: &Bitmapset) -> u32 {
    unimplemented!()
}
pub fn bitmap_match(_key1: &Bitmapset, _key2: &Bitmapset) -> i32 {
    unimplemented!()
}

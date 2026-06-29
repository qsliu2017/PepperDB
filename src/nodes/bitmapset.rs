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

    /// Drop trailing all-zero words so the canonical empty set has no words and
    /// `PartialEq`/`==` matches `bms_equal` (which ignores trailing zero words in C
    /// because C never keeps them). Used by every mutating op below.
    fn normalize(&mut self) {
        while self.words.last() == Some(&0) {
            self.words.pop();
        }
    }
}

/// (wordnum, bitnum-within-word) for a nonnegative set member.
fn word_bit(x: i32) -> (usize, u32) {
    crate::assert!(x >= 0);
    let x = x as usize;
    (x / BITS_PER_BITMAPWORD as usize, (x % BITS_PER_BITMAPWORD as usize) as u32)
}

pub fn bms_copy(a: &Bitmapset) -> Bitmapset {
    a.clone()
}
pub fn bms_equal(a: &Bitmapset, b: &Bitmapset) -> bool {
    // Tolerate trailing zero words on either side (C-equivalent comparison).
    let n = a.words.len().max(b.words.len());
    (0..n).all(|i| a.words.get(i).copied().unwrap_or(0) == b.words.get(i).copied().unwrap_or(0))
}
pub fn bms_compare(a: &Bitmapset, b: &Bitmapset) -> i32 {
    // C compares by highest set bit first, then lexicographically high->low.
    let na = bms_num_members(a);
    let nb = bms_num_members(b);
    if na != nb {
        return if na < nb { -1 } else { 1 };
    }
    let n = a.words.len().max(b.words.len());
    for i in (0..n).rev() {
        let aw = a.words.get(i).copied().unwrap_or(0);
        let bw = b.words.get(i).copied().unwrap_or(0);
        if aw != bw {
            return if aw < bw { -1 } else { 1 };
        }
    }
    0
}
pub fn bms_make_singleton(x: i32) -> Bitmapset {
    bms_add_member(Bitmapset::default(), x)
}
// Freeing is RAII (Drop); kept for parity.
pub fn bms_free(_a: Bitmapset) {}

pub fn bms_union(a: &Bitmapset, b: &Bitmapset) -> Bitmapset {
    bms_add_members(a.clone(), b)
}
pub fn bms_intersect(a: &Bitmapset, b: &Bitmapset) -> Bitmapset {
    bms_int_members(a.clone(), b)
}
pub fn bms_difference(a: &Bitmapset, b: &Bitmapset) -> Bitmapset {
    bms_del_members(a.clone(), b)
}
pub fn bms_is_subset(a: &Bitmapset, b: &Bitmapset) -> bool {
    // Every bit of a is also in b.
    a.words
        .iter()
        .enumerate()
        .all(|(i, &aw)| aw & !b.words.get(i).copied().unwrap_or(0) == 0)
}
pub fn bms_subset_compare(a: &Bitmapset, b: &Bitmapset) -> BMS_Comparison {
    let a_sub_b = bms_is_subset(a, b);
    let b_sub_a = bms_is_subset(b, a);
    match (a_sub_b, b_sub_a) {
        (true, true) => BMS_Comparison::EQUAL,
        (true, false) => BMS_Comparison::SUBSET1,
        (false, true) => BMS_Comparison::SUBSET2,
        (false, false) => BMS_Comparison::DIFFERENT,
    }
}
pub fn bms_is_member(x: i32, a: &Bitmapset) -> bool {
    if x < 0 {
        return false;
    }
    let (w, b) = word_bit(x);
    a.words.get(w).is_some_and(|&word| word & (1 << b) != 0)
}
pub fn bms_member_index(a: &Bitmapset, x: i32) -> i32 {
    // Number of set members less than x; -1 if x is not a member.
    if !bms_is_member(x, a) {
        return -1;
    }
    let mut idx = 0;
    let mut cur = -1;
    while let Some(m) = bms_next_member(a, cur) {
        if m == x {
            return idx;
        }
        idx += 1;
        cur = m;
    }
    -1
}
pub fn bms_overlap(a: &Bitmapset, b: &Bitmapset) -> bool {
    a.words
        .iter()
        .enumerate()
        .any(|(i, &aw)| aw & b.words.get(i).copied().unwrap_or(0) != 0)
}
/// C: `bms_overlap_list(a, b)` where `b` is an IntList -> `Vec<i32>` (`&[i32]`).
pub fn bms_overlap_list(a: &Bitmapset, b: &[i32]) -> bool {
    b.iter().any(|&x| bms_is_member(x, a))
}
pub fn bms_nonempty_difference(a: &Bitmapset, b: &Bitmapset) -> bool {
    a.words
        .iter()
        .enumerate()
        .any(|(i, &aw)| aw & !b.words.get(i).copied().unwrap_or(0) != 0)
}
pub fn bms_singleton_member(a: &Bitmapset) -> i32 {
    if let Some(m) = bms_get_singleton_member(a) {
        return m;
    }
    crate::elog!(crate::utils::elog::ERROR, "bitmapset is not a singleton");
    -1 // keep compiler quiet (elog(ERROR) does not return)
}
/// C: `bms_get_singleton_member(a, *member)` -- bool + out-param -> `Option<i32>`.
pub fn bms_get_singleton_member(a: &Bitmapset) -> Option<i32> {
    let mut found = None;
    let mut cur = -1;
    while let Some(m) = bms_next_member(a, cur) {
        if found.is_some() {
            return None;
        }
        found = Some(m);
        cur = m;
    }
    found
}
pub fn bms_num_members(a: &Bitmapset) -> i32 {
    a.words.iter().map(|w| w.count_ones() as i32).sum()
}
pub fn bms_membership(a: &Bitmapset) -> BMS_Membership {
    match bms_num_members(a) {
        0 => BMS_Membership::EMPTY_SET,
        1 => BMS_Membership::SINGLETON,
        _ => BMS_Membership::MULTIPLE,
    }
}

/* these routines recycle (modify or free) their non-const inputs */

pub fn bms_add_member(mut a: Bitmapset, x: i32) -> Bitmapset {
    crate::assert!(x >= 0);
    let (w, b) = word_bit(x);
    if a.words.len() <= w {
        a.words.resize(w + 1, 0);
    }
    a.words[w] |= 1 << b;
    a
}
pub fn bms_del_member(mut a: Bitmapset, x: i32) -> Bitmapset {
    if x >= 0 {
        let (w, b) = word_bit(x);
        if let Some(word) = a.words.get_mut(w) {
            *word &= !(1 << b);
        }
        a.normalize();
    }
    a
}
pub fn bms_add_members(mut a: Bitmapset, b: &Bitmapset) -> Bitmapset {
    if a.words.len() < b.words.len() {
        a.words.resize(b.words.len(), 0);
    }
    for (i, &bw) in b.words.iter().enumerate() {
        a.words[i] |= bw;
    }
    a
}
pub fn bms_replace_members(mut a: Bitmapset, b: &Bitmapset) -> Bitmapset {
    a.words.clear();
    a.words.extend_from_slice(&b.words);
    a.normalize();
    a
}
pub fn bms_add_range(mut a: Bitmapset, lower: i32, upper: i32) -> Bitmapset {
    if upper >= lower {
        for x in lower..=upper {
            a = bms_add_member(a, x);
        }
    }
    a
}
pub fn bms_int_members(mut a: Bitmapset, b: &Bitmapset) -> Bitmapset {
    for i in 0..a.words.len() {
        a.words[i] &= b.words.get(i).copied().unwrap_or(0);
    }
    a.normalize();
    a
}
pub fn bms_del_members(mut a: Bitmapset, b: &Bitmapset) -> Bitmapset {
    for i in 0..a.words.len() {
        a.words[i] &= !b.words.get(i).copied().unwrap_or(0);
    }
    a.normalize();
    a
}
#[allow(clippy::needless_pass_by_value, reason = "1:1 PG port: bms_join recycles (consumes) both inputs per the C contract")]
pub fn bms_join(a: Bitmapset, b: Bitmapset) -> Bitmapset {
    bms_add_members(a, &b)
}

/* iteration: C returns -2 when exhausted -> Option<i32> */

/// C: `bms_next_member(a, prevbit)` -- next set member strictly greater than
/// `prevbit`. Pass `-1` to start; `None` when exhausted.
pub fn bms_next_member(a: &Bitmapset, prevbit: i32) -> Option<i32> {
    let start = prevbit + 1;
    if start < 0 {
        return None;
    }
    let (mut w, b) = word_bit(start);
    // Mask off bits below `start` in the first word.
    let mut mask = !0u64 << b;
    while w < a.words.len() {
        let word = a.words[w] & mask;
        if word != 0 {
            let bit = word.trailing_zeros();
            return Some((w * BITS_PER_BITMAPWORD as usize) as i32 + bit as i32);
        }
        w += 1;
        mask = !0u64;
    }
    None
}
/// C: `bms_prev_member(a, prevbit)` -- previous set member strictly less than
/// `prevbit`. Pass a value past the top (or any large int) to start.
pub fn bms_prev_member(a: &Bitmapset, prevbit: i32) -> Option<i32> {
    if a.words.is_empty() || prevbit == 0 {
        return None;
    }
    let max_bit = (a.words.len() * BITS_PER_BITMAPWORD as usize) as i32 - 1;
    let start = if prevbit < 0 || prevbit - 1 > max_bit { max_bit } else { prevbit - 1 };
    if start < 0 {
        return None;
    }
    let (sw, sb) = word_bit(start);
    let mut w = sw as isize;
    // Mask off bits above `start` in the first word.
    let mut mask = if sb == 63 { !0u64 } else { (1u64 << (sb + 1)) - 1 };
    while w >= 0 {
        let word = a.words[w as usize] & mask;
        if word != 0 {
            let bit = word.ilog2();
            return Some((w as usize * BITS_PER_BITMAPWORD as usize) as i32 + bit as i32);
        }
        w -= 1;
        mask = !0u64;
    }
    None
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

#[cfg(test)]
mod tests {
    use super::*;

    fn set(members: &[i32]) -> Bitmapset {
        members.iter().fold(Bitmapset::default(), |a, &x| bms_add_member(a, x))
    }

    #[test]
    fn add_member_is_member() {
        let a = set(&[1, 2, 64, 200]);
        assert!(bms_is_member(1, &a));
        assert!(bms_is_member(64, &a));
        assert!(bms_is_member(200, &a));
        assert!(!bms_is_member(3, &a));
        assert!(!bms_is_member(-1, &a));
    }

    #[test]
    fn num_members_and_membership() {
        assert_eq!(bms_num_members(&set(&[])), 0);
        assert_eq!(bms_num_members(&set(&[5, 5, 5])), 1);
        assert_eq!(bms_num_members(&set(&[1, 2, 70])), 3);
        assert_eq!(bms_membership(&set(&[])), BMS_Membership::EMPTY_SET);
        assert_eq!(bms_membership(&set(&[9])), BMS_Membership::SINGLETON);
        assert_eq!(bms_membership(&set(&[9, 10])), BMS_Membership::MULTIPLE);
    }

    #[test]
    fn union_intersect_difference() {
        let a = set(&[1, 2, 3]);
        let b = set(&[2, 3, 4]);
        assert!(bms_equal(&bms_union(&a, &b), &set(&[1, 2, 3, 4])));
        assert!(bms_equal(&bms_intersect(&a, &b), &set(&[2, 3])));
        assert!(bms_equal(&bms_difference(&a, &b), &set(&[1])));
    }

    #[test]
    fn subset_and_overlap() {
        let a = set(&[1, 2]);
        let b = set(&[1, 2, 3]);
        assert!(bms_is_subset(&a, &b));
        assert!(!bms_is_subset(&b, &a));
        assert!(bms_overlap(&a, &b));
        assert!(!bms_overlap(&set(&[1]), &set(&[2])));
        assert_eq!(bms_subset_compare(&a, &b), BMS_Comparison::SUBSET1);
        assert_eq!(bms_subset_compare(&b, &a), BMS_Comparison::SUBSET2);
        assert_eq!(bms_subset_compare(&a, &a), BMS_Comparison::EQUAL);
        assert_eq!(bms_subset_compare(&set(&[1]), &set(&[2])), BMS_Comparison::DIFFERENT);
    }

    #[test]
    fn next_member_iteration() {
        let a = set(&[2, 5, 64, 130]);
        let mut got = Vec::new();
        let mut cur = -1;
        while let Some(m) = bms_next_member(&a, cur) {
            got.push(m);
            cur = m;
        }
        assert_eq!(got, vec![2, 5, 64, 130]);
        assert_eq!(bms_next_member(&Bitmapset::default(), -1), None);
    }

    #[test]
    fn prev_member_iteration() {
        let a = set(&[2, 5, 64, 130]);
        let mut got = Vec::new();
        let mut cur = -1; // start past the top
        while let Some(m) = bms_prev_member(&a, cur) {
            got.push(m);
            cur = m;
        }
        assert_eq!(got, vec![130, 64, 5, 2]);
    }

    #[test]
    fn singleton_and_member_index() {
        assert_eq!(bms_get_singleton_member(&set(&[42])), Some(42));
        assert_eq!(bms_get_singleton_member(&set(&[1, 2])), None);
        assert_eq!(bms_get_singleton_member(&set(&[])), None);
        let a = set(&[3, 7, 9]);
        assert_eq!(bms_member_index(&a, 3), 0);
        assert_eq!(bms_member_index(&a, 7), 1);
        assert_eq!(bms_member_index(&a, 9), 2);
        assert_eq!(bms_member_index(&a, 5), -1);
    }

    #[test]
    fn del_member_normalizes_empty() {
        let a = bms_del_member(set(&[5]), 5);
        assert!(a.is_empty());
        assert!(bms_equal(&a, &Bitmapset::default()));
        assert_eq!(a.words.len(), 0);
    }

    #[test]
    fn add_range_and_singleton() {
        assert!(bms_equal(&bms_add_range(Bitmapset::default(), 2, 5), &set(&[2, 3, 4, 5])));
        assert!(bms_equal(&bms_make_singleton(7), &set(&[7])));
    }
}

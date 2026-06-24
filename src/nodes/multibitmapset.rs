//! Translated from PostgreSQL src/include/nodes/multibitmapset.h

// A multibitmapset is a List of Bitmapsets: members are identified by two small
// integers (listidx, bitidx). C represents it as `List *`; here `Vec<Bitmapset>`
// (the empty set is an empty Vec). Not a node.

use crate::nodes::bitmapset::Bitmapset;

pub type MultiBitmapset = Vec<Bitmapset>;

pub fn mbms_add_member(a: MultiBitmapset, listidx: i32, bitidx: i32) -> MultiBitmapset {
    let _ = (a, listidx, bitidx);
    unimplemented!()
}

pub fn mbms_add_members(a: MultiBitmapset, b: &[Bitmapset]) -> MultiBitmapset {
    let _ = (a, b);
    unimplemented!()
}

pub fn mbms_int_members(a: MultiBitmapset, b: &[Bitmapset]) -> MultiBitmapset {
    let _ = (a, b);
    unimplemented!()
}

pub fn mbms_is_member(listidx: i32, bitidx: i32, a: &[Bitmapset]) -> bool {
    let _ = (listidx, bitidx, a);
    unimplemented!()
}

pub fn mbms_overlap_sets(a: &[Bitmapset], b: &[Bitmapset]) -> Bitmapset {
    let _ = (a, b);
    unimplemented!()
}

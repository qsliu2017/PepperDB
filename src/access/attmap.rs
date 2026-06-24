//! Translated from PostgreSQL src/include/access/attmap.h
//! Definitions for PostgreSQL attribute mappings

use crate::access::attnum::AttrNumber;
use crate::access::tupdesc::TupleDesc;

/// Attribute mapping structure.
///
/// Maps attribute numbers between a pair of relations ('input' and 'output').
/// In-memory: the C `AttrNumber *attnums` + `int maplen` collapses to a `Vec`;
/// `maplen` is `attnums.len()`. Dropped/absent columns are stored as 0.
pub struct AttrMap {
    pub attnums: Vec<AttrNumber>,
}

impl AttrMap {
    pub fn maplen(&self) -> usize {
        self.attnums.len()
    }
}

pub fn make_attrmap(_maplen: i32) -> AttrMap {
    unimplemented!()
}

// free_attrmap: dropping the AttrMap reclaims it under Rust ownership.

/// Conversion routines to build mappings. TupleDesc is itself a pointer alias.
pub fn build_attrmap_by_name(
    _indesc: TupleDesc,
    _outdesc: TupleDesc,
    _missing_ok: bool,
) -> AttrMap {
    unimplemented!()
}

/// Returns None when no mapping is required (C returns NULL).
pub fn build_attrmap_by_name_if_req(
    _indesc: TupleDesc,
    _outdesc: TupleDesc,
    _missing_ok: bool,
) -> Option<AttrMap> {
    unimplemented!()
}

pub fn build_attrmap_by_position(
    _indesc: TupleDesc,
    _outdesc: TupleDesc,
    _msg: &str,
) -> AttrMap {
    unimplemented!()
}

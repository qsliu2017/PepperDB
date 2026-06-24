//! Translated from PostgreSQL src/include/access/tupdesc.h
//! POSTGRES tuple descriptor definitions. In-memory (no layout contract).

use crate::access::attnum::AttrNumber;
use crate::catalog::pg_attribute::{Form_pg_attribute, FormData_pg_attribute};
use crate::nodes::nodes::Node;
use crate::postgres_ext::Oid;

// AttrMissing's full definition lives in access/tupdesc_details.h (level 5).
// Rule 7: opaque local placeholder, repointed in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::access::tupdesc_details::AttrMissing in Phase 2")]
pub struct AttrMissing {
    _private: (),
}

pub struct AttrDefault {
    pub adnum: AttrNumber,
    pub adbin: String, // nodeToString representation of expr
}

pub struct ConstrCheck {
    pub ccname: String,
    pub ccbin: String, // nodeToString representation of expr
    pub ccenforced: bool,
    pub ccvalid: bool,
    pub ccnoinherit: bool, // non-inheritable constraint
}

/// Constraints of a tuple.
#[allow(deprecated)]
pub struct TupleConstr {
    pub defval: Vec<AttrDefault>,
    pub check: Vec<ConstrCheck>,
    pub missing: Option<Vec<AttrMissing>>, // missing attribute values, None if none
    pub has_not_null: bool,                // any not-null, including not-valid ones
    pub has_generated_stored: bool,
    pub has_generated_virtual: bool,
}

/// Cut-down version of FormData_pg_attribute for fast tuple deformation.
/// In-memory cache; populated from FormData_pg_attribute.
pub struct CompactAttribute {
    pub attcacheoff: i32, // fixed offset into tuple, or -1
    pub attlen: i16,      // attr len in bytes, -1 = varlen, -2 = cstring
    pub attbyval: bool,
    pub attispackable: bool,
    pub atthasmissing: bool,
    pub attisdropped: bool,
    pub attgenerated: bool,
    pub attnullability: u8, // ATTNULLABLE_* below
    pub attalignby: u8,     // alignment requirement in bytes
}

// Valid values for CompactAttribute::attnullability.
pub const ATTNULLABLE_UNRESTRICTED: u8 = b'f'; // no constraint exists
pub const ATTNULLABLE_UNKNOWN: u8 = b'u'; // constraint exists, validity unknown
pub const ATTNULLABLE_VALID: u8 = b'v'; // valid constraint exists
pub const ATTNULLABLE_INVALID: u8 = b'i'; // constraint exists, marked invalid

/// Describes the structure of tuples. In-memory: the two C flexible-array tails
/// (compact_attrs[] then a FormData_pg_attribute[]) become owned Vecs; the C
/// pointer-based TupleDescAttr/TupleDescCompactAttr accessors become slice
/// indexing. Reference counting (tdrefcount) is retained for relcache/typcache
/// descriptors (-1 means not counted).
pub struct TupleDescData {
    pub natts: i32,        // number of attributes
    pub tdtypeid: Oid,     // composite type ID for tuple type
    pub tdtypmod: i32,     // typmod for tuple type
    pub tdrefcount: i32,   // reference count, or -1 if not counting
    pub constr: Option<Box<TupleConstr>>, // constraints, None if none
    pub compact_attrs: Vec<CompactAttribute>,
    pub attrs: Vec<FormData_pg_attribute>,
}

/// TupleDesc is a handle to a TupleDescData (C uses a bare pointer).
pub type TupleDesc = *mut TupleDescData; // TODO(ptr): likely Arc/Rc in Phase 2

impl TupleDescData {
    /// Accessor for the i'th FormData_pg_attribute (C TupleDescAttr).
    pub fn attr(&self, i: usize) -> &FormData_pg_attribute {
        &self.attrs[i]
    }

    /// Accessor for the i'th CompactAttribute (C TupleDescCompactAttr).
    pub fn compact_attr(&self, i: usize) -> &CompactAttribute {
        &self.compact_attrs[i]
    }
}

pub fn populate_compact_attribute(_tupdesc: TupleDesc, _attnum: i32) {
    unimplemented!()
}

pub fn verify_compact_attribute(_tupdesc: TupleDesc, _attnum: i32) {
    unimplemented!()
}

pub fn CreateTemplateTupleDesc(_natts: i32) -> TupleDesc {
    unimplemented!()
}

pub fn CreateTupleDesc(_natts: i32, _attrs: &[Form_pg_attribute]) -> TupleDesc {
    unimplemented!()
}

pub fn CreateTupleDescCopy(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!()
}

pub fn CreateTupleDescTruncatedCopy(_tupdesc: TupleDesc, _natts: i32) -> TupleDesc {
    unimplemented!()
}

pub fn CreateTupleDescCopyConstr(_tupdesc: TupleDesc) -> TupleDesc {
    unimplemented!()
}

pub fn TupleDescCopy(_dst: TupleDesc, _src: TupleDesc) {
    unimplemented!()
}

pub fn TupleDescCopyEntry(
    _dst: TupleDesc,
    _dstAttno: AttrNumber,
    _src: TupleDesc,
    _srcAttno: AttrNumber,
) {
    unimplemented!()
}

pub fn FreeTupleDesc(_tupdesc: TupleDesc) {
    unimplemented!()
}

pub fn IncrTupleDescRefCount(_tupdesc: TupleDesc) {
    unimplemented!()
}

pub fn DecrTupleDescRefCount(_tupdesc: TupleDesc) {
    unimplemented!()
}

/// C PinTupleDesc: increments the refcount only if the descriptor is counted.
pub fn PinTupleDesc(tupdesc: &mut TupleDescData) {
    if tupdesc.tdrefcount >= 0 {
        tupdesc.tdrefcount += 1;
    }
}

/// C ReleaseTupleDesc: decrements the refcount only if the descriptor is counted.
pub fn ReleaseTupleDesc(tupdesc: &mut TupleDescData) {
    if tupdesc.tdrefcount >= 0 {
        tupdesc.tdrefcount -= 1;
    }
}

pub fn equalTupleDescs(_tupdesc1: TupleDesc, _tupdesc2: TupleDesc) -> bool {
    unimplemented!()
}

pub fn equalRowTypes(_tupdesc1: TupleDesc, _tupdesc2: TupleDesc) -> bool {
    unimplemented!()
}

pub fn hashRowType(_desc: TupleDesc) -> u32 {
    unimplemented!()
}

pub fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _attributeName: &str,
    _oidtypeid: Oid,
    _typmod: i32,
    _attdim: i32,
) {
    unimplemented!()
}

pub fn TupleDescInitBuiltinEntry(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _attributeName: &str,
    _oidtypeid: Oid,
    _typmod: i32,
    _attdim: i32,
) {
    unimplemented!()
}

pub fn TupleDescInitEntryCollation(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _collationid: Oid,
) {
    unimplemented!()
}

pub fn BuildDescFromLists(
    _names: &[String],
    _types: &[Oid],
    _typmods: &[i32],
    _collations: &[Oid],
) -> TupleDesc {
    unimplemented!()
}

pub fn TupleDescGetDefault(_tupdesc: TupleDesc, _attnum: AttrNumber) -> Node {
    unimplemented!()
}

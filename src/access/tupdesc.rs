//! Translated from PostgreSQL src/include/access/tupdesc.h
//! POSTGRES tuple descriptor definitions. In-memory (no layout contract).

use std::sync::Arc;

use crate::access::attnum::AttrNumber;
use crate::access::tupdesc_details::AttrMissing;
use crate::catalog::pg_attribute::FormData_pg_attribute;
use crate::nodes::nodes::Node;
use crate::postgres_ext::Oid;

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
#[derive(Clone, PartialEq, Eq)]
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

impl CompactAttribute {
    /// An empty compact attribute (all-zero except `attcacheoff = -1`).
    #[must_use]
    pub fn new() -> Self {
        Self {
            attcacheoff: -1,
            attlen: 0,
            attbyval: false,
            attispackable: false,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby: 0,
        }
    }
}

impl Default for CompactAttribute {
    fn default() -> Self {
        Self::new()
    }
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

/// Handle to a `TupleDescData`: a shared, reference-counted owner. C uses a bare,
/// freely-copied pointer manually reference-counted via `tdrefcount`; here that
/// becomes an `Arc`, which allows the multiple simultaneous co-owners the result
/// descriptor needs (`Portal` -> `QueryDesc` -> `estate`/`planstate`/`dest`/slot)
/// and gives pointer-identity via [`Arc::ptr_eq`] (used by the printtup receiver
/// to detect a mid-stream descriptor change). A "no descriptor" slot is
/// `Option<TupleDesc>` (`None` = the old null pointer).
///
/// Memory lifetime is now `Arc`'s: the descriptor is freed when the last handle
/// drops. The `tdrefcount` field/methods are retained for PG resource-owner
/// bookkeeping semantics (vestigial until resowner integration); they no longer
/// govern freeing. Construct from a by-value `TupleDescData` with `Arc::new`;
/// read through the `Arc` (deref to `&TupleDescData`, rules.md s5); mutate a
/// shared descriptor with [`Arc::make_mut`] (copy-on-write, rules.md s8).
pub type TupleDesc = Arc<TupleDescData>;

impl TupleDescData {
    /// Accessor for the i'th FormData_pg_attribute (C TupleDescAttr).
    pub fn attr(&self, i: usize) -> &FormData_pg_attribute {
        &self.attrs[i]
    }

    /// Accessor for the i'th CompactAttribute (C TupleDescCompactAttr).
    pub fn compact_attr(&self, i: usize) -> &CompactAttribute {
        &self.compact_attrs[i]
    }

    /// `IncrTupleDescRefCount`: bump a reference-counted descriptor's refcount.
    ///
    /// `Arc` now owns the descriptor's MEMORY lifetime; `tdrefcount` is retained
    /// only for PG resource-owner BOOKKEEPING (vestigial until resowner
    /// integration). In C this also logs the reference in `CurrentResourceOwner`
    /// so an `ERROR` unwind releases it; here an unwind drops the `Arc` handle
    /// directly, so the counter is advisory and never governs freeing.
    pub fn incr_ref_count(&mut self) {
        crate::assert!(self.tdrefcount >= 0);
        self.tdrefcount += 1;
    }

    /// `DecrTupleDescRefCount`: drop a reference taken by `incr_ref_count`.
    ///
    /// Adjusts the advisory `tdrefcount` only; the descriptor is freed when the
    /// last `Arc` drops, not here (see `incr_ref_count`).
    pub fn decr_ref_count(&mut self) {
        crate::assert!(self.tdrefcount > 0);
        self.tdrefcount -= 1;
    }
}

// The real bodies live in the backend module
// (`crate::backend::access::common::tupdesc`) as inherent methods on
// `TupleDescData`. The C-named entry points below are deprecated `#[inline]`
// shims delegating there, so existing `crate::access::tupdesc::<CName>` call
// sites keep resolving while new code is nudged to the methods. The
// per-descriptor operations take `&`/`&mut TupleDescData` (no `unsafe`, no raw
// pointers); only create/free move the owning `TupleDesc` handle.

#[deprecated(note = "use TupleDescData::populate_compact_attribute")]
#[inline]
pub fn populate_compact_attribute(tupdesc: &mut TupleDescData, attnum: i32) {
    tupdesc.populate_compact_attribute(attnum as usize);
}

#[deprecated(note = "use TupleDescData::verify_compact_attribute")]
#[inline]
pub fn verify_compact_attribute(tupdesc: &TupleDescData, attnum: i32) {
    tupdesc.verify_compact_attribute(attnum as usize);
}

#[deprecated(note = "use TupleDescData::create_template")]
#[inline]
pub fn CreateTemplateTupleDesc(natts: i32) -> TupleDescData {
    TupleDescData::create_template(natts)
}

#[deprecated(note = "use TupleDescData::create")]
#[inline]
pub fn CreateTupleDesc(natts: i32, attrs: &[&FormData_pg_attribute]) -> TupleDescData {
    TupleDescData::create(natts, attrs)
}

#[deprecated(note = "use TupleDescData::create_copy")]
#[inline]
pub fn CreateTupleDescCopy(tupdesc: &TupleDescData) -> TupleDescData {
    tupdesc.create_copy()
}

#[deprecated(note = "use TupleDescData::create_truncated_copy")]
#[inline]
pub fn CreateTupleDescTruncatedCopy(tupdesc: &TupleDescData, natts: i32) -> TupleDescData {
    tupdesc.create_truncated_copy(natts)
}

#[deprecated(note = "use TupleDescData::create_copy_constr")]
#[inline]
pub fn CreateTupleDescCopyConstr(tupdesc: &TupleDescData) -> TupleDescData {
    tupdesc.create_copy_constr()
}

#[deprecated(note = "use TupleDescData::copy_into")]
#[inline]
pub fn TupleDescCopy(dst: &mut TupleDescData, src: &TupleDescData) {
    src.copy_into(dst);
}

#[deprecated(note = "use TupleDescData::copy_entry")]
#[inline]
pub fn TupleDescCopyEntry(
    dst: &mut TupleDescData,
    dstAttno: AttrNumber,
    src: &TupleDescData,
    srcAttno: AttrNumber,
) {
    TupleDescData::copy_entry(dst, dstAttno, src, srcAttno);
}

#[deprecated(note = "TupleDescData drops on scope exit; explicit free is unnecessary")]
#[inline]
pub fn FreeTupleDesc(tupdesc: TupleDescData) {
    crate::assert!(tupdesc.tdrefcount <= 0);
    drop(tupdesc);
}

#[deprecated(note = "use TupleDescData::incr_ref_count")]
#[inline]
pub fn IncrTupleDescRefCount(tupdesc: &mut TupleDescData) {
    tupdesc.incr_ref_count();
}

#[deprecated(note = "use TupleDescData::decr_ref_count")]
#[inline]
pub fn DecrTupleDescRefCount(tupdesc: &mut TupleDescData) {
    tupdesc.decr_ref_count();
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

#[deprecated(note = "use TupleDescData::equals")]
#[inline]
pub fn equalTupleDescs(tupdesc1: &TupleDescData, tupdesc2: &TupleDescData) -> bool {
    tupdesc1.equals(tupdesc2)
}

#[deprecated(note = "use TupleDescData::row_types_equal")]
#[inline]
pub fn equalRowTypes(tupdesc1: &TupleDescData, tupdesc2: &TupleDescData) -> bool {
    tupdesc1.row_types_equal(tupdesc2)
}

#[deprecated(note = "use TupleDescData::hash_row_type")]
#[inline]
pub fn hashRowType(desc: &TupleDescData) -> u32 {
    desc.hash_row_type()
}

#[deprecated(note = "use TupleDescData::init_entry")]
#[inline]
pub fn TupleDescInitEntry(
    desc: &mut TupleDescData,
    attributeNumber: AttrNumber,
    attributeName: Option<&str>,
    oidtypeid: Oid,
    typmod: i32,
    attdim: i32,
) {
    desc.init_entry(attributeNumber, attributeName, oidtypeid, typmod, attdim);
}

#[deprecated(note = "use TupleDescData::init_builtin_entry")]
#[inline]
pub fn TupleDescInitBuiltinEntry(
    desc: &mut TupleDescData,
    attributeNumber: AttrNumber,
    attributeName: &str,
    oidtypeid: Oid,
    typmod: i32,
    attdim: i32,
) {
    desc.init_builtin_entry(attributeNumber, attributeName, oidtypeid, typmod, attdim);
}

#[deprecated(note = "use TupleDescData::init_entry_collation")]
#[inline]
pub fn TupleDescInitEntryCollation(
    desc: &mut TupleDescData,
    attributeNumber: AttrNumber,
    collationid: Oid,
) {
    desc.init_entry_collation(attributeNumber, collationid);
}

#[deprecated(note = "use build_desc_from_lists")]
#[inline]
pub fn BuildDescFromLists(
    names: &[String],
    types: &[Oid],
    typmods: &[i32],
    collations: &[Oid],
) -> TupleDescData {
    crate::backend::access::common::tupdesc::build_desc_from_lists(names, types, typmods, collations)
}

#[deprecated(note = "use TupleDescData::get_default")]
#[inline]
pub fn TupleDescGetDefault(tupdesc: &TupleDescData, attnum: AttrNumber) -> Option<Node> {
    tupdesc.get_default(attnum)
}

//! Translated from PostgreSQL src/include/utils/expandedrecord.h

use bitflags::bitflags;

use crate::access::htup::HeapTuple;
use crate::access::tupdesc::TupleDesc;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::expandeddatum::ExpandedObjectHeader;
use crate::utils::palloc::{MemoryContext, MemoryContextCallback};

/// ID for debugging crosschecks.
pub const ER_MAGIC: i32 = 1384727874;

bitflags! {
    /// Assorted flag bits in ExpandedRecordHeader.flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ErFlag: i32 {
        const FVALUE_VALID    = 0x0001; // fvalue is up to date?
        const FVALUE_ALLOCED  = 0x0002; // fvalue is local storage?
        const DVALUES_VALID   = 0x0004; // dvalues/dnulls are up to date?
        const DVALUES_ALLOCED = 0x0008; // any field values local storage?
        const HAVE_EXTERNAL   = 0x0010; // any field values are external?
        const TUPDESC_ALLOCED = 0x0020; // tupdesc is local storage?
        const IS_DOMAIN       = 0x0040; // er_decltypeid is domain?
        const IS_DUMMY        = 0x0080; // this header is dummy
        /// Flag bits not cleared when replacing tuple data.
        const FLAGS_NON_DATA =
            Self::TUPDESC_ALLOCED.bits() | Self::IS_DOMAIN.bits() | Self::IS_DUMMY.bits();
    }
}

/// In-memory control structure for a composite expanded object (the expanded
/// record lives in its own private memory context).
pub struct ExpandedRecordHeader {
    /// Standard header for expanded objects.
    pub hdr: ExpandedObjectHeader,
    /// Magic value identifying an expanded record (debugging only).
    pub er_magic: i32,
    /// Assorted flag bits.
    pub flags: ErFlag,
    /// Declared type of the record variable (could be a domain type).
    pub er_decltypeid: Oid,
    /// Actual composite type OID; never a domain.
    pub er_typeid: Oid,
    /// typmod of the composite type.
    pub er_typmod: i32,
    /// Tuple descriptor, if we have one.
    pub er_tupdesc: Option<TupleDesc>,
    /// Unique-within-process identifier for the tupdesc.
    pub er_tupdesc_id: u64,
    /// Datum-array representation, if valid; length matches er_tupdesc->natts.
    pub dvalues: Option<Vec<Datum>>,
    pub dnulls: Option<Vec<bool>>,
    pub nfields: i32,
    /// Current flat-equivalent space requirement, else 0.
    pub flat_size: usize,
    pub data_len: usize,
    pub hoff: i32,
    pub hasnull: bool,
    /// Flat representation if we have one.
    pub fvalue: Option<HeapTuple>,
    pub fstartptr: *mut u8, // TODO(ptr): start of flat data area
    pub fendptr: *mut u8,   // TODO(ptr): end+1 of flat data area
    /// Short-lived context for some operations.
    pub er_short_term_cxt: MemoryContext,
    /// Dummy record header used for domain checking (ER_FLAG_IS_DOMAIN).
    pub er_dummy_header: Option<Box<Self>>,
    /// Cache space for domain_check().
    pub er_domaininfo: *mut u8, // TODO(ptr): opaque domain cache
    /// Callback info (active if er_mcb.arg is not NULL).
    pub er_mcb: MemoryContextCallback,
}

pub fn ExpandedRecordGetDatum(erh: &ExpandedRecordHeader) -> Datum {
    unimplemented!() // EOHPGetRWDatum(&erh->hdr)
}

pub fn ExpandedRecordGetRODatum(erh: &ExpandedRecordHeader) -> Datum {
    unimplemented!() // EOHPGetRODatum(&erh->hdr)
}

/// ExpandedRecordIsEmpty: neither dvalues nor fvalue is valid.
pub fn ExpandedRecordIsEmpty(erh: &ExpandedRecordHeader) -> bool {
    !erh.flags
        .intersects(ErFlag::DVALUES_VALID | ErFlag::FVALUE_VALID)
}

/// ExpandedRecordIsDomain.
pub fn ExpandedRecordIsDomain(erh: &ExpandedRecordHeader) -> bool {
    erh.flags.contains(ErFlag::IS_DOMAIN)
}

/// Information returned by expanded_record_lookup_field().
pub struct ExpandedRecordFieldInfo {
    pub fnumber: i32,
    pub ftypeid: Oid,
    pub ftypmod: i32,
    pub fcollation: Oid,
}

pub fn make_expanded_record_from_typeid(
    type_id: Oid,
    typmod: i32,
    parentcontext: MemoryContext,
) -> Box<ExpandedRecordHeader> {
    unimplemented!()
}

pub fn make_expanded_record_from_tupdesc(
    tupdesc: TupleDesc,
    parentcontext: MemoryContext,
) -> Box<ExpandedRecordHeader> {
    unimplemented!()
}

pub fn make_expanded_record_from_exprecord(
    olderh: &mut ExpandedRecordHeader,
    parentcontext: MemoryContext,
) -> Box<ExpandedRecordHeader> {
    unimplemented!()
}

pub fn expanded_record_set_tuple(
    erh: &mut ExpandedRecordHeader,
    tuple: HeapTuple,
    copy: bool,
    expand_external: bool,
) {
    unimplemented!()
}

pub fn make_expanded_record_from_datum(recorddatum: Datum, parentcontext: MemoryContext) -> Datum {
    unimplemented!()
}

pub fn expanded_record_fetch_tupdesc(erh: &mut ExpandedRecordHeader) -> TupleDesc {
    unimplemented!()
}

pub fn expanded_record_get_tuple(erh: &mut ExpandedRecordHeader) -> HeapTuple {
    unimplemented!()
}

pub fn DatumGetExpandedRecord(d: Datum) -> Box<ExpandedRecordHeader> {
    unimplemented!()
}

pub fn deconstruct_expanded_record(erh: &mut ExpandedRecordHeader) {
    unimplemented!()
}

/// Returns false when no such field; fills finfo on success.
pub fn expanded_record_lookup_field(
    erh: &mut ExpandedRecordHeader,
    fieldname: &str,
    finfo: &mut ExpandedRecordFieldInfo,
) -> bool {
    unimplemented!()
}

/// C `bool *isnull` out-param folded into Option.
pub fn expanded_record_fetch_field(erh: &mut ExpandedRecordHeader, fnumber: i32) -> Option<Datum> {
    unimplemented!()
}

pub fn expanded_record_set_field_internal(
    erh: &mut ExpandedRecordHeader,
    fnumber: i32,
    new_value: Datum,
    isnull: bool,
    expand_external: bool,
    check_constraints: bool,
) {
    unimplemented!()
}

pub fn expanded_record_set_fields(
    erh: &mut ExpandedRecordHeader,
    new_values: &[Datum],
    isnulls: &[bool],
    expand_external: bool,
) {
    unimplemented!()
}

/// Public wrapper for expanded_record_set_field_internal (check_constraints = true).
pub fn expanded_record_set_field(
    erh: &mut ExpandedRecordHeader,
    fnumber: i32,
    new_value: Datum,
    isnull: bool,
    expand_external: bool,
) {
    expanded_record_set_field_internal(erh, fnumber, new_value, isnull, expand_external, true);
}

/// Inline fast path: tupdesc for the expanded record's actual type.
pub fn expanded_record_get_tupdesc(erh: &mut ExpandedRecordHeader) -> TupleDesc {
    unimplemented!() // erh->er_tupdesc if present, else fetch
}

/// Inline fast path: value of a record field (folds isnull into Option).
pub fn expanded_record_get_field(erh: &mut ExpandedRecordHeader, fnumber: i32) -> Option<Datum> {
    unimplemented!()
}

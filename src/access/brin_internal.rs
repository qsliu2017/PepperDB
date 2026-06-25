//! Translated from PostgreSQL src/include/access/brin_internal.h

use crate::access::genam::{
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInfo, IndexScanDesc,
    IndexUniqueCheck, IndexVacuumInfo,
};
use crate::access::skey::ScanKey;
use crate::access::tupdesc::TupleDesc;
use crate::c::bytea;
use crate::nodes::tidbitmap::TIDBitmap;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::memutils::MemoryContext;
use crate::utils::rel::Relation;
use crate::utils::typcache::TypeCacheEntry;

/// struct returned by "OpcInfo" amproc (in-memory). On-disk FAM `typcache[]`
/// becomes a `Vec`.
pub struct BrinOpcInfo {
    /// Number of columns stored in an index column of this opclass
    pub nstored: u16,
    /// Regular processing of NULLs in BrinValues?
    pub regular_nulls: bool,
    /// Opaque pointer for the opclass' private use
    pub opaque: *mut core::ffi::c_void, // TODO(ptr): opclass-private state
    /// Type cache entries of the stored columns
    pub typcache: Vec<*mut TypeCacheEntry>, // TODO(ptr)
}

/// the size of a BrinOpcInfo for the given number of columns
pub const fn sizeof_brin_opc_info(ncols: usize) -> usize {
    core::mem::offset_of!(BrinOpcInfo, typcache)
        + core::mem::size_of::<*mut TypeCacheEntry>() * ncols
}

/// Decodes a BRIN tuple between on-disk and in-memory form (in-memory state).
pub struct BrinDesc {
    /// Containing memory context
    pub context: MemoryContext,
    /// the index relation itself
    pub index: Relation,
    /// tuple descriptor of the index relation
    pub tupdesc: TupleDesc,
    /// cached copy for on-disk tuples; generated at first use
    pub disktdesc: TupleDesc,
    /// total number of Datum entries that are stored on-disk for all columns
    pub totalstored: i32,
    /// per-column info; tupdesc->natts entries long (on-disk FAM -> Vec)
    pub info: Vec<*mut BrinOpcInfo>, // TODO(ptr)
}

/*
 * Globally-known function support numbers for BRIN indexes.  Individual
 * opclasses can define more function support numbers, which must fall into
 * BRIN_FIRST_OPTIONAL_PROCNUM .. BRIN_LAST_OPTIONAL_PROCNUM.
 */
pub const BRIN_PROCNUM_OPCINFO: u16 = 1;
pub const BRIN_PROCNUM_ADDVALUE: u16 = 2;
pub const BRIN_PROCNUM_CONSISTENT: u16 = 3;
pub const BRIN_PROCNUM_UNION: u16 = 4;
pub const BRIN_MANDATORY_NPROCS: u16 = 4;
pub const BRIN_PROCNUM_OPTIONS: u16 = 5; // optional
/* procedure numbers up to 10 are reserved for BRIN future expansion */
pub const BRIN_FIRST_OPTIONAL_PROCNUM: u16 = 11;
pub const BRIN_LAST_OPTIONAL_PROCNUM: u16 = 15;

// BRIN_DEBUG / BRIN_elog are debug-only no-op macros; no Rust equivalent needed.

/* brin.c */
pub fn brin_build_desc(_rel: Relation) -> *mut BrinDesc {
    unimplemented!()
}
pub fn brin_free_desc(_bdesc: &mut BrinDesc) {
    unimplemented!()
}
pub fn brinbuild(
    _heap: Relation,
    _index: Relation,
    _index_info: &mut IndexInfo,
) -> *mut IndexBuildResult {
    unimplemented!()
}
pub fn brinbuildempty(_index: Relation) {
    unimplemented!()
}
pub fn brininsert(
    _idx_rel: Relation,
    _values: &[Datum],
    _nulls: &[bool],
    _heaptid: &mut ItemPointerData,
    _heap_rel: Relation,
    _check_unique: IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: &mut IndexInfo,
) -> bool {
    unimplemented!()
}
pub fn brininsertcleanup(_index: Relation, _index_info: &mut IndexInfo) {
    unimplemented!()
}
pub fn brinbeginscan(_r: Relation, _nkeys: i32, _norderbys: i32) -> IndexScanDesc {
    unimplemented!()
}
pub fn bringetbitmap(_scan: IndexScanDesc, _tbm: &mut TIDBitmap) -> i64 {
    unimplemented!()
}
pub fn brinrescan(
    _scan: IndexScanDesc,
    _scankey: ScanKey,
    _nscankeys: i32,
    _orderbys: ScanKey,
    _norderbys: i32,
) {
    unimplemented!()
}
pub fn brinendscan(_scan: IndexScanDesc) {
    unimplemented!()
}
pub fn brinbulkdelete(
    _info: &mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
    _callback: &mut IndexBulkDeleteCallback,
    _callback_state: *mut core::ffi::c_void,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}
pub fn brinvacuumcleanup(
    _info: &mut IndexVacuumInfo,
    _stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}
pub fn brinoptions(_reloptions: Datum, _validate: bool) -> *mut bytea {
    unimplemented!()
}

/* brin_validate.c */
pub fn brinvalidate(_opclassoid: Oid) -> bool {
    unimplemented!()
}

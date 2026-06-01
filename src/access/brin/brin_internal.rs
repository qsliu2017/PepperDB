//! access/brin_internal.h - internal declarations for BRIN indexes

use std::ffi::{c_int, c_void};

use crate::access::common::scankey::ScanKey;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::index::amapi::{
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexScanDesc,
    IndexUniqueCheck, IndexVacuumInfo,
};
use crate::c::{int64, uint16, FLEXIBLE_ARRAY_MEMBER};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::itemptr::ItemPointer;
use crate::utils::mmgr::memnodes::MemoryContext;
use crate::utils::rel::Relation;

// "utils/typcache.h" -> TypeCacheEntry: canonical home is utils/cache/typcache.
pub use crate::utils::cache::typcache::TypeCacheEntry;

// "storage/bufpage.h" -> bytea via brinoptions; bytea lives in crate::c.
// "nodes/tidbitmap.h" -> TIDBitmap; canonical home crate::nodes::tidbitmap.
use crate::c::bytea;
use crate::nodes::tidbitmap::TIDBitmap;

// "nodes/execnodes.h" -> struct IndexInfo (used as `struct IndexInfo *`).
use crate::nodes::execnodes::IndexInfo;

/*
 * A BrinDesc is a struct designed to enable decoding a BRIN tuple from the
 * on-disk format to an in-memory tuple and vice-versa.
 */

/* struct returned by "OpcInfo" amproc */
#[repr(C)]
pub struct BrinOpcInfo {
    /* Number of columns stored in an index column of this opclass */
    pub oi_nstored: uint16,

    /* Regular processing of NULLs in BrinValues? */
    pub oi_regular_nulls: bool,

    /* Opaque pointer for the opclass' private use */
    pub oi_opaque: *mut c_void,

    /* Type cache entries of the stored columns */
    pub oi_typcache: [*mut TypeCacheEntry; FLEXIBLE_ARRAY_MEMBER],
}

/* the size of a BrinOpcInfo for the given number of columns */
#[inline]
pub fn SizeofBrinOpcInfo(ncols: usize) -> usize {
    // offsetof(BrinOpcInfo, oi_typcache) + sizeof(TypeCacheEntry *) * ncols
    std::mem::offset_of!(BrinOpcInfo, oi_typcache)
        + std::mem::size_of::<*mut TypeCacheEntry>() * ncols
}

#[repr(C)]
pub struct BrinDesc {
    /* Containing memory context */
    pub bd_context: MemoryContext,

    /* the index relation itself */
    pub bd_index: Relation,

    /* tuple descriptor of the index relation */
    pub bd_tupdesc: TupleDesc,

    /* cached copy for on-disk tuples; generated at first use */
    pub bd_disktdesc: TupleDesc,

    /* total number of Datum entries that are stored on-disk for all columns */
    pub bd_totalstored: c_int,

    /* per-column info; bd_tupdesc->natts entries long */
    pub bd_info: [*mut BrinOpcInfo; FLEXIBLE_ARRAY_MEMBER],
}

/*
 * Globally-known function support numbers for BRIN indexes.  Individual
 * opclasses can define more function support numbers, which must fall into
 * BRIN_FIRST_OPTIONAL_PROCNUM .. BRIN_LAST_OPTIONAL_PROCNUM.
 */
pub const BRIN_PROCNUM_OPCINFO: c_int = 1;
pub const BRIN_PROCNUM_ADDVALUE: c_int = 2;
pub const BRIN_PROCNUM_CONSISTENT: c_int = 3;
pub const BRIN_PROCNUM_UNION: c_int = 4;
pub const BRIN_MANDATORY_NPROCS: c_int = 4;
pub const BRIN_PROCNUM_OPTIONS: c_int = 5; /* optional */
/* procedure numbers up to 10 are reserved for BRIN future expansion */
pub const BRIN_FIRST_OPTIONAL_PROCNUM: c_int = 11;
pub const BRIN_LAST_OPTIONAL_PROCNUM: c_int = 15;

/* #undef BRIN_DEBUG -- BRIN_DEBUG not defined; BRIN_elog(args) is ((void) 0) */
#[inline]
pub fn BRIN_elog() {
    // ((void) 0)
}

/* brin.c */
pub unsafe fn brin_build_desc(rel: Relation) -> *mut BrinDesc {
    unimplemented!()
}
pub unsafe fn brin_free_desc(bdesc: *mut BrinDesc) {
    unimplemented!()
}
pub unsafe fn brinbuild(
    heap: Relation,
    index: Relation,
    indexInfo: *mut IndexInfo,
) -> *mut IndexBuildResult {
    unimplemented!()
}
pub unsafe fn brinbuildempty(index: Relation) {
    unimplemented!()
}
pub unsafe fn brininsert(
    idxRel: Relation,
    values: *mut Datum,
    nulls: *mut bool,
    heaptid: ItemPointer,
    heapRel: Relation,
    checkUnique: IndexUniqueCheck,
    indexUnchanged: bool,
    indexInfo: *mut IndexInfo,
) -> bool {
    unimplemented!()
}
pub unsafe fn brininsertcleanup(index: Relation, indexInfo: *mut IndexInfo) {
    unimplemented!()
}
pub unsafe fn brinbeginscan(r: Relation, nkeys: c_int, norderbys: c_int) -> IndexScanDesc {
    unimplemented!()
}
pub unsafe fn bringetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    unimplemented!()
}
pub unsafe fn brinrescan(
    scan: IndexScanDesc,
    scankey: ScanKey,
    nscankeys: c_int,
    orderbys: ScanKey,
    norderbys: c_int,
) {
    unimplemented!()
}
pub unsafe fn brinendscan(scan: IndexScanDesc) {
    unimplemented!()
}
pub unsafe fn brinbulkdelete(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut c_void,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}
pub unsafe fn brinvacuumcleanup(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    unimplemented!()
}
pub unsafe fn brinoptions(reloptions: Datum, validate: bool) -> *mut bytea {
    unimplemented!()
}

/* brin_validate.c */
pub unsafe fn brinvalidate(opclassoid: Oid) -> bool {
    unimplemented!()
}

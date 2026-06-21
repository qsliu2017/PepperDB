//! Translation of postgres/src/include/catalog/pg_publication.h and the
//! backend C API in postgres/src/backend/catalog/pg_publication.c
//!
//! The `FormData_pg_publication` struct: the fixed-layout part of a
//! pg_publication catalog row.  The C CATALOG(pg_publication) struct has no
//! `#ifdef CATALOG_VARLEN` section, so every declared column is part of this
//! in-memory struct.  The EXPOSE_TO_CLIENT_CODE section of the header defines
//! the PublishGencolsType enum (values of the pubgencols column), translated
//! below as pub consts.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use crate::c::{int16, int2vector, NameData};
use crate::nodes::bitmapset::{
    bms_add_member, bms_is_member, bms_next_member, bms_num_members, Bitmapset,
};
use crate::nodes::pg_list::{
    lappend, lappend_oid, lfirst, lfirst_oid, list_concat, list_concat_unique_oid,
    list_deduplicate_oid, list_free, list_length, list_member_oid, list_nth, list_oid_cmp,
    list_sort, List, ListCell, NIL,
};
use crate::postgres_ext::Oid;
use crate::utils::builtins::buildint2vector;
use crate::{
    current_cell, elog, ereport, errmsg, foreach, foreach_delete_current, Assert, PG_GETARG_OID,
    PG_RETURN_BOOL, PG_RETURN_NULL,
};
use core::ffi::{c_char, c_int, c_void};

/* log levels  TODO(pg-port): real values from utils/elog.h */
const ERROR: c_int = 21;

/* ERRCODEs (folded into /* C also: */ comments at call sites) */

/* ----------------------------------------------------------------
 * Local type aliases / forward declarations for unported deps.
 * ---------------------------------------------------------------- */

/* Datum  TODO(pg-port): real def lives in postgres.h */
type Datum = usize;
/* HeapTuple  TODO(pg-port): access/htup.h */
type HeapTuple = *mut c_void;
/* Relation  TODO(pg-port): utils/rel.h */
type Relation = *mut RelationData;
/* Node  TODO(pg-port): nodes/nodes.h */
type Node = c_void;
/* MemoryContext  TODO(pg-port): nodes/memnodes.h */
type MemoryContext = *mut c_void;
/* TupleDesc  TODO(pg-port): access/tupdesc.h */
type TupleDesc = *mut c_void;
/* AttrNumber  TODO(pg-port): access/attnum.h */
type AttrNumber = i16;
/* PublicationPartOpt  TODO(pg-port): catalog/pg_publication.h */
type PublicationPartOpt = c_int;
/* PublishGencolsType  TODO(pg-port): catalog/pg_publication.h */
type PublishGencolsType = c_int;
/* PG_FUNCTION_ARGS  TODO(pg-port): fmgr.h */
use crate::utils::fmgr::FunctionCallInfo;
/* CatCList  TODO(pg-port): utils/catcache.h */
#[repr(C)]
pub struct CatCList {
    pub n_members: c_int,
    pub members: *mut *mut CatCTup,
}
#[repr(C)]
pub struct CatCTup {
    pub tuple: HeapTupleData,
}
#[repr(C)]
pub struct HeapTupleData {
    _opaque: [u8; 0],
}
/* RelationData  TODO(pg-port): utils/rel.h */
#[repr(C)]
pub struct RelationData {
    pub rd_rel: Form_pg_class,
}
/* Form_pg_class fields used here  TODO(pg-port): catalog/pg_class.h */
#[repr(C)]
pub struct FormData_pg_class {
    pub oid: Oid,
    pub relkind: c_char,
    pub relpersistence: c_char,
    pub relispartition: bool,
}
type Form_pg_class = *mut FormData_pg_class;
/* Form_pg_attribute fields used here  TODO(pg-port): catalog/pg_attribute.h */
#[repr(C)]
pub struct FormData_pg_attribute {
    pub attnum: AttrNumber,
    pub attgenerated: c_char,
    pub attisdropped: bool,
}
type Form_pg_attribute = *mut FormData_pg_attribute;
/* Form_pg_publication_rel  TODO(pg-port): catalog/pg_publication_rel.h */
#[repr(C)]
pub struct FormData_pg_publication_rel {
    pub prpubid: Oid,
    pub prrelid: Oid,
}
type Form_pg_publication_rel = *mut FormData_pg_publication_rel;
/* Form_pg_publication_namespace  TODO(pg-port): catalog/pg_publication_namespace.h */
#[repr(C)]
pub struct FormData_pg_publication_namespace {
    pub pnpubid: Oid,
    pub pnnspid: Oid,
}
type Form_pg_publication_namespace = *mut FormData_pg_publication_namespace;
/* ArrayType  TODO(pg-port): utils/array.h */
#[repr(C)]
pub struct ArrayType {
    _opaque: [u8; 0],
}

/* PublicationActions  TODO(pg-port): nodes/pathnodes.h */
#[repr(C)]
pub struct PublicationActions {
    pub pubinsert: bool,
    pub pubupdate: bool,
    pub pubdelete: bool,
    pub pubtruncate: bool,
}

/* Publication  TODO(pg-port): catalog/pg_publication.h */
#[repr(C)]
pub struct Publication {
    pub oid: Oid,
    pub name: *mut c_char,
    pub alltables: bool,
    pub pubviaroot: bool,
    pub pubgencols_type: PublishGencolsType,
    pub pubactions: PublicationActions,
}

/*
 * PublicationRelInfo: opened relation + its where clause + column list.
 */
#[repr(C)]
pub struct PublicationRelInfo {
    pub relation: Relation,
    pub whereClause: *mut Node,
    pub columns: *mut List,
}

/* Records association between publication and published table */
#[repr(C)]
struct published_rel {
    relid: Oid, /* OID of published table */
    pubid: Oid, /* OID of publication that publishes this table. */
}

/* ----------------------------------------------------------------
 * Local constants mirroring the C #defines used below.
 * ---------------------------------------------------------------- */

const RELKIND_RELATION: c_char = b'r' as c_char;
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;
const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;
const RELPERSISTENCE_UNLOGGED: c_char = b'u' as c_char;
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;
const ATTRIBUTE_GENERATED_STORED: c_char = b's' as c_char;
const ATTRIBUTE_GENERATED_VIRTUAL: c_char = b'v' as c_char;

const FirstNormalObjectId: Oid = 16384;
const InvalidOid: Oid = 0;
const InvalidAttrNumber: AttrNumber = 0;
const PG_INT16_MAX: c_int = 32767;

const PUBLICATION_PART_ROOT: PublicationPartOpt = 0;
const PUBLICATION_PART_ALL: PublicationPartOpt = 1;
const PUBLICATION_PART_LEAF: PublicationPartOpt = 2;

/* syscache IDs  TODO(pg-port): real values from utils/syscache.h */
const RELOID: c_int = 57;
const PUBLICATIONOID: c_int = 51;
const PUBLICATIONRELMAP: c_int = 53;
const PUBLICATIONNAMESPACEMAP: c_int = 50;

/* lock modes  TODO(pg-port): storage/lockdefs.h */
const NoLock: c_int = 0;
const AccessShareLock: c_int = 1;
const RowExclusiveLock: c_int = 5;

/* strategy / proc numbers  TODO(pg-port): access/stratnum.h, utils/fmgroids.h */
const BTEqualStrategyNumber: c_int = 3;
const F_OIDEQ: Oid = 184;
const F_BOOLEQ: Oid = 60;
const F_CHAREQ: Oid = 61;

/* scan directions  TODO(pg-port): access/sdir.h */
const ForwardScanDirection: c_int = 1;

/* dependency types  TODO(pg-port): catalog/dependency.h */
const DEPENDENCY_AUTO: c_char = b'a' as c_char;
const DEPENDENCY_NORMAL: c_char = b'n' as c_char;

/* type OIDs  TODO(pg-port): catalog/pg_type_d.h */
const OIDOID: Oid = 26;
const TEXTOID: Oid = 25;
const INT2VECTOROID: Oid = 22;
const PG_NODE_TREEOID: Oid = 194;

/* catalog relation / index OIDs  TODO(pg-port): catalog/_d.h */
const RelationRelationId: Oid = 1259;
const NamespaceRelationId: Oid = 2615;
const PublicationRelationId: Oid = 6104;
const PublicationRelRelationId: Oid = 6106;
const PublicationNamespaceRelationId: Oid = 6237;
const PublicationRelObjectIndexId: Oid = 6112;
const PublicationRelPrpubidIndexId: Oid = 6113;
const PublicationNamespaceObjectIndexId: Oid = 6239;
const PublicationNamespacePnnspidPnpubidIndexId: Oid = 6240;

/* pg_publication_rel attribute numbers  TODO(pg-port): catalog/pg_publication_rel_d.h */
const Anum_pg_publication_rel_oid: AttrNumber = 1;
const Anum_pg_publication_rel_prpubid: AttrNumber = 2;
const Anum_pg_publication_rel_prrelid: AttrNumber = 3;
const Anum_pg_publication_rel_prqual: AttrNumber = 4;
const Anum_pg_publication_rel_prattrs: AttrNumber = 5;
const Natts_pg_publication_rel: usize = 5;

/* pg_publication_namespace attribute numbers  TODO(pg-port) */
const Anum_pg_publication_namespace_oid: AttrNumber = 1;
const Anum_pg_publication_namespace_pnpubid: AttrNumber = 2;
const Anum_pg_publication_namespace_pnnspid: AttrNumber = 3;
const Natts_pg_publication_namespace: usize = 3;

/* pg_publication attribute numbers  TODO(pg-port) */
const Anum_pg_publication_puballtables: AttrNumber = 4;

/* pg_class attribute numbers  TODO(pg-port): catalog/pg_class_d.h */
const Anum_pg_class_relkind: AttrNumber = 17;
const Anum_pg_class_relnamespace: AttrNumber = 3;

/*
 * FormData_pg_publication - the fixed part of a pg_publication row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_publication {
    /* oid */
    pub oid: Oid,
    /* name of the publication */
    pub pubname: NameData,
    /* publication owner */
    pub pubowner: Oid,
    /* encompass all tables in the database (except unlogged and temp ones) */
    pub puballtables: bool,
    /* true if inserts are published */
    pub pubinsert: bool,
    /* true if updates are published */
    pub pubupdate: bool,
    /* true if deletes are published */
    pub pubdelete: bool,
    /* true if truncates are published */
    pub pubtruncate: bool,
    /* true if partition changes are published using root schema */
    pub pubviaroot: bool,
    /* 'n'(none)/'s'(stored): how generated column data should be published */
    pub pubgencols: c_char,
}

/*
 * Form_pg_publication corresponds to a pointer to a tuple with the format of
 * the pg_publication relation.
 */
pub type Form_pg_publication = *mut FormData_pg_publication;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * PublishGencolsType - values of the pubgencols column.
 * ----------------------------------------------------------------
 */

/* Generated columns present should not be replicated. */
pub const PUBLISH_GENCOLS_NONE: c_char = b'n' as c_char;
/* Generated columns present should be replicated. */
pub const PUBLISH_GENCOLS_STORED: c_char = b's' as c_char;

/* ----------------------------------------------------------------
 * Local TODO(pg-port) stubs for deps not yet ported anywhere in src/.
 * Prefer importing the real fn once it lands.
 * ---------------------------------------------------------------- */

/* fmgr / FunctionCallInfo helpers */
#[repr(C)]
pub struct ObjectAddress {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: c_int,
}
const InvalidObjectAddress: ObjectAddress = ObjectAddress {
    classId: InvalidOid,
    objectId: InvalidOid,
    objectSubId: 0,
};

/* ScanKey / scan descriptors  TODO(pg-port): access/skey.h, access/genam.h */
#[repr(C)]
pub struct ScanKeyData {
    _opaque: [u8; 0],
}
type SysScanDesc = *mut c_void;
type TableScanDesc = *mut c_void;

/* SRF support  TODO(pg-port): funcapi.h */
#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: *mut c_void,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: TupleDesc,
}

unsafe fn RelationGetForm(rel: Relation) -> Form_pg_class {
    (*rel).rd_rel
}
unsafe fn RelationGetRelid(rel: Relation) -> Oid {
    (*(*rel).rd_rel).oid
}
unsafe fn RelationGetRelationName(_rel: Relation) -> *const c_char {
    /* TODO(pg-port): utils/rel.h NameStr(rd_rel->relname) */
    core::ptr::null()
}
unsafe fn RelationGetDescr(_rel: Relation) -> TupleDesc {
    /* TODO(pg-port): utils/rel.h */
    core::ptr::null_mut()
}
unsafe fn IsCatalogRelation(_rel: Relation) -> bool { crate::catalog::catalog::IsCatalogRelation(_rel as _) }
unsafe fn IsCatalogRelationOid(_relid: Oid) -> bool { crate::catalog::catalog::IsCatalogRelationOid(_relid as _) }
unsafe fn IsCatalogNamespace(_nspid: Oid) -> bool { crate::catalog::catalog::IsCatalogNamespace(_nspid as _) }
unsafe fn IsToastNamespace(_nspid: Oid) -> bool { crate::catalog::catalog::IsToastNamespace(_nspid as _) }
unsafe fn isAnyTempNamespace(_nspid: Oid) -> bool { crate::catalog::namespace::isAnyTempNamespace(_nspid as _) }
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char { crate::utils::cache::lsyscache::get_namespace_name(_nspid as _) }
fn errdetail_relkind_not_supported(_relkind: c_char) { unimplemented!() }

/* syscache  TODO(pg-port): utils/syscache.h */
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    core::ptr::null_mut()
}
unsafe fn SearchSysCache2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    core::ptr::null_mut()
}
unsafe fn SearchSysCacheCopy2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    core::ptr::null_mut()
}
unsafe fn SearchSysCacheExists2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> bool { crate::utils::cache::syscache::SearchSysCacheExists2(_cacheId as _, _key1 as _, _key2 as _) }
unsafe fn SearchSysCacheList1(_cacheId: c_int, _key1: Datum) -> *mut CatCList {
    core::ptr::null_mut()
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {}
unsafe fn ReleaseSysCacheList(_list: *mut CatCList) {}
unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: AttrNumber,
    _isNull: *mut bool,
) -> Datum {
    0
}
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}
unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void {
    /* TODO(pg-port): access/htup_details.h */
    core::ptr::null_mut()
}

/* Datum conversion  TODO(pg-port): postgres.h */
fn ObjectIdGetDatum(oid: Oid) -> Datum {
    oid as Datum
}
fn BoolGetDatum(b: bool) -> Datum {
    b as Datum
}
fn CharGetDatum(c: c_char) -> Datum {
    c as u8 as Datum
}
fn PointerGetDatum(p: *const c_void) -> Datum {
    p as Datum
}
fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    0
}
unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    core::ptr::null_mut()
}
unsafe fn PG_GETARG_ARRAYTYPE_P(_n: c_int) -> *mut ArrayType {
    core::ptr::null_mut()
}
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType { unimplemented!() }
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    0
}

/* table am  TODO(pg-port): access/table.h, access/tableam.h, access/heapam.h */
unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    core::ptr::null_mut()
}
unsafe fn table_close(_relation: Relation, _lockmode: c_int) {}
unsafe fn table_beginscan_catalog(
    _relation: Relation,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> TableScanDesc { unimplemented!() }
unsafe fn table_endscan(_scan: TableScanDesc) {}
unsafe fn heap_getnext(_scan: TableScanDesc, _direction: c_int) -> HeapTuple {
    core::ptr::null_mut()
}
unsafe fn heap_form_tuple(
    _tupleDescriptor: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple { unimplemented!() }
unsafe fn heap_freetuple(_htup: HeapTuple) { crate::access::common::heaptuple::heap_freetuple(_htup as _) }

/* genam  TODO(pg-port): access/genam.h */
unsafe fn ScanKeyInit(
    _entry: *mut ScanKeyData,
    _attributeNumber: AttrNumber,
    _strategy: c_int,
    _procedure: Oid,
    _argument: Datum,
) { crate::access::common::scankey::ScanKeyInit(_entry as _, _attributeNumber as _, _strategy as _, _procedure as _, _argument as _) }
unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc { unimplemented!() }
unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    core::ptr::null_mut()
}
unsafe fn systable_endscan(_sysscan: SysScanDesc) {}

/* catalog indexing / oid allocation  TODO(pg-port): catalog/indexing.h, catalog/catalog.h */
unsafe fn GetNewOidWithIndex(_relation: Relation, _indexId: Oid, _oidcolumn: AttrNumber) -> Oid {
    InvalidOid
}
unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) {}

/* dependency  TODO(pg-port): catalog/dependency.h */
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: c_char,
) { crate::catalog::pg_depend::recordDependencyOn(_depender as _, _referenced as _, _behavior as _) }
unsafe fn recordDependencyOnSingleRelExpr(
    _depender: *const ObjectAddress,
    _expr: *mut Node,
    _relId: Oid,
    _behavior: c_char,
    _self_behavior: c_char,
    _reverse_self: bool,
) { crate::catalog::dependency::recordDependencyOnSingleRelExpr(_depender as _, _expr as _, _relId as _, _behavior as _, _self_behavior as _, _reverse_self as _) }

/* lsyscache / partition / inherits  TODO(pg-port): utils/lsyscache.h, catalog/partition.h, catalog/pg_inherits.h */
unsafe fn get_rel_relkind(_relid: Oid) -> c_char {
    0
}
unsafe fn get_rel_relispartition(_relid: Oid) -> bool { crate::utils::cache::lsyscache::get_rel_relispartition(_relid as _) }
unsafe fn get_rel_namespace(_relid: Oid) -> Oid { crate::utils::cache::lsyscache::get_rel_namespace(_relid as _) }
unsafe fn get_attnum(_relid: Oid, _attname: *const c_char) -> AttrNumber {
    InvalidAttrNumber
}
unsafe fn get_partition_ancestors(_relid: Oid) -> *mut List { crate::catalog::partition::get_partition_ancestors(_relid as _) }
unsafe fn find_all_inheritors(_parentrelId: Oid, _lockmode: c_int, _numparents: *mut c_int) -> *mut List {
    NIL
}

/* publication helpers ported elsewhere  TODO(pg-port): commands/publicationcmds.c */
unsafe fn InvalidatePublicationRels(_relids: *mut List) { unimplemented!() }
unsafe fn get_publication_oid(_pubname: *const c_char, _missing_ok: bool) -> Oid { crate::utils::cache::lsyscache::get_publication_oid(_pubname as _, _missing_ok as _) }

/* memory contexts  TODO(pg-port): utils/palloc.h, utils/memutils.h */
unsafe fn palloc(_size: usize) -> *mut c_void {
    core::ptr::null_mut()
}
unsafe fn pstrdup(_s: *const c_char) -> *mut c_char {
    core::ptr::null_mut()
}
unsafe fn MemoryContextSwitchTo(_context: MemoryContext) -> MemoryContext {
    core::ptr::null_mut()
}

/* SRF / funcapi  TODO(pg-port): funcapi.h, nodes/execnodes.h */
unsafe fn deconstruct_array_builtin(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) { crate::utils::adt::arrayfuncs::deconstruct_array_builtin(_array as _, _elmtype as _, _elemsp as _, _nullsp as _, _nelemsp as _) }
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc { unimplemented!() }
unsafe fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _attributeName: *const c_char,
    _oidtypeid: Oid,
    _typmod: i32,
    _attdim: c_int,
) { crate::access::common::tupdesc::TupleDescInitEntry(_desc as _, _attributeNumber as _, _attributeName as _, _oidtypeid as _, _typmod as _, _attdim as _) }
unsafe fn BlessTupleDesc(tupdesc: TupleDesc) -> TupleDesc {
    tupdesc
}
unsafe fn SRF_IS_FIRSTCALL(_fcinfo: FunctionCallInfo) -> bool {
    false
}
unsafe fn SRF_FIRSTCALL_INIT(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    core::ptr::null_mut()
}
unsafe fn SRF_PERCALL_SETUP(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    core::ptr::null_mut()
}
unsafe fn SRF_RETURN_NEXT(_funcctx: *mut FuncCallContext, result: Datum) -> Datum {
    result
}
unsafe fn SRF_RETURN_DONE(_funcctx: *mut FuncCallContext) -> Datum {
    0
}

/* TupleDesc field accessors  TODO(pg-port): access/tupdesc.h */
unsafe fn TupleDescNatts(_desc: TupleDesc) -> c_int {
    0
}
unsafe fn TupleDescAttr(_desc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    core::ptr::null_mut()
}

/*
 * Check if relation can be in given publication and throws appropriate
 * error if not.
 */
unsafe fn check_publication_add_relation(targetrel: Relation) {
    /* Must be a regular or partitioned table */
    if (*RelationGetForm(targetrel)).relkind != RELKIND_RELATION
        && (*RelationGetForm(targetrel)).relkind != RELKIND_PARTITIONED_TABLE
    {
        ereport!(
            ERROR,
            errmsg!(
                "cannot add relation \"{}\" to publication",
                std::ffi::CStr::from_ptr(RelationGetRelationName(targetrel)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
         * errdetail_relkind_not_supported(RelationGetForm(targetrel)->relkind) */
    }

    /* Can't be system table */
    if IsCatalogRelation(targetrel) {
        ereport!(
            ERROR,
            errmsg!(
                "cannot add relation \"{}\" to publication",
                std::ffi::CStr::from_ptr(RelationGetRelationName(targetrel)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
         * errdetail("This operation is not supported for system tables.") */
    }

    /* UNLOGGED and TEMP relations cannot be part of publication. */
    if (*(*targetrel).rd_rel).relpersistence == RELPERSISTENCE_TEMP {
        ereport!(
            ERROR,
            errmsg!(
                "cannot add relation \"{}\" to publication",
                std::ffi::CStr::from_ptr(RelationGetRelationName(targetrel)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
         * errdetail("This operation is not supported for temporary tables.") */
    } else if (*(*targetrel).rd_rel).relpersistence == RELPERSISTENCE_UNLOGGED {
        ereport!(
            ERROR,
            errmsg!(
                "cannot add relation \"{}\" to publication",
                std::ffi::CStr::from_ptr(RelationGetRelationName(targetrel)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
         * errdetail("This operation is not supported for unlogged tables.") */
    }
}

/*
 * Check if schema can be in given publication and throw appropriate error if
 * not.
 */
unsafe fn check_publication_add_schema(schemaid: Oid) {
    /* Can't be system namespace */
    if IsCatalogNamespace(schemaid) || IsToastNamespace(schemaid) {
        ereport!(
            ERROR,
            errmsg!(
                "cannot add schema \"{}\" to publication",
                std::ffi::CStr::from_ptr(get_namespace_name(schemaid)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
         * errdetail("This operation is not supported for system schemas.") */
    }

    /* Can't be temporary namespace */
    if isAnyTempNamespace(schemaid) {
        ereport!(
            ERROR,
            errmsg!(
                "cannot add schema \"{}\" to publication",
                std::ffi::CStr::from_ptr(get_namespace_name(schemaid)).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
         * errdetail("Temporary schemas cannot be replicated.") */
    }
}

/*
 * Returns if relation represented by oid and Form_pg_class entry
 * is publishable.
 *
 * Does same checks as check_publication_add_relation() above, but does not
 * need relation to be opened and also does not throw errors.
 *
 * XXX  This also excludes all tables with relid < FirstNormalObjectId, ie all
 * tables created during initdb.  See the long comment in the C source.
 */
unsafe fn is_publishable_class(relid: Oid, reltuple: Form_pg_class) -> bool {
    ((*reltuple).relkind == RELKIND_RELATION || (*reltuple).relkind == RELKIND_PARTITIONED_TABLE)
        && !IsCatalogRelationOid(relid)
        && (*reltuple).relpersistence == RELPERSISTENCE_PERMANENT
        && relid >= FirstNormalObjectId
}

/*
 * Another variant of is_publishable_class(), taking a Relation.
 */
pub unsafe fn is_publishable_relation(rel: Relation) -> bool {
    is_publishable_class(RelationGetRelid(rel), (*rel).rd_rel)
}

/*
 * SQL-callable variant of the above
 *
 * This returns null when the relation does not exist.  This is intended to be
 * used for example in psql to avoid gratuitous errors when there are
 * concurrent catalog changes.
 */
pub unsafe fn pg_relation_is_publishable(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let tuple: HeapTuple;
    let result: bool;

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        PG_RETURN_NULL!(fcinfo);
    }
    result = is_publishable_class(relid, GETSTRUCT(tuple) as Form_pg_class);
    ReleaseSysCache(tuple);
    PG_RETURN_BOOL!(result)
}

/*
 * Returns true if the ancestor is in the list of published relations.
 * Otherwise, returns false.
 */
unsafe fn is_ancestor_member_tableinfos(ancestor: Oid, table_infos: *mut List) -> bool {
    let mut lc: *mut ListCell;

    foreach!(lc, table_infos, {
        let relid: Oid = (*(lfirst(current_cell!(lc)) as *mut published_rel)).relid;

        if relid == ancestor {
            return true;
        }
    });

    false
}

/*
 * Filter out the partitions whose parent tables are also present in the list.
 */
unsafe fn filter_partitions(mut table_infos: *mut List) {
    let mut lc: *mut ListCell;

    foreach!(lc, table_infos, {
        let mut skip: bool = false;
        let mut ancestors: *mut List = NIL;
        let mut lc2: *mut ListCell;
        let table_info: *mut published_rel = lfirst(current_cell!(lc)) as *mut published_rel;

        if get_rel_relispartition((*table_info).relid) {
            ancestors = get_partition_ancestors((*table_info).relid);
        }

        foreach!(lc2, ancestors, {
            let ancestor: Oid = lfirst_oid(current_cell!(lc2));

            if is_ancestor_member_tableinfos(ancestor, table_infos) {
                skip = true;
                break;
            }
        });

        if skip {
            table_infos = foreach_delete_current!(table_infos, lc);
        }
    });
}

/*
 * Returns true if any schema is associated with the publication, false if no
 * schema is associated with the publication.
 */
pub unsafe fn is_schema_publication(pubid: Oid) -> bool {
    let pubschsrel: Relation;
    let mut scankey: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let tup: HeapTuple;
    let result: bool;

    pubschsrel = table_open(PublicationNamespaceRelationId, AccessShareLock);
    ScanKeyInit(
        &mut scankey,
        Anum_pg_publication_namespace_pnpubid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(pubid),
    );

    scan = systable_beginscan(
        pubschsrel,
        PublicationNamespacePnnspidPnpubidIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut scankey,
    );
    tup = systable_getnext(scan);
    result = HeapTupleIsValid(tup);

    systable_endscan(scan);
    table_close(pubschsrel, AccessShareLock);

    result
}

/*
 * Returns true if the relation has column list associated with the
 * publication, false otherwise.
 *
 * If a column list is found, the corresponding bitmap is returned through the
 * cols parameter, if provided. The bitmap is constructed within the given
 * memory context (mcxt).
 */
pub unsafe fn check_and_fetch_column_list(
    pub_: *mut Publication,
    relid: Oid,
    mcxt: MemoryContext,
    cols: *mut *mut Bitmapset,
) -> bool {
    let cftuple: HeapTuple;
    let mut found: bool = false;

    if (*pub_).alltables {
        return false;
    }

    cftuple = SearchSysCache2(
        PUBLICATIONRELMAP,
        ObjectIdGetDatum(relid),
        ObjectIdGetDatum((*pub_).oid),
    );
    if HeapTupleIsValid(cftuple) {
        let cfdatum: Datum;
        let mut isnull: bool = false;

        /* Lookup the column list attribute. */
        cfdatum = SysCacheGetAttr(
            PUBLICATIONRELMAP,
            cftuple,
            Anum_pg_publication_rel_prattrs,
            &mut isnull,
        );

        /* Was a column list found? */
        if !isnull {
            /* Build the column list bitmap in the given memory context. */
            if !cols.is_null() {
                *cols = pub_collist_to_bitmapset(*cols, cfdatum, mcxt);
            }

            found = true;
        }

        ReleaseSysCache(cftuple);
    }

    found
}

/*
 * Gets the relations based on the publication partition option for a specified
 * relation.
 */
pub unsafe fn GetPubPartitionOptionRelations(
    mut result: *mut List,
    pub_partopt: PublicationPartOpt,
    relid: Oid,
) -> *mut List {
    if get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE && pub_partopt != PUBLICATION_PART_ROOT {
        let all_parts: *mut List = find_all_inheritors(relid, NoLock, core::ptr::null_mut());

        if pub_partopt == PUBLICATION_PART_ALL {
            result = list_concat(result, all_parts);
        } else if pub_partopt == PUBLICATION_PART_LEAF {
            let mut lc: *mut ListCell;

            foreach!(lc, all_parts, {
                let partOid: Oid = lfirst_oid(current_cell!(lc));

                if get_rel_relkind(partOid) != RELKIND_PARTITIONED_TABLE {
                    result = lappend_oid(result, partOid);
                }
            });
        } else {
            Assert!(false);
        }
    } else {
        result = lappend_oid(result, relid);
    }

    result
}

/*
 * Returns the relid of the topmost ancestor that is published via this
 * publication if any and set its ancestor level to ancestor_level,
 * otherwise returns InvalidOid.
 *
 * The ancestor_level value allows us to compare the results for multiple
 * publications, and decide which value is higher up.
 *
 * Note that the list of ancestors should be ordered such that the topmost
 * ancestor is at the end of the list.
 */
pub unsafe fn GetTopMostAncestorInPublication(
    puboid: Oid,
    ancestors: *mut List,
    ancestor_level: *mut c_int,
) -> Oid {
    let mut lc: *mut ListCell;
    let mut topmost_relid: Oid = InvalidOid;
    let mut level: c_int = 0;

    /*
     * Find the "topmost" ancestor that is in this publication.
     */
    foreach!(lc, ancestors, {
        let ancestor: Oid = lfirst_oid(current_cell!(lc));
        let apubids: *mut List = GetRelationPublications(ancestor);
        let mut aschemaPubids: *mut List = NIL;

        level += 1;

        if list_member_oid(apubids, puboid) {
            topmost_relid = ancestor;

            if !ancestor_level.is_null() {
                *ancestor_level = level;
            }
        } else {
            aschemaPubids = GetSchemaPublications(get_rel_namespace(ancestor));
            if list_member_oid(aschemaPubids, puboid) {
                topmost_relid = ancestor;

                if !ancestor_level.is_null() {
                    *ancestor_level = level;
                }
            }
        }

        list_free(apubids);
        list_free(aschemaPubids);
    });

    topmost_relid
}

/*
 * attnumstoint2vector
 *		Convert a Bitmapset of AttrNumbers into an int2vector.
 *
 * AttrNumber numbers are 0-based, i.e., not offset by
 * FirstLowInvalidHeapAttributeNumber.
 */
unsafe fn attnumstoint2vector(attrs: *mut Bitmapset) -> *mut int2vector {
    let result: *mut int2vector;
    let n: c_int = bms_num_members(attrs);
    let mut i: c_int = -1;
    let mut j: c_int = 0;

    result = buildint2vector(core::ptr::null(), n);

    loop {
        i = bms_next_member(attrs, i);
        if i < 0 {
            break;
        }
        Assert!(i <= PG_INT16_MAX);

        *(*result).values.as_mut_ptr().add(j as usize) = i as int16;
        j += 1;
    }

    result
}

/* ObjectAddress helpers  TODO(pg-port): catalog/objectaddress.h */
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, classId: Oid, objectId: Oid) {
    addr.classId = classId;
    addr.objectId = objectId;
    addr.objectSubId = 0;
}
unsafe fn ObjectAddressSubSet(addr: &mut ObjectAddress, classId: Oid, objectId: Oid, subId: c_int) {
    addr.classId = classId;
    addr.objectId = objectId;
    addr.objectSubId = subId;
}
/* nodeToString  TODO(pg-port): nodes/outfuncs.c */
unsafe fn nodeToString(_obj: *const Node) -> *mut c_char { crate::nodes::outfuncs::nodeToString(_obj as _) }
/* strVal  TODO(pg-port): nodes/value.h */
unsafe fn strVal(_v: *mut c_void) -> *mut c_char { crate::parser_link_shims::strVal(_v as _) }
fn AttrNumberIsForUserDefinedAttr(attnum: AttrNumber) -> bool {
    attnum > 0
}
/* array accessors  TODO(pg-port): utils/array.h */
unsafe fn ARR_DIMS(_arr: *mut ArrayType) -> *mut c_int {
    core::ptr::null_mut()
}
unsafe fn ARR_DATA_PTR(_arr: *mut ArrayType) -> *mut c_char { crate::utils::array::ARR_DATA_PTR(_arr as _) }

/*
 * Insert new publication / relation mapping.
 */
pub unsafe fn publication_add_relation(
    pubid: Oid,
    pri: *mut PublicationRelInfo,
    if_not_exists: bool,
) -> ObjectAddress {
    let rel: Relation;
    let tup: HeapTuple;
    let mut values: [Datum; Natts_pg_publication_rel] = [0; Natts_pg_publication_rel];
    let mut nulls: [bool; Natts_pg_publication_rel] = [false; Natts_pg_publication_rel];
    let targetrel: Relation = (*pri).relation;
    let relid: Oid = RelationGetRelid(targetrel);
    let pubreloid: Oid;
    let attnums: *mut Bitmapset;
    let pub_: *mut Publication = GetPublication(pubid);
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();
    let mut relids: *mut List = NIL;
    let mut i: c_int;

    rel = table_open(PublicationRelRelationId, RowExclusiveLock);

    /*
     * Check for duplicates. Note that this does not really prevent duplicates,
     * it's here just to provide nicer error message in common case. The real
     * protection is the unique key on the catalog.
     */
    if SearchSysCacheExists2(
        PUBLICATIONRELMAP,
        ObjectIdGetDatum(relid),
        ObjectIdGetDatum(pubid),
    ) {
        table_close(rel, RowExclusiveLock);

        if if_not_exists {
            return InvalidObjectAddress;
        }

        ereport!(
            ERROR,
            errmsg!(
                "relation \"{}\" is already member of publication \"{}\"",
                std::ffi::CStr::from_ptr(RelationGetRelationName(targetrel)).to_string_lossy(),
                std::ffi::CStr::from_ptr((*pub_).name).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }

    check_publication_add_relation(targetrel);

    /* Validate and translate column names into a Bitmapset of attnums. */
    attnums = pub_collist_validate((*pri).relation, (*pri).columns);

    /* Form a tuple. */
    /* memset(values, 0, ...); memset(nulls, false, ...) - done by init above */

    pubreloid = GetNewOidWithIndex(
        rel,
        PublicationRelObjectIndexId,
        Anum_pg_publication_rel_oid,
    );
    values[(Anum_pg_publication_rel_oid - 1) as usize] = ObjectIdGetDatum(pubreloid);
    values[(Anum_pg_publication_rel_prpubid - 1) as usize] = ObjectIdGetDatum(pubid);
    values[(Anum_pg_publication_rel_prrelid - 1) as usize] = ObjectIdGetDatum(relid);

    /* Add qualifications, if available */
    if !(*pri).whereClause.is_null() {
        values[(Anum_pg_publication_rel_prqual - 1) as usize] =
            CStringGetTextDatum(nodeToString((*pri).whereClause));
    } else {
        nulls[(Anum_pg_publication_rel_prqual - 1) as usize] = true;
    }

    /* Add column list, if available */
    if !(*pri).columns.is_null() {
        values[(Anum_pg_publication_rel_prattrs - 1) as usize] =
            PointerGetDatum(attnumstoint2vector(attnums) as *const c_void);
    } else {
        nulls[(Anum_pg_publication_rel_prattrs - 1) as usize] = true;
    }

    tup = heap_form_tuple(RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr());

    /* Insert tuple into catalog. */
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    /* Register dependencies as needed */
    ObjectAddressSet(&mut myself, PublicationRelRelationId, pubreloid);

    /* Add dependency on the publication */
    ObjectAddressSet(&mut referenced, PublicationRelationId, pubid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* Add dependency on the relation */
    ObjectAddressSet(&mut referenced, RelationRelationId, relid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* Add dependency on the objects mentioned in the qualifications */
    if !(*pri).whereClause.is_null() {
        recordDependencyOnSingleRelExpr(
            &myself,
            (*pri).whereClause,
            relid,
            DEPENDENCY_NORMAL,
            DEPENDENCY_NORMAL,
            false,
        );
    }

    /* Add dependency on the columns, if any are listed */
    i = -1;
    loop {
        i = bms_next_member(attnums, i);
        if i < 0 {
            break;
        }
        ObjectAddressSubSet(&mut referenced, RelationRelationId, relid, i);
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    /* Close the table. */
    table_close(rel, RowExclusiveLock);

    /*
     * Invalidate relcache so that publication info is rebuilt.
     *
     * For the partitioned tables, we must invalidate all partitions contained
     * in the respective partition hierarchies, not just the one explicitly
     * mentioned in the publication. This is required because we implicitly
     * publish the child tables when the parent table is published.
     */
    relids = GetPubPartitionOptionRelations(relids, PUBLICATION_PART_ALL, relid);

    InvalidatePublicationRels(relids);

    myself
}

/*
 * pub_collist_validate
 *		Process and validate the 'columns' list and ensure the columns are all
 *		valid to use for a publication.  Checks for and raises an ERROR for any
 *		unknown columns, system columns, duplicate columns, or virtual
 *		generated columns.
 *
 * Looks up each column's attnum and returns a 0-based Bitmapset of the
 * corresponding attnums.
 */
pub unsafe fn pub_collist_validate(targetrel: Relation, columns: *mut List) -> *mut Bitmapset {
    let mut set: *mut Bitmapset = core::ptr::null_mut();
    let mut lc: *mut ListCell;
    let tupdesc: TupleDesc = RelationGetDescr(targetrel);

    foreach!(lc, columns, {
        let colname: *mut c_char = strVal(lfirst(current_cell!(lc)));
        let attnum: AttrNumber = get_attnum(RelationGetRelid(targetrel), colname);

        if attnum == InvalidAttrNumber {
            ereport!(
                ERROR,
                errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(colname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(RelationGetRelationName(targetrel)).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
        }

        if !AttrNumberIsForUserDefinedAttr(attnum) {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot use system column \"{}\" in publication column list",
                    std::ffi::CStr::from_ptr(colname).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE) */
        }

        if (*TupleDescAttr(tupdesc, (attnum - 1) as c_int)).attgenerated
            == ATTRIBUTE_GENERATED_VIRTUAL
        {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot use virtual generated column \"{}\" in publication column list",
                    std::ffi::CStr::from_ptr(colname).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_INVALID_COLUMN_REFERENCE) */
        }

        if bms_is_member(attnum as c_int, set) {
            ereport!(
                ERROR,
                errmsg!(
                    "duplicate column \"{}\" in publication column list",
                    std::ffi::CStr::from_ptr(colname).to_string_lossy()
                )
            );
            /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
        }

        set = bms_add_member(set, attnum as c_int);
    });

    set
}

/*
 * Transform a column list (represented by an array Datum) to a bitmapset.
 *
 * If columns isn't NULL, add the column numbers to that set.
 *
 * If mcxt isn't NULL, build the bitmapset in that context.
 */
pub unsafe fn pub_collist_to_bitmapset(
    columns: *mut Bitmapset,
    pubcols: Datum,
    mcxt: MemoryContext,
) -> *mut Bitmapset {
    let mut result: *mut Bitmapset = columns;
    let arr: *mut ArrayType;
    let nelems: c_int;
    let elems: *mut int16;
    let mut oldcxt: MemoryContext = core::ptr::null_mut();

    arr = DatumGetArrayTypeP(pubcols);
    nelems = *ARR_DIMS(arr).add(0);
    elems = ARR_DATA_PTR(arr) as *mut int16;

    /* If a memory context was specified, switch to it. */
    if !mcxt.is_null() {
        oldcxt = MemoryContextSwitchTo(mcxt);
    }

    for i in 0..nelems {
        result = bms_add_member(result, *elems.add(i as usize) as c_int);
    }

    if !mcxt.is_null() {
        MemoryContextSwitchTo(oldcxt);
    }

    result
}

/*
 * Returns a bitmap representing the columns of the specified table.
 *
 * Generated columns are included if include_gencols_type is
 * PUBLISH_GENCOLS_STORED.
 */
pub unsafe fn pub_form_cols_map(
    relation: Relation,
    include_gencols_type: PublishGencolsType,
) -> *mut Bitmapset {
    let mut result: *mut Bitmapset = core::ptr::null_mut();
    let desc: TupleDesc = RelationGetDescr(relation);

    for i in 0..TupleDescNatts(desc) {
        let att: Form_pg_attribute = TupleDescAttr(desc, i);

        if (*att).attisdropped {
            continue;
        }

        if (*att).attgenerated != 0 {
            /* We only support replication of STORED generated cols. */
            if (*att).attgenerated != ATTRIBUTE_GENERATED_STORED {
                continue;
            }

            /* User hasn't requested to replicate STORED generated cols. */
            if include_gencols_type != PUBLISH_GENCOLS_STORED as PublishGencolsType {
                continue;
            }
        }

        result = bms_add_member(result, (*att).attnum as c_int);
    }

    result
}

/*
 * Insert new publication / schema mapping.
 */
pub unsafe fn publication_add_schema(
    pubid: Oid,
    schemaid: Oid,
    if_not_exists: bool,
) -> ObjectAddress {
    let rel: Relation;
    let tup: HeapTuple;
    let mut values: [Datum; Natts_pg_publication_namespace] = [0; Natts_pg_publication_namespace];
    let mut nulls: [bool; Natts_pg_publication_namespace] =
        [false; Natts_pg_publication_namespace];
    let psschid: Oid;
    let pub_: *mut Publication = GetPublication(pubid);
    let mut schemaRels: *mut List = NIL;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();

    rel = table_open(PublicationNamespaceRelationId, RowExclusiveLock);

    /*
     * Check for duplicates. Note that this does not really prevent duplicates,
     * it's here just to provide nicer error message in common case. The real
     * protection is the unique key on the catalog.
     */
    if SearchSysCacheExists2(
        PUBLICATIONNAMESPACEMAP,
        ObjectIdGetDatum(schemaid),
        ObjectIdGetDatum(pubid),
    ) {
        table_close(rel, RowExclusiveLock);

        if if_not_exists {
            return InvalidObjectAddress;
        }

        ereport!(
            ERROR,
            errmsg!(
                "schema \"{}\" is already member of publication \"{}\"",
                std::ffi::CStr::from_ptr(get_namespace_name(schemaid)).to_string_lossy(),
                std::ffi::CStr::from_ptr((*pub_).name).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }

    check_publication_add_schema(schemaid);

    /* Form a tuple */
    /* memset(values, 0, ...); memset(nulls, false, ...) - done by init above */

    psschid = GetNewOidWithIndex(
        rel,
        PublicationNamespaceObjectIndexId,
        Anum_pg_publication_namespace_oid,
    );
    values[(Anum_pg_publication_namespace_oid - 1) as usize] = ObjectIdGetDatum(psschid);
    values[(Anum_pg_publication_namespace_pnpubid - 1) as usize] = ObjectIdGetDatum(pubid);
    values[(Anum_pg_publication_namespace_pnnspid - 1) as usize] = ObjectIdGetDatum(schemaid);

    tup = heap_form_tuple(RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr());

    /* Insert tuple into catalog */
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    ObjectAddressSet(&mut myself, PublicationNamespaceRelationId, psschid);

    /* Add dependency on the publication */
    ObjectAddressSet(&mut referenced, PublicationRelationId, pubid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* Add dependency on the schema */
    ObjectAddressSet(&mut referenced, NamespaceRelationId, schemaid);
    recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);

    /* Close the table */
    table_close(rel, RowExclusiveLock);

    /*
     * Invalidate relcache so that publication info is rebuilt. See
     * publication_add_relation for why we need to consider all the partitions.
     */
    schemaRels = GetSchemaPublicationRelations(schemaid, PUBLICATION_PART_ALL);
    InvalidatePublicationRels(schemaRels);

    myself
}

/* additional helpers  TODO(pg-port) */
unsafe fn NameStr(name: *mut NameData) -> *mut c_char {
    (*name).data.as_mut_ptr()
}
fn OidIsValid(objectId: Oid) -> bool {
    objectId != InvalidOid
}

/* Gets list of publication oids for a relation */
pub unsafe fn GetRelationPublications(relid: Oid) -> *mut List {
    let mut result: *mut List = NIL;
    let pubrellist: *mut CatCList;
    let mut i: c_int;

    /* Find all publications associated with the relation. */
    pubrellist = SearchSysCacheList1(PUBLICATIONRELMAP, ObjectIdGetDatum(relid));
    i = 0;
    while i < (*pubrellist).n_members {
        let tup: HeapTuple = &mut (**(*pubrellist).members.add(i as usize)).tuple as *mut _ as HeapTuple;
        let pubid: Oid = (*(GETSTRUCT(tup) as Form_pg_publication_rel)).prpubid;

        result = lappend_oid(result, pubid);
        i += 1;
    }

    ReleaseSysCacheList(pubrellist);

    result
}

/*
 * Gets list of relation oids for a publication.
 *
 * This should only be used FOR TABLE publications, the FOR ALL TABLES should
 * use GetAllTablesPublicationRelations().
 */
pub unsafe fn GetPublicationRelations(pubid: Oid, pub_partopt: PublicationPartOpt) -> *mut List {
    let mut result: *mut List;
    let pubrelsrel: Relation;
    let mut scankey: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    /* Find all publications associated with the relation. */
    pubrelsrel = table_open(PublicationRelRelationId, AccessShareLock);

    ScanKeyInit(
        &mut scankey,
        Anum_pg_publication_rel_prpubid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(pubid),
    );

    scan = systable_beginscan(
        pubrelsrel,
        PublicationRelPrpubidIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut scankey,
    );

    result = NIL;
    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let pubrel: Form_pg_publication_rel = GETSTRUCT(tup) as Form_pg_publication_rel;
        result = GetPubPartitionOptionRelations(result, pub_partopt, (*pubrel).prrelid);
    }

    systable_endscan(scan);
    table_close(pubrelsrel, AccessShareLock);

    /* Now sort and de-duplicate the result list */
    list_sort(result, list_oid_cmp);
    list_deduplicate_oid(result);

    result
}

/*
 * Gets list of publication oids for publications marked as FOR ALL TABLES.
 */
pub unsafe fn GetAllTablesPublications() -> *mut List {
    let mut result: *mut List;
    let rel: Relation;
    let mut scankey: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    /* Find all publications that are marked as for all tables. */
    rel = table_open(PublicationRelationId, AccessShareLock);

    ScanKeyInit(
        &mut scankey,
        Anum_pg_publication_puballtables,
        BTEqualStrategyNumber,
        F_BOOLEQ,
        BoolGetDatum(true),
    );

    scan = systable_beginscan(rel, InvalidOid, false, core::ptr::null_mut(), 1, &mut scankey);

    result = NIL;
    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let oid: Oid = (*(GETSTRUCT(tup) as Form_pg_publication)).oid;

        result = lappend_oid(result, oid);
    }

    systable_endscan(scan);
    table_close(rel, AccessShareLock);

    result
}

/*
 * Gets list of all relation published by FOR ALL TABLES publication(s).
 *
 * If the publication publishes partition changes via their respective root
 * partitioned tables, we must exclude partitions in favor of including the
 * root partitioned tables.
 */
pub unsafe fn GetAllTablesPublicationRelations(pubviaroot: bool) -> *mut List {
    let classRel: Relation;
    let mut key: [ScanKeyData; 1] = [core::mem::zeroed()];
    let mut scan: TableScanDesc;
    let mut tuple: HeapTuple;
    let mut result: *mut List = NIL;

    classRel = table_open(RelationRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_class_relkind,
        BTEqualStrategyNumber,
        F_CHAREQ,
        CharGetDatum(RELKIND_RELATION),
    );

    scan = table_beginscan_catalog(classRel, 1, key.as_mut_ptr());

    loop {
        tuple = heap_getnext(scan, ForwardScanDirection);
        if tuple.is_null() {
            break;
        }
        let relForm: Form_pg_class = GETSTRUCT(tuple) as Form_pg_class;
        let relid: Oid = (*relForm).oid;

        if is_publishable_class(relid, relForm) && !((*relForm).relispartition && pubviaroot) {
            result = lappend_oid(result, relid);
        }
    }

    table_endscan(scan);

    if pubviaroot {
        ScanKeyInit(
            &mut key[0],
            Anum_pg_class_relkind,
            BTEqualStrategyNumber,
            F_CHAREQ,
            CharGetDatum(RELKIND_PARTITIONED_TABLE),
        );

        scan = table_beginscan_catalog(classRel, 1, key.as_mut_ptr());

        loop {
            tuple = heap_getnext(scan, ForwardScanDirection);
            if tuple.is_null() {
                break;
            }
            let relForm: Form_pg_class = GETSTRUCT(tuple) as Form_pg_class;
            let relid: Oid = (*relForm).oid;

            if is_publishable_class(relid, relForm) && !(*relForm).relispartition {
                result = lappend_oid(result, relid);
            }
        }

        table_endscan(scan);
    }

    table_close(classRel, AccessShareLock);
    result
}

/*
 * Gets the list of schema oids for a publication.
 *
 * This should only be used FOR TABLES IN SCHEMA publications.
 */
pub unsafe fn GetPublicationSchemas(pubid: Oid) -> *mut List {
    let mut result: *mut List = NIL;
    let pubschsrel: Relation;
    let mut scankey: ScanKeyData = core::mem::zeroed();
    let scan: SysScanDesc;
    let mut tup: HeapTuple;

    /* Find all schemas associated with the publication */
    pubschsrel = table_open(PublicationNamespaceRelationId, AccessShareLock);

    ScanKeyInit(
        &mut scankey,
        Anum_pg_publication_namespace_pnpubid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(pubid),
    );

    scan = systable_beginscan(
        pubschsrel,
        PublicationNamespacePnnspidPnpubidIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut scankey,
    );
    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        let pubsch: Form_pg_publication_namespace =
            GETSTRUCT(tup) as Form_pg_publication_namespace;

        result = lappend_oid(result, (*pubsch).pnnspid);
    }

    systable_endscan(scan);
    table_close(pubschsrel, AccessShareLock);

    result
}

/*
 * Gets the list of publication oids associated with a specified schema.
 */
pub unsafe fn GetSchemaPublications(schemaid: Oid) -> *mut List {
    let mut result: *mut List = NIL;
    let pubschlist: *mut CatCList;
    let mut i: c_int;

    /* Find all publications associated with the schema */
    pubschlist = SearchSysCacheList1(PUBLICATIONNAMESPACEMAP, ObjectIdGetDatum(schemaid));
    i = 0;
    while i < (*pubschlist).n_members {
        let tup: HeapTuple =
            &mut (**(*pubschlist).members.add(i as usize)).tuple as *mut _ as HeapTuple;
        let pubid: Oid = (*(GETSTRUCT(tup) as Form_pg_publication_namespace)).pnpubid;

        result = lappend_oid(result, pubid);
        i += 1;
    }

    ReleaseSysCacheList(pubschlist);

    result
}

/*
 * Get the list of publishable relation oids for a specified schema.
 */
pub unsafe fn GetSchemaPublicationRelations(
    schemaid: Oid,
    pub_partopt: PublicationPartOpt,
) -> *mut List {
    let classRel: Relation;
    let mut key: [ScanKeyData; 1] = [core::mem::zeroed()];
    let scan: TableScanDesc;
    let mut tuple: HeapTuple;
    let mut result: *mut List = NIL;

    Assert!(OidIsValid(schemaid));

    classRel = table_open(RelationRelationId, AccessShareLock);

    ScanKeyInit(
        &mut key[0],
        Anum_pg_class_relnamespace,
        BTEqualStrategyNumber,
        F_OIDEQ,
        schemaid as Datum,
    );

    /* get all the relations present in the specified schema */
    scan = table_beginscan_catalog(classRel, 1, key.as_mut_ptr());
    loop {
        tuple = heap_getnext(scan, ForwardScanDirection);
        if tuple.is_null() {
            break;
        }
        let relForm: Form_pg_class = GETSTRUCT(tuple) as Form_pg_class;
        let relid: Oid = (*relForm).oid;
        let relkind: c_char;

        if !is_publishable_class(relid, relForm) {
            continue;
        }

        relkind = get_rel_relkind(relid);
        if relkind == RELKIND_RELATION {
            result = lappend_oid(result, relid);
        } else if relkind == RELKIND_PARTITIONED_TABLE {
            let mut partitionrels: *mut List = NIL;

            /*
             * It is quite possible that some of the partitions are in a
             * different schema than the parent table, so we need to get such
             * partitions separately.
             */
            partitionrels =
                GetPubPartitionOptionRelations(partitionrels, pub_partopt, (*relForm).oid);
            result = list_concat_unique_oid(result, partitionrels);
        }
    }

    table_endscan(scan);
    table_close(classRel, AccessShareLock);
    result
}

/*
 * Gets the list of all relations published by FOR TABLES IN SCHEMA
 * publication.
 */
pub unsafe fn GetAllSchemaPublicationRelations(
    pubid: Oid,
    pub_partopt: PublicationPartOpt,
) -> *mut List {
    let mut result: *mut List = NIL;
    let pubschemalist: *mut List = GetPublicationSchemas(pubid);
    let mut cell: *mut ListCell;

    foreach!(cell, pubschemalist, {
        let schemaid: Oid = lfirst_oid(current_cell!(cell));
        let mut schemaRels: *mut List = NIL;

        schemaRels = GetSchemaPublicationRelations(schemaid, pub_partopt);
        result = list_concat(result, schemaRels);
    });

    result
}

/*
 * Get publication using oid
 *
 * The Publication struct and its data are palloc'ed here.
 */
pub unsafe fn GetPublication(pubid: Oid) -> *mut Publication {
    let tup: HeapTuple;
    let pub_: *mut Publication;
    let pubform: Form_pg_publication;

    tup = SearchSysCache1(PUBLICATIONOID, ObjectIdGetDatum(pubid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for publication {}", pubid);
    }

    pubform = GETSTRUCT(tup) as Form_pg_publication;

    pub_ = palloc(core::mem::size_of::<Publication>()) as *mut Publication;
    (*pub_).oid = pubid;
    (*pub_).name = pstrdup(NameStr(&mut (*pubform).pubname));
    (*pub_).alltables = (*pubform).puballtables;
    (*pub_).pubactions.pubinsert = (*pubform).pubinsert;
    (*pub_).pubactions.pubupdate = (*pubform).pubupdate;
    (*pub_).pubactions.pubdelete = (*pubform).pubdelete;
    (*pub_).pubactions.pubtruncate = (*pubform).pubtruncate;
    (*pub_).pubviaroot = (*pubform).pubviaroot;
    (*pub_).pubgencols_type = (*pubform).pubgencols as PublishGencolsType;

    ReleaseSysCache(tup);

    pub_
}

/*
 * Get Publication using name.
 */
pub unsafe fn GetPublicationByName(pubname: *const c_char, missing_ok: bool) -> *mut Publication {
    let oid: Oid;

    oid = get_publication_oid(pubname, missing_ok);

    if OidIsValid(oid) {
        GetPublication(oid)
    } else {
        core::ptr::null_mut()
    }
}

/*
 * Get information of the tables in the given publication array.
 *
 * Returns pubid, relid, column list, row filter for each table.
 */
pub unsafe fn pg_get_publication_tables(fcinfo: FunctionCallInfo) -> Datum {
    const NUM_PUBLICATION_TABLES_ELEM: usize = 4;
    let mut funcctx: *mut FuncCallContext;
    let mut table_infos: *mut List = NIL;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL(fcinfo) {
        let tupdesc: TupleDesc;
        let oldcontext: MemoryContext;
        let arr: *mut ArrayType;
        let mut elems: *mut Datum = core::ptr::null_mut();
        let mut nelems: c_int = 0;
        let mut viaroot: bool = false;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT(fcinfo);

        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        /*
         * Deconstruct the parameter into elements where each element is a
         * publication name.
         */
        arr = PG_GETARG_ARRAYTYPE_P(0);
        deconstruct_array_builtin(
            arr,
            TEXTOID,
            &mut elems,
            core::ptr::null_mut(),
            &mut nelems,
        );

        /* Get Oids of tables from each publication. */
        for i in 0..nelems {
            let pub_elem: *mut Publication;
            let mut pub_elem_tables: *mut List;
            let mut lc: *mut ListCell;

            pub_elem = GetPublicationByName(TextDatumGetCString(*elems.add(i as usize)), false);

            /*
             * Publications support partitioned tables. If
             * publish_via_partition_root is false, all changes are replicated
             * using leaf partition identity and schema, so we only need those.
             * Otherwise, get the partitioned table itself.
             */
            if (*pub_elem).alltables {
                pub_elem_tables = GetAllTablesPublicationRelations((*pub_elem).pubviaroot);
            } else {
                let relids: *mut List;
                let schemarelids: *mut List;

                relids = GetPublicationRelations(
                    (*pub_elem).oid,
                    if (*pub_elem).pubviaroot {
                        PUBLICATION_PART_ROOT
                    } else {
                        PUBLICATION_PART_LEAF
                    },
                );
                schemarelids = GetAllSchemaPublicationRelations(
                    (*pub_elem).oid,
                    if (*pub_elem).pubviaroot {
                        PUBLICATION_PART_ROOT
                    } else {
                        PUBLICATION_PART_LEAF
                    },
                );
                pub_elem_tables = list_concat_unique_oid(relids, schemarelids);
            }

            /*
             * Record the published table and the corresponding publication so
             * that we can get row filters and column lists later.
             *
             * When a table is published by multiple publications, to obtain all
             * row filters and column lists, the structure related to this table
             * will be recorded multiple times.
             */
            foreach!(lc, pub_elem_tables, {
                let table_info: *mut published_rel =
                    palloc(core::mem::size_of::<published_rel>()) as *mut published_rel;

                (*table_info).relid = lfirst_oid(current_cell!(lc));
                (*table_info).pubid = (*pub_elem).oid;
                table_infos = lappend(table_infos, table_info as *mut c_void);
            });

            /* At least one publication is using publish_via_partition_root. */
            if (*pub_elem).pubviaroot {
                viaroot = true;
            }
        }

        /*
         * If the publication publishes partition changes via their respective
         * root partitioned tables, we must exclude partitions in favor of
         * including the root partitioned tables. Otherwise, the function could
         * return both the child and parent tables which could cause data of the
         * child table to be double-published on the subscriber side.
         */
        if viaroot {
            filter_partitions(table_infos);
        }

        /* Construct a tuple descriptor for the result rows. */
        tupdesc = CreateTemplateTupleDesc(NUM_PUBLICATION_TABLES_ELEM as c_int);
        TupleDescInitEntry(tupdesc, 1, c"pubid".as_ptr(), OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2, c"relid".as_ptr(), OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3, c"attrs".as_ptr(), INT2VECTOROID, -1, 0);
        TupleDescInitEntry(tupdesc, 4, c"qual".as_ptr(), PG_NODE_TREEOID, -1, 0);

        (*funcctx).tuple_desc = BlessTupleDesc(tupdesc);
        (*funcctx).user_fctx = table_infos as *mut c_void;

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP(fcinfo);
    table_infos = (*funcctx).user_fctx as *mut List;

    if ((*funcctx).call_cntr as c_int) < list_length(table_infos) {
        let mut pubtuple: HeapTuple = core::ptr::null_mut();
        let rettuple: HeapTuple;
        let pub_: *mut Publication;
        let table_info: *mut published_rel =
            list_nth(table_infos, (*funcctx).call_cntr as c_int) as *mut published_rel;
        let relid: Oid = (*table_info).relid;
        let schemaid: Oid = get_rel_namespace(relid);
        let mut values: [Datum; NUM_PUBLICATION_TABLES_ELEM] = [0; NUM_PUBLICATION_TABLES_ELEM];
        let mut nulls: [bool; NUM_PUBLICATION_TABLES_ELEM] = [false; NUM_PUBLICATION_TABLES_ELEM];

        /*
         * Form tuple with appropriate data.
         */

        pub_ = GetPublication((*table_info).pubid);

        values[0] = ObjectIdGetDatum((*pub_).oid);
        values[1] = ObjectIdGetDatum(relid);

        /*
         * We don't consider row filters or column lists for FOR ALL TABLES or
         * FOR TABLES IN SCHEMA publications.
         */
        if !(*pub_).alltables
            && !SearchSysCacheExists2(
                PUBLICATIONNAMESPACEMAP,
                ObjectIdGetDatum(schemaid),
                ObjectIdGetDatum((*pub_).oid),
            )
        {
            pubtuple = SearchSysCacheCopy2(
                PUBLICATIONRELMAP,
                ObjectIdGetDatum(relid),
                ObjectIdGetDatum((*pub_).oid),
            );
        }

        if HeapTupleIsValid(pubtuple) {
            /* Lookup the column list attribute. */
            values[2] = SysCacheGetAttr(
                PUBLICATIONRELMAP,
                pubtuple,
                Anum_pg_publication_rel_prattrs,
                &mut nulls[2],
            );

            /* Null indicates no filter. */
            values[3] = SysCacheGetAttr(
                PUBLICATIONRELMAP,
                pubtuple,
                Anum_pg_publication_rel_prqual,
                &mut nulls[3],
            );
        } else {
            nulls[2] = true;
            nulls[3] = true;
        }

        /* Show all columns when the column list is not specified. */
        if nulls[2] {
            let rel: Relation = table_open(relid, AccessShareLock);
            let mut nattnums: c_int = 0;
            let attnums: *mut int16;
            let desc: TupleDesc = RelationGetDescr(rel);

            attnums = palloc(TupleDescNatts(desc) as usize * core::mem::size_of::<int16>())
                as *mut int16;

            for i in 0..TupleDescNatts(desc) {
                let att: Form_pg_attribute = TupleDescAttr(desc, i);

                if (*att).attisdropped {
                    continue;
                }

                if (*att).attgenerated != 0 {
                    /* We only support replication of STORED generated cols. */
                    if (*att).attgenerated != ATTRIBUTE_GENERATED_STORED {
                        continue;
                    }

                    /* User hasn't requested to replicate STORED generated cols. */
                    if (*pub_).pubgencols_type != PUBLISH_GENCOLS_STORED as PublishGencolsType {
                        continue;
                    }
                }

                *attnums.add(nattnums as usize) = (*att).attnum;
                nattnums += 1;
            }

            if nattnums > 0 {
                values[2] = PointerGetDatum(buildint2vector(attnums, nattnums) as *const c_void);
                nulls[2] = false;
            }

            table_close(rel, AccessShareLock);
        }

        rettuple = heap_form_tuple((*funcctx).tuple_desc, values.as_mut_ptr(), nulls.as_mut_ptr());

        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(rettuple));
    }

    SRF_RETURN_DONE(funcctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // pubname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_publication, pubname), 4);
        // pubowner follows the NAMEDATALEN-byte pubname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_publication, pubowner),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_publication>()
                >= core::mem::offset_of!(FormData_pg_publication, pubgencols)
                    + core::mem::size_of::<c_char>()
        );
    }
}

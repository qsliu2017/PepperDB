/*-------------------------------------------------------------------------
 *
 * heap.rs
 *    code to create and destroy POSTGRES heap relations
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/catalog/heap.c
 *
 *
 * INTERFACE ROUTINES
 *        heap_create()            - Create an uncataloged heap relation
 *        heap_create_with_catalog() - Create a cataloged relation
 *        heap_drop_with_catalog() - Removes named relation from catalogs
 *
 * NOTES
 *    this code taken from access/heap/create.c, which contains
 *    the old heap_create_with_catalog, amcreate, and amdestroy.
 *    those routines will soon call these routines using the function
 *    manager,
 *    just like the poorly named "NewXXX" routines do.  The
 *    "New" routines are all going to die soon, once and for all!
 *        -cim 1/13/91
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use core::ffi::{c_char, c_int};

// -- core types -----------------------------------------------------------
use crate::utils::rel::{Relation, RelationData};
use crate::access::common::tupdesc::{
    TupleDesc, TupleDescData, TupleDescAttr,
    CreateTupleDesc, FreeTupleDesc,
    TYPALIGN_SHORT, TYPALIGN_INT, TYPALIGN_DOUBLE,
    TYPSTORAGE_PLAIN, TYPSTORAGE_EXTENDED,
};
use crate::access::htup_details::{
    HeapTuple, HeapTupleData, HeapTupleIsValid, GETSTRUCT, fastgetattr,
};
use crate::access::common::heaptuple::{
    heap_form_tuple, heap_modify_tuple, heap_freetuple, heap_copytuple,
    SelfItemPointerAttributeNumber, MinTransactionIdAttributeNumber,
    MinCommandIdAttributeNumber, MaxTransactionIdAttributeNumber,
    MaxCommandIdAttributeNumber, TableOidAttributeNumber,
};
use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::storage::lockdefs::{
    LOCKMODE, NoLock, AccessShareLock, RowShareLock, RowExclusiveLock,
    ShareUpdateExclusiveLock, ShareLock, ExclusiveLock, AccessExclusiveLock,
};
use crate::nodes::pg_list::{
    List, ListCell,
    list_head, lnext, lfirst, lfirst_oid, lfirst_int,
    lappend, lappend_oid, lappend_int,
    list_length, list_member_oid, list_copy,
    list_delete_last, list_delete_cell, list_delete_nth_cell,
    list_nth, list_append_unique_oid,
    list_union, list_free, list_sort, list_deduplicate_oid,
    list_oid_cmp, list_sort_comparator,
};
// NIL imported from objectaddress_impl below
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};
use crate::access::stratnum::StrategyNumber;
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{Var, OnCommitAction};
use crate::nodes::parsenodes::{Constraint, RelFileNumber};
use crate::nodes::parsenodes::ConstrType::*;   // CONSTR_CHECK, CONSTR_DEFAULT, CONSTR_NOTNULL, ...
use crate::access::transam::{InvalidTransactionId};
use crate::access::transam::multixact::InvalidMultiXactId;
use crate::access::transam::xlogprefetcher::InvalidRelFileNumber;
use crate::access::common::relation::{relation_open, relation_close};
use crate::access::index::genam::{systable_beginscan, systable_endscan, SysScanDesc};
use crate::access::index::genam::systable_getnext as _systable_getnext_void;
/// Wrapper: cast genam's *mut c_void result to *mut HeapTupleData
#[inline]
unsafe fn systable_getnext(scan: SysScanDesc) -> *mut HeapTupleData {
    _systable_getnext_void(scan) as *mut HeapTupleData
}
use crate::access::index::indexam::{index_open, index_close};
use crate::access::table::table::{table_open, table_close};
use crate::catalog::pg_attribute::{
    FormData_pg_attribute, Form_pg_attribute, FormExtraData_pg_attribute,
    ATTRIBUTE_GENERATED_VIRTUAL,
};
use crate::catalog::pg_class::{
    FormData_pg_class, Form_pg_class,
    RELKIND_RELATION, RELKIND_INDEX, RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
    RELKIND_VIEW, RELKIND_MATVIEW, RELKIND_COMPOSITE_TYPE,
    RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_PARTITIONED_INDEX,
};
use crate::catalog::pg_type::{
    TYPTYPE_BASE, TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN, TYPTYPE_PSEUDO, TYPTYPE_RANGE,
    TYPCATEGORY_ARRAY, TYPCATEGORY_COMPOSITE,
};
use crate::catalog::pg_constraint::{
    FormData_pg_constraint, Form_pg_constraint,
    CONSTRAINT_CHECK, CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL,
};
use crate::catalog::pg_type_d::{TIDOID, XIDOID, CIDOID, OIDOID, ANYARRAYOID, RECORDOID};
const RECORDARRAYOID: Oid = 2287; // pg_type_d.h: RECORDARRAYOID
use crate::catalog::pg_known_oids::GLOBALTABLESPACE_OID;
use crate::catalog::catalog::{
    IsCatalogNamespace, IsToastNamespace,
    GetNewRelFileNumber, FirstUnpinnedObjectId,
};
use crate::catalog::catalog_oids::{
    AttributeRelationId, RelationRelationId, TypeRelationId, CollationRelationId,
    NamespaceRelationId, AccessMethodRelationId, ConstraintRelationId,
    ForeignTableRelationId, InheritsRelationId, StatisticRelationId,
    PartitionedRelationId, OperatorClassRelationId,
};
// Index OIDs not yet exported from catalog_oids
const AttributeRelidNumIndexId: Oid        = 2658; // pg_attribute_relid_attnam_index
const AttributeRelidAttnoIndexId: Oid      = 2659; // pg_attribute_relid_attnum_index
const InheritsRelidSeqnoIndexId: Oid       = 2655; // pg_inherits_relid_seqno_index
const ConstraintOidIndexId: Oid            = 2666; // pg_constraint_oid_index
const ConstraintRelidTypidNameIndexId: Oid = 2664; // pg_constraint_conrelid_contypid_conname_index
const StatisticRelidAttnumInhIndexId: Oid  = 2696; // pg_statistic_relid_att_inh_index
use crate::catalog::indexing::{
    CatalogIndexState, MAX_CATALOG_MULTI_INSERT_BYTES,
    CatalogOpenIndexes, CatalogCloseIndexes,
    CatalogTupleInsert, CatalogTupleInsertWithInfo,
    CatalogTuplesMultiInsertWithInfo, CatalogTupleUpdate, CatalogTupleDelete,
};
use crate::catalog::objectaddress_impl::{
    ObjectAddress, OidIsValid,
    ObjectIdGetDatum, Int16GetDatum, Int32GetDatum,
    BTEqualStrategyNumber,
    F_OIDEQ,
    TEXTOID,
    INVALID_OBJECT_ADDRESS, ObjectAddressSet,
    GetSysCacheOid2,
    SearchSysCache1, SearchSysCacheCopy1,
    ReleaseSysCache,
    table_open as _table_open_stub,   // hide stub; real is from access::table
    table_close as _table_close_stub,
    RelationGetDescr as _rd_stub,     // hide stub; real below
    RelationGetRelid, RelationGetRelationName,
    ScanKeyInit as _ski_stub,         // hide stub; real from access::common::scankey
    strVal, linitial,
    lappend as _lappend_stub,
    list_length as _ll_stub,
    NIL,
    format_type_be,
    TextDatumGetCString, CStringGetTextDatum,
    heap_copytuple as _hct_stub,
    SearchSysCache1 as _sc1_stub,
    ReleaseSysCache as _rs_stub,
};
// Datum conversion functions -- public in postgres.rs
use crate::postgres::{
    BoolGetDatum, CharGetDatum, PointerGetDatum,
    TransactionIdGetDatum, Float4GetDatum,
};
use crate::catalog::storage::{RelationCreateStorage, RelationDropStorage};
// NodeTag from nodes.rs (proper enum with T_Const, T_Var, T_List, etc.)
// crate::catalog::namespace has `type NodeTag = u32` which is incompatible
use crate::nodes::nodes::NodeTag;
use crate::catalog::catalog_oids;
use crate::utils::cache::relcache::{
    RelationBuildLocalRelation, RelationForgetRelation, RelationGetIndexList,
    InvalidRelFileNumber as _rfn_relcache,
    // Natts_pg_class, Natts_pg_attribute - use local usize consts instead
    // Anum_pg_attribute_attrelid, Anum_pg_attribute_attnum, Anum_pg_attribute_attmissingval
    //   - use local AttrNumber consts instead (to get i16 type)
    Anum_pg_class_oid,
    RELKIND_HAS_TABLE_AM, RELKIND_HAS_STORAGE,
    IsBinaryUpgrade,
};
use crate::utils::init::globals::MyDatabaseTableSpace;
use crate::utils::cache::syscache::{
    SearchSysCache2, SearchSysCacheAttName,
    SysCacheGetAttr,
    GetSysCacheOid as _gscoid,
};
use crate::utils::cache::lsyscache::{
    get_namespace_name, get_attname, get_attnum, get_attgenerated,
    get_relname_relid, get_rel_name,
    get_typtype, getBaseType, get_element_type,
    get_range_subtype, get_range_collation,
    get_typ_typrelid, type_is_collatable,
    format_type_be as _ftb_lsyscache,
    Anum_pg_class_oid as _anum_pg_class_oid_lsc,
    Anum_pg_attribute_attoptions,
};
use crate::utils::adt::arrayfuncs::construct_array;
use crate::utils::adt::int::buildint2vector;
use crate::utils::adt::oid::buildoidvector;
use crate::utils::adt::name::namestrcpy;
use crate::utils::builtins::{
    CStringGetTextDatum as _cstd_builtins,
    TextDatumGetCString as _tdc_builtins,
    buildint2vector as _biv_builtins,
    buildoidvector as _bov_builtins,
    namestrcpy as _ns_builtins,
};
use crate::common::int::pg_add_s16_overflow;
use crate::miscadmin::{IsBootstrapProcessingMode, IsNormalProcessingMode, check_stack_depth};
use crate::catalog::catalog::IsCatalogNamespace as _icns;
use crate::storage::lmgr::lmgr::LockRelationOid;
use crate::utils::activity::pgstat_relation::{pgstat_create_relation, pgstat_drop_relation};
use crate::nodes::nodes::Node as _NodeAlias;
use crate::nodes::nodeFuncs::{
    exprType, check_functions_in_node,
    expression_tree_walker,
};
// nodeToString: no outfuncs module yet -- use stub
unsafe fn nodeToString(obj: *mut c_void) -> *mut c_char { core::ptr::null_mut() /* TODO(pg-port) */ }
use crate::nodes::read::stringToNode;
use crate::nodes::equalfuncs::equal;
use crate::nodes::primnodes::Expr;
use crate::optimizer::optimizer::{
    contain_mutable_functions_after_planning, contain_var_clause,
    pull_var_clause,
};
use crate::parser::parse_node::{ParseState, make_parsestate};
use crate::parser::parse_collate::assign_expr_collations;
use crate::parser::parsetree::rt_fetch;
use crate::executor::tuptable::TupleTableSlot;
use crate::executor::execTuples::{
    TTSOpsHeapTuple, MakeSingleTupleTableSlot, ExecDropSingleTupleTableSlot,
    ExecStoreVirtualTuple,
};
use crate::executor::tuptable::ExecClearTuple;
use crate::catalog::storage::RelationTruncate;
use crate::utils::rel::RelationGetDescr;

// -- pg_attribute Anum stubs (not yet in a shared generated file) ---------
// Real values from src/include/catalog/pg_attribute.h
// attrelid/attnum/attmissingval come from relcache as i32; shadow with AttrNumber
const Anum_pg_attribute_attrelid: AttrNumber        = 1; // shadows relcache i32 import
const Anum_pg_attribute_attnum: AttrNumber          = 6; // shadows relcache i32 import
const Anum_pg_attribute_attmissingval: AttrNumber   = 43; // shadows relcache i32 import
const Anum_pg_attribute_attname: AttrNumber         = 2;
const Anum_pg_attribute_atttypid: AttrNumber        = 3;
const Anum_pg_attribute_attlen: AttrNumber          = 4;
const Anum_pg_attribute_attndims: AttrNumber        = 5;
const Anum_pg_attribute_atttypmod: AttrNumber       = 7;
const Anum_pg_attribute_attbyval: AttrNumber        = 8;
const Anum_pg_attribute_attalign: AttrNumber        = 9;
const Anum_pg_attribute_attstorage: AttrNumber      = 10;
const Anum_pg_attribute_attcompression: AttrNumber  = 11;
const Anum_pg_attribute_attnotnull: AttrNumber      = 12;
const Anum_pg_attribute_atthasdef: AttrNumber       = 13;
const Anum_pg_attribute_atthasmissing: AttrNumber   = 14;
const Anum_pg_attribute_attidentity: AttrNumber     = 15;
const Anum_pg_attribute_attgenerated: AttrNumber    = 16;
const Anum_pg_attribute_attisdropped: AttrNumber    = 17;
const Anum_pg_attribute_attislocal: AttrNumber      = 18;
const Anum_pg_attribute_attinhcount: AttrNumber     = 19;
const Anum_pg_attribute_attcollation: AttrNumber    = 20;
const Anum_pg_attribute_attstattarget: AttrNumber   = 21;
const Anum_pg_attribute_attacl: AttrNumber          = 22;
const Anum_pg_attribute_attoptions_local: AttrNumber = 23;
const Anum_pg_attribute_attfdwoptions: AttrNumber   = 24;
const Anum_pg_attribute_attmissingval_local: AttrNumber = 25;
// Nat values as usize (for array sizes; shadow relcache i32 imports)
// Must be >= max Anum used; pg_attribute has 43 cols in PG18 (attmissingval is col 43)
const Natts_pg_attribute: usize     = 43; // shadows relcache import
const Natts_pg_class: usize         = 34; // shadows relcache import
const Natts_pg_partitioned_table: usize = 8;

// -- pg_class Anum stubs --------------------------------------------------
const Anum_pg_class_relname: AttrNumber            = 2;
const Anum_pg_class_relnamespace: AttrNumber       = 3;
const Anum_pg_class_reltype: AttrNumber            = 4;
const Anum_pg_class_reloftype: AttrNumber          = 5;
const Anum_pg_class_relowner: AttrNumber           = 6;
const Anum_pg_class_relam: AttrNumber              = 7;
const Anum_pg_class_relfilenode: AttrNumber        = 8;
const Anum_pg_class_reltablespace: AttrNumber      = 9;
const Anum_pg_class_relpages: AttrNumber           = 10;
const Anum_pg_class_reltuples: AttrNumber          = 11;
const Anum_pg_class_relallvisible: AttrNumber      = 12;
const Anum_pg_class_relallfrozen: AttrNumber       = 13;
const Anum_pg_class_reltoastrelid: AttrNumber      = 14;
const Anum_pg_class_relhasindex: AttrNumber        = 15;
const Anum_pg_class_relisshared: AttrNumber        = 16;
const Anum_pg_class_relpersistence: AttrNumber     = 17;
const Anum_pg_class_relkind: AttrNumber            = 18;
const Anum_pg_class_relnatts: AttrNumber           = 19;
const Anum_pg_class_relchecks: AttrNumber          = 20;
const Anum_pg_class_relhasrules: AttrNumber        = 21;
const Anum_pg_class_relhastriggers: AttrNumber     = 22;
const Anum_pg_class_relrowsecurity: AttrNumber     = 23;
const Anum_pg_class_relforcerowsecurity: AttrNumber = 24;
const Anum_pg_class_relhassubclass: AttrNumber     = 25;
const Anum_pg_class_relispopulated: AttrNumber     = 26;
const Anum_pg_class_relreplident: AttrNumber       = 27;
const Anum_pg_class_relispartition: AttrNumber     = 28;
const Anum_pg_class_relrewrite: AttrNumber         = 29;
const Anum_pg_class_relfrozenxid: AttrNumber       = 30;
const Anum_pg_class_relminmxid: AttrNumber         = 31;
const Anum_pg_class_relacl: AttrNumber             = 32;
const Anum_pg_class_reloptions: AttrNumber         = 33;
const Anum_pg_class_relpartbound: AttrNumber       = 34;

// -- pg_constraint Anum stubs ---------------------------------------------
const Anum_pg_constraint_conrelid: AttrNumber      = 2;
const Anum_pg_constraint_contypid: AttrNumber      = 3;
const Anum_pg_constraint_conname: AttrNumber       = 1;
const Anum_pg_constraint_oid: AttrNumber           = 0; // oid system column placeholder
const Anum_pg_constraint_conbin: AttrNumber        = 14;

// -- pg_inherits Anum stubs -----------------------------------------------
const Anum_pg_inherits_inhrelid: AttrNumber        = 1;

// -- pg_statistic Anum stubs ----------------------------------------------
const Anum_pg_statistic_starelid: AttrNumber       = 1;
const Anum_pg_statistic_staattnum: AttrNumber      = 2;

// -- pg_partitioned_table Anum stubs --------------------------------------
const Anum_pg_partitioned_table_partrelid: AttrNumber  = 1;
const Anum_pg_partitioned_table_partstrat: AttrNumber  = 2;
const Anum_pg_partitioned_table_partnatts: AttrNumber  = 3;
const Anum_pg_partitioned_table_partdefid: AttrNumber  = 4;
const Anum_pg_partitioned_table_partattrs: AttrNumber  = 5;
const Anum_pg_partitioned_table_partclass: AttrNumber  = 6;
const Anum_pg_partitioned_table_partcollation: AttrNumber = 7;
const Anum_pg_partitioned_table_partexprs: AttrNumber  = 8;

// -- pg_type Anum stubs ---------------------------------------------------
const Anum_pg_type_oid: AttrNumber                 = 1;

// -- Syscache IDs ---------------------------------------------------------
const RELOID: c_int       = 52;
const ATTNUM: c_int       = 4;
const TYPENAMENSP: c_int  = 76;
const FOREIGNTABLEREL: c_int = 25;
const PARTRELID: c_int    = 47;

// -- Datum helpers not yet in prelude -------------------------------------
// TransactionIdGetDatum and Float4GetDatum are now from postgres.rs (above)
// MultiXactIdGetDatum: define locally
#[inline]
fn MultiXactIdGetDatum(x: MultiXactId) -> Datum { x as Datum }
// Use crate::c::NameData (= nameData) which FormData_pg_attribute already uses
use crate::c::NameData;
use crate::pg_config::NAMEDATALEN as C_NAMEDATALEN;
#[inline]
unsafe fn NameGetDatum(n: *const NameData) -> Datum {
    n as Datum
}
/// Build a NameData from a byte string literal at compile time.
/// The input must be exactly NAMEDATALEN bytes (including trailing nuls).
const fn make_name(bytes: &[u8; 64]) -> NameData {
    // SAFETY: [u8; 64] and [i8; 64] have identical layout
    // We're in a const fn so use unsafe transmute
    let data: [i8; 64] = unsafe { core::mem::transmute(*bytes) };
    NameData { data }
}

// -- CheckAttributeType flags ---------------------------------------------
const CHKATYPE_ANYARRAY: c_int   = 0x01;
const CHKATYPE_ANYRECORD: c_int  = 0x02;
const CHKATYPE_IS_PARTKEY: c_int = 0x04;
const CHKATYPE_IS_VIRTUAL: c_int = 0x08;

// -- Dependency kinds -----------------------------------------------------
const DEPENDENCY_NORMAL: c_char   = b'n' as c_char;
const DEPENDENCY_INTERNAL: c_char = b'i' as c_char;

// -- Function OIDs (RegProcedure) -----------------------------------------
const F_RECORD_IN: Oid                 = 2290;
const F_RECORD_OUT: Oid                = 2291;
const F_RECORD_RECV: Oid               = 2292;
const F_RECORD_SEND: Oid               = 2293;
const F_ARRAY_IN: Oid                  = 750;
const F_ARRAY_OUT: Oid                 = 751;
const F_ARRAY_RECV: Oid                = 2400;
const F_ARRAY_SEND: Oid                = 2401;
const F_ARRAY_TYPANALYZE: Oid          = 3179;
const F_ARRAY_SUBSCRIPT_HANDLER: Oid   = 6179;
const F_NAMEEQ: Oid                    = 93;
const F_INT2EQ: Oid                    = 63;
const F_INT2LE: Oid                    = 64;

// -- Misc constants -------------------------------------------------------
const DEFAULT_TYPDELIM: c_char     = b',' as c_char;
const DEFAULT_COLLATION_OID: Oid   = 100;
const NAMEDATALEN: usize           = 64;

// -- Object types (ObjectType enum values) --------------------------------
use crate::nodes::parsenodes::ObjectType;
const OBJECT_TABLE: ObjectType    = ObjectType::OBJECT_TABLE;
const OBJECT_SEQUENCE: ObjectType = ObjectType::OBJECT_SEQUENCE;

// -- ParseState fields index etc ------------------------------------------
type Index = c_int;

// -- RELKIND_HAS_TABLESPACE macro (from C: relkind != RELKIND_VIEW && != COMPOSITE && != PARTITIONED_INDEX) --
#[inline]
fn RELKIND_HAS_TABLESPACE(relkind: c_char) -> bool {
    relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE
        && relkind != RELKIND_PARTITIONED_INDEX
}

// -- RelFileNumberIsValid -------------------------------------------------
#[inline]
fn RelFileNumberIsValid(n: RelFileNumber) -> bool { n != 0 }

// -- Acl type (opaque for heap.rs) ----------------------------------------
type Acl = c_void;

// -- RawColumnDefault stub ------------------------------------------------
#[repr(C)]
struct RawColumnDefault {
    attnum: AttrNumber,
    raw_default: *mut Node,
    generated: c_char,
    generated_when: c_char,
}

// -- FormData_pg_statistic stub -------------------------------------------
#[repr(C)]
struct FormData_pg_statistic {
    starelid: Oid,
    staattnum: AttrNumber,
    stainherit: bool,
    stanullfrac: f32,
    stawidth: i32,
    stadistinct: f32,
}

// -- PartitionBoundSpec stub ----------------------------------------------
use crate::nodes::parsenodes::PartitionBoundSpec;

// T_Const, T_Var, T_List are variants of crate::nodes::nodes::NodeTag
use crate::nodes::nodes::NodeTag::{T_Const, T_Var, T_List};

// -- IsA macro ------------------------------------------------------------
#[inline]
unsafe fn IsA(node: *mut Node, tag: NodeTag) -> bool {
    !node.is_null() && (*node).r#type == tag
}

// -- OnCommitAction constants ---------------------------------------------
const ONCOMMIT_NOOP: OnCommitAction = OnCommitAction::ONCOMMIT_NOOP;

// -- EXPR_KIND constants --------------------------------------------------
// ParseExprKind: define locally (parse_expr module may not exist yet)
#[allow(non_camel_case_types)]
#[derive(Copy, Clone, PartialEq, Eq)]
#[repr(C)]
enum ParseExprKind {
    EXPR_KIND_NONE = 0,
    EXPR_KIND_COLUMN_DEFAULT,
    EXPR_KIND_CHECK_CONSTRAINT,
    EXPR_KIND_GENERATED_COLUMN,
}
type _ParseExprKindAlias = ParseExprKind;
const EXPR_KIND_COLUMN_DEFAULT: ParseExprKind    = ParseExprKind::EXPR_KIND_COLUMN_DEFAULT;
const EXPR_KIND_CHECK_CONSTRAINT: ParseExprKind  = ParseExprKind::EXPR_KIND_CHECK_CONSTRAINT;
const EXPR_KIND_GENERATED_COLUMN: ParseExprKind  = ParseExprKind::EXPR_KIND_GENERATED_COLUMN;

// -- COERCION/COERCE constants --------------------------------------------
use crate::nodes::primnodes::CoercionContext;
use crate::nodes::primnodes::CoercionForm;
const COERCION_ASSIGNMENT: CoercionContext = CoercionContext::COERCION_ASSIGNMENT;
const COERCE_IMPLICIT_CAST: CoercionForm   = CoercionForm::COERCE_IMPLICIT_CAST;

// -- CONSTRAINT_RELATION (used as ObjectClass in pg_constraint) ----------
const CONSTRAINT_RELATION: c_char = b'r' as c_char;

// -- CStr_to_str helper (52 uses in this file) ----------------------------
unsafe fn CStr_to_str<'a>(p: *const c_char) -> std::borrow::Cow<'a, str> {
    if p.is_null() {
        std::borrow::Cow::Borrowed("")
    } else {
        std::ffi::CStr::from_ptr(p).to_string_lossy()
    }
}

// -- CookedConstraint (defined in C as heap.h) ----------------------------
/// Precooked (already transformed) constraint description
#[repr(C)]
pub struct CookedConstraint {
    pub contype: crate::nodes::parsenodes::ConstrType,
    pub conoid: Oid,
    pub name: *mut c_char,
    pub attnum: AttrNumber,
    pub expr: *mut Node,
    pub is_enforced: bool,
    pub skip_validation: bool,
    pub is_local: bool,
    pub inhcount: c_int,
    pub is_no_inherit: bool,
}

// -- stub helpers for functions not yet publicly exported elsewhere --------

// Catalog OIDs not yet in catalog_oids.rs
const AttributeRelidNameIndexId: Oid = 2658;  // pg_attribute_relid_attnam_index

// Rel helpers
#[inline]
unsafe fn RelationGetNamespace(rel: Relation) -> Oid {
    (*(*rel).rd_rel).relnamespace
}
#[inline]
unsafe fn RelationGetNumberOfAttributes(rel: Relation) -> c_int {
    (*(*rel).rd_rel).relnatts as c_int
}
#[inline]
unsafe fn RelationGetPartitionDesc(_rel: Relation, _omit: bool) -> *mut c_void {
    core::ptr::null_mut() /* TODO(pg-port) */
}

// Cache invalidation
#[inline]
unsafe fn CacheInvalidateRelcache(_rel: Relation) { /* TODO(pg-port) */ }
#[inline]
unsafe fn CacheInvalidateRelcacheByRelid(_relid: Oid) { /* TODO(pg-port) */ }

// Dependency recording stubs
type ObjectAddresses = c_void;
unsafe fn new_object_addresses() -> *mut ObjectAddresses { core::ptr::null_mut() /* TODO(pg-port) */ }
unsafe fn add_exact_object_address(_addr: *const ObjectAddress, _addrs: *mut ObjectAddresses) { /* TODO(pg-port) */ }
unsafe fn record_object_address_dependencies(_myself: *const ObjectAddress, _addrs: *mut ObjectAddresses, _deptype: c_char) { /* TODO(pg-port) */ }
unsafe fn free_object_addresses(_addrs: *mut ObjectAddresses) { /* TODO(pg-port) */ }
unsafe fn recordDependencyOn(_myself: *const ObjectAddress, _referenced: *const ObjectAddress, _deptype: c_char) { /* TODO(pg-port) */ }
unsafe fn recordDependencyOnOwner(_classId: Oid, _objectId: Oid, _ownerId: Oid) { /* TODO(pg-port) */ }
unsafe fn recordDependencyOnNewAcl(_classId: Oid, _objectId: Oid, _objsubId: c_int, _ownerId: Oid, _acl: *mut Acl) { /* TODO(pg-port) */ }
unsafe fn recordDependencyOnCurrentExtension(_myself: *const ObjectAddress, _replace: bool) { /* TODO(pg-port) */ }
unsafe fn recordDependencyOnTablespace(_classId: Oid, _objectId: Oid, _tablespace: Oid) { /* TODO(pg-port) */ }
unsafe fn recordDependencyOnSingleRelExpr(_myself: *const ObjectAddress, _expr: *mut Node, _relid: Oid, _normal_dep: c_char, _self_dep: c_char, _reverse_self: bool) { /* TODO(pg-port) */ }

// ObjectAddressSubSet (subset relation for attribute-level dependencies)
#[inline]
unsafe fn ObjectAddressSubSet(addr: *mut ObjectAddress, classId: Oid, objectId: Oid, objectSubId: c_int) {
    (*addr).classId = classId;
    (*addr).objectId = objectId;
    (*addr).objectSubId = objectSubId;
}

// Hooks
unsafe fn InvokeObjectPostCreateHookArg(_classId: Oid, _objectId: Oid, _objectSubId: c_int, _is_internal: bool) { /* TODO(pg-port) */ }

// Partition helpers
unsafe fn get_partition_parent(_relid: Oid, _even_if_detached: bool) -> Oid { InvalidOid /* TODO(pg-port) */ }
unsafe fn get_default_partition_oid(_parentOid: Oid) -> Oid { InvalidOid /* TODO(pg-port) */ }
unsafe fn get_default_oid_from_partdesc(_partdesc: *mut c_void) -> Oid { InvalidOid /* TODO(pg-port) */ }
unsafe fn update_default_partition_oid(_parentId: Oid, _defaultPartId: Oid) { /* TODO(pg-port) */ }
// RemovePartitionKeyByRelId: real implementation is later in this file

// ON COMMIT actions
unsafe fn register_on_commit_action(_relid: Oid, _action: OnCommitAction) { /* TODO(pg-port) */ }
unsafe fn remove_on_commit_action(_relid: Oid) { /* TODO(pg-port) */ }

// Subscription
unsafe fn RemoveSubscriptionRel(_subid: Oid, _relid: Oid) { /* TODO(pg-port) */ }

// Table operations
unsafe fn table_relation_set_new_filelocator(_rel: Relation, _newlocator: *const c_void, _persistence: c_char, _freezeXid: *mut TransactionId, _minmulti: *mut MultiXactId) { /* TODO(pg-port) */ }
unsafe fn table_relation_nontransactional_truncate(_rel: Relation) { /* TODO(pg-port) */ }

// ACL
unsafe fn get_user_default_acl(_objtype: ObjectType, _ownerId: Oid, _nsp_oid: Oid) -> *mut Acl {
    core::ptr::null_mut() /* TODO(pg-port) */
}

// CheckTable safety
unsafe fn CheckTableNotInUse(_rel: Relation, _stmt: *const c_char) { /* TODO(pg-port) */ }
unsafe fn CheckTableForSerializableConflictIn(_rel: Relation) { /* TODO(pg-port) */ }

// Constraint management
unsafe fn CreateConstraintEntry(
    _constraintName: *const c_char, _constraintNamespace: Oid,
    _constraintType: c_char, _isDeferrable: bool, _isDeferred: bool,
    _isEnforced: bool, _isValidated: bool, _parentConstrId: Oid,
    _relId: Oid, _attrs: *const AttrNumber, _numAttrs: c_int, _numFKAttrs: c_int,
    _domainId: Oid, _indexOid: Oid, _foreignRelId: Oid,
    _foreignAttrs: *mut AttrNumber, _numForeignAttrs: *mut c_int,
    _fkActionUpdate: *mut c_char, _fkActionDelete: *mut c_char,
    _num_fk_del_set_cols: c_int, _fkMatchType: c_char, _fkDelSetAttrs: c_char,
    _exclOp: *mut c_void, _num_excl: c_int, _exclDelSetAttrs: c_char,
    _exclOpPtr: *mut c_void,
    _expr: *mut Node, _exprString: *mut c_char,
    _conIsLocal: bool, _conInhCount: int16, _conNoInherit: bool,
    _conPeriod: bool, _is_internal: bool,
) -> Oid { InvalidOid /* TODO(pg-port) */ }

unsafe fn ChooseConstraintName(_rel_name: *const c_char, _col_name: *const c_char, _label: *const c_char, _namespaceid: Oid, _others: *mut List) -> *mut c_char { core::ptr::null_mut() /* TODO(pg-port) */ }
unsafe fn ConstraintNameIsUsed(_typ: c_char, _relid: Oid, _name: *mut c_char) -> bool { false /* TODO(pg-port) */ }
unsafe fn AdjustNotNullInheritance(_relid: Oid, _attnum: AttrNumber, _conname: *mut c_char, _is_local: bool, _is_no_inherit: bool, _skip_validation: bool) -> bool { false /* TODO(pg-port) */ }
unsafe fn MergeWithExistingConstraint_inner(_rel: Relation, _ccname: *const c_char, _expr: *mut Node, _allow_merge: bool, _is_local: bool, _is_enforced: bool, _initially_valid: bool, _no_inherit: bool) -> bool { false /* TODO(pg-port) */ }

// Type creation
unsafe fn TypeCreate(
    _newTypeOid: Oid, _typeName: *const c_char, _typeNamespace: Oid,
    _relationOid: Oid, _relationKind: c_char, _ownerId: Oid,
    _internalSize: i32, _typeType: c_char, _typeCategory: c_char,
    _typePreferred: bool, _typDelim: c_char,
    _inputProcedure: Oid, _outputProcedure: Oid,
    _receiveProcedure: Oid, _sendProcedure: Oid,
    _typmodinProcedure: Oid, _typmodoutProcedure: Oid,
    _analyzeProcedure: Oid, _subscriptProcedure: Oid,
    _elemType: Oid, _isImplicitArray: bool, _arrayType: Oid,
    _baseType: Oid, _defaultTypeValue: *const c_char, _defaultTypeBin: *const c_char,
    _passedByValue: bool, _alignment: c_char, _storage: c_char,
    _typeMod: i32, _typNDims: i32, _typeNotNull: bool, _typeCollation: Oid,
) -> ObjectAddress { INVALID_OBJECT_ADDRESS /* TODO(pg-port) */ }
unsafe fn AssignTypeArrayOid() -> Oid { InvalidOid /* TODO(pg-port) */ }
unsafe fn makeArrayTypeName(_typname: *const c_char, _typeNamespace: Oid) -> *mut c_char { core::ptr::null_mut() /* TODO(pg-port) */ }
unsafe fn moveArrayTypeName(_typeOid: Oid, _typname: *const c_char, _typeNamespace: Oid) -> bool { false /* TODO(pg-port) */ }

// Expr/parse utilities
unsafe fn transformExpr(_pstate: *mut ParseState, _expr: *mut Node, _kind: ParseExprKind) -> *mut Node { core::ptr::null_mut() /* TODO(pg-port) */ }
unsafe fn coerce_to_target_type(_pstate: *mut ParseState, _expr: *mut Node, _exprtype: Oid, _targettype: Oid, _targettypmod: i32, _ccontext: CoercionContext, _cformat: CoercionForm, _location: c_int) -> *mut Node { core::ptr::null_mut() /* TODO(pg-port) */ }
unsafe fn coerce_to_boolean(_pstate: *mut ParseState, _expr: *mut Node, _constructname: *const c_char) -> *mut Node { core::ptr::null_mut() /* TODO(pg-port) */ }
unsafe fn addRangeTableEntryForRelation(_pstate: *mut ParseState, _rel: Relation, _lockmode: LOCKMODE, _alias: *mut c_void, _inh: bool, _inFromCl: bool) -> *mut c_void { core::ptr::null_mut() /* TODO(pg-port) */ }
unsafe fn addNSItemToQuery(_pstate: *mut ParseState, _nsitem: *mut c_void, _addToJoinList: bool, _addToRelNameSpace: bool, _addToVarNameSpace: bool) { /* TODO(pg-port) */ }
// cookConstraint: real implementation is later in this file
unsafe fn StoreAttrDefault(_rel: Relation, _attnum: AttrNumber, _expr: *mut Node, _is_internal: bool) -> Oid { InvalidOid /* TODO(pg-port) */ }

// Index management
unsafe fn index_build(_heap_rel: Relation, _index_rel: Relation, _index_info: *mut c_void, _validate: bool, _check_unique: bool) { /* TODO(pg-port) */ }
unsafe fn BuildDummyIndexInfo(_index_rel: Relation) -> *mut c_void { core::ptr::null_mut() /* TODO(pg-port) */ }

// lsyscache
unsafe fn get_attname_local(_relid: Oid, _attnum: AttrNumber, _missing_ok: bool) -> *mut c_char { core::ptr::null_mut() /* TODO(pg-port stub) */ }

// Misc
unsafe fn OidFunctionCall3(_functionId: Oid, _arg1: Datum, _arg2: Datum, _arg3: Datum) -> Datum { 0 /* TODO(pg-port) */ }
unsafe fn CommandCounterIncrement() { /* TODO(pg-port) */ }
unsafe fn SearchSysCacheCopy1_local(_cacheId: c_int, _key1: Datum) -> HeapTuple { core::ptr::null_mut() /* TODO(pg-port) */ }
unsafe fn SetRelationNumChecks_stub(_rel: Relation, _numchecks: c_int) { /* TODO(pg-port) */ }
unsafe fn heap_truncate_find_FKs_stub(_relationIds: *mut List) -> *mut List { NIL /* TODO(pg-port) */ }

// list_nth_node for typed list access
#[inline]
unsafe fn list_nth_node_Constraint(list: *mut List, n: c_int) -> *mut c_void {
    crate::nodes::pg_list::list_nth(list, n)
}

// libc helpers already in crate (via c.rs or prelude)
// libc string/mem stubs (these are C intrinsics used in the translated code)
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    libc_strcmp_impl(a, b)
}
unsafe fn libc_strcmp_impl(a: *const c_char, b: *const c_char) -> c_int {
    let mut i = 0usize;
    loop {
        let ca = *a.add(i) as u8;
        let cb = *b.add(i) as u8;
        if ca != cb { return (ca as c_int) - (cb as c_int); }
        if ca == 0  { return 0; }
        i += 1;
    }
}
// snprintf(buf, size, fmt, ...) - variadic; only the common int-arg case needed here
unsafe fn libc_snprintf(buf: *mut c_char, size: usize, fmt: *const c_char, n: c_int) {
    // Render "........pg.dropped.N........" where fmt contains "{}" placeholder (Rust-style).
    // In C the actual format is "........pg.dropped.%d........" but translated to Rust-style.
    // Use a fixed-size buffer for simplicity.
    let s = format!("{}\0", n);
    // Find "{}" in fmt string and substitute
    let fmt_str = std::ffi::CStr::from_ptr(fmt).to_string_lossy();
    let result = fmt_str.replace("{}", &n.to_string());
    let result_c = std::ffi::CString::new(result).unwrap_or_default();
    let bytes = result_c.as_bytes_with_nul();
    let copy_len = bytes.len().min(size);
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, copy_len);
}
unsafe fn libc_memset(ptr: *mut c_void, val: c_int, n: usize) {
    core::ptr::write_bytes(ptr as *mut u8, val as u8, n);
}

// -- Additional missing items -------------------------------------------
use crate::utils::cache::syscache::SearchSysCacheCopy;
#[inline]
unsafe fn SearchSysCacheCopy2(cacheId: c_int, key1: Datum, key2: Datum) -> *mut HeapTupleData {
    SearchSysCacheCopy(cacheId, key1, key2, 0, 0) as *mut HeapTupleData
}
use crate::access::htup_details::MaxHeapAttributeNumber;
use crate::access::stratnum::BTLessEqualStrategyNumber;
use crate::nodes::primnodes::Const;
use crate::storage::itemptr::ItemPointerData;
// list_make1_oid is a #[macro_export] macro - available at crate root
// No use statement needed; invoke as list_make1_oid!(...)

/* Potentially set by pg_upgrade_support functions */
pub static mut binary_upgrade_next_heap_pg_class_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_toast_pg_class_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_heap_pg_class_relfilenumber: RelFileNumber =
    InvalidRelFileNumber;
pub static mut binary_upgrade_next_toast_pg_class_relfilenumber: RelFileNumber =
    InvalidRelFileNumber;

/* ----------------------------------------------------------------
 *            XXX UGLY HARD CODED BADNESS FOLLOWS XXX
 *
 *        these should all be moved to someplace in the lib/catalog
 *        module, if not obliterated first.
 * ----------------------------------------------------------------
 */


/*
 * Note:
 *        Should the system special case these attributes in the future?
 *        Advantage:    consume much less space in the ATTRIBUTE relation.
 *        Disadvantage:  special cases will be all over the place.
 */

/*
 * The initializers below do not include trailing variable length fields,
 * but that's OK - we're never going to reference anything beyond the
 * fixed-size portion of the structure anyway.  Fields that can default
 * to zeroes are also not mentioned.
 */

// SAFETY: zeroed() is safe for FormData_pg_attribute (all-zeroes is valid)
static A1: FormData_pg_attribute = unsafe { FormData_pg_attribute {
    attname: make_name(b"ctid\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"),
    atttypid: TIDOID,
    attlen: core::mem::size_of::<ItemPointerData>() as int16,
    attnum: SelfItemPointerAttributeNumber as int16,
    atttypmod: -1,
    attbyval: false,
    attalign: TYPALIGN_SHORT,
    attstorage: TYPSTORAGE_PLAIN,
    attnotnull: true,
    attislocal: true,
    ..core::mem::zeroed()
}};

static A2: FormData_pg_attribute = unsafe { FormData_pg_attribute {
    attname: make_name(b"xmin\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"),
    atttypid: XIDOID,
    attlen: core::mem::size_of::<TransactionId>() as int16,
    attnum: MinTransactionIdAttributeNumber as int16,
    atttypmod: -1,
    attbyval: true,
    attalign: TYPALIGN_INT,
    attstorage: TYPSTORAGE_PLAIN,
    attnotnull: true,
    attislocal: true,
    ..core::mem::zeroed()
}};

static A3: FormData_pg_attribute = unsafe { FormData_pg_attribute {
    attname: make_name(b"cmin\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"),
    atttypid: CIDOID,
    attlen: core::mem::size_of::<CommandId>() as int16,
    attnum: MinCommandIdAttributeNumber as int16,
    atttypmod: -1,
    attbyval: true,
    attalign: TYPALIGN_INT,
    attstorage: TYPSTORAGE_PLAIN,
    attnotnull: true,
    attislocal: true,
    ..core::mem::zeroed()
}};

static A4: FormData_pg_attribute = unsafe { FormData_pg_attribute {
    attname: make_name(b"xmax\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"),
    atttypid: XIDOID,
    attlen: core::mem::size_of::<TransactionId>() as int16,
    attnum: MaxTransactionIdAttributeNumber as int16,
    atttypmod: -1,
    attbyval: true,
    attalign: TYPALIGN_INT,
    attstorage: TYPSTORAGE_PLAIN,
    attnotnull: true,
    attislocal: true,
    ..core::mem::zeroed()
}};

static A5: FormData_pg_attribute = unsafe { FormData_pg_attribute {
    attname: make_name(b"cmax\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"),
    atttypid: CIDOID,
    attlen: core::mem::size_of::<CommandId>() as int16,
    attnum: MaxCommandIdAttributeNumber as int16,
    atttypmod: -1,
    attbyval: true,
    attalign: TYPALIGN_INT,
    attstorage: TYPSTORAGE_PLAIN,
    attnotnull: true,
    attislocal: true,
    ..core::mem::zeroed()
}};

/*
 * We decided to call this attribute "tableoid" rather than say
 * "classoid" on the basis that in the future there may be more than one
 * table of a particular class/type. In any case table is still the word
 * used in SQL.
 */
static A6: FormData_pg_attribute = unsafe { FormData_pg_attribute {
    attname: make_name(b"tableoid\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"),
    atttypid: OIDOID,
    attlen: core::mem::size_of::<Oid>() as int16,
    attnum: TableOidAttributeNumber as int16,
    atttypmod: -1,
    attbyval: true,
    attalign: TYPALIGN_INT,
    attstorage: TYPSTORAGE_PLAIN,
    attnotnull: true,
    attislocal: true,
    ..core::mem::zeroed()
}};

struct SysAttArray([*const FormData_pg_attribute; 6]);
// SAFETY: these pointers point to immutable statics, never mutated.
unsafe impl Sync for SysAttArray {}
static SYS_ATT: SysAttArray = SysAttArray([
    &A1 as *const FormData_pg_attribute,
    &A2 as *const FormData_pg_attribute,
    &A3 as *const FormData_pg_attribute,
    &A4 as *const FormData_pg_attribute,
    &A5 as *const FormData_pg_attribute,
    &A6 as *const FormData_pg_attribute,
]);

/*
 * This function returns a Form_pg_attribute pointer for a system attribute.
 * Note that we elog if the presented attno is invalid, which would only
 * happen if there's a problem upstream.
 */
pub unsafe fn SystemAttributeDefinition(attno: AttrNumber) -> *const FormData_pg_attribute {
    if attno >= 0 || attno < -(SYS_ATT.0.len() as AttrNumber) {
        elog!(ERROR, "invalid system attribute number {}", attno);
    }
    SYS_ATT.0[(-attno - 1) as usize]
}

/*
 * If the given name is a system attribute name, return a Form_pg_attribute
 * pointer for a prototype definition.  If not, return NULL.
 */
pub unsafe fn SystemAttributeByName(attname: *const c_char) -> *const FormData_pg_attribute {
    for j in 0..SYS_ATT.0.len() {
        let att = &*SYS_ATT.0[j];
        if libc_strcmp(NameStr(&att.attname), attname) == 0 {
            return SYS_ATT.0[j];
        }
    }
    core::ptr::null()
}


/* ----------------------------------------------------------------
 *        XXX END OF UGLY HARD CODED BADNESS XXX
 * ---------------------------------------------------------------- */


/* ----------------------------------------------------------------
 *        heap_create        - Create an uncataloged heap relation
 *
 *        Note API change: the caller must now always provide the OID
 *        to use for the relation.  The relfilenumber may be (and in
 *        the simplest cases is) left unspecified.
 *
 *        create_storage indicates whether or not to create the storage.
 *        However, even if create_storage is true, no storage will be
 *        created if the relkind is one that doesn't have storage.
 *
 *        rel->rd_rel is initialized by RelationBuildLocalRelation,
 *        and is mostly zeroes at return.
 * ----------------------------------------------------------------
 */
pub unsafe fn heap_create(
    relname: *const c_char,
    relnamespace: Oid,
    reltablespace: Oid,
    relid: Oid,
    relfilenumber: RelFileNumber,
    accessmtd: Oid,
    tup_desc: *mut TupleDescData,
    relkind: c_char,
    relpersistence: c_char,
    shared_relation: bool,
    mapped_relation: bool,
    allow_system_table_mods: bool,
    relfrozenxid: *mut TransactionId,
    relminmxid: *mut MultiXactId,
    create_storage: bool,
) -> Relation {
    /* The caller must have provided an OID for the relation. */
    Assert!(OidIsValid(relid));

    /*
     * Don't allow creating relations in pg_catalog directly, even though it
     * is allowed to move user defined relations there. Semantics with search
     * paths including pg_catalog are too confusing for now.
     *
     * But allow creating indexes on relations in pg_catalog even if
     * allow_system_table_mods = off, upper layers already guarantee it's on a
     * user defined relation, not a system one.
     */
    if !allow_system_table_mods
        && ((IsCatalogNamespace(relnamespace) && relkind != RELKIND_INDEX)
            || IsToastNamespace(relnamespace))
        && IsNormalProcessingMode()
    {
        ereport!(ERROR, errmsg!(
                "permission denied to create \"{}.{}\"",
                CStr_to_str(get_namespace_name(relnamespace)),
                CStr_to_str(relname)
            )) /* C also: errcode, errdetail */;
    }

    *relfrozenxid = InvalidTransactionId;
    *relminmxid = InvalidMultiXactId;

    /*
     * Force reltablespace to zero if the relation kind does not support
     * tablespaces.  This is mainly just for cleanliness' sake.
     */
    let mut reltablespace = reltablespace;
    if !RELKIND_HAS_TABLESPACE(relkind) {
        reltablespace = InvalidOid;
    }

    /* Don't create storage for relkinds without physical storage. */
    let mut create_storage = create_storage;
    let mut relfilenumber = relfilenumber;
    if !RELKIND_HAS_STORAGE(relkind) {
        create_storage = false;
    } else {
        /*
         * If relfilenumber is unspecified by the caller then create storage
         * with oid same as relid.
         */
        if !RelFileNumberIsValid(relfilenumber) {
            relfilenumber = relid;
        }
    }

    /*
     * Never allow a pg_class entry to explicitly specify the database's
     * default tablespace in reltablespace; force it to zero instead. This
     * ensures that if the database is cloned with a different default
     * tablespace, the pg_class entry will still match where CREATE DATABASE
     * will put the physically copied relation.
     *
     * Yes, this is a bit of a hack.
     */
    if reltablespace == MyDatabaseTableSpace {
        reltablespace = InvalidOid;
    }

    /*
     * build the relcache entry.
     */
    let rel = RelationBuildLocalRelation(
        relname,
        relnamespace,
        tup_desc,
        relid,
        accessmtd,
        relfilenumber,
        reltablespace,
        shared_relation,
        mapped_relation,
        relpersistence,
        relkind,
    );

    /*
     * Have the storage manager create the relation's disk file, if needed.
     *
     * For tables, the AM callback creates both the main and the init fork.
     * For others, only the main fork is created; the other forks will be
     * created on demand.
     */
    if create_storage {
        if RELKIND_HAS_TABLE_AM((*(*rel).rd_rel).relkind) {
            table_relation_set_new_filelocator(
                rel,
                &(*rel).rd_locator as *const _ as *const c_void,
                relpersistence,
                relfrozenxid,
                relminmxid,
            );
        } else if RELKIND_HAS_STORAGE((*(*rel).rd_rel).relkind) {
            // blkreftable::RelFileLocator and storage::relfilelocator::RelFileLocator are layout-compatible
            RelationCreateStorage(core::mem::transmute((*rel).rd_locator), relpersistence, true); /* TODO(pg-port): catalog/storage.c */
        } else {
            Assert!(false);
        }
    }

    /*
     * If a tablespace is specified, removal of that tablespace is normally
     * protected by the existence of a physical file; but for relations with
     * no files, add a pg_shdepend entry to account for that.
     */
    if !create_storage && reltablespace != InvalidOid {
        recordDependencyOnTablespace(RelationRelationId, relid, reltablespace); /* TODO(pg-port): dependency */
    }

    /* ensure that stats are dropped if transaction aborts */
    pgstat_create_relation(rel);

    rel
}

/* ----------------------------------------------------------------
 *        heap_create_with_catalog        - Create a cataloged relation
 *
 *        this is done in multiple steps:
 *
 *        1) CheckAttributeNamesTypes() is used to make certain the tuple
 *           descriptor contains a valid set of attribute names and types
 *
 *        2) pg_class is opened and get_relname_relid()
 *           performs a scan to ensure that no relation with the
 *           same name already exists.
 *
 *        3) heap_create() is called to create the new relation on disk.
 *
 *        4) TypeCreate() is called to define a new type corresponding
 *           to the new relation.
 *
 *        5) AddNewRelationTuple() is called to register the
 *           relation in pg_class.
 *
 *        6) AddNewAttributeTuples() is called to register the
 *           new relation's schema in pg_attribute.
 *
 *        7) StoreConstraints() is called            - vadim 08/22/97
 *
 *        8) the relations are closed and the new relation's oid
 *           is returned.
 *
 * ----------------------------------------------------------------
 */

/* --------------------------------
 *        CheckAttributeNamesTypes
 *
 *        this is used to make certain the tuple descriptor contains a
 *        valid set of attribute names and datatypes.  a problem simply
 *        generates ereport(ERROR) which aborts the current transaction.
 *
 *        relkind is the relkind of the relation to be created.
 *        flags controls which datatypes are allowed, cf CheckAttributeType.
 * --------------------------------
 */
pub unsafe fn CheckAttributeNamesTypes(tupdesc: *mut TupleDescData, relkind: c_char, flags: c_int) {
    let natts = (*tupdesc).natts;

    /* Sanity check on column count */
    if natts < 0 || natts > MaxHeapAttributeNumber {
        ereport!(ERROR, errmsg!("tables can have at most {} columns", MaxHeapAttributeNumber)) /* C also: errcode */;
    }

    /*
     * first check for collision with system attribute names
     *
     * Skip this for a view or type relation, since those don't have system
     * attributes.
     */
    if relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE {
        for i in 0..natts {
            let attr = TupleDescAttr(tupdesc, i);
            if !SystemAttributeByName(NameStr(&(*attr).attname)).is_null() {
                ereport!(ERROR, errmsg!(
                        "column name \"{}\" conflicts with a system column name",
                        CStr_to_str(NameStr(&(*attr).attname))
                    )) /* C also: errcode */;
            }
        }
    }

    /*
     * next check for repeated attribute names
     */
    for i in 1..natts {
        for j in 0..i {
            if libc_strcmp(
                NameStr(&(*TupleDescAttr(tupdesc, j)).attname),
                NameStr(&(*TupleDescAttr(tupdesc, i)).attname),
            ) == 0
            {
                ereport!(ERROR, errmsg!(
                        "column name \"{}\" specified more than once",
                        CStr_to_str(NameStr(&(*TupleDescAttr(tupdesc, j)).attname))
                    )) /* C also: errcode */;
            }
        }
    }

    /*
     * next check the attribute types
     */
    for i in 0..natts {
        let attr = TupleDescAttr(tupdesc, i);
        let extra_flags = flags
            | (if (*attr).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
                CHKATYPE_IS_VIRTUAL
            } else {
                0
            });
        CheckAttributeType(
            NameStr(&(*attr).attname),
            (*attr).atttypid,
            (*attr).attcollation,
            NIL,
            extra_flags,
        );
    }
}

/* --------------------------------
 *        CheckAttributeType
 *
 *        Verify that the proposed datatype of an attribute is legal.
 *        This is needed mainly because there are types (and pseudo-types)
 *        in the catalogs that we do not support as elements of real tuples.
 *        We also check some other properties required of a table column.
 *
 * If the attribute is being proposed for addition to an existing table or
 * composite type, pass a one-element list of the rowtype OID as
 * containing_rowtypes.  When checking a to-be-created rowtype, it's
 * sufficient to pass NIL, because there could not be any recursive reference
 * to a not-yet-existing rowtype.
 *
 * flags is a bitmask controlling which datatypes we allow.  For the most
 * part, pseudo-types are disallowed as attribute types, but there are some
 * exceptions: ANYARRAYOID, RECORDOID, and RECORDARRAYOID can be allowed
 * in some cases.  (This works because values of those type classes are
 * self-identifying to some extent.  However, RECORDOID and RECORDARRAYOID
 * are reliably identifiable only within a session, since the identity info
 * may use a typmod that is only locally assigned.  The caller is expected
 * to know whether these cases are safe.)
 *
 * flags can also control the phrasing of the error messages.  If
 * CHKATYPE_IS_PARTKEY is specified, "attname" should be a partition key
 * column number as text, not a real column name.
 * --------------------------------
 */
pub unsafe fn CheckAttributeType(
    attname: *const c_char,
    atttypid: Oid,
    attcollation: Oid,
    containing_rowtypes: *mut List,
    flags: c_int,
) {
    let att_typtype = get_typtype(atttypid);
    let att_typelem: Oid;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    if att_typtype == TYPTYPE_PSEUDO {
        /*
         * We disallow pseudo-type columns, with the exception of ANYARRAY,
         * RECORD, and RECORD[] when the caller says that those are OK.
         *
         * We don't need to worry about recursive containment for RECORD and
         * RECORD[] because (a) no named composite type should be allowed to
         * contain those, and (b) two "anonymous" record types couldn't be
         * considered to be the same type, so infinite recursion isn't
         * possible.
         */
        if !((atttypid == ANYARRAYOID && (flags & CHKATYPE_ANYARRAY) != 0)
            || (atttypid == RECORDOID && (flags & CHKATYPE_ANYRECORD) != 0)
            || (atttypid == RECORDARRAYOID && (flags & CHKATYPE_ANYRECORD) != 0))
        {
            if (flags & CHKATYPE_IS_PARTKEY) != 0 {
                /* translator: first {} is an integer not a name */
                ereport!(ERROR, errmsg!(
                        "partition key column {} has pseudo-type {}",
                        CStr_to_str(attname),
                        CStr_to_str(format_type_be(atttypid))
                    )) /* C also: errcode */;
            } else {
                ereport!(ERROR, errmsg!(
                        "column \"{}\" has pseudo-type {}",
                        CStr_to_str(attname),
                        CStr_to_str(format_type_be(atttypid))
                    )) /* C also: errcode */;
            }
        }
    } else if att_typtype == TYPTYPE_DOMAIN {
        /*
         * Prevent virtual generated columns from having a domain type.  We
         * would have to enforce domain constraints when columns underlying
         * the generated column change.  This could possibly be implemented,
         * but it's not.
         */
        if (flags & CHKATYPE_IS_VIRTUAL) != 0 {
            ereport!(ERROR, errmsg!(
                    "virtual generated column \"{}\" cannot have a domain type",
                    CStr_to_str(attname)
                )) /* C also: errcode */;
        }

        /*
         * If it's a domain, recurse to check its base type.
         */
        CheckAttributeType(
            attname,
            getBaseType(atttypid),
            attcollation,
            containing_rowtypes,
            flags,
        );
    } else if att_typtype == TYPTYPE_COMPOSITE {
        /*
         * For a composite type, recurse into its attributes.
         */

        /*
         * Check for self-containment.  Eventually we might be able to allow
         * this (just return without complaint, if so) but it's not clear how
         * many other places would require anti-recursion defenses before it
         * would be safe to allow tables to contain their own rowtype.
         */
        if list_member_oid(containing_rowtypes, atttypid) {
            ereport!(ERROR, errmsg!(
                    "composite type {} cannot be made a member of itself",
                    CStr_to_str(format_type_be(atttypid))
                )) /* C also: errcode */;
        }

        let containing_rowtypes = lappend_oid(containing_rowtypes, atttypid);

        let relation = relation_open(get_typ_typrelid(atttypid), AccessShareLock);
        let tupdesc = RelationGetDescr(relation);

        for i in 0..(*tupdesc).natts {
            let attr = TupleDescAttr(tupdesc, i);
            if (*attr).attisdropped {
                continue;
            }
            CheckAttributeType(
                NameStr(&(*attr).attname),
                (*attr).atttypid,
                (*attr).attcollation,
                containing_rowtypes,
                flags & !CHKATYPE_IS_PARTKEY,
            );
        }

        relation_close(relation, AccessShareLock);

        list_delete_last(containing_rowtypes);
    } else if att_typtype == TYPTYPE_RANGE {
        /*
         * If it's a range, recurse to check its subtype.
         */
        CheckAttributeType(
            attname,
            get_range_subtype(atttypid),
            get_range_collation(atttypid),
            containing_rowtypes,
            flags,
        );
    } else {
        att_typelem = get_element_type(atttypid);
        if OidIsValid(att_typelem) {
            /*
             * Must recurse into array types, too, in case they are composite.
             */
            CheckAttributeType(attname, att_typelem, attcollation, containing_rowtypes, flags);
        }
    }

    /*
     * For consistency with check_virtual_generated_security().
     */
    if (flags & CHKATYPE_IS_VIRTUAL) != 0 && atttypid >= FirstUnpinnedObjectId {
        ereport!(ERROR, errmsg!(
                "virtual generated column \"{}\" cannot have a user-defined type",
                CStr_to_str(attname)
            )) /* C also: errcode, errdetail */;
    }

    /*
     * This might not be strictly invalid per SQL standard, but it is pretty
     * useless, and it cannot be dumped, so we must disallow it.
     */
    if !OidIsValid(attcollation) && type_is_collatable(atttypid) {
        if (flags & CHKATYPE_IS_PARTKEY) != 0 {
            /* translator: first {} is an integer not a name */
            ereport!(ERROR, errmsg!(
                    "no collation was derived for partition key column {} with collatable type {}",
                    CStr_to_str(attname),
                    CStr_to_str(format_type_be(atttypid))
                )) /* C also: errcode, errhint */;
        } else {
            ereport!(ERROR, errmsg!(
                    "no collation was derived for column \"{}\" with collatable type {}",
                    CStr_to_str(attname),
                    CStr_to_str(format_type_be(atttypid))
                )) /* C also: errcode, errhint */;
        }
    }
}

/*
 * InsertPgAttributeTuples
 *        Construct and insert a set of tuples in pg_attribute.
 *
 * Caller has already opened and locked pg_attribute.  tupdesc contains the
 * attributes to insert.  tupdesc_extra supplies the values for certain
 * variable-length/nullable pg_attribute fields and must contain the same
 * number of elements as tupdesc or be NULL.  The other variable-length fields
 * of pg_attribute are always initialized to null values.
 *
 * indstate is the index state for CatalogTupleInsertWithInfo.  It can be
 * passed as NULL, in which case we'll fetch the necessary info.  (Don't do
 * this when inserting multiple attributes, because it's a tad more
 * expensive.)
 *
 * new_rel_oid is the relation OID assigned to the attributes inserted.
 * If set to InvalidOid, the relation OID from tupdesc is used instead.
 */
pub unsafe fn InsertPgAttributeTuples(
    pg_attribute_rel: Relation,
    tupdesc: *mut TupleDescData,
    new_rel_oid: Oid,
    tupdesc_extra: *const FormExtraData_pg_attribute,
    mut indstate: CatalogIndexState,
) {
    let td = RelationGetDescr(pg_attribute_rel);

    /* Initialize the number of slots to use */
    let nslots = Min(
        (*tupdesc).natts,
        (MAX_CATALOG_MULTI_INSERT_BYTES / core::mem::size_of::<FormData_pg_attribute>()) as i32,
    );
    let slot: *mut *mut TupleTableSlot = palloc(
        (core::mem::size_of::<*mut TupleTableSlot>() * nslots as usize) as Size,
    ) as *mut *mut TupleTableSlot;
    for i in 0..nslots {
        *slot.add(i as usize) = MakeSingleTupleTableSlot(td, &TTSOpsHeapTuple);
    }

    let mut natts = 0i32;
    let mut close_index = false;
    while natts < (*tupdesc).natts {
        let slotCount = natts % nslots; /* reuse slot indices in batches */
        let attrs = TupleDescAttr(tupdesc, natts);
        let attrs_extra: *const FormExtraData_pg_attribute = if !tupdesc_extra.is_null() {
            tupdesc_extra.add(natts as usize)
        } else {
            core::ptr::null()
        };

        ExecClearTuple(*slot.add(slotCount as usize));

        libc_memset(
            (*(*slot.add(slotCount as usize))).tts_isnull as *mut c_void,
            0,
            ((*(*(*slot.add(slotCount as usize))).tts_tupleDescriptor).natts as usize)
                * core::mem::size_of::<bool>(),
        );

        let sv = (*(*slot.add(slotCount as usize))).tts_values;
        let sn = (*(*slot.add(slotCount as usize))).tts_isnull;

        if new_rel_oid != InvalidOid {
            *sv.add(Anum_pg_attribute_attrelid as usize - 1) =
                ObjectIdGetDatum(new_rel_oid);
        } else {
            *sv.add(Anum_pg_attribute_attrelid as usize - 1) =
                ObjectIdGetDatum((*attrs).attrelid);
        }

        *sv.add(Anum_pg_attribute_attname as usize - 1) =
            NameGetDatum(&(*attrs).attname);
        *sv.add(Anum_pg_attribute_atttypid as usize - 1) =
            ObjectIdGetDatum((*attrs).atttypid);
        *sv.add(Anum_pg_attribute_attlen as usize - 1) =
            Int16GetDatum((*attrs).attlen);
        *sv.add(Anum_pg_attribute_attnum as usize - 1) =
            Int16GetDatum((*attrs).attnum);
        *sv.add(Anum_pg_attribute_atttypmod as usize - 1) =
            Int32GetDatum((*attrs).atttypmod);
        *sv.add(Anum_pg_attribute_attndims as usize - 1) =
            Int16GetDatum((*attrs).attndims);
        *sv.add(Anum_pg_attribute_attbyval as usize - 1) =
            BoolGetDatum((*attrs).attbyval);
        *sv.add(Anum_pg_attribute_attalign as usize - 1) =
            CharGetDatum((*attrs).attalign);
        *sv.add(Anum_pg_attribute_attstorage as usize - 1) =
            CharGetDatum((*attrs).attstorage);
        *sv.add(Anum_pg_attribute_attcompression as usize - 1) =
            CharGetDatum((*attrs).attcompression);
        *sv.add(Anum_pg_attribute_attnotnull as usize - 1) =
            BoolGetDatum((*attrs).attnotnull);
        *sv.add(Anum_pg_attribute_atthasdef as usize - 1) =
            BoolGetDatum((*attrs).atthasdef);
        *sv.add(Anum_pg_attribute_atthasmissing as usize - 1) =
            BoolGetDatum((*attrs).atthasmissing);
        *sv.add(Anum_pg_attribute_attidentity as usize - 1) =
            CharGetDatum((*attrs).attidentity);
        *sv.add(Anum_pg_attribute_attgenerated as usize - 1) =
            CharGetDatum((*attrs).attgenerated);
        *sv.add(Anum_pg_attribute_attisdropped as usize - 1) =
            BoolGetDatum((*attrs).attisdropped);
        *sv.add(Anum_pg_attribute_attislocal as usize - 1) =
            BoolGetDatum((*attrs).attislocal);
        *sv.add(Anum_pg_attribute_attinhcount as usize - 1) =
            Int16GetDatum((*attrs).attinhcount);
        *sv.add(Anum_pg_attribute_attcollation as usize - 1) =
            ObjectIdGetDatum((*attrs).attcollation);

        if !attrs_extra.is_null() {
            *sv.add(Anum_pg_attribute_attstattarget as usize - 1) =
                (*attrs_extra).attstattarget.value;
            *sn.add(Anum_pg_attribute_attstattarget as usize - 1) =
                (*attrs_extra).attstattarget.isnull;

            *sv.add(Anum_pg_attribute_attoptions as usize - 1) =
                (*attrs_extra).attoptions.value;
            *sn.add(Anum_pg_attribute_attoptions as usize - 1) =
                (*attrs_extra).attoptions.isnull;
        } else {
            *sn.add(Anum_pg_attribute_attstattarget as usize - 1) = true;
            *sn.add(Anum_pg_attribute_attoptions as usize - 1) = true;
        }

        /*
         * The remaining fields are not set for new columns.
         */
        *sn.add(Anum_pg_attribute_attacl as usize - 1) = true;
        *sn.add(Anum_pg_attribute_attfdwoptions as usize - 1) = true;
        *sn.add(Anum_pg_attribute_attmissingval as usize - 1) = true;

        ExecStoreVirtualTuple(*slot.add(slotCount as usize));

        /*
         * If slots are full or the end of processing has been reached, insert
         * a batch of tuples.
         */
        if slotCount == nslots - 1 || natts == (*tupdesc).natts - 1 {
            /* fetch index info only when we know we need it */
            if indstate.is_null() {
                indstate = CatalogOpenIndexes(pg_attribute_rel);
                close_index = true;
            }

            /* insert the new tuples and update the indexes */
            CatalogTuplesMultiInsertWithInfo(
                pg_attribute_rel,
                slot,
                slotCount + 1,
                indstate,
            );
        }

        natts += 1;
    }

    if close_index {
        CatalogCloseIndexes(indstate);
    }
    for i in 0..nslots {
        ExecDropSingleTupleTableSlot(*slot.add(i as usize));
    }
    pfree(slot as *mut c_void);
}

/* --------------------------------
 *        AddNewAttributeTuples
 *
 *        this registers the new relation's schema by adding
 *        tuples to pg_attribute.
 * --------------------------------
 */
unsafe fn AddNewAttributeTuples(new_rel_oid: Oid, tupdesc: *mut TupleDescData, relkind: c_char) {
    let natts = (*tupdesc).natts;
    let mut myself: ObjectAddress = INVALID_OBJECT_ADDRESS;
    let mut referenced: ObjectAddress = INVALID_OBJECT_ADDRESS;

    /*
     * open pg_attribute and its indexes.
     */
    let rel = table_open(AttributeRelationId, RowExclusiveLock);
    let indstate = CatalogOpenIndexes(rel);

    InsertPgAttributeTuples(rel, tupdesc, new_rel_oid, core::ptr::null(), indstate);

    /* add dependencies on their datatypes and collations */
    for i in 0..natts {
        let attr = TupleDescAttr(tupdesc, i);

        /* Add dependency info */
        ObjectAddressSubSet(&mut myself, RelationRelationId, new_rel_oid, i + 1);
        ObjectAddressSet(&mut referenced, TypeRelationId, (*attr).atttypid);
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL); /* TODO(pg-port): dependency */

        /* The default collation is pinned, so don't bother recording it */
        if OidIsValid((*attr).attcollation) && (*attr).attcollation != DEFAULT_COLLATION_OID {
            ObjectAddressSet(&mut referenced, CollationRelationId, (*attr).attcollation);
            recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL); /* TODO(pg-port): dependency */
        }
    }

    /*
     * Next we add the system attributes.  Skip all for a view or type
     * relation.  We don't bother with making datatype dependencies here,
     * since presumably all these types are pinned.
     */
    if relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE {
        let td = CreateTupleDesc(
            SYS_ATT.0.len() as i32,
            SYS_ATT.0.as_ptr() as *mut *mut FormData_pg_attribute,
        );
        InsertPgAttributeTuples(rel, td, new_rel_oid, core::ptr::null(), indstate);
        FreeTupleDesc(td);
    }

    /*
     * clean up
     */
    CatalogCloseIndexes(indstate);
    table_close(rel, RowExclusiveLock);
}

/* --------------------------------
 *        InsertPgClassTuple
 *
 *        Construct and insert a new tuple in pg_class.
 *
 * Caller has already opened and locked pg_class.
 * Tuple data is taken from new_rel_desc->rd_rel, except for the
 * variable-width fields which are not present in a cached reldesc.
 * relacl and reloptions are passed in Datum form (to avoid having
 * to reference the data types in heap.h).  Pass (Datum) 0 to set them
 * to NULL.
 * --------------------------------
 */
pub unsafe fn InsertPgClassTuple(
    pg_class_desc: Relation,
    new_rel_desc: Relation,
    new_rel_oid: Oid,
    relacl: Datum,
    reloptions: Datum,
) {
    let rd_rel: Form_pg_class = (*new_rel_desc).rd_rel;
    let mut values: [Datum; Natts_pg_class] = [0; Natts_pg_class];
    let mut nulls: [bool; Natts_pg_class] = [false; Natts_pg_class];

    /* This is a tad tedious, but way cleaner than what we used to do... */
    values[Anum_pg_class_oid as usize - 1] = ObjectIdGetDatum(new_rel_oid);
    values[Anum_pg_class_relname as usize - 1] = NameGetDatum(&(*rd_rel).relname);
    values[Anum_pg_class_relnamespace as usize - 1] =
        ObjectIdGetDatum((*rd_rel).relnamespace);
    values[Anum_pg_class_reltype as usize - 1] = ObjectIdGetDatum((*rd_rel).reltype);
    values[Anum_pg_class_reloftype as usize - 1] = ObjectIdGetDatum((*rd_rel).reloftype);
    values[Anum_pg_class_relowner as usize - 1] = ObjectIdGetDatum((*rd_rel).relowner);
    values[Anum_pg_class_relam as usize - 1] = ObjectIdGetDatum((*rd_rel).relam);
    values[Anum_pg_class_relfilenode as usize - 1] =
        ObjectIdGetDatum((*rd_rel).relfilenode);
    values[Anum_pg_class_reltablespace as usize - 1] =
        ObjectIdGetDatum((*rd_rel).reltablespace);
    values[Anum_pg_class_relpages as usize - 1] = Int32GetDatum((*rd_rel).relpages);
    values[Anum_pg_class_reltuples as usize - 1] = Float4GetDatum((*rd_rel).reltuples);
    values[Anum_pg_class_relallvisible as usize - 1] =
        Int32GetDatum((*rd_rel).relallvisible);
    values[Anum_pg_class_relallfrozen as usize - 1] =
        Int32GetDatum((*rd_rel).relallfrozen);
    values[Anum_pg_class_reltoastrelid as usize - 1] =
        ObjectIdGetDatum((*rd_rel).reltoastrelid);
    values[Anum_pg_class_relhasindex as usize - 1] = BoolGetDatum((*rd_rel).relhasindex);
    values[Anum_pg_class_relisshared as usize - 1] = BoolGetDatum((*rd_rel).relisshared);
    values[Anum_pg_class_relpersistence as usize - 1] =
        CharGetDatum((*rd_rel).relpersistence);
    values[Anum_pg_class_relkind as usize - 1] = CharGetDatum((*rd_rel).relkind);
    values[Anum_pg_class_relnatts as usize - 1] = Int16GetDatum((*rd_rel).relnatts);
    values[Anum_pg_class_relchecks as usize - 1] = Int16GetDatum((*rd_rel).relchecks);
    values[Anum_pg_class_relhasrules as usize - 1] = BoolGetDatum((*rd_rel).relhasrules);
    values[Anum_pg_class_relhastriggers as usize - 1] =
        BoolGetDatum((*rd_rel).relhastriggers);
    values[Anum_pg_class_relrowsecurity as usize - 1] =
        BoolGetDatum((*rd_rel).relrowsecurity);
    values[Anum_pg_class_relforcerowsecurity as usize - 1] =
        BoolGetDatum((*rd_rel).relforcerowsecurity);
    values[Anum_pg_class_relhassubclass as usize - 1] =
        BoolGetDatum((*rd_rel).relhassubclass);
    values[Anum_pg_class_relispopulated as usize - 1] =
        BoolGetDatum((*rd_rel).relispopulated);
    values[Anum_pg_class_relreplident as usize - 1] =
        CharGetDatum((*rd_rel).relreplident);
    values[Anum_pg_class_relispartition as usize - 1] =
        BoolGetDatum((*rd_rel).relispartition);
    values[Anum_pg_class_relrewrite as usize - 1] =
        ObjectIdGetDatum((*rd_rel).relrewrite);
    values[Anum_pg_class_relfrozenxid as usize - 1] =
        TransactionIdGetDatum((*rd_rel).relfrozenxid);
    values[Anum_pg_class_relminmxid as usize - 1] =
        MultiXactIdGetDatum((*rd_rel).relminmxid);

    if relacl != 0 {
        values[Anum_pg_class_relacl as usize - 1] = relacl;
    } else {
        nulls[Anum_pg_class_relacl as usize - 1] = true;
    }
    if reloptions != 0 {
        values[Anum_pg_class_reloptions as usize - 1] = reloptions;
    } else {
        nulls[Anum_pg_class_reloptions as usize - 1] = true;
    }

    /* relpartbound is set by updating this tuple, if necessary */
    nulls[Anum_pg_class_relpartbound as usize - 1] = true;

    let tup = heap_form_tuple(RelationGetDescr(pg_class_desc), values.as_mut_ptr(), nulls.as_mut_ptr());

    /* finally insert the new tuple, update the indexes, and clean up */
    CatalogTupleInsert(pg_class_desc, tup);

    heap_freetuple(tup);
}

/* --------------------------------
 *        AddNewRelationTuple
 *
 *        this registers the new relation in the catalogs by
 *        adding a tuple to pg_class.
 * --------------------------------
 */
unsafe fn AddNewRelationTuple(
    pg_class_desc: Relation,
    new_rel_desc: Relation,
    new_rel_oid: Oid,
    new_type_oid: Oid,
    reloftype: Oid,
    relowner: Oid,
    relkind: c_char,
    relfrozenxid: TransactionId,
    relminmxid: TransactionId,
    relacl: Datum,
    reloptions: Datum,
) {
    /*
     * first we update some of the information in our uncataloged relation's
     * relation descriptor.
     */
    let new_rel_reltup: Form_pg_class = (*new_rel_desc).rd_rel;

    /* The relation is empty */
    (*new_rel_reltup).relpages = 0;
    (*new_rel_reltup).reltuples = -1.0;
    (*new_rel_reltup).relallvisible = 0;
    (*new_rel_reltup).relallfrozen = 0;

    /* Sequences always have a known size */
    if relkind == RELKIND_SEQUENCE {
        (*new_rel_reltup).relpages = 1;
        (*new_rel_reltup).reltuples = 1.0;
    }

    (*new_rel_reltup).relfrozenxid = relfrozenxid;
    (*new_rel_reltup).relminmxid = relminmxid;
    (*new_rel_reltup).relowner = relowner;
    (*new_rel_reltup).reltype = new_type_oid;
    (*new_rel_reltup).reloftype = reloftype;

    /* relispartition is always set by updating this tuple later */
    (*new_rel_reltup).relispartition = false;

    /* fill rd_att's type ID with something sane even if reltype is zero */
    (*(*new_rel_desc).rd_att).tdtypeid = if new_type_oid != 0 { new_type_oid } else { RECORDOID };
    (*(*new_rel_desc).rd_att).tdtypmod = -1;

    /* Now build and insert the tuple */
    InsertPgClassTuple(pg_class_desc, new_rel_desc, new_rel_oid, relacl, reloptions);
}


/* --------------------------------
 *        AddNewRelationType -
 *
 *        define a composite type corresponding to the new relation
 * --------------------------------
 */
unsafe fn AddNewRelationType(
    type_name: *const c_char,
    type_namespace: Oid,
    new_rel_oid: Oid,
    new_rel_kind: c_char,
    ownerid: Oid,
    new_row_type: Oid,
    new_array_type: Oid,
) -> ObjectAddress {
    TypeCreate(
        new_row_type,           /* optional predetermined OID */
        type_name,              /* type name */
        type_namespace,         /* type namespace */
        new_rel_oid,            /* relation oid */
        new_rel_kind,           /* relation kind */
        ownerid,                /* owner's ID */
        -1,                     /* internal size (varlena) */
        TYPTYPE_COMPOSITE,      /* type-type (composite) */
        TYPCATEGORY_COMPOSITE,  /* type-category (ditto) */
        false,                  /* composite types are never preferred */
        DEFAULT_TYPDELIM,       /* default array delimiter */
        F_RECORD_IN,            /* input procedure */
        F_RECORD_OUT,           /* output procedure */
        F_RECORD_RECV,          /* receive procedure */
        F_RECORD_SEND,          /* send procedure */
        InvalidOid,             /* typmodin procedure - none */
        InvalidOid,             /* typmodout procedure - none */
        InvalidOid,             /* analyze procedure - default */
        InvalidOid,             /* subscript procedure - none */
        InvalidOid,             /* array element type - irrelevant */
        false,                  /* this is not an array type */
        new_array_type,         /* array type if any */
        InvalidOid,             /* domain base type - irrelevant */
        core::ptr::null(),      /* default value - none */
        core::ptr::null(),      /* default binary representation */
        false,                  /* passed by reference */
        TYPALIGN_DOUBLE,        /* alignment - must be the largest! */
        TYPSTORAGE_EXTENDED,    /* fully TOASTable */
        -1,                     /* typmod */
        0,                      /* array dimensions for typBaseType */
        false,                  /* Type NOT NULL */
        InvalidOid,             /* rowtypes never have a collation */
    )
}

/* --------------------------------
 *        heap_create_with_catalog
 *
 *        creates a new cataloged relation.  see comments above.
 *
 * Arguments:
 *    relname: name to give to new rel
 *    relnamespace: OID of namespace it goes in
 *    reltablespace: OID of tablespace it goes in
 *    relid: OID to assign to new rel, or InvalidOid to select a new OID
 *    reltypeid: OID to assign to rel's rowtype, or InvalidOid to select one
 *    reloftypeid: if a typed table, OID of underlying type; else InvalidOid
 *    ownerid: OID of new rel's owner
 *    accessmtd: OID of new rel's access method
 *    tupdesc: tuple descriptor (source of column definitions)
 *    cooked_constraints: list of precooked check constraints and defaults
 *    relkind: relkind for new rel
 *    relpersistence: rel's persistence status (permanent, temp, or unlogged)
 *    shared_relation: true if it's to be a shared relation
 *    mapped_relation: true if the relation will use the relfilenumber map
 *    oncommit: ON COMMIT marking (only relevant if it's a temp table)
 *    reloptions: reloptions in Datum form, or (Datum) 0 if none
 *    use_user_acl: true if should look for user-defined default permissions;
 *        if false, relacl is always set NULL
 *    allow_system_table_mods: true to allow creation in system namespaces
 *    is_internal: is this a system-generated catalog?
 *    relrewrite: link to original relation during a table rewrite
 *
 * Output parameters:
 *    typaddress: if not null, gets the object address of the new pg_type entry
 *    (this must be null if the relkind is one that doesn't get a pg_type entry)
 *
 * Returns the OID of the new relation
 * --------------------------------
 */
pub unsafe fn heap_create_with_catalog(
    relname: *const c_char,
    relnamespace: Oid,
    reltablespace: Oid,
    relid: Oid,
    reltypeid: Oid,
    reloftypeid: Oid,
    ownerid: Oid,
    accessmtd: Oid,
    tupdesc: *mut TupleDescData,
    cooked_constraints: *mut List,
    relkind: c_char,
    relpersistence: c_char,
    shared_relation: bool,
    mapped_relation: bool,
    oncommit: OnCommitAction,
    reloptions: Datum,
    use_user_acl: bool,
    allow_system_table_mods: bool,
    is_internal: bool,
    relrewrite: Oid,
    typaddress: *mut ObjectAddress,
) -> Oid {
    let pg_class_desc = table_open(RelationRelationId, RowExclusiveLock);

    /*
     * sanity checks
     */
    Assert!(IsNormalProcessingMode() || IsBootstrapProcessingMode());

    /*
     * Validate proposed tupdesc for the desired relkind.  If
     * allow_system_table_mods is on, allow ANYARRAY to be used; this is a
     * hack to allow creating pg_statistic and cloning it during VACUUM FULL.
     */
    CheckAttributeNamesTypes(
        tupdesc,
        relkind,
        if allow_system_table_mods { CHKATYPE_ANYARRAY } else { 0 },
    );

    /*
     * This would fail later on anyway, if the relation already exists.  But
     * by catching it here we can emit a nicer error message.
     */
    let existing_relid = get_relname_relid(relname, relnamespace);
    if existing_relid != InvalidOid {
        ereport!(ERROR, errmsg!("relation \"{}\" already exists", CStr_to_str(relname))) /* C also: errcode */;
    }

    /*
     * Since we are going to create a rowtype as well, also check for
     * collision with an existing type name.  If there is one and it's an
     * autogenerated array, we can rename it out of the way; otherwise we can
     * at least give a good error message.
     */
    let old_type_oid = GetSysCacheOid2(
        TYPENAMENSP,
        Anum_pg_type_oid,
        CStringGetDatum(relname),
        ObjectIdGetDatum(relnamespace),
    );
    if OidIsValid(old_type_oid) {
        if !moveArrayTypeName(old_type_oid, relname, relnamespace) {
            /* TODO(pg-port): typecmds */
            ereport!(ERROR, errmsg!("type \"{}\" already exists", CStr_to_str(relname))) /* C also: errcode, errhint */;
        }
    }

    /*
     * Shared relations must be in pg_global (last-ditch check)
     */
    if shared_relation && reltablespace != GLOBALTABLESPACE_OID {
        elog!(ERROR, "shared relations must be placed in pg_global tablespace");
    }

    /*
     * Allocate an OID for the relation, unless we were told what to use.
     *
     * The OID will be the relfilenumber as well, so make sure it doesn't
     * collide with either pg_class OIDs or existing physical files.
     */
    let mut relid = relid;
    let mut relfilenumber: RelFileNumber = InvalidRelFileNumber;
    if !OidIsValid(relid) {
        /* Use binary-upgrade override for pg_class.oid and relfilenumber */
        if IsBinaryUpgrade {
            /*
             * Indexes are not supported here; they use
             * binary_upgrade_next_index_pg_class_oid.
             */
            Assert!(relkind != RELKIND_INDEX);
            Assert!(relkind != RELKIND_PARTITIONED_INDEX);

            if relkind == RELKIND_TOASTVALUE {
                /* There might be no TOAST table, so we have to test for it. */
                if OidIsValid(binary_upgrade_next_toast_pg_class_oid) {
                    relid = binary_upgrade_next_toast_pg_class_oid;
                    binary_upgrade_next_toast_pg_class_oid = InvalidOid;

                    if !RelFileNumberIsValid(
                        binary_upgrade_next_toast_pg_class_relfilenumber,
                    ) {
                        ereport!(ERROR, errmsg!("toast relfilenumber value not set when in binary upgrade mode")) /* C also: errcode */;
                    }

                    relfilenumber = binary_upgrade_next_toast_pg_class_relfilenumber;
                    binary_upgrade_next_toast_pg_class_relfilenumber =
                        InvalidRelFileNumber;
                }
            } else {
                if !OidIsValid(binary_upgrade_next_heap_pg_class_oid) {
                    ereport!(ERROR, errmsg!("pg_class heap OID value not set when in binary upgrade mode")) /* C also: errcode */;
                }

                relid = binary_upgrade_next_heap_pg_class_oid;
                binary_upgrade_next_heap_pg_class_oid = InvalidOid;

                if RELKIND_HAS_STORAGE(relkind) {
                    if !RelFileNumberIsValid(
                        binary_upgrade_next_heap_pg_class_relfilenumber,
                    ) {
                        ereport!(ERROR, errmsg!(
                                "relfilenumber value not set when in binary upgrade mode"
                            )) /* C also: errcode */;
                    }

                    relfilenumber = binary_upgrade_next_heap_pg_class_relfilenumber;
                    binary_upgrade_next_heap_pg_class_relfilenumber =
                        InvalidRelFileNumber;
                }
            }
        }

        if !OidIsValid(relid) {
            relid = GetNewRelFileNumber(reltablespace, pg_class_desc, relpersistence);
        }
    }

    /*
     * Other sessions' catalog scans can't find this until we commit.  Hence,
     * it doesn't hurt to hold AccessExclusiveLock.  Do it here so callers
     * can't accidentally vary in their lock mode or acquisition timing.
     */
    LockRelationOid(relid, AccessExclusiveLock);

    /*
     * Determine the relation's initial permissions.
     */
    let relacl: *mut Acl;
    if use_user_acl {
        relacl = match relkind {
            RELKIND_RELATION
            | RELKIND_VIEW
            | RELKIND_MATVIEW
            | RELKIND_FOREIGN_TABLE
            | RELKIND_PARTITIONED_TABLE => {
                get_user_default_acl(OBJECT_TABLE, ownerid, relnamespace)
            }
            RELKIND_SEQUENCE => {
                get_user_default_acl(OBJECT_SEQUENCE, ownerid, relnamespace)
            }
            _ => core::ptr::null_mut(),
        };
    } else {
        relacl = core::ptr::null_mut();
    }

    /*
     * Create the relcache entry (mostly dummy at this point) and the physical
     * disk file.  (If we fail further down, it's the smgr's responsibility to
     * remove the disk file again.)
     *
     * NB: Note that passing create_storage = true is correct even for binary
     * upgrade.  The storage we create here will be replaced later, but we
     * need to have something on disk in the meanwhile.
     */
    let mut relfrozenxid: TransactionId = InvalidTransactionId;
    let mut relminmxid: MultiXactId = InvalidMultiXactId;

    let new_rel_desc = heap_create(
        relname,
        relnamespace,
        reltablespace,
        relid,
        relfilenumber,
        accessmtd,
        tupdesc,
        relkind,
        relpersistence,
        shared_relation,
        mapped_relation,
        allow_system_table_mods,
        &mut relfrozenxid,
        &mut relminmxid,
        true,
    );

    Assert!(relid == RelationGetRelid(new_rel_desc));

    (*(*new_rel_desc).rd_rel).relrewrite = relrewrite;

    /*
     * Decide whether to create a pg_type entry for the relation's rowtype.
     * These types are made except where the use of a relation as such is an
     * implementation detail: toast tables, sequences and indexes.
     */
    let new_type_oid: Oid;
    if !(relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_PARTITIONED_INDEX)
    {
        /*
         * We'll make an array over the composite type, too.  For largely
         * historical reasons, the array type's OID is assigned first.
         */
        let new_array_oid = AssignTypeArrayOid(); /* TODO(pg-port): typecmds */

        /*
         * Make the pg_type entry for the composite type.  The OID of the
         * composite type can be preselected by the caller, but if reltypeid
         * is InvalidOid, we'll generate a new OID for it.
         *
         * NOTE: we could get a unique-index failure here, in case someone
         * else is creating the same type name in parallel but hadn't
         * committed yet when we checked for a duplicate name above.
         */
        let new_type_addr = AddNewRelationType(
            relname,
            relnamespace,
            relid,
            relkind,
            ownerid,
            reltypeid,
            new_array_oid,
        );
        new_type_oid = new_type_addr.objectId;
        if !typaddress.is_null() {
            *typaddress = new_type_addr;
        }

        /* Now create the array type. */
        let relarrayname = makeArrayTypeName(relname, relnamespace); /* TODO(pg-port): typecmds */

        TypeCreate(
            new_array_oid,          /* force the type's OID to this */
            relarrayname,           /* Array type name */
            relnamespace,           /* Same namespace as parent */
            InvalidOid,             /* Not composite, no relationOid */
            0,                      /* relkind, also N/A here */
            ownerid,                /* owner's ID */
            -1,                     /* Internal size (varlena) */
            TYPTYPE_BASE,           /* Not composite - typelem is */
            TYPCATEGORY_ARRAY,      /* type-category (array) */
            false,                  /* array types are never preferred */
            DEFAULT_TYPDELIM,       /* default array delimiter */
            F_ARRAY_IN,             /* array input proc */
            F_ARRAY_OUT,            /* array output proc */
            F_ARRAY_RECV,           /* array recv (bin) proc */
            F_ARRAY_SEND,           /* array send (bin) proc */
            InvalidOid,             /* typmodin procedure - none */
            InvalidOid,             /* typmodout procedure - none */
            F_ARRAY_TYPANALYZE,     /* array analyze procedure */
            F_ARRAY_SUBSCRIPT_HANDLER, /* array subscript procedure */
            new_type_oid,           /* array element type - the rowtype */
            true,                   /* yes, this is an array type */
            InvalidOid,             /* this has no array type */
            InvalidOid,             /* domain base type - irrelevant */
            core::ptr::null(),      /* default value - none */
            core::ptr::null(),      /* default binary representation */
            false,                  /* passed by reference */
            TYPALIGN_DOUBLE,        /* alignment - must be the largest! */
            TYPSTORAGE_EXTENDED,    /* fully TOASTable */
            -1,                     /* typmod */
            0,                      /* array dimensions for typBaseType */
            false,                  /* Type NOT NULL */
            InvalidOid,             /* rowtypes never have a collation */
        );

        pfree(relarrayname as *mut c_void);
    } else {
        /* Caller should not be expecting a type to be created. */
        Assert!(reltypeid == InvalidOid);
        Assert!(typaddress.is_null());

        new_type_oid = InvalidOid;
    }

    /*
     * now create an entry in pg_class for the relation.
     *
     * NOTE: we could get a unique-index failure here, in case someone else is
     * creating the same relation name in parallel but hadn't committed yet
     * when we checked for a duplicate name above.
     */
    AddNewRelationTuple(
        pg_class_desc,
        new_rel_desc,
        relid,
        new_type_oid,
        reloftypeid,
        ownerid,
        relkind,
        relfrozenxid,
        relminmxid,
        PointerGetDatum(relacl as *const c_void),
        reloptions,
    );

    /*
     * now add tuples to pg_attribute for the attributes in our new relation.
     */
    AddNewAttributeTuples(relid, (*new_rel_desc).rd_att, relkind);

    /*
     * Make a dependency link to force the relation to be deleted if its
     * namespace is.  Also make a dependency link to its owner, as well as
     * dependencies for any roles mentioned in the default ACL.
     *
     * For composite types, these dependencies are tracked for the pg_type
     * entry, so we needn't record them here.  Likewise, TOAST tables don't
     * need a namespace dependency (they live in a pinned namespace) nor an
     * owner dependency (they depend indirectly through the parent table), nor
     * should they have any ACL entries.  The same applies for extension
     * dependencies.
     *
     * Also, skip this in bootstrap mode, since we don't make dependencies
     * while bootstrapping.
     */
    if relkind != RELKIND_COMPOSITE_TYPE
        && relkind != RELKIND_TOASTVALUE
        && !IsBootstrapProcessingMode()
    {
        let mut myself: ObjectAddress = INVALID_OBJECT_ADDRESS;
        let mut referenced: ObjectAddress = INVALID_OBJECT_ADDRESS;

        ObjectAddressSet(&mut myself, RelationRelationId, relid);

        recordDependencyOnOwner(RelationRelationId, relid, ownerid); /* TODO(pg-port): dependency */

        recordDependencyOnNewAcl(RelationRelationId, relid, 0, ownerid, relacl); /* TODO(pg-port): dependency */

        recordDependencyOnCurrentExtension(&myself, false); /* TODO(pg-port): dependency */

        let addrs = new_object_addresses();

        ObjectAddressSet(&mut referenced, NamespaceRelationId, relnamespace);
        add_exact_object_address(&referenced, addrs);

        if OidIsValid(reloftypeid) {
            ObjectAddressSet(&mut referenced, TypeRelationId, reloftypeid);
            add_exact_object_address(&referenced, addrs);
        }

        /*
         * Make a dependency link to force the relation to be deleted if its
         * access method is.
         *
         * No need to add an explicit dependency for the toast table, as the
         * main table depends on it.  Partitioned tables may not have an
         * access method set.
         */
        if (RELKIND_HAS_TABLE_AM(relkind) && relkind != RELKIND_TOASTVALUE)
            || (relkind == RELKIND_PARTITIONED_TABLE && OidIsValid(accessmtd))
        {
            ObjectAddressSet(&mut referenced, AccessMethodRelationId, accessmtd);
            add_exact_object_address(&referenced, addrs);
        }

        record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL); /* TODO(pg-port): dependency */
        free_object_addresses(addrs);
    }

    /* Post creation hook for new relation */
    InvokeObjectPostCreateHookArg(RelationRelationId, relid, 0, is_internal);

    /*
     * Store any supplied CHECK constraints and defaults.
     *
     * NB: this may do a CommandCounterIncrement and rebuild the relcache
     * entry, so the relation must be valid and self-consistent at this point.
     * In particular, there are not yet constraints and defaults anywhere.
     */
    StoreConstraints(new_rel_desc, cooked_constraints, is_internal);

    /*
     * If there's a special on-commit action, remember it
     */
    if oncommit != ONCOMMIT_NOOP {
        register_on_commit_action(relid, oncommit);
    }

    /*
     * ok, the relation has been cataloged, so close our relations and return
     * the OID of the newly created relation.
     */
    table_close(new_rel_desc, NoLock); /* do not unlock till end of xact */
    table_close(pg_class_desc, RowExclusiveLock);

    relid
}

/*
 *        RelationRemoveInheritance
 *
 * Formerly, this routine checked for child relations and aborted the
 * deletion if any were found.  Now we rely on the dependency mechanism
 * to check for or delete child relations.  By the time we get here,
 * there are no children and we need only remove any pg_inherits rows
 * linking this relation to its parent(s).
 */
unsafe fn RelationRemoveInheritance(relid: Oid) {
    let catalog_relation = table_open(InheritsRelationId, RowExclusiveLock);
    let mut key: ScanKeyData = core::mem::zeroed();

    ScanKeyInit(
        &mut key,
        Anum_pg_inherits_inhrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );

    let scan = systable_beginscan(
        catalog_relation,
        InheritsRelidSeqnoIndexId,
        true,
        core::ptr::null_mut(),
        1,
        &mut key,
    );

    let mut tuple: HeapTuple;
    loop {
        tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            break;
        }
        CatalogTupleDelete(catalog_relation, &mut (*tuple).t_self);
    }

    systable_endscan(scan);
    table_close(catalog_relation, RowExclusiveLock);
}

/*
 *        DeleteRelationTuple
 *
 * Remove pg_class row for the given relid.
 *
 * Note: this is shared by relation deletion and index deletion.  It's
 * not intended for use anyplace else.
 */
pub unsafe fn DeleteRelationTuple(relid: Oid) {
    /* Grab an appropriate lock on the pg_class relation */
    let pg_class_desc = table_open(RelationRelationId, RowExclusiveLock);

    let tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }

    /* delete the relation tuple from pg_class, and finish up */
    CatalogTupleDelete(pg_class_desc, &mut (*tup).t_self);

    ReleaseSysCache(tup);

    table_close(pg_class_desc, RowExclusiveLock);
}

/*
 *        DeleteAttributeTuples
 *
 * Remove pg_attribute rows for the given relid.
 *
 * Note: this is shared by relation deletion and index deletion.  It's
 * not intended for use anyplace else.
 */
pub unsafe fn DeleteAttributeTuples(relid: Oid) {
    /* Grab an appropriate lock on the pg_attribute relation */
    let attrel = table_open(AttributeRelationId, RowExclusiveLock);

    /* Use the index to scan only attributes of the target relation */
    let mut key: [ScanKeyData; 1] = [core::mem::zeroed(); 1];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );

    let scan = systable_beginscan(attrel, AttributeRelidNumIndexId, true, core::ptr::null_mut(), 1, key.as_mut_ptr());

    /* Delete all the matching tuples */
    loop {
        let atttup = systable_getnext(scan);
        if atttup.is_null() {
            break;
        }
        CatalogTupleDelete(attrel, &mut (*atttup).t_self);
    }

    /* Clean up after the scan */
    systable_endscan(scan);
    table_close(attrel, RowExclusiveLock);
}

/*
 *        DeleteSystemAttributeTuples
 *
 * Remove pg_attribute rows for system columns of the given relid.
 *
 * Note: this is only used when converting a table to a view.  Views don't
 * have system columns, so we should remove them from pg_attribute.
 */
pub unsafe fn DeleteSystemAttributeTuples(relid: Oid) {
    /* Grab an appropriate lock on the pg_attribute relation */
    let attrel = table_open(AttributeRelationId, RowExclusiveLock);

    /* Use the index to scan only system attributes of the target relation */
    let mut key: [ScanKeyData; 2] = [core::mem::zeroed(); 2];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_attribute_attnum,
        BTLessEqualStrategyNumber,
        F_INT2LE,
        Int16GetDatum(0),
    );

    let scan = systable_beginscan(attrel, AttributeRelidNumIndexId, true, core::ptr::null_mut(), 2, key.as_mut_ptr());

    /* Delete all the matching tuples */
    loop {
        let atttup = systable_getnext(scan);
        if atttup.is_null() {
            break;
        }
        CatalogTupleDelete(attrel, &mut (*atttup).t_self);
    }

    /* Clean up after the scan */
    systable_endscan(scan);
    table_close(attrel, RowExclusiveLock);
}

/*
 *        RemoveAttributeById
 *
 * This is the guts of ALTER TABLE DROP COLUMN: actually mark the attribute
 * deleted in pg_attribute.  We also remove pg_statistic entries for it.
 * (Everything else needed, such as getting rid of any pg_attrdef entry,
 * is handled by dependency.c.)
 */
pub unsafe fn RemoveAttributeById(relid: Oid, attnum: AttrNumber) {
    let mut values_att: [Datum; Natts_pg_attribute] = [0; Natts_pg_attribute];
    let mut nulls_att: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
    let mut replaces_att: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];

    /*
     * Grab an exclusive lock on the target table, which we will NOT release
     * until end of transaction.  (In the simple case where we are directly
     * dropping this column, ATExecDropColumn already did this ... but when
     * cascading from a drop of some other object, we may not have any lock.)
     */
    let rel = relation_open(relid, AccessExclusiveLock);
    let attr_rel = table_open(AttributeRelationId, RowExclusiveLock);

    let mut tuple = SearchSysCacheCopy2(
        ATTNUM,
        ObjectIdGetDatum(relid),
        Int16GetDatum(attnum),
    );
    if !HeapTupleIsValid(tuple) {
        /* shouldn't happen */
        elog!(
            ERROR,
            "cache lookup failed for attribute {} of relation {}",
            attnum,
            relid
        );
    }
    let att_struct: Form_pg_attribute = GETSTRUCT(tuple) as Form_pg_attribute;

    /* Mark the attribute as dropped */
    (*att_struct).attisdropped = true;

    /*
     * Set the type OID to invalid.  A dropped attribute's type link cannot be
     * relied on (once the attribute is dropped, the type might be too).
     * Fortunately we do not need the type row --- the only really essential
     * information is the type's typlen and typalign, which are preserved in
     * the attribute's attlen and attalign.  We set atttypid to zero here as a
     * means of catching code that incorrectly expects it to be valid.
     */
    (*att_struct).atttypid = InvalidOid;

    /* Remove any not-null constraint the column may have */
    (*att_struct).attnotnull = false;

    /* Unset this so no one tries to look up the generation expression */
    (*att_struct).attgenerated = b'\0' as c_char;

    /*
     * Change the column name to something that isn't likely to conflict
     */
    let mut newattname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    libc_snprintf(
        newattname.as_mut_ptr(),
        NAMEDATALEN,
        "........pg.dropped.{}........\0".as_ptr() as *const c_char,
        attnum as c_int,
    );
    namestrcpy(&mut (*att_struct).attname, newattname.as_ptr());

    /* Clear the missing value */
    (*att_struct).atthasmissing = false;
    nulls_att[Anum_pg_attribute_attmissingval as usize - 1] = true;
    replaces_att[Anum_pg_attribute_attmissingval as usize - 1] = true;

    /*
     * Clear the other nullable fields.  This saves some space in pg_attribute
     * and removes no longer useful information.
     */
    nulls_att[Anum_pg_attribute_attstattarget as usize - 1] = true;
    replaces_att[Anum_pg_attribute_attstattarget as usize - 1] = true;
    nulls_att[Anum_pg_attribute_attacl as usize - 1] = true;
    replaces_att[Anum_pg_attribute_attacl as usize - 1] = true;
    nulls_att[Anum_pg_attribute_attoptions as usize - 1] = true;
    replaces_att[Anum_pg_attribute_attoptions as usize - 1] = true;
    nulls_att[Anum_pg_attribute_attfdwoptions as usize - 1] = true;
    replaces_att[Anum_pg_attribute_attfdwoptions as usize - 1] = true;

    tuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(attr_rel),
        values_att.as_mut_ptr(),
        nulls_att.as_mut_ptr(),
        replaces_att.as_mut_ptr(),
    );

    CatalogTupleUpdate(attr_rel, &mut (*tuple).t_self, tuple);

    /*
     * Because updating the pg_attribute row will trigger a relcache flush for
     * the target relation, we need not do anything else to notify other
     * backends of the change.
     */

    table_close(attr_rel, RowExclusiveLock);

    RemoveStatistics(relid, attnum);

    relation_close(rel, NoLock);
}

/*
 * heap_drop_with_catalog    - removes specified relation from catalogs
 *
 * Note that this routine is not responsible for dropping objects that are
 * linked to the pg_class entry via dependencies (for example, indexes and
 * constraints).  Those are deleted by the dependency-tracing logic in
 * dependency.c before control gets here.  In general, therefore, this routine
 * should never be called directly; go through performDeletion() instead.
 */
pub unsafe fn heap_drop_with_catalog(relid: Oid) {
    let mut parent_oid: Oid = InvalidOid;
    let mut default_part_oid: Oid = InvalidOid;

    /*
     * To drop a partition safely, we must grab exclusive lock on its parent,
     * because another backend might be about to execute a query on the parent
     * table.  If it relies on previously cached partition descriptor, then it
     * could attempt to access the just-dropped relation as its partition. We
     * must therefore take a table lock strong enough to prevent all queries
     * on the table from proceeding until we commit and send out a
     * shared-cache-inval notice that will make them update their partition
     * descriptors.
     */
    let tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    if (*(GETSTRUCT(tuple) as Form_pg_class)).relispartition {
        /*
         * We have to lock the parent if the partition is being detached,
         * because it's possible that some query still has a partition
         * descriptor that includes this partition.
         */
        parent_oid = get_partition_parent(relid, true); /* TODO(pg-port): partitioning */
        LockRelationOid(parent_oid, AccessExclusiveLock);

        /*
         * If this is not the default partition, dropping it will change the
         * default partition's partition constraint, so we must lock it.
         */
        default_part_oid = get_default_partition_oid(parent_oid); /* TODO(pg-port): partitioning */
        if OidIsValid(default_part_oid) && relid != default_part_oid {
            LockRelationOid(default_part_oid, AccessExclusiveLock);
        }
    }

    ReleaseSysCache(tuple);

    /*
     * Open and lock the relation.
     */
    let rel = relation_open(relid, AccessExclusiveLock);

    /*
     * There can no longer be anyone *else* touching the relation, but we
     * might still have open queries or cursors, or pending trigger events, in
     * our own session.
     */
    CheckTableNotInUse(rel, b"DROP TABLE\0".as_ptr() as *const c_char);

    /*
     * This effectively deletes all rows in the table, and may be done in a
     * serializable transaction.  In that case we must record a rw-conflict in
     * to this transaction from each transaction holding a predicate lock on
     * the table.
     */
    CheckTableForSerializableConflictIn(rel);

    /*
     * Delete pg_foreign_table tuple first.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE {
        let ftrel = table_open(ForeignTableRelationId, RowExclusiveLock);

        let fttuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
        if !HeapTupleIsValid(fttuple) {
            elog!(ERROR, "cache lookup failed for foreign table {}", relid);
        }

        CatalogTupleDelete(ftrel, &mut (*fttuple).t_self);

        ReleaseSysCache(fttuple);
        table_close(ftrel, RowExclusiveLock);
    }

    /*
     * If a partitioned table, delete the pg_partitioned_table tuple.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        RemovePartitionKeyByRelId(relid);
    }

    /*
     * If the relation being dropped is the default partition itself,
     * invalidate its entry in pg_partitioned_table.
     */
    if relid == default_part_oid {
        update_default_partition_oid(parent_oid, InvalidOid); /* TODO(pg-port): partitioning */
    }

    /*
     * Schedule unlinking of the relation's physical files at commit.
     */
    if RELKIND_HAS_STORAGE((*(*rel).rd_rel).relkind) {
        RelationDropStorage(rel as *mut c_void); /* TODO(pg-port): catalog/storage.c */
    }

    /* ensure that stats are dropped if transaction commits */
    pgstat_drop_relation(rel);

    /*
     * Close relcache entry, but *keep* AccessExclusiveLock on the relation
     * until transaction commit.  This ensures no one else will try to do
     * something with the doomed relation.
     */
    relation_close(rel, NoLock);

    /*
     * Remove any associated relation synchronization states.
     */
    RemoveSubscriptionRel(InvalidOid, relid);

    /*
     * Forget any ON COMMIT action for the rel
     */
    remove_on_commit_action(relid);

    /*
     * Flush the relation from the relcache.  We want to do this before
     * starting to remove catalog entries, just to be certain that no relcache
     * entry rebuild will happen partway through.  (That should not really
     * matter, since we don't do CommandCounterIncrement here, but let's be
     * safe.)
     */
    RelationForgetRelation(relid);

    /*
     * remove inheritance information
     */
    RelationRemoveInheritance(relid);

    /*
     * delete statistics
     */
    RemoveStatistics(relid, 0);

    /*
     * delete attribute tuples
     */
    DeleteAttributeTuples(relid);

    /*
     * delete relation tuple
     */
    DeleteRelationTuple(relid);

    if OidIsValid(parent_oid) {
        /*
         * If this is not the default partition, the partition constraint of
         * the default partition has changed to include the portion of the key
         * space previously covered by the dropped partition.
         */
        if OidIsValid(default_part_oid) && relid != default_part_oid {
            CacheInvalidateRelcacheByRelid(default_part_oid);
        }

        /*
         * Invalidate the parent's relcache so that the partition is no longer
         * included in its partition descriptor.
         */
        CacheInvalidateRelcacheByRelid(parent_oid);
        /* keep the lock */
    }
}


/*
 * RelationClearMissing
 *
 * Set atthasmissing and attmissingval to false/null for all attributes
 * where they are currently set. This can be safely and usefully done if
 * the table is rewritten (e.g. by VACUUM FULL or CLUSTER) where we know there
 * are no rows left with less than a full complement of attributes.
 *
 * The caller must have an AccessExclusive lock on the relation.
 */
pub unsafe fn RelationClearMissing(rel: Relation) {
    let relid = RelationGetRelid(rel);
    let natts = RelationGetNumberOfAttributes(rel);
    let mut repl_val: [Datum; Natts_pg_attribute] = [0; Natts_pg_attribute];
    let mut repl_null: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
    let mut repl_repl: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];

    repl_val[Anum_pg_attribute_atthasmissing as usize - 1] = BoolGetDatum(false);
    repl_null[Anum_pg_attribute_attmissingval as usize - 1] = true;

    repl_repl[Anum_pg_attribute_atthasmissing as usize - 1] = true;
    repl_repl[Anum_pg_attribute_attmissingval as usize - 1] = true;

    /* Get a lock on pg_attribute */
    let attr_rel = table_open(AttributeRelationId, RowExclusiveLock);

    /* process each non-system attribute, including any dropped columns */
    for attnum in 1..=natts {
        let tuple = SearchSysCache2(
            ATTNUM,
            ObjectIdGetDatum(relid),
            Int16GetDatum(attnum as i16),
        );
        if !HeapTupleIsValid(tuple) {
            /* shouldn't happen */
            elog!(
                ERROR,
                "cache lookup failed for attribute {} of relation {}",
                attnum,
                relid
            );
        }

        let attrtuple: Form_pg_attribute = GETSTRUCT(tuple) as Form_pg_attribute;

        /* ignore any where atthasmissing is not true */
        if (*attrtuple).atthasmissing {
            let newtuple = heap_modify_tuple(
                tuple,
                RelationGetDescr(attr_rel),
                repl_val.as_mut_ptr(),
                repl_null.as_mut_ptr(),
                repl_repl.as_mut_ptr(),
            );

            CatalogTupleUpdate(attr_rel, &mut (*newtuple).t_self, newtuple);

            heap_freetuple(newtuple);
        }

        ReleaseSysCache(tuple);
    }

    /*
     * Our update of the pg_attribute rows will force a relcache rebuild, so
     * there's nothing else to do here.
     */
    table_close(attr_rel, RowExclusiveLock);
}

/*
 * StoreAttrMissingVal
 *
 * Set the missing value of a single attribute.
 */
pub unsafe fn StoreAttrMissingVal(rel: Relation, attnum: AttrNumber, mut missingval: Datum) {
    let mut values_att: [Datum; Natts_pg_attribute] = [0; Natts_pg_attribute];
    let mut nulls_att: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
    let mut replaces_att: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];

    /* This is only supported for plain tables */
    Assert!((*(*rel).rd_rel).relkind == RELKIND_RELATION);

    /* Fetch the pg_attribute row */
    let attrrel = table_open(AttributeRelationId, RowExclusiveLock);

    let atttup = SearchSysCache2(
        ATTNUM,
        ObjectIdGetDatum(RelationGetRelid(rel)),
        Int16GetDatum(attnum),
    );
    if !HeapTupleIsValid(atttup) {
        /* shouldn't happen */
        elog!(
            ERROR,
            "cache lookup failed for attribute {} of relation {}",
            attnum,
            RelationGetRelid(rel)
        );
    }
    let att_struct: Form_pg_attribute = GETSTRUCT(atttup) as Form_pg_attribute;

    /* Make a one-element array containing the value */
    missingval = PointerGetDatum(construct_array(
        &mut missingval,
        1,
        (*att_struct).atttypid,
        (*att_struct).attlen as i32,
        (*att_struct).attbyval,
        (*att_struct).attalign,
    ) as *const c_void);

    /* Update the pg_attribute row */
    values_att[Anum_pg_attribute_atthasmissing as usize - 1] = BoolGetDatum(true);
    replaces_att[Anum_pg_attribute_atthasmissing as usize - 1] = true;

    values_att[Anum_pg_attribute_attmissingval as usize - 1] = missingval;
    replaces_att[Anum_pg_attribute_attmissingval as usize - 1] = true;

    let newtup = heap_modify_tuple(
        atttup,
        RelationGetDescr(attrrel),
        values_att.as_mut_ptr(),
        nulls_att.as_mut_ptr(),
        replaces_att.as_mut_ptr(),
    );
    CatalogTupleUpdate(attrrel, &mut (*newtup).t_self, newtup);

    /* clean up */
    ReleaseSysCache(atttup);
    table_close(attrrel, RowExclusiveLock);
}

/*
 * SetAttrMissing
 *
 * Set the missing value of a single attribute. This should only be used by
 * binary upgrade. Takes an AccessExclusive lock on the relation owning the
 * attribute.
 */
pub unsafe fn SetAttrMissing(relid: Oid, attname: *mut c_char, value: *mut c_char) {
    let mut values_att: [Datum; Natts_pg_attribute] = [0; Natts_pg_attribute];
    let mut nulls_att: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];
    let mut replaces_att: [bool; Natts_pg_attribute] = [false; Natts_pg_attribute];

    /* lock the table the attribute belongs to */
    let tablerel = table_open(relid, AccessExclusiveLock);

    /* Don't do anything unless it's a plain table */
    if (*(*tablerel).rd_rel).relkind != RELKIND_RELATION {
        table_close(tablerel, AccessExclusiveLock);
        return;
    }

    /* Lock the attribute row and get the data */
    let attrrel = table_open(AttributeRelationId, RowExclusiveLock);
    let atttup = SearchSysCacheAttName(relid, attname);
    if !HeapTupleIsValid(atttup) {
        elog!(
            ERROR,
            "cache lookup failed for attribute {} of relation {}",
            CStr_to_str(attname),
            relid
        );
    }
    let att_struct: Form_pg_attribute = GETSTRUCT(atttup) as Form_pg_attribute;

    /* get an array value from the value string */
    let missingval = OidFunctionCall3(
        F_ARRAY_IN,
        CStringGetDatum(value),
        ObjectIdGetDatum((*att_struct).atttypid),
        Int32GetDatum((*att_struct).atttypmod),
    );

    /* update the tuple - set atthasmissing and attmissingval */
    values_att[Anum_pg_attribute_atthasmissing as usize - 1] = BoolGetDatum(true);
    replaces_att[Anum_pg_attribute_atthasmissing as usize - 1] = true;
    values_att[Anum_pg_attribute_attmissingval as usize - 1] = missingval;
    replaces_att[Anum_pg_attribute_attmissingval as usize - 1] = true;

    let newtup = heap_modify_tuple(
        atttup,
        RelationGetDescr(attrrel),
        values_att.as_mut_ptr(),
        nulls_att.as_mut_ptr(),
        replaces_att.as_mut_ptr(),
    );
    CatalogTupleUpdate(attrrel, &mut (*newtup).t_self, newtup);

    /* clean up */
    ReleaseSysCache(atttup);
    table_close(attrrel, RowExclusiveLock);
    table_close(tablerel, AccessExclusiveLock);
}

/*
 * Store a check-constraint expression for the given relation.
 *
 * Caller is responsible for updating the count of constraints
 * in the pg_class entry for the relation.
 *
 * The OID of the new constraint is returned.
 */
unsafe fn StoreRelCheck(
    rel: Relation,
    ccname: *const c_char,
    expr: *mut Node,
    is_enforced: bool,
    is_validated: bool,
    is_local: bool,
    inhcount: int16,
    is_no_inherit: bool,
    is_internal: bool,
) -> Oid {
    /*
     * Flatten expression to string form for storage.
     */
    let ccbin = nodeToString(expr as *mut c_void);

    /*
     * Find columns of rel that are used in expr
     *
     * NB: pull_var_clause is okay here only because we don't allow subselects
     * in check constraints; it would fail to examine the contents of
     * subselects.
     */
    let var_list = pull_var_clause(expr, 0);
    let keycount = list_length(var_list);

    let att_nos: *mut int16;
    if keycount > 0 {
        att_nos = palloc((keycount as usize * core::mem::size_of::<int16>()) as Size) as *mut int16;
        let mut i: c_int = 0;
        let mut lc = list_head(var_list);
        while !lc.is_null() {
            let var: *mut Var = lfirst(lc) as *mut Var;
            let mut j: c_int = 0;
            while j < i {
                if *att_nos.add(j as usize) == (*var).varattno {
                    break;
                }
                j += 1;
            }
            if j == i {
                *att_nos.add(i as usize) = (*var).varattno;
                i += 1;
            }
            lc = lnext(var_list, lc);
        }
        let keycount = i;

        /*
         * Partitioned tables do not contain any rows themselves, so a NO INHERIT
         * constraint makes no sense.
         */
        if is_no_inherit && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            ereport!(ERROR, errmsg!(
                    "cannot add NO INHERIT constraint to partitioned table \"{}\"",
                    CStr_to_str(RelationGetRelationName(rel))
                )) /* C also: errcode */;
        }

        /*
         * Create the Check Constraint
         */
        let constr_oid = CreateConstraintEntry(
            ccname,
            RelationGetNamespace(rel),
            CONSTRAINT_CHECK,
            false,       /* Is Deferrable */
            false,       /* Is Deferred */
            is_enforced,
            is_validated,
            InvalidOid,  /* no parent constraint */
            RelationGetRelid(rel),
            att_nos,
            keycount,
            keycount,
            InvalidOid,  /* not a domain constraint */
            InvalidOid,  /* no associated index */
            InvalidOid,  /* Foreign key fields */
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            0,
            b' ' as c_char,
            b' ' as c_char,
            core::ptr::null_mut(),
            0,
            b' ' as c_char,
            core::ptr::null_mut(), /* not an exclusion constraint */
            expr,        /* Tree form of check constraint */
            ccbin,       /* Binary form of check constraint */
            is_local,    /* conislocal */
            inhcount,    /* coninhcount */
            is_no_inherit, /* connoinherit */
            false,       /* conperiod */
            is_internal, /* internally constructed? */
        );

        pfree(ccbin as *mut c_void);

        return constr_oid;
    }

    /*
     * Partitioned tables do not contain any rows themselves, so a NO INHERIT
     * constraint makes no sense.
     */
    if is_no_inherit && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        ereport!(ERROR, errmsg!(
                "cannot add NO INHERIT constraint to partitioned table \"{}\"",
                CStr_to_str(RelationGetRelationName(rel))
            )) /* C also: errcode */;
    }

    att_nos = core::ptr::null_mut();

    /*
     * Create the Check Constraint
     */
    let constr_oid = CreateConstraintEntry(
        ccname,
        RelationGetNamespace(rel),
        CONSTRAINT_CHECK,
        false,
        false,
        is_enforced,
        is_validated,
        InvalidOid,
        RelationGetRelid(rel),
        att_nos,
        0,
        0,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        0,
        b' ' as c_char,
        b' ' as c_char,
        core::ptr::null_mut(),
        0,
        b' ' as c_char,
        core::ptr::null_mut(),
        expr,
        ccbin,
        is_local,
        inhcount,
        is_no_inherit,
        false,
        is_internal,
    );

    pfree(ccbin as *mut c_void);

    constr_oid
}

/*
 * Store a not-null constraint for the given relation
 *
 * The OID of the new constraint is returned.
 */
unsafe fn StoreRelNotNull(
    rel: Relation,
    nnname: *const c_char,
    attnum: AttrNumber,
    is_validated: bool,
    is_local: bool,
    inhcount: i32,
    is_no_inherit: bool,
) -> Oid {
    Assert!(attnum > InvalidAttrNumber);

    CreateConstraintEntry(
        nnname,
        RelationGetNamespace(rel),
        CONSTRAINT_NOTNULL,
        false,
        false,
        true,  /* Is Enforced */
        is_validated,
        InvalidOid,
        RelationGetRelid(rel),
        &attnum,
        1,
        1,
        InvalidOid, /* not a domain constraint */
        InvalidOid, /* no associated index */
        InvalidOid, /* Foreign key fields */
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        0,
        b' ' as c_char,
        b' ' as c_char,
        core::ptr::null_mut(),
        0,
        b' ' as c_char,
        core::ptr::null_mut(), /* not an exclusion constraint */
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        is_local,
        inhcount as int16,
        is_no_inherit,
        false,
        false,
    )
}

/*
 * Store defaults and CHECK constraints (passed as a list of CookedConstraint).
 *
 * Each CookedConstraint struct is modified to store the new catalog tuple OID.
 *
 * NOTE: only pre-cooked expressions will be passed this way, which is to
 * say expressions inherited from an existing relation.  Newly parsed
 * expressions can be added later, by direct calls to StoreAttrDefault
 * and StoreRelCheck (see AddRelationNewConstraints()).
 */
unsafe fn StoreConstraints(rel: Relation, cooked_constraints: *mut List, is_internal: bool) {
    let mut numchecks: c_int = 0;

    if cooked_constraints.is_null() {
        return; /* nothing to do */
    }

    /*
     * Deparsing of constraint expressions will fail unless the just-created
     * pg_attribute tuples for this relation are made visible.  So, bump the
     * command counter.  CAUTION: this will cause a relcache entry rebuild.
     */
    CommandCounterIncrement();

    let mut lc = list_head(cooked_constraints);
    while !lc.is_null() {
        let con: *mut CookedConstraint = lfirst(lc) as *mut CookedConstraint;

        match (*con).contype {
            CONSTR_DEFAULT => {
                (*con).conoid = StoreAttrDefault(rel, (*con).attnum, (*con).expr, is_internal);
            }
            CONSTR_CHECK => {
                (*con).conoid = StoreRelCheck(
                    rel,
                    (*con).name,
                    (*con).expr,
                    (*con).is_enforced,
                    !(*con).skip_validation,
                    (*con).is_local,
                    (*con).inhcount as int16,
                    (*con).is_no_inherit,
                    is_internal,
                );
                numchecks += 1;
            }
            _ => {
                elog!(ERROR, "unrecognized constraint type: {}", (*con).contype as c_int);
            }
        }

        lc = lnext(cooked_constraints, lc);
    }

    if numchecks > 0 {
        SetRelationNumChecks(rel, numchecks);
    }
}

/*
 * AddRelationNewConstraints
 *
 * Add new column default expressions and/or constraint check expressions
 * to an existing relation.  This is defined to do both for efficiency in
 * DefineRelation, but of course you can do just one or the other by passing
 * empty lists.
 *
 * rel: relation to be modified
 * newColDefaults: list of RawColumnDefault structures
 * newConstraints: list of Constraint nodes
 * allow_merge: true if check constraints may be merged with existing ones
 * is_local: true if definition is local, false if it's inherited
 * is_internal: true if result of some internal process, not a user request
 * queryString: used during expression transformation of default values and
 *        cooked CHECK constraints
 *
 * All entries in newColDefaults will be processed.  Entries in newConstraints
 * will be processed only if they are CONSTR_CHECK or CONSTR_NOTNULL types.
 *
 * Returns a list of CookedConstraint nodes that shows the cooked form of
 * the default and constraint expressions added to the relation.
 *
 * NB: caller should have opened rel with some self-conflicting lock mode,
 * and should hold that lock till end of transaction; for normal cases that'll
 * be AccessExclusiveLock, but if caller knows that the constraint is already
 * enforced by some other means, it can be ShareUpdateExclusiveLock.  Also, we
 * assume the caller has done a CommandCounterIncrement if necessary to make
 * the relation's catalog tuples visible.
 */
pub unsafe fn AddRelationNewConstraints(
    rel: Relation,
    new_col_defaults: *mut List,
    new_constraints: *mut List,
    allow_merge: bool,
    is_local: bool,
    is_internal: bool,
    query_string: *const c_char,
) -> *mut List {
    let mut cooked_constraints: *mut List = NIL;
    let tuple_desc = RelationGetDescr(rel);
    let old_constr = (*tuple_desc).constr;
    let num_old_checks: c_int = if !old_constr.is_null() {
        (*old_constr).num_check as c_int
    } else {
        0
    };

    /*
     * Create a dummy ParseState and insert the target relation as its sole
     * rangetable entry.  We need a ParseState for transformExpr.
     */
    let pstate = make_parsestate(core::ptr::null_mut());
    (*pstate).p_sourcetext = query_string;
    let nsitem = addRangeTableEntryForRelation(
        pstate,
        rel,
        AccessShareLock,
        core::ptr::null_mut(),
        false,
        true,
    );
    addNSItemToQuery(pstate, nsitem, true, true, true);

    /*
     * Process column default expressions.
     */
    let mut lc = list_head(new_col_defaults);
    while !lc.is_null() {
        let col_def: *mut RawColumnDefault = lfirst(lc) as *mut RawColumnDefault;
        let atp: Form_pg_attribute =
            TupleDescAttr((*rel).rd_att, (*col_def).attnum as i32 - 1);

        let expr = cookDefault(
            pstate,
            (*col_def).raw_default,
            (*atp).atttypid,
            (*atp).atttypmod,
            NameStr(&(*atp).attname),
            (*atp).attgenerated,
        );

        /*
         * If the expression is just a NULL constant, we do not bother to make
         * an explicit pg_attrdef entry, since the default behavior is
         * equivalent.  This applies to column defaults, but not for
         * generation expressions.
         *
         * Note a nonobvious property of this test: if the column is of a
         * domain type, what we'll get is not a bare null Const but a
         * CoerceToDomain expr, so we will not discard the default.  This is
         * critical because the column default needs to be retained to
         * override any default that the domain might have.
         */
        if expr.is_null()
            || (!(*col_def).generated != 0
                && IsA(expr, T_Const)
                && (*(expr as *mut Const)).constisnull)
        {
            lc = lnext(new_col_defaults, lc);
            continue;
        }

        let def_oid = StoreAttrDefault(rel, (*col_def).attnum, expr, is_internal);

        let cooked: *mut CookedConstraint =
            palloc(core::mem::size_of::<CookedConstraint>() as Size) as *mut CookedConstraint;
        (*cooked).contype = CONSTR_DEFAULT;
        (*cooked).conoid = def_oid;
        (*cooked).name = core::ptr::null_mut();
        (*cooked).attnum = (*col_def).attnum;
        (*cooked).expr = expr;
        (*cooked).is_enforced = true;
        (*cooked).skip_validation = false;
        (*cooked).is_local = is_local;
        (*cooked).inhcount = if is_local { 0 } else { 1 };
        (*cooked).is_no_inherit = false;
        cooked_constraints = lappend(cooked_constraints, cooked as *mut c_void);

        lc = lnext(new_col_defaults, lc);
    }

    /*
     * Process constraint expressions.
     */
    let mut numchecks = num_old_checks;
    let mut checknames: *mut List = NIL;
    let mut nnnames: *mut List = NIL;

    let mut lc = list_head(new_constraints);
    while !lc.is_null() {
        let cdef: *mut Constraint = lfirst(lc) as *mut Constraint;

        if (*cdef).contype == CONSTR_CHECK {
            let ccname: *mut c_char;
            let expr: *mut Node;

            if !(*cdef).raw_expr.is_null() {
                Assert!((*cdef).cooked_expr.is_null());

                /*
                 * Transform raw parsetree to executable expression, and
                 * verify it's valid as a CHECK constraint.
                 */
                expr = cookConstraint(pstate, (*cdef).raw_expr, RelationGetRelationName(rel) as *mut c_char);
            } else {
                Assert!(!(*cdef).cooked_expr.is_null());

                /*
                 * Here, we assume the parser will only pass us valid CHECK
                 * expressions, so we do no particular checking.
                 */
                expr = stringToNode((*cdef).cooked_expr) as *mut Node;
            }

            /*
             * Check name uniqueness, or generate a name if none was given.
             */
            if !(*cdef).conname.is_null() {
                ccname = (*cdef).conname;
                /* Check against other new constraints */
                /* Needed because we don't do CommandCounterIncrement in loop */
                let mut clc2 = list_head(checknames);
                while !clc2.is_null() {
                    let chkname: *mut c_char = lfirst(clc2) as *mut c_char;
                    if libc_strcmp(chkname, ccname) == 0 {
                        ereport!(ERROR, errmsg!(
                                "check constraint \"{}\" already exists",
                                CStr_to_str(ccname)
                            )) /* C also: errcode */;
                    }
                    clc2 = lnext(checknames, clc2);
                }

                /* save name for future checks */
                checknames = lappend(checknames, ccname as *mut c_void);

                /*
                 * Check against pre-existing constraints.  If we are allowed
                 * to merge with an existing constraint, there's no more to do
                 * here. (We omit the duplicate constraint from the result,
                 * which is what ATAddCheckNNConstraint wants.)
                 */
                if MergeWithExistingConstraint(
                    rel,
                    ccname,
                    expr,
                    allow_merge,
                    is_local,
                    (*cdef).is_enforced,
                    (*cdef).initially_valid,
                    (*cdef).is_no_inherit,
                ) {
                    lc = lnext(new_constraints, lc);
                    continue;
                }
            } else {
                /*
                 * When generating a name, we want to create "tab_col_check"
                 * for a column constraint and "tab_check" for a table
                 * constraint.  We no longer have any info about the syntactic
                 * positioning of the constraint phrase, so we approximate
                 * this by seeing whether the expression references more than
                 * one column.  (If the user played by the rules, the result
                 * is the same...)
                 *
                 * Note: pull_var_clause() doesn't descend into sublinks, but
                 * we eliminated those above; and anyway this only needs to be
                 * an approximate answer.
                 */
                let mut vars = pull_var_clause(expr, 0);

                /* eliminate duplicates */
                vars = list_union(NIL, vars);

                let colname: *mut c_char = if list_length(vars) == 1 {
                    get_attname(
                        RelationGetRelid(rel),
                        (*(linitial(vars) as *mut Var)).varattno,
                        true,
                    )
                } else {
                    core::ptr::null_mut()
                };

                ccname = ChooseConstraintName(
                    RelationGetRelationName(rel),
                    colname,
                    b"check\0".as_ptr() as *const c_char,
                    RelationGetNamespace(rel),
                    checknames,
                );

                /* save name for future checks */
                checknames = lappend(checknames, ccname as *mut c_void);
            }

            /*
             * OK, store it.
             */
            let constr_oid = StoreRelCheck(
                rel,
                ccname,
                expr,
                (*cdef).is_enforced,
                (*cdef).initially_valid,
                is_local,
                if is_local { 0 } else { 1 },
                (*cdef).is_no_inherit,
                is_internal,
            );

            numchecks += 1;

            let cooked: *mut CookedConstraint =
                palloc(core::mem::size_of::<CookedConstraint>() as Size) as *mut CookedConstraint;
            (*cooked).contype = CONSTR_CHECK;
            (*cooked).conoid = constr_oid;
            (*cooked).name = ccname;
            (*cooked).attnum = 0;
            (*cooked).expr = expr;
            (*cooked).is_enforced = (*cdef).is_enforced;
            (*cooked).skip_validation = (*cdef).skip_validation;
            (*cooked).is_local = is_local;
            (*cooked).inhcount = if is_local { 0 } else { 1 };
            (*cooked).is_no_inherit = (*cdef).is_no_inherit;
            cooked_constraints = lappend(cooked_constraints, cooked as *mut c_void);
        } else if (*cdef).contype == CONSTR_NOTNULL {
            let inhcount: int16 = if is_local { 0 } else { 1 };

            /* Determine which column to modify */
            let colnum = get_attnum(
                RelationGetRelid(rel),
                strVal(linitial((*cdef).keys)),
            );
            if colnum == InvalidAttrNumber {
                ereport!(ERROR, errmsg!(
                        "column \"{}\" of relation \"{}\" does not exist",
                        CStr_to_str(strVal(linitial((*cdef).keys))),
                        CStr_to_str(RelationGetRelationName(rel))
                    )) /* C also: errcode */;
            }
            if colnum < InvalidAttrNumber {
                ereport!(ERROR, errmsg!(
                        "cannot add not-null constraint on system column \"{}\"",
                        CStr_to_str(strVal(linitial((*cdef).keys)))
                    )) /* C also: errcode */;
            }

            Assert!((*cdef).initially_valid != (*cdef).skip_validation);

            /*
             * If the column already has a not-null constraint, we don't want
             * to add another one; adjust inheritance status as needed.  This
             * also checks whether the existing constraint matches the
             * requested validity.
             */
            if AdjustNotNullInheritance(
                RelationGetRelid(rel),
                colnum,
                (*cdef).conname,
                is_local,
                (*cdef).is_no_inherit,
                (*cdef).skip_validation,
            ) {
                lc = lnext(new_constraints, lc);
                continue;
            }

            /*
             * If a constraint name is specified, check that it isn't already
             * used.  Otherwise, choose a non-conflicting one ourselves.
             */
            let nnname: *mut c_char;
            if !(*cdef).conname.is_null() {
                if ConstraintNameIsUsed(
                    CONSTRAINT_RELATION,
                    RelationGetRelid(rel),
                    (*cdef).conname,
                ) {
                    ereport!(ERROR, errmsg!(
                            "constraint \"{}\" for relation \"{}\" already exists",
                            CStr_to_str((*cdef).conname),
                            CStr_to_str(RelationGetRelationName(rel))
                        )) /* C also: errcode */;
                }
                nnname = (*cdef).conname;
            } else {
                nnname = ChooseConstraintName(
                    RelationGetRelationName(rel),
                    strVal(linitial((*cdef).keys)),
                    b"not_null\0".as_ptr() as *const c_char,
                    RelationGetNamespace(rel),
                    nnnames,
                );
            }
            nnnames = lappend(nnnames, nnname as *mut c_void);

            let constr_oid = StoreRelNotNull(
                rel,
                nnname,
                colnum,
                (*cdef).initially_valid,
                is_local,
                inhcount as i32,
                (*cdef).is_no_inherit,
            );

            let nn_cooked: *mut CookedConstraint =
                palloc(core::mem::size_of::<CookedConstraint>() as Size) as *mut CookedConstraint;
            (*nn_cooked).contype = CONSTR_NOTNULL;
            (*nn_cooked).conoid = constr_oid;
            (*nn_cooked).name = nnname;
            (*nn_cooked).attnum = colnum;
            (*nn_cooked).expr = core::ptr::null_mut();
            (*nn_cooked).is_enforced = true;
            (*nn_cooked).skip_validation = (*cdef).skip_validation;
            (*nn_cooked).is_local = is_local;
            (*nn_cooked).inhcount = inhcount as i32;
            (*nn_cooked).is_no_inherit = (*cdef).is_no_inherit;

            cooked_constraints = lappend(cooked_constraints, nn_cooked as *mut c_void);
        }

        lc = lnext(new_constraints, lc);
    }

    /*
     * Update the count of constraints in the relation's pg_class tuple. We do
     * this even if there was no change, in order to ensure that an SI update
     * message is sent out for the pg_class tuple, which will force other
     * backends to rebuild their relcache entries for the rel. (This is
     * critical if we added defaults but not constraints.)
     */
    SetRelationNumChecks(rel, numchecks);

    cooked_constraints
}

/*
 * Check for a pre-existing check constraint that conflicts with a proposed
 * new one, and either adjust its conislocal/coninhcount settings or throw
 * error as needed.
 *
 * Returns true if merged (constraint is a duplicate), or false if it's
 * got a so-far-unique name, or throws error if conflict.
 *
 * XXX See MergeConstraintsIntoExisting too if you change this code.
 */
unsafe fn MergeWithExistingConstraint(
    rel: Relation,
    ccname: *const c_char,
    expr: *mut Node,
    allow_merge: bool,
    is_local: bool,
    is_enforced: bool,
    is_initially_valid: bool,
    is_no_inherit: bool,
) -> bool {
    let mut found = false;
    let mut allow_merge = allow_merge;

    /* Search for a pg_constraint entry with same name and relation */
    let con_desc = table_open(ConstraintRelationId, RowExclusiveLock);

    let mut skey: [ScanKeyData; 3] = [core::mem::zeroed(); 3];
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_constraint_conrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_constraint_contypid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(InvalidOid),
    );
    ScanKeyInit(
        &mut skey[2],
        Anum_pg_constraint_conname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(ccname),
    );

    let conscan = systable_beginscan(
        con_desc,
        ConstraintRelidTypidNameIndexId,
        true,
        core::ptr::null_mut(),
        3,
        skey.as_mut_ptr(),
    );

    /* There can be at most one matching row */
    let tup = systable_getnext(conscan);
    if HeapTupleIsValid(tup) {
        let con: Form_pg_constraint = GETSTRUCT(tup) as Form_pg_constraint;

        /* Found it.  Conflicts if not identical check constraint */
        if (*con).contype == CONSTRAINT_CHECK {
            let mut isnull: bool = false;
            let val = fastgetattr(
                tup,
                Anum_pg_constraint_conbin as c_int,
                (*con_desc).rd_att,
                &mut isnull,
            );
            if isnull {
                elog!(ERROR, "null conbin for rel {}", CStr_to_str(RelationGetRelationName(rel)));
            }
            if equal(expr as *const c_void, stringToNode(TextDatumGetCString(val)) as *const c_void) {
                found = true;
            }
        }

        /*
         * If the existing constraint is purely inherited (no local
         * definition) then interpret addition of a local constraint as a
         * legal merge.  This allows ALTER ADD CONSTRAINT on parent and child
         * tables to be given in either order with same end state.  However if
         * the relation is a partition, all inherited constraints are always
         * non-local, including those that were merged.
         */
        if is_local && !(*con).conislocal && !(*(*rel).rd_rel).relispartition {
            allow_merge = true;
        }

        if !found || !allow_merge {
            ereport!(ERROR, errmsg!(
                    "constraint \"{}\" for relation \"{}\" already exists",
                    CStr_to_str(ccname),
                    CStr_to_str(RelationGetRelationName(rel))
                )) /* C also: errcode */;
        }

        /* If the child constraint is "no inherit" then cannot merge */
        if (*con).connoinherit {
            ereport!(ERROR, errmsg!(
                    "constraint \"{}\" conflicts with non-inherited constraint on relation \"{}\"",
                    CStr_to_str(ccname),
                    CStr_to_str(RelationGetRelationName(rel))
                )) /* C also: errcode */;
        }

        /*
         * Must not change an existing inherited constraint to "no inherit"
         * status.  That's because inherited constraints should be able to
         * propagate to lower-level children.
         */
        if (*con).coninhcount > 0 && is_no_inherit {
            ereport!(ERROR, errmsg!(
                    "constraint \"{}\" conflicts with inherited constraint on relation \"{}\"",
                    CStr_to_str(ccname),
                    CStr_to_str(RelationGetRelationName(rel))
                )) /* C also: errcode */;
        }

        /*
         * If the child constraint is "not valid" then cannot merge with a
         * valid parent constraint.
         */
        if is_initially_valid && (*con).conenforced && !(*con).convalidated {
            ereport!(ERROR, errmsg!(
                    "constraint \"{}\" conflicts with NOT VALID constraint on relation \"{}\"",
                    CStr_to_str(ccname),
                    CStr_to_str(RelationGetRelationName(rel))
                )) /* C also: errcode */;
        }

        /*
         * A non-enforced child constraint cannot be merged with an enforced
         * parent constraint. However, the reverse is allowed, where the child
         * constraint is enforced.
         */
        if (!is_local && is_enforced && !(*con).conenforced)
            || (is_local && !is_enforced && (*con).conenforced)
        {
            ereport!(ERROR, errmsg!(
                    "constraint \"{}\" conflicts with NOT ENFORCED constraint on relation \"{}\"",
                    CStr_to_str(ccname),
                    CStr_to_str(RelationGetRelationName(rel))
                )) /* C also: errcode */;
        }

        /* OK to update the tuple */
        ereport!(NOTICE, errmsg!("merging constraint \"{}\" with inherited definition", CStr_to_str(ccname)));

        let tup = heap_copytuple(tup);
        let con: Form_pg_constraint = GETSTRUCT(tup) as Form_pg_constraint;

        /*
         * In case of partitions, an inherited constraint must be inherited
         * only once since it cannot have multiple parents and it is never
         * considered local.
         */
        if (*(*rel).rd_rel).relispartition {
            (*con).coninhcount = 1;
            (*con).conislocal = false;
        } else {
            if is_local {
                (*con).conislocal = true;
            } else {
                let mut new_inhcount: int16 = 0;
                if pg_add_s16_overflow((*con).coninhcount, 1, &mut new_inhcount) {
                    ereport!(ERROR, errmsg!("too many inheritance parents")) /* C also: errcode */;
                }
                (*con).coninhcount = new_inhcount;
            }
        }

        if is_no_inherit {
            Assert!(is_local);
            (*con).connoinherit = true;
        }

        /*
         * If the child constraint is required to be enforced while the parent
         * constraint is not, this should be allowed by marking the child
         * constraint as enforced. In the reverse case, an error would have
         * already been thrown before reaching this point.
         */
        if is_enforced && !(*con).conenforced {
            Assert!(is_local);
            (*con).conenforced = true;
            (*con).convalidated = true;
        }

        CatalogTupleUpdate(con_desc, &mut (*tup).t_self, tup);
    }

    systable_endscan(conscan);
    table_close(con_desc, RowExclusiveLock);

    found
}

/*
 * Create the not-null constraints when creating a new relation
 *
 * These come from two sources: the 'constraints' list (of Constraint) is
 * specified directly by the user; the 'old_notnulls' list (of
 * CookedConstraint) comes from inheritance.  We create one constraint
 * for each column, giving priority to user-specified ones, and setting
 * inhcount according to how many parents cause each column to get a
 * not-null constraint.  If a user-specified name clashes with another
 * user-specified name, an error is raised.  'existing_constraints'
 * is a list of already defined constraint names, which should be avoided
 * when generating further ones.
 *
 * Returns a list of AttrNumber for columns that need to have the attnotnull
 * flag set.
 */
pub unsafe fn AddRelationNotNullConstraints(
    rel: Relation,
    mut constraints: *mut List,
    mut old_notnulls: *mut List,
    existing_constraints: *mut List,
) -> *mut List {
    let mut nncols: *mut List = NIL;

    /*
     * We track two lists of names: nnnames keeps all the constraint names,
     * givennames tracks user-generated names.  The distinction is important,
     * because we must raise error for user-generated name conflicts, but for
     * system-generated name conflicts we just generate another.
     */
    let mut nnnames: *mut List = list_copy(existing_constraints); /* don't scribble on input */
    let mut givennames: *mut List = NIL;

    /*
     * First, create all not-null constraints that are directly specified by
     * the user.  Note that inheritance might have given us another source for
     * each, so we must scan the old_notnulls list and increment inhcount for
     * each element with identical attnum.  We delete from there any element
     * that we process.
     *
     * We don't use foreach() here because we have two nested loops over the
     * constraint list, with possible element deletions in the inner one. If
     * we used foreach_delete_current() it could only fix up the state of one
     * of the loops, so it seems cleaner to use looping over list indexes for
     * both loops.  Note that any deletion will happen beyond where the outer
     * loop is, so its index never needs adjustment.
     */
    let mut outerpos: c_int = 0;
    while outerpos < list_length(constraints) {
        let constr: *mut Constraint =
            list_nth_node_Constraint(constraints, outerpos) as *mut Constraint;
        let mut inhcount: c_int = 0;

        Assert!((*constr).contype == CONSTR_NOTNULL);

        let attnum = get_attnum(
            RelationGetRelid(rel),
            strVal(linitial((*constr).keys)),
        );
        if attnum == InvalidAttrNumber {
            ereport!(ERROR, errmsg!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    CStr_to_str(strVal(linitial((*constr).keys))),
                    CStr_to_str(RelationGetRelationName(rel))
                )) /* C also: errcode */;
        }
        if attnum < InvalidAttrNumber {
            ereport!(ERROR, errmsg!(
                    "cannot add not-null constraint on system column \"{}\"",
                    CStr_to_str(strVal(linitial((*constr).keys)))
                )) /* C also: errcode */;
        }

        /*
         * A column can only have one not-null constraint, so discard any
         * additional ones that appear for columns we already saw; but check
         * that the NO INHERIT flags match.
         */
        let mut restpos = outerpos + 1;
        while restpos < list_length(constraints) {
            let other: *mut Constraint =
                list_nth_node_Constraint(constraints, restpos) as *mut Constraint;
            if libc_strcmp(
                strVal(linitial((*constr).keys)),
                strVal(linitial((*other).keys)),
            ) == 0
            {
                if (*other).is_no_inherit != (*constr).is_no_inherit {
                    ereport!(ERROR, errmsg!(
                            "conflicting NO INHERIT declaration for not-null constraint on column \"{}\"",
                            CStr_to_str(strVal(linitial((*constr).keys)))
                        )) /* C also: errcode */;
                }

                /*
                 * Preserve constraint name if one is specified, but raise an
                 * error if conflicting ones are specified.
                 */
                if !(*other).conname.is_null() {
                    if (*constr).conname.is_null() {
                        (*constr).conname = pstrdup((*other).conname);
                    } else if libc_strcmp((*constr).conname, (*other).conname) != 0 {
                        ereport!(ERROR, errmsg!(
                                "conflicting not-null constraint names \"{}\" and \"{}\"",
                                CStr_to_str((*constr).conname),
                                CStr_to_str((*other).conname)
                            )) /* C also: errcode */;
                    }
                }

                /* XXX do we need to verify any other fields? */
                constraints = list_delete_nth_cell(constraints, restpos);
            } else {
                restpos += 1;
            }
        }

        /*
         * Search in the list of inherited constraints for any entries on the
         * same column; determine an inheritance count from that.  Also, if at
         * least one parent has a constraint for this column, then we must not
         * accept a user specification for a NO INHERIT one.  Any constraint
         * from parents that we process here is deleted from the list: we no
         * longer need to process it in the loop below.
         */
        let mut nlc = list_head(old_notnulls);
        while !nlc.is_null() {
            let old: *mut CookedConstraint = lfirst(nlc) as *mut CookedConstraint;
            let next = lnext(old_notnulls, nlc);
            if (*old).attnum == attnum {
                /*
                 * If we get a constraint from the parent, having a local NO
                 * INHERIT one doesn't work.
                 */
                if (*constr).is_no_inherit {
                    ereport!(ERROR, errmsg!(
                            "cannot define not-null constraint with NO INHERIT on column \"{}\"",
                            CStr_to_str(strVal(linitial((*constr).keys)))
                        )) /* C also: errcode, errdetail */;
                }

                inhcount += 1;
                old_notnulls = list_delete_cell(old_notnulls, nlc);
            }
            nlc = next;
        }

        /*
         * Determine a constraint name, which may have been specified by the
         * user, or raise an error if a conflict exists with another
         * user-specified name.
         */
        let conname: *mut c_char;
        if !(*constr).conname.is_null() {
            let mut glc = list_head(givennames);
            while !glc.is_null() {
                let thisname: *mut c_char = lfirst(glc) as *mut c_char;
                if libc_strcmp(thisname, (*constr).conname) == 0 {
                    ereport!(ERROR, errmsg!(
                            "constraint \"{}\" for relation \"{}\" already exists",
                            CStr_to_str((*constr).conname),
                            CStr_to_str(RelationGetRelationName(rel))
                        )) /* C also: errcode */;
                }
                glc = lnext(givennames, glc);
            }

            conname = (*constr).conname;
            givennames = lappend(givennames, conname as *mut c_void);
        } else {
            conname = ChooseConstraintName(
                RelationGetRelationName(rel),
                get_attname(RelationGetRelid(rel), attnum, false),
                b"not_null\0".as_ptr() as *const c_char,
                RelationGetNamespace(rel),
                nnnames,
            );
        }
        nnnames = lappend(nnnames, conname as *mut c_void);

        StoreRelNotNull(
            rel,
            conname,
            attnum,
            true,
            true,
            inhcount,
            (*constr).is_no_inherit,
        );

        nncols = lappend_int(nncols, attnum as c_int);

        outerpos += 1;
    }

    /*
     * If any column remains in the old_notnulls list, we must create a not-
     * null constraint marked not-local for that column.  Because multiple
     * parents could specify a not-null constraint for the same column, we
     * must count how many there are and set an appropriate inhcount
     * accordingly, deleting elements we've already processed.
     *
     * We don't use foreach() here because we have two nested loops over the
     * constraint list, with possible element deletions in the inner one. If
     * we used foreach_delete_current() it could only fix up the state of one
     * of the loops, so it seems cleaner to use looping over list indexes for
     * both loops.  Note that any deletion will happen beyond where the outer
     * loop is, so its index never needs adjustment.
     */
    let mut outerpos: c_int = 0;
    while outerpos < list_length(old_notnulls) {
        let cooked: *mut CookedConstraint =
            list_nth(old_notnulls, outerpos) as *mut CookedConstraint;
        let mut conname: *mut c_char = core::ptr::null_mut();
        let mut inhcount: c_int = 1;

        Assert!((*cooked).contype == CONSTR_NOTNULL);
        Assert!(!(*cooked).name.is_null());

        /*
         * Preserve the first non-conflicting constraint name we come across.
         */
        if conname.is_null() {
            conname = (*cooked).name;
        }

        let mut restpos = outerpos + 1;
        while restpos < list_length(old_notnulls) {
            let other: *mut CookedConstraint =
                list_nth(old_notnulls, restpos) as *mut CookedConstraint;
            Assert!(!(*other).name.is_null());
            if (*other).attnum == (*cooked).attnum {
                if conname.is_null() {
                    conname = (*other).name;
                }

                inhcount += 1;
                old_notnulls = list_delete_nth_cell(old_notnulls, restpos);
            } else {
                restpos += 1;
            }
        }

        /* If we got a name, make sure it isn't one we've already used */
        if !conname.is_null() {
            let mut nlc = list_head(nnnames);
            while !nlc.is_null() {
                let thisname: *mut c_char = lfirst(nlc) as *mut c_char;
                if libc_strcmp(thisname, conname) == 0 {
                    conname = core::ptr::null_mut();
                    break;
                }
                nlc = lnext(nnnames, nlc);
            }
        }

        /* and choose a name, if needed */
        if conname.is_null() {
            conname = ChooseConstraintName(
                RelationGetRelationName(rel),
                get_attname(RelationGetRelid(rel), (*cooked).attnum, false),
                b"not_null\0".as_ptr() as *const c_char,
                RelationGetNamespace(rel),
                nnnames,
            );
        }
        nnnames = lappend(nnnames, conname as *mut c_void);

        /* ignore the origin constraint's is_local and inhcount */
        StoreRelNotNull(rel, conname, (*cooked).attnum, true, false, inhcount, false);

        nncols = lappend_int(nncols, (*cooked).attnum as c_int);

        outerpos += 1;
    }

    nncols
}

/*
 * Update the count of constraints in the relation's pg_class tuple.
 *
 * Caller had better hold exclusive lock on the relation.
 *
 * An important side effect is that a SI update message will be sent out for
 * the pg_class tuple, which will force other backends to rebuild their
 * relcache entries for the rel.  Also, this backend will rebuild its
 * own relcache entry at the next CommandCounterIncrement.
 */
unsafe fn SetRelationNumChecks(rel: Relation, numchecks: c_int) {
    let relrel = table_open(RelationRelationId, RowExclusiveLock);
    let reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
    if !HeapTupleIsValid(reltup) {
        elog!(
            ERROR,
            "cache lookup failed for relation {}",
            RelationGetRelid(rel)
        );
    }
    let rel_struct: Form_pg_class = GETSTRUCT(reltup) as Form_pg_class;

    if (*rel_struct).relchecks != numchecks as int16 {
        (*rel_struct).relchecks = numchecks as int16;
        CatalogTupleUpdate(relrel, &mut (*reltup).t_self, reltup);
    } else {
        /* Skip the disk update, but force relcache inval anyway */
        CacheInvalidateRelcache(rel);
    }

    heap_freetuple(reltup);
    table_close(relrel, RowExclusiveLock);
}

/*
 * Check for references to generated columns
 */
unsafe fn check_nested_generated_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    let pstate: *mut ParseState = context as *mut ParseState;

    if node.is_null() {
        return false;
    } else if IsA(node, T_Var) {
        let var: *mut Var = node as *mut Var;

        let relid = (*rt_fetch((*var).varno as u32, (*pstate).p_rtable)).relid;
        if !OidIsValid(relid) {
            return false; /* XXX shouldn't we raise an error? */
        }

        let attnum = (*var).varattno;

        if attnum > 0 && get_attgenerated(relid, attnum) != 0 {
            ereport!(ERROR, errmsg!(
                    "cannot use generated column \"{}\" in column generation expression",
                    CStr_to_str(get_attname(relid, attnum, false))
                )) /* C also: errcode, errdetail, parser_errposition */;
        }
        /* A whole-row Var is necessarily self-referential, so forbid it */
        if attnum == 0 {
            ereport!(ERROR, errmsg!("cannot use whole-row variable in column generation expression")) /* C also: errcode, errdetail, parser_errposition */;
        }
        /* System columns were already checked in the parser */

        return false;
    } else {
        return expression_tree_walker(
            node,
            Some(check_nested_generated_walker),
            context,
        );
    }
}

unsafe fn check_nested_generated(pstate: *mut ParseState, node: *mut Node) {
    check_nested_generated_walker(node, pstate as *mut c_void);
}

/*
 * Check security of virtual generated column expression.
 *
 * Just like selecting from a view is exploitable (CVE-2024-7348), selecting
 * from a table with virtual generated columns is exploitable.  Users who are
 * concerned about this can avoid selecting from views, but telling them to
 * avoid selecting from tables is less practical.
 *
 * To address this, this restricts generation expressions for virtual
 * generated columns are restricted to using built-in functions and types.  We
 * assume that built-in functions and types cannot be exploited for this
 * purpose.  Note the overall security also requires that all functions in use
 * a immutable.  (For example, there are some built-in non-immutable functions
 * that can run arbitrary SQL.)  The immutability is checked elsewhere, since
 * that is a property that needs to hold independent of security
 * considerations.
 *
 * In the future, this could be expanded by some new mechanism to declare
 * other functions and types as safe or trusted for this purpose, but that is
 * to be designed.
 */

/*
 * Callback for check_functions_in_node() that determines whether a function
 * is user-defined.
 */
// Note: check_functions_in_node expects a different callback type; stub here
unsafe fn contains_user_functions_checker(
    func_id: Oid,
    _context: *mut c_void,
) -> bool {
    func_id >= FirstUnpinnedObjectId
}

/*
 * Checks for all the things we don't want in the generation expressions of
 * virtual generated columns for security reasons.  Errors out if it finds
 * one.
 */
unsafe fn check_virtual_generated_security_walker(
    node: *mut Node,
    context: *mut c_void,
) -> bool {
    let pstate: *mut ParseState = context as *mut ParseState;

    if node.is_null() {
        return false;
    }

    if !IsA(node, T_List) {
        if check_functions_in_node(
            node,
            Some(contains_user_functions_checker),
            core::ptr::null_mut(),
        ) {
            ereport!(ERROR, errmsg!("generation expression uses user-defined function")) /* C also: errcode, errdetail, parser_errposition */;
        }

        /*
         * check_functions_in_node() doesn't check some node types (see
         * comment there).  We handle CoerceToDomain and MinMaxExpr by
         * checking for built-in types.  The other listed node types cannot
         * call user-definable SQL-visible functions.
         *
         * We furthermore need this type check to handle built-in, immutable
         * polymorphic functions such as array_eq().
         */
        if exprType(node) >= FirstUnpinnedObjectId {
            ereport!(ERROR, errmsg!("generation expression uses user-defined type")) /* C also: errcode, errdetail, parser_errposition */;
        }
    }

    expression_tree_walker(
        node,
        Some(check_virtual_generated_security_walker),
        context,
    )
}

unsafe fn check_virtual_generated_security(pstate: *mut ParseState, node: *mut Node) {
    check_virtual_generated_security_walker(node, pstate as *mut c_void);
}

/*
 * Take a raw default and convert it to a cooked format ready for
 * storage.
 *
 * Parse state should be set up to recognize any vars that might appear
 * in the expression.  (Even though we plan to reject vars, it's more
 * user-friendly to give the correct error message than "unknown var".)
 *
 * If atttypid is not InvalidOid, coerce the expression to the specified
 * type (and typmod atttypmod).   attname is only needed in this case:
 * it is used in the error message, if any.
 */
pub unsafe fn cookDefault(
    pstate: *mut ParseState,
    raw_default: *mut Node,
    atttypid: Oid,
    atttypmod: int32,
    attname: *const c_char,
    attgenerated: c_char,
) -> *mut Node {
    Assert!(!raw_default.is_null());

    /*
     * Transform raw parsetree to executable expression.
     */
    let mut expr = transformExpr(
        pstate,
        raw_default,
        if attgenerated != 0 {
            EXPR_KIND_GENERATED_COLUMN
        } else {
            EXPR_KIND_COLUMN_DEFAULT
        },
    );

    if attgenerated != 0 {
        /* Disallow refs to other generated columns */
        check_nested_generated(pstate, expr);

        /* Disallow mutable functions */
        if contain_mutable_functions_after_planning(expr as *mut Expr) {
            ereport!(ERROR, errmsg!("generation expression is not immutable")) /* C also: errcode */;
        }

        /* Check security of expressions for virtual generated column */
        if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
            check_virtual_generated_security(pstate, expr);
        }
    } else {
        /*
         * For a default expression, transformExpr() should have rejected
         * column references.
         */
        Assert!(!contain_var_clause(expr));
    }

    /*
     * Coerce the expression to the correct type and typmod, if given. This
     * should match the parser's processing of non-defaulted expressions ---
     * see transformAssignedExpr().
     */
    if OidIsValid(atttypid) {
        let type_id = exprType(expr);

        expr = coerce_to_target_type(
            pstate,
            expr,
            type_id,
            atttypid,
            atttypmod,
            COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        );
        if expr.is_null() {
            ereport!(ERROR, errmsg!(
                    "column \"{}\" is of type {} but default expression is of type {}",
                    CStr_to_str(attname),
                    CStr_to_str(format_type_be(atttypid)),
                    CStr_to_str(format_type_be(type_id))
                )) /* C also: errcode, errhint */;
        }
    }

    /*
     * Finally, take care of collations in the finished expression.
     */
    assign_expr_collations(pstate, expr);

    expr
}

/*
 * Take a raw CHECK constraint expression and convert it to a cooked format
 * ready for storage.
 *
 * Parse state must be set up to recognize any vars that might appear
 * in the expression.
 */
unsafe fn cookConstraint(
    pstate: *mut ParseState,
    raw_constraint: *mut Node,
    relname: *mut c_char,
) -> *mut Node {
    /*
     * Transform raw parsetree to executable expression.
     */
    let expr = transformExpr(pstate, raw_constraint, EXPR_KIND_CHECK_CONSTRAINT);

    /*
     * Make sure it yields a boolean result.
     */
    let expr = coerce_to_boolean(pstate, expr, b"CHECK\0".as_ptr() as *const c_char);

    /*
     * Take care of collations.
     */
    assign_expr_collations(pstate, expr);

    /*
     * Make sure no outside relations are referred to (this is probably dead
     * code now that add_missing_from is history).
     */
    if list_length((*pstate).p_rtable) != 1 {
        ereport!(ERROR, errmsg!(
                "only table \"{}\" can be referenced in check constraint",
                CStr_to_str(relname)
            )) /* C also: errcode */;
    }

    expr
}

/*
 * CopyStatistics --- copy entries in pg_statistic from one rel to another
 */
pub unsafe fn CopyStatistics(fromrelid: Oid, torelid: Oid) {
    let statrel = table_open(StatisticRelationId, RowExclusiveLock);

    /* Now search for stat records */
    let mut key: [ScanKeyData; 1] = [core::mem::zeroed(); 1];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_statistic_starelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(fromrelid),
    );

    let scan = systable_beginscan(
        statrel,
        StatisticRelidAttnumInhIndexId,
        true,
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );

    let mut indstate: CatalogIndexState = core::ptr::null_mut();

    loop {
        let mut tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }

        /* make a modifiable copy */
        tup = heap_copytuple(tup);
        let statform: *mut FormData_pg_statistic =
            GETSTRUCT(tup) as *mut FormData_pg_statistic;

        /* update the copy of the tuple and insert it */
        (*statform).starelid = torelid;

        /* fetch index information when we know we need it */
        if indstate.is_null() {
            indstate = CatalogOpenIndexes(statrel);
        }

        CatalogTupleInsertWithInfo(statrel, tup, indstate);

        heap_freetuple(tup);
    }

    systable_endscan(scan);

    if !indstate.is_null() {
        CatalogCloseIndexes(indstate);
    }
    table_close(statrel, RowExclusiveLock);
}

/*
 * RemoveStatistics --- remove entries in pg_statistic for a rel or column
 *
 * If attnum is zero, remove all entries for rel; else remove only the one(s)
 * for that column.
 */
pub unsafe fn RemoveStatistics(relid: Oid, attnum: AttrNumber) {
    let pgstatistic = table_open(StatisticRelationId, RowExclusiveLock);

    let mut key: [ScanKeyData; 2] = [core::mem::zeroed(); 2];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_statistic_starelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );

    let nkeys: c_int;
    if attnum == 0 {
        nkeys = 1;
    } else {
        ScanKeyInit(
            &mut key[1],
            Anum_pg_statistic_staattnum,
            BTEqualStrategyNumber,
            F_INT2EQ,
            Int16GetDatum(attnum),
        );
        nkeys = 2;
    }

    let scan = systable_beginscan(
        pgstatistic,
        StatisticRelidAttnumInhIndexId,
        true,
        core::ptr::null_mut(),
        nkeys,
        key.as_mut_ptr(),
    );

    /* we must loop even when attnum != 0, in case of inherited stats */
    loop {
        let tuple = systable_getnext(scan);
        if !HeapTupleIsValid(tuple) {
            break;
        }
        CatalogTupleDelete(pgstatistic, &mut (*tuple).t_self);
    }

    systable_endscan(scan);
    table_close(pgstatistic, RowExclusiveLock);
}


/*
 * RelationTruncateIndexes - truncate all indexes associated
 * with the heap relation to zero tuples.
 *
 * The routine will truncate and then reconstruct the indexes on
 * the specified relation.  Caller must hold exclusive lock on rel.
 */
unsafe fn RelationTruncateIndexes(heap_relation: Relation) {
    /* Ask the relcache to produce a list of the indexes of the rel */
    let index_list = RelationGetIndexList(heap_relation);
    let mut lc = list_head(index_list);
    while !lc.is_null() {
        let index_id: Oid = lfirst_oid(lc);

        /* Open the index relation; use exclusive lock, just to be sure */
        let current_index = index_open(index_id, AccessExclusiveLock);

        /*
         * Fetch info needed for index_build.  Since we know there are no
         * tuples that actually need indexing, we can use a dummy IndexInfo.
         * This is slightly cheaper to build, but the real point is to avoid
         * possibly running user-defined code in index expressions or
         * predicates.  We might be getting invoked during ON COMMIT
         * processing, and we don't want to run any such code then.
         */
        let index_info = BuildDummyIndexInfo(current_index);

        /*
         * Now truncate the actual file (and discard buffers).
         */
        RelationTruncate(current_index as *mut c_void, 0);

        /* Initialize the index and rebuild */
        /* Note: we do not need to re-establish pkey setting */
        index_build(heap_relation, current_index, index_info, true, false);

        /* We're done with this index */
        index_close(current_index, NoLock);

        lc = lnext(index_list, lc);
    }
}

/*
 *     heap_truncate
 *
 *     This routine deletes all data within all the specified relations.
 *
 * This is not transaction-safe!  There is another, transaction-safe
 * implementation in commands/tablecmds.c.  We now use this only for
 * ON COMMIT truncation of temporary tables, where it doesn't matter.
 */
pub unsafe fn heap_truncate(relids: *mut List) {
    let mut relations: *mut List = NIL;

    /* Open relations for processing, and grab exclusive access on each */
    let mut cell = list_head(relids);
    while !cell.is_null() {
        let rid: Oid = lfirst_oid(cell);
        let rel = table_open(rid, AccessExclusiveLock);
        relations = lappend(relations, rel as *mut c_void);
        cell = lnext(relids, cell);
    }

    /* Don't allow truncate on tables that are referenced by foreign keys */
    heap_truncate_check_FKs(relations, true);

    /* OK to do it */
    let mut cell = list_head(relations);
    while !cell.is_null() {
        let rel: Relation = lfirst(cell) as Relation;

        /* Truncate the relation */
        heap_truncate_one_rel(rel);

        /* Close the relation, but keep exclusive lock on it until commit */
        table_close(rel, NoLock);

        cell = lnext(relations, cell);
    }
}

/*
 *     heap_truncate_one_rel
 *
 *     This routine deletes all data within the specified relation.
 *
 * This is not transaction-safe, because the truncation is done immediately
 * and cannot be rolled back later.  Caller is responsible for having
 * checked permissions etc, and must have obtained AccessExclusiveLock.
 */
pub unsafe fn heap_truncate_one_rel(rel: Relation) {
    /*
     * Truncate the relation.  Partitioned tables have no storage, so there is
     * nothing to do for them here.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        return;
    }

    /* Truncate the underlying relation */
    table_relation_nontransactional_truncate(rel);

    /* If the relation has indexes, truncate the indexes too */
    RelationTruncateIndexes(rel);

    /* If there is a toast table, truncate that too */
    let toastrelid = (*(*rel).rd_rel).reltoastrelid;
    if OidIsValid(toastrelid) {
        let toastrel = table_open(toastrelid, AccessExclusiveLock);

        table_relation_nontransactional_truncate(toastrel);
        RelationTruncateIndexes(toastrel);
        /* keep the lock... */
        table_close(toastrel, NoLock);
    }
}

/*
 * heap_truncate_check_FKs
 *        Check for foreign keys referencing a list of relations that
 *        are to be truncated, and raise error if there are any
 *
 * We disallow such FKs (except self-referential ones) since the whole point
 * of TRUNCATE is to not scan the individual rows to be thrown away.
 *
 * This is split out so it can be shared by both implementations of truncate.
 * Caller should already hold a suitable lock on the relations.
 *
 * tempTables is only used to select an appropriate error message.
 */
pub unsafe fn heap_truncate_check_FKs(relations: *mut List, temp_tables: bool) {
    let mut oids: *mut List = NIL;

    /*
     * Build a list of OIDs of the interesting relations.
     *
     * If a relation has no triggers, then it can neither have FKs nor be
     * referenced by a FK from another table, so we can ignore it.  For
     * partitioned tables, FKs have no triggers, so we must include them
     * anyway.
     */
    let mut cell = list_head(relations);
    while !cell.is_null() {
        let rel: Relation = lfirst(cell) as Relation;

        if (*(*rel).rd_rel).relhastriggers
            || (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE
        {
            oids = lappend_oid(oids, RelationGetRelid(rel));
        }
        cell = lnext(relations, cell);
    }

    /*
     * Fast path: if no relation has triggers, none has FKs either.
     */
    if oids.is_null() {
        return;
    }

    /*
     * Otherwise, must scan pg_constraint.  We make one pass with all the
     * relations considered; if this finds nothing, then all is well.
     */
    let dependents = heap_truncate_find_FKs(oids);
    if dependents.is_null() {
        return;
    }

    /*
     * Otherwise we repeat the scan once per relation to identify a particular
     * pair of relations to complain about.  This is pretty slow, but
     * performance shouldn't matter much in a failure path.  The reason for
     * doing things this way is to ensure that the message produced is not
     * dependent on chance row locations within pg_constraint.
     */
    let mut cell = list_head(oids);
    while !cell.is_null() {
        let relid: Oid = lfirst_oid(cell);
        let dependents = heap_truncate_find_FKs(crate::list_make1_oid!(relid));

        let mut cell2 = list_head(dependents);
        while !cell2.is_null() {
            let relid2: Oid = lfirst_oid(cell2);

            if !list_member_oid(oids, relid2) {
                let relname = get_rel_name(relid);
                let relname2 = get_rel_name(relid2);

                if temp_tables {
                    ereport!(ERROR, errmsg!("unsupported ON COMMIT and foreign key combination")) /* C also: errcode, errdetail */;
                } else {
                    ereport!(ERROR, errmsg!("cannot truncate a table referenced in a foreign key constraint")) /* C also: errcode, errdetail, errhint */;
                }
            }

            cell2 = lnext(dependents, cell2);
        }

        cell = lnext(oids, cell);
    }
}

/*
 * heap_truncate_find_FKs
 *        Find relations having foreign keys referencing any of the given rels
 *
 * Input and result are both lists of relation OIDs.  The result contains
 * no duplicates, does *not* include any rels that were already in the input
 * list, and is sorted in OID order.  (The last property is enforced mainly
 * to guarantee consistent behavior in the regression tests; we don't want
 * behavior to change depending on chance row locations within pg_constraint.)
 *
 * Note: caller should already have appropriate lock on all rels mentioned
 * in relationIds.  Since adding or dropping an FK requires exclusive lock
 * on both rels, this ensures that the answer will be stable.
 */
pub unsafe fn heap_truncate_find_FKs(relation_ids: *mut List) -> *mut List {
    let mut result: *mut List = NIL;
    let mut oids = list_copy(relation_ids);

    /*
     * Must scan pg_constraint.  Right now, it is a seqscan because there is
     * no available index on confrelid.
     */
    let fkey_rel = table_open(ConstraintRelationId, AccessShareLock);

    'restart: loop {
        let mut restart = false;
        let mut parent_cons: *mut List = NIL;

        let fkey_scan = systable_beginscan(
            fkey_rel,
            InvalidOid,
            false,
            core::ptr::null_mut(),
            0,
            core::ptr::null_mut(),
        );

        loop {
            let tuple = systable_getnext(fkey_scan);
            if !HeapTupleIsValid(tuple) {
                break;
            }
            let con: Form_pg_constraint = GETSTRUCT(tuple) as Form_pg_constraint;

            /* Not a foreign key */
            if (*con).contype != CONSTRAINT_FOREIGN {
                continue;
            }

            /* Not referencing one of our list of tables */
            if !list_member_oid(oids, (*con).confrelid) {
                continue;
            }

            /*
             * If this constraint has a parent constraint which we have not seen
             * yet, keep track of it for the second loop, below.  Tracking parent
             * constraints allows us to climb up to the top-level constraint and
             * look for all possible relations referencing the partitioned table.
             */
            if OidIsValid((*con).conparentid)
                && !list_member_oid(parent_cons, (*con).conparentid)
            {
                parent_cons = lappend_oid(parent_cons, (*con).conparentid);
            }

            /*
             * Add referencer to result, unless present in input list.  (Don't
             * worry about dupes: we'll fix that below).
             */
            if !list_member_oid(relation_ids, (*con).conrelid) {
                result = lappend_oid(result, (*con).conrelid);
            }
        }

        systable_endscan(fkey_scan);

        /*
         * Process each parent constraint we found to add the list of referenced
         * relations by them to the oids list.  If we do add any new such
         * relations, redo the first loop above.  Also, if we see that the parent
         * constraint in turn has a parent, add that so that we process all
         * relations in a single additional pass.
         */
        let mut plc = list_head(parent_cons);
        while !plc.is_null() {
            let parent: Oid = lfirst_oid(plc);
            let mut key: ScanKeyData = core::mem::zeroed();

            ScanKeyInit(
                &mut key,
                Anum_pg_constraint_oid,
                BTEqualStrategyNumber,
                F_OIDEQ,
                ObjectIdGetDatum(parent),
            );

            let fkey_scan = systable_beginscan(
                fkey_rel,
                ConstraintOidIndexId,
                true,
                core::ptr::null_mut(),
                1,
                &mut key,
            );

            let tuple = systable_getnext(fkey_scan);
            if HeapTupleIsValid(tuple) {
                let con: Form_pg_constraint = GETSTRUCT(tuple) as Form_pg_constraint;

                /*
                 * pg_constraint rows always appear for partitioned hierarchies
                 * this way: on the each side of the constraint, one row appears
                 * for each partition that points to the top-most table on the
                 * other side.
                 *
                 * Because of this arrangement, we can correctly catch all
                 * relevant relations by adding to 'parent_cons' all rows with
                 * valid conparentid, and to the 'oids' list all rows with a zero
                 * conparentid.  If any oids are added to 'oids', redo the first
                 * loop above by setting 'restart'.
                 */
                if OidIsValid((*con).conparentid) {
                    parent_cons =
                        list_append_unique_oid(parent_cons, (*con).conparentid);
                } else if !list_member_oid(oids, (*con).confrelid) {
                    oids = lappend_oid(oids, (*con).confrelid);
                    restart = true;
                }
            }

            systable_endscan(fkey_scan);

            plc = lnext(parent_cons, plc);
        }

        list_free(parent_cons);
        if restart {
            continue 'restart;
        }
        break;
    }

    table_close(fkey_rel, AccessShareLock);
    list_free(oids);

    /* Now sort and de-duplicate the result list */
    list_sort(result, list_oid_cmp);
    list_deduplicate_oid(result);

    result
}

/*
 * StorePartitionKey
 *        Store information about the partition key rel into the catalog
 */
pub unsafe fn StorePartitionKey(
    rel: Relation,
    strategy: c_char,
    partnatts: int16,
    partattrs: *mut AttrNumber,
    partexprs: *mut List,
    partopclass: *mut Oid,
    partcollation: *mut Oid,
) {
    let partattrs_vec = buildint2vector(partattrs, partnatts as c_int);
    let partopclass_vec = buildoidvector(partopclass, partnatts as c_int);
    let partcollation_vec = buildoidvector(partcollation, partnatts as c_int);

    /* Convert the expressions (if any) to a text datum */
    let partexpr_datum: Datum;
    if !partexprs.is_null() {
        let expr_string = nodeToString(partexprs as *mut c_void);
        partexpr_datum = CStringGetTextDatum(expr_string);
        pfree(expr_string as *mut c_void);
    } else {
        partexpr_datum = 0;
    }

    let pg_partitioned_table = table_open(PartitionedRelationId, RowExclusiveLock);

    let mut values: [Datum; Natts_pg_partitioned_table] = [0; Natts_pg_partitioned_table];
    let mut nulls: [bool; Natts_pg_partitioned_table] = [false; Natts_pg_partitioned_table];

    /* Only this can ever be NULL */
    if partexpr_datum == 0 {
        nulls[Anum_pg_partitioned_table_partexprs as usize - 1] = true;
    }

    values[Anum_pg_partitioned_table_partrelid as usize - 1] =
        ObjectIdGetDatum(RelationGetRelid(rel));
    values[Anum_pg_partitioned_table_partstrat as usize - 1] = CharGetDatum(strategy);
    values[Anum_pg_partitioned_table_partnatts as usize - 1] = Int16GetDatum(partnatts);
    values[Anum_pg_partitioned_table_partdefid as usize - 1] =
        ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_partitioned_table_partattrs as usize - 1] =
        PointerGetDatum(partattrs_vec as *const c_void);
    values[Anum_pg_partitioned_table_partclass as usize - 1] =
        PointerGetDatum(partopclass_vec as *const c_void);
    values[Anum_pg_partitioned_table_partcollation as usize - 1] =
        PointerGetDatum(partcollation_vec as *const c_void);
    values[Anum_pg_partitioned_table_partexprs as usize - 1] = partexpr_datum;

    let tuple = heap_form_tuple(
        RelationGetDescr(pg_partitioned_table),
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
    );

    CatalogTupleInsert(pg_partitioned_table, tuple);
    table_close(pg_partitioned_table, RowExclusiveLock);

    /* Mark this relation as dependent on a few things as follows */
    let addrs = new_object_addresses();
    let mut myself: ObjectAddress = INVALID_OBJECT_ADDRESS;
    let mut referenced: ObjectAddress = INVALID_OBJECT_ADDRESS;
    ObjectAddressSet(&mut myself, RelationRelationId, RelationGetRelid(rel));

    /* Operator class and collation per key column */
    for i in 0..partnatts as usize {
        ObjectAddressSet(&mut referenced, OperatorClassRelationId, *partopclass.add(i));
        add_exact_object_address(&referenced, addrs);

        /* The default collation is pinned, so don't bother recording it */
        if OidIsValid(*partcollation.add(i)) && *partcollation.add(i) != DEFAULT_COLLATION_OID {
            ObjectAddressSet(&mut referenced, CollationRelationId, *partcollation.add(i));
            add_exact_object_address(&referenced, addrs);
        }
    }

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL); /* TODO(pg-port): dependency */
    free_object_addresses(addrs);

    /*
     * The partitioning columns are made internally dependent on the table,
     * because we cannot drop any of them without dropping the whole table.
     * (ATExecDropColumn independently enforces that, but it's not bulletproof
     * so we need the dependencies too.)
     */
    for i in 0..partnatts as usize {
        if *partattrs.add(i) == 0 {
            continue; /* ignore expressions here */
        }

        ObjectAddressSubSet(
            &mut referenced,
            RelationRelationId,
            RelationGetRelid(rel),
            *partattrs.add(i) as i32,
        );
        recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL); /* TODO(pg-port): dependency */
    }

    /*
     * Also consider anything mentioned in partition expressions.  External
     * references (e.g. functions) get NORMAL dependencies.  Table columns
     * mentioned in the expressions are handled the same as plain partitioning
     * columns, i.e. they become internally dependent on the whole table.
     */
    if !partexprs.is_null() {
        recordDependencyOnSingleRelExpr(
            &myself,
            partexprs as *mut Node,
            RelationGetRelid(rel),
            DEPENDENCY_NORMAL,
            DEPENDENCY_INTERNAL,
            true, /* reverse the self-deps */
        ); /* TODO(pg-port): dependency */
    }

    /*
     * We must invalidate the relcache so that the next
     * CommandCounterIncrement() will cause the same to be rebuilt using the
     * information in just created catalog entry.
     */
    CacheInvalidateRelcache(rel);
}

/*
 *    RemovePartitionKeyByRelId
 *        Remove pg_partitioned_table entry for a relation
 */
pub unsafe fn RemovePartitionKeyByRelId(relid: Oid) {
    let rel = table_open(PartitionedRelationId, RowExclusiveLock);

    let tuple = SearchSysCache1(PARTRELID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tuple) {
        elog!(
            ERROR,
            "cache lookup failed for partition key of relation {}",
            relid
        );
    }

    CatalogTupleDelete(rel, &mut (*tuple).t_self);

    ReleaseSysCache(tuple);
    table_close(rel, RowExclusiveLock);
}

/*
 * StorePartitionBound
 *        Update pg_class tuple of rel to store the partition bound and set
 *        relispartition to true
 *
 * If this is the default partition, also update the default partition OID in
 * pg_partitioned_table.
 *
 * Also, invalidate the parent's relcache, so that the next rebuild will load
 * the new partition's info into its partition descriptor.  If there is a
 * default partition, we must invalidate its relcache entry as well.
 */
pub unsafe fn StorePartitionBound(rel: Relation, parent: Relation, bound: *mut PartitionBoundSpec) {
    /* Update pg_class tuple */
    let class_rel = table_open(RelationRelationId, RowExclusiveLock);
    let tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", RelationGetRelid(rel));
    }

    #[cfg(debug_assertions)]  // was USE_ASSERT_CHECKING
    {
        let class_form: Form_pg_class = GETSTRUCT(tuple) as Form_pg_class;
        Assert!(!(*class_form).relispartition);
        let mut isnull: bool = false;
        SysCacheGetAttr(
            RELOID,
            tuple,
            Anum_pg_class_relpartbound,
            &mut isnull,
        );
        Assert!(isnull);
    }

    /* Fill in relpartbound value */
    let mut new_val: [Datum; Natts_pg_class] = [0; Natts_pg_class];
    let mut new_null: [bool; Natts_pg_class] = [false; Natts_pg_class];
    let mut new_repl: [bool; Natts_pg_class] = [false; Natts_pg_class];
    new_val[Anum_pg_class_relpartbound as usize - 1] =
        CStringGetTextDatum(nodeToString(bound as *mut c_void));
    new_null[Anum_pg_class_relpartbound as usize - 1] = false;
    new_repl[Anum_pg_class_relpartbound as usize - 1] = true;
    let newtuple = heap_modify_tuple(
        tuple,
        RelationGetDescr(class_rel),
        new_val.as_mut_ptr(),
        new_null.as_mut_ptr(),
        new_repl.as_mut_ptr(),
    );
    /* Also set the flag */
    (*(GETSTRUCT(newtuple) as Form_pg_class)).relispartition = true;

    /*
     * We already checked for no inheritance children, but reset
     * relhassubclass in case it was left over.
     */
    if (*(*rel).rd_rel).relkind == RELKIND_RELATION && (*(*rel).rd_rel).relhassubclass {
        (*(GETSTRUCT(newtuple) as Form_pg_class)).relhassubclass = false;
    }

    CatalogTupleUpdate(class_rel, &mut (*newtuple).t_self, newtuple);
    heap_freetuple(newtuple);
    table_close(class_rel, RowExclusiveLock);

    /*
     * If we're storing bounds for the default partition, update
     * pg_partitioned_table too.
     */
    if (*bound).is_default {
        update_default_partition_oid(RelationGetRelid(parent), RelationGetRelid(rel)); /* TODO(pg-port): partitioning */
    }

    /* Make these updates visible */
    CommandCounterIncrement();

    /*
     * The partition constraint for the default partition depends on the
     * partition bounds of every other partition, so we must invalidate the
     * relcache entry for that partition every time a partition is added or
     * removed.
     */
    let default_part_oid =
        get_default_oid_from_partdesc(RelationGetPartitionDesc(parent, true)); /* TODO(pg-port): partitioning */
    if OidIsValid(default_part_oid) {
        CacheInvalidateRelcacheByRelid(default_part_oid);
    }

    CacheInvalidateRelcache(parent);
}

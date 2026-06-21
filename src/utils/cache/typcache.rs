//! typcache.rs
//!   POSTGRES type cache code
//!
//! Translated 1:1 from postgres/src/backend/utils/cache/typcache.c
//!
//! The type cache exists to speed lookup of certain information about data
//! types that is not directly available from a type's pg_type row.  For
//! example, we use a type's default btree opclass, or the default hash
//! opclass if no btree opclass exists, to determine which operators should
//! be used for grouping and sorting the type (GROUP BY, ORDER BY ASC/DESC).
//!
//! Several seemingly-odd choices have been made to support use of the type
//! cache by generic array and record handling routines, such as array_eq(),
//! record_cmp(), and hash_array().  Because those routines are used as index
//! support operations, they cannot leak memory.  To allow them to execute
//! efficiently, all information that they would like to re-use across calls
//! is kept in the type cache.
//!
//! Once created, a type cache entry lives as long as the backend does, so
//! there is no need for a call to release a cache entry.  If the type is
//! dropped, the cache entry simply becomes wasted storage.  This is not
//! expected to happen often, and assuming that typcache entries are good
//! permanently allows caching pointers to them in long-lived places.
//!
//! We have some provisions for updating cache entries if the stored data
//! becomes obsolete.  Core data extracted from the pg_type row is updated
//! when we detect updates to pg_type.  Information dependent on opclasses is
//! cleared if we detect updates to pg_opclass.  We also support clearing the
//! tuple descriptor and operator/function parts of a rowtype's cache entry,
//! since those may need to change as a consequence of ALTER TABLE.  Domain
//! constraint changes are also tracked properly.
//!
//! The companion header src/include/utils/typcache.h is merged below; this
//! file is the canonical home for TypeCacheEntry.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/cache/typcache.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use crate::prelude::*;
use crate::makeNode;
use crate::current_cell;
use crate::storage::lockdefs::AccessShareLock;

// Port helpers (cstr_to_string / strcmp / memcpy have no canonical home yet).
unsafe fn cstr_to_string(p: *const c_char) -> String {
    std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
}
extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn memcpy(dst: *mut core::ffi::c_void, src: *const core::ffi::c_void, n: usize) -> *mut core::ffi::c_void;
}


use std::ffi::{c_char, c_int, c_void};

use crate::c::{int16, int32, uint32, uint64, Size};
use crate::postgres_ext::Oid;

use crate::access::common::relation::{relation_close, relation_open};
use crate::access::common::session::CurrentSession;
use crate::access::common::tupdesc::{
    CreateTupleDescCopy, CreateTupleDescCopyConstr, TupleDesc, TupleDescAttr,
};
use crate::access::hash::hashutil::{HASHEXTENDED_PROC, HASHSTANDARD_PROC};
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::nbtree::nbtvalidate::BTORDER_PROC;
use crate::access::stratnum::{
    BTEqualStrategyNumber, BTGreaterStrategyNumber, BTLessStrategyNumber, HTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTLessEqualStrategyNumber,
};
use crate::catalog::pg_constraint::{Form_pg_constraint, CONSTRAINT_CHECK};
use crate::catalog::pg_enum::Form_pg_enum;
use crate::catalog::pg_known_oids::{
    ARRAY_EQ_OP, ARRAY_GT_OP, ARRAY_LT_OP, BTREE_AM_OID, HASH_AM_OID, RECORD_EQ_OP, RECORD_GT_OP,
    RECORD_LT_OP,
};
use crate::catalog::pg_range::Form_pg_range;
use crate::catalog::pg_type::{
    Form_pg_type, TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN, TYPTYPE_ENUM, TYPTYPE_MULTIRANGE,
    TYPTYPE_RANGE,
};
use crate::catalog::pg_type_d::RECORDOID;
use crate::common::int::pg_cmp_u32;
use crate::nodes::bitmapset::{
    bms_add_member, bms_copy, bms_free, bms_is_member, bms_make_singleton, Bitmapset,
};
use crate::nodes::execnodes::{
    DomainConstraintState, DomainConstraintType::DOM_CONSTRAINT_CHECK,
    DomainConstraintType::DOM_CONSTRAINT_NOTNULL, ExprState,
};
use crate::nodes::pg_list::{lappend, lcons, List, ListCell, NIL};
use crate::nodes::primnodes::Expr;
use crate::nodes::read::stringToNode;
use crate::port::pg_bitutils::pg_nextpower2_32;
use crate::utils::fmgr::{fmgr_info_cxt, FmgrInfo};
use crate::utils::hash::dynahash::{
    get_hash_value, hash_create, hash_search, hash_seq_init, hash_seq_init_with_hash_value,
    hash_seq_search, HASHCTL, HASH_BLOBS, HASH_COMPARE, HASH_ELEM, HASH_FUNCTION, HASH_SEQ_STATUS,
    HTAB,
};
use crate::utils::hash::dynahash::HASHACTION::{HASH_ENTER, HASH_FIND, HASH_REMOVE};
use crate::utils::mmgr::mcxt::{
    CacheMemoryContext, MemoryContextSetParent, TopMemoryContext,
};
use crate::utils::palloc::MemoryContextCallback;
use crate::utils::rel::{Relation, RelationGetDescr};

// ----------------------------------------------------------------------------
// Stubs for not-yet-translated subsystems.  Each TODO marks the real home.
// ----------------------------------------------------------------------------

/* TODO(pg-port): real syscache identifiers live in utils/cache/syscache.rs
 * (SysCacheIdentifier enum).  Stub the IDs typcache.c uses until they exist. */
const TYPEOID: c_int = 82;
const CLAOID: c_int = 14;
const CONSTROID: c_int = 19;
const RANGETYPE: c_int = 55;

/* TODO(pg-port): real F_OIDEQ lives in utils/fmgroids.h (generated). */
const F_OIDEQ: crate::c::RegProcedure = 184;

/* TODO(pg-port): real F_* function OIDs live in utils/fmgroids.h (generated). */
const F_BTARRAYCMP: Oid = 0;
const F_BTRECORDCMP: Oid = 0;
const F_HASH_ARRAY: Oid = 0;
const F_HASH_RECORD: Oid = 0;
const F_HASH_RANGE: Oid = 0;
const F_HASH_MULTIRANGE: Oid = 0;
const F_HASH_ARRAY_EXTENDED: Oid = 0;
const F_HASH_RECORD_EXTENDED: Oid = 0;
const F_HASH_RANGE_EXTENDED: Oid = 0;
const F_HASH_MULTIRANGE_EXTENDED: Oid = 0;

/* TODO(pg-port): real catalog Anum/index OIDs live in catalog/pg_constraint.h
 * and catalog/pg_enum.h (generated *_d.h). */
const Anum_pg_constraint_contypid: crate::access::attnum::AttrNumber = 0;
const Anum_pg_constraint_conbin: crate::access::attnum::AttrNumber = 0;
const ConstraintRelationId: Oid = 2606;
const ConstraintTypidIndexId: Oid = 2665;
const Anum_pg_enum_enumtypid: crate::access::attnum::AttrNumber = 0;
const EnumRelationId: Oid = 3501;
const EnumTypIdLabelIndexId: Oid = 3502;

/* TODO(pg-port): real GetSysCacheHashValue1 lives in utils/cache/syscache.rs
 * (macro wrapping GetSysCacheHashValue with explicit 0 keys). */
unsafe fn GetSysCacheHashValue1(cacheId: c_int, key1: Datum) -> uint32 {
    crate::utils::cache::syscache::GetSysCacheHashValue(cacheId, key1, 0, 0, 0)
}

/* TODO(pg-port): real SearchSysCache1/ReleaseSysCache live in
 * utils/cache/syscache.rs. */
unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple {
    crate::utils::cache::syscache::SearchSysCache1(cacheId, key1)
}
unsafe fn ReleaseSysCache(tuple: HeapTuple) {
    crate::utils::cache::syscache::ReleaseSysCache(tuple)
}

/* TODO(pg-port): real lsyscache routines live in utils/cache/lsyscache.rs. */
unsafe fn GetDefaultOpClass(type_id: Oid, am_id: Oid) -> Oid {
    crate::commands::indexcmds::GetDefaultOpClass_full(type_id, am_id)
}
unsafe fn get_opclass_family(opclass: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_opclass_family(opclass)
}
unsafe fn get_opclass_input_type(opclass: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_opclass_input_type(opclass)
}
unsafe fn get_opfamily_member(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    strategy: int16,
) -> Oid {
    crate::utils::cache::lsyscache::get_opfamily_member(opfamily, lefttype, righttype, strategy)
}
unsafe fn get_opfamily_proc(
    opfamily: Oid,
    lefttype: Oid,
    righttype: Oid,
    procnum: int16,
) -> Oid {
    crate::utils::cache::lsyscache::get_opfamily_proc(opfamily, lefttype, righttype, procnum)
}
unsafe fn get_opcode(opno: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_opcode(opno)
}
unsafe fn get_base_element_type(typid: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_base_element_type(typid)
}
unsafe fn getBaseTypeAndTypmod(typid: Oid, typmod: *mut int32) -> Oid {
    crate::utils::cache::lsyscache::getBaseTypeAndTypmod(typid, typmod)
}
unsafe fn get_multirange_range(_multirangeOid: Oid) -> Oid {
    InvalidOid
}

/* TODO(pg-port): real format_type_be lives in utils/adt/format_type.rs. */
unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char {
    c"???".as_ptr() as *mut c_char
}

/* TODO(pg-port): real TextDatumGetCString lives in utils/adt/varlena.rs. */
unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    null_mut()
}

/* TODO(pg-port): real cache-invalidation callbacks live in utils/cache/inval.rs. */
type SyscacheCallbackFunction = Option<unsafe extern "C" fn(arg: Datum, cacheid: c_int, hashvalue: uint32)>;
type RelcacheCallbackFunction = Option<unsafe extern "C" fn(arg: Datum, relid: Oid)>;
unsafe fn CacheRegisterRelcacheCallback(_func: RelcacheCallbackFunction, _arg: Datum) {}
unsafe fn CacheRegisterSyscacheCallback(
    _cacheid: c_int,
    _func: SyscacheCallbackFunction,
    _arg: Datum,
) {
}

/* TODO(pg-port): real CreateCacheMemoryContext lives in utils/mmgr/mcxt.rs. */
unsafe fn CreateCacheMemoryContext() {}

/* TODO(pg-port): real fastgetattr lives in access/htup_details.rs. */
unsafe fn fastgetattr(
    _tup: HeapTuple,
    _attnum: crate::access::attnum::AttrNumber,
    _tupleDesc: TupleDesc,
    isnull: *mut bool,
) -> Datum {
    *isnull = true;
    0
}

/* TODO(pg-port): real systable scan API lives in access/index/genam.rs. */
type SysScanDesc = *mut c_void;
#[repr(C)]
struct ScanKeyData {
    _opaque: [u8; 0],
}
unsafe fn ScanKeyInit(
    _entry: *mut ScanKeyData,
    _attributeNumber: crate::access::attnum::AttrNumber,
    _strategy: int16,
    _procedure: crate::c::RegProcedure,
    _argument: Datum,
) {
}
unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc {
    null_mut()
}
unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    null_mut()
}
unsafe fn systable_endscan(_sysscan: SysScanDesc) {}

/* TODO(pg-port): real table_open/table_close live in access/table/table.rs. */
unsafe fn table_open(relationId: Oid, lockmode: crate::storage::lockdefs::LOCKMODE) -> Relation {
    relation_open(relationId, lockmode)
}
unsafe fn table_close(relation: Relation, lockmode: crate::storage::lockdefs::LOCKMODE) {
    relation_close(relation, lockmode)
}

/* TODO(pg-port): real executor entry points live in executor/executor.rs. */
unsafe fn ExecInitExpr(_node: *mut Expr, _parent: *mut c_void) -> *mut ExprState {
    null_mut()
}

/* TODO(pg-port): real expression_planner lives in optimizer/util/clauses.rs. */
unsafe fn expression_planner(expr: *mut Expr) -> *mut Expr {
    expr
}

/* TODO(pg-port): real tupdesc refcount + free + size helpers live in
 * access/common/tupdesc.rs. */
unsafe fn IncrTupleDescRefCount(tupdesc: TupleDesc) {
    (*tupdesc).tdrefcount += 1;
}
unsafe fn DecrTupleDescRefCount(tupdesc: TupleDesc) {
    (*tupdesc).tdrefcount -= 1;
}
unsafe fn FreeTupleDesc(_tupdesc: TupleDesc) {}
unsafe fn PinTupleDesc(_tupdesc: TupleDesc) {}
unsafe fn TupleDescSize(_tupdesc: TupleDesc) -> Size { crate::access::common::tupdesc::TupleDescSize(_tupdesc) }
unsafe fn TupleDescCopy(_dst: TupleDesc, _src: TupleDesc) {}

/* TODO(pg-port): real equalRowTypes/hashRowType live in access/common/tupdesc.rs. */
unsafe fn equalRowTypes(_tupdesc1: TupleDesc, _tupdesc2: TupleDesc) -> bool {
    false
}
unsafe fn hashRowType(_desc: TupleDesc) -> uint32 {
    0
}

/* TODO(pg-port): real repalloc0_array macro lives in utils/palloc.h.  This
 * expands repalloc + zero-fill of the newly added array tail. */
unsafe fn repalloc0_array_RecordCacheArrayEntry(
    pointer: *mut RecordCacheArrayEntry,
    oldcount: int32,
    newcount: int32,
) -> *mut RecordCacheArrayEntry {
    let newptr = crate::prelude::repalloc(
        pointer as *mut c_void,
        (newcount as Size) * core::mem::size_of::<RecordCacheArrayEntry>(),
    ) as *mut RecordCacheArrayEntry;
    let added = (newcount - oldcount) as Size;
    if added > 0 {
        crate::c::MemSet(
            newptr.add(oldcount as usize) as *mut c_void,
            0,
            added * core::mem::size_of::<RecordCacheArrayEntry>(),
        );
    }
    newptr
}

/* TODO(pg-port): real AllocSetContextCreate macro lives at crate root; the
 * non-macro typcache call sites pass (parent, name, ALLOCSET_SMALL_SIZES). */
unsafe fn AllocSetContextCreate_SmallSizes(
    parent: MemoryContext,
    name: *const c_char,
) -> MemoryContext {
    crate::AllocSetContextCreate!(parent, name, crate::utils::memutils::ALLOCSET_SMALL_SIZES)
}

/* TODO(pg-port): real DSA/dshash machinery lives in utils/dsa.rs and lib/dshash.rs. */
type dsa_pointer = usize;
type dsa_area = c_void;
type dsm_segment = c_void;
type dshash_table = c_void;
type dshash_table_handle = uint32;
type pg_atomic_uint32 = uint32;
const LWTRANCHE_PER_SESSION_RECORD_TYPE: c_int = 0;
const LWTRANCHE_PER_SESSION_RECORD_TYPMOD: c_int = 0;
#[repr(C)]
struct dshash_parameters {
    key_size: Size,
    entry_size: Size,
    compare_function: dshash_compare_function,
    hash_function: dshash_hash_function,
    copy_function: dshash_copy_function,
    tranche_id: c_int,
}
type dshash_compare_function =
    Option<unsafe extern "C" fn(a: *const c_void, b: *const c_void, size: Size, arg: *mut c_void) -> c_int>;
type dshash_hash_function =
    Option<unsafe extern "C" fn(a: *const c_void, size: Size, arg: *mut c_void) -> uint32>;
type dshash_copy_function =
    Option<unsafe extern "C" fn(a: *mut c_void, b: *const c_void, size: Size, arg: *mut c_void)>;
unsafe fn dsa_get_address(_area: *mut dsa_area, _dp: dsa_pointer) -> *mut c_void {
    null_mut()
}
unsafe fn dsa_allocate(_area: *mut dsa_area, _size: Size) -> dsa_pointer {
    0
}
unsafe fn dsa_free(_area: *mut dsa_area, _dp: dsa_pointer) {}
unsafe fn dshash_create(
    _area: *mut dsa_area,
    _params: *const dshash_parameters,
    _arg: *mut c_void,
) -> *mut dshash_table { unimplemented!() }
unsafe fn dshash_attach(
    _area: *mut dsa_area,
    _params: *const dshash_parameters,
    _handle: dshash_table_handle,
    _arg: *mut c_void,
) -> *mut dshash_table { unimplemented!() }
unsafe fn dshash_detach(_hash_table: *mut dshash_table) {}
unsafe fn dshash_get_hash_table_handle(_hash_table: *mut dshash_table) -> dshash_table_handle { unimplemented!() }
unsafe fn dshash_find(
    _hash_table: *mut dshash_table,
    _key: *const c_void,
    _exclusive: bool,
) -> *mut c_void { unimplemented!() }
unsafe fn dshash_find_or_insert(
    _hash_table: *mut dshash_table,
    _key: *const c_void,
    _found: *mut bool,
) -> *mut c_void { unimplemented!() }
unsafe fn dshash_delete_key(_hash_table: *mut dshash_table, _key: *const c_void) -> bool { unimplemented!() }
unsafe fn dshash_release_lock(_hash_table: *mut dshash_table, _entry: *mut c_void) {}
unsafe extern "C" fn dshash_memcpy(_a: *mut c_void, _b: *const c_void, _size: Size, _arg: *mut c_void) {
}
unsafe extern "C" fn dshash_memcmp(
    _a: *const c_void,
    _b: *const c_void,
    _size: Size,
    _arg: *mut c_void,
) -> c_int { crate::lib::dshash::dshash_memcmp(_a, _b, _size, _arg) }
unsafe extern "C" fn dshash_memhash(_a: *const c_void, _size: Size, _arg: *mut c_void) -> uint32 { crate::lib::dshash::dshash_memhash(_a, _size, _arg) }
unsafe fn pg_atomic_init_u32(ptr: *mut pg_atomic_uint32, val: uint32) {
    *ptr = val;
}
unsafe fn pg_atomic_fetch_add_u32(ptr: *mut pg_atomic_uint32, add_: uint32) -> uint32 {
    let old = *ptr;
    *ptr = old + add_;
    old
}
unsafe fn on_dsm_detach(
    _seg: *mut dsm_segment,
    _function: Option<unsafe extern "C" fn(seg: *mut dsm_segment, datum: Datum)>,
    _arg: Datum,
) {
}
unsafe fn IsParallelWorker() -> bool {
    false
}

/* TODO(pg-port): real INJECTION_POINT macro lives in utils/misc/injection_point.rs. */
macro_rules! INJECTION_POINT {
    ($name:expr, $arg:expr) => {{}};
}

/* ----------------------------------------------------------------------------
 *
 * typcache.h declarations (src/include/utils/typcache.h)
 *
 * The TypeCacheEntry struct is the canonical home here; ~19 other files
 * `pub use` it from this module.
 * ----------------------------------------------------------------------------
 */

/* DomainConstraintCache is an opaque struct known only within typcache.c */
/* (defined below as a private struct) */

/* TypeCacheEnumData is an opaque struct known only within typcache.c */
/* (defined below as a private struct) */

#[repr(C)]
pub struct TypeCacheEntry {
    /* typeId is the hash lookup key and MUST BE FIRST */
    pub type_id: Oid, /* OID of the data type */

    pub type_id_hash: uint32, /* hashed value of the OID */

    /* some subsidiary information copied from the pg_type row */
    pub typlen: int16,
    pub typbyval: bool,
    pub typalign: c_char,
    pub typstorage: c_char,
    pub typtype: c_char,
    pub typrelid: Oid,
    pub typsubscript: Oid,
    pub typelem: Oid,
    pub typarray: Oid,
    pub typcollation: Oid,

    /*
     * Information obtained from opfamily entries
     *
     * These will be InvalidOid if no match could be found, or if the
     * information hasn't yet been requested.  Also note that for array and
     * composite types, typcache.c checks that the contained types are
     * comparable or hashable before allowing eq_opr etc to become set.
     */
    pub btree_opf: Oid,      /* the default btree opclass' family */
    pub btree_opintype: Oid, /* the default btree opclass' opcintype */
    pub hash_opf: Oid,       /* the default hash opclass' family */
    pub hash_opintype: Oid,  /* the default hash opclass' opcintype */
    pub eq_opr: Oid,         /* the equality operator */
    pub lt_opr: Oid,         /* the less-than operator */
    pub gt_opr: Oid,         /* the greater-than operator */
    pub cmp_proc: Oid,       /* the btree comparison function */
    pub hash_proc: Oid,      /* the hash calculation function */
    pub hash_extended_proc: Oid, /* the extended hash calculation function */

    /*
     * Pre-set-up fmgr call info for the equality operator, the btree
     * comparison function, and the hash calculation function.  These are kept
     * in the type cache to avoid problems with memory leaks in repeated calls
     * to functions such as array_eq, array_cmp, hash_array.  There is not
     * currently a need to maintain call info for the lt_opr or gt_opr.
     */
    pub eq_opr_finfo: FmgrInfo,
    pub cmp_proc_finfo: FmgrInfo,
    pub hash_proc_finfo: FmgrInfo,
    pub hash_extended_proc_finfo: FmgrInfo,

    /*
     * Tuple descriptor if it's a composite type (row type).  NULL if not
     * composite or information hasn't yet been requested.  (NOTE: this is a
     * reference-counted tupledesc.)
     *
     * To simplify caching dependent info, tupDesc_identifier is an identifier
     * for this tupledesc that is unique for the life of the process, and
     * changes anytime the tupledesc does.  Zero if not yet determined.
     */
    pub tupDesc: TupleDesc,
    pub tupDesc_identifier: uint64,

    /*
     * Fields computed when TYPECACHE_RANGE_INFO is requested.  Zeroes if not
     * a range type or information hasn't yet been requested.  Note that
     * rng_cmp_proc_finfo could be different from the element type's default
     * btree comparison function.
     */
    pub rngelemtype: *mut TypeCacheEntry, /* range's element type */
    pub rng_opfamily: Oid,                /* opfamily to use for range comparisons */
    pub rng_collation: Oid,               /* collation for comparisons, if any */
    pub rng_cmp_proc_finfo: FmgrInfo,     /* comparison function */
    pub rng_canonical_finfo: FmgrInfo,    /* canonicalization function, if any */
    pub rng_subdiff_finfo: FmgrInfo,      /* difference function, if any */

    /*
     * Fields computed when TYPECACHE_MULTIRANGE_INFO is required.
     */
    pub rngtype: *mut TypeCacheEntry, /* multirange's range underlying type */

    /*
     * Domain's base type and typmod if it's a domain type.  Zeroes if not
     * domain, or if information hasn't been requested.
     */
    pub domainBaseType: Oid,
    pub domainBaseTypmod: int32,

    /*
     * Domain constraint data if it's a domain type.  NULL if not domain, or
     * if domain has no constraints, or if information hasn't been requested.
     */
    pub domainData: *mut DomainConstraintCache,

    /* Private data, for internal use of typcache.c only */
    pub flags: c_int, /* flags about what we've computed */

    /*
     * Private information about an enum type.  NULL if not enum or
     * information hasn't been requested.
     */
    pub enumData: *mut TypeCacheEnumData,

    /* We also maintain a list of all known domain-type cache entries */
    pub nextDomain: *mut TypeCacheEntry,
}

/* Bit flags to indicate which fields a given caller needs to have set */
pub const TYPECACHE_EQ_OPR: c_int = 0x00001;
pub const TYPECACHE_LT_OPR: c_int = 0x00002;
pub const TYPECACHE_GT_OPR: c_int = 0x00004;
pub const TYPECACHE_CMP_PROC: c_int = 0x00008;
pub const TYPECACHE_HASH_PROC: c_int = 0x00010;
pub const TYPECACHE_EQ_OPR_FINFO: c_int = 0x00020;
pub const TYPECACHE_CMP_PROC_FINFO: c_int = 0x00040;
pub const TYPECACHE_HASH_PROC_FINFO: c_int = 0x00080;
pub const TYPECACHE_TUPDESC: c_int = 0x00100;
pub const TYPECACHE_BTREE_OPFAMILY: c_int = 0x00200;
pub const TYPECACHE_HASH_OPFAMILY: c_int = 0x00400;
pub const TYPECACHE_RANGE_INFO: c_int = 0x00800;
pub const TYPECACHE_DOMAIN_BASE_INFO: c_int = 0x01000;
pub const TYPECACHE_DOMAIN_CONSTR_INFO: c_int = 0x02000;
pub const TYPECACHE_HASH_EXTENDED_PROC: c_int = 0x04000;
pub const TYPECACHE_HASH_EXTENDED_PROC_FINFO: c_int = 0x08000;
pub const TYPECACHE_MULTIRANGE_INFO: c_int = 0x10000;

/* This value will not equal any valid tupledesc identifier, nor 0 */
pub const INVALID_TUPLEDESC_IDENTIFIER: uint64 = 1;

/*
 * Callers wishing to maintain a long-lived reference to a domain's constraint
 * set must store it in one of these.  Use InitDomainConstraintRef() and
 * UpdateDomainConstraintRef() to manage it.  Note: DomainConstraintState is
 * considered an executable expression type, so it's defined in execnodes.h.
 */
#[repr(C)]
pub struct DomainConstraintRef {
    pub constraints: *mut List, /* list of DomainConstraintState nodes */
    pub refctx: MemoryContext,  /* context holding DomainConstraintRef */
    pub tcache: *mut TypeCacheEntry, /* typcache entry for domain type */
    pub need_exprstate: bool,   /* does caller need check_exprstate? */

    /* Management data --- treat these fields as private to typcache.c */
    pub dcc: *mut DomainConstraintCache, /* current constraints, or NULL if none */
    pub callback: MemoryContextCallback, /* used to release refcount when done */
}

/* SharedRecordTypmodRegistry is defined below (opaque to the header). */

/* ----------------------------------------------------------------------------
 * typcache.c
 * ----------------------------------------------------------------------------
 */

/* The main type cache hashtable searched by lookup_type_cache */
static mut TypeCacheHash: *mut HTAB = null_mut();

/*
 * The mapping of relation's OID to the corresponding composite type OID.
 * We're keeping the map entry when the corresponding typentry has something
 * to clear i.e it has either TCFLAGS_HAVE_PG_TYPE_DATA, or
 * TCFLAGS_OPERATOR_FLAGS, or tupdesc.
 */
static mut RelIdToTypeIdCacheHash: *mut HTAB = null_mut();

#[repr(C)]
struct RelIdToTypeIdCacheEntry {
    relid: Oid,           /* OID of the relation */
    composite_typid: Oid, /* OID of the relation's composite type */
}

/* List of type cache entries for domain types */
static mut firstDomainTypeEntry: *mut TypeCacheEntry = null_mut();

/* Private flag bits in the TypeCacheEntry.flags field */
const TCFLAGS_HAVE_PG_TYPE_DATA: c_int = 0x000001;
const TCFLAGS_CHECKED_BTREE_OPCLASS: c_int = 0x000002;
const TCFLAGS_CHECKED_HASH_OPCLASS: c_int = 0x000004;
const TCFLAGS_CHECKED_EQ_OPR: c_int = 0x000008;
const TCFLAGS_CHECKED_LT_OPR: c_int = 0x000010;
const TCFLAGS_CHECKED_GT_OPR: c_int = 0x000020;
const TCFLAGS_CHECKED_CMP_PROC: c_int = 0x000040;
const TCFLAGS_CHECKED_HASH_PROC: c_int = 0x000080;
const TCFLAGS_CHECKED_HASH_EXTENDED_PROC: c_int = 0x000100;
const TCFLAGS_CHECKED_ELEM_PROPERTIES: c_int = 0x000200;
const TCFLAGS_HAVE_ELEM_EQUALITY: c_int = 0x000400;
const TCFLAGS_HAVE_ELEM_COMPARE: c_int = 0x000800;
const TCFLAGS_HAVE_ELEM_HASHING: c_int = 0x001000;
const TCFLAGS_HAVE_ELEM_EXTENDED_HASHING: c_int = 0x002000;
const TCFLAGS_CHECKED_FIELD_PROPERTIES: c_int = 0x004000;
const TCFLAGS_HAVE_FIELD_EQUALITY: c_int = 0x008000;
const TCFLAGS_HAVE_FIELD_COMPARE: c_int = 0x010000;
const TCFLAGS_HAVE_FIELD_HASHING: c_int = 0x020000;
const TCFLAGS_HAVE_FIELD_EXTENDED_HASHING: c_int = 0x040000;
const TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS: c_int = 0x080000;
const TCFLAGS_DOMAIN_BASE_IS_COMPOSITE: c_int = 0x100000;

/* The flags associated with equality/comparison/hashing are all but these: */
const TCFLAGS_OPERATOR_FLAGS: c_int = !(TCFLAGS_HAVE_PG_TYPE_DATA
    | TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS
    | TCFLAGS_DOMAIN_BASE_IS_COMPOSITE);

/*
 * Data stored about a domain type's constraints.  Note that we do not create
 * this struct for the common case of a constraint-less domain; we just set
 * domainData to NULL to indicate that.
 *
 * Within a DomainConstraintCache, we store expression plan trees, but the
 * check_exprstate fields of the DomainConstraintState nodes are just NULL.
 * When needed, expression evaluation nodes are built by flat-copying the
 * DomainConstraintState nodes and applying ExecInitExpr to check_expr.
 * Such a node tree is not part of the DomainConstraintCache, but is
 * considered to belong to a DomainConstraintRef.
 */
#[repr(C)]
pub struct DomainConstraintCache {
    pub constraints: *mut List, /* list of DomainConstraintState nodes */
    pub dccContext: MemoryContext, /* memory context holding all associated data */
    pub dccRefCount: std::ffi::c_long, /* number of references to this struct */
}

/* Private information to support comparisons of enum values */
#[repr(C)]
#[derive(Clone, Copy)]
struct EnumItem {
    enum_oid: Oid,        /* OID of one enum value */
    sort_order: float4,   /* its sort position */
}

#[repr(C)]
pub struct TypeCacheEnumData {
    pub bitmap_base: Oid,             /* OID corresponding to bit 0 of bitmapset */
    pub sorted_values: *mut Bitmapset, /* Set of OIDs known to be in order */
    pub num_values: c_int,            /* total number of values in enum */
    enum_values: [EnumItem; 0],       /* FLEXIBLE_ARRAY_MEMBER */
}

/*
 * We use a separate table for storing the definitions of non-anonymous
 * record types.  Once defined, a record type will be remembered for the
 * life of the backend.  Subsequent uses of the "same" record type (where
 * sameness means equalRowTypes) will refer to the existing table entry.
 *
 * Stored record types are remembered in a linear array of TupleDescs,
 * which can be indexed quickly with the assigned typmod.  There is also
 * a hash table to speed searches for matching TupleDescs.
 */

#[repr(C)]
struct RecordCacheEntry {
    tupdesc: TupleDesc,
}

/*
 * To deal with non-anonymous record types that are exchanged by backends
 * involved in a parallel query, we also need a shared version of the above.
 */
#[repr(C)]
pub struct SharedRecordTypmodRegistry {
    /* A hash table for finding a matching TupleDesc. */
    record_table_handle: dshash_table_handle,
    /* A hash table for finding a TupleDesc by typmod. */
    typmod_table_handle: dshash_table_handle,
    /* A source of new record typmod numbers. */
    next_typmod: pg_atomic_uint32,
}

/*
 * When using shared tuple descriptors as hash table keys we need a way to be
 * able to search for an equal shared TupleDesc using a backend-local
 * TupleDesc.  So we use this type which can hold either, and hash and compare
 * functions that know how to handle both.
 */
#[repr(C)]
struct SharedRecordTableKey {
    u: SharedRecordTableKeyUnion,
    shared: bool,
}
#[repr(C)]
union SharedRecordTableKeyUnion {
    local_tupdesc: TupleDesc,
    shared_tupdesc: dsa_pointer,
}

/*
 * The shared version of RecordCacheEntry.  This lets us look up a typmod
 * using a TupleDesc which may be in local or shared memory.
 */
#[repr(C)]
struct SharedRecordTableEntry {
    key: SharedRecordTableKey,
}

/*
 * An entry in SharedRecordTypmodRegistry's typmod table.  This lets us look
 * up a TupleDesc in shared memory using a typmod.
 */
#[repr(C)]
struct SharedTypmodTableEntry {
    typmod: uint32,
    shared_tupdesc: dsa_pointer,
}

static mut in_progress_list: *mut Oid = null_mut();
static mut in_progress_list_len: c_int = 0;
static mut in_progress_list_maxlen: c_int = 0;

/*
 * A comparator function for SharedRecordTableKey.
 */
unsafe extern "C" fn shared_record_table_compare(
    a: *const c_void,
    b: *const c_void,
    _size: Size,
    arg: *mut c_void,
) -> c_int {
    let area = arg as *mut dsa_area;
    let k1 = a as *mut SharedRecordTableKey;
    let k2 = b as *mut SharedRecordTableKey;
    let t1: TupleDesc;
    let t2: TupleDesc;

    if (*k1).shared {
        t1 = dsa_get_address(area, (*k1).u.shared_tupdesc) as TupleDesc;
    } else {
        t1 = (*k1).u.local_tupdesc;
    }

    if (*k2).shared {
        t2 = dsa_get_address(area, (*k2).u.shared_tupdesc) as TupleDesc;
    } else {
        t2 = (*k2).u.local_tupdesc;
    }

    if equalRowTypes(t1, t2) {
        0
    } else {
        1
    }
}

/*
 * A hash function for SharedRecordTableKey.
 */
unsafe extern "C" fn shared_record_table_hash(
    a: *const c_void,
    _size: Size,
    arg: *mut c_void,
) -> uint32 {
    let area = arg as *mut dsa_area;
    let k = a as *mut SharedRecordTableKey;
    let t: TupleDesc;

    if (*k).shared {
        t = dsa_get_address(area, (*k).u.shared_tupdesc) as TupleDesc;
    } else {
        t = (*k).u.local_tupdesc;
    }

    hashRowType(t)
}

/* Parameters for SharedRecordTypmodRegistry's TupleDesc table. */
static srtr_record_table_params: dshash_parameters = dshash_parameters {
    key_size: core::mem::size_of::<SharedRecordTableKey>(), /* unused */
    entry_size: core::mem::size_of::<SharedRecordTableEntry>(),
    compare_function: Some(shared_record_table_compare),
    hash_function: Some(shared_record_table_hash),
    copy_function: Some(dshash_memcpy),
    tranche_id: LWTRANCHE_PER_SESSION_RECORD_TYPE,
};

/* Parameters for SharedRecordTypmodRegistry's typmod hash table. */
static srtr_typmod_table_params: dshash_parameters = dshash_parameters {
    key_size: core::mem::size_of::<uint32>(),
    entry_size: core::mem::size_of::<SharedTypmodTableEntry>(),
    compare_function: Some(dshash_memcmp),
    hash_function: Some(dshash_memhash),
    copy_function: Some(dshash_memcpy),
    tranche_id: LWTRANCHE_PER_SESSION_RECORD_TYPMOD,
};

/* hashtable for recognizing registered record types */
static mut RecordCacheHash: *mut HTAB = null_mut();

#[repr(C)]
struct RecordCacheArrayEntry {
    id: uint64,
    tupdesc: TupleDesc,
}

/* array of info about registered record types, indexed by assigned typmod */
static mut RecordCacheArray: *mut RecordCacheArrayEntry = null_mut();
static mut RecordCacheArrayLen: int32 = 0; /* allocated length of above array */
static mut NextRecordTypmod: int32 = 0; /* number of entries used */

/*
 * Process-wide counter for generating unique tupledesc identifiers.
 * Zero and one (INVALID_TUPLEDESC_IDENTIFIER) aren't allowed to be chosen
 * as identifiers, so we start the counter at INVALID_TUPLEDESC_IDENTIFIER.
 */
static mut tupledesc_id_counter: uint64 = INVALID_TUPLEDESC_IDENTIFIER;

/*
 * Hash function compatible with one-arg system cache hash function.
 */
unsafe extern "C" fn type_cache_syshash(key: *const c_void, keysize: Size) -> uint32 {
    Assert!(keysize == core::mem::size_of::<Oid>());
    GetSysCacheHashValue1(TYPEOID, ObjectIdGetDatum(*(key as *const Oid)))
}

/*
 * lookup_type_cache
 *
 * Fetch the type cache entry for the specified datatype, and make sure that
 * all the fields requested by bits in 'flags' are valid.
 *
 * The result is never NULL --- we will ereport() if the passed type OID is
 * invalid.  Note however that we may fail to find one or more of the
 * values requested by 'flags'; the caller needs to check whether the fields
 * are InvalidOid or not.
 *
 * Note that while filling TypeCacheEntry we might process concurrent
 * invalidation messages, causing our not-yet-filled TypeCacheEntry to be
 * invalidated.  In this case, we typically only clear flags while values are
 * still available for the caller.  It's expected that the caller holds
 * enough locks on type-depending objects that the values are still relevant.
 * It's also important that the tupdesc is filled after all other
 * TypeCacheEntry items for TYPTYPE_COMPOSITE.  So, tupdesc can't get
 * invalidated during the lookup_type_cache() call.
 */
#[no_mangle]
pub unsafe fn lookup_type_cache(type_id: Oid, mut flags: c_int) -> *mut TypeCacheEntry {
    let mut typentry: *mut TypeCacheEntry;
    let mut found: bool = false;
    let in_progress_offset: c_int;

    if TypeCacheHash.is_null() {
        /* First time through: initialize the hash table */
        let mut ctl: HASHCTL = core::mem::zeroed();
        let allocsize: c_int;

        ctl.keysize = core::mem::size_of::<Oid>();
        ctl.entrysize = core::mem::size_of::<TypeCacheEntry>();

        /*
         * TypeCacheEntry takes hash value from the system cache. For
         * TypeCacheHash we use the same hash in order to speedup search by
         * hash value. This is used by hash_seq_init_with_hash_value().
         */
        ctl.hash = Some(type_cache_syshash);

        TypeCacheHash = hash_create(
            c"Type information cache".as_ptr(),
            64,
            &ctl,
            HASH_ELEM | HASH_FUNCTION,
        );

        Assert!(RelIdToTypeIdCacheHash.is_null());

        ctl.keysize = core::mem::size_of::<Oid>();
        ctl.entrysize = core::mem::size_of::<RelIdToTypeIdCacheEntry>();
        RelIdToTypeIdCacheHash = hash_create(
            c"Map from relid to OID of cached composite type".as_ptr(),
            64,
            &ctl,
            HASH_ELEM | HASH_BLOBS,
        );

        /* Also set up callbacks for SI invalidations */
        CacheRegisterRelcacheCallback(Some(TypeCacheRelCallback), 0);
        CacheRegisterSyscacheCallback(TYPEOID, Some(TypeCacheTypCallback), 0);
        CacheRegisterSyscacheCallback(CLAOID, Some(TypeCacheOpcCallback), 0);
        CacheRegisterSyscacheCallback(CONSTROID, Some(TypeCacheConstrCallback), 0);

        /* Also make sure CacheMemoryContext exists */
        if CacheMemoryContext.is_null() {
            CreateCacheMemoryContext();
        }

        /*
         * reserve enough in_progress_list slots for many cases
         */
        allocsize = 4;
        in_progress_list = MemoryContextAlloc(
            CacheMemoryContext as crate::utils::palloc::MemoryContext,
            (allocsize as Size) * core::mem::size_of::<Oid>(),
        ) as *mut Oid;
        in_progress_list_maxlen = allocsize;
    }

    Assert!(!TypeCacheHash.is_null() && !RelIdToTypeIdCacheHash.is_null());

    /* Register to catch invalidation messages */
    if in_progress_list_len >= in_progress_list_maxlen {
        let allocsize: c_int;

        allocsize = in_progress_list_maxlen * 2;
        in_progress_list = repalloc(
            in_progress_list as *mut c_void,
            (allocsize as Size) * core::mem::size_of::<Oid>(),
        ) as *mut Oid;
        in_progress_list_maxlen = allocsize;
    }
    in_progress_offset = in_progress_list_len;
    in_progress_list_len += 1;
    *in_progress_list.add(in_progress_offset as usize) = type_id;

    /* Try to look up an existing entry */
    typentry = hash_search(
        TypeCacheHash,
        &type_id as *const Oid as *const c_void,
        HASH_FIND,
        null_mut(),
    ) as *mut TypeCacheEntry;
    if typentry.is_null() {
        /*
         * If we didn't find one, we want to make one.  But first look up the
         * pg_type row, just to make sure we don't make a cache entry for an
         * invalid type OID.  If the type OID is not valid, present a
         * user-facing error, since some code paths such as domain_in() allow
         * this function to be reached with a user-supplied OID.
         */
        let tp: HeapTuple;
        let typtup: Form_pg_type;

        tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id));
        if !HeapTupleIsValid(tp) {
            ereport!(
                ERROR,
                errmsg!("type with OID {} does not exist", type_id)
            );
        }
        typtup = GETSTRUCT(tp) as Form_pg_type;
        if !(*typtup).typisdefined {
            ereport!(
                ERROR,
                errmsg!(
                    "type \"{}\" is only a shell",
                    cstr_to_string(NameStr(&(*typtup).typname))
                )
            );
        }

        /* Now make the typcache entry */
        typentry = hash_search(
            TypeCacheHash,
            &type_id as *const Oid as *const c_void,
            HASH_ENTER,
            &mut found,
        ) as *mut TypeCacheEntry;
        Assert!(!found); /* it wasn't there a moment ago */

        MemSet(
            typentry as *mut c_void,
            0,
            core::mem::size_of::<TypeCacheEntry>(),
        );

        /* These fields can never change, by definition */
        (*typentry).type_id = type_id;
        (*typentry).type_id_hash =
            get_hash_value(TypeCacheHash, &type_id as *const Oid as *const c_void);

        /* Keep this part in sync with the code below */
        (*typentry).typlen = (*typtup).typlen;
        (*typentry).typbyval = (*typtup).typbyval;
        (*typentry).typalign = (*typtup).typalign;
        (*typentry).typstorage = (*typtup).typstorage;
        (*typentry).typtype = (*typtup).typtype;
        (*typentry).typrelid = (*typtup).typrelid;
        (*typentry).typsubscript = (*typtup).typsubscript;
        (*typentry).typelem = (*typtup).typelem;
        (*typentry).typarray = (*typtup).typarray;
        (*typentry).typcollation = (*typtup).typcollation;
        (*typentry).flags |= TCFLAGS_HAVE_PG_TYPE_DATA;

        /* If it's a domain, immediately thread it into the domain cache list */
        if (*typentry).typtype == TYPTYPE_DOMAIN {
            (*typentry).nextDomain = firstDomainTypeEntry;
            firstDomainTypeEntry = typentry;
        }

        ReleaseSysCache(tp);
    } else if ((*typentry).flags & TCFLAGS_HAVE_PG_TYPE_DATA) == 0 {
        /*
         * We have an entry, but its pg_type row got changed, so reload the
         * data obtained directly from pg_type.
         */
        let tp: HeapTuple;
        let typtup: Form_pg_type;

        tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id));
        if !HeapTupleIsValid(tp) {
            ereport!(
                ERROR,
                errmsg!("type with OID {} does not exist", type_id)
            );
        }
        typtup = GETSTRUCT(tp) as Form_pg_type;
        if !(*typtup).typisdefined {
            ereport!(
                ERROR,
                errmsg!(
                    "type \"{}\" is only a shell",
                    cstr_to_string(NameStr(&(*typtup).typname))
                )
            );
        }

        /*
         * Keep this part in sync with the code above.  Many of these fields
         * shouldn't ever change, particularly typtype, but copy 'em anyway.
         */
        (*typentry).typlen = (*typtup).typlen;
        (*typentry).typbyval = (*typtup).typbyval;
        (*typentry).typalign = (*typtup).typalign;
        (*typentry).typstorage = (*typtup).typstorage;
        (*typentry).typtype = (*typtup).typtype;
        (*typentry).typrelid = (*typtup).typrelid;
        (*typentry).typsubscript = (*typtup).typsubscript;
        (*typentry).typelem = (*typtup).typelem;
        (*typentry).typarray = (*typtup).typarray;
        (*typentry).typcollation = (*typtup).typcollation;
        (*typentry).flags |= TCFLAGS_HAVE_PG_TYPE_DATA;

        ReleaseSysCache(tp);
    }

    /*
     * Look up opclasses if we haven't already and any dependent info is
     * requested.
     */
    if (flags
        & (TYPECACHE_EQ_OPR
            | TYPECACHE_LT_OPR
            | TYPECACHE_GT_OPR
            | TYPECACHE_CMP_PROC
            | TYPECACHE_EQ_OPR_FINFO
            | TYPECACHE_CMP_PROC_FINFO
            | TYPECACHE_BTREE_OPFAMILY))
        != 0
        && ((*typentry).flags & TCFLAGS_CHECKED_BTREE_OPCLASS) == 0
    {
        let opclass: Oid;

        opclass = GetDefaultOpClass(type_id, BTREE_AM_OID);
        if OidIsValid(opclass) {
            (*typentry).btree_opf = get_opclass_family(opclass);
            (*typentry).btree_opintype = get_opclass_input_type(opclass);
        } else {
            (*typentry).btree_opintype = InvalidOid;
            (*typentry).btree_opf = (*typentry).btree_opintype;
        }

        /*
         * Reset information derived from btree opclass.  Note in particular
         * that we'll redetermine the eq_opr even if we previously found one;
         * this matters in case a btree opclass has been added to a type that
         * previously had only a hash opclass.
         */
        (*typentry).flags &= !(TCFLAGS_CHECKED_EQ_OPR
            | TCFLAGS_CHECKED_LT_OPR
            | TCFLAGS_CHECKED_GT_OPR
            | TCFLAGS_CHECKED_CMP_PROC);
        (*typentry).flags |= TCFLAGS_CHECKED_BTREE_OPCLASS;
    }

    /*
     * If we need to look up equality operator, and there's no btree opclass,
     * force lookup of hash opclass.
     */
    if (flags & (TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO)) != 0
        && ((*typentry).flags & TCFLAGS_CHECKED_EQ_OPR) == 0
        && (*typentry).btree_opf == InvalidOid
    {
        flags |= TYPECACHE_HASH_OPFAMILY;
    }

    if (flags
        & (TYPECACHE_HASH_PROC
            | TYPECACHE_HASH_PROC_FINFO
            | TYPECACHE_HASH_EXTENDED_PROC
            | TYPECACHE_HASH_EXTENDED_PROC_FINFO
            | TYPECACHE_HASH_OPFAMILY))
        != 0
        && ((*typentry).flags & TCFLAGS_CHECKED_HASH_OPCLASS) == 0
    {
        let opclass: Oid;

        opclass = GetDefaultOpClass(type_id, HASH_AM_OID);
        if OidIsValid(opclass) {
            (*typentry).hash_opf = get_opclass_family(opclass);
            (*typentry).hash_opintype = get_opclass_input_type(opclass);
        } else {
            (*typentry).hash_opintype = InvalidOid;
            (*typentry).hash_opf = (*typentry).hash_opintype;
        }

        /*
         * Reset information derived from hash opclass.  We do *not* reset the
         * eq_opr; if we already found one from the btree opclass, that
         * decision is still good.
         */
        (*typentry).flags &=
            !(TCFLAGS_CHECKED_HASH_PROC | TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
        (*typentry).flags |= TCFLAGS_CHECKED_HASH_OPCLASS;
    }

    /*
     * Look for requested operators and functions, if we haven't already.
     */
    if (flags & (TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO)) != 0
        && ((*typentry).flags & TCFLAGS_CHECKED_EQ_OPR) == 0
    {
        let mut eq_opr: Oid = InvalidOid;

        if (*typentry).btree_opf != InvalidOid {
            eq_opr = get_opfamily_member(
                (*typentry).btree_opf,
                (*typentry).btree_opintype,
                (*typentry).btree_opintype,
                BTEqualStrategyNumber as int16,
            );
        }
        if eq_opr == InvalidOid && (*typentry).hash_opf != InvalidOid {
            eq_opr = get_opfamily_member(
                (*typentry).hash_opf,
                (*typentry).hash_opintype,
                (*typentry).hash_opintype,
                HTEqualStrategyNumber as int16,
            );
        }

        /*
         * If the proposed equality operator is array_eq or record_eq, check
         * to see if the element type or column types support equality.  If
         * not, array_eq or record_eq would fail at runtime, so we don't want
         * to report that the type has equality.  (We can omit similar
         * checking for ranges and multiranges because ranges can't be created
         * in the first place unless their subtypes support equality.)
         */
        if eq_opr == ARRAY_EQ_OP && !array_element_has_equality(typentry) {
            eq_opr = InvalidOid;
        } else if eq_opr == RECORD_EQ_OP && !record_fields_have_equality(typentry) {
            eq_opr = InvalidOid;
        }

        /* Force update of eq_opr_finfo only if we're changing state */
        if (*typentry).eq_opr != eq_opr {
            (*typentry).eq_opr_finfo.fn_oid = InvalidOid;
        }

        (*typentry).eq_opr = eq_opr;

        /*
         * Reset info about hash functions whenever we pick up new info about
         * equality operator.  This is so we can ensure that the hash
         * functions match the operator.
         */
        (*typentry).flags &=
            !(TCFLAGS_CHECKED_HASH_PROC | TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
        (*typentry).flags |= TCFLAGS_CHECKED_EQ_OPR;
    }
    if (flags & TYPECACHE_LT_OPR) != 0 && ((*typentry).flags & TCFLAGS_CHECKED_LT_OPR) == 0 {
        let mut lt_opr: Oid = InvalidOid;

        if (*typentry).btree_opf != InvalidOid {
            lt_opr = get_opfamily_member(
                (*typentry).btree_opf,
                (*typentry).btree_opintype,
                (*typentry).btree_opintype,
                BTLessStrategyNumber as int16,
            );
        }

        /*
         * As above, make sure array_cmp or record_cmp will succeed; but again
         * we need no special check for ranges or multiranges.
         */
        if lt_opr == ARRAY_LT_OP && !array_element_has_compare(typentry) {
            lt_opr = InvalidOid;
        } else if lt_opr == RECORD_LT_OP && !record_fields_have_compare(typentry) {
            lt_opr = InvalidOid;
        }

        (*typentry).lt_opr = lt_opr;
        (*typentry).flags |= TCFLAGS_CHECKED_LT_OPR;
    }
    if (flags & TYPECACHE_GT_OPR) != 0 && ((*typentry).flags & TCFLAGS_CHECKED_GT_OPR) == 0 {
        let mut gt_opr: Oid = InvalidOid;

        if (*typentry).btree_opf != InvalidOid {
            gt_opr = get_opfamily_member(
                (*typentry).btree_opf,
                (*typentry).btree_opintype,
                (*typentry).btree_opintype,
                BTGreaterStrategyNumber as int16,
            );
        }

        /*
         * As above, make sure array_cmp or record_cmp will succeed; but again
         * we need no special check for ranges or multiranges.
         */
        if gt_opr == ARRAY_GT_OP && !array_element_has_compare(typentry) {
            gt_opr = InvalidOid;
        } else if gt_opr == RECORD_GT_OP && !record_fields_have_compare(typentry) {
            gt_opr = InvalidOid;
        }

        (*typentry).gt_opr = gt_opr;
        (*typentry).flags |= TCFLAGS_CHECKED_GT_OPR;
    }
    if (flags & (TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO)) != 0
        && ((*typentry).flags & TCFLAGS_CHECKED_CMP_PROC) == 0
    {
        let mut cmp_proc: Oid = InvalidOid;

        if (*typentry).btree_opf != InvalidOid {
            cmp_proc = get_opfamily_proc(
                (*typentry).btree_opf,
                (*typentry).btree_opintype,
                (*typentry).btree_opintype,
                BTORDER_PROC,
            );
        }

        /*
         * As above, make sure array_cmp or record_cmp will succeed; but again
         * we need no special check for ranges or multiranges.
         */
        if cmp_proc == F_BTARRAYCMP && !array_element_has_compare(typentry) {
            cmp_proc = InvalidOid;
        } else if cmp_proc == F_BTRECORDCMP && !record_fields_have_compare(typentry) {
            cmp_proc = InvalidOid;
        }

        /* Force update of cmp_proc_finfo only if we're changing state */
        if (*typentry).cmp_proc != cmp_proc {
            (*typentry).cmp_proc_finfo.fn_oid = InvalidOid;
        }

        (*typentry).cmp_proc = cmp_proc;
        (*typentry).flags |= TCFLAGS_CHECKED_CMP_PROC;
    }
    if (flags & (TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO)) != 0
        && ((*typentry).flags & TCFLAGS_CHECKED_HASH_PROC) == 0
    {
        let mut hash_proc: Oid = InvalidOid;

        /*
         * We insist that the eq_opr, if one has been determined, match the
         * hash opclass; else report there is no hash function.
         */
        if (*typentry).hash_opf != InvalidOid
            && (!OidIsValid((*typentry).eq_opr)
                || (*typentry).eq_opr
                    == get_opfamily_member(
                        (*typentry).hash_opf,
                        (*typentry).hash_opintype,
                        (*typentry).hash_opintype,
                        HTEqualStrategyNumber as int16,
                    ))
        {
            hash_proc = get_opfamily_proc(
                (*typentry).hash_opf,
                (*typentry).hash_opintype,
                (*typentry).hash_opintype,
                HASHSTANDARD_PROC as int16,
            );
        }

        /*
         * As above, make sure hash_array, hash_record, or hash_range will
         * succeed.
         */
        if hash_proc == F_HASH_ARRAY && !array_element_has_hashing(typentry) {
            hash_proc = InvalidOid;
        } else if hash_proc == F_HASH_RECORD && !record_fields_have_hashing(typentry) {
            hash_proc = InvalidOid;
        } else if hash_proc == F_HASH_RANGE && !range_element_has_hashing(typentry) {
            hash_proc = InvalidOid;
        }

        /*
         * Likewise for hash_multirange.
         */
        if hash_proc == F_HASH_MULTIRANGE && !multirange_element_has_hashing(typentry) {
            hash_proc = InvalidOid;
        }

        /* Force update of hash_proc_finfo only if we're changing state */
        if (*typentry).hash_proc != hash_proc {
            (*typentry).hash_proc_finfo.fn_oid = InvalidOid;
        }

        (*typentry).hash_proc = hash_proc;
        (*typentry).flags |= TCFLAGS_CHECKED_HASH_PROC;
    }
    if (flags & (TYPECACHE_HASH_EXTENDED_PROC | TYPECACHE_HASH_EXTENDED_PROC_FINFO)) != 0
        && ((*typentry).flags & TCFLAGS_CHECKED_HASH_EXTENDED_PROC) == 0
    {
        let mut hash_extended_proc: Oid = InvalidOid;

        /*
         * We insist that the eq_opr, if one has been determined, match the
         * hash opclass; else report there is no hash function.
         */
        if (*typentry).hash_opf != InvalidOid
            && (!OidIsValid((*typentry).eq_opr)
                || (*typentry).eq_opr
                    == get_opfamily_member(
                        (*typentry).hash_opf,
                        (*typentry).hash_opintype,
                        (*typentry).hash_opintype,
                        HTEqualStrategyNumber as int16,
                    ))
        {
            hash_extended_proc = get_opfamily_proc(
                (*typentry).hash_opf,
                (*typentry).hash_opintype,
                (*typentry).hash_opintype,
                HASHEXTENDED_PROC as int16,
            );
        }

        /*
         * As above, make sure hash_array_extended, hash_record_extended, or
         * hash_range_extended will succeed.
         */
        if hash_extended_proc == F_HASH_ARRAY_EXTENDED
            && !array_element_has_extended_hashing(typentry)
        {
            hash_extended_proc = InvalidOid;
        } else if hash_extended_proc == F_HASH_RECORD_EXTENDED
            && !record_fields_have_extended_hashing(typentry)
        {
            hash_extended_proc = InvalidOid;
        } else if hash_extended_proc == F_HASH_RANGE_EXTENDED
            && !range_element_has_extended_hashing(typentry)
        {
            hash_extended_proc = InvalidOid;
        }

        /*
         * Likewise for hash_multirange_extended.
         */
        if hash_extended_proc == F_HASH_MULTIRANGE_EXTENDED
            && !multirange_element_has_extended_hashing(typentry)
        {
            hash_extended_proc = InvalidOid;
        }

        /* Force update of proc finfo only if we're changing state */
        if (*typentry).hash_extended_proc != hash_extended_proc {
            (*typentry).hash_extended_proc_finfo.fn_oid = InvalidOid;
        }

        (*typentry).hash_extended_proc = hash_extended_proc;
        (*typentry).flags |= TCFLAGS_CHECKED_HASH_EXTENDED_PROC;
    }

    /*
     * Set up fmgr lookup info as requested
     *
     * Note: we tell fmgr the finfo structures live in CacheMemoryContext,
     * which is not quite right (they're really in the hash table's private
     * memory context) but this will do for our purposes.
     *
     * Note: the code above avoids invalidating the finfo structs unless the
     * referenced operator/function OID actually changes.  This is to prevent
     * unnecessary leakage of any subsidiary data attached to an finfo, since
     * that would cause session-lifespan memory leaks.
     */
    if (flags & TYPECACHE_EQ_OPR_FINFO) != 0
        && (*typentry).eq_opr_finfo.fn_oid == InvalidOid
        && (*typentry).eq_opr != InvalidOid
    {
        let eq_opr_func: Oid;

        eq_opr_func = get_opcode((*typentry).eq_opr);
        if eq_opr_func != InvalidOid {
            fmgr_info_cxt(
                eq_opr_func,
                &raw mut (*typentry).eq_opr_finfo,
                CacheMemoryContext as crate::utils::palloc::MemoryContext,
            );
        }
    }
    if (flags & TYPECACHE_CMP_PROC_FINFO) != 0
        && (*typentry).cmp_proc_finfo.fn_oid == InvalidOid
        && (*typentry).cmp_proc != InvalidOid
    {
        fmgr_info_cxt(
            (*typentry).cmp_proc,
            &raw mut (*typentry).cmp_proc_finfo,
            CacheMemoryContext as crate::utils::palloc::MemoryContext,
        );
    }
    if (flags & TYPECACHE_HASH_PROC_FINFO) != 0
        && (*typentry).hash_proc_finfo.fn_oid == InvalidOid
        && (*typentry).hash_proc != InvalidOid
    {
        fmgr_info_cxt(
            (*typentry).hash_proc,
            &raw mut (*typentry).hash_proc_finfo,
            CacheMemoryContext as crate::utils::palloc::MemoryContext,
        );
    }
    if (flags & TYPECACHE_HASH_EXTENDED_PROC_FINFO) != 0
        && (*typentry).hash_extended_proc_finfo.fn_oid == InvalidOid
        && (*typentry).hash_extended_proc != InvalidOid
    {
        fmgr_info_cxt(
            (*typentry).hash_extended_proc,
            &raw mut (*typentry).hash_extended_proc_finfo,
            CacheMemoryContext as crate::utils::palloc::MemoryContext,
        );
    }

    /*
     * If it's a composite type (row type), get tupdesc if requested
     */
    if (flags & TYPECACHE_TUPDESC) != 0
        && (*typentry).tupDesc.is_null()
        && (*typentry).typtype == TYPTYPE_COMPOSITE
    {
        load_typcache_tupdesc(typentry);
    }

    /*
     * If requested, get information about a range type
     *
     * This includes making sure that the basic info about the range element
     * type is up-to-date.
     */
    if (flags & TYPECACHE_RANGE_INFO) != 0 && (*typentry).typtype == TYPTYPE_RANGE {
        if (*typentry).rngelemtype.is_null() {
            load_rangetype_info(typentry);
        } else if ((*(*typentry).rngelemtype).flags & TCFLAGS_HAVE_PG_TYPE_DATA) == 0 {
            lookup_type_cache((*(*typentry).rngelemtype).type_id, 0);
        }
    }

    /*
     * If requested, get information about a multirange type
     */
    if (flags & TYPECACHE_MULTIRANGE_INFO) != 0
        && (*typentry).rngtype.is_null()
        && (*typentry).typtype == TYPTYPE_MULTIRANGE
    {
        load_multirangetype_info(typentry);
    }

    /*
     * If requested, get information about a domain type
     */
    if (flags & TYPECACHE_DOMAIN_BASE_INFO) != 0
        && (*typentry).domainBaseType == InvalidOid
        && (*typentry).typtype == TYPTYPE_DOMAIN
    {
        (*typentry).domainBaseTypmod = -1;
        (*typentry).domainBaseType =
            getBaseTypeAndTypmod(type_id, &raw mut (*typentry).domainBaseTypmod);
    }
    if (flags & TYPECACHE_DOMAIN_CONSTR_INFO) != 0
        && ((*typentry).flags & TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS) == 0
        && (*typentry).typtype == TYPTYPE_DOMAIN
    {
        load_domaintype_info(typentry);
    }

    INJECTION_POINT!("typecache-before-rel-type-cache-insert", null_mut());

    Assert!(in_progress_offset + 1 == in_progress_list_len);
    in_progress_list_len -= 1;

    insert_rel_type_cache_if_needed(typentry);

    typentry
}

/*
 * load_typcache_tupdesc --- helper routine to set up composite type's tupDesc
 */
unsafe fn load_typcache_tupdesc(typentry: *mut TypeCacheEntry) {
    let rel: Relation;

    if !OidIsValid((*typentry).typrelid) {
        /* should not happen */
        elog!(
            ERROR,
            "invalid typrelid for composite type {}",
            (*typentry).type_id
        );
    }
    rel = relation_open((*typentry).typrelid, AccessShareLock);
    Assert!((*(*rel).rd_rel).reltype == (*typentry).type_id);

    /*
     * Link to the tupdesc and increment its refcount (we assert it's a
     * refcounted descriptor).  We don't use IncrTupleDescRefCount() for this,
     * because the reference mustn't be entered in the current resource owner;
     * it can outlive the current query.
     */
    (*typentry).tupDesc = RelationGetDescr(rel);

    Assert!((*(*typentry).tupDesc).tdrefcount > 0);
    (*(*typentry).tupDesc).tdrefcount += 1;

    /*
     * In future, we could take some pains to not change tupDesc_identifier if
     * the tupdesc didn't really change; but for now it's not worth it.
     */
    tupledesc_id_counter += 1;
    (*typentry).tupDesc_identifier = tupledesc_id_counter;

    relation_close(rel, AccessShareLock);
}

/*
 * load_rangetype_info --- helper routine to set up range type information
 */
unsafe fn load_rangetype_info(typentry: *mut TypeCacheEntry) {
    let pg_range: Form_pg_range;
    let tup: HeapTuple;
    let subtypeOid: Oid;
    let opclassOid: Oid;
    let canonicalOid: Oid;
    let subdiffOid: Oid;
    let opfamilyOid: Oid;
    let opcintype: Oid;
    let cmpFnOid: Oid;

    /* get information from pg_range */
    tup = SearchSysCache1(RANGETYPE, ObjectIdGetDatum((*typentry).type_id));
    /* should not fail, since we already checked typtype ... */
    if !HeapTupleIsValid(tup) {
        elog!(
            ERROR,
            "cache lookup failed for range type {}",
            (*typentry).type_id
        );
    }
    pg_range = GETSTRUCT(tup) as Form_pg_range;

    subtypeOid = (*pg_range).rngsubtype;
    (*typentry).rng_collation = (*pg_range).rngcollation;
    opclassOid = (*pg_range).rngsubopc;
    canonicalOid = (*pg_range).rngcanonical;
    subdiffOid = (*pg_range).rngsubdiff;

    ReleaseSysCache(tup);

    /* get opclass properties and look up the comparison function */
    opfamilyOid = get_opclass_family(opclassOid);
    opcintype = get_opclass_input_type(opclassOid);
    (*typentry).rng_opfamily = opfamilyOid;

    cmpFnOid = get_opfamily_proc(opfamilyOid, opcintype, opcintype, BTORDER_PROC);
    if !RegProcedureIsValid(cmpFnOid) {
        elog!(
            ERROR,
            "missing support function {}({},{}) in opfamily {}",
            BTORDER_PROC,
            opcintype,
            opcintype,
            opfamilyOid
        );
    }

    /* set up cached fmgrinfo structs */
    fmgr_info_cxt(
        cmpFnOid,
        &raw mut (*typentry).rng_cmp_proc_finfo,
        CacheMemoryContext as crate::utils::palloc::MemoryContext,
    );
    if OidIsValid(canonicalOid) {
        fmgr_info_cxt(
            canonicalOid,
            &raw mut (*typentry).rng_canonical_finfo,
            CacheMemoryContext as crate::utils::palloc::MemoryContext,
        );
    }
    if OidIsValid(subdiffOid) {
        fmgr_info_cxt(
            subdiffOid,
            &raw mut (*typentry).rng_subdiff_finfo,
            CacheMemoryContext as crate::utils::palloc::MemoryContext,
        );
    }

    /* Lastly, set up link to the element type --- this marks data valid */
    (*typentry).rngelemtype = lookup_type_cache(subtypeOid, 0);
}

/*
 * load_multirangetype_info --- helper routine to set up multirange type
 * information
 */
unsafe fn load_multirangetype_info(typentry: *mut TypeCacheEntry) {
    let rangetypeOid: Oid;

    rangetypeOid = get_multirange_range((*typentry).type_id);
    if !OidIsValid(rangetypeOid) {
        elog!(
            ERROR,
            "cache lookup failed for multirange type {}",
            (*typentry).type_id
        );
    }

    (*typentry).rngtype = lookup_type_cache(rangetypeOid, TYPECACHE_RANGE_INFO);
}

/*
 * load_domaintype_info --- helper routine to set up domain constraint info
 *
 * Note: we assume we're called in a relatively short-lived context, so it's
 * okay to leak data into the current context while scanning pg_constraint.
 * We build the new DomainConstraintCache data in a context underneath
 * CurrentMemoryContext, and reparent it under CacheMemoryContext when
 * complete.
 */
unsafe fn load_domaintype_info(typentry: *mut TypeCacheEntry) {
    let mut typeOid: Oid = (*typentry).type_id;
    let mut dcc: *mut DomainConstraintCache;
    let mut notNull: bool = false;
    let mut ccons: *mut *mut DomainConstraintState;
    let mut cconslen: c_int;
    let conRel: Relation;
    let mut oldcxt: MemoryContext;

    /*
     * If we're here, any existing constraint info is stale, so release it.
     * For safety, be sure to null the link before trying to delete the data.
     */
    if !(*typentry).domainData.is_null() {
        dcc = (*typentry).domainData;
        (*typentry).domainData = null_mut();
        decr_dcc_refcount(dcc);
    }

    /*
     * We try to optimize the common case of no domain constraints, so don't
     * create the dcc object and context until we find a constraint.  Likewise
     * for the temp sorting array.
     */
    dcc = null_mut();
    ccons = null_mut();
    cconslen = 0;

    /*
     * Scan pg_constraint for relevant constraints.  We want to find
     * constraints for not just this domain, but any ancestor domains, so the
     * outer loop crawls up the domain stack.
     */
    conRel = table_open(ConstraintRelationId, AccessShareLock);

    loop {
        let tup: HeapTuple;
        let mut conTup: HeapTuple;
        let typTup: Form_pg_type;
        let mut nccons: c_int = 0;
        let mut key: [ScanKeyData; 1] = [core::mem::zeroed()];
        let scan: SysScanDesc;

        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
        if !HeapTupleIsValid(tup) {
            elog!(ERROR, "cache lookup failed for type {}", typeOid);
        }
        typTup = GETSTRUCT(tup) as Form_pg_type;

        if (*typTup).typtype != TYPTYPE_DOMAIN {
            /* Not a domain, so done */
            ReleaseSysCache(tup);
            break;
        }

        /* Test for NOT NULL Constraint */
        if (*typTup).typnotnull {
            notNull = true;
        }

        /* Look for CHECK Constraints on this domain */
        ScanKeyInit(
            &raw mut key[0],
            Anum_pg_constraint_contypid,
            BTEqualStrategyNumber as int16,
            F_OIDEQ,
            ObjectIdGetDatum(typeOid),
        );

        scan = systable_beginscan(
            conRel,
            ConstraintTypidIndexId,
            true,
            null_mut(),
            1,
            key.as_mut_ptr(),
        );

        loop {
            conTup = systable_getnext(scan);
            if !HeapTupleIsValid(conTup) {
                break;
            }
            let c = GETSTRUCT(conTup) as Form_pg_constraint;
            let val: Datum;
            let mut isNull: bool = false;
            let constring: *mut c_char;
            let mut check_expr: *mut Expr;
            let r: *mut DomainConstraintState;

            /* Ignore non-CHECK constraints */
            if (*c).contype != CONSTRAINT_CHECK {
                continue;
            }

            /* Not expecting conbin to be NULL, but we'll test for it anyway */
            val = fastgetattr(
                conTup,
                Anum_pg_constraint_conbin,
                (*conRel).rd_att,
                &raw mut isNull,
            );
            if isNull {
                elog!(
                    ERROR,
                    "domain \"{}\" constraint \"{}\" has NULL conbin",
                    cstr_to_string(NameStr(&(*typTup).typname)),
                    cstr_to_string(NameStr(&(*c).conname))
                );
            }

            /* Convert conbin to C string in caller context */
            constring = TextDatumGetCString(val);

            /* Create the DomainConstraintCache object and context if needed */
            if dcc.is_null() {
                let cxt: MemoryContext;

                cxt = AllocSetContextCreate_SmallSizes(
                    CurrentMemoryContext,
                    c"Domain constraints".as_ptr(),
                );
                dcc = MemoryContextAlloc(cxt, core::mem::size_of::<DomainConstraintCache>())
                    as *mut DomainConstraintCache;
                (*dcc).constraints = NIL;
                (*dcc).dccContext = cxt;
                (*dcc).dccRefCount = 0;
            }

            /* Create node trees in DomainConstraintCache's context */
            oldcxt = MemoryContextSwitchTo((*dcc).dccContext);

            check_expr = stringToNode(constring) as *mut Expr;

            /*
             * Plan the expression, since ExecInitExpr will expect that.
             *
             * Note: caching the result of expression_planner() is not very
             * good practice.  Ideally we'd use a CachedExpression here so
             * that we would react promptly to, eg, changes in inlined
             * functions.  However, because we don't support mutable domain
             * CHECK constraints, it's not really clear that it's worth the
             * extra overhead to do that.
             */
            check_expr = expression_planner(check_expr);

            r = makeNode!(DomainConstraintState, T_DomainConstraintState);
            (*r).constrainttype = DOM_CONSTRAINT_CHECK;
            (*r).name = pstrdup(NameStr(&(*c).conname));
            (*r).check_expr = check_expr;
            (*r).check_exprstate = null_mut();

            MemoryContextSwitchTo(oldcxt);

            /* Accumulate constraints in an array, for sorting below */
            if ccons.is_null() {
                cconslen = 8;
                ccons = palloc((cconslen as Size) * core::mem::size_of::<*mut DomainConstraintState>())
                    as *mut *mut DomainConstraintState;
            } else if nccons >= cconslen {
                cconslen *= 2;
                ccons = repalloc(
                    ccons as *mut c_void,
                    (cconslen as Size) * core::mem::size_of::<*mut DomainConstraintState>(),
                ) as *mut *mut DomainConstraintState;
            }
            *ccons.add(nccons as usize) = r;
            nccons += 1;
        }

        systable_endscan(scan);

        if nccons > 0 {
            /*
             * Sort the items for this domain, so that CHECKs are applied in a
             * deterministic order.
             */
            if nccons > 1 {
                qsort_dcs(ccons, nccons as Size);
            }

            /*
             * Now attach them to the overall list.  Use lcons() here because
             * constraints of parent domains should be applied earlier.
             */
            oldcxt = MemoryContextSwitchTo((*dcc).dccContext);
            while nccons > 0 {
                nccons -= 1;
                (*dcc).constraints = lcons(*ccons.add(nccons as usize) as *mut c_void, (*dcc).constraints);
            }
            MemoryContextSwitchTo(oldcxt);
        }

        /* loop to next domain in stack */
        typeOid = (*typTup).typbasetype;
        ReleaseSysCache(tup);
    }

    table_close(conRel, AccessShareLock);

    /*
     * Only need to add one NOT NULL check regardless of how many domains in
     * the stack request it.
     */
    if notNull {
        let r: *mut DomainConstraintState;

        /* Create the DomainConstraintCache object and context if needed */
        if dcc.is_null() {
            let cxt: MemoryContext;

            cxt = AllocSetContextCreate_SmallSizes(
                CurrentMemoryContext,
                c"Domain constraints".as_ptr(),
            );
            dcc = MemoryContextAlloc(cxt, core::mem::size_of::<DomainConstraintCache>())
                as *mut DomainConstraintCache;
            (*dcc).constraints = NIL;
            (*dcc).dccContext = cxt;
            (*dcc).dccRefCount = 0;
        }

        /* Create node trees in DomainConstraintCache's context */
        oldcxt = MemoryContextSwitchTo((*dcc).dccContext);

        r = makeNode!(DomainConstraintState, T_DomainConstraintState);

        (*r).constrainttype = DOM_CONSTRAINT_NOTNULL;
        (*r).name = pstrdup(c"NOT NULL".as_ptr());
        (*r).check_expr = null_mut();
        (*r).check_exprstate = null_mut();

        /* lcons to apply the nullness check FIRST */
        (*dcc).constraints = lcons(r as *mut c_void, (*dcc).constraints);

        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * If we made a constraint object, move it into CacheMemoryContext and
     * attach it to the typcache entry.
     */
    if !dcc.is_null() {
        MemoryContextSetParent((*dcc).dccContext as crate::utils::mmgr::memnodes::MemoryContext, CacheMemoryContext);
        (*typentry).domainData = dcc;
        (*dcc).dccRefCount += 1; /* count the typcache's reference */
    }

    /* Either way, the typcache entry's domain data is now valid. */
    (*typentry).flags |= TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS;
}

/*
 * qsort comparator to sort DomainConstraintState pointers by name
 */
unsafe fn dcs_cmp(a: *const c_void, b: *const c_void) -> c_int {
    let ca = a as *const *const DomainConstraintState;
    let cb = b as *const *const DomainConstraintState;

    strcmp((**ca).name, (**cb).name)
}

/* qsort over an array of DomainConstraintState pointers using dcs_cmp */
unsafe fn qsort_dcs(base: *mut *mut DomainConstraintState, nmemb: Size) {
    let slice = core::slice::from_raw_parts_mut(base, nmemb as usize);
    slice.sort_by(|x, y| {
        match dcs_cmp(
            x as *const *mut DomainConstraintState as *const c_void,
            y as *const *mut DomainConstraintState as *const c_void,
        ) {
            n if n < 0 => core::cmp::Ordering::Less,
            0 => core::cmp::Ordering::Equal,
            _ => core::cmp::Ordering::Greater,
        }
    });
}

/*
 * decr_dcc_refcount --- decrement a DomainConstraintCache's refcount,
 * and free it if no references remain
 */
unsafe fn decr_dcc_refcount(dcc: *mut DomainConstraintCache) {
    Assert!((*dcc).dccRefCount > 0);
    (*dcc).dccRefCount -= 1;
    if (*dcc).dccRefCount <= 0 {
        MemoryContextDelete((*dcc).dccContext);
    }
}

/*
 * Context reset/delete callback for a DomainConstraintRef
 */
unsafe extern "C" fn dccref_deletion_callback(arg: *mut c_void) {
    let r#ref = arg as *mut DomainConstraintRef;
    let dcc = (*r#ref).dcc;

    /* Paranoia --- be sure link is nulled before trying to release */
    if !dcc.is_null() {
        (*r#ref).constraints = NIL;
        (*r#ref).dcc = null_mut();
        decr_dcc_refcount(dcc);
    }
}

/*
 * prep_domain_constraints --- prepare domain constraints for execution
 *
 * The expression trees stored in the DomainConstraintCache's list are
 * converted to executable expression state trees stored in execctx.
 */
unsafe fn prep_domain_constraints(constraints: *mut List, execctx: MemoryContext) -> *mut List {
    let mut result: *mut List = NIL;
    let oldcxt: MemoryContext;
    let mut lc: *mut ListCell;

    oldcxt = MemoryContextSwitchTo(execctx);

    crate::foreach!(lc, constraints, {
        let r = crate::nodes::pg_list::lfirst(current_cell!(lc)) as *mut DomainConstraintState;
        let newr: *mut DomainConstraintState;

        newr = makeNode!(DomainConstraintState, T_DomainConstraintState);
        (*newr).constrainttype = (*r).constrainttype;
        (*newr).name = (*r).name;
        (*newr).check_expr = (*r).check_expr;
        (*newr).check_exprstate = ExecInitExpr((*r).check_expr, null_mut());

        result = lappend(result, newr as *mut c_void);
    });

    MemoryContextSwitchTo(oldcxt);

    result
}

/*
 * InitDomainConstraintRef --- initialize a DomainConstraintRef struct
 *
 * Caller must tell us the MemoryContext in which the DomainConstraintRef
 * lives.  The ref will be cleaned up when that context is reset/deleted.
 *
 * Caller must also tell us whether it wants check_exprstate fields to be
 * computed in the DomainConstraintState nodes attached to this ref.
 * If it doesn't, we need not make a copy of the DomainConstraintState list.
 */
pub unsafe fn InitDomainConstraintRef(
    type_id: Oid,
    r#ref: *mut DomainConstraintRef,
    refctx: MemoryContext,
    need_exprstate: bool,
) {
    /* Look up the typcache entry --- we assume it survives indefinitely */
    (*r#ref).tcache = lookup_type_cache(type_id, TYPECACHE_DOMAIN_CONSTR_INFO);
    (*r#ref).need_exprstate = need_exprstate;
    /* For safety, establish the callback before acquiring a refcount */
    (*r#ref).refctx = refctx;
    (*r#ref).dcc = null_mut();
    (*r#ref).callback.func = Some(dccref_deletion_callback);
    (*r#ref).callback.arg = r#ref as *mut c_void;
    crate::utils::mmgr::mcxt::MemoryContextRegisterResetCallback(refctx as crate::utils::mmgr::memnodes::MemoryContext, &raw mut (*r#ref).callback);
    /* Acquire refcount if there are constraints, and set up exported list */
    if !(*(*r#ref).tcache).domainData.is_null() {
        (*r#ref).dcc = (*(*r#ref).tcache).domainData;
        (*(*r#ref).dcc).dccRefCount += 1;
        if (*r#ref).need_exprstate {
            (*r#ref).constraints =
                prep_domain_constraints((*(*r#ref).dcc).constraints, (*r#ref).refctx);
        } else {
            (*r#ref).constraints = (*(*r#ref).dcc).constraints;
        }
    } else {
        (*r#ref).constraints = NIL;
    }
}

/*
 * UpdateDomainConstraintRef --- recheck validity of domain constraint info
 *
 * If the domain's constraint set changed, ref->constraints is updated to
 * point at a new list of cached constraints.
 *
 * In the normal case where nothing happened to the domain, this is cheap
 * enough that it's reasonable (and expected) to check before *each* use
 * of the constraint info.
 */
pub unsafe fn UpdateDomainConstraintRef(r#ref: *mut DomainConstraintRef) {
    let typentry: *mut TypeCacheEntry = (*r#ref).tcache;

    /* Make sure typcache entry's data is up to date */
    if ((*typentry).flags & TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS) == 0
        && (*typentry).typtype == TYPTYPE_DOMAIN
    {
        load_domaintype_info(typentry);
    }

    /* Transfer to ref object if there's new info, adjusting refcounts */
    if (*r#ref).dcc != (*typentry).domainData {
        /* Paranoia --- be sure link is nulled before trying to release */
        let mut dcc: *mut DomainConstraintCache = (*r#ref).dcc;

        if !dcc.is_null() {
            /*
             * Note: we just leak the previous list of executable domain
             * constraints.  Alternatively, we could keep those in a child
             * context of ref->refctx and free that context at this point.
             * However, in practice this code path will be taken so seldom
             * that the extra bookkeeping for a child context doesn't seem
             * worthwhile; we'll just allow a leak for the lifespan of refctx.
             */
            (*r#ref).constraints = NIL;
            (*r#ref).dcc = null_mut();
            decr_dcc_refcount(dcc);
        }
        dcc = (*typentry).domainData;
        if !dcc.is_null() {
            (*r#ref).dcc = dcc;
            (*dcc).dccRefCount += 1;
            if (*r#ref).need_exprstate {
                (*r#ref).constraints = prep_domain_constraints((*dcc).constraints, (*r#ref).refctx);
            } else {
                (*r#ref).constraints = (*dcc).constraints;
            }
        }
    }
}

/*
 * DomainHasConstraints --- utility routine to check if a domain has constraints
 *
 * This is defined to return false, not fail, if type is not a domain.
 */
pub unsafe fn DomainHasConstraints(type_id: Oid) -> bool {
    let typentry: *mut TypeCacheEntry;

    /*
     * Note: a side effect is to cause the typcache's domain data to become
     * valid.  This is fine since we'll likely need it soon if there is any.
     */
    typentry = lookup_type_cache(type_id, TYPECACHE_DOMAIN_CONSTR_INFO);

    !(*typentry).domainData.is_null()
}

/*
 * array_element_has_equality and friends are helper routines to check
 * whether we should believe that array_eq and related functions will work
 * on the given array type or composite type.
 *
 * The logic above may call these repeatedly on the same type entry, so we
 * make use of the typentry->flags field to cache the results once known.
 * Also, we assume that we'll probably want all these facts about the type
 * if we want any, so we cache them all using only one lookup of the
 * component datatype(s).
 */

unsafe fn array_element_has_equality(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_ELEM_PROPERTIES) == 0 {
        cache_array_element_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_ELEM_EQUALITY) != 0
}

unsafe fn array_element_has_compare(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_ELEM_PROPERTIES) == 0 {
        cache_array_element_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_ELEM_COMPARE) != 0
}

unsafe fn array_element_has_hashing(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_ELEM_PROPERTIES) == 0 {
        cache_array_element_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_ELEM_HASHING) != 0
}

unsafe fn array_element_has_extended_hashing(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_ELEM_PROPERTIES) == 0 {
        cache_array_element_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_ELEM_EXTENDED_HASHING) != 0
}

unsafe fn cache_array_element_properties(typentry: *mut TypeCacheEntry) {
    let elem_type: Oid = get_base_element_type((*typentry).type_id);

    if OidIsValid(elem_type) {
        let elementry: *mut TypeCacheEntry;

        elementry = lookup_type_cache(
            elem_type,
            TYPECACHE_EQ_OPR
                | TYPECACHE_CMP_PROC
                | TYPECACHE_HASH_PROC
                | TYPECACHE_HASH_EXTENDED_PROC,
        );
        if OidIsValid((*elementry).eq_opr) {
            (*typentry).flags |= TCFLAGS_HAVE_ELEM_EQUALITY;
        }
        if OidIsValid((*elementry).cmp_proc) {
            (*typentry).flags |= TCFLAGS_HAVE_ELEM_COMPARE;
        }
        if OidIsValid((*elementry).hash_proc) {
            (*typentry).flags |= TCFLAGS_HAVE_ELEM_HASHING;
        }
        if OidIsValid((*elementry).hash_extended_proc) {
            (*typentry).flags |= TCFLAGS_HAVE_ELEM_EXTENDED_HASHING;
        }
    }
    (*typentry).flags |= TCFLAGS_CHECKED_ELEM_PROPERTIES;
}

/*
 * Likewise, some helper functions for composite types.
 */

unsafe fn record_fields_have_equality(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_FIELD_PROPERTIES) == 0 {
        cache_record_field_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_FIELD_EQUALITY) != 0
}

unsafe fn record_fields_have_compare(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_FIELD_PROPERTIES) == 0 {
        cache_record_field_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_FIELD_COMPARE) != 0
}

unsafe fn record_fields_have_hashing(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_FIELD_PROPERTIES) == 0 {
        cache_record_field_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_FIELD_HASHING) != 0
}

unsafe fn record_fields_have_extended_hashing(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_FIELD_PROPERTIES) == 0 {
        cache_record_field_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_FIELD_EXTENDED_HASHING) != 0
}

unsafe fn cache_record_field_properties(typentry: *mut TypeCacheEntry) {
    /*
     * For type RECORD, we can't really tell what will work, since we don't
     * have access here to the specific anonymous type.  Just assume that
     * equality and comparison will (we may get a failure at runtime).  We
     * could also claim that hashing works, but then if code that has the
     * option between a comparison-based (sort-based) and a hash-based plan
     * chooses hashing, stuff could fail that would otherwise work if it chose
     * a comparison-based plan.  In practice more types support comparison
     * than hashing.
     */
    if (*typentry).type_id == RECORDOID {
        (*typentry).flags |= TCFLAGS_HAVE_FIELD_EQUALITY | TCFLAGS_HAVE_FIELD_COMPARE;
    } else if (*typentry).typtype == TYPTYPE_COMPOSITE {
        let tupdesc: TupleDesc;
        let mut newflags: c_int;
        let mut i: c_int;

        /* Fetch composite type's tupdesc if we don't have it already */
        if (*typentry).tupDesc.is_null() {
            load_typcache_tupdesc(typentry);
        }
        tupdesc = (*typentry).tupDesc;

        /* Must bump the refcount while we do additional catalog lookups */
        IncrTupleDescRefCount(tupdesc);

        /* Have each property if all non-dropped fields have the property */
        newflags = TCFLAGS_HAVE_FIELD_EQUALITY
            | TCFLAGS_HAVE_FIELD_COMPARE
            | TCFLAGS_HAVE_FIELD_HASHING
            | TCFLAGS_HAVE_FIELD_EXTENDED_HASHING;
        i = 0;
        while i < (*tupdesc).natts {
            let fieldentry: *mut TypeCacheEntry;
            let attr = TupleDescAttr(tupdesc, i);

            if (*attr).attisdropped {
                i += 1;
                continue;
            }

            fieldentry = lookup_type_cache(
                (*attr).atttypid,
                TYPECACHE_EQ_OPR
                    | TYPECACHE_CMP_PROC
                    | TYPECACHE_HASH_PROC
                    | TYPECACHE_HASH_EXTENDED_PROC,
            );
            if !OidIsValid((*fieldentry).eq_opr) {
                newflags &= !TCFLAGS_HAVE_FIELD_EQUALITY;
            }
            if !OidIsValid((*fieldentry).cmp_proc) {
                newflags &= !TCFLAGS_HAVE_FIELD_COMPARE;
            }
            if !OidIsValid((*fieldentry).hash_proc) {
                newflags &= !TCFLAGS_HAVE_FIELD_HASHING;
            }
            if !OidIsValid((*fieldentry).hash_extended_proc) {
                newflags &= !TCFLAGS_HAVE_FIELD_EXTENDED_HASHING;
            }

            /* We can drop out of the loop once we disprove all bits */
            if newflags == 0 {
                break;
            }
            i += 1;
        }
        (*typentry).flags |= newflags;

        DecrTupleDescRefCount(tupdesc);
    } else if (*typentry).typtype == TYPTYPE_DOMAIN {
        /* If it's domain over composite, copy base type's properties */
        let baseentry: *mut TypeCacheEntry;

        /* load up basetype info if we didn't already */
        if (*typentry).domainBaseType == InvalidOid {
            (*typentry).domainBaseTypmod = -1;
            (*typentry).domainBaseType =
                getBaseTypeAndTypmod((*typentry).type_id, &raw mut (*typentry).domainBaseTypmod);
        }
        baseentry = lookup_type_cache(
            (*typentry).domainBaseType,
            TYPECACHE_EQ_OPR
                | TYPECACHE_CMP_PROC
                | TYPECACHE_HASH_PROC
                | TYPECACHE_HASH_EXTENDED_PROC,
        );
        if (*baseentry).typtype == TYPTYPE_COMPOSITE {
            (*typentry).flags |= TCFLAGS_DOMAIN_BASE_IS_COMPOSITE;
            (*typentry).flags |= (*baseentry).flags
                & (TCFLAGS_HAVE_FIELD_EQUALITY
                    | TCFLAGS_HAVE_FIELD_COMPARE
                    | TCFLAGS_HAVE_FIELD_HASHING
                    | TCFLAGS_HAVE_FIELD_EXTENDED_HASHING);
        }
    }
    (*typentry).flags |= TCFLAGS_CHECKED_FIELD_PROPERTIES;
}

/*
 * Likewise, some helper functions for range and multirange types.
 *
 * We can borrow the flag bits for array element properties to use for range
 * element properties, since those flag bits otherwise have no use in a
 * range or multirange type's typcache entry.
 */

unsafe fn range_element_has_hashing(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_ELEM_PROPERTIES) == 0 {
        cache_range_element_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_ELEM_HASHING) != 0
}

unsafe fn range_element_has_extended_hashing(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_ELEM_PROPERTIES) == 0 {
        cache_range_element_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_ELEM_EXTENDED_HASHING) != 0
}

unsafe fn cache_range_element_properties(typentry: *mut TypeCacheEntry) {
    /* load up subtype link if we didn't already */
    if (*typentry).rngelemtype.is_null() && (*typentry).typtype == TYPTYPE_RANGE {
        load_rangetype_info(typentry);
    }

    if !(*typentry).rngelemtype.is_null() {
        let elementry: *mut TypeCacheEntry;

        /* might need to calculate subtype's hash function properties */
        elementry = lookup_type_cache(
            (*(*typentry).rngelemtype).type_id,
            TYPECACHE_HASH_PROC | TYPECACHE_HASH_EXTENDED_PROC,
        );
        if OidIsValid((*elementry).hash_proc) {
            (*typentry).flags |= TCFLAGS_HAVE_ELEM_HASHING;
        }
        if OidIsValid((*elementry).hash_extended_proc) {
            (*typentry).flags |= TCFLAGS_HAVE_ELEM_EXTENDED_HASHING;
        }
    }
    (*typentry).flags |= TCFLAGS_CHECKED_ELEM_PROPERTIES;
}

unsafe fn multirange_element_has_hashing(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_ELEM_PROPERTIES) == 0 {
        cache_multirange_element_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_ELEM_HASHING) != 0
}

unsafe fn multirange_element_has_extended_hashing(typentry: *mut TypeCacheEntry) -> bool {
    if ((*typentry).flags & TCFLAGS_CHECKED_ELEM_PROPERTIES) == 0 {
        cache_multirange_element_properties(typentry);
    }
    ((*typentry).flags & TCFLAGS_HAVE_ELEM_EXTENDED_HASHING) != 0
}

unsafe fn cache_multirange_element_properties(typentry: *mut TypeCacheEntry) {
    /* load up range link if we didn't already */
    if (*typentry).rngtype.is_null() && (*typentry).typtype == TYPTYPE_MULTIRANGE {
        load_multirangetype_info(typentry);
    }

    if !(*typentry).rngtype.is_null() && !(*(*typentry).rngtype).rngelemtype.is_null() {
        let elementry: *mut TypeCacheEntry;

        /* might need to calculate subtype's hash function properties */
        elementry = lookup_type_cache(
            (*(*(*typentry).rngtype).rngelemtype).type_id,
            TYPECACHE_HASH_PROC | TYPECACHE_HASH_EXTENDED_PROC,
        );
        if OidIsValid((*elementry).hash_proc) {
            (*typentry).flags |= TCFLAGS_HAVE_ELEM_HASHING;
        }
        if OidIsValid((*elementry).hash_extended_proc) {
            (*typentry).flags |= TCFLAGS_HAVE_ELEM_EXTENDED_HASHING;
        }
    }
    (*typentry).flags |= TCFLAGS_CHECKED_ELEM_PROPERTIES;
}

/*
 * Make sure that RecordCacheArray and RecordIdentifierArray are large enough
 * to store 'typmod'.
 */
unsafe fn ensure_record_cache_typmod_slot_exists(typmod: int32) {
    if RecordCacheArray.is_null() {
        RecordCacheArray = MemoryContextAllocZero(
            CacheMemoryContext as crate::utils::palloc::MemoryContext,
            64 * core::mem::size_of::<RecordCacheArrayEntry>(),
        ) as *mut RecordCacheArrayEntry;
        RecordCacheArrayLen = 64;
    }

    if typmod >= RecordCacheArrayLen {
        let newlen: int32 = pg_nextpower2_32((typmod + 1) as uint32) as int32;

        RecordCacheArray =
            repalloc0_array_RecordCacheArrayEntry(RecordCacheArray, RecordCacheArrayLen, newlen);
        RecordCacheArrayLen = newlen;
    }
}

/*
 * lookup_rowtype_tupdesc_internal --- internal routine to lookup a rowtype
 *
 * Same API as lookup_rowtype_tupdesc_noerror, but the returned tupdesc
 * hasn't had its refcount bumped.
 */
unsafe fn lookup_rowtype_tupdesc_internal(type_id: Oid, typmod: int32, noError: bool) -> TupleDesc {
    if type_id != RECORDOID {
        /*
         * It's a named composite type, so use the regular typcache.
         */
        let typentry: *mut TypeCacheEntry;

        typentry = lookup_type_cache(type_id, TYPECACHE_TUPDESC);
        if (*typentry).tupDesc.is_null() && !noError {
            ereport!(
                ERROR,
                errmsg!(
                    "type {} is not composite",
                    cstr_to_string(format_type_be(type_id))
                )
            );
        }
        (*typentry).tupDesc
    } else {
        /*
         * It's a transient record type, so look in our record-type table.
         */
        if typmod >= 0 {
            /* It is already in our local cache? */
            if typmod < RecordCacheArrayLen
                && !(*RecordCacheArray.add(typmod as usize)).tupdesc.is_null()
            {
                return (*RecordCacheArray.add(typmod as usize)).tupdesc;
            }

            /* Are we attached to a shared record typmod registry? */
            if !(*CurrentSession).shared_typmod_registry.is_null() {
                let entry: *mut SharedTypmodTableEntry;

                /* Try to find it in the shared typmod index. */
                entry = dshash_find(
                    (*CurrentSession).shared_typmod_table,
                    &typmod as *const int32 as *const c_void,
                    false,
                ) as *mut SharedTypmodTableEntry;
                if !entry.is_null() {
                    let tupdesc: TupleDesc;

                    tupdesc =
                        dsa_get_address((*CurrentSession).area, (*entry).shared_tupdesc) as TupleDesc;
                    Assert!(typmod == (*tupdesc).tdtypmod);

                    /* We may need to extend the local RecordCacheArray. */
                    ensure_record_cache_typmod_slot_exists(typmod);

                    /*
                     * Our local array can now point directly to the TupleDesc
                     * in shared memory, which is non-reference-counted.
                     */
                    (*RecordCacheArray.add(typmod as usize)).tupdesc = tupdesc;
                    Assert!((*tupdesc).tdrefcount == -1);

                    /*
                     * We don't share tupdesc identifiers across processes, so
                     * assign one locally.
                     */
                    tupledesc_id_counter += 1;
                    (*RecordCacheArray.add(typmod as usize)).id = tupledesc_id_counter;

                    dshash_release_lock(
                        (*CurrentSession).shared_typmod_table,
                        entry as *mut c_void,
                    );

                    return (*RecordCacheArray.add(typmod as usize)).tupdesc;
                }
            }
        }

        if !noError {
            ereport!(
                ERROR,
                errmsg!("record type has not been registered")
            );
        }
        null_mut()
    }
}

/*
 * lookup_rowtype_tupdesc
 *
 * Given a typeid/typmod that should describe a known composite type,
 * return the tuple descriptor for the type.  Will ereport on failure.
 * (Use ereport because this is reachable with user-specified OIDs,
 * for example from record_in().)
 *
 * Note: on success, we increment the refcount of the returned TupleDesc,
 * and log the reference in CurrentResourceOwner.  Caller must call
 * ReleaseTupleDesc when done using the tupdesc.  (There are some
 * cases in which the returned tupdesc is not refcounted, in which
 * case PinTupleDesc/ReleaseTupleDesc are no-ops; but in these cases
 * the tupdesc is guaranteed to live till process exit.)
 */
#[no_mangle]
pub unsafe fn lookup_rowtype_tupdesc(type_id: Oid, typmod: int32) -> TupleDesc {
    let tupDesc: TupleDesc;

    tupDesc = lookup_rowtype_tupdesc_internal(type_id, typmod, false);
    PinTupleDesc(tupDesc);
    tupDesc
}

/*
 * lookup_rowtype_tupdesc_noerror
 *
 * As above, but if the type is not a known composite type and noError
 * is true, returns NULL instead of ereport'ing.  (Note that if a bogus
 * type_id is passed, you'll get an ereport anyway.)
 */
pub unsafe fn lookup_rowtype_tupdesc_noerror(type_id: Oid, typmod: int32, noError: bool) -> TupleDesc {
    let tupDesc: TupleDesc;

    tupDesc = lookup_rowtype_tupdesc_internal(type_id, typmod, noError);
    if !tupDesc.is_null() {
        PinTupleDesc(tupDesc);
    }
    tupDesc
}

/*
 * lookup_rowtype_tupdesc_copy
 *
 * Like lookup_rowtype_tupdesc(), but the returned TupleDesc has been
 * copied into the CurrentMemoryContext and is not reference-counted.
 */
pub unsafe fn lookup_rowtype_tupdesc_copy(type_id: Oid, typmod: int32) -> TupleDesc {
    let tmp: TupleDesc;

    tmp = lookup_rowtype_tupdesc_internal(type_id, typmod, false);
    CreateTupleDescCopyConstr(tmp)
}

/*
 * lookup_rowtype_tupdesc_domain
 *
 * Same as lookup_rowtype_tupdesc_noerror(), except that the type can also be
 * a domain over a named composite type; so this is effectively equivalent to
 * lookup_rowtype_tupdesc_noerror(getBaseType(type_id), typmod, noError)
 * except for being a tad faster.
 *
 * Note: the reason we don't fold the look-through-domain behavior into plain
 * lookup_rowtype_tupdesc() is that we want callers to know they might be
 * dealing with a domain.  Otherwise they might construct a tuple that should
 * be of the domain type, but not apply domain constraints.
 */
pub unsafe fn lookup_rowtype_tupdesc_domain(type_id: Oid, typmod: int32, noError: bool) -> TupleDesc {
    let tupDesc: TupleDesc;

    if type_id != RECORDOID {
        /*
         * Check for domain or named composite type.  We might as well load
         * whichever data is needed.
         */
        let typentry: *mut TypeCacheEntry;

        typentry = lookup_type_cache(type_id, TYPECACHE_TUPDESC | TYPECACHE_DOMAIN_BASE_INFO);
        if (*typentry).typtype == TYPTYPE_DOMAIN {
            return lookup_rowtype_tupdesc_noerror(
                (*typentry).domainBaseType,
                (*typentry).domainBaseTypmod,
                noError,
            );
        }
        if (*typentry).tupDesc.is_null() && !noError {
            ereport!(
                ERROR,
                errmsg!(
                    "type {} is not composite",
                    cstr_to_string(format_type_be(type_id))
                )
            );
        }
        tupDesc = (*typentry).tupDesc;
    } else {
        tupDesc = lookup_rowtype_tupdesc_internal(type_id, typmod, noError);
    }
    if !tupDesc.is_null() {
        PinTupleDesc(tupDesc);
    }
    tupDesc
}

/*
 * Hash function for the hash table of RecordCacheEntry.
 */
unsafe extern "C" fn record_type_typmod_hash(data: *const c_void, _size: Size) -> uint32 {
    let entry = data as *mut RecordCacheEntry;

    hashRowType((*entry).tupdesc)
}

/*
 * Match function for the hash table of RecordCacheEntry.
 */
unsafe extern "C" fn record_type_typmod_compare(
    a: *const c_void,
    b: *const c_void,
    _size: Size,
) -> c_int {
    let left = a as *mut RecordCacheEntry;
    let right = b as *mut RecordCacheEntry;

    if equalRowTypes((*left).tupdesc, (*right).tupdesc) {
        0
    } else {
        1
    }
}

/*
 * assign_record_type_typmod
 *
 * Given a tuple descriptor for a RECORD type, find or create a cache entry
 * for the type, and set the tupdesc's tdtypmod field to a value that will
 * identify this cache entry to lookup_rowtype_tupdesc.
 */
pub unsafe fn assign_record_type_typmod(tupDesc: TupleDesc) {
    let mut recentry: *mut RecordCacheEntry;
    let entDesc: TupleDesc;
    let mut found: bool = false;
    let oldcxt: MemoryContext;

    Assert!((*tupDesc).tdtypeid == RECORDOID);

    if RecordCacheHash.is_null() {
        /* First time through: initialize the hash table */
        let mut ctl: HASHCTL = core::mem::zeroed();

        ctl.keysize = core::mem::size_of::<TupleDesc>(); /* just the pointer */
        ctl.entrysize = core::mem::size_of::<RecordCacheEntry>();
        ctl.hash = Some(record_type_typmod_hash);
        ctl.r#match = Some(record_type_typmod_compare);
        RecordCacheHash = hash_create(
            c"Record information cache".as_ptr(),
            64,
            &ctl,
            HASH_ELEM | HASH_FUNCTION | HASH_COMPARE,
        );

        /* Also make sure CacheMemoryContext exists */
        if CacheMemoryContext.is_null() {
            CreateCacheMemoryContext();
        }
    }

    /*
     * Find a hashtable entry for this tuple descriptor. We don't use
     * HASH_ENTER yet, because if it's missing, we need to make sure that all
     * the allocations succeed before we create the new entry.
     */
    recentry = hash_search(
        RecordCacheHash,
        &tupDesc as *const TupleDesc as *const c_void,
        HASH_FIND,
        &mut found,
    ) as *mut RecordCacheEntry;
    if found && !(*recentry).tupdesc.is_null() {
        (*tupDesc).tdtypmod = (*(*recentry).tupdesc).tdtypmod;
        return;
    }

    /* Not present, so need to manufacture an entry */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as crate::utils::palloc::MemoryContext);

    /* Look in the SharedRecordTypmodRegistry, if attached */
    entDesc = find_or_make_matching_shared_tupledesc(tupDesc);
    if entDesc.is_null() {
        /*
         * Make sure we have room before we CreateTupleDescCopy() or advance
         * NextRecordTypmod.
         */
        ensure_record_cache_typmod_slot_exists(NextRecordTypmod);

        /* Reference-counted local cache only. */
        let entDesc = CreateTupleDescCopy(tupDesc);
        (*entDesc).tdrefcount = 1;
        (*entDesc).tdtypmod = NextRecordTypmod;
        NextRecordTypmod += 1;

        (*RecordCacheArray.add((*entDesc).tdtypmod as usize)).tupdesc = entDesc;

        /* Assign a unique tupdesc identifier, too. */
        tupledesc_id_counter += 1;
        (*RecordCacheArray.add((*entDesc).tdtypmod as usize)).id = tupledesc_id_counter;

        /* Fully initialized; create the hash table entry */
        recentry = hash_search(
            RecordCacheHash,
            &tupDesc as *const TupleDesc as *const c_void,
            HASH_ENTER,
            null_mut(),
        ) as *mut RecordCacheEntry;
        (*recentry).tupdesc = entDesc;

        /* Update the caller's tuple descriptor. */
        (*tupDesc).tdtypmod = (*entDesc).tdtypmod;

        MemoryContextSwitchTo(oldcxt);
        return;
    } else {
        ensure_record_cache_typmod_slot_exists((*entDesc).tdtypmod);
    }

    (*RecordCacheArray.add((*entDesc).tdtypmod as usize)).tupdesc = entDesc;

    /* Assign a unique tupdesc identifier, too. */
    tupledesc_id_counter += 1;
    (*RecordCacheArray.add((*entDesc).tdtypmod as usize)).id = tupledesc_id_counter;

    /* Fully initialized; create the hash table entry */
    recentry = hash_search(
        RecordCacheHash,
        &tupDesc as *const TupleDesc as *const c_void,
        HASH_ENTER,
        null_mut(),
    ) as *mut RecordCacheEntry;
    (*recentry).tupdesc = entDesc;

    /* Update the caller's tuple descriptor. */
    (*tupDesc).tdtypmod = (*entDesc).tdtypmod;

    MemoryContextSwitchTo(oldcxt);
}

/*
 * assign_record_type_identifier
 *
 * Get an identifier, which will be unique over the lifespan of this backend
 * process, for the current tuple descriptor of the specified composite type.
 * For named composite types, the value is guaranteed to change if the type's
 * definition does.  For registered RECORD types, the value will not change
 * once assigned, since the registered type won't either.  If an anonymous
 * RECORD type is specified, we return a new identifier on each call.
 */
pub unsafe fn assign_record_type_identifier(type_id: Oid, typmod: int32) -> uint64 {
    if type_id != RECORDOID {
        /*
         * It's a named composite type, so use the regular typcache.
         */
        let typentry: *mut TypeCacheEntry;

        typentry = lookup_type_cache(type_id, TYPECACHE_TUPDESC);
        if (*typentry).tupDesc.is_null() {
            ereport!(
                ERROR,
                errmsg!(
                    "type {} is not composite",
                    cstr_to_string(format_type_be(type_id))
                )
            );
        }
        Assert!((*typentry).tupDesc_identifier != 0);
        (*typentry).tupDesc_identifier
    } else {
        /*
         * It's a transient record type, so look in our record-type table.
         */
        if typmod >= 0
            && typmod < RecordCacheArrayLen
            && !(*RecordCacheArray.add(typmod as usize)).tupdesc.is_null()
        {
            Assert!((*RecordCacheArray.add(typmod as usize)).id != 0);
            return (*RecordCacheArray.add(typmod as usize)).id;
        }

        /* For anonymous or unrecognized record type, generate a new ID */
        tupledesc_id_counter += 1;
        tupledesc_id_counter
    }
}

/*
 * Return the amount of shmem required to hold a SharedRecordTypmodRegistry.
 * This exists only to avoid exposing private innards of
 * SharedRecordTypmodRegistry in a header.
 */
pub unsafe fn SharedRecordTypmodRegistryEstimate() -> usize {
    core::mem::size_of::<SharedRecordTypmodRegistry>()
}

/*
 * Initialize 'registry' in a pre-existing shared memory region, which must be
 * maximally aligned and have space for SharedRecordTypmodRegistryEstimate()
 * bytes.
 *
 * 'area' will be used to allocate shared memory space as required for the
 * typemod registration.  The current process, expected to be a leader process
 * in a parallel query, will be attached automatically and its current record
 * types will be loaded into *registry.  While attached, all calls to
 * assign_record_type_typmod will use the shared registry.  Worker backends
 * will need to attach explicitly.
 *
 * Note that this function takes 'area' and 'segment' as arguments rather than
 * accessing them via CurrentSession, because they aren't installed there
 * until after this function runs.
 */
pub unsafe fn SharedRecordTypmodRegistryInit(
    registry: *mut SharedRecordTypmodRegistry,
    segment: *mut dsm_segment,
    area: *mut dsa_area,
) {
    let old_context: MemoryContext;
    let record_table: *mut dshash_table;
    let typmod_table: *mut dshash_table;
    let mut typmod: int32;

    Assert!(!IsParallelWorker());

    /* We can't already be attached to a shared registry. */
    Assert!((*CurrentSession).shared_typmod_registry.is_null());
    Assert!((*CurrentSession).shared_record_table.is_null());
    Assert!((*CurrentSession).shared_typmod_table.is_null());

    old_context = MemoryContextSwitchTo(TopMemoryContext as crate::utils::palloc::MemoryContext);

    /* Create the hash table of tuple descriptors indexed by themselves. */
    record_table = dshash_create(area, &srtr_record_table_params, area as *mut c_void);

    /* Create the hash table of tuple descriptors indexed by typmod. */
    typmod_table = dshash_create(area, &srtr_typmod_table_params, null_mut());

    MemoryContextSwitchTo(old_context);

    /* Initialize the SharedRecordTypmodRegistry. */
    (*registry).record_table_handle = dshash_get_hash_table_handle(record_table);
    (*registry).typmod_table_handle = dshash_get_hash_table_handle(typmod_table);
    pg_atomic_init_u32(&raw mut (*registry).next_typmod, NextRecordTypmod as uint32);

    /*
     * Copy all entries from this backend's private registry into the shared
     * registry.
     */
    typmod = 0;
    while typmod < NextRecordTypmod {
        let typmod_table_entry: *mut SharedTypmodTableEntry;
        let record_table_entry: *mut SharedRecordTableEntry;
        let mut record_table_key: SharedRecordTableKey = core::mem::zeroed();
        let shared_dp: dsa_pointer;
        let tupdesc: TupleDesc;
        let mut found: bool = false;

        tupdesc = (*RecordCacheArray.add(typmod as usize)).tupdesc;
        if tupdesc.is_null() {
            typmod += 1;
            continue;
        }

        /* Copy the TupleDesc into shared memory. */
        shared_dp = share_tupledesc(area, tupdesc, typmod as uint32);

        /* Insert into the typmod table. */
        typmod_table_entry = dshash_find_or_insert(
            typmod_table,
            &raw mut (*tupdesc).tdtypmod as *const c_void,
            &mut found,
        ) as *mut SharedTypmodTableEntry;
        if found {
            elog!(ERROR, "cannot create duplicate shared record typmod");
        }
        (*typmod_table_entry).typmod = (*tupdesc).tdtypmod as uint32;
        (*typmod_table_entry).shared_tupdesc = shared_dp;
        dshash_release_lock(typmod_table, typmod_table_entry as *mut c_void);

        /* Insert into the record table. */
        record_table_key.shared = false;
        record_table_key.u.local_tupdesc = tupdesc;
        record_table_entry = dshash_find_or_insert(
            record_table,
            &record_table_key as *const SharedRecordTableKey as *const c_void,
            &mut found,
        ) as *mut SharedRecordTableEntry;
        if !found {
            (*record_table_entry).key.shared = true;
            (*record_table_entry).key.u.shared_tupdesc = shared_dp;
        }
        dshash_release_lock(record_table, record_table_entry as *mut c_void);

        typmod += 1;
    }

    /*
     * Set up the global state that will tell assign_record_type_typmod and
     * lookup_rowtype_tupdesc_internal about the shared registry.
     */
    (*CurrentSession).shared_record_table = record_table;
    (*CurrentSession).shared_typmod_table = typmod_table;
    (*CurrentSession).shared_typmod_registry = registry as *mut core::ffi::c_void;

    /*
     * We install a detach hook in the leader, but only to handle cleanup on
     * failure during GetSessionDsmHandle().  Once GetSessionDsmHandle() pins
     * the memory, the leader process will use a shared registry until it
     * exits.
     */
    on_dsm_detach(segment, Some(shared_record_typmod_registry_detach), 0);
}

/*
 * Attach to 'registry', which must have been initialized already by another
 * backend.  Future calls to assign_record_type_typmod and
 * lookup_rowtype_tupdesc_internal will use the shared registry until the
 * current session is detached.
 */
pub unsafe fn SharedRecordTypmodRegistryAttach(registry: *mut SharedRecordTypmodRegistry) {
    let old_context: MemoryContext;
    let record_table: *mut dshash_table;
    let typmod_table: *mut dshash_table;

    Assert!(IsParallelWorker());

    /* We can't already be attached to a shared registry. */
    Assert!(!CurrentSession.is_null());
    Assert!(!(*CurrentSession).segment.is_null());
    Assert!(!(*CurrentSession).area.is_null());
    Assert!((*CurrentSession).shared_typmod_registry.is_null());
    Assert!((*CurrentSession).shared_record_table.is_null());
    Assert!((*CurrentSession).shared_typmod_table.is_null());

    /*
     * We can't already have typmods in our local cache, because they'd clash
     * with those imported by SharedRecordTypmodRegistryInit.  This should be
     * a freshly started parallel worker.  If we ever support worker
     * recycling, a worker would need to zap its local cache in between
     * servicing different queries, in order to be able to call this and
     * synchronize typmods with a new leader; but that's problematic because
     * we can't be very sure that record-typmod-related state hasn't escaped
     * to anywhere else in the process.
     */
    Assert!(NextRecordTypmod == 0);

    old_context = MemoryContextSwitchTo(TopMemoryContext as crate::utils::palloc::MemoryContext);

    /* Attach to the two hash tables. */
    record_table = dshash_attach(
        (*CurrentSession).area,
        &srtr_record_table_params,
        (*registry).record_table_handle,
        (*CurrentSession).area as *mut c_void,
    );
    typmod_table = dshash_attach(
        (*CurrentSession).area,
        &srtr_typmod_table_params,
        (*registry).typmod_table_handle,
        null_mut(),
    );

    MemoryContextSwitchTo(old_context);

    /*
     * Set up detach hook to run at worker exit.  Currently this is the same
     * as the leader's detach hook, but in future they might need to be
     * different.
     */
    on_dsm_detach(
        (*CurrentSession).segment,
        Some(shared_record_typmod_registry_detach),
        PointerGetDatum(registry as *const c_void),
    );

    /*
     * Set up the session state that will tell assign_record_type_typmod and
     * lookup_rowtype_tupdesc_internal about the shared registry.
     */
    (*CurrentSession).shared_typmod_registry = registry as *mut core::ffi::c_void;
    (*CurrentSession).shared_record_table = record_table;
    (*CurrentSession).shared_typmod_table = typmod_table;
}

/*
 * InvalidateCompositeTypeCacheEntry
 *		Invalidate particular TypeCacheEntry on Relcache inval callback
 *
 * Delete the cached tuple descriptor (if any) for the given composite
 * type, and reset whatever info we have cached about the composite type's
 * comparability.
 */
unsafe fn InvalidateCompositeTypeCacheEntry(typentry: *mut TypeCacheEntry) {
    let hadTupDescOrOpclass: bool;

    Assert!((*typentry).typtype == TYPTYPE_COMPOSITE && OidIsValid((*typentry).typrelid));

    hadTupDescOrOpclass =
        !(*typentry).tupDesc.is_null() || ((*typentry).flags & TCFLAGS_OPERATOR_FLAGS) != 0;

    /* Delete tupdesc if we have it */
    if !(*typentry).tupDesc.is_null() {
        /*
         * Release our refcount and free the tupdesc if none remain. We can't
         * use DecrTupleDescRefCount here because this reference is not logged
         * by the current resource owner.
         */
        Assert!((*(*typentry).tupDesc).tdrefcount > 0);
        (*(*typentry).tupDesc).tdrefcount -= 1;
        if (*(*typentry).tupDesc).tdrefcount == 0 {
            FreeTupleDesc((*typentry).tupDesc);
        }
        (*typentry).tupDesc = null_mut();

        /*
         * Also clear tupDesc_identifier, so that anyone watching it will
         * realize that the tupdesc has changed.
         */
        (*typentry).tupDesc_identifier = 0;
    }

    /* Reset equality/comparison/hashing validity information */
    (*typentry).flags &= !TCFLAGS_OPERATOR_FLAGS;

    /*
     * Call delete_rel_type_cache_if_needed() if we actually cleared
     * something.
     */
    if hadTupDescOrOpclass {
        delete_rel_type_cache_if_needed(typentry);
    }
}

/*
 * TypeCacheRelCallback
 *		Relcache inval callback function
 *
 * Delete the cached tuple descriptor (if any) for the given rel's composite
 * type, or for all composite types if relid == InvalidOid.  Also reset
 * whatever info we have cached about the composite type's comparability.
 *
 * This is called when a relcache invalidation event occurs for the given
 * relid.  We can't use syscache to find a type corresponding to the given
 * relation because the code can be called outside of transaction. Thus, we
 * use the RelIdToTypeIdCacheHash map to locate appropriate typcache entry.
 */
unsafe extern "C" fn TypeCacheRelCallback(_arg: Datum, relid: Oid) {
    let mut typentry: *mut TypeCacheEntry;

    /*
     * RelIdToTypeIdCacheHash and TypeCacheHash should exist, otherwise this
     * callback wouldn't be registered
     */
    if OidIsValid(relid) {
        let relentry: *mut RelIdToTypeIdCacheEntry;

        /*
         * Find an RelIdToTypeIdCacheHash entry, which should exist as soon as
         * corresponding typcache entry has something to clean.
         */
        relentry = hash_search(
            RelIdToTypeIdCacheHash,
            &relid as *const Oid as *const c_void,
            HASH_FIND,
            null_mut(),
        ) as *mut RelIdToTypeIdCacheEntry;

        if !relentry.is_null() {
            typentry = hash_search(
                TypeCacheHash,
                &raw const (*relentry).composite_typid as *const c_void,
                HASH_FIND,
                null_mut(),
            ) as *mut TypeCacheEntry;

            if !typentry.is_null() {
                Assert!((*typentry).typtype == TYPTYPE_COMPOSITE);
                Assert!(relid == (*typentry).typrelid);

                InvalidateCompositeTypeCacheEntry(typentry);
            }
        }

        /*
         * Visit all the domain types sequentially.  Typically, this shouldn't
         * affect performance since domain types are less tended to bloat.
         * Domain types are created manually, unlike composite types which are
         * automatically created for every temporary table.
         */
        typentry = firstDomainTypeEntry;
        while !typentry.is_null() {
            /*
             * If it's domain over composite, reset flags.  (We don't bother
             * trying to determine whether the specific base type needs a
             * reset.)  Note that if we haven't determined whether the base
             * type is composite, we don't need to reset anything.
             */
            if ((*typentry).flags & TCFLAGS_DOMAIN_BASE_IS_COMPOSITE) != 0 {
                (*typentry).flags &= !TCFLAGS_OPERATOR_FLAGS;
            }
            typentry = (*typentry).nextDomain;
        }
    } else {
        let mut status: HASH_SEQ_STATUS = core::mem::zeroed();

        /*
         * Relid is invalid. By convention, we need to reset all composite
         * types in cache. Also, we should reset flags for domain types, and
         * we loop over all entries in hash, so, do it in a single scan.
         */
        hash_seq_init(&mut status, TypeCacheHash);
        loop {
            typentry = hash_seq_search(&mut status) as *mut TypeCacheEntry;
            if typentry.is_null() {
                break;
            }
            if (*typentry).typtype == TYPTYPE_COMPOSITE {
                InvalidateCompositeTypeCacheEntry(typentry);
            } else if (*typentry).typtype == TYPTYPE_DOMAIN {
                /*
                 * If it's domain over composite, reset flags.  (We don't
                 * bother trying to determine whether the specific base type
                 * needs a reset.)  Note that if we haven't determined whether
                 * the base type is composite, we don't need to reset
                 * anything.
                 */
                if ((*typentry).flags & TCFLAGS_DOMAIN_BASE_IS_COMPOSITE) != 0 {
                    (*typentry).flags &= !TCFLAGS_OPERATOR_FLAGS;
                }
            }
        }
    }
}

/*
 * TypeCacheTypCallback
 *		Syscache inval callback function
 *
 * This is called when a syscache invalidation event occurs for any
 * pg_type row.  If we have information cached about that type, mark
 * it as needing to be reloaded.
 */
unsafe extern "C" fn TypeCacheTypCallback(_arg: Datum, _cacheid: c_int, hashvalue: uint32) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut typentry: *mut TypeCacheEntry;

    /* TypeCacheHash must exist, else this callback wouldn't be registered */

    /*
     * By convention, zero hash value is passed to the callback as a sign that
     * it's time to invalidate the whole cache. See sinval.c, inval.c and
     * InvalidateSystemCachesExtended().
     */
    if hashvalue == 0 {
        hash_seq_init(&mut status, TypeCacheHash);
    } else {
        hash_seq_init_with_hash_value(&mut status, TypeCacheHash, hashvalue);
    }

    loop {
        typentry = hash_seq_search(&mut status) as *mut TypeCacheEntry;
        if typentry.is_null() {
            break;
        }
        let hadPgTypeData: bool = ((*typentry).flags & TCFLAGS_HAVE_PG_TYPE_DATA) != 0;

        Assert!(hashvalue == 0 || (*typentry).type_id_hash == hashvalue);

        /*
         * Mark the data obtained directly from pg_type as invalid.  Also, if
         * it's a domain, typnotnull might've changed, so we'll need to
         * recalculate its constraints.
         */
        (*typentry).flags &=
            !(TCFLAGS_HAVE_PG_TYPE_DATA | TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS);

        /*
         * Call delete_rel_type_cache_if_needed() if we cleaned
         * TCFLAGS_HAVE_PG_TYPE_DATA flag previously.
         */
        if hadPgTypeData {
            delete_rel_type_cache_if_needed(typentry);
        }
    }
}

/*
 * TypeCacheOpcCallback
 *		Syscache inval callback function
 *
 * This is called when a syscache invalidation event occurs for any pg_opclass
 * row.  In principle we could probably just invalidate data dependent on the
 * particular opclass, but since updates on pg_opclass are rare in production
 * it doesn't seem worth a lot of complication: we just mark all cached data
 * invalid.
 *
 * Note that we don't bother watching for updates on pg_amop or pg_amproc.
 * This should be safe because ALTER OPERATOR FAMILY ADD/DROP OPERATOR/FUNCTION
 * is not allowed to be used to add/drop the primary operators and functions
 * of an opclass, only cross-type members of a family; and the latter sorts
 * of members are not going to get cached here.
 */
unsafe extern "C" fn TypeCacheOpcCallback(_arg: Datum, _cacheid: c_int, _hashvalue: uint32) {
    let mut status: HASH_SEQ_STATUS = core::mem::zeroed();
    let mut typentry: *mut TypeCacheEntry;

    /* TypeCacheHash must exist, else this callback wouldn't be registered */
    hash_seq_init(&mut status, TypeCacheHash);
    loop {
        typentry = hash_seq_search(&mut status) as *mut TypeCacheEntry;
        if typentry.is_null() {
            break;
        }
        let hadOpclass: bool = ((*typentry).flags & TCFLAGS_OPERATOR_FLAGS) != 0;

        /* Reset equality/comparison/hashing validity information */
        (*typentry).flags &= !TCFLAGS_OPERATOR_FLAGS;

        /*
         * Call delete_rel_type_cache_if_needed() if we actually cleared some
         * of TCFLAGS_OPERATOR_FLAGS.
         */
        if hadOpclass {
            delete_rel_type_cache_if_needed(typentry);
        }
    }
}

/*
 * TypeCacheConstrCallback
 *		Syscache inval callback function
 *
 * This is called when a syscache invalidation event occurs for any
 * pg_constraint row.  We flush information about domain constraints
 * when this happens.
 *
 * It's slightly annoying that we can't tell whether the inval event was for
 * a domain constraint record or not; there's usually more update traffic
 * for table constraints than domain constraints, so we'll do a lot of
 * useless flushes.  Still, this is better than the old no-caching-at-all
 * approach to domain constraints.
 */
unsafe extern "C" fn TypeCacheConstrCallback(_arg: Datum, _cacheid: c_int, _hashvalue: uint32) {
    let mut typentry: *mut TypeCacheEntry;

    /*
     * Because this is called very frequently, and typically very few of the
     * typcache entries are for domains, we don't use hash_seq_search here.
     * Instead we thread all the domain-type entries together so that we can
     * visit them cheaply.
     */
    typentry = firstDomainTypeEntry;
    while !typentry.is_null() {
        /* Reset domain constraint validity information */
        (*typentry).flags &= !TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS;
        typentry = (*typentry).nextDomain;
    }
}

/*
 * Check if given OID is part of the subset that's sortable by comparisons
 */
#[inline]
unsafe fn enum_known_sorted(enumdata: *mut TypeCacheEnumData, arg: Oid) -> bool {
    let offset: Oid;

    if arg < (*enumdata).bitmap_base {
        return false;
    }
    offset = arg - (*enumdata).bitmap_base;
    if offset > i32::MAX as Oid {
        return false;
    }
    bms_is_member(offset as c_int, (*enumdata).sorted_values)
}

/*
 * compare_values_of_enum
 *		Compare two members of an enum type.
 *		Return <0, 0, or >0 according as arg1 <, =, or > arg2.
 *
 * Note: currently, the enumData cache is refreshed only if we are asked
 * to compare an enum value that is not already in the cache.  This is okay
 * because there is no support for re-ordering existing values, so comparisons
 * of previously cached values will return the right answer even if other
 * values have been added since we last loaded the cache.
 *
 * Note: the enum logic has a special-case rule about even-numbered versus
 * odd-numbered OIDs, but we take no account of that rule here; this
 * routine shouldn't even get called when that rule applies.
 */
pub unsafe fn compare_values_of_enum(tcache: *mut TypeCacheEntry, arg1: Oid, arg2: Oid) -> c_int {
    let mut enumdata: *mut TypeCacheEnumData;
    let mut item1: *mut EnumItem;
    let mut item2: *mut EnumItem;

    /*
     * Equal OIDs are certainly equal --- this case was probably handled by
     * our caller, but we may as well check.
     */
    if arg1 == arg2 {
        return 0;
    }

    /* Load up the cache if first time through */
    if (*tcache).enumData.is_null() {
        load_enum_cache_data(tcache);
    }
    enumdata = (*tcache).enumData;

    /*
     * If both OIDs are known-sorted, we can just compare them directly.
     */
    if enum_known_sorted(enumdata, arg1) && enum_known_sorted(enumdata, arg2) {
        if arg1 < arg2 {
            return -1;
        } else {
            return 1;
        }
    }

    /*
     * Slow path: we have to identify their actual sort-order positions.
     */
    item1 = find_enumitem(enumdata, arg1);
    item2 = find_enumitem(enumdata, arg2);

    if item1.is_null() || item2.is_null() {
        /*
         * We couldn't find one or both values.  That means the enum has
         * changed under us, so re-initialize the cache and try again. We
         * don't bother retrying the known-sorted case in this path.
         */
        load_enum_cache_data(tcache);
        enumdata = (*tcache).enumData;

        item1 = find_enumitem(enumdata, arg1);
        item2 = find_enumitem(enumdata, arg2);

        /*
         * If we still can't find the values, complain: we must have corrupt
         * data.
         */
        if item1.is_null() {
            elog!(
                ERROR,
                "enum value {} not found in cache for enum {}",
                arg1,
                cstr_to_string(format_type_be((*tcache).type_id))
            );
        }
        if item2.is_null() {
            elog!(
                ERROR,
                "enum value {} not found in cache for enum {}",
                arg2,
                cstr_to_string(format_type_be((*tcache).type_id))
            );
        }
    }

    if (*item1).sort_order < (*item2).sort_order {
        -1
    } else if (*item1).sort_order > (*item2).sort_order {
        1
    } else {
        0
    }
}

/*
 * Load (or re-load) the enumData member of the typcache entry.
 */
unsafe fn load_enum_cache_data(tcache: *mut TypeCacheEntry) {
    let enumdata: *mut TypeCacheEnumData;
    let enum_rel: Relation;
    let enum_scan: SysScanDesc;
    let mut enum_tuple: HeapTuple;
    let mut skey: ScanKeyData = core::mem::zeroed();
    let mut items: *mut EnumItem;
    let mut numitems: c_int;
    let mut maxitems: c_int;
    let mut bitmap_base: Oid;
    let mut bitmap: *mut Bitmapset;
    let oldcxt: MemoryContext;
    let mut bm_size: c_int;
    let mut start_pos: c_int;

    /* Check that this is actually an enum */
    if (*tcache).typtype != TYPTYPE_ENUM {
        ereport!(
            ERROR,
            errmsg!(
                "{} is not an enum",
                cstr_to_string(format_type_be((*tcache).type_id))
            )
        );
    }

    /*
     * Read all the information for members of the enum type.  We collect the
     * info in working memory in the caller's context, and then transfer it to
     * permanent memory in CacheMemoryContext.  This minimizes the risk of
     * leaking memory from CacheMemoryContext in the event of an error partway
     * through.
     */
    maxitems = 64;
    items = palloc(core::mem::size_of::<EnumItem>() * maxitems as usize) as *mut EnumItem;
    numitems = 0;

    /* Scan pg_enum for the members of the target enum type. */
    ScanKeyInit(
        &raw mut skey,
        Anum_pg_enum_enumtypid,
        BTEqualStrategyNumber as int16,
        F_OIDEQ,
        ObjectIdGetDatum((*tcache).type_id),
    );

    enum_rel = table_open(EnumRelationId, AccessShareLock);
    enum_scan = systable_beginscan(
        enum_rel,
        EnumTypIdLabelIndexId,
        true,
        null_mut(),
        1,
        &raw mut skey,
    );

    loop {
        enum_tuple = systable_getnext(enum_scan);
        if !HeapTupleIsValid(enum_tuple) {
            break;
        }
        let en = GETSTRUCT(enum_tuple) as Form_pg_enum;

        if numitems >= maxitems {
            maxitems *= 2;
            items = repalloc(
                items as *mut c_void,
                core::mem::size_of::<EnumItem>() * maxitems as usize,
            ) as *mut EnumItem;
        }
        (*items.add(numitems as usize)).enum_oid = (*en).oid;
        (*items.add(numitems as usize)).sort_order = (*en).enumsortorder;
        numitems += 1;
    }

    systable_endscan(enum_scan);
    table_close(enum_rel, AccessShareLock);

    /* Sort the items into OID order */
    qsort_enumitems(items, numitems as Size);

    /*
     * Here, we create a bitmap listing a subset of the enum's OIDs that are
     * known to be in order and can thus be compared with just OID comparison.
     *
     * The point of this is that the enum's initial OIDs were certainly in
     * order, so there is some subset that can be compared via OID comparison;
     * and we'd rather not do binary searches unnecessarily.
     *
     * This is somewhat heuristic, and might identify a subset of OIDs that
     * isn't exactly what the type started with.  That's okay as long as the
     * subset is correctly sorted.
     */
    bitmap_base = InvalidOid;
    bitmap = null_mut();
    bm_size = 1; /* only save sets of at least 2 OIDs */

    start_pos = 0;
    while start_pos < numitems - 1 {
        /*
         * Identify longest sorted subsequence starting at start_pos
         */
        let mut this_bitmap: *mut Bitmapset = bms_make_singleton(0);
        let mut this_bm_size: c_int = 1;
        let start_oid: Oid = (*items.add(start_pos as usize)).enum_oid;
        let mut prev_order: float4 = (*items.add(start_pos as usize)).sort_order;
        let mut i: c_int;

        i = start_pos + 1;
        while i < numitems {
            let offset: Oid;

            offset = (*items.add(i as usize)).enum_oid - start_oid;
            /* quit if bitmap would be too large; cutoff is arbitrary */
            if offset >= 8192 {
                break;
            }
            /* include the item if it's in-order */
            if (*items.add(i as usize)).sort_order > prev_order {
                prev_order = (*items.add(i as usize)).sort_order;
                this_bitmap = bms_add_member(this_bitmap, offset as c_int);
                this_bm_size += 1;
            }
            i += 1;
        }

        /* Remember it if larger than previous best */
        if this_bm_size > bm_size {
            bms_free(bitmap);
            bitmap_base = start_oid;
            bitmap = this_bitmap;
            bm_size = this_bm_size;
        } else {
            bms_free(this_bitmap);
        }

        /*
         * Done if it's not possible to find a longer sequence in the rest of
         * the list.  In typical cases this will happen on the first
         * iteration, which is why we create the bitmaps on the fly instead of
         * doing a second pass over the list.
         */
        if bm_size >= (numitems - start_pos - 1) {
            break;
        }
        start_pos += 1;
    }

    /* OK, copy the data into CacheMemoryContext */
    oldcxt = MemoryContextSwitchTo(CacheMemoryContext as crate::utils::palloc::MemoryContext);
    enumdata = palloc(
        core::mem::offset_of!(TypeCacheEnumData, enum_values)
            + numitems as usize * core::mem::size_of::<EnumItem>(),
    ) as *mut TypeCacheEnumData;
    (*enumdata).bitmap_base = bitmap_base;
    (*enumdata).sorted_values = bms_copy(bitmap);
    (*enumdata).num_values = numitems;
    memcpy(
        (*enumdata).enum_values.as_mut_ptr() as *mut c_void,
        items as *const c_void,
        numitems as usize * core::mem::size_of::<EnumItem>(),
    );
    MemoryContextSwitchTo(oldcxt);

    pfree(items as *mut c_void);
    bms_free(bitmap);

    /* And link the finished cache struct into the typcache */
    if !(*tcache).enumData.is_null() {
        pfree((*tcache).enumData as *mut c_void);
    }
    (*tcache).enumData = enumdata;
}

/*
 * Locate the EnumItem with the given OID, if present
 */
unsafe fn find_enumitem(enumdata: *mut TypeCacheEnumData, arg: Oid) -> *mut EnumItem {
    /* On some versions of Solaris, bsearch of zero items dumps core */
    if (*enumdata).num_values <= 0 {
        return null_mut();
    }

    let base = (*enumdata).enum_values.as_mut_ptr();
    let n = (*enumdata).num_values as usize;
    let slice = core::slice::from_raw_parts(base, n);
    match slice.binary_search_by(|probe| {
        match enum_oid_cmp(
            &arg as *const Oid as *const c_void, /* unused shape */
            probe as *const EnumItem as *const c_void,
        ) {
            // binary_search_by expects ordering of probe vs target; emulate
            // bsearch(srch, ...) by comparing probe.enum_oid to arg.
            _ => {
                if probe.enum_oid < arg {
                    core::cmp::Ordering::Less
                } else if probe.enum_oid > arg {
                    core::cmp::Ordering::Greater
                } else {
                    core::cmp::Ordering::Equal
                }
            }
        }
    }) {
        Ok(idx) => base.add(idx),
        Err(_) => null_mut(),
    }
}

/*
 * qsort comparison function for OID-ordered EnumItems
 */
unsafe fn enum_oid_cmp(left: *const c_void, right: *const c_void) -> c_int {
    let l = left as *const EnumItem;
    let r = right as *const EnumItem;

    pg_cmp_u32((*l).enum_oid, (*r).enum_oid)
}

/* qsort over an array of EnumItems using enum_oid_cmp */
unsafe fn qsort_enumitems(base: *mut EnumItem, nmemb: Size) {
    let slice = core::slice::from_raw_parts_mut(base, nmemb as usize);
    slice.sort_by(|x, y| {
        match enum_oid_cmp(
            x as *const EnumItem as *const c_void,
            y as *const EnumItem as *const c_void,
        ) {
            n if n < 0 => core::cmp::Ordering::Less,
            0 => core::cmp::Ordering::Equal,
            _ => core::cmp::Ordering::Greater,
        }
    });
}

/*
 * Copy 'tupdesc' into newly allocated shared memory in 'area', set its typmod
 * to the given value and return a dsa_pointer.
 */
unsafe fn share_tupledesc(area: *mut dsa_area, tupdesc: TupleDesc, typmod: uint32) -> dsa_pointer {
    let shared_dp: dsa_pointer;
    let shared: TupleDesc;

    shared_dp = dsa_allocate(area, TupleDescSize(tupdesc));
    shared = dsa_get_address(area, shared_dp) as TupleDesc;
    TupleDescCopy(shared, tupdesc);
    (*shared).tdtypmod = typmod as int32;

    shared_dp
}

/*
 * If we are attached to a SharedRecordTypmodRegistry, use it to find or
 * create a shared TupleDesc that matches 'tupdesc'.  Otherwise return NULL.
 * Tuple descriptors returned by this function are not reference counted, and
 * will exist at least as long as the current backend remained attached to the
 * current session.
 */
unsafe fn find_or_make_matching_shared_tupledesc(tupdesc: TupleDesc) -> TupleDesc {
    let result: TupleDesc;
    let mut key: SharedRecordTableKey = core::mem::zeroed();
    let mut record_table_entry: *mut SharedRecordTableEntry;
    let typmod_table_entry: *mut SharedTypmodTableEntry;
    let shared_dp: dsa_pointer;
    let mut found: bool = false;
    let typmod: uint32;

    /* If not even attached, nothing to do. */
    if (*CurrentSession).shared_typmod_registry.is_null() {
        return null_mut();
    }

    /* Try to find a matching tuple descriptor in the record table. */
    key.shared = false;
    key.u.local_tupdesc = tupdesc;
    record_table_entry = dshash_find(
        (*CurrentSession).shared_record_table,
        &key as *const SharedRecordTableKey as *const c_void,
        false,
    ) as *mut SharedRecordTableEntry;
    if !record_table_entry.is_null() {
        Assert!((*record_table_entry).key.shared);
        dshash_release_lock(
            (*CurrentSession).shared_record_table,
            record_table_entry as *mut c_void,
        );
        result = dsa_get_address(
            (*CurrentSession).area,
            (*record_table_entry).key.u.shared_tupdesc,
        ) as TupleDesc;
        Assert!((*result).tdrefcount == -1);

        return result;
    }

    /* Allocate a new typmod number.  This will be wasted if we error out. */
    typmod = pg_atomic_fetch_add_u32(
        &raw mut (*((*CurrentSession).shared_typmod_registry as *mut SharedRecordTypmodRegistry)).next_typmod,
        1,
    );

    /* Copy the TupleDesc into shared memory. */
    shared_dp = share_tupledesc((*CurrentSession).area, tupdesc, typmod);

    /*
     * Create an entry in the typmod table so that others will understand this
     * typmod number.
     *
     * Note: the C source wraps the dshash_find_or_insert below in PG_TRY/
     * PG_CATCH to dsa_free(shared_dp) on error.  The Rust port omits the
     * setjmp-style guard; on error the shared allocation is leaked.
     */
    typmod_table_entry = dshash_find_or_insert(
        (*CurrentSession).shared_typmod_table,
        &typmod as *const uint32 as *const c_void,
        &mut found,
    ) as *mut SharedTypmodTableEntry;
    if found {
        dsa_free((*CurrentSession).area, shared_dp);
        elog!(ERROR, "cannot create duplicate shared record typmod");
    }
    (*typmod_table_entry).typmod = typmod;
    (*typmod_table_entry).shared_tupdesc = shared_dp;
    dshash_release_lock(
        (*CurrentSession).shared_typmod_table,
        typmod_table_entry as *mut c_void,
    );

    /*
     * Finally create an entry in the record table so others with matching
     * tuple descriptors can reuse the typmod.
     */
    record_table_entry = dshash_find_or_insert(
        (*CurrentSession).shared_record_table,
        &key as *const SharedRecordTableKey as *const c_void,
        &mut found,
    ) as *mut SharedRecordTableEntry;
    if found {
        /*
         * Someone concurrently inserted a matching tuple descriptor since the
         * first time we checked.  Use that one instead.
         */
        dshash_release_lock(
            (*CurrentSession).shared_record_table,
            record_table_entry as *mut c_void,
        );

        /* Might as well free up the space used by the one we created. */
        found = dshash_delete_key(
            (*CurrentSession).shared_typmod_table,
            &typmod as *const uint32 as *const c_void,
        );
        Assert!(found);
        dsa_free((*CurrentSession).area, shared_dp);

        /* Return the one we found. */
        Assert!((*record_table_entry).key.shared);
        result = dsa_get_address(
            (*CurrentSession).area,
            (*record_table_entry).key.u.shared_tupdesc,
        ) as TupleDesc;
        Assert!((*result).tdrefcount == -1);

        return result;
    }

    /* Store it and return it. */
    (*record_table_entry).key.shared = true;
    (*record_table_entry).key.u.shared_tupdesc = shared_dp;
    dshash_release_lock(
        (*CurrentSession).shared_record_table,
        record_table_entry as *mut c_void,
    );
    result = dsa_get_address((*CurrentSession).area, shared_dp) as TupleDesc;
    Assert!((*result).tdrefcount == -1);

    result
}

/*
 * On-DSM-detach hook to forget about the current shared record typmod
 * infrastructure.  This is currently used by both leader and workers.
 */
unsafe extern "C" fn shared_record_typmod_registry_detach(_segment: *mut dsm_segment, _datum: Datum) {
    /* Be cautious here: maybe we didn't finish initializing. */
    if !(*CurrentSession).shared_record_table.is_null() {
        dshash_detach((*CurrentSession).shared_record_table);
        (*CurrentSession).shared_record_table = null_mut();
    }
    if !(*CurrentSession).shared_typmod_table.is_null() {
        dshash_detach((*CurrentSession).shared_typmod_table);
        (*CurrentSession).shared_typmod_table = null_mut();
    }
    (*CurrentSession).shared_typmod_registry = null_mut();
}

/*
 * Insert RelIdToTypeIdCacheHash entry if needed.
 */
unsafe fn insert_rel_type_cache_if_needed(typentry: *mut TypeCacheEntry) {
    /* Immediately quit for non-composite types */
    if (*typentry).typtype != TYPTYPE_COMPOSITE {
        return;
    }

    /* typrelid should be given for composite types */
    Assert!(OidIsValid((*typentry).typrelid));

    /*
     * Insert a RelIdToTypeIdCacheHash entry if the typentry have any
     * information indicating it should be here.
     */
    if ((*typentry).flags & TCFLAGS_HAVE_PG_TYPE_DATA) != 0
        || ((*typentry).flags & TCFLAGS_OPERATOR_FLAGS) != 0
        || !(*typentry).tupDesc.is_null()
    {
        let relentry: *mut RelIdToTypeIdCacheEntry;
        let mut found: bool = false;

        relentry = hash_search(
            RelIdToTypeIdCacheHash,
            &raw const (*typentry).typrelid as *const c_void,
            HASH_ENTER,
            &mut found,
        ) as *mut RelIdToTypeIdCacheEntry;
        (*relentry).relid = (*typentry).typrelid;
        (*relentry).composite_typid = (*typentry).type_id;
    }
}

/*
 * Delete entry RelIdToTypeIdCacheHash if needed after resetting of the
 * TCFLAGS_HAVE_PG_TYPE_DATA flag, or any of TCFLAGS_OPERATOR_FLAGS,
 * or tupDesc.
 */
unsafe fn delete_rel_type_cache_if_needed(typentry: *mut TypeCacheEntry) {
    #[cfg(debug_assertions)]
    let is_in_progress: bool = {
        let mut found_in_progress = false;
        let mut i: c_int = 0;
        while i < in_progress_list_len {
            if *in_progress_list.add(i as usize) == (*typentry).type_id {
                found_in_progress = true;
                break;
            }
            i += 1;
        }
        found_in_progress
    };

    /* Immediately quit for non-composite types */
    if (*typentry).typtype != TYPTYPE_COMPOSITE {
        return;
    }

    /* typrelid should be given for composite types */
    Assert!(OidIsValid((*typentry).typrelid));

    /*
     * Delete a RelIdToTypeIdCacheHash entry if the typentry doesn't have any
     * information indicating entry should be still there.
     */
    if ((*typentry).flags & TCFLAGS_HAVE_PG_TYPE_DATA) == 0
        && ((*typentry).flags & TCFLAGS_OPERATOR_FLAGS) == 0
        && (*typentry).tupDesc.is_null()
    {
        let mut found: bool = false;

        hash_search(
            RelIdToTypeIdCacheHash,
            &raw const (*typentry).typrelid as *const c_void,
            HASH_REMOVE,
            &mut found,
        );
        #[cfg(debug_assertions)]
        Assert!(found || is_in_progress);
        let _ = found;
    } else {
        #[cfg(debug_assertions)]
        {
            /*
             * In assert-enabled builds otherwise check for RelIdToTypeIdCacheHash
             * entry if it should exist.
             */
            let mut found: bool = false;

            if !is_in_progress {
                hash_search(
                    RelIdToTypeIdCacheHash,
                    &raw const (*typentry).typrelid as *const c_void,
                    HASH_FIND,
                    &mut found,
                );
                Assert!(found);
            }
        }
    }
}

/*
 * Add possibly missing RelIdToTypeId entries related to TypeCacheHash
 * entries, marked as in-progress by lookup_type_cache().  It may happen
 * in case of an error or interruption during the lookup_type_cache() call.
 */
unsafe fn finalize_in_progress_typentries() {
    let mut i: c_int;

    i = 0;
    while i < in_progress_list_len {
        let typentry: *mut TypeCacheEntry;

        typentry = hash_search(
            TypeCacheHash,
            in_progress_list.add(i as usize) as *const c_void,
            HASH_FIND,
            null_mut(),
        ) as *mut TypeCacheEntry;
        if !typentry.is_null() {
            insert_rel_type_cache_if_needed(typentry);
        }
        i += 1;
    }

    in_progress_list_len = 0;
}

pub unsafe fn AtEOXact_TypeCache() {
    finalize_in_progress_typentries();
}

pub unsafe fn AtEOSubXact_TypeCache() {
    finalize_in_progress_typentries();
}

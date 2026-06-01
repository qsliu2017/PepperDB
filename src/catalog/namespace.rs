//! Translation of postgres/src/backend/catalog/namespace.c
//!
//! Code to support accessing and searching namespaces (schema search path).
//!
//! This is separate from pg_namespace.rs, which contains routines that
//! directly manipulate the pg_namespace system catalog.  This module
//! provides routines associated with defining a "namespace search path"
//! and implementing search-path-controlled searches.
//!
//! #include mapping:
//!   postgres.h                -> crate::prelude::*
//!   access/htup_details.h     -> crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT}
//!   catalog/namespace.h       -> this file (types + pub fns)
//!   catalog/pg_authid.h       -> crate::catalog::pg_authid::{Form_pg_authid, FormData_pg_authid}
//!   catalog/pg_class.h        -> crate::catalog::pg_class::{Form_pg_class, RELPERSISTENCE_*}
//!   catalog/pg_collation.h    -> crate::catalog::pg_collation::Form_pg_collation
//!   catalog/pg_conversion.h   -> crate::catalog::pg_conversion::Form_pg_conversion
//!   catalog/pg_namespace.h    -> crate::catalog::pg_namespace::Form_pg_namespace
//!   catalog/pg_opclass.h      -> crate::catalog::pg_opclass::Form_pg_opclass
//!   catalog/pg_operator.h     -> crate::catalog::pg_operator::Form_pg_operator
//!   catalog/pg_opfamily.h     -> crate::catalog::pg_opfamily::Form_pg_opfamily
//!   catalog/pg_proc.h         -> crate::catalog::pg_proc::Form_pg_proc
//!   catalog/pg_statistic_ext.h-> crate::catalog::pg_statistic_ext::Form_pg_statistic_ext
//!   catalog/pg_ts_config.h    -> crate::catalog::pg_ts_config::Form_pg_ts_config
//!   catalog/pg_ts_dict.h      -> crate::catalog::pg_ts_dict::Form_pg_ts_dict
//!   catalog/pg_ts_parser.h    -> crate::catalog::pg_ts_parser::Form_pg_ts_parser
//!   catalog/pg_ts_template.h  -> crate::catalog::pg_ts_template::Form_pg_ts_template
//!   catalog/pg_type.h         -> crate::catalog::pg_type::Form_pg_type
//!   miscadmin.h               -> GetUserId, IsBootstrapProcessingMode, MyDatabaseId stubs
//!   storage/lockdefs.h        -> crate::storage::lockdefs::LOCKMODE
//!   utils/lsyscache.h         -> crate::utils::cache::lsyscache::get_namespace_name (REAL)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use core::ffi::CStr;

use crate::{PG_GETARG_OID, PG_RETURN_BOOL, PG_RETURN_NULL, PG_RETURN_OID};
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::catalog::catalog_oids::{
    DatabaseRelationId, NamespaceRelationId, RelationRelationId,
};
use crate::catalog::pg_authid::Form_pg_authid;
use crate::catalog::pg_class::{
    Form_pg_class, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
};
use crate::nodes::pg_list::{List, ListCell};
use crate::utils::fmgr::FunctionCallInfoBaseData;
use crate::catalog::pg_collation::Form_pg_collation;
use crate::catalog::pg_conversion::Form_pg_conversion;
use crate::catalog::pg_namespace::Form_pg_namespace;
use crate::catalog::pg_opclass::Form_pg_opclass;
use crate::catalog::pg_operator::Form_pg_operator;
use crate::catalog::pg_opfamily::Form_pg_opfamily;
use crate::catalog::pg_proc::Form_pg_proc;
use crate::catalog::pg_statistic_ext::Form_pg_statistic_ext;
use crate::catalog::pg_ts_config::Form_pg_ts_config;
use crate::catalog::pg_ts_dict::Form_pg_ts_dict;
use crate::catalog::pg_ts_parser::Form_pg_ts_parser;
use crate::catalog::pg_ts_template::Form_pg_ts_template;
use crate::catalog::pg_type::Form_pg_type;
use crate::catalog::pg_known_oids::{PG_CATALOG_NAMESPACE, PG_TOAST_NAMESPACE};
use crate::nodes::primnodes::RangeVar;
use crate::storage::lockdefs::{AccessShareLock, LOCKMODE, NoLock};
use crate::utils::cache::lsyscache::get_namespace_name;

// ProcNumber type (storage/procnumber.h)
pub type ProcNumber = c_int;
pub const INVALID_PROC_NUMBER: ProcNumber = -1;

// SubTransactionId (from c.h / access/transam.h)
pub type SubTransactionId = u32;
pub const InvalidSubTransactionId: SubTransactionId = 0;

/*
 * The namespace search path is a possibly-empty list of namespace OIDs.
 * In addition to the explicit list, implicitly-searched namespaces
 * may be included:
 *
 * 1. If a TEMP table namespace has been initialized in this session, it
 * is implicitly searched first.
 *
 * 2. The system catalog namespace is always searched.  If the system
 * namespace is present in the explicit path then it will be searched in
 * the specified order; otherwise it will be searched after TEMP tables and
 * *before* the explicit list.  (It might seem that the system namespace
 * should be implicitly last, but this behavior appears to be required by
 * SQL99.  Also, this provides a way to search the system namespace first
 * without thereby making it the default creation target namespace.)
 *
 * For security reasons, searches using the search path will ignore the temp
 * namespace when searching for any object type other than relations and
 * types.  (We must allow types since temp tables have rowtypes.)
 *
 * The default creation target namespace is always the first element of the
 * explicit list.  If the explicit list is empty, there is no default target.
 *
 * The textual specification of search_path can include "$user" to refer to
 * the namespace named the same as the current user, if any.  (This is just
 * ignored if there is no such namespace.)  Also, it can include "pg_temp"
 * to refer to the current backend's temp namespace.  This is usually also
 * ignorable if the temp namespace hasn't been set up, but there's a special
 * case: if "pg_temp" appears first then it should be the default creation
 * target.  We kluge this case a little bit so that the temp namespace isn't
 * set up until the first attempt to create something in it.  (The reason for
 * klugery is that we can't create the temp namespace outside a transaction,
 * but initial GUC processing of search_path happens outside a transaction.)
 * activeTempCreationPending is true if "pg_temp" appears first in the string
 * but is not reflected in activeCreationNamespace because the namespace isn't
 * set up yet.
 *
 * In bootstrap mode, the search path is set equal to "pg_catalog", so that
 * the system namespace is the only one searched or inserted into.
 * initdb is also careful to set search_path to "pg_catalog" for its
 * post-bootstrap standalone backend runs.  Otherwise the default search
 * path is determined by GUC.  The factory default path contains the PUBLIC
 * namespace (if it exists), preceded by the user's personal namespace
 * (if one exists).
 *
 * activeSearchPath is always the actually active path; it points to
 * baseSearchPath which is the list derived from namespace_search_path.
 *
 * If baseSearchPathValid is false, then baseSearchPath (and other derived
 * variables) need to be recomputed from namespace_search_path, or retrieved
 * from the search path cache if there haven't been any syscache
 * invalidations.  We mark it invalid upon an assignment to
 * namespace_search_path or receipt of a syscache invalidation event for
 * pg_namespace or pg_authid.  The recomputation is done during the next
 * lookup attempt.
 *
 * Any namespaces mentioned in namespace_search_path that are not readable
 * by the current user ID are simply left out of baseSearchPath; so
 * we have to be willing to recompute the path when current userid changes.
 * namespaceUser is the userid the path has been computed for.
 *
 * Note: all data pointed to by these List variables is in TopMemoryContext.
 *
 * activePathGeneration is incremented whenever the effective values of
 * activeSearchPath/activeCreationNamespace/activeTempCreationPending change.
 * This can be used to quickly detect whether any change has happened since
 * a previous examination of the search path state.
 */

/* These variables define the actually active state: */

static mut activeSearchPath: *mut List = core::ptr::null_mut();

/* default place to create stuff; if InvalidOid, no default */
static mut activeCreationNamespace: Oid = InvalidOid;

/* if true, activeCreationNamespace is wrong, it should be temp namespace */
static mut activeTempCreationPending: bool = false;

/* current generation counter; make sure this is never zero */
static mut activePathGeneration: u64 = 1;

/* These variables are the values last derived from namespace_search_path: */

static mut baseSearchPath: *mut List = core::ptr::null_mut();

static mut baseCreationNamespace: Oid = InvalidOid;

static mut baseTempCreationPending: bool = false;

static mut namespaceUser: Oid = InvalidOid;

/* The above four values are valid only if baseSearchPathValid */
static mut baseSearchPathValid: bool = true;

/*
 * Storage for search path cache.  Clear searchPathCacheValid as a simple
 * way to invalidate *all* the cache entries, not just the active one.
 */
static mut searchPathCacheValid: bool = false;
static mut SearchPathCacheContext: MemoryContext = core::ptr::null_mut();

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SearchPathCacheKey {
    pub searchPath: *const c_char,
    pub roleid: Oid,
}

#[repr(C)]
pub struct SearchPathCacheEntry {
    pub key: SearchPathCacheKey,
    pub oidlist: *mut List,       /* namespace OIDs that pass ACL checks */
    pub finalPath: *mut List,     /* cached final computed search path */
    pub firstNS: Oid,             /* first explicitly-listed namespace */
    pub temp_missing: bool,
    pub forceRecompute: bool,     /* force recompute of finalPath */
    /* needed for simplehash */
    pub status: c_char,
}

/*
 * myTempNamespace is InvalidOid until and unless a TEMP namespace is set up
 * in a particular backend session (this happens when a CREATE TEMP TABLE
 * command is first executed).  Thereafter it's the OID of the temp namespace.
 *
 * myTempToastNamespace is the OID of the namespace for my temp tables' toast
 * tables.  It is set when myTempNamespace is, and is InvalidOid before that.
 *
 * myTempNamespaceSubID shows whether we've created the TEMP namespace in the
 * current subtransaction.  The flag propagates up the subtransaction tree,
 * so the main transaction will correctly recognize the flag if all
 * intermediate subtransactions commit.  When it is InvalidSubTransactionId,
 * we either haven't made the TEMP namespace yet, or have successfully
 * committed its creation, depending on whether myTempNamespace is valid.
 */
static mut myTempNamespace: Oid = InvalidOid;

static mut myTempToastNamespace: Oid = InvalidOid;

static mut myTempNamespaceSubID: SubTransactionId = InvalidSubTransactionId;

/*
 * This is the user's textual search path specification --- it's the value
 * of the GUC variable 'search_path'.
 */
#[no_mangle]
pub static mut namespace_search_path: *mut c_char = core::ptr::null_mut();

/*
 * Result of checkTempNamespaceStatus
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TempNamespaceStatus {
    TEMP_NAMESPACE_NOT_TEMP, /* nonexistent, or non-temp namespace */
    TEMP_NAMESPACE_IDLE,     /* exists, belongs to no active session */
    TEMP_NAMESPACE_IN_USE,   /* belongs to some active session */
}

/*
 * Structure for xxxSearchPathMatcher functions
 *
 * The generation counter is private to namespace.c and shouldn't be touched
 * by other code.  It can be initialized to zero if necessary (that means
 * "not known equal to the current active path").
 */
#[repr(C)]
pub struct SearchPathMatcher {
    pub schemas: *mut List,    /* OIDs of explicitly named schemas */
    pub addCatalog: bool,      /* implicitly prepend pg_catalog? */
    pub addTemp: bool,         /* implicitly prepend temp schema? */
    pub generation: u64,       /* for quick detection of equality to active */
}

/*
 * Option flag bits for RangeVarGetRelidExtended().
 */
#[repr(C)]
pub enum RVROption {
    RVR_MISSING_OK  = 1 << 0, /* don't error if relation doesn't exist */
    RVR_NOWAIT      = 1 << 1, /* error if relation cannot be locked */
    RVR_SKIP_LOCKED = 1 << 2, /* skip if relation cannot be locked */
}

pub type RangeVarGetRelidCallback = Option<
    unsafe extern "C" fn(
        relation: *const RangeVar,
        relId: Oid,
        oldRelId: Oid,
        callback_arg: *mut c_void,
    ),
>;

/*
 * RangeVarGetRelid macro (from namespace.h)
 * Calls RangeVarGetRelidExtended with missing_ok flag.
 */
#[inline]
pub unsafe fn RangeVarGetRelid(
    relation: *const RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> Oid {
    RangeVarGetRelidExtended(
        relation,
        lockmode,
        if missing_ok { RVROption::RVR_MISSING_OK as u32 } else { 0 },
        None,
        core::ptr::null_mut(),
    )
}

/*
 * FuncCandidateList -- list of possible functions or operators
 * found by namespace lookup.  Each function/operator is identified
 * by OID and by argument types; the list must be pruned by type
 * resolution rules that are embodied in the parser, not here.
 */
#[repr(C)]
pub struct _FuncCandidateList {
    pub next: *mut _FuncCandidateList,
    pub pathpos: c_int,        /* for internal use of namespace lookup */
    pub oid: Oid,              /* the function or operator's OID */
    pub nominalnargs: c_int,   /* either pronargs or length(proallargtypes) */
    pub nargs: c_int,          /* number of arg types returned */
    pub nvargs: c_int,         /* number of args to become variadic array */
    pub ndargs: c_int,         /* number of defaulted args */
    pub argnumbers: *mut c_int, /* args' positional indexes, if named call */
    pub args: [Oid; 1],        /* arg types (flexible array member, first elem) */
}
pub type FuncCandidateList = *mut _FuncCandidateList;

// ---------------------------------------------------------------------------
// TODO(pg-port) stubs -- external C symbols not yet ported
// ---------------------------------------------------------------------------

extern "C" {
    // miscadmin.h
    fn GetUserId() -> Oid;
    fn IsBootstrapProcessingMode() -> bool;
    static MyDatabaseId: Oid;
    static mut MyXactFlags: c_int;
    // storage/procnumber.h
    static MyProcNumber: ProcNumber;

    // xact.h
    fn GetCurrentSubTransactionId() -> SubTransactionId;
    fn CommandCounterIncrement();
    fn AbortOutOfAnyTransaction();
    fn StartTransactionCommand();
    fn CommitTransactionCommand();

    // xlog.h
    fn RecoveryInProgress() -> bool;

    // parallel.h
    fn IsParallelWorker() -> bool;

    // procarray.h
    static mut MyProc: *mut PGPROC;
    fn ProcNumberGetProc(procNumber: ProcNumber) -> *mut PGPROC;

    // storage/ipc.h
    fn before_shmem_exit(
        f: Option<unsafe extern "C" fn(code: c_int, arg: Datum)>,
        arg: Datum,
    );

    // utils/snapmgr.h
    fn GetTransactionSnapshot() -> *mut c_void;
    fn PushActiveSnapshot(snap: *mut c_void);
    fn PopActiveSnapshot();

    // storage/lmgr.h
    fn LockRelationOid(relid: Oid, lockmode: LOCKMODE);
    fn UnlockRelationOid(relid: Oid, lockmode: LOCKMODE);
    fn ConditionalLockRelationOid(relid: Oid, lockmode: LOCKMODE) -> bool;
    fn LockDatabaseObject(classid: Oid, objid: Oid, objsubid: u32, lockmode: LOCKMODE);
    fn UnlockDatabaseObject(classid: Oid, objid: Oid, objsubid: u32, lockmode: LOCKMODE);

    // utils/catcache.h
    fn AcceptInvalidationMessages();
    static SharedInvalidMessageCounter: u64;
    fn CacheRegisterSyscacheCallback(
        cacheid: c_int,
        func: Option<unsafe extern "C" fn(arg: Datum, cacheid: c_int, hashvalue: u32)>,
        arg: Datum,
    );

    // utils/inval.h (object access hook)
    static object_access_hook: *mut c_void;

    // utils/syscache.h -- TODO(pg-port): SearchSysCache not fully ported
    fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn SearchSysCache3(
        cacheId: c_int,
        key1: Datum,
        key2: Datum,
        key3: Datum,
    ) -> HeapTuple;
    fn SearchSysCache4(
        cacheId: c_int,
        key1: Datum,
        key2: Datum,
        key3: Datum,
        key4: Datum,
    ) -> HeapTuple;
    fn ReleaseSysCache(tuple: HeapTuple);
    fn SearchSysCacheList1(cacheId: c_int, key1: Datum) -> *mut CatCList;
    fn SearchSysCacheList3(
        cacheId: c_int,
        key1: Datum,
        key2: Datum,
        key3: Datum,
    ) -> *mut CatCList;
    fn ReleaseSysCacheList(list: *mut CatCList);
    fn SearchSysCacheExists2(cacheId: c_int, key1: Datum, key2: Datum) -> bool;
    fn SysCacheGetAttr(
        cacheId: c_int,
        tup: HeapTuple,
        attributeNumber: c_int,
        isNull: *mut bool,
    ) -> Datum;
    fn GetSysCacheOid1(cacheId: c_int, oidcol: c_int, key1: Datum) -> Oid;
    fn GetSysCacheOid2(cacheId: c_int, oidcol: c_int, key1: Datum, key2: Datum) -> Oid;
    fn GetSysCacheOid3(
        cacheId: c_int,
        oidcol: c_int,
        key1: Datum,
        key2: Datum,
        key3: Datum,
    ) -> Oid;

    // utils/acl.h -- TODO(pg-port): ACL checks
    fn object_aclcheck(
        classid: Oid,
        objectid: Oid,
        roleid: Oid,
        mode: AclMode,
    ) -> AclResult;
    fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool;
    fn aclcheck_error(
        aclerr: AclResult,
        objtype: ObjectType,
        objectname: *const c_char,
    );

    // utils/memutils.h
    fn AllocSetContextCreate(
        parent: MemoryContext,
        name: *const c_char,
        minContextSize: usize,
        initBlockSize: usize,
        maxBlockSize: usize,
    ) -> MemoryContext;
    fn MemoryContextReset(context: MemoryContext);
    fn MemoryContextStrdup(context: MemoryContext, string: *const c_char) -> *mut c_char;

    // utils/varlena.h
    fn SplitIdentifierString(
        rawstring: *mut c_char,
        separator: c_char,
        namelist: *mut *mut List,
    ) -> bool;
    fn quote_identifier(ident: *const c_char) -> *const c_char;

    // commands/dbcommands.h
    fn get_database_name(dbid: Oid) -> *mut c_char;
    fn get_relname_relid(relname: *const c_char, namespaceId: Oid) -> Oid;
    fn get_rel_relkind(relid: Oid) -> c_char;
    fn get_relkind_objtype(relkind: c_char) -> ObjectType;

    // catalog/objectaccess.h
    fn InvokeNamespaceSearchHook(namespaceId: Oid, iserror: bool) -> bool;

    // catalog/dependency.h
    fn performDeletion(
        object: *const ObjectAddress,
        behavior: DropBehavior,
        flags: c_int,
    );

    // catalog/pg_namespace.h helper
    fn NamespaceCreate(
        nspName: *const c_char,
        ownerId: Oid,
        isTemp: bool,
    ) -> Oid;

    // nodes/makefuncs.h
    fn makeRangeVar(
        schemaname: *mut c_char,
        relname: *mut c_char,
        location: c_int,
    ) -> *mut RangeVar;
    fn makeString(str_: *mut c_char) -> *mut Value;

    // mb/pg_wchar.h
    fn GetDatabaseEncoding() -> c_int;
    fn GetDatabaseEncodingName() -> *const c_char;
    fn is_encoding_supported_by_icu(encoding: c_int) -> bool;

    // utils/memutils.h ALLOCSET_DEFAULT_SIZES
    static ALLOCSET_DEFAULT_MINSIZE: usize;
    static ALLOCSET_DEFAULT_INITSIZE: usize;
    static ALLOCSET_DEFAULT_MAXSIZE: usize;

    // utils/lsyscache.h -- FindDefaultConversion
    fn FindDefaultConversion(
        namespaceId: Oid,
        for_encoding: i32,
        to_encoding: i32,
    ) -> Oid;

    // access/xact.h flags
    static XACT_FLAGS_ACCESSEDTEMPNAMESPACE: c_int;

    // GUC hooks (utils/guc_hooks.h)
    fn GUC_check_errdetail(fmt: *const c_char, ...);
}

// ---------------------------------------------------------------------------
// Syscache ID stubs -- TODO(pg-port): move to proper syscache module
// ---------------------------------------------------------------------------
const RELOID: c_int = 26;
const TYPEOID: c_int = 31;
const TYPENAMENSP: c_int = 29; // TODO(pg-port): syscache id for (pg_type name, nsp)
const PROCOID: c_int = 21;
const PROCNAMEARGSNSP: c_int = 20;
const OPEROID: c_int = 18;
const OPERNAMENSP: c_int = 19;
const CLAOID: c_int = 5;
const CLAAMNAMENSP: c_int = 4;
const OPFAMILYOID: c_int = 22;
const OPFAMILYAMNAMENSP: c_int = 23;
const COLLOID: c_int = 8;
const COLLNAMEENCNSP: c_int = 7;
const CONVOID: c_int = 9;
const CONNAMENSP: c_int = 10;
const STATEXTOID: c_int = 27;
const STATEXTNAMENSP: c_int = 28;
const TSPARSEROID: c_int = 32;
const TSPARSERNAMENSP: c_int = 33;
const TSDICTOID: c_int = 34;
const TSDICTNAMENSP: c_int = 35;
const TSTEMPLATEOID: c_int = 36;
const TSTEMPLATENAMENSP: c_int = 37;
const TSCONFIGOID: c_int = 38;
const TSCONFIGNAMENSP: c_int = 39;
const NAMESPACENAME: c_int = 17;
const AUTHOID: c_int = 2;
const AUTHMEMROLEMEM: c_int = 3;
const DATABASEOID: c_int = 11;
const NAMESPACEOID: c_int = 16;

// Anum_ stubs (attribute numbers)
const Anum_pg_type_oid: c_int = 1;
const Anum_pg_opclass_oid: c_int = 1;
const Anum_pg_opfamily_oid: c_int = 1;
const Anum_pg_collation_oid: c_int = 1;
const Anum_pg_conversion_oid: c_int = 1;
const Anum_pg_statistic_ext_oid: c_int = 1;
const Anum_pg_ts_parser_oid: c_int = 1;
const Anum_pg_ts_dict_oid: c_int = 1;
const Anum_pg_ts_template_oid: c_int = 1;
const Anum_pg_ts_config_oid: c_int = 1;
const Anum_pg_namespace_oid: c_int = 1;
const Anum_pg_proc_proallargtypes: c_int = 21;
const Anum_pg_proc_proargnames: c_int = 22;

// AclMode / AclResult / ObjectType stubs
pub type AclMode = u32;
pub const ACL_USAGE: AclMode = 1 << 0;
pub const ACL_CREATE: AclMode = 1 << 1;
pub const ACL_CREATE_TEMP: AclMode = 1 << 2;

#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AclResult {
    ACLCHECK_OK,
    ACLCHECK_NOT_OWNER,
    ACLCHECK_NO_PRIV,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub enum ObjectType {
    OBJECT_SCHEMA,
    OBJECT_TABLE,
    OBJECT_INDEX,
    /* ... others not needed here */
}

#[repr(C)]
#[derive(Clone, Copy)]
pub enum DropBehavior {
    DROP_RESTRICT,
    DROP_CASCADE,
}

// performDeletion flags
const PERFORM_DELETION_INTERNAL: c_int        = 0x0001;
const PERFORM_DELETION_QUIETLY: c_int         = 0x0002;
const PERFORM_DELETION_SKIP_ORIGINAL: c_int   = 0x0004;
const PERFORM_DELETION_SKIP_EXTENSIONS: c_int = 0x0008;

// ObjectAddress
#[repr(C)]
pub struct ObjectAddress {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: c_int,
}

// BOOTSTRAP_SUPERUSERID
const BOOTSTRAP_SUPERUSERID: Oid = 10;

// NAMEDATALEN
const NAMEDATALEN: usize = 64;

// FUNC_MAX_ARGS
const FUNC_MAX_ARGS: usize = 100;

// FUNC_PARAM_* mode constants
const FUNC_PARAM_IN: c_char = b'i' as c_char;
const FUNC_PARAM_INOUT: c_char = b'b' as c_char;
const FUNC_PARAM_VARIADIC: c_char = b'v' as c_char;

// CatCList and CatCTup stubs (utils/catcache.h)
#[repr(C)]
pub struct CatCTup {
    pub tuple: HeapTupleData,
}

#[repr(C)]
pub struct HeapTupleData {
    pub t_len: u32,
    pub t_self: ItemPointerData,
    pub t_tableOid: Oid,
    pub t_data: *mut c_void,
}

#[repr(C)]
pub struct ItemPointerData {
    pub ip_blkid: [u8; 4],
    pub ip_posid: u16,
}

#[repr(C)]
pub struct CatCList {
    pub n_members: c_int,
    pub ordered: bool,
    pub members: *mut *mut CatCTup,
}

// PGPROC partial stub
#[repr(C)]
pub struct PGPROC {
    pub databaseId: Oid,
    pub tempNamespaceId: Oid,
    /* ... many more fields omitted */
}

// Value node (nodes/value.h)
#[repr(C)]
pub struct Value {
    pub type_: NodeTag,
    pub val: ValueUnion,
}

#[repr(C)]
pub union ValueUnion {
    pub ival: c_long,
    pub fval: f64,
    pub str_: *mut c_char,
}

// SPCACHE_RESET_THRESHOLD
const SPCACHE_RESET_THRESHOLD: u32 = 256;

// ALLOCSET macros (from utils/memutils.h)
const ALLOCSET_DEFAULT_MINSIZE_C: usize  = 0;
const ALLOCSET_DEFAULT_INITSIZE_C: usize = 8 * 1024;
const ALLOCSET_DEFAULT_MAXSIZE_C: usize  = 8 * 1024 * 1024;

// SearchPathCache -- simplehash wrapper (opaque pointer here)
// In C this is nsphash_hash*, a generated simplehash table.
// We represent it as a raw pointer to an opaque type.
#[repr(C)]
pub struct NspHashTable {
    _opaque: [u8; 0],
}

static mut SearchPathCache: *mut NspHashTable = core::ptr::null_mut();
static mut LastSearchPathCacheEntry: *mut SearchPathCacheEntry = core::ptr::null_mut();

extern "C" {
    // simplehash operations -- TODO(pg-port): port simplehash or keep as extern
    fn nsphash_create(
        ctx: MemoryContext,
        nelem: u32,
        private_data: *mut c_void,
    ) -> *mut NspHashTable;
    fn nsphash_lookup(
        tb: *mut NspHashTable,
        key: SearchPathCacheKey,
    ) -> *mut SearchPathCacheEntry;
    fn nsphash_insert(
        tb: *mut NspHashTable,
        key: SearchPathCacheKey,
        found: *mut bool,
    ) -> *mut SearchPathCacheEntry;
    fn nsphash_get_num_entries(tb: *mut NspHashTable) -> u32;

    // get_func_arg_info (utils/lsyscache.h)
    fn get_func_arg_info(
        procTup: HeapTuple,
        p_argtypes: *mut *mut Oid,
        p_argnames: *mut *mut *mut c_char,
        p_argmodes: *mut *mut c_char,
    ) -> c_int;

    fn equal(a: *const c_void, b: *const c_void) -> bool;
    fn list_copy(list: *const List) -> *mut List;
    fn list_free(list: *mut List);
    fn list_make1_oid(x1: Oid) -> *mut List;
    fn lappend_oid(list: *mut List, datum: Oid) -> *mut List;
    fn lcons_oid(datum: Oid, list: *mut List) -> *mut List;
    fn list_member_oid(list: *const List, datum: Oid) -> bool;
    fn list_length(list: *const List) -> c_int;
    fn list_head(list: *const List) -> *mut ListCell;
    fn lnext(list: *const List, lc: *mut ListCell) -> *mut ListCell;
    fn list_delete_first(list: *mut List) -> *mut List;
    fn linitial_oid(list: *const List) -> Oid;
    fn lfirst_oid(lc: *const ListCell) -> Oid;
    fn lfirst(lc: *const ListCell) -> *mut c_void;
    fn linitial(list: *const List) -> *mut c_void;
    fn lsecond(list: *const List) -> *mut c_void;
    fn lthird(list: *const List) -> *mut c_void;
    fn list_make1(x: *mut c_void) -> *mut List;

    fn strVal(v: *const c_void) -> *mut c_char;
    fn nodeTag(node: *const c_void) -> NodeTag;
    fn IsA_fn(node: *const c_void, tag: NodeTag) -> bool;

    fn pstrdup(str_: *const c_char) -> *mut c_char;
    fn pfree(ptr: *mut c_void);
    fn palloc(size: usize) -> *mut c_void;
    fn palloc0(size: usize) -> *mut c_void;
    fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext;

    fn snprintf(s: *mut c_char, n: usize, format: *const c_char, ...) -> c_int;
    fn atoi(s: *const c_char) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(dst: *mut c_void, c: c_int, n: usize) -> *mut c_void;

    // TopMemoryContext (from utils/memutils.h)
    static TopMemoryContext: MemoryContext;

    // array helpers (utils/array.h)
    fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType;
    fn ARR_DIMS(arr: *mut ArrayType) -> *mut c_int;
    fn ARR_NDIM(arr: *mut ArrayType) -> c_int;
    fn ARR_HASNULL(arr: *mut ArrayType) -> bool;
    fn ARR_ELEMTYPE(arr: *mut ArrayType) -> Oid;
    fn ARR_DATA_PTR(arr: *mut ArrayType) -> *mut c_void;
}

// Opaque array type for palloc compat
#[repr(C)]
pub struct ArrayType {
    _opaque: [u8; 0],
}

// NodeTag stubs
pub type NodeTag = u32;
const T_String: NodeTag = 602;
const T_A_Star: NodeTag = 600;

// OIDOID
const OIDOID: Oid = 26;

// StringInfoData for NameListToString / NameListToQuotedString
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}
pub type StringInfo = *mut StringInfoData;

extern "C" {
    fn initStringInfo(str_: StringInfo);
    fn appendStringInfoChar(str_: StringInfo, ch: c_char);
    fn appendStringInfoString(str_: StringInfo, s: *const c_char);
}

// pg_proc.proargtypes is CATALOG_VARLEN and not a fixed field of FormData_pg_proc.
// TODO(pg-port): pg_proc.proargtypes (CATALOG_VARLEN) accessor not ported.
unsafe fn pg_proc_proargtypes_values(_procform: Form_pg_proc) -> *mut Oid {
    unimplemented!("pg_proc.proargtypes (CATALOG_VARLEN) accessor not ported")
}

// Max() macro equivalent
#[inline]
fn Max(a: c_int, b: c_int) -> c_int {
    if a > b { a } else { b }
}

// offsetof
macro_rules! offsetof_struct {
    ($type:ty, $field:ident) => {
        core::mem::offset_of!($type, $field)
    };
}

// SPACE_PER_OP (from OpernameGetCandidates)
// MAXALIGN(offsetof(_FuncCandidateList, args) + 2 * sizeof(Oid))
// We approximate MAXALIGN as rounding up to 8-byte alignment.
#[inline]
fn space_per_op() -> usize {
    let base = core::mem::offset_of!(_FuncCandidateList, args) + 2 * core::mem::size_of::<Oid>();
    (base + 7) & !7
}

// ---------------------------------------------------------------------------
// spcache helpers (wrapping the simplehash table)
// ---------------------------------------------------------------------------

/*
 * Create or reset search_path cache as necessary.
 */
unsafe fn spcache_init() {
    if !SearchPathCache.is_null()
        && searchPathCacheValid
        && nsphash_get_num_entries(SearchPathCache) < SPCACHE_RESET_THRESHOLD
    {
        return;
    }

    searchPathCacheValid = false;
    baseSearchPathValid = false;

    /*
     * Make sure we don't leave dangling pointers if a failure happens during
     * initialization.
     */
    SearchPathCache = core::ptr::null_mut();
    LastSearchPathCacheEntry = core::ptr::null_mut();

    if SearchPathCacheContext.is_null() {
        /* Make the context we'll keep search path cache hashtable in */
        let name = b"search_path processing cache\0".as_ptr() as *const c_char;
        SearchPathCacheContext = AllocSetContextCreate(
            TopMemoryContext,
            name,
            ALLOCSET_DEFAULT_MINSIZE_C,
            ALLOCSET_DEFAULT_INITSIZE_C,
            ALLOCSET_DEFAULT_MAXSIZE_C,
        );
    } else {
        MemoryContextReset(SearchPathCacheContext);
    }

    /* arbitrary initial starting size of 16 elements */
    SearchPathCache = nsphash_create(SearchPathCacheContext, 16, core::ptr::null_mut());
    searchPathCacheValid = true;
}

/*
 * Look up entry in search path cache without inserting. Returns NULL if not
 * present.
 */
unsafe fn spcache_lookup(
    searchPath: *const c_char,
    roleid: Oid,
) -> *mut SearchPathCacheEntry {
    if !LastSearchPathCacheEntry.is_null()
        && (*LastSearchPathCacheEntry).key.roleid == roleid
        && strcmp((*LastSearchPathCacheEntry).key.searchPath, searchPath) == 0
    {
        return LastSearchPathCacheEntry;
    } else {
        let cachekey = SearchPathCacheKey { searchPath, roleid };
        let entry = nsphash_lookup(SearchPathCache, cachekey);
        if !entry.is_null() {
            LastSearchPathCacheEntry = entry;
        }
        return entry;
    }
}

/*
 * Look up or insert entry in search path cache.
 *
 * Initialize key safely, so that OOM does not leave an entry without a valid
 * key. Caller must ensure that non-key contents are properly initialized.
 */
unsafe fn spcache_insert(
    searchPath: *const c_char,
    roleid: Oid,
) -> *mut SearchPathCacheEntry {
    if !LastSearchPathCacheEntry.is_null()
        && (*LastSearchPathCacheEntry).key.roleid == roleid
        && strcmp((*LastSearchPathCacheEntry).key.searchPath, searchPath) == 0
    {
        return LastSearchPathCacheEntry;
    } else {
        let mut cachekey = SearchPathCacheKey { searchPath, roleid };

        /*
         * searchPath is not saved in SearchPathCacheContext. First perform a
         * lookup, and copy searchPath only if we need to create a new entry.
         */
        let mut entry = nsphash_lookup(SearchPathCache, cachekey);

        if entry.is_null() {
            let mut found: bool = false;

            cachekey.searchPath =
                MemoryContextStrdup(SearchPathCacheContext, searchPath);
            entry = nsphash_insert(SearchPathCache, cachekey, &mut found);
            Assert!(!found);

            (*entry).oidlist = core::ptr::null_mut();
            (*entry).finalPath = core::ptr::null_mut();
            (*entry).firstNS = InvalidOid;
            (*entry).temp_missing = false;
            (*entry).forceRecompute = false;
            /* do not touch entry->status, used by simplehash */
        }

        LastSearchPathCacheEntry = entry;
        return entry;
    }
}

// ===========================================================================
// PART 2: RangeVarGetRelidExtended, RangeVarGetCreationNamespace,
//         RangeVarGetAndCheckCreationNamespace, RangeVarAdjustRelationPersistence,
//         RelnameGetRelid, RelationIsVisible[Ext]
// ===========================================================================

/*
 * RangeVarGetRelidExtended
 *		Given a RangeVar describing an existing relation,
 *		select the proper namespace and look up the relation OID.
 *
 * If the schema or relation is not found, return InvalidOid if flags contains
 * RVR_MISSING_OK, otherwise raise an error.
 *
 * If flags contains RVR_NOWAIT, throw an error if we'd have to wait for a
 * lock.
 *
 * If flags contains RVR_SKIP_LOCKED, return InvalidOid if we'd have to wait
 * for a lock.
 *
 * flags cannot contain both RVR_NOWAIT and RVR_SKIP_LOCKED.
 *
 * Note that if RVR_MISSING_OK and RVR_SKIP_LOCKED are both specified, a
 * return value of InvalidOid could either mean the relation is missing or it
 * could not be locked.
 *
 * Callback allows caller to check permissions or acquire additional locks
 * prior to grabbing the relation lock.
 */
pub unsafe fn RangeVarGetRelidExtended(
    relation: *const RangeVar,
    lockmode: LOCKMODE,
    flags: u32,
    callback: RangeVarGetRelidCallback,
    callback_arg: *mut c_void,
) -> Oid {
    let mut inval_count: u64;
    let mut relId: Oid;
    let mut oldRelId: Oid = InvalidOid;
    let mut retry: bool = false;
    let missing_ok: bool = (flags & RVROption::RVR_MISSING_OK as u32) != 0;

    /* verify that flags do not conflict */
    Assert!(
        !((flags & RVROption::RVR_NOWAIT as u32 != 0)
            && (flags & RVROption::RVR_SKIP_LOCKED as u32 != 0)),
    );

    /*
     * We check the catalog name and then ignore it.
     */
    if !(*relation).catalogname.is_null() {
        if strcmp((*relation).catalogname, get_database_name(MyDatabaseId)) != 0 {
            ereport!(ERROR, errmsg!(
                    "cross-database references are not implemented: \"{}.{}.{}\"",
                    CStr::from_ptr((*relation).catalogname).to_string_lossy(),
                    CStr::from_ptr((*relation).schemaname).to_string_lossy(),
                    CStr::from_ptr((*relation).relname).to_string_lossy()
                )) /* C also: errcode */;
        }
    }

    /*
     * DDL operations can change the results of a name lookup.  Since all such
     * operations will generate invalidation messages, we keep track of
     * whether any such messages show up while we're performing the operation,
     * and retry until either (1) no more invalidation messages show up or (2)
     * the answer doesn't change.
     *
     * But if lockmode = NoLock, then we assume that either the caller is OK
     * with the answer changing under them, or that they already hold some
     * appropriate lock, and therefore return the first answer we get without
     * checking for invalidation messages.  Also, if the requested lock is
     * already held, LockRelationOid will not AcceptInvalidationMessages, so
     * we may fail to notice a change.  We could protect against that case by
     * calling AcceptInvalidationMessages() before beginning this loop, but
     * that would add a significant amount overhead, so for now we don't.
     */
    'retry_loop: loop {
        /*
         * Remember this value, so that, after looking up the relation name
         * and locking its OID, we can check whether any invalidation messages
         * have been processed that might require a do-over.
         */
        inval_count = SharedInvalidMessageCounter;

        /*
         * Some non-default relpersistence value may have been specified.  The
         * parser never generates such a RangeVar in simple DML, but it can
         * happen in contexts such as "CREATE TEMP TABLE foo (f1 int PRIMARY
         * KEY)".  Such a command will generate an added CREATE INDEX
         * operation, which must be careful to find the temp table, even when
         * pg_temp is not first in the search path.
         */
        if (*relation).relpersistence == RELPERSISTENCE_TEMP as c_char {
            if !OidIsValid(myTempNamespace) {
                relId = InvalidOid; /* this probably can't happen? */
            } else {
                if !(*relation).schemaname.is_null() {
                    let namespaceId: Oid;

                    namespaceId =
                        LookupExplicitNamespace((*relation).schemaname, missing_ok);

                    /*
                     * For missing_ok, allow a non-existent schema name to
                     * return InvalidOid.
                     */
                    if namespaceId != myTempNamespace {
                        ereport!(ERROR, errmsg!("temporary tables cannot specify a schema name")) /* C also: errcode */;
                    }
                }

                relId = get_relname_relid((*relation).relname, myTempNamespace);
            }
        } else if !(*relation).schemaname.is_null() {
            let namespaceId: Oid;

            /* use exact schema given */
            namespaceId = LookupExplicitNamespace((*relation).schemaname, missing_ok);
            if missing_ok && !OidIsValid(namespaceId) {
                relId = InvalidOid;
            } else {
                relId = get_relname_relid((*relation).relname, namespaceId);
            }
        } else {
            /* search the namespace path */
            relId = RelnameGetRelid((*relation).relname);
        }

        /*
         * Invoke caller-supplied callback, if any.
         *
         * This callback is a good place to check permissions: we haven't
         * taken the table lock yet (and it's really best to check permissions
         * before locking anything!), but we've gotten far enough to know what
         * OID we think we should lock.  Of course, concurrent DDL might
         * change things while we're waiting for the lock, but in that case
         * the callback will be invoked again for the new OID.
         */
        if let Some(cb) = callback {
            cb(relation, relId, oldRelId, callback_arg);
        }

        /*
         * If no lock requested, we assume the caller knows what they're
         * doing.  They should have already acquired a heavyweight lock on
         * this relation earlier in the processing of this same statement, so
         * it wouldn't be appropriate to AcceptInvalidationMessages() here, as
         * that might pull the rug out from under them.
         */
        if lockmode == NoLock {
            break 'retry_loop;
        }

        /*
         * If, upon retry, we get back the same OID we did last time, then the
         * invalidation messages we processed did not change the final answer.
         * So we're done.
         *
         * If we got a different OID, we've locked the relation that used to
         * have this name rather than the one that does now.  So release the
         * lock.
         */
        if retry {
            if relId == oldRelId {
                break 'retry_loop;
            }
            if OidIsValid(oldRelId) {
                UnlockRelationOid(oldRelId, lockmode);
            }
        }

        /*
         * Lock relation.  This will also accept any pending invalidation
         * messages.  If we got back InvalidOid, indicating not found, then
         * there's nothing to lock, but we accept invalidation messages
         * anyway, to flush any negative catcache entries that may be
         * lingering.
         */
        if !OidIsValid(relId) {
            AcceptInvalidationMessages();
        } else if (flags & (RVROption::RVR_NOWAIT as u32 | RVROption::RVR_SKIP_LOCKED as u32)) == 0 {
            LockRelationOid(relId, lockmode);
        } else if !ConditionalLockRelationOid(relId, lockmode) {
            let elevel: c_int = if (flags & RVROption::RVR_SKIP_LOCKED as u32) != 0 {
                DEBUG1
            } else {
                ERROR
            };

            if !(*relation).schemaname.is_null() {
                ereport!(elevel, errmsg!(
                        "could not obtain lock on relation \"{}.{}\"",
                        CStr::from_ptr((*relation).schemaname).to_string_lossy(),
                        CStr::from_ptr((*relation).relname).to_string_lossy()
                    )) /* C also: errcode */;
            } else {
                ereport!(elevel, errmsg!(
                        "could not obtain lock on relation \"{}\"",
                        CStr::from_ptr((*relation).relname).to_string_lossy()
                    )) /* C also: errcode */;
            }

            return InvalidOid;
        }

        /*
         * If no invalidation message were processed, we're done!
         */
        if inval_count == SharedInvalidMessageCounter {
            break 'retry_loop;
        }

        /*
         * Something may have changed.  Let's repeat the name lookup, to make
         * sure this name still references the same relation it did
         * previously.
         */
        retry = true;
        oldRelId = relId;
    }

    if !OidIsValid(relId) {
        let elevel: c_int = if missing_ok { DEBUG1 } else { ERROR };

        if !(*relation).schemaname.is_null() {
            ereport!(elevel, errmsg!(
                    "relation \"{}.{}\" does not exist",
                    CStr::from_ptr((*relation).schemaname).to_string_lossy(),
                    CStr::from_ptr((*relation).relname).to_string_lossy()
                )) /* C also: errcode */;
        } else {
            ereport!(elevel, errmsg!(
                    "relation \"{}\" does not exist",
                    CStr::from_ptr((*relation).relname).to_string_lossy()
                )) /* C also: errcode */;
        }
    }
    relId
}

/*
 * RangeVarGetCreationNamespace
 *		Given a RangeVar describing a to-be-created relation,
 *		choose which namespace to create it in.
 *
 * Note: calling this may result in a CommandCounterIncrement operation.
 * That will happen on the first request for a temp table in any particular
 * backend run; we will need to either create or clean out the temp schema.
 */
pub unsafe fn RangeVarGetCreationNamespace(newRelation: *const RangeVar) -> Oid {
    let namespaceId: Oid;

    /*
     * We check the catalog name and then ignore it.
     */
    if !(*newRelation).catalogname.is_null() {
        if strcmp((*newRelation).catalogname, get_database_name(MyDatabaseId)) != 0 {
            ereport!(ERROR, errmsg!(
                    "cross-database references are not implemented: \"{}.{}.{}\"",
                    CStr::from_ptr((*newRelation).catalogname).to_string_lossy(),
                    CStr::from_ptr((*newRelation).schemaname).to_string_lossy(),
                    CStr::from_ptr((*newRelation).relname).to_string_lossy()
                )) /* C also: errcode */;
        }
    }

    if !(*newRelation).schemaname.is_null() {
        /* check for pg_temp alias */
        if strcmp((*newRelation).schemaname, b"pg_temp\0".as_ptr() as *const c_char) == 0 {
            /* Initialize temp namespace */
            AccessTempTableNamespace(false);
            return myTempNamespace;
        }
        /* use exact schema given */
        namespaceId = get_namespace_oid((*newRelation).schemaname, false);
        /* we do not check for USAGE rights here! */
    } else if (*newRelation).relpersistence == RELPERSISTENCE_TEMP as c_char {
        /* Initialize temp namespace */
        AccessTempTableNamespace(false);
        return myTempNamespace;
    } else {
        /* use the default creation namespace */
        recomputeNamespacePath();
        if activeTempCreationPending {
            /* Need to initialize temp namespace */
            AccessTempTableNamespace(true);
            return myTempNamespace;
        }
        namespaceId = activeCreationNamespace;
        if !OidIsValid(namespaceId) {
            ereport!(ERROR, errmsg!("no schema has been selected to create in")) /* C also: errcode */;
        }
    }

    /* Note: callers will check for CREATE rights when appropriate */

    namespaceId
}

/*
 * RangeVarGetAndCheckCreationNamespace
 *
 * This function returns the OID of the namespace in which a new relation
 * with a given name should be created.  If the user does not have CREATE
 * permission on the target namespace, this function will instead signal
 * an ERROR.
 *
 * If non-NULL, *existing_relation_id is set to the OID of any existing relation
 * with the same name which already exists in that namespace, or to InvalidOid
 * if no such relation exists.
 *
 * If lockmode != NoLock, the specified lock mode is acquired on the existing
 * relation, if any, provided that the current user owns the target relation.
 * However, if lockmode != NoLock and the user does not own the target
 * relation, we throw an ERROR, as we must not try to lock relations the
 * user does not have permissions on.
 *
 * As a side effect, this function acquires AccessShareLock on the target
 * namespace.  Without this, the namespace could be dropped before our
 * transaction commits, leaving behind relations with relnamespace pointing
 * to a no-longer-existent namespace.
 *
 * As a further side-effect, if the selected namespace is a temporary namespace,
 * we mark the RangeVar as RELPERSISTENCE_TEMP.
 */
pub unsafe fn RangeVarGetAndCheckCreationNamespace(
    relation: *mut RangeVar,
    lockmode: LOCKMODE,
    existing_relation_id: *mut Oid,
) -> Oid {
    let mut inval_count: u64;
    let mut relid: Oid;
    let mut oldrelid: Oid = InvalidOid;
    let mut nspid: Oid;
    let mut oldnspid: Oid = InvalidOid;
    let mut retry: bool = false;

    /*
     * We check the catalog name and then ignore it.
     */
    if !(*relation).catalogname.is_null() {
        if strcmp((*relation).catalogname, get_database_name(MyDatabaseId)) != 0 {
            ereport!(ERROR, errmsg!(
                    "cross-database references are not implemented: \"{}.{}.{}\"",
                    CStr::from_ptr((*relation).catalogname).to_string_lossy(),
                    CStr::from_ptr((*relation).schemaname).to_string_lossy(),
                    CStr::from_ptr((*relation).relname).to_string_lossy()
                )) /* C also: errcode */;
        }
    }

    /*
     * As in RangeVarGetRelidExtended(), we guard against concurrent DDL
     * operations by tracking whether any invalidation messages are processed
     * while we're doing the name lookups and acquiring locks.  See comments
     * in that function for a more detailed explanation of this logic.
     */
    loop {
        let aclresult: AclResult;

        inval_count = SharedInvalidMessageCounter;

        /* Look up creation namespace and check for existing relation. */
        nspid = RangeVarGetCreationNamespace(relation);
        Assert!(OidIsValid(nspid));
        if !existing_relation_id.is_null() {
            relid = get_relname_relid((*relation).relname, nspid);
        } else {
            relid = InvalidOid;
        }

        /*
         * In bootstrap processing mode, we don't bother with permissions or
         * locking.  Permissions might not be working yet, and locking is
         * unnecessary.
         */
        if IsBootstrapProcessingMode() {
            break;
        }

        /* Check namespace permissions. */
        let aclresult2 =
            object_aclcheck(NamespaceRelationId, nspid, GetUserId(), ACL_CREATE);
        if aclresult2 != AclResult::ACLCHECK_OK {
            aclcheck_error(aclresult2, ObjectType::OBJECT_SCHEMA, get_namespace_name(nspid));
        }

        if retry {
            /* If nothing changed, we're done. */
            if relid == oldrelid && nspid == oldnspid {
                break;
            }
            /* If creation namespace has changed, give up old lock. */
            if nspid != oldnspid {
                UnlockDatabaseObject(NamespaceRelationId, oldnspid, 0, AccessShareLock);
            }
            /* If name points to something different, give up old lock. */
            if relid != oldrelid && OidIsValid(oldrelid) && lockmode != NoLock {
                UnlockRelationOid(oldrelid, lockmode);
            }
        }

        /* Lock namespace. */
        if nspid != oldnspid {
            LockDatabaseObject(NamespaceRelationId, nspid, 0, AccessShareLock);
        }

        /* Lock relation, if required if and we have permission. */
        if lockmode != NoLock && OidIsValid(relid) {
            if !object_ownercheck(RelationRelationId, relid, GetUserId()) {
                aclcheck_error(
                    AclResult::ACLCHECK_NOT_OWNER,
                    get_relkind_objtype(get_rel_relkind(relid)),
                    (*relation).relname,
                );
            }
            if relid != oldrelid {
                LockRelationOid(relid, lockmode);
            }
        }

        /* If no invalidation message were processed, we're done! */
        if inval_count == SharedInvalidMessageCounter {
            break;
        }

        /* Something may have changed, so recheck our work. */
        retry = true;
        oldrelid = relid;
        oldnspid = nspid;
    }

    RangeVarAdjustRelationPersistence(relation, nspid);
    if !existing_relation_id.is_null() {
        *existing_relation_id = relid;
    }
    nspid
}

/*
 * Adjust the relpersistence for an about-to-be-created relation based on the
 * creation namespace, and throw an error for invalid combinations.
 */
pub unsafe fn RangeVarAdjustRelationPersistence(newRelation: *mut RangeVar, nspid: Oid) {
    match (*newRelation).relpersistence as u8 {
        x if x == RELPERSISTENCE_TEMP as u8 => {
            if !isTempOrTempToastNamespace(nspid) {
                if isAnyTempNamespace(nspid) {
                    ereport!(ERROR, errmsg!(
                            "cannot create relations in temporary schemas of other sessions"
                        )) /* C also: errcode */;
                } else {
                    ereport!(ERROR, errmsg!("cannot create temporary relation in non-temporary schema")) /* C also: errcode */;
                }
            }
        }
        x if x == RELPERSISTENCE_PERMANENT as u8 => {
            if isTempOrTempToastNamespace(nspid) {
                (*newRelation).relpersistence = RELPERSISTENCE_TEMP as c_char;
            } else if isAnyTempNamespace(nspid) {
                ereport!(ERROR, errmsg!(
                        "cannot create relations in temporary schemas of other sessions"
                    )) /* C also: errcode */;
            }
        }
        _ => {
            if isAnyTempNamespace(nspid) {
                ereport!(ERROR, errmsg!(
                        "only temporary relations may be created in temporary schemas"
                    )) /* C also: errcode */;
            }
        }
    }
}

/*
 * RelnameGetRelid
 *		Try to resolve an unqualified relation name.
 *		Returns OID if relation found in search path, else InvalidOid.
 */
pub unsafe fn RelnameGetRelid(relname: *const c_char) -> Oid {
    let mut relid: Oid;
    let mut l: *mut ListCell;

    recomputeNamespacePath();

    l = list_head(activeSearchPath);
    while !l.is_null() {
        let namespaceId: Oid = lfirst_oid(l);

        relid = get_relname_relid(relname, namespaceId);
        if OidIsValid(relid) {
            return relid;
        }
        l = lnext(activeSearchPath, l);
    }

    /* Not found in path */
    InvalidOid
}


/*
 * RelationIsVisible
 *		Determine whether a relation (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified relation name".
 */
pub unsafe fn RelationIsVisible(relid: Oid) -> bool {
    RelationIsVisibleExt(relid, core::ptr::null_mut())
}

/*
 * RelationIsVisibleExt
 *		As above, but if the relation isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn RelationIsVisibleExt(relid: Oid, is_missing: *mut bool) -> bool {
    let reltup: HeapTuple;
    let relform: Form_pg_class;
    let relnamespace: Oid;
    let visible: bool;

    reltup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(reltup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    relform = GETSTRUCT(reltup) as Form_pg_class;

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    relnamespace = (*relform).relnamespace;
    if relnamespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, relnamespace)
    {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another relation of the same name earlier in the path. So
         * we must do a slow check for conflicting relations.
         */
        let relname: *mut c_char = NameStr(&(*relform).relname) as *mut c_char;
        let mut l: *mut ListCell;

        let mut vis = false;
        l = list_head(activeSearchPath);
        'vis_loop: while !l.is_null() {
            let namespaceId: Oid = lfirst_oid(l);

            if namespaceId == relnamespace {
                /* Found it first in path */
                vis = true;
                break 'vis_loop;
            }
            if OidIsValid(get_relname_relid(relname, namespaceId)) {
                /* Found something else first in path */
                break 'vis_loop;
            }
            l = lnext(activeSearchPath, l);
        }
        visible = vis;
    }

    ReleaseSysCache(reltup);

    visible
}

// ===========================================================================
// PART 3: TypenameGetTypid[Extended], TypeIsVisible[Ext],
//         FuncnameGetCandidates, MatchNamedCall, FunctionIsVisible[Ext]
// ===========================================================================

/*
 * TypenameGetTypid
 *		Wrapper for binary compatibility.
 */
pub unsafe fn TypenameGetTypid(typname: *const c_char) -> Oid {
    TypenameGetTypidExtended(typname, true)
}

/*
 * TypenameGetTypidExtended
 *		Try to resolve an unqualified datatype name.
 *		Returns OID if type found in search path, else InvalidOid.
 *
 * This is essentially the same as RelnameGetRelid.
 */
pub unsafe fn TypenameGetTypidExtended(typname: *const c_char, temp_ok: bool) -> Oid {
    let mut typid: Oid;
    let mut l: *mut ListCell;

    recomputeNamespacePath();

    l = list_head(activeSearchPath);
    while !l.is_null() {
        let namespaceId: Oid = lfirst_oid(l);

        if !temp_ok && namespaceId == myTempNamespace {
            l = lnext(activeSearchPath, l);
            continue; /* do not look in temp namespace */
        }

        typid = GetSysCacheOid2(
            TYPENAMENSP,
            Anum_pg_type_oid,
            PointerGetDatum(typname as *const c_void),
            ObjectIdGetDatum(namespaceId),
        );
        if OidIsValid(typid) {
            return typid;
        }
        l = lnext(activeSearchPath, l);
    }

    /* Not found in path */
    InvalidOid
}

/*
 * TypeIsVisible
 *		Determine whether a type (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified type name".
 */
pub unsafe fn TypeIsVisible(typid: Oid) -> bool {
    TypeIsVisibleExt(typid, core::ptr::null_mut())
}

/*
 * TypeIsVisibleExt
 *		As above, but if the type isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn TypeIsVisibleExt(typid: Oid, is_missing: *mut bool) -> bool {
    let typtup: HeapTuple;
    let typform: Form_pg_type;
    let typnamespace: Oid;
    let visible: bool;

    typtup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
    if !HeapTupleIsValid(typtup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for type {}", typid);
    }
    typform = GETSTRUCT(typtup) as Form_pg_type;

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    typnamespace = (*typform).typnamespace;
    if typnamespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, typnamespace)
    {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another type of the same name earlier in the path. So we
         * must do a slow check for conflicting types.
         */
        let typname: *mut c_char = NameStr(&(*typform).typname) as *mut c_char;
        let mut l: *mut ListCell;

        let mut vis = false;
        l = list_head(activeSearchPath);
        'vis_loop: while !l.is_null() {
            let namespaceId: Oid = lfirst_oid(l);

            if namespaceId == typnamespace {
                /* Found it first in path */
                vis = true;
                break 'vis_loop;
            }
            if SearchSysCacheExists2(
                TYPENAMENSP,
                PointerGetDatum(typname as *const c_void),
                ObjectIdGetDatum(namespaceId),
            ) {
                /* Found something else first in path */
                break 'vis_loop;
            }
            l = lnext(activeSearchPath, l);
        }
        visible = vis;
    }

    ReleaseSysCache(typtup);

    visible
}


/*
 * FuncnameGetCandidates
 *		Given a possibly-qualified function name and argument count,
 *		retrieve a list of the possible matches.
 *
 * If nargs is -1, we return all functions matching the given name,
 * regardless of argument count.  (argnames must be NIL, and expand_variadic
 * and expand_defaults must be false, in this case.)
 *
 * If argnames isn't NIL, we are considering a named- or mixed-notation call,
 * and only functions having all the listed argument names will be returned.
 * (We assume that length(argnames) <= nargs and all the passed-in names are
 * distinct.)  The returned structs will include an argnumbers array showing
 * the actual argument index for each logical argument position.
 *
 * If expand_variadic is true, then variadic functions having the same number
 * or fewer arguments will be retrieved, with the variadic argument and any
 * additional argument positions filled with the variadic element type.
 * nvargs in the returned struct is set to the number of such arguments.
 * If expand_variadic is false, variadic arguments are not treated specially,
 * and the returned nvargs will always be zero.
 *
 * If expand_defaults is true, functions that could match after insertion of
 * default argument values will also be retrieved.  In this case the returned
 * structs could have nargs > passed-in nargs, and ndargs is set to the number
 * of additional args (which can be retrieved from the function's
 * proargdefaults entry).
 *
 * If include_out_arguments is true, then OUT-mode arguments are considered to
 * be included in the argument list.  Their types are included in the returned
 * arrays, and argnumbers are indexes in proallargtypes not proargtypes.
 * We also set nominalnargs to be the length of proallargtypes not proargtypes.
 * Otherwise OUT-mode arguments are ignored.
 *
 * It is not possible for nvargs and ndargs to both be nonzero in the same
 * list entry, since default insertion allows matches to functions with more
 * than nargs arguments while the variadic transformation requires the same
 * number or less.
 *
 * When argnames isn't NIL, the returned args[] type arrays are not ordered
 * according to the functions' declarations, but rather according to the call:
 * first any positional arguments, then the named arguments, then defaulted
 * arguments (if needed and allowed by expand_defaults).  The argnumbers[]
 * array can be used to map this back to the catalog information.
 * argnumbers[k] is set to the proargtypes or proallargtypes index of the
 * k'th call argument.
 *
 * We search a single namespace if the function name is qualified, else
 * all namespaces in the search path.  In the multiple-namespace case,
 * we arrange for entries in earlier namespaces to mask identical entries in
 * later namespaces.
 *
 * When expanding variadics, we arrange for non-variadic functions to mask
 * variadic ones if the expanded argument list is the same.  It is still
 * possible for there to be conflicts between different variadic functions,
 * however.
 *
 * It is guaranteed that the return list will never contain multiple entries
 * with identical argument lists.  When expand_defaults is true, the entries
 * could have more than nargs positions, but we still guarantee that they are
 * distinct in the first nargs positions.  However, if argnames isn't NIL or
 * either expand_variadic or expand_defaults is true, there might be multiple
 * candidate functions that expand to identical argument lists.  Rather than
 * throw error here, we report such situations by returning a single entry
 * with oid = 0 that represents a set of such conflicting candidates.
 * The caller might end up discarding such an entry anyway, but if it selects
 * such an entry it should react as though the call were ambiguous.
 *
 * If missing_ok is true, an empty list (NULL) is returned if the name was
 * schema-qualified with a schema that does not exist.  Likewise if no
 * candidate is found for other reasons.
 */
pub unsafe fn FuncnameGetCandidates(
    names: *mut List,
    nargs: c_int,
    argnames: *mut List,
    expand_variadic: bool,
    expand_defaults: bool,
    include_out_arguments: bool,
    missing_ok: bool,
) -> FuncCandidateList {
    let mut resultList: FuncCandidateList = core::ptr::null_mut();
    let mut any_special: bool = false;
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut funcname: *mut c_char = core::ptr::null_mut();
    let namespaceId: Oid;
    let catlist: *mut CatCList;
    let mut i: c_int;

    /* check for caller error */
    Assert!(nargs >= 0 || !(expand_variadic | expand_defaults));

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &mut schemaname, &mut funcname);

    if !schemaname.is_null() {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if !OidIsValid(namespaceId) {
            return core::ptr::null_mut();
        }
    } else {
        /* flag to indicate we need namespace search */
        namespaceId = InvalidOid;
        recomputeNamespacePath();
    }

    /* Search syscache by name only */
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));

    i = 0;
    while i < (*catlist).n_members {
        let proctup: HeapTuple = &mut (*(*(*catlist).members.add(i as usize))).tuple
            as *mut HeapTupleData as HeapTuple;
        let procform: Form_pg_proc = GETSTRUCT(proctup) as Form_pg_proc;
        let mut proargtypes: *mut Oid = pg_proc_proargtypes_values(procform);
        let mut pronargs: c_int = (*procform).pronargs as c_int;
        let effective_nargs: c_int;
        let mut pathpos: c_int = 0;
        let variadic: bool;
        let use_defaults: bool;
        let va_elem_type: Oid;
        let mut argnumbers: *mut c_int = core::ptr::null_mut();
        let newResult: FuncCandidateList;

        if OidIsValid(namespaceId) {
            /* Consider only procs in specified namespace */
            if (*procform).pronamespace != namespaceId {
                i += 1;
                continue;
            }
        } else {
            /*
             * Consider only procs that are in the search path and are not in
             * the temp namespace.
             */
            let mut nsp: *mut ListCell;
            let mut found_nsp = false;

            nsp = list_head(activeSearchPath);
            while !nsp.is_null() {
                if (*procform).pronamespace == lfirst_oid(nsp)
                    && (*procform).pronamespace != myTempNamespace
                {
                    found_nsp = true;
                    break;
                }
                pathpos += 1;
                nsp = lnext(activeSearchPath, nsp);
            }
            if !found_nsp {
                i += 1;
                continue; /* proc is not in search path */
            }
        }

        /*
         * If we are asked to match to OUT arguments, then use the
         * proallargtypes array (which includes those); otherwise use
         * proargtypes (which doesn't).  Of course, if proallargtypes is null,
         * we always use proargtypes.
         */
        if include_out_arguments {
            let mut proallargtypes_datum: Datum = 0;
            let mut isNull: bool = false;

            proallargtypes_datum = SysCacheGetAttr(
                PROCNAMEARGSNSP,
                proctup,
                Anum_pg_proc_proallargtypes,
                &mut isNull,
            );
            if !isNull {
                let arr: *mut ArrayType = DatumGetArrayTypeP(proallargtypes_datum);

                pronargs = *ARR_DIMS(arr);
                if ARR_NDIM(arr) != 1
                    || pronargs < 0
                    || ARR_HASNULL(arr)
                    || ARR_ELEMTYPE(arr) != OIDOID
                {
                    elog!(ERROR, "proallargtypes is not a 1-D Oid array or it contains nulls");
                }
                Assert!(pronargs >= (*procform).pronargs as c_int);
                proargtypes = ARR_DATA_PTR(arr) as *mut Oid;
            }
        }

        if !argnames.is_null() && list_length(argnames) > 0 {
            /*
             * Call uses named or mixed notation
             *
             * Named or mixed notation can match a variadic function only if
             * expand_variadic is off; otherwise there is no way to match the
             * presumed-nameless parameters expanded from the variadic array.
             */
            if OidIsValid((*procform).provariadic) && expand_variadic {
                i += 1;
                continue;
            }
            va_elem_type = InvalidOid;
            variadic = false;

            /*
             * Check argument count.
             */
            Assert!(nargs >= 0); /* -1 not supported with argnames */

            if pronargs > nargs && expand_defaults {
                /* Ignore if not enough default expressions */
                if nargs + ((*procform).pronargdefaults as c_int) < pronargs {
                    i += 1;
                    continue;
                }
                use_defaults = true;
            } else {
                use_defaults = false;
            }

            /* Ignore if it doesn't match requested argument count */
            if pronargs != nargs && !use_defaults {
                i += 1;
                continue;
            }

            /* Check for argument name match, generate positional mapping */
            if !MatchNamedCall(
                proctup,
                nargs,
                argnames,
                include_out_arguments,
                pronargs,
                &mut argnumbers,
            ) {
                i += 1;
                continue;
            }

            /* Named argument matching is always "special" */
            any_special = true;
        } else {
            /*
             * Call uses positional notation
             *
             * Check if function is variadic, and get variadic element type if
             * so.  If expand_variadic is false, we should just ignore
             * variadic-ness.
             */
            if pronargs <= nargs && expand_variadic {
                va_elem_type = (*procform).provariadic;
                variadic = OidIsValid(va_elem_type);
                any_special |= variadic;
            } else {
                va_elem_type = InvalidOid;
                variadic = false;
            }

            /*
             * Check if function can match by using parameter defaults.
             */
            if pronargs > nargs && expand_defaults {
                /* Ignore if not enough default expressions */
                if nargs + ((*procform).pronargdefaults as c_int) < pronargs {
                    i += 1;
                    continue;
                }
                use_defaults = true;
                any_special = true;
            } else {
                use_defaults = false;
            }

            /* Ignore if it doesn't match requested argument count */
            if nargs >= 0 && pronargs != nargs && !variadic && !use_defaults {
                i += 1;
                continue;
            }
        }

        /*
         * We must compute the effective argument list so that we can easily
         * compare it to earlier results.  We waste a palloc cycle if it gets
         * masked by an earlier result, but really that's a pretty infrequent
         * case so it's not worth worrying about.
         */
        effective_nargs = Max(pronargs, nargs);
        newResult = palloc(
            core::mem::offset_of!(_FuncCandidateList, args)
                + (effective_nargs as usize) * core::mem::size_of::<Oid>(),
        ) as FuncCandidateList;
        (*newResult).pathpos = pathpos;
        (*newResult).oid = (*procform).oid;
        (*newResult).nominalnargs = pronargs;
        (*newResult).nargs = effective_nargs;
        (*newResult).argnumbers = argnumbers;
        if !argnumbers.is_null() {
            /* Re-order the argument types into call's logical order */
            for j in 0..pronargs as usize {
                (*newResult).args.as_mut_ptr().add(j)
                    .write(*proargtypes.add(*argnumbers.add(j) as usize));
            }
        } else {
            /* Simple positional case, just copy proargtypes as-is */
            memcpy(
                (*newResult).args.as_mut_ptr() as *mut c_void,
                proargtypes as *const c_void,
                (pronargs as usize) * core::mem::size_of::<Oid>(),
            );
        }
        if variadic {
            (*newResult).nvargs = effective_nargs - pronargs + 1;
            /* Expand variadic argument into N copies of element type */
            for j in (pronargs - 1) as usize..effective_nargs as usize {
                (*newResult).args.as_mut_ptr().add(j).write(va_elem_type);
            }
        } else {
            (*newResult).nvargs = 0;
        }
        (*newResult).ndargs = if use_defaults { pronargs - nargs } else { 0 };

        /*
         * Does it have the same arguments as something we already accepted?
         * If so, decide what to do to avoid returning duplicate argument
         * lists.  We can skip this check for the single-namespace case if no
         * special (named, variadic or defaults) match has been made, since
         * then the unique index on pg_proc guarantees all the matches have
         * different argument lists.
         */
        if !resultList.is_null() && (any_special || !OidIsValid(namespaceId)) {
            /*
             * If we have an ordered list from SearchSysCacheList (the normal
             * case), then any conflicting proc must immediately adjoin this
             * one in the list, so we only need to look at the newest result
             * item.  If we have an unordered list, we have to scan the whole
             * result list.  Also, if either the current candidate or any
             * previous candidate is a special match, we can't assume that
             * conflicts are adjacent.
             *
             * We ignore defaulted arguments in deciding what is a match.
             */
            let mut prevResult: FuncCandidateList;

            if (*catlist).ordered && !any_special {
                /* ndargs must be 0 if !any_special */
                if (*newResult).nargs == (*resultList).nargs
                    && memcmp(
                        (*newResult).args.as_ptr() as *const c_void,
                        (*resultList).args.as_ptr() as *const c_void,
                        ((*newResult).nargs as usize) * core::mem::size_of::<Oid>(),
                    ) == 0
                {
                    prevResult = resultList;
                } else {
                    prevResult = core::ptr::null_mut();
                }
            } else {
                let cmp_nargs: c_int = (*newResult).nargs - (*newResult).ndargs;
                prevResult = resultList;

                while !prevResult.is_null() {
                    if cmp_nargs == (*prevResult).nargs - (*prevResult).ndargs
                        && memcmp(
                            (*newResult).args.as_ptr() as *const c_void,
                            (*prevResult).args.as_ptr() as *const c_void,
                            (cmp_nargs as usize) * core::mem::size_of::<Oid>(),
                        ) == 0
                    {
                        break;
                    }
                    prevResult = (*prevResult).next;
                }
            }

            if !prevResult.is_null() {
                /*
                 * We have a match with a previous result.  Decide which one
                 * to keep, or mark it ambiguous if we can't decide.  The
                 * logic here is preference > 0 means prefer the old result,
                 * preference < 0 means prefer the new, preference = 0 means
                 * ambiguous.
                 */
                let preference: c_int;

                if pathpos != (*prevResult).pathpos {
                    /*
                     * Prefer the one that's earlier in the search path.
                     */
                    preference = pathpos - (*prevResult).pathpos;
                } else if variadic && (*prevResult).nvargs == 0 {
                    /*
                     * With variadic functions we could have, for example,
                     * both foo(numeric) and foo(variadic numeric[]) in the
                     * same namespace; if so we prefer the non-variadic match
                     * on efficiency grounds.
                     */
                    preference = 1;
                } else if !variadic && (*prevResult).nvargs > 0 {
                    preference = -1;
                } else {
                    /*----------
                     * We can't decide.  This can happen with, for example,
                     * both foo(numeric, variadic numeric[]) and
                     * foo(variadic numeric[]) in the same namespace, or
                     * both foo(int) and foo (int, int default something)
                     * in the same namespace, or both foo(a int, b text)
                     * and foo(b text, a int) in the same namespace.
                     *----------
                     */
                    preference = 0;
                }

                if preference > 0 {
                    /* keep previous result */
                    pfree(newResult as *mut c_void);
                    i += 1;
                    continue;
                } else if preference < 0 {
                    /* remove previous result from the list */
                    if prevResult == resultList {
                        resultList = (*prevResult).next;
                    } else {
                        let mut prevPrevResult: FuncCandidateList = resultList;

                        while !prevPrevResult.is_null() {
                            if prevResult == (*prevPrevResult).next {
                                (*prevPrevResult).next = (*prevResult).next;
                                break;
                            }
                            prevPrevResult = (*prevPrevResult).next;
                        }
                        Assert!(!prevPrevResult.is_null()); /* assert we found it */
                    }
                    pfree(prevResult as *mut c_void);
                    /* fall through to add newResult to list */
                } else {
                    /* mark old result as ambiguous, discard new */
                    (*prevResult).oid = InvalidOid;
                    pfree(newResult as *mut c_void);
                    i += 1;
                    continue;
                }
            }
        }

        /*
         * Okay to add it to result list
         */
        (*newResult).next = resultList;
        resultList = newResult;
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    resultList
}

/*
 * MatchNamedCall
 *		Given a pg_proc heap tuple and a call's list of argument names,
 *		check whether the function could match the call.
 *
 * The call could match if all supplied argument names are accepted by
 * the function, in positions after the last positional argument, and there
 * are defaults for all unsupplied arguments.
 *
 * If include_out_arguments is true, we are treating OUT arguments as
 * included in the argument list.  pronargs is the number of arguments
 * we're considering (the length of either proargtypes or proallargtypes).
 *
 * The number of positional arguments is nargs - list_length(argnames).
 * Note caller has already done basic checks on argument count.
 *
 * On match, return true and fill *argnumbers with a palloc'd array showing
 * the mapping from call argument positions to actual function argument
 * numbers.  Defaulted arguments are included in this map, at positions
 * after the last supplied argument.
 */
unsafe fn MatchNamedCall(
    proctup: HeapTuple,
    nargs: c_int,
    argnames: *mut List,
    include_out_arguments: bool,
    pronargs: c_int,
    argnumbers: *mut *mut c_int,
) -> bool {
    let procform: Form_pg_proc = GETSTRUCT(proctup) as Form_pg_proc;
    let numposargs: c_int = nargs - list_length(argnames);
    let pronallargs: c_int;
    let mut p_argtypes: *mut Oid = core::ptr::null_mut();
    let mut p_argnames: *mut *mut c_char = core::ptr::null_mut();
    let mut p_argmodes: *mut c_char = core::ptr::null_mut();
    let mut arggiven: [bool; FUNC_MAX_ARGS] = [false; FUNC_MAX_ARGS];
    let mut isnull: bool = false;
    let mut ap: c_int;  /* call args position */
    let mut pp: c_int;  /* proargs position */
    let mut lc: *mut ListCell;

    Assert!(!argnames.is_null() && list_length(argnames) > 0);
    Assert!(numposargs >= 0);
    Assert!(nargs <= pronargs);

    /* Ignore this function if its proargnames is null */
    SysCacheGetAttr(PROCOID, proctup, Anum_pg_proc_proargnames, &mut isnull);
    if isnull {
        return false;
    }

    /* OK, let's extract the argument names and types */
    pronallargs = get_func_arg_info(
        proctup,
        &mut p_argtypes,
        &mut p_argnames,
        &mut p_argmodes,
    );
    Assert!(!p_argnames.is_null());

    Assert!(if include_out_arguments {
        pronargs == pronallargs
    } else {
        pronargs <= pronallargs
    });

    /* initialize state for matching */
    *argnumbers = palloc((pronargs as usize) * core::mem::size_of::<c_int>()) as *mut c_int;
    memset(
        arggiven.as_mut_ptr() as *mut c_void,
        0,
        (pronargs as usize) * core::mem::size_of::<bool>(),
    );

    /* there are numposargs positional args before the named args */
    ap = 0;
    while ap < numposargs {
        *(*argnumbers).add(ap as usize) = ap;
        arggiven[ap as usize] = true;
        ap += 1;
    }

    /* now examine the named args */
    lc = list_head(argnames);
    while !lc.is_null() {
        let argname: *mut c_char = lfirst(lc) as *mut c_char;
        let mut found: bool = false;
        let mut ii: c_int = 0;

        pp = 0;
        while ii < pronallargs {
            /* consider only input params, except with include_out_arguments */
            if !include_out_arguments
                && !p_argmodes.is_null()
                && (*p_argmodes.add(ii as usize) != FUNC_PARAM_IN
                    && *p_argmodes.add(ii as usize) != FUNC_PARAM_INOUT
                    && *p_argmodes.add(ii as usize) != FUNC_PARAM_VARIADIC)
            {
                ii += 1;
                continue;
            }
            if !(*p_argnames.add(ii as usize)).is_null()
                && strcmp(*p_argnames.add(ii as usize), argname) == 0
            {
                /* fail if argname matches a positional argument */
                if arggiven[pp as usize] {
                    return false;
                }
                arggiven[pp as usize] = true;
                *(*argnumbers).add(ap as usize) = pp;
                found = true;
                break;
            }
            /* increase pp only for considered parameters */
            pp += 1;
            ii += 1;
        }
        /* if name isn't in proargnames, fail */
        if !found {
            return false;
        }
        ap += 1;
        lc = lnext(argnames, lc);
    }

    Assert!(ap == nargs); /* processed all actual parameters */

    /* Check for default arguments */
    if nargs < pronargs {
        let first_arg_with_default: c_int =
            pronargs - (*procform).pronargdefaults as c_int;

        pp = numposargs;
        while pp < pronargs {
            if arggiven[pp as usize] {
                pp += 1;
                continue;
            }
            /* fail if arg not given and no default available */
            if pp < first_arg_with_default {
                return false;
            }
            *(*argnumbers).add(ap as usize) = pp;
            ap += 1;
            pp += 1;
        }
    }

    Assert!(ap == pronargs); /* processed all function parameters */

    true
}

/*
 * FunctionIsVisible
 *		Determine whether a function (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified function name with exact argument matches".
 */
pub unsafe fn FunctionIsVisible(funcid: Oid) -> bool {
    FunctionIsVisibleExt(funcid, core::ptr::null_mut())
}

/*
 * FunctionIsVisibleExt
 *		As above, but if the function isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn FunctionIsVisibleExt(funcid: Oid, is_missing: *mut bool) -> bool {
    let proctup: HeapTuple;
    let procform: Form_pg_proc;
    let pronamespace: Oid;
    let visible: bool;

    proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if !HeapTupleIsValid(proctup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for function {}", funcid);
    }
    procform = GETSTRUCT(proctup) as Form_pg_proc;

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    pronamespace = (*procform).pronamespace;
    if pronamespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, pronamespace)
    {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another proc of the same name and arguments earlier in
         * the path.  So we must do a slow check to see if this is the same
         * proc that would be found by FuncnameGetCandidates.
         */
        let proname: *mut c_char = NameStr(&(*procform).proname) as *mut c_char;
        let nargs: c_int = (*procform).pronargs as c_int;
        let mut clist: FuncCandidateList;

        let mut vis = false;

        clist = FuncnameGetCandidates(
            list_make1(makeString(proname) as *mut c_void),
            nargs,
            core::ptr::null_mut(),
            false,
            false,
            false,
            false,
        );

        while !clist.is_null() {
            if memcmp(
                (*clist).args.as_ptr() as *const c_void,
                pg_proc_proargtypes_values(procform) as *const c_void,
                (nargs as usize) * core::mem::size_of::<Oid>(),
            ) == 0
            {
                /* Found the expected entry; is it the right proc? */
                vis = (*clist).oid == funcid;
                break;
            }
            clist = (*clist).next;
        }
        visible = vis;
    }

    ReleaseSysCache(proctup);

    visible
}

// ===========================================================================
// PART 4: OpernameGetOprid, OpernameGetCandidates, OperatorIsVisible[Ext],
//         OpclassnameGetOpcid, OpclassIsVisible[Ext],
//         OpfamilynameGetOpfid, OpfamilyIsVisible[Ext],
//         lookup_collation, CollationGetCollid, CollationIsVisible[Ext],
//         ConversionGetConid, ConversionIsVisible[Ext]
// ===========================================================================

/*
 * OpernameGetOprid
 *		Given a possibly-qualified operator name and exact input datatypes,
 *		look up the operator.  Returns InvalidOid if not found.
 *
 * Pass oprleft = InvalidOid for a prefix op.
 *
 * If the operator name is not schema-qualified, it is sought in the current
 * namespace search path.  If the name is schema-qualified and the given
 * schema does not exist, InvalidOid is returned.
 */
pub unsafe fn OpernameGetOprid(names: *mut List, oprleft: Oid, oprright: Oid) -> Oid {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut opername: *mut c_char = core::ptr::null_mut();
    let catlist: *mut CatCList;
    let mut l: *mut ListCell;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &mut schemaname, &mut opername);

    if !schemaname.is_null() {
        /* search only in exact schema given */
        let namespaceId: Oid;

        namespaceId = LookupExplicitNamespace(schemaname, true);
        if OidIsValid(namespaceId) {
            let opertup: HeapTuple;

            opertup = SearchSysCache4(
                OPERNAMENSP,
                CStringGetDatum(opername),
                ObjectIdGetDatum(oprleft),
                ObjectIdGetDatum(oprright),
                ObjectIdGetDatum(namespaceId),
            );
            if HeapTupleIsValid(opertup) {
                let operclass: Form_pg_operator = GETSTRUCT(opertup) as Form_pg_operator;
                let result: Oid = (*operclass).oid;

                ReleaseSysCache(opertup);
                return result;
            }
        }

        return InvalidOid;
    }

    /* Search syscache by name and argument types */
    catlist = SearchSysCacheList3(
        OPERNAMENSP,
        CStringGetDatum(opername),
        ObjectIdGetDatum(oprleft),
        ObjectIdGetDatum(oprright),
    );

    if (*catlist).n_members == 0 {
        /* no hope, fall out early */
        ReleaseSysCacheList(catlist);
        return InvalidOid;
    }

    /*
     * We have to find the list member that is first in the search path, if
     * there's more than one.  This doubly-nested loop looks ugly, but in
     * practice there should usually be few catlist members.
     */
    recomputeNamespacePath();

    l = list_head(activeSearchPath);
    while !l.is_null() {
        let namespaceId: Oid = lfirst_oid(l);
        let mut i: c_int = 0;

        if namespaceId == myTempNamespace {
            l = lnext(activeSearchPath, l);
            continue; /* do not look in temp namespace */
        }

        while i < (*catlist).n_members {
            let opertup: HeapTuple = &mut (*(*(*catlist).members.add(i as usize))).tuple
                as *mut HeapTupleData as HeapTuple;
            let operform: Form_pg_operator = GETSTRUCT(opertup) as Form_pg_operator;

            if (*operform).oprnamespace == namespaceId {
                let result: Oid = (*operform).oid;

                ReleaseSysCacheList(catlist);
                return result;
            }
            i += 1;
        }
        l = lnext(activeSearchPath, l);
    }

    ReleaseSysCacheList(catlist);
    InvalidOid
}

/*
 * OpernameGetCandidates
 *		Given a possibly-qualified operator name and operator kind,
 *		retrieve a list of the possible matches.
 *
 * If oprkind is '\0', we return all operators matching the given name,
 * regardless of arguments.
 *
 * We search a single namespace if the operator name is qualified, else
 * all namespaces in the search path.  The return list will never contain
 * multiple entries with identical argument lists --- in the multiple-
 * namespace case, we arrange for entries in earlier namespaces to mask
 * identical entries in later namespaces.
 *
 * The returned items always have two args[] entries --- the first will be
 * InvalidOid for a prefix oprkind.  nargs is always 2, too.
 */
pub unsafe fn OpernameGetCandidates(
    names: *mut List,
    oprkind: c_char,
    missing_schema_ok: bool,
) -> FuncCandidateList {
    let mut resultList: FuncCandidateList = core::ptr::null_mut();
    let mut resultSpace: *mut c_char = core::ptr::null_mut();
    let mut nextResult: usize = 0;
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut opername: *mut c_char = core::ptr::null_mut();
    let namespaceId: Oid;
    let catlist: *mut CatCList;
    let mut i: c_int;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &mut schemaname, &mut opername);

    if !schemaname.is_null() {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_schema_ok);
        if missing_schema_ok && !OidIsValid(namespaceId) {
            return core::ptr::null_mut();
        }
    } else {
        /* flag to indicate we need namespace search */
        namespaceId = InvalidOid;
        recomputeNamespacePath();
    }

    /* Search syscache by name only */
    catlist = SearchSysCacheList1(OPERNAMENSP, CStringGetDatum(opername));

    /*
     * In typical scenarios, most if not all of the operators found by the
     * catcache search will end up getting returned; and there can be quite a
     * few, for common operator names such as '=' or '+'.  To reduce the time
     * spent in palloc, we allocate the result space as an array large enough
     * to hold all the operators.
     */
    if (*catlist).n_members > 0 {
        resultSpace = palloc(((*catlist).n_members as usize) * space_per_op()) as *mut c_char;
    }

    i = 0;
    while i < (*catlist).n_members {
        let opertup: HeapTuple = &mut (*(*(*catlist).members.add(i as usize))).tuple
            as *mut HeapTupleData as HeapTuple;
        let operform: Form_pg_operator = GETSTRUCT(opertup) as Form_pg_operator;
        let mut pathpos: c_int = 0;
        let newResult: FuncCandidateList;

        /* Ignore operators of wrong kind, if specific kind requested */
        if oprkind != 0 && (*operform).oprkind != oprkind {
            i += 1;
            continue;
        }

        if OidIsValid(namespaceId) {
            /* Consider only opers in specified namespace */
            if (*operform).oprnamespace != namespaceId {
                i += 1;
                continue;
            }
            /* No need to check args, they must all be different */
        } else {
            /*
             * Consider only opers that are in the search path and are not in
             * the temp namespace.
             */
            let mut nsp: *mut ListCell;
            let mut found_nsp = false;

            nsp = list_head(activeSearchPath);
            while !nsp.is_null() {
                if (*operform).oprnamespace == lfirst_oid(nsp)
                    && (*operform).oprnamespace != myTempNamespace
                {
                    found_nsp = true;
                    break;
                }
                pathpos += 1;
                nsp = lnext(activeSearchPath, nsp);
            }
            if !found_nsp {
                i += 1;
                continue; /* oper is not in search path */
            }

            /*
             * Okay, it's in the search path, but does it have the same
             * arguments as something we already accepted?  If so, keep only
             * the one that appears earlier in the search path.
             */
            if !resultList.is_null() {
                let mut prevResult: FuncCandidateList;

                if (*catlist).ordered {
                    if (*operform).oprleft == (*resultList).args[0]
                        && (*operform).oprright == *(*resultList).args.as_ptr().add(1)
                    {
                        prevResult = resultList;
                    } else {
                        prevResult = core::ptr::null_mut();
                    }
                } else {
                    prevResult = resultList;
                    while !prevResult.is_null() {
                        if (*operform).oprleft == (*prevResult).args[0]
                            && (*operform).oprright == *(*prevResult).args.as_ptr().add(1)
                        {
                            break;
                        }
                        prevResult = (*prevResult).next;
                    }
                }
                if !prevResult.is_null() {
                    /* We have a match with a previous result */
                    Assert!(pathpos != (*prevResult).pathpos);
                    if pathpos > (*prevResult).pathpos {
                        i += 1;
                        continue; /* keep previous result */
                    }
                    /* replace previous result */
                    (*prevResult).pathpos = pathpos;
                    (*prevResult).oid = (*operform).oid;
                    i += 1;
                    continue; /* args are same, of course */
                }
            }
        }

        /*
         * Okay to add it to result list
         */
        newResult = resultSpace.add(nextResult) as FuncCandidateList;
        nextResult += space_per_op();

        (*newResult).pathpos = pathpos;
        (*newResult).oid = (*operform).oid;
        (*newResult).nominalnargs = 2;
        (*newResult).nargs = 2;
        (*newResult).nvargs = 0;
        (*newResult).ndargs = 0;
        (*newResult).argnumbers = core::ptr::null_mut();
        (*newResult).args[0] = (*operform).oprleft;
        (*newResult).args.as_mut_ptr().add(1).write((*operform).oprright);
        (*newResult).next = resultList;
        resultList = newResult;
        i += 1;
    }

    ReleaseSysCacheList(catlist);

    resultList
}

/*
 * OperatorIsVisible
 *		Determine whether an operator (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified operator name with exact argument matches".
 */
pub unsafe fn OperatorIsVisible(oprid: Oid) -> bool {
    OperatorIsVisibleExt(oprid, core::ptr::null_mut())
}

/*
 * OperatorIsVisibleExt
 *		As above, but if the operator isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn OperatorIsVisibleExt(oprid: Oid, is_missing: *mut bool) -> bool {
    let oprtup: HeapTuple;
    let oprform: Form_pg_operator;
    let oprnamespace: Oid;
    let visible: bool;

    oprtup = SearchSysCache1(OPEROID, ObjectIdGetDatum(oprid));
    if !HeapTupleIsValid(oprtup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for operator {}", oprid);
    }
    oprform = GETSTRUCT(oprtup) as Form_pg_operator;

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    oprnamespace = (*oprform).oprnamespace;
    if oprnamespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, oprnamespace)
    {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another operator of the same name and arguments earlier
         * in the path.  So we must do a slow check to see if this is the same
         * operator that would be found by OpernameGetOprid.
         */
        let oprname: *mut c_char = NameStr(&(*oprform).oprname) as *mut c_char;

        visible = OpernameGetOprid(
            list_make1(makeString(oprname) as *mut c_void),
            (*oprform).oprleft,
            (*oprform).oprright,
        ) == oprid;
    }

    ReleaseSysCache(oprtup);

    visible
}


/*
 * OpclassnameGetOpcid
 *		Try to resolve an unqualified index opclass name.
 *		Returns OID if opclass found in search path, else InvalidOid.
 *
 * This is essentially the same as TypenameGetTypid, but we have to have
 * an extra argument for the index AM OID.
 */
pub unsafe fn OpclassnameGetOpcid(amid: Oid, opcname: *const c_char) -> Oid {
    let mut opcid: Oid;
    let mut l: *mut ListCell;

    recomputeNamespacePath();

    l = list_head(activeSearchPath);
    while !l.is_null() {
        let namespaceId: Oid = lfirst_oid(l);

        if namespaceId == myTempNamespace {
            l = lnext(activeSearchPath, l);
            continue; /* do not look in temp namespace */
        }

        opcid = GetSysCacheOid3(
            CLAAMNAMENSP,
            Anum_pg_opclass_oid,
            ObjectIdGetDatum(amid),
            PointerGetDatum(opcname as *const c_void),
            ObjectIdGetDatum(namespaceId),
        );
        if OidIsValid(opcid) {
            return opcid;
        }
        l = lnext(activeSearchPath, l);
    }

    /* Not found in path */
    InvalidOid
}

/*
 * OpclassIsVisible
 *		Determine whether an opclass (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified opclass name".
 */
pub unsafe fn OpclassIsVisible(opcid: Oid) -> bool {
    OpclassIsVisibleExt(opcid, core::ptr::null_mut())
}

/*
 * OpclassIsVisibleExt
 *		As above, but if the opclass isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn OpclassIsVisibleExt(opcid: Oid, is_missing: *mut bool) -> bool {
    let opctup: HeapTuple;
    let opcform: Form_pg_opclass;
    let opcnamespace: Oid;
    let visible: bool;

    opctup = SearchSysCache1(CLAOID, ObjectIdGetDatum(opcid));
    if !HeapTupleIsValid(opctup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for opclass {}", opcid);
    }
    opcform = GETSTRUCT(opctup) as Form_pg_opclass;

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    opcnamespace = (*opcform).opcnamespace;
    if opcnamespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, opcnamespace)
    {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another opclass of the same name earlier in the path. So
         * we must do a slow check to see if this opclass would be found by
         * OpclassnameGetOpcid.
         */
        let opcname: *mut c_char = NameStr(&(*opcform).opcname) as *mut c_char;

        visible = OpclassnameGetOpcid((*opcform).opcmethod, opcname) == opcid;
    }

    ReleaseSysCache(opctup);

    visible
}

/*
 * OpfamilynameGetOpfid
 *		Try to resolve an unqualified index opfamily name.
 *		Returns OID if opfamily found in search path, else InvalidOid.
 *
 * This is essentially the same as TypenameGetTypid, but we have to have
 * an extra argument for the index AM OID.
 */
pub unsafe fn OpfamilynameGetOpfid(amid: Oid, opfname: *const c_char) -> Oid {
    let mut opfid: Oid;
    let mut l: *mut ListCell;

    recomputeNamespacePath();

    l = list_head(activeSearchPath);
    while !l.is_null() {
        let namespaceId: Oid = lfirst_oid(l);

        if namespaceId == myTempNamespace {
            l = lnext(activeSearchPath, l);
            continue; /* do not look in temp namespace */
        }

        opfid = GetSysCacheOid3(
            OPFAMILYAMNAMENSP,
            Anum_pg_opfamily_oid,
            ObjectIdGetDatum(amid),
            PointerGetDatum(opfname as *const c_void),
            ObjectIdGetDatum(namespaceId),
        );
        if OidIsValid(opfid) {
            return opfid;
        }
        l = lnext(activeSearchPath, l);
    }

    /* Not found in path */
    InvalidOid
}

/*
 * OpfamilyIsVisible
 *		Determine whether an opfamily (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified opfamily name".
 */
pub unsafe fn OpfamilyIsVisible(opfid: Oid) -> bool {
    OpfamilyIsVisibleExt(opfid, core::ptr::null_mut())
}

/*
 * OpfamilyIsVisibleExt
 *		As above, but if the opfamily isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn OpfamilyIsVisibleExt(opfid: Oid, is_missing: *mut bool) -> bool {
    let opftup: HeapTuple;
    let opfform: Form_pg_opfamily;
    let opfnamespace: Oid;
    let visible: bool;

    opftup = SearchSysCache1(OPFAMILYOID, ObjectIdGetDatum(opfid));
    if !HeapTupleIsValid(opftup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for opfamily {}", opfid);
    }
    opfform = GETSTRUCT(opftup) as Form_pg_opfamily;

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    opfnamespace = (*opfform).opfnamespace;
    if opfnamespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, opfnamespace)
    {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another opfamily of the same name earlier in the path. So
         * we must do a slow check to see if this opfamily would be found by
         * OpfamilynameGetOpfid.
         */
        let opfname: *mut c_char = NameStr(&(*opfform).opfname) as *mut c_char;

        visible = OpfamilynameGetOpfid((*opfform).opfmethod, opfname) == opfid;
    }

    ReleaseSysCache(opftup);

    visible
}

/*
 * lookup_collation
 *		If there's a collation of the given name/namespace, and it works
 *		with the given encoding, return its OID.  Else return InvalidOid.
 */
unsafe fn lookup_collation(
    collname: *const c_char,
    collnamespace: Oid,
    encoding: i32,
) -> Oid {
    let mut collid: Oid;
    let colltup: HeapTuple;
    let collform: Form_pg_collation;

    /* Check for encoding-specific entry (exact match) */
    collid = GetSysCacheOid3(
        COLLNAMEENCNSP,
        Anum_pg_collation_oid,
        PointerGetDatum(collname as *const c_void),
        Int32GetDatum(encoding),
        ObjectIdGetDatum(collnamespace),
    );
    if OidIsValid(collid) {
        return collid;
    }

    /*
     * Check for any-encoding entry.  This takes a bit more work: while libc
     * collations with collencoding = -1 do work with all encodings, ICU
     * collations only work with certain encodings, so we have to check that
     * aspect before deciding it's a match.
     */
    colltup = SearchSysCache3(
        COLLNAMEENCNSP,
        PointerGetDatum(collname as *const c_void),
        Int32GetDatum(-1),
        ObjectIdGetDatum(collnamespace),
    );
    if !HeapTupleIsValid(colltup) {
        return InvalidOid;
    }
    collform = GETSTRUCT(colltup) as Form_pg_collation;
    if (*collform).collprovider == COLLPROVIDER_ICU {
        if is_encoding_supported_by_icu(encoding) {
            collid = (*collform).oid;
        } else {
            collid = InvalidOid;
        }
    } else {
        collid = (*collform).oid;
    }
    ReleaseSysCache(colltup);
    collid
}

// COLLPROVIDER_ICU constant (pg_collation.h)
const COLLPROVIDER_ICU: c_char = b'i' as c_char;

/*
 * CollationGetCollid
 *		Try to resolve an unqualified collation name.
 *		Returns OID if collation found in search path, else InvalidOid.
 *
 * Note that this will only find collations that work with the current
 * database's encoding.
 */
pub unsafe fn CollationGetCollid(collname: *const c_char) -> Oid {
    let dbencoding: i32 = GetDatabaseEncoding();
    let mut l: *mut ListCell;

    recomputeNamespacePath();

    l = list_head(activeSearchPath);
    while !l.is_null() {
        let namespaceId: Oid = lfirst_oid(l);
        let collid: Oid;

        if namespaceId == myTempNamespace {
            l = lnext(activeSearchPath, l);
            continue; /* do not look in temp namespace */
        }

        collid = lookup_collation(collname, namespaceId, dbencoding);
        if OidIsValid(collid) {
            return collid;
        }
        l = lnext(activeSearchPath, l);
    }

    /* Not found in path */
    InvalidOid
}

/*
 * CollationIsVisible
 *		Determine whether a collation (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified collation name".
 *
 * Note that only collations that work with the current database's encoding
 * will be considered visible.
 */
pub unsafe fn CollationIsVisible(collid: Oid) -> bool {
    CollationIsVisibleExt(collid, core::ptr::null_mut())
}

/*
 * CollationIsVisibleExt
 *		As above, but if the collation isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn CollationIsVisibleExt(collid: Oid, is_missing: *mut bool) -> bool {
    let colltup: HeapTuple;
    let collform: Form_pg_collation;
    let collnamespace: Oid;
    let visible: bool;

    colltup = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
    if !HeapTupleIsValid(colltup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for collation {}", collid);
    }
    collform = GETSTRUCT(colltup) as Form_pg_collation;

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    collnamespace = (*collform).collnamespace;
    if collnamespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, collnamespace)
    {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another collation of the same name earlier in the path,
         * or it might not work with the current DB encoding.  So we must do a
         * slow check to see if this collation would be found by
         * CollationGetCollid.
         */
        let collname: *mut c_char = NameStr(&(*collform).collname) as *mut c_char;

        visible = CollationGetCollid(collname) == collid;
    }

    ReleaseSysCache(colltup);

    visible
}


/*
 * ConversionGetConid
 *		Try to resolve an unqualified conversion name.
 *		Returns OID if conversion found in search path, else InvalidOid.
 *
 * This is essentially the same as RelnameGetRelid.
 */
pub unsafe fn ConversionGetConid(conname: *const c_char) -> Oid {
    let mut conid: Oid;
    let mut l: *mut ListCell;

    recomputeNamespacePath();

    l = list_head(activeSearchPath);
    while !l.is_null() {
        let namespaceId: Oid = lfirst_oid(l);

        if namespaceId == myTempNamespace {
            l = lnext(activeSearchPath, l);
            continue; /* do not look in temp namespace */
        }

        conid = GetSysCacheOid2(
            CONNAMENSP,
            Anum_pg_conversion_oid,
            PointerGetDatum(conname as *const c_void),
            ObjectIdGetDatum(namespaceId),
        );
        if OidIsValid(conid) {
            return conid;
        }
        l = lnext(activeSearchPath, l);
    }

    /* Not found in path */
    InvalidOid
}

/*
 * ConversionIsVisible
 *		Determine whether a conversion (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified conversion name".
 */
pub unsafe fn ConversionIsVisible(conid: Oid) -> bool {
    ConversionIsVisibleExt(conid, core::ptr::null_mut())
}

/*
 * ConversionIsVisibleExt
 *		As above, but if the conversion isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn ConversionIsVisibleExt(conid: Oid, is_missing: *mut bool) -> bool {
    let contup: HeapTuple;
    let conform: Form_pg_conversion;
    let connamespace: Oid;
    let visible: bool;

    contup = SearchSysCache1(CONVOID, ObjectIdGetDatum(conid));
    if !HeapTupleIsValid(contup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for conversion {}", conid);
    }
    conform = GETSTRUCT(contup) as Form_pg_conversion;

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    connamespace = (*conform).connamespace;
    if connamespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, connamespace)
    {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another conversion of the same name earlier in the path.
         * So we must do a slow check to see if this conversion would be found
         * by ConversionGetConid.
         */
        let conname: *mut c_char = NameStr(&(*conform).conname) as *mut c_char;

        visible = ConversionGetConid(conname) == conid;
    }

    ReleaseSysCache(contup);

    visible
}

// ===========================================================================
// PART 5: get_statistics_object_oid, StatisticsObjIsVisible[Ext],
//         TS parser/dict/template/config lookups + IsVisible[Ext],
//         DeconstructQualifiedName, LookupNamespaceNoError,
//         LookupExplicitNamespace, LookupCreationNamespace,
//         CheckSetNamespace, QualifiedNameGetCreationNamespace,
//         get_namespace_oid, makeRangeVarFromNameList,
//         NameListToString, NameListToQuotedString
// ===========================================================================

/*
 * get_statistics_object_oid - find a statistics object by possibly qualified name
 *
 * If not found, returns InvalidOid if missing_ok, else throws error
 */
pub unsafe fn get_statistics_object_oid(names: *mut List, missing_ok: bool) -> Oid {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut stats_name: *mut c_char = core::ptr::null_mut();
    let namespaceId: Oid;
    let mut stats_oid: Oid = InvalidOid;
    let mut l: *mut ListCell;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &mut schemaname, &mut stats_name);

    if !schemaname.is_null() {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if missing_ok && !OidIsValid(namespaceId) {
            stats_oid = InvalidOid;
        } else {
            stats_oid = GetSysCacheOid2(
                STATEXTNAMENSP,
                Anum_pg_statistic_ext_oid,
                PointerGetDatum(stats_name as *const c_void),
                ObjectIdGetDatum(namespaceId),
            );
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath();

        l = list_head(activeSearchPath);
        while !l.is_null() {
            let ns: Oid = lfirst_oid(l);

            if ns == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }
            stats_oid = GetSysCacheOid2(
                STATEXTNAMENSP,
                Anum_pg_statistic_ext_oid,
                PointerGetDatum(stats_name as *const c_void),
                ObjectIdGetDatum(ns),
            );
            if OidIsValid(stats_oid) {
                break;
            }
            l = lnext(activeSearchPath, l);
        }
    }

    if !OidIsValid(stats_oid) && !missing_ok {
        ereport!(ERROR, errmsg!(
                "statistics object \"{}\" does not exist",
                CStr::from_ptr(NameListToString(names)).to_string_lossy()
            )) /* C also: errcode */;
    }

    stats_oid
}

/*
 * StatisticsObjIsVisible
 *		Determine whether a statistics object (identified by OID) is visible in
 *		the current search path.  Visible means "would be found by searching
 *		for the unqualified statistics object name".
 */
pub unsafe fn StatisticsObjIsVisible(stxid: Oid) -> bool {
    StatisticsObjIsVisibleExt(stxid, core::ptr::null_mut())
}

/*
 * StatisticsObjIsVisibleExt
 *		As above, but if the statistics object isn't found and is_missing is
 *		not NULL, then set *is_missing = true and return false instead of
 *		throwing an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn StatisticsObjIsVisibleExt(stxid: Oid, is_missing: *mut bool) -> bool {
    let stxtup: HeapTuple;
    let stxform: Form_pg_statistic_ext;
    let stxnamespace: Oid;
    let visible: bool;

    stxtup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(stxid));
    if !HeapTupleIsValid(stxtup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for statistics object {}", stxid);
    }
    stxform = GETSTRUCT(stxtup) as Form_pg_statistic_ext;

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    stxnamespace = (*stxform).stxnamespace;
    if stxnamespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, stxnamespace)
    {
        visible = false;
    } else {
        /*
         * If it is in the path, it might still not be visible; it could be
         * hidden by another statistics object of the same name earlier in the
         * path. So we must do a slow check for conflicting objects.
         */
        let stxname: *mut c_char = NameStr(&(*stxform).stxname) as *mut c_char;
        let mut l: *mut ListCell;

        let mut vis = false;
        l = list_head(activeSearchPath);
        'vis_loop: while !l.is_null() {
            let namespaceId: Oid = lfirst_oid(l);

            if namespaceId == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            if namespaceId == stxnamespace {
                /* Found it first in path */
                vis = true;
                break 'vis_loop;
            }
            if SearchSysCacheExists2(
                STATEXTNAMENSP,
                PointerGetDatum(stxname as *const c_void),
                ObjectIdGetDatum(namespaceId),
            ) {
                /* Found something else first in path */
                break 'vis_loop;
            }
            l = lnext(activeSearchPath, l);
        }
        visible = vis;
    }

    ReleaseSysCache(stxtup);

    visible
}

/*
 * get_ts_parser_oid - find a TS parser by possibly qualified name
 *
 * If not found, returns InvalidOid if missing_ok, else throws error
 */
pub unsafe fn get_ts_parser_oid(names: *mut List, missing_ok: bool) -> Oid {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut parser_name: *mut c_char = core::ptr::null_mut();
    let namespaceId: Oid;
    let mut prsoid: Oid = InvalidOid;
    let mut l: *mut ListCell;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &mut schemaname, &mut parser_name);

    if !schemaname.is_null() {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if missing_ok && !OidIsValid(namespaceId) {
            prsoid = InvalidOid;
        } else {
            prsoid = GetSysCacheOid2(
                TSPARSERNAMENSP,
                Anum_pg_ts_parser_oid,
                PointerGetDatum(parser_name as *const c_void),
                ObjectIdGetDatum(namespaceId),
            );
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath();

        l = list_head(activeSearchPath);
        while !l.is_null() {
            let ns: Oid = lfirst_oid(l);

            if ns == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            prsoid = GetSysCacheOid2(
                TSPARSERNAMENSP,
                Anum_pg_ts_parser_oid,
                PointerGetDatum(parser_name as *const c_void),
                ObjectIdGetDatum(ns),
            );
            if OidIsValid(prsoid) {
                break;
            }
            l = lnext(activeSearchPath, l);
        }
    }

    if !OidIsValid(prsoid) && !missing_ok {
        ereport!(ERROR, errmsg!(
                "text search parser \"{}\" does not exist",
                CStr::from_ptr(NameListToString(names)).to_string_lossy()
            )) /* C also: errcode */;
    }

    prsoid
}

/*
 * TSParserIsVisible
 *		Determine whether a parser (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified parser name".
 */
pub unsafe fn TSParserIsVisible(prsId: Oid) -> bool {
    TSParserIsVisibleExt(prsId, core::ptr::null_mut())
}

/*
 * TSParserIsVisibleExt
 *		As above, but if the parser isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn TSParserIsVisibleExt(prsId: Oid, is_missing: *mut bool) -> bool {
    let tup: HeapTuple;
    let form: Form_pg_ts_parser;
    let namespace: Oid;
    let visible: bool;

    tup = SearchSysCache1(TSPARSEROID, ObjectIdGetDatum(prsId));
    if !HeapTupleIsValid(tup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for text search parser {}", prsId);
    }
    form = GETSTRUCT(tup) as Form_pg_ts_parser;

    recomputeNamespacePath();

    /*
     * Quick check: if it ain't in the path at all, it ain't visible. Items in
     * the system namespace are surely in the path and so we needn't even do
     * list_member_oid() for them.
     */
    namespace = (*form).prsnamespace;
    if namespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, namespace)
    {
        visible = false;
    } else {
        let name: *mut c_char = NameStr(&(*form).prsname) as *mut c_char;
        let mut l: *mut ListCell;

        let mut vis = false;
        l = list_head(activeSearchPath);
        'vis_loop: while !l.is_null() {
            let namespaceId: Oid = lfirst_oid(l);

            if namespaceId == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            if namespaceId == namespace {
                /* Found it first in path */
                vis = true;
                break 'vis_loop;
            }
            if SearchSysCacheExists2(
                TSPARSERNAMENSP,
                PointerGetDatum(name as *const c_void),
                ObjectIdGetDatum(namespaceId),
            ) {
                /* Found something else first in path */
                break 'vis_loop;
            }
            l = lnext(activeSearchPath, l);
        }
        visible = vis;
    }

    ReleaseSysCache(tup);

    visible
}

/*
 * get_ts_dict_oid - find a TS dictionary by possibly qualified name
 *
 * If not found, returns InvalidOid if missing_ok, else throws error
 */
pub unsafe fn get_ts_dict_oid(names: *mut List, missing_ok: bool) -> Oid {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut dict_name: *mut c_char = core::ptr::null_mut();
    let namespaceId: Oid;
    let mut dictoid: Oid = InvalidOid;
    let mut l: *mut ListCell;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &mut schemaname, &mut dict_name);

    if !schemaname.is_null() {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if missing_ok && !OidIsValid(namespaceId) {
            dictoid = InvalidOid;
        } else {
            dictoid = GetSysCacheOid2(
                TSDICTNAMENSP,
                Anum_pg_ts_dict_oid,
                PointerGetDatum(dict_name as *const c_void),
                ObjectIdGetDatum(namespaceId),
            );
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath();

        l = list_head(activeSearchPath);
        while !l.is_null() {
            let ns: Oid = lfirst_oid(l);

            if ns == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            dictoid = GetSysCacheOid2(
                TSDICTNAMENSP,
                Anum_pg_ts_dict_oid,
                PointerGetDatum(dict_name as *const c_void),
                ObjectIdGetDatum(ns),
            );
            if OidIsValid(dictoid) {
                break;
            }
            l = lnext(activeSearchPath, l);
        }
    }

    if !OidIsValid(dictoid) && !missing_ok {
        ereport!(ERROR, errmsg!(
                "text search dictionary \"{}\" does not exist",
                CStr::from_ptr(NameListToString(names)).to_string_lossy()
            )) /* C also: errcode */;
    }

    dictoid
}

/*
 * TSDictionaryIsVisible
 *		Determine whether a dictionary (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified dictionary name".
 */
pub unsafe fn TSDictionaryIsVisible(dictId: Oid) -> bool {
    TSDictionaryIsVisibleExt(dictId, core::ptr::null_mut())
}

/*
 * TSDictionaryIsVisibleExt
 *		As above, but if the dictionary isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn TSDictionaryIsVisibleExt(dictId: Oid, is_missing: *mut bool) -> bool {
    let tup: HeapTuple;
    let form: Form_pg_ts_dict;
    let namespace: Oid;
    let visible: bool;

    tup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dictId));
    if !HeapTupleIsValid(tup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for text search dictionary {}", dictId);
    }
    form = GETSTRUCT(tup) as Form_pg_ts_dict;

    recomputeNamespacePath();

    namespace = (*form).dictnamespace;
    if namespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, namespace)
    {
        visible = false;
    } else {
        let name: *mut c_char = NameStr(&(*form).dictname) as *mut c_char;
        let mut l: *mut ListCell;

        let mut vis = false;
        l = list_head(activeSearchPath);
        'vis_loop: while !l.is_null() {
            let namespaceId: Oid = lfirst_oid(l);

            if namespaceId == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            if namespaceId == namespace {
                /* Found it first in path */
                vis = true;
                break 'vis_loop;
            }
            if SearchSysCacheExists2(
                TSDICTNAMENSP,
                PointerGetDatum(name as *const c_void),
                ObjectIdGetDatum(namespaceId),
            ) {
                /* Found something else first in path */
                break 'vis_loop;
            }
            l = lnext(activeSearchPath, l);
        }
        visible = vis;
    }

    ReleaseSysCache(tup);

    visible
}

/*
 * get_ts_template_oid - find a TS template by possibly qualified name
 *
 * If not found, returns InvalidOid if missing_ok, else throws error
 */
pub unsafe fn get_ts_template_oid(names: *mut List, missing_ok: bool) -> Oid {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut template_name: *mut c_char = core::ptr::null_mut();
    let namespaceId: Oid;
    let mut tmploid: Oid = InvalidOid;
    let mut l: *mut ListCell;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &mut schemaname, &mut template_name);

    if !schemaname.is_null() {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if missing_ok && !OidIsValid(namespaceId) {
            tmploid = InvalidOid;
        } else {
            tmploid = GetSysCacheOid2(
                TSTEMPLATENAMENSP,
                Anum_pg_ts_template_oid,
                PointerGetDatum(template_name as *const c_void),
                ObjectIdGetDatum(namespaceId),
            );
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath();

        l = list_head(activeSearchPath);
        while !l.is_null() {
            let ns: Oid = lfirst_oid(l);

            if ns == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            tmploid = GetSysCacheOid2(
                TSTEMPLATENAMENSP,
                Anum_pg_ts_template_oid,
                PointerGetDatum(template_name as *const c_void),
                ObjectIdGetDatum(ns),
            );
            if OidIsValid(tmploid) {
                break;
            }
            l = lnext(activeSearchPath, l);
        }
    }

    if !OidIsValid(tmploid) && !missing_ok {
        ereport!(ERROR, errmsg!(
                "text search template \"{}\" does not exist",
                CStr::from_ptr(NameListToString(names)).to_string_lossy()
            )) /* C also: errcode */;
    }

    tmploid
}

/*
 * TSTemplateIsVisible
 *		Determine whether a template (identified by OID) is visible in the
 *		current search path.  Visible means "would be found by searching
 *		for the unqualified template name".
 */
pub unsafe fn TSTemplateIsVisible(tmplId: Oid) -> bool {
    TSTemplateIsVisibleExt(tmplId, core::ptr::null_mut())
}

/*
 * TSTemplateIsVisibleExt
 *		As above, but if the template isn't found and is_missing is not NULL,
 *		then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn TSTemplateIsVisibleExt(tmplId: Oid, is_missing: *mut bool) -> bool {
    let tup: HeapTuple;
    let form: Form_pg_ts_template;
    let namespace: Oid;
    let visible: bool;

    tup = SearchSysCache1(TSTEMPLATEOID, ObjectIdGetDatum(tmplId));
    if !HeapTupleIsValid(tup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for text search template {}", tmplId);
    }
    form = GETSTRUCT(tup) as Form_pg_ts_template;

    recomputeNamespacePath();

    namespace = (*form).tmplnamespace;
    if namespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, namespace)
    {
        visible = false;
    } else {
        let name: *mut c_char = NameStr(&(*form).tmplname) as *mut c_char;
        let mut l: *mut ListCell;

        let mut vis = false;
        l = list_head(activeSearchPath);
        'vis_loop: while !l.is_null() {
            let namespaceId: Oid = lfirst_oid(l);

            if namespaceId == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            if namespaceId == namespace {
                /* Found it first in path */
                vis = true;
                break 'vis_loop;
            }
            if SearchSysCacheExists2(
                TSTEMPLATENAMENSP,
                PointerGetDatum(name as *const c_void),
                ObjectIdGetDatum(namespaceId),
            ) {
                /* Found something else first in path */
                break 'vis_loop;
            }
            l = lnext(activeSearchPath, l);
        }
        visible = vis;
    }

    ReleaseSysCache(tup);

    visible
}

/*
 * get_ts_config_oid - find a TS config by possibly qualified name
 *
 * If not found, returns InvalidOid if missing_ok, else throws error
 */
pub unsafe fn get_ts_config_oid(names: *mut List, missing_ok: bool) -> Oid {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut config_name: *mut c_char = core::ptr::null_mut();
    let namespaceId: Oid;
    let mut cfgoid: Oid = InvalidOid;
    let mut l: *mut ListCell;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &mut schemaname, &mut config_name);

    if !schemaname.is_null() {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if missing_ok && !OidIsValid(namespaceId) {
            cfgoid = InvalidOid;
        } else {
            cfgoid = GetSysCacheOid2(
                TSCONFIGNAMENSP,
                Anum_pg_ts_config_oid,
                PointerGetDatum(config_name as *const c_void),
                ObjectIdGetDatum(namespaceId),
            );
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath();

        l = list_head(activeSearchPath);
        while !l.is_null() {
            let ns: Oid = lfirst_oid(l);

            if ns == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            cfgoid = GetSysCacheOid2(
                TSCONFIGNAMENSP,
                Anum_pg_ts_config_oid,
                PointerGetDatum(config_name as *const c_void),
                ObjectIdGetDatum(ns),
            );
            if OidIsValid(cfgoid) {
                break;
            }
            l = lnext(activeSearchPath, l);
        }
    }

    if !OidIsValid(cfgoid) && !missing_ok {
        ereport!(ERROR, errmsg!(
                "text search configuration \"{}\" does not exist",
                CStr::from_ptr(NameListToString(names)).to_string_lossy()
            )) /* C also: errcode */;
    }

    cfgoid
}

/*
 * TSConfigIsVisible
 *		Determine whether a text search configuration (identified by OID)
 *		is visible in the current search path.  Visible means "would be found
 *		by searching for the unqualified text search configuration name".
 */
pub unsafe fn TSConfigIsVisible(cfgid: Oid) -> bool {
    TSConfigIsVisibleExt(cfgid, core::ptr::null_mut())
}

/*
 * TSConfigIsVisibleExt
 *		As above, but if the configuration isn't found and is_missing is not
 *		NULL, then set *is_missing = true and return false instead of throwing
 *		an error.  (Caller must initialize *is_missing = false.)
 */
unsafe fn TSConfigIsVisibleExt(cfgid: Oid, is_missing: *mut bool) -> bool {
    let tup: HeapTuple;
    let form: Form_pg_ts_config;
    let namespace: Oid;
    let visible: bool;

    tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgid));
    if !HeapTupleIsValid(tup) {
        if !is_missing.is_null() {
            *is_missing = true;
            return false;
        }
        elog!(ERROR, "cache lookup failed for text search configuration {}", cfgid);
    }
    form = GETSTRUCT(tup) as Form_pg_ts_config;

    recomputeNamespacePath();

    namespace = (*form).cfgnamespace;
    if namespace != PG_CATALOG_NAMESPACE
        && !list_member_oid(activeSearchPath, namespace)
    {
        visible = false;
    } else {
        let name: *mut c_char = NameStr(&(*form).cfgname) as *mut c_char;
        let mut l: *mut ListCell;

        let mut vis = false;
        l = list_head(activeSearchPath);
        'vis_loop: while !l.is_null() {
            let namespaceId: Oid = lfirst_oid(l);

            if namespaceId == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            if namespaceId == namespace {
                /* Found it first in path */
                vis = true;
                break 'vis_loop;
            }
            if SearchSysCacheExists2(
                TSCONFIGNAMENSP,
                PointerGetDatum(name as *const c_void),
                ObjectIdGetDatum(namespaceId),
            ) {
                /* Found something else first in path */
                break 'vis_loop;
            }
            l = lnext(activeSearchPath, l);
        }
        visible = vis;
    }

    ReleaseSysCache(tup);

    visible
}


/*
 * DeconstructQualifiedName
 *		Given a possibly-qualified name expressed as a list of String nodes,
 *		extract the schema name and object name.
 *
 * *nspname_p is set to NULL if there is no explicit schema name.
 */
pub unsafe fn DeconstructQualifiedName(
    names: *const List,
    nspname_p: *mut *mut c_char,
    objname_p: *mut *mut c_char,
) {
    let catalogname: *mut c_char;
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut objname: *mut c_char = core::ptr::null_mut();

    match list_length(names) {
        1 => {
            objname = strVal(linitial(names) as *const c_void);
        }
        2 => {
            schemaname = strVal(linitial(names) as *const c_void);
            objname = strVal(lsecond(names) as *const c_void);
        }
        3 => {
            catalogname = strVal(linitial(names) as *const c_void);
            schemaname = strVal(lsecond(names) as *const c_void);
            objname = strVal(lthird(names) as *const c_void);

            /*
             * We check the catalog name and then ignore it.
             */
            if strcmp(catalogname, get_database_name(MyDatabaseId)) != 0 {
                ereport!(ERROR, errmsg!(
                        "cross-database references are not implemented: {}",
                        CStr::from_ptr(NameListToString(names as *mut List)).to_string_lossy()
                    )) /* C also: errcode */;
            }
        }
        _ => {
            ereport!(ERROR, errmsg!(
                    "improper qualified name (too many dotted names): {}",
                    CStr::from_ptr(NameListToString(names as *mut List)).to_string_lossy()
                )) /* C also: errcode */;
        }
    }

    *nspname_p = schemaname;
    *objname_p = objname;
}

/*
 * LookupNamespaceNoError
 *		Look up a schema name.
 *
 * Returns the namespace OID, or InvalidOid if not found.
 *
 * Note this does NOT perform any permissions check --- callers are
 * responsible for being sure that an appropriate check is made.
 * In the majority of cases LookupExplicitNamespace is preferable.
 */
pub unsafe fn LookupNamespaceNoError(nspname: *const c_char) -> Oid {
    /* check for pg_temp alias */
    if strcmp(nspname, b"pg_temp\0".as_ptr() as *const c_char) == 0 {
        if OidIsValid(myTempNamespace) {
            InvokeNamespaceSearchHook(myTempNamespace, true);
            return myTempNamespace;
        }

        /*
         * Since this is used only for looking up existing objects, there is
         * no point in trying to initialize the temp namespace here; and doing
         * so might create problems for some callers. Just report "not found".
         */
        return InvalidOid;
    }

    get_namespace_oid(nspname, true)
}

/*
 * LookupExplicitNamespace
 *		Process an explicitly-specified schema name: look up the schema
 *		and verify we have USAGE (lookup) rights in it.
 *
 * Returns the namespace OID
 */
pub unsafe fn LookupExplicitNamespace(nspname: *const c_char, missing_ok: bool) -> Oid {
    let namespaceId: Oid;
    let aclresult: AclResult;

    /* check for pg_temp alias */
    if strcmp(nspname, b"pg_temp\0".as_ptr() as *const c_char) == 0 {
        if OidIsValid(myTempNamespace) {
            return myTempNamespace;
        }

        /*
         * Since this is used only for looking up existing objects, there is
         * no point in trying to initialize the temp namespace here; and doing
         * so might create problems for some callers --- just fall through.
         */
    }

    namespaceId = get_namespace_oid(nspname, missing_ok);
    if missing_ok && !OidIsValid(namespaceId) {
        return InvalidOid;
    }

    let aclresult2 =
        object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(), ACL_USAGE);
    if aclresult2 != AclResult::ACLCHECK_OK {
        aclcheck_error(aclresult2, ObjectType::OBJECT_SCHEMA, nspname);
    }
    /* Schema search hook for this lookup */
    InvokeNamespaceSearchHook(namespaceId, true);

    namespaceId
}

/*
 * LookupCreationNamespace
 *		Look up the schema and verify we have CREATE rights on it.
 *
 * This is just like LookupExplicitNamespace except for the different
 * permission check, and that we are willing to create pg_temp if needed.
 *
 * Note: calling this may result in a CommandCounterIncrement operation,
 * if we have to create or clean out the temp namespace.
 */
pub unsafe fn LookupCreationNamespace(nspname: *const c_char) -> Oid {
    let namespaceId: Oid;
    let aclresult: AclResult;

    /* check for pg_temp alias */
    if strcmp(nspname, b"pg_temp\0".as_ptr() as *const c_char) == 0 {
        /* Initialize temp namespace */
        AccessTempTableNamespace(false);
        return myTempNamespace;
    }

    namespaceId = get_namespace_oid(nspname, false);

    let aclresult2 =
        object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(), ACL_CREATE);
    if aclresult2 != AclResult::ACLCHECK_OK {
        aclcheck_error(aclresult2, ObjectType::OBJECT_SCHEMA, nspname);
    }

    namespaceId
}

/*
 * Common checks on switching namespaces.
 *
 * We complain if either the old or new namespaces is a temporary schema
 * (or temporary toast schema), or if either the old or new namespaces is the
 * TOAST schema.
 */
pub unsafe fn CheckSetNamespace(oldNspOid: Oid, nspOid: Oid) {
    /* disallow renaming into or out of temp schemas */
    if isAnyTempNamespace(nspOid) || isAnyTempNamespace(oldNspOid) {
        ereport!(ERROR, errmsg!("cannot move objects into or out of temporary schemas")) /* C also: errcode */;
    }

    /* same for TOAST schema */
    if nspOid == PG_TOAST_NAMESPACE || oldNspOid == PG_TOAST_NAMESPACE {
        ereport!(ERROR, errmsg!("cannot move objects into or out of TOAST schema")) /* C also: errcode */;
    }
}

/*
 * QualifiedNameGetCreationNamespace
 *		Given a possibly-qualified name for an object (in List-of-Strings
 *		format), determine what namespace the object should be created in.
 *		Also extract and return the object name (last component of list).
 *
 * Note: this does not apply any permissions check.  Callers must check
 * for CREATE rights on the selected namespace when appropriate.
 *
 * Note: calling this may result in a CommandCounterIncrement operation,
 * if we have to create or clean out the temp namespace.
 */
pub unsafe fn QualifiedNameGetCreationNamespace(
    names: *const List,
    objname_p: *mut *mut c_char,
) -> Oid {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let namespaceId: Oid;

    /* deconstruct the name list */
    DeconstructQualifiedName(names, &mut schemaname, objname_p);

    if !schemaname.is_null() {
        /* check for pg_temp alias */
        if strcmp(schemaname, b"pg_temp\0".as_ptr() as *const c_char) == 0 {
            /* Initialize temp namespace */
            AccessTempTableNamespace(false);
            return myTempNamespace;
        }
        /* use exact schema given */
        namespaceId = get_namespace_oid(schemaname, false);
        /* we do not check for USAGE rights here! */
    } else {
        /* use the default creation namespace */
        recomputeNamespacePath();
        if activeTempCreationPending {
            /* Need to initialize temp namespace */
            AccessTempTableNamespace(true);
            return myTempNamespace;
        }
        namespaceId = activeCreationNamespace;
        if !OidIsValid(namespaceId) {
            ereport!(ERROR, errmsg!("no schema has been selected to create in")) /* C also: errcode */;
        }
    }

    namespaceId
}

/*
 * get_namespace_oid - given a namespace name, look up the OID
 *
 * If missing_ok is false, throw an error if namespace name not found.  If
 * true, just return InvalidOid.
 */
pub unsafe fn get_namespace_oid(nspname: *const c_char, missing_ok: bool) -> Oid {
    let oid: Oid;

    oid = GetSysCacheOid1(
        NAMESPACENAME,
        Anum_pg_namespace_oid,
        CStringGetDatum(nspname),
    );
    if !OidIsValid(oid) && !missing_ok {
        ereport!(ERROR, errmsg!("schema \"{}\" does not exist", CStr::from_ptr(nspname).to_string_lossy())) /* C also: errcode */;
    }

    oid
}

/*
 * makeRangeVarFromNameList
 *		Utility routine to convert a qualified-name list into RangeVar form.
 */
pub unsafe fn makeRangeVarFromNameList(names: *const List) -> *mut RangeVar {
    let rel: *mut RangeVar = makeRangeVar(
        core::ptr::null_mut(),
        core::ptr::null_mut(),
        -1,
    );

    match list_length(names) {
        1 => {
            (*rel).relname = strVal(linitial(names) as *const c_void);
        }
        2 => {
            (*rel).schemaname = strVal(linitial(names) as *const c_void);
            (*rel).relname = strVal(lsecond(names) as *const c_void);
        }
        3 => {
            (*rel).catalogname = strVal(linitial(names) as *const c_void);
            (*rel).schemaname = strVal(lsecond(names) as *const c_void);
            (*rel).relname = strVal(lthird(names) as *const c_void);
        }
        _ => {
            ereport!(ERROR, errmsg!(
                    "improper relation name (too many dotted names): {}",
                    CStr::from_ptr(NameListToString(names as *mut List)).to_string_lossy()
                )) /* C also: errcode */;
        }
    }

    rel
}

/*
 * NameListToString
 *		Utility routine to convert a qualified-name list into a string.
 *
 * This is used primarily to form error messages, and so we do not quote
 * the list elements, for the sake of legibility.
 *
 * In most scenarios the list elements should always be String values,
 * but we also allow A_Star for the convenience of ColumnRef processing.
 */
pub unsafe fn NameListToString(names: *const List) -> *mut c_char {
    let mut string: StringInfoData = core::mem::zeroed();
    let mut l: *mut ListCell;

    initStringInfo(&mut string);

    l = list_head(names);
    while !l.is_null() {
        let name: *mut c_void = lfirst(l) as *mut c_void;

        if l != list_head(names) {
            appendStringInfoChar(&mut string, b'.' as c_char);
        }

        if nodeTag(name) == T_String {
            appendStringInfoString(&mut string, strVal(name));
        } else if nodeTag(name) == T_A_Star {
            appendStringInfoChar(&mut string, b'*' as c_char);
        } else {
            elog!(
                ERROR,
                "unexpected node type in name list: {}",
                nodeTag(name)
            );
        }
        l = lnext(names, l);
    }

    string.data
}

/*
 * NameListToQuotedString
 *		Utility routine to convert a qualified-name list into a string.
 *
 * Same as above except that names will be double-quoted where necessary,
 * so the string could be re-parsed (eg, by textToQualifiedNameList).
 */
pub unsafe fn NameListToQuotedString(names: *const List) -> *mut c_char {
    let mut string: StringInfoData = core::mem::zeroed();
    let mut l: *mut ListCell;

    initStringInfo(&mut string);

    l = list_head(names);
    while !l.is_null() {
        if l != list_head(names) {
            appendStringInfoChar(&mut string, b'.' as c_char);
        }
        appendStringInfoString(
            &mut string,
            quote_identifier(strVal(lfirst(l) as *mut c_void)),
        );
        l = lnext(names, l);
    }

    string.data
}

// ===========================================================================
// PART 6: isTempNamespace, isTempToastNamespace, isTempOrTempToastNamespace,
//         isAnyTempNamespace, isOtherTempNamespace, checkTempNamespaceStatus,
//         GetTempNamespaceProcNumber, GetTempToastNamespace,
//         GetTempNamespaceState, SetTempNamespaceState,
//         GetSearchPathMatcher, CopySearchPathMatcher,
//         SearchPathMatchesCurrentEnvironment,
//         get_collation_oid, get_conversion_oid, FindDefaultConversionProc
// ===========================================================================

/*
 * isTempNamespace - is the given namespace my temporary-table namespace?
 */
pub unsafe fn isTempNamespace(namespaceId: Oid) -> bool {
    OidIsValid(myTempNamespace) && myTempNamespace == namespaceId
}

/*
 * isTempToastNamespace - is the given namespace my temporary-toast-table
 *		namespace?
 */
pub unsafe fn isTempToastNamespace(namespaceId: Oid) -> bool {
    OidIsValid(myTempToastNamespace) && myTempToastNamespace == namespaceId
}

/*
 * isTempOrTempToastNamespace - is the given namespace my temporary-table
 *		namespace or my temporary-toast-table namespace?
 */
pub unsafe fn isTempOrTempToastNamespace(namespaceId: Oid) -> bool {
    OidIsValid(myTempNamespace)
        && (myTempNamespace == namespaceId || myTempToastNamespace == namespaceId)
}

/*
 * isAnyTempNamespace - is the given namespace a temporary-table namespace
 * (either my own, or another backend's)?  Temporary-toast-table namespaces
 * are included, too.
 */
pub unsafe fn isAnyTempNamespace(namespaceId: Oid) -> bool {
    let result: bool;
    let nspname: *mut c_char;

    /* True if the namespace name starts with "pg_temp_" or "pg_toast_temp_" */
    nspname = get_namespace_name(namespaceId);
    if nspname.is_null() {
        return false; /* no such namespace? */
    }
    result = (strncmp(nspname, b"pg_temp_\0".as_ptr() as *const c_char, 8) == 0)
        || (strncmp(nspname, b"pg_toast_temp_\0".as_ptr() as *const c_char, 14) == 0);
    pfree(nspname as *mut c_void);
    result
}

/*
 * isOtherTempNamespace - is the given namespace some other backend's
 * temporary-table namespace (including temporary-toast-table namespaces)?
 *
 * Note: for most purposes in the C code, this function is obsolete.  Use
 * RELATION_IS_OTHER_TEMP() instead to detect non-local temp relations.
 */
pub unsafe fn isOtherTempNamespace(namespaceId: Oid) -> bool {
    /* If it's my own temp namespace, say "false" */
    if isTempOrTempToastNamespace(namespaceId) {
        return false;
    }
    /* Else, if it's any temp namespace, say "true" */
    isAnyTempNamespace(namespaceId)
}

/*
 * checkTempNamespaceStatus - is the given namespace owned and actively used
 * by a backend?
 *
 * Note: this can be used while scanning relations in pg_class to detect
 * orphaned temporary tables or namespaces with a backend connected to a
 * given database.  The result may be out of date quickly, so the caller
 * must be careful how to handle this information.
 */
pub unsafe fn checkTempNamespaceStatus(namespaceId: Oid) -> TempNamespaceStatus {
    let proc_ptr: *mut PGPROC;
    let procNumber: ProcNumber;

    Assert!(OidIsValid(MyDatabaseId));

    procNumber = GetTempNamespaceProcNumber(namespaceId);

    /* No such namespace, or its name shows it's not temp? */
    if procNumber == INVALID_PROC_NUMBER {
        return TempNamespaceStatus::TEMP_NAMESPACE_NOT_TEMP;
    }

    /* Is the backend alive? */
    proc_ptr = ProcNumberGetProc(procNumber);
    if proc_ptr.is_null() {
        return TempNamespaceStatus::TEMP_NAMESPACE_IDLE;
    }

    /* Is the backend connected to the same database we are looking at? */
    if (*proc_ptr).databaseId != MyDatabaseId {
        return TempNamespaceStatus::TEMP_NAMESPACE_IDLE;
    }

    /* Does the backend own the temporary namespace? */
    if (*proc_ptr).tempNamespaceId != namespaceId {
        return TempNamespaceStatus::TEMP_NAMESPACE_IDLE;
    }

    /* Yup, so namespace is busy */
    TempNamespaceStatus::TEMP_NAMESPACE_IN_USE
}

/*
 * GetTempNamespaceProcNumber - if the given namespace is a temporary-table
 * namespace (either my own, or another backend's), return the proc number
 * that owns it.  Temporary-toast-table namespaces are included, too.
 * If it isn't a temp namespace, return INVALID_PROC_NUMBER.
 */
pub unsafe fn GetTempNamespaceProcNumber(namespaceId: Oid) -> ProcNumber {
    let result: c_int;
    let nspname: *mut c_char;

    /* See if the namespace name starts with "pg_temp_" or "pg_toast_temp_" */
    nspname = get_namespace_name(namespaceId);
    if nspname.is_null() {
        return INVALID_PROC_NUMBER; /* no such namespace? */
    }
    if strncmp(nspname, b"pg_temp_\0".as_ptr() as *const c_char, 8) == 0 {
        result = atoi(nspname.add(8));
    } else if strncmp(nspname, b"pg_toast_temp_\0".as_ptr() as *const c_char, 14) == 0 {
        result = atoi(nspname.add(14));
    } else {
        result = INVALID_PROC_NUMBER;
    }
    pfree(nspname as *mut c_void);
    result
}

/*
 * GetTempToastNamespace - get the OID of my temporary-toast-table namespace,
 * which must already be assigned.  (This is only used when creating a toast
 * table for a temp table, so we must have already done InitTempTableNamespace)
 */
pub unsafe fn GetTempToastNamespace() -> Oid {
    Assert!(OidIsValid(myTempToastNamespace));
    myTempToastNamespace
}


/*
 * GetTempNamespaceState - fetch status of session's temporary namespace
 *
 * This is used for conveying state to a parallel worker, and is not meant
 * for general-purpose access.
 */
pub unsafe fn GetTempNamespaceState(
    tempNamespaceId: *mut Oid,
    tempToastNamespaceId: *mut Oid,
) {
    /* Return namespace OIDs, or 0 if session has not created temp namespace */
    *tempNamespaceId = myTempNamespace;
    *tempToastNamespaceId = myTempToastNamespace;
}

/*
 * SetTempNamespaceState - set status of session's temporary namespace
 *
 * This is used for conveying state to a parallel worker, and is not meant for
 * general-purpose access.  By transferring these namespace OIDs to workers,
 * we ensure they will have the same notion of the search path as their leader
 * does.
 */
pub unsafe fn SetTempNamespaceState(
    tempNamespaceId: Oid,
    tempToastNamespaceId: Oid,
) {
    /* Worker should not have created its own namespaces ... */
    Assert!(myTempNamespace == InvalidOid);
    Assert!(myTempToastNamespace == InvalidOid);
    Assert!(myTempNamespaceSubID == InvalidSubTransactionId);

    /* Assign same namespace OIDs that leader has */
    myTempNamespace = tempNamespaceId;
    myTempToastNamespace = tempToastNamespaceId;

    /*
     * It's fine to leave myTempNamespaceSubID == InvalidSubTransactionId.
     * Even if the namespace is new so far as the leader is concerned, it's
     * not new to the worker, and we certainly wouldn't want the worker trying
     * to destroy it.
     */

    baseSearchPathValid = false; /* may need to rebuild list */
    searchPathCacheValid = false;
}


/*
 * GetSearchPathMatcher - fetch current search path definition.
 *
 * The result structure is allocated in the specified memory context
 * (which might or might not be equal to CurrentMemoryContext); but any
 * junk created by revalidation calculations will be in CurrentMemoryContext.
 */
pub unsafe fn GetSearchPathMatcher(context: MemoryContext) -> *mut SearchPathMatcher {
    let result: *mut SearchPathMatcher;
    let mut schemas: *mut List;
    let oldcxt: MemoryContext;

    recomputeNamespacePath();

    oldcxt = MemoryContextSwitchTo(context);

    result = palloc0(core::mem::size_of::<SearchPathMatcher>()) as *mut SearchPathMatcher;
    schemas = list_copy(activeSearchPath);
    while !schemas.is_null() && linitial_oid(schemas) != activeCreationNamespace {
        if linitial_oid(schemas) == myTempNamespace {
            (*result).addTemp = true;
        } else {
            Assert!(linitial_oid(schemas) == PG_CATALOG_NAMESPACE);
            (*result).addCatalog = true;
        }
        schemas = list_delete_first(schemas);
    }
    (*result).schemas = schemas;
    (*result).generation = activePathGeneration;

    MemoryContextSwitchTo(oldcxt);

    result
}

/*
 * CopySearchPathMatcher - copy the specified SearchPathMatcher.
 *
 * The result structure is allocated in CurrentMemoryContext.
 */
pub unsafe fn CopySearchPathMatcher(path: *mut SearchPathMatcher) -> *mut SearchPathMatcher {
    let result: *mut SearchPathMatcher;

    result = palloc(core::mem::size_of::<SearchPathMatcher>()) as *mut SearchPathMatcher;
    (*result).schemas = list_copy((*path).schemas);
    (*result).addCatalog = (*path).addCatalog;
    (*result).addTemp = (*path).addTemp;
    (*result).generation = (*path).generation;

    result
}

/*
 * SearchPathMatchesCurrentEnvironment - does path match current environment?
 *
 * This is tested over and over in some common code paths, and in the typical
 * scenario where the active search path seldom changes, it'll always succeed.
 * We make that case fast by keeping a generation counter that is advanced
 * whenever the active search path changes.
 */
pub unsafe fn SearchPathMatchesCurrentEnvironment(path: *mut SearchPathMatcher) -> bool {
    let mut lc: *mut ListCell;
    let mut lcp: *mut ListCell;

    recomputeNamespacePath();

    /* Quick out if already known equal to active path. */
    if (*path).generation == activePathGeneration {
        return true;
    }

    /* We scan down the activeSearchPath to see if it matches the input. */
    lc = list_head(activeSearchPath);

    /* If path->addTemp, first item should be my temp namespace. */
    if (*path).addTemp {
        if !lc.is_null() && lfirst_oid(lc) == myTempNamespace {
            lc = lnext(activeSearchPath, lc);
        } else {
            return false;
        }
    }
    /* If path->addCatalog, next item should be pg_catalog. */
    if (*path).addCatalog {
        if !lc.is_null() && lfirst_oid(lc) == PG_CATALOG_NAMESPACE {
            lc = lnext(activeSearchPath, lc);
        } else {
            return false;
        }
    }
    /* We should now be looking at the activeCreationNamespace. */
    if activeCreationNamespace
        != (if !lc.is_null() {
            lfirst_oid(lc)
        } else {
            InvalidOid
        })
    {
        return false;
    }
    /* The remainder of activeSearchPath should match path->schemas. */
    lcp = list_head((*path).schemas);
    while !lcp.is_null() {
        if !lc.is_null() && lfirst_oid(lc) == lfirst_oid(lcp) {
            lc = lnext(activeSearchPath, lc);
        } else {
            return false;
        }
        lcp = lnext((*path).schemas, lcp);
    }
    if !lc.is_null() {
        return false;
    }

    /*
     * Update path->generation so that future tests will return quickly, so
     * long as the active search path doesn't change.
     */
    (*path).generation = activePathGeneration;

    true
}

/*
 * get_collation_oid - find a collation by possibly qualified name
 *
 * Note that this will only find collations that work with the current
 * database's encoding.
 */
pub unsafe fn get_collation_oid(collname: *mut List, missing_ok: bool) -> Oid {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut collation_name: *mut c_char = core::ptr::null_mut();
    let dbencoding: i32 = GetDatabaseEncoding();
    let namespaceId: Oid;
    let mut colloid: Oid;
    let mut l: *mut ListCell;

    /* deconstruct the name list */
    DeconstructQualifiedName(collname, &mut schemaname, &mut collation_name);

    if !schemaname.is_null() {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if missing_ok && !OidIsValid(namespaceId) {
            return InvalidOid;
        }

        colloid = lookup_collation(collation_name, namespaceId, dbencoding);
        if OidIsValid(colloid) {
            return colloid;
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath();

        l = list_head(activeSearchPath);
        while !l.is_null() {
            let ns: Oid = lfirst_oid(l);

            if ns == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            colloid = lookup_collation(collation_name, ns, dbencoding);
            if OidIsValid(colloid) {
                return colloid;
            }
            l = lnext(activeSearchPath, l);
        }
    }

    /* Not found in path */
    if !missing_ok {
        ereport!(ERROR, errmsg!(
                "collation \"{}\" for encoding \"{}\" does not exist",
                CStr::from_ptr(NameListToString(collname)).to_string_lossy(),
                CStr::from_ptr(GetDatabaseEncodingName()).to_string_lossy()
            )) /* C also: errcode */;
    }
    InvalidOid
}

/*
 * get_conversion_oid - find a conversion by possibly qualified name
 */
pub unsafe fn get_conversion_oid(conname: *mut List, missing_ok: bool) -> Oid {
    let mut schemaname: *mut c_char = core::ptr::null_mut();
    let mut conversion_name: *mut c_char = core::ptr::null_mut();
    let namespaceId: Oid;
    let mut conoid: Oid = InvalidOid;
    let mut l: *mut ListCell;

    /* deconstruct the name list */
    DeconstructQualifiedName(conname, &mut schemaname, &mut conversion_name);

    if !schemaname.is_null() {
        /* use exact schema given */
        namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
        if missing_ok && !OidIsValid(namespaceId) {
            conoid = InvalidOid;
        } else {
            conoid = GetSysCacheOid2(
                CONNAMENSP,
                Anum_pg_conversion_oid,
                PointerGetDatum(conversion_name as *const c_void),
                ObjectIdGetDatum(namespaceId),
            );
        }
    } else {
        /* search for it in search path */
        recomputeNamespacePath();

        l = list_head(activeSearchPath);
        while !l.is_null() {
            let ns: Oid = lfirst_oid(l);

            if ns == myTempNamespace {
                l = lnext(activeSearchPath, l);
                continue; /* do not look in temp namespace */
            }

            conoid = GetSysCacheOid2(
                CONNAMENSP,
                Anum_pg_conversion_oid,
                PointerGetDatum(conversion_name as *const c_void),
                ObjectIdGetDatum(ns),
            );
            if OidIsValid(conoid) {
                return conoid;
            }
            l = lnext(activeSearchPath, l);
        }
    }

    /* Not found in path */
    if !OidIsValid(conoid) && !missing_ok {
        ereport!(ERROR, errmsg!(
                "conversion \"{}\" does not exist",
                CStr::from_ptr(NameListToString(conname)).to_string_lossy()
            )) /* C also: errcode */;
    }
    conoid
}

/*
 * FindDefaultConversionProc - find default encoding conversion proc
 */
pub unsafe fn FindDefaultConversionProc(for_encoding: i32, to_encoding: i32) -> Oid {
    let mut proc_: Oid;
    let mut l: *mut ListCell;

    recomputeNamespacePath();

    l = list_head(activeSearchPath);
    while !l.is_null() {
        let namespaceId: Oid = lfirst_oid(l);

        if namespaceId == myTempNamespace {
            l = lnext(activeSearchPath, l);
            continue; /* do not look in temp namespace */
        }

        proc_ = FindDefaultConversion(namespaceId, for_encoding, to_encoding);
        if OidIsValid(proc_) {
            return proc_;
        }
        l = lnext(activeSearchPath, l);
    }

    /* Not found in path */
    InvalidOid
}

// ===========================================================================
// PART 7: preprocessNamespacePath, finalNamespacePath, cachedNamespacePath,
//         recomputeNamespacePath, AccessTempTableNamespace, InitTempTableNamespace,
//         AtEOXact_Namespace, AtEOSubXact_Namespace,
//         RemoveTempRelations, RemoveTempRelationsCallback, ResetTempTableNamespace,
//         check_search_path, assign_search_path, InitializeSearchPath,
//         InvalidationCallback, fetch_search_path, fetch_search_path_array,
//         pg_*_is_visible SQL-callable functions
// ===========================================================================

/*
 * Look up namespace IDs and perform ACL checks. Return newly-allocated list.
 */
unsafe fn preprocessNamespacePath(
    searchPath: *const c_char,
    roleid: Oid,
    temp_missing: *mut bool,
) -> *mut List {
    let rawname: *mut c_char;
    let mut namelist: *mut List = core::ptr::null_mut();
    let mut oidlist: *mut List;
    let mut l: *mut ListCell;

    /* Need a modifiable copy */
    rawname = pstrdup(searchPath);

    /* Parse string into list of identifiers */
    if !SplitIdentifierString(rawname, b',' as c_char, &mut namelist) {
        /* syntax error in name list */
        /* this should not happen if GUC checked check_search_path */
        elog!(ERROR, "invalid list syntax");
    }

    /*
     * Convert the list of names to a list of OIDs.  If any names are not
     * recognizable or we don't have read access, just leave them out of the
     * list.  (We can't raise an error, since the search_path setting has
     * already been accepted.)  Don't make duplicate entries, either.
     */
    oidlist = core::ptr::null_mut();
    *temp_missing = false;
    l = list_head(namelist);
    while !l.is_null() {
        let curname: *mut c_char = lfirst(l) as *mut c_char;
        let namespaceId: Oid;

        if strcmp(curname, b"$user\0".as_ptr() as *const c_char) == 0 {
            /* $user --- substitute namespace matching user name, if any */
            let tuple: HeapTuple;

            tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
            if HeapTupleIsValid(tuple) {
                let rname: *mut c_char;

                rname = NameStr(&(*(GETSTRUCT(tuple) as Form_pg_authid)).rolname) as *mut c_char;
                namespaceId = get_namespace_oid(rname, true);
                ReleaseSysCache(tuple);
                if OidIsValid(namespaceId)
                    && object_aclcheck(
                        NamespaceRelationId,
                        namespaceId,
                        roleid,
                        ACL_USAGE,
                    ) == AclResult::ACLCHECK_OK
                {
                    oidlist = lappend_oid(oidlist, namespaceId);
                }
            }
        } else if strcmp(curname, b"pg_temp\0".as_ptr() as *const c_char) == 0 {
            /* pg_temp --- substitute temp namespace, if any */
            if OidIsValid(myTempNamespace) {
                oidlist = lappend_oid(oidlist, myTempNamespace);
            } else {
                /* If it ought to be the creation namespace, set flag */
                if oidlist.is_null() {
                    *temp_missing = true;
                }
            }
        } else {
            /* normal namespace reference */
            namespaceId = get_namespace_oid(curname, true);
            if OidIsValid(namespaceId)
                && object_aclcheck(
                    NamespaceRelationId,
                    namespaceId,
                    roleid,
                    ACL_USAGE,
                ) == AclResult::ACLCHECK_OK
            {
                oidlist = lappend_oid(oidlist, namespaceId);
            }
        }
        l = lnext(namelist, l);
    }

    pfree(rawname as *mut c_void);
    list_free(namelist);

    oidlist
}

/*
 * Remove duplicates, run namespace search hooks, and prepend
 * implicitly-searched namespaces. Return newly-allocated list.
 *
 * If an object_access_hook is present, this must always be recalculated. It
 * may seem that duplicate elimination is not dependent on the result of the
 * hook, but if a hook returns different results on different calls for the
 * same namespace ID, then it could affect the order in which that namespace
 * appears in the final list.
 */
unsafe fn finalNamespacePath(oidlist: *mut List, firstNS: *mut Oid) -> *mut List {
    let mut finalPath: *mut List = core::ptr::null_mut();
    let mut lc: *mut ListCell;

    lc = list_head(oidlist);
    while !lc.is_null() {
        let namespaceId: Oid = lfirst_oid(lc);

        if !list_member_oid(finalPath, namespaceId) {
            if InvokeNamespaceSearchHook(namespaceId, false) {
                finalPath = lappend_oid(finalPath, namespaceId);
            }
        }
        lc = lnext(oidlist, lc);
    }

    /*
     * Remember the first member of the explicit list.  (Note: this is
     * nominally wrong if temp_missing, but we need it anyway to distinguish
     * explicit from implicit mention of pg_catalog.)
     */
    if finalPath.is_null() {
        *firstNS = InvalidOid;
    } else {
        *firstNS = linitial_oid(finalPath);
    }

    /*
     * Add any implicitly-searched namespaces to the list.  Note these go on
     * the front, not the back; also notice that we do not check USAGE
     * permissions for these.
     */
    if !list_member_oid(finalPath, PG_CATALOG_NAMESPACE) {
        finalPath = lcons_oid(PG_CATALOG_NAMESPACE, finalPath);
    }

    if OidIsValid(myTempNamespace) && !list_member_oid(finalPath, myTempNamespace) {
        finalPath = lcons_oid(myTempNamespace, finalPath);
    }

    finalPath
}

/*
 * Retrieve search path information from the cache; or if not there, fill
 * it. The returned entry is valid only until the next call to this function.
 */
unsafe fn cachedNamespacePath(
    searchPath: *const c_char,
    roleid: Oid,
) -> *const SearchPathCacheEntry {
    let mut oldcxt: MemoryContext;
    let entry: *mut SearchPathCacheEntry;

    spcache_init();

    entry = spcache_insert(searchPath, roleid);

    /*
     * An OOM may have resulted in a cache entry with missing 'oidlist' or
     * 'finalPath', so just compute whatever is missing.
     */

    if (*entry).oidlist.is_null() {
        oldcxt = MemoryContextSwitchTo(SearchPathCacheContext);
        (*entry).oidlist = preprocessNamespacePath(
            searchPath,
            roleid,
            &mut (*entry).temp_missing,
        );
        MemoryContextSwitchTo(oldcxt);
    }

    /*
     * If a hook is set, we must recompute finalPath from the oidlist each
     * time, because the hook may affect the result. This is still much faster
     * than recomputing from the string (and doing catalog lookups and ACL
     * checks).
     */
    if (*entry).finalPath.is_null() || !object_access_hook.is_null() || (*entry).forceRecompute {
        list_free((*entry).finalPath);
        (*entry).finalPath = core::ptr::null_mut();

        oldcxt = MemoryContextSwitchTo(SearchPathCacheContext);
        (*entry).finalPath = finalNamespacePath((*entry).oidlist, &mut (*entry).firstNS);
        MemoryContextSwitchTo(oldcxt);

        /*
         * If an object_access_hook is set when finalPath is calculated, the
         * result may be affected by the hook. Force recomputation of
         * finalPath the next time this cache entry is used, even if the
         * object_access_hook is not set at that time.
         */
        (*entry).forceRecompute = !object_access_hook.is_null();
    }

    entry
}

/*
 * recomputeNamespacePath - recompute path derived variables if needed.
 */
unsafe fn recomputeNamespacePath() {
    let roleid: Oid = GetUserId();
    let pathChanged: bool;
    let entry: *const SearchPathCacheEntry;

    /* Do nothing if path is already valid. */
    if baseSearchPathValid && namespaceUser == roleid {
        return;
    }

    entry = cachedNamespacePath(namespace_search_path, roleid);

    if baseCreationNamespace == (*entry).firstNS
        && baseTempCreationPending == (*entry).temp_missing
        && equal(
            (*entry).finalPath as *const c_void,
            baseSearchPath as *const c_void,
        )
    {
        let path_changed = false;
        /* pathChanged = false branch -- fall through */
        /* Mark the path valid. */
        baseSearchPathValid = true;
        namespaceUser = roleid;

        /* And make it active. */
        activeSearchPath = baseSearchPath;
        activeCreationNamespace = baseCreationNamespace;
        activeTempCreationPending = baseTempCreationPending;
        /* (no generation bump since nothing changed) */
        return;
    }

    {
        let oldcxt: MemoryContext;
        let newpath: *mut List;

        /* Must save OID list in permanent storage. */
        oldcxt = MemoryContextSwitchTo(TopMemoryContext);
        newpath = list_copy((*entry).finalPath);
        MemoryContextSwitchTo(oldcxt);

        /* Now safe to assign to state variables. */
        list_free(baseSearchPath);
        baseSearchPath = newpath;
        baseCreationNamespace = (*entry).firstNS;
        baseTempCreationPending = (*entry).temp_missing;
    }

    /* Mark the path valid. */
    baseSearchPathValid = true;
    namespaceUser = roleid;

    /* And make it active. */
    activeSearchPath = baseSearchPath;
    activeCreationNamespace = baseCreationNamespace;
    activeTempCreationPending = baseTempCreationPending;

    /*
     * Bump the generation only if something actually changed.  (Notice that
     * what we compared to was the old state of the base path variables.)
     */
    activePathGeneration += 1;
}

/*
 * AccessTempTableNamespace
 *		Provide access to a temporary namespace, potentially creating it
 *		if not present yet.  This routine registers if the namespace gets
 *		in use in this transaction.  'force' can be set to true to allow
 *		the caller to enforce the creation of the temporary namespace for
 *		use in this backend, which happens if its creation is pending.
 */
unsafe fn AccessTempTableNamespace(force: bool) {
    /*
     * Make note that this temporary namespace has been accessed in this
     * transaction.
     */
    MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE;

    /*
     * If the caller attempting to access a temporary schema expects the
     * creation of the namespace to be pending and should be enforced, then go
     * through the creation.
     */
    if !force && OidIsValid(myTempNamespace) {
        return;
    }

    /*
     * The temporary tablespace does not exist yet and is wanted, so
     * initialize it.
     */
    InitTempTableNamespace();
}

/*
 * InitTempTableNamespace
 *		Initialize temp table namespace on first use in a particular backend
 */
unsafe fn InitTempTableNamespace() {
    let mut namespaceName: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let namespaceId: Oid;
    let toastspaceId: Oid;

    Assert!(!OidIsValid(myTempNamespace));

    /*
     * First, do permission check to see if we are authorized to make temp
     * tables.  We use a nonstandard error message here since "databasename:
     * permission denied" might be a tad cryptic.
     *
     * Note that ACL_CREATE_TEMP rights are rechecked in pg_namespace_aclmask;
     * that's necessary since current user ID could change during the session.
     * But there's no need to make the namespace in the first place until a
     * temp table creation request is made by someone with appropriate rights.
     */
    if object_aclcheck(DatabaseRelationId, MyDatabaseId, GetUserId(), ACL_CREATE_TEMP)
        != AclResult::ACLCHECK_OK
    {
        ereport!(ERROR, errmsg!(
                "permission denied to create temporary tables in database \"{}\"",
                CStr::from_ptr(get_database_name(MyDatabaseId)).to_string_lossy()
            )) /* C also: errcode */;
    }

    /*
     * Do not allow a Hot Standby session to make temp tables.  Aside from
     * problems with modifying the system catalogs, there is a naming
     * conflict: pg_temp_N belongs to the session with proc number N on the
     * primary, not to a hot standby session with the same proc number.  We
     * should not be able to get here anyway due to XactReadOnly checks, but
     * let's just make real sure.  Note that this also backstops various
     * operations that allow XactReadOnly transactions to modify temp tables;
     * they'd need RecoveryInProgress checks if not for this.
     */
    if RecoveryInProgress() {
        ereport!(ERROR, errmsg!("cannot create temporary tables during recovery")) /* C also: errcode */;
    }

    /* Parallel workers can't create temporary tables, either. */
    if IsParallelWorker() {
        ereport!(ERROR, errmsg!("cannot create temporary tables during a parallel operation")) /* C also: errcode */;
    }

    snprintf(
        namespaceName.as_mut_ptr(),
        NAMEDATALEN,
        b"pg_temp_{}\0".as_ptr() as *const c_char,
        MyProcNumber,
    );

    let mut namespaceId2 = get_namespace_oid(namespaceName.as_ptr(), true);
    if !OidIsValid(namespaceId2) {
        /*
         * First use of this temp namespace in this database; create it. The
         * temp namespaces are always owned by the superuser.  We leave their
         * permissions at default --- i.e., no access except to superuser ---
         * to ensure that unprivileged users can't peek at other backends'
         * temp tables.  This works because the places that access the temp
         * namespace for my own backend skip permissions checks on it.
         */
        namespaceId2 =
            NamespaceCreate(namespaceName.as_ptr(), BOOTSTRAP_SUPERUSERID, true);
        /* Advance command counter to make namespace visible */
        CommandCounterIncrement();
    } else {
        /*
         * If the namespace already exists, clean it out (in case the former
         * owner crashed without doing so).
         */
        RemoveTempRelations(namespaceId2);
    }

    /*
     * If the corresponding toast-table namespace doesn't exist yet, create
     * it. (We assume there is no need to clean it out if it does exist, since
     * dropping a parent table should make its toast table go away.)
     */
    snprintf(
        namespaceName.as_mut_ptr(),
        NAMEDATALEN,
        b"pg_toast_temp_{}\0".as_ptr() as *const c_char,
        MyProcNumber,
    );

    let mut toastspaceId2 = get_namespace_oid(namespaceName.as_ptr(), true);
    if !OidIsValid(toastspaceId2) {
        toastspaceId2 =
            NamespaceCreate(namespaceName.as_ptr(), BOOTSTRAP_SUPERUSERID, true);
        /* Advance command counter to make namespace visible */
        CommandCounterIncrement();
    }

    /*
     * Okay, we've prepared the temp namespace ... but it's not committed yet,
     * so all our work could be undone by transaction rollback.  Set flag for
     * AtEOXact_Namespace to know what to do.
     */
    myTempNamespace = namespaceId2;
    myTempToastNamespace = toastspaceId2;

    /*
     * Mark MyProc as owning this namespace which other processes can use to
     * decide if a temporary namespace is in use or not.  We assume that
     * assignment of namespaceId is an atomic operation.  Even if it is not,
     * the temporary relation which resulted in the creation of this temporary
     * namespace is still locked until the current transaction commits, and
     * its pg_namespace row is not visible yet.  However it does not matter:
     * this flag makes the namespace as being in use, so no objects created on
     * it would be removed concurrently.
     */
    (*MyProc).tempNamespaceId = namespaceId2;

    /* It should not be done already. */
    Assert!(myTempNamespaceSubID == InvalidSubTransactionId);
    myTempNamespaceSubID = GetCurrentSubTransactionId();

    baseSearchPathValid = false; /* need to rebuild list */
    searchPathCacheValid = false;
}

/*
 * End-of-transaction cleanup for namespaces.
 */
pub unsafe fn AtEOXact_Namespace(isCommit: bool, parallel: bool) {
    /*
     * If we abort the transaction in which a temp namespace was selected,
     * we'll have to do any creation or cleanout work over again.  So, just
     * forget the namespace entirely until next time.  On the other hand, if
     * we commit then register an exit callback to clean out the temp tables
     * at backend shutdown.  (We only want to register the callback once per
     * session, so this is a good place to do it.)
     */
    if myTempNamespaceSubID != InvalidSubTransactionId && !parallel {
        if isCommit {
            before_shmem_exit(Some(RemoveTempRelationsCallback), 0 as Datum);
        } else {
            myTempNamespace = InvalidOid;
            myTempToastNamespace = InvalidOid;
            baseSearchPathValid = false; /* need to rebuild list */
            searchPathCacheValid = false;

            /*
             * Reset the temporary namespace flag in MyProc.  We assume that
             * this operation is atomic.
             *
             * Because this transaction is aborting, the pg_namespace row is
             * not visible to anyone else anyway, but that doesn't matter:
             * it's not a problem if objects contained in this namespace are
             * removed concurrently.
             */
            (*MyProc).tempNamespaceId = InvalidOid;
        }
        myTempNamespaceSubID = InvalidSubTransactionId;
    }
}

/*
 * AtEOSubXact_Namespace
 *
 * At subtransaction commit, propagate the temp-namespace-creation
 * flag to the parent subtransaction.
 *
 * At subtransaction abort, forget the flag if we set it up.
 */
pub unsafe fn AtEOSubXact_Namespace(
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) {
    if myTempNamespaceSubID == mySubid {
        if isCommit {
            myTempNamespaceSubID = parentSubid;
        } else {
            myTempNamespaceSubID = InvalidSubTransactionId;
            /* TEMP namespace creation failed, so reset state */
            myTempNamespace = InvalidOid;
            myTempToastNamespace = InvalidOid;
            baseSearchPathValid = false; /* need to rebuild list */
            searchPathCacheValid = false;

            /*
             * Reset the temporary namespace flag in MyProc.  We assume that
             * this operation is atomic.
             *
             * Because this subtransaction is aborting, the pg_namespace row
             * is not visible to anyone else anyway, but that doesn't matter:
             * it's not a problem if objects contained in this namespace are
             * removed concurrently.
             */
            (*MyProc).tempNamespaceId = InvalidOid;
        }
    }
}

/*
 * Remove all relations in the specified temp namespace.
 *
 * This is called at backend shutdown (if we made any temp relations).
 * It is also called when we begin using a pre-existing temp namespace,
 * in order to clean out any relations that might have been created by
 * a crashed backend.
 */
unsafe fn RemoveTempRelations(tempNamespaceId: Oid) {
    let object: ObjectAddress = ObjectAddress {
        classId: NamespaceRelationId,
        objectId: tempNamespaceId,
        objectSubId: 0,
    };

    /*
     * We want to get rid of everything in the target namespace, but not the
     * namespace itself (deleting it only to recreate it later would be a
     * waste of cycles).  Hence, specify SKIP_ORIGINAL.  It's also an INTERNAL
     * deletion, and we want to not drop any extensions that might happen to
     * own temp objects.
     */
    performDeletion(
        &object,
        DropBehavior::DROP_CASCADE,
        PERFORM_DELETION_INTERNAL
            | PERFORM_DELETION_QUIETLY
            | PERFORM_DELETION_SKIP_ORIGINAL
            | PERFORM_DELETION_SKIP_EXTENSIONS,
    );
}

/*
 * Callback to remove temp relations at backend exit.
 */
unsafe extern "C" fn RemoveTempRelationsCallback(code: c_int, arg: Datum) {
    if OidIsValid(myTempNamespace) { /* should always be true */
        /* Need to ensure we have a usable transaction. */
        AbortOutOfAnyTransaction();
        StartTransactionCommand();
        PushActiveSnapshot(GetTransactionSnapshot());

        RemoveTempRelations(myTempNamespace);

        PopActiveSnapshot();
        CommitTransactionCommand();
    }
}

/*
 * Remove all temp tables from the temporary namespace.
 */
pub unsafe fn ResetTempTableNamespace() {
    if OidIsValid(myTempNamespace) {
        RemoveTempRelations(myTempNamespace);
    }
}


/*
 * Routines for handling the GUC variable 'search_path'.
 */

/* check_hook: validate new search_path value */
pub unsafe fn check_search_path(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: c_int,
) -> bool {
    let mut roleid: Oid = InvalidOid;
    let searchPath: *const c_char = *newval;
    let rawname: *mut c_char;
    let mut namelist: *mut List = core::ptr::null_mut();
    let use_cache: bool = !SearchPathCacheContext.is_null();

    /*
     * We used to try to check that the named schemas exist, but there are
     * many valid use-cases for having search_path settings that include
     * schemas that don't exist; and often, we are not inside a transaction
     * here and so can't consult the system catalogs anyway.  So now, the only
     * requirement is syntactic validity of the identifier list.
     *
     * Checking only the syntactic validity also allows us to use the search
     * path cache (if available) to avoid calling SplitIdentifierString() on
     * the same string repeatedly.
     */
    if use_cache {
        spcache_init();

        roleid = GetUserId();

        if !spcache_lookup(searchPath, roleid).is_null() {
            return true;
        }
    }

    /*
     * Ensure validity check succeeds before creating cache entry.
     */

    rawname = pstrdup(searchPath); /* need a modifiable copy */

    /* Parse string into list of identifiers */
    if !SplitIdentifierString(rawname, b',' as c_char, &mut namelist) {
        /* syntax error in name list */
        GUC_check_errdetail(b"List syntax is invalid.\0".as_ptr() as *const c_char);
        pfree(rawname as *mut c_void);
        list_free(namelist);
        return false;
    }
    pfree(rawname as *mut c_void);
    list_free(namelist);

    /* OK to create empty cache entry */
    if use_cache {
        spcache_insert(searchPath, roleid);
    }

    true
}

/* assign_hook: do extra actions as needed */
pub unsafe fn assign_search_path(newval: *const c_char, extra: *mut c_void) {
    /* don't access search_path during bootstrap */
    Assert!(!IsBootstrapProcessingMode());

    /*
     * We mark the path as needing recomputation, but don't do anything until
     * it's needed.  This avoids trying to do database access during GUC
     * initialization, or outside a transaction.
     *
     * This does not invalidate the search path cache, so if this value had
     * been previously set and no syscache invalidations happened,
     * recomputation may not be necessary.
     */
    baseSearchPathValid = false;
}

/*
 * InitializeSearchPath: initialize module during InitPostgres.
 *
 * This is called after we are up enough to be able to do catalog lookups.
 */
pub unsafe fn InitializeSearchPath() {
    if IsBootstrapProcessingMode() {
        /*
         * In bootstrap mode, the search path must be 'pg_catalog' so that
         * tables are created in the proper namespace; ignore the GUC setting.
         */
        let oldcxt: MemoryContext;

        oldcxt = MemoryContextSwitchTo(TopMemoryContext);
        baseSearchPath = list_make1_oid(PG_CATALOG_NAMESPACE);
        MemoryContextSwitchTo(oldcxt);
        baseCreationNamespace = PG_CATALOG_NAMESPACE;
        baseTempCreationPending = false;
        baseSearchPathValid = true;
        namespaceUser = GetUserId();
        activeSearchPath = baseSearchPath;
        activeCreationNamespace = baseCreationNamespace;
        activeTempCreationPending = baseTempCreationPending;
        activePathGeneration += 1; /* pro forma */
    } else {
        /*
         * In normal mode, arrange for a callback on any syscache invalidation
         * that will affect the search_path cache.
         */

        /* namespace name or ACLs may have changed */
        CacheRegisterSyscacheCallback(
            NAMESPACEOID,
            Some(InvalidationCallback),
            0 as Datum,
        );

        /* role name may affect the meaning of "$user" */
        CacheRegisterSyscacheCallback(AUTHOID, Some(InvalidationCallback), 0 as Datum);

        /* role membership may affect ACLs */
        CacheRegisterSyscacheCallback(
            AUTHMEMROLEMEM,
            Some(InvalidationCallback),
            0 as Datum,
        );

        /* database owner may affect ACLs */
        CacheRegisterSyscacheCallback(
            DATABASEOID,
            Some(InvalidationCallback),
            0 as Datum,
        );

        /* Force search path to be recomputed on next use */
        baseSearchPathValid = false;
        searchPathCacheValid = false;
    }
}

/*
 * InvalidationCallback
 *		Syscache inval callback function
 */
unsafe extern "C" fn InvalidationCallback(arg: Datum, cacheid: c_int, hashvalue: u32) {
    /*
     * Force search path to be recomputed on next use, also invalidating the
     * search path cache (because namespace names, ACLs, or role names may
     * have changed).
     */
    baseSearchPathValid = false;
    searchPathCacheValid = false;
}

/*
 * Fetch the active search path. The return value is a palloc'ed list
 * of OIDs; the caller is responsible for freeing this storage as
 * appropriate.
 *
 * The returned list includes the implicitly-prepended namespaces only if
 * includeImplicit is true.
 *
 * Note: calling this may result in a CommandCounterIncrement operation,
 * if we have to create or clean out the temp namespace.
 */
pub unsafe fn fetch_search_path(includeImplicit: bool) -> *mut List {
    let mut result: *mut List;

    recomputeNamespacePath();

    /*
     * If the temp namespace should be first, force it to exist.  This is so
     * that callers can trust the result to reflect the actual default
     * creation namespace.  It's a bit bogus to do this here, since
     * current_schema() is supposedly a stable function without side-effects,
     * but the alternatives seem worse.
     */
    if activeTempCreationPending {
        AccessTempTableNamespace(true);
        recomputeNamespacePath();
    }

    result = list_copy(activeSearchPath);
    if !includeImplicit {
        while !result.is_null() && linitial_oid(result) != activeCreationNamespace {
            result = list_delete_first(result);
        }
    }

    result
}

/*
 * Fetch the active search path into a caller-allocated array of OIDs.
 * Returns the number of path entries.  (If this is more than sarray_len,
 * then the data didn't fit and is not all stored.)
 *
 * The returned list always includes the implicitly-prepended namespaces,
 * but never includes the temp namespace.  (This is suitable for existing
 * users, which would want to ignore the temp namespace anyway.)  This
 * definition allows us to not worry about initializing the temp namespace.
 */
pub unsafe fn fetch_search_path_array(sarray: *mut Oid, sarray_len: c_int) -> c_int {
    let mut count: c_int = 0;
    let mut l: *mut ListCell;

    recomputeNamespacePath();

    l = list_head(activeSearchPath);
    while !l.is_null() {
        let namespaceId: Oid = lfirst_oid(l);

        if namespaceId == myTempNamespace {
            l = lnext(activeSearchPath, l);
            continue; /* do not include temp namespace */
        }

        if count < sarray_len {
            *sarray.add(count as usize) = namespaceId;
        }
        count += 1;
        l = lnext(activeSearchPath, l);
    }

    count
}


/*
 * Export the FooIsVisible functions as SQL-callable functions.
 *
 * Note: as of Postgres 8.4, these will silently return NULL if called on
 * a nonexistent object OID, rather than failing.  This is to avoid race
 * condition errors when a query that's scanning a catalog using an MVCC
 * snapshot uses one of these functions.  The underlying IsVisible functions
 * always use an up-to-date snapshot and so might see the object as already
 * gone when it's still visible to the transaction snapshot.
 */

pub unsafe fn pg_table_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = RelationIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_type_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = TypeIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_function_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = FunctionIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_operator_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = OperatorIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_opclass_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = OpclassIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_opfamily_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = OpfamilyIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_collation_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = CollationIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_conversion_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = ConversionIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_statistics_obj_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = StatisticsObjIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_ts_parser_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = TSParserIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_ts_dict_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = TSDictionaryIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_ts_template_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = TSTemplateIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_ts_config_is_visible(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: bool;
    let mut is_missing: bool = false;

    result = TSConfigIsVisibleExt(oid, &mut is_missing);

    if is_missing {
        PG_RETURN_NULL!(fcinfo)
    }
    PG_RETURN_BOOL!(result)
}

pub unsafe fn pg_my_temp_schema(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    PG_RETURN_OID!(myTempNamespace)
}

pub unsafe fn pg_is_other_temp_schema(fcinfo: *mut FunctionCallInfoBaseData) -> Datum {
    let oid: Oid = PG_GETARG_OID!(fcinfo, 0);

    PG_RETURN_BOOL!(isOtherTempNamespace(oid))
}

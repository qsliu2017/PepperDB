/*-------------------------------------------------------------------------
 *
 * subscriptioncmds.rs
 *		subscription catalog manipulation functions
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/backend/commands/subscriptioncmds.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};
use core::ptr;

use crate::access::htup_details::HeapTupleData;
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_subscription::{
    Form_pg_subscription, FormData_pg_subscription,
    LOGICALREP_ORIGIN_ANY, LOGICALREP_ORIGIN_NONE,
    LOGICALREP_STREAM_PARALLEL,
    LOGICALREP_TWOPHASE_STATE_DISABLED, LOGICALREP_TWOPHASE_STATE_ENABLED,
    LOGICALREP_TWOPHASE_STATE_PENDING,
};
use crate::catalog::pg_subscription_rel::{
    SUBREL_STATE_INIT, SUBREL_STATE_READY, SUBREL_STATE_SYNCDONE,
};
use crate::nodes::pg_list::{List, ListCell, NIL};
use crate::nodes::parsenodes::{
    AlterSubscriptionStmt, CreateSubscriptionStmt, DefElem, DropSubscriptionStmt,
    AlterSubscriptionType::{self, *},
};
use crate::nodes::nodes::NodeTag;
use crate::nodes::primnodes::RangeVar;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;
use crate::utils::rel::RelationData;
use crate::access::transam::xlogdefs::{XLogRecPtr, InvalidXLogRecPtr, XLogRecPtrIsInvalid};
use crate::access::transam::xlogreader::RepOriginId;
use crate::replication::logical::launcher::{
    logicalrep_workers_find, logicalrep_worker_stop,
};
use crate::replication::logical::origin::{
    replorigin_by_name, replorigin_create, replorigin_drop_by_name,
    replorigin_get_progress,
};
use crate::replication::logical::tablesync::{
    Subscription, SubscriptionRelState,
    UpdateTwoPhaseState,
};
use crate::replication::logicallauncher::{ApplyLauncherWakeupAtCommit, ApplyLauncherForgetWorkerStartTime};
use crate::replication::logicalworker::LogicalRepWorkersWakeupAtCommit;
use crate::replication::worker_internal::ReplicationOriginNameForLogicalRep;
use crate::replication::logical::tablesync::ReplicationSlotNameForTablesync;
use crate::replication::walreceiver::{walrcv_connect, walrcv_create_slot};
use crate::access::transam::twophase::LookupGXactBySubid;
use crate::executor::execReplication::CheckSubscriptionRelkind;
use crate::utils::activity::pgstat_subscription::{pgstat_create_subscription, pgstat_drop_subscription};
use crate::utils::fmgr::dfmgr::load_file;
use crate::{foreach, current_cell};

/* HeapTuple = *mut HeapTupleData */
type HeapTuple = *mut HeapTupleData;
type Relation = *mut RelationData;
type Datum = usize;
type TupleDesc = *mut c_void;
type MemoryContext = *mut c_void;
type bits32 = u32;
type TupleTableSlot = c_void;

/* -----------------------------------------------------------------------
 * Options that can be specified by the user in CREATE/ALTER SUBSCRIPTION
 * command.
 * ----------------------------------------------------------------------- */
const SUBOPT_CONNECT:            bits32 = 0x00000001;
const SUBOPT_ENABLED:            bits32 = 0x00000002;
const SUBOPT_CREATE_SLOT:        bits32 = 0x00000004;
const SUBOPT_SLOT_NAME:          bits32 = 0x00000008;
const SUBOPT_COPY_DATA:          bits32 = 0x00000010;
const SUBOPT_SYNCHRONOUS_COMMIT: bits32 = 0x00000020;
const SUBOPT_REFRESH:            bits32 = 0x00000040;
const SUBOPT_BINARY:             bits32 = 0x00000080;
const SUBOPT_STREAMING:          bits32 = 0x00000100;
const SUBOPT_TWOPHASE_COMMIT:    bits32 = 0x00000200;
const SUBOPT_DISABLE_ON_ERR:     bits32 = 0x00000400;
const SUBOPT_PASSWORD_REQUIRED:  bits32 = 0x00000800;
const SUBOPT_RUN_AS_OWNER:       bits32 = 0x00001000;
const SUBOPT_FAILOVER:           bits32 = 0x00002000;
const SUBOPT_LSN:                bits32 = 0x00004000;
const SUBOPT_ORIGIN:             bits32 = 0x00008000;

/* check if the 'val' has 'bits' set */
macro_rules! IsSet {
    ($val:expr, $bits:expr) => {
        (($val) & ($bits)) == ($bits)
    };
}

/* pg_subscription catalog attribute numbers  TODO(pg-port) */
const Anum_pg_subscription_oid:               usize = 1;
const Anum_pg_subscription_subdbid:           usize = 2;
const Anum_pg_subscription_subskiplsn:        usize = 3;
const Anum_pg_subscription_subname:           usize = 4;
const Anum_pg_subscription_subowner:          usize = 5;
const Anum_pg_subscription_subenabled:        usize = 6;
const Anum_pg_subscription_subbinary:         usize = 7;
const Anum_pg_subscription_substream:         usize = 8;
const Anum_pg_subscription_subtwophasestate:  usize = 9;
const Anum_pg_subscription_subdisableonerr:   usize = 10;
const Anum_pg_subscription_subpasswordrequired: usize = 11;
const Anum_pg_subscription_subrunasowner:     usize = 12;
const Anum_pg_subscription_subfailover:       usize = 13;
const Anum_pg_subscription_subconninfo:       usize = 14;
const Anum_pg_subscription_subslotname:       usize = 15;
const Anum_pg_subscription_subsynccommit:     usize = 16;
const Anum_pg_subscription_subpublications:   usize = 17;
const Anum_pg_subscription_suborigin:         usize = 18;
const Natts_pg_subscription: usize = 18;

/* OID constants  TODO(pg-port) */
const SubscriptionRelationId: Oid    = 6100;
const SubscriptionRelRelationId: Oid = 6101;
const SubscriptionObjectIndexId: Oid = 6102;
const DatabaseRelationId: Oid        = 1262;

/* syscache IDs  TODO(pg-port) */
const SUBSCRIPTIONNAME: c_int = 310;
const SUBSCRIPTIONOID:  c_int = 311;

/* lock modes  TODO(pg-port) */
const NoLock:             c_int = 0;
const AccessShareLock:    c_int = 1;
const RowExclusiveLock:   c_int = 5;
const AccessExclusiveLock: c_int = 8;

/* ACL  TODO(pg-port) */
const ACLCHECK_OK:       c_int = 0;
const ACLCHECK_NOT_OWNER: c_int = 1;
const ACL_CREATE:        c_int = 4;

/* ObjectType  TODO(pg-port) */
const OBJECT_DATABASE:     c_int = 10;
const OBJECT_SUBSCRIPTION: c_int = 50;

/* replication slot creation snapshot mode  TODO(pg-port) */
const CRS_NOEXPORT_SNAPSHOT: c_int = 2;

/* GUC set context  TODO(pg-port) */
const PGC_BACKEND:   c_int = 5;
const PGC_S_TEST:    c_int = 8;
const GUC_ACTION_SET: c_int = 0;

/* errcode constants  TODO(pg-port) */
const ERRCODE_SYNTAX_ERROR:                  c_int = 0x42601;
const ERRCODE_DUPLICATE_OBJECT:             c_int = 0x42710;
const ERRCODE_INSUFFICIENT_PRIVILEGE:       c_int = 0x28000;
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0x55000;
const ERRCODE_INVALID_PARAMETER_VALUE:      c_int = 0x22023;
const ERRCODE_FEATURE_NOT_SUPPORTED:        c_int = 0x0A000;
const ERRCODE_UNDEFINED_OBJECT:             c_int = 0x42704;
const ERRCODE_CONNECTION_FAILURE:           c_int = 0x08006;
const ERRCODE_INVALID_OBJECT_DEFINITION:    c_int = 0x42P16;

const ERROR:   c_int = 21;
const WARNING: c_int = 19;
const NOTICE:  c_int = 18;
const LOG:     c_int = 17;
const DEBUG1:  c_int = 15;

const NAMEDATALEN: usize = 64;
const TEXTOID: Oid = 25;
const INT2VECTOROID: Oid = 22;
const NAMEARRAYOID: Oid  = 1003;

/* WALRCV status codes  TODO(pg-port) */
const WALRCV_OK_COMMAND: c_int = 0;
const WALRCV_OK_TUPLES:  c_int = 1;
const WALRCV_ERROR:      c_int = 2;

/* ROLE_PG_CREATE_SUBSCRIPTION  TODO(pg-port) */
const ROLE_PG_CREATE_SUBSCRIPTION: Oid = 4545;

/* MyDatabaseId - global  TODO(pg-port) */
extern "C" {
    static mut MyDatabaseId: Oid;
    fn GetUserId() -> Oid;
    fn superuser() -> bool;
    fn superuser_arg(roleid: Oid) -> bool;
    fn has_privs_of_role(member: Oid, role: Oid) -> bool;
    fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: c_int) -> c_int;
    fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool;
    fn aclcheck_error(aclerr: c_int, objtype: c_int, objectname: *const c_char);
    fn check_can_set_role(member: Oid, role: Oid);

    fn table_open(oid: Oid, lockmode: c_int) -> Relation;
    fn table_close(rel: Relation, lockmode: c_int);

    fn GetSysCacheOid2(cacheId: c_int, oidattnum: i16, key1: Datum, key2: Datum) -> Oid;
    fn SearchSysCache2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple;
    fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple;
    fn SearchSysCacheCopy2(cacheId: c_int, key1: Datum, key2: Datum) -> HeapTuple;
    fn SysCacheGetAttr(cacheId: c_int, tup: HeapTuple, attnum: i16, isnull: *mut bool) -> Datum;
    fn SysCacheGetAttrNotNull(cacheId: c_int, tup: HeapTuple, attnum: i16) -> Datum;
    fn ReleaseSysCache(tup: HeapTuple);
    fn HeapTupleIsValid(tup: HeapTuple) -> bool;

    fn GETSTRUCT(tup: HeapTuple) -> *mut c_void;
    fn heap_form_tuple(tupdesc: TupleDesc, values: *mut Datum, isnull: *mut bool) -> HeapTuple;
    fn heap_modify_tuple(
        tuple: HeapTuple, tupdesc: TupleDesc,
        replValues: *mut Datum, replIsnull: *mut bool, doReplace: *mut bool,
    ) -> HeapTuple;
    fn heap_freetuple(tup: HeapTuple);

    fn RelationGetDescr(rel: Relation) -> TupleDesc;

    fn CatalogTupleInsert(rel: Relation, tup: HeapTuple) -> Oid;
    fn CatalogTupleUpdate(rel: Relation, otid: *mut c_void, tup: HeapTuple);
    fn CatalogTupleDelete(rel: Relation, tid: *mut c_void);

    fn GetNewOidWithIndex(rel: Relation, indexId: Oid, oidcolumn: i16) -> Oid;

    fn ObjectAddressSet(addr: *mut ObjectAddress, classId: Oid, objectId: Oid);
    fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int);
    fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int);
    fn InvokeObjectDropHook(classId: Oid, objectId: Oid, subId: c_int);
    fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid);
    fn changeDependencyOnOwner(classId: Oid, objectId: Oid, newOwner: Oid);
    fn deleteSharedDependencyRecordsFor(classId: Oid, objectId: Oid, objectSubId: c_int);
    fn EventTriggerSQLDropAddObject(object: *const ObjectAddress, original: bool, normal: bool);

    fn LockSharedObject(classid: Oid, objid: Oid, objsubid: c_int, lockmode: c_int);

    fn defGetString(def: *mut DefElem) -> *mut c_char;
    fn defGetBoolean(def: *mut DefElem) -> bool;
    fn errorConflictingDefElem(def: *mut DefElem, pstate: *mut ParseState);

    fn ReplicationSlotValidateName(name: *const c_char, elevel: c_int);
    fn set_config_option(
        name: *const c_char, value: *const c_char,
        context: c_int, source: c_int, action: c_int,
        changeVal: bool, elevel: c_int, is_reload: bool,
    ) -> bool;
    fn PreventInTransactionBlock(isTopLevel: bool, stmtType: *const c_char);

    fn walrcv_check_conninfo(conninfo: *const c_char, must_use_password: bool);
    fn walrcv_disconnect(conn: *mut c_void);
    fn walrcv_exec(conn: *mut c_void, cmd: *const c_char, nRetTypes: c_int, retTypes: *const Oid) -> *mut WalRcvExecResult;
    fn walrcv_clear_result(res: *mut WalRcvExecResult);
    fn walrcv_server_version(conn: *mut c_void) -> c_int;
    fn walrcv_alter_slot(conn: *mut c_void, slotname: *const c_char, failover: *const bool, twophase: *const bool);

    fn GetPublicationsStr(publications: *mut List, dest: *mut StringInfoData, quote_literal: bool);
    fn GetSubscription(subid: Oid, missing_ok: bool) -> *mut Subscription;
    fn GetSubscriptionRelations(subid: Oid, not_ready: bool) -> *mut List;
    fn GetSubscriptionRelState(subid: Oid, relid: Oid, lsn: *mut XLogRecPtr) -> c_char;
    fn AddSubscriptionRelState(subid: Oid, relid: Oid, state: c_char, sublsn: XLogRecPtr, retain_lock: bool);
    fn RemoveSubscriptionRel(subid: Oid, relid: Oid);

    fn RangeVarGetRelid(rv: *mut RangeVar, lockmode: c_int, missing_ok: bool) -> Oid;
    fn get_rel_relkind(relid: Oid) -> c_char;
    fn get_rel_name(relid: Oid) -> *mut c_char;
    fn get_rel_namespace(relid: Oid) -> Oid;
    fn get_namespace_name(nspid: Oid) -> *mut c_char;
    fn get_database_name(dbid: Oid) -> *mut c_char;
    fn OidIsValid(oid: Oid) -> bool;

    fn makeStringInfo() -> *mut StringInfoData;
    fn destroyStringInfo(str_: *mut StringInfoData);
    fn initStringInfo(str_: *mut StringInfoData);
    fn appendStringInfoString(str_: *mut StringInfoData, s: *const c_char);
    fn appendStringInfo(str_: *mut StringInfoData, fmt: *const c_char, ...);
    fn appendStringInfoChar(str_: *mut StringInfoData, ch: c_char);
    fn pfree(ptr: *mut c_void);
    fn pstrdup(s: *const c_char) -> *mut c_char;
    fn palloc(size: usize) -> *mut c_void;
    fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int;

    fn AllocSetContextCreate(parent: MemoryContext, name: *const c_char, minctxsize: usize, initblocksize: usize, maxblocksize: usize) -> MemoryContext;
    fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext;
    fn MemoryContextDelete(context: MemoryContext);

    fn construct_array_builtin(elems: *mut Datum, nelems: c_int, elmtype: Oid) -> *mut ArrayType;
    fn PointerGetDatum(ptr: *mut c_void) -> Datum;
    fn ObjectIdGetDatum(oid: Oid) -> Datum;
    fn BoolGetDatum(b: bool) -> Datum;
    fn CharGetDatum(ch: c_char) -> Datum;
    fn CStringGetDatum(s: *const c_char) -> Datum;
    fn CStringGetTextDatum(s: *const c_char) -> Datum;
    fn LSNGetDatum(lsn: XLogRecPtr) -> Datum;
    fn TextDatumGetCString(d: Datum) -> *mut c_char;
    fn DatumGetName(d: Datum) -> *mut NameData;
    fn DirectFunctionCall1(func: unsafe extern "C" fn(Datum) -> Datum, arg1: Datum) -> Datum;
    fn namein(s: Datum) -> Datum;
    fn pg_lsn_in(s: Datum) -> Datum;
    fn quote_identifier(ident: *const c_char) -> *const c_char;

    fn makeRangeVar(schemaname: *const c_char, relname: *const c_char, location: c_int) -> *mut RangeVar;
    fn makeString(str_: *const c_char) -> *mut c_void;
    fn strVal(v: *const c_void) -> *mut c_char;

    fn list_copy(list: *mut List) -> *mut List;
    fn list_delete(list: *mut List, ptr_: *mut c_void) -> *mut List;
    fn list_free(list: *mut List);
    fn list_length(list: *const List) -> c_int;
    fn list_member(list: *const List, datum: *const c_void) -> bool;
    fn list_append_unique(list: *mut List, datum: *mut c_void) -> *mut List;
    fn foreach_delete_current(list: *mut List, cell: *mut ListCell) -> *mut List;
    fn lappend(list: *mut List, datum: *mut c_void) -> *mut List;
    fn lfirst(lc: *const ListCell) -> *mut c_void;

    fn MakeSingleTupleTableSlot(tupdesc: TupleDesc, ops: *const c_void) -> *mut TupleTableSlot;
    fn ExecDropSingleTupleTableSlot(slot: *mut TupleTableSlot);
    fn ExecClearTuple(slot: *mut TupleTableSlot) -> *mut TupleTableSlot;
    fn slot_getattr(slot: *mut TupleTableSlot, attnum: c_int, isnull: *mut bool) -> Datum;
    fn tuplestore_gettupleslot(state: *mut c_void, forward: bool, copy: bool, slot: *mut TupleTableSlot) -> bool;

    fn nodeTag(node: *const c_void) -> NodeTag;
    fn intVal(v: *const c_void) -> c_int;

    static TTSOpsMinimalTuple: c_void;

    fn qsort(base: *mut c_void, nmemb: usize, size: usize, compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int);
    fn bsearch(key: *const c_void, base: *const c_void, nmemb: usize, size: usize, compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int) -> *mut c_void;
    fn oid_cmp(a: *const c_void, b: *const c_void) -> c_int;

    fn NameStr(n: *const NameData) -> *const c_char;
    static CurrentMemoryContext: MemoryContext;
}

/* ALLOCSET_DEFAULT_SIZES tuple TODO(pg-port) */
const ALLOCSET_DEFAULT_MINSIZE:  usize = 0;
const ALLOCSET_DEFAULT_INITSIZE: usize = 8192;
const ALLOCSET_DEFAULT_MAXSIZE:  usize = 8388608;

/* WalRcvExecResult  TODO(pg-port) */
#[repr(C)]
pub struct WalRcvExecResult {
    pub status:    c_int,
    pub sqlstate:  c_int,
    pub err:       *mut c_char,
    pub tupledesc: TupleDesc,
    pub tuplestore: *mut c_void,
}

/* ArrayType  TODO(pg-port) */
#[repr(C)]
pub struct ArrayType { _opaque: [u8; 0] }

/* NameData  TODO(pg-port) */
#[repr(C)]
pub struct NameData { pub data: [c_char; 64] }

/* StringInfoData  TODO(pg-port) */
#[repr(C)]
pub struct StringInfoData {
    pub data:   *mut c_char,
    pub len:    c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}

/*
 * Structure to hold a bitmap representing the user-provided CREATE/ALTER
 * SUBSCRIPTION command options and the parsed/default values of each of them.
 */
struct SubOpts {
    specified_opts:     bits32,
    slot_name:          *mut c_char,
    synchronous_commit: *mut c_char,
    connect:            bool,
    enabled:            bool,
    create_slot:        bool,
    copy_data:          bool,
    refresh:            bool,
    binary:             bool,
    streaming:          c_char,
    twophase:           bool,
    disableonerr:       bool,
    passwordrequired:   bool,
    runasowner:         bool,
    failover:           bool,
    origin:             *mut c_char,
    lsn:                XLogRecPtr,
}

impl SubOpts {
    fn zeroed() -> Self {
        SubOpts {
            specified_opts:     0,
            slot_name:          ptr::null_mut(),
            synchronous_commit: ptr::null_mut(),
            connect:            false,
            enabled:            false,
            create_slot:        false,
            copy_data:          false,
            refresh:            false,
            binary:             false,
            streaming:          0,
            twophase:           false,
            disableonerr:       false,
            passwordrequired:   false,
            runasowner:         false,
            failover:           false,
            origin:             ptr::null_mut(),
            lsn:                0,
        }
    }
}

/*
 * Common option parsing function for CREATE and ALTER SUBSCRIPTION commands.
 *
 * Since not all options can be specified in both commands, this function
 * will report an error if mutually exclusive options are specified.
 */
unsafe fn parse_subscription_options(
    pstate: *mut ParseState,
    stmt_options: *mut List,
    supported_opts: bits32,
    opts: *mut SubOpts,
) {
    let lc: *mut ListCell;

    /* Start out with cleared opts. */
    *opts = SubOpts::zeroed();

    /* caller must expect some option */
    Assert!(supported_opts != 0);

    /* If connect option is supported, these others also need to be. */
    Assert!(
        !IsSet!(supported_opts, SUBOPT_CONNECT) ||
        IsSet!(supported_opts, SUBOPT_ENABLED | SUBOPT_CREATE_SLOT | SUBOPT_COPY_DATA)
    );

    /* Set default values for the supported options. */
    if IsSet!(supported_opts, SUBOPT_CONNECT) {
        (*opts).connect = true;
    }
    if IsSet!(supported_opts, SUBOPT_ENABLED) {
        (*opts).enabled = true;
    }
    if IsSet!(supported_opts, SUBOPT_CREATE_SLOT) {
        (*opts).create_slot = true;
    }
    if IsSet!(supported_opts, SUBOPT_COPY_DATA) {
        (*opts).copy_data = true;
    }
    if IsSet!(supported_opts, SUBOPT_REFRESH) {
        (*opts).refresh = true;
    }
    if IsSet!(supported_opts, SUBOPT_BINARY) {
        (*opts).binary = false;
    }
    if IsSet!(supported_opts, SUBOPT_STREAMING) {
        (*opts).streaming = LOGICALREP_STREAM_PARALLEL;
    }
    if IsSet!(supported_opts, SUBOPT_TWOPHASE_COMMIT) {
        (*opts).twophase = false;
    }
    if IsSet!(supported_opts, SUBOPT_DISABLE_ON_ERR) {
        (*opts).disableonerr = false;
    }
    if IsSet!(supported_opts, SUBOPT_PASSWORD_REQUIRED) {
        (*opts).passwordrequired = true;
    }
    if IsSet!(supported_opts, SUBOPT_RUN_AS_OWNER) {
        (*opts).runasowner = false;
    }
    if IsSet!(supported_opts, SUBOPT_FAILOVER) {
        (*opts).failover = false;
    }
    if IsSet!(supported_opts, SUBOPT_ORIGIN) {
        (*opts).origin = pstrdup(LOGICALREP_ORIGIN_ANY.as_ptr() as *const c_char);
    }

    /* Parse options */
    foreach!(lc, stmt_options, {
        let defel = lfirst(lc) as *mut DefElem;

        if IsSet!(supported_opts, SUBOPT_CONNECT) &&
            libc_strcmp((*defel).defname, b"connect\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_CONNECT) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_CONNECT;
            (*opts).connect = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_ENABLED) &&
            libc_strcmp((*defel).defname, b"enabled\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_ENABLED) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_ENABLED;
            (*opts).enabled = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_CREATE_SLOT) &&
            libc_strcmp((*defel).defname, b"create_slot\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_CREATE_SLOT) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_CREATE_SLOT;
            (*opts).create_slot = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_SLOT_NAME) &&
            libc_strcmp((*defel).defname, b"slot_name\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_SLOT_NAME) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_SLOT_NAME;
            (*opts).slot_name = defGetString(defel);

            /* Setting slot_name = NONE is treated as no slot name. */
            if libc_strcmp((*opts).slot_name, b"none\0".as_ptr() as _) == 0 {
                (*opts).slot_name = ptr::null_mut();
            } else {
                ReplicationSlotValidateName((*opts).slot_name, ERROR);
            }
        } else if IsSet!(supported_opts, SUBOPT_COPY_DATA) &&
            libc_strcmp((*defel).defname, b"copy_data\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_COPY_DATA) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_COPY_DATA;
            (*opts).copy_data = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_SYNCHRONOUS_COMMIT) &&
            libc_strcmp((*defel).defname, b"synchronous_commit\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_SYNCHRONOUS_COMMIT) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_SYNCHRONOUS_COMMIT;
            (*opts).synchronous_commit = defGetString(defel);

            /* Test if the given value is valid for synchronous_commit GUC. */
            let _ = set_config_option(
                b"synchronous_commit\0".as_ptr() as _,
                (*opts).synchronous_commit,
                PGC_BACKEND, PGC_S_TEST, GUC_ACTION_SET,
                false, 0, false,
            );
        } else if IsSet!(supported_opts, SUBOPT_REFRESH) &&
            libc_strcmp((*defel).defname, b"refresh\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_REFRESH) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_REFRESH;
            (*opts).refresh = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_BINARY) &&
            libc_strcmp((*defel).defname, b"binary\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_BINARY) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_BINARY;
            (*opts).binary = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_STREAMING) &&
            libc_strcmp((*defel).defname, b"streaming\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_STREAMING) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_STREAMING;
            (*opts).streaming = defGetStreamingMode(defel);
        } else if IsSet!(supported_opts, SUBOPT_TWOPHASE_COMMIT) &&
            libc_strcmp((*defel).defname, b"two_phase\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_TWOPHASE_COMMIT) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_TWOPHASE_COMMIT;
            (*opts).twophase = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_DISABLE_ON_ERR) &&
            libc_strcmp((*defel).defname, b"disable_on_error\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_DISABLE_ON_ERR) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_DISABLE_ON_ERR;
            (*opts).disableonerr = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_PASSWORD_REQUIRED) &&
            libc_strcmp((*defel).defname, b"password_required\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_PASSWORD_REQUIRED) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_PASSWORD_REQUIRED;
            (*opts).passwordrequired = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_RUN_AS_OWNER) &&
            libc_strcmp((*defel).defname, b"run_as_owner\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_RUN_AS_OWNER) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_RUN_AS_OWNER;
            (*opts).runasowner = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_FAILOVER) &&
            libc_strcmp((*defel).defname, b"failover\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_FAILOVER) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_FAILOVER;
            (*opts).failover = defGetBoolean(defel);
        } else if IsSet!(supported_opts, SUBOPT_ORIGIN) &&
            libc_strcmp((*defel).defname, b"origin\0".as_ptr() as _) == 0
        {
            if IsSet!((*opts).specified_opts, SUBOPT_ORIGIN) {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts).specified_opts |= SUBOPT_ORIGIN;
            pfree((*opts).origin as *mut c_void);

            /*
             * Even though the "origin" parameter allows only "none" and "any"
             * values, it is implemented as a string type so that the
             * parameter can be extended in future versions to support
             * filtering using origin names specified by the user.
             */
            (*opts).origin = defGetString(defel);

            if (pg_strcasecmp((*opts).origin, LOGICALREP_ORIGIN_NONE.as_ptr() as _) != 0) &&
               (pg_strcasecmp((*opts).origin, LOGICALREP_ORIGIN_ANY.as_ptr() as _) != 0)
            {
                ereport!(ERROR, errmsg!("unrecognized origin value: \"{}\"", std::ffi::CStr::from_ptr((*opts).origin).to_string_lossy()) /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */);
            }
        } else if IsSet!(supported_opts, SUBOPT_LSN) &&
            libc_strcmp((*defel).defname, b"lsn\0".as_ptr() as _) == 0
        {
            let lsn_str: *mut c_char = defGetString(defel);
            let lsn: XLogRecPtr;

            if IsSet!((*opts).specified_opts, SUBOPT_LSN) {
                errorConflictingDefElem(defel, pstate);
            }

            /* Setting lsn = NONE is treated as resetting LSN */
            if libc_strcmp(lsn_str, b"none\0".as_ptr() as _) == 0 {
                lsn = InvalidXLogRecPtr;
            } else {
                /* Parse the argument as LSN */
                lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(lsn_str)));

                if XLogRecPtrIsInvalid(lsn) {
                    ereport!(ERROR, errmsg!("invalid WAL location (LSN): {}", std::ffi::CStr::from_ptr(lsn_str).to_string_lossy()) /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */);
                }
            }

            (*opts).specified_opts |= SUBOPT_LSN;
            (*opts).lsn = lsn;
        } else {
            ereport!(ERROR, errmsg!("unrecognized subscription parameter: \"{}\"", std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy()) /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);
        }
    });

    /*
     * We've been explicitly asked to not connect, that requires some
     * additional processing.
     */
    if !(*opts).connect && IsSet!(supported_opts, SUBOPT_CONNECT) {
        /* Check for incompatible options from the user. */
        if (*opts).enabled && IsSet!((*opts).specified_opts, SUBOPT_ENABLED) {
            ereport!(ERROR, errmsg!("{} and {} are mutually exclusive options", "connect = false", "enabled = true") /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);  /* translator: both %s are strings of the form "option = value" */
        }

        if (*opts).create_slot && IsSet!((*opts).specified_opts, SUBOPT_CREATE_SLOT) {
            ereport!(ERROR, errmsg!("{} and {} are mutually exclusive options", "connect = false", "create_slot = true") /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);
        }

        if (*opts).copy_data && IsSet!((*opts).specified_opts, SUBOPT_COPY_DATA) {
            ereport!(ERROR, errmsg!("{} and {} are mutually exclusive options", "connect = false", "copy_data = true") /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);
        }

        /* Change the defaults of other options. */
        (*opts).enabled = false;
        (*opts).create_slot = false;
        (*opts).copy_data = false;
    }

    /*
     * Do additional checking for disallowed combination when slot_name = NONE
     * was used.
     */
    if (*opts).slot_name.is_null() && IsSet!((*opts).specified_opts, SUBOPT_SLOT_NAME) {
        if (*opts).enabled {
            if IsSet!((*opts).specified_opts, SUBOPT_ENABLED) {
                ereport!(ERROR, errmsg!("{} and {} are mutually exclusive options", "slot_name = NONE", "enabled = true") /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);  /* translator: both %s are strings of the form "option = value" */
            } else {
                ereport!(ERROR, errmsg!("subscription with {} must also set {}", "slot_name = NONE", "enabled = false") /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);  /* translator: both %s are strings of the form "option = value" */
            }
        }

        if (*opts).create_slot {
            if IsSet!((*opts).specified_opts, SUBOPT_CREATE_SLOT) {
                ereport!(ERROR, errmsg!("{} and {} are mutually exclusive options", "slot_name = NONE", "create_slot = true") /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);  /* translator: both %s are strings of the form "option = value" */
            } else {
                ereport!(ERROR, errmsg!("subscription with {} must also set {}", "slot_name = NONE", "create_slot = false") /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);  /* translator: both %s are strings of the form "option = value" */
            }
        }
    }
}

/*
 * Check that the specified publications are present on the publisher.
 */
unsafe fn check_publications(wrconn: *mut c_void, publications: *mut List) {
    let mut res: *mut WalRcvExecResult;
    let cmd: *mut StringInfoData;
    let slot: *mut TupleTableSlot;
    let mut publicationsCopy: *mut List = NIL;
    let tableRow: [Oid; 1] = [TEXTOID];

    cmd = makeStringInfo();
    appendStringInfoString(cmd,
        b"SELECT t.pubname FROM\n pg_catalog.pg_publication t WHERE\n t.pubname IN (\0".as_ptr() as _);
    GetPublicationsStr(publications, cmd, true);
    appendStringInfoChar(cmd, b')' as c_char);

    res = walrcv_exec(wrconn, (*cmd).data, 1, tableRow.as_ptr());
    destroyStringInfo(cmd);

    if (*res).status != WALRCV_OK_TUPLES {
        ereport!(ERROR, errmsg!("could not receive list of publications from the publisher: {}", std::ffi::CStr::from_ptr((*res).err).to_string_lossy()));
    }

    publicationsCopy = list_copy(publications);

    /* Process publication(s). */
    slot = MakeSingleTupleTableSlot((*res).tupledesc, &TTSOpsMinimalTuple);
    while tuplestore_gettupleslot((*res).tuplestore, true, false, slot) {
        let mut pubname: *mut c_char;
        let mut isnull: bool = false;

        pubname = TextDatumGetCString(slot_getattr(slot, 1, &mut isnull));
        Assert!(!isnull);

        /* Delete the publication present in publisher from the list. */
        publicationsCopy = list_delete(publicationsCopy, makeString(pubname) as *mut c_void);
        ExecClearTuple(slot);
    }

    ExecDropSingleTupleTableSlot(slot);

    walrcv_clear_result(res);

    if list_length(publicationsCopy) > 0 {
        /* Prepare the list of non-existent publication(s) for error message. */
        let pubnames: *mut StringInfoData = makeStringInfo();

        GetPublicationsStr(publicationsCopy, pubnames, false);
        ereport!(WARNING, errmsg!("publication {} does not exist on the publisher", std::ffi::CStr::from_ptr((*pubnames).data).to_string_lossy()) /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */);
    }
}

/*
 * Auxiliary function to build a text array out of a list of String nodes.
 */
unsafe fn publicationListToArray(publist: *mut List) -> Datum {
    let arr: *mut ArrayType;
    let datums: *mut Datum;
    let memcxt: MemoryContext;
    let oldcxt: MemoryContext;

    /* Create memory context for temporary allocations. */
    memcxt = AllocSetContextCreate(
        CurrentMemoryContext,
        b"publicationListToArray to array\0".as_ptr() as _,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
    );
    oldcxt = MemoryContextSwitchTo(memcxt);

    datums = palloc(core::mem::size_of::<Datum>() * list_length(publist) as usize) as *mut Datum;

    check_duplicates_in_publist(publist, datums);

    MemoryContextSwitchTo(oldcxt);

    arr = construct_array_builtin(datums, list_length(publist), TEXTOID);

    MemoryContextDelete(memcxt);

    return PointerGetDatum(arr as *mut c_void);
}

/*
 * Create new subscription.
 */
pub unsafe fn CreateSubscription(
    pstate: *mut ParseState,
    stmt: *mut CreateSubscriptionStmt,
    isTopLevel: bool,
) -> ObjectAddress {
    let mut rel: Relation;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut subid: Oid;
    let mut nulls: [bool; Natts_pg_subscription] = [false; Natts_pg_subscription];
    let mut values: [Datum; Natts_pg_subscription] = [0; Natts_pg_subscription];
    let owner: Oid = GetUserId();
    let mut tup: HeapTuple;
    let mut conninfo: *mut c_char;
    let mut originname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let mut publications: *mut List;
    let supported_opts: bits32;
    let mut opts: SubOpts = SubOpts::zeroed();
    let aclresult: c_int;

    /*
     * Parse and check options.
     *
     * Connection and publication should not be specified here.
     */
    supported_opts = SUBOPT_CONNECT | SUBOPT_ENABLED | SUBOPT_CREATE_SLOT |
                     SUBOPT_SLOT_NAME | SUBOPT_COPY_DATA |
                     SUBOPT_SYNCHRONOUS_COMMIT | SUBOPT_BINARY |
                     SUBOPT_STREAMING | SUBOPT_TWOPHASE_COMMIT |
                     SUBOPT_DISABLE_ON_ERR | SUBOPT_PASSWORD_REQUIRED |
                     SUBOPT_RUN_AS_OWNER | SUBOPT_FAILOVER | SUBOPT_ORIGIN;
    parse_subscription_options(pstate, (*stmt).options, supported_opts, &mut opts);

    /*
     * Since creating a replication slot is not transactional, rolling back
     * the transaction leaves the created replication slot.  So we cannot run
     * CREATE SUBSCRIPTION inside a transaction block if creating a
     * replication slot.
     */
    if opts.create_slot {
        PreventInTransactionBlock(isTopLevel,
            b"CREATE SUBSCRIPTION ... WITH (create_slot = true)\0".as_ptr() as _);
    }

    /*
     * We don't want to allow unprivileged users to be able to trigger
     * attempts to access arbitrary network destinations, so require the user
     * to have been specifically authorized to create subscriptions.
     */
    if !has_privs_of_role(owner, ROLE_PG_CREATE_SUBSCRIPTION) {
        ereport!(ERROR, errmsg!("permission denied to create subscription") /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);  /* C also: errdetail("Only roles with privileges of the \"%s\" role may create subscriptions.", "pg_create_subscription") */
    }

    /*
     * Since a subscription is a database object, we also check for CREATE
     * permission on the database.
     */
    aclresult = object_aclcheck(DatabaseRelationId, MyDatabaseId, owner, ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_DATABASE, get_database_name(MyDatabaseId));
    }

    /*
     * Non-superusers are required to set a password for authentication, and
     * that password must be used by the target server, but the superuser can
     * exempt a subscription from this requirement.
     */
    if !opts.passwordrequired && !superuser_arg(owner) {
        ereport!(ERROR, errmsg!("password_required=false is superuser-only") /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);  /* C also: errhint("Subscriptions with the password_required option set to false may only be created or modified by the superuser.") */
    }

    /*
     * If built with appropriate switch, whine when regression-testing
     * conventions for subscription names are violated.
     */
    // #ifdef ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS not enabled

    rel = table_open(SubscriptionRelationId, RowExclusiveLock);

    /* Check if name is used */
    subid = GetSysCacheOid2(SUBSCRIPTIONNAME, Anum_pg_subscription_oid as i16,
                            MyDatabaseId as Datum, CStringGetDatum((*stmt).subname));
    if OidIsValid(subid) {
        ereport!(ERROR, errmsg!("subscription \"{}\" already exists", std::ffi::CStr::from_ptr((*stmt).subname).to_string_lossy()) /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */);
    }

    if !IsSet!(opts.specified_opts, SUBOPT_SLOT_NAME) && opts.slot_name.is_null() {
        opts.slot_name = (*stmt).subname;
    }

    /* The default for synchronous_commit of subscriptions is off. */
    if opts.synchronous_commit.is_null() {
        opts.synchronous_commit = b"off\0".as_ptr() as *mut c_char;
    }

    conninfo = (*stmt).conninfo;
    publications = (*stmt).publication;

    /* Load the library providing us libpq calls. */
    load_file(b"libpqwalreceiver\0".as_ptr() as _, false);

    /* Check the connection info string. */
    walrcv_check_conninfo(conninfo, opts.passwordrequired && !superuser());

    /* Everything ok, form a new tuple. */
    ptr::write_bytes(values.as_mut_ptr(), 0, Natts_pg_subscription);
    ptr::write_bytes(nulls.as_mut_ptr() as *mut u8, 0, Natts_pg_subscription);

    subid = GetNewOidWithIndex(rel, SubscriptionObjectIndexId,
                               Anum_pg_subscription_oid as i16);
    values[Anum_pg_subscription_oid - 1] = ObjectIdGetDatum(subid);
    values[Anum_pg_subscription_subdbid - 1] = ObjectIdGetDatum(MyDatabaseId);
    values[Anum_pg_subscription_subskiplsn - 1] = LSNGetDatum(InvalidXLogRecPtr);
    values[Anum_pg_subscription_subname - 1] =
        DirectFunctionCall1(namein, CStringGetDatum((*stmt).subname));
    values[Anum_pg_subscription_subowner - 1] = ObjectIdGetDatum(owner);
    values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(opts.enabled);
    values[Anum_pg_subscription_subbinary - 1] = BoolGetDatum(opts.binary);
    values[Anum_pg_subscription_substream - 1] = CharGetDatum(opts.streaming);
    values[Anum_pg_subscription_subtwophasestate - 1] =
        CharGetDatum(if opts.twophase {
            LOGICALREP_TWOPHASE_STATE_PENDING
        } else {
            LOGICALREP_TWOPHASE_STATE_DISABLED
        });
    values[Anum_pg_subscription_subdisableonerr - 1] = BoolGetDatum(opts.disableonerr);
    values[Anum_pg_subscription_subpasswordrequired - 1] = BoolGetDatum(opts.passwordrequired);
    values[Anum_pg_subscription_subrunasowner - 1] = BoolGetDatum(opts.runasowner);
    values[Anum_pg_subscription_subfailover - 1] = BoolGetDatum(opts.failover);
    values[Anum_pg_subscription_subconninfo - 1] = CStringGetTextDatum(conninfo);
    if !opts.slot_name.is_null() {
        values[Anum_pg_subscription_subslotname - 1] =
            DirectFunctionCall1(namein, CStringGetDatum(opts.slot_name));
    } else {
        nulls[Anum_pg_subscription_subslotname - 1] = true;
    }
    values[Anum_pg_subscription_subsynccommit - 1] =
        CStringGetTextDatum(opts.synchronous_commit);
    values[Anum_pg_subscription_subpublications - 1] =
        publicationListToArray(publications);
    values[Anum_pg_subscription_suborigin - 1] =
        CStringGetTextDatum(opts.origin);

    tup = heap_form_tuple(RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr());

    /* Insert tuple into catalog. */
    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    recordDependencyOnOwner(SubscriptionRelationId, subid, owner);

    ReplicationOriginNameForLogicalRep(subid, InvalidOid, originname.as_mut_ptr(),
                                       core::mem::size_of::<[c_char; NAMEDATALEN]>() as c_int);
    replorigin_create(originname.as_mut_ptr());

    /*
     * Connect to remote side to execute requested commands and fetch table
     * info.
     */
    if opts.connect {
        let mut err: *mut c_char = ptr::null_mut();
        let wrconn: *mut c_void;
        let mut tables: *mut List;
        let lc: *mut ListCell;
        let table_state: c_char;
        let must_use_password: bool;

        /* Try to connect to the publisher. */
        must_use_password = !superuser_arg(owner) && opts.passwordrequired;
        wrconn = walrcv_connect(conninfo, true, true, must_use_password,
                                (*stmt).subname, &mut err);
        if wrconn.is_null() {
            ereport!(ERROR, errmsg!("subscription \"{}\" could not connect to the publisher: {}", std::ffi::CStr::from_ptr((*stmt).subname).to_string_lossy(), std::ffi::CStr::from_ptr(err).to_string_lossy()) /* C also: errcode(ERRCODE_CONNECTION_FAILURE) */);
        }

        /* PG_TRY */
        {
            check_publications(wrconn, publications);
            check_publications_origin(wrconn, publications, opts.copy_data,
                                      opts.origin, ptr::null_mut(), 0, (*stmt).subname);

            /*
             * Set sync state based on if we were asked to do data copy or
             * not.
             */
            table_state = if opts.copy_data { SUBREL_STATE_INIT } else { SUBREL_STATE_READY };

            /*
             * Get the table list from publisher and build local table status
             * info.
             */
            tables = fetch_table_list(wrconn, publications);
            foreach!(lc, tables, {
                let rv: *mut RangeVar = lfirst(current_cell!(lc)) as *mut RangeVar;
                let relid: Oid;

                relid = RangeVarGetRelid(rv, AccessShareLock, false);

                /* Check for supported relkind. */
                CheckSubscriptionRelkind(get_rel_relkind(relid),
                                         (*rv).schemaname, (*rv).relname);

                AddSubscriptionRelState(subid, relid, table_state,
                                        InvalidXLogRecPtr, true);
            });

            /*
             * If requested, create permanent slot for the subscription. We
             * won't use the initial snapshot for anything, so no need to
             * export it.
             */
            if opts.create_slot {
                let mut twophase_enabled: bool = false;

                Assert!(!opts.slot_name.is_null());

                /*
                 * Even if two_phase is set, don't create the slot with
                 * two-phase enabled. Will enable it once all the tables are
                 * synced and ready. This avoids race-conditions like prepared
                 * transactions being skipped due to changes not being applied
                 * due to checks in should_apply_changes_for_rel() when
                 * tablesync for the corresponding tables are in progress. See
                 * comments atop worker.c.
                 *
                 * Note that if tables were specified but copy_data is false
                 * then it is safe to enable two_phase up-front because those
                 * tables are already initially in READY state. When the
                 * subscription has no tables, we leave the twophase state as
                 * PENDING, to allow ALTER SUBSCRIPTION ... REFRESH
                 * PUBLICATION to work.
                 */
                if opts.twophase && !opts.copy_data && tables != NIL {
                    twophase_enabled = true;
                }

                walrcv_create_slot(wrconn, opts.slot_name, false, twophase_enabled,
                                   opts.failover, CRS_NOEXPORT_SNAPSHOT, ptr::null_mut());

                if twophase_enabled {
                    UpdateTwoPhaseState(subid, LOGICALREP_TWOPHASE_STATE_ENABLED);
                }

                ereport!(NOTICE, errmsg!("created replication slot \"{}\" on publisher", std::ffi::CStr::from_ptr(opts.slot_name).to_string_lossy()));
            }
        }
        /* PG_FINALLY */
        walrcv_disconnect(wrconn);
        /* PG_END_TRY */
    } else {
        ereport!(WARNING, errmsg!("subscription was created, but is not connected"));  /* C also: errhint("To initiate replication, you must manually create the replication slot, enable the subscription, and refresh the subscription.") */
    }

    table_close(rel, RowExclusiveLock);

    pgstat_create_subscription(subid);

    if opts.enabled {
        ApplyLauncherWakeupAtCommit();
    }

    ObjectAddressSet(&mut myself, SubscriptionRelationId, subid);

    InvokeObjectPostCreateHook(SubscriptionRelationId, subid, 0);

    return myself;
}

unsafe fn AlterSubscription_refresh(
    sub: *mut Subscription,
    copy_data: bool,
    validate_publications: *mut List,
) {
    let mut err: *mut c_char = ptr::null_mut();
    let mut pubrel_names: *mut List;
    let mut subrel_states: *mut List;
    let mut subrel_local_oids: *mut Oid;
    let mut pubrel_local_oids: *mut Oid;
    let lc: *mut ListCell;
    let mut off: c_int;
    let mut remove_rel_len: c_int;
    let mut subrel_count: c_int;
    let mut rel: Relation = ptr::null_mut();
    let mut sub_remove_rels: *mut SubRemoveRels;
    let wrconn: *mut c_void;
    let must_use_password: bool;

    /* Load the library providing us libpq calls. */
    load_file(b"libpqwalreceiver\0".as_ptr() as _, false);

    /* Try to connect to the publisher. */
    must_use_password = (*sub).passwordrequired && !(*sub).ownersuperuser;
    wrconn = walrcv_connect((*sub).conninfo, true, true, must_use_password,
                            (*sub).name, &mut err);
    if wrconn.is_null() {
        ereport!(ERROR, errmsg!("subscription \"{}\" could not connect to the publisher: {}", std::ffi::CStr::from_ptr((*sub).name).to_string_lossy(), std::ffi::CStr::from_ptr(err).to_string_lossy()) /* C also: errcode(ERRCODE_CONNECTION_FAILURE) */);
    }

    /* PG_TRY */
    {
        if !validate_publications.is_null() {
            check_publications(wrconn, validate_publications);
        }

        /* Get the table list from publisher. */
        pubrel_names = fetch_table_list(wrconn, (*sub).publications);

        /* Get local table list. */
        subrel_states = GetSubscriptionRelations((*sub).oid, false);
        subrel_count = list_length(subrel_states);

        /*
         * Build qsorted array of local table oids for faster lookup. This can
         * potentially contain all tables in the database so speed of lookup
         * is important.
         */
        subrel_local_oids = palloc(subrel_count as usize * core::mem::size_of::<Oid>()) as *mut Oid;
        off = 0;
        foreach!(lc, subrel_states, {
            let relstate: *mut SubscriptionRelState = lfirst(current_cell!(lc)) as *mut SubscriptionRelState;

            *subrel_local_oids.add(off as usize) = (*relstate).relid;
            off += 1;
        });
        qsort(subrel_local_oids as *mut c_void, subrel_count as usize,
              core::mem::size_of::<Oid>(), oid_cmp);

        check_publications_origin(wrconn, (*sub).publications, copy_data,
                                  (*sub).origin, subrel_local_oids,
                                  subrel_count, (*sub).name);

        /*
         * Rels that we want to remove from subscription and drop any slots
         * and origins corresponding to them.
         */
        sub_remove_rels = palloc(subrel_count as usize * core::mem::size_of::<SubRemoveRels>()) as *mut SubRemoveRels;

        /*
         * Walk over the remote tables and try to match them to locally known
         * tables. If the table is not known locally create a new state for
         * it.
         *
         * Also builds array of local oids of remote tables for the next step.
         */
        off = 0;
        pubrel_local_oids = palloc(list_length(pubrel_names) as usize * core::mem::size_of::<Oid>()) as *mut Oid;

        foreach!(lc, pubrel_names, {
            let rv: *mut RangeVar = lfirst(current_cell!(lc)) as *mut RangeVar;
            let relid: Oid;

            relid = RangeVarGetRelid(rv, AccessShareLock, false);

            /* Check for supported relkind. */
            CheckSubscriptionRelkind(get_rel_relkind(relid),
                                     (*rv).schemaname, (*rv).relname);

            *pubrel_local_oids.add(off as usize) = relid;
            off += 1;

            if bsearch(&relid as *const Oid as *const c_void, subrel_local_oids as *const c_void,
                       subrel_count as usize, core::mem::size_of::<Oid>(), oid_cmp).is_null() {
                AddSubscriptionRelState((*sub).oid, relid,
                                        if copy_data { SUBREL_STATE_INIT } else { SUBREL_STATE_READY },
                                        InvalidXLogRecPtr, true);
                ereport!(DEBUG1, errmsg!("table \"{}.{}\" added to subscription \"{}\"", std::ffi::CStr::from_ptr((*rv).schemaname).to_string_lossy(), std::ffi::CStr::from_ptr((*rv).relname).to_string_lossy(), std::ffi::CStr::from_ptr((*sub).name).to_string_lossy()));
            }
        });

        /*
         * Next remove state for tables we should not care about anymore using
         * the data we collected above
         */
        qsort(pubrel_local_oids as *mut c_void, list_length(pubrel_names) as usize,
              core::mem::size_of::<Oid>(), oid_cmp);

        remove_rel_len = 0;
        off = 0;
        while off < subrel_count {
            let relid: Oid = *subrel_local_oids.add(off as usize);

            if bsearch(&relid as *const Oid as *const c_void, pubrel_local_oids as *const c_void,
                       list_length(pubrel_names) as usize, core::mem::size_of::<Oid>(), oid_cmp).is_null() {
                let state: c_char;
                let mut statelsn: XLogRecPtr = 0;

                /*
                 * Lock pg_subscription_rel with AccessExclusiveLock to
                 * prevent any race conditions with the apply worker
                 * re-launching workers at the same time this code is trying
                 * to remove those tables.
                 *
                 * Even if new worker for this particular rel is restarted it
                 * won't be able to make any progress as we hold exclusive
                 * lock on pg_subscription_rel till the transaction end. It
                 * will simply exit as there is no corresponding rel entry.
                 *
                 * This locking also ensures that the state of rels won't
                 * change till we are done with this refresh operation.
                 */
                if rel.is_null() {
                    rel = table_open(SubscriptionRelRelationId, AccessExclusiveLock);
                }

                /* Last known rel state. */
                state = GetSubscriptionRelState((*sub).oid, relid, &mut statelsn);

                (*sub_remove_rels.add(remove_rel_len as usize)).relid = relid;
                (*sub_remove_rels.add(remove_rel_len as usize)).state = state;
                remove_rel_len += 1;

                RemoveSubscriptionRel((*sub).oid, relid);

                logicalrep_worker_stop((*sub).oid, relid);

                /*
                 * For READY state, we would have already dropped the
                 * tablesync origin.
                 */
                if state != SUBREL_STATE_READY {
                    let mut originname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

                    /*
                     * Drop the tablesync's origin tracking if exists.
                     *
                     * It is possible that the origin is not yet created for
                     * tablesync worker, this can happen for the states before
                     * SUBREL_STATE_DATASYNC. The tablesync worker or apply
                     * worker can also concurrently try to drop the origin and
                     * by this time the origin might be already removed. For
                     * these reasons, passing missing_ok = true.
                     */
                    ReplicationOriginNameForLogicalRep((*sub).oid, relid, originname.as_mut_ptr(),
                                                       core::mem::size_of::<[c_char; NAMEDATALEN]>() as c_int);
                    replorigin_drop_by_name(originname.as_mut_ptr(), true, false);
                }

                ereport!(DEBUG1, errmsg!("table \"{}.{}\" removed from subscription \"{}\"", std::ffi::CStr::from_ptr(get_namespace_name(get_rel_namespace(relid))).to_string_lossy(), std::ffi::CStr::from_ptr(get_rel_name(relid)).to_string_lossy(), std::ffi::CStr::from_ptr((*sub).name).to_string_lossy()));
            }
            off += 1;
        }

        /*
         * Drop the tablesync slots associated with removed tables. This has
         * to be at the end because otherwise if there is an error while doing
         * the database operations we won't be able to rollback dropped slots.
         */
        off = 0;
        while off < remove_rel_len {
            if (*sub_remove_rels.add(off as usize)).state != SUBREL_STATE_READY &&
               (*sub_remove_rels.add(off as usize)).state != SUBREL_STATE_SYNCDONE {
                let mut syncslotname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

                /*
                 * For READY/SYNCDONE states we know the tablesync slot has
                 * already been dropped by the tablesync worker.
                 *
                 * For other states, there is no certainty, maybe the slot
                 * does not exist yet. Also, if we fail after removing some of
                 * the slots, next time, it will again try to drop already
                 * dropped slots and fail. For these reasons, we allow
                 * missing_ok = true for the drop.
                 */
                ReplicationSlotNameForTablesync((*sub).oid, (*sub_remove_rels.add(off as usize)).relid,
                                                syncslotname.as_mut_ptr(),
                                                core::mem::size_of::<[c_char; NAMEDATALEN]>());
                ReplicationSlotDropAtPubNode(wrconn, syncslotname.as_mut_ptr(), true);
            }
            off += 1;
        }
    }
    /* PG_FINALLY */
    walrcv_disconnect(wrconn);
    /* PG_END_TRY */

    if !rel.is_null() {
        table_close(rel, NoLock);
    }
}

/*
 * Common checks for altering failover and two_phase options.
 */
unsafe fn CheckAlterSubOption(
    sub: *mut Subscription,
    option: *const c_char,
    slot_needs_update: bool,
    isTopLevel: bool,
) {
    /*
     * The checks in this function are required only for failover and
     * two_phase options.
     */
    Assert!(libc_strcmp(option, b"failover\0".as_ptr() as _) == 0 ||
            libc_strcmp(option, b"two_phase\0".as_ptr() as _) == 0);

    /*
     * Do not allow changing the option if the subscription is enabled. This
     * is because both failover and two_phase options of the slot on the
     * publisher cannot be modified if the slot is currently acquired by the
     * existing walsender.
     *
     * Note that two_phase is enabled (aka changed from 'false' to 'true') on
     * the publisher by the existing walsender, so we could have allowed that
     * even when the subscription is enabled. But we kept this restriction for
     * the sake of consistency and simplicity.
     */
    if (*sub).enabled {
        ereport!(ERROR, errmsg!("cannot set option \"{}\" for enabled subscription", std::ffi::CStr::from_ptr(option).to_string_lossy()) /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);
    }

    if slot_needs_update {
        let mut cmd: StringInfoData = core::mem::zeroed();

        /*
         * A valid slot must be associated with the subscription for us to
         * modify any of the slot's properties.
         */
        if (*sub).slotname.is_null() {
            ereport!(ERROR, errmsg!("cannot set option \"{}\" for a subscription that does not have a slot name", std::ffi::CStr::from_ptr(option).to_string_lossy()) /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);
        }

        /* The changed option of the slot can't be rolled back. */
        initStringInfo(&mut cmd);
        appendStringInfo(&mut cmd, b"ALTER SUBSCRIPTION ... SET (%s)\0".as_ptr() as _, option);

        PreventInTransactionBlock(isTopLevel, cmd.data);
        pfree(cmd.data as *mut c_void);
    }
}

/*
 * Alter the existing subscription.
 */
pub unsafe fn AlterSubscription(
    pstate: *mut ParseState,
    stmt: *mut AlterSubscriptionStmt,
    isTopLevel: bool,
) -> ObjectAddress {
    let rel: Relation;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut nulls: [bool; Natts_pg_subscription] = [false; Natts_pg_subscription];
    let mut replaces: [bool; Natts_pg_subscription] = [false; Natts_pg_subscription];
    let mut values: [Datum; Natts_pg_subscription] = [0; Natts_pg_subscription];
    let mut tup: HeapTuple;
    let subid: Oid;
    let mut update_tuple: bool = false;
    let mut update_failover: bool = false;
    let mut update_two_phase: bool = false;
    let sub: *mut Subscription;
    let form: Form_pg_subscription;
    let supported_opts: bits32;
    let mut opts: SubOpts = SubOpts::zeroed();

    rel = table_open(SubscriptionRelationId, RowExclusiveLock);

    /* Fetch the existing tuple. */
    tup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, MyDatabaseId as Datum,
                              CStringGetDatum((*stmt).subname));

    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, errmsg!("subscription \"{}\" does not exist", std::ffi::CStr::from_ptr((*stmt).subname).to_string_lossy()) /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */);
    }

    form = GETSTRUCT(tup) as Form_pg_subscription;
    subid = (*form).oid;

    /* must be owner */
    if !object_ownercheck(SubscriptionRelationId, subid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SUBSCRIPTION, (*stmt).subname);
    }

    sub = GetSubscription(subid, false);

    /*
     * Don't allow non-superuser modification of a subscription with
     * password_required=false.
     */
    if !(*sub).passwordrequired && !superuser() {
        ereport!(ERROR, errmsg!("password_required=false is superuser-only") /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);  /* C also: errhint("Subscriptions with the password_required option set to false may only be created or modified by the superuser.") */
    }

    /* Lock the subscription so nobody else can do anything with it. */
    LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock);

    /* Form a new tuple. */
    ptr::write_bytes(values.as_mut_ptr(), 0, Natts_pg_subscription);
    ptr::write_bytes(nulls.as_mut_ptr() as *mut u8, 0, Natts_pg_subscription);
    ptr::write_bytes(replaces.as_mut_ptr() as *mut u8, 0, Natts_pg_subscription);

    match (*stmt).kind {
        ALTER_SUBSCRIPTION_OPTIONS => {
            supported_opts = SUBOPT_SLOT_NAME |
                             SUBOPT_SYNCHRONOUS_COMMIT | SUBOPT_BINARY |
                             SUBOPT_STREAMING | SUBOPT_TWOPHASE_COMMIT |
                             SUBOPT_DISABLE_ON_ERR |
                             SUBOPT_PASSWORD_REQUIRED |
                             SUBOPT_RUN_AS_OWNER | SUBOPT_FAILOVER |
                             SUBOPT_ORIGIN;

            parse_subscription_options(pstate, (*stmt).options, supported_opts, &mut opts);

            if IsSet!(opts.specified_opts, SUBOPT_SLOT_NAME) {
                /*
                 * The subscription must be disabled to allow slot_name as
                 * 'none', otherwise, the apply worker will repeatedly try
                 * to stream the data using that slot_name which neither
                 * exists on the publisher nor the user will be allowed to
                 * create it.
                 */
                if (*sub).enabled && opts.slot_name.is_null() {
                    ereport!(ERROR, errmsg!("cannot set {} for enabled subscription", "slot_name = NONE") /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);
                }

                if !opts.slot_name.is_null() {
                    values[Anum_pg_subscription_subslotname - 1] =
                        DirectFunctionCall1(namein, CStringGetDatum(opts.slot_name));
                } else {
                    nulls[Anum_pg_subscription_subslotname - 1] = true;
                }
                replaces[Anum_pg_subscription_subslotname - 1] = true;
            }

            if !opts.synchronous_commit.is_null() {
                values[Anum_pg_subscription_subsynccommit - 1] =
                    CStringGetTextDatum(opts.synchronous_commit);
                replaces[Anum_pg_subscription_subsynccommit - 1] = true;
            }

            if IsSet!(opts.specified_opts, SUBOPT_BINARY) {
                values[Anum_pg_subscription_subbinary - 1] =
                    BoolGetDatum(opts.binary);
                replaces[Anum_pg_subscription_subbinary - 1] = true;
            }

            if IsSet!(opts.specified_opts, SUBOPT_STREAMING) {
                values[Anum_pg_subscription_substream - 1] =
                    CharGetDatum(opts.streaming);
                replaces[Anum_pg_subscription_substream - 1] = true;
            }

            if IsSet!(opts.specified_opts, SUBOPT_DISABLE_ON_ERR) {
                values[Anum_pg_subscription_subdisableonerr - 1] =
                    BoolGetDatum(opts.disableonerr);
                replaces[Anum_pg_subscription_subdisableonerr - 1] = true;
            }

            if IsSet!(opts.specified_opts, SUBOPT_PASSWORD_REQUIRED) {
                /* Non-superuser may not disable password_required. */
                if !opts.passwordrequired && !superuser() {
                    ereport!(ERROR, errmsg!("password_required=false is superuser-only") /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);  /* C also: errhint("Subscriptions with the password_required option set to false may only be created or modified by the superuser.") */
                }

                values[Anum_pg_subscription_subpasswordrequired - 1] =
                    BoolGetDatum(opts.passwordrequired);
                replaces[Anum_pg_subscription_subpasswordrequired - 1] = true;
            }

            if IsSet!(opts.specified_opts, SUBOPT_RUN_AS_OWNER) {
                values[Anum_pg_subscription_subrunasowner - 1] =
                    BoolGetDatum(opts.runasowner);
                replaces[Anum_pg_subscription_subrunasowner - 1] = true;
            }

            if IsSet!(opts.specified_opts, SUBOPT_TWOPHASE_COMMIT) {
                /*
                 * We need to update both the slot and the subscription
                 * for the two_phase option. We can enable the two_phase
                 * option for a slot only once the initial data
                 * synchronization is done. This is to avoid missing some
                 * data as explained in comments atop worker.c.
                 */
                update_two_phase = !opts.twophase;

                CheckAlterSubOption(sub, b"two_phase\0".as_ptr() as _, update_two_phase, isTopLevel);

                /*
                 * Modifying the two_phase slot option requires a slot
                 * lookup by slot name, so changing the slot name at the
                 * same time is not allowed.
                 */
                if update_two_phase && IsSet!(opts.specified_opts, SUBOPT_SLOT_NAME) {
                    ereport!(ERROR, errmsg!("\"slot_name\" and \"two_phase\" cannot be altered at the same time") /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);
                }

                /*
                 * Note that workers may still survive even if the
                 * subscription has been disabled.
                 *
                 * Ensure workers have already been exited to avoid
                 * getting prepared transactions while we are disabling
                 * the two_phase option. Otherwise, the changes of an
                 * already prepared transaction can be replicated again
                 * along with its corresponding commit, leading to
                 * duplicate data or errors.
                 */
                if logicalrep_workers_find(subid, true, true) {
                    ereport!(ERROR, errmsg!("cannot alter \"two_phase\" when logical replication worker is still running") /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);  /* C also: errhint("Try again after some time.") */
                }

                /*
                 * two_phase cannot be disabled if there are any
                 * uncommitted prepared transactions present otherwise it
                 * can lead to duplicate data or errors as explained in
                 * the comment above.
                 */
                if update_two_phase &&
                   (*sub).twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED &&
                   LookupGXactBySubid(subid) {
                    ereport!(ERROR, errmsg!("cannot disable \"two_phase\" when prepared transactions exist") /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);  /* C also: errhint("Resolve these transactions and try again.") */
                }

                /* Change system catalog accordingly */
                values[Anum_pg_subscription_subtwophasestate - 1] =
                    CharGetDatum(if opts.twophase {
                        LOGICALREP_TWOPHASE_STATE_PENDING
                    } else {
                        LOGICALREP_TWOPHASE_STATE_DISABLED
                    });
                replaces[Anum_pg_subscription_subtwophasestate - 1] = true;
            }

            if IsSet!(opts.specified_opts, SUBOPT_FAILOVER) {
                /*
                 * Similar to the two_phase case above, we need to update
                 * the failover option for both the slot and the
                 * subscription.
                 */
                update_failover = true;

                CheckAlterSubOption(sub, b"failover\0".as_ptr() as _, update_failover, isTopLevel);

                values[Anum_pg_subscription_subfailover - 1] =
                    BoolGetDatum(opts.failover);
                replaces[Anum_pg_subscription_subfailover - 1] = true;
            }

            if IsSet!(opts.specified_opts, SUBOPT_ORIGIN) {
                values[Anum_pg_subscription_suborigin - 1] =
                    CStringGetTextDatum(opts.origin);
                replaces[Anum_pg_subscription_suborigin - 1] = true;
            }

            update_tuple = true;
        }

        ALTER_SUBSCRIPTION_ENABLED => {
            parse_subscription_options(pstate, (*stmt).options, SUBOPT_ENABLED, &mut opts);
            Assert!(IsSet!(opts.specified_opts, SUBOPT_ENABLED));

            if (*sub).slotname.is_null() && opts.enabled {
                ereport!(ERROR, errmsg!("cannot enable subscription that does not have a slot name") /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);
            }

            values[Anum_pg_subscription_subenabled - 1] =
                BoolGetDatum(opts.enabled);
            replaces[Anum_pg_subscription_subenabled - 1] = true;

            if opts.enabled {
                ApplyLauncherWakeupAtCommit();
            }

            update_tuple = true;
        }

        ALTER_SUBSCRIPTION_CONNECTION => {
            /* Load the library providing us libpq calls. */
            load_file(b"libpqwalreceiver\0".as_ptr() as _, false);
            /* Check the connection info string. */
            walrcv_check_conninfo((*stmt).conninfo,
                                  (*sub).passwordrequired && !(*sub).ownersuperuser);

            values[Anum_pg_subscription_subconninfo - 1] =
                CStringGetTextDatum((*stmt).conninfo);
            replaces[Anum_pg_subscription_subconninfo - 1] = true;
            update_tuple = true;
        }

        ALTER_SUBSCRIPTION_SET_PUBLICATION => {
            supported_opts = SUBOPT_COPY_DATA | SUBOPT_REFRESH;
            parse_subscription_options(pstate, (*stmt).options, supported_opts, &mut opts);

            values[Anum_pg_subscription_subpublications - 1] =
                publicationListToArray((*stmt).publication);
            replaces[Anum_pg_subscription_subpublications - 1] = true;

            update_tuple = true;

            /* Refresh if user asked us to. */
            if opts.refresh {
                if !(*sub).enabled {
                    ereport!(ERROR, errmsg!("ALTER SUBSCRIPTION with refresh is not allowed for disabled subscriptions") /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);  /* C also: errhint("Use ALTER SUBSCRIPTION ... SET PUBLICATION ... WITH (refresh = false).") */
                }

                /*
                 * See ALTER_SUBSCRIPTION_REFRESH for details why this is
                 * not allowed.
                 */
                if (*sub).twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED && opts.copy_data {
                    ereport!(ERROR, errmsg!("ALTER SUBSCRIPTION with refresh and copy_data is not allowed when two_phase is enabled") /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);  /* C also: errhint("Use ALTER SUBSCRIPTION ... SET PUBLICATION with refresh = false, or with copy_data = false, or use DROP/CREATE SUBSCRIPTION.") */
                }

                PreventInTransactionBlock(isTopLevel, b"ALTER SUBSCRIPTION with refresh\0".as_ptr() as _);

                /* Make sure refresh sees the new list of publications. */
                (*sub).publications = (*stmt).publication;

                AlterSubscription_refresh(sub, opts.copy_data, (*stmt).publication);
            }
        }

        ALTER_SUBSCRIPTION_ADD_PUBLICATION | ALTER_SUBSCRIPTION_DROP_PUBLICATION => {
            let publist: *mut List;
            let isadd: bool = matches!((*stmt).kind, ALTER_SUBSCRIPTION_ADD_PUBLICATION);

            supported_opts = SUBOPT_REFRESH | SUBOPT_COPY_DATA;
            parse_subscription_options(pstate, (*stmt).options, supported_opts, &mut opts);

            publist = merge_publications((*sub).publications, (*stmt).publication, isadd, (*stmt).subname);
            values[Anum_pg_subscription_subpublications - 1] =
                publicationListToArray(publist);
            replaces[Anum_pg_subscription_subpublications - 1] = true;

            update_tuple = true;

            /* Refresh if user asked us to. */
            if opts.refresh {
                /* We only need to validate user specified publications. */
                let validate_publications: *mut List = if isadd { (*stmt).publication } else { NIL };

                if !(*sub).enabled {
                    ereport!(ERROR, errmsg!("ALTER SUBSCRIPTION with refresh is not allowed for disabled subscriptions") /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);  /* translator: %s is an SQL ALTER command */  /* C also: errhint("Use %s instead.", isadd ? "ALTER SUBSCRIPTION ... ADD PUBLICATION ... WITH (refresh = false)" : "ALTER SUBSCRIPTION ... DROP PUBLICATION ... WITH (refresh = false)") */
                }

                /*
                 * See ALTER_SUBSCRIPTION_REFRESH for details why this is
                 * not allowed.
                 */
                if (*sub).twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED && opts.copy_data {
                    ereport!(ERROR, errmsg!("ALTER SUBSCRIPTION with refresh and copy_data is not allowed when two_phase is enabled") /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);  /* translator: %s is an SQL ALTER command */  /* C also: errhint("Use %s with refresh = false, or with copy_data = false, or use DROP/CREATE SUBSCRIPTION.", isadd ? "ALTER SUBSCRIPTION ... ADD PUBLICATION" : "ALTER SUBSCRIPTION ... DROP PUBLICATION") */
                }

                PreventInTransactionBlock(isTopLevel, b"ALTER SUBSCRIPTION with refresh\0".as_ptr() as _);

                /* Refresh the new list of publications. */
                (*sub).publications = publist;

                AlterSubscription_refresh(sub, opts.copy_data, validate_publications);
            }
        }

        ALTER_SUBSCRIPTION_REFRESH => {
            if !(*sub).enabled {
                ereport!(ERROR, errmsg!("ALTER SUBSCRIPTION ... REFRESH is not allowed for disabled subscriptions") /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);
            }

            parse_subscription_options(pstate, (*stmt).options, SUBOPT_COPY_DATA, &mut opts);

            /*
             * The subscription option "two_phase" requires that
             * replication has passed the initial table synchronization
             * phase before the two_phase becomes properly enabled.
             *
             * But, having reached this two-phase commit "enabled" state
             * we must not allow any subsequent table initialization to
             * occur. So the ALTER SUBSCRIPTION ... REFRESH is disallowed
             * when the user had requested two_phase = on mode.
             *
             * The exception to this restriction is when copy_data =
             * false, because when copy_data is false the tablesync will
             * start already in READY state and will exit directly without
             * doing anything.
             *
             * For more details see comments atop worker.c.
             */
            if (*sub).twophasestate == LOGICALREP_TWOPHASE_STATE_ENABLED && opts.copy_data {
                ereport!(ERROR, errmsg!("ALTER SUBSCRIPTION ... REFRESH with copy_data is not allowed when two_phase is enabled") /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);  /* C also: errhint("Use ALTER SUBSCRIPTION ... REFRESH with copy_data = false, or use DROP/CREATE SUBSCRIPTION.") */
            }

            PreventInTransactionBlock(isTopLevel, b"ALTER SUBSCRIPTION ... REFRESH\0".as_ptr() as _);

            AlterSubscription_refresh(sub, opts.copy_data, NIL);
        }

        ALTER_SUBSCRIPTION_SKIP => {
            parse_subscription_options(pstate, (*stmt).options, SUBOPT_LSN, &mut opts);

            /* ALTER SUBSCRIPTION ... SKIP supports only LSN option */
            Assert!(IsSet!(opts.specified_opts, SUBOPT_LSN));

            /*
             * If the user sets subskiplsn, we do a sanity check to make
             * sure that the specified LSN is a probable value.
             */
            if !XLogRecPtrIsInvalid(opts.lsn) {
                let originid: RepOriginId;
                let mut originname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
                let remote_lsn: XLogRecPtr;

                ReplicationOriginNameForLogicalRep(subid, InvalidOid,
                                                   originname.as_mut_ptr(),
                                                   core::mem::size_of::<[c_char; NAMEDATALEN]>() as c_int);
                originid = replorigin_by_name(originname.as_mut_ptr(), false);
                remote_lsn = replorigin_get_progress(originid, false);

                /* Check the given LSN is at least a future LSN */
                if !XLogRecPtrIsInvalid(remote_lsn) && opts.lsn < remote_lsn {
                    ereport!(ERROR, errmsg!("skip WAL location (LSN {}/{:08X}) must be greater than origin LSN {}/{:08X}", (opts.lsn >> 32) as u32, opts.lsn as u32, (remote_lsn >> 32) as u32, remote_lsn as u32) /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */);
                }
            }

            values[Anum_pg_subscription_subskiplsn - 1] = LSNGetDatum(opts.lsn);
            replaces[Anum_pg_subscription_subskiplsn - 1] = true;

            update_tuple = true;
        }

        _ => {
            elog!(ERROR, "unrecognized ALTER SUBSCRIPTION kind {}", (*stmt).kind as c_int);
        }
    }

    /* Update the catalog if needed. */
    if update_tuple {
        tup = heap_modify_tuple(tup, RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr(),
                                replaces.as_mut_ptr());

        CatalogTupleUpdate(rel, &mut (*tup).t_self as *mut _ as *mut c_void, tup);

        heap_freetuple(tup);
    }

    /*
     * Try to acquire the connection necessary for altering the slot, if
     * needed.
     *
     * This has to be at the end because otherwise if there is an error while
     * doing the database operations we won't be able to rollback altered
     * slot.
     */
    if update_failover || update_two_phase {
        let must_use_password: bool;
        let mut err: *mut c_char = ptr::null_mut();
        let wrconn: *mut c_void;

        /* Load the library providing us libpq calls. */
        load_file(b"libpqwalreceiver\0".as_ptr() as _, false);

        /* Try to connect to the publisher. */
        must_use_password = (*sub).passwordrequired && !(*sub).ownersuperuser;
        wrconn = walrcv_connect((*sub).conninfo, true, true, must_use_password,
                                (*sub).name, &mut err);
        if wrconn.is_null() {
            ereport!(ERROR, errmsg!("subscription \"{}\" could not connect to the publisher: {}", std::ffi::CStr::from_ptr((*sub).name).to_string_lossy(), std::ffi::CStr::from_ptr(err).to_string_lossy()) /* C also: errcode(ERRCODE_CONNECTION_FAILURE) */);
        }

        /* PG_TRY */
        {
            walrcv_alter_slot(wrconn, (*sub).slotname,
                              if update_failover { &opts.failover as *const bool } else { ptr::null() },
                              if update_two_phase { &opts.twophase as *const bool } else { ptr::null() });
        }
        /* PG_FINALLY */
        walrcv_disconnect(wrconn);
        /* PG_END_TRY */
    }

    table_close(rel, RowExclusiveLock);

    ObjectAddressSet(&mut myself, SubscriptionRelationId, subid);

    InvokeObjectPostAlterHook(SubscriptionRelationId, subid, 0);

    /* Wake up related replication workers to handle this change quickly. */
    LogicalRepWorkersWakeupAtCommit(subid);

    return myself;
}

/*
 * Drop a subscription
 */
pub unsafe fn DropSubscription(stmt: *mut DropSubscriptionStmt, isTopLevel: bool) {
    let rel: Relation;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut tup: HeapTuple;
    let subid: Oid;
    let subowner: Oid;
    let mut datum: Datum;
    let mut isnull: bool = false;
    let subname: *mut c_char;
    let conninfo: *mut c_char;
    let slotname: *mut c_char;
    let subworkers: *mut List;
    let lc: *mut ListCell;
    let mut originname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let mut err: *mut c_char = ptr::null_mut();
    let wrconn: *mut c_void;
    let form: Form_pg_subscription;
    let rstates: *mut List;
    let must_use_password: bool;

    /*
     * The launcher may concurrently start a new worker for this subscription.
     * During initialization, the worker checks for subscription validity and
     * exits if the subscription has already been dropped. See
     * InitializeLogRepWorker.
     */
    rel = table_open(SubscriptionRelationId, RowExclusiveLock);

    tup = SearchSysCache2(SUBSCRIPTIONNAME, MyDatabaseId as Datum,
                          CStringGetDatum((*stmt).subname));

    if !HeapTupleIsValid(tup) {
        table_close(rel, NoLock);

        if !(*stmt).missing_ok {
            ereport!(ERROR, errmsg!("subscription \"{}\" does not exist", std::ffi::CStr::from_ptr((*stmt).subname).to_string_lossy()) /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */);
        } else {
            ereport!(NOTICE, errmsg!("subscription \"{}\" does not exist, skipping", std::ffi::CStr::from_ptr((*stmt).subname).to_string_lossy()));
        }

        return;
    }

    form = GETSTRUCT(tup) as Form_pg_subscription;
    subid = (*form).oid;
    subowner = (*form).subowner;
    must_use_password = !superuser_arg(subowner) && (*form).subpasswordrequired;

    /* must be owner */
    if !object_ownercheck(SubscriptionRelationId, subid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SUBSCRIPTION, (*stmt).subname);
    }

    /* DROP hook for the subscription being removed */
    InvokeObjectDropHook(SubscriptionRelationId, subid, 0);

    /*
     * Lock the subscription so nobody else can do anything with it (including
     * the replication workers).
     */
    LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock);

    /* Get subname */
    datum = SysCacheGetAttrNotNull(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subname as i16);
    subname = pstrdup(NameStr(DatumGetName(datum)));

    /* Get conninfo */
    datum = SysCacheGetAttrNotNull(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subconninfo as i16);
    conninfo = TextDatumGetCString(datum);

    /* Get slotname */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subslotname as i16, &mut isnull);
    if !isnull {
        slotname = pstrdup(NameStr(DatumGetName(datum)));
    } else {
        slotname = ptr::null_mut();
    }

    /*
     * Since dropping a replication slot is not transactional, the replication
     * slot stays dropped even if the transaction rolls back.  So we cannot
     * run DROP SUBSCRIPTION inside a transaction block if dropping the
     * replication slot.  Also, in this case, we report a message for dropping
     * the subscription to the cumulative stats system.
     *
     * XXX The command name should really be something like "DROP SUBSCRIPTION
     * of a subscription that is associated with a replication slot", but we
     * don't have the proper facilities for that.
     */
    if !slotname.is_null() {
        PreventInTransactionBlock(isTopLevel, b"DROP SUBSCRIPTION\0".as_ptr() as _);
    }

    ObjectAddressSet(&mut myself, SubscriptionRelationId, subid);
    EventTriggerSQLDropAddObject(&myself, true, true);

    /* Remove the tuple from catalog. */
    CatalogTupleDelete(rel, &mut (*tup).t_self as *mut _ as *mut c_void);

    ReleaseSysCache(tup);

    /*
     * Stop all the subscription workers immediately.
     *
     * This is necessary if we are dropping the replication slot, so that the
     * slot becomes accessible.
     *
     * It is also necessary if the subscription is disabled and was disabled
     * in the same transaction.  Then the workers haven't seen the disabling
     * yet and will still be running, leading to hangs later when we want to
     * drop the replication origin.  If the subscription was disabled before
     * this transaction, then there shouldn't be any workers left, so this
     * won't make a difference.
     *
     * New workers won't be started because we hold an exclusive lock on the
     * subscription till the end of the transaction.
     */
    subworkers = logicalrep_workers_find(subid, false, true);
    foreach!(lc, subworkers, {
        let w: *mut LogicalRepWorker = lfirst(current_cell!(lc)) as *mut LogicalRepWorker;

        logicalrep_worker_stop((*w).subid, (*w).relid);
    });
    list_free(subworkers);

    /*
     * Remove the no-longer-useful entry in the launcher's table of apply
     * worker start times.
     *
     * If this transaction rolls back, the launcher might restart a failed
     * apply worker before wal_retrieve_retry_interval milliseconds have
     * elapsed, but that's pretty harmless.
     */
    ApplyLauncherForgetWorkerStartTime(subid);

    /*
     * Cleanup of tablesync replication origins.
     *
     * Any READY-state relations would already have dealt with clean-ups.
     *
     * Note that the state can't change because we have already stopped both
     * the apply and tablesync workers and they can't restart because of
     * exclusive lock on the subscription.
     */
    rstates = GetSubscriptionRelations(subid, true);
    foreach!(lc, rstates, {
        let rstate: *mut SubscriptionRelState = lfirst(current_cell!(lc)) as *mut SubscriptionRelState;
        let relid: Oid = (*rstate).relid;

        /* Only cleanup resources of tablesync workers */
        if !OidIsValid(relid) {
            continue;
        }

        /*
         * Drop the tablesync's origin tracking if exists.
         *
         * It is possible that the origin is not yet created for tablesync
         * worker so passing missing_ok = true. This can happen for the states
         * before SUBREL_STATE_DATASYNC.
         */
        ReplicationOriginNameForLogicalRep(subid, relid, originname.as_mut_ptr(),
                                           core::mem::size_of::<[c_char; NAMEDATALEN]>() as c_int);
        replorigin_drop_by_name(originname.as_mut_ptr(), true, false);
    });

    /* Clean up dependencies */
    deleteSharedDependencyRecordsFor(SubscriptionRelationId, subid, 0);

    /* Remove any associated relation synchronization states. */
    RemoveSubscriptionRel(subid, InvalidOid);

    /* Remove the origin tracking if exists. */
    ReplicationOriginNameForLogicalRep(subid, InvalidOid, originname.as_mut_ptr(),
                                       core::mem::size_of::<[c_char; NAMEDATALEN]>() as c_int);
    replorigin_drop_by_name(originname.as_mut_ptr(), true, false);

    /*
     * Tell the cumulative stats system that the subscription is getting
     * dropped.
     */
    pgstat_drop_subscription(subid);

    /*
     * If there is no slot associated with the subscription, we can finish
     * here.
     */
    if slotname.is_null() && rstates == NIL {
        table_close(rel, NoLock);
        return;
    }

    /*
     * Try to acquire the connection necessary for dropping slots.
     *
     * Note: If the slotname is NONE/NULL then we allow the command to finish
     * and users need to manually cleanup the apply and tablesync worker slots
     * later.
     *
     * This has to be at the end because otherwise if there is an error while
     * doing the database operations we won't be able to rollback dropped
     * slot.
     */
    load_file(b"libpqwalreceiver\0".as_ptr() as _, false);

    wrconn = walrcv_connect(conninfo, true, true, must_use_password, subname, &mut err);
    if wrconn.is_null() {
        if slotname.is_null() {
            /* be tidy */
            list_free(rstates);
            table_close(rel, NoLock);
            return;
        } else {
            ReportSlotConnectionError(rstates, subid, slotname, err);
        }
    }

    /* PG_TRY */
    {
        foreach!(lc, rstates, {
            let rstate: *mut SubscriptionRelState = lfirst(current_cell!(lc)) as *mut SubscriptionRelState;
            let relid: Oid = (*rstate).relid;

            /* Only cleanup resources of tablesync workers */
            if !OidIsValid(relid) {
                continue;
            }

            /*
             * Drop the tablesync slots associated with removed tables.
             *
             * For SYNCDONE/READY states, the tablesync slot is known to have
             * already been dropped by the tablesync worker.
             *
             * For other states, there is no certainty, maybe the slot does
             * not exist yet. Also, if we fail after removing some of the
             * slots, next time, it will again try to drop already dropped
             * slots and fail. For these reasons, we allow missing_ok = true
             * for the drop.
             */
            if (*rstate).state != SUBREL_STATE_SYNCDONE {
                let mut syncslotname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

                ReplicationSlotNameForTablesync(subid, relid, syncslotname.as_mut_ptr(),
                                                core::mem::size_of::<[c_char; NAMEDATALEN]>());
                ReplicationSlotDropAtPubNode(wrconn, syncslotname.as_mut_ptr(), true);
            }
        });

        list_free(rstates);

        /*
         * If there is a slot associated with the subscription, then drop the
         * replication slot at the publisher.
         */
        if !slotname.is_null() {
            ReplicationSlotDropAtPubNode(wrconn, slotname, false);
        }
    }
    /* PG_FINALLY */
    walrcv_disconnect(wrconn);
    /* PG_END_TRY */

    table_close(rel, NoLock);
}

/*
 * Drop the replication slot at the publisher node using the replication
 * connection.
 *
 * missing_ok - if true then only issue a LOG message if the slot doesn't
 * exist.
 */
pub unsafe fn ReplicationSlotDropAtPubNode(wrconn: *mut c_void, slotname: *mut c_char, missing_ok: bool) {
    let mut cmd: StringInfoData = core::mem::zeroed();

    Assert!(!wrconn.is_null());

    load_file(b"libpqwalreceiver\0".as_ptr() as _, false);

    initStringInfo(&mut cmd);
    appendStringInfo(&mut cmd, b"DROP_REPLICATION_SLOT %s WAIT\0".as_ptr() as _,
                     quote_identifier(slotname));

    /* PG_TRY */
    {
        let res: *mut WalRcvExecResult;

        res = walrcv_exec(wrconn, cmd.data, 0, ptr::null());

        if (*res).status == WALRCV_OK_COMMAND {
            /* NOTICE. Success. */
            ereport!(NOTICE, errmsg!("dropped replication slot \"{}\" on publisher", std::ffi::CStr::from_ptr(slotname).to_string_lossy()));
        } else if (*res).status == WALRCV_ERROR &&
                  missing_ok &&
                  (*res).sqlstate == ERRCODE_UNDEFINED_OBJECT {
            /* LOG. Error, but missing_ok = true. */
            ereport!(LOG, errmsg!("could not drop replication slot \"{}\" on publisher: {}", std::ffi::CStr::from_ptr(slotname).to_string_lossy(), std::ffi::CStr::from_ptr((*res).err).to_string_lossy()));
        } else {
            /* ERROR. */
            ereport!(ERROR, errmsg!("could not drop replication slot \"{}\" on publisher: {}", std::ffi::CStr::from_ptr(slotname).to_string_lossy(), std::ffi::CStr::from_ptr((*res).err).to_string_lossy()) /* C also: errcode(ERRCODE_CONNECTION_FAILURE) */);
        }

        walrcv_clear_result(res);
    }
    /* PG_FINALLY */
    pfree(cmd.data as *mut c_void);
    /* PG_END_TRY */
}

/*
 * Internal workhorse for changing a subscription owner
 */
unsafe fn AlterSubscriptionOwner_internal(rel: Relation, tup: HeapTuple, newOwnerId: Oid) {
    let form: Form_pg_subscription;
    let aclresult: c_int;

    form = GETSTRUCT(tup) as Form_pg_subscription;

    if (*form).subowner == newOwnerId {
        return;
    }

    if !object_ownercheck(SubscriptionRelationId, (*form).oid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SUBSCRIPTION,
                       NameStr(&(*form).subname));
    }

    /*
     * Don't allow non-superuser modification of a subscription with
     * password_required=false.
     */
    if !(*form).subpasswordrequired && !superuser() {
        ereport!(ERROR, errmsg!("password_required=false is superuser-only") /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */);  /* C also: errhint("Subscriptions with the password_required option set to false may only be created or modified by the superuser.") */
    }

    /* Must be able to become new owner */
    check_can_set_role(GetUserId(), newOwnerId);

    /*
     * current owner must have CREATE on database
     *
     * This is consistent with how ALTER SCHEMA ... OWNER TO works, but some
     * other object types behave differently (e.g. you can't give a table to a
     * user who lacks CREATE privileges on a schema).
     */
    aclresult = object_aclcheck(DatabaseRelationId, MyDatabaseId, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_DATABASE, get_database_name(MyDatabaseId));
    }

    (*form).subowner = newOwnerId;
    CatalogTupleUpdate(rel, &mut (*tup).t_self as *mut _ as *mut c_void, tup);

    /* Update owner dependency reference */
    changeDependencyOnOwner(SubscriptionRelationId, (*form).oid, newOwnerId);

    InvokeObjectPostAlterHook(SubscriptionRelationId, (*form).oid, 0);

    /* Wake up related background processes to handle this change quickly. */
    ApplyLauncherWakeupAtCommit();
    LogicalRepWorkersWakeupAtCommit((*form).oid);
}

/*
 * Change subscription owner -- by name
 */
pub unsafe fn AlterSubscriptionOwner(name: *const c_char, newOwnerId: Oid) -> ObjectAddress {
    let subid: Oid;
    let tup: HeapTuple;
    let rel: Relation;
    let mut address: ObjectAddress = core::mem::zeroed();
    let form: Form_pg_subscription;

    rel = table_open(SubscriptionRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, MyDatabaseId as Datum, CStringGetDatum(name));

    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, errmsg!("subscription \"{}\" does not exist", std::ffi::CStr::from_ptr(name).to_string_lossy()) /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */);
    }

    form = GETSTRUCT(tup) as Form_pg_subscription;
    subid = (*form).oid;

    AlterSubscriptionOwner_internal(rel, tup, newOwnerId);

    ObjectAddressSet(&mut address, SubscriptionRelationId, subid);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);

    return address;
}

/*
 * Change subscription owner -- by OID
 */
pub unsafe fn AlterSubscriptionOwner_oid(subid: Oid, newOwnerId: Oid) {
    let tup: HeapTuple;
    let rel: Relation;

    rel = table_open(SubscriptionRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));

    if !HeapTupleIsValid(tup) {
        ereport!(ERROR, errmsg!("subscription with OID {} does not exist", subid) /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */);
    }

    AlterSubscriptionOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    table_close(rel, RowExclusiveLock);
}

/*
 * Check and log a warning if the publisher has subscribed to the same table,
 * its partition ancestors (if it's a partition), or its partition children (if
 * it's a partitioned table), from some other publishers. This check is
 * required only if "copy_data = true" and "origin = none" for CREATE
 * SUBSCRIPTION and ALTER SUBSCRIPTION ... REFRESH statements to notify the
 * user that data having origin might have been copied.
 *
 * This check need not be performed on the tables that are already added
 * because incremental sync for those tables will happen through WAL and the
 * origin of the data can be identified from the WAL records.
 *
 * subrel_local_oids contains the list of relation oids that are already
 * present on the subscriber.
 */
unsafe fn check_publications_origin(
    wrconn: *mut c_void,
    publications: *mut List,
    copydata: bool,
    origin: *mut c_char,
    subrel_local_oids: *mut Oid,
    subrel_count: c_int,
    subname: *const c_char,
) {
    let res: *mut WalRcvExecResult;
    let mut cmd: StringInfoData = core::mem::zeroed();
    let slot: *mut TupleTableSlot;
    let tableRow: [Oid; 1] = [TEXTOID];
    let mut publist: *mut List = NIL;
    let mut i: c_int;

    if !copydata || origin.is_null() ||
       (pg_strcasecmp(origin, LOGICALREP_ORIGIN_NONE.as_ptr() as _) != 0) {
        return;
    }

    initStringInfo(&mut cmd);
    appendStringInfoString(&mut cmd,
        b"SELECT DISTINCT P.pubname AS pubname\n\
          FROM pg_publication P,\n\
               LATERAL pg_get_publication_tables(P.pubname) GPT\n\
               JOIN pg_subscription_rel PS ON (GPT.relid = PS.srrelid OR\
               GPT.relid IN (SELECT relid FROM pg_partition_ancestors(PS.srrelid) UNION\
                             SELECT relid FROM pg_partition_tree(PS.srrelid))),\n\
               pg_class C JOIN pg_namespace N ON (N.oid = C.relnamespace)\n\
          WHERE C.oid = GPT.relid AND P.pubname IN (\0".as_ptr() as _);
    GetPublicationsStr(publications, &mut cmd, true);
    appendStringInfoString(&mut cmd, b")\n\0".as_ptr() as _);

    /*
     * In case of ALTER SUBSCRIPTION ... REFRESH, subrel_local_oids contains
     * the list of relation oids that are already present on the subscriber.
     * This check should be skipped for these tables.
     */
    i = 0;
    while i < subrel_count {
        let relid: Oid = *subrel_local_oids.add(i as usize);
        let schemaname: *mut c_char = get_namespace_name(get_rel_namespace(relid));
        let tablename: *mut c_char = get_rel_name(relid);

        appendStringInfo(&mut cmd, b"AND NOT (N.nspname = '%s' AND C.relname = '%s')\n\0".as_ptr() as _,
                         schemaname, tablename);
        i += 1;
    }

    res = walrcv_exec(wrconn, cmd.data, 1, tableRow.as_ptr());
    pfree(cmd.data as *mut c_void);

    if (*res).status != WALRCV_OK_TUPLES {
        ereport!(ERROR, errmsg!("could not receive list of replicated tables from the publisher: {}", std::ffi::CStr::from_ptr((*res).err).to_string_lossy()) /* C also: errcode(ERRCODE_CONNECTION_FAILURE) */);
    }

    /* Process tables. */
    slot = MakeSingleTupleTableSlot((*res).tupledesc, &TTSOpsMinimalTuple);
    while tuplestore_gettupleslot((*res).tuplestore, true, false, slot) {
        let pubname: *mut c_char;
        let mut isnull: bool = false;

        pubname = TextDatumGetCString(slot_getattr(slot, 1, &mut isnull));
        Assert!(!isnull);

        ExecClearTuple(slot);
        publist = list_append_unique(publist, makeString(pubname));
    }

    /*
     * Log a warning if the publisher has subscribed to the same table from
     * some other publisher. We cannot know the origin of data during the
     * initial sync. Data origins can be found only from the WAL by looking at
     * the origin id.
     *
     * XXX: For simplicity, we don't check whether the table has any data or
     * not. If the table doesn't have any data then we don't need to
     * distinguish between data having origin and data not having origin so we
     * can avoid logging a warning in that case.
     */
    if !publist.is_null() {
        let pubnames: *mut StringInfoData = makeStringInfo();

        /* Prepare the list of publication(s) for warning message. */
        GetPublicationsStr(publist, pubnames, false);
        ereport!(WARNING, errmsg!("subscription \"{}\" requested copy_data with origin = NONE but might copy data that had a different origin", std::ffi::CStr::from_ptr(subname).to_string_lossy()) /* C also: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */);  /* C also: errdetail_plural("The subscription being created subscribes to a publication (%s) that contains tables that are written to by other subscriptions.", "The subscription being created subscribes to publications (%s) that contain tables that are written to by other subscriptions.", list_length(publist), pubnames->data) */  /* C also: errhint("Verify that initial data copied from the publisher tables did not come from other origins.") */
    }

    ExecDropSingleTupleTableSlot(slot);

    walrcv_clear_result(res);
}

/*
 * Get the list of tables which belong to specified publications on the
 * publisher connection.
 *
 * Note that we don't support the case where the column list is different for
 * the same table in different publications to avoid sending unwanted column
 * information for some of the rows. This can happen when both the column
 * list and row filter are specified for different publications.
 */
unsafe fn fetch_table_list(wrconn: *mut c_void, publications: *mut List) -> *mut List {
    let res: *mut WalRcvExecResult;
    let mut cmd: StringInfoData = core::mem::zeroed();
    let slot: *mut TupleTableSlot;
    let mut tableRow: [Oid; 3] = [TEXTOID, TEXTOID, InvalidOid];
    let mut tablelist: *mut List = NIL;
    let server_version: c_int = walrcv_server_version(wrconn);
    let check_columnlist: bool = server_version >= 150000;
    let pub_names: *mut StringInfoData = makeStringInfo();

    initStringInfo(&mut cmd);

    /* Build the pub_names comma-separated string. */
    GetPublicationsStr(publications, pub_names, true);

    /* Get the list of tables from the publisher. */
    if server_version >= 160000 {
        tableRow[2] = INT2VECTOROID;

        /*
         * From version 16, we allowed passing multiple publications to the
         * function pg_get_publication_tables. This helped to filter out the
         * partition table whose ancestor is also published in this
         * publication array.
         *
         * Join pg_get_publication_tables with pg_publication to exclude
         * non-existing publications.
         *
         * Note that attrs are always stored in sorted order so we don't need
         * to worry if different publications have specified them in a
         * different order. See pub_collist_validate.
         */
        appendStringInfo(&mut cmd,
            b"SELECT DISTINCT n.nspname, c.relname, gpt.attrs\n\
                     FROM pg_class c\n\
                       JOIN pg_namespace n ON n.oid = c.relnamespace\n\
                       JOIN ( SELECT (pg_get_publication_tables(VARIADIC array_agg(pubname::text))).*\n\
                              FROM pg_publication\n\
                              WHERE pubname IN ( %s )) AS gpt\n\
                           ON gpt.relid = c.oid\n\0".as_ptr() as _,
            (*pub_names).data);
    } else {
        tableRow[2] = NAMEARRAYOID;
        appendStringInfoString(&mut cmd,
            b"SELECT DISTINCT t.schemaname, t.tablename \n\0".as_ptr() as _);

        /* Get column lists for each relation if the publisher supports it */
        if check_columnlist {
            appendStringInfoString(&mut cmd, b", t.attnames\n\0".as_ptr() as _);
        }

        appendStringInfo(&mut cmd,
            b"FROM pg_catalog.pg_publication_tables t\n WHERE t.pubname IN ( %s )\0".as_ptr() as _,
            (*pub_names).data);
    }

    destroyStringInfo(pub_names);

    res = walrcv_exec(wrconn, cmd.data, if check_columnlist { 3 } else { 2 }, tableRow.as_ptr());
    pfree(cmd.data as *mut c_void);

    if (*res).status != WALRCV_OK_TUPLES {
        ereport!(ERROR, errmsg!("could not receive list of replicated tables from the publisher: {}", std::ffi::CStr::from_ptr((*res).err).to_string_lossy()) /* C also: errcode(ERRCODE_CONNECTION_FAILURE) */);
    }

    /* Process tables. */
    slot = MakeSingleTupleTableSlot((*res).tupledesc, &TTSOpsMinimalTuple);
    while tuplestore_gettupleslot((*res).tuplestore, true, false, slot) {
        let nspname: *mut c_char;
        let relname: *mut c_char;
        let mut isnull: bool = false;
        let rv: *mut RangeVar;

        nspname = TextDatumGetCString(slot_getattr(slot, 1, &mut isnull));
        Assert!(!isnull);
        relname = TextDatumGetCString(slot_getattr(slot, 2, &mut isnull));
        Assert!(!isnull);

        rv = makeRangeVar(nspname, relname, -1);

        if check_columnlist && list_member(tablelist, rv as *const c_void) {
            ereport!(ERROR, errmsg!("cannot use different column lists for table \"{}.{}\" in different publications", std::ffi::CStr::from_ptr(nspname).to_string_lossy(), std::ffi::CStr::from_ptr(relname).to_string_lossy()) /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */);
        } else {
            tablelist = lappend(tablelist, rv as *mut c_void);
        }

        ExecClearTuple(slot);
    }
    ExecDropSingleTupleTableSlot(slot);

    walrcv_clear_result(res);

    return tablelist;
}

/*
 * This is to report the connection failure while dropping replication slots.
 * Here, we report the WARNING for all tablesync slots so that user can drop
 * them manually, if required.
 */
unsafe fn ReportSlotConnectionError(rstates: *mut List, subid: Oid, slotname: *mut c_char, err: *mut c_char) {
    let lc: *mut ListCell;

    foreach!(lc, rstates, {
        let rstate: *mut SubscriptionRelState = lfirst(current_cell!(lc)) as *mut SubscriptionRelState;
        let relid: Oid = (*rstate).relid;

        /* Only cleanup resources of tablesync workers */
        if !OidIsValid(relid) {
            continue;
        }

        /*
         * Caller needs to ensure that relstate doesn't change underneath us.
         * See DropSubscription where we get the relstates.
         */
        if (*rstate).state != SUBREL_STATE_SYNCDONE {
            let mut syncslotname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];

            ReplicationSlotNameForTablesync(subid, relid, syncslotname.as_mut_ptr(),
                                            core::mem::size_of::<[c_char; NAMEDATALEN]>());
            elog!(WARNING, "could not drop tablesync replication slot \"{}\"", std::ffi::CStr::from_ptr(syncslotname.as_ptr()).to_string_lossy());
        }
    });

    ereport!(ERROR, errmsg!("could not connect to publisher when attempting to drop replication slot \"{}\": {}", std::ffi::CStr::from_ptr(slotname).to_string_lossy(), std::ffi::CStr::from_ptr(err).to_string_lossy()) /* C also: errcode(ERRCODE_CONNECTION_FAILURE) */);  /* translator: %s is an SQL ALTER command */  /* C also: errhint("Use %s to disable the subscription, and then use %s to disassociate it from the slot.", "ALTER SUBSCRIPTION ... DISABLE", "ALTER SUBSCRIPTION ... SET (slot_name = NONE)") */
}

/*
 * Check for duplicates in the given list of publications and error out if
 * found one.  Add publications to datums as text datums, if datums is not
 * NULL.
 */
unsafe fn check_duplicates_in_publist(publist: *mut List, datums: *mut Datum) {
    let cell: *mut ListCell;
    let mut j: c_int = 0;

    foreach!(cell, publist, {
        let name: *mut c_char = strVal(lfirst(current_cell!(cell)));
        let pcell: *mut ListCell;

        foreach!(pcell, publist, {
            let pname: *mut c_char = strVal(lfirst(current_cell!(pcell)));

            if current_cell!(pcell) == current_cell!(cell) {
                break;
            }

            if libc_strcmp(name, pname) == 0 {
                ereport!(ERROR, errmsg!("publication name \"{}\" used more than once", std::ffi::CStr::from_ptr(pname).to_string_lossy()) /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */);
            }
        });

        if !datums.is_null() {
            *datums.add(j as usize) = CStringGetTextDatum(name);
            j += 1;
        }
    });
}

/*
 * Merge current subscription's publications and user-specified publications
 * from ADD/DROP PUBLICATIONS.
 *
 * If addpub is true, we will add the list of publications into oldpublist.
 * Otherwise, we will delete the list of publications from oldpublist.  The
 * returned list is a copy, oldpublist itself is not changed.
 *
 * subname is the subscription name, for error messages.
 */
unsafe fn merge_publications(
    oldpublist: *mut List,
    newpublist: *mut List,
    addpub: bool,
    subname: *const c_char,
) -> *mut List {
    let lc: *mut ListCell;
    let mut oldpublist: *mut List = list_copy(oldpublist);

    check_duplicates_in_publist(newpublist, ptr::null_mut());

    foreach!(lc, newpublist, {
        let name: *mut c_char = strVal(lfirst(current_cell!(lc)));
        let lc2: *mut ListCell;
        let mut found: bool = false;

        foreach!(lc2, oldpublist, {
            let pubname: *mut c_char = strVal(lfirst(current_cell!(lc2)));

            if libc_strcmp(name, pubname) == 0 {
                found = true;
                if addpub {
                    ereport!(ERROR, errmsg!("publication \"{}\" is already in subscription \"{}\"", std::ffi::CStr::from_ptr(name).to_string_lossy(), std::ffi::CStr::from_ptr(subname).to_string_lossy()) /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */);
                } else {
                    oldpublist = foreach_delete_current(oldpublist, current_cell!(lc2));
                }

                break;
            }
        });

        if addpub && !found {
            oldpublist = lappend(oldpublist, makeString(name));
        } else if !addpub && !found {
            ereport!(ERROR, errmsg!("publication \"{}\" is not in subscription \"{}\"", std::ffi::CStr::from_ptr(name).to_string_lossy(), std::ffi::CStr::from_ptr(subname).to_string_lossy()) /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */);
        }
    });

    /*
     * XXX Probably no strong reason for this, but for now it's to make ALTER
     * SUBSCRIPTION ... DROP PUBLICATION consistent with SET PUBLICATION.
     */
    if oldpublist.is_null() {
        ereport!(ERROR, errmsg!("cannot drop all the publications from a subscription") /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */);
    }

    return oldpublist;
}

/*
 * Extract the streaming mode value from a DefElem.  This is like
 * defGetBoolean() but also accepts the special value of "parallel".
 */
pub unsafe fn defGetStreamingMode(def: *mut DefElem) -> c_char {
    /*
     * If no parameter value given, assume "true" is meant.
     */
    if (*def).arg.is_null() {
        return LOGICALREP_STREAM_ON;
    }

    /*
     * Allow 0, 1, "false", "true", "off", "on" or "parallel".
     */
    match nodeTag((*def).arg) {
        NodeTag::T_Integer => {
            match intVal((*def).arg) {
                0 => return LOGICALREP_STREAM_OFF,
                1 => return LOGICALREP_STREAM_ON,
                _ => {
                    /* otherwise, error out below */
                }
            }
        }
        _ => {
            let sval: *mut c_char = defGetString(def);

            /*
             * The set of strings accepted here should match up with the
             * grammar's opt_boolean_or_string production.
             */
            if pg_strcasecmp(sval, b"false\0".as_ptr() as _) == 0 ||
               pg_strcasecmp(sval, b"off\0".as_ptr() as _) == 0 {
                return LOGICALREP_STREAM_OFF;
            }
            if pg_strcasecmp(sval, b"true\0".as_ptr() as _) == 0 ||
               pg_strcasecmp(sval, b"on\0".as_ptr() as _) == 0 {
                return LOGICALREP_STREAM_ON;
            }
            if pg_strcasecmp(sval, b"parallel\0".as_ptr() as _) == 0 {
                return LOGICALREP_STREAM_PARALLEL;
            }
        }
    }

    ereport!(ERROR, errmsg!("{} requires a Boolean value or \"parallel\"", std::ffi::CStr::from_ptr((*def).defname).to_string_lossy()) /* C also: errcode(ERRCODE_SYNTAX_ERROR) */);
    return LOGICALREP_STREAM_OFF; /* keep compiler quiet */
}

/* ------------------------------------------------------------------------
 * Additional dependency stubs referenced above but defined in other .c files.
 * ------------------------------------------------------------------------ */

/* InvalidOid  TODO(pg-port) */
const InvalidOid: Oid = 0;

/* streaming mode values  TODO(pg-port) */
const LOGICALREP_STREAM_OFF:      c_char = b'f' as c_char;
const LOGICALREP_STREAM_ON:       c_char = b't' as c_char;

extern "C" {
    /* libc string compare; named to avoid clashing with PG's strcmp wrappers */
    fn libc_strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn errmsg_internal(fmt: *const c_char, ...) -> c_int;
}

/* LogicalRepWorker  TODO(pg-port) */
#[repr(C)]
pub struct LogicalRepWorker {
    pub subid: Oid,
    pub relid: Oid,
}

/* SubRemoveRels - local struct from AlterSubscription_refresh  TODO(pg-port) */
#[repr(C)]
pub struct SubRemoveRels {
    pub relid: Oid,
    pub state: c_char,
}

/*-------------------------------------------------------------------------
 *
 * tablespace.c
 *	  Commands to manipulate table spaces
 *
 * Tablespaces in PostgreSQL are designed to allow users to determine
 * where the data file(s) for a given database object reside on the file
 * system.
 *
 * A tablespace represents a directory on the file system. At tablespace
 * creation time, the directory must be empty. To simplify things and
 * remove the possibility of having file name conflicts, we isolate
 * files within a tablespace into database-specific subdirectories.
 *
 * To support file access via the information given in RelFileLocator, we
 * maintain a symbolic-link map in $PGDATA/pg_tblspc. The symlinks are
 * named by tablespace OIDs and point to the actual tablespace directories.
 * There is also a per-cluster version directory in each tablespace.
 * Thus the full path to an arbitrary file is
 *			$PGDATA/pg_tblspc/spcoid/PG_MAJORVER_CATVER/dboid/relfilenumber
 * e.g.
 *			$PGDATA/pg_tblspc/20981/PG_9.0_201002161/719849/83292814
 *
 * There are two tablespaces created at initdb time: pg_global (for shared
 * tables) and pg_default (for everything else).  For backwards compatibility
 * and to remain functional on platforms without symlinks, these tablespaces
 * are accessed specially: they are respectively
 *			$PGDATA/global/relfilenumber
 *			$PGDATA/base/dboid/relfilenumber
 *
 * To allow CREATE DATABASE to give a new database a default tablespace
 * that's different from the template database's default, we make the
 * provision that a zero in pg_class.reltablespace means the database's
 * default tablespace.  Without this, CREATE DATABASE would have to go in
 * and munge the system catalogs of the new database.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/tablespace.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_imports)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};
use std::ffi::CStr;

use crate::access::htup_details::{HeapTupleData, HeapTuple, HeapTupleIsValid};
use crate::access::transam::xlogreader::XLogReaderState;
use crate::storage::itemptr::ItemPointerData;
use crate::catalog::objectaccess::ObjectAddress;
use crate::nodes::pg_list::{List, ListCell, lfirst, list_length, list_free};
use crate::nodes::parsenodes::{
    AlterTableSpaceOptionsStmt, CreateTableSpaceStmt, DropTableSpaceStmt,
};
use crate::utils::misc::guc::GucSource;
use crate::access::rmgrdesc::tblspcdesc::{
    xl_tblspc_create_rec, xl_tblspc_drop_rec, XLOG_TBLSPC_CREATE, XLOG_TBLSPC_DROP,
};
use crate::{foreach, current_cell};

/* -------------------------------------------------------------------------
 * Local type stubs for unported dependencies
 * ------------------------------------------------------------------------- */

// Relation pointer  TODO(pg-port)
#[repr(C)] pub struct RelationData { _opaque: [u8; 0] }
type Relation = *mut RelationData;

// TableScanDesc  TODO(pg-port)
#[repr(C)] pub struct TableScanDescData { _opaque: [u8; 0] }
type TableScanDesc = *mut TableScanDescData;

// ScanKeyData  TODO(pg-port)
#[repr(C)] #[derive(Clone, Copy)] pub struct ScanKeyDataStruct { _opaque: [u8; 64] }
type ScanKeyData = ScanKeyDataStruct;

// TupleDesc  TODO(pg-port)
#[repr(C)] pub struct TupleDescData { _opaque: [u8; 0] }
type TupleDesc = *mut TupleDescData;

// Form_pg_tablespace  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_tablespace {
    pub oid: Oid,
    pub spcname: NameData,
    pub spcowner: Oid,
}
type Form_pg_tablespace = *mut FormData_pg_tablespace;

// NameData  TODO(pg-port)
#[repr(C)] #[derive(Clone, Copy)] pub struct NameData { pub data: [c_char; 64] }

// DIR / dirent  TODO(pg-port)
#[repr(C)] pub struct DIR { _opaque: [u8; 0] }
#[repr(C)] pub struct dirent { pub d_name: [c_char; 256] }

// stat / statbuf  TODO(pg-port)
#[repr(C)] #[derive(Clone, Copy)] pub struct stat_struct { _opaque: [u8; 144] }

// AclResult  TODO(pg-port)
#[repr(C)] #[derive(PartialEq, Clone, Copy)] pub enum AclResult { ACLCHECK_OK = 0, ACLCHECK_NOT_OWNER, ACLCHECK_NO_PRIV }
use AclResult::*;

// ItemPointerData comes from storage::itemptr (matches HeapTupleData.t_self).

// LOCKMODE  TODO(pg-port)
type LOCKMODE = c_int;
const NoLock: LOCKMODE = 0;
const AccessShareLock: LOCKMODE = 1;
const RowExclusiveLock: LOCKMODE = 3;

// LWLockMode / LWLock  TODO(pg-port)
type LWLockMode = c_int;
const LW_EXCLUSIVE: LWLockMode = 0;

// RelFileNumber  TODO(pg-port)
type RelFileNumber = Oid;

/* -------------------------------------------------------------------------
 * Compile-time constant stubs  TODO(pg-port)
 * ------------------------------------------------------------------------- */

extern "C" {
    static TableSpaceRelationId: Oid;
    static TablespaceOidIndexId: Oid;
    static GLOBALTABLESPACE_OID: Oid;
    static InvalidOid: Oid;
    static MyDatabaseId: Oid;
    static MyDatabaseTableSpace: Oid;
    static IsBinaryUpgrade: bool;
    static allowSystemTableMods: bool;
    static InRecovery: bool;
    static DataDir: *mut c_char;
    static pg_dir_create_mode: c_int;
    static mut TopTransactionContext: MemoryContext;
}

/* GUC variables */
#[no_mangle] pub static mut default_tablespace: *mut c_char = null_mut();
#[no_mangle] pub static mut temp_tablespaces: *mut c_char = null_mut();
#[no_mangle] pub static mut allow_in_place_tablespaces: bool = false;

#[no_mangle] pub static mut binary_upgrade_next_pg_tablespace_oid: Oid = 0;

// Severity levels not in elog prelude subset have aliases there; LOG/WARNING/NOTICE/ERROR/PANIC come from prelude.

// Natts / Anum for pg_tablespace  TODO(pg-port)
const Natts_pg_tablespace: usize = 5;
const Anum_pg_tablespace_oid: usize = 1;
const Anum_pg_tablespace_spcname: usize = 2;
const Anum_pg_tablespace_spcowner: usize = 3;
const Anum_pg_tablespace_spcacl: usize = 4;
const Anum_pg_tablespace_spcoptions: usize = 5;

// Path-length / name-length constants  TODO(pg-port)
const MAXPGPATH: usize = 1024;
const OIDCHARS: usize = 10;
const FORKNAMECHARS: usize = 4;
const TABLESPACE_VERSION_DIRECTORY: &CStr = c"PG_18_202504071";
const PG_TBLSPC_DIR: &CStr = c"pg_tblspc";

// Errcodes  TODO(pg-port)
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_INVALID_NAME: c_int = 0;
const ERRCODE_INVALID_OBJECT_DEFINITION: c_int = 0;
const ERRCODE_RESERVED_NAME: c_int = 0;
const ERRCODE_DUPLICATE_OBJECT: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST: c_int = 0;
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0;
const ERRCODE_WRONG_OBJECT_TYPE: c_int = 0;
const ERRCODE_UNDEFINED_FILE: c_int = 0;
const ERRCODE_OBJECT_IN_USE: c_int = 0;
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;

// OBJECT_TABLESPACE  TODO(pg-port)
const OBJECT_TABLESPACE: c_int = 0;

// ACL_CREATE  TODO(pg-port)
const ACL_CREATE: c_int = 0x0004;

// BTEqualStrategyNumber  TODO(pg-port)
const BTEqualStrategyNumber: c_int = 3;

// F_NAMEEQ / F_OIDEQ  TODO(pg-port)
const F_NAMEEQ: Oid = 0;
const F_OIDEQ: Oid = 0;

// ForwardScanDirection  TODO(pg-port)
const ForwardScanDirection: c_int = 1;

// relpersistence  TODO(pg-port)
const RELPERSISTENCE_TEMP: c_char = b't' as c_char;

// XLog rmgr id  TODO(pg-port)
const RM_TBLSPC_ID: u8 = 9;
const XLR_INFO_MASK: u8 = 0x0f;

// Checkpoint flags  TODO(pg-port)
const CHECKPOINT_IMMEDIATE: c_int = 0x0001;
const CHECKPOINT_FORCE: c_int = 0x0002;
const CHECKPOINT_WAIT: c_int = 0x0004;

// PROCSIGNAL_BARRIER_SMGRRELEASE  TODO(pg-port)
const PROCSIGNAL_BARRIER_SMGRRELEASE: c_int = 1;

// GUC source value (re-export of guc enum used as scalar via casts below).

/* -------------------------------------------------------------------------
 * External function stubs  TODO(pg-port)
 * ------------------------------------------------------------------------- */

extern "C" {
    fn superuser() -> bool;
    fn get_rolespec_oid(rolespec: *mut c_void, missing_ok: bool) -> Oid;
    fn GetUserId() -> Oid;
    fn pstrdup(str_: *const c_char) -> *mut c_char;
    fn canonicalize_path(path: *mut c_char);
    fn is_absolute_path(path: *const c_char) -> bool;
    fn path_is_prefix_of_path(path1: *const c_char, path2: *const c_char) -> bool;
    fn IsReservedName(name: *const c_char) -> bool;
    fn table_open(relationId: Oid, lockmode: LOCKMODE) -> Relation;
    fn table_close(relation: Relation, lockmode: LOCKMODE);
    fn GetNewOidWithIndex(relation: Relation, indexId: Oid, oidcolumn: i16) -> Oid;
    fn namein(fcinfo: *mut c_void) -> Datum;
    fn DirectFunctionCall1Coll(func: *const c_void, collation: Oid, arg1: Datum) -> Datum;
    fn transformRelOptions(oldOptions: Datum, defList: *mut List, namspace: *const c_char,
                           validnsps: *mut *mut c_char, acceptOidsOff: bool, isReset: bool) -> Datum;
    fn tablespace_reloptions(reloptions: Datum, validate: bool) -> Datum;
    fn heap_form_tuple(tupleDescriptor: TupleDesc, values: *mut Datum, isnull: *mut bool) -> HeapTuple;
    fn heap_freetuple(htup: HeapTuple);
    fn heap_copytuple(tuple: HeapTuple) -> HeapTuple;
    fn heap_modify_tuple(tuple: HeapTuple, tupleDesc: TupleDesc, replValues: *mut Datum,
                         replIsnull: *mut bool, doReplace: *mut bool) -> HeapTuple;
    fn heap_getattr(tup: HeapTuple, attnum: c_int, tupleDesc: TupleDesc, isnull: *mut bool) -> Datum;
    fn CatalogTupleInsert(heapRel: Relation, tup: HeapTuple);
    fn CatalogTupleUpdate(heapRel: Relation, otid: *mut ItemPointerData, tup: HeapTuple);
    fn CatalogTupleDelete(heapRel: Relation, otid: *mut ItemPointerData);
    fn recordDependencyOnOwner(classId: Oid, objectId: Oid, owner: Oid);
    fn deleteSharedDependencyRecordsFor(classId: Oid, objectId: Oid, objectSubId: i32);
    fn checkSharedDependencies(classId: Oid, objectId: Oid,
                               detail_msg: *mut *mut c_char, detail_log_msg: *mut *mut c_char) -> bool;
    fn DeleteSharedComments(oid: Oid, classoid: Oid);
    fn DeleteSharedSecurityLabel(object: Oid, classoid: Oid);
    fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool;
    fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: c_int) -> AclResult;
    fn aclcheck_error(aclerr: AclResult, objtype: c_int, objectname: *const c_char);
    fn IsPinnedObject(classId: Oid, objectId: Oid) -> bool;
    fn table_beginscan_catalog(relation: Relation, nkeys: c_int, key: *mut ScanKeyData) -> TableScanDesc;
    fn heap_getnext(scan: TableScanDesc, direction: c_int) -> HeapTuple;
    fn table_endscan(scan: TableScanDesc);
    fn ScanKeyInit(entry: *mut ScanKeyData, attributeNumber: i16, strategy: c_int,
                   procedure: Oid, argument: Datum);
    fn GETSTRUCT(tuple: HeapTuple) -> *mut c_void;
    fn namestrcpy(name: *mut NameData, str_: *const c_char) -> c_int;
    fn RelationGetDescr(relation: Relation) -> TupleDesc;
    fn LWLockAcquire(lock: *mut c_void, mode: LWLockMode) -> bool;
    fn LWLockRelease(lock: *mut c_void);
    // TablespaceCreateLock is a named LWLock; expose a getter  TODO(pg-port)
    fn TablespaceCreateLock() -> *mut c_void;
    fn GetDatabasePath(dbOid: Oid, spcOid: Oid) -> *mut c_char;
    fn MakePGDirectory(directoryName: *const c_char) -> c_int;
    fn pg_mkdir_p(path: *mut c_char, omode: c_int) -> c_int;
    fn get_parent_directory(path: *mut c_char);
    fn AllocateDir(dirname: *const c_char) -> *mut DIR;
    fn ReadDir(dir: *mut DIR, dirname: *const c_char) -> *mut dirent;
    fn FreeDir(dir: *mut DIR) -> c_int;
    fn RequestCheckpoint(flags: c_int);
    fn WaitForProcSignalBarrier(generation: u64);
    fn EmitProcSignalBarrier(r#type: c_int) -> u64;
    fn ResolveRecoveryConflictWithTablespace(tsid: Oid);
    fn ForceSyncCommit();
    fn InvokeObjectPostCreateHookArg(classId: Oid, objectId: Oid, subId: c_int, is_internal: bool);
    fn InvokeObjectDropHookArg(classId: Oid, objectId: Oid, subId: c_int, dropflags: c_int);
    fn InvokeObjectPostAlterHookArg(classId: Oid, objectId: Oid, subId: c_int,
                                    auxiliaryId: Oid, is_internal: bool);
    fn XLogBeginInsert();
    fn XLogRegisterData(data: *const c_void, len: usize);
    fn XLogInsert(rmid: u8, info: u8) -> u64;
    fn XLogRecGetInfo(record: *mut XLogReaderState) -> u8;
    fn XLogRecGetData(record: *mut XLogReaderState) -> *mut c_void;
    fn XLogRecHasAnyBlockRefs(record: *mut XLogReaderState) -> bool;
    fn SplitIdentifierString(rawstring: *mut c_char, separator: c_char,
                             namelist: *mut *mut List) -> bool;
    fn palloc(size: usize) -> *mut c_void;
    fn pfree(pointer: *mut c_void);
    fn guc_malloc(elevel: c_int, size: usize) -> *mut c_void;
    fn GUC_check_errdetail_impl(fmt: *const c_char, ...);
    fn IsTransactionState() -> bool;
    fn SetTempTablespaces(tableSpaces: *mut Oid, numSpaces: c_int);
    fn TempTablespacesAreSet() -> bool;
    fn GetNextTempTableSpace() -> Oid;
    // libc-style filesystem calls
    fn stat(path: *const c_char, buf: *mut stat_struct) -> c_int;
    fn lstat(path: *const c_char, buf: *mut stat_struct) -> c_int;
    fn chmod(path: *const c_char, mode: c_int) -> c_int;
    fn symlink(target: *const c_char, linkpath: *const c_char) -> c_int;
    fn unlink(path: *const c_char) -> c_int;
    fn rmdir(path: *const c_char) -> c_int;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// errno helpers / S_ISxxx macros and a few inline helpers  TODO(pg-port)
extern "C" { fn __error() -> *mut c_int; }
#[cfg(not(target_os = "macos"))]
extern "C" { fn __errno_location() -> *mut c_int; }
#[inline] unsafe fn errno() -> c_int {
    #[cfg(target_os = "macos")] { *__error() }
    #[cfg(not(target_os = "macos"))] { *__errno_location() }
}

const ENOENT: c_int = 2;
const EEXIST: c_int = 17;

// stat helpers: extract st_mode and test the file type.  TODO(pg-port)
extern "C" {
    fn pgport_stat_mode(buf: *const stat_struct) -> u32;
}
const S_IFMT: u32 = 0o170000;
const S_IFDIR: u32 = 0o040000;
const S_IFLNK: u32 = 0o120000;
#[inline] unsafe fn S_ISDIR(buf: *const stat_struct) -> bool {
    (pgport_stat_mode(buf) & S_IFMT) == S_IFDIR
}
#[inline] unsafe fn S_ISLNK(buf: *const stat_struct) -> bool {
    (pgport_stat_mode(buf) & S_IFMT) == S_IFLNK
}

// Datum helpers  TODO(pg-port)
#[inline] fn ObjectIdGetDatum(oid: Oid) -> Datum { oid as Datum }
#[inline] fn CStringGetDatum(s: *const c_char) -> Datum { s as Datum }
#[inline] fn OidIsValid(oid: Oid) -> bool { oid != unsafe { InvalidOid } }

// psprintf shims  TODO(pg-port)
extern "C" {
    fn psprintf_s_u(fmt: *const c_char, s: *const c_char, u: Oid) -> *mut c_char;
    fn psprintf_s_s(fmt: *const c_char, a: *const c_char, b: *const c_char) -> *mut c_char;
    fn psprintf_u(fmt: *const c_char, u: Oid) -> *mut c_char;
    fn psprintf_s_u_s(fmt: *const c_char, a: *const c_char, b: Oid, c: *const c_char) -> *mut c_char;
}

// InvokeObjectPostCreateHook / InvokeObjectDropHook / InvokeObjectPostAlterHook wrappers
#[inline] unsafe fn InvokeObjectPostCreateHook(classId: Oid, objectId: Oid, subId: c_int) {
    InvokeObjectPostCreateHookArg(classId, objectId, subId, false);
}
#[inline] unsafe fn InvokeObjectDropHook(classId: Oid, objectId: Oid, subId: c_int) {
    InvokeObjectDropHookArg(classId, objectId, subId, 0);
}
#[inline] unsafe fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int) {
    InvokeObjectPostAlterHookArg(classId, objectId, subId, unsafe { InvalidOid }, false);
}

// DirectFunctionCall1(namein, arg) wrapper.
#[inline] unsafe fn DirectFunctionCall1_namein(arg1: Datum) -> Datum {
    DirectFunctionCall1Coll(namein as *const c_void, unsafe { InvalidOid }, arg1)
}

// ObjectAddressSet  TODO(pg-port)
#[inline] fn ObjectAddressSet(address: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    address.classId = class_id;
    address.objectId = object_id;
    address.objectSubId = 0;
}

// NameStr(name) -> pointer to the name's character data  TODO(pg-port)
macro_rules! NameStr {
    ($name:expr) => {
        ($name).data.as_ptr() as *mut c_char
    };
}
use NameStr;

/* forward decls of file-local static functions */
// static void create_tablespace_directories(const char *location, const Oid tablespaceoid);
// static bool destroy_tablespace_directories(Oid tablespaceoid, bool redo);

/*
 * Each database using a table space is isolated into its own name space
 * by a subdirectory named for the database OID.  On first creation of an
 * object in the tablespace, create the subdirectory.  If the subdirectory
 * already exists, fall through quietly.
 *
 * isRedo indicates that we are creating an object during WAL replay.
 * In this case we will cope with the possibility of the tablespace
 * directory not being there either --- this could happen if we are
 * replaying an operation on a table in a subsequently-dropped tablespace.
 * We handle this by making a directory in the place where the tablespace
 * symlink would normally be.  This isn't an exact replay of course, but
 * it's the best we can do given the available information.
 *
 * If tablespaces are not supported, we still need it in case we have to
 * re-create a database subdirectory (of $PGDATA/base) during WAL replay.
 */
pub unsafe fn TablespaceCreateDbspace(spcOid: Oid, dbOid: Oid, isRedo: bool) {
    let mut st: stat_struct = core::mem::zeroed();
    let dir: *mut c_char;

    /*
     * The global tablespace doesn't have per-database subdirectories, so
     * nothing to do for it.
     */
    if spcOid == GLOBALTABLESPACE_OID {
        return;
    }

    Assert!(OidIsValid(spcOid));
    Assert!(OidIsValid(dbOid));

    dir = GetDatabasePath(dbOid, spcOid);

    if stat(dir, &mut st) < 0 {
        /* Directory does not exist? */
        if errno() == ENOENT {
            /*
             * Acquire TablespaceCreateLock to ensure that no DROP TABLESPACE
             * or TablespaceCreateDbspace is running concurrently.
             */
            LWLockAcquire(TablespaceCreateLock(), LW_EXCLUSIVE);

            /*
             * Recheck to see if someone created the directory while we were
             * waiting for lock.
             */
            if stat(dir, &mut st) == 0 && S_ISDIR(&st) {
                /* Directory was created */
            } else {
                /* Directory creation failed? */
                if MakePGDirectory(dir) < 0 {
                    /* Failure other than not exists or not in WAL replay? */
                    if errno() != ENOENT || !isRedo {
                        ereport!(ERROR,
                                 // C: errcode_for_file_access()
                                 errmsg!("could not create directory \"{}\": %m",
                                         CStr::from_ptr(dir).to_string_lossy()));
                    }

                    /*
                     * During WAL replay, it's conceivable that several levels
                     * of directories are missing if tablespaces are dropped
                     * further ahead of the WAL stream than we're currently
                     * replaying.  An easy way forward is to create them as
                     * plain directories and hope they are removed by further
                     * WAL replay if necessary.  If this also fails, there is
                     * trouble we cannot get out of, so just report that and
                     * bail out.
                     */
                    if pg_mkdir_p(dir, pg_dir_create_mode) < 0 {
                        ereport!(ERROR,
                                 // C: errcode_for_file_access()
                                 errmsg!("could not create directory \"{}\": %m",
                                         CStr::from_ptr(dir).to_string_lossy()));
                    }
                }
            }

            LWLockRelease(TablespaceCreateLock());
        } else {
            ereport!(ERROR,
                     // C: errcode_for_file_access()
                     errmsg!("could not stat directory \"{}\": %m",
                             CStr::from_ptr(dir).to_string_lossy()));
        }
    } else {
        /* Is it not a directory? */
        if !S_ISDIR(&st) {
            ereport!(ERROR,
                     // C: errcode(ERRCODE_WRONG_OBJECT_TYPE)
                     errmsg!("\"{}\" exists but is not a directory",
                             CStr::from_ptr(dir).to_string_lossy()));
        }
    }

    pfree(dir as *mut c_void);
}

/*
 * Create a table space
 *
 * Only superusers can create a tablespace. This seems a reasonable restriction
 * since we're determining the system layout and, anyway, we probably have
 * root if we're doing this kind of activity
 */
pub unsafe fn CreateTableSpace(stmt: *mut CreateTableSpaceStmt) -> Oid {
    let rel: Relation;
    let mut values: [Datum; Natts_pg_tablespace] = [0 as Datum; Natts_pg_tablespace];
    let mut nulls: [bool; Natts_pg_tablespace] = [false; Natts_pg_tablespace];
    let tuple: HeapTuple;
    let mut tablespaceoid: Oid;
    let location: *mut c_char;
    let ownerId: Oid;
    let newOptions: Datum;
    let in_place: bool;

    /* Must be superuser */
    if !superuser() {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                 // C also: errhint("Must be superuser to create a tablespace.")
                 errmsg!("permission denied to create tablespace \"{}\"",
                         CStr::from_ptr((*stmt).tablespacename).to_string_lossy()));
    }

    /* However, the eventual owner of the tablespace need not be */
    if !(*stmt).owner.is_null() {
        ownerId = get_rolespec_oid((*stmt).owner as *mut c_void, false);
    } else {
        ownerId = GetUserId();
    }

    /* Unix-ify the offered path, and strip any trailing slashes */
    location = pstrdup((*stmt).location);
    canonicalize_path(location);

    /* disallow quotes, else CREATE DATABASE would be at risk */
    if !strchr(location, b'\'' as c_int).is_null() {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_INVALID_NAME)
                 errmsg!("tablespace location cannot contain single quotes"));
    }

    in_place = allow_in_place_tablespaces && strlen(location) == 0;

    /*
     * Allowing relative paths seems risky
     *
     * This also helps us ensure that location is not empty or whitespace,
     * unless specifying a developer-only in-place tablespace.
     */
    if !in_place && !is_absolute_path(location) {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                 errmsg!("tablespace location must be an absolute path"));
    }

    /*
     * Check that location isn't too long. Remember that we're going to append
     * 'PG_XXX/<dboid>/<relid>_<fork>.<nnn>'.  FYI, we never actually
     * reference the whole path here, but MakePGDirectory() uses the first two
     * parts.
     */
    if strlen(location) + 1 + strlen(TABLESPACE_VERSION_DIRECTORY.as_ptr()) + 1 +
        OIDCHARS + 1 + OIDCHARS + 1 + FORKNAMECHARS + 1 + OIDCHARS > MAXPGPATH {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                 errmsg!("tablespace location \"{}\" is too long",
                         CStr::from_ptr(location).to_string_lossy()));
    }

    /* Warn if the tablespace is in the data directory. */
    if path_is_prefix_of_path(DataDir, location) {
        ereport!(WARNING,
                 // C: errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                 errmsg!("tablespace location should not be inside the data directory"));
    }

    /*
     * Disallow creation of tablespaces named "pg_xxx"; we reserve this
     * namespace for system purposes.
     */
    if !allowSystemTableMods && IsReservedName((*stmt).tablespacename) {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_RESERVED_NAME)
                 // C also: errdetail("The prefix \"pg_\" is reserved for system tablespaces.")
                 errmsg!("unacceptable tablespace name \"{}\"",
                         CStr::from_ptr((*stmt).tablespacename).to_string_lossy()));
    }

    /*
     * If built with appropriate switch, whine when regression-testing
     * conventions for tablespace names are violated.
     */
    // #ifdef ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS
    //     if (strncmp(stmt->tablespacename, "regress_", 8) != 0)
    //         elog(WARNING, "tablespaces created by regression test cases should have names starting with \"regress_\"");
    // #endif

    /*
     * Check that there is no other tablespace by this name.  (The unique
     * index would catch this anyway, but might as well give a friendlier
     * message.)
     */
    if OidIsValid(get_tablespace_oid((*stmt).tablespacename, true)) {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_DUPLICATE_OBJECT)
                 errmsg!("tablespace \"{}\" already exists",
                         CStr::from_ptr((*stmt).tablespacename).to_string_lossy()));
    }

    /*
     * Insert tuple into pg_tablespace.  The purpose of doing this first is to
     * lock the proposed tablename against other would-be creators. The
     * insertion will roll back if we find problems below.
     */
    rel = table_open(TableSpaceRelationId, RowExclusiveLock);

    if IsBinaryUpgrade {
        /* Use binary-upgrade override for tablespace oid */
        if !OidIsValid(binary_upgrade_next_pg_tablespace_oid) {
            ereport!(ERROR,
                     // C: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                     errmsg!("pg_tablespace OID value not set when in binary upgrade mode"));
        }

        tablespaceoid = binary_upgrade_next_pg_tablespace_oid;
        binary_upgrade_next_pg_tablespace_oid = InvalidOid;
    } else {
        tablespaceoid = GetNewOidWithIndex(rel, TablespaceOidIndexId,
                                           Anum_pg_tablespace_oid as i16);
    }
    values[Anum_pg_tablespace_oid - 1] = ObjectIdGetDatum(tablespaceoid);
    values[Anum_pg_tablespace_spcname - 1] =
        DirectFunctionCall1_namein(CStringGetDatum((*stmt).tablespacename));
    values[Anum_pg_tablespace_spcowner - 1] =
        ObjectIdGetDatum(ownerId);
    nulls[Anum_pg_tablespace_spcacl - 1] = true;

    /* Generate new proposed spcoptions (text array) */
    newOptions = transformRelOptions(0 as Datum,
                                     (*stmt).options,
                                     null(), null_mut(), false, false);
    let _ = tablespace_reloptions(newOptions, true);
    if newOptions != 0 as Datum {
        values[Anum_pg_tablespace_spcoptions - 1] = newOptions;
    } else {
        nulls[Anum_pg_tablespace_spcoptions - 1] = true;
    }

    tuple = heap_form_tuple(RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(rel, tuple);

    heap_freetuple(tuple);

    /* Record dependency on owner */
    recordDependencyOnOwner(TableSpaceRelationId, tablespaceoid, ownerId);

    /* Post creation hook for new tablespace */
    InvokeObjectPostCreateHook(TableSpaceRelationId, tablespaceoid, 0);

    create_tablespace_directories(location, tablespaceoid);

    /* Record the filesystem change in XLOG */
    {
        let mut xlrec: xl_tblspc_create_rec = core::mem::zeroed();

        xlrec.ts_id = tablespaceoid;

        XLogBeginInsert();
        XLogRegisterData(&xlrec as *const xl_tblspc_create_rec as *const c_void,
                         core::mem::offset_of!(xl_tblspc_create_rec, ts_path));
        XLogRegisterData(location as *const c_void, strlen(location) + 1);

        let _ = XLogInsert(RM_TBLSPC_ID, XLOG_TBLSPC_CREATE);
    }

    /*
     * Force synchronous commit, to minimize the window between creating the
     * symlink on-disk and marking the transaction committed.  It's not great
     * that there is any window at all, but definitely we don't want to make
     * it larger than necessary.
     */
    ForceSyncCommit();

    pfree(location as *mut c_void);

    /* We keep the lock on pg_tablespace until commit */
    table_close(rel, NoLock);

    return tablespaceoid;
}

/*
 * Drop a table space
 *
 * Be careful to check that the tablespace is empty.
 */
pub unsafe fn DropTableSpace(stmt: *mut DropTableSpaceStmt) {
    let tablespacename: *mut c_char = (*stmt).tablespacename;
    let scandesc: TableScanDesc;
    let rel: Relation;
    let tuple: HeapTuple;
    let spcform: Form_pg_tablespace;
    let mut entry: [ScanKeyData; 1] = [core::mem::zeroed(); 1];
    let tablespaceoid: Oid;
    let mut detail: *mut c_char = null_mut();
    let mut detail_log: *mut c_char = null_mut();

    /*
     * Find the target tuple
     */
    rel = table_open(TableSpaceRelationId, RowExclusiveLock);

    ScanKeyInit(&mut entry[0],
                Anum_pg_tablespace_spcname as i16,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(tablespacename));
    scandesc = table_beginscan_catalog(rel, 1, entry.as_mut_ptr());
    tuple = heap_getnext(scandesc, ForwardScanDirection);

    if !HeapTupleIsValid(tuple) {
        if !(*stmt).missing_ok {
            ereport!(ERROR,
                     // C: errcode(ERRCODE_UNDEFINED_OBJECT)
                     errmsg!("tablespace \"{}\" does not exist",
                             CStr::from_ptr(tablespacename).to_string_lossy()));
        } else {
            ereport!(NOTICE,
                     errmsg!("tablespace \"{}\" does not exist, skipping",
                             CStr::from_ptr(tablespacename).to_string_lossy()));
            table_endscan(scandesc);
            table_close(rel, NoLock);
        }
        return;
    }

    spcform = GETSTRUCT(tuple) as Form_pg_tablespace;
    tablespaceoid = (*spcform).oid;

    /* Must be tablespace owner */
    if !object_ownercheck(TableSpaceRelationId, tablespaceoid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLESPACE,
                       tablespacename);
    }

    /* Disallow drop of the standard tablespaces, even by superuser */
    if IsPinnedObject(TableSpaceRelationId, tablespaceoid) {
        aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_TABLESPACE,
                       tablespacename);
    }

    /* Check for pg_shdepend entries depending on this tablespace */
    if checkSharedDependencies(TableSpaceRelationId, tablespaceoid,
                               &mut detail, &mut detail_log) {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
                 // C also: errdetail_internal("%s", detail), errdetail_log("%s", detail_log)
                 errmsg!("tablespace \"{}\" cannot be dropped because some objects depend on it",
                         CStr::from_ptr(tablespacename).to_string_lossy()));
    }

    /* DROP hook for the tablespace being removed */
    InvokeObjectDropHook(TableSpaceRelationId, tablespaceoid, 0);

    /*
     * Remove the pg_tablespace tuple (this will roll back if we fail below)
     */
    CatalogTupleDelete(rel, &mut (*tuple).t_self);

    table_endscan(scandesc);

    /*
     * Remove any comments or security labels on this tablespace.
     */
    DeleteSharedComments(tablespaceoid, TableSpaceRelationId);
    DeleteSharedSecurityLabel(tablespaceoid, TableSpaceRelationId);

    /*
     * Remove dependency on owner.
     */
    deleteSharedDependencyRecordsFor(TableSpaceRelationId, tablespaceoid, 0);

    /*
     * Acquire TablespaceCreateLock to ensure that no TablespaceCreateDbspace
     * is running concurrently.
     */
    LWLockAcquire(TablespaceCreateLock(), LW_EXCLUSIVE);

    /*
     * Try to remove the physical infrastructure.
     */
    if !destroy_tablespace_directories(tablespaceoid, false) {
        /*
         * Not all files deleted?  However, there can be lingering empty files
         * in the directories, left behind by for example DROP TABLE, that
         * have been scheduled for deletion at next checkpoint (see comments
         * in mdunlink() for details).  We could just delete them immediately,
         * but we can't tell them apart from important data files that we
         * mustn't delete.  So instead, we force a checkpoint which will clean
         * out any lingering files, and try again.
         */
        RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

        /*
         * On Windows, an unlinked file persists in the directory listing
         * until no process retains an open handle for the file.  The DDL
         * commands that schedule files for unlink send invalidation messages
         * directing other PostgreSQL processes to close the files, but
         * nothing guarantees they'll be processed in time.  So, we'll also
         * use a global barrier to ask all backends to close all files, and
         * wait until they're finished.
         */
        LWLockRelease(TablespaceCreateLock());
        WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));
        LWLockAcquire(TablespaceCreateLock(), LW_EXCLUSIVE);

        /* And now try again. */
        if !destroy_tablespace_directories(tablespaceoid, false) {
            /* Still not empty, the files must be important then */
            ereport!(ERROR,
                     // C: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                     errmsg!("tablespace \"{}\" is not empty",
                             CStr::from_ptr(tablespacename).to_string_lossy()));
        }
    }

    /* Record the filesystem change in XLOG */
    {
        let mut xlrec: xl_tblspc_drop_rec = core::mem::zeroed();

        xlrec.ts_id = tablespaceoid;

        XLogBeginInsert();
        XLogRegisterData(&xlrec as *const xl_tblspc_drop_rec as *const c_void,
                         core::mem::size_of::<xl_tblspc_drop_rec>());

        let _ = XLogInsert(RM_TBLSPC_ID, XLOG_TBLSPC_DROP);
    }

    /*
     * Note: because we checked that the tablespace was empty, there should be
     * no need to worry about flushing shared buffers or free space map
     * entries for relations in the tablespace.
     */

    /*
     * Force synchronous commit, to minimize the window between removing the
     * files on-disk and marking the transaction committed.  It's not great
     * that there is any window at all, but definitely we don't want to make
     * it larger than necessary.
     */
    ForceSyncCommit();

    /*
     * Allow TablespaceCreateDbspace again.
     */
    LWLockRelease(TablespaceCreateLock());

    /* We keep the lock on pg_tablespace until commit */
    table_close(rel, NoLock);
}


/*
 * create_tablespace_directories
 *
 *	Attempt to create filesystem infrastructure linking $PGDATA/pg_tblspc/
 *	to the specified directory
 */
unsafe fn create_tablespace_directories(location: *const c_char, tablespaceoid: Oid) {
    let linkloc: *mut c_char;
    let location_with_version_dir: *mut c_char;
    let mut st: stat_struct = core::mem::zeroed();
    let in_place: bool;

    linkloc = psprintf_s_u(c"%s/%u".as_ptr(), PG_TBLSPC_DIR.as_ptr(), tablespaceoid);

    /*
     * If we're asked to make an 'in place' tablespace, create the directory
     * directly where the symlink would normally go.  This is a developer-only
     * option for now, to facilitate regression testing.
     */
    in_place = strlen(location) == 0;

    if in_place {
        if MakePGDirectory(linkloc) < 0 && errno() != EEXIST {
            ereport!(ERROR,
                     // C: errcode_for_file_access()
                     errmsg!("could not create directory \"{}\": %m",
                             CStr::from_ptr(linkloc).to_string_lossy()));
        }
    }

    location_with_version_dir = psprintf_s_s(c"%s/%s".as_ptr(),
                                             if in_place { linkloc } else { location as *mut c_char },
                                             TABLESPACE_VERSION_DIRECTORY.as_ptr());

    /*
     * Attempt to coerce target directory to safe permissions.  If this fails,
     * it doesn't exist or has the wrong owner.  Not needed for in-place mode,
     * because in that case we created the directory with the desired
     * permissions.
     */
    if !in_place && chmod(location, pg_dir_create_mode) != 0 {
        if errno() == ENOENT {
            ereport!(ERROR,
                     // C: errcode(ERRCODE_UNDEFINED_FILE)
                     // C also: InRecovery ? errhint("Create this directory for the tablespace before restarting the server.") : 0
                     errmsg!("directory \"{}\" does not exist",
                             CStr::from_ptr(location).to_string_lossy()));
        } else {
            ereport!(ERROR,
                     // C: errcode_for_file_access()
                     errmsg!("could not set permissions on directory \"{}\": %m",
                             CStr::from_ptr(location).to_string_lossy()));
        }
    }

    /*
     * The creation of the version directory prevents more than one tablespace
     * in a single location.  This imitates TablespaceCreateDbspace(), but it
     * ignores concurrency and missing parent directories.  The chmod() would
     * have failed in the absence of a parent.  pg_tablespace_spcname_index
     * prevents concurrency.
     */
    if stat(location_with_version_dir, &mut st) < 0 {
        if errno() != ENOENT {
            ereport!(ERROR,
                     // C: errcode_for_file_access()
                     errmsg!("could not stat directory \"{}\": %m",
                             CStr::from_ptr(location_with_version_dir).to_string_lossy()));
        } else if MakePGDirectory(location_with_version_dir) < 0 {
            ereport!(ERROR,
                     // C: errcode_for_file_access()
                     errmsg!("could not create directory \"{}\": %m",
                             CStr::from_ptr(location_with_version_dir).to_string_lossy()));
        }
    } else if !S_ISDIR(&st) {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_WRONG_OBJECT_TYPE)
                 errmsg!("\"{}\" exists but is not a directory",
                         CStr::from_ptr(location_with_version_dir).to_string_lossy()));
    } else if !InRecovery {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_OBJECT_IN_USE)
                 errmsg!("directory \"{}\" already in use as a tablespace",
                         CStr::from_ptr(location_with_version_dir).to_string_lossy()));
    }

    /*
     * In recovery, remove old symlink, in case it points to the wrong place.
     */
    if !in_place && InRecovery {
        remove_tablespace_symlink(linkloc);
    }

    /*
     * Create the symlink under PGDATA
     */
    if !in_place && symlink(location, linkloc) < 0 {
        ereport!(ERROR,
                 // C: errcode_for_file_access()
                 errmsg!("could not create symbolic link \"{}\": %m",
                         CStr::from_ptr(linkloc).to_string_lossy()));
    }

    pfree(linkloc as *mut c_void);
    pfree(location_with_version_dir as *mut c_void);
}


/*
 * destroy_tablespace_directories
 *
 * Attempt to remove filesystem infrastructure for the tablespace.
 *
 * 'redo' indicates we are redoing a drop from XLOG; in that case we should
 * not throw an ERROR for problems, just LOG them.  The worst consequence of
 * not removing files here would be failure to release some disk space, which
 * does not justify throwing an error that would require manual intervention
 * to get the database running again.
 *
 * Returns true if successful, false if some subdirectory is not empty
 */
unsafe fn destroy_tablespace_directories(tablespaceoid: Oid, redo: bool) -> bool {
    let linkloc: *mut c_char;
    let linkloc_with_version_dir: *mut c_char;
    let dirdesc: *mut DIR;
    let mut de: *mut dirent;
    let mut subfile: *mut c_char;
    let mut st: stat_struct = core::mem::zeroed();

    linkloc_with_version_dir = psprintf_s_u_s(c"%s/%u/%s".as_ptr(), PG_TBLSPC_DIR.as_ptr(),
                                              tablespaceoid, TABLESPACE_VERSION_DIRECTORY.as_ptr());

    /*
     * Check if the tablespace still contains any files.  We try to rmdir each
     * per-database directory we find in it.  rmdir failure implies there are
     * still files in that subdirectory, so give up.  (We do not have to worry
     * about undoing any already completed rmdirs, since the next attempt to
     * use the tablespace from that database will simply recreate the
     * subdirectory via TablespaceCreateDbspace.)
     *
     * Since we hold TablespaceCreateLock, no one else should be creating any
     * fresh subdirectories in parallel. It is possible that new files are
     * being created within subdirectories, though, so the rmdir call could
     * fail.  Worst consequence is a less friendly error message.
     *
     * If redo is true then ENOENT is a likely outcome here, and we allow it
     * to pass without comment.  In normal operation we still allow it, but
     * with a warning.  This is because even though ProcessUtility disallows
     * DROP TABLESPACE in a transaction block, it's possible that a previous
     * DROP failed and rolled back after removing the tablespace directories
     * and/or symlink.  We want to allow a new DROP attempt to succeed at
     * removing the catalog entries (and symlink if still present), so we
     * should not give a hard error here.
     */
    // C goto target 'remove_symlink: the block below breaks 'remove_symlink to skip
    // directly to the symlink-removal code that follows it.
    'remove_symlink: {
        dirdesc = AllocateDir(linkloc_with_version_dir);
        if dirdesc.is_null() {
            if errno() == ENOENT {
                if !redo {
                    ereport!(WARNING,
                             // C: errcode_for_file_access()
                             errmsg!("could not open directory \"{}\": %m",
                                     CStr::from_ptr(linkloc_with_version_dir).to_string_lossy()));
                }
                /* The symlink might still exist, so go try to remove it */
                break 'remove_symlink;
            } else if redo {
                /* in redo, just log other types of error */
                ereport!(LOG,
                         // C: errcode_for_file_access()
                         errmsg!("could not open directory \"{}\": %m",
                                 CStr::from_ptr(linkloc_with_version_dir).to_string_lossy()));
                pfree(linkloc_with_version_dir as *mut c_void);
                return false;
            }
            /* else let ReadDir report the error */
        }

        loop {
            de = ReadDir(dirdesc, linkloc_with_version_dir);
            if de.is_null() {
                break;
            }

            if strcmp((*de).d_name.as_ptr(), c".".as_ptr()) == 0 ||
                strcmp((*de).d_name.as_ptr(), c"..".as_ptr()) == 0 {
                continue;
            }

            subfile = psprintf_s_s(c"%s/%s".as_ptr(), linkloc_with_version_dir,
                                   (*de).d_name.as_ptr());

            /* This check is just to deliver a friendlier error message */
            if !redo && !directory_is_empty(subfile) {
                FreeDir(dirdesc);
                pfree(subfile as *mut c_void);
                pfree(linkloc_with_version_dir as *mut c_void);
                return false;
            }

            /* remove empty directory */
            if rmdir(subfile) < 0 {
                ereport!(if redo { LOG } else { ERROR },
                         // C: errcode_for_file_access()
                         errmsg!("could not remove directory \"{}\": %m",
                                 CStr::from_ptr(subfile).to_string_lossy()));
            }

            pfree(subfile as *mut c_void);
        }

        FreeDir(dirdesc);

        /* remove version directory */
        if rmdir(linkloc_with_version_dir) < 0 {
            ereport!(if redo { LOG } else { ERROR },
                     // C: errcode_for_file_access()
                     errmsg!("could not remove directory \"{}\": %m",
                             CStr::from_ptr(linkloc_with_version_dir).to_string_lossy()));
            pfree(linkloc_with_version_dir as *mut c_void);
            return false;
        }
    } // 'remove_symlink:

    /*
     * Try to remove the symlink.  We must however deal with the possibility
     * that it's a directory instead of a symlink --- this could happen during
     * WAL replay (see TablespaceCreateDbspace).
     *
     * Note: in the redo case, we'll return true if this final step fails;
     * there's no point in retrying it.  Also, ENOENT should provoke no more
     * than a warning.
     */
    linkloc = pstrdup(linkloc_with_version_dir);
    get_parent_directory(linkloc);
    if lstat(linkloc, &mut st) < 0 {
        let saved_errno = errno();

        ereport!(if redo { LOG } else if saved_errno == ENOENT { WARNING } else { ERROR },
                 // C: errcode_for_file_access()
                 errmsg!("could not stat file \"{}\": %m",
                         CStr::from_ptr(linkloc).to_string_lossy()));
    } else if S_ISDIR(&st) {
        if rmdir(linkloc) < 0 {
            let saved_errno = errno();

            ereport!(if redo { LOG } else if saved_errno == ENOENT { WARNING } else { ERROR },
                     // C: errcode_for_file_access()
                     errmsg!("could not remove directory \"{}\": %m",
                             CStr::from_ptr(linkloc).to_string_lossy()));
        }
    } else if S_ISLNK(&st) {
        if unlink(linkloc) < 0 {
            let saved_errno = errno();

            ereport!(if redo { LOG } else if saved_errno == ENOENT { WARNING } else { ERROR },
                     // C: errcode_for_file_access()
                     errmsg!("could not remove symbolic link \"{}\": %m",
                             CStr::from_ptr(linkloc).to_string_lossy()));
        }
    } else {
        /* Refuse to remove anything that's not a directory or symlink */
        ereport!(if redo { LOG } else { ERROR },
                 // C: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                 errmsg!("\"{}\" is not a directory or symbolic link",
                         CStr::from_ptr(linkloc).to_string_lossy()));
    }

    pfree(linkloc_with_version_dir as *mut c_void);
    pfree(linkloc as *mut c_void);

    return true;
}


/*
 * Check if a directory is empty.
 *
 * This probably belongs somewhere else, but not sure where...
 */
pub unsafe fn directory_is_empty(path: *const c_char) -> bool {
    let dirdesc: *mut DIR;
    let mut de: *mut dirent;

    dirdesc = AllocateDir(path);

    loop {
        de = ReadDir(dirdesc, path);
        if de.is_null() {
            break;
        }

        if strcmp((*de).d_name.as_ptr(), c".".as_ptr()) == 0 ||
            strcmp((*de).d_name.as_ptr(), c"..".as_ptr()) == 0 {
            continue;
        }
        FreeDir(dirdesc);
        return false;
    }

    FreeDir(dirdesc);
    return true;
}

/*
 *	remove_tablespace_symlink
 *
 * This function removes symlinks in pg_tblspc.  On Windows, junction points
 * act like directories so we must be able to apply rmdir.  This function
 * works like the symlink removal code in destroy_tablespace_directories,
 * except that failure to remove is always an ERROR.  But if the file doesn't
 * exist at all, that's OK.
 */
pub unsafe fn remove_tablespace_symlink(linkloc: *const c_char) {
    let mut st: stat_struct = core::mem::zeroed();

    if lstat(linkloc, &mut st) < 0 {
        if errno() == ENOENT {
            return;
        }
        ereport!(ERROR,
                 // C: errcode_for_file_access()
                 errmsg!("could not stat file \"{}\": %m",
                         CStr::from_ptr(linkloc).to_string_lossy()));
    }

    if S_ISDIR(&st) {
        /*
         * This will fail if the directory isn't empty, but not if it's a
         * junction point.
         */
        if rmdir(linkloc) < 0 && errno() != ENOENT {
            ereport!(ERROR,
                     // C: errcode_for_file_access()
                     errmsg!("could not remove directory \"{}\": %m",
                             CStr::from_ptr(linkloc).to_string_lossy()));
        }
    } else if S_ISLNK(&st) {
        if unlink(linkloc) < 0 && errno() != ENOENT {
            ereport!(ERROR,
                     // C: errcode_for_file_access()
                     errmsg!("could not remove symbolic link \"{}\": %m",
                             CStr::from_ptr(linkloc).to_string_lossy()));
        }
    } else {
        /* Refuse to remove anything that's not a directory or symlink */
        ereport!(ERROR,
                 // C: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                 errmsg!("\"{}\" is not a directory or symbolic link",
                         CStr::from_ptr(linkloc).to_string_lossy()));
    }
}

/*
 * Rename a tablespace
 */
pub unsafe fn RenameTableSpace(oldname: *const c_char, newname: *const c_char) -> ObjectAddress {
    let tspId: Oid;
    let rel: Relation;
    let mut entry: [ScanKeyData; 1] = [core::mem::zeroed(); 1];
    let mut scan: TableScanDesc;
    let mut tup: HeapTuple;
    let newtuple: HeapTuple;
    let newform: Form_pg_tablespace;
    let mut address: ObjectAddress = core::mem::zeroed();

    /* Search pg_tablespace */
    rel = table_open(TableSpaceRelationId, RowExclusiveLock);

    ScanKeyInit(&mut entry[0],
                Anum_pg_tablespace_spcname as i16,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(oldname));
    scan = table_beginscan_catalog(rel, 1, entry.as_mut_ptr());
    tup = heap_getnext(scan, ForwardScanDirection);
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_UNDEFINED_OBJECT)
                 errmsg!("tablespace \"{}\" does not exist",
                         CStr::from_ptr(oldname).to_string_lossy()));
    }

    newtuple = heap_copytuple(tup);
    newform = GETSTRUCT(newtuple) as Form_pg_tablespace;
    tspId = (*newform).oid;

    table_endscan(scan);

    /* Must be owner */
    if !object_ownercheck(TableSpaceRelationId, tspId, GetUserId()) {
        aclcheck_error(ACLCHECK_NO_PRIV, OBJECT_TABLESPACE, oldname);
    }

    /* Validate new name */
    if !allowSystemTableMods && IsReservedName(newname) {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_RESERVED_NAME)
                 // C also: errdetail("The prefix \"pg_\" is reserved for system tablespaces.")
                 errmsg!("unacceptable tablespace name \"{}\"",
                         CStr::from_ptr(newname).to_string_lossy()));
    }

    /*
     * If built with appropriate switch, whine when regression-testing
     * conventions for tablespace names are violated.
     */
    // #ifdef ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS
    //     if (strncmp(newname, "regress_", 8) != 0)
    //         elog(WARNING, "tablespaces created by regression test cases should have names starting with \"regress_\"");
    // #endif

    /* Make sure the new name doesn't exist */
    ScanKeyInit(&mut entry[0],
                Anum_pg_tablespace_spcname as i16,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(newname));
    scan = table_beginscan_catalog(rel, 1, entry.as_mut_ptr());
    tup = heap_getnext(scan, ForwardScanDirection);
    if HeapTupleIsValid(tup) {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_DUPLICATE_OBJECT)
                 errmsg!("tablespace \"{}\" already exists",
                         CStr::from_ptr(newname).to_string_lossy()));
    }

    table_endscan(scan);

    /* OK, update the entry */
    namestrcpy(&mut (*newform).spcname, newname);

    CatalogTupleUpdate(rel, &mut (*newtuple).t_self, newtuple);

    InvokeObjectPostAlterHook(TableSpaceRelationId, tspId, 0);

    ObjectAddressSet(&mut address, TableSpaceRelationId, tspId);

    table_close(rel, NoLock);

    return address;
}

/*
 * Alter table space options
 */
pub unsafe fn AlterTableSpaceOptions(stmt: *mut AlterTableSpaceOptionsStmt) -> Oid {
    let rel: Relation;
    let mut entry: [ScanKeyData; 1] = [core::mem::zeroed(); 1];
    let scandesc: TableScanDesc;
    let tup: HeapTuple;
    let tablespaceoid: Oid;
    let datum: Datum;
    let newOptions: Datum;
    let mut repl_val: [Datum; Natts_pg_tablespace] = [0 as Datum; Natts_pg_tablespace];
    let mut isnull: bool = false;
    let mut repl_null: [bool; Natts_pg_tablespace] = [false; Natts_pg_tablespace];
    let mut repl_repl: [bool; Natts_pg_tablespace] = [false; Natts_pg_tablespace];
    let newtuple: HeapTuple;

    /* Search pg_tablespace */
    rel = table_open(TableSpaceRelationId, RowExclusiveLock);

    ScanKeyInit(&mut entry[0],
                Anum_pg_tablespace_spcname as i16,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum((*stmt).tablespacename));
    scandesc = table_beginscan_catalog(rel, 1, entry.as_mut_ptr());
    tup = heap_getnext(scandesc, ForwardScanDirection);
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_UNDEFINED_OBJECT)
                 errmsg!("tablespace \"{}\" does not exist",
                         CStr::from_ptr((*stmt).tablespacename).to_string_lossy()));
    }

    tablespaceoid = (*(GETSTRUCT(tup) as Form_pg_tablespace)).oid;

    /* Must be owner of the existing object */
    if !object_ownercheck(TableSpaceRelationId, tablespaceoid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLESPACE,
                       (*stmt).tablespacename);
    }

    /* Generate new proposed spcoptions (text array) */
    datum = heap_getattr(tup, Anum_pg_tablespace_spcoptions as c_int,
                         RelationGetDescr(rel), &mut isnull);
    newOptions = transformRelOptions(if isnull { 0 as Datum } else { datum },
                                     (*stmt).options, null(), null_mut(), false,
                                     (*stmt).isReset);
    let _ = tablespace_reloptions(newOptions, true);

    /* Build new tuple. */
    repl_null.fill(false);
    repl_repl.fill(false);
    if newOptions != 0 as Datum {
        repl_val[Anum_pg_tablespace_spcoptions - 1] = newOptions;
    } else {
        repl_null[Anum_pg_tablespace_spcoptions - 1] = true;
    }
    repl_repl[Anum_pg_tablespace_spcoptions - 1] = true;
    newtuple = heap_modify_tuple(tup, RelationGetDescr(rel), repl_val.as_mut_ptr(),
                                 repl_null.as_mut_ptr(), repl_repl.as_mut_ptr());

    /* Update system catalog. */
    CatalogTupleUpdate(rel, &mut (*newtuple).t_self, newtuple);

    InvokeObjectPostAlterHook(TableSpaceRelationId, tablespaceoid, 0);

    heap_freetuple(newtuple);

    /* Conclude heap scan. */
    table_endscan(scandesc);
    table_close(rel, NoLock);

    return tablespaceoid;
}

/*
 * Routines for handling the GUC variable 'default_tablespace'.
 */

/* check_hook: validate new default_tablespace */
pub unsafe fn check_default_tablespace(newval: *mut *mut c_char, extra: *mut *mut c_void,
                                       source: GucSource) -> bool {
    /*
     * If we aren't inside a transaction, or connected to a database, we
     * cannot do the catalog accesses necessary to verify the name.  Must
     * accept the value on faith.
     */
    if IsTransactionState() && MyDatabaseId != InvalidOid {
        if **newval != b'\0' as c_char &&
            !OidIsValid(get_tablespace_oid(*newval, true)) {
            /*
             * When source == PGC_S_TEST, don't throw a hard error for a
             * nonexistent tablespace, only a NOTICE.  See comments in guc.h.
             */
            if source == GucSource::PGC_S_TEST {
                ereport!(NOTICE,
                         // C: errcode(ERRCODE_UNDEFINED_OBJECT)
                         errmsg!("tablespace \"{}\" does not exist",
                                 CStr::from_ptr(*newval).to_string_lossy()));
            } else {
                GUC_check_errdetail_impl(c"Tablespace \"%s\" does not exist.".as_ptr(),
                                         *newval);
                return false;
            }
        }
    }

    return true;
}

/*
 * GetDefaultTablespace -- get the OID of the current default tablespace
 *
 * Temporary objects have different default tablespaces, hence the
 * relpersistence parameter must be specified.  Also, for partitioned tables,
 * we disallow specifying the database default, so that needs to be specified
 * too.
 *
 * May return InvalidOid to indicate "use the database's default tablespace".
 *
 * Note that caller is expected to check appropriate permissions for any
 * result other than InvalidOid.
 *
 * This exists to hide (and possibly optimize the use of) the
 * default_tablespace GUC variable.
 */
pub unsafe fn GetDefaultTablespace(relpersistence: c_char, partitioned: bool) -> Oid {
    let mut result: Oid;

    /* The temp-table case is handled elsewhere */
    if relpersistence == RELPERSISTENCE_TEMP {
        PrepareTempTablespaces();
        return GetNextTempTableSpace();
    }

    /* Fast path for default_tablespace == "" */
    if default_tablespace.is_null() || *default_tablespace == b'\0' as c_char {
        return InvalidOid;
    }

    /*
     * It is tempting to cache this lookup for more speed, but then we would
     * fail to detect the case where the tablespace was dropped since the GUC
     * variable was set.  Note also that we don't complain if the value fails
     * to refer to an existing tablespace; we just silently return InvalidOid,
     * causing the new object to be created in the database's tablespace.
     */
    result = get_tablespace_oid(default_tablespace, true);

    /*
     * Allow explicit specification of database's default tablespace in
     * default_tablespace without triggering permissions checks.  Don't allow
     * specifying that when creating a partitioned table, however, since the
     * result is confusing.
     */
    if result == MyDatabaseTableSpace {
        if partitioned {
            ereport!(ERROR,
                     // C: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                     errmsg!("cannot specify default tablespace for partitioned relations"));
        }
        result = InvalidOid;
    }
    return result;
}


/*
 * Routines for handling the GUC variable 'temp_tablespaces'.
 */

#[repr(C)]
struct temp_tablespaces_extra {
    /* Array of OIDs to be passed to SetTempTablespaces() */
    numSpcs: c_int,
    tblSpcs: [Oid; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/* check_hook: validate new temp_tablespaces */
pub unsafe fn check_temp_tablespaces(newval: *mut *mut c_char, extra: *mut *mut c_void,
                                     source: GucSource) -> bool {
    let rawname: *mut c_char;
    let mut namelist: *mut List = null_mut();

    /* Need a modifiable copy of string */
    rawname = pstrdup(*newval);

    /* Parse string into list of identifiers */
    if !SplitIdentifierString(rawname, b',' as c_char, &mut namelist) {
        /* syntax error in name list */
        GUC_check_errdetail_impl(c"List syntax is invalid.".as_ptr());
        pfree(rawname as *mut c_void);
        list_free(namelist);
        return false;
    }

    /*
     * If we aren't inside a transaction, or connected to a database, we
     * cannot do the catalog accesses necessary to verify the name.  Must
     * accept the value on faith. Fortunately, there's then also no need to
     * pass the data to fd.c.
     */
    if IsTransactionState() && MyDatabaseId != InvalidOid {
        let myextra: *mut temp_tablespaces_extra;
        let tblSpcs: *mut Oid;
        let mut numSpcs: c_int;

        /* temporary workspace until we are done verifying the list */
        tblSpcs = palloc(list_length(namelist) as usize * core::mem::size_of::<Oid>()) as *mut Oid;
        numSpcs = 0;
        foreach!(l, namelist, {
            // C `continue` -> `break 'cont` (skips to next iteration; foreach!'s
            // index increment runs after this block).
            'cont: {
            let curname = lfirst(current_cell!(l)) as *mut c_char;
            let curoid: Oid;
            let aclresult: AclResult;

            /* Allow an empty string (signifying database default) */
            if *curname == b'\0' as c_char {
                /* InvalidOid signifies database's default tablespace */
                *tblSpcs.offset(numSpcs as isize) = InvalidOid;
                numSpcs += 1;
                break 'cont;
            }

            /*
             * In an interactive SET command, we ereport for bad info.  When
             * source == PGC_S_TEST, don't throw a hard error for a
             * nonexistent tablespace, only a NOTICE.  See comments in guc.h.
             */
            curoid = get_tablespace_oid(curname, source <= GucSource::PGC_S_TEST);
            if curoid == InvalidOid {
                if source == GucSource::PGC_S_TEST {
                    ereport!(NOTICE,
                             // C: errcode(ERRCODE_UNDEFINED_OBJECT)
                             errmsg!("tablespace \"{}\" does not exist",
                                     CStr::from_ptr(curname).to_string_lossy()));
                }
                break 'cont;
            }

            /*
             * Allow explicit specification of database's default tablespace
             * in temp_tablespaces without triggering permissions checks.
             */
            if curoid == MyDatabaseTableSpace {
                /* InvalidOid signifies database's default tablespace */
                *tblSpcs.offset(numSpcs as isize) = InvalidOid;
                numSpcs += 1;
                break 'cont;
            }

            /* Check permissions, similarly complaining only if interactive */
            aclresult = object_aclcheck(TableSpaceRelationId, curoid, GetUserId(),
                                        ACL_CREATE);
            if aclresult != ACLCHECK_OK {
                if source >= GucSource::PGC_S_INTERACTIVE {
                    aclcheck_error(aclresult, OBJECT_TABLESPACE, curname);
                }
                break 'cont;
            }

            *tblSpcs.offset(numSpcs as isize) = curoid;
            numSpcs += 1;
            } // 'cont:
        });

        /* Now prepare an "extra" struct for assign_temp_tablespaces */
        myextra = guc_malloc(LOG, core::mem::offset_of!(temp_tablespaces_extra, tblSpcs) +
                             numSpcs as usize * core::mem::size_of::<Oid>()) as *mut temp_tablespaces_extra;
        if myextra.is_null() {
            return false;
        }
        (*myextra).numSpcs = numSpcs;
        memcpy((*myextra).tblSpcs.as_mut_ptr() as *mut c_void, tblSpcs as *const c_void,
               numSpcs as usize * core::mem::size_of::<Oid>());
        *extra = myextra as *mut c_void;

        pfree(tblSpcs as *mut c_void);
    }

    pfree(rawname as *mut c_void);
    list_free(namelist);

    return true;
}

/* assign_hook: do extra actions as needed */
pub unsafe fn assign_temp_tablespaces(newval: *const c_char, extra: *mut c_void) {
    let myextra: *mut temp_tablespaces_extra = extra as *mut temp_tablespaces_extra;

    /*
     * If check_temp_tablespaces was executed inside a transaction, then pass
     * the list it made to fd.c.  Otherwise, clear fd.c's list; we must be
     * still outside a transaction, or else restoring during transaction exit,
     * and in either case we can just let the next PrepareTempTablespaces call
     * make things sane.
     */
    if !myextra.is_null() {
        SetTempTablespaces((*myextra).tblSpcs.as_mut_ptr(), (*myextra).numSpcs);
    } else {
        SetTempTablespaces(null_mut(), 0);
    }
}

/*
 * PrepareTempTablespaces -- prepare to use temp tablespaces
 *
 * If we have not already done so in the current transaction, parse the
 * temp_tablespaces GUC variable and tell fd.c which tablespace(s) to use
 * for temp files.
 */
pub unsafe fn PrepareTempTablespaces() {
    let rawname: *mut c_char;
    let mut namelist: *mut List = null_mut();
    let tblSpcs: *mut Oid;
    let mut numSpcs: c_int;

    /* No work if already done in current transaction */
    if TempTablespacesAreSet() {
        return;
    }

    /*
     * Can't do catalog access unless within a transaction.  This is just a
     * safety check in case this function is called by low-level code that
     * could conceivably execute outside a transaction.  Note that in such a
     * scenario, fd.c will fall back to using the current database's default
     * tablespace, which should always be OK.
     */
    if !IsTransactionState() {
        return;
    }

    /* Need a modifiable copy of string */
    rawname = pstrdup(temp_tablespaces);

    /* Parse string into list of identifiers */
    if !SplitIdentifierString(rawname, b',' as c_char, &mut namelist) {
        /* syntax error in name list */
        SetTempTablespaces(null_mut(), 0);
        pfree(rawname as *mut c_void);
        list_free(namelist);
        return;
    }

    /* Store tablespace OIDs in an array in TopTransactionContext */
    tblSpcs = MemoryContextAlloc(TopTransactionContext,
                                 list_length(namelist) as usize * core::mem::size_of::<Oid>()) as *mut Oid;
    numSpcs = 0;
    foreach!(l, namelist, {
        // C `continue` -> `break 'cont` (foreach!'s index increment runs after).
        'cont: {
        let curname = lfirst(current_cell!(l)) as *mut c_char;
        let curoid: Oid;
        let aclresult: AclResult;

        /* Allow an empty string (signifying database default) */
        if *curname == b'\0' as c_char {
            /* InvalidOid signifies database's default tablespace */
            *tblSpcs.offset(numSpcs as isize) = InvalidOid;
            numSpcs += 1;
            break 'cont;
        }

        /* Else verify that name is a valid tablespace name */
        curoid = get_tablespace_oid(curname, true);
        if curoid == InvalidOid {
            /* Skip any bad list elements */
            break 'cont;
        }

        /*
         * Allow explicit specification of database's default tablespace in
         * temp_tablespaces without triggering permissions checks.
         */
        if curoid == MyDatabaseTableSpace {
            /* InvalidOid signifies database's default tablespace */
            *tblSpcs.offset(numSpcs as isize) = InvalidOid;
            numSpcs += 1;
            break 'cont;
        }

        /* Check permissions similarly */
        aclresult = object_aclcheck(TableSpaceRelationId, curoid, GetUserId(),
                                    ACL_CREATE);
        if aclresult != ACLCHECK_OK {
            break 'cont;
        }

        *tblSpcs.offset(numSpcs as isize) = curoid;
        numSpcs += 1;
        } // 'cont:
    });

    SetTempTablespaces(tblSpcs, numSpcs);

    pfree(rawname as *mut c_void);
    list_free(namelist);
}


/*
 * get_tablespace_oid - given a tablespace name, look up the OID
 *
 * If missing_ok is false, throw an error if tablespace name not found.  If
 * true, just return InvalidOid.
 */
pub unsafe fn get_tablespace_oid(tablespacename: *const c_char, missing_ok: bool) -> Oid {
    let mut result: Oid;
    let rel: Relation;
    let scandesc: TableScanDesc;
    let tuple: HeapTuple;
    let mut entry: [ScanKeyData; 1] = [core::mem::zeroed(); 1];

    /*
     * Search pg_tablespace.  We use a heapscan here even though there is an
     * index on name, on the theory that pg_tablespace will usually have just
     * a few entries and so an indexed lookup is a waste of effort.
     */
    rel = table_open(TableSpaceRelationId, AccessShareLock);

    ScanKeyInit(&mut entry[0],
                Anum_pg_tablespace_spcname as i16,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(tablespacename));
    scandesc = table_beginscan_catalog(rel, 1, entry.as_mut_ptr());
    tuple = heap_getnext(scandesc, ForwardScanDirection);

    /* We assume that there can be at most one matching tuple */
    if HeapTupleIsValid(tuple) {
        result = (*(GETSTRUCT(tuple) as Form_pg_tablespace)).oid;
    } else {
        result = InvalidOid;
    }

    table_endscan(scandesc);
    table_close(rel, AccessShareLock);

    if !OidIsValid(result) && !missing_ok {
        ereport!(ERROR,
                 // C: errcode(ERRCODE_UNDEFINED_OBJECT)
                 errmsg!("tablespace \"{}\" does not exist",
                         CStr::from_ptr(tablespacename).to_string_lossy()));
    }

    return result;
}

/*
 * get_tablespace_name - given a tablespace OID, look up the name
 *
 * Returns a palloc'd string, or NULL if no such tablespace.
 */
pub unsafe fn get_tablespace_name(spc_oid: Oid) -> *mut c_char {
    let result: *mut c_char;
    let rel: Relation;
    let scandesc: TableScanDesc;
    let tuple: HeapTuple;
    let mut entry: [ScanKeyData; 1] = [core::mem::zeroed(); 1];

    /*
     * Search pg_tablespace.  We use a heapscan here even though there is an
     * index on oid, on the theory that pg_tablespace will usually have just a
     * few entries and so an indexed lookup is a waste of effort.
     */
    rel = table_open(TableSpaceRelationId, AccessShareLock);

    ScanKeyInit(&mut entry[0],
                Anum_pg_tablespace_oid as i16,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(spc_oid));
    scandesc = table_beginscan_catalog(rel, 1, entry.as_mut_ptr());
    tuple = heap_getnext(scandesc, ForwardScanDirection);

    /* We assume that there can be at most one matching tuple */
    if HeapTupleIsValid(tuple) {
        result = pstrdup(NameStr!((*(GETSTRUCT(tuple) as Form_pg_tablespace)).spcname));
    } else {
        result = null_mut();
    }

    table_endscan(scandesc);
    table_close(rel, AccessShareLock);

    return result;
}


/*
 * TABLESPACE resource manager's routines
 */
pub unsafe fn tblspc_redo(record: *mut XLogReaderState) {
    let info: u8 = XLogRecGetInfo(record) & !XLR_INFO_MASK;

    /* Backup blocks are not used in tblspc records */
    Assert!(!XLogRecHasAnyBlockRefs(record));

    if info == XLOG_TBLSPC_CREATE {
        let xlrec = XLogRecGetData(record) as *mut xl_tblspc_create_rec;
        let location = (*xlrec).ts_path.as_ptr();

        create_tablespace_directories(location, (*xlrec).ts_id);
    } else if info == XLOG_TBLSPC_DROP {
        let xlrec = XLogRecGetData(record) as *mut xl_tblspc_drop_rec;

        /* Close all smgr fds in all backends. */
        WaitForProcSignalBarrier(EmitProcSignalBarrier(PROCSIGNAL_BARRIER_SMGRRELEASE));

        /*
         * If we issued a WAL record for a drop tablespace it implies that
         * there were no files in it at all when the DROP was done. That means
         * that no permanent objects can exist in it at this point.
         *
         * It is possible for standby users to be using this tablespace as a
         * location for their temporary files, so if we fail to remove all
         * files then do conflict processing and try again, if currently
         * enabled.
         *
         * Other possible reasons for failure include bollixed file
         * permissions on a standby server when they were okay on the primary,
         * etc etc. There's not much we can do about that, so just remove what
         * we can and press on.
         */
        if !destroy_tablespace_directories((*xlrec).ts_id, true) {
            ResolveRecoveryConflictWithTablespace((*xlrec).ts_id);

            /*
             * If we did recovery processing then hopefully the backends who
             * wrote temp files should have cleaned up and exited by now.  So
             * retry before complaining.  If we fail again, this is just a LOG
             * condition, because it's not worth throwing an ERROR for (as
             * that would crash the database and require manual intervention
             * before we could get past this WAL record on restart).
             */
            if !destroy_tablespace_directories((*xlrec).ts_id, true) {
                ereport!(LOG,
                         // C: errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                         // C also: errhint("You can remove the directories manually if necessary.")
                         errmsg!("directories for tablespace {} could not be removed",
                                 (*xlrec).ts_id));
            }
        }
    } else {
        elog!(PANIC, "tblspc_redo: unknown op code {}", info);
    }
}

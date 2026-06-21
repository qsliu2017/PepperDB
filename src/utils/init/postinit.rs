/*-------------------------------------------------------------------------
 *
 * postinit.c -> postinit.rs
 *   postgres initialization utilities
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/utils/init/postinit.c
 *
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
// TODO(pg-port): real DATABASEOID cache id lives in utils/cache/syscache.h
const DATABASEOID: c_int = 21;

// ---------------------------------------------------------------------------
// REAL imports from ported modules
// ---------------------------------------------------------------------------

use crate::access::common::scankey::{ScanKeyData, ScanKeyInit};
use crate::access::common::session::InitializeSession;
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::index::genam::{systable_beginscan, systable_endscan, systable_getnext};
use crate::access::sdir::ForwardScanDirection;
use crate::access::stratnum::BTEqualStrategyNumber;
use crate::access::transam::xloginsert::InitXLogInsert;
use crate::c::NameData;
use crate::catalog::catalog_oids::{
    AuthIdRelationId, DatabaseRelationId, DbRoleSettingRelationId,
};
use crate::catalog::pg_database::Form_pg_database;
use crate::lib::stringinfo::{initStringInfo, StringInfoData};
use crate::miscadmin::{
    AmAutoVacuumLauncherProcess, AmAutoVacuumWorkerProcess, AmBackgroundWorkerProcess,
    AmLogicalSlotSyncWorkerProcess, AmRegularBackendProcess, IsBinaryUpgrade,
    IsBootstrapProcessingMode, IsUnderPostmaster, SetDatabasePath, ValidatePgVersion,
    INIT_PG_LOAD_SESSION_LIBS, INIT_PG_OVERRIDE_ALLOW_CONNS, INIT_PG_OVERRIDE_ROLE_LOGIN,
};
use crate::miscadmin::{
    GetSessionUserId, GetUserId, InitializeSessionUserId, InitializeSessionUserIdStandalone,
    InitializeSystemUser,
};
use crate::port::pgsleep::pg_usleep;
use crate::port::strlcpy::strlcpy;
use crate::storage::ipc::ipc::before_shmem_exit;
use crate::storage::lmgr::lmgr::LockSharedObject;
use crate::storage::lockdefs::{AccessShareLock, RowExclusiveLock};
use crate::utils::cache::syscache::{
    ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheGetAttrNotNull,
};
use crate::utils::elog::{DEBUG3, ERROR, FATAL, LOG, WARNING};
use crate::utils::misc::timeout::{
    disable_timeout, enable_timeout_after, RegisterTimeout, TimeoutId,
    CLIENT_CONNECTION_CHECK_TIMEOUT, DEADLOCK_TIMEOUT, IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
    IDLE_STATS_UPDATE_TIMEOUT, IDLE_SESSION_TIMEOUT, LOCK_TIMEOUT, STATEMENT_TIMEOUT,
    TRANSACTION_TIMEOUT,
};
use crate::utils::portal::EnablePortalManager;
use crate::utils::rel::RelationData;
use crate::c::{Max, Min};

// ---------------------------------------------------------------------------
// Relation type (local alias, *mut RelationData)
// ---------------------------------------------------------------------------
type Relation = *mut RelationData;

// ---------------------------------------------------------------------------
// Pg_database attribute number constants (catalog/pg_database_d.h)
// TODO(pg-port): real Anum_pg_database_* live in catalog/pg_database_d.h (genbki output)
// ---------------------------------------------------------------------------
const Anum_pg_database_datname: i16 = 2;
const Anum_pg_database_oid: i16 = 1;
const Anum_pg_database_datcollate: i16 = 13;
const Anum_pg_database_datctype: i16 = 14;
const Anum_pg_database_datcollversion: i16 = 17;
const Anum_pg_database_datlocale: i16 = 15;

// pg_database index OIDs (catalog/catalog.rs already has them; reuse literals
// to avoid a cross-module const reference that would require pub visibility there)
const DatabaseNameIndexId: Oid = 2671;
const DatabaseOidIndexId: Oid = 2672;

// fmgr regproc OIDs for built-in comparison functions
const F_NAMEEQ: RegProcedure = 62; // nameeq (name, name)
const F_OIDEQ: RegProcedure = 184; // oideq (oid, oid)

// COLLPROVIDER_LIBC ('c') from pg_collation.h
const COLLPROVIDER_LIBC: c_char = b'c' as c_char;

// ---------------------------------------------------------------------------
// GUC types / values needed here
// TODO(pg-port): real GucContext/GucSource live in utils/guc.h
// ---------------------------------------------------------------------------
type GucContext = c_int;
type GucSource = c_int;
const PGC_INTERNAL: GucContext = 0;
const PGC_BACKEND: GucContext = 4;
const PGC_SU_BACKEND: GucContext = 3;
const PGC_S_DYNAMIC_DEFAULT: GucSource = 1;
const PGC_S_CLIENT: GucSource = 9;
const PGC_S_DATABASE_USER: GucSource = 8;
const PGC_S_USER: GucSource = 7;
const PGC_S_DATABASE: GucSource = 6;
const PGC_S_GLOBAL: GucSource = 5;

// ---------------------------------------------------------------------------
// Syscache SysScanDesc
// TODO(pg-port): real SysScanDesc lives in access/relscan.rs
// ---------------------------------------------------------------------------
use crate::access::index::genam::SysScanDesc;

// ---------------------------------------------------------------------------
// Snapshot type
// TODO(pg-port): real Snapshot lives in utils/snapshot.h
// ---------------------------------------------------------------------------
type Snapshot = *mut c_void;

// ---------------------------------------------------------------------------
// TableScanDesc
// ---------------------------------------------------------------------------
use crate::access::relscan::TableScanDesc;

// ---------------------------------------------------------------------------
// NAMEDATALEN
// ---------------------------------------------------------------------------
use crate::pg_config::NAMEDATALEN;

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported symbols
// ---------------------------------------------------------------------------

// TODO(pg-port): real criticalSharedRelcachesBuilt lives in utils/cache/relcache.c
unsafe fn criticalSharedRelcachesBuilt() -> bool {
    false // TODO(pg-port): real criticalSharedRelcachesBuilt lives in utils/cache/relcache.c
}

// TODO(pg-port): real RelationCacheInitialize lives in utils/cache/relcache.c
unsafe fn RelationCacheInitialize() {
    crate::utils::cache::relcache::RelationCacheInitialize()
}

// TODO(pg-port): real RelationCacheInitializePhase2 lives in utils/cache/relcache.c
unsafe fn RelationCacheInitializePhase2() {
    crate::utils::cache::relcache::RelationCacheInitializePhase2()
}

// TODO(pg-port): real RelationCacheInitializePhase3 lives in utils/cache/relcache.c
unsafe fn RelationCacheInitializePhase3() {
    crate::utils::cache::relcache::RelationCacheInitializePhase3()
}

// TODO(pg-port): real InitCatalogCache lives in utils/cache/syscache.c
unsafe fn InitCatalogCache() {
    crate::utils::cache::syscache::InitCatalogCache()
}

// TODO(pg-port): real InitPlanCache lives in utils/cache/plancache.c
unsafe fn InitPlanCache() {
    crate::utils::cache::plancache::InitPlanCache()
}

// TODO(pg-port): real InitProcessPhase2 lives in storage/lmgr/proc.c
unsafe fn InitProcessPhase2() {
    crate::storage::lmgr::proc::InitProcessPhase2()
}

// TODO(pg-port): real StartupXLOG lives in access/transam/xlog.c
unsafe fn StartupXLOG() {
    crate::access::transam::xlog::StartupXLOG();
}

// TODO(pg-port): real CreateAuxProcessResourceOwner lives in postmaster/auxprocess.c
unsafe fn CreateAuxProcessResourceOwner() {
    crate::utils::resowner::resowner::CreateAuxProcessResourceOwner();
}

// TODO(pg-port): real ReleaseAuxProcessResources lives in postmaster/auxprocess.c
unsafe fn ReleaseAuxProcessResources(_is_commit: bool) {
    crate::utils::resowner::resowner::ReleaseAuxProcessResources(_is_commit);
}

// TODO(pg-port): real StartTransactionCommand lives in access/transam/xact.c
unsafe fn StartTransactionCommand() {
    crate::access::transam::xact::StartTransactionCommand()
}

// TODO(pg-port): real CommitTransactionCommand lives in access/transam/xact.c
unsafe fn CommitTransactionCommand() {
    crate::access::transam::xact::CommitTransactionCommand()
}

// TODO(pg-port): real AbortOutOfAnyTransaction lives in access/transam/xact.c
unsafe fn AbortOutOfAnyTransaction() {
    crate::access::transam::xact::AbortOutOfAnyTransaction()
}

// TODO(pg-port): real SetCurrentStatementStartTimestamp lives in access/transam/xact.c
unsafe fn SetCurrentStatementStartTimestamp() {
    crate::access::transam::xact::SetCurrentStatementStartTimestamp()
}

// TODO(pg-port): real XactIsoLevel lives in access/transam/xact.c
static mut XactIsoLevel: c_int = 0;
const XACT_READ_COMMITTED: c_int = 1;

// TODO(pg-port): real SharedInvalBackendInit lives in storage/ipc/sinval.c
unsafe fn SharedInvalBackendInit(_sendOnly: bool) {
    crate::storage::ipc::sinvaladt::SharedInvalBackendInit(_sendOnly)
}

// TODO(pg-port): real ProcSignalInit lives in storage/ipc/procsignal.c
unsafe fn ProcSignalInit(_cancel_key: *const uint8, _cancel_key_length: c_int) {
    crate::storage::ipc::procsignal::ProcSignalInit(_cancel_key, _cancel_key_length)
}

// TODO(pg-port): real CheckDeadLockAlert lives in storage/lmgr/deadlock.c
unsafe extern "C" fn CheckDeadLockAlert() {
    unimplemented!() // TODO(pg-port): real CheckDeadLockAlert lives in storage/lmgr/deadlock.c
}

// TODO(pg-port): real InvalidateCatalogSnapshot lives in utils/snapmgr.c
unsafe fn InvalidateCatalogSnapshot() {
    crate::utils::time::snapmgr::InvalidateCatalogSnapshot()
}

// TODO(pg-port): real GetCatalogSnapshot lives in utils/snapmgr.c
unsafe fn GetCatalogSnapshot(_relid: Oid) -> Snapshot {
    crate::utils::time::snapmgr::GetCatalogSnapshot(_relid) as _
}

// TODO(pg-port): real RegisterSnapshot lives in utils/snapmgr.c
unsafe fn RegisterSnapshot(_snapshot: Snapshot) -> Snapshot {
    crate::utils::time::snapmgr::RegisterSnapshot(_snapshot as _) as _
}

// TODO(pg-port): real UnregisterSnapshot lives in utils/snapmgr.c
unsafe fn UnregisterSnapshot(_snapshot: Snapshot) {
    crate::utils::time::snapmgr::UnregisterSnapshot(_snapshot as _)
}

// TODO(pg-port): real LockReleaseAll lives in storage/lmgr/lock.c
unsafe fn LockReleaseAll(_lockmethodid: u16, _allLocks: bool) {
    crate::storage::lmgr::lock::LockReleaseAll(_lockmethodid as _, _allLocks)
}

// USER_LOCKMETHOD = 2 (storage/lmgr/lock.h)
const USER_LOCKMETHOD: u16 = 2;

// TODO(pg-port): real CurrentResourceOwner lives in utils/resowner/resowner.c
static mut CurrentResourceOwner: *mut c_void = core::ptr::null_mut();

// TODO(pg-port): real pgstat_beinit lives in utils/activity/pgstat.c
unsafe fn pgstat_beinit() {
    // best-effort backend stats; no-op for bring-up
}

// TODO(pg-port): real pgstat_bestart_initial lives in utils/activity/pgstat.c
unsafe fn pgstat_bestart_initial() {
    // best-effort backend stats; no-op for bring-up
}

// TODO(pg-port): real pgstat_bestart_final lives in utils/activity/pgstat.c
unsafe fn pgstat_bestart_final() {
    // best-effort backend stats; no-op for bring-up
}

// TODO(pg-port): real pgstat_bestart_security lives in utils/activity/pgstat.c
unsafe fn pgstat_bestart_security() {
    // best-effort backend stats; no-op for bring-up
}

// TODO(pg-port): real pgstat_before_server_shutdown lives in utils/activity/pgstat.c
unsafe extern "C" fn pgstat_before_server_shutdown(_code: c_int, _arg: Datum) {
    unimplemented!() // TODO(pg-port): real pgstat_before_server_shutdown lives in utils/activity/pgstat.c
}

// TODO(pg-port): real pgstat_initialize lives in utils/activity/pgstat.c
unsafe fn pgstat_initialize() {
    crate::utils::activity::pgstat::pgstat_initialize()
}

// TODO(pg-port): real pgaio_init_backend lives in storage/aio/aio_subsys.c
unsafe fn pgaio_init_backend() {
    crate::storage::aio::aio_init::pgaio_init_backend()
}

// TODO(pg-port): real InitSync lives in storage/sync.c
unsafe fn InitSync() {
    crate::storage::sync::sync::InitSync()
}

// TODO(pg-port): real smgrinit lives in storage/smgr/smgr.c
unsafe fn smgrinit() {
    crate::storage::smgr::smgr::smgrinit()
}

// TODO(pg-port): real InitBufferManagerAccess lives in storage/buffer/bufmgr.c
unsafe fn InitBufferManagerAccess() {
    crate::storage::buffer::bufmgr::InitBufferManagerAccess()
}

// TODO(pg-port): real InitTemporaryFileAccess lives in storage/file/fd.c
unsafe fn InitTemporaryFileAccess() {
    crate::storage::file::fd::InitTemporaryFileAccess()
}

// TODO(pg-port): real InitLockManagerAccess lives in storage/lmgr/lock.c
unsafe fn InitLockManagerAccess() {
    crate::storage::lmgr::lock::InitLockManagerAccess()
}

unsafe fn ReplicationSlotInitialize() {
    crate::replication::slot::ReplicationSlotInitialize()
}

// TODO(pg-port): real DebugFileOpen lives in utils/misc/ps_status.c
unsafe fn DebugFileOpen() {}

// TODO(pg-port): real InitFileAccess lives in storage/file/fd.c
unsafe fn InitFileAccess() {
    crate::storage::file::fd::InitFileAccess()
}

// TODO(pg-port): real SetLatch lives in storage/ipc/latch.c
unsafe fn SetLatch(_latch: *mut c_void) {
    crate::storage::ipc::latch::SetLatch(_latch as _);
}

// TODO(pg-port): real initialize_acl lives in utils/acl.c
unsafe fn initialize_acl() {
    crate::utils::adt::acl::initialize_acl();
}

// TODO(pg-port): real object_aclcheck lives in utils/acl.c
unsafe fn object_aclcheck(
    classid: Oid,
    objid: Oid,
    roleid: Oid,
    mode: AclMode,
) -> AclResult {
    crate::catalog::aclchk::object_aclcheck(classid as _, objid as _, roleid as _, mode as _) as _
}

// TODO(pg-port): real AclMode/AclResult live in utils/acl.h
type AclMode = uint32;
type AclResult = c_int;
const ACL_CONNECT: AclMode = 1 << 7; // pg_aclchk.h
const ACLCHECK_OK: AclResult = 0;

// TODO(pg-port): real CountDBConnections lives in storage/lmgr/procarray.c
unsafe fn CountDBConnections(_dbid: Oid) -> c_int {
    crate::storage::ipc::procarray::CountDBConnections(_dbid as _) as _
}

// TODO(pg-port): real HaveNFreeProcs lives in storage/lmgr/procarray.c
unsafe fn HaveNFreeProcs(_n: c_int, _nfree: *mut c_int) -> bool {
    crate::storage::lmgr::proc::HaveNFreeProcs(_n as _, _nfree as _)
}

// TODO(pg-port): real has_privs_of_role lives in utils/acl.c
unsafe fn has_privs_of_role(_member: Oid, _role: Oid) -> bool {
    crate::utils::adt::acl::has_privs_of_role(_member as _, _role as _)
}

// TODO(pg-port): real superuser() lives in utils/misc/superuser.c
unsafe fn superuser() -> bool {
    crate::miscadmin::superuser()
}

// TODO(pg-port): real GetDatabasePath lives in storage/file/fd.c (or catalog/storage.c)
unsafe fn GetDatabasePath(_dbid: Oid, _tablespace: Oid) -> *mut c_char {
    crate::common::relpath::GetDatabasePath(_dbid as _, _tablespace as _) as _
}

use crate::mb::mbutils::SetDatabaseEncoding;

// TODO(pg-port): real GetDatabaseEncodingName lives in mb/pg_wchar.c
unsafe fn GetDatabaseEncodingName() -> *const c_char {
    crate::mb::mbutils::GetDatabaseEncodingName() as _
}

// TODO(pg-port): real SetConfigOption lives in utils/misc/guc.c
unsafe fn SetConfigOption(
    name: *const c_char,
    value: *const c_char,
    context: GucContext,
    source: GucSource,
) {
    crate::utils::misc::guc::SetConfigOption(name, value, core::mem::transmute(context), core::mem::transmute(source))
}

// TODO(pg-port): real pg_perm_setlocale lives in port/pg_locale.c
unsafe fn pg_perm_setlocale(_category: c_int, _locale: *const c_char) -> *mut c_char {
    crate::utils::adt::pg_locale::pg_perm_setlocale(_category as _, _locale as _) as _
}

// TODO(pg-port): real database_ctype_is_c lives in utils/adt/pg_locale.c
static mut database_ctype_is_c: bool = false;

// TODO(pg-port): real init_database_collation lives in utils/adt/pg_locale.c
unsafe fn init_database_collation() {
    crate::utils::adt::pg_locale::init_database_collation();
}

// TODO(pg-port): real get_collation_actual_version lives in utils/adt/pg_locale.c
unsafe fn get_collation_actual_version(
    provider: c_char,
    locale: *const c_char,
) -> *mut c_char {
    crate::utils::adt::pg_locale::get_collation_actual_version(provider, locale)
}

// TODO(pg-port): real TextDatumGetCString lives in utils/adt/varlena.c
unsafe fn TextDatumGetCString(d: Datum) -> *mut c_char {
    crate::utils::adt::varlena::TextDatumGetCString(d)
}

// TODO(pg-port): real quote_identifier lives in utils/adt/ruleutils.c
unsafe fn quote_identifier(_ident: *const c_char) -> *const c_char {
    unimplemented!() // TODO(pg-port): real quote_identifier lives in utils/adt/ruleutils.c
}

// TODO(pg-port): real InitializeSearchPath lives in catalog/namespace.c
unsafe fn InitializeSearchPath() {
    crate::catalog::namespace::InitializeSearchPath();
}

// TODO(pg-port): real InitializeClientEncoding lives in mb/pg_wchar.c
unsafe fn InitializeClientEncoding() {
    crate::mb::mbutils::InitializeClientEncoding();
}

// TODO(pg-port): real process_session_preload_libraries lives in utils/misc/miscinit.c
unsafe fn process_session_preload_libraries() {
    crate::miscadmin::process_session_preload_libraries();
}

// TODO(pg-port): real ApplySetting lives in catalog/pg_db_role_setting.c
unsafe fn ApplySetting(
    snapshot: Snapshot,
    databaseid: Oid,
    roleid: Oid,
    relsetting: Relation,
    source: GucSource,
) {
    crate::catalog::pg_db_role_setting::ApplySetting(snapshot as _, databaseid, roleid, relsetting as _, core::mem::transmute(source))
}

// TODO(pg-port): real process_postgres_switches lives in tcop/postgres.c
unsafe fn process_postgres_switches(
    _argc: c_int,
    _argv: *mut *mut c_char,
    _ctx: GucContext,
    _dbname: *mut *const c_char,
) -> c_int {
    crate::tcop::postgres::process_postgres_switches(_argc as _, _argv as _, _ctx as _, _dbname as _);
    0
}

// TODO(pg-port): real set_ps_display lives in utils/misc/ps_status.c
unsafe fn set_ps_display(_activity: *const c_char) {
    crate::utils::misc::ps_status::set_ps_display(_activity as _);
}

// TODO(pg-port): real ClientAuthentication lives in libpq/auth.c
unsafe fn ClientAuthentication(_port: *mut c_void) {
    crate::libpq::auth::ClientAuthentication(_port as _);
}

// TODO(pg-port): real GetCurrentTimestamp lives in utils/adt/timestamp.c
use crate::utils::init::globals::TimestampTz;
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    crate::utils::adt::timestamp::GetCurrentTimestamp()
}

// TODO(pg-port): real database_is_invalid_form lives in commands/dbcommands.c
unsafe fn database_is_invalid_form(_form: Form_pg_database) -> bool {
    false // TODO(pg-port): commands::dbcommands not declared; normal db is valid
}

// TODO(pg-port): real namestrcmp lives in utils/adt/name.c (stub exists in utils/builtins.rs)
unsafe fn namestrcmp(_name: *const NameData, _str: *const c_char) -> c_int {
    crate::utils::builtins::namestrcmp(_name as _, _str as _) as _
}

// TODO(pg-port): real am_walsender / am_db_walsender live in replication/walsender.c
static mut am_walsender: bool = false;
static mut am_db_walsender: bool = false;

// TODO(pg-port): real PostAuthDelay lives in utils/misc/guc.c
static mut PostAuthDelay: c_int = 0;

// TODO(pg-port): real AuthenticationTimeout lives in utils/misc/guc.c
static mut AuthenticationTimeout: c_int = 60;

// TODO(pg-port): real log_connections / LOG_CONNECTION_AUTHORIZATION live in
//   libpq/be-secure.c / utils/misc/guc.c
static mut log_connections: c_int = 0;
const LOG_CONNECTION_AUTHORIZATION: c_int = 1 << 1;

// TODO(pg-port): conn_timing structure lives in tcop/backend_startup.c
// Modelling as an opaque struct with the two used fields.
#[repr(C)]
struct ConnTimingData {
    auth_start: TimestampTz,
    auth_end: TimestampTz,
}
static mut conn_timing: ConnTimingData = ConnTimingData {
    auth_start: 0,
    auth_end: 0,
};

// ClientAuthInProgress is the process-global flag (defined in backend_link_shims);
// declare it extern so the reset here clears the same symbol elog reads.
extern "C" {
    static mut ClientAuthInProgress: bool;
}

// TODO(pg-port): real MyClientConnectionInfo / auth_method / authn_id live in
//   libpq/libpq-be.h; using opaque pointer here
#[repr(C)]
struct ClientConnectionInfo {
    authn_id: *const c_char,
    auth_method: c_int,
}
static mut MyClientConnectionInfo: ClientConnectionInfo = ClientConnectionInfo {
    authn_id: core::ptr::null(),
    auth_method: 0,
};

// TODO(pg-port): real hba_authname lives in libpq/hba.c
unsafe fn hba_authname(_method: c_int) -> *const c_char {
    unimplemented!() // TODO(pg-port): real hba_authname lives in libpq/hba.c
}

// TODO(pg-port): real appendStringInfo lives in lib/stringinfo.c
// Use the crate macro instead of the C variadic form; translating %s/%u as {}.
use crate::appendStringInfo;

// TODO(pg-port): real pfree lives in utils/mmgr/mcxt.c (already in prelude via palloc.rs)
// TODO(pg-port): real pstrdup lives in utils/mmgr/mcxt.c (already in prelude)
// TODO(pg-port): real palloc lives in utils/mmgr/mcxt.c (already in prelude)

// Template1DbOid (catalog/pg_database_d.h)
const Template1DbOid: Oid = 1;

// DEFAULTTABLESPACE_OID (catalog/pg_tablespace_d.h)
const DEFAULTTABLESPACE_OID: Oid = 1663;

// MAX_BACKENDS (storage/procnumber.h) - reuse from the ported module
use crate::storage::procnumber::MAX_BACKENDS;

// GUC variables needed by InitializeMaxBackends
use crate::utils::init::globals::{max_worker_processes, MaxBackends, MaxConnections};
// TODO(pg-port): real autovacuum_worker_slots lives in autovacuum.c GUC
static mut autovacuum_worker_slots: c_int = 3;
// TODO(pg-port): real max_wal_senders lives in walsender.c GUC
static mut max_wal_senders: c_int = 10;
// TODO(pg-port): real NUM_SPECIAL_WORKER_PROCS lives in storage/lmgr/proc.h
unsafe fn num_special_worker_procs() -> c_int {
    3 // TODO(pg-port): real NUM_SPECIAL_WORKER_PROCS lives in storage/lmgr/proc.h
}

// TODO(pg-port): real FastPathLockGroupsPerBackend lives in storage/lmgr/proc.h
static mut FastPathLockGroupsPerBackend: u32 = 0;
// TODO(pg-port): real FP_LOCK_SLOTS_PER_GROUP / FP_LOCK_GROUPS_PER_BACKEND_MAX live in
//   storage/lmgr/proc.h
const FP_LOCK_SLOTS_PER_GROUP: u32 = 16;
const FP_LOCK_GROUPS_PER_BACKEND_MAX: u32 = 64;

// TODO(pg-port): real max_locks_per_xact lives in utils/misc/guc.c
static mut max_locks_per_xact: c_int = 64;

// TODO(pg-port): real SuperuserReservedConnections / ReservedConnections live in
//   utils/misc/guc.c
static mut SuperuserReservedConnections: c_int = 3;
static mut ReservedConnections: c_int = 0;

// ROLE_PG_USE_RESERVED_CONNECTIONS from catalog/pg_authid_d.h
use crate::catalog::pg_known_oids::ROLE_PG_USE_RESERVED_CONNECTIONS;

// TODO(pg-port): real MyProc lives in storage/lmgr/proc.h / proc.c
//   Using an opaque extern pointer.
extern "C" {
    static mut MyProc: *mut PgProcStub;
}
// MyProcPort, MyLatch, MyCancelKey, MyCancelKeyLength come from globals.rs.

// TODO(pg-port): real PGPROC structure lives in storage/lmgr/proc.h
#[repr(C)]
struct PgProcStub {
    databaseId: Oid,
}

// TODO(pg-port): real INJECTION_POINT lives in utils/injection_point.c
// Omit - calls are dropped as the feature is a debug instrumentation hook.

// libc access(2) / errno
extern "C" {
    fn access(path: *const c_char, mode: c_int) -> c_int;
}
const F_OK: c_int = 0;

// errno access
unsafe fn get_errno() -> c_int {
    *libc_errno_location()
}
extern "C" {
    #[cfg_attr(target_os = "linux", link_name = "__errno_location")]
    #[cfg_attr(target_os = "macos", link_name = "__error")]
    fn libc_errno_location() -> *mut c_int;
}
const ENOENT: c_int = 2;

// libc strcmp
extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

// libc strcpy
extern "C" {
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
}

// table_open/table_close
use crate::access::table::table::{table_close, table_open};

// heap_getnext / heap_copytuple
// TODO(pg-port): real heap_getnext lives in access/heap/heapam.c (unwired).
unsafe fn heap_getnext(_scan: *mut c_void, _dir: c_int) -> HeapTuple { core::ptr::null_mut() }
use crate::access::common::heaptuple::heap_copytuple;

// table_beginscan_catalog
use crate::access::table::tableam::table_beginscan_catalog;

// TODO(pg-port): real table_endscan lives in access/tableam.h (inline via scan_end vtable)
unsafe fn table_endscan(_scan: TableScanDesc) {
    crate::access::table::tableam::table_endscan(_scan as _)
}

// pg_nextpower2_32 for InitializeFastPathLocks
use crate::port::pg_bitutils::pg_nextpower2_32;

// Max/Min helpers from prelude
// Max/Min come from crate::c via the import above

// LC_COLLATE / LC_CTYPE (libc constants)
const LC_COLLATE: c_int = libc::LC_COLLATE;
const LC_CTYPE: c_int = libc::LC_CTYPE;

/*** InitPostgres support ***/


/*
 * GetDatabaseTuple -- fetch the pg_database row for a database
 *
 * This is used during backend startup when we don't yet have any access to
 * system catalogs in general.  In the worst case, we can seqscan pg_database
 * using nothing but the hard-wired descriptor that relcache.c creates for
 * pg_database.  In more typical cases, relcache.c was able to load
 * descriptors for both pg_database and its indexes from the shared relcache
 * cache file, and so we can do an indexscan.  criticalSharedRelcachesBuilt
 * tells whether we got the cached descriptors.
 */
unsafe fn GetDatabaseTuple(dbname: *const c_char) -> HeapTuple {
    let tuple: HeapTuple;
    let relation: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 1] = core::mem::zeroed();

    /*
     * form a scan key
     */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_database_datname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(dbname),
    );

    /*
     * Open pg_database and fetch a tuple.  Force heap scan if we haven't yet
     * built the critical shared relcache entries (i.e., we're starting up
     * without a shared relcache cache file).
     */
    relation = table_open(DatabaseRelationId, AccessShareLock);
    scan = systable_beginscan(
        relation,
        DatabaseNameIndexId,
        criticalSharedRelcachesBuilt(),
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );

    tuple = systable_getnext(scan) as HeapTuple;

    /* Must copy tuple before releasing buffer */
    let tuple = if HeapTupleIsValid(tuple) {
        heap_copytuple(tuple)
    } else {
        tuple
    };

    /* all done */
    systable_endscan(scan);
    table_close(relation, AccessShareLock);

    tuple
}

/*
 * GetDatabaseTupleByOid -- as above, but search by database OID
 */
unsafe fn GetDatabaseTupleByOid(dboid: Oid) -> HeapTuple {
    let tuple: HeapTuple;
    let relation: Relation;
    let scan: SysScanDesc;
    let mut key: [ScanKeyData; 1] = core::mem::zeroed();

    /*
     * form a scan key
     */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_database_oid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(dboid),
    );

    /*
     * Open pg_database and fetch a tuple.  Force heap scan if we haven't yet
     * built the critical shared relcache entries (i.e., we're starting up
     * without a shared relcache cache file).
     */
    relation = table_open(DatabaseRelationId, AccessShareLock);
    scan = systable_beginscan(
        relation,
        DatabaseOidIndexId,
        criticalSharedRelcachesBuilt(),
        core::ptr::null_mut(),
        1,
        key.as_mut_ptr(),
    );

    tuple = systable_getnext(scan) as HeapTuple;

    /* Must copy tuple before releasing buffer */
    let tuple = if HeapTupleIsValid(tuple) {
        heap_copytuple(tuple)
    } else {
        tuple
    };

    /* all done */
    systable_endscan(scan);
    table_close(relation, AccessShareLock);

    tuple
}


/*
 * PerformAuthentication -- authenticate a remote client
 *
 * returns: nothing.  Will not return at all if there's any failure.
 */
// EXEC_BACKEND section omitted -- not compiled in this port (no EXEC_BACKEND define).
unsafe fn PerformAuthentication(port: *mut c_void) {
    /* This should be set already, but let's make sure */
    ClientAuthInProgress = true; /* limit visibility of log messages */

    /* Capture authentication start time for logging */
    conn_timing.auth_start = GetCurrentTimestamp();

    /*
     * Set up a timeout in case a buggy or malicious client fails to respond
     * during authentication.  Since we're inside a transaction and might do
     * database access, we have to use the statement_timeout infrastructure.
     */
    enable_timeout_after(STATEMENT_TIMEOUT, AuthenticationTimeout * 1000);

    /*
     * Now perform authentication exchange.
     */
    set_ps_display(c"authentication".as_ptr());
    ClientAuthentication(port); /* might not return, if failure */

    /*
     * Done with authentication.  Disable the timeout, and log if needed.
     */
    disable_timeout(STATEMENT_TIMEOUT, false);

    /* Capture authentication end time for logging */
    conn_timing.auth_end = GetCurrentTimestamp();

    if (log_connections & LOG_CONNECTION_AUTHORIZATION) != 0 {
        let mut logmsg: StringInfoData = core::mem::zeroed();
        let port_user_name: *const c_char = (*(port as *mut PortStub)).user_name;
        let port_database_name: *const c_char = (*(port as *mut PortStub)).database_name;
        let port_application_name: *const c_char = (*(port as *mut PortStub)).application_name;

        initStringInfo(&mut logmsg);
        if am_walsender {
            appendStringInfo!(
                &mut logmsg,
                "replication connection authorized: user={}",
                core::ffi::CStr::from_ptr(port_user_name).to_string_lossy()
            );
        } else {
            appendStringInfo!(
                &mut logmsg,
                "connection authorized: user={}",
                core::ffi::CStr::from_ptr(port_user_name).to_string_lossy()
            );
        }
        if !am_walsender {
            appendStringInfo!(
                &mut logmsg,
                " database={}",
                core::ffi::CStr::from_ptr(port_database_name).to_string_lossy()
            );
        }

        if !port_application_name.is_null() {
            appendStringInfo!(
                &mut logmsg,
                " application_name={}",
                core::ffi::CStr::from_ptr(port_application_name).to_string_lossy()
            );
        }

        // USE_SSL gated block omitted -- translate the non-SSL fallback (nothing to emit).
        // ENABLE_GSS gated block omitted -- translate the non-GSS fallback (nothing to emit).

        ereport!(LOG, errmsg!("{}", core::ffi::CStr::from_ptr(logmsg.data).to_string_lossy()));
        pfree(logmsg.data as *mut c_void);
    }

    set_ps_display(c"startup".as_ptr());

    ClientAuthInProgress = false; /* client_min_messages is active now */
}

// TODO(pg-port): Port struct (libpq-be.h) -- opaque stub for the fields used above.
#[repr(C)]
struct PortStub {
    user_name: *const c_char,
    database_name: *const c_char,
    application_name: *const c_char,
    cmdline_options: *const c_char,
    guc_options: *mut c_void, /* List* */
}


/*
 * CheckMyDatabase -- fetch information from the pg_database entry for our DB
 */
unsafe fn CheckMyDatabase(
    name: *const c_char,
    am_superuser: bool,
    override_allow_connections: bool,
) {
    let tup: HeapTuple;
    let dbform: Form_pg_database;
    let mut datum: Datum;
    let mut isnull: bool = false;
    let collate: *mut c_char;
    let ctype: *mut c_char;

    /* Fetch our pg_database row normally, via syscache */
    tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for database {}", MyDatabaseId);
    }
    dbform = GETSTRUCT(tup) as Form_pg_database;

    /* This recheck is strictly paranoia */
    if strcmp(name, crate::c::NameStr(&(*dbform).datname)) != 0 {
        ereport!(
            FATAL,
            errmsg!(
                "database \"{}\" has disappeared from pg_database",
                core::ffi::CStr::from_ptr(name).to_string_lossy()
            )
        );
    }

    /*
     * Check permissions to connect to the database.
     *
     * These checks are not enforced when in standalone mode, so that there is
     * a way to recover from disabling all access to all databases, for
     * example "UPDATE pg_database SET datallowconn = false;".
     */
    if IsUnderPostmaster {
        /*
         * Check that the database is currently allowing connections.
         * (Background processes can override this test and the next one by
         * setting override_allow_connections.)
         */
        if !(*dbform).datallowconn && !override_allow_connections {
            ereport!(
                FATAL,
                errmsg!(
                    "database \"{}\" is not currently accepting connections",
                    core::ffi::CStr::from_ptr(name).to_string_lossy()
                )
            );
        }

        /*
         * Check privilege to connect to the database.  (The am_superuser test
         * is redundant, but since we have the flag, might as well check it
         * and save a few cycles.)
         */
        if !am_superuser
            && !override_allow_connections
            && object_aclcheck(DatabaseRelationId, MyDatabaseId, GetUserId(), ACL_CONNECT)
                != ACLCHECK_OK
        {
            ereport!(
                FATAL,
                errmsg!(
                    "permission denied for database \"{}\"",
                    core::ffi::CStr::from_ptr(name).to_string_lossy()
                )
            );
        }

        /*
         * Check connection limit for this database.  We enforce the limit
         * only for regular backends, since other process types have their own
         * PGPROC pools.
         *
         * There is a race condition here --- we create our PGPROC before
         * checking for other PGPROCs.  If two backends did this at about the
         * same time, they might both think they were over the limit, while
         * ideally one should succeed and one fail.  Getting that to work
         * exactly seems more trouble than it is worth, however; instead we
         * just document that the connection limit is approximate.
         */
        if (*dbform).datconnlimit >= 0
            && AmRegularBackendProcess()
            && !am_superuser
            && CountDBConnections(MyDatabaseId) > (*dbform).datconnlimit
        {
            ereport!(
                FATAL,
                errmsg!(
                    "too many connections for database \"{}\"",
                    core::ffi::CStr::from_ptr(name).to_string_lossy()
                )
            );
        }
    }

    /*
     * OK, we're golden.  Next to-do item is to save the encoding info out of
     * the pg_database tuple.
     */
    SetDatabaseEncoding((*dbform).encoding);
    /* Record it as a GUC internal option, too */
    SetConfigOption(
        c"server_encoding".as_ptr(),
        GetDatabaseEncodingName(),
        PGC_INTERNAL,
        PGC_S_DYNAMIC_DEFAULT,
    );
    /* If we have no other source of client_encoding, use server encoding */
    SetConfigOption(
        c"client_encoding".as_ptr(),
        GetDatabaseEncodingName(),
        PGC_BACKEND,
        PGC_S_DYNAMIC_DEFAULT,
    );

    /* assign locale variables */
    datum = SysCacheGetAttrNotNull(DATABASEOID, tup, Anum_pg_database_datcollate);
    collate = TextDatumGetCString(datum);
    datum = SysCacheGetAttrNotNull(DATABASEOID, tup, Anum_pg_database_datctype);
    ctype = TextDatumGetCString(datum);

    if pg_perm_setlocale(LC_COLLATE, collate).is_null() {
        ereport!(
            FATAL,
            errmsg!("database locale is incompatible with operating system")
        );
    }

    if pg_perm_setlocale(LC_CTYPE, ctype).is_null() {
        ereport!(
            FATAL,
            errmsg!("database locale is incompatible with operating system")
        );
    }

    if strcmp(ctype, c"C".as_ptr()) == 0 || strcmp(ctype, c"POSIX".as_ptr()) == 0 {
        database_ctype_is_c = true;
    }

    init_database_collation();

    /*
     * Check collation version.  See similar code in
     * pg_newlocale_from_collation().  Note that here we warn instead of error
     * in any case, so that we don't prevent connecting.
     */
    datum = SysCacheGetAttr(
        DATABASEOID,
        tup,
        Anum_pg_database_datcollversion,
        &mut isnull,
    );
    if !isnull {
        let actual_versionstr: *mut c_char;
        let collversionstr: *mut c_char;
        let locale: *mut c_char;

        collversionstr = TextDatumGetCString(datum);

        if (*dbform).datlocprovider == COLLPROVIDER_LIBC {
            locale = collate;
        } else {
            datum = SysCacheGetAttrNotNull(
                DATABASEOID,
                tup,
                Anum_pg_database_datlocale,
            );
            locale = TextDatumGetCString(datum);
        }

        actual_versionstr =
            get_collation_actual_version((*dbform).datlocprovider, locale);
        if actual_versionstr.is_null() {
            /* should not happen */
            elog!(
                crate::utils::elog::WARNING,
                "database \"{}\" has no actual collation version, but a version was recorded",
                core::ffi::CStr::from_ptr(name).to_string_lossy()
            );
        } else if strcmp(actual_versionstr, collversionstr) != 0 {
            ereport!(
                WARNING,
                errmsg!(
                    "database \"{}\" has a collation version mismatch",
                    core::ffi::CStr::from_ptr(name).to_string_lossy()
                )
            );
        }
    }

    ReleaseSysCache(tup);
}


/*
 * pg_split_opts -- split a string of options and append it to an argv array
 *
 * The caller is responsible for ensuring the argv array is large enough.  The
 * maximum possible number of arguments added by this routine is
 * (strlen(optstr) + 1) / 2.
 *
 * Because some option values can contain spaces we allow escaping using
 * backslashes, with \\ representing a literal backslash.
 */
pub unsafe fn pg_split_opts(argv: *mut *mut c_char, argcp: *mut c_int, optstr: *const c_char) {
    use crate::lib::stringinfo::{appendStringInfoChar, resetStringInfo};

    let mut s: crate::lib::stringinfo::StringInfoData = core::mem::zeroed();
    let mut cur = optstr;

    initStringInfo(&mut s);

    while *cur != 0 {
        let mut last_was_escape = false;

        resetStringInfo(&mut s);

        /* skip over leading space */
        while *cur != 0 && (*cur as u8).is_ascii_whitespace() {
            cur = cur.add(1);
        }

        if *cur == 0 {
            break;
        }

        /*
         * Parse a single option, stopping at the first space, unless it's
         * escaped.
         */
        while *cur != 0 {
            if (*cur as u8).is_ascii_whitespace() && !last_was_escape {
                break;
            }

            if !last_was_escape && *cur == b'\\' as c_char {
                last_was_escape = true;
            } else {
                last_was_escape = false;
                appendStringInfoChar(&mut s, *cur);
            }

            cur = cur.add(1);
        }

        /* now store the option in the next argv[] position */
        *argv.add(*argcp as usize) = pstrdup(s.data);
        *argcp += 1;
    }

    pfree(s.data as *mut c_void);
}

/*
 * Initialize MaxBackends value from config options.
 *
 * This must be called after modules have had the chance to alter GUCs in
 * shared_preload_libraries and before shared memory size is determined.
 *
 * Note that in EXEC_BACKEND environment, the value is passed down from
 * postmaster to subprocesses via BackendParameters in SubPostmasterMain; only
 * postmaster itself and processes not under postmaster control should call
 * this.
 */
pub unsafe fn InitializeMaxBackends() {
    Assert!(MaxBackends == 0);

    /* Note that this does not include "auxiliary" processes */
    MaxBackends = MaxConnections
        + autovacuum_worker_slots
        + max_worker_processes
        + max_wal_senders
        + num_special_worker_procs();

    if MaxBackends as uint32 > MAX_BACKENDS {
        ereport!(
            ERROR,
            errmsg!(
                "too many server processes configured"
            )
        );
    }
}

/*
 * Initialize the number of fast-path lock slots in PGPROC.
 *
 * This must be called after modules have had the chance to alter GUCs in
 * shared_preload_libraries and before shared memory size is determined.
 */
pub unsafe fn InitializeFastPathLocks() {
    /* Should be initialized only once. */
    Assert!(FastPathLockGroupsPerBackend == 0);

    /*
     * Based on the max_locks_per_transaction GUC, as that's a good indicator
     * of the expected number of locks, figure out the value for
     * FastPathLockGroupsPerBackend.  This must be a power-of-two.  We cap the
     * value at FP_LOCK_GROUPS_PER_BACKEND_MAX and insist the value is at
     * least 1.
     *
     * The default max_locks_per_transaction = 64 means 4 groups by default.
     */
    FastPathLockGroupsPerBackend = Max(
        Min(
            pg_nextpower2_32(max_locks_per_xact as u32) / FP_LOCK_SLOTS_PER_GROUP,
            FP_LOCK_GROUPS_PER_BACKEND_MAX,
        ),
        1,
    );

    /* Validate we did get a power-of-two */
    Assert!(FastPathLockGroupsPerBackend == pg_nextpower2_32(FastPathLockGroupsPerBackend));
}

/*
 * Early initialization of a backend (either standalone or under postmaster).
 * This happens even before InitPostgres.
 *
 * This is separate from InitPostgres because it is also called by auxiliary
 * processes, such as the background writer process, which may not call
 * InitPostgres at all.
 */
pub unsafe fn BaseInit() {
    Assert!(!MyProc.is_null());

    /*
     * Initialize our input/output/debugging file descriptors.
     */
    DebugFileOpen();

    /*
     * Initialize file access. Done early so other subsystems can access
     * files.
     */
    InitFileAccess();

    /*
     * Initialize statistics reporting. This needs to happen early to ensure
     * that pgstat's shutdown callback runs after the shutdown callbacks of
     * all subsystems that can produce stats (like e.g. transaction commits
     * can).
     */
    pgstat_initialize();

    /*
     * Initialize AIO before infrastructure that might need to actually
     * execute AIO.
     */
    pgaio_init_backend();

    /* Do local initialization of storage and buffer managers */
    InitSync();
    smgrinit();
    InitBufferManagerAccess();

    /*
     * Initialize temporary file access after pgstat, so that the temporary
     * file shutdown hook can report temporary file statistics.
     */
    InitTemporaryFileAccess();

    /*
     * Initialize local buffers for WAL record construction, in case we ever
     * try to insert XLOG.
     */
    InitXLogInsert();

    /* Initialize lock manager's local structs */
    InitLockManagerAccess();

    /*
     * Initialize replication slots after pgstat. The exit hook might need to
     * drop ephemeral slots, which in turn triggers stats reporting.
     */
    ReplicationSlotInitialize();
}


/* --------------------------------
 * InitPostgres
 *		Initialize POSTGRES.
 *
 * Parameters:
 *	in_dbname, dboid: specify database to connect to, as described below
 *	username, useroid: specify role to connect as, as described below
 *	flags:
 *	  - INIT_PG_LOAD_SESSION_LIBS to honor [session|local]_preload_libraries.
 *	  - INIT_PG_OVERRIDE_ALLOW_CONNS to connect despite !datallowconn.
 *	  - INIT_PG_OVERRIDE_ROLE_LOGIN to connect despite !rolcanlogin.
 *	out_dbname: optional output parameter, see below; pass NULL if not used
 *
 * The database can be specified by name, using the in_dbname parameter, or by
 * OID, using the dboid parameter.  Specify NULL or InvalidOid respectively
 * for the unused parameter.  If dboid is provided, the actual database
 * name can be returned to the caller in out_dbname.  If out_dbname isn't
 * NULL, it must point to a buffer of size NAMEDATALEN.
 *
 * Similarly, the role can be passed by name, using the username parameter,
 * or by OID using the useroid parameter.
 *
 * In bootstrap mode the database and username parameters are NULL/InvalidOid.
 * The autovacuum launcher process doesn't specify these parameters either,
 * because it only goes far enough to be able to read pg_database; it doesn't
 * connect to any particular database.  An autovacuum worker specifies a
 * database but not a username; conversely, a physical walsender specifies
 * username but not database.
 *
 * By convention, INIT_PG_LOAD_SESSION_LIBS should be passed in "flags" in
 * "interactive" sessions (including standalone backends), but not in
 * background processes such as autovacuum.  Note in particular that it
 * shouldn't be true in parallel worker processes; those have another
 * mechanism for replicating their leader's set of loaded libraries.
 *
 * We expect that InitProcess() was already called, so we already have a
 * PGPROC struct ... but it's not completely filled in yet.
 *
 * Note:
 *		Be very careful with the order of calls in the InitPostgres function.
 * --------------------------------
 */
pub unsafe fn InitPostgres(
    in_dbname: *const c_char,
    mut dboid: Oid,
    username: *const c_char,
    useroid: Oid,
    flags: bits32,
    out_dbname: *mut c_char,
) {
    let bootstrap: bool = IsBootstrapProcessingMode();
    let am_superuser: bool;
    let fullpath: *mut c_char;
    let mut dbname: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let mut nfree: c_int = 0;

    elog!(DEBUG3, "InitPostgres");

    /*
     * Add my PGPROC struct to the ProcArray.
     *
     * Once I have done this, I am visible to other backends!
     */
    InitProcessPhase2();

    /* Initialize status reporting */
    pgstat_beinit();

    /*
     * And initialize an entry in the PgBackendStatus array.  That way, if
     * LWLocks or third-party authentication should happen to hang, it is
     * possible to retrieve some information about what is going on.
     */
    if !bootstrap {
        pgstat_bestart_initial();
        // INJECTION_POINT("init-pre-auth", NULL) omitted -- debug instrumentation.
    }

    /*
     * Initialize my entry in the shared-invalidation manager's array of
     * per-backend data.
     */
    SharedInvalBackendInit(false);

    ProcSignalInit(MyCancelKey.as_ptr(), MyCancelKeyLength);

    /*
     * Also set up timeout handlers needed for backend operation.  We need
     * these in every case except bootstrap.
     */
    if !bootstrap {
        RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);
        RegisterTimeout(STATEMENT_TIMEOUT, StatementTimeoutHandler);
        RegisterTimeout(LOCK_TIMEOUT, LockTimeoutHandler);
        RegisterTimeout(
            IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
            IdleInTransactionSessionTimeoutHandler,
        );
        RegisterTimeout(TRANSACTION_TIMEOUT, TransactionTimeoutHandler);
        RegisterTimeout(IDLE_SESSION_TIMEOUT, IdleSessionTimeoutHandler);
        RegisterTimeout(CLIENT_CONNECTION_CHECK_TIMEOUT, ClientCheckTimeoutHandler);
        RegisterTimeout(IDLE_STATS_UPDATE_TIMEOUT, IdleStatsUpdateTimeoutHandler);
    }

    /*
     * If this is either a bootstrap process or a standalone backend, start up
     * the XLOG machinery, and register to have it closed down at exit. In
     * other cases, the startup process is responsible for starting up the
     * XLOG machinery, and the checkpointer for closing it down.
     */
    if !IsUnderPostmaster {
        /*
         * We don't yet have an aux-process resource owner, but StartupXLOG
         * and ShutdownXLOG will need one.  Hence, create said resource owner
         * (and register a callback to clean it up after ShutdownXLOG runs).
         */
        CreateAuxProcessResourceOwner();

        StartupXLOG();
        /* Release (and warn about) any buffer pins leaked in StartupXLOG */
        ReleaseAuxProcessResources(true);
        /* Reset CurrentResourceOwner to nothing for the moment */
        CurrentResourceOwner = core::ptr::null_mut();

        /*
         * Use before_shmem_exit() so that ShutdownXLOG() can rely on DSM
         * segments etc to work (which in turn is required for pgstats).
         */
        before_shmem_exit(pgstat_before_server_shutdown, 0);
        before_shmem_exit(ShutdownXLOG_cb, 0);
    }

    /*
     * Initialize the relation cache and the system catalog caches.  Note that
     * no catalog access happens here; we only set up the hashtable structure.
     * We must do this before starting a transaction because transaction abort
     * would try to touch these hashtables.
     */
    RelationCacheInitialize();
    InitCatalogCache();
    InitPlanCache();

    /* Initialize portal manager */
    EnablePortalManager();

    /*
     * Load relcache entries for the shared system catalogs.  This must create
     * at least entries for pg_database and catalogs used for authentication.
     */
    RelationCacheInitializePhase2();

    /*
     * Set up process-exit callback to do pre-shutdown cleanup.  This is the
     * one of the first before_shmem_exit callbacks we register; thus, this
     * will be one the last things we do before low-level modules like the
     * buffer manager begin to close down.  We need to have this in place
     * before we begin our first transaction --- if we fail during the
     * initialization transaction, as is entirely possible, we need the
     * AbortTransaction call to clean up.
     */
    before_shmem_exit(ShutdownPostgres, 0);

    /* The autovacuum launcher is done here */
    if AmAutoVacuumLauncherProcess() {
        /* fill in the remainder of this entry in the PgBackendStatus array */
        pgstat_bestart_final();

        return;
    }

    /*
     * Start a new transaction here before first access to db.
     */
    if !bootstrap {
        /* statement_timestamp must be set for timeouts to work correctly */
        SetCurrentStatementStartTimestamp();
        StartTransactionCommand();

        /*
         * transaction_isolation will have been set to the default by the
         * above.  If the default is "serializable", and we are in hot
         * standby, we will fail if we don't change it to something lower.
         * Fortunately, "read committed" is plenty good enough.
         */
        XactIsoLevel = XACT_READ_COMMITTED;
    }

    /*
     * Perform client authentication if necessary, then figure out our
     * postgres user ID, and see if we are a superuser.
     *
     * In standalone mode, autovacuum worker processes and slot sync worker
     * process, we use a fixed ID, otherwise we figure it out from the
     * authenticated user name.
     */
    if bootstrap || AmAutoVacuumWorkerProcess() || AmLogicalSlotSyncWorkerProcess() {
        InitializeSessionUserIdStandalone();
        am_superuser = true;
    } else if !IsUnderPostmaster {
        InitializeSessionUserIdStandalone();
        am_superuser = true;
        if !ThereIsAtLeastOneRole() {
            ereport!(
                WARNING,
                errmsg!(
                    "no roles are defined in this database system"
                )
            );
        }
    } else if AmBackgroundWorkerProcess() {
        if username.is_null() && !OidIsValid(useroid) {
            InitializeSessionUserIdStandalone();
            am_superuser = true;
        } else {
            InitializeSessionUserId(
                username,
                useroid,
                (flags & INIT_PG_OVERRIDE_ROLE_LOGIN as bits32) != 0,
            );
            am_superuser = superuser();
        }
    } else {
        /* normal multiuser case */
        Assert!(!MyProcPort.is_null());
        PerformAuthentication(MyProcPort);
        InitializeSessionUserId(username, useroid, false);
        /* ensure that auth_method is actually valid, aka authn_id is not NULL */
        if !MyClientConnectionInfo.authn_id.is_null() {
            InitializeSystemUser(
                MyClientConnectionInfo.authn_id,
                hba_authname(MyClientConnectionInfo.auth_method),
            );
        }
        am_superuser = superuser();
    }

    /* Report any SSL/GSS details for the session. */
    if !MyProcPort.is_null() {
        Assert!(!bootstrap);

        pgstat_bestart_security();
    }

    /*
     * Binary upgrades only allowed super-user connections
     */
    if IsBinaryUpgrade && !am_superuser {
        ereport!(
            FATAL,
            errmsg!("must be superuser to connect in binary upgrade mode")
        );
    }

    /*
     * The last few regular connection slots are reserved for superusers and
     * roles with privileges of pg_use_reserved_connections.  We do not apply
     * these limits to background processes, since they all have their own
     * pools of PGPROC slots.
     *
     * Note: At this point, the new backend has already claimed a proc struct,
     * so we must check whether the number of free slots is strictly less than
     * the reserved connection limits.
     */
    if AmRegularBackendProcess()
        && !am_superuser
        && (SuperuserReservedConnections + ReservedConnections) > 0
        && !HaveNFreeProcs(
            SuperuserReservedConnections + ReservedConnections,
            &mut nfree,
        )
    {
        if nfree < SuperuserReservedConnections {
            ereport!(
                FATAL,
                errmsg!(
                    "remaining connection slots are reserved for roles with the {} attribute",
                    "SUPERUSER"
                )
            );
        }

        if !has_privs_of_role(GetUserId(), ROLE_PG_USE_RESERVED_CONNECTIONS) {
            ereport!(
                FATAL,
                errmsg!(
                    "remaining connection slots are reserved for roles with privileges of the \"{}\" role",
                    "pg_use_reserved_connections"
                )
            );
        }
    }

    /* Check replication permissions needed for walsender processes. */
    if am_walsender {
        Assert!(!bootstrap);

        if !has_rolreplication(GetUserId()) {
            ereport!(
                FATAL,
                errmsg!("permission denied to start WAL sender")
            );
        }
    }

    /*
     * If this is a plain walsender only supporting physical replication, we
     * don't want to connect to any particular database. Just finish the
     * backend startup by processing any options from the startup packet, and
     * we're done.
     */
    if am_walsender && !am_db_walsender {
        /* process any options passed in the startup packet */
        if !MyProcPort.is_null() {
            process_startup_options(MyProcPort, am_superuser);
        }

        /* Apply PostAuthDelay as soon as we've read all options */
        if PostAuthDelay > 0 {
            pg_usleep(PostAuthDelay as c_long * 1_000_000);
        }

        /* initialize client encoding */
        InitializeClientEncoding();

        /* fill in the remainder of this entry in the PgBackendStatus array */
        pgstat_bestart_final();

        /* close the transaction we started above */
        CommitTransactionCommand();

        return;
    }

    /*
     * Set up the global variables holding database id and default tablespace.
     * But note we won't actually try to touch the database just yet.
     *
     * We take a shortcut in the bootstrap case, otherwise we have to look up
     * the db's entry in pg_database.
     */
    if bootstrap {
        dboid = Template1DbOid;
        MyDatabaseTableSpace = DEFAULTTABLESPACE_OID;
    } else if !in_dbname.is_null() {
        let tuple: HeapTuple;
        let dbform: Form_pg_database;

        tuple = GetDatabaseTuple(in_dbname);
        if !HeapTupleIsValid(tuple) {
            ereport!(
                FATAL,
                errmsg!(
                    "database \"{}\" does not exist",
                    core::ffi::CStr::from_ptr(in_dbname).to_string_lossy()
                )
            );
        }
        dbform = GETSTRUCT(tuple) as Form_pg_database;
        dboid = (*dbform).oid;
    } else if !OidIsValid(dboid) {
        /*
         * If this is a background worker not bound to any particular
         * database, we're done now.  Everything that follows only makes sense
         * if we are bound to a specific database.  We do need to close the
         * transaction we started before returning.
         */
        if !bootstrap {
            pgstat_bestart_final();
            CommitTransactionCommand();
        }
        return;
    }

    /*
     * Now, take a writer's lock on the database we are trying to connect to.
     * If there is a concurrently running DROP DATABASE on that database, this
     * will block us until it finishes (and has committed its update of
     * pg_database).
     *
     * Note that the lock is not held long, only until the end of this startup
     * transaction.  This is OK since we will advertise our use of the
     * database in the ProcArray before dropping the lock (in fact, that's the
     * next thing to do).  Anyone trying a DROP DATABASE after this point will
     * see us in the array once they have the lock.  Ordering is important for
     * this because we don't want to advertise ourselves as being in this
     * database until we have the lock; otherwise we create what amounts to a
     * deadlock with CountOtherDBBackends().
     *
     * Note: use of RowExclusiveLock here is reasonable because we envision
     * our session as being a concurrent writer of the database.  If we had a
     * way of declaring a session as being guaranteed-read-only, we could use
     * AccessShareLock for such sessions and thereby not conflict against
     * CREATE DATABASE.
     */
    if !bootstrap {
        LockSharedObject(DatabaseRelationId, dboid, 0u16, RowExclusiveLock);
    }

    /*
     * Recheck pg_database to make sure the target database hasn't gone away.
     * If there was a concurrent DROP DATABASE, this ensures we will die
     * cleanly without creating a mess.
     */
    if !bootstrap {
        let tuple: HeapTuple;
        let datform: Form_pg_database;

        tuple = GetDatabaseTupleByOid(dboid);

        // Initialize datform to satisfy borrow checker; conditional on HeapTupleIsValid below.
        let datform_ptr: Form_pg_database = if HeapTupleIsValid(tuple) {
            GETSTRUCT(tuple) as Form_pg_database
        } else {
            core::ptr::null_mut()
        };

        if !HeapTupleIsValid(tuple)
            || (!in_dbname.is_null() && namestrcmp(&(*datform_ptr).datname, in_dbname) != 0)
        {
            if !in_dbname.is_null() {
                ereport!(
                    FATAL,
                    errmsg!(
                        "database \"{}\" does not exist",
                        core::ffi::CStr::from_ptr(in_dbname).to_string_lossy()
                    )
                );
            } else {
                ereport!(
                    FATAL,
                    errmsg!("database {} does not exist", dboid)
                );
            }
        }

        strlcpy(
            dbname.as_mut_ptr(),
            crate::c::NameStr(&(*datform_ptr).datname),
            NAMEDATALEN,
        );

        if database_is_invalid_form(datform_ptr) {
            ereport!(
                FATAL,
                errmsg!(
                    "cannot connect to invalid database \"{}\"",
                    core::ffi::CStr::from_ptr(dbname.as_ptr()).to_string_lossy()
                )
            );
        }

        MyDatabaseTableSpace = (*datform_ptr).dattablespace;
        MyDatabaseHasLoginEventTriggers = (*datform_ptr).dathasloginevt;
        /* pass the database name back to the caller */
        if !out_dbname.is_null() {
            strcpy(out_dbname, dbname.as_ptr());
        }
    }

    /*
     * Now that we rechecked, we are certain to be connected to a database and
     * thus can set MyDatabaseId.
     *
     * It is important that MyDatabaseId only be set once we are sure that the
     * target database can no longer be concurrently dropped or renamed.  For
     * example, without this guarantee, pgstat_update_dbstats() could create
     * entries for databases that were just dropped in the pgstat shutdown
     * callback, which could confuse other code paths like the autovacuum
     * scheduler.
     */
    MyDatabaseId = dboid;

    /*
     * Now we can mark our PGPROC entry with the database ID.
     *
     * We assume this is an atomic store so no lock is needed; though actually
     * things would work fine even if it weren't atomic.  Anyone searching the
     * ProcArray for this database's ID should hold the database lock, so they
     * would not be executing concurrently with this store.  A process looking
     * for another database's ID could in theory see a chance match if it read
     * a partially-updated databaseId value; but as long as all such searches
     * wait and retry, as in CountOtherDBBackends(), they will certainly see
     * the correct value on their next try.
     */
    (*MyProc).databaseId = MyDatabaseId;

    /*
     * We established a catalog snapshot while reading pg_authid and/or
     * pg_database; but until we have set up MyDatabaseId, we won't react to
     * incoming sinval messages for unshared catalogs, so we won't realize it
     * if the snapshot has been invalidated.  Assume it's no good anymore.
     */
    InvalidateCatalogSnapshot();

    /*
     * Now we should be able to access the database directory safely. Verify
     * it's there and looks reasonable.
     */
    fullpath = GetDatabasePath(MyDatabaseId, MyDatabaseTableSpace);

    if !bootstrap {
        if access(fullpath, F_OK) == -1 {
            if get_errno() == ENOENT {
                ereport!(
                    FATAL,
                    errmsg!(
                        "database \"{}\" does not exist",
                        core::ffi::CStr::from_ptr(dbname.as_ptr()).to_string_lossy()
                    )
                );
            } else {
                ereport!(
                    FATAL,
                    errmsg!(
                        "could not access directory \"{}\"",
                        core::ffi::CStr::from_ptr(fullpath).to_string_lossy()
                    )
                );
            }
        }

        ValidatePgVersion(fullpath);
    }

    SetDatabasePath(fullpath);
    pfree(fullpath as *mut c_void);

    /*
     * It's now possible to do real access to the system catalogs.
     *
     * Load relcache entries for the system catalogs.  This must create at
     * least the minimum set of "nailed-in" cache entries.
     */
    RelationCacheInitializePhase3();

    /* set up ACL framework (so CheckMyDatabase can check permissions) */
    initialize_acl();

    /*
     * Re-read the pg_database row for our database, check permissions and set
     * up database-specific GUC settings.  We can't do this until all the
     * database-access infrastructure is up.  (Also, it wants to know if the
     * user is a superuser, so the above stuff has to happen first.)
     */
    if !bootstrap {
        CheckMyDatabase(
            dbname.as_ptr(),
            am_superuser,
            (flags & INIT_PG_OVERRIDE_ALLOW_CONNS as bits32) != 0,
        );
    }

    /*
     * Now process any command-line switches and any additional GUC variable
     * settings passed in the startup packet.   We couldn't do this before
     * because we didn't know if client is a superuser.
     */
    if !MyProcPort.is_null() {
        process_startup_options(MyProcPort, am_superuser);
    }

    /* Process pg_db_role_setting options */
    process_settings(MyDatabaseId, GetSessionUserId());

    /* Apply PostAuthDelay as soon as we've read all options */
    if PostAuthDelay > 0 {
        pg_usleep(PostAuthDelay as c_long * 1_000_000);
    }

    /*
     * Initialize various default states that can't be set up until we've
     * selected the active user and gotten the right GUC settings.
     */

    /* set default namespace search path */
    InitializeSearchPath();

    /* initialize client encoding */
    InitializeClientEncoding();

    /* Initialize this backend's session state. */
    InitializeSession();

    /*
     * If this is an interactive session, load any libraries that should be
     * preloaded at backend start.  Since those are determined by GUCs, this
     * can't happen until GUC settings are complete, but we want it to happen
     * during the initial transaction in case anything that requires database
     * access needs to be done.
     */
    if (flags & INIT_PG_LOAD_SESSION_LIBS as bits32) != 0 {
        process_session_preload_libraries();
    }

    /* fill in the remainder of this entry in the PgBackendStatus array */
    if !bootstrap {
        pgstat_bestart_final();
    }

    /* close the transaction we started above */
    if !bootstrap {
        CommitTransactionCommand();
    }
}

/*
 * Process any command-line switches and any additional GUC variable
 * settings passed in the startup packet.
 */
unsafe fn process_startup_options(port: *mut c_void, am_superuser: bool) {
    let gucctx: GucContext;
    let port = port as *mut PortStub;

    gucctx = if am_superuser { PGC_SU_BACKEND } else { PGC_BACKEND };

    /*
     * First process any command-line switches that were included in the
     * startup packet, if we are in a regular backend.
     */
    if !(*port).cmdline_options.is_null() {
        /*
         * The maximum possible number of commandline arguments that could
         * come from port->cmdline_options is (strlen + 1) / 2; see
         * pg_split_opts().
         */
        let av: *mut *mut c_char;
        let maxac: c_int;
        let mut ac: c_int;

        maxac = 2 + (libc_strlen((*port).cmdline_options) as c_int + 1) / 2;

        av = palloc(maxac as Size * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
        ac = 0;

        *av.add(0) = c"postgres".as_ptr() as *mut c_char;
        ac += 1;

        pg_split_opts(av, &mut ac, (*port).cmdline_options);

        *av.add(ac as usize) = core::ptr::null_mut();

        Assert!(ac < maxac);

        let _ = process_postgres_switches(ac, av, gucctx, core::ptr::null_mut());
    }

    /*
     * Process any additional GUC variable settings passed in startup packet.
     * These are handled exactly like command-line variables.
     */
    // TODO(pg-port): real List/ListCell iteration lives in nodes/pg_list.rs;
    // the guc_options List* field needs proper list traversal.
    // For now, translate the list loop using list_head/lfirst/lnext.
    {
        use crate::nodes::pg_list::{lfirst, list_head, lnext, ListCell};
        let mut gucopts: *mut ListCell = list_head((*port).guc_options as *mut crate::nodes::pg_list::List);
        while !gucopts.is_null() {
            let name: *const c_char = lfirst(gucopts) as *const c_char;
            gucopts = lnext((*port).guc_options as *mut crate::nodes::pg_list::List, gucopts);

            let value: *const c_char = lfirst(gucopts) as *const c_char;
            gucopts = lnext((*port).guc_options as *mut crate::nodes::pg_list::List, gucopts);

            SetConfigOption(name, value, gucctx, PGC_S_CLIENT);
        }
    }
}

// strlen shim (libc)
extern "C" {
    fn strlen(s: *const c_char) -> Size;
}
unsafe fn libc_strlen(s: *const c_char) -> Size {
    strlen(s)
}

/*
 * Load GUC settings from pg_db_role_setting.
 *
 * We try specific settings for the database/role combination, as well as
 * general for this database and for this user.
 */
unsafe fn process_settings(databaseid: Oid, roleid: Oid) {
    let relsetting: Relation;
    let snapshot: Snapshot;

    if !IsUnderPostmaster {
        return;
    }

    relsetting = table_open(DbRoleSettingRelationId, AccessShareLock);

    /* read all the settings under the same snapshot for efficiency */
    snapshot = RegisterSnapshot(GetCatalogSnapshot(DbRoleSettingRelationId));

    /* Later settings are ignored if set earlier. */
    ApplySetting(snapshot, databaseid, roleid, relsetting, PGC_S_DATABASE_USER);
    ApplySetting(snapshot, InvalidOid, roleid, relsetting, PGC_S_USER);
    ApplySetting(snapshot, databaseid, InvalidOid, relsetting, PGC_S_DATABASE);
    ApplySetting(snapshot, InvalidOid, InvalidOid, relsetting, PGC_S_GLOBAL);

    UnregisterSnapshot(snapshot);
    table_close(relsetting, AccessShareLock);
}

/*
 * Backend-shutdown callback.  Do cleanup that we want to be sure happens
 * before all the supporting modules begin to nail their doors shut via
 * their own callbacks.
 *
 * User-level cleanup, such as temp-relation removal and UNLISTEN, happens
 * via separate callbacks that execute before this one.  We don't combine the
 * callbacks because we still want this one to happen if the user-level
 * cleanup fails.
 */
unsafe extern "C" fn ShutdownPostgres(_code: c_int, _arg: Datum) {
    /* Make sure we've killed any active transaction */
    AbortOutOfAnyTransaction();

    /*
     * User locks are not released by transaction end, so be sure to release
     * them explicitly.
     */
    LockReleaseAll(USER_LOCKMETHOD, true);
}

// ShutdownXLOG wrapper for before_shmem_exit callback signature.
// TODO(pg-port): real ShutdownXLOG lives in access/transam/xlog.c
unsafe extern "C" fn ShutdownXLOG_cb(_code: c_int, _arg: Datum) {
    unimplemented!() // TODO(pg-port): real ShutdownXLOG lives in access/transam/xlog.c
}


/*
 * STATEMENT_TIMEOUT handler: trigger a query-cancel interrupt.
 */
unsafe extern "C" fn StatementTimeoutHandler() {
    let sig: c_int = if ClientAuthInProgress {
        /*
         * During authentication the timeout is used to deal with
         * authentication_timeout - we want to quit in response to such timeouts.
         */
        libc_SIGTERM
    } else {
        libc_SIGINT
    };

    // HAVE_SETSID: try to signal whole process group
    // Conditionally compiled on most Unix targets; use extern shim.
    kill_process_group(sig);
    libc_kill(MyProcPid, sig);
}

/*
 * LOCK_TIMEOUT handler: trigger a query-cancel interrupt.
 */
unsafe extern "C" fn LockTimeoutHandler() {
    // HAVE_SETSID: try to signal whole process group
    kill_process_group(libc_SIGINT);
    libc_kill(MyProcPid, libc_SIGINT);
}

unsafe extern "C" fn TransactionTimeoutHandler() {
    TransactionTimeoutPending = true as sig_atomic_t;
    InterruptPending = true as sig_atomic_t;
    SetLatch(MyLatch);
}

unsafe extern "C" fn IdleInTransactionSessionTimeoutHandler() {
    IdleInTransactionSessionTimeoutPending = true as sig_atomic_t;
    InterruptPending = true as sig_atomic_t;
    SetLatch(MyLatch);
}

unsafe extern "C" fn IdleSessionTimeoutHandler() {
    IdleSessionTimeoutPending = true as sig_atomic_t;
    InterruptPending = true as sig_atomic_t;
    SetLatch(MyLatch);
}

unsafe extern "C" fn IdleStatsUpdateTimeoutHandler() {
    IdleStatsUpdateTimeoutPending = true as sig_atomic_t;
    InterruptPending = true as sig_atomic_t;
    SetLatch(MyLatch);
}

unsafe extern "C" fn ClientCheckTimeoutHandler() {
    CheckClientConnectionPending = true as sig_atomic_t;
    InterruptPending = true as sig_atomic_t;
    SetLatch(MyLatch);
}

// Signal helpers (POSIX)
extern "C" {
    fn kill(pid: c_int, sig: c_int) -> c_int;
}
// SIGINT = 2, SIGTERM = 15 on all POSIX platforms we target
const libc_SIGINT: c_int = 2;
const libc_SIGTERM: c_int = 15;
unsafe fn libc_kill(pid: c_int, sig: c_int) {
    kill(pid, sig);
}
/// Attempt to signal the whole process group (HAVE_SETSID path).
unsafe fn kill_process_group(sig: c_int) {
    kill(-MyProcPid, sig);
}

// Globals used in signal handlers and init body -- pull from globals module
use crate::utils::init::globals::{
    CheckClientConnectionPending, IdleInTransactionSessionTimeoutPending,
    IdleSessionTimeoutPending, IdleStatsUpdateTimeoutPending, InterruptPending,
    MyCancelKey, MyCancelKeyLength,
    MyDatabaseHasLoginEventTriggers, MyDatabaseId, MyDatabaseTableSpace,
    MyLatch, MyProcPid, MyProcPort, TransactionTimeoutPending,
};

// sig_atomic_t alias
type sig_atomic_t = crate::utils::init::globals::sig_atomic_t;

// has_rolreplication (miscadmin.rs)
use crate::miscadmin::has_rolreplication;

/*
 * Returns true if at least one role is defined in this database cluster.
 */
unsafe fn ThereIsAtLeastOneRole() -> bool {
    let pg_authid_rel: Relation;
    let scan: TableScanDesc;
    let result: bool;

    pg_authid_rel = table_open(AuthIdRelationId, AccessShareLock);

    scan = table_beginscan_catalog(pg_authid_rel, 0, core::ptr::null_mut());
    result = !heap_getnext(scan as *mut c_void, ForwardScanDirection).is_null();

    table_endscan(scan);
    table_close(pg_authid_rel, AccessShareLock);

    result
}

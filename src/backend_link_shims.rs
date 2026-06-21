//! Link shims: define the C symbols that scattered `extern "C"` stubs across the boot
//! path reference, so the postmaster binary LINKS and can begin executing. Function shims
//! `unimplemented!()` until forwarded/wired; static shims are zero placeholders. Non-primitive
//! pointer types are normalized to c_void (ABI-equivalent). Generated; see memory pepperdb-regress.

#![allow(non_upper_case_globals, non_snake_case, clippy::all)]
use core::ffi::{c_int, c_uint, c_char, c_short, c_long, c_ulong, c_double, c_void};
use crate::c::*;
use crate::postgres_ext::Oid;
use crate::postgres::Datum;
type TimestampTz = i64;
type pgsocket = c_int;

#[no_mangle] pub static mut AF_UNIX: c_int = 1 as c_int; /* AF_UNIX/AF_LOCAL = 1 on Linux/macOS */
#[no_mangle] pub static mut AF_UNSPEC: c_int = 0 as c_int;
#[no_mangle] pub static mut ARCHIVE_MODE_OFF: c_int = 0 as c_int;
#[no_mangle] pub unsafe extern "C" fn AddWaitEventToSet(set: *mut c_void, events: uint32, fd: pgsocket, latch: *mut c_void, user_data: *mut c_void) -> c_int { crate::storage::ipc::waiteventset::AddWaitEventToSet(set as _, events, fd, latch as _, user_data) }
#[no_mangle] pub unsafe extern "C" fn BackgroundWorkerList() -> *mut c_void { &raw mut crate::postmaster::bgworker::BackgroundWorkerList as *mut c_void }
#[no_mangle] pub unsafe extern "C" fn BackgroundWorkerStateChange(allow: bool) { crate::postmaster::bgworker::BackgroundWorkerStateChange(allow) }
#[no_mangle] pub unsafe extern "C" fn BackgroundWorkerStopNotifications(pid: c_int) { unimplemented!("link-shim: BackgroundWorkerStopNotifications") }
#[no_mangle] pub unsafe extern "C" fn CheckLogrotateSignal() -> bool { unimplemented!("link-shim: CheckLogrotateSignal") }
#[no_mangle] pub static mut ClientAuthInProgress: bool = false;
#[no_mangle] pub static mut ConfigFileName: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub unsafe extern "C" fn CreateWaitEventSet(context: *mut c_void, nevents: c_int) -> *mut c_void { crate::storage::ipc::waiteventset::CreateWaitEventSet(context as _, nevents) as *mut c_void }
#[no_mangle] pub static mut EnableHotStandby: bool = false;
#[no_mangle] pub unsafe extern "C" fn FindDefaultConversion(name_space: Oid, for_encoding: c_int, to_encoding: c_int) -> Oid { crate::catalog::pg_conversion::FindDefaultConversion(name_space, for_encoding, to_encoding) }
#[no_mangle] pub unsafe extern "C" fn ForgetBackgroundWorker(rw: *mut c_void) { crate::postmaster::bgworker::ForgetBackgroundWorker(rw as _) }
#[no_mangle] pub unsafe extern "C" fn ForgetUnstartedBackgroundWorkers() { unimplemented!("link-shim: ForgetUnstartedBackgroundWorkers") }
#[no_mangle] pub unsafe extern "C" fn FreeWaitEventSet(set: *mut c_void) { crate::storage::ipc::waiteventset::FreeWaitEventSet(set as _) }
#[no_mangle] pub unsafe extern "C" fn GetCurrentTimestamp() -> TimestampTz { crate::utils::adt::timestamp::GetCurrentTimestamp() }
#[no_mangle] pub unsafe extern "C" fn GetMemoryChunkContext(pointer: *mut c_void) -> *mut c_void { crate::utils::mmgr::mcxt::GetMemoryChunkContext(pointer) as *mut c_void }
#[no_mangle] pub static mut HbaFileName: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut IdentFileName: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub unsafe extern "C" fn InitializeWaitEventSupport() { crate::storage::ipc::waiteventset::InitializeWaitEventSupport() }
#[no_mangle] pub unsafe extern "C" fn InitializeWalConsistencyChecking() { crate::access::transam::xlog::InitializeWalConsistencyChecking() }
#[no_mangle] pub unsafe extern "C" fn IsInParallelMode() -> bool { false }
#[no_mangle] pub static mut LOG_METAINFO_DATAFILE: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub unsafe extern "C" fn LocalProcessControlFile(reset: bool) { crate::access::transam::xlog::LocalProcessControlFile(reset) }
#[no_mangle] pub static mut Logging_collector: bool = false;
#[no_mangle] pub unsafe extern "C" fn MemoryContextAllocZero(context: *mut c_void, size: usize) -> *mut c_void { crate::utils::palloc::MemoryContextAllocZero(context as _, size) }
#[no_mangle] pub unsafe extern "C" fn AllocSetContextCreate(parent: *mut c_void, name: *const core::ffi::c_char, min_size: usize, init_size: usize, max_size: usize) -> *mut c_void { crate::utils::mmgr::aset::AllocSetContextCreateInternal(parent as _, name, min_size, init_size, max_size) as _ }
// complex static Mode: ProcessingMode
#[no_mangle] pub static mut PG_VERSION_STR: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub unsafe extern "C" fn ParseConfigFile(config_file: *const c_void, strict: bool, calling_file: *const c_void, calling_lineno: c_int, depth: c_int, elevel: c_int, head_p: *mut c_void, tail_p: *mut c_void) -> bool {
    // bring-up: real postgresql.conf parser (guc-file.l) unported; return an EMPTY config list
    // (success) so the postmaster uses GUC compiled defaults. TODO: real config-file parsing.
    if !head_p.is_null() { *(head_p as *mut *mut c_void) = core::ptr::null_mut(); }
    if !tail_p.is_null() { *(tail_p as *mut *mut c_void) = core::ptr::null_mut(); }
    true
}
#[no_mangle] pub unsafe extern "C" fn ReleaseSysCache(tuple: *mut c_void) { crate::utils::cache::syscache::ReleaseSysCache(tuple as _) }
#[no_mangle] pub unsafe extern "C" fn ReleaseSysCacheList(list: *mut c_void) { crate::utils::cache::catcache::ReleaseCatCacheList(list as _) }
#[no_mangle] pub unsafe extern "C" fn RemoveLogrotateSignalFiles() { libc::unlink(c"logrotate".as_ptr()); }
#[no_mangle] pub unsafe extern "C" fn ReportBackgroundWorkerExit(rw: *mut c_void) { crate::postmaster::bgworker::ReportBackgroundWorkerExit(rw as _) }
#[no_mangle] pub unsafe extern "C" fn ReportBackgroundWorkerPID(rw: *mut c_void) { crate::postmaster::bgworker::ReportBackgroundWorkerPID(rw as _) }
#[no_mangle] pub unsafe extern "C" fn ResetBackgroundWorkerCrashTimes() { crate::postmaster::bgworker::ResetBackgroundWorkerCrashTimes() }
#[no_mangle] pub unsafe extern "C" fn SearchSysCache1(cacheid: c_int, key1: u64) -> *mut c_void { crate::utils::cache::syscache::SearchSysCache1(cacheid, key1 as _) as *mut c_void }
#[no_mangle] pub unsafe extern "C" fn SearchSysCacheList1(cacheId: c_int, key1: Datum) -> *mut c_void { crate::utils::cache::syscache::SearchSysCacheList(cacheId, 1, key1, 0 as Datum, 0 as Datum) as *mut c_void }
#[no_mangle] pub unsafe extern "C" fn SysCacheGetAttr(cacheid: c_int, tuple: *mut c_void, attnum: c_int, isnull: *mut c_void) -> u64 { crate::utils::cache::syscache::SysCacheGetAttr(cacheid, tuple as _, attnum as _, isnull as _) as u64 }
#[no_mangle] pub unsafe extern "C" fn SysLogger_Start(child_slot: c_int) -> c_int { unimplemented!("link-shim: SysLogger_Start") }
#[no_mangle] pub unsafe extern "C" fn TimestampTzPlusMilliseconds(t: TimestampTz, ms: c_long) -> TimestampTz { unimplemented!("link-shim: TimestampTzPlusMilliseconds") }
#[no_mangle] pub unsafe extern "C" fn WaitEventSetWait(set: *mut c_void, timeout: c_int, occurred_events: *mut c_void, nevents: c_int, wait_event_info: uint32) -> c_int { crate::storage::ipc::waiteventset::WaitEventSetWait(set as _, timeout as c_long, occurred_events as _, nevents, wait_event_info) }
#[no_mangle] pub static mut XLogArchiveMode: c_int = 0 as c_int;
// Bring-up: WAL archiving is off (archive_mode default off) -> false.
#[no_mangle] pub unsafe extern "C" fn XLogArchivingActive() -> bool { false }
#[no_mangle] pub unsafe extern "C" fn XLogArchivingAlways() -> bool { false }
#[no_mangle] pub unsafe extern "C" fn bloom_add_element(filter: *mut c_void, elem: *const c_void, len: usize) { unimplemented!("link-shim: bloom_add_element") }
#[no_mangle] pub unsafe extern "C" fn bloom_create(total_elems: int64, bloom_work_mem: c_int, seed: uint64) -> *mut c_void { unimplemented!("link-shim: bloom_create") }
#[no_mangle] pub unsafe extern "C" fn bloom_free(filter: *mut c_void) { unimplemented!("link-shim: bloom_free") }
#[no_mangle] pub unsafe extern "C" fn bloom_lacks_element(filter: *mut c_void, elem: *const c_void, len: usize) -> bool { unimplemented!("link-shim: bloom_lacks_element") }
#[no_mangle] pub static mut client_min_messages: c_int = 0 as c_int;
#[no_mangle] pub unsafe extern "C" fn convert_GUC_name_for_parameter_acl(name: *const c_void) -> *mut c_void { unimplemented!("link-shim: convert_GUC_name_for_parameter_acl") }
pub unsafe extern "C" fn cstring_to_text(s: *const c_void) -> *mut c_void { unimplemented!("link-shim: cstring_to_text") }
#[no_mangle] pub static mut debug_query_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut external_pid_file: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub unsafe extern "C" fn get_stack_depth_rlimit() -> isize { crate::utils::misc::stack_depth::get_stack_depth_rlimit() as isize }
#[no_mangle] pub unsafe extern "C" fn load_hba() -> bool { crate::libpq::hba::load_hba() }
#[no_mangle] pub unsafe extern "C" fn load_ident() -> bool { crate::libpq::hba::load_ident() }
// Symbols newly reachable from the child-launch (fork) path.
#[no_mangle] pub unsafe extern "C" fn FreeWaitEventSetAfterFork(set: *mut c_void) { crate::storage::ipc::waiteventset::FreeWaitEventSetAfterFork(set as _) }
#[no_mangle] pub unsafe extern "C" fn ReleaseExternalFD() { crate::storage::file::fd::ReleaseExternalFD() }
#[no_mangle] pub unsafe extern "C" fn pg_strong_random_init() { crate::port::pg_strong_random::pg_strong_random_init() }
// syslogPipe: real def lives in the unwired syslogger.rs; logging_collector is off.
#[no_mangle] pub static mut syslogPipe: [c_int; 2] = [-1, -1];
// pgaio_my_backend (aio.c): per-backend AIO state pointer; only declared extern in aio_internal.rs.
#[no_mangle] pub static mut pgaio_my_backend: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut log_min_messages: c_int = 0 as c_int;
#[no_mangle] pub unsafe extern "C" fn make_absolute_path(path: *const c_void) -> *mut c_void { crate::port::path::make_absolute_path(path as *const c_char) as *mut c_void }
#[no_mangle] pub unsafe extern "C" fn palloc(size: usize) -> *mut c_void { crate::utils::palloc::palloc(size) }
#[no_mangle] pub unsafe extern "C" fn parse_bool(value: *const c_void, result: *mut c_void) -> bool { crate::utils::adt::bool::parse_bool(value as *const c_char, result as *mut bool) }
#[no_mangle] pub unsafe extern "C" fn pfree(ptr: *mut c_void) { crate::utils::palloc::pfree(ptr) }
// complex static pg_global_prng_state: PgPrngState
// Minimal real prng shims (state is a 16-byte [u64;2]). Enough to boot past InitProcessGlobals.
#[no_mangle] pub unsafe extern "C" fn pg_prng_seed(state: *mut c_void, seed: u64) {
    let s = state as *mut u64; *s = seed.wrapping_add(0x9E3779B97F4A7C15); *s.add(1) = seed ^ 0xD1B54A32D192ED03;
}
#[no_mangle] pub unsafe extern "C" fn pg_prng_strong_seed(state: *mut c_void) -> bool {
    libc::arc4random_buf(state, 16); true
}
#[no_mangle] pub unsafe extern "C" fn pg_prng_uint32(state: *mut c_void) -> u32 {
    let s = state as *mut u64; let mut x = *s; x ^= x << 13; x ^= x >> 7; x ^= x << 17; *s = x; (x >> 32) as u32
}
#[no_mangle] pub unsafe extern "C" fn pg_strcasecmp(s1: *const c_void, s2: *const c_void) -> c_int { crate::port::pgstrcasecmp::pg_strcasecmp(s1 as *const c_char, s2 as *const c_char) }
#[no_mangle] pub unsafe extern "C" fn pstrdup(s: *const c_void) -> *mut c_void { crate::utils::palloc::pstrdup(s as *const c_char) as *mut c_void }
#[no_mangle] pub unsafe extern "C" fn record_config_file_error(msg: *const c_void, filename: *const c_void, lineno: c_int, head_p: *mut c_void, tail_p: *mut c_void) { unimplemented!("link-shim: record_config_file_error") }
#[no_mangle] pub unsafe extern "C" fn superuser() -> bool { crate::utils::misc::superuser::superuser() }
// libpgcommon_srv.a renames these via pg_wchar.h #defines; loadable C modules
// (regress.c) resolve the *_private names at dlopen time.
#[no_mangle] pub unsafe extern "C" fn pg_char_to_encoding_private(name: *const c_char) -> c_int { crate::common::encnames::pg_char_to_encoding(name) }
#[no_mangle] pub unsafe extern "C" fn pg_encoding_to_char_private(encoding: c_int) -> *const c_char { crate::common::encnames::pg_encoding_to_char(encoding) }
#[no_mangle] pub unsafe extern "C" fn pg_valid_server_encoding_private(name: *const c_char) -> c_int { crate::common::encnames::pg_valid_server_encoding(name) }
#[no_mangle] pub unsafe extern "C" fn superuser_arg(roleid: Oid) -> bool { crate::utils::misc::superuser::superuser_arg(roleid) }
#[no_mangle] pub static mut sync_replication_slots: bool = false;
#[no_mangle] pub unsafe extern "C" fn truncate_identifier(ident: *mut c_void, len: usize, warn: bool) { crate::parser::scansup::truncate_identifier(ident as *mut c_char, len as i32, warn) }
#[no_mangle] pub unsafe extern "C" fn waitpid_sys(pid: c_int, stat_loc: *mut c_void, options: c_int) -> c_int { libc::waitpid(pid, stat_loc as *mut c_int, options) }
#[no_mangle] pub static mut wal_level: c_int = 0 as c_int;
#[no_mangle] pub unsafe extern "C" fn write_stderr(fmt: *const c_void, a1: *mut c_void) {
    // best-effort: print to fd 2 with up to one format arg (most write_stderr calls are "%s: ...", x).
    extern "C" { fn dprintf(fd: c_int, fmt: *const c_char, ...) -> c_int; }
    dprintf(2, fmt as *const c_char, a1);
}
#[no_mangle] pub static mut pg_global_prng_state: [u64; 2] = [0, 0]; // PgPrngState placeholder

// fd.rs/sysv_shmem.rs reference `stat$INODE64`/`lstat$INODE64` (a macOS x86 hack that has no
// arm64 symbol). Provide those names, forwarding to libc (which links the correct arm64 stat).
#[export_name = "stat$INODE64"]
pub unsafe extern "C" fn stat_inode64(path: *const c_char, buf: *mut c_void) -> c_int {
    libc::stat(path, buf as *mut libc::stat)
}
#[export_name = "lstat$INODE64"]
pub unsafe extern "C" fn lstat_inode64(path: *const c_char, buf: *mut c_void) -> c_int {
    libc::lstat(path, buf as *mut libc::stat)
}


// ---- GUC subsystem globals + benign hook shims (generated) ----
#[no_mangle] pub static mut AllowAlterSystem: bool = false;
#[no_mangle] pub static mut AuthenticationTimeout: c_int = 0 as c_int;
#[no_mangle] pub static mut CommitDelay: c_int = 0 as c_int;
#[no_mangle] pub static mut CommitSiblings: c_int = 0 as c_int;
#[no_mangle] pub static mut DeadlockTimeout: c_int = 0 as c_int;
#[no_mangle] pub static mut Debug_pretty_print: bool = false;
#[no_mangle] pub static mut Debug_print_parse: bool = false;
#[no_mangle] pub static mut Debug_print_plan: bool = false;
#[no_mangle] pub static mut Debug_print_rewritten: bool = false;
#[no_mangle] pub static mut DefaultXactDeferrable: bool = false;
#[no_mangle] pub static mut DefaultXactIsoLevel: c_int = 0 as c_int;
#[no_mangle] pub static mut DefaultXactReadOnly: bool = false;
#[no_mangle] pub static mut EnableSSL: bool = false;
#[no_mangle] pub static mut Extension_control_path: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut Geqo_effort: c_int = 0 as c_int;
#[no_mangle] pub static mut Geqo_generations: c_int = 0 as c_int;
#[no_mangle] pub static mut Geqo_pool_size: c_int = 0 as c_int;
#[no_mangle] pub static mut Geqo_seed: c_double = 0 as c_double;
#[no_mangle] pub static mut Geqo_selection_bias: c_double = 0 as c_double;
#[no_mangle] pub static mut GinFuzzySearchLimit: c_int = 0 as c_int;
#[no_mangle] pub static mut IdleInTransactionSessionTimeout: c_int = 0 as c_int;
#[no_mangle] pub static mut IdleSessionTimeout: c_int = 0 as c_int;
// ListenAddresses: canonical #[no_mangle] def lives in postmaster::postmaster.
#[no_mangle] pub static mut LockTimeout: c_int = 0 as c_int;
#[no_mangle] pub static mut Log_RotationAge: c_int = 0 as c_int;
#[no_mangle] pub static mut Log_RotationSize: c_int = 0 as c_int;
#[no_mangle] pub static mut Log_destination_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut Log_directory: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut Log_disconnections: bool = false;
#[no_mangle] pub static mut Log_error_verbosity: c_int = 0 as c_int;
#[no_mangle] pub static mut Log_file_mode: c_int = 0 as c_int;
#[no_mangle] pub static mut Log_filename: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut Log_line_prefix: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut Log_truncate_on_rotation: bool = false;
#[no_mangle] pub static mut Password_encryption: c_int = 0 as c_int;
#[no_mangle] pub static mut PostAuthDelay: c_int = 0 as c_int;
// PostPortNumber: canonical #[no_mangle] def lives in postmaster::postmaster.
#[no_mangle] pub static mut PreAuthDelay: c_int = 0 as c_int;
#[no_mangle] pub static mut PrimaryConnInfo: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut PrimarySlotName: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut ReservedConnections: c_int = 0 as c_int;
#[no_mangle] pub static mut SessionReplicationRole: c_int = 0 as c_int;
#[no_mangle] pub static mut StatementTimeout: c_int = 0 as c_int;
#[no_mangle] pub static mut SuperuserReservedConnections: c_int = 0 as c_int;
#[no_mangle] pub static mut SyncRepStandbyNames: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut TransactionTimeout: c_int = 0 as c_int;
#[no_mangle] pub static mut Transform_null_equals: bool = false;
// Unix_socket_directories: canonical #[no_mangle] def lives in postmaster::postmaster.
// Unix_socket_group / Unix_socket_permissions: canonical #[no_mangle] defs live in libpq::pqcomm.
#[no_mangle] pub static mut XLOGbuffers: c_int = 0 as c_int;
#[no_mangle] pub static mut XLogArchiveCommand: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut XLogArchiveLibrary: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut XLogArchiveTimeout: c_int = 0 as c_int;
#[no_mangle] pub static mut XactDeferrable: bool = false;
#[no_mangle] pub static mut XactIsoLevel: c_int = 0 as c_int;
#[no_mangle] pub static mut XactReadOnly: bool = false;
// allow_in_place_tablespaces now provided by commands/tablespace.rs (wired)
#[no_mangle] pub static mut application_name: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut archiveCleanupCommand: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut assert_enabled: bool = false;
#[no_mangle] pub static mut backend_flush_after: c_int = 0 as c_int;
#[no_mangle] pub static mut backslash_quote: c_int = 0 as c_int;
#[no_mangle] pub static mut bgwriter_flush_after: c_int = 0 as c_int;
#[no_mangle] pub static mut bgwriter_lru_maxpages: c_int = 0 as c_int;
#[no_mangle] pub static mut block_size: c_int = 0 as c_int;
#[no_mangle] pub static mut bonjour_name: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut check_function_bodies: bool = false;
#[no_mangle] pub static mut checkpoint_flush_after: c_int = 0 as c_int;
#[no_mangle] pub static mut client_connection_check_interval: c_int = 0 as c_int;
#[no_mangle] pub static mut client_encoding_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut cluster_name: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut constraint_exclusion: c_int = 0 as c_int;
#[no_mangle] pub static mut cpu_index_tuple_cost: c_double = 0 as c_double;
#[no_mangle] pub static mut cpu_tuple_cost: c_double = 0 as c_double;
#[no_mangle] pub static mut createrole_self_grant: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut current_role_is_superuser: bool = false;
#[no_mangle] pub static mut data_checksums: bool = false;
#[no_mangle] pub static mut data_directory: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut data_sync_retry: bool = false;
#[no_mangle] pub static mut datestyle_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut debug_discard_caches: c_int = 0 as c_int;
#[no_mangle] pub static mut debug_io_direct_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut debug_logical_replication_streaming: c_int = 0 as c_int;
#[no_mangle] pub static mut debug_parallel_query: c_int = 0 as c_int;
#[no_mangle] pub static mut default_statistics_target: c_int = 0 as c_int;
#[no_mangle] pub static mut default_table_access_method: *mut c_void = core::ptr::null_mut();
// default_tablespace now provided by commands/tablespace.rs (wired)
#[no_mangle] pub static mut default_toast_compression: c_int = 0 as c_int;
#[no_mangle] pub static mut dynamic_shared_memory_type: c_int = 0 as c_int;
#[no_mangle] pub static mut effective_cache_size: c_int = 0 as c_int;
#[no_mangle] pub static mut effective_io_concurrency: c_int = 0 as c_int;
#[no_mangle] pub static mut enable_bonjour: bool = false;
#[no_mangle] pub static mut enable_distinct_reordering: bool = false;
#[no_mangle] pub static mut enable_geqo: bool = false;
#[no_mangle] pub static mut enable_group_by_reordering: bool = false;
#[no_mangle] pub static mut enable_self_join_elimination: bool = false;
#[no_mangle] pub static mut escape_string_warning: bool = false;
#[no_mangle] pub static mut event_source: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut event_triggers: bool = false;
/* extra_float_digits is the GUC-backed symbol defined in utils/adt/float.rs */
#[no_mangle] pub static mut file_copy_method: c_int = 0 as c_int;
#[no_mangle] pub static mut file_extend_method: c_int = 0 as c_int;
#[no_mangle] pub static mut from_collapse_limit: c_int = 0 as c_int;
#[no_mangle] pub static mut fullPageWrites: bool = false;
#[no_mangle] pub static mut geqo_threshold: c_int = 0 as c_int;
#[no_mangle] pub static mut gin_pending_list_limit: c_int = 0 as c_int;
#[no_mangle] pub static mut hot_standby_feedback: bool = false;
#[no_mangle] pub static mut huge_page_size: c_int = 0 as c_int;
#[no_mangle] pub static mut huge_pages: c_int = 0 as c_int;
#[no_mangle] pub static mut huge_pages_status: c_int = 0 as c_int;
#[no_mangle] pub static mut icu_validation_level: c_int = 0 as c_int;
#[no_mangle] pub static mut idle_replication_slot_timeout_secs: c_int = 0 as c_int;
#[no_mangle] pub static mut ignore_checksum_failure: bool = false;
#[no_mangle] pub static mut ignore_invalid_pages: bool = false;
#[no_mangle] pub static mut in_hot_standby_guc: bool = false;
#[no_mangle] pub static mut integer_datetimes: bool = false;
#[no_mangle] pub static mut io_combine_limit_guc: c_int = 0 as c_int;
#[no_mangle] pub static mut io_workers: c_int = 0 as c_int;
#[no_mangle] pub static mut join_collapse_limit: c_int = 0 as c_int;
#[no_mangle] pub static mut log_checkpoints: bool = false;
#[no_mangle] pub static mut log_executor_stats: bool = false;
#[no_mangle] pub static mut log_hostname: bool = false;
#[no_mangle] pub static mut log_lock_failures: bool = false;
#[no_mangle] pub static mut log_lock_waits: bool = false;
#[no_mangle] pub static mut log_min_duration_sample: c_int = 0 as c_int;
#[no_mangle] pub static mut log_min_duration_statement: c_int = 0 as c_int;
#[no_mangle] pub static mut log_min_error_statement: c_int = 0 as c_int;
#[no_mangle] pub static mut log_parameter_max_length: c_int = 0 as c_int;
#[no_mangle] pub static mut log_parameter_max_length_on_error: c_int = 0 as c_int;
#[no_mangle] pub static mut log_parser_stats: bool = false;
#[no_mangle] pub static mut log_planner_stats: bool = false;
#[no_mangle] pub static mut log_recovery_conflict_waits: bool = false;
#[no_mangle] pub static mut log_statement: c_int = 0 as c_int;
#[no_mangle] pub static mut log_statement_sample_rate: c_double = 0 as c_double;
#[no_mangle] pub static mut log_statement_stats: bool = false;
#[no_mangle] pub static mut log_temp_files: c_int = 0 as c_int;
#[no_mangle] pub static mut log_timezone_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut logical_decoding_work_mem: c_int = 0 as c_int;
#[no_mangle] pub static mut maintenance_io_concurrency: c_int = 0 as c_int;
#[no_mangle] pub static mut max_active_replication_origins: c_int = 0 as c_int;
#[no_mangle] pub static mut max_files_per_process: c_int = 0 as c_int;
#[no_mangle] pub static mut max_function_args: c_int = 0 as c_int;
#[no_mangle] pub static mut max_identifier_length: c_int = 0 as c_int;
#[no_mangle] pub static mut max_index_keys: c_int = 0 as c_int;
#[no_mangle] pub static mut max_locks_per_xact: c_int = 0 as c_int;
#[no_mangle] pub static mut max_logical_replication_workers: c_int = 0 as c_int;
#[no_mangle] pub static mut max_parallel_apply_workers_per_subscription: c_int = 0 as c_int;
#[no_mangle] pub static mut max_parallel_workers_per_gather: c_int = 0 as c_int;
#[no_mangle] pub static mut max_predicate_locks_per_page: c_int = 0 as c_int;
#[no_mangle] pub static mut max_predicate_locks_per_relation: c_int = 0 as c_int;
#[no_mangle] pub static mut max_predicate_locks_per_xact: c_int = 0 as c_int;
#[no_mangle] pub static mut max_replication_slots: c_int = 0 as c_int;
#[no_mangle] pub static mut max_slot_wal_keep_size_mb: c_int = 0 as c_int;
#[no_mangle] pub static mut max_standby_archive_delay: c_int = 0 as c_int;
#[no_mangle] pub static mut max_standby_streaming_delay: c_int = 0 as c_int;
#[no_mangle] pub static mut max_sync_workers_per_subscription: c_int = 0 as c_int;
#[no_mangle] pub static mut max_wal_size_mb: c_int = 0 as c_int;
#[no_mangle] pub static mut min_dynamic_shared_memory: c_int = 0 as c_int;
#[no_mangle] pub static mut min_parallel_index_scan_size: c_int = 0 as c_int;
#[no_mangle] pub static mut min_parallel_table_scan_size: c_int = 0 as c_int;
#[no_mangle] pub static mut min_wal_size_mb: c_int = 0 as c_int;
#[no_mangle] pub static mut num_os_semaphores: c_int = 0 as c_int;
#[no_mangle] pub static mut num_temp_buffers: c_int = 0 as c_int;
#[no_mangle] pub static mut oauth_validator_libraries_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut parallel_leader_participation: bool = false;
#[no_mangle] pub static mut parallel_setup_cost: c_double = 0 as c_double;
#[no_mangle] pub static mut parallel_tuple_cost: c_double = 0 as c_double;
#[no_mangle] pub static mut pgstat_track_activities: bool = false;
#[no_mangle] pub static mut pgstat_track_activity_query_size: c_int = 0 as c_int;
#[no_mangle] pub static mut pgstat_track_functions: c_int = 0 as c_int;
#[no_mangle] pub static mut plan_cache_mode: c_int = 0 as c_int;
#[no_mangle] pub static mut random_page_cost: c_double = 0 as c_double;
#[no_mangle] pub static mut recoveryEndCommand: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut recoveryRestoreCommand: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut recoveryTargetAction: c_int = 0 as c_int;
#[no_mangle] pub static mut recoveryTargetInclusive: bool = false;
#[no_mangle] pub static mut recovery_init_sync_method: c_int = 0 as c_int;
#[no_mangle] pub static mut recovery_min_apply_delay: c_int = 0 as c_int;
#[no_mangle] pub static mut recovery_prefetch: c_int = 0 as c_int;
#[no_mangle] pub static mut recovery_target_lsn_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut recovery_target_name_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut recovery_target_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut recovery_target_time_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut recovery_target_timeline_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut recovery_target_xid_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut remove_temp_files_after_crash: bool = false;
#[no_mangle] pub static mut restart_after_crash: bool = false;
#[no_mangle] pub static mut restrict_nonsystem_relation_kind_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut role_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut scram_sha_256_iterations: c_int = 0 as c_int;
#[no_mangle] pub static mut segment_size: c_int = 0 as c_int;
#[no_mangle] pub static mut send_abort_for_crash: bool = false;
#[no_mangle] pub static mut send_abort_for_kill: bool = false;
#[no_mangle] pub static mut seq_page_cost: c_double = 0 as c_double;
#[no_mangle] pub static mut server_encoding_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut server_version_num: c_int = 0 as c_int;
#[no_mangle] pub static mut server_version_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut session_authorization_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut shared_memory_size_in_huge_pages: c_int = 0 as c_int;
#[no_mangle] pub static mut shared_memory_size_mb: c_int = 0 as c_int;
#[no_mangle] pub static mut ssl_renegotiation_limit: c_int = 0 as c_int;
#[no_mangle] pub static mut standard_conforming_strings: bool = false;
#[no_mangle] pub static mut synchronize_seqscans: bool = false;
#[no_mangle] pub static mut synchronized_standby_slots: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut synchronous_commit: c_int = 0 as c_int;
#[no_mangle] pub static mut syslog_facility: c_int = 0 as c_int;
#[no_mangle] pub static mut syslog_ident_str: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut syslog_sequence_numbers: bool = false;
#[no_mangle] pub static mut syslog_split_messages: bool = false;
#[no_mangle] pub static mut tcp_keepalives_count: c_int = 0 as c_int;
#[no_mangle] pub static mut tcp_keepalives_idle: c_int = 0 as c_int;
#[no_mangle] pub static mut tcp_keepalives_interval: c_int = 0 as c_int;
#[no_mangle] pub static mut tcp_user_timeout: c_int = 0 as c_int;
#[no_mangle] pub static mut temp_file_limit: c_int = 0 as c_int;
// temp_tablespaces now provided by commands/tablespace.rs (wired)
#[no_mangle] pub static mut timezone_abbreviations_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut timezone_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut trace_sort: bool = false;
#[no_mangle] pub static mut track_cost_delay_timing: bool = false;
#[no_mangle] pub static mut track_io_timing: bool = false;
#[no_mangle] pub static mut track_wal_io_timing: bool = false;
#[no_mangle] pub static mut vacuum_failsafe_age: c_int = 0 as c_int;
#[no_mangle] pub static mut vacuum_freeze_min_age: c_int = 0 as c_int;
#[no_mangle] pub static mut vacuum_freeze_table_age: c_int = 0 as c_int;
#[no_mangle] pub static mut vacuum_multixact_failsafe_age: c_int = 0 as c_int;
#[no_mangle] pub static mut vacuum_multixact_freeze_min_age: c_int = 0 as c_int;
#[no_mangle] pub static mut vacuum_multixact_freeze_table_age: c_int = 0 as c_int;
#[no_mangle] pub static mut vacuum_truncate: bool = false;
#[no_mangle] pub static mut wal_block_size: c_int = 0 as c_int;
#[no_mangle] pub static mut wal_compression: c_int = 0 as c_int;
#[no_mangle] pub static mut wal_consistency_checking_string: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut wal_decode_buffer_size: c_int = 0 as c_int;
#[no_mangle] pub static mut wal_init_zero: bool = false;
#[no_mangle] pub static mut wal_keep_size_mb: c_int = 0 as c_int;
#[no_mangle] pub static mut wal_log_hints: bool = false;
#[no_mangle] pub static mut wal_receiver_create_temp_slot: bool = false;
#[no_mangle] pub static mut wal_receiver_status_interval: c_int = 0 as c_int;
#[no_mangle] pub static mut wal_receiver_timeout: c_int = 0 as c_int;
#[no_mangle] pub static mut wal_recycle: bool = false;
#[no_mangle] pub static mut wal_retrieve_retry_interval: c_int = 0 as c_int;
#[no_mangle] pub static mut wal_segment_size: c_int = 0 as c_int;
#[no_mangle] pub static mut wal_sync_method: c_int = 0 as c_int;
#[no_mangle] pub static mut xmlbinary: c_int = 0 as c_int;
#[no_mangle] pub static mut xmloption: c_int = 0 as c_int;
#[no_mangle] pub static mut zero_damaged_pages: bool = false;
#[no_mangle] pub unsafe extern "C" fn assign_application_name(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_backtrace_functions(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_client_encoding(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_createrole_self_grant(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_datestyle(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_debug_io_direct(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_default_text_search_config(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_io_combine_limit(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_io_max_combine_limit(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_io_method(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_locale_messages(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_locale_monetary(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_locale_numeric(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_locale_time(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_log_destination(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_log_timezone(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_maintenance_io_concurrency(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_max_stack_depth(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_max_wal_size(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_recovery_prefetch(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_recovery_target(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_recovery_target_lsn(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_recovery_target_name(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_recovery_target_time(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_recovery_target_timeline(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_recovery_target_xid(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_restrict_nonsystem_relation_kind(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_role(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_search_path(newval: *const c_void, extra: *mut c_void) { }
#[repr(C)] struct pdb_role_auth_extra { roleid: Oid, is_superuser: bool }
#[no_mangle] pub unsafe extern "C" fn assign_session_authorization(_newval: *const c_void, extra: *mut c_void) {
    let myextra = extra as *mut pdb_role_auth_extra;
    if std::env::var_os("PDB_AUTH").is_some() { eprintln!("PDB_AUTH assign pid={} extra_null={} roleid={}", std::process::id(), myextra.is_null(), if myextra.is_null() {0} else {(*myextra).roleid}); }
    if myextra.is_null() { return; }
    crate::utils::init::miscinit::SetSessionAuthorization((*myextra).roleid, (*myextra).is_superuser);
}
#[no_mangle] pub unsafe extern "C" fn assign_session_replication_role(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_stats_fetch_consistency(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_synchronized_standby_slots(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_synchronous_commit(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_synchronous_standby_names(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_syslog_facility(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_syslog_ident(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_tcp_keepalives_count(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_tcp_keepalives_idle(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_tcp_keepalives_interval(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_tcp_user_timeout(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_temp_tablespaces(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_timezone(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_timezone_abbreviations(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_transaction_timeout(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_wal_consistency_checking(newval: *const c_void, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn assign_wal_sync_method(newval: c_int, extra: *mut c_void) { }
#[no_mangle] pub unsafe extern "C" fn check_application_name(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_backtrace_functions(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_bonjour(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_canonical_path(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_client_connection_check_interval(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_client_encoding(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_cluster_name(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_commit_ts_buffers(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_createrole_self_grant(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_datestyle(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_debug_io_direct(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_default_table_access_method(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_default_tablespace(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_default_text_search_config(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_default_with_oids(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_io_max_concurrency(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_locale_messages(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_locale_monetary(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_locale_numeric(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_locale_time(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_log_destination(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_log_stats(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_log_timezone(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_max_stack_depth(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_multixact_member_buffers(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_multixact_offset_buffers(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_notify_buffers(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_primary_slot_name(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_recovery_prefetch(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_recovery_target(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_recovery_target_lsn(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_recovery_target_name(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_recovery_target_time(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_recovery_target_timeline(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_recovery_target_xid(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_restrict_nonsystem_relation_kind(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_role(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_search_path(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_serial_buffers(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_session_authorization(newval: *mut c_void, extra: *mut c_void, _source: c_int) -> bool {
    let newval = newval as *mut *mut c_char;
    let extra = extra as *mut *mut c_void;
    if (*newval).is_null() { return true; }
    let roleid: Oid; let is_superuser: bool;
    if crate::access::transam::parallel::InitializingParallelWorker {
        roleid = crate::utils::init::miscinit::GetSessionUserId();
        is_superuser = crate::utils::init::miscinit::GetSessionUserIsSuperuser();
    } else {
        if !crate::access::transam::xact::IsTransactionState() { return false; }
        let role_tup = crate::utils::cache::syscache::SearchSysCache1(
            crate::utils::cache::syscache_ids_gen::AUTHNAME,
            crate::postgres::PointerGetDatum(*newval as *const c_void));
        if role_tup.is_null() { return false; }
        let roleform = crate::access::htup_details::GETSTRUCT(role_tup as _) as crate::catalog::pg_authid::Form_pg_authid;
        roleid = (*roleform).oid;
        is_superuser = (*roleform).rolsuper;
        crate::utils::cache::syscache::ReleaseSysCache(role_tup);
        let auth = crate::utils::init::miscinit::GetAuthenticatedUserId();
        if roleid != auth && !crate::utils::misc::superuser::superuser_arg(auth) { return false; }
    }
    let myextra = libc::malloc(core::mem::size_of::<pdb_role_auth_extra>()) as *mut pdb_role_auth_extra;
    if myextra.is_null() { return false; }
    (*myextra).roleid = roleid;
    (*myextra).is_superuser = is_superuser;
    *extra = myextra as *mut c_void;
    if std::env::var_os("PDB_AUTH").is_some() { eprintln!("PDB_AUTH check_session_authorization OK roleid={} super={}", roleid, is_superuser); }
    true
}
#[no_mangle] pub unsafe extern "C" fn check_ssl(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_stage_log_stats(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_subtrans_buffers(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_synchronized_standby_slots(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_synchronous_standby_names(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_temp_buffers(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_temp_tablespaces(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_timezone(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_timezone_abbreviations(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_transaction_buffers(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_transaction_deferrable(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_transaction_isolation(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_transaction_read_only(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_vacuum_buffer_usage_limit(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_wal_buffers(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_wal_consistency_checking(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub unsafe extern "C" fn check_wal_segment_size(newval: *mut c_void, extra: *mut c_void, source: *mut c_void) -> bool { true }
#[no_mangle] pub static mut cpu_operator_cost: c_double = 0.0;
#[no_mangle] pub unsafe extern "C" fn show_archive_command() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_data_directory_mode() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_in_hot_standby() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_log_file_mode() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_log_timezone() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_role() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_tcp_keepalives_count() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_tcp_keepalives_idle() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_tcp_keepalives_interval() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_tcp_user_timeout() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_timezone() -> *const c_void { core::ptr::null() }
#[no_mangle] pub unsafe extern "C" fn show_unix_socket_permissions() -> *const c_void { core::ptr::null() }

// ---- enum option-list arrays (xlog.rs unwired; provide real symbols here) ----
use crate::utils::misc::guc::config_enum_entry;

// ---- shmem-init boot link symbols (BufferManagerShmemInit etc.) ----
// Runtime-set shmem pointer globals: only `extern "C" { static }` declarations exist
// in the codebase (buf_internals.rs, shmem.rs, walsender_private.rs, aio_internal.rs).
// Provide the single canonical definition here. Opaque pointer targets normalized to
// c_void (ABI-equivalent). BackendWritebackContext is struct-valued so it keeps its
// real type, zero-initialized.
#[no_mangle] pub static mut BufferDescriptors: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut BufferIOCVArray: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut CkptBufferIds: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut ShmemIndexLock: *mut c_void = core::ptr::null_mut();
// Canonical individual (builtin) LWLock name globals. Each is assigned at
// runtime by InitializeBuiltinLWLockPointers() to &MainLWLockArray[id].lock.
#[no_mangle] pub static mut OidGenLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut XidGenLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut ProcArrayLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut SInvalReadLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut SInvalWriteLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut WALBufMappingLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut WALWriteLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut ControlFileLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut MultiXactGenLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut RelCacheInitLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut CheckpointerCommLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut TwoPhaseStateLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut TablespaceCreateLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut BtreeVacuumLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut AddinShmemInitLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut AutovacuumLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut AutovacuumScheduleLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut SyncScanLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut RelationMappingLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut NotifyQueueLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut SerializableXactHashLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut SerializableFinishedListLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut SerializablePredicateListLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut SyncRepLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut BackgroundWorkerLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut DynamicSharedMemoryControlLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut AutoFileLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut ReplicationSlotAllocationLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut ReplicationSlotControlLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut CommitTsLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut ReplicationOriginLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut MultiXactTruncationLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut LogicalRepWorkerLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut XactTruncationLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut WrapLimitsVacuumLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut NotifyQueueTailLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut WaitEventCustomLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut WALSummarizerLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut DSMRegistryLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut InjectionPointLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut SerialControlLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut AioWorkerSubmissionQueueLock: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut WalSndCtl: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut pgaio_ctl: *mut c_void = core::ptr::null_mut();
#[no_mangle] pub static mut pgaio_method_ops: *const c_void = &crate::storage::aio::method_sync::pgaio_sync_ops as *const _ as *const c_void;
#[no_mangle] pub static mut BackendWritebackContext: crate::storage::buf_internals::WritebackContext =
    crate::storage::buf_internals::WritebackContext {
        max_pending: core::ptr::null_mut(),
        nr_pending: 0,
        pending_writebacks: [crate::storage::buf_internals::PendingWriteback {
            tag: crate::storage::buf_internals::BufferTag {
                spcOid: 0, dbOid: 0, relNumber: 0, forkNum: 0, blockNum: 0,
            },
        }; crate::storage::buf_internals::WRITEBACK_MAX_PENDING_FLUSHES],
    };

// ---- shmem-init boot link symbols: function forwarders ----
// Real impls live in the named modules but are not #[no_mangle]; some have non-C
// (generic/&-ref) signatures. Forward through C-ABI shims, casting pointers with `as _`.
#[no_mangle] pub unsafe extern "C" fn ShmemInitStruct(name: *const c_void, size: usize, found_ptr: *mut c_void) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(name as *const c_char, size, found_ptr as *mut bool)
}
#[no_mangle] pub unsafe extern "C" fn add_size(s1: usize, s2: usize) -> usize { crate::storage::ipc::shmem::add_size(s1, s2) }
#[no_mangle] pub unsafe extern "C" fn mul_size(s1: usize, s2: usize) -> usize { crate::storage::ipc::shmem::mul_size(s1, s2) }
#[no_mangle] pub unsafe extern "C" fn MAXALIGN(len: usize) -> usize { crate::c::MAXALIGN(len) }
#[no_mangle] pub unsafe extern "C" fn dlist_init(head: *mut c_void) { crate::lib::ilist::dlist_init(head as _) }
#[no_mangle] pub unsafe extern "C" fn dclist_init(head: *mut c_void) { crate::lib::ilist::dclist_init(head as _) }
#[no_mangle] pub unsafe extern "C" fn dclist_push_head(head: *mut c_void, node: *mut c_void) { crate::lib::ilist::dclist_push_head(head as _, node as _) }
#[no_mangle] pub unsafe extern "C" fn pg_atomic_init_u32(ptr: *mut c_void, val: u32) {
    crate::port::atomics::pg_atomic_init_u32_impl(&*(ptr as *const crate::port::atomics::pg_atomic_uint32), val)
}
#[no_mangle] pub unsafe extern "C" fn pg_atomic_init_flag(ptr: *mut c_void) {
    crate::port::atomics::pg_atomic_init_flag_impl(&*(ptr as *const crate::port::atomics::pg_atomic_flag))
}
// macOS has sigsetjmp (not Linux's __sigsetjmp); the prompt-cancellation path that
// references it is unused server-side, so a first-return stub satisfies the linker.
#[no_mangle] pub unsafe extern "C" fn __sigsetjmp(_env: *mut c_void, _savemask: c_int) -> c_int { 0 }
// storage/ipc.h PG_ENSURE_ERROR_CLEANUP/PG_END_ENSURE_ERROR_CLEANUP: TRY/FINALLY wrappers
// used by the index bulk-build path (not exercised during relcache init). Linking stubs.
#[no_mangle] pub unsafe extern "C" fn PG_ENSURE_ERROR_CLEANUP(_f: *const c_void, _arg: usize) {}
#[no_mangle] pub unsafe extern "C" fn PG_END_ENSURE_ERROR_CLEANUP(_f: *const c_void, _arg: usize) {}

// macOS equivalents for Linux-only libc symbols referenced by pg_locale_libc.
#[no_mangle] pub unsafe extern "C" fn __errno_location() -> *mut c_int { libc::__error() }
#[no_mangle] pub unsafe extern "C" fn gnu_get_libc_version() -> *const c_char { c"2.38".as_ptr() }

// GetSysCacheOid* are declared `extern "C"` in catalog/namespace.rs; provide the symbols.
#[no_mangle] pub unsafe extern "C" fn GetSysCacheOid2(cache_id: c_int, oid_col: c_int, key1: Datum, key2: Datum) -> Oid {
    crate::utils::cache::lsyscache::GetSysCacheOid2(cache_id, oid_col as i16, key1, key2)
}

#[no_mangle] pub unsafe extern "C" fn GetSysCacheOid3(cache_id: c_int, oid_col: c_int, key1: Datum, key2: Datum, key3: Datum) -> Oid {
    crate::utils::cache::syscache::GetSysCacheOid(cache_id, oid_col as i16, key1, key2, key3, 0 as Datum)
}
#[no_mangle] pub unsafe extern "C" fn SearchSysCache3(cache_id: c_int, key1: Datum, key2: Datum, key3: Datum) -> crate::access::htup_details::HeapTuple {
    crate::utils::cache::syscache::SearchSysCache3(cache_id, key1, key2, key3)
}
#[no_mangle] pub unsafe extern "C" fn GetDatabaseEncoding() -> c_int {
    crate::utils::mb::mbutils::GetDatabaseEncoding()
}
#[no_mangle] pub unsafe extern "C" fn GetDatabaseEncodingName() -> *const c_char {
    crate::utils::mb::mbutils::GetDatabaseEncodingName()
}
#[no_mangle] pub unsafe extern "C" fn is_encoding_supported_by_icu(encoding: c_int) -> bool {
    crate::common::encnames::is_encoding_supported_by_icu(encoding)
}

#[no_mangle] pub unsafe extern "C" fn get_func_arg_info(proc_tup: *mut c_void, p_argtypes: *mut *mut Oid, p_argnames: *mut *mut *mut c_char, p_argmodes: *mut *mut c_char) -> c_int {
    crate::utils::fmgr::funcapi::get_func_arg_info(proc_tup as _, p_argtypes, p_argnames, p_argmodes)
}

// parse_utilcmd dependency surface (reachable via transformCreateStmt)
#[no_mangle] pub unsafe extern "C" fn CommandCounterIncrement() { crate::access::transam::xact::CommandCounterIncrement() }
#[no_mangle] pub unsafe extern "C" fn GetCurrentSubTransactionId() -> u32 { crate::access::transam::xact::GetCurrentSubTransactionId() }
#[no_mangle] pub unsafe extern "C" fn IsParallelWorker() -> bool { false }
#[no_mangle] pub unsafe extern "C" fn LockDatabaseObject(classid: Oid, objid: Oid, sub: u16, lockmode: c_int) { crate::catalog::objectaddress_impl::LockDatabaseObject(classid, objid, sub, lockmode) }
#[no_mangle] pub unsafe extern "C" fn UnlockDatabaseObject(classid: Oid, objid: Oid, sub: u16, lockmode: c_int) { crate::catalog::objectaddress_impl::UnlockDatabaseObject(classid, objid, sub, lockmode) }
#[no_mangle] pub unsafe extern "C" fn get_object_attnum_oid(class_id: Oid) -> i16 { crate::catalog::objectaddress_impl::get_object_attnum_oid(class_id) }
#[no_mangle] pub static mut MyXactFlags: c_int = 0;
#[no_mangle] pub static XACT_FLAGS_ACCESSEDTEMPNAMESPACE: c_int = 1;

#[no_mangle] pub unsafe extern "C" fn get_object_oid_index(class_id: Oid) -> Oid { crate::catalog::objectaddress_impl::get_object_oid_index(class_id) }
#[no_mangle] pub unsafe extern "C" fn get_rel_relkind(relid: Oid) -> c_char { crate::utils::cache::lsyscache::get_rel_relkind(relid) }
#[no_mangle] pub unsafe extern "C" fn get_relkind_objtype(relkind: c_char) -> c_int { crate::catalog::objectaddress_impl::get_relkind_objtype(relkind) as c_int }
#[no_mangle] pub unsafe extern "C" fn performDeletion(object: *const c_void, behavior: c_int, flags: c_int) { crate::catalog::dependency::performDeletion(object as _, core::mem::transmute(behavior), flags) }

#[no_mangle] pub unsafe extern "C" fn RELATION_IS_LOCAL(_rel: *mut c_void) -> bool { false }
#[no_mangle] pub unsafe extern "C" fn pgstat_progress_update_param(_index: c_int, _val: i64) {}

// commands/analyze.rs -> commands/vacuum.rs + access/relation.rs (wired 2026-06-20)
#[no_mangle] pub unsafe extern "C" fn vacuum_open_relation(relid: Oid, relation: *mut c_void, options: c_int, verbose: bool, lmode: c_int) -> *mut c_void { crate::commands::vacuum::vacuum_open_relation(relid, relation as _, options as _, verbose, lmode as _) as _ }
#[no_mangle] pub unsafe extern "C" fn vacuum_is_permitted_for_relation(relid: Oid, classForm: *mut c_void, options: c_int) -> bool { crate::commands::vacuum::vacuum_is_permitted_for_relation(relid, classForm as _, options as _) }
#[no_mangle] pub unsafe extern "C" fn relation_close(relation: *mut c_void, lockmode: c_int) { crate::access::common::relation::relation_close(relation as _, lockmode as _) }
#[no_mangle] pub unsafe extern "C" fn RELATION_IS_OTHER_TEMP(relation: *mut c_void) -> bool { let rel = relation as *mut crate::utils::rel::RelationData; (*(*rel).rd_rel).relpersistence == b't' as c_char && !(*rel).rd_islocaltemp }
// access/nbtree/nbtutils.rs -> access/common/indextuple.rs (wired 2026-06-20)
#[no_mangle] pub unsafe extern "C" fn index_truncate_tuple(tupdesc: *mut c_void, itup: *mut c_void, newnatts: c_int) -> *mut c_void { crate::access::common::indextuple::index_truncate_tuple(tupdesc as _, itup as _, newnatts) as _ }
// catalog/pg_shdepend.rs (wired 2026-06-20, type/acl path)
#[no_mangle] pub unsafe extern "C" fn updateAclDependencies(classId: Oid, objectId: Oid, objectSubId: c_int, ownerId: Oid, noldmembers: c_int, oldmembers: *mut Oid, nnewmembers: c_int, newmembers: *mut Oid) { crate::catalog::pg_shdepend::updateAclDependencies(classId, objectId, objectSubId, ownerId, noldmembers, oldmembers as _, nnewmembers, newmembers as _) }

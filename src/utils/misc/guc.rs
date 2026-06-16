//--------------------------------------------------------------------
// guc.c -> guc.rs
//
// Support for grand unified configuration scheme, including SET
// command, configuration file, and command line options.
//
// This file contains the generic option processing infrastructure.
// guc_funcs.c contains SQL-level functionality, including SET/SHOW
// commands and various system-administration SQL functions.
// guc_tables.c contains the arrays that define all the built-in
// GUC variables.  Code that implements variable-specific behavior
// is scattered around the system in check, assign, and show hooks.
//
// See src/backend/utils/misc/README for more information.
//
//
// Copyright (c) 2000-2025, PostgreSQL Global Development Group
// Written by Peter Eisentraut <peter_e@gmx.net>.
//
// IDENTIFICATION
//   src/backend/utils/misc/guc.c
//
//--------------------------------------------------------------------

use crate::prelude::*;

use std::ffi::{c_char, c_double, c_int, c_uint, c_void};
use std::ptr;

// lib/ilist types
use crate::lib::ilist::{
    dlist_delete, dlist_head, dlist_iter, dlist_mutable_iter, dlist_node, dlist_push_tail,
    slist_head, slist_node,
};

// palloc / MemoryContext
// AllocSetContextCreate, MemoryContextAllocExtended, MemoryContextAllocZero,
// MemoryContextSwitchTo, TopMemoryContext, ALLOCSET_DEFAULT_SIZES come from prelude.
use crate::utils::palloc::MCXT_ALLOC_NO_OOM;
use crate::utils::mmgr::mcxt::TopTransactionContext;

// List type
use crate::nodes::pg_list::{lappend, list_free, List, NIL};

// dynahash
use crate::utils::hash::dynahash::{
    hash_create, hash_get_num_entries, hash_search, hash_seq_init, hash_seq_search,
    HASH_COMPARE, HASH_CONTEXT, HASH_ELEM, HASH_ENTER, HASH_ENTER_NULL, HASH_FIND,
    HASH_FUNCTION, HASH_REMOVE, HASH_SEQ_STATUS, HASHCTL, HTAB,
};

// Oid - from postgres_ext (already in prelude, but explicit for clarity)
use crate::postgres_ext::Oid;

// StringInfo
use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoString, initStringInfo, resetStringInfo,
    StringInfoData,
};

// MemoryContext type alias - from palloc (already in prelude)

// ---------------------------------------------------------------------------
// Merged from utils/guc.h
// ---------------------------------------------------------------------------

/// Maximum for integer GUC variables measured in kilobytes of memory.
#[cfg(target_pointer_width = "64")]
pub const MAX_KILOBYTES: c_int = c_int::MAX;
#[cfg(not(target_pointer_width = "64"))]
pub const MAX_KILOBYTES: c_int = c_int::MAX / 1024;

/// Automatic configuration file name for ALTER SYSTEM.
pub const PG_AUTOCONF_FILENAME: &str = "postgresql.auto.conf\0";

/// Context required to set a variable.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GucContext {
    PGC_INTERNAL,
    PGC_POSTMASTER,
    PGC_SIGHUP,
    PGC_SU_BACKEND,
    PGC_BACKEND,
    PGC_SUSET,
    PGC_USERSET,
}

/// Source of the current setting.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum GucSource {
    PGC_S_DEFAULT,           // hard-wired default ("boot_val")
    PGC_S_DYNAMIC_DEFAULT,   // default computed during initialization
    PGC_S_ENV_VAR,           // postmaster environment variable
    PGC_S_FILE,              // postgresql.conf
    PGC_S_ARGV,              // postmaster command line
    PGC_S_GLOBAL,            // global in-database setting
    PGC_S_DATABASE,          // per-database setting
    PGC_S_USER,              // per-user setting
    PGC_S_DATABASE_USER,     // per-user-and-database setting
    PGC_S_CLIENT,            // from client connection request
    PGC_S_OVERRIDE,          // special case to forcibly set default
    PGC_S_INTERACTIVE,       // dividing line for error reporting
    PGC_S_TEST,              // test per-database or per-user setting
    PGC_S_SESSION,           // SET command
}

/// Types of set_config_option actions.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum GucAction {
    GUC_ACTION_SET,    // regular SET command
    GUC_ACTION_LOCAL,  // SET LOCAL command
    GUC_ACTION_SAVE,   // function SET option, or temp assignment
}

/// Separator between namespace and name in a qualified GUC name.
pub const GUC_QUALIFIER_SEPARATOR: u8 = b'.';

// Bit values in "flags" field of a GUC variable.
pub const GUC_LIST_INPUT: c_int          = 0x000001; // input can be list format
pub const GUC_LIST_QUOTE: c_int          = 0x000002; // double-quote list elements
pub const GUC_NO_SHOW_ALL: c_int         = 0x000004; // exclude from SHOW ALL
pub const GUC_NO_RESET: c_int            = 0x000008; // disallow RESET and SAVE
pub const GUC_NO_RESET_ALL: c_int        = 0x000010; // exclude from RESET ALL
pub const GUC_EXPLAIN: c_int             = 0x000020; // include in EXPLAIN
pub const GUC_REPORT: c_int              = 0x000040; // auto-report changes to client
pub const GUC_NOT_IN_SAMPLE: c_int       = 0x000080; // not in postgresql.conf.sample
pub const GUC_DISALLOW_IN_FILE: c_int    = 0x000100; // can't set in postgresql.conf
pub const GUC_CUSTOM_PLACEHOLDER: c_int  = 0x000200; // placeholder for custom variable
pub const GUC_SUPERUSER_ONLY: c_int      = 0x000400; // show only to superusers
pub const GUC_IS_NAME: c_int             = 0x000800; // limit string to NAMEDATALEN-1
pub const GUC_NOT_WHILE_SEC_REST: c_int  = 0x001000; // can't set if security restricted
pub const GUC_DISALLOW_IN_AUTO_FILE: c_int = 0x002000; // can't set in PG_AUTOCONF_FILENAME
pub const GUC_RUNTIME_COMPUTED: c_int    = 0x004000; // delay processing in 'postgres -C'
pub const GUC_ALLOW_IN_PARALLEL: c_int   = 0x008000; // allow setting in parallel mode

pub const GUC_UNIT_KB: c_int             = 0x01000000; // value is in kilobytes
pub const GUC_UNIT_BLOCKS: c_int         = 0x02000000; // value is in blocks
pub const GUC_UNIT_XBLOCKS: c_int        = 0x03000000; // value is in xlog blocks
pub const GUC_UNIT_MB: c_int             = 0x04000000; // value is in megabytes
pub const GUC_UNIT_BYTE: c_int           = 0x05000000; // value is in bytes
pub const GUC_UNIT_MEMORY: c_int         = 0x0F000000; // mask for size-related units

pub const GUC_UNIT_MS: c_int             = 0x10000000; // value is in milliseconds
pub const GUC_UNIT_S: c_int              = 0x20000000; // value is in seconds
pub const GUC_UNIT_MIN: c_int            = 0x30000000; // value is in minutes
pub const GUC_UNIT_TIME: c_int           = 0x70000000; // mask for time-related units

pub const GUC_UNIT: c_int = GUC_UNIT_MEMORY | GUC_UNIT_TIME;

// ---------------------------------------------------------------------------
// Merged from utils/guc_tables.h
// ---------------------------------------------------------------------------

/// Supported GUC variable types.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum config_type {
    PGC_BOOL,
    PGC_INT,
    PGC_REAL,
    PGC_STRING,
    PGC_ENUM,
}

/// Union of all possible GUC values.
#[repr(C)]
pub union config_var_val {
    pub boolval:   bool,
    pub intval:    c_int,
    pub realval:   c_double,
    pub stringval: *mut c_char,
    pub enumval:   c_int,
}

impl Default for config_var_val {
    fn default() -> Self { config_var_val { intval: 0 } }
}

/// Value + opaque extra struct for check/assign hooks.
#[repr(C)]
pub struct config_var_value {
    pub val:   config_var_val,
    pub extra: *mut c_void,
}

impl Default for config_var_value {
    fn default() -> Self {
        config_var_value { val: Default::default(), extra: ptr::null_mut() }
    }
}

/// Groupings for display in pg_settings.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum config_group {
    UNGROUPED,
    FILE_LOCATIONS,
    CONN_AUTH_SETTINGS,
    CONN_AUTH_TCP,
    CONN_AUTH_AUTH,
    CONN_AUTH_SSL,
    RESOURCES_MEM,
    RESOURCES_DISK,
    RESOURCES_KERNEL,
    RESOURCES_BGWRITER,
    RESOURCES_IO,
    RESOURCES_WORKER_PROCESSES,
    WAL_SETTINGS,
    WAL_CHECKPOINTS,
    WAL_ARCHIVING,
    WAL_RECOVERY,
    WAL_ARCHIVE_RECOVERY,
    WAL_RECOVERY_TARGET,
    WAL_SUMMARIZATION,
    REPLICATION_SENDING,
    REPLICATION_PRIMARY,
    REPLICATION_STANDBY,
    REPLICATION_SUBSCRIBERS,
    QUERY_TUNING_METHOD,
    QUERY_TUNING_COST,
    QUERY_TUNING_GEQO,
    QUERY_TUNING_OTHER,
    LOGGING_WHERE,
    LOGGING_WHEN,
    LOGGING_WHAT,
    PROCESS_TITLE,
    STATS_MONITORING,
    STATS_CUMULATIVE,
    VACUUM_AUTOVACUUM,
    VACUUM_COST_DELAY,
    VACUUM_DEFAULT,
    VACUUM_FREEZING,
    CLIENT_CONN_STATEMENT,
    CLIENT_CONN_LOCALE,
    CLIENT_CONN_PRELOAD,
    CLIENT_CONN_OTHER,
    LOCK_MANAGEMENT,
    COMPAT_OPTIONS_PREVIOUS,
    COMPAT_OPTIONS_OTHER,
    ERROR_HANDLING_OPTIONS,
    PRESET_OPTIONS,
    CUSTOM_OPTIONS,
    DEVELOPER_OPTIONS,
}

/// Stack state for transactional GUC changes.
#[repr(C)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum GucStackState {
    GUC_SAVE,       // entry caused by function SET option
    GUC_SET,        // entry caused by plain SET command
    GUC_LOCAL,      // entry caused by SET LOCAL command
    GUC_SET_LOCAL,  // entry caused by SET then SET LOCAL
}

/// Per-transaction stack entry for GUC variables.
#[repr(C)]
pub struct GucStack {
    pub prev:            *mut GucStack,
    pub nest_level:      c_int,
    pub state:           GucStackState,
    pub source:          GucSource,
    pub scontext:        GucContext,
    pub masked_scontext: GucContext,
    pub srole:           Oid,
    pub masked_srole:    Oid,
    pub prior:           config_var_value,
    pub masked:          config_var_value,
}

// bit values in status field of config_generic
pub const GUC_IS_IN_FILE:      c_int = 0x0001; // found it in config file
pub const GUC_PENDING_RESTART: c_int = 0x0002; // changed value cannot be applied yet
pub const GUC_NEEDS_REPORT:    c_int = 0x0004; // new value must be reported to client

/// Generic fields applicable to all types of GUC variables.
#[repr(C)]
pub struct config_generic {
    // constant fields, must be set correctly in initial value:
    pub name:         *const c_char,  // name of variable - MUST BE FIRST
    pub context:      GucContext,
    pub group:        config_group,
    pub short_desc:   *const c_char,
    pub long_desc:    *const c_char,
    pub flags:        c_int,
    // variable fields, initialized at runtime:
    pub vartype:      config_type,
    pub status:       c_int,
    pub source:       GucSource,
    pub reset_source: GucSource,
    pub scontext:     GucContext,
    pub reset_scontext: GucContext,
    pub srole:        Oid,
    pub reset_srole:  Oid,
    pub stack:        *mut GucStack,
    pub extra:        *mut c_void,
    pub nondef_link:  dlist_node,
    pub stack_link:   slist_node,
    pub report_link:  slist_node,
    pub last_reported: *mut c_char,
    pub sourcefile:   *mut c_char,
    pub sourceline:   c_int,
}

/// Hook function typedefs (per-variable check/assign/show).
pub type GucBoolCheckHook   = Option<unsafe extern "C" fn(newval: *mut bool, extra: *mut *mut c_void, source: GucSource) -> bool>;
pub type GucIntCheckHook    = Option<unsafe extern "C" fn(newval: *mut c_int, extra: *mut *mut c_void, source: GucSource) -> bool>;
pub type GucRealCheckHook   = Option<unsafe extern "C" fn(newval: *mut c_double, extra: *mut *mut c_void, source: GucSource) -> bool>;
pub type GucStringCheckHook = Option<unsafe extern "C" fn(newval: *mut *mut c_char, extra: *mut *mut c_void, source: GucSource) -> bool>;
pub type GucEnumCheckHook   = Option<unsafe extern "C" fn(newval: *mut c_int, extra: *mut *mut c_void, source: GucSource) -> bool>;

pub type GucBoolAssignHook   = Option<unsafe extern "C" fn(newval: bool, extra: *mut c_void)>;
pub type GucIntAssignHook    = Option<unsafe extern "C" fn(newval: c_int, extra: *mut c_void)>;
pub type GucRealAssignHook   = Option<unsafe extern "C" fn(newval: c_double, extra: *mut c_void)>;
pub type GucStringAssignHook = Option<unsafe extern "C" fn(newval: *const c_char, extra: *mut c_void)>;
pub type GucEnumAssignHook   = Option<unsafe extern "C" fn(newval: c_int, extra: *mut c_void)>;

pub type GucShowHook = Option<unsafe extern "C" fn() -> *const c_char>;

/// Config struct for bool variables.
#[repr(C)]
pub struct config_bool {
    pub gen:          config_generic,
    pub variable:     *mut bool,
    pub boot_val:     bool,
    pub check_hook:   GucBoolCheckHook,
    pub assign_hook:  GucBoolAssignHook,
    pub show_hook:    GucShowHook,
    pub reset_val:    bool,
    pub reset_extra:  *mut c_void,
}

/// Config struct for integer variables.
#[repr(C)]
pub struct config_int {
    pub gen:          config_generic,
    pub variable:     *mut c_int,
    pub boot_val:     c_int,
    pub min:          c_int,
    pub max:          c_int,
    pub check_hook:   GucIntCheckHook,
    pub assign_hook:  GucIntAssignHook,
    pub show_hook:    GucShowHook,
    pub reset_val:    c_int,
    pub reset_extra:  *mut c_void,
}

/// Config struct for floating-point variables.
#[repr(C)]
pub struct config_real {
    pub gen:          config_generic,
    pub variable:     *mut c_double,
    pub boot_val:     c_double,
    pub min:          c_double,
    pub max:          c_double,
    pub check_hook:   GucRealCheckHook,
    pub assign_hook:  GucRealAssignHook,
    pub show_hook:    GucShowHook,
    pub reset_val:    c_double,
    pub reset_extra:  *mut c_void,
}

/// Config struct for string variables.
#[repr(C)]
pub struct config_string {
    pub gen:          config_generic,
    pub variable:     *mut *mut c_char,
    pub boot_val:     *const c_char,
    pub check_hook:   GucStringCheckHook,
    pub assign_hook:  GucStringAssignHook,
    pub show_hook:    GucShowHook,
    pub reset_val:    *mut c_char,
    pub reset_extra:  *mut c_void,
}

/// One entry in an enum GUC's options table.
#[repr(C)]
pub struct config_enum_entry {
    pub name:   *const c_char,
    pub val:    c_int,
    pub hidden: bool,
}

/// Config struct for enum variables.
#[repr(C)]
pub struct config_enum {
    pub gen:          config_generic,
    pub variable:     *mut c_int,
    pub boot_val:     c_int,
    pub options:      *const config_enum_entry,
    pub check_hook:   GucEnumCheckHook,
    pub assign_hook:  GucEnumAssignHook,
    pub show_hook:    GucShowHook,
    pub reset_val:    c_int,
    pub reset_extra:  *mut c_void,
}

/// Opaque handle type returned by get_config_handle().
pub type config_handle = config_generic;

// ---------------------------------------------------------------------------
// ConfigVariable (from conffiles / guc.h)
// ---------------------------------------------------------------------------

/// Name/value pair from a configuration file.
#[repr(C)]
pub struct ConfigVariable {
    pub name:       *mut c_char,
    pub value:      *mut c_char,
    pub errmsg:     *mut c_char,
    pub filename:   *mut c_char,
    pub sourceline: c_int,
    pub ignore:     bool,
    pub applied:    bool,
    pub next:       *mut ConfigVariable,
}

// ---------------------------------------------------------------------------
// External declarations for dependencies not yet ported (TODO stubs)
// ---------------------------------------------------------------------------

extern "C" {
    // conffiles.c / guc-file.l output
    fn ParseConfigFile(
        config_file: *const c_char,
        strict: bool,
        calling_file: *const c_char,
        calling_lineno: c_int,
        depth: c_int,
        elevel: c_int,
        head_p: *mut *mut ConfigVariable,
        tail_p: *mut *mut ConfigVariable,
    ) -> bool;
    fn ParseConfigFp(
        fp: *mut c_void,
        config_file: *const c_char,
        depth: c_int,
        elevel: c_int,
        head_p: *mut *mut ConfigVariable,
        tail_p: *mut *mut ConfigVariable,
    ) -> bool;
    fn FreeConfigVariables(list: *mut ConfigVariable);

    // guc_internal.h
    fn record_config_file_error(
        msg: *const c_char,
        filename: *const c_char,
        lineno: c_int,
        head_p: *mut *mut ConfigVariable,
        tail_p: *mut *mut ConfigVariable,
    );
    fn guc_name_compare(namea: *const c_char, nameb: *const c_char) -> c_int;

    // builtins.h / identifier
    fn truncate_identifier(ident: *mut c_char, len: usize, warn: bool);
    fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn escape_single_quotes_ascii(src: *const c_char) -> *mut c_char;
    fn parse_bool(value: *const c_char, result: *mut bool) -> bool;

    // miscadmin.h
    fn IsBootstrapProcessingMode() -> bool;
    fn IsUnderPostmaster() -> bool;
    fn IsInParallelMode() -> bool;
    fn InLocalUserIdChange() -> bool;
    fn InSecurityRestrictedOperation() -> bool;
    fn RecoveryInProgress() -> bool;
    fn GetUserId() -> Oid;
    fn superuser() -> bool;
    fn SetDataDir(path: *const c_char);
    fn make_absolute_path(path: *const c_char) -> *mut c_char;

    // access/xact.h
    fn GetCurrentTimestamp() -> i64;

    // storage/lwlock.h
    fn LWLockAcquire(lock: c_int, mode: c_int);
    fn LWLockRelease(lock: c_int);

    // storage/fd.h
    fn AllocateFile(name: *const c_char, mode: *const c_char) -> *mut c_void;
    fn FreeFile(file: *mut c_void) -> c_int;
    fn BasicOpenFile(filename: *const c_char, fileflags: c_int) -> c_int;
    fn durable_rename(oldpath: *const c_char, newpath: *const c_char, elevel: c_int) -> c_int;
    fn pg_fsync(fd: c_int) -> c_int;

    // utils/acl.h
    fn pg_parameter_aclcheck(name: *const c_char, roleid: Oid, acl_mode: c_int) -> c_int;

    // catalog/objectaccess.h
    fn InvokeObjectPostAlterHookArgStr(
        classId: Oid,
        objName: *const c_char,
        subId: c_int,
        stmtType: c_int,
        is_internal: bool,
    );

    // pg_timezone.h
    fn pg_timezone_initialize();

    // tcop/tcopprot.h
    fn whereToSendOutput() -> c_int;

    // pqformat.h
    fn pq_beginmessage(buf: *mut StringInfoData, msg_type: u8);
    fn pq_sendstring(buf: *mut StringInfoData, str_: *const c_char);
    fn pq_endmessage(buf: *mut StringInfoData);

    // utils/memutils.h
    fn repalloc_extended(pointer: *mut c_void, size: usize, flags: c_int) -> *mut c_void;
    fn GetMemoryChunkContext(pointer: *mut c_void) -> MemoryContext;

    // port / utils
    fn get_stack_depth_rlimit() -> isize;
    fn pg_rotate_left32(word: u32, n: c_int) -> u32;
    fn add_size(s1: usize, s2: usize) -> usize;

    // utils/builtins.h
    fn TextDatumGetCString(datum: usize) -> *mut c_char;
    fn CStringGetTextDatum(s: *const c_char) -> usize;
    fn psprintf(fmt: *const c_char, ...) -> *mut c_char;
    fn pstrdup(s: *const c_char) -> *mut c_char;
    fn pfree(ptr: *mut c_void);
    fn palloc(size: usize) -> *mut c_void;
    fn MemoryContextAllocZero(context: MemoryContext, size: usize) -> *mut c_void;

    // array / text
    fn array_ref(
        array: *mut c_void,
        nSubscripts: c_int,
        indx: *const c_int,
        arraytyplen: c_int,
        elmlen: c_int,
        elmbyval: bool,
        elmalign: u8,
        isNull: *mut bool,
    ) -> usize;
    fn array_set(
        array: *mut c_void,
        nSubscripts: c_int,
        indx: *const c_int,
        dataValue: usize,
        isNull: bool,
        arraytyplen: c_int,
        elmlen: c_int,
        elmbyval: bool,
        elmalign: u8,
    ) -> *mut c_void;
    fn construct_array_builtin(elems: *const usize, nelems: c_int, elmtype: Oid) -> *mut c_void;

    // parser/scansup.h
    fn ExtractSetVariableArgs(stmt: *mut c_void) -> *mut c_char;

    // libc
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strcspn(s: *const c_char, reject: *const c_char) -> usize;
    fn strlcpy(dst: *mut c_char, src: *const c_char, n: usize) -> usize;
    fn isspace(c: c_int) -> c_int;
    fn isnan(x: c_double) -> c_int;
    fn abs(x: c_int) -> c_int;
    fn rint(x: c_double) -> c_double;
    fn fabs(x: c_double) -> c_double;
    fn strtol(nptr: *const c_char, endptr: *mut *mut c_char, base: c_int) -> i64;
    fn strtod(nptr: *const c_char, endptr: *mut *mut c_char) -> c_double;
    fn vsnprintf(s: *mut c_char, n: usize, fmt: *const c_char, args: *mut c_void) -> c_int;
    fn stat(path: *const c_char, buf: *mut c_void) -> c_int;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn close(fd: c_int) -> c_int;
    fn unlink(pathname: *const c_char) -> c_int;
    fn rename(oldpath: *const c_char, newpath: *const c_char) -> c_int;
    fn fgetc(stream: *mut c_void) -> c_int;
    fn fprintf(stream: *mut c_void, fmt: *const c_char, ...) -> c_int;
    fn fputc(c: c_int, stream: *mut c_void) -> c_int;
    fn fwrite(ptr: *const c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize;
    fn fread(ptr: *mut c_void, size: usize, nmemb: usize, stream: *mut c_void) -> usize;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn malloc(size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
    fn getenv(name: *const c_char) -> *mut c_char;
    fn errno() -> c_int;
    fn __error() -> *mut c_int;
    fn strerror(errnum: c_int) -> *mut c_char;
}

#[inline]
unsafe fn set_errno(e: c_int) {
    *__error() = e;
}

/// Render the current errno as a String, for use where C wrote "%m".
unsafe fn strerror_string() -> String {
    std::ffi::CStr::from_ptr(strerror(*__error())).to_string_lossy().into_owned()
}

/// Borrow a NUL-terminated C string as a byte slice (without the NUL).
#[inline]
unsafe fn cstr_bytes<'a>(p: *const c_char) -> &'a [u8] {
    std::ffi::CStr::from_ptr(p).to_bytes()
}

/// TODO(pg-port): errcontext() from utils/elog.h; emits an error-context line.
macro_rules! errcontext {
    ($($arg:tt)*) => {{ let _ = format!($($arg)*); }};
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CONFIG_FILENAME: &[u8]    = b"postgresql.conf\0";
const HBA_FILENAME: &[u8]       = b"pg_hba.conf\0";
const IDENT_FILENAME: &[u8]     = b"pg_ident.conf\0";

// exec_backend feature not used in this port; constants retained as dead code
#[cfg(any())]
const CONFIG_EXEC_PARAMS: &[u8]     = b"global/config_exec_params\0";
#[cfg(any())]
const CONFIG_EXEC_PARAMS_NEW: &[u8] = b"global/config_exec_params.new\0";

/// Precision with which REAL type guc values are printed for GUC serialization.
const REALTYPE_PRECISION: usize = 17;

/// Safe search path when executing code as the table owner.
const GUC_SAFE_SEARCH_PATH: &[u8] = b"pg_catalog, pg_temp\0";

// Block sizes (normally set by configure / pg_config.h).
const BLCKSZ: usize = 8192;
const XLOG_BLCKSZ: usize = 8192;

// STACK_DEPTH_SLOP from miscadmin.h
const STACK_DEPTH_SLOP: isize = 512 * 1024;

// AclResult constants (from utils/acl.h)
const ACLCHECK_OK: c_int = 0;

// LW lock modes
const LW_EXCLUSIVE: c_int = 0;

// DestRemote (from tcop/dest.h)
const DEST_REMOTE: c_int = 1;

// Oid constants
const BOOTSTRAP_SUPERUSERID: Oid = 10;
const TEXTOID: Oid = 25;
const ParameterAclRelationId: Oid = 6243;

// ACL constants
const ACL_SET: c_int          = 1 << 8;
const ACL_ALTER_SYSTEM: c_int = 1 << 9;

// AutoFileLock slot (placeholder index - real value from lwlock.h)
const AUTOFILE_LOCK: c_int = 0; // TODO(pg-port): replace with actual AutoFileLock index

// CONF_FILE_START_DEPTH
const CONF_FILE_START_DEPTH: c_int = 0;

// PqMsg_ParameterStatus
const PqMsg_ParameterStatus: u8 = b'S';

// open(2) flags
const O_CREAT: c_int  = 0o100;
const O_RDWR: c_int   = 2;
const O_TRUNC: c_int  = 0o1000;

// TYPALIGN_INT
const TYPALIGN_INT: u8 = b'i';

// error codes (errcode.h)
const ERRCODE_OUT_OF_MEMORY:           c_int = 0x53300;
const ERRCODE_UNDEFINED_OBJECT:        c_int = 0x42704;
const ERRCODE_CONFIG_FILE_ERROR:       c_int = 0xF0000;
const ERRCODE_CANT_CHANGE_RUNTIME_PARAM: c_int = 0x55000;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0x22023;
const ERRCODE_INVALID_NAME:            c_int = 0x42602;
const ERRCODE_INSUFFICIENT_PRIVILEGE:  c_int = 0x42501;
const ERRCODE_FEATURE_NOT_SUPPORTED:   c_int = 0x0A000;
const ERRCODE_INVALID_TRANSACTION_STATE: c_int = 0x25000;
// ERRCODE_INTERNAL_ERROR = MAKE_SQLSTATE('X','X','0','0','0') -> 0x5858300 in PG
const ERRCODE_INTERNAL_ERROR:          c_int = 0x58_58_00_03 as c_int; // XX000
const ERRCODE_SYNTAX_ERROR:            c_int = 0x42601;

// elog levels
const DEBUG3: c_int  = 23;
const DEBUG5: c_int  = 21;
const LOG: c_int     = 32;
const WARNING: c_int = 19;
const ERROR: c_int   = 21; // NOTE: PG ERROR == 21 in elog.h
const FATAL: c_int   = 22;
const PANIC: c_int   = 23;
#[allow(dead_code)]
const EOF: c_int     = -1;
#[allow(dead_code)]
const ENOENT: c_int  = 2; // TODO(pg-port): from <errno.h>

// VAR_SET_VALUE etc (parsenodes.h) -- also in guc_funcs.rs but duplicated here for self-containment
const VAR_SET_VALUE:   c_int = 0;
const VAR_SET_DEFAULT: c_int = 1;
const VAR_RESET:       c_int = 4;
const VAR_RESET_ALL:   c_int = 5;

// ---------------------------------------------------------------------------
// Module-level statics (translated from C file-scope variables)
// ---------------------------------------------------------------------------

// TODO(pg-port): guc_tables.c data arrays - separate file, stub empty here
#[allow(non_upper_case_globals)]
pub static mut ConfigureNamesBool: *mut config_bool = ptr::null_mut();
// TODO(pg-port): ConfigureNamesInt - defined in guc_tables.c
#[allow(non_upper_case_globals)]
pub static mut ConfigureNamesInt: *mut config_int = ptr::null_mut();
// TODO(pg-port): ConfigureNamesReal - defined in guc_tables.c
#[allow(non_upper_case_globals)]
pub static mut ConfigureNamesReal: *mut config_real = ptr::null_mut();
// TODO(pg-port): ConfigureNamesString - defined in guc_tables.c
#[allow(non_upper_case_globals)]
pub static mut ConfigureNamesString: *mut config_string = ptr::null_mut();
// TODO(pg-port): ConfigureNamesEnum - defined in guc_tables.c
#[allow(non_upper_case_globals)]
pub static mut ConfigureNamesEnum: *mut config_enum = ptr::null_mut();

// check hook support variables (exported in guc.h)
pub static mut GUC_check_errmsg_string:    *mut c_char = ptr::null_mut();
pub static mut GUC_check_errdetail_string: *mut c_char = ptr::null_mut();
pub static mut GUC_check_errhint_string:   *mut c_char = ptr::null_mut();

static mut GUC_check_errcode_value: c_int = 0; // ERRCODE_INVALID_PARAMETER_VALUE placeholder

static mut reserved_class_prefix: *mut List = ptr::null_mut(); // NIL initially

// Memory context holding all GUC-related data
static mut GUCMemoryContext: MemoryContext = ptr::null_mut();

// Hash table for looking up GUCs by name
static mut guc_hashtab: *mut HTAB = ptr::null_mut();

// Lists of variables with special properties
static mut guc_nondef_list: dlist_head  = dlist_head { head: crate::lib::ilist::dlist_node { next: ptr::null_mut(), prev: ptr::null_mut() } };
static mut guc_stack_list:  slist_head  = slist_head { head: crate::lib::ilist::slist_node { next: ptr::null_mut() } };
static mut guc_report_list: slist_head  = slist_head { head: crate::lib::ilist::slist_node { next: ptr::null_mut() } };

static mut reporting_enabled: bool = false; // true to enable GUC_REPORT
static mut GUCNestLevel: c_int = 0; // 1 when in main transaction

// GUC hash entry
#[repr(C)]
struct GUCHashEntry {
    gucname: *const c_char,        // hash key
    gucvar:  *mut config_generic,  // -> GUC's defining structure
}

// ---------------------------------------------------------------------------
// Unit conversion tables
// ---------------------------------------------------------------------------

const MAX_UNIT_LEN: usize = 3; // length of longest recognized unit string

#[repr(C)]
struct unit_conversion {
    unit:       [u8; MAX_UNIT_LEN + 1], // unit string like "kB" or "min"
    base_unit:  c_int,                  // GUC_UNIT_XXX
    multiplier: c_double,               // factor for converting unit -> base_unit
}

// make a const unit_conversion from a &str, base_unit, multiplier
macro_rules! uc {
    ($unit:expr, $base:expr, $mult:expr) => {{
        let src = $unit.as_bytes();
        let mut arr = [0u8; MAX_UNIT_LEN + 1];
        let mut i = 0;
        while i < src.len() && i < MAX_UNIT_LEN {
            arr[i] = src[i];
            i += 1;
        }
        unit_conversion { unit: arr, base_unit: $base, multiplier: $mult }
    }};
}

static memory_units_hint: &[u8] = b"Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\".\0";

static MEMORY_UNIT_CONVERSION_TABLE: &[unit_conversion] = &[
    unit_conversion { unit: *b"TB\0\0", base_unit: GUC_UNIT_BYTE, multiplier: 1024.0 * 1024.0 * 1024.0 * 1024.0 },
    unit_conversion { unit: *b"GB\0\0", base_unit: GUC_UNIT_BYTE, multiplier: 1024.0 * 1024.0 * 1024.0 },
    unit_conversion { unit: *b"MB\0\0", base_unit: GUC_UNIT_BYTE, multiplier: 1024.0 * 1024.0 },
    unit_conversion { unit: *b"kB\0\0", base_unit: GUC_UNIT_BYTE, multiplier: 1024.0 },
    unit_conversion { unit: *b"B\0\0\0", base_unit: GUC_UNIT_BYTE, multiplier: 1.0 },
    unit_conversion { unit: *b"TB\0\0", base_unit: GUC_UNIT_KB, multiplier: 1024.0 * 1024.0 * 1024.0 },
    unit_conversion { unit: *b"GB\0\0", base_unit: GUC_UNIT_KB, multiplier: 1024.0 * 1024.0 },
    unit_conversion { unit: *b"MB\0\0", base_unit: GUC_UNIT_KB, multiplier: 1024.0 },
    unit_conversion { unit: *b"kB\0\0", base_unit: GUC_UNIT_KB, multiplier: 1.0 },
    unit_conversion { unit: *b"B\0\0\0", base_unit: GUC_UNIT_KB, multiplier: 1.0 / 1024.0 },
    unit_conversion { unit: *b"TB\0\0", base_unit: GUC_UNIT_MB, multiplier: 1024.0 * 1024.0 },
    unit_conversion { unit: *b"GB\0\0", base_unit: GUC_UNIT_MB, multiplier: 1024.0 },
    unit_conversion { unit: *b"MB\0\0", base_unit: GUC_UNIT_MB, multiplier: 1.0 },
    unit_conversion { unit: *b"kB\0\0", base_unit: GUC_UNIT_MB, multiplier: 1.0 / 1024.0 },
    unit_conversion { unit: *b"B\0\0\0", base_unit: GUC_UNIT_MB, multiplier: 1.0 / (1024.0 * 1024.0) },
    // BLOCKS (depends on BLCKSZ=8192 -> BLCKSZ/1024 = 8)
    unit_conversion { unit: *b"TB\0\0", base_unit: GUC_UNIT_BLOCKS, multiplier: (1024.0 * 1024.0 * 1024.0) / (BLCKSZ as f64 / 1024.0) },
    unit_conversion { unit: *b"GB\0\0", base_unit: GUC_UNIT_BLOCKS, multiplier: (1024.0 * 1024.0) / (BLCKSZ as f64 / 1024.0) },
    unit_conversion { unit: *b"MB\0\0", base_unit: GUC_UNIT_BLOCKS, multiplier: 1024.0 / (BLCKSZ as f64 / 1024.0) },
    unit_conversion { unit: *b"kB\0\0", base_unit: GUC_UNIT_BLOCKS, multiplier: 1.0 / (BLCKSZ as f64 / 1024.0) },
    unit_conversion { unit: *b"B\0\0\0", base_unit: GUC_UNIT_BLOCKS, multiplier: 1.0 / BLCKSZ as f64 },
    // XBLOCKS (depends on XLOG_BLCKSZ=8192)
    unit_conversion { unit: *b"TB\0\0", base_unit: GUC_UNIT_XBLOCKS, multiplier: (1024.0 * 1024.0 * 1024.0) / (XLOG_BLCKSZ as f64 / 1024.0) },
    unit_conversion { unit: *b"GB\0\0", base_unit: GUC_UNIT_XBLOCKS, multiplier: (1024.0 * 1024.0) / (XLOG_BLCKSZ as f64 / 1024.0) },
    unit_conversion { unit: *b"MB\0\0", base_unit: GUC_UNIT_XBLOCKS, multiplier: 1024.0 / (XLOG_BLCKSZ as f64 / 1024.0) },
    unit_conversion { unit: *b"kB\0\0", base_unit: GUC_UNIT_XBLOCKS, multiplier: 1.0 / (XLOG_BLCKSZ as f64 / 1024.0) },
    unit_conversion { unit: *b"B\0\0\0", base_unit: GUC_UNIT_XBLOCKS, multiplier: 1.0 / XLOG_BLCKSZ as f64 },
    // end marker
    unit_conversion { unit: [0u8; 4], base_unit: 0, multiplier: 0.0 },
];

static time_units_hint: &[u8] = b"Valid units for this parameter are \"us\", \"ms\", \"s\", \"min\", \"h\", and \"d\".\0";

static TIME_UNIT_CONVERSION_TABLE: &[unit_conversion] = &[
    unit_conversion { unit: *b"d\0\0\0", base_unit: GUC_UNIT_MS, multiplier: (1000 * 60 * 60 * 24) as f64 },
    unit_conversion { unit: *b"h\0\0\0", base_unit: GUC_UNIT_MS, multiplier: (1000 * 60 * 60) as f64 },
    unit_conversion { unit: *b"min\0",   base_unit: GUC_UNIT_MS, multiplier: (1000 * 60) as f64 },
    unit_conversion { unit: *b"s\0\0\0", base_unit: GUC_UNIT_MS, multiplier: 1000.0 },
    unit_conversion { unit: *b"ms\0\0",  base_unit: GUC_UNIT_MS, multiplier: 1.0 },
    unit_conversion { unit: *b"us\0\0",  base_unit: GUC_UNIT_MS, multiplier: 1.0 / 1000.0 },
    unit_conversion { unit: *b"d\0\0\0", base_unit: GUC_UNIT_S,  multiplier: (60 * 60 * 24) as f64 },
    unit_conversion { unit: *b"h\0\0\0", base_unit: GUC_UNIT_S,  multiplier: (60 * 60) as f64 },
    unit_conversion { unit: *b"min\0",   base_unit: GUC_UNIT_S,  multiplier: 60.0 },
    unit_conversion { unit: *b"s\0\0\0", base_unit: GUC_UNIT_S,  multiplier: 1.0 },
    unit_conversion { unit: *b"ms\0\0",  base_unit: GUC_UNIT_S,  multiplier: 1.0 / 1000.0 },
    unit_conversion { unit: *b"us\0\0",  base_unit: GUC_UNIT_S,  multiplier: 1.0 / (1000.0 * 1000.0) },
    unit_conversion { unit: *b"d\0\0\0", base_unit: GUC_UNIT_MIN, multiplier: (60 * 24) as f64 },
    unit_conversion { unit: *b"h\0\0\0", base_unit: GUC_UNIT_MIN, multiplier: 60.0 },
    unit_conversion { unit: *b"min\0",   base_unit: GUC_UNIT_MIN, multiplier: 1.0 },
    unit_conversion { unit: *b"s\0\0\0", base_unit: GUC_UNIT_MIN, multiplier: 1.0 / 60.0 },
    unit_conversion { unit: *b"ms\0\0",  base_unit: GUC_UNIT_MIN, multiplier: 1.0 / (1000.0 * 60.0) },
    unit_conversion { unit: *b"us\0\0",  base_unit: GUC_UNIT_MIN, multiplier: 1.0 / (1000.0 * 1000.0 * 60.0) },
    unit_conversion { unit: [0u8; 4], base_unit: 0, multiplier: 0.0 },
];

// Obsolete GUC name mappings: old_name, new_name pairs, NULL-terminated.
// Must be `const` (not `static`) because *const u8 is not Sync.
const MAP_OLD_GUC_NAMES: &[*const u8] = &[
    b"sort_mem\0".as_ptr(),
    b"work_mem\0".as_ptr(),
    b"vacuum_mem\0".as_ptr(),
    b"maintenance_work_mem\0".as_ptr(),
    b"ssl_ecdh_curve\0".as_ptr(),
    b"ssl_groups\0".as_ptr(),
    core::ptr::null(),
];

// GUC variables declared in guc.h pointing elsewhere in the codebase.
// These are extern symbols; the actual definitions live in guc_tables.c / other modules.
// Provide extern "C" references.
extern "C" {
    pub static mut DataDir: *const c_char;
    pub static mut PgReloadTime: i64;
    // config file name variables
    pub static mut ConfigFileName:     *mut c_char;
    pub static mut HbaFileName:        *mut c_char;
    pub static mut IdentFileName:      *mut c_char;
    pub static mut external_pid_file:  *mut c_char;
    // runtime state
    pub static mut process_shared_preload_libraries_in_progress: bool;
    pub static mut in_hot_standby_guc: bool;
    pub static mut AllowAlterSystem:   bool;
    pub static mut error_context_stack: *mut ErrorContextCallback;
}

/// ErrorContextCallback for RestoreGUCState error reporting.
#[repr(C)]
pub struct ErrorContextCallback {
    pub callback: Option<unsafe extern "C" fn(arg: *mut c_void)>,
    pub previous: *mut ErrorContextCallback,
    pub arg:      *mut c_void,
}

// ---------------------------------------------------------------------------
// ProcessConfigFileInternal and related helpers
// ---------------------------------------------------------------------------

/// ProcessConfigFileInternal handles both actual config file (re)loads and
/// execution of show_all_file_settings().  In the latter case we don't apply
/// any of the settings, but we make all the usual validity checks, and we
/// return the ConfigVariable list so that it can be printed out by
/// show_all_file_settings().
pub unsafe fn ProcessConfigFileInternal(
    context: GucContext,
    applySettings: bool,
    elevel: c_int,
) -> *mut ConfigVariable {
    let mut error = false;
    let mut applying = false;
    let mut ConfFileWithError: *const c_char = ConfigFileName;
    let mut head: *mut ConfigVariable = ptr::null_mut();
    let mut tail: *mut ConfigVariable = ptr::null_mut();
    let mut status: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut hentry: *mut GUCHashEntry;

    // Parse the main config file into a list of option names and values
    ConfFileWithError = ConfigFileName;
    head = ptr::null_mut();
    tail = ptr::null_mut();

    if !ParseConfigFile(
        ConfigFileName,
        true,
        ptr::null(),
        0,
        CONF_FILE_START_DEPTH,
        elevel,
        &mut head,
        &mut tail,
    ) {
        // Syntax error(s) detected in the file, so bail out
        error = true;
        // goto bail_out
    } else {
        // Parse the PG_AUTOCONF_FILENAME file, if present, after the main file to
        // replace any parameters set by ALTER SYSTEM command.
        if !DataDir.is_null() {
            if !ParseConfigFile(
                PG_AUTOCONF_FILENAME.as_ptr() as *const c_char,
                false,
                ptr::null(),
                0,
                CONF_FILE_START_DEPTH,
                elevel,
                &mut head,
                &mut tail,
            ) {
                error = true;
                ConfFileWithError = PG_AUTOCONF_FILENAME.as_ptr() as *const c_char;
            }
        } else {
            // DataDir not set: prune all items except last "data_directory"
            let mut newlist: *mut ConfigVariable = ptr::null_mut();
            let mut item = head;
            while !item.is_null() {
                if !(*item).ignore
                    && strcmp((*item).name, b"data_directory\0".as_ptr() as *const c_char) == 0
                {
                    newlist = item;
                }
                item = (*item).next;
            }
            if !newlist.is_null() {
                (*newlist).next = ptr::null_mut();
            }
            head = newlist;
            tail = newlist;
            // Quick exit if data_directory not present
            if head.is_null() {
                return head;
            }
        }

        if !error {
            // Mark all extant GUC variables as not present in the config file
            hash_seq_init(&mut status, guc_hashtab);
            loop {
                hentry = hash_seq_search(&mut status) as *mut GUCHashEntry;
                if hentry.is_null() { break; }
                let gconf = (*hentry).gucvar;
                (*gconf).status &= !GUC_IS_IN_FILE;
            }

            // Check if all the supplied option names are valid
            let mut item = head;
            while !item.is_null() {
                if (*item).ignore {
                    item = (*item).next;
                    continue;
                }
                let record = find_option((*item).name, false, true, elevel);
                if !record.is_null() {
                    // If already marked, this is a duplicate entry
                    if ((*record).status & GUC_IS_IN_FILE) != 0 {
                        let mut pitem = head;
                        while pitem != item {
                            if !(*pitem).ignore
                                && strcmp((*pitem).name, (*item).name) == 0
                            {
                                (*pitem).ignore = true;
                            }
                            pitem = (*pitem).next;
                        }
                    }
                    // Mark it as present in file
                    (*record).status |= GUC_IS_IN_FILE;
                } else if !valid_custom_variable_name(
                    std::ffi::CStr::from_ptr((*item).name).to_bytes(),
                ) {
                    // Invalid non-custom variable, complain
                    ereport!(elevel, errmsg!(
                        "unrecognized configuration parameter \"{}\" in file \"{}\" line {}",
                        std::ffi::CStr::from_ptr((*item).name).to_string_lossy(),
                        std::ffi::CStr::from_ptr((*item).filename).to_string_lossy(),
                        (*item).sourceline
                    ));
                    (*item).errmsg = pstrdup(b"unrecognized configuration parameter\0".as_ptr() as *const c_char);
                    error = true;
                    ConfFileWithError = (*item).filename;
                }
                item = (*item).next;
            }
        }

        // If errors detected, don't apply changes
        if !error {
            applying = true;

            // Check for variables removed from config file; revert reset values
            hash_seq_init(&mut status, guc_hashtab);
            loop {
                hentry = hash_seq_search(&mut status) as *mut GUCHashEntry;
                if hentry.is_null() { break; }
                let gconf = (*hentry).gucvar;
                let mut stack: *mut GucStack;

                if (*gconf).reset_source != GucSource::PGC_S_FILE
                    || ((*gconf).status & GUC_IS_IN_FILE) != 0
                {
                    continue;
                }
                if ((*gconf).context as i32) < (GucContext::PGC_SIGHUP as i32) {
                    (*gconf).status |= GUC_PENDING_RESTART;
                    ereport!(elevel, errmsg!(
                        "parameter \"{}\" cannot be changed without restarting the server",
                        std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy()
                    ));
                    record_config_file_error(
                        psprintf(
                            b"parameter \"{}\" cannot be changed without restarting the server\0".as_ptr() as *const c_char,
                            (*gconf).name,
                        ),
                        ptr::null(),
                        0,
                        &mut head,
                        &mut tail,
                    );
                    error = true;
                    continue;
                }

                if !applySettings { continue; }

                // Reset any "file" sources to "default"
                if (*gconf).reset_source == GucSource::PGC_S_FILE {
                    (*gconf).reset_source = GucSource::PGC_S_DEFAULT;
                }
                if (*gconf).source == GucSource::PGC_S_FILE {
                    set_guc_source(gconf, GucSource::PGC_S_DEFAULT);
                }
                stack = (*gconf).stack;
                while !stack.is_null() {
                    if (*stack).source == GucSource::PGC_S_FILE {
                        (*stack).source = GucSource::PGC_S_DEFAULT;
                    }
                    stack = (*stack).prev;
                }

                // Re-apply the wired-in default
                if set_config_option(
                    (*gconf).name,
                    ptr::null(),
                    context,
                    GucSource::PGC_S_DEFAULT,
                    GucAction::GUC_ACTION_SET,
                    true,
                    0,
                    false,
                ) > 0 && context == GucContext::PGC_SIGHUP
                {
                    ereport!(elevel, errmsg!(
                        "parameter \"{}\" removed from configuration file, reset to default",
                        std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy()
                    ));
                }
            }

            // Restore env-var / dynamic-default variables if re-loading
            if context == GucContext::PGC_SIGHUP && applySettings {
                InitializeGUCOptionsFromEnvironment();
                pg_timezone_abbrev_initialize();
                SetConfigOption(
                    b"client_encoding\0".as_ptr() as *const c_char,
                    GetDatabaseEncodingName(),
                    GucContext::PGC_BACKEND,
                    GucSource::PGC_S_DYNAMIC_DEFAULT,
                );
            }

            // Now apply the values from the config file
            let mut item = head;
            while !item.is_null() {
                let mut pre_value: *mut c_char = ptr::null_mut();

                if (*item).ignore {
                    item = (*item).next;
                    continue;
                }

                // In SIGHUP cases in the postmaster, report changes
                if context == GucContext::PGC_SIGHUP && applySettings && !IsUnderPostmaster() {
                    let preval = GetConfigOption((*item).name, true, false);
                    let preval = if preval.is_null() { b"\0".as_ptr() as *const c_char } else { preval };
                    pre_value = pstrdup(preval);
                }

                let scres = set_config_option(
                    (*item).name,
                    (*item).value,
                    context,
                    GucSource::PGC_S_FILE,
                    GucAction::GUC_ACTION_SET,
                    applySettings,
                    0,
                    false,
                );
                if scres > 0 {
                    if !pre_value.is_null() {
                        let post_value = GetConfigOption((*item).name, true, false);
                        let post_value = if post_value.is_null() { b"\0".as_ptr() as *const c_char } else { post_value };
                        if strcmp(pre_value, post_value) != 0 {
                            ereport!(elevel, errmsg!(
                                "parameter \"{}\" changed to \"{}\"",
                                std::ffi::CStr::from_ptr((*item).name).to_string_lossy(),
                                std::ffi::CStr::from_ptr((*item).value).to_string_lossy()
                            ));
                        }
                    }
                    (*item).applied = true;
                } else if scres == 0 {
                    error = true;
                    (*item).errmsg = pstrdup(b"setting could not be applied\0".as_ptr() as *const c_char);
                    ConfFileWithError = (*item).filename;
                } else {
                    // no error, but variable's active value was not changed
                    (*item).applied = true;
                }

                // Update source location unless there was an error
                if scres != 0 && applySettings {
                    set_config_sourcefile((*item).name, (*item).filename, (*item).sourceline);
                }

                if !pre_value.is_null() {
                    pfree(pre_value as *mut c_void);
                }
                item = (*item).next;
            }

            // Remember when we last successfully loaded the config file
            if applySettings {
                PgReloadTime = GetCurrentTimestamp();
            }
        }
    } // end else (not bail_out from first parse)

    // bail_out:
    if error && applySettings {
        if context == GucContext::PGC_POSTMASTER {
            ereport!(ERROR, errmsg!(
                "configuration file \"{}\" contains errors",
                std::ffi::CStr::from_ptr(ConfFileWithError).to_string_lossy()
            ));
        } else if applying {
            ereport!(elevel, errmsg!(
                "configuration file \"{}\" contains errors; unaffected changes were applied",
                std::ffi::CStr::from_ptr(ConfFileWithError).to_string_lossy()
            ));
        } else {
            ereport!(elevel, errmsg!(
                "configuration file \"{}\" contains errors; no changes were applied",
                std::ffi::CStr::from_ptr(ConfFileWithError).to_string_lossy()
            ));
        }
    }

    // Successful or otherwise, return the collected data list
    head
}

/// Stub for GetDatabaseEncodingName (defined in mb/mbutils.c).
/// TODO(pg-port): replace with real import once mb module is wired up.
unsafe fn GetDatabaseEncodingName() -> *const c_char {
    b"SQL_ASCII\0".as_ptr() as *const c_char
}

// ---------------------------------------------------------------------------
// GUC memory allocation helpers
// ---------------------------------------------------------------------------

/// Allocate memory from GUCMemoryContext.  Reports OOM at elevel.
pub unsafe fn guc_malloc(elevel: c_int, size: usize) -> *mut c_void {
    let data = MemoryContextAllocExtended(GUCMemoryContext, size, MCXT_ALLOC_NO_OOM);
    if data.is_null() {
        ereport!(elevel, errmsg!("out of memory"));
    }
    data
}

/// Reallocate memory from GUCMemoryContext.  Reports OOM at elevel.
pub unsafe fn guc_realloc(elevel: c_int, old: *mut c_void, size: usize) -> *mut c_void {
    let data = if !old.is_null() {
        // Help catch old code that malloc's GUC data
        debug_assert!(GetMemoryChunkContext(old) == GUCMemoryContext);
        repalloc_extended(old, size, MCXT_ALLOC_NO_OOM)
    } else {
        // Like realloc(3), allow old == NULL
        MemoryContextAllocExtended(GUCMemoryContext, size, MCXT_ALLOC_NO_OOM)
    };
    if data.is_null() {
        ereport!(elevel, errmsg!("out of memory"));
    }
    data
}

/// Duplicate a string into GUCMemoryContext.
pub unsafe fn guc_strdup(elevel: c_int, src: *const c_char) -> *mut c_char {
    let len = strlen(src) + 1;
    let data = guc_malloc(elevel, len) as *mut c_char;
    if !data.is_null() {
        memcpy(data as *mut c_void, src as *const c_void, len);
    }
    data
}

/// Free memory previously allocated with guc_malloc / guc_strdup.
/// Allows ptr == NULL (like free(3), unlike pfree).
pub unsafe fn guc_free(ptr: *mut c_void) {
    if !ptr.is_null() {
        debug_assert!(GetMemoryChunkContext(ptr) == GUCMemoryContext);
        pfree(ptr);
    }
}

// ---------------------------------------------------------------------------
// String / extra field management
// ---------------------------------------------------------------------------

/// Detect whether strval is referenced anywhere in a GUC string item.
unsafe fn string_field_used(conf: *mut config_string, strval: *mut c_char) -> bool {
    if strval == *(*conf).variable || strval == (*conf).reset_val || strval == (*conf).boot_val as *mut c_char {
        return true;
    }
    let mut stack = (*conf).gen.stack;
    while !stack.is_null() {
        if strval == (*stack).prior.val.stringval || strval == (*stack).masked.val.stringval {
            return true;
        }
        stack = (*stack).prev;
    }
    false
}

/// Assign to a field of a string GUC item; free prior value if no longer referenced.
unsafe fn set_string_field(conf: *mut config_string, field: *mut *mut c_char, newval: *mut c_char) {
    let oldval = *field;
    *field = newval;
    if !oldval.is_null() && !string_field_used(conf, oldval) {
        guc_free(oldval as *mut c_void);
    }
}

/// Detect whether extra is referenced anywhere in a GUC item.
unsafe fn extra_field_used(gconf: *mut config_generic, extra: *mut c_void) -> bool {
    if extra == (*gconf).extra {
        return true;
    }
    match (*gconf).vartype {
        config_type::PGC_BOOL => {
            if extra == (*(gconf as *mut config_bool)).reset_extra { return true; }
        }
        config_type::PGC_INT => {
            if extra == (*(gconf as *mut config_int)).reset_extra { return true; }
        }
        config_type::PGC_REAL => {
            if extra == (*(gconf as *mut config_real)).reset_extra { return true; }
        }
        config_type::PGC_STRING => {
            if extra == (*(gconf as *mut config_string)).reset_extra { return true; }
        }
        config_type::PGC_ENUM => {
            if extra == (*(gconf as *mut config_enum)).reset_extra { return true; }
        }
    }
    let mut stack = (*gconf).stack;
    while !stack.is_null() {
        if extra == (*stack).prior.extra || extra == (*stack).masked.extra {
            return true;
        }
        stack = (*stack).prev;
    }
    false
}

/// Assign to an "extra" field of a GUC item; free prior value if no longer referenced.
unsafe fn set_extra_field(gconf: *mut config_generic, field: *mut *mut c_void, newval: *mut c_void) {
    let oldval = *field;
    *field = newval;
    if !oldval.is_null() && !extra_field_used(gconf, oldval) {
        guc_free(oldval);
    }
}

/// Copy a variable's active value into a stack entry.
unsafe fn set_stack_value(gconf: *mut config_generic, val: *mut config_var_value) {
    match (*gconf).vartype {
        config_type::PGC_BOOL => {
            (*val).val.boolval = *(*(gconf as *mut config_bool)).variable;
        }
        config_type::PGC_INT => {
            (*val).val.intval = *(*(gconf as *mut config_int)).variable;
        }
        config_type::PGC_REAL => {
            (*val).val.realval = *(*(gconf as *mut config_real)).variable;
        }
        config_type::PGC_STRING => {
            set_string_field(
                gconf as *mut config_string,
                &mut (*val).val.stringval,
                *(*(gconf as *mut config_string)).variable,
            );
        }
        config_type::PGC_ENUM => {
            (*val).val.enumval = *(*(gconf as *mut config_enum)).variable;
        }
    }
    set_extra_field(gconf, &mut (*val).extra, (*gconf).extra);
}

/// Discard a no-longer-needed value in a stack entry.
unsafe fn discard_stack_value(gconf: *mut config_generic, val: *mut config_var_value) {
    match (*gconf).vartype {
        config_type::PGC_BOOL
        | config_type::PGC_INT
        | config_type::PGC_REAL
        | config_type::PGC_ENUM => {
            // no need to do anything for non-pointer types
        }
        config_type::PGC_STRING => {
            set_string_field(gconf as *mut config_string, &mut (*val).val.stringval, ptr::null_mut());
        }
    }
    set_extra_field(gconf, &mut (*val).extra, ptr::null_mut());
}

// ---------------------------------------------------------------------------
// get_guc_variables / build_guc_variables
// ---------------------------------------------------------------------------

/// Fetch a palloc'd, sorted array of GUC struct pointers.
pub unsafe fn get_guc_variables(num_vars: *mut c_int) -> *mut *mut config_generic {
    let n = hash_get_num_entries(guc_hashtab) as usize;
    *num_vars = n as c_int;
    let result = palloc(std::mem::size_of::<*mut config_generic>() * n) as *mut *mut config_generic;
    let mut status: HASH_SEQ_STATUS = std::mem::zeroed();
    hash_seq_init(&mut status, guc_hashtab);
    let mut i: usize = 0;
    loop {
        let hentry = hash_seq_search(&mut status) as *mut GUCHashEntry;
        if hentry.is_null() { break; }
        *result.add(i) = (*hentry).gucvar;
        i += 1;
    }
    debug_assert!(i == n);
    // Sort by name
    let slice = std::slice::from_raw_parts_mut(result, n);
    slice.sort_unstable_by(|a, b| {
        let na = std::ffi::CStr::from_ptr((**a).name);
        let nb = std::ffi::CStr::from_ptr((**b).name);
        guc_name_compare_rs(na.to_bytes(), nb.to_bytes()).cmp(&0)
    });
    result
}

/// Build the GUC hash table.
pub unsafe fn build_guc_variables() {
    // Create the memory context that will hold all GUC-related data
    debug_assert!(GUCMemoryContext.is_null());
    GUCMemoryContext = AllocSetContextCreate!(
        TopMemoryContext,
        b"GUCMemoryContext\0".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES
    );

    // Count all the built-in variables, and set their vartypes
    let num_vars: usize = 0;

    // TODO(pg-port): ConfigureNamesBool/Int/Real/String/Enum are defined in
    // guc_tables.c; count is done by scanning sentinel (gen.name == NULL).
    // For now assume 0 entries since statics are null-initialized stubs.

    // Create hash table with 20% slack
    let size_vars = (num_vars + num_vars / 4).max(16);
    let mut hash_ctl: HASHCTL = std::mem::zeroed();
    hash_ctl.keysize   = std::mem::size_of::<*const c_char>();
    hash_ctl.entrysize = std::mem::size_of::<GUCHashEntry>();
    hash_ctl.hash      = Some(guc_name_hash_fn);
    hash_ctl.r#match   = Some(guc_name_match_fn);
    hash_ctl.hcxt      = GUCMemoryContext;
    guc_hashtab = hash_create(
        b"GUC hash table\0".as_ptr() as *const c_char,
        size_vars as i64,
        &hash_ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT,
    );

    // Insert built-in bool variables
    if !ConfigureNamesBool.is_null() {
        let mut i = 0;
        loop {
            let conf = ConfigureNamesBool.add(i);
            if (*conf).gen.name.is_null() { break; }
            (*conf).gen.vartype = config_type::PGC_BOOL;
            let gucvar = &mut (*conf).gen as *mut config_generic;
            let mut found: bool = false;
            let hentry = hash_search(guc_hashtab, &(*gucvar).name as *const *const c_char as *const c_void, HASH_ENTER, &mut found) as *mut GUCHashEntry;
            debug_assert!(!found);
            (*hentry).gucvar = gucvar;
            i += 1;
        }
    }
    // TODO(pg-port): insert Int, Real, String, Enum similarly once tables are ported
}

/// Add a new GUC variable to the hash of known variables.
unsafe fn add_guc_variable(var: *mut config_generic, elevel: c_int) -> bool {
    let mut found = false;
    let hentry = hash_search(guc_hashtab, &(*var).name as *const *const c_char as *const c_void, HASH_ENTER_NULL, &mut found) as *mut GUCHashEntry;
    if hentry.is_null() {
        ereport!(elevel, errmsg!("out of memory"));
        return false;
    }
    debug_assert!(!found);
    (*hentry).gucvar = var;
    true
}

// ---------------------------------------------------------------------------
// guc_name_compare (exported) and hash helpers (local)
// ---------------------------------------------------------------------------

/// Compare two GUC names case-insensitively.  Stable under setlocale.
pub fn guc_name_compare_rs(namea: &[u8], nameb: &[u8]) -> c_int {
    let mut ia = namea.iter();
    let mut ib = nameb.iter();
    loop {
        match (ia.next(), ib.next()) {
            (None, None) => return 0,
            (Some(_), None) => return 1,
            (None, Some(_)) => return -1,
            (Some(&ca), Some(&cb)) => {
                let fa = if ca.is_ascii_uppercase() { ca + 32 } else { ca };
                let fb = if cb.is_ascii_uppercase() { cb + 32 } else { cb };
                if fa != fb { return (fa as c_int) - (fb as c_int); }
            }
        }
    }
}

/// C-callable wrapper for guc_name_compare.
pub unsafe extern "C" fn guc_name_compare_c(namea: *const c_char, nameb: *const c_char) -> c_int {
    let a = std::ffi::CStr::from_ptr(namea).to_bytes();
    let b = std::ffi::CStr::from_ptr(nameb).to_bytes();
    guc_name_compare_rs(a, b)
}

/*
 * comparator for qsorting and bsearching guc_variables array
 */
pub unsafe extern "C" fn guc_var_compare(a: *const c_void, b: *const c_void) -> c_int {
    let namea = **(a as *const *const *const c_char);
    let nameb = **(b as *const *const *const c_char);

    guc_name_compare_c(namea, nameb)
}

unsafe extern "C" fn guc_name_hash_fn(key: *const c_void, _keysize: usize) -> u32 {
    let name = *(key as *const *const c_char);
    let mut result: u32 = 0;
    let mut p = name;
    loop {
        let ch = *p;
        if ch == 0 { break; }
        let ch = if (ch as u8).is_ascii_uppercase() { (ch as u8 + 32) as c_char } else { ch };
        result = pg_rotate_left32(result, 5);
        result ^= (ch as u8) as u32;
        p = p.add(1);
    }
    result
}

unsafe extern "C" fn guc_name_match_fn(key1: *const c_void, key2: *const c_void, _keysize: usize) -> c_int {
    let name1 = *(key1 as *const *const c_char);
    let name2 = *(key2 as *const *const c_char);
    let a = std::ffi::CStr::from_ptr(name1).to_bytes();
    let b = std::ffi::CStr::from_ptr(name2).to_bytes();
    guc_name_compare_rs(a, b)
}

// ---------------------------------------------------------------------------
// valid_custom_variable_name / assignable_custom_variable_name
// ---------------------------------------------------------------------------

/// Return true if name looks like a valid custom variable name (two or more
/// identifiers separated by dots).
pub fn valid_custom_variable_name(name: &[u8]) -> bool {
    let mut saw_sep = false;
    let mut name_start = true;
    for &b in name {
        if b == GUC_QUALIFIER_SEPARATOR {
            if name_start { return false; } // empty name component
            saw_sep = true;
            name_start = true;
        } else if b.is_ascii_alphabetic() || b == b'_' || b >= 0x80 {
            name_start = false;
        } else if !name_start && (b.is_ascii_digit() || b == b'$') {
            // ok as non-first character
        } else {
            return false;
        }
    }
    if name_start { return false; } // empty name component
    saw_sep
}

/// Decide whether an unrecognized variable name may be SET.
unsafe fn assignable_custom_variable_name(name: *const c_char, skip_errors: bool, elevel: c_int) -> bool {
    let bytes = std::ffi::CStr::from_ptr(name).to_bytes();
    let sep = bytes.iter().position(|&b| b == GUC_QUALIFIER_SEPARATOR);
    if let Some(sep_pos) = sep {
        let class_len = sep_pos;
        if !valid_custom_variable_name(bytes) {
            if !skip_errors {
                ereport!(elevel, errmsg!(
                    "invalid configuration parameter name \"{}\"",
                    std::ffi::CStr::from_ptr(name).to_string_lossy()
                ));
            }
            return false;
        }
        // Check against reserved prefixes
        let lc = reserved_class_prefix;
        while !lc.is_null() {
            // TODO(pg-port): iterate List properly; using lfirst stub
            // For now: skip reserved prefix check until list is wired
            break;
        }
        return true;
    }
    // Unrecognized single-part name
    if !skip_errors {
        ereport!(elevel, errmsg!("unrecognized configuration parameter \"{}\"", std::ffi::CStr::from_ptr(name).to_string_lossy()));
    }
    false
}

/// Create and add a placeholder variable for a custom variable name.
unsafe fn add_placeholder_variable(name: *const c_char, elevel: c_int) -> *mut config_generic {
    let sz = std::mem::size_of::<config_string>() + std::mem::size_of::<*mut c_char>();
    let var = guc_malloc(elevel, sz) as *mut config_string;
    if var.is_null() { return ptr::null_mut(); }
    memset(var as *mut c_void, 0, sz);
    let gen = &mut (*var).gen as *mut config_generic;

    (*gen).name = guc_strdup(elevel, name);
    if (*gen).name.is_null() {
        guc_free(var as *mut c_void);
        return ptr::null_mut();
    }

    (*gen).context   = GucContext::PGC_USERSET;
    (*gen).group     = config_group::CUSTOM_OPTIONS;
    (*gen).short_desc = b"GUC placeholder variable\0".as_ptr() as *const c_char;
    (*gen).flags     = GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_CUSTOM_PLACEHOLDER;
    (*gen).vartype   = config_type::PGC_STRING;

    // The char* is allocated at the end of the struct
    (*var).variable = (var.add(1)) as *mut *mut c_char;

    if !add_guc_variable(gen, elevel) {
        guc_free((*gen).name as *mut c_void);
        guc_free(var as *mut c_void);
        return ptr::null_mut();
    }
    gen
}

// ---------------------------------------------------------------------------
// find_option
// ---------------------------------------------------------------------------

/// Look up option "name".  Optionally create placeholder, skip errors, etc.
pub unsafe fn find_option(
    name: *const c_char,
    create_placeholders: bool,
    skip_errors: bool,
    elevel: c_int,
) -> *mut config_generic {
    debug_assert!(!name.is_null());

    // Look it up using the hash table
    let mut found = false;
    let hentry = hash_search(guc_hashtab, &name as *const *const c_char as *const c_void, HASH_FIND, &mut found) as *mut GUCHashEntry;
    if !hentry.is_null() && found {
        return (*hentry).gucvar;
    }

    // See if the name is an obsolete name for a variable
    let name_bytes = std::ffi::CStr::from_ptr(name).to_bytes();
    let mut i = 0;
    while !MAP_OLD_GUC_NAMES[i].is_null() {
        if guc_name_compare_rs(name_bytes, std::ffi::CStr::from_ptr(MAP_OLD_GUC_NAMES[i] as *const c_char).to_bytes()) == 0 {
            return find_option(MAP_OLD_GUC_NAMES[i + 1] as *const c_char, false, skip_errors, elevel);
        }
        i += 2;
    }

    if create_placeholders {
        if assignable_custom_variable_name(name, skip_errors, elevel) {
            return add_placeholder_variable(name, elevel);
        } else {
            return ptr::null_mut();
        }
    }

    // Unknown name and not supposed to make a placeholder
    if !skip_errors {
        ereport!(elevel, errmsg!("unrecognized configuration parameter \"{}\"", std::ffi::CStr::from_ptr(name).to_string_lossy()));
    }
    ptr::null_mut()
}

// ---------------------------------------------------------------------------
// convert_GUC_name_for_parameter_acl / check_GUC_name_for_parameter_acl
// ---------------------------------------------------------------------------

/// Convert a GUC name to the canonical form used in pg_parameter_acl.
pub unsafe fn convert_GUC_name_for_parameter_acl(name: *const c_char) -> *mut c_char {
    // Apply old-GUC-name mapping
    let mut name_use = name;
    let name_bytes = std::ffi::CStr::from_ptr(name).to_bytes();
    let mut i = 0;
    while !MAP_OLD_GUC_NAMES[i].is_null() {
        if guc_name_compare_rs(name_bytes, std::ffi::CStr::from_ptr(MAP_OLD_GUC_NAMES[i] as *const c_char).to_bytes()) == 0 {
            name_use = MAP_OLD_GUC_NAMES[i + 1] as *const c_char;
            break;
        }
        i += 2;
    }
    // Apply case-folding that matches guc_name_compare
    let result = pstrdup(name_use);
    let mut ptr = result;
    while *ptr != 0 {
        let ch = *ptr as u8;
        if ch.is_ascii_uppercase() {
            *ptr = (ch + 32) as c_char;
        }
        ptr = ptr.add(1);
    }
    result
}

/// Check whether we should allow creation of a pg_parameter_acl entry for name.
pub unsafe fn check_GUC_name_for_parameter_acl(name: *const c_char) {
    if !find_option(name, false, true, DEBUG5).is_null() {
        return;
    }
    assignable_custom_variable_name(name, false, ERROR);
}

// ---------------------------------------------------------------------------
// check_GUC_init (USE_ASSERT_CHECKING)
// ---------------------------------------------------------------------------

#[cfg(debug_assertions)]
unsafe fn check_GUC_init(gconf: *mut config_generic) -> bool {
    match (*gconf).vartype {
        config_type::PGC_BOOL => {
            let conf = gconf as *mut config_bool;
            if *(*conf).variable && !(*conf).boot_val {
                elog!(LOG, "GUC (PGC_BOOL) {}, boot_val={}, C-var={}", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy(), (*conf).boot_val as c_int, *(*conf).variable as c_int);
                return false;
            }
        }
        config_type::PGC_INT => {
            let conf = gconf as *mut config_int;
            if *(*conf).variable != 0 && *(*conf).variable != (*conf).boot_val {
                elog!(LOG, "GUC (PGC_INT) {}, boot_val={}, C-var={}", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy(), (*conf).boot_val, *(*conf).variable);
                return false;
            }
        }
        config_type::PGC_REAL => {
            let conf = gconf as *mut config_real;
            if *(*conf).variable != 0.0 && *(*conf).variable != (*conf).boot_val {
                elog!(LOG, "GUC (PGC_REAL) {}, boot_val={}, C-var={}", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy(), (*conf).boot_val, *(*conf).variable);
                return false;
            }
        }
        config_type::PGC_STRING => {
            let conf = gconf as *mut config_string;
            if !(*(*conf).variable).is_null()
                && ((*conf).boot_val.is_null()
                    || strcmp(*(*conf).variable, (*conf).boot_val as *mut c_char) != 0)
            {
                elog!(LOG, "GUC (PGC_STRING) {} mismatch", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy());
                return false;
            }
        }
        config_type::PGC_ENUM => {
            let conf = gconf as *mut config_enum;
            if *(*conf).variable != (*conf).boot_val {
                elog!(LOG, "GUC (PGC_ENUM) {}, boot_val={}, C-var={}", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy(), (*conf).boot_val, *(*conf).variable);
                return false;
            }
        }
    }
    // Flag combinations: GUC_NO_SHOW_ALL requires GUC_NOT_IN_SAMPLE
    if ((*gconf).flags & GUC_NO_SHOW_ALL) != 0 && ((*gconf).flags & GUC_NOT_IN_SAMPLE) == 0 {
        elog!(LOG, "GUC {} flags: NO_SHOW_ALL and !NOT_IN_SAMPLE", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy());
        return false;
    }
    true
}

#[cfg(not(debug_assertions))]
#[inline(always)]
unsafe fn check_GUC_init(_gconf: *mut config_generic) -> bool { true }

// ---------------------------------------------------------------------------
// InitializeGUCOptions / InitializeGUCOptionsFromEnvironment / InitializeOneGUCOption
// ---------------------------------------------------------------------------

/// Initialize GUC options during program startup.
pub unsafe fn InitializeGUCOptions() {
    // Before log_line_prefix could possibly receive a nonempty setting,
    // make sure that timezone processing is minimally alive.
    pg_timezone_initialize();

    // Create GUCMemoryContext and build hash table of all GUC variables
    build_guc_variables();

    // Load all variables with their compiled-in defaults
    let mut status: HASH_SEQ_STATUS = std::mem::zeroed();
    hash_seq_init(&mut status, guc_hashtab);
    loop {
        let hentry = hash_seq_search(&mut status) as *mut GUCHashEntry;
        if hentry.is_null() { break; }
        debug_assert!(check_GUC_init((*hentry).gucvar));
        InitializeOneGUCOption((*hentry).gucvar);
    }

    reporting_enabled = false;

    // Prevent any attempt to override the transaction modes from non-interactive sources
    SetConfigOption(b"transaction_isolation\0".as_ptr() as *const c_char, b"read committed\0".as_ptr() as *const c_char, GucContext::PGC_POSTMASTER, GucSource::PGC_S_OVERRIDE);
    SetConfigOption(b"transaction_read_only\0".as_ptr() as *const c_char, b"no\0".as_ptr() as *const c_char, GucContext::PGC_POSTMASTER, GucSource::PGC_S_OVERRIDE);
    SetConfigOption(b"transaction_deferrable\0".as_ptr() as *const c_char, b"no\0".as_ptr() as *const c_char, GucContext::PGC_POSTMASTER, GucSource::PGC_S_OVERRIDE);

    // Process environment-variable defaults
    InitializeGUCOptionsFromEnvironment();
}

/// Assign any GUC values that can come from the server's environment.
unsafe fn InitializeGUCOptionsFromEnvironment() {
    let env = getenv(b"PGPORT\0".as_ptr() as *const c_char);
    if !env.is_null() {
        SetConfigOption(b"port\0".as_ptr() as *const c_char, env, GucContext::PGC_POSTMASTER, GucSource::PGC_S_ENV_VAR);
    }
    let env = getenv(b"PGDATESTYLE\0".as_ptr() as *const c_char);
    if !env.is_null() {
        SetConfigOption(b"datestyle\0".as_ptr() as *const c_char, env, GucContext::PGC_POSTMASTER, GucSource::PGC_S_ENV_VAR);
    }
    let env = getenv(b"PGCLIENTENCODING\0".as_ptr() as *const c_char);
    if !env.is_null() {
        SetConfigOption(b"client_encoding\0".as_ptr() as *const c_char, env, GucContext::PGC_POSTMASTER, GucSource::PGC_S_ENV_VAR);
    }
    // rlimit / stack depth
    let stack_rlimit = get_stack_depth_rlimit();
    if stack_rlimit > 0 {
        let mut new_limit = (stack_rlimit - STACK_DEPTH_SLOP) / 1024;
        if new_limit > 100 {
            let source;
            if new_limit < 2048 {
                source = GucSource::PGC_S_ENV_VAR;
            } else {
                new_limit = 2048;
                source = GucSource::PGC_S_DYNAMIC_DEFAULT;
            }
            let mut limbuf = [0u8; 16];
            snprintf(limbuf.as_mut_ptr() as *mut c_char, 16, b"{}\0".as_ptr() as *const c_char, new_limit as c_int);
            SetConfigOption(b"max_stack_depth\0".as_ptr() as *const c_char, limbuf.as_ptr() as *const c_char, GucContext::PGC_POSTMASTER, source);
        }
    }
}

/// Initialize one GUC option variable to its compiled-in default.
unsafe fn InitializeOneGUCOption(gconf: *mut config_generic) {
    (*gconf).status = 0;
    (*gconf).source = GucSource::PGC_S_DEFAULT;
    (*gconf).reset_source = GucSource::PGC_S_DEFAULT;
    (*gconf).scontext = GucContext::PGC_INTERNAL;
    (*gconf).reset_scontext = GucContext::PGC_INTERNAL;
    (*gconf).srole = BOOTSTRAP_SUPERUSERID;
    (*gconf).reset_srole = BOOTSTRAP_SUPERUSERID;
    (*gconf).stack = ptr::null_mut();
    (*gconf).extra = ptr::null_mut();
    (*gconf).last_reported = ptr::null_mut();
    (*gconf).sourcefile = ptr::null_mut();
    (*gconf).sourceline = 0;

    match (*gconf).vartype {
        config_type::PGC_BOOL => {
            let conf = gconf as *mut config_bool;
            let mut newval = (*conf).boot_val;
            let mut extra: *mut c_void = ptr::null_mut();
            if !call_bool_check_hook(conf, &mut newval, &mut extra, GucSource::PGC_S_DEFAULT, LOG) {
                elog!(FATAL, "failed to initialize {} to {}", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy(), newval as c_int);
            }
            if let Some(h) = (*conf).assign_hook { h(newval, extra); }
            *(*conf).variable = newval;
            (*conf).reset_val = newval;
            (*gconf).extra = extra;
            (*conf).reset_extra = extra;
        }
        config_type::PGC_INT => {
            let conf = gconf as *mut config_int;
            let mut newval = (*conf).boot_val;
            let mut extra: *mut c_void = ptr::null_mut();
            debug_assert!(newval >= (*conf).min);
            debug_assert!(newval <= (*conf).max);
            if !call_int_check_hook(conf, &mut newval, &mut extra, GucSource::PGC_S_DEFAULT, LOG) {
                elog!(FATAL, "failed to initialize {} to {}", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy(), newval);
            }
            if let Some(h) = (*conf).assign_hook { h(newval, extra); }
            *(*conf).variable = newval;
            (*conf).reset_val = newval;
            (*gconf).extra = extra;
            (*conf).reset_extra = extra;
        }
        config_type::PGC_REAL => {
            let conf = gconf as *mut config_real;
            let mut newval = (*conf).boot_val;
            let mut extra: *mut c_void = ptr::null_mut();
            debug_assert!(newval >= (*conf).min);
            debug_assert!(newval <= (*conf).max);
            if !call_real_check_hook(conf, &mut newval, &mut extra, GucSource::PGC_S_DEFAULT, LOG) {
                elog!(FATAL, "failed to initialize {} to {}", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy(), newval);
            }
            if let Some(h) = (*conf).assign_hook { h(newval, extra); }
            *(*conf).variable = newval;
            (*conf).reset_val = newval;
            (*gconf).extra = extra;
            (*conf).reset_extra = extra;
        }
        config_type::PGC_STRING => {
            let conf = gconf as *mut config_string;
            let mut newval: *mut c_char = if !(*conf).boot_val.is_null() {
                guc_strdup(FATAL, (*conf).boot_val)
            } else {
                ptr::null_mut()
            };
            let mut extra: *mut c_void = ptr::null_mut();
            if !call_string_check_hook(conf, &mut newval, &mut extra, GucSource::PGC_S_DEFAULT, LOG) {
                elog!(FATAL, "failed to initialize {}", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy());
            }
            if let Some(h) = (*conf).assign_hook { h(newval, extra); }
            *(*conf).variable = newval;
            (*conf).reset_val = newval;
            (*gconf).extra = extra;
            (*conf).reset_extra = extra;
        }
        config_type::PGC_ENUM => {
            let conf = gconf as *mut config_enum;
            let mut newval = (*conf).boot_val;
            let mut extra: *mut c_void = ptr::null_mut();
            if !call_enum_check_hook(conf, &mut newval, &mut extra, GucSource::PGC_S_DEFAULT, LOG) {
                elog!(FATAL, "failed to initialize {} to {}", std::ffi::CStr::from_ptr((*gconf).name).to_string_lossy(), newval);
            }
            if let Some(h) = (*conf).assign_hook { h(newval, extra); }
            *(*conf).variable = newval;
            (*conf).reset_val = newval;
            (*gconf).extra = extra;
            (*conf).reset_extra = extra;
        }
    }
}

// ---------------------------------------------------------------------------
// RemoveGUCFromLists
// ---------------------------------------------------------------------------

unsafe fn RemoveGUCFromLists(gconf: *mut config_generic) {
    // TODO(pg-port): dlist_delete / slist_delete need proper ilist wrappers
    // For now: stubs that do nothing (safe since lists are only used for iteration)
    // TODO(pg-port): implement dlist_delete(&gconf->nondef_link) when ilist API is available
    // TODO(pg-port): implement slist_delete for stack_link and report_link
    let _ = gconf;
}

// ---------------------------------------------------------------------------
// set_guc_source
// ---------------------------------------------------------------------------

unsafe fn set_guc_source(gconf: *mut config_generic, newsource: GucSource) {
    /* Adjust nondef list membership if appropriate for change */
    if (*gconf).source == GucSource::PGC_S_DEFAULT {
        if newsource != GucSource::PGC_S_DEFAULT {
            dlist_push_tail(&mut guc_nondef_list, &mut (*gconf).nondef_link);
        }
    } else {
        if newsource == GucSource::PGC_S_DEFAULT {
            dlist_delete(&mut (*gconf).nondef_link);
        }
    }
    /* Now update the source field */
    (*gconf).source = newsource;
}

// ---------------------------------------------------------------------------
// push_old_value / AtStart_GUC / NewGUCNestLevel / RestrictSearchPath / AtEOXact_GUC
// ---------------------------------------------------------------------------

unsafe fn push_old_value(gconf: *mut config_generic, action: GucAction) {
    if GUCNestLevel == 0 { return; }

    // Do we already have a stack entry of the current nest level?
    let stack = (*gconf).stack;
    if !stack.is_null() && (*stack).nest_level >= GUCNestLevel {
        debug_assert!((*stack).nest_level == GUCNestLevel);
        match action {
            GucAction::GUC_ACTION_SET => {
                if (*stack).state == GucStackState::GUC_SET_LOCAL {
                    discard_stack_value(gconf, &mut (*stack).masked);
                }
                (*stack).state = GucStackState::GUC_SET;
            }
            GucAction::GUC_ACTION_LOCAL => {
                if (*stack).state == GucStackState::GUC_SET {
                    (*stack).masked_scontext = (*gconf).scontext;
                    (*stack).masked_srole = (*gconf).srole;
                    set_stack_value(gconf, &mut (*stack).masked);
                    (*stack).state = GucStackState::GUC_SET_LOCAL;
                }
                // in all other cases, no change to stack entry
            }
            GucAction::GUC_ACTION_SAVE => {
                debug_assert!((*stack).state == GucStackState::GUC_SAVE);
            }
        }
        return;
    }

    // Push a new stack entry
    // Cast: mcxt::MemoryContextData and palloc::MemoryContextData are structurally identical opaque types
    let stack = MemoryContextAllocZero(TopTransactionContext as crate::utils::palloc::MemoryContext, std::mem::size_of::<GucStack>()) as *mut GucStack;
    (*stack).prev = (*gconf).stack;
    (*stack).nest_level = GUCNestLevel;
    (*stack).state = match action {
        GucAction::GUC_ACTION_SET   => GucStackState::GUC_SET,
        GucAction::GUC_ACTION_LOCAL => GucStackState::GUC_LOCAL,
        GucAction::GUC_ACTION_SAVE  => GucStackState::GUC_SAVE,
    };
    (*stack).source   = (*gconf).source;
    (*stack).scontext = (*gconf).scontext;
    (*stack).srole    = (*gconf).srole;
    set_stack_value(gconf, &mut (*stack).prior);

    // TODO(pg-port): slist_push_head(&guc_stack_list, &gconf->stack_link) once ilist is wired
    (*gconf).stack = stack;
}

pub unsafe fn AtStart_GUC() {
    if GUCNestLevel != 0 {
        elog!(WARNING, "GUC nest level = {} at transaction start", GUCNestLevel);
    }
    GUCNestLevel = 1;
}

pub unsafe fn NewGUCNestLevel() -> c_int {
    GUCNestLevel += 1;
    GUCNestLevel
}

pub unsafe fn RestrictSearchPath() {
    if !IsBootstrapProcessingMode() {
        set_config_option(
            b"search_path\0".as_ptr() as *const c_char,
            GUC_SAFE_SEARCH_PATH.as_ptr() as *const c_char,
            GucContext::PGC_USERSET,
            GucSource::PGC_S_SESSION,
            GucAction::GUC_ACTION_SAVE,
            true,
            0,
            false,
        );
    }
}

pub unsafe fn AtEOXact_GUC(isCommit: bool, nestLevel: c_int) {
    // TODO(pg-port): slist_foreach_modify - needs proper slist iterator
    // Skeleton: iterate guc_stack_list when ilist wrappers are available
    GUCNestLevel = nestLevel - 1;
}

// ---------------------------------------------------------------------------
// BeginReportingGUCOptions / ReportChangedGUCOptions / ReportGUCOption
// ---------------------------------------------------------------------------

pub unsafe fn BeginReportingGUCOptions() {
    if whereToSendOutput() != DEST_REMOTE {
        return;
    }
    reporting_enabled = true;

    // Hack for in_hot_standby
    if RecoveryInProgress() {
        SetConfigOption(
            b"in_hot_standby\0".as_ptr() as *const c_char,
            b"true\0".as_ptr() as *const c_char,
            GucContext::PGC_INTERNAL,
            GucSource::PGC_S_OVERRIDE,
        );
    }

    // Transmit initial values of interesting variables
    let mut status: HASH_SEQ_STATUS = std::mem::zeroed();
    hash_seq_init(&mut status, guc_hashtab);
    loop {
        let hentry = hash_seq_search(&mut status) as *mut GUCHashEntry;
        if hentry.is_null() { break; }
        let conf = (*hentry).gucvar;
        if ((*conf).flags & GUC_REPORT) != 0 {
            ReportGUCOption(conf);
        }
    }
}

pub unsafe fn ReportChangedGUCOptions() {
    if !reporting_enabled { return; }

    // Hack: check in_hot_standby
    if in_hot_standby_guc && !RecoveryInProgress() {
        SetConfigOption(
            b"in_hot_standby\0".as_ptr() as *const c_char,
            b"false\0".as_ptr() as *const c_char,
            GucContext::PGC_INTERNAL,
            GucSource::PGC_S_OVERRIDE,
        );
    }

    // TODO(pg-port): slist_foreach_modify(iter, &guc_report_list) once ilist is wired
}

unsafe fn ReportGUCOption(record: *mut config_generic) {
    let val = ShowGUCOption(record, false);

    let needs_send = if (*record).last_reported.is_null() {
        true
    } else {
        strcmp(val, (*record).last_reported) != 0
    };

    if needs_send {
        let mut msgbuf: StringInfoData = std::mem::zeroed();
        pq_beginmessage(&mut msgbuf, PqMsg_ParameterStatus);
        pq_sendstring(&mut msgbuf, (*record).name);
        pq_sendstring(&mut msgbuf, val);
        pq_endmessage(&mut msgbuf);

        guc_free((*record).last_reported as *mut c_void);
        (*record).last_reported = guc_strdup(LOG, val);
    }

    pfree(val as *mut c_void);
}

// ---------------------------------------------------------------------------
// Unit conversion
// ---------------------------------------------------------------------------

/// Convert a value from a human-friendly unit to the given base unit.
unsafe fn convert_to_base_unit(
    value: c_double,
    unit: *const c_char,
    base_unit: c_int,
    base_value: *mut c_double,
) -> bool {
    let mut unitstr = [0u8; MAX_UNIT_LEN + 1];
    let mut unitlen: usize = 0;
    let mut p = unit;
    while *p != 0 && isspace(*p as c_int) == 0 && unitlen < MAX_UNIT_LEN {
        unitstr[unitlen] = *p as u8;
        unitlen += 1;
        p = p.add(1);
    }
    // allow whitespace after unit
    while isspace(*p as c_int) != 0 { p = p.add(1); }
    if *p != 0 { return false; } // unit too long or garbage after it

    let table: &[unit_conversion] = if (base_unit & GUC_UNIT_MEMORY) != 0 {
        MEMORY_UNIT_CONVERSION_TABLE
    } else {
        TIME_UNIT_CONVERSION_TABLE
    };

    let mut i = 0;
    while table[i].unit[0] != 0 {
        if base_unit == table[i].base_unit && &unitstr[..unitlen] == &table[i].unit[..unitlen] {
            let mut cvalue = value * table[i].multiplier;
            // Round to next smaller unit if fractional
            if i + 1 < table.len() && table[i + 1].unit[0] != 0
                && base_unit == table[i + 1].base_unit
            {
                cvalue = rint(cvalue / table[i + 1].multiplier) * table[i + 1].multiplier;
            }
            *base_value = cvalue;
            return true;
        }
        i += 1;
    }
    false
}

/// Convert int value in base unit to human-friendly unit.
unsafe fn convert_int_from_base_unit(
    base_value: i64,
    base_unit: c_int,
    value: *mut i64,
    unit: *mut *const c_char,
) {
    *unit = ptr::null();
    let table: &[unit_conversion] = if (base_unit & GUC_UNIT_MEMORY) != 0 {
        MEMORY_UNIT_CONVERSION_TABLE
    } else {
        TIME_UNIT_CONVERSION_TABLE
    };
    let mut i = 0;
    while table[i].unit[0] != 0 {
        if base_unit == table[i].base_unit {
            if table[i].multiplier <= 1.0
                || base_value % (table[i].multiplier as i64) == 0
            {
                *value = (base_value as f64 / table[i].multiplier).round() as i64;
                *unit = table[i].unit.as_ptr() as *const c_char;
                break;
            }
        }
        i += 1;
    }
    debug_assert!(!(*unit).is_null());
}

/// Convert float value in base unit to human-friendly unit.
unsafe fn convert_real_from_base_unit(
    base_value: c_double,
    base_unit: c_int,
    value: *mut c_double,
    unit: *mut *const c_char,
) {
    *unit = ptr::null();
    let table: &[unit_conversion] = if (base_unit & GUC_UNIT_MEMORY) != 0 {
        MEMORY_UNIT_CONVERSION_TABLE
    } else {
        TIME_UNIT_CONVERSION_TABLE
    };
    let mut i = 0;
    while table[i].unit[0] != 0 {
        if base_unit == table[i].base_unit {
            *value = base_value / table[i].multiplier;
            *unit = table[i].unit.as_ptr() as *const c_char;
            if *value > 0.0 && fabs((rint(*value) / *value) - 1.0) <= 1e-8 {
                break;
            }
        }
        i += 1;
    }
    debug_assert!(!(*unit).is_null());
}

/// Return name of a GUC's base unit (e.g. "ms"), or NULL if unitless.
pub unsafe fn get_config_unit_name(flags: c_int) -> *const c_char {
    match flags & GUC_UNIT {
        0            => ptr::null(),
        GUC_UNIT_BYTE  => b"B\0".as_ptr() as *const c_char,
        GUC_UNIT_KB    => b"kB\0".as_ptr() as *const c_char,
        GUC_UNIT_MB    => b"MB\0".as_ptr() as *const c_char,
        GUC_UNIT_BLOCKS => {
            static mut BBUF: [u8; 8] = [0u8; 8];
            if BBUF[0] == 0 {
                snprintf(BBUF.as_mut_ptr() as *mut c_char, 8, b"{}kB\0".as_ptr() as *const c_char, BLCKSZ as c_int / 1024);
            }
            BBUF.as_ptr() as *const c_char
        }
        GUC_UNIT_XBLOCKS => {
            static mut XBUF: [u8; 8] = [0u8; 8];
            if XBUF[0] == 0 {
                snprintf(XBUF.as_mut_ptr() as *mut c_char, 8, b"{}kB\0".as_ptr() as *const c_char, XLOG_BLCKSZ as c_int / 1024);
            }
            XBUF.as_ptr() as *const c_char
        }
        GUC_UNIT_MS    => b"ms\0".as_ptr() as *const c_char,
        GUC_UNIT_S     => b"s\0".as_ptr() as *const c_char,
        GUC_UNIT_MIN   => b"min\0".as_ptr() as *const c_char,
        _ => {
            elog!(ERROR, "unrecognized GUC units value: {}", flags & GUC_UNIT);
            ptr::null()
        }
    }
}

// ---------------------------------------------------------------------------
// parse_int / parse_real
// ---------------------------------------------------------------------------

/// Try to parse value as an integer, with optional unit.
pub unsafe fn parse_int(
    value: *const c_char,
    result: *mut c_int,
    flags: c_int,
    hintmsg: *mut *const c_char,
) -> bool {
    let mut val: c_double;
    let mut endptr: *mut c_char = ptr::null_mut();

    if !result.is_null()   { *result = 0; }
    if !hintmsg.is_null()  { *hintmsg = ptr::null(); }

    // Try integer parse first (allowing octal or hex)
    let err_saved = errno();
    val = strtol(value, &mut endptr, 0) as c_double;
    if *endptr == b'.' as c_char || *endptr == b'e' as c_char || *endptr == b'E' as c_char || errno() == 34 /* ERANGE */ {
        val = strtod(value, &mut endptr);
    }
    if endptr == value as *mut c_char || errno() == 34 { return false; }
    if isnan(val) != 0 { return false; }

    // Allow whitespace between number and unit
    while isspace(*endptr as c_int) != 0 { endptr = endptr.add(1); }

    if *endptr != 0 {
        if (flags & GUC_UNIT) == 0 { return false; }
        if !convert_to_base_unit(val, endptr as *const c_char, flags & GUC_UNIT, &mut val) {
            if !hintmsg.is_null() {
                *hintmsg = if (flags & GUC_UNIT_MEMORY) != 0 {
                    memory_units_hint.as_ptr() as *const c_char
                } else {
                    time_units_hint.as_ptr() as *const c_char
                };
            }
            return false;
        }
    }

    val = rint(val);
    if val > i32::MAX as c_double || val < i32::MIN as c_double {
        if !hintmsg.is_null() {
            *hintmsg = b"Value exceeds integer range.\0".as_ptr() as *const c_char;
        }
        return false;
    }
    if !result.is_null() { *result = val as c_int; }
    true
}

/// Try to parse value as a floating point number, with optional unit.
pub unsafe fn parse_real(
    value: *const c_char,
    result: *mut c_double,
    flags: c_int,
    hintmsg: *mut *const c_char,
) -> bool {
    let mut val: c_double;
    let mut endptr: *mut c_char = ptr::null_mut();

    if !result.is_null()   { *result = 0.0; }
    if !hintmsg.is_null()  { *hintmsg = ptr::null(); }

    val = strtod(value, &mut endptr);
    if endptr == value as *mut c_char || errno() == 34 { return false; }
    if isnan(val) != 0 { return false; }

    while isspace(*endptr as c_int) != 0 { endptr = endptr.add(1); }

    if *endptr != 0 {
        if (flags & GUC_UNIT) == 0 { return false; }
        if !convert_to_base_unit(val, endptr as *const c_char, flags & GUC_UNIT, &mut val) {
            if !hintmsg.is_null() {
                *hintmsg = if (flags & GUC_UNIT_MEMORY) != 0 {
                    memory_units_hint.as_ptr() as *const c_char
                } else {
                    time_units_hint.as_ptr() as *const c_char
                };
            }
            return false;
        }
    }

    if !result.is_null() { *result = val; }
    true
}

// ---------------------------------------------------------------------------
// config_enum lookup / ShowGUCOption
// ---------------------------------------------------------------------------

/// Lookup the name for an enum option with the selected value.
pub unsafe fn config_enum_lookup_by_value(record: *mut config_enum, val: c_int) -> *const c_char {
    let mut entry = (*record).options;
    while !entry.is_null() && !(*entry).name.is_null() {
        if (*entry).val == val {
            return (*entry).name;
        }
        entry = entry.add(1);
    }
    elog!(ERROR, "could not find enum option {} for {}", val, std::ffi::CStr::from_ptr((*record).gen.name).to_string_lossy());
    ptr::null() // silence compiler
}

/// Lookup the value for an enum option with the selected name (case-insensitive).
pub unsafe fn config_enum_lookup_by_name(record: *mut config_enum, value: *const c_char, retval: *mut c_int) -> bool {
    let mut entry = (*record).options;
    while !entry.is_null() && !(*entry).name.is_null() {
        if pg_strcasecmp(value, (*entry).name) == 0 {
            *retval = (*entry).val;
            return true;
        }
        entry = entry.add(1);
    }
    *retval = 0;
    false
}

/// Return a palloc'd string listing all available options for an enum GUC.
pub unsafe fn config_enum_get_options(
    record: *mut config_enum,
    prefix: *const c_char,
    suffix: *const c_char,
    separator: *const c_char,
) -> *mut c_char {
    let mut retstr: StringInfoData = std::mem::zeroed();
    initStringInfo(&mut retstr);
    appendStringInfoString(&mut retstr, prefix);
    let seplen = strlen(separator);

    let mut entry = (*record).options;
    while !entry.is_null() && !(*entry).name.is_null() {
        if !(*entry).hidden {
            appendStringInfoString(&mut retstr, (*entry).name);
            appendBinaryStringInfo(&mut retstr, separator as *const c_void, seplen as c_int);
        }
        entry = entry.add(1);
    }

    if retstr.len >= seplen as c_int {
        *retstr.data.add((retstr.len - seplen as c_int) as usize) = 0;
        retstr.len -= seplen as c_int;
    }

    appendStringInfoString(&mut retstr, suffix);
    retstr.data
}

/// Get string value of a GUC variable, optionally with unit suffix.
pub unsafe fn ShowGUCOption(record: *mut config_generic, use_units: bool) -> *mut c_char {
    let mut buffer = [0u8; 256];
    let val: *const c_char;

    match (*record).vartype {
        config_type::PGC_BOOL => {
            let conf = record as *mut config_bool;
            if let Some(h) = (*conf).show_hook {
                val = h();
            } else {
                val = if *(*conf).variable { b"on\0".as_ptr() as *const c_char } else { b"off\0".as_ptr() as *const c_char };
            }
        }
        config_type::PGC_INT => {
            let conf = record as *mut config_int;
            if let Some(h) = (*conf).show_hook {
                val = h();
            } else {
                let mut result = *(*conf).variable as i64;
                let mut unit: *const c_char = ptr::null();
                if use_units && result > 0 && ((*record).flags & GUC_UNIT) != 0 {
                    convert_int_from_base_unit(result, (*record).flags & GUC_UNIT, &mut result, &mut unit);
                    // TODO(pg-port): INT64_FORMAT = "{}" on most platforms
                    snprintf(buffer.as_mut_ptr() as *mut c_char, 256, b"{}{}\0".as_ptr() as *const c_char, result, if unit.is_null() { b"\0".as_ptr() as *const c_char } else { unit });
                } else {
                    snprintf(buffer.as_mut_ptr() as *mut c_char, 256, b"{}\0".as_ptr() as *const c_char, *(*conf).variable);
                }
                val = buffer.as_ptr() as *const c_char;
            }
        }
        config_type::PGC_REAL => {
            let conf = record as *mut config_real;
            if let Some(h) = (*conf).show_hook {
                val = h();
            } else {
                let mut result = *(*conf).variable;
                let mut unit: *const c_char = ptr::null();
                if use_units && result > 0.0 && ((*record).flags & GUC_UNIT) != 0 {
                    convert_real_from_base_unit(result, (*record).flags & GUC_UNIT, &mut result, &mut unit);
                    snprintf(buffer.as_mut_ptr() as *mut c_char, 256, b"{}{}\0".as_ptr() as *const c_char, result, if unit.is_null() { b"\0".as_ptr() as *const c_char } else { unit });
                } else {
                    snprintf(buffer.as_mut_ptr() as *mut c_char, 256, b"{}\0".as_ptr() as *const c_char, result);
                }
                val = buffer.as_ptr() as *const c_char;
            }
        }
        config_type::PGC_STRING => {
            let conf = record as *mut config_string;
            if let Some(h) = (*conf).show_hook {
                val = h();
            } else if !(*(*conf).variable).is_null() && *(*(*conf).variable) != 0 {
                val = *(*conf).variable;
            } else {
                val = b"\0".as_ptr() as *const c_char;
            }
        }
        config_type::PGC_ENUM => {
            let conf = record as *mut config_enum;
            if let Some(h) = (*conf).show_hook {
                val = h();
            } else {
                val = config_enum_lookup_by_value(conf, *(*conf).variable);
            }
        }
    }

    pstrdup(val)
}

// ---------------------------------------------------------------------------
// GetConfigOption / GetConfigOptionResetString / GetConfigOptionFlags
// ---------------------------------------------------------------------------

pub unsafe fn GetConfigOption(
    name: *const c_char,
    missing_ok: bool,
    restrict_privileged: bool,
) -> *const c_char {
    static mut BUFFER: [u8; 256] = [0u8; 256];
    let record = find_option(name, false, missing_ok, ERROR);
    if record.is_null() { return ptr::null(); }
    if restrict_privileged && !ConfigOptionIsVisible(record) {
        ereport!(ERROR, errmsg!("permission denied to examine \"{}\"", std::ffi::CStr::from_ptr(name).to_string_lossy()));
    }
    match (*record).vartype {
        config_type::PGC_BOOL => {
            if *(*( record as *mut config_bool)).variable { b"on\0".as_ptr() as *const c_char }
            else { b"off\0".as_ptr() as *const c_char }
        }
        config_type::PGC_INT => {
            snprintf(BUFFER.as_mut_ptr() as *mut c_char, 256, b"{}\0".as_ptr() as *const c_char, *( *(record as *mut config_int)).variable);
            BUFFER.as_ptr() as *const c_char
        }
        config_type::PGC_REAL => {
            snprintf(BUFFER.as_mut_ptr() as *mut c_char, 256, b"{}\0".as_ptr() as *const c_char, *( *(record as *mut config_real)).variable);
            BUFFER.as_ptr() as *const c_char
        }
        config_type::PGC_STRING => {
            let v = *(*(record as *mut config_string)).variable;
            if !v.is_null() { v } else { b"\0".as_ptr() as *const c_char }
        }
        config_type::PGC_ENUM => {
            config_enum_lookup_by_value(record as *mut config_enum, *( *(record as *mut config_enum)).variable)
        }
    }
}

pub unsafe fn GetConfigOptionResetString(name: *const c_char) -> *const c_char {
    static mut BUFFER: [u8; 256] = [0u8; 256];
    let record = find_option(name, false, false, ERROR);
    debug_assert!(!record.is_null());
    if !ConfigOptionIsVisible(record) {
        ereport!(ERROR, errmsg!("permission denied to examine \"{}\"", std::ffi::CStr::from_ptr(name).to_string_lossy()));
    }
    match (*record).vartype {
        config_type::PGC_BOOL => {
            if (*(record as *mut config_bool)).reset_val { b"on\0".as_ptr() as *const c_char }
            else { b"off\0".as_ptr() as *const c_char }
        }
        config_type::PGC_INT => {
            snprintf(BUFFER.as_mut_ptr() as *mut c_char, 256, b"{}\0".as_ptr() as *const c_char, (*(record as *mut config_int)).reset_val);
            BUFFER.as_ptr() as *const c_char
        }
        config_type::PGC_REAL => {
            snprintf(BUFFER.as_mut_ptr() as *mut c_char, 256, b"{}\0".as_ptr() as *const c_char, (*(record as *mut config_real)).reset_val);
            BUFFER.as_ptr() as *const c_char
        }
        config_type::PGC_STRING => {
            let v = (*(record as *mut config_string)).reset_val;
            if !v.is_null() { v } else { b"\0".as_ptr() as *const c_char }
        }
        config_type::PGC_ENUM => {
            config_enum_lookup_by_value(record as *mut config_enum, (*(record as *mut config_enum)).reset_val)
        }
    }
}

pub unsafe fn GetConfigOptionFlags(name: *const c_char, missing_ok: bool) -> c_int {
    let record = find_option(name, false, missing_ok, ERROR);
    if record.is_null() { return 0; }
    (*record).flags
}

/// Stub for ConfigOptionIsVisible (defined in guc_funcs.c).
/// TODO(pg-port): replace with real check once guc_funcs is wired to this module.
pub unsafe fn ConfigOptionIsVisible(conf: *const config_generic) -> bool {
    if ((*conf).flags & GUC_SUPERUSER_ONLY) != 0 {
        return superuser();
    }
    true
}

pub unsafe fn GetConfigOptionByName(
    name: *const c_char,
    varname: *mut *const c_char,
    missing_ok: bool,
) -> *mut c_char {
    let record = find_option(name, false, missing_ok, ERROR);
    if record.is_null() {
        if !varname.is_null() { *varname = ptr::null(); }
        return ptr::null_mut();
    }
    if !ConfigOptionIsVisible(record) {
        ereport!(ERROR, errmsg!("permission denied to examine \"{}\"", std::ffi::CStr::from_ptr(name).to_string_lossy()));
    }
    if !varname.is_null() { *varname = (*record).name; }
    ShowGUCOption(record, true)
}

// ---------------------------------------------------------------------------
// parse_and_validate_value
// ---------------------------------------------------------------------------

unsafe fn parse_and_validate_value(
    record: *mut config_generic,
    value: *const c_char,
    source: GucSource,
    elevel: c_int,
    newval: *mut config_var_val,
    newextra: *mut *mut c_void,
) -> bool {
    match (*record).vartype {
        config_type::PGC_BOOL => {
            let conf = record as *mut config_bool;
            if !parse_bool(value, &mut (*newval).boolval) {
                ereport!(elevel, errmsg!("parameter \"{}\" requires a Boolean value", std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy()));
                return false;
            }
            if !call_bool_check_hook(conf, &mut (*newval).boolval, newextra, source, elevel) {
                return false;
            }
        }
        config_type::PGC_INT => {
            let conf = record as *mut config_int;
            let mut hintmsg: *const c_char = ptr::null();
            if !parse_int(value, &mut (*newval).intval, (*conf).gen.flags, &mut hintmsg) {
                ereport!(elevel, errmsg!("invalid value for parameter \"{}\": \"{}\"", std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy(), std::ffi::CStr::from_ptr(value).to_string_lossy()));
                return false;
            }
            if (*newval).intval < (*conf).min || (*newval).intval > (*conf).max {
                let unit = get_config_unit_name((*conf).gen.flags);
                let unitspace: *const c_char = if !unit.is_null() { b" \0".as_ptr() as *const c_char } else { b"\0".as_ptr() as *const c_char };
                let unit = if unit.is_null() { b"\0".as_ptr() as *const c_char } else { unit };
                ereport!(elevel, errmsg!(
                    "{}{}{} is outside the valid range for parameter \"{}\" ({}{}{} .. {}{}{})",
                    (*newval).intval, std::ffi::CStr::from_ptr(unitspace).to_string_lossy(), std::ffi::CStr::from_ptr(unit).to_string_lossy(),
                    std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy(),
                    (*conf).min, std::ffi::CStr::from_ptr(unitspace).to_string_lossy(), std::ffi::CStr::from_ptr(unit).to_string_lossy(),
                    (*conf).max, std::ffi::CStr::from_ptr(unitspace).to_string_lossy(), std::ffi::CStr::from_ptr(unit).to_string_lossy()
                ));
                return false;
            }
            if !call_int_check_hook(conf, &mut (*newval).intval, newextra, source, elevel) {
                return false;
            }
        }
        config_type::PGC_REAL => {
            let conf = record as *mut config_real;
            let mut hintmsg: *const c_char = ptr::null();
            if !parse_real(value, &mut (*newval).realval, (*conf).gen.flags, &mut hintmsg) {
                ereport!(elevel, errmsg!("invalid value for parameter \"{}\": \"{}\"", std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy(), std::ffi::CStr::from_ptr(value).to_string_lossy()));
                return false;
            }
            if (*newval).realval < (*conf).min || (*newval).realval > (*conf).max {
                let unit = get_config_unit_name((*conf).gen.flags);
                let unitspace: *const c_char = if !unit.is_null() { b" \0".as_ptr() as *const c_char } else { b"\0".as_ptr() as *const c_char };
                let unit = if unit.is_null() { b"\0".as_ptr() as *const c_char } else { unit };
                ereport!(elevel, errmsg!(
                    "{}{}{} is outside the valid range for parameter \"{}\" ({}{}{} .. {}{}{})",
                    (*newval).realval, std::ffi::CStr::from_ptr(unitspace).to_string_lossy(), std::ffi::CStr::from_ptr(unit).to_string_lossy(),
                    std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy(),
                    (*conf).min, std::ffi::CStr::from_ptr(unitspace).to_string_lossy(), std::ffi::CStr::from_ptr(unit).to_string_lossy(),
                    (*conf).max, std::ffi::CStr::from_ptr(unitspace).to_string_lossy(), std::ffi::CStr::from_ptr(unit).to_string_lossy()
                ));
                return false;
            }
            if !call_real_check_hook(conf, &mut (*newval).realval, newextra, source, elevel) {
                return false;
            }
        }
        config_type::PGC_STRING => {
            let conf = record as *mut config_string;
            (*newval).stringval = guc_strdup(elevel, value);
            if (*newval).stringval.is_null() { return false; }
            if ((*conf).gen.flags & GUC_IS_NAME) != 0 {
                truncate_identifier((*newval).stringval, strlen((*newval).stringval), true);
            }
            if !call_string_check_hook(conf, &mut (*newval).stringval, newextra, source, elevel) {
                guc_free((*newval).stringval as *mut c_void);
                (*newval).stringval = ptr::null_mut();
                return false;
            }
        }
        config_type::PGC_ENUM => {
            let conf = record as *mut config_enum;
            if !config_enum_lookup_by_name(conf, value, &mut (*newval).enumval) {
                let hintmsg = config_enum_get_options(conf, b"Available values: \0".as_ptr() as *const c_char, b".\0".as_ptr() as *const c_char, b", \0".as_ptr() as *const c_char);
                ereport!(elevel, errmsg!("invalid value for parameter \"{}\": \"{}\"", std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy(), std::ffi::CStr::from_ptr(value).to_string_lossy()));
                if !hintmsg.is_null() { pfree(hintmsg as *mut c_void); }
                return false;
            }
            if !call_enum_check_hook(conf, &mut (*newval).enumval, newextra, source, elevel) {
                return false;
            }
        }
    }
    true
}

// ---------------------------------------------------------------------------
// set_config_option / set_config_option_ext / set_config_with_handle
// ---------------------------------------------------------------------------

/// Sets option `name' to given value.  Main external entry point.
pub unsafe fn set_config_option(
    name: *const c_char,
    value: *const c_char,
    context: GucContext,
    source: GucSource,
    action: GucAction,
    changeVal: bool,
    elevel: c_int,
    is_reload: bool,
) -> c_int {
    let srole = if source >= GucSource::PGC_S_INTERACTIVE || source == GucSource::PGC_S_CLIENT {
        GetUserId()
    } else {
        BOOTSTRAP_SUPERUSERID
    };
    set_config_with_handle(name, ptr::null_mut(), value, context, source, srole, action, changeVal, elevel, is_reload)
}

/// Like set_config_option but lets caller specify which role OID is setting the value.
pub unsafe fn set_config_option_ext(
    name: *const c_char,
    value: *const c_char,
    context: GucContext,
    source: GucSource,
    srole: Oid,
    action: GucAction,
    changeVal: bool,
    elevel: c_int,
    is_reload: bool,
) -> c_int {
    set_config_with_handle(name, ptr::null_mut(), value, context, source, srole, action, changeVal, elevel, is_reload)
}

/// Core function for setting a GUC variable.
pub unsafe fn set_config_with_handle(
    name: *const c_char,
    handle: *mut config_handle,
    value: *const c_char,
    context: GucContext,
    source: GucSource,
    srole: Oid,
    action: GucAction,
    mut changeVal: bool,
    mut elevel: c_int,
    is_reload: bool,
) -> c_int {
    let record: *mut config_generic;
    let mut newval_union: config_var_val = Default::default();
    let mut newextra: *mut c_void = ptr::null_mut();
    let mut prohibitValueChange = false;
    let makeDefault: bool;
    let context = context;

    if elevel == 0 {
        elevel = if source == GucSource::PGC_S_DEFAULT || source == GucSource::PGC_S_FILE {
            if IsUnderPostmaster() { DEBUG3 } else { LOG }
        } else if source == GucSource::PGC_S_GLOBAL || source == GucSource::PGC_S_DATABASE
            || source == GucSource::PGC_S_USER || source == GucSource::PGC_S_DATABASE_USER
        {
            WARNING
        } else {
            ERROR
        };
    }

    if handle.is_null() {
        record = find_option(name, true, false, elevel);
        if record.is_null() { return 0; }
    } else {
        record = handle;
    }

    // Check parallel mode restriction
    if IsInParallelMode() && changeVal && action != GucAction::GUC_ACTION_SAVE
        && ((*record).flags & GUC_ALLOW_IN_PARALLEL) == 0
    {
        ereport!(elevel, errmsg!("parameter \"{}\" cannot be set during a parallel operation", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
        return 0;
    }

    // Check context restrictions
    match (*record).context {
        GucContext::PGC_INTERNAL => {
            if context != GucContext::PGC_INTERNAL {
                ereport!(elevel, errmsg!("parameter \"{}\" cannot be changed", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
                return 0;
            }
        }
        GucContext::PGC_POSTMASTER => {
            if context == GucContext::PGC_SIGHUP {
                prohibitValueChange = true;
            } else if context != GucContext::PGC_POSTMASTER {
                ereport!(elevel, errmsg!("parameter \"{}\" cannot be changed without restarting the server", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
                return 0;
            }
        }
        GucContext::PGC_SIGHUP => {
            if context != GucContext::PGC_SIGHUP && context != GucContext::PGC_POSTMASTER {
                ereport!(elevel, errmsg!("parameter \"{}\" cannot be changed now", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
                return 0;
            }
        }
        GucContext::PGC_SU_BACKEND => {
            if context == GucContext::PGC_BACKEND {
                let aclresult = pg_parameter_aclcheck((*record).name, srole, ACL_SET);
                if aclresult != ACLCHECK_OK {
                    ereport!(elevel, errmsg!("permission denied to set parameter \"{}\"", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
                    return 0;
                }
            }
            // fall through to PGC_BACKEND handling
            if context == GucContext::PGC_SIGHUP {
                if IsUnderPostmaster() && changeVal && !is_reload { return -1; }
            } else if context != GucContext::PGC_POSTMASTER && context != GucContext::PGC_BACKEND
                && context != GucContext::PGC_SU_BACKEND && source != GucSource::PGC_S_CLIENT
            {
                ereport!(elevel, errmsg!("parameter \"{}\" cannot be set after connection start", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
                return 0;
            }
        }
        GucContext::PGC_BACKEND => {
            if context == GucContext::PGC_SIGHUP {
                if IsUnderPostmaster() && changeVal && !is_reload { return -1; }
            } else if context != GucContext::PGC_POSTMASTER && context != GucContext::PGC_BACKEND
                && context != GucContext::PGC_SU_BACKEND && source != GucSource::PGC_S_CLIENT
            {
                ereport!(elevel, errmsg!("parameter \"{}\" cannot be set after connection start", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
                return 0;
            }
        }
        GucContext::PGC_SUSET => {
            if context == GucContext::PGC_USERSET || context == GucContext::PGC_BACKEND {
                let aclresult = pg_parameter_aclcheck((*record).name, srole, ACL_SET);
                if aclresult != ACLCHECK_OK {
                    ereport!(elevel, errmsg!("permission denied to set parameter \"{}\"", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
                    return 0;
                }
            }
        }
        GucContext::PGC_USERSET => { /* always okay */ }
    }

    // Security restriction checks
    if ((*record).flags & GUC_NOT_WHILE_SEC_REST) != 0 {
        if InLocalUserIdChange() {
            ereport!(elevel, errmsg!("cannot set parameter \"{}\" within security-definer function", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
            return 0;
        }
        if InSecurityRestrictedOperation() {
            ereport!(elevel, errmsg!("cannot set parameter \"{}\" within security-restricted operation", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
            return 0;
        }
    }

    // Disallow resetting and saving GUC_NO_RESET values
    if ((*record).flags & GUC_NO_RESET) != 0 {
        if value.is_null() {
            ereport!(elevel, errmsg!("parameter \"{}\" cannot be reset", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
            return 0;
        }
        if action == GucAction::GUC_ACTION_SAVE {
            ereport!(elevel, errmsg!("parameter \"{}\" cannot be set locally in functions", std::ffi::CStr::from_ptr((*record).name).to_string_lossy()));
            return 0;
        }
    }

    makeDefault = changeVal && (source <= GucSource::PGC_S_OVERRIDE)
        && (!value.is_null() || source == GucSource::PGC_S_DEFAULT);

    if (*record).source > source {
        if changeVal && !makeDefault {
            // setting ignored because previous source is higher priority
            return -1;
        }
        changeVal = false;
    }

    // Evaluate value and set variable - handled per vartype below.
    // To keep the match arm lengths manageable we call a helper.
    set_config_value_inner(record, value, source, context, srole, action, changeVal, makeDefault, prohibitValueChange, &mut newval_union, &mut newextra, elevel, is_reload)
}

/// Inner dispatcher called from set_config_with_handle for the actual value assignment.
unsafe fn set_config_value_inner(
    record: *mut config_generic,
    value: *const c_char,
    mut source: GucSource,
    mut context: GucContext,
    mut srole: Oid,
    action: GucAction,
    changeVal: bool,
    makeDefault: bool,
    prohibitValueChange: bool,
    newval_union: *mut config_var_val,
    newextra: *mut *mut c_void,
    elevel: c_int,
    is_reload: bool,
) -> c_int {
    macro_rules! apply_non_string {
        ($conf:expr, $field:ident, $newval_field:ident, $pv_check:expr) => {{
            if prohibitValueChange {
                if !(*newextra).is_null() && !extra_field_used(&mut (*$conf).gen as *mut config_generic, *newextra) {
                    guc_free(*newextra);
                }
                if $pv_check {
                    (*record).status |= GUC_PENDING_RESTART;
                    ereport!(elevel, errmsg!("parameter \"{}\" cannot be changed without restarting the server", std::ffi::CStr::from_ptr((*$conf).gen.name).to_string_lossy()));
                    return 0;
                }
                (*record).status &= !GUC_PENDING_RESTART;
                return -1;
            }
            if changeVal {
                if !makeDefault { push_old_value(record, action); }
                if let Some(h) = (*$conf).assign_hook { h((*newval_union).$newval_field, *newextra); }
                *(*$conf).variable = (*newval_union).$newval_field;
                set_extra_field(record, &mut (*record).extra, *newextra);
                set_guc_source(record, source);
                (*record).scontext = context;
                (*record).srole = srole;
            }
            if makeDefault {
                if (*$conf).gen.reset_source <= source {
                    (*$conf).reset_val = (*newval_union).$newval_field;
                    set_extra_field(record, &mut (*$conf).reset_extra, *newextra);
                    (*$conf).gen.reset_source = source;
                    (*$conf).gen.reset_scontext = context;
                    (*$conf).gen.reset_srole = srole;
                }
                let mut stack = (*record).stack;
                while !stack.is_null() {
                    if (*stack).source <= source {
                        (*stack).prior.val.$newval_field = (*newval_union).$newval_field;
                        set_extra_field(record, &mut (*stack).prior.extra, *newextra);
                        (*stack).source = source;
                        (*stack).scontext = context;
                        (*stack).srole = srole;
                    }
                    stack = (*stack).prev;
                }
            }
            if !(*newextra).is_null() && !extra_field_used(record, *newextra) {
                guc_free(*newextra);
            }
        }};
    }

    match (*record).vartype {
        config_type::PGC_BOOL => {
            let conf = record as *mut config_bool;
            if !value.is_null() {
                if !parse_and_validate_value(record, value, source, elevel, newval_union, newextra) { return 0; }
            } else if source == GucSource::PGC_S_DEFAULT {
                (*newval_union).boolval = (*conf).boot_val;
                if !call_bool_check_hook(conf, &mut (*newval_union).boolval, newextra, source, elevel) { return 0; }
            } else {
                (*newval_union).boolval = (*conf).reset_val;
                *newextra = (*conf).reset_extra;
                source = (*conf).gen.reset_source;
                context = (*conf).gen.reset_scontext;
                srole = (*conf).gen.reset_srole;
            }
            apply_non_string!(conf, variable, boolval, *(*conf).variable != (*newval_union).boolval);
        }
        config_type::PGC_INT => {
            let conf = record as *mut config_int;
            if !value.is_null() {
                if !parse_and_validate_value(record, value, source, elevel, newval_union, newextra) { return 0; }
            } else if source == GucSource::PGC_S_DEFAULT {
                (*newval_union).intval = (*conf).boot_val;
                if !call_int_check_hook(conf, &mut (*newval_union).intval, newextra, source, elevel) { return 0; }
            } else {
                (*newval_union).intval = (*conf).reset_val;
                *newextra = (*conf).reset_extra;
                source = (*conf).gen.reset_source;
                context = (*conf).gen.reset_scontext;
                srole = (*conf).gen.reset_srole;
            }
            apply_non_string!(conf, variable, intval, *(*conf).variable != (*newval_union).intval);
        }
        config_type::PGC_REAL => {
            let conf = record as *mut config_real;
            if !value.is_null() {
                if !parse_and_validate_value(record, value, source, elevel, newval_union, newextra) { return 0; }
            } else if source == GucSource::PGC_S_DEFAULT {
                (*newval_union).realval = (*conf).boot_val;
                if !call_real_check_hook(conf, &mut (*newval_union).realval, newextra, source, elevel) { return 0; }
            } else {
                (*newval_union).realval = (*conf).reset_val;
                *newextra = (*conf).reset_extra;
                source = (*conf).gen.reset_source;
                context = (*conf).gen.reset_scontext;
                srole = (*conf).gen.reset_srole;
            }
            apply_non_string!(conf, variable, realval, *(*conf).variable != (*newval_union).realval);
        }
        config_type::PGC_STRING => {
            let conf = record as *mut config_string;
            let orig_context = context;
            let orig_source  = source;
            let orig_srole   = srole;
            if !value.is_null() {
                if !parse_and_validate_value(record, value, source, elevel, newval_union, newextra) { return 0; }
            } else if source == GucSource::PGC_S_DEFAULT {
                if !(*conf).boot_val.is_null() {
                    (*newval_union).stringval = guc_strdup(elevel, (*conf).boot_val);
                    if (*newval_union).stringval.is_null() { return 0; }
                } else {
                    (*newval_union).stringval = ptr::null_mut();
                }
                if !call_string_check_hook(conf, &mut (*newval_union).stringval, newextra, source, elevel) {
                    guc_free((*newval_union).stringval as *mut c_void);
                    return 0;
                }
            } else {
                (*newval_union).stringval = (*conf).reset_val;
                *newextra = (*conf).reset_extra;
                source = (*conf).gen.reset_source;
                context = (*conf).gen.reset_scontext;
                srole = (*conf).gen.reset_srole;
            }
            if prohibitValueChange {
                let newval_different = (*(*conf).variable).is_null() || (*newval_union).stringval.is_null()
                    || strcmp(*(*conf).variable, (*newval_union).stringval) != 0;
                if !(*newval_union).stringval.is_null() && !string_field_used(conf, (*newval_union).stringval) {
                    guc_free((*newval_union).stringval as *mut c_void);
                }
                if !(*newextra).is_null() && !extra_field_used(record, *newextra) {
                    guc_free(*newextra);
                }
                if newval_different {
                    (*record).status |= GUC_PENDING_RESTART;
                    ereport!(elevel, errmsg!("parameter \"{}\" cannot be changed without restarting the server", std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy()));
                    return 0;
                }
                (*record).status &= !GUC_PENDING_RESTART;
                return -1;
            }
            if changeVal {
                if !makeDefault { push_old_value(record, action); }
                if let Some(h) = (*conf).assign_hook { h((*newval_union).stringval, *newextra); }
                set_string_field(conf, (*conf).variable, (*newval_union).stringval);
                set_extra_field(record, &mut (*record).extra, *newextra);
                set_guc_source(record, source);
                (*record).scontext = context;
                (*record).srole = srole;

                // Ugly hack: during SET session_authorization, forcibly do SET ROLE NONE
                if !is_reload && strcmp((*conf).gen.name, b"session_authorization\0".as_ptr() as *const c_char) == 0 {
                    let role_source = if orig_source == GucSource::PGC_S_OVERRIDE {
                        GucSource::PGC_S_DYNAMIC_DEFAULT
                    } else {
                        orig_source
                    };
                    set_config_with_handle(
                        b"role\0".as_ptr() as *const c_char,
                        ptr::null_mut(),
                        if !value.is_null() { b"none\0".as_ptr() as *const c_char } else { ptr::null() },
                        orig_context, role_source, orig_srole,
                        action, true, elevel, false,
                    );
                }
            }
            if makeDefault {
                if (*conf).gen.reset_source <= source {
                    set_string_field(conf, &mut (*conf).reset_val, (*newval_union).stringval);
                    set_extra_field(record, &mut (*conf).reset_extra, *newextra);
                    (*conf).gen.reset_source = source;
                    (*conf).gen.reset_scontext = context;
                    (*conf).gen.reset_srole = srole;
                }
                let mut stack = (*record).stack;
                while !stack.is_null() {
                    if (*stack).source <= source {
                        set_string_field(conf, &mut (*stack).prior.val.stringval, (*newval_union).stringval);
                        set_extra_field(record, &mut (*stack).prior.extra, *newextra);
                        (*stack).source = source;
                        (*stack).scontext = context;
                        (*stack).srole = srole;
                    }
                    stack = (*stack).prev;
                }
            }
            if !(*newval_union).stringval.is_null() && !string_field_used(conf, (*newval_union).stringval) {
                guc_free((*newval_union).stringval as *mut c_void);
            }
            if !(*newextra).is_null() && !extra_field_used(record, *newextra) {
                guc_free(*newextra);
            }
        }
        config_type::PGC_ENUM => {
            let conf = record as *mut config_enum;
            if !value.is_null() {
                if !parse_and_validate_value(record, value, source, elevel, newval_union, newextra) { return 0; }
            } else if source == GucSource::PGC_S_DEFAULT {
                (*newval_union).enumval = (*conf).boot_val;
                if !call_enum_check_hook(conf, &mut (*newval_union).enumval, newextra, source, elevel) { return 0; }
            } else {
                (*newval_union).enumval = (*conf).reset_val;
                *newextra = (*conf).reset_extra;
                source = (*conf).gen.reset_source;
                context = (*conf).gen.reset_scontext;
                srole = (*conf).gen.reset_srole;
            }
            apply_non_string!(conf, variable, enumval, *(*conf).variable != (*newval_union).enumval);
        }
    }

    if changeVal && ((*record).flags & GUC_REPORT) != 0 && ((*record).status & GUC_NEEDS_REPORT) == 0 {
        (*record).status |= GUC_NEEDS_REPORT;
        // TODO(pg-port): slist_push_head(&guc_report_list, &record->report_link)
    }

    if changeVal { 1 } else { -1 }
}

// ---------------------------------------------------------------------------
// get_config_handle / set_config_sourcefile / SetConfigOption
// ---------------------------------------------------------------------------

pub unsafe fn get_config_handle(name: *const c_char) -> *mut config_handle {
    let gen = find_option(name, false, false, 0);
    if !gen.is_null() && ((*gen).flags & GUC_CUSTOM_PLACEHOLDER) == 0 {
        return gen;
    }
    ptr::null_mut()
}

unsafe fn set_config_sourcefile(name: *const c_char, sourcefile: *mut c_char, sourceline: c_int) {
    let elevel = if IsUnderPostmaster() { DEBUG3 } else { LOG };
    let record = find_option(name, true, false, elevel);
    if record.is_null() { return; }
    let sourcefile = guc_strdup(elevel, sourcefile);
    guc_free((*record).sourcefile as *mut c_void);
    (*record).sourcefile = sourcefile;
    (*record).sourceline = sourceline;
}

/// Public API wrapper for set_config_option.
pub unsafe fn SetConfigOption(
    name: *const c_char,
    value: *const c_char,
    context: GucContext,
    source: GucSource,
) {
    set_config_option(name, value, context, source, GucAction::GUC_ACTION_SET, true, 0, false);
}

/// Wrapper for ProcessConfigFile (stub - calls ProcessConfigFileInternal).
/// TODO(pg-port): ProcessConfigFile in guc-file.l output needs to call this.
pub unsafe fn ProcessConfigFile(context: GucContext) {
    ProcessConfigFileInternal(context, true, LOG);
}

// ---------------------------------------------------------------------------
// SelectConfigFiles / pg_timezone_abbrev_initialize / ResetAllOptions
// ---------------------------------------------------------------------------

pub unsafe fn SelectConfigFiles(userDoption: *const c_char, progname: *const c_char) -> bool {
    let configdir: *mut c_char;
    let mut fname: *mut c_char;
    let mut fname_is_malloced: bool;
    let data_directory_rec: *mut config_string;

    configdir = if !userDoption.is_null() {
        make_absolute_path(userDoption)
    } else {
        make_absolute_path(getenv(b"PGDATA\0".as_ptr() as *const c_char))
    };

    if !configdir.is_null() {
        let mut stat_buf: [u8; 128] = [0u8; 128]; // struct stat placeholder
        if stat(configdir, stat_buf.as_mut_ptr() as *mut c_void) != 0 {
            // write_stderr equivalent: just a no-op stub
            // TODO(pg-port): write_stderr once port module is wired
            return false;
        }
    }

    if !ConfigFileName.is_null() {
        fname = make_absolute_path(ConfigFileName);
        fname_is_malloced = true;
    } else if !configdir.is_null() {
        let cfn = CONFIG_FILENAME.as_ptr() as *const c_char;
        let len = strlen(configdir) + strlen(cfn) + 2;
        fname = guc_malloc(FATAL, len) as *mut c_char;
        snprintf(fname, len, b"{}/{}\0".as_ptr() as *const c_char, configdir, cfn);
        fname_is_malloced = false;
    } else {
        return false;
    }

    SetConfigOption(b"config_file\0".as_ptr() as *const c_char, fname, GucContext::PGC_POSTMASTER, GucSource::PGC_S_OVERRIDE);

    if fname_is_malloced { free(fname as *mut c_void); } else { guc_free(fname as *mut c_void); }

    let mut stat_buf: [u8; 128] = [0u8; 128];
    if stat(ConfigFileName, stat_buf.as_mut_ptr() as *mut c_void) != 0 {
        free(configdir as *mut c_void);
        return false;
    }

    ProcessConfigFile(GucContext::PGC_POSTMASTER);

    data_directory_rec = find_option(b"data_directory\0".as_ptr() as *const c_char, false, false, PANIC) as *mut config_string;
    if !(*data_directory_rec).variable.is_null() && !(*(*data_directory_rec).variable).is_null() {
        SetDataDir(*(*data_directory_rec).variable);
    } else if !configdir.is_null() {
        SetDataDir(configdir);
    } else {
        return false;
    }

    SetConfigOption(b"data_directory\0".as_ptr() as *const c_char, DataDir, GucContext::PGC_POSTMASTER, GucSource::PGC_S_OVERRIDE);

    ProcessConfigFile(GucContext::PGC_POSTMASTER);

    pg_timezone_abbrev_initialize();

    // HBA file
    if !HbaFileName.is_null() {
        fname = make_absolute_path(HbaFileName);
        fname_is_malloced = true;
    } else if !configdir.is_null() {
        let hfn = HBA_FILENAME.as_ptr() as *const c_char;
        let len = strlen(configdir) + strlen(hfn) + 2;
        fname = guc_malloc(FATAL, len) as *mut c_char;
        snprintf(fname, len, b"{}/{}\0".as_ptr() as *const c_char, configdir, hfn);
        fname_is_malloced = false;
    } else {
        return false;
    }
    SetConfigOption(b"hba_file\0".as_ptr() as *const c_char, fname, GucContext::PGC_POSTMASTER, GucSource::PGC_S_OVERRIDE);
    if fname_is_malloced { free(fname as *mut c_void); } else { guc_free(fname as *mut c_void); }

    // ident file
    if !IdentFileName.is_null() {
        fname = make_absolute_path(IdentFileName);
        fname_is_malloced = true;
    } else if !configdir.is_null() {
        let ifn = IDENT_FILENAME.as_ptr() as *const c_char;
        let len = strlen(configdir) + strlen(ifn) + 2;
        fname = guc_malloc(FATAL, len) as *mut c_char;
        snprintf(fname, len, b"{}/{}\0".as_ptr() as *const c_char, configdir, ifn);
        fname_is_malloced = false;
    } else {
        return false;
    }
    SetConfigOption(b"ident_file\0".as_ptr() as *const c_char, fname, GucContext::PGC_POSTMASTER, GucSource::PGC_S_OVERRIDE);
    if fname_is_malloced { free(fname as *mut c_void); } else { guc_free(fname as *mut c_void); }

    free(configdir as *mut c_void);
    true
}

unsafe fn pg_timezone_abbrev_initialize() {
    SetConfigOption(b"timezone_abbreviations\0".as_ptr() as *const c_char, b"Default\0".as_ptr() as *const c_char, GucContext::PGC_POSTMASTER, GucSource::PGC_S_DYNAMIC_DEFAULT);
}

/// Reset all options to their saved default values (implements RESET ALL).
pub unsafe fn ResetAllOptions() {
    // TODO(pg-port): dlist_foreach_modify over guc_nondef_list once ilist is wired
    // Skeleton below (loops would go here when dlist iterator is available)
}

// ---------------------------------------------------------------------------
// GUC check-hook error code support
// ---------------------------------------------------------------------------

pub unsafe fn GUC_check_errcode(sqlerrcode: c_int) {
    GUC_check_errcode_value = sqlerrcode;
}

// ---------------------------------------------------------------------------
// call_*_check_hook helpers
// ---------------------------------------------------------------------------

unsafe fn call_bool_check_hook(
    conf: *mut config_bool,
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
    elevel: c_int,
) -> bool {
    let check_hook = match (*conf).check_hook { None => return true, Some(h) => h };
    GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    GUC_check_errmsg_string    = ptr::null_mut();
    GUC_check_errdetail_string = ptr::null_mut();
    GUC_check_errhint_string   = ptr::null_mut();
    if !check_hook(newval, extra, source) {
        if !GUC_check_errmsg_string.is_null() {
            ereport!(elevel, errmsg!("{}", std::ffi::CStr::from_ptr(GUC_check_errmsg_string).to_string_lossy()));
        } else {
            ereport!(elevel, errmsg!("invalid value for parameter \"{}\": {}", std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy(), *newval as c_int));
        }
        // FlushErrorState - TODO(pg-port): call when error.c is wired
        return false;
    }
    true
}

unsafe fn call_int_check_hook(
    conf: *mut config_int,
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
    elevel: c_int,
) -> bool {
    let check_hook = match (*conf).check_hook { None => return true, Some(h) => h };
    GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    GUC_check_errmsg_string    = ptr::null_mut();
    GUC_check_errdetail_string = ptr::null_mut();
    GUC_check_errhint_string   = ptr::null_mut();
    if !check_hook(newval, extra, source) {
        if !GUC_check_errmsg_string.is_null() {
            ereport!(elevel, errmsg!("{}", std::ffi::CStr::from_ptr(GUC_check_errmsg_string).to_string_lossy()));
        } else {
            ereport!(elevel, errmsg!("invalid value for parameter \"{}\": {}", std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy(), *newval));
        }
        return false;
    }
    true
}

unsafe fn call_real_check_hook(
    conf: *mut config_real,
    newval: *mut c_double,
    extra: *mut *mut c_void,
    source: GucSource,
    elevel: c_int,
) -> bool {
    let check_hook = match (*conf).check_hook { None => return true, Some(h) => h };
    GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    GUC_check_errmsg_string    = ptr::null_mut();
    GUC_check_errdetail_string = ptr::null_mut();
    GUC_check_errhint_string   = ptr::null_mut();
    if !check_hook(newval, extra, source) {
        if !GUC_check_errmsg_string.is_null() {
            ereport!(elevel, errmsg!("{}", std::ffi::CStr::from_ptr(GUC_check_errmsg_string).to_string_lossy()));
        } else {
            ereport!(elevel, errmsg!("invalid value for parameter \"{}\": {}", std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy(), *newval));
        }
        return false;
    }
    true
}

unsafe fn call_string_check_hook(
    conf: *mut config_string,
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
    elevel: c_int,
) -> bool {
    let check_hook = match (*conf).check_hook { None => return true, Some(h) => h };
    GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    GUC_check_errmsg_string    = ptr::null_mut();
    GUC_check_errdetail_string = ptr::null_mut();
    GUC_check_errhint_string   = ptr::null_mut();
    // Note: in the C code this uses PG_TRY/PG_CATCH; we can't do that in Rust
    // without the PG TRY infrastructure.  For now, call directly.
    // TODO(pg-port): wrap in pg_try when error.c exception mechanism is ported.
    if !check_hook(newval, extra, source) {
        if !GUC_check_errmsg_string.is_null() {
            ereport!(elevel, errmsg!("{}", std::ffi::CStr::from_ptr(GUC_check_errmsg_string).to_string_lossy()));
        } else {
            let v = if (*newval).is_null() { b"\0".as_ptr() as *const c_char } else { *newval as *const c_char };
            ereport!(elevel, errmsg!("invalid value for parameter \"{}\": \"{}\"", std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy(), std::ffi::CStr::from_ptr(v).to_string_lossy()));
        }
        return false;
    }
    true
}

unsafe fn call_enum_check_hook(
    conf: *mut config_enum,
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
    elevel: c_int,
) -> bool {
    let check_hook = match (*conf).check_hook { None => return true, Some(h) => h };
    GUC_check_errcode_value = ERRCODE_INVALID_PARAMETER_VALUE;
    GUC_check_errmsg_string    = ptr::null_mut();
    GUC_check_errdetail_string = ptr::null_mut();
    GUC_check_errhint_string   = ptr::null_mut();
    if !check_hook(newval, extra, source) {
        if !GUC_check_errmsg_string.is_null() {
            ereport!(elevel, errmsg!("{}", std::ffi::CStr::from_ptr(GUC_check_errmsg_string).to_string_lossy()));
        } else {
            let enumname = config_enum_lookup_by_value(conf, *newval);
            ereport!(elevel, errmsg!("invalid value for parameter \"{}\": \"{}\"", std::ffi::CStr::from_ptr((*conf).gen.name).to_string_lossy(), std::ffi::CStr::from_ptr(enumname).to_string_lossy()));
        }
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// DefineCustomXxxVariable
// ---------------------------------------------------------------------------

unsafe fn init_custom_variable(
    name: *const c_char,
    short_desc: *const c_char,
    long_desc: *const c_char,
    context: GucContext,
    flags: c_int,
    vartype: config_type,
    sz: usize,
) -> *mut config_generic {
    // Only allow custom PGC_POSTMASTER variables during shared library preload
    if context == GucContext::PGC_POSTMASTER && !process_shared_preload_libraries_in_progress {
        elog!(FATAL, "cannot create PGC_POSTMASTER variables after startup");
    }
    if (flags & GUC_LIST_QUOTE) != 0 {
        elog!(FATAL, "extensions cannot define GUC_LIST_QUOTE variables");
    }
    // Restrict pljava known-bad variables
    let mut context = context;
    if context == GucContext::PGC_USERSET {
        if strcmp(name, b"pljava.classpath\0".as_ptr() as *const c_char) == 0
            || strcmp(name, b"pljava.vmoptions\0".as_ptr() as *const c_char) == 0
        {
            context = GucContext::PGC_SUSET;
        }
    }
    let gen = guc_malloc(FATAL, sz) as *mut config_generic;
    memset(gen as *mut c_void, 0, sz);
    (*gen).name = guc_strdup(FATAL, name);
    (*gen).context = context;
    (*gen).group = config_group::CUSTOM_OPTIONS;
    (*gen).short_desc = short_desc;
    (*gen).long_desc = long_desc;
    (*gen).flags = flags;
    (*gen).vartype = vartype;
    gen
}

unsafe fn define_custom_variable(variable: *mut config_generic) {
    debug_assert!(check_GUC_init(variable));
    let name = (*variable).name;
    let mut found = false;
    let hentry = hash_search(guc_hashtab, &name as *const *const c_char as *const c_void, HASH_FIND, &mut found) as *mut GUCHashEntry;
    if hentry.is_null() || !found {
        InitializeOneGUCOption(variable);
        add_guc_variable(variable, ERROR);
        return;
    }
    if ((*(*hentry).gucvar).flags & GUC_CUSTOM_PLACEHOLDER) == 0 {
        ereport!(ERROR, errmsg!("attempt to redefine parameter \"{}\"", std::ffi::CStr::from_ptr(name).to_string_lossy()));
    }
    debug_assert!((*(*hentry).gucvar).vartype == config_type::PGC_STRING);
    let pHolder = (*hentry).gucvar as *mut config_string;
    InitializeOneGUCOption(variable);
    (*hentry).gucname = name;
    (*hentry).gucvar = variable;
    RemoveGUCFromLists(&mut (*pHolder).gen);

    // Reapply reset value if any
    if !(*pHolder).reset_val.is_null() {
        set_config_option_ext(name, (*pHolder).reset_val, (*pHolder).gen.reset_scontext, (*pHolder).gen.reset_source, (*pHolder).gen.reset_srole, GucAction::GUC_ACTION_SET, true, WARNING, false);
    }
    debug_assert!((*variable).stack.is_null());
    reapply_stacked_values(variable, pHolder, (*pHolder).gen.stack, *(*pHolder).variable, (*pHolder).gen.scontext, (*pHolder).gen.source, (*pHolder).gen.srole);

    if !(*pHolder).gen.sourcefile.is_null() {
        set_config_sourcefile(name, (*pHolder).gen.sourcefile, (*pHolder).gen.sourceline);
    }
    set_string_field(pHolder, (*pHolder).variable, ptr::null_mut());
    set_string_field(pHolder, &mut (*pHolder).reset_val, ptr::null_mut());
    guc_free(pHolder as *mut c_void);
}

unsafe fn reapply_stacked_values(
    variable: *mut config_generic,
    pHolder: *mut config_string,
    stack: *mut GucStack,
    curvalue: *const c_char,
    curscontext: GucContext,
    cursource: GucSource,
    cursrole: Oid,
) {
    let name = (*variable).name;
    let oldvarstack = (*variable).stack;

    if !stack.is_null() {
        reapply_stacked_values(variable, pHolder, (*stack).prev, (*stack).prior.val.stringval, (*stack).scontext, (*stack).source, (*stack).srole);

        match (*stack).state {
            GucStackState::GUC_SAVE => {
                set_config_option_ext(name, curvalue, curscontext, cursource, cursrole, GucAction::GUC_ACTION_SAVE, true, WARNING, false);
            }
            GucStackState::GUC_SET => {
                set_config_option_ext(name, curvalue, curscontext, cursource, cursrole, GucAction::GUC_ACTION_SET, true, WARNING, false);
            }
            GucStackState::GUC_LOCAL => {
                set_config_option_ext(name, curvalue, curscontext, cursource, cursrole, GucAction::GUC_ACTION_LOCAL, true, WARNING, false);
            }
            GucStackState::GUC_SET_LOCAL => {
                set_config_option_ext(name, (*stack).masked.val.stringval, (*stack).masked_scontext, GucSource::PGC_S_SESSION, (*stack).masked_srole, GucAction::GUC_ACTION_SET, true, WARNING, false);
                set_config_option_ext(name, curvalue, curscontext, cursource, cursrole, GucAction::GUC_ACTION_LOCAL, true, WARNING, false);
            }
        }

        if (*variable).stack != oldvarstack {
            (*(*variable).stack).nest_level = (*stack).nest_level;
        }
    } else {
        if curvalue != (*pHolder).reset_val
            || curscontext != (*pHolder).gen.reset_scontext
            || cursource != (*pHolder).gen.reset_source
            || cursrole != (*pHolder).gen.reset_srole
        {
            set_config_option_ext(name, curvalue, curscontext, cursource, cursrole, GucAction::GUC_ACTION_SET, true, WARNING, false);
            if !(*variable).stack.is_null() {
                // TODO(pg-port): slist_delete when ilist wired
                (*variable).stack = ptr::null_mut();
            }
        }
    }
}

pub unsafe fn DefineCustomBoolVariable(
    name: *const c_char, short_desc: *const c_char, long_desc: *const c_char,
    valueAddr: *mut bool, bootValue: bool, context: GucContext, flags: c_int,
    check_hook: GucBoolCheckHook, assign_hook: GucBoolAssignHook, show_hook: GucShowHook,
) {
    let var = init_custom_variable(name, short_desc, long_desc, context, flags, config_type::PGC_BOOL, std::mem::size_of::<config_bool>()) as *mut config_bool;
    (*var).variable   = valueAddr;
    (*var).boot_val   = bootValue;
    (*var).reset_val  = bootValue;
    (*var).check_hook = check_hook;
    (*var).assign_hook = assign_hook;
    (*var).show_hook  = show_hook;
    define_custom_variable(&mut (*var).gen);
}

pub unsafe fn DefineCustomIntVariable(
    name: *const c_char, short_desc: *const c_char, long_desc: *const c_char,
    valueAddr: *mut c_int, bootValue: c_int, minValue: c_int, maxValue: c_int,
    context: GucContext, flags: c_int,
    check_hook: GucIntCheckHook, assign_hook: GucIntAssignHook, show_hook: GucShowHook,
) {
    let var = init_custom_variable(name, short_desc, long_desc, context, flags, config_type::PGC_INT, std::mem::size_of::<config_int>()) as *mut config_int;
    (*var).variable   = valueAddr;
    (*var).boot_val   = bootValue;
    (*var).reset_val  = bootValue;
    (*var).min = minValue;
    (*var).max = maxValue;
    (*var).check_hook = check_hook;
    (*var).assign_hook = assign_hook;
    (*var).show_hook  = show_hook;
    define_custom_variable(&mut (*var).gen);
}

pub unsafe fn DefineCustomRealVariable(
    name: *const c_char, short_desc: *const c_char, long_desc: *const c_char,
    valueAddr: *mut c_double, bootValue: c_double, minValue: c_double, maxValue: c_double,
    context: GucContext, flags: c_int,
    check_hook: GucRealCheckHook, assign_hook: GucRealAssignHook, show_hook: GucShowHook,
) {
    let var = init_custom_variable(name, short_desc, long_desc, context, flags, config_type::PGC_REAL, std::mem::size_of::<config_real>()) as *mut config_real;
    (*var).variable   = valueAddr;
    (*var).boot_val   = bootValue;
    (*var).reset_val  = bootValue;
    (*var).min = minValue;
    (*var).max = maxValue;
    (*var).check_hook = check_hook;
    (*var).assign_hook = assign_hook;
    (*var).show_hook  = show_hook;
    define_custom_variable(&mut (*var).gen);
}

pub unsafe fn DefineCustomStringVariable(
    name: *const c_char, short_desc: *const c_char, long_desc: *const c_char,
    valueAddr: *mut *mut c_char, bootValue: *const c_char,
    context: GucContext, flags: c_int,
    check_hook: GucStringCheckHook, assign_hook: GucStringAssignHook, show_hook: GucShowHook,
) {
    let var = init_custom_variable(name, short_desc, long_desc, context, flags, config_type::PGC_STRING, std::mem::size_of::<config_string>()) as *mut config_string;
    (*var).variable   = valueAddr;
    (*var).boot_val   = bootValue;
    (*var).check_hook = check_hook;
    (*var).assign_hook = assign_hook;
    (*var).show_hook  = show_hook;
    define_custom_variable(&mut (*var).gen);
}

pub unsafe fn DefineCustomEnumVariable(
    name: *const c_char, short_desc: *const c_char, long_desc: *const c_char,
    valueAddr: *mut c_int, bootValue: c_int, options: *const config_enum_entry,
    context: GucContext, flags: c_int,
    check_hook: GucEnumCheckHook, assign_hook: GucEnumAssignHook, show_hook: GucShowHook,
) {
    let var = init_custom_variable(name, short_desc, long_desc, context, flags, config_type::PGC_ENUM, std::mem::size_of::<config_enum>()) as *mut config_enum;
    (*var).variable   = valueAddr;
    (*var).boot_val   = bootValue;
    (*var).reset_val  = bootValue;
    (*var).options    = options;
    (*var).check_hook = check_hook;
    (*var).assign_hook = assign_hook;
    (*var).show_hook  = show_hook;
    define_custom_variable(&mut (*var).gen);
}

/// Mark the given GUC prefix as "reserved".
pub unsafe fn MarkGUCPrefixReserved(className: *const c_char) {
    let classLen = strlen(className);
    let mut status: HASH_SEQ_STATUS = std::mem::zeroed();
    hash_seq_init(&mut status, guc_hashtab);
    loop {
        let hentry = hash_seq_search(&mut status) as *mut GUCHashEntry;
        if hentry.is_null() { break; }
        let var = (*hentry).gucvar;
        if ((*var).flags & GUC_CUSTOM_PLACEHOLDER) != 0
            && strncmp(className, (*var).name, classLen) == 0
            && *(*var).name.add(classLen) == GUC_QUALIFIER_SEPARATOR as c_char
        {
            ereport!(WARNING, errmsg!("invalid configuration parameter name \"{}\", removing it", std::ffi::CStr::from_ptr((*var).name).to_string_lossy()));
            hash_search(guc_hashtab, &(*var).name as *const *const c_char as *const c_void, HASH_REMOVE, ptr::null_mut());
            RemoveGUCFromLists(var);
        }
    }
    let oldcontext = MemoryContextSwitchTo(GUCMemoryContext);
    reserved_class_prefix = lappend(reserved_class_prefix, pstrdup(className) as *mut c_void);
    MemoryContextSwitchTo(oldcontext);
}

// ---------------------------------------------------------------------------
// get_explain_guc_options
// ---------------------------------------------------------------------------

pub unsafe fn get_explain_guc_options(num: *mut c_int) -> *mut *mut config_generic {
    *num = 0;

    /*
     * While only a fraction of all the GUC variables are marked GUC_EXPLAIN,
     * it doesn't seem worth dynamically resizing this array.
     */
    let result = palloc(std::mem::size_of::<*mut config_generic>() * hash_get_num_entries(guc_hashtab) as usize) as *mut *mut config_generic;

    /* We need only consider GUCs with source not PGC_S_DEFAULT */
    let mut iter: dlist_iter = std::mem::zeroed();
    crate::dlist_foreach!(iter, &mut guc_nondef_list, {
        let conf = crate::dlist_container!(config_generic, nondef_link, iter.cur);

        /* return only parameters marked for inclusion in explain */
        if ((*conf).flags & GUC_EXPLAIN) == 0 {
            continue;
        }

        /* return only options visible to the current user */
        if !ConfigOptionIsVisible(conf) {
            continue;
        }

        /* return only options that are different from their boot values */
        let modified: bool;

        match (*conf).vartype {
            config_type::PGC_BOOL => {
                let lconf = conf as *mut config_bool;
                modified = (*lconf).boot_val != *(*lconf).variable;
            }
            config_type::PGC_INT => {
                let lconf = conf as *mut config_int;
                modified = (*lconf).boot_val != *(*lconf).variable;
            }
            config_type::PGC_REAL => {
                let lconf = conf as *mut config_real;
                modified = (*lconf).boot_val != *(*lconf).variable;
            }
            config_type::PGC_STRING => {
                let lconf = conf as *mut config_string;
                if (*lconf).boot_val.is_null() && (*(*lconf).variable).is_null() {
                    modified = false;
                } else if (*lconf).boot_val.is_null() || (*(*lconf).variable).is_null() {
                    modified = true;
                } else {
                    modified = strcmp((*lconf).boot_val, *(*lconf).variable) != 0;
                }
            }
            config_type::PGC_ENUM => {
                let lconf = conf as *mut config_enum;
                modified = (*lconf).boot_val != *(*lconf).variable;
            }
        }

        if !modified {
            continue;
        }

        /* OK, report it */
        *result.add(*num as usize) = conf;
        *num += 1;
    });

    result
}

// ---------------------------------------------------------------------------
// ParseLongOption
// ---------------------------------------------------------------------------

pub unsafe fn ParseLongOption(string: *const c_char, name: *mut *mut c_char, value: *mut *mut c_char) {
    debug_assert!(!string.is_null());
    debug_assert!(!name.is_null());
    debug_assert!(!value.is_null());

    let equal_pos = strcspn(string, b"=\0".as_ptr() as *const c_char);
    if *string.add(equal_pos) == b'=' as c_char {
        *name = palloc(equal_pos + 1) as *mut c_char;
        strlcpy(*name, string, equal_pos + 1);
        *value = pstrdup(string.add(equal_pos + 1));
    } else {
        *name = pstrdup(string);
        *value = ptr::null_mut();
    }
    // Convert '-' to '_' in name
    let mut cp = *name;
    while *cp != 0 {
        if *cp == b'-' as c_char { *cp = b'_' as c_char; }
        cp = cp.add(1);
    }
}

// ---------------------------------------------------------------------------
// TransformGUCArray / ProcessGUCArray / GUCArrayAdd / GUCArrayDelete / GUCArrayReset
// ---------------------------------------------------------------------------

pub unsafe fn TransformGUCArray(
    array: *mut c_void, /* ArrayType* */
    names:  *mut *mut List,
    values: *mut *mut List,
) {
    // TODO(pg-port): ARR_DIMS / ARR_ELEMTYPE / array_ref - stub until array.rs is ported
    *names  = NIL;
    *values = NIL;
}

pub unsafe fn ProcessGUCArray(
    array: *mut c_void, /* ArrayType* */
    context: GucContext,
    source: GucSource,
    action: GucAction,
) {
    let mut gucNames:  *mut List = ptr::null_mut();
    let mut gucValues: *mut List = ptr::null_mut();
    TransformGUCArray(array, &mut gucNames, &mut gucValues);
    // TODO(pg-port): forboth iterator once list.rs is wired
    list_free(gucNames);
    list_free(gucValues);
}

pub unsafe fn GUCArrayAdd(array: *mut c_void, name: *const c_char, value: *const c_char) -> *mut c_void {
    // TODO(pg-port): full implementation requires array.rs; return input unchanged
    array
}

pub unsafe fn GUCArrayDelete(array: *mut c_void, name: *const c_char) -> *mut c_void {
    array
}

pub unsafe fn GUCArrayReset(array: *mut c_void) -> *mut c_void {
    if array.is_null() { return ptr::null_mut(); }
    if superuser() { return ptr::null_mut(); }
    array
}

// ---------------------------------------------------------------------------
// write_auto_conf_file / replace_auto_config_value
// ---------------------------------------------------------------------------

const ENOSPC: c_int = 28;

unsafe fn write_auto_conf_file(fd: c_int, filename: *const c_char, head: *mut ConfigVariable) {
    let mut buf: StringInfoData = std::mem::zeroed();

    initStringInfo(&mut buf);

    /* Emit file header containing warning comment */
    appendStringInfoString(&mut buf, b"# Do not edit this file manually!\n\0".as_ptr() as *const c_char);
    appendStringInfoString(&mut buf, b"# It will be overwritten by the ALTER SYSTEM command.\n\0".as_ptr() as *const c_char);

    set_errno(0);
    if write(fd, buf.data as *const c_void, buf.len as usize) != buf.len as isize {
        /* if write didn't set errno, assume problem is no disk space */
        if errno() == 0 {
            set_errno(ENOSPC);
        }
        ereport!(ERROR, errmsg!("could not write to file \"{}\": {}", std::ffi::CStr::from_ptr(filename).to_string_lossy(), strerror_string()));
        /* C also: errcode_for_file_access() */
    }

    /* Emit each parameter, properly quoting the value */
    let mut item = head;
    while !item.is_null() {
        resetStringInfo(&mut buf);

        appendStringInfoString(&mut buf, (*item).name);
        appendStringInfoString(&mut buf, b" = '\0".as_ptr() as *const c_char);

        let escaped = escape_single_quotes_ascii((*item).value);
        if escaped.is_null() {
            ereport!(ERROR, errmsg!("out of memory"));
            /* C also: errcode(ERRCODE_OUT_OF_MEMORY) */
        }
        appendStringInfoString(&mut buf, escaped);
        free(escaped as *mut c_void);

        appendStringInfoString(&mut buf, b"'\n\0".as_ptr() as *const c_char);

        set_errno(0);
        if write(fd, buf.data as *const c_void, buf.len as usize) != buf.len as isize {
            /* if write didn't set errno, assume problem is no disk space */
            if errno() == 0 {
                set_errno(ENOSPC);
            }
            ereport!(ERROR, errmsg!("could not write to file \"{}\": {}", std::ffi::CStr::from_ptr(filename).to_string_lossy(), strerror_string()));
            /* C also: errcode_for_file_access() */
        }

        item = (*item).next;
    }

    /* fsync before considering the write to be successful */
    if pg_fsync(fd) != 0 {
        ereport!(ERROR, errmsg!("could not fsync file \"{}\": {}", std::ffi::CStr::from_ptr(filename).to_string_lossy(), strerror_string()));
        /* C also: errcode_for_file_access() */
    }

    pfree(buf.data as *mut c_void);
}

/*
 * Update the given list of configuration parameters, adding, replacing
 * or deleting the entry for item "name" (delete if "value" == NULL).
 */
unsafe fn replace_auto_config_value(
    head_p: *mut *mut ConfigVariable,
    tail_p: *mut *mut ConfigVariable,
    name: *const c_char,
    value: *const c_char,
) {
    let mut prev: *mut ConfigVariable = ptr::null_mut();

    /*
     * Remove any existing match(es) for "name".  Normally there'd be at most
     * one, but if external tools have modified the config file, there could
     * be more.
     */
    let mut item = *head_p;
    while !item.is_null() {
        let next = (*item).next;
        if guc_name_compare_c((*item).name, name) == 0 {
            /* found a match, delete it */
            if !prev.is_null() {
                (*prev).next = next;
            } else {
                *head_p = next;
            }
            if next.is_null() {
                *tail_p = prev;
            }

            pfree((*item).name as *mut c_void);
            pfree((*item).value as *mut c_void);
            pfree((*item).filename as *mut c_void);
            pfree(item as *mut c_void);
        } else {
            prev = item;
        }
        item = next;
    }

    /* Done if we're trying to delete it */
    if value.is_null() {
        return;
    }

    /* OK, append a new entry */
    let item = palloc(std::mem::size_of::<ConfigVariable>()) as *mut ConfigVariable;
    (*item).name = pstrdup(name);
    (*item).value = pstrdup(value);
    (*item).errmsg = ptr::null_mut();
    (*item).filename = pstrdup(b"\0".as_ptr() as *const c_char); /* new item has no location */
    (*item).sourceline = 0;
    (*item).ignore = false;
    (*item).applied = false;
    (*item).next = ptr::null_mut();

    if (*head_p).is_null() {
        *head_p = item;
    } else {
        (**tail_p).next = item;
    }
    *tail_p = item;
}

/*
 * Validate a proposed option setting for GUCArrayAdd/Delete/Reset.
 *
 * name is the option name.  value is the proposed value for the Add case,
 * or NULL for the Delete/Reset cases.  If skipIfNoPermissions is true, it's
 * not an error to have no permissions to set the option.
 *
 * Returns true if OK, false if skipIfNoPermissions is true and user does not
 * have permission to change this option (all other error cases result in an
 * error being thrown).
 */
unsafe fn validate_option_array_item(name: *const c_char, value: *const c_char, skipIfNoPermissions: bool) -> bool {
    /*
     * There are three cases to consider:
     *
     * name is a known GUC variable.  Check the value normally, check
     * permissions normally (i.e., allow if variable is USERSET, or if it's
     * SUSET and user is superuser or holds ACL_SET permissions).
     *
     * name is not known, but exists or can be created as a placeholder (i.e.,
     * it has a valid custom name).  We allow this case if you're a superuser,
     * otherwise not.  Superusers are assumed to know what they're doing. We
     * can't allow it for other users, because when the placeholder is
     * resolved it might turn out to be a SUSET variable.
     *
     * name is not known and can't be created as a placeholder.  Throw error,
     * unless skipIfNoPermissions or reset_custom is true.  If reset_custom is
     * true, this is a RESET or RESET ALL operation for an unknown custom GUC
     * with a reserved prefix, in which case we want to fall through to the
     * placeholder case described in the preceding paragraph (else there'd be
     * no way for users to remove them).  Otherwise, return false.
     */
    let reset_custom = value.is_null() && valid_custom_variable_name(cstr_bytes(name));
    let gconf = find_option(name, true, skipIfNoPermissions || reset_custom, ERROR);
    if gconf.is_null() && !reset_custom {
        /* not known, failed to make a placeholder */
        return false;
    }

    if gconf.is_null() || ((*gconf).flags & GUC_CUSTOM_PLACEHOLDER) != 0 {
        /*
         * We cannot do any meaningful check on the value, so only permissions
         * are useful to check.
         */
        if superuser() || pg_parameter_aclcheck(name, GetUserId(), ACL_SET) == ACLCHECK_OK {
            return true;
        }
        if skipIfNoPermissions {
            return false;
        }
        ereport!(ERROR, errmsg!("permission denied to set parameter \"{}\"", std::ffi::CStr::from_ptr(name).to_string_lossy()));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    /* manual permissions check so we can avoid an error being thrown */
    if (*gconf).context == GucContext::PGC_USERSET {
        /* ok */
    } else if (*gconf).context == GucContext::PGC_SUSET
        && (superuser() || pg_parameter_aclcheck(name, GetUserId(), ACL_SET) == ACLCHECK_OK)
    {
        /* ok */
    } else if skipIfNoPermissions {
        return false;
    }
    /* if a permissions error should be thrown, let set_config_option do it */

    /* test for permissions and valid option value */
    set_config_option(
        name,
        value,
        if superuser() { GucContext::PGC_SUSET } else { GucContext::PGC_USERSET },
        GucSource::PGC_S_TEST,
        GucAction::GUC_ACTION_SET,
        false,
        0,
        false,
    );

    true
}

// ---------------------------------------------------------------------------
// AlterSystemSetConfigFile
// ---------------------------------------------------------------------------

pub unsafe fn AlterSystemSetConfigFile(altersysstmt: *mut c_void /* AlterSystemStmt* */) {
    // TODO(pg-port): full implementation requires parsenodes::AlterSystemStmt,
    // libpq::pqformat, storage::lwlock, etc.
    // Stub: panics to signal unimplemented path.
    unimplemented!("AlterSystemSetConfigFile: TODO(pg-port)")
}

// ---------------------------------------------------------------------------
// GUC serialization: can_skip_gucvar / estimate_variable_size /
// EstimateGUCStateSpace / do_serialize / do_serialize_binary /
// serialize_variable / SerializeGUCState / read_gucstate /
// read_gucstate_binary / guc_restore_error_context_callback / RestoreGUCState
// ---------------------------------------------------------------------------

/*
 * can_skip_gucvar:
 * Decide whether SerializeGUCState can skip sending this GUC variable,
 * or whether RestoreGUCState can skip resetting this GUC to default.
 */
unsafe fn can_skip_gucvar(gconf: *mut config_generic) -> bool {
    (*gconf).context == GucContext::PGC_POSTMASTER
        || (*gconf).context == GucContext::PGC_INTERNAL
        || (*gconf).source == GucSource::PGC_S_DEFAULT
}

/*
 * estimate_variable_size:
 *		Compute space needed for dumping the given GUC variable.
 *
 * It's OK to overestimate, but not to underestimate.
 */
unsafe fn estimate_variable_size(gconf: *mut config_generic) -> usize {
    let mut valsize: usize = 0;

    /* Skippable GUCs consume zero space. */
    if can_skip_gucvar(gconf) {
        return 0;
    }

    /* Name, plus trailing zero byte. */
    let mut size = strlen((*gconf).name) + 1;

    /* Get the maximum display length of the GUC value. */
    match (*gconf).vartype {
        config_type::PGC_BOOL => {
            valsize = 5; /* max(strlen('true'), strlen('false')) */
        }
        config_type::PGC_INT => {
            let conf = gconf as *mut config_int;
            /*
             * Instead of getting the exact display length, use max
             * length.  Also reduce the max length for typical ranges of
             * small values.  Maximum value is 2147483647, i.e. 10 chars.
             * Include one byte for sign.
             */
            if (*(*conf).variable).abs() < 1000 {
                valsize = 3 + 1;
            } else {
                valsize = 10 + 1;
            }
        }
        config_type::PGC_REAL => {
            /*
             * We are going to print it with %e with REALTYPE_PRECISION
             * fractional digits.  Account for sign, leading digit,
             * decimal point, and exponent with up to 3 digits.  E.g.
             * -3.99329042340000021e+110
             */
            valsize = 1 + 1 + 1 + REALTYPE_PRECISION + 5;
        }
        config_type::PGC_STRING => {
            let conf = gconf as *mut config_string;
            /*
             * If the value is NULL, we transmit it as an empty string.
             * Although this is not physically the same value, GUC
             * generally treats a NULL the same as empty string.
             */
            if !(*(*conf).variable).is_null() {
                valsize = strlen(*(*conf).variable);
            } else {
                valsize = 0;
            }
        }
        config_type::PGC_ENUM => {
            let conf = gconf as *mut config_enum;
            valsize = strlen(config_enum_lookup_by_value(conf, *(*conf).variable));
        }
    }

    /* Allow space for terminating zero-byte for value */
    size = add_size(size, valsize + 1);

    if !(*gconf).sourcefile.is_null() {
        size = add_size(size, strlen((*gconf).sourcefile));
    }

    /* Allow space for terminating zero-byte for sourcefile */
    size = add_size(size, 1);

    /* Include line whenever file is nonempty. */
    if !(*gconf).sourcefile.is_null() && *(*gconf).sourcefile != 0 {
        size = add_size(size, std::mem::size_of_val(&(*gconf).sourceline));
    }

    size = add_size(size, std::mem::size_of_val(&(*gconf).source));
    size = add_size(size, std::mem::size_of_val(&(*gconf).scontext));
    size = add_size(size, std::mem::size_of_val(&(*gconf).srole));

    size
}

/*
 * EstimateGUCStateSpace:
 * Returns the size needed to store the GUC state for the current process
 */
pub unsafe fn EstimateGUCStateSpace() -> usize {
    /* Add space reqd for saving the data size of the guc state */
    let mut size = std::mem::size_of::<usize>();

    /*
     * Add up the space needed for each GUC variable.
     *
     * We need only process non-default GUCs.
     */
    let mut iter: dlist_iter = std::mem::zeroed();
    crate::dlist_foreach!(iter, &mut guc_nondef_list, {
        let gconf = crate::dlist_container!(config_generic, nondef_link, iter.cur);
        size = add_size(size, estimate_variable_size(gconf));
    });

    size
}

/*
 * do_serialize:
 * Copies the formatted string into the destination.  Moves ahead the
 * destination pointer, and decrements the maxbytes by that many bytes. If
 * maxbytes is not sufficient to copy the string, error out.
 *
 * In C this is variadic with a printf format; here the caller renders the
 * value into a NUL-terminated byte slice and we copy that.
 */
unsafe fn do_serialize(destptr: *mut *mut c_char, maxbytes: *mut usize, s: &[u8]) {
    if (*maxbytes as isize) <= 0 {
        elog!(ERROR, "not enough space to serialize GUC state");
    }

    /* s does not include the NUL terminator; length to write is s.len() */
    let n = s.len();
    if n >= *maxbytes {
        /* This shouldn't happen either, really. */
        elog!(ERROR, "not enough space to serialize GUC state");
    }

    memcpy(*destptr as *mut c_void, s.as_ptr() as *const c_void, n);
    *(*destptr).add(n) = 0; /* NUL terminator */

    /* Shift the destptr ahead of the null terminator */
    *destptr = (*destptr).add(n + 1);
    *maxbytes -= n + 1;
}

/* Binary copy version of do_serialize() */
unsafe fn do_serialize_binary(destptr: *mut *mut c_char, maxbytes: *mut usize, val: *const c_void, valsize: usize) {
    if valsize > *maxbytes {
        elog!(ERROR, "not enough space to serialize GUC state");
    }

    memcpy(*destptr as *mut c_void, val, valsize);
    *destptr = (*destptr).add(valsize);
    *maxbytes -= valsize;
}

/*
 * serialize_variable:
 * Dumps name, value and other information of a GUC variable into destptr.
 */
unsafe fn serialize_variable(destptr: *mut *mut c_char, maxbytes: *mut usize, gconf: *mut config_generic) {
    /* Ignore skippable GUCs. */
    if can_skip_gucvar(gconf) {
        return;
    }

    do_serialize(destptr, maxbytes, cstr_bytes((*gconf).name));

    match (*gconf).vartype {
        config_type::PGC_BOOL => {
            let conf = gconf as *mut config_bool;
            let s: &[u8] = if *(*conf).variable { b"true" } else { b"false" };
            do_serialize(destptr, maxbytes, s);
        }
        config_type::PGC_INT => {
            let conf = gconf as *mut config_int;
            let s = format!("{}", *(*conf).variable);
            do_serialize(destptr, maxbytes, s.as_bytes());
        }
        config_type::PGC_REAL => {
            let conf = gconf as *mut config_real;
            let s = format!("{:.*e}", REALTYPE_PRECISION, *(*conf).variable);
            do_serialize(destptr, maxbytes, s.as_bytes());
        }
        config_type::PGC_STRING => {
            let conf = gconf as *mut config_string;
            /* NULL becomes empty string, see estimate_variable_size() */
            if (*(*conf).variable).is_null() {
                do_serialize(destptr, maxbytes, b"");
            } else {
                do_serialize(destptr, maxbytes, cstr_bytes(*(*conf).variable));
            }
        }
        config_type::PGC_ENUM => {
            let conf = gconf as *mut config_enum;
            do_serialize(destptr, maxbytes, cstr_bytes(config_enum_lookup_by_value(conf, *(*conf).variable)));
        }
    }

    if (*gconf).sourcefile.is_null() {
        do_serialize(destptr, maxbytes, b"");
    } else {
        do_serialize(destptr, maxbytes, cstr_bytes((*gconf).sourcefile));
    }

    if !(*gconf).sourcefile.is_null() && *(*gconf).sourcefile != 0 {
        do_serialize_binary(destptr, maxbytes, &(*gconf).sourceline as *const c_int as *const c_void, std::mem::size_of_val(&(*gconf).sourceline));
    }

    do_serialize_binary(destptr, maxbytes, &(*gconf).source as *const GucSource as *const c_void, std::mem::size_of_val(&(*gconf).source));
    do_serialize_binary(destptr, maxbytes, &(*gconf).scontext as *const GucContext as *const c_void, std::mem::size_of_val(&(*gconf).scontext));
    do_serialize_binary(destptr, maxbytes, &(*gconf).srole as *const Oid as *const c_void, std::mem::size_of_val(&(*gconf).srole));
}

/*
 * SerializeGUCState:
 * Dumps the complete GUC state onto the memory location at start_address.
 */
pub unsafe fn SerializeGUCState(maxsize: usize, start_address: *mut c_char) {
    /* Reserve space for saving the actual size of the guc state */
    let szsize = std::mem::size_of::<usize>();
    debug_assert!(maxsize > szsize);
    let mut curptr = start_address.add(szsize);
    let mut bytes_left = maxsize - szsize;

    /* We need only consider GUCs with source not PGC_S_DEFAULT */
    let mut iter: dlist_iter = std::mem::zeroed();
    crate::dlist_foreach!(iter, &mut guc_nondef_list, {
        let gconf = crate::dlist_container!(config_generic, nondef_link, iter.cur);
        serialize_variable(&mut curptr, &mut bytes_left, gconf);
    });

    /* Store actual size without assuming alignment of start_address. */
    let actual_size = maxsize - bytes_left - szsize;
    memcpy(start_address as *mut c_void, &actual_size as *const usize as *const c_void, szsize);
}

/*
 * read_gucstate:
 * Actually it does not read anything, just returns the srcptr. But it does
 * move the srcptr past the terminating zero byte, so that the caller is ready
 * to read the next string.
 */
unsafe fn read_gucstate(srcptr: *mut *mut c_char, srcend: *mut c_char) -> *mut c_char {
    let retptr = *srcptr;

    if *srcptr >= srcend {
        elog!(ERROR, "incomplete GUC state");
    }

    /* The string variables are all null terminated */
    let mut ptr = *srcptr;
    while ptr < srcend && *ptr != 0 {
        ptr = ptr.add(1);
    }

    if ptr >= srcend {
        elog!(ERROR, "could not find null terminator in GUC state");
    }

    /* Set the new position to the byte following the terminating NUL */
    *srcptr = ptr.add(1);

    retptr
}

/* Binary read version of read_gucstate(). Copies into dest */
unsafe fn read_gucstate_binary(srcptr: *mut *mut c_char, srcend: *mut c_char, dest: *mut c_void, size: usize) {
    if (*srcptr).add(size) > srcend {
        elog!(ERROR, "incomplete GUC state");
    }

    memcpy(dest, *srcptr as *const c_void, size);
    *srcptr = (*srcptr).add(size);
}

/*
 * Callback used to add a context message when reporting errors that occur
 * while trying to restore GUCs in parallel workers.
 */
unsafe extern "C" fn guc_restore_error_context_callback(arg: *mut c_void) {
    let error_context_name_and_value = arg as *mut *mut c_char;

    if !error_context_name_and_value.is_null() {
        errcontext!(
            "while setting parameter \"{}\" to \"{}\"",
            std::ffi::CStr::from_ptr(*error_context_name_and_value.add(0)).to_string_lossy(),
            std::ffi::CStr::from_ptr(*error_context_name_and_value.add(1)).to_string_lossy()
        );
    }
}

/*
 * RestoreGUCState:
 * Reads the GUC state at the specified address and sets this process's
 * GUCs to match.
 */
pub unsafe fn RestoreGUCState(gucstate: *mut c_void) {
    let mut srcptr = gucstate as *mut c_char;

    /*
     * First, ensure that all potentially-shippable GUCs are reset to their
     * default values.
     */
    let mut miter: dlist_mutable_iter = std::mem::zeroed();
    crate::dlist_foreach_modify!(miter, &mut guc_nondef_list, {
        let gconf = crate::dlist_container!(config_generic, nondef_link, miter.cur);

        /* Do nothing if non-shippable or if already at PGC_S_DEFAULT. */
        if can_skip_gucvar(gconf) {
            continue;
        }

        /*
         * We can use InitializeOneGUCOption to reset the GUC to default, but
         * first we must free any existing subsidiary data to avoid leaking
         * memory.
         */
        debug_assert!((*gconf).stack.is_null());
        guc_free((*gconf).extra);
        guc_free((*gconf).last_reported as *mut c_void);
        guc_free((*gconf).sourcefile as *mut c_void);
        match (*gconf).vartype {
            config_type::PGC_BOOL => {
                let conf = gconf as *mut config_bool;
                if !(*conf).reset_extra.is_null() && (*conf).reset_extra != (*gconf).extra {
                    guc_free((*conf).reset_extra);
                }
            }
            config_type::PGC_INT => {
                let conf = gconf as *mut config_int;
                if !(*conf).reset_extra.is_null() && (*conf).reset_extra != (*gconf).extra {
                    guc_free((*conf).reset_extra);
                }
            }
            config_type::PGC_REAL => {
                let conf = gconf as *mut config_real;
                if !(*conf).reset_extra.is_null() && (*conf).reset_extra != (*gconf).extra {
                    guc_free((*conf).reset_extra);
                }
            }
            config_type::PGC_STRING => {
                let conf = gconf as *mut config_string;
                guc_free(*(*conf).variable as *mut c_void);
                if !(*conf).reset_val.is_null() && (*conf).reset_val != *(*conf).variable {
                    guc_free((*conf).reset_val as *mut c_void);
                }
                if !(*conf).reset_extra.is_null() && (*conf).reset_extra != (*gconf).extra {
                    guc_free((*conf).reset_extra);
                }
            }
            config_type::PGC_ENUM => {
                let conf = gconf as *mut config_enum;
                if !(*conf).reset_extra.is_null() && (*conf).reset_extra != (*gconf).extra {
                    guc_free((*conf).reset_extra);
                }
            }
        }
        /* Remove it from any lists it's in. */
        RemoveGUCFromLists(gconf);
        /* Now we can reset the struct to PGS_S_DEFAULT state. */
        InitializeOneGUCOption(gconf);
    });

    /* First item is the length of the subsequent data */
    let mut len: usize = 0;
    memcpy(&mut len as *mut usize as *mut c_void, gucstate, std::mem::size_of::<usize>());

    srcptr = srcptr.add(std::mem::size_of::<usize>());
    let srcend = srcptr.add(len);

    /* If the GUC value check fails, we want errors to show useful context. */
    let mut error_context_callback: ErrorContextCallback = std::mem::zeroed();
    error_context_callback.callback = Some(guc_restore_error_context_callback);
    error_context_callback.previous = error_context_stack;
    error_context_callback.arg = ptr::null_mut();
    error_context_stack = &mut error_context_callback;

    /* Restore all the listed GUCs. */
    while srcptr < srcend {
        let varname = read_gucstate(&mut srcptr, srcend);
        let varvalue = read_gucstate(&mut srcptr, srcend);
        let varsourcefile = read_gucstate(&mut srcptr, srcend);
        let mut varsourceline: c_int = 0;
        if *varsourcefile != 0 {
            read_gucstate_binary(&mut srcptr, srcend, &mut varsourceline as *mut c_int as *mut c_void, std::mem::size_of::<c_int>());
        } else {
            varsourceline = 0;
        }
        let mut varsource: GucSource = GucSource::PGC_S_DEFAULT;
        let mut varscontext: GucContext = GucContext::PGC_INTERNAL;
        let mut varsrole: Oid = 0;
        read_gucstate_binary(&mut srcptr, srcend, &mut varsource as *mut GucSource as *mut c_void, std::mem::size_of::<GucSource>());
        read_gucstate_binary(&mut srcptr, srcend, &mut varscontext as *mut GucContext as *mut c_void, std::mem::size_of::<GucContext>());
        read_gucstate_binary(&mut srcptr, srcend, &mut varsrole as *mut Oid as *mut c_void, std::mem::size_of::<Oid>());

        let mut error_context_name_and_value: [*mut c_char; 2] = [varname, varvalue];
        error_context_callback.arg = &mut error_context_name_and_value[0] as *mut *mut c_char as *mut c_void;
        let result = set_config_option_ext(
            varname,
            varvalue,
            varscontext,
            varsource,
            varsrole,
            GucAction::GUC_ACTION_SET,
            true,
            ERROR,
            true,
        );
        if result <= 0 {
            ereport!(ERROR, errmsg!("parameter \"{}\" could not be set", std::ffi::CStr::from_ptr(varname).to_string_lossy()));
            /* C also: errcode(ERRCODE_INTERNAL_ERROR) */
        }
        if *varsourcefile != 0 {
            set_config_sourcefile(varname, varsourcefile, varsourceline);
        }
        error_context_callback.arg = ptr::null_mut();
    }

    error_context_stack = error_context_callback.previous;
}

// ---------------------------------------------------------------------------
// EXEC_BACKEND non-default variable I/O
// ---------------------------------------------------------------------------

// exec_backend feature not used in this port; functions omitted

/*
 *	These routines dump out all non-default GUC options into a binary
 *	file that is read by all exec'ed backends.  The format is:
 *
 *		variable name, string, null terminated
 *		variable value, string, null terminated
 *		variable sourcefile, string, null terminated (empty if none)
 *		variable sourceline, integer
 *		variable source, integer
 *		variable scontext, integer
 *		variable srole, OID
 */
#[cfg(any())]
unsafe fn write_one_nondefault_variable(fp: *mut c_void, gconf: *mut config_generic) {
    debug_assert!((*gconf).source != GucSource::PGC_S_DEFAULT);

    fprintf(fp, c"%s".as_ptr(), (*gconf).name);
    fputc(0, fp);

    match (*gconf).vartype {
        config_type::PGC_BOOL => {
            let conf = gconf as *mut config_bool;
            if *(*conf).variable {
                fprintf(fp, c"true".as_ptr());
            } else {
                fprintf(fp, c"false".as_ptr());
            }
        }

        config_type::PGC_INT => {
            let conf = gconf as *mut config_int;
            fprintf(fp, c"%d".as_ptr(), *(*conf).variable);
        }

        config_type::PGC_REAL => {
            let conf = gconf as *mut config_real;
            fprintf(fp, c"%.17g".as_ptr(), *(*conf).variable);
        }

        config_type::PGC_STRING => {
            let conf = gconf as *mut config_string;
            if !(*(*conf).variable).is_null() {
                fprintf(fp, c"%s".as_ptr(), *(*conf).variable);
            }
        }

        config_type::PGC_ENUM => {
            let conf = gconf as *mut config_enum;
            fprintf(fp, c"%s".as_ptr(),
                    config_enum_lookup_by_value(conf, *(*conf).variable));
        }
    }

    fputc(0, fp);

    if !(*gconf).sourcefile.is_null() {
        fprintf(fp, c"%s".as_ptr(), (*gconf).sourcefile);
    }
    fputc(0, fp);

    fwrite(&(*gconf).sourceline as *const _ as *const c_void, 1, std::mem::size_of_val(&(*gconf).sourceline), fp);
    fwrite(&(*gconf).source as *const _ as *const c_void, 1, std::mem::size_of_val(&(*gconf).source), fp);
    fwrite(&(*gconf).scontext as *const _ as *const c_void, 1, std::mem::size_of_val(&(*gconf).scontext), fp);
    fwrite(&(*gconf).srole as *const _ as *const c_void, 1, std::mem::size_of_val(&(*gconf).srole), fp);
}

#[cfg(any())]
pub unsafe fn write_nondefault_variables(context: GucContext) {
    let elevel: c_int;
    let fp: *mut c_void;

    debug_assert!(context == GucContext::PGC_POSTMASTER || context == GucContext::PGC_SIGHUP);

    elevel = if context == GucContext::PGC_SIGHUP { LOG } else { ERROR };

    /*
     * Open file
     */
    fp = AllocateFile(CONFIG_EXEC_PARAMS_NEW.as_ptr() as *const c_char, c"w".as_ptr());
    if fp.is_null() {
        ereport!(elevel,
                 errmsg!("could not write to file \"{}\": {}",
                         std::ffi::CStr::from_ptr(CONFIG_EXEC_PARAMS_NEW.as_ptr() as *const c_char).to_string_lossy(),
                         strerror_string()));
        /* C also: errcode_for_file_access() */
        return;
    }

    /* We need only consider GUCs with source not PGC_S_DEFAULT */
    let mut iter: dlist_iter = std::mem::zeroed();
    crate::dlist_foreach!(iter, &mut guc_nondef_list, {
        let gconf = crate::dlist_container!(config_generic, nondef_link, iter.cur);
        write_one_nondefault_variable(fp, gconf);
    });

    if FreeFile(fp) != 0 {
        ereport!(elevel,
                 errmsg!("could not write to file \"{}\": {}",
                         std::ffi::CStr::from_ptr(CONFIG_EXEC_PARAMS_NEW.as_ptr() as *const c_char).to_string_lossy(),
                         strerror_string()));
        /* C also: errcode_for_file_access() */
        return;
    }

    /*
     * Put new file in place.  This could delay on Win32, but we don't hold
     * any exclusive locks.
     */
    rename(CONFIG_EXEC_PARAMS_NEW.as_ptr() as *const c_char,
           CONFIG_EXEC_PARAMS.as_ptr() as *const c_char);
}

/*
 *	Read string, including null byte from file
 *
 *	Return NULL on EOF and nothing read
 */
#[cfg(any())]
unsafe fn read_string_with_null(fp: *mut c_void) -> *mut c_char {
    let mut i: c_int = 0;
    let mut ch: c_int;
    let mut maxlen: c_int = 256;
    let mut str: *mut c_char = ptr::null_mut();

    loop {
        ch = fgetc(fp);
        if ch == EOF {
            if i == 0 {
                return ptr::null_mut();
            } else {
                elog!(FATAL, "invalid format of exec config params file");
            }
        }
        if i == 0 {
            str = guc_malloc(FATAL, maxlen as usize) as *mut c_char;
        } else if i == maxlen {
            maxlen *= 2;
            str = guc_realloc(FATAL, str as *mut c_void, maxlen as usize) as *mut c_char;
        }
        *str.add(i as usize) = ch as c_char;
        i += 1;
        if ch == 0 {
            break;
        }
    }

    str
}

#[cfg(any())]
pub unsafe fn read_nondefault_variables() {
    let fp: *mut c_void;
    let mut varname: *mut c_char;
    let mut varvalue: *mut c_char;
    let mut varsourcefile: *mut c_char;
    let mut varsourceline: c_int = 0;
    let mut varsource: GucSource = std::mem::zeroed();
    let mut varscontext: GucContext = std::mem::zeroed();
    let mut varsrole: Oid = 0;

    /*
     * Open file
     */
    fp = AllocateFile(CONFIG_EXEC_PARAMS.as_ptr() as *const c_char, c"r".as_ptr());
    if fp.is_null() {
        /* File not found is fine */
        if errno() != ENOENT {
            ereport!(FATAL,
                     errmsg!("could not read from file \"{}\": {}",
                             std::ffi::CStr::from_ptr(CONFIG_EXEC_PARAMS.as_ptr() as *const c_char).to_string_lossy(),
                             strerror_string()));
            /* C also: errcode_for_file_access() */
        }
        return;
    }

    loop {
        varname = read_string_with_null(fp);
        if varname.is_null() {
            break;
        }

        if find_option(varname, true, false, FATAL).is_null() {
            elog!(FATAL, "failed to locate variable \"{}\" in exec config params file",
                  std::ffi::CStr::from_ptr(varname).to_string_lossy());
        }

        varvalue = read_string_with_null(fp);
        if varvalue.is_null() {
            elog!(FATAL, "invalid format of exec config params file");
        }
        varsourcefile = read_string_with_null(fp);
        if varsourcefile.is_null() {
            elog!(FATAL, "invalid format of exec config params file");
        }
        if fread(&mut varsourceline as *mut _ as *mut c_void, 1, std::mem::size_of_val(&varsourceline), fp)
            != std::mem::size_of_val(&varsourceline) {
            elog!(FATAL, "invalid format of exec config params file");
        }
        if fread(&mut varsource as *mut _ as *mut c_void, 1, std::mem::size_of_val(&varsource), fp)
            != std::mem::size_of_val(&varsource) {
            elog!(FATAL, "invalid format of exec config params file");
        }
        if fread(&mut varscontext as *mut _ as *mut c_void, 1, std::mem::size_of_val(&varscontext), fp)
            != std::mem::size_of_val(&varscontext) {
            elog!(FATAL, "invalid format of exec config params file");
        }
        if fread(&mut varsrole as *mut _ as *mut c_void, 1, std::mem::size_of_val(&varsrole), fp)
            != std::mem::size_of_val(&varsrole) {
            elog!(FATAL, "invalid format of exec config params file");
        }

        set_config_option_ext(varname, varvalue,
                              varscontext, varsource, varsrole,
                              GucAction::GUC_ACTION_SET, true, 0, true);
        if *varsourcefile != 0 {
            set_config_sourcefile(varname, varsourcefile, varsourceline);
        }

        guc_free(varname as *mut c_void);
        guc_free(varvalue as *mut c_void);
        guc_free(varsourcefile as *mut c_void);
    }

    FreeFile(fp);
}

//! Translated from PostgreSQL src/include/utils/guc.h
//! External declarations pertaining to Grand Unified Configuration.
//!
//! The `config_generic`/`config_bool`/... records (defined in C in
//! guc_tables.h) are translated here alongside guc.h's public API, since the
//! task groups them together. The GUC_* option bits are a `bitflags!` set; the
//! GUC_UNIT_* sub-field is a mutually-exclusive multi-bit selector and so an
//! `enum` (bitflags-port.md appendix 3.5), not bitflags.

use bitflags::bitflags;

use crate::access::tupdesc::TupleDesc;
use crate::nodes::parsenodes::{AlterSystemStmt, VariableSetStmt};
use crate::postgres_ext::Oid;
use crate::tcop::dest::DestReceiver;
use crate::utils::array::ArrayType;

/// Max for integer GUCs measured in kilobytes (64-bit target: size_t > 4 bytes).
pub const MAX_KILOBYTES: i32 = i32::MAX;

/// Automatic configuration file name for ALTER SYSTEM.
pub const PG_AUTOCONF_FILENAME: &str = "postgresql.auto.conf";

pub const GUC_QUALIFIER_SEPARATOR: char = '.';

/// Contexts: when an option can be set. INTERNAL < POSTMASTER < ... < USERSET.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GucContext {
    INTERNAL,
    POSTMASTER,
    SIGHUP,
    SU_BACKEND,
    BACKEND,
    SUSET,
    USERSET,
}

/// Source of the current setting. A new setting takes effect only if the prior
/// setting had the same or lower level. INTERACTIVE is the dividing line
/// for error reporting, not a real source.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GucSource {
    DEFAULT,         // hard-wired default ("boot_val")
    DYNAMIC_DEFAULT, // default computed during initialization
    ENV_VAR,         // postmaster environment variable
    FILE,            // postgresql.conf
    ARGV,            // postmaster command line
    GLOBAL,          // global in-database setting
    DATABASE,        // per-database setting
    USER,            // per-user setting
    DATABASE_USER,   // per-user-and-database setting
    CLIENT,          // from client connection request
    OVERRIDE,        // special case to forcibly set default
    INTERACTIVE,     // dividing line for error reporting
    TEST,            // test per-database or per-user setting
    SESSION,         // SET command
}

/// Types of set_config_option actions.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GucAction {
    SET,   // regular SET command
    LOCAL, // SET LOCAL command
    SAVE,  // function SET option, or temp assignment
}

/// A name-value pair (with source location) from parsing config file(s); also
/// abused to carry config-file error reports (errmsg != None).
pub struct ConfigVariable {
    pub name: Option<String>,
    pub value: Option<String>,
    pub errmsg: Option<String>,
    pub filename: Option<String>,
    pub sourceline: i32,
    pub ignore: bool,
    pub applied: bool,
    pub next: Option<Box<Self>>,
}

/// One acceptable value of an enum GUC. `hidden` values are accepted but not
/// listed when guc.c is asked for acceptable values.
pub struct config_enum_entry {
    pub name: &'static str,
    pub val: i32,
    pub hidden: bool,
}

// --- Per-variable check/assign/show hook function-pointer typedefs ---
// The C hooks take `void **extra` (an opaque malloc'd struct produced by the
// check hook and consumed by the assign hook). Modeled as a raw pointer slot.
// TODO(ptr): becomes an owned/typed extra payload in a later pass.

pub type GucBoolCheckHook = fn(newval: &mut bool, extra: &mut *mut (), source: GucSource) -> bool;
pub type GucIntCheckHook = fn(newval: &mut i32, extra: &mut *mut (), source: GucSource) -> bool;
pub type GucRealCheckHook = fn(newval: &mut f64, extra: &mut *mut (), source: GucSource) -> bool;
pub type GucStringCheckHook =
    fn(newval: &mut Option<String>, extra: &mut *mut (), source: GucSource) -> bool;
pub type GucEnumCheckHook = fn(newval: &mut i32, extra: &mut *mut (), source: GucSource) -> bool;

pub type GucBoolAssignHook = fn(newval: bool, extra: *mut ());
pub type GucIntAssignHook = fn(newval: i32, extra: *mut ());
pub type GucRealAssignHook = fn(newval: f64, extra: *mut ());
pub type GucStringAssignHook = fn(newval: &str, extra: *mut ());
pub type GucEnumAssignHook = fn(newval: i32, extra: *mut ());

pub type GucShowHook = fn() -> &'static str;

bitflags! {
    /// Option flags of a GUC variable ("flags" field). These do not appear on
    /// disk. NB: the GUC_UNIT_* sub-field shares the same word but is a separate
    /// `GucUnit` enum (mutually exclusive codes), kept out of this set.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct GucFlags: i32 {
        const LIST_INPUT          = 0x000001; // input can be list format
        const LIST_QUOTE          = 0x000002; // double-quote list elements
        const NO_SHOW_ALL         = 0x000004; // exclude from SHOW ALL
        const NO_RESET            = 0x000008; // disallow RESET and SAVE
        const NO_RESET_ALL        = 0x000010; // exclude from RESET ALL
        const EXPLAIN             = 0x000020; // include in EXPLAIN
        const REPORT              = 0x000040; // auto-report changes to client
        const NOT_IN_SAMPLE       = 0x000080; // not in postgresql.conf.sample
        const DISALLOW_IN_FILE    = 0x000100; // can't set in postgresql.conf
        const CUSTOM_PLACEHOLDER  = 0x000200; // placeholder for custom variable
        const SUPERUSER_ONLY      = 0x000400; // show only to superusers
        const IS_NAME             = 0x000800; // limit string to NAMEDATALEN-1
        const NOT_WHILE_SEC_REST  = 0x001000; // can't set if security restricted
        const DISALLOW_IN_AUTO_FILE = 0x002000; // can't set in PG_AUTOCONF_FILENAME
        const RUNTIME_COMPUTED    = 0x004000; // delay processing in 'postgres -C'
        const ALLOW_IN_PARALLEL   = 0x008000; // allow setting in parallel mode
    }
}

/// The GUC_UNIT_* sub-field: a mutually-exclusive 4-bit selector packed in the
/// high bits of "flags" (NOT bitflags -- OR-combining units is meaningless, see
/// bitflags-port.md 3.5). The raw values match the C `#define`s so they can be
/// masked out of/into the flags word.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GucUnit {
    None = 0,
    Kb = 0x01000000,      // GUC_UNIT_KB
    Blocks = 0x02000000,  // GUC_UNIT_BLOCKS
    XBlocks = 0x03000000, // GUC_UNIT_XBLOCKS
    Mb = 0x04000000,      // GUC_UNIT_MB
    Byte = 0x05000000,    // GUC_UNIT_BYTE
    Ms = 0x10000000,      // GUC_UNIT_MS
    S = 0x20000000,       // GUC_UNIT_S
    Min = 0x30000000,     // GUC_UNIT_MIN
}

/// Mask for size-related units (GUC_UNIT_MEMORY).
pub const GUC_UNIT_MEMORY: i32 = 0x0F000000;
/// Mask for time-related units (GUC_UNIT_TIME).
pub const GUC_UNIT_TIME: i32 = 0x70000000;
/// Combined unit mask (GUC_UNIT).
pub const GUC_UNIT: i32 = GUC_UNIT_MEMORY | GUC_UNIT_TIME;

impl GucUnit {
    /// Extract the unit selector from a raw "flags" word.
    pub fn from_flags(flags: i32) -> Self {
        match flags & GUC_UNIT {
            0x01000000 => Self::Kb,
            0x02000000 => Self::Blocks,
            0x03000000 => Self::XBlocks,
            0x04000000 => Self::Mb,
            0x05000000 => Self::Byte,
            0x10000000 => Self::Ms,
            0x20000000 => Self::S,
            0x30000000 => Self::Min,
            _ => Self::None,
        }
    }
}

// --- GUC variable records (guc_tables.h) ---

/// GUC variable types.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum config_type {
    BOOL,
    INT,
    REAL,
    STRING,
    ENUM,
}

/// The actual value of a GUC variable. C is a `union`; the discriminant is the
/// owning record's `vartype`, so this maps to an enum.
#[derive(Debug, Clone)]
pub enum config_var_val {
    Bool(bool),
    Int(i32),
    Real(f64),
    String(Option<String>),
    Enum(i32),
}

/// A GUC value plus its check-hook-produced opaque "extra".
pub struct config_var_value {
    pub val: config_var_val,
    pub extra: *mut (), // TODO(ptr): typed extra payload
}

/// Groupings to organize run-time options for display (pg_settings).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Stack entry state for a not-yet-committed transactional GUC change. Almost
/// GucAction, but with a fourth state for SET+LOCAL.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GucStackState {
    SAVE,      // entry caused by function SET option
    SET,       // entry caused by plain SET command
    LOCAL,     // entry caused by SET LOCAL command
    SET_LOCAL, // entry caused by SET then SET LOCAL
}

/// Saved prior value of a GUC during an uncommitted transactional change.
pub struct GucStack {
    pub prev: Option<Box<Self>>,
    pub nest_level: i32,
    pub state: GucStackState,
    pub source: GucSource,
    pub scontext: GucContext,
    pub masked_scontext: GucContext,
    pub srole: Oid,
    pub masked_srole: Oid,
    pub prior: config_var_value,
    pub masked: config_var_value,
}

/// Status bits of a GUC variable (`status` field). Transient/runtime state.
pub const GUC_IS_IN_FILE: i32 = 0x0001; // found it in config file
pub const GUC_PENDING_RESTART: i32 = 0x0002; // changed value cannot be applied yet
pub const GUC_NEEDS_REPORT: i32 = 0x0004; // new value must be reported to client

/// Generic fields applicable to all GUC variable records. The C `dlist_node`/
/// `slist_node` link fields are dropped: the maintenance lists become owned Rust
/// collections under the single-process model.
pub struct config_generic {
    // constant fields, must be set correctly in initial value:
    pub name: &'static str,
    pub context: GucContext,
    pub group: config_group,
    pub short_desc: &'static str,
    pub long_desc: &'static str,
    pub flags: i32, // GucFlags bits plus the GucUnit selector
    // variable fields, initialized at runtime:
    pub vartype: config_type,
    pub status: i32,
    pub source: GucSource,
    pub reset_source: GucSource,
    pub scontext: GucContext,
    pub reset_scontext: GucContext,
    pub srole: Oid,
    pub reset_srole: Oid,
    pub stack: Option<Box<GucStack>>,
    pub extra: *mut (), // TODO(ptr)
    pub last_reported: Option<String>,
    pub sourcefile: Option<String>,
    pub sourceline: i32,
}

/// A bool GUC record. `variable` points at the backing global (a raw slot here).
pub struct config_bool {
    pub r#gen: config_generic,
    pub variable: *mut bool, // TODO(ptr): backing global
    pub boot_val: bool,
    pub check_hook: Option<GucBoolCheckHook>,
    pub assign_hook: Option<GucBoolAssignHook>,
    pub show_hook: Option<GucShowHook>,
    pub reset_val: bool,
    pub reset_extra: *mut (),
}

/// An int GUC record.
pub struct config_int {
    pub r#gen: config_generic,
    pub variable: *mut i32, // TODO(ptr)
    pub boot_val: i32,
    pub min: i32,
    pub max: i32,
    pub check_hook: Option<GucIntCheckHook>,
    pub assign_hook: Option<GucIntAssignHook>,
    pub show_hook: Option<GucShowHook>,
    pub reset_val: i32,
    pub reset_extra: *mut (),
}

/// A real (double) GUC record.
pub struct config_real {
    pub r#gen: config_generic,
    pub variable: *mut f64, // TODO(ptr)
    pub boot_val: f64,
    pub min: f64,
    pub max: f64,
    pub check_hook: Option<GucRealCheckHook>,
    pub assign_hook: Option<GucRealAssignHook>,
    pub show_hook: Option<GucShowHook>,
    pub reset_val: f64,
    pub reset_extra: *mut (),
}

/// A string GUC record. boot_val may be NULL in C (-> None).
pub struct config_string {
    pub r#gen: config_generic,
    pub variable: *mut Option<String>, // TODO(ptr)
    pub boot_val: Option<&'static str>,
    pub check_hook: Option<GucStringCheckHook>,
    pub assign_hook: Option<GucStringAssignHook>,
    pub show_hook: Option<GucShowHook>,
    pub reset_val: Option<String>,
    pub reset_extra: *mut (),
}

/// An enum GUC record.
pub struct config_enum {
    pub r#gen: config_generic,
    pub variable: *mut i32, // TODO(ptr)
    pub boot_val: i32,
    pub options: &'static [config_enum_entry],
    pub check_hook: Option<GucEnumCheckHook>,
    pub assign_hook: Option<GucEnumAssignHook>,
    pub show_hook: Option<GucShowHook>,
    pub reset_val: i32,
    pub reset_extra: *mut (),
}

/// Opaque handle to a `config_generic` (C `config_handle`). TODO(ptr).
pub type config_handle = config_generic;

// --- Config file parsing (returns success bool -> Result; head/tail out-params
// collapse into the returned list) ---

pub fn ParseConfigFile(
    _config_file: &str,
    _strict: bool,
    _calling_file: Option<&str>,
    _calling_lineno: i32,
    _depth: i32,
    _elevel: i32,
) -> Result<Vec<ConfigVariable>, ()> {
    unimplemented!()
}

pub fn ParseConfigDirectory(
    _includedir: &str,
    _calling_file: Option<&str>,
    _calling_lineno: i32,
    _depth: i32,
    _elevel: i32,
) -> Result<Vec<ConfigVariable>, ()> {
    unimplemented!()
}

pub fn FreeConfigVariables(_list: Vec<ConfigVariable>) {}

pub fn DeescapeQuotedString(_s: &str) -> String {
    unimplemented!()
}

// --- Functions exported by guc.c ---

pub use crate::backend::utils::misc::guc::SetConfigOption;

#[allow(clippy::too_many_arguments)]
pub fn DefineCustomBoolVariable(
    _name: &str,
    _short_desc: Option<&str>,
    _long_desc: Option<&str>,
    _value_addr: *mut bool,
    _boot_value: bool,
    _context: GucContext,
    _flags: i32,
    _check_hook: Option<GucBoolCheckHook>,
    _assign_hook: Option<GucBoolAssignHook>,
    _show_hook: Option<GucShowHook>,
) {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
pub fn DefineCustomIntVariable(
    _name: &str,
    _short_desc: Option<&str>,
    _long_desc: Option<&str>,
    _value_addr: *mut i32,
    _boot_value: i32,
    _min_value: i32,
    _max_value: i32,
    _context: GucContext,
    _flags: i32,
    _check_hook: Option<GucIntCheckHook>,
    _assign_hook: Option<GucIntAssignHook>,
    _show_hook: Option<GucShowHook>,
) {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
pub fn DefineCustomRealVariable(
    _name: &str,
    _short_desc: Option<&str>,
    _long_desc: Option<&str>,
    _value_addr: *mut f64,
    _boot_value: f64,
    _min_value: f64,
    _max_value: f64,
    _context: GucContext,
    _flags: i32,
    _check_hook: Option<GucRealCheckHook>,
    _assign_hook: Option<GucRealAssignHook>,
    _show_hook: Option<GucShowHook>,
) {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
pub fn DefineCustomStringVariable(
    _name: &str,
    _short_desc: Option<&str>,
    _long_desc: Option<&str>,
    _value_addr: *mut Option<String>,
    _boot_value: Option<&str>,
    _context: GucContext,
    _flags: i32,
    _check_hook: Option<GucStringCheckHook>,
    _assign_hook: Option<GucStringAssignHook>,
    _show_hook: Option<GucShowHook>,
) {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
pub fn DefineCustomEnumVariable(
    _name: &str,
    _short_desc: Option<&str>,
    _long_desc: Option<&str>,
    _value_addr: *mut i32,
    _boot_value: i32,
    _options: &'static [config_enum_entry],
    _context: GucContext,
    _flags: i32,
    _check_hook: Option<GucEnumCheckHook>,
    _assign_hook: Option<GucEnumAssignHook>,
    _show_hook: Option<GucShowHook>,
) {
    unimplemented!()
}

pub fn MarkGUCPrefixReserved(_class_name: &str) {
    unimplemented!()
}

pub use crate::backend::utils::misc::guc::{GetConfigOption, GetConfigOptionResetString};

pub fn GetConfigOptionFlags(_name: &str, _missing_ok: bool) -> i32 {
    unimplemented!()
}

pub fn ProcessConfigFile(_context: GucContext) {
    unimplemented!()
}

pub fn convert_GUC_name_for_parameter_acl(_name: &str) -> String {
    unimplemented!()
}

pub fn check_GUC_name_for_parameter_acl(_name: &str) {
    unimplemented!()
}

pub fn InitializeGUCOptions() {
    unimplemented!()
}

pub fn SelectConfigFiles(_user_doption: Option<&str>, _progname: &str) -> bool {
    unimplemented!()
}

pub use crate::backend::utils::misc::guc::{
    AtEOXact_GUC, AtStart_GUC, NewGUCNestLevel, ResetAllOptions, RestrictSearchPath,
};

pub fn BeginReportingGUCOptions() {
    unimplemented!()
}

pub fn ReportChangedGUCOptions() {
    unimplemented!()
}

/// Split "name=value" into (name, value) (C out-params).
pub fn ParseLongOption(_string: &str) -> (String, Option<String>) {
    unimplemented!()
}

pub fn get_config_unit_name(_flags: i32) -> Option<&'static str> {
    unimplemented!()
}

/// Parse an int with unit handling. Err carries the optional hint message.
pub fn parse_int(_value: &str, _flags: i32) -> Result<i32, Option<&'static str>> {
    unimplemented!()
}

/// Parse a real with unit handling. Err carries the optional hint message.
pub fn parse_real(_value: &str, _flags: i32) -> Result<f64, Option<&'static str>> {
    unimplemented!()
}

pub use crate::backend::utils::misc::guc::{set_config_option, set_config_option_ext};

#[allow(clippy::too_many_arguments)]
pub fn set_config_with_handle(
    _name: &str,
    _handle: &mut config_handle,
    _value: Option<&str>,
    _context: GucContext,
    _source: GucSource,
    _srole: Oid,
    _action: GucAction,
    _change_val: bool,
    _elevel: i32,
    _is_reload: bool,
) -> i32 {
    unimplemented!()
}

/// Find a GUC's handle by name; None if unknown.
pub fn get_config_handle(_name: &str) -> Option<&'static mut config_handle> {
    unimplemented!()
}

pub fn AlterSystemSetConfigFile(_altersysstmt: &AlterSystemStmt) {
    unimplemented!()
}

pub use crate::backend::utils::misc::guc::GetConfigOptionByName;

/// Transform a GUC array into (names, values) lists (C out-params).
pub fn TransformGUCArray(_array: &ArrayType) -> (Vec<String>, Vec<String>) {
    unimplemented!()
}

pub fn ProcessGUCArray(
    _array: &ArrayType,
    _context: GucContext,
    _source: GucSource,
    _action: GucAction,
) {
    unimplemented!()
}

pub fn GUCArrayAdd(_array: Option<&ArrayType>, _name: &str, _value: &str) -> *mut ArrayType {
    unimplemented!()
}

pub fn GUCArrayDelete(_array: Option<&ArrayType>, _name: &str) -> *mut ArrayType {
    unimplemented!()
}

pub fn GUCArrayReset(_array: Option<&ArrayType>) -> *mut ArrayType {
    unimplemented!()
}

// GUC serialization (parallel worker state transfer).
pub fn EstimateGUCStateSpace() -> usize {
    unimplemented!()
}

pub fn SerializeGUCState(_maxsize: usize, _start_address: *mut u8) {
    unimplemented!()
}

pub fn RestoreGUCState(_gucstate: *mut ()) {
    unimplemented!()
}

// --- Functions exported by guc_funcs.c ---

pub use crate::backend::utils::misc::guc_funcs::{
    ExecSetVariableStmt, ExtractSetVariableArgs, GetPGVariable, SetPGVariable,
};

pub fn GetPGVariableResultDesc(_name: &str) -> TupleDesc {
    unimplemented!()
}

// --- Support for messages reported from GUC check hooks ---
// TODO(global): these process-global strings become task-local under the async
// model.
pub static mut GUC_check_errmsg_string: Option<String> = None;
pub static mut GUC_check_errdetail_string: Option<String> = None;
pub static mut GUC_check_errhint_string: Option<String> = None;

pub fn GUC_check_errcode(_sqlerrcode: i32) {
    unimplemented!()
}

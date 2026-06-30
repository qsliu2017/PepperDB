//! PG `src/backend/utils/misc/guc_tables.c` -- the built-in GUC variable
//! definition tables (ConfigureNamesBool/Int/Real/String/Enum) plus the table
//! machinery the engine reads.
//!
//! Redesign (rules.md s10): C's five parallel `struct config_bool[]`/etc. arrays
//! that each embed a `void *variable` backing-global pointer collapse into one
//! [`ConfigVarDef`] type whose [`ConfigKind`] carries the per-type boot value,
//! range, options, and the (sync) check/assign/show hooks. The live value lives
//! in `guc.rs`'s per-task store, not behind a pointer, so the tables here are
//! pure `static` data. [`all_config_vars`] concatenates them in the engine's
//! build order.
//!
//! STAGED long tail (rules.md s4): only ~30 of PG's ~350 variables are defined
//! here -- the ones already referenced elsewhere in the tree (`work_mem`,
//! `maintenance_work_mem`, `search_path`, `client_encoding`) plus the common
//! settings SET/SHOW exercises (`DateStyle`, `TimeZone`, `statement_timeout`,
//! the `enable_*` planner toggles, the `*transaction_*` characteristics). The
//! table is data: extend it as more subsystems come online.

use crate::utils::guc::{config_enum_entry, config_group, GucContext, GucFlags};
use crate::utils::guc_tables::{ConfigKind, ConfigVarDef};

use crate::backend::commands::variable as varhooks;

/// PG `MAX_KILOBYTES` upper bound for memory GUCs.
const MAX_KILOBYTES: i32 = i32::MAX;

// --- transaction_isolation enum options (xact.h levels) ---
static ISOLATION_OPTIONS: &[config_enum_entry] = &[
    config_enum_entry {
        name: "serializable",
        val: crate::access::xact::XACT_SERIALIZABLE,
        hidden: false,
    },
    config_enum_entry {
        name: "repeatable read",
        val: crate::access::xact::XACT_REPEATABLE_READ,
        hidden: false,
    },
    config_enum_entry {
        name: "read committed",
        val: crate::access::xact::XACT_READ_COMMITTED,
        hidden: false,
    },
    config_enum_entry {
        name: "read uncommitted",
        val: crate::access::xact::XACT_READ_UNCOMMITTED,
        hidden: false,
    },
];

/// Codes for `bytea_output` (PG `enum bytea_output_type`): escape=0, hex=1.
static BYTEA_OUTPUT_OPTIONS: &[config_enum_entry] = &[
    config_enum_entry {
        name: "escape",
        val: 0,
        hidden: false,
    },
    config_enum_entry {
        name: "hex",
        val: 1,
        hidden: false,
    },
];

macro_rules! def_generic {
    ($name:literal, $ctx:ident, $group:ident, $desc:literal, $flags:expr, $kind:expr) => {
        ConfigVarDef {
            name: $name,
            context: GucContext::$ctx,
            group: config_group::$group,
            short_desc: $desc,
            flags: $flags,
            kind: $kind,
        }
    };
}

/// PG `ConfigureNamesBool`: the boolean variables (reachable subset).
pub static CONFIGURE_NAMES_BOOL: &[ConfigVarDef] = &[
    def_generic!(
        "enable_seqscan",
        USERSET,
        QUERY_TUNING_METHOD,
        "Enables the planner's use of sequential-scan plans.",
        GucFlags::EXPLAIN,
        ConfigKind::Bool {
            boot: true,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "enable_indexscan",
        USERSET,
        QUERY_TUNING_METHOD,
        "Enables the planner's use of index-scan plans.",
        GucFlags::EXPLAIN,
        ConfigKind::Bool {
            boot: true,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "enable_bitmapscan",
        USERSET,
        QUERY_TUNING_METHOD,
        "Enables the planner's use of bitmap-scan plans.",
        GucFlags::EXPLAIN,
        ConfigKind::Bool {
            boot: true,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "enable_sort",
        USERSET,
        QUERY_TUNING_METHOD,
        "Enables the planner's use of explicit sort steps.",
        GucFlags::EXPLAIN,
        ConfigKind::Bool {
            boot: true,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "enable_hashjoin",
        USERSET,
        QUERY_TUNING_METHOD,
        "Enables the planner's use of hash join plans.",
        GucFlags::EXPLAIN,
        ConfigKind::Bool {
            boot: true,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "enable_nestloop",
        USERSET,
        QUERY_TUNING_METHOD,
        "Enables the planner's use of nested-loop join plans.",
        GucFlags::EXPLAIN,
        ConfigKind::Bool {
            boot: true,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "enable_mergejoin",
        USERSET,
        QUERY_TUNING_METHOD,
        "Enables the planner's use of merge join plans.",
        GucFlags::EXPLAIN,
        ConfigKind::Bool {
            boot: true,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "default_transaction_read_only",
        USERSET,
        CLIENT_CONN_STATEMENT,
        "Sets the default read-only status of new transactions.",
        GucFlags::empty(),
        ConfigKind::Bool {
            boot: false,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "transaction_read_only",
        USERSET,
        CLIENT_CONN_STATEMENT,
        "Sets the current transaction's read-only status.",
        GucFlags::NO_RESET_ALL.union(GucFlags::NOT_IN_SAMPLE),
        ConfigKind::Bool {
            boot: false,
            check: Some(varhooks::check_transaction_read_only),
            assign: None,
        }
    ),
    def_generic!(
        "transaction_deferrable",
        USERSET,
        CLIENT_CONN_STATEMENT,
        "Whether to defer a read-only serializable transaction until it can be executed with no possible serialization failures.",
        GucFlags::NO_RESET_ALL.union(GucFlags::NOT_IN_SAMPLE),
        ConfigKind::Bool {
            boot: false,
            check: Some(varhooks::check_transaction_deferrable),
            assign: None,
        }
    ),
];

/// PG `ConfigureNamesInt`: the integer variables (reachable subset).
pub static CONFIGURE_NAMES_INT: &[ConfigVarDef] = &[
    def_generic!(
        "max_connections",
        POSTMASTER,
        CONN_AUTH_SETTINGS,
        "Sets the maximum number of concurrent connections.",
        GucFlags::empty(),
        ConfigKind::Int {
            boot: 100,
            min: 1,
            max: 262143,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "work_mem",
        USERSET,
        RESOURCES_MEM,
        "Sets the maximum memory to be used for query workspaces.",
        GucFlags::empty(),
        ConfigKind::Int {
            boot: 4096,
            min: 64,
            max: MAX_KILOBYTES,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "maintenance_work_mem",
        USERSET,
        RESOURCES_MEM,
        "Sets the maximum memory to be used for maintenance operations.",
        GucFlags::empty(),
        ConfigKind::Int {
            boot: 65536,
            min: 64,
            max: MAX_KILOBYTES,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "statement_timeout",
        USERSET,
        CLIENT_CONN_STATEMENT,
        "Sets the maximum allowed duration of any statement.",
        GucFlags::empty(),
        ConfigKind::Int {
            boot: 0,
            min: 0,
            max: i32::MAX,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "temp_buffers",
        USERSET,
        RESOURCES_MEM,
        "Sets the maximum number of temporary buffers used by each session.",
        GucFlags::empty(),
        ConfigKind::Int {
            boot: 1024,
            min: 100,
            max: i32::MAX / 2,
            check: None,
            assign: None,
        }
    ),
];

/// PG `ConfigureNamesReal`: the real variables (reachable subset).
pub static CONFIGURE_NAMES_REAL: &[ConfigVarDef] = &[
    def_generic!(
        "seq_page_cost",
        USERSET,
        QUERY_TUNING_COST,
        "Sets the planner's estimate of the cost of a sequentially fetched disk page.",
        GucFlags::empty(),
        ConfigKind::Real {
            boot: 1.0,
            min: 0.0,
            max: f64::MAX,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "random_page_cost",
        USERSET,
        QUERY_TUNING_COST,
        "Sets the planner's estimate of the cost of a nonsequentially fetched disk page.",
        GucFlags::empty(),
        ConfigKind::Real {
            boot: 4.0,
            min: 0.0,
            max: f64::MAX,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "cpu_tuple_cost",
        USERSET,
        QUERY_TUNING_COST,
        "Sets the planner's estimate of the cost of processing each tuple (row).",
        GucFlags::empty(),
        ConfigKind::Real {
            boot: 0.01,
            min: 0.0,
            max: f64::MAX,
            check: None,
            assign: None,
        }
    ),
];

/// PG `ConfigureNamesString`: the string variables (reachable subset).
pub static CONFIGURE_NAMES_STRING: &[ConfigVarDef] = &[
    def_generic!(
        "search_path",
        USERSET,
        CLIENT_CONN_STATEMENT,
        "Sets the schema search order for names that are not schema-qualified.",
        GucFlags::LIST_INPUT.union(GucFlags::LIST_QUOTE),
        ConfigKind::Str {
            boot: Some("\"$user\", public"),
            check: Some(varhooks::check_search_path),
            assign: Some(varhooks::assign_search_path),
            show: None,
        }
    ),
    def_generic!(
        "client_encoding",
        USERSET,
        CLIENT_CONN_LOCALE,
        "Sets the client's character set encoding.",
        GucFlags::IS_NAME.union(GucFlags::REPORT),
        ConfigKind::Str {
            boot: Some("SQL_ASCII"),
            check: Some(varhooks::check_client_encoding),
            assign: None,
            show: None,
        }
    ),
    def_generic!(
        "DateStyle",
        USERSET,
        CLIENT_CONN_LOCALE,
        "Sets the display format for date and time values.",
        GucFlags::LIST_INPUT.union(GucFlags::REPORT),
        ConfigKind::Str {
            boot: Some("ISO, MDY"),
            check: Some(varhooks::check_datestyle),
            assign: None,
            show: None,
        }
    ),
    def_generic!(
        "TimeZone",
        USERSET,
        CLIENT_CONN_LOCALE,
        "Sets the time zone for displaying and interpreting time stamps.",
        GucFlags::REPORT,
        ConfigKind::Str {
            boot: Some("GMT"),
            check: Some(varhooks::check_timezone),
            assign: None,
            show: None,
        }
    ),
    def_generic!(
        "application_name",
        USERSET,
        LOGGING_WHAT,
        "Sets the application name to be reported in statistics and logs.",
        GucFlags::REPORT.union(GucFlags::NOT_IN_SAMPLE),
        ConfigKind::Str {
            boot: Some(""),
            check: None,
            assign: None,
            show: None,
        }
    ),
];

/// PG `ConfigureNamesEnum`: the enum variables (reachable subset).
pub static CONFIGURE_NAMES_ENUM: &[ConfigVarDef] = &[
    def_generic!(
        "default_transaction_isolation",
        USERSET,
        CLIENT_CONN_STATEMENT,
        "Sets the transaction isolation level of each new transaction.",
        GucFlags::empty(),
        ConfigKind::Enum {
            boot: crate::access::xact::XACT_READ_COMMITTED,
            options: ISOLATION_OPTIONS,
            check: None,
            assign: None,
        }
    ),
    def_generic!(
        "transaction_isolation",
        USERSET,
        CLIENT_CONN_STATEMENT,
        "Sets the current transaction's isolation level.",
        GucFlags::NO_RESET_ALL.union(GucFlags::NOT_IN_SAMPLE),
        ConfigKind::Enum {
            boot: crate::access::xact::XACT_READ_COMMITTED,
            options: ISOLATION_OPTIONS,
            check: Some(varhooks::check_transaction_isolation),
            assign: None,
        }
    ),
    def_generic!(
        "bytea_output",
        USERSET,
        CLIENT_CONN_STATEMENT,
        "Sets the output format for bytea.",
        GucFlags::empty(),
        ConfigKind::Enum {
            boot: 1,
            options: BYTEA_OUTPUT_OPTIONS,
            check: None,
            assign: None,
        }
    ),
];

/// All built-in variable definitions, in PG's build order (bool, int, real,
/// string, enum). The engine snapshots these into per-task `GucVariable`s.
pub fn all_config_vars() -> Vec<&'static ConfigVarDef> {
    CONFIGURE_NAMES_BOOL
        .iter()
        .chain(CONFIGURE_NAMES_INT)
        .chain(CONFIGURE_NAMES_REAL)
        .chain(CONFIGURE_NAMES_STRING)
        .chain(CONFIGURE_NAMES_ENUM)
        .collect()
}

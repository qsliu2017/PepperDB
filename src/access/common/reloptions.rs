//! Translation of postgres/src/backend/access/common/reloptions.c
//! (merged with the parts of postgres/src/include/access/reloptions.h it needs:
//! the relopt_type / relopt_kind enums, the relopt_gen / relopt_value /
//! relopt_bool / relopt_int / relopt_real / relopt_enum / relopt_string /
//! relopt_enum_elt_def / relopt_parse_elt / local_relopt / local_relopts
//! structs, the fn-ptr typedefs, and the GET_STRING_RELOPTION macro).
//!
//! Core support for relation options (pg_class.reloptions)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping (from reloptions.c):
//!   postgres.h                  -> crate::prelude
//!   <float.h>                   -> f64::MAX for DBL_MAX
//!   access/gist_private.h       -> crate::access::gist::gist_private (GIST_*_FILLFACTOR, GistOptBufferingMode)
//!   access/hash.h               -> crate::access::hash::hash (HASH_*_FILLFACTOR)
//!   access/heaptoast.h          -> crate::access::heap::heaptoast (TOAST_TUPLE_TARGET[_MAIN])
//!   access/htup_details.h       -> crate::access::htup_details (HeapTuple, GETSTRUCT, fastgetattr)
//!   access/nbtree.h             -> BTREE_*_FILLFACTOR (STUB const below; nbtree.h not split out)
//!   access/reloptions.h         -> MERGED HERE
//!   access/spgist_private.h     -> crate::access::spgist::spgist_private (SPGIST_*_FILLFACTOR)
//!   catalog/pg_type.h           -> TEXTOID from crate::catalog::pg_known_oids
//!   commands/defrem.h           -> defGetString / defGetBoolean from crate::commands::defrem
//!   commands/tablespace.h       -> (only TableSpaceOpts, stubbed locally)
//!   nodes/makefuncs.h           -> makeString / makeDefElem from crate::nodes::makefuncs
//!   utils/array.h               -> ArrayType / ArrayBuildState + array helpers
//!   utils/builtins.h            -> TextDatumGetCString / parse_int / parse_real / parse_bool
//!   utils/guc.h                 -> MAX_KILOBYTES
//!   utils/memutils.h            -> TopMemoryContext / MemoryContext* helpers
//!   utils/rel.h                 -> StdRdOptions / ViewOptions / AutoVacOpts / STDRD_OPTION_* /
//!                                  VIEW_OPTION_* (the big option structs are used only via
//!                                  offsetof, and the rel.h structs are not canonically homed
//!                                  yet, so they are stubbed locally with TODO(pg-port)).
//!
//! WHAT IS REAL vs STUBBED:
//!   The whole reloptions.c logic is translated 1:1. The dependency stubs are:
//!   parse_bool/parse_int/parse_real (other .c files), the rel.h option structs
//!   used only for offsetof, and a handful of fillfactor / IO-concurrency consts
//!   from headers not yet split out.

use crate::prelude::*; // Datum, Oid, Size, bits32, c_*, palloc*, pfree, pstrdup, elog!, ereport!, errmsg!,
                       // Assert!, MemoryContext*, TopMemoryContext, lengthof!, null/null_mut, VARHDRSZ, bytea, text
use core::ffi::CStr;
use core::mem::size_of;
use core::ptr;

use crate::access::common::tupdesc::TupleDesc; // access/tupdesc.h
use crate::access::gist::gist_private::{
    GIST_DEFAULT_FILLFACTOR, GIST_MIN_FILLFACTOR, GIST_OPTION_BUFFERING_AUTO,
    GIST_OPTION_BUFFERING_OFF, GIST_OPTION_BUFFERING_ON,
}; // access/gist_private.h
// access/hash.h (HASH_*_FILLFACTOR); hash.rs is not split out as a submodule yet,
// so define locally matching postgres/src/include/access/hash.h.
const HASH_DEFAULT_FILLFACTOR: c_int = 75;
const HASH_MIN_FILLFACTOR: c_int = 10;
use crate::access::heap::heaptoast::{TOAST_TUPLE_TARGET, TOAST_TUPLE_TARGET_MAIN}; // access/heaptoast.h
use crate::access::htup_details::{fastgetattr, HeapTuple, GETSTRUCT}; // access/htup_details.h
use crate::access::index::amapi::amoptions_function; // access/amapi.h (amoptions_function)
use crate::access::spgist::spgist_private::{SPGIST_DEFAULT_FILLFACTOR, SPGIST_MIN_FILLFACTOR}; // access/spgist_private.h
use crate::catalog::pg_class::{
    Form_pg_class, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_TOASTVALUE, RELKIND_VIEW,
}; // catalog/pg_class.h
use crate::catalog::pg_type_d::TEXTOID; // catalog/pg_type.h
use crate::commands::defrem::{defGetBoolean, defGetString}; // commands/defrem.h
use crate::nodes::makefuncs::makeDefElem; // nodes/makefuncs.h
use crate::nodes::nodes::Node; // nodes/nodes.h (Node)
use crate::nodes::parsenodes::DefElem; // nodes/parsenodes.h (DefElem)
use crate::nodes::pg_list::{lappend, list_length, List, ListCell, NIL}; // nodes/pg_list.h
use crate::nodes::value::makeString; // nodes/value.h (makeString)
use crate::varatt::{SET_VARSIZE, VARDATA, VARSIZE}; // varatt.h (VARDATA/VARSIZE/SET_VARSIZE)
use crate::port::pgstrcasecmp::pg_strcasecmp; // port (pg_strcasecmp)
use crate::storage::lockdefs::{
    AccessExclusiveLock, NoLock, ShareUpdateExclusiveLock, LOCKMODE,
}; // storage/lock.h
use crate::storage::lmgr::lock::DoLockModesConflict; // storage/lock.h (DoLockModesConflict)
use crate::utils::adt::arrayfuncs::{
    accumArrayResult, deconstruct_array_builtin, makeArrayResult, ArrayBuildState,
}; // utils/array.h
use crate::utils::array::ArrayType; // utils/array.h (ArrayType)
use crate::utils::builtins::{parse_bool, TextDatumGetCString}; // utils/builtins.h
use crate::utils::misc::guc::{parse_int, parse_real, MAX_KILOBYTES}; // utils/guc.h
use crate::utils::mmgr::mcxt::MemoryContextStrdup; // utils/memutils.h (MemoryContextStrdup)

use crate::{elog, ereport, errmsg, foreach};

// ----- gettext no-ops (C: _() / gettext_noop / errdetail_internal "%s") -----
macro_rules! gettext_noop {
    ($s:literal) => {
        concat!($s, "\0").as_ptr() as *const c_char
    };
}

// ----- C limit/INT_MAX helpers -----
const INT_MAX: c_int = c_int::MAX;
const DBL_MAX: f64 = f64::MAX;

// TODO(pg-port): real constants live in headers not yet split out.
const HEAP_DEFAULT_FILLFACTOR: c_int = 100; // access/htup_details.h
const HEAP_MIN_FILLFACTOR: c_int = 10; // access/htup_details.h
const BTREE_DEFAULT_FILLFACTOR: c_int = 90; // access/nbtree.h
const BTREE_MIN_FILLFACTOR: c_int = 10; // access/nbtree.h
const MAX_IO_CONCURRENCY: c_int = 1000; // storage/bufmgr.h

// TODO(pg-port): StdRdOptIndexCleanup / STDRD_OPTION_* live in utils/rel.h
type StdRdOptIndexCleanup = c_int;
const STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO: StdRdOptIndexCleanup = 0;
const STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON: StdRdOptIndexCleanup = 1;
const STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF: StdRdOptIndexCleanup = 2;

// TODO(pg-port): ViewOptCheckOption / VIEW_OPTION_* live in utils/rel.h
type ViewOptCheckOption = c_int;
const VIEW_OPTION_CHECK_OPTION_NOT_SET: ViewOptCheckOption = 0;
const VIEW_OPTION_CHECK_OPTION_LOCAL: ViewOptCheckOption = 1;
const VIEW_OPTION_CHECK_OPTION_CASCADED: ViewOptCheckOption = 2;

// TODO(pg-port): Anum_pg_class_reloptions lives in catalog/pg_class_d.h (generated).
const Anum_pg_class_reloptions: c_int = 33;

// ----- errcode placeholders folded into block comments (single-message ereport!) -----
// C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errcode(ERRCODE_SYNTAX_ERROR),
//   errcode(ERRCODE_INVALID_PARAMETER_VALUE), errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
//   errcode(ERRCODE_WRONG_OBJECT_TYPE).

/*
 * =========================================================================
 *  reloptions.h  --  merged type definitions
 * =========================================================================
 */

/* types supported by reloptions */
pub type relopt_type = c_int;
pub const RELOPT_TYPE_BOOL: relopt_type = 0;
pub const RELOPT_TYPE_INT: relopt_type = 1;
pub const RELOPT_TYPE_REAL: relopt_type = 2;
pub const RELOPT_TYPE_ENUM: relopt_type = 3;
pub const RELOPT_TYPE_STRING: relopt_type = 4;

/* kinds supported by reloptions */
pub type relopt_kind = c_int;
pub const RELOPT_KIND_LOCAL: relopt_kind = 0;
pub const RELOPT_KIND_HEAP: relopt_kind = 1 << 0;
pub const RELOPT_KIND_TOAST: relopt_kind = 1 << 1;
pub const RELOPT_KIND_BTREE: relopt_kind = 1 << 2;
pub const RELOPT_KIND_HASH: relopt_kind = 1 << 3;
pub const RELOPT_KIND_GIN: relopt_kind = 1 << 4;
pub const RELOPT_KIND_GIST: relopt_kind = 1 << 5;
pub const RELOPT_KIND_ATTRIBUTE: relopt_kind = 1 << 6;
pub const RELOPT_KIND_TABLESPACE: relopt_kind = 1 << 7;
pub const RELOPT_KIND_SPGIST: relopt_kind = 1 << 8;
pub const RELOPT_KIND_VIEW: relopt_kind = 1 << 9;
pub const RELOPT_KIND_BRIN: relopt_kind = 1 << 10;
pub const RELOPT_KIND_PARTITIONED: relopt_kind = 1 << 11;
/* if you add a new kind, make sure you update "last_default" too */
pub const RELOPT_KIND_LAST_DEFAULT: relopt_kind = RELOPT_KIND_PARTITIONED;
/* some compilers treat enums as signed ints, so we can't use 1 << 31 */
pub const RELOPT_KIND_MAX: relopt_kind = 1 << 30;

/* reloption namespaces allowed for heaps -- currently only TOAST */
// C: #define HEAP_RELOPT_NAMESPACES { "toast", NULL }
pub const HEAP_RELOPT_NAMESPACES: [*const c_char; 2] = [c"toast".as_ptr(), ptr::null()];

/* generic struct to hold shared data */
#[repr(C)]
pub struct relopt_gen {
    pub name: *const c_char, /* must be first (used as list termination marker) */
    pub desc: *const c_char,
    pub kinds: bits32,
    pub lockmode: LOCKMODE,
    pub namelen: c_int,
    pub r#type: relopt_type,
}

/* holds a parsed value */
#[repr(C)]
pub union relopt_value_union {
    pub bool_val: bool,
    pub int_val: c_int,
    pub real_val: f64,
    pub enum_val: c_int,
    pub string_val: *mut c_char, /* allocated separately */
}

#[repr(C)]
pub struct relopt_value {
    pub gen: *mut relopt_gen,
    pub isset: bool,
    pub values: relopt_value_union,
}

/* reloptions records for specific variable types */
#[repr(C)]
pub struct relopt_bool {
    pub gen: relopt_gen,
    pub default_val: bool,
}

#[repr(C)]
pub struct relopt_int {
    pub gen: relopt_gen,
    pub default_val: c_int,
    pub min: c_int,
    pub max: c_int,
}

#[repr(C)]
pub struct relopt_real {
    pub gen: relopt_gen,
    pub default_val: f64,
    pub min: f64,
    pub max: f64,
}

/*
 * relopt_enum_elt_def -- One member of the array of acceptable values
 * of an enum reloption.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct relopt_enum_elt_def {
    pub string_val: *const c_char,
    pub symbol_val: c_int,
}

#[repr(C)]
pub struct relopt_enum {
    pub gen: relopt_gen,
    pub members: *mut relopt_enum_elt_def,
    pub default_val: c_int,
    pub detailmsg: *const c_char,
    /* null-terminated array of members */
}

/* validation routines for strings */
pub type validate_string_relopt = Option<unsafe extern "C" fn(value: *const c_char)>;
pub type fill_string_relopt =
    Option<unsafe extern "C" fn(value: *const c_char, ptr: *mut c_void) -> Size>;

/* validation routine for the whole option set */
pub type relopts_validator = Option<
    unsafe extern "C" fn(parsed_options: *mut c_void, vals: *mut relopt_value, nvals: c_int),
>;

#[repr(C)]
pub struct relopt_string {
    pub gen: relopt_gen,
    pub default_len: c_int,
    pub default_isnull: bool,
    pub validate_cb: validate_string_relopt,
    pub fill_cb: fill_string_relopt,
    pub default_val: *mut c_char,
}

/* This is the table datatype for build_reloptions() */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct relopt_parse_elt {
    pub optname: *const c_char,  /* option's name */
    pub opttype: relopt_type,    /* option's datatype */
    pub offset: c_int,           /* offset of field in result struct */
    pub isset_offset: c_int,     /* optional offset of an "is set" field */
}
// The parse tables below are file-scope `static` (matching C's `static const`),
// so the raw `optname` pointer they carry must be Sync.  The pointer only ever
// targets a 'static c-string literal, so this is sound.
unsafe impl Sync for relopt_parse_elt {}

/* Local reloption definition */
#[repr(C)]
pub struct local_relopt {
    pub option: *mut relopt_gen, /* option definition */
    pub offset: c_int,           /* offset of parsed value in bytea structure */
}

/* Structure to hold local reloption data for build_local_reloptions() */
#[repr(C)]
pub struct local_relopts {
    pub options: *mut List,       /* list of local_relopt definitions */
    pub validators: *mut List,    /* list of relopts_validator callbacks */
    pub relopt_struct_size: Size, /* size of parsed bytea structure */
}

/*
 * Utility macro to get a value for a string reloption once the options
 * are parsed.
 */
// C: #define GET_STRING_RELOPTION(optstruct, member) ...  (lives in the header,
// used by consumers, not by reloptions.c itself; not needed here.)

/*
 * TODO(pg-port): the big option structs (StdRdOptions, ViewOptions,
 * AutoVacOpts, AttributeOpts, TableSpaceOpts) live in utils/rel.h and are used
 * here only as the targets of offsetof() in the parse tables.  They are not yet
 * canonically homed, so we stub them locally with the exact field layout from
 * utils/rel.h / commands/tablespace.h / utils/attoptcache.h.
 */
#[repr(C)]
pub struct AutoVacOpts {
    pub enabled: bool,
    pub vacuum_threshold: c_int,
    pub vacuum_max_threshold: c_int,
    pub vacuum_ins_threshold: c_int,
    pub analyze_threshold: c_int,
    pub vacuum_cost_limit: c_int,
    pub freeze_min_age: c_int,
    pub freeze_max_age: c_int,
    pub freeze_table_age: c_int,
    pub multixact_freeze_min_age: c_int,
    pub multixact_freeze_max_age: c_int,
    pub multixact_freeze_table_age: c_int,
    pub log_min_duration: c_int,
    pub vacuum_cost_delay: f64,
    pub vacuum_scale_factor: f64,
    pub vacuum_ins_scale_factor: f64,
    pub analyze_scale_factor: f64,
}

#[repr(C)]
pub struct StdRdOptions {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub fillfactor: c_int,
    pub toast_tuple_target: c_int,
    pub autovacuum: AutoVacOpts,
    pub user_catalog_table: bool,
    pub parallel_workers: c_int,
    pub vacuum_index_cleanup: StdRdOptIndexCleanup,
    pub vacuum_truncate: bool,
    pub vacuum_truncate_set: bool,
    pub vacuum_max_eager_freeze_failure_rate: f64,
}

#[repr(C)]
pub struct ViewOptions {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub security_barrier: bool,
    pub security_invoker: bool,
    pub check_option: ViewOptCheckOption,
}

#[repr(C)]
pub struct AttributeOpts {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub n_distinct: f64,
    pub n_distinct_inherited: f64,
}

#[repr(C)]
pub struct TableSpaceOpts {
    pub vl_len_: int32, /* varlena header (do not touch directly!) */
    pub random_page_cost: f64,
    pub seq_page_cost: f64,
    pub effective_io_concurrency: c_int,
    pub maintenance_io_concurrency: c_int,
}

/*
 * =========================================================================
 *  reloptions.c
 * =========================================================================
 */

/*
 * Contents of pg_class.reloptions
 *
 * (see the long comment in the C source for the rules on adding options and
 * choosing lock levels.)
 */

// gen_lit!: build a relopt_gen from string literals (name/desc become c-string
// pointers).  namelen/type are filled in by initialize_reloptions() at runtime,
// matching the C static-initializer that leaves them zero.
macro_rules! gen_lit {
    ($name:literal, $desc:literal, $kinds:expr, $lockmode:expr) => {
        relopt_gen {
            name: concat!($name, "\0").as_ptr() as *const c_char,
            desc: concat!($desc, "\0").as_ptr() as *const c_char,
            kinds: ($kinds) as bits32,
            lockmode: $lockmode,
            namelen: 0,
            r#type: 0,
        }
    };
}
// gen_null!: the {{NULL}} list terminator (only `name` matters; it is NULL).
macro_rules! gen_null {
    () => {
        relopt_gen {
            name: ptr::null(),
            desc: ptr::null(),
            kinds: 0,
            lockmode: 0,
            namelen: 0,
            r#type: 0,
        }
    };
}

static mut boolRelOpts: [relopt_bool; 9] = [
    relopt_bool {
        gen: gen_lit!(
            "autosummarize",
            "Enables automatic summarization on this BRIN index",
            RELOPT_KIND_BRIN,
            AccessExclusiveLock
        ),
        default_val: false,
    },
    relopt_bool {
        gen: gen_lit!(
            "autovacuum_enabled",
            "Enables autovacuum in this relation",
            RELOPT_KIND_HEAP | RELOPT_KIND_TOAST,
            ShareUpdateExclusiveLock
        ),
        default_val: true,
    },
    relopt_bool {
        gen: gen_lit!(
            "user_catalog_table",
            "Declare a table as an additional catalog table, e.g. for the purpose of logical replication",
            RELOPT_KIND_HEAP,
            AccessExclusiveLock
        ),
        default_val: false,
    },
    relopt_bool {
        gen: gen_lit!(
            "fastupdate",
            "Enables \"fast update\" feature for this GIN index",
            RELOPT_KIND_GIN,
            AccessExclusiveLock
        ),
        default_val: true,
    },
    relopt_bool {
        gen: gen_lit!(
            "security_barrier",
            "View acts as a row security barrier",
            RELOPT_KIND_VIEW,
            AccessExclusiveLock
        ),
        default_val: false,
    },
    relopt_bool {
        gen: gen_lit!(
            "security_invoker",
            "Privileges on underlying relations are checked as the invoking user, not the view owner",
            RELOPT_KIND_VIEW,
            AccessExclusiveLock
        ),
        default_val: false,
    },
    relopt_bool {
        gen: gen_lit!(
            "vacuum_truncate",
            "Enables vacuum to truncate empty pages at the end of this table",
            RELOPT_KIND_HEAP | RELOPT_KIND_TOAST,
            ShareUpdateExclusiveLock
        ),
        default_val: true,
    },
    relopt_bool {
        gen: gen_lit!(
            "deduplicate_items",
            "Enables \"deduplicate items\" feature for this btree index",
            RELOPT_KIND_BTREE,
            ShareUpdateExclusiveLock /* since it applies only to later inserts */
        ),
        default_val: true,
    },
    /* list terminator */
    relopt_bool {
        gen: gen_null!(),
        default_val: false,
    },
];

// reli!: a relopt_int table entry from string literals.
macro_rules! reli {
    ($name:literal, $desc:literal, $kinds:expr, $lockmode:expr, $def:expr, $min:expr, $max:expr) => {
        relopt_int {
            gen: gen_lit!($name, $desc, $kinds, $lockmode),
            default_val: $def,
            min: $min,
            max: $max,
        }
    };
}

static mut intRelOpts: [relopt_int; 24] = [
    reli!("fillfactor", "Packs table pages only to this percentage",
        RELOPT_KIND_HEAP, ShareUpdateExclusiveLock, /* applies only to later inserts */
        HEAP_DEFAULT_FILLFACTOR, HEAP_MIN_FILLFACTOR, 100),
    reli!("fillfactor", "Packs btree index pages only to this percentage",
        RELOPT_KIND_BTREE, ShareUpdateExclusiveLock, /* applies only to later inserts */
        BTREE_DEFAULT_FILLFACTOR, BTREE_MIN_FILLFACTOR, 100),
    reli!("fillfactor", "Packs hash index pages only to this percentage",
        RELOPT_KIND_HASH, ShareUpdateExclusiveLock, /* applies only to later inserts */
        HASH_DEFAULT_FILLFACTOR, HASH_MIN_FILLFACTOR, 100),
    reli!("fillfactor", "Packs gist index pages only to this percentage",
        RELOPT_KIND_GIST, ShareUpdateExclusiveLock, /* applies only to later inserts */
        GIST_DEFAULT_FILLFACTOR, GIST_MIN_FILLFACTOR, 100),
    reli!("fillfactor", "Packs spgist index pages only to this percentage",
        RELOPT_KIND_SPGIST, ShareUpdateExclusiveLock, /* applies only to later inserts */
        SPGIST_DEFAULT_FILLFACTOR, SPGIST_MIN_FILLFACTOR, 100),
    reli!("autovacuum_vacuum_threshold",
        "Minimum number of tuple updates or deletes prior to vacuum",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 0, INT_MAX),
    reli!("autovacuum_vacuum_max_threshold",
        "Maximum number of tuple updates or deletes prior to vacuum",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -2, -1, INT_MAX),
    reli!("autovacuum_vacuum_insert_threshold",
        "Minimum number of tuple inserts prior to vacuum, or -1 to disable insert vacuums",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -2, -1, INT_MAX),
    reli!("autovacuum_analyze_threshold",
        "Minimum number of tuple inserts, updates or deletes prior to analyze",
        RELOPT_KIND_HEAP, ShareUpdateExclusiveLock, -1, 0, INT_MAX),
    reli!("autovacuum_vacuum_cost_limit",
        "Vacuum cost amount available before napping, for autovacuum",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 1, 10000),
    reli!("autovacuum_freeze_min_age",
        "Minimum age at which VACUUM should freeze a table row, for autovacuum",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 0, 1000000000),
    reli!("autovacuum_multixact_freeze_min_age",
        "Minimum multixact age at which VACUUM should freeze a row multixact's, for autovacuum",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 0, 1000000000),
    reli!("autovacuum_freeze_max_age",
        "Age at which to autovacuum a table to prevent transaction ID wraparound",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 100000, 2000000000),
    reli!("autovacuum_multixact_freeze_max_age",
        "Multixact age at which to autovacuum a table to prevent multixact wraparound",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 10000, 2000000000),
    reli!("autovacuum_freeze_table_age",
        "Age at which VACUUM should perform a full table sweep to freeze row versions",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 0, 2000000000),
    reli!("autovacuum_multixact_freeze_table_age",
        "Age of multixact at which VACUUM should perform a full table sweep to freeze row versions",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, 0, 2000000000),
    reli!("log_autovacuum_min_duration",
        "Sets the minimum execution time above which autovacuum actions will be logged",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1, -1, INT_MAX),
    reli!("toast_tuple_target",
        "Sets the target tuple length at which external columns will be toasted",
        RELOPT_KIND_HEAP, ShareUpdateExclusiveLock,
        TOAST_TUPLE_TARGET as c_int, 128, TOAST_TUPLE_TARGET_MAIN as c_int),
    reli!("pages_per_range",
        "Number of pages that each page range covers in a BRIN index",
        RELOPT_KIND_BRIN, AccessExclusiveLock, 128, 1, 131072),
    reli!("gin_pending_list_limit",
        "Maximum size of the pending list for this GIN index, in kilobytes.",
        RELOPT_KIND_GIN, AccessExclusiveLock, -1, 64, MAX_KILOBYTES),
    reli!("effective_io_concurrency",
        "Number of simultaneous requests that can be handled efficiently by the disk subsystem.",
        RELOPT_KIND_TABLESPACE, ShareUpdateExclusiveLock, -1, 0, MAX_IO_CONCURRENCY),
    reli!("maintenance_io_concurrency",
        "Number of simultaneous requests that can be handled efficiently by the disk subsystem for maintenance work.",
        RELOPT_KIND_TABLESPACE, ShareUpdateExclusiveLock, -1, 0, MAX_IO_CONCURRENCY),
    reli!("parallel_workers",
        "Number of parallel processes that can be used per executor node for this relation.",
        RELOPT_KIND_HEAP, ShareUpdateExclusiveLock, -1, 0, 1024),
    /* list terminator */
    relopt_int { gen: gen_null!(), default_val: 0, min: 0, max: 0 },
];

// relr!: a relopt_real table entry from string literals.
macro_rules! relr {
    ($name:literal, $desc:literal, $kinds:expr, $lockmode:expr, $def:expr, $min:expr, $max:expr) => {
        relopt_real {
            gen: gen_lit!($name, $desc, $kinds, $lockmode),
            default_val: $def,
            min: $min,
            max: $max,
        }
    };
}

static mut realRelOpts: [relopt_real; 11] = [
    relr!("autovacuum_vacuum_cost_delay",
        "Vacuum cost delay in milliseconds, for autovacuum",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1.0, 0.0, 100.0),
    relr!("autovacuum_vacuum_scale_factor",
        "Number of tuple updates or deletes prior to vacuum as a fraction of reltuples",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1.0, 0.0, 100.0),
    relr!("autovacuum_vacuum_insert_scale_factor",
        "Number of tuple inserts prior to vacuum as a fraction of reltuples",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1.0, 0.0, 100.0),
    relr!("autovacuum_analyze_scale_factor",
        "Number of tuple inserts, updates or deletes prior to analyze as a fraction of reltuples",
        RELOPT_KIND_HEAP, ShareUpdateExclusiveLock, -1.0, 0.0, 100.0),
    relr!("vacuum_max_eager_freeze_failure_rate",
        "Fraction of pages in a relation vacuum can scan and fail to freeze before disabling eager scanning.",
        RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock, -1.0, 0.0, 1.0),
    relr!("seq_page_cost",
        "Sets the planner's estimate of the cost of a sequentially fetched disk page.",
        RELOPT_KIND_TABLESPACE, ShareUpdateExclusiveLock, -1.0, 0.0, DBL_MAX),
    relr!("random_page_cost",
        "Sets the planner's estimate of the cost of a nonsequentially fetched disk page.",
        RELOPT_KIND_TABLESPACE, ShareUpdateExclusiveLock, -1.0, 0.0, DBL_MAX),
    relr!("n_distinct",
        "Sets the planner's estimate of the number of distinct values appearing in a column (excluding child relations).",
        RELOPT_KIND_ATTRIBUTE, ShareUpdateExclusiveLock, 0.0, -1.0, DBL_MAX),
    relr!("n_distinct_inherited",
        "Sets the planner's estimate of the number of distinct values appearing in a column (including child relations).",
        RELOPT_KIND_ATTRIBUTE, ShareUpdateExclusiveLock, 0.0, -1.0, DBL_MAX),
    relr!("vacuum_cleanup_index_scale_factor",
        "Deprecated B-Tree parameter.",
        RELOPT_KIND_BTREE, ShareUpdateExclusiveLock, -1.0, 0.0, 1e10),
    /* list terminator */
    relopt_real { gen: gen_null!(), default_val: 0.0, min: 0.0, max: 0.0 },
];

// elt!: a relopt_enum_elt_def from a string literal + symbol.
macro_rules! elt {
    ($s:literal, $v:expr) => {
        relopt_enum_elt_def {
            string_val: concat!($s, "\0").as_ptr() as *const c_char,
            symbol_val: $v,
        }
    };
}
macro_rules! elt_null {
    () => {
        relopt_enum_elt_def { string_val: ptr::null(), symbol_val: 0 }
    };
}

/* values from StdRdOptIndexCleanup */
static mut StdRdOptIndexCleanupValues: [relopt_enum_elt_def; 10] = [
    elt!("auto", STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO),
    elt!("on", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON),
    elt!("off", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF),
    elt!("true", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON),
    elt!("false", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF),
    elt!("yes", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON),
    elt!("no", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF),
    elt!("1", STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON),
    elt!("0", STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF),
    elt_null!(), /* list terminator */
];

/* values from GistOptBufferingMode */
static mut gistBufferingOptValues: [relopt_enum_elt_def; 4] = [
    elt!("auto", GIST_OPTION_BUFFERING_AUTO),
    elt!("on", GIST_OPTION_BUFFERING_ON),
    elt!("off", GIST_OPTION_BUFFERING_OFF),
    elt_null!(), /* list terminator */
];

/* values from ViewOptCheckOption */
static mut viewCheckOptValues: [relopt_enum_elt_def; 3] = [
    /* no value for NOT_SET */
    elt!("local", VIEW_OPTION_CHECK_OPTION_LOCAL),
    elt!("cascaded", VIEW_OPTION_CHECK_OPTION_CASCADED),
    elt_null!(), /* list terminator */
];

static mut enumRelOpts: [relopt_enum; 4] = [
    relopt_enum {
        gen: gen_lit!("vacuum_index_cleanup", "Controls index vacuuming and index cleanup",
            RELOPT_KIND_HEAP | RELOPT_KIND_TOAST, ShareUpdateExclusiveLock),
        members: &raw mut StdRdOptIndexCleanupValues as *mut relopt_enum_elt_def,
        default_val: STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO,
        detailmsg: gettext_noop!("Valid values are \"on\", \"off\", and \"auto\"."),
    },
    relopt_enum {
        gen: gen_lit!("buffering", "Enables buffering build for this GiST index",
            RELOPT_KIND_GIST, AccessExclusiveLock),
        members: &raw mut gistBufferingOptValues as *mut relopt_enum_elt_def,
        default_val: GIST_OPTION_BUFFERING_AUTO,
        detailmsg: gettext_noop!("Valid values are \"on\", \"off\", and \"auto\"."),
    },
    relopt_enum {
        gen: gen_lit!("check_option", "View has WITH CHECK OPTION defined (local or cascaded).",
            RELOPT_KIND_VIEW, AccessExclusiveLock),
        members: &raw mut viewCheckOptValues as *mut relopt_enum_elt_def,
        default_val: VIEW_OPTION_CHECK_OPTION_NOT_SET,
        detailmsg: gettext_noop!("Valid values are \"local\" and \"cascaded\"."),
    },
    /* list terminator */
    relopt_enum { gen: gen_null!(), members: ptr::null_mut(), default_val: 0, detailmsg: ptr::null() },
];

static mut stringRelOpts: [relopt_string; 1] = [
    /* list terminator */
    relopt_string {
        gen: gen_null!(),
        default_len: 0,
        default_isnull: false,
        validate_cb: None,
        fill_cb: None,
        default_val: ptr::null_mut(),
    },
];

static mut relOpts: *mut *mut relopt_gen = ptr::null_mut();
static mut last_assigned_kind: bits32 = RELOPT_KIND_LAST_DEFAULT as bits32;

static mut num_custom_options: c_int = 0;
static mut custom_options: *mut *mut relopt_gen = ptr::null_mut();
static mut need_initialization: bool = true;

/*
 * Get the length of a string reloption (either default or the user-defined
 * value).  This is used for allocation purposes when building a set of
 * relation options.
 */
// C: #define GET_STRING_RELOPTION_LEN(option)
unsafe fn GET_STRING_RELOPTION_LEN(option: &relopt_value) -> Size {
    if option.isset {
        strlen(option.values.string_val) as Size
    } else {
        (*(option.gen as *mut relopt_string)).default_len as Size
    }
}

// libc functions used directly by the C source.
extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// TODO(pg-port): DatumGetArrayTypeP is the fmgr.h macro that detoasts a Datum
// into an ArrayType*.  The real (toast-aware) version lives in
// utils/adt/arrayfuncs.rs but is not pub; until it is exported we use the
// no-detoast cast (correct for already-flat datums, which is the common case
// for reloptions catalog values).
#[inline]
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType {
    DatumGetPointer(d) as *mut ArrayType
}

/*
 * initialize_reloptions
 *		initialization routine, must be called before parsing
 *
 * Initialize the relOpts array and fill each variable's type and name length.
 */
unsafe fn initialize_reloptions() {
    let mut i: c_int;
    let mut j: c_int;

    j = 0;
    i = 0;
    while !boolRelOpts[i as usize].gen.name.is_null() {
        Assert!(DoLockModesConflict(
            boolRelOpts[i as usize].gen.lockmode,
            boolRelOpts[i as usize].gen.lockmode
        ));
        j += 1;
        i += 1;
    }
    i = 0;
    while !intRelOpts[i as usize].gen.name.is_null() {
        Assert!(DoLockModesConflict(
            intRelOpts[i as usize].gen.lockmode,
            intRelOpts[i as usize].gen.lockmode
        ));
        j += 1;
        i += 1;
    }
    i = 0;
    while !realRelOpts[i as usize].gen.name.is_null() {
        Assert!(DoLockModesConflict(
            realRelOpts[i as usize].gen.lockmode,
            realRelOpts[i as usize].gen.lockmode
        ));
        j += 1;
        i += 1;
    }
    i = 0;
    while !enumRelOpts[i as usize].gen.name.is_null() {
        Assert!(DoLockModesConflict(
            enumRelOpts[i as usize].gen.lockmode,
            enumRelOpts[i as usize].gen.lockmode
        ));
        j += 1;
        i += 1;
    }
    i = 0;
    while !stringRelOpts[i as usize].gen.name.is_null() {
        Assert!(DoLockModesConflict(
            stringRelOpts[i as usize].gen.lockmode,
            stringRelOpts[i as usize].gen.lockmode
        ));
        j += 1;
        i += 1;
    }
    j += num_custom_options;

    if !relOpts.is_null() {
        pfree(relOpts as *mut c_void);
    }
    relOpts = MemoryContextAlloc(
        TopMemoryContext,
        (j as Size + 1) * size_of::<*mut relopt_gen>(),
    ) as *mut *mut relopt_gen;

    j = 0;
    i = 0;
    while !boolRelOpts[i as usize].gen.name.is_null() {
        *relOpts.offset(j as isize) = &raw mut boolRelOpts[i as usize].gen;
        (**relOpts.offset(j as isize)).r#type = RELOPT_TYPE_BOOL;
        (**relOpts.offset(j as isize)).namelen = strlen((**relOpts.offset(j as isize)).name) as c_int;
        j += 1;
        i += 1;
    }

    i = 0;
    while !intRelOpts[i as usize].gen.name.is_null() {
        *relOpts.offset(j as isize) = &raw mut intRelOpts[i as usize].gen;
        (**relOpts.offset(j as isize)).r#type = RELOPT_TYPE_INT;
        (**relOpts.offset(j as isize)).namelen = strlen((**relOpts.offset(j as isize)).name) as c_int;
        j += 1;
        i += 1;
    }

    i = 0;
    while !realRelOpts[i as usize].gen.name.is_null() {
        *relOpts.offset(j as isize) = &raw mut realRelOpts[i as usize].gen;
        (**relOpts.offset(j as isize)).r#type = RELOPT_TYPE_REAL;
        (**relOpts.offset(j as isize)).namelen = strlen((**relOpts.offset(j as isize)).name) as c_int;
        j += 1;
        i += 1;
    }

    i = 0;
    while !enumRelOpts[i as usize].gen.name.is_null() {
        *relOpts.offset(j as isize) = &raw mut enumRelOpts[i as usize].gen;
        (**relOpts.offset(j as isize)).r#type = RELOPT_TYPE_ENUM;
        (**relOpts.offset(j as isize)).namelen = strlen((**relOpts.offset(j as isize)).name) as c_int;
        j += 1;
        i += 1;
    }

    i = 0;
    while !stringRelOpts[i as usize].gen.name.is_null() {
        *relOpts.offset(j as isize) = &raw mut stringRelOpts[i as usize].gen;
        (**relOpts.offset(j as isize)).r#type = RELOPT_TYPE_STRING;
        (**relOpts.offset(j as isize)).namelen = strlen((**relOpts.offset(j as isize)).name) as c_int;
        j += 1;
        i += 1;
    }

    i = 0;
    while i < num_custom_options {
        *relOpts.offset(j as isize) = *custom_options.offset(i as isize);
        j += 1;
        i += 1;
    }

    /* add a list terminator */
    *relOpts.offset(j as isize) = ptr::null_mut();

    /* flag the work is complete */
    need_initialization = false;
}

/*
 * add_reloption_kind
 *		Create a new relopt_kind value, to be used in custom reloptions by
 *		user-defined AMs.
 */
pub unsafe fn add_reloption_kind() -> relopt_kind {
    /* don't hand out the last bit so that the enum's behavior is portable */
    if last_assigned_kind >= RELOPT_KIND_MAX as bits32 {
        ereport!(
            ERROR,
            errmsg!("user-defined relation parameter types limit exceeded")
        );
        // C also: errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
    }
    last_assigned_kind <<= 1;
    last_assigned_kind as relopt_kind
}

/*
 * add_reloption
 *		Add an already-created custom reloption to the list, and recompute the
 *		main parser table.
 */
unsafe fn add_reloption(newoption: *mut relopt_gen) {
    static mut max_custom_options: c_int = 0;

    if num_custom_options >= max_custom_options {
        let oldcxt: MemoryContext;

        oldcxt = MemoryContextSwitchTo(TopMemoryContext);

        if max_custom_options == 0 {
            max_custom_options = 8;
            custom_options = palloc(max_custom_options as Size * size_of::<*mut relopt_gen>())
                as *mut *mut relopt_gen;
        } else {
            max_custom_options *= 2;
            custom_options = repalloc(
                custom_options as *mut c_void,
                max_custom_options as Size * size_of::<*mut relopt_gen>(),
            ) as *mut *mut relopt_gen;
        }
        MemoryContextSwitchTo(oldcxt);
    }
    *custom_options.offset(num_custom_options as isize) = newoption;
    num_custom_options += 1;

    need_initialization = true;
}

/*
 * init_local_reloptions
 *		Initialize local reloptions that will parsed into bytea structure of
 * 		'relopt_struct_size'.
 */
pub unsafe fn init_local_reloptions(relopts: *mut local_relopts, relopt_struct_size: Size) {
    (*relopts).options = NIL;
    (*relopts).validators = NIL;
    (*relopts).relopt_struct_size = relopt_struct_size;
}

/*
 * register_reloptions_validator
 *		Register custom validation callback that will be called at the end of
 *		build_local_reloptions().
 */
pub unsafe fn register_reloptions_validator(
    relopts: *mut local_relopts,
    validator: relopts_validator,
) {
    (*relopts).validators = lappend((*relopts).validators, validator_to_ptr(validator));
}

// Helper: a function pointer is stored in the List as a void*; round-trip it.
#[inline]
fn validator_to_ptr(v: relopts_validator) -> *mut c_void {
    unsafe { core::mem::transmute::<relopts_validator, *mut c_void>(v) }
}
#[inline]
fn ptr_to_validator(p: *mut c_void) -> relopts_validator {
    unsafe { core::mem::transmute::<*mut c_void, relopts_validator>(p) }
}

/*
 * add_local_reloption
 *		Add an already-created custom reloption to the local list.
 */
unsafe fn add_local_reloption(relopts: *mut local_relopts, newoption: *mut relopt_gen, offset: c_int) {
    let opt: *mut local_relopt = palloc(size_of::<local_relopt>()) as *mut local_relopt;

    Assert!((offset as Size) < (*relopts).relopt_struct_size);

    (*opt).option = newoption;
    (*opt).offset = offset;

    (*relopts).options = lappend((*relopts).options, opt as *mut c_void);
}

/*
 * allocate_reloption
 *		Allocate a new reloption and initialize the type-agnostic fields
 *		(for types other than string)
 */
unsafe fn allocate_reloption(
    kinds: bits32,
    r#type: c_int,
    name: *const c_char,
    desc: *const c_char,
    lockmode: LOCKMODE,
) -> *mut relopt_gen {
    let oldcxt: MemoryContext;
    let size: usize;
    let newoption: *mut relopt_gen;

    if kinds != RELOPT_KIND_LOCAL as bits32 {
        oldcxt = MemoryContextSwitchTo(TopMemoryContext);
    } else {
        oldcxt = ptr::null_mut();
    }

    match r#type {
        RELOPT_TYPE_BOOL => {
            size = size_of::<relopt_bool>();
        }
        RELOPT_TYPE_INT => {
            size = size_of::<relopt_int>();
        }
        RELOPT_TYPE_REAL => {
            size = size_of::<relopt_real>();
        }
        RELOPT_TYPE_ENUM => {
            size = size_of::<relopt_enum>();
        }
        RELOPT_TYPE_STRING => {
            size = size_of::<relopt_string>();
        }
        _ => {
            elog!(ERROR, "unsupported reloption type {}", r#type);
            return ptr::null_mut(); /* keep compiler quiet */
        }
    }

    newoption = palloc(size) as *mut relopt_gen;

    (*newoption).name = pstrdup(name);
    if !desc.is_null() {
        (*newoption).desc = pstrdup(desc);
    } else {
        (*newoption).desc = ptr::null();
    }
    (*newoption).kinds = kinds;
    (*newoption).namelen = strlen(name) as c_int;
    (*newoption).r#type = r#type;
    (*newoption).lockmode = lockmode;

    if !oldcxt.is_null() {
        MemoryContextSwitchTo(oldcxt);
    }

    newoption
}

/*
 * init_bool_reloption
 *		Allocate and initialize a new boolean reloption
 */
unsafe fn init_bool_reloption(
    kinds: bits32,
    name: *const c_char,
    desc: *const c_char,
    default_val: bool,
    lockmode: LOCKMODE,
) -> *mut relopt_bool {
    let newoption: *mut relopt_bool;

    newoption = allocate_reloption(kinds, RELOPT_TYPE_BOOL, name, desc, lockmode) as *mut relopt_bool;
    (*newoption).default_val = default_val;

    newoption
}

/*
 * add_bool_reloption
 *		Add a new boolean reloption
 */
pub unsafe fn add_bool_reloption(
    kinds: bits32,
    name: *const c_char,
    desc: *const c_char,
    default_val: bool,
    lockmode: LOCKMODE,
) {
    let newoption: *mut relopt_bool = init_bool_reloption(kinds, name, desc, default_val, lockmode);

    add_reloption(newoption as *mut relopt_gen);
}

/*
 * add_local_bool_reloption
 *		Add a new boolean local reloption
 *
 * 'offset' is offset of bool-typed field.
 */
pub unsafe fn add_local_bool_reloption(
    relopts: *mut local_relopts,
    name: *const c_char,
    desc: *const c_char,
    default_val: bool,
    offset: c_int,
) {
    let newoption: *mut relopt_bool =
        init_bool_reloption(RELOPT_KIND_LOCAL as bits32, name, desc, default_val, 0);

    add_local_reloption(relopts, newoption as *mut relopt_gen, offset);
}

/*
 * init_real_reloption
 *		Allocate and initialize a new integer reloption
 */
unsafe fn init_int_reloption(
    kinds: bits32,
    name: *const c_char,
    desc: *const c_char,
    default_val: c_int,
    min_val: c_int,
    max_val: c_int,
    lockmode: LOCKMODE,
) -> *mut relopt_int {
    let newoption: *mut relopt_int;

    newoption = allocate_reloption(kinds, RELOPT_TYPE_INT, name, desc, lockmode) as *mut relopt_int;
    (*newoption).default_val = default_val;
    (*newoption).min = min_val;
    (*newoption).max = max_val;

    newoption
}

/*
 * add_int_reloption
 *		Add a new integer reloption
 */
pub unsafe fn add_int_reloption(
    kinds: bits32,
    name: *const c_char,
    desc: *const c_char,
    default_val: c_int,
    min_val: c_int,
    max_val: c_int,
    lockmode: LOCKMODE,
) {
    let newoption: *mut relopt_int =
        init_int_reloption(kinds, name, desc, default_val, min_val, max_val, lockmode);

    add_reloption(newoption as *mut relopt_gen);
}

/*
 * add_local_int_reloption
 *		Add a new local integer reloption
 *
 * 'offset' is offset of int-typed field.
 */
pub unsafe fn add_local_int_reloption(
    relopts: *mut local_relopts,
    name: *const c_char,
    desc: *const c_char,
    default_val: c_int,
    min_val: c_int,
    max_val: c_int,
    offset: c_int,
) {
    let newoption: *mut relopt_int = init_int_reloption(
        RELOPT_KIND_LOCAL as bits32,
        name,
        desc,
        default_val,
        min_val,
        max_val,
        0,
    );

    add_local_reloption(relopts, newoption as *mut relopt_gen, offset);
}

/*
 * init_real_reloption
 *		Allocate and initialize a new real reloption
 */
unsafe fn init_real_reloption(
    kinds: bits32,
    name: *const c_char,
    desc: *const c_char,
    default_val: f64,
    min_val: f64,
    max_val: f64,
    lockmode: LOCKMODE,
) -> *mut relopt_real {
    let newoption: *mut relopt_real;

    newoption =
        allocate_reloption(kinds, RELOPT_TYPE_REAL, name, desc, lockmode) as *mut relopt_real;
    (*newoption).default_val = default_val;
    (*newoption).min = min_val;
    (*newoption).max = max_val;

    newoption
}

/*
 * add_real_reloption
 *		Add a new float reloption
 */
pub unsafe fn add_real_reloption(
    kinds: bits32,
    name: *const c_char,
    desc: *const c_char,
    default_val: f64,
    min_val: f64,
    max_val: f64,
    lockmode: LOCKMODE,
) {
    let newoption: *mut relopt_real =
        init_real_reloption(kinds, name, desc, default_val, min_val, max_val, lockmode);

    add_reloption(newoption as *mut relopt_gen);
}

/*
 * add_local_real_reloption
 *		Add a new local float reloption
 *
 * 'offset' is offset of double-typed field.
 */
pub unsafe fn add_local_real_reloption(
    relopts: *mut local_relopts,
    name: *const c_char,
    desc: *const c_char,
    default_val: f64,
    min_val: f64,
    max_val: f64,
    offset: c_int,
) {
    let newoption: *mut relopt_real = init_real_reloption(
        RELOPT_KIND_LOCAL as bits32,
        name,
        desc,
        default_val,
        min_val,
        max_val,
        0,
    );

    add_local_reloption(relopts, newoption as *mut relopt_gen, offset);
}

/*
 * init_enum_reloption
 *		Allocate and initialize a new enum reloption
 */
unsafe fn init_enum_reloption(
    kinds: bits32,
    name: *const c_char,
    desc: *const c_char,
    members: *mut relopt_enum_elt_def,
    default_val: c_int,
    detailmsg: *const c_char,
    lockmode: LOCKMODE,
) -> *mut relopt_enum {
    let newoption: *mut relopt_enum;

    newoption =
        allocate_reloption(kinds, RELOPT_TYPE_ENUM, name, desc, lockmode) as *mut relopt_enum;
    (*newoption).members = members;
    (*newoption).default_val = default_val;
    (*newoption).detailmsg = detailmsg;

    newoption
}

/*
 * add_enum_reloption
 *		Add a new enum reloption
 *
 * The members array must have a terminating NULL entry.
 *
 * The detailmsg is shown when unsupported values are passed, and has this
 * form:   "Valid values are \"foo\", \"bar\", and \"bar\"."
 *
 * The members array and detailmsg are not copied -- caller must ensure that
 * they are valid throughout the life of the process.
 */
pub unsafe fn add_enum_reloption(
    kinds: bits32,
    name: *const c_char,
    desc: *const c_char,
    members: *mut relopt_enum_elt_def,
    default_val: c_int,
    detailmsg: *const c_char,
    lockmode: LOCKMODE,
) {
    let newoption: *mut relopt_enum =
        init_enum_reloption(kinds, name, desc, members, default_val, detailmsg, lockmode);

    add_reloption(newoption as *mut relopt_gen);
}

/*
 * add_local_enum_reloption
 *		Add a new local enum reloption
 *
 * 'offset' is offset of int-typed field.
 */
pub unsafe fn add_local_enum_reloption(
    relopts: *mut local_relopts,
    name: *const c_char,
    desc: *const c_char,
    members: *mut relopt_enum_elt_def,
    default_val: c_int,
    detailmsg: *const c_char,
    offset: c_int,
) {
    let newoption: *mut relopt_enum = init_enum_reloption(
        RELOPT_KIND_LOCAL as bits32,
        name,
        desc,
        members,
        default_val,
        detailmsg,
        0,
    );

    add_local_reloption(relopts, newoption as *mut relopt_gen, offset);
}

/*
 * init_string_reloption
 *		Allocate and initialize a new string reloption
 */
unsafe fn init_string_reloption(
    kinds: bits32,
    name: *const c_char,
    desc: *const c_char,
    default_val: *const c_char,
    validator: validate_string_relopt,
    filler: fill_string_relopt,
    lockmode: LOCKMODE,
) -> *mut relopt_string {
    let newoption: *mut relopt_string;

    /* make sure the validator/default combination is sane */
    if let Some(validate) = validator {
        validate(default_val);
    }

    newoption =
        allocate_reloption(kinds, RELOPT_TYPE_STRING, name, desc, lockmode) as *mut relopt_string;
    (*newoption).validate_cb = validator;
    (*newoption).fill_cb = filler;
    if !default_val.is_null() {
        if kinds == RELOPT_KIND_LOCAL as bits32 {
            (*newoption).default_val = strdup(default_val);
        } else {
            (*newoption).default_val = MemoryContextStrdup(TopMemoryContext, default_val);
        }
        (*newoption).default_len = strlen(default_val) as c_int;
        (*newoption).default_isnull = false;
    } else {
        (*newoption).default_val = c"".as_ptr() as *mut c_char;
        (*newoption).default_len = 0;
        (*newoption).default_isnull = true;
    }

    newoption
}

/*
 * add_string_reloption
 *		Add a new string reloption
 *
 * "validator" is an optional function pointer that can be used to test the
 * validity of the values.  It must elog(ERROR) when the argument string is
 * not acceptable for the variable.  Note that the default value must pass
 * the validation.
 */
pub unsafe fn add_string_reloption(
    kinds: bits32,
    name: *const c_char,
    desc: *const c_char,
    default_val: *const c_char,
    validator: validate_string_relopt,
    lockmode: LOCKMODE,
) {
    let newoption: *mut relopt_string =
        init_string_reloption(kinds, name, desc, default_val, validator, None, lockmode);

    add_reloption(newoption as *mut relopt_gen);
}

/*
 * add_local_string_reloption
 *		Add a new local string reloption
 *
 * 'offset' is offset of int-typed field that will store offset of string value
 * in the resulting bytea structure.
 */
pub unsafe fn add_local_string_reloption(
    relopts: *mut local_relopts,
    name: *const c_char,
    desc: *const c_char,
    default_val: *const c_char,
    validator: validate_string_relopt,
    filler: fill_string_relopt,
    offset: c_int,
) {
    let newoption: *mut relopt_string = init_string_reloption(
        RELOPT_KIND_LOCAL as bits32,
        name,
        desc,
        default_val,
        validator,
        filler,
        0,
    );

    add_local_reloption(relopts, newoption as *mut relopt_gen, offset);
}

// libc strdup (used by init_string_reloption for RELOPT_KIND_LOCAL)
extern "C" {
    fn strdup(s: *const c_char) -> *mut c_char;
}

/*
 * Transform a relation options list (list of DefElem) into the text array
 * format that is kept in pg_class.reloptions, including only those options
 * that are in the passed namespace.  The output values do not include the
 * namespace.
 *
 * This is used for three cases: CREATE TABLE/INDEX, ALTER TABLE SET, and
 * ALTER TABLE RESET.  In the ALTER cases, oldOptions is the existing
 * reloptions value (possibly NULL), and we replace or remove entries
 * as needed.
 *
 * If acceptOidsOff is true, then we allow oids = false, but throw error when
 * on. This is solely needed for backwards compatibility.
 *
 * Note that this is not responsible for determining whether the options
 * are valid, but it does check that namespaces for all the options given are
 * listed in validnsps.  The NULL namespace is always valid and need not be
 * explicitly listed.  Passing a NULL pointer means that only the NULL
 * namespace is valid.
 *
 * Both oldOptions and the result are text arrays (or NULL for "default"),
 * but we declare them as Datums to avoid including array.h in reloptions.h.
 */
pub unsafe fn transformRelOptions(
    oldOptions: Datum,
    defList: *mut List,
    namspace: *const c_char,
    validnsps: *const *const c_char,
    acceptOidsOff: bool,
    isReset: bool,
) -> Datum {
    let result: Datum;
    let mut astate: *mut ArrayBuildState;
    let mut cell: *mut ListCell;

    /* no change if empty list */
    if defList == NIL {
        return oldOptions;
    }

    /* We build new array using accumArrayResult */
    astate = ptr::null_mut();

    /* Copy any oldOptions that aren't to be replaced */
    if PointerIsValid(DatumGetPointer(oldOptions)) {
        let array: *mut ArrayType = DatumGetArrayTypeP(oldOptions);
        let mut oldoptions: *mut Datum = ptr::null_mut();
        let mut noldoptions: c_int = 0;
        let mut i: c_int;

        deconstruct_array_builtin(array, TEXTOID, &mut oldoptions, ptr::null_mut(), &mut noldoptions);

        i = 0;
        while i < noldoptions {
            let opt_i = *oldoptions.offset(i as isize);
            let text_str: *mut c_char = VARDATA(DatumGetPointer(opt_i));
            let text_len: c_int = VARSIZE(DatumGetPointer(opt_i)) as c_int - VARHDRSZ;

            /* Search for a match in defList */
            // C: foreach(cell, defList) ... break on match; cell == NULL if none.
            // foreach! cannot host `continue`/`break`, so we drive the iterator by
            // hand and use a 'cont block for C's `continue`.
            cell = NIL as *mut ListCell;
            {
                let mut __i: c_int = 0;
                'foreach: while __i < list_length(defList) {
                    cell = (*defList).elements.offset(__i as isize);
                    let def: *mut DefElem = *(cell as *mut *mut DefElem);
                    let kw_len: c_int;

                    'cont: {
                        /* ignore if not in the same namespace */
                        if namspace.is_null() {
                            if !(*def).defnamespace.is_null() {
                                break 'cont;
                            }
                        } else if (*def).defnamespace.is_null() {
                            break 'cont;
                        } else if strcmp((*def).defnamespace, namspace) != 0 {
                            break 'cont;
                        }

                        kw_len = strlen((*def).defname) as c_int;
                        if text_len > kw_len
                            && *text_str.offset(kw_len as isize) == b'=' as c_char
                            && strncmp(text_str, (*def).defname, kw_len as usize) == 0
                        {
                            break 'foreach; // C: break out of the foreach -> cell stays set
                        }
                    }
                    cell = NIL as *mut ListCell;
                    __i += 1;
                }
            }
            if cell.is_null() {
                /* No match, so keep old option */
                astate = accumArrayResult(
                    astate,
                    *oldoptions.offset(i as isize),
                    false,
                    TEXTOID,
                    CurrentMemoryContext,
                );
            }
            i += 1;
        }
    }

    /*
     * If CREATE/SET, add new options to array; if RESET, just check that the
     * user didn't say RESET (option=val).  (Must do this because the grammar
     * doesn't enforce it.)
     */
    // C: foreach(cell, defList) { ... }.  The body uses `continue`, which
    // foreach! cannot host, so each iteration runs inside a 'cont block where
    // C's `continue` becomes `break 'cont`.
    foreach!(lc, defList, {
      'cont: {
        let def: *mut DefElem = *(crate::current_cell!(lc) as *mut *mut DefElem);

        if isReset {
            if !(*def).arg.is_null() {
                ereport!(
                    ERROR,
                    errmsg!("RESET must not include values for parameters")
                );
                // C also: errcode(ERRCODE_SYNTAX_ERROR)
            }
        } else {
            let name: *const c_char;
            let value: *const c_char;
            let t: *mut text;
            let len: Size;

            /*
             * Error out if the namespace is not valid.  A NULL namespace is
             * always valid.
             */
            if !(*def).defnamespace.is_null() {
                let mut valid: bool = false;
                let mut i: c_int;

                if !validnsps.is_null() {
                    i = 0;
                    while !(*validnsps.offset(i as isize)).is_null() {
                        if strcmp((*def).defnamespace, *validnsps.offset(i as isize)) == 0 {
                            valid = true;
                            break;
                        }
                        i += 1;
                    }
                }

                if !valid {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "unrecognized parameter namespace \"{}\"",
                            CStr::from_ptr((*def).defnamespace).to_string_lossy()
                        )
                    );
                    // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                }
            }

            /* ignore if not in the same namespace */
            if namspace.is_null() {
                if !(*def).defnamespace.is_null() {
                    break 'cont;
                }
            } else if (*def).defnamespace.is_null() {
                break 'cont;
            } else if strcmp((*def).defnamespace, namspace) != 0 {
                break 'cont;
            }

            /*
             * Flatten the DefElem into a text string like "name=arg". If we
             * have just "name", assume "name=true" is meant.  Note: the
             * namespace is not output.
             */
            name = (*def).defname;
            if !(*def).arg.is_null() {
                value = defGetString(def);
            } else {
                value = c"true".as_ptr();
            }

            /* Insist that name not contain "=", else "a=b=c" is ambiguous */
            if !strchr(name, b'=' as c_int).is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "invalid option name \"{}\": must not contain \"=\"",
                        CStr::from_ptr(name).to_string_lossy()
                    )
                );
                // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            }

            /*
             * This is not a great place for this test, but there's no other
             * convenient place to filter the option out. As WITH (oids =
             * false) will be removed someday, this seems like an acceptable
             * amount of ugly.
             */
            if acceptOidsOff && (*def).defnamespace.is_null() && strcmp(name, c"oids".as_ptr()) == 0
            {
                if defGetBoolean(def) {
                    ereport!(
                        ERROR,
                        errmsg!("tables declared WITH OIDS are not supported")
                    );
                    // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                }
                /* skip over option, reloptions machinery doesn't know it */
                break 'cont;
            }

            len = VARHDRSZ as Size + strlen(name) + 1 + strlen(value);
            /* +1 leaves room for sprintf's trailing null */
            t = palloc(len + 1) as *mut text;
            SET_VARSIZE(t as *mut c_char, len as int32);
            sprintf(
                VARDATA(t as *const c_char),
                c"%s=%s".as_ptr(),
                name,
                value,
            );

            astate = accumArrayResult(
                astate,
                PointerGetDatum(t as *const c_void),
                false,
                TEXTOID,
                CurrentMemoryContext,
            );
        }
      }
    });

    if !astate.is_null() {
        result = makeArrayResult(astate, CurrentMemoryContext);
    } else {
        result = 0 as Datum;
    }

    result
}

// libc sprintf (variadic) used by transformRelOptions.
extern "C" {
    fn sprintf(s: *mut c_char, fmt: *const c_char, ...) -> c_int;
}

/*
 * Convert the text-array format of reloptions into a List of DefElem.
 * This is the inverse of transformRelOptions().
 */
pub unsafe fn untransformRelOptions(options: Datum) -> *mut List {
    let mut result: *mut List = NIL;
    let array: *mut ArrayType;
    let mut optiondatums: *mut Datum = ptr::null_mut();
    let mut noptions: c_int = 0;
    let mut i: c_int;

    /* Nothing to do if no options */
    if !PointerIsValid(DatumGetPointer(options)) {
        return result;
    }

    array = DatumGetArrayTypeP(options);

    deconstruct_array_builtin(array, TEXTOID, &mut optiondatums, ptr::null_mut(), &mut noptions);

    i = 0;
    while i < noptions {
        let s: *mut c_char;
        let mut p: *mut c_char;
        let mut val: *mut Node = ptr::null_mut();

        s = TextDatumGetCString(*optiondatums.offset(i as isize));
        p = strchr(s, b'=' as c_int);
        if !p.is_null() {
            *p = b'\0' as c_char;
            p = p.add(1);
            val = makeString(p) as *mut Node;
        }
        result = lappend(result, makeDefElem(s, val, -1) as *mut c_void);
        i += 1;
    }

    result
}

/*
 * Extract and parse reloptions from a pg_class tuple.
 *
 * This is a low-level routine, expected to be used by relcache code and
 * callers that do not have a table's relcache entry (e.g. autovacuum).  For
 * other uses, consider grabbing the rd_options pointer from the relcache entry
 * instead.
 *
 * tupdesc is pg_class' tuple descriptor.  amoptions is a pointer to the index
 * AM's options parser function in the case of a tuple corresponding to an
 * index, or NULL otherwise.
 */
pub unsafe fn extractRelOptions(
    tuple: HeapTuple,
    tupdesc: TupleDesc,
    amoptions: amoptions_function,
) -> *mut bytea {
    let options: *mut bytea;
    let mut isnull: bool = false;
    let datum: Datum;
    let classForm: Form_pg_class;

    datum = fastgetattr(tuple, Anum_pg_class_reloptions as c_int, tupdesc, &mut isnull);
    if isnull {
        return ptr::null_mut();
    }

    classForm = GETSTRUCT(tuple) as Form_pg_class;

    /* Parse into appropriate format; don't error out here */
    let relkind = (*classForm).relkind;
    if relkind == RELKIND_RELATION || relkind == RELKIND_TOASTVALUE || relkind == RELKIND_MATVIEW {
        options = heap_reloptions(relkind, datum, false);
    } else if relkind == RELKIND_PARTITIONED_TABLE {
        options = partitioned_table_reloptions(datum, false);
    } else if relkind == RELKIND_VIEW {
        options = view_reloptions(datum, false);
    } else if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
        options = index_reloptions(amoptions, datum, false);
    } else if relkind == RELKIND_FOREIGN_TABLE {
        options = ptr::null_mut();
    } else {
        Assert!(false); /* can't get here */
        options = ptr::null_mut(); /* keep compiler quiet */
    }

    options
}

unsafe fn parseRelOptionsInternal(
    options: Datum,
    validate: bool,
    reloptions: *mut relopt_value,
    numoptions: c_int,
) {
    let array: *mut ArrayType = DatumGetArrayTypeP(options);
    let mut optiondatums: *mut Datum = ptr::null_mut();
    let mut noptions: c_int = 0;
    let mut i: c_int;

    deconstruct_array_builtin(array, TEXTOID, &mut optiondatums, ptr::null_mut(), &mut noptions);

    i = 0;
    while i < noptions {
        let opt_i = *optiondatums.offset(i as isize);
        let text_str: *mut c_char = VARDATA(DatumGetPointer(opt_i));
        let text_len: c_int = VARSIZE(DatumGetPointer(opt_i)) as c_int - VARHDRSZ;
        let mut j: c_int;

        /* Search for a match in reloptions */
        j = 0;
        while j < numoptions {
            let kw_len: c_int = (*(*reloptions.offset(j as isize)).gen).namelen;

            if text_len > kw_len
                && *text_str.offset(kw_len as isize) == b'=' as c_char
                && strncmp(text_str, (*(*reloptions.offset(j as isize)).gen).name, kw_len as usize)
                    == 0
            {
                parse_one_reloption(reloptions.offset(j as isize), text_str, text_len, validate);
                break;
            }
            j += 1;
        }

        if j >= numoptions && validate {
            let s: *mut c_char;
            let p: *mut c_char;

            s = TextDatumGetCString(*optiondatums.offset(i as isize));
            p = strchr(s, b'=' as c_int);
            if !p.is_null() {
                *p = b'\0' as c_char;
            }
            ereport!(
                ERROR,
                errmsg!(
                    "unrecognized parameter \"{}\"",
                    CStr::from_ptr(s).to_string_lossy()
                )
            );
            // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        }
        i += 1;
    }

    /* It's worth avoiding memory leaks in this function */
    pfree(optiondatums as *mut c_void);

    if (array as *mut c_void) != DatumGetPointer(options) as *mut c_void {
        pfree(array as *mut c_void);
    }
}

/*
 * Interpret reloptions that are given in text-array format.
 *
 * (See the long comment in the C source for the contract.)
 */
unsafe fn parseRelOptions(
    options: Datum,
    validate: bool,
    kind: relopt_kind,
    numrelopts: *mut c_int,
) -> *mut relopt_value {
    let mut reloptions: *mut relopt_value = ptr::null_mut();
    let mut numoptions: c_int = 0;
    let mut i: c_int;
    let mut j: c_int;

    if need_initialization {
        initialize_reloptions();
    }

    /* Build a list of expected options, based on kind */

    i = 0;
    while !(*relOpts.offset(i as isize)).is_null() {
        if (**relOpts.offset(i as isize)).kinds & kind as bits32 != 0 {
            numoptions += 1;
        }
        i += 1;
    }

    if numoptions > 0 {
        reloptions = palloc(numoptions as Size * size_of::<relopt_value>()) as *mut relopt_value;

        i = 0;
        j = 0;
        while !(*relOpts.offset(i as isize)).is_null() {
            if (**relOpts.offset(i as isize)).kinds & kind as bits32 != 0 {
                (*reloptions.offset(j as isize)).gen = *relOpts.offset(i as isize);
                (*reloptions.offset(j as isize)).isset = false;
                j += 1;
            }
            i += 1;
        }
    }

    /* Done if no options */
    if PointerIsValid(DatumGetPointer(options)) {
        parseRelOptionsInternal(options, validate, reloptions, numoptions);
    }

    *numrelopts = numoptions;
    reloptions
}

/* Parse local unregistered options. */
unsafe fn parseLocalRelOptions(
    relopts: *mut local_relopts,
    options: Datum,
    validate: bool,
) -> *mut relopt_value {
    let nopts: c_int = list_length((*relopts).options);
    let values: *mut relopt_value =
        palloc(size_of::<relopt_value>() * nopts as usize) as *mut relopt_value;
    let mut i: c_int = 0;

    foreach!(lc, (*relopts).options, {
        let opt: *mut local_relopt = *(crate::current_cell!(lc) as *mut *mut local_relopt);

        (*values.offset(i as isize)).gen = (*opt).option;
        (*values.offset(i as isize)).isset = false;

        i += 1;
    });

    if options != 0 as Datum {
        parseRelOptionsInternal(options, validate, values, nopts);
    }

    values
}

/*
 * Subroutine for parseRelOptions, to parse and validate a single option's
 * value
 */
unsafe fn parse_one_reloption(
    option: *mut relopt_value,
    text_str: *mut c_char,
    text_len: c_int,
    validate: bool,
) {
    let value: *mut c_char;
    let value_len: c_int;
    let mut parsed: bool;
    let mut nofree: bool = false;

    if (*option).isset && validate {
        ereport!(
            ERROR,
            errmsg!(
                "parameter \"{}\" specified more than once",
                CStr::from_ptr((*(*option).gen).name).to_string_lossy()
            )
        );
        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
    }

    value_len = text_len - (*(*option).gen).namelen - 1;
    value = palloc(value_len as Size + 1) as *mut c_char;
    memcpy(
        value as *mut c_void,
        text_str.offset((*(*option).gen).namelen as isize + 1) as *const c_void,
        value_len as usize,
    );
    *value.offset(value_len as isize) = b'\0' as c_char;

    match (*(*option).gen).r#type {
        RELOPT_TYPE_BOOL => {
            parsed = parse_bool(value, &mut (*option).values.bool_val);
            if validate && !parsed {
                ereport!(
                    ERROR,
                    errmsg!(
                        "invalid value for boolean option \"{}\": {}",
                        CStr::from_ptr((*(*option).gen).name).to_string_lossy(),
                        CStr::from_ptr(value).to_string_lossy()
                    )
                );
                // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            }
        }
        RELOPT_TYPE_INT => {
            let optint: *mut relopt_int = (*option).gen as *mut relopt_int;

            parsed = parse_int(value, &mut (*option).values.int_val, 0, ptr::null_mut());
            if validate && !parsed {
                ereport!(
                    ERROR,
                    errmsg!(
                        "invalid value for integer option \"{}\": {}",
                        CStr::from_ptr((*(*option).gen).name).to_string_lossy(),
                        CStr::from_ptr(value).to_string_lossy()
                    )
                );
                // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            }
            if validate
                && ((*option).values.int_val < (*optint).min
                    || (*option).values.int_val > (*optint).max)
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "value {} out of bounds for option \"{}\"",
                        CStr::from_ptr(value).to_string_lossy(),
                        CStr::from_ptr((*(*option).gen).name).to_string_lossy()
                    )
                );
                // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                //   errdetail("Valid values are between \"%d\" and \"%d\".",
                //             optint->min, optint->max)
            }
        }
        RELOPT_TYPE_REAL => {
            let optreal: *mut relopt_real = (*option).gen as *mut relopt_real;

            parsed = parse_real(value, &mut (*option).values.real_val, 0, ptr::null_mut());
            if validate && !parsed {
                ereport!(
                    ERROR,
                    errmsg!(
                        "invalid value for floating point option \"{}\": {}",
                        CStr::from_ptr((*(*option).gen).name).to_string_lossy(),
                        CStr::from_ptr(value).to_string_lossy()
                    )
                );
                // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            }
            if validate
                && ((*option).values.real_val < (*optreal).min
                    || (*option).values.real_val > (*optreal).max)
            {
                ereport!(
                    ERROR,
                    errmsg!(
                        "value {} out of bounds for option \"{}\"",
                        CStr::from_ptr(value).to_string_lossy(),
                        CStr::from_ptr((*(*option).gen).name).to_string_lossy()
                    )
                );
                // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                //   errdetail("Valid values are between \"%f\" and \"%f\".",
                //             optreal->min, optreal->max)
            }
        }
        RELOPT_TYPE_ENUM => {
            let optenum: *mut relopt_enum = (*option).gen as *mut relopt_enum;
            let mut elt: *mut relopt_enum_elt_def;

            parsed = false;
            elt = (*optenum).members;
            while !(*elt).string_val.is_null() {
                if pg_strcasecmp(value, (*elt).string_val) == 0 {
                    (*option).values.enum_val = (*elt).symbol_val;
                    parsed = true;
                    break;
                }
                elt = elt.add(1);
            }
            if validate && !parsed {
                ereport!(
                    ERROR,
                    errmsg!(
                        "invalid value for enum option \"{}\": {}",
                        CStr::from_ptr((*(*option).gen).name).to_string_lossy(),
                        CStr::from_ptr(value).to_string_lossy()
                    )
                );
                // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                //   optenum->detailmsg ? errdetail_internal("%s", _(optenum->detailmsg)) : 0
            }

            /*
             * If value is not among the allowed string values, but we are
             * not asked to validate, just use the default numeric value.
             */
            if !parsed {
                (*option).values.enum_val = (*optenum).default_val;
            }
        }
        RELOPT_TYPE_STRING => {
            let optstring: *mut relopt_string = (*option).gen as *mut relopt_string;

            (*option).values.string_val = value;
            nofree = true;
            if validate {
                if let Some(validate_cb) = (*optstring).validate_cb {
                    validate_cb(value);
                }
            }
            parsed = true;
        }
        _ => {
            elog!(ERROR, "unsupported reloption type {}", (*(*option).gen).r#type);
            parsed = true; /* quiet compiler */
        }
    }

    if parsed {
        (*option).isset = true;
    }
    if !nofree {
        pfree(value as *mut c_void);
    }
}

// offsetof: nested-field offset_of for the parse tables (C composes two
// offsetof() calls; Rust's offset_of! takes a dotted field path).
macro_rules! offsetof {
    ($t:ty, $($field:tt)+) => {
        core::mem::offset_of!($t, $($field)+) as c_int
    };
}

// pe! / pe_set!: relopt_parse_elt initializers (isset_offset defaults to 0).
macro_rules! pe {
    ($name:literal, $opttype:expr, $offset:expr) => {
        relopt_parse_elt {
            optname: concat!($name, "\0").as_ptr() as *const c_char,
            opttype: $opttype,
            offset: $offset,
            isset_offset: 0,
        }
    };
}
macro_rules! pe_set {
    ($name:literal, $opttype:expr, $offset:expr, $isset:expr) => {
        relopt_parse_elt {
            optname: concat!($name, "\0").as_ptr() as *const c_char,
            opttype: $opttype,
            offset: $offset,
            isset_offset: $isset,
        }
    };
}

/*
 * Given the result from parseRelOptions, allocate a struct that's of the
 * specified base size plus any extra space that's needed for string variables.
 *
 * "base" should be sizeof(struct) of the reloptions struct (StdRdOptions or
 * equivalent).
 */
unsafe fn allocateReloptStruct(
    base: Size,
    options: *mut relopt_value,
    numoptions: c_int,
) -> *mut c_void {
    let mut size: Size = base;
    let mut i: c_int;

    i = 0;
    while i < numoptions {
        let optval: *mut relopt_value = options.offset(i as isize);

        if (*(*optval).gen).r#type == RELOPT_TYPE_STRING {
            let optstr: *mut relopt_string = (*optval).gen as *mut relopt_string;

            if let Some(fill_cb) = (*optstr).fill_cb {
                let val: *const c_char = if (*optval).isset {
                    (*optval).values.string_val
                } else if (*optstr).default_isnull {
                    ptr::null()
                } else {
                    (*optstr).default_val
                };

                size += fill_cb(val, ptr::null_mut());
            } else {
                size += GET_STRING_RELOPTION_LEN(&*optval) + 1;
            }
        }
        i += 1;
    }

    palloc0(size)
}

/*
 * Given the result of parseRelOptions and a parsing table, fill in the
 * struct (previously allocated with allocateReloptStruct) with the parsed
 * values.
 *
 * (See the long comment in the C source.)
 */
unsafe fn fillRelOptions(
    rdopts: *mut c_void,
    basesize: Size,
    options: *mut relopt_value,
    numoptions: c_int,
    validate: bool,
    elems: *const relopt_parse_elt,
    numelems: c_int,
) {
    let mut i: c_int;
    let mut offset: c_int = basesize as c_int;

    i = 0;
    while i < numoptions {
        let mut j: c_int;
        let mut found: bool = false;

        j = 0;
        while j < numelems {
            if strcmp(
                (*(*options.offset(i as isize)).gen).name,
                (*elems.offset(j as isize)).optname,
            ) == 0
            {
                let optstring: *mut relopt_string;
                let itempos: *mut c_char =
                    (rdopts as *mut c_char).offset((*elems.offset(j as isize)).offset as isize);
                let string_val: *mut c_char;

                /*
                 * If isset_offset is provided, store whether the reloption is
                 * set there.
                 */
                if (*elems.offset(j as isize)).isset_offset > 0 {
                    let setpos: *mut c_char = (rdopts as *mut c_char)
                        .offset((*elems.offset(j as isize)).isset_offset as isize);

                    *(setpos as *mut bool) = (*options.offset(i as isize)).isset;
                }

                match (*(*options.offset(i as isize)).gen).r#type {
                    RELOPT_TYPE_BOOL => {
                        *(itempos as *mut bool) = if (*options.offset(i as isize)).isset {
                            (*options.offset(i as isize)).values.bool_val
                        } else {
                            (*((*options.offset(i as isize)).gen as *mut relopt_bool)).default_val
                        };
                    }
                    RELOPT_TYPE_INT => {
                        *(itempos as *mut c_int) = if (*options.offset(i as isize)).isset {
                            (*options.offset(i as isize)).values.int_val
                        } else {
                            (*((*options.offset(i as isize)).gen as *mut relopt_int)).default_val
                        };
                    }
                    RELOPT_TYPE_REAL => {
                        *(itempos as *mut f64) = if (*options.offset(i as isize)).isset {
                            (*options.offset(i as isize)).values.real_val
                        } else {
                            (*((*options.offset(i as isize)).gen as *mut relopt_real)).default_val
                        };
                    }
                    RELOPT_TYPE_ENUM => {
                        *(itempos as *mut c_int) = if (*options.offset(i as isize)).isset {
                            (*options.offset(i as isize)).values.enum_val
                        } else {
                            (*((*options.offset(i as isize)).gen as *mut relopt_enum)).default_val
                        };
                    }
                    RELOPT_TYPE_STRING => {
                        optstring = (*options.offset(i as isize)).gen as *mut relopt_string;
                        if (*options.offset(i as isize)).isset {
                            string_val = (*options.offset(i as isize)).values.string_val;
                        } else if !(*optstring).default_isnull {
                            string_val = (*optstring).default_val;
                        } else {
                            string_val = ptr::null_mut();
                        }

                        if let Some(fill_cb) = (*optstring).fill_cb {
                            let size: Size = fill_cb(
                                string_val,
                                (rdopts as *mut c_char).offset(offset as isize) as *mut c_void,
                            );

                            if size != 0 {
                                *(itempos as *mut c_int) = offset;
                                offset += size as c_int;
                            } else {
                                *(itempos as *mut c_int) = 0;
                            }
                        } else if string_val.is_null() {
                            *(itempos as *mut c_int) = 0;
                        } else {
                            strcpy(
                                (rdopts as *mut c_char).offset(offset as isize),
                                string_val,
                            );
                            *(itempos as *mut c_int) = offset;
                            offset += strlen(string_val) as c_int + 1;
                        }
                    }
                    _ => {
                        elog!(
                            ERROR,
                            "unsupported reloption type {}",
                            (*(*options.offset(i as isize)).gen).r#type
                        );
                    }
                }
                found = true;
                break;
            }
            j += 1;
        }
        if validate && !found {
            elog!(
                ERROR,
                "reloption \"{}\" not found in parse table",
                CStr::from_ptr((*(*options.offset(i as isize)).gen).name).to_string_lossy()
            );
        }
        i += 1;
    }
    SET_VARSIZE(rdopts as *mut c_char, offset);
}

/*
 * Option parser for anything that uses StdRdOptions.
 */
pub unsafe fn default_reloptions(reloptions: Datum, validate: bool, kind: relopt_kind) -> *mut bytea {
    static tab: [relopt_parse_elt; 24] = [
        pe!("fillfactor", RELOPT_TYPE_INT, offsetof!(StdRdOptions, fillfactor)),
        pe!("autovacuum_enabled", RELOPT_TYPE_BOOL,
            offsetof!(StdRdOptions, autovacuum.enabled)),
        pe!("autovacuum_vacuum_threshold", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.vacuum_threshold)),
        pe!("autovacuum_vacuum_max_threshold", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.vacuum_max_threshold)),
        pe!("autovacuum_vacuum_insert_threshold", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.vacuum_ins_threshold)),
        pe!("autovacuum_analyze_threshold", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.analyze_threshold)),
        pe!("autovacuum_vacuum_cost_limit", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.vacuum_cost_limit)),
        pe!("autovacuum_freeze_min_age", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.freeze_min_age)),
        pe!("autovacuum_freeze_max_age", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.freeze_max_age)),
        pe!("autovacuum_freeze_table_age", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.freeze_table_age)),
        pe!("autovacuum_multixact_freeze_min_age", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.multixact_freeze_min_age)),
        pe!("autovacuum_multixact_freeze_max_age", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.multixact_freeze_max_age)),
        pe!("autovacuum_multixact_freeze_table_age", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.multixact_freeze_table_age)),
        pe!("log_autovacuum_min_duration", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, autovacuum.log_min_duration)),
        pe!("toast_tuple_target", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, toast_tuple_target)),
        pe!("autovacuum_vacuum_cost_delay", RELOPT_TYPE_REAL,
            offsetof!(StdRdOptions, autovacuum.vacuum_cost_delay)),
        pe!("autovacuum_vacuum_scale_factor", RELOPT_TYPE_REAL,
            offsetof!(StdRdOptions, autovacuum.vacuum_scale_factor)),
        pe!("autovacuum_vacuum_insert_scale_factor", RELOPT_TYPE_REAL,
            offsetof!(StdRdOptions, autovacuum.vacuum_ins_scale_factor)),
        pe!("autovacuum_analyze_scale_factor", RELOPT_TYPE_REAL,
            offsetof!(StdRdOptions, autovacuum.analyze_scale_factor)),
        pe!("user_catalog_table", RELOPT_TYPE_BOOL,
            offsetof!(StdRdOptions, user_catalog_table)),
        pe!("parallel_workers", RELOPT_TYPE_INT,
            offsetof!(StdRdOptions, parallel_workers)),
        pe!("vacuum_index_cleanup", RELOPT_TYPE_ENUM,
            offsetof!(StdRdOptions, vacuum_index_cleanup)),
        pe_set!("vacuum_truncate", RELOPT_TYPE_BOOL,
            offsetof!(StdRdOptions, vacuum_truncate),
            offsetof!(StdRdOptions, vacuum_truncate_set)),
        pe!("vacuum_max_eager_freeze_failure_rate", RELOPT_TYPE_REAL,
            offsetof!(StdRdOptions, vacuum_max_eager_freeze_failure_rate)),
    ];

    build_reloptions(
        reloptions,
        validate,
        kind,
        size_of::<StdRdOptions>(),
        tab.as_ptr(),
        lengthof!(tab) as c_int,
    ) as *mut bytea
}

/*
 * build_reloptions
 *
 * Parses "reloptions" provided by the caller, returning them in a
 * structure containing the parsed options.  (See the C source for details.)
 */
#[no_mangle]
pub unsafe fn build_reloptions(
    reloptions: Datum,
    validate: bool,
    kind: relopt_kind,
    relopt_struct_size: Size,
    relopt_elems: *const relopt_parse_elt,
    num_relopt_elems: c_int,
) -> *mut c_void {
    let mut numoptions: c_int = 0;
    let options: *mut relopt_value;
    let rdopts: *mut c_void;

    /* parse options specific to given relation option kind */
    options = parseRelOptions(reloptions, validate, kind, &mut numoptions);
    Assert!(numoptions <= num_relopt_elems);

    /* if none set, we're done */
    if numoptions == 0 {
        Assert!(options.is_null());
        return ptr::null_mut();
    }

    /* allocate and fill the structure */
    rdopts = allocateReloptStruct(relopt_struct_size, options, numoptions);
    fillRelOptions(
        rdopts,
        relopt_struct_size,
        options,
        numoptions,
        validate,
        relopt_elems,
        num_relopt_elems,
    );

    pfree(options as *mut c_void);

    rdopts
}

/*
 * Parse local options, allocate a bytea struct that's of the specified
 * 'base_size' plus any extra space that's needed for string variables,
 * fill its option's fields located at the given offsets and return it.
 */
pub unsafe fn build_local_reloptions(
    relopts: *mut local_relopts,
    options: Datum,
    validate: bool,
) -> *mut c_void {
    let noptions: c_int = list_length((*relopts).options);
    let elems: *mut relopt_parse_elt =
        palloc(size_of::<relopt_parse_elt>() * noptions as usize) as *mut relopt_parse_elt;
    let vals: *mut relopt_value;
    let opts: *mut c_void;
    let mut i: c_int = 0;

    foreach!(lc, (*relopts).options, {
        let opt: *mut local_relopt = *(crate::current_cell!(lc) as *mut *mut local_relopt);

        (*elems.offset(i as isize)).optname = (*(*opt).option).name;
        (*elems.offset(i as isize)).opttype = (*(*opt).option).r#type;
        (*elems.offset(i as isize)).offset = (*opt).offset;
        (*elems.offset(i as isize)).isset_offset = 0; /* not supported for local relopts yet */

        i += 1;
    });

    vals = parseLocalRelOptions(relopts, options, validate);
    opts = allocateReloptStruct((*relopts).relopt_struct_size, vals, noptions);
    fillRelOptions(
        opts,
        (*relopts).relopt_struct_size,
        vals,
        noptions,
        validate,
        elems,
        noptions,
    );

    if validate {
        foreach!(lc, (*relopts).validators, {
            let v = ptr_to_validator(*(crate::current_cell!(lc) as *mut *mut c_void));
            if let Some(validator) = v {
                validator(opts, vals, noptions);
            }
        });
    }

    if !elems.is_null() {
        pfree(elems as *mut c_void);
    }

    opts
}

/*
 * Option parser for partitioned tables
 */
pub unsafe fn partitioned_table_reloptions(reloptions: Datum, validate: bool) -> *mut bytea {
    if validate && reloptions != 0 as Datum {
        ereport!(
            ERROR,
            errmsg!("cannot specify storage parameters for a partitioned table")
        );
        // C also: errcode(ERRCODE_WRONG_OBJECT_TYPE),
        //   errhint("Specify storage parameters for its leaf partitions instead.")
    }
    ptr::null_mut()
}

/*
 * Option parser for views
 */
pub unsafe fn view_reloptions(reloptions: Datum, validate: bool) -> *mut bytea {
    static tab: [relopt_parse_elt; 3] = [
        pe!("security_barrier", RELOPT_TYPE_BOOL, offsetof!(ViewOptions, security_barrier)),
        pe!("security_invoker", RELOPT_TYPE_BOOL, offsetof!(ViewOptions, security_invoker)),
        pe!("check_option", RELOPT_TYPE_ENUM, offsetof!(ViewOptions, check_option)),
    ];

    build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_VIEW,
        size_of::<ViewOptions>(),
        tab.as_ptr(),
        lengthof!(tab) as c_int,
    ) as *mut bytea
}

/*
 * Parse options for heaps, views and toast tables.
 */
pub unsafe fn heap_reloptions(relkind: c_char, reloptions: Datum, validate: bool) -> *mut bytea {
    let rdopts: *mut StdRdOptions;

    if relkind == RELKIND_TOASTVALUE {
        rdopts = default_reloptions(reloptions, validate, RELOPT_KIND_TOAST) as *mut StdRdOptions;
        if !rdopts.is_null() {
            /* adjust default-only parameters for TOAST relations */
            (*rdopts).fillfactor = 100;
            (*rdopts).autovacuum.analyze_threshold = -1;
            (*rdopts).autovacuum.analyze_scale_factor = -1.0;
        }
        rdopts as *mut bytea
    } else if relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW {
        default_reloptions(reloptions, validate, RELOPT_KIND_HEAP)
    } else {
        /* other relkinds are not supported */
        ptr::null_mut()
    }
}

/*
 * Parse options for indexes.
 *
 *	amoptions	index AM's option parser function
 *	reloptions	options as text[] datum
 *	validate	error flag
 */
pub unsafe fn index_reloptions(
    amoptions: amoptions_function,
    reloptions: Datum,
    validate: bool,
) -> *mut bytea {
    Assert!(amoptions.is_some());

    /* Assume function is strict */
    if !PointerIsValid(DatumGetPointer(reloptions)) {
        return ptr::null_mut();
    }

    (amoptions.unwrap())(reloptions, validate)
}

/*
 * Option parser for attribute reloptions
 */
pub unsafe fn attribute_reloptions(reloptions: Datum, validate: bool) -> *mut bytea {
    static tab: [relopt_parse_elt; 2] = [
        pe!("n_distinct", RELOPT_TYPE_REAL, offsetof!(AttributeOpts, n_distinct)),
        pe!("n_distinct_inherited", RELOPT_TYPE_REAL,
            offsetof!(AttributeOpts, n_distinct_inherited)),
    ];

    build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_ATTRIBUTE,
        size_of::<AttributeOpts>(),
        tab.as_ptr(),
        lengthof!(tab) as c_int,
    ) as *mut bytea
}

/*
 * Option parser for tablespace reloptions
 */
pub unsafe fn tablespace_reloptions(reloptions: Datum, validate: bool) -> *mut bytea {
    static tab: [relopt_parse_elt; 4] = [
        pe!("random_page_cost", RELOPT_TYPE_REAL, offsetof!(TableSpaceOpts, random_page_cost)),
        pe!("seq_page_cost", RELOPT_TYPE_REAL, offsetof!(TableSpaceOpts, seq_page_cost)),
        pe!("effective_io_concurrency", RELOPT_TYPE_INT,
            offsetof!(TableSpaceOpts, effective_io_concurrency)),
        pe!("maintenance_io_concurrency", RELOPT_TYPE_INT,
            offsetof!(TableSpaceOpts, maintenance_io_concurrency)),
    ];

    build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_TABLESPACE,
        size_of::<TableSpaceOpts>(),
        tab.as_ptr(),
        lengthof!(tab) as c_int,
    ) as *mut bytea
}

/*
 * Determine the required LOCKMODE from an option list.
 *
 * Called from AlterTableGetLockLevel(), see that function
 * for a longer explanation of how this works.
 */
pub unsafe fn AlterTableGetRelOptionsLockLevel(defList: *mut List) -> LOCKMODE {
    let mut lockmode: LOCKMODE = NoLock;

    if defList == NIL {
        return AccessExclusiveLock;
    }

    if need_initialization {
        initialize_reloptions();
    }

    foreach!(cell, defList, {
        let def: *mut DefElem = *(crate::current_cell!(cell) as *mut *mut DefElem);
        let mut i: c_int;

        i = 0;
        while !(*relOpts.offset(i as isize)).is_null() {
            if strncmp(
                (**relOpts.offset(i as isize)).name,
                (*def).defname,
                ((**relOpts.offset(i as isize)).namelen + 1) as usize,
            ) == 0
            {
                if lockmode < (**relOpts.offset(i as isize)).lockmode {
                    lockmode = (**relOpts.offset(i as isize)).lockmode;
                }
            }
            i += 1;
        }
    });

    lockmode
}

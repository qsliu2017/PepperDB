/*-------------------------------------------------------------------------
 *
 * bootstrap.rs
 *    routines to support running postgres in 'bootstrap' mode
 * bootstrap mode is used to create the initial template database
 *
 * Merged from:
 *   postgres/src/backend/bootstrap/bootstrap.c
 *   postgres/src/include/bootstrap/bootstrap.h
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *   src/bootstrap/bootstrap.rs
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

// ---- from bootstrap.h -------------------------------------------------------

/// Maximum number of attributes in a relation supported at bootstrap time
/// (i.e., the max possible in a system table).
pub const MAXATTR: usize = 40;

/// Nullness values for DefineAttr().
pub const BOOTCOL_NULL_AUTO: c_int = 1;
pub const BOOTCOL_NULL_FORCE_NULL: c_int = 2;
pub const BOOTCOL_NULL_FORCE_NOT_NULL: c_int = 3;

// ---- real module imports ----------------------------------------------------

use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::access::htup_details::HeapTuple;
use crate::access::common::tupdesc::{CreateTupleDesc, TupleDescAttr, TupleDesc};
use crate::access::htup_details::GETSTRUCT;
use crate::access::relscan::TableScanDesc;
use crate::access::sdir::ForwardScanDirection;
use crate::access::table::table::{table_close, table_open, table_openrv};
use crate::access::table::tableam::table_beginscan_catalog;
use crate::access::index::indexam::{index_close, index_open, NoLock};
// TODO(pg-port): real heap_getnext/simple_heap_insert live in access/heap/heapam.c (unwired).
unsafe fn heap_getnext(_scan: *mut c_void, _dir: c_int) -> HeapTuple { core::ptr::null_mut() }
unsafe fn simple_heap_insert(_rel: crate::utils::rel::Relation, _tup: HeapTuple) {}
use crate::catalog::catalog_oids::TypeRelationId;
use crate::catalog::pg_attribute::{
    FormData_pg_attribute, Form_pg_attribute, ATTRIBUTE_FIXED_PART_SIZE,
};
use crate::catalog::pg_type::{
    FormData_pg_type, Form_pg_type,
    TYPALIGN_CHAR, TYPALIGN_INT, TYPALIGN_SHORT,
    TYPSTORAGE_EXTENDED, TYPSTORAGE_PLAIN,
};
use crate::catalog::pg_type_d::{
    ACLITEMOID, BOOLOID, BYTEAOID, CHAROID, CIDOID, FLOAT4OID, INT2OID, INT2VECTOROID,
    INT4ARRAYOID, INT4OID, NAMEOID, OIDOID, OIDVECTOROID, PG_NODE_TREEOID, REGCLASSOID,
    REGNAMESPACEOID, REGPROCOID, REGROLEOID, REGTYPEOID, TEXTOID, TIDOID, XIDOID,
};
use crate::catalog::pg_known_oids::{C_COLLATION_OID, DEFAULT_COLLATION_OID};
use crate::common::link_canary::pg_link_canary_is_frontend;
use crate::miscadmin::{
    BootstrapProcessing, IsUnderPostmaster, NormalProcessing, SetProcessingMode,
    IgnoreSystemIndexes, InitializeMaxBackends, InitStandaloneProcess,
    InitializeFastPathLocks, checkDataDir, ChangeToDataDir, CreateDataDirLockFile,
    InitPostgres, BaseInit, OutputFileName,
};
use crate::nodes::execnodes::IndexInfo;
use crate::nodes::makefuncs::makeRangeVar;
use crate::nodes::pg_list::{lappend, list_free_deep, lfirst, NIL, List, ListCell};
use crate::pg_config_manual::MAXPGPATH;
use crate::port::port_api::strlcpy;
use crate::postmaster::pmchild::InitPostmasterChildSlots;
use crate::storage::ipc::ipci::CreateSharedMemoryAndSemaphores;
use crate::storage::ipc::ipc::proc_exit;
use crate::storage::lockdefs::LOCKMODE;
use crate::utils::builtins::namestrcpy;
use crate::utils::cache::relmapper::RelationMapFinishBootstrap;
use crate::utils::fmgr::{OidInputFunctionCall, OidOutputFunctionCall};
use crate::utils::rel::{RelationGetNumberOfAttributes, RelationGetRelationName, Relation};
use crate::libpq::pqsignal::{
    pqsignal, SIG_DFL, SIGHUP, SIGINT, SIGQUIT, SIGTERM,
};

// ---- stubs for not-yet-translated callees -----------------------------------

// TODO(pg-port): real BootStrapXLOG lives in access/transam/xlog.c
unsafe fn BootStrapXLOG(_checksum_version: u32) { unimplemented!("BootStrapXLOG") }

// TODO(pg-port): real InitProcess lives in storage/lmgr/proc.c
unsafe fn InitProcess() { unimplemented!("InitProcess") }

// TODO(pg-port): real StartTransactionCommand lives in access/transam/xact.c
unsafe fn StartTransactionCommand() { unimplemented!("StartTransactionCommand") }

// TODO(pg-port): real CommitTransactionCommand lives in access/transam/xact.c
unsafe fn CommitTransactionCommand() { unimplemented!("CommitTransactionCommand") }

// TODO(pg-port): real set_max_safe_fds lives in storage/file/fd.c
unsafe fn set_max_safe_fds() { unimplemented!("set_max_safe_fds") }

// TODO(pg-port): real InitializeGUCOptions lives in utils/misc/guc.c
unsafe fn InitializeGUCOptions() { unimplemented!("InitializeGUCOptions") }

// TODO(pg-port): real SelectConfigFiles lives in utils/misc/guc.c
unsafe fn SelectConfigFiles(_userDoption: *const c_char, _progname: *const c_char) -> bool {
    unimplemented!("SelectConfigFiles")
}

// TODO(pg-port): real SetConfigOption lives in utils/misc/guc.c
pub type GucContext = c_int;
pub type GucSource = c_int;
pub const PGC_POSTMASTER: GucContext = 0;
pub const PGC_INTERNAL: GucContext = 3;
pub const PGC_S_ARGV: GucSource = 4;
pub const PGC_S_DYNAMIC_DEFAULT: GucSource = 1;
unsafe fn SetConfigOption(
    _name: *const c_char,
    _value: *const c_char,
    _context: GucContext,
    _source: GucSource,
) {
    unimplemented!("SetConfigOption")
}

// TODO(pg-port): real ParseLongOption lives in utils/misc/guc.c
unsafe fn ParseLongOption(
    _string: *const c_char,
    _name: *mut *mut c_char,
    _value: *mut *mut c_char,
) {
    unimplemented!("ParseLongOption")
}

// TODO(pg-port): real parse_dispatch_option lives in postmaster/postmaster.c;
// DISPATCH_POSTMASTER means "no special dispatch match found"
pub const DISPATCH_POSTMASTER: c_int = 0;
unsafe fn parse_dispatch_option(_opt: *const c_char) -> c_int {
    unimplemented!("parse_dispatch_option")
}

// TODO(pg-port): real psprintf lives in lib/psprintf.c / utils/mmgr/mcxt.c
unsafe fn psprintf(fmt: *const c_char, arg: *const c_char) -> *mut c_char {
    let _ = (fmt, arg);
    unimplemented!("psprintf")
}

// TODO(pg-port): real write_stderr lives in utils/misc/ps_status.c / common/
unsafe fn write_stderr(msg: *const c_char) {
    let _ = msg;
    // low-level stderr write, usable before elog is ready
    unimplemented!("write_stderr")
}

// TODO(pg-port): real table_endscan lives in access/table/tableam.c
unsafe fn table_endscan(_scan: TableScanDesc) { unimplemented!("table_endscan") }

// TODO(pg-port): real index_build lives in catalog/index.c
unsafe fn index_build(
    _heap: Relation,
    _ind: Relation,
    _info: *const IndexInfo,
    _isprimary: bool,
    _isreindex: bool,
) {
    unimplemented!("index_build")
}

// TODO(pg-port): real copyObject lives in nodes/copyfuncs.c
unsafe fn copyObject(obj: *const c_void) -> *mut c_void {
    obj as *mut c_void
}

// TODO(pg-port): real errmsg_internal - same contract as errmsg! in this shim
macro_rules! errmsg_internal {
    ($($arg:tt)*) => { format!($($arg)*) };
}

// fmgroids.h F_xxx constants: not yet translated (utils/fmgroids.h is generated).
// TODO(pg-port): real F_xxx live in utils/fmgroids.h (generated by Gen_fmgrtab.pl).
pub type RegProcedure = Oid;
pub const F_BOOLIN: RegProcedure = 1242;
pub const F_BOOLOUT: RegProcedure = 1243;
pub const F_BYTEAIN: RegProcedure = 1244;
pub const F_BYTEAOUT: RegProcedure = 1245;
pub const F_CHARIN: RegProcedure = 1246;
pub const F_CHAROUT: RegProcedure = 1247;
pub const F_INT2IN: RegProcedure = 38;
pub const F_INT2OUT: RegProcedure = 39;
pub const F_INT4IN: RegProcedure = 42;
pub const F_INT4OUT: RegProcedure = 43;
pub const F_FLOAT4IN: RegProcedure = 200;
pub const F_FLOAT4OUT: RegProcedure = 201;
pub const F_NAMEIN: RegProcedure = 34;
pub const F_NAMEOUT: RegProcedure = 35;
pub const F_REGCLASSIN: RegProcedure = 2218;
pub const F_REGCLASSOUT: RegProcedure = 2219;
pub const F_REGPROCIN: RegProcedure = 44;
pub const F_REGPROCOUT: RegProcedure = 45;
pub const F_REGTYPEIN: RegProcedure = 2220;
pub const F_REGTYPEOUT: RegProcedure = 2221;
pub const F_REGROLEIN: RegProcedure = 4142;
pub const F_REGROLEOUT: RegProcedure = 4143;
pub const F_REGNAMESPACEIN: RegProcedure = 4144;
pub const F_REGNAMESPACEOUT: RegProcedure = 4145;
pub const F_TEXTIN: RegProcedure = 46;
pub const F_TEXTOUT: RegProcedure = 47;
pub const F_OIDIN: RegProcedure = 1798;
pub const F_OIDOUT: RegProcedure = 1799;
pub const F_TIDIN: RegProcedure = 204;
pub const F_TIDOUT: RegProcedure = 205;
pub const F_XIDIN: RegProcedure = 140;
pub const F_XIDOUT: RegProcedure = 141;
pub const F_CIDIN: RegProcedure = 133;
pub const F_CIDOUT: RegProcedure = 134;
pub const F_PG_NODE_TREE_IN: RegProcedure = 194; // placeholder OID
pub const F_PG_NODE_TREE_OUT: RegProcedure = 195; // placeholder OID
pub const F_INT2VECTORIN: RegProcedure = 40;
pub const F_INT2VECTOROUT: RegProcedure = 41;
pub const F_OIDVECTORIN: RegProcedure = 54;
pub const F_OIDVECTOROUT: RegProcedure = 55;
pub const F_ARRAY_IN: RegProcedure = 750;
pub const F_ARRAY_OUT: RegProcedure = 751;

// TODO(pg-port): real InvalidCompressionMethod lives in access/toast_compression.h
// (already translated at crate::access::common::toast_compression::InvalidCompressionMethod)
pub const InvalidCompressionMethod: c_char = b'\0' as c_char;

// TODO(pg-port): real ERRCODE_SYNTAX_ERROR lives in utils/errcodes.h (generated).
const ERRCODE_SYNTAX_ERROR: c_int = 0;

// TODO(pg-port): real PG_DATA_CHECKSUM_VERSION lives in storage/bufpage.h
// (already translated at crate::storage::bufpage::PG_DATA_CHECKSUM_VERSION)
pub const PG_DATA_CHECKSUM_VERSION: u32 = 1;

// ---- from bootstrap.h: scanner/parser interface (boot_yyparse / boot_yylex) -
// These are generated by bootparse.y / bootscanner.l; stubs provided here.

/// Opaque scanner state (flex reentrant scanner).
// TODO(pg-port): real yyscan_t is generated by bootscanner.l
pub type yyscan_t = *mut c_void;

/// Token value union (generated by bootparse.y).
// TODO(pg-port): real YYSTYPE is generated by bootparse.y
#[repr(C)]
pub union YYSTYPE {
    pub ival: c_int,
    pub str_val: *mut c_char,
}

/// boot_yylex_init -- initialize the reentrant bootstrap scanner.
// TODO(pg-port): real boot_yylex_init is generated by bootscanner.l
pub unsafe fn boot_yylex_init(yyscannerp: *mut yyscan_t) -> c_int {
    let _ = yyscannerp;
    unimplemented!("boot_yylex_init")
}

/// boot_yyparse -- parse bootstrap input using the reentrant scanner.
// TODO(pg-port): real boot_yyparse is generated by bootparse.y
pub unsafe fn boot_yyparse(_yyscanner: yyscan_t) -> c_int {
    unimplemented!("boot_yyparse")
}

/// boot_yylex -- return the next bootstrap token.
// TODO(pg-port): real boot_yylex is generated by bootscanner.l
pub unsafe fn boot_yylex(_yylval_param: *mut YYSTYPE, _yyscanner: yyscan_t) -> c_int {
    unimplemented!("boot_yylex")
}

/// boot_yyerror -- report a bootstrap parse error and do not return.
// TODO(pg-port): real boot_yyerror is generated by bootparse.y
pub unsafe fn boot_yyerror(_yyscanner: yyscan_t, message: *const c_char) -> ! {
    let _ = message;
    unimplemented!("boot_yyerror")
}

// ---- global variables (bootstrap.h PGDLLIMPORT declarations) ----------------

/// current relation descriptor (BKI OPEN command)
pub static mut boot_reldesc: Relation = core::ptr::null_mut();

/// attribute info for the relation being built/opened
pub static mut attrtypes: [Form_pg_attribute; MAXATTR] =
    [core::ptr::null_mut(); MAXATTR];

/// number of attributes for the currently open relation
pub static mut numattr: c_int = 0;

// ---- file-private types -----------------------------------------------------

/// Basic information associated with each type.  Used before pg_type is
/// filled, so it covers the datatypes used as column types in the core
/// "bootstrapped" catalogs.
///
///   XXX several of these input/output functions do catalog scans
///       (e.g., F_REGPROCIN scans pg_proc).  This obviously creates some
///       order dependencies in the catalog creation process.
#[derive(Copy, Clone)]
struct TypInfo {
    name: [c_char; 64 /* NAMEDATALEN */],
    oid: Oid,
    elem: Oid,
    len: i16,
    byval: bool,
    align: c_char,
    storage: c_char,
    collation: Oid,
    inproc: RegProcedure,
    outproc: RegProcedure,
}

/// Helper to build a TypInfo from a &str literal at compile time.
/// The name must be <= 63 bytes (NAMEDATALEN-1).
const fn typinfo(
    name_str: &[u8],
    oid: Oid,
    elem: Oid,
    len: i16,
    byval: bool,
    align: c_char,
    storage: c_char,
    collation: Oid,
    inproc: RegProcedure,
    outproc: RegProcedure,
) -> TypInfo {
    let mut name = [0i8; 64];
    let mut i = 0usize;
    while i < name_str.len() && i < 63 {
        name[i] = name_str[i] as c_char;
        i += 1;
    }
    TypInfo { name, oid, elem, len, byval, align, storage, collation, inproc, outproc }
}

static TYP_INFO: [TypInfo; 24] = [
    typinfo(b"bool",         BOOLOID,         0,           1,   true,  TYPALIGN_CHAR,  TYPSTORAGE_PLAIN,    InvalidOid,              F_BOOLIN,         F_BOOLOUT),
    typinfo(b"bytea",        BYTEAOID,        0,          -1,   false, TYPALIGN_INT,   TYPSTORAGE_EXTENDED, InvalidOid,              F_BYTEAIN,        F_BYTEAOUT),
    typinfo(b"char",         CHAROID,         0,           1,   true,  TYPALIGN_CHAR,  TYPSTORAGE_PLAIN,    InvalidOid,              F_CHARIN,         F_CHAROUT),
    typinfo(b"int2",         INT2OID,         0,           2,   true,  TYPALIGN_SHORT, TYPSTORAGE_PLAIN,    InvalidOid,              F_INT2IN,         F_INT2OUT),
    typinfo(b"int4",         INT4OID,         0,           4,   true,  TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_INT4IN,         F_INT4OUT),
    typinfo(b"float4",       FLOAT4OID,       0,           4,   true,  TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_FLOAT4IN,       F_FLOAT4OUT),
    typinfo(b"name",         NAMEOID,         CHAROID,    64,   false, TYPALIGN_CHAR,  TYPSTORAGE_PLAIN,    C_COLLATION_OID,         F_NAMEIN,         F_NAMEOUT),
    typinfo(b"regclass",     REGCLASSOID,     0,           4,   true,  TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_REGCLASSIN,     F_REGCLASSOUT),
    typinfo(b"regproc",      REGPROCOID,      0,           4,   true,  TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_REGPROCIN,      F_REGPROCOUT),
    typinfo(b"regtype",      REGTYPEOID,      0,           4,   true,  TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_REGTYPEIN,      F_REGTYPEOUT),
    typinfo(b"regrole",      REGROLEOID,      0,           4,   true,  TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_REGROLEIN,      F_REGROLEOUT),
    typinfo(b"regnamespace", REGNAMESPACEOID, 0,           4,   true,  TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_REGNAMESPACEIN, F_REGNAMESPACEOUT),
    typinfo(b"text",         TEXTOID,         0,          -1,   false, TYPALIGN_INT,   TYPSTORAGE_EXTENDED, DEFAULT_COLLATION_OID,   F_TEXTIN,         F_TEXTOUT),
    typinfo(b"oid",          OIDOID,          0,           4,   true,  TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_OIDIN,          F_OIDOUT),
    typinfo(b"tid",          TIDOID,          0,           6,   false, TYPALIGN_SHORT, TYPSTORAGE_PLAIN,    InvalidOid,              F_TIDIN,          F_TIDOUT),
    typinfo(b"xid",          XIDOID,          0,           4,   true,  TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_XIDIN,          F_XIDOUT),
    typinfo(b"cid",          CIDOID,          0,           4,   true,  TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_CIDIN,          F_CIDOUT),
    typinfo(b"pg_node_tree", PG_NODE_TREEOID, 0,          -1,   false, TYPALIGN_INT,   TYPSTORAGE_EXTENDED, DEFAULT_COLLATION_OID,   F_PG_NODE_TREE_IN,F_PG_NODE_TREE_OUT),
    typinfo(b"int2vector",   INT2VECTOROID,   INT2OID,    -1,   false, TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_INT2VECTORIN,   F_INT2VECTOROUT),
    typinfo(b"oidvector",    OIDVECTOROID,    OIDOID,     -1,   false, TYPALIGN_INT,   TYPSTORAGE_PLAIN,    InvalidOid,              F_OIDVECTORIN,    F_OIDVECTOROUT),
    typinfo(b"_int4",        INT4ARRAYOID,    INT4OID,    -1,   false, TYPALIGN_INT,   TYPSTORAGE_EXTENDED, InvalidOid,              F_ARRAY_IN,       F_ARRAY_OUT),
    typinfo(b"_text",        1009,            TEXTOID,    -1,   false, TYPALIGN_INT,   TYPSTORAGE_EXTENDED, DEFAULT_COLLATION_OID,   F_ARRAY_IN,       F_ARRAY_OUT),
    typinfo(b"_oid",         1028,            OIDOID,     -1,   false, TYPALIGN_INT,   TYPSTORAGE_EXTENDED, InvalidOid,              F_ARRAY_IN,       F_ARRAY_OUT),
    typinfo(b"_char",        1002,            CHAROID,    -1,   false, TYPALIGN_INT,   TYPSTORAGE_EXTENDED, InvalidOid,              F_ARRAY_IN,       F_ARRAY_OUT),
    // _aclitem is handled below; use count 24 and add _aclitem as index [24] via N_TYPES=25
];

// The C source has 25 entries (including _aclitem). We split it so the const fn
// can handle ACLITEMOID which appears after all the above.
// Rather than a separate fn, replicate _aclitem inline:
static _TYP_INFO_ACLITEM: TypInfo = typinfo(
    b"_aclitem", 1034, ACLITEMOID, -1, false,
    TYPALIGN_INT, TYPSTORAGE_EXTENDED, InvalidOid,
    F_ARRAY_IN, F_ARRAY_OUT,
);

/// Total number of built-in bootstrap types (= TYP_INFO.len() + 1 for _aclitem).
const N_TYPES: usize = 25;

/// Return a TypInfo by index (0..N_TYPES).
#[inline]
fn typ_info(i: usize) -> &'static TypInfo {
    if i < TYP_INFO.len() {
        &TYP_INFO[i]
    } else {
        &_TYP_INFO_ACLITEM
    }
}

/// a hack: maps an OID to the full FormData_pg_type row from pg_type.
struct Typmap {
    am_oid: Oid,
    am_typ: FormData_pg_type,
}

/// List of struct typmap* (loaded from pg_type when first needed).
static mut Typ: *mut List = NIL;

/// Points to the most recently found entry in Typ (set by gettype).
static mut Ap: *mut Typmap = core::ptr::null_mut();

/// Current row's attribute values (parallel to attrtypes[]).
static mut values: [Datum; MAXATTR] = [0; MAXATTR];

/// NULL flags for current row.
static mut Nulls: [bool; MAXATTR] = [false; MAXATTR];

/// Special no-GC memory context for index registration.
static mut nogc: MemoryContext = core::ptr::null_mut();

// ---- IndexList (deferred index build) ---------------------------------------

/// Record of an index that has been declared but not yet built.
///
///   At bootstrap time we first declare all the indices to be built, then
///   build them.  The IndexList stores enough information to allow us to
///   build the indices after they've been declared.
struct IndexList {
    il_heap: Oid,
    il_ind: Oid,
    il_info: *mut IndexInfo,
    il_next: *mut IndexList,
}

static mut ILHead: *mut IndexList = core::ptr::null_mut();

// ---- private helpers --------------------------------------------------------

/*
 * In shared memory checker mode, all we really want to do is create shared
 * memory and semaphores (just to prove we can do it with the current GUC
 * settings).  Since, in fact, that was already done by
 * CreateSharedMemoryAndSemaphores(), we have nothing more to do here.
 */
unsafe fn CheckerModeMain() -> ! {
    proc_exit(0);
}

/*
 * Set up signal handling for a bootstrap process
 */
unsafe fn bootstrap_signals() {
    Assert!(!IsUnderPostmaster);

    /*
     * We don't actually need any non-default signal handling in bootstrap
     * mode; "curl up and die" is a sufficient response for all these cases.
     * Let's set that handling explicitly, as documentation if nothing else.
     */
    pqsignal(SIGHUP, SIG_DFL);
    pqsignal(SIGINT, SIG_DFL);
    pqsignal(SIGTERM, SIG_DFL);
    pqsignal(SIGQUIT, SIG_DFL);
}

/*
 * AllocateAttribute
 *
 * Note: bootstrap never sets any per-column ACLs, so we only need
 * ATTRIBUTE_FIXED_PART_SIZE space per attribute.
 */
unsafe fn AllocateAttribute() -> Form_pg_attribute {
    MemoryContextAllocZero(TopMemoryContext, ATTRIBUTE_FIXED_PART_SIZE) as Form_pg_attribute
}

/*
 * cleanup
 */
unsafe fn cleanup() {
    if !boot_reldesc.is_null() {
        closerel(core::ptr::null_mut());
    }
}

/*
 * populate_typ_list
 *
 * Load the Typ list by reading pg_type.
 */
unsafe fn populate_typ_list() {
    let rel: Relation;
    let scan: TableScanDesc;
    let old: MemoryContext;

    Assert!(Typ == NIL);

    rel = table_open(TypeRelationId, NoLock);
    scan = table_beginscan_catalog(rel, 0, core::ptr::null_mut());
    old = MemoryContextSwitchTo(TopMemoryContext);
    'scan: loop {
        let tup = heap_getnext(scan as *mut c_void, ForwardScanDirection);
        if tup.is_null() {
            break 'scan;
        }
        let typ_form = GETSTRUCT(tup) as Form_pg_type;
        let newtyp = palloc(core::mem::size_of::<Typmap>()) as *mut Typmap;
        Typ = lappend(Typ, newtyp as *mut c_void);

        (*newtyp).am_oid = (*typ_form).oid;
        core::ptr::copy_nonoverlapping(
            typ_form as *const FormData_pg_type,
            &raw mut (*newtyp).am_typ,
            1,
        );
    }
    MemoryContextSwitchTo(old);
    table_endscan(scan);
    table_close(rel, NoLock);
}

/*
 * gettype
 *
 * NB: this is really ugly; it will return an integer index into TYP_INFO[],
 * and not an OID at all, until the first reference to a type not known in
 * TYP_INFO[].  At that point it will read and cache pg_type in Typ,
 * and subsequently return a real OID (and set the global pointer Ap to
 * point at the found row in Typ).  So caller must check whether Typ is
 * still NIL to determine what the return value is!
 */
unsafe fn gettype(type_: *const c_char) -> Oid {
    if Typ != NIL {
        // first pass: search the cached Typ list
        let lc = if !(*Typ).elements.is_null() {
            (*Typ).elements
        } else {
            core::ptr::null_mut()
        };
        // iterate via raw pointer arithmetic (foreach equivalent)
        let len = (*Typ).length as usize;
        let elems = (*Typ).elements;
        for idx in 0..len {
            let app = (*elems.add(idx)).ptr_value as *mut Typmap;
            let nm = (*app).am_typ.typname.data.as_ptr() as *const c_char;
            if libc_strncmp(nm, type_, 64 /* NAMEDATALEN */) == 0 {
                Ap = app;
                return (*app).am_oid;
            }
        }

        /*
         * The type wasn't known; reload the pg_type contents and check again
         * to handle composite types, added since last populating the list.
         */
        list_free_deep(Typ);
        Typ = NIL;
        populate_typ_list();

        /*
         * Calling gettype would result in infinite recursion for types
         * missing in pg_type, so just repeat the lookup.
         */
        let len2 = (*Typ).length as usize;
        let elems2 = (*Typ).elements;
        for idx in 0..len2 {
            let app = (*elems2.add(idx)).ptr_value as *mut Typmap;
            let nm = (*app).am_typ.typname.data.as_ptr() as *const c_char;
            if libc_strncmp(nm, type_, 64) == 0 {
                Ap = app;
                return (*app).am_oid;
            }
        }
    } else {
        for i in 0..N_TYPES {
            let ti = typ_info(i);
            if libc_strncmp(type_, ti.name.as_ptr(), 64) == 0 {
                return i as Oid;
            }
        }
        // Not in TYP_INFO, so we'd better be able to read pg_type now
        elog!(DEBUG4, "external type: {:?}", type_);
        populate_typ_list();
        return gettype(type_);
    }
    elog!(ERROR, "unrecognized type \"{:?}\"", type_);
    // not reached, here to make compiler happy
    0
}

/// Thin shim around C's strncmp (not yet available as a Rust stdlib call for raw ptrs).
// TODO(pg-port): replace with a proper safe wrapper once ported.
#[inline]
unsafe fn libc_strncmp(a: *const c_char, b: *const c_char, n: usize) -> c_int {
    extern "C" {
        fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    }
    strncmp(a, b, n)
}

// ---- public API (bootstrap.h) -----------------------------------------------

/*
 * The main entry point for running the backend in bootstrap mode
 *
 * The bootstrap mode is used to initialize the template database.
 * The bootstrap backend doesn't speak SQL, but instead expects
 * commands in a special bootstrap language.
 *
 * When check_only is true, startup is done only far enough to verify that
 * the current configuration, particularly the passed in options pertaining
 * to shared memory sizing, options work (or at least do not cause an error
 * up to shared memory creation).
 */
pub unsafe fn BootstrapModeMain(argc: c_int, argv: *mut *mut c_char, check_only: bool) -> ! {
    let mut i: c_int;
    let progname: *mut c_char = *argv;
    let mut user_doption: *mut c_char = core::ptr::null_mut();
    let mut bootstrap_data_checksum_version: u32 = 0; /* No checksum */
    let mut scanner: yyscan_t = core::ptr::null_mut();

    Assert!(!IsUnderPostmaster);

    InitStandaloneProcess(*argv);

    /* Set defaults, to be overridden by explicit options below */
    InitializeGUCOptions();

    /* an initial --boot or --check should be present */
    Assert!(
        argc > 1
            && (libc_strcmp(*argv.add(1), b"--boot\0".as_ptr() as *const c_char) == 0
                || libc_strcmp(*argv.add(1), b"--check\0".as_ptr() as *const c_char) == 0)
    );
    // argv++; argc--;
    let argv = argv.add(1);
    let argc = argc - 1;

    // Manual argv scan replacing getopt(argc, argv, "B:c:d:D:Fkr:X:-:").
    // optind tracks the current position.
    let mut optind: c_int = 1;
    'getopt: loop {
        if optind >= argc {
            break 'getopt;
        }
        let arg: *mut c_char = *argv.add(optind as usize);
        if *arg != b'-' as c_char {
            break 'getopt;
        }
        let flag = *arg.add(1) as u8 as c_char;
        optind += 1;

        // Helper: fetch required optarg (next argv element or rest of current arg).
        // For simplicity we treat all opts as "next element" (GNU-style split).
        macro_rules! optarg {
            () => {{
                if optind >= argc {
                    write_stderr(b"missing option argument\0".as_ptr() as *const c_char);
                    proc_exit(1);
                }
                let v = *argv.add(optind as usize);
                optind += 1;
                v
            }};
        }

        match flag as u8 {
            b'B' => {
                let v = optarg!();
                SetConfigOption(
                    b"shared_buffers\0".as_ptr() as *const c_char,
                    v,
                    PGC_POSTMASTER,
                    PGC_S_ARGV,
                );
            }
            b'-' => {
                /*
                 * Error if the user misplaced a special must-be-first option
                 * for dispatching to a subprogram.  parse_dispatch_option()
                 * returns DISPATCH_POSTMASTER if it doesn't find a match, so
                 * error for anything else.
                 */
                let optarg_val = optarg!();
                if parse_dispatch_option(optarg_val) != DISPATCH_POSTMASTER {
                    ereport!(
                        ERROR,
                        errmsg!("--{} must be first argument", "-")
                    );
                }
                /* FALLTHROUGH to 'c' logic */
                let mut name: *mut c_char = core::ptr::null_mut();
                let mut value: *mut c_char = core::ptr::null_mut();
                ParseLongOption(optarg_val, &raw mut name, &raw mut value);
                if value.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!("--{} requires a value", "-")
                    );
                }
                SetConfigOption(name, value, PGC_POSTMASTER, PGC_S_ARGV);
                pfree(name as *mut c_void);
                pfree(value as *mut c_void);
            }
            b'c' => {
                let optarg_val = optarg!();
                let mut name: *mut c_char = core::ptr::null_mut();
                let mut value: *mut c_char = core::ptr::null_mut();
                ParseLongOption(optarg_val, &raw mut name, &raw mut value);
                if value.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!("-c {} requires a value", "-")
                    );
                }
                SetConfigOption(name, value, PGC_POSTMASTER, PGC_S_ARGV);
                pfree(name as *mut c_void);
                pfree(value as *mut c_void);
            }
            b'D' => {
                user_doption = pstrdup(optarg!());
            }
            b'd' => {
                /* Turn on debugging for the bootstrap process. */
                let optarg_val = optarg!();
                let debugstr = psprintf(
                    b"debug%s\0".as_ptr() as *const c_char,
                    optarg_val,
                );
                SetConfigOption(
                    b"log_min_messages\0".as_ptr() as *const c_char,
                    debugstr,
                    PGC_POSTMASTER,
                    PGC_S_ARGV,
                );
                SetConfigOption(
                    b"client_min_messages\0".as_ptr() as *const c_char,
                    debugstr,
                    PGC_POSTMASTER,
                    PGC_S_ARGV,
                );
                pfree(debugstr as *mut c_void);
            }
            b'F' => {
                SetConfigOption(
                    b"fsync\0".as_ptr() as *const c_char,
                    b"false\0".as_ptr() as *const c_char,
                    PGC_POSTMASTER,
                    PGC_S_ARGV,
                );
            }
            b'k' => {
                bootstrap_data_checksum_version = PG_DATA_CHECKSUM_VERSION;
            }
            b'r' => {
                let v = optarg!();
                strlcpy(
                    OutputFileName.as_mut_ptr(),
                    v,
                    MAXPGPATH,
                );
            }
            b'X' => {
                let v = optarg!();
                SetConfigOption(
                    b"wal_segment_size\0".as_ptr() as *const c_char,
                    v,
                    PGC_INTERNAL,
                    PGC_S_DYNAMIC_DEFAULT,
                );
            }
            _ => {
                write_stderr(b"Try \"<prog> --help\" for more information.\n\0".as_ptr()
                    as *const c_char);
                proc_exit(1);
            }
        }
    }

    if optind != argc {
        write_stderr(b"invalid command-line arguments\0".as_ptr() as *const c_char);
        proc_exit(1);
    }

    /* Acquire configuration parameters */
    if !SelectConfigFiles(user_doption, progname) {
        proc_exit(1);
    }

    /*
     * Validate we have been given a reasonable-looking DataDir and change
     * into it
     */
    checkDataDir();
    ChangeToDataDir();

    CreateDataDirLockFile(false);

    SetProcessingMode(BootstrapProcessing);
    IgnoreSystemIndexes = true;

    InitializeMaxBackends();

    /*
     * Even though bootstrapping runs in single-process mode, initialize
     * postmaster child slots array so that --check can detect running out of
     * shared memory or other resources if max_connections is set too high.
     */
    InitPostmasterChildSlots();

    InitializeFastPathLocks();

    CreateSharedMemoryAndSemaphores();

    /*
     * Estimate number of openable files.  This is essential too in --check
     * mode, because on some platforms semaphores count as open files.
     */
    set_max_safe_fds();

    /*
     * XXX: It might make sense to move this into its own function at some
     * point. Right now it seems like it'd cause more code duplication than
     * it's worth.
     */
    if check_only {
        SetProcessingMode(NormalProcessing);
        CheckerModeMain();
        // CheckerModeMain does not return; abort() serves as a compiler hint.
        #[allow(unreachable_code)]
        core::hint::unreachable_unchecked();
    }

    /*
     * Do backend-like initialization for bootstrap mode
     */
    InitProcess();

    BaseInit();

    bootstrap_signals();
    BootStrapXLOG(bootstrap_data_checksum_version);

    /*
     * To ensure that src/common/link-canary.c is linked into the backend, we
     * must call it from somewhere.  Here is as good as anywhere.
     */
    if pg_link_canary_is_frontend() {
        elog!(ERROR, "backend is incorrectly linked to frontend functions");
    }

    InitPostgres(
        core::ptr::null_mut(),
        InvalidOid,
        core::ptr::null_mut(),
        InvalidOid,
        0,
        core::ptr::null_mut(),
    );

    /* Initialize stuff for bootstrap-file processing */
    i = 0;
    while i < MAXATTR as c_int {
        attrtypes[i as usize] = core::ptr::null_mut();
        Nulls[i as usize] = false;
        i += 1;
    }

    if boot_yylex_init(&raw mut scanner) != 0 {
        elog!(ERROR, "yylex_init() failed: (errno)");
    }

    /*
     * Process bootstrap input.
     */
    StartTransactionCommand();
    boot_yyparse(scanner);
    CommitTransactionCommand();

    /*
     * We should now know about all mapped relations, so it's okay to write
     * out the initial relation mapping files.
     */
    RelationMapFinishBootstrap();

    /* Clean up and exit */
    cleanup();
    proc_exit(0);
}

// ---- misc functions ---------------------------------------------------------

/* ----------------------------------------------------------------
 *               MANUAL BACKEND INTERACTIVE INTERFACE COMMANDS
 * ----------------------------------------------------------------
 */

/* ----------------
 *    boot_openrel
 *
 * Execute BKI OPEN command.
 * ----------------
 */
pub unsafe fn boot_openrel(relname: *mut c_char) {
    let mut i: c_int;

    if libc_strlen(relname) >= 64 /* NAMEDATALEN */ {
        *relname.add(63) = b'\0' as c_char;
    }

    /*
     * pg_type must be filled before any OPEN command is executed, hence we
     * can now populate Typ if we haven't yet.
     */
    if Typ == NIL {
        populate_typ_list();
    }

    if !boot_reldesc.is_null() {
        closerel(core::ptr::null_mut());
    }

    elog!(
        DEBUG4,
        "open relation {:?}, attrsize {}",
        relname,
        ATTRIBUTE_FIXED_PART_SIZE
    );

    boot_reldesc = table_openrv(
        makeRangeVar(core::ptr::null_mut(), relname, -1),
        NoLock,
    );
    numattr = RelationGetNumberOfAttributes(boot_reldesc);
    i = 0;
    while i < numattr {
        if attrtypes[i as usize].is_null() {
            attrtypes[i as usize] = AllocateAttribute();
        }
        core::ptr::copy_nonoverlapping(
            TupleDescAttr((*boot_reldesc).rd_att, i) as *const u8,
            attrtypes[i as usize] as *mut u8,
            ATTRIBUTE_FIXED_PART_SIZE,
        );

        {
            let at: Form_pg_attribute = attrtypes[i as usize];
            elog!(
                DEBUG4,
                "create attribute {} name {:?} len {} num {} type {}",
                i,
                (*at).attname.data.as_ptr(),
                (*at).attlen,
                (*at).attnum,
                (*at).atttypid
            );
        }
        i += 1;
    }
}

/* ----------------
 *    closerel
 * ----------------
 */
pub unsafe fn closerel(relname: *mut c_char) {
    if !relname.is_null() {
        if !boot_reldesc.is_null() {
            if libc_strcmp(RelationGetRelationName(boot_reldesc), relname) != 0 {
                elog!(
                    ERROR,
                    "close of {:?} when {:?} was expected",
                    relname,
                    RelationGetRelationName(boot_reldesc)
                );
            }
        } else {
            elog!(ERROR, "close of {:?} before any relation was opened", relname);
        }
    }

    if boot_reldesc.is_null() {
        elog!(ERROR, "no open relation to close");
    } else {
        elog!(DEBUG4, "close relation {:?}", RelationGetRelationName(boot_reldesc));
        table_close(boot_reldesc, NoLock);
        boot_reldesc = core::ptr::null_mut();
    }
}

/* ----------------
 * DEFINEATTR()
 *
 * define a <field,type> pair
 * if there are n fields in a relation to be created, this routine
 * will be called n times
 * ----------------
 */
pub unsafe fn DefineAttr(name: *mut c_char, type_: *mut c_char, attnum: c_int, nullness: c_int) {
    let typeoid: Oid;

    if !boot_reldesc.is_null() {
        elog!(WARNING, "no open relations allowed with CREATE command");
        closerel(core::ptr::null_mut());
    }

    if attrtypes[attnum as usize].is_null() {
        attrtypes[attnum as usize] = AllocateAttribute();
    }
    MemSet(attrtypes[attnum as usize] as *mut c_void, 0, ATTRIBUTE_FIXED_PART_SIZE);

    namestrcpy(&raw mut (*attrtypes[attnum as usize]).attname, name);
    elog!(
        DEBUG4,
        "column {:?} {:?}",
        (*attrtypes[attnum as usize]).attname.data.as_ptr(),
        type_
    );
    (*attrtypes[attnum as usize]).attnum = (attnum + 1) as i16;

    typeoid = gettype(type_);

    if Typ != NIL {
        (*attrtypes[attnum as usize]).atttypid = (*Ap).am_oid;
        (*attrtypes[attnum as usize]).attlen = (*Ap).am_typ.typlen;
        (*attrtypes[attnum as usize]).attbyval = (*Ap).am_typ.typbyval;
        (*attrtypes[attnum as usize]).attalign = (*Ap).am_typ.typalign;
        (*attrtypes[attnum as usize]).attstorage = (*Ap).am_typ.typstorage;
        (*attrtypes[attnum as usize]).attcompression = InvalidCompressionMethod;
        (*attrtypes[attnum as usize]).attcollation = (*Ap).am_typ.typcollation;
        /* if an array type, assume 1-dimensional attribute */
        if OidIsValid((*Ap).am_typ.typelem)
            && (*Ap).am_typ.typlen < 0
        {
            (*attrtypes[attnum as usize]).attndims = 1;
        } else {
            (*attrtypes[attnum as usize]).attndims = 0;
        }
    } else {
        let ti = typ_info(typeoid as usize);
        (*attrtypes[attnum as usize]).atttypid = ti.oid;
        (*attrtypes[attnum as usize]).attlen = ti.len;
        (*attrtypes[attnum as usize]).attbyval = ti.byval;
        (*attrtypes[attnum as usize]).attalign = ti.align;
        (*attrtypes[attnum as usize]).attstorage = ti.storage;
        (*attrtypes[attnum as usize]).attcompression = InvalidCompressionMethod;
        (*attrtypes[attnum as usize]).attcollation = ti.collation;
        /* if an array type, assume 1-dimensional attribute */
        if OidIsValid(ti.elem) && (*attrtypes[attnum as usize]).attlen < 0 {
            (*attrtypes[attnum as usize]).attndims = 1;
        } else {
            (*attrtypes[attnum as usize]).attndims = 0;
        }
    }

    /*
     * If a system catalog column is collation-aware, force it to use C
     * collation, so that its behavior is independent of the database's
     * collation.  This is essential to allow template0 to be cloned with a
     * different database collation.
     */
    if OidIsValid((*attrtypes[attnum as usize]).attcollation) {
        (*attrtypes[attnum as usize]).attcollation = C_COLLATION_OID;
    }

    (*attrtypes[attnum as usize]).atttypmod = -1;
    (*attrtypes[attnum as usize]).attislocal = true;

    if nullness == BOOTCOL_NULL_FORCE_NOT_NULL {
        (*attrtypes[attnum as usize]).attnotnull = true;
    } else if nullness == BOOTCOL_NULL_FORCE_NULL {
        (*attrtypes[attnum as usize]).attnotnull = false;
    } else {
        Assert!(nullness == BOOTCOL_NULL_AUTO);

        /*
         * Mark as "not null" if type is fixed-width and prior columns are
         * likewise fixed-width and not-null.  This corresponds to case where
         * column can be accessed directly via C struct declaration.
         */
        if (*attrtypes[attnum as usize]).attlen > 0 {
            let mut j: c_int = 0;
            /* check earlier attributes */
            while j < attnum {
                if (*attrtypes[j as usize]).attlen <= 0
                    || !(*attrtypes[j as usize]).attnotnull
                {
                    break;
                }
                j += 1;
            }
            if j == attnum {
                (*attrtypes[attnum as usize]).attnotnull = true;
            }
        }
    }
}

/* ----------------
 *    InsertOneTuple
 *
 * If objectid is not zero, it is a specific OID to assign to the tuple.
 * Otherwise, an OID will be assigned (if necessary) by heap_insert.
 * ----------------
 */
pub unsafe fn InsertOneTuple() {
    let tuple: HeapTuple;
    let tup_desc: TupleDesc;
    let mut i: c_int;

    elog!(DEBUG4, "inserting row with {} columns", numattr);

    tup_desc = CreateTupleDesc(numattr, attrtypes.as_mut_ptr());
    tuple = heap_form_tuple(tup_desc, values.as_mut_ptr(), Nulls.as_mut_ptr());
    pfree(tup_desc as *mut c_void); /* just free's tupDesc, not the attrtypes */

    simple_heap_insert(boot_reldesc, tuple);
    heap_freetuple(tuple);
    elog!(DEBUG4, "row inserted");

    /*
     * Reset null markers for next tuple
     */
    i = 0;
    while i < numattr {
        Nulls[i as usize] = false;
        i += 1;
    }
}

/* ----------------
 *    InsertOneValue
 * ----------------
 */
pub unsafe fn InsertOneValue(value: *mut c_char, i: c_int) {
    let typoid: Oid;
    let mut typlen: i16 = 0;
    let mut typbyval: bool = false;
    let mut typalign: c_char = 0;
    let mut typdelim: c_char = 0;
    let mut typioparam: Oid = 0;
    let mut typinput: Oid = 0;
    let mut typoutput: Oid = 0;

    Assert!(i >= 0 && i < MAXATTR as c_int);

    elog!(DEBUG4, "inserting column {} value {:?}", i, value);

    typoid = (*TupleDescAttr((*boot_reldesc).rd_att, i)).atttypid;

    boot_get_type_io_data(
        typoid,
        &raw mut typlen,
        &raw mut typbyval,
        &raw mut typalign,
        &raw mut typdelim,
        &raw mut typioparam,
        &raw mut typinput,
        &raw mut typoutput,
    );

    values[i as usize] = OidInputFunctionCall(typinput, value, typioparam, -1);

    /*
     * We use ereport not elog here so that parameters aren't evaluated unless
     * the message is going to be printed, which generally it isn't
     */
    ereport!(
        DEBUG4,
        errmsg_internal!(
            "inserted -> {}",
            core::ffi::CStr::from_ptr(OidOutputFunctionCall(typoutput, values[i as usize]))
                .to_string_lossy()
        )
    );
}

/* ----------------
 *    InsertOneNull
 * ----------------
 */
pub unsafe fn InsertOneNull(i: c_int) {
    elog!(DEBUG4, "inserting column {} NULL", i);
    Assert!(i >= 0 && i < MAXATTR as c_int);
    if (*TupleDescAttr((*boot_reldesc).rd_att, i)).attnotnull {
        elog!(
            ERROR,
            "NULL value specified for not-null column \"{:?}\" of relation \"{:?}\"",
            (*TupleDescAttr((*boot_reldesc).rd_att, i)).attname.data.as_ptr(),
            RelationGetRelationName(boot_reldesc)
        );
    }
    values[i as usize] = PointerGetDatum(core::ptr::null());
    Nulls[i as usize] = true;
}

/* ----------------
 *    boot_get_type_io_data
 *
 * Obtain type I/O information at bootstrap time.  This intentionally has
 * almost the same API as lsyscache.c's get_type_io_data, except that
 * we only support obtaining the typinput and typoutput routines, not
 * the binary I/O routines.  It is exported so that array_in and array_out
 * can be made to work during early bootstrap.
 * ----------------
 */
pub unsafe fn boot_get_type_io_data(
    typid: Oid,
    typlen: *mut i16,
    typbyval: *mut bool,
    typalign: *mut c_char,
    typdelim: *mut c_char,
    typioparam: *mut Oid,
    typinput: *mut Oid,
    typoutput: *mut Oid,
) {
    if Typ != NIL {
        /* We have the boot-time contents of pg_type, so use it */
        let mut ap: *mut Typmap = core::ptr::null_mut();
        let len = (*Typ).length as usize;
        let elems = (*Typ).elements;
        let mut found = false;
        'search: for idx in 0..len {
            ap = (*elems.add(idx)).ptr_value as *mut Typmap;
            if (*ap).am_oid == typid {
                found = true;
                break 'search;
            }
        }

        if !found || (*ap).am_oid != typid {
            elog!(ERROR, "type OID {} not found in Typ list", typid);
        }

        *typlen = (*ap).am_typ.typlen;
        *typbyval = (*ap).am_typ.typbyval;
        *typalign = (*ap).am_typ.typalign;
        *typdelim = (*ap).am_typ.typdelim;

        /* XXX this logic must match getTypeIOParam() */
        if OidIsValid((*ap).am_typ.typelem) {
            *typioparam = (*ap).am_typ.typelem;
        } else {
            *typioparam = typid;
        }

        *typinput = (*ap).am_typ.typinput;
        *typoutput = (*ap).am_typ.typoutput;
    } else {
        /* We don't have pg_type yet, so use the hard-wired TYP_INFO array */
        let mut typeindex = N_TYPES; /* sentinel */
        for j in 0..N_TYPES {
            if typ_info(j).oid == typid {
                typeindex = j;
                break;
            }
        }
        if typeindex >= N_TYPES {
            elog!(ERROR, "type OID {} not found in TypInfo", typid);
        }

        let ti = typ_info(typeindex);
        *typlen = ti.len;
        *typbyval = ti.byval;
        *typalign = ti.align;
        /* We assume typdelim is ',' for all boot-time types */
        *typdelim = b',' as c_char;

        /* XXX this logic must match getTypeIOParam() */
        if OidIsValid(ti.elem) {
            *typioparam = ti.elem;
        } else {
            *typioparam = typid;
        }

        *typinput = ti.inproc;
        *typoutput = ti.outproc;
    }
}

/*
 * index_register() -- record an index that has been set up for building
 *                     later.
 *
 *   At bootstrap time, we define a bunch of indexes on system catalogs.
 *   We postpone actually building the indexes until just before we're
 *   finished with initialization, however.  This is because the indexes
 *   themselves have catalog entries, and those have to be included in the
 *   indexes on those catalogs.  Doing it in two phases is the simplest
 *   way of making sure the indexes have the right contents at the end.
 */
pub unsafe fn index_register(heap: Oid, ind: Oid, index_info: *const IndexInfo) {
    let newind: *mut IndexList;
    let oldcxt: MemoryContext;

    /*
     * XXX mao 10/31/92 -- don't gc index reldescs, associated info at
     * bootstrap time.  we'll declare the indexes now, but want to create them
     * later.
     */

    if nogc.is_null() {
        // C passes NULL as parent (top-level context). The shim macro returns the
        // parent unchanged, so we use TopMemoryContext so nogc is non-null and
        // MemoryContextSwitchTo/MemoryContextAllocZero work correctly.
        nogc = AllocSetContextCreate!(
            TopMemoryContext,
            c"BootstrapNoGC".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
    }

    oldcxt = MemoryContextSwitchTo(nogc);

    newind = palloc(core::mem::size_of::<IndexList>()) as *mut IndexList;
    (*newind).il_heap = heap;
    (*newind).il_ind = ind;
    (*newind).il_info = palloc(core::mem::size_of::<IndexInfo>()) as *mut IndexInfo;

    core::ptr::copy_nonoverlapping(index_info, (*newind).il_info, 1);
    /* expressions will likely be null, but may as well copy it */
    (*(*newind).il_info).ii_Expressions =
        copyObject((*index_info).ii_Expressions as *const c_void) as *mut List;
    (*(*newind).il_info).ii_ExpressionsState = NIL;
    /* predicate will likely be null, but may as well copy it */
    (*(*newind).il_info).ii_Predicate =
        copyObject((*index_info).ii_Predicate as *const c_void) as *mut List;
    (*(*newind).il_info).ii_PredicateState = core::ptr::null_mut();
    /* no exclusion constraints at bootstrap time, so no need to copy */
    Assert!((*index_info).ii_ExclusionOps.is_null());
    Assert!((*index_info).ii_ExclusionProcs.is_null());
    Assert!((*index_info).ii_ExclusionStrats.is_null());

    (*newind).il_next = ILHead;
    ILHead = newind;

    MemoryContextSwitchTo(oldcxt);
}

/*
 * build_indices -- fill in all the indexes registered earlier
 */
pub unsafe fn build_indices() {
    while !ILHead.is_null() {
        let heap: Relation;
        let ind: Relation;

        /* need not bother with locks during bootstrap */
        heap = table_open((*ILHead).il_heap, NoLock);
        ind = index_open((*ILHead).il_ind, NoLock);

        index_build(heap, ind, (*ILHead).il_info, false, false);

        index_close(ind, NoLock);
        table_close(heap, NoLock);

        ILHead = (*ILHead).il_next;
    }
}

// ---- small C stdlib shims used within this module ---------------------------

#[inline]
unsafe fn libc_strlen(s: *const c_char) -> usize {
    extern "C" {
        fn strlen(s: *const c_char) -> usize;
    }
    strlen(s)
}

#[inline]
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    extern "C" {
        fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    }
    strcmp(a, b)
}

//--------------------------------------------------------------------
//
// guc_funcs.c
//
// SQL commands and SQL-accessible functions related to GUC variables.
//
//
// Copyright (c) 2000-2025, PostgreSQL Global Development Group
// Written by Peter Eisentraut <peter_e@gmx.net>.
//
// IDENTIFICATION
//	  src/backend/utils/misc/guc_funcs.c
//
//--------------------------------------------------------------------

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::int32;

// List/Node/NodeTag and the node-tag helper.
use crate::nodes::nodes::{nodeTag, Node, NodeTag};
use crate::nodes::pg_list::{lfirst, linitial, list_head, list_length, List};

// AttrNumber lives in access/attnum.
use crate::access::attnum::AttrNumber;

// Function-call plumbing (fmgr.h).
use crate::utils::fmgr::FunctionCallInfo;

// text<->cstring helpers (builtins.h).
use crate::utils::builtins::{CStringGetTextDatum, TextDatumGetCString};

// #[macro_export] macros live at the crate root; import them so the `!`-call
// sites below resolve.
use crate::{
    appendStringInfo, castNode, current_cell, foreach, intVal, strVal, IsA, linitial_node,
    list_make1, DirectFunctionCall1, DirectFunctionCall3, PG_ARGISNULL, PG_GETARG_BOOL,
    PG_GETARG_DATUM, PG_RETURN_NULL, PG_RETURN_TEXT_P,
};

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

// PG_RETURN_ARRAYTYPE_P(x): not yet ported to the crate root; in C this is
// `return PointerGetDatum(x)`.  Provide a faithful local macro.
macro_rules! PG_RETURN_ARRAYTYPE_P {
    ($x:expr) => {
        return PointerGetDatum($x as *const c_void)
    };
}

// SRF_* set-returning-function macros: not yet ported to the crate root.
// Mirror funcapi.h by delegating to local stubs.
macro_rules! SRF_IS_FIRSTCALL {
    ($fcinfo:expr) => {
        srf_is_firstcall($fcinfo)
    };
}
macro_rules! SRF_FIRSTCALL_INIT {
    ($fcinfo:expr) => {
        srf_firstcall_init($fcinfo)
    };
}
macro_rules! SRF_PERCALL_SETUP {
    ($fcinfo:expr) => {
        srf_percall_setup($fcinfo)
    };
}
macro_rules! SRF_RETURN_NEXT {
    ($fctx:expr, $result:expr) => {
        return srf_return_next($fctx, $result)
    };
}
macro_rules! SRF_RETURN_DONE {
    ($fctx:expr) => {
        return srf_return_done($fctx)
    };
}

// ---- Local stub types (faithful structure; resolved across module boundaries) ----

pub type DestReceiver = c_void;
pub type TupleDesc = *mut c_void;
pub type TupOutputState = c_void;
pub type ArrayType = c_void;
pub type AttInMetadata = c_void;
pub type FuncCallContext = c_void;
pub type ReturnSetInfo = c_void;
pub type HeapTuple = *mut c_void;
pub type ConfigVariable = c_void;

#[repr(C)]
pub struct config_generic {
    pub name: *const c_char,
    pub flags: c_int,
    pub group: c_int,
    pub short_desc: *const c_char,
    pub long_desc: *const c_char,
    pub context: c_int,
    pub vartype: c_int,
    pub source: c_int,
    pub status: c_int,
    pub sourcefile: *const c_char,
    pub sourceline: c_int,
}

#[repr(C)]
pub struct config_bool {
    pub gen: config_generic,
    pub boot_val: bool,
    pub reset_val: bool,
}

#[repr(C)]
pub struct config_int {
    pub gen: config_generic,
    pub boot_val: c_int,
    pub reset_val: c_int,
    pub min: c_int,
    pub max: c_int,
}

#[repr(C)]
pub struct config_real {
    pub gen: config_generic,
    pub boot_val: f64,
    pub reset_val: f64,
    pub min: f64,
    pub max: f64,
}

#[repr(C)]
pub struct config_string {
    pub gen: config_generic,
    pub boot_val: *mut c_char,
    pub reset_val: *mut c_char,
}

#[repr(C)]
pub struct config_enum {
    pub gen: config_generic,
    pub boot_val: c_int,
    pub reset_val: c_int,
}

// VariableSetStmt and related nodes (faithful subset of fields used here)
pub type GucAction = c_int;
pub const GUC_ACTION_SET: GucAction = 0;
pub const GUC_ACTION_LOCAL: GucAction = 1;
pub const GUC_ACTION_SAVE: GucAction = 2;

pub type GucContext = c_int;
pub const PGC_SUSET: GucContext = 0;
pub const PGC_USERSET: GucContext = 0;

pub type GucSource = c_int;
pub const PGC_S_SESSION: GucSource = 0;
pub const PGC_S_FILE: GucSource = 0;
pub const PGC_SIGHUP: GucContext = 0;

pub type VariableSetKind = c_int;
pub const VAR_SET_VALUE: VariableSetKind = 0;
pub const VAR_SET_DEFAULT: VariableSetKind = 1;
pub const VAR_SET_CURRENT: VariableSetKind = 2;
pub const VAR_SET_MULTI: VariableSetKind = 3;
pub const VAR_RESET: VariableSetKind = 4;
pub const VAR_RESET_ALL: VariableSetKind = 5;

#[repr(C)]
pub struct VariableSetStmt {
    pub r#type: c_int, // NodeTag (must be present so kind/name/args land at the right offsets)
    pub kind: VariableSetKind,
    pub name: *mut c_char,
    pub args: *mut List,
    pub is_local: bool,
}

#[repr(C)]
pub struct DefElem {
    pub defname: *mut c_char,
    pub arg: *mut Node,
}

#[repr(C)]
pub struct TypeName {
    pub dummy: c_int,
}

#[repr(C)]
pub struct TypeCast {
    pub arg: *mut Node,
    pub typeName: *mut TypeName,
}

#[repr(C)]
pub struct A_Const {
    pub r#type: c_int, // NodeTag
    pub _pad: c_int,   // ValUnion is 8-aligned in C; pad so `val` lands at offset 8
    pub val: Node,
    pub isnull: bool,
    pub location: c_int,
}

// Float node from value.rs
#[repr(C)]
pub struct Float {
    pub fval: *mut c_char,
}

// GUC flag bits
pub const GUC_LIST_INPUT: c_int = 0x0001;
pub const GUC_LIST_QUOTE: c_int = 0x0002;
pub const GUC_NO_SHOW_ALL: c_int = 0x0008;
pub const GUC_SUPERUSER_ONLY: c_int = 0x0100;
pub const GUC_EXPLAIN: c_int = 0x100000;
pub const GUC_NO_RESET: c_int = 0x400000;
pub const GUC_NO_RESET_ALL: c_int = 0x20000;
pub const GUC_NOT_IN_SAMPLE: c_int = 0x0004;
pub const GUC_RUNTIME_COMPUTED: c_int = 0x4000000;
pub const GUC_PENDING_RESTART: c_int = 0x0002;

pub const PGC_BOOL: c_int = 0;
pub const PGC_INT: c_int = 1;
pub const PGC_REAL: c_int = 2;
pub const PGC_STRING: c_int = 3;
pub const PGC_ENUM: c_int = 4;

pub const MAX_GUC_FLAGS: usize = 6;
pub const NUM_PG_SETTINGS_ATTS: usize = 17;
pub const NUM_PG_FILE_SETTINGS_ATTS: usize = 7;

// ---- local stubs for unported helpers ----

unsafe fn IsInParallelMode() -> bool {
    crate::access::transam::xact::IsInParallelMode_real()
}
unsafe fn WarnNoTransactionBlock(isTopLevel: bool, stmtType: *const c_char) {
    crate::access::transam::xact::WarnNoTransactionBlock(isTopLevel, stmtType)
}
unsafe fn set_config_option(
    name: *const c_char,
    value: *const c_char,
    context: GucContext,
    source: GucSource,
    action: GucAction,
    changeVal: bool,
    elevel: c_int,
    is_reload: bool,
) -> c_int {
    crate::utils::misc::guc::set_config_option(
        name,
        value,
        std::mem::transmute(context),
        std::mem::transmute(source),
        std::mem::transmute(action),
        changeVal,
        elevel,
        is_reload,
    )
}
unsafe fn superuser() -> bool {
    crate::utils::misc::superuser::superuser()
}
unsafe fn ExtractSetVariableArgs_stub() {}
unsafe fn ImportSnapshot(_idstr: *const c_char) {
    unimplemented!() // TODO: utils/time/snapmgr.c (real impl exists but pulls in a
                     // broken `fstat$INODE64` link-name in snapmgr.rs that fails to
                     // link on modern macOS; forward once that is fixed)
}
unsafe fn ResetAllOptions() {
    crate::utils::misc::guc::ResetAllOptions()
}
unsafe fn InvokeObjectPostAlterHookArgStr(
    _classId: Oid,
    _objectName: *const c_char,
    _subId: c_int,
    _auxiliaryId: c_int,
    _is_internal: bool,
) {
    // object_access_hook is null unless an extension (e.g. sepgsql) registers it;
    // a no-op is the correct default behavior.
}
unsafe fn GetConfigOptionByName(
    name: *const c_char,
    varname: *mut *const c_char,
    missing_ok: bool,
) -> *mut c_char {
    crate::utils::misc::guc::GetConfigOptionByName(name, varname, missing_ok)
}
unsafe fn find_option(
    name: *const c_char,
    create_placeholders: bool,
    skip_errors: bool,
    elevel: c_int,
) -> *mut config_generic {
    crate::utils::misc::guc::find_option(name, create_placeholders, skip_errors, elevel) as _
}
unsafe fn initStringInfo(str: *mut StringInfoData) {
    crate::lib::stringinfo::initStringInfo(str as _)
}
unsafe fn appendStringInfoString(str: *mut StringInfoData, s: *const c_char) {
    crate::lib::stringinfo::appendStringInfoString(str as _, s)
}
unsafe fn quote_identifier(ident: *const c_char) -> *const c_char {
    // Minimal port of ruleutils.c quote_identifier: return as-is if it's a safe
    // bare identifier ([a-z_][a-z0-9_]*), else return a double-quoted palloc'd copy.
    // (Keyword check is omitted; GUC names that reach here are safe lowercase.)
    let bytes = std::ffi::CStr::from_ptr(ident).to_bytes();
    let safe = !bytes.is_empty()
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes.iter().all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_');
    if safe {
        return ident;
    }
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len() + 4);
    out.push(b'"');
    for &c in bytes {
        if c == b'"' {
            out.push(b'"');
        }
        out.push(c);
    }
    out.push(b'"');
    out.push(0);
    let p = crate::utils::palloc::palloc(out.len()) as *mut c_char;
    core::ptr::copy_nonoverlapping(out.as_ptr(), p as *mut u8, out.len());
    p
}
unsafe fn typenameTypeIdAndMod(
    pstate: *mut c_void,
    typeName: *mut TypeName,
    typeid_p: *mut Oid,
    typmod_p: *mut int32,
) {
    crate::parser::parse_type::typenameTypeIdAndMod(pstate as _, typeName as _, typeid_p, typmod_p)
}
unsafe fn interval_in(fcinfo: FunctionCallInfo) -> Datum {
    crate::utils::adt::timestamp::interval_in(fcinfo)
}
unsafe fn interval_out(fcinfo: FunctionCallInfo) -> Datum {
    crate::utils::adt::timestamp::interval_out(fcinfo)
}
unsafe fn guc_name_compare(namea: *const c_char, nameb: *const c_char) -> c_int {
    crate::utils::misc::guc::guc_name_compare_c(namea, nameb)
}
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc {
    unimplemented!() // TODO: access/common/tupdesc.c
}
unsafe fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _attributeName: *const c_char,
    _oidtypeid: Oid,
    _typmod: int32,
    _attdim: c_int,
) {
    unimplemented!() // TODO: access/common/tupdesc.c
}
unsafe fn TupleDescInitBuiltinEntry(
    _desc: TupleDesc,
    _attributeNumber: AttrNumber,
    _attributeName: *const c_char,
    _oidtypeid: Oid,
    _typmod: int32,
    _attdim: c_int,
) {
    unimplemented!() // TODO: access/common/tupdesc.c
}
unsafe fn begin_tup_output_tupdesc(
    _dest: *mut DestReceiver,
    _tupdesc: TupleDesc,
    _tts_ops: *const c_void,
) -> *mut TupOutputState {
    unimplemented!() // TODO: access/common/printtup.c
}
unsafe fn do_text_output_oneline(_tstate: *mut TupOutputState, _str_to_emit: *const c_char) {
    unimplemented!() // TODO: access/common/printtup.c
}
unsafe fn end_tup_output(_tstate: *mut TupOutputState) {
    unimplemented!() // TODO: access/common/printtup.c
}
unsafe fn do_tup_output(_tstate: *mut TupOutputState, _values: *mut Datum, _isnull: *mut bool) {
    unimplemented!() // TODO: access/common/printtup.c
}
unsafe fn get_guc_variables(_num_vars: *mut c_int) -> *mut *mut config_generic {
    unimplemented!() // TODO: utils/misc/guc.c
}
unsafe fn ShowGUCOption(_record: *mut config_generic, _use_units: bool) -> *mut c_char {
    unimplemented!() // TODO: utils/misc/guc.c
}
unsafe fn cstring_to_text(_s: *const c_char) -> *mut c_void {
    unimplemented!() // TODO: utils/adt/varlena.c
}
unsafe fn construct_array_builtin(_elems: *mut Datum, _nelems: c_int, _elmtype: Oid) -> *mut ArrayType {
    unimplemented!() // TODO: utils/adt/arrayfuncs.c
}
unsafe fn has_privs_of_role(_member: Oid, _role: Oid) -> bool {
    unimplemented!() // TODO: utils/adt/acl.c
}
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn get_config_unit_name(_flags: c_int) -> *const c_char {
    unimplemented!() // TODO: utils/misc/guc.c
}
unsafe fn config_enum_get_options(
    _record: *mut config_enum,
    _prefix: *const c_char,
    _suffix: *const c_char,
    _separator: *const c_char,
) -> *mut c_char {
    unimplemented!() // TODO: utils/misc/guc.c
}
unsafe fn config_enum_lookup_by_value(_record: *mut config_enum, _val: c_int) -> *const c_char {
    unimplemented!() // TODO: utils/misc/guc.c
}
unsafe fn TupleDescGetAttInMetadata(_tupdesc: TupleDesc) -> *mut AttInMetadata {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn BuildTupleFromCStrings(_attinmeta: *mut AttInMetadata, _values: *mut *mut c_char) -> HeapTuple {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn ProcessConfigFileInternal(_context: GucContext, _applySettings: bool, _elevel: c_int) -> *mut ConfigVariable {
    unimplemented!() // TODO: utils/misc/guc-file.l
}
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn tuplestore_putvalues(
    _state: *mut c_void,
    _tdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}

// StringInfo stub
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}

// Oid constants
pub const TEXTOID: Oid = 25;
pub const TEXTARRAYOID: Oid = 1009;
pub const INT4OID: Oid = 23;
pub const BOOLOID: Oid = 16;
pub const INTERVALOID: Oid = 1186;
pub const ParameterAclRelationId: Oid = 6243;
pub const ROLE_PG_READ_ALL_SETTINGS: Oid = 3374;
pub const ACL_SET: c_int = 1 << 11;

use crate::executor::execTuples::TTSOpsVirtual;

// config_group_names / GucContext_Names / config_type_names / GucSource_Names externs
extern "C" {
    static config_group_names: *const *const c_char;
    static GucContext_Names: *const *const c_char;
    static config_type_names: *const *const c_char;
    static GucSource_Names: *const *const c_char;
}

// C's `_(x)` gettext translation macro; modeled here as an identity passthrough
// (`_` is a reserved wildcard in Rust and cannot name a function).
unsafe fn gettext(s: *const c_char) -> *const c_char {
    s
}

static mut DUMMY_FLATTEN: () = ();

/*
 * SET command
 */
#[no_mangle]
pub unsafe extern "C" fn ExecSetVariableStmt(stmt: *mut VariableSetStmt, isTopLevel: bool) {
    let action: GucAction = if (*stmt).is_local {
        GUC_ACTION_LOCAL
    } else {
        GUC_ACTION_SET
    };

    /*
     * Workers synchronize these parameters at the start of the parallel
     * operation; then, we block SET during the operation.
     */
    if IsInParallelMode() {
        ereport!(ERROR, "cannot set parameters during a parallel operation");
    }

    if std::env::var_os("PDB_BT").is_some() {
        let nm = if (*stmt).name.is_null() { "<null>".to_string() } else { std::ffi::CStr::from_ptr((*stmt).name).to_string_lossy().into_owned() };
        eprintln!("PDB_BT ExecSetVariableStmt kind={} name={}", (*stmt).kind as i32, nm);
    }
    match (*stmt).kind {
        VAR_SET_VALUE | VAR_SET_CURRENT => {
            if (*stmt).is_local {
                WarnNoTransactionBlock(isTopLevel, c"SET LOCAL".as_ptr());
            }
            let _ = set_config_option(
                (*stmt).name,
                ExtractSetVariableArgs(stmt),
                if superuser() { PGC_SUSET } else { PGC_USERSET },
                PGC_S_SESSION,
                action,
                true,
                0,
                false,
            );
        }
        VAR_SET_MULTI => {
            /*
             * Special-case SQL syntaxes.  The TRANSACTION and SESSION
             * CHARACTERISTICS cases effectively set more than one variable
             * per statement.  TRANSACTION SNAPSHOT only takes one argument,
             * but we put it here anyway since it's a special case and not
             * related to any GUC variable.
             */
            if strcmp((*stmt).name, c"TRANSACTION".as_ptr()) == 0 {
                WarnNoTransactionBlock(isTopLevel, c"SET TRANSACTION".as_ptr());

                foreach!(head, (*stmt).args, {
                    let item = lfirst(current_cell!(head)) as *mut DefElem;

                    if strcmp((*item).defname, c"transaction_isolation".as_ptr()) == 0 {
                        SetPGVariable(
                            c"transaction_isolation".as_ptr(),
                            list_make1!((*item).arg as *mut c_void),
                            (*stmt).is_local,
                        );
                    } else if strcmp((*item).defname, c"transaction_read_only".as_ptr()) == 0 {
                        SetPGVariable(
                            c"transaction_read_only".as_ptr(),
                            list_make1!((*item).arg as *mut c_void),
                            (*stmt).is_local,
                        );
                    } else if strcmp((*item).defname, c"transaction_deferrable".as_ptr()) == 0 {
                        SetPGVariable(
                            c"transaction_deferrable".as_ptr(),
                            list_make1!((*item).arg as *mut c_void),
                            (*stmt).is_local,
                        );
                    } else {
                        elog!(
                            ERROR,
                            "unexpected SET TRANSACTION element: {}",
                            cstr_display((*item).defname)
                        );
                    }
                });
            } else if strcmp((*stmt).name, c"SESSION CHARACTERISTICS".as_ptr()) == 0 {
                foreach!(head, (*stmt).args, {
                    let item = lfirst(current_cell!(head)) as *mut DefElem;

                    if strcmp((*item).defname, c"transaction_isolation".as_ptr()) == 0 {
                        SetPGVariable(
                            c"default_transaction_isolation".as_ptr(),
                            list_make1!((*item).arg as *mut c_void),
                            (*stmt).is_local,
                        );
                    } else if strcmp((*item).defname, c"transaction_read_only".as_ptr()) == 0 {
                        SetPGVariable(
                            c"default_transaction_read_only".as_ptr(),
                            list_make1!((*item).arg as *mut c_void),
                            (*stmt).is_local,
                        );
                    } else if strcmp((*item).defname, c"transaction_deferrable".as_ptr()) == 0 {
                        SetPGVariable(
                            c"default_transaction_deferrable".as_ptr(),
                            list_make1!((*item).arg as *mut c_void),
                            (*stmt).is_local,
                        );
                    } else {
                        elog!(
                            ERROR,
                            "unexpected SET SESSION element: {}",
                            cstr_display((*item).defname)
                        );
                    }
                });
            } else if strcmp((*stmt).name, c"TRANSACTION SNAPSHOT".as_ptr()) == 0 {
                let con = linitial_node!(A_Const, T_A_Const, (*stmt).args) as *mut A_Const;

                if (*stmt).is_local {
                    ereport!(ERROR, "SET LOCAL TRANSACTION SNAPSHOT is not implemented");
                }

                WarnNoTransactionBlock(isTopLevel, c"SET TRANSACTION".as_ptr());
                ImportSnapshot(strVal!(&raw mut (*con).val));
            } else {
                elog!(
                    ERROR,
                    "unexpected SET MULTI element: {}",
                    cstr_display((*stmt).name)
                );
            }
        }
        VAR_SET_DEFAULT => {
            if (*stmt).is_local {
                WarnNoTransactionBlock(isTopLevel, c"SET LOCAL".as_ptr());
            }
            /* fall through */
            let _ = set_config_option(
                (*stmt).name,
                std::ptr::null(),
                if superuser() { PGC_SUSET } else { PGC_USERSET },
                PGC_S_SESSION,
                action,
                true,
                0,
                false,
            );
        }
        VAR_RESET => {
            let _ = set_config_option(
                (*stmt).name,
                std::ptr::null(),
                if superuser() { PGC_SUSET } else { PGC_USERSET },
                PGC_S_SESSION,
                action,
                true,
                0,
                false,
            );
        }
        VAR_RESET_ALL => {
            ResetAllOptions();
        }
        _ => {}
    }

    /* Invoke the post-alter hook for setting this GUC variable, by name. */
    InvokeObjectPostAlterHookArgStr(
        ParameterAclRelationId,
        (*stmt).name,
        ACL_SET,
        (*stmt).kind,
        false,
    );
}

/*
 * Get the value to assign for a VariableSetStmt, or NULL if it's RESET.
 * The result is palloc'd.
 *
 * This is exported for use by actions such as ALTER ROLE SET.
 */
#[no_mangle]
pub unsafe extern "C" fn ExtractSetVariableArgs(stmt: *mut VariableSetStmt) -> *mut c_char {
    match (*stmt).kind {
        VAR_SET_VALUE => flatten_set_variable_args((*stmt).name, (*stmt).args),
        VAR_SET_CURRENT => GetConfigOptionByName((*stmt).name, std::ptr::null_mut(), false),
        _ => std::ptr::null_mut(),
    }
}

/*
 * flatten_set_variable_args
 *		Given a parsenode List as emitted by the grammar for SET,
 *		convert to the flat string representation used by GUC.
 *
 * We need to be told the name of the variable the args are for, because
 * the flattening rules vary (ugh).
 *
 * The result is NULL if args is NIL (i.e., SET ... TO DEFAULT), otherwise
 * a palloc'd string.
 */
unsafe fn flatten_set_variable_args(name: *const c_char, args: *mut List) -> *mut c_char {
    let record: *mut config_generic;
    let flags: c_int;
    let mut buf: StringInfoData = std::mem::zeroed();

    /* Fast path if just DEFAULT */
    if args.is_null() {
        return std::ptr::null_mut();
    }

    /*
     * Get flags for the variable; if it's not known, use default flags.
     * (Caller might throw error later, but not our business to do so here.)
     */
    record = find_option(name, false, true, WARNING);
    if !record.is_null() {
        flags = (*record).flags;
    } else {
        flags = 0;
    }

    /* Complain if list input and non-list variable */
    if (flags & GUC_LIST_INPUT) == 0 && list_length(args) != 1 {
        elog!(ERROR, "SET {} takes only one argument", cstr_display(name));
    }

    initStringInfo(&mut buf);

    /*
     * Each list member may be a plain A_Const node, or an A_Const within a
     * TypeCast; the latter case is supported only for ConstInterval arguments
     * (for SET TIME ZONE).
     */
    foreach!(l, args, {
        let mut arg = lfirst(current_cell!(l)) as *mut Node;
        let val: *mut c_char;
        let mut typeName: *mut TypeName = std::ptr::null_mut();
        let con: *mut A_Const;

        if current_cell!(l) != list_head(args) {
            appendStringInfoString(&mut buf, c", ".as_ptr());
        }

        if IsA!(arg, T_TypeCast) {
            let tc = arg as *mut TypeCast;

            arg = (*tc).arg;
            typeName = (*tc).typeName;
        }

        if std::env::var_os("PDB_BT").is_some() {
            eprintln!("PDB_BT ExtractSetVar arg_tag={} isA_AConst={} T_A_Const={}",
                nodeTag(arg) as c_int, IsA!(arg, T_A_Const), NodeTag::T_A_Const as c_int);
        }
        if !IsA!(arg, T_A_Const) {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(arg) as c_int);
        }
        con = arg as *mut A_Const;

        match nodeTag(&(*con).val) {
            NodeTag::T_Integer => {
                appendStringInfo!(&raw mut buf as *mut _, "{}", intVal!(&raw mut (*con).val));
            }
            NodeTag::T_Float => {
                /* represented as a string, so just copy it */
                appendStringInfoString(
                    &mut buf,
                    (*(castNode!(Float, T_Float, &raw mut (*con).val) as *mut Float)).fval,
                );
            }
            NodeTag::T_String => {
                val = strVal!(&raw mut (*con).val);
                if !typeName.is_null() {
                    /*
                     * Must be a ConstInterval argument for TIME ZONE. Coerce
                     * to interval and back to normalize the value and account
                     * for any typmod.
                     */
                    let typoid: Oid = 0;
                    let mut typoid = typoid;
                    let mut typmod: int32 = 0;
                    let interval: Datum;
                    let intervalout: *mut c_char;

                    typenameTypeIdAndMod(
                        std::ptr::null_mut(),
                        typeName,
                        &mut typoid,
                        &mut typmod,
                    );
                    Assert!(typoid == INTERVALOID);

                    interval = DirectFunctionCall3!(
                        interval_in,
                        CStringGetDatum(val),
                        ObjectIdGetDatum(InvalidOid),
                        Int32GetDatum(typmod)
                    );

                    intervalout =
                        DatumGetCString(DirectFunctionCall1!(interval_out, interval));
                    appendStringInfo!(&raw mut buf as *mut _, "INTERVAL '{}'", cstr_display(intervalout));
                } else {
                    /*
                     * Plain string literal or identifier.  For quote mode,
                     * quote it if it's not a vanilla identifier.
                     */
                    if flags & GUC_LIST_QUOTE != 0 {
                        appendStringInfoString(&mut buf, quote_identifier(val));
                    } else {
                        appendStringInfoString(&mut buf, val);
                    }
                }
            }
            _ => {
                elog!(
                    ERROR,
                    "unrecognized node type: {}",
                    nodeTag(&(*con).val) as c_int
                );
            }
        }
    });

    buf.data
}

/*
 * SetPGVariable - SET command exported as an easily-C-callable function.
 *
 * This provides access to SET TO value, as well as SET TO DEFAULT (expressed
 * by passing args == NIL), but not SET FROM CURRENT functionality.
 */
#[no_mangle]
pub unsafe extern "C" fn SetPGVariable(name: *const c_char, args: *mut List, is_local: bool) {
    let argstring = flatten_set_variable_args(name, args);

    /* Note SET DEFAULT (argstring == NULL) is equivalent to RESET */
    let _ = set_config_option(
        name,
        argstring,
        if superuser() { PGC_SUSET } else { PGC_USERSET },
        PGC_S_SESSION,
        if is_local {
            GUC_ACTION_LOCAL
        } else {
            GUC_ACTION_SET
        },
        true,
        0,
        false,
    );
}

/*
 * SET command wrapped as a SQL callable function.
 */
#[no_mangle]
pub unsafe extern "C" fn set_config_by_name(fcinfo: FunctionCallInfo) -> Datum {
    let name: *mut c_char;
    let value: *mut c_char;
    let new_value: *mut c_char;
    let is_local: bool;

    if PG_ARGISNULL!(fcinfo, 0) {
        ereport!(ERROR, "SET requires parameter name");
    }

    /* Get the GUC variable name */
    name = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, 0));

    /* Get the desired value or set to NULL for a reset request */
    if PG_ARGISNULL!(fcinfo, 1) {
        value = std::ptr::null_mut();
    } else {
        value = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, 1));
    }

    /*
     * Get the desired state of is_local. Default to false if provided value
     * is NULL
     */
    if PG_ARGISNULL!(fcinfo, 2) {
        is_local = false;
    } else {
        is_local = PG_GETARG_BOOL!(fcinfo, 2);
    }

    /* Note SET DEFAULT (argstring == NULL) is equivalent to RESET */
    let _ = set_config_option(
        name,
        value,
        if superuser() { PGC_SUSET } else { PGC_USERSET },
        PGC_S_SESSION,
        if is_local {
            GUC_ACTION_LOCAL
        } else {
            GUC_ACTION_SET
        },
        true,
        0,
        false,
    );

    /* get the new current value */
    new_value = GetConfigOptionByName(name, std::ptr::null_mut(), false);

    /* Convert return string to text */
    PG_RETURN_TEXT_P!(cstring_to_text(new_value))
}

/*
 * SHOW command
 */
#[no_mangle]
pub unsafe extern "C" fn GetPGVariable(name: *const c_char, dest: *mut DestReceiver) {
    if guc_name_compare(name, c"all".as_ptr()) == 0 {
        ShowAllGUCConfig(dest);
    } else {
        ShowGUCConfigOption(name, dest);
    }
}

/*
 * Get a tuple descriptor for SHOW's result
 */
#[no_mangle]
pub unsafe extern "C" fn GetPGVariableResultDesc(name: *const c_char) -> TupleDesc {
    let tupdesc: TupleDesc;

    if guc_name_compare(name, c"all".as_ptr()) == 0 {
        /* need a tuple descriptor representing three TEXT columns */
        tupdesc = CreateTemplateTupleDesc(3);
        TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"name".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(
            tupdesc,
            2 as AttrNumber,
            c"setting".as_ptr(),
            TEXTOID,
            -1,
            0,
        );
        TupleDescInitEntry(
            tupdesc,
            3 as AttrNumber,
            c"description".as_ptr(),
            TEXTOID,
            -1,
            0,
        );
    } else {
        let mut varname: *const c_char = std::ptr::null();

        /* Get the canonical spelling of name */
        let _ = GetConfigOptionByName(name, &mut varname, false);

        /* need a tuple descriptor representing a single TEXT column */
        tupdesc = CreateTemplateTupleDesc(1);
        TupleDescInitEntry(tupdesc, 1 as AttrNumber, varname, TEXTOID, -1, 0);
    }
    tupdesc
}

/*
 * SHOW one variable
 */
unsafe fn ShowGUCConfigOption(name: *const c_char, dest: *mut DestReceiver) {
    let tstate: *mut TupOutputState;
    let tupdesc: TupleDesc;
    let mut varname: *const c_char = std::ptr::null();
    let value: *mut c_char;

    /* Get the value and canonical spelling of name */
    value = GetConfigOptionByName(name, &mut varname, false);

    /* need a tuple descriptor representing a single TEXT column */
    tupdesc = CreateTemplateTupleDesc(1);
    TupleDescInitBuiltinEntry(tupdesc, 1 as AttrNumber, varname, TEXTOID, -1, 0);

    /* prepare for projection of tuples */
    tstate = begin_tup_output_tupdesc(dest, tupdesc, std::ptr::addr_of!(TTSOpsVirtual) as *const c_void);

    /* Send it */
    do_text_output_oneline(tstate, value);

    end_tup_output(tstate);
}

/*
 * SHOW ALL command
 */
unsafe fn ShowAllGUCConfig(dest: *mut DestReceiver) {
    let guc_vars: *mut *mut config_generic;
    let mut num_vars: c_int = 0;
    let tstate: *mut TupOutputState;
    let tupdesc: TupleDesc;
    let mut values: [Datum; 3] = [0; 3];
    let mut isnull: [bool; 3] = [false, false, false];

    /* collect the variables, in sorted order */
    guc_vars = get_guc_variables(&mut num_vars);

    /* need a tuple descriptor representing three TEXT columns */
    tupdesc = CreateTemplateTupleDesc(3);
    TupleDescInitBuiltinEntry(tupdesc, 1 as AttrNumber, c"name".as_ptr(), TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(
        tupdesc,
        2 as AttrNumber,
        c"setting".as_ptr(),
        TEXTOID,
        -1,
        0,
    );
    TupleDescInitBuiltinEntry(
        tupdesc,
        3 as AttrNumber,
        c"description".as_ptr(),
        TEXTOID,
        -1,
        0,
    );

    /* prepare for projection of tuples */
    tstate = begin_tup_output_tupdesc(dest, tupdesc, std::ptr::addr_of!(TTSOpsVirtual) as *const c_void);

    for i in 0..num_vars {
        let conf = *guc_vars.offset(i as isize);
        let setting: *mut c_char;

        /* skip if marked NO_SHOW_ALL */
        if (*conf).flags & GUC_NO_SHOW_ALL != 0 {
            continue;
        }

        /* return only options visible to the current user */
        if !ConfigOptionIsVisible(conf) {
            continue;
        }

        /* assign to the values array */
        values[0] = PointerGetDatum(cstring_to_text((*conf).name));

        setting = ShowGUCOption(conf, true);
        if !setting.is_null() {
            values[1] = PointerGetDatum(cstring_to_text(setting));
            isnull[1] = false;
        } else {
            values[1] = PointerGetDatum(std::ptr::null_mut());
            isnull[1] = true;
        }

        if !(*conf).short_desc.is_null() {
            values[2] = PointerGetDatum(cstring_to_text((*conf).short_desc));
            isnull[2] = false;
        } else {
            values[2] = PointerGetDatum(std::ptr::null_mut());
            isnull[2] = true;
        }

        /* send it to dest */
        do_tup_output(tstate, values.as_mut_ptr(), isnull.as_mut_ptr());

        /* clean up */
        pfree(DatumGetPointer(values[0]) as *mut c_void);
        if !setting.is_null() {
            pfree(setting as *mut c_void);
            pfree(DatumGetPointer(values[1]) as *mut c_void);
        }
        if !(*conf).short_desc.is_null() {
            pfree(DatumGetPointer(values[2]) as *mut c_void);
        }
    }

    end_tup_output(tstate);
}

/*
 * Return some of the flags associated to the specified GUC in the shape of
 * a text array, and NULL if it does not exist.  An empty array is returned
 * if the GUC exists without any meaningful flags to show.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_settings_get_flags(fcinfo: FunctionCallInfo) -> Datum {
    let varname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, 0));
    let record: *mut config_generic;
    let mut cnt: usize = 0;
    let mut flags: [Datum; MAX_GUC_FLAGS] = [0; MAX_GUC_FLAGS];
    let a: *mut ArrayType;

    record = find_option(varname, false, true, ERROR);

    /* return NULL if no such variable */
    if record.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    if (*record).flags & GUC_EXPLAIN != 0 {
        flags[cnt] = CStringGetTextDatum(c"EXPLAIN".as_ptr());
        cnt += 1;
    }
    if (*record).flags & GUC_NO_RESET != 0 {
        flags[cnt] = CStringGetTextDatum(c"NO_RESET".as_ptr());
        cnt += 1;
    }
    if (*record).flags & GUC_NO_RESET_ALL != 0 {
        flags[cnt] = CStringGetTextDatum(c"NO_RESET_ALL".as_ptr());
        cnt += 1;
    }
    if (*record).flags & GUC_NO_SHOW_ALL != 0 {
        flags[cnt] = CStringGetTextDatum(c"NO_SHOW_ALL".as_ptr());
        cnt += 1;
    }
    if (*record).flags & GUC_NOT_IN_SAMPLE != 0 {
        flags[cnt] = CStringGetTextDatum(c"NOT_IN_SAMPLE".as_ptr());
        cnt += 1;
    }
    if (*record).flags & GUC_RUNTIME_COMPUTED != 0 {
        flags[cnt] = CStringGetTextDatum(c"RUNTIME_COMPUTED".as_ptr());
        cnt += 1;
    }

    Assert!(cnt <= MAX_GUC_FLAGS);

    /* Returns the record as Datum */
    a = construct_array_builtin(flags.as_mut_ptr(), cnt as c_int, TEXTOID);
    PG_RETURN_ARRAYTYPE_P!(a)
}

/*
 * Return whether or not the GUC variable is visible to the current user.
 */
#[no_mangle]
pub unsafe extern "C" fn ConfigOptionIsVisible(conf: *mut config_generic) -> bool {
    if (*conf).flags & GUC_SUPERUSER_ONLY != 0
        && !has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS)
    {
        false
    } else {
        true
    }
}

/*
 * Extract fields to show in pg_settings for given variable.
 */
unsafe fn GetConfigOptionValues(conf: *mut config_generic, values: *mut *const c_char) {
    let mut buffer: [c_char; 256] = [0; 256];

    /* first get the generic attributes */

    /* name */
    *values.offset(0) = (*conf).name;

    /* setting: use ShowGUCOption in order to avoid duplicating the logic */
    *values.offset(1) = ShowGUCOption(conf, false);

    /* unit, if any (NULL is fine) */
    *values.offset(2) = get_config_unit_name((*conf).flags);

    /* group */
    *values.offset(3) = gettext(*config_group_names.offset((*conf).group as isize));

    /* short_desc */
    *values.offset(4) = if !(*conf).short_desc.is_null() {
        gettext((*conf).short_desc)
    } else {
        std::ptr::null()
    };

    /* extra_desc */
    *values.offset(5) = if !(*conf).long_desc.is_null() {
        gettext((*conf).long_desc)
    } else {
        std::ptr::null()
    };

    /* context */
    *values.offset(6) = *GucContext_Names.offset((*conf).context as isize);

    /* vartype */
    *values.offset(7) = *config_type_names.offset((*conf).vartype as isize);

    /* source */
    *values.offset(8) = *GucSource_Names.offset((*conf).source as isize);

    /* now get the type specific attributes */
    match (*conf).vartype {
        PGC_BOOL => {
            let lconf = conf as *mut config_bool;

            /* min_val */
            *values.offset(9) = std::ptr::null();

            /* max_val */
            *values.offset(10) = std::ptr::null();

            /* enumvals */
            *values.offset(11) = std::ptr::null();

            /* boot_val */
            *values.offset(12) = pstrdup(if (*lconf).boot_val {
                c"on".as_ptr()
            } else {
                c"off".as_ptr()
            });

            /* reset_val */
            *values.offset(13) = pstrdup(if (*lconf).reset_val {
                c"on".as_ptr()
            } else {
                c"off".as_ptr()
            });
        }
        PGC_INT => {
            let lconf = conf as *mut config_int;

            /* min_val */
            snprintf(buffer.as_mut_ptr(), 256, c"%d".as_ptr(), (*lconf).min);
            *values.offset(9) = pstrdup(buffer.as_ptr());

            /* max_val */
            snprintf(buffer.as_mut_ptr(), 256, c"%d".as_ptr(), (*lconf).max);
            *values.offset(10) = pstrdup(buffer.as_ptr());

            /* enumvals */
            *values.offset(11) = std::ptr::null();

            /* boot_val */
            snprintf(buffer.as_mut_ptr(), 256, c"%d".as_ptr(), (*lconf).boot_val);
            *values.offset(12) = pstrdup(buffer.as_ptr());

            /* reset_val */
            snprintf(buffer.as_mut_ptr(), 256, c"%d".as_ptr(), (*lconf).reset_val);
            *values.offset(13) = pstrdup(buffer.as_ptr());
        }
        PGC_REAL => {
            let lconf = conf as *mut config_real;

            /* min_val */
            snprintf(buffer.as_mut_ptr(), 256, c"%g".as_ptr(), (*lconf).min);
            *values.offset(9) = pstrdup(buffer.as_ptr());

            /* max_val */
            snprintf(buffer.as_mut_ptr(), 256, c"%g".as_ptr(), (*lconf).max);
            *values.offset(10) = pstrdup(buffer.as_ptr());

            /* enumvals */
            *values.offset(11) = std::ptr::null();

            /* boot_val */
            snprintf(buffer.as_mut_ptr(), 256, c"%g".as_ptr(), (*lconf).boot_val);
            *values.offset(12) = pstrdup(buffer.as_ptr());

            /* reset_val */
            snprintf(buffer.as_mut_ptr(), 256, c"%g".as_ptr(), (*lconf).reset_val);
            *values.offset(13) = pstrdup(buffer.as_ptr());
        }
        PGC_STRING => {
            let lconf = conf as *mut config_string;

            /* min_val */
            *values.offset(9) = std::ptr::null();

            /* max_val */
            *values.offset(10) = std::ptr::null();

            /* enumvals */
            *values.offset(11) = std::ptr::null();

            /* boot_val */
            if (*lconf).boot_val.is_null() {
                *values.offset(12) = std::ptr::null();
            } else {
                *values.offset(12) = pstrdup((*lconf).boot_val);
            }

            /* reset_val */
            if (*lconf).reset_val.is_null() {
                *values.offset(13) = std::ptr::null();
            } else {
                *values.offset(13) = pstrdup((*lconf).reset_val);
            }
        }
        PGC_ENUM => {
            let lconf = conf as *mut config_enum;

            /* min_val */
            *values.offset(9) = std::ptr::null();

            /* max_val */
            *values.offset(10) = std::ptr::null();

            /* enumvals */

            /*
             * NOTE! enumvals with double quotes in them are not
             * supported!
             */
            *values.offset(11) = config_enum_get_options(
                conf as *mut config_enum,
                c"{\"".as_ptr(),
                c"\"}".as_ptr(),
                c"\",\"".as_ptr(),
            );

            /* boot_val */
            *values.offset(12) =
                pstrdup(config_enum_lookup_by_value(lconf, (*lconf).boot_val));

            /* reset_val */
            *values.offset(13) =
                pstrdup(config_enum_lookup_by_value(lconf, (*lconf).reset_val));
        }
        _ => {
            /*
             * should never get here, but in case we do, set 'em to NULL
             */

            /* min_val */
            *values.offset(9) = std::ptr::null();

            /* max_val */
            *values.offset(10) = std::ptr::null();

            /* enumvals */
            *values.offset(11) = std::ptr::null();

            /* boot_val */
            *values.offset(12) = std::ptr::null();

            /* reset_val */
            *values.offset(13) = std::ptr::null();
        }
    }

    /*
     * If the setting came from a config file, set the source location. For
     * security reasons, we don't show source file/line number for
     * insufficiently-privileged users.
     */
    if (*conf).source == PGC_S_FILE
        && has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS)
    {
        *values.offset(14) = (*conf).sourcefile;
        snprintf(buffer.as_mut_ptr(), 256, c"%d".as_ptr(), (*conf).sourceline);
        *values.offset(15) = pstrdup(buffer.as_ptr());
    } else {
        *values.offset(14) = std::ptr::null();
        *values.offset(15) = std::ptr::null();
    }

    *values.offset(16) = if (*conf).status & GUC_PENDING_RESTART != 0 {
        c"t".as_ptr()
    } else {
        c"f".as_ptr()
    };
}

/*
 * show_config_by_name - equiv to SHOW X command but implemented as
 * a function.
 */
#[no_mangle]
pub unsafe extern "C" fn show_config_by_name(fcinfo: FunctionCallInfo) -> Datum {
    let varname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, 0));
    let varval: *mut c_char;

    /* Get the value */
    varval = GetConfigOptionByName(varname, std::ptr::null_mut(), false);

    /* Convert to text */
    PG_RETURN_TEXT_P!(cstring_to_text(varval))
}

/*
 * show_config_by_name_missing_ok - equiv to SHOW X command but implemented as
 * a function.  If X does not exist, suppress the error and just return NULL
 * if missing_ok is true.
 */
#[no_mangle]
pub unsafe extern "C" fn show_config_by_name_missing_ok(fcinfo: FunctionCallInfo) -> Datum {
    let varname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, 0));
    let missing_ok = PG_GETARG_BOOL!(fcinfo, 1);
    let varval: *mut c_char;

    /* Get the value */
    varval = GetConfigOptionByName(varname, std::ptr::null_mut(), missing_ok);

    /* return NULL if no such variable */
    if varval.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    /* Convert to text */
    PG_RETURN_TEXT_P!(cstring_to_text(varval))
}

/*
 * show_all_settings - equiv to SHOW ALL command but implemented as
 * a Table Function.
 */
#[no_mangle]
pub unsafe extern "C" fn show_all_settings(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let mut guc_vars: *mut *mut config_generic;
    let mut num_vars: c_int = 0;
    let tupdesc: TupleDesc;
    let call_cntr: c_int;
    let max_calls: c_int;
    let mut attinmeta: *mut AttInMetadata;
    let oldcontext: MemoryContext;

    /* stuff done only on the first call of the function */
    if SRF_IS_FIRSTCALL!(fcinfo) {
        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT!(fcinfo);

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx_multi_call_memory_ctx(funcctx));

        /*
         * need a tuple descriptor representing NUM_PG_SETTINGS_ATTS columns
         * of the appropriate types
         */
        tupdesc = CreateTemplateTupleDesc(NUM_PG_SETTINGS_ATTS as c_int);
        TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"name".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2 as AttrNumber, c"setting".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3 as AttrNumber, c"unit".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 4 as AttrNumber, c"category".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 5 as AttrNumber, c"short_desc".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 6 as AttrNumber, c"extra_desc".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 7 as AttrNumber, c"context".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 8 as AttrNumber, c"vartype".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 9 as AttrNumber, c"source".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 10 as AttrNumber, c"min_val".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 11 as AttrNumber, c"max_val".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 12 as AttrNumber, c"enumvals".as_ptr(), TEXTARRAYOID, -1, 0);
        TupleDescInitEntry(tupdesc, 13 as AttrNumber, c"boot_val".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 14 as AttrNumber, c"reset_val".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 15 as AttrNumber, c"sourcefile".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 16 as AttrNumber, c"sourceline".as_ptr(), INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, 17 as AttrNumber, c"pending_restart".as_ptr(), BOOLOID, -1, 0);

        /*
         * Generate attribute metadata needed later to produce tuples from raw
         * C strings
         */
        attinmeta = TupleDescGetAttInMetadata(tupdesc);
        funcctx_set_attinmeta(funcctx, attinmeta);

        /* collect the variables, in sorted order */
        guc_vars = get_guc_variables(&mut num_vars);

        /* use user_fctx to remember the array location */
        funcctx_set_user_fctx(funcctx, guc_vars as *mut c_void);

        /* total number of tuples to be returned */
        funcctx_set_max_calls(funcctx, num_vars);

        MemoryContextSwitchTo(oldcontext);
    }

    /* stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP!(fcinfo);

    guc_vars = funcctx_user_fctx(funcctx) as *mut *mut config_generic;
    call_cntr = funcctx_call_cntr(funcctx);
    max_calls = funcctx_max_calls(funcctx);
    attinmeta = funcctx_attinmeta(funcctx);

    let mut call_cntr = call_cntr;
    while call_cntr < max_calls
    /* do when there is more left to send */
    {
        let conf = *guc_vars.offset(call_cntr as isize);
        let mut values: [*mut c_char; NUM_PG_SETTINGS_ATTS] = [std::ptr::null_mut(); NUM_PG_SETTINGS_ATTS];
        let tuple: HeapTuple;
        let result: Datum;

        /* skip if marked NO_SHOW_ALL or if not visible to current user */
        if (*conf).flags & GUC_NO_SHOW_ALL != 0 || !ConfigOptionIsVisible(conf) {
            call_cntr = funcctx_incr_call_cntr(funcctx);
            continue;
        }

        /* extract values for the current variable */
        GetConfigOptionValues(conf, values.as_mut_ptr() as *mut *const c_char);

        /* build a tuple */
        tuple = BuildTupleFromCStrings(attinmeta, values.as_mut_ptr());

        /* make the tuple into a datum */
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT!(funcctx, result);
    }

    /* do when there is no more left */
    SRF_RETURN_DONE!(funcctx)
}

/*
 * show_all_file_settings
 *
 * Returns a table of all parameter settings in all configuration files
 * which includes the config file pathname, the line number, a sequence number
 * indicating the order in which the settings were encountered, the parameter
 * name and value, a bool showing if the value could be applied, and possibly
 * an associated error message.  (For problems such as syntax errors, the
 * parameter name/value might be NULL.)
 *
 * Note: no filtering is done here, instead we depend on the GRANT system
 * to prevent unprivileged users from accessing this function or the view
 * built on top of it.
 */
#[no_mangle]
pub unsafe extern "C" fn show_all_file_settings(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo = fcinfo_resultinfo(fcinfo) as *mut ReturnSetInfo;
    let mut conf: *mut ConfigVariable;
    let mut seqno: c_int;

    /* Scan the config files using current context as workspace */
    conf = ProcessConfigFileInternal(PGC_SIGHUP, false, DEBUG3);

    /* Build a tuplestore to return our results in */
    InitMaterializedSRF(fcinfo, 0);

    /* Process the results and create a tuplestore */
    seqno = 1;
    while !conf.is_null() {
        let mut values: [Datum; NUM_PG_FILE_SETTINGS_ATTS] = [0; NUM_PG_FILE_SETTINGS_ATTS];
        let mut nulls: [bool; NUM_PG_FILE_SETTINGS_ATTS] = [false; NUM_PG_FILE_SETTINGS_ATTS];

        /* sourcefile */
        if !cv_filename(conf).is_null() {
            values[0] = PointerGetDatum(cstring_to_text(cv_filename(conf)));
        } else {
            nulls[0] = true;
        }

        /* sourceline (not meaningful if no sourcefile) */
        if !cv_filename(conf).is_null() {
            values[1] = Int32GetDatum(cv_sourceline(conf));
        } else {
            nulls[1] = true;
        }

        /* seqno */
        values[2] = Int32GetDatum(seqno);

        /* name */
        if !cv_name(conf).is_null() {
            values[3] = PointerGetDatum(cstring_to_text(cv_name(conf)));
        } else {
            nulls[3] = true;
        }

        /* setting */
        if !cv_value(conf).is_null() {
            values[4] = PointerGetDatum(cstring_to_text(cv_value(conf)));
        } else {
            nulls[4] = true;
        }

        /* applied */
        values[5] = BoolGetDatum(cv_applied(conf));

        /* error */
        if !cv_errmsg(conf).is_null() {
            values[6] = PointerGetDatum(cstring_to_text(cv_errmsg(conf)));
        } else {
            nulls[6] = true;
        }

        /* shove row into tuplestore */
        tuplestore_putvalues(
            rsinfo_setResult(rsinfo),
            rsinfo_setDesc(rsinfo),
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        conf = cv_next(conf);
        seqno += 1;
    }

    0 as Datum
}

// ---- SRF / funcapi stubs (funcapi.h, not yet ported) ----

unsafe fn srf_is_firstcall(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn srf_firstcall_init(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn srf_percall_setup(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn srf_return_next(_fctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn srf_return_done(_fctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO: funcapi.h
}

// ---- accessor stubs for opaque struct fields ----

unsafe fn funcctx_multi_call_memory_ctx(_funcctx: *mut FuncCallContext) -> MemoryContext {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn funcctx_set_attinmeta(_funcctx: *mut FuncCallContext, _attinmeta: *mut AttInMetadata) {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn funcctx_set_user_fctx(_funcctx: *mut FuncCallContext, _user_fctx: *mut c_void) {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn funcctx_set_max_calls(_funcctx: *mut FuncCallContext, _max_calls: c_int) {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn funcctx_user_fctx(_funcctx: *mut FuncCallContext) -> *mut c_void {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn funcctx_call_cntr(_funcctx: *mut FuncCallContext) -> c_int {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn funcctx_max_calls(_funcctx: *mut FuncCallContext) -> c_int {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn funcctx_attinmeta(_funcctx: *mut FuncCallContext) -> *mut AttInMetadata {
    unimplemented!() // TODO: funcapi.c
}
unsafe fn funcctx_incr_call_cntr(_funcctx: *mut FuncCallContext) -> c_int {
    unimplemented!() // TODO: funcapi.c (++funcctx->call_cntr)
}
unsafe fn fcinfo_resultinfo(_fcinfo: FunctionCallInfo) -> *mut c_void {
    unimplemented!() // TODO: fmgr
}
unsafe fn rsinfo_setResult(_rsinfo: *mut ReturnSetInfo) -> *mut c_void {
    unimplemented!() // TODO: executor/execTuples.c
}
unsafe fn rsinfo_setDesc(_rsinfo: *mut ReturnSetInfo) -> TupleDesc {
    unimplemented!() // TODO: executor/execTuples.c
}
unsafe fn cv_filename(_conf: *mut ConfigVariable) -> *const c_char {
    unimplemented!() // TODO: utils/misc/guc-file.l
}
unsafe fn cv_sourceline(_conf: *mut ConfigVariable) -> c_int {
    unimplemented!() // TODO: utils/misc/guc-file.l
}
unsafe fn cv_name(_conf: *mut ConfigVariable) -> *const c_char {
    unimplemented!() // TODO: utils/misc/guc-file.l
}
unsafe fn cv_value(_conf: *mut ConfigVariable) -> *const c_char {
    unimplemented!() // TODO: utils/misc/guc-file.l
}
unsafe fn cv_applied(_conf: *mut ConfigVariable) -> bool {
    unimplemented!() // TODO: utils/misc/guc-file.l
}
unsafe fn cv_errmsg(_conf: *mut ConfigVariable) -> *const c_char {
    unimplemented!() // TODO: utils/misc/guc-file.l
}
unsafe fn cv_next(_conf: *mut ConfigVariable) -> *mut ConfigVariable {
    unimplemented!() // TODO: utils/misc/guc-file.l
}

// helper to render a *const c_char in an elog! format slot
unsafe fn cstr_display(s: *const c_char) -> impl std::fmt::Display {
    std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

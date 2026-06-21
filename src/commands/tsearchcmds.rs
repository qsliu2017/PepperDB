//! commands/tsearchcmds.c - Routines for tsearch manipulation commands.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/commands/tsearchcmds.c

use crate::prelude::*;
use crate::{foreach, current_cell, lfirst_node, foreach_delete_current, strVal, IsA, appendStringInfo};

use crate::nodes::pg_list::{List, lfirst, lnext, lappend, list_length};
use crate::nodes::nodes::Node;
use crate::nodes::makefuncs::makeDefElem;
use crate::nodes::value::{makeInteger, makeFloat, makeBoolean, makeString};
use crate::nodes::parsenodes::{
    DefElem, AlterTSDictionaryStmt, AlterTSConfigurationStmt,
    ObjectType, OBJECT_SCHEMA, OBJECT_TSDICTIONARY, OBJECT_TSCONFIGURATION,
    ACL_CREATE,
};
use crate::lib::stringinfo::{
    StringInfoData, initStringInfo, appendStringInfoString, appendStringInfoChar,
};
use crate::miscadmin::{GetUserId, IsUnderPostmaster};

// ---------------------------------------------------------------------------
// Single entry of List returned by getTokenTypes()
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct TSTokenTypeItem {
    pub num: c_int,        // token type number
    pub name: *mut c_char, // token type name
}

// ---------------------------------------------------------------------------
// Local type stubs and dependency stubs for unported callees.  Each mirrors the
// C signature so the real translations can drop in later.  // TODO(pg-port)
// ---------------------------------------------------------------------------

// HeapTuple is a single pointer (HeapTupleData*).
use crate::access::htup_details::HeapTupleData;
type HeapTuple = *mut HeapTupleData;
use crate::storage::itemptr::ItemPointerData;

// Relation pointer  TODO(pg-port)
#[repr(C)] pub struct RelationData { _opaque: [u8; 0] }
type Relation = *mut RelationData;

// SysScanDesc / ScanKeyData  TODO(pg-port)
#[repr(C)] pub struct SysScanDescData { _opaque: [u8; 0] }
type SysScanDesc = *mut SysScanDescData;
#[repr(C)] pub struct ScanKeyData { _opaque: [u8; 64] }

// TupleDesc  TODO(pg-port)
#[repr(C)] pub struct TupleDescData { _opaque: [u8; 0] }
type TupleDesc = *mut TupleDescData;

// TupleTableSlot / CatalogIndexState  TODO(pg-port)
#[repr(C)] pub struct TupleTableSlotData {
    _opaque_head: [u8; 0],
    pub tts_isnull: *mut bool,
    pub tts_values: *mut Datum,
    pub tts_tupleDescriptor: TupleDesc,
}
type TupleTableSlot = *mut TupleTableSlotData;
#[repr(C)] pub struct CatalogIndexStateData { _opaque: [u8; 0] }
type CatalogIndexState = *mut CatalogIndexStateData;

// ObjectAddress / ObjectAddresses  TODO(pg-port)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAddress {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: int32,
}
#[repr(C)] pub struct ObjectAddresses { _opaque: [u8; 0] }

// text type  TODO(pg-port)
#[repr(C)] pub struct text { _opaque: [u8; 0] }

// AclResult  TODO(pg-port)
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 2;

// Catalog Form structs  TODO(pg-port)
#[repr(C)] pub struct FormData_pg_ts_parser {
    pub oid: Oid,
    pub prsname: [c_char; 64],
    pub prsnamespace: Oid,
    pub prsstart: Oid,
    pub prstoken: Oid,
    pub prsend: Oid,
    pub prsheadline: Oid,
    pub prslextype: Oid,
}
type Form_pg_ts_parser = *mut FormData_pg_ts_parser;
#[repr(C)] pub struct FormData_pg_ts_dict {
    pub oid: Oid,
    pub dictname: [c_char; 64],
    pub dictnamespace: Oid,
    pub dictowner: Oid,
    pub dicttemplate: Oid,
}
type Form_pg_ts_dict = *mut FormData_pg_ts_dict;
#[repr(C)] pub struct FormData_pg_ts_template {
    pub oid: Oid,
    pub tmplname: [c_char; 64],
    pub tmplnamespace: Oid,
    pub tmplinit: Oid,
    pub tmpllexize: Oid,
}
type Form_pg_ts_template = *mut FormData_pg_ts_template;
#[repr(C)] pub struct FormData_pg_ts_config {
    pub oid: Oid,
    pub cfgname: [c_char; 64],
    pub cfgnamespace: Oid,
    pub cfgowner: Oid,
    pub cfgparser: Oid,
}
type Form_pg_ts_config = *mut FormData_pg_ts_config;
#[repr(C)] pub struct FormData_pg_ts_config_map {
    pub mapcfg: Oid,
    pub maptokentype: int32,
    pub mapseqno: int32,
    pub mapdict: Oid,
}
type Form_pg_ts_config_map = *mut FormData_pg_ts_config_map;

// tsearch/ts_public.h: LexDescr  TODO(pg-port)
#[repr(C)] pub struct LexDescr {
    pub lexid: c_int,
    pub alias: *mut c_char,
    pub descr: *mut c_char,
}

// tsearch/ts_cache.h: TSParserCacheEntry  TODO(pg-port)
#[repr(C)] pub struct TSParserCacheEntry {
    _opaque_head: [u8; 0],
    pub lextypeOid: Oid,
}

// LOCKMODE  TODO(pg-port)
type LOCKMODE = c_int;
const RowExclusiveLock: LOCKMODE = 3;

// DependencyType  TODO(pg-port)
type DependencyType = c_int;
const DEPENDENCY_NORMAL: DependencyType = b'n' as c_int;

// Strategy / procedure numbers  TODO(pg-port)
const BTEqualStrategyNumber: c_int = 3;
const F_OIDEQ: Oid = 184;
const F_INT4EQ: Oid = 65;

// pg_type OIDs  TODO(pg-port)
const INTERNALOID: Oid = 2281;
const INT4OID: Oid = 23;
const TSQUERYOID: Oid = 3615;
const VOIDOID: Oid = 2278;

// catalog relation OIDs and index OIDs  TODO(pg-port)
const TSParserRelationId: Oid = 3601;
const TSParserOidIndexId: Oid = 3651;
const TSDictionaryRelationId: Oid = 3600;
const TSDictionaryOidIndexId: Oid = 3605;
const TSTemplateRelationId: Oid = 3764;
const TSTemplateOidIndexId: Oid = 3766;
const TSConfigRelationId: Oid = 3602;
const TSConfigOidIndexId: Oid = 3712;
const TSConfigMapRelationId: Oid = 3603;
const TSConfigMapIndexId: Oid = 3609;
const NamespaceRelationId: Oid = 2615;
const ProcedureRelationId: Oid = 1255;

// pg_ts_parser attribute numbers  TODO(pg-port)
const Natts_pg_ts_parser: usize = 8;
const Anum_pg_ts_parser_oid: c_int = 1;
const Anum_pg_ts_parser_prsname: c_int = 2;
const Anum_pg_ts_parser_prsnamespace: c_int = 3;
const Anum_pg_ts_parser_prsstart: c_int = 4;
const Anum_pg_ts_parser_prstoken: c_int = 5;
const Anum_pg_ts_parser_prsend: c_int = 6;
const Anum_pg_ts_parser_prsheadline: c_int = 7;
const Anum_pg_ts_parser_prslextype: c_int = 8;

// pg_ts_dict attribute numbers  TODO(pg-port)
const Natts_pg_ts_dict: usize = 6;
const Anum_pg_ts_dict_oid: c_int = 1;
const Anum_pg_ts_dict_dictname: c_int = 2;
const Anum_pg_ts_dict_dictnamespace: c_int = 3;
const Anum_pg_ts_dict_dictowner: c_int = 4;
const Anum_pg_ts_dict_dicttemplate: c_int = 5;
const Anum_pg_ts_dict_dictinitoption: c_int = 6;

// pg_ts_template attribute numbers  TODO(pg-port)
const Natts_pg_ts_template: usize = 5;
const Anum_pg_ts_template_oid: c_int = 1;
const Anum_pg_ts_template_tmplname: c_int = 2;
const Anum_pg_ts_template_tmplnamespace: c_int = 3;
const Anum_pg_ts_template_tmplinit: c_int = 4;
const Anum_pg_ts_template_tmpllexize: c_int = 5;

// pg_ts_config attribute numbers  TODO(pg-port)
const Natts_pg_ts_config: usize = 5;
const Anum_pg_ts_config_oid: c_int = 1;
const Anum_pg_ts_config_cfgname: c_int = 2;
const Anum_pg_ts_config_cfgnamespace: c_int = 3;
const Anum_pg_ts_config_cfgowner: c_int = 4;
const Anum_pg_ts_config_cfgparser: c_int = 5;

// pg_ts_config_map attribute numbers  TODO(pg-port)
const Natts_pg_ts_config_map: usize = 4;
const Anum_pg_ts_config_map_mapcfg: c_int = 1;
const Anum_pg_ts_config_map_maptokentype: c_int = 2;
const Anum_pg_ts_config_map_mapseqno: c_int = 3;
const Anum_pg_ts_config_map_mapdict: c_int = 4;

// syscache id constants  TODO(pg-port)
const TSTEMPLATEOID: c_int = 80;
const TSDICTOID: c_int = 76;
const TSCONFIGOID: c_int = 74;

// catalog multi-insert batch size  TODO(pg-port)
const MAX_CATALOG_MULTI_INSERT_BYTES: usize = 65535;

// TTSOpsHeapTuple: the heap-tuple slot operations vtable  TODO(pg-port)
#[repr(C)] pub struct TupleTableSlotOps { _opaque: [u8; 0] }
extern "C" {
    static TTSOpsHeapTuple: TupleTableSlotOps;
}

// ESCAPE_STRING_SYNTAX from the parser  TODO(pg-port)
const ESCAPE_STRING_SYNTAX: c_char = b'E' as c_char;

// errcode constants  TODO(pg-port)
const ERRCODE_INVALID_OBJECT_DEFINITION: c_int = 0;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_SYNTAX_ERROR: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;

// ----- catalog / access stubs -----
unsafe fn table_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation { unimplemented!() /* TODO(pg-port) */ }
unsafe fn table_close(_relation: Relation, _lockmode: LOCKMODE) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn GETSTRUCT(_tup: HeapTuple) -> *mut c_void { unimplemented!() /* TODO(pg-port) */ }
unsafe fn RelationGetDescr(_rel: Relation) -> TupleDesc { unimplemented!() /* TODO(pg-port) */ }
unsafe fn TupleDescNatts(_tupdesc: TupleDesc) -> c_int { unimplemented!() /* TODO(pg-port): tupdesc->natts */ }
unsafe fn heap_form_tuple(_tupdesc: TupleDesc, _values: *mut Datum, _isnull: *mut bool) -> HeapTuple { unimplemented!() /* TODO(pg-port) */ }
unsafe fn heap_modify_tuple(_tuple: HeapTuple, _tupdesc: TupleDesc, _repl_values: *mut Datum, _repl_isnull: *mut bool, _do_replace: *mut bool) -> HeapTuple { unimplemented!() /* TODO(pg-port) */ }
unsafe fn heap_freetuple(_htup: HeapTuple) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn CatalogTupleUpdate(_heapRel: Relation, _otid: *mut ItemPointerData, _tup: HeapTuple) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn CatalogTupleUpdateWithInfo(_heapRel: Relation, _otid: *mut ItemPointerData, _tup: HeapTuple, _indstate: CatalogIndexState) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn CatalogTupleDelete(_heapRel: Relation, _tid: *mut ItemPointerData) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn CatalogOpenIndexes(_heapRel: Relation) -> CatalogIndexState { unimplemented!() /* TODO(pg-port) */ }
unsafe fn CatalogCloseIndexes(_indstate: CatalogIndexState) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn CatalogTuplesMultiInsertWithInfo(_heapRel: Relation, _slot: *mut TupleTableSlot, _ntuples: c_int, _indstate: CatalogIndexState) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn GetNewOidWithIndex(_relation: Relation, _indexId: Oid, _oidcolumn: c_int) -> Oid { unimplemented!() /* TODO(pg-port) */ }
unsafe fn CommandCounterIncrement() { unimplemented!() /* TODO(pg-port) */ }

// ----- syscache stubs -----
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple { unimplemented!() /* TODO(pg-port) */ }
unsafe fn ReleaseSysCache(_tuple: HeapTuple) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn SysCacheGetAttr(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int, _isnull: *mut bool) -> Datum { unimplemented!() /* TODO(pg-port) */ }
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool { !tuple.is_null() }

// ----- systable scan stubs -----
unsafe fn systable_beginscan(_heapRelation: Relation, _indexId: Oid, _indexOK: bool, _snapshot: *mut c_void, _nkeys: c_int, _key: *mut ScanKeyData) -> SysScanDesc { unimplemented!() /* TODO(pg-port) */ }
unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple { unimplemented!() /* TODO(pg-port) */ }
unsafe fn systable_endscan(_sysscan: SysScanDesc) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn ScanKeyInit(_entry: *mut ScanKeyData, _attributeNumber: c_int, _strategy: c_int, _procedure: Oid, _argument: Datum) { unimplemented!() /* TODO(pg-port) */ }

// ----- executor slot stubs -----
unsafe fn MakeSingleTupleTableSlot(_tupdesc: TupleDesc, _tts_ops: *const TupleTableSlotOps) -> TupleTableSlot { unimplemented!() /* TODO(pg-port) */ }
unsafe fn ExecDropSingleTupleTableSlot(_slot: TupleTableSlot) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn ExecClearTuple(_slot: TupleTableSlot) -> TupleTableSlot { unimplemented!() /* TODO(pg-port) */ }
unsafe fn ExecStoreVirtualTuple(_slot: TupleTableSlot) -> TupleTableSlot { unimplemented!() /* TODO(pg-port) */ }

// ----- dependency stubs -----
unsafe fn new_object_addresses() -> *mut ObjectAddresses { unimplemented!() /* TODO(pg-port) */ }
unsafe fn add_exact_object_address(_object: *const ObjectAddress, _addrs: *mut ObjectAddresses) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn record_object_address_dependencies(_depender: *const ObjectAddress, _referenced: *mut ObjectAddresses, _behavior: DependencyType) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn free_object_addresses(_addrs: *mut ObjectAddresses) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn recordDependencyOnOwner(_classId: Oid, _objectId: Oid, _owner: Oid) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn recordDependencyOnCurrentExtension(_object: *const ObjectAddress, _isReplace: bool) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn deleteDependencyRecordsFor(_classId: Oid, _objectId: Oid, _skipExtensionDeps: bool) -> c_long { unimplemented!() /* TODO(pg-port) */ }
unsafe fn deleteSharedDependencyRecordsFor(_classId: Oid, _objectId: Oid, _objectSubId: int32) { unimplemented!() /* TODO(pg-port) */ }

// ----- ts cache stub -----
unsafe fn lookup_ts_parser_cache(_prsId: Oid) -> *mut TSParserCacheEntry { unimplemented!() /* TODO(pg-port) */ }

// ----- acl / namespace stubs -----
unsafe fn superuser() -> bool { unimplemented!() /* TODO(pg-port) */ }
unsafe fn object_aclcheck(_classid: Oid, _objectid: Oid, _roleid: Oid, _mode: AclMode) -> AclResult { unimplemented!() /* TODO(pg-port) */ }
unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool { unimplemented!() /* TODO(pg-port) */ }
unsafe fn aclcheck_error(_aclerr: AclResult, _objtype: ObjectType, _objectname: *const c_char) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char { unimplemented!() /* TODO(pg-port) */ }
unsafe fn QualifiedNameGetCreationNamespace(_names: *mut List, _objname_p: *mut *mut c_char) -> Oid { unimplemented!() /* TODO(pg-port) */ }
unsafe fn NameListToString(_names: *mut List) -> *mut c_char { unimplemented!() /* TODO(pg-port) */ }

// ----- defrem / lsyscache / parse_func stubs -----
unsafe fn defGetQualifiedName(_def: *mut DefElem) -> *mut List { unimplemented!() /* TODO(pg-port) */ }
unsafe fn defGetString(_def: *mut DefElem) -> *mut c_char { unimplemented!() /* TODO(pg-port) */ }
unsafe fn LookupFuncName(_funcname: *mut List, _nargs: c_int, _argtypes: *const Oid, _missing_ok: bool) -> Oid { unimplemented!() /* TODO(pg-port) */ }
unsafe fn get_func_rettype(_funcid: Oid) -> Oid { unimplemented!() /* TODO(pg-port) */ }
unsafe fn func_signature_string(_funcname: *mut List, _nargs: c_int, _argnames: *mut List, _argtypes: *const Oid) -> *mut c_char { unimplemented!() /* TODO(pg-port) */ }
unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char { unimplemented!() /* TODO(pg-port) */ }
unsafe fn get_ts_parser_oid(_names: *mut List, _missing_ok: bool) -> Oid { unimplemented!() /* TODO(pg-port) */ }
unsafe fn get_ts_dict_oid(_names: *mut List, _missing_ok: bool) -> Oid { unimplemented!() /* TODO(pg-port) */ }
unsafe fn get_ts_template_oid(_names: *mut List, _missing_ok: bool) -> Oid { unimplemented!() /* TODO(pg-port) */ }
unsafe fn get_ts_config_oid(_names: *mut List, _missing_ok: bool) -> Oid { unimplemented!() /* TODO(pg-port) */ }

// ----- fmgr / object access stubs -----
unsafe fn OidFunctionCall1(_functionId: Oid, _arg1: Datum) -> Datum { unimplemented!() /* TODO(pg-port) */ }
unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: c_int) { unimplemented!("STUB InvokeObjectPostCreateHook") }
unsafe fn InvokeObjectPostAlterHook(_classId: Oid, _objectId: Oid, _subId: c_int) { unimplemented!("STUB InvokeObjectPostAlterHook") }

// ----- node / catalog helpers -----
unsafe fn copyObjectImpl(_from: *const c_void) -> *mut c_void { unimplemented!() /* TODO(pg-port) */ }
unsafe fn EventTriggerCollectAlterTSConfig(_stmt: *mut AlterTSConfigurationStmt, _cfgId: Oid, _dictIds: *mut Oid, _ndicts: c_int) { unimplemented!() /* TODO(pg-port) */ }

// ----- string / varlena helpers -----
unsafe fn quote_identifier(_ident: *const c_char) -> *const c_char { unimplemented!() /* TODO(pg-port) */ }
unsafe fn cstring_to_text_with_len(_s: *const c_char, _len: c_int) -> *mut text { unimplemented!() /* TODO(pg-port) */ }
unsafe fn text_to_cstring(_t: *const text) -> *mut c_char { unimplemented!() /* TODO(pg-port) */ }
unsafe fn namestrcpy(_name: *mut NameData, _str: *const c_char) { unimplemented!() /* TODO(pg-port) */ }
unsafe fn strtoint(_str: *const c_char, _endptr: *mut *mut c_char, _base: c_int) -> c_int { unimplemented!() /* TODO(pg-port) */ }

// NameStr(name): C macro yielding the name's char* data.
unsafe fn NameStr(name: *const NameData) -> *const c_char { (*name).data.as_ptr() }

// ----- libc-ish helpers -----
extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strtod(nptr: *const c_char, endptr: *mut *mut c_char) -> f64;
    fn __error() -> *mut c_int; // macOS errno location
}
unsafe fn isspace_c(ch: c_uchar) -> bool { (ch as char).is_ascii_whitespace() }
unsafe fn set_errno(v: c_int) { *__error() = v; }
unsafe fn get_errno() -> c_int { *__error() }

// AclMode (privilege bitmask) - re-use parsenodes' definition.
use crate::nodes::parsenodes::AclMode;

// NIL is the null List pointer; named NIL_LIST here to avoid clashing with the
// generic NIL alias used elsewhere.
const NIL_LIST: *mut List = null_mut();

// NameGetDatum(name): the address of the NameData, as a Datum  TODO(pg-port)
unsafe fn NameGetDatum(name: *const NameData) -> Datum {
    PointerGetDatum(name as *const c_void)
}

// Min(a, b)
fn Min<T: PartialOrd>(a: T, b: T) -> T { if a < b { a } else { b } }

// VARDATA_ANY / VARSIZE_ANY_EXHDR / DatumGetTextPP  TODO(pg-port)
unsafe fn DatumGetTextPP(_d: Datum) -> *mut text { unimplemented!() /* TODO(pg-port) */ }
unsafe fn VARDATA_ANY(_vlena: *mut text) -> *mut c_char { unimplemented!() /* TODO(pg-port) */ }
unsafe fn VARSIZE_ANY_EXHDR(_vlena: *mut text) -> c_int { unimplemented!() /* TODO(pg-port) */ }

// SQL_STR_DOUBLE: returns true if ch needs doubling in a string literal  TODO(pg-port)
unsafe fn SQL_STR_DOUBLE(ch: c_char, escape_backslash: bool) -> bool {
    ch == b'\'' as c_char || (escape_backslash && ch == b'\\' as c_char)
}

// copyObject macro analog: copies a List of DefElem  TODO(pg-port)
unsafe fn copyObject_List(from: *mut List) -> *mut List {
    copyObjectImpl(from as *const c_void) as *mut List
}

// ObjectAddressSet(addr, class, object): set classId/objectId, subId = 0.
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, classId: Oid, objectId: Oid) {
    addr.classId = classId;
    addr.objectId = objectId;
    addr.objectSubId = 0;
}

/* --------------------- TS Parser commands ------------------------ */

/*
 * lookup a parser support function and return its OID (as a Datum)
 *
 * attnum is the pg_ts_parser column the function will go into
 */
unsafe fn get_ts_parser_func(defel: *mut DefElem, attnum: c_int) -> Datum {
    let funcName: *mut List = defGetQualifiedName(defel);
    let mut typeId: [Oid; 3] = [InvalidOid; 3];
    let mut retTypeId: Oid;
    let nargs: c_int;
    let procOid: Oid;

    retTypeId = INTERNALOID; /* correct for most */
    typeId[0] = INTERNALOID;
    match attnum {
        x if x == Anum_pg_ts_parser_prsstart => {
            nargs = 2;
            typeId[1] = INT4OID;
        }
        x if x == Anum_pg_ts_parser_prstoken => {
            nargs = 3;
            typeId[1] = INTERNALOID;
            typeId[2] = INTERNALOID;
        }
        x if x == Anum_pg_ts_parser_prsend => {
            nargs = 1;
            retTypeId = VOIDOID;
        }
        x if x == Anum_pg_ts_parser_prsheadline => {
            nargs = 3;
            typeId[1] = INTERNALOID;
            typeId[2] = TSQUERYOID;
        }
        x if x == Anum_pg_ts_parser_prslextype => {
            nargs = 1;

            /*
             * Note: because the lextype method returns type internal, it must
             * have an internal-type argument for security reasons.  The
             * argument is not actually used, but is just passed as a zero.
             */
        }
        _ => {
            /* should not be here */
            elog!(ERROR, "unrecognized attribute for text search parser: {}", attnum);
            #[allow(unreachable_code)]
            { nargs = 0; } /* keep compiler quiet */
        }
    }

    procOid = LookupFuncName(funcName, nargs, typeId.as_ptr(), false);
    if get_func_rettype(procOid) != retTypeId {
        ereport!(ERROR,
            errmsg!("function {} should return type {}",
                std::ffi::CStr::from_ptr(func_signature_string(funcName, nargs, NIL_LIST, typeId.as_ptr())).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(retTypeId)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    ObjectIdGetDatum(procOid)
}

/*
 * make pg_depend entries for a new pg_ts_parser entry
 *
 * Return value is the address of said new entry.
 */
unsafe fn makeParserDependencies(tuple: HeapTuple) -> ObjectAddress {
    let prs: Form_pg_ts_parser = GETSTRUCT(tuple) as Form_pg_ts_parser;
    let mut myself: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let mut referenced: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let addrs: *mut ObjectAddresses;

    ObjectAddressSet(&mut myself, TSParserRelationId, (*prs).oid);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    addrs = new_object_addresses();

    /* dependency on namespace */
    ObjectAddressSet(&mut referenced, NamespaceRelationId, (*prs).prsnamespace);
    add_exact_object_address(&referenced, addrs);

    /* dependencies on functions */
    ObjectAddressSet(&mut referenced, ProcedureRelationId, (*prs).prsstart);
    add_exact_object_address(&referenced, addrs);

    referenced.objectId = (*prs).prstoken;
    add_exact_object_address(&referenced, addrs);

    referenced.objectId = (*prs).prsend;
    add_exact_object_address(&referenced, addrs);

    referenced.objectId = (*prs).prslextype;
    add_exact_object_address(&referenced, addrs);

    if OidIsValid((*prs).prsheadline) {
        referenced.objectId = (*prs).prsheadline;
        add_exact_object_address(&referenced, addrs);
    }

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    myself
}

/*
 * CREATE TEXT SEARCH PARSER
 */
pub unsafe fn DefineTSParser(names: *mut List, parameters: *mut List) -> ObjectAddress {
    let prsname: *mut c_char;
    let prsRel: Relation;
    let tup: HeapTuple;
    let mut values: [Datum; Natts_pg_ts_parser] = [0; Natts_pg_ts_parser];
    let mut nulls: [bool; Natts_pg_ts_parser] = [false; Natts_pg_ts_parser];
    let mut pname: NameData = std::mem::zeroed();
    let prsOid: Oid;
    let namespaceoid: Oid;
    let address: ObjectAddress;

    if !superuser() {
        ereport!(ERROR,
            errmsg!("must be superuser to create text search parsers"));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    prsRel = table_open(TSParserRelationId, RowExclusiveLock);

    /* Convert list of names to a name and namespace */
    let mut prsname_out: *mut c_char = null_mut();
    namespaceoid = QualifiedNameGetCreationNamespace(names, &mut prsname_out);
    prsname = prsname_out;

    /* initialize tuple fields with name/namespace */
    /* memset(values, 0) / memset(nulls, false) handled by initializers above */

    prsOid = GetNewOidWithIndex(prsRel, TSParserOidIndexId, Anum_pg_ts_parser_oid);
    values[(Anum_pg_ts_parser_oid - 1) as usize] = ObjectIdGetDatum(prsOid);
    namestrcpy(&mut pname, prsname);
    values[(Anum_pg_ts_parser_prsname - 1) as usize] = NameGetDatum(&pname);
    values[(Anum_pg_ts_parser_prsnamespace - 1) as usize] = ObjectIdGetDatum(namespaceoid);

    /*
     * loop over the definition list and extract the information we need.
     */
    foreach!(pl, parameters, {
        let defel: *mut DefElem = lfirst(current_cell!(pl)) as *mut DefElem;

        if strcmp((*defel).defname, c"start".as_ptr()) == 0 {
            values[(Anum_pg_ts_parser_prsstart - 1) as usize] =
                get_ts_parser_func(defel, Anum_pg_ts_parser_prsstart);
        } else if strcmp((*defel).defname, c"gettoken".as_ptr()) == 0 {
            values[(Anum_pg_ts_parser_prstoken - 1) as usize] =
                get_ts_parser_func(defel, Anum_pg_ts_parser_prstoken);
        } else if strcmp((*defel).defname, c"end".as_ptr()) == 0 {
            values[(Anum_pg_ts_parser_prsend - 1) as usize] =
                get_ts_parser_func(defel, Anum_pg_ts_parser_prsend);
        } else if strcmp((*defel).defname, c"headline".as_ptr()) == 0 {
            values[(Anum_pg_ts_parser_prsheadline - 1) as usize] =
                get_ts_parser_func(defel, Anum_pg_ts_parser_prsheadline);
        } else if strcmp((*defel).defname, c"lextypes".as_ptr()) == 0 {
            values[(Anum_pg_ts_parser_prslextype - 1) as usize] =
                get_ts_parser_func(defel, Anum_pg_ts_parser_prslextype);
        } else {
            ereport!(ERROR,
                errmsg!("text search parser parameter \"{}\" not recognized",
                    std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        }
    });

    /*
     * Validation
     */
    if !OidIsValid(DatumGetObjectId(values[(Anum_pg_ts_parser_prsstart - 1) as usize])) {
        ereport!(ERROR,
            errmsg!("text search parser start method is required"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    if !OidIsValid(DatumGetObjectId(values[(Anum_pg_ts_parser_prstoken - 1) as usize])) {
        ereport!(ERROR,
            errmsg!("text search parser gettoken method is required"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    if !OidIsValid(DatumGetObjectId(values[(Anum_pg_ts_parser_prsend - 1) as usize])) {
        ereport!(ERROR,
            errmsg!("text search parser end method is required"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    if !OidIsValid(DatumGetObjectId(values[(Anum_pg_ts_parser_prslextype - 1) as usize])) {
        ereport!(ERROR,
            errmsg!("text search parser lextypes method is required"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /*
     * Looks good, insert
     */
    tup = heap_form_tuple(RelationGetDescr(prsRel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(prsRel, tup);

    address = makeParserDependencies(tup);

    /* Post creation hook for new text search parser */
    InvokeObjectPostCreateHook(TSParserRelationId, prsOid, 0);

    heap_freetuple(tup);

    table_close(prsRel, RowExclusiveLock);

    address
}

/* ---------------------- TS Dictionary commands -----------------------*/

/*
 * make pg_depend entries for a new pg_ts_dict entry
 *
 * Return value is address of the new entry
 */
unsafe fn makeDictionaryDependencies(tuple: HeapTuple) -> ObjectAddress {
    let dict: Form_pg_ts_dict = GETSTRUCT(tuple) as Form_pg_ts_dict;
    let mut myself: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let mut referenced: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let addrs: *mut ObjectAddresses;

    ObjectAddressSet(&mut myself, TSDictionaryRelationId, (*dict).oid);

    /* dependency on owner */
    recordDependencyOnOwner(myself.classId, myself.objectId, (*dict).dictowner);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    addrs = new_object_addresses();

    /* dependency on namespace */
    ObjectAddressSet(&mut referenced, NamespaceRelationId, (*dict).dictnamespace);
    add_exact_object_address(&referenced, addrs);

    /* dependency on template */
    ObjectAddressSet(&mut referenced, TSTemplateRelationId, (*dict).dicttemplate);
    add_exact_object_address(&referenced, addrs);

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    myself
}

/*
 * verify that a template's init method accepts a proposed option list
 */
unsafe fn verify_dictoptions(tmplId: Oid, mut dictoptions: *mut List) {
    let tup: HeapTuple;
    let tform: Form_pg_ts_template;
    let initmethod: Oid;

    /*
     * Suppress this test when running in a standalone backend.  This is a
     * hack to allow initdb to create prefab dictionaries that might not
     * actually be usable in template1's encoding (due to using external files
     * that can't be translated into template1's encoding).  We want to create
     * them anyway, since they might be usable later in other databases.
     */
    if !IsUnderPostmaster {
        return;
    }

    tup = SearchSysCache1(TSTEMPLATEOID, ObjectIdGetDatum(tmplId));
    if !HeapTupleIsValid(tup) {
        /* should not happen */
        elog!(ERROR, "cache lookup failed for text search template {}", tmplId);
    }
    tform = GETSTRUCT(tup) as Form_pg_ts_template;

    initmethod = (*tform).tmplinit;

    if !OidIsValid(initmethod) {
        /* If there is no init method, disallow any options */
        if !dictoptions.is_null() {
            ereport!(ERROR,
                errmsg!("text search template \"{}\" does not accept options",
                    std::ffi::CStr::from_ptr(NameStr((*tform).tmplname.as_ptr() as *const NameData)).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        }
    } else {
        /*
         * Copy the options just in case init method thinks it can scribble on
         * them ...
         */
        dictoptions = copyObject_List(dictoptions);

        /*
         * Call the init method and see if it complains.  We don't worry about
         * it leaking memory, since our command will soon be over anyway.
         */
        let _ = OidFunctionCall1(initmethod, PointerGetDatum(dictoptions as *const c_void));
    }

    ReleaseSysCache(tup);
}

/*
 * CREATE TEXT SEARCH DICTIONARY
 */
pub unsafe fn DefineTSDictionary(names: *mut List, parameters: *mut List) -> ObjectAddress {
    let dictRel: Relation;
    let tup: HeapTuple;
    let mut values: [Datum; Natts_pg_ts_dict] = [0; Natts_pg_ts_dict];
    let mut nulls: [bool; Natts_pg_ts_dict] = [false; Natts_pg_ts_dict];
    let mut dname: NameData = std::mem::zeroed();
    let mut templId: Oid = InvalidOid;
    let mut dictoptions: *mut List = NIL_LIST;
    let dictOid: Oid;
    let namespaceoid: Oid;
    let aclresult: AclResult;
    let dictname: *mut c_char;
    let address: ObjectAddress;

    /* Convert list of names to a name and namespace */
    let mut dictname_out: *mut c_char = null_mut();
    namespaceoid = QualifiedNameGetCreationNamespace(names, &mut dictname_out);
    dictname = dictname_out;

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(NamespaceRelationId, namespaceoid, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceoid));
    }

    /*
     * loop over the definition list and extract the information we need.
     */
    foreach!(pl, parameters, {
        let defel: *mut DefElem = lfirst(current_cell!(pl)) as *mut DefElem;

        if strcmp((*defel).defname, c"template".as_ptr()) == 0 {
            templId = get_ts_template_oid(defGetQualifiedName(defel), false);
        } else {
            /* Assume it's an option for the dictionary itself */
            dictoptions = lappend(dictoptions, defel as *mut c_void);
        }
    });

    /*
     * Validation
     */
    if !OidIsValid(templId) {
        ereport!(ERROR,
            errmsg!("text search template is required"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    verify_dictoptions(templId, dictoptions);


    dictRel = table_open(TSDictionaryRelationId, RowExclusiveLock);

    /*
     * Looks good, insert
     */
    /* memset(values, 0) / memset(nulls, false) handled by initializers above */

    dictOid = GetNewOidWithIndex(dictRel, TSDictionaryOidIndexId, Anum_pg_ts_dict_oid);
    values[(Anum_pg_ts_dict_oid - 1) as usize] = ObjectIdGetDatum(dictOid);
    namestrcpy(&mut dname, dictname);
    values[(Anum_pg_ts_dict_dictname - 1) as usize] = NameGetDatum(&dname);
    values[(Anum_pg_ts_dict_dictnamespace - 1) as usize] = ObjectIdGetDatum(namespaceoid);
    values[(Anum_pg_ts_dict_dictowner - 1) as usize] = ObjectIdGetDatum(GetUserId());
    values[(Anum_pg_ts_dict_dicttemplate - 1) as usize] = ObjectIdGetDatum(templId);
    if !dictoptions.is_null() {
        values[(Anum_pg_ts_dict_dictinitoption - 1) as usize] =
            PointerGetDatum(serialize_deflist(dictoptions) as *const c_void);
    } else {
        nulls[(Anum_pg_ts_dict_dictinitoption - 1) as usize] = true;
    }

    tup = heap_form_tuple(RelationGetDescr(dictRel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(dictRel, tup);

    address = makeDictionaryDependencies(tup);

    /* Post creation hook for new text search dictionary */
    InvokeObjectPostCreateHook(TSDictionaryRelationId, dictOid, 0);

    heap_freetuple(tup);

    table_close(dictRel, RowExclusiveLock);

    address
}

/*
 * ALTER TEXT SEARCH DICTIONARY
 */
pub unsafe fn AlterTSDictionary(stmt: *mut AlterTSDictionaryStmt) -> ObjectAddress {
    let tup: HeapTuple;
    let newtup: HeapTuple;
    let rel: Relation;
    let dictId: Oid;
    let mut dictoptions: *mut List;
    let opt: Datum;
    let mut isnull: bool = false;
    let mut repl_val: [Datum; Natts_pg_ts_dict] = [0; Natts_pg_ts_dict];
    let mut repl_null: [bool; Natts_pg_ts_dict] = [false; Natts_pg_ts_dict];
    let mut repl_repl: [bool; Natts_pg_ts_dict] = [false; Natts_pg_ts_dict];
    let mut address: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };

    dictId = get_ts_dict_oid((*stmt).dictname, false);

    rel = table_open(TSDictionaryRelationId, RowExclusiveLock);

    tup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dictId));

    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for text search dictionary {}", dictId);
    }

    /* must be owner */
    if !object_ownercheck(TSDictionaryRelationId, dictId, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TSDICTIONARY,
                       NameListToString((*stmt).dictname));
    }

    /* deserialize the existing set of options */
    opt = SysCacheGetAttr(TSDICTOID, tup, Anum_pg_ts_dict_dictinitoption, &mut isnull);
    if isnull {
        dictoptions = NIL_LIST;
    } else {
        dictoptions = deserialize_deflist(opt);
    }

    /*
     * Modify the options list as per specified changes
     */
    foreach!(pl, (*stmt).options, {
        let defel: *mut DefElem = lfirst(current_cell!(pl)) as *mut DefElem;

        /*
         * Remove any matches ...
         */
        foreach!(cell, dictoptions, {
            let oldel: *mut DefElem = lfirst(current_cell!(cell)) as *mut DefElem;

            if strcmp((*oldel).defname, (*defel).defname) == 0 {
                dictoptions = foreach_delete_current!(dictoptions, cell);
            }
        });

        /*
         * and add new value if it's got one
         */
        if !(*defel).arg.is_null() {
            dictoptions = lappend(dictoptions, defel as *mut c_void);
        }
    });

    /*
     * Validate
     */
    verify_dictoptions((*(GETSTRUCT(tup) as Form_pg_ts_dict)).dicttemplate, dictoptions);

    /*
     * Looks good, update
     */
    /* memset(repl_val, 0) / memset(repl_null, false) / memset(repl_repl, false) handled above */

    if !dictoptions.is_null() {
        repl_val[(Anum_pg_ts_dict_dictinitoption - 1) as usize] =
            PointerGetDatum(serialize_deflist(dictoptions) as *const c_void);
    } else {
        repl_null[(Anum_pg_ts_dict_dictinitoption - 1) as usize] = true;
    }
    repl_repl[(Anum_pg_ts_dict_dictinitoption - 1) as usize] = true;

    newtup = heap_modify_tuple(tup, RelationGetDescr(rel),
                               repl_val.as_mut_ptr(), repl_null.as_mut_ptr(), repl_repl.as_mut_ptr());

    CatalogTupleUpdate(rel, &mut (*newtup).t_self, newtup);

    InvokeObjectPostAlterHook(TSDictionaryRelationId, dictId, 0);

    ObjectAddressSet(&mut address, TSDictionaryRelationId, dictId);

    /*
     * NOTE: because we only support altering the options, not the template,
     * there is no need to update dependencies.  This might have to change if
     * the options ever reference inside-the-database objects.
     */

    heap_freetuple(newtup);
    ReleaseSysCache(tup);

    table_close(rel, RowExclusiveLock);

    address
}

/* ---------------------- TS Template commands -----------------------*/

/*
 * lookup a template support function and return its OID (as a Datum)
 *
 * attnum is the pg_ts_template column the function will go into
 */
unsafe fn get_ts_template_func(defel: *mut DefElem, attnum: c_int) -> Datum {
    let funcName: *mut List = defGetQualifiedName(defel);
    let mut typeId: [Oid; 4] = [InvalidOid; 4];
    let retTypeId: Oid;
    let nargs: c_int;
    let procOid: Oid;

    retTypeId = INTERNALOID;
    typeId[0] = INTERNALOID;
    typeId[1] = INTERNALOID;
    typeId[2] = INTERNALOID;
    typeId[3] = INTERNALOID;
    match attnum {
        x if x == Anum_pg_ts_template_tmplinit => {
            nargs = 1;
        }
        x if x == Anum_pg_ts_template_tmpllexize => {
            nargs = 4;
        }
        _ => {
            /* should not be here */
            elog!(ERROR, "unrecognized attribute for text search template: {}", attnum);
            #[allow(unreachable_code)]
            { nargs = 0; } /* keep compiler quiet */
        }
    }

    procOid = LookupFuncName(funcName, nargs, typeId.as_ptr(), false);
    if get_func_rettype(procOid) != retTypeId {
        ereport!(ERROR,
            errmsg!("function {} should return type {}",
                std::ffi::CStr::from_ptr(func_signature_string(funcName, nargs, NIL_LIST, typeId.as_ptr())).to_string_lossy(),
                std::ffi::CStr::from_ptr(format_type_be(retTypeId)).to_string_lossy()));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    ObjectIdGetDatum(procOid)
}

/*
 * make pg_depend entries for a new pg_ts_template entry
 */
unsafe fn makeTSTemplateDependencies(tuple: HeapTuple) -> ObjectAddress {
    let tmpl: Form_pg_ts_template = GETSTRUCT(tuple) as Form_pg_ts_template;
    let mut myself: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let mut referenced: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let addrs: *mut ObjectAddresses;

    ObjectAddressSet(&mut myself, TSTemplateRelationId, (*tmpl).oid);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    addrs = new_object_addresses();

    /* dependency on namespace */
    ObjectAddressSet(&mut referenced, NamespaceRelationId, (*tmpl).tmplnamespace);
    add_exact_object_address(&referenced, addrs);

    /* dependencies on functions */
    ObjectAddressSet(&mut referenced, ProcedureRelationId, (*tmpl).tmpllexize);
    add_exact_object_address(&referenced, addrs);

    if OidIsValid((*tmpl).tmplinit) {
        referenced.objectId = (*tmpl).tmplinit;
        add_exact_object_address(&referenced, addrs);
    }

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    myself
}

/*
 * CREATE TEXT SEARCH TEMPLATE
 */
pub unsafe fn DefineTSTemplate(names: *mut List, parameters: *mut List) -> ObjectAddress {
    let tmplRel: Relation;
    let tup: HeapTuple;
    let mut values: [Datum; Natts_pg_ts_template] = [0; Natts_pg_ts_template];
    let mut nulls: [bool; Natts_pg_ts_template] = [false; Natts_pg_ts_template];
    let mut dname: NameData = std::mem::zeroed();
    let mut i: c_int;
    let tmplOid: Oid;
    let namespaceoid: Oid;
    let tmplname: *mut c_char;
    let address: ObjectAddress;

    if !superuser() {
        ereport!(ERROR,
            errmsg!("must be superuser to create text search templates"));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    /* Convert list of names to a name and namespace */
    let mut tmplname_out: *mut c_char = null_mut();
    namespaceoid = QualifiedNameGetCreationNamespace(names, &mut tmplname_out);
    tmplname = tmplname_out;

    tmplRel = table_open(TSTemplateRelationId, RowExclusiveLock);

    i = 0;
    while (i as usize) < Natts_pg_ts_template {
        nulls[i as usize] = false;
        values[i as usize] = ObjectIdGetDatum(InvalidOid);
        i += 1;
    }

    tmplOid = GetNewOidWithIndex(tmplRel, TSTemplateOidIndexId, Anum_pg_ts_dict_oid);
    values[(Anum_pg_ts_template_oid - 1) as usize] = ObjectIdGetDatum(tmplOid);
    namestrcpy(&mut dname, tmplname);
    values[(Anum_pg_ts_template_tmplname - 1) as usize] = NameGetDatum(&dname);
    values[(Anum_pg_ts_template_tmplnamespace - 1) as usize] = ObjectIdGetDatum(namespaceoid);

    /*
     * loop over the definition list and extract the information we need.
     */
    foreach!(pl, parameters, {
        let defel: *mut DefElem = lfirst(current_cell!(pl)) as *mut DefElem;

        if strcmp((*defel).defname, c"init".as_ptr()) == 0 {
            values[(Anum_pg_ts_template_tmplinit - 1) as usize] =
                get_ts_template_func(defel, Anum_pg_ts_template_tmplinit);
            nulls[(Anum_pg_ts_template_tmplinit - 1) as usize] = false;
        } else if strcmp((*defel).defname, c"lexize".as_ptr()) == 0 {
            values[(Anum_pg_ts_template_tmpllexize - 1) as usize] =
                get_ts_template_func(defel, Anum_pg_ts_template_tmpllexize);
            nulls[(Anum_pg_ts_template_tmpllexize - 1) as usize] = false;
        } else {
            ereport!(ERROR,
                errmsg!("text search template parameter \"{}\" not recognized",
                    std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        }
    });

    /*
     * Validation
     */
    if !OidIsValid(DatumGetObjectId(values[(Anum_pg_ts_template_tmpllexize - 1) as usize])) {
        ereport!(ERROR,
            errmsg!("text search template lexize method is required"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    /*
     * Looks good, insert
     */
    tup = heap_form_tuple(RelationGetDescr(tmplRel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(tmplRel, tup);

    address = makeTSTemplateDependencies(tup);

    /* Post creation hook for new text search template */
    InvokeObjectPostCreateHook(TSTemplateRelationId, tmplOid, 0);

    heap_freetuple(tup);

    table_close(tmplRel, RowExclusiveLock);

    address
}

/* ---------------------- TS Configuration commands -----------------------*/

/*
 * Finds syscache tuple of configuration.
 * Returns NULL if no such cfg.
 */
unsafe fn GetTSConfigTuple(names: *mut List) -> HeapTuple {
    let tup: HeapTuple;
    let cfgId: Oid;

    cfgId = get_ts_config_oid(names, true);
    if !OidIsValid(cfgId) {
        return null_mut();
    }

    tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgId));

    if !HeapTupleIsValid(tup) {
        /* should not happen */
        elog!(ERROR, "cache lookup failed for text search configuration {}", cfgId);
    }

    tup
}

/*
 * make pg_depend entries for a new or updated pg_ts_config entry
 *
 * Pass opened pg_ts_config_map relation if there might be any config map
 * entries for the config.
 */
unsafe fn makeConfigurationDependencies(tuple: HeapTuple, removeOld: bool, mapRel: Relation) -> ObjectAddress {
    let cfg: Form_pg_ts_config = GETSTRUCT(tuple) as Form_pg_ts_config;
    let addrs: *mut ObjectAddresses;
    let mut myself: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };
    let mut referenced: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };

    myself.classId = TSConfigRelationId;
    myself.objectId = (*cfg).oid;
    myself.objectSubId = 0;

    /* for ALTER case, first flush old dependencies, except extension deps */
    if removeOld {
        deleteDependencyRecordsFor(myself.classId, myself.objectId, true);
        deleteSharedDependencyRecordsFor(myself.classId, myself.objectId, 0);
    }

    /*
     * We use an ObjectAddresses list to remove possible duplicate
     * dependencies from the config map info.  The pg_ts_config items
     * shouldn't be duplicates, but might as well fold them all into one call.
     */
    addrs = new_object_addresses();

    /* dependency on namespace */
    referenced.classId = NamespaceRelationId;
    referenced.objectId = (*cfg).cfgnamespace;
    referenced.objectSubId = 0;
    add_exact_object_address(&referenced, addrs);

    /* dependency on owner */
    recordDependencyOnOwner(myself.classId, myself.objectId, (*cfg).cfgowner);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, removeOld);

    /* dependency on parser */
    referenced.classId = TSParserRelationId;
    referenced.objectId = (*cfg).cfgparser;
    referenced.objectSubId = 0;
    add_exact_object_address(&referenced, addrs);

    /* dependencies on dictionaries listed in config map */
    if !mapRel.is_null() {
        let mut skey: ScanKeyData = std::mem::zeroed();
        let scan: SysScanDesc;
        let mut maptup: HeapTuple;

        /* CCI to ensure we can see effects of caller's changes */
        CommandCounterIncrement();

        ScanKeyInit(&mut skey,
                    Anum_pg_ts_config_map_mapcfg,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(myself.objectId));

        scan = systable_beginscan(mapRel, TSConfigMapIndexId, true,
                                  null_mut(), 1, &mut skey);

        loop {
            maptup = systable_getnext(scan);
            if !HeapTupleIsValid(maptup) {
                break;
            }
            let cfgmap: Form_pg_ts_config_map = GETSTRUCT(maptup) as Form_pg_ts_config_map;

            referenced.classId = TSDictionaryRelationId;
            referenced.objectId = (*cfgmap).mapdict;
            referenced.objectSubId = 0;
            add_exact_object_address(&referenced, addrs);
        }

        systable_endscan(scan);
    }

    /* Record 'em (this includes duplicate elimination) */
    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);

    free_object_addresses(addrs);

    myself
}

/*
 * CREATE TEXT SEARCH CONFIGURATION
 */
pub unsafe fn DefineTSConfiguration(names: *mut List, parameters: *mut List, copied: *mut ObjectAddress) -> ObjectAddress {
    let cfgRel: Relation;
    let mut mapRel: Relation = null_mut();
    let mut tup: HeapTuple;
    let mut values: [Datum; Natts_pg_ts_config] = [0; Natts_pg_ts_config];
    let mut nulls: [bool; Natts_pg_ts_config] = [false; Natts_pg_ts_config];
    let aclresult: AclResult;
    let namespaceoid: Oid;
    let cfgname: *mut c_char;
    let mut cname: NameData = std::mem::zeroed();
    let mut sourceOid: Oid = InvalidOid;
    let mut prsOid: Oid = InvalidOid;
    let cfgOid: Oid;
    let address: ObjectAddress;

    /* Convert list of names to a name and namespace */
    let mut cfgname_out: *mut c_char = null_mut();
    namespaceoid = QualifiedNameGetCreationNamespace(names, &mut cfgname_out);
    cfgname = cfgname_out;

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(NamespaceRelationId, namespaceoid, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceoid));
    }

    /*
     * loop over the definition list and extract the information we need.
     */
    foreach!(pl, parameters, {
        let defel: *mut DefElem = lfirst(current_cell!(pl)) as *mut DefElem;

        if strcmp((*defel).defname, c"parser".as_ptr()) == 0 {
            prsOid = get_ts_parser_oid(defGetQualifiedName(defel), false);
        } else if strcmp((*defel).defname, c"copy".as_ptr()) == 0 {
            sourceOid = get_ts_config_oid(defGetQualifiedName(defel), false);
        } else {
            ereport!(ERROR,
                errmsg!("text search configuration parameter \"{}\" not recognized",
                    std::ffi::CStr::from_ptr((*defel).defname).to_string_lossy()));
            /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
        }
    });

    if OidIsValid(sourceOid) && OidIsValid(prsOid) {
        ereport!(ERROR,
            errmsg!("cannot specify both PARSER and COPY options"));
        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
    }

    /* make copied tsconfig available to callers */
    if !copied.is_null() && OidIsValid(sourceOid) {
        ObjectAddressSet(&mut *copied, TSConfigRelationId, sourceOid);
    }

    /*
     * Look up source config if given.
     */
    if OidIsValid(sourceOid) {
        let cfg: Form_pg_ts_config;

        tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(sourceOid));
        if !HeapTupleIsValid(tup) {
            elog!(ERROR, "cache lookup failed for text search configuration {}", sourceOid);
        }

        cfg = GETSTRUCT(tup) as Form_pg_ts_config;

        /* use source's parser */
        prsOid = (*cfg).cfgparser;

        ReleaseSysCache(tup);
    }

    /*
     * Validation
     */
    if !OidIsValid(prsOid) {
        ereport!(ERROR,
            errmsg!("text search parser is required"));
        /* C also: errcode(ERRCODE_INVALID_OBJECT_DEFINITION) */
    }

    cfgRel = table_open(TSConfigRelationId, RowExclusiveLock);

    /*
     * Looks good, build tuple and insert
     */
    /* memset(values, 0) / memset(nulls, false) handled by initializers above */

    cfgOid = GetNewOidWithIndex(cfgRel, TSConfigOidIndexId, Anum_pg_ts_config_oid);
    values[(Anum_pg_ts_config_oid - 1) as usize] = ObjectIdGetDatum(cfgOid);
    namestrcpy(&mut cname, cfgname);
    values[(Anum_pg_ts_config_cfgname - 1) as usize] = NameGetDatum(&cname);
    values[(Anum_pg_ts_config_cfgnamespace - 1) as usize] = ObjectIdGetDatum(namespaceoid);
    values[(Anum_pg_ts_config_cfgowner - 1) as usize] = ObjectIdGetDatum(GetUserId());
    values[(Anum_pg_ts_config_cfgparser - 1) as usize] = ObjectIdGetDatum(prsOid);

    tup = heap_form_tuple(RelationGetDescr(cfgRel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(cfgRel, tup);

    if OidIsValid(sourceOid) {
        /*
         * Copy token-dicts map from source config
         */
        let mut skey: ScanKeyData = std::mem::zeroed();
        let scan: SysScanDesc;
        let mut maptup: HeapTuple;
        let mapDesc: TupleDesc;
        let slot: *mut TupleTableSlot;
        let indstate: CatalogIndexState;
        let max_slots: c_int;
        let mut slot_init_count: c_int;
        let mut slot_stored_count: c_int;

        mapRel = table_open(TSConfigMapRelationId, RowExclusiveLock);
        mapDesc = RelationGetDescr(mapRel);

        indstate = CatalogOpenIndexes(mapRel);

        /*
         * Allocate the slots to use, but delay costly initialization until we
         * know that they will be used.
         */
        max_slots = (MAX_CATALOG_MULTI_INSERT_BYTES
            / core::mem::size_of::<FormData_pg_ts_config_map>()) as c_int;
        slot = palloc(core::mem::size_of::<*mut TupleTableSlot>() * max_slots as usize)
            as *mut TupleTableSlot;

        ScanKeyInit(&mut skey,
                    Anum_pg_ts_config_map_mapcfg,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(sourceOid));

        scan = systable_beginscan(mapRel, TSConfigMapIndexId, true,
                                  null_mut(), 1, &mut skey);

        /* number of slots currently storing tuples */
        slot_stored_count = 0;
        /* number of slots currently initialized */
        slot_init_count = 0;

        loop {
            maptup = systable_getnext(scan);
            if !HeapTupleIsValid(maptup) {
                break;
            }
            let cfgmap: Form_pg_ts_config_map = GETSTRUCT(maptup) as Form_pg_ts_config_map;

            if slot_init_count < max_slots {
                *slot.add(slot_stored_count as usize) =
                    MakeSingleTupleTableSlot(mapDesc, &TTSOpsHeapTuple);
                slot_init_count += 1;
            }

            let cur = *slot.add(slot_stored_count as usize);
            ExecClearTuple(cur);

            std::ptr::write_bytes((*cur).tts_isnull, 0,
                TupleDescNatts((*cur).tts_tupleDescriptor) as usize);

            *(*cur).tts_values.add((Anum_pg_ts_config_map_mapcfg - 1) as usize) = cfgOid as Datum;
            *(*cur).tts_values.add((Anum_pg_ts_config_map_maptokentype - 1) as usize) = (*cfgmap).maptokentype as Datum;
            *(*cur).tts_values.add((Anum_pg_ts_config_map_mapseqno - 1) as usize) = (*cfgmap).mapseqno as Datum;
            *(*cur).tts_values.add((Anum_pg_ts_config_map_mapdict - 1) as usize) = (*cfgmap).mapdict as Datum;

            ExecStoreVirtualTuple(cur);
            slot_stored_count += 1;

            /* If slots are full, insert a batch of tuples */
            if slot_stored_count == max_slots {
                CatalogTuplesMultiInsertWithInfo(mapRel, slot, slot_stored_count, indstate);
                slot_stored_count = 0;
            }
        }

        /* Insert any tuples left in the buffer */
        if slot_stored_count > 0 {
            CatalogTuplesMultiInsertWithInfo(mapRel, slot, slot_stored_count, indstate);
        }

        let mut i: c_int = 0;
        while i < slot_init_count {
            ExecDropSingleTupleTableSlot(*slot.add(i as usize));
            i += 1;
        }

        systable_endscan(scan);
        CatalogCloseIndexes(indstate);
    }

    address = makeConfigurationDependencies(tup, false, mapRel);

    /* Post creation hook for new text search configuration */
    InvokeObjectPostCreateHook(TSConfigRelationId, cfgOid, 0);

    heap_freetuple(tup);

    if !mapRel.is_null() {
        table_close(mapRel, RowExclusiveLock);
    }
    table_close(cfgRel, RowExclusiveLock);

    address
}

/*
 * Guts of TS configuration deletion.
 */
pub unsafe fn RemoveTSConfigurationById(cfgId: Oid) {
    let relCfg: Relation;
    let relMap: Relation;
    let mut tup: HeapTuple;
    let mut skey: ScanKeyData = std::mem::zeroed();
    let scan: SysScanDesc;

    /* Remove the pg_ts_config entry */
    relCfg = table_open(TSConfigRelationId, RowExclusiveLock);

    tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(cfgId));

    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for text search dictionary {}", cfgId);
    }

    CatalogTupleDelete(relCfg, &mut (*tup).t_self);

    ReleaseSysCache(tup);

    table_close(relCfg, RowExclusiveLock);

    /* Remove any pg_ts_config_map entries */
    relMap = table_open(TSConfigMapRelationId, RowExclusiveLock);

    ScanKeyInit(&mut skey,
                Anum_pg_ts_config_map_mapcfg,
                BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(cfgId));

    scan = systable_beginscan(relMap, TSConfigMapIndexId, true,
                              null_mut(), 1, &mut skey);

    loop {
        tup = systable_getnext(scan);
        if !HeapTupleIsValid(tup) {
            break;
        }
        CatalogTupleDelete(relMap, &mut (*tup).t_self);
    }

    systable_endscan(scan);

    table_close(relMap, RowExclusiveLock);
}

/*
 * ALTER TEXT SEARCH CONFIGURATION - main entry point
 */
pub unsafe fn AlterTSConfiguration(stmt: *mut AlterTSConfigurationStmt) -> ObjectAddress {
    let tup: HeapTuple;
    let cfgId: Oid;
    let relMap: Relation;
    let mut address: ObjectAddress = ObjectAddress { classId: 0, objectId: 0, objectSubId: 0 };

    /* Find the configuration */
    tup = GetTSConfigTuple((*stmt).cfgname);
    if !HeapTupleIsValid(tup) {
        ereport!(ERROR,
            errmsg!("text search configuration \"{}\" does not exist",
                std::ffi::CStr::from_ptr(NameListToString((*stmt).cfgname)).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    cfgId = (*(GETSTRUCT(tup) as Form_pg_ts_config)).oid;

    /* must be owner */
    if !object_ownercheck(TSConfigRelationId, cfgId, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TSCONFIGURATION,
                       NameListToString((*stmt).cfgname));
    }

    relMap = table_open(TSConfigMapRelationId, RowExclusiveLock);

    /* Add or drop mappings */
    if !(*stmt).dicts.is_null() {
        MakeConfigurationMapping(stmt, tup, relMap);
    } else if !(*stmt).tokentype.is_null() {
        DropConfigurationMapping(stmt, tup, relMap);
    }

    /* Update dependencies */
    makeConfigurationDependencies(tup, true, relMap);

    InvokeObjectPostAlterHook(TSConfigRelationId, cfgId, 0);

    ObjectAddressSet(&mut address, TSConfigRelationId, cfgId);

    table_close(relMap, RowExclusiveLock);

    ReleaseSysCache(tup);

    address
}

/*
 * Check whether a token type name is a member of a TSTokenTypeItem list.
 */
unsafe fn tstoken_list_member(token_name: *mut c_char, tokens: *mut List) -> bool {
    let mut found: bool = false;

    foreach!(c, tokens, {
        let ts: *mut TSTokenTypeItem = lfirst(current_cell!(c)) as *mut TSTokenTypeItem;

        if strcmp(token_name, (*ts).name) == 0 {
            found = true;
            break;
        }
    });

    found
}

/*
 * Translate a list of token type names to a list of unique TSTokenTypeItem.
 *
 * Duplicated entries list are removed from tokennames.
 */
unsafe fn getTokenTypes(prsId: Oid, tokennames: *mut List) -> *mut List {
    let prs: *mut TSParserCacheEntry = lookup_ts_parser_cache(prsId);
    let list: *mut LexDescr;
    let mut result: *mut List = NIL_LIST;
    let ntoken: c_int;

    ntoken = list_length(tokennames);
    if ntoken == 0 {
        return NIL_LIST;
    }

    if !OidIsValid((*prs).lextypeOid) {
        elog!(ERROR, "method lextype isn't defined for text search parser {}", prsId);
    }

    /* lextype takes one dummy argument */
    list = DatumGetPointer(OidFunctionCall1((*prs).lextypeOid, 0 as Datum)) as *mut LexDescr;

    foreach!(tn, tokennames, {
        let val = lfirst_node!(crate::nodes::value::String, T_String, current_cell!(tn));
        let mut found: bool = false;
        let mut j: c_int;

        /* Skip if this token is already in the result */
        if tstoken_list_member(strVal!(val), result) {
            // continue
        } else {
            j = 0;
            while !list.is_null() && (*list.add(j as usize)).lexid != 0 {
                if strcmp(strVal!(val), (*list.add(j as usize)).alias) == 0 {
                    let ts: *mut TSTokenTypeItem =
                        palloc0(core::mem::size_of::<TSTokenTypeItem>()) as *mut TSTokenTypeItem;

                    (*ts).num = (*list.add(j as usize)).lexid;
                    (*ts).name = pstrdup(strVal!(val));
                    result = lappend(result, ts as *mut c_void);
                    found = true;
                    break;
                }
                j += 1;
            }
            if !found {
                ereport!(ERROR,
                    errmsg!("token type \"{}\" does not exist",
                        std::ffi::CStr::from_ptr(strVal!(val)).to_string_lossy()));
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            }
        }
    });

    result
}

/*
 * ALTER TEXT SEARCH CONFIGURATION ADD/ALTER MAPPING
 */
unsafe fn MakeConfigurationMapping(stmt: *mut AlterTSConfigurationStmt, tup: HeapTuple, relMap: Relation) {
    let tsform: Form_pg_ts_config;
    let cfgId: Oid;
    let mut skey: [ScanKeyData; 2] = [std::mem::zeroed(), std::mem::zeroed()];
    let mut scan: SysScanDesc;
    let mut maptup: HeapTuple;
    let mut i: c_int;
    let mut j: c_int;
    let prsId: Oid;
    let tokens: *mut List;
    let ntoken: c_int;
    let dictIds: *mut Oid;
    let ndict: c_int;
    let indstate: CatalogIndexState;

    tsform = GETSTRUCT(tup) as Form_pg_ts_config;
    cfgId = (*tsform).oid;
    prsId = (*tsform).cfgparser;

    tokens = getTokenTypes(prsId, (*stmt).tokentype);
    ntoken = list_length(tokens);

    if (*stmt).r#override {
        /*
         * delete maps for tokens if they exist and command was ALTER
         */
        foreach!(c, tokens, {
            let ts: *mut TSTokenTypeItem = lfirst(current_cell!(c)) as *mut TSTokenTypeItem;

            ScanKeyInit(&mut skey[0],
                        Anum_pg_ts_config_map_mapcfg,
                        BTEqualStrategyNumber, F_OIDEQ,
                        ObjectIdGetDatum(cfgId));
            ScanKeyInit(&mut skey[1],
                        Anum_pg_ts_config_map_maptokentype,
                        BTEqualStrategyNumber, F_INT4EQ,
                        Int32GetDatum((*ts).num));

            scan = systable_beginscan(relMap, TSConfigMapIndexId, true,
                                      null_mut(), 2, skey.as_mut_ptr());

            loop {
                maptup = systable_getnext(scan);
                if !HeapTupleIsValid(maptup) {
                    break;
                }
                CatalogTupleDelete(relMap, &mut (*maptup).t_self);
            }

            systable_endscan(scan);
        });
    }

    /*
     * Convert list of dictionary names to array of dict OIDs
     */
    ndict = list_length((*stmt).dicts);
    dictIds = palloc(core::mem::size_of::<Oid>() * ndict as usize) as *mut Oid;
    i = 0;
    foreach!(c, (*stmt).dicts, {
        let names: *mut List = lfirst(current_cell!(c)) as *mut List;

        *dictIds.add(i as usize) = get_ts_dict_oid(names, false);
        i += 1;
    });

    indstate = CatalogOpenIndexes(relMap);

    if (*stmt).replace {
        /*
         * Replace a specific dictionary in existing entries
         */
        let dictOld: Oid = *dictIds.add(0);
        let dictNew: Oid = *dictIds.add(1);

        ScanKeyInit(&mut skey[0],
                    Anum_pg_ts_config_map_mapcfg,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(cfgId));

        scan = systable_beginscan(relMap, TSConfigMapIndexId, true,
                                  null_mut(), 1, skey.as_mut_ptr());

        loop {
            maptup = systable_getnext(scan);
            if !HeapTupleIsValid(maptup) {
                break;
            }
            let cfgmap: Form_pg_ts_config_map = GETSTRUCT(maptup) as Form_pg_ts_config_map;

            /*
             * check if it's one of target token types
             */
            if !tokens.is_null() {
                let mut tokmatch: bool = false;

                foreach!(c, tokens, {
                    let ts: *mut TSTokenTypeItem = lfirst(current_cell!(c)) as *mut TSTokenTypeItem;

                    if (*cfgmap).maptokentype == (*ts).num {
                        tokmatch = true;
                        break;
                    }
                });
                if !tokmatch {
                    continue;
                }
            }

            /*
             * replace dictionary if match
             */
            if (*cfgmap).mapdict == dictOld {
                let mut repl_val: [Datum; Natts_pg_ts_config_map] = [0; Natts_pg_ts_config_map];
                let mut repl_null: [bool; Natts_pg_ts_config_map] = [false; Natts_pg_ts_config_map];
                let mut repl_repl: [bool; Natts_pg_ts_config_map] = [false; Natts_pg_ts_config_map];
                let newtup: HeapTuple;

                /* memset(repl_val, 0) / repl_null / repl_repl handled by initializers */

                repl_val[(Anum_pg_ts_config_map_mapdict - 1) as usize] = ObjectIdGetDatum(dictNew);
                repl_repl[(Anum_pg_ts_config_map_mapdict - 1) as usize] = true;

                newtup = heap_modify_tuple(maptup, RelationGetDescr(relMap),
                                           repl_val.as_mut_ptr(), repl_null.as_mut_ptr(), repl_repl.as_mut_ptr());
                CatalogTupleUpdateWithInfo(relMap, &mut (*newtup).t_self, newtup, indstate);
            }
        }

        systable_endscan(scan);
    } else {
        let slot: *mut TupleTableSlot;
        let mut slotCount: c_int = 0;
        let nslots: c_int;

        /* Allocate the slots to use and initialize them */
        nslots = Min(ntoken * ndict,
                     (MAX_CATALOG_MULTI_INSERT_BYTES
                        / core::mem::size_of::<FormData_pg_ts_config_map>()) as c_int);
        slot = palloc(core::mem::size_of::<*mut TupleTableSlot>() * nslots as usize)
            as *mut TupleTableSlot;
        i = 0;
        while i < nslots {
            *slot.add(i as usize) = MakeSingleTupleTableSlot(RelationGetDescr(relMap), &TTSOpsHeapTuple);
            i += 1;
        }

        /*
         * Insertion of new entries
         */
        foreach!(c, tokens, {
            let ts: *mut TSTokenTypeItem = lfirst(current_cell!(c)) as *mut TSTokenTypeItem;

            j = 0;
            while j < ndict {
                let cur = *slot.add(slotCount as usize);
                ExecClearTuple(cur);

                std::ptr::write_bytes((*cur).tts_isnull, 0,
                    TupleDescNatts((*cur).tts_tupleDescriptor) as usize);

                *(*cur).tts_values.add((Anum_pg_ts_config_map_mapcfg - 1) as usize) = ObjectIdGetDatum(cfgId);
                *(*cur).tts_values.add((Anum_pg_ts_config_map_maptokentype - 1) as usize) = Int32GetDatum((*ts).num);
                *(*cur).tts_values.add((Anum_pg_ts_config_map_mapseqno - 1) as usize) = Int32GetDatum(j + 1);
                *(*cur).tts_values.add((Anum_pg_ts_config_map_mapdict - 1) as usize) = ObjectIdGetDatum(*dictIds.add(j as usize));

                ExecStoreVirtualTuple(cur);
                slotCount += 1;

                /* If slots are full, insert a batch of tuples */
                if slotCount == nslots {
                    CatalogTuplesMultiInsertWithInfo(relMap, slot, slotCount, indstate);
                    slotCount = 0;
                }
                j += 1;
            }
        });

        /* Insert any tuples left in the buffer */
        if slotCount > 0 {
            CatalogTuplesMultiInsertWithInfo(relMap, slot, slotCount, indstate);
        }

        i = 0;
        while i < nslots {
            ExecDropSingleTupleTableSlot(*slot.add(i as usize));
            i += 1;
        }
    }

    /* clean up */
    CatalogCloseIndexes(indstate);

    EventTriggerCollectAlterTSConfig(stmt, cfgId, dictIds, ndict);
}

/*
 * ALTER TEXT SEARCH CONFIGURATION DROP MAPPING
 */
unsafe fn DropConfigurationMapping(stmt: *mut AlterTSConfigurationStmt, tup: HeapTuple, relMap: Relation) {
    let tsform: Form_pg_ts_config;
    let cfgId: Oid;
    let mut skey: [ScanKeyData; 2] = [std::mem::zeroed(), std::mem::zeroed()];
    let mut maptup: HeapTuple;
    let prsId: Oid;
    let tokens: *mut List;

    tsform = GETSTRUCT(tup) as Form_pg_ts_config;
    cfgId = (*tsform).oid;
    prsId = (*tsform).cfgparser;

    tokens = getTokenTypes(prsId, (*stmt).tokentype);

    foreach!(c, tokens, {
        let ts: *mut TSTokenTypeItem = lfirst(current_cell!(c)) as *mut TSTokenTypeItem;
        let mut found: bool = false;

        ScanKeyInit(&mut skey[0],
                    Anum_pg_ts_config_map_mapcfg,
                    BTEqualStrategyNumber, F_OIDEQ,
                    ObjectIdGetDatum(cfgId));
        ScanKeyInit(&mut skey[1],
                    Anum_pg_ts_config_map_maptokentype,
                    BTEqualStrategyNumber, F_INT4EQ,
                    Int32GetDatum((*ts).num));

        let scan: SysScanDesc = systable_beginscan(relMap, TSConfigMapIndexId, true,
                                      null_mut(), 2, skey.as_mut_ptr());

        loop {
            maptup = systable_getnext(scan);
            if !HeapTupleIsValid(maptup) {
                break;
            }
            CatalogTupleDelete(relMap, &mut (*maptup).t_self);
            found = true;
        }

        systable_endscan(scan);

        if !found {
            if !(*stmt).missing_ok {
                ereport!(ERROR,
                    errmsg!("mapping for token type \"{}\" does not exist",
                        std::ffi::CStr::from_ptr((*ts).name).to_string_lossy()));
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            } else {
                ereport!(NOTICE,
                    errmsg!("mapping for token type \"{}\" does not exist, skipping",
                        std::ffi::CStr::from_ptr((*ts).name).to_string_lossy()));
            }
        }
    });

    EventTriggerCollectAlterTSConfig(stmt, cfgId, null_mut(), 0);
}


/*
 * Serialize dictionary options, producing a TEXT datum from a List of DefElem
 *
 * This is used to form the value stored in pg_ts_dict.dictinitoption.
 * For the convenience of pg_dump, the output is formatted exactly as it
 * would need to appear in CREATE TEXT SEARCH DICTIONARY to reproduce the
 * same options.
 */
pub unsafe fn serialize_deflist(deflist: *mut List) -> *mut text {
    let result: *mut text;
    let mut buf: StringInfoData = std::mem::zeroed();

    initStringInfo(&mut buf);

    foreach!(l, deflist, {
        let defel: *mut DefElem = lfirst(current_cell!(l)) as *mut DefElem;
        let mut val: *mut c_char = defGetString(defel);

        appendStringInfo!(&mut buf, "{} = ",
            std::ffi::CStr::from_ptr(quote_identifier((*defel).defname)).to_string_lossy());

        /*
         * If the value is a T_Integer or T_Float, emit it without quotes,
         * otherwise with quotes.  This is essential to allow correct
         * reconstruction of the node type as well as the value.
         */
        if IsA!((*defel).arg, T_Integer) || IsA!((*defel).arg, T_Float) {
            appendStringInfoString(&mut buf, val);
        } else {
            /* If backslashes appear, force E syntax to quote them safely */
            if !strchr(val, b'\\' as c_int).is_null() {
                appendStringInfoChar(&mut buf, ESCAPE_STRING_SYNTAX);
            }
            appendStringInfoChar(&mut buf, b'\'' as c_char);
            while *val != 0 {
                let ch: c_char = *val;
                val = val.add(1);

                if SQL_STR_DOUBLE(ch, true) {
                    appendStringInfoChar(&mut buf, ch);
                }
                appendStringInfoChar(&mut buf, ch);
            }
            appendStringInfoChar(&mut buf, b'\'' as c_char);
        }
        if !lnext(deflist, current_cell!(l)).is_null() {
            appendStringInfoString(&mut buf, c", ".as_ptr());
        }
    });

    result = cstring_to_text_with_len(buf.data, buf.len);
    pfree(buf.data as *mut c_void);
    result
}

/*
 * Deserialize dictionary options, reconstructing a List of DefElem from TEXT
 *
 * This is also used for prsheadline options, so for backward compatibility
 * we need to accept a few things serialize_deflist() will never emit:
 * in particular, unquoted and double-quoted strings.
 */
#[derive(Clone, Copy, PartialEq, Eq)]
enum ds_state {
    CS_WAITKEY,
    CS_INKEY,
    CS_INQKEY,
    CS_WAITEQ,
    CS_WAITVALUE,
    CS_INSQVALUE,
    CS_INDQVALUE,
    CS_INWVALUE,
}
use ds_state::*;

pub unsafe fn deserialize_deflist(txt: Datum) -> *mut List {
    let in_: *mut text = DatumGetTextPP(txt); /* in case it's toasted */
    let mut result: *mut List = NIL_LIST;
    let len: c_int = VARSIZE_ANY_EXHDR(in_);
    let mut ptr: *mut c_char;
    let endptr: *mut c_char;
    let workspace: *mut c_char;
    let mut wsptr: *mut c_char = null_mut();
    let mut startvalue: *mut c_char = null_mut();
    let mut state: ds_state = CS_WAITKEY;

    workspace = palloc((len + 1) as usize) as *mut c_char; /* certainly enough room */
    ptr = VARDATA_ANY(in_);
    endptr = ptr.add(len as usize);
    while ptr < endptr {
        match state {
            CS_WAITKEY => {
                if isspace_c(*ptr as c_uchar) || *ptr == b',' as c_char {
                    // continue
                } else if *ptr == b'"' as c_char {
                    wsptr = workspace;
                    state = CS_INQKEY;
                } else {
                    wsptr = workspace;
                    *wsptr = *ptr;
                    wsptr = wsptr.add(1);
                    state = CS_INKEY;
                }
            }
            CS_INKEY => {
                if isspace_c(*ptr as c_uchar) {
                    *wsptr = b'\0' as c_char;
                    wsptr = wsptr.add(1);
                    state = CS_WAITEQ;
                } else if *ptr == b'=' as c_char {
                    *wsptr = b'\0' as c_char;
                    wsptr = wsptr.add(1);
                    state = CS_WAITVALUE;
                } else {
                    *wsptr = *ptr;
                    wsptr = wsptr.add(1);
                }
            }
            CS_INQKEY => {
                if *ptr == b'"' as c_char {
                    if ptr.add(1) < endptr && *ptr.add(1) == b'"' as c_char {
                        /* copy only one of the two quotes */
                        *wsptr = *ptr;
                        wsptr = wsptr.add(1);
                        ptr = ptr.add(1);
                    } else {
                        *wsptr = b'\0' as c_char;
                        wsptr = wsptr.add(1);
                        state = CS_WAITEQ;
                    }
                } else {
                    *wsptr = *ptr;
                    wsptr = wsptr.add(1);
                }
            }
            CS_WAITEQ => {
                if *ptr == b'=' as c_char {
                    state = CS_WAITVALUE;
                } else if !isspace_c(*ptr as c_uchar) {
                    ereport!(ERROR,
                        errmsg!("invalid parameter list format: \"{}\"",
                            std::ffi::CStr::from_ptr(text_to_cstring(in_)).to_string_lossy()));
                    /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
                }
            }
            CS_WAITVALUE => {
                if *ptr == b'\'' as c_char {
                    startvalue = wsptr;
                    state = CS_INSQVALUE;
                } else if *ptr == b'E' as c_char && ptr.add(1) < endptr && *ptr.add(1) == b'\'' as c_char {
                    ptr = ptr.add(1);
                    startvalue = wsptr;
                    state = CS_INSQVALUE;
                } else if *ptr == b'"' as c_char {
                    startvalue = wsptr;
                    state = CS_INDQVALUE;
                } else if !isspace_c(*ptr as c_uchar) {
                    startvalue = wsptr;
                    *wsptr = *ptr;
                    wsptr = wsptr.add(1);
                    state = CS_INWVALUE;
                }
            }
            CS_INSQVALUE => {
                if *ptr == b'\'' as c_char {
                    if ptr.add(1) < endptr && *ptr.add(1) == b'\'' as c_char {
                        /* copy only one of the two quotes */
                        *wsptr = *ptr;
                        wsptr = wsptr.add(1);
                        ptr = ptr.add(1);
                    } else {
                        *wsptr = b'\0' as c_char;
                        wsptr = wsptr.add(1);
                        result = lappend(result,
                            buildDefItem(workspace, startvalue, true) as *mut c_void);
                        state = CS_WAITKEY;
                    }
                } else if *ptr == b'\\' as c_char {
                    if ptr.add(1) < endptr && *ptr.add(1) == b'\\' as c_char {
                        /* copy only one of the two backslashes */
                        *wsptr = *ptr;
                        wsptr = wsptr.add(1);
                        ptr = ptr.add(1);
                    } else {
                        *wsptr = *ptr;
                        wsptr = wsptr.add(1);
                    }
                } else {
                    *wsptr = *ptr;
                    wsptr = wsptr.add(1);
                }
            }
            CS_INDQVALUE => {
                if *ptr == b'"' as c_char {
                    if ptr.add(1) < endptr && *ptr.add(1) == b'"' as c_char {
                        /* copy only one of the two quotes */
                        *wsptr = *ptr;
                        wsptr = wsptr.add(1);
                        ptr = ptr.add(1);
                    } else {
                        *wsptr = b'\0' as c_char;
                        wsptr = wsptr.add(1);
                        result = lappend(result,
                            buildDefItem(workspace, startvalue, true) as *mut c_void);
                        state = CS_WAITKEY;
                    }
                } else {
                    *wsptr = *ptr;
                    wsptr = wsptr.add(1);
                }
            }
            CS_INWVALUE => {
                if *ptr == b',' as c_char || isspace_c(*ptr as c_uchar) {
                    *wsptr = b'\0' as c_char;
                    wsptr = wsptr.add(1);
                    result = lappend(result,
                        buildDefItem(workspace, startvalue, false) as *mut c_void);
                    state = CS_WAITKEY;
                } else {
                    *wsptr = *ptr;
                    wsptr = wsptr.add(1);
                }
            }
        }
        ptr = ptr.add(1);
    }

    if state == CS_INWVALUE {
        *wsptr = b'\0' as c_char;
        wsptr = wsptr.add(1);
        result = lappend(result,
            buildDefItem(workspace, startvalue, false) as *mut c_void);
    } else if state != CS_WAITKEY {
        ereport!(ERROR,
            errmsg!("invalid parameter list format: \"{}\"",
                std::ffi::CStr::from_ptr(text_to_cstring(in_)).to_string_lossy()));
        /* C also: errcode(ERRCODE_SYNTAX_ERROR) */
    }
    let _ = wsptr;

    pfree(workspace as *mut c_void);

    result
}

/*
 * Build one DefElem for deserialize_deflist
 */
unsafe fn buildDefItem(name: *const c_char, val: *const c_char, was_quoted: bool) -> *mut DefElem {
    /* If input was quoted, always emit as string */
    if !was_quoted && *val != 0 {
        let v: c_int;
        let mut endptr: *mut c_char = null_mut();

        /* Try to parse as an integer */
        set_errno(0);
        v = strtoint(val, &mut endptr, 10);
        if get_errno() == 0 && *endptr == 0 {
            return makeDefElem(pstrdup(name), makeInteger(v) as *mut Node, -1);
        }
        /* Nope, how about as a float? */
        set_errno(0);
        let _ = strtod(val, &mut endptr);
        if get_errno() == 0 && *endptr == 0 {
            return makeDefElem(pstrdup(name), makeFloat(pstrdup(val)) as *mut Node, -1);
        }

        if strcmp(val, c"true".as_ptr()) == 0 {
            return makeDefElem(pstrdup(name), makeBoolean(true) as *mut Node, -1);
        }
        if strcmp(val, c"false".as_ptr()) == 0 {
            return makeDefElem(pstrdup(name), makeBoolean(false) as *mut Node, -1);
        }
    }
    /* Just make it a string */
    makeDefElem(pstrdup(name), makeString(pstrdup(val)) as *mut Node, -1)
}

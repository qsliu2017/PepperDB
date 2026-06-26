//! Translated from PostgreSQL src/include/catalog/pg_proc.h

#![allow(
    clippy::fn_params_excessive_bools,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::c::{float4, regproc, text, varlena, NameData};
use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// pg_list is tombstoned; List* collapses to Vec. Element type varies per call.
pub type List<T = ()> = Vec<T>;

// BKI_BOOTSTRAP BKI_ROWTYPE_OID(81,ProcedureRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const ProcedureRelationId: Oid = Oid(1255);
pub const ProcedureRelation_Rowtype_Id: Oid = Oid(81);

// oidvector / pg_node_tree / aclitem catalog fields are varlena; modeled here.
pub type Oidvector = text;
pub type PgNodeTree = text;
pub type Aclitem = text;

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_proc {
    pub oid: Oid,
    pub proname: NameData,
    pub pronamespace: Oid, // BKI_LOOKUP(pg_namespace)
    pub proowner: Oid,     // BKI_LOOKUP(pg_authid)
    pub prolang: Oid,      // BKI_LOOKUP(pg_language)
    pub procost: float4,
    pub prorows: float4,
    pub provariadic: Oid, // BKI_LOOKUP_OPT(pg_type)
    pub prosupport: regproc, // BKI_LOOKUP_OPT(pg_proc)
    pub prokind: i8,         // see PROKIND_*
    pub prosecdef: bool,
    pub proleakproof: bool,
    pub proisstrict: bool,
    pub proretset: bool,
    pub provolatile: i8, // see PROVOLATILE_*
    pub proparallel: i8, // see PROPARALLEL_*
    pub pronargs: i16,
    pub pronargdefaults: i16,
    pub prorettype: Oid, // BKI_LOOKUP(pg_type)
    // variable-length, but direct access allowed:
    pub proargtypes: Oidvector, // BKI_LOOKUP(pg_type) BKI_FORCE_NOT_NULL
    // CATALOG_VARLEN (not in fixed part):
    pub proallargtypes: [Oid; 1],  // Oid[] (NULL if IN only)
    pub proargmodes: [i8; 1],      // char[] (NULL if IN only)
    pub proargnames: [text; 1],    // text[] (NULL if no names)
    pub proargdefaults: PgNodeTree, // list of default expr trees
    pub protrftypes: [Oid; 1],     // Oid[] transform types
    pub prosrc: text,              // BKI_FORCE_NOT_NULL
    pub probin: text,              // can be NULL
    pub prosqlbody: PgNodeTree,    // pre-parsed SQL function body
    pub proconfig: [text; 1],      // text[] procedure-local GUCs
    pub proacl: [Aclitem; 1],      // aclitem[]
}

pub type Form_pg_proc = *mut FormData_pg_proc; // TODO(ptr)

// DECLARE_TOAST(pg_proc, 2836, 2837)
// DECLARE_UNIQUE_INDEX_PKEY(pg_proc_oid_index, 2690, ProcedureOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_proc_proname_args_nsp_index, 2691, ProcedureNameArgsNspIndexId, ...)
// MAKE_SYSCACHE(PROCOID, pg_proc_oid_index, 128)
// MAKE_SYSCACHE(PROCNAMEARGSNSP, pg_proc_proname_args_nsp_index, 128)

// Symbolic values for prokind column.
pub const PROKIND_FUNCTION: i8 = b'f' as i8;
pub const PROKIND_AGGREGATE: i8 = b'a' as i8;
pub const PROKIND_WINDOW: i8 = b'w' as i8;
pub const PROKIND_PROCEDURE: i8 = b'p' as i8;

// Symbolic values for provolatile column.
pub const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;
pub const PROVOLATILE_STABLE: i8 = b's' as i8;
pub const PROVOLATILE_VOLATILE: i8 = b'v' as i8;

// Symbolic values for proparallel column.
pub const PROPARALLEL_SAFE: i8 = b's' as i8;
pub const PROPARALLEL_RESTRICTED: i8 = b'r' as i8;
pub const PROPARALLEL_UNSAFE: i8 = b'u' as i8;

// Symbolic values for proargmodes column.
pub const PROARGMODE_IN: i8 = b'i' as i8;
pub const PROARGMODE_OUT: i8 = b'o' as i8;
pub const PROARGMODE_INOUT: i8 = b'b' as i8;
pub const PROARGMODE_VARIADIC: i8 = b'v' as i8;
pub const PROARGMODE_TABLE: i8 = b't' as i8;

pub fn ProcedureCreate(
    _procedure_name: &str,
    _proc_namespace: Oid,
    _replace: bool,
    _returns_set: bool,
    _return_type: Oid,
    _proowner: Oid,
    _language_object_id: Oid,
    _language_validator: Oid,
    _prosrc: &str,
    _probin: &str,
    _prosqlbody: &Node,
    _prokind: i8,
    _security_definer: bool,
    _is_leak_proof: bool,
    _is_strict: bool,
    _volatility: i8,
    _parallel: i8,
    _parameter_types: &varlena,
    _all_parameter_types: Datum,
    _parameter_modes: Datum,
    _parameter_names: Datum,
    _parameter_defaults: &List,
    _trftypes: Datum,
    _trfoids: &List,
    _proconfig: Datum,
    _prosupport: Oid,
    _procost: float4,
    _prorows: float4,
) -> ObjectAddress {
    unimplemented!()
}

pub fn function_parse_error_transpose(_prosrc: &str) -> bool {
    unimplemented!()
}

pub fn oid_array_to_list(_datum: Datum) -> List {
    unimplemented!()
}

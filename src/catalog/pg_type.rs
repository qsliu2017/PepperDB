//! Translated from PostgreSQL src/include/catalog/pg_type.h

#![allow(
    clippy::fn_params_excessive_bools,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::access::htup::HeapTuple;
use crate::c::{regproc, text, NameData};
use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::Node;
use crate::postgres_ext::Oid;
use crate::utils::acl::AclItem;

// BKI_BOOTSTRAP BKI_ROWTYPE_OID(71,TypeRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const TypeRelationId: Oid = Oid::new(1247);
pub const TypeRelation_Rowtype_Id: Oid = Oid::new(71);

// pg_node_tree catalog field is varlena (text); aclitem is the fixed acl struct.
pub type PgNodeTree = text;

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_type {
    pub oid: Oid,
    pub typname: NameData,
    pub typnamespace: Oid, // BKI_LOOKUP(pg_namespace)
    pub typowner: Oid,     // BKI_LOOKUP(pg_authid)
    pub typlen: i16,
    pub typbyval: bool,
    pub typtype: i8,     // see TYPTYPE_*
    pub typcategory: i8, // see TYPCATEGORY_*
    pub typispreferred: bool,
    pub typisdefined: bool,
    pub typdelim: i8,
    pub typrelid: Oid,       // BKI_LOOKUP_OPT(pg_class)
    pub typsubscript: regproc, // BKI_LOOKUP_OPT(pg_proc)
    pub typelem: Oid,        // BKI_LOOKUP_OPT(pg_type)
    pub typarray: Oid,       // BKI_LOOKUP_OPT(pg_type)
    pub typinput: regproc,   // BKI_LOOKUP(pg_proc)
    pub typoutput: regproc,  // BKI_LOOKUP(pg_proc)
    pub typreceive: regproc, // BKI_LOOKUP_OPT(pg_proc)
    pub typsend: regproc,    // BKI_LOOKUP_OPT(pg_proc)
    pub typmodin: regproc,   // BKI_LOOKUP_OPT(pg_proc)
    pub typmodout: regproc,  // BKI_LOOKUP_OPT(pg_proc)
    pub typanalyze: regproc, // BKI_LOOKUP_OPT(pg_proc)
    pub typalign: i8,        // see TYPALIGN_*
    pub typstorage: i8,      // see TYPSTORAGE_*
    pub typnotnull: bool,
    pub typbasetype: Oid, // BKI_LOOKUP_OPT(pg_type)
    pub typtypmod: i32,
    pub typndims: i32,
    pub typcollation: Oid, // BKI_LOOKUP_OPT(pg_collation)
    // CATALOG_VARLEN (not in fixed part):
    pub typdefaultbin: PgNodeTree, // default expr (domains)
    pub typdefault: text,          // human-readable default
    pub typacl: [AclItem; 1],      // aclitem[]
}

pub type Form_pg_type = *mut FormData_pg_type; // TODO(ptr)

// DECLARE_TOAST(pg_type, 4171, 4172)
// DECLARE_UNIQUE_INDEX_PKEY(pg_type_oid_index, 2703, TypeOidIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_type_typname_nsp_index, 2704, TypeNameNspIndexId, ...)
// MAKE_SYSCACHE(TYPEOID, pg_type_oid_index, 64)
// MAKE_SYSCACHE(TYPENAMENSP, pg_type_typname_nsp_index, 64)

/// pg_type_oid_index: unique index on oid (the pkey).
pub const TypeOidIndexId: Oid = Oid::new(2703);
/// pg_type_typname_nsp_index: unique index on (typname, typnamespace).
pub const TypeNameNspIndexId: Oid = Oid::new(2704);

// typtype values.
pub const TYPTYPE_BASE: i8 = b'b' as i8;
pub const TYPTYPE_COMPOSITE: i8 = b'c' as i8;
pub const TYPTYPE_DOMAIN: i8 = b'd' as i8;
pub const TYPTYPE_ENUM: i8 = b'e' as i8;
pub const TYPTYPE_MULTIRANGE: i8 = b'm' as i8;
pub const TYPTYPE_PSEUDO: i8 = b'p' as i8;
pub const TYPTYPE_RANGE: i8 = b'r' as i8;

// typcategory values.
pub const TYPCATEGORY_INVALID: i8 = 0;
pub const TYPCATEGORY_ARRAY: i8 = b'A' as i8;
pub const TYPCATEGORY_BOOLEAN: i8 = b'B' as i8;
pub const TYPCATEGORY_COMPOSITE: i8 = b'C' as i8;
pub const TYPCATEGORY_DATETIME: i8 = b'D' as i8;
pub const TYPCATEGORY_ENUM: i8 = b'E' as i8;
pub const TYPCATEGORY_GEOMETRIC: i8 = b'G' as i8;
pub const TYPCATEGORY_NETWORK: i8 = b'I' as i8;
pub const TYPCATEGORY_NUMERIC: i8 = b'N' as i8;
pub const TYPCATEGORY_PSEUDOTYPE: i8 = b'P' as i8;
pub const TYPCATEGORY_RANGE: i8 = b'R' as i8;
pub const TYPCATEGORY_STRING: i8 = b'S' as i8;
pub const TYPCATEGORY_TIMESPAN: i8 = b'T' as i8;
pub const TYPCATEGORY_USER: i8 = b'U' as i8;
pub const TYPCATEGORY_BITSTRING: i8 = b'V' as i8;
pub const TYPCATEGORY_UNKNOWN: i8 = b'X' as i8;
pub const TYPCATEGORY_INTERNAL: i8 = b'Z' as i8;

// typalign values.
pub const TYPALIGN_CHAR: i8 = b'c' as i8;
pub const TYPALIGN_SHORT: i8 = b's' as i8;
pub const TYPALIGN_INT: i8 = b'i' as i8;
pub const TYPALIGN_DOUBLE: i8 = b'd' as i8;

// typstorage values.
pub const TYPSTORAGE_PLAIN: i8 = b'p' as i8;
pub const TYPSTORAGE_EXTERNAL: i8 = b'e' as i8;
pub const TYPSTORAGE_EXTENDED: i8 = b'x' as i8;
pub const TYPSTORAGE_MAIN: i8 = b'm' as i8;

// Polymorphic-type tests. The ANY*OID type OIDs are generated into
// crate::catalog::genbki by build.rs (from pg_type.dat typnames).
use std::sync::Arc;

use crate::catalog::genbki::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYRANGEOID, MONEYOID, PG_LSNOID,
};
use crate::utils::rel::RelationData;

pub fn IsPolymorphicTypeFamily1(typid: Oid) -> bool {
    matches!(
        typid,
        x if x == ANYELEMENTOID
            || x == ANYARRAYOID
            || x == ANYNONARRAYOID
            || x == ANYENUMOID
            || x == ANYRANGEOID
            || x == ANYMULTIRANGEOID
    )
}

pub fn IsPolymorphicTypeFamily2(typid: Oid) -> bool {
    matches!(
        typid,
        x if x == ANYCOMPATIBLEOID
            || x == ANYCOMPATIBLEARRAYOID
            || x == ANYCOMPATIBLENONARRAYOID
            || x == ANYCOMPATIBLERANGEOID
            || x == ANYCOMPATIBLEMULTIRANGEOID
    )
}

pub fn IsPolymorphicType(typid: Oid) -> bool {
    IsPolymorphicTypeFamily1(typid) || IsPolymorphicTypeFamily2(typid)
}

// TODO: implement via typsubscript == F_ARRAY_SUBSCRIPT_HANDLER && typelem != 0.
pub fn IsTrueArrayType(_type_form: &FormData_pg_type) -> bool {
    unimplemented!()
}

// Backwards-compat spellings of the generated type OIDs.
pub const CASHOID: Oid = MONEYOID;
pub const LSNOID: Oid = PG_LSNOID;

pub fn TypeShellMake(_type_name: &str, _type_namespace: Oid, _owner_id: Oid) -> ObjectAddress {
    unimplemented!()
}

pub use crate::backend::catalog::pg_type::type_create as TypeCreate;

pub fn GenerateTypeDependencies(
    _type_tuple: HeapTuple,
    _type_catalog: &Arc<RelationData>,
    _default_expr: &Node,
    _typacl: &[u8], // void* acl -> opaque bytes; TODO(ptr)
    _relation_kind: i8,
    _is_implicit_array: bool,
    _is_dependent_type: bool,
    _make_extension_dep: bool,
    _rebuild: bool,
) {
    unimplemented!()
}

pub fn RenameTypeInternal(_type_oid: Oid, _new_type_name: &str, _type_namespace: Oid) {
    unimplemented!()
}

pub fn makeArrayTypeName(_type_name: &str, _type_namespace: Oid) -> String {
    unimplemented!()
}

pub fn moveArrayTypeName(_type_oid: Oid, _type_name: &str, _type_namespace: Oid) -> bool {
    unimplemented!()
}

pub fn makeMultirangeTypeName(_range_type_name: &str, _type_namespace: Oid) -> String {
    unimplemented!()
}

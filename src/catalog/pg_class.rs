//! Translated from PostgreSQL src/include/catalog/pg_class.h

use crate::c::{float4, text, NameData, TransactionId};
use crate::postgres_ext::Oid;
use crate::utils::acl::AclItem;

// BKI_BOOTSTRAP BKI_ROWTYPE_OID(83,RelationRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const RelationRelationId: Oid = Oid(1259);
pub const RelationRelation_Rowtype_Id: Oid = Oid(83);

// pg_node_tree catalog field = varlena (serialized node tree); modeled as text for now.
pub type PgNodeTree = text;

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_class {
    pub oid: Oid,
    pub relname: NameData,
    pub relnamespace: Oid, // BKI_LOOKUP(pg_namespace)
    pub reltype: Oid,      // BKI_LOOKUP_OPT(pg_type)
    pub reloftype: Oid,    // BKI_LOOKUP_OPT(pg_type)
    pub relowner: Oid,     // BKI_LOOKUP(pg_authid)
    pub relam: Oid,        // BKI_LOOKUP_OPT(pg_am)
    pub relfilenode: Oid,
    pub reltablespace: Oid, // BKI_LOOKUP_OPT(pg_tablespace)
    pub relpages: i32,
    pub reltuples: float4,
    pub relallvisible: i32,
    pub relallfrozen: i32,
    pub reltoastrelid: Oid, // BKI_LOOKUP_OPT(pg_class)
    pub relhasindex: bool,
    pub relisshared: bool,
    pub relpersistence: i8,
    pub relkind: i8,
    pub relnatts: i16,
    pub relchecks: i16,
    pub relhasrules: bool,
    pub relhastriggers: bool,
    pub relhassubclass: bool,
    pub relrowsecurity: bool,
    pub relforcerowsecurity: bool,
    pub relispopulated: bool,
    pub relreplident: i8,
    pub relispartition: bool,
    pub relrewrite: Oid, // BKI_LOOKUP_OPT(pg_class)
    pub relfrozenxid: TransactionId,
    pub relminmxid: TransactionId, // really a MultiXactId
    // CATALOG_VARLEN (not in fixed part) -- variable-length fields:
    pub relacl: [AclItem; 1], // aclitem[1]
    pub reloptions: [text; 1],
    pub relpartbound: PgNodeTree,
}

// CLASS_TUPLE_SIZE = offsetof(relminmxid) + sizeof(TransactionId)
pub const CLASS_TUPLE_SIZE: usize =
    core::mem::offset_of!(FormData_pg_class, relminmxid) + core::mem::size_of::<TransactionId>();

pub type Form_pg_class = *mut FormData_pg_class; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_class_oid_index, 2662, ClassOidIndexId)
// DECLARE_UNIQUE_INDEX(pg_class_relname_nsp_index, 2663, ClassNameNspIndexId)
// DECLARE_INDEX(pg_class_tblspc_relfilenode_index, 3455, ClassTblspcRelfilenodeIndexId)
// MAKE_SYSCACHE(RELOID, pg_class_oid_index, 128)
// MAKE_SYSCACHE(RELNAMENSP, pg_class_relname_nsp_index, 128)

pub const RELKIND_RELATION: i8 = b'r' as i8;
pub const RELKIND_INDEX: i8 = b'i' as i8;
pub const RELKIND_SEQUENCE: i8 = b'S' as i8;
pub const RELKIND_TOASTVALUE: i8 = b't' as i8;
pub const RELKIND_VIEW: i8 = b'v' as i8;
pub const RELKIND_MATVIEW: i8 = b'm' as i8;
pub const RELKIND_COMPOSITE_TYPE: i8 = b'c' as i8;
pub const RELKIND_FOREIGN_TABLE: i8 = b'f' as i8;
pub const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;
pub const RELKIND_PARTITIONED_INDEX: i8 = b'I' as i8;

pub const RELPERSISTENCE_PERMANENT: i8 = b'p' as i8;
pub const RELPERSISTENCE_UNLOGGED: i8 = b'u' as i8;
pub const RELPERSISTENCE_TEMP: i8 = b't' as i8;

pub const REPLICA_IDENTITY_DEFAULT: i8 = b'd' as i8;
pub const REPLICA_IDENTITY_NOTHING: i8 = b'n' as i8;
pub const REPLICA_IDENTITY_FULL: i8 = b'f' as i8;
pub const REPLICA_IDENTITY_INDEX: i8 = b'i' as i8;

pub const fn RELKIND_HAS_STORAGE(relkind: i8) -> bool {
    relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW
}

pub const fn RELKIND_HAS_PARTITIONS(relkind: i8) -> bool {
    relkind == RELKIND_PARTITIONED_TABLE || relkind == RELKIND_PARTITIONED_INDEX
}

pub const fn RELKIND_HAS_TABLESPACE(relkind: i8) -> bool {
    (RELKIND_HAS_STORAGE(relkind) || RELKIND_HAS_PARTITIONS(relkind)) && relkind != RELKIND_SEQUENCE
}

pub const fn RELKIND_HAS_TABLE_AM(relkind: i8) -> bool {
    relkind == RELKIND_RELATION || relkind == RELKIND_TOASTVALUE || relkind == RELKIND_MATVIEW
}

pub fn errdetail_relkind_not_supported(_relkind: i8) -> i32 {
    unimplemented!()
}

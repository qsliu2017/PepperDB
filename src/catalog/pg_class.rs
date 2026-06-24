//! Translated from PostgreSQL src/include/catalog/pg_class.h

use crate::c::{float4, text, NameData, TransactionId};
use crate::postgres_ext::Oid;

// BKI_BOOTSTRAP BKI_ROWTYPE_OID(83,RelationRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const RelationRelationId: Oid = Oid(1259);
pub const RelationRelation_Rowtype_Id: Oid = Oid(83);

// pg_node_tree catalog field = varlena (serialized node tree); modeled as text for now.
pub type PgNodeTree = text; // TODO(struct-forward)

#[repr(C)]
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
    pub relacl: [Aclitem; 1], // aclitem[1]; TODO(struct-forward)
    pub reloptions: [text; 1],
    pub relpartbound: PgNodeTree,
}

// aclitem placeholder; real def lives in utils/acl.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::acl::AclItem in Phase 2")]
#[repr(C)]
pub struct Aclitem {
    pub ai_grantee: Oid,
    pub ai_grantor: Oid,
    pub ai_privs: u64,
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

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_class_oid: i32 = 1;
pub const Anum_pg_class_relname: i32 = 2;
pub const Anum_pg_class_relnamespace: i32 = 3;
pub const Anum_pg_class_reltype: i32 = 4;
pub const Anum_pg_class_reloftype: i32 = 5;
pub const Anum_pg_class_relowner: i32 = 6;
pub const Anum_pg_class_relam: i32 = 7;
pub const Anum_pg_class_relfilenode: i32 = 8;
pub const Anum_pg_class_reltablespace: i32 = 9;
pub const Anum_pg_class_relpages: i32 = 10;
pub const Anum_pg_class_reltuples: i32 = 11;
pub const Anum_pg_class_relallvisible: i32 = 12;
pub const Anum_pg_class_relallfrozen: i32 = 13;
pub const Anum_pg_class_reltoastrelid: i32 = 14;
pub const Anum_pg_class_relhasindex: i32 = 15;
pub const Anum_pg_class_relisshared: i32 = 16;
pub const Anum_pg_class_relpersistence: i32 = 17;
pub const Anum_pg_class_relkind: i32 = 18;
pub const Anum_pg_class_relnatts: i32 = 19;
pub const Anum_pg_class_relchecks: i32 = 20;
pub const Anum_pg_class_relhasrules: i32 = 21;
pub const Anum_pg_class_relhastriggers: i32 = 22;
pub const Anum_pg_class_relhassubclass: i32 = 23;
pub const Anum_pg_class_relrowsecurity: i32 = 24;
pub const Anum_pg_class_relforcerowsecurity: i32 = 25;
pub const Anum_pg_class_relispopulated: i32 = 26;
pub const Anum_pg_class_relreplident: i32 = 27;
pub const Anum_pg_class_relispartition: i32 = 28;
pub const Anum_pg_class_relrewrite: i32 = 29;
pub const Anum_pg_class_relfrozenxid: i32 = 30;
pub const Anum_pg_class_relminmxid: i32 = 31;
pub const Anum_pg_class_relacl: i32 = 32;
pub const Anum_pg_class_reloptions: i32 = 33;
pub const Anum_pg_class_relpartbound: i32 = 34;
pub const Natts_pg_class: i32 = 34;

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

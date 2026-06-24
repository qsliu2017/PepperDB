//! Translated from PostgreSQL src/include/catalog/pg_attribute.h

use crate::c::{text, NameData};
use crate::postgres::NullableDatum;
use crate::postgres_ext::Oid;

// BKI_BOOTSTRAP BKI_ROWTYPE_OID(75,AttributeRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const AttributeRelationId: Oid = Oid(1249);
pub const AttributeRelation_Rowtype_Id: Oid = Oid(75);

#[repr(C)]
pub struct FormData_pg_attribute {
    pub attrelid: Oid, // BKI_LOOKUP(pg_class)
    pub attname: NameData,
    pub atttypid: Oid, // BKI_LOOKUP_OPT(pg_type)
    pub attlen: i16,
    pub attnum: i16,
    pub atttypmod: i32,
    pub attndims: i16,
    pub attbyval: bool,
    pub attalign: i8,
    pub attstorage: i8,
    pub attcompression: i8,
    pub attnotnull: bool,
    pub atthasdef: bool,
    pub atthasmissing: bool,
    pub attidentity: i8,
    pub attgenerated: i8,
    pub attisdropped: bool,
    pub attislocal: bool,
    pub attinhcount: i16,
    pub attcollation: Oid, // BKI_LOOKUP_OPT(pg_collation)
    // CATALOG_VARLEN (not in fixed part) -- variable-length/nullable fields:
    pub attstattarget: i16, // BKI_FORCE_NULL
    pub attacl: [Aclitem; 1], // aclitem[1]; TODO(struct-forward): repoint to pg_type aclitem
    pub attoptions: [text; 1],
    pub attfdwoptions: [text; 1],
    pub attmissingval: Anyarray, // anyarray; TODO(struct-forward)
}

// aclitem placeholder (catalog ACL element); real def lives in utils/acl.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::acl::AclItem in Phase 2")]
#[repr(C)]
pub struct Aclitem {
    pub ai_grantee: Oid,
    pub ai_grantor: Oid,
    pub ai_privs: u64,
}

// anyarray pseudo-type tail = varlena array; modeled as text (varlena) for now.
pub type Anyarray = text; // TODO(struct-forward)

// ATTRIBUTE_FIXED_PART_SIZE = offsetof(attcollation) + sizeof(Oid)
pub const ATTRIBUTE_FIXED_PART_SIZE: usize =
    core::mem::offset_of!(FormData_pg_attribute, attcollation) + core::mem::size_of::<Oid>();

pub type Form_pg_attribute = *mut FormData_pg_attribute; // TODO(ptr)

// Fields excluded by CATALOG_VARLEN, for DDL use alongside FormData_pg_attribute.
pub struct FormExtraData_pg_attribute {
    pub attstattarget: NullableDatum,
    pub attoptions: NullableDatum,
}

// DECLARE_UNIQUE_INDEX(pg_attribute_relid_attnam_index, 2658, AttributeRelidNameIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_attribute_relid_attnum_index, 2659, AttributeRelidNumIndexId)
// MAKE_SYSCACHE(ATTNAME, pg_attribute_relid_attnam_index, 32)
// MAKE_SYSCACHE(ATTNUM, pg_attribute_relid_attnum_index, 128)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_attribute_attrelid: i32 = 1;
pub const Anum_pg_attribute_attname: i32 = 2;
pub const Anum_pg_attribute_atttypid: i32 = 3;
pub const Anum_pg_attribute_attlen: i32 = 4;
pub const Anum_pg_attribute_attnum: i32 = 5;
pub const Anum_pg_attribute_atttypmod: i32 = 6;
pub const Anum_pg_attribute_attndims: i32 = 7;
pub const Anum_pg_attribute_attbyval: i32 = 8;
pub const Anum_pg_attribute_attalign: i32 = 9;
pub const Anum_pg_attribute_attstorage: i32 = 10;
pub const Anum_pg_attribute_attcompression: i32 = 11;
pub const Anum_pg_attribute_attnotnull: i32 = 12;
pub const Anum_pg_attribute_atthasdef: i32 = 13;
pub const Anum_pg_attribute_atthasmissing: i32 = 14;
pub const Anum_pg_attribute_attidentity: i32 = 15;
pub const Anum_pg_attribute_attgenerated: i32 = 16;
pub const Anum_pg_attribute_attisdropped: i32 = 17;
pub const Anum_pg_attribute_attislocal: i32 = 18;
pub const Anum_pg_attribute_attinhcount: i32 = 19;
pub const Anum_pg_attribute_attcollation: i32 = 20;
pub const Anum_pg_attribute_attstattarget: i32 = 21;
pub const Anum_pg_attribute_attacl: i32 = 22;
pub const Anum_pg_attribute_attoptions: i32 = 23;
pub const Anum_pg_attribute_attfdwoptions: i32 = 24;
pub const Anum_pg_attribute_attmissingval: i32 = 25;
pub const Natts_pg_attribute: i32 = 25;

pub const ATTRIBUTE_IDENTITY_ALWAYS: i8 = b'a' as i8;
pub const ATTRIBUTE_IDENTITY_BY_DEFAULT: i8 = b'd' as i8;
pub const ATTRIBUTE_GENERATED_STORED: i8 = b's' as i8;
pub const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

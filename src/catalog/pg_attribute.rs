//! Translated from PostgreSQL src/include/catalog/pg_attribute.h

use crate::c::{text, varlena, NameData, NAMEDATALEN};
use crate::nodes::parsenodes::AclMode;
use crate::postgres::NullableDatum;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::acl::AclItem;

// BKI_BOOTSTRAP BKI_ROWTYPE_OID(75,AttributeRelation_Rowtype_Id) BKI_SCHEMA_MACRO
pub const AttributeRelationId: Oid = Oid(1249);
pub const AttributeRelation_Rowtype_Id: Oid = Oid(75);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
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
    pub attacl: [AclItem; 1], // aclitem[1]
    pub attoptions: [text; 1],
    pub attfdwoptions: [text; 1],
    pub attmissingval: Anyarray, // anyarray
}

// anyarray pseudo-type tail = varlena array; modeled as text (varlena) for now.
pub type Anyarray = text;

// ATTRIBUTE_FIXED_PART_SIZE = offsetof(attcollation) + sizeof(Oid)
pub const ATTRIBUTE_FIXED_PART_SIZE: usize =
    core::mem::offset_of!(FormData_pg_attribute, attcollation) + core::mem::size_of::<Oid>();

impl FormData_pg_attribute {
    /// An empty attribute row. The `CATALOG_VARLEN` tail fields (attacl/
    /// attoptions/attfdwoptions/attmissingval) are never present in an in-memory
    /// tupdesc; only the fixed part is ever touched. They are constructed as
    /// empty/zero placeholders so the `#[repr(C)]` struct is well-formed.
    #[must_use]
    pub fn new() -> Self {
        let empty_varlena = || varlena { vl_len_: [0u8; 4], dat: [] };
        Self {
            attrelid: InvalidOid,
            attname: NameData { data: [0u8; NAMEDATALEN] },
            atttypid: InvalidOid,
            attlen: 0,
            attnum: 0,
            atttypmod: -1,
            attndims: 0,
            attbyval: false,
            attalign: 0,
            attstorage: 0,
            attcompression: 0,
            attnotnull: false,
            atthasdef: false,
            atthasmissing: false,
            attidentity: 0,
            attgenerated: 0,
            attisdropped: false,
            attislocal: false,
            attinhcount: 0,
            attcollation: InvalidOid,
            attstattarget: 0,
            attacl: [AclItem {
                grantee: InvalidOid,
                grantor: InvalidOid,
                privs: AclMode::from_bits_retain(0),
            }],
            attoptions: [empty_varlena()],
            attfdwoptions: [empty_varlena()],
            attmissingval: empty_varlena(),
        }
    }
}

impl Default for FormData_pg_attribute {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for FormData_pg_attribute {
    /// Copy the fixed part (the only part an in-memory tupdesc uses) byte-for-byte
    /// as C `memcpy` does; the variable-length tail is reconstructed empty.
    fn clone(&self) -> Self {
        let mut out = Self::new();
        // SAFETY: both are valid `FormData_pg_attribute`; the first
        // `ATTRIBUTE_FIXED_PART_SIZE` bytes are the fixed part. `#[repr(C)]` makes
        // the layout stable, and a plain-old-data attribute row has no Drop, so a
        // byte copy of the fixed part is sound (mirrors the C memcpy).
        unsafe {
            core::ptr::copy_nonoverlapping(
                core::ptr::from_ref::<Self>(self).cast::<u8>(),
                core::ptr::from_mut::<Self>(&mut out).cast::<u8>(),
                ATTRIBUTE_FIXED_PART_SIZE,
            );
        }
        out
    }
}

// The C `Form_pg_attribute` (`FormData_pg_attribute *`) is gone: attributes live
// INSIDE a `TupleDescData` (its `attrs: Vec<FormData_pg_attribute>`), so an
// attribute handle is a borrow into the owning descriptor -- `TupleDescAttr`/
// `TupleDescData::attr` return `&FormData_pg_attribute`. Stub/param signatures
// that took the old pointer now take `&`/`&mut FormData_pg_attribute`.

// Fields excluded by CATALOG_VARLEN, for DDL use alongside FormData_pg_attribute.
pub struct FormExtraData_pg_attribute {
    pub attstattarget: NullableDatum,
    pub attoptions: NullableDatum,
}

// DECLARE_UNIQUE_INDEX(pg_attribute_relid_attnam_index, 2658, AttributeRelidNameIndexId)
// DECLARE_UNIQUE_INDEX_PKEY(pg_attribute_relid_attnum_index, 2659, AttributeRelidNumIndexId)
// MAKE_SYSCACHE(ATTNAME, pg_attribute_relid_attnam_index, 32)
// MAKE_SYSCACHE(ATTNUM, pg_attribute_relid_attnum_index, 128)

pub const ATTRIBUTE_IDENTITY_ALWAYS: i8 = b'a' as i8;
pub const ATTRIBUTE_IDENTITY_BY_DEFAULT: i8 = b'd' as i8;
pub const ATTRIBUTE_GENERATED_STORED: i8 = b's' as i8;
pub const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

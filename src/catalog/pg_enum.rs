//! Translated from PostgreSQL src/include/catalog/pg_enum.h

use crate::c::{float4, NameData};
use crate::postgres_ext::Oid;

pub const EnumRelationId: Oid = Oid(3501);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_enum {
    pub oid: Oid,
    pub enumtypid: Oid, // BKI_LOOKUP(pg_type)
    pub enumsortorder: float4,
    pub enumlabel: NameData,
}

pub type Form_pg_enum = *mut FormData_pg_enum; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_enum_oid_index, 3502, EnumOidIndexId)
// DECLARE_UNIQUE_INDEX(pg_enum_typid_label_index, 3503, EnumTypIdLabelIndexId)
// DECLARE_UNIQUE_INDEX(pg_enum_typid_sortorder_index, 3534, EnumTypIdSortOrderIndexId)
// MAKE_SYSCACHE(ENUMOID, pg_enum_oid_index, 8)
// MAKE_SYSCACHE(ENUMTYPOIDNAME, pg_enum_typid_label_index, 8)

// prototypes for functions in pg_enum.c

pub fn EnumValuesCreate(_enum_type_oid: Oid, _vals: &[Oid]) {
    unimplemented!()
}

pub fn EnumValuesDelete(_enum_type_oid: Oid) {
    unimplemented!()
}

pub fn AddEnumLabel(
    _enum_type_oid: Oid,
    _new_val: &str,
    _neighbor: Option<&str>,
    _new_val_is_after: bool,
    _skip_if_exists: bool,
) {
    unimplemented!()
}

pub fn RenameEnumLabel(_enum_type_oid: Oid, _old_val: &str, _new_val: &str) {
    unimplemented!()
}

pub fn EnumUncommitted(_enum_id: Oid) -> bool {
    unimplemented!()
}

pub fn EstimateUncommittedEnumsSpace() -> usize {
    unimplemented!()
}

pub fn SerializeUncommittedEnums(_space: &mut [u8], _size: usize) {
    unimplemented!()
}

pub fn RestoreUncommittedEnums(_space: &[u8]) {
    unimplemented!()
}

pub fn AtEOXact_Enum() {
    unimplemented!()
}

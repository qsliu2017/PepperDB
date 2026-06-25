//! Translated from PostgreSQL src/include/catalog/pg_db_role_setting.h

use crate::c::text;
use crate::nodes::parsenodes::VariableSetStmt;
use crate::postgres_ext::Oid;
use crate::utils::guc::GucSource;
use crate::utils::rel::Relation;
use crate::utils::snapshot::Snapshot;

pub const DbRoleSettingRelationId: Oid = Oid(2964);

// CATALOG(pg_db_role_setting,2964,DbRoleSettingRelationId) BKI_SHARED_RELATION
#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_db_role_setting {
    pub setdatabase: Oid, // BKI_LOOKUP_OPT(pg_database); database, or 0 for role-specific
    pub setrole: Oid,     // BKI_LOOKUP_OPT(pg_authid); role, or 0 for database-specific
    // CATALOG_VARLEN (not in fixed part):
    pub setconfig: text, // GUC settings to apply at login (text[])
}

pub type Form_pg_db_role_setting = *mut FormData_pg_db_role_setting; // TODO(ptr)

// DECLARE_TOAST_WITH_MACRO(pg_db_role_setting, 2966, 2967, PgDbRoleSettingToastTable, PgDbRoleSettingToastIndex)
// DECLARE_UNIQUE_INDEX_PKEY(pg_db_role_setting_databaseid_rol_index, 2965, DbRoleSettingDatidRolidIndexId, ...)

pub fn AlterSetting(_databaseid: Oid, _roleid: Oid, _setstmt: &VariableSetStmt) {
    unimplemented!()
}

pub fn DropSetting(_databaseid: Oid, _roleid: Oid) {
    unimplemented!()
}

pub fn ApplySetting(
    _snapshot: &Snapshot,
    _databaseid: Oid,
    _roleid: Oid,
    _relsetting: &Relation,
    _source: GucSource,
) {
    unimplemented!()
}

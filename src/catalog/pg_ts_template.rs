//! Translated from PostgreSQL src/include/catalog/pg_ts_template.h

use crate::c::{NameData, regproc};
use crate::postgres_ext::Oid;

pub const TSTemplateRelationId: Oid = Oid(3764);

#[repr(C)]
pub struct FormData_pg_ts_template {
    pub oid: Oid,
    pub tmplname: NameData,
    pub tmplnamespace: Oid,
    pub tmplinit: regproc,
    pub tmpllexize: regproc,
}

pub type Form_pg_ts_template = *mut FormData_pg_ts_template; // TODO(ptr)

// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]
pub const Anum_pg_ts_template_oid: i32 = 1;
pub const Anum_pg_ts_template_tmplname: i32 = 2;
pub const Anum_pg_ts_template_tmplnamespace: i32 = 3;
pub const Anum_pg_ts_template_tmplinit: i32 = 4;
pub const Anum_pg_ts_template_tmpllexize: i32 = 5;
pub const Natts_pg_ts_template: i32 = 5;

// DECLARE_UNIQUE_INDEX(pg_ts_template_tmplname_index, 3766, TSTemplateNameNspIndexId, ...)
// DECLARE_UNIQUE_INDEX_PKEY(pg_ts_template_oid_index, 3767, TSTemplateOidIndexId, ...)
// MAKE_SYSCACHE(TSTEMPLATENAMENSP, ...); MAKE_SYSCACHE(TSTEMPLATEOID, ...)

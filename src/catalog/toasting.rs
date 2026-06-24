//! Translated from PostgreSQL src/include/catalog/toasting.h
//!
//! Definitions to support creation of toast tables. (The `DECLARE_TOAST` /
//! `DECLARE_TOAST_WITH_MACRO` BKI macros live in the individual `pg_*.h` catalog
//! headers, not here; this header only declares toasting.c's prototypes.)

use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::lock::LOCKMODE;

pub fn NewRelationCreateToastTable(_rel_oid: Oid, _reloptions: Datum) {
    unimplemented!()
}

pub fn NewHeapCreateToastTable(
    _rel_oid: Oid,
    _reloptions: Datum,
    _lockmode: LOCKMODE,
    _oid_old_toast: Oid,
) {
    unimplemented!()
}

pub fn AlterTableCreateToastTable(_rel_oid: Oid, _reloptions: Datum, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn BootstrapToastTable(_rel_name: &str, _toast_oid: Oid, _toast_index_oid: Oid) {
    unimplemented!()
}

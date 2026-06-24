//! Translated from PostgreSQL src/include/catalog/binary_upgrade.h
//
// Process-global GUC-ish variables used during pg_upgrade. PGDLLIMPORT externs
// -> static mut for now; the single-process port will move these into a Session
// / upgrade context later.

use crate::common::relpath::RelFileNumber;
use crate::postgres_ext::{InvalidOid, Oid};

// TODO(global): migrate these process-globals into a threaded upgrade context.
pub static mut binary_upgrade_next_pg_tablespace_oid: Oid = InvalidOid;

pub static mut binary_upgrade_next_pg_type_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_array_pg_type_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_mrng_pg_type_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_mrng_array_pg_type_oid: Oid = InvalidOid;

pub static mut binary_upgrade_next_heap_pg_class_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_heap_pg_class_relfilenumber: RelFileNumber = InvalidOid;
pub static mut binary_upgrade_next_index_pg_class_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_index_pg_class_relfilenumber: RelFileNumber = InvalidOid;
pub static mut binary_upgrade_next_toast_pg_class_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_toast_pg_class_relfilenumber: RelFileNumber = InvalidOid;

pub static mut binary_upgrade_next_pg_enum_oid: Oid = InvalidOid;
pub static mut binary_upgrade_next_pg_authid_oid: Oid = InvalidOid;

pub static mut binary_upgrade_record_init_privs: bool = false;

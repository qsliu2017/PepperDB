//! binary_upgrade.h - variables used for binary upgrades

use crate::common::relpath::RelFileNumber;
use crate::postgres_ext::Oid;

// In C these are PGDLLIMPORT extern globals (defined in backend/utils/misc).
// Translate each `extern Oid foo;` / `extern bool foo;` to a mutable global.

pub static mut binary_upgrade_next_pg_tablespace_oid: Oid = 0;

pub static mut binary_upgrade_next_pg_type_oid: Oid = 0;
pub static mut binary_upgrade_next_array_pg_type_oid: Oid = 0;
pub static mut binary_upgrade_next_mrng_pg_type_oid: Oid = 0;
pub static mut binary_upgrade_next_mrng_array_pg_type_oid: Oid = 0;

pub static mut binary_upgrade_next_heap_pg_class_oid: Oid = 0;
pub static mut binary_upgrade_next_heap_pg_class_relfilenumber: RelFileNumber = 0;
pub static mut binary_upgrade_next_index_pg_class_oid: Oid = 0;
pub static mut binary_upgrade_next_index_pg_class_relfilenumber: RelFileNumber = 0;
pub static mut binary_upgrade_next_toast_pg_class_oid: Oid = 0;
pub static mut binary_upgrade_next_toast_pg_class_relfilenumber: RelFileNumber = 0;

pub static mut binary_upgrade_next_pg_enum_oid: Oid = 0;
pub static mut binary_upgrade_next_pg_authid_oid: Oid = 0;

pub static mut binary_upgrade_record_init_privs: bool = false;

//! catalog/catversion.h - "Catalog version number" for PostgreSQL.
//!
//! The catalog version number is used to flag incompatible changes in the
//! PostgreSQL system catalogs. The version number stored in pg_control by
//! initdb is checked against the version number compiled into the backend at
//! startup time, so that a backend can refuse to run in an incompatible
//! database.

/*
 * We could use anything we wanted for version numbers, but the "YYYYMMDDN"
 * style often used for DNS zone serial numbers is recommended. YYYYMMDD are
 * the date of the change, and N is the number of the change on that day.
 */

/* yyyymmddN */
pub const CATALOG_VERSION_NO: i32 = 202506291;

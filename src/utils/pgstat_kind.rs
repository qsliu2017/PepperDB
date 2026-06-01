//! pgstat_kind.h - statistics kinds for the cumulative statistics system.

use crate::c::uint32;

/// The types of statistics entries.
pub type PgStat_Kind = uint32;

/// Range of IDs allowed, for built-in and custom kinds.
/// Minimum ID allowed.
pub const PGSTAT_KIND_MIN: PgStat_Kind = 1;
/// Maximum ID allowed.
pub const PGSTAT_KIND_MAX: PgStat_Kind = 32;

/// use 0 for INVALID, to catch zero-initialized data.
pub const PGSTAT_KIND_INVALID: PgStat_Kind = 0;

/* stats for variable-numbered objects */
/// database-wide statistics
pub const PGSTAT_KIND_DATABASE: PgStat_Kind = 1;
/// per-table statistics
pub const PGSTAT_KIND_RELATION: PgStat_Kind = 2;
/// per-function statistics
pub const PGSTAT_KIND_FUNCTION: PgStat_Kind = 3;
/// per-slot statistics
pub const PGSTAT_KIND_REPLSLOT: PgStat_Kind = 4;
/// per-subscription statistics
pub const PGSTAT_KIND_SUBSCRIPTION: PgStat_Kind = 5;
/// per-backend statistics
pub const PGSTAT_KIND_BACKEND: PgStat_Kind = 6;

/* stats for fixed-numbered objects */
pub const PGSTAT_KIND_ARCHIVER: PgStat_Kind = 7;
pub const PGSTAT_KIND_BGWRITER: PgStat_Kind = 8;
pub const PGSTAT_KIND_CHECKPOINTER: PgStat_Kind = 9;
pub const PGSTAT_KIND_IO: PgStat_Kind = 10;
pub const PGSTAT_KIND_SLRU: PgStat_Kind = 11;
pub const PGSTAT_KIND_WAL: PgStat_Kind = 12;

pub const PGSTAT_KIND_BUILTIN_MIN: PgStat_Kind = PGSTAT_KIND_DATABASE;
pub const PGSTAT_KIND_BUILTIN_MAX: PgStat_Kind = PGSTAT_KIND_WAL;
pub const PGSTAT_KIND_BUILTIN_SIZE: PgStat_Kind = PGSTAT_KIND_BUILTIN_MAX + 1;

/* Custom stats kinds */

/// Range of IDs allowed for custom stats kinds.
pub const PGSTAT_KIND_CUSTOM_MIN: PgStat_Kind = 24;
pub const PGSTAT_KIND_CUSTOM_MAX: PgStat_Kind = PGSTAT_KIND_MAX;
pub const PGSTAT_KIND_CUSTOM_SIZE: PgStat_Kind =
    PGSTAT_KIND_CUSTOM_MAX - PGSTAT_KIND_CUSTOM_MIN + 1;

/// PgStat_Kind to use for extensions that require an ID, but are still in
/// development and have not reserved their own unique kind ID yet. See:
/// https://wiki.postgresql.org/wiki/CustomCumulativeStats
pub const PGSTAT_KIND_EXPERIMENTAL: PgStat_Kind = 24;

#[inline]
pub fn pgstat_is_kind_builtin(kind: PgStat_Kind) -> bool {
    kind >= PGSTAT_KIND_BUILTIN_MIN && kind <= PGSTAT_KIND_BUILTIN_MAX
}

#[inline]
pub fn pgstat_is_kind_custom(kind: PgStat_Kind) -> bool {
    kind >= PGSTAT_KIND_CUSTOM_MIN && kind <= PGSTAT_KIND_CUSTOM_MAX
}

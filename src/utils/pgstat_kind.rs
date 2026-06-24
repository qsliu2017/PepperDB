//! Translated from PostgreSQL src/include/utils/pgstat_kind.h
//
// Statistics-kind IDs for the cumulative statistics system. These are sequential
// ordinals (not flags), so they stay plain consts over a u32 alias; the two inline
// range checks are translated in full.

pub type PgStat_Kind = u32;

pub const PGSTAT_KIND_MIN: PgStat_Kind = 1;
pub const PGSTAT_KIND_MAX: PgStat_Kind = 32;

pub const PGSTAT_KIND_INVALID: PgStat_Kind = 0;

// Variable-numbered objects.
pub const PGSTAT_KIND_DATABASE: PgStat_Kind = 1;
pub const PGSTAT_KIND_RELATION: PgStat_Kind = 2;
pub const PGSTAT_KIND_FUNCTION: PgStat_Kind = 3;
pub const PGSTAT_KIND_REPLSLOT: PgStat_Kind = 4;
pub const PGSTAT_KIND_SUBSCRIPTION: PgStat_Kind = 5;
pub const PGSTAT_KIND_BACKEND: PgStat_Kind = 6;

// Fixed-numbered objects.
pub const PGSTAT_KIND_ARCHIVER: PgStat_Kind = 7;
pub const PGSTAT_KIND_BGWRITER: PgStat_Kind = 8;
pub const PGSTAT_KIND_CHECKPOINTER: PgStat_Kind = 9;
pub const PGSTAT_KIND_IO: PgStat_Kind = 10;
pub const PGSTAT_KIND_SLRU: PgStat_Kind = 11;
pub const PGSTAT_KIND_WAL: PgStat_Kind = 12;

pub const PGSTAT_KIND_BUILTIN_MIN: PgStat_Kind = PGSTAT_KIND_DATABASE;
pub const PGSTAT_KIND_BUILTIN_MAX: PgStat_Kind = PGSTAT_KIND_WAL;
pub const PGSTAT_KIND_BUILTIN_SIZE: PgStat_Kind = PGSTAT_KIND_BUILTIN_MAX + 1;

// Custom stats kinds.
pub const PGSTAT_KIND_CUSTOM_MIN: PgStat_Kind = 24;
pub const PGSTAT_KIND_CUSTOM_MAX: PgStat_Kind = PGSTAT_KIND_MAX;
pub const PGSTAT_KIND_CUSTOM_SIZE: PgStat_Kind =
    PGSTAT_KIND_CUSTOM_MAX - PGSTAT_KIND_CUSTOM_MIN + 1;

pub const PGSTAT_KIND_EXPERIMENTAL: PgStat_Kind = 24;

pub const fn pgstat_is_kind_builtin(kind: PgStat_Kind) -> bool {
    kind >= PGSTAT_KIND_BUILTIN_MIN && kind <= PGSTAT_KIND_BUILTIN_MAX
}

pub const fn pgstat_is_kind_custom(kind: PgStat_Kind) -> bool {
    kind >= PGSTAT_KIND_CUSTOM_MIN && kind <= PGSTAT_KIND_CUSTOM_MAX
}

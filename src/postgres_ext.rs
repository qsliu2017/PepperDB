//! Translated from PostgreSQL src/include/postgres_ext.h

/// Object ID is a fundamental type in Postgres (C: `typedef unsigned int Oid`).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Oid(pub u32);

pub const InvalidOid: Oid = Oid(0);
pub const OID_MAX: Oid = Oid(u32::MAX);

/// C: `#define atooid(x) ((Oid) strtoul((x), NULL, 10))`
pub fn atooid(x: &str) -> Oid {
    Oid(x.trim().parse().unwrap_or(0))
}

/// Deprecated name for int64_t, formerly used in client API declarations.
pub type pg_int64 = i64;

// Identifiers of error message fields (PG_DIAG_*).
pub const PG_DIAG_SEVERITY: u8 = b'S';
pub const PG_DIAG_SEVERITY_NONLOCALIZED: u8 = b'V';
pub const PG_DIAG_SQLSTATE: u8 = b'C';
pub const PG_DIAG_MESSAGE_PRIMARY: u8 = b'M';
pub const PG_DIAG_MESSAGE_DETAIL: u8 = b'D';
pub const PG_DIAG_MESSAGE_HINT: u8 = b'H';
pub const PG_DIAG_STATEMENT_POSITION: u8 = b'P';
pub const PG_DIAG_INTERNAL_POSITION: u8 = b'p';
pub const PG_DIAG_INTERNAL_QUERY: u8 = b'q';
pub const PG_DIAG_CONTEXT: u8 = b'W';
pub const PG_DIAG_SCHEMA_NAME: u8 = b's';
pub const PG_DIAG_TABLE_NAME: u8 = b't';
pub const PG_DIAG_COLUMN_NAME: u8 = b'c';
pub const PG_DIAG_DATATYPE_NAME: u8 = b'd';
pub const PG_DIAG_CONSTRAINT_NAME: u8 = b'n';
pub const PG_DIAG_SOURCE_FILE: u8 = b'F';
pub const PG_DIAG_SOURCE_LINE: u8 = b'L';
pub const PG_DIAG_SOURCE_FUNCTION: u8 = b'R';

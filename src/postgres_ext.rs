//! Translated from PostgreSQL src/include/postgres_ext.h

use core::num::NonZeroU32;

/// Object ID is a fundamental type in Postgres (C: `typedef unsigned int Oid`).
///
/// Backed by `Option<NonZeroU32>` so the niche keeps `Oid` at exactly 4 bytes
/// while making `InvalidOid` (PG's `0`) an explicit `None`. The in-memory byte
/// image is unchanged: `None` is the all-zero word (== PG `InvalidOid`),
/// `Some(n)` is `n` in native byte order, so on-disk and wire serialization
/// stay bit-compatible.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Oid(pub Option<NonZeroU32>);

// The NonZeroU32 niche keeps Oid at 4 bytes. `Option<Oid>` is NOT 4 bytes: the
// single zero-niche is already spent on Oid's own `None` (== InvalidOid).
const _: () = assert!(core::mem::size_of::<Oid>() == 4);

impl Oid {
    /// PG `InvalidOid` (0).
    pub const INVALID: Self = Self(None);
    /// PG `OID_MAX` (`u32::MAX`).
    pub const MAX: Self = Self(Some(NonZeroU32::MAX));

    /// Construct from a raw u32; `0` maps to `INVALID`.
    #[inline]
    #[must_use]
    pub const fn new(v: u32) -> Self {
        Self(NonZeroU32::new(v))
    }

    /// Raw u32 value; `INVALID` yields `0`.
    #[inline]
    #[must_use]
    pub const fn get(self) -> u32 {
        match self.0 {
            Some(n) => n.get(),
            None => 0,
        }
    }

    /// True unless this is `INVALID`.
    #[inline]
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.0.is_some()
    }
}

impl From<u32> for Oid {
    #[inline]
    fn from(v: u32) -> Self {
        Self::new(v)
    }
}

impl From<Oid> for u32 {
    #[inline]
    fn from(o: Oid) -> Self {
        o.get()
    }
}

impl core::fmt::Display for Oid {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(&self.get(), f)
    }
}

/// PG `InvalidOid` (0). Compat alias for [`Oid::INVALID`].
pub const InvalidOid: Oid = Oid::INVALID;
/// PG `OID_MAX`. Compat alias for [`Oid::MAX`].
pub const OID_MAX: Oid = Oid::MAX;

/// C: `#define atooid(x) ((Oid) strtoul((x), NULL, 10))`
pub fn atooid(x: &str) -> Oid {
    Oid::new(x.trim().parse().unwrap_or(0))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oid_invalid_roundtrips_to_zero() {
        assert_eq!(Oid::INVALID.get(), 0);
        assert_eq!(Oid::new(0), Oid::INVALID);
        assert!(!Oid::INVALID.is_valid());
        assert_eq!(InvalidOid, Oid::INVALID);
    }

    #[test]
    fn oid_value_roundtrips() {
        for v in [1u32, 96, 1247, 6104, u32::MAX] {
            assert_eq!(Oid::new(v).get(), v);
            assert!(Oid::new(v).is_valid());
        }
        assert_eq!(Oid::MAX.get(), u32::MAX);
    }

    /// On-disk/wire compatibility: the byte image of an Oid is its raw u32 in
    /// native order, with INVALID == the all-zero word (PG `InvalidOid`).
    #[test]
    fn oid_byte_image_matches_raw_u32() {
        for v in [0u32, 1, 1247, u32::MAX] {
            let oid = Oid::new(v);
            // Reinterpret the 4-byte Oid as a u32 and back.
            let bytes = oid.get().to_ne_bytes();
            assert_eq!(bytes, v.to_ne_bytes());
            assert_eq!(Oid::new(u32::from_ne_bytes(bytes)), oid);
        }
        // INVALID is the zero word.
        assert_eq!(Oid::INVALID.get().to_ne_bytes(), 0u32.to_ne_bytes());
    }

    #[test]
    fn oid_size_is_four_bytes() {
        assert_eq!(core::mem::size_of::<Oid>(), 4);
    }
}

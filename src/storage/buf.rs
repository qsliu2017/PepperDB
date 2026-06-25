//! Translated from PostgreSQL src/include/storage/buf.h
//!
//! The buffer handle. PostgreSQL overloads a signed `Buffer = int`: 0 is invalid,
//! a positive `N` is the shared buffer at `buf_id = N - 1`, and a negative value
//! `-i - 1` is the local (temp-relation) buffer at index `i`. This is an
//! IN-MEMORY handle (never on disk or on the wire), so the port replaces the
//! sign-overloaded integer with a clear [`BufId`] enum -- a buffer is either
//! invalid, a global (shared-pool) index, or a local (per-task pool) index, and
//! the dispatch points that used to branch on sign now `match`.

/// A buffer-manager handle. Invalid, a 0-based index into the shared buffer pool
/// ([`BufId::Global`]), or a 0-based index into the per-task local (temp-rel)
/// pool ([`BufId::Local`]). `Copy + Send + Sync` (the payloads are `u32`), so it
/// crosses `.await` points and tasks freely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BufId {
    /// C: `InvalidBuffer` (0).
    Invalid,
    /// A shared-pool buffer at this 0-based `buf_id`. C: positive `Buffer`.
    Global(u32),
    /// A local (temp-relation) buffer at this 0-based index. C: negative `Buffer`.
    Local(u32),
}

impl BufId {
    /// True iff the handle refers to a real buffer (not [`BufId::Invalid`]).
    #[inline]
    pub fn is_valid(&self) -> bool {
        !matches!(self, BufId::Invalid)
    }

    /// True iff this is a local (temp-relation) buffer.
    #[inline]
    pub fn is_local(&self) -> bool {
        matches!(self, BufId::Local(_))
    }

    /// True iff this is a shared-pool buffer.
    #[inline]
    pub fn is_global(&self) -> bool {
        matches!(self, BufId::Global(_))
    }

    /// Construct a shared-pool handle for a 0-based pool index.
    #[inline]
    pub fn global(index: u32) -> Self {
        BufId::Global(index)
    }

    /// Construct a local-pool handle for a 0-based pool index.
    #[inline]
    pub fn local(index: u32) -> Self {
        BufId::Local(index)
    }

    /// The 0-based shared-pool index, if this is a [`BufId::Global`].
    #[inline]
    pub fn as_global(&self) -> Option<u32> {
        match self {
            BufId::Global(i) => Some(*i),
            _ => None,
        }
    }

    /// The 0-based local-pool index, if this is a [`BufId::Local`].
    #[inline]
    pub fn as_local(&self) -> Option<u32> {
        match self {
            BufId::Local(i) => Some(*i),
            _ => None,
        }
    }

    /// PG-compat bridge: the legacy sign-overloaded `int` encoding
    /// (`Invalid -> 0`, `Global(i) -> i + 1`, `Local(i) -> -i - 1`). For deferred
    /// consumers still threading PG's integer and for debug printing only -- new
    /// code uses the enum.
    #[inline]
    pub fn to_legacy_i32(&self) -> i32 {
        match self {
            BufId::Invalid => 0,
            BufId::Global(i) => (*i as i32) + 1,
            BufId::Local(i) => -(*i as i32) - 1,
        }
    }

    /// PG-compat bridge: decode the legacy sign-overloaded `int` encoding. Inverse
    /// of [`to_legacy_i32`](Self::to_legacy_i32). Not for new code.
    #[inline]
    pub fn from_legacy_i32(i: i32) -> Self {
        if i == 0 {
            BufId::Invalid
        } else if i > 0 {
            BufId::Global((i - 1) as u32)
        } else {
            BufId::Local((-i - 1) as u32)
        }
    }
}

/// The PG-named buffer handle type. Kept as an alias of [`BufId`] so PG-named
/// signatures in deferred stubs read naturally; [`BufId`] is the real type.
pub type Buffer = BufId;

/// C: `InvalidBuffer`.
pub const INVALID_BUFFER: Buffer = BufId::Invalid;

/// True iff the buffer is invalid. C: `BufferIsInvalid`.
#[deprecated(note = "use `buffer.is_valid()` (negated)")]
#[inline]
pub fn buffer_is_invalid(buffer: Buffer) -> bool {
    !buffer.is_valid()
}

/// True iff the buffer is local (not visible to other backends). C: `BufferIsLocal`.
#[deprecated(note = "use `buffer.is_local()`")]
#[inline]
pub fn buffer_is_local(buffer: Buffer) -> bool {
    buffer.is_local()
}

/// Opaque; BufferAccessStrategyData is private to freelist.c (not ported).
pub struct BufferAccessStrategy {
    _opaque: [u8; 0],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predicates_and_accessors() {
        assert!(!BufId::Invalid.is_valid());
        assert!(BufId::Global(0).is_valid());
        assert!(BufId::Local(0).is_valid());

        assert!(BufId::Global(3).is_global());
        assert!(!BufId::Global(3).is_local());
        assert!(BufId::Local(3).is_local());
        assert!(!BufId::Local(3).is_global());

        assert_eq!(BufId::global(7), BufId::Global(7));
        assert_eq!(BufId::local(7), BufId::Local(7));

        assert_eq!(BufId::Global(5).as_global(), Some(5));
        assert_eq!(BufId::Global(5).as_local(), None);
        assert_eq!(BufId::Local(5).as_local(), Some(5));
        assert_eq!(BufId::Local(5).as_global(), None);
        assert_eq!(BufId::Invalid.as_global(), None);
        assert_eq!(BufId::Invalid.as_local(), None);
    }

    #[test]
    fn legacy_encoding_matches_pg_and_round_trips() {
        // PG's sign-overloaded encoding.
        assert_eq!(BufId::Invalid.to_legacy_i32(), 0);
        assert_eq!(BufId::Global(0).to_legacy_i32(), 1);
        assert_eq!(BufId::Local(0).to_legacy_i32(), -1);
        assert_eq!(BufId::Global(41).to_legacy_i32(), 42);
        assert_eq!(BufId::Local(41).to_legacy_i32(), -42);

        for b in [BufId::Invalid, BufId::Global(0), BufId::Local(0), BufId::Global(999), BufId::Local(999)] {
            assert_eq!(BufId::from_legacy_i32(b.to_legacy_i32()), b);
        }
        assert_eq!(BufId::from_legacy_i32(0), BufId::Invalid);
        assert_eq!(BufId::from_legacy_i32(1), BufId::Global(0));
        assert_eq!(BufId::from_legacy_i32(-1), BufId::Local(0));
    }
}

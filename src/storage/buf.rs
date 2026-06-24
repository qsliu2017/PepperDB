//! Translated from PostgreSQL src/include/storage/buf.h

/// Buffer identifiers. Zero is invalid, positive = shared buffer index
/// (1..NBuffers), negative = local buffer index (-1 .. -NLocBuffer).
pub type Buffer = i32;

pub const INVALID_BUFFER: Buffer = 0;

/// True iff the buffer is invalid.
pub const fn buffer_is_invalid(buffer: Buffer) -> bool {
    buffer == INVALID_BUFFER
}

/// True iff the buffer is local (not visible to other backends).
pub const fn buffer_is_local(buffer: Buffer) -> bool {
    buffer < 0
}

// TODO(struct-forward): BufferAccessStrategyData is private to freelist.c;
// repoint to crate::storage::freelist in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::storage::freelist::BufferAccessStrategy in Phase 2")]
pub struct BufferAccessStrategy {
    _opaque: [u8; 0],
}

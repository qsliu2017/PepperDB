//! storage/buf.h - Basic buffer manager data types.

/*
 * Buffer identifiers.
 *
 * Zero is invalid, positive is the index of a shared buffer (1..NBuffers),
 * negative is the index of a local buffer (-1 .. -NLocBuffer).
 */
pub type Buffer = i32;

pub const InvalidBuffer: Buffer = 0;

/*
 * BufferIsInvalid
 *		True iff the buffer is invalid.
 */
#[inline]
pub fn BufferIsInvalid(buffer: Buffer) -> bool {
    buffer == InvalidBuffer
}

/*
 * BufferIsLocal
 *		True iff the buffer is local (not visible to other backends).
 */
#[inline]
pub fn BufferIsLocal(buffer: Buffer) -> bool {
    buffer < 0
}

/*
 * Buffer access strategy objects.
 *
 * BufferAccessStrategyData is private to freelist.c
 */
// Opaque struct defined privately in freelist.c; only the pointer typedef is public.
#[repr(C)]
pub struct BufferAccessStrategyData {
    _private: [u8; 0],
}

pub type BufferAccessStrategy = *mut BufferAccessStrategyData;

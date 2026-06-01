//! Translation of postgres/src/include/utils/palloc.h - bootstrap allocator.
//!
//! In PostgreSQL, every allocation belongs to a MemoryContext; resetting or
//! deleting a context frees all of its chunks at once (see utils/mmgr/README).
//! That subsystem (mcxt.c, aset.c, generation.c, slab.c) is a large translation
//! of its own. Until it lands, this module provides a *working* drop-in allocator
//! backed by the Rust global allocator: each chunk stores its own size in an
//! 8-byte (MAXALIGN) header so `pfree`/`repalloc` can recover the layout.
//!
//! The MemoryContext API surface is present but currently context-less.
//!
//! TODO(pg-port): replace with the real MemoryContext system.

use crate::c::{Size, MAXALIGN};
use crate::utils::memutils::MaxAllocSize;
use core::ffi::{c_char, c_void};
use std::alloc::{self, Layout};

/// Opaque memory context handle. `CurrentMemoryContext` is the default target of
/// `palloc`. The canonical `MemoryContextData` lives in `utils::mmgr::memnodes`
/// (the home of the C `nodes/memnodes.h` struct); palloc.h declares the
/// allocation *functions*. Re-export so the whole tree shares one type identity.
pub use crate::utils::mmgr::memnodes::{MemoryContext, MemoryContextData};

use crate::nodes::nodes::NodeTag;

/// A single non-NULL sentinel context for the bootstrap allocator. PostgreSQL
/// always has a valid `CurrentMemoryContext` after MemoryContextInit(); some code
/// (e.g. dynahash) asserts `MemoryContextIsValid(...)`, so we point the current
/// context at this sentinel rather than NULL. Allocation ignores the context.
static mut bootstrap_context: MemoryContextData = MemoryContextData {
    r#type: NodeTag::T_AllocSetContext,
    isReset: true,
    allowInCritSection: false,
    mem_allocated: 0,
    methods: core::ptr::null(),
    parent: core::ptr::null_mut(),
    firstchild: core::ptr::null_mut(),
    prevchild: core::ptr::null_mut(),
    nextchild: core::ptr::null_mut(),
    name: core::ptr::null(),
    ident: core::ptr::null(),
    reset_cbs: core::ptr::null_mut(),
};

/// `CurrentMemoryContext`: the default allocation context for `palloc`. The
/// bootstrap allocator is context-less; this points at a non-NULL sentinel so
/// `MemoryContextIsValid` holds. `MemoryContextSwitchTo` updates it as usual.
///
/// TODO(pg-port): becomes meaningful with the real MemoryContext system.
pub static mut CurrentMemoryContext: MemoryContext = &raw mut bootstrap_context;

/// `TopMemoryContext`: the permanent top-level context. Points at the same
/// bootstrap sentinel until the real MemoryContext system lands.
pub static mut TopMemoryContext: MemoryContext = &raw mut bootstrap_context;

/// `MemoryContextSwitchTo(context)`: install `context` as current, returning the
/// previous one (palloc.h inline). Context-less here, but preserves the protocol.
///
/// # Safety
/// Accesses the `CurrentMemoryContext` global.
#[inline]
pub unsafe fn MemoryContextSwitchTo(context: MemoryContext) -> MemoryContext {
    let old = CurrentMemoryContext;
    CurrentMemoryContext = context;
    old
}

pub type MemoryContextCallbackFunction = Option<unsafe extern "C" fn(arg: *mut c_void)>;

#[repr(C)]
pub struct MemoryContextCallback {
    pub func: MemoryContextCallbackFunction,
    pub arg: *mut c_void,
    pub next: *mut MemoryContextCallback,
}

// Flags for MemoryContextAllocExtended.
pub const MCXT_ALLOC_HUGE: i32 = 0x01;
pub const MCXT_ALLOC_NO_OOM: i32 = 0x02;
pub const MCXT_ALLOC_ZERO: i32 = 0x04;

/// Size of the per-chunk header storing the usable byte count. MAXALIGN-sized so
/// the returned pointer keeps maximum alignment.
const HEADER: usize = MAXALIGN(core::mem::size_of::<usize>());
const ALIGN: usize = crate::pg_config::MAXIMUM_ALIGNOF;

#[inline]
fn layout_for(total: usize) -> Layout {
    Layout::from_size_align(total, ALIGN).expect("invalid palloc layout")
}

/// `palloc(size)`: allocate `size` bytes in the current memory context.
///
/// # Safety
/// The returned pointer must eventually be released with [`pfree`] (or resized
/// with [`repalloc`]); the usual raw-pointer aliasing rules apply.
pub unsafe fn palloc(size: Size) -> *mut c_void {
    if size > MaxAllocSize {
        crate::utils::elog::emit_log(
            crate::utils::elog::ERROR,
            &format!("invalid memory alloc request size {}", size),
            file!(),
            line!(),
        );
    }
    let total = HEADER + size;
    let base = alloc::alloc(layout_for(total));
    if base.is_null() {
        alloc::handle_alloc_error(layout_for(total));
    }
    *(base as *mut usize) = size;
    base.add(HEADER) as *mut c_void
}

/// `palloc0(size)`: like `palloc` but zero-initialized.
///
/// # Safety
/// See [`palloc`].
pub unsafe fn palloc0(size: Size) -> *mut c_void {
    let p = palloc(size);
    core::ptr::write_bytes(p as *mut u8, 0, size);
    p
}

/// `palloc_extended(size, flags)`: honors MCXT_ALLOC_ZERO; other flags are ignored
/// by the bootstrap allocator.
///
/// # Safety
/// See [`palloc`].
pub unsafe fn palloc_extended(size: Size, flags: i32) -> *mut c_void {
    let p = palloc(size);
    if flags & MCXT_ALLOC_ZERO != 0 {
        core::ptr::write_bytes(p as *mut u8, 0, size);
    }
    p
}

#[inline]
unsafe fn chunk_base(pointer: *mut c_void) -> *mut u8 {
    (pointer as *mut u8).sub(HEADER)
}

#[inline]
unsafe fn chunk_size(pointer: *mut c_void) -> usize {
    *(chunk_base(pointer) as *mut usize)
}

/// `pfree(pointer)`: release a chunk obtained from `palloc`/`repalloc`.
///
/// # Safety
/// `pointer` must have come from this allocator and not been freed already.
pub unsafe fn pfree(pointer: *mut c_void) {
    let base = chunk_base(pointer);
    let size = *(base as *mut usize);
    alloc::dealloc(base, layout_for(HEADER + size));
}

/// `repalloc(pointer, size)`: resize a chunk, preserving its contents.
///
/// # Safety
/// `pointer` must have come from this allocator.
pub unsafe fn repalloc(pointer: *mut c_void, size: Size) -> *mut c_void {
    if size > MaxAllocSize {
        crate::utils::elog::emit_log(
            crate::utils::elog::ERROR,
            &format!("invalid memory alloc request size {}", size),
            file!(),
            line!(),
        );
    }
    let base = chunk_base(pointer);
    let oldsize = *(base as *mut usize);
    let new_total = HEADER + size;
    let newbase = alloc::realloc(base, layout_for(HEADER + oldsize), new_total);
    if newbase.is_null() {
        alloc::handle_alloc_error(layout_for(new_total));
    }
    *(newbase as *mut usize) = size;
    newbase.add(HEADER) as *mut c_void
}

/// `repalloc0(pointer, oldsize, size)`: resize then zero the freshly grown tail.
///
/// # Safety
/// See [`repalloc`]; `oldsize` must be the previous logical size.
pub unsafe fn repalloc0(pointer: *mut c_void, oldsize: Size, size: Size) -> *mut c_void {
    let p = repalloc(pointer, size);
    if size > oldsize {
        core::ptr::write_bytes((p as *mut u8).add(oldsize), 0, size - oldsize);
    }
    p
}

// ---- MemoryContext* entry points (context-less in the bootstrap allocator) ----

/// `MemoryContextAlloc(context, size)`.
///
/// # Safety
/// See [`palloc`].
pub unsafe fn MemoryContextAlloc(_context: MemoryContext, size: Size) -> *mut c_void {
    palloc(size)
}

/// `MemoryContextAllocZero(context, size)`.
///
/// # Safety
/// See [`palloc`].
pub unsafe fn MemoryContextAllocZero(_context: MemoryContext, size: Size) -> *mut c_void {
    palloc0(size)
}

/// `MemoryContextAllocExtended(context, size, flags)`: honors MCXT_ALLOC_ZERO;
/// other flags ignored by the bootstrap allocator.
///
/// # Safety
/// See [`palloc`].
pub unsafe fn MemoryContextAllocExtended(
    _context: MemoryContext,
    size: Size,
    flags: i32,
) -> *mut c_void {
    palloc_extended(size, flags)
}

/// `MemoryContextIsValid(context)`: true if non-NULL (bootstrap approximation).
#[inline]
pub fn MemoryContextIsValid(context: MemoryContext) -> bool {
    !context.is_null()
}

/// `MemoryContextSetIdentifier(context, id)`: attach a debug identifier. No-op in
/// the bootstrap allocator.
///
/// # Safety
/// Matches the C signature; arguments are ignored here.
pub unsafe fn MemoryContextSetIdentifier(_context: MemoryContext, _id: *const c_char) {
    // TODO(pg-port): store the identifier on the real MemoryContext.
}

/// `GetMemoryChunkContext(pointer)` (utils/mmgr/mcxt.c): the context a chunk lives
/// in. The bootstrap allocator is context-less, so this returns NULL; callers feed
/// it back to `MemoryContextAlloc`, which ignores the context.
///
/// # Safety
/// `pointer` must be a chunk from this allocator (unused here, but matches the API).
pub unsafe fn GetMemoryChunkContext(_pointer: *mut c_void) -> MemoryContext {
    // TODO(pg-port): real MemoryContext bookkeeping (utils/mmgr).
    core::ptr::null_mut()
}

/// `pstrdup(in)`: duplicate a NUL-terminated C string into palloc'd memory.
///
/// # Safety
/// `s` must point to a valid NUL-terminated C string.
pub unsafe fn pstrdup(s: *const c_char) -> *mut c_char {
    let len = strlen(s);
    let p = palloc(len + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(s, p, len + 1);
    p
}

/// `pnstrdup(in, len)`: copy at most `len` bytes, NUL-terminating the result.
///
/// # Safety
/// `s` must be valid for `len` bytes.
pub unsafe fn pnstrdup(s: *const c_char, len: Size) -> *mut c_char {
    // honor an embedded NUL within the first `len` bytes, like C strnlen.
    let mut n = 0usize;
    while n < len && *s.add(n) != 0 {
        n += 1;
    }
    let p = palloc(n + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(s, p, n);
    *p.add(n) = 0;
    p
}

/// Minimal `strlen` over a C string (bootstrap helper; mirrors libc strlen).
///
/// # Safety
/// `s` must point to a valid NUL-terminated C string.
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

//! Translation of postgres/src/include/nodes/memnodes.h
//!
//! POSTGRES memory-context node definitions: the abstract `MemoryContextData`
//! and its virtual method table `MemoryContextMethods`.
//!
//! NOTE (port staging): this is the REAL MemoryContext machinery, translated
//! additively under `utils/mmgr` while the rest of the crate still uses the
//! context-less bootstrap allocator in `utils::palloc`. To avoid clashing with
//! the bootstrap `MemoryContext`/`MemoryContextData`/`MemoryContextIsValid`
//! symbols, this module imports types explicitly rather than globbing the
//! prelude. The final rewiring step unifies the two (see project notes / the
//! `TODO(pg-port): unify with utils::palloc` markers).

use crate::c::Size;
use crate::nodes::nodes::{nodeTag, NodeTag};
use crate::utils::palloc::MemoryContextCallback;
use crate::IsA;
use core::ffi::{c_char, c_int, c_void};

/// `MemoryContext`: a pointer to the context struct (historical typedef).
pub type MemoryContext = *mut MemoryContextData;

/*
 * MemoryContextCounters
 *		Summarization state for MemoryContextStats collection.
 */
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct MemoryContextCounters {
    /// Total number of malloc blocks
    pub nblocks: Size,
    /// Total number of free chunks
    pub freechunks: Size,
    /// Total bytes requested from malloc
    pub totalspace: Size,
    /// The unused portion of totalspace
    pub freespace: Size,
}

/// Callback used by MemoryContextStats to print a context's stats line.
pub type MemoryStatsPrintFunc = Option<
    unsafe fn(
        context: MemoryContext,
        passthru: *mut c_void,
        stats_string: *const c_char,
        print_to_stderr: bool,
    ),
>;

/*
 * MemoryContextMethods - the virtual function table for a context implementation.
 *
 * Node types that are actual implementations of memory contexts must begin with
 * the same fields as MemoryContextData.
 */
#[repr(C)]
pub struct MemoryContextMethods {
    /// Allocate `size` bytes into `context`; must handle MCXT_ALLOC_HUGE/NO_OOM.
    pub alloc: Option<unsafe fn(context: MemoryContext, size: Size, flags: c_int) -> *mut c_void>,
    /// Free a chunk (named free_p in case someone #define's free()).
    pub free_p: Option<unsafe fn(pointer: *mut c_void)>,
    /// Resize an existing allocation; must handle MCXT_ALLOC_HUGE/NO_OOM.
    pub realloc: Option<unsafe fn(pointer: *mut c_void, size: Size, flags: c_int) -> *mut c_void>,
    /// Invalidate all allocations and prepare for reuse.
    pub reset: Option<unsafe fn(context: MemoryContext)>,
    /// Free all memory consumed by the context.
    pub delete_context: Option<unsafe fn(context: MemoryContext)>,
    /// Return the MemoryContext a pointer belongs to.
    pub get_chunk_context: Option<unsafe fn(pointer: *mut c_void) -> MemoryContext>,
    /// Bytes consumed by a pointer including alignment + chunk-header overhead.
    pub get_chunk_space: Option<unsafe fn(pointer: *mut c_void) -> Size>,
    /// True if no allocations since creation or last reset.
    pub is_empty: Option<unsafe fn(context: MemoryContext) -> bool>,
    pub stats: Option<
        unsafe fn(
            context: MemoryContext,
            printfunc: MemoryStatsPrintFunc,
            passthru: *mut c_void,
            totals: *mut MemoryContextCounters,
            print_to_stderr: bool,
        ),
    >,
    // MEMORY_CONTEXT_CHECKING: `check` is omitted (not compiled in by default).
    // TODO(pg-port): add `check` under a cfg when MEMORY_CONTEXT_CHECKING is modeled.
}

/*
 * MemoryContextData - the abstract base of every context implementation.
 * Implementations (AllocSet, Generation, Slab, Bump) embed this as their first
 * field, so a pointer to any of them can be treated as a MemoryContext.
 */
#[repr(C)]
pub struct MemoryContextData {
    /// identifies exact kind of context
    pub r#type: NodeTag,
    /// T = no space alloced since last reset
    pub isReset: bool,
    /// allow palloc in critical section
    pub allowInCritSection: bool,
    /// track memory allocated for this context
    pub mem_allocated: Size,
    /// virtual function table
    pub methods: *const MemoryContextMethods,
    /// NULL if no parent (toplevel context)
    pub parent: MemoryContext,
    /// head of linked list of children
    pub firstchild: MemoryContext,
    /// previous child of same parent
    pub prevchild: MemoryContext,
    /// next child of same parent
    pub nextchild: MemoryContext,
    /// context name
    pub name: *const c_char,
    /// context ID if any
    pub ident: *const c_char,
    /// list of reset/delete callbacks
    pub reset_cbs: *mut MemoryContextCallback,
}

/// `MemoryContextIsValid(context)`: true iff `context` is non-NULL and is one of
/// the known context implementations. (The real version; distinct from the
/// bootstrap `utils::palloc::MemoryContextIsValid`.)
///
/// # Safety
/// `context` must be NULL or point to a node beginning with a NodeTag.
#[inline]
pub unsafe fn MemoryContextIsValid(context: MemoryContext) -> bool {
    !context.is_null()
        && (IsA!(context, T_AllocSetContext)
            || IsA!(context, T_SlabContext)
            || IsA!(context, T_GenerationContext)
            || IsA!(context, T_BumpContext))
}

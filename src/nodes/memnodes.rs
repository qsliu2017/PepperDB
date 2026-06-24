//! Translated from PostgreSQL src/include/nodes/memnodes.h


/// Summarization state for `MemoryContextStats` collection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemoryContextCounters {
    pub nblocks: usize,
    pub freechunks: usize,
    pub totalspace: usize,
    pub freespace: usize,
}

/// Callback to run at memory context reset/delete.
///
/// C threads caller state via `void *arg`; in Rust the closure captures it,
/// so the `arg` field disappears (function-mapping section 6.3).
// TODO(struct-forward): real definition lives in utils/palloc.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::palloc in Phase 2")]
pub struct MemoryContextCallback {
    pub func: Box<dyn FnOnce()>,
}

/// The built-in memory-context implementations. C dispatches through a vtable of
/// function pointers (`MemoryContextMethods`); since the set of kinds is closed
/// (routine-struct.md section 4), Rust stores the kind and dispatches by match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum MemoryContextKind {
    AllocSet,
    Slab,
    Generation,
    Bump,
}

/// Virtual function table for a memory context implementation.
///
/// Group C ("all callbacks mandatory") -> a trait with no supertraits
/// (routine-struct.md). Kept for later static dispatch; the live context stores a
/// `MemoryContextKind`, not a `dyn` of this trait. `check` is only present under
/// `MEMORY_CONTEXT_CHECKING`.
pub trait MemoryContextMethods {
    /// C: `alloc(context, size, flags)`. Handles `MCXT_ALLOC_HUGE`/`MCXT_ALLOC_NO_OOM`.
    fn alloc(&self, context: &mut MemoryContext, size: usize, flags: i32) -> *mut u8;
    /// C: `free_p(pointer)`.
    fn free_p(&self, pointer: *mut u8);
    /// C: `realloc(pointer, size, flags)`.
    fn realloc(&self, pointer: *mut u8, size: usize, flags: i32) -> *mut u8;
    /// C: `reset(context)`.
    fn reset(&self, context: &mut MemoryContext);
    /// C: `delete_context(context)`.
    fn delete_context(&self, context: &mut MemoryContext);
    /// C: `get_chunk_context(pointer)`.
    fn get_chunk_context(&self, pointer: *mut u8) -> MemoryContext;
    /// C: `get_chunk_space(pointer)`.
    fn get_chunk_space(&self, pointer: *mut u8) -> usize;
    /// C: `is_empty(context)`.
    fn is_empty(&self, context: &MemoryContext) -> bool;
    /// C: `stats(...)`. `MemoryStatsPrintFunc` + `passthru` fold into a closure.
    fn stats(
        &self,
        context: &MemoryContext,
        printfunc: impl FnMut(&MemoryContext, &str, bool),
        totals: Option<&mut MemoryContextCounters>,
        print_to_stderr: bool,
    );
    /// C: `check(context)` -- only under `MEMORY_CONTEXT_CHECKING`. Default no-op.
    fn check(&self, _context: &MemoryContext) {}
}

/// A logical context in which memory allocations occur.
///
/// C `MemoryContextData` value struct; the `MemoryContext` typedef is a pointer
/// to it. In-memory state -> idiomatic; the parent/child pointers become a tree
/// resolved in Phase 2 (memory contexts -> arena/Box + RAII).
// TODO(ptr): parent/child links default to None; ownership resolved with the .c body.
pub struct MemoryContextData {
    pub is_reset: bool,
    pub allow_in_crit_section: bool,
    pub mem_allocated: usize,
    pub kind: MemoryContextKind,
    pub parent: Option<MemoryContext>,
    pub firstchild: Option<MemoryContext>,
    pub prevchild: Option<MemoryContext>,
    pub nextchild: Option<MemoryContext>,
    pub name: Option<String>,
    pub ident: Option<String>,
    #[allow(deprecated)]
    pub reset_cbs: Vec<MemoryContextCallback>,
}

/// C: `typedef struct MemoryContextData *MemoryContext` -- a handle.
// Modeled as an owning Box for the skeleton; the arena/RAII model lands in Phase 2.
pub type MemoryContext = Box<MemoryContextData>;

/// C: `MemoryContextIsValid(context)` -- a real, recognized context type.
pub fn MemoryContextIsValid(context: &MemoryContextData) -> bool {
    // TODO(struct-forward): once NodeTag carries the context-type tags, test
    // AllocSetContext/SlabContext/GenerationContext/BumpContext.
    let _ = context;
    true
}

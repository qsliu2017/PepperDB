//! Memory-context management (postgres/src/backend/utils/mmgr + nodes/memnodes.h).
//!
//! The real, context-based allocator. Translated additively while the rest of the
//! crate still uses the context-less bootstrap allocator in [`crate::utils::palloc`];
//! the final step rewires `palloc`/`pfree` to dispatch through these contexts.

pub mod portalmem;
pub mod dsa;
pub mod freepage;
pub mod alignedalloc;
pub mod aset;
pub mod bump;
pub mod generation;
pub mod mcxt;
pub mod memdebug;
pub mod memnodes;
pub mod memutils_internal;
pub mod memutils_memorychunk;
pub mod slab;

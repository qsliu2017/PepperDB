//! Directory module: src/backend/utils/init
//!
//! Backend/session initialization: the C-side translations of globals.c,
//! postinit.c, miscinit.c, and usercontext.c. The header-origin type/function
//! shims live in `src/miscadmin.rs` and `src/utils/*`; this directory holds the
//! `.c` implementations.

pub mod globals;
pub mod miscinit;
pub mod postinit;
pub mod usercontext;

//! Just-in-time compilation support
//! (postgres/src/backend/jit + postgres/src/include/jit).
//!
//! Header-only layer so far. The LLVM-backed emitter headers reference LLVM C/C++
//! types that are locally stubbed as c_void until/unless an LLVM backend is wired.

pub mod SectionMemoryManager;
pub mod llvmjit_backport;
pub mod llvmjit_emit;

pub mod jit;

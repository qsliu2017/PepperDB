//! jit/llvmjit_backport.h - controls conditional compilation for backported LLVM code.
//!
//! This header is purely preprocessor with no Rust-meaningful runtime content. It
//! includes <llvm/Config/llvm-config.h> (a system/LLVM config header, not portable
//! to Rust) and conditionally defines USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER on
//! aarch64 targets.
//!
//! In C, the macro USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER is defined only when
//! compiling for __aarch64__, as a workaround for an LLVM RuntimeDyld bug where
//! llvm::SectionMemoryManager can place allocations too far apart for the generated
//! code on larger-memory ARM systems. See the patched llvm::backport::SectionMemoryManager
//! (src/backend/jit/llvm/SectionMemoryManager.cpp) for the replacement.
//!
//! The C gate `#if defined(__aarch64__)` is reflected here via cfg(target_arch).
//! Per project convention this is NOT a Cargo feature; it is a target-arch gate, so
//! the corresponding marker constant is expressed unconditionally as a bool computed
//! from the target architecture.

/// True when the backported SectionMemoryManager should be used.
///
/// Mirrors the C `#if defined(__aarch64__) #define USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER`
/// guard. On aarch64 this is `true`; otherwise `false`.
#[cfg(target_arch = "aarch64")]
pub const USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER: bool = true;

/// True when the backported SectionMemoryManager should be used.
///
/// Mirrors the C `#if defined(__aarch64__) #define USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER`
/// guard. On aarch64 this is `true`; otherwise `false`.
#[cfg(not(target_arch = "aarch64"))]
pub const USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER: bool = false;

//! port/darwin.h - macOS (Darwin) platform tweaks.
//!
//! This header is purely C preprocessor platform-config (feature-detection
//! `#define`s). The macros below only ever gate conditional compilation in the
//! C sources; they have no runtime Rust-level representation. They are exposed
//! here as marker `pub const` values for faithfulness.

/// `#define __darwin__ 1` - identifies the Darwin platform.
pub const __darwin__: i32 = 1;

// `#if HAVE_DECL_F_FULLFSYNC` (not present before macOS 10.3):
//   `#define HAVE_FSYNC_WRITETHROUGH`
// This is conditionally defined at C compile time when F_FULLFSYNC is declared.
// On modern macOS it is always defined; exposed here as a marker constant.
/// macOS supports fsync write-through via F_FULLFSYNC.
pub const HAVE_FSYNC_WRITETHROUGH: bool = true;

/// `#define USE_PREFETCH` - macOS has a platform-specific prefetch impl.
pub const USE_PREFETCH: bool = true;

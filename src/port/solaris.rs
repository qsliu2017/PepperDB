//! port/solaris.h - Solaris platform tweaks; mostly preprocessor platform-config.
//!
//! The bulk of this header normalizes compiler-defined `__xxx` arch symbols
//! (`__i386`, `__amd64`, `__x86_64`, `__sparc`) and conditionally includes
//! `<sys/isa_defs.h>` - none of which carry over to Rust. The only
//! Rust-meaningful symbol is the PAM legacy-nonconst flag below.

/// On original Solaris, PAM conversation procs lack a "const" in their
/// declaration; defining this causes OpenIndiana to declare `pam_conv` per the
/// Solaris tradition, and is also used to control omitting the "const" in our
/// own code.
pub const _PAM_LEGACY_NONCONST: c_int = 1;

use std::ffi::c_int;

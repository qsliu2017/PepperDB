//! Checksum implementation for data pages (storage/page/checksum.c).
//!
//! 1:1 translation. The C file is only a compilation shim:
//!   #include "storage/checksum.h"
//!   #include "storage/checksum_impl.h"   // the actual code
//! Following the header+impl merge convention, that code is translated into
//! `src/storage/checksum.rs` (checksum.h + checksum_impl.h). This module mirrors
//! the C compilation unit, which contributes no symbols of its own beyond what
//! the included impl provides.

pub use crate::storage::checksum::*;

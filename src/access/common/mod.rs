//! access/common (postgres/src/backend/access/common) - shared access-method support.
//!
//! TOAST de/compression so far (detoast + toast_internals); the external/on-disk
//! paths are stubbed pending the heap/relation layer.

pub mod attmap;
pub mod relation;
pub mod session;
pub mod bufmask;
pub mod detoast;
pub mod heaptuple;
pub mod indextuple;
pub mod printsimple;
pub mod printtup;
pub mod reloptions;
pub mod scankey;
pub mod syncscan;
pub mod toast_compression;
pub mod toast_internals;
pub mod tupconvert;
pub mod tupdesc;
pub mod tupdesc_details;
pub mod tidstore;

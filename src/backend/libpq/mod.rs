//! Directory module: src/backend/libpq
//!
//! The backend libpq wire layer: `pqcomm` (low-level framed FE/BE I/O over the
//! task-local connection), `pqformat` (message build/parse), `be_secure` (the
//! secure-connection passthrough; plaintext in M1).

#[path = "be-secure.rs"]
pub mod be_secure;
pub mod pqcomm;
pub mod pqformat;

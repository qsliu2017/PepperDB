//! Directory module: src/include/libpq

// === scaffold: child modules (Phase 0) ===
pub mod auth;
#[path = "be-fsstubs.rs"]
pub mod be_fsstubs;
#[path = "be-gssapi-common.rs"]
pub mod be_gssapi_common;
pub mod crypt;
pub mod hba;
pub mod ifaddr;
pub mod libpq;
#[path = "libpq-be.rs"]
pub mod libpq_be;
#[path = "libpq-be-fe-helpers.rs"]
pub mod libpq_be_fe_helpers;
#[path = "libpq-fs.rs"]
pub mod libpq_fs;
pub mod oauth;
#[path = "pg-gssapi.rs"]
pub mod pg_gssapi;
pub mod pqcomm;
pub mod pqformat;
pub mod pqmq;
pub mod pqsignal;
pub mod protocol;
pub mod sasl;
pub mod scram;
// === end scaffold ===

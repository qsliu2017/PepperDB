//! common/openssl.h - OpenSSL supporting functionality shared between frontend and backend.
//!
//! The original header is guarded by `#ifdef USE_OPENSSL` and the MAX version is
//! selected at compile time from the OpenSSL/LibreSSL feature macros
//! (TLS1_3_VERSION / TLS1_2_VERSION / TLS1_1_VERSION). We translate the constants
//! verbatim; MAX_OPENSSL_TLS_VERSION is set to the highest branch ("TLSv1.3"),
//! matching a modern OpenSSL build that defines TLS1_3_VERSION.

/// Oldest TLS protocol version of interest. SSLv3 and older are disabled in
/// library setup, so TLSv1 is the minimum.
pub const MIN_OPENSSL_TLS_VERSION: &str = "TLSv1";

/// Max TLS protocol version the library supports. Selected at C compile time
/// from TLS1_3_VERSION / TLS1_2_VERSION / TLS1_1_VERSION; here we take the
/// highest branch (TLS1_3_VERSION defined).
pub const MAX_OPENSSL_TLS_VERSION: &str = "TLSv1.3";

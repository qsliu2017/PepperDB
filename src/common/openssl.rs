//! Translated from PostgreSQL src/include/common/openssl.h
// OpenSSL TLS version bounds; only meaningful in an OpenSSL build (USE_OPENSSL).

pub const MIN_OPENSSL_TLS_VERSION: &str = "TLSv1";
pub const MAX_OPENSSL_TLS_VERSION: &str = "TLSv1.3";

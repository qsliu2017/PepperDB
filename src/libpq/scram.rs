//! Translated from PostgreSQL src/include/libpq/scram.h
//! Interface to libpq/scram.c.

use crate::common::cryptohash::PgCryptohashType;

/// Number of iterations when generating new secrets (GUC).
pub static mut scram_sha_256_iterations: i32 = 4096;

/// SASL implementation for SCRAM-SHA-256. The C `const pg_be_sasl_mech
/// pg_be_scram_mech` is a vtable instance; here it is a unit struct implementing
/// the `BeSaslMech` trait (see crate::libpq::sasl).
pub struct PgBeScramMech;

/// Build a SCRAM verifier secret from a plaintext password.
pub fn pg_be_scram_build_secret(_password: &str) -> String {
    unimplemented!()
}

/// Parsed components of a SCRAM secret.
pub struct ScramSecret {
    pub iterations: i32,
    pub hash_type: PgCryptohashType,
    pub key_length: i32,
    pub salt: String,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

/// Parse a SCRAM secret; returns None if it is malformed.
pub fn parse_scram_secret(_secret: &str) -> Option<ScramSecret> {
    unimplemented!()
}

/// Verify a plaintext password against a stored SCRAM secret.
pub fn scram_verify_plain_password(_username: &str, _password: &str, _secret: &str) -> bool {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/common/scram-common.h
//! Helper functions used for SCRAM authentication.

use crate::common::cryptohash::PgCryptohashType;
use crate::common::sha2::PG_SHA256_DIGEST_LENGTH;

/// Name of SCRAM mechanisms per IANA.
pub const SCRAM_SHA_256_NAME: &str = "SCRAM-SHA-256";
/// With channel binding.
pub const SCRAM_SHA_256_PLUS_NAME: &str = "SCRAM-SHA-256-PLUS";

/// Length of SCRAM keys (client and server).
pub const SCRAM_SHA_256_KEY_LEN: usize = PG_SHA256_DIGEST_LENGTH;

/// Max of SCRAM_SHA_*_KEY_LEN among supported hash methods.
pub const SCRAM_MAX_KEY_LEN: usize = SCRAM_SHA_256_KEY_LEN;

/// Size of random nonce generated in the authentication exchange (raw bytes).
pub const SCRAM_RAW_NONCE_LEN: usize = 18;

/// Length of salt when generating new secrets, in bytes.
pub const SCRAM_DEFAULT_SALT_LEN: usize = 16;

/// Default number of iterations when generating secret (>= 4096 per RFC 7677).
pub const SCRAM_SHA_256_DEFAULT_ITERATIONS: i32 = 4096;

// errstr out-param folds into the Err payload; status int -> Result.

pub fn scram_salted_password(
    password: &str,
    hash_type: PgCryptohashType,
    key_length: i32,
    salt: &[u8],
    iterations: i32,
    result: &mut [u8],
) -> Result<(), String> {
    let _ = (password, hash_type, key_length, salt, iterations, result);
    unimplemented!()
}

pub fn scram_h(
    input: &[u8],
    hash_type: PgCryptohashType,
    key_length: i32,
    result: &mut [u8],
) -> Result<(), String> {
    let _ = (input, hash_type, key_length, result);
    unimplemented!()
}

pub fn scram_client_key(
    salted_password: &[u8],
    hash_type: PgCryptohashType,
    key_length: i32,
    result: &mut [u8],
) -> Result<(), String> {
    let _ = (salted_password, hash_type, key_length, result);
    unimplemented!()
}

pub fn scram_server_key(
    salted_password: &[u8],
    hash_type: PgCryptohashType,
    key_length: i32,
    result: &mut [u8],
) -> Result<(), String> {
    let _ = (salted_password, hash_type, key_length, result);
    unimplemented!()
}

/// Build a SCRAM verifier secret; returns the secret text or an error string.
pub fn scram_build_secret(
    hash_type: PgCryptohashType,
    key_length: i32,
    salt: &[u8],
    iterations: i32,
    password: &str,
) -> Result<String, String> {
    let _ = (hash_type, key_length, salt, iterations, password);
    unimplemented!()
}

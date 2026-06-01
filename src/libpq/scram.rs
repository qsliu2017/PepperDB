//! libpq/scram.h - Interface to libpq/scram.c (SCRAM-SHA-256 auth secrets)

use crate::c::uint8;
use crate::common::cryptohash::pg_cryptohash_type;
use std::ffi::{c_char, c_int};

// TODO: dedup - pg_be_sasl_mech is defined in libpq/sasl.h, not yet ported.
pub type pg_be_sasl_mech = c_void;
#[allow(non_camel_case_types)]
type c_void = std::ffi::c_void;

// extern PGDLLIMPORT int scram_sha_256_iterations;
extern "C" {
    pub static mut scram_sha_256_iterations: c_int;

    // extern PGDLLIMPORT const pg_be_sasl_mech pg_be_scram_mech;
    pub static pg_be_scram_mech: pg_be_sasl_mech;
}

// extern char *pg_be_scram_build_secret(const char *password);
pub unsafe fn pg_be_scram_build_secret(password: *const c_char) -> *mut c_char {
    unimplemented!()
}

// extern bool parse_scram_secret(const char *secret, int *iterations,
//     pg_cryptohash_type *hash_type, int *key_length, char **salt,
//     uint8 *stored_key, uint8 *server_key);
pub unsafe fn parse_scram_secret(
    secret: *const c_char,
    iterations: *mut c_int,
    hash_type: *mut pg_cryptohash_type,
    key_length: *mut c_int,
    salt: *mut *mut c_char,
    stored_key: *mut uint8,
    server_key: *mut uint8,
) -> bool {
    unimplemented!()
}

// extern bool scram_verify_plain_password(const char *username,
//     const char *password, const char *secret);
pub unsafe fn scram_verify_plain_password(
    username: *const c_char,
    password: *const c_char,
    secret: *const c_char,
) -> bool {
    unimplemented!()
}

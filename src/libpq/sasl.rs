//! libpq/sasl.h - Defines the SASL mechanism interface for the backend.

use std::ffi::{c_char, c_int, c_void};

use crate::lib::stringinfo::StringInfo;

// Port comes from libpq/libpq-be.h, which is not yet ported. Stub locally.
// TODO: dedup with libpq-be Port once ported.
pub type Port = c_void;

/* Status codes for message exchange */
pub const PG_SASL_EXCHANGE_CONTINUE: c_int = 0;
pub const PG_SASL_EXCHANGE_SUCCESS: c_int = 1;
pub const PG_SASL_EXCHANGE_FAILURE: c_int = 2;

/*
 * Maximum accepted size of SASL messages.
 *
 * The messages that the server or libpq generate are much smaller than this,
 * but have some headroom.
 */
pub const PG_MAX_SASL_MESSAGE_LENGTH: c_int = 1024;

/*
 * Backend SASL mechanism callbacks and metadata.
 *
 * To implement a backend mechanism, declare a pg_be_sasl_mech struct with
 * appropriate callback implementations.  Then pass the mechanism to
 * CheckSASLAuth() during ClientAuthentication(), once the server has decided
 * which authentication method to use.
 */
#[repr(C)]
pub struct pg_be_sasl_mech {
    /* get_mechanisms(): retrieves the list of SASL mechanism names. */
    pub get_mechanisms: Option<unsafe extern "C" fn(port: *mut Port, buf: StringInfo)>,

    /* init(): initializes mechanism-specific state for a connection. */
    pub init: Option<
        unsafe extern "C" fn(
            port: *mut Port,
            mech: *const c_char,
            shadow_pass: *const c_char,
        ) -> *mut c_void,
    >,

    /* exchange(): produces a server challenge to be sent to the client. */
    pub exchange: Option<
        unsafe extern "C" fn(
            state: *mut c_void,
            input: *const c_char,
            inputlen: c_int,
            output: *mut *mut c_char,
            outputlen: *mut c_int,
            logdetail: *mut *const c_char,
        ) -> c_int,
    >,

    /* The maximum size allowed for client SASLResponses. */
    pub max_message_length: c_int,
}

/* Common implementation for auth.c */
pub unsafe fn CheckSASLAuth(
    mech: *const pg_be_sasl_mech,
    port: *mut Port,
    shadow_pass: *mut c_char,
    logdetail: *mut *const c_char,
) -> c_int {
    unimplemented!()
}

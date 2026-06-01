//! libpq/oauth.h - Interface to libpq/auth-oauth.c

use std::ffi::{c_char, c_int, c_void};

use crate::c::uint32;

// libpq/libpq-be.h - HbaLine not yet ported; local stub.
// TODO: dedup
pub type HbaLine = c_void;

// libpq/sasl.h - pg_be_sasl_mech not yet ported; local stub.
// TODO: dedup
pub type pg_be_sasl_mech = c_void;

// extern PGDLLIMPORT char *oauth_validator_libraries_string;
extern "C" {
    pub static mut oauth_validator_libraries_string: *mut c_char;
}

#[repr(C)]
pub struct ValidatorModuleState {
    /// Holds the server's PG_VERSION_NUM. Reserved for future extensibility.
    pub sversion: c_int,

    /// Private data pointer for use by a validator module. This can be used to
    /// store state for the module that will be passed to each of its callbacks.
    pub private_data: *mut c_void,
}

#[repr(C)]
pub struct ValidatorModuleResult {
    /// Should be set to true if the token carries sufficient permissions for
    /// the bearer to connect.
    pub authorized: bool,

    /// If the token authenticates the user, this should be set to a palloc'd
    /// string containing the SYSTEM_USER to use for HBA mapping.
    pub authn_id: *mut c_char,
}

/*
 * Validator module callbacks
 */
pub type ValidatorStartupCB = Option<unsafe extern "C" fn(state: *mut ValidatorModuleState)>;
pub type ValidatorShutdownCB = Option<unsafe extern "C" fn(state: *mut ValidatorModuleState)>;
pub type ValidatorValidateCB = Option<
    unsafe extern "C" fn(
        state: *const ValidatorModuleState,
        token: *const c_char,
        role: *const c_char,
        result: *mut ValidatorModuleResult,
    ) -> bool,
>;

/*
 * Identifies the compiled ABI version of the validator module.
 */
pub const PG_OAUTH_VALIDATOR_MAGIC: uint32 = 0x20250220;

#[repr(C)]
pub struct OAuthValidatorCallbacks {
    /// must be set to PG_OAUTH_VALIDATOR_MAGIC
    pub magic: uint32,

    pub startup_cb: ValidatorStartupCB,
    pub shutdown_cb: ValidatorShutdownCB,
    pub validate_cb: ValidatorValidateCB,
}

/*
 * Type of the shared library symbol _PG_oauth_validator_module_init.
 */
pub type OAuthValidatorModuleInit =
    Option<unsafe extern "C" fn() -> *const OAuthValidatorCallbacks>;

// extern PGDLLEXPORT const OAuthValidatorCallbacks *_PG_oauth_validator_module_init(void);
pub unsafe fn _PG_oauth_validator_module_init() -> *const OAuthValidatorCallbacks {
    unimplemented!()
}

/* Implementation */
// extern PGDLLIMPORT const pg_be_sasl_mech pg_be_oauth_mech;
extern "C" {
    pub static pg_be_oauth_mech: pg_be_sasl_mech;
}

/*
 * Ensure a validator named in the HBA is permitted by the configuration.
 */
pub unsafe fn check_oauth_validator(
    hbaline: *mut HbaLine,
    elevel: c_int,
    err_msg: *mut *mut c_char,
) -> bool {
    unimplemented!()
}

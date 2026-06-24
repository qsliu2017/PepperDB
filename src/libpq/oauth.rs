//! Translated from PostgreSQL src/include/libpq/oauth.h
//! Interface to libpq/auth-oauth.c.

use crate::libpq::hba::HbaLine;

/// GUC: list of permitted OAuth validator libraries.
pub static mut oauth_validator_libraries_string: Option<String> = None;

/// `ValidatorModuleState` - per-module state threaded through the callbacks.
pub struct ValidatorModuleState {
    /// Holds the server's PG_VERSION_NUM. Reserved for future extensibility.
    pub sversion: i32,
    /// Private data for use by a validator module (passed to each callback).
    pub private_data: *mut core::ffi::c_void, // TODO(ptr)
}

/// `ValidatorModuleResult` - outcome of validating a bearer token.
pub struct ValidatorModuleResult {
    /// True if the token carries sufficient permissions to connect.
    pub authorized: bool,
    /// The SYSTEM_USER to use for HBA mapping (set even on failure for logging).
    pub authn_id: Option<String>,
}

/// `PG_OAUTH_VALIDATOR_MAGIC` - compiled ABI version of a validator module.
pub const PG_OAUTH_VALIDATOR_MAGIC: u32 = 0x20250220;

/// `OAuthValidatorCallbacks` is a runtime-loaded extension vtable, so per
/// routine-struct.md appendix B (open case) it is a struct of `fn` pointers, not a
/// trait object. `validate` is the only required callback; `startup`/`shutdown` are
/// optional (`None`).
pub struct OAuthValidatorCallbacks {
    /// Must be PG_OAUTH_VALIDATOR_MAGIC.
    pub magic: u32,
    pub startup: Option<fn(&mut ValidatorModuleState)>,
    pub shutdown: Option<fn(&mut ValidatorModuleState)>,
    /// Required: validate `token` for `role`, filling in `result`.
    pub validate: fn(&ValidatorModuleState, &str, &str, &mut ValidatorModuleResult) -> bool,
}

/// SASL implementation for OAuth (the C `const pg_be_sasl_mech pg_be_oauth_mech`
/// vtable; here a unit struct implementing crate::libpq::sasl::BeSaslMech).
pub struct PgBeOauthMech;

/// Entry point a validator shared library exports (`_PG_oauth_validator_module_init`).
/// Returns the module's validator callback table.
pub fn pg_oauth_validator_module_init() -> &'static OAuthValidatorCallbacks {
    unimplemented!()
}

/// Ensure a validator named in the HBA is permitted by the configuration.
/// Returns Ok on success, or Err(message) describing why it is not permitted.
pub fn check_oauth_validator(_hbaline: &HbaLine, _elevel: i32) -> Result<(), String> {
    unimplemented!()
}

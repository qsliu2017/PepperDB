//! Translated from PostgreSQL src/include/libpq/auth.h
//
// Network authentication routines. In-memory API; signatures kept, bodies stubbed.

use crate::libpq::libpq_be::Port;
use crate::libpq::pqcomm::AuthRequest;

// Max accepted GSS/SSPI token (and ordinary password packet) length.
pub const PG_MAX_AUTH_TOKEN_LENGTH: i32 = 65535;

// GUCs (process-global, deferred): pg_krb_server_keyfile, pg_krb_caseins_users,
// pg_gss_accept_delegation.

pub fn ClientAuthentication(port: &mut Port) {
    unimplemented!()
}
pub fn sendAuthRequest(port: &mut Port, areq: AuthRequest, extradata: &[u8]) {
    unimplemented!()
}
pub fn set_authn_id(port: &mut Port, id: &str) {
    unimplemented!()
}

// Hook for plugins to get control in ClientAuthentication().
// fn-pointer hook -> a fn-pointer type alias; the global `ClientAuthentication_hook`
// is process-global mutable state (deferred). The 2nd arg is the auth status code.
pub type ClientAuthentication_hook_type = fn(&mut Port, i32);

// Hook type for password manglers (LDAP). Owned String in/out (C took/returned char*).
pub type auth_password_hook_typ = fn(String) -> String;

// Global hook `ldap_password_hook` (default LDAP password mutator, overridable by a
// shared library) is process-global mutable state, deferred.

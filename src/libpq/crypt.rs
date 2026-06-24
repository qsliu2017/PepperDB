//! Translated from PostgreSQL src/include/libpq/crypt.h
//! Interface to libpq/crypt.c.

/// Valid password hashes may be very long; 512 bytes covers all practical use.
pub const MAX_ENCRYPTED_PASSWORD_LEN: usize = 512;

/// Enables deprecation warnings for MD5 passwords.
pub static mut MD5_PASSWORD_WARNINGS: bool = true;

/// Types of password hashes or secrets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PasswordType {
    PASSWORD_TYPE_PLAINTEXT = 0,
    PASSWORD_TYPE_MD5,
    PASSWORD_TYPE_SCRAM_SHA_256,
}

pub fn get_password_type(shadow_pass: &str) -> PasswordType {
    let _ = shadow_pass;
    unimplemented!()
}

pub fn encrypt_password(target_type: PasswordType, role: &str, password: &str) -> String {
    let _ = (target_type, role, password);
    unimplemented!()
}

/// Fetch the stored password for `role`; None if no such role/password.
/// On error returns Err with a log-detail string.
pub fn get_role_password(role: &str) -> Result<Option<String>, String> {
    let _ = role;
    unimplemented!()
}

// The C functions return a STATUS_OK/STATUS_ERROR int plus a `**logdetail`
// out-param; both fold into Result, with logdetail as the Err payload.

pub fn md5_crypt_verify(
    role: &str,
    shadow_pass: &str,
    client_pass: &str,
    md5_salt: &[u8],
) -> Result<(), Option<String>> {
    let _ = (role, shadow_pass, client_pass, md5_salt);
    unimplemented!()
}

pub fn plain_crypt_verify(role: &str, shadow_pass: &str, client_pass: &str) -> Result<(), Option<String>> {
    let _ = (role, shadow_pass, client_pass);
    unimplemented!()
}

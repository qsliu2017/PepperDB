//! libpq/crypt.c - functions for dealing with encrypted passwords stored in
//! pg_authid.rolpassword.

use crate::prelude::*;

use crate::common::cryptohash::pg_cryptohash_type;
use crate::common::md5::{MD5_PASSWD_CHARSET, MD5_PASSWD_LEN};
use crate::common::scram_common::SCRAM_MAX_KEY_LEN;
use crate::libpq::scram::{
    parse_scram_secret, pg_be_scram_build_secret, scram_verify_plain_password,
};
use crate::miscadmin::TimestampTz;
use crate::postgres::{Datum, PointerGetDatum};
use crate::utils::activity::pgstat::GetCurrentTimestamp;
use crate::utils::builtins::TextDatumGetCString;
use crate::common::md5_common::pg_md5_encrypt;

// libc string routines used verbatim from the C source.
extern "C" {
    fn strlen(s: *const c_char) -> Size;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: Size) -> c_int;
    fn strspn(s: *const c_char, accept: *const c_char) -> Size;
}

// ---------------------------------------------------------------------------
// crypt.h definitions (not separately ported here).
// ---------------------------------------------------------------------------

// #define MAX_ENCRYPTED_PASSWORD_LEN (512)
pub const MAX_ENCRYPTED_PASSWORD_LEN: c_int = 512;

/*
 * Types of password hashes or secrets.
 *
 * Plaintext passwords can be passed in by the user, in a CREATE/ALTER USER
 * command. They will be encrypted to MD5 or SCRAM-SHA-256 format, before
 * storing on-disk, so only MD5 and SCRAM-SHA-256 passwords should appear in
 * pg_authid.rolpassword. They are also the allowed values for the
 * password_encryption GUC.
 */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum PasswordType {
    PASSWORD_TYPE_PLAINTEXT = 0,
    PASSWORD_TYPE_MD5,
    PASSWORD_TYPE_SCRAM_SHA_256,
}
pub use PasswordType::*;

// ---------------------------------------------------------------------------
// Locally-stubbed dependencies (syscache / catalog / string helpers not yet
// ported). TODO(pg-port): replace with real ports.
// ---------------------------------------------------------------------------

// utils/syscache.h - SysCacheIdentifier value (AUTHNAME) and helpers.
// TODO(pg-port): real syscache infrastructure (utils/cache/syscache.c).
#[allow(non_upper_case_globals)]
const AUTHNAME: c_int = 0;

// catalog/pg_authid.h attribute numbers.
// TODO(pg-port): generated from pg_authid catalog.
#[allow(non_upper_case_globals)]
const Anum_pg_authid_rolpassword: c_int = 0;
#[allow(non_upper_case_globals)]
const Anum_pg_authid_rolvaliduntil: c_int = 0;

use crate::access::htup_details::{HeapTuple, HeapTupleIsValid};

// utils/syscache.h - SearchSysCache1 / SysCacheGetAttr / ReleaseSysCache.
// TODO(pg-port): real syscache routines (utils/cache/syscache.c).
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    null_mut()
}
unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    isnull: *mut bool,
) -> Datum {
    *isnull = true;
    0
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {}

// utils/timestamp.h - DatumGetTimestampTz(X).
// TODO(pg-port): real timestamp adt (utils/adt/timestamp.c).
#[inline]
unsafe fn DatumGetTimestampTz(X: Datum) -> TimestampTz {
    X as TimestampTz
}

// utils/palloc.h - psprintf is variadic; until the real port lands we provide
// the single-/double-argument string forms used here.
// TODO(pg-port): real psprintf (utils/mmgr/mcxt.c).
unsafe fn psprintf_s(_fmt: *const c_char, _arg: *const c_char) -> *mut c_char {
    null_mut()
}

// NLS marker; in the C source this is the gettext "_()" macro.
#[inline]
unsafe fn gettext_(s: *const c_char) -> *const c_char {
    s
}

// Materialize a C string into a Rust String for printf-style elog/ereport args.
unsafe fn cstr(s: *const c_char) -> std::string::String {
    if s.is_null() {
        return std::string::String::new();
    }
    std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

/// Enables deprecation warnings for MD5 passwords.
#[no_mangle]
pub static mut md5_password_warnings: bool = true;

/*
 * Fetch stored password for a user, for authentication.
 *
 * On error, returns NULL, and stores a palloc'd string describing the reason,
 * for the postmaster log, in *logdetail.  The error reason should *not* be
 * sent to the client, to avoid giving away user information!
 */
pub unsafe fn get_role_password(
    role: *const c_char,
    logdetail: *mut *const c_char,
) -> *mut c_char {
    let mut vuntil: TimestampTz = 0;
    let roleTup: HeapTuple;
    let mut datum: Datum;
    let mut isnull: bool = false;
    let shadow_pass: *mut c_char;

    /* Get role info from pg_authid */
    roleTup = SearchSysCache1(AUTHNAME, PointerGetDatum(role as *const c_void));
    if !HeapTupleIsValid(roleTup) {
        *logdetail = psprintf_s(
            gettext_(c"Role \"%s\" does not exist.".as_ptr()),
            role,
        );
        return null_mut(); /* no such user */
    }

    datum = SysCacheGetAttr(AUTHNAME, roleTup, Anum_pg_authid_rolpassword, &mut isnull);
    if isnull {
        ReleaseSysCache(roleTup);
        *logdetail = psprintf_s(
            gettext_(c"User \"%s\" has no password assigned.".as_ptr()),
            role,
        );
        return null_mut(); /* user has no password */
    }
    shadow_pass = TextDatumGetCString(datum);

    datum = SysCacheGetAttr(AUTHNAME, roleTup, Anum_pg_authid_rolvaliduntil, &mut isnull);
    if !isnull {
        vuntil = DatumGetTimestampTz(datum);
    }

    ReleaseSysCache(roleTup);

    /*
     * Password OK, but check to be sure we are not past rolvaliduntil
     */
    if !isnull && vuntil < GetCurrentTimestamp() {
        *logdetail = psprintf_s(
            gettext_(c"User \"%s\" has an expired password.".as_ptr()),
            role,
        );
        return null_mut();
    }

    shadow_pass
}

/*
 * What kind of a password type is 'shadow_pass'?
 */
pub unsafe fn get_password_type(shadow_pass: *const c_char) -> PasswordType {
    let mut encoded_salt: *mut c_char = null_mut();
    let mut iterations: c_int = 0;
    let mut key_length: c_int = 0;
    let mut hash_type: pg_cryptohash_type = pg_cryptohash_type::PG_MD5;
    let mut stored_key: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let mut server_key: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];

    if strncmp(shadow_pass, c"md5".as_ptr(), 3) == 0
        && strlen(shadow_pass) == MD5_PASSWD_LEN
        && strspn(shadow_pass.add(3), MD5_PASSWD_CHARSET.as_ptr() as *const c_char)
            == MD5_PASSWD_LEN - 3
    {
        return PASSWORD_TYPE_MD5;
    }
    if parse_scram_secret(
        shadow_pass,
        &mut iterations,
        &mut hash_type,
        &mut key_length,
        &mut encoded_salt,
        stored_key.as_mut_ptr(),
        server_key.as_mut_ptr(),
    ) {
        return PASSWORD_TYPE_SCRAM_SHA_256;
    }
    PASSWORD_TYPE_PLAINTEXT
}

/*
 * Given a user-supplied password, convert it into a secret of
 * 'target_type' kind.
 *
 * If the password is already in encrypted form, we cannot reverse the
 * hash, so it is stored as it is regardless of the requested type.
 */
pub unsafe fn encrypt_password(
    target_type: PasswordType,
    role: *const c_char,
    password: *const c_char,
) -> *mut c_char {
    let guessed_type = get_password_type(password);
    let mut encrypted_password: *mut c_char = null_mut();
    let mut errstr: *const c_char = null_mut();

    if guessed_type != PASSWORD_TYPE_PLAINTEXT {
        /*
         * Cannot convert an already-encrypted password from one format to
         * another, so return it as it is.
         */
        encrypted_password = pstrdup(password);
    } else {
        match target_type {
            PASSWORD_TYPE_MD5 => {
                encrypted_password = palloc((MD5_PASSWD_LEN + 1) as Size) as *mut c_char;

                if !pg_md5_encrypt(
                    password,
                    role as *const uint8,
                    strlen(role),
                    encrypted_password,
                    &mut errstr,
                ) {
                    elog!(ERROR, "password encryption failed: {}", cstr(errstr));
                }
            }

            PASSWORD_TYPE_SCRAM_SHA_256 => {
                encrypted_password = pg_be_scram_build_secret(password);
            }

            PASSWORD_TYPE_PLAINTEXT => {
                elog!(ERROR, "cannot encrypt password with 'plaintext'");
            }
        }
    }

    Assert!(!encrypted_password.is_null());

    /*
     * Valid password hashes may be very long, but we don't want to store
     * anything that might need out-of-line storage, since de-TOASTing won't
     * work during authentication because we haven't selected a database yet
     * and cannot read pg_class. 512 bytes should be more than enough for all
     * practical use, so fail for anything longer.
     */
    if !encrypted_password.is_null() /* keep compiler quiet */
        && strlen(encrypted_password) > MAX_ENCRYPTED_PASSWORD_LEN as Size
    {
        /*
         * We don't expect any of our own hashing routines to produce hashes
         * that are too long.
         */
        Assert!(guessed_type != PASSWORD_TYPE_PLAINTEXT);

        elog!(
            ERROR,
            "encrypted password is too long; encrypted passwords must be no longer than {} bytes.",
            MAX_ENCRYPTED_PASSWORD_LEN
        );
    }

    if md5_password_warnings
        && get_password_type(encrypted_password) == PASSWORD_TYPE_MD5
    {
        ereport!(WARNING, "setting an MD5-encrypted password");
    }

    encrypted_password
}

/*
 * Check MD5 authentication response, and return STATUS_OK or STATUS_ERROR.
 *
 * 'shadow_pass' is the user's correct password or password hash, as stored
 * in pg_authid.rolpassword.
 * 'client_pass' is the response given by the remote user to the MD5 challenge.
 * 'md5_salt' is the salt used in the MD5 authentication challenge.
 *
 * In the error case, save a string at *logdetail that will be sent to the
 * postmaster log (but not the client).
 */
pub unsafe fn md5_crypt_verify(
    role: *const c_char,
    shadow_pass: *const c_char,
    client_pass: *const c_char,
    md5_salt: *const uint8,
    md5_salt_len: c_int,
    logdetail: *mut *const c_char,
) -> c_int {
    let retval: c_int;
    let mut crypt_pwd: [c_char; MD5_PASSWD_LEN + 1] = [0; MD5_PASSWD_LEN + 1];
    let mut errstr: *const c_char = null_mut();

    Assert!(md5_salt_len > 0);

    if get_password_type(shadow_pass) != PASSWORD_TYPE_MD5 {
        /* incompatible password hash format. */
        *logdetail = psprintf_s(
            gettext_(
                c"User \"%s\" has a password that cannot be used with MD5 authentication."
                    .as_ptr(),
            ),
            role,
        );
        return STATUS_ERROR;
    }

    /*
     * Compute the correct answer for the MD5 challenge.
     */
    /* stored password already encrypted, only do salt */
    if !pg_md5_encrypt(
        shadow_pass.add(strlen(c"md5".as_ptr())),
        md5_salt,
        md5_salt_len as Size,
        crypt_pwd.as_mut_ptr(),
        &mut errstr,
    ) {
        *logdetail = errstr;
        return STATUS_ERROR;
    }

    if strcmp(client_pass, crypt_pwd.as_ptr()) == 0 {
        retval = STATUS_OK;
    } else {
        *logdetail = psprintf_s(
            gettext_(c"Password does not match for user \"%s\".".as_ptr()),
            role,
        );
        retval = STATUS_ERROR;
    }

    retval
}

/*
 * Check given password for given user, and return STATUS_OK or STATUS_ERROR.
 *
 * 'shadow_pass' is the user's correct password hash, as stored in
 * pg_authid.rolpassword.
 * 'client_pass' is the password given by the remote user.
 *
 * In the error case, store a string at *logdetail that will be sent to the
 * postmaster log (but not the client).
 */
pub unsafe fn plain_crypt_verify(
    role: *const c_char,
    shadow_pass: *const c_char,
    client_pass: *const c_char,
    logdetail: *mut *const c_char,
) -> c_int {
    let mut crypt_client_pass: [c_char; MD5_PASSWD_LEN + 1] = [0; MD5_PASSWD_LEN + 1];
    let mut errstr: *const c_char = null_mut();

    /*
     * Client sent password in plaintext.  If we have an MD5 hash stored, hash
     * the password the client sent, and compare the hashes.  Otherwise
     * compare the plaintext passwords directly.
     */
    match get_password_type(shadow_pass) {
        PASSWORD_TYPE_SCRAM_SHA_256 => {
            if scram_verify_plain_password(role, client_pass, shadow_pass) {
                return STATUS_OK;
            } else {
                *logdetail = psprintf_s(
                    gettext_(c"Password does not match for user \"%s\".".as_ptr()),
                    role,
                );
                return STATUS_ERROR;
            }
        }

        PASSWORD_TYPE_MD5 => {
            if !pg_md5_encrypt(
                client_pass,
                role as *const uint8,
                strlen(role),
                crypt_client_pass.as_mut_ptr(),
                &mut errstr,
            ) {
                *logdetail = errstr;
                return STATUS_ERROR;
            }
            if strcmp(crypt_client_pass.as_ptr(), shadow_pass) == 0 {
                return STATUS_OK;
            } else {
                *logdetail = psprintf_s(
                    gettext_(c"Password does not match for user \"%s\".".as_ptr()),
                    role,
                );
                return STATUS_ERROR;
            }
        }

        PASSWORD_TYPE_PLAINTEXT => {
            /*
             * We never store passwords in plaintext, so this shouldn't
             * happen.
             */
        }
    }

    /*
     * This shouldn't happen.  Plain "password" authentication is possible
     * with any kind of stored password hash.
     */
    *logdetail = psprintf_s(
        gettext_(c"Password of user \"%s\" is in unrecognized format.".as_ptr()),
        role,
    );
    STATUS_ERROR
}

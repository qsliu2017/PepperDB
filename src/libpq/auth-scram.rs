/*-------------------------------------------------------------------------
 *
 * auth-scram.c
 *    Server-side implementation of the SASL SCRAM-SHA-256 mechanism.
 *
 * See the following RFCs for more details:
 * - RFC 5802: https://tools.ietf.org/html/rfc5802
 * - RFC 5803: https://tools.ietf.org/html/rfc5803
 * - RFC 7677: https://tools.ietf.org/html/rfc7677
 *
 * Here are some differences:
 *
 * - Username from the authentication exchange is not used. The client
 *   should send an empty string as the username.
 *
 * - If the password isn't valid UTF-8, or contains characters prohibited
 *   by the SASLprep profile, we skip the SASLprep pre-processing and use
 *   the raw bytes in calculating the hash.
 *
 * - If channel binding is used, the channel binding type is always
 *   "tls-server-end-point".  The spec says the default is "tls-unique"
 *   (RFC 5802, section 6.1. Default Channel Binding), but there are some
 *   problems with that.  Firstly, not all SSL libraries provide an API to
 *   get the TLS Finished message, required to use "tls-unique".  Secondly,
 *   "tls-unique" is not specified for TLS v1.3, and as of this writing,
 *   it's not clear if there will be a replacement.  We could support both
 *   "tls-server-end-point" and "tls-unique", but for our use case,
 *   "tls-unique" doesn't really have any advantages.  The main advantage
 *   of "tls-unique" would be that it works even if the server doesn't
 *   have a certificate, but PostgreSQL requires a server certificate
 *   whenever SSL is used, anyway.
 *
 *
 * The password stored in pg_authid consists of the iteration count, salt,
 * StoredKey and ServerKey.
 *
 * SASLprep usage
 * --------------
 *
 * One notable difference to the SCRAM specification is that while the
 * specification dictates that the password is in UTF-8, and prohibits
 * certain characters, we are more lenient.  If the password isn't a valid
 * UTF-8 string, or contains prohibited characters, the raw bytes are used
 * to calculate the hash instead, without SASLprep processing.  This is
 * because PostgreSQL supports other encodings too, and the encoding being
 * used during authentication is undefined (client_encoding isn't set until
 * after authentication).  In effect, we try to interpret the password as
 * UTF-8 and apply SASLprep processing, but if it looks invalid, we assume
 * that it's in some other encoding.
 *
 * In the worst case, we misinterpret a password that's in a different
 * encoding as being Unicode, because it happens to consists entirely of
 * valid UTF-8 bytes, and we apply Unicode normalization to it.  As long
 * as we do that consistently, that will not lead to failed logins.
 * Fortunately, the UTF-8 byte sequences that are ignored by SASLprep
 * don't correspond to any commonly used characters in any of the other
 * supported encodings, so it should not lead to any significant loss in
 * entropy, even if the normalization is incorrectly applied to a
 * non-UTF-8 password.
 *
 * Error handling
 * --------------
 *
 * Don't reveal user information to an unauthenticated client.  We don't
 * want an attacker to be able to probe whether a particular username is
 * valid.  In SCRAM, the server has to read the salt and iteration count
 * from the user's stored secret, and send it to the client.  To avoid
 * revealing whether a user exists, when the client tries to authenticate
 * with a username that doesn't exist, or doesn't have a valid SCRAM
 * secret in pg_authid, we create a fake salt and iteration count
 * on-the-fly, and proceed with the authentication with that.  In the end,
 * we'll reject the attempt, as if an incorrect password was given.  When
 * we are performing a "mock" authentication, the 'doomed' flag in
 * scram_state is set.
 *
 * In the error messages, avoid printing strings from the client, unless
 * you check that they are pure ASCII.  We don't want an unauthenticated
 * attacker to be able to spam the logs with characters that are not valid
 * to the encoding being used, whatever that is.  We cannot avoid that in
 * general, after logging in, but let's do what we can here.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/libpq/auth-scram.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::missing_safety_doc)]

use crate::prelude::*;

use crate::common::base64::{
    pg_b64_dec_len, pg_b64_decode, pg_b64_enc_len, pg_b64_encode,
};
use crate::common::cryptohash::{
    pg_cryptohash_create, pg_cryptohash_ctx, pg_cryptohash_final, pg_cryptohash_free,
    pg_cryptohash_init, pg_cryptohash_type, pg_cryptohash_update,
};
use crate::common::cryptohash::pg_cryptohash_type::*;
use crate::common::hmac::{
    pg_hmac_create, pg_hmac_ctx, pg_hmac_error, pg_hmac_final, pg_hmac_free, pg_hmac_init,
    pg_hmac_update,
};
use crate::common::saslprep::{pg_saslprep, pg_saslprep_rc, SASLPREP_SUCCESS};
use crate::common::scram_common::{
    scram_H, scram_SaltedPassword, scram_ServerKey, scram_build_secret, SCRAM_DEFAULT_SALT_LEN,
    SCRAM_MAX_KEY_LEN, SCRAM_RAW_NONCE_LEN, SCRAM_SHA_256_DEFAULT_ITERATIONS,
    SCRAM_SHA_256_KEY_LEN, SCRAM_SHA_256_NAME, SCRAM_SHA_256_PLUS_NAME,
};
use crate::common::sha2::PG_SHA256_DIGEST_LENGTH;
use crate::catalog::pg_control::MOCK_AUTH_NONCE_LEN;
use crate::lib::stringinfo::{appendStringInfoChar, appendStringInfoString, StringInfo};
use crate::libpq::crypt::{get_password_type, PasswordType::*};
use crate::libpq::libpq_be::{be_tls_get_certificate_hash, Port};
use crate::libpq::sasl::{
    pg_be_sasl_mech, PG_MAX_SASL_MESSAGE_LENGTH, PG_SASL_EXCHANGE_CONTINUE,
    PG_SASL_EXCHANGE_FAILURE, PG_SASL_EXCHANGE_SUCCESS,
};
use crate::port::pg_strong_random::pg_strong_random;

// ---------------------------------------------------------------------------
// Stub error-code constants (not yet in a shared errcodes module).
// TODO(pg-port): real constants live in utils/errcodes.h -> a generated module.
// ---------------------------------------------------------------------------
const ERRCODE_PROTOCOL_VIOLATION: c_int = 0;
const ERRCODE_INTERNAL_ERROR: c_int = 0;
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
const ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION: c_int = 0;

// ---------------------------------------------------------------------------
// Unported symbol stubs
// ---------------------------------------------------------------------------

// TODO(pg-port): real GetMockAuthenticationNonce lives in access/xlog.c
//   (declared in src/include/access/xlog.h).
unsafe fn GetMockAuthenticationNonce() -> *mut c_char { crate::access::transam::xlog::GetMockAuthenticationNonce() }

// ---------------------------------------------------------------------------
// Minimal libc-style helpers (mirrors pattern in scram_common.rs / base64.rs)
// ---------------------------------------------------------------------------

#[inline]
unsafe fn strlen(s: *const c_char) -> Size {
    let mut n: Size = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

#[inline]
unsafe fn strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut i: usize = 0;
    loop {
        let ca = *a.add(i) as u8;
        let cb = *b.add(i) as u8;
        if ca != cb {
            return ca as c_int - cb as c_int;
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

#[inline]
unsafe fn memcmp(a: *const c_void, b: *const c_void, n: Size) -> c_int {
    let a = a as *const u8;
    let b = b as *const u8;
    for i in 0..n {
        let ca = *a.add(i);
        let cb = *b.add(i);
        if ca != cb {
            return ca as c_int - cb as c_int;
        }
    }
    0
}

#[inline]
unsafe fn memcpy(dst: *mut c_void, src: *const c_void, n: Size) {
    core::ptr::copy_nonoverlapping(src as *const u8, dst as *mut u8, n);
}

#[inline]
unsafe fn memset(dst: *mut c_void, val: c_int, n: Size) {
    core::ptr::write_bytes(dst as *mut u8, val as u8, n);
}

/*
 * strsep - split *stringp at the first byte matching any byte in delim.
 * NUL-terminates the token and advances *stringp.
 */
#[inline]
unsafe fn strsep(stringp: *mut *mut c_char, delim: *const c_char) -> *mut c_char {
    if (*stringp).is_null() {
        return null_mut();
    }
    let begin: *mut c_char = *stringp;
    let mut p: *mut c_char = begin;
    'outer: loop {
        if *p == 0 {
            *stringp = null_mut();
            break 'outer;
        }
        let mut d: *const c_char = delim;
        while *d != 0 {
            if *p == *d {
                *p = 0;
                *stringp = p.add(1);
                break 'outer;
            }
            d = d.add(1);
        }
        p = p.add(1);
    }
    begin
}

/* strtol - decimal only, enough for parse_scram_secret */
#[inline]
unsafe fn strtol(s: *const c_char, endptr: *mut *mut c_char, _base: c_int) -> c_long {
    let mut p: *const c_char = s;
    let mut val: c_long = 0;
    let mut got_digit = false;
    while *p == b' ' as c_char || *p == b'\t' as c_char {
        p = p.add(1);
    }
    let sign: c_long = if *p == b'-' as c_char {
        p = p.add(1);
        -1
    } else {
        if *p == b'+' as c_char {
            p = p.add(1);
        }
        1
    };
    while *p >= b'0' as c_char && *p <= b'9' as c_char {
        val = val * 10 + (*p as u8 - b'0') as c_long;
        got_digit = true;
        p = p.add(1);
    }
    if !endptr.is_null() {
        *endptr = if got_digit { p as *mut c_char } else { s as *mut c_char };
    }
    val * sign
}

/*
 * Render a NUL-terminated C string for use in format!/{} placeholders.
 * NULL renders as "".
 */
#[inline]
unsafe fn cstr_to_str(s: *const c_char) -> &'static str {
    if s.is_null() {
        return "";
    }
    let len = strlen(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    core::str::from_utf8_unchecked(bytes)
}

// ---------------------------------------------------------------------------
// Private psprintf shims (replace C variadics with concrete helpers)
// ---------------------------------------------------------------------------

/* "User \"<name>\" does not have a valid SCRAM secret." */
unsafe fn psprintf_user_no_scram(user_name: *const c_char) -> *mut c_char {
    let msg = format!(
        "User \"{}\" does not have a valid SCRAM secret.",
        cstr_to_str(user_name)
    );
    let bytes = msg.as_bytes();
    let buf: *mut c_char = palloc(bytes.len() + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, bytes.len());
    *buf.add(bytes.len()) = 0;
    buf
}

/* "r=<cn><sn>,s=<salt>,i=<iters>" */
unsafe fn psprintf_server_first(
    client_nonce: *const c_char,
    server_nonce: *const c_char,
    salt: *const c_char,
    iterations: c_int,
) -> *mut c_char {
    let msg = format!(
        "r={}{},s={},i={}",
        cstr_to_str(client_nonce),
        cstr_to_str(server_nonce),
        cstr_to_str(salt),
        iterations
    );
    let bytes = msg.as_bytes();
    let buf: *mut c_char = palloc(bytes.len() + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, bytes.len());
    *buf.add(bytes.len()) = 0;
    buf
}

/* "v=<base64>" */
unsafe fn psprintf_v(sig_b64: *const c_char) -> *mut c_char {
    let msg = format!("v={}", cstr_to_str(sig_b64));
    let bytes = msg.as_bytes();
    let buf: *mut c_char = palloc(bytes.len() + 1) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, bytes.len());
    *buf.add(bytes.len()) = 0;
    buf
}

// ---------------------------------------------------------------------------
// Mechanism declaration (merged from libpq/scram.h)
// ---------------------------------------------------------------------------

/// Number of iterations to use when generating new secrets.
pub static mut scram_sha_256_iterations: c_int = SCRAM_SHA_256_DEFAULT_ITERATIONS;

/// SASL mechanism callbacks for SCRAM-SHA-256.
pub static pg_be_scram_mech: pg_be_sasl_mech = pg_be_sasl_mech {
    get_mechanisms: Some(unsafe { core::mem::transmute(scram_get_mechanisms as unsafe extern "C" fn(_, _)) }),
    init: Some(unsafe { core::mem::transmute(scram_init as unsafe extern "C" fn(_, _, _) -> _) }),
    exchange: Some(scram_exchange),
    max_message_length: PG_MAX_SASL_MESSAGE_LENGTH,
};

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/*
 * Status data for a SCRAM authentication exchange.  This should be kept
 * internal to this file.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum scram_state_enum {
    SCRAM_AUTH_INIT,
    SCRAM_AUTH_SALT_SENT,
    SCRAM_AUTH_FINISHED,
}
use scram_state_enum::*;

#[repr(C)]
struct scram_state {
    state: scram_state_enum,

    username: *const c_char, /* username from startup packet */

    port: *mut Port,
    channel_binding_in_use: bool,

    /* State data depending on the hash type */
    hash_type: pg_cryptohash_type,
    key_length: c_int,

    iterations: c_int,
    salt: *mut c_char,                     /* base64-encoded */
    ClientKey: [uint8; SCRAM_MAX_KEY_LEN],
    StoredKey: [uint8; SCRAM_MAX_KEY_LEN],
    ServerKey: [uint8; SCRAM_MAX_KEY_LEN],

    /* Fields of the first message from client */
    cbind_flag: c_char,
    client_first_message_bare: *mut c_char,
    client_username: *mut c_char,
    client_nonce: *mut c_char,

    /* Fields from the last message from client */
    client_final_message_without_proof: *mut c_char,
    client_final_nonce: *mut c_char,
    ClientProof: [uint8; SCRAM_MAX_KEY_LEN],

    /* Fields generated in the server */
    server_first_message: *mut c_char,
    server_nonce: *mut c_char,

    /*
     * If something goes wrong during the authentication, or we are performing
     * a "mock" authentication (see comments at top of file), the 'doomed'
     * flag is set.  A reason for the failure, for the server log, is put in
     * 'logdetail'.
     */
    doomed: bool,
    logdetail: *mut c_char,
}

// ---------------------------------------------------------------------------
// Mechanism callbacks
// ---------------------------------------------------------------------------

/*
 * Get a list of SASL mechanisms that this module supports.
 *
 * For the convenience of building the FE/BE packet that lists the
 * mechanisms, the names are appended to the given StringInfo buffer,
 * separated by '\0' bytes.
 */
unsafe extern "C" fn scram_get_mechanisms(port: *mut Port, buf: StringInfo) {
    /*
     * Advertise the mechanisms in decreasing order of importance.  So the
     * channel-binding variants go first, if they are supported.  Channel
     * binding is only supported with SSL.
     */
    // #ifdef USE_SSL
    if (*port).ssl_in_use {
        appendStringInfoString(buf, SCRAM_SHA_256_PLUS_NAME.as_ptr() as *const c_char);
        appendStringInfoChar(buf, b'\0' as c_char);
    }
    // #endif
    appendStringInfoString(buf, SCRAM_SHA_256_NAME.as_ptr() as *const c_char);
    appendStringInfoChar(buf, b'\0' as c_char);
}

/*
 * Initialize a new SCRAM authentication exchange status tracker.  This
 * needs to be called before doing any exchange.  It will be filled later
 * after the beginning of the exchange with authentication information.
 *
 * 'selected_mech' identifies the SASL mechanism that the client selected.
 * It should be one of the mechanisms that we support, as returned by
 * scram_get_mechanisms().
 *
 * 'shadow_pass' is the role's stored secret, from pg_authid.rolpassword.
 * The username was provided by the client in the startup message, and is
 * available in port->user_name.  If 'shadow_pass' is NULL, we still perform
 * an authentication exchange, but it will fail, as if an incorrect password
 * was given.
 */
unsafe extern "C" fn scram_init(
    port: *mut Port,
    selected_mech: *const c_char,
    shadow_pass: *const c_char,
) -> *mut c_void {
    let state: *mut scram_state;
    let got_secret: bool;

    state = palloc0(core::mem::size_of::<scram_state>()) as *mut scram_state;
    (*state).port = port;
    (*state).state = SCRAM_AUTH_INIT;

    /*
     * Parse the selected mechanism.
     *
     * Note that if we don't support channel binding, or if we're not using
     * SSL at all, we would not have advertised the PLUS variant in the first
     * place.  If the client nevertheless tries to select it, it's a protocol
     * violation like selecting any other SASL mechanism we don't support.
     */
    // #ifdef USE_SSL
    if strcmp(selected_mech, SCRAM_SHA_256_PLUS_NAME.as_ptr() as *const c_char) == 0
        && (*port).ssl_in_use
    {
        (*state).channel_binding_in_use = true;
    } else
    // #endif
    if strcmp(selected_mech, SCRAM_SHA_256_NAME.as_ptr() as *const c_char) == 0 {
        (*state).channel_binding_in_use = false;
    } else {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            ERROR,
            errmsg!("client selected an invalid SASL authentication mechanism")
        );
    }

    /*
     * Parse the stored secret.
     */
    if !shadow_pass.is_null() {
        let password_type = get_password_type(shadow_pass);

        if password_type == PASSWORD_TYPE_SCRAM_SHA_256 {
            if parse_scram_secret(
                shadow_pass,
                &mut (*state).iterations,
                &mut (*state).hash_type,
                &mut (*state).key_length,
                &mut (*state).salt,
                (*state).StoredKey.as_mut_ptr(),
                (*state).ServerKey.as_mut_ptr(),
            ) {
                got_secret = true;
            } else {
                /*
                 * The password looked like a SCRAM secret, but could not be
                 * parsed.
                 */
                ereport!(
                    LOG,
                    errmsg!(
                        "invalid SCRAM secret for user \"{}\"",
                        cstr_to_str((*(*state).port).user_name)
                    )
                );
                got_secret = false;
            }
        } else {
            /*
             * The user doesn't have SCRAM secret. (You cannot do SCRAM
             * authentication with an MD5 hash.)
             */
            (*state).logdetail = psprintf_user_no_scram((*(*state).port).user_name);
            got_secret = false;
        }
    } else {
        /*
         * The caller requested us to perform a dummy authentication.  This is
         * considered normal, since the caller requested it, so don't set log
         * detail.
         */
        got_secret = false;
    }

    /*
     * If the user did not have a valid SCRAM secret, we still go through the
     * motions with a mock one, and fail as if the client supplied an
     * incorrect password.  This is to avoid revealing information to an
     * attacker.
     */
    if !got_secret {
        mock_scram_secret(
            (*(*state).port).user_name,
            &mut (*state).hash_type,
            &mut (*state).iterations,
            &mut (*state).key_length,
            &mut (*state).salt,
            (*state).StoredKey.as_mut_ptr(),
            (*state).ServerKey.as_mut_ptr(),
        );
        (*state).doomed = true;
    }

    state as *mut c_void
}

/*
 * Continue a SCRAM authentication exchange.
 *
 * 'input' is the SCRAM payload sent by the client.  On the first call,
 * 'input' contains the "Initial Client Response" that the client sent as
 * part of the SASLInitialResponse message, or NULL if no Initial Client
 * Response was given.  (The SASL specification distinguishes between an
 * empty response and non-existing one.)  On subsequent calls, 'input'
 * cannot be NULL.  For convenience in this function, the caller must
 * ensure that there is a null terminator at input[inputlen].
 *
 * The next message to send to client is saved in 'output', for a length
 * of 'outputlen'.  In the case of an error, optionally store a palloc'd
 * string at *logdetail that will be sent to the postmaster log (but not
 * the client).
 */
unsafe extern "C" fn scram_exchange(
    opaq: *mut c_void,
    input: *const c_char,
    inputlen: c_int,
    output: *mut *mut c_char,
    outputlen: *mut c_int,
    logdetail: *mut *const c_char,
) -> c_int {
    let state: *mut scram_state = opaq as *mut scram_state;
    let result: c_int;

    *output = null_mut();

    /*
     * If the client didn't include an "Initial Client Response" in the
     * SASLInitialResponse message, send an empty challenge, to which the
     * client will respond with the same data that usually comes in the
     * Initial Client Response.
     */
    if input.is_null() {
        Assert!((*state).state == SCRAM_AUTH_INIT);

        *output = pstrdup(b"\0".as_ptr() as *const c_char);
        *outputlen = 0;
        return PG_SASL_EXCHANGE_CONTINUE;
    }

    /*
     * Check that the input length agrees with the string length of the input.
     * We can ignore inputlen after this.
     */
    if inputlen == 0 {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            ERROR,
            errmsg!("malformed SCRAM message")
        );
    }
    if inputlen != strlen(input) as c_int {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            ERROR,
            errmsg!("malformed SCRAM message")
        );
    }

    match (*state).state {
        SCRAM_AUTH_INIT => {
            /*
             * Initialization phase.  Receive the first message from client
             * and be sure that it parsed correctly.  Then send the challenge
             * to the client.
             */
            read_client_first_message(state, input);

            /* prepare message to send challenge */
            *output = build_server_first_message(state);

            (*state).state = SCRAM_AUTH_SALT_SENT;
            result = PG_SASL_EXCHANGE_CONTINUE;
        }

        SCRAM_AUTH_SALT_SENT => {
            /*
             * Final phase for the server.  Receive the response to the
             * challenge previously sent, verify, and let the client know that
             * everything went well (or not).
             */
            read_client_final_message(state, input);

            if !verify_final_nonce(state) {
                let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                ereport!(
                    ERROR,
                    errmsg!("invalid SCRAM response")
                );
            }

            /*
             * Now check the final nonce and the client proof.
             *
             * If we performed a "mock" authentication that we knew would fail
             * from the get go, this is where we fail.
             *
             * The SCRAM specification includes an error code,
             * "invalid-proof", for authentication failure, but it also allows
             * erroring out in an application-specific way.  We choose to do
             * the latter, so that the error message for invalid password is
             * the same for all authentication methods.  The caller will call
             * ereport(), when we return PG_SASL_EXCHANGE_FAILURE with no
             * output.
             *
             * NB: the order of these checks is intentional.  We calculate the
             * client proof even in a mock authentication, even though it's
             * bound to fail, to thwart timing attacks to determine if a role
             * with the given name exists or not.
             */
            if !verify_client_proof(state) || (*state).doomed {
                result = PG_SASL_EXCHANGE_FAILURE;
            } else {
                /* Build final message for client */
                *output = build_server_final_message(state);

                /* Success! */
                result = PG_SASL_EXCHANGE_SUCCESS;
                (*state).state = SCRAM_AUTH_FINISHED;
            }
        }

        _ => {
            elog!(ERROR, "invalid SCRAM exchange state");
            result = PG_SASL_EXCHANGE_FAILURE;
        }
    }

    if result == PG_SASL_EXCHANGE_FAILURE
        && !(*state).logdetail.is_null()
        && !logdetail.is_null()
    {
        *logdetail = (*state).logdetail;
    }

    if !(*output).is_null() {
        *outputlen = strlen(*output) as c_int;
    }

    if result == PG_SASL_EXCHANGE_SUCCESS && (*state).state == SCRAM_AUTH_FINISHED {
        // MyProcPort is declared as `extern "C"` in crate::miscadmin.
        use crate::miscadmin::MyProcPort;
        let proc_port: *mut Port = MyProcPort;
        memcpy(
            (*proc_port).scram_ClientKey.as_mut_ptr() as *mut c_void,
            (*state).ClientKey.as_ptr() as *const c_void,
            core::mem::size_of_val(&(*proc_port).scram_ClientKey),
        );
        memcpy(
            (*proc_port).scram_ServerKey.as_mut_ptr() as *mut c_void,
            (*state).ServerKey.as_ptr() as *const c_void,
            core::mem::size_of_val(&(*proc_port).scram_ServerKey),
        );
        (*proc_port).has_scram_keys = true;
    }

    result
}

// ---------------------------------------------------------------------------
// Public API (merged from libpq/scram.h)
// ---------------------------------------------------------------------------

/*
 * Construct a SCRAM secret, for storing in pg_authid.rolpassword.
 *
 * The result is palloc'd, so caller is responsible for freeing it.
 */
pub unsafe fn pg_be_scram_build_secret(password: *const c_char) -> *mut c_char {
    let mut prep_password: *mut c_char = null_mut();
    let rc: pg_saslprep_rc;
    let mut saltbuf: [uint8; SCRAM_DEFAULT_SALT_LEN as usize] =
        [0; SCRAM_DEFAULT_SALT_LEN as usize];
    let result: *mut c_char;
    let mut errstr: *const c_char = null();

    /*
     * Normalize the password with SASLprep.  If that doesn't work, because
     * the password isn't valid UTF-8 or contains prohibited characters, just
     * proceed with the original password.  (See comments at top of file.)
     */
    rc = pg_saslprep(password, &mut prep_password);
    let password: *const c_char = if rc == SASLPREP_SUCCESS {
        prep_password as *const c_char
    } else {
        password
    };

    /* Generate random salt */
    if !pg_strong_random(
        saltbuf.as_mut_ptr() as *mut c_void,
        SCRAM_DEFAULT_SALT_LEN as Size,
    ) {
        let _ = errcode(ERRCODE_INTERNAL_ERROR);
        ereport!(ERROR, errmsg!("could not generate random salt"));
    }

    result = scram_build_secret(
        PG_SHA256,
        SCRAM_SHA_256_KEY_LEN as c_int,
        saltbuf.as_ptr(),
        SCRAM_DEFAULT_SALT_LEN,
        scram_sha_256_iterations,
        password,
        &mut errstr,
    );

    if !prep_password.is_null() {
        pfree(prep_password as *mut c_void);
    }

    result
}

/*
 * Verify a plaintext password against a SCRAM secret.  This is used when
 * performing plaintext password authentication for a user that has a SCRAM
 * secret stored in pg_authid.
 */
pub unsafe fn scram_verify_plain_password(
    username: *const c_char,
    password: *const c_char,
    secret: *const c_char,
) -> bool {
    let mut encoded_salt: *mut c_char = null_mut();
    let salt: *mut uint8;
    let mut saltlen: c_int;
    let mut iterations: c_int = 0;
    let mut key_length: c_int = 0;
    let mut hash_type: pg_cryptohash_type = PG_SHA256;
    let mut salted_password: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let mut stored_key: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let mut server_key: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let mut computed_key: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let mut prep_password: *mut c_char = null_mut();
    let rc: pg_saslprep_rc;
    let mut errstr: *const c_char = null();

    if !parse_scram_secret(
        secret,
        &mut iterations,
        &mut hash_type,
        &mut key_length,
        &mut encoded_salt,
        stored_key.as_mut_ptr(),
        server_key.as_mut_ptr(),
    ) {
        /*
         * The password looked like a SCRAM secret, but could not be parsed.
         */
        ereport!(
            LOG,
            errmsg!("invalid SCRAM secret for user \"{}\"", cstr_to_str(username))
        );
        return false;
    }

    saltlen = pg_b64_dec_len(strlen(encoded_salt) as c_int);
    salt = palloc(saltlen as Size) as *mut uint8;
    saltlen = pg_b64_decode(encoded_salt, strlen(encoded_salt) as c_int, salt, saltlen);
    if saltlen < 0 {
        ereport!(
            LOG,
            errmsg!("invalid SCRAM secret for user \"{}\"", cstr_to_str(username))
        );
        return false;
    }

    /* Normalize the password */
    rc = pg_saslprep(password, &mut prep_password);
    let password: *const c_char = if rc == SASLPREP_SUCCESS {
        prep_password as *const c_char
    } else {
        password
    };

    /* Compute Server Key based on the user-supplied plaintext password */
    if scram_SaltedPassword(
        password,
        hash_type,
        key_length,
        salt,
        saltlen,
        iterations,
        salted_password.as_mut_ptr(),
        &mut errstr,
    ) < 0
        || scram_ServerKey(
            salted_password.as_ptr(),
            hash_type,
            key_length,
            computed_key.as_mut_ptr(),
            &mut errstr,
        ) < 0
    {
        elog!(
            ERROR,
            "could not compute server key: {}",
            cstr_to_str(errstr)
        );
    }

    if !prep_password.is_null() {
        pfree(prep_password as *mut c_void);
    }

    /*
     * Compare the secret's Server Key with the one computed from the
     * user-supplied password.
     */
    memcmp(
        computed_key.as_ptr() as *const c_void,
        server_key.as_ptr() as *const c_void,
        key_length as Size,
    ) == 0
}

/*
 * Parse and validate format of given SCRAM secret.
 *
 * On success, the iteration count, salt, stored key, and server key are
 * extracted from the secret, and returned to the caller.  For 'stored_key'
 * and 'server_key', the caller must pass pre-allocated buffers of size
 * SCRAM_MAX_KEY_LEN.  Salt is returned as a base64-encoded, null-terminated
 * string.  The buffer for the salt is palloc'd by this function.
 *
 * Returns true if the SCRAM secret has been parsed, and false otherwise.
 */
pub unsafe fn parse_scram_secret(
    secret: *const c_char,
    iterations: *mut c_int,
    hash_type: *mut pg_cryptohash_type,
    key_length: *mut c_int,
    salt: *mut *mut c_char,
    stored_key: *mut uint8,
    server_key: *mut uint8,
) -> bool {
    let mut v: *mut c_char;
    let mut p: *mut c_char;
    let scheme_str: *mut c_char;
    let salt_str: *mut c_char;
    let iterations_str: *mut c_char;
    let storedkey_str: *mut c_char;
    let serverkey_str: *mut c_char;
    let mut decoded_len: c_int;
    let decoded_salt_buf: *mut uint8;
    let decoded_stored_buf: *mut uint8;
    let decoded_server_buf: *mut uint8;

    /*
     * The secret is of form:
     *
     * SCRAM-SHA-256$<iterations>:<salt>$<storedkey>:<serverkey>
     */
    v = pstrdup(secret);
    scheme_str = strsep(&mut v, b"$\0".as_ptr() as *const c_char);
    if v.is_null() {
        *salt = null_mut();
        return false; /* invalid_secret */
    }
    iterations_str = strsep(&mut v, b":\0".as_ptr() as *const c_char);
    if v.is_null() {
        *salt = null_mut();
        return false;
    }
    salt_str = strsep(&mut v, b"$\0".as_ptr() as *const c_char);
    if v.is_null() {
        *salt = null_mut();
        return false;
    }
    storedkey_str = strsep(&mut v, b":\0".as_ptr() as *const c_char);
    if v.is_null() {
        *salt = null_mut();
        return false;
    }
    serverkey_str = v;

    /* Parse the fields */
    if strcmp(scheme_str, b"SCRAM-SHA-256\0".as_ptr() as *const c_char) != 0 {
        *salt = null_mut();
        return false;
    }
    *hash_type = PG_SHA256;
    *key_length = SCRAM_SHA_256_KEY_LEN as c_int;

    p = null_mut();
    *iterations = strtol(iterations_str, &mut p, 10) as c_int;
    if !p.is_null() && *p != 0 {
        *salt = null_mut();
        return false;
    }

    /*
     * Verify that the salt is in Base64-encoded format, by decoding it,
     * although we return the encoded version to the caller.
     */
    decoded_len = pg_b64_dec_len(strlen(salt_str) as c_int);
    decoded_salt_buf = palloc(decoded_len as Size) as *mut uint8;
    decoded_len = pg_b64_decode(salt_str, strlen(salt_str) as c_int, decoded_salt_buf, decoded_len);
    if decoded_len < 0 {
        *salt = null_mut();
        return false;
    }
    *salt = pstrdup(salt_str);

    /*
     * Decode StoredKey and ServerKey.
     */
    decoded_len = pg_b64_dec_len(strlen(storedkey_str) as c_int);
    decoded_stored_buf = palloc(decoded_len as Size) as *mut uint8;
    decoded_len = pg_b64_decode(
        storedkey_str,
        strlen(storedkey_str) as c_int,
        decoded_stored_buf,
        decoded_len,
    );
    if decoded_len != *key_length {
        *salt = null_mut();
        return false;
    }
    memcpy(stored_key as *mut c_void, decoded_stored_buf as *const c_void, *key_length as Size);

    decoded_len = pg_b64_dec_len(strlen(serverkey_str) as c_int);
    decoded_server_buf = palloc(decoded_len as Size) as *mut uint8;
    decoded_len = pg_b64_decode(
        serverkey_str,
        strlen(serverkey_str) as c_int,
        decoded_server_buf,
        decoded_len,
    );
    if decoded_len != *key_length {
        *salt = null_mut();
        return false;
    }
    memcpy(server_key as *mut c_void, decoded_server_buf as *const c_void, *key_length as Size);

    true
}

// ---------------------------------------------------------------------------
// File-private helpers
// ---------------------------------------------------------------------------

/*
 * Generate plausible SCRAM secret parameters for mock authentication.
 *
 * In a normal authentication, these are extracted from the secret
 * stored in the server.  This function generates values that look
 * realistic, for when there is no stored secret, using SCRAM-SHA-256.
 *
 * Like in parse_scram_secret(), for 'stored_key' and 'server_key', the
 * caller must pass pre-allocated buffers of size SCRAM_MAX_KEY_LEN, and
 * the buffer for the salt is palloc'd by this function.
 */
unsafe fn mock_scram_secret(
    username: *const c_char,
    hash_type: *mut pg_cryptohash_type,
    iterations: *mut c_int,
    key_length: *mut c_int,
    salt: *mut *mut c_char,
    stored_key: *mut uint8,
    server_key: *mut uint8,
) {
    let raw_salt: *mut uint8;
    let encoded_salt: *mut c_char;
    let mut encoded_len: c_int;

    /* Enforce the use of SHA-256, which would be realistic enough */
    *hash_type = PG_SHA256;
    *key_length = SCRAM_SHA_256_KEY_LEN as c_int;

    /*
     * Generate deterministic salt.
     *
     * Note that we cannot reveal any information to an attacker here so the
     * error messages need to remain generic.  This should never fail anyway
     * as the salt generated for mock authentication uses the cluster's nonce
     * value.
     */
    raw_salt = scram_mock_salt(username, *hash_type, *key_length);
    if raw_salt.is_null() {
        elog!(ERROR, "could not encode salt");
    }

    encoded_len = pg_b64_enc_len(SCRAM_DEFAULT_SALT_LEN);
    /* don't forget the zero-terminator */
    encoded_salt = palloc((encoded_len + 1) as Size) as *mut c_char;
    encoded_len = pg_b64_encode(raw_salt, SCRAM_DEFAULT_SALT_LEN, encoded_salt, encoded_len);

    if encoded_len < 0 {
        elog!(ERROR, "could not encode salt");
    }
    *encoded_salt.add(encoded_len as usize) = b'\0' as c_char;

    *salt = encoded_salt;
    *iterations = SCRAM_SHA_256_DEFAULT_ITERATIONS;

    /* StoredKey and ServerKey are not used in a doomed authentication */
    memset(stored_key as *mut c_void, 0, SCRAM_MAX_KEY_LEN as Size);
    memset(server_key as *mut c_void, 0, SCRAM_MAX_KEY_LEN as Size);
}

/*
 * Read the value in a given SCRAM exchange message for given attribute.
 */
unsafe fn read_attr_value(input: *mut *mut c_char, attr: c_char) -> *mut c_char {
    let mut begin: *mut c_char = *input;
    let mut end: *mut c_char;

    if *begin != attr {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            ERROR,
            errmsg!(
                "malformed SCRAM message, expected attribute \"{}\" but found \"{}\"",
                attr as u8 as char,
                cstr_to_str(sanitize_char(*begin))
            )
        );
    }
    begin = begin.add(1);

    if *begin != b'=' as c_char {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            ERROR,
            errmsg!(
                "malformed SCRAM message, expected character \"=\" for attribute \"{}\"",
                attr as u8 as char
            )
        );
    }
    begin = begin.add(1);

    end = begin;
    while *end != 0 && *end != b',' as c_char {
        end = end.add(1);
    }

    if *end != 0 {
        *end = b'\0' as c_char;
        *input = end.add(1);
    } else {
        *input = end;
    }

    begin
}

unsafe fn is_scram_printable(p: *mut c_char) -> bool {
    /*------
     * Printable characters, as defined by SCRAM spec: (RFC 5802)
     *
     *  printable       = %x21-2B / %x2D-7E
     *                    ;; Printable ASCII except ",".
     *                    ;; Note that any "printable" is also
     *                    ;; a valid "value".
     *------
     */
    let mut p: *mut c_char = p;
    while *p != 0 {
        let c = *p as u8;
        if c < 0x21 || c > 0x7E || c == 0x2C /* comma */ {
            return false;
        }
        p = p.add(1);
    }
    true
}

/*
 * Convert an arbitrary byte to printable form.  For error messages.
 *
 * If it's a printable ASCII character, print it as a single character.
 * otherwise, print it in hex.
 *
 * The returned pointer points to a static buffer.
 */
unsafe fn sanitize_char(c: c_char) -> *mut c_char {
    static mut buf: [c_char; 5] = [0; 5];
    let uc = c as u8;
    if uc >= 0x21 && uc <= 0x7E {
        buf[0] = b'\'' as c_char;
        buf[1] = c;
        buf[2] = b'\'' as c_char;
        buf[3] = b'\0' as c_char;
    } else {
        let hi = (uc >> 4) & 0xF;
        let lo = uc & 0xF;
        buf[0] = b'0' as c_char;
        buf[1] = b'x' as c_char;
        buf[2] = if hi < 10 { b'0' + hi } else { b'a' + (hi - 10) } as c_char;
        buf[3] = if lo < 10 { b'0' + lo } else { b'a' + (lo - 10) } as c_char;
        buf[4] = b'\0' as c_char;
    }
    buf.as_mut_ptr()
}

/*
 * Convert an arbitrary string to printable form, for error messages.
 *
 * Anything that's not a printable ASCII character is replaced with
 * '?', and the string is truncated at 30 characters.
 *
 * The returned pointer points to a static buffer.
 */
unsafe fn sanitize_str(s: *const c_char) -> *mut c_char {
    static mut buf: [c_char; 31] = [0; 31];
    let mut i: usize = 0;
    while i < 30 {
        let c = *s.add(i);
        if c == 0 {
            break;
        }
        let uc = c as u8;
        buf[i] = if uc >= 0x21 && uc <= 0x7E { c } else { b'?' as c_char };
        i += 1;
    }
    buf[i] = 0;
    buf.as_mut_ptr()
}

/*
 * Read the next attribute and value in a SCRAM exchange message.
 *
 * The attribute character is set in *attr_p, the attribute value is the
 * return value.
 */
unsafe fn read_any_attr(input: *mut *mut c_char, attr_p: *mut c_char) -> *mut c_char {
    let mut begin: *mut c_char = *input;
    let attr: c_char = *begin;

    if attr == 0 {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            ERROR,
            errmsg!("malformed SCRAM message, attribute expected but found end of string")
        );
    }

    /*------
     * attr-val          = ALPHA "=" value
     *                   ;; Generic syntax of any attribute sent
     *                   ;; by server or client
     *------
     */
    let au = attr as u8;
    if !((au >= b'A' && au <= b'Z') || (au >= b'a' && au <= b'z')) {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            ERROR,
            errmsg!(
                "malformed SCRAM message, attribute expected but found invalid character \"{}\"",
                cstr_to_str(sanitize_char(attr))
            )
        );
    }
    if !attr_p.is_null() {
        *attr_p = attr;
    }
    begin = begin.add(1);

    if *begin != b'=' as c_char {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            ERROR,
            errmsg!(
                "malformed SCRAM message, expected character \"=\" for attribute \"{}\"",
                attr as u8 as char
            )
        );
    }
    begin = begin.add(1);

    let mut end: *mut c_char = begin;
    while *end != 0 && *end != b',' as c_char {
        end = end.add(1);
    }

    if *end != 0 {
        *end = b'\0' as c_char;
        *input = end.add(1);
    } else {
        *input = end;
    }

    begin
}

/*
 * Read and parse the first message from client in the context of a SCRAM
 * authentication exchange message.
 *
 * At this stage, any errors will be reported directly with ereport(ERROR).
 */
unsafe fn read_client_first_message(state: *mut scram_state, input: *const c_char) {
    let mut p: *mut c_char = pstrdup(input);
    let channel_binding_type: *mut c_char;

    /*------
     * The syntax for the client-first-message is: (RFC 5802)
     *
     * saslname           = 1*(value-safe-char / "=2C" / "=3D")
     * authzid            = "a=" saslname
     * cb-name            = 1*(ALPHA / DIGIT / "." / "-")
     * gs2-cbind-flag     = ("p=" cb-name) / "n" / "y"
     * gs2-header         = gs2-cbind-flag "," [ authzid ] ","
     * username           = "n=" saslname
     * reserved-mext      = "m=" 1*(value-char)
     * nonce              = "r=" c-nonce [s-nonce]
     * c-nonce            = printable
     * client-first-message-bare =
     *                      [reserved-mext ","]
     *                      username "," nonce ["," extensions]
     * client-first-message =
     *                      gs2-header client-first-message-bare
     *
     * For example:
     * n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL
     *------
     */

    /*
     * Read gs2-cbind-flag.  (For details see also RFC 5802 Section 6 "Channel
     * Binding".)
     */
    (*state).cbind_flag = *p;
    match *p as u8 {
        b'n' => {
            /*
             * The client does not support channel binding or has simply
             * decided to not use it.  In that case just let it go.
             */
            if (*state).channel_binding_in_use {
                let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                ereport!(
                    ERROR,
                    errmsg!("malformed SCRAM message, client selected SCRAM-SHA-256-PLUS but message has no channel binding data")
                );
            }
            p = p.add(1);
            if *p != b',' as c_char {
                let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                ereport!(
                    ERROR,
                    errmsg!(
                        "malformed SCRAM message, comma expected but found character \"{}\"",
                        cstr_to_str(sanitize_char(*p))
                    )
                );
            }
            p = p.add(1);
        }
        b'y' => {
            /*
             * The client supports channel binding and thinks that the server
             * does not.  In this case, the server must fail authentication if
             * it supports channel binding.
             */
            if (*state).channel_binding_in_use {
                let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                ereport!(
                    ERROR,
                    errmsg!("malformed SCRAM message, client selected SCRAM-SHA-256-PLUS but message has no channel binding data")
                );
            }
            // #ifdef USE_SSL
            if (*(*state).port).ssl_in_use {
                let _ = errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION);
                ereport!(
                    ERROR,
                    errmsg!("SCRAM channel binding negotiation error")
                );
            }
            // #endif
            p = p.add(1);
            if *p != b',' as c_char {
                let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                ereport!(
                    ERROR,
                    errmsg!(
                        "malformed SCRAM message, comma expected but found character \"{}\"",
                        cstr_to_str(sanitize_char(*p))
                    )
                );
            }
            p = p.add(1);
        }
        b'p' => {
            /*
             * The client requires channel binding.  Channel binding type
             * follows, e.g., "p=tls-server-end-point".
             */
            if !(*state).channel_binding_in_use {
                let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                ereport!(
                    ERROR,
                    errmsg!("malformed SCRAM message, client selected SCRAM-SHA-256 without channel binding but message includes channel binding data")
                );
            }

            channel_binding_type = read_attr_value(&mut p, b'p' as c_char);

            /*
             * The only channel binding type we support is
             * tls-server-end-point.
             */
            if strcmp(
                channel_binding_type,
                b"tls-server-end-point\0".as_ptr() as *const c_char,
            ) != 0
            {
                let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                ereport!(
                    ERROR,
                    errmsg!(
                        "unsupported SCRAM channel-binding type \"{}\"",
                        cstr_to_str(sanitize_str(channel_binding_type))
                    )
                );
            }
        }
        _ => {
            let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
            ereport!(
                ERROR,
                errmsg!(
                    "malformed SCRAM message, unexpected channel-binding flag \"{}\"",
                    cstr_to_str(sanitize_char(*p))
                )
            );
        }
    }

    /*
     * Forbid optional authzid (authorization identity).  We don't support it.
     */
    if *p == b'a' as c_char {
        let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        ereport!(
            ERROR,
            errmsg!("client uses authorization identity, but it is not supported")
        );
    }
    if *p != b',' as c_char {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            ERROR,
            errmsg!(
                "malformed SCRAM message, unexpected attribute \"{}\" in client-first-message",
                cstr_to_str(sanitize_char(*p))
            )
        );
    }
    p = p.add(1);

    (*state).client_first_message_bare = pstrdup(p);

    /*
     * Any mandatory extensions would go here.  We don't support any.
     *
     * RFC 5802 specifies error code "e=extensions-not-supported" for this,
     * but it can only be sent in the server-final message.  We prefer to fail
     * immediately (which the RFC also allows).
     */
    if *p == b'm' as c_char {
        let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
        ereport!(
            ERROR,
            errmsg!("client requires an unsupported SCRAM extension")
        );
    }

    /*
     * Read username.  Note: this is ignored.  We use the username from the
     * startup message instead, still it is kept around if provided as it
     * proves to be useful for debugging purposes.
     */
    (*state).client_username = read_attr_value(&mut p, b'n' as c_char);

    /* read nonce and check that it is made of only printable characters */
    (*state).client_nonce = read_attr_value(&mut p, b'r' as c_char);
    if !is_scram_printable((*state).client_nonce) {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(ERROR, errmsg!("non-printable characters in SCRAM nonce"));
    }

    /*
     * There can be any number of optional extensions after this.  We don't
     * support any extensions, so ignore them.
     */
    while *p != 0 {
        read_any_attr(&mut p, null_mut());
    }

    /* success! */
}

/*
 * Verify the final nonce contained in the last message received from
 * client in an exchange.
 */
unsafe fn verify_final_nonce(state: *mut scram_state) -> bool {
    let client_nonce_len: usize = strlen((*state).client_nonce) as usize;
    let server_nonce_len: usize = strlen((*state).server_nonce) as usize;
    let final_nonce_len: usize = strlen((*state).client_final_nonce) as usize;

    if final_nonce_len != client_nonce_len + server_nonce_len {
        return false;
    }
    if memcmp(
        (*state).client_final_nonce as *const c_void,
        (*state).client_nonce as *const c_void,
        client_nonce_len,
    ) != 0
    {
        return false;
    }
    if memcmp(
        (*state).client_final_nonce.add(client_nonce_len) as *const c_void,
        (*state).server_nonce as *const c_void,
        server_nonce_len,
    ) != 0
    {
        return false;
    }

    true
}

/*
 * Verify the client proof contained in the last message received from
 * client in an exchange.  Returns true if the verification is a success,
 * or false for a failure.
 */
unsafe fn verify_client_proof(state: *mut scram_state) -> bool {
    let mut ClientSignature: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let mut client_StoredKey: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let ctx: *mut pg_hmac_ctx = pg_hmac_create((*state).hash_type);
    let mut i: c_int;
    let mut errstr: *const c_char = null();

    /*
     * Calculate ClientSignature.  Note that we don't log directly a failure
     * here even when processing the calculations as this could involve a mock
     * authentication.
     */
    if pg_hmac_init(ctx, (*state).StoredKey.as_ptr(), (*state).key_length as Size) < 0
        || pg_hmac_update(
            ctx,
            (*state).client_first_message_bare as *const uint8,
            strlen((*state).client_first_message_bare),
        ) < 0
        || pg_hmac_update(ctx, b",".as_ptr() as *const uint8, 1) < 0
        || pg_hmac_update(
            ctx,
            (*state).server_first_message as *const uint8,
            strlen((*state).server_first_message),
        ) < 0
        || pg_hmac_update(ctx, b",".as_ptr() as *const uint8, 1) < 0
        || pg_hmac_update(
            ctx,
            (*state).client_final_message_without_proof as *const uint8,
            strlen((*state).client_final_message_without_proof),
        ) < 0
        || pg_hmac_final(ctx, ClientSignature.as_mut_ptr(), (*state).key_length as Size) < 0
    {
        elog!(
            ERROR,
            "could not calculate client signature: {}",
            cstr_to_str(pg_hmac_error(ctx))
        );
    }

    pg_hmac_free(ctx);

    /* Extract the ClientKey that the client calculated from the proof */
    i = 0;
    while i < (*state).key_length {
        (*state).ClientKey[i as usize] =
            (*state).ClientProof[i as usize] ^ ClientSignature[i as usize];
        i += 1;
    }

    /* Hash it one more time, and compare with StoredKey */
    if scram_H(
        (*state).ClientKey.as_ptr(),
        (*state).hash_type,
        (*state).key_length,
        client_StoredKey.as_mut_ptr(),
        &mut errstr,
    ) < 0
    {
        elog!(ERROR, "could not hash stored key: {}", cstr_to_str(errstr));
    }

    if memcmp(
        client_StoredKey.as_ptr() as *const c_void,
        (*state).StoredKey.as_ptr() as *const c_void,
        (*state).key_length as Size,
    ) != 0
    {
        return false;
    }

    true
}

/*
 * Build the first server-side message sent to the client in a SCRAM
 * communication exchange.
 */
unsafe fn build_server_first_message(state: *mut scram_state) -> *mut c_char {
    /*------
     * The syntax for the server-first-message is: (RFC 5802)
     *
     * server-first-message =
     *                  [reserved-mext ","] nonce "," salt ","
     *                  iteration-count ["," extensions]
     *
     * nonce              = "r=" c-nonce [s-nonce]
     * c-nonce            = printable
     * s-nonce            = printable
     * salt               = "s=" base64
     * iteration-count    = "i=" posit-number
     *
     * Example:
     * r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096
     *------
     */

    /*
     * Per the spec, the nonce may consist of any printable ASCII characters.
     * For convenience, however, we don't use the whole range available,
     * rather, we generate some random bytes, and base64 encode them.
     */
    let mut raw_nonce: [uint8; SCRAM_RAW_NONCE_LEN as usize] = [0; SCRAM_RAW_NONCE_LEN as usize];
    let mut encoded_len: c_int;

    if !pg_strong_random(
        raw_nonce.as_mut_ptr() as *mut c_void,
        SCRAM_RAW_NONCE_LEN as Size,
    ) {
        let _ = errcode(ERRCODE_INTERNAL_ERROR);
        ereport!(ERROR, errmsg!("could not generate random nonce"));
    }

    encoded_len = pg_b64_enc_len(SCRAM_RAW_NONCE_LEN);
    /* don't forget the zero-terminator */
    (*state).server_nonce = palloc((encoded_len + 1) as Size) as *mut c_char;
    encoded_len = pg_b64_encode(
        raw_nonce.as_ptr(),
        SCRAM_RAW_NONCE_LEN,
        (*state).server_nonce,
        encoded_len,
    );
    if encoded_len < 0 {
        let _ = errcode(ERRCODE_INTERNAL_ERROR);
        ereport!(ERROR, errmsg!("could not encode random nonce"));
    }
    *(*state).server_nonce.add(encoded_len as usize) = b'\0' as c_char;

    (*state).server_first_message = psprintf_server_first(
        (*state).client_nonce,
        (*state).server_nonce,
        (*state).salt,
        (*state).iterations,
    );

    pstrdup((*state).server_first_message)
}

/*
 * Read and parse the final message received from client.
 */
unsafe fn read_client_final_message(state: *mut scram_state, input: *const c_char) {
    let mut attr: c_char = 0;
    let channel_binding: *mut c_char;
    let mut value: *mut c_char;
    let begin: *mut c_char;
    let mut proof: *mut c_char = null_mut();
    let mut p: *mut c_char;
    let client_proof: *mut uint8;
    let client_proof_len: c_int;

    begin = pstrdup(input);
    p = begin;

    /*------
     * The syntax for the client-final-message is: (RFC 5802)
     *
     * gs2-header         = gs2-cbind-flag "," [ authzid ] ","
     * cbind-input        = gs2-header [ cbind-data ]
     * channel-binding    = "c=" base64
     * proof              = "p=" base64
     * client-final-message-without-proof =
     *                      channel-binding "," nonce ["," extensions]
     * client-final-message =
     *                      client-final-message-without-proof "," proof
     *------
     */

    /*
     * Read channel binding.  This repeats the channel-binding flags and is
     * then followed by the actual binding data depending on the type.
     */
    channel_binding = read_attr_value(&mut p, b'c' as c_char);
    if (*state).channel_binding_in_use {
        // #ifdef USE_SSL
        let cbind_data: *const c_char;
        let mut cbind_data_len: Size = 0;
        let cbind_header_len: Size;
        let cbind_input: *mut c_char;
        let cbind_input_len: Size;
        let b64_message: *mut c_char;
        let mut b64_message_len: c_int;

        Assert!((*state).cbind_flag == b'p' as c_char);

        /* Fetch hash data of server's SSL certificate */
        cbind_data =
            be_tls_get_certificate_hash((*state).port, &mut cbind_data_len) as *const c_char;

        /* should not happen */
        if cbind_data.is_null() || cbind_data_len == 0 {
            elog!(ERROR, "could not get server certificate hash");
        }

        cbind_header_len =
            strlen(b"p=tls-server-end-point,,\0".as_ptr() as *const c_char); /* p=type,, */
        cbind_input_len = cbind_header_len + cbind_data_len;
        cbind_input = palloc(cbind_input_len) as *mut c_char;
        /* snprintf(cbind_input, cbind_input_len, "p=tls-server-end-point,,") */
        let header = b"p=tls-server-end-point,,";
        core::ptr::copy_nonoverlapping(header.as_ptr() as *const c_char, cbind_input, cbind_header_len);
        memcpy(
            cbind_input.add(cbind_header_len) as *mut c_void,
            cbind_data as *const c_void,
            cbind_data_len,
        );

        b64_message_len = pg_b64_enc_len(cbind_input_len as c_int);
        /* don't forget the zero-terminator */
        b64_message = palloc((b64_message_len + 1) as Size) as *mut c_char;
        b64_message_len = pg_b64_encode(
            cbind_input as *const uint8,
            cbind_input_len as c_int,
            b64_message,
            b64_message_len,
        );
        if b64_message_len < 0 {
            elog!(ERROR, "could not encode channel binding data");
        }
        *b64_message.add(b64_message_len as usize) = b'\0' as c_char;

        /*
         * Compare the value sent by the client with the value expected by the
         * server.
         */
        if strcmp(channel_binding, b64_message) != 0 {
            let _ = errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION);
            ereport!(ERROR, errmsg!("SCRAM channel binding check failed"));
        }
        // #else
        //     elog(ERROR, "channel binding not supported by this build");
        // #endif
    } else {
        /*
         * If we are not using channel binding, the binding data is expected
         * to always be "biws", which is "n,," base64-encoded, or "eSws",
         * which is "y,,".  We also have to check whether the flag is the same
         * one that the client originally sent.
         */
        if !(strcmp(channel_binding, b"biws\0".as_ptr() as *const c_char) == 0
            && (*state).cbind_flag == b'n' as c_char)
            && !(strcmp(channel_binding, b"eSws\0".as_ptr() as *const c_char) == 0
                && (*state).cbind_flag == b'y' as c_char)
        {
            let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
            ereport!(
                ERROR,
                errmsg!("unexpected SCRAM channel-binding attribute in client-final-message")
            );
        }
    }

    (*state).client_final_nonce = read_attr_value(&mut p, b'r' as c_char);

    /* ignore optional extensions, read until we find "p" attribute */
    loop {
        proof = p.sub(1);
        value = read_any_attr(&mut p, &mut attr);
        if attr == b'p' as c_char {
            break;
        }
    }

    client_proof_len = pg_b64_dec_len(strlen(value) as c_int);
    client_proof = palloc(client_proof_len as Size) as *mut uint8;
    if pg_b64_decode(value, strlen(value) as c_int, client_proof, client_proof_len)
        != (*state).key_length
    {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(ERROR, errmsg!("malformed SCRAM message, malformed proof in client-final-message"));
    }
    memcpy(
        (*state).ClientProof.as_mut_ptr() as *mut c_void,
        client_proof as *const c_void,
        (*state).key_length as Size,
    );
    pfree(client_proof as *mut c_void);

    if *p != 0 {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            ERROR,
            errmsg!("malformed SCRAM message, garbage found at end of client-final-message")
        );
    }

    let proof_offset: usize = proof.offset_from(begin) as usize;
    (*state).client_final_message_without_proof = palloc(proof_offset + 1) as *mut c_char;
    memcpy(
        (*state).client_final_message_without_proof as *mut c_void,
        input as *const c_void,
        proof_offset,
    );
    *(*state).client_final_message_without_proof.add(proof_offset) = b'\0' as c_char;
}

/*
 * Build the final server-side message of an exchange.
 */
unsafe fn build_server_final_message(state: *mut scram_state) -> *mut c_char {
    let mut ServerSignature: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let server_signature_base64: *mut c_char;
    let mut siglen: c_int;
    let ctx: *mut pg_hmac_ctx = pg_hmac_create((*state).hash_type);

    /* calculate ServerSignature */
    if pg_hmac_init(ctx, (*state).ServerKey.as_ptr(), (*state).key_length as Size) < 0
        || pg_hmac_update(
            ctx,
            (*state).client_first_message_bare as *const uint8,
            strlen((*state).client_first_message_bare),
        ) < 0
        || pg_hmac_update(ctx, b",".as_ptr() as *const uint8, 1) < 0
        || pg_hmac_update(
            ctx,
            (*state).server_first_message as *const uint8,
            strlen((*state).server_first_message),
        ) < 0
        || pg_hmac_update(ctx, b",".as_ptr() as *const uint8, 1) < 0
        || pg_hmac_update(
            ctx,
            (*state).client_final_message_without_proof as *const uint8,
            strlen((*state).client_final_message_without_proof),
        ) < 0
        || pg_hmac_final(ctx, ServerSignature.as_mut_ptr(), (*state).key_length as Size) < 0
    {
        elog!(
            ERROR,
            "could not calculate server signature: {}",
            cstr_to_str(pg_hmac_error(ctx))
        );
    }

    pg_hmac_free(ctx);

    siglen = pg_b64_enc_len((*state).key_length);
    /* don't forget the zero-terminator */
    server_signature_base64 = palloc((siglen + 1) as Size) as *mut c_char;
    siglen = pg_b64_encode(
        ServerSignature.as_ptr(),
        (*state).key_length,
        server_signature_base64,
        siglen,
    );
    if siglen < 0 {
        elog!(ERROR, "could not encode server signature");
    }
    *server_signature_base64.add(siglen as usize) = b'\0' as c_char;

    /*------
     * The syntax for the server-final-message is: (RFC 5802)
     *
     * verifier           = "v=" base64
     *                   ;; base-64 encoded ServerSignature.
     *
     * server-final-message = (server-error / verifier)
     *                   ["," extensions]
     *------
     */
    psprintf_v(server_signature_base64)
}

/*
 * Deterministically generate salt for mock authentication, using a SHA256
 * hash based on the username and a cluster-level secret key.  Returns a
 * pointer to a static buffer of size SCRAM_DEFAULT_SALT_LEN, or NULL.
 */
unsafe fn scram_mock_salt(
    username: *const c_char,
    hash_type: pg_cryptohash_type,
    key_length: c_int,
) -> *mut uint8 {
    let ctx: *mut pg_cryptohash_ctx;
    static mut sha_digest: [uint8; SCRAM_MAX_KEY_LEN] = [0; SCRAM_MAX_KEY_LEN];
    let mock_auth_nonce: *mut c_char = GetMockAuthenticationNonce();

    /*
     * Generate salt using a SHA256 hash of the username and the cluster's
     * mock authentication nonce.  (This works as long as the salt length is
     * not larger than the SHA256 digest length.  If the salt is smaller, the
     * caller will just ignore the extra data.)
     */
    // StaticAssertDecl(PG_SHA256_DIGEST_LENGTH >= SCRAM_DEFAULT_SALT_LEN, ...)
    const _: () = assert!(
        PG_SHA256_DIGEST_LENGTH >= SCRAM_DEFAULT_SALT_LEN as usize,
        "salt length greater than SHA256 digest length"
    );

    /*
     * This may be worth refreshing if support for more hash methods is
     * added.
     */
    Assert!(hash_type == PG_SHA256);

    ctx = pg_cryptohash_create(hash_type);
    if pg_cryptohash_init(ctx) < 0
        || pg_cryptohash_update(ctx, username as *const uint8, strlen(username)) < 0
        || pg_cryptohash_update(
            ctx,
            mock_auth_nonce as *const uint8,
            MOCK_AUTH_NONCE_LEN,
        ) < 0
        || pg_cryptohash_final(ctx, sha_digest.as_mut_ptr(), key_length as Size) < 0
    {
        pg_cryptohash_free(ctx);
        return null_mut();
    }
    pg_cryptohash_free(ctx);

    sha_digest.as_mut_ptr()
}

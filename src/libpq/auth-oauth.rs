//! libpq/auth-oauth.c - Server-side implementation of the SASL OAUTHBEARER mechanism.
//!
//! See the following RFC for more details:
//! - RFC 7628: https://datatracker.ietf.org/doc/html/rfc7628

use crate::prelude::*;

use core::ffi::CStr;

use crate::common::oauth_common::OAUTHBEARER_NAME;
use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoString, initStringInfo, StringInfo, StringInfoData,
};
use crate::libpq::auth::set_authn_id;
use crate::libpq::hba::{check_usermap, HbaLine};
use crate::libpq::libpq_be::{MyClientConnectionInfo, Port};
use crate::libpq::oauth::{
    OAuthValidatorCallbacks, OAuthValidatorModuleInit, ValidatorModuleResult,
    ValidatorModuleState, PG_OAUTH_VALIDATOR_MAGIC,
};
use crate::libpq::sasl::{
    pg_be_sasl_mech, PG_SASL_EXCHANGE_CONTINUE, PG_SASL_EXCHANGE_FAILURE,
    PG_SASL_EXCHANGE_SUCCESS,
};
use crate::nodes::pg_list::{linitial, list_free_deep, List, NIL};
use crate::pg_config::PG_VERSION_NUM;
use crate::port::explicit_bzero::explicit_bzero;
use crate::port::pgstrcasecmp::pg_strncasecmp;
use crate::utils::adt::json::escape_json;
use crate::utils::fmgr::dfmgr::load_external_function;
use crate::utils::mmgr::mcxt::MemoryContextRegisterResetCallback;
use crate::utils::palloc::{MemoryContextCallback, MemoryContextCallbackFunction};

// libpq/auth.h - PG_MAX_AUTH_TOKEN_LENGTH (matches the value used in auth.rs).
const PG_MAX_AUTH_TOKEN_LENGTH: c_int = 65535;

// utils/errcodes.h (generated table not yet ported). Stub the codes used here.
// These are folded into "C also:" comments at the ereport! call sites.
const ERRCODE_PROTOCOL_VIOLATION: c_int = 0;
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
const ERRCODE_INTERNAL_ERROR: c_int = 0;
const ERRCODE_CONFIG_FILE_ERROR: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

// COMMERROR elevel from utils/elog.h (not re-exported via prelude). Stub.
// TODO(pg-port): import from utils::elog once available.
const COMMERROR: c_int = 19;

// TODO(pg-port): utils/adt/varlena.c SplitDirectoriesString (varlena.h).
unsafe fn SplitDirectoriesString(
    rawstring: *mut c_char,
    separator: c_char,
    namelist: *mut *mut List,
) -> bool { crate::utils::adt::varlena::SplitDirectoriesString(rawstring as _, separator as _, namelist as _) }

// TODO(pg-port): utils/mmgr/mcxt.c psprintf (psprintf.h); varargs not modeled.
unsafe fn psprintf(fmt: *const c_char, _arg: *const c_char) -> *mut c_char {
    pstrdup(fmt)
}

extern "C" {
    // <stdio.h> snprintf, used by sanitize_char().
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    // <string.h> helpers used directly on C strings.
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strstr(haystack: *const c_char, needle: *const c_char) -> *mut c_char;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
}

/* GUC */
#[no_mangle]
pub static mut oauth_validator_libraries_string: *mut c_char = null_mut();

static mut validator_module_state: *mut ValidatorModuleState = null_mut();
static mut ValidatorCallbacks: *const OAuthValidatorCallbacks = null();

/* Mechanism declaration */
#[no_mangle]
pub static pg_be_oauth_mech: pg_be_sasl_mech = pg_be_sasl_mech {
    get_mechanisms: Some(oauth_get_mechanisms),
    init: Some(oauth_init),
    exchange: Some(oauth_exchange),

    max_message_length: PG_MAX_AUTH_TOKEN_LENGTH,
};

/* Valid states for the oauth_exchange() machine. */
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
enum oauth_state {
    OAUTH_STATE_INIT = 0,
    OAUTH_STATE_ERROR,
    OAUTH_STATE_FINISHED,
}
use oauth_state::*;

/* Mechanism callback state. */
#[repr(C)]
struct oauth_ctx {
    state: oauth_state,
    port: *mut Port,
    issuer: *const c_char,
    scope: *const c_char,
}

/* Constants seen in an OAUTHBEARER client initial response. */
const KVSEP: c_char = 0x01; /* separator byte for key/value pairs */
const AUTH_KEY: &CStr = c"auth"; /* key containing the Authorization header */
const BEARER_SCHEME: &CStr = c"Bearer "; /* required header scheme (case-insensitive!) */

/*
 * Retrieves the OAUTHBEARER mechanism list (currently a single item).
 *
 * For a full description of the API, see libpq/sasl.h.
 */
unsafe extern "C" fn oauth_get_mechanisms(_port: *mut Port, buf: StringInfo) {
    /* Only OAUTHBEARER is supported. */
    let name = std::ffi::CString::new(OAUTHBEARER_NAME).unwrap();
    appendStringInfoString(buf, name.as_ptr());
    appendStringInfoChar(buf, b'\0' as c_char);
}

/*
 * Initializes mechanism state and loads the configured validator module.
 *
 * For a full description of the API, see libpq/sasl.h.
 */
unsafe extern "C" fn oauth_init(
    port: *mut Port,
    selected_mech: *const c_char,
    _shadow_pass: *const c_char,
) -> *mut c_void {
    let ctx: *mut oauth_ctx;

    let mech_name = std::ffi::CString::new(OAUTHBEARER_NAME).unwrap();
    if strcmp(selected_mech, mech_name.as_ptr()) != 0 {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        let _ = ERRCODE_PROTOCOL_VIOLATION;
        ereport!(
            ERROR,
            errmsg!("client selected an invalid SASL authentication mechanism")
        );
    }

    ctx = palloc0(core::mem::size_of::<oauth_ctx>()) as *mut oauth_ctx;

    (*ctx).state = OAUTH_STATE_INIT;
    (*ctx).port = port;

    Assert!(!(*port).hba.is_null());
    (*ctx).issuer = (*(*port).hba).oauth_issuer;
    (*ctx).scope = (*(*port).hba).oauth_scope;

    load_validator_library((*(*port).hba).oauth_validator);

    ctx as *mut c_void
}

/*
 * Implements the OAUTHBEARER SASL exchange (RFC 7628, Sec. 3.2). This pulls
 * apart the client initial response and validates the Bearer token. It also
 * handles the dummy error response for a failed handshake, as described in
 * Sec. 3.2.3.
 *
 * For a full description of the API, see libpq/sasl.h.
 */
unsafe extern "C" fn oauth_exchange(
    opaq: *mut c_void,
    input: *const c_char,
    inputlen: c_int,
    output: *mut *mut c_char,
    outputlen: *mut c_int,
    _logdetail: *mut *const c_char,
) -> c_int {
    let input_copy: *mut c_char;
    let mut p: *mut c_char;
    let cbind_flag: c_char;
    let auth: *mut c_char;
    let status: c_int;

    let ctx: *mut oauth_ctx = opaq as *mut oauth_ctx;

    *output = null_mut();
    *outputlen = -1;

    /*
     * If the client didn't include an "Initial Client Response" in the
     * SASLInitialResponse message, send an empty challenge, to which the
     * client will respond with the same data that usually comes in the
     * Initial Client Response.
     */
    if input.is_null() {
        Assert!((*ctx).state == OAUTH_STATE_INIT);

        *output = pstrdup(c"".as_ptr());
        *outputlen = 0;
        return PG_SASL_EXCHANGE_CONTINUE;
    }

    /*
     * Check that the input length agrees with the string length of the input.
     */
    if inputlen == 0 {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail("The message is empty.")
        ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
    }
    if inputlen as usize != strlen(input) {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail("Message length does not match input length.")
        ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
    }

    match (*ctx).state {
        OAUTH_STATE_INIT => {
            /* Handle this case below. */
        }

        OAUTH_STATE_ERROR => {
            /*
             * Only one response is valid for the client during authentication
             * failure: a single kvsep.
             */
            if inputlen != 1 || *input != KVSEP {
                // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
                //         errdetail("Client did not send a kvsep response.")
                ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
            }

            /* The (failed) handshake is now complete. */
            (*ctx).state = OAUTH_STATE_FINISHED;
            return PG_SASL_EXCHANGE_FAILURE;
        }

        _ => {
            elog!(ERROR, "invalid OAUTHBEARER exchange state");
            #[allow(unreachable_code)]
            {
                return PG_SASL_EXCHANGE_FAILURE;
            }
        }
    }

    /* Handle the client's initial message. */
    input_copy = pstrdup(input);
    p = input_copy;

    /*
     * OAUTHBEARER does not currently define a channel binding (so there is no
     * OAUTHBEARER-PLUS, and we do not accept a 'p' specifier). We accept a
     * 'y' specifier purely for the remote chance that a future specification
     * could define one; then future clients can still interoperate with this
     * server implementation. 'n' is the expected case.
     */
    cbind_flag = *p;
    match cbind_flag as u8 {
        b'p' => {
            // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
            //         errdetail("The server does not support channel binding for OAuth, but the client message includes channel binding data.")
            ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
        }

        b'y' /* fall through */ | b'n' => {
            p = p.add(1);
            if *p != b',' as c_char {
                // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
                //         errdetail("Comma expected, but found character \"%s\".", sanitize_char(*p))
                ereport!(
                    ERROR,
                    errmsg!(
                        "malformed OAUTHBEARER message: Comma expected, but found character {}",
                        CStr::from_ptr(sanitize_char(*p)).to_string_lossy()
                    )
                );
            }
            p = p.add(1);
        }

        _ => {
            // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
            //         errdetail("Unexpected channel-binding flag \"%s\".", sanitize_char(cbind_flag))
            ereport!(
                ERROR,
                errmsg!(
                    "malformed OAUTHBEARER message: Unexpected channel-binding flag {}",
                    CStr::from_ptr(sanitize_char(cbind_flag)).to_string_lossy()
                )
            );
        }
    }

    /*
     * Forbid optional authzid (authorization identity).  We don't support it.
     */
    if *p == b'a' as c_char {
        // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        let _ = ERRCODE_FEATURE_NOT_SUPPORTED;
        ereport!(
            ERROR,
            errmsg!("client uses authorization identity, but it is not supported")
        );
    }
    if *p != b',' as c_char {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail("Unexpected attribute \"%s\" in client-first-message.", sanitize_char(*p))
        ereport!(
            ERROR,
            errmsg!(
                "malformed OAUTHBEARER message: Unexpected attribute {} in client-first-message",
                CStr::from_ptr(sanitize_char(*p)).to_string_lossy()
            )
        );
    }
    p = p.add(1);

    /* All remaining fields are separated by the RFC's kvsep (\x01). */
    if *p != KVSEP {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail("Key-value separator expected, but found character \"%s\".", sanitize_char(*p))
        ereport!(
            ERROR,
            errmsg!(
                "malformed OAUTHBEARER message: Key-value separator expected, but found character {}",
                CStr::from_ptr(sanitize_char(*p)).to_string_lossy()
            )
        );
    }
    p = p.add(1);

    auth = parse_kvpairs_for_auth(&mut p);
    if auth.is_null() {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail("Message does not contain an auth value.")
        ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
    }

    /* We should be at the end of our message. */
    if *p != 0 {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail("Message contains additional data after the final terminator.")
        ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
    }

    if !validate((*ctx).port, auth) {
        generate_error_response(ctx, output, outputlen);

        (*ctx).state = OAUTH_STATE_ERROR;
        status = PG_SASL_EXCHANGE_CONTINUE;
    } else {
        (*ctx).state = OAUTH_STATE_FINISHED;
        status = PG_SASL_EXCHANGE_SUCCESS;
    }

    /* Don't let extra copies of the bearer token hang around. */
    explicit_bzero(input_copy as *mut c_void, inputlen as Size);

    status
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

    if c >= 0x21 && c <= 0x7E {
        snprintf(
            buf.as_mut_ptr(),
            core::mem::size_of_val(&buf),
            c"'%c'".as_ptr(),
            c as c_int,
        );
    } else {
        snprintf(
            buf.as_mut_ptr(),
            core::mem::size_of_val(&buf),
            c"0x%02x".as_ptr(),
            (c as c_uchar) as c_int,
        );
    }
    buf.as_mut_ptr()
}

/*
 * Performs syntactic validation of a key and value from the initial client
 * response. (Semantic validation of interesting values must be performed
 * later.)
 */
unsafe fn validate_kvpair(key: *const c_char, mut val: *const c_char) {
    /*-----
     * From Sec 3.1:
     *     key            = 1*(ALPHA)
     */
    static key_allowed_set: &CStr = c"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    let span: usize;

    if *key == 0 {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail("Message contains an empty key name.")
        ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
    }

    span = strspn(key, key_allowed_set.as_ptr());
    if *key.add(span) != b'\0' as c_char {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail("Message contains an invalid key name.")
        ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
    }

    /*-----
     * From Sec 3.1:
     *     value          = *(VCHAR / SP / HTAB / CR / LF )
     *
     * The VCHAR (visible character) class is large; a loop is more
     * straightforward than strspn().
     */
    while *val != 0 {
        if 0x21 <= *val && *val <= 0x7E {
            val = val.add(1);
            continue; /* VCHAR */
        }

        match *val as u8 {
            b' ' | b'\t' | b'\r' | b'\n' => {
                /* SP, HTAB, CR, LF */
            }

            _ => {
                // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
                //         errdetail("Message contains an invalid value.")
                ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
            }
        }
        val = val.add(1);
    }
}

/*
 * Consumes all kvpairs in an OAUTHBEARER exchange message. If the "auth" key is
 * found, its value is returned.
 */
unsafe fn parse_kvpairs_for_auth(input: *mut *mut c_char) -> *mut c_char {
    let mut pos: *mut c_char = *input;
    let mut auth: *mut c_char = null_mut();

    /*----
     * The relevant ABNF, from Sec. 3.1:
     *
     *     kvsep          = %x01
     *     key            = 1*(ALPHA)
     *     value          = *(VCHAR / SP / HTAB / CR / LF )
     *     kvpair         = key "=" value kvsep
     *   ;;gs2-header     = See RFC 5801
     *     client-resp    = (gs2-header kvsep *kvpair kvsep) / kvsep
     *
     * By the time we reach this code, the gs2-header and initial kvsep have
     * already been validated. We start at the beginning of the first kvpair.
     */

    while *pos != 0 {
        let end: *mut c_char;
        let sep: *mut c_char;
        let key: *mut c_char;
        let value: *mut c_char;

        /*
         * Find the end of this kvpair. Note that input is null-terminated by
         * the SASL code, so the strchr() is bounded.
         */
        end = strchr(pos, KVSEP as c_int);
        if end.is_null() {
            // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
            //         errdetail("Message contains an unterminated key/value pair.")
            ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
        }
        *end = b'\0' as c_char;

        if pos == end {
            /* Empty kvpair, signifying the end of the list. */
            *input = pos.add(1);
            return auth;
        }

        /*
         * Find the end of the key name.
         */
        sep = strchr(pos, b'=' as c_int);
        if sep.is_null() {
            // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
            //         errdetail("Message contains a key without a value.")
            ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
        }
        *sep = b'\0' as c_char;

        /* Both key and value are now safely terminated. */
        key = pos;
        value = sep.add(1);
        validate_kvpair(key, value);

        if strcmp(key, AUTH_KEY.as_ptr()) == 0 {
            if !auth.is_null() {
                // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
                //         errdetail("Message contains multiple auth values.")
                ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));
            }

            auth = value;
        } else {
            /*
             * The RFC also defines the host and port keys, but they are not
             * required for OAUTHBEARER and we do not use them. Also, per Sec.
             * 3.1, any key/value pairs we don't recognize must be ignored.
             */
        }

        /* Move to the next pair. */
        pos = end.add(1);
    }

    // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
    //         errdetail("Message did not contain a final terminator.")
    ereport!(ERROR, errmsg!("malformed OAUTHBEARER message"));

    #[allow(unreachable_code)]
    {
        // pg_unreachable();
        null_mut()
    }
}

/*
 * Builds the JSON response for failed authentication (RFC 7628, Sec. 3.2.2).
 * This contains the required scopes for entry and a pointer to the OAuth/OpenID
 * discovery document, which the client may use to conduct its OAuth flow.
 */
unsafe fn generate_error_response(
    ctx: *mut oauth_ctx,
    output: *mut *mut c_char,
    outputlen: *mut c_int,
) {
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut issuer: StringInfoData = core::mem::zeroed();

    /*
     * The admin needs to set an issuer and scope for OAuth to work. There's
     * not really a way to hide this from the user, either, because we can't
     * choose a "default" issuer, so be honest in the failure message. (In
     * practice such configurations are rejected during HBA parsing.)
     */
    if (*ctx).issuer.is_null() || (*ctx).scope.is_null() {
        // C also: errcode(ERRCODE_INTERNAL_ERROR)
        //         errdetail_log("The issuer and scope parameters must be set in pg_hba.conf.")
        let _ = ERRCODE_INTERNAL_ERROR;
        ereport!(
            FATAL,
            errmsg!("OAuth is not properly configured for this user")
        );
    }

    /*
     * Build a default .well-known URI based on our issuer, unless the HBA has
     * already provided one.
     */
    initStringInfo(&mut issuer);
    appendStringInfoString(&mut issuer, (*ctx).issuer);
    if strstr((*ctx).issuer, c"/.well-known/".as_ptr()).is_null() {
        appendStringInfoString(&mut issuer, c"/.well-known/openid-configuration".as_ptr());
    }

    initStringInfo(&mut buf);

    /*
     * Escaping the string here is belt-and-suspenders defensive programming
     * since escapable characters aren't valid in either the issuer URI or the
     * scope list, but the HBA doesn't enforce that yet.
     */
    appendStringInfoString(&mut buf, c"{ \"status\": \"invalid_token\", ".as_ptr());

    appendStringInfoString(&mut buf, c"\"openid-configuration\": ".as_ptr());
    escape_json(&mut buf, issuer.data);
    pfree(issuer.data as *mut c_void);

    appendStringInfoString(&mut buf, c", \"scope\": ".as_ptr());
    escape_json(&mut buf, (*ctx).scope);

    appendStringInfoString(&mut buf, c" }".as_ptr());

    *output = buf.data;
    *outputlen = buf.len;
}

/*-----
 * Validates the provided Authorization header and returns the token from
 * within it. NULL is returned on validation failure.
 *
 * Only Bearer tokens are accepted. The ABNF is defined in RFC 6750, Sec.
 * 2.1:
 *
 *      b64token    = 1*( ALPHA / DIGIT /
 *                        "-" / "." / "_" / "~" / "+" / "/" ) *"="
 *      credentials = "Bearer" 1*SP b64token
 *
 * The "credentials" construction is what we receive in our auth value.
 *
 * Since that spec is subordinate to HTTP (i.e. the HTTP Authorization
 * header format; RFC 9110 Sec. 11), the "Bearer" scheme string must be
 * compared case-insensitively. (This is not mentioned in RFC 6750, but the
 * OAUTHBEARER spec points it out: RFC 7628 Sec. 4.)
 *
 * Invalid formats are technically a protocol violation, but we shouldn't
 * reflect any information about the sensitive Bearer token back to the
 * client; log at COMMERROR instead.
 */
unsafe fn validate_token_format(header: *const c_char) -> *const c_char {
    let mut span: usize;
    let mut token: *const c_char;
    static b64token_allowed_set: &CStr =
        c"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~+/";

    /* Missing auth headers should be handled by the caller. */
    Assert!(!header.is_null());

    if *header == b'\0' as c_char {
        /*
         * A completely empty auth header represents a query for
         * authentication parameters. The client expects it to fail; there's
         * no need to make any extra noise in the logs.
         *
         * TODO: should we find a way to return STATUS_EOF at the top level,
         * to suppress the authentication error entirely?
         */
        return null();
    }

    if pg_strncasecmp(header, BEARER_SCHEME.as_ptr(), strlen(BEARER_SCHEME.as_ptr()) as Size) != 0 {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail_log("Client response indicated a non-Bearer authentication scheme.")
        ereport!(COMMERROR, errmsg!("malformed OAuth bearer token"));
        return null();
    }

    /* Pull the bearer token out of the auth value. */
    token = header.add(strlen(BEARER_SCHEME.as_ptr()));

    /* Swallow any additional spaces. */
    while *token == b' ' as c_char {
        token = token.add(1);
    }

    /* Tokens must not be empty. */
    if *token == 0 {
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail_log("Bearer token is empty.")
        ereport!(COMMERROR, errmsg!("malformed OAuth bearer token"));
        return null();
    }

    /*
     * Make sure the token contains only allowed characters. Tokens may end
     * with any number of '=' characters.
     */
    span = strspn(token, b64token_allowed_set.as_ptr());
    while *token.add(span) == b'=' as c_char {
        span += 1;
    }

    if *token.add(span) != b'\0' as c_char {
        /*
         * This error message could be more helpful by printing the
         * problematic character(s), but that'd be a bit like printing a piece
         * of someone's password into the logs.
         */
        // C also: errcode(ERRCODE_PROTOCOL_VIOLATION)
        //         errdetail_log("Bearer token is not in the correct format.")
        ereport!(COMMERROR, errmsg!("malformed OAuth bearer token"));
        return null();
    }

    token
}

/*
 * Checks that the "auth" kvpair in the client response contains a syntactically
 * valid Bearer token, then passes it along to the loaded validator module for
 * authorization. Returns true if validation succeeds.
 */
unsafe fn validate(port: *mut Port, auth: *const c_char) -> bool {
    let map_status: c_int;
    let ret: *mut ValidatorModuleResult;
    let token: *const c_char;
    let mut status: bool;

    /* Ensure that we have a correct token to validate */
    token = validate_token_format(auth);
    if token.is_null() {
        return false;
    }

    /*
     * Ensure that we have a validation library loaded, this should always be
     * the case and an error here is indicative of a bug.
     */
    if ValidatorCallbacks.is_null() || (*ValidatorCallbacks).validate_cb.is_none() {
        // C also: errcode(ERRCODE_INTERNAL_ERROR)
        let _ = ERRCODE_INTERNAL_ERROR;
        ereport!(
            FATAL,
            errmsg!("validation of OAuth token requested without a validator loaded")
        );
    }

    /* Call the validation function from the validator module */
    ret = palloc0(core::mem::size_of::<ValidatorModuleResult>()) as *mut ValidatorModuleResult;
    if !((*ValidatorCallbacks).validate_cb.unwrap())(
        validator_module_state,
        token,
        (*port).user_name,
        ret,
    ) {
        // C also: errcode(ERRCODE_INTERNAL_ERROR)
        ereport!(
            WARNING,
            errmsg!("internal error in OAuth validator module")
        );
        return false;
    }

    /*
     * Log any authentication results even if the token isn't authorized; it
     * might be useful for auditing or troubleshooting.
     */
    if !(*ret).authn_id.is_null() {
        set_authn_id(port, (*ret).authn_id);
    }

    'cleanup: {
        if !(*ret).authorized {
            // C also: errdetail_log("Validator failed to authorize the provided token.")
            ereport!(
                LOG,
                errmsg!(
                    "OAuth bearer authentication failed for user \"{}\"",
                    CStr::from_ptr((*port).user_name).to_string_lossy()
                )
            );

            status = false;
            break 'cleanup;
        }

        if (*(*port).hba).oauth_skip_usermap {
            /*
             * If the validator is our authorization authority, we're done.
             * Authentication may or may not have been performed depending on the
             * validator implementation; all that matters is that the validator
             * says the user can log in with the target role.
             */
            status = true;
            break 'cleanup;
        }

        /* Make sure the validator authenticated the user. */
        if (*ret).authn_id.is_null() || *(*ret).authn_id == b'\0' as c_char {
            // C also: errdetail_log("Validator provided no identity.")
            ereport!(
                LOG,
                errmsg!(
                    "OAuth bearer authentication failed for user \"{}\"",
                    CStr::from_ptr((*port).user_name).to_string_lossy()
                )
            );

            status = false;
            break 'cleanup;
        }

        /* Finally, check the user map. */
        map_status = check_usermap(
            (*(*port).hba).usermap,
            (*port).user_name,
            MyClientConnectionInfo.authn_id,
            false,
        );
        status = map_status == STATUS_OK;
    }

    /*
     * Clear and free the validation result from the validator module once
     * we're done with it.
     */
    if !(*ret).authn_id.is_null() {
        pfree((*ret).authn_id as *mut c_void);
    }
    pfree(ret as *mut c_void);

    status
}

/*
 * load_validator_library
 *
 * Load the configured validator library in order to perform token validation.
 * There is no built-in fallback since validation is implementation specific. If
 * no validator library is configured, or if it fails to load, then error out
 * since token validation won't be possible.
 */
unsafe fn load_validator_library(libname: *const c_char) {
    let validator_init: OAuthValidatorModuleInit;
    let mcb: *mut MemoryContextCallback;

    /*
     * The presence, and validity, of libname has already been established by
     * check_oauth_validator so we don't need to perform more than Assert
     * level checking here.
     */
    Assert!(!libname.is_null() && *libname != 0);

    validator_init = core::mem::transmute::<*mut c_void, OAuthValidatorModuleInit>(
        load_external_function(
            libname,
            c"_PG_oauth_validator_module_init".as_ptr(),
            false,
            null_mut(),
        ),
    );

    /*
     * The validator init function is required since it will set the callbacks
     * for the validator library.
     */
    if validator_init.is_none() {
        ereport!(
            ERROR,
            errmsg!(
                "{} module \"{}\" must define the symbol {}",
                "OAuth validator",
                CStr::from_ptr(libname).to_string_lossy(),
                "_PG_oauth_validator_module_init"
            )
        );
    }

    ValidatorCallbacks = (validator_init.unwrap())();
    Assert!(!ValidatorCallbacks.is_null());

    /*
     * Check the magic number, to protect against break-glass scenarios where
     * the ABI must change within a major version. load_external_function()
     * already checks for compatibility across major versions.
     */
    if (*ValidatorCallbacks).magic != PG_OAUTH_VALIDATOR_MAGIC {
        // C also: errdetail("Server has magic number 0x%08X, module has 0x%08X.",
        //                   PG_OAUTH_VALIDATOR_MAGIC, ValidatorCallbacks->magic)
        ereport!(
            ERROR,
            errmsg!(
                "{} module \"{}\": magic number mismatch",
                "OAuth validator",
                CStr::from_ptr(libname).to_string_lossy()
            )
        );
    }

    /*
     * Make sure all required callbacks are present in the ValidatorCallbacks
     * structure. Right now only the validation callback is required.
     */
    if (*ValidatorCallbacks).validate_cb.is_none() {
        ereport!(
            ERROR,
            errmsg!(
                "{} module \"{}\" must provide a {} callback",
                "OAuth validator",
                CStr::from_ptr(libname).to_string_lossy(),
                "validate_cb"
            )
        );
    }

    /* Allocate memory for validator library private state data */
    validator_module_state =
        palloc0(core::mem::size_of::<ValidatorModuleState>()) as *mut ValidatorModuleState;
    (*validator_module_state).sversion = PG_VERSION_NUM as c_int;

    if (*ValidatorCallbacks).startup_cb.is_some() {
        ((*ValidatorCallbacks).startup_cb.unwrap())(validator_module_state);
    }

    /* Shut down the library before cleaning up its state. */
    mcb = palloc0(core::mem::size_of::<MemoryContextCallback>()) as *mut MemoryContextCallback;
    (*mcb).func = Some(shutdown_validator_library as unsafe extern "C" fn(*mut c_void));

    MemoryContextRegisterResetCallback(CurrentMemoryContext, mcb);
}

/*
 * Call the validator module's shutdown callback, if one is provided. This is
 * invoked during memory context reset.
 */
unsafe extern "C" fn shutdown_validator_library(_arg: *mut c_void) {
    if (*ValidatorCallbacks).shutdown_cb.is_some() {
        ((*ValidatorCallbacks).shutdown_cb.unwrap())(validator_module_state);
    }
}

/*
 * Ensure an OAuth validator named in the HBA is permitted by the configuration.
 *
 * If the validator is currently unset and exactly one library is declared in
 * oauth_validator_libraries, then that library will be used as the validator.
 * Otherwise the name must be present in the list of oauth_validator_libraries.
 */
pub unsafe fn check_oauth_validator(
    hbaline: *mut HbaLine,
    elevel: c_int,
    err_msg: *mut *mut c_char,
) -> bool {
    let line_num: c_int = (*hbaline).linenumber;
    let file_name: *const c_char = (*hbaline).sourcefile;
    let rawstring: *mut c_char;
    let mut elemlist: *mut List = NIL;

    *err_msg = null_mut();

    if *oauth_validator_libraries_string == b'\0' as c_char {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR)
        //         errcontext("line %d of configuration file \"%s\"", line_num, file_name)
        let _ = (ERRCODE_CONFIG_FILE_ERROR, line_num, file_name);
        ereport!(
            elevel,
            errmsg!(
                "oauth_validator_libraries must be set for authentication method {}",
                "oauth"
            )
        );
        *err_msg = psprintf(
            c"oauth_validator_libraries must be set for authentication method %s".as_ptr(),
            c"oauth".as_ptr(),
        );
        return false;
    }

    /* SplitDirectoriesString needs a modifiable copy */
    rawstring = pstrdup(oauth_validator_libraries_string);

    'done: {
        if !SplitDirectoriesString(rawstring, b',' as c_char, &mut elemlist) {
            /* syntax error in list */
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR)
            ereport!(
                elevel,
                errmsg!(
                    "invalid list syntax in parameter \"{}\"",
                    "oauth_validator_libraries"
                )
            );
            *err_msg = psprintf(
                c"invalid list syntax in parameter \"%s\"".as_ptr(),
                c"oauth_validator_libraries".as_ptr(),
            );
            break 'done;
        }

        if (*hbaline).oauth_validator.is_null() {
            if (*elemlist).length == 1 {
                (*hbaline).oauth_validator = pstrdup(linitial(elemlist) as *const c_char);
                break 'done;
            }

            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR)
            //         errcontext("line %d of configuration file \"%s\"", line_num, file_name)
            ereport!(
                elevel,
                errmsg!("authentication method \"oauth\" requires argument \"validator\" to be set when oauth_validator_libraries contains multiple options")
            );
            *err_msg = pstrdup(c"authentication method \"oauth\" requires argument \"validator\" to be set when oauth_validator_libraries contains multiple options".as_ptr());
            break 'done;
        }

        foreach_ptr!(c_char, allowed, elemlist, {
            if strcmp(allowed, (*hbaline).oauth_validator) == 0 {
                break 'done;
            }
        });

        // C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE)
        //         errcontext("line %d of configuration file \"%s\"", line_num, file_name)
        let _ = ERRCODE_INVALID_PARAMETER_VALUE;
        ereport!(
            elevel,
            errmsg!(
                "validator \"{}\" is not permitted by {}",
                CStr::from_ptr((*hbaline).oauth_validator).to_string_lossy(),
                "oauth_validator_libraries"
            )
        );
        *err_msg = psprintf(
            c"validator \"%s\" is not permitted by %s".as_ptr(),
            (*hbaline).oauth_validator,
        );
    }

    list_free_deep(elemlist);
    pfree(rawstring as *mut c_void);

    (*err_msg).is_null()
}

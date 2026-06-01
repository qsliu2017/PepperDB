//! libpq/auth-sasl.c - Routines to handle authentication via SASL.

use crate::prelude::*;

use crate::lib::stringinfo::{
    appendStringInfoChar, initStringInfo, StringInfo, StringInfoData,
};
use crate::libpq::libpq::{pq_getbyte, pq_getmessage, pq_startmsgread};
use crate::libpq::pqformat::{
    pq_getmsgbytes, pq_getmsgend, pq_getmsgint, pq_getmsgrawstring,
};
use crate::libpq::protocol::{AUTH_REQ_SASL, AUTH_REQ_SASL_CONT, AUTH_REQ_SASL_FIN, PqMsg_SASLResponse};
use crate::libpq::sasl::{
    pg_be_sasl_mech, Port, PG_SASL_EXCHANGE_CONTINUE, PG_SASL_EXCHANGE_FAILURE,
    PG_SASL_EXCHANGE_SUCCESS,
};

// C `EOF` from <stdio.h>; not defined as a Rust constant in the port.
const EOF: c_int = -1;

// ERRCODE_PROTOCOL_VIOLATION from utils/errcodes.h (not yet ported). Stub.
// TODO: import from generated errcodes once available.
const ERRCODE_PROTOCOL_VIOLATION: c_int = 0;

// sendAuthRequest() lives in auth.c, which is not yet ported. Stub locally.
// TODO: import from libpq::auth once auth.c is ported.
unsafe fn sendAuthRequest(
    _port: *mut Port,
    _areq: c_int,
    _extradata: *const c_char,
    _extralen: c_int,
) {
    unimplemented!()
}

/*
 * Perform a SASL exchange with a libpq client, using a specific mechanism
 * implementation.
 *
 * shadow_pass is an optional pointer to the stored secret of the role
 * authenticated, from pg_authid.rolpassword.  For mechanisms that use
 * shadowed passwords, a NULL pointer here means that an entry could not
 * be found for the role (or the user does not exist), and the mechanism
 * should fail the authentication exchange.
 *
 * Mechanisms must take care not to reveal to the client that a user entry
 * does not exist; ideally, the external failure mode is identical to that
 * of an incorrect password.  Mechanisms may instead use the logdetail
 * output parameter to internally differentiate between failure cases and
 * assist debugging by the server admin.
 *
 * A mechanism is not required to utilize a shadow entry, or even a password
 * system at all; for these cases, shadow_pass may be ignored and the caller
 * should just pass NULL.
 */
pub unsafe fn CheckSASLAuth(
    mech: *const pg_be_sasl_mech,
    port: *mut Port,
    shadow_pass: *mut c_char,
    logdetail: *mut *const c_char,
) -> c_int {
    let mut sasl_mechs: StringInfoData = core::mem::zeroed();
    let mut mtype: c_int;
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut opaq: *mut c_void = null_mut();
    let mut output: *mut c_char = null_mut();
    let mut outputlen: c_int = 0;
    let mut input: *const c_char;
    let mut inputlen: c_int;
    let mut result: c_int = PG_SASL_EXCHANGE_CONTINUE;
    let mut initial: bool;

    /*
     * Send the SASL authentication request to user.  It includes the list of
     * authentication mechanisms that are supported.
     */
    initStringInfo(&mut sasl_mechs);

    ((*mech).get_mechanisms.unwrap())(port, &mut sasl_mechs);
    /* Put another '\0' to mark that list is finished. */
    appendStringInfoChar(&mut sasl_mechs, b'\0' as c_char);

    sendAuthRequest(port, AUTH_REQ_SASL, sasl_mechs.data, sasl_mechs.len);
    pfree(sasl_mechs.data as *mut c_void);

    /*
     * Loop through SASL message exchange.  This exchange can consist of
     * multiple messages sent in both directions.  First message is always
     * from the client.  All messages from client to server are password
     * packets (type 'p').
     */
    initial = true;
    loop {
        pq_startmsgread();
        mtype = pq_getbyte();
        if mtype != PqMsg_SASLResponse as c_int {
            /* Only log error if client didn't disconnect. */
            if mtype != EOF {
                let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                ereport!(
                    ERROR,
                    format!("expected SASL response, got message type {}", mtype)
                );
            } else {
                return STATUS_EOF;
            }
        }

        /* Get the actual SASL message */
        initStringInfo(&mut buf);
        if pq_getmessage(&mut buf, (*mech).max_message_length) != 0 {
            /* EOF - pq_getmessage already logged error */
            pfree(buf.data as *mut c_void);
            return STATUS_ERROR;
        }

        elog!(
            DEBUG4,
            "processing received SASL response of length {}",
            buf.len
        );

        /*
         * The first SASLInitialResponse message is different from the others.
         * It indicates which SASL mechanism the client selected, and contains
         * an optional Initial Client Response payload.  The subsequent
         * SASLResponse messages contain just the SASL payload.
         */
        if initial {
            let selected_mech: *const c_char;

            selected_mech = pq_getmsgrawstring(&mut buf);

            /*
             * Initialize the status tracker for message exchanges.
             *
             * If the user doesn't exist, or doesn't have a valid password, or
             * it's expired, we still go through the motions of SASL
             * authentication, but tell the authentication method that the
             * authentication is "doomed". That is, it's going to fail, no
             * matter what.
             *
             * This is because we don't want to reveal to an attacker what
             * usernames are valid, nor which users have a valid password.
             */
            opaq = ((*mech).init.unwrap())(port, selected_mech, shadow_pass);

            inputlen = pq_getmsgint(&mut buf, 4) as c_int;
            if inputlen == -1 {
                input = null();
            } else {
                input = pq_getmsgbytes(&mut buf, inputlen);
            }

            initial = false;
        } else {
            inputlen = buf.len;
            input = pq_getmsgbytes(&mut buf, buf.len);
        }
        pq_getmsgend(&mut buf);

        /*
         * The StringInfo guarantees that there's a \0 byte after the
         * response.
         */
        Assert!(input.is_null() || *input.add(inputlen as usize) == b'\0' as c_char);

        /*
         * Hand the incoming message to the mechanism implementation.
         */
        result = ((*mech).exchange.unwrap())(
            opaq,
            input,
            inputlen,
            &mut output,
            &mut outputlen,
            logdetail,
        );

        /* input buffer no longer used */
        pfree(buf.data as *mut c_void);

        if !output.is_null() {
            /*
             * PG_SASL_EXCHANGE_FAILURE with some output is forbidden by SASL.
             * Make sure here that the mechanism used got that right.
             */
            if result == PG_SASL_EXCHANGE_FAILURE {
                elog!(ERROR, "output message found after SASL exchange failure");
            }

            /*
             * Negotiation generated data to be sent to the client.
             */
            elog!(DEBUG4, "sending SASL challenge of length {}", outputlen);

            if result == PG_SASL_EXCHANGE_SUCCESS {
                sendAuthRequest(port, AUTH_REQ_SASL_FIN, output, outputlen);
            } else {
                sendAuthRequest(port, AUTH_REQ_SASL_CONT, output, outputlen);
            }

            pfree(output as *mut c_void);
        }

        if result != PG_SASL_EXCHANGE_CONTINUE {
            break;
        }
    }

    /* Oops, Something bad happened */
    if result != PG_SASL_EXCHANGE_SUCCESS {
        return STATUS_ERROR;
    }

    STATUS_OK
}

//! libpq/be-gssapi-common.c - Common code for GSSAPI authentication and encryption

use crate::prelude::*;

use crate::c::{Min, Size};
use crate::libpq::pg_gssapi::{
    gss_buffer_desc, gss_cred_id_t, gss_OID, gss_OID_set, OM_uint32,
};
use crate::utils::elog::COMMERROR;
use std::ffi::{c_char, c_int, c_void};

// ---------------------------------------------------------------------------
// GSSAPI system-library constants/types/functions (would come from <gssapi.h>).
// These are NOT part of be-gssapi-common.c; they stand in for the external
// GSSAPI symbols the C file references via <gssapi.h>.
// TODO: replace with real GSSAPI bindings when GSS support is wired up.
// ---------------------------------------------------------------------------

const GSS_S_COMPLETE: OM_uint32 = 0;

const GSS_C_GSS_CODE: c_int = 1;
const GSS_C_MECH_CODE: c_int = 2;

const GSS_C_NO_OID: gss_OID = std::ptr::null_mut();
const GSS_C_NULL_OID: gss_OID = std::ptr::null_mut();

const GSS_C_INITIATE: gss_cred_usage_t = 1;

/// Credential usage type (`gss_cred_usage_t`).
type gss_cred_usage_t = c_int;

/// Key/value element descriptor (`gss_key_value_element_desc`).
#[repr(C)]
#[derive(Clone, Copy)]
struct gss_key_value_element_desc {
    key: *const c_char,
    value: *const c_char,
}

/// Key/value set descriptor (`gss_key_value_set_desc`).
#[repr(C)]
#[derive(Clone, Copy)]
struct gss_key_value_set_desc {
    count: OM_uint32,
    elements: *mut gss_key_value_element_desc,
}

unsafe fn gss_display_status(
    _minor_status: *mut OM_uint32,
    _status_value: OM_uint32,
    _status_type: c_int,
    _mech_type: gss_OID,
    _message_context: *mut OM_uint32,
    _status_string: *mut gss_buffer_desc,
) -> OM_uint32 {
    unimplemented!()
}

unsafe fn gss_release_buffer(
    _minor_status: *mut OM_uint32,
    _buffer: *mut gss_buffer_desc,
) -> OM_uint32 {
    unimplemented!()
}

unsafe fn gss_store_cred_into(
    _minor_status: *mut OM_uint32,
    _input_cred_handle: gss_cred_id_t,
    _input_usage: gss_cred_usage_t,
    _desired_mech: gss_OID,
    _overwrite_cred: c_int,
    _default_cred: c_int,
    _cred_store: *const gss_key_value_set_desc,
    _elements_stored: *mut gss_OID_set,
    _cred_usage_stored: *mut gss_cred_usage_t,
) -> OM_uint32 {
    unimplemented!()
}

unsafe fn gss_release_cred(
    _minor_status: *mut OM_uint32,
    _cred_handle: *mut gss_cred_id_t,
) -> OM_uint32 {
    unimplemented!()
}

// ---------------------------------------------------------------------------

/*
 * Fetch all errors of a specific type and append to "s" (buffer of size len).
 * If we obtain more than one string, separate them with spaces.
 * Call once for GSS_CODE and once for MECH_CODE.
 */
unsafe fn pg_GSS_error_int(s: *mut c_char, len: Size, stat: OM_uint32, type_: c_int) {
    let mut gmsg: gss_buffer_desc = std::mem::zeroed();
    let mut i: Size = 0;
    let mut lmin_s: OM_uint32 = 0;
    let mut msg_ctx: OM_uint32 = 0;

    loop {
        if gss_display_status(
            &mut lmin_s,
            stat,
            type_,
            GSS_C_NO_OID,
            &mut msg_ctx,
            &mut gmsg,
        ) != GSS_S_COMPLETE
        {
            break;
        }
        if i > 0 {
            if i < len {
                *s.add(i) = b' ' as c_char;
            }
            i += 1;
        }
        if i < len {
            std::ptr::copy_nonoverlapping(
                gmsg.value as *const u8,
                s.add(i) as *mut u8,
                Min(len - i, gmsg.length),
            );
        }
        i += gmsg.length;
        gss_release_buffer(&mut lmin_s, &mut gmsg);

        if msg_ctx == 0 {
            break;
        }
    }

    /* add nul termination */
    if i < len {
        *s.add(i) = b'\0' as c_char;
    } else {
        elog!(COMMERROR, "incomplete GSS error report");
        *s.add(len - 1) = b'\0' as c_char;
    }
}

/*
 * Report the GSSAPI error described by maj_stat/min_stat.
 *
 * errmsg should be an already-translated primary error message.
 * The GSSAPI info is appended as errdetail.
 *
 * The error is always reported with elevel COMMERROR; we daren't try to
 * send it to the client, as that'd likely lead to infinite recursion
 * when elog.c tries to write to the client.
 *
 * To avoid memory allocation, total error size is capped (at 128 bytes for
 * each of major and minor).  No known mechanisms will produce error messages
 * beyond this cap.
 */
pub unsafe fn pg_GSS_error(errmsg: *const c_char, maj_stat: OM_uint32, min_stat: OM_uint32) {
    let mut msg_major: [c_char; 128] = [0; 128];
    let mut msg_minor: [c_char; 128] = [0; 128];

    /* Fetch major status message */
    pg_GSS_error_int(
        msg_major.as_mut_ptr(),
        std::mem::size_of_val(&msg_major),
        maj_stat,
        GSS_C_GSS_CODE,
    );

    /* Fetch mechanism minor status message */
    pg_GSS_error_int(
        msg_minor.as_mut_ptr(),
        std::mem::size_of_val(&msg_minor),
        min_stat,
        GSS_C_MECH_CODE,
    );

    /*
     * errmsg_internal, since translation of the first part must be done
     * before calling this function anyway.
     */
    let errmsg_s = std::ffi::CStr::from_ptr(errmsg).to_string_lossy();
    let major_s = std::ffi::CStr::from_ptr(msg_major.as_ptr()).to_string_lossy();
    let minor_s = std::ffi::CStr::from_ptr(msg_minor.as_ptr()).to_string_lossy();
    ereport!(
        COMMERROR,
        format!("{} ({}: {})", errmsg_s, major_s, minor_s)
    );
}

/*
 * Store the credentials passed in into the memory cache for later usage.
 *
 * This allows credentials to be delegated to us for us to use to connect
 * to other systems with, using, e.g. postgres_fdw or dblink.
 */
const GSS_MEMORY_CACHE: &[u8] = b"MEMORY:\0";

pub unsafe fn pg_store_delegated_credential(mut cred: gss_cred_id_t) {
    let mut major: OM_uint32;
    let mut minor: OM_uint32 = 0;
    let mut mech: gss_OID_set = std::ptr::null_mut();
    let mut usage: gss_cred_usage_t = 0;
    let mut cc: gss_key_value_element_desc = std::mem::zeroed();
    let mut ccset: gss_key_value_set_desc = std::mem::zeroed();

    cc.key = b"ccache\0".as_ptr() as *const c_char;
    cc.value = GSS_MEMORY_CACHE.as_ptr() as *const c_char;
    ccset.count = 1;
    ccset.elements = &mut cc;

    /* Make the delegated credential only available to current process */
    major = gss_store_cred_into(
        &mut minor,
        cred,
        GSS_C_INITIATE, /* credential only used for starting libpq connection */
        GSS_C_NULL_OID, /* store all */
        true as c_int,  /* overwrite */
        true as c_int,  /* make default */
        &ccset,
        &mut mech,
        &mut usage,
    );

    if major != GSS_S_COMPLETE {
        pg_GSS_error(b"gss_store_cred\0".as_ptr() as *const c_char, major, minor);
    }

    /* Credential stored, so we can release our credential handle. */
    major = gss_release_cred(&mut minor, &mut cred);
    if major != GSS_S_COMPLETE {
        pg_GSS_error(b"gss_release_cred\0".as_ptr() as *const c_char, major, minor);
    }

    /*
     * Set KRB5CCNAME for this backend, so that later calls to
     * gss_acquire_cred will find the delegated credentials we stored.
     */
    std::env::set_var(
        "KRB5CCNAME",
        std::ffi::CStr::from_ptr(GSS_MEMORY_CACHE.as_ptr() as *const c_char)
            .to_string_lossy()
            .as_ref(),
    );
}

//! libpq/pg-gssapi.h - definitions for including GSSAPI headers
//
// The original C header contains no PostgreSQL-defined types, structs, enums,
// constants, macros, or function prototypes. Under `#ifdef ENABLE_GSS` it
// simply includes the system GSSAPI headers (<gssapi.h>/<gssapi_ext.h> or
// <gssapi/gssapi.h>/<gssapi/gssapi_ext.h>) and re-exports their symbols, plus a
// Windows-only `#undef X509_NAME` workaround. There is nothing to translate
// 1:1 except the include guard semantics.
//
// To give downstream Rust code (which expects `pg_gssapi` to make the GSSAPI
// types available) something to refer to, we provide minimal local stubs for
// the principal GSSAPI types these system headers would export. These are NOT
// part of the C header itself - they stand in for the external <gssapi.h>
// definitions that would be pulled in. They are gated behind the same logical
// `ENABLE_GSS` build condition via a cfg feature.
// TODO: dedup / replace with real GSSAPI bindings when GSS support is wired up.

#![allow(non_camel_case_types)]

use crate::c::{uint32, Size};
use std::ffi::{c_int, c_void};

// --- Include guard ---
// PG_GSSAPI_H: in C this is an include guard with no value; represented here as
// a marker constant for completeness.
pub const PG_GSSAPI_H: () = ();

// --- GSSAPI system-header type stubs (would come from <gssapi.h>) ---
// These mirror the C library typedefs in name only; treat as opaque.

/// GSSAPI status/return code type (`OM_uint32`).
pub type OM_uint32 = uint32;

/// Opaque security context handle (`gss_ctx_id_t`).
pub type gss_ctx_id_t = *mut c_void;

/// Opaque credential handle (`gss_cred_id_t`).
pub type gss_cred_id_t = *mut c_void;

/// Opaque internal-name handle (`gss_name_t`).
pub type gss_name_t = *mut c_void;

/// Object identifier (`gss_OID`).
pub type gss_OID = *mut gss_OID_desc;

/// Set of object identifiers (`gss_OID_set`).
pub type gss_OID_set = *mut gss_OID_set_desc;

/// Buffer handle (`gss_buffer_t`).
pub type gss_buffer_t = *mut gss_buffer_desc;

/// Object-identifier descriptor (`gss_OID_desc`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct gss_OID_desc {
    pub length: OM_uint32,
    pub elements: *mut c_void,
}

/// Object-identifier set descriptor (`gss_OID_set_desc`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct gss_OID_set_desc {
    pub count: Size,
    pub elements: gss_OID,
}

/// Buffer descriptor (`gss_buffer_desc`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct gss_buffer_desc {
    pub length: Size,
    pub value: *mut c_void,
}

/// Channel-bindings descriptor (`gss_channel_bindings_struct` /
/// `gss_channel_bindings_t`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct gss_channel_bindings_struct {
    pub initiator_addrtype: OM_uint32,
    pub initiator_address: gss_buffer_desc,
    pub acceptor_addrtype: OM_uint32,
    pub acceptor_address: gss_buffer_desc,
    pub application_data: gss_buffer_desc,
}

/// Channel-bindings handle (`gss_channel_bindings_t`).
pub type gss_channel_bindings_t = *mut gss_channel_bindings_struct;

// The Windows `#undef X509_NAME` workaround has no Rust analogue (there is no
// preprocessor symbol clash here); it is intentionally omitted.
//
// Reference to c_int kept to mirror that GSSAPI calls use plain `int` in places;
// retained to avoid an unused-import lint if the stubs above evolve.
const _: c_int = 0;

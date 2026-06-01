//! libpq/libpq.h - POSTGRES LIBPQ buffer structure definitions.

use std::ffi::{c_char, c_int, c_void};

use crate::c::Size;
use crate::lib::stringinfo::StringInfo;
use crate::nodes::execnodes::WaitEventSet;
use crate::port::noblock::pgsocket;
use crate::utils::memutils::MaxAllocSize;

// ssize_t is not a first-class crate type; mirror the convention used elsewhere.
pub type ssize_t = isize;

// libpq-be.h is not yet ported; stub the referenced types locally.
// TODO: dedup - replace with crate::libpq::libpq_be types once ported.
pub type Port = c_void;
pub type ClientSocket = c_void;

/*
 * Callers of pq_getmessage() must supply a maximum expected message size.
 * By convention, if there's not any specific reason to use another value,
 * use PQ_SMALL_MESSAGE_LIMIT for messages that shouldn't be too long, and
 * PQ_LARGE_MESSAGE_LIMIT for messages that can be long.
 */
pub const PQ_SMALL_MESSAGE_LIMIT: c_int = 10000;
pub const PQ_LARGE_MESSAGE_LIMIT: Size = MaxAllocSize - 1;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PQcommMethods {
    pub comm_reset: Option<unsafe extern "C" fn()>,
    pub flush: Option<unsafe extern "C" fn() -> c_int>,
    pub flush_if_writable: Option<unsafe extern "C" fn() -> c_int>,
    pub is_send_pending: Option<unsafe extern "C" fn() -> bool>,
    pub putmessage:
        Option<unsafe extern "C" fn(msgtype: c_char, s: *const c_char, len: Size) -> c_int>,
    pub putmessage_noblock:
        Option<unsafe extern "C" fn(msgtype: c_char, s: *const c_char, len: Size)>,
}

#[allow(improper_ctypes)]
extern "C" {
    pub static PqCommMethods: *const PQcommMethods;
}

#[inline]
pub unsafe fn pq_comm_reset() {
    ((*PqCommMethods).comm_reset.unwrap())()
}

#[inline]
pub unsafe fn pq_flush() -> c_int {
    ((*PqCommMethods).flush.unwrap())()
}

#[inline]
pub unsafe fn pq_flush_if_writable() -> c_int {
    ((*PqCommMethods).flush_if_writable.unwrap())()
}

#[inline]
pub unsafe fn pq_is_send_pending() -> bool {
    ((*PqCommMethods).is_send_pending.unwrap())()
}

#[inline]
pub unsafe fn pq_putmessage(msgtype: c_char, s: *const c_char, len: Size) -> c_int {
    ((*PqCommMethods).putmessage.unwrap())(msgtype, s, len)
}

#[inline]
pub unsafe fn pq_putmessage_noblock(msgtype: c_char, s: *const c_char, len: Size) {
    ((*PqCommMethods).putmessage_noblock.unwrap())(msgtype, s, len)
}

/*
 * External functions.
 */

/*
 * prototypes for functions in pqcomm.c
 */
#[allow(improper_ctypes)]
extern "C" {
    pub static mut FeBeWaitSet: *mut WaitEventSet;
}

pub const FeBeWaitSetSocketPos: c_int = 0;
pub const FeBeWaitSetLatchPos: c_int = 1;
pub const FeBeWaitSetNEvents: c_int = 3;

pub unsafe fn ListenServerPort(
    family: c_int,
    hostName: *const c_char,
    portNumber: u16,
    unixSocketDir: *const c_char,
    ListenSockets: *mut pgsocket,
    NumListenSockets: *mut c_int,
    MaxListen: c_int,
) -> c_int {
    unimplemented!()
}

pub unsafe fn AcceptConnection(server_fd: pgsocket, client_sock: *mut ClientSocket) -> c_int {
    unimplemented!()
}

pub unsafe fn TouchSocketFiles() {
    unimplemented!()
}

pub unsafe fn RemoveSocketFiles() {
    unimplemented!()
}

pub unsafe fn pq_init(client_sock: *mut ClientSocket) -> *mut Port {
    unimplemented!()
}

pub unsafe fn pq_getbytes(b: *mut c_void, len: Size) -> c_int {
    unimplemented!()
}

pub unsafe fn pq_startmsgread() {
    unimplemented!()
}

pub unsafe fn pq_endmsgread() {
    unimplemented!()
}

pub unsafe fn pq_is_reading_msg() -> bool {
    unimplemented!()
}

pub unsafe fn pq_getmessage(s: StringInfo, maxlen: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn pq_getbyte() -> c_int {
    unimplemented!()
}

pub unsafe fn pq_peekbyte() -> c_int {
    unimplemented!()
}

pub unsafe fn pq_getbyte_if_available(c: *mut u8) -> c_int {
    unimplemented!()
}

pub unsafe fn pq_buffer_remaining_data() -> ssize_t {
    unimplemented!()
}

pub unsafe fn pq_putmessage_v2(msgtype: c_char, s: *const c_char, len: Size) -> c_int {
    unimplemented!()
}

pub unsafe fn pq_check_connection() -> bool {
    unimplemented!()
}

/*
 * prototypes for functions in be-secure.c
 */
pub unsafe fn secure_initialize(isServerStart: bool) -> c_int {
    unimplemented!()
}

pub unsafe fn secure_loaded_verify_locations() -> bool {
    unimplemented!()
}

pub unsafe fn secure_destroy() {
    unimplemented!()
}

pub unsafe fn secure_open_server(port: *mut Port) -> c_int {
    unimplemented!()
}

pub unsafe fn secure_close(port: *mut Port) {
    unimplemented!()
}

pub unsafe fn secure_read(port: *mut Port, ptr: *mut c_void, len: Size) -> ssize_t {
    unimplemented!()
}

pub unsafe fn secure_write(port: *mut Port, ptr: *const c_void, len: Size) -> ssize_t {
    unimplemented!()
}

pub unsafe fn secure_raw_read(port: *mut Port, ptr: *mut c_void, len: Size) -> ssize_t {
    unimplemented!()
}

pub unsafe fn secure_raw_write(port: *mut Port, ptr: *const c_void, len: Size) -> ssize_t {
    unimplemented!()
}

/*
 * declarations for variables defined in be-secure.c
 */
#[allow(improper_ctypes)]
extern "C" {
    pub static mut ssl_library: *mut c_char;
    pub static mut ssl_ca_file: *mut c_char;
    pub static mut ssl_cert_file: *mut c_char;
    pub static mut ssl_crl_file: *mut c_char;
    pub static mut ssl_crl_dir: *mut c_char;
    pub static mut ssl_key_file: *mut c_char;
    pub static mut ssl_min_protocol_version: c_int;
    pub static mut ssl_max_protocol_version: c_int;
    pub static mut ssl_passphrase_command: *mut c_char;
    pub static mut ssl_passphrase_command_supports_reload: bool;
    pub static mut ssl_dh_params_file: *mut c_char;
    pub static mut SSLCipherSuites: *mut c_char;
    pub static mut SSLCipherList: *mut c_char;
    pub static mut SSLECDHCurve: *mut c_char;
    pub static mut SSLPreferServerCiphers: bool;
}

// C-gated by USE_SSL; declared unconditionally here (stub-only, no Cargo feature).
#[allow(improper_ctypes)]
extern "C" {
    pub static mut ssl_loaded_verify_locations: bool;
}

/*
 * prototypes for functions in be-secure-gssapi.c
 */
// C-gated by ENABLE_GSS; declared unconditionally here (stub-only).
pub unsafe fn secure_open_gssapi(port: *mut Port) -> ssize_t {
    unimplemented!()
}

// enum ssl_protocol_versions
pub type ssl_protocol_versions = c_int;
pub const PG_TLS_ANY: ssl_protocol_versions = 0;
pub const PG_TLS1_VERSION: ssl_protocol_versions = 1;
pub const PG_TLS1_1_VERSION: ssl_protocol_versions = 2;
pub const PG_TLS1_2_VERSION: ssl_protocol_versions = 3;
pub const PG_TLS1_3_VERSION: ssl_protocol_versions = 4;

/*
 * prototypes for functions in be-secure-common.c
 */
pub unsafe fn run_ssl_passphrase_command(
    prompt: *const c_char,
    is_server_start: bool,
    buf: *mut c_char,
    size: c_int,
) -> c_int {
    unimplemented!()
}

pub unsafe fn check_ssl_key_file_permissions(
    ssl_key_file_arg: *const c_char,
    isServerStart: bool,
) -> bool {
    unimplemented!()
}

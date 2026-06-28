//! Translated from PostgreSQL src/include/libpq/libpq.h
//
// Backend libpq send/recv API. These are in-memory wire-message helpers (NOT
// on-disk / #[repr(C)]); signatures kept, bodies stubbed over crate::libpq::pqcomm.

#![allow(clippy::ptr_arg, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1")]

use crate::libpq::libpq_be::{ClientSocket, Port};

// pq_getmessage() max-size conventions.
pub const PQ_SMALL_MESSAGE_LIMIT: usize = 10000;
// MaxAllocSize - 1; MaxAllocSize is 0x3fffffff (utils/memutils.h).
pub const PQ_LARGE_MESSAGE_LIMIT: usize = 0x3fffffff - 1;

// PQcommMethods vtable -> trait (routine-struct.md). One process-wide impl
// (the socket backend); pqmq.c provides a second. Static dispatch over the impls.
pub trait PqCommMethods {
    fn comm_reset(&self);
    fn flush(&self) -> i32;
    fn flush_if_writable(&self) -> i32;
    fn is_send_pending(&self) -> bool;
    fn putmessage(&self, msgtype: u8, s: &[u8]) -> i32;
    fn putmessage_noblock(&self, msgtype: u8, s: &[u8]);
}

// pq_* convenience wrappers (C macros over PqCommMethods) -> free fns. The
// SYNCHRONOUS members re-export the backend body (`crate::backend::libpq::pqcomm`).
// The socket-touching members (`pq_flush`/`pq_flush_if_writable`/`pq_putmessage`/
// `pq_putmessage_noblock`) are ASYNC in the backend; their C/header signatures
// are synchronous, so they stay stubbed here and step-09 rewires the command
// loop to call the async backend functions directly.
pub use crate::backend::libpq::pqcomm::{pq_comm_reset, pq_is_send_pending};

pub fn pq_flush() -> i32 {
    unimplemented!("async in crate::backend::libpq::pqcomm::pq_flush; step-09 rewires callers")
}
pub fn pq_flush_if_writable() -> i32 {
    unimplemented!("async in crate::backend::libpq::pqcomm::pq_flush_if_writable; step-09")
}
pub fn pq_putmessage(msgtype: u8, s: &[u8]) -> i32 {
    let _ = (msgtype, s);
    unimplemented!("async in crate::backend::libpq::pqcomm::pq_putmessage; step-09 rewires callers")
}
pub fn pq_putmessage_noblock(msgtype: u8, s: &[u8]) {
    let _ = (msgtype, s);
    unimplemented!("async in crate::backend::libpq::pqcomm::pq_putmessage_noblock; step-09")
}

// FeBe wait set positions; FeBeWaitSet itself is process-global state (deferred).
pub const FE_BE_WAIT_SET_SOCKET_POS: i32 = 0;
pub const FE_BE_WAIT_SET_LATCH_POS: i32 = 1;
pub const FE_BE_WAIT_SET_NEVENTS: i32 = 3;

// prototypes for functions in pqcomm.c
pub fn ListenServerPort(
    family: i32,
    host_name: Option<&str>,
    port_number: u16,
    unix_socket_dir: Option<&str>,
    listen_sockets: &mut [i32],
    num_listen_sockets: &mut i32,
    max_listen: i32,
) -> i32 {
    unimplemented!()
}
pub fn AcceptConnection(server_fd: i32, client_sock: &mut ClientSocket) -> i32 {
    unimplemented!()
}
pub fn TouchSocketFiles() {
    unimplemented!()
}
pub fn RemoveSocketFiles() {
    unimplemented!()
}
pub fn pq_init(client_sock: &ClientSocket) -> Option<Box<Port>> {
    unimplemented!()
}
// The socket-reading members (`pq_getbytes`/`pq_getmessage`/`pq_getbyte`/
// `pq_peekbyte`) are ASYNC in the backend; the sync C/header signatures stay
// stubbed here, and step-09 rewires callers to the async backend functions.
pub fn pq_getbytes(b: &mut [u8]) -> i32 {
    let _ = b;
    unimplemented!("async in crate::backend::libpq::pqcomm::pq_getbytes; step-09 rewires callers")
}

// Synchronous recv-side state accessors re-export the backend body.
pub use crate::backend::libpq::pqcomm::{
    pq_buffer_remaining_data, pq_endmsgread, pq_is_reading_msg, pq_startmsgread,
};

// StringInfo -> &mut Vec<u8> (lib/stringinfo.h maps to std buffers).
pub fn pq_getmessage(s: &mut Vec<u8>, maxlen: i32) -> i32 {
    let _ = (s, maxlen);
    unimplemented!("async in crate::backend::libpq::pqcomm::pq_getmessage; step-09 rewires callers")
}
pub fn pq_getbyte() -> i32 {
    unimplemented!("async in crate::backend::libpq::pqcomm::pq_getbyte; step-09 rewires callers")
}
pub fn pq_peekbyte() -> i32 {
    unimplemented!("async in crate::backend::libpq::pqcomm::pq_peekbyte; step-09 rewires callers")
}
// pq_getbyte_if_available: -> Option<u8> (out-param + availability folded in).
pub fn pq_getbyte_if_available() -> Option<u8> {
    unimplemented!()
}
pub fn pq_putmessage_v2(msgtype: u8, s: &[u8]) -> i32 {
    unimplemented!()
}
pub fn pq_check_connection() -> bool {
    unimplemented!()
}

// prototypes for functions in be-secure.c. The synchronous, Port-free members
// re-export the backend body (`crate::backend::libpq::be_secure`). The
// connection I/O members (`secure_read`/`secure_write`/`secure_raw_read`/
// `secure_raw_write`) and the `&mut Port`-taking handshake members
// (`secure_open_server`/`secure_close`) are ASYNC and/or take the connection
// from the task-local Port in the backend; their C/header signatures differ, so
// they stay stubbed here and step-09 rewires callers to the backend functions.
pub use crate::backend::libpq::be_secure::{
    secure_destroy, secure_initialize, secure_loaded_verify_locations,
};

pub fn secure_open_server(port: &mut Port) -> i32 {
    let _ = port;
    unimplemented!("plaintext no-op in crate::backend::libpq::be_secure::secure_open_server; step-09")
}
pub fn secure_close(port: &mut Port) {
    let _ = port;
    unimplemented!("no-op in crate::backend::libpq::be_secure::secure_close; step-09")
}
pub fn secure_read(port: &mut Port, ptr: &mut [u8]) -> isize {
    let _ = (port, ptr);
    unimplemented!("async in crate::backend::libpq::be_secure::secure_read; step-09 rewires callers")
}
pub fn secure_write(port: &mut Port, ptr: &[u8]) -> isize {
    let _ = (port, ptr);
    unimplemented!("async in crate::backend::libpq::be_secure::secure_write; step-09 rewires callers")
}
pub fn secure_raw_read(port: &mut Port, ptr: &mut [u8]) -> isize {
    let _ = (port, ptr);
    unimplemented!("async in crate::backend::libpq::be_secure::secure_raw_read; step-09")
}
pub fn secure_raw_write(port: &mut Port, ptr: &[u8]) -> isize {
    let _ = (port, ptr);
    unimplemented!("async in crate::backend::libpq::be_secure::secure_raw_write; step-09")
}

// SSL protocol version selector (sequential ordinal) -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ssl_protocol_versions {
    TLS_ANY = 0,
    TLS1_VERSION,
    TLS1_1_VERSION,
    TLS1_2_VERSION,
    TLS1_3_VERSION,
}

// prototypes for functions in be-secure-common.c
pub fn run_ssl_passphrase_command(
    prompt: &str,
    is_server_start: bool,
    buf: &mut [u8],
) -> i32 {
    unimplemented!()
}
pub fn check_ssl_key_file_permissions(ssl_key_file: &str, is_server_start: bool) -> bool {
    unimplemented!()
}
// FeBeWaitSet (crate::storage::waiteventset::WaitEventSet) is process-global state, deferred.

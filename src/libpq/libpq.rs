//! Translated from PostgreSQL src/include/libpq/libpq.h
//
// Backend libpq send/recv API. These are in-memory wire-message helpers (NOT
// on-disk / #[repr(C)]); signatures kept, bodies stubbed over crate::libpq::pqcomm.

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

// pq_* convenience wrappers (C macros over PqCommMethods) -> free fns.
pub fn pq_comm_reset() {
    unimplemented!()
}
pub fn pq_flush() -> i32 {
    unimplemented!()
}
pub fn pq_flush_if_writable() -> i32 {
    unimplemented!()
}
pub fn pq_is_send_pending() -> bool {
    unimplemented!()
}
pub fn pq_putmessage(msgtype: u8, s: &[u8]) -> i32 {
    unimplemented!()
}
pub fn pq_putmessage_noblock(msgtype: u8, s: &[u8]) {
    unimplemented!()
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
pub fn pq_getbytes(b: &mut [u8]) -> i32 {
    unimplemented!()
}
pub fn pq_startmsgread() {
    unimplemented!()
}
pub fn pq_endmsgread() {
    unimplemented!()
}
pub fn pq_is_reading_msg() -> bool {
    unimplemented!()
}
// StringInfo -> &mut Vec<u8> (lib/stringinfo.h maps to std buffers).
pub fn pq_getmessage(s: &mut Vec<u8>, maxlen: i32) -> i32 {
    unimplemented!()
}
pub fn pq_getbyte() -> i32 {
    unimplemented!()
}
pub fn pq_peekbyte() -> i32 {
    unimplemented!()
}
// pq_getbyte_if_available: -> Option<u8> (out-param + availability folded in).
pub fn pq_getbyte_if_available() -> Option<u8> {
    unimplemented!()
}
pub fn pq_buffer_remaining_data() -> isize {
    unimplemented!()
}
pub fn pq_putmessage_v2(msgtype: u8, s: &[u8]) -> i32 {
    unimplemented!()
}
pub fn pq_check_connection() -> bool {
    unimplemented!()
}

// prototypes for functions in be-secure.c
pub fn secure_initialize(is_server_start: bool) -> i32 {
    unimplemented!()
}
pub fn secure_loaded_verify_locations() -> bool {
    unimplemented!()
}
pub fn secure_destroy() {
    unimplemented!()
}
pub fn secure_open_server(port: &mut Port) -> i32 {
    unimplemented!()
}
pub fn secure_close(port: &mut Port) {
    unimplemented!()
}
pub fn secure_read(port: &mut Port, ptr: &mut [u8]) -> isize {
    unimplemented!()
}
pub fn secure_write(port: &mut Port, ptr: &[u8]) -> isize {
    unimplemented!()
}
pub fn secure_raw_read(port: &mut Port, ptr: &mut [u8]) -> isize {
    unimplemented!()
}
pub fn secure_raw_write(port: &mut Port, ptr: &[u8]) -> isize {
    unimplemented!()
}

// SSL protocol version selector (sequential ordinal) -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ssl_protocol_versions {
    PG_TLS_ANY = 0,
    PG_TLS1_VERSION,
    PG_TLS1_1_VERSION,
    PG_TLS1_2_VERSION,
    PG_TLS1_3_VERSION,
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

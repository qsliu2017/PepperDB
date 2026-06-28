//! PG `src/backend/libpq/be-secure.c` -- setting up a secure connection to the
//! frontend.
//!
//! Single-process port (M1): TLS and GSSAPI are not implemented, so the secure
//! layer is a thin plaintext passthrough to the tokio socket owned by the
//! task-local [`crate::backend::libpq::pqcomm`] state. The SSL/GSS branches of
//! the C originals are `deleted by redesign:` (no `port->ssl_in_use` /
//! `port->gss->enc` path exists yet); the `be_tls_*` / `be_gssapi_*` leaves
//! remain `unimplemented!()` in `libpq-be.rs`.
//!
//! The C `secure_read`/`secure_write` open-code a non-blocking socket read plus
//! a `WaitEventSet` sleep on the FeBeWaitSet to implement interruptible blocking
//! I/O. Under tokio the blocking is the `.await` on the socket itself, so the
//! WaitEventSet retry loop is `deleted by redesign:` -- we await the socket
//! directly. `secure_raw_read`/`secure_raw_write` are the actual socket-I/O
//! leaves; here they delegate to the task-local stream via `pqcomm`.
//!
//! These functions are `async` because the socket read/write is the async leaf
//! (rules.md s5). They consume the `raw_buf` "unread" data (used by the C SSL
//! startup to push back bytes read before the handshake) before touching the
//! socket, matching `secure_raw_read`.

use crate::backend::libpq::pqcomm;

/// PG `secure_initialize`. No SSL context to build in M1.
pub fn secure_initialize(_is_server_start: bool) -> i32 {
    0
}

/// PG `secure_destroy`. No SSL context to tear down.
pub fn secure_destroy() {}

/// PG `secure_loaded_verify_locations`. No CA store loaded without SSL.
pub fn secure_loaded_verify_locations() -> bool {
    false
}

/// PG `secure_open_server`. With TLS/GSS deferred this is a no-op success: the
/// connection stays in plaintext. The C body's buffered-data pushback + TLS
/// handshake is `deleted by redesign:` until SSL lands.
pub fn secure_open_server() -> i32 {
    0
}

/// PG `secure_close`. No secure session to close in plaintext mode.
pub fn secure_close() {}

/// PG `secure_read`. Read up to `buf.len()` bytes from the connection into
/// `buf`, returning the number of bytes read (0 = EOF, `< 0` not used here --
/// errors are surfaced as `Err`).
///
/// The C function runs `ProcessClientReadInterrupt` around the read and, in
/// blocking mode, sleeps on the FeBeWaitSet until the socket is readable. Under
/// tokio the `.await` provides the interruptible blocking, so the WaitEventSet
/// loop is `deleted by redesign:`; interrupt delivery happens through the
/// per-task `ProcSignal` latch arm at the command loop's `select!`, not here.
pub async fn secure_read(buf: &mut [u8]) -> std::io::Result<usize> {
    secure_raw_read(buf).await
}

/// PG `secure_raw_read`. The socket-I/O leaf for reads. Consumes any `raw_buf`
/// pushback first (c.f. libpq-be.h), then performs one socket read.
pub async fn secure_raw_read(buf: &mut [u8]) -> std::io::Result<usize> {
    if let Some(n) = pqcomm::consume_raw_buf(buf) {
        return Ok(n);
    }
    pqcomm::socket_read(buf).await
}

/// PG `secure_write`. Write `buf` to the connection, returning bytes written.
/// As in `secure_read`, the FeBeWaitSet blocking-retry loop is replaced by the
/// `.await` on the socket write (`deleted by redesign:` WaitEventSet).
pub async fn secure_write(buf: &[u8]) -> std::io::Result<usize> {
    secure_raw_write(buf).await
}

/// PG `secure_raw_write`. The socket-I/O leaf for writes.
pub async fn secure_raw_write(buf: &[u8]) -> std::io::Result<usize> {
    pqcomm::socket_write(buf).await
}

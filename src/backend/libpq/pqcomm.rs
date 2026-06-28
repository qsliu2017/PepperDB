//! PG `src/backend/libpq/pqcomm.c` -- low-level FE/BE communication.
//!
//! These routines shove framed bytes across the connection, ignorant of message
//! semantics: a caller builds a message with `pqformat` and emits it in one
//! `pq_putmessage`; reads come in through `pq_getmessage` and friends.
//!
//! ## State model (rules.md s1/s6.1/s8)
//!
//! PostgreSQL keeps the connection in a per-backend process-global `MyProcPort`
//! ([`Port`]) plus a set of per-backend `static` byte buffers
//! (`PqRecvBuffer`/`PqRecvPointer`/`PqRecvLength`, `PqSendBuffer`/`PqSendPointer`
//! /`PqSendStart`) and status flags (`PqCommBusy`/`PqCommReadingMsg`). In this
//! single-process tokio port each backend is a task, so all of that becomes
//! per-task state published as a task-local [`PQ_COMM`] (the sibling of the
//! per-task [`crate::session::Session`]).
//!
//! [`PqComm`] splits into two parts so the lock-across-await invariant is
//! structural:
//! - `socket`: the async I/O leaf -- a `tokio::sync::Mutex` over the boxed
//!   stream. The socket read/write IS the `.await`, so it is legitimately held
//!   across `.await` (an async-aware lock; rules.md s5).
//! - `state`: a `parking_lot::Mutex<PqCommState>` holding the buffers, the
//!   cursors, the busy/reading flags, and the [`Port`] metadata (`raw_buf`
//!   pushback). This is the "receive buffers = per-task state" pattern of
//!   rules.md s8; a `parking_lot::Mutex` rather than a `RefCell` so the future
//!   stays `Send` for the multi-thread runtime (a `RefCell` is `!Sync`, which
//!   would make `Arc<PqComm>` `!Send` and the holding future `!Send` -- the same
//!   reason `Session` uses atomics + `parking_lot::Mutex`). Only the owning task
//!   ever locks it.
//!
//! ## The hard invariant (rules.md s5/s8)
//!
//! The `state` guard is NEVER held across an `.await`. Every async routine here
//! locks `state`, copies/decides what it needs, DROPS the guard, THEN awaits the
//! socket, then re-locks to store the result. Clippy `await_holding_lock = deny`
//! enforces this. The socket `Mutex` and the `state` `Mutex` are never locked at
//! the same time.
//!
//! ## Async coloring (rules.md s5)
//!
//! Anything that can touch the socket is `async`: `pq_recvbuf`, `pq_getbyte`,
//! `pq_getbytes`, `pq_getmessage`, `pq_putmessage`, `pq_flush`,
//! `internal_flush`. Pure buffer/flag accessors (`pq_startmsgread`,
//! `pq_endmsgread`, `pq_is_reading_msg`, `pq_buffer_remaining_data`) stay
//! synchronous.
//!
//! ## Scope (rules.md s1, redesign)
//!
//! `ListenServerPort`/`AcceptConnection`/`Lock_AF_UNIX`/`Setup_AF_UNIX`/
//! `TouchSocketFiles`/`RemoveSocketFiles` (the postmaster listen-socket setup)
//! are owned by the foundation's tokio `TcpListener` in postmaster.rs and are
//! `deleted by redesign:` here. `socket_set_nonblocking` is N/A (tokio sockets
//! are always non-blocking; blocking is the `.await`). The TCP-keepalive
//! getters/setters and GUC hooks operate on the OS socket directly and are kept
//! as no-op stubs in `libpq-be.rs` until the postmaster wires real keepalives.

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;

use crate::backend::libpq::be_secure;
use crate::ereport;
use crate::libpq::pqcomm::MAX_STARTUP_PACKET_LENGTH;
use crate::utils::elog::{COMMERROR, FATAL};
use crate::utils::errcodes::ERRCODE_PROTOCOL_VIOLATION;

/// PG `PQ_SEND_BUFFER_SIZE`.
const PQ_SEND_BUFFER_SIZE: usize = 8192;
/// PG `PQ_RECV_BUFFER_SIZE`.
const PQ_RECV_BUFFER_SIZE: usize = 8192;

/// PG `EOF` sentinel. The C low-level routines return `EOF` (-1) on trouble;
/// the translated routines return `Result<_, RecvError>` instead, but the
/// `pq_getbyte`-style "byte or EOF" callers still want a sentinel.
pub const EOF: i32 = -1;

/// The boxed backend socket. A trait object so pqcomm is decoupled from the
/// concrete stream type (a `tokio::net::TcpStream` in production, a
/// `tokio::io::DuplexStream` / `UnixStream` in tests) and self-contained.
pub trait BackendStream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> BackendStream for T {}

/// Mutable per-task comm state: the buffers, cursors, flags, and the slice of
/// [`Port`](crate::libpq::libpq_be::Port) metadata pqcomm touches. Guarded by a
/// `parking_lot::Mutex`; the guard is never held across an `.await`.
struct PqCommState {
    // --- receive buffer (PG PqRecvBuffer/PqRecvPointer/PqRecvLength) ---
    recv_buffer: Box<[u8; PQ_RECV_BUFFER_SIZE]>,
    /// Next index to read a byte from `recv_buffer` (PG `PqRecvPointer`).
    recv_pointer: usize,
    /// End of data available in `recv_buffer` (PG `PqRecvLength`).
    recv_length: usize,

    // --- send buffer (PG PqSendBuffer/PqSendPointer/PqSendStart) ---
    /// Usually 8k, can grow for an oversized noblock message (PG `PqSendBuffer`).
    send_buffer: Vec<u8>,
    /// Next index to store a byte in `send_buffer` (PG `PqSendPointer`).
    send_pointer: usize,
    /// Next index to send a byte from `send_buffer` (PG `PqSendStart`).
    send_start: usize,

    // --- status flags ---
    /// PG `PqCommBusy`: busy sending data to the client.
    comm_busy: bool,
    /// PG `PqCommReadingMsg`: in the middle of reading a message.
    comm_reading_msg: bool,

    // --- Port "unread" pushback (libpq-be.h raw_buf), used by SSL setup ---
    raw_buf: Option<Vec<u8>>,
    raw_buf_consumed: usize,
}

impl PqCommState {
    fn new() -> Self {
        Self {
            recv_buffer: Box::new([0u8; PQ_RECV_BUFFER_SIZE]),
            recv_pointer: 0,
            recv_length: 0,
            send_buffer: Vec::with_capacity(PQ_SEND_BUFFER_SIZE),
            send_pointer: 0,
            send_start: 0,
            comm_busy: false,
            comm_reading_msg: false,
            raw_buf: None,
            raw_buf_consumed: 0,
        }
    }
}

/// Per-task connection state: the async socket plus the buffer/flag state. The
/// per-task analog of PG's `MyProcPort` + the pqcomm `static` buffers.
pub struct PqComm {
    /// The async I/O leaf. A `tokio::sync::Mutex` so the guard may be held
    /// across the socket `.await` (rules.md s5). Only the owning task locks it.
    socket: AsyncMutex<Box<dyn BackendStream>>,
    /// The buffers/flags/Port metadata. A `parking_lot::Mutex` (NOT `RefCell`):
    /// the guard is `!Send` but is never held across an `.await` (lock, decide,
    /// drop, then await), so the future stays `Send` for the multi-thread
    /// runtime -- the same Send discipline `Session` follows. `await_holding_lock`
    /// = deny enforces the drop-before-await invariant (rules.md s5/s8).
    state: Mutex<PqCommState>,
}

impl PqComm {
    /// Build a comm layer over `stream`. PG `pq_init` (minus the socket-option /
    /// WaitEventSet setup, which is `deleted by redesign:`): allocate the Port +
    /// buffers and zero the cursors. The keepalive/NODELAY socket options and
    /// the `FeBeWaitSet` are postmaster/tokio concerns, not done here.
    pub fn new<S: BackendStream + 'static>(stream: S) -> Self {
        Self {
            socket: AsyncMutex::new(Box::new(stream)),
            state: Mutex::new(PqCommState::new()),
        }
    }
}

tokio::task_local! {
    /// The current task's connection. Published by [`scope`] for a backend task,
    /// mirroring how [`crate::session`] publishes the `Session`. `Arc` so the
    /// value can be cloned out for a nested scope; it is not held across an
    /// `.await` as a borrow.
    static PQ_COMM: Arc<PqComm>;
}

/// Run `f` with `comm` published as this task's connection (PG: backend operates
/// with `MyProcPort` set). The step-09 command loop wraps its body in this.
pub async fn scope<F, T>(comm: Arc<PqComm>, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    PQ_COMM.scope(comm, f).await
}

/// This task's connection. Panics if not inside a [`scope`] (a programming
/// error: the low-level routines require an active Port, as PG asserts
/// `MyProcPort != NULL`).
fn comm() -> Arc<PqComm> {
    #[allow(
        clippy::expect_used,
        reason = "documented precondition: caller is inside a scope() task-local, mirroring PG's MyProcPort != NULL"
    )]
    PQ_COMM
        .try_with(std::clone::Clone::clone)
        .expect("no PqComm in scope for this task (MyProcPort is NULL)")
}

/// Whether this task has an active connection (PG `MyProcPort != NULL`).
pub fn has_comm() -> bool {
    PQ_COMM.try_with(|_| ()).is_ok()
}

// ---------------------------------------------------------------------------
// socket leaves (called by be-secure.rs). One socket read / write each.
// ---------------------------------------------------------------------------

/// One socket read into `buf`; returns bytes read (0 = EOF). The actual async
/// leaf behind `secure_raw_read`. Holds only the async socket `Mutex` across
/// `.await` -- never the `state` Mutex.
pub async fn socket_read(buf: &mut [u8]) -> std::io::Result<usize> {
    let comm = comm();
    let mut sock = comm.socket.lock().await;
    sock.read(buf).await
}

/// One socket write of `buf`; returns bytes written. The async leaf behind
/// `secure_raw_write`.
pub async fn socket_write(buf: &[u8]) -> std::io::Result<usize> {
    let comm = comm();
    let mut sock = comm.socket.lock().await;
    sock.write(buf).await
}

/// Consume up to `buf.len()` bytes of the Port `raw_buf` "unread" pushback into
/// `buf`, returning the count, or `None` if there is no pushback. PG
/// `secure_raw_read`'s first branch. Synchronous: touches only the `state` Mutex.
pub fn consume_raw_buf(buf: &mut [u8]) -> Option<usize> {
    let comm = comm();
    let mut st = comm.state.lock();
    let from = st.raw_buf_consumed;
    let raw = st.raw_buf.as_ref()?;
    let avail = raw.len() - from;
    if avail == 0 {
        return None;
    }
    let n = avail.min(buf.len());
    buf[..n].copy_from_slice(&raw[from..from + n]);
    let raw_len = raw.len();
    st.raw_buf_consumed += n;
    if st.raw_buf_consumed >= raw_len {
        st.raw_buf = None;
        st.raw_buf_consumed = 0;
    }
    Some(n)
}

// ---------------------------------------------------------------------------
// Low-level input
// ---------------------------------------------------------------------------

/// PG `pq_recvbuf`: load some bytes into the input buffer. Returns `Ok(())` on
/// success, `Err(())` on EOF/trouble (PG returns 0 / `EOF`).
///
/// Left-justifies any unread data, then does ONE `secure_read` into the tail.
/// The `state` Mutex is locked to compact + compute the read window, DROPPED
/// before the `.await`, and re-locked to commit the read length.
async fn pq_recvbuf() -> Result<(), ()> {
    let comm = comm();

    // Phase 1 (sync): compact the buffer and compute where to read into.
    let read_at = {
        let mut st = comm.state.lock();
        if st.recv_pointer > 0 {
            if st.recv_length > st.recv_pointer {
                // still some unread data, left-justify it in the buffer
                let (from, to) = (st.recv_pointer, st.recv_length);
                st.recv_buffer.copy_within(from..to, 0);
                st.recv_length -= st.recv_pointer;
            } else {
                st.recv_length = 0;
            }
            st.recv_pointer = 0;
        }
        st.recv_length
    }; // borrow dropped here, before the await

    // Phase 2 (async leaf): read into a heap scratch (not a stack array, which
    // would bloat the future), to avoid holding the state lock across the await.
    let want = PQ_RECV_BUFFER_SIZE - read_at;
    let mut scratch = vec![0u8; want];
    let n = loop {
        match be_secure::secure_read(&mut scratch).await {
            Ok(n) => break n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => {
                // Careful: an ereport() to the client would recurse to here;
                // COMMERROR (< ERROR) logs to the server only and returns.
                ereport!(COMMERROR, |e| {
                    e.errcode_for_socket_access()
                        .errmsg("could not receive data from client");
                });
                return Err(());
            }
        }
    };
    if n == 0 {
        return Err(()); // EOF
    }

    // Phase 3 (sync): commit the bytes.
    let mut st = comm.state.lock();
    let dst = st.recv_length;
    st.recv_buffer[dst..dst + n].copy_from_slice(&scratch[..n]);
    st.recv_length += n;
    Ok(())
}

/// PG `pq_getbyte`: get a single byte from the connection, or `EOF`.
pub async fn pq_getbyte() -> i32 {
    crate::assert!(reading_msg());
    loop {
        {
            let comm = comm();
            let mut st = comm.state.lock();
            if st.recv_pointer < st.recv_length {
                let b = st.recv_buffer[st.recv_pointer];
                st.recv_pointer += 1;
                return i32::from(b);
            }
        } // drop borrow before await
        if pq_recvbuf().await.is_err() {
            return EOF;
        }
    }
}

/// PG `pq_peekbyte`: like `pq_getbyte` but does not advance the cursor.
pub async fn pq_peekbyte() -> i32 {
    crate::assert!(reading_msg());
    loop {
        {
            let comm = comm();
            let st = comm.state.lock();
            if st.recv_pointer < st.recv_length {
                return i32::from(st.recv_buffer[st.recv_pointer]);
            }
        }
        if pq_recvbuf().await.is_err() {
            return EOF;
        }
    }
}

/// PG `pq_getbytes`: get a known number of bytes from the connection into `b`.
/// Returns `Ok(())` on success, `Err(())` (PG `EOF`) on trouble.
pub async fn pq_getbytes(b: &mut [u8]) -> Result<(), ()> {
    crate::assert!(reading_msg());
    let mut filled = 0usize;
    while filled < b.len() {
        // Ensure there is buffered data.
        loop {
            let avail = {
                let comm = comm();
                let st = comm.state.lock();
                st.recv_length.saturating_sub(st.recv_pointer)
            };
            if avail > 0 {
                break;
            }
            pq_recvbuf().await?;
        }
        // Copy as much as available, up to the remaining need.
        let comm = comm();
        let mut st = comm.state.lock();
        let avail = st.recv_length - st.recv_pointer;
        let amount = avail.min(b.len() - filled);
        let from = st.recv_pointer;
        b[filled..filled + amount].copy_from_slice(&st.recv_buffer[from..from + amount]);
        st.recv_pointer += amount;
        filled += amount;
    }
    Ok(())
}

/// PG `pq_discardbytes`: throw away a known number of bytes (resync after a read
/// error). Returns `Ok(())`/`Err(())` (PG 0/`EOF`).
async fn pq_discardbytes(mut len: usize) -> Result<(), ()> {
    crate::assert!(reading_msg());
    while len > 0 {
        loop {
            let avail = {
                let comm = comm();
                let st = comm.state.lock();
                st.recv_length.saturating_sub(st.recv_pointer)
            };
            if avail > 0 {
                break;
            }
            pq_recvbuf().await?;
        }
        let comm = comm();
        let mut st = comm.state.lock();
        let amount = (st.recv_length - st.recv_pointer).min(len);
        st.recv_pointer += amount;
        len -= amount;
    }
    Ok(())
}

/// PG `pq_buffer_remaining_data`: bytes already in the receive buffer. Does not
/// read more. Synchronous.
pub fn pq_buffer_remaining_data() -> isize {
    let comm = comm();
    let st = comm.state.lock();
    crate::assert!(st.recv_length >= st.recv_pointer);
    (st.recv_length - st.recv_pointer) as isize
}

/// PG `pq_startmsgread`: begin reading a message. Must precede any `pq_get*`.
/// Synchronous. A read already active means lost protocol sync -> `FATAL`.
pub fn pq_startmsgread() {
    let comm = comm();
    let already = {
        let mut st = comm.state.lock();
        if st.comm_reading_msg {
            true
        } else {
            st.comm_reading_msg = true;
            false
        }
    };
    if already {
        // borrow dropped above, before the raising ereport
        ereport!(FATAL, |e| {
            e.errcode(ERRCODE_PROTOCOL_VIOLATION).errmsg(
                "terminating connection because protocol synchronization was lost",
            );
        });
    }
}

/// PG `pq_endmsgread`: finish reading a message. Synchronous.
pub fn pq_endmsgread() {
    let comm = comm();
    let mut st = comm.state.lock();
    crate::assert!(st.comm_reading_msg);
    st.comm_reading_msg = false;
}

/// PG `pq_is_reading_msg`: are we currently reading a message?
pub fn pq_is_reading_msg() -> bool {
    reading_msg()
}

fn reading_msg() -> bool {
    if !has_comm() {
        return false;
    }
    comm().state.lock().comm_reading_msg
}

/// PG `pq_getmessage`: read a length-prefixed message body into `s` (the length
/// word is stripped; `s` is the StringInfo, here a `Vec<u8>`). `maxlen` caps the
/// accepted size. Returns `Ok(())` / `Err(())` (PG 0 / `EOF`).
///
/// Mirrors the C control flow including the discard-on-overflow resync, but the
/// "ridiculously large message" allocation failure that C catches with
/// `PG_TRY`/`enlargeStringInfo` cannot occur here (a `Vec` grow either succeeds
/// or aborts), so the `maxlen` ceiling alone bounds the allocation.
pub async fn pq_getmessage(s: &mut Vec<u8>, maxlen: usize) -> Result<(), ()> {
    crate::assert!(reading_msg());
    s.clear();

    // Read the 4-byte length word.
    let mut len_buf = [0u8; 4];
    if pq_getbytes(&mut len_buf).await.is_err() {
        ereport!(COMMERROR, |e| {
            e.errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("unexpected EOF within message length word");
        });
        return Err(());
    }
    let len = u32::from_be_bytes(len_buf) as usize;

    if len < 4 || len > maxlen {
        ereport!(COMMERROR, |e| {
            e.errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("invalid message length");
        });
        return Err(());
    }
    let len = len - 4; // discount the length word itself

    if len > 0 {
        s.reserve(len);
        s.resize(len, 0);
        if pq_getbytes(s).await.is_err() {
            ereport!(COMMERROR, |e| {
                e.errcode(ERRCODE_PROTOCOL_VIOLATION)
                    .errmsg("incomplete message from client");
            });
            return Err(());
        }
    }

    // finished reading the message
    {
        let comm = comm();
        comm.state.lock().comm_reading_msg = false;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Low-level output
// ---------------------------------------------------------------------------

/// PG `internal_putbytes`: append `b` to the send buffer, flushing when full.
/// Returns `Ok(())`/`Err(())` (PG 0/`EOF`).
async fn internal_putbytes(b: &[u8]) -> Result<(), ()> {
    // TODO(perf): C internal_putbytes bypasses the copy and sends directly from the source when len >= send-buffer-size and the buffer is empty; we always copy.
    let mut off = 0usize;
    while off < b.len() {
        // If buffer is full, flush it out.
        let full = {
            let comm = comm();
            let st = comm.state.lock();
            st.send_pointer >= st.send_buffer.capacity().max(PQ_SEND_BUFFER_SIZE)
        };
        if full {
            internal_flush().await?;
        }

        let comm = comm();
        let mut st = comm.state.lock();
        let bufsize = st.send_buffer.capacity().max(PQ_SEND_BUFFER_SIZE);
        let amount = (bufsize - st.send_pointer).min(b.len() - off);
        let need = st.send_pointer + amount;
        if st.send_buffer.len() < need {
            st.send_buffer.resize(need, 0);
        }
        let at = st.send_pointer;
        st.send_buffer[at..at + amount].copy_from_slice(&b[off..off + amount]);
        st.send_pointer += amount;
        off += amount;
    }
    Ok(())
}

/// PG `internal_flush` / `internal_flush_buffer`: send the buffered output.
/// Returns `Ok(())`/`Err(())` (PG 0/`EOF`). Drains the send buffer with one or
/// more `secure_write` calls, never holding the `state` Mutex across the `.await`.
async fn internal_flush() -> Result<(), ()> {
    let comm = comm();
    loop {
        // Snapshot the pending window, dropping the borrow before the write.
        let chunk = {
            let st = comm.state.lock();
            if st.send_start >= st.send_pointer {
                None
            } else {
                Some(st.send_buffer[st.send_start..st.send_pointer].to_vec())
            }
        };
        let Some(chunk) = chunk else {
            // Fully sent; reset cursors.
            let mut st = comm.state.lock();
            st.send_start = 0;
            st.send_pointer = 0;
            return Ok(());
        };

        match be_secure::secure_write(&chunk).await {
            Ok(0) => {
                // Treated as a hard send failure (cannot make progress).
                let mut st = comm.state.lock();
                st.send_start = 0;
                st.send_pointer = 0;
                return Err(());
            }
            Ok(n) => {
                comm.state.lock().send_start += n;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => {
                ereport!(COMMERROR, |e| {
                    e.errcode_for_socket_access()
                        .errmsg("could not send data to client");
                });
                let mut st = comm.state.lock();
                st.send_start = 0;
                st.send_pointer = 0;
                return Err(());
            }
        }
    }
}

/// PG `pq_flush` (`socket_flush`): flush pending output. Returns `Ok(())` /
/// `Err(())` (PG 0/`EOF`). No-op on a reentrant (busy) call.
pub async fn pq_flush() -> Result<(), ()> {
    {
        let comm = comm();
        let mut st = comm.state.lock();
        if st.comm_busy {
            return Ok(());
        }
        st.comm_busy = true;
    }
    let res = internal_flush().await;
    comm().state.lock().comm_busy = false;
    res
}

/// PG `pq_flush_if_writable` (`socket_flush_if_writable`): flush only if there
/// is pending data. With tokio the socket is non-blocking under the hood and the
/// write future yields rather than blocking, so this is `pq_flush` gated on
/// pending data.
pub async fn pq_flush_if_writable() -> Result<(), ()> {
    {
        let comm = comm();
        let st = comm.state.lock();
        if st.send_pointer == st.send_start {
            return Ok(()); // nothing to do
        }
        if st.comm_busy {
            return Ok(());
        }
    }
    pq_flush().await
}

/// PG `pq_is_send_pending` (`socket_is_send_pending`): is there buffered output?
pub fn pq_is_send_pending() -> bool {
    let comm = comm();
    let st = comm.state.lock();
    st.send_start < st.send_pointer
}

/// PG `pq_putmessage` (`socket_putmessage`): send a typed, length-prefixed
/// message. A 4-byte length word (`len + 4`) is inserted before the body.
/// Returns `Ok(())`/`Err(())` (PG 0/`EOF`). Suppressed (no-op success) while
/// pqcomm is busy, matching C.
pub async fn pq_putmessage(msgtype: u8, s: &[u8]) -> Result<(), ()> {
    crate::assert!(msgtype != 0);
    {
        let comm = comm();
        let mut st = comm.state.lock();
        if st.comm_busy {
            return Ok(());
        }
        st.comm_busy = true;
    }
    let res = put_message_body(msgtype, s).await;
    comm().state.lock().comm_busy = false;
    res
}

async fn put_message_body(msgtype: u8, s: &[u8]) -> Result<(), ()> {
    internal_putbytes(&[msgtype]).await?;
    let n32 = (s.len() as u32 + 4).to_be_bytes();
    internal_putbytes(&n32).await?;
    internal_putbytes(s).await
}

/// PG `pq_putmessage_noblock` (`socket_putmessage_noblock`): like `pq_putmessage`
/// but enlarges the send buffer so the message always fits without a blocking
/// flush. The growth is implicit here because `internal_putbytes` extends the
/// `Vec`; we still pre-reserve so the message never triggers a mid-write flush.
pub async fn pq_putmessage_noblock(msgtype: u8, s: &[u8]) {
    {
        let comm = comm();
        let mut st = comm.state.lock();
        let required = st.send_pointer + 1 + 4 + s.len();
        let cur_len = st.send_buffer.len();
        if required > st.send_buffer.capacity().max(PQ_SEND_BUFFER_SIZE) {
            st.send_buffer.reserve(required - cur_len);
        }
    }
    // Should not fail when the message fits in the buffer.
    let _ = pq_putmessage(msgtype, s).await;
}

/// PG `pq_comm_reset` (`socket_comm_reset`): reset libpq during error recovery.
/// Keeps pending data, clears the busy flag. Synchronous.
pub fn pq_comm_reset() {
    let comm = comm();
    comm.state.lock().comm_busy = false;
}

// ---------------------------------------------------------------------------
// re-export the startup max for callers that referenced the C symbol via pqcomm
// ---------------------------------------------------------------------------

/// PG `MAX_STARTUP_PACKET_LENGTH`, re-exported for symmetry with the C module.
pub const MAX_STARTUP_PACKET_LEN: usize = MAX_STARTUP_PACKET_LENGTH;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Frame a body the way a client would: type byte + int32(len incl. self) + body.
    fn framed(msgtype: u8, body: &[u8]) -> Vec<u8> {
        let mut v = vec![msgtype];
        v.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
        v.extend_from_slice(body);
        v
    }

    async fn with_comm<S, F, Fut, T>(stream: S, f: F) -> T
    where
        S: BackendStream + 'static,
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        scope(Arc::new(PqComm::new(stream)), f()).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn getmessage_reads_a_framed_message() {
        let (server, mut client) = tokio::io::duplex(1024);
        // client writes: type 'Q', body "SELECT 1\0" (length word is len+4)
        let body = b"SELECT 1\0";
        let writer = tokio::spawn(async move {
            client.write_all(&((body.len() as u32 + 4).to_be_bytes())).await.unwrap();
            client.write_all(body).await.unwrap();
            client.flush().await.unwrap();
        });

        let got = with_comm(server, || async {
            pq_startmsgread();
            let mut msg = Vec::new();
            pq_getmessage(&mut msg, MAX_STARTUP_PACKET_LEN).await.unwrap();
            msg
        })
        .await;
        writer.await.unwrap();
        assert_eq!(got, body);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn getmessage_reassembles_split_body() {
        let (server, mut client) = tokio::io::duplex(64);
        let body = b"hello world this is split";
        let writer = tokio::spawn(async move {
            // length word first
            client.write_all(&((body.len() as u32 + 4).to_be_bytes())).await.unwrap();
            client.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            // body in two halves across two writes
            client.write_all(&body[..10]).await.unwrap();
            client.flush().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            client.write_all(&body[10..]).await.unwrap();
            client.flush().await.unwrap();
        });
        let got = with_comm(server, || async {
            pq_startmsgread();
            let mut msg = Vec::new();
            pq_getmessage(&mut msg, 10000).await.unwrap();
            msg
        })
        .await;
        writer.await.unwrap();
        assert_eq!(got, body);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn getbyte_and_getbytes() {
        let (server, mut client) = tokio::io::duplex(1024);
        let writer = tokio::spawn(async move {
            client.write_all(b"ABCDE").await.unwrap();
            client.flush().await.unwrap();
        });
        let (first, rest) = with_comm(server, || async {
            pq_startmsgread();
            let first = pq_getbyte().await;
            let mut rest = [0u8; 4];
            pq_getbytes(&mut rest).await.unwrap();
            (first, rest)
        })
        .await;
        writer.await.unwrap();
        assert_eq!(first, i32::from(b'A'));
        assert_eq!(&rest, b"BCDE");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn putmessage_then_flush_produces_exact_bytes() {
        let (server, mut client) = tokio::io::duplex(1024);
        let reader = tokio::spawn(async move {
            let mut buf = Vec::new();
            // read exactly the framed bytes we expect: 'R' + len + body
            let mut hdr = [0u8; 5];
            client.read_exact(&mut hdr).await.unwrap();
            buf.extend_from_slice(&hdr);
            let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
            let mut body = vec![0u8; len - 4];
            client.read_exact(&mut body).await.unwrap();
            buf.extend_from_slice(&body);
            buf
        });
        with_comm(server, || async {
            pq_putmessage(b'R', b"\x00\x00\x00\x00").await.unwrap();
            pq_flush().await.unwrap();
        })
        .await;
        let got = reader.await.unwrap();
        assert_eq!(got, framed(b'R', b"\x00\x00\x00\x00"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_drains_large_message_across_socket_buffer() {
        // duplex capacity smaller than the message forces multiple secure_write.
        let (server, mut client) = tokio::io::duplex(64);
        let body = vec![0xABu8; 5000];
        let body2 = body.clone();
        let reader = tokio::spawn(async move {
            let mut hdr = [0u8; 5];
            client.read_exact(&mut hdr).await.unwrap();
            let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
            let mut got = vec![0u8; len - 4];
            client.read_exact(&mut got).await.unwrap();
            (hdr[0], got)
        });
        with_comm(server, || async {
            pq_putmessage(b'D', &body2).await.unwrap();
            pq_flush().await.unwrap();
        })
        .await;
        let (ty, got) = reader.await.unwrap();
        assert_eq!(ty, b'D');
        assert_eq!(got, body);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn buffer_remaining_data_does_not_overread() {
        let (server, mut client) = tokio::io::duplex(1024);
        let writer = tokio::spawn(async move {
            client.write_all(b"XY").await.unwrap();
            client.flush().await.unwrap();
        });
        let remaining = with_comm(server, || async {
            pq_startmsgread();
            // pull one byte to force a recv of both
            let _ = pq_getbyte().await;
            pq_buffer_remaining_data()
        })
        .await;
        writer.await.unwrap();
        assert_eq!(remaining, 1);
    }
}

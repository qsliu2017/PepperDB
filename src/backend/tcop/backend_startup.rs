//! Translated from PostgreSQL src/backend/tcop/backend_startup.c
//!
//! The per-connection backend entry. In PG this is `BackendMain` -> startup
//! packet read + auth + handoff to `PostgresMain`. Under the single-process
//! async model the supervisor (postmaster.rs) spawns one task per accepted
//! connection running [`backend_main`]; there is no fork/exec and no
//! startup-data marshalling -- the socket and shared state are passed by value.
//!
//! Part B replaces Part A's placeholder body with the real path: read the
//! startup packet directly off the socket (pqcomm is deferred, so framing is
//! one-shot tokio I/O), handle SSL/GSS negotiation and cancel requests, register
//! the per-task proc-signal slot, then run [`crate::backend::tcop::postgres`]'s
//! `PostgresMain` command loop nested inside the three per-task task-local scopes
//! (Session, proc-signal slot, resource owner). The supervisor's per-child
//! `cancel` Notify is selected against so a shutdown raises ProcDie.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;

use crate::backend::postmaster::postmaster::ChildKey;
use crate::backend::storage::ipc::procsignal::{self, ProcSignalSlot};
use crate::backend::utils::init::postinit::backend_task_init;
use crate::backend::utils::resowner::resowner::{self, ResourceOwner};
use crate::libpq::pqcomm::{CANCEL_REQUEST_CODE, NEGOTIATE_GSS_CODE, NEGOTIATE_SSL_CODE};
use crate::miscadmin::BackendType;
use crate::session::{self, Session};
use crate::shared_state::SharedState;
use crate::storage::latch::Latch;
use crate::storage::procsignal::MAX_CANCEL_KEY_LENGTH;

/// Hard cap on the startup-packet body (PG `MAX_STARTUP_PACKET_LENGTH`). A
/// larger length is rejected as a protocol violation rather than allocating it.
const MAX_STARTUP_PACKET_LENGTH: usize = 10000;

/// Per-connection backend entry. The supervisor spawns this (wrapped in
/// `catch_unwind`) for every accepted connection.
///
/// Parameters:
/// - `stream`: the accepted client socket (owned; the task drives all I/O).
/// - `peer`: the client's address, for logging / `Port`-equivalent identity.
/// - `shared`: the `Arc<SharedState>` for this task's access to shared
///   subsystems; the proc-signal slot is registered via `shared.proc_signal()`.
/// - `identity`: the supervisor's child-registry key (opaque; logging only).
/// - `cancel`: the supervisor's per-child termination Notify (PG's SIGTERM
///   target). The command loop selects on it to raise ProcDie on shutdown.
pub async fn backend_main(
    mut stream: TcpStream,
    peer: SocketAddr,
    shared: Arc<SharedState>,
    identity: ChildKey,
    cancel: Arc<Notify>,
) {
    let _ = identity;
    crate::elog!(crate::utils::elog::LOG, format!("backend connected: {peer}"));

    // Preserve Part A's observation hook so the supervisor's accept/spawn/panic
    // tests keep working (they assert on CONNECTED / PANIC_ON_CONNECT).
    #[cfg(test)]
    test_hook::on_backend_connected(peer);

    // Identity slice (step 08): a Session with a synthetic proc-pid. No catalog,
    // auth, or proc-array access.
    let session = backend_task_init(BackendType::BACKEND).await;
    let proc_pid = session.proc_pid();

    // Read the startup packet (handling SSL/GSS negotiation and cancel requests).
    // A cancel request or a hard error closes the connection and returns.
    let startup = match read_startup_packet(&mut stream, &shared, peer).await {
        StartupOutcome::Startup(params) => params,
        StartupOutcome::Cancel | StartupOutcome::Closed => return,
    };

    // Apply the parsed parameters to the session (db/user; OID resolution is the
    // deferred auth/catalog phase).
    if let Some(db) = startup.database.as_deref() {
        session.set_database_name(Some(db.to_string()));
    }

    // Generate the query-cancel key.
    // TODO(rng): use a CSPRNG (predictable cancel key is a security weakness).
    let cancel_key = placeholder_cancel_key(proc_pid, session.start_time());

    // Register the proc-signal slot and publish it for the command loop.
    let latch = Arc::new(Latch::new());
    let (slot_key, slot) = shared.proc_signal().register(proc_pid, &cancel_key, latch.clone());

    // Backend top-level resource owner (step 06).
    let owner = ResourceOwner::create(None, "backend");

    // Nest the three task-local scopes so Session/proc-signal/resource-owner
    // `current()` all resolve inside PostgresMain.
    session::scope(
        session,
        procsignal::scope(
            slot.clone(),
            resowner::scope(owner, run_backend(stream, startup, slot, cancel)),
        ),
    )
    .await;

    // Deregister the slot on exit (stale key is a no-op if already gone).
    shared.proc_signal().deregister(slot_key);
}

/// Run `PostgresMain` racing the supervisor's `cancel` Notify and the slot latch.
/// When `cancel` fires (shutdown), raise ProcDie on the slot and ring the latch
/// so the next `CHECK_FOR_INTERRUPTS` in `PostgresMain` terminates the backend.
async fn run_backend(
    stream: TcpStream,
    startup: StartupParams,
    slot: Arc<ProcSignalSlot>,
    cancel: Arc<Notify>,
) {
    let dbname = startup.database.unwrap_or_default();
    let username = startup.user.unwrap_or_default();

    let main = crate::backend::tcop::postgres::postgres_main(stream, dbname, username);
    tokio::pin!(main);

    loop {
        tokio::select! {
            // PostgresMain returned (Terminate / EOF) -- the command loop exited.
            () = &mut main => return,

            // Supervisor shutdown: set ProcDie + interrupt_pending and ring the
            // latch. PostgresMain's CHECK_FOR_INTERRUPTS then terminates (FATAL).
            () = cancel.notified() => {
                use std::sync::atomic::Ordering;
                slot.flags.proc_die_pending.store(true, Ordering::Release);
                slot.flags.interrupt_pending.store(true, Ordering::Release);
                slot.latch.set();
                // Keep driving PostgresMain so it observes the interrupt; do not
                // re-arm this branch (notified() is consumed).
            }
        }
    }
}

/// The parsed contents of a real (v3) startup packet.
#[derive(Debug, Default)]
struct StartupParams {
    user: Option<String>,
    database: Option<String>,
    /// Remaining key/value parameter pairs (options, application_name, ...).
    options: Vec<(String, String)>,
}

/// What `read_startup_packet` resolved the connection to.
enum StartupOutcome {
    /// A real v3 startup packet was parsed.
    Startup(StartupParams),
    /// A cancel request was handled (routed to send_cancel); close the socket.
    Cancel,
    /// The connection closed or errored before a usable packet arrived.
    Closed,
}

/// Read and classify the startup packet, looping over SSL/GSS negotiation. Uses
/// direct tokio framing (pqcomm deferred): an Int32 length prefix then the body.
async fn read_startup_packet(
    stream: &mut TcpStream,
    shared: &Arc<SharedState>,
    peer: SocketAddr,
) -> StartupOutcome {
    loop {
        let body = match read_length_prefixed(stream).await {
            Some(b) => b,
            None => return StartupOutcome::Closed,
        };
        if body.len() < 4 {
            crate::elog!(crate::utils::elog::LOG, format!("short startup packet from {peer}"));
            return StartupOutcome::Closed;
        }
        let code = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);

        match code {
            NEGOTIATE_SSL_CODE | NEGOTIATE_GSS_CODE => {
                // TLS/GSS are deferred: decline with a single 'N' byte and loop
                // to read the real startup packet on the same plaintext socket.
                if stream.write_all(b"N").await.is_err() {
                    return StartupOutcome::Closed;
                }
                continue;
            }
            CANCEL_REQUEST_CODE => {
                // Body: code(4) | backend pid(4) | cancel key(remaining).
                if body.len() < 8 {
                    return StartupOutcome::Closed;
                }
                let pid = i32::from_be_bytes([body[4], body[5], body[6], body[7]]);
                let key = &body[8..];
                shared.proc_signal().send_cancel(pid, key);
                // Cancel requests get no reply; just close.
                return StartupOutcome::Cancel;
            }
            _ => {
                // A real protocol version (e.g. 0x00030000). Parse the trailing
                // null-terminated key/value parameter pairs.
                return StartupOutcome::Startup(parse_startup_params(&body[4..]));
            }
        }
    }
}

/// Read an Int32-length-prefixed frame: the leading Int32 (big-endian) is the
/// total length INCLUDING itself, so the body is `len - 4` bytes. Returns the
/// body (without the length prefix), or `None` on EOF / error / oversize.
async fn read_length_prefixed(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.ok()?;
    let total = i32::from_be_bytes(len_buf);
    if total < 4 {
        return None;
    }
    let body_len = (total - 4) as usize;
    if body_len > MAX_STARTUP_PACKET_LENGTH {
        return None;
    }
    let mut body = vec![0u8; body_len];
    stream.read_exact(&mut body).await.ok()?;
    Some(body)
}

/// Parse the v3 startup-packet body after the protocol version: a sequence of
/// null-terminated `key\0value\0` pairs ended by a final empty key (`\0`).
fn parse_startup_params(buf: &[u8]) -> StartupParams {
    let mut params = StartupParams::default();
    let mut it = buf.split(|&b| b == 0);
    loop {
        let key = match it.next() {
            Some(k) if !k.is_empty() => k,
            _ => break, // empty key (terminator) or end of buffer
        };
        let value = it.next().unwrap_or(&[]);
        let key = String::from_utf8_lossy(key).into_owned();
        let value = String::from_utf8_lossy(value).into_owned();
        match key.as_str() {
            "user" => params.user = Some(value),
            "database" => params.database = Some(value),
            _ => params.options.push((key, value)),
        }
    }
    params
}

/// Placeholder query-cancel key. PG fills `MyCancelKey` from `pg_strong_random`;
/// no CSPRNG dependency exists yet, so derive a deterministic key from the
/// synthetic proc-pid and start time.
/// TODO(rng): use a CSPRNG (predictable cancel key is a security weakness).
fn placeholder_cancel_key(proc_pid: i32, start_time: i64) -> Vec<u8> {
    let seed = (proc_pid as u64) ^ ((start_time as u64) << 17);
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
    (0..MAX_CANCEL_KEY_LENGTH)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

/// Test-only observation hooks so Part A's supervisor tests can assert that the
/// backend actually ran (registry count + the panic path), without depending on
/// log output. Kept for Part A compatibility.
#[cfg(test)]
pub mod test_hook {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// `PANIC_ON_CONNECT` / `CONNECTED` are process-global; any test that touches
    /// the connection hook must hold this so one test's panic flag doesn't bleed
    /// into another's backend. Shared by both the supervisor and backend-startup
    /// test modules. Recovers from poisoning (some tests intentionally panic).
    static SERIAL: Mutex<()> = Mutex::new(());
    pub fn serial() -> std::sync::MutexGuard<'static, ()> {
        SERIAL.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Incremented once each time the backend reaches connection.
    pub static CONNECTED: AtomicUsize = AtomicUsize::new(0);

    /// When true, the backend panics immediately (to exercise the supervisor's
    /// `catch_unwind`).
    pub static PANIC_ON_CONNECT: std::sync::atomic::AtomicBool =
        std::sync::atomic::AtomicBool::new(false);

    pub fn on_backend_connected(_peer: SocketAddr) {
        CONNECTED.fetch_add(1, Ordering::SeqCst);
        if PANIC_ON_CONNECT.load(Ordering::SeqCst) {
            panic!("test-induced backend panic");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::postmaster::postmaster::start_supervisor;
    use crate::shared_state::SharedStateConfig;
    use std::time::Duration;
    use tokio::net::TcpStream as ClientStream;

    #[test]
    fn parses_user_and_database_pairs() {
        // "user\0alice\0database\0db1\0\0"
        let body = b"user\0alice\0database\0db1\0\0";
        let params = parse_startup_params(body);
        assert_eq!(params.user.as_deref(), Some("alice"));
        assert_eq!(params.database.as_deref(), Some("db1"));
    }

    #[test]
    fn placeholder_key_is_full_length() {
        let key = placeholder_cancel_key(1_000_000, 12345);
        assert_eq!(key.len(), MAX_CANCEL_KEY_LENGTH);
    }

    fn loopback_port0() -> SocketAddr {
        (std::net::Ipv4Addr::LOCALHOST, 0).into()
    }

    /// Length-prefixed startup frame: Int32 total length (incl itself) + body.
    fn framed(body: &[u8]) -> Vec<u8> {
        let total = (body.len() + 4) as i32;
        let mut out = total.to_be_bytes().to_vec();
        out.extend_from_slice(body);
        out
    }

    async fn wait_until<F: Fn() -> bool>(pred: F, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if pred() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        pred()
    }

    // HEADLINE: accept -> startup-parse -> slot register. Send a well-formed v3
    // startup packet, then assert the backend registered its proc-signal slot.
    // Disconnect before any query so the deferred pq_* stub is not relied upon
    // (the backend may panic on the deferred BackendKeyData send; that is caught
    // by the supervisor's catch_unwind and leaves the slot registered).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_packet_registers_slot() {
        let _hook = test_hook::serial();
        let (sup, handle) = start_supervisor(loopback_port0(), SharedStateConfig::default()).await;

        let mut client = ClientStream::connect(sup.local_addr).await.expect("connect");
        // protocol 0x00030000 + "user\0alice\0database\0db1\0\0".
        let mut body = 0x0003_0000u32.to_be_bytes().to_vec();
        body.extend_from_slice(b"user\0alice\0database\0db1\0\0");
        client.write_all(&framed(&body)).await.expect("write startup");

        assert!(
            wait_until(|| sup.shared.proc_signal().len() == 1, Duration::from_secs(2)).await,
            "backend should have registered exactly one proc-signal slot"
        );

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    // SSL NEGOTIATION: an SSLRequest must get a single 'N' (no SSL) reply.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ssl_request_gets_n_reply() {
        let _hook = test_hook::serial();
        let (sup, handle) = start_supervisor(loopback_port0(), SharedStateConfig::default()).await;

        let mut client = ClientStream::connect(sup.local_addr).await.expect("connect");
        let ssl = crate::libpq::pqcomm::NEGOTIATE_SSL_CODE.to_be_bytes();
        client.write_all(&framed(&ssl)).await.expect("write SSLRequest");

        let mut reply = [0u8; 1];
        tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut reply))
            .await
            .expect("backend should reply")
            .expect("read 'N'");
        assert_eq!(reply[0], b'N', "SSLRequest should be declined with 'N'");

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }

    // CANCEL REQUEST: register a slot directly, then feed a CancelRequest packet
    // (pid + key) over a fresh connection; assert the slot's query-cancel flag is
    // set. (Capturing the real key over the wire is blocked by the deferred
    // BackendKeyData pq stub, so we seed the slot via the proc_signal API per the
    // brief and verify backend_startup routes the CancelRequest to send_cancel.)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_request_sets_target_flag() {
        use std::sync::atomic::Ordering;

        let _hook = test_hook::serial();
        let (sup, handle) = start_supervisor(loopback_port0(), SharedStateConfig::default()).await;

        // Seed a target slot.
        let latch = Arc::new(Latch::new());
        let key = vec![0xABu8; MAX_CANCEL_KEY_LENGTH];
        let (_slot_key, slot) = sup.shared.proc_signal().register(7777, &key, latch);
        assert!(!slot.flags.query_cancel_pending.load(Ordering::Acquire));

        // Send a CancelRequest: code(4) + pid(4) + key.
        let mut body = crate::libpq::pqcomm::CANCEL_REQUEST_CODE.to_be_bytes().to_vec();
        body.extend_from_slice(&7777i32.to_be_bytes());
        body.extend_from_slice(&key);
        let mut client = ClientStream::connect(sup.local_addr).await.expect("connect");
        client.write_all(&framed(&body)).await.expect("write CancelRequest");

        assert!(
            wait_until(
                || slot.flags.query_cancel_pending.load(Ordering::Acquire),
                Duration::from_secs(2)
            )
            .await,
            "CancelRequest should set the target slot's query_cancel_pending"
        );

        drop(client);
        sup.shutdown.trigger();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor drains")
            .expect("supervisor task ok");
    }
}

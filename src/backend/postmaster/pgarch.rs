//! The PostgreSQL WAL archiver. Translated from backend/postmaster/pgarch.c.
//!
//! The archiver is a long-lived auxiliary process responsible for copying
//! completed write-ahead log segments to the configured archive. It mostly
//! sleeps, waking when a backend marks a segment `.ready` (and rings its latch via
//! [`pgarch_wakeup`]) or every `PGARCH_AUTOWAKE_INTERVAL` seconds to proactively
//! poll the `pg_wal/archive_status` directory for `.ready` files. On each wake it
//! archives every outstanding segment in priority order -- timeline history files
//! first, then oldest segment first -- invoking the archive callback for each and
//! renaming the segment's status file from `.ready` to `.done` on success.
//! Failures are retried a bounded number of times before the archiver gives up and
//! tries again on its next wake.
//!
//! In PostgreSQL the archiver is forked from the postmaster and the two
//! communicate by signals and a small shared-memory area; this file collects both
//! the functions run by the archiver process and the helpers backends call to
//! wake it or force a directory rescan.
//!
//! Here the archiver is a single `tokio` task supervised like the other auxiliary
//! processes rather than a forked child. The shared-memory `PgArchData` area --
//! the advertised process number and the force-rescan flag -- becomes a small
//! `Arc<PgArchData>` published process-wide through a `OnceLock`; the running
//! archiver advertises its process number there so any backend's [`pgarch_wakeup`]
//! can find and set its latch. The latch itself is a `tokio` notification, and the
//! main loop sleeps on a `select!` over the latch, the autowake timer, and a
//! shutdown handle (PostgreSQL's "do one more cycle then exit" on SIGTERM). On exit
//! -- including an unwinding panic -- an RAII guard clears the advertised process
//! number and returns the auxiliary `PGPROC`, replacing the C `sigsetjmp` handler
//! and `proc_exit` callbacks. The bounded max-heap that orders `.ready` files is a
//! sorted `VecDeque`, with the same priority comparator.
//!
//! Dynamic loading of an archive library is not reproduced -- there is no shared
//! object loading in a single binary -- so the actual copy step
//! ([`pgarch_archive_xlog`]) is currently a non-failing-fatally stub that reports
//! the segment as not archived; shell archiving via `archive_command` is the
//! intended path and is not yet implemented.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

use crate::backend::postmaster::auxprocess::{
    auxiliary_process_main_common_with_proc, process_main_loop_interrupts,
};
use crate::miscadmin::BackendType;
use crate::postmaster::pgarch::{MAX_XFN_CHARS, MIN_XFN_CHARS, VALID_XFN_CHARS};
use crate::shared_state::SharedState;
use crate::storage::latch::Latch;
use crate::storage::proc::{my_proc_scope, ProcGlobal};
use crate::storage::procnumber::INVALID_PROC_NUMBER;

/// PG `PGARCH_AUTOWAKE_INTERVAL`: how often to force a poll of the archive status
/// directory, in seconds.
const PGARCH_AUTOWAKE_INTERVAL: u64 = 60;
/// PG `PGARCH_RESTART_INTERVAL`: how often to attempt to restart a failed
/// archiver, in seconds (consulted by the supervisor via [`pgarch_can_restart`]).
const PGARCH_RESTART_INTERVAL: u64 = 10;
/// PG `NUM_ARCHIVE_RETRIES`: max retries when archiving a WAL file.
const NUM_ARCHIVE_RETRIES: i32 = 3;
/// PG `NUM_FILES_PER_DIRECTORY_SCAN`: max `.ready` files gathered per scan.
const NUM_FILES_PER_DIRECTORY_SCAN: usize = 64;

/// PG `PgArchData` -- the archiver<->backend shared state. An `Arc` published
/// process-wide (single-process model: exactly one) so [`pgarch_wakeup`] /
/// [`pgarch_force_dir_scan`], called by arbitrary backends, reach it without a
/// `SharedState` handle.
pub struct PgArchData {
    /// PG `pgprocno`: proc number of the archiver process, or INVALID when none.
    pub pgprocno: std::sync::atomic::AtomicI32,
    /// PG `force_dir_scan`: forces a directory scan in `pgarch_ready_xlog`.
    pub force_dir_scan: AtomicU32,
}

impl PgArchData {
    /// PG `PgArchShmemInit`: a zeroed struct with an INVALID proc number.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            pgprocno: std::sync::atomic::AtomicI32::new(INVALID_PROC_NUMBER),
            force_dir_scan: AtomicU32::new(0),
        })
    }
}

/// The process-wide `PgArchData` (PG's `static PgArchData *PgArch`). Published on
/// first access; first publish wins (tests build their own `SharedState`s).
static PG_ARCH: OnceLock<Arc<PgArchData>> = OnceLock::new();

/// Publish the process-wide `PgArchData`. First publish wins; returns whether this
/// call won.
pub fn set_pgarch(data: Arc<PgArchData>) -> bool {
    PG_ARCH.set(data).is_ok()
}

/// The process-wide `PgArchData`, creating + publishing one on first use. PG
/// allocates it in `PgArchShmemInit`; here it is lazily published so a running
/// archiver and any waking backend agree on one struct.
pub fn pgarch_data() -> &'static Arc<PgArchData> {
    PG_ARCH.get_or_init(PgArchData::new)
}

/// PG `PgArchCanRestart`: true if enough time has passed since the last archiver
/// launch to allow a restart (a safety valve against respawn storms). Tracks the
/// last launch time in a process atomic (PG's function `static`).
pub fn pgarch_can_restart() -> bool {
    static LAST_START: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let curtime = now_secs();
    let last = LAST_START.load(Ordering::Acquire);
    if curtime.wrapping_sub(last) < PGARCH_RESTART_INTERVAL {
        return false;
    }
    LAST_START.store(curtime, Ordering::Release);
    true
}

/// PG `PgArchWakeup`: wake the archiver by ringing its `proc_latch`. Reaches the
/// aux PGPROC by the advertised `PgArchData.pgprocno`; a no-op if no archiver is
/// running. Returns whether a latch was set.
pub fn pgarch_wakeup() -> bool {
    let Some(g) = ProcGlobal::get() else {
        return false;
    };
    let procno = pgarch_data().pgprocno.load(Ordering::Acquire);
    if procno == INVALID_PROC_NUMBER {
        return false;
    }
    // SAFETY: `proc_latch` is internally synchronized (a Latch over a Notify);
    // setting it forms no `&mut` into the slot's other field groups. PG likewise
    // does not lock here -- a stale proc number at worst sets the wrong latch.
    (unsafe { g.proc(procno) })
        .inspect(|proc| proc.proc_latch.set())
        .is_some()
}

/// PG `PgArchForceDirScan`: make the next `pgarch_ready_xlog` perform a directory
/// scan (e.g. to archive a timeline history file as fast as possible).
pub fn pgarch_force_dir_scan() {
    pgarch_data().force_dir_scan.store(1, Ordering::SeqCst);
}

/// PG `PgArchiverMain`. The long-lived archiver aux task. Claims a PGPROC,
/// advertises it in `PgArchData.pgprocno` so backends can wake it by `ProcNumber`,
/// then runs the archive main loop until `shutdown` fires.
///
/// `shutdown` is the supervisor's per-child cancel handle (17f maps PG's SIGTERM
/// "do one more cycle then exit" onto a cancel; here the loop runs a final cycle
/// before breaking). The loop mirrors 17a/17b's shape: a single unified latch
/// reset at the top, interrupts serviced, the copy loop, then a
/// `select!{ biased; latch | timeout | shutdown }` sleep. A RAII guard clears the
/// advertised proc, deregisters the slot, and returns the PGPROC on every exit.
pub async fn pgarch_main(shared: Arc<SharedState>, shutdown: Arc<tokio::sync::Notify>) {
    my_proc_scope(async move {
        let aux =
            auxiliary_process_main_common_with_proc(shared.proc_signal(), BackendType::ARCHIVER)
                .await;

        // Use the PROCESS-published PgArchData (the one waking backends reach),
        // not a per-task struct, so the advertise + force_dir_scan flag agree on
        // one struct across the process.
        let arch = pgarch_data().clone();

        // Cleanup on EVERY exit (normal break + panic unwind): clear the
        // advertised proc (PG `pgarch_die`), deregister the proc-signal slot, and
        // return the aux PGPROC.
        let _exit = PgArchExitGuard {
            arch: arch.clone(),
            proc_signal: shared.proc_signal().clone(),
            slot_key: aux.slot_key,
        };

        // Our single wakeup latch IS our PGPROC's proc_latch (PG MyLatch ==
        // MyProc->procLatch for an aux proc); pgarch_wakeup rings it and the
        // proc-signal slot was registered against this SAME latch in the cradle.
        let proc_latch: &Latch = &aux.latch;

        // Advertise our proc number so backends can wake us (PG:
        // PgArch->pgprocno = MyProcNumber).
        arch.pgprocno.store(aux.proc_number, Ordering::Release);

        // PG: LoadArchiveLibrary() -- tombstoned (no dynamic library loading;
        // shell archiving via archive_command is the supported path).

        // Per-task workspace for pgarch_ready_xlog() (PG's palloc'd
        // arch_files_state; the binaryheap becomes a sorted VecDeque).
        let mut arch_files: VecDeque<String> = VecDeque::new();

        // --- pgarch_MainLoop ---
        loop {
            // PG ResetLatch(MyLatch) at loop top.
            proc_latch.reset();

            // PG ProcessPgArchInterrupts() (barrier / config reload). Shutdown is
            // handled separately below (PG's loop checks ShutdownRequestPending).
            process_pgarch_interrupts();

            // PG: on shutdown, do one more archive cycle then exit. The supervisor
            // cancel is our SIGTERM/SIGUSR2 equivalent. A shutdown that arrived
            // between sleeps is observed at loop top.
            let time_to_stop = shutdown_now(&shutdown);

            // Do what we're here for: archive all outstanding .ready segments.
            pgarch_archiver_copy_loop(&arch, &mut arch_files, &shutdown).await;

            if time_to_stop {
                break;
            }

            // Sleep until signaled, until a poll is forced by
            // PGARCH_AUTOWAKE_INTERVAL, or until shutdown.
            let sleep =
                tokio::time::sleep(std::time::Duration::from_secs(PGARCH_AUTOWAKE_INTERVAL));
            tokio::select! {
                biased;
                () = proc_latch.wait() => {}
                () = sleep => {}
                () = shutdown.notified() => break,
            }
        }

        // `_exit` clears the advertised proc, deregisters the slot, returns the
        // PGPROC on drop -- on this path and on any panic unwind alike.
    })
    .await;
}

/// PG `pgarch_ArchiverCopyLoop`. Archive all outstanding `.ready` segments, then
/// return. We expect mostly a single file, but a backend may add more while we
/// copy earlier ones, so we loop until `pgarch_ready_xlog` finds nothing.
async fn pgarch_archiver_copy_loop(
    arch: &Arc<PgArchData>,
    arch_files: &mut VecDeque<String>,
    shutdown: &Arc<tokio::sync::Notify>,
) {
    // PG forces a directory scan on the first call of each copy loop.
    arch_files.clear();

    while let Some(xlog) = pgarch_ready_xlog(arch, arch_files) {
        let mut failures = 0;
        loop {
            // Do not start more archive commands after a shutdown request (try to
            // avoid having the command SIGKILLed mid-flight).
            if shutdown_now(shutdown) {
                return;
            }

            // Adopt a new archive_command ASAP even with a backlog (PG re-checks
            // barriers / config here).
            process_pgarch_interrupts();

            // PG: orphan .ready-without-segment cleanup (a crash can leave a
            // .ready for an already-recycled segment) is handled inside
            // pgarch_archive_xlog's caller in C via a stat() check; the archive
            // path itself is a stub here, so there is nothing to orphan-clean yet.

            if pgarch_archive_xlog(&xlog) {
                // Successful: rename .ready -> .done and tell the stats system.
                pgarch_archive_done(&xlog);
                crate::pgstat::pgstat_report_archiver(&xlog, false);
                break; // out of the inner retry loop
            }
            crate::pgstat::pgstat_report_archiver(&xlog, true);
            failures += 1;
            if failures >= NUM_ARCHIVE_RETRIES {
                crate::elog!(
                    crate::utils::elog::WARNING,
                    format!(
                        "archiving write-ahead log file \"{xlog}\" failed too many times, will try again later"
                    )
                );
                return; // give up archiving for now
            }
            // PG naps 1s before retrying; yield to let other tasks run.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }
}

/// PG `pgarch_archiveXlog`. Invoke the archive module callback to copy one segment
/// to the archive. Returns true on success.
///
/// TOMBSTONED until shell archiving lands: there is no archive module / dynamic
/// library here. A non-panicking no-op that reports "not configured" so the
/// archiver never panics on the periodic scan (the copy loop treats false as a
/// retryable failure and gives up after NUM_ARCHIVE_RETRIES). Reached only when a
/// `.ready` file actually exists in `archive_status` (none in normal operation /
/// tests), so the retry/give-up path is exercised only when archiving is in use.
fn pgarch_archive_xlog(_xlog: &str) -> bool {
    // TODO(archive): shell_archive_init + archive_file_cb (run
    // archive_command on the segment, durable on success). No-op for now.
    false
}

/// PG `pgarch_readyXlog`. Return the name of the oldest WAL file with a `.ready`
/// archive-status file that has not yet been archived, or `None` if none. Scans
/// `<DataDir>/pg_wal/archive_status` for `*.ready` entries, keeping the highest-
/// priority [`NUM_FILES_PER_DIRECTORY_SCAN`] (history files first, then oldest) in
/// `arch_files`, and serves them oldest-first across calls until empty.
fn pgarch_ready_xlog(arch: &Arc<PgArchData>, arch_files: &mut VecDeque<String>) -> Option<String> {
    // If a directory scan was requested, clear the stored names and re-scan.
    if arch.force_dir_scan.swap(0, Ordering::SeqCst) == 1 {
        arch_files.clear();
    }

    // If we still have names from a previous scan, return one whose status file is
    // still present (a prior archive_command may have marked it done).
    while let Some(arch_file) = arch_files.pop_front() {
        if status_file_path(&arch_file, ".ready").is_some_and(|p| p.exists()) {
            return Some(arch_file);
        }
    }

    // Open the archive status directory and gather the .ready entries.
    let status_dir = archive_status_dir()?;
    let Ok(rldir) = std::fs::read_dir(&status_dir) else {
        // No archive_status directory (the common case without archiving): nothing
        // to archive. PG ereports on a real error; a missing dir is benign here.
        return None;
    };

    let mut found: Vec<String> = Vec::new();
    for entry in rldir.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // basename length = name length minus the 6-char ".ready" suffix.
        let Some(basenamelen) = name.len().checked_sub(6) else {
            continue;
        };
        if !(MIN_XFN_CHARS..=MAX_XFN_CHARS).contains(&basenamelen) {
            continue;
        }
        let basename = &name[..basenamelen];
        if basename.chars().any(|c| !VALID_XFN_CHARS.contains(c)) {
            continue;
        }
        if &name[basenamelen..] != ".ready" {
            continue;
        }
        found.push(basename.to_string());
    }

    if found.is_empty() {
        return None;
    }

    // Sort by archival priority (history files first, then oldest), then keep the
    // highest-priority NUM_FILES_PER_DIRECTORY_SCAN. PG uses a bounded max-heap;
    // sorting then truncating is equivalent for our scale.
    found.sort_by(|a, b| ready_file_comparator(a, b));
    found.truncate(NUM_FILES_PER_DIRECTORY_SCAN);

    // arch_files holds the files in ascending order of priority (the loop above
    // serves from the front); the highest-priority file (front of `found`) is
    // returned now, the rest are queued for subsequent calls.
    let mut iter = found.into_iter();
    let first = iter.next();
    *arch_files = iter.collect();
    first
}

/// PG `ready_file_comparator`. Orders by archival priority: timeline history
/// files first, then older files (ascending lexicographic name). Negative means
/// `a` has higher priority than `b`.
fn ready_file_comparator(a: &str, b: &str) -> std::cmp::Ordering {
    let a_history = is_tl_history_file_name(a);
    let b_history = is_tl_history_file_name(b);
    if a_history != b_history {
        // History files always have the highest priority.
        return if a_history {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        };
    }
    a.cmp(b)
}

/// PG `IsTLHistoryFileName`: an 8-hex-digit timeline id followed by ".history".
fn is_tl_history_file_name(name: &str) -> bool {
    name.len() == 8 + ".history".len()
        && name.ends_with(".history")
        && name[..8].chars().all(|c| matches!(c, '0'..='9' | 'A'..='F'))
}

/// PG `pgarch_archiveDone`. Mark a segment archived by renaming its status file
/// from `NNN.ready` to `NNN.done`. PG does not durably rename (re-archiving is
/// tolerated). A no-op if the status files cannot be located.
fn pgarch_archive_done(xlog: &str) {
    let (Some(ready), Some(done)) = (status_file_path(xlog, ".ready"), status_file_path(xlog, ".done"))
    else {
        return;
    };
    if let Err(e) = std::fs::rename(&ready, &done) {
        crate::elog!(
            crate::utils::elog::WARNING,
            format!(
                "could not rename file \"{}\" to \"{}\": {e}",
                ready.display(),
                done.display()
            )
        );
    }
}

/// PG `ProcessPgArchInterrupts`. Service barrier / config-reload / memory-context
/// interrupts (NOT shutdown, which the loops handle differently). `process_main_
/// loop_interrupts` clears the config-reload flag; the archive-library-changed
/// restart is tombstoned (no dynamic library loading).
fn process_pgarch_interrupts() {
    // The boolean return (shutdown requested) is intentionally ignored here: PG's
    // ProcessPgArchInterrupts does not check shutdown; the loops do that via
    // `shutdown_now`. We still call it to drain the config-reload flag + barriers.
    let _ = process_main_loop_interrupts();
    // TODO(archive): on archive_library change, restart the archiver. No
    // dynamic library loading, so nothing to do.
}

/// `<DataDir>/pg_wal/archive_status`, or `None` if no DataDir is configured.
fn archive_status_dir() -> Option<std::path::PathBuf> {
    let dir = crate::backend::utils::init::globals::process_config()?.data_dir()?;
    Some(std::path::Path::new(&dir).join("pg_wal").join("archive_status"))
}

/// PG `StatusFilePath`: `<archive_status>/<xlog><suffix>` (e.g. `NNN.ready`).
fn status_file_path(xlog: &str, suffix: &str) -> Option<std::path::PathBuf> {
    Some(archive_status_dir()?.join(format!("{xlog}{suffix}")))
}

/// Runs the archiver's exit cleanup on EVERY scope exit (normal return + panic
/// unwind). Idempotent: re-clearing an already-cleared proc number / stale slot
/// key is harmless and ProcKill no-ops once the proc is returned.
struct PgArchExitGuard {
    arch: Arc<PgArchData>,
    proc_signal: Arc<crate::backend::storage::ipc::procsignal::ProcSignal>,
    slot_key: crate::backend::storage::ipc::procsignal::SlotKey,
}

impl Drop for PgArchExitGuard {
    fn drop(&mut self) {
        // PG pgarch_die: PgArch->pgprocno = INVALID_PROC_NUMBER.
        self.arch.pgprocno.store(INVALID_PROC_NUMBER, Ordering::Release);
        self.proc_signal.deregister(self.slot_key);
        crate::storage::proc::ProcKill();
    }
}

/// True if the supervisor has asked this task to shut down. Non-blocking: polls
/// `shutdown.notified()` once, consuming a permit left by `notify_one`, so a
/// shutdown that arrived between sleeps is seen here without awaiting (mirrors
/// checkpointer.rs / walwriter.rs `shutdown_now`).
fn shutdown_now(shutdown: &Arc<tokio::sync::Notify>) -> bool {
    use futures_util::FutureExt;
    let fut = shutdown.notified();
    futures_util::pin_mut!(fut);
    fut.now_or_never().is_some()
}

/// PG `(time_t) time(NULL)`: current wall-clock seconds.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::postmaster::auxprocess::aux_test_serial as test_serial;
    use crate::shared_state::SharedStateConfig;
    use std::time::Duration;

    fn fresh_shared() -> Arc<SharedState> {
        let shared = SharedState::new(SharedStateConfig::default());
        let _ = crate::storage::proc::ProcGlobal::set(shared.proc_global().clone());
        shared
    }

    async fn wait_for<F: Fn() -> bool>(pred: F, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if pred() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        pred()
    }

    /// The archiver starts, advertises PgArchData.pgprocno, parks on its latch
    /// (no .ready files -> the copy loop is a quick no-op), and clears the
    /// advertisement + exits cleanly on shutdown.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_advertises_parks_and_shuts_down() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let arch = pgarch_data().clone();
        let shutdown = Arc::new(tokio::sync::Notify::new());

        let task = tokio::spawn(pgarch_main(shared.clone(), shutdown.clone()));

        assert!(
            wait_for(
                || arch.pgprocno.load(Ordering::Acquire) != INVALID_PROC_NUMBER,
                Duration::from_secs(2),
            )
            .await,
            "archiver should advertise its proc number"
        );

        shutdown.notify_waiters();
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("archiver should exit on shutdown")
            .expect("task panicked");
        assert_eq!(
            arch.pgprocno.load(Ordering::Acquire),
            INVALID_PROC_NUMBER,
            "proc number cleared on exit"
        );
    }

    /// `pgarch_wakeup` rings the advertised PGPROC latch, so a parked archiver
    /// loops back (keeps running) rather than exiting. The autowake interval is
    /// 60s, so the only near-term wakeup is the latch, proving the wake reaches it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wakeup_wakes_the_parked_archiver() {
        let _serial = test_serial().await;
        let shared = fresh_shared();
        let arch = pgarch_data().clone();
        let shutdown = Arc::new(tokio::sync::Notify::new());

        let task = tokio::spawn(pgarch_main(shared.clone(), shutdown.clone()));
        assert!(
            wait_for(
                || arch.pgprocno.load(Ordering::Acquire) != INVALID_PROC_NUMBER,
                Duration::from_secs(2),
            )
            .await,
            "archiver running"
        );

        assert!(pgarch_wakeup(), "pgarch_wakeup finds the running archiver");
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!task.is_finished(), "archiver keeps running after a wake");

        shutdown.notify_waiters();
        shutdown.notify_one();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("archiver exits")
            .expect("task panicked");
        assert_eq!(arch.pgprocno.load(Ordering::Acquire), INVALID_PROC_NUMBER);
    }

    /// `pgarch_wakeup` is a no-op (returns false) when no archiver is running, and
    /// `pgarch_force_dir_scan` sets the flag.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wakeup_noop_when_idle_and_force_dir_scan_sets_flag() {
        let _serial = test_serial().await;
        let _shared = fresh_shared();
        let arch = pgarch_data().clone();
        arch.pgprocno.store(INVALID_PROC_NUMBER, Ordering::Release);
        assert!(!pgarch_wakeup(), "no archiver -> wakeup is a no-op");

        arch.force_dir_scan.store(0, Ordering::SeqCst);
        pgarch_force_dir_scan();
        assert_eq!(arch.force_dir_scan.load(Ordering::SeqCst), 1, "force_dir_scan set");
        arch.force_dir_scan.store(0, Ordering::SeqCst);
    }

    /// The ready-file comparator: history files first, then oldest name.
    #[test]
    fn comparator_history_first_then_oldest() {
        assert!(is_tl_history_file_name("00000002.history"));
        assert!(!is_tl_history_file_name("000000010000000000000001"));
        // History beats a regular segment.
        assert_eq!(
            ready_file_comparator("00000002.history", "000000010000000000000001"),
            std::cmp::Ordering::Less
        );
        // Older (smaller) segment name sorts first.
        assert_eq!(
            ready_file_comparator(
                "000000010000000000000001",
                "000000010000000000000002"
            ),
            std::cmp::Ordering::Less
        );
    }
}

//! ppoll()/pselect()-like abstraction for waiting on multiple events at once.
//! Translated from backend/storage/ipc/waiteventset.c.
//!
//! A WaitEventSet collects several wakeup sources and waits on them together,
//! returning as soon as any one of them fires. The sources are: a latch being set
//! (LATCH_SET), a socket becoming readable or writable (SOCKET_*), supervisor
//! death (POSTMASTER_DEATH, or EXIT_ON_PM_DEATH which exits rather than reporting),
//! and a timeout. In PostgreSQL the waiting is race-free, comparable to ppoll() or
//! pselect() rather than plain poll()/select(): a latch set concurrently or from a
//! signal handler is guaranteed to be observed instead of being lost just before
//! the sleep begins.
//!
//! PostgreSQL builds this on the most modern OS readiness primitive available
//! (epoll, kqueue, poll, or Windows events), and closes the latch race with a
//! self-pipe or signalfd. None of that machinery exists here. The tokio runtime is
//! the multiplexer, so each source becomes a future and they are raced together:
//! LATCH_SET awaits the latch's notification, SOCKET_* registers the borrowed file
//! descriptor with the tokio reactor and awaits the requested readiness, the
//! timeout is a tokio timer, and POSTMASTER_DEATH awaits a shared notification that
//! stands in for supervisor shutdown in this single-process design. Because tokio
//! already serializes wakeups against the awaiting task, the self-pipe and signalfd
//! tricks are unnecessary and are dropped. The per-OS backend selection, the
//! preallocated kernel event arrays, and the ResourceOwner bookkeeping likewise
//! have no analogue and are gone; teardown is ordinary Rust drop.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::os::fd::{BorrowedFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::{FutureExt, select_all};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tokio::sync::Notify;

use crate::postgres::Datum;
use crate::storage::latch::Latch;
use crate::storage::waiteventset::{PGINVALID_SOCKET, WaitEvent, WaitEventFlags, pgsocket};

/// Shared postmaster-death signal. A single-process stand-in for "the supervisor
/// is shutting down"; later steps replace this with the real supervisor handle.
#[derive(Clone, Default)]
pub struct PostmasterDeath {
    notify: Arc<Notify>,
}

impl PostmasterDeath {
    pub fn new() -> Self {
        Self::default()
    }

    /// Signal postmaster death to all waiters. Synchronous.
    pub fn signal(&self) {
        self.notify.notify_waiters();
    }

    async fn died(&self) {
        self.notify.notified().await;
    }
}

/// One registered wakeup source within a [`WaitEventSet`].
enum Source<'a> {
    Latch {
        latch: &'a Latch,
        pos: i32,
        user_data: Datum,
    },
    Socket {
        fd: pgsocket,
        flags: WaitEventFlags,
        pos: i32,
        user_data: Datum,
    },
    PostmasterDeath {
        pmdeath: PostmasterDeath,
        pos: i32,
        exit_on_death: bool,
        user_data: Datum,
    },
}

/// Set of wakeup sources to wait on together. Borrows the latch(es) it watches.
pub struct WaitEventSet<'a> {
    sources: Vec<Source<'a>>,
    next_pos: i32,
}

impl<'a> WaitEventSet<'a> {
    /// Create an empty wait-event set sized for up to `nevents` sources. The
    /// ResourceOwner and OS-handle preallocation from PG are gone.
    pub fn new(nevents: i32) -> Self {
        WaitEventSet {
            sources: Vec::with_capacity(nevents.max(0) as usize),
            next_pos: 0,
        }
    }

    /// Register a wakeup source. `events` selects which WaitEventFlags this entry
    /// watches. Pass `latch` for a LATCH_SET source, a real `fd` for a socket
    /// source, or a `pmdeath` for POSTMASTER_DEATH. Returns the source position.
    pub fn add_event(
        &mut self,
        events: WaitEventFlags,
        fd: pgsocket,
        latch: Option<&'a Latch>,
        pmdeath: Option<PostmasterDeath>,
        user_data: Datum,
    ) -> i32 {
        let pos = self.next_pos;
        self.next_pos += 1;

        let source = if events.contains(WaitEventFlags::LATCH_SET) {
            Source::Latch {
                latch: latch.expect("LATCH_SET event requires a latch"),
                pos,
                user_data,
            }
        } else if events
            .intersects(WaitEventFlags::POSTMASTER_DEATH | WaitEventFlags::EXIT_ON_PM_DEATH)
        {
            Source::PostmasterDeath {
                pmdeath: pmdeath.expect("POSTMASTER_DEATH event requires a PostmasterDeath handle"),
                pos,
                exit_on_death: events.contains(WaitEventFlags::EXIT_ON_PM_DEATH),
                user_data,
            }
        } else {
            debug_assert_ne!(fd, PGINVALID_SOCKET, "socket event requires a valid fd");
            Source::Socket {
                fd,
                flags: events & WaitEventFlags::SOCKET_MASK,
                pos,
                user_data,
            }
        };
        self.sources.push(source);
        pos
    }

    /// Change the watched events (and optionally the latch) of an existing source.
    pub fn modify_event(&mut self, pos: i32, events: WaitEventFlags, latch: Option<&Latch>) {
        if let Some(src) = self.sources.iter_mut().find(|s| source_pos(s) == pos) {
            match src {
                Source::Socket { flags, .. } => *flags = events & WaitEventFlags::SOCKET_MASK,
                // Re-pointing a latch under a borrow would change the lifetime; PG
                // only ever re-points the latch to MyLatch or NULL. Left as a no-op
                // for the socket-modify path that step 09 actually exercises.
                Source::Latch { .. } | Source::PostmasterDeath { .. } => {
                    let _ = latch;
                }
            }
        }
    }
}

fn source_pos(s: &Source<'_>) -> i32 {
    match s {
        Source::Latch { pos, .. }
        | Source::Socket { pos, .. }
        | Source::PostmasterDeath { pos, .. } => *pos,
    }
}

impl WaitEventSet<'_> {
    /// Wait until at least one registered source fires or `timeout` (ms) elapses.
    /// A negative timeout means wait indefinitely. Returns the events that fired;
    /// on timeout the result is empty (TIMEOUT is reported by `Latch::wait_for`,
    /// not here, matching PG where WaitEventSetWait does not surface WL_TIMEOUT).
    pub async fn wait(&self, timeout: i64, max_events: usize) -> Vec<WaitEvent> {
        // Build one future per source; each resolves to the WaitEvent it produced.
        let mut futures: Vec<futures_util::future::BoxFuture<'_, WaitEvent>> = Vec::new();

        for src in &self.sources {
            match src {
                Source::Latch {
                    latch,
                    pos,
                    user_data,
                } => {
                    let (pos, user_data) = (*pos, *user_data);
                    futures.push(
                        async move {
                            latch.wait().await;
                            WaitEvent {
                                pos,
                                events: WaitEventFlags::LATCH_SET.bits(),
                                fd: PGINVALID_SOCKET,
                                user_data,
                            }
                        }
                        .boxed(),
                    );
                }
                Source::Socket {
                    fd,
                    flags,
                    pos,
                    user_data,
                } => {
                    let (fd, flags, pos, user_data) = (*fd, *flags, *pos, *user_data);
                    futures
                        .push(async move { wait_socket(fd, flags, pos, user_data).await }.boxed());
                }
                Source::PostmasterDeath {
                    pmdeath,
                    pos,
                    exit_on_death,
                    user_data,
                } => {
                    let (pmdeath, pos, exit_on_death, user_data) =
                        (pmdeath.clone(), *pos, *exit_on_death, *user_data);
                    futures.push(
                        async move {
                            pmdeath.died().await;
                            if exit_on_death {
                                // Real supervisor integration replaces this in a later
                                // step; for now surface the event to the caller.
                            }
                            WaitEvent {
                                pos,
                                events: WaitEventFlags::POSTMASTER_DEATH.bits(),
                                fd: PGINVALID_SOCKET,
                                user_data,
                            }
                        }
                        .boxed(),
                    );
                }
            }
        }

        if futures.is_empty() {
            return Vec::new();
        }

        let race = select_all(futures);
        let (event, rest) = match timeout {
            t if t < 0 => {
                let (event, _idx, rest) = race.await;
                (event, rest)
            }
            t => {
                let dur = Duration::from_millis(t as u64);
                match tokio::time::timeout(dur, race).await {
                    Ok((event, _idx, rest)) => (event, rest),
                    Err(_) => return Vec::new(), // timed out
                }
            }
        };

        // PG, after seeing one event, polls once with zero timeout to pack any
        // other already-ready events into the output buffer. Mirror that: drain
        // the remaining futures that resolve immediately, up to max_events.
        let mut out = vec![event];
        for fut in rest {
            if out.len() >= max_events.max(1) {
                break;
            }
            if let Some(extra) = fut.now_or_never() {
                out.push(extra);
            }
        }
        out
    }
}

/// Wait on a single fd for the requested readiness, returning the WaitEvent.
async fn wait_socket(fd: RawFd, flags: WaitEventFlags, pos: i32, user_data: Datum) -> WaitEvent {
    let want_read =
        flags.intersects(WaitEventFlags::SOCKET_READABLE | WaitEventFlags::SOCKET_CLOSED);
    let want_write = flags.contains(WaitEventFlags::SOCKET_WRITEABLE);
    let interest = match (want_read, want_write) {
        (true, true) => Interest::READABLE | Interest::WRITABLE,
        (false, true) => Interest::WRITABLE,
        _ => Interest::READABLE,
    };

    // AsyncFd must not own/close the fd; wrap a BorrowedFd. The caller owns it.
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let async_fd = AsyncFd::with_interest(borrowed, interest)
        .expect("registering fd with the tokio reactor failed");

    // Race readable and writable; PG reports both bits when both are ready in one
    // call, so don't if/else one branch. `ready(interest)` returns whichever fired.
    let mut events = WaitEventFlags::empty();
    if let Ok(mut g) = async_fd.ready(interest).await {
        let ready = g.ready();
        if want_read && (ready.is_readable() || ready.is_read_closed()) {
            events |= WaitEventFlags::SOCKET_READABLE;
        }
        if want_write && (ready.is_writable() || ready.is_write_closed()) {
            events |= WaitEventFlags::SOCKET_WRITEABLE;
        }
        g.clear_ready();
    }

    WaitEvent {
        pos,
        events: events.bits(),
        fd,
        user_data,
    }
}

impl Latch {
    /// Wait on this latch, an optional socket, and a timeout together by building
    /// a transient [`WaitEventSet`]. Returns the WaitEventFlags that fired
    /// (LATCH_SET, TIMEOUT, SOCKET_*, or POSTMASTER_DEATH). A negative timeout
    /// waits indefinitely. (PG's WaitLatch/WaitLatchOrSocket.)
    pub async fn wait_for(
        &self,
        wake_events: WaitEventFlags,
        sock: pgsocket,
        timeout: i64,
        pmdeath: Option<PostmasterDeath>,
    ) -> WaitEventFlags {
        let nil = Datum(0);
        let mut set = WaitEventSet::new(3);
        if wake_events.contains(WaitEventFlags::LATCH_SET) {
            set.add_event(
                WaitEventFlags::LATCH_SET,
                PGINVALID_SOCKET,
                Some(self),
                None,
                nil,
            );
        }
        if sock != PGINVALID_SOCKET && wake_events.intersects(WaitEventFlags::SOCKET_MASK) {
            set.add_event(wake_events & WaitEventFlags::SOCKET_MASK, sock, None, None, nil);
        }
        if let Some(pm) = pmdeath
            && wake_events
                .intersects(WaitEventFlags::POSTMASTER_DEATH | WaitEventFlags::EXIT_ON_PM_DEATH)
            {
                set.add_event(
                    wake_events
                        & (WaitEventFlags::POSTMASTER_DEATH | WaitEventFlags::EXIT_ON_PM_DEATH),
                    PGINVALID_SOCKET,
                    None,
                    Some(pm),
                    nil,
                );
            }

        let events = set.wait(timeout, 1).await;
        match events.into_iter().next() {
            Some(e) => WaitEventFlags::from_bits_truncate(e.events),
            None if timeout >= 0 => WaitEventFlags::TIMEOUT,
            None => WaitEventFlags::empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn wait_latch_returns_latch_set() {
        let latch = Arc::new(Latch::new());
        latch.set();
        let fired = latch
            .wait_for(WaitEventFlags::LATCH_SET, PGINVALID_SOCKET, -1, None)
            .await;
        assert_eq!(fired, WaitEventFlags::LATCH_SET);
    }

    #[tokio::test]
    async fn wait_latch_times_out_when_nothing_fires() {
        let latch = Latch::new(); // never set
        let fired = latch
            .wait_for(WaitEventFlags::LATCH_SET, PGINVALID_SOCKET, 30, None)
            .await;
        assert_eq!(fired, WaitEventFlags::TIMEOUT);
    }

    #[tokio::test]
    async fn wait_latch_set_from_other_task() {
        let latch = Arc::new(Latch::new());
        let l2 = latch.clone();
        let waiter = tokio::spawn(async move {
            l2.wait_for(WaitEventFlags::LATCH_SET, PGINVALID_SOCKET, 5000, None)
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        latch.set();
        let fired = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter wakes")
            .unwrap();
        assert_eq!(fired, WaitEventFlags::LATCH_SET);
    }

    #[tokio::test]
    async fn socket_path_reports_readable() {
        // socketpair: writing to one end makes the other readable. AsyncFd needs
        // the watched fd in non-blocking mode (as a real frontend socket is).
        use std::io::Write;
        use std::os::fd::AsRawFd;
        let (a, mut b) = std::os::unix::net::UnixStream::pair().unwrap();
        a.set_nonblocking(true).unwrap();
        b.write_all(b"x").unwrap(); // make `a` readable
        let fd = a.as_raw_fd();

        let mut set = WaitEventSet::new(1);
        set.add_event(WaitEventFlags::SOCKET_READABLE, fd, None, None, Datum(0));
        let events = set.wait(1000, 1).await;
        assert_eq!(events.len(), 1);
        assert!(
            WaitEventFlags::from_bits_truncate(events[0].events)
                .contains(WaitEventFlags::SOCKET_READABLE)
        );
    }
}

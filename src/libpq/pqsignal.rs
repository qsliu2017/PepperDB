//! Backend signal(2) support.
//!
//! Source: postgres/src/backend/libpq/pqsignal.c
//! Merged decls from: postgres/src/include/libpq/pqsignal.h
//!   (the POSIX `sigset_t` masks UnBlockSig/BlockSig/StartupBlockSig and the
//!    `pqinitmask` prototype). The WIN32 emulation branch of the header is
//!    dropped (this port targets POSIX only).
//!
//! The `pqsignal()` installer below corresponds to src/port/pqsignal.c
//! (referenced by the header banner as "see also src/port/pqsignal.c"). We
//! translate its essential behavior - install a handler with SA_RESTART via
//! sigaction() - REAL via libc. The `wrapper_handler` indirection from
//! src/port/pqsignal.c (which restores errno and guards against MyProcPid
//! drift in system(3) children) is intentionally NOT ported here because it
//! depends on unported globals (MyProcPid, PostmasterPid, IsUnderPostmaster)
//! and the `pqsignal_handlers[]` table; the handler is installed directly.
//!
//! FULLY REAL via the POSIX signal API bound through `extern "C"`.

use crate::prelude::*;

// ---------------------------------------------------------------------------
// Platform sigset_t.
//
// On macOS, sigset_t is a 32-bit unsigned integer. On Linux (glibc), it is an
// opaque 128-byte structure (__sigset_t = { unsigned long __val[16] } on
// 64-bit). We model each with a #[repr(C)] type of the correct size/alignment.
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
pub type sigset_t = u32;

#[cfg(not(target_os = "macos"))]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct sigset_t {
    // glibc __sigset_t: unsigned long __val[_SIGSET_NWORDS], _SIGSET_NWORDS = 1024/(8*sizeof(unsigned long)) = 16 on 64-bit.
    __val: [c_ulong; 16],
}

#[cfg(not(target_os = "macos"))]
impl sigset_t {
    const fn zeroed() -> sigset_t {
        sigset_t { __val: [0; 16] }
    }
}

// ---------------------------------------------------------------------------
// Platform `struct sigaction`.
//
// macOS (sys/signal.h):
//     struct sigaction {
//         union __sigaction_u __sigaction_u;  // function pointer, sa_handler
//         sigset_t sa_mask;
//         int      sa_flags;
//     };
// The union is pointer-sized; we model sa_handler directly as the function
// pointer field since we only ever set SA_RESTART (never SA_SIGINFO).
//
// Linux (bits/sigaction.h, 64-bit):
//     struct sigaction {
//         union { sighandler_t sa_handler; void (*sa_sigaction)(...); } __sigaction_handler;
//         sigset_t  sa_mask;
//         int       sa_flags;
//         void    (*sa_restorer)(void);
//     };
// ---------------------------------------------------------------------------

/// A C signal handler: `void (*)(int)`.
pub type SigHandler = Option<unsafe extern "C" fn(c_int)>;

#[cfg(target_os = "macos")]
#[repr(C)]
pub struct sigaction {
    pub sa_handler: SigHandler, // union __sigaction_u (pointer-sized)
    pub sa_mask: sigset_t,
    pub sa_flags: c_int,
}

#[cfg(not(target_os = "macos"))]
#[repr(C)]
pub struct sigaction {
    pub sa_handler: SigHandler, // union __sigaction_handler (pointer-sized)
    pub sa_mask: sigset_t,
    pub sa_flags: c_int,
    pub sa_restorer: Option<unsafe extern "C" fn()>,
}

// ---------------------------------------------------------------------------
// POSIX signal API bindings.
// ---------------------------------------------------------------------------

extern "C" {
    fn sigemptyset(set: *mut sigset_t) -> c_int;
    fn sigfillset(set: *mut sigset_t) -> c_int;
    fn sigaddset(set: *mut sigset_t, signum: c_int) -> c_int;
    fn sigdelset(set: *mut sigset_t, signum: c_int) -> c_int;
    fn sigismember(set: *const sigset_t, signum: c_int) -> c_int;
    fn sigaction(signum: c_int, act: *const sigaction, oldact: *mut sigaction) -> c_int;
}

// ---------------------------------------------------------------------------
// Signal number and flag constants (POSIX; values identical on macOS & Linux
// for the common signals used here).
// ---------------------------------------------------------------------------

pub const SIGHUP: c_int = 1;
pub const SIGINT: c_int = 2;
pub const SIGQUIT: c_int = 3;
pub const SIGILL: c_int = 4;
pub const SIGTRAP: c_int = 5;
pub const SIGABRT: c_int = 6;
pub const SIGFPE: c_int = 8;
pub const SIGKILL: c_int = 9;
pub const SIGBUS: c_int = if cfg!(target_os = "macos") { 10 } else { 7 };
pub const SIGSEGV: c_int = 11;
pub const SIGSYS: c_int = if cfg!(target_os = "macos") { 12 } else { 31 };
pub const SIGPIPE: c_int = 13;
pub const SIGALRM: c_int = 14;
pub const SIGTERM: c_int = 15;
pub const SIGCONT: c_int = if cfg!(target_os = "macos") { 19 } else { 18 };
pub const SIGCHLD: c_int = if cfg!(target_os = "macos") { 20 } else { 17 };
pub const SIGUSR1: c_int = if cfg!(target_os = "macos") { 30 } else { 10 };
pub const SIGUSR2: c_int = if cfg!(target_os = "macos") { 31 } else { 12 };

/// SA_RESTART: restart interruptible system calls instead of failing with EINTR.
pub const SA_RESTART: c_int = if cfg!(target_os = "macos") { 0x0002 } else { 0x10000000 };
/// SA_NOCLDSTOP: do not generate SIGCHLD when children stop.
pub const SA_NOCLDSTOP: c_int = if cfg!(target_os = "macos") { 0x0008 } else { 0x00000001 };

/// SIG_DFL / SIG_IGN sentinel handlers (cast from integers 0 / 1).
pub const SIG_DFL: SigHandler = None;
// SIG_IGN is the function pointer with value 1; modeled separately where needed.

// ---------------------------------------------------------------------------
// Global signal masks (pqsignal.h: extern PGDLLIMPORT sigset_t ...).
//
// C uses zero-initialized globals filled in by pqinitmask(). We mirror that
// with mutable statics initialized to an all-zero mask. Access is unsafe, as
// in the C original these are process-global and set up once at startup before
// any signal handling.
// ---------------------------------------------------------------------------

#[cfg(target_os = "macos")]
const EMPTY_SIGSET: sigset_t = 0;
#[cfg(not(target_os = "macos"))]
const EMPTY_SIGSET: sigset_t = sigset_t::zeroed();

pub static mut UnBlockSig: sigset_t = EMPTY_SIGSET;
pub static mut BlockSig: sigset_t = EMPTY_SIGSET;
pub static mut StartupBlockSig: sigset_t = EMPTY_SIGSET;

// ---------------------------------------------------------------------------
// pqinitmask - initialize BlockSig, UnBlockSig, and StartupBlockSig.
//
// BlockSig is the set of signals to block when blocking signals: all signals
// we normally expect to get, but NOT signals that should never be turned off.
// StartupBlockSig is BlockSig minus SIGTERM, SIGQUIT, SIGALRM. UnBlockSig is
// the empty set (block nothing).
// ---------------------------------------------------------------------------

pub unsafe fn pqinitmask() {
    sigemptyset(&raw mut UnBlockSig);

    // Note: InitializeWaitEventSupport() modifies UnBlockSig.

    // First set all signals, then clear some.
    sigfillset(&raw mut BlockSig);
    sigfillset(&raw mut StartupBlockSig);

    // Unmark those signals that should never be blocked. All these signals
    // exist on both macOS and Linux, so no per-signal cfg is needed.
    sigdelset(&raw mut BlockSig, SIGTRAP);
    sigdelset(&raw mut StartupBlockSig, SIGTRAP);

    sigdelset(&raw mut BlockSig, SIGABRT);
    sigdelset(&raw mut StartupBlockSig, SIGABRT);

    sigdelset(&raw mut BlockSig, SIGILL);
    sigdelset(&raw mut StartupBlockSig, SIGILL);

    sigdelset(&raw mut BlockSig, SIGFPE);
    sigdelset(&raw mut StartupBlockSig, SIGFPE);

    sigdelset(&raw mut BlockSig, SIGSEGV);
    sigdelset(&raw mut StartupBlockSig, SIGSEGV);

    sigdelset(&raw mut BlockSig, SIGBUS);
    sigdelset(&raw mut StartupBlockSig, SIGBUS);

    sigdelset(&raw mut BlockSig, SIGSYS);
    sigdelset(&raw mut StartupBlockSig, SIGSYS);

    sigdelset(&raw mut BlockSig, SIGCONT);
    sigdelset(&raw mut StartupBlockSig, SIGCONT);

    // Signals unique to startup.
    sigdelset(&raw mut StartupBlockSig, SIGQUIT);
    sigdelset(&raw mut StartupBlockSig, SIGTERM);
    sigdelset(&raw mut StartupBlockSig, SIGALRM);
}

// ---------------------------------------------------------------------------
// pqsignal - set up a signal handler, with SA_RESTART, for signal "signo".
//
// Translated from src/port/pqsignal.c (the backend `pqsignal_be`). The
// errno-saving wrapper_handler indirection is omitted (see module docs); the
// caller-supplied handler is installed directly. SIGCHLD additionally gets
// SA_NOCLDSTOP, matching the C original.
// ---------------------------------------------------------------------------

pub unsafe fn pqsignal(signo: c_int, func: SigHandler) {
    Assert!(signo > 0);

    let mut act: sigaction = core::mem::zeroed();
    act.sa_handler = func;
    sigemptyset(&raw mut act.sa_mask);
    act.sa_flags = SA_RESTART;
    if signo == SIGCHLD {
        act.sa_flags |= SA_NOCLDSTOP;
    }
    if sigaction(signo, &act, null_mut()) < 0 {
        // C: Assert(false) - probably indicates a coding error.
        Assert!(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build the masks and assert sigismember reflects pqinitmask's choices,
    // without installing any process-wide signal disposition.
    #[test]
    fn initmask_membership() {
        unsafe {
            pqinitmask();

            // UnBlockSig blocks nothing.
            assert_eq!(sigismember(&raw const UnBlockSig, SIGTERM), 0);
            assert_eq!(sigismember(&raw const UnBlockSig, SIGUSR1), 0);

            // BlockSig blocks "normal" signals like SIGTERM/SIGHUP/SIGUSR1...
            assert_eq!(sigismember(&raw const BlockSig, SIGTERM), 1);
            assert_eq!(sigismember(&raw const BlockSig, SIGHUP), 1);
            assert_eq!(sigismember(&raw const BlockSig, SIGUSR1), 1);
            // ...but never blocks the "should never block" signals.
            assert_eq!(sigismember(&raw const BlockSig, SIGSEGV), 0);
            assert_eq!(sigismember(&raw const BlockSig, SIGILL), 0);
            assert_eq!(sigismember(&raw const BlockSig, SIGFPE), 0);
            assert_eq!(sigismember(&raw const BlockSig, SIGBUS), 0);
            assert_eq!(sigismember(&raw const BlockSig, SIGCONT), 0);

            // StartupBlockSig is BlockSig minus SIGTERM, SIGQUIT, SIGALRM.
            assert_eq!(sigismember(&raw const StartupBlockSig, SIGTERM), 0);
            assert_eq!(sigismember(&raw const StartupBlockSig, SIGQUIT), 0);
            assert_eq!(sigismember(&raw const StartupBlockSig, SIGALRM), 0);
            // but still blocks the ordinary ones.
            assert_eq!(sigismember(&raw const StartupBlockSig, SIGHUP), 1);
            assert_eq!(sigismember(&raw const StartupBlockSig, SIGUSR1), 1);
        }
    }

    // Install a no-op handler for SIGUSR2 (harmless) and ensure pqsignal
    // returns without tripping the failure Assert.
    #[test]
    fn install_noop_handler() {
        unsafe extern "C" fn noop(_signo: c_int) {}
        unsafe {
            pqsignal(SIGUSR2, Some(noop));
            // Restore default so the test leaves no lingering disposition.
            pqsignal(SIGUSR2, SIG_DFL);
        }
    }
}

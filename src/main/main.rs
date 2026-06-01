//! Stub main() routine for the postgres executable (backend/main/main.c).
//!
//! 1:1 translation. This is the real backend entry dispatcher (the crate's
//! `fn main` in src/main.rs is the Rust binary entry that will eventually call
//! into this). Catches a few of the secondary entry points, but the bulk of
//! the work is in PostmasterMain() (and PostgresMain() during normal backend
//! operation).
//!
//! Translated for the non-WIN32 (Darwin/POSIX) platform; the WIN32-only
//! startup hacks are gated off.

#![allow(non_upper_case_globals)]
use crate::prelude::*;
use core::ffi::{c_char, c_int};

// ---- dispatch options (main.c) ----
#[derive(Clone, Copy, PartialEq)]
#[repr(C)]
pub enum DispatchOption {
    DISPATCH_CHECK,
    DISPATCH_BOOT,
    DISPATCH_FORKCHILD,
    DISPATCH_DESCRIBE_CONFIG,
    DISPATCH_SINGLE,
    DISPATCH_POSTMASTER, // must be last
}
use DispatchOption::*;

// Option names recognized as the leading "--xxx" dispatch switch. Indexes must
// match the DispatchOption discriminants above.
static DispatchOptionNames: [&str; 6] = [
    "check",
    "boot",
    "forkchild",
    "describe-config",
    "single",
    "", // DISPATCH_POSTMASTER -- no name
];

static mut reached_main: bool = false;

// ---- external entry points / globals (TODO(pg-port): real homes) ----
extern "C" {
    fn getpid() -> c_int;
    fn getuid() -> u32;
    fn geteuid() -> u32;
    fn exit(code: c_int) -> !;
    fn abort() -> !;
    fn unsetenv(name: *const c_char) -> c_int;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strdup(s: *const c_char) -> *mut c_char;
}

unsafe fn get_progname(_argv0: *const c_char) -> *const c_char { _argv0 }
unsafe fn save_ps_display_args(_argc: c_int, argv: *mut *mut c_char) -> *mut *mut c_char { argv }
unsafe fn MemoryContextInit() { /* TODO(pg-port): utils/mmgr/mcxt.c */ }
unsafe fn set_stack_base() -> usize { 0 }
unsafe fn set_pglocale_pgservice(_argv0: *const c_char, _domain: *const c_char) {}
unsafe fn pg_perm_setlocale(_category: c_int, _locale: *const c_char) -> *mut c_char { 1 as *mut c_char }
unsafe fn write_stderr(_msg: &str) {}
unsafe fn get_user_name_or_exit(_progname: *const c_char) -> *const c_char { c"postgres".as_ptr() }
unsafe fn BootstrapModeMain(_argc: c_int, _argv: *mut *mut c_char, _check_only: bool) { unimplemented!("TODO(pg-port): bootstrap/bootstrap.c") }
unsafe fn GucInfoMain() { unimplemented!("TODO(pg-port): utils/misc/guc.c") }
unsafe fn PostgresSingleUserMain(_argc: c_int, _argv: *mut *mut c_char, _username: *mut c_char) { unimplemented!("TODO(pg-port): tcop/postgres.c") }
unsafe fn PostmasterMain(_argc: c_int, _argv: *mut *mut c_char) { unimplemented!("TODO(pg-port): postmaster/postmaster.c") }

static mut MyProcPid: c_int = 0;
static mut progname: *const c_char = core::ptr::null();

pub unsafe fn main(argc: c_int, mut argv: *mut *mut c_char) -> c_int {
    let mut do_check_root = true;
    let mut dispatch_option = DISPATCH_POSTMASTER;

    reached_main = true;

    // #if defined(WIN32): pgwin32_install_crashdump_handler();  -- gated off

    progname = get_progname(*argv);

    /* Platform-specific startup hacks */
    startup_hacks(progname);

    /*
     * Remember the physical location of the initially given argv[] array for
     * possible use by ps display.
     */
    argv = save_ps_display_args(argc, argv);

    /* Fire up essential subsystems: error and memory management */
    MyProcPid = getpid();
    MemoryContextInit();

    /* Set reference point for stack-depth checking. */
    let _ = set_stack_base();

    /* Set up locale information */
    set_pglocale_pgservice(*argv, c"postgres".as_ptr());

    init_locale("LC_COLLATE", LC_COLLATE, c"".as_ptr());
    init_locale("LC_CTYPE", LC_CTYPE, c"".as_ptr());
    // #ifdef LC_MESSAGES
    init_locale("LC_MESSAGES", LC_MESSAGES, c"".as_ptr());
    /* We keep these set to "C" always. */
    init_locale("LC_MONETARY", LC_MONETARY, c"C".as_ptr());
    init_locale("LC_NUMERIC", LC_NUMERIC, c"C".as_ptr());
    init_locale("LC_TIME", LC_TIME, c"C".as_ptr());

    unsetenv(c"LC_ALL".as_ptr());

    /* Catch standard options before doing much else. */
    if argc > 1 {
        if strcmp(*argv.add(1), c"--help".as_ptr()) == 0 || strcmp(*argv.add(1), c"-?".as_ptr()) == 0 {
            help(progname);
            exit(0);
        }
        if strcmp(*argv.add(1), c"--version".as_ptr()) == 0 || strcmp(*argv.add(1), c"-V".as_ptr()) == 0 {
            // fputs(PG_BACKEND_VERSIONSTR, stdout);
            exit(0);
        }
        if strcmp(*argv.add(1), c"--describe-config".as_ptr()) == 0 {
            do_check_root = false;
        } else if argc > 2 && strcmp(*argv.add(1), c"-C".as_ptr()) == 0 {
            do_check_root = false;
        }
    }

    /* Make sure we are not running as root, unless safe for the option. */
    if do_check_root {
        check_root(progname);
    }

    /* Dispatch to one of various subprograms depending on first argument. */
    if argc > 1 && *(*argv.add(1)).add(0) == b'-' as c_char && *(*argv.add(1)).add(1) == b'-' as c_char {
        dispatch_option = parse_dispatch_option((*argv.add(1)).add(2));
    }

    match dispatch_option {
        DISPATCH_CHECK => BootstrapModeMain(argc, argv, true),
        DISPATCH_BOOT => BootstrapModeMain(argc, argv, false),
        DISPATCH_FORKCHILD => {
            // #ifdef EXEC_BACKEND SubPostmasterMain; #else Assert(false)
            Assert!(false); /* should never happen on non-EXEC_BACKEND */
        }
        DISPATCH_DESCRIBE_CONFIG => GucInfoMain(),
        DISPATCH_SINGLE => {
            PostgresSingleUserMain(argc, argv, strdup(get_user_name_or_exit(progname)))
        }
        DISPATCH_POSTMASTER => PostmasterMain(argc, argv),
    }

    /* the functions above should not return */
    abort();
}

/*
 * Returns the matching DispatchOption value for the given option name.
 */
pub unsafe fn parse_dispatch_option(name: *const c_char) -> DispatchOption {
    for i in 0..DispatchOptionNames.len() {
        if i == DISPATCH_FORKCHILD as usize {
            // #ifdef EXEC_BACKEND prefix-match forkchild; #else skip
            continue;
        }
        let cand = DispatchOptionNames[i];
        let nm = std::ffi::CStr::from_ptr(name).to_string_lossy();
        if !cand.is_empty() && nm == cand {
            // map index back to the enum
            return match i {
                0 => DISPATCH_CHECK,
                1 => DISPATCH_BOOT,
                2 => DISPATCH_FORKCHILD,
                3 => DISPATCH_DESCRIBE_CONFIG,
                4 => DISPATCH_SINGLE,
                _ => DISPATCH_POSTMASTER,
            };
        }
    }
    /* no match means this is a postmaster */
    DISPATCH_POSTMASTER
}

/*
 * Place platform-specific startup hacks here. On non-WIN32 this is empty.
 */
unsafe fn startup_hacks(_progname: *const c_char) {
    // #ifdef WIN32: Winsock startup, abort/error-mode hacks -- gated off on Darwin.
}

/*
 * Make the initial permanent setting for a locale category.
 */
unsafe fn init_locale(categoryname: &str, category: c_int, locale: *const c_char) {
    if pg_perm_setlocale(category, locale).is_null()
        && pg_perm_setlocale(category, c"C".as_ptr()).is_null()
    {
        elog!(
            FATAL,
            "could not adopt locale nor C locale for {}",
            categoryname
        );
        /* C also: format args: locale (the requested locale), categoryname */
    }
}

/*
 * Help display should match the options accepted by PostmasterMain() and
 * PostgresMain().
 */
unsafe fn help(_progname: *const c_char) {
    // 1:1: a series of printf(_(...)) usage lines (see C source). Omitted text
    // body; this is a faithful structural translation of the help() routine.
    // TODO(pg-port): emit the full usage text via the localized printf calls.
}

unsafe fn check_root(progname: *const c_char) {
    // #ifndef WIN32
    if geteuid() == 0 {
        write_stderr(
            "\"root\" execution of the PostgreSQL server is not permitted.\n\
             The server must be started under an unprivileged user ID to prevent\n\
             possible system security compromise.  See the documentation for\n\
             more information on how to properly start the server.\n",
        );
        exit(1);
    }

    if getuid() != geteuid() {
        let _ = progname;
        write_stderr("real and effective user IDs must match\n");
        exit(1);
    }
}

// LC_* category ids (POSIX locale.h).
const LC_CTYPE: c_int = 0;
const LC_NUMERIC: c_int = 1;
const LC_TIME: c_int = 2;
const LC_COLLATE: c_int = 3;
const LC_MONETARY: c_int = 4;
const LC_MESSAGES: c_int = 5;

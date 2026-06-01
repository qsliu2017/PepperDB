/*
 * fork_process.c
 *	 A simple wrapper on top of fork(). This does not handle the
 *	 EXEC_BACKEND case; it might be extended to do so, but it would be
 *	 considerably more complex.
 *
 * Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/fork_process.c
 *
 * Companion header: src/include/postmaster/fork_process.h
 */

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void, CStr};

// extern statics / functions used here
extern "C" {
    fn fork() -> libc_pid_t;
    fn getpid() -> libc_pid_t;
    fn getenv(name: *const c_char) -> *mut c_char;
    fn open(path: *const c_char, oflag: c_int, ...) -> c_int;
    fn close(fd: c_int) -> c_int;
    fn write(fd: c_int, buf: *const c_void, count: usize) -> isize;
    fn strlen(s: *const c_char) -> usize;
    fn fflush(stream: *mut c_void) -> c_int;
    fn sigprocmask(how: c_int, set: *const SigsetT, oldset: *mut SigsetT) -> c_int;
}

// pid_t
#[allow(non_camel_case_types)]
pub type libc_pid_t = i32;

// sigset_t stand-in.  On the platforms we care about this is an opaque
// blob; we just need something the right size to pass to sigprocmask().
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SigsetT {
    pub __val: [u64; 16],
}

impl SigsetT {
    pub const fn new() -> Self {
        SigsetT { __val: [0; 16] }
    }
}

// SIG_SETMASK from <signal.h>.  Value differs by platform; on Linux/glibc it
// is 2, on macOS/BSD it is 3.
#[cfg(target_os = "linux")]
pub const SIG_SETMASK: c_int = 2;
#[cfg(not(target_os = "linux"))]
pub const SIG_SETMASK: c_int = 3;

// O_WRONLY from <fcntl.h>
pub const O_WRONLY: c_int = 1;

// BlockSig is the set of signals blocked in postmaster children at startup.
// Defined in pqsignal.c; declared here as an extern static mut.
extern "C" {
    static mut BlockSig: SigsetT;
}

// MyProcPid is defined in globals.c (miscadmin.h).
extern "C" {
    static mut MyProcPid: c_int;
}

// pg_strong_random_init() does post-fork initialization for random number
// generation (port/pg_strong_random.c).
extern "C" {
    fn pg_strong_random_init();
}

/*
 * Wrapper for fork(). Return values are the same as those for fork():
 * -1 if the fork failed, 0 in the child process, and the PID of the
 * child in the parent process.  Signals are blocked while forking, so
 * the child must unblock.
 */
#[cfg(not(target_os = "windows"))]
pub unsafe fn fork_process() -> libc_pid_t {
    let result: libc_pid_t;
    let oomfilename: *const c_char;
    let mut save_mask: SigsetT = SigsetT::new();

    /*
     * Flush stdio channels just before fork, to avoid double-output problems.
     */
    fflush(std::ptr::null_mut());

    /*
     * We start postmaster children with signals blocked.  This allows them to
     * install their own handlers before unblocking, to avoid races where they
     * might run the postmaster's handler and miss an important control
     * signal. With more analysis this could potentially be relaxed.
     */
    sigprocmask(
        SIG_SETMASK,
        std::ptr::addr_of!(BlockSig),
        std::ptr::addr_of_mut!(save_mask),
    );
    result = fork();
    if result == 0 {
        /* fork succeeded, in child */
        MyProcPid = getpid();

        /*
         * By default, Linux tends to kill the postmaster in out-of-memory
         * situations, because it blames the postmaster for the sum of child
         * process sizes *including shared memory*.  (This is unbelievably
         * stupid, but the kernel hackers seem uninterested in improving it.)
         * Therefore it's often a good idea to protect the postmaster by
         * setting its OOM score adjustment negative (which has to be done in
         * a root-owned startup script).  Since the adjustment is inherited by
         * child processes, this would ordinarily mean that all the
         * postmaster's children are equally protected against OOM kill, which
         * is not such a good idea.  So we provide this code to allow the
         * children to change their OOM score adjustments again.  Both the
         * file name to write to and the value to write are controlled by
         * environment variables, which can be set by the same startup script
         * that did the original adjustment.
         */
        oomfilename = getenv(c"PG_OOM_ADJUST_FILE".as_ptr());

        if !oomfilename.is_null() {
            /*
             * Use open() not stdio, to ensure we control the open flags. Some
             * Linux security environments reject anything but O_WRONLY.
             */
            let fd: c_int = open(oomfilename, O_WRONLY, 0);

            /* We ignore all errors */
            if fd >= 0 {
                let mut oomvalue: *const c_char = getenv(c"PG_OOM_ADJUST_VALUE".as_ptr());

                if oomvalue.is_null() {
                    /* supply a useful default */
                    oomvalue = c"0".as_ptr();
                }

                let rc: isize = write(fd, oomvalue as *const c_void, strlen(oomvalue));
                let _ = rc;
                close(fd);
            }
        }

        /* do post-fork initialization for random number generation */
        pg_strong_random_init();
    } else {
        /* in parent, restore signal mask */
        sigprocmask(
            SIG_SETMASK,
            std::ptr::addr_of!(save_mask),
            std::ptr::null_mut(),
        );
    }

    result
}

// Silence unused import warning for CStr when not otherwise referenced.
#[allow(unused_imports)]
use CStr as _CStr;

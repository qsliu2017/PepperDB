//-------------------------------------------------------------------------
//
// getpeereid.rs
//		get peer userid for UNIX-domain socket connection
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
//
// IDENTIFICATION
//	  src/port/getpeereid.c
//
//-------------------------------------------------------------------------

// Note: no #[cfg(test)] here -- getpeereid needs a live connected unix-domain
// socket to exercise, so there is no pure logic worth unit-testing.

use crate::prelude::*;

// uid_t / gid_t are 32-bit unsigned on both macOS and Linux.
#[allow(non_camel_case_types)]
pub type uid_t = u32;
#[allow(non_camel_case_types)]
pub type gid_t = u32;

// ---------------------------------------------------------------------------
// macOS branch: HAVE_GETPEEREID.
//
// On the C side this platform simply #defines getpeereid to the libc one
// (declared in <unistd.h>). We cannot name our own Rust fn `getpeereid` and
// also link the libc `getpeereid` under the same name, so import the system
// symbol under a local alias and forward to it.
// ---------------------------------------------------------------------------
#[cfg(target_os = "macos")]
mod sys {
    use super::{gid_t, uid_t};
    use crate::prelude::c_int;
    extern "C" {
        #[link_name = "getpeereid"]
        pub fn sys_getpeereid(sock: c_int, uid: *mut uid_t, gid: *mut gid_t) -> c_int;
    }
}

// BSD-style getpeereid() for platforms that lack it.
#[cfg(target_os = "macos")]
pub unsafe fn getpeereid(sock: c_int, uid: *mut uid_t, gid: *mut gid_t) -> c_int {
    // HAVE_GETPEEREID: just call the libc implementation.
    sys::sys_getpeereid(sock, uid, gid)
}

// ---------------------------------------------------------------------------
// Linux branch: SO_PEERCRED via getsockopt().
// ---------------------------------------------------------------------------
#[cfg(not(target_os = "macos"))]
mod linux_impl {
    use super::{gid_t, uid_t};
    use crate::prelude::{c_int, c_void};

    // <bits/socket.h>
    const SOL_SOCKET: c_int = 1;
    const SO_PEERCRED: c_int = 17;

    // socklen_t is u32 on Linux.
    #[allow(non_camel_case_types)]
    type socklen_t = u32;

    // struct ucred { pid_t pid; uid_t uid; gid_t gid; }; pid_t is c_int.
    #[repr(C)]
    struct ucred {
        pid: c_int,
        uid: u32,
        gid: u32,
    }

    extern "C" {
        fn getsockopt(
            sockfd: c_int,
            level: c_int,
            optname: c_int,
            optval: *mut c_void,
            optlen: *mut socklen_t,
        ) -> c_int;
    }

    // Linux: use getsockopt(SO_PEERCRED).
    pub unsafe fn getpeereid(sock: c_int, uid: *mut uid_t, gid: *mut gid_t) -> c_int {
        let mut peercred: ucred = ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut so_len: socklen_t = core::mem::size_of::<ucred>() as socklen_t;

        if getsockopt(
            sock,
            SOL_SOCKET,
            SO_PEERCRED,
            &mut peercred as *mut ucred as *mut c_void,
            &mut so_len,
        ) != 0
            || so_len != core::mem::size_of::<ucred>() as socklen_t
        {
            return -1;
        }
        *uid = peercred.uid;
        *gid = peercred.gid;
        0
    }
}

#[cfg(not(target_os = "macos"))]
pub unsafe fn getpeereid(sock: c_int, uid: *mut uid_t, gid: *mut gid_t) -> c_int {
    linux_impl::getpeereid(sock, uid, gid)
}

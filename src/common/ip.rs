//! ip.rs
//!   IPv6-aware network access.
//!
//! Port of postgres/src/common/ip.c (PostgreSQL 18.3).
//!
//! Provides `pg_getaddrinfo_all`, `pg_freeaddrinfo_all`, and
//! `pg_getnameinfo_all`, thin wrappers over the libc resolver routines that add
//! a special case for AF_UNIX sockets (handled by our own
//! `getaddrinfo_unix`/`getnameinfo_unix`).
//!
//! This is a shared (frontend/backend) file, so it does NOT use ereport!/elog!.
//!
//! Layout notes / cfg assumptions:
//!  - `struct addrinfo` field order differs between macOS and glibc: macOS puts
//!    `ai_canonname` before `ai_addr`, glibc puts `ai_addr` before
//!    `ai_canonname`. Both layouts are encoded below via cfg.
//!  - `struct sockaddr_un` differs: macOS has a leading `sun_len: u8` and a
//!    `sun_family: u8` with `sun_path[104]`; glibc has no `sun_len`, a
//!    `sun_family: u16`, and `sun_path[108]`.
//!  - The EAI_* error codes differ in value between platforms; both are cfg'd.

use crate::prelude::*;

// AF_UNIX is 1 on both macOS and Linux.
const AF_UNIX: c_int = 1;

// SOCK_STREAM differs: macOS = 1, Linux = 1 as well. (Both are 1.)
const SOCK_STREAM: c_int = 1;

// getaddrinfo error return codes (values are platform-specific).
#[cfg(target_os = "macos")]
const EAI_FAIL: c_int = 4;
#[cfg(target_os = "macos")]
const EAI_MEMORY: c_int = 6;
#[cfg(not(target_os = "macos"))]
const EAI_FAIL: c_int = -4;
#[cfg(not(target_os = "macos"))]
const EAI_MEMORY: c_int = -10;

// socklen_t is u32 on both supported platforms.
#[allow(non_camel_case_types)]
type socklen_t = u32;

// Opaque placeholder for `struct sockaddr` (we only ever pass pointers around).
#[repr(C)]
pub struct sockaddr {
    _private: [u8; 0],
}

// Opaque placeholder for `struct sockaddr_storage`. We only read its leading
// family discriminator; the exact size is unimportant here since callers pass
// us a pointer to a properly sized object. macOS uses ss_len:u8 + ss_family:u8,
// glibc uses ss_family:u16. We expose a helper to read the family.
#[cfg(target_os = "macos")]
#[repr(C)]
pub struct sockaddr_storage {
    pub ss_len: u8,
    pub ss_family: u8,
    _pad: [u8; 126],
}
#[cfg(not(target_os = "macos"))]
#[repr(C)]
pub struct sockaddr_storage {
    pub ss_family: u16,
    _pad: [u8; 126],
}

impl sockaddr_storage {
    #[inline]
    fn family(&self) -> c_int {
        self.ss_family as c_int
    }
}

// struct addrinfo - field order is platform-specific (see header note).
#[cfg(target_os = "macos")]
#[repr(C)]
pub struct addrinfo {
    pub ai_flags: c_int,
    pub ai_family: c_int,
    pub ai_socktype: c_int,
    pub ai_protocol: c_int,
    pub ai_addrlen: socklen_t,
    pub ai_canonname: *mut c_char,
    pub ai_addr: *mut sockaddr,
    pub ai_next: *mut addrinfo,
}

#[cfg(not(target_os = "macos"))]
#[repr(C)]
pub struct addrinfo {
    pub ai_flags: c_int,
    pub ai_family: c_int,
    pub ai_socktype: c_int,
    pub ai_protocol: c_int,
    pub ai_addrlen: socklen_t,
    pub ai_addr: *mut sockaddr,
    pub ai_canonname: *mut c_char,
    pub ai_next: *mut addrinfo,
}

// struct sockaddr_un - macOS has a leading length byte and 104-byte path;
// glibc has no length byte, a u16 family, and a 108-byte path.
#[cfg(target_os = "macos")]
#[repr(C)]
pub struct sockaddr_un {
    pub sun_len: u8,
    pub sun_family: u8,
    pub sun_path: [c_char; 104],
}
#[cfg(not(target_os = "macos"))]
#[repr(C)]
pub struct sockaddr_un {
    pub sun_family: u16,
    pub sun_path: [c_char; 108],
}

impl sockaddr_un {
    #[inline]
    fn set_family(&mut self) {
        #[cfg(target_os = "macos")]
        {
            self.sun_family = AF_UNIX as u8;
        }
        #[cfg(not(target_os = "macos"))]
        {
            self.sun_family = AF_UNIX as u16;
        }
    }

    #[inline]
    fn family(&self) -> c_int {
        self.sun_family as c_int
    }
}

// libc declarations (no `libc` crate per project policy).
extern "C" {
    fn getaddrinfo(
        node: *const c_char,
        service: *const c_char,
        hints: *const addrinfo,
        res: *mut *mut addrinfo,
    ) -> c_int;
    fn freeaddrinfo(res: *mut addrinfo);
    fn getnameinfo(
        sa: *const sockaddr,
        salen: socklen_t,
        host: *mut c_char,
        hostlen: socklen_t,
        serv: *mut c_char,
        servlen: socklen_t,
        flags: c_int,
    ) -> c_int;

    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strlen(s: *const c_char) -> usize;

    // calloc/free are used to build/free the Unix-socket addrinfo, matching the
    // C source's use of calloc/free (NOT palloc) so pg_freeaddrinfo_all's
    // free() path is correct.
    fn calloc(nmemb: usize, size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
}

/// pg_getaddrinfo_all - get address info for Unix, IPv4 and IPv6 sockets.
#[no_mangle]
pub unsafe extern "C" fn pg_getaddrinfo_all(
    hostname: *const c_char,
    servname: *const c_char,
    hintp: *const addrinfo,
    result: *mut *mut addrinfo,
) -> c_int {
    // not all versions of getaddrinfo() zero *result on failure
    *result = null_mut();

    if (*hintp).ai_family == AF_UNIX {
        return getaddrinfo_unix(servname, hintp, result);
    }

    // NULL has special meaning to getaddrinfo().
    let node = if hostname.is_null() || *hostname == 0 {
        null()
    } else {
        hostname
    };

    getaddrinfo(node, servname, hintp, result)
}

/// pg_freeaddrinfo_all - free addrinfo structures for IPv4, IPv6, or Unix.
///
/// Note: the ai_family field of the original hint structure must be passed so
/// that we can tell whether the addrinfo struct was built by the system's
/// getaddrinfo() routine or our own getaddrinfo_unix() routine.
#[no_mangle]
pub unsafe extern "C" fn pg_freeaddrinfo_all(hint_ai_family: c_int, mut ai: *mut addrinfo) {
    if hint_ai_family == AF_UNIX {
        // struct was built by getaddrinfo_unix (see pg_getaddrinfo_all)
        while !ai.is_null() {
            let p = ai;
            ai = (*ai).ai_next;
            free((*p).ai_addr as *mut c_void);
            free(p as *mut c_void);
        }
    } else {
        // struct was built by getaddrinfo()
        if !ai.is_null() {
            freeaddrinfo(ai);
        }
    }
}

/// pg_getnameinfo_all - get name info for Unix, IPv4 and IPv6 sockets.
///
/// The API differs from standard getnameinfo() in two ways: the addr parameter
/// is sockaddr_storage rather than struct sockaddr, and the node and service
/// fields are guaranteed to be filled with something even on failure.
#[no_mangle]
pub unsafe extern "C" fn pg_getnameinfo_all(
    addr: *const sockaddr_storage,
    salen: c_int,
    node: *mut c_char,
    nodelen: c_int,
    service: *mut c_char,
    servicelen: c_int,
    flags: c_int,
) -> c_int {
    let rc = if !addr.is_null() && (*addr).family() == AF_UNIX {
        getnameinfo_unix(
            addr as *const sockaddr_un,
            salen,
            node,
            nodelen,
            service,
            servicelen,
            flags,
        )
    } else {
        getnameinfo(
            addr as *const sockaddr,
            salen as socklen_t,
            node,
            nodelen as socklen_t,
            service,
            servicelen as socklen_t,
            flags,
        )
    };

    if rc != 0 {
        if !node.is_null() {
            strlcpy(node, b"???\0".as_ptr() as *const c_char, nodelen as usize);
        }
        if !service.is_null() {
            strlcpy(
                service,
                b"???\0".as_ptr() as *const c_char,
                servicelen as usize,
            );
        }
    }

    rc
}

/// getaddrinfo_unix - get unix socket info using IPv6-compatible API.
///
/// Bugs: only one addrinfo is set even though hintsp is NULL or ai_socktype is
/// 0; AI_CANONNAME is not supported.
unsafe fn getaddrinfo_unix(
    path: *const c_char,
    hintsp: *const addrinfo,
    result: *mut *mut addrinfo,
) -> c_int {
    *result = null_mut();

    // sizeof(unp->sun_path)
    let sun_path_size = core::mem::size_of::<[c_char; SUN_PATH_LEN]>();
    if (strlen(path) as usize) >= sun_path_size {
        return EAI_FAIL;
    }

    // hints = {0}, then either defaults (NULL hintsp) or a copy of *hintsp.
    let mut hints: addrinfo = core::mem::zeroed();
    if hintsp.is_null() {
        hints.ai_family = AF_UNIX;
        hints.ai_socktype = SOCK_STREAM;
    } else {
        // memcpy(&hints, hintsp, sizeof(hints))
        core::ptr::copy_nonoverlapping(hintsp, &mut hints as *mut addrinfo, 1);
    }

    if hints.ai_socktype == 0 {
        hints.ai_socktype = SOCK_STREAM;
    }

    if hints.ai_family != AF_UNIX {
        // shouldn't have been called
        return EAI_FAIL;
    }

    let aip = calloc(1, core::mem::size_of::<addrinfo>()) as *mut addrinfo;
    if aip.is_null() {
        return EAI_MEMORY;
    }

    let unp = calloc(1, core::mem::size_of::<sockaddr_un>()) as *mut sockaddr_un;
    if unp.is_null() {
        free(aip as *mut c_void);
        return EAI_MEMORY;
    }

    (*aip).ai_family = AF_UNIX;
    (*aip).ai_socktype = hints.ai_socktype;
    (*aip).ai_protocol = hints.ai_protocol;
    (*aip).ai_next = null_mut();
    (*aip).ai_canonname = null_mut();
    *result = aip;

    (*unp).set_family();
    (*aip).ai_addr = unp as *mut sockaddr;
    (*aip).ai_addrlen = core::mem::size_of::<sockaddr_un>() as socklen_t;

    strcpy((*unp).sun_path.as_mut_ptr(), path);

    // If the supplied path starts with '@', replace that with a zero byte for
    // the internal representation. In that mode, the entire sun_path is the
    // address; set the address length to only include the original string's
    // length so the trailing zero bytes do not appear in OS socket lists.
    if *path == b'@' as c_char {
        (*unp).sun_path[0] = 0;
        (*aip).ai_addrlen = (sun_path_offset() + strlen(path) as usize) as socklen_t;
    }

    0
}

/// Convert a unix-socket address to a hostname/service. Counterpart of
/// getnameinfo_unix() in the C source.
unsafe fn getnameinfo_unix(
    sa: *const sockaddr_un,
    _salen: c_int,
    node: *mut c_char,
    nodelen: c_int,
    service: *mut c_char,
    servicelen: c_int,
    _flags: c_int,
) -> c_int {
    // Invalid arguments.
    if sa.is_null()
        || (*sa).family() != AF_UNIX
        || (node.is_null() && service.is_null())
    {
        return EAI_FAIL;
    }

    if !node.is_null() {
        let ret = snprintf_str(node, nodelen as usize, b"[local]\0");
        if ret < 0 || ret >= nodelen {
            return EAI_MEMORY;
        }
    }

    if !service.is_null() {
        // Check whether it looks like an abstract socket, but it could also
        // just be an empty string.
        let p0 = (*sa).sun_path[0];
        let p1 = (*sa).sun_path[1];
        let ret = if p0 == 0 && p1 != 0 {
            // "@%s", sun_path + 1
            snprintf_at(service, servicelen as usize, (*sa).sun_path.as_ptr().add(1))
        } else {
            // "%s", sun_path
            snprintf_cstr(service, servicelen as usize, (*sa).sun_path.as_ptr())
        };
        if ret < 0 || ret >= servicelen {
            return EAI_MEMORY;
        }
    }

    0
}

// ---- small libc/portability helpers ----------------------------------------

// Length of sockaddr_un::sun_path on this platform.
#[cfg(target_os = "macos")]
const SUN_PATH_LEN: usize = 104;
#[cfg(not(target_os = "macos"))]
const SUN_PATH_LEN: usize = 108;

// offsetof(struct sockaddr_un, sun_path).
#[inline]
fn sun_path_offset() -> usize {
    // Stable way to compute the field offset without nightly offset_of!.
    let base = core::mem::MaybeUninit::<sockaddr_un>::uninit();
    let base_ptr = base.as_ptr();
    unsafe {
        let field_ptr = core::ptr::addr_of!((*base_ptr).sun_path);
        (field_ptr as usize) - (base_ptr as usize)
    }
}

extern "C" {
    // strlcpy is a BSD/portability libc function (present on macOS; provided by
    // PostgreSQL's port layer on glibc). Matches the C source's strlcpy() use.
    fn strlcpy(dst: *mut c_char, src: *const c_char, siz: usize) -> usize;

    // C snprintf, used to honor the exact truncation/return semantics the C
    // source relies on (return value compared against the buffer length).
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

/// snprintf(dst, n, "%s", literal) where `literal` is a NUL-terminated byte
/// string; returns the C snprintf return value (chars that would have been
/// written, excluding the NUL).
unsafe fn snprintf_str(dst: *mut c_char, n: usize, literal: &[u8]) -> c_int {
    snprintf(
        dst,
        n,
        b"%s\0".as_ptr() as *const c_char,
        literal.as_ptr() as *const c_char,
    )
}

/// snprintf(dst, n, "%s", src) for a C string pointer.
unsafe fn snprintf_cstr(dst: *mut c_char, n: usize, src: *const c_char) -> c_int {
    snprintf(dst, n, b"%s\0".as_ptr() as *const c_char, src)
}

/// snprintf(dst, n, "@%s", src) for a C string pointer.
unsafe fn snprintf_at(dst: *mut c_char, n: usize, src: *const c_char) -> c_int {
    snprintf(dst, n, b"@%s\0".as_ptr() as *const c_char, src)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pure test of the sun_path copy: building a Unix sockaddr from a path and
    // verifying the bytes land in sun_path with a terminating NUL, mirroring
    // the strcpy() in getaddrinfo_unix.
    #[test]
    fn sun_path_copy_length() {
        unsafe {
            let path = b"/tmp/.s.PGSQL.5432\0";
            let mut un: sockaddr_un = core::mem::zeroed();
            un.set_family();
            strcpy(un.sun_path.as_mut_ptr(), path.as_ptr() as *const c_char);

            assert_eq!(un.family(), AF_UNIX);
            // Copied bytes should match (excluding the trailing NUL count).
            let copied = strlen(un.sun_path.as_ptr());
            assert_eq!(copied, path.len() - 1);
            for (i, &b) in path[..path.len() - 1].iter().enumerate() {
                assert_eq!(un.sun_path[i], b as c_char);
            }
            // Terminating NUL is present.
            assert_eq!(un.sun_path[copied], 0);
        }
    }

    #[test]
    fn sun_path_offset_is_nonzero() {
        // sun_path follows the family (and, on macOS, the length byte), so its
        // offset must be at least 2.
        assert!(sun_path_offset() >= 2);
        assert_eq!(SUN_PATH_LEN, core::mem::size_of::<[c_char; SUN_PATH_LEN]>());
    }
}

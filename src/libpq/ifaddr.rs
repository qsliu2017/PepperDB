//! libpq/ifaddr.c - IP netmask calculations, and enumerating network interfaces.
//!
//! Translated 1:1 from postgres/src/backend/libpq/ifaddr.c.
//!
//! The system network structs (`sockaddr`, `sockaddr_storage`, `sockaddr_in`,
//! `sockaddr_in6`, `in_addr`, `in6_addr`, `ifaddrs`) and the libc primitives
//! (`getifaddrs`/`freeifaddrs`/`socket`/`close`/`strtol`/`memset`/`memcpy`)
//! are not provided by any ported header in this tree, so they are declared
//! locally here, matching the on-disk layout of the platform's <sys/socket.h>,
//! <netinet/in.h> and <ifaddrs.h>.
//!
//! Of the four mutually-exclusive `pg_foreach_ifaddr` implementations in the C
//! source (WIN32 / HAVE_GETIFADDRS / SIOCGIFCONF / loopback fallback), the
//! HAVE_GETIFADDRS variant is the live one on the macOS/Linux build targets and
//! is the one translated below.

use crate::prelude::*;

use crate::port::noblock::pgsocket;
use crate::port::pg_bswap::pg_hton32;
use crate::port::port_api::PGINVALID_SOCKET;

// ---------------------------------------------------------------------------
// libpq/ifaddr.h: the callback signature.
//
//   typedef void (*PgIfAddrCallback) (struct sockaddr *addr,
//                                     struct sockaddr *netmask,
//                                     void *cb_data);
// ---------------------------------------------------------------------------
#[allow(non_camel_case_types)]
pub type PgIfAddrCallback =
    unsafe extern "C" fn(addr: *mut sockaddr, netmask: *mut sockaddr, cb_data: *mut c_void);

// ---------------------------------------------------------------------------
// System constants from <sys/socket.h> / <netinet/in.h>.
// AF_INET is 2 on Linux and macOS; AF_INET6 is 30 on macOS, 10 on Linux.
// SOCK_DGRAM is 2 on Linux, 2 on macOS as well. INADDR_ANY is 0.0.0.0.
// ---------------------------------------------------------------------------
const AF_INET: c_int = 2;

#[cfg(any(target_os = "macos", target_os = "ios"))]
const AF_INET6: c_int = 30;
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
const AF_INET6: c_int = 10;

#[cfg(any(target_os = "macos", target_os = "ios"))]
const SOCK_DGRAM: c_int = 2;
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
const SOCK_DGRAM: c_int = 2;

const INADDR_ANY: uint32 = 0;

// sa_family_t is uint8 on macOS/BSD (sockaddr has a leading sa_len byte) and
// uint16 on Linux (no sa_len).
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
#[allow(non_camel_case_types)]
type sa_family_t = uint8;
#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
)))]
#[allow(non_camel_case_types)]
type sa_family_t = uint16;

#[allow(non_camel_case_types)]
type in_port_t = uint16;

// ---------------------------------------------------------------------------
// struct in_addr / struct in6_addr (<netinet/in.h>).
// ---------------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct in_addr {
    pub s_addr: uint32,
}

#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct in6_addr {
    pub s6_addr: [uint8; 16],
}

// ---------------------------------------------------------------------------
// struct sockaddr (<sys/socket.h>).
//
// macOS/BSD: { uint8 sa_len; uint8 sa_family; char sa_data[14]; }
// Linux:     { uint16 sa_family; char sa_data[14]; }
// ---------------------------------------------------------------------------
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct sockaddr {
    pub sa_len: uint8,
    pub sa_family: sa_family_t,
    pub sa_data: [c_char; 14],
}
#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
)))]
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct sockaddr {
    pub sa_family: sa_family_t,
    pub sa_data: [c_char; 14],
}

// ---------------------------------------------------------------------------
// struct sockaddr_storage (<sys/socket.h>).  Large enough to hold any address;
// 128 bytes on both Linux and macOS.  We model it as an opaque byte blob with
// the leading family field placed at the platform-correct offset, mirroring the
// C union behavior used throughout this file (the code only ever touches
// ->ss_family plus raw bytes via memcpy).
// ---------------------------------------------------------------------------
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct sockaddr_storage {
    pub ss_len: uint8,
    pub ss_family: sa_family_t,
    pub __ss_pad: [c_char; 126],
}
#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
)))]
#[repr(C, align(8))]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct sockaddr_storage {
    pub ss_family: sa_family_t,
    pub __ss_pad: [c_char; 126],
}

// ---------------------------------------------------------------------------
// struct sockaddr_in / struct sockaddr_in6 (<netinet/in.h>).
// ---------------------------------------------------------------------------
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct sockaddr_in {
    pub sin_len: uint8,
    pub sin_family: sa_family_t,
    pub sin_port: in_port_t,
    pub sin_addr: in_addr,
    pub sin_zero: [c_char; 8],
}
#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
)))]
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct sockaddr_in {
    pub sin_family: sa_family_t,
    pub sin_port: in_port_t,
    pub sin_addr: in_addr,
    pub sin_zero: [c_char; 8],
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct sockaddr_in6 {
    pub sin6_len: uint8,
    pub sin6_family: sa_family_t,
    pub sin6_port: in_port_t,
    pub sin6_flowinfo: uint32,
    pub sin6_addr: in6_addr,
    pub sin6_scope_id: uint32,
}
#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
)))]
#[repr(C)]
#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct sockaddr_in6 {
    pub sin6_family: sa_family_t,
    pub sin6_port: in_port_t,
    pub sin6_flowinfo: uint32,
    pub sin6_addr: in6_addr,
    pub sin6_scope_id: uint32,
}

// ---------------------------------------------------------------------------
// struct ifaddrs (<ifaddrs.h>).  Layout is identical on Linux and macOS.
// ---------------------------------------------------------------------------
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct ifaddrs {
    pub ifa_next: *mut ifaddrs,
    pub ifa_name: *mut c_char,
    pub ifa_flags: c_uint,
    pub ifa_addr: *mut sockaddr,
    pub ifa_netmask: *mut sockaddr,
    pub ifa_dstaddr: *mut sockaddr,
    pub ifa_data: *mut c_void,
}

// ---------------------------------------------------------------------------
// libc primitives.
// ---------------------------------------------------------------------------
extern "C" {
    fn getifaddrs(ifap: *mut *mut ifaddrs) -> c_int;
    fn freeifaddrs(ifa: *mut ifaddrs);
    fn socket(domain: c_int, ty: c_int, protocol: c_int) -> c_int;
    fn strtol(nptr: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_long;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// IN6_IS_ADDR_UNSPECIFIED(a): true if the 16 address bytes are all zero.
#[inline]
unsafe fn IN6_IS_ADDR_UNSPECIFIED(a: *const in6_addr) -> bool {
    (*a).s6_addr == [0u8; 16]
}

/*
 * pg_range_sockaddr - is addr within the subnet specified by netaddr/netmask ?
 *
 * Note: caller must already have verified that all three addresses are
 * in the same address family; and AF_UNIX addresses are not supported.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_range_sockaddr(
    addr: *const sockaddr_storage,
    netaddr: *const sockaddr_storage,
    netmask: *const sockaddr_storage,
) -> c_int {
    if (*addr).ss_family as c_int == AF_INET {
        range_sockaddr_AF_INET(
            addr as *const sockaddr_in,
            netaddr as *const sockaddr_in,
            netmask as *const sockaddr_in,
        )
    } else if (*addr).ss_family as c_int == AF_INET6 {
        range_sockaddr_AF_INET6(
            addr as *const sockaddr_in6,
            netaddr as *const sockaddr_in6,
            netmask as *const sockaddr_in6,
        )
    } else {
        0
    }
}

unsafe fn range_sockaddr_AF_INET(
    addr: *const sockaddr_in,
    netaddr: *const sockaddr_in,
    netmask: *const sockaddr_in,
) -> c_int {
    if (((*addr).sin_addr.s_addr ^ (*netaddr).sin_addr.s_addr) & (*netmask).sin_addr.s_addr) == 0 {
        1
    } else {
        0
    }
}

unsafe fn range_sockaddr_AF_INET6(
    addr: *const sockaddr_in6,
    netaddr: *const sockaddr_in6,
    netmask: *const sockaddr_in6,
) -> c_int {
    let mut i: c_int = 0;
    while i < 16 {
        if (((*addr).sin6_addr.s6_addr[i as usize] ^ (*netaddr).sin6_addr.s6_addr[i as usize])
            & (*netmask).sin6_addr.s6_addr[i as usize])
            != 0
        {
            return 0;
        }
        i += 1;
    }

    1
}

/*
 *	pg_sockaddr_cidr_mask - make a network mask of the appropriate family
 *	  and required number of significant bits
 *
 * numbits can be null, in which case the mask is fully set.
 *
 * The resulting mask is placed in *mask, which had better be big enough.
 *
 * Return value is 0 if okay, -1 if not.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_sockaddr_cidr_mask(
    mask: *mut sockaddr_storage,
    numbits: *mut c_char,
    family: c_int,
) -> c_int {
    let mut bits: c_long;
    let mut endptr: *mut c_char = null_mut();

    if numbits.is_null() {
        bits = if family == AF_INET { 32 } else { 128 };
    } else {
        bits = strtol(numbits, &mut endptr, 10);
        if *numbits == b'\0' as c_char || *endptr != b'\0' as c_char {
            return -1;
        }
    }

    match family {
        x if x == AF_INET => {
            let mut mask4: sockaddr_in = core::mem::zeroed();
            let maskl: c_long;

            if bits < 0 || bits > 32 {
                return -1;
            }
            memset(
                &mut mask4 as *mut sockaddr_in as *mut c_void,
                0,
                core::mem::size_of::<sockaddr_in>(),
            );
            /* avoid "x << 32", which is not portable */
            if bits > 0 {
                maskl = ((0xffffffffu64 << (32 - bits as c_int)) & 0xffffffffu64) as c_long;
            } else {
                maskl = 0;
            }
            mask4.sin_addr.s_addr = pg_hton32(maskl as uint32);
            memcpy(
                mask as *mut c_void,
                &mask4 as *const sockaddr_in as *const c_void,
                core::mem::size_of::<sockaddr_in>(),
            );
        }

        x if x == AF_INET6 => {
            let mut mask6: sockaddr_in6 = core::mem::zeroed();
            let mut i: c_int;

            if bits < 0 || bits > 128 {
                return -1;
            }
            memset(
                &mut mask6 as *mut sockaddr_in6 as *mut c_void,
                0,
                core::mem::size_of::<sockaddr_in6>(),
            );
            i = 0;
            while i < 16 {
                if bits <= 0 {
                    mask6.sin6_addr.s6_addr[i as usize] = 0;
                } else if bits >= 8 {
                    mask6.sin6_addr.s6_addr[i as usize] = 0xff;
                } else {
                    mask6.sin6_addr.s6_addr[i as usize] =
                        ((0xff << (8 - bits as c_int)) & 0xff) as uint8;
                }
                bits -= 8;
                i += 1;
            }
            memcpy(
                mask as *mut c_void,
                &mask6 as *const sockaddr_in6 as *const c_void,
                core::mem::size_of::<sockaddr_in6>(),
            );
        }

        _ => {
            return -1;
        }
    }

    (*mask).ss_family = family as sa_family_t;
    0
}

/*
 * Run the callback function for the addr/mask, after making sure the
 * mask is sane for the addr.
 */
unsafe fn run_ifaddr_callback(
    callback: PgIfAddrCallback,
    cb_data: *mut c_void,
    addr: *mut sockaddr,
    mut mask: *mut sockaddr,
) {
    let mut fullmask: sockaddr_storage = core::mem::zeroed();

    if addr.is_null() {
        return;
    }

    /* Check that the mask is valid */
    if !mask.is_null() {
        if (*mask).sa_family != (*addr).sa_family {
            mask = null_mut();
        } else if (*mask).sa_family as c_int == AF_INET {
            if (*(mask as *mut sockaddr_in)).sin_addr.s_addr == INADDR_ANY {
                mask = null_mut();
            }
        } else if (*mask).sa_family as c_int == AF_INET6 {
            if IN6_IS_ADDR_UNSPECIFIED(&(*(mask as *mut sockaddr_in6)).sin6_addr) {
                mask = null_mut();
            }
        }
    }

    /* If mask is invalid, generate our own fully-set mask */
    if mask.is_null() {
        pg_sockaddr_cidr_mask(&mut fullmask, null_mut(), (*addr).sa_family as c_int);
        mask = &mut fullmask as *mut sockaddr_storage as *mut sockaddr;
    }

    callback(addr, mask, cb_data);
}

/*
 * Enumerate the system's network interface addresses and call the callback
 * for each one.  Returns 0 if successful, -1 if trouble.
 *
 * This version uses the getifaddrs() interface, which is available on
 * BSDs, macOS, Solaris, illumos and Linux.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_foreach_ifaddr(
    callback: PgIfAddrCallback,
    cb_data: *mut c_void,
) -> c_int {
    let mut ifa: *mut ifaddrs = null_mut();
    let mut l: *mut ifaddrs;

    if getifaddrs(&mut ifa) < 0 {
        return -1;
    }

    l = ifa;
    while !l.is_null() {
        run_ifaddr_callback(callback, cb_data, (*l).ifa_addr, (*l).ifa_netmask);
        l = (*l).ifa_next;
    }

    freeifaddrs(ifa);
    0
}

// Reference the otherwise-unused socket/pgsocket bindings so the SIOCGIFCONF
// branch's dependencies stay wired without dead-code churn; the live
// `pg_foreach_ifaddr` above is the getifaddrs() variant.
#[allow(dead_code)]
unsafe fn _unused_socket_refs() -> pgsocket {
    let s = socket(AF_INET, SOCK_DGRAM, 0);
    if s == PGINVALID_SOCKET {
        return -1;
    }
    s
}

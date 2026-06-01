//! Translation of postgres/src/backend/utils/adt/inet_cidr_ntop.c
//!
//! The inet/cidr network-number BINARY -> TEXT formatter (BIND-derived, by Paul
//! Vixie / Vadim Kogan, ISC).  Unlike inet_net_ntop (which formats arbitrary
//! host/network addresses), this unit always produces CIDR-style output
//! ("1.2.0.0/16", "1:2::/64"): the mask length is always appended and the host
//! part is zeroed.  Self-contained byte manipulation: no varlena, no fmgr, no
//! catalog.
//!
//! C #includes mapped to this crate:
//!   - "postgres.h"          -> crate::prelude (not otherwise needed here)
//!   - <sys/socket.h>, <netinet/in.h>, <arpa/inet.h>
//!                           -> system AF_INET6 + errno values, inlined as consts
//!                              (mirrors src/port/inet_net_ntop.rs)
//!   - "utils/builtins.h"    -> declares pg_inet_cidr_ntop (this function)
//!   - "utils/inet.h"        -> PGSQL_AF_INET / PGSQL_AF_INET6 constants
//!
//! Translated (all real, nothing stubbed):
//!   - `pg_inet_cidr_ntop`     (public entry, dispatches on `af`)
//!   - `inet_cidr_ntop_ipv4`   (static helper)
//!   - `inet_cidr_ntop_ipv6`   (static helper)
//!
//! Conventions (AF constant names, errno location/values, the SPRINTF-style
//! buffer building via snprintf, and the extern "C" libc bindings) are copied
//! directly from the already-ported sibling src/port/inet_net_ntop.rs.  The
//! goto-based error paths (`goto emsgsize`) become `set_errno(EMSGSIZE); return
//! null_mut()`.

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};

// libc bindings, same set/idiom as inet_net_ntop.rs. We use snprintf (with a
// generous bound) where the C used the unbounded SPRINTF(sprintf) macro; the
// buffers are always known to be large enough at each call site, matching the C
// reasoning.
extern "C" {
    fn snprintf(s: *mut c_char, n: usize, format: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// errno access (platform errno location), mirroring inet_net_ntop.rs.
#[cfg(target_os = "macos")]
extern "C" {
    #[link_name = "__error"]
    fn errno_location() -> *mut c_int;
}
#[cfg(not(target_os = "macos"))]
extern "C" {
    #[link_name = "__errno_location"]
    fn errno_location() -> *mut c_int;
}

#[inline]
unsafe fn set_errno(e: c_int) {
    *errno_location() = e;
}

// <errno.h> values; macOS values used per file-specific notes. Only the NULL
// return matters to callers, not the precise errno number.
const EINVAL: c_int = 22; // 22 on Linux and macOS
#[cfg(target_os = "macos")]
const EMSGSIZE: c_int = 40;
#[cfg(not(target_os = "macos"))]
const EMSGSIZE: c_int = 90;
#[cfg(target_os = "macos")]
const EAFNOSUPPORT: c_int = 47;
#[cfg(not(target_os = "macos"))]
const EAFNOSUPPORT: c_int = 97;

// utils/inet.h: PGSQL_AF_INET == AF_INET (2 on Linux and macOS), and
// PGSQL_AF_INET6 == AF_INET + 1.
const PGSQL_AF_INET: c_int = 2 + 0;
const PGSQL_AF_INET6: c_int = 2 + 1;

/*
 * char *
 * pg_inet_cidr_ntop(af, src, bits, dst, size)
 *	convert network number from network to presentation format.
 *	generates CIDR style result always.
 * return:
 *	pointer to dst, or NULL if an error occurred (check errno).
 * author:
 *	Paul Vixie (ISC), July 1996
 */
///
/// # Safety
/// `src` must point to at least 4 bytes (IPv4) or 16 bytes (IPv6) per `af`;
/// `dst` must be valid for `size` bytes.
pub unsafe fn pg_inet_cidr_ntop(
    af: c_int,
    src: *const c_void,
    bits: c_int,
    dst: *mut c_char,
    size: usize,
) -> *mut c_char {
    match af {
        PGSQL_AF_INET => inet_cidr_ntop_ipv4(src as *const u8, bits, dst, size),
        PGSQL_AF_INET6 => inet_cidr_ntop_ipv6(src as *const u8, bits, dst, size),
        _ => {
            set_errno(EAFNOSUPPORT);
            null_mut()
        }
    }
}

/*
 * static char *
 * inet_cidr_ntop_ipv4(src, bits, dst, size)
 *	convert IPv4 network number from network to presentation format.
 *	generates CIDR style result always.
 * return:
 *	pointer to dst, or NULL if an error occurred (check errno).
 * note:
 *	network byte order assumed.  this means 192.5.5.240/28 has
 *	0b11110000 in its fourth octet.
 * author:
 *	Paul Vixie (ISC), July 1996
 */
unsafe fn inet_cidr_ntop_ipv4(
    src: *const u8,
    bits: c_int,
    dst: *mut c_char,
    mut size: usize,
) -> *mut c_char {
    let odst = dst;
    let mut dst = dst;
    let mut src = src;

    if bits < 0 || bits > 32 {
        set_errno(EINVAL);
        return null_mut();
    }

    if bits == 0 {
        // sizeof "0" == 2 (1 char + NUL)
        if size < "0".len() + 1 {
            set_errno(EMSGSIZE);
            return null_mut();
        }
        *dst = b'0' as c_char;
        dst = dst.add(1);
        size -= 1;
        *dst = 0;
    }

    /* Format whole octets. */
    // C: for (b = bits / 8; b > 0; b--)
    let mut b = bits / 8;
    while b > 0 {
        // sizeof "255." == 5 (4 chars + NUL)
        if size <= "255.".len() + 1 {
            set_errno(EMSGSIZE);
            return null_mut();
        }
        let t = dst;
        // dst += SPRINTF((dst, "%u", *src++));
        let n = snprintf(dst, 4, b"%u\0".as_ptr() as *const c_char, *src as c_int);
        src = src.add(1);
        dst = dst.add(n as usize);
        if b > 1 {
            *dst = b'.' as c_char;
            dst = dst.add(1);
            *dst = 0;
        }
        size -= (dst as usize) - (t as usize);
        b -= 1;
    }

    /* Format partial octet. */
    let b = bits % 8;
    if b > 0 {
        // sizeof ".255" == 5 (4 chars + NUL)
        if size <= ".255".len() + 1 {
            set_errno(EMSGSIZE);
            return null_mut();
        }
        let t = dst;
        if dst != odst {
            *dst = b'.' as c_char;
            dst = dst.add(1);
        }
        // m = ((1 << b) - 1) << (8 - b);
        let m: u32 = (((1u32 << b) - 1) << (8 - b)) & 0xff;
        let n = snprintf(
            dst,
            4,
            b"%u\0".as_ptr() as *const c_char,
            ((*src as u32) & m) as c_int,
        );
        dst = dst.add(n as usize);
        size -= (dst as usize) - (t as usize);
    }

    /* Format CIDR /width. */
    // sizeof "/32" == 4 (3 chars + NUL)
    if size <= "/32".len() + 1 {
        set_errno(EMSGSIZE);
        return null_mut();
    }
    let n = snprintf(dst, 5, b"/%u\0".as_ptr() as *const c_char, bits);
    dst = dst.add(n as usize);
    let _ = dst; // mirror C: final dst value unused after this point
    odst
}

/*
 * static char *
 * inet_cidr_ntop_ipv6(src, bits, dst, size)
 *	convert IPv6 network number from network to presentation format.
 *	generates CIDR style result always. Picks the shortest representation
 *	unless the IP is really IPv4.
 *	always prints specified number of bits (bits).
 * return:
 *	pointer to dst, or NULL if an error occurred (check errno).
 * note:
 *	network byte order assumed.  this means 192.5.5.240/28 has
 *	0x11110000 in its fourth octet.
 * author:
 *	Vadim Kogan (UCB), June 2001
 *	Original version (IPv4) by Paul Vixie (ISC), July 1996
 */
unsafe fn inet_cidr_ntop_ipv6(
    src: *const u8,
    bits: c_int,
    dst: *mut c_char,
    size: usize,
) -> *mut c_char {
    let mut is_ipv4 = 0;
    let mut inbuf = [0u8; 16];
    // char outbuf[sizeof("xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:255.255.255.255/128")];
    // That literal is 49 chars + NUL = 50 bytes.
    let mut outbuf = [0i8; 50];

    if bits < 0 || bits > 128 {
        set_errno(EINVAL);
        return null_mut();
    }

    let cp_start = outbuf.as_mut_ptr();
    let mut cp = cp_start;

    if bits == 0 {
        *cp = b':' as c_char;
        cp = cp.add(1);
        *cp = b':' as c_char;
        cp = cp.add(1);
        *cp = 0;
    } else {
        /* Copy src to private buffer.  Zero host part. */
        let p = ((bits + 7) / 8) as usize;
        memcpy(
            inbuf.as_mut_ptr() as *mut c_void,
            src as *const c_void,
            p,
        );
        memset(
            inbuf.as_mut_ptr().add(p) as *mut c_void,
            0,
            16 - p,
        );
        let b = bits % 8;
        if b != 0 {
            // m = ((u_int) ~0) << (8 - b);
            let m: u32 = (!0u32) << (8 - b);
            inbuf[p - 1] &= (m & 0xff) as u8;
        }

        // s walks inbuf; track as an index to keep bounds-checking sane.
        let s = inbuf;
        let mut si: usize = 0;

        /* how many words need to be displayed in output */
        let mut words = ((bits + 15) / 16) as usize;
        if words == 1 {
            words = 2;
        }

        /* Find the longest substring of zero's */
        let mut zero_s: usize = 0;
        let mut zero_l: usize = 0;
        let mut tmp_zero_s: usize = 0;
        let mut tmp_zero_l: usize = 0;
        // C: for (i = 0; i < (words * 2); i += 2)
        let mut i: usize = 0;
        while i < words * 2 {
            if (s[i] | s[i + 1]) == 0 {
                if tmp_zero_l == 0 {
                    tmp_zero_s = i / 2;
                }
                tmp_zero_l += 1;
            } else if tmp_zero_l != 0 && zero_l < tmp_zero_l {
                zero_s = tmp_zero_s;
                zero_l = tmp_zero_l;
                tmp_zero_l = 0;
            }
            i += 2;
        }

        if tmp_zero_l != 0 && zero_l < tmp_zero_l {
            zero_s = tmp_zero_s;
            zero_l = tmp_zero_l;
        }

        // is_ipv4 detection. Note C uses signed ints throughout; we compare with
        // usize literals. s[10]/s[11]/s[14]/s[15] are read from the 16-byte inbuf.
        if zero_l != words
            && zero_s == 0
            && (zero_l == 6
                || (zero_l == 5 && s[10] == 0xff && s[11] == 0xff)
                || (zero_l == 7 && s[14] != 0 && s[15] != 1))
        {
            is_ipv4 = 1;
        }

        /* Format whole words. */
        // C: for (p = 0; p < words; p++)
        let mut p: usize = 0;
        while p < words {
            if zero_l != 0 && p >= zero_s && p < zero_s + zero_l {
                /* Time to skip some zeros */
                if p == zero_s {
                    *cp = b':' as c_char;
                    cp = cp.add(1);
                }
                if p == words - 1 {
                    *cp = b':' as c_char;
                    cp = cp.add(1);
                }
                si += 1;
                si += 1;
                p += 1;
                continue;
            }

            if is_ipv4 != 0 && p > 5 {
                *cp = if p == 6 { b':' as c_char } else { b'.' as c_char };
                cp = cp.add(1);
                let n = snprintf(cp, 4, b"%u\0".as_ptr() as *const c_char, s[si] as c_int);
                si += 1;
                cp = cp.add(n as usize);
                /* we can potentially drop the last octet */
                if p != 7 || bits > 120 {
                    *cp = b'.' as c_char;
                    cp = cp.add(1);
                    let n = snprintf(cp, 4, b"%u\0".as_ptr() as *const c_char, s[si] as c_int);
                    si += 1;
                    cp = cp.add(n as usize);
                }
            } else {
                if cp != cp_start {
                    *cp = b':' as c_char;
                    cp = cp.add(1);
                }
                // cp += SPRINTF((cp, "%x", *s * 256 + s[1]));
                let word = (s[si] as u32) * 256 + (s[si + 1] as u32);
                let n = snprintf(cp, 5, b"%x\0".as_ptr() as *const c_char, word as c_int);
                cp = cp.add(n as usize);
                si += 2;
            }
            p += 1;
        }
        let _ = si; // mirror C: final s position unused after the loop
    }

    /* Format CIDR /width. */
    // (void) SPRINTF((cp, "/%u", bits));
    snprintf(cp, 6, b"/%u\0".as_ptr() as *const c_char, bits);
    if strlen(cp_start) + 1 > size {
        set_errno(EMSGSIZE);
        return null_mut();
    }
    strcpy(dst, cp_start);
    dst
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: run pg_inet_cidr_ntop into a stack buffer and return the produced
    // C string as a Rust String (None on NULL/error return).
    unsafe fn ntop(af: c_int, src: &[u8], bits: c_int) -> Option<String> {
        let mut buf = [0i8; 64];
        let r = pg_inet_cidr_ntop(
            af,
            src.as_ptr() as *const c_void,
            bits,
            buf.as_mut_ptr(),
            buf.len(),
        );
        if r.is_null() {
            return None;
        }
        let len = strlen(buf.as_ptr());
        let bytes: &[u8] = core::slice::from_raw_parts(buf.as_ptr() as *const u8, len);
        Some(String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[test]
    fn ipv4_basic() {
        unsafe {
            // 192.168.1.0/24 -> CIDR prints only the bits/8 = 3 whole octets
            // (the trailing zero octet beyond the prefix length is dropped).
            assert_eq!(
                ntop(PGSQL_AF_INET, &[192, 168, 1, 0], 24).as_deref(),
                Some("192.168.1/24")
            );
            // /16 prints only the network part (CIDR network number).
            assert_eq!(
                ntop(PGSQL_AF_INET, &[1, 2, 0, 0], 16).as_deref(),
                Some("1.2/16")
            );
            // /32 still appends the mask (unlike inet_net_ntop).
            assert_eq!(
                ntop(PGSQL_AF_INET, &[10, 0, 0, 1], 32).as_deref(),
                Some("10.0.0.1/32")
            );
            // bits == 0 -> "0/0".
            assert_eq!(
                ntop(PGSQL_AF_INET, &[0, 0, 0, 0], 0).as_deref(),
                Some("0/0")
            );
        }
    }

    #[test]
    fn ipv4_partial_octet() {
        unsafe {
            // /28: three whole octets + a partial fourth masked to 0b11110000.
            // 0xff & 0xf0 == 240.
            assert_eq!(
                ntop(PGSQL_AF_INET, &[192, 5, 5, 0xff], 28).as_deref(),
                Some("192.5.5.240/28")
            );
            // /4: just the high nibble of the first octet.
            assert_eq!(
                ntop(PGSQL_AF_INET, &[0xff, 0, 0, 0], 4).as_deref(),
                Some("240/4")
            );
        }
    }

    #[test]
    fn ipv4_bad_bits() {
        unsafe {
            assert_eq!(ntop(PGSQL_AF_INET, &[1, 2, 3, 4], 33), None);
            assert_eq!(ntop(PGSQL_AF_INET, &[1, 2, 3, 4], -1), None);
        }
    }

    #[test]
    fn ipv6_basic() {
        unsafe {
            // 2001:db8::/32 -- network number, words shown = (32+15)/16 = 2.
            let mut a = [0u8; 16];
            a[0] = 0x20;
            a[1] = 0x01;
            a[2] = 0x0d;
            a[3] = 0xb8;
            assert_eq!(
                ntop(PGSQL_AF_INET6, &a, 32).as_deref(),
                Some("2001:db8/32")
            );
            // bits == 0 -> "::/0".
            let z = [0u8; 16];
            assert_eq!(ntop(PGSQL_AF_INET6, &z, 0).as_deref(), Some("::/0"));
        }
    }

    #[test]
    fn ipv6_full() {
        unsafe {
            // /64 -> first four 16-bit words, then /64.
            let a = [
                0x20, 0x01, 0x0d, 0xb8, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04, 0x00,
                0x05, 0x00, 0x06,
            ];
            assert_eq!(
                ntop(PGSQL_AF_INET6, &a, 64).as_deref(),
                Some("2001:db8:1:2/64")
            );
            // Full 128-bit address with every word present.
            assert_eq!(
                ntop(PGSQL_AF_INET6, &a, 128).as_deref(),
                Some("2001:db8:1:2:3:4:5:6/128")
            );
        }
    }

    #[test]
    fn ipv6_host_part_zeroed() {
        unsafe {
            // /24: bits%8 == 0 so no partial-byte mask, but host bytes are zeroed.
            // p = (24+7)/8 = 3 bytes copied; rest zeroed. words = (24+15)/16 = 2.
            let a = [
                0x20, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0, 0, 0, 0, 0,
            ];
            // Only first 3 bytes survive: 0x20 0x01 0xff -> words 0x2001, 0xff00.
            assert_eq!(
                ntop(PGSQL_AF_INET6, &a, 24).as_deref(),
                Some("2001:ff00/24")
            );
        }
    }

    #[test]
    fn ipv6_bad_bits() {
        unsafe {
            assert_eq!(ntop(PGSQL_AF_INET6, &[0u8; 16], 129), None);
            assert_eq!(ntop(PGSQL_AF_INET6, &[0u8; 16], -1), None);
        }
    }

    #[test]
    fn bad_af() {
        unsafe {
            assert_eq!(ntop(99, &[0, 0, 0, 0], 0), None);
        }
    }
}

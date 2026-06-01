//! Translation of postgres/src/port/inet_net_ntop.c
//!
//! The inet/cidr BINARY -> TEXT formatter (BIND-derived, by Paul Vixie / ISC).
//! Converts a host/network address from network (binary) to presentation
//! (text) format.  This unit is self-contained byte manipulation: no varlena,
//! no fmgr, no catalog.
//!
//! Translated:
//!   - `pg_inet_net_ntop`     (public entry, dispatches on `af`)
//!   - `inet_net_ntop_ipv4`   (static helper)
//!   - `inet_net_ntop_ipv6`   (static helper)
//!   - `decoct`               (static helper, IPv4 octet decoder used by v6 path)
//!
//! Note on the C source: this file only contains the *ntop* (binary->text)
//! direction.  `pg_inet_net_pton` lives in inet_net_pton.c (a separate unit),
//! so the tests below round-trip through a tiny in-test byte builder rather than
//! the real pton, which is not part of this translation unit.
//!
//! The C uses `sprintf`/`strlen`/`memset`/`strcpy` plus the `SPRINTF` macro that
//! returns the number of characters written.  We bind `snprintf`/`strlen`/`strcpy`
//! via `extern "C"` and reproduce the exact byte/pointer arithmetic and the
//! goto-based error paths (each `goto emsgsize` / error `return NULL` becomes a
//! direct `errno = ...; return null`).

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};

// libc bindings used exactly as the C does. We use snprintf (with a generous
// bound) where the C used the unbounded SPRINTF(sprintf) macro; the buffers are
// always known to be large enough at each call site, matching the C reasoning.
extern "C" {
    fn snprintf(s: *mut c_char, n: usize, format: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// errno access (platform errno location), mirroring numutils.rs convention.
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

// <errno.h> values. macOS values used (per file-specific notes); these error
// paths are not behaviourally critical -- only the NULL return matters to callers.
const EINVAL: c_int = 22; // 22 on Linux and macOS
#[cfg(target_os = "macos")]
const EMSGSIZE: c_int = 40;
#[cfg(not(target_os = "macos"))]
const EMSGSIZE: c_int = 90;
#[cfg(target_os = "macos")]
const EAFNOSUPPORT: c_int = 47;
#[cfg(not(target_os = "macos"))]
const EAFNOSUPPORT: c_int = 97;

// In a frontend build the C defines these as (AF_INET + 0)/(AF_INET + 1); the
// backend gets them from utils/inet.h. Either way AF_INET == 2 on both Linux and
// macOS, and pg_inet_net_ntop() assumes PGSQL_AF_INET == AF_INET.
const PGSQL_AF_INET: c_int = 2 + 0;
const PGSQL_AF_INET6: c_int = 2 + 1;
// System AF_INET6: 30 on macOS, 10 on Linux. The C also accepts AF_INET6 when it
// differs from PGSQL_AF_INET6, so we mirror that extra case.
#[cfg(target_os = "macos")]
const AF_INET6: c_int = 30;
#[cfg(not(target_os = "macos"))]
const AF_INET6: c_int = 10;

// #define NS_IN6ADDRSZ 16 / #define NS_INT16SZ 2
const NS_IN6ADDRSZ: usize = 16;
const NS_INT16SZ: usize = 2;

/*
 * char *
 * pg_inet_net_ntop(af, src, bits, dst, size)
 *	convert host/network address from network to presentation format.
 *	"src"'s size is determined from its "af".
 * return:
 *	pointer to dst, or NULL if an error occurred (check errno).
 * note:
 *	192.5.5.1/28 has a nonzero host part, which means it isn't a network
 *	as called for by pg_inet_net_pton() but it can be a host address with
 *	an included netmask.
 * author:
 *	Paul Vixie (ISC), October 1998
 */
///
/// # Safety
/// `src` must point to at least 4 bytes (IPv4) or 16 bytes (IPv6) per `af`;
/// `dst` must be valid for `size` bytes.
pub unsafe fn pg_inet_net_ntop(
    af: c_int,
    src: *const c_void,
    bits: c_int,
    dst: *mut c_char,
    size: usize,
) -> *mut c_char {
    /*
     * We need to cover both the address family constants used by the PG inet
     * type (PGSQL_AF_INET and PGSQL_AF_INET6) and those used by the system
     * libraries (AF_INET and AF_INET6).  We can safely assume PGSQL_AF_INET
     * == AF_INET, but the INET6 constants are very likely to be different.
     */
    if af == PGSQL_AF_INET {
        return inet_net_ntop_ipv4(src as *const u8, bits, dst, size);
    }
    if af == PGSQL_AF_INET6 || (AF_INET6 != PGSQL_AF_INET6 && af == AF_INET6) {
        return inet_net_ntop_ipv6(src as *const u8, bits, dst, size);
    }
    set_errno(EAFNOSUPPORT);
    null_mut()
}

/*
 * static char *
 * inet_net_ntop_ipv4(src, bits, dst, size)
 *	convert IPv4 network address from network to presentation format.
 *	"src"'s size is determined from its "af".
 * return:
 *	pointer to dst, or NULL if an error occurred (check errno).
 * note:
 *	network byte order assumed.  this means 192.5.5.240/28 has
 *	0b11110000 in its fourth octet.
 * author:
 *	Paul Vixie (ISC), October 1998
 */
unsafe fn inet_net_ntop_ipv4(
    src: *const u8,
    bits: c_int,
    dst: *mut c_char,
    mut size: usize,
) -> *mut c_char {
    let odst = dst;
    let mut dst = dst;
    let mut src = src;
    let len: c_int = 4;

    if bits < 0 || bits > 32 {
        set_errno(EINVAL);
        return null_mut();
    }

    /* Always format all four octets, regardless of mask length. */
    // C: for (b = len; b > 0; b--)
    let mut b = len;
    while b > 0 {
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
        // dst += SPRINTF((dst, "%u", *src++));
        let n = snprintf(dst, 4, b"%u\0".as_ptr() as *const c_char, *src as c_int);
        src = src.add(1);
        dst = dst.add(n as usize);
        size -= (dst as usize) - (t as usize);
        b -= 1;
    }

    /* don't print masklen if 32 bits */
    if bits != 32 {
        // sizeof "/32" == 4 (3 chars + NUL)
        if size <= "/32".len() + 1 {
            set_errno(EMSGSIZE);
            return null_mut();
        }
        let n = snprintf(dst, 5, b"/%u\0".as_ptr() as *const c_char, bits);
        dst = dst.add(n as usize);
    }

    let _ = dst; // mirror C: final dst value unused after this point
    odst
}

unsafe fn decoct(src: *const u8, bytes: c_int, dst: *mut c_char, mut size: usize) -> c_int {
    let odst = dst;
    let mut dst = dst;
    let mut src = src;

    // C: for (b = 1; b <= bytes; b++)
    let mut b: c_int = 1;
    while b <= bytes {
        // sizeof "255." == 5 (4 chars + NUL)
        if size <= "255.".len() + 1 {
            return 0;
        }
        let t = dst;
        let n = snprintf(dst, 4, b"%u\0".as_ptr() as *const c_char, *src as c_int);
        src = src.add(1);
        dst = dst.add(n as usize);
        if b != bytes {
            *dst = b'.' as c_char;
            dst = dst.add(1);
            *dst = 0;
        }
        size -= (dst as usize) - (t as usize);
        b += 1;
    }
    ((dst as usize) - (odst as usize)) as c_int
}

unsafe fn inet_net_ntop_ipv6(
    src: *const u8,
    bits: c_int,
    dst: *mut c_char,
    size: usize,
) -> *mut c_char {
    /*
     * Note that int32_t and int16_t need only be "at least" large enough to
     * contain a value of the specified size.  On some systems, like Crays,
     * there is no such thing as an integer variable with 16 bits. Keep this
     * in mind if you think this function should have been coded to use
     * pointer overlays.  All the world's not a VAX.
     */
    // char tmp[sizeof "ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255/128"];
    // That literal is 49 chars + NUL = 50 bytes.
    let mut tmp = [0i8; 50];

    // struct { int base, len; } best, cur;
    let mut best_base: c_int;
    let mut best_len: c_int;
    let mut cur_base: c_int;
    let mut cur_len: c_int;

    let mut words = [0u32; NS_IN6ADDRSZ / NS_INT16SZ];

    if bits < -1 || bits > 128 {
        set_errno(EINVAL);
        return null_mut();
    }

    /*
     * Preprocess: Copy the input (bytewise) array into a wordwise array. Find
     * the longest run of 0x00's in src[] for :: shorthanding.
     */
    memset(
        words.as_mut_ptr() as *mut c_void,
        b'\0' as c_int,
        core::mem::size_of_val(&words),
    );
    for i in 0..NS_IN6ADDRSZ {
        // words[i / 2] |= (src[i] << ((1 - (i % 2)) << 3));
        let shift = (1 - (i % 2)) << 3;
        words[i / 2] |= (*src.add(i) as u32) << shift;
    }
    best_base = -1;
    cur_base = -1;
    best_len = 0;
    cur_len = 0;
    for i in 0..(NS_IN6ADDRSZ / NS_INT16SZ) {
        if words[i] == 0 {
            if cur_base == -1 {
                cur_base = i as c_int;
                cur_len = 1;
            } else {
                cur_len += 1;
            }
        } else if cur_base != -1 {
            if best_base == -1 || cur_len > best_len {
                best_base = cur_base;
                best_len = cur_len;
            }
            cur_base = -1;
        }
    }
    if cur_base != -1 {
        if best_base == -1 || cur_len > best_len {
            best_base = cur_base;
            best_len = cur_len;
        }
    }
    if best_base != -1 && best_len < 2 {
        best_base = -1;
    }

    /*
     * Format the result.
     */
    let tp_start = tmp.as_mut_ptr();
    let mut tp = tp_start;
    let mut i: usize = 0;
    while i < (NS_IN6ADDRSZ / NS_INT16SZ) {
        /* Are we inside the best run of 0x00's? */
        if best_base != -1 && (i as c_int) >= best_base && (i as c_int) < (best_base + best_len) {
            if (i as c_int) == best_base {
                *tp = b':' as c_char;
                tp = tp.add(1);
            }
            i += 1;
            continue;
        }
        /* Are we following an initial run of 0x00s or any real hex? */
        if i != 0 {
            *tp = b':' as c_char;
            tp = tp.add(1);
        }
        /* Is this address an encapsulated IPv4? */
        if i == 6
            && best_base == 0
            && (best_len == 6
                || (best_len == 7 && words[7] != 0x0001)
                || (best_len == 5 && words[5] == 0xffff))
        {
            // n = decoct(src + 12, 4, tp, sizeof tmp - (tp - tmp));
            let used = (tp as usize) - (tp_start as usize);
            let n = decoct(src.add(12), 4, tp, tmp.len() - used);
            if n == 0 {
                set_errno(EMSGSIZE);
                return null_mut();
            }
            tp = tp.add(strlen(tp));
            break;
        }
        // tp += SPRINTF((tp, "%x", words[i]));
        let written = snprintf(tp, 5, b"%x\0".as_ptr() as *const c_char, words[i] as c_int);
        tp = tp.add(written as usize);
        i += 1;
    }

    /* Was it a trailing run of 0x00's? */
    if best_base != -1 && (best_base + best_len) == (NS_IN6ADDRSZ / NS_INT16SZ) as c_int {
        *tp = b':' as c_char;
        tp = tp.add(1);
    }
    *tp = 0;

    if bits != -1 && bits != 128 {
        let written = snprintf(tp, 6, b"/%u\0".as_ptr() as *const c_char, bits);
        tp = tp.add(written as usize);
    }

    /*
     * Check for overflow, copy, and we're done.
     */
    if ((tp as usize) - (tp_start as usize)) > size {
        set_errno(EMSGSIZE);
        return null_mut();
    }
    strcpy(dst, tp_start);
    dst
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: run pg_inet_net_ntop into a stack buffer and return the produced
    // C string as a Rust String (None on NULL/error return).
    unsafe fn ntop(af: c_int, src: &[u8], bits: c_int) -> Option<String> {
        let mut buf = [0i8; 64];
        let r = pg_inet_net_ntop(
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
        let bytes: &[u8] =
            core::slice::from_raw_parts(buf.as_ptr() as *const u8, len);
        Some(String::from_utf8(bytes.to_vec()).unwrap())
    }

    #[test]
    fn ipv4_basic() {
        unsafe {
            // 192.168.1.0/24
            assert_eq!(
                ntop(PGSQL_AF_INET, &[192, 168, 1, 0], 24).as_deref(),
                Some("192.168.1.0/24")
            );
            // /32 omits the mask length.
            assert_eq!(
                ntop(PGSQL_AF_INET, &[10, 0, 0, 1], 32).as_deref(),
                Some("10.0.0.1")
            );
            // 0.0.0.0/0
            assert_eq!(
                ntop(PGSQL_AF_INET, &[0, 0, 0, 0], 0).as_deref(),
                Some("0.0.0.0/0")
            );
            // host part preserved (nonzero with short mask).
            assert_eq!(
                ntop(PGSQL_AF_INET, &[192, 5, 5, 1], 28).as_deref(),
                Some("192.5.5.1/28")
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
            // 2001:db8:: with longest-zero-run :: compression. bits=128 omits mask.
            let mut a = [0u8; 16];
            a[0] = 0x20;
            a[1] = 0x01;
            a[2] = 0x0d;
            a[3] = 0xb8;
            assert_eq!(
                ntop(PGSQL_AF_INET6, &a, 128).as_deref(),
                Some("2001:db8::")
            );
            // all-zero -> "::" with mask shown for bits != 128/-1.
            let z = [0u8; 16];
            assert_eq!(ntop(PGSQL_AF_INET6, &z, 0).as_deref(), Some("::/0"));
            // bits == -1 also omits the mask.
            assert_eq!(ntop(PGSQL_AF_INET6, &z, -1).as_deref(), Some("::"));
        }
    }

    #[test]
    fn ipv6_full() {
        unsafe {
            // No long zero run: every word nonzero -> fully expanded.
            let a = [
                0x20, 0x01, 0x0d, 0xb8, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04, 0x00,
                0x05, 0x00, 0x06,
            ];
            assert_eq!(
                ntop(PGSQL_AF_INET6, &a, 64).as_deref(),
                Some("2001:db8:1:2:3:4:5:6/64")
            );
        }
    }

    #[test]
    fn ipv6_v4_mapped() {
        unsafe {
            // ::ffff:1.2.3.4  (best.base==0, best.len==5, words[5]==0xffff)
            let mut a = [0u8; 16];
            a[10] = 0xff;
            a[11] = 0xff;
            a[12] = 1;
            a[13] = 2;
            a[14] = 3;
            a[15] = 4;
            assert_eq!(
                ntop(PGSQL_AF_INET6, &a, 128).as_deref(),
                Some("::ffff:1.2.3.4")
            );
        }
    }

    #[test]
    fn bad_af() {
        unsafe {
            assert_eq!(ntop(99, &[0, 0, 0, 0], 0), None);
        }
    }
}

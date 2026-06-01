//! Translation of postgres/src/backend/utils/adt/inet_net_pton.c
//!
//! The inet/cidr TEXT->BINARY parser (BIND-derived, ISC code). Converts a network
//! number from presentation (text) format to network (binary) format.
//!
//!   Copyright (c) 2004 by Internet Systems Consortium, Inc. ("ISC")
//!   Copyright (c) 1996,1999 by Internet Software Consortium.
//!
//! This file is self-contained byte manipulation: no varlena, no fmgr, no catalog.
//! Functions take/return `c_int` and raw pointers, matching the C signatures.
//!
//! Translated FULLY (no stubs):
//!   - pg_inet_net_pton        (public entry; dispatches on `af`)
//!   - inet_net_pton_ipv4 / inet_cidr_pton_ipv4
//!   - inet_net_pton_ipv6 / inet_cidr_pton_ipv6
//!   - getbits / getv4         (shared IPv6 helpers)
//!
//! These return the number of network bits, or -1 on error (with `errno` set via the
//! platform errno_location pattern, mirroring numutils.rs). The numeric return value
//! matters exactly; the errno value is only read by callers for the message text, so
//! the exact errno numbers are not critical (just nonzero) -- see the TODO below.
//!
//! `<ctype.h>` isdigit/isxdigit/isupper/tolower are bound via extern "C" (the same
//! convention as numutils.rs / scansup.rs). The remaining byte work (strchr, memset,
//! memcpy, strlen) is done directly in Rust over raw pointers / slices.

use core::ffi::{c_char, c_int, c_uchar, c_uint, c_void};

// <ctype.h>: called exactly as the C does, via `is*((unsigned char) ch)`.
extern "C" {
    fn isdigit(ch: c_int) -> c_int;
    fn isxdigit(ch: c_int) -> c_int;
    fn isupper(ch: c_int) -> c_int;
    fn tolower(ch: c_int) -> c_int;
}

// errno access (platform errno location), same pattern as numutils.rs.
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

// <errno.h>. Values differ by OS; these are the macOS values. The exact numbers are
// not load-bearing here (callers only read errno to choose a message string), but we
// match macOS to be precise on this platform.
// TODO(pg-port): pull these from a central errno module once one exists; Linux differs
// (e.g. EAFNOSUPPORT=97, EMSGSIZE=90) -- the values below are macOS's.
const ENOENT: c_int = 2;
#[allow(dead_code)]
const EINVAL: c_int = 22;
const EMSGSIZE: c_int = 40;
const EAFNOSUPPORT: c_int = 47;

// PGSQL_AF_INET / PGSQL_AF_INET6 are (AF_INET + 0) and (AF_INET + 1). The C pulls
// AF_INET from <sys/socket.h>; it is 2 on both Linux and macOS.
const PGSQL_AF_INET: c_int = 2 + 0;
const PGSQL_AF_INET6: c_int = 2 + 1;

// <arpa/nameser.h>-style size macros used by the IPv6 path.
const NS_IN6ADDRSZ: usize = 16;
const NS_INT16SZ: usize = 2;
const NS_INADDRSZ: usize = 4;

#[inline]
unsafe fn set_errno(e: c_int) {
    *errno_location() = e;
}

/// Index of `ch` in the NUL-terminated byte table `tab`, or `None` if absent.
/// This is the C `strchr(tab, ch) - tab` idiom; note that in C, `strchr` matches the
/// terminating '\0' too, but every call site here pre-screens `ch` with `is*digit`, so
/// `ch` is never '\0' when we reach the lookup (matching the C behavior).
#[inline]
fn strchr_index(tab: &[u8], ch: c_int) -> Option<usize> {
    let b = ch as u8;
    tab.iter().position(|&c| c == b)
}

/*
 * int
 * pg_inet_net_pton(af, src, dst, size)
 *	convert network number from presentation to network format.
 *	accepts hex octets, hex strings, decimal octets, and /CIDR.
 *	"size" is in bytes and describes "dst".
 * return:
 *	number of bits, either imputed classfully or specified with /CIDR,
 *	or -1 if some failure occurred (check errno).  ENOENT means it was
 *	not a valid network specification.
 * author:
 *	Paul Vixie (ISC), June 1996
 *
 * Changes:
 *	I added the inet_cidr_pton function (also from Paul) and changed
 *	the names to reflect their current use.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_inet_net_pton(
    af: c_int,
    src: *const c_char,
    dst: *mut c_void,
    size: usize,
) -> c_int {
    // The C compares `size == -1` where size is size_t; that is the all-ones value.
    let size_is_minus_one = size == usize::MAX;
    match af {
        PGSQL_AF_INET => {
            if size_is_minus_one {
                inet_net_pton_ipv4(src, dst as *mut c_uchar)
            } else {
                inet_cidr_pton_ipv4(src, dst as *mut c_uchar, size)
            }
        }
        PGSQL_AF_INET6 => {
            if size_is_minus_one {
                inet_net_pton_ipv6(src, dst as *mut c_uchar)
            } else {
                inet_cidr_pton_ipv6(src, dst as *mut c_uchar, size)
            }
        }
        _ => {
            set_errno(EAFNOSUPPORT);
            -1
        }
    }
}

/*
 * static int
 * inet_cidr_pton_ipv4(src, dst, size)
 *	convert IPv4 network number from presentation to network format.
 *	accepts hex octets, hex strings, decimal octets, and /CIDR.
 *	"size" is in bytes and describes "dst".
 * return:
 *	number of bits, either imputed classfully or specified with /CIDR,
 *	or -1 if some failure occurred (check errno).  ENOENT means it was
 *	not an IPv4 network specification.
 * note:
 *	network byte order assumed.  this means 192.5.5.240/28 has
 *	0b11110000 in its fourth octet.
 * author:
 *	Paul Vixie (ISC), June 1996
 */
unsafe fn inet_cidr_pton_ipv4(src: *const c_char, dst: *mut c_uchar, mut size: usize) -> c_int {
    const XDIGITS: &[u8] = b"0123456789abcdef";
    const DIGITS: &[u8] = b"0123456789";
    let mut n: c_int;
    let mut ch: c_int;
    let mut tmp: c_int = 0;
    let mut dirty: c_int;
    let mut bits: c_int;
    let odst: *const c_uchar = dst;
    let mut dst = dst;
    let mut src = src;

    ch = *src as c_int;
    src = src.add(1);
    if ch == '0' as c_int
        && (*src == 'x' as c_char || *src == 'X' as c_char)
        && isxdigit(*src.add(1) as c_uchar as c_int) != 0
    {
        /* Hexadecimal: Eat nybble string. */
        if size == 0 {
            return emsgsize();
        }
        dirty = 0;
        src = src.add(1); /* skip x or X. */
        loop {
            ch = *src as c_int;
            src = src.add(1);
            if ch == '\0' as c_int || isxdigit(ch as c_uchar as c_int) == 0 {
                break;
            }
            if isupper(ch as c_uchar as c_int) != 0 {
                ch = tolower(ch as c_uchar as c_int);
            }
            n = strchr_index(XDIGITS, ch).unwrap() as c_int;
            debug_assert!(n >= 0 && n <= 15);
            if dirty == 0 {
                tmp = n;
            } else {
                tmp = (tmp << 4) | n;
            }
            dirty += 1;
            if dirty == 2 {
                // C: `if (size-- <= 0U)` -- size is unsigned, so it is the post-decrement
                // test `size == 0` (then size wraps), but the guard above ensured size>0.
                if size == 0 {
                    return emsgsize();
                }
                size -= 1;
                *dst = tmp as c_uchar;
                dst = dst.add(1);
                dirty = 0;
            }
        }
        if dirty != 0 {
            /* Odd trailing nybble? */
            if size == 0 {
                return emsgsize();
            }
            size -= 1;
            *dst = (tmp << 4) as c_uchar;
            dst = dst.add(1);
        }
    } else if isdigit(ch as c_uchar as c_int) != 0 {
        /* Decimal: eat dotted digit string. */
        loop {
            tmp = 0;
            loop {
                n = strchr_index(DIGITS, ch).unwrap() as c_int;
                debug_assert!(n >= 0 && n <= 9);
                tmp *= 10;
                tmp += n;
                if tmp > 255 {
                    return enoent();
                }
                ch = *src as c_int;
                src = src.add(1);
                if ch == '\0' as c_int || isdigit(ch as c_uchar as c_int) == 0 {
                    break;
                }
            }
            if size == 0 {
                return emsgsize();
            }
            size -= 1;
            *dst = tmp as c_uchar;
            dst = dst.add(1);
            if ch == '\0' as c_int || ch == '/' as c_int {
                break;
            }
            if ch != '.' as c_int {
                return enoent();
            }
            ch = *src as c_int;
            src = src.add(1);
            if isdigit(ch as c_uchar as c_int) == 0 {
                return enoent();
            }
        }
    } else {
        return enoent();
    }

    bits = -1;
    if ch == '/' as c_int && isdigit(*src as c_uchar as c_int) != 0 && dst as usize > odst as usize {
        /* CIDR width specifier.  Nothing can follow it. */
        ch = *src as c_int; /* Skip over the /. */
        src = src.add(1);
        bits = 0;
        loop {
            n = strchr_index(DIGITS, ch).unwrap() as c_int;
            debug_assert!(n >= 0 && n <= 9);
            bits *= 10;
            bits += n;
            ch = *src as c_int;
            src = src.add(1);
            if ch == '\0' as c_int || isdigit(ch as c_uchar as c_int) == 0 {
                break;
            }
        }
        if ch != '\0' as c_int {
            return enoent();
        }
        if bits > 32 {
            return emsgsize();
        }
    }

    /* Fiery death and destruction unless we prefetched EOS. */
    if ch != '\0' as c_int {
        return enoent();
    }

    /* If nothing was written to the destination, we found no address. */
    if dst as usize == odst as usize {
        return enoent();
    }
    /* If no CIDR spec was given, infer width from net class. */
    if bits == -1 {
        if *odst >= 240 {
            /* Class E */
            bits = 32;
        } else if *odst >= 224 {
            /* Class D */
            bits = 8;
        } else if *odst >= 192 {
            /* Class C */
            bits = 24;
        } else if *odst >= 128 {
            /* Class B */
            bits = 16;
        } else {
            /* Class A */
            bits = 8;
        }
        /* If imputed mask is narrower than specified octets, widen. */
        let written = dst as isize - odst as isize;
        if (bits as isize) < written * 8 {
            bits = (written * 8) as c_int;
        }

        /*
         * If there are no additional bits specified for a class D address
         * adjust bits to 4.
         */
        if bits == 8 && *odst == 224 {
            bits = 4;
        }
    }
    /* Extend network to cover the actual mask. */
    while (bits as isize) > (dst as isize - odst as isize) * 8 {
        if size == 0 {
            return emsgsize();
        }
        size -= 1;
        *dst = b'\0';
        dst = dst.add(1);
    }
    bits
}

/*
 * int
 * inet_net_pton_ipv4(af, src, dst, *bits)
 *	convert network address from presentation to network format.
 *	accepts inet_pton()'s input for this "af" plus trailing "/CIDR".
 *	"dst" is assumed large enough for its "af".  "bits" is set to the
 *	/CIDR prefix length, which can have defaults (like /32 for IPv4).
 * return:
 *	-1 if an error occurred (inspect errno; ENOENT means bad format).
 *	0 if successful conversion occurred.
 * note:
 *	192.5.5.1/28 has a nonzero host part, which means it isn't a network
 *	as called for by inet_cidr_pton() but it can be a host address with
 *	an included netmask.
 * author:
 *	Paul Vixie (ISC), October 1998
 */
unsafe fn inet_net_pton_ipv4(src: *const c_char, dst: *mut c_uchar) -> c_int {
    const DIGITS: &[u8] = b"0123456789";
    let odst: *const c_uchar = dst;
    let mut n: c_int;
    let mut ch: c_int;
    let mut tmp: c_int;
    let mut bits: c_int;
    let mut size: usize = 4;
    let mut dst = dst;
    let mut src = src;

    /* Get the mantissa. */
    loop {
        ch = *src as c_int;
        src = src.add(1);
        if isdigit(ch as c_uchar as c_int) == 0 {
            break;
        }
        tmp = 0;
        loop {
            n = strchr_index(DIGITS, ch).unwrap() as c_int;
            debug_assert!(n >= 0 && n <= 9);
            tmp *= 10;
            tmp += n;
            if tmp > 255 {
                return enoent();
            }
            ch = *src as c_int;
            src = src.add(1);
            if ch == '\0' as c_int || isdigit(ch as c_uchar as c_int) == 0 {
                break;
            }
        }
        // C: `if (size-- == 0)` -- post-decrement test against 0.
        if size == 0 {
            return emsgsize();
        }
        size -= 1;
        *dst = tmp as c_uchar;
        dst = dst.add(1);
        if ch == '\0' as c_int || ch == '/' as c_int {
            break;
        }
        if ch != '.' as c_int {
            return enoent();
        }
    }

    /* Get the prefix length if any. */
    bits = -1;
    if ch == '/' as c_int && isdigit(*src as c_uchar as c_int) != 0 && dst as usize > odst as usize {
        /* CIDR width specifier.  Nothing can follow it. */
        ch = *src as c_int; /* Skip over the /. */
        src = src.add(1);
        bits = 0;
        loop {
            n = strchr_index(DIGITS, ch).unwrap() as c_int;
            debug_assert!(n >= 0 && n <= 9);
            bits *= 10;
            bits += n;
            ch = *src as c_int;
            src = src.add(1);
            if ch == '\0' as c_int || isdigit(ch as c_uchar as c_int) == 0 {
                break;
            }
        }
        if ch != '\0' as c_int {
            return enoent();
        }
        if bits > 32 {
            return emsgsize();
        }
    }

    /* Fiery death and destruction unless we prefetched EOS. */
    if ch != '\0' as c_int {
        return enoent();
    }

    /* Prefix length can default to /32 only if all four octets spec'd. */
    if bits == -1 {
        if dst as isize - odst as isize == 4 {
            bits = 32;
        } else {
            return enoent();
        }
    }

    /* If nothing was written to the destination, we found no address. */
    if dst as usize == odst as usize {
        return enoent();
    }

    /* If prefix length overspecifies mantissa, life is bad. */
    if (bits / 8) as isize > (dst as isize - odst as isize) {
        return enoent();
    }

    /* Extend address to four octets. */
    while size > 0 {
        size -= 1;
        *dst = 0;
        dst = dst.add(1);
    }

    bits
}

fn getbits(src: *const c_char, bitsp: *mut c_int) -> c_int {
    const DIGITS: &[u8] = b"0123456789";
    let mut n: c_int;
    let mut val: c_int;
    let mut ch: c_char;
    let mut src = src;

    val = 0;
    n = 0;
    unsafe {
        loop {
            ch = *src;
            src = src.add(1);
            if ch == 0 {
                break;
            }
            match strchr_index(DIGITS, ch as c_uchar as c_int) {
                Some(pch) => {
                    let prev_n = n;
                    n += 1;
                    if prev_n != 0 && val == 0 {
                        /* no leading zeros */
                        return 0;
                    }
                    val *= 10;
                    val += pch as c_int;
                    if val > 128 {
                        /* range */
                        return 0;
                    }
                    continue;
                }
                None => return 0,
            }
        }
        if n == 0 {
            return 0;
        }
        *bitsp = val;
    }
    1
}

fn getv4(src: *const c_char, dst: *mut c_uchar, bitsp: *mut c_int) -> c_int {
    const DIGITS: &[u8] = b"0123456789";
    let odst: *const c_uchar = dst;
    let mut n: c_int;
    let mut val: c_uint;
    let mut ch: c_char;
    let mut dst = dst;
    let mut src = src;

    val = 0;
    n = 0;
    unsafe {
        loop {
            ch = *src;
            src = src.add(1);
            if ch == 0 {
                break;
            }
            match strchr_index(DIGITS, ch as c_uchar as c_int) {
                Some(pch) => {
                    let prev_n = n;
                    n += 1;
                    if prev_n != 0 && val == 0 {
                        /* no leading zeros */
                        return 0;
                    }
                    val *= 10;
                    val += pch as c_uint;
                    if val > 255 {
                        /* range */
                        return 0;
                    }
                    continue;
                }
                None => {}
            }
            if ch == '.' as c_char || ch == '/' as c_char {
                if dst as isize - odst as isize > 3 {
                    /* too many octets? */
                    return 0;
                }
                *dst = val as c_uchar;
                dst = dst.add(1);
                if ch == '/' as c_char {
                    return getbits(src, bitsp);
                }
                val = 0;
                n = 0;
                continue;
            }
            return 0;
        }
        if n == 0 {
            return 0;
        }
        if dst as isize - odst as isize > 3 {
            /* too many octets? */
            return 0;
        }
        *dst = val as c_uchar;
        // dst = dst.add(1);  // matches C: *dst++ = val; (post-increment value unused)
    }
    1
}

fn inet_net_pton_ipv6(src: *const c_char, dst: *mut c_uchar) -> c_int {
    unsafe { inet_cidr_pton_ipv6(src, dst, 16) }
}

unsafe fn inet_cidr_pton_ipv6(src: *const c_char, dst: *mut c_uchar, size: usize) -> c_int {
    const XDIGITS_L: &[u8] = b"0123456789abcdef";
    const XDIGITS_U: &[u8] = b"0123456789ABCDEF";
    let mut tmp: [c_uchar; NS_IN6ADDRSZ] = [0; NS_IN6ADDRSZ];
    let mut ch: c_int;
    let mut saw_xdigit: c_int;
    let mut val: c_uint;
    let mut digits: c_int;
    let mut bits: c_int;
    let mut src = src;

    if size < NS_IN6ADDRSZ {
        return emsgsize();
    }

    // memset((tp = tmp), '\0', NS_IN6ADDRSZ);  -- tmp already zeroed above.
    let tp_base: *mut c_uchar = tmp.as_mut_ptr();
    let mut tp: *mut c_uchar = tp_base;
    let mut endp: *mut c_uchar = tp_base.add(NS_IN6ADDRSZ);
    let mut colonp: *mut c_uchar = core::ptr::null_mut();
    /* Leading :: requires some special handling. */
    if *src == ':' as c_char {
        src = src.add(1);
        if *src != ':' as c_char {
            return enoent();
        }
    }
    let mut curtok: *const c_char = src;
    saw_xdigit = 0;
    val = 0;
    digits = 0;
    bits = -1;
    loop {
        ch = *src as c_int;
        src = src.add(1);
        if ch == '\0' as c_int {
            break;
        }

        // pch = strchr(xdigits = xdigits_l, ch); if NULL, pch = strchr(xdigits = xdigits_u, ch)
        let mut found: Option<usize> = strchr_index(XDIGITS_L, ch);
        if found.is_none() {
            found = strchr_index(XDIGITS_U, ch);
        }
        if let Some(pch) = found {
            val <<= 4;
            val |= pch as c_uint;
            digits += 1;
            if digits > 4 {
                return enoent();
            }
            saw_xdigit = 1;
            continue;
        }
        if ch == ':' as c_int {
            curtok = src;
            if saw_xdigit == 0 {
                if !colonp.is_null() {
                    return enoent();
                }
                colonp = tp;
                continue;
            } else if *src == '\0' as c_char {
                return enoent();
            }
            if tp.add(NS_INT16SZ) > endp {
                return enoent();
            }
            *tp = ((val >> 8) & 0xff) as c_uchar;
            tp = tp.add(1);
            *tp = (val & 0xff) as c_uchar;
            tp = tp.add(1);
            saw_xdigit = 0;
            digits = 0;
            val = 0;
            continue;
        }
        if ch == '.' as c_int
            && (tp.add(NS_INADDRSZ) <= endp)
            && getv4(curtok, tp, &mut bits) > 0
        {
            tp = tp.add(NS_INADDRSZ);
            saw_xdigit = 0;
            break; /* '\0' was seen by inet_pton4(). */
        }
        if ch == '/' as c_int && getbits(src, &mut bits) > 0 {
            break;
        }
        return enoent();
    }
    if saw_xdigit != 0 {
        if tp.add(NS_INT16SZ) > endp {
            return enoent();
        }
        *tp = ((val >> 8) & 0xff) as c_uchar;
        tp = tp.add(1);
        *tp = (val & 0xff) as c_uchar;
        tp = tp.add(1);
    }
    if bits == -1 {
        bits = 128;
    }

    endp = tp_base.add(16);

    if !colonp.is_null() {
        /*
         * Since some memmove()'s erroneously fail to handle overlapping
         * regions, we'll do the shift by hand.
         */
        let n = tp as isize - colonp as isize;
        let mut i: isize;

        if tp == endp {
            return enoent();
        }
        i = 1;
        while i <= n {
            *endp.offset(-i) = *colonp.offset(n - i);
            *colonp.offset(n - i) = 0;
            i += 1;
        }
        tp = endp;
    }
    if tp != endp {
        return enoent();
    }

    /*
     * Copy out the result.
     */
    core::ptr::copy_nonoverlapping(tp_base as *const c_uchar, dst, NS_IN6ADDRSZ);

    bits
}

// --- goto-target error paths, mirroring the C `enoent:` / `emsgsize:` labels. ---
#[inline]
fn enoent() -> c_int {
    unsafe { set_errno(ENOENT) };
    -1
}

#[inline]
fn emsgsize() -> c_int {
    unsafe { set_errno(EMSGSIZE) };
    -1
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::c_void;

    /// Tiny inline ntop helper for IPv4 round-trip assertions in tests (not part of
    /// the C file). Formats the first `nbytes` octets as dotted quad.
    fn ipv4_octets(buf: &[u8; 4]) -> String {
        format!("{}.{}.{}.{}", buf[0], buf[1], buf[2], buf[3])
    }

    fn pton_cidr_ipv4(s: &str) -> (c_int, [u8; 4]) {
        let cs = std::ffi::CString::new(s).unwrap();
        let mut dst = [0u8; 4];
        let bits = unsafe {
            pg_inet_net_pton(
                PGSQL_AF_INET,
                cs.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
            )
        };
        (bits, dst)
    }

    fn pton_net_ipv4(s: &str) -> (c_int, [u8; 4]) {
        let cs = std::ffi::CString::new(s).unwrap();
        let mut dst = [0u8; 4];
        let bits = unsafe {
            // size == (size_t)-1 selects inet_net_pton_ipv4
            pg_inet_net_pton(
                PGSQL_AF_INET,
                cs.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                usize::MAX,
            )
        };
        (bits, dst)
    }

    fn pton_cidr_ipv6(s: &str) -> (c_int, [u8; 16]) {
        let cs = std::ffi::CString::new(s).unwrap();
        let mut dst = [0u8; 16];
        let bits = unsafe {
            pg_inet_net_pton(
                PGSQL_AF_INET6,
                cs.as_ptr(),
                dst.as_mut_ptr() as *mut c_void,
                dst.len(),
            )
        };
        (bits, dst)
    }

    #[test]
    fn cidr_ipv4_basic() {
        // "192.168.1.0/24" -> bytes 192.168.1.0, bits 24.
        let (bits, dst) = pton_cidr_ipv4("192.168.1.0/24");
        assert_eq!(bits, 24);
        assert_eq!(&dst, &[192, 168, 1, 0]);
        assert_eq!(ipv4_octets(&dst), "192.168.1.0");
    }

    #[test]
    fn cidr_ipv4_classful_inference() {
        // "10/8": only one octet, class A -> /8.
        let (bits, dst) = pton_cidr_ipv4("10");
        assert_eq!(bits, 8);
        assert_eq!(dst[0], 10);
        // "192.5.5" no CIDR: class C -> /24 (widened to cover 3 octets).
        let (bits2, dst2) = pton_cidr_ipv4("192.5.5");
        assert_eq!(bits2, 24);
        assert_eq!(&dst2[..3], &[192, 5, 5]);
    }

    #[test]
    fn net_ipv4_full_quad_defaults_32() {
        // inet_net_pton_ipv4: all four octets, no /CIDR -> /32.
        let (bits, dst) = pton_net_ipv4("192.168.1.5");
        assert_eq!(bits, 32);
        assert_eq!(&dst, &[192, 168, 1, 5]);
    }

    #[test]
    fn net_ipv4_with_cidr() {
        let (bits, dst) = pton_net_ipv4("10.0.0.0/8");
        assert_eq!(bits, 8);
        assert_eq!(&dst, &[10, 0, 0, 0]);
    }

    #[test]
    fn net_ipv4_partial_without_cidr_is_error() {
        // Fewer than four octets and no /CIDR -> ENOENT (-1).
        let (bits, _) = pton_net_ipv4("192.168");
        assert_eq!(bits, -1);
    }

    #[test]
    fn cidr_ipv4_hex() {
        // "0xC0A80100" -> 192.168.1.0, classfully /32 (class E? 0xC0=192 -> class C /24,
        // widened to 4 octets => 32).
        let (bits, dst) = pton_cidr_ipv4("0xc0a80100");
        assert_eq!(bits, 32);
        assert_eq!(&dst, &[0xc0, 0xa8, 0x01, 0x00]);
    }

    #[test]
    fn cidr_ipv6_basic() {
        // "::1" -> loopback, default /128.
        let (bits, dst) = pton_cidr_ipv6("::1");
        assert_eq!(bits, 128);
        let mut expect = [0u8; 16];
        expect[15] = 1;
        assert_eq!(&dst, &expect);
    }

    #[test]
    fn cidr_ipv6_with_prefix() {
        // "2001:db8::/32".
        let (bits, dst) = pton_cidr_ipv6("2001:db8::/32");
        assert_eq!(bits, 32);
        assert_eq!(dst[0], 0x20);
        assert_eq!(dst[1], 0x01);
        assert_eq!(dst[2], 0x0d);
        assert_eq!(dst[3], 0xb8);
        assert_eq!(&dst[4..], &[0u8; 12]);
    }

    #[test]
    fn cidr_ipv6_embedded_v4() {
        // "::ffff:192.168.1.1" -> last 4 bytes are the v4 address.
        let (bits, dst) = pton_cidr_ipv6("::ffff:192.168.1.1");
        assert_eq!(bits, 128);
        assert_eq!(&dst[10..12], &[0xff, 0xff]);
        assert_eq!(&dst[12..], &[192, 168, 1, 1]);
    }

    #[test]
    fn unsupported_af_sets_eafnosupport() {
        let cs = std::ffi::CString::new("1.2.3.4").unwrap();
        let mut dst = [0u8; 4];
        let r = unsafe {
            pg_inet_net_pton(99, cs.as_ptr(), dst.as_mut_ptr() as *mut c_void, dst.len())
        };
        assert_eq!(r, -1);
        assert_eq!(unsafe { *errno_location() }, EAFNOSUPPORT);
    }
}

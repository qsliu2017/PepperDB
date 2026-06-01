//! Translation of postgres/src/port/inet_aton.c
//!
//! `inet_aton()` - convert an IPv4 dotted-quad (or a/a.b/a.b.c shorthand, in
//! decimal/octal/hex) string into a network-order `struct in_addr`.  Derived from
//! the 4.3BSD code shipped with PostgreSQL for platforms lacking it.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group

use crate::c::uint32;
use crate::port::pg_bswap::pg_hton32;
use core::ffi::{c_char, c_int};

extern "C" {
    fn isdigit(ch: c_int) -> c_int;
    fn isxdigit(ch: c_int) -> c_int;
    fn islower(ch: c_int) -> c_int;
    fn isspace(ch: c_int) -> c_int;
}

/// `struct in_addr` (netinet/in.h): a 32-bit IPv4 address in network byte order.
#[repr(C)]
pub struct in_addr {
    pub s_addr: uint32,
}

/// inet_aton: returns 1 on success (and stores into `*addr` if non-null), 0 on bad input.
///
/// # Safety
/// `cp` is a valid NUL-terminated C string; `addr` is null or writable.
pub unsafe fn inet_aton(mut cp: *const c_char, addr: *mut in_addr) -> c_int {
    let mut val: u32;
    let mut base: u32;
    let mut c: c_char;
    let mut parts: [u32; 4] = [0; 4];
    let mut np: usize = 0; /* index into parts (C used `pp` pointer) */

    loop {
        /*
         * Collect number up to '.'.  Values are specified as for C: 0x=hex,
         * 0=octal, other=decimal.
         */
        val = 0;
        base = 10;
        if *cp as u8 == b'0' {
            cp = cp.add(1);
            if *cp as u8 == b'x' || *cp as u8 == b'X' {
                base = 16;
                cp = cp.add(1);
            } else {
                base = 8;
            }
        }
        loop {
            c = *cp;
            if c == 0 {
                break;
            }
            if isdigit(c as u8 as c_int) != 0 {
                val = val.wrapping_mul(base).wrapping_add((c as u8 - b'0') as u32);
                cp = cp.add(1);
                continue;
            }
            if base == 16 && isxdigit(c as u8 as c_int) != 0 {
                val = (val << 4)
                    + (c as u8 as u32 + 10
                        - (if islower(c as u8 as c_int) != 0 { b'a' } else { b'A' }) as u32);
                cp = cp.add(1);
                continue;
            }
            break;
        }
        if *cp as u8 == b'.' {
            /*
             * Internet format:  a.b.c.d  a.b.c (c is 16 bits)  a.b (b is 24 bits)
             */
            if np >= 3 || val > 0xff {
                return 0;
            }
            parts[np] = val;
            np += 1;
            cp = cp.add(1);
        } else {
            break;
        }
    }

    /* Check for trailing junk. */
    while *cp != 0 {
        let ch = *cp;
        cp = cp.add(1);
        if isspace(ch as u8 as c_int) == 0 {
            return 0;
        }
    }

    /* Concoct the address according to the number of parts specified. */
    let n = np + 1;
    match n {
        1 => { /* a -- 32 bits */ }
        2 => {
            /* a.b -- 8.24 bits */
            if val > 0xff_ffff {
                return 0;
            }
            val |= parts[0] << 24;
        }
        3 => {
            /* a.b.c -- 8.8.16 bits */
            if val > 0xffff {
                return 0;
            }
            val |= (parts[0] << 24) | (parts[1] << 16);
        }
        4 => {
            /* a.b.c.d -- 8.8.8.8 bits */
            if val > 0xff {
                return 0;
            }
            val |= (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8);
        }
        _ => {}
    }
    if !addr.is_null() {
        (*addr).s_addr = pg_hton32(val);
    }
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inet_aton_parses() {
        unsafe {
            let mut a = in_addr { s_addr: 0 };
            // 192.168.1.5 -> 0xC0A80105 in host order, stored network-order.
            assert_eq!(inet_aton(c"192.168.1.5".as_ptr(), &mut a), 1);
            assert_eq!(a.s_addr, pg_hton32(0xC0A8_0105));

            // shorthand "10" -> 0.0.0.10 (a == 32 bits)
            assert_eq!(inet_aton(c"10".as_ptr(), &mut a), 1);
            assert_eq!(a.s_addr, pg_hton32(10));

            // hex + octal components
            assert_eq!(inet_aton(c"0x7f.0.0.01".as_ptr(), &mut a), 1);
            assert_eq!(a.s_addr, pg_hton32(0x7f00_0001));

            // out of range / malformed -> 0
            assert_eq!(inet_aton(c"256.1.1.1".as_ptr(), core::ptr::null_mut()), 0);
            assert_eq!(inet_aton(c"1.2.3.4.5".as_ptr(), core::ptr::null_mut()), 0);
            assert_eq!(inet_aton(c"1.2.3.junk".as_ptr(), core::ptr::null_mut()), 0);
        }
    }
}

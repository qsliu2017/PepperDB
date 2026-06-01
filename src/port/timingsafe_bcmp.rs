//! Translation of postgres/src/port/timingsafe_bcmp.c
//! (declaration in postgres/src/include/port.h)
//!
//! $OpenBSD: timingsafe_bcmp.c,v 1.3 2015/08/31 02:53:57 guenther Exp $
//!
//! Copyright (c) 2010 Damien Miller.  All rights reserved.
//!
//! Permission to use, copy, modify, and distribute this software for any
//! purpose with or without fee is hereby granted, provided that the above
//! copyright notice and this permission notice appear in all copies.
//!
//! THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
//! WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
//! MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
//! ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
//! WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
//! ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
//! OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

use crate::prelude::*;

/// Constant-time memory compare. Returns 0 when the first `n` bytes of `b1` and
/// `b2` are equal, nonzero otherwise. Unlike `memcmp`/`bcmp`, the running time
/// depends only on `n`, not on the contents of the buffers, making it safe for
/// comparing secrets (no early exit, branch-free accumulation).
///
/// # Safety
/// `b1` and `b2` must each be valid for reads of `n` bytes.
#[no_mangle]
pub unsafe extern "C" fn timingsafe_bcmp(b1: *const c_void, b2: *const c_void, n: Size) -> c_int {
    // TODO(pg-port): USE_SSL branch returns CRYPTO_memcmp(b1, b2, n); not yet wired up.
    let mut p1 = b1 as *const c_uchar;
    let mut p2 = b2 as *const c_uchar;
    let mut ret: c_int = 0;

    let mut n = n;
    while n > 0 {
        ret |= (*p1 ^ *p2) as c_int;
        p1 = p1.add(1);
        p2 = p2.add(1);
        n -= 1;
    }
    (ret != 0) as c_int
}

//! Translation of postgres/src/backend/libpq/pqformat.c
//!                (+ the inline pq_sendint*/pq_writeint* helpers from
//!                 postgres/src/include/libpq/pqformat.h)
//!
//! Routines to convert data between the frontend/backend binary wire format and
//! the internal representation, accumulating into / consuming from a StringInfo.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped: lib/stringinfo (via crate::lib::stringinfo), port/pg_bswap
//! (crate::port::pg_bswap pg_hton*/pg_ntoh*).
//!
//! STUBBED (deps not yet ported):
//!  - The character-set-conversion senders/getters pq_sendcountedtext/pq_sendtext/
//!    pq_sendstring/pq_getmsgtext/pq_getmsgstring/pq_puttextmessage need mb/mbutils
//!    (pg_server_to_client / pg_client_to_server), not yet translated.
//!  - The message-framing routines pq_beginmessage[_reuse]/pq_endmessage[_reuse]/
//!    pq_putemptymessage need the libpq comm layer (pq_putmessage, pqcomm.c).
//!  - pq_endtypsend returns a `bytea` and needs varatt.h (SET_VARSIZE) + the varlena
//!    type, not yet translated.

use crate::prelude::*;
use crate::lib::stringinfo::{
    appendBinaryStringInfo, appendStringInfoChar, enlargeStringInfo, initStringInfo, StringInfo,
};
use crate::port::pg_bswap::{pg_hton16, pg_hton32, pg_hton64, pg_ntoh16, pg_ntoh32, pg_ntoh64};
use crate::c::{float4, float8, int64, uint16, uint32, uint64, uint8};
use core::ffi::{c_char, c_int, c_uint, c_void};

/* errcodes.h classification (errcode() shim ignores the value). */
// TODO(pg-port): ERRCODE_PROTOCOL_VIOLATION from utils/errcodes.h.
const _ERRCODE_PROTOCOL_VIOLATION: c_int = 0;

// ================================================================
//   Inline write/send helpers (merged from pqformat.h)
// ================================================================
//
// pq_writeintN: write a (host-endian -> network-endian) integer into the buffer
// at buf->len, advancing buf->len.  Caller must have ensured capacity.

/// # Safety
/// `buf` must point to a valid StringInfo with at least `sizeof` free bytes.
#[inline]
pub unsafe fn pq_writeint8(buf: StringInfo, i: uint8) {
    let ni: uint8 = i;
    Assert!((*buf).len as usize + core::mem::size_of::<uint8>() <= (*buf).maxlen as usize);
    core::ptr::copy_nonoverlapping(
        &ni as *const uint8 as *const c_char,
        (*buf).data.add((*buf).len as usize),
        core::mem::size_of::<uint8>(),
    );
    (*buf).len += core::mem::size_of::<uint8>() as c_int;
}

/// # Safety
/// See [`pq_writeint8`].
#[inline]
pub unsafe fn pq_writeint16(buf: StringInfo, i: uint16) {
    let ni: uint16 = pg_hton16(i);
    Assert!((*buf).len as usize + core::mem::size_of::<uint16>() <= (*buf).maxlen as usize);
    core::ptr::copy_nonoverlapping(
        &ni as *const uint16 as *const c_char,
        (*buf).data.add((*buf).len as usize),
        core::mem::size_of::<uint16>(),
    );
    (*buf).len += core::mem::size_of::<uint16>() as c_int;
}

/// # Safety
/// See [`pq_writeint8`].
#[inline]
pub unsafe fn pq_writeint32(buf: StringInfo, i: uint32) {
    let ni: uint32 = pg_hton32(i);
    Assert!((*buf).len as usize + core::mem::size_of::<uint32>() <= (*buf).maxlen as usize);
    core::ptr::copy_nonoverlapping(
        &ni as *const uint32 as *const c_char,
        (*buf).data.add((*buf).len as usize),
        core::mem::size_of::<uint32>(),
    );
    (*buf).len += core::mem::size_of::<uint32>() as c_int;
}

/// # Safety
/// See [`pq_writeint8`].
#[inline]
pub unsafe fn pq_writeint64(buf: StringInfo, i: uint64) {
    let ni: uint64 = pg_hton64(i);
    Assert!((*buf).len as usize + core::mem::size_of::<uint64>() <= (*buf).maxlen as usize);
    core::ptr::copy_nonoverlapping(
        &ni as *const uint64 as *const c_char,
        (*buf).data.add((*buf).len as usize),
        core::mem::size_of::<uint64>(),
    );
    (*buf).len += core::mem::size_of::<uint64>() as c_int;
}

/// # Safety
/// `buf` must point to a valid StringInfo.
#[inline]
pub unsafe fn pq_sendint8(buf: StringInfo, i: uint8) {
    enlargeStringInfo(buf, core::mem::size_of::<uint8>() as c_int);
    pq_writeint8(buf, i);
}
/// # Safety
/// See [`pq_sendint8`].
#[inline]
pub unsafe fn pq_sendint16(buf: StringInfo, i: uint16) {
    enlargeStringInfo(buf, core::mem::size_of::<uint16>() as c_int);
    pq_writeint16(buf, i);
}
/// # Safety
/// See [`pq_sendint8`].
#[inline]
pub unsafe fn pq_sendint32(buf: StringInfo, i: uint32) {
    enlargeStringInfo(buf, core::mem::size_of::<uint32>() as c_int);
    pq_writeint32(buf, i);
}
/// # Safety
/// See [`pq_sendint8`].
#[inline]
pub unsafe fn pq_sendint64(buf: StringInfo, i: uint64) {
    enlargeStringInfo(buf, core::mem::size_of::<uint64>() as c_int);
    pq_writeint64(buf, i);
}

/// `pq_sendint` - the deprecated width-dispatching sender (kept for back-compat).
///
/// # Safety
/// See [`pq_sendint8`].
pub unsafe fn pq_sendint(buf: StringInfo, i: uint32, b: c_int) {
    match b {
        1 => pq_sendint8(buf, i as uint8),
        2 => pq_sendint16(buf, i as uint16),
        4 => pq_sendint32(buf, i),
        _ => {
            elog!(ERROR, "unsupported integer size {}", b);
        }
    }
}

// ================================================================
//   pqformat.c
// ================================================================

/*
 *		pq_sendbytes	- append raw data to a StringInfo buffer
 *
 * # Safety
 * `buf` valid; `data` readable for `datalen` bytes.
 */
pub unsafe fn pq_sendbytes(buf: StringInfo, data: *const c_void, datalen: c_int) {
    /* use variant that maintains a trailing null-byte, out of caution */
    appendBinaryStringInfo(buf, data, datalen);
}

/*
 *		pq_send_ascii_string - append a null-terminated, ASCII-checked text string
 *
 * This function intentionally bypasses encoding conversion and instead just
 * sends the data as-is, ensuring that all characters are 7-bit ASCII.
 *
 * # Safety
 * `buf` valid; `str` is a NUL-terminated C string.
 */
pub unsafe fn pq_send_ascii_string(buf: StringInfo, mut str: *const c_char) {
    while *str != 0 {
        let mut ch: c_char = *str;

        if (ch as u8) & 0x80 != 0 {
            ch = b'?' as c_char;
        }
        appendStringInfoChar(buf, ch);
        str = str.add(1);
    }
    appendStringInfoChar(buf, b'\0' as c_char);
}

/*
 *		pq_sendfloat4	- append a float4 to a StringInfo buffer
 */
pub unsafe fn pq_sendfloat4(buf: StringInfo, f: float4) {
    // union { float4 f; uint32 i; } reinterpret
    pq_sendint32(buf, f.to_bits());
}

/*
 *		pq_sendfloat8	- append a float8 to a StringInfo buffer
 */
pub unsafe fn pq_sendfloat8(buf: StringInfo, f: float8) {
    // union { float8 f; int64 i; } reinterpret
    pq_sendint64(buf, f.to_bits());
}

/*
 *		pq_begintypsend		- initialize for constructing a bytea result
 *
 * # Safety
 * `buf` points to a (possibly uninitialized) StringInfoData to be init'd.
 */
pub unsafe fn pq_begintypsend(buf: StringInfo) {
    initStringInfo(buf);
    /* Reserve four bytes for the bytea length word */
    appendStringInfoChar(buf, b'\0' as c_char);
    appendStringInfoChar(buf, b'\0' as c_char);
    appendStringInfoChar(buf, b'\0' as c_char);
    appendStringInfoChar(buf, b'\0' as c_char);
}

/*
 *		pq_endtypsend	- finish constructing a bytea result
 *
 * The data buffer is returned as the palloc'd bytea value.  The StringInfoData is
 * assumed to be a local in the caller and need not be pfree'd.
 *
 * # Safety
 * `buf` was initialized by [`pq_begintypsend`] (so its first VARHDRSZ bytes are
 * reserved) and `buf->data` is palloc'd.
 */
pub unsafe fn pq_endtypsend(buf: StringInfo) -> *mut crate::c::bytea {
    let result = (*buf).data as *mut crate::c::bytea;

    /* Insert correct length into bytea length word */
    Assert!((*buf).len >= VARHDRSZ);
    crate::varatt::SET_VARSIZE((*buf).data, (*buf).len);

    result
}

// ---- character-set-conversion senders (need mb/mbutils) ----

pub unsafe fn pq_sendcountedtext(buf: StringInfo, str: *const c_char, slen: c_int) {
    let _ = (buf, str, slen);
    unimplemented!("pq_sendcountedtext: mb/mbutils (pg_server_to_client) not yet translated")
}
pub unsafe fn pq_sendtext(buf: StringInfo, str: *const c_char, slen: c_int) {
    let _ = (buf, str, slen);
    unimplemented!("pq_sendtext: mb/mbutils (pg_server_to_client) not yet translated")
}
pub unsafe fn pq_sendstring(buf: StringInfo, str: *const c_char) {
    let _ = (buf, str);
    unimplemented!("pq_sendstring: mb/mbutils (pg_server_to_client) not yet translated")
}

// ---- message framing (need libpq comm layer pq_putmessage) ----

pub unsafe fn pq_beginmessage(buf: StringInfo, msgtype: c_char) {
    let _ = (buf, msgtype);
    unimplemented!("pq_beginmessage: libpq comm layer not yet translated")
}
pub unsafe fn pq_beginmessage_reuse(buf: StringInfo, msgtype: c_char) {
    let _ = (buf, msgtype);
    unimplemented!("pq_beginmessage_reuse: libpq comm layer not yet translated")
}
pub unsafe fn pq_endmessage(buf: StringInfo) {
    let _ = buf;
    unimplemented!("pq_endmessage: libpq comm layer (pq_putmessage) not yet translated")
}
pub unsafe fn pq_endmessage_reuse(buf: StringInfo) {
    let _ = buf;
    unimplemented!("pq_endmessage_reuse: libpq comm layer (pq_putmessage) not yet translated")
}
pub unsafe fn pq_puttextmessage(msgtype: c_char, str: *const c_char) {
    let _ = (msgtype, str);
    unimplemented!("pq_puttextmessage: libpq comm layer + mbutils not yet translated")
}
pub unsafe fn pq_putemptymessage(msgtype: c_char) {
    let _ = msgtype;
    unimplemented!("pq_putemptymessage: libpq comm layer (pq_putmessage) not yet translated")
}

// ----------------------------------------------------------------
//   Message-reading routines (fully translated; self-contained)
// ----------------------------------------------------------------

/*
 *		pq_getmsgbyte	- get a raw byte from a message buffer
 *
 * # Safety
 * `msg` points to a valid StringInfo whose `data` is valid for `len` bytes.
 */
pub unsafe fn pq_getmsgbyte(msg: StringInfo) -> c_int {
    if (*msg).cursor >= (*msg).len {
        ereport!(ERROR, errmsg!("no data left in message"));
    }
    let b = *(*msg).data.add((*msg).cursor as usize) as u8 as c_int;
    (*msg).cursor += 1;
    b
}

/*
 *		pq_getmsgint	- get a binary integer from a message buffer.  Unsigned.
 *
 * # Safety
 * See [`pq_getmsgbyte`].
 */
pub unsafe fn pq_getmsgint(msg: StringInfo, b: c_int) -> c_uint {
    match b {
        1 => {
            let mut n8: uint8 = 0;
            pq_copymsgbytes(msg, &mut n8 as *mut uint8 as *mut c_void, 1);
            n8 as c_uint
        }
        2 => {
            let mut n16: uint16 = 0;
            pq_copymsgbytes(msg, &mut n16 as *mut uint16 as *mut c_void, 2);
            pg_ntoh16(n16) as c_uint
        }
        4 => {
            let mut n32: uint32 = 0;
            pq_copymsgbytes(msg, &mut n32 as *mut uint32 as *mut c_void, 4);
            pg_ntoh32(n32) as c_uint
        }
        _ => {
            elog!(ERROR, "unsupported integer size {}", b);
            #[allow(unreachable_code)]
            {
                0 /* keep compiler quiet */
            }
        }
    }
}

/*
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer
 *
 * # Safety
 * See [`pq_getmsgbyte`].
 */
pub unsafe fn pq_getmsgint64(msg: StringInfo) -> int64 {
    let mut n64: uint64 = 0;
    pq_copymsgbytes(msg, &mut n64 as *mut uint64 as *mut c_void, core::mem::size_of::<uint64>() as c_int);
    pg_ntoh64(n64) as int64
}

/*
 *		pq_getmsgfloat4 - get a float4 from a message buffer
 *
 * # Safety
 * See [`pq_getmsgbyte`].
 */
pub unsafe fn pq_getmsgfloat4(msg: StringInfo) -> float4 {
    float4::from_bits(pq_getmsgint(msg, 4))
}

/*
 *		pq_getmsgfloat8 - get a float8 from a message buffer
 *
 * # Safety
 * See [`pq_getmsgbyte`].
 */
pub unsafe fn pq_getmsgfloat8(msg: StringInfo) -> float8 {
    float8::from_bits(pq_getmsgint64(msg) as uint64)
}

/*
 *		pq_getmsgbytes	- get raw data from a message buffer.  Returns a pointer
 *		directly into the message buffer.
 *
 * # Safety
 * See [`pq_getmsgbyte`]; the returned pointer aliases `msg->data`.
 */
pub unsafe fn pq_getmsgbytes(msg: StringInfo, datalen: c_int) -> *const c_char {
    if datalen < 0 || datalen > ((*msg).len - (*msg).cursor) {
        ereport!(ERROR, errmsg!("insufficient data left in message"));
    }
    let result = (*msg).data.add((*msg).cursor as usize) as *const c_char;
    (*msg).cursor += datalen;
    result
}

/*
 *		pq_copymsgbytes - copy raw data from a message buffer to caller's buffer.
 *
 * # Safety
 * See [`pq_getmsgbyte`]; `buf` must be writable for `datalen` bytes.
 */
pub unsafe fn pq_copymsgbytes(msg: StringInfo, buf: *mut c_void, datalen: c_int) {
    if datalen < 0 || datalen > ((*msg).len - (*msg).cursor) {
        ereport!(ERROR, errmsg!("insufficient data left in message"));
    }
    core::ptr::copy_nonoverlapping(
        (*msg).data.add((*msg).cursor as usize) as *const c_char,
        buf as *mut c_char,
        datalen as usize,
    );
    (*msg).cursor += datalen;
}

/*
 *		pq_getmsgtext	- get a counted text string (with conversion)  [STUBBED]
 */
pub unsafe fn pq_getmsgtext(msg: StringInfo, rawbytes: c_int, nbytes: *mut c_int) -> *mut c_char {
    let _ = (msg, rawbytes, nbytes);
    unimplemented!("pq_getmsgtext: mb/mbutils (pg_client_to_server) not yet translated")
}

/*
 *		pq_getmsgstring - get a null-terminated text string (with conversion)  [STUBBED]
 */
pub unsafe fn pq_getmsgstring(msg: StringInfo) -> *const c_char {
    let _ = msg;
    unimplemented!("pq_getmsgstring: mb/mbutils (pg_client_to_server) not yet translated")
}

/*
 *		pq_getmsgrawstring - get a null-terminated text string - NO conversion.
 *		Returns a pointer directly into the message buffer.
 *
 * # Safety
 * See [`pq_getmsgbyte`]; the returned pointer aliases `msg->data`.
 */
pub unsafe fn pq_getmsgrawstring(msg: StringInfo) -> *const c_char {
    let str = (*msg).data.add((*msg).cursor as usize);

    /*
     * Safe to use strlen here: a StringInfo has a trailing null byte.  Check we
     * found a null inside the message.
     */
    let slen = strlen(str) as c_int;
    if (*msg).cursor + slen >= (*msg).len {
        ereport!(ERROR, errmsg!("invalid string in message"));
    }
    (*msg).cursor += slen + 1;

    str as *const c_char
}

/*
 *		pq_getmsgend	- verify message fully consumed
 *
 * # Safety
 * See [`pq_getmsgbyte`].
 */
pub unsafe fn pq_getmsgend(msg: StringInfo) {
    if (*msg).cursor != (*msg).len {
        ereport!(ERROR, errmsg!("invalid message format"));
    }
}

// libc strlen for the raw-string scan (string.h, included by postgres.h).
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::stringinfo::StringInfoData;

    // Build a fresh StringInfo on the heap for sending into.
    unsafe fn new_sibuf() -> StringInfo {
        let buf = palloc(core::mem::size_of::<StringInfoData>()) as StringInfo;
        initStringInfo(buf);
        buf
    }

    #[test]
    fn send_then_get_roundtrip() {
        unsafe {
            let buf = new_sibuf();
            pq_sendint16(buf, 0x1234);
            pq_sendint32(buf, 0xDEADBEEF);
            pq_sendint64(buf, 0x0102030405060708);
            pq_sendfloat4(buf, 3.5_f32);
            pq_sendfloat8(buf, -2.25_f64);
            pq_sendint8(buf, 0xAB);

            // network byte order: first byte of the int16 must be the high byte
            assert_eq!(*(*buf).data.add(0) as u8, 0x12);
            assert_eq!(*(*buf).data.add(1) as u8, 0x34);

            // Now read it all back via a reading cursor over the same bytes.
            (*buf).cursor = 0;
            assert_eq!(pq_getmsgint(buf, 2), 0x1234);
            assert_eq!(pq_getmsgint(buf, 4), 0xDEADBEEF);
            assert_eq!(pq_getmsgint64(buf) as u64, 0x0102030405060708);
            assert_eq!(pq_getmsgfloat4(buf), 3.5_f32);
            assert_eq!(pq_getmsgfloat8(buf), -2.25_f64);
            assert_eq!(pq_getmsgbyte(buf), 0xAB);
            pq_getmsgend(buf); // exactly consumed
        }
    }

    #[test]
    #[should_panic]
    fn getmsgint_past_end_errors() {
        unsafe {
            let buf = new_sibuf();
            pq_sendint16(buf, 7);
            (*buf).cursor = 0;
            // ask for 4 bytes when only 2 are present -> ERROR
            pq_getmsgint(buf, 4);
        }
    }
}

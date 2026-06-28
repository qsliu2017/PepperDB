//! PG `src/backend/libpq/pqformat.c` -- formatting and parsing FE/BE messages.
//!
//! Outgoing messages are built up in an expansible buffer and emitted in one
//! `pq_putmessage`. Incoming messages are parsed from an already-received buffer.
//!
//! ## Builders are synchronous; senders are async (rules.md s5)
//!
//! The `pq_send*` appenders only touch an in-memory buffer, so they stay
//! synchronous. Only `pq_endmessage` (and the one-shot `pq_puttextmessage` /
//! `pq_putemptymessage`) reach the socket via `pqcomm::pq_putmessage`, so those
//! are `async`. The `pq_getmsg*` readers operate on an already-received body and
//! are synchronous.
//!
//! ## StringInfo -> [`PqMsg`]
//!
//! C builds a message in a `StringInfo` and stashes the message-type byte in the
//! buffer's `cursor` field. `StringInfo` is tombstoned (`Vec<u8>`/`String`), and
//! a bare `Vec<u8>` cannot carry the type byte, so the message-build buffer is a
//! small [`PqMsg`] (`{ msgtype, data: Vec<u8> }`). The reader side uses
//! [`MsgReader`] (`std::io::Cursor<&[u8]>`), the cursor advancing as the C
//! `StringInfo.cursor` did.

use std::io::Read;

use crate::backend::libpq::pqcomm;
use crate::utils::elog::ERROR;
use crate::utils::errcodes::ERRCODE_PROTOCOL_VIOLATION;
use crate::{elog, ereport};

/// Read cursor over a received message body. PG `StringInfo` + its `cursor`
/// field on the parse side.
pub type MsgReader<'a> = std::io::Cursor<&'a [u8]>;

/// A message being assembled for the frontend. Replaces the C `StringInfo`
/// whose `cursor` field stashed the message-type byte (`pq_beginmessage`).
#[derive(Debug, Clone, Default)]
pub struct PqMsg {
    /// The message-type byte (C: stashed in `StringInfo.cursor`).
    pub msgtype: u8,
    /// The message body, appended to by the `pq_send*` builders.
    pub data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Message assembly (synchronous builders)
// ---------------------------------------------------------------------------

/// PG `pq_beginmessage`: start a message of `msgtype`.
pub fn pq_beginmessage(buf: &mut PqMsg, msgtype: u8) {
    buf.msgtype = msgtype;
    buf.data.clear();
}

/// PG `pq_beginmessage_reuse`: start a message, reusing the buffer's capacity.
pub fn pq_beginmessage_reuse(buf: &mut PqMsg, msgtype: u8) {
    buf.msgtype = msgtype;
    buf.data.clear();
}

/// PG `pq_sendbytes`: append raw data.
pub fn pq_sendbytes(buf: &mut PqMsg, data: &[u8]) {
    buf.data.extend_from_slice(data);
}

/// PG `pq_sendcountedtext`: append a 4-byte length followed by the string. M1
/// has no encoding conversion (`pg_server_to_client` is identity), so the count
/// is the byte length and the bytes are appended verbatim.
pub fn pq_sendcountedtext(buf: &mut PqMsg, s: &str) {
    let bytes = s.as_bytes();
    pq_sendint32(buf, bytes.len() as u32);
    buf.data.extend_from_slice(bytes);
}

/// PG `pq_sendtext`: append a (non-null-terminated) text string. No conversion
/// in M1.
pub fn pq_sendtext(buf: &mut PqMsg, s: &str) {
    buf.data.extend_from_slice(s.as_bytes());
}

/// PG `pq_sendstring`: append a null-terminated text string. No conversion in M1.
pub fn pq_sendstring(buf: &mut PqMsg, s: &str) {
    buf.data.extend_from_slice(s.as_bytes());
    buf.data.push(0);
}

/// PG `pq_send_ascii_string`: append a null-terminated string, replacing any
/// non-7-bit-ASCII byte with `?` (used for last-ditch error messages).
pub fn pq_send_ascii_string(buf: &mut PqMsg, s: &str) {
    for &b in s.as_bytes() {
        buf.data.push(if b & 0x80 != 0 { b'?' } else { b });
    }
    buf.data.push(0);
}

/// PG `pq_sendbyte`: append a raw byte.
pub fn pq_sendbyte(buf: &mut PqMsg, byt: u8) {
    buf.data.push(byt);
}

/// PG `pq_sendint8` (1-byte int).
pub fn pq_sendint8(buf: &mut PqMsg, i: u8) {
    buf.data.push(i);
}

/// PG `pq_sendint16` (2-byte int, network byte order).
pub fn pq_sendint16(buf: &mut PqMsg, i: u16) {
    buf.data.extend_from_slice(&i.to_be_bytes());
}

/// PG `pq_sendint32` (4-byte int, network byte order).
pub fn pq_sendint32(buf: &mut PqMsg, i: u32) {
    buf.data.extend_from_slice(&i.to_be_bytes());
}

/// PG `pq_sendint64` (8-byte int, network byte order).
pub fn pq_sendint64(buf: &mut PqMsg, i: u64) {
    buf.data.extend_from_slice(&i.to_be_bytes());
}

/// PG `pq_sendint`: append a binary integer of `b` bytes (1/2/4).
pub fn pq_sendint(buf: &mut PqMsg, i: u32, b: i32) {
    match b {
        1 => pq_sendint8(buf, i as u8),
        2 => pq_sendint16(buf, i as u16),
        4 => pq_sendint32(buf, i),
        _ => crate::assert!(false, "unsupported integer size {b}"),
    }
}

/// PG `pq_sendfloat4`: float4 is byte-swapped the same as int4.
pub fn pq_sendfloat4(buf: &mut PqMsg, f: f32) {
    pq_sendint32(buf, f.to_bits());
}

/// PG `pq_sendfloat8`: float8 is byte-swapped the same as int8.
pub fn pq_sendfloat8(buf: &mut PqMsg, f: f64) {
    pq_sendint64(buf, f.to_bits());
}

/// PG `pq_begintypsend`: start a bytea result, reserving 4 bytes for the varlena
/// length word.
pub fn pq_begintypsend(buf: &mut PqMsg) {
    buf.msgtype = 0;
    buf.data.clear();
    buf.data.extend_from_slice(&[0, 0, 0, 0]);
}

/// PG `pq_endtypsend`: finish a bytea result. The varlena length-word patching
/// (`SET_VARSIZE`) belongs to the varlena layer (deferred); for M1 this returns
/// the assembled bytes including the reserved 4-byte header.
pub fn pq_endtypsend(buf: PqMsg) -> Vec<u8> {
    buf.data
}

// ---------------------------------------------------------------------------
// Message output (async -- reach the socket via pqcomm::pq_putmessage)
// ---------------------------------------------------------------------------

/// PG `pq_endmessage`: send the completed message to the frontend, consuming the
/// buffer (the C `pfree(buf->data)`).
pub async fn pq_endmessage(buf: PqMsg) {
    let _ = pqcomm::pq_putmessage(buf.msgtype, &buf.data).await;
}

/// PG `pq_endmessage_reuse`: send the completed message, leaving `buf.data`
/// intact so the buffer can be reused via `pq_beginmessage_reuse`.
pub async fn pq_endmessage_reuse(buf: &PqMsg) {
    let _ = pqcomm::pq_putmessage(buf.msgtype, &buf.data).await;
}

/// PG `pq_puttextmessage`: send a one-step null-terminated text message. No
/// encoding conversion in M1.
pub async fn pq_puttextmessage(msgtype: u8, s: &str) {
    let mut body = Vec::with_capacity(s.len() + 1);
    body.extend_from_slice(s.as_bytes());
    body.push(0);
    let _ = pqcomm::pq_putmessage(msgtype, &body).await;
}

/// PG `pq_putemptymessage`: send a message with an empty body.
pub async fn pq_putemptymessage(msgtype: u8) {
    let _ = pqcomm::pq_putmessage(msgtype, &[]).await;
}

// ---------------------------------------------------------------------------
// Message parsing (synchronous readers over an already-received body)
// ---------------------------------------------------------------------------

/// How many bytes remain unread in the reader.
fn remaining(msg: &MsgReader) -> usize {
    let total = msg.get_ref().len() as u64;
    (total - msg.position()) as usize
}

/// PG `pq_getmsgbyte`: read one raw byte. Protocol error if none left.
pub fn pq_getmsgbyte(msg: &mut MsgReader) -> i32 {
    let mut b = [0u8; 1];
    if msg.read_exact(&mut b).is_err() {
        ereport!(ERROR, |e| {
            e.errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("no data left in message");
        });
        unreachable!("ereport!(ERROR) raises");
    }
    i32::from(b[0])
}

/// PG `pq_getmsgint`: read an unsigned integer of `b` bytes (network order).
pub fn pq_getmsgint(msg: &mut MsgReader, b: i32) -> u32 {
    match b {
        1 => {
            let mut n = [0u8; 1];
            pq_copymsgbytes(msg, &mut n);
            u32::from(n[0])
        }
        2 => {
            let mut n = [0u8; 2];
            pq_copymsgbytes(msg, &mut n);
            u32::from(u16::from_be_bytes(n))
        }
        4 => {
            let mut n = [0u8; 4];
            pq_copymsgbytes(msg, &mut n);
            u32::from_be_bytes(n)
        }
        _ => {
            elog!(ERROR, format!("unsupported integer size {b}"));
            unreachable!("elog!(ERROR) raises");
        }
    }
}

/// PG `pq_getmsgint64`: read a binary 8-byte int (network order).
pub fn pq_getmsgint64(msg: &mut MsgReader) -> i64 {
    let mut n = [0u8; 8];
    pq_copymsgbytes(msg, &mut n);
    i64::from_be_bytes(n)
}

/// PG `pq_getmsgfloat4`.
pub fn pq_getmsgfloat4(msg: &mut MsgReader) -> f32 {
    f32::from_bits(pq_getmsgint(msg, 4))
}

/// PG `pq_getmsgfloat8`.
pub fn pq_getmsgfloat8(msg: &mut MsgReader) -> f64 {
    f64::from_bits(pq_getmsgint64(msg) as u64)
}

/// PG `pq_getmsgbytes`: borrow `datalen` raw bytes directly from the body,
/// advancing the cursor. Protocol error if insufficient data.
pub fn pq_getmsgbytes<'a>(msg: &mut MsgReader<'a>, datalen: i32) -> &'a [u8] {
    if datalen < 0 || datalen as usize > remaining(msg) {
        ereport!(ERROR, |e| {
            e.errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("insufficient data left in message");
        });
    }
    let start = msg.position() as usize;
    let end = start + datalen as usize;
    let full: &'a [u8] = msg.get_ref();
    msg.set_position(end as u64);
    &full[start..end]
}

/// PG `pq_copymsgbytes`: copy raw data out of the body into `buf`. Protocol
/// error if insufficient data.
pub fn pq_copymsgbytes(msg: &mut MsgReader, buf: &mut [u8]) {
    if buf.len() > remaining(msg) {
        ereport!(ERROR, |e| {
            e.errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("insufficient data left in message");
        });
    }
    let start = msg.position() as usize;
    buf.copy_from_slice(&msg.get_ref()[start..start + buf.len()]);
    msg.set_position((start + buf.len()) as u64);
}

/// PG `pq_getmsgtext`: read `rawbytes` and return the decoded text. No encoding
/// conversion in M1, so this is the raw bytes as a `String` (lossy, since the
/// body is not guaranteed valid UTF-8 in general; M1 client encoding is UTF-8).
pub fn pq_getmsgtext(msg: &mut MsgReader, rawbytes: i32) -> String {
    let bytes = pq_getmsgbytes(msg, rawbytes);
    String::from_utf8_lossy(bytes).into_owned()
}

/// PG `pq_getmsgstring`: read a null-terminated string (with conversion).
/// Returns the bytes between the cursor and the terminating NUL. Protocol error
/// if no NUL is found inside the message.
pub fn pq_getmsgstring<'a>(msg: &mut MsgReader<'a>) -> &'a str {
    getmsg_cstring(msg)
}

/// PG `pq_getmsgrawstring`: read a null-terminated string with NO conversion.
pub fn pq_getmsgrawstring<'a>(msg: &mut MsgReader<'a>) -> &'a str {
    getmsg_cstring(msg)
}

/// Shared body of `pq_getmsgstring`/`pq_getmsgrawstring` (encoding conversion is
/// identity in M1). Returns the string up to (not including) the NUL terminator.
fn getmsg_cstring<'a>(msg: &mut MsgReader<'a>) -> &'a str {
    let start = msg.position() as usize;
    let full: &'a [u8] = msg.get_ref();
    let Some(rel_nul) = full[start..].iter().position(|&b| b == 0) else {
        ereport!(ERROR, |e| {
            e.errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("invalid string in message");
        });
        unreachable!("ereport!(ERROR) raises");
    };
    let end = start + rel_nul;
    msg.set_position((end + 1) as u64); // skip the NUL
    std::str::from_utf8(&full[start..end]).unwrap_or("")
}

/// PG `pq_getmsgend`: verify the body was fully consumed.
pub fn pq_getmsgend(msg: &MsgReader) {
    if remaining(msg) != 0 {
        ereport!(ERROR, |e| {
            e.errcode(ERRCODE_PROTOCOL_VIOLATION)
                .errmsg("invalid message format");
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_a_typed_message_exact_bytes() {
        let mut m = PqMsg::default();
        pq_beginmessage(&mut m, b'T');
        pq_sendint16(&mut m, 1);
        pq_sendstring(&mut m, "id");
        pq_sendint32(&mut m, 0xDEAD_BEEF);
        assert_eq!(m.msgtype, b'T');
        let mut expect = Vec::new();
        expect.extend_from_slice(&1u16.to_be_bytes());
        expect.extend_from_slice(b"id\0");
        expect.extend_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
        assert_eq!(m.data, expect);
    }

    #[test]
    fn getmsgint_string_bytes_roundtrip() {
        // body: int16(7), "hi\0", int32(42), int64(-1)
        let mut body = Vec::new();
        body.extend_from_slice(&7u16.to_be_bytes());
        body.extend_from_slice(b"hi\0");
        body.extend_from_slice(&42u32.to_be_bytes());
        body.extend_from_slice(&(-1i64).to_be_bytes());

        let mut msg = MsgReader::new(&body[..]);
        assert_eq!(pq_getmsgint(&mut msg, 2), 7);
        assert_eq!(pq_getmsgstring(&mut msg), "hi");
        assert_eq!(pq_getmsgint(&mut msg, 4), 42);
        assert_eq!(pq_getmsgint64(&mut msg), -1);
        pq_getmsgend(&msg); // exact consume: no panic
    }

    #[test]
    fn getmsgbytes_borrows_and_advances() {
        let body = b"\x01\x02\x03\x04rest\0".to_vec();
        let mut msg = MsgReader::new(&body[..]);
        let four = pq_getmsgbytes(&mut msg, 4);
        assert_eq!(four, &[1, 2, 3, 4]);
        assert_eq!(pq_getmsgstring(&mut msg), "rest");
    }

    // ereport!(ERROR) raises via panic_any(ErrorData) (not a string payload), so
    // these assert the raise with catch_unwind, matching the repo convention.
    #[test]
    fn getmsgend_rejects_trailing_bytes() {
        let r = std::panic::catch_unwind(|| {
            let body = b"\x00\x01trailing".to_vec();
            let mut msg = MsgReader::new(&body[..]);
            let _ = pq_getmsgint(&mut msg, 2);
            pq_getmsgend(&msg); // 8 bytes left -> ERROR
        });
        assert!(r.is_err());
    }

    #[test]
    fn getmsgint_underflow_is_protocol_error() {
        let r = std::panic::catch_unwind(|| {
            let body = b"\x00".to_vec(); // only 1 byte, ask for 4
            let mut msg = MsgReader::new(&body[..]);
            pq_getmsgint(&mut msg, 4)
        });
        assert!(r.is_err());
    }

    #[test]
    fn float_roundtrip() {
        let mut m = PqMsg::default();
        pq_sendfloat4(&mut m, 1.5);
        pq_sendfloat8(&mut m, -2.25);
        let mut msg = MsgReader::new(&m.data[..]);
        // exact bit compare (values are exactly representable; avoids float_cmp)
        assert_eq!(pq_getmsgfloat4(&mut msg).to_bits(), 1.5f32.to_bits());
        assert_eq!(pq_getmsgfloat8(&mut msg).to_bits(), (-2.25f64).to_bits());
    }
}

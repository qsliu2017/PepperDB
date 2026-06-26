//! Translated from PostgreSQL src/include/libpq/pqformat.h
//
// Formatting and parsing of FE/BE messages. WIRE PROTOCOL: in-memory model, NOT
// repr(C). Writes go to an ordinary `Vec<u8>` (StringInfo is tombstoned); reads
// go through a byte cursor. All multi-byte integers are explicit big-endian
// (network byte order). The C `enlargeStringInfo`/preallocation dance is
// unnecessary - `Vec::extend_from_slice` grows as needed - so the `pq_write*`/
// `pq_send*` split collapses to a single set of append helpers.
#![allow(clippy::needless_pass_by_value, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use std::io::Cursor;

/// Read cursor over a received message body. Wraps the bytes plus a position;
/// `std::io::Cursor` gives the position tracking the C `StringInfo.cursor` did.
pub type MsgReader<'a> = Cursor<&'a [u8]>;

// --- message framing ---------------------------------------------------------

/// Begin a message of the given type (C pq_beginmessage).
pub fn pq_beginmessage(buf: &mut Vec<u8>, msgtype: u8) {
    let _ = (buf, msgtype);
    unimplemented!()
}

pub fn pq_beginmessage_reuse(buf: &mut Vec<u8>, msgtype: u8) {
    let _ = (buf, msgtype);
    unimplemented!()
}

pub fn pq_endmessage(buf: &mut Vec<u8>) {
    let _ = buf;
    unimplemented!()
}

pub fn pq_endmessage_reuse(buf: &mut Vec<u8>) {
    let _ = buf;
    unimplemented!()
}

// --- send (append) helpers ---------------------------------------------------

pub fn pq_sendbytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(data);
}

pub fn pq_sendcountedtext(buf: &mut Vec<u8>, s: &str, slen: i32) {
    let _ = (buf, s, slen);
    unimplemented!()
}

pub fn pq_sendtext(buf: &mut Vec<u8>, s: &str, slen: i32) {
    let _ = (buf, s, slen);
    unimplemented!()
}

/// Append a null-terminated string (with client-encoding conversion).
pub fn pq_sendstring(buf: &mut Vec<u8>, s: &str) {
    let _ = (buf, s);
    unimplemented!()
}

pub fn pq_send_ascii_string(buf: &mut Vec<u8>, s: &str) {
    let _ = (buf, s);
    unimplemented!()
}

pub fn pq_sendfloat4(buf: &mut Vec<u8>, f: f32) {
    pq_sendint32(buf, f.to_bits());
}

pub fn pq_sendfloat8(buf: &mut Vec<u8>, f: f64) {
    pq_sendint64(buf, f.to_bits());
}

// Binary integer appenders (network byte order). pq_writeintN/pq_sendintN of the
// C header collapse into these since Vec grows on demand.

pub fn pq_sendint8(buf: &mut Vec<u8>, i: u8) {
    buf.push(i);
}

pub fn pq_sendint16(buf: &mut Vec<u8>, i: u16) {
    buf.extend_from_slice(&i.to_be_bytes());
}

pub fn pq_sendint32(buf: &mut Vec<u8>, i: u32) {
    buf.extend_from_slice(&i.to_be_bytes());
}

pub fn pq_sendint64(buf: &mut Vec<u8>, i: u64) {
    buf.extend_from_slice(&i.to_be_bytes());
}

pub fn pq_sendbyte(buf: &mut Vec<u8>, byt: u8) {
    pq_sendint8(buf, byt);
}

/// Append a binary integer of `b` bytes. Deprecated in C; prefer the sized fns.
pub fn pq_sendint(buf: &mut Vec<u8>, i: u32, b: i32) {
    match b {
        1 => pq_sendint8(buf, i as u8),
        2 => pq_sendint16(buf, i as u16),
        4 => pq_sendint32(buf, i),
        _ => panic!("unsupported integer size {b}"),
    }
}

/// Begin building a type's binary output value.
pub fn pq_begintypsend(buf: &mut Vec<u8>) {
    let _ = buf;
    unimplemented!()
}

/// Finish a type's binary output value, returning the bytea payload.
pub fn pq_endtypsend(buf: Vec<u8>) -> Vec<u8> {
    let _ = buf;
    unimplemented!()
}

pub fn pq_puttextmessage(msgtype: u8, s: &str) {
    let _ = (msgtype, s);
    unimplemented!()
}

pub fn pq_putemptymessage(msgtype: u8) {
    let _ = msgtype;
    unimplemented!()
}

// --- get (parse) helpers over a read cursor ----------------------------------
// The C functions take a StringInfo whose `cursor` field advances; here the
// MsgReader cursor position advances instead. A short read is a protocol error;
// returned as Err / handled by the protocol layer.

pub fn pq_getmsgbyte(msg: &mut MsgReader) -> i32 {
    let _ = msg;
    unimplemented!()
}

/// Read an unsigned integer of `b` bytes (network byte order).
pub fn pq_getmsgint(msg: &mut MsgReader, b: i32) -> u32 {
    let _ = (msg, b);
    unimplemented!()
}

pub fn pq_getmsgint64(msg: &mut MsgReader) -> i64 {
    let _ = msg;
    unimplemented!()
}

pub fn pq_getmsgfloat4(msg: &mut MsgReader) -> f32 {
    f32::from_bits(pq_getmsgint(msg, 4))
}

pub fn pq_getmsgfloat8(msg: &mut MsgReader) -> f64 {
    f64::from_bits(pq_getmsgint64(msg) as u64)
}

/// Borrow `datalen` raw bytes from the cursor, advancing it.
pub fn pq_getmsgbytes<'a>(msg: &mut MsgReader<'a>, datalen: i32) -> &'a [u8] {
    let _ = (msg, datalen);
    unimplemented!()
}

/// Copy `datalen` bytes out of the cursor into `buf`.
pub fn pq_copymsgbytes(msg: &mut MsgReader, buf: &mut [u8]) {
    let _ = (msg, buf);
    unimplemented!()
}

/// Read `rawbytes`, convert from client encoding; returns the decoded text.
pub fn pq_getmsgtext(msg: &mut MsgReader, rawbytes: i32) -> String {
    let _ = (msg, rawbytes);
    unimplemented!()
}

/// Read a null-terminated string (with encoding conversion).
pub fn pq_getmsgstring<'a>(msg: &mut MsgReader<'a>) -> &'a str {
    let _ = msg;
    unimplemented!()
}

/// Read a null-terminated string without encoding conversion.
pub fn pq_getmsgrawstring<'a>(msg: &mut MsgReader<'a>) -> &'a str {
    let _ = msg;
    unimplemented!()
}

/// Verify the cursor is at end of message.
pub fn pq_getmsgend(msg: &MsgReader) {
    let _ = msg;
    unimplemented!()
}

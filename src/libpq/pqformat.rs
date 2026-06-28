//! Translated from PostgreSQL src/include/libpq/pqformat.h
//
// Formatting and parsing of FE/BE messages. WIRE PROTOCOL: in-memory model, NOT
// repr(C). The definitions live in `crate::backend::libpq::pqformat` (the .c
// body); this header re-exports them so `use crate::libpq::pqformat::<name>`
// keeps resolving.
//
// Builders append into a `PqMsg` (the StringInfo replacement carrying the
// message-type byte); the `pq_getmsg*` readers operate on a `MsgReader` cursor
// over an already-received body. The `pq_send*` builders and `pq_getmsg*`
// readers are synchronous; `pq_endmessage`/`pq_puttextmessage`/
// `pq_putemptymessage` are async (they reach the socket via pqcomm).
//
// The `pq_beginmessage`/`pq_send*`/`pq_end*` builders are now `#[deprecated]`
// shims delegating to inherent `PqMsg` methods (rules.md s3); re-exporting them
// here so `use crate::libpq::pqformat::pq_*` keeps resolving is deliberate, so
// the header allows the deprecation.
#![allow(deprecated)]

pub use crate::backend::libpq::pqformat::{
    pq_begintypsend, pq_beginmessage, pq_beginmessage_reuse, pq_copymsgbytes, pq_endmessage,
    pq_endmessage_reuse, pq_endtypsend, pq_getmsgbyte, pq_getmsgbytes, pq_getmsgend,
    pq_getmsgfloat4, pq_getmsgfloat8, pq_getmsgint, pq_getmsgint64, pq_getmsgrawstring,
    pq_getmsgstring, pq_getmsgtext, pq_putemptymessage, pq_puttextmessage, pq_send_ascii_string,
    pq_sendbyte, pq_sendbytes, pq_sendcountedtext, pq_sendfloat4, pq_sendfloat8, pq_sendint,
    pq_sendint16, pq_sendint32, pq_sendint64, pq_sendint8, pq_sendstring, pq_sendtext, MsgReader,
    PqMsg,
};

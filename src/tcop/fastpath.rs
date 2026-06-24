//! Translated from PostgreSQL src/include/tcop/fastpath.h
//
// StringInfo (lib/stringinfo.h) is tombstoned -> the wire message buffer is a
// byte slice here.

pub fn HandleFunctionRequest(msg_buf: &[u8]) {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/common/base64.h
//! Base64 without whitespace support.

/// Encode `src` into `dst`; Ok holds the number of bytes written.
pub fn pg_b64_encode(src: &[u8], dst: &mut [u8]) -> Result<usize, ()> {
    let _ = (src, dst);
    unimplemented!()
}

/// Decode `src` into `dst`; Ok holds the number of bytes written.
pub fn pg_b64_decode(src: &[u8], dst: &mut [u8]) -> Result<usize, ()> {
    let _ = (src, dst);
    unimplemented!()
}

/// Maximum encoded length for `srclen` input bytes.
pub fn pg_b64_enc_len(srclen: i32) -> i32 {
    let _ = srclen;
    unimplemented!()
}

/// Maximum decoded length for `srclen` input bytes.
pub fn pg_b64_dec_len(srclen: i32) -> i32 {
    let _ = srclen;
    unimplemented!()
}

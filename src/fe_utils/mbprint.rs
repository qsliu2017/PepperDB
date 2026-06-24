//! Translated from PostgreSQL src/include/fe_utils/mbprint.h
//
// Multibyte character printing support for frontend code.

/// A formatted display line: its bytes plus computed display width.
pub struct LinePtr {
    pub ptr: Vec<u8>,
    pub width: i32,
}

/// C: `unsigned char *mbvalidate(unsigned char *pwcs, int encoding)`. Returns
/// the (in-place repaired) string; modeled as taking and returning the buffer.
pub fn mbvalidate(_pwcs: &mut [u8], _encoding: i32) -> &mut [u8] {
    unimplemented!()
}

pub fn pg_wcswidth(_pwcs: &[u8], _encoding: i32) -> i32 {
    unimplemented!()
}

/// C fills a caller-provided `lines` array; here it returns the formatted lines.
pub fn pg_wcsformat(_pwcs: &[u8], _encoding: i32, _count: i32) -> Vec<LinePtr> {
    unimplemented!()
}

/// Display metrics of a multibyte string. C uses three int out-params.
pub struct WcsSize {
    pub width: i32,
    pub height: i32,
    pub format_size: i32,
}

pub fn pg_wcssize(_pwcs: &[u8], _encoding: i32) -> WcsSize {
    unimplemented!()
}

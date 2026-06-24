//! Translated from PostgreSQL src/include/common/unicode_case.h
//! Routines for converting character case (API over unicode_case_table).

use crate::mb::pg_wchar::pg_wchar;

// WordBoundaryNext: a callback advancing over word boundaries. The C `void *wbstate`
// opaque context maps to a captured closure, taken as `impl FnMut` (generic).

pub fn unicode_lowercase_simple(code: pg_wchar) -> pg_wchar {
    let _ = code;
    unimplemented!()
}

pub fn unicode_titlecase_simple(code: pg_wchar) -> pg_wchar {
    let _ = code;
    unimplemented!()
}

pub fn unicode_uppercase_simple(code: pg_wchar) -> pg_wchar {
    let _ = code;
    unimplemented!()
}

pub fn unicode_casefold_simple(code: pg_wchar) -> pg_wchar {
    let _ = code;
    unimplemented!()
}

pub fn unicode_strlower(dst: &mut [u8], src: &[u8], srclen: isize, full: bool) -> usize {
    let _ = (dst, src, srclen, full);
    unimplemented!()
}

pub fn unicode_strtitle(
    dst: &mut [u8],
    src: &[u8],
    srclen: isize,
    full: bool,
    wbnext: impl FnMut() -> usize,
) -> usize {
    let _ = (dst, src, srclen, full, wbnext);
    unimplemented!()
}

pub fn unicode_strupper(dst: &mut [u8], src: &[u8], srclen: isize, full: bool) -> usize {
    let _ = (dst, src, srclen, full);
    unimplemented!()
}

pub fn unicode_strfold(dst: &mut [u8], src: &[u8], srclen: isize, full: bool) -> usize {
    let _ = (dst, src, srclen, full);
    unimplemented!()
}

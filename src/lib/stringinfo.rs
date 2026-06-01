//! Translation of postgres/src/include/lib/stringinfo.h
//!                + postgres/src/common/stringinfo.c
//!
//! StringInfo is an extensible string buffer (capacity limited to ~1GB). It can
//! hold either NUL-terminated C strings or arbitrary binary data; storage is
//! allocated with `palloc`/`repalloc`.
//!
//! Note on the printf-style API: C's `appendStringInfo(str, fmt, ...)` /
//! `appendStringInfoVA` rely on C varargs, which Rust cannot express directly.
//! They are replaced by the [`appendStringInfo!`] macro, which uses Rust's own
//! `format!` and forwards the result to [`appendBinaryStringInfo`]. Translated
//! call sites convert printf `%` placeholders into Rust `{}` placeholders.

use crate::prelude::*;
use core::ffi::{c_char, c_int, c_void};

/// StringInfoData holds an extensible string.
///
/// * `data`   - the current buffer.
/// * `len`    - current string length; for writable strings `data[len] == '\0'`.
/// * `maxlen` - allocated size of `data`. Zero marks a read-only StringInfo.
/// * `cursor` - a scratch scan position, untouched by the routines here.
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}

pub type StringInfo = *mut StringInfoData;

/// Default initial allocation size.
pub const STRINGINFO_DEFAULT_SIZE: c_int = 1024;

/// `initStringInfoInternal`: initialize `str` to an empty string with the given
/// initial buffer size. (static inline in stringinfo.c)
///
/// # Safety
/// `str` must point to writable StringInfoData storage.
#[inline]
unsafe fn initStringInfoInternal(str: StringInfo, initsize: c_int) {
    Assert!(initsize >= 1 && initsize as Size <= MaxAllocSize);

    (*str).data = palloc(initsize as Size) as *mut c_char;
    (*str).maxlen = initsize;
    resetStringInfo(str);
}

/// `makeStringInfoInternal`: allocate and initialize a StringInfoData.
///
/// # Safety
/// Returns a palloc'd pointer the caller must release.
#[inline]
unsafe fn makeStringInfoInternal(initsize: c_int) -> StringInfo {
    let res = palloc(core::mem::size_of::<StringInfoData>()) as StringInfo;
    initStringInfoInternal(res, initsize);
    res
}

/// `makeStringInfo`: create an empty StringInfoData and return a pointer to it.
///
/// # Safety
/// Returns a palloc'd pointer the caller must release with [`destroyStringInfo`].
pub unsafe fn makeStringInfo() -> StringInfo {
    makeStringInfoInternal(STRINGINFO_DEFAULT_SIZE)
}

/// `makeStringInfoExt`: like [`makeStringInfo`] but with a chosen initial size.
///
/// # Safety
/// See [`makeStringInfo`].
pub unsafe fn makeStringInfoExt(initsize: c_int) -> StringInfo {
    makeStringInfoInternal(initsize)
}

/// `initStringInfo`: initialize a (locally-owned) StringInfoData to empty.
///
/// # Safety
/// `str` must point to writable StringInfoData storage.
pub unsafe fn initStringInfo(str: StringInfo) {
    initStringInfoInternal(str, STRINGINFO_DEFAULT_SIZE);
}

/// `initStringInfoExt`: like [`initStringInfo`] with a chosen initial size.
///
/// # Safety
/// See [`initStringInfo`].
pub unsafe fn initStringInfoExt(str: StringInfo, initsize: c_int) {
    initStringInfoInternal(str, initsize);
}

/// `initReadOnlyStringInfo`: point `str` at an existing buffer without copying.
/// The buffer storage is the caller's responsibility; `maxlen` is set to 0 so the
/// result cannot be appended to or reset. (inline in stringinfo.h)
///
/// # Safety
/// `data` must remain valid for at least `len` bytes for the StringInfo's lifetime.
pub unsafe fn initReadOnlyStringInfo(str: StringInfo, data: *mut c_char, len: c_int) {
    (*str).data = data;
    (*str).len = len;
    (*str).maxlen = 0; // read-only
    (*str).cursor = 0;
}

/// `initStringInfoFromString`: point `str` at an existing palloc'd, NUL-terminated
/// buffer (which may later be repalloc'd by the append functions). (inline)
///
/// # Safety
/// `data` must be a palloc'd chunk of at least `len + 1` bytes, NUL at `data[len]`.
pub unsafe fn initStringInfoFromString(str: StringInfo, data: *mut c_char, len: c_int) {
    Assert!(*data.add(len as usize) == 0);

    (*str).data = data;
    (*str).len = len;
    (*str).maxlen = len + 1;
    (*str).cursor = 0;
}

/// `resetStringInfo`: clear contents while keeping the buffer valid. Not allowed
/// on read-only StringInfos.
///
/// # Safety
/// `str` must be a writable StringInfo.
pub unsafe fn resetStringInfo(str: StringInfo) {
    // don't allow resets of read-only StringInfos
    Assert!((*str).maxlen != 0);

    *(*str).data = 0; // data[0] = '\0'
    (*str).len = 0;
    (*str).cursor = 0;
}

/// `appendStringInfoString`: append a NUL-terminated C string.
///
/// # Safety
/// `s` must be a valid NUL-terminated C string; `str` must be writable.
pub unsafe fn appendStringInfoString(str: StringInfo, s: *const c_char) {
    appendBinaryStringInfo(str, s as *const c_void, strlen(s) as c_int);
}

/// `appendStringInfoChar`: append a single byte (and keep the trailing NUL).
///
/// # Safety
/// `str` must be writable.
pub unsafe fn appendStringInfoChar(str: StringInfo, ch: c_char) {
    // Make more room if needed
    if (*str).len + 1 >= (*str).maxlen {
        enlargeStringInfo(str, 1);
    }

    // OK, append the character
    *(*str).data.add((*str).len as usize) = ch;
    (*str).len += 1;
    *(*str).data.add((*str).len as usize) = 0;
}

/// `appendStringInfoSpaces`: append `count` spaces.
///
/// # Safety
/// `str` must be writable.
pub unsafe fn appendStringInfoSpaces(str: StringInfo, count: c_int) {
    if count > 0 {
        // Make more room if needed
        enlargeStringInfo(str, count);

        // OK, append the spaces
        core::ptr::write_bytes(
            (*str).data.add((*str).len as usize) as *mut u8,
            b' ',
            count as usize,
        );
        (*str).len += count;
        *(*str).data.add((*str).len as usize) = 0;
    }
}

/// `appendBinaryStringInfo`: append arbitrary binary data, keeping a trailing NUL.
///
/// # Safety
/// `data` must be valid for `datalen` bytes; `str` must be writable.
pub unsafe fn appendBinaryStringInfo(str: StringInfo, data: *const c_void, datalen: c_int) {
    Assert!(!str.is_null());

    // Make more room if needed
    enlargeStringInfo(str, datalen);

    // OK, append the data
    core::ptr::copy_nonoverlapping(
        data as *const u8,
        (*str).data.add((*str).len as usize) as *mut u8,
        datalen as usize,
    );
    (*str).len += datalen;

    // Keep a trailing null in place.
    *(*str).data.add((*str).len as usize) = 0;
}

/// `appendBinaryStringInfoNT`: append binary data without ensuring a trailing NUL.
///
/// # Safety
/// See [`appendBinaryStringInfo`].
pub unsafe fn appendBinaryStringInfoNT(str: StringInfo, data: *const c_void, datalen: c_int) {
    Assert!(!str.is_null());

    enlargeStringInfo(str, datalen);

    core::ptr::copy_nonoverlapping(
        data as *const u8,
        (*str).data.add((*str).len as usize) as *mut u8,
        datalen as usize,
    );
    (*str).len += datalen;
}

/// `enlargeStringInfo`: ensure room for `needed` more bytes (excluding the NUL).
/// Grows by doubling. Errors on absurd requests or exceeding MaxAllocSize.
///
/// # Safety
/// `str` must be a writable StringInfo.
pub unsafe fn enlargeStringInfo(str: StringInfo, mut needed: c_int) {
    // validate this is not a read-only StringInfo
    Assert!((*str).maxlen != 0);

    // Guard against out-of-range "needed" values.
    if needed < 0 {
        // should not happen
        elog!(ERROR, "invalid string enlargement request size: {}", needed);
    }
    if (needed as Size) >= (MaxAllocSize - (*str).len as Size) {
        ereport!(
            ERROR,
            errmsg!(
                "string buffer exceeds maximum allowed length ({} bytes); cannot enlarge string buffer containing {} bytes by {} more bytes",
                MaxAllocSize,
                (*str).len,
                needed
            )
        );
    }

    needed += (*str).len + 1; // total space required now

    // Because of the above test, we now have needed <= MaxAllocSize

    if needed <= (*str).maxlen {
        return; // got enough space already
    }

    // Double the buffer size each time it overflows; more if 'needed' is big.
    let mut newlen = 2 * (*str).maxlen;
    while needed > newlen {
        newlen = 2 * newlen;
    }

    // Clamp to MaxAllocSize. (We still have newlen >= needed.)
    if newlen > MaxAllocSize as c_int {
        newlen = MaxAllocSize as c_int;
    }

    (*str).data = repalloc((*str).data as *mut c_void, newlen as Size) as *mut c_char;

    (*str).maxlen = newlen;
}

/// `destroyStringInfo`: free a StringInfo and its buffer (opposite of
/// [`makeStringInfo`]). Only valid for palloc'd, writable StringInfos.
///
/// # Safety
/// `str` must have been produced by [`makeStringInfo`]/[`makeStringInfoExt`].
pub unsafe fn destroyStringInfo(str: StringInfo) {
    // don't allow destroys of read-only StringInfos
    Assert!((*str).maxlen != 0);

    pfree((*str).data as *mut c_void);
    pfree(str as *mut c_void);
}

/// `appendStringInfoCharMacro(str, ch)`: fast-path single-byte append (the C macro
/// form of [`appendStringInfoChar`]). `str` is evaluated multiple times.
#[macro_export]
macro_rules! appendStringInfoCharMacro {
    ($str:expr, $ch:expr) => {{
        let s = $str;
        if (*s).len + 1 >= (*s).maxlen {
            $crate::lib::stringinfo::appendStringInfoChar(s, $ch);
        } else {
            *(*s).data.add((*s).len as usize) = $ch;
            (*s).len += 1;
            *(*s).data.add((*s).len as usize) = 0;
        }
    }};
}

/// `appendStringInfo(str, fmt, ...)`: format and append, the printf-style append.
/// Replaces the C varargs API with Rust `format!`; placeholders use `{}` form.
///
/// Usage: `appendStringInfo!(buf, "row {} of {}", i, n)`.
#[macro_export]
macro_rules! appendStringInfo {
    ($str:expr, $($arg:tt)*) => {{
        let __s = format!($($arg)*);
        $crate::lib::stringinfo::appendBinaryStringInfo(
            $str,
            __s.as_ptr() as *const core::ffi::c_void,
            __s.len() as core::ffi::c_int,
        );
    }};
}

/// Minimal `strlen` over a C string (mirrors libc strlen for the port).
///
/// # Safety
/// `s` must point to a valid NUL-terminated C string.
#[inline]
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Read the C-string contents of a StringInfo back into a Rust String.
    unsafe fn as_str(s: StringInfo) -> String {
        let bytes = core::slice::from_raw_parts((*s).data as *const u8, (*s).len as usize);
        String::from_utf8_lossy(bytes).into_owned()
    }

    #[test]
    fn append_and_grow() {
        unsafe {
            let s = makeStringInfo();
            assert_eq!((*s).len, 0);
            assert_eq!((*s).maxlen, STRINGINFO_DEFAULT_SIZE);

            appendStringInfoString(s, c"hello".as_ptr());
            appendStringInfoChar(s, b' ' as c_char);
            appendStringInfo!(s, "world {}", 42);
            assert_eq!(as_str(s), "hello world 42");

            // force several growth doublings past the default 1024 bytes
            for _ in 0..5000 {
                appendStringInfoChar(s, b'x' as c_char);
            }
            assert_eq!((*s).len as usize, "hello world 42".len() + 5000);
            assert!((*s).maxlen > STRINGINFO_DEFAULT_SIZE);
            // trailing NUL invariant
            assert_eq!(*(*s).data.add((*s).len as usize), 0);

            resetStringInfo(s);
            assert_eq!((*s).len, 0);
            assert_eq!(as_str(s), "");

            destroyStringInfo(s);
        }
    }

    #[test]
    fn append_spaces_and_binary() {
        unsafe {
            let mut sd = StringInfoData {
                data: core::ptr::null_mut(),
                len: 0,
                maxlen: 0,
                cursor: 0,
            };
            initStringInfo(&mut sd);
            appendStringInfoSpaces(&mut sd, 3);
            appendBinaryStringInfo(&mut sd, b"ab\0cd".as_ptr() as *const c_void, 5);
            assert_eq!((&sd as *const _ as StringInfo).as_ref().unwrap().len, 8);
            pfree(sd.data as *mut c_void);
        }
    }
}

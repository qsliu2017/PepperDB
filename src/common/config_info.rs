//! config_info.rs
//!     Common code for pg_config output
//!
//! 1:1 translation of postgres/src/common/config_info.c (+ ConfigData from
//! common/config_info.h).
//!
//! NOTE on placeholders: the original C pulls the build-time configuration in
//! from #defines emitted by the configure/meson build (CONFIGURE_ARGS, VAL_CC,
//! VAL_CPPFLAGS, VAL_CFLAGS, VAL_CFLAGS_SL, VAL_LDFLAGS, VAL_LDFLAGS_EX,
//! VAL_LDFLAGS_SL, VAL_LIBS, PG_VERSION). PepperDB has no such generated build
//! header yet, so those are represented here as placeholder consts (see below).
//! When a real build-config header lands, swap these consts for the generated
//! values. Every C `#ifdef VAL_x / #else "not recorded"` branch is preserved by
//! giving each VAL_* placeholder a concrete (non-`None`) value -- i.e. the
//! "recorded" path is taken; the `_("not recorded")` fallback is documented but
//! not exercised since the placeholders are always present.
//!
//! The path-relativization (get_*_path / cleanup_path) is NOT stubbed here: it
//! is provided for real by crate::port::path (a 1:1 port of src/port/path.c).

use crate::prelude::*;
use crate::port::path::{
    cleanup_path, get_doc_path, get_etc_path, get_html_path, get_include_path,
    get_includeserver_path, get_lib_path, get_locale_path, get_man_path, get_pkginclude_path,
    get_pkglib_path, get_share_path,
};
use crate::port::strlcpy::strlcpy;

// MAXPGPATH from pg_config_manual.h (#define MAXPGPATH 1024).
const MAXPGPATH: usize = 1024;

extern "C" {
    fn strrchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strlcat(dst: *mut c_char, src: *const c_char, siz: usize) -> usize;
    fn strlen(s: *const c_char) -> usize;
}

// --- Build-time configuration placeholders (see module note). ---
//
// In C these come from "pg_config_paths.h" / generated build defines. They are
// compile-time C string literals; here they are byte-string consts (NUL
// terminated) and are PLACEHOLDERS until a generated build header exists.
const CONFIGURE_ARGS: &[u8] = b"placeholder: configure args not recorded\0";
const VAL_CC: &[u8] = b"placeholder-cc\0";
const VAL_CPPFLAGS: &[u8] = b"placeholder-cppflags\0";
const VAL_CFLAGS: &[u8] = b"placeholder-cflags\0";
const VAL_CFLAGS_SL: &[u8] = b"placeholder-cflags-sl\0";
const VAL_LDFLAGS: &[u8] = b"placeholder-ldflags\0";
const VAL_LDFLAGS_EX: &[u8] = b"placeholder-ldflags-ex\0";
const VAL_LDFLAGS_SL: &[u8] = b"placeholder-ldflags-sl\0";
const VAL_LIBS: &[u8] = b"placeholder-libs\0";
// PG_VERSION from pg_config.h: PostgreSQL 18.3 (placeholder for the generated
// "PostgreSQL " PG_VERSION literal).
const VERSION_STR: &[u8] = b"PostgreSQL 18.3\0";

/// typedef struct ConfigData { char *name; char *setting; } ConfigData;
#[repr(C)]
pub struct ConfigData {
    pub name: *mut c_char,
    pub setting: *mut c_char,
}

// pstrdup a &[u8] byte-string literal (must be NUL-terminated).
#[inline]
unsafe fn pstrdup_lit(lit: &[u8]) -> *mut c_char {
    pstrdup(lit.as_ptr() as *const c_char)
}

/// get_configdata(const char *my_exec_path, size_t *configdata_len)
///
/// Get configure-time constants. The caller is responsible for pfreeing the
/// result.
///
/// # Safety
/// `my_exec_path` must be a valid NUL-terminated C string; `configdata_len`
/// must be a valid writable pointer.
pub unsafe fn get_configdata(
    my_exec_path: *const c_char,
    configdata_len: *mut Size,
) -> *mut ConfigData {
    let mut path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut i: usize = 0;

    // Adjust this to match the number of items filled below.
    *configdata_len = 23;
    let configdata =
        palloc((*configdata_len) * core::mem::size_of::<ConfigData>()) as *mut ConfigData;

    let pp = path.as_mut_ptr();

    macro_rules! entry {
        ($name:expr, $setting:expr) => {{
            (*configdata.add(i)).name = pstrdup_lit($name);
            (*configdata.add(i)).setting = $setting;
            i += 1;
        }};
    }

    // BINDIR: directory part of my_exec_path.
    (*configdata.add(i)).name = pstrdup_lit(b"BINDIR\0");
    strlcpy(pp, my_exec_path, MAXPGPATH);
    let lastsep = strrchr(pp, b'/' as c_int);
    if !lastsep.is_null() {
        *lastsep = 0;
    }
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // DOCDIR
    (*configdata.add(i)).name = pstrdup_lit(b"DOCDIR\0");
    get_doc_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // HTMLDIR
    (*configdata.add(i)).name = pstrdup_lit(b"HTMLDIR\0");
    get_html_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // INCLUDEDIR
    (*configdata.add(i)).name = pstrdup_lit(b"INCLUDEDIR\0");
    get_include_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // PKGINCLUDEDIR
    (*configdata.add(i)).name = pstrdup_lit(b"PKGINCLUDEDIR\0");
    get_pkginclude_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // INCLUDEDIR-SERVER
    (*configdata.add(i)).name = pstrdup_lit(b"INCLUDEDIR-SERVER\0");
    get_includeserver_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // LIBDIR
    (*configdata.add(i)).name = pstrdup_lit(b"LIBDIR\0");
    get_lib_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // PKGLIBDIR
    (*configdata.add(i)).name = pstrdup_lit(b"PKGLIBDIR\0");
    get_pkglib_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // LOCALEDIR
    (*configdata.add(i)).name = pstrdup_lit(b"LOCALEDIR\0");
    get_locale_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // MANDIR
    (*configdata.add(i)).name = pstrdup_lit(b"MANDIR\0");
    get_man_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // SHAREDIR
    (*configdata.add(i)).name = pstrdup_lit(b"SHAREDIR\0");
    get_share_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // SYSCONFDIR
    (*configdata.add(i)).name = pstrdup_lit(b"SYSCONFDIR\0");
    get_etc_path(my_exec_path, pp);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // PGXS: pkglib path + "/pgxs/src/makefiles/pgxs.mk".
    (*configdata.add(i)).name = pstrdup_lit(b"PGXS\0");
    get_pkglib_path(my_exec_path, pp);
    strlcat(pp, b"/pgxs/src/makefiles/pgxs.mk\0".as_ptr() as *const c_char, MAXPGPATH);
    cleanup_path(pp);
    (*configdata.add(i)).setting = pstrdup(pp);
    i += 1;

    // CONFIGURE
    entry!(b"CONFIGURE\0", pstrdup_lit(CONFIGURE_ARGS));

    // CC (C: #ifdef VAL_CC ... #else "not recorded"; placeholder is present).
    entry!(b"CC\0", pstrdup_lit(VAL_CC));

    // CPPFLAGS
    entry!(b"CPPFLAGS\0", pstrdup_lit(VAL_CPPFLAGS));

    // CFLAGS
    entry!(b"CFLAGS\0", pstrdup_lit(VAL_CFLAGS));

    // CFLAGS_SL
    entry!(b"CFLAGS_SL\0", pstrdup_lit(VAL_CFLAGS_SL));

    // LDFLAGS
    entry!(b"LDFLAGS\0", pstrdup_lit(VAL_LDFLAGS));

    // LDFLAGS_EX
    entry!(b"LDFLAGS_EX\0", pstrdup_lit(VAL_LDFLAGS_EX));

    // LDFLAGS_SL
    entry!(b"LDFLAGS_SL\0", pstrdup_lit(VAL_LDFLAGS_SL));

    // LIBS
    entry!(b"LIBS\0", pstrdup_lit(VAL_LIBS));

    // VERSION: "PostgreSQL " PG_VERSION.
    entry!(b"VERSION\0", pstrdup_lit(VERSION_STR));

    Assert!(i == *configdata_len);
    let _ = strlen; // referenced for completeness with the C string ABI.

    configdata
}

#[cfg(test)]
mod tests {
    use super::*;

    // strcmp helper for comparing a returned C string against a literal.
    unsafe fn cstr_eq(p: *const c_char, lit: &[u8]) -> bool {
        if p.is_null() {
            return false;
        }
        let len = strlen(p);
        if len != lit.len() {
            return false;
        }
        for k in 0..len {
            if *p.add(k) as u8 != lit[k] {
                return false;
            }
        }
        true
    }

    #[test]
    fn returns_expected_count_and_first_entry() {
        unsafe {
            let exec = b"/usr/local/pgsql/bin/postgres\0";
            let mut len: Size = 0;
            let data = get_configdata(exec.as_ptr() as *const c_char, &mut len);

            // The C source fills exactly 23 entries.
            assert_eq!(len, 23);
            assert!(!data.is_null());

            // First entry name is "BINDIR".
            assert!(cstr_eq((*data).name, b"BINDIR"));
            // BINDIR setting is the directory part of my_exec_path.
            assert!(cstr_eq((*data).setting, b"/usr/local/pgsql/bin"));

            // Last entry is VERSION with the placeholder version string.
            let last = data.add((len - 1) as usize);
            assert!(cstr_eq((*last).name, b"VERSION"));
            assert!(cstr_eq((*last).setting, b"PostgreSQL 18.3"));

            // Spot-check a middle name to confirm ordering (index 13 == CONFIGURE).
            assert!(cstr_eq((*data.add(13)).name, b"CONFIGURE"));

            // Free per-entry strings then the array.
            for k in 0..(len as usize) {
                pfree((*data.add(k)).name as *mut c_void);
                pfree((*data.add(k)).setting as *mut c_void);
            }
            pfree(data as *mut c_void);
        }
    }
}

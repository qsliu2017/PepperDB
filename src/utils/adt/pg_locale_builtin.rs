//! pg_locale_builtin.c - PostgreSQL locale utilities for builtin provider.

use crate::prelude::*;

use crate::common::unicode_case::{
    unicode_strfold, unicode_strlower, unicode_strtitle, unicode_strupper,
};
use crate::common::unicode_category::pg_u_isalnum;
use crate::mb::pg_wchar::{pg_wchar, unicode_utf8len, utf8_to_unicode, GetDatabaseEncoding};
use crate::miscadmin::MyDatabaseId;
use crate::utils::builtins::TextDatumGetCString;
use crate::utils::mmgr::mcxt::MemoryContextStrdup;

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

// ---------------------------------------------------------------------------
// Unported dependencies (local stubs). pg_locale.h types and syscache helpers
// are not yet translated.
// ---------------------------------------------------------------------------

// TODO(pg-port): pg_locale.h not ported - struct pg_locale_struct / pg_locale_t.
#[repr(C)]
pub struct pg_locale_struct {
    pub deterministic: bool,
    pub collate_is_c: bool,
    pub ctype_is_c: bool,
    pub provider: c_char,
    pub info: pg_locale_info,
}

#[repr(C)]
pub union pg_locale_info {
    pub builtin: pg_locale_builtin_info,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct pg_locale_builtin_info {
    pub locale: *const c_char,
    pub casemap_full: bool,
}

pub type pg_locale_t = *mut pg_locale_struct;

// TODO(pg-port): catalog/pg_collation.h - COLLPROVIDER_BUILTIN.
const COLLPROVIDER_BUILTIN: c_char = b'b' as c_char;

// TODO(pg-port): catalog/pg_collation_d.h - DEFAULT_COLLATION_OID.
const DEFAULT_COLLATION_OID: Oid = 100;

// TODO(pg-port): utils/syscache.h - syscache ids.
const DATABASEOID: c_int = 0;
const COLLOID: c_int = 0;

// TODO(pg-port): catalog/pg_database.h - attribute number.
const Anum_pg_database_datlocale: c_int = 0;
// TODO(pg-port): catalog/pg_collation.h - attribute number.
const Anum_pg_collation_colllocale: c_int = 0;

// TODO(pg-port): utils/syscache.h not ported.
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!()
}

// TODO(pg-port): utils/syscache.h not ported.
unsafe fn SysCacheGetAttrNotNull(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int) -> Datum {
    unimplemented!()
}

// TODO(pg-port): utils/syscache.h not ported.
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!()
}

// TODO(pg-port): pg_locale_builtin's builtin_validate_locale (in pg_locale.c) not ported.
unsafe fn builtin_validate_locale(_encoding: c_int, _locale: *const c_char) {
    unimplemented!()
}

/// `HeapTuple` (access/htup.h): opaque heap tuple pointer.
// TODO(pg-port): use real HeapTuple once access/htup.h is wired into prelude.
pub type HeapTuple = *mut c_void;

unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}

// ---------------------------------------------------------------------------
// WordBoundaryState and initcap word boundary iterator.
// ---------------------------------------------------------------------------

#[repr(C)]
struct WordBoundaryState {
    str: *const c_char,
    len: usize,
    offset: usize,
    posix: bool,
    init: bool,
    prev_alnum: bool,
}

/*
 * Simple word boundary iterator that draws boundaries each time the result of
 * pg_u_isalnum() changes.
 */
fn initcap_wbnext(state: *mut c_void) -> usize {
    unsafe {
        let wbstate = state as *mut WordBoundaryState;

        while (*wbstate).offset < (*wbstate).len
            && *(*wbstate).str.add((*wbstate).offset) != b'\0' as c_char
        {
            let u: pg_wchar =
                utf8_to_unicode(((*wbstate).str as *const u8).add((*wbstate).offset));
            let curr_alnum: bool = pg_u_isalnum(u, (*wbstate).posix);

            if !(*wbstate).init || curr_alnum != (*wbstate).prev_alnum {
                let prev_offset: usize = (*wbstate).offset;

                (*wbstate).init = true;
                (*wbstate).offset += unicode_utf8len(u) as usize;
                (*wbstate).prev_alnum = curr_alnum;
                return prev_offset;
            }

            (*wbstate).offset += unicode_utf8len(u) as usize;
        }

        (*wbstate).len
    }
}

pub unsafe fn strlower_builtin(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: isize,
    locale: pg_locale_t,
) -> usize {
    unicode_strlower(dest, destsize, src, srclen, (*locale).info.builtin.casemap_full)
}

pub unsafe fn strtitle_builtin(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: isize,
    locale: pg_locale_t,
) -> usize {
    let mut wbstate = WordBoundaryState {
        str: src,
        len: srclen as usize,
        offset: 0,
        posix: !(*locale).info.builtin.casemap_full,
        init: false,
        prev_alnum: false,
    };

    unicode_strtitle(
        dest,
        destsize,
        src,
        srclen,
        (*locale).info.builtin.casemap_full,
        initcap_wbnext,
        &mut wbstate as *mut WordBoundaryState as *mut c_void,
    )
}

pub unsafe fn strupper_builtin(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: isize,
    locale: pg_locale_t,
) -> usize {
    unicode_strupper(dest, destsize, src, srclen, (*locale).info.builtin.casemap_full)
}

pub unsafe fn strfold_builtin(
    dest: *mut c_char,
    destsize: usize,
    src: *const c_char,
    srclen: isize,
    locale: pg_locale_t,
) -> usize {
    unicode_strfold(dest, destsize, src, srclen, (*locale).info.builtin.casemap_full)
}

pub unsafe fn create_pg_locale_builtin(collid: Oid, context: MemoryContext) -> pg_locale_t {
    let locstr: *const c_char;
    let result: pg_locale_t;

    if collid == DEFAULT_COLLATION_OID {
        let tp: HeapTuple;
        let datum: Datum;

        tp = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
        if !HeapTupleIsValid(tp) {
            elog!(ERROR, "cache lookup failed for database");
        }
        datum = SysCacheGetAttrNotNull(DATABASEOID, tp, Anum_pg_database_datlocale);
        locstr = TextDatumGetCString(datum);
        ReleaseSysCache(tp);
    } else {
        let tp: HeapTuple;
        let datum: Datum;

        tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
        if !HeapTupleIsValid(tp) {
            elog!(ERROR, "cache lookup failed for collation");
        }
        datum = SysCacheGetAttrNotNull(COLLOID, tp, Anum_pg_collation_colllocale);
        locstr = TextDatumGetCString(datum);
        ReleaseSysCache(tp);
    }

    builtin_validate_locale(GetDatabaseEncoding(), locstr);

    result = MemoryContextAllocZero(context, core::mem::size_of::<pg_locale_struct>())
        as pg_locale_t;

    (*result).info.builtin.locale = MemoryContextStrdup(context as *mut _, locstr);
    (*result).info.builtin.casemap_full =
        strcmp(locstr, c"PG_UNICODE_FAST".as_ptr()) == 0;
    (*result).provider = COLLPROVIDER_BUILTIN;
    (*result).deterministic = true;
    (*result).collate_is_c = true;
    (*result).ctype_is_c = strcmp(locstr, c"C".as_ptr()) == 0;

    result
}

pub unsafe fn get_collation_actual_version_builtin(collcollate: *const c_char) -> *mut c_char {
    /*
     * The only two supported locales (C and C.UTF-8) are both based on memcmp
     * and are not expected to change, but track the version anyway.
     *
     * Note that the character semantics may change for some locales, but the
     * collation version only tracks changes to sort order.
     */
    if strcmp(collcollate, c"C".as_ptr()) == 0 {
        return c"1".as_ptr() as *mut c_char;
    } else if strcmp(collcollate, c"C.UTF-8".as_ptr()) == 0 {
        return c"1".as_ptr() as *mut c_char;
    } else if strcmp(collcollate, c"PG_UNICODE_FAST".as_ptr()) == 0 {
        return c"1".as_ptr() as *mut c_char;
    } else {
        ereport!(ERROR, "invalid locale name for builtin provider");
        unreachable!()
    }
}

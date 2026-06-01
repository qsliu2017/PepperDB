//! tsearch/dict_ispell.c - Ispell dictionary interface

use crate::prelude::*;

use crate::commands::defrem::defGetString;
use crate::nodes::parsenodes::DefElem;
use crate::nodes::pg_list::{lfirst, list_head, lnext, List, ListCell};
use crate::utils::fmgr::FunctionCallInfo;
use crate::tsearch::dict_simple::TSLexeme;
use crate::tsearch::ts_public::{
    get_tsearch_config_filename, readstoplist, searchstoplist, StopList,
};
use crate::{PG_GETARG_INT32, PG_GETARG_POINTER, PG_RETURN_POINTER};

use std::ffi::{c_char, c_int};

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

// catalog/pg_collation_d.h
const DEFAULT_COLLATION_OID: Oid = 100;

// ----------------------------------------------------------------------------
// IspellDict + Ispell build/normalize routines.  tsearch/dicts/spell.c is not
// ported yet, so IspellDict is an opaque-by-size struct and the NI* routines
// are local stubs.
// ----------------------------------------------------------------------------

// STUB: tsearch/dicts/spell.h IspellDict.  Not ported.
#[repr(C)]
pub struct IspellDict {
    _opaque: [u8; 0],
}

// TODO(pg-port): translate tsearch/dicts/spell.c (NI* + IspellDict).
unsafe fn NIStartBuild(_dict: *mut IspellDict) {
    unimplemented!()
}

// TODO(pg-port): translate tsearch/dicts/spell.c NIImportDictionary.
unsafe fn NIImportDictionary(_dict: *mut IspellDict, _filename: *const c_char) {
    unimplemented!()
}

// TODO(pg-port): translate tsearch/dicts/spell.c NIImportAffixes.
unsafe fn NIImportAffixes(_dict: *mut IspellDict, _filename: *const c_char) {
    unimplemented!()
}

// TODO(pg-port): translate tsearch/dicts/spell.c NISortDictionary.
unsafe fn NISortDictionary(_dict: *mut IspellDict) {
    unimplemented!()
}

// TODO(pg-port): translate tsearch/dicts/spell.c NISortAffixes.
unsafe fn NISortAffixes(_dict: *mut IspellDict) {
    unimplemented!()
}

// TODO(pg-port): translate tsearch/dicts/spell.c NIFinishBuild.
unsafe fn NIFinishBuild(_dict: *mut IspellDict) {
    unimplemented!()
}

// TODO(pg-port): translate tsearch/dicts/spell.c NINormalizeWord.
unsafe fn NINormalizeWord(_dict: *mut IspellDict, _word: *mut c_char) -> *mut TSLexeme {
    unimplemented!()
}

// STUB: utils/formatting.c str_tolower.  Not ported; signature mirrors
// str_tolower(buff, nbytes, collid) returning a palloc'd NUL-terminated string.
// TODO(pg-port): route through utils/formatting.c str_tolower.
unsafe fn str_tolower(_buff: *const c_char, _nbytes: c_int, _collid: Oid) -> *mut c_char {
    unimplemented!()
}

// ----------------------------------------------------------------------------

#[repr(C)]
pub struct DictISpell {
    pub stoplist: StopList,
    pub obj: IspellDict,
}

#[unsafe(no_mangle)]
pub unsafe fn dispell_init(fcinfo: FunctionCallInfo) -> Datum {
    let dictoptions = PG_GETARG_POINTER!(fcinfo, 0) as *mut List;
    let mut affloaded = false;
    let mut dictloaded = false;
    let mut stoploaded = false;

    let d = palloc0(core::mem::size_of::<DictISpell>()) as *mut DictISpell;

    NIStartBuild(&mut (*d).obj);

    let mut l: *mut ListCell = list_head(dictoptions);
    while !l.is_null() {
        let defel = lfirst(l) as *mut DefElem;

        if strcmp((*defel).defname, c"dictfile".as_ptr()) == 0 {
            if dictloaded {
                ereport!(ERROR, errmsg!("multiple DictFile parameters"));
            }
            NIImportDictionary(
                &mut (*d).obj,
                get_tsearch_config_filename(defGetString(defel), c"dict".as_ptr()),
            );
            dictloaded = true;
        } else if strcmp((*defel).defname, c"afffile".as_ptr()) == 0 {
            if affloaded {
                ereport!(ERROR, errmsg!("multiple AffFile parameters"));
            }
            NIImportAffixes(
                &mut (*d).obj,
                get_tsearch_config_filename(defGetString(defel), c"affix".as_ptr()),
            );
            affloaded = true;
        } else if strcmp((*defel).defname, c"stopwords".as_ptr()) == 0 {
            if stoploaded {
                ereport!(ERROR, errmsg!("multiple StopWords parameters"));
            }
            readstoplist(defGetString(defel), &mut (*d).stoplist, Some(str_tolower_wordop));
            stoploaded = true;
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "unrecognized Ispell parameter: \"{}\"",
                    core::ffi::CStr::from_ptr((*defel).defname).to_string_lossy()
                )
            );
        }

        l = lnext(dictoptions, l);
    }

    if affloaded && dictloaded {
        NISortDictionary(&mut (*d).obj);
        NISortAffixes(&mut (*d).obj);
    } else if !affloaded {
        ereport!(ERROR, errmsg!("missing AffFile parameter"));
    } else {
        ereport!(ERROR, errmsg!("missing DictFile parameter"));
    }

    NIFinishBuild(&mut (*d).obj);

    PG_RETURN_POINTER!(d);
}

// str_tolower adapted to the readstoplist wordop signature
// (*const c_char, Size, Oid) -> *mut c_char.
unsafe extern "C" fn str_tolower_wordop(
    buff: *const c_char,
    nbytes: Size,
    collid: Oid,
) -> *mut c_char {
    str_tolower(buff, nbytes as c_int, collid)
}

#[unsafe(no_mangle)]
pub unsafe fn dispell_lexize(fcinfo: FunctionCallInfo) -> Datum {
    let d = PG_GETARG_POINTER!(fcinfo, 0) as *mut DictISpell;
    let r#in = PG_GETARG_POINTER!(fcinfo, 1) as *mut c_char;
    let len = PG_GETARG_INT32!(fcinfo, 2);

    if len <= 0 {
        PG_RETURN_POINTER!(null_mut::<TSLexeme>());
    }

    let txt = str_tolower(r#in, len, DEFAULT_COLLATION_OID);
    let res = NINormalizeWord(&mut (*d).obj, txt);

    if res.is_null() {
        PG_RETURN_POINTER!(null_mut::<TSLexeme>());
    }

    let mut cptr = res;
    let mut ptr = cptr;
    while !(*ptr).lexeme.is_null() {
        if searchstoplist(&mut (*d).stoplist, (*ptr).lexeme) {
            pfree((*ptr).lexeme as *mut core::ffi::c_void);
            (*ptr).lexeme = null_mut();
        } else {
            if cptr != ptr {
                core::ptr::copy_nonoverlapping(ptr, cptr, 1);
            }
            cptr = cptr.add(1);
        }
        ptr = ptr.add(1);
    }
    (*cptr).lexeme = null_mut();

    PG_RETURN_POINTER!(res);
}

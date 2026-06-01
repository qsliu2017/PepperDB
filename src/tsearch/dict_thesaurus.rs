//! src/backend/tsearch/dict_thesaurus.c
//!
//! Thesaurus dictionary: phrase to phrase substitution
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{uint16, uint32};
use crate::commands::define::defGetString;
use crate::mb::mbutils::pg_mblen_cstr;
use crate::nodes::parsenodes::DefElem;
use crate::nodes::pg_list::{lfirst, List, ListCell};
use crate::tsearch::dict_simple::{TSLexeme, TSL_ADDPOS};
use crate::tsearch::ts_locale::{
    t_iseq, tsearch_readline, tsearch_readline_begin, tsearch_readline_end, tsearch_readline_state,
};
use crate::tsearch::ts_public::DictSubState;
use crate::tsearch::ts_utils::get_tsearch_config_filename;
use crate::utils::cache::ts_cache::{lookup_ts_dictionary_cache, TSDictionaryCacheEntry};
use crate::utils::fmgr::FmgrInfo;
use crate::{current_cell, foreach};

/*
 * Temporary we use TSLexeme.flags for inner use...
 */
const DT_USEASIS: uint16 = 0x1000;

#[repr(C)]
struct LexemeInfo {
    idsubst: uint32,       /* entry's number in DictThesaurus->subst */
    posinsubst: uint16,    /* pos info in entry */
    tnvariant: uint16,     /* total num lexemes in one variant */
    nextentry: *mut LexemeInfo,
    nextvariant: *mut LexemeInfo,
}

#[repr(C)]
struct TheLexeme {
    lexeme: *mut c_char,
    entries: *mut LexemeInfo,
}

#[repr(C)]
struct TheSubstitute {
    lastlexeme: uint16, /* number lexemes to substitute */
    reslen: uint16,
    res: *mut TSLexeme, /* prepared substituted result */
}

#[repr(C)]
struct DictThesaurus {
    /* subdictionary to normalize lexemes */
    subdictOid: Oid,
    subdict: *mut TSDictionaryCacheEntry,

    /* Array to search lexeme by exact match */
    wrds: *mut TheLexeme,
    nwrds: c_int, /* current number of words */
    ntwrds: c_int, /* allocated array length */

    /*
     * Storage of substituted result, n-th element is for n-th expression
     */
    subst: *mut TheSubstitute,
    nsubst: c_int,
}

unsafe fn newLexeme(
    d: *mut DictThesaurus,
    b: *mut c_char,
    e: *mut c_char,
    idsubst: uint32,
    posinsubst: uint16,
) {
    let ptr: *mut TheLexeme;

    if (*d).nwrds >= (*d).ntwrds {
        if (*d).ntwrds == 0 {
            (*d).ntwrds = 16;
            (*d).wrds =
                palloc(size_of::<TheLexeme>() * (*d).ntwrds as usize) as *mut TheLexeme;
        } else {
            (*d).ntwrds *= 2;
            (*d).wrds = repalloc(
                (*d).wrds as *mut c_void,
                size_of::<TheLexeme>() * (*d).ntwrds as usize,
            ) as *mut TheLexeme;
        }
    }

    ptr = (*d).wrds.add((*d).nwrds as usize);
    (*d).nwrds += 1;

    let len = e.offset_from(b) as usize;
    (*ptr).lexeme = palloc(len + 1) as *mut c_char;

    memcpy((*ptr).lexeme as *mut c_void, b as *const c_void, len);
    *(*ptr).lexeme.add(len) = b'\0' as c_char;

    (*ptr).entries = palloc(size_of::<LexemeInfo>()) as *mut LexemeInfo;

    (*(*ptr).entries).nextentry = std::ptr::null_mut();
    (*(*ptr).entries).idsubst = idsubst;
    (*(*ptr).entries).posinsubst = posinsubst;
}

// addWrd uses C function-static variables nres/ntres
static mut ADDWRD_NRES: c_int = 0;
static mut ADDWRD_NTRES: c_int = 0;

unsafe fn addWrd(
    d: *mut DictThesaurus,
    b: *mut c_char,
    e: *mut c_char,
    idsubst: uint32,
    nwrd: uint16,
    posinsubst: uint32,
    useasis: bool,
) {
    let ptr: *mut TheSubstitute;

    if nwrd == 0 {
        ADDWRD_NRES = 0;
        ADDWRD_NTRES = 0;

        if idsubst >= (*d).nsubst as uint32 {
            if (*d).nsubst == 0 {
                (*d).nsubst = 16;
                (*d).subst = palloc(size_of::<TheSubstitute>() * (*d).nsubst as usize)
                    as *mut TheSubstitute;
            } else {
                (*d).nsubst *= 2;
                (*d).subst = repalloc(
                    (*d).subst as *mut c_void,
                    size_of::<TheSubstitute>() * (*d).nsubst as usize,
                ) as *mut TheSubstitute;
            }
        }
    }

    ptr = (*d).subst.add(idsubst as usize);

    (*ptr).lastlexeme = (posinsubst - 1) as uint16;

    if ADDWRD_NRES + 1 >= ADDWRD_NTRES {
        if ADDWRD_NTRES == 0 {
            ADDWRD_NTRES = 2;
            (*ptr).res =
                palloc(size_of::<TSLexeme>() * ADDWRD_NTRES as usize) as *mut TSLexeme;
        } else {
            ADDWRD_NTRES *= 2;
            (*ptr).res = repalloc(
                (*ptr).res as *mut c_void,
                size_of::<TSLexeme>() * ADDWRD_NTRES as usize,
            ) as *mut TSLexeme;
        }
    }

    let len = e.offset_from(b) as usize;
    (*(*ptr).res.add(ADDWRD_NRES as usize)).lexeme = palloc(len + 1) as *mut c_char;
    memcpy(
        (*(*ptr).res.add(ADDWRD_NRES as usize)).lexeme as *mut c_void,
        b as *const c_void,
        len,
    );
    *(*(*ptr).res.add(ADDWRD_NRES as usize)).lexeme.add(len) = b'\0' as c_char;

    (*(*ptr).res.add(ADDWRD_NRES as usize)).nvariant = nwrd;
    if useasis {
        (*(*ptr).res.add(ADDWRD_NRES as usize)).flags = DT_USEASIS;
    } else {
        (*(*ptr).res.add(ADDWRD_NRES as usize)).flags = 0;
    }

    ADDWRD_NRES += 1;
    (*(*ptr).res.add(ADDWRD_NRES as usize)).lexeme = std::ptr::null_mut();
}

const TR_WAITLEX: c_int = 1;
const TR_INLEX: c_int = 2;
const TR_WAITSUBS: c_int = 3;
const TR_INSUBS: c_int = 4;

unsafe fn thesaurusRead(mut filename: *const c_char, d: *mut DictThesaurus) {
    let mut trst: tsearch_readline_state = std::mem::zeroed();
    let mut idsubst: uint32 = 0;
    let mut useasis: bool = false;
    let mut line: *mut c_char;

    filename = get_tsearch_config_filename(filename, c"ths".as_ptr());
    if !tsearch_readline_begin(&mut trst, filename) {
        ereport!(
            ERROR,
            errmsg!("could not open thesaurus file")
        );
    }

    loop {
        line = tsearch_readline(&mut trst);
        if line.is_null() {
            break;
        }

        let mut ptr: *mut c_char;
        let mut state: c_int = TR_WAITLEX;
        let mut beginwrd: *mut c_char = std::ptr::null_mut();
        let mut posinsubst: uint32 = 0;
        let mut nwrd: uint32 = 0;

        ptr = line;

        /* is it a comment? */
        while *ptr != 0 && isspace(*ptr as u8 as c_int) != 0 {
            ptr = ptr.add(pg_mblen_cstr(ptr) as usize);
        }

        if t_iseq(ptr, b'#' as c_char)
            || *ptr == b'\0' as c_char
            || t_iseq(ptr, b'\n' as c_char)
            || t_iseq(ptr, b'\r' as c_char)
        {
            pfree(line as *mut c_void);
            continue;
        }

        while *ptr != 0 {
            if state == TR_WAITLEX {
                if t_iseq(ptr, b':' as c_char) {
                    if posinsubst == 0 {
                        ereport!(ERROR, errmsg!("unexpected delimiter"));
                    }
                    state = TR_WAITSUBS;
                } else if isspace(*ptr as u8 as c_int) == 0 {
                    beginwrd = ptr;
                    state = TR_INLEX;
                }
            } else if state == TR_INLEX {
                if t_iseq(ptr, b':' as c_char) {
                    newLexeme(d, beginwrd, ptr, idsubst, posinsubst as uint16);
                    posinsubst += 1;
                    state = TR_WAITSUBS;
                } else if isspace(*ptr as u8 as c_int) != 0 {
                    newLexeme(d, beginwrd, ptr, idsubst, posinsubst as uint16);
                    posinsubst += 1;
                    state = TR_WAITLEX;
                }
            } else if state == TR_WAITSUBS {
                if t_iseq(ptr, b'*' as c_char) {
                    useasis = true;
                    state = TR_INSUBS;
                    beginwrd = ptr.add(pg_mblen_cstr(ptr) as usize);
                } else if t_iseq(ptr, b'\\' as c_char) {
                    useasis = false;
                    state = TR_INSUBS;
                    beginwrd = ptr.add(pg_mblen_cstr(ptr) as usize);
                } else if isspace(*ptr as u8 as c_int) == 0 {
                    useasis = false;
                    beginwrd = ptr;
                    state = TR_INSUBS;
                }
            } else if state == TR_INSUBS {
                if isspace(*ptr as u8 as c_int) != 0 {
                    if ptr == beginwrd {
                        ereport!(ERROR, errmsg!("unexpected end of line or lexeme"));
                    }
                    addWrd(d, beginwrd, ptr, idsubst, nwrd as uint16, posinsubst, useasis);
                    nwrd += 1;
                    state = TR_WAITSUBS;
                }
            } else {
                elog!(ERROR, "unrecognized thesaurus state: {}", state);
            }

            ptr = ptr.add(pg_mblen_cstr(ptr) as usize);
        }

        if state == TR_INSUBS {
            if ptr == beginwrd {
                ereport!(ERROR, errmsg!("unexpected end of line or lexeme"));
            }
            addWrd(d, beginwrd, ptr, idsubst, nwrd as uint16, posinsubst, useasis);
            nwrd += 1;
        }

        idsubst += 1;

        if !(nwrd != 0 && posinsubst != 0) {
            ereport!(ERROR, errmsg!("unexpected end of line"));
        }

        if nwrd != (nwrd as uint16) as uint32 || posinsubst != (posinsubst as uint16) as uint32 {
            ereport!(ERROR, errmsg!("too many lexemes in thesaurus entry"));
        }

        pfree(line as *mut c_void);
    }

    (*d).nsubst = idsubst as c_int;

    tsearch_readline_end(&mut trst);
}

unsafe fn addCompiledLexeme(
    mut newwrds: *mut TheLexeme,
    nnw: *mut c_int,
    tnm: *mut c_int,
    lexeme: *mut TSLexeme,
    src: *mut LexemeInfo,
    tnvariant: uint16,
) -> *mut TheLexeme {
    if *nnw >= *tnm {
        *tnm *= 2;
        newwrds = repalloc(
            newwrds as *mut c_void,
            size_of::<TheLexeme>() * *tnm as usize,
        ) as *mut TheLexeme;
    }

    (*newwrds.add(*nnw as usize)).entries =
        palloc(size_of::<LexemeInfo>()) as *mut LexemeInfo;

    if !lexeme.is_null() && !(*lexeme).lexeme.is_null() {
        (*newwrds.add(*nnw as usize)).lexeme = pstrdup((*lexeme).lexeme);
        (*(*newwrds.add(*nnw as usize)).entries).tnvariant = tnvariant;
    } else {
        (*newwrds.add(*nnw as usize)).lexeme = std::ptr::null_mut();
        (*(*newwrds.add(*nnw as usize)).entries).tnvariant = 1;
    }

    (*(*newwrds.add(*nnw as usize)).entries).idsubst = (*src).idsubst;
    (*(*newwrds.add(*nnw as usize)).entries).posinsubst = (*src).posinsubst;

    (*(*newwrds.add(*nnw as usize)).entries).nextentry = std::ptr::null_mut();

    *nnw += 1;
    newwrds
}

unsafe fn cmpLexemeInfo(a: *mut LexemeInfo, b: *mut LexemeInfo) -> c_int {
    if a.is_null() || b.is_null() {
        return 0;
    }

    if (*a).idsubst == (*b).idsubst {
        if (*a).posinsubst == (*b).posinsubst {
            if (*a).tnvariant == (*b).tnvariant {
                return 0;
            }

            return if (*a).tnvariant > (*b).tnvariant { 1 } else { -1 };
        }

        return if (*a).posinsubst > (*b).posinsubst { 1 } else { -1 };
    }

    if (*a).idsubst > (*b).idsubst { 1 } else { -1 }
}

unsafe fn cmpLexeme(a: *const TheLexeme, b: *const TheLexeme) -> c_int {
    if (*a).lexeme.is_null() {
        if (*b).lexeme.is_null() {
            return 0;
        } else {
            return 1;
        }
    } else if (*b).lexeme.is_null() {
        return -1;
    }

    strcmp((*a).lexeme, (*b).lexeme)
}

unsafe extern "C" fn cmpLexemeQ(a: *const c_void, b: *const c_void) -> c_int {
    cmpLexeme(a as *const TheLexeme, b as *const TheLexeme)
}

unsafe extern "C" fn cmpTheLexeme(a: *const c_void, b: *const c_void) -> c_int {
    let la = a as *const TheLexeme;
    let lb = b as *const TheLexeme;
    let res: c_int;

    res = cmpLexeme(la, lb);
    if res != 0 {
        return res;
    }

    -cmpLexemeInfo((*la).entries, (*lb).entries)
}

unsafe fn compileTheLexeme(d: *mut DictThesaurus) {
    let mut i: c_int;
    let mut nnw: c_int = 0;
    let mut tnm: c_int = 16;
    let mut newwrds: *mut TheLexeme =
        palloc(size_of::<TheLexeme>() * tnm as usize) as *mut TheLexeme;
    let mut ptrwrds: *mut TheLexeme;

    i = 0;
    while i < (*d).nwrds {
        let mut ptr: *mut TSLexeme;

        if strcmp((*(*d).wrds.add(i as usize)).lexeme, c"?".as_ptr()) == 0 {
            /* Is stop word marker? */
            newwrds = addCompiledLexeme(
                newwrds,
                &mut nnw,
                &mut tnm,
                std::ptr::null_mut(),
                (*(*d).wrds.add(i as usize)).entries,
                0,
            );
        } else {
            ptr = DatumGetPointer(FunctionCall4(
                &mut (*(*d).subdict).lexize,
                PointerGetDatum((*(*d).subdict).dictData),
                PointerGetDatum((*(*d).wrds.add(i as usize)).lexeme as *const c_void),
                Int32GetDatum(strlen((*(*d).wrds.add(i as usize)).lexeme) as i32),
                PointerGetDatum(std::ptr::null()),
            )) as *mut TSLexeme;

            if ptr.is_null() {
                elog!(
                    ERROR,
                    "thesaurus sample word \"{}\" isn't recognized by subdictionary (rule {})",
                    "?",
                    (*(*(*d).wrds.add(i as usize)).entries).idsubst + 1
                );
            } else if (*ptr).lexeme.is_null() {
                elog!(
                    ERROR,
                    "thesaurus sample word \"{}\" is a stop word (rule {})",
                    "?",
                    (*(*(*d).wrds.add(i as usize)).entries).idsubst + 1
                );
            } else {
                while !(*ptr).lexeme.is_null() {
                    let mut remptr: *mut TSLexeme = ptr.add(1);
                    let mut tnvar: c_int = 1;
                    let curvar: c_int = (*ptr).nvariant as c_int;

                    /* compute n words in one variant */
                    while !(*remptr).lexeme.is_null() {
                        if (*remptr).nvariant != (*remptr.sub(1)).nvariant {
                            break;
                        }
                        tnvar += 1;
                        remptr = remptr.add(1);
                    }

                    remptr = ptr;
                    while !(*remptr).lexeme.is_null() && (*remptr).nvariant as c_int == curvar {
                        newwrds = addCompiledLexeme(
                            newwrds,
                            &mut nnw,
                            &mut tnm,
                            remptr,
                            (*(*d).wrds.add(i as usize)).entries,
                            tnvar as uint16,
                        );
                        remptr = remptr.add(1);
                    }

                    ptr = remptr;
                }
            }
        }

        pfree((*(*d).wrds.add(i as usize)).lexeme as *mut c_void);
        pfree((*(*d).wrds.add(i as usize)).entries as *mut c_void);

        i += 1;
    }

    if !(*d).wrds.is_null() {
        pfree((*d).wrds as *mut c_void);
    }
    (*d).wrds = newwrds;
    (*d).nwrds = nnw;
    (*d).ntwrds = tnm;

    if (*d).nwrds > 1 {
        qsort(
            (*d).wrds as *mut c_void,
            (*d).nwrds as usize,
            size_of::<TheLexeme>(),
            cmpTheLexeme,
        );

        /* uniq */
        newwrds = (*d).wrds;
        ptrwrds = (*d).wrds.add(1);
        while ptrwrds.offset_from((*d).wrds) < (*d).nwrds as isize {
            if cmpLexeme(ptrwrds, newwrds) == 0 {
                if cmpLexemeInfo((*ptrwrds).entries, (*newwrds).entries) != 0 {
                    (*(*ptrwrds).entries).nextentry = (*newwrds).entries;
                    (*newwrds).entries = (*ptrwrds).entries;
                } else {
                    pfree((*ptrwrds).entries as *mut c_void);
                }

                if !(*ptrwrds).lexeme.is_null() {
                    pfree((*ptrwrds).lexeme as *mut c_void);
                }
            } else {
                newwrds = newwrds.add(1);
                *newwrds = std::ptr::read(ptrwrds);
            }

            ptrwrds = ptrwrds.add(1);
        }

        (*d).nwrds = (newwrds.offset_from((*d).wrds) + 1) as c_int;
        (*d).wrds = repalloc(
            (*d).wrds as *mut c_void,
            size_of::<TheLexeme>() * (*d).nwrds as usize,
        ) as *mut TheLexeme;
    }
}

unsafe fn compileTheSubstitute(d: *mut DictThesaurus) {
    let mut i: c_int;

    i = 0;
    while i < (*d).nsubst {
        let rem: *mut TSLexeme = (*(*d).subst.add(i as usize)).res;
        let mut outptr: *mut TSLexeme;
        let mut inptr: *mut TSLexeme;
        let mut n: c_int = 2;

        (*(*d).subst.add(i as usize)).res =
            palloc(size_of::<TSLexeme>() * n as usize) as *mut TSLexeme;
        outptr = (*(*d).subst.add(i as usize)).res;
        (*outptr).lexeme = std::ptr::null_mut();
        inptr = rem;

        while !inptr.is_null() && !(*inptr).lexeme.is_null() {
            let mut lexized: *mut TSLexeme;
            let mut tmplex: [TSLexeme; 2] = std::mem::zeroed();

            if (*inptr).flags & DT_USEASIS != 0 {
                /* do not lexize */
                tmplex[0] = core::ptr::read(inptr);
                tmplex[0].flags = 0;
                tmplex[1].lexeme = std::ptr::null_mut();
                lexized = tmplex.as_mut_ptr();
            } else {
                lexized = DatumGetPointer(FunctionCall4(
                    &mut (*(*d).subdict).lexize,
                    PointerGetDatum((*(*d).subdict).dictData),
                    PointerGetDatum((*inptr).lexeme as *const c_void),
                    Int32GetDatum(strlen((*inptr).lexeme) as i32),
                    PointerGetDatum(std::ptr::null()),
                )) as *mut TSLexeme;
            }

            if !lexized.is_null() && !(*lexized).lexeme.is_null() {
                let toset: isize =
                    if !(*lexized).lexeme.is_null() && outptr != (*(*d).subst.add(i as usize)).res {
                        outptr.offset_from((*(*d).subst.add(i as usize)).res)
                    } else {
                        -1
                    };

                while !(*lexized).lexeme.is_null() {
                    if outptr.offset_from((*(*d).subst.add(i as usize)).res) + 1 >= n as isize {
                        let diff = outptr.offset_from((*(*d).subst.add(i as usize)).res);

                        n *= 2;
                        (*(*d).subst.add(i as usize)).res = repalloc(
                            (*(*d).subst.add(i as usize)).res as *mut c_void,
                            size_of::<TSLexeme>() * n as usize,
                        ) as *mut TSLexeme;
                        outptr = (*(*d).subst.add(i as usize)).res.offset(diff);
                    }

                    *outptr = core::ptr::read(lexized);
                    (*outptr).lexeme = pstrdup((*lexized).lexeme);

                    outptr = outptr.add(1);
                    lexized = lexized.add(1);
                }

                if toset > 0 {
                    (*(*(*d).subst.add(i as usize)).res.offset(toset)).flags |= TSL_ADDPOS;
                }
            } else if !lexized.is_null() {
                elog!(
                    ERROR,
                    "thesaurus substitute word \"{}\" is a stop word (rule {})",
                    "?",
                    i + 1
                );
            } else {
                elog!(
                    ERROR,
                    "thesaurus substitute word \"{}\" isn't recognized by subdictionary (rule {})",
                    "?",
                    i + 1
                );
            }

            if !(*inptr).lexeme.is_null() {
                pfree((*inptr).lexeme as *mut c_void);
            }
            inptr = inptr.add(1);
        }

        if outptr == (*(*d).subst.add(i as usize)).res {
            elog!(
                ERROR,
                "thesaurus substitute phrase is empty (rule {})",
                i + 1
            );
        }

        (*(*d).subst.add(i as usize)).reslen =
            outptr.offset_from((*(*d).subst.add(i as usize)).res) as uint16;

        pfree(rem as *mut c_void);

        i += 1;
    }
}

#[no_mangle]
pub unsafe extern "C" fn thesaurus_init(fcinfo: FunctionCallInfo) -> Datum {
    let dictoptions: *mut List = PG_GETARG_POINTER(fcinfo, 0) as *mut List;
    let d: *mut DictThesaurus;
    let mut subdictname: *mut c_char = std::ptr::null_mut();
    let mut fileloaded: bool = false;
    let namelist: *mut List;
    let l: *mut ListCell;

    d = palloc0(size_of::<DictThesaurus>()) as *mut DictThesaurus;

    foreach!(l, dictoptions, {
        let defel: *mut DefElem = lfirst(current_cell!(l)) as *mut DefElem;

        if strcmp((*defel).defname, c"dictfile".as_ptr()) == 0 {
            if fileloaded {
                ereport!(ERROR, errmsg!("multiple DictFile parameters"));
            }
            thesaurusRead(defGetString(defel), d);
            fileloaded = true;
        } else if strcmp((*defel).defname, c"dictionary".as_ptr()) == 0 {
            if !subdictname.is_null() {
                ereport!(ERROR, errmsg!("multiple Dictionary parameters"));
            }
            subdictname = pstrdup(defGetString(defel));
        } else {
            elog!(
                ERROR,
                "unrecognized Thesaurus parameter: \"{}\"",
                "?"
            );
        }
    });

    if !fileloaded {
        ereport!(ERROR, errmsg!("missing DictFile parameter"));
    }
    if subdictname.is_null() {
        ereport!(ERROR, errmsg!("missing Dictionary parameter"));
    }

    namelist = stringToQualifiedNameList(subdictname, std::ptr::null_mut());
    (*d).subdictOid = get_ts_dict_oid(namelist, false);
    (*d).subdict = lookup_ts_dictionary_cache((*d).subdictOid);

    compileTheLexeme(d);
    compileTheSubstitute(d);

    PG_RETURN_POINTER(d as *mut c_void)
}

unsafe fn findTheLexeme(d: *mut DictThesaurus, lexeme: *mut c_char) -> *mut LexemeInfo {
    let mut key: TheLexeme = std::mem::zeroed();
    let res: *mut TheLexeme;

    if (*d).nwrds == 0 {
        return std::ptr::null_mut();
    }

    key.lexeme = lexeme;
    key.entries = std::ptr::null_mut();

    res = bsearch(
        &key as *const TheLexeme as *const c_void,
        (*d).wrds as *const c_void,
        (*d).nwrds as usize,
        size_of::<TheLexeme>(),
        cmpLexemeQ,
    ) as *mut TheLexeme;

    if res.is_null() {
        return std::ptr::null_mut();
    }
    (*res).entries
}

unsafe fn matchIdSubst(mut stored: *mut LexemeInfo, idsubst: uint32) -> bool {
    let mut res: bool = true;

    if !stored.is_null() {
        res = false;

        while !stored.is_null() {
            if (*stored).idsubst == idsubst {
                res = true;
                break;
            }
            stored = (*stored).nextvariant;
        }
    }

    res
}

unsafe fn findVariant(
    mut in_: *mut LexemeInfo,
    stored: *mut LexemeInfo,
    curpos: uint16,
    newin: *mut *mut LexemeInfo,
    newn: c_int,
) -> *mut LexemeInfo {
    loop {
        let mut i: c_int;
        let mut ptr: *mut LexemeInfo = *newin.add(0);

        i = 0;
        while i < newn {
            while !(*newin.add(i as usize)).is_null()
                && (**newin.add(i as usize)).idsubst < (*ptr).idsubst
            {
                *newin.add(i as usize) = (**newin.add(i as usize)).nextentry;
            }

            if (*newin.add(i as usize)).is_null() {
                return in_;
            }

            if (**newin.add(i as usize)).idsubst > (*ptr).idsubst {
                ptr = *newin.add(i as usize);
                i = -1;
                i += 1;
                continue;
            }

            while (**newin.add(i as usize)).idsubst == (*ptr).idsubst {
                if (**newin.add(i as usize)).posinsubst == curpos
                    && (**newin.add(i as usize)).tnvariant == newn as uint16
                {
                    ptr = *newin.add(i as usize);
                    break;
                }

                *newin.add(i as usize) = (**newin.add(i as usize)).nextentry;
                if (*newin.add(i as usize)).is_null() {
                    return in_;
                }
            }

            if (**newin.add(i as usize)).idsubst != (*ptr).idsubst {
                ptr = *newin.add(i as usize);
                i = -1;
                i += 1;
                continue;
            }

            i += 1;
        }

        if i == newn
            && matchIdSubst(stored, (*ptr).idsubst)
            && (in_.is_null() || !matchIdSubst(in_, (*ptr).idsubst))
        {
            /* found */
            (*ptr).nextvariant = in_;
            in_ = ptr;
        }

        /* step forward */
        i = 0;
        while i < newn {
            *newin.add(i as usize) = (**newin.add(i as usize)).nextentry;
            i += 1;
        }
    }
}

unsafe fn copyTSLexeme(ts: *mut TheSubstitute) -> *mut TSLexeme {
    let res: *mut TSLexeme;
    let mut i: uint16;

    res = palloc(size_of::<TSLexeme>() * ((*ts).reslen as usize + 1)) as *mut TSLexeme;
    i = 0;
    while i < (*ts).reslen {
        *res.add(i as usize) = core::ptr::read((*ts).res.add(i as usize));
        (*res.add(i as usize)).lexeme = pstrdup((*(*ts).res.add(i as usize)).lexeme);
        i += 1;
    }

    (*res.add((*ts).reslen as usize)).lexeme = std::ptr::null_mut();

    res
}

unsafe fn checkMatch(
    d: *mut DictThesaurus,
    mut info: *mut LexemeInfo,
    curpos: uint16,
    moreres: *mut bool,
) -> *mut TSLexeme {
    *moreres = false;
    while !info.is_null() {
        Assert!((*info).idsubst < (*d).nsubst as uint32);
        if !(*info).nextvariant.is_null() {
            *moreres = true;
        }
        if (*(*d).subst.add((*info).idsubst as usize)).lastlexeme == curpos {
            return copyTSLexeme((*d).subst.add((*info).idsubst as usize));
        }
        info = (*info).nextvariant;
    }

    std::ptr::null_mut()
}

#[no_mangle]
pub unsafe extern "C" fn thesaurus_lexize(fcinfo: FunctionCallInfo) -> Datum {
    let d: *mut DictThesaurus = PG_GETARG_POINTER(fcinfo, 0) as *mut DictThesaurus;
    let dstate: *mut DictSubState = PG_GETARG_POINTER(fcinfo, 3) as *mut DictSubState;
    let mut res: *mut TSLexeme;
    let stored: *mut LexemeInfo;
    let mut info: *mut LexemeInfo = std::ptr::null_mut();
    let mut curpos: uint16 = 0;
    let mut moreres: bool = false;

    if PG_NARGS(fcinfo) != 4 || dstate.is_null() {
        elog!(ERROR, "forbidden call of thesaurus or nested call");
    }

    if (*dstate).isend {
        return PG_RETURN_POINTER(std::ptr::null_mut());
    }
    stored = (*dstate).private_state as *mut LexemeInfo;

    if !stored.is_null() {
        curpos = (*stored).posinsubst + 1;
    }

    if !(*(*d).subdict).isvalid {
        (*d).subdict = lookup_ts_dictionary_cache((*d).subdictOid);
    }

    res = DatumGetPointer(FunctionCall4(
        &mut (*(*d).subdict).lexize,
        PointerGetDatum((*(*d).subdict).dictData),
        PG_GETARG_DATUM(fcinfo, 1),
        PG_GETARG_DATUM(fcinfo, 2),
        PointerGetDatum(std::ptr::null()),
    )) as *mut TSLexeme;

    if !res.is_null() && !(*res).lexeme.is_null() {
        let mut ptr: *mut TSLexeme = res;
        let basevar: *mut TSLexeme;

        // basevar declared mut via local rebind below
        let _ = basevar;
        let mut basevar: *mut TSLexeme;

        while !(*ptr).lexeme.is_null() {
            let nv: uint16 = (*ptr).nvariant;
            let mut i: uint16;
            let mut nlex: uint16 = 0;
            let infos: *mut *mut LexemeInfo;

            basevar = ptr;
            while !(*ptr).lexeme.is_null() && nv == (*ptr).nvariant {
                nlex += 1;
                ptr = ptr.add(1);
            }

            infos = palloc(size_of::<*mut LexemeInfo>() * nlex as usize) as *mut *mut LexemeInfo;
            i = 0;
            while i < nlex {
                *infos.add(i as usize) = findTheLexeme(d, (*basevar.add(i as usize)).lexeme);
                if (*infos.add(i as usize)).is_null() {
                    break;
                }
                i += 1;
            }

            if i < nlex {
                /* no chance to find */
                pfree(infos as *mut c_void);
                continue;
            }

            info = findVariant(info, stored, curpos, infos, nlex as c_int);
        }
    } else if !res.is_null() {
        /* stop-word */
        let mut infos: *mut LexemeInfo = findTheLexeme(d, std::ptr::null_mut());

        info = findVariant(std::ptr::null_mut(), stored, curpos, &mut infos, 1);
    } else {
        info = std::ptr::null_mut(); /* word isn't recognized */
    }

    (*dstate).private_state = info as *mut c_void;

    if info.is_null() {
        (*dstate).getnext = false;
        return PG_RETURN_POINTER(std::ptr::null_mut());
    }

    res = checkMatch(d, info, curpos, &mut moreres);
    if !res.is_null() {
        (*dstate).getnext = moreres;
        return PG_RETURN_POINTER(res as *mut c_void);
    }

    (*dstate).getnext = true;

    PG_RETURN_POINTER(std::ptr::null_mut())
}

// ---- local stubs for unported dependencies ----

pub type FunctionCallInfo = *mut c_void;

unsafe fn stringToQualifiedNameList(_string: *mut c_char, _escontext: *mut c_void) -> *mut List {
    unimplemented!() // TODO: src/backend/catalog/namespace.c
}

unsafe fn get_ts_dict_oid(_names: *mut List, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO: src/backend/utils/cache/ts_cache.c
}

unsafe fn FunctionCall4(
    _flinfo: *mut FmgrInfo,
    _arg1: Datum,
    _arg2: Datum,
    _arg3: Datum,
    _arg4: Datum,
) -> Datum {
    unimplemented!() // TODO: src/backend/utils/fmgr/fmgr.c
}

unsafe fn PG_GETARG_POINTER(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut c_void {
    unimplemented!() // TODO: src/include/fmgr.h
}

unsafe fn PG_GETARG_DATUM(_fcinfo: FunctionCallInfo, _n: c_int) -> Datum {
    unimplemented!() // TODO: src/include/fmgr.h
}

unsafe fn PG_NARGS(_fcinfo: FunctionCallInfo) -> c_int {
    unimplemented!() // TODO: src/include/fmgr.h
}

unsafe fn PG_RETURN_POINTER(_p: *mut c_void) -> Datum {
    unimplemented!() // TODO: src/include/fmgr.h
}

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn isspace(c: c_int) -> c_int;
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    );
    fn bsearch(
        key: *const c_void,
        base: *const c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    ) -> *mut c_void;
}

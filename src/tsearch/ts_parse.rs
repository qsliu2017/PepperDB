//! ts_parse.c
//!		main parse functions for tsearch
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!	  src/backend/tsearch/ts_parse.c
//!
//! #include mapping:
//!   - "postgres.h"             -> crate::prelude::* (Datum, c-types, palloc/pfree/
//!                                  repalloc, MemoryContext helpers, elog!/ereport!)
//!   - "tsearch/ts_cache.h"     -> the TS*CacheEntry / ListDictionary structs are
//!                                  merged below (no ts_cache.rs yet); the three
//!                                  lookup_ts_*_cache prototypes are STUBBED.
//!   - "tsearch/ts_utils.h"     -> ParsedWord/ParsedText merged below (file-local,
//!                                  matching the canonical ts_utils.h layout);
//!                                  tsCompareString from utils::adt::tsvector.
//!   - "varatt.h"               -> SET_VARSIZE from crate::varatt.
//!
//! REAL vs STUB:
//!   * The whole Lexize subsystem (LexizeInit/LPLAddTail/LPLRemoveHead/
//!     LexizeAddLemm/RemoveHead/setCorrLex/moveToWaste/setNewTmpRes/LexizeExec),
//!     parsetext, the headline framework (hladdword/hlfinditem/addHLParsedLex/
//!     hlparsetext) and generateHeadline are translated 1:1.
//!   * lookup_ts_config_cache / lookup_ts_parser_cache / lookup_ts_dictionary_cache
//!     are STUBBED (ts_cache.c not ported).
//!   * FunctionCall1/3/4 collation-less wrappers are local shims around the
//!     FunctionCallNColl entry points (matching tsearch/dict.rs); FunctionCall2 is
//!     the crate macro.

use crate::prelude::*;

use crate::utils::adt::ts_type::{GETOPERAND, GETQUERY, QueryItem, QueryOperand, TSQuery, QI_VAL};
use crate::utils::adt::ts_type::{LIMITPOS, MAXSTRLEN};
use crate::utils::adt::tsvector::tsCompareString;

use crate::utils::fmgr::{FmgrInfo, FunctionCall1Coll, FunctionCall3Coll, FunctionCall4Coll};

use crate::tsearch::ts_public::{
    DictSubState, HeadlineParsedText, HeadlineWordEntry, TSLexeme, TSL_ADDPOS, TSL_FILTER,
    TSL_PREFIX,
};

use crate::utils::elog::NOTICE;
use crate::utils::palloc::MemoryContext;
use crate::varatt::SET_VARSIZE;

use std::ffi::{c_char, c_int, c_void};

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

// ----------------------------------------------------------------------------
// Merged from tsearch/ts_cache.h (no ts_cache.rs yet).
// All TS*CacheEntry structs share the common (objId,isvalid) header.
// ----------------------------------------------------------------------------

/* typedef struct { int len; Oid *dictIds; } ListDictionary; */
#[repr(C)]
pub struct ListDictionary {
    pub len: c_int,
    pub dictIds: *mut Oid,
}

#[repr(C)]
pub struct TSParserCacheEntry {
    /* prsId is the hash lookup key and MUST BE FIRST */
    pub prsId: Oid, /* OID of the parser */
    pub isvalid: bool,

    pub startOid: Oid,
    pub tokenOid: Oid,
    pub endOid: Oid,
    pub headlineOid: Oid,
    pub lextypeOid: Oid,

    /* Pre-set-up fmgr call of most needed parser's methods */
    pub prsstart: FmgrInfo,
    pub prstoken: FmgrInfo,
    pub prsend: FmgrInfo,
    pub prsheadline: FmgrInfo,
}

#[repr(C)]
pub struct TSDictionaryCacheEntry {
    /* dictId is the hash lookup key and MUST BE FIRST */
    pub dictId: Oid,
    pub isvalid: bool,

    /* most frequent fmgr call */
    pub lexizeOid: Oid,
    pub lexize: FmgrInfo,

    pub dictCtx: MemoryContext, /* memory context to store private data */
    pub dictData: *mut c_void,
}

#[repr(C)]
pub struct TSConfigCacheEntry {
    /* cfgId is the hash lookup key and MUST BE FIRST */
    pub cfgId: Oid,
    pub isvalid: bool,

    pub prsId: Oid,

    pub lenmap: c_int,
    pub map: *mut ListDictionary,
}

// TODO: tsearch/ts_cache.c lookup_ts_parser_cache not yet ported.
unsafe fn lookup_ts_parser_cache(prsId: Oid) -> *mut TSParserCacheEntry {
    let _ = prsId;
    unimplemented!() // TODO: tsearch/ts_cache.c
}

// TODO: tsearch/ts_cache.c lookup_ts_dictionary_cache not yet ported.
unsafe fn lookup_ts_dictionary_cache(dictId: Oid) -> *mut TSDictionaryCacheEntry {
    let _ = dictId;
    unimplemented!() // TODO: tsearch/ts_cache.c
}

// TODO: tsearch/ts_cache.c lookup_ts_config_cache not yet ported.
unsafe fn lookup_ts_config_cache(cfgId: Oid) -> *mut TSConfigCacheEntry {
    let _ = cfgId;
    unimplemented!() // TODO: tsearch/ts_cache.c
}

// ----------------------------------------------------------------------------
// Collation-less FunctionCall wrappers (matching tsearch/dict.rs).
// utils/fmgr.c FunctionCallN == FunctionCallNColl with InvalidOid collation.
// ----------------------------------------------------------------------------

unsafe fn FunctionCall1(flinfo: *mut FmgrInfo, arg1: Datum) -> Datum {
    FunctionCall1Coll(flinfo, InvalidOid, arg1)
}

unsafe fn FunctionCall3(flinfo: *mut FmgrInfo, arg1: Datum, arg2: Datum, arg3: Datum) -> Datum {
    FunctionCall3Coll(flinfo, InvalidOid, arg1, arg2, arg3)
}

unsafe fn FunctionCall4(
    flinfo: *mut FmgrInfo,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
) -> Datum {
    FunctionCall4Coll(flinfo, InvalidOid, arg1, arg2, arg3, arg4)
}

// FunctionCall2 is the crate macro (takes flinfo as first arg).
use crate::FunctionCall2;

// ----------------------------------------------------------------------------
// Merged from tsearch/ts_utils.h: ParsedWord / ParsedText.
// (These are file-local in C as well, used by parsetext to fill prs.)
// ----------------------------------------------------------------------------

#[repr(C)]
pub union ParsedWordPos {
    pub pos: uint16,
    /*
     * When apos array is used, apos[0] is the number of elements in the
     * array (excluding apos[0]), and alen is the allocated size of the
     * array.  We do not allow more than MAXNUMPOS array elements.
     */
    pub apos: *mut uint16,
}

#[repr(C)]
pub struct ParsedWord {
    pub flags: uint16, /* currently, only TSL_PREFIX */
    pub len: uint16,
    pub nvariant: uint16,
    pub alen: uint16,
    pub pos: ParsedWordPos,
    pub word: *mut c_char,
}

#[repr(C)]
pub struct ParsedText {
    pub words: *mut ParsedWord,
    pub lenwords: int32,
    pub curwords: int32,
    pub pos: int32,
}

const IGNORE_LONGLEXEME: c_int = 1;

/*
 * Lexize subsystem
 */

#[repr(C)]
pub struct ParsedLex {
    pub r#type: c_int,
    pub lemm: *mut c_char,
    pub lenlemm: c_int,
    pub next: *mut ParsedLex,
}

#[repr(C)]
pub struct ListParsedLex {
    pub head: *mut ParsedLex,
    pub tail: *mut ParsedLex,
}

#[repr(C)]
pub struct LexizeData {
    pub cfg: *mut TSConfigCacheEntry,
    pub curDictId: Oid,
    pub posDict: c_int,
    pub dictState: DictSubState,
    pub curSub: *mut ParsedLex,
    pub towork: ListParsedLex, /* current list to work */
    pub waste: ListParsedLex,  /* list of lexemes that already lexized */

    /*
     * fields to store last variant to lexize (basically, thesaurus or similar
     * to, which wants several lexemes
     */
    pub lastRes: *mut ParsedLex,
    pub tmpRes: *mut TSLexeme,
}

unsafe fn LexizeInit(ld: *mut LexizeData, cfg: *mut TSConfigCacheEntry) {
    (*ld).cfg = cfg;
    (*ld).curDictId = InvalidOid;
    (*ld).posDict = 0;
    (*ld).towork.head = null_mut();
    (*ld).towork.tail = null_mut();
    (*ld).curSub = null_mut();
    (*ld).waste.head = null_mut();
    (*ld).waste.tail = null_mut();
    (*ld).lastRes = null_mut();
    (*ld).tmpRes = null_mut();
}

unsafe fn LPLAddTail(list: *mut ListParsedLex, newpl: *mut ParsedLex) {
    if !(*list).tail.is_null() {
        (*(*list).tail).next = newpl;
        (*list).tail = newpl;
    } else {
        (*list).head = newpl;
        (*list).tail = newpl;
    }
    (*newpl).next = null_mut();
}

unsafe fn LPLRemoveHead(list: *mut ListParsedLex) -> *mut ParsedLex {
    let res = (*list).head;

    if !(*list).head.is_null() {
        (*list).head = (*(*list).head).next;
    }

    if (*list).head.is_null() {
        (*list).tail = null_mut();
    }

    res
}

unsafe fn LexizeAddLemm(ld: *mut LexizeData, type_: c_int, lemm: *mut c_char, lenlemm: c_int) {
    let newpl = palloc(core::mem::size_of::<ParsedLex>()) as *mut ParsedLex;

    (*newpl).r#type = type_;
    (*newpl).lemm = lemm;
    (*newpl).lenlemm = lenlemm;
    LPLAddTail(&mut (*ld).towork, newpl);
    (*ld).curSub = (*ld).towork.tail;
}

unsafe fn RemoveHead(ld: *mut LexizeData) {
    LPLAddTail(&mut (*ld).waste, LPLRemoveHead(&mut (*ld).towork));

    (*ld).posDict = 0;
}

unsafe fn setCorrLex(ld: *mut LexizeData, correspondLexem: *mut *mut ParsedLex) {
    if !correspondLexem.is_null() {
        *correspondLexem = (*ld).waste.head;
    } else {
        let mut tmp: *mut ParsedLex;
        let mut ptr: *mut ParsedLex = (*ld).waste.head;

        while !ptr.is_null() {
            tmp = (*ptr).next;
            pfree(ptr as *mut c_void);
            ptr = tmp;
        }
    }
    (*ld).waste.head = null_mut();
    (*ld).waste.tail = null_mut();
}

unsafe fn moveToWaste(ld: *mut LexizeData, stop: *mut ParsedLex) {
    let mut go = true;

    while !(*ld).towork.head.is_null() && go {
        if (*ld).towork.head == stop {
            (*ld).curSub = (*stop).next;
            go = false;
        }
        RemoveHead(ld);
    }
}

unsafe fn setNewTmpRes(ld: *mut LexizeData, lex: *mut ParsedLex, res: *mut TSLexeme) {
    if !(*ld).tmpRes.is_null() {
        let mut ptr: *mut TSLexeme = (*ld).tmpRes;
        while !(*ptr).lexeme.is_null() {
            pfree((*ptr).lexeme as *mut c_void);
            ptr = ptr.add(1);
        }
        pfree((*ld).tmpRes as *mut c_void);
    }
    (*ld).tmpRes = res;
    (*ld).lastRes = lex;
}

unsafe fn LexizeExec(ld: *mut LexizeData, correspondLexem: *mut *mut ParsedLex) -> *mut TSLexeme {
    let mut i: c_int;
    let mut map: *mut ListDictionary;
    let mut dict: *mut TSDictionaryCacheEntry;
    let mut res: *mut TSLexeme;

    if (*ld).curDictId == InvalidOid {
        /*
         * usual mode: dictionary wants only one word, but we should keep in
         * mind that we should go through all stack
         */

        while !(*ld).towork.head.is_null() {
            let curVal: *mut ParsedLex = (*ld).towork.head;
            let mut curValLemm: *mut c_char = (*curVal).lemm;
            let mut curValLenLemm: c_int = (*curVal).lenlemm;

            map = (*(*ld).cfg).map.add((*curVal).r#type as usize);

            if (*curVal).r#type == 0
                || (*curVal).r#type >= (*(*ld).cfg).lenmap
                || (*map).len == 0
            {
                /* skip this type of lexeme */
                RemoveHead(ld);
                continue;
            }

            i = (*ld).posDict;
            while i < (*map).len {
                dict = lookup_ts_dictionary_cache(*(*map).dictIds.add(i as usize));

                (*ld).dictState.isend = false;
                (*ld).dictState.getnext = false;
                (*ld).dictState.private_state = null_mut();
                res = DatumGetPointer(FunctionCall4(
                    &mut (*dict).lexize,
                    PointerGetDatum((*dict).dictData),
                    PointerGetDatum(curValLemm as *const c_void),
                    Int32GetDatum(curValLenLemm),
                    PointerGetDatum(&mut (*ld).dictState as *mut DictSubState as *const c_void),
                )) as *mut TSLexeme;

                if (*ld).dictState.getnext {
                    /*
                     * dictionary wants next word, so setup and store current
                     * position and go to multiword mode
                     */

                    (*ld).curDictId = DatumGetObjectId(*(*map).dictIds.add(i as usize) as Datum);
                    (*ld).posDict = i + 1;
                    (*ld).curSub = (*curVal).next;
                    if !res.is_null() {
                        setNewTmpRes(ld, curVal, res);
                    }
                    return LexizeExec(ld, correspondLexem);
                }

                if res.is_null() {
                    /* dictionary doesn't know this lexeme */
                    i += 1;
                    continue;
                }

                if (*res).flags as c_int & TSL_FILTER != 0 {
                    curValLemm = (*res).lexeme;
                    curValLenLemm = strlen((*res).lexeme) as c_int;
                    i += 1;
                    continue;
                }

                RemoveHead(ld);
                setCorrLex(ld, correspondLexem);
                return res;
            }

            RemoveHead(ld);
        }
    } else {
        /* curDictId is valid */
        dict = lookup_ts_dictionary_cache((*ld).curDictId);

        /*
         * Dictionary ld->curDictId asks us about following words
         */

        while !(*ld).curSub.is_null() {
            let curVal: *mut ParsedLex = (*ld).curSub;

            map = (*(*ld).cfg).map.add((*curVal).r#type as usize);

            if (*curVal).r#type != 0 {
                let mut dictExists = false;

                if (*curVal).r#type >= (*(*ld).cfg).lenmap || (*map).len == 0 {
                    /* skip this type of lexeme */
                    (*ld).curSub = (*curVal).next;
                    continue;
                }

                /*
                 * We should be sure that current type of lexeme is recognized
                 * by our dictionary: we just check is it exist in list of
                 * dictionaries ?
                 */
                i = 0;
                while i < (*map).len && !dictExists {
                    if (*ld).curDictId == DatumGetObjectId(*(*map).dictIds.add(i as usize) as Datum)
                    {
                        dictExists = true;
                    }
                    i += 1;
                }

                if !dictExists {
                    /*
                     * Dictionary can't work with current type of lexeme,
                     * return to basic mode and redo all stored lexemes
                     */
                    (*ld).curDictId = InvalidOid;
                    return LexizeExec(ld, correspondLexem);
                }
            }

            (*ld).dictState.isend = (*curVal).r#type == 0;
            (*ld).dictState.getnext = false;

            res = DatumGetPointer(FunctionCall4(
                &mut (*dict).lexize,
                PointerGetDatum((*dict).dictData),
                PointerGetDatum((*curVal).lemm as *const c_void),
                Int32GetDatum((*curVal).lenlemm),
                PointerGetDatum(&mut (*ld).dictState as *mut DictSubState as *const c_void),
            )) as *mut TSLexeme;

            if (*ld).dictState.getnext {
                /* Dictionary wants one more */
                (*ld).curSub = (*curVal).next;
                if !res.is_null() {
                    setNewTmpRes(ld, curVal, res);
                }
                continue;
            }

            if !res.is_null() || !(*ld).tmpRes.is_null() {
                /*
                 * Dictionary normalizes lexemes, so we remove from stack all
                 * used lexemes, return to basic mode and redo end of stack
                 * (if it exists)
                 */
                if !res.is_null() {
                    moveToWaste(ld, (*ld).curSub);
                } else {
                    res = (*ld).tmpRes;
                    moveToWaste(ld, (*ld).lastRes);
                }

                /* reset to initial state */
                (*ld).curDictId = InvalidOid;
                (*ld).posDict = 0;
                (*ld).lastRes = null_mut();
                (*ld).tmpRes = null_mut();
                setCorrLex(ld, correspondLexem);
                return res;
            }

            /*
             * Dict don't want next lexem and didn't recognize anything, redo
             * from ld->towork.head
             */
            (*ld).curDictId = InvalidOid;
            return LexizeExec(ld, correspondLexem);
        }
    }

    setCorrLex(ld, correspondLexem);
    null_mut()
}

/*
 * Parse string and lexize words.
 *
 * prs will be filled in.
 */
pub unsafe fn parsetext(cfgId: Oid, prs: *mut ParsedText, buf: *mut c_char, buflen: c_int) {
    let mut type_: c_int;
    let mut lenlemm: c_int = 0; /* silence compiler warning */
    let mut lemm: *mut c_char = null_mut();
    let mut ldata: LexizeData = core::mem::zeroed();
    let mut norms: *mut TSLexeme;
    let cfg: *mut TSConfigCacheEntry;
    let prsobj: *mut TSParserCacheEntry;
    let prsdata: *mut c_void;

    cfg = lookup_ts_config_cache(cfgId);
    prsobj = lookup_ts_parser_cache((*cfg).prsId);

    prsdata = DatumGetPointer(FunctionCall2!(
        &mut (*prsobj).prsstart,
        PointerGetDatum(buf as *const c_void),
        Int32GetDatum(buflen)
    )) as *mut c_void;

    LexizeInit(&mut ldata, cfg);

    loop {
        type_ = DatumGetInt32(FunctionCall3(
            &mut (*prsobj).prstoken,
            PointerGetDatum(prsdata),
            PointerGetDatum(&mut lemm as *mut *mut c_char as *const c_void),
            PointerGetDatum(&mut lenlemm as *mut c_int as *const c_void),
        ));

        if type_ > 0 && lenlemm >= MAXSTRLEN as c_int {
            if IGNORE_LONGLEXEME != 0 {
                elog!(
                    NOTICE,
                    "word is too long to be indexed; words longer than {} characters are ignored",
                    MAXSTRLEN
                );
                continue;
            } else {
                elog!(
                    ERROR,
                    "word is too long to be indexed; words longer than {} characters are ignored",
                    MAXSTRLEN
                );
            }
        }

        LexizeAddLemm(&mut ldata, type_, lemm, lenlemm);

        loop {
            norms = LexizeExec(&mut ldata, null_mut());
            if norms.is_null() {
                break;
            }

            let mut ptr: *mut TSLexeme = norms;

            (*prs).pos += 1; /* set pos */

            while !(*ptr).lexeme.is_null() {
                if (*prs).curwords == (*prs).lenwords {
                    (*prs).lenwords *= 2;
                    (*prs).words = repalloc(
                        (*prs).words as *mut c_void,
                        (*prs).lenwords as usize * core::mem::size_of::<ParsedWord>(),
                    ) as *mut ParsedWord;
                }

                if (*ptr).flags as c_int & TSL_ADDPOS != 0 {
                    (*prs).pos += 1;
                }
                let w = (*prs).words.add((*prs).curwords as usize);
                (*w).len = strlen((*ptr).lexeme) as uint16;
                (*w).word = (*ptr).lexeme;
                (*w).nvariant = (*ptr).nvariant;
                (*w).flags = ((*ptr).flags as c_int & TSL_PREFIX) as uint16;
                (*w).alen = 0;
                (*w).pos.pos = LIMITPOS((*prs).pos) as uint16;
                ptr = ptr.add(1);
                (*prs).curwords += 1;
            }
            pfree(norms as *mut c_void);
        }

        if !(type_ > 0) {
            break;
        }
    }

    FunctionCall1(&mut (*prsobj).prsend, PointerGetDatum(prsdata));
}

/*
 * Headline framework
 */

/* Add a word to prs->words[] */
unsafe fn hladdword(prs: *mut HeadlineParsedText, buf: *mut c_char, buflen: c_int, type_: c_int) {
    if (*prs).curwords >= (*prs).lenwords {
        (*prs).lenwords *= 2;
        (*prs).words = repalloc(
            (*prs).words as *mut c_void,
            (*prs).lenwords as usize * core::mem::size_of::<HeadlineWordEntry>(),
        ) as *mut HeadlineWordEntry;
    }
    let w = (*prs).words.add((*prs).curwords as usize);
    core::ptr::write_bytes(w, 0, 1);
    (*w).set_type(type_ as uint8 as uint32);
    (*w).set_len(buflen as uint32);
    (*w).word = palloc(buflen as usize) as *mut c_char;
    core::ptr::copy_nonoverlapping(buf, (*w).word, buflen as usize);
    (*prs).curwords += 1;
}

/*
 * Add pos and matching-query-item data to the just-added word.
 * Here, buf/buflen represent a processed lexeme, not raw token text.
 *
 * If the query contains more than one matching item, we replicate
 * the last-added word so that each item can be pointed to.  The
 * duplicate entries are marked with repeated = 1.
 */
unsafe fn hlfinditem(
    prs: *mut HeadlineParsedText,
    query: TSQuery,
    pos: int32,
    buf: *mut c_char,
    buflen: c_int,
) {
    let mut i: c_int;
    let mut item: *mut QueryItem = GETQUERY(query);
    let word: *mut HeadlineWordEntry;

    while (*prs).curwords + (*query).size >= (*prs).lenwords {
        (*prs).lenwords *= 2;
        (*prs).words = repalloc(
            (*prs).words as *mut c_void,
            (*prs).lenwords as usize * core::mem::size_of::<HeadlineWordEntry>(),
        ) as *mut HeadlineWordEntry;
    }

    word = (*prs).words.add(((*prs).curwords - 1) as usize);
    (*word).pos = LIMITPOS(pos) as uint16;
    i = 0;
    while i < (*query).size {
        if (*item).r#type == QI_VAL
            && tsCompareString(
                GETOPERAND(query).add((*item).qoperand.distance() as usize),
                (*item).qoperand.length() as c_int,
                buf,
                buflen,
                (*item).qoperand.prefix,
            ) == 0
        {
            if !(*word).item.is_null() {
                core::ptr::copy_nonoverlapping(
                    word,
                    (*prs).words.add((*prs).curwords as usize),
                    1,
                );
                (*(*prs).words.add((*prs).curwords as usize)).item =
                    &raw mut (*item).qoperand as *mut _;
                (*(*prs).words.add((*prs).curwords as usize)).set_repeated(1);
                (*prs).curwords += 1;
            } else {
                (*word).item = &raw mut (*item).qoperand as *mut _;
            }
        }
        item = item.add(1);
        i += 1;
    }
}

unsafe fn addHLParsedLex(
    prs: *mut HeadlineParsedText,
    query: TSQuery,
    mut lexs: *mut ParsedLex,
    norms: *mut TSLexeme,
) {
    let mut tmplexs: *mut ParsedLex;
    let mut ptr: *mut TSLexeme;
    let mut savedpos: int32;

    while !lexs.is_null() {
        if (*lexs).r#type > 0 {
            hladdword(prs, (*lexs).lemm, (*lexs).lenlemm, (*lexs).r#type);
        }

        ptr = norms;
        savedpos = (*prs).vectorpos;
        while !ptr.is_null() && !(*ptr).lexeme.is_null() {
            if (*ptr).flags as c_int & TSL_ADDPOS != 0 {
                savedpos += 1;
            }
            hlfinditem(prs, query, savedpos, (*ptr).lexeme, strlen((*ptr).lexeme) as c_int);
            ptr = ptr.add(1);
        }

        tmplexs = (*lexs).next;
        pfree(lexs as *mut c_void);
        lexs = tmplexs;
    }

    if !norms.is_null() {
        ptr = norms;
        while !(*ptr).lexeme.is_null() {
            if (*ptr).flags as c_int & TSL_ADDPOS != 0 {
                (*prs).vectorpos += 1;
            }
            pfree((*ptr).lexeme as *mut c_void);
            ptr = ptr.add(1);
        }
        pfree(norms as *mut c_void);
    }
}

pub unsafe fn hlparsetext(
    cfgId: Oid,
    prs: *mut HeadlineParsedText,
    query: TSQuery,
    buf: *mut c_char,
    buflen: c_int,
) {
    let mut type_: c_int;
    let mut lenlemm: c_int = 0; /* silence compiler warning */
    let mut lemm: *mut c_char = null_mut();
    let mut ldata: LexizeData = core::mem::zeroed();
    let mut norms: *mut TSLexeme;
    let mut lexs: *mut ParsedLex = null_mut();
    let cfg: *mut TSConfigCacheEntry;
    let prsobj: *mut TSParserCacheEntry;
    let prsdata: *mut c_void;

    cfg = lookup_ts_config_cache(cfgId);
    prsobj = lookup_ts_parser_cache((*cfg).prsId);

    prsdata = DatumGetPointer(FunctionCall2!(
        &mut (*prsobj).prsstart,
        PointerGetDatum(buf as *const c_void),
        Int32GetDatum(buflen)
    )) as *mut c_void;

    LexizeInit(&mut ldata, cfg);

    loop {
        type_ = DatumGetInt32(FunctionCall3(
            &mut (*prsobj).prstoken,
            PointerGetDatum(prsdata),
            PointerGetDatum(&mut lemm as *mut *mut c_char as *const c_void),
            PointerGetDatum(&mut lenlemm as *mut c_int as *const c_void),
        ));

        if type_ > 0 && lenlemm >= MAXSTRLEN as c_int {
            if IGNORE_LONGLEXEME != 0 {
                elog!(
                    NOTICE,
                    "word is too long to be indexed; words longer than {} characters are ignored",
                    MAXSTRLEN
                );
                continue;
            } else {
                elog!(
                    ERROR,
                    "word is too long to be indexed; words longer than {} characters are ignored",
                    MAXSTRLEN
                );
            }
        }

        LexizeAddLemm(&mut ldata, type_, lemm, lenlemm);

        loop {
            norms = LexizeExec(&mut ldata, &mut lexs);
            if !norms.is_null() {
                (*prs).vectorpos += 1;
                addHLParsedLex(prs, query, lexs, norms);
            } else {
                addHLParsedLex(prs, query, lexs, null_mut());
            }
            if norms.is_null() {
                break;
            }
        }

        if !(type_ > 0) {
            break;
        }
    }

    FunctionCall1(&mut (*prsobj).prsend, PointerGetDatum(prsdata));
}

/*
 * Generate the headline, as a text object, from HeadlineParsedText.
 */
pub unsafe fn generateHeadline(prs: *mut HeadlineParsedText) -> *mut text {
    let mut out: *mut text;
    let mut ptr: *mut c_char;
    let mut len: c_int = 128;
    let mut numfragments: c_int = 0;
    let mut infrag: int16 = 0;

    let mut wrd: *mut HeadlineWordEntry = (*prs).words;

    out = palloc(len as usize) as *mut text;
    ptr = (out as *mut c_char).add(VARHDRSZ as usize);

    while (wrd as isize - (*prs).words as isize)
        / (core::mem::size_of::<HeadlineWordEntry>() as isize)
        < (*prs).curwords as isize
    {
        while (*wrd).len() as c_int
            + (*prs).stopsellen as c_int
            + (*prs).startsellen as c_int
            + (*prs).fragdelimlen as c_int
            + (ptr as isize - out as isize) as c_int
            >= len
        {
            let dist = ptr as isize - out as isize;

            len *= 2;
            out = repalloc(out as *mut c_void, len as usize) as *mut text;
            ptr = (out as *mut c_char).offset(dist);
        }

        if (*wrd).r#in() != 0 && (*wrd).repeated() == 0 {
            if infrag == 0 {
                /* start of a new fragment */
                infrag = 1;
                numfragments += 1;
                /* add a fragment delimiter if this is after the first one */
                if numfragments > 1 {
                    core::ptr::copy_nonoverlapping(
                        (*prs).fragdelim,
                        ptr,
                        (*prs).fragdelimlen as usize,
                    );
                    ptr = ptr.add((*prs).fragdelimlen as usize);
                }
            }
            if (*wrd).replace() != 0 {
                *ptr = b' ' as c_char;
                ptr = ptr.add(1);
            } else if (*wrd).skip() == 0 {
                if (*wrd).selected() != 0 {
                    core::ptr::copy_nonoverlapping(
                        (*prs).startsel,
                        ptr,
                        (*prs).startsellen as usize,
                    );
                    ptr = ptr.add((*prs).startsellen as usize);
                }
                core::ptr::copy_nonoverlapping((*wrd).word, ptr, (*wrd).len() as usize);
                ptr = ptr.add((*wrd).len() as usize);
                if (*wrd).selected() != 0 {
                    core::ptr::copy_nonoverlapping(
                        (*prs).stopsel,
                        ptr,
                        (*prs).stopsellen as usize,
                    );
                    ptr = ptr.add((*prs).stopsellen as usize);
                }
            }
        } else if (*wrd).repeated() == 0 {
            if infrag != 0 {
                infrag = 0;
            }
            pfree((*wrd).word as *mut c_void);
        }

        wrd = wrd.add(1);
    }

    SET_VARSIZE(out as *mut c_char, (ptr as isize - out as isize) as int32);
    out
}

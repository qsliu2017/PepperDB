//! src/backend/tsearch/to_tsany.c
//!
//! to_ts* function definitions
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

/*
 * Opaque data structure, which is passed by parse_tsquery() to pushval_morph().
 */
#[repr(C)]
struct MorphOpaque {
    cfg_id: Oid,

    /*
     * Single tsquery morph could be parsed into multiple words.  When these
     * words reside in adjacent positions, they are connected using this
     * operator.  Usually, that is OP_PHRASE, which requires word positions of
     * a complex morph to exactly match the tsvector.
     */
    qoperator: c_int,
}

#[repr(C)]
struct TSVectorBuildState {
    prs: *mut ParsedText,
    cfgId: Oid,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn get_current_ts_config(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_OID!(getTSCurrentConfig(true))
}

/*
 * to_tsvector
 */
unsafe extern "C" fn compareWORD(a: *const c_void, b: *const c_void) -> c_int {
    let a = a as *const ParsedWord;
    let b = b as *const ParsedWord;
    let mut res: c_int;

    res = tsCompareString(
        (*a).word,
        (*a).len,
        (*b).word,
        (*b).len,
        false,
    );

    if res == 0 {
        if (*a).pos.pos == (*b).pos.pos {
            return 0;
        }

        res = if (*a).pos.pos > (*b).pos.pos { 1 } else { -1 };
    }

    res
}

unsafe fn uniqueWORD(a: *mut ParsedWord, l: int32) -> c_int {
    let mut ptr: *mut ParsedWord;
    let mut res: *mut ParsedWord;
    let mut tmppos: c_int;

    if l == 1 {
        tmppos = LIMITPOS!((*a).pos.pos) as c_int;
        (*a).alen = 2;
        (*a).pos.apos = palloc(std::mem::size_of::<uint16>() * (*a).alen as usize) as *mut uint16;
        *(*a).pos.apos.add(0) = 1;
        *(*a).pos.apos.add(1) = tmppos as uint16;
        return l;
    }

    res = a;
    ptr = a.add(1);

    /*
     * Sort words with its positions
     */
    qsort(
        a as *mut c_void,
        l as usize,
        std::mem::size_of::<ParsedWord>(),
        compareWORD,
    );

    /*
     * Initialize first word and its first position
     */
    tmppos = LIMITPOS!((*a).pos.pos) as c_int;
    (*a).alen = 2;
    (*a).pos.apos = palloc(std::mem::size_of::<uint16>() * (*a).alen as usize) as *mut uint16;
    *(*a).pos.apos.add(0) = 1;
    *(*a).pos.apos.add(1) = tmppos as uint16;

    /*
     * Summarize position information for each word
     */
    while ptr.offset_from(a) < l as isize {
        if !((*ptr).len == (*res).len
            && strncmp((*ptr).word, (*res).word, (*res).len as usize) == 0)
        {
            /*
             * Got a new word, so put it in result
             */
            res = res.add(1);
            (*res).len = (*ptr).len;
            (*res).word = (*ptr).word;
            tmppos = LIMITPOS!((*ptr).pos.pos) as c_int;
            (*res).alen = 2;
            (*res).pos.apos =
                palloc(std::mem::size_of::<uint16>() * (*res).alen as usize) as *mut uint16;
            *(*res).pos.apos.add(0) = 1;
            *(*res).pos.apos.add(1) = tmppos as uint16;
        } else {
            /*
             * The word already exists, so adjust position information. But
             * before we should check size of position's array, max allowed
             * value for position and uniqueness of position
             */
            pfree((*ptr).word as *mut c_void);
            let apos0 = *(*res).pos.apos.add(0);
            if apos0 < (MAXNUMPOS - 1) as uint16
                && *(*res).pos.apos.add(apos0 as usize) != (MAXENTRYPOS - 1) as uint16
                && *(*res).pos.apos.add(apos0 as usize) != LIMITPOS!((*ptr).pos.pos) as uint16
            {
                if (*(*res).pos.apos.add(0) + 1) as c_int >= (*res).alen {
                    (*res).alen *= 2;
                    (*res).pos.apos = repalloc(
                        (*res).pos.apos as *mut c_void,
                        std::mem::size_of::<uint16>() * (*res).alen as usize,
                    ) as *mut uint16;
                }
                let apos0 = *(*res).pos.apos.add(0);
                if apos0 == 0
                    || *(*res).pos.apos.add(apos0 as usize) != LIMITPOS!((*ptr).pos.pos) as uint16
                {
                    *(*res).pos.apos.add(apos0 as usize + 1) = LIMITPOS!((*ptr).pos.pos) as uint16;
                    *(*res).pos.apos.add(0) += 1;
                }
            }
        }
        ptr = ptr.add(1);
    }

    (res.offset_from(a) + 1) as c_int
}

/*
 * make value of tsvector, given parsed text
 *
 * Note: frees prs->words and subsidiary data.
 */
pub unsafe fn make_tsvector(prs: *mut ParsedText) -> TSVector {
    let mut i: c_int;
    let mut j: c_int;
    let mut lenstr: c_int = 0;
    let totallen: c_int;
    let r#in: TSVector;
    let mut ptr: *mut WordEntry;
    let str_: *mut c_char;
    let mut stroff: c_int;

    /* Merge duplicate words */
    if (*prs).curwords > 0 {
        (*prs).curwords = uniqueWORD((*prs).words, (*prs).curwords);
    }

    /* Determine space needed */
    i = 0;
    while i < (*prs).curwords {
        let w = (*prs).words.add(i as usize);
        lenstr += (*w).len;
        if (*w).alen != 0 {
            lenstr = SHORTALIGN!(lenstr) as c_int;
            lenstr += std::mem::size_of::<uint16>() as c_int
                + (*(*w).pos.apos.add(0)) as c_int * std::mem::size_of::<WordEntryPos>() as c_int;
        }
        i += 1;
    }

    if lenstr > MAXSTRPOS as c_int {
        ereport!(
            ERROR,
            "string is too long for tsvector"
        );
    }

    totallen = CALCDATASIZE!((*prs).curwords, lenstr) as c_int;
    r#in = palloc0(totallen as usize) as TSVector;
    SET_VARSIZE!(r#in as *mut c_void, totallen as usize);
    (*r#in).size_ = (*prs).curwords as int32;

    ptr = ARRPTR!(r#in);
    str_ = STRPTR!(r#in);
    stroff = 0;
    i = 0;
    while i < (*prs).curwords {
        let w = (*prs).words.add(i as usize);
        (*ptr).set_len((*w).len as u32);
        (*ptr).set_pos(stroff as u32);
        std::ptr::copy_nonoverlapping(
            (*w).word,
            str_.add(stroff as usize),
            (*w).len as usize,
        );
        stroff += (*w).len;
        pfree((*w).word as *mut c_void);
        if (*w).alen != 0 {
            let k = *(*w).pos.apos.add(0) as c_int;
            let wptr: *mut WordEntryPos;

            if k > 0xFFFF {
                elog!(ERROR, "positions array too long");
            }

            (*ptr).set_haspos(1);
            stroff = SHORTALIGN!(stroff) as c_int;
            *(str_.add(stroff as usize) as *mut uint16) = k as uint16;
            wptr = POSDATAPTR!(r#in, ptr);
            j = 0;
            while j < k {
                WEP_SETWEIGHT!(*wptr.add(j as usize), 0);
                WEP_SETPOS!(*wptr.add(j as usize), *(*w).pos.apos.add((j + 1) as usize));
                j += 1;
            }
            stroff += std::mem::size_of::<uint16>() as c_int
                + k * std::mem::size_of::<WordEntryPos>() as c_int;
            pfree((*w).pos.apos as *mut c_void);
        } else {
            (*ptr).set_haspos(0);
        }
        ptr = ptr.add(1);
        i += 1;
    }

    if !(*prs).words.is_null() {
        pfree((*prs).words as *mut c_void);
    }

    r#in
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn to_tsvector_byid(fcinfo: FunctionCallInfo) -> Datum {
    let cfgId: Oid = PG_GETARG_OID!(fcinfo, 0);
    let r#in: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut prs: ParsedText = std::mem::zeroed();
    let out: TSVector;

    prs.lenwords = (VARSIZE_ANY_EXHDR!(r#in) / 6) as c_int; /* just estimation of word's
                                                             * number */
    if prs.lenwords < 2 {
        prs.lenwords = 2;
    } else if prs.lenwords as usize > MaxAllocSize / std::mem::size_of::<ParsedWord>() {
        prs.lenwords = (MaxAllocSize / std::mem::size_of::<ParsedWord>()) as c_int;
    }
    prs.curwords = 0;
    prs.pos = 0;
    prs.words = palloc(std::mem::size_of::<ParsedWord>() * prs.lenwords as usize) as *mut ParsedWord;

    parsetext(
        cfgId,
        &mut prs,
        VARDATA_ANY!(r#in),
        VARSIZE_ANY_EXHDR!(r#in) as c_int,
    );

    PG_FREE_IF_COPY!(r#in, 1);

    out = make_tsvector(&mut prs);

    PG_RETURN_TSVECTOR!(out)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn to_tsvector(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let cfgId: Oid;

    cfgId = getTSCurrentConfig(true);
    PG_RETURN_DATUM!(DirectFunctionCall2(
        to_tsvector_byid,
        ObjectIdGetDatum(cfgId),
        PointerGetDatum(r#in as *const c_void)
    ))
}

/*
 * Worker function for jsonb(_string)_to_tsvector(_byid)
 */
unsafe fn jsonb_to_tsvector_worker(cfgId: Oid, jb: *mut Jsonb, flags: uint32) -> TSVector {
    let mut state: TSVectorBuildState = std::mem::zeroed();
    let mut prs: ParsedText = std::mem::zeroed();

    prs.words = std::ptr::null_mut();
    prs.curwords = 0;
    state.prs = &mut prs;
    state.cfgId = cfgId;

    iterate_jsonb_values(
        jb,
        flags,
        &mut state as *mut _ as *mut c_void,
        add_to_tsvector,
    );

    make_tsvector(&mut prs)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn jsonb_string_to_tsvector_byid(fcinfo: FunctionCallInfo) -> Datum {
    let cfgId: Oid = PG_GETARG_OID!(fcinfo, 0);
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 1);
    let result: TSVector;

    result = jsonb_to_tsvector_worker(cfgId, jb, jtiString);
    PG_FREE_IF_COPY!(jb, 1);

    PG_RETURN_TSVECTOR!(result)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn jsonb_string_to_tsvector(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let cfgId: Oid;
    let result: TSVector;

    cfgId = getTSCurrentConfig(true);
    result = jsonb_to_tsvector_worker(cfgId, jb, jtiString);
    PG_FREE_IF_COPY!(jb, 0);

    PG_RETURN_TSVECTOR!(result)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn jsonb_to_tsvector_byid(fcinfo: FunctionCallInfo) -> Datum {
    let cfgId: Oid = PG_GETARG_OID!(fcinfo, 0);
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 1);
    let jbFlags: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 2);
    let result: TSVector;
    let flags: uint32 = parse_jsonb_index_flags(jbFlags);

    result = jsonb_to_tsvector_worker(cfgId, jb, flags);
    PG_FREE_IF_COPY!(jb, 1);
    PG_FREE_IF_COPY!(jbFlags, 2);

    PG_RETURN_TSVECTOR!(result)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn jsonb_to_tsvector(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 0);
    let jbFlags: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 1);
    let cfgId: Oid;
    let result: TSVector;
    let flags: uint32 = parse_jsonb_index_flags(jbFlags);

    cfgId = getTSCurrentConfig(true);
    result = jsonb_to_tsvector_worker(cfgId, jb, flags);
    PG_FREE_IF_COPY!(jb, 0);
    PG_FREE_IF_COPY!(jbFlags, 1);

    PG_RETURN_TSVECTOR!(result)
}

/*
 * Worker function for json(_string)_to_tsvector(_byid)
 */
unsafe fn json_to_tsvector_worker(cfgId: Oid, json: *mut text, flags: uint32) -> TSVector {
    let mut state: TSVectorBuildState = std::mem::zeroed();
    let mut prs: ParsedText = std::mem::zeroed();

    prs.words = std::ptr::null_mut();
    prs.curwords = 0;
    state.prs = &mut prs;
    state.cfgId = cfgId;

    iterate_json_values(
        json,
        flags,
        &mut state as *mut _ as *mut c_void,
        add_to_tsvector,
    );

    make_tsvector(&mut prs)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn json_string_to_tsvector_byid(fcinfo: FunctionCallInfo) -> Datum {
    let cfgId: Oid = PG_GETARG_OID!(fcinfo, 0);
    let json: *mut text = PG_GETARG_TEXT_P!(fcinfo, 1);
    let result: TSVector;

    result = json_to_tsvector_worker(cfgId, json, jtiString);
    PG_FREE_IF_COPY!(json, 1);

    PG_RETURN_TSVECTOR!(result)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn json_string_to_tsvector(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_P!(fcinfo, 0);
    let cfgId: Oid;
    let result: TSVector;

    cfgId = getTSCurrentConfig(true);
    result = json_to_tsvector_worker(cfgId, json, jtiString);
    PG_FREE_IF_COPY!(json, 0);

    PG_RETURN_TSVECTOR!(result)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn json_to_tsvector_byid(fcinfo: FunctionCallInfo) -> Datum {
    let cfgId: Oid = PG_GETARG_OID!(fcinfo, 0);
    let json: *mut text = PG_GETARG_TEXT_P!(fcinfo, 1);
    let jbFlags: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 2);
    let result: TSVector;
    let flags: uint32 = parse_jsonb_index_flags(jbFlags);

    result = json_to_tsvector_worker(cfgId, json, flags);
    PG_FREE_IF_COPY!(json, 1);
    PG_FREE_IF_COPY!(jbFlags, 2);

    PG_RETURN_TSVECTOR!(result)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn json_to_tsvector(fcinfo: FunctionCallInfo) -> Datum {
    let json: *mut text = PG_GETARG_TEXT_P!(fcinfo, 0);
    let jbFlags: *mut Jsonb = PG_GETARG_JSONB_P!(fcinfo, 1);
    let cfgId: Oid;
    let result: TSVector;
    let flags: uint32 = parse_jsonb_index_flags(jbFlags);

    cfgId = getTSCurrentConfig(true);
    result = json_to_tsvector_worker(cfgId, json, flags);
    PG_FREE_IF_COPY!(json, 0);
    PG_FREE_IF_COPY!(jbFlags, 1);

    PG_RETURN_TSVECTOR!(result)
}

/*
 * Parse lexemes in an element of a json(b) value, add to TSVectorBuildState.
 */
unsafe extern "C" fn add_to_tsvector(_state: *mut c_void, elem_value: *mut c_char, elem_len: c_int) {
    let state = _state as *mut TSVectorBuildState;
    let prs = (*state).prs;
    let prevwords: int32;

    if (*prs).words.is_null() {
        /*
         * First time through: initialize words array to a reasonable size.
         * (parsetext() will realloc it bigger as needed.)
         */
        (*prs).lenwords = 16;
        (*prs).words =
            palloc(std::mem::size_of::<ParsedWord>() * (*prs).lenwords as usize) as *mut ParsedWord;
        (*prs).curwords = 0;
        (*prs).pos = 0;
    }

    prevwords = (*prs).curwords;

    parsetext((*state).cfgId, prs, elem_value, elem_len);

    /*
     * If we extracted any words from this JSON element, advance pos to create
     * an artificial break between elements.  This is because we don't want
     * phrase searches to think that the last word in this element is adjacent
     * to the first word in the next one.
     */
    if (*prs).curwords > prevwords {
        (*prs).pos += 1;
    }
}

/*
 * to_tsquery
 */

/*
 * This function is used for morph parsing.
 *
 * The value is passed to parsetext which will call the right dictionary to
 * lexize the word. If it turns out to be a stopword, we push a QI_VALSTOP
 * to the stack.
 *
 * All words belonging to the same variant are pushed as an ANDed list,
 * and different variants are ORed together.
 */
unsafe extern "C" fn pushval_morph(
    opaque: Datum,
    state: TSQueryParserState,
    strval: *mut c_char,
    lenval: c_int,
    weight: int16,
    prefix: bool,
) {
    let mut count: int32 = 0;
    let mut prs: ParsedText = std::mem::zeroed();
    let mut variant: uint32;
    let mut pos: uint32 = 0;
    let mut cntvar: uint32;
    let mut cntpos: uint32 = 0;
    let mut cnt: uint32;
    let data = DatumGetPointer(opaque) as *mut MorphOpaque;

    prs.lenwords = 4;
    prs.curwords = 0;
    prs.pos = 0;
    prs.words = palloc(std::mem::size_of::<ParsedWord>() * prs.lenwords as usize) as *mut ParsedWord;

    parsetext((*data).cfg_id, &mut prs, strval, lenval);

    if prs.curwords > 0 {
        while count < prs.curwords {
            /*
             * Were any stop words removed? If so, fill empty positions with
             * placeholders linked by an appropriate operator.
             */
            if pos > 0 && pos + 1 < (*prs.words.add(count as usize)).pos.pos as uint32 {
                while pos + 1 < (*prs.words.add(count as usize)).pos.pos as uint32 {
                    /* put placeholders for each missing stop word */
                    pushStop(state);
                    if cntpos != 0 {
                        pushOperator(state, (*data).qoperator, 1);
                    }
                    cntpos += 1;
                    pos += 1;
                }
            }

            /* save current word's position */
            pos = (*prs.words.add(count as usize)).pos.pos as uint32;

            /* Go through all variants obtained from this token */
            cntvar = 0;
            while count < prs.curwords && pos == (*prs.words.add(count as usize)).pos.pos as uint32 {
                variant = (*prs.words.add(count as usize)).nvariant as uint32;

                /* Push all words belonging to the same variant */
                cnt = 0;
                while count < prs.curwords
                    && pos == (*prs.words.add(count as usize)).pos.pos as uint32
                    && variant == (*prs.words.add(count as usize)).nvariant as uint32
                {
                    let w = prs.words.add(count as usize);
                    pushValue(
                        state,
                        (*w).word,
                        (*w).len,
                        weight,
                        ((*w).flags as c_int & TSL_PREFIX) != 0 || prefix,
                    );
                    pfree((*w).word as *mut c_void);
                    if cnt != 0 {
                        pushOperator(state, OP_AND, 0);
                    }
                    cnt += 1;
                    count += 1;
                }

                if cntvar != 0 {
                    pushOperator(state, OP_OR, 0);
                }
                cntvar += 1;
            }

            if cntpos != 0 {
                /* distance may be useful */
                pushOperator(state, (*data).qoperator, 1);
            }

            cntpos += 1;
        }

        pfree(prs.words as *mut c_void);
    } else {
        pushStop(state);
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn to_tsquery_byid(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let query: TSQuery;
    let mut data: MorphOpaque = std::mem::zeroed();

    data.cfg_id = PG_GETARG_OID!(fcinfo, 0);

    /*
     * Passing OP_PHRASE as a qoperator makes tsquery require matching of word
     * positions of a complex morph exactly match the tsvector.  Also, when
     * the complex morphs are connected with OP_PHRASE operator, we connect
     * all their words into the OP_PHRASE sequence.
     */
    data.qoperator = OP_PHRASE;

    query = parse_tsquery(
        text_to_cstring(r#in),
        pushval_morph,
        PointerGetDatum(&mut data as *mut _ as *const c_void),
        0,
        std::ptr::null_mut(),
    );

    PG_RETURN_TSQUERY!(query)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn to_tsquery(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let cfgId: Oid;

    cfgId = getTSCurrentConfig(true);
    PG_RETURN_DATUM!(DirectFunctionCall2(
        to_tsquery_byid,
        ObjectIdGetDatum(cfgId),
        PointerGetDatum(r#in as *const c_void)
    ))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn plainto_tsquery_byid(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let query: TSQuery;
    let mut data: MorphOpaque = std::mem::zeroed();

    data.cfg_id = PG_GETARG_OID!(fcinfo, 0);

    /*
     * parse_tsquery() with P_TSQ_PLAIN flag takes the whole input text as a
     * single morph.  Passing OP_PHRASE as a qoperator makes tsquery require
     * matching of all words independently on their positions.
     */
    data.qoperator = OP_AND;

    query = parse_tsquery(
        text_to_cstring(r#in),
        pushval_morph,
        PointerGetDatum(&mut data as *mut _ as *const c_void),
        P_TSQ_PLAIN,
        std::ptr::null_mut(),
    );

    PG_RETURN_POINTER!(query as *const c_void)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn plainto_tsquery(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let cfgId: Oid;

    cfgId = getTSCurrentConfig(true);
    PG_RETURN_DATUM!(DirectFunctionCall2(
        plainto_tsquery_byid,
        ObjectIdGetDatum(cfgId),
        PointerGetDatum(r#in as *const c_void)
    ))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phraseto_tsquery_byid(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let query: TSQuery;
    let mut data: MorphOpaque = std::mem::zeroed();

    data.cfg_id = PG_GETARG_OID!(fcinfo, 0);

    /*
     * parse_tsquery() with P_TSQ_PLAIN flag takes the whole input text as a
     * single morph.  Passing OP_PHRASE as a qoperator makes tsquery require
     * matching of word positions.
     */
    data.qoperator = OP_PHRASE;

    query = parse_tsquery(
        text_to_cstring(r#in),
        pushval_morph,
        PointerGetDatum(&mut data as *mut _ as *const c_void),
        P_TSQ_PLAIN,
        std::ptr::null_mut(),
    );

    PG_RETURN_TSQUERY!(query)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn phraseto_tsquery(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let cfgId: Oid;

    cfgId = getTSCurrentConfig(true);
    PG_RETURN_DATUM!(DirectFunctionCall2(
        phraseto_tsquery_byid,
        ObjectIdGetDatum(cfgId),
        PointerGetDatum(r#in as *const c_void)
    ))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn websearch_to_tsquery_byid(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut data: MorphOpaque = std::mem::zeroed();
    let query: TSQuery;

    data.cfg_id = PG_GETARG_OID!(fcinfo, 0);

    /*
     * Passing OP_PHRASE as a qoperator makes tsquery require matching of word
     * positions of a complex morph exactly match the tsvector.  Also, when
     * the complex morphs are given in quotes, we connect all their words into
     * the OP_PHRASE sequence.
     */
    data.qoperator = OP_PHRASE;

    query = parse_tsquery(
        text_to_cstring(r#in),
        pushval_morph,
        PointerGetDatum(&mut data as *mut _ as *const c_void),
        P_TSQ_WEB,
        std::ptr::null_mut(),
    );

    PG_RETURN_TSQUERY!(query)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn websearch_to_tsquery(fcinfo: FunctionCallInfo) -> Datum {
    let r#in: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let cfgId: Oid;

    cfgId = getTSCurrentConfig(true);
    PG_RETURN_DATUM!(DirectFunctionCall2(
        websearch_to_tsquery_byid,
        ObjectIdGetDatum(cfgId),
        PointerGetDatum(r#in as *const c_void)
    ))
}

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies
// ---------------------------------------------------------------------------

type text = crate::c::c_void; // text varlena; treated as opaque
type Jsonb = crate::c::c_void;
type TSVector = *mut c_void;
type TSQuery = *mut c_void;
type TSQueryParserState = *mut c_void;
type FunctionCallInfo = *mut c_void;

#[repr(C)]
struct ParsedWordPos {
    pos: uint16,
}

#[repr(C)]
union ParsedWordPosUnion {
    pos: int32,
    apos: *mut uint16,
}

#[repr(C)]
struct ParsedWord {
    len: int32,
    nvariant: int32,
    flags: uint16,
    alen: int32,
    word: *mut c_char,
    pos: ParsedWordPosUnion,
}

#[repr(C)]
struct ParsedText {
    lenwords: c_int,
    curwords: int32,
    pos: int32,
    words: *mut ParsedWord,
}

#[repr(C)]
struct WordEntry {
    bits: u32, // bitfield: haspos:1, len:11, pos:20
}

impl WordEntry {
    unsafe fn set_haspos(&mut self, _v: u32) {
        unimplemented!() // TODO: src/include/tsearch/ts_type.h
    }
    unsafe fn set_len(&mut self, _v: u32) {
        unimplemented!() // TODO: src/include/tsearch/ts_type.h
    }
    unsafe fn set_pos(&mut self, _v: u32) {
        unimplemented!() // TODO: src/include/tsearch/ts_type.h
    }
}

type WordEntryPos = uint16;

const MAXNUMPOS: c_int = 256;
const MAXENTRYPOS: c_int = 1 << 14;
const MAXSTRPOS: c_int = (1 << 20) - 1;
const TSL_PREFIX: c_int = 0x04;
const OP_AND: int8 = 2;
const OP_OR: int8 = 3;
const OP_PHRASE: int8 = 4;
const P_TSQ_PLAIN: c_int = 0x0001;
const P_TSQ_WEB: c_int = 0x0002;
const jtiString: uint32 = 0x01;

#[allow(non_camel_case_types)]
type int8 = c_int;

unsafe fn LIMITPOS(p: int32) -> int32 {
    if p >= MAXENTRYPOS { MAXENTRYPOS - 1 } else { p }
}
#[macro_export]
macro_rules! LIMITPOS {
    ($p:expr) => {{ if ($p as i32) >= ((1i32 << 14) - 1) { (1i32 << 14) - 1 } else { $p as i32 } }};
}
pub use LIMITPOS;

#[macro_export]
macro_rules! SHORTALIGN {
    ($len:expr) => {{ (($len as usize) + 1) & !1usize }};
}
pub use SHORTALIGN;

#[macro_export]
macro_rules! CALCDATASIZE {
    ($nentries:expr, $lenstr:expr) => {{ unimplemented!() }};
}
pub use CALCDATASIZE;

#[macro_export]
macro_rules! ARRPTR {
    ($x:expr) => {{ unimplemented!() }};
}
pub use ARRPTR;

#[macro_export]
macro_rules! STRPTR {
    ($x:expr) => {{ unimplemented!() }};
}
pub use STRPTR;

#[macro_export]
macro_rules! POSDATAPTR {
    ($x:expr, $e:expr) => {{ unimplemented!() }};
}
pub use POSDATAPTR;

#[macro_export]
macro_rules! WEP_SETWEIGHT {
    ($x:expr, $v:expr) => {{ let _ = ($v); }};
}
pub use WEP_SETWEIGHT;

#[macro_export]
macro_rules! WEP_SETPOS {
    ($x:expr, $v:expr) => {{ let _ = ($v); }};
}
pub use WEP_SETPOS;

unsafe fn getTSCurrentConfig(_emitError: bool) -> Oid {
    unimplemented!() // TODO: src/backend/utils/cache/ts_cache.c
}
unsafe fn tsCompareString(
    _a: *mut c_char,
    _lena: int32,
    _b: *mut c_char,
    _lenb: int32,
    _prefix: bool,
) -> c_int {
    unimplemented!() // TODO: src/backend/utils/adt/tsvector_op.c
}
unsafe fn parsetext(_cfgId: Oid, _prs: *mut ParsedText, _buf: *mut c_char, _buflen: c_int) {
    unimplemented!() // TODO: src/backend/tsearch/ts_parse.c
}
unsafe fn parse_tsquery(
    _buf: *mut c_char,
    _pushval: unsafe extern "C" fn(Datum, TSQueryParserState, *mut c_char, c_int, int16, bool),
    _opaque: Datum,
    _flags: c_int,
    _escontext: *mut c_void,
) -> TSQuery {
    unimplemented!() // TODO: src/backend/utils/adt/tsquery.c
}
unsafe fn pushValue(
    _state: TSQueryParserState,
    _strval: *mut c_char,
    _lenval: c_int,
    _weight: int16,
    _prefix: bool,
) {
    unimplemented!() // TODO: src/backend/utils/adt/tsquery.c
}
unsafe fn pushStop(_state: TSQueryParserState) {
    unimplemented!() // TODO: src/backend/utils/adt/tsquery.c
}
unsafe fn pushOperator(_state: TSQueryParserState, _oper: int8, _distance: int16) {
    unimplemented!() // TODO: src/backend/utils/adt/tsquery.c
}
unsafe fn text_to_cstring(_t: *const text) -> *mut c_char {
    unimplemented!() // TODO: src/backend/utils/adt/varlena.c
}
unsafe fn iterate_jsonb_values(
    _jb: *mut Jsonb,
    _flags: uint32,
    _state: *mut c_void,
    _action: unsafe extern "C" fn(*mut c_void, *mut c_char, c_int),
) {
    unimplemented!() // TODO: src/backend/utils/adt/jsonfuncs.c
}
unsafe fn iterate_json_values(
    _json: *mut text,
    _flags: uint32,
    _state: *mut c_void,
    _action: unsafe extern "C" fn(*mut c_void, *mut c_char, c_int),
) {
    unimplemented!() // TODO: src/backend/utils/adt/jsonfuncs.c
}
unsafe fn parse_jsonb_index_flags(_jb: *mut Jsonb) -> uint32 {
    unimplemented!() // TODO: src/backend/utils/adt/jsonfuncs.c
}
unsafe fn DirectFunctionCall2(
    _func: unsafe extern "C" fn(FunctionCallInfo) -> Datum,
    _arg1: Datum,
    _arg2: Datum,
) -> Datum {
    unimplemented!() // TODO: src/backend/utils/fmgr/fmgr.c
}
unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!() // TODO: src/include/postgres.h
}
unsafe fn PointerGetDatum(_p: *const c_void) -> Datum {
    unimplemented!() // TODO: src/include/postgres.h
}
unsafe fn DatumGetPointer(_d: Datum) -> *mut c_void {
    unimplemented!() // TODO: src/include/postgres.h
}
unsafe fn qsort(
    _base: *mut c_void,
    _nmemb: usize,
    _size: usize,
    _compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
) {
    unimplemented!() // TODO: libc qsort
}

extern "C" {
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
}

const MaxAllocSize: usize = 0x3fffffff;

#[macro_export]
macro_rules! PG_GETARG_OID {
    ($f:expr, $n:expr) => {{ unimplemented!() }};
}
pub use PG_GETARG_OID;

#[macro_export]
macro_rules! PG_GETARG_TEXT_PP {
    ($f:expr, $n:expr) => {{ unimplemented!() }};
}
pub use PG_GETARG_TEXT_PP;

#[macro_export]
macro_rules! PG_GETARG_TEXT_P {
    ($f:expr, $n:expr) => {{ unimplemented!() }};
}
pub use PG_GETARG_TEXT_P;

#[macro_export]
macro_rules! PG_GETARG_JSONB_P {
    ($f:expr, $n:expr) => {{ unimplemented!() }};
}
pub use PG_GETARG_JSONB_P;

#[macro_export]
macro_rules! PG_FREE_IF_COPY {
    ($p:expr, $n:expr) => {{ let _ = &$p; }};
}
pub use PG_FREE_IF_COPY;

#[macro_export]
macro_rules! PG_RETURN_OID {
    ($x:expr) => {{ unimplemented!() }};
}
pub use PG_RETURN_OID;

#[macro_export]
macro_rules! PG_RETURN_TSVECTOR {
    ($x:expr) => {{ unimplemented!() }};
}
pub use PG_RETURN_TSVECTOR;

#[macro_export]
macro_rules! PG_RETURN_TSQUERY {
    ($x:expr) => {{ unimplemented!() }};
}
pub use PG_RETURN_TSQUERY;

#[macro_export]
macro_rules! PG_RETURN_POINTER {
    ($x:expr) => {{ unimplemented!() }};
}
pub use PG_RETURN_POINTER;

#[macro_export]
macro_rules! PG_RETURN_DATUM {
    ($x:expr) => {{ $x }};
}
pub use PG_RETURN_DATUM;

#[macro_export]
macro_rules! VARSIZE_ANY_EXHDR {
    ($p:expr) => {{ unimplemented!() }};
}
pub use VARSIZE_ANY_EXHDR;

#[macro_export]
macro_rules! VARDATA_ANY {
    ($p:expr) => {{ unimplemented!() }};
}
pub use VARDATA_ANY;

#[macro_export]
macro_rules! SET_VARSIZE {
    ($p:expr, $len:expr) => {{ let _ = ($len); }};
}
pub use SET_VARSIZE;

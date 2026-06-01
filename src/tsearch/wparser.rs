//! wparser.rs
//!   Standard interface to word parser
//!
//! Translated 1:1 from postgres/src/backend/tsearch/wparser.c
//!
//! wparser.c
//!		Standard interface to word parser
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//!
//! IDENTIFICATION
//!	  src/backend/tsearch/wparser.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::{
    DirectFunctionCall3, FunctionCall1, FunctionCall2, OidFunctionCall1, PG_FREE_IF_COPY,
    PG_GETARG_DATUM, PG_GETARG_OID, PG_GETARG_POINTER, PG_NARGS, PG_RETURN_DATUM, PG_RETURN_POINTER,
};

// fmgr.h has no crate-root macros for the 4-arg DirectFunctionCall or the 3-arg
// FunctionCall; call the *Coll helpers directly the way those macros would expand.
use crate::utils::fmgr::{DirectFunctionCall4Coll, FunctionCall3Coll};

use crate::c::{int32, text};
use crate::nodes::pg_list::{List, NIL};
use crate::tsearch::ts_parse::{generateHeadline, hlparsetext};
use crate::tsearch::ts_public::{HeadlineParsedText, HeadlineWordEntry, LexDescr};
use crate::utils::adt::ts_type::TSQuery;
use crate::utils::cache::ts_cache::{
    getTSCurrentConfig, lookup_ts_config_cache, lookup_ts_parser_cache, TSConfigCacheEntry,
    TSParserCacheEntry,
};
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::palloc::MemoryContext;
use crate::varatt::{VARDATA_ANY, VARSIZE_ANY_EXHDR};

extern "C" {
    fn sprintf(s: *mut c_char, fmt: *const c_char, ...) -> c_int;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// fmgr.h: DirectFunctionCall4(func, arg1..arg4) and FunctionCall3(flinfo, arg1..arg3).
// The crate root only exports the lower-arity variants, so expand these locally
// exactly as the fmgr macros do (collation defaulting to InvalidOid).
macro_rules! DirectFunctionCall4 {
    ($func:expr, $a1:expr, $a2:expr, $a3:expr, $a4:expr) => {
        DirectFunctionCall4Coll($func, crate::postgres_ext::InvalidOid, $a1, $a2, $a3, $a4)
    };
}
macro_rules! FunctionCall3 {
    ($flinfo:expr, $a1:expr, $a2:expr, $a3:expr) => {
        FunctionCall3Coll($flinfo, crate::postgres_ext::InvalidOid, $a1, $a2, $a3)
    };
}

/* ---------------------------------------------------------------------------
 * funcapi.h / funcapi.c -- set-returning-function support machinery.
 * Not yet ported; stubbed locally so this unit translates 1:1, matching the
 * convention used by utils/adt/lockfuncs.rs and utils/adt/partitionfuncs.rs.
 * --------------------------------------------------------------------------- */

// funcapi.h: cross-call persistence context for set-returning functions.
#[repr(C)]
pub struct FuncCallContext {
    pub call_cntr: u64,
    pub max_calls: u64,
    pub user_fctx: *mut c_void,
    pub attinmeta: *mut c_void,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: TupleDesc,
}

pub type TupleDesc = *mut c_void;
pub type HeapTuple = *mut c_void;

// funcapi.h: TypeFuncClass (only TYPEFUNC_COMPOSITE used here)
const TYPEFUNC_COMPOSITE: c_int = 1;

// funcapi.h: SRF_IS_FIRSTCALL()
unsafe fn SRF_IS_FIRSTCALL() -> bool {
    unimplemented!() // TODO(pg-port): funcapi.h
}
// funcapi.h: SRF_FIRSTCALL_INIT()
unsafe fn SRF_FIRSTCALL_INIT() -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): funcapi.h
}
// funcapi.h: SRF_PERCALL_SETUP()
unsafe fn SRF_PERCALL_SETUP() -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): funcapi.h
}
// funcapi.h: SRF_RETURN_NEXT(funcctx, result)
unsafe fn SRF_RETURN_NEXT(_funcctx: *mut FuncCallContext, _result: Datum) -> Datum {
    unimplemented!() // TODO(pg-port): funcapi.h
}
// funcapi.h: SRF_RETURN_DONE(funcctx)
unsafe fn SRF_RETURN_DONE(_funcctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO(pg-port): funcapi.h
}

// funcapi.c: get_call_result_type(fcinfo, resultTypeId, resultTupleDesc)
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> c_int {
    unimplemented!() // TODO(pg-port): funcapi.c
}
// funcapi.c: TupleDescGetAttInMetadata(tupdesc)
unsafe fn TupleDescGetAttInMetadata(_tupdesc: TupleDesc) -> *mut c_void {
    unimplemented!() // TODO(pg-port): funcapi.c
}
// execTuples.c: BuildTupleFromCStrings(attinmeta, values)
unsafe fn BuildTupleFromCStrings(_attinmeta: *mut c_void, _values: *mut *mut c_char) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/common/execTuples.c
}
// funcapi.h: HeapTupleGetDatum(tuple)
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!() // TODO(pg-port): funcapi.h
}

/* ---------------------------------------------------------------------------
 * Other not-yet-ported symbols referenced by wparser.c.
 * --------------------------------------------------------------------------- */

// catalog/namespace.h: get_ts_parser_oid()
// TODO(pg-port): real get_ts_parser_oid lives in catalog/namespace.c
unsafe fn get_ts_parser_oid(_names: *mut List, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO(pg-port): catalog/namespace.c
}

// utils/varlena.h: textToQualifiedNameList()
// TODO(pg-port): real textToQualifiedNameList lives in utils/adt/varlena.c
unsafe fn textToQualifiedNameList(_textval: *mut text) -> *mut List {
    unimplemented!() // TODO(pg-port): utils/adt/varlena.c
}

// commands/defrem.h: deserialize_deflist()
// TODO(pg-port): real deserialize_deflist lives in commands/define.c (commands/defrem.rs)
unsafe fn deserialize_deflist(_txt: Datum) -> *mut List {
    unimplemented!() // TODO(pg-port): commands/define.c
}

// Jsonb opaque type (utils/jsonb.h) -- not yet ported.
// TODO(pg-port): real Jsonb lives in utils/adt/jsonb.h
#[repr(C)]
pub struct Jsonb {
    _opaque: [u8; 0],
}

// utils/jsonfuncs.h: JsonTransformStringValuesAction
pub type JsonTransformStringValuesAction =
    Option<unsafe extern "C" fn(state: *mut c_void, elem_value: *mut c_char, elem_len: c_int) -> *mut text>;

// utils/jsonfuncs.h: transform_json_string_values()
// TODO(pg-port): real transform_json_string_values lives in utils/adt/jsonfuncs.c
unsafe fn transform_json_string_values(
    _json: *mut text,
    _action_state: *mut c_void,
    _transform_action: JsonTransformStringValuesAction,
) -> *mut text {
    unimplemented!() // TODO(pg-port): utils/adt/jsonfuncs.c
}

// utils/jsonfuncs.h: transform_jsonb_string_values()
// TODO(pg-port): real transform_jsonb_string_values lives in utils/adt/jsonfuncs.c
unsafe fn transform_jsonb_string_values(
    _jsonb: *mut Jsonb,
    _action_state: *mut c_void,
    _transform_action: JsonTransformStringValuesAction,
) -> *mut Jsonb {
    unimplemented!() // TODO(pg-port): utils/adt/jsonfuncs.c
}

// PG_GETARG_TSQUERY(n): de-toast the arg datum to a TSQuery.
// TODO(pg-port): real PG_GETARG_TSQUERY macro lives in utils/adt/ts_type.h
unsafe fn PG_GETARG_TSQUERY(_fcinfo: FunctionCallInfo, _n: c_int) -> TSQuery {
    unimplemented!() // TODO(pg-port): utils/adt/ts_type.h
}

// PG_GETARG_JSONB_P(n): de-toast the arg datum to a Jsonb.
// TODO(pg-port): real PG_GETARG_JSONB_P macro lives in utils/jsonb.h
unsafe fn PG_GETARG_JSONB_P(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut Jsonb {
    unimplemented!() // TODO(pg-port): utils/jsonb.h
}

// PG_GETARG_TEXT_PP(n): de-toast (without unpacking the short header) the text arg.
// TODO(pg-port): real PG_GETARG_TEXT_PP macro lives in fmgr.h
unsafe fn PG_GETARG_TEXT_PP(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut text {
    unimplemented!() // TODO(pg-port): fmgr.h
}

// PG_GETARG_TEXT_P(n): de-toast (and unpack) the text arg.
// TODO(pg-port): real PG_GETARG_TEXT_P macro lives in fmgr.h
unsafe fn PG_GETARG_TEXT_P(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut text {
    unimplemented!() // TODO(pg-port): fmgr.h
}

// PG_RETURN_TEXT_P(x) -- text result. (local stub; fmgr macro not visible here)
unsafe fn PG_RETURN_TEXT_P(x: *mut text) -> Datum {
    PointerGetDatum(x as *const c_void)
}

// PG_RETURN_JSONB_P(x) -- jsonb result.
// TODO(pg-port): real PG_RETURN_JSONB_P macro lives in utils/jsonb.h
unsafe fn PG_RETURN_JSONB_P(x: *mut Jsonb) -> Datum {
    PointerGetDatum(x as *const c_void)
}

/******sql-level interface******/

#[repr(C)]
struct TSTokenTypeStorage {
    cur: c_int,
    list: *mut LexDescr,
}

/* state for ts_headline_json_* */
#[repr(C)]
struct HeadlineJsonState {
    prs: *mut HeadlineParsedText,
    cfg: *mut TSConfigCacheEntry,
    prsobj: *mut TSParserCacheEntry,
    query: TSQuery,
    prsoptions: *mut List,
    transformed: bool,
}

unsafe fn tt_setup_firstcall(funcctx: *mut FuncCallContext, fcinfo: FunctionCallInfo, prsid: Oid) {
    let mut tupdesc: TupleDesc = std::ptr::null_mut();
    let oldcontext: MemoryContext;
    let st: *mut TSTokenTypeStorage;
    let prs: *mut TSParserCacheEntry = lookup_ts_parser_cache(prsid);

    if !OidIsValid((*prs).lextypeOid) {
        elog!(
            ERROR,
            "method lextype isn't defined for text search parser {}",
            prsid
        );
    }

    oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

    st = palloc(std::mem::size_of::<TSTokenTypeStorage>()) as *mut TSTokenTypeStorage;
    (*st).cur = 0;
    /* lextype takes one dummy argument */
    (*st).list = DatumGetPointer(OidFunctionCall1!((*prs).lextypeOid, 0 as Datum)) as *mut LexDescr;
    (*funcctx).user_fctx = st as *mut c_void;

    if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }
    (*funcctx).tuple_desc = tupdesc;
    (*funcctx).attinmeta = TupleDescGetAttInMetadata(tupdesc);

    MemoryContextSwitchTo(oldcontext);
}

unsafe fn tt_process_call(funcctx: *mut FuncCallContext) -> Datum {
    let st: *mut TSTokenTypeStorage;

    st = (*funcctx).user_fctx as *mut TSTokenTypeStorage;
    if !(*st).list.is_null() && (*(*st).list.offset((*st).cur as isize)).lexid != 0 {
        let result: Datum;
        let mut values: [*mut c_char; 3] = [std::ptr::null_mut(); 3];
        let mut txtid: [c_char; 16] = [0; 16];
        let tuple: HeapTuple;

        sprintf(
            txtid.as_mut_ptr(),
            c"%d".as_ptr(),
            (*(*st).list.offset((*st).cur as isize)).lexid,
        );
        values[0] = txtid.as_mut_ptr();
        values[1] = (*(*st).list.offset((*st).cur as isize)).alias;
        values[2] = (*(*st).list.offset((*st).cur as isize)).descr;

        tuple = BuildTupleFromCStrings((*funcctx).attinmeta, values.as_mut_ptr());
        result = HeapTupleGetDatum(tuple);

        pfree(values[1] as *mut c_void);
        pfree(values[2] as *mut c_void);
        (*st).cur += 1;
        return result;
    }
    0 as Datum
}

pub unsafe fn ts_token_type_byid(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let result: Datum;

    if SRF_IS_FIRSTCALL() {
        funcctx = SRF_FIRSTCALL_INIT();
        tt_setup_firstcall(funcctx, fcinfo, PG_GETARG_OID!(fcinfo, 0));
    }

    funcctx = SRF_PERCALL_SETUP();

    result = tt_process_call(funcctx);
    if result != 0 as Datum {
        return SRF_RETURN_NEXT(funcctx, result);
    }
    SRF_RETURN_DONE(funcctx)
}

pub unsafe fn ts_token_type_byname(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let result: Datum;

    if SRF_IS_FIRSTCALL() {
        let prsname: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
        let prsId: Oid;

        funcctx = SRF_FIRSTCALL_INIT();
        prsId = get_ts_parser_oid(textToQualifiedNameList(prsname), false);
        tt_setup_firstcall(funcctx, fcinfo, prsId);
    }

    funcctx = SRF_PERCALL_SETUP();

    result = tt_process_call(funcctx);
    if result != 0 as Datum {
        return SRF_RETURN_NEXT(funcctx, result);
    }
    SRF_RETURN_DONE(funcctx)
}

#[repr(C)]
struct LexemeEntry {
    r#type: c_int,
    lexeme: *mut c_char,
}

#[repr(C)]
struct PrsStorage {
    cur: c_int,
    len: c_int,
    list: *mut LexemeEntry,
}

unsafe fn prs_setup_firstcall(
    funcctx: *mut FuncCallContext,
    fcinfo: FunctionCallInfo,
    prsid: Oid,
    txt: *mut text,
) {
    let mut tupdesc: TupleDesc = std::ptr::null_mut();
    let oldcontext: MemoryContext;
    let st: *mut PrsStorage;
    let prs: *mut TSParserCacheEntry = lookup_ts_parser_cache(prsid);
    let mut lex: *mut c_char = std::ptr::null_mut();
    let mut llen: c_int = 0;
    let mut r#type: c_int;
    let prsdata: *mut c_void;

    oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

    st = palloc(std::mem::size_of::<PrsStorage>()) as *mut PrsStorage;
    (*st).cur = 0;
    (*st).len = 16;
    (*st).list =
        palloc(std::mem::size_of::<LexemeEntry>() * (*st).len as usize) as *mut LexemeEntry;

    prsdata = DatumGetPointer(FunctionCall2!(
        &raw mut (*prs).prsstart,
        PointerGetDatum(VARDATA_ANY(txt as *const c_char) as *const c_void),
        Int32GetDatum(VARSIZE_ANY_EXHDR(txt as *const c_char) as int32)
    )) as *mut c_void;

    loop {
        r#type = DatumGetInt32(FunctionCall3!(
            &raw mut (*prs).prstoken,
            PointerGetDatum(prsdata as *const c_void),
            PointerGetDatum(&raw mut lex as *const c_void),
            PointerGetDatum(&raw mut llen as *const c_void)
        ));
        if r#type == 0 {
            break;
        }

        if (*st).cur >= (*st).len {
            (*st).len = 2 * (*st).len;
            (*st).list = repalloc(
                (*st).list as *mut c_void,
                std::mem::size_of::<LexemeEntry>() * (*st).len as usize,
            ) as *mut LexemeEntry;
        }
        (*(*st).list.offset((*st).cur as isize)).lexeme = palloc((llen + 1) as usize) as *mut c_char;
        memcpy(
            (*(*st).list.offset((*st).cur as isize)).lexeme as *mut c_void,
            lex as *const c_void,
            llen as usize,
        );
        *(*(*st).list.offset((*st).cur as isize))
            .lexeme
            .offset(llen as isize) = b'\0' as c_char;
        (*(*st).list.offset((*st).cur as isize)).r#type = r#type;
        (*st).cur += 1;
    }

    FunctionCall1!(&raw mut (*prs).prsend, PointerGetDatum(prsdata as *const c_void));

    (*st).len = (*st).cur;
    (*st).cur = 0;

    (*funcctx).user_fctx = st as *mut c_void;
    if get_call_result_type(fcinfo, std::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }
    (*funcctx).tuple_desc = tupdesc;
    (*funcctx).attinmeta = TupleDescGetAttInMetadata(tupdesc);
    MemoryContextSwitchTo(oldcontext);
}

unsafe fn prs_process_call(funcctx: *mut FuncCallContext) -> Datum {
    let st: *mut PrsStorage;

    st = (*funcctx).user_fctx as *mut PrsStorage;
    if (*st).cur < (*st).len {
        let result: Datum;
        let mut values: [*mut c_char; 2] = [std::ptr::null_mut(); 2];
        let mut tid: [c_char; 16] = [0; 16];
        let tuple: HeapTuple;

        values[0] = tid.as_mut_ptr();
        sprintf(
            tid.as_mut_ptr(),
            c"%d".as_ptr(),
            (*(*st).list.offset((*st).cur as isize)).r#type,
        );
        values[1] = (*(*st).list.offset((*st).cur as isize)).lexeme;
        tuple = BuildTupleFromCStrings((*funcctx).attinmeta, values.as_mut_ptr());
        result = HeapTupleGetDatum(tuple);

        pfree(values[1] as *mut c_void);
        (*st).cur += 1;
        return result;
    }
    0 as Datum
}

pub unsafe fn ts_parse_byid(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let result: Datum;

    if SRF_IS_FIRSTCALL() {
        let txt: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);

        funcctx = SRF_FIRSTCALL_INIT();
        prs_setup_firstcall(funcctx, fcinfo, PG_GETARG_OID!(fcinfo, 0), txt);
        PG_FREE_IF_COPY!(fcinfo, txt, 1);
    }

    funcctx = SRF_PERCALL_SETUP();

    result = prs_process_call(funcctx);
    if result != 0 as Datum {
        return SRF_RETURN_NEXT(funcctx, result);
    }
    SRF_RETURN_DONE(funcctx)
}

pub unsafe fn ts_parse_byname(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let result: Datum;

    if SRF_IS_FIRSTCALL() {
        let prsname: *mut text = PG_GETARG_TEXT_PP(fcinfo, 0);
        let txt: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
        let prsId: Oid;

        funcctx = SRF_FIRSTCALL_INIT();
        prsId = get_ts_parser_oid(textToQualifiedNameList(prsname), false);
        prs_setup_firstcall(funcctx, fcinfo, prsId, txt);
    }

    funcctx = SRF_PERCALL_SETUP();

    result = prs_process_call(funcctx);
    if result != 0 as Datum {
        return SRF_RETURN_NEXT(funcctx, result);
    }
    SRF_RETURN_DONE(funcctx)
}

pub unsafe fn ts_headline_byid_opt(fcinfo: FunctionCallInfo) -> Datum {
    let tsconfig: Oid = PG_GETARG_OID!(fcinfo, 0);
    let r#in: *mut text = PG_GETARG_TEXT_PP(fcinfo, 1);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 2);
    let opt: *mut text =
        if (PG_NARGS!(fcinfo) as c_int) > 3 && !PG_GETARG_POINTER!(fcinfo, 3).is_null() {
            PG_GETARG_TEXT_PP(fcinfo, 3)
        } else {
            std::ptr::null_mut()
        };
    let mut prs: HeadlineParsedText = std::mem::zeroed();
    let prsoptions: *mut List;
    let out: *mut text;
    let cfg: *mut TSConfigCacheEntry;
    let prsobj: *mut TSParserCacheEntry;

    cfg = lookup_ts_config_cache(tsconfig);
    prsobj = lookup_ts_parser_cache((*cfg).prsId);

    if !OidIsValid((*prsobj).headlineOid) {
        ereport!(
            ERROR,
            errmsg!("text search parser does not support headline creation")
        );
    }

    memset(
        &raw mut prs as *mut c_void,
        0,
        std::mem::size_of::<HeadlineParsedText>(),
    );
    prs.lenwords = 32;
    prs.words = palloc(std::mem::size_of::<HeadlineWordEntry>() * prs.lenwords as usize)
        as *mut HeadlineWordEntry;

    hlparsetext(
        (*cfg).cfgId,
        &mut prs,
        query,
        VARDATA_ANY(r#in as *const c_char),
        VARSIZE_ANY_EXHDR(r#in as *const c_char) as c_int,
    );

    if !opt.is_null() {
        prsoptions = deserialize_deflist(PointerGetDatum(opt as *const c_void));
    } else {
        prsoptions = NIL;
    }

    FunctionCall3!(
        &raw mut (*prsobj).prsheadline,
        PointerGetDatum(&raw mut prs as *const c_void),
        PointerGetDatum(prsoptions as *const c_void),
        PointerGetDatum(query as *const c_void)
    );

    out = generateHeadline(&mut prs);

    PG_FREE_IF_COPY!(fcinfo, r#in, 1);
    PG_FREE_IF_COPY!(fcinfo, query, 2);
    if !opt.is_null() {
        PG_FREE_IF_COPY!(fcinfo, opt, 3);
    }
    pfree(prs.words as *mut c_void);
    pfree(prs.startsel as *mut c_void);
    pfree(prs.stopsel as *mut c_void);

    PG_RETURN_POINTER!(out)
}

pub unsafe fn ts_headline_byid(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall3!(
        ts_headline_byid_opt,
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1),
        PG_GETARG_DATUM!(fcinfo, 2)
    ))
}

pub unsafe fn ts_headline(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall3!(
        ts_headline_byid_opt,
        ObjectIdGetDatum(getTSCurrentConfig(true)),
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1)
    ))
}

pub unsafe fn ts_headline_opt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall4!(
        ts_headline_byid_opt,
        ObjectIdGetDatum(getTSCurrentConfig(true)),
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1),
        PG_GETARG_DATUM!(fcinfo, 2)
    ))
}

pub unsafe fn ts_headline_jsonb_byid_opt(fcinfo: FunctionCallInfo) -> Datum {
    let tsconfig: Oid = PG_GETARG_OID!(fcinfo, 0);
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 2);
    let opt: *mut text =
        if (PG_NARGS!(fcinfo) as c_int) > 3 && !PG_GETARG_POINTER!(fcinfo, 3).is_null() {
            PG_GETARG_TEXT_P(fcinfo, 3)
        } else {
            std::ptr::null_mut()
        };
    let out: *mut Jsonb;
    let action: JsonTransformStringValuesAction = Some(headline_json_value_action);
    let mut prs: HeadlineParsedText = std::mem::zeroed();
    let state: *mut HeadlineJsonState =
        palloc0(std::mem::size_of::<HeadlineJsonState>()) as *mut HeadlineJsonState;

    memset(
        &raw mut prs as *mut c_void,
        0,
        std::mem::size_of::<HeadlineParsedText>(),
    );
    prs.lenwords = 32;
    prs.words = palloc(std::mem::size_of::<HeadlineWordEntry>() * prs.lenwords as usize)
        as *mut HeadlineWordEntry;

    (*state).prs = &mut prs;
    (*state).cfg = lookup_ts_config_cache(tsconfig);
    (*state).prsobj = lookup_ts_parser_cache((*(*state).cfg).prsId);
    (*state).query = query;
    if !opt.is_null() {
        (*state).prsoptions = deserialize_deflist(PointerGetDatum(opt as *const c_void));
    } else {
        (*state).prsoptions = NIL;
    }

    if !OidIsValid((*(*state).prsobj).headlineOid) {
        ereport!(
            ERROR,
            errmsg!("text search parser does not support headline creation")
        );
    }

    out = transform_jsonb_string_values(jb, state as *mut c_void, action);

    PG_FREE_IF_COPY!(fcinfo, jb, 1);
    PG_FREE_IF_COPY!(fcinfo, query, 2);
    if !opt.is_null() {
        PG_FREE_IF_COPY!(fcinfo, opt, 3);
    }

    pfree(prs.words as *mut c_void);

    if (*state).transformed {
        pfree(prs.startsel as *mut c_void);
        pfree(prs.stopsel as *mut c_void);
    }

    PG_RETURN_JSONB_P(out)
}

pub unsafe fn ts_headline_jsonb(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall3!(
        ts_headline_jsonb_byid_opt,
        ObjectIdGetDatum(getTSCurrentConfig(true)),
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1)
    ))
}

pub unsafe fn ts_headline_jsonb_byid(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall3!(
        ts_headline_jsonb_byid_opt,
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1),
        PG_GETARG_DATUM!(fcinfo, 2)
    ))
}

pub unsafe fn ts_headline_jsonb_opt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall4!(
        ts_headline_jsonb_byid_opt,
        ObjectIdGetDatum(getTSCurrentConfig(true)),
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1),
        PG_GETARG_DATUM!(fcinfo, 2)
    ))
}

pub unsafe fn ts_headline_json_byid_opt(fcinfo: FunctionCallInfo) -> Datum {
    let tsconfig: Oid = PG_GETARG_OID!(fcinfo, 0);
    let json: *mut text = PG_GETARG_TEXT_P(fcinfo, 1);
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 2);
    let opt: *mut text =
        if (PG_NARGS!(fcinfo) as c_int) > 3 && !PG_GETARG_POINTER!(fcinfo, 3).is_null() {
            PG_GETARG_TEXT_P(fcinfo, 3)
        } else {
            std::ptr::null_mut()
        };
    let out: *mut text;
    let action: JsonTransformStringValuesAction = Some(headline_json_value_action);

    let mut prs: HeadlineParsedText = std::mem::zeroed();
    let state: *mut HeadlineJsonState =
        palloc0(std::mem::size_of::<HeadlineJsonState>()) as *mut HeadlineJsonState;

    memset(
        &raw mut prs as *mut c_void,
        0,
        std::mem::size_of::<HeadlineParsedText>(),
    );
    prs.lenwords = 32;
    prs.words = palloc(std::mem::size_of::<HeadlineWordEntry>() * prs.lenwords as usize)
        as *mut HeadlineWordEntry;

    (*state).prs = &mut prs;
    (*state).cfg = lookup_ts_config_cache(tsconfig);
    (*state).prsobj = lookup_ts_parser_cache((*(*state).cfg).prsId);
    (*state).query = query;
    if !opt.is_null() {
        (*state).prsoptions = deserialize_deflist(PointerGetDatum(opt as *const c_void));
    } else {
        (*state).prsoptions = NIL;
    }

    if !OidIsValid((*(*state).prsobj).headlineOid) {
        ereport!(
            ERROR,
            errmsg!("text search parser does not support headline creation")
        );
    }

    out = transform_json_string_values(json, state as *mut c_void, action);

    PG_FREE_IF_COPY!(fcinfo, json, 1);
    PG_FREE_IF_COPY!(fcinfo, query, 2);
    if !opt.is_null() {
        PG_FREE_IF_COPY!(fcinfo, opt, 3);
    }
    pfree(prs.words as *mut c_void);

    if (*state).transformed {
        pfree(prs.startsel as *mut c_void);
        pfree(prs.stopsel as *mut c_void);
    }

    PG_RETURN_TEXT_P(out)
}

pub unsafe fn ts_headline_json(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall3!(
        ts_headline_json_byid_opt,
        ObjectIdGetDatum(getTSCurrentConfig(true)),
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1)
    ))
}

pub unsafe fn ts_headline_json_byid(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall3!(
        ts_headline_json_byid_opt,
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1),
        PG_GETARG_DATUM!(fcinfo, 2)
    ))
}

pub unsafe fn ts_headline_json_opt(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_DATUM!(DirectFunctionCall4!(
        ts_headline_json_byid_opt,
        ObjectIdGetDatum(getTSCurrentConfig(true)),
        PG_GETARG_DATUM!(fcinfo, 0),
        PG_GETARG_DATUM!(fcinfo, 1),
        PG_GETARG_DATUM!(fcinfo, 2)
    ))
}

/*
 * Return headline in text from, generated from a json(b) element
 */
unsafe fn headline_json_value(_state: *mut c_void, elem_value: *mut c_char, elem_len: c_int) -> *mut text {
    let state: *mut HeadlineJsonState = _state as *mut HeadlineJsonState;

    let prs: *mut HeadlineParsedText = (*state).prs;
    let cfg: *mut TSConfigCacheEntry = (*state).cfg;
    let prsobj: *mut TSParserCacheEntry = (*state).prsobj;
    let query: TSQuery = (*state).query;
    let prsoptions: *mut List = (*state).prsoptions;

    (*prs).curwords = 0;
    hlparsetext((*cfg).cfgId, prs, query, elem_value, elem_len);
    FunctionCall3!(
        &raw mut (*prsobj).prsheadline,
        PointerGetDatum(prs as *const c_void),
        PointerGetDatum(prsoptions as *const c_void),
        PointerGetDatum(query as *const c_void)
    );

    (*state).transformed = true;
    generateHeadline(prs)
}

/*
 * Adapter matching JsonTransformStringValuesAction's "extern C" signature.
 * In C the cast `(JsonTransformStringValuesAction) headline_json_value` is
 * legal because the prototypes match; here we wrap to satisfy Rust's typing.
 */
unsafe extern "C" fn headline_json_value_action(
    state: *mut c_void,
    elem_value: *mut c_char,
    elem_len: c_int,
) -> *mut text {
    headline_json_value(state, elem_value, elem_len)
}

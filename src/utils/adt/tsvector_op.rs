//! Translation of postgres/src/backend/utils/adt/tsvector_op.c
//!
//! Operations over the `tsvector` type: comparison, strip, setweight, concat,
//! length, conversion to/from arrays, unnest, the tsquery match (@@) family,
//! ts_stat, and the tsvector update trigger.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include`s mapped:
//!   limits.h                 -> INT_MAX (core::i32::MAX) where TS_phrase_output needs it
//!   access/htup_details.h    -> heap_form_tuple / heap_modify_tuple_by_cols (executor) - STUB
//!   catalog/namespace.h      -> stringToQualifiedNameList / get_ts_config_oid       - STUB
//!   catalog/pg_type.h        -> TEXTOID / INT2OID / CHAROID / TSVECTOROID ... (oids)  - STUB
//!   commands/trigger.h       -> TriggerData / CALLED_AS_TRIGGER / TRIGGER_FIRED_*    - STUB
//!   common/int.h             -> crate::common::int (pg_cmp_s32)
//!   executor/spi.h           -> SPI_* (ts_stat_sql)                                   - STUB
//!   funcapi.h                -> set-returning-function (SRF) machinery                - STUB
//!   lib/qunique.h            -> qunique (used by array/prefix paths)                  - STUB
//!   mb/pg_wchar.h            -> pg_mblen (ts_stat_sql weight scan)                     - STUB
//!   miscadmin.h              -> check_stack_depth / CHECK_FOR_INTERRUPTS              - STUB
//!   parser/parse_coerce.h    -> IsBinaryCoercible                                     - STUB
//!   tsearch/ts_utils.h       -> TSQuery / QueryItem / QueryOperand / ExecPhraseData /
//!                               TSExecuteCallback / TSTernaryValue and the @@ engine.
//!                               tsquery.c is NOT yet ported, so the entire TSQuery
//!                               match family (TS_execute*, checkcondition_*,
//!                               TS_phrase_*, ts_match_*) is STUBBED.
//!   utils/array.h            -> ArrayType / construct_array_builtin /
//!                               deconstruct_array_builtin.  construct/deconstruct are
//!                               NOT yet ported, so every array-consuming/producing
//!                               function is STUBBED.
//!   utils/builtins.h         -> cstring_to_text_with_len (crate::utils::adt::varlena)
//!   utils/regproc.h, utils/rel.h -> regproc / Relation helpers (trigger)             - STUB
//!
//!   The TSVector type + its access macros and the lexeme/position helpers
//!   (WordEntry / WordEntryPos / WordEntryPosVector / ARRPTR / STRPTR / _POSVECPTR /
//!   POSDATALEN / POSDATAPTR / CALCDATASIZE / WEP_* / LIMITPOS / MAXNUMPOS /
//!   MAXENTRYPOS / MAXSTRPOS / DatumGetTSVector / TSVectorGetDatum / tsCompareString)
//!   are all imported from the already-ported sibling crate::utils::adt::tsvector.
//!
//! NOTE: tsCompareString physically lives in this C file, but the sibling
//! tsvector.rs already translated it inline (compareentry there depends on it).
//! To avoid a duplicate definition we IMPORT it here rather than re-defining it.
//!
//! TRANSLATED FULLY (self-contained over the ported TSVector):
//!   silly_cmp_tsvector, tsvector_lt/le/eq/ge/gt/ne/cmp (the TSVECTORCMPFUNC family),
//!   tsvector_strip, tsvector_length, tsvector_setweight, add_pos, tsvector_bsearch,
//!   tsvector_concat.
//!
//! STUBBED (deps not yet ported):
//!   - tsvector_setweight_by_filter, tsvector_to_array, array_to_tsvector,
//!     tsvector_filter, tsvector_delete_str/_arr, tsvector_delete_by_indices,
//!     compare_int, compare_text_lexemes  -> need utils/array.h construct/deconstruct.
//!   - tsvector_unnest, ts_stat1/2          -> need funcapi SRF (+ array / SPI).
//!   - the @@ family: checkclass_str, checkcondition_str, TS_phrase_output,
//!     TS_phrase_execute, TS_execute, TS_execute_ternary, TS_execute_recurse,
//!     TS_execute_locations(_recurse), tsquery_requires_match, ts_match_qv/vq/tt/tq
//!     -> need the TSQuery type (tsquery.c not yet ported).
//!   - ts_accum/insertStatEntry/... and tsvector_update_trigger* -> need SPI / executor
//!     / trigger manager / catalog.

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::varatt::*;

use crate::{
    PG_GETARG_CHAR, PG_GETARG_DATUM, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_INT32,
    PG_RETURN_POINTER, PG_FREE_IF_COPY, PG_RETURN_DATUM, DirectFunctionCall2,
    list_make1, foreach, current_cell, DirectFunctionCall1,
};
use crate::utils::adt::ts_type::TSQueryGetDatum;
// pg_list support used by TS_execute_locations (the @@ location-list engine).
use crate::nodes::pg_list::{List, NIL, lappend, lfirst, list_concat};
use crate::c::{int32, uint16, uint32, Size, SHORTALIGN};
use crate::common::int::pg_cmp_s32;
use crate::utils::adt::tsvector::{
    TSVector, WordEntry, WordEntryPos, WordEntryPosVector, ARRPTR, CALCDATASIZE, DatumGetTSVector,
    LIMITPOS, MAXENTRYPOS, MAXNUMPOS, MAXSTRPOS, POSDATALEN, POSDATAPTR, STRPTR, TSVectorGetDatum,
    WEP_GETPOS, WEP_GETWEIGHT, WEP_SETPOS, WEP_SETWEIGHT, _POSVECPTR, compareWordEntryPos,
    tsCompareString,
};
use crate::utils::adt::tsquery_util::{
    QueryItem, QueryOperand, TSQuery, GETOPERAND, GETQUERY, OP_AND, OP_NOT, OP_OR, OP_PHRASE, QI_VAL,
};
use crate::utils::misc::stack_depth::check_stack_depth;
use crate::postgres::{DatumGetBool, DatumGetChar, Int16GetDatum, PointerGetDatum};
use crate::utils::adt::varlena::cstring_to_text_with_len;
use crate::c::{int8, text};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::attnum::AttrNumber;
use crate::lib::qunique::qunique;
use core::ffi::{c_char, c_int, c_void};

/*
 * funcapi.h: set-returning-function machinery.  funcapi.rs is not wired into
 * the module tree yet, so (mirroring sibling adt modules such as regexp.rs)
 * the FuncCallContext / AttInMetadata layouts and the get_call_result_type
 * helper are declared locally with TODO(pg-port) stubs.
 */
#[repr(C)]
struct FuncCallContext {
    call_cntr: u64,
    max_calls: u64,
    user_fctx: *mut c_void,
    attinmeta: *mut AttInMetadata,
    multi_call_memory_ctx: MemoryContext,
    tuple_desc: TupleDesc,
}
#[repr(C)]
struct AttInMetadata {
    _opaque: [u8; 0],
}
/* enum TypeFuncClass member used here. */
const TYPEFUNC_COMPOSITE: c_int = 0;
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> c_int {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c get_call_result_type
}

/* catalog/pg_type.h OIDs used by the array-producing/consuming paths. */
const TEXTOID: Oid = 25;
const INT2OID: Oid = 21;
const INT2ARRAYOID: Oid = 1005;
const TEXTARRAYOID: Oid = 1009;
const CHAROID: Oid = 18;
const TSVECTOROID: Oid = 3614;
const REGCONFIGOID: Oid = 3734;

/* errcodes.h classifications (errcode() shim ignores the value). */
const ERRCODE_NULL_VALUE_NOT_ALLOWED: c_int = 0;
const ERRCODE_ZERO_LENGTH_CHARACTER_STRING: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
const ERRCODE_UNDEFINED_COLUMN: c_int = 0;
const ERRCODE_DATATYPE_MISMATCH: c_int = 0;

extern "C" {
    fn sprintf(s: *mut c_char, fmt: *const c_char, ...) -> c_int;
}

// ----------------------------------------------------------------
//   Unported dependency stubs (TODO(pg-port)).  These mirror the
//   per-file stub convention used by sibling adt modules until the
//   underlying units (utils/array.c arrayfuncs, executor/spi.c,
//   funcapi SRF, commands/trigger.c, catalog/namespace.c, the text
//   search parser) are translated.
// ----------------------------------------------------------------

/* utils/array.h construct_array_builtin / deconstruct_array_builtin. */
unsafe fn construct_array_builtin(_elems: *mut Datum, _nelems: c_int, _elmtype: Oid) -> *mut c_void {
    unimplemented!() // TODO(pg-port): utils/adt/arrayfuncs.c construct_array_builtin
}
unsafe fn deconstruct_array_builtin(
    _arr: *mut c_void,
    _elmtype: Oid,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!() // TODO(pg-port): utils/adt/arrayfuncs.c deconstruct_array_builtin
}

/* PG_GETARG_ARRAYTYPE_P(n): detoast + cast to ArrayType (identity for in-line). */
#[inline]
unsafe fn PG_GETARG_ARRAYTYPE_P(fcinfo: FunctionCallInfo, n: usize) -> *mut c_void {
    DatumGetPointer(PG_GETARG_DATUM!(fcinfo, n)) as *mut c_void
}

/* qsort(base, n, sizeof(int), compare_int) over a c_int array. */
unsafe fn qsort_int(base: *mut c_int, n: c_int) {
    let sl = core::slice::from_raw_parts_mut(base, n as usize);
    sl.sort_by(|a, b| {
        compare_int(a as *const c_int as *const c_void, b as *const c_int as *const c_void).cmp(&0)
    });
}

/* qsort(base, n, sizeof(Datum), compare_text_lexemes) over a Datum array. */
unsafe fn qsort_datum_lexemes(base: *mut Datum, n: c_int) {
    let sl = core::slice::from_raw_parts_mut(base, n as usize);
    sl.sort_by(|a, b| {
        compare_text_lexemes(a as *const Datum as *const c_void, b as *const Datum as *const c_void)
            .cmp(&0)
    });
}

// SRF_* macros (funcapi.h): expand to local srf_* helper stubs, matching the
// per-file convention used by sibling adt modules (regexp.rs etc.).
macro_rules! SRF_IS_FIRSTCALL {
    ($fcinfo:expr) => { srf_is_firstcall($fcinfo) };
}
macro_rules! SRF_FIRSTCALL_INIT {
    ($fcinfo:expr) => { srf_firstcall_init($fcinfo) };
}
macro_rules! SRF_PERCALL_SETUP {
    ($fcinfo:expr) => { srf_percall_setup($fcinfo) };
}
macro_rules! SRF_RETURN_NEXT {
    ($fcinfo:expr, $fctx:expr, $result:expr) => { return srf_return_next($fcinfo, $fctx, $result) };
}
macro_rules! SRF_RETURN_DONE {
    ($fcinfo:expr, $fctx:expr) => { return srf_return_done($fcinfo, $fctx) };
}

unsafe fn srf_is_firstcall(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_firstcall_init(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_percall_setup(_fcinfo: FunctionCallInfo) -> *mut FuncCallContext {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_return_next(
    _fcinfo: FunctionCallInfo,
    _fctx: *mut FuncCallContext,
    _result: Datum,
) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn srf_return_done(_fcinfo: FunctionCallInfo, _fctx: *mut FuncCallContext) -> Datum {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}

/* tupdesc.h / funcapi.h tuple-building helpers (executor / access). */
unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc {
    unimplemented!() // TODO(pg-port): access/common/tupdesc.c
}
unsafe fn TupleDescInitEntry(
    _td: TupleDesc, _ano: AttrNumber, _name: *const c_char, _typid: Oid, _typmod: i32, _attdim: c_int,
) {
    unimplemented!() // TODO(pg-port): access/common/tupdesc.c
}
unsafe fn TupleDescGetAttInMetadata(_td: TupleDesc) -> *mut AttInMetadata {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
unsafe fn heap_form_tuple(_td: TupleDesc, _values: *mut Datum, _nulls: *mut bool) -> *mut c_void {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn BuildTupleFromCStrings(_attinmeta: *mut AttInMetadata, _values: *mut *mut c_char) -> *mut c_void {
    unimplemented!() // TODO(pg-port): utils/fmgr/funcapi.c
}
#[inline]
unsafe fn HeapTupleGetDatum(_tuple: *mut c_void) -> Datum {
    unimplemented!() // TODO(pg-port): funcapi.h HeapTupleGetDatum
}

/* mb/pg_wchar.h pg_mblen_range: length of multibyte char at buf, bounded by end. */
unsafe fn pg_mblen_range(_buf: *const c_char, _end: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): src/common/wchar.c pg_mblen_range
}

/* executor/spi.h: SPI cursor/plan machinery (ts_stat_sql). */
type SPIPlanPtr = *mut c_void;
type Portal = *mut c_void;
unsafe fn SPI_connect() -> c_int {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
unsafe fn SPI_finish() -> c_int {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
unsafe fn SPI_prepare(_src: *const c_char, _nargs: c_int, _argtypes: *mut Oid) -> SPIPlanPtr {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
unsafe fn SPI_cursor_open(
    _name: *const c_char, _plan: SPIPlanPtr, _values: *mut Datum, _nulls: *const c_char, _read_only: bool,
) -> Portal {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
unsafe fn SPI_cursor_fetch(_portal: Portal, _forward: bool, _count: i64) {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
unsafe fn SPI_cursor_close(_portal: Portal) {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
unsafe fn SPI_freeplan(_plan: SPIPlanPtr) -> c_int {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
unsafe fn SPI_freetuptable(_tuptable: *mut SPITupleTable) {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
unsafe fn SPI_gettypeid(_tupdesc: TupleDesc, _fnumber: c_int) -> Oid {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
unsafe fn SPI_getbinval(
    _row: *mut c_void, _tupdesc: TupleDesc, _fnumber: c_int, _isnull: *mut bool,
) -> Datum {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
unsafe fn SPI_fnumber(_tupdesc: TupleDesc, _fname: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): executor/spi.c
}
const SPI_ERROR_NOATTRIBUTE: c_int = -9;
#[repr(C)]
struct SPITupleTable {
    tupdesc: TupleDesc,
    vals: *mut *mut c_void,
}
extern "C" {
    static mut SPI_tuptable: *mut SPITupleTable;
    static SPI_processed: u64;
}
#[repr(C)]
struct TupleDescData {
    natts: c_int,
}

/* parser/parse_coerce.h IsBinaryCoercible. */
unsafe fn IsBinaryCoercible(_srctype: Oid, _targettype: Oid) -> bool {
    unimplemented!() // TODO(pg-port): parser/parse_coerce.c IsBinaryCoercible
}

/* utils/builtins.h text_to_cstring. */
unsafe fn text_to_cstring(_t: *const text) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/adt/varlena.c text_to_cstring
}

/* catalog/namespace.h: tsconfig name resolution. */
unsafe fn stringToQualifiedNameList(
    _string: *const c_char, _escontext: *mut c_void,
) -> *mut crate::nodes::pg_list::List {
    unimplemented!() // TODO(pg-port): catalog/namespace.c stringToQualifiedNameList
}
unsafe fn get_ts_config_oid(_names: *mut crate::nodes::pg_list::List, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO(pg-port): commands/tsearchcmds.c get_ts_config_oid
}

/* The text search parser glue (tsvector.c sibling units). */
#[repr(C)]
struct ParsedWord {
    _opaque: [u8; 0],
}
#[repr(C)]
struct ParsedText {
    lenwords: c_int,
    curwords: c_int,
    pos: c_int,
    words: *mut ParsedWord,
}
unsafe fn parsetext(_cfgId: Oid, _prs: *mut ParsedText, _buf: *mut c_char, _buflen: c_int) {
    unimplemented!() // TODO(pg-port): tsearch/ts_parse.c parsetext
}
unsafe fn make_tsvector(_prs: *mut ParsedText) -> TSVector {
    unimplemented!() // TODO(pg-port): utils/adt/tsvector.c make_tsvector
}

/* to_tsvector / plainto_tsquery fmgr entry points (ts_parse / tsquery parser). */
unsafe fn to_tsvector(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO(pg-port): tsearch/to_tsany.c to_tsvector
}
unsafe fn plainto_tsquery(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO(pg-port): tsearch/to_tsany.c plainto_tsquery
}

/* commands/trigger.h: trigger manager context + event-bit predicates. */
#[repr(C)]
struct TriggerData {
    tg_event: u32,
    tg_trigtuple: *mut c_void,
    tg_newtuple: *mut c_void,
    tg_trigger: *mut Trigger,
    tg_relation: *mut RelationData,
    tg_updatedcols: *mut c_void,
}
#[repr(C)]
struct Trigger {
    tgnargs: i16,
    tgargs: *mut *mut c_char,
}
#[repr(C)]
struct RelationData {
    rd_att: TupleDesc,
}
type Relation = *mut RelationData;
unsafe fn CALLED_AS_TRIGGER(fcinfo: FunctionCallInfo) -> bool {
    !(*fcinfo).context.is_null()
        && crate::IsA!((*fcinfo).context as *mut c_void, T_TriggerData)
}
unsafe fn TRIGGER_FIRED_FOR_ROW(_tg_event: u32) -> bool {
    unimplemented!() // TODO(pg-port): commands/trigger.h TRIGGER_FIRED_FOR_ROW
}
unsafe fn TRIGGER_FIRED_BEFORE(_tg_event: u32) -> bool {
    unimplemented!() // TODO(pg-port): commands/trigger.h TRIGGER_FIRED_BEFORE
}
unsafe fn TRIGGER_FIRED_BY_INSERT(_tg_event: u32) -> bool {
    unimplemented!() // TODO(pg-port): commands/trigger.h TRIGGER_FIRED_BY_INSERT
}
unsafe fn TRIGGER_FIRED_BY_UPDATE(_tg_event: u32) -> bool {
    unimplemented!() // TODO(pg-port): commands/trigger.h TRIGGER_FIRED_BY_UPDATE
}
unsafe fn bms_is_member(_x: c_int, _set: *mut c_void) -> bool {
    unimplemented!() // TODO(pg-port): nodes/bitmapset.c bms_is_member
}
const FirstLowInvalidHeapAttributeNumber: c_int = -8;
unsafe fn heap_modify_tuple_by_cols(
    _tuple: *mut c_void, _tupleDesc: TupleDesc, _nCols: c_int, _replCols: *mut c_int,
    _replValues: *mut Datum, _replIsnull: *mut bool,
) -> *mut c_void {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c heap_modify_tuple_by_cols
}
unsafe fn DatumGetObjectId(d: Datum) -> Oid {
    d as Oid
}

/* limits.h INT_MAX, used by TS_phrase_output. */
const INT_MAX: c_int = c_int::MAX;

extern "C" {
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

// ================================================================
//   types local to tsvector_op.c
// ================================================================

/*
 * typedef struct { WordEntry *arrb; WordEntry *arre; char *values; char *operand; } CHKVAL;
 *
 * The opaque `arg` threaded through TS_execute/checkcondition_str: it describes
 * the tsvector being matched (entry array bounds + lexeme/operand storage).
 */
#[repr(C)]
struct CHKVAL {
    arrb: *mut WordEntry,
    arre: *mut WordEntry,
    values: *mut c_char,
    operand: *mut c_char,
}

// ----------------------------------------------------------------
//   tsearch/ts_utils.h: TSQuery execution support
// ----------------------------------------------------------------

/*
 * TS_execute requires ternary logic to handle NOT with phrase matches.
 *
 * typedef enum { TS_NO, TS_YES, TS_MAYBE } TSTernaryValue;
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TSTernaryValue {
    TS_NO = 0,    /* definitely no match */
    TS_YES = 1,   /* definitely does match */
    TS_MAYBE = 2, /* can't verify match for lack of pos data */
}
pub use TSTernaryValue::{TS_MAYBE, TS_NO, TS_YES};

/*
 * struct ExecPhraseData is passed to a TSExecuteCallback function if we need
 * lexeme position data (because of a phrase-match operator in the tsquery).
 * All fields are initially zeroed by the caller.
 */
#[repr(C)]
pub struct ExecPhraseData {
    pub npos: c_int,           /* number of positions reported */
    pub allocated: bool,       /* pos points to palloc'd data? */
    pub negate: bool,          /* positions are where query is NOT matched */
    pub pos: *mut WordEntryPos, /* ordered, non-duplicate lexeme positions */
    pub width: c_int,          /* width of match in lexemes, less 1 */
}

impl ExecPhraseData {
    /* memset(&x, 0, sizeof(x)) equivalent */
    #[inline]
    fn zeroed() -> ExecPhraseData {
        ExecPhraseData {
            npos: 0,
            allocated: false,
            negate: false,
            pos: null_mut(),
            width: 0,
        }
    }
}

/*
 * Signature for TSQuery lexeme check functions.
 *
 * C: typedef TSTernaryValue (*TSExecuteCallback)(void *arg, QueryOperand *val,
 *                                                ExecPhraseData *data);
 * Modeled as a plain unsafe fn pointer, matching the vtable/callback convention
 * used elsewhere in the crate (e.g. AppendState.choose_next_subplan).
 */
pub type TSExecuteCallback =
    unsafe fn(arg: *mut c_void, val: *mut QueryOperand, data: *mut ExecPhraseData) -> TSTernaryValue;

/*
 * Flag bits for TS_execute (ts_utils.h).
 */
pub const TS_EXEC_EMPTY: uint32 = 0x00;
/* NOT sub-expressions are automatically evaluated to be true. */
pub const TS_EXEC_SKIP_NOT: uint32 = 0x01;
/* allow OP_PHRASE to be executed lossily in the absence of position info. */
pub const TS_EXEC_PHRASE_NO_POS: uint32 = 0x02;

/*
 * typedef struct StatEntry { ... } StatEntry;  -- ts_stat support, stubbed.
 */
#[repr(C)]
#[allow(dead_code)]
struct StatEntry {
    ndoc: uint32, /* zero indicates that we were already here while walking the tree */
    nentry: uint32,
    left: *mut StatEntry,
    right: *mut StatEntry,
    lenlexeme: uint32,
    lexeme: [c_char; FLEXIBLE_ARRAY_MEMBER],
}

/* #define STATENTRYHDRSZ (offsetof(StatEntry, lexeme)) */
#[allow(dead_code)]
#[inline]
fn STATENTRYHDRSZ() -> usize {
    core::mem::offset_of!(StatEntry, lexeme)
}

#[repr(C)]
#[allow(dead_code)]
struct TSVectorStat {
    weight: int32,
    maxdepth: uint32,
    stack: *mut *mut StatEntry,
    stackpos: uint32,
    root: *mut StatEntry,
}

// ----------------------------------------------------------------
//   PG_GETARG_TSVECTOR(n): the C macro detoasts; with TOAST unported it is
//   the identity for in-line datums (mirrors tsvector.rs).
// ----------------------------------------------------------------
#[inline]
unsafe fn PG_GETARG_TSVECTOR(datum: Datum) -> TSVector {
    DatumGetTSVector(datum)
}

/*
 * PG_GETARG_TSVECTOR_COPY(n): the C macro detoasts into a fresh copy; with
 * TOAST unported it is the identity for in-line datums (mirrors above).
 */
#[inline]
unsafe fn PG_GETARG_TSVECTOR_COPY(fcinfo: FunctionCallInfo, n: usize) -> TSVector {
    DatumGetTSVector(PG_GETARG_DATUM!(fcinfo, n))
}

/*
 * Order: haspos, len, word, for all positions (pos, weight)
 */
unsafe fn silly_cmp_tsvector(a: TSVector, b: TSVector) -> c_int {
    if VARSIZE(a as *const c_char) < VARSIZE(b as *const c_char) {
        return -1;
    } else if VARSIZE(a as *const c_char) > VARSIZE(b as *const c_char) {
        return 1;
    } else if (*a).size < (*b).size {
        return -1;
    } else if (*a).size > (*b).size {
        return 1;
    } else {
        let mut aptr: *mut WordEntry = ARRPTR(a);
        let mut bptr: *mut WordEntry = ARRPTR(b);
        let mut i: c_int = 0;
        let mut res: c_int;

        while i < (*a).size {
            if (*aptr).haspos() != (*bptr).haspos() {
                return if (*aptr).haspos() > (*bptr).haspos() {
                    -1
                } else {
                    1
                };
            } else if {
                res = tsCompareString(
                    STRPTR(a).add((*aptr).pos() as usize),
                    (*aptr).len() as c_int,
                    STRPTR(b).add((*bptr).pos() as usize),
                    (*bptr).len() as c_int,
                    false,
                );
                res != 0
            } {
                return res;
            } else if (*aptr).haspos() != 0 {
                let mut ap: *mut WordEntryPos = POSDATAPTR(a, aptr);
                let mut bp: *mut WordEntryPos = POSDATAPTR(b, bptr);
                let mut j: c_int;

                if POSDATALEN(a, aptr) != POSDATALEN(b, bptr) {
                    return if POSDATALEN(a, aptr) > POSDATALEN(b, bptr) {
                        -1
                    } else {
                        1
                    };
                }

                j = 0;
                while j < POSDATALEN(a, aptr) {
                    if WEP_GETPOS(*ap) != WEP_GETPOS(*bp) {
                        return if WEP_GETPOS(*ap) > WEP_GETPOS(*bp) { -1 } else { 1 };
                    } else if WEP_GETWEIGHT(*ap) != WEP_GETWEIGHT(*bp) {
                        return if WEP_GETWEIGHT(*ap) > WEP_GETWEIGHT(*bp) {
                            -1
                        } else {
                            1
                        };
                    }
                    ap = ap.add(1);
                    bp = bp.add(1);
                    j += 1;
                }
            }

            aptr = aptr.add(1);
            bptr = bptr.add(1);
            i += 1;
        }
    }

    0
}

/*
 * #define TSVECTORCMPFUNC(type, action, ret) ...
 *
 * The C macro stamps out seven fmgr functions, each of which compares the two
 * argument tsvectors with silly_cmp_tsvector and returns `res action 0`.  We
 * expand them by hand here.
 */
macro_rules! TSVECTORCMPFUNC {
    ($name:ident, $cmp:tt, bool) => {
        pub unsafe fn $name(fcinfo: FunctionCallInfo) -> Datum {
            let a: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
            let b: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 1));
            let res: c_int = silly_cmp_tsvector(a, b);
            PG_FREE_IF_COPY!(fcinfo, a, 0);
            PG_FREE_IF_COPY!(fcinfo, b, 1);
            PG_RETURN_BOOL!(res $cmp 0)
        }
    };
    ($name:ident, $cmp:tt, int32) => {
        pub unsafe fn $name(fcinfo: FunctionCallInfo) -> Datum {
            let a: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
            let b: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 1));
            let res: c_int = silly_cmp_tsvector(a, b);
            PG_FREE_IF_COPY!(fcinfo, a, 0);
            PG_FREE_IF_COPY!(fcinfo, b, 1);
            // cmp variant: action is `+`, i.e. `res + 0`.
            PG_RETURN_INT32!(res $cmp 0)
        }
    };
}

TSVECTORCMPFUNC!(tsvector_lt, <, bool);
TSVECTORCMPFUNC!(tsvector_le, <=, bool);
TSVECTORCMPFUNC!(tsvector_eq, ==, bool);
TSVECTORCMPFUNC!(tsvector_ge, >=, bool);
TSVECTORCMPFUNC!(tsvector_gt, >, bool);
TSVECTORCMPFUNC!(tsvector_ne, !=, bool);
TSVECTORCMPFUNC!(tsvector_cmp, +, int32);

pub unsafe fn tsvector_strip(fcinfo: FunctionCallInfo) -> Datum {
    let in_: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let out: TSVector;
    let mut i: c_int;
    let mut len: c_int = 0;
    let arrin: *mut WordEntry = ARRPTR(in_);
    let arrout: *mut WordEntry;
    let mut cur: *mut c_char;

    i = 0;
    while i < (*in_).size {
        len += (*arrin.add(i as usize)).len() as c_int;
        i += 1;
    }

    let lenb = CALCDATASIZE((*in_).size, len) as c_int;
    out = palloc0(lenb as Size) as TSVector;
    SET_VARSIZE(out as *mut c_char, lenb);
    (*out).size = (*in_).size;
    arrout = ARRPTR(out);
    cur = STRPTR(out);
    i = 0;
    while i < (*in_).size {
        memcpy(
            cur as *mut c_void,
            STRPTR(in_).add((*arrin.add(i as usize)).pos() as usize) as *const c_void,
            (*arrin.add(i as usize)).len() as usize,
        );
        (*arrout.add(i as usize)).set_haspos(0);
        (*arrout.add(i as usize)).set_len((*arrin.add(i as usize)).len());
        (*arrout.add(i as usize)).set_pos((cur as isize - STRPTR(out) as isize) as u32);
        cur = cur.add((*arrout.add(i as usize)).len() as usize);
        i += 1;
    }

    PG_FREE_IF_COPY!(fcinfo, in_, 0);
    PG_RETURN_POINTER!(out)
}

pub unsafe fn tsvector_length(fcinfo: FunctionCallInfo) -> Datum {
    let in_: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let ret: int32 = (*in_).size;

    PG_FREE_IF_COPY!(fcinfo, in_, 0);
    PG_RETURN_INT32!(ret)
}

pub unsafe fn tsvector_setweight(fcinfo: FunctionCallInfo) -> Datum {
    let in_: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let cw: c_char = PG_GETARG_CHAR!(fcinfo, 1);
    let out: TSVector;
    let mut i: c_int;
    let mut j: c_int;
    let mut entry: *mut WordEntry;
    let mut p: *mut WordEntryPos;
    let mut w: c_int = 0;

    match cw as u8 {
        b'A' | b'a' => w = 3,
        b'B' | b'b' => w = 2,
        b'C' | b'c' => w = 1,
        b'D' | b'd' => w = 0,
        _ => {
            /* internal error */
            elog!(ERROR, "unrecognized weight: {}", cw as c_int);
        }
    }

    out = palloc(VARSIZE(in_ as *const c_char) as Size) as TSVector;
    memcpy(
        out as *mut c_void,
        in_ as *const c_void,
        VARSIZE(in_ as *const c_char) as usize,
    );
    entry = ARRPTR(out);
    i = (*out).size;
    while {
        let old = i;
        i -= 1;
        old != 0
    } {
        j = POSDATALEN(out, entry);
        if j != 0 {
            p = POSDATAPTR(out, entry);
            while {
                let old = j;
                j -= 1;
                old != 0
            } {
                WEP_SETWEIGHT(&mut *p, w);
                p = p.add(1);
            }
        }
        entry = entry.add(1);
    }

    PG_FREE_IF_COPY!(fcinfo, in_, 0);
    PG_RETURN_POINTER!(out)
}

/*
 * setweight(tsin tsvector, char_weight "char", lexemes "text"[])
 *
 * Assign weight w to elements of tsin that are listed in lexemes.
 *
 * TODO(pg-port): needs utils/array.h deconstruct_array_builtin (not yet ported).
 */
pub unsafe fn tsvector_setweight_by_filter(fcinfo: FunctionCallInfo) -> Datum {
    let tsin: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let char_weight: c_char = PG_GETARG_CHAR!(fcinfo, 1);
    let lexemes: *mut c_void = PG_GETARG_ARRAYTYPE_P(fcinfo, 2);

    let tsout: TSVector;
    let mut i: c_int;
    let mut j: c_int;
    let mut nlexemes: c_int = 0;
    let mut weight: c_int = 0;
    let entry: *mut WordEntry;
    let mut dlexemes: *mut Datum = null_mut();
    let mut nulls: *mut bool = null_mut();

    match char_weight as u8 {
        b'A' | b'a' => weight = 3,
        b'B' | b'b' => weight = 2,
        b'C' | b'c' => weight = 1,
        b'D' | b'd' => weight = 0,
        _ => {
            /* internal error */
            elog!(ERROR, "unrecognized weight: {}", char_weight as u8 as char);
        }
    }

    tsout = palloc(VARSIZE(tsin as *const c_char) as Size) as TSVector;
    memcpy(
        tsout as *mut c_void,
        tsin as *const c_void,
        VARSIZE(tsin as *const c_char) as usize,
    );
    entry = ARRPTR(tsout);

    deconstruct_array_builtin(lexemes, TEXTOID, &mut dlexemes, &mut nulls, &mut nlexemes);

    /*
     * Assuming that lexemes array is significantly shorter than tsvector we
     * can iterate through lexemes performing binary search of each lexeme
     * from lexemes in tsvector.
     */
    i = 0;
    while i < nlexemes {
        let lex: *mut c_char;
        let lex_len: c_int;
        let lex_pos: c_int;

        /* Ignore null array elements, they surely don't match */
        if *nulls.add(i as usize) {
            i += 1;
            continue;
        }

        lex = VARDATA(DatumGetPointer(*dlexemes.add(i as usize)) as *const c_char);
        lex_len = VARSIZE(DatumGetPointer(*dlexemes.add(i as usize)) as *const c_char) as c_int
            - VARHDRSZ;
        lex_pos = tsvector_bsearch(tsout, lex, lex_len);

        if lex_pos >= 0 && {
            j = POSDATALEN(tsout, entry.add(lex_pos as usize));
            j != 0
        } {
            let mut p: *mut WordEntryPos = POSDATAPTR(tsout, entry.add(lex_pos as usize));

            while {
                let old = j;
                j -= 1;
                old != 0
            } {
                WEP_SETWEIGHT(&mut *p, weight);
                p = p.add(1);
            }
        }
        i += 1;
    }

    PG_FREE_IF_COPY!(fcinfo, tsin, 0);
    PG_FREE_IF_COPY!(fcinfo, lexemes, 2);

    PG_RETURN_POINTER!(tsout)
}

/*
 * #define compareEntry(pa, a, pb, b) \
 *     tsCompareString((pa) + (a)->pos, (a)->len, (pb) + (b)->pos, (b)->len, false)
 */
#[inline]
unsafe fn compareEntry(
    pa: *mut c_char,
    a: *const WordEntry,
    pb: *mut c_char,
    b: *const WordEntry,
) -> int32 {
    tsCompareString(
        pa.add((*a).pos() as usize),
        (*a).len() as c_int,
        pb.add((*b).pos() as usize),
        (*b).len() as c_int,
        false,
    )
}

/*
 * Add positions from src to dest after offsetting them by maxpos.
 * Return the number added (might be less than expected due to overflow)
 */
unsafe fn add_pos(
    src: TSVector,
    srcptr: *mut WordEntry,
    dest: TSVector,
    destptr: *mut WordEntry,
    maxpos: int32,
) -> int32 {
    let clen: *mut uint16 = &mut (*_POSVECPTR(dest, destptr)).npos;
    let mut i: c_int;
    let slen: uint16 = POSDATALEN(src, srcptr) as uint16;
    let startlen: uint16;
    let spos: *mut WordEntryPos = POSDATAPTR(src, srcptr);
    let dpos: *mut WordEntryPos = POSDATAPTR(dest, destptr);

    if (*destptr).haspos() == 0 {
        *clen = 0;
    }

    startlen = *clen;
    i = 0;
    while (i as uint16) < slen
        && *clen < MAXNUMPOS as uint16
        && (*clen == 0 || WEP_GETPOS(*dpos.add((*clen - 1) as usize)) != MAXENTRYPOS - 1)
    {
        WEP_SETWEIGHT(
            &mut *dpos.add(*clen as usize),
            WEP_GETWEIGHT(*spos.add(i as usize)),
        );
        WEP_SETPOS(
            &mut *dpos.add(*clen as usize),
            LIMITPOS(WEP_GETPOS(*spos.add(i as usize)) + maxpos),
        );
        *clen += 1;
        i += 1;
    }

    if *clen != startlen {
        (*destptr).set_haspos(1);
    }
    (*clen - startlen) as int32
}

/*
 * Perform binary search of given lexeme in TSVector.
 * Returns lexeme position in TSVector's entry array or -1 if lexeme wasn't
 * found.
 */
#[allow(dead_code)]
unsafe fn tsvector_bsearch(tsv: TSVector, lexeme: *mut c_char, lexeme_len: c_int) -> c_int {
    let arrin: *mut WordEntry = ARRPTR(tsv);
    let mut StopLow: c_int = 0;
    let mut StopHigh: c_int = (*tsv).size;
    let mut StopMiddle: c_int;
    let mut cmp: c_int;

    while StopLow < StopHigh {
        StopMiddle = (StopLow + StopHigh) / 2;

        cmp = tsCompareString(
            lexeme,
            lexeme_len,
            STRPTR(tsv).add((*arrin.add(StopMiddle as usize)).pos() as usize),
            (*arrin.add(StopMiddle as usize)).len() as c_int,
            false,
        );

        if cmp < 0 {
            StopHigh = StopMiddle;
        } else if cmp > 0 {
            StopLow = StopMiddle + 1;
        } else {
            /* found it */
            return StopMiddle;
        }
    }

    -1
}

/*
 * qsort comparator functions
 *
 * TODO(pg-port): compare_int / compare_text_lexemes are only used by the
 * array-consuming functions (tsvector_delete_by_indices, array_to_tsvector),
 * which are themselves stubbed pending utils/array.h.  Kept as stubs for parity.
 */
#[allow(dead_code)]
unsafe fn compare_int(va: *const c_void, vb: *const c_void) -> c_int {
    let a: c_int = *(va as *const c_int);
    let b: c_int = *(vb as *const c_int);
    pg_cmp_s32(a, b)
}

#[allow(dead_code)]
unsafe fn compare_text_lexemes(va: *const c_void, vb: *const c_void) -> c_int {
    // C: Datum a/b -> VARDATA_ANY / VARSIZE_ANY_EXHDR -> tsCompareString.
    let a: Datum = *(va as *const Datum);
    let b: Datum = *(vb as *const Datum);
    let alex: *mut c_char = VARDATA_ANY(DatumGetPointer(a) as *const c_char);
    let alex_len: c_int = VARSIZE_ANY_EXHDR(DatumGetPointer(a) as *const c_char) as c_int;
    let blex: *mut c_char = VARDATA_ANY(DatumGetPointer(b) as *const c_char);
    let blex_len: c_int = VARSIZE_ANY_EXHDR(DatumGetPointer(b) as *const c_char) as c_int;

    tsCompareString(alex, alex_len, blex, blex_len, false)
}

/*
 * Internal routine to delete lexemes from TSVector by array of offsets.
 *
 * TODO(pg-port): self-contained over TSVector, but only reached from
 * tsvector_delete_str/_arr, which are blocked on utils/array.h + lib/qunique.h.
 * Stubbed to keep the dependency surface small until those land.
 */
unsafe fn tsvector_delete_by_indices(
    tsv: TSVector,
    indices_to_delete: *mut c_int,
    mut indices_count: c_int,
) -> TSVector {
    let tsout: TSVector;
    let arrin: *mut WordEntry = ARRPTR(tsv);
    let arrout: *mut WordEntry;
    let data: *mut c_char = STRPTR(tsv);
    let dataout: *mut c_char;
    let mut i: c_int; /* index in arrin */
    let mut j: c_int; /* index in arrout */
    let mut k: c_int; /* index in indices_to_delete */
    let mut curoff: c_int; /* index in dataout area */

    /*
     * Sort the filter array to simplify membership checks below.  Also, get
     * rid of any duplicate entries, so that we can assume that indices_count
     * is exactly equal to the number of lexemes that will be removed.
     */
    if indices_count > 1 {
        qsort_int(indices_to_delete, indices_count);
        indices_count = qunique(
            indices_to_delete as *mut c_void,
            indices_count as usize,
            core::mem::size_of::<c_int>(),
            compare_int,
        ) as c_int;
    }

    /*
     * Here we overestimate tsout size, since we don't know how much space is
     * used by the deleted lexeme(s).  We will set exact size below.
     */
    tsout = palloc0(VARSIZE(tsv as *const c_char) as Size) as TSVector;

    /* This count must be correct because STRPTR(tsout) relies on it. */
    (*tsout).size = (*tsv).size - indices_count;

    /*
     * Copy tsv to tsout, skipping lexemes listed in indices_to_delete.
     */
    arrout = ARRPTR(tsout);
    dataout = STRPTR(tsout);
    curoff = 0;
    i = 0;
    j = 0;
    k = 0;
    while i < (*tsv).size {
        /*
         * If current i is present in indices_to_delete, skip this lexeme.
         * Since indices_to_delete is already sorted, we only need to check
         * the current (k'th) entry.
         */
        if k < indices_count && i == *indices_to_delete.add(k as usize) {
            k += 1;
            i += 1;
            continue;
        }

        /* Copy lexeme and its positions and weights */
        memcpy(
            dataout.add(curoff as usize) as *mut c_void,
            data.add((*arrin.add(i as usize)).pos() as usize) as *const c_void,
            (*arrin.add(i as usize)).len() as usize,
        );
        (*arrout.add(j as usize)).set_haspos((*arrin.add(i as usize)).haspos());
        (*arrout.add(j as usize)).set_len((*arrin.add(i as usize)).len());
        (*arrout.add(j as usize)).set_pos(curoff as u32);
        curoff += (*arrin.add(i as usize)).len() as c_int;
        if (*arrin.add(i as usize)).haspos() != 0 {
            let len: c_int = POSDATALEN(tsv, arrin.add(i as usize))
                * core::mem::size_of::<WordEntryPos>() as c_int
                + core::mem::size_of::<uint16>() as c_int;

            curoff = SHORTALIGN(curoff as usize) as c_int;
            memcpy(
                dataout.add(curoff as usize) as *mut c_void,
                STRPTR(tsv).add(SHORTALIGN(
                    ((*arrin.add(i as usize)).pos() + (*arrin.add(i as usize)).len()) as usize,
                )) as *const c_void,
                len as usize,
            );
            curoff += len;
        }

        j += 1;
        i += 1;
    }

    /*
     * k should now be exactly equal to indices_count. If it isn't then the
     * caller provided us with indices outside of [0, tsv->size) range and
     * estimation of tsout's size is wrong.
     */
    Assert!(k == indices_count);

    SET_VARSIZE(tsout as *mut c_char, CALCDATASIZE((*tsout).size, curoff) as c_int);
    tsout
}

/*
 * Delete given lexeme from tsvector.
 * Implementation of user-level ts_delete(tsvector, text).
 *
 * TODO(pg-port): needs tsvector_delete_by_indices (qunique), stubbed above.
 */
pub unsafe fn tsvector_delete_str(fcinfo: FunctionCallInfo) -> Datum {
    let tsin: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let tsout: TSVector;
    let tlexeme: *mut text = DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 1)) as *mut text;
    let lexeme: *mut c_char = VARDATA_ANY(tlexeme as *const c_char);
    let lexeme_len: c_int = VARSIZE_ANY_EXHDR(tlexeme as *const c_char) as c_int;
    let mut skip_index: c_int;

    skip_index = tsvector_bsearch(tsin, lexeme, lexeme_len);
    if skip_index == -1 {
        PG_RETURN_POINTER!(tsin);
    }

    tsout = tsvector_delete_by_indices(tsin, &mut skip_index, 1);

    PG_FREE_IF_COPY!(fcinfo, tsin, 0);
    PG_FREE_IF_COPY!(fcinfo, tlexeme, 1);
    PG_RETURN_POINTER!(tsout)
}

/*
 * Delete given array of lexemes from tsvector.
 * Implementation of user-level ts_delete(tsvector, text[]).
 *
 * TODO(pg-port): needs utils/array.h deconstruct_array_builtin + qunique.
 */
pub unsafe fn tsvector_delete_arr(fcinfo: FunctionCallInfo) -> Datum {
    let tsin: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let tsout: TSVector;
    let lexemes: *mut c_void = PG_GETARG_ARRAYTYPE_P(fcinfo, 1);
    let mut i: c_int;
    let mut nlex: c_int = 0;
    let mut skip_count: c_int;
    let skip_indices: *mut c_int;
    let mut dlexemes: *mut Datum = null_mut();
    let mut nulls: *mut bool = null_mut();

    deconstruct_array_builtin(lexemes, TEXTOID, &mut dlexemes, &mut nulls, &mut nlex);

    /*
     * In typical use case array of lexemes to delete is relatively small. So
     * here we optimize things for that scenario: iterate through lexarr
     * performing binary search of each lexeme from lexarr in tsvector.
     */
    skip_indices = palloc0(nlex as usize * core::mem::size_of::<c_int>()) as *mut c_int;
    i = 0;
    skip_count = 0;
    while i < nlex {
        let lex: *mut c_char;
        let lex_len: c_int;
        let lex_pos: c_int;

        /* Ignore null array elements, they surely don't match */
        if *nulls.add(i as usize) {
            i += 1;
            continue;
        }

        lex = VARDATA(DatumGetPointer(*dlexemes.add(i as usize)) as *const c_char);
        lex_len = VARSIZE(DatumGetPointer(*dlexemes.add(i as usize)) as *const c_char) as c_int
            - VARHDRSZ;
        lex_pos = tsvector_bsearch(tsin, lex, lex_len);

        if lex_pos >= 0 {
            *skip_indices.add(skip_count as usize) = lex_pos;
            skip_count += 1;
        }
        i += 1;
    }

    tsout = tsvector_delete_by_indices(tsin, skip_indices, skip_count);

    pfree(skip_indices as *mut c_void);
    PG_FREE_IF_COPY!(fcinfo, tsin, 0);
    PG_FREE_IF_COPY!(fcinfo, lexemes, 1);

    PG_RETURN_POINTER!(tsout)
}

/*
 * Expand tsvector as table with following columns:
 *     lexeme: lexeme text
 *     positions: integer array of lexeme positions
 *     weights: char array of weights corresponding to positions
 *
 * TODO(pg-port): set-returning function -> needs funcapi (SRF) + utils/array.h.
 */
pub unsafe fn tsvector_unnest(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let tsin: TSVector;

    if SRF_IS_FIRSTCALL!(fcinfo) {
        let oldcontext: MemoryContext;
        let mut tupdesc: TupleDesc;

        funcctx = SRF_FIRSTCALL_INIT!(fcinfo);
        oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(3);
        TupleDescInitEntry(tupdesc, 1 as AttrNumber, c"lexeme".as_ptr(), TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, 2 as AttrNumber, c"positions".as_ptr(), INT2ARRAYOID, -1, 0);
        TupleDescInitEntry(tupdesc, 3 as AttrNumber, c"weights".as_ptr(), TEXTARRAYOID, -1, 0);
        if get_call_result_type(fcinfo, null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
            elog!(ERROR, "return type must be a row type");
        }
        (*funcctx).tuple_desc = tupdesc;

        (*funcctx).user_fctx = PG_GETARG_TSVECTOR_COPY(fcinfo, 0) as *mut c_void;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP!(fcinfo);
    tsin = (*funcctx).user_fctx as TSVector;

    if ((*funcctx).call_cntr as c_int) < (*tsin).size {
        let arrin: *mut WordEntry = ARRPTR(tsin);
        let data: *mut c_char = STRPTR(tsin);
        let tuple: *mut c_void;
        let mut j: c_int;
        let i: c_int = (*funcctx).call_cntr as c_int;
        let mut nulls: [bool; 3] = [false, false, false];
        let mut values: [Datum; 3] = [0, 0, 0];

        values[0] = PointerGetDatum(cstring_to_text_with_len(
            data.add((*arrin.add(i as usize)).pos() as usize),
            (*arrin.add(i as usize)).len() as c_int,
        ) as *const c_void);

        if (*arrin.add(i as usize)).haspos() != 0 {
            let posv: *mut WordEntryPosVector;
            let positions: *mut Datum;
            let weights: *mut Datum;
            let mut weight: c_char;

            /*
             * Internally tsvector stores position and weight in the same
             * uint16 (2 bits for weight, 14 for position). Here we extract
             * that in two separate arrays.
             */
            posv = _POSVECPTR(tsin, arrin.add(i as usize));
            positions = palloc((*posv).npos as usize * core::mem::size_of::<Datum>()) as *mut Datum;
            weights = palloc((*posv).npos as usize * core::mem::size_of::<Datum>()) as *mut Datum;
            j = 0;
            while j < (*posv).npos as c_int {
                *positions.add(j as usize) =
                    Int16GetDatum(WEP_GETPOS(*(*posv).pos.as_ptr().add(j as usize)) as int16);
                weight = (b'D' as c_char) - WEP_GETWEIGHT(*(*posv).pos.as_ptr().add(j as usize)) as c_char;
                *weights.add(j as usize) =
                    PointerGetDatum(cstring_to_text_with_len(&weight, 1) as *const c_void);
                j += 1;
            }

            values[1] = PointerGetDatum(construct_array_builtin(
                positions,
                (*posv).npos as c_int,
                INT2OID,
            ));
            values[2] = PointerGetDatum(construct_array_builtin(
                weights,
                (*posv).npos as c_int,
                TEXTOID,
            ));
        } else {
            nulls[1] = true;
            nulls[2] = true;
        }

        tuple = heap_form_tuple((*funcctx).tuple_desc, values.as_mut_ptr(), nulls.as_mut_ptr());
        SRF_RETURN_NEXT!(fcinfo, funcctx, HeapTupleGetDatum(tuple));
    } else {
        SRF_RETURN_DONE!(fcinfo, funcctx);
    }
}

/*
 * Convert tsvector to array of lexemes.
 *
 * TODO(pg-port): needs utils/array.h construct_array_builtin (not yet ported).
 */
pub unsafe fn tsvector_to_array(fcinfo: FunctionCallInfo) -> Datum {
    let tsin: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let arrin: *mut WordEntry = ARRPTR(tsin);
    let elements: *mut Datum;
    let mut i: c_int;
    let array: *mut c_void;

    elements = palloc((*tsin).size as usize * core::mem::size_of::<Datum>()) as *mut Datum;

    i = 0;
    while i < (*tsin).size {
        *elements.add(i as usize) = PointerGetDatum(cstring_to_text_with_len(
            STRPTR(tsin).add((*arrin.add(i as usize)).pos() as usize),
            (*arrin.add(i as usize)).len() as c_int,
        ) as *const c_void);
        i += 1;
    }

    array = construct_array_builtin(elements, (*tsin).size, TEXTOID);

    pfree(elements as *mut c_void);
    PG_FREE_IF_COPY!(fcinfo, tsin, 0);
    PG_RETURN_POINTER!(array)
}

/*
 * Build tsvector from array of lexemes.
 *
 * TODO(pg-port): needs utils/array.h deconstruct_array_builtin + lib/qunique.h.
 */
pub unsafe fn array_to_tsvector(fcinfo: FunctionCallInfo) -> Datum {
    let v: *mut c_void = PG_GETARG_ARRAYTYPE_P(fcinfo, 0);
    let tsout: TSVector;
    let mut dlexemes: *mut Datum = null_mut();
    let arrout: *mut WordEntry;
    let mut nulls: *mut bool = null_mut();
    let mut nitems: c_int = 0;
    let mut i: c_int;
    let tslen: c_int;
    let mut datalen: c_int = 0;
    let mut cur: *mut c_char;

    deconstruct_array_builtin(v, TEXTOID, &mut dlexemes, &mut nulls, &mut nitems);

    /*
     * Reject nulls and zero length strings (maybe we should just ignore them,
     * instead?)
     */
    i = 0;
    while i < nitems {
        if *nulls.add(i as usize) {
            /* C also: errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED) */
            let _ = errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED);
            ereport!(ERROR, errmsg!("lexeme array may not contain nulls"));
        }

        if VARSIZE(DatumGetPointer(*dlexemes.add(i as usize)) as *const c_char) as c_int - VARHDRSZ
            == 0
        {
            /* C also: errcode(ERRCODE_ZERO_LENGTH_CHARACTER_STRING) */
            let _ = errcode(ERRCODE_ZERO_LENGTH_CHARACTER_STRING);
            ereport!(ERROR, errmsg!("lexeme array may not contain empty strings"));
        }
        i += 1;
    }

    /* Sort and de-dup, because this is required for a valid tsvector. */
    if nitems > 1 {
        qsort_datum_lexemes(dlexemes, nitems);
        nitems = qunique(
            dlexemes as *mut c_void,
            nitems as usize,
            core::mem::size_of::<Datum>(),
            compare_text_lexemes,
        ) as c_int;
    }

    /* Calculate space needed for surviving lexemes. */
    i = 0;
    while i < nitems {
        datalen +=
            VARSIZE(DatumGetPointer(*dlexemes.add(i as usize)) as *const c_char) as c_int - VARHDRSZ;
        i += 1;
    }
    tslen = CALCDATASIZE(nitems, datalen) as c_int;

    /* Allocate and fill tsvector. */
    tsout = palloc0(tslen as Size) as TSVector;
    SET_VARSIZE(tsout as *mut c_char, tslen);
    (*tsout).size = nitems;

    arrout = ARRPTR(tsout);
    cur = STRPTR(tsout);
    i = 0;
    while i < nitems {
        let lex: *mut c_char = VARDATA(DatumGetPointer(*dlexemes.add(i as usize)) as *const c_char);
        let lex_len: c_int =
            VARSIZE(DatumGetPointer(*dlexemes.add(i as usize)) as *const c_char) as c_int - VARHDRSZ;

        memcpy(cur as *mut c_void, lex as *const c_void, lex_len as usize);
        (*arrout.add(i as usize)).set_haspos(0);
        (*arrout.add(i as usize)).set_len(lex_len as u32);
        (*arrout.add(i as usize)).set_pos((cur as isize - STRPTR(tsout) as isize) as u32);
        cur = cur.add(lex_len as usize);
        i += 1;
    }

    PG_FREE_IF_COPY!(fcinfo, v, 0);
    PG_RETURN_POINTER!(tsout)
}

/*
 * ts_filter(): keep only lexemes with given weights in tsvector.
 *
 * TODO(pg-port): needs utils/array.h deconstruct_array_builtin (not yet ported).
 */
pub unsafe fn tsvector_filter(fcinfo: FunctionCallInfo) -> Datum {
    let tsin: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let tsout: TSVector;
    let weights: *mut c_void = PG_GETARG_ARRAYTYPE_P(fcinfo, 1);
    let arrin: *mut WordEntry = ARRPTR(tsin);
    let arrout: *mut WordEntry;
    let datain: *mut c_char = STRPTR(tsin);
    let dataout: *mut c_char;
    let mut dweights: *mut Datum = null_mut();
    let mut nulls: *mut bool = null_mut();
    let mut nweights: c_int = 0;
    let mut i: c_int;
    let mut j: c_int;
    let mut cur_pos: c_int = 0;
    let mut mask: c_char = 0;

    deconstruct_array_builtin(weights, CHAROID, &mut dweights, &mut nulls, &mut nweights);

    i = 0;
    while i < nweights {
        let char_weight: c_char;

        if *nulls.add(i as usize) {
            /* C also: errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED) */
            let _ = errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED);
            ereport!(ERROR, errmsg!("weight array may not contain nulls"));
        }

        char_weight = DatumGetChar(*dweights.add(i as usize));
        match char_weight as u8 {
            b'A' | b'a' => mask |= 8,
            b'B' | b'b' => mask |= 4,
            b'C' | b'c' => mask |= 2,
            b'D' | b'd' => mask |= 1,
            _ => {
                /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
                let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
                ereport!(
                    ERROR,
                    errmsg!("unrecognized weight: \"{}\"", char_weight as u8 as char)
                );
            }
        }
        i += 1;
    }

    tsout = palloc0(VARSIZE(tsin as *const c_char) as Size) as TSVector;
    (*tsout).size = (*tsin).size;
    arrout = ARRPTR(tsout);
    dataout = STRPTR(tsout);

    i = 0;
    j = 0;
    while i < (*tsin).size {
        let posvin: *mut WordEntryPosVector;
        let posvout: *mut WordEntryPosVector;
        let mut npos: c_int = 0;
        let mut k: c_int;

        if (*arrin.add(i as usize)).haspos() == 0 {
            i += 1;
            continue;
        }

        posvin = _POSVECPTR(tsin, arrin.add(i as usize));
        posvout = dataout
            .add(SHORTALIGN((cur_pos + (*arrin.add(i as usize)).len() as c_int) as usize))
            as *mut WordEntryPosVector;

        k = 0;
        while k < (*posvin).npos as c_int {
            if (mask & (1 << WEP_GETWEIGHT(*(*posvin).pos.as_ptr().add(k as usize)))) != 0 {
                *(*posvout).pos.as_mut_ptr().add(npos as usize) =
                    *(*posvin).pos.as_ptr().add(k as usize);
                npos += 1;
            }
            k += 1;
        }

        /* if no satisfactory positions found, skip lexeme */
        if npos == 0 {
            i += 1;
            continue;
        }

        (*arrout.add(j as usize)).set_haspos(1);
        (*arrout.add(j as usize)).set_len((*arrin.add(i as usize)).len());
        (*arrout.add(j as usize)).set_pos(cur_pos as u32);

        memcpy(
            dataout.add(cur_pos as usize) as *mut c_void,
            datain.add((*arrin.add(i as usize)).pos() as usize) as *const c_void,
            (*arrin.add(i as usize)).len() as usize,
        );
        (*posvout).npos = npos as uint16;
        cur_pos += SHORTALIGN((*arrin.add(i as usize)).len() as usize) as c_int;
        cur_pos += POSDATALEN(tsout, arrout.add(j as usize))
            * core::mem::size_of::<WordEntryPos>() as c_int
            + core::mem::size_of::<uint16>() as c_int;
        j += 1;
        i += 1;
    }

    (*tsout).size = j;
    if dataout != STRPTR(tsout) {
        memmove(
            STRPTR(tsout) as *mut c_void,
            dataout as *const c_void,
            cur_pos as usize,
        );
    }

    SET_VARSIZE(tsout as *mut c_char, CALCDATASIZE((*tsout).size, cur_pos) as c_int);

    PG_FREE_IF_COPY!(fcinfo, tsin, 0);
    PG_RETURN_POINTER!(tsout)
}

pub unsafe fn tsvector_concat(fcinfo: FunctionCallInfo) -> Datum {
    let in1: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let in2: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 1));
    let out: TSVector;
    let mut ptr: *mut WordEntry;
    let mut ptr1: *mut WordEntry;
    let mut ptr2: *mut WordEntry;
    let mut p: *mut WordEntryPos;
    let mut maxpos: c_int = 0;
    let mut i: c_int;
    let mut j: c_int;
    let mut i1: c_int;
    let mut i2: c_int;
    let mut dataoff: c_int;
    let mut output_bytes: c_int;
    let output_size: c_int;
    let data: *mut c_char;
    let data1: *mut c_char;
    let data2: *mut c_char;

    /* Get max position in in1; we'll need this to offset in2's positions */
    ptr = ARRPTR(in1);
    i = (*in1).size;
    while {
        let old = i;
        i -= 1;
        old != 0
    } {
        j = POSDATALEN(in1, ptr);
        if j != 0 {
            p = POSDATAPTR(in1, ptr);
            while {
                let old = j;
                j -= 1;
                old != 0
            } {
                if WEP_GETPOS(*p) > maxpos {
                    maxpos = WEP_GETPOS(*p);
                }
                p = p.add(1);
            }
        }
        ptr = ptr.add(1);
    }

    ptr1 = ARRPTR(in1);
    ptr2 = ARRPTR(in2);
    data1 = STRPTR(in1);
    data2 = STRPTR(in2);
    i1 = (*in1).size;
    i2 = (*in2).size;

    /*
     * Conservative estimate of space needed.  We might need all the data in
     * both inputs, and conceivably add a pad byte before position data for
     * each item where there was none before.
     */
    output_bytes =
        VARSIZE(in1 as *const c_char) as c_int + VARSIZE(in2 as *const c_char) as c_int + i1 + i2;

    out = palloc0(output_bytes as Size) as TSVector;
    SET_VARSIZE(out as *mut c_char, output_bytes);

    /*
     * We must make out->size valid so that STRPTR(out) is sensible.  We'll
     * collapse out any unused space at the end.
     */
    (*out).size = (*in1).size + (*in2).size;

    ptr = ARRPTR(out);
    data = STRPTR(out);
    dataoff = 0;
    while i1 != 0 && i2 != 0 {
        let cmp: c_int = compareEntry(data1, ptr1, data2, ptr2);

        if cmp < 0 {
            /* in1 first */
            (*ptr).set_haspos((*ptr1).haspos());
            (*ptr).set_len((*ptr1).len());
            memcpy(
                data.add(dataoff as usize) as *mut c_void,
                data1.add((*ptr1).pos() as usize) as *const c_void,
                (*ptr1).len() as usize,
            );
            (*ptr).set_pos(dataoff as u32);
            dataoff += (*ptr1).len() as c_int;
            if (*ptr).haspos() != 0 {
                dataoff = SHORTALIGN(dataoff as usize) as c_int;
                memcpy(
                    data.add(dataoff as usize) as *mut c_void,
                    _POSVECPTR(in1, ptr1) as *const c_void,
                    POSDATALEN(in1, ptr1) as usize * core::mem::size_of::<WordEntryPos>()
                        + core::mem::size_of::<uint16>(),
                );
                dataoff += POSDATALEN(in1, ptr1) * core::mem::size_of::<WordEntryPos>() as c_int
                    + core::mem::size_of::<uint16>() as c_int;
            }

            ptr = ptr.add(1);
            ptr1 = ptr1.add(1);
            i1 -= 1;
        } else if cmp > 0 {
            /* in2 first */
            (*ptr).set_haspos((*ptr2).haspos());
            (*ptr).set_len((*ptr2).len());
            memcpy(
                data.add(dataoff as usize) as *mut c_void,
                data2.add((*ptr2).pos() as usize) as *const c_void,
                (*ptr2).len() as usize,
            );
            (*ptr).set_pos(dataoff as u32);
            dataoff += (*ptr2).len() as c_int;
            if (*ptr).haspos() != 0 {
                let addlen: c_int = add_pos(in2, ptr2, out, ptr, maxpos);

                if addlen == 0 {
                    (*ptr).set_haspos(0);
                } else {
                    dataoff = SHORTALIGN(dataoff as usize) as c_int;
                    dataoff += addlen * core::mem::size_of::<WordEntryPos>() as c_int
                        + core::mem::size_of::<uint16>() as c_int;
                }
            }

            ptr = ptr.add(1);
            ptr2 = ptr2.add(1);
            i2 -= 1;
        } else {
            (*ptr).set_haspos((*ptr1).haspos() | (*ptr2).haspos());
            (*ptr).set_len((*ptr1).len());
            memcpy(
                data.add(dataoff as usize) as *mut c_void,
                data1.add((*ptr1).pos() as usize) as *const c_void,
                (*ptr1).len() as usize,
            );
            (*ptr).set_pos(dataoff as u32);
            dataoff += (*ptr1).len() as c_int;
            if (*ptr).haspos() != 0 {
                if (*ptr1).haspos() != 0 {
                    dataoff = SHORTALIGN(dataoff as usize) as c_int;
                    memcpy(
                        data.add(dataoff as usize) as *mut c_void,
                        _POSVECPTR(in1, ptr1) as *const c_void,
                        POSDATALEN(in1, ptr1) as usize * core::mem::size_of::<WordEntryPos>()
                            + core::mem::size_of::<uint16>(),
                    );
                    dataoff += POSDATALEN(in1, ptr1) * core::mem::size_of::<WordEntryPos>() as c_int
                        + core::mem::size_of::<uint16>() as c_int;
                    if (*ptr2).haspos() != 0 {
                        dataoff += add_pos(in2, ptr2, out, ptr, maxpos)
                            * core::mem::size_of::<WordEntryPos>() as c_int;
                    }
                } else {
                    /* must have ptr2->haspos */
                    let addlen: c_int = add_pos(in2, ptr2, out, ptr, maxpos);

                    if addlen == 0 {
                        (*ptr).set_haspos(0);
                    } else {
                        dataoff = SHORTALIGN(dataoff as usize) as c_int;
                        dataoff += addlen * core::mem::size_of::<WordEntryPos>() as c_int
                            + core::mem::size_of::<uint16>() as c_int;
                    }
                }
            }

            ptr = ptr.add(1);
            ptr1 = ptr1.add(1);
            ptr2 = ptr2.add(1);
            i1 -= 1;
            i2 -= 1;
        }
    }

    while i1 != 0 {
        (*ptr).set_haspos((*ptr1).haspos());
        (*ptr).set_len((*ptr1).len());
        memcpy(
            data.add(dataoff as usize) as *mut c_void,
            data1.add((*ptr1).pos() as usize) as *const c_void,
            (*ptr1).len() as usize,
        );
        (*ptr).set_pos(dataoff as u32);
        dataoff += (*ptr1).len() as c_int;
        if (*ptr).haspos() != 0 {
            dataoff = SHORTALIGN(dataoff as usize) as c_int;
            memcpy(
                data.add(dataoff as usize) as *mut c_void,
                _POSVECPTR(in1, ptr1) as *const c_void,
                POSDATALEN(in1, ptr1) as usize * core::mem::size_of::<WordEntryPos>()
                    + core::mem::size_of::<uint16>(),
            );
            dataoff += POSDATALEN(in1, ptr1) * core::mem::size_of::<WordEntryPos>() as c_int
                + core::mem::size_of::<uint16>() as c_int;
        }

        ptr = ptr.add(1);
        ptr1 = ptr1.add(1);
        i1 -= 1;
    }

    while i2 != 0 {
        (*ptr).set_haspos((*ptr2).haspos());
        (*ptr).set_len((*ptr2).len());
        memcpy(
            data.add(dataoff as usize) as *mut c_void,
            data2.add((*ptr2).pos() as usize) as *const c_void,
            (*ptr2).len() as usize,
        );
        (*ptr).set_pos(dataoff as u32);
        dataoff += (*ptr2).len() as c_int;
        if (*ptr).haspos() != 0 {
            let addlen: c_int = add_pos(in2, ptr2, out, ptr, maxpos);

            if addlen == 0 {
                (*ptr).set_haspos(0);
            } else {
                dataoff = SHORTALIGN(dataoff as usize) as c_int;
                dataoff += addlen * core::mem::size_of::<WordEntryPos>() as c_int
                    + core::mem::size_of::<uint16>() as c_int;
            }
        }

        ptr = ptr.add(1);
        ptr2 = ptr2.add(1);
        i2 -= 1;
    }

    /*
     * Instead of checking each offset individually, we check for overflow of
     * pos fields once at the end.
     */
    if dataoff > MAXSTRPOS {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!(
                "string is too long for tsvector ({} bytes, max {} bytes)",
                dataoff,
                MAXSTRPOS
            )
        );
    }

    /*
     * Adjust sizes (asserting that we didn't overrun the original estimates)
     * and collapse out any unused array entries.
     */
    output_size =
        ((ptr as isize - ARRPTR(out) as isize) / core::mem::size_of::<WordEntry>() as isize) as c_int;
    Assert!(output_size <= (*out).size);
    (*out).size = output_size;
    if data != STRPTR(out) {
        memmove(
            STRPTR(out) as *mut c_void,
            data as *const c_void,
            dataoff as usize,
        );
    }
    output_bytes = CALCDATASIZE((*out).size, dataoff) as c_int;
    Assert!(output_bytes <= VARSIZE(out as *const c_char) as c_int);
    SET_VARSIZE(out as *mut c_char, output_bytes);

    PG_FREE_IF_COPY!(fcinfo, in1, 0);
    PG_FREE_IF_COPY!(fcinfo, in2, 1);
    PG_RETURN_POINTER!(out)
}

// ================================================================
//   tsquery match (@@) engine
// ================================================================
//
// The TSQuery type (QueryItem / QueryOperand / GETQUERY / GETOPERAND / OP_*) is
// supplied by the sibling crate::utils::adt::tsquery_util (module-local copies
// pending tsquery.c).  ExecPhraseData / TSExecuteCallback / TSTernaryValue /
// the TS_EXEC_* flags are declared near the top of this file.

/* CHECK_FOR_INTERRUPTS(): no-op pending the signal/interrupt machinery. */
#[inline]
fn CHECK_FOR_INTERRUPTS() {}

/*
 * Check weight info or/and fill 'data' with the required positions.
 */
unsafe fn checkclass_str(
    chkval: *mut CHKVAL,
    entry: *mut WordEntry,
    val: *mut QueryOperand,
    data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let mut result: TSTernaryValue = TS_NO;

    Assert!(data.is_null() || (*data).npos == 0);

    if (*entry).haspos() != 0 {
        /*
         * We can't use the _POSVECPTR macro here because the pointer to the
         * tsvector's lexeme storage is already contained in chkval->values.
         */
        let posvec: *mut WordEntryPosVector = (*chkval)
            .values
            .add(SHORTALIGN(((*entry).pos() + (*entry).len()) as usize))
            as *mut WordEntryPosVector;
        let posvec_pos: *mut WordEntryPos = (*posvec).pos.as_mut_ptr();
        let posvec_npos: c_int = (*posvec).npos as c_int;

        if (*val).weight != 0 && !data.is_null() {
            let mut posvec_iter: *mut WordEntryPos = posvec_pos;
            let mut dptr: *mut WordEntryPos;

            /*
             * Filter position information by weights
             */
            (*data).pos =
                palloc(core::mem::size_of::<WordEntryPos>() * posvec_npos as usize) as *mut WordEntryPos;
            dptr = (*data).pos;
            (*data).allocated = true;

            /* Is there a position with a matching weight? */
            while posvec_iter < posvec_pos.add(posvec_npos as usize) {
                /* If true, append this position to the data->pos */
                if ((*val).weight & (1 << WEP_GETWEIGHT(*posvec_iter))) != 0 {
                    *dptr = WEP_GETPOS(*posvec_iter) as WordEntryPos;
                    dptr = dptr.add(1);
                }
                posvec_iter = posvec_iter.add(1);
            }

            (*data).npos = (dptr as isize - (*data).pos as isize) as c_int
                / core::mem::size_of::<WordEntryPos>() as c_int;

            if (*data).npos > 0 {
                result = TS_YES;
            } else {
                pfree((*data).pos as *mut c_void);
                (*data).pos = null_mut();
                (*data).allocated = false;
            }
        } else if (*val).weight != 0 {
            let mut posvec_iter: *mut WordEntryPos = posvec_pos;

            /* Is there a position with a matching weight? */
            while posvec_iter < posvec_pos.add(posvec_npos as usize) {
                if ((*val).weight & (1 << WEP_GETWEIGHT(*posvec_iter))) != 0 {
                    result = TS_YES;
                    break; /* no need to go further */
                }
                posvec_iter = posvec_iter.add(1);
            }
        } else if !data.is_null() {
            (*data).npos = posvec_npos;
            (*data).pos = posvec_pos;
            (*data).allocated = false;
            result = TS_YES;
        } else {
            /* simplest case: no weight check, positions not needed */
            result = TS_YES;
        }
    } else {
        /*
         * Position info is lacking, so if the caller requires it, we can only
         * say that maybe there is a match.
         */
        if !data.is_null() {
            result = TS_MAYBE;
        } else {
            result = TS_YES;
        }
    }

    result
}

/*
 * TS_execute callback for matching a tsquery operand to plain tsvector data.
 */
unsafe fn checkcondition_str(
    checkval: *mut c_void,
    val: *mut QueryOperand,
    data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let chkval: *mut CHKVAL = checkval as *mut CHKVAL;
    let mut StopLow: *mut WordEntry = (*chkval).arrb;
    let mut StopHigh: *mut WordEntry = (*chkval).arre;
    let mut StopMiddle: *mut WordEntry = StopHigh;
    let mut res: TSTernaryValue = TS_NO;

    /* Loop invariant: StopLow <= val < StopHigh */
    while StopLow < StopHigh {
        let difference: c_int;

        StopMiddle = StopLow.add(
            ((StopHigh as isize - StopLow as isize)
                / core::mem::size_of::<WordEntry>() as isize / 2) as usize,
        );
        difference = tsCompareString(
            (*chkval).operand.add((*val).distance() as usize),
            (*val).length() as c_int,
            (*chkval).values.add((*StopMiddle).pos() as usize),
            (*StopMiddle).len() as c_int,
            false,
        );

        if difference == 0 {
            /* Check weight info & fill 'data' with positions */
            res = checkclass_str(chkval, StopMiddle, val, data);
            break;
        } else if difference > 0 {
            StopLow = StopMiddle.add(1);
        } else {
            StopHigh = StopMiddle;
        }
    }

    /*
     * If it's a prefix search, we should also consider lexemes that the
     * search term is a prefix of.
     */
    if (*val).prefix && (res != TS_YES || !data.is_null()) {
        let mut allpos: *mut WordEntryPos = null_mut();
        let mut npos: c_int = 0;
        let mut totalpos: c_int = 0;

        /* adjust start position for corner case */
        if StopLow >= StopHigh {
            StopMiddle = StopHigh;
        }

        /* we don't try to re-use any data from the initial match */
        if !data.is_null() {
            if (*data).allocated {
                pfree((*data).pos as *mut c_void);
            }
            (*data).pos = null_mut();
            (*data).allocated = false;
            (*data).npos = 0;
        }
        res = TS_NO;

        while (res != TS_YES || !data.is_null())
            && StopMiddle < (*chkval).arre
            && tsCompareString(
                (*chkval).operand.add((*val).distance() as usize),
                (*val).length() as c_int,
                (*chkval).values.add((*StopMiddle).pos() as usize),
                (*StopMiddle).len() as c_int,
                true,
            ) == 0
        {
            let subres: TSTernaryValue = checkclass_str(chkval, StopMiddle, val, data);

            if subres != TS_NO {
                if !data.is_null() {
                    /*
                     * We need to join position information
                     */
                    if subres == TS_MAYBE {
                        res = TS_MAYBE;
                        npos = 0;
                        if !allpos.is_null() {
                            pfree(allpos as *mut c_void);
                        }
                        break;
                    }

                    while npos + (*data).npos > totalpos {
                        if totalpos == 0 {
                            totalpos = 256;
                            allpos = palloc(
                                core::mem::size_of::<WordEntryPos>() * totalpos as usize,
                            ) as *mut WordEntryPos;
                        } else {
                            totalpos *= 2;
                            allpos = repalloc(
                                allpos as *mut c_void,
                                core::mem::size_of::<WordEntryPos>() * totalpos as usize,
                            ) as *mut WordEntryPos;
                        }
                    }

                    memcpy(
                        allpos.add(npos as usize) as *mut c_void,
                        (*data).pos as *const c_void,
                        core::mem::size_of::<WordEntryPos>() * (*data).npos as usize,
                    );
                    npos += (*data).npos;

                    /* don't leak storage from individual matches */
                    if (*data).allocated {
                        pfree((*data).pos as *mut c_void);
                    }
                    (*data).pos = null_mut();
                    (*data).allocated = false;
                    /* it's important to reset data->npos before next loop */
                    (*data).npos = 0;
                } else {
                    /* Don't need positions, just handle YES/MAYBE */
                    if subres == TS_YES || res == TS_NO {
                        res = subres;
                    }
                }
            }

            StopMiddle = StopMiddle.add(1);
        }

        if !data.is_null() && npos > 0 {
            /* Sort and make unique array of found positions */
            (*data).pos = allpos;
            qsort_wep(allpos, npos);
            (*data).npos = qunique_wep(allpos, npos);
            (*data).allocated = true;
            res = TS_YES;
        }
    }

    res
}

/* qsort(pos, npos, sizeof(WordEntryPos), compareWordEntryPos) */
unsafe fn qsort_wep(pos: *mut WordEntryPos, npos: c_int) {
    let sl = core::slice::from_raw_parts_mut(pos, npos as usize);
    sl.sort_by(|x, y| {
        let r = compareWordEntryPos(
            x as *const WordEntryPos as *const c_void,
            y as *const WordEntryPos as *const c_void,
        );
        r.cmp(&0)
    });
}

/*
 * qunique(pos, npos, sizeof(WordEntryPos), compareWordEntryPos): remove adjacent
 * duplicates from a sorted array, returning the new length.  lib/qunique.h is not
 * yet ported, so the (tiny) algorithm is inlined here.
 */
unsafe fn qunique_wep(pos: *mut WordEntryPos, npos: c_int) -> c_int {
    if npos <= 1 {
        return npos;
    }
    let mut last: c_int = 0;
    let mut i: c_int = 1;
    while i < npos {
        if compareWordEntryPos(
            pos.add(i as usize) as *const c_void,
            pos.add(last as usize) as *const c_void,
        ) != 0
        {
            last += 1;
            if last != i {
                *pos.add(last as usize) = *pos.add(i as usize);
            }
        }
        i += 1;
    }
    last + 1
}

/*
 * Compute output position list for a tsquery operator in phrase mode.
 */
const TSPO_L_ONLY: c_int = 0x01; /* emit positions appearing only in L */
const TSPO_R_ONLY: c_int = 0x02; /* emit positions appearing only in R */
const TSPO_BOTH: c_int = 0x04; /* emit positions appearing in both L&R */

unsafe fn TS_phrase_output(
    data: *mut ExecPhraseData,
    Ldata: *mut ExecPhraseData,
    Rdata: *mut ExecPhraseData,
    emit: c_int,
    Loffset: c_int,
    Roffset: c_int,
    max_npos: c_int,
) -> TSTernaryValue {
    let mut Lindex: c_int;
    let mut Rindex: c_int;

    /* Loop until both inputs are exhausted */
    Lindex = 0;
    Rindex = 0;
    while Lindex < (*Ldata).npos || Rindex < (*Rdata).npos {
        let Lpos: c_int;
        let Rpos: c_int;
        let mut output_pos: c_int = 0;

        if Lindex < (*Ldata).npos {
            Lpos = WEP_GETPOS(*(*Ldata).pos.add(Lindex as usize)) + Loffset;
        } else {
            /* L array exhausted, so we're done if R_ONLY isn't set */
            if (emit & TSPO_R_ONLY) == 0 {
                break;
            }
            Lpos = INT_MAX;
        }
        if Rindex < (*Rdata).npos {
            Rpos = WEP_GETPOS(*(*Rdata).pos.add(Rindex as usize)) + Roffset;
        } else {
            /* R array exhausted, so we're done if L_ONLY isn't set */
            if (emit & TSPO_L_ONLY) == 0 {
                break;
            }
            Rpos = INT_MAX;
        }

        /* Merge-join the two input lists */
        if Lpos < Rpos {
            if (emit & TSPO_L_ONLY) != 0 {
                output_pos = Lpos;
            }
            Lindex += 1;
        } else if Lpos == Rpos {
            if (emit & TSPO_BOTH) != 0 {
                output_pos = Rpos;
            }
            Lindex += 1;
            Rindex += 1;
        } else {
            /* Lpos > Rpos */
            if (emit & TSPO_R_ONLY) != 0 {
                output_pos = Rpos;
            }
            Rindex += 1;
        }

        if output_pos > 0 {
            if !data.is_null() {
                /* Store position, first allocating output array if needed */
                if (*data).pos.is_null() {
                    (*data).pos = palloc(
                        max_npos as usize * core::mem::size_of::<WordEntryPos>(),
                    ) as *mut WordEntryPos;
                    (*data).allocated = true;
                }
                *(*data).pos.add((*data).npos as usize) = output_pos as WordEntryPos;
                (*data).npos += 1;
            } else {
                /* Exact positions not needed, return TS_YES at first hit. */
                return TS_YES;
            }
        }
    }

    if !data.is_null() && (*data).npos > 0 {
        Assert!((*data).npos <= max_npos);
        return TS_YES;
    }
    TS_NO
}

/*
 * Execute tsquery at or below an OP_PHRASE operator.
 */
unsafe fn TS_phrase_execute(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    flags: uint32,
    chkcond: TSExecuteCallback,
    data: *mut ExecPhraseData,
) -> TSTernaryValue {
    let mut Ldata: ExecPhraseData;
    let mut Rdata: ExecPhraseData;
    let lmatch: TSTernaryValue;
    let rmatch: TSTernaryValue;
    let Loffset: c_int;
    let Roffset: c_int;
    let maxwidth: c_int;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /* ... and let's check for query cancel while we're at it */
    CHECK_FOR_INTERRUPTS();

    if (*curitem).type_() == QI_VAL {
        return chkcond(arg, curitem as *mut QueryOperand, data);
    }

    match (*curitem).qoperator.oper {
        OP_NOT => {
            if (flags & TS_EXEC_SKIP_NOT) != 0 {
                /* with SKIP_NOT, report NOT as "match everywhere" */
                Assert!((*data).npos == 0 && !(*data).negate);
                (*data).negate = true;
                return TS_YES;
            }
            match TS_phrase_execute(curitem.add(1), arg, flags, chkcond, data) {
                TS_NO => {
                    /* change "match nowhere" to "match everywhere" */
                    Assert!((*data).npos == 0 && !(*data).negate);
                    (*data).negate = true;
                    return TS_YES;
                }
                TS_YES => {
                    if (*data).npos > 0 {
                        /* we have some positions, invert negate flag */
                        (*data).negate = !(*data).negate;
                        return TS_YES;
                    } else if (*data).negate {
                        /* change "match everywhere" to "match nowhere" */
                        (*data).negate = false;
                        return TS_NO;
                    }
                    /* Should not get here if result was TS_YES */
                    Assert!(false);
                }
                TS_MAYBE => {
                    /* match positions are, and remain, uncertain */
                    return TS_MAYBE;
                }
            }
        }

        OP_PHRASE | OP_AND => {
            Ldata = ExecPhraseData::zeroed();
            Rdata = ExecPhraseData::zeroed();

            lmatch = TS_phrase_execute(
                curitem.add((*curitem).qoperator.left as usize),
                arg,
                flags,
                chkcond,
                &mut Ldata,
            );
            if lmatch == TS_NO {
                return TS_NO;
            }

            rmatch = TS_phrase_execute(curitem.add(1), arg, flags, chkcond, &mut Rdata);
            if rmatch == TS_NO {
                return TS_NO;
            }

            if lmatch == TS_MAYBE || rmatch == TS_MAYBE {
                return TS_MAYBE;
            }

            if (*curitem).qoperator.oper == OP_PHRASE {
                Loffset = (*curitem).qoperator.distance as c_int + Rdata.width;
                Roffset = 0;
                if !data.is_null() {
                    (*data).width =
                        (*curitem).qoperator.distance as c_int + Ldata.width + Rdata.width;
                }
            } else {
                maxwidth = core::cmp::max(Ldata.width, Rdata.width);
                Loffset = maxwidth - Ldata.width;
                Roffset = maxwidth - Rdata.width;
                if !data.is_null() {
                    (*data).width = maxwidth;
                }
            }

            if Ldata.negate && Rdata.negate {
                /* !L & !R: treat as !(L | R) */
                let _ = TS_phrase_output(
                    data,
                    &mut Ldata,
                    &mut Rdata,
                    TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                    Loffset,
                    Roffset,
                    Ldata.npos + Rdata.npos,
                );
                if !data.is_null() {
                    (*data).negate = true;
                }
                return TS_YES;
            } else if Ldata.negate {
                /* !L & R */
                return TS_phrase_output(
                    data, &mut Ldata, &mut Rdata, TSPO_R_ONLY, Loffset, Roffset, Rdata.npos,
                );
            } else if Rdata.negate {
                /* L & !R */
                return TS_phrase_output(
                    data, &mut Ldata, &mut Rdata, TSPO_L_ONLY, Loffset, Roffset, Ldata.npos,
                );
            } else {
                /* straight AND */
                return TS_phrase_output(
                    data,
                    &mut Ldata,
                    &mut Rdata,
                    TSPO_BOTH,
                    Loffset,
                    Roffset,
                    core::cmp::min(Ldata.npos, Rdata.npos),
                );
            }
        }

        OP_OR => {
            Ldata = ExecPhraseData::zeroed();
            Rdata = ExecPhraseData::zeroed();

            lmatch = TS_phrase_execute(
                curitem.add((*curitem).qoperator.left as usize),
                arg,
                flags,
                chkcond,
                &mut Ldata,
            );
            rmatch = TS_phrase_execute(curitem.add(1), arg, flags, chkcond, &mut Rdata);

            if lmatch == TS_NO && rmatch == TS_NO {
                return TS_NO;
            }

            if lmatch == TS_MAYBE || rmatch == TS_MAYBE {
                return TS_MAYBE;
            }

            /* Cope with undefined output width from failed submatch. */
            if lmatch == TS_NO {
                Ldata.width = 0;
            }
            if rmatch == TS_NO {
                Rdata.width = 0;
            }

            maxwidth = core::cmp::max(Ldata.width, Rdata.width);
            Loffset = maxwidth - Ldata.width;
            Roffset = maxwidth - Rdata.width;
            (*data).width = maxwidth;

            if Ldata.negate && Rdata.negate {
                /* !L | !R: treat as !(L & R) */
                let _ = TS_phrase_output(
                    data,
                    &mut Ldata,
                    &mut Rdata,
                    TSPO_BOTH,
                    Loffset,
                    Roffset,
                    core::cmp::min(Ldata.npos, Rdata.npos),
                );
                (*data).negate = true;
                return TS_YES;
            } else if Ldata.negate {
                /* !L | R: treat as !(L & !R) */
                let _ = TS_phrase_output(
                    data, &mut Ldata, &mut Rdata, TSPO_L_ONLY, Loffset, Roffset, Ldata.npos,
                );
                (*data).negate = true;
                return TS_YES;
            } else if Rdata.negate {
                /* L | !R: treat as !(!L & R) */
                let _ = TS_phrase_output(
                    data, &mut Ldata, &mut Rdata, TSPO_R_ONLY, Loffset, Roffset, Rdata.npos,
                );
                (*data).negate = true;
                return TS_YES;
            } else {
                /* straight OR */
                return TS_phrase_output(
                    data,
                    &mut Ldata,
                    &mut Rdata,
                    TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                    Loffset,
                    Roffset,
                    Ldata.npos + Rdata.npos,
                );
            }
        }

        other => {
            elog!(ERROR, "unrecognized operator: {}", other as c_int);
            unreachable!();
        }
    }

    /* not reachable, but keep compiler quiet */
    TS_NO
}

/*
 * Evaluate tsquery boolean expression.
 */
pub unsafe fn TS_execute(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    flags: uint32,
    chkcond: TSExecuteCallback,
) -> bool {
    /*
     * If we get TS_MAYBE from the recursion, return true.  We could only see
     * that result if the caller passed TS_EXEC_PHRASE_NO_POS.
     */
    TS_execute_recurse(curitem, arg, flags, chkcond) != TS_NO
}

/*
 * Evaluate tsquery boolean expression (TS_MAYBE returned as-is).
 */
pub unsafe fn TS_execute_ternary(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    flags: uint32,
    chkcond: TSExecuteCallback,
) -> TSTernaryValue {
    TS_execute_recurse(curitem, arg, flags, chkcond)
}

/*
 * TS_execute recursion for operators above any phrase operator.
 */
unsafe fn TS_execute_recurse(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    flags: uint32,
    chkcond: TSExecuteCallback,
) -> TSTernaryValue {
    let lmatch: TSTernaryValue;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /* ... and let's check for query cancel while we're at it */
    CHECK_FOR_INTERRUPTS();

    if (*curitem).type_() == QI_VAL {
        return chkcond(arg, curitem as *mut QueryOperand, null_mut() /* no pos info */);
    }

    match (*curitem).qoperator.oper {
        OP_NOT => {
            if (flags & TS_EXEC_SKIP_NOT) != 0 {
                return TS_YES;
            }
            match TS_execute_recurse(curitem.add(1), arg, flags, chkcond) {
                TS_NO => return TS_YES,
                TS_YES => return TS_NO,
                TS_MAYBE => return TS_MAYBE,
            }
        }

        OP_AND => {
            lmatch =
                TS_execute_recurse(curitem.add((*curitem).qoperator.left as usize), arg, flags, chkcond);
            if lmatch == TS_NO {
                return TS_NO;
            }
            match TS_execute_recurse(curitem.add(1), arg, flags, chkcond) {
                TS_NO => return TS_NO,
                TS_YES => return lmatch,
                TS_MAYBE => return TS_MAYBE,
            }
        }

        OP_OR => {
            lmatch =
                TS_execute_recurse(curitem.add((*curitem).qoperator.left as usize), arg, flags, chkcond);
            if lmatch == TS_YES {
                return TS_YES;
            }
            match TS_execute_recurse(curitem.add(1), arg, flags, chkcond) {
                TS_NO => return lmatch,
                TS_YES => return TS_YES,
                TS_MAYBE => return TS_MAYBE,
            }
        }

        OP_PHRASE => {
            match TS_phrase_execute(curitem, arg, flags, chkcond, null_mut()) {
                TS_NO => return TS_NO,
                TS_YES => return TS_YES,
                TS_MAYBE => {
                    return if (flags & TS_EXEC_PHRASE_NO_POS) != 0 {
                        TS_MAYBE
                    } else {
                        TS_NO
                    }
                }
            }
        }

        other => {
            elog!(ERROR, "unrecognized operator: {}", other as c_int);
            unreachable!();
        }
    }
}

/*
 * Evaluate tsquery and report locations of matching terms.
 *
 * On successful match, the result is a List of ExecPhraseData structs.
 */
pub unsafe fn TS_execute_locations(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    flags: uint32,
    chkcond: TSExecuteCallback,
) -> *mut crate::nodes::pg_list::List {
    let mut result: *mut crate::nodes::pg_list::List = NIL;

    /* No flags supported, as yet */
    Assert!(flags == TS_EXEC_EMPTY);
    if TS_execute_locations_recurse(curitem, arg, chkcond, &mut result) {
        return result;
    }
    NIL
}

/*
 * TS_execute_locations recursion for operators above any phrase operator.
 */
unsafe fn TS_execute_locations_recurse(
    curitem: *mut QueryItem,
    arg: *mut c_void,
    chkcond: TSExecuteCallback,
    locations: *mut *mut crate::nodes::pg_list::List,
) -> bool {
    let lmatch: bool;
    let rmatch: bool;
    let mut llocations: *mut crate::nodes::pg_list::List = NIL;
    let mut rlocations: *mut crate::nodes::pg_list::List = NIL;
    let data: *mut ExecPhraseData;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    /* ... and let's check for query cancel while we're at it */
    CHECK_FOR_INTERRUPTS();

    /* Default locations result is empty */
    *locations = NIL;

    if (*curitem).type_() == QI_VAL {
        let d = palloc0(core::mem::size_of::<ExecPhraseData>()) as *mut ExecPhraseData;
        if chkcond(arg, curitem as *mut QueryOperand, d) == TS_YES {
            *locations = list_make1!(d);
            return true;
        }
        pfree(d as *mut c_void);
        return false;
    }

    match (*curitem).qoperator.oper {
        OP_NOT => {
            if !TS_execute_locations_recurse(curitem.add(1), arg, chkcond, &mut llocations) {
                return true; /* we don't pass back any locations */
            }
            false
        }

        OP_AND => {
            if !TS_execute_locations_recurse(
                curitem.add((*curitem).qoperator.left as usize),
                arg,
                chkcond,
                &mut llocations,
            ) {
                return false;
            }
            if !TS_execute_locations_recurse(curitem.add(1), arg, chkcond, &mut rlocations) {
                return false;
            }
            *locations = list_concat(llocations, rlocations);
            true
        }

        OP_OR => {
            lmatch = TS_execute_locations_recurse(
                curitem.add((*curitem).qoperator.left as usize),
                arg,
                chkcond,
                &mut llocations,
            );
            rmatch = TS_execute_locations_recurse(curitem.add(1), arg, chkcond, &mut rlocations);
            if lmatch || rmatch {
                /*
                 * Generate an AND'able location struct from each combination of
                 * sub-matches (disjunctive law).
                 */
                if llocations == NIL {
                    *locations = rlocations;
                } else if rlocations == NIL {
                    *locations = llocations;
                } else {
                    foreach!(ll, llocations, {
                        let ldata = lfirst(current_cell!(ll)) as *mut ExecPhraseData;
                        foreach!(lr, rlocations, {
                            let rdata = lfirst(current_cell!(lr)) as *mut ExecPhraseData;
                            let d =
                                palloc0(core::mem::size_of::<ExecPhraseData>()) as *mut ExecPhraseData;
                            let _ = TS_phrase_output(
                                d,
                                ldata,
                                rdata,
                                TSPO_BOTH | TSPO_L_ONLY | TSPO_R_ONLY,
                                0,
                                0,
                                (*ldata).npos + (*rdata).npos,
                            );
                            /* Report the larger width, as explained above. */
                            (*d).width = core::cmp::max((*ldata).width, (*rdata).width);
                            *locations = lappend(*locations, d as *mut c_void);
                        });
                    });
                }
                return true;
            }
            false
        }

        OP_PHRASE => {
            /* We can hand this off to TS_phrase_execute */
            data = palloc0(core::mem::size_of::<ExecPhraseData>()) as *mut ExecPhraseData;
            if TS_phrase_execute(curitem, arg, TS_EXEC_EMPTY, chkcond, data) == TS_YES {
                if !(*data).negate {
                    *locations = list_make1!(data);
                }
                return true;
            }
            pfree(data as *mut c_void);
            false
        }

        other => {
            elog!(ERROR, "unrecognized operator: {}", other as c_int);
            unreachable!();
        }
    }
}

/*
 * Detect whether a tsquery boolean expression requires any positive matches.
 */
pub unsafe fn tsquery_requires_match(curitem: *mut QueryItem) -> bool {
    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    if (*curitem).type_() == QI_VAL {
        return true;
    }

    match (*curitem).qoperator.oper {
        OP_NOT => {
            /* Assume there are no required matches underneath a NOT. */
            false
        }

        /* Treat OP_PHRASE as OP_AND here */
        OP_PHRASE | OP_AND => {
            /* If either side requires a match, we're good */
            if tsquery_requires_match(curitem.add((*curitem).qoperator.left as usize)) {
                true
            } else {
                tsquery_requires_match(curitem.add(1))
            }
        }

        OP_OR => {
            /* Both sides must require a match */
            if tsquery_requires_match(curitem.add((*curitem).qoperator.left as usize)) {
                tsquery_requires_match(curitem.add(1))
            } else {
                false
            }
        }

        other => {
            elog!(ERROR, "unrecognized operator: {}", other as c_int);
            unreachable!();
        }
    }
}

// ----------------------------------------------------------------
//   PG_GETARG_TSQUERY / DatumGetTSQuery / TSQueryGetDatum
//   (ts_type.h macros; the C PG_GETARG_TSQUERY detoasts, which is the identity
//   for in-line datums with TOAST unported -- mirrors PG_GETARG_TSVECTOR).
// ----------------------------------------------------------------
#[inline]
unsafe fn DatumGetTSQuery(x: Datum) -> TSQuery {
    crate::varatt::pg_detoast_datum_packed(DatumGetPointer(x) as *mut c_void) as TSQuery
}
#[inline]
unsafe fn PG_GETARG_TSQUERY(fcinfo: FunctionCallInfo, n: usize) -> TSQuery {
    DatumGetTSQuery(PG_GETARG_DATUM!(fcinfo, n))
}

/*
 * boolean operations
 */
pub unsafe fn ts_match_qv(fcinfo: FunctionCallInfo) -> Datum {
    /* PG_RETURN_DATUM(DirectFunctionCall2(ts_match_vq, ARG1, ARG0)); */
    PG_RETURN_DATUM!(DirectFunctionCall2!(
        ts_match_vq,
        PG_GETARG_DATUM!(fcinfo, 1),
        PG_GETARG_DATUM!(fcinfo, 0)
    ))
}

pub unsafe fn ts_match_vq(fcinfo: FunctionCallInfo) -> Datum {
    let val: TSVector = PG_GETARG_TSVECTOR(PG_GETARG_DATUM!(fcinfo, 0));
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 1);
    let mut chkval: CHKVAL = CHKVAL {
        arrb: null_mut(),
        arre: null_mut(),
        values: null_mut(),
        operand: null_mut(),
    };
    let result: bool;

    /* empty query matches nothing */
    if (*query).size == 0 {
        PG_FREE_IF_COPY!(fcinfo, val, 0);
        PG_FREE_IF_COPY!(fcinfo, query, 1);
        PG_RETURN_BOOL!(false);
    }

    chkval.arrb = ARRPTR(val);
    chkval.arre = chkval.arrb.add((*val).size as usize);
    chkval.values = STRPTR(val);
    chkval.operand = GETOPERAND(query);
    result = TS_execute(
        GETQUERY(query),
        &mut chkval as *mut CHKVAL as *mut c_void,
        TS_EXEC_EMPTY,
        checkcondition_str,
    );

    PG_FREE_IF_COPY!(fcinfo, val, 0);
    PG_FREE_IF_COPY!(fcinfo, query, 1);
    PG_RETURN_BOOL!(result)
}

pub unsafe fn ts_match_tt(fcinfo: FunctionCallInfo) -> Datum {
    let vector: TSVector;
    let query: TSQuery;
    let res: bool;

    vector = DatumGetTSVector(DirectFunctionCall1!(to_tsvector, PG_GETARG_DATUM!(fcinfo, 0)));
    query = DatumGetTSQuery(DirectFunctionCall1!(plainto_tsquery, PG_GETARG_DATUM!(fcinfo, 1)));

    res = DatumGetBool(DirectFunctionCall2!(
        ts_match_vq,
        TSVectorGetDatum(vector),
        TSQueryGetDatum(query)
    ));

    pfree(vector as *mut c_void);
    pfree(query as *mut c_void);

    PG_RETURN_BOOL!(res)
}

pub unsafe fn ts_match_tq(fcinfo: FunctionCallInfo) -> Datum {
    let vector: TSVector;
    let query: TSQuery = PG_GETARG_TSQUERY(fcinfo, 1);
    let res: bool;

    vector = DatumGetTSVector(DirectFunctionCall1!(to_tsvector, PG_GETARG_DATUM!(fcinfo, 0)));

    res = DatumGetBool(DirectFunctionCall2!(
        ts_match_vq,
        TSVectorGetDatum(vector),
        TSQueryGetDatum(query)
    ));

    pfree(vector as *mut c_void);
    PG_FREE_IF_COPY!(fcinfo, query, 1);

    PG_RETURN_BOOL!(res)
}

// ================================================================
//   ts_stat statistic function support
// ================================================================

/*
 * Returns the number of positions in value 'wptr' within tsvector 'txt',
 * that have a weight equal to one of the weights in 'weight' bitmask.
 */
unsafe fn check_weight(txt: TSVector, wptr: *mut WordEntry, weight: int8) -> c_int {
    let mut len: c_int = POSDATALEN(txt, wptr);
    let mut num: c_int = 0;
    let mut ptr: *mut WordEntryPos = POSDATAPTR(txt, wptr);

    while {
        let old = len;
        len -= 1;
        old != 0
    } {
        if (weight & (1 << WEP_GETWEIGHT(*ptr))) != 0 {
            num += 1;
        }
        ptr = ptr.add(1);
    }
    num
}

/*
 * #define compareStatWord(a,e,t) \
 *     tsCompareString((a)->lexeme, (a)->lenlexeme, STRPTR(t) + (e)->pos, (e)->len, false)
 */
#[inline]
unsafe fn compareStatWord(a: *mut StatEntry, e: *mut WordEntry, t: TSVector) -> int32 {
    tsCompareString(
        (*a).lexeme.as_mut_ptr(),
        (*a).lenlexeme as c_int,
        STRPTR(t).add((*e).pos() as usize),
        (*e).len() as c_int,
        false,
    )
}

unsafe fn insertStatEntry(
    persistentContext: MemoryContext,
    stat: *mut TSVectorStat,
    txt: TSVector,
    off: uint32,
) {
    let we: *mut WordEntry = ARRPTR(txt).add(off as usize);
    let mut node: *mut StatEntry = (*stat).root;
    let mut pnode: *mut StatEntry = null_mut();
    let n: c_int;
    let mut res: c_int = 0;
    let mut depth: uint32 = 1;

    if (*stat).weight == 0 {
        n = if (*we).haspos() != 0 {
            POSDATALEN(txt, we)
        } else {
            1
        };
    } else {
        n = if (*we).haspos() != 0 {
            check_weight(txt, we, (*stat).weight as int8)
        } else {
            0
        };
    }

    if n == 0 {
        return; /* nothing to insert */
    }

    while !node.is_null() {
        res = compareStatWord(node, we, txt);

        if res == 0 {
            break;
        } else {
            pnode = node;
            node = if res < 0 { (*node).left } else { (*node).right };
        }
        depth += 1;
    }

    if depth > (*stat).maxdepth {
        (*stat).maxdepth = depth;
    }

    if node.is_null() {
        node = MemoryContextAlloc(persistentContext, STATENTRYHDRSZ() + (*we).len() as usize)
            as *mut StatEntry;
        (*node).left = null_mut();
        (*node).right = null_mut();
        (*node).ndoc = 1;
        (*node).nentry = n as uint32;
        (*node).lenlexeme = (*we).len();
        memcpy(
            (*node).lexeme.as_mut_ptr() as *mut c_void,
            STRPTR(txt).add((*we).pos() as usize) as *const c_void,
            (*node).lenlexeme as usize,
        );

        if pnode.is_null() {
            (*stat).root = node;
        } else if res < 0 {
            (*pnode).left = node;
        } else {
            (*pnode).right = node;
        }
    } else {
        (*node).ndoc += 1;
        (*node).nentry += n as uint32;
    }
}

unsafe fn chooseNextStatEntry(
    persistentContext: MemoryContext,
    stat: *mut TSVectorStat,
    txt: TSVector,
    low: uint32,
    high: uint32,
    offset: uint32,
) {
    let mut pos: uint32;
    let middle: uint32 = (low + high) >> 1;

    pos = (low + middle) >> 1;
    if low != middle && pos >= offset && pos - offset < (*txt).size as uint32 {
        insertStatEntry(persistentContext, stat, txt, pos - offset);
    }
    pos = (high + middle + 1) >> 1;
    if middle + 1 != high && pos >= offset && pos - offset < (*txt).size as uint32 {
        insertStatEntry(persistentContext, stat, txt, pos - offset);
    }

    if low != middle {
        chooseNextStatEntry(persistentContext, stat, txt, low, middle, offset);
    }
    if high != middle + 1 {
        chooseNextStatEntry(persistentContext, stat, txt, middle + 1, high, offset);
    }
}

/*
 * This is written like a custom aggregate function, because the original plan
 * was to do just that.  See the C source for the historical note.
 */
unsafe fn ts_accum(
    persistentContext: MemoryContext,
    mut stat: *mut TSVectorStat,
    data: Datum,
) -> *mut TSVectorStat {
    let txt: TSVector = DatumGetTSVector(data);
    let mut i: uint32;
    let mut nbit: uint32 = 0;
    let offset: uint32;

    if stat.is_null() {
        /* Init in first */
        stat = MemoryContextAllocZero(persistentContext, core::mem::size_of::<TSVectorStat>())
            as *mut TSVectorStat;
        (*stat).maxdepth = 1;
    }

    /* simple check of correctness */
    if txt.is_null() || (*txt).size == 0 {
        if !txt.is_null() && txt != DatumGetPointer(data) as TSVector {
            pfree(txt as *mut c_void);
        }
        return stat;
    }

    i = ((*txt).size - 1) as uint32;
    while i > 0 {
        nbit += 1;
        i >>= 1;
    }

    nbit = 1 << nbit;
    offset = (nbit - (*txt).size as uint32) / 2;

    insertStatEntry(persistentContext, stat, txt, (nbit >> 1) - offset);
    chooseNextStatEntry(persistentContext, stat, txt, 0, nbit, offset);

    stat
}

unsafe fn ts_setup_firstcall(
    fcinfo: FunctionCallInfo,
    funcctx: *mut FuncCallContext,
    stat: *mut TSVectorStat,
) {
    let mut tupdesc: TupleDesc = null_mut();
    let oldcontext: MemoryContext;
    let mut node: *mut StatEntry;

    (*funcctx).user_fctx = stat as *mut c_void;

    oldcontext = MemoryContextSwitchTo((*funcctx).multi_call_memory_ctx);

    (*stat).stack = palloc0(
        core::mem::size_of::<*mut StatEntry>() * ((*stat).maxdepth + 1) as usize,
    ) as *mut *mut StatEntry;
    (*stat).stackpos = 0;

    node = (*stat).root;
    /* find leftmost value */
    if node.is_null() {
        *(*stat).stack.add((*stat).stackpos as usize) = null_mut();
    } else {
        loop {
            *(*stat).stack.add((*stat).stackpos as usize) = node;
            if !(*node).left.is_null() {
                (*stat).stackpos += 1;
                node = (*node).left;
            } else {
                break;
            }
        }
    }
    Assert!((*stat).stackpos <= (*stat).maxdepth);

    if get_call_result_type(fcinfo, null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }
    (*funcctx).tuple_desc = tupdesc;
    (*funcctx).attinmeta = TupleDescGetAttInMetadata(tupdesc);

    MemoryContextSwitchTo(oldcontext);
}

unsafe fn walkStatEntryTree(stat: *mut TSVectorStat) -> *mut StatEntry {
    let mut node: *mut StatEntry = *(*stat).stack.add((*stat).stackpos as usize);

    if node.is_null() {
        return null_mut();
    }

    if (*node).ndoc != 0 {
        /* return entry itself: we already was at left sublink */
        return node;
    } else if !(*node).right.is_null()
        && (*node).right != *(*stat).stack.add(((*stat).stackpos + 1) as usize)
    {
        /* go on right sublink */
        (*stat).stackpos += 1;
        node = (*node).right;

        /* find most-left value */
        loop {
            *(*stat).stack.add((*stat).stackpos as usize) = node;
            if !(*node).left.is_null() {
                (*stat).stackpos += 1;
                node = (*node).left;
            } else {
                break;
            }
        }
        Assert!((*stat).stackpos <= (*stat).maxdepth);
    } else {
        /* we already return all left subtree, itself and  right subtree */
        if (*stat).stackpos == 0 {
            return null_mut();
        }

        (*stat).stackpos -= 1;
        return walkStatEntryTree(stat);
    }

    node
}

unsafe fn ts_process_call(funcctx: *mut FuncCallContext) -> Datum {
    let st: *mut TSVectorStat;
    let entry: *mut StatEntry;

    st = (*funcctx).user_fctx as *mut TSVectorStat;

    entry = walkStatEntryTree(st);

    if !entry.is_null() {
        let result: Datum;
        let mut values: [*mut c_char; 3] = [null_mut(); 3];
        let mut ndoc: [c_char; 16] = [0; 16];
        let mut nentry: [c_char; 16] = [0; 16];
        let tuple: *mut c_void;

        values[0] = palloc((*entry).lenlexeme as usize + 1) as *mut c_char;
        memcpy(
            values[0] as *mut c_void,
            (*entry).lexeme.as_ptr() as *const c_void,
            (*entry).lenlexeme as usize,
        );
        *values[0].add((*entry).lenlexeme as usize) = b'\0' as c_char;
        sprintf(ndoc.as_mut_ptr(), c"%d".as_ptr(), (*entry).ndoc);
        values[1] = ndoc.as_mut_ptr();
        sprintf(nentry.as_mut_ptr(), c"%d".as_ptr(), (*entry).nentry);
        values[2] = nentry.as_mut_ptr();

        tuple = BuildTupleFromCStrings((*funcctx).attinmeta, values.as_mut_ptr());
        result = HeapTupleGetDatum(tuple);

        pfree(values[0] as *mut c_void);

        /* mark entry as already visited */
        (*entry).ndoc = 0;

        return result;
    }

    0 as Datum
}

unsafe fn ts_stat_sql(
    persistentContext: MemoryContext,
    txt: *mut text,
    ws: *mut text,
) -> *mut TSVectorStat {
    let query: *mut c_char = text_to_cstring(txt);
    let mut stat: *mut TSVectorStat;
    let mut isnull: bool = false;
    let portal: Portal;
    let plan: SPIPlanPtr;

    plan = SPI_prepare(query, 0, null_mut());
    if plan.is_null() {
        /* internal error */
        elog!(
            ERROR,
            "SPI_prepare(\"{}\") failed",
            std::ffi::CStr::from_ptr(query).to_string_lossy()
        );
    }

    portal = SPI_cursor_open(null_mut(), plan, null_mut(), null_mut(), true);
    if portal.is_null() {
        /* internal error */
        elog!(
            ERROR,
            "SPI_cursor_open(\"{}\") failed",
            std::ffi::CStr::from_ptr(query).to_string_lossy()
        );
    }

    SPI_cursor_fetch(portal, true, 100);

    if SPI_tuptable.is_null()
        || (*(*SPI_tuptable).tupdesc.cast::<TupleDescData>()).natts != 1
        || !IsBinaryCoercible(SPI_gettypeid((*SPI_tuptable).tupdesc, 1), TSVECTOROID)
    {
        /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(ERROR, errmsg!("ts_stat query must return one tsvector column"));
    }

    stat = MemoryContextAllocZero(persistentContext, core::mem::size_of::<TSVectorStat>())
        as *mut TSVectorStat;
    (*stat).maxdepth = 1;

    if !ws.is_null() {
        let mut buf: *mut c_char;
        let end: *const c_char;

        buf = VARDATA_ANY(ws as *const c_char);
        end = buf.add(VARSIZE_ANY_EXHDR(ws as *const c_char) as usize);
        while (buf as *const c_char) < end {
            let len: c_int = pg_mblen_range(buf, end);

            if len == 1 {
                match *buf as u8 {
                    b'A' | b'a' => (*stat).weight |= 1 << 3,
                    b'B' | b'b' => (*stat).weight |= 1 << 2,
                    b'C' | b'c' => (*stat).weight |= 1 << 1,
                    b'D' | b'd' => (*stat).weight |= 1,
                    _ => (*stat).weight |= 0,
                }
            }
            buf = buf.add(len as usize);
        }
    }

    while SPI_processed > 0 {
        let mut i: u64;

        i = 0;
        while i < SPI_processed {
            let data: Datum = SPI_getbinval(
                *(*SPI_tuptable).vals.add(i as usize),
                (*SPI_tuptable).tupdesc,
                1,
                &mut isnull,
            );

            if !isnull {
                stat = ts_accum(persistentContext, stat, data);
            }
            i += 1;
        }

        SPI_freetuptable(SPI_tuptable);
        SPI_cursor_fetch(portal, true, 100);
    }

    SPI_freetuptable(SPI_tuptable);
    SPI_cursor_close(portal);
    SPI_freeplan(plan);
    pfree(query as *mut c_void);

    stat
}

pub unsafe fn ts_stat1(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let result: Datum;

    if SRF_IS_FIRSTCALL!(fcinfo) {
        let stat: *mut TSVectorStat;
        let txt: *mut text = DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut text;

        funcctx = SRF_FIRSTCALL_INIT!(fcinfo);
        SPI_connect();
        stat = ts_stat_sql((*funcctx).multi_call_memory_ctx, txt, null_mut());
        PG_FREE_IF_COPY!(fcinfo, txt, 0);
        ts_setup_firstcall(fcinfo, funcctx, stat);
        SPI_finish();
    }

    funcctx = SRF_PERCALL_SETUP!(fcinfo);
    result = ts_process_call(funcctx);
    if result != 0 as Datum {
        SRF_RETURN_NEXT!(fcinfo, funcctx, result);
    }
    SRF_RETURN_DONE!(fcinfo, funcctx);
}

pub unsafe fn ts_stat2(fcinfo: FunctionCallInfo) -> Datum {
    let mut funcctx: *mut FuncCallContext;
    let result: Datum;

    if SRF_IS_FIRSTCALL!(fcinfo) {
        let stat: *mut TSVectorStat;
        let txt: *mut text = DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 0)) as *mut text;
        let ws: *mut text = DatumGetPointer(PG_GETARG_DATUM!(fcinfo, 1)) as *mut text;

        funcctx = SRF_FIRSTCALL_INIT!(fcinfo);
        SPI_connect();
        stat = ts_stat_sql((*funcctx).multi_call_memory_ctx, txt, ws);
        PG_FREE_IF_COPY!(fcinfo, txt, 0);
        PG_FREE_IF_COPY!(fcinfo, ws, 1);
        ts_setup_firstcall(fcinfo, funcctx, stat);
        SPI_finish();
    }

    funcctx = SRF_PERCALL_SETUP!(fcinfo);
    result = ts_process_call(funcctx);
    if result != 0 as Datum {
        SRF_RETURN_NEXT!(fcinfo, funcctx, result);
    }
    SRF_RETURN_DONE!(fcinfo, funcctx);
}

// ================================================================
//   tsvector update trigger
// ================================================================
//
// Triggers for automatic update of a tsvector column from text column(s).
//
// Trigger arguments are either
//      name of tsvector col, name of tsconfig to use, name(s) of text col(s)
//      name of tsvector col, name of regconfig col, name(s) of text col(s)
// ie, tsconfig can either be specified by name, or indirectly as the contents
// of a regconfig field in the row.  If the name is used, it must be explicitly
// schema-qualified.

pub unsafe fn tsvector_update_trigger_byid(fcinfo: FunctionCallInfo) -> Datum {
    tsvector_update_trigger(fcinfo, false)
}

pub unsafe fn tsvector_update_trigger_bycolumn(fcinfo: FunctionCallInfo) -> Datum {
    tsvector_update_trigger(fcinfo, true)
}

unsafe fn tsvector_update_trigger(fcinfo: FunctionCallInfo, config_column: bool) -> Datum {
    let trigdata: *mut TriggerData;
    let trigger: *mut Trigger;
    let rel: Relation;
    let mut rettuple: *mut c_void = null_mut();
    let tsvector_attr_num: c_int;
    let mut i: c_int;
    let mut prs: ParsedText = core::mem::zeroed();
    let mut datum: Datum;
    let mut isnull: bool = false;
    let mut txt: *mut text;
    let cfgId: Oid;
    let mut update_needed: bool = false;

    /* Check call context */
    if !CALLED_AS_TRIGGER(fcinfo) {
        /* internal error */
        elog!(ERROR, "tsvector_update_trigger: not fired by trigger manager");
    }

    trigdata = (*fcinfo).context as *mut TriggerData;
    if !TRIGGER_FIRED_FOR_ROW((*trigdata).tg_event) {
        elog!(ERROR, "tsvector_update_trigger: must be fired for row");
    }
    if !TRIGGER_FIRED_BEFORE((*trigdata).tg_event) {
        elog!(ERROR, "tsvector_update_trigger: must be fired BEFORE event");
    }

    if TRIGGER_FIRED_BY_INSERT((*trigdata).tg_event) {
        rettuple = (*trigdata).tg_trigtuple;
        update_needed = true;
    } else if TRIGGER_FIRED_BY_UPDATE((*trigdata).tg_event) {
        rettuple = (*trigdata).tg_newtuple;
        update_needed = false; /* computed below */
    } else {
        elog!(ERROR, "tsvector_update_trigger: must be fired for INSERT or UPDATE");
    }

    trigger = (*trigdata).tg_trigger;
    rel = (*trigdata).tg_relation;

    if (*trigger).tgnargs < 3 {
        elog!(ERROR, "tsvector_update_trigger: arguments must be tsvector_field, ts_config, text_field1, ...)");
    }

    /* Find the target tsvector column */
    tsvector_attr_num = SPI_fnumber((*rel).rd_att, *(*trigger).tgargs.add(0));
    if tsvector_attr_num == SPI_ERROR_NOATTRIBUTE {
        /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
        let _ = errcode(ERRCODE_UNDEFINED_COLUMN);
        ereport!(
            ERROR,
            errmsg!(
                "tsvector column \"{}\" does not exist",
                std::ffi::CStr::from_ptr(*(*trigger).tgargs.add(0)).to_string_lossy()
            )
        );
    }
    /* This will effectively reject system columns, so no separate test: */
    if !IsBinaryCoercible(SPI_gettypeid((*rel).rd_att, tsvector_attr_num), TSVECTOROID) {
        /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
        let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
        ereport!(
            ERROR,
            errmsg!(
                "column \"{}\" is not of tsvector type",
                std::ffi::CStr::from_ptr(*(*trigger).tgargs.add(0)).to_string_lossy()
            )
        );
    }

    /* Find the configuration to use */
    if config_column {
        let config_attr_num: c_int;

        config_attr_num = SPI_fnumber((*rel).rd_att, *(*trigger).tgargs.add(1));
        if config_attr_num == SPI_ERROR_NOATTRIBUTE {
            /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
            let _ = errcode(ERRCODE_UNDEFINED_COLUMN);
            ereport!(
                ERROR,
                errmsg!(
                    "configuration column \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(*(*trigger).tgargs.add(1)).to_string_lossy()
                )
            );
        }
        if !IsBinaryCoercible(SPI_gettypeid((*rel).rd_att, config_attr_num), REGCONFIGOID) {
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!(
                    "column \"{}\" is not of regconfig type",
                    std::ffi::CStr::from_ptr(*(*trigger).tgargs.add(1)).to_string_lossy()
                )
            );
        }

        datum = SPI_getbinval(rettuple, (*rel).rd_att, config_attr_num, &mut isnull);
        if isnull {
            /* C also: errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED) */
            let _ = errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED);
            ereport!(
                ERROR,
                errmsg!(
                    "configuration column \"{}\" must not be null",
                    std::ffi::CStr::from_ptr(*(*trigger).tgargs.add(1)).to_string_lossy()
                )
            );
        }
        cfgId = DatumGetObjectId(datum);
    } else {
        let names: *mut crate::nodes::pg_list::List;

        names = stringToQualifiedNameList(*(*trigger).tgargs.add(1), null_mut());
        /* require a schema so that results are not search path dependent */
        if crate::nodes::pg_list::list_length(names) < 2 {
            /* C also: errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(
                ERROR,
                errmsg!(
                    "text search configuration name \"{}\" must be schema-qualified",
                    std::ffi::CStr::from_ptr(*(*trigger).tgargs.add(1)).to_string_lossy()
                )
            );
        }
        cfgId = get_ts_config_oid(names, false);
    }

    /* initialize parse state */
    prs.lenwords = 32;
    prs.curwords = 0;
    prs.pos = 0;
    prs.words =
        palloc(core::mem::size_of::<ParsedWord>() * prs.lenwords as usize) as *mut ParsedWord;

    /* find all words in indexable column(s) */
    i = 2;
    while i < (*trigger).tgnargs as c_int {
        let numattr: c_int;

        numattr = SPI_fnumber((*rel).rd_att, *(*trigger).tgargs.add(i as usize));
        if numattr == SPI_ERROR_NOATTRIBUTE {
            /* C also: errcode(ERRCODE_UNDEFINED_COLUMN) */
            let _ = errcode(ERRCODE_UNDEFINED_COLUMN);
            ereport!(
                ERROR,
                errmsg!(
                    "column \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(*(*trigger).tgargs.add(i as usize)).to_string_lossy()
                )
            );
        }
        if !IsBinaryCoercible(SPI_gettypeid((*rel).rd_att, numattr), TEXTOID) {
            /* C also: errcode(ERRCODE_DATATYPE_MISMATCH) */
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!(
                    "column \"{}\" is not of a character type",
                    std::ffi::CStr::from_ptr(*(*trigger).tgargs.add(i as usize)).to_string_lossy()
                )
            );
        }

        if bms_is_member(numattr - FirstLowInvalidHeapAttributeNumber, (*trigdata).tg_updatedcols) {
            update_needed = true;
        }

        datum = SPI_getbinval(rettuple, (*rel).rd_att, numattr, &mut isnull);
        if isnull {
            i += 1;
            continue;
        }

        txt = crate::varatt::pg_detoast_datum_packed(DatumGetPointer(datum) as *mut c_void)
            as *mut text;

        parsetext(
            cfgId,
            &mut prs,
            VARDATA_ANY(txt as *const c_char),
            VARSIZE_ANY_EXHDR(txt as *const c_char) as c_int,
        );

        if txt != DatumGetPointer(datum) as *mut text {
            pfree(txt as *mut c_void);
        }
        i += 1;
    }

    if update_needed {
        /* make tsvector value */
        datum = TSVectorGetDatum(make_tsvector(&mut prs));
        isnull = false;

        /* and insert it into tuple */
        rettuple = heap_modify_tuple_by_cols(
            rettuple,
            (*rel).rd_att,
            1,
            &mut (tsvector_attr_num as c_int),
            &mut datum,
            &mut isnull,
        );

        pfree(DatumGetPointer(datum) as *mut c_void);
    }

    PointerGetDatum(rettuple)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::adt::tsvector::{TSVectorData, MAXSTRLEN};

    /*
     * tsvectorin's parser (tsvector_parser.c) is stubbed with unimplemented!(),
     * so we cannot build TSVectors through the I/O path at test time.  Instead we
     * assemble plain (no-position) TSVectors by hand here, matching the on-disk
     * layout that array_to_tsvector / tsvector_strip would produce: a sorted,
     * de-duplicated set of lexemes with haspos = 0.
     */
    unsafe fn make_plain_tsvector(lexemes: &[&[u8]]) -> TSVector {
        let n = lexemes.len() as c_int;
        let mut datalen: c_int = 0;
        for lex in lexemes {
            assert!((lex.len() as c_int) < MAXSTRLEN);
            datalen += lex.len() as c_int;
        }
        let total = CALCDATASIZE(n, datalen) as c_int;
        let v = palloc0(total as Size) as TSVector;
        SET_VARSIZE(v as *mut c_char, total);
        (*v).size = n;

        let arr = ARRPTR(v);
        let strbase = STRPTR(v);
        let mut off: c_int = 0;
        for (i, lex) in lexemes.iter().enumerate() {
            (*arr.add(i)).set_haspos(0);
            (*arr.add(i)).set_len(lex.len() as u32);
            (*arr.add(i)).set_pos(off as u32);
            memcpy(
                strbase.add(off as usize) as *mut c_void,
                lex.as_ptr() as *const c_void,
                lex.len(),
            );
            off += lex.len() as c_int;
        }
        v
    }

    /* Read back the lexemes of a (plain) tsvector for assertions. */
    unsafe fn lexemes_of(v: TSVector) -> Vec<Vec<u8>> {
        let arr = ARRPTR(v);
        let strbase = STRPTR(v);
        let mut out = Vec::new();
        for i in 0..(*v).size as usize {
            let e = arr.add(i);
            let p = strbase.add((*e).pos() as usize) as *const u8;
            let len = (*e).len() as usize;
            out.push(core::slice::from_raw_parts(p, len).to_vec());
        }
        out
    }

    /* Build a 1-arg fcinfo carrying a single TSVector datum. */
    macro_rules! call1 {
        ($func:expr, $arg:expr) => {{
            crate::LOCAL_FCINFO!(fcinfo, 1);
            crate::InitFunctionCallInfoData!(fcinfo, null_mut(), 1, 0, null_mut(), null_mut());
            (*(*fcinfo).args.as_mut_ptr().add(0)).value = TSVectorGetDatum($arg);
            (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
            $func(fcinfo)
        }};
    }

    /* Build a 2-arg fcinfo carrying two TSVector datums. */
    macro_rules! call2 {
        ($func:expr, $a:expr, $b:expr) => {{
            crate::LOCAL_FCINFO!(fcinfo, 2);
            crate::InitFunctionCallInfoData!(fcinfo, null_mut(), 2, 0, null_mut(), null_mut());
            (*(*fcinfo).args.as_mut_ptr().add(0)).value = TSVectorGetDatum($a);
            (*(*fcinfo).args.as_mut_ptr().add(0)).isnull = false;
            (*(*fcinfo).args.as_mut_ptr().add(1)).value = TSVectorGetDatum($b);
            (*(*fcinfo).args.as_mut_ptr().add(1)).isnull = false;
            $func(fcinfo)
        }};
    }

    #[test]
    fn cmp_eq_ne() {
        unsafe {
            let a = make_plain_tsvector(&[b"a", b"b", b"c"]);
            let a2 = make_plain_tsvector(&[b"a", b"b", b"c"]);
            let b = make_plain_tsvector(&[b"a", b"b", b"d"]);

            // a == a
            assert!(DatumGetBool(call2!(tsvector_eq, a, a2)));
            // a != b
            assert!(!DatumGetBool(call2!(tsvector_eq, a, b)));
            assert!(DatumGetBool(call2!(tsvector_ne, a, b)));

            // cmp(a,a) == 0
            assert_eq!(DatumGetInt32(call2!(tsvector_cmp, a, a2)), 0);
            // a and b have equal size/varsize; differ in last lexeme 'c' vs 'd'.
            // silly_cmp returns tsCompareString('c','d') < 0, so cmp(a,b) < 0.
            assert!(DatumGetInt32(call2!(tsvector_cmp, a, b)) < 0);
            assert!(DatumGetInt32(call2!(tsvector_cmp, b, a)) > 0);
            assert!(DatumGetBool(call2!(tsvector_lt, a, b)));
            assert!(DatumGetBool(call2!(tsvector_le, a, a2)));
            assert!(DatumGetBool(call2!(tsvector_gt, b, a)));
            assert!(DatumGetBool(call2!(tsvector_ge, a, a2)));
        }
    }

    #[test]
    fn length_is_lexeme_count() {
        unsafe {
            let a = make_plain_tsvector(&[b"a", b"b", b"c"]);
            assert_eq!(DatumGetInt32(call1!(tsvector_length, a)), 3);
        }
    }

    #[test]
    fn strip_keeps_lexemes_drops_positions() {
        unsafe {
            let a = make_plain_tsvector(&[b"a", b"b", b"c"]);
            let stripped = DatumGetPointer(call1!(tsvector_strip, a)) as TSVector;
            assert_eq!((*stripped).size, 3);
            let lex = lexemes_of(stripped);
            assert_eq!(lex, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
            // all entries have haspos cleared
            let arr = ARRPTR(stripped);
            for i in 0..3 {
                assert_eq!((*arr.add(i)).haspos(), 0);
            }
        }
    }

    #[test]
    fn concat_merges_sorted_lexemes() {
        unsafe {
            // "a b" || "c d" => "a b c d"
            let l = make_plain_tsvector(&[b"a", b"b"]);
            let r = make_plain_tsvector(&[b"c", b"d"]);
            let out = DatumGetPointer(call2!(tsvector_concat, l, r)) as TSVector;
            assert_eq!((*out).size, 4);
            assert_eq!(
                lexemes_of(out),
                vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
            );

            // overlapping lexeme should collapse: "a b" || "b c" => "a b c"
            let l2 = make_plain_tsvector(&[b"a", b"b"]);
            let r2 = make_plain_tsvector(&[b"b", b"c"]);
            let out2 = DatumGetPointer(call2!(tsvector_concat, l2, r2)) as TSVector;
            assert_eq!((*out2).size, 3);
            assert_eq!(
                lexemes_of(out2),
                vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
            );
        }
    }

    // Silence "field never read" on the layout-only structs.
    #[allow(dead_code)]
    fn _touch_layout() {
        let _ = core::mem::size_of::<CHKVAL>();
        let _ = core::mem::size_of::<TSVectorStat>();
        let _ = STATENTRYHDRSZ();
        let _ = core::mem::size_of::<TSVectorData>();
    }
}

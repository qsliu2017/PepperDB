//! Translated from PostgreSQL src/include/utils/jsonpath.h

use bitflags::bitflags;

use crate::c::text;
use crate::executor::tablefunc::TableFuncRoutine;
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::JsonWrapper;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::jsonb::JsonbValue;
use crate::utils::numeric::Numeric;

/// On-disk jsonpath varlena. Fixed header followed by a flexible data area;
/// the trailing `data[]` lives in the buffer behind a slice accessor.
#[repr(C)]
pub struct JsonPath {
    pub vl_len_: i32, // varlena header (do not touch directly!)
    pub header: u32,  // version and flags (see below)
                      // char data[FLEXIBLE_ARRAY_MEMBER] follows (on-disk FAM)
}

pub const JSONPATH_VERSION: u32 = 0x01;
pub const JSONPATH_LAX: u32 = 0x80000000;
/// offsetof(JsonPath, data) -- fixed-header size.
pub const JSONPATH_HDRSZ: usize = core::mem::size_of::<JsonPath>();

impl JsonPath {
    /// SAFETY: `self` heads a varlena buffer of its recorded VARSIZE.
    pub fn data(&self) -> &[u8] {
        unimplemented!()
    }
}

/// jspIsScalar: jpiNull..=jpiBool are scalar literals.
pub fn jspIsScalar(t: JsonPathItemType) -> bool {
    (t as i32) >= (JsonPathItemType::jpiNull as i32)
        && (t as i32) <= (JsonPathItemType::jpiBool as i32)
}

/// All node types of a jsonpath expression. The first four alias jsonb's
/// jbv* scalar tags; the rest follow in fixed on-disk order (never reorder).
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonPathItemType {
    jpiNull = 0x0,    // = jbvNull
    jpiString = 1,    // = jbvString
    jpiNumeric = 2,   // = jbvNumeric
    jpiBool = 3,      // = jbvBool
    jpiAnd,
    jpiOr,
    jpiNot,
    jpiIsUnknown,
    jpiEqual,
    jpiNotEqual,
    jpiLess,
    jpiGreater,
    jpiLessOrEqual,
    jpiGreaterOrEqual,
    jpiAdd,
    jpiSub,
    jpiMul,
    jpiDiv,
    jpiMod,
    jpiPlus,
    jpiMinus,
    jpiAnyArray,
    jpiAnyKey,
    jpiIndexArray,
    jpiAny,
    jpiKey,
    jpiCurrent,
    jpiRoot,
    jpiVariable,
    jpiFilter,
    jpiExists,
    jpiType,
    jpiSize,
    jpiAbs,
    jpiFloor,
    jpiCeiling,
    jpiDouble,
    jpiDatetime,
    jpiKeyValue,
    jpiSubscript,
    jpiLast,
    jpiStartsWith,
    jpiLikeRegex,
    jpiBigint,
    jpiBoolean,
    jpiDate,
    jpiDecimal,
    jpiInteger,
    jpiNumber,
    jpiStringFunc,
    jpiTime,
    jpiTimeTz,
    jpiTimestamp,
    jpiTimestampTz,
}

bitflags! {
    /// XQuery regex mode flags for the LIKE_REGEX predicate.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct JspRegex: u32 {
        const ICASE  = 0x01; // i flag, case insensitive
        const DOTALL = 0x02; // s flag, dot matches newline
        const MLINE  = 0x04; // m flag, ^/$ match at newlines
        const WSPACE = 0x08; // x flag, ignore whitespace in pattern
        const QUOTE  = 0x10; // q flag, no special characters
    }
}

/// In-memory parsed view of a node within a JsonPath value.
pub struct JsonPathItem {
    pub r#type: JsonPathItemType,
    /// position from base to next node
    pub nextPos: i32,
    /// pointer into JsonPath value; positions are relative to this base
    pub base: *mut u8, // TODO(ptr): borrows into the JsonPath buffer
    pub content: JsonPathItemContent,
}

/// The C `union content` -> tagged enum (variant chosen by `type`).
pub enum JsonPathItemContent {
    /// classic binary operator (and, or, ...)
    Args { left: i32, right: i32 },
    /// any unary operation
    Arg(i32),
    /// jpiIndexArray: array of (from, to) index pairs
    Array { nelems: i32, elems: *mut JsonPathItemArrayElem }, // TODO(ptr)
    /// jpiAny: level bounds
    AnyBounds { first: u32, last: u32 },
    /// bool, numeric and string/key literal storage
    Value { data: *mut u8, datalen: i32 }, // TODO(ptr); datalen filled only for string/key
    /// jpiLikeRegex
    LikeRegex {
        expr: i32,
        pattern: *mut u8, // TODO(ptr)
        patternlen: i32,
        flags: u32,
    },
}

pub struct JsonPathItemArrayElem {
    pub from: i32,
    pub to: i32,
}

/// jspHasNext: there is a following node.
pub fn jspHasNext(jsp: &JsonPathItem) -> bool {
    jsp.nextPos > 0
}

pub fn jspInit(v: &mut JsonPathItem, js: &JsonPath) {
    unimplemented!()
}

pub fn jspInitByBuffer(v: &mut JsonPathItem, base: *mut u8, pos: i32) {
    unimplemented!()
}

/// Returns false when there is no next node.
pub fn jspGetNext(v: &JsonPathItem, a: &mut JsonPathItem) -> bool {
    unimplemented!()
}

pub fn jspGetArg(v: &JsonPathItem, a: &mut JsonPathItem) {
    unimplemented!()
}

pub fn jspGetLeftArg(v: &JsonPathItem, a: &mut JsonPathItem) {
    unimplemented!()
}

pub fn jspGetRightArg(v: &JsonPathItem, a: &mut JsonPathItem) {
    unimplemented!()
}

pub fn jspGetNumeric(v: &JsonPathItem) -> Numeric {
    unimplemented!()
}

pub fn jspGetBool(v: &JsonPathItem) -> bool {
    unimplemented!()
}

/// C `char *jspGetString(v, int32 *len)` -> returns (bytes, len).
pub fn jspGetString(v: &JsonPathItem) -> (*mut u8, i32) {
    unimplemented!()
}

pub fn jspGetArraySubscript(
    v: &JsonPathItem,
    from: &mut JsonPathItem,
    to: &mut JsonPathItem,
    i: i32,
) -> bool {
    unimplemented!()
}

pub fn jspIsMutable(path: &JsonPath, varnames: &[Box<Node>], varexprs: &[Box<Node>]) -> bool {
    unimplemented!()
}

pub fn jspOperationName(t: JsonPathItemType) -> &'static str {
    unimplemented!()
}

// Parsing support data structures (in-memory).

/// In-memory parsed jsonpath item (recursive via Box).
pub struct JsonPathParseItem {
    pub r#type: JsonPathItemType,
    pub next: Option<Box<JsonPathParseItem>>, // next in path
    pub value: JsonPathParseValue,
}

pub enum JsonPathParseValue {
    Args {
        left: Option<Box<JsonPathParseItem>>,
        right: Option<Box<JsonPathParseItem>>,
    },
    Arg(Option<Box<JsonPathParseItem>>),
    Array {
        nelems: i32,
        elems: Vec<JsonPathParseArrayElem>,
    },
    AnyBounds {
        first: u32,
        last: u32,
    },
    LikeRegex {
        expr: Option<Box<JsonPathParseItem>>,
        pattern: *mut u8, // TODO(ptr): may not be null-terminated
        patternlen: u32,
        flags: u32,
    },
    Numeric(Numeric),
    Boolean(bool),
    String {
        len: u32,
        val: *mut u8, // TODO(ptr): may not be null-terminated
    },
}

pub struct JsonPathParseArrayElem {
    pub from: Option<Box<JsonPathParseItem>>,
    pub to: Option<Box<JsonPathParseItem>>,
}

pub struct JsonPathParseResult {
    pub expr: Option<Box<JsonPathParseItem>>,
    pub lax: bool,
}

pub fn parsejsonpath(
    str: &str,
    len: i32,
    escontext: Option<&mut Node>,
) -> Box<JsonPathParseResult> {
    unimplemented!()
}

/// C out-param `int *result` + bool success -> Option (None on failure).
pub fn jspConvertRegexFlags(xflags: u32, escontext: Option<&mut Node>) -> Option<i32> {
    unimplemented!()
}

/// Details about external variables passed into the jsonpath executor.
pub struct JsonPathVariable {
    pub name: *mut u8, // TODO(ptr)
    pub namelen: i32,
    pub typid: Oid,
    pub typmod: i32,
    pub value: Datum,
    pub isnull: bool,
}

// SQL/JSON query functions. The C `bool *error` / `bool *empty` out-params are
// preserved as &mut for now (they carry tri-state error suppression).

pub fn JsonPathExists(jb: Datum, jp: &JsonPath, error: Option<&mut bool>, vars: &[Datum]) -> bool {
    unimplemented!()
}

pub fn JsonPathQuery(
    jb: Datum,
    jp: &JsonPath,
    wrapper: JsonWrapper,
    empty: &mut bool,
    error: Option<&mut bool>,
    vars: &[Datum],
    column_name: &str,
) -> Datum {
    unimplemented!()
}

pub fn JsonPathValue(
    jb: Datum,
    jp: &JsonPath,
    empty: &mut bool,
    error: Option<&mut bool>,
    vars: &[Datum],
    column_name: &str,
) -> Box<JsonbValue> {
    unimplemented!()
}

/// For JSON_TABLE(). C `extern const TableFuncRoutine JsonbTableRoutine` global
/// -> accessor stub (a `static` cannot hold an `unimplemented!()` initializer).
pub fn JsonbTableRoutine() -> &'static dyn TableFuncRoutine {
    unimplemented!()
}

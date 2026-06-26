//! Translated from PostgreSQL src/include/tsearch/ts_utils.h

use bitflags::bitflags;

use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::tsearch::ts_public::HeadlineParsedText;
use crate::tsearch::ts_type::{QueryItem, QueryOperand, TSQuery, TSVector, WordEntryPos};

// Common parse definitions for tsvector and tsquery.

/// Opaque tsvector parser state (private in tsvector_parser.c).
pub struct TSVectorParseState {
    _private: (),
}

bitflags! {
    /// `P_TSV_*` - flag bits that can be passed to init_tsvector_parser.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PTsvFlags: i32 {
        const OPR_IS_DELIM = 1 << 0;
        const IS_TSQUERY   = 1 << 1;
        const IS_WEB       = 1 << 2;
    }
}

pub fn init_tsvector_parser(_input: &str, _flags: PTsvFlags) -> TSVectorParseState {
    unimplemented!()
}

pub fn reset_tsvector_parser(_state: &mut TSVectorParseState, _input: &str) {
    unimplemented!()
}

/// One token read from a tsvector parser. `pos` is the optional position array.
pub struct TSVectorToken<'a> {
    pub strval: &'a str,
    pub pos: Option<Vec<WordEntryPos>>,
    pub endptr: usize,
}

/// gettoken_tsvector: returns the next token, or None at end of input.
pub fn gettoken_tsvector(_state: &mut TSVectorParseState) -> Option<TSVectorToken<'_>> {
    unimplemented!()
}

pub fn close_tsvector_parser(_state: TSVectorParseState) {
    unimplemented!()
}

/// `ISOPERATOR` - true if the char begins a tsquery operator.
pub const fn is_operator(x: u8) -> bool {
    matches!(x, b'!' | b'&' | b'|' | b'(' | b')' | b'<')
}

// parse_tsquery.

/// Opaque tsquery parser state (private in tsquery.c).
pub struct TSQueryParserState {
    _private: (),
}

/// `PushFunction` - callback used by parse_tsquery to push values onto the stack.
/// The C `Datum opaque` context becomes captured closure state.
pub type PushFunction<'a> =
    dyn FnMut(&mut TSQueryParserState, &str, i16 /* tokenweights */, bool /* prefix */) + 'a;

bitflags! {
    /// `P_TSQ_*` - flag bits that can be passed to parse_tsquery.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PTsqFlags: i32 {
        const PLAIN = 1 << 0;
        const WEB   = 1 << 1;
    }
}

pub fn parse_tsquery(_buf: &str, _pushval: &mut PushFunction, _flags: PTsqFlags) -> TSQuery {
    unimplemented!()
}

// Functions for use by PushFunction implementations.

pub fn push_value(
    _state: &mut TSQueryParserState,
    _strval: &str,
    _weight: i16,
    _prefix: bool,
) {
    unimplemented!()
}

pub fn push_stop(_state: &mut TSQueryParserState) {
    unimplemented!()
}

pub fn push_operator(_state: &mut TSQueryParserState, _oper: i8, _distance: i16) {
    unimplemented!()
}

/// `ParsedWord` - parse plain text and lexize words.
pub struct ParsedWord {
    pub flags: u16, // currently, only TSL_PREFIX
    pub len: u16,
    pub nvariant: u16,
    pub alen: u16,
    /// Single position, or the position array (apos[0] is its length).
    pub pos: ParsedWordPos,
    pub word: Option<String>,
}

pub enum ParsedWordPos {
    Single(u16),
    Array(Vec<u16>),
}

pub struct ParsedText {
    pub words: Vec<ParsedWord>,
    pub curwords: i32,
    pub pos: i32,
}

pub fn parsetext(_cfg_id: Oid, _prs: &mut ParsedText, _buf: &str) {
    unimplemented!()
}

// headline framework.

pub fn hlparsetext(_cfg_id: Oid, _prs: &mut HeadlineParsedText, _query: TSQuery, _buf: &str) {
    unimplemented!()
}

pub fn generate_headline(_prs: &mut HeadlineParsedText) -> *mut crate::c::text {
    unimplemented!()
}

// TSQuery execution support.

/// `TSTernaryValue` - ternary logic to handle NOT with phrase matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TSTernaryValue {
    /// Definitely no match.
    No,
    /// Definitely does match.
    Yes,
    /// Can't verify match for lack of pos data.
    Maybe,
}

/// `ExecPhraseData` - lexeme position data passed to a TSExecuteCallback.
pub struct ExecPhraseData {
    pub npos: i32,
    pub allocated: bool,
    pub negate: bool,
    pub pos: Option<Vec<WordEntryPos>>,
    pub width: i32,
}

/// `TSExecuteCallback` - TSQuery lexeme check function. The C `void *arg`
/// context becomes captured closure state; `data` is an optional out-param.
pub type TSExecuteCallback<'a> =
    dyn FnMut(&QueryOperand, Option<&mut ExecPhraseData>) -> TSTernaryValue + 'a;

bitflags! {
    /// `TS_EXEC_*` - flag bits for TS_execute.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TsExecFlags: u32 {
        const EMPTY        = 0x00;
        const SKIP_NOT     = 0x01;
        const PHRASE_NO_POS = 0x02;
    }
}

pub fn ts_execute(
    _curitem: &QueryItem,
    _flags: TsExecFlags,
    _chkcond: &mut TSExecuteCallback,
) -> bool {
    unimplemented!()
}

pub fn ts_execute_ternary(
    _curitem: &QueryItem,
    _flags: TsExecFlags,
    _chkcond: &mut TSExecuteCallback,
) -> TSTernaryValue {
    unimplemented!()
}

pub fn ts_execute_locations(
    _curitem: &QueryItem,
    _flags: TsExecFlags,
    _chkcond: &mut TSExecuteCallback,
) -> Vec<ExecPhraseData> {
    unimplemented!()
}

pub fn tsquery_requires_match(_curitem: &QueryItem) -> bool {
    unimplemented!()
}

// to_ts* - text transformation to tsvector, tsquery.

pub fn make_tsvector(_prs: &mut ParsedText) -> TSVector {
    unimplemented!()
}

pub fn ts_compare_string(_a: &str, _b: &str, _prefix: bool) -> i32 {
    unimplemented!()
}

/// `TSearchStrategyNumber` - (tsvector|text) @@ tsquery.
pub const TSEARCH_STRATEGY_NUMBER: i32 = 1;
/// `TSearchWithClassStrategyNumber` - tsvector @@@ tsquery.
pub const TSEARCH_WITH_CLASS_STRATEGY_NUMBER: i32 = 2;

// TSQuery Utilities.

pub fn clean_not(_ptr: &QueryItem) -> Vec<QueryItem> {
    unimplemented!()
}

pub fn cleanup_tsquery_stopwords(_input: TSQuery, _noisy: bool) -> TSQuery {
    unimplemented!()
}

/// `QTNode` - in-memory tsquery node tree (used by query normalization).
pub struct QTNode {
    pub valnode: Option<Box<QueryItem>>,
    pub flags: QtnFlags,
    pub word: Option<String>,
    pub sign: u32,
    pub child: Vec<Self>,
}

bitflags! {
    /// `QTN_*` - bits in QTNode.flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct QtnFlags: u32 {
        const NEEDFREE = 0x01;
        const NOCHANGE = 0x02;
        const WORDFREE = 0x04;
    }
}

pub type TSQuerySign = u64;

/// `TSQS_SIGLEN` - number of bits in a TSQuerySign.
pub const TSQS_SIGLEN: usize = core::mem::size_of::<TSQuerySign>() * crate::pg_config_manual::BITS_PER_BYTE;

/// `TSQuerySignGetDatum` - pack a sign into a Datum.
pub fn tsquery_sign_get_datum(x: TSQuerySign) -> Datum {
    Datum(x as usize)
}

/// `DatumGetTSQuerySign` - unpack a sign from a Datum.
pub fn datum_get_tsquery_sign(x: Datum) -> TSQuerySign {
    x.0 as u64
}

pub fn qt2qtn(_input: &QueryItem, _operand: &str) -> QTNode {
    unimplemented!()
}

pub fn qtn2qt(_input: &QTNode) -> TSQuery {
    unimplemented!()
}

pub fn qtn_free(_input: QTNode) {
    unimplemented!()
}

pub fn qtn_sort(_input: &mut QTNode) {
    unimplemented!()
}

pub fn qtn_ternary(_input: &mut QTNode) {
    unimplemented!()
}

pub fn qtn_binary(_input: &mut QTNode) {
    unimplemented!()
}

pub fn qt_node_compare(_an: &QTNode, _bn: &QTNode) -> i32 {
    unimplemented!()
}

pub fn qtn_copy(_input: &QTNode) -> QTNode {
    unimplemented!()
}

pub fn qtn_clear_flags(_input: &mut QTNode, _flags: QtnFlags) {
    unimplemented!()
}

pub fn qtn_eq(_a: &QTNode, _b: &QTNode) -> bool {
    unimplemented!()
}

pub fn make_tsquery_sign(_a: TSQuery) -> TSQuerySign {
    unimplemented!()
}

/// findsubquery: returns the rewritten root, and whether a match was found.
pub fn findsubquery(
    _root: QTNode,
    _ex: &QTNode,
    _subs: Option<&QTNode>,
) -> (QTNode, bool) {
    unimplemented!()
}

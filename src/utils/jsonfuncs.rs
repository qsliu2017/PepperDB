//! Translated from PostgreSQL src/include/utils/jsonfuncs.h

use bitflags::bitflags;

use crate::c::text;
use crate::common::jsonapi::{JsonLexContext, JsonParseErrorType, JsonSemAction, JsonTokenType};
use crate::nodes::nodes::Node;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::jsonb::Jsonb;

bitflags! {
    /// Flag types for iterate_json(b)_values to specify which elements to iterate.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct JsonToIndex: u32 {
        const KEY     = 0x01;
        const STRING  = 0x02;
        const NUMERIC = 0x04;
        const BOOL    = 0x08;
        const ALL = Self::KEY.bits() | Self::STRING.bits() | Self::NUMERIC.bits() | Self::BOOL.bits();
    }
}

// Callback fn-pointers + void *state -> captured closures (function-mapping 6.3).
// JsonIterateStringValuesAction(void *state, char *elem_value, int elem_len)
//   -> impl FnMut(&str, i32)
// JsonTransformStringValuesAction(void *state, char *elem_value, int elem_len) -> text*
//   -> impl FnMut(&str, i32) -> text

/// Type categories returned by json_categorize_type.
pub enum JsonTypeCategory {
    Null,
    Bool,
    Numeric,
    Date,
    Timestamp,
    TimestampTz,
    Json,
    Jsonb,
    Array,
    Composite,
    Cast,
    Other,
}

pub fn makeJsonLexContext(
    lex: Option<&mut JsonLexContext>,
    json: &text,
    need_escapes: bool,
) -> Box<JsonLexContext> {
    unimplemented!()
}

/// Returns bool success + routes errors through escontext.
pub fn pg_parse_json_or_errsave(
    lex: &mut JsonLexContext,
    sem: &JsonSemAction,
    escontext: Option<&mut Node>,
) -> bool {
    unimplemented!()
}

pub fn json_errsave_error(
    error: JsonParseErrorType,
    lex: &mut JsonLexContext,
    escontext: Option<&mut Node>,
) {
    unimplemented!()
}

pub fn json_get_first_token(json: &text, throw_error: bool) -> JsonTokenType {
    unimplemented!()
}

pub fn parse_jsonb_index_flags(jb: &Jsonb) -> u32 {
    unimplemented!()
}

pub fn iterate_jsonb_values(
    jb: &Jsonb,
    flags: u32,
    action: impl FnMut(&str, i32),
) {
    unimplemented!()
}

pub fn iterate_json_values(
    json: &text,
    flags: u32,
    action: impl FnMut(&str, i32),
) {
    unimplemented!()
}

pub fn transform_jsonb_string_values(
    jsonb: &Jsonb,
    transform_action: impl FnMut(&str, i32) -> Box<text>,
) -> Box<Jsonb> {
    unimplemented!()
}

pub fn transform_json_string_values(
    json: &text,
    transform_action: impl FnMut(&str, i32) -> Box<text>,
) -> Box<text> {
    unimplemented!()
}

/// C out-params `(JsonTypeCategory *tcategory, Oid *outfuncoid)` -> tuple.
pub fn json_categorize_type(typoid: Oid, is_jsonb: bool) -> (JsonTypeCategory, Oid) {
    unimplemented!()
}

pub fn datum_to_json(val: Datum, tcategory: JsonTypeCategory, outfuncoid: Oid) -> Datum {
    unimplemented!()
}

pub fn datum_to_jsonb(val: Datum, tcategory: JsonTypeCategory, outfuncoid: Oid) -> Datum {
    unimplemented!()
}

pub fn jsonb_from_text(js: &text, unique_keys: bool) -> Datum {
    unimplemented!()
}

/// `void **cache` opaque cache + `bool *isnull` out-param folded into return.
pub fn json_populate_type(
    json_val: Datum,
    json_type: Oid,
    typid: Oid,
    typmod: i32,
    cache: &mut Option<Box<()>>, // TODO(ptr): opaque populate-type cache
    omit_quotes: bool,
    escontext: Option<&mut Node>,
) -> Option<Datum> {
    unimplemented!()
}

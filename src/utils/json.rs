//! Translated from PostgreSQL src/include/utils/json.h

// lib/stringinfo.h is a tombstone: StringInfo -> &mut String.

use crate::c::text;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

pub fn escape_json(buf: &mut String, s: &str) {
    unimplemented!()
}
pub fn escape_json_with_len(buf: &mut String, s: &str, len: i32) {
    unimplemented!()
}
pub fn escape_json_text(buf: &mut String, txt: &text) {
    unimplemented!()
}
pub fn JsonEncodeDateTime(buf: &mut String, value: Datum, typid: Oid, tzp: Option<&i32>) -> String {
    unimplemented!()
}
pub fn to_json_is_immutable(typoid: Oid) -> bool {
    unimplemented!()
}
pub fn json_build_object_worker(
    args: &[Datum],
    nulls: &[bool],
    types: &[Oid],
    absent_on_null: bool,
    unique_keys: bool,
) -> Datum {
    unimplemented!()
}
pub fn json_build_array_worker(
    args: &[Datum],
    nulls: &[bool],
    types: &[Oid],
    absent_on_null: bool,
) -> Datum {
    unimplemented!()
}
pub fn json_validate(json: &text, check_unique_keys: bool, throw_error: bool) -> bool {
    unimplemented!()
}

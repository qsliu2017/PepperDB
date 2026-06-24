//! Translated from PostgreSQL src/include/fe_utils/string_utils.h
//
// String-processing utilities for frontend code.

// PGconn (libpq-fe.h) and PQExpBuffer (pqexpbuffer.h) are libpq client types.
// TODO(struct-forward): repoint to crate::interfaces::libpq::{PGconn,PQExpBuffer} in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::interfaces::libpq::PGconn in Phase 2")]
pub struct PGconn {
    _private: (),
}
pub type PQExpBuffer = String;

// Globals controlling fmtId()/fmtQualifiedId().
pub static mut QUOTE_ALL_IDENTIFIERS: i32 = 0;
// C: `PQExpBuffer (*getLocalPQExpBuffer)(void)` -- a settable hook returning a
// scratch buffer. Modeled as an optional function pointer.
pub static mut GET_LOCAL_PQ_EXP_BUFFER: Option<fn() -> PQExpBuffer> = None;

pub fn fmt_id(_rawid: &str) -> String {
    unimplemented!()
}

pub fn fmt_id_enc(_rawid: &str, _encoding: i32) -> String {
    unimplemented!()
}

pub fn fmt_qualified_id(_schema: &str, _id: &str) -> String {
    unimplemented!()
}

pub fn fmt_qualified_id_enc(_schema: &str, _id: &str, _encoding: i32) -> String {
    unimplemented!()
}

pub fn set_fmt_encoding(_encoding: i32) {
    unimplemented!()
}

pub fn format_pg_version_number(_version_number: i32, _include_minor: bool) -> String {
    unimplemented!()
}

pub fn append_string_literal(_buf: &mut PQExpBuffer, _str: &str, _encoding: i32, _std_strings: bool) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn append_string_literal_conn(_buf: &mut PQExpBuffer, _str: &str, _conn: &PGconn) {
    unimplemented!()
}

pub fn append_string_literal_dq(_buf: &mut PQExpBuffer, _str: &str, _dqprefix: &str) {
    unimplemented!()
}

pub fn append_bytea_literal(_buf: &mut PQExpBuffer, _str: &[u8], _std_strings: bool) {
    unimplemented!()
}

pub fn append_shell_string(_buf: &mut PQExpBuffer, _str: &str) {
    unimplemented!()
}

pub fn append_shell_string_no_error(_buf: &mut PQExpBuffer, _str: &str) -> bool {
    unimplemented!()
}

pub fn append_conn_str_val(_buf: &mut PQExpBuffer, _str: &str) {
    unimplemented!()
}

pub fn append_psql_meta_connect(_buf: &mut PQExpBuffer, _dbname: &str) {
    unimplemented!()
}

/// C: `bool parsePGArray(const char *atext, char ***itemarray, int *nitems)`.
/// The success flag + out array fold into `Option<Vec<String>>`.
pub fn parse_pg_array(_atext: &str) -> Option<Vec<String>> {
    unimplemented!()
}

pub fn append_pg_array(_buffer: &mut PQExpBuffer, _value: &str) {
    unimplemented!()
}

pub fn append_reloptions_array(
    _buffer: &mut PQExpBuffer,
    _reloptions: &str,
    _prefix: &str,
    _encoding: i32,
    _std_strings: bool,
) -> bool {
    unimplemented!()
}

/// C: `bool processSQLNamePattern(..., int *dotcnt)`. Returns the success flag
/// paired with the dot count out-param.
#[allow(deprecated, clippy::too_many_arguments)]
pub fn process_sql_name_pattern(
    _conn: &PGconn,
    _buf: &mut PQExpBuffer,
    _pattern: Option<&str>,
    _have_where: bool,
    _force_escape: bool,
    _schemavar: Option<&str>,
    _namevar: Option<&str>,
    _altnamevar: Option<&str>,
    _visibilityrule: Option<&str>,
    _dbnamebuf: Option<&mut PQExpBuffer>,
) -> (bool, i32) {
    unimplemented!()
}

/// C uses an `int *dotcnt` out-param; returns the dot count.
pub fn pattern_to_sql_regex(
    _encoding: i32,
    _dbnamebuf: Option<&mut PQExpBuffer>,
    _schemabuf: &mut PQExpBuffer,
    _namebuf: &mut PQExpBuffer,
    _pattern: &str,
    _force_escape: bool,
    _want_literal_dbname: bool,
) -> i32 {
    unimplemented!()
}

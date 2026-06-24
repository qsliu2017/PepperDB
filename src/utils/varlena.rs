//! Translated from PostgreSQL src/include/utils/varlena.h
//!
//! Functions for the variable-length built-in types (text/bytea).

use crate::c::text;
use crate::postgres_ext::Oid;
use crate::utils::sortsupport::SortSupport;

pub fn varstr_cmp(_arg1: &str, _len1: i32, _arg2: &str, _len2: i32, _collid: Oid) -> i32 {
    unimplemented!()
}

pub fn varstr_sortsupport(_ssup: SortSupport<'_>, _typid: Oid, _collid: Oid) {
    unimplemented!()
}

pub fn varstr_levenshtein(
    _source: &str,
    _slen: i32,
    _target: &str,
    _tlen: i32,
    _ins_c: i32,
    _del_c: i32,
    _sub_c: i32,
    _trusted: bool,
) -> i32 {
    unimplemented!()
}

pub fn varstr_levenshtein_less_equal(
    _source: &str,
    _slen: i32,
    _target: &str,
    _tlen: i32,
    _ins_c: i32,
    _del_c: i32,
    _sub_c: i32,
    _max_d: i32,
    _trusted: bool,
) -> i32 {
    unimplemented!()
}

/// `textToQualifiedNameList` - `List *` of name strings.
pub fn text_to_qualified_name_list(_textval: &text) -> Vec<String> {
    unimplemented!()
}

/// `SplitIdentifierString` - bool success + `List **namelist` out-param.
/// Mutates `rawstring` in place in C; here returns the parsed names on success.
pub fn split_identifier_string(_rawstring: &mut str, _separator: u8) -> Option<Vec<String>> {
    unimplemented!()
}

/// `SplitDirectoriesString` - bool success + `List **namelist` out-param.
pub fn split_directories_string(_rawstring: &mut str, _separator: u8) -> Option<Vec<String>> {
    unimplemented!()
}

/// `SplitGUCList` - bool success + `List **namelist` out-param.
pub fn split_guc_list(_rawstring: &mut str, _separator: u8) -> Option<Vec<String>> {
    unimplemented!()
}

pub fn replace_text_regexp(
    _src_text: &text,
    _pattern_text: &text,
    _replace_text: &text,
    _cflags: i32,
    _collation: Oid,
    _search_start: i32,
    _n: i32,
) -> *mut text {
    unimplemented!()
}

/// State for incremental closest-match search (levenshtein).
pub struct ClosestMatchState {
    pub source: String,
    pub min_d: i32,
    pub max_d: i32,
    pub match_: Option<String>,
}

pub fn init_closest_match(_state: &mut ClosestMatchState, _source: &str, _max_d: i32) {
    unimplemented!()
}

pub fn update_closest_match(_state: &mut ClosestMatchState, _candidate: &str) {
    unimplemented!()
}

/// `getClosestMatch` - returns NULL if no match -> Option.
pub fn get_closest_match(_state: &ClosestMatchState) -> Option<&str> {
    unimplemented!()
}

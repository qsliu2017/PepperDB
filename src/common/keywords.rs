//! Translated from PostgreSQL src/include/common/keywords.h
//! PostgreSQL's list of SQL keywords (the ScanKeyword API).

use crate::common::kwlookup::ScanKeywordList;

// Keyword categories --- should match lists in gram.y.
pub const UNRESERVED_KEYWORD: u8 = 0;
pub const COL_NAME_KEYWORD: u8 = 1;
pub const TYPE_FUNC_NAME_KEYWORD: u8 = 2;
pub const RESERVED_KEYWORD: u8 = 3;

// TODO(generated): build.rs emits these tables from gram.y keyword lists.
pub fn scan_keywords() -> &'static ScanKeywordList {
    unimplemented!()
}
pub fn scan_keyword_categories() -> &'static [u8] {
    unimplemented!()
}
pub fn scan_keyword_bare_label() -> &'static [bool] {
    unimplemented!()
}

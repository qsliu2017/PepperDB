//! Translated from PostgreSQL src/include/common/keywords.h
//! PostgreSQL's list of SQL keywords (the ScanKeyword API).
//!
//! C uses `gen_keywordlist.pl` to emit a perfect-hash offset table from the
//! `PG_KEYWORD` list. Here the keyword list already lives idiomatically as the
//! ASCII-sorted `crate::parser::kwlist::KEYWORDS` static, so lookup is a binary
//! search over it - no generated hash table is needed.

use crate::parser::kwlist::{BareLabel, Keyword, KeywordCategory, KEYWORDS};

// Keyword categories --- should match lists in gram.y.
pub const UNRESERVED_KEYWORD: u8 = 0;
pub const COL_NAME_KEYWORD: u8 = 1;
pub const TYPE_FUNC_NAME_KEYWORD: u8 = 2;
pub const RESERVED_KEYWORD: u8 = 3;

/// The SQL keyword set. KEYWORDS is kept in ASCII order (kwlist.h requirement).
pub fn scan_keywords() -> &'static [Keyword] {
    KEYWORDS
}

/// C `ScanKeywordLookup`: find `text` in the SQL keyword list, returning its
/// index, or `None`. `text` must already be lower-cased (the scanner downcases
/// before lookup), matching the all-lowercase kwlist. Binary search (ASCII order).
pub fn scan_keyword_lookup(text: &str) -> Option<usize> {
    KEYWORDS.binary_search_by(|&(name, _, _)| name.cmp(text)).ok()
}

/// Category of the keyword at `idx`.
pub fn keyword_category(idx: usize) -> KeywordCategory {
    KEYWORDS[idx].1
}

/// Whether the keyword at `idx` is usable as a bare column label (no AS).
pub fn keyword_is_bare_label(idx: usize) -> bool {
    matches!(KEYWORDS[idx].2, BareLabel::BARE_LABEL)
}

/// Number of SQL keywords.
pub fn num_sql_keywords() -> usize {
    KEYWORDS.len()
}

//! Translated from PostgreSQL src/include/mb/stringinfo_mb.h
//! Multibyte-aware StringInfo support.
//
// StringInfo is tombstoned (-> String); the append helper takes `&mut String`.

/// Append `s`, quoted and multibyte-aware, truncated to `maxlen` if non-negative.
pub fn append_string_info_string_quoted(str: &mut String, s: &str, maxlen: i32) {
    let _ = (str, s, maxlen);
    unimplemented!()
}

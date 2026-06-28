//! Translated from PostgreSQL src/backend/parser/scansup.c
//! Support routines for the lexical scanner.

use crate::pg_config_manual::NAMEDATALEN;

/// PG `downcase_truncate_identifier`: downcase and truncate an identifier.
pub fn downcase_truncate_identifier(ident: &str, len: i32, warn: bool) -> String {
    downcase_identifier(ident, len, warn, true)
}

/// PG `downcase_identifier`: convert an identifier to lower case for parsing.
///
/// SQL99 specifies Unicode-aware case normalization; PG uses a dumbed-down
/// ASCII downcasing (plus a locale `tolower` for high-bit bytes in single-byte
/// encodings). We are UTF-8 throughout, so high-bit bytes are part of a
/// multibyte sequence and are left untouched - matching PG's multibyte branch.
pub fn downcase_identifier(ident: &str, len: i32, warn: bool, truncate: bool) -> String {
    let bytes = &ident.as_bytes()[..len as usize];
    // ASCII-only downcasing; high-bit bytes (UTF-8 multibyte) pass through
    // untouched - so we work on bytes and reassemble, never `byte as char`
    // (which would reinterpret a continuation byte as its own codepoint).
    let lowered: Vec<u8> = bytes.iter().map(u8::to_ascii_lowercase).collect();
    // Input was valid UTF-8 and only ASCII bytes changed, so this never fails.
    let mut result = String::from_utf8(lowered).unwrap_or_else(|_| ident[..len as usize].to_string());

    let rlen = result.len() as i32;
    if result.len() >= NAMEDATALEN && truncate {
        truncate_identifier(&mut result, rlen, warn);
    }

    result
}

/// PG `truncate_identifier`: truncate an overlength identifier in place.
pub fn truncate_identifier(ident: &mut String, len: i32, warn: bool) {
    if len as usize >= NAMEDATALEN {
        // pg_mbcliplen would clip on a char boundary; chars().take is the UTF-8
        // analog (NAMEDATALEN-1 bytes max, never splitting a multibyte char).
        let clip = char_clip_len(ident, NAMEDATALEN - 1);
        if warn {
            let truncated = &ident[..clip];
            crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_NAME_TOO_LONG)
                    .errmsg(format!("identifier \"{ident}\" will be truncated to \"{truncated}\""));
            });
        }
        ident.truncate(clip);
    }
}

/// Largest byte length <= `limit` that ends on a UTF-8 char boundary. PG's
/// `pg_mbcliplen` equivalent for the server (UTF-8) encoding.
fn char_clip_len(s: &str, limit: usize) -> usize {
    if s.len() <= limit {
        return s.len();
    }
    let mut end = limit;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    end
}

/// PG `scanner_isspace`: the whitespace set flex treats as token separators.
/// Identical to flex's, NOT libc `isspace` (which is locale-dependent).
pub fn scanner_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downcase_basic() {
        assert_eq!(downcase_identifier("SeLeCt", 6, false, true), "select");
        assert_eq!(downcase_truncate_identifier("ABC", 3, false), "abc");
    }

    #[test]
    fn downcase_keeps_highbit() {
        // UTF-8 multibyte ("é" = 0xC3 0xA9) is passed through unchanged.
        let s = "Café";
        assert_eq!(downcase_identifier(s, s.len() as i32, false, true), "café");
    }

    #[test]
    fn truncate_long_identifier() {
        let mut s = "a".repeat(100);
        let len = s.len() as i32;
        truncate_identifier(&mut s, len, false);
        assert_eq!(s.len(), NAMEDATALEN - 1);
    }

    #[test]
    fn truncate_short_noop() {
        let mut s = "short".to_string();
        truncate_identifier(&mut s, 5, false);
        assert_eq!(s, "short");
    }

    #[test]
    fn isspace_set() {
        for c in [b' ', b'\t', b'\n', b'\r', 0x0c] {
            assert!(scanner_isspace(c));
        }
        assert!(!scanner_isspace(b'x'));
        assert!(!scanner_isspace(0x0b));
    }
}

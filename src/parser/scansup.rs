//! Translated from PostgreSQL src/include/parser/scansup.h
//! Scanner support routines used by the core lexer.

pub fn downcase_truncate_identifier(_ident: &str, _len: i32, _warn: bool) -> String {
    unimplemented!()
}

pub fn downcase_identifier(_ident: &str, _len: i32, _warn: bool, _truncate: bool) -> String {
    unimplemented!()
}

pub fn truncate_identifier(_ident: &mut String, _len: i32, _warn: bool) {
    unimplemented!()
}

pub fn scanner_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

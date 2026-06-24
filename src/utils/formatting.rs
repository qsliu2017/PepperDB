//! Translated from PostgreSQL src/include/utils/formatting.h

use crate::postgres::Datum;
use crate::postgres_ext::Oid;

pub fn str_tolower(buff: &str, collid: Oid) -> String {
    let _ = (buff, collid);
    unimplemented!()
}

pub fn str_toupper(buff: &str, collid: Oid) -> String {
    let _ = (buff, collid);
    unimplemented!()
}

pub fn str_initcap(buff: &str, collid: Oid) -> String {
    let _ = (buff, collid);
    unimplemented!()
}

pub fn str_casefold(buff: &str, collid: Oid) -> String {
    let _ = (buff, collid);
    unimplemented!()
}

pub fn asc_tolower(buff: &str) -> String {
    let _ = buff;
    unimplemented!()
}

pub fn asc_toupper(buff: &str) -> String {
    let _ = buff;
    unimplemented!()
}

pub fn asc_initcap(buff: &str) -> String {
    let _ = buff;
    unimplemented!()
}

/// Outputs of parse_datetime (typid/typmod/tz out-params bundled).
pub struct ParsedDatetime {
    pub value: Datum,
    pub typid: Oid,
    pub typmod: i32,
    pub tz: i32,
}

/// parse_datetime: escontext soft-error folds into Result.
pub fn parse_datetime(
    date_txt: &str,
    fmt: &str,
    collid: Oid,
    strict: bool,
) -> Result<ParsedDatetime, String> {
    let _ = (date_txt, fmt, collid, strict);
    unimplemented!()
}

pub fn datetime_format_has_tz(fmt_str: &str) -> bool {
    let _ = fmt_str;
    unimplemented!()
}

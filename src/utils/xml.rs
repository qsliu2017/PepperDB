//! Translated from PostgreSQL src/include/utils/xml.h
//! Declarations for XML data type support.

use crate::c::{text, varlena};
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{XmlExpr, XmlOptionType};
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

/// XML value: a varlena (on-disk text-like layout).
pub type xmltype = varlena;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlStandaloneType {
    Yes,
    No,
    NoValue,
    Omitted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XmlBinaryType {
    Base64,
    Hex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgXmlStrictness {
    /// ignore errors unless function result indicates error condition
    Legacy,
    /// ignore non-parser messages
    Wellformed,
    /// report all notices/warnings/errors
    All,
}

/// struct PgXmlErrorContext is private to xml.c.
pub struct PgXmlErrorContext {
    _private: (),
}

pub fn DatumGetXmlP(_x: Datum) -> Box<xmltype> {
    unimplemented!() // PG_DETOAST_DATUM
}

pub fn XmlPGetDatum(_x: &xmltype) -> Datum {
    unimplemented!() // PointerGetDatum
}

pub fn pg_xml_init_library() {
    unimplemented!()
}

pub fn pg_xml_init(_strictness: PgXmlStrictness) -> Box<PgXmlErrorContext> {
    unimplemented!()
}

pub fn pg_xml_done(_errcxt: &mut PgXmlErrorContext, _is_error: bool) {
    unimplemented!()
}

pub fn pg_xml_error_occurred(_errcxt: &PgXmlErrorContext) -> bool {
    unimplemented!()
}

pub fn xml_ereport(_errcxt: &mut PgXmlErrorContext, _level: i32, _sqlcode: i32, _msg: &str) {
    unimplemented!()
}

pub fn xmlconcat(_args: Vec<Node>) -> Box<xmltype> {
    unimplemented!()
}

pub fn xmlelement(
    _xexpr: &XmlExpr,
    _named_argvalue: &[Datum],
    _named_argnull: &[bool],
    _argvalue: &[Datum],
    _argnull: &[bool],
) -> Box<xmltype> {
    unimplemented!()
}

pub fn xmlparse(
    _data: &text,
    _xmloption_arg: XmlOptionType,
    _preserve_whitespace: bool,
) -> Box<xmltype> {
    unimplemented!()
}

pub fn xmlpi(_target: &str, _arg: &text, _arg_is_null: bool) -> (Box<xmltype>, bool) {
    unimplemented!() // out-param `result_is_null` folded into the tuple
}

pub fn xmlroot(_data: &xmltype, _version: &text, _standalone: i32) -> Box<xmltype> {
    unimplemented!()
}

pub fn xml_is_document(_arg: &xmltype) -> bool {
    unimplemented!()
}

pub fn xmltotext_with_options(
    _data: &xmltype,
    _xmloption_arg: XmlOptionType,
    _indent: bool,
) -> Box<text> {
    unimplemented!()
}

pub fn escape_xml(_str: &str) -> String {
    unimplemented!()
}

pub fn map_sql_identifier_to_xml_name(
    _ident: &str,
    _fully_escaped: bool,
    _escape_period: bool,
) -> String {
    unimplemented!()
}

pub fn map_xml_name_to_sql_identifier(_name: &str) -> String {
    unimplemented!()
}

pub fn map_sql_value_to_xml_value(_value: Datum, _typ: Oid, _xml_escape_strings: bool) -> String {
    unimplemented!()
}

/// XmlBinaryType, but int for guc enum.
pub static mut xmlbinary: i32 = 0;

/// XmlOptionType, but int for guc enum.
pub static mut xmloption: i32 = 0;

/// TableFuncRoutine for the SQL/XML `XMLTABLE` construct.
/// (routine struct -> trait; the C `const XmlTableRoutine` is the impl singleton.)
pub struct XmlTableRoutine;

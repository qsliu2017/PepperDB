//! Translated from PostgreSQL src/include/fe_utils/print.h
//
// Query-result printing support for frontend code. All in-memory; no layout
// contract.

use crate::postgres_ext::Oid;

/// Opaque frontend libpq handle; client lib not ported.
pub struct PGresult {
    _private: (),
}
// C FILE* sink. TODO(ptr): re-type to a std::io::Write at the I/O boundary.
pub struct CFile {
    _private: (),
}

pub const DEFAULT_PAGER: &str = "more";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrintFormat {
    Nothing = 0, // ensure callers initialize this
    Aligned,
    Asciidoc,
    Csv,
    Html,
    Latex,
    LatexLongtable,
    TroffMs,
    Unaligned,
    Wrapped,
}

/// Line drawing characters for a given context.
pub struct PrintTextLineFormat {
    pub hrule: &'static str,
    pub leftvrule: &'static str,
    pub midvrule: &'static str,
    pub rightvrule: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrintTextRule {
    Top = 0,
    Middle,
    Bottom,
    Data,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrintTextLineWrap {
    None,
    Wrap,
    Newline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrintXheaderWidthType {
    Full,
    Column,
    Page,
    ExactWidth,
}

/// A complete line style.
pub struct PrintTextFormat {
    pub name: &'static str,
    pub lrule: [PrintTextLineFormat; 4], // indexed by PrintTextRule
    pub midvrule_nl: &'static str,
    pub midvrule_wrap: &'static str,
    pub midvrule_blank: &'static str,
    pub header_nl_left: &'static str,
    pub header_nl_right: &'static str,
    pub nl_left: &'static str,
    pub nl_right: &'static str,
    pub wrap_left: &'static str,
    pub wrap_right: &'static str,
    pub wrap_right_border: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnicodeLinestyle {
    Single = 0,
    Double,
}

pub struct Separator {
    pub separator: Option<String>,
    pub separator_zero: bool,
}

pub struct PrintTableOpt {
    pub format: PrintFormat,
    pub expanded: u16, // 0=no, 1=yes, 2=auto
    pub expanded_header_width_type: PrintXheaderWidthType,
    pub expanded_header_exact_width: i32,
    pub border: u16, // 0=none, 1=dividing lines, 2=full
    pub pager: u16,  // 0=off 1=on 2=always
    pub pager_min_lines: i32,
    pub tuples_only: bool,
    pub start_table: bool,
    pub stop_table: bool,
    pub default_footer: bool,
    pub prior_records: u64,
    pub line_style: Option<&'static PrintTextFormat>,
    pub field_sep: Separator,
    pub record_sep: Separator,
    pub csv_field_sep: [u8; 2],
    pub numeric_locale: bool,
    pub table_attr: Option<String>,
    pub encoding: i32,
    pub env_columns: i32,
    pub columns: i32,
    pub unicode_border_linestyle: UnicodeLinestyle,
    pub unicode_column_linestyle: UnicodeLinestyle,
    pub unicode_header_linestyle: UnicodeLinestyle,
}

// C's singly-linked footer list collapses to a Vec on PrintTableContent.
pub struct PrintTableContent<'a> {
    pub opt: &'a PrintTableOpt,
    pub title: Option<String>,
    pub ncolumns: i32,
    pub nrows: i32,
    pub headers: Vec<String>,
    pub cells: Vec<String>,
    pub cellsadded: u64,
    pub footers: Vec<String>,
    pub aligns: Vec<u8>, // 'l' or 'r' per column
}

pub struct PrintQueryOpt {
    pub topt: PrintTableOpt,
    pub null_print: Option<String>,
    pub title: Option<String>,
    pub footers: Option<Vec<String>>,
    pub translate_header: bool,
    pub translate_columns: Vec<bool>,
}

// volatile sig_atomic_t set from the cancel handler.
pub static CANCEL_PRESSED: core::sync::atomic::AtomicBool =
    core::sync::atomic::AtomicBool::new(false);

pub fn disable_sigpipe_trap() {
    unimplemented!()
}

pub fn restore_sigpipe_trap() {
    unimplemented!()
}

pub fn set_sigpipe_trap_state(_ignore: bool) {
    unimplemented!()
}

pub fn page_output(_lines: i32, _topt: &PrintTableOpt) -> CFile {
    unimplemented!()
}

pub fn close_pager(_pagerpipe: CFile) {
    unimplemented!()
}

pub fn html_escaped_print(_in: &str, _fout: &mut CFile) {
    unimplemented!()
}

pub fn print_table_init<'a>(
    _opt: &'a PrintTableOpt,
    _title: Option<&str>,
    _ncolumns: i32,
    _nrows: i32,
) -> PrintTableContent<'a> {
    unimplemented!()
}

pub fn print_table_add_header(
    _content: &mut PrintTableContent,
    _header: String,
    _translate: bool,
    _align: u8,
) {
    unimplemented!()
}

pub fn print_table_add_cell(
    _content: &mut PrintTableContent,
    _cell: String,
    _translate: bool,
    _mustfree: bool,
) {
    unimplemented!()
}

pub fn print_table_add_footer(_content: &mut PrintTableContent, _footer: &str) {
    unimplemented!()
}

pub fn print_table_set_footer(_content: &mut PrintTableContent, _footer: &str) {
    unimplemented!()
}

pub fn print_table_cleanup(_content: &mut PrintTableContent) {
    unimplemented!()
}

pub fn print_table(_cont: &PrintTableContent, _fout: &mut CFile, _is_pager: bool, _flog: &mut CFile) {
    unimplemented!()
}

pub fn print_query(
    _result: &PGresult,
    _opt: &PrintQueryOpt,
    _fout: &mut CFile,
    _is_pager: bool,
    _flog: &mut CFile,
) {
    unimplemented!()
}

pub fn column_type_alignment(_typid: Oid) -> u8 {
    unimplemented!()
}

pub fn set_decimal_locale() {
    unimplemented!()
}

pub fn get_line_style(_opt: &PrintTableOpt) -> &'static PrintTextFormat {
    unimplemented!()
}

pub fn refresh_utf8format(_opt: &PrintTableOpt) {
    unimplemented!()
}

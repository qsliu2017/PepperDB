//! Translated from PostgreSQL src/include/commands/explain_format.h
//! Prototypes for explain_format.c (output formatting for EXPLAIN).

use crate::commands::explain_state::ExplainState;
pub use crate::commands::explain_state::ExplainFormat;

pub fn explain_property_list(_qlabel: &str, _data: &[String], _es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_property_list_nested(_qlabel: &str, _data: &[String], _es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_property_text(_qlabel: &str, _value: &str, _es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_property_integer(_qlabel: &str, _unit: &str, _value: i64, _es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_property_uinteger(_qlabel: &str, _unit: &str, _value: u64, _es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_property_float(
    _qlabel: &str,
    _unit: &str,
    _value: f64,
    _ndigits: i32,
    _es: &mut ExplainState,
) {
    unimplemented!()
}

pub fn explain_property_bool(_qlabel: &str, _value: bool, _es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_open_group(_objtype: &str, _labelname: &str, _labeled: bool, _es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_close_group(_objtype: &str, _labelname: &str, _labeled: bool, _es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_open_set_aside_group(
    _objtype: &str,
    _labelname: &str,
    _labeled: bool,
    _depth: i32,
    _es: &mut ExplainState,
) {
    unimplemented!()
}

/// C writes the saved group state through `int *state_save` -> return it.
pub fn explain_save_group(_es: &mut ExplainState, _depth: i32) -> i32 {
    unimplemented!()
}

/// C reads the saved group state from `int *state_save` -> take it by value.
pub fn explain_restore_group(_es: &mut ExplainState, _depth: i32, _state_save: i32) {
    unimplemented!()
}

pub fn explain_dummy_group(_objtype: &str, _labelname: &str, _es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_begin_output(_es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_end_output(_es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_separate_plans(_es: &mut ExplainState) {
    unimplemented!()
}

pub fn explain_indent_text(_es: &mut ExplainState) {
    unimplemented!()
}

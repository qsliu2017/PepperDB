//! Translated from PostgreSQL src/include/utils/guc_tables.h
//! Declarations of tables used by GUC. See src/backend/utils/misc/README.
//!
//! The record types this header defines in C (`config_type`, `config_var_val`,
//! `config_var_value`, `config_group`, `GucStackState`, `GucStack`,
//! `config_generic`, `config_bool`/`int`/`real`/`string`/`enum`, the status
//! bits) are already translated in `crate::utils::guc` (the task grouped them
//! there). They are re-exported here so the canonical guc_tables.h items resolve
//! from this module, and this file adds the table machinery (name arrays, the
//! built-in ConfigureNames* arrays, and the lookup/build functions).

pub use crate::utils::guc::{
    config_bool, config_enum, config_generic, config_group, config_int, config_real, config_string,
    config_type, config_var_val, config_var_value, GucStack, GucStackState, GUC_IS_IN_FILE,
    GUC_NEEDS_REPORT, GUC_PENDING_RESTART,
};

// Constant tables corresponding to the enums above and in guc.h.
// (C `const char *const xxx[]`; indexed by the matching enum's discriminant.)
pub static config_group_names: &[&str] = &[];
pub static config_type_names: &[&str] = &[];
pub static GucContext_Names: &[&str] = &[];
pub static GucSource_Names: &[&str] = &[];

// Data arrays defining all the built-in GUC variables. Populated at startup;
// empty in the skeleton.
pub const ConfigureNamesBool: &[config_bool] = &[];
pub const ConfigureNamesInt: &[config_int] = &[];
pub const ConfigureNamesReal: &[config_real] = &[];
pub const ConfigureNamesString: &[config_string] = &[];
pub const ConfigureNamesEnum: &[config_enum] = &[];

/// Look up a GUC variable, returning a config_generic. None if unknown (the C
/// NULL return when not found / skip_errors).
pub fn find_option(
    _name: &str,
    _create_placeholders: bool,
    _skip_errors: bool,
    _elevel: i32,
) -> Option<&'static mut config_generic> {
    unimplemented!()
}

/// Variables to show in EXPLAIN (C returns array + count out-param).
pub fn get_explain_guc_options() -> Vec<&'static mut config_generic> {
    unimplemented!()
}

/// Get the string value of a variable.
pub fn ShowGUCOption(_record: &config_generic, _use_units: bool) -> String {
    unimplemented!()
}

/// Whether the GUC variable is visible to the current user.
pub fn ConfigOptionIsVisible(_conf: &config_generic) -> bool {
    unimplemented!()
}

/// The current set of variables (C returns array + count out-param).
pub fn get_guc_variables() -> Vec<&'static mut config_generic> {
    unimplemented!()
}

pub fn build_guc_variables() {
    unimplemented!()
}

// Search in enum options.

/// Map an enum GUC's value to its option name; None if not found.
pub fn config_enum_lookup_by_value(_record: &config_enum, _val: i32) -> Option<&'static str> {
    unimplemented!()
}

/// Map an enum GUC's option name to its value; None if not a valid name (C bool
/// + int out-param).
pub fn config_enum_lookup_by_name(_record: &config_enum, _value: &str) -> Option<i32> {
    unimplemented!()
}

pub fn config_enum_get_options(
    _record: &config_enum,
    _prefix: &str,
    _suffix: &str,
    _separator: &str,
) -> String {
    unimplemented!()
}

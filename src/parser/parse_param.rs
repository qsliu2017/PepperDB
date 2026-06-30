//! Translated from PostgreSQL src/include/parser/parse_param.h

pub use crate::backend::parser::parse_param::{
    check_variable_parameters, query_contains_extern_params, setup_parse_fixed_parameters,
    setup_parse_variable_parameters,
};

// Re-export the hook-state types so callers (analyze.c, prepare.c, the
// extended-protocol Parse path) can construct/inspect parameter setups.
pub use crate::backend::parser::parse_param::{
    collected_param_types, FixedParamState, ParamRefHookState, VarParamState,
};

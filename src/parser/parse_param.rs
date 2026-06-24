//! Translated from PostgreSQL src/include/parser/parse_param.h

use crate::nodes::parsenodes::Query;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

pub fn setup_parse_fixed_parameters(
    _pstate: &mut ParseState,
    _param_types: &[Oid],
    _num_params: i32,
) {
    unimplemented!()
}

/// `paramTypes`/`numParams` are in/out -> a growable `Vec<Oid>`.
pub fn setup_parse_variable_parameters(_pstate: &mut ParseState, _param_types: &mut Vec<Oid>) {
    unimplemented!()
}

pub fn check_variable_parameters(_pstate: &mut ParseState, _query: &mut Query) {
    unimplemented!()
}

pub fn query_contains_extern_params(_query: &Query) -> bool {
    unimplemented!()
}

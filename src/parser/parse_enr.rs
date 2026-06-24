//! Translated from PostgreSQL src/include/parser/parse_enr.h

use crate::parser::parse_node::ParseState;
use crate::utils::queryenvironment::EphemeralNamedRelationMetadata;

pub fn name_matches_visible_ENR(_pstate: &mut ParseState, _refname: &str) -> bool {
    unimplemented!()
}

pub fn get_visible_ENR(
    _pstate: &mut ParseState,
    _refname: &str,
) -> EphemeralNamedRelationMetadata {
    unimplemented!()
}

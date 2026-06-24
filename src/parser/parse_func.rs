//! Translated from PostgreSQL src/include/parser/parse_func.h

use crate::catalog::namespace::FuncCandidateList;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{FuncCall, ObjectType, ObjectWithArgs};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

/// Result codes for func_get_detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuncDetailCode {
    /// no matching function
    NotFound,
    /// too many matching functions
    Multiple,
    /// found a matching regular function
    Normal,
    /// found a matching procedure
    Procedure,
    /// found a matching aggregate function
    Aggregate,
    /// found a matching window function
    WindowFunc,
    /// it's a type coercion request
    Coercion,
}

/// Outputs of `func_get_detail` (formerly trailing pointer out-params).
pub struct FuncDetail {
    pub funcid: Oid,
    pub rettype: Oid,
    pub retset: bool,
    pub nvargs: i32,
    pub vatype: Oid,
    pub true_typeids: Vec<Oid>,
    pub argdefaults: Vec<Box<Node>>,
}

pub fn ParseFuncOrColumn(
    _pstate: &mut ParseState,
    _funcname: Vec<Box<Node>>,
    _fargs: Vec<Box<Node>>,
    _last_srf: Option<Box<Node>>,
    _fn_: &FuncCall,
    _proc_call: bool,
    _location: i32,
) -> Box<Node> {
    unimplemented!()
}

pub fn func_get_detail(
    _funcname: Vec<Box<Node>>,
    _fargs: Vec<Box<Node>>,
    _fargnames: Vec<Box<Node>>,
    _nargs: i32,
    _argtypes: &[Oid],
    _expand_variadic: bool,
    _expand_defaults: bool,
    _include_out_arguments: bool,
) -> (FuncDetailCode, FuncDetail) {
    unimplemented!()
}

/// Returns the number of matches; fills `candidates` out-param.
pub fn func_match_argtypes(
    _nargs: i32,
    _input_typeids: &[Oid],
    _raw_candidates: FuncCandidateList,
    _candidates: &mut FuncCandidateList,
) -> i32 {
    unimplemented!()
}

pub fn func_select_candidate(
    _nargs: i32,
    _input_typeids: &[Oid],
    _candidates: FuncCandidateList,
) -> FuncCandidateList {
    unimplemented!()
}

pub fn make_fn_arguments(
    _pstate: &mut ParseState,
    _fargs: &mut [Box<Node>],
    _actual_arg_types: &[Oid],
    _declared_arg_types: &[Oid],
) {
    unimplemented!()
}

pub fn funcname_signature_string(
    _funcname: &str,
    _nargs: i32,
    _argnames: &[Box<Node>],
    _argtypes: &[Oid],
) -> String {
    unimplemented!()
}

pub fn func_signature_string(
    _funcname: Vec<Box<Node>>,
    _nargs: i32,
    _argnames: &[Box<Node>],
    _argtypes: &[Oid],
) -> String {
    unimplemented!()
}

/// `InvalidOid` sentinel (when `missing_ok`) -> `Option`.
pub fn LookupFuncName(
    _funcname: Vec<Box<Node>>,
    _nargs: i32,
    _argtypes: &[Oid],
    _missing_ok: bool,
) -> Option<Oid> {
    unimplemented!()
}

/// `InvalidOid` sentinel (when `missing_ok`) -> `Option`.
pub fn LookupFuncWithArgs(
    _objtype: ObjectType,
    _func: &ObjectWithArgs,
    _missing_ok: bool,
) -> Option<Oid> {
    unimplemented!()
}

pub fn check_srf_call_placement(
    _pstate: &mut ParseState,
    _last_srf: Option<Box<Node>>,
    _location: i32,
) {
    unimplemented!()
}

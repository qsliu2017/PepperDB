//! Translated from PostgreSQL src/include/parser/parse_agg.h

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{Query, WindowDef};
use crate::nodes::primnodes::{Aggref, GroupingFunc, WindowFunc};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

pub fn transformAggregateCall(
    _pstate: &mut ParseState,
    _agg: &mut Aggref,
    _args: Vec<Box<Node>>,
    _aggorder: Vec<Box<Node>>,
    _agg_distinct: bool,
) {
    unimplemented!()
}

pub fn transformGroupingFunc(_pstate: &mut ParseState, _p: &mut GroupingFunc) -> Box<Node> {
    unimplemented!()
}

pub fn transformWindowFuncCall(
    _pstate: &mut ParseState,
    _wfunc: &mut WindowFunc,
    _windef: &WindowDef,
) {
    unimplemented!()
}

pub fn parseCheckAggregates(_pstate: &mut ParseState, _qry: &mut Query) {
    unimplemented!()
}

pub fn expand_grouping_sets(
    _grouping_sets: Vec<Box<Node>>,
    _group_distinct: bool,
    _limit: i32,
) -> Vec<Box<Node>> {
    unimplemented!()
}

/// Returns the number of aggregate input arguments; fills `input_types`.
pub fn get_aggregate_argtypes(_aggref: &Aggref, _input_types: &mut [Oid]) -> i32 {
    unimplemented!()
}

pub fn resolve_aggregate_transtype(
    _aggfuncid: Oid,
    _aggtranstype: Oid,
    _input_types: &[Oid],
    _num_arguments: i32,
) -> Oid {
    unimplemented!()
}

pub fn agg_args_support_sendreceive(_aggref: &Aggref) -> bool {
    unimplemented!()
}

/// Builds the transfn (and optional inverse transfn) expression trees.
pub fn build_aggregate_transfn_expr(
    _agg_input_types: &[Oid],
    _agg_num_inputs: i32,
    _agg_num_direct_inputs: i32,
    _agg_variadic: bool,
    _agg_state_type: Oid,
    _agg_input_collation: Oid,
    _transfn_oid: Oid,
    _invtransfn_oid: Oid,
) -> (Box<Node>, Option<Box<Node>>) {
    unimplemented!()
}

pub fn build_aggregate_serialfn_expr(_serialfn_oid: Oid) -> Box<Node> {
    unimplemented!()
}

pub fn build_aggregate_deserialfn_expr(_deserialfn_oid: Oid) -> Box<Node> {
    unimplemented!()
}

pub fn build_aggregate_finalfn_expr(
    _agg_input_types: &[Oid],
    _num_finalfn_inputs: i32,
    _agg_state_type: Oid,
    _agg_result_type: Oid,
    _agg_input_collation: Oid,
    _finalfn_oid: Oid,
) -> Box<Node> {
    unimplemented!()
}

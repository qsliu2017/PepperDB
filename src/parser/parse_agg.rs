//! Translated from PostgreSQL src/include/parser/parse_agg.h

use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{Aggref, GroupingFunc};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

/// PG `transformAggregateCall`. See `crate::backend::parser::parse_agg` (M5 body):
/// fills the Aggref's args/aggargtypes and marks `pstate.p_has_aggs`.
pub use crate::backend::parser::parse_agg::transformAggregateCall;

/// PG `transformWindowFuncCall`. See `crate::backend::parser::parse_agg` (M12 body):
/// records the WindowDef in pstate.p_windowdefs and links the WindowFunc's winref.
pub use crate::backend::parser::parse_agg::transformWindowFuncCall;

pub fn transformGroupingFunc(_pstate: &mut ParseState, _p: &mut GroupingFunc) -> Node {
    unimplemented!()
}

/// PG `parseCheckAggregates`. See `crate::backend::parser::parse_agg` (M5 body):
/// verifies the targetlist/HAVING reference only grouped columns or aggregates.
pub use crate::backend::parser::parse_agg::parseCheckAggregates;

pub fn expand_grouping_sets(
    _grouping_sets: Vec<Node>,
    _group_distinct: bool,
    _limit: i32,
) -> Vec<Node> {
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
) -> (Node, Option<Node>) {
    unimplemented!()
}

pub fn build_aggregate_serialfn_expr(_serialfn_oid: Oid) -> Node {
    unimplemented!()
}

pub fn build_aggregate_deserialfn_expr(_deserialfn_oid: Oid) -> Node {
    unimplemented!()
}

pub fn build_aggregate_finalfn_expr(
    _agg_input_types: &[Oid],
    _num_finalfn_inputs: i32,
    _agg_state_type: Oid,
    _agg_result_type: Oid,
    _agg_input_collation: Oid,
    _finalfn_oid: Oid,
) -> Node {
    unimplemented!()
}

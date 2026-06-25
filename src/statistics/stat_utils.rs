//! Translated from PostgreSQL src/include/statistics/stat_utils.h

use crate::fmgr::FunctionCallInfo;
use crate::nodes::primnodes::RangeVar;
use crate::postgres_ext::Oid;

pub struct StatsArgInfo {
    pub argname: &'static str,
    pub argtype: Oid,
}

pub fn stats_check_required_arg(fcinfo: FunctionCallInfo<'_>, arginfo: &[StatsArgInfo], argnum: i32) {
    unimplemented!()
}

pub fn stats_check_arg_array(fcinfo: FunctionCallInfo<'_>, arginfo: &[StatsArgInfo], argnum: i32) -> bool {
    unimplemented!()
}

pub fn stats_check_arg_pair(
    fcinfo: FunctionCallInfo<'_>,
    arginfo: &[StatsArgInfo],
    argnum1: i32,
    argnum2: i32,
) -> bool {
    unimplemented!()
}

pub fn RangeVarCallbackForStats(relation: &RangeVar, rel_id: Oid, old_relid: Oid, arg: *mut ()) {
    // TODO(ptr): void *arg -> closure context once call sites are known
    unimplemented!()
}

pub fn stats_fill_fcinfo_from_arg_pairs(
    pairs_fcinfo: FunctionCallInfo<'_>,
    positional_fcinfo: FunctionCallInfo<'_>,
    arginfo: &[StatsArgInfo],
) -> bool {
    unimplemented!()
}

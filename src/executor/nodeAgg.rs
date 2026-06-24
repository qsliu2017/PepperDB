//! Translated from PostgreSQL src/include/executor/nodeAgg.h

use crate::access::attnum::AttrNumber;
use crate::access::parallel::{ParallelContext, ParallelWorkerContext};
use crate::nodes::bitmapset::Bitmapset;
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::execnodes::{
    AggState, EState, ExprState, FmgrInfo, FunctionCallInfo, TupleDesc, TupleHashIterator,
    TupleHashTable,
};
use crate::nodes::nodes::AggStrategy;
use crate::nodes::plannodes::{Agg, Sort};
use crate::nodes::primnodes::Aggref;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::tuplesort::Tuplesortstate;

/// AggStatePerTransData - per aggregate state value working state.
#[allow(deprecated)]
pub struct AggStatePerTransData {
    pub aggref: Option<Box<Aggref>>,
    pub aggshared: bool,
    pub aggsortrequired: bool,
    pub num_inputs: i32,
    pub num_trans_inputs: i32,
    pub transfn_oid: Oid,
    pub serialfn_oid: Oid,
    pub deserialfn_oid: Oid,
    pub aggtranstype: Oid,
    pub transfn: FmgrInfo,
    pub serialfn: FmgrInfo,
    pub deserialfn: FmgrInfo,
    pub agg_collation: Oid,
    pub num_sort_cols: i32,
    pub num_distinct_cols: i32,
    pub sort_col_idx: Vec<AttrNumber>,
    pub sort_operators: Vec<Oid>,
    pub sort_collations: Vec<Oid>,
    pub sort_nulls_first: Vec<bool>,
    pub equalfn_one: FmgrInfo,
    pub equalfn_multi: Option<Box<ExprState>>,
    /// initial value from pg_aggregate entry (NULL -> None).
    pub init_value: Option<Datum>,
    pub inputtype_len: i16,
    pub transtype_len: i16,
    pub inputtype_by_val: bool,
    pub transtype_by_val: bool,
    pub sortslot: Option<Box<TupleTableSlot>>,
    pub uniqslot: Option<Box<TupleTableSlot>>,
    pub sortdesc: TupleDesc,
    /// single-column DISTINCT last value (NULL/none -> None).
    pub lastdatum: Option<Datum>,
    pub haslast: bool,
    pub sortstates: Vec<Box<Tuplesortstate>>,
    pub transfn_fcinfo: FunctionCallInfo,
    pub serialfn_fcinfo: FunctionCallInfo,
    pub deserialfn_fcinfo: FunctionCallInfo,
}

/// AggStatePerAggData - per-aggregate information for the final function.
#[allow(deprecated)]
pub struct AggStatePerAggData {
    pub aggref: Option<Box<Aggref>>,
    pub transno: i32,
    pub finalfn_oid: Oid,
    pub finalfn: FmgrInfo,
    pub num_final_args: i32,
    pub aggdirectargs: Vec<Box<ExprState>>,
    pub resulttype_len: i16,
    pub resulttype_by_val: bool,
    pub shareable: bool,
}

/// AggStatePerGroupData - per-aggregate-per-group working state.
pub struct AggStatePerGroupData {
    pub trans_value: Datum,
    pub trans_value_is_null: bool,
    pub no_trans_value: bool,
}

/// AggStatePerPhaseData - per-grouping-set-phase state.
#[allow(deprecated)]
pub struct AggStatePerPhaseData {
    pub aggstrategy: AggStrategy,
    pub numsets: i32,
    pub gset_lengths: Vec<i32>,
    pub grouped_cols: Vec<Box<Bitmapset>>,
    pub eqfunctions: Vec<Box<ExprState>>,
    pub aggnode: Option<Box<Agg>>,
    pub sortnode: Option<Box<Sort>>,
    pub evaltrans: Option<Box<ExprState>>,
    /// first idx: 0 outerops / 1 TTSOpsMinimalTuple; second: 0 no-null / 1 null-check.
    pub evaltrans_cache: [[Option<Box<ExprState>>; 2]; 2],
}

/// AggStatePerHashData - per-hashtable state (one per grouping set).
#[allow(deprecated)]
pub struct AggStatePerHashData {
    pub hashtable: TupleHashTable,
    pub hashiter: TupleHashIterator,
    pub hashslot: Option<Box<TupleTableSlot>>,
    pub hashfunctions: Vec<FmgrInfo>,
    pub eqfuncoids: Vec<Oid>,
    pub num_cols: i32,
    pub numhash_grp_cols: i32,
    pub largest_grp_col_idx: i32,
    pub hash_grp_col_idx_input: Vec<AttrNumber>,
    pub hash_grp_col_idx_hash: Vec<AttrNumber>,
    pub aggnode: Option<Box<Agg>>,
}

// TODO(ptr)
pub fn ExecInitAgg(_node: &Agg, _estate: &mut EState, _eflags: i32) -> *mut AggState {
    unimplemented!()
}

pub fn ExecEndAgg(_node: &mut AggState) {
    unimplemented!()
}

pub fn ExecReScanAgg(_node: &mut AggState) {
    unimplemented!()
}

pub fn hash_agg_entry_size(
    _num_trans: i32,
    _tuple_width: usize,
    _transition_space: usize,
) -> usize {
    unimplemented!()
}

/// Out-params (mem_limit, ngroups_limit, num_partitions) -> named struct.
pub struct HashAggLimits {
    pub mem_limit: usize,
    pub ngroups_limit: u64,
    pub num_partitions: i32,
}

pub fn hash_agg_set_limits(
    _hashentrysize: f64,
    _input_groups: f64,
    _used_bits: i32,
) -> HashAggLimits {
    unimplemented!()
}

pub fn ExecAggEstimate(_node: &mut AggState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecAggInitializeDSM(_node: &mut AggState, _pcxt: &mut ParallelContext) {
    unimplemented!()
}

pub fn ExecAggInitializeWorker(_node: &mut AggState, _pwcxt: &mut ParallelWorkerContext) {
    unimplemented!()
}

pub fn ExecAggRetrieveInstrumentation(_node: &mut AggState) {
    unimplemented!()
}

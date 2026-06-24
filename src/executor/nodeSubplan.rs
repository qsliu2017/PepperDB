//! Translated from PostgreSQL src/include/executor/nodeSubplan.h

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::execnodes::{ExprContext, PlanState, SubPlanState};
use crate::nodes::primnodes::SubPlan;
use crate::postgres::Datum;

pub fn ExecInitSubPlan(_subplan: &SubPlan, _parent: &mut PlanState) -> *mut SubPlanState {
    unimplemented!() // TODO(ptr)
}
// Datum + bool *isNull out-param -> Option<Datum> (None == SQL NULL).
pub fn ExecSubPlan(_node: &mut SubPlanState, _econtext: &mut ExprContext) -> Option<Datum> {
    unimplemented!()
}
pub fn ExecReScanSetParamPlan(_node: &mut SubPlanState, _parent: &mut PlanState) {
    unimplemented!()
}
pub fn ExecSetParamPlan(_node: &mut SubPlanState, _econtext: &mut ExprContext) {
    unimplemented!()
}
pub fn ExecSetParamPlanMulti(_params: &Bitmapset, _econtext: &mut ExprContext) {
    unimplemented!()
}

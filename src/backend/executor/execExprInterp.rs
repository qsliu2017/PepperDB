//! Expression interpreter. Translated from
//! backend/executor/execExprInterp.c (disposition: grow).
//!
//! Step 08 translates the PORTABLE switch-based `ExecInterpExpr` (PG's
//! `EEO_SWITCH`/`EEO_CASE` path, NOT the computed-goto path) as a Rust `match`
//! over the opcode, filling only `EEOP_CONST`, `EEOP_ASSIGN_TMP[_MAKE_RO]`,
//! `EEOP_DONE_RETURN` and `EEOP_DONE_NO_RETURN`. Every other opcode is a clean
//! `not_yet_reachable` arm that grows in later milestones (rules.md s4).
//!
//! Execution is fully synchronous (rules.md s5): the const/projection path
//! touches no I/O leaf, so the interpreter does not `.await`.

use crate::executor::execExpr::{ExprEvalOp, ExprEvalStepData};
use crate::executor::tuptable::slot_getsomeattrs;
use crate::nodes::execnodes::{EeoFlag, ExprContext, ExprState};
use crate::postgres::Datum;

/// Panic for an opcode not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(op: ExprEvalOp) -> ! {
    unimplemented!("ExecInterpExpr: opcode {op:?} not yet translated for this milestone");
}

/// PG `ExecReadyInterpretedExpr`: finalize a compiled ExprState for the
/// interpreter. The portable path sets `evalfunc = ExecInterpExpr` directly
/// (PG additionally routes the first call through `ExecInterpExprStillValid` to
/// re-validate; that validity dance is a JIT/plan-cache concern with no analogue
/// on the M1 path, so the interpreter is installed straight away). The
/// fast-path specializations (ExecJust*) are a perf optimization left to grow.
pub fn exec_ready_interpreted_expr(state: &mut ExprState) {
    crate::assert!(!state.steps.is_empty());
    let last = state.steps.last().map(|s| s.opcode);
    crate::assert!(matches!(
        last,
        Some(ExprEvalOp::DONE_RETURN | ExprEvalOp::DONE_NO_RETURN)
    ));

    if state.flags.contains(EeoFlag::INTERPRETER_INITIALIZED) {
        return;
    }
    state.evalfunc = Some(exec_interp_expr);
    state.flags.insert(EeoFlag::INTERPRETER_INITIALIZED);
}

/// PG `ExecInterpExpr`: the portable switch interpreter. Walks `state.steps`,
/// dispatching each opcode, until a DONE step returns. `is_null` carries the
/// scalar result's null flag for value-returning exprs; for a projection
/// (DONE_NO_RETURN) the values were deposited into the result slot and the
/// returned Datum is unused.
///
/// Matches the `ExprStateEvalFunc` signature (state, econtext, &mut is_null).
pub fn exec_interp_expr(state: &mut ExprState, econtext: &mut ExprContext, is_null: &mut bool) -> Datum {
    let _ = econtext;

    let mut pc = 0usize;
    loop {
        // Read the opcode + payload by value/copy so we don't hold a borrow of
        // `state.steps` across the writes into `state.resvalue`/`resultslot`.
        let opcode = state.steps[pc].opcode;
        match opcode {
            ExprEvalOp::CONST => {
                let ExprEvalStepData::Constval(c) = &state.steps[pc].d else {
                    unreachable!("EEOP_CONST without Constval payload");
                };
                let (cval, cnull) = (c.value, c.isnull);
                state.resnull = cnull;
                state.resvalue = cval;
                pc += 1;
            }
            ExprEvalOp::ASSIGN_TMP => {
                let ExprEvalStepData::AssignTmp(a) = &state.steps[pc].d else {
                    unreachable!("EEOP_ASSIGN_TMP without AssignTmp payload");
                };
                let resultnum = a.resultnum as usize;
                let (v, n) = (state.resvalue, state.resnull);
                let slot = state
                    .resultslot
                    .as_mut()
                    .unwrap_or_else(|| unimplemented!("ASSIGN_TMP with no result slot"));
                slot.values[resultnum] = v;
                slot.isnull[resultnum] = n;
                pc += 1;
            }
            ExprEvalOp::ASSIGN_TMP_MAKE_RO => {
                // PG flattens an expanded datum to read-only here. The M1 const
                // path never produces an expanded (by-ref) datum, so this is the
                // plain copy; the MakeExpandedObjectReadOnly call grows with
                // varlena/expanded-object support.
                let ExprEvalStepData::AssignTmp(a) = &state.steps[pc].d else {
                    unreachable!("EEOP_ASSIGN_TMP_MAKE_RO without AssignTmp payload");
                };
                let resultnum = a.resultnum as usize;
                let (v, n) = (state.resvalue, state.resnull);
                let slot = state
                    .resultslot
                    .as_mut()
                    .unwrap_or_else(|| unimplemented!("ASSIGN_TMP_MAKE_RO with no result slot"));
                slot.isnull[resultnum] = n;
                slot.values[resultnum] = v;
                pc += 1;
            }
            ExprEvalOp::SCAN_FETCHSOME => {
                // EEOP_SCAN_FETCHSOME: force the scan slot's value/null arrays
                // valid up through `last_var`. The scan slot is a virtual slot
                // (the SeqScan deforms the heap tuple into it), whose arrays are
                // always fully valid, so getsomeattrs is a no-op here -- but call
                // it for faithfulness (it grows when non-virtual scan slots land).
                let ExprEvalStepData::Fetch(f) = &state.steps[pc].d else {
                    unreachable!("EEOP_SCAN_FETCHSOME without Fetch payload");
                };
                let last_var = f.last_var;
                let slot = econtext
                    .ecxt_scantuple
                    .as_mut()
                    .unwrap_or_else(|| unimplemented!("SCAN_FETCHSOME with no scan tuple"));
                slot_getsomeattrs(slot, last_var);
                pc += 1;
            }
            ExprEvalOp::SCAN_VAR => {
                // EEOP_SCAN_VAR: read tts_values[attnum]/tts_isnull[attnum] from
                // the scan slot (aliased by econtext->ecxt_scantuple) into the
                // scratch. `attnum` is the 0-based attribute index.
                let ExprEvalStepData::Var(v) = &state.steps[pc].d else {
                    unreachable!("EEOP_SCAN_VAR without Var payload");
                };
                let attnum = v.attnum as usize;
                let slot = econtext
                    .ecxt_scantuple
                    .as_ref()
                    .unwrap_or_else(|| unimplemented!("SCAN_VAR with no scan tuple"));
                state.resvalue = slot.values[attnum];
                state.resnull = slot.isnull[attnum];
                pc += 1;
            }
            ExprEvalOp::ASSIGN_SCAN_VAR => {
                // EEOP_ASSIGN_SCAN_VAR: copy scan slot attr `attnum` straight into
                // the result slot at `resultnum` (the projection fast path for a
                // simple Var; fuses SCAN_VAR + ASSIGN_TMP).
                let ExprEvalStepData::AssignVar(a) = &state.steps[pc].d else {
                    unreachable!("EEOP_ASSIGN_SCAN_VAR without AssignVar payload");
                };
                let (attnum, resultnum) = (a.attnum as usize, a.resultnum as usize);
                let (v, n) = {
                    let scan = econtext
                        .ecxt_scantuple
                        .as_ref()
                        .unwrap_or_else(|| unimplemented!("ASSIGN_SCAN_VAR with no scan tuple"));
                    (scan.values[attnum], scan.isnull[attnum])
                };
                let slot = state
                    .resultslot
                    .as_mut()
                    .unwrap_or_else(|| unimplemented!("ASSIGN_SCAN_VAR with no result slot"));
                slot.values[resultnum] = v;
                slot.isnull[resultnum] = n;
                pc += 1;
            }
            ExprEvalOp::DONE_RETURN => {
                *is_null = state.resnull;
                return state.resvalue;
            }
            ExprEvalOp::DONE_NO_RETURN => {
                // Projection terminator: the row is already in the result slot.
                return Datum(0);
            }
            other => not_yet_reachable(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::execExpr::exec_init_expr;
    use crate::backend::nodes::makefuncs::make_const;
    use crate::catalog::genbki::INT4OID;
    use crate::nodes::nodes::Node;
    use crate::postgres::{DatumGetInt32, Int32GetDatum};
    use crate::postgres_ext::InvalidOid;

    fn const_int4_node(v: i32) -> Node {
        let con = make_const(INT4OID, -1, InvalidOid, 4, Int32GetDatum(v), false, true);
        Node::Const(Box::new(con))
    }

    #[test]
    fn interp_const_only_returns_the_value() {
        let node = const_int4_node(99);
        let mut state = exec_init_expr(Some(&node), None).expect("const expr state");

        // Steps: [EEOP_CONST, EEOP_DONE_RETURN].
        assert_eq!(state.steps.len(), 2);
        assert_eq!(state.steps[0].opcode, ExprEvalOp::CONST);
        assert_eq!(state.steps[1].opcode, ExprEvalOp::DONE_RETURN);
        assert!(state.flags.contains(EeoFlag::INTERPRETER_INITIALIZED));

        let mut econtext = ExprContext::default();
        let mut is_null = true;
        let v = exec_interp_expr(&mut state, &mut econtext, &mut is_null);
        assert!(!is_null);
        assert_eq!(DatumGetInt32(v), 99);
    }

    #[test]
    fn interp_runs_through_evalfunc_pointer() {
        let node = const_int4_node(-5);
        let mut state = exec_init_expr(Some(&node), None).expect("const expr state");
        let f = state.evalfunc.expect("evalfunc installed by ExecReadyInterpretedExpr");
        let mut econtext = ExprContext::default();
        let mut is_null = true;
        let v = f(&mut state, &mut econtext, &mut is_null);
        assert!(!is_null);
        assert_eq!(DatumGetInt32(v), -5);
    }
}

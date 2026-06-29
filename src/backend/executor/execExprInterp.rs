//! Expression interpreter. Translated from
//! backend/executor/execExprInterp.c (disposition: grow).
//!
//! Step 08 translated the PORTABLE switch-based `ExecInterpExpr` (PG's
//! `EEO_SWITCH`/`EEO_CASE` path, NOT the computed-goto path) as a Rust `match`
//! over the opcode, filling the Const + projection opcodes. Step 21 (M3) adds the
//! operator/function (`EEOP_FUNCEXPR[_STRICT]`), boolean (`EEOP_BOOL_*_STEP`),
//! qual (`EEOP_QUAL`), and the conditional/unconditional jumps the bool/qual
//! short-circuits target. Every other opcode is a clean `not_yet_reachable` arm
//! that grows in later milestones (rules.md s4).
//!
//! Execution is fully synchronous (rules.md s5): the interpreter touches no I/O
//! leaf -- the scan loop deformed the row into the scan slot before calling in --
//! so the interpreter does not `.await`. The fmgr call goes through the
//! `FmgrInfo`/`FunctionCallInfoBaseData` owned by the step (genuinely `Send`, no
//! raw pointer), so the executor stays `Send` (rules.md s5/s10).

use crate::executor::execExpr::{ExprEvalOp, ExprEvalStep, ExprEvalStepData};
use crate::executor::tuptable::slot_getsomeattrs;
use crate::nodes::execnodes::{EeoFlag, ExprContext, ExprState};
use crate::postgres::{BoolGetDatum, Datum, DatumGetBool};

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
    let mut pc = 0usize;
    loop {
        let opcode = state.steps[pc].opcode;
        match opcode {
            // Result-slot-writing opcodes are top-level only (projection). They
            // read the scratch (`state.resvalue`/`resnull`).
            ExprEvalOp::ASSIGN_TMP | ExprEvalOp::ASSIGN_TMP_MAKE_RO => {
                // ASSIGN_TMP_MAKE_RO would flatten an expanded datum to read-only;
                // no by-ref/expanded datum is produced on the M3 scalar path, so it
                // is the plain copy (the MakeExpandedObjectReadOnly grows with
                // varlena/expanded-object support).
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
            ExprEvalOp::ASSIGN_SCAN_VAR => {
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
            ExprEvalOp::DONE_NO_RETURN => {
                // Projection terminator: the row is already in the result slot.
                return Datum(0);
            }
            ExprEvalOp::DONE_RETURN => {
                *is_null = state.resnull;
                return state.resvalue;
            }
            // Every other opcode is a scalar step: run it against the scratch.
            _ => {
                let next = exec_interp_step(&mut state.steps[pc], econtext, pc, &mut state.resvalue, &mut state.resnull);
                pc = next;
            }
        }
    }
}

/// Run one scalar `ExprEvalStep` (no result-slot write): update `resvalue`/
/// `resnull` (the scratch) and return the next program counter (`pc + 1`, or the
/// step's jump target for a short-circuit). Shared by the top-level interpreter
/// and the per-argument sub-program runner.
fn exec_interp_step(
    step: &mut ExprEvalStep,
    econtext: &mut ExprContext,
    pc: usize,
    resvalue: &mut Datum,
    resnull: &mut bool,
) -> usize {
    match step.opcode {
        ExprEvalOp::CONST => {
            let ExprEvalStepData::Constval(c) = &step.d else {
                unreachable!("EEOP_CONST without Constval payload");
            };
            *resnull = c.isnull;
            *resvalue = c.value;
            pc + 1
        }
        ExprEvalOp::SCAN_FETCHSOME => {
            // Force the scan slot's value/null arrays valid up through `last_var`.
            // The scan slot is virtual (the SeqScan deforms the heap tuple into
            // it), so getsomeattrs is a no-op here -- but call it for faithfulness
            // (it grows when non-virtual scan slots land).
            let ExprEvalStepData::Fetch(f) = &step.d else {
                unreachable!("EEOP_SCAN_FETCHSOME without Fetch payload");
            };
            let last_var = f.last_var;
            let slot = econtext
                .ecxt_scantuple
                .as_mut()
                .unwrap_or_else(|| unimplemented!("SCAN_FETCHSOME with no scan tuple"));
            slot_getsomeattrs(slot, last_var);
            pc + 1
        }
        ExprEvalOp::SCAN_VAR => {
            // Read tts_values[attnum]/tts_isnull[attnum] from the scan slot into
            // the scratch. `attnum` is the 0-based attribute index.
            let ExprEvalStepData::Var(v) = &step.d else {
                unreachable!("EEOP_SCAN_VAR without Var payload");
            };
            let attnum = v.attnum as usize;
            let slot = econtext
                .ecxt_scantuple
                .as_ref()
                .unwrap_or_else(|| unimplemented!("SCAN_VAR with no scan tuple"));
            *resvalue = slot.values[attnum];
            *resnull = slot.isnull[attnum];
            pc + 1
        }
        ExprEvalOp::FUNCEXPR => {
            eval_funcexpr(step, econtext, false, resvalue, resnull);
            pc + 1
        }
        ExprEvalOp::FUNCEXPR_STRICT
        | ExprEvalOp::FUNCEXPR_STRICT_1
        | ExprEvalOp::FUNCEXPR_STRICT_2 => {
            eval_funcexpr(step, econtext, true, resvalue, resnull);
            pc + 1
        }
        // AND: if a clause is NULL, remember it (anynull); if FALSE, the result is
        // FALSE -- short-circuit to jumpdone. STEP_FIRST resets anynull first.
        ExprEvalOp::BOOL_AND_STEP_FIRST
        | ExprEvalOp::BOOL_AND_STEP
        | ExprEvalOp::BOOL_AND_STEP_LAST => {
            let opcode = step.opcode;
            let ExprEvalStepData::Boolexpr(b) = &step.d else {
                unreachable!("EEOP_BOOL_AND_* without Boolexpr payload");
            };
            eval_bool_and_step(opcode, b, pc, resvalue, resnull)
        }
        ExprEvalOp::BOOL_OR_STEP_FIRST
        | ExprEvalOp::BOOL_OR_STEP
        | ExprEvalOp::BOOL_OR_STEP_LAST => {
            let opcode = step.opcode;
            let ExprEvalStepData::Boolexpr(b) = &step.d else {
                unreachable!("EEOP_BOOL_OR_* without Boolexpr payload");
            };
            eval_bool_or_step(opcode, b, pc, resvalue, resnull)
        }
        ExprEvalOp::BOOL_NOT_STEP => {
            // NULL in -> NULL out (resnull untouched); else logical negation.
            *resvalue = BoolGetDatum(!DatumGetBool(*resvalue));
            pc + 1
        }
        ExprEvalOp::QUAL => {
            // Simplified BOOL_AND_STEP for ExecQual: if the argument (== result)
            // is FALSE or NULL, the whole qual is FALSE -- bail out early.
            let ExprEvalStepData::Qualexpr(q) = &step.d else {
                unreachable!("EEOP_QUAL without Qualexpr payload");
            };
            if *resnull || !DatumGetBool(*resvalue) {
                *resnull = false;
                *resvalue = BoolGetDatum(false);
                q.jumpdone as usize
            } else {
                // leave TRUE in place in case this is the last qual.
                pc + 1
            }
        }
        ExprEvalOp::JUMP => {
            let ExprEvalStepData::Jump(j) = &step.d else {
                unreachable!("EEOP_JUMP without Jump payload");
            };
            j.jumpdone as usize
        }
        // DONE_RETURN/DONE_NO_RETURN are handled by the two callers
        // (exec_interp_expr top level, run_scalar sub-program) before dispatch.
        other => not_yet_reachable(other),
    }
}

/// EEOP_BOOL_AND_STEP[_FIRST|_LAST]: if a clause is NULL, remember it (anynull);
/// if FALSE, the AND result is FALSE -- short-circuit to jumpdone. STEP_FIRST
/// resets anynull; STEP_LAST resolves NULL-if-any-null (three-valued logic).
fn eval_bool_and_step(
    opcode: ExprEvalOp,
    b: &crate::executor::execExpr::BoolexprData,
    pc: usize,
    resvalue: &mut Datum,
    resnull: &mut bool,
) -> usize {
    if opcode == ExprEvalOp::BOOL_AND_STEP_FIRST {
        set_anynull(b, false);
    }
    if opcode == ExprEvalOp::BOOL_AND_STEP_LAST {
        if !*resnull && DatumGetBool(*resvalue) && anynull(b) {
            *resvalue = Datum(0);
            *resnull = true;
        }
        pc + 1
    } else if *resnull {
        set_anynull(b, true);
        pc + 1
    } else if DatumGetBool(*resvalue) {
        pc + 1
    } else {
        // FALSE: result is already FALSE, bail out early.
        b.jumpdone as usize
    }
}

/// EEOP_BOOL_OR_STEP[_FIRST|_LAST]: if a clause is NULL, remember it; if TRUE, the
/// OR result is TRUE -- short-circuit to jumpdone. STEP_FIRST resets anynull;
/// STEP_LAST resolves NULL-if-any-null.
fn eval_bool_or_step(
    opcode: ExprEvalOp,
    b: &crate::executor::execExpr::BoolexprData,
    pc: usize,
    resvalue: &mut Datum,
    resnull: &mut bool,
) -> usize {
    if opcode == ExprEvalOp::BOOL_OR_STEP_FIRST {
        set_anynull(b, false);
    }
    if opcode == ExprEvalOp::BOOL_OR_STEP_LAST {
        if !*resnull && !DatumGetBool(*resvalue) && anynull(b) {
            *resvalue = Datum(0);
            *resnull = true;
        }
        pc + 1
    } else if *resnull {
        set_anynull(b, true);
        pc + 1
    } else if DatumGetBool(*resvalue) {
        // TRUE: result is already TRUE, bail out early.
        b.jumpdone as usize
    } else {
        pc + 1
    }
}

/// Evaluate a function/operator call: run each argument's sub-program into
/// `fcinfo.args[i]`, apply the strict-NULL check (NULL arg -> NULL result, no
/// call), then invoke the resolved fmgr function and store its result + isnull.
fn eval_funcexpr(
    step: &mut ExprEvalStep,
    econtext: &mut ExprContext,
    strict: bool,
    resvalue: &mut Datum,
    resnull: &mut bool,
) {
    let ExprEvalStepData::Func(f) = &mut step.d else {
        unreachable!("EEOP_FUNCEXPR* without Func payload");
    };
    let nargs = f.nargs as usize;

    // Evaluate each non-const argument first (const args were filled into fcinfo
    // at init time). `arg_steps` and `fcinfo_data` are disjoint fields of `f`, so
    // mutating the arg sub-programs here does not alias the fcinfo borrow below.
    for i in 0..nargs {
        if let Some(steps) = f.arg_steps[i].as_mut() {
            let (v, n) = run_scalar(steps, econtext);
            let fcinfo = f
                .fcinfo_data
                .as_mut()
                .unwrap_or_else(|| unimplemented!("FUNCEXPR with no fcinfo"));
            fcinfo.args[i].value = v;
            fcinfo.args[i].isnull = n;
        }
    }

    let fcinfo = f
        .fcinfo_data
        .as_mut()
        .unwrap_or_else(|| unimplemented!("FUNCEXPR with no fcinfo"));

    if strict {
        for i in 0..nargs {
            if fcinfo.args[i].isnull {
                *resnull = true;
                return;
            }
        }
    }

    let fn_addr = f
        .fn_addr
        .unwrap_or_else(|| unimplemented!("FUNCEXPR with no fn_addr"));
    fcinfo.isnull = false;
    let d = fn_addr(fcinfo);
    *resvalue = d;
    *resnull = fcinfo.isnull;
}

/// Run an argument's compiled sub-program (a flat step list ending in
/// `EEOP_DONE_RETURN`) against `econtext`, returning its scalar `(value, isnull)`.
/// Used for non-constant function arguments (rules.md s10: args are owned steps,
/// not `resv` pointers into the fcinfo). Takes `&mut` so a nested FUNCEXPR/BOOL
/// arg's interior step state (fcinfo, anynull) is updated in place.
fn run_scalar(prog: &mut [ExprEvalStep], econtext: &mut ExprContext) -> (Datum, bool) {
    // The arg sub-program never writes a result slot, so a local scratch suffices.
    let mut scratch_v = Datum(0);
    let mut scratch_n = true;
    let mut pc = 0usize;
    loop {
        if prog[pc].opcode == ExprEvalOp::DONE_RETURN {
            return (scratch_v, scratch_n);
        }
        let next = exec_interp_step(&mut prog[pc], econtext, pc, &mut scratch_v, &mut scratch_n);
        pc = next;
    }
}

/// Read/write the shared `anynull` cell of a BOOL step (PG `*op->d.boolexpr.anynull`).
/// The cell is shared by every step of one BoolExpr, so STEP_LAST observes a NULL
/// recorded by any earlier STEP_FIRST/STEP (three-valued logic).
fn anynull(b: &crate::executor::execExpr::BoolexprData) -> bool {
    b.anynull
        .as_ref()
        .is_some_and(|c| c.load(std::sync::atomic::Ordering::Relaxed))
}
fn set_anynull(b: &crate::executor::execExpr::BoolexprData, v: bool) {
    if let Some(c) = b.anynull.as_ref() {
        c.store(v, std::sync::atomic::Ordering::Relaxed);
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

    use crate::backend::executor::execExpr::{exec_init_qual, exec_qual};
    use crate::nodes::primnodes::{BoolExprType, OpExpr};
    use crate::postgres::{BoolGetDatum, DatumGetBool};
    use crate::postgres_ext::Oid;
    use crate::utils::fmgroids::{F_INT4GT, F_INT4PL};

    fn opexpr(opno: u32, opfuncid: Oid, resulttype: Oid, args: Vec<Node>) -> Node {
        Node::OpExpr(Box::new(OpExpr {
            opno: Oid(opno),
            opfuncid,
            opresulttype: resulttype,
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args,
            location: -1,
        }))
    }

    /// The FUNCEXPR interp step: int4pl(2, 3) = 5 via the resolved fmgr function.
    #[test]
    fn funcexpr_int4pl_2_3_is_5() {
        let node = opexpr(551, F_INT4PL, INT4OID, vec![const_int4_node(2), const_int4_node(3)]);
        let mut state = exec_init_expr(Some(&node), None).expect("opexpr state");
        // Both args are Const -> STRICT_2 with inline-filled fcinfo args.
        assert_eq!(state.steps[0].opcode, ExprEvalOp::FUNCEXPR_STRICT_2);
        let mut econtext = ExprContext::default();
        let mut is_null = true;
        let v = exec_interp_expr(&mut state, &mut econtext, &mut is_null);
        assert!(!is_null);
        assert_eq!(DatumGetInt32(v), 5);
    }

    /// A bool comparison via FUNCEXPR: int4gt(3, 0) is TRUE; int4gt(0, 3) is FALSE.
    #[test]
    fn funcexpr_int4gt_truth() {
        for (a, b, want) in [(3, 0, true), (0, 3, false), (5, 5, false)] {
            let node = opexpr(521, F_INT4GT, crate::catalog::genbki::BOOLOID,
                vec![const_int4_node(a), const_int4_node(b)]);
            let mut state = exec_init_expr(Some(&node), None).expect("opexpr state");
            let mut econtext = ExprContext::default();
            let mut is_null = true;
            let v = exec_interp_expr(&mut state, &mut econtext, &mut is_null);
            assert!(!is_null);
            assert_eq!(DatumGetBool(v), want, "int4gt({a},{b})");
        }
    }

    fn bool_const(v: bool) -> Node {
        let con = make_const(crate::catalog::genbki::BOOLOID, -1, InvalidOid, 1, BoolGetDatum(v), false, true);
        Node::Const(Box::new(con))
    }

    /// ExecQual: empty qual is always-true (None state).
    #[test]
    fn exec_qual_empty_is_true() {
        let mut state = exec_init_qual(&[], None);
        let mut econtext = ExprContext::default();
        assert!(exec_qual(state.as_deref_mut(), &mut econtext));
    }

    /// ExecQual short-circuits: a single TRUE clause passes, a FALSE clause fails.
    #[test]
    fn exec_qual_true_false() {
        // qual [true] -> true.
        let mut q = exec_init_qual(&[bool_const(true)], None).expect("qual");
        let mut econtext = ExprContext::default();
        assert!(exec_qual(Some(&mut q), &mut econtext));

        // qual [false] -> false.
        let mut q = exec_init_qual(&[bool_const(false)], None).expect("qual");
        assert!(!exec_qual(Some(&mut q), &mut econtext));

        // qual [true, false] -> false (the AND of an implicit-AND list).
        let mut q = exec_init_qual(&[bool_const(true), bool_const(false)], None).expect("qual");
        assert!(!exec_qual(Some(&mut q), &mut econtext));

        // qual [true, true] -> true.
        let mut q = exec_init_qual(&[bool_const(true), bool_const(true)], None).expect("qual");
        assert!(exec_qual(Some(&mut q), &mut econtext));
    }

    /// ExecQual treats a NULL clause result as FALSE (three-valued logic).
    #[test]
    fn exec_qual_null_is_false() {
        // A NULL boolean Const: EEOP_QUAL sees resnull -> whole qual FALSE.
        let con = make_const(crate::catalog::genbki::BOOLOID, -1, InvalidOid, 1, BoolGetDatum(true), true, true);
        let mut q = exec_init_qual(&[Node::Const(Box::new(con))], None).expect("qual");
        let mut econtext = ExprContext::default();
        assert!(!exec_qual(Some(&mut q), &mut econtext));
    }

    /// A NULL boolean Const (constisnull = true).
    fn bool_null() -> Node {
        let con = make_const(crate::catalog::genbki::BOOLOID, -1, InvalidOid, 1, BoolGetDatum(true), true, true);
        Node::Const(Box::new(con))
    }

    /// BoolExpr AND/OR/NOT three-valued logic through the interpreter.
    #[test]
    fn boolexpr_three_valued() {
        use crate::nodes::primnodes::BoolExpr;
        let andn = |args: Vec<Node>| Node::BoolExpr(Box::new(BoolExpr {
            boolop: BoolExprType::AND_EXPR, args, location: -1,
        }));
        let orn = |args: Vec<Node>| Node::BoolExpr(Box::new(BoolExpr {
            boolop: BoolExprType::OR_EXPR, args, location: -1,
        }));
        let and = |a: Node, b: Node| andn(vec![a, b]);
        let or = |a: Node, b: Node| orn(vec![a, b]);
        let not = |a: Node| Node::BoolExpr(Box::new(BoolExpr {
            boolop: BoolExprType::NOT_EXPR, args: vec![a], location: -1,
        }));

        let eval = |node: Node| -> (bool, bool) {
            let mut state = exec_init_expr(Some(&node), None).expect("bool state");
            let mut econtext = ExprContext::default();
            let mut is_null = false;
            let v = exec_interp_expr(&mut state, &mut econtext, &mut is_null);
            (DatumGetBool(v), is_null)
        };

        // TRUE AND FALSE = FALSE; TRUE AND TRUE = TRUE.
        assert_eq!(eval(and(bool_const(true), bool_const(false))), (false, false));
        assert_eq!(eval(and(bool_const(true), bool_const(true))), (true, false));
        // FALSE OR TRUE = TRUE; FALSE OR FALSE = FALSE.
        assert_eq!(eval(or(bool_const(false), bool_const(true))), (true, false));
        assert_eq!(eval(or(bool_const(false), bool_const(false))), (false, false));
        // NOT TRUE = FALSE; NOT FALSE = TRUE.
        assert!(!eval(not(bool_const(true))).0);
        assert!(eval(not(bool_const(false))).0);

        // Three-valued NULL logic across the shared anynull cell. The NULL-result
        // cases (isnull = true) all FAIL with a per-step anynull cell -- STEP_LAST
        // reads its own still-false cell and leaves a non-NULL result -- so they pin
        // the shared-cell fix. The result *value* is don't-care once isnull is true
        // (PG leaves whatever was last in the scratch), so assert only the flag
        // there; assert the value for the non-NULL results.
        let isnull = |node: Node| eval(node).1;

        // AND with NULL.
        assert!(isnull(and(bool_const(true), bool_null())), "true AND NULL = NULL");
        assert!(isnull(and(bool_null(), bool_const(true))), "NULL AND true = NULL");
        assert_eq!(eval(and(bool_const(false), bool_null())), (false, false), "false AND NULL = false");
        assert_eq!(eval(and(bool_null(), bool_const(false))), (false, false), "NULL AND false = false");
        // 3-arg AND with the NULL in the middle (the regression case).
        assert!(
            isnull(andn(vec![bool_const(true), bool_null(), bool_const(true)])),
            "true AND NULL AND true = NULL"
        );

        // OR with NULL.
        assert!(isnull(or(bool_const(false), bool_null())), "false OR NULL = NULL");
        assert!(isnull(or(bool_null(), bool_const(false))), "NULL OR false = NULL");
        assert_eq!(eval(or(bool_const(true), bool_null())), (true, false), "true OR NULL = true");
        assert_eq!(eval(or(bool_null(), bool_const(true))), (true, false), "NULL OR true = true");
        // 3-arg OR with the NULL in the middle.
        assert!(
            isnull(orn(vec![bool_const(false), bool_null(), bool_const(false)])),
            "false OR NULL OR false = NULL"
        );

        // NOT NULL = NULL.
        assert!(isnull(not(bool_null())), "NOT NULL = NULL");
    }
}

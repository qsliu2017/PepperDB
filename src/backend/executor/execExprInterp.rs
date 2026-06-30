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
            // Join projection: copy an attr straight from the outer/inner tuple
            // into the result slot (the simple-Var fast path for OUTER_VAR/
            // INNER_VAR-resolved projection targets).
            ExprEvalOp::ASSIGN_OUTER_VAR | ExprEvalOp::ASSIGN_INNER_VAR => {
                let ExprEvalStepData::AssignVar(a) = &state.steps[pc].d else {
                    unreachable!("EEOP_ASSIGN_OUTER/INNER_VAR without AssignVar payload");
                };
                let (attnum, resultnum) = (a.attnum as usize, a.resultnum as usize);
                let src = if opcode == ExprEvalOp::ASSIGN_OUTER_VAR {
                    econtext.ecxt_outertuple.as_ref()
                } else {
                    econtext.ecxt_innertuple.as_ref()
                };
                let (v, n) = {
                    let slot = src.unwrap_or_else(|| {
                        unimplemented!("ASSIGN_OUTER/INNER_VAR with no source tuple")
                    });
                    (slot.values[attnum], slot.isnull[attnum])
                };
                let slot = state
                    .resultslot
                    .as_mut()
                    .unwrap_or_else(|| unimplemented!("ASSIGN_OUTER/INNER_VAR with no result slot"));
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
#[allow(
    clippy::too_many_lines,
    reason = "1:1 with PG ExecInterpExpr's single per-opcode dispatch switch; splitting the arms across functions would obscure the EEOP <-> handler mapping"
)]
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
        ExprEvalOp::OUTER_FETCHSOME | ExprEvalOp::INNER_FETCHSOME => {
            // Force the outer/inner join slot deformed through `last_var`. The join
            // input slots are virtual (snapshotted from the child), so getsomeattrs
            // is a no-op, but call it for faithfulness.
            let ExprEvalStepData::Fetch(f) = &step.d else {
                unreachable!("EEOP_OUTER/INNER_FETCHSOME without Fetch payload");
            };
            let last_var = f.last_var;
            let slot = if step.opcode == ExprEvalOp::OUTER_FETCHSOME {
                econtext.ecxt_outertuple.as_mut()
            } else {
                econtext.ecxt_innertuple.as_mut()
            }
            .unwrap_or_else(|| unimplemented!("OUTER/INNER_FETCHSOME with no source tuple"));
            slot_getsomeattrs(slot, last_var);
            pc + 1
        }
        ExprEvalOp::OUTER_VAR | ExprEvalOp::INNER_VAR => {
            // Read tts_values[attnum]/tts_isnull[attnum] from the outer/inner join
            // slot into the scratch. `attnum` is the 0-based attribute index.
            let ExprEvalStepData::Var(v) = &step.d else {
                unreachable!("EEOP_OUTER/INNER_VAR without Var payload");
            };
            let attnum = v.attnum as usize;
            let slot = if step.opcode == ExprEvalOp::OUTER_VAR {
                econtext.ecxt_outertuple.as_ref()
            } else {
                econtext.ecxt_innertuple.as_ref()
            }
            .unwrap_or_else(|| unimplemented!("OUTER/INNER_VAR with no source tuple"));
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
        // M4 (step 23): NULLIF -- like a strict 2-arg function, but if the two args
        // compare equal (the "=" operator returns TRUE) the result is NULL; else the
        // result is the FIRST argument (not the operator's boolean result).
        ExprEvalOp::NULLIF => {
            eval_nullif(step, econtext, resvalue, resnull);
            pc + 1
        }
        // CoerceViaIO: out-func(source) -> cstring, then in-func(target). NULL in,
        // NULL out (strict).
        ExprEvalOp::IOCOERCE => {
            eval_iocoerce(step, econtext, resvalue, resnull);
            pc + 1
        }
        ExprEvalOp::CASE => {
            eval_case(step, econtext, resvalue, resnull);
            pc + 1
        }
        ExprEvalOp::COALESCE => {
            eval_coalesce(step, econtext, resvalue, resnull);
            pc + 1
        }
        ExprEvalOp::MINMAX => {
            eval_minmax(step, econtext, resvalue, resnull);
            pc + 1
        }
        ExprEvalOp::PARAM_EXEC => {
            eval_param_exec(step, econtext, resvalue, resnull);
            pc + 1
        }
        ExprEvalOp::PARAM_EXTERN => {
            eval_param_extern(step, econtext, resvalue, resnull);
            pc + 1
        }
        ExprEvalOp::AGGREF => {
            // Read the finalized per-group aggregate value nodeAgg deposited in
            // econtext.ecxt_aggvalues[aggno] / ecxt_aggnulls[aggno].
            let ExprEvalStepData::Aggref(a) = &step.d else {
                unreachable!("EEOP_AGGREF without Aggref payload");
            };
            let aggno = a.aggno as usize;
            *resvalue = econtext.ecxt_aggvalues.get(aggno).copied().unwrap_or(Datum(0));
            *resnull = econtext.ecxt_aggnulls.get(aggno).copied().unwrap_or(true);
            pc + 1
        }
        // DONE_RETURN/DONE_NO_RETURN are handled by the two callers
        // (exec_interp_expr top level, run_scalar sub-program) before dispatch.
        other => not_yet_reachable(other),
    }
}

/// EEOP_NULLIF: run the two args (a NullIfExpr is a strict 2-arg "=" OpExpr-shaped
/// node). If either arg is NULL the result is the first arg (PG: a NULL first arg
/// yields NULL, which is the first arg). If the "=" comparison is TRUE the result is
/// NULL; otherwise the result is the first argument.
fn eval_nullif(
    step: &mut ExprEvalStep,
    econtext: &mut ExprContext,
    resvalue: &mut Datum,
    resnull: &mut bool,
) {
    use crate::postgres::DatumGetBool;
    let ExprEvalStepData::Func(f) = &mut step.d else {
        unreachable!("EEOP_NULLIF without Func payload");
    };
    let nargs = f.nargs as usize;
    crate::assert!(nargs == 2, "NULLIF has exactly two arguments");

    // Evaluate both args (const args were filled at init; sub-programs run here).
    for i in 0..nargs {
        if let Some(steps) = f.arg_steps[i].as_mut() {
            let (v, n) = run_scalar(steps, econtext);
            let fcinfo = f.fcinfo_data.as_mut().unwrap_or_else(|| unimplemented!("NULLIF fcinfo"));
            fcinfo.args[i].value = v;
            fcinfo.args[i].isnull = n;
        }
    }
    let fcinfo = f.fcinfo_data.as_mut().unwrap_or_else(|| unimplemented!("NULLIF fcinfo"));
    let (arg0, arg0null) = (fcinfo.args[0].value, fcinfo.args[0].isnull);

    // The result defaults to the first argument.
    *resvalue = arg0;
    *resnull = arg0null;

    // If either input is NULL, the "=" operator (strict) yields NULL -> not equal,
    // so the result stays the first argument.
    if fcinfo.args[0].isnull || fcinfo.args[1].isnull {
        return;
    }
    let fn_addr = f.fn_addr.unwrap_or_else(|| unimplemented!("NULLIF fn_addr"));
    fcinfo.isnull = false;
    let eq = fn_addr(fcinfo);
    if !fcinfo.isnull && DatumGetBool(eq) {
        // Args are equal -> NULLIF returns NULL.
        *resnull = true;
    }
}

/// EEOP_IOCOERCE: source typoutput -> cstring -> target typinput. NULL passes
/// through (the I/O coercion is strict in PG's CoerceViaIO).
fn eval_iocoerce(
    step: &mut ExprEvalStep,
    econtext: &mut ExprContext,
    resvalue: &mut Datum,
    resnull: &mut bool,
) {
    use crate::backend::utils::fmgr::fmgr::{OutputFunctionCall, InputFunctionCall};
    let ExprEvalStepData::Iocoerce(io) = &mut step.d else {
        unreachable!("EEOP_IOCOERCE without Iocoerce payload");
    };
    let (v, n) = run_scalar(&mut io.arg_steps, econtext);
    if n {
        *resnull = true;
        return;
    }
    // Output the source value to its text form, then feed it to the target's input.
    let out_flinfo = io.finfo_out.as_mut().unwrap_or_else(|| unimplemented!("IOCOERCE out finfo"));
    let s = OutputFunctionCall(out_flinfo, v);
    let in_flinfo = io.finfo_in.as_mut().unwrap_or_else(|| unimplemented!("IOCOERCE in finfo"));
    let typioparam = io.typioparam;
    let out = InputFunctionCall(in_flinfo, &s, typioparam, -1)
        .unwrap_or_else(|| unimplemented!("IOCOERCE input returned NULL"));
    *resvalue = out;
    *resnull = false;
}

/// EEOP_CASE: evaluate the WHEN conditions in order; the first TRUE arm's result is
/// the value; if none is TRUE, the ELSE (default) result. (The simple-CASE test arg
/// with its CaseTestExpr placeholder is staged; the searched form -- the M4
/// milestone -- needs no test value.)
fn eval_case(
    step: &mut ExprEvalStep,
    econtext: &mut ExprContext,
    resvalue: &mut Datum,
    resnull: &mut bool,
) {
    use crate::postgres::DatumGetBool;
    let ExprEvalStepData::Case(c) = &mut step.d else {
        unreachable!("EEOP_CASE without Case payload");
    };
    if c.arg_steps.is_some() {
        unimplemented!("EEOP_CASE: simple-CASE test value (CaseTestExpr) not yet reachable");
    }
    for (cond, result) in &mut c.when_steps {
        let (cv, cn) = run_scalar(cond, econtext);
        if !cn && DatumGetBool(cv) {
            let (rv, rn) = run_scalar(result, econtext);
            *resvalue = rv;
            *resnull = rn;
            return;
        }
    }
    let (dv, dn) = run_scalar(&mut c.default_steps, econtext);
    *resvalue = dv;
    *resnull = dn;
}

/// EEOP_COALESCE: return the first non-NULL argument; NULL if all are NULL.
fn eval_coalesce(
    step: &mut ExprEvalStep,
    econtext: &mut ExprContext,
    resvalue: &mut Datum,
    resnull: &mut bool,
) {
    let ExprEvalStepData::Coalesce(c) = &mut step.d else {
        unreachable!("EEOP_COALESCE without Coalesce payload");
    };
    for arg in &mut c.arg_steps {
        let (v, n) = run_scalar(arg, econtext);
        if !n {
            *resvalue = v;
            *resnull = false;
            return;
        }
    }
    *resnull = true;
}

/// EEOP_MINMAX: GREATEST/LEAST. Evaluate every argument; skip NULLs; fold with the
/// type's btree comparison function (>0 means arg0 > arg1). The result is NULL only
/// if every argument is NULL.
fn eval_minmax(
    step: &mut ExprEvalStep,
    econtext: &mut ExprContext,
    resvalue: &mut Datum,
    resnull: &mut bool,
) {
    use crate::nodes::primnodes::MinMaxOp;
    use crate::postgres::DatumGetInt32;
    let ExprEvalStepData::Minmax(m) = &mut step.d else {
        unreachable!("EEOP_MINMAX without Minmax payload");
    };
    let nelems = m.nelems as usize;
    for i in 0..nelems {
        let (v, n) = run_scalar(&mut m.arg_steps[i], econtext);
        m.values[i] = v;
        m.nulls[i] = n;
    }

    let cmp_addr = m.cmp_addr.unwrap_or_else(|| unimplemented!("MINMAX cmp fn"));
    let is_greatest = matches!(m.op, MinMaxOp::GREATEST);
    let mut have: Option<Datum> = None;
    for i in 0..nelems {
        if m.nulls[i] {
            continue;
        }
        let candidate = m.values[i];
        match have {
            None => have = Some(candidate),
            Some(cur) => {
                let fcinfo = m.fcinfo_data.as_mut().unwrap_or_else(|| unimplemented!("MINMAX fcinfo"));
                fcinfo.args[0].value = candidate;
                fcinfo.args[0].isnull = false;
                fcinfo.args[1].value = cur;
                fcinfo.args[1].isnull = false;
                fcinfo.isnull = false;
                let cmp = DatumGetInt32(cmp_addr(fcinfo));
                // GREATEST keeps the larger (cmp > 0), LEAST the smaller (cmp < 0).
                if (is_greatest && cmp > 0) || (!is_greatest && cmp < 0) {
                    have = Some(candidate);
                }
            }
        }
    }
    match have {
        Some(v) => {
            *resvalue = v;
            *resnull = false;
        }
        None => *resnull = true,
    }
}

/// PG `ExecEvalParamExec`: read an executor (PARAM_EXEC) param's value from the
/// EState's `ecxt_param_exec_vals[paramid]`. PG lazily evaluates a not-yet-computed
/// subplan param here (`prm->execPlan != NULL` -> ExecSetParamPlan); that lazy path
/// is staged until correlated SubPlans are reachable (M9 reaches only pre-filled
/// PARAM_EXEC slots, e.g. EPQ / initplan outputs).
fn eval_param_exec(
    step: &ExprEvalStep,
    econtext: &ExprContext,
    resvalue: &mut Datum,
    resnull: &mut bool,
) {
    let ExprEvalStepData::Param(p) = &step.d else {
        unreachable!("EEOP_PARAM_EXEC without Param payload");
    };
    let prms = econtext
        .ecxt_param_exec_vals
        .as_ref()
        .unwrap_or_else(|| unimplemented!("PARAM_EXEC with no ecxt_param_exec_vals"));
    let prm = prms.lock()[p.paramid as usize].clone();
    if prm.exec_plan != 0 {
        unimplemented!("PARAM_EXEC lazy subplan evaluation (ExecSetParamPlan) not yet reachable");
    }
    *resvalue = prm.value;
    *resnull = prm.isnull;
}

/// PG `ExecEvalParamExtern`: read an external (PARAM_EXTERN, `$n`) param's value
/// from the portal's `ParamListInfo`. A dynamic-param `paramFetch` hook gets a
/// chance first (else the static `params[paramid-1]` slot is read). The fetched
/// param's type must match the type recorded when the plan was compiled.
fn eval_param_extern(
    step: &ExprEvalStep,
    econtext: &mut ExprContext,
    resvalue: &mut Datum,
    resnull: &mut bool,
) {
    let ExprEvalStepData::Param(p) = &step.d else {
        unreachable!("EEOP_PARAM_EXTERN without Param payload");
    };
    let param_id = p.paramid;
    let expected_type = p.paramtype;

    let param_info = econtext
        .ecxt_param_list_info
        .as_mut()
        .filter(|pi| param_id > 0 && param_id <= pi.num_params);
    if let Some(param_info) = param_info {
        // Give the hook a chance in case the parameter is dynamic.
        let mut workspace = empty_param_extern();
        let prm = match param_info.param_fetch {
            Some(fetch) => fetch(param_info, param_id, false, &mut workspace),
            None => clone_param_extern(&param_info.params[param_id as usize - 1]),
        };
        if crate::c::OidIsValid(prm.ptype) {
            // Safety check in case a hook did something unexpected.
            if prm.ptype != expected_type {
                param_type_mismatch(param_id);
            }
            *resvalue = prm.value;
            *resnull = prm.isnull;
            return;
        }
    }
    no_value_for_parameter(param_id);
}

/// A zero-valued `ParamExternData` workspace (PG passes `&prmdata` to paramFetch).
fn empty_param_extern() -> crate::nodes::params::ParamExternData {
    crate::nodes::params::ParamExternData {
        value: Datum(0),
        isnull: true,
        pflags: crate::nodes::params::ParamFlags::empty(),
        ptype: crate::postgres_ext::InvalidOid,
    }
}

fn clone_param_extern(
    src: &crate::nodes::params::ParamExternData,
) -> crate::nodes::params::ParamExternData {
    crate::nodes::params::ParamExternData {
        value: src.value,
        isnull: src.isnull,
        pflags: src.pflags,
        ptype: src.ptype,
    }
}

#[cold]
fn param_type_mismatch(param_id: i32) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_DATATYPE_MISMATCH).errmsg(format!(
            "type of parameter {param_id} does not match that when preparing the plan"
        ));
    });
    unreachable!("ereport(ERROR) diverges");
}

#[cold]
fn no_value_for_parameter(param_id: i32) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("no value found for parameter {param_id}"));
    });
    unreachable!("ereport(ERROR) diverges");
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
            opno: Oid::new(opno),
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

    use crate::nodes::params::{ParamExecData, ParamExternData, ParamFlags, ParamListInfoData};
    use crate::nodes::primnodes::{Param, ParamKind};

    fn extern_param(paramid: i32, paramtype: Oid) -> Node {
        Node::Param(Box::new(Param {
            paramkind: ParamKind::EXTERN,
            paramid,
            paramtype,
            paramtypmod: -1,
            paramcollid: InvalidOid,
            location: -1,
        }))
    }

    /// A ParamListInfo carrying static $n values (no fetch hook).
    fn param_list(values: &[(Oid, i32)]) -> ParamListInfoData {
        let params = values
            .iter()
            .map(|&(ptype, v)| ParamExternData {
                value: Int32GetDatum(v),
                isnull: false,
                pflags: ParamFlags::empty(),
                ptype,
            })
            .collect::<Vec<_>>();
        ParamListInfoData {
            param_fetch: None,
            param_compile: None,
            parser_setup: None,
            param_values_str: None,
            num_params: values.len() as i32,
            params,
        }
    }

    /// An ExprContext carrying the given external-param list.
    fn econtext_with_extern(params: ParamListInfoData) -> ExprContext {
        ExprContext { ecxt_param_list_info: Some(Box::new(params)), ..ExprContext::default() }
    }

    /// An ExprContext carrying the given executor-param array.
    fn econtext_with_exec(vals: Vec<ParamExecData>) -> ExprContext {
        ExprContext {
            ecxt_param_exec_vals: Some(std::sync::Arc::new(parking_lot::Mutex::new(vals))),
            ..ExprContext::default()
        }
    }

    /// PARAM_EXTERN: `$1 + 1` with a ParamListInfo {$1 = 41} evaluates to 42.
    #[test]
    fn param_extern_plus_one_is_42() {
        let node = opexpr(551, F_INT4PL, INT4OID, vec![extern_param(1, INT4OID), const_int4_node(1)]);
        let mut state = exec_init_expr(Some(&node), None).expect("opexpr state");
        // The $1 arg is not a Const, so the inline-Const fast path does not apply:
        // the param is read by an EEOP_PARAM_EXTERN sub-step.
        let mut econtext = econtext_with_extern(param_list(&[(INT4OID, 41)]));
        let mut is_null = true;
        let v = exec_interp_expr(&mut state, &mut econtext, &mut is_null);
        assert!(!is_null);
        assert_eq!(DatumGetInt32(v), 42);
    }

    /// PARAM_EXTERN with a type mismatch vs the compiled paramtype raises
    /// ereport(ERROR) with ERRCODE_DATATYPE_MISMATCH; caught via pg_try/pg_catch.
    #[test]
    fn param_extern_type_mismatch_errors() {
        let sqlstate = crate::backend::utils::error::elog::pg_try(|| {
            let node = extern_param(1, INT4OID);
            let mut state = exec_init_expr(Some(&node), None).expect("param state");
            // ParamListInfo declares $1 as BOOLOID, but the plan compiled it int4.
            let mut econtext =
                econtext_with_extern(param_list(&[(crate::catalog::genbki::BOOLOID, 1)]));
            let mut is_null = true;
            let _ = exec_interp_expr(&mut state, &mut econtext, &mut is_null);
            None
        })
        .pg_catch(|edata| Some(edata.sqlerrcode))
        .done();
        assert_eq!(sqlstate, Some(crate::utils::errcodes::ERRCODE_DATATYPE_MISMATCH));
    }

    /// PARAM_EXEC: `$0 + 1` reading from ecxt_param_exec_vals[$0 = 41] -> 42.
    #[test]
    fn param_exec_plus_one_is_42() {
        let exec_param = Node::Param(Box::new(Param {
            paramkind: ParamKind::EXEC,
            paramid: 0,
            paramtype: INT4OID,
            paramtypmod: -1,
            paramcollid: InvalidOid,
            location: -1,
        }));
        let node = opexpr(551, F_INT4PL, INT4OID, vec![exec_param, const_int4_node(1)]);
        let mut state = exec_init_expr(Some(&node), None).expect("opexpr state");
        let mut econtext = econtext_with_exec(vec![ParamExecData {
            exec_plan: 0,
            value: Int32GetDatum(41),
            isnull: false,
        }]);
        let mut is_null = true;
        let v = exec_interp_expr(&mut state, &mut econtext, &mut is_null);
        assert!(!is_null);
        assert_eq!(DatumGetInt32(v), 42);
    }
}

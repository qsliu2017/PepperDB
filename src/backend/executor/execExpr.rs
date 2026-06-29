//! Expression compilation: turn an expression tree into `ExprEvalStep`s.
//! Translated from backend/executor/execExpr.c (disposition: grow).
//!
//! Step 08 fills only the Const + projection path: `ExecInitExprRec` emits
//! `EEOP_CONST`; `ExecBuildProjectionInfo` emits `[CONST, ASSIGN_TMP*,
//! DONE_NO_RETURN]`; `ExecInitExpr` emits `[CONST, DONE_RETURN]`. The
//! `ExecInitExprRec` node switch and the projection's per-target emitter are
//! correct-for-reachable; every other node kind is a clean `not_yet_reachable`
//! guard that grows in later milestones (rules.md s4).

#![allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: ExecInitExpr/ExecInitQual take `parent` (PlanState) per the C API to register Var/Param/SubPlan state; consumed once those node kinds are reachable"
)]

use crate::executor::execExpr::{
    AssignTmpData, AssignVarData, BoolexprData, ConstvalData, ExprEvalOp, ExprEvalStep,
    ExprEvalStepData, FetchData, FuncData, QualexprData, VarData,
};
use crate::nodes::execnodes::{EeoFlag, ExprContext, ExprState, PlanState, ProjectionInfo};
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{BoolExpr, BoolExprType, Expr, FuncExpr, OpExpr, Var, VarReturningType};
use crate::postgres::{Datum, NullableDatum};
use crate::postgres_ext::Oid;

use crate::backend::executor::execExprInterp::exec_ready_interpreted_expr;
use crate::fmgr::{
    fmgr_info, fmgr_info_set_expr, FmgrInfo, FunctionCallInfoBaseData, InitFunctionCallInfoData,
};

/// Panic for an expression node kind not yet translated (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `ExprEvalPushStep`: append a step to the ExprState's instruction list.
pub fn expr_eval_push_step(es: &mut ExprState, s: ExprEvalStep) {
    es.steps.push(s);
    es.steps_len = i32::try_from(es.steps.len()).unwrap_or(es.steps_len);
}

/// PG `ExecReadyExpr`: finalize a compiled ExprState (JIT then interpreter). M1
/// has no JIT, so always the interpreter.
fn exec_ready_expr(state: &mut ExprState) {
    exec_ready_interpreted_expr(state);
}

/// PG `ExecInitExprRec`: compile one expression node into steps that deposit the
/// result through `resv`/`resnull` (here: indices into the step list / the
/// ExprState scratch). Step 08 lives the `Const` arm.
///
/// The result-target is the ExprState's own scratch (`resvalue`/`resnull`); the
/// caller (projection or ExecInitExpr) then either copies it into the result
/// slot (ASSIGN_TMP) or returns it (DONE_RETURN). Mirrors PG, where `resv`/
/// `resnull` for the top expr point at `state->resvalue`/`state->resnull`.
fn exec_init_expr_rec(node: &Node, state: &mut ExprState) {
    match node {
        Node::Const(con) => {
            // EEOP_CONST: copy the literal into the scratch register.
            let step = ExprEvalStep {
                opcode: ExprEvalOp::CONST,
                resvalue: None,
                resnull: None,
                d: ExprEvalStepData::Constval(ConstvalData {
                    value: con.constvalue,
                    isnull: con.constisnull,
                }),
            };
            expr_eval_push_step(state, step);
        }
        Node::Var(var) => {
            // EEOP_SCAN_VAR: read the scan-slot attribute into the scratch. M3
            // plans only SeqScans, so every Var is a scan Var (OUTER/INNER for join
            // inputs grow with join nodes). System / whole-row Vars grow later.
            crate::assert!(var.varattno > 0, "system/whole-row Var not yet reachable");
            crate::assert!(
                var.varreturningtype == VarReturningType::DEFAULT,
                "OLD/NEW returning Var not yet reachable"
            );
            expr_eval_push_step(
                state,
                ExprEvalStep {
                    opcode: ExprEvalOp::SCAN_VAR,
                    resvalue: None,
                    resnull: None,
                    d: ExprEvalStepData::Var(VarData {
                        attnum: i32::from(var.varattno) - 1,
                        vartype: var.vartype,
                        varreturningtype: var.varreturningtype,
                    }),
                },
            );
        }
        Node::OpExpr(op) => {
            // An OpExpr is a function call through the operator's opfuncid.
            let step = exec_init_func(node, &op.args, op.opfuncid, op.inputcollid);
            expr_eval_push_step(state, step);
        }
        Node::FuncExpr(func) => {
            if func.funcretset {
                not_yet_reachable("ExecInitExprRec: set-returning function");
            }
            let step = exec_init_func(node, &func.args, func.funcid, func.inputcollid);
            expr_eval_push_step(state, step);
        }
        Node::BoolExpr(b) => exec_init_boolexpr(b, state),
        // M4 (step 23): NULLIF is an OpExpr-shaped node; the interp special-cases it
        // (NULL when args compare equal). Compiled like a strict 2-arg function over
        // the "=" operator, then tagged NULLIF.
        Node::NullIfExpr(op) => {
            let mut step = exec_init_func(node, &op.args, op.opfuncid, op.inputcollid);
            step.opcode = ExprEvalOp::NULLIF;
            expr_eval_push_step(state, step);
        }
        // RelabelType: a no-op binary relabel -- just compile the inner expr (its
        // result already has the right physical representation).
        Node::RelabelType(r) => {
            let arg = r.arg.as_ref().unwrap_or_else(|| not_yet_reachable("RelabelType: no arg"));
            exec_init_expr_rec(arg, state);
        }
        Node::CoerceViaIO(c) => exec_init_coerce_via_io(c, state),
        Node::CaseExpr(c) => exec_init_case(c, state),
        Node::CoalesceExpr(c) => exec_init_coalesce(c, state),
        Node::MinMaxExpr(m) => exec_init_minmax(m, state),
        // EEOP_AGGREF: inside an Agg node, an Aggref evaluates to the finalized
        // per-group value nodeAgg deposited in `econtext.ecxt_aggvalues[aggno]`.
        // The planner stamps `aggno` (the aggregate's slot in that array).
        Node::Aggref(aggref) => {
            expr_eval_push_step(
                state,
                ExprEvalStep {
                    opcode: ExprEvalOp::AGGREF,
                    resvalue: None,
                    resnull: None,
                    d: ExprEvalStepData::Aggref(crate::executor::execExpr::AggrefData {
                        aggno: aggref.aggno,
                    }),
                },
            );
        }
        other => not_yet_reachable(&format!("ExecInitExprRec: {other:?}")),
    }
}

/// PG `ExecInitExprRec` T_CoerceViaIO arm: out-func(source) then in-func(target).
/// Resolves both I/O functions at init (Send-owned fmgr addrs); the arg sub-program
/// produces the value to coerce.
fn exec_init_coerce_via_io(c: &crate::nodes::primnodes::CoerceViaIO, state: &mut ExprState) {
    use crate::backend::utils::cache::lsyscache::{get_type_input_info, get_type_output_info};
    use crate::executor::execExpr::IocoerceData;
    use crate::nodes::nodeFuncs::exprType;

    let arg = c.arg.as_ref().unwrap_or_else(|| not_yet_reachable("CoerceViaIO: no arg"));
    let source_type = exprType(arg);
    let (typoutput, _) = get_type_output_info(source_type);
    let (typinput, typioparam) = get_type_input_info(c.resulttype);

    let mut out_flinfo = empty_flinfo();
    fmgr_info(typoutput, &mut out_flinfo);
    let mut in_flinfo = empty_flinfo();
    fmgr_info(typinput, &mut in_flinfo);

    let arg_steps = compile_scalar_subprogram(arg);

    expr_eval_push_step(
        state,
        ExprEvalStep {
            opcode: ExprEvalOp::IOCOERCE,
            resvalue: None,
            resnull: None,
            d: ExprEvalStepData::Iocoerce(IocoerceData {
                out_addr: out_flinfo.fn_addr,
                in_addr: in_flinfo.fn_addr,
                finfo_out: Some(Box::new(out_flinfo)),
                fcinfo_data_out: None,
                finfo_in: Some(Box::new(in_flinfo)),
                fcinfo_data_in: None,
                typioparam,
                arg_steps,
            }),
        },
    );
}

/// A zeroed FmgrInfo for an init-time `fmgr_info` lookup.
fn empty_flinfo() -> FmgrInfo {
    FmgrInfo {
        fn_addr: None,
        oid: Oid(0),
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: (),
        expr: None,
    }
}

/// PG `ExecInitExprRec` T_CaseExpr arm (M4 self-contained form): compile the test
/// arg + each (cond, result) arm + the ELSE default into owned sub-programs. The
/// interp evaluates them with CASE short-circuit semantics. (PG emits flat
/// CASE_TESTVAL + JUMP_IF_NOT_TRUE/JUMP steps; the owned-subprogram form is the
/// Send-faithful equivalent -- same semantics, no resv pointers, rules.md s10.)
fn exec_init_case(c: &crate::nodes::primnodes::CaseExpr, state: &mut ExprState) {
    use crate::executor::execExpr::CaseData;

    let arg_steps = c.arg.as_ref().map(compile_scalar_subprogram);
    let when_steps = c
        .args
        .iter()
        .map(|arm| {
            let Node::CaseWhen(w) = arm else {
                not_yet_reachable("ExecInitExprRec: CASE arm is not a CaseWhen");
            };
            let cond = w.expr.as_ref().unwrap_or_else(|| not_yet_reachable("CaseWhen: no condition"));
            let result = w.result.as_ref().unwrap_or_else(|| not_yet_reachable("CaseWhen: no result"));
            (compile_scalar_subprogram(cond), compile_scalar_subprogram(result))
        })
        .collect::<Vec<_>>();
    let default_steps = c.defresult.as_ref().map_or_else(
        || compile_scalar_subprogram(&null_const_node(c.casetype)),
        compile_scalar_subprogram,
    );

    expr_eval_push_step(
        state,
        ExprEvalStep {
            opcode: ExprEvalOp::CASE,
            resvalue: None,
            resnull: None,
            d: ExprEvalStepData::Case(CaseData { arg_steps, when_steps, default_steps }),
        },
    );
}

/// A typed-NULL Const node (the synthesized CASE ELSE when none is written).
fn null_const_node(typeid: Oid) -> Node {
    Node::Const(Box::new(crate::nodes::makefuncs::makeNullConst(
        typeid,
        -1,
        Oid(0),
    )))
}

/// PG `ExecInitExprRec` T_CoalesceExpr arm (M4 self-contained form): each argument
/// is an owned sub-program; the interp returns the first non-NULL.
fn exec_init_coalesce(c: &crate::nodes::primnodes::CoalesceExpr, state: &mut ExprState) {
    use crate::executor::execExpr::CoalesceData;
    let arg_steps = c.args.iter().map(compile_scalar_subprogram).collect();
    expr_eval_push_step(
        state,
        ExprEvalStep {
            opcode: ExprEvalOp::COALESCE,
            resvalue: None,
            resnull: None,
            d: ExprEvalStepData::Coalesce(CoalesceData { arg_steps }),
        },
    );
}

/// PG `ExecInitExprRec` T_MinMaxExpr arm: look up the type's btree comparison
/// function, compile each argument into an owned sub-program, and push a MINMAX step
/// the interp folds (GREATEST keeps the max, LEAST the min; NULLs are skipped).
fn exec_init_minmax(m: &crate::nodes::primnodes::MinMaxExpr, state: &mut ExprState) {
    use crate::executor::execExpr::MinmaxData;

    let nelems = m.args.len();
    let cmp_proc = type_cmp_proc(m.minmaxtype);
    let mut cmp_flinfo = empty_flinfo();
    fmgr_info(cmp_proc, &mut cmp_flinfo);
    let cmp_fn = cmp_flinfo.fn_addr;

    let mut fcinfo = FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: Oid(0),
        isnull: false,
        nargs: 0,
        args: vec![NullableDatum { value: Datum(0), isnull: true }; 2],
    };
    InitFunctionCallInfoData(&mut fcinfo, None, 2, m.inputcollid, None, None);

    let arg_steps = m.args.iter().map(compile_scalar_subprogram).collect();

    expr_eval_push_step(
        state,
        ExprEvalStep {
            opcode: ExprEvalOp::MINMAX,
            resvalue: None,
            resnull: None,
            d: ExprEvalStepData::Minmax(MinmaxData {
                values: vec![Datum(0); nelems],
                nulls: vec![true; nelems],
                nelems: i32::try_from(nelems).unwrap_or(0),
                op: m.op,
                finfo: Some(Box::new(cmp_flinfo)),
                fcinfo_data: Some(Box::new(fcinfo)),
                cmp_addr: cmp_fn,
                arg_steps,
            }),
        },
    );
}

/// The btree comparison function (proc OID) for a MinMax result type. M4 covers the
/// numeric tower + date/timestamp; the general lookup (TYPECACHE_CMP_PROC over the
/// default btree opclass) grows with the type cache.
fn type_cmp_proc(typeid: Oid) -> Oid {
    use crate::catalog::genbki::{
        DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, NUMERICOID, TIMESTAMPOID,
    };
    use crate::utils::fmgroids as f;
    match typeid {
        t if t == INT4OID => f::F_BTINT4CMP,
        t if t == INT2OID => f::F_BTINT2CMP,
        t if t == INT8OID => f::F_BTINT8CMP,
        t if t == FLOAT4OID => f::F_BTFLOAT4CMP,
        t if t == FLOAT8OID => f::F_BTFLOAT8CMP,
        t if t == NUMERICOID => f::F_NUMERIC_CMP,
        t if t == DATEOID => f::F_DATE_CMP,
        t if t == TIMESTAMPOID => f::F_TIMESTAMP_CMP,
        _ => not_yet_reachable(&format!("MinMax: no comparison function for type {}", typeid.0)),
    }
}

/// PG `ExecInitFunc`: build the `EEOP_FUNCEXPR[_STRICT]` step for an OpExpr /
/// FuncExpr. Looks up the function (`fmgr_info`), sizes the fcinfo for `nargs`,
/// fills constant args inline (PG's const-arg shortcut) and compiles each
/// non-constant arg into its own sub-program (`arg_steps[i]`), then chooses the
/// opcode by strictness.
#[allow(clippy::similar_names, reason = "flinfo/fcinfo mirror PG's ExecInitFunc identifiers")]
fn exec_init_func(node: &Node, args: &[Node], funcid: Oid, inputcollid: Oid) -> ExprEvalStep {
    let nargs = args.len();

    let mut flinfo = FmgrInfo {
        fn_addr: None,
        oid: Oid(0),
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: (),
        expr: None,
    };
    fmgr_info(funcid, &mut flinfo);
    fmgr_info_set_expr(Some(Box::new(node.clone())), &mut flinfo);

    if flinfo.retset {
        not_yet_reachable("ExecInitFunc: set-returning function");
    }
    let strict = flinfo.strict;
    let fn_addr = flinfo.fn_addr;

    let mut fcinfo = FunctionCallInfoBaseData {
        flinfo: None,
        context: None,
        resultinfo: None,
        fncollation: Oid(0),
        isnull: false,
        nargs: 0,
        args: vec![NullableDatum { value: Datum(0), isnull: true }; nargs],
    };
    InitFunctionCallInfoData(
        &mut fcinfo,
        None,
        i16::try_from(nargs).unwrap_or(0),
        inputcollid,
        None,
        None,
    );

    // Fill const args inline; compile non-const args into sub-programs.
    let mut arg_steps: Vec<Option<Vec<ExprEvalStep>>> = Vec::with_capacity(nargs);
    for arg in args {
        match arg {
            Node::Const(con) => {
                arg_steps.push(None);
                let i = arg_steps.len() - 1;
                fcinfo.args[i].value = con.constvalue;
                fcinfo.args[i].isnull = con.constisnull;
            }
            other => arg_steps.push(Some(compile_scalar_subprogram(other))),
        }
    }

    // Choose the opcode by strictness (PG also has _1/_2 fast paths and the
    // fusage variants; the plain STRICT covers them semantically here).
    let opcode = if strict && nargs > 0 {
        match nargs {
            1 => ExprEvalOp::FUNCEXPR_STRICT_1,
            2 => ExprEvalOp::FUNCEXPR_STRICT_2,
            _ => ExprEvalOp::FUNCEXPR_STRICT,
        }
    } else {
        ExprEvalOp::FUNCEXPR
    };

    ExprEvalStep {
        opcode,
        resvalue: None,
        resnull: None,
        d: ExprEvalStepData::Func(FuncData {
            finfo: Some(Box::new(flinfo)),
            fcinfo_data: Some(Box::new(fcinfo)),
            fn_addr,
            nargs: i32::try_from(nargs).unwrap_or(0),
            make_ro: false,
            arg_steps,
        }),
    }
}

/// PG `ExecInitExprRec` T_BoolExpr arm: emit the AND/OR/NOT step sequence. Each
/// argument is evaluated into the scratch, then the appropriate BOOL step
/// inspects it (three-valued logic, short-circuit jump to the end on FALSE for
/// AND / TRUE for OR). AND/OR split into FIRST/STEP*/LAST (anynull lives across
/// the steps); NOT is a single step.
fn exec_init_boolexpr(b: &BoolExpr, state: &mut ExprState) {
    let nargs = b.args.len();
    let mut jump_steps: Vec<usize> = Vec::with_capacity(nargs);

    // PG `anynull = palloc(sizeof(bool))`: one cell per BoolExpr, shared by every
    // AND/OR step so a NULL seen by an early step reaches STEP_LAST. Unused by NOT.
    let anynull = (b.boolop != BoolExprType::NOT_EXPR)
        .then(|| std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)));

    for (off, arg) in b.args.iter().enumerate() {
        exec_init_expr_rec(arg, state);

        let opcode = match b.boolop {
            BoolExprType::AND_EXPR => {
                crate::assert!(nargs >= 2);
                if off == 0 {
                    ExprEvalOp::BOOL_AND_STEP_FIRST
                } else if off + 1 == nargs {
                    ExprEvalOp::BOOL_AND_STEP_LAST
                } else {
                    ExprEvalOp::BOOL_AND_STEP
                }
            }
            BoolExprType::OR_EXPR => {
                crate::assert!(nargs >= 2);
                if off == 0 {
                    ExprEvalOp::BOOL_OR_STEP_FIRST
                } else if off + 1 == nargs {
                    ExprEvalOp::BOOL_OR_STEP_LAST
                } else {
                    ExprEvalOp::BOOL_OR_STEP
                }
            }
            BoolExprType::NOT_EXPR => {
                crate::assert!(nargs == 1);
                ExprEvalOp::BOOL_NOT_STEP
            }
        };

        let d = ExprEvalStepData::Boolexpr(BoolexprData {
            anynull: anynull.clone(),
            jumpdone: -1,
        });
        expr_eval_push_step(state, ExprEvalStep { opcode, resvalue: None, resnull: None, d });
        jump_steps.push(state.steps.len() - 1);
    }

    // Patch every BOOL step's jumpdone to the step after the sequence.
    let target = i32::try_from(state.steps.len()).unwrap_or(0);
    for idx in jump_steps {
        if let ExprEvalStepData::Boolexpr(bd) = &mut state.steps[idx].d {
            bd.jumpdone = target;
        }
    }
}

/// Compile a scalar expression into a standalone sub-program (its own step list
/// ending in `EEOP_DONE_RETURN`), for use as a function argument. The interpreter
/// runs it into the fcinfo arg slot (rules.md s10: owned steps, no `resv` ptr).
fn compile_scalar_subprogram(node: &Node) -> Vec<ExprEvalStep> {
    let mut sub = ExprState::default();
    exec_init_expr_rec(node, &mut sub);
    expr_eval_push_step(
        &mut sub,
        ExprEvalStep {
            opcode: ExprEvalOp::DONE_RETURN,
            resvalue: None,
            resnull: None,
            d: ExprEvalStepData::Constval(ConstvalData { value: Datum(0), isnull: false }),
        },
    );
    sub.steps
}

/// PG `ExecInitExpr`: compile a standalone scalar expression. Terminates with
/// `EEOP_DONE_RETURN` (returns the scratch value). `parent` is unused on the M1
/// path (no Var/Param/SubPlan to register against the PlanState).
pub fn exec_init_expr(node: Option<&Expr>, parent: Option<&mut PlanState>) -> Option<Box<ExprState>> {
    let _ = parent;
    // `Expr` is an alias of `Node` in this port (primnodes.rs).
    let node = node?;
    let mut state = Box::new(ExprState {
        expr: Some(node.clone()),
        ..ExprState::default()
    });

    exec_init_expr_rec(node, &mut state);

    expr_eval_push_step(
        &mut state,
        ExprEvalStep {
            opcode: ExprEvalOp::DONE_RETURN,
            resvalue: None,
            resnull: None,
            d: ExprEvalStepData::Constval(ConstvalData {
                value: Datum(0),
                isnull: false,
            }),
        },
    );
    exec_ready_expr(&mut state);
    Some(state)
}

/// PG `ExecBuildProjectionInfo`: compile a targetlist into a ProjectionInfo whose
/// steps deposit each target into `slot` (the projection's result slot).
///
/// A target that is a simple scan `Var` short-circuits to `EEOP_ASSIGN_SCAN_VAR`
/// (copy the scan slot attr straight into the result slot) -- this is the
/// `SELECT * FROM t` / `SELECT a FROM t` path. Any other target compiles the
/// expr into the scratch, then `EEOP_ASSIGN_TMP[_MAKE_RO]` (the const path). When
/// any Var is present the list is prefixed with one `EEOP_SCAN_FETCHSOME` that
/// forces the scan slot deformed up through the largest referenced attribute. The
/// list ends with `EEOP_DONE_NO_RETURN`.
pub fn exec_build_projection_info(
    target_list: &[Node],
    econtext: &mut ExprContext,
    slot: Box<crate::executor::tuptable::TupleTableSlot>,
    parent: Option<&mut PlanState>,
    input_desc: Option<crate::access::tupdesc::TupleDesc>,
) -> Box<ProjectionInfo> {
    let _ = (econtext, parent);

    let mut state = ExprState {
        resultslot: Some(slot),
        ..ExprState::default()
    };

    // Prologue: if any target is a scan Var, deform the scan slot up through the
    // largest referenced attno (PG emits one EEOP_*_FETCHSOME per slot kind).
    let max_scan_attno = target_list
        .iter()
        .filter_map(|n| match n {
            Node::TargetEntry(tle) => match tle.expr.as_ref() {
                Some(Node::Var(v)) => Some(i32::from(v.varattno)),
                _ => None,
            },
            _ => None,
        })
        .max();
    if let Some(last_var) = max_scan_attno {
        let known_desc = input_desc
            
            .unwrap_or_else(|| not_yet_reachable("ExecBuildProjectionInfo: scan Var without input desc"));
        expr_eval_push_step(
            &mut state,
            ExprEvalStep {
                opcode: ExprEvalOp::SCAN_FETCHSOME,
                resvalue: None,
                resnull: None,
                d: ExprEvalStepData::Fetch(FetchData {
                    last_var,
                    fixed: false,
                    known_desc,
                    kind: None,
                }),
            },
        );
    }

    for n in target_list {
        let Node::TargetEntry(tle) = n else {
            not_yet_reachable("ExecBuildProjectionInfo: tlist entry is not a TargetEntry");
        };
        let expr = tle
            .expr
            .as_ref()
            .unwrap_or_else(|| not_yet_reachable("ExecBuildProjectionInfo: empty target expr"));
        let resultnum = i32::from(tle.resno) - 1;

        // Simple-Var fast path: copy the scan slot attr into the result slot.
        if let Node::Var(var) = expr {
            push_assign_scan_var(&mut state, var, resultnum);
            continue;
        }

        // General branch: compile the expr into the scratch, then ASSIGN_TMP.
        exec_init_expr_rec(expr, &mut state);

        // PG: get_typlen(exprType(tle->expr)) == -1 chooses ASSIGN_TMP_MAKE_RO.
        // get_typlen reaches the (untranslated) syscache; on the const path the
        // typlen is carried directly on the Const node, so read it there.
        let make_ro = expr_typlen(expr) == -1;
        let opcode = if make_ro {
            ExprEvalOp::ASSIGN_TMP_MAKE_RO
        } else {
            ExprEvalOp::ASSIGN_TMP
        };
        expr_eval_push_step(
            &mut state,
            ExprEvalStep {
                opcode,
                resvalue: None,
                resnull: None,
                d: ExprEvalStepData::AssignTmp(AssignTmpData { resultnum }),
            },
        );
    }

    expr_eval_push_step(
        &mut state,
        ExprEvalStep {
            opcode: ExprEvalOp::DONE_NO_RETURN,
            resvalue: None,
            resnull: None,
            d: ExprEvalStepData::Constval(ConstvalData {
                value: Datum(0),
                isnull: false,
            }),
        },
    );

    exec_ready_expr(&mut state);

    Box::new(ProjectionInfo {
        state,
        expr_context: None,
    })
}

/// Emit `EEOP_ASSIGN_SCAN_VAR` for a simple scan `Var`. M2 plans only SeqScans,
/// so every projection Var is a scan var (OUTER_VAR/INNER_VAR for join inputs
/// grow with join nodes). `attnum` is the 0-based scan-slot attribute index.
fn push_assign_scan_var(state: &mut ExprState, var: &Var, resultnum: i32) {
    crate::assert!(var.varattno > 0, "system/whole-row Var in projection not yet reachable");
    crate::assert!(
        var.varreturningtype == VarReturningType::DEFAULT,
        "OLD/NEW returning Var not yet reachable"
    );
    expr_eval_push_step(
        state,
        ExprEvalStep {
            opcode: ExprEvalOp::ASSIGN_SCAN_VAR,
            resvalue: None,
            resnull: None,
            d: ExprEvalStepData::AssignVar(AssignVarData {
                resultnum,
                attnum: i32::from(var.varattno) - 1,
            }),
        },
    );
}

/// PG `ExecInitQual`: compile a qual (an implicit-AND list of boolean clauses)
/// for `ExecQual`. An empty qual yields None (C returns NULL, which `ExecQual`
/// treats as always-true). Each clause is compiled into the scratch, then an
/// `EEOP_QUAL` step short-circuits the whole qual to FALSE if that clause is
/// FALSE or NULL; otherwise control falls through. The last clause's TRUE value
/// is the qual's result, returned by the trailing `EEOP_DONE_RETURN`.
///
/// The scan slot is virtual (fully deformed by the SeqScan), so PG's
/// `ExecCreateExprSetupSteps` FETCHSOME prologue is a documented no-op here and
/// is omitted; `EEOP_SCAN_VAR` reads `ecxt_scantuple` directly.
pub fn exec_init_qual(qual: &[Node], parent: Option<&mut PlanState>) -> Option<Box<ExprState>> {
    let _ = parent;
    if qual.is_empty() {
        return None;
    }

    let mut state = Box::new(ExprState {
        flags: EeoFlag::IS_QUAL,
        ..ExprState::default()
    });

    let mut qual_steps: Vec<usize> = Vec::with_capacity(qual.len());
    for clause in qual {
        // First evaluate the clause expression into the scratch.
        exec_init_expr_rec(clause, &mut state);
        // Then emit EEOP_QUAL to detect FALSE/NULL and short-circuit.
        expr_eval_push_step(
            &mut state,
            ExprEvalStep {
                opcode: ExprEvalOp::QUAL,
                resvalue: None,
                resnull: None,
                d: ExprEvalStepData::Qualexpr(QualexprData { jumpdone: -1 }),
            },
        );
        qual_steps.push(state.steps.len() - 1);
    }

    // Adjust the QUAL jump targets to the step after the qual sequence.
    let target = i32::try_from(state.steps.len()).unwrap_or(0);
    for idx in qual_steps {
        if let ExprEvalStepData::Qualexpr(q) = &mut state.steps[idx].d {
            q.jumpdone = target;
        }
    }

    expr_eval_push_step(
        &mut state,
        ExprEvalStep {
            opcode: ExprEvalOp::DONE_RETURN,
            resvalue: None,
            resnull: None,
            d: ExprEvalStepData::Constval(ConstvalData { value: Datum(0), isnull: false }),
        },
    );

    exec_ready_expr(&mut state);
    Some(state)
}

/// PG `ExecQual` (inline in executor.h): evaluate a qual prepared by
/// `ExecInitQual`. `None` (empty qual) is always-true. The `EEOP_QUAL` steps
/// guarantee a non-NULL boolean result, so the qual's value is the answer.
pub fn exec_qual(state: Option<&mut ExprState>, econtext: &mut ExprContext) -> bool {
    let Some(state) = state else {
        return true;
    };
    crate::assert!(state.flags.contains(EeoFlag::IS_QUAL));
    let evalfunc = state
        .evalfunc
        .unwrap_or_else(|| unimplemented!("ExecQual: qual not ready"));
    let mut isnull = false;
    let ret = evalfunc(state, econtext, &mut isnull);
    crate::assert!(!isnull, "EEOP_QUAL should never return NULL");
    crate::postgres::DatumGetBool(ret)
}

/// typlen of an expr's result (selects ASSIGN_TMP vs ASSIGN_TMP_MAKE_RO; -1 means
/// varlena/expanded -> MAKE_RO). A `Const` carries its own `constlen`; an
/// OpExpr/FuncExpr routes its result type through the (warm) syscache via
/// `get_typlenbyval`. Other node kinds grow later.
fn expr_typlen(expr: &Node) -> i32 {
    let typlen = |oid: Oid| i32::from(crate::backend::utils::cache::lsyscache::get_typlenbyval(oid).0);
    match expr {
        Node::Const(con) => con.constlen,
        Node::OpExpr(op) | Node::NullIfExpr(op) => typlen(op.opresulttype),
        Node::FuncExpr(f) => typlen(f.funcresulttype),
        Node::RelabelType(r) => typlen(r.resulttype),
        Node::CoerceViaIO(c) => typlen(c.resulttype),
        Node::CaseExpr(c) => typlen(c.casetype),
        Node::CoalesceExpr(c) => typlen(c.coalescetype),
        Node::MinMaxExpr(m) => typlen(m.minmaxtype),
        other => not_yet_reachable(&format!("expr_typlen: {other:?}")),
    }
}

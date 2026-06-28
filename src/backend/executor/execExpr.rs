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
    AssignTmpData, AssignVarData, ConstvalData, ExprEvalOp, ExprEvalStep, ExprEvalStepData,
    FetchData, VarData,
};
use crate::nodes::execnodes::{ExprContext, ExprState, PlanState, ProjectionInfo};
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{Expr, Var, VarReturningType};
use crate::postgres::Datum;

use crate::backend::executor::execExprInterp::exec_ready_interpreted_expr;

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
        other => not_yet_reachable(&format!("ExecInitExprRec: {other:?}")),
    }
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

/// PG `ExecInitQual`: an empty qual (the M1 const path has none) yields None
/// (C returns NULL, which `ExecQual` treats as always-true). A non-empty qual
/// grows in later milestones.
pub fn exec_init_qual(qual: &[Node], parent: Option<&mut PlanState>) -> Option<Box<ExprState>> {
    let _ = parent;
    if qual.is_empty() {
        return None;
    }
    not_yet_reachable("ExecInitQual: non-empty qual");
}

/// typlen of an expr's result. The M1 path only reaches a `Const`, which carries
/// its own `constlen`; generic types route through the (untranslated) syscache
/// and grow later.
fn expr_typlen(expr: &Node) -> i32 {
    match expr {
        Node::Const(con) => con.constlen,
        other => not_yet_reachable(&format!("expr_typlen: {other:?}")),
    }
}

//! Handle the parse analysis of expressions. Translated from
//! backend/parser/parse_expr.c.
//!
//! Non-type-centric free functions (`transformExpr`, `ParseExprKindName`); bodies
//! here as snake_case `pub fn`s with the C symbol in the doc comment, re-exported
//! from `crate::parser::parse_expr` under the C names.
//!
//! Disposition: `grow`. `transform_expr_recurse` is parse_expr.c's central
//! `switch (nodeTag)` dispatcher. It is scaffolded so each later milestone fills
//! one arm (ColumnRef, A_Expr, FuncCall, ...) without restructuring; for M1 only
//! the `A_Const` arm (-> `make_const`) and the trivially-reachable NULL arm are
//! live. Every other tag routes through a single clearly-marked
//! `not_yet_reachable` staging arm (rules.md s4); none is half-written.

use crate::catalog::genbki::UNKNOWNOID;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{ColumnRef, ColumnRefField};
use crate::parser::parse_node::{ParseExprKind, ParseState};

/// Panic for an expression node tag whose transform arm reaches a subsystem not
/// yet translated for this milestone (rules.md s4). Distinct from PG's
/// `elog(ERROR, "unrecognized node type")` for a genuinely bad tag.
#[cold]
fn not_yet_reachable(node: &Node) -> ! {
    unimplemented!("transformExprRecurse: node tag not yet reachable for this milestone: {node:?}");
}

/// PG `transformExpr`: analyze and transform an expression, saving/restoring the
/// `p_expr_kind` identity around the recursion so context-specific error messages
/// are correct. A NULL input transforms to NULL.
pub fn transformExpr(
    pstate: &mut ParseState,
    expr: Option<Node>,
    expr_kind: ParseExprKind,
) -> Option<Node> {
    crate::assert!(expr_kind != ParseExprKind::None);
    let sv_expr_kind = pstate.p_expr_kind;
    pstate.p_expr_kind = expr_kind;

    let result = transform_expr_recurse(pstate, expr);

    pstate.p_expr_kind = sv_expr_kind;
    result
}

/// PG `transformExprRecurse`: the per-nodetag transform dispatcher (file-local in
/// parse_expr.c, so private here). Grows one arm per milestone.
fn transform_expr_recurse(pstate: &mut ParseState, expr: Option<Node>) -> Option<Node> {
    // Need do nothing for an empty subexpression.
    let expr = expr?;

    // PG guards recursion depth with check_stack_depth(); the recursive descent
    // here is bounded by the same call graph and grows with it.
    match expr {
        Node::A_Const(aconst) => {
            Some(Node::Const(crate::parser::parse_node::make_const(pstate, &aconst)))
        }
        Node::ColumnRef(cref) => Some(transform_column_ref(pstate, &cref)),
        Node::A_Expr(aexpr) => Some(transform_a_expr(pstate, *aexpr)),
        Node::BoolExpr(bexpr) => Some(transform_bool_expr(pstate, *bexpr)),
        Node::FuncCall(fc) => Some(transform_func_call(pstate, &fc)),
        // M4 (step 23): casts + conditional expressions.
        Node::TypeCast(tc) => Some(transform_type_cast(pstate, *tc)),
        Node::CaseExpr(c) => Some(transform_case_expr(pstate, *c)),
        Node::CoalesceExpr(c) => Some(transform_coalesce_expr(pstate, *c)),
        Node::MinMaxExpr(m) => Some(transform_min_max_expr(pstate, *m)),
        Node::ParamRef(pref) => Some(transform_param_ref(pstate, *pref)),
        // M12 (step 44): a sub-SELECT expression (EXISTS / IN / ANY / ALL / scalar).
        // The raw sub-select was analyzed into a Query by the async pre-analyze pass
        // (see analyze::pre_analyze_sublinks) so this sync transform only validates +
        // builds the testexpr.
        Node::SubLink(sl) => Some(transformSubLink(pstate, *sl)),
        Node::BooleanTest(b) => Some(transform_bool_test(pstate, *b)),
        // ... arms are filled by later milestones; for now they route here cleanly.
        other => not_yet_reachable(&other),
    }
}

/// PG `transformTypeCast`: transform the cast's argument, then coerce it to the
/// named target type in EXPLICIT context (the `CAST(x AS t)` / `x::t` form).
fn transform_type_cast(pstate: &mut ParseState, tc: crate::nodes::parsenodes::TypeCast) -> Node {
    use crate::nodes::nodeFuncs::exprType;
    use crate::nodes::primnodes::{CoercionContext, CoercionForm};

    let type_name = tc.typeName.unwrap_or_else(|| not_yet_reachable_msg("TypeCast: missing typeName"));
    let (target_type, target_typmod) =
        crate::parser::parse_type::typenameTypeIdAndMod(pstate, &type_name);

    let expr = transform_expr_recurse(pstate, tc.arg)
        .unwrap_or_else(|| not_yet_reachable_msg("TypeCast: NULL argument"));
    let input_type = exprType(&expr);

    crate::parser::parse_coerce::coerce_to_target_type(
        pstate,
        Some(expr),
        input_type,
        target_type,
        target_typmod,
        CoercionContext::EXPLICIT,
        CoercionForm::EXPLICIT_CAST,
        tc.location,
    )
    .unwrap_or_else(|| cannot_cast(input_type, target_type))
}

#[cold]
fn cannot_cast(input_type: crate::postgres_ext::Oid, target_type: crate::postgres_ext::Oid) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_CANNOT_COERCE)
            .errmsg(format!("cannot cast type {} to {}", input_type.get(), target_type.get()));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `transformCaseExpr`: transform the (optional) test arg + the WHEN/THEN arms +
/// the ELSE default, pick the common result type, and coerce every result branch to
/// it. The simple-CASE form (a test arg) expands each WHEN into `arg = whenval`.
fn transform_case_expr(pstate: &mut ParseState, c: crate::nodes::primnodes::CaseExpr) -> Node {
    use crate::nodes::nodeFuncs::{exprType, exprTypmod};
    use crate::nodes::primnodes::{CaseExpr, CaseTestExpr, CaseWhen};
    use crate::parser::parse_coerce::{coerce_to_boolean, coerce_to_common_type, select_common_type};
    use crate::postgres_ext::InvalidOid;

    // Transform the test expression, if any, and build the CaseTestExpr placeholder.
    let arg = transform_expr_recurse(pstate, c.arg);
    let arg = arg.map(|a| {
        // An untyped literal test arg is forced to text (PG).
        if exprType(&a) == UNKNOWNOID {
            coerce_to_common_type(pstate, a, crate::catalog::genbki::TEXTOID, "CASE")
        } else {
            a
        }
    });
    let placeholder = arg.as_ref().map(|a| CaseTestExpr {
        typeId: exprType(a),
        typeMod: exprTypmod(a),
        collation: InvalidOid,
    });

    // Transform each WHEN/THEN arm. For the simple form, expand `WHEN v` into the
    // implicit-equality `placeholder = v`.
    let mut new_args: Vec<Node> = Vec::with_capacity(c.args.len());
    let mut result_exprs: Vec<Node> = Vec::new();
    for arm in c.args {
        let Node::CaseWhen(w) = arm else {
            not_yet_reachable_msg("transformCaseExpr: arm is not a CaseWhen");
        };
        let warg = if let Some(ph) = &placeholder {
            let a = crate::nodes::makefuncs::makeSimpleA_Expr(
                crate::nodes::parsenodes::A_Expr_Kind::OP,
                "=",
                Some(Node::CaseTestExpr(Box::new(ph.clone()))),
                w.expr,
                w.location,
            );
            Some(Node::A_Expr(Box::new(a)))
        } else {
            w.expr
        };
        let cond = transform_expr_recurse(pstate, warg)
            .unwrap_or_else(|| not_yet_reachable_msg("transformCaseExpr: NULL WHEN"));
        let cond = coerce_to_boolean(pstate, cond, "CASE/WHEN");
        let result = transform_expr_recurse(pstate, w.result)
            .unwrap_or_else(|| not_yet_reachable_msg("transformCaseExpr: NULL THEN"));
        result_exprs.push(result.clone());
        new_args.push(Node::CaseWhen(Box::new(CaseWhen {
            expr: Some(cond),
            result: Some(result),
            location: w.location,
        })));
    }

    // Transform the default (ELSE); an absent ELSE is a NULL A_Const (PG synthesizes
    // `makeNode(A_Const)` with isnull). make_const turns that into an UNKNOWN NULL
    // Const, which select_common_type ignores and coerce_to_common_type retypes.
    let raw_default = c.defresult.unwrap_or_else(|| {
        Node::A_Const(Box::new(crate::nodes::parsenodes::A_Const {
            val: crate::nodes::parsenodes::ValUnion::Integer(crate::nodes::value::makeInteger(0)),
            isnull: true,
            location: -1,
        }))
    });
    let defresult = transform_expr_recurse(pstate, Some(raw_default))
        .unwrap_or_else(|| not_yet_reachable_msg("transformCaseExpr: NULL default"));

    // The default is the most-significant type (PG prepends it).
    let mut all_results = vec![defresult.clone()];
    all_results.extend(result_exprs);
    let (ptype, _) = select_common_type(pstate, &all_results, "CASE");

    let defresult = coerce_to_common_type(pstate, defresult, ptype, "CASE/ELSE");
    let new_args = new_args
        .into_iter()
        .map(|arm| {
            let Node::CaseWhen(w) = arm else { unreachable!() };
            let result = coerce_to_common_type(
                pstate,
                w.result.unwrap_or_else(|| not_yet_reachable_msg("CaseWhen: NULL result")),
                ptype,
                "CASE/WHEN",
            );
            Node::CaseWhen(Box::new(CaseWhen { expr: w.expr, result: Some(result), location: w.location }))
        })
        .collect();

    Node::CaseExpr(Box::new(CaseExpr {
        casetype: ptype,
        casecollid: InvalidOid,
        arg,
        args: new_args,
        defresult: Some(defresult),
        location: c.location,
    }))
}

/// PG `transformCoalesceExpr`: transform each argument, pick the common type, and
/// coerce every argument to it.
fn transform_coalesce_expr(pstate: &mut ParseState, c: crate::nodes::primnodes::CoalesceExpr) -> Node {
    use crate::parser::parse_coerce::{coerce_to_common_type, select_common_type};
    use crate::postgres_ext::InvalidOid;

    let newargs: Vec<Node> = c
        .args
        .into_iter()
        .map(|a| {
            transform_expr_recurse(pstate, Some(a))
                .unwrap_or_else(|| not_yet_reachable_msg("transformCoalesceExpr: NULL arg"))
        })
        .collect();
    let (ctype, _) = select_common_type(pstate, &newargs, "COALESCE");
    let newcoercedargs = newargs
        .into_iter()
        .map(|a| coerce_to_common_type(pstate, a, ctype, "COALESCE"))
        .collect();

    Node::CoalesceExpr(Box::new(crate::nodes::primnodes::CoalesceExpr {
        coalescetype: ctype,
        coalescecollid: InvalidOid,
        args: newcoercedargs,
        location: c.location,
    }))
}

/// PG `transformMinMaxExpr`: transform each GREATEST/LEAST argument, pick the common
/// type, and coerce every argument to it.
fn transform_min_max_expr(pstate: &mut ParseState, m: crate::nodes::primnodes::MinMaxExpr) -> Node {
    use crate::parser::parse_coerce::{coerce_to_common_type, select_common_type};
    use crate::postgres_ext::InvalidOid;

    let funcname = if matches!(m.op, crate::nodes::primnodes::MinMaxOp::GREATEST) {
        "GREATEST"
    } else {
        "LEAST"
    };
    let newargs: Vec<Node> = m
        .args
        .into_iter()
        .map(|a| {
            transform_expr_recurse(pstate, Some(a))
                .unwrap_or_else(|| not_yet_reachable_msg("transformMinMaxExpr: NULL arg"))
        })
        .collect();
    let (mtype, _) = select_common_type(pstate, &newargs, funcname);
    let newcoercedargs = newargs
        .into_iter()
        .map(|a| coerce_to_common_type(pstate, a, mtype, funcname))
        .collect();

    Node::MinMaxExpr(Box::new(crate::nodes::primnodes::MinMaxExpr {
        minmaxtype: mtype,
        minmaxcollid: InvalidOid,
        inputcollid: InvalidOid,
        op: m.op,
        args: newcoercedargs,
        location: m.location,
    }))
}

/// PG `transformAExprOp` (the AEXPR_OP arm of transformAExpr): transform both
/// operands and resolve the operator into an `OpExpr` via `make_op`. M3 reaches the
/// plain binary operator; the row-comparison / "expr op (subselect)" / scalar-array
/// special cases grow at their milestones.
fn transform_a_expr(pstate: &mut ParseState, a: crate::nodes::parsenodes::A_Expr) -> Node {
    use crate::nodes::parsenodes::A_Expr_Kind;
    match a.kind {
        A_Expr_Kind::OP => {
            let last_srf = pstate.p_last_srf.clone();
            let lexpr = transform_expr_recurse(pstate, a.lexpr);
            let rexpr = transform_expr_recurse(pstate, a.rexpr);
            crate::parser::parse_oper::make_op(pstate, &a.name, lexpr, rexpr, last_srf.as_ref(), a.location)
        }
        // M4 (step 23): NULLIF(a, b) -> a NullIfExpr (an OpExpr-shaped node carrying
        // the "=" operator; the executor returns NULL when the operands are equal).
        A_Expr_Kind::NULLIF => transform_nullif(pstate, a),
        _ => unimplemented!("transformAExpr: A_Expr kind {:?} not yet reachable for this milestone", a.kind),
    }
}

/// PG `transformAExprNullIf`: resolve the "=" operator over the two operands and
/// wrap it as a NullIfExpr (a `NullIfExpr` is an `OpExpr` with the comparison op;
/// the interpreter returns NULL when the args compare equal, else the first arg).
/// PG checks the operator yields boolean. M4 reaches int4/numeric/float8 "=".
fn transform_nullif(pstate: &mut ParseState, a: crate::nodes::parsenodes::A_Expr) -> Node {
    use crate::catalog::genbki::BOOLOID;
    let last_srf = pstate.p_last_srf.clone();
    let lexpr = transform_expr_recurse(pstate, a.lexpr);
    let rexpr = transform_expr_recurse(pstate, a.rexpr);
    // make_op resolves "=" to an OpExpr(opno, opfuncid, opresulttype=bool).
    let result = crate::parser::parse_oper::make_op(
        pstate, &a.name, lexpr, rexpr, last_srf.as_ref(), a.location,
    );
    let Node::OpExpr(op) = result else {
        not_yet_reachable_msg("transformAExprNullIf: NULLIF operator did not resolve to an OpExpr");
    };
    // The operator must yield boolean (PG raises otherwise).
    if op.opresulttype != BOOLOID {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DATATYPE_MISMATCH)
                .errmsg("NULLIF requires = operator to yield boolean".to_owned());
        });
        unreachable!("ereport(ERROR) diverges");
    }
    // A NullIfExpr is an OpExpr-shaped node (distinct node tag) whose result type is
    // its first argument's type. Relabel opresulttype to arg0's type so exprType
    // reports correctly, and emit the NullIfExpr tag (the interpreter special-cases
    // it: NULL when the args compare equal, else arg0).
    let arg0_type = crate::nodes::nodeFuncs::exprType(&op.args[0]);
    Node::NullIfExpr(Box::new(crate::nodes::primnodes::OpExpr {
        opresulttype: arg0_type,
        ..*op
    }))
}

/// PG `transformBoolExpr`: transform each argument to bool and build the BoolExpr.
/// Each argument is coerced to boolean (`coerce_to_boolean`); for M3 the AND/OR/NOT
/// operands are already boolean expressions (comparisons), so the coercion is the
/// identity. The non-boolean-argument coercion grows with `coerce_to_boolean`.
fn transform_bool_expr(pstate: &mut ParseState, b: crate::nodes::primnodes::BoolExpr) -> Node {
    let args = b
        .args
        .into_iter()
        .map(|arg| {
            transform_expr_recurse(pstate, Some(arg))
                .unwrap_or_else(|| not_yet_reachable_msg("transformBoolExpr: NULL argument"))
        })
        .collect();
    Node::BoolExpr(Box::new(crate::nodes::primnodes::BoolExpr {
        boolop: b.boolop,
        args,
        location: b.location,
    }))
}

/// PG `transformBooleanTest`: transform the arg, coerce it to boolean (naming the
/// IS [NOT] {TRUE|FALSE|UNKNOWN} clause in the error), and rebuild the BooleanTest.
fn transform_bool_test(pstate: &mut ParseState, b: crate::nodes::primnodes::BooleanTest) -> Node {
    use crate::nodes::primnodes::{BooleanTest, BoolTestType};
    use crate::parser::parse_coerce::coerce_to_boolean;

    let clausename = match b.booltesttype {
        BoolTestType::TRUE => "IS TRUE",
        BoolTestType::NOT_TRUE => "IS NOT TRUE",
        BoolTestType::FALSE => "IS FALSE",
        BoolTestType::NOT_FALSE => "IS NOT FALSE",
        BoolTestType::UNKNOWN => "IS UNKNOWN",
        BoolTestType::NOT_UNKNOWN => "IS NOT UNKNOWN",
    };

    let arg = transform_expr_recurse(pstate, b.arg)
        .unwrap_or_else(|| not_yet_reachable_msg("transformBooleanTest: NULL argument"));
    let arg = coerce_to_boolean(pstate, arg, clausename);

    Node::BooleanTest(Box::new(BooleanTest {
        arg: Some(arg),
        booltesttype: b.booltesttype,
        location: b.location,
    }))
}

/// PG `transformFuncCall` -> `ParseFuncOrColumn`: transform the argument list and
/// resolve the function (or column projection) into a FuncExpr.
fn transform_func_call(pstate: &mut ParseState, fc: &crate::nodes::parsenodes::FuncCall) -> Node {
    let args = fc
        .args
        .iter()
        .cloned()
        .map(|arg| {
            transform_expr_recurse(pstate, Some(arg))
                .unwrap_or_else(|| not_yet_reachable_msg("transformFuncCall: NULL argument"))
        })
        .collect();
    crate::parser::parse_func::ParseFuncOrColumn(pstate, &fc.funcname, args, fc, fc.location)
}

#[cold]
fn not_yet_reachable_msg(msg: &str) -> ! {
    unimplemented!("{msg}");
}

/// PG `transformSubLink`: finish analyzing a sub-SELECT expression. The raw
/// sub-select was already analyzed into a `Query` by the async pre-analyze pass
/// (`analyze::pre_analyze_sublinks`), which runs after the FROM clause is in place
/// so correlation (outer) column references resolve to uplevel Vars. Here we set
/// `p_has_sub_links`, validate the column count, and (for ANY/ALL/ROWCOMPARE) build
/// the row-comparison testexpr using PARAM_SUBLINK placeholders for the subquery's
/// output columns. EXISTS/EXPR carry no testexpr.
#[allow(non_snake_case)]
pub fn transformSubLink(pstate: &mut ParseState, mut sublink: crate::nodes::primnodes::SubLink) -> Node {
    use crate::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};
    use crate::nodes::primnodes::{Param, ParamKind, SubLinkType};

    pstate.p_has_sub_links = true;

    let Some(Node::Query(qtree)) = sublink.subselect.as_ref() else {
        not_yet_reachable_msg("transformSubLink: sub-select was not pre-analyzed into a Query");
    };
    if qtree.commandType != crate::nodes::nodes::CmdType::SELECT {
        not_yet_reachable_msg("transformSubLink: non-SELECT command in SubLink");
    }

    // The non-junk output columns of the subquery's target list.
    let nonjunk: Vec<&crate::nodes::primnodes::TargetEntry> = qtree
        .targetList
        .iter()
        .filter_map(|n| match n {
            Node::TargetEntry(te) if !te.resjunk => Some(&**te),
            _ => None,
        })
        .collect();

    match sublink.subLinkType {
        SubLinkType::EXISTS_SUBLINK => {
            sublink.testexpr = None;
            sublink.operName = Vec::new();
        }
        SubLinkType::EXPR_SUBLINK => {
            if nonjunk.len() != 1 {
                subquery_one_column_error(pstate, sublink.location);
            }
            sublink.testexpr = None;
            sublink.operName = Vec::new();
        }
        SubLinkType::ANY_SUBLINK | SubLinkType::ALL_SUBLINK => {
            // The parser stored the raw left-hand expression in `testexpr`; transform
            // it now (it is no longer top-level). For our single-column support the
            // lhs is a scalar expression.
            let lefthand = transform_expr_recurse(pstate, sublink.testexpr.take());
            let lefthand = lefthand.unwrap_or_else(|| {
                not_yet_reachable_msg("transformSubLink: ANY/ALL with no left-hand expression")
            });

            // Build a PARAM_SUBLINK Param per non-junk output column. Single-column
            // only here (multi-column row comparison grows with RowExpr).
            if nonjunk.len() != 1 {
                subquery_one_column_error(pstate, sublink.location);
            }
            let tent = nonjunk[0];
            let texpr = tent.expr.as_ref().unwrap_or_else(|| {
                not_yet_reachable_msg("transformSubLink: subquery target has no expression")
            });
            let param = Node::Param(Box::new(Param {
                paramkind: ParamKind::SUBLINK,
                paramid: i32::from(tent.resno),
                paramtype: exprType(texpr),
                paramtypmod: exprTypmod(texpr),
                paramcollid: exprCollation(texpr),
                location: -1,
            }));

            // make_row_comparison_op single-column case: `lefthand <op> PARAM_SUBLINK`.
            let testexpr = crate::backend::parser::parse_oper::make_op(
                pstate,
                &sublink.operName,
                Some(lefthand),
                Some(param),
                pstate.p_last_srf.clone().as_ref(),
                sublink.location,
            );
            sublink.testexpr = Some(testexpr);
        }
        other => {
            not_yet_reachable_msg(&format!("transformSubLink: subLinkType {other:?} not yet reachable"));
        }
    }

    Node::SubLink(Box::new(sublink))
}

#[cold]
fn subquery_one_column_error(_pstate: &ParseState, _location: i32) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
            .errmsg("subquery must return only one column".to_owned());
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `transformParamRef`: the core parser knows nothing about Params; if a
/// paramref hook is set, call it. If not, or it returns NULL, raise "there is no
/// parameter $n". (The hooks are installed by parse_param's setup functions.)
fn transform_param_ref(pstate: &mut ParseState, mut pref: crate::nodes::parsenodes::ParamRef) -> Node {
    let result = pstate.p_paramref_hook.and_then(|hook| hook(pstate, &mut pref));
    result.unwrap_or_else(|| no_such_parameter(pref.number))
}

#[cold]
fn no_such_parameter(number: i32) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_PARAMETER)
            .errmsg(format!("there is no parameter ${number}"));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `transformColumnRef`: resolve a `ColumnRef` to a `Var` (or whole-row ref).
/// M2 covers an unqualified column (`a`) and a table-qualified column (`t.a`); the
/// 3-part (schema-qualified) form, whole-row `t.*`, the pre/post columnref hooks,
/// and the backwards-compatible bare-relation-name path grow at their milestones.
fn transform_column_ref(pstate: &mut ParseState, cref: &ColumnRef) -> Node {
    use crate::backend::parser::parse_relation::{col_name_to_var, scan_ns_item_for_column};

    match cref.fields.as_slice() {
        [ColumnRefField::String(field1)] => {
            let colname = &field1.sval;
            col_name_to_var(pstate, colname, false, cref.location)
                .unwrap_or_else(|| undefined_column(colname))
        }
        [ColumnRefField::String(field1), ColumnRefField::String(field2)] => {
            let relname = &field1.sval;
            let colname = &field2.sval;
            // PG `transformColumnRef`: a table-qualified column searches the current
            // level first, then walks up the parent ParseStates (a correlated
            // sub-select reference), incrementing sublevels_up so the Var carries the
            // right varlevelsup (M12, step 44).
            let mut levels_up: crate::c::Index = 0;
            let mut cur: Option<&ParseState> = Some(pstate);
            while let Some(ps) = cur {
                if let Some(idx) = refname_namespace_item(ps, relname) {
                    return scan_ns_item_for_column(&ps.p_namespace[idx], levels_up, colname, cref.location)
                        .unwrap_or_else(|| undefined_column(colname));
                }
                cur = ps.parent_parse_state.as_deref();
                levels_up += 1;
            }
            missing_from_entry(relname)
        }
        [.., ColumnRefField::Star(_)] => {
            unimplemented!("transformColumnRef: whole-row (table.*) reference not yet translated for this milestone");
        }
        _ => unimplemented!("transformColumnRef: 3+-part / schema-qualified column reference not yet translated for this milestone"),
    }
}

/// PG `refnameNamespaceItem` (M2 subset): find the index of a namespace item whose
/// refname (eref aliasname) equals `relname`. Only the current level is searched
/// (no parent-level / schema-qualified lookup yet).
pub(crate) fn refname_namespace_item(pstate: &ParseState, relname: &str) -> Option<usize> {
    pstate
        .p_namespace
        .iter()
        .position(|ns| ns.rel_visible && ns.names.aliasname.as_deref() == Some(relname))
}

#[cold]
fn undefined_column(colname: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!("column \"{colname}\" does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}

#[cold]
fn missing_from_entry(relname: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE).errmsg(format!(
            "missing FROM-clause entry for table \"{relname}\""
        ));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `ParseExprKindName`: the user-facing name of a `ParseExprKind`, for error
/// messages ("... is not allowed in WHERE", etc).
#[allow(
    clippy::match_same_arms,
    reason = "1:1 with PG's per-ParseExprKind switch; distinct kinds share a label \
              (WHERE/COPY WHERE, VALUES, RETURNING) - merging arms loses the mapping"
)]
pub fn ParseExprKindName(expr_kind: ParseExprKind) -> &'static str {
    use ParseExprKind as K;
    match expr_kind {
        K::None => "invalid expression context",
        K::Other => "extension expression",
        K::JoinOn => "JOIN/ON",
        K::JoinUsing => "JOIN/USING",
        K::FromSubselect => "sub-SELECT in FROM",
        K::FromFunction => "function in FROM",
        K::Where => "WHERE",
        K::Policy => "POLICY",
        K::Having => "HAVING",
        K::Filter => "FILTER",
        K::WindowPartition => "window PARTITION BY",
        K::WindowOrder => "window ORDER BY",
        K::WindowFrameRange => "window RANGE",
        K::WindowFrameRows => "window ROWS",
        K::WindowFrameGroups => "window GROUPS",
        K::SelectTarget => "SELECT",
        K::InsertTarget => "INSERT",
        K::UpdateSource | K::UpdateTarget => "UPDATE",
        K::MergeWhen => "MERGE WHEN",
        K::GroupBy => "GROUP BY",
        K::OrderBy => "ORDER BY",
        K::DistinctOn => "DISTINCT ON",
        K::Limit => "LIMIT",
        K::Offset => "OFFSET",
        K::Returning | K::MergeReturning => "RETURNING",
        K::Values | K::ValuesSingle => "VALUES",
        K::CheckConstraint | K::DomainCheck => "CHECK",
        K::ColumnDefault | K::FunctionDefault => "DEFAULT",
        K::IndexExpression => "index expression",
        K::IndexPredicate => "index predicate",
        K::StatsExpression => "statistics expression",
        K::AlterColTransform => "USING",
        K::ExecuteParameter => "EXECUTE",
        K::TriggerWhen => "WHEN",
        K::PartitionBound => "partition bound",
        K::PartitionExpression => "PARTITION BY",
        K::CallArgument => "CALL",
        K::CopyWhere => "WHERE",
        K::GeneratedColumn => "GENERATED AS",
        K::CycleMark => "CYCLE",
    }
}

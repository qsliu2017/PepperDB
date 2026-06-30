//! Function-call (and func-vs-column) resolution for the parser. Translated from
//! backend/parser/parse_func.c.
//!
//! Non-type-centric free functions (`ParseFuncOrColumn`, `func_get_detail`,
//! `make_fn_expr`); bodies here as snake_case `pub fn`s, re-exported under the C
//! names from the header `crate::parser::parse_func`.
//!
//! Disposition: M3 FULL for a plain function call over known argument types.
//! `ParseFuncOrColumn` transforms the call into a `FuncExpr`: `func_get_detail`
//! resolves the function name + argument types to its pg_proc entry (exact match via
//! the PROCNAMEARGSNSP syscache), and `make_fn_expr` builds the node. The
//! aggregate/window/variadic/named-argument/default-argument paths, the
//! type-coercion candidate resolution, and the func-vs-column projection fallback
//! grow at their milestones; the M3-reachable `f(int4, ...)` exact-match call
//! resolves directly.

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to Form_pg_proc (MAXALIGN'd body covers the Form alignment)"
)]

use crate::access::htup::HeapTupleData;
use crate::access::htup_details::GETSTRUCT;
use crate::backend::utils::cache::syscache::{release_sys_cache, search_sys_cache};
use crate::c::NameData;
use crate::catalog::pg_namespace::PG_CATALOG_NAMESPACE;
use crate::catalog::pg_proc::{Form_pg_proc, FormData_pg_proc};
use crate::nodes::nodeFuncs::exprType;
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{CoercionForm, FuncExpr};
use crate::parser::parse_func::FuncDetailCode;
use crate::parser::parse_node::ParseState;
use crate::postgres::{Datum, NameGetDatum, ObjectIdGetDatum, PointerGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::syscache::SysCacheIdentifier;

/// The resolved-function details `func_get_detail` returns (PG's trailing pointer
/// out-params, folded into one struct). M3 fills the regular-function subset; M5
/// (step 26) adds `prokind` so the aggregate path is recognized.
pub struct FuncDetail {
    pub funcid: Oid,
    pub rettype: Oid,
    pub retset: bool,
    pub prokind: i8,
}

/// Read the `Form_pg_proc` out of a held syscache tuple; the borrow is tied to the
/// tuple borrow (rule 10).
///
/// SAFETY: `tuple`'s fixed part is a pg_proc row (a held PROCNAMEARGSNSP hit).
unsafe fn proc_form(tuple: &HeapTupleData) -> &FormData_pg_proc {
    let p: Form_pg_proc = GETSTRUCT(tuple).cast::<FormData_pg_proc>();
    // SAFETY: `p` points into `tuple`'s body; the borrow is rooted in `tuple`.
    unsafe { &*p }
}

/// Build a `NameData` for a function name (the PROCNAMEARGSNSP name key is a `name`
/// column).
fn name_key(name: &str) -> NameData {
    let mut nd = NameData { data: [0u8; crate::c::NAMEDATALEN] };
    let bytes = name.as_bytes();
    let n = bytes.len().min(crate::c::NAMEDATALEN - 1);
    nd.data[..n].copy_from_slice(&bytes[..n]);
    nd
}

/// Deconstruct a function name list (M3 subset): an unqualified function name is a
/// one-element list of a String value node. The schema-qualified form grows with
/// the multi-namespace resolver.
fn func_name_str(funcname: &[Node]) -> &str {
    match funcname {
        [Node::String_(s)] => &s.sval,
        _ => unimplemented!("function name: schema-qualified function not yet reachable"),
    }
}

/// PG `func_get_detail` (M3 subset): resolve `funcname` over `argtypes` to a regular
/// function by exact match via the PROCNAMEARGSNSP syscache (proname, proargtypes
/// oidvector, pronamespace). The candidate-list resolution (binary-coercible /
/// polymorphic matches, variadic/default expansion), aggregates, window functions,
/// and type-coercion requests grow at their milestones.
#[must_use]
pub fn func_get_detail(funcname: &[Node], argtypes: &[Oid]) -> (FuncDetailCode, Option<FuncDetail>) {
    let name = func_name_str(funcname);

    // Exact match first (the M3-reachable regular/aggregate call).
    if let Some(mut detail) = lookup_proc_by_argvec(name, argtypes) {
        let code = code_for_prokind(detail.prokind);
        // A polymorphic declared return type can only appear via the polymorphic
        // match below; an exact match returns the row's concrete rettype as-is.
        let _ = &mut detail;
        return (code, Some(detail));
    }

    // M12 (step 42): polymorphic fallback for the window functions whose arguments
    // are `anyelement` / `anycompatible` (lag/lead/first_value/last_value/nth_value).
    // Substitute the polymorphic arg type for each concrete arg and retry; if the row
    // resolves, the declared `anyelement` return type is the actual first-arg type.
    if !argtypes.is_empty() {
        // The reachable window functions take a polymorphic value at position 0 (and,
        // for the 3-arg lag/lead-with-default form, at position 2); the offset at
        // position 1 stays concrete. Try those substitution masks.
        let masks: &[&[usize]] = &[&[0], &[0, 2]];
        for poly in [ANYELEMENTOID, ANYCOMPATIBLEOID] {
            for mask in masks {
                let poly_args: Vec<Oid> = argtypes
                    .iter()
                    .enumerate()
                    .map(|(i, &t)| if mask.contains(&i) { poly } else { t })
                    .collect();
                if let Some(mut detail) = lookup_proc_by_argvec(name, &poly_args) {
                    // A polymorphic declared return type resolves to the first
                    // argument's concrete type (the window funcs return `anyelement`).
                    if is_polymorphic_type(detail.rettype) {
                        detail.rettype = argtypes[0];
                    }
                    let code = code_for_prokind(detail.prokind);
                    return (code, Some(detail));
                }
            }
        }
    }

    (FuncDetailCode::NotFound, None)
}

/// Look up a pg_proc row by (name, exact argtype vector) via PROCNAMEARGSNSP.
fn lookup_proc_by_argvec(name: &str, argtypes: &[Oid]) -> Option<FuncDetail> {
    let nd = name_key(name);
    let argvec = crate::utils::builtins::buildoidvector(argtypes);
    let keys = [
        NameGetDatum(&nd),
        PointerGetDatum(argvec.cast::<u8>()),
        ObjectIdGetDatum(PG_CATALOG_NAMESPACE),
    ];
    let tup = search_sys_cache(SysCacheIdentifier::PROCNAMEARGSNSP, &keys)?;
    // SAFETY: `tup` is a held PROCNAMEARGSNSP hit -> a pg_proc row.
    let form = unsafe { proc_form(&*tup) };
    let detail = FuncDetail {
        funcid: form.oid,
        rettype: form.prorettype,
        retset: form.proretset,
        prokind: form.prokind,
    };
    release_sys_cache(tup);
    Some(detail)
}

/// Map a pg_proc `prokind` to the FuncDetailCode the caller dispatches on.
fn code_for_prokind(prokind: i8) -> FuncDetailCode {
    // M5 (step 26): an aggregate proc (prokind 'a') routes to the aggregate path.
    // M12 (step 42): a window proc (prokind 'w') routes to the window path.
    match prokind {
        crate::catalog::pg_proc::PROKIND_AGGREGATE => FuncDetailCode::Aggregate,
        crate::catalog::pg_proc::PROKIND_WINDOW => FuncDetailCode::WindowFunc,
        _ => FuncDetailCode::Normal,
    }
}

/// The polymorphic pseudo-type OIDs the window-function fallback substitutes.
const ANYELEMENTOID: Oid = Oid::new(2283);
const ANYCOMPATIBLEOID: Oid = Oid::new(5077);
const ANYARRAYOID: Oid = Oid::new(2277);

/// Whether a declared type is polymorphic (resolved to a concrete type from the
/// actual arguments).
fn is_polymorphic_type(t: Oid) -> bool {
    t == ANYELEMENTOID || t == ANYCOMPATIBLEOID || t == ANYARRAYOID
}

/// PG `make_fn_expr` (the FuncExpr-building tail of ParseFuncOrColumn): build the
/// `FuncExpr` from the resolved function and the (already type-correct) arguments.
fn make_fn_expr(detail: &FuncDetail, args: Vec<Node>, funcformat: CoercionForm, location: i32) -> Node {
    Node::FuncExpr(Box::new(FuncExpr {
        funcid: detail.funcid,
        funcresulttype: detail.rettype,
        funcretset: detail.retset,
        funcvariadic: false,
        funcformat,
        // funccollid / inputcollid are set by parse_collate.c.
        funccollid: InvalidOid,
        inputcollid: InvalidOid,
        args,
        location,
    }))
}

/// PG `ParseFuncOrColumn` (M3 subset): resolve a function call `name(args)` to a
/// `FuncExpr`. The arguments are already transformed. M3 covers a plain positional
/// call whose argument types exactly match a regular function; the func-vs-column
/// projection fallback, aggregates/window funcs, VARIADIC / named / default args,
/// and the argument type-coercion (`make_fn_arguments`) grow at their milestones.
pub fn parse_func_or_column(
    pstate: &mut ParseState,
    funcname: &[Node],
    fargs: Vec<Node>,
    fn_: &crate::nodes::parsenodes::FuncCall,
    location: i32,
) -> Node {
    let actual_arg_types: Vec<Oid> = fargs.iter().map(exprType).collect();

    let (fdresult, detail) = func_get_detail(funcname, &actual_arg_types);
    let over = fn_.over.is_some();
    match fdresult {
        // M12 (step 42): a true window function (prokind 'w') requires an OVER
        // clause; an aggregate or ordinary function WITH an OVER clause is a window
        // call too (a "window aggregate"). Both build a WindowFunc.
        FuncDetailCode::WindowFunc => {
            let detail = detail.unwrap_or_else(|| unreachable!("WINDOWFUNC implies a detail"));
            if !over {
                let name = func_name_str(funcname);
                crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                    e.errcode(crate::utils::errcodes::ERRCODE_WRONG_OBJECT_TYPE)
                        .errmsg(format!("window function {name} requires an OVER clause"));
                });
                unreachable!("ereport(ERROR) diverges");
            }
            make_windowfunc(pstate, &detail, fargs, fn_, false, location)
        }
        FuncDetailCode::Aggregate if over => {
            // An ordinary aggregate invoked with OVER -> a window aggregate.
            let detail = detail.unwrap_or_else(|| unreachable!("AGGREGATE implies a detail"));
            make_windowfunc(pstate, &detail, fargs, fn_, true, location)
        }
        FuncDetailCode::Normal => {
            let detail = detail.unwrap_or_else(|| unreachable!("NORMAL implies a detail"));
            if over {
                not_yet_reachable("ParseFuncOrColumn: OVER on a non-aggregate/non-window function");
            }
            // M3 argument types match the function's declared types exactly, so no
            // make_fn_arguments coercion is needed (wired with the coercible path).
            make_fn_expr(&detail, fargs, CoercionForm::EXPLICIT_CALL, location)
        }
        FuncDetailCode::Aggregate => {
            // M5 (step 26): build the Aggref and resolve it through transformAggregateCall.
            let detail = detail.unwrap_or_else(|| unreachable!("AGGREGATE implies a detail"));
            make_aggref(pstate, &detail, fargs, fn_, location)
        }
        _ => {
            // The window / procedure / coercion FuncDetailCodes and the func-vs-column
            // projection fallback grow at their milestones; an unresolved name errors.
            let name = func_name_str(funcname);
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_FUNCTION)
                    .errmsg(format!("function {name} does not exist"));
            });
            unreachable!("ereport(ERROR) diverges");
        }
    }
}

/// PG `ParseFuncOrColumn` aggregate arm (M5 subset): build the partially-filled
/// `Aggref` (aggfnoid/aggtype/args/aggstar from the resolved detail + raw call) and
/// hand it to `transformAggregateCall`, which fills the remaining fields and marks
/// `pstate.p_hasAggs`. Returns the `Aggref` node.
fn make_aggref(
    pstate: &mut ParseState,
    detail: &FuncDetail,
    fargs: Vec<Node>,
    fn_: &crate::nodes::parsenodes::FuncCall,
    location: i32,
) -> Node {
    use crate::nodes::nodes::AggSplit;
    use crate::nodes::primnodes::Aggref;

    let mut aggref = Aggref {
        aggfnoid: detail.funcid,
        aggtype: detail.rettype,
        aggcollid: InvalidOid,
        inputcollid: InvalidOid,
        aggtranstype: InvalidOid, // set by the planner (resolve_aggregate_transtype)
        aggargtypes: Vec::new(),
        aggdirectargs: Vec::new(),
        args: Vec::new(),  // filled by transformAggregateCall (TargetEntry list)
        aggorder: Vec::new(),
        aggdistinct: Vec::new(),
        aggfilter: None,
        aggstar: fn_.agg_star,
        aggvariadic: false,
        aggkind: b'n' as i8,
        aggpresorted: false,
        agglevelsup: 0,
        aggsplit: AggSplit::SIMPLE,
        aggno: -1,
        aggtransno: -1,
        location,
    };

    crate::parser::parse_agg::transformAggregateCall(
        pstate,
        &mut aggref,
        fargs,
        fn_.agg_order.clone(),
        fn_.agg_distinct,
    );

    Node::Aggref(Box::new(aggref))
}

/// Panic for a func path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `ParseFuncOrColumn` window arm (M12, step 42): build a `WindowFunc` from the
/// resolved function and the raw call's OVER window, then link it to its
/// `WindowClause` via `transformWindowFuncCall`. `is_agg` marks a window aggregate
/// (an ordinary aggregate with OVER, evaluated via the agg trans/final path) versus
/// a true window function (row_number etc.).
fn make_windowfunc(
    pstate: &mut ParseState,
    detail: &FuncDetail,
    fargs: Vec<Node>,
    fn_: &crate::nodes::parsenodes::FuncCall,
    is_agg: bool,
    location: i32,
) -> Node {
    use crate::nodes::primnodes::WindowFunc;

    if fn_.agg_distinct {
        not_yet_reachable("ParseFuncOrColumn: DISTINCT in a window function");
    }
    if !fn_.agg_order.is_empty() {
        not_yet_reachable("ParseFuncOrColumn: ORDER BY in a window function");
    }
    if fn_.agg_filter.is_some() {
        not_yet_reachable("ParseFuncOrColumn: FILTER on a window function");
    }

    let mut wfunc = WindowFunc {
        winfnoid: detail.funcid,
        wintype: detail.rettype,
        wincollid: InvalidOid,
        inputcollid: InvalidOid,
        args: fargs,
        aggfilter: None,
        runCondition: Vec::new(),
        winref: 0,             // set by transformWindowFuncCall
        winstar: fn_.agg_star,
        winagg: is_agg,
        location,
    };

    let over = fn_
        .over
        .as_ref()
        .unwrap_or_else(|| unreachable!("window call without OVER"));
    crate::parser::parse_agg::transformWindowFuncCall(pstate, &mut wfunc, over);

    Node::WindowFunc(Box::new(wfunc))
}

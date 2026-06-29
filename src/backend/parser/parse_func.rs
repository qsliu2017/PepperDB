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
/// out-params, folded into one struct). M3 fills the regular-function subset.
pub struct FuncDetail {
    pub funcid: Oid,
    pub rettype: Oid,
    pub retset: bool,
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
    let nd = name_key(name);
    let argvec = crate::utils::builtins::buildoidvector(argtypes);
    let keys = [
        NameGetDatum(&nd),
        PointerGetDatum(argvec.cast::<u8>()),
        ObjectIdGetDatum(PG_CATALOG_NAMESPACE),
    ];
    let Some(tup) = search_sys_cache(SysCacheIdentifier::PROCNAMEARGSNSP, &keys) else {
        return (FuncDetailCode::NotFound, None);
    };
    // SAFETY: `tup` is a held PROCNAMEARGSNSP hit -> a pg_proc row.
    let form = unsafe { proc_form(&*tup) };
    let detail = FuncDetail { funcid: form.oid, rettype: form.prorettype, retset: form.proretset };
    release_sys_cache(tup);
    (FuncDetailCode::Normal, detail.into())
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
    _pstate: &ParseState,
    funcname: &[Node],
    fargs: Vec<Node>,
    fn_: &crate::nodes::parsenodes::FuncCall,
    location: i32,
) -> Node {
    let _ = fn_;
    let actual_arg_types: Vec<Oid> = fargs.iter().map(exprType).collect();

    let (fdresult, detail) = func_get_detail(funcname, &actual_arg_types);
    if fdresult == FuncDetailCode::Normal {
        let detail = detail.unwrap_or_else(|| unreachable!("NORMAL implies a detail"));
        // M3 argument types match the function's declared types exactly, so no
        // make_fn_arguments coercion is needed (wired with the coercible path).
        return make_fn_expr(&detail, fargs, CoercionForm::EXPLICIT_CALL, location);
    }
    // The aggregate / window / procedure / coercion FuncDetailCodes and the
    // func-vs-column projection fallback grow at their milestones; an unresolved
    // name is an error here.
    let name = func_name_str(funcname);
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!("function {name} does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}

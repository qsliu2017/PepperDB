//! Operator resolution for the parser. Translated from
//! backend/parser/parse_oper.c (+ the OpernameGetOprid helper from namespace.c).
//!
//! Non-type-centric free functions (`make_op`, `oper`, `LookupOperName`); bodies
//! here as snake_case `pub fn`s, re-exported under the C names from the header
//! `crate::parser::parse_oper`.
//!
//! Disposition: M3 FULL for the binary-operator-over-known-types path. `make_op`
//! resolves a binary operator name + left/right type OIDs to its pg_operator entry
//! (via the OPERNAMENSP syscache, exact match), reads `oprcode`/`oprresult`, and
//! builds the OpExpr. The oper-cache lookaside, the candidate-resolution fallback
//! (`oper_select_candidate` for binary-coercible matches), prefix/postfix operators,
//! and `make_scalar_array_op` grow at their milestones; the M3-reachable arithmetic
//! and comparison operators over int4 resolve by exact match alone.

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to Form_pg_operator (MAXALIGN'd body covers the Form alignment)"
)]

use crate::access::htup::HeapTupleData;
use crate::access::htup_details::GETSTRUCT;
use crate::backend::utils::cache::lsyscache::get_func_retset;
use crate::backend::utils::cache::syscache::{release_sys_cache, search_sys_cache};
use crate::c::NameData;
use crate::catalog::pg_namespace::PG_CATALOG_NAMESPACE;
use crate::catalog::pg_operator::{Form_pg_operator, FormData_pg_operator};
use crate::nodes::nodeFuncs::exprType;
use crate::nodes::nodes::Node;
use crate::nodes::primnodes::OpExpr;
use crate::parser::parse_node::ParseState;
use crate::postgres::{Datum, NameGetDatum, ObjectIdGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::syscache::SysCacheIdentifier;

/// A held operator syscache tuple, exactly PG's `Operator` typedef (an opaque
/// `HeapTuple` that must be `ReleaseSysCache`d).
pub type Operator = crate::access::htup::HeapTuple;

/// Read the `Form_pg_operator` out of a held syscache tuple. The borrow is tied to
/// the tuple borrow (rule 10); the caller must hold the syscache reference for at
/// least as long as the borrow lives.
///
/// SAFETY: `tuple`'s fixed part is a pg_operator row (a held OPEROID/OPERNAMENSP
/// syscache hit).
unsafe fn oper_form(tuple: &HeapTupleData) -> &FormData_pg_operator {
    let p: Form_pg_operator = GETSTRUCT(tuple).cast::<FormData_pg_operator>();
    // SAFETY: `p` points into `tuple`'s body; the borrow is rooted in `tuple`.
    unsafe { &*p }
}

/// Build a `NameData` for an operator name (the OPERNAMENSP name key is a `name`
/// column; the catcache compares the NUL-padded 64-byte form).
fn name_key(name: &str) -> NameData {
    let mut nd = NameData { data: [0u8; crate::c::NAMEDATALEN] };
    let bytes = name.as_bytes();
    let n = bytes.len().min(crate::c::NAMEDATALEN - 1);
    nd.data[..n].copy_from_slice(&bytes[..n]);
    nd
}

/// PG `OpernameGetOprid` (M3 subset): resolve an unqualified operator name + left
/// and right type OIDs to its operator OID via the OPERNAMENSP syscache, searching
/// the `pg_catalog` namespace (the active search-path walk grows with the multi-
/// namespace resolver). Returns `InvalidOid` if no exact-typed operator exists.
#[must_use]
pub fn opername_get_oprid(opername: &str, oprleft: Oid, oprright: Oid) -> Oid {
    let nd = name_key(opername);
    let keys = [
        NameGetDatum(&nd),
        ObjectIdGetDatum(oprleft),
        ObjectIdGetDatum(oprright),
        ObjectIdGetDatum(PG_CATALOG_NAMESPACE),
    ];
    let Some(tup) = search_sys_cache(SysCacheIdentifier::OPERNAMENSP, &keys) else {
        return InvalidOid;
    };
    // SAFETY: `tup` is a held OPERNAMENSP hit -> a pg_operator row.
    let oid = unsafe { oper_form(&*tup) }.oid;
    release_sys_cache(tup);
    oid
}

/// PG `binary_oper_exact`: try for an exact (oprleft, oprright) match. The
/// UNKNOWN-substitution / base-type retry grows with the unknown-literal path; M3
/// reaches the both-types-known case.
fn binary_oper_exact(opername: &str, arg1: Oid, arg2: Oid) -> Oid {
    opername_get_oprid(opername, arg1, arg2)
}

/// Deconstruct an operator name list (PG `DeconstructQualifiedName`, M3 subset):
/// an unqualified operator name is a one-element list of a String value node. The
/// schema-qualified form grows with the multi-namespace resolver.
fn oper_name_str(opname: &[Node]) -> &str {
    match opname {
        [Node::String_(s)] => &s.sval,
        _ => unimplemented!("operator name: schema-qualified operator not yet reachable"),
    }
}

/// PG `oper`: find the binary operator for `opername` over (`ltype`, `rtype`),
/// returning the held OPEROID syscache tuple. M3 resolves by exact match; the
/// candidate-list fallback (`oper_select_candidate`) and the oper-cache lookaside
/// grow later. `no_error` returns `None` instead of raising when unresolved.
#[must_use]
pub fn oper(
    pstate: &ParseState,
    opname: &[Node],
    ltype: Oid,
    rtype: Oid,
    no_error: bool,
    location: i32,
) -> Option<Operator> {
    let opername = oper_name_str(opname);
    let oper_oid = binary_oper_exact(opername, ltype, rtype);
    if oper_oid != InvalidOid
        && let Some(tup) =
            search_sys_cache(SysCacheIdentifier::OPEROID, &[ObjectIdGetDatum(oper_oid)])
    {
        return Some(tup);
    }
    if no_error {
        return None;
    }
    op_error(pstate, opername, ltype, rtype, location);
}

/// PG `make_op`: resolve a binary operator and build the `OpExpr`. M3 covers the
/// binary form over two known argument types; the prefix form, polymorphic /
/// binary-coercible argument coercion (`make_fn_arguments`), and the set-returning
/// operator check grow at their milestones. `last_srf` (the most recent
/// set-returning function, for SRF placement checks) is threaded but unused until
/// the set-returning operator path lands.
pub fn make_op(
    pstate: &ParseState,
    opname: &[Node],
    ltree: Option<Node>,
    rtree: Option<Node>,
    last_srf: Option<&Node>,
    location: i32,
) -> Node {
    let _ = last_srf;
    let Some(rtree) = rtree else {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
                .errmsg("postfix operators are not supported".to_owned());
        });
        unreachable!("ereport(ERROR) diverges");
    };

    let Some(ltree) = ltree else {
        // Prefix operator (e.g. unary minus). Resolution mirrors the binary case
        // but with InvalidOid left type; the candidate machinery + coercion grow
        // with the broader unary path.
        unimplemented!("make_op: prefix operator resolution not yet reachable for this milestone");
    };

    let ltype_id = exprType(&ltree);
    let rtype_id = exprType(&rtree);
    let tup = oper(pstate, opname, ltype_id, rtype_id, false, location)
        .unwrap_or_else(|| unreachable!("oper(no_error=false) raises on failure"));

    // SAFETY: `tup` is a held OPEROID hit -> a pg_operator row.
    let opform = unsafe { oper_form(&*tup) };

    if opform.oprcode == InvalidOid {
        let opno = opform.oid;
        release_sys_cache(tup);
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_FUNCTION)
                .errmsg(format!("operator is only a shell (opno {})", opno.get()));
        });
        unreachable!("ereport(ERROR) diverges");
    }

    let opno = opform.oid;
    let opfuncid = opform.oprcode;
    let opresulttype = opform.oprresult;
    release_sys_cache(tup);

    // M3 argument types match the operator's declared types exactly (int4/int4),
    // so make_fn_arguments performs no coercion; the binary-coercible/UNKNOWN
    // coercion is wired with the unknown-literal path.
    let opretset = get_func_retset(opfuncid);

    let result = OpExpr {
        opno,
        opfuncid,
        opresulttype,
        opretset,
        // opcollid / inputcollid are set by parse_collate.c (assign_expr_collations).
        opcollid: InvalidOid,
        inputcollid: InvalidOid,
        args: vec![ltree, rtree],
        location,
    };
    Node::OpExpr(Box::new(result))
}

/// PG `op_error`: raise the "operator does not exist" error. Diverges (>= ERROR).
#[cold]
fn op_error(pstate: &ParseState, opname: &str, ltype: Oid, rtype: Oid, location: i32) -> ! {
    let _ = (pstate, location);
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_FUNCTION).errmsg(format!(
            "operator does not exist: {opname} (left {}, right {})",
            ltype.get(), rtype.get()
        ));
    });
    unreachable!("ereport(ERROR) diverges");
}

//! Other node-manipulation utility functions for the parser. Translated from
//! backend/parser/parse_node.c.
//!
//! Non-type-centric free functions (PG names `make_parsestate`, `make_const`,
//! `transformContainerType`, ...). Bodies live here as snake_case `pub fn`s with
//! the C symbol in the doc comment; the header `crate::parser::parse_node`
//! re-exports each under its C name so call sites keep resolving.
//!
//! Disposition: `full`. `ParseState` is per-call state (rules.md s8): a plain
//! owned struct threaded by `&mut`, NOT a task-local or shared singleton.
//! `palloc0(ParseState)` becomes constructing the struct with its non-zero
//! defaults; `pfree` is `Drop`. `make_const` is translated for every value-node
//! arm whose target type machinery is already present (Integer/Boolean/String);
//! the Float/BitString arms reach `numeric_in`/`bit_in`, not yet translated, and
//! stage to `unimplemented!()` per rules.md s4.

use crate::catalog::genbki::{BOOLOID, INT4OID, UNKNOWNOID};
use crate::nodes::makefuncs::makeConst;
use crate::nodes::parsenodes::{A_Const, ValUnion};
use crate::nodes::primnodes::Const;
use crate::nodes::value::{boolVal, intVal};
use crate::parser::parse_node::{ParseExprKind, ParseState};
use crate::postgres::{BoolGetDatum, Datum, Int32GetDatum};
use crate::postgres_ext::InvalidOid;

/// PG `make_parsestate`: allocate and initialize a `ParseState`.
///
/// `palloc0` zero-fills, then PG sets the fields that don't start at zero/null/
/// false. Here we build the struct directly with those defaults: `p_next_resno`
/// starts at 1 and `p_resolve_unknowns` at true; everything else is the empty/
/// null/false default. When a parent is supplied, the source text, the parser
/// hooks, and the query environment are inherited.
/// Build a fresh child `ParseState` that inherits the parent's source text, parser
/// hooks, query environment, and CTE namespace, but has its OWN rangetable /
/// namespace / join state. Unlike [`make_parsestate`] this does NOT move the parent
/// into the stack link (the caller keeps `&mut parent`), so it is used where each
/// sibling sub-Query must be analyzed against a clean namespace while still seeing
/// the enclosing WITH list -- e.g. the per-branch SELECTs of a set operation (PG's
/// `parse_sub_analyze` over a transient child ParseState). The caller is
/// responsible for propagating the child's `p_has_aggs` / `p_has_sub_links` /
/// `p_has_modifying_cte` flags back to the parent.
pub fn make_child_parsestate(parent: &ParseState) -> Box<ParseState> {
    Box::new(ParseState {
        parent_parse_state: None,
        p_sourcetext: parent.p_sourcetext.clone(),
        p_rtable: Vec::new(),
        p_rteperminfos: Vec::new(),
        p_joinexprs: Vec::new(),
        p_nullingrels: Vec::new(),
        p_joinlist: Vec::new(),
        p_namespace: Vec::new(),
        p_lateral_active: false,
        p_ctenamespace: parent.p_ctenamespace.clone(),
        p_future_ctes: parent.p_future_ctes.clone(),
        p_parent_cte: parent.p_parent_cte.clone(),
        p_target_relation: None,
        p_target_nsitem: None,
        p_grouping_nsitem: None,
        p_is_insert: false,
        p_windowdefs: Vec::new(),
        p_expr_kind: ParseExprKind::None,
        p_next_resno: 1,
        p_multiassign_exprs: Vec::new(),
        p_locking_clause: Vec::new(),
        p_locked_from_parent: false,
        p_resolve_unknowns: true,
        p_query_env: None,
        p_has_aggs: false,
        p_has_window_funcs: false,
        p_has_target_srfs: false,
        p_has_sub_links: false,
        p_has_modifying_cte: false,
        p_last_srf: None,
        p_pre_columnref_hook: parent.p_pre_columnref_hook,
        p_post_columnref_hook: parent.p_post_columnref_hook,
        p_paramref_hook: parent.p_paramref_hook,
        p_coerce_param_hook: parent.p_coerce_param_hook,
        p_ref_hook_state: crate::backend::parser::parse_param::ParamRefHookState::None,
    })
}

pub fn make_parsestate(parent_parse_state: Option<Box<ParseState>>) -> Box<ParseState> {
    // Inherited-from-parent fields (copied before the parent is moved into the
    // stack link). All None/default at top level. The query environment is shared
    // (a pointer in C); it stays None until parse_sub_analyze threads it, which is
    // not reachable for M1.
    let (p_sourcetext, pre, post, paramref, coerce) =
        parent_parse_state.as_deref().map_or((None, None, None, None, None), |parent| {
            (
                parent.p_sourcetext.clone(),
                parent.p_pre_columnref_hook,
                parent.p_post_columnref_hook,
                parent.p_paramref_hook,
                parent.p_coerce_param_hook,
            )
        });

    Box::new(ParseState {
        parent_parse_state,
        p_sourcetext,
        p_rtable: Vec::new(),
        p_rteperminfos: Vec::new(),
        p_joinexprs: Vec::new(),
        p_nullingrels: Vec::new(),
        p_joinlist: Vec::new(),
        p_namespace: Vec::new(),
        p_lateral_active: false,
        p_ctenamespace: Vec::new(),
        p_future_ctes: Vec::new(),
        p_parent_cte: None,
        p_target_relation: None,
        p_target_nsitem: None,
        p_grouping_nsitem: None,
        p_is_insert: false,
        p_windowdefs: Vec::new(),
        p_expr_kind: ParseExprKind::None,
        p_next_resno: 1,
        p_multiassign_exprs: Vec::new(),
        p_locking_clause: Vec::new(),
        p_locked_from_parent: false,
        p_resolve_unknowns: true,
        p_query_env: None,
        p_has_aggs: false,
        p_has_window_funcs: false,
        p_has_target_srfs: false,
        p_has_sub_links: false,
        p_has_modifying_cte: false,
        p_last_srf: None,
        p_pre_columnref_hook: pre,
        p_post_columnref_hook: post,
        p_paramref_hook: paramref,
        p_coerce_param_hook: coerce,
        p_ref_hook_state: crate::backend::parser::parse_param::ParamRefHookState::None,
    })
}

/// PG `free_parsestate`: validate the resno count and release a `ParseState`.
///
/// PG bounds the target-list size at `MaxTupleAttributeNumber` (resnos overflow
/// `AttrNumber` past that) and closes any open target relation before `pfree`.
/// The relation-close and `pfree` are `Drop` here; this keeps the resno guard.
pub fn free_parsestate(pstate: &mut ParseState) {
    if pstate.p_next_resno - 1 > crate::access::htup_details::MaxTupleAttributeNumber {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_TOO_MANY_COLUMNS).errmsg(format!(
                "target lists can have at most {} entries",
                crate::access::htup_details::MaxTupleAttributeNumber
            ));
        });
    }
    // Closing p_target_relation (table_close) and pfree(pstate) are RAII in this
    // port; nothing further to do here.
}

/// PG `make_const`: build a `Const` node from an `A_Const` literal value.
///
/// `pstate` is needed only by the Float/BitString error-position callback paths
/// (deferred); the Integer/Boolean/String arms ignore it. A NULL `A_Const`
/// produces an UNKNOWN-typed null const, matching PG.
pub fn make_const(_pstate: &mut ParseState, aconst: &A_Const) -> Box<Const> {
    if aconst.isnull {
        // return a null const
        let mut con = makeConst(UNKNOWNOID, -1, InvalidOid, -2, Datum(0), true, false);
        con.location = aconst.location;
        return Box::new(con);
    }

    let (val, typeid, typelen, typebyval): (Datum, _, i32, bool) = match &aconst.val {
        ValUnion::Integer(i) => (Int32GetDatum(intVal(i)), INT4OID, 4, true),
        ValUnion::Boolean(b) => (BoolGetDatum(boolVal(b)), BOOLOID, 1, true),
        ValUnion::String(s) => {
            // PG `makeConst(UNKNOWNOID, ..., CStringGetDatum(strVal(val)), ...)`: an
            // UNKNOWN-typed const whose value is the literal text as a cstring
            // (UNKNOWN's internal repr == cstring). The owned `String` is leaked to a
            // NUL-terminated buffer so the Datum (a `*const i8`) stays valid for the
            // plan's lifetime, then coerce_type retypes it via the target's typinput.
            // SQL string literals carry no interior NUL (the scanner rejects it); if
            // one slips through, truncate at the first NUL (C-string semantics).
            let bytes = s.sval.as_bytes();
            let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
            let cstr = std::ffi::CString::new(&bytes[..end]).unwrap_or_default();
            let ptr: *const i8 = std::boxed::Box::leak(cstr.into_boxed_c_str()).as_ptr();
            let mut con = makeConst(
                UNKNOWNOID, -1, InvalidOid, -2, crate::postgres::CStringGetDatum(ptr), false, false,
            );
            con.location = aconst.location;
            return Box::new(con);
        }
        ValUnion::Float(_f) => {
            // Could be an oversize integer or a true float; PG runs
            // pg_strtoint64_safe then falls back to numeric_in. Both reach
            // not-yet-translated numeric machinery.
            unimplemented!("make_const T_Float: pg_strtoint64_safe / numeric_in deferred")
        }
        ValUnion::BitString(_b) => {
            unimplemented!("make_const T_BitString: bit_in deferred")
        }
        ValUnion::Node(_) => {
            crate::elog!(
                crate::utils::elog::ERROR,
                "unrecognized node type in A_Const value".to_string()
            );
            unreachable!("elog(ERROR) diverges");
        }
    };

    // typmod -1 is OK for all cases; all cases are uncollatable types.
    let mut con = makeConst(typeid, -1, InvalidOid, typelen, val, false, typebyval);
    con.location = aconst.location;
    Box::new(con)
}

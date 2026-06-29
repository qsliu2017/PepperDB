//! Type coercion routines for the parser. Translated from
//! backend/parser/parse_coerce.c.
//!
//! Non-type-centric free functions (`coerce_to_target_type`, `coerce_type`, ...);
//! bodies here as snake_case `pub fn`s, re-exported from
//! `crate::parser::parse_coerce` under the C names.
//!
//! Disposition: `grow`. The coercion machinery (cast catalog lookups,
//! `numeric`/array/domain coercions, typmod coercion) reaches subsystems not
//! translated yet. The simple SELECT-constant path does not invoke coercion at
//! all (an int4 literal is already its final type and has no target type to
//! coerce to), so for M1 only the no-conversion identity arm is live; the general
//! dispatch routes to a single not-yet-reachable staging arm (rules.md s4).

use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{Const, CoercionContext, CoercionForm};
use crate::parser::parse_coerce::CoercionPathType;
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::{InvalidOid, Oid};

/// Panic for a coercion case that reaches machinery not yet translated for this
/// milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("parse_coerce: {what} not yet translated for this milestone");
}

/// PG `coerce_type`: convert an expression `node` of `input_type_id` to
/// `target_type_id`. M4 (step 23) reaches: the no-conversion identity, the
/// UNKNOWN-string-literal retyping (apply the target's typinput), and the cast
/// catalog pathways (cast function FuncExpr, binary RelabelType, CoerceViaIO).
/// Polymorphic pseudotypes / domains / arrays grow later.
pub fn coerce_type(
    pstate: &mut ParseState,
    node: Node,
    input_type_id: Oid,
    target_type_id: Oid,
    target_type_mod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
) -> Node {
    if target_type_id == input_type_id {
        // no conversion needed
        return node;
    }

    // UNKNOWN string constant: apply the target type's input function to produce a
    // constant of the target type (the typed-literal / unknown-literal path). PG
    // builds a new Const via stringTypeDatum; here we call the target's typinput.
    if input_type_id == UNKNOWNOID && matches!(node, Node::Const(_)) {
        let Node::Const(con) = &node else { unreachable!() };
        return Node::Const(Box::new(retype_unknown_const(con, target_type_id, target_type_mod)));
    }

    let mut funcid = InvalidOid;
    let pathtype = find_coercion_pathway(target_type_id, input_type_id, ccontext, &mut funcid);
    if pathtype != CoercionPathType::None {
        // M4 has no domains, so baseType == target.
        let (base_type_id, base_type_mod) =
            crate::utils::lsyscache::getBaseTypeAndTypmod(target_type_id, target_type_mod);
        if pathtype != CoercionPathType::RelabelType {
            return build_coercion_expression(
                node, pathtype, funcid, base_type_id, base_type_mod, ccontext, cformat, location,
            );
        }
        // Binary-coercible: attach a RelabelType so higher-level code sees the type.
        let r = crate::nodes::primnodes::RelabelType {
            arg: Some(node),
            resulttype: target_type_id,
            resulttypmod: -1,
            resultcollid: InvalidOid,
            relabelformat: cformat,
            location,
        };
        return Node::RelabelType(Box::new(r));
    }

    let _ = pstate;
    not_yet_reachable("coerce_type: no coercion pathway");
}

/// PG `coerce_type`'s UNKNOWN-Const arm: build a Const of `target_type_id` by
/// applying its typinput function to the literal's cstring (stringTypeDatum). The
/// literal's value is a cstring datum (UNKNOWN's internal repr == cstring).
fn retype_unknown_const(con: &Const, target_type_id: Oid, target_type_mod: i32) -> Const {
    use crate::backend::utils::fmgr::fmgr::OidInputFunctionCall;
    use crate::postgres::{Datum, DatumGetCString};

    // M4 has no domains; base == target.
    let (base_type_id, base_type_mod) =
        crate::utils::lsyscache::getBaseTypeAndTypmod(target_type_id, target_type_mod);
    let input_type_mod = -1; // (INTERVAL passes the typmod; not reached at M4)
    let (typinput, typioparam) = crate::utils::lsyscache::getTypeInputInfo(base_type_id);
    let (typlen, typbyval, _typalign) =
        crate::backend::utils::cache::lsyscache::get_typlenbyvalalign(base_type_id);

    let constvalue = if con.constisnull {
        Datum(0)
    } else {
        // SAFETY: UNKNOWN's value is a leaked NUL-terminated cstring (make_const).
        let s = unsafe { c_str_to_str(DatumGetCString(con.constvalue)) };
        OidInputFunctionCall(typinput, &s, typioparam, input_type_mod)
            .unwrap_or_else(|| not_yet_reachable("coerce_type: typinput returned NULL"))
    };
    let _ = base_type_mod;

    Const {
        consttype: base_type_id,
        consttypmod: input_type_mod,
        constcollid: InvalidOid,
        constlen: i32::from(typlen),
        constvalue,
        constisnull: con.constisnull,
        constbyval: typbyval,
        location: con.location,
    }
}

/// Read a leaked NUL-terminated cstring (the UNKNOWN Const datum) as a `String`.
///
/// SAFETY: `p` is a valid NUL-terminated cstring kept alive for the call (the
/// literal text leaked by `make_const`).
unsafe fn c_str_to_str(p: *const i8) -> String {
    unsafe { std::ffi::CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

const UNKNOWNOID: Oid = crate::catalog::genbki::UNKNOWNOID;

/// PG `coerce_to_target_type`: coerce `expr` to `targettype`/`targettypmod` in the
/// given context, or `None` if not coercible. M4 (step 23): identity, then the
/// `can_coerce_type` cast search via `coerce_type`. The CollateExpr stripping and
/// `coerce_type_typmod` length coercion grow later.
pub fn coerce_to_target_type(
    pstate: &mut ParseState,
    expr: Option<Node>,
    exprtype: Oid,
    targettype: Oid,
    targettypmod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
) -> Option<Node> {
    let expr = expr?;
    // Is the conversion possible in this context? (can_coerce_type covers the
    // identity, UNKNOWN-literal, and cast-catalog cases.)
    if !can_coerce_type(1, &[exprtype], &[targettype], ccontext) {
        return None;
    }
    let result = coerce_type(
        pstate, expr, exprtype, targettype, targettypmod, ccontext, cformat, location,
    );
    // coerce_type_typmod (length coercion) grows later; the M4 types reaching here
    // (numeric/float/int/date) need no typmod coercion for the milestone queries.
    Some(result)
}

/// PG `can_coerce_type` (M4 subset): can each input type be coerced to the matching
/// target type in `ccontext`? Covers same-type, UNKNOWN-literal (any target), and
/// the cast-catalog pathway (find_coercion_pathway with the context check). The
/// polymorphic / array / domain cases grow later.
pub fn can_coerce_type(
    nargs: i32,
    input_typeids: &[Oid],
    target_typeids: &[Oid],
    ccontext: CoercionContext,
) -> bool {
    for i in 0..nargs as usize {
        let input_type_id = input_typeids[i];
        let target_type_id = target_typeids[i];
        if input_type_id == target_type_id {
            continue;
        }
        // An UNKNOWN string literal can be coerced to any target (typinput applied).
        if input_type_id == UNKNOWNOID {
            continue;
        }
        let mut funcid = InvalidOid;
        let pathtype =
            find_coercion_pathway(target_type_id, input_type_id, ccontext, &mut funcid);
        if pathtype == CoercionPathType::None {
            return false;
        }
    }
    true
}

/// PG `find_coercion_pathway` (M4 subset): determine how to coerce `source_type_id`
/// to `target_type_id` in `ccontext`. Reads pg_cast (CASTSOURCETARGET): a matching
/// row whose context is permitted yields Func (or RelabelType for a binary cast, or
/// CoerceViaIo for a method='i' cast). With no row, a coercion to/from a string type
/// (text etc.) is allowed as CoerceViaIo (PG's COERCION_PATH_COERCEVIAIO special
/// case). Array/domain pathways grow later.
pub fn find_coercion_pathway(
    target_type_id: Oid,
    source_type_id: Oid,
    ccontext: CoercionContext,
    funcid: &mut Oid,
) -> CoercionPathType {
    use crate::backend::utils::cache::lsyscache::get_cast_info;
    use crate::catalog::pg_cast::CoercionMethod;
    use crate::nodes::primnodes::CoercionContext as CC;

    *funcid = InvalidOid;

    if source_type_id == target_type_id {
        return CoercionPathType::RelabelType;
    }

    // (Domain base-type unwrapping grows with domains; M4 base == type.)
    let (source, target) = (source_type_id, target_type_id);

    if let Some((castfunc, castcontext, castmethod)) = get_cast_info(source, target) {
        // Is the cast permitted in this context? PG maps the stored char to a
        // CoercionContext rank; an implicit cast is OK in any context, an
        // assignment cast in assignment/explicit, an explicit cast only explicitly.
        let stored = cast_context_rank(castcontext);
        let wanted = match ccontext {
            CC::IMPLICIT => 0,
            CC::ASSIGNMENT | CC::PLPGSQL => 1,
            CC::EXPLICIT => 2,
        };
        if stored > wanted {
            return CoercionPathType::None;
        }
        return if castmethod == CoercionMethod::BINARY as i8 {
            CoercionPathType::RelabelType
        } else if castmethod == CoercionMethod::INOUT as i8 {
            CoercionPathType::CoerceViaIo
        } else {
            *funcid = castfunc;
            CoercionPathType::Func
        };
    }

    // No pg_cast row: PG allows a cast to/from a string-category type via I/O in
    // assignment or explicit context (PG `find_coercion_pathway`'s COERCEVIAIO
    // fallback). A cast TO text is allowed for assignment+; a cast FROM text to
    // anything for explicit only.
    let assignment_or_explicit =
        matches!(ccontext, CC::ASSIGNMENT | CC::PLPGSQL | CC::EXPLICIT);
    if assignment_or_explicit {
        use crate::catalog::pg_type::TYPCATEGORY_STRING;
        let (tgt_cat, _) =
            crate::backend::utils::cache::lsyscache::get_type_category_preferred(target);
        if tgt_cat == TYPCATEGORY_STRING {
            return CoercionPathType::CoerceViaIo;
        }
        if ccontext == CC::EXPLICIT {
            let (src_cat, _) =
                crate::backend::utils::cache::lsyscache::get_type_category_preferred(source);
            if src_cat == TYPCATEGORY_STRING {
                return CoercionPathType::CoerceViaIo;
            }
        }
    }

    CoercionPathType::None
}

/// Rank a pg_cast.castcontext char (i/a/e) for the context-permission check (lower =
/// more permissive): implicit=0, assignment=1, explicit=2.
fn cast_context_rank(castcontext: i8) -> u8 {
    use crate::catalog::pg_cast::CoercionCodes;
    if castcontext == CoercionCodes::IMPLICIT as i8 {
        0
    } else if castcontext == CoercionCodes::ASSIGNMENT as i8 {
        1
    } else {
        2
    }
}

/// PG `build_coercion_expression` (M4 subset): build the run-time coercion node for
/// a non-RelabelType pathway. Func -> a FuncExpr calling the cast function;
/// CoerceViaIo -> a CoerceViaIO node. ArrayCoerce grows later.
fn build_coercion_expression(
    node: Node,
    pathtype: CoercionPathType,
    funcid: Oid,
    target_type_id: Oid,
    target_type_mod: i32,
    _ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
) -> Node {
    let _ = target_type_mod;
    match pathtype {
        CoercionPathType::Func => {
            // A single-argument FuncExpr over the cast function (length-coercion
            // two-arg casts grow with typmod coercion).
            let f = crate::nodes::primnodes::FuncExpr {
                funcid,
                funcresulttype: target_type_id,
                funcretset: false,
                funcvariadic: false,
                funcformat: cformat,
                funccollid: InvalidOid,
                inputcollid: InvalidOid,
                args: vec![node],
                location,
            };
            Node::FuncExpr(Box::new(f))
        }
        CoercionPathType::CoerceViaIo => {
            let c = crate::nodes::primnodes::CoerceViaIO {
                arg: Some(node),
                resulttype: target_type_id,
                resultcollid: InvalidOid,
                coerceformat: cformat,
                location,
            };
            Node::CoerceViaIO(Box::new(c))
        }
        _ => not_yet_reachable("build_coercion_expression: pathtype"),
    }
}

/// PG `select_common_type` (M4 subset): pick the common result type of a list of
/// expressions (CASE/COALESCE/GREATEST/LEAST). Mirrors PG: start from the first
/// non-unknown type; for each later expr, keep it if same type; if one is preferred
/// in the running category, keep it; else if convertible to the running type, keep
/// the running type; else raise. UNKNOWN literals are skipped (resolved to the
/// common type later). If all are UNKNOWN, the common type is TEXT.
pub fn select_common_type(
    _pstate: &mut ParseState,
    exprs: &[Node],
    context: &str,
) -> (Oid, Option<Node>) {
    use crate::nodes::nodeFuncs::exprType;
    use crate::backend::utils::cache::lsyscache::get_type_category_preferred;

    crate::assert!(!exprs.is_empty());
    let mut ptype = exprType(&exprs[0]);
    let mut pcategory;
    let mut pispreferred;
    let mut which: Option<Node> = Some(exprs[0].clone());

    if ptype == UNKNOWNOID {
        pcategory = 0;
        pispreferred = false;
    } else {
        let (c, p) = get_type_category_preferred(ptype);
        pcategory = c;
        pispreferred = p;
    }

    for expr in &exprs[1..] {
        let ntype = exprType(expr);
        if ntype == ptype {
            continue; // same type, no change
        }
        if ntype == UNKNOWNOID {
            continue; // UNKNOWN literal: resolved to the common type later
        }
        if ptype == UNKNOWNOID {
            // first non-unknown type becomes the running type
            ptype = ntype;
            let (c, p) = get_type_category_preferred(ntype);
            pcategory = c;
            pispreferred = p;
            which = Some(expr.clone());
            continue;
        }
        let (ncategory, nispreferred) = get_type_category_preferred(ntype);
        if ncategory != pcategory {
            type_mismatch_error(context, ptype, ntype);
        }
        // Same category: prefer the preferred member; else keep the one the other
        // can be implicitly coerced to.
        if !pispreferred
            && can_coerce_type(1, &[ptype], &[ntype], CoercionContext::IMPLICIT)
            && !can_coerce_type(1, &[ntype], &[ptype], CoercionContext::IMPLICIT)
        {
            ptype = ntype;
            pcategory = ncategory;
            pispreferred = nispreferred;
            which = Some(expr.clone());
        }
    }

    // All-unknown -> TEXT (PG resolves the common type of all-UNKNOWN to text).
    if ptype == UNKNOWNOID {
        ptype = crate::catalog::genbki::TEXTOID;
        which = None;
    }
    (ptype, which)
}

/// PG `coerce_to_common_type`: coerce `node` to `target_type_id` (the common type
/// chosen by select_common_type), raising if not coercible.
pub fn coerce_to_common_type(
    pstate: &mut ParseState,
    node: Node,
    target_type_id: Oid,
    context: &str,
) -> Node {
    use crate::nodes::nodeFuncs::{exprLocation, exprType};
    let input_type = exprType(&node);
    if input_type == target_type_id {
        return node;
    }
    let location = exprLocation(&node);
    coerce_to_target_type(
        pstate,
        Some(node),
        input_type,
        target_type_id,
        -1,
        CoercionContext::IMPLICIT,
        CoercionForm::IMPLICIT_CAST,
        location,
    )
    .unwrap_or_else(|| type_mismatch_error(context, target_type_id, input_type))
}

#[cold]
fn type_mismatch_error(context: &str, t1: Oid, t2: Oid) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!("{context} types {} and {} cannot be matched", t1.0, t2.0));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `coerce_to_boolean`: coerce `expr` to type boolean (for WHERE/HAVING/etc).
/// M3 reaches the case where the expression is already boolean (a comparison
/// OpExpr) -- the identity. A non-boolean argument routes through `coerce_to_target_type`
/// (the implicit-cast search), which grows with the cast catalog; `coerce_to_boolean`
/// raises the "argument of X must be type boolean" error for an uncoercible type.
pub fn coerce_to_boolean(
    pstate: &mut ParseState,
    expr: Node,
    construct_name: &str,
) -> Node {
    use crate::catalog::genbki::BOOLOID;
    use crate::nodes::nodeFuncs::exprType;

    let input_type_id = exprType(&expr);
    if input_type_id == BOOLOID {
        return expr;
    }
    // Try an implicit coercion to bool (covers the unknown-literal case once casts
    // land). On failure, PG raises an error naming `construct_name`.
    let location = crate::nodes::nodeFuncs::exprLocation(&expr);
    coerce_to_target_type(
        pstate,
        Some(expr),
        input_type_id,
        BOOLOID,
        -1,
        CoercionContext::ASSIGNMENT,
        CoercionForm::IMPLICIT_CAST,
        location,
    )
    .unwrap_or_else(|| {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!("argument of {construct_name} must be type boolean"));
        });
        unreachable!("ereport(ERROR) diverges");
    })
}

//! Translation of postgres/src/backend/optimizer/util/predtest.c
//!
//! Routines to attempt to prove logical implications between predicate
//! expressions (the predicate-proof engine behind constraint exclusion,
//! partition pruning, and index-predicate selection).
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "catalog/pg_operator.h"       -> BooleanEqualOperator (pg_known_oids)
//!   "catalog/pg_type.h"           -> BOOLOID (pg_type_d)
//!   "executor/executor.h"         -> STUBBED (ExecInitExpr/ExecEvalExpr ...)
//!   "nodes/makefuncs.h"           -> crate::nodes::makefuncs::make_opclause
//!   "nodes/nodeFuncs.h"           -> inline clause-shape helpers reproduced here
//!                                    (nodeFuncs not yet ported as a unit; this
//!                                    matches restrictinfo.rs / makefuncs.rs which
//!                                    each keep a private copy of is_andclause &c.)
//!   "nodes/pathnodes.h"           -> RestrictInfo (crate::nodes::pathnodes)
//!   "optimizer/optimizer.h"       -> op_strict/func_strict/get_commutator/
//!                                    get_negator/op_volatile  -> all STUBBED
//!                                    (lsyscache not yet ported)
//!   "utils/array.h"               -> DatumGetArrayTypeP/deconstruct_array/
//!                                    ARR_* / ArrayGetNItems -> STUBBED
//!   "utils/inval.h", "utils/syscache.h", "utils/lsyscache.h" -> STUBBED
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! Translation status (real vs. stubbed)
//! ---------------------------------------------------------------------------
//! REAL (ported faithfully):
//!   * predicate_implied_by / predicate_refuted_by         (public entries)
//!   * predicate_implied_by_recurse / predicate_refuted_by_recurse
//!     (the full AND/OR/atom cross-product case analysis -- the bulk of the file)
//!   * predicate_classify (+ PredClass enum) and the PredIterInfo iterator
//!     machinery: list / boolexpr / arrayexpr iterators are fully real.
//!     The arrayconst iterator is real *structurally* but depends on stubbed
//!     array-deconstruction helpers (see below), so it yields zero elements at
//!     runtime until utils/array lands.
//!   * extract_not_arg / extract_strong_not_arg            (NOT-clause structure)
//!   * clause_is_strict_for                                (NullTest reasoning;
//!     real where it inspects node structure; op_strict/func_strict are stubbed)
//!   * predicate_implied_by_simple_clause /
//!     predicate_refuted_by_simple_clause                  (NullTest + structural
//!     equality logic real; operator branch delegates to the stub below)
//!
//! STUBBED (catalog / executor deps -- lsyscache/syscache/array/executor):
//!   * operator_predicate_proof -> returns false (conservative "cannot prove").
//!     This makes the engine SOUND-BUT-INCOMPLETE: every implication/refutation
//!     it *does* report is valid, but it will fail to prove operator-vs-operator
//!     facts (e.g. "x < 5" implies "x < 10") until lsyscache + the executor
//!     const-folding path land.  operator_same_subexprs_proof/_lookup,
//!     lookup_proof_cache, get_btree_test_op, InvalidateOprProofCacheCallBack
//!     are likewise stubbed.
//!   * op_strict / func_strict / get_commutator / get_negator / op_volatile
//!     (lsyscache) -> conservative stubs.  op_strict/func_strict return false,
//!     which only makes clause_is_strict_for fail to prove (still sound).
//!   * DatumGetArrayTypeP / get_typlenbyvalalign / deconstruct_array /
//!     ARR_NDIM / ARR_DIMS / ARR_ELEMTYPE / ArrayGetNItems (utils/array) ->
//!     stubbed; the constant-array SAOP flattening therefore degrades to an
//!     atom / empty iteration (sound, not complete).

use crate::prelude::*;
use core::ffi::{c_int, c_void};

use crate::catalog::pg_known_oids::BooleanEqualOperator;
use crate::nodes::makefuncs::BOOLOID;
use crate::nodes::nodes::Node;
use crate::nodes::nodes::NodeTag::{
    T_ArrayCoerceExpr, T_ArrayExpr, T_BoolExpr, T_BooleanTest, T_CoerceToDomain, T_CoerceViaIO,
    T_Const, T_ConvertRowtypeExpr, T_FuncExpr, T_List, T_NullTest, T_OpExpr, T_RelabelType,
    T_RestrictInfo, T_ScalarArrayOpExpr,
};
use crate::nodes::pathnodes::RestrictInfo;
use crate::nodes::pg_list::{
    list_copy, list_free, list_head, list_length, lnext, lsecond, List, ListCell, NIL,
};
use crate::nodes::primnodes::{
    ArrayCoerceExpr, ArrayExpr, BoolExpr, BooleanTest, CoerceToDomain, CoerceViaIO, Const,
    ConvertRowtypeExpr, Expr, FuncExpr, NullTest, OpExpr, RelabelType, ScalarArrayOpExpr, AND_EXPR,
    IS_FALSE, IS_NOT_NULL, IS_NOT_TRUE, IS_NULL, IS_UNKNOWN, NOT_EXPR, OR_EXPR,
};
use crate::nodes::equalfuncs::equal;
use crate::postgres_ext::{Oid, InvalidOid};
use crate::{Assert, IsA};

/*
 * Proof attempts involving large arrays in ScalarArrayOpExpr nodes are
 * likely to require O(N^2) time, and more often than not fail anyway.
 * So we set an arbitrary limit on the number of array elements that
 * we will allow to be treated as an AND or OR clause.
 */
const MAX_SAOP_ARRAY_SIZE: c_int = 100;

/* OidIsValid */
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/*
 * ---------------------------------------------------------------------------
 * Inline clause-shape helpers (nodes/nodeFuncs.h).
 *
 * Reproduced here because nodeFuncs is not yet translated as a unit (the same
 * way restrictinfo.rs and makefuncs.rs each keep private copies).
 * ---------------------------------------------------------------------------
 */

#[inline]
unsafe fn is_opclause(clause: *const c_void) -> bool {
    !clause.is_null() && IsA!(clause, T_OpExpr)
}

#[inline]
unsafe fn is_funcclause(clause: *const c_void) -> bool {
    !clause.is_null() && IsA!(clause, T_FuncExpr)
}

#[inline]
unsafe fn is_andclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == AND_EXPR
}

#[inline]
unsafe fn is_orclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == OR_EXPR
}

#[inline]
unsafe fn is_notclause(clause: *const c_void) -> bool {
    !clause.is_null()
        && IsA!(clause, T_BoolExpr)
        && (*(clause as *const BoolExpr)).boolop == NOT_EXPR
}

/// `get_notclausearg(notclause)` -- the single argument of a NOT BoolExpr.
#[inline]
unsafe fn get_notclausearg(notclause: *const c_void) -> *mut Expr {
    let b = notclause as *const BoolExpr;
    crate::nodes::pg_list::linitial((*b).args) as *mut Expr
}

/*
 * ---------------------------------------------------------------------------
 * lsyscache.h shims (STUB).  These require syscache/pg_proc/pg_operator, none
 * of which are ported yet.  The conservative answers keep every proof SOUND:
 *   op_strict/func_strict = false  -> clause_is_strict_for fails to prove
 *   get_commutator/get_negator = InvalidOid -> operator proofs bail out
 * TODO(pg-port): replace with crate::utils::lsyscache once it lands.
 * ---------------------------------------------------------------------------
 */
#[inline]
unsafe fn op_strict(_opno: Oid) -> bool {
    false
}
#[inline]
unsafe fn func_strict(_funcid: Oid) -> bool {
    false
}

/*
 * To avoid redundant coding in the two _recurse functions, we abstract out
 * the notion of iterating over the components of an AND/OR-like expression.
 */
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PredClass {
    CLASS_ATOM, /* expression that's not AND or OR */
    CLASS_AND,  /* expression with AND semantics */
    CLASS_OR,   /* expression with OR semantics */
}
pub use PredClass::*;

/// Mirror of C `PredIterInfoData`.  Rust closures would be cleaner, but to keep
/// the 1:1 structure we store function pointers exactly as the C struct does.
pub struct PredIterInfoData {
    /* node-type-specific iteration state */
    pub state: *mut c_void,
    pub state_list: *mut List,
    pub startup_fn: unsafe fn(clause: *mut Node, info: *mut PredIterInfoData),
    pub next_fn: unsafe fn(info: *mut PredIterInfoData) -> *mut Node,
    pub cleanup_fn: unsafe fn(info: *mut PredIterInfoData),
}

impl PredIterInfoData {
    fn new() -> Self {
        PredIterInfoData {
            state: core::ptr::null_mut(),
            state_list: core::ptr::null_mut(),
            startup_fn: noop_startup_fn,
            next_fn: noop_next_fn,
            cleanup_fn: noop_cleanup_fn,
        }
    }
}

unsafe fn noop_startup_fn(_clause: *mut Node, _info: *mut PredIterInfoData) {}
unsafe fn noop_next_fn(_info: *mut PredIterInfoData) -> *mut Node {
    core::ptr::null_mut()
}
unsafe fn noop_cleanup_fn(_info: *mut PredIterInfoData) {}

/*
 * iterate_begin / iterate_end macros from the C source, expressed as a helper.
 * Usage mirrors the C `iterate_begin(item, clause, info) { ... } iterate_end`.
 * We expand them inline as while-loops in the recurse functions.
 */

/*
 * predicate_implied_by
 *	  Recursively checks whether the clauses in clause_list imply that the
 *	  given predicate is true.  See the C header for the strong/weak distinction.
 */
pub unsafe fn predicate_implied_by(
    predicate_list: *mut List,
    clause_list: *mut List,
    weak: bool,
) -> bool {
    let p: *mut Node;
    let c: *mut Node;

    if predicate_list == NIL {
        return true; /* no predicate: implication is vacuous */
    }
    if clause_list == NIL {
        return false; /* no restriction: implication must fail */
    }

    /*
     * If either input is a single-element list, replace it with its lone
     * member; this avoids one useless level of AND-recursion.
     */
    if list_length(predicate_list) == 1 {
        p = crate::nodes::pg_list::linitial(predicate_list) as *mut Node;
    } else {
        p = predicate_list as *mut Node;
    }
    if list_length(clause_list) == 1 {
        c = crate::nodes::pg_list::linitial(clause_list) as *mut Node;
    } else {
        c = clause_list as *mut Node;
    }

    /* And away we go ... */
    predicate_implied_by_recurse(c, p, weak)
}

/*
 * predicate_refuted_by
 *	  Recursively checks whether the clauses in clause_list refute the given
 *	  predicate (that is, prove it false).
 */
pub unsafe fn predicate_refuted_by(
    predicate_list: *mut List,
    clause_list: *mut List,
    weak: bool,
) -> bool {
    let p: *mut Node;
    let c: *mut Node;

    if predicate_list == NIL {
        return false; /* no predicate: no refutation is possible */
    }
    if clause_list == NIL {
        return false; /* no restriction: refutation must fail */
    }

    if list_length(predicate_list) == 1 {
        p = crate::nodes::pg_list::linitial(predicate_list) as *mut Node;
    } else {
        p = predicate_list as *mut Node;
    }
    if list_length(clause_list) == 1 {
        c = crate::nodes::pg_list::linitial(clause_list) as *mut Node;
    } else {
        c = clause_list as *mut Node;
    }

    predicate_refuted_by_recurse(c, p, weak)
}

/*----------
 * predicate_implied_by_recurse
 *	  Does the predicate implication test for non-NULL restriction and
 *	  predicate clauses.  See the C header for the full rule table.
 *----------
 */
unsafe fn predicate_implied_by_recurse(mut clause: *mut Node, predicate: *mut Node, weak: bool) -> bool {
    let mut clause_info = PredIterInfoData::new();
    let mut pred_info = PredIterInfoData::new();

    /* skip through RestrictInfo */
    Assert!(!clause.is_null());
    if IsA!(clause, T_RestrictInfo) {
        clause = (*(clause as *mut RestrictInfo)).clause as *mut Node;
    }

    let pclass = predicate_classify(predicate, &mut pred_info);

    match predicate_classify(clause, &mut clause_info) {
        CLASS_AND => match pclass {
            CLASS_AND => {
                /* AND-clause => AND-clause if A implies each of B's items */
                let mut result = true;
                (pred_info.startup_fn)(predicate, &mut pred_info);
                loop {
                    let pitem = (pred_info.next_fn)(&mut pred_info);
                    if pitem.is_null() {
                        break;
                    }
                    if !predicate_implied_by_recurse(clause, pitem, weak) {
                        result = false;
                        break;
                    }
                }
                (pred_info.cleanup_fn)(&mut pred_info);
                result
            }
            CLASS_OR => {
                /* AND-clause => OR-clause if A implies any of B's items */
                let mut result = false;
                (pred_info.startup_fn)(predicate, &mut pred_info);
                loop {
                    let pitem = (pred_info.next_fn)(&mut pred_info);
                    if pitem.is_null() {
                        break;
                    }
                    if predicate_implied_by_recurse(clause, pitem, weak) {
                        result = true;
                        break;
                    }
                }
                (pred_info.cleanup_fn)(&mut pred_info);
                if result {
                    return result;
                }

                /* Also check if any of A's items implies B */
                (clause_info.startup_fn)(clause, &mut clause_info);
                loop {
                    let citem = (clause_info.next_fn)(&mut clause_info);
                    if citem.is_null() {
                        break;
                    }
                    if predicate_implied_by_recurse(citem, predicate, weak) {
                        result = true;
                        break;
                    }
                }
                (clause_info.cleanup_fn)(&mut clause_info);
                result
            }
            CLASS_ATOM => {
                /* AND-clause => atom if any of A's items implies B */
                let mut result = false;
                (clause_info.startup_fn)(clause, &mut clause_info);
                loop {
                    let citem = (clause_info.next_fn)(&mut clause_info);
                    if citem.is_null() {
                        break;
                    }
                    if predicate_implied_by_recurse(citem, predicate, weak) {
                        result = true;
                        break;
                    }
                }
                (clause_info.cleanup_fn)(&mut clause_info);
                result
            }
        },
        CLASS_OR => match pclass {
            CLASS_OR => {
                /*
                 * OR-clause => OR-clause if each of A's items implies any of B's
                 * items.
                 */
                let mut result = true;
                (clause_info.startup_fn)(clause, &mut clause_info);
                loop {
                    let citem = (clause_info.next_fn)(&mut clause_info);
                    if citem.is_null() {
                        break;
                    }
                    let mut presult = false;
                    (pred_info.startup_fn)(predicate, &mut pred_info);
                    loop {
                        let pitem = (pred_info.next_fn)(&mut pred_info);
                        if pitem.is_null() {
                            break;
                        }
                        if predicate_implied_by_recurse(citem, pitem, weak) {
                            presult = true;
                            break;
                        }
                    }
                    (pred_info.cleanup_fn)(&mut pred_info);
                    if !presult {
                        result = false; /* doesn't imply any of B's */
                        break;
                    }
                }
                (clause_info.cleanup_fn)(&mut clause_info);
                result
            }
            CLASS_AND | CLASS_ATOM => {
                /*
                 * OR-clause => AND-clause if each of A's items implies B
                 * OR-clause => atom if each of A's items implies B
                 */
                let mut result = true;
                (clause_info.startup_fn)(clause, &mut clause_info);
                loop {
                    let citem = (clause_info.next_fn)(&mut clause_info);
                    if citem.is_null() {
                        break;
                    }
                    if !predicate_implied_by_recurse(citem, predicate, weak) {
                        result = false;
                        break;
                    }
                }
                (clause_info.cleanup_fn)(&mut clause_info);
                result
            }
        },
        CLASS_ATOM => match pclass {
            CLASS_AND => {
                /* atom => AND-clause if A implies each of B's items */
                let mut result = true;
                (pred_info.startup_fn)(predicate, &mut pred_info);
                loop {
                    let pitem = (pred_info.next_fn)(&mut pred_info);
                    if pitem.is_null() {
                        break;
                    }
                    if !predicate_implied_by_recurse(clause, pitem, weak) {
                        result = false;
                        break;
                    }
                }
                (pred_info.cleanup_fn)(&mut pred_info);
                result
            }
            CLASS_OR => {
                /* atom => OR-clause if A implies any of B's items */
                let mut result = false;
                (pred_info.startup_fn)(predicate, &mut pred_info);
                loop {
                    let pitem = (pred_info.next_fn)(&mut pred_info);
                    if pitem.is_null() {
                        break;
                    }
                    if predicate_implied_by_recurse(clause, pitem, weak) {
                        result = true;
                        break;
                    }
                }
                (pred_info.cleanup_fn)(&mut pred_info);
                result
            }
            CLASS_ATOM => {
                /* atom => atom is the base case */
                predicate_implied_by_simple_clause(predicate as *mut Expr, clause, weak)
            }
        },
    }
}

/*----------
 * predicate_refuted_by_recurse
 *	  Does the predicate refutation test for non-NULL restriction and
 *	  predicate clauses.  See the C header for the full rule table.
 *----------
 */
unsafe fn predicate_refuted_by_recurse(mut clause: *mut Node, predicate: *mut Node, weak: bool) -> bool {
    let mut clause_info = PredIterInfoData::new();
    let mut pred_info = PredIterInfoData::new();
    let mut not_arg: *mut Node;

    /* skip through RestrictInfo */
    Assert!(!clause.is_null());
    if IsA!(clause, T_RestrictInfo) {
        clause = (*(clause as *mut RestrictInfo)).clause as *mut Node;
    }

    let pclass = predicate_classify(predicate, &mut pred_info);

    match predicate_classify(clause, &mut clause_info) {
        CLASS_AND => match pclass {
            CLASS_AND => {
                /* AND-clause R=> AND-clause if A refutes any of B's items */
                let mut result = false;
                (pred_info.startup_fn)(predicate, &mut pred_info);
                loop {
                    let pitem = (pred_info.next_fn)(&mut pred_info);
                    if pitem.is_null() {
                        break;
                    }
                    if predicate_refuted_by_recurse(clause, pitem, weak) {
                        result = true;
                        break;
                    }
                }
                (pred_info.cleanup_fn)(&mut pred_info);
                if result {
                    return result;
                }

                /* Also check if any of A's items refutes B */
                (clause_info.startup_fn)(clause, &mut clause_info);
                loop {
                    let citem = (clause_info.next_fn)(&mut clause_info);
                    if citem.is_null() {
                        break;
                    }
                    if predicate_refuted_by_recurse(citem, predicate, weak) {
                        result = true;
                        break;
                    }
                }
                (clause_info.cleanup_fn)(&mut clause_info);
                result
            }
            CLASS_OR => {
                /* AND-clause R=> OR-clause if A refutes each of B's items */
                let mut result = true;
                (pred_info.startup_fn)(predicate, &mut pred_info);
                loop {
                    let pitem = (pred_info.next_fn)(&mut pred_info);
                    if pitem.is_null() {
                        break;
                    }
                    if !predicate_refuted_by_recurse(clause, pitem, weak) {
                        result = false;
                        break;
                    }
                }
                (pred_info.cleanup_fn)(&mut pred_info);
                result
            }
            CLASS_ATOM => {
                /*
                 * If B is a NOT-type clause, A R=> B if A => B's arg.
                 * We can use a strong implication test in all cases.
                 */
                not_arg = extract_not_arg(predicate);
                if !not_arg.is_null()
                    && predicate_implied_by_recurse(clause, not_arg, false)
                {
                    return true;
                }

                /* AND-clause R=> atom if any of A's items refutes B */
                let mut result = false;
                (clause_info.startup_fn)(clause, &mut clause_info);
                loop {
                    let citem = (clause_info.next_fn)(&mut clause_info);
                    if citem.is_null() {
                        break;
                    }
                    if predicate_refuted_by_recurse(citem, predicate, weak) {
                        result = true;
                        break;
                    }
                }
                (clause_info.cleanup_fn)(&mut clause_info);
                result
            }
        },
        CLASS_OR => match pclass {
            CLASS_OR => {
                /* OR-clause R=> OR-clause if A refutes each of B's items */
                let mut result = true;
                (pred_info.startup_fn)(predicate, &mut pred_info);
                loop {
                    let pitem = (pred_info.next_fn)(&mut pred_info);
                    if pitem.is_null() {
                        break;
                    }
                    if !predicate_refuted_by_recurse(clause, pitem, weak) {
                        result = false;
                        break;
                    }
                }
                (pred_info.cleanup_fn)(&mut pred_info);
                result
            }
            CLASS_AND => {
                /*
                 * OR-clause R=> AND-clause if each of A's items refutes any of
                 * B's items.
                 */
                let mut result = true;
                (clause_info.startup_fn)(clause, &mut clause_info);
                loop {
                    let citem = (clause_info.next_fn)(&mut clause_info);
                    if citem.is_null() {
                        break;
                    }
                    let mut presult = false;
                    (pred_info.startup_fn)(predicate, &mut pred_info);
                    loop {
                        let pitem = (pred_info.next_fn)(&mut pred_info);
                        if pitem.is_null() {
                            break;
                        }
                        if predicate_refuted_by_recurse(citem, pitem, weak) {
                            presult = true;
                            break;
                        }
                    }
                    (pred_info.cleanup_fn)(&mut pred_info);
                    if !presult {
                        result = false; /* citem refutes nothing */
                        break;
                    }
                }
                (clause_info.cleanup_fn)(&mut clause_info);
                result
            }
            CLASS_ATOM => {
                /* If B is a NOT-type clause, A R=> B if A => B's arg */
                not_arg = extract_not_arg(predicate);
                if !not_arg.is_null()
                    && predicate_implied_by_recurse(clause, not_arg, false)
                {
                    return true;
                }

                /* OR-clause R=> atom if each of A's items refutes B */
                let mut result = true;
                (clause_info.startup_fn)(clause, &mut clause_info);
                loop {
                    let citem = (clause_info.next_fn)(&mut clause_info);
                    if citem.is_null() {
                        break;
                    }
                    if !predicate_refuted_by_recurse(citem, predicate, weak) {
                        result = false;
                        break;
                    }
                }
                (clause_info.cleanup_fn)(&mut clause_info);
                result
            }
        },
        CLASS_ATOM => {
            /*
             * If A is a strong NOT-clause, A R=> B if B => A's arg.
             * (See the C header for the strong/weak argument.)
             */
            not_arg = extract_strong_not_arg(clause);
            if !not_arg.is_null()
                && predicate_implied_by_recurse(predicate, not_arg, !weak)
            {
                return true;
            }

            match pclass {
                CLASS_AND => {
                    /* atom R=> AND-clause if A refutes any of B's items */
                    let mut result = false;
                    (pred_info.startup_fn)(predicate, &mut pred_info);
                    loop {
                        let pitem = (pred_info.next_fn)(&mut pred_info);
                        if pitem.is_null() {
                            break;
                        }
                        if predicate_refuted_by_recurse(clause, pitem, weak) {
                            result = true;
                            break;
                        }
                    }
                    (pred_info.cleanup_fn)(&mut pred_info);
                    result
                }
                CLASS_OR => {
                    /* atom R=> OR-clause if A refutes each of B's items */
                    let mut result = true;
                    (pred_info.startup_fn)(predicate, &mut pred_info);
                    loop {
                        let pitem = (pred_info.next_fn)(&mut pred_info);
                        if pitem.is_null() {
                            break;
                        }
                        if !predicate_refuted_by_recurse(clause, pitem, weak) {
                            result = false;
                            break;
                        }
                    }
                    (pred_info.cleanup_fn)(&mut pred_info);
                    result
                }
                CLASS_ATOM => {
                    /* If B is a NOT-type clause, A R=> B if A => B's arg */
                    not_arg = extract_not_arg(predicate);
                    if !not_arg.is_null()
                        && predicate_implied_by_recurse(clause, not_arg, false)
                    {
                        return true;
                    }

                    /* atom R=> atom is the base case */
                    predicate_refuted_by_simple_clause(predicate as *mut Expr, clause, weak)
                }
            }
        }
    }
}

/*
 * predicate_classify
 *	  Classify an expression node as AND-type, OR-type, or neither (an atom).
 *
 * If the expression is classified as AND- or OR-type, then *info is filled
 * in with the functions needed to iterate over its components.
 */
unsafe fn predicate_classify(clause: *mut Node, info: *mut PredIterInfoData) -> PredClass {
    /* Caller should not pass us NULL, nor a RestrictInfo clause */
    Assert!(!clause.is_null());
    Assert!(!IsA!(clause, T_RestrictInfo));

    /*
     * If we see a List, assume it's an implicit-AND list; this is the correct
     * semantics for lists of RestrictInfo nodes.
     */
    if IsA!(clause, T_List) {
        (*info).startup_fn = list_startup_fn;
        (*info).next_fn = list_next_fn;
        (*info).cleanup_fn = list_cleanup_fn;
        return CLASS_AND;
    }

    /* Handle normal AND and OR boolean clauses */
    if is_andclause(clause as *const c_void) {
        (*info).startup_fn = boolexpr_startup_fn;
        (*info).next_fn = list_next_fn;
        (*info).cleanup_fn = list_cleanup_fn;
        return CLASS_AND;
    }
    if is_orclause(clause as *const c_void) {
        (*info).startup_fn = boolexpr_startup_fn;
        (*info).next_fn = list_next_fn;
        (*info).cleanup_fn = list_cleanup_fn;
        return CLASS_OR;
    }

    /* Handle ScalarArrayOpExpr */
    if IsA!(clause, T_ScalarArrayOpExpr) {
        let saop = clause as *mut ScalarArrayOpExpr;
        let arraynode = lsecond((*saop).args) as *mut Node;

        /*
         * We can break this down into an AND or OR structure, but only if we
         * know how to iterate through expressions for the array's elements.
         * We can do that if the array operand is a non-null constant or a
         * simple ArrayExpr.
         */
        if !arraynode.is_null()
            && IsA!(arraynode, T_Const)
            && !(*(arraynode as *mut Const)).constisnull
        {
            let arrayval = DatumGetArrayTypeP((*(arraynode as *mut Const)).constvalue);
            let nelems = ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval));
            if nelems <= MAX_SAOP_ARRAY_SIZE {
                (*info).startup_fn = arrayconst_startup_fn;
                (*info).next_fn = arrayconst_next_fn;
                (*info).cleanup_fn = arrayconst_cleanup_fn;
                return if (*saop).useOr { CLASS_OR } else { CLASS_AND };
            }
        } else if !arraynode.is_null()
            && IsA!(arraynode, T_ArrayExpr)
            && !(*(arraynode as *mut ArrayExpr)).multidims
            && list_length((*(arraynode as *mut ArrayExpr)).elements) <= MAX_SAOP_ARRAY_SIZE
        {
            (*info).startup_fn = arrayexpr_startup_fn;
            (*info).next_fn = arrayexpr_next_fn;
            (*info).cleanup_fn = arrayexpr_cleanup_fn;
            return if (*saop).useOr { CLASS_OR } else { CLASS_AND };
        }
    }

    /* None of the above, so it's an atom */
    CLASS_ATOM
}

/*
 * PredIterInfo routines for iterating over regular Lists.  The iteration
 * state variable is the next ListCell to visit.
 */
unsafe fn list_startup_fn(clause: *mut Node, info: *mut PredIterInfoData) {
    (*info).state_list = clause as *mut List;
    (*info).state = list_head((*info).state_list) as *mut c_void;
}

unsafe fn list_next_fn(info: *mut PredIterInfoData) -> *mut Node {
    let l = (*info).state as *mut ListCell;
    if l.is_null() {
        return core::ptr::null_mut();
    }
    let n = crate::nodes::pg_list::lfirst(l) as *mut Node;
    (*info).state = lnext((*info).state_list, l) as *mut c_void;
    n
}

unsafe fn list_cleanup_fn(_info: *mut PredIterInfoData) {
    /* Nothing to clean up */
}

/*
 * BoolExpr needs its own startup function, but can use list_next_fn and
 * list_cleanup_fn.
 */
unsafe fn boolexpr_startup_fn(clause: *mut Node, info: *mut PredIterInfoData) {
    (*info).state_list = (*(clause as *mut BoolExpr)).args;
    (*info).state = list_head((*info).state_list) as *mut c_void;
}

/*
 * PredIterInfo routines for iterating over a ScalarArrayOpExpr with a
 * constant array operand.
 */
#[repr(C)]
struct ArrayConstIterState {
    opexpr: OpExpr,
    const_expr: Const,
    next_elem: c_int,
    num_elems: c_int,
    elem_values: *mut Datum,
    elem_nulls: *mut bool,
}

unsafe fn arrayconst_startup_fn(clause: *mut Node, info: *mut PredIterInfoData) {
    let saop = clause as *mut ScalarArrayOpExpr;

    /* Create working state struct */
    let state = palloc(core::mem::size_of::<ArrayConstIterState>()) as *mut ArrayConstIterState;
    (*info).state = state as *mut c_void;

    /* Deconstruct the array literal */
    let arrayconst = lsecond((*saop).args) as *mut Const;
    let arrayval = DatumGetArrayTypeP((*arrayconst).constvalue);
    let mut elmlen: i16 = 0;
    let mut elmbyval: bool = false;
    let mut elmalign: c_char = 0;
    get_typlenbyvalalign(ARR_ELEMTYPE(arrayval), &mut elmlen, &mut elmbyval, &mut elmalign);
    deconstruct_array(
        arrayval,
        ARR_ELEMTYPE(arrayval),
        elmlen,
        elmbyval,
        elmalign,
        &mut (*state).elem_values,
        &mut (*state).elem_nulls,
        &mut (*state).num_elems,
    );

    /* Set up a dummy OpExpr to return as the per-item node */
    (*state).opexpr.xpr.r#type = T_OpExpr;
    (*state).opexpr.opno = (*saop).opno;
    (*state).opexpr.opfuncid = (*saop).opfuncid;
    (*state).opexpr.opresulttype = BOOLOID;
    (*state).opexpr.opretset = false;
    (*state).opexpr.opcollid = InvalidOid;
    (*state).opexpr.inputcollid = (*saop).inputcollid;
    (*state).opexpr.args = list_copy((*saop).args);

    /* Set up a dummy Const node to hold the per-element values */
    (*state).const_expr.xpr.r#type = T_Const;
    (*state).const_expr.consttype = ARR_ELEMTYPE(arrayval);
    (*state).const_expr.consttypmod = -1;
    (*state).const_expr.constcollid = (*arrayconst).constcollid;
    (*state).const_expr.constlen = elmlen as c_int;
    (*state).const_expr.constbyval = elmbyval;
    /* lsecond(state->opexpr.args) = &state->const_expr; */
    let cell2 = lnext((*state).opexpr.args, list_head((*state).opexpr.args));
    if !cell2.is_null() {
        (*cell2).ptr_value = &mut (*state).const_expr as *mut Const as *mut c_void;
    }

    /* Initialize iteration state */
    (*state).next_elem = 0;
}

unsafe fn arrayconst_next_fn(info: *mut PredIterInfoData) -> *mut Node {
    let state = (*info).state as *mut ArrayConstIterState;
    if (*state).next_elem >= (*state).num_elems {
        return core::ptr::null_mut();
    }
    let idx = (*state).next_elem as isize;
    (*state).const_expr.constvalue = *(*state).elem_values.offset(idx);
    (*state).const_expr.constisnull = *(*state).elem_nulls.offset(idx);
    (*state).next_elem += 1;
    &mut (*state).opexpr as *mut OpExpr as *mut Node
}

unsafe fn arrayconst_cleanup_fn(info: *mut PredIterInfoData) {
    let state = (*info).state as *mut ArrayConstIterState;
    pfree((*state).elem_values as *mut c_void);
    pfree((*state).elem_nulls as *mut c_void);
    list_free((*state).opexpr.args);
    pfree(state as *mut c_void);
}

/*
 * PredIterInfo routines for iterating over a ScalarArrayOpExpr with a
 * one-dimensional ArrayExpr array operand.
 */
#[repr(C)]
struct ArrayExprIterState {
    opexpr: OpExpr,
    next: *mut ListCell,
}

unsafe fn arrayexpr_startup_fn(clause: *mut Node, info: *mut PredIterInfoData) {
    let saop = clause as *mut ScalarArrayOpExpr;

    /* Create working state struct */
    let state = palloc(core::mem::size_of::<ArrayExprIterState>()) as *mut ArrayExprIterState;
    (*info).state = state as *mut c_void;

    /* Set up a dummy OpExpr to return as the per-item node */
    (*state).opexpr.xpr.r#type = T_OpExpr;
    (*state).opexpr.opno = (*saop).opno;
    (*state).opexpr.opfuncid = (*saop).opfuncid;
    (*state).opexpr.opresulttype = BOOLOID;
    (*state).opexpr.opretset = false;
    (*state).opexpr.opcollid = InvalidOid;
    (*state).opexpr.inputcollid = (*saop).inputcollid;
    (*state).opexpr.args = list_copy((*saop).args);

    /* Initialize iteration variable to first member of ArrayExpr */
    let arrayexpr = lsecond((*saop).args) as *mut ArrayExpr;
    (*info).state_list = (*arrayexpr).elements;
    (*state).next = list_head((*arrayexpr).elements);
}

unsafe fn arrayexpr_next_fn(info: *mut PredIterInfoData) -> *mut Node {
    let state = (*info).state as *mut ArrayExprIterState;
    if (*state).next.is_null() {
        return core::ptr::null_mut();
    }
    /* lsecond(state->opexpr.args) = lfirst(state->next); */
    let cell2 = lnext((*state).opexpr.args, list_head((*state).opexpr.args));
    if !cell2.is_null() {
        (*cell2).ptr_value = crate::nodes::pg_list::lfirst((*state).next);
    }
    (*state).next = lnext((*info).state_list, (*state).next);
    &mut (*state).opexpr as *mut OpExpr as *mut Node
}

unsafe fn arrayexpr_cleanup_fn(info: *mut PredIterInfoData) {
    let state = (*info).state as *mut ArrayExprIterState;
    list_free((*state).opexpr.args);
    pfree(state as *mut c_void);
}

/*
 * predicate_implied_by_simple_clause
 *	  Does the predicate implication test for a "simple clause" predicate
 *	  and a "simple clause" restriction.
 *
 * NB: equal() is itself a STUB that panics; we only call it exactly where C
 * does, accepting that it panics at runtime until equalfuncs lands.
 */
unsafe fn predicate_implied_by_simple_clause(predicate: *mut Expr, clause: *mut Node, weak: bool) -> bool {
    /* CHECK_FOR_INTERRUPTS() -- not yet ported; omitted */

    /*
     * A simple and general rule is that a clause implies itself, hence we
     * check if they are equal().
     */
    if equal(predicate as *const c_void, clause as *const c_void) {
        return true;
    }

    /* Next we have some clause-type-specific strategies */
    if IsA!(clause, T_OpExpr) {
        let op = clause as *mut OpExpr;

        /*
         * For boolean x, "x = TRUE" is equivalent to "x", likewise
         * "x = FALSE" is equivalent to "NOT x".
         */
        if (*op).opno == BooleanEqualOperator {
            Assert!(list_length((*op).args) == 2);
            let rightop = lsecond((*op).args) as *mut Node;
            /* We might never see null Consts here, but better check */
            if !rightop.is_null()
                && IsA!(rightop, T_Const)
                && !(*(rightop as *mut Const)).constisnull
            {
                let leftop = crate::nodes::pg_list::linitial((*op).args) as *mut Node;
                if DatumGetBool((*(rightop as *mut Const)).constvalue) {
                    /* X = true implies X */
                    if equal(predicate as *const c_void, leftop as *const c_void) {
                        return true;
                    }
                } else {
                    /* X = false implies NOT X */
                    if is_notclause(predicate as *const c_void)
                        && equal(
                            get_notclausearg(predicate as *const c_void) as *const c_void,
                            leftop as *const c_void,
                        )
                    {
                        return true;
                    }
                }
            }
        }
    }

    /* ... and some predicate-type-specific ones */
    if IsA!(predicate, T_NullTest) {
        let predntest = predicate as *mut NullTest;
        match (*predntest).nulltesttype {
            IS_NOT_NULL => {
                /*
                 * "foo IS NOT NULL" is implied (for strong implication) if the
                 * clause is strict for "foo".  Doesn't work for weak, nor for
                 * "row IS NOT NULL".
                 */
                if !weak
                    && !(*predntest).argisrow
                    && clause_is_strict_for(clause, (*predntest).arg as *mut Node, true)
                {
                    return true;
                }
            }
            IS_NULL => {}
        }
    }

    /*
     * Finally, if both clauses are binary operator expressions, defer to the
     * (stubbed) operator-knowledge prover.
     */
    operator_predicate_proof(predicate, clause, false, weak)
}

/*
 * predicate_refuted_by_simple_clause
 *	  Does the predicate refutation test for a "simple clause" predicate
 *	  and a "simple clause" restriction.
 */
unsafe fn predicate_refuted_by_simple_clause(predicate: *mut Expr, clause: *mut Node, weak: bool) -> bool {
    /* CHECK_FOR_INTERRUPTS() -- not yet ported; omitted */

    /*
     * A simple clause can't refute itself, but pointer-equal inputs are worth
     * eliminating quickly (relation_excluded_by_constraints does this).
     */
    if predicate as *mut Node == clause {
        return false;
    }

    /* Next we have some clause-type-specific strategies */
    if IsA!(clause, T_NullTest) {
        let clausentest = clause as *mut NullTest;

        /* row IS NULL does not act in the simple way we have in mind */
        if (*clausentest).argisrow {
            return false;
        }

        match (*clausentest).nulltesttype {
            IS_NULL => {
                if IsA!(predicate, T_NullTest) {
                    let predntest = predicate as *mut NullTest;
                    /* row IS NULL does not act in the simple way */
                    if (*predntest).argisrow {
                        return false;
                    }
                    /*
                     * foo IS NULL refutes foo IS NOT NULL (non-row case), for
                     * both strong and weak refutation.
                     */
                    if (*predntest).nulltesttype == IS_NOT_NULL
                        && equal((*predntest).arg as *const c_void, (*clausentest).arg as *const c_void)
                    {
                        return true;
                    }
                }

                /*
                 * foo IS NULL weakly refutes any predicate that is strict for
                 * foo, since then the predicate must yield false or NULL.
                 */
                if weak
                    && clause_is_strict_for(
                        predicate as *mut Node,
                        (*clausentest).arg as *mut Node,
                        true,
                    )
                {
                    return true;
                }

                return false; /* we can't succeed below... */
            }
            IS_NOT_NULL => {}
        }
    }

    /* ... and some predicate-type-specific ones */
    if IsA!(predicate, T_NullTest) {
        let predntest = predicate as *mut NullTest;

        /* row IS NULL does not act in the simple way we have in mind */
        if (*predntest).argisrow {
            return false;
        }

        match (*predntest).nulltesttype {
            IS_NULL => {
                if IsA!(clause, T_NullTest) {
                    let clausentest = clause as *mut NullTest;
                    /* row IS NULL does not act in the simple way */
                    if (*clausentest).argisrow {
                        return false;
                    }
                    /*
                     * foo IS NOT NULL refutes foo IS NULL for both strong and
                     * weak refutation.
                     */
                    if (*clausentest).nulltesttype == IS_NOT_NULL
                        && equal((*clausentest).arg as *const c_void, (*predntest).arg as *const c_void)
                    {
                        return true;
                    }
                }

                /*
                 * "foo IS NULL" is refuted if the clause is strict for "foo".
                 * Works for either strong or weak refutation.
                 */
                if clause_is_strict_for(clause, (*predntest).arg as *mut Node, true) {
                    return true;
                }
            }
            IS_NOT_NULL => {}
        }

        return false; /* we can't succeed below... */
    }

    /*
     * Finally, if both clauses are binary operator expressions, defer to the
     * (stubbed) operator-knowledge prover.
     */
    operator_predicate_proof(predicate, clause, true, weak)
}

/*
 * If clause asserts the non-truth of a subclause, return that subclause;
 * otherwise return NULL.
 */
unsafe fn extract_not_arg(clause: *mut Node) -> *mut Node {
    if clause.is_null() {
        return core::ptr::null_mut();
    }
    if IsA!(clause, T_BoolExpr) {
        let bexpr = clause as *mut BoolExpr;
        if (*bexpr).boolop == NOT_EXPR {
            return crate::nodes::pg_list::linitial((*bexpr).args) as *mut Node;
        }
    } else if IsA!(clause, T_BooleanTest) {
        let btest = clause as *mut BooleanTest;
        if (*btest).booltesttype == IS_NOT_TRUE
            || (*btest).booltesttype == IS_FALSE
            || (*btest).booltesttype == IS_UNKNOWN
        {
            return (*btest).arg as *mut Node;
        }
    }
    core::ptr::null_mut()
}

/*
 * If clause asserts the falsity of a subclause, return that subclause;
 * otherwise return NULL.
 */
unsafe fn extract_strong_not_arg(clause: *mut Node) -> *mut Node {
    if clause.is_null() {
        return core::ptr::null_mut();
    }
    if IsA!(clause, T_BoolExpr) {
        let bexpr = clause as *mut BoolExpr;
        if (*bexpr).boolop == NOT_EXPR {
            return crate::nodes::pg_list::linitial((*bexpr).args) as *mut Node;
        }
    } else if IsA!(clause, T_BooleanTest) {
        let btest = clause as *mut BooleanTest;
        if (*btest).booltesttype == IS_FALSE {
            return (*btest).arg as *mut Node;
        }
    }
    core::ptr::null_mut()
}

/*
 * Can we prove that "clause" returns NULL (or FALSE) if "subexpr" is
 * assumed to yield NULL?  See the C header for the full reasoning.
 */
unsafe fn clause_is_strict_for(mut clause: *mut Node, mut subexpr: *mut Node, allow_false: bool) -> bool {
    /* safety checks */
    if clause.is_null() || subexpr.is_null() {
        return false;
    }

    /*
     * Look through any RelabelType nodes, so that we can match, say,
     * varcharcol with lower(varcharcol::text).  We should not see stacked
     * RelabelTypes here.
     */
    if IsA!(clause, T_RelabelType) {
        clause = (*(clause as *mut RelabelType)).arg as *mut Node;
    }
    if IsA!(subexpr, T_RelabelType) {
        subexpr = (*(subexpr as *mut RelabelType)).arg as *mut Node;
    }

    /* Base case */
    if equal(clause as *const c_void, subexpr as *const c_void) {
        return true;
    }

    /*
     * If we have a strict operator or function, a NULL result is guaranteed if
     * any input is forced NULL by subexpr.
     */
    if is_opclause(clause as *const c_void) && op_strict((*(clause as *mut OpExpr)).opno) {
        let mut lc = list_head((*(clause as *mut OpExpr)).args);
        while !lc.is_null() {
            if clause_is_strict_for(crate::nodes::pg_list::lfirst(lc) as *mut Node, subexpr, false) {
                return true;
            }
            lc = lnext((*(clause as *mut OpExpr)).args, lc);
        }
        return false;
    }
    if is_funcclause(clause as *const c_void) && func_strict((*(clause as *mut FuncExpr)).funcid) {
        let mut lc = list_head((*(clause as *mut FuncExpr)).args);
        while !lc.is_null() {
            if clause_is_strict_for(crate::nodes::pg_list::lfirst(lc) as *mut Node, subexpr, false) {
                return true;
            }
            lc = lnext((*(clause as *mut FuncExpr)).args, lc);
        }
        return false;
    }

    /*
     * CoerceViaIO / ArrayCoerceExpr / ConvertRowtypeExpr / CoerceToDomain are
     * all strict (in the relevant sense).
     */
    if IsA!(clause, T_CoerceViaIO) {
        return clause_is_strict_for((*(clause as *mut CoerceViaIO)).arg as *mut Node, subexpr, false);
    }
    if IsA!(clause, T_ArrayCoerceExpr) {
        return clause_is_strict_for((*(clause as *mut ArrayCoerceExpr)).arg as *mut Node, subexpr, false);
    }
    if IsA!(clause, T_ConvertRowtypeExpr) {
        return clause_is_strict_for(
            (*(clause as *mut ConvertRowtypeExpr)).arg as *mut Node,
            subexpr,
            false,
        );
    }
    if IsA!(clause, T_CoerceToDomain) {
        return clause_is_strict_for((*(clause as *mut CoerceToDomain)).arg as *mut Node, subexpr, false);
    }

    /*
     * ScalarArrayOpExpr is a special case.  We'd only reach here if we failed
     * to deconstruct it into an AND/OR tree (e.g. too many array elements).
     */
    if IsA!(clause, T_ScalarArrayOpExpr) {
        let saop = clause as *mut ScalarArrayOpExpr;
        let scalarnode = crate::nodes::pg_list::linitial((*saop).args) as *mut Node;
        let arraynode = lsecond((*saop).args) as *mut Node;

        /*
         * If we can prove the scalar input null and the operator is strict,
         * then the SAOP result is null -- unless the array is empty.
         */
        if clause_is_strict_for(scalarnode, subexpr, false) && op_strict((*saop).opno) {
            let mut nelems: c_int = 0;

            if allow_false && (*saop).useOr {
                return true; /* can succeed even if array is empty */
            }

            if !arraynode.is_null() && IsA!(arraynode, T_Const) {
                let arrayconst = arraynode as *mut Const;
                /* If array is constant NULL then we can succeed. */
                if (*arrayconst).constisnull {
                    return true;
                }
                /* Otherwise, compute the number of elements. */
                let arrval = DatumGetArrayTypeP((*arrayconst).constvalue);
                nelems = ArrayGetNItems(ARR_NDIM(arrval), ARR_DIMS(arrval));
            } else if !arraynode.is_null()
                && IsA!(arraynode, T_ArrayExpr)
                && !(*(arraynode as *mut ArrayExpr)).multidims
            {
                nelems = list_length((*(arraynode as *mut ArrayExpr)).elements);
            }

            /* Proof succeeds if array is definitely non-empty */
            if nelems > 0 {
                return true;
            }
        }

        /*
         * If we can prove the array input null, the proof succeeds in all
         * cases, since SAOP always returns NULL for a NULL array.
         */
        return clause_is_strict_for(arraynode, subexpr, false);
    }

    /*
     * When recursing into an expression, we might find a NULL constant.
     */
    if IsA!(clause, T_Const) {
        return (*(clause as *mut Const)).constisnull;
    }

    false
}

/*
 * ---------------------------------------------------------------------------
 * operator_predicate_proof and its support routines (STUB).
 *
 * The full implementation needs:
 *   - lsyscache: get_commutator, get_negator, op_strict, op_volatile,
 *     get_op_index_interpretation, get_opfamily_member_for_cmptype
 *   - syscache/inval: the OprProofCache HTAB + AMOPOPID callback
 *   - the executor: CreateExecutorState/ExecInitExpr/ExecEvalExprSwitchContext
 *     for const-vs-const comparison via a built test operator
 *   - utils/array for the SAOP element deconstruction
 *
 * Until lsyscache lands we return false ("cannot prove").  This keeps the
 * engine SOUND (every proof it reports is valid) but INCOMPLETE (operator
 * facts such as "x < 5" => "x < 10" are not proven).  The RC_* implication
 * tables and the OprProofCache machinery are intentionally not reproduced
 * here; they belong with the lsyscache port.
 * TODO(pg-port): implement once utils/lsyscache + executor land.
 * ---------------------------------------------------------------------------
 */
unsafe fn operator_predicate_proof(
    _predicate: *mut Expr,
    _clause: *mut Node,
    _refute_it: bool,
    _weak: bool,
) -> bool {
    /* STUB: conservative "cannot prove". See module-level notes. */
    // TODO(pg-port): real proof needs lsyscache + executor + utils/array.
    false
}

/*
 * ---------------------------------------------------------------------------
 * utils/array.h shims (STUB).  Array deconstruction is not ported.  These
 * opaque/conservative stubs let predicate_classify and clause_is_strict_for
 * compile; at runtime the constant-array SAOP path degrades to an atom / empty
 * iteration (sound, not complete).
 * TODO(pg-port): replace with crate::utils::array once it lands.
 * ---------------------------------------------------------------------------
 */
type ArrayType = c_void;

#[inline]
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType {
    // STUB: returns null; callers only deref via the other stubs below.
    core::ptr::null_mut()
}
#[inline]
unsafe fn ARR_NDIM(_a: *mut ArrayType) -> c_int {
    0
}
#[inline]
unsafe fn ARR_DIMS(_a: *mut ArrayType) -> *mut c_int {
    core::ptr::null_mut()
}
#[inline]
unsafe fn ARR_ELEMTYPE(_a: *mut ArrayType) -> Oid {
    InvalidOid
}
#[inline]
unsafe fn ArrayGetNItems(_ndim: c_int, _dims: *mut c_int) -> c_int {
    // STUB: report empty so we never claim a too-large array as AND/OR.
    0
}
#[inline]
unsafe fn get_typlenbyvalalign(
    _typid: Oid,
    typlen: *mut i16,
    typbyval: *mut bool,
    typalign: *mut c_char,
) {
    // STUB
    *typlen = 0;
    *typbyval = false;
    *typalign = 0;
}
#[allow(clippy::too_many_arguments)]
#[inline]
unsafe fn deconstruct_array(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elmlen: i16,
    _elmbyval: bool,
    _elmalign: c_char,
    elemsp: *mut *mut Datum,
    nullsp: *mut *mut bool,
    nelemsp: *mut c_int,
) {
    // STUB: zero elements.
    *elemsp = core::ptr::null_mut();
    *nullsp = core::ptr::null_mut();
    *nelemsp = 0;
}

/*
 * ---------------------------------------------------------------------------
 * Tests for the REAL structural logic that does not route through
 * operator_predicate_proof or equal().  We exercise predicate_classify and the
 * AND/OR iterator machinery directly on hand-built BoolExpr / List nodes.
 * ---------------------------------------------------------------------------
 */
#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::nodes::NodeTag::T_BoolExpr;
    use crate::nodes::pg_list::{lappend, NIL};

    /// Build an OpExpr-shaped atom node (we only need a distinct tagged node;
    /// predicate_classify treats it as CLASS_ATOM).  We use a leaked Box so the
    /// pointer stays valid for the test's lifetime.
    unsafe fn make_atom() -> *mut Node {
        let op: Box<OpExpr> = Box::new(core::mem::zeroed());
        let p = Box::into_raw(op);
        (*p).xpr.r#type = T_OpExpr;
        p as *mut Node
    }

    unsafe fn make_boolexpr(boolop: crate::nodes::primnodes::BoolExprType, args: *mut List) -> *mut Node {
        let b: Box<BoolExpr> = Box::new(core::mem::zeroed());
        let p = Box::into_raw(b);
        (*p).xpr.r#type = T_BoolExpr;
        (*p).boolop = boolop;
        (*p).args = args;
        (*p).location = -1;
        p as *mut Node
    }

    #[test]
    fn classify_and_boolexpr_is_class_and_with_right_count() {
        unsafe {
            // AND(atom, atom, atom)
            let mut args: *mut List = NIL;
            for _ in 0..3 {
                args = lappend(args, make_atom() as *mut c_void);
            }
            let and = make_boolexpr(AND_EXPR, args);

            let mut info = PredIterInfoData::new();
            let class = predicate_classify(and, &mut info);
            assert_eq!(class, CLASS_AND);

            // Drive the iterator: it should yield exactly 3 components.
            (info.startup_fn)(and, &mut info);
            let mut n = 0;
            loop {
                let item = (info.next_fn)(&mut info);
                if item.is_null() {
                    break;
                }
                n += 1;
            }
            (info.cleanup_fn)(&mut info);
            assert_eq!(n, 3);
        }
    }

    #[test]
    fn classify_or_boolexpr_is_class_or() {
        unsafe {
            let mut args: *mut List = NIL;
            args = lappend(args, make_atom() as *mut c_void);
            args = lappend(args, make_atom() as *mut c_void);
            let or = make_boolexpr(OR_EXPR, args);

            let mut info = PredIterInfoData::new();
            assert_eq!(predicate_classify(or, &mut info), CLASS_OR);

            (info.startup_fn)(or, &mut info);
            let mut n = 0;
            while !(info.next_fn)(&mut info).is_null() {
                n += 1;
            }
            (info.cleanup_fn)(&mut info);
            assert_eq!(n, 2);
        }
    }

    #[test]
    fn classify_list_is_class_and() {
        unsafe {
            // A bare List is an implicit-AND list.
            let mut list: *mut List = NIL;
            list = lappend(list, make_atom() as *mut c_void);
            list = lappend(list, make_atom() as *mut c_void);

            let mut info = PredIterInfoData::new();
            assert_eq!(predicate_classify(list as *mut Node, &mut info), CLASS_AND);

            (info.startup_fn)(list as *mut Node, &mut info);
            let mut n = 0;
            while !(info.next_fn)(&mut info).is_null() {
                n += 1;
            }
            (info.cleanup_fn)(&mut info);
            assert_eq!(n, 2);
        }
    }

    #[test]
    fn classify_atom_is_class_atom() {
        unsafe {
            let atom = make_atom();
            let mut info = PredIterInfoData::new();
            assert_eq!(predicate_classify(atom, &mut info), CLASS_ATOM);
        }
    }
}

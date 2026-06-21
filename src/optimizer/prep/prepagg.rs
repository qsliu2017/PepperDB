/*-------------------------------------------------------------------------
 *
 * prepagg.c
 *	  Routines to preprocess aggregate function calls
 *
 * If there are identical aggregate calls in the query, they only need to
 * be computed once.  Also, some aggregate functions can share the same
 * transition state, so that we only need to call the final function for
 * them separately.  These optimizations are independent of how the
 * aggregates are executed.
 *
 * preprocess_aggrefs() detects those cases, creates AggInfo and
 * AggTransInfo structs for each aggregate and transition state that needs
 * to be computed, and sets the 'aggno' and 'transno' fields in the Aggrefs
 * accordingly.  It also resolves polymorphic transition types, and sets
 * the 'aggtranstype' fields accordingly.
 *
 * XXX: The AggInfo and AggTransInfo structs are thrown away after
 * planning, so executor startup has to perform some of the same lookups
 * of transition functions and initial values that we do here.  One day, we
 * might want to carry that information to the Agg nodes to save the effort
 * at executor startup.  The Agg nodes are constructed much later in the
 * planning, however, so it's not trivial.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/prep/prepagg.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;
use crate::{foreach, current_cell, makeNode, IsA, castNode, list_nth_node, lfirst_node, linitial_node};


use crate::postgres_ext::Oid;
use crate::nodes::pg_list::{List, ListCell};
use crate::nodes::nodes::Node;
use std::ffi::{c_int, c_char};
use crate::c::{int16, int32};

/* -----------------
 * Resolve the transition type of all Aggrefs, and determine which Aggrefs
 * can share aggregate or transition state.
 *
 * Information about the aggregates and transition functions are collected
 * in the root->agginfos and root->aggtransinfos lists.  The 'aggtranstype',
 * 'aggno', and 'aggtransno' fields of each Aggref are filled in.
 *
 * NOTE: This modifies the Aggrefs in the input expression in-place!
 *
 * We try to optimize by detecting duplicate aggregate functions so that
 * their state and final values are re-used, rather than needlessly being
 * re-calculated independently.  We also detect aggregates that are not
 * the same, but which can share the same transition state.
 *
 * Scenarios:
 *
 * 1. Identical aggregate function calls appear in the query:
 *
 *	  SELECT SUM(x) FROM ... HAVING SUM(x) > 0
 *
 *	  Since these aggregates are identical, we only need to calculate
 *	  the value once.  Both aggregates will share the same 'aggno' value.
 *
 * 2. Two different aggregate functions appear in the query, but the
 *	  aggregates have the same arguments, transition functions and
 *	  initial values (and, presumably, different final functions):
 *
 *	  SELECT AVG(x), STDDEV(x) FROM ...
 *
 *	  In this case we must create a new AggInfo for the varying aggregate,
 *	  and we need to call the final functions separately, but we need
 *	  only run the transition function once.  (This requires that the
 *	  final functions be nondestructive of the transition state, but
 *	  that's required anyway for other reasons.)
 *
 * For either of these optimizations to be valid, all aggregate properties
 * used in the transition phase must be the same, including any modifiers
 * such as ORDER BY, DISTINCT and FILTER, and the arguments mustn't
 * contain any volatile functions.
 * -----------------
 */
pub unsafe fn preprocess_aggrefs(root: *mut PlannerInfo, clause: *mut Node) {
    let _ = preprocess_aggrefs_walker(clause, root);
}

unsafe fn preprocess_aggref(aggref: *mut Aggref, root: *mut PlannerInfo) {
    let aggTuple: HeapTuple;
    let aggform: Form_pg_aggregate;
    let aggtransfn: Oid;
    let aggfinalfn: Oid;
    let aggcombinefn: Oid;
    let aggserialfn: Oid;
    let aggdeserialfn: Oid;
    let mut aggtranstype: Oid;
    let mut aggtranstypmod: int32;
    let aggtransspace: int32;
    let shareable: bool;
    let mut aggno: c_int;
    let mut transno: c_int;
    let mut same_input_transnos: *mut List = std::ptr::null_mut();
    let mut resulttypeLen: int16 = 0;
    let mut resulttypeByVal: bool = false;
    let textInitVal: Datum;
    let initValue: Datum;
    let mut initValueIsNull: bool = false;
    let mut transtypeByVal: bool = false;
    let mut transtypeLen: int16 = 0;
    let mut inputTypes: [Oid; FUNC_MAX_ARGS] = [Oid::default(); FUNC_MAX_ARGS];
    let numArguments: c_int;

    Assert!((*aggref).agglevelsup == 0);

    /*
     * Fetch info about the aggregate from pg_aggregate.  Note it's correct to
     * ignore the moving-aggregate variant, since what we're concerned with
     * here is aggregates not window functions.
     */
    aggTuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum((*aggref).aggfnoid));
    if !HeapTupleIsValid(aggTuple) {
        elog!(ERROR, "cache lookup failed for aggregate {}", (*aggref).aggfnoid);
    }
    aggform = GETSTRUCT(aggTuple) as Form_pg_aggregate;
    aggtransfn = (*aggform).aggtransfn;
    aggfinalfn = (*aggform).aggfinalfn;
    aggcombinefn = (*aggform).aggcombinefn;
    aggserialfn = (*aggform).aggserialfn;
    aggdeserialfn = (*aggform).aggdeserialfn;
    aggtranstype = (*aggform).aggtranstype;
    aggtransspace = (*aggform).aggtransspace;

    /*
     * Resolve the possibly-polymorphic aggregate transition type.
     */

    /* extract argument types (ignoring any ORDER BY expressions) */
    numArguments = get_aggregate_argtypes(aggref, inputTypes.as_mut_ptr());

    /* resolve actual type of transition state, if polymorphic */
    aggtranstype = resolve_aggregate_transtype(
        (*aggref).aggfnoid,
        aggtranstype,
        inputTypes.as_mut_ptr(),
        numArguments,
    );
    (*aggref).aggtranstype = aggtranstype;

    /*
     * If transition state is of same type as first aggregated input, assume
     * it's the same typmod (same width) as well.  This works for cases like
     * MAX/MIN and is probably somewhat reasonable otherwise.
     */
    aggtranstypmod = -1;
    if !(*aggref).args.is_null() {
        let tle = linitial((*aggref).args) as *mut TargetEntry;

        if aggtranstype == exprType((*tle).expr as *mut Node) {
            aggtranstypmod = exprTypmod((*tle).expr as *mut Node);
        }
    }

    /*
     * If finalfn is marked read-write, we can't share transition states; but
     * it is okay to share states for AGGMODIFY_SHAREABLE aggs.
     *
     * In principle, in a partial aggregate, we could share the transition
     * state even if the final function is marked as read-write, because the
     * partial aggregate doesn't execute the final function.  But it's too
     * early to know whether we're going perform a partial aggregate.
     */
    shareable = (*aggform).aggfinalmodify != AGGMODIFY_READ_WRITE;

    /* get info about the output value's datatype */
    get_typlenbyval((*aggref).aggtype, &mut resulttypeLen, &mut resulttypeByVal);

    /* get initial value */
    textInitVal = SysCacheGetAttr(
        AGGFNOID,
        aggTuple,
        Anum_pg_aggregate_agginitval,
        &mut initValueIsNull,
    );
    if initValueIsNull {
        initValue = 0 as Datum;
    } else {
        initValue = GetAggInitVal(textInitVal, aggtranstype);
    }

    ReleaseSysCache(aggTuple);

    /*
     * 1. See if this is identical to another aggregate function call that
     * we've seen already.
     */
    aggno = find_compatible_agg(root, aggref, &mut same_input_transnos);
    if aggno != -1 {
        let agginfo = list_nth_node!(AggInfo, T_AggInfo, (*root).agginfos, aggno);

        (*agginfo).aggrefs = lappend((*agginfo).aggrefs, aggref as *mut std::ffi::c_void);
        transno = (*agginfo).transno;
    } else {
        let agginfo = makeNode!(AggInfo, T_AggInfo);

        (*agginfo).finalfn_oid = aggfinalfn;
        (*agginfo).aggrefs = list_make1(aggref as *mut std::ffi::c_void);
        (*agginfo).shareable = shareable;

        aggno = list_length((*root).agginfos);
        (*root).agginfos = lappend((*root).agginfos, agginfo as *mut std::ffi::c_void);

        /*
         * Count it, and check for cases requiring ordered input.  Note that
         * ordered-set aggs always have nonempty aggorder.  Any ordered-input
         * case also defeats partial aggregation.
         */
        if (*aggref).aggorder != NIL || (*aggref).aggdistinct != NIL {
            (*root).numOrderedAggs += 1;
            (*root).hasNonPartialAggs = true;
        }

        get_typlenbyval(aggtranstype, &mut transtypeLen, &mut transtypeByVal);

        /*
         * 2. See if this aggregate can share transition state with another
         * aggregate that we've initialized already.
         */
        transno = find_compatible_trans(
            root,
            aggref,
            shareable,
            aggtransfn,
            aggtranstype,
            transtypeLen as c_int,
            transtypeByVal,
            aggcombinefn,
            aggserialfn,
            aggdeserialfn,
            initValue,
            initValueIsNull,
            same_input_transnos,
        );
        if transno == -1 {
            let transinfo = makeNode!(AggTransInfo, T_AggTransInfo);

            (*transinfo).args = (*aggref).args;
            (*transinfo).aggfilter = (*aggref).aggfilter;
            (*transinfo).transfn_oid = aggtransfn;
            (*transinfo).combinefn_oid = aggcombinefn;
            (*transinfo).serialfn_oid = aggserialfn;
            (*transinfo).deserialfn_oid = aggdeserialfn;
            (*transinfo).aggtranstype = aggtranstype;
            (*transinfo).aggtranstypmod = aggtranstypmod;
            (*transinfo).transtypeLen = transtypeLen as c_int;
            (*transinfo).transtypeByVal = transtypeByVal;
            (*transinfo).aggtransspace = aggtransspace;
            (*transinfo).initValue = initValue;
            (*transinfo).initValueIsNull = initValueIsNull;

            transno = list_length((*root).aggtransinfos);
            (*root).aggtransinfos =
                lappend((*root).aggtransinfos, transinfo as *mut std::ffi::c_void);

            /*
             * Check whether partial aggregation is feasible, unless we
             * already found out that we can't do it.
             */
            if !(*root).hasNonPartialAggs {
                /*
                 * If there is no combine function, then partial aggregation
                 * is not possible.
                 */
                if !OidIsValid((*transinfo).combinefn_oid) {
                    (*root).hasNonPartialAggs = true;
                }
                /*
                 * If we have any aggs with transtype INTERNAL then we must
                 * check whether they have serialization/deserialization
                 * functions; if not, we can't serialize partial-aggregation
                 * results.
                 */
                else if (*transinfo).aggtranstype == INTERNALOID {
                    if !OidIsValid((*transinfo).serialfn_oid)
                        || !OidIsValid((*transinfo).deserialfn_oid)
                    {
                        (*root).hasNonSerialAggs = true;
                    }

                    /*
                     * array_agg_serialize and array_agg_deserialize make use
                     * of the aggregate non-byval input type's send and
                     * receive functions.  There's a chance that the type
                     * being aggregated has one or both of these functions
                     * missing.  In this case we must not allow the
                     * aggregate's serial and deserial functions to be used.
                     * It would be nice not to have special case this and
                     * instead provide some sort of supporting function within
                     * the aggregate to do this, but for now, that seems like
                     * overkill for this one case.
                     */
                    if ((*transinfo).serialfn_oid == F_ARRAY_AGG_SERIALIZE
                        || (*transinfo).deserialfn_oid == F_ARRAY_AGG_DESERIALIZE)
                        && !agg_args_support_sendreceive(aggref)
                    {
                        (*root).hasNonSerialAggs = true;
                    }
                }
            }
        }
        (*agginfo).transno = transno;
    }

    /*
     * Fill in the fields in the Aggref (aggtranstype was set above already)
     */
    (*aggref).aggno = aggno;
    (*aggref).aggtransno = transno;
}

unsafe fn preprocess_aggrefs_walker(node: *mut Node, root: *mut PlannerInfo) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Aggref) {
        let aggref = node as *mut Aggref;

        preprocess_aggref(aggref, root);

        /*
         * We assume that the parser checked that there are no aggregates (of
         * this level anyway) in the aggregated arguments, direct arguments,
         * or filter clause.  Hence, we need not recurse into any of them.
         */
        return false;
    }
    Assert!(!IsA!(node, T_SubLink));
    expression_tree_walker(
        node,
        preprocess_aggrefs_walker as *mut std::ffi::c_void,
        root as *mut std::ffi::c_void,
    )
}

/*
 * find_compatible_agg - search for a previously initialized per-Agg struct
 *
 * Searches the previously looked at aggregates to find one which is compatible
 * with this one, with the same input parameters.  If no compatible aggregate
 * can be found, returns -1.
 *
 * As a side-effect, this also collects a list of existing, shareable per-Trans
 * structs with matching inputs.  If no identical Aggref is found, the list is
 * passed later to find_compatible_trans, to see if we can at least reuse
 * the state value of another aggregate.
 */
unsafe fn find_compatible_agg(
    root: *mut PlannerInfo,
    newagg: *mut Aggref,
    same_input_transnos: *mut *mut List,
) -> c_int {
    let lc: *mut ListCell;
    let mut aggno: c_int;

    *same_input_transnos = NIL;

    /* we mustn't reuse the aggref if it contains volatile function calls */
    if contain_volatile_functions(newagg as *mut Node) {
        return -1;
    }

    /*
     * Search through the list of already seen aggregates.  If we find an
     * existing identical aggregate call, then we can re-use that one.  While
     * searching, we'll also collect a list of Aggrefs with the same input
     * parameters.  If no matching Aggref is found, the caller can potentially
     * still re-use the transition state of one of them.  (At this stage we
     * just compare the parsetrees; whether different aggregates share the
     * same transition function will be checked later.)
     */
    aggno = -1;
    foreach!(lc, (*root).agginfos, {
        let agginfo = lfirst_node!(AggInfo, T_AggInfo, current_cell!(lc));
        let existingRef: *mut Aggref;

        aggno += 1;

        existingRef = linitial_node!(Aggref, T_Aggref, (*agginfo).aggrefs);

        /* all of the following must be the same or it's no match */
        if (*newagg).inputcollid != (*existingRef).inputcollid
            || (*newagg).aggtranstype != (*existingRef).aggtranstype
            || (*newagg).aggstar != (*existingRef).aggstar
            || (*newagg).aggvariadic != (*existingRef).aggvariadic
            || (*newagg).aggkind != (*existingRef).aggkind
            || !equal((*newagg).args as *const _, (*existingRef).args as *const _)
            || !equal((*newagg).aggorder as *const _, (*existingRef).aggorder as *const _)
            || !equal((*newagg).aggdistinct as *const _, (*existingRef).aggdistinct as *const _)
            || !equal((*newagg).aggfilter as *const _, (*existingRef).aggfilter as *const _)
        {
            continue;
        }

        /* if it's the same aggregate function then report exact match */
        if (*newagg).aggfnoid == (*existingRef).aggfnoid
            && (*newagg).aggtype == (*existingRef).aggtype
            && (*newagg).aggcollid == (*existingRef).aggcollid
            && equal(
                (*newagg).aggdirectargs as *const _,
                (*existingRef).aggdirectargs as *const _,
            )
        {
            list_free(*same_input_transnos);
            *same_input_transnos = NIL;
            return aggno;
        }

        /*
         * Not identical, but it had the same inputs.  If the final function
         * permits sharing, return its transno to the caller, in case we can
         * re-use its per-trans state.  (If there's already sharing going on,
         * we might report a transno more than once.  find_compatible_trans is
         * cheap enough that it's not worth spending cycles to avoid that.)
         */
        if (*agginfo).shareable {
            *same_input_transnos = lappend_int(*same_input_transnos, (*agginfo).transno);
        }
    });

    -1
}

/*
 * find_compatible_trans - search for a previously initialized per-Trans
 * struct
 *
 * Searches the list of transnos for a per-Trans struct with the same
 * transition function and initial condition. (The inputs have already been
 * verified to match.)
 */
unsafe fn find_compatible_trans(
    root: *mut PlannerInfo,
    _newagg: *mut Aggref,
    shareable: bool,
    aggtransfn: Oid,
    aggtranstype: Oid,
    transtypeLen: c_int,
    transtypeByVal: bool,
    aggcombinefn: Oid,
    aggserialfn: Oid,
    aggdeserialfn: Oid,
    initValue: Datum,
    initValueIsNull: bool,
    transnos: *mut List,
) -> c_int {
    let lc: *mut ListCell;

    /* If this aggregate can't share transition states, give up */
    if !shareable {
        return -1;
    }

    foreach!(lc, transnos, {
        let transno = lfirst_int(current_cell!(lc));
        let pertrans = list_nth_node!(AggTransInfo, T_AggTransInfo, (*root).aggtransinfos, transno);

        /*
         * if the transfns or transition state types are not the same then the
         * state can't be shared.
         */
        if aggtransfn != (*pertrans).transfn_oid || aggtranstype != (*pertrans).aggtranstype {
            continue;
        }

        /*
         * The serialization and deserialization functions must match, if
         * present, as we're unable to share the trans state for aggregates
         * which will serialize or deserialize into different formats.
         * Remember that these will be InvalidOid if they're not required for
         * this agg node.
         */
        if aggserialfn != (*pertrans).serialfn_oid || aggdeserialfn != (*pertrans).deserialfn_oid {
            continue;
        }

        /*
         * Combine function must also match.  We only care about the combine
         * function with partial aggregates, but it's too early in the
         * planning to know if we will do partial aggregation, so be
         * conservative.
         */
        if aggcombinefn != (*pertrans).combinefn_oid {
            continue;
        }

        /*
         * Check that the initial condition matches, too.
         */
        if initValueIsNull && (*pertrans).initValueIsNull {
            return transno;
        }

        if !initValueIsNull
            && !(*pertrans).initValueIsNull
            && datumIsEqual(
                initValue,
                (*pertrans).initValue,
                transtypeByVal,
                transtypeLen as crate::c::Size as isize,
            )
        {
            return transno;
        }
    });
    -1
}

unsafe fn GetAggInitVal(textInitVal: Datum, transtype: Oid) -> Datum {
    let mut typinput: Oid = Oid::default();
    let mut typioparam: Oid = Oid::default();
    let strInitVal: *mut c_char;
    let initVal: Datum;

    getTypeInputInfo(transtype, &mut typinput, &mut typioparam);
    strInitVal = TextDatumGetCString(textInitVal);
    initVal = OidInputFunctionCall(typinput, strInitVal, typioparam, -1);
    pfree(strInitVal as *mut std::ffi::c_void);
    initVal
}

/*
 * get_agg_clause_costs
 *	  Process the PlannerInfo's 'aggtransinfos' and 'agginfos' lists
 *	  accumulating the cost information about them.
 *
 * 'aggsplit' tells us the expected partial-aggregation mode, which affects
 * the cost estimates.
 *
 * NOTE that the costs are ADDED to those already in *costs ... so the caller
 * is responsible for zeroing the struct initially.
 *
 * For each AggTransInfo, we add the cost of an aggregate transition using
 * either the transfn or combinefn depending on the 'aggsplit' value.  We also
 * account for the costs of any aggfilters and any serializations and
 * deserializations of the transition state and also estimate the total space
 * needed for the transition states as if each aggregate's state was stored in
 * memory concurrently (as would be done in a HashAgg plan).
 *
 * For each AggInfo in the 'agginfos' list we add the cost of running the
 * final function and the direct args, if any.
 */
pub unsafe fn get_agg_clause_costs(
    root: *mut PlannerInfo,
    aggsplit: AggSplit,
    costs: *mut AggClauseCosts,
) {
    let lc: *mut ListCell;

    foreach!(lc, (*root).aggtransinfos, {
        let transinfo = lfirst_node!(AggTransInfo, T_AggTransInfo, current_cell!(lc));

        /*
         * Add the appropriate component function execution costs to
         * appropriate totals.
         */
        if DO_AGGSPLIT_COMBINE(aggsplit) {
            /* charge for combining previously aggregated states */
            add_function_cost(
                root,
                (*transinfo).combinefn_oid,
                std::ptr::null_mut(),
                &mut (*costs).transCost,
            );
        } else {
            add_function_cost(
                root,
                (*transinfo).transfn_oid,
                std::ptr::null_mut(),
                &mut (*costs).transCost,
            );
        }
        if DO_AGGSPLIT_DESERIALIZE(aggsplit) && OidIsValid((*transinfo).deserialfn_oid) {
            add_function_cost(
                root,
                (*transinfo).deserialfn_oid,
                std::ptr::null_mut(),
                &mut (*costs).transCost,
            );
        }
        if DO_AGGSPLIT_SERIALIZE(aggsplit) && OidIsValid((*transinfo).serialfn_oid) {
            add_function_cost(
                root,
                (*transinfo).serialfn_oid,
                std::ptr::null_mut(),
                &mut (*costs).finalCost,
            );
        }

        /*
         * These costs are incurred only by the initial aggregate node, so we
         * mustn't include them again at upper levels.
         */
        if !DO_AGGSPLIT_COMBINE(aggsplit) {
            /* add the input expressions' cost to per-input-row costs */
            let mut argcosts: QualCost = std::mem::zeroed();

            cost_qual_eval_node(&mut argcosts, (*transinfo).args as *mut Node, root);
            (*costs).transCost.startup += argcosts.startup;
            (*costs).transCost.per_tuple += argcosts.per_tuple;

            /*
             * Add any filter's cost to per-input-row costs.
             *
             * XXX Ideally we should reduce input expression costs according
             * to filter selectivity, but it's not clear it's worth the
             * trouble.
             */
            if !(*transinfo).aggfilter.is_null() {
                cost_qual_eval_node(&mut argcosts, (*transinfo).aggfilter as *mut Node, root);
                (*costs).transCost.startup += argcosts.startup;
                (*costs).transCost.per_tuple += argcosts.per_tuple;
            }
        }

        /*
         * If the transition type is pass-by-value then it doesn't add
         * anything to the required size of the hashtable.  If it is
         * pass-by-reference then we have to add the estimated size of the
         * value itself, plus palloc overhead.
         */
        if !(*transinfo).transtypeByVal {
            let mut avgwidth: int32;

            /* Use average width if aggregate definition gave one */
            if (*transinfo).aggtransspace > 0 {
                avgwidth = (*transinfo).aggtransspace;
            } else if (*transinfo).transfn_oid == F_ARRAY_APPEND {
                /*
                 * If the transition function is array_append(), it'll use an
                 * expanded array as transvalue, which will occupy at least
                 * ALLOCSET_SMALL_INITSIZE and possibly more.  Use that as the
                 * estimate for lack of a better idea.
                 */
                avgwidth = ALLOCSET_SMALL_INITSIZE as int32;
            } else {
                avgwidth =
                    get_typavgwidth((*transinfo).aggtranstype, (*transinfo).aggtranstypmod);
            }

            avgwidth = MAXALIGN(avgwidth as usize) as int32;
            (*costs).transitionSpace +=
                (avgwidth as usize) + 2 * std::mem::size_of::<*mut std::ffi::c_void>();
        } else if (*transinfo).aggtranstype == INTERNALOID {
            /*
             * INTERNAL transition type is a special case: although INTERNAL
             * is pass-by-value, it's almost certainly being used as a pointer
             * to some large data structure.  The aggregate definition can
             * provide an estimate of the size.  If it doesn't, then we assume
             * ALLOCSET_DEFAULT_INITSIZE, which is a good guess if the data is
             * being kept in a private memory context, as is done by
             * array_agg() for instance.
             */
            if (*transinfo).aggtransspace > 0 {
                (*costs).transitionSpace += (*transinfo).aggtransspace as usize;
            } else {
                (*costs).transitionSpace += ALLOCSET_DEFAULT_INITSIZE;
            }
        }
    });

    foreach!(lc, (*root).agginfos, {
        let agginfo = lfirst_node!(AggInfo, T_AggInfo, current_cell!(lc));
        let aggref = linitial_node!(Aggref, T_Aggref, (*agginfo).aggrefs);

        /*
         * Add the appropriate component function execution costs to
         * appropriate totals.
         */
        if !DO_AGGSPLIT_SKIPFINAL(aggsplit) && OidIsValid((*agginfo).finalfn_oid) {
            add_function_cost(
                root,
                (*agginfo).finalfn_oid,
                std::ptr::null_mut(),
                &mut (*costs).finalCost,
            );
        }

        /*
         * If there are direct arguments, treat their evaluation cost like the
         * cost of the finalfn.
         */
        if !(*aggref).aggdirectargs.is_null() {
            let mut argcosts: QualCost = std::mem::zeroed();

            cost_qual_eval_node(&mut argcosts, (*aggref).aggdirectargs as *mut Node, root);
            (*costs).finalCost.startup += argcosts.startup;
            (*costs).finalCost.per_tuple += argcosts.per_tuple;
        }
    });
}

/* ---- local stubs for unported dependencies ---- */

#[allow(non_camel_case_types)]
pub type HeapTuple = *mut std::ffi::c_void;
#[allow(non_camel_case_types)]
pub type Form_pg_aggregate = *mut crate::catalog::pg_aggregate::FormData_pg_aggregate;

pub use crate::nodes::pathnodes::{PlannerInfo, AggInfo, AggTransInfo, QualCost, AggClauseCosts};
pub use crate::nodes::primnodes::{Aggref, TargetEntry};

#[allow(non_camel_case_types)]
pub type AggSplit = c_int;

pub const FUNC_MAX_ARGS: usize = 100;
pub const NIL: *mut List = std::ptr::null_mut();
pub const AGGFNOID: c_int = crate::utils::cache::syscache_ids_gen::AGGFNOID;
pub const Anum_pg_aggregate_agginitval: c_int = 21;
pub const AGGMODIFY_READ_WRITE: c_char = crate::catalog::pg_aggregate::AGGMODIFY_READ_WRITE;
pub const INTERNALOID: Oid = crate::catalog::pg_type_d::INTERNALOID;
pub const F_ARRAY_AGG_SERIALIZE: Oid = 6294;
pub const F_ARRAY_AGG_DESERIALIZE: Oid = 6295;
pub const F_ARRAY_APPEND: Oid = 378;
pub const ALLOCSET_SMALL_INITSIZE: usize = 1024;
pub const ALLOCSET_DEFAULT_INITSIZE: usize = 8 * 1024;

unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple {
    crate::utils::cache::syscache::SearchSysCache1(cacheId, key1) as _
}
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}
unsafe fn GETSTRUCT(tuple: HeapTuple) -> *mut std::ffi::c_void {
    crate::access::htup_details::GETSTRUCT(tuple as _) as _
}
unsafe fn ReleaseSysCache(tuple: HeapTuple) {
    crate::utils::cache::syscache::ReleaseSysCache(tuple as _)
}
unsafe fn SysCacheGetAttr(
    cacheId: c_int,
    tup: HeapTuple,
    attributeNumber: c_int,
    isNull: *mut bool,
) -> Datum {
    crate::utils::cache::syscache::SysCacheGetAttr(cacheId, tup as _, attributeNumber as _, isNull)
}
unsafe fn get_aggregate_argtypes(aggref: *mut Aggref, inputTypes: *mut Oid) -> c_int {
    crate::parser::parse_agg::get_aggregate_argtypes(aggref as _, inputTypes)
}
pub unsafe fn resolve_aggregate_transtype(
    aggfuncid: Oid,
    aggtranstype: Oid,
    inputTypes: *mut Oid,
    numArguments: c_int,
) -> Oid {
    crate::parser::parse_agg::resolve_aggregate_transtype(aggfuncid, aggtranstype, inputTypes, numArguments)
}
unsafe fn agg_args_support_sendreceive(aggref: *mut Aggref) -> bool {
    crate::parser::parse_agg::agg_args_support_sendreceive(aggref as _)
}
unsafe fn get_typlenbyval(typid: Oid, typlen: *mut int16, typbyval: *mut bool) {
    crate::utils::cache::lsyscache::get_typlenbyval(typid, typlen, typbyval)
}
unsafe fn get_typavgwidth(typid: Oid, typmod: int32) -> int32 {
    crate::utils::cache::lsyscache::get_typavgwidth(typid, typmod)
}
unsafe fn getTypeInputInfo(type_: Oid, typInput: *mut Oid, typIOParam: *mut Oid) {
    crate::utils::cache::lsyscache::getTypeInputInfo(type_, typInput, typIOParam)
}
unsafe fn exprType(expr: *mut Node) -> Oid {
    crate::nodes::nodeFuncs::exprType(expr as _)
}
unsafe fn exprTypmod(expr: *mut Node) -> int32 {
    crate::nodes::nodeFuncs::exprTypmod(expr as _)
}
unsafe fn expression_tree_walker(
    node: *mut Node,
    walker: *mut std::ffi::c_void,
    context: *mut std::ffi::c_void,
) -> bool {
    crate::nodes::nodeFuncs::expression_tree_walker(node as _, core::mem::transmute(walker), context) as _
}
unsafe fn contain_volatile_functions(clause: *mut Node) -> bool {
    crate::optimizer::util::clauses::contain_volatile_functions(clause as _)
}
unsafe fn cost_qual_eval_node(cost: *mut QualCost, qual: *mut Node, root: *mut PlannerInfo) {
    crate::optimizer::path::costsize::cost_qual_eval_node(cost as _, qual as _, root as _)
}
unsafe fn add_function_cost(
    root: *mut PlannerInfo,
    funcid: Oid,
    node: *mut Node,
    cost: *mut QualCost,
) {
    crate::optimizer::path::costsize::add_function_cost(root as _, funcid, node as _, cost as _)
}
unsafe fn TextDatumGetCString(d: Datum) -> *mut c_char {
    crate::utils::builtins::TextDatumGetCString(d)
}
unsafe fn OidInputFunctionCall(
    functionId: Oid,
    str: *mut c_char,
    typioparam: Oid,
    typmod: int32,
) -> Datum {
    crate::utils::fmgr::OidInputFunctionCall(functionId, str, typioparam, typmod)
}
unsafe fn datumIsEqual(value1: Datum, value2: Datum, typByVal: bool, typLen: isize) -> bool {
    crate::utils::adt::datum::datumIsEqual(value1, value2, typByVal, typLen as c_int)
}
unsafe fn equal(a: *const std::ffi::c_void, b: *const std::ffi::c_void) -> bool {
    crate::nodes::equalfuncs::equal(a, b)
}
unsafe fn linitial(l: *mut List) -> *mut std::ffi::c_void {
    crate::nodes::pg_list::linitial(l)
}
unsafe fn lappend(list: *mut List, datum: *mut std::ffi::c_void) -> *mut List {
    crate::nodes::list::lappend(list, datum)
}
unsafe fn lappend_int(list: *mut List, datum: c_int) -> *mut List {
    crate::nodes::list::lappend_int(list, datum)
}
unsafe fn list_make1(datum: *mut std::ffi::c_void) -> *mut List {
    crate::list_make1!(datum)
}
unsafe fn list_length(list: *mut List) -> c_int {
    crate::nodes::pg_list::list_length(list)
}
unsafe fn list_free(list: *mut List) {
    crate::nodes::list::list_free(list)
}
unsafe fn lfirst_int(lc: *mut ListCell) -> c_int {
    crate::nodes::pg_list::lfirst_int(lc)
}
unsafe fn DO_AGGSPLIT_COMBINE(aggsplit: AggSplit) -> bool {
    crate::nodes::nodes::DO_AGGSPLIT_COMBINE(core::mem::transmute(aggsplit))
}
unsafe fn DO_AGGSPLIT_SERIALIZE(aggsplit: AggSplit) -> bool {
    crate::nodes::nodes::DO_AGGSPLIT_SERIALIZE(core::mem::transmute(aggsplit))
}
unsafe fn DO_AGGSPLIT_DESERIALIZE(aggsplit: AggSplit) -> bool {
    crate::nodes::nodes::DO_AGGSPLIT_DESERIALIZE(core::mem::transmute(aggsplit))
}
unsafe fn DO_AGGSPLIT_SKIPFINAL(aggsplit: AggSplit) -> bool {
    crate::nodes::nodes::DO_AGGSPLIT_SKIPFINAL(core::mem::transmute(aggsplit))
}

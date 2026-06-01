//! ts_selfuncs.c - Selectivity estimation functions for text search operators.

use crate::prelude::*;

use crate::c::{float4, float8, int16, text};
use crate::nodes::nodes::{Node, Selectivity};
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::Const;
use crate::utils::fmgr::FunctionCallInfo;
use crate::catalog::pg_statistic::Form_pg_statistic;
use crate::catalog::pg_type_d::{TSQUERYOID, TSVECTOROID};
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::varatt::{VARSIZE_ANY_EXHDR, VARDATA_ANY, VARATT_IS_COMPRESSED, VARATT_IS_EXTERNAL};
use crate::utils::misc::stack_depth::check_stack_depth;

use crate::utils::adt::ts_type::{
    TSQuery, QueryItem, QueryOperand, DatumGetTSQuery, GETQUERY, GETOPERAND,
    QI_VAL, OP_NOT, OP_AND, OP_OR, OP_PHRASE,
};

use crate::{
    IsA, PG_GETARG_POINTER, PG_GETARG_INT32, PG_RETURN_FLOAT8,
};

/*
 * The default text search selectivity is chosen to be small enough to
 * encourage indexscans for typical table densities.  See selfuncs.h and
 * DEFAULT_EQ_SEL for details.
 */
const DEFAULT_TS_MATCH_SEL: f64 = 0.005;

/*
 * CLAMP_PROBABILITY (from selfuncs.h): force a probability estimate into [0,1].
 * selfuncs.h is not yet ported, so inline the macro here.
 */
#[inline]
fn CLAMP_PROBABILITY(p: &mut Selectivity) {
    if *p < 0.0 {
        *p = 0.0;
    } else if *p > 1.0 {
        *p = 1.0;
    }
}

/* lookup table type for binary searching through MCELEMs */
#[repr(C)]
#[derive(Clone, Copy)]
struct TextFreq {
    element: *mut text,
    frequency: float4,
}

/* type of keys for bsearch'ing through an array of TextFreqs */
#[repr(C)]
#[derive(Clone, Copy)]
struct LexemeKey {
    lexeme: *mut c_char,
    length: c_int,
}

/* tsquery_opr_selec_no_stats(query) -> tsquery_opr_selec(GETQUERY(query), GETOPERAND(query), NULL, 0, 0) */
#[inline]
unsafe fn tsquery_opr_selec_no_stats(query: TSQuery) -> Selectivity {
    tsquery_opr_selec(GETQUERY(query), GETOPERAND(query), null_mut(), 0, 0.0)
}

/*
 *	tsmatchsel -- Selectivity of "@@"
 *
 * restriction selectivity function for tsvector @@ tsquery and
 * tsquery @@ tsvector
 */
pub unsafe fn tsmatchsel(fcinfo: FunctionCallInfo) -> Datum {
    let root = PG_GETARG_POINTER!(fcinfo, 0) as *mut PlannerInfo;

    // #ifdef NOT_USED: Oid operator = PG_GETARG_OID(1);
    let args = PG_GETARG_POINTER!(fcinfo, 2) as *mut List;
    let varRelid = PG_GETARG_INT32!(fcinfo, 3);
    let mut vardata: VariableStatData = core::mem::zeroed();
    let mut other: *mut Node = null_mut();
    let mut varonleft: bool = false;
    let mut selec: Selectivity;

    /*
     * If expression is not variable = something or something = variable, then
     * punt and return a default estimate.
     */
    if !get_restriction_variable(root, args, varRelid,
                                 &mut vardata, &mut other, &mut varonleft) {
        PG_RETURN_FLOAT8!(DEFAULT_TS_MATCH_SEL);
    }

    /*
     * Can't do anything useful if the something is not a constant, either.
     */
    if !IsA!(other, T_Const) {
        ReleaseVariableStats(&mut vardata);
        PG_RETURN_FLOAT8!(DEFAULT_TS_MATCH_SEL);
    }

    /*
     * The "@@" operator is strict, so we can cope with NULL right away
     */
    if (*(other as *mut Const)).constisnull {
        ReleaseVariableStats(&mut vardata);
        PG_RETURN_FLOAT8!(0.0);
    }

    /*
     * OK, there's a Var and a Const we're dealing with here.  We need the
     * Const to be a TSQuery, else we can't do anything useful.  We have to
     * check this because the Var might be the TSQuery not the TSVector.
     *
     * Also check that the Var really is a TSVector, in case this estimator is
     * mistakenly attached to some other operator.
     */
    if (*(other as *mut Const)).consttype == TSQUERYOID &&
        vardata.vartype == TSVECTOROID {
        /* tsvector @@ tsquery or the other way around */
        selec = tsquerysel(&mut vardata, (*(other as *mut Const)).constvalue);
    } else {
        /* If we can't see the query structure, must punt */
        selec = DEFAULT_TS_MATCH_SEL;
    }

    ReleaseVariableStats(&mut vardata);

    CLAMP_PROBABILITY(&mut selec);

    PG_RETURN_FLOAT8!(selec as float8);
}

/*
 *	tsmatchjoinsel -- join selectivity of "@@"
 *
 * join selectivity function for tsvector @@ tsquery and tsquery @@ tsvector
 */
pub unsafe fn tsmatchjoinsel(_fcinfo: FunctionCallInfo) -> Datum {
    /* for the moment we just punt */
    PG_RETURN_FLOAT8!(DEFAULT_TS_MATCH_SEL);
}

/*
 * @@ selectivity for tsvector var vs tsquery constant
 */
unsafe fn tsquerysel(vardata: *mut VariableStatData, constval: Datum) -> Selectivity {
    let mut selec: Selectivity;

    /* The caller made sure the const is a TSQuery, so get it now */
    let query: TSQuery = DatumGetTSQuery(constval);

    /* Empty query matches nothing */
    if (*query).size == 0 {
        return 0.0 as Selectivity;
    }

    if HeapTupleIsValid((*vardata).statsTuple) {
        let stats: Form_pg_statistic;
        let mut sslot: AttStatsSlot = core::mem::zeroed();

        stats = GETSTRUCT((*vardata).statsTuple) as Form_pg_statistic;

        /* MCELEM will be an array of TEXT elements for a tsvector column */
        if get_attstatsslot(&mut sslot, (*vardata).statsTuple,
                            STATISTIC_KIND_MCELEM, InvalidOid,
                            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS) {
            /*
             * There is a most-common-elements slot for the tsvector Var, so
             * use that.
             */
            selec = mcelem_tsquery_selec(query, sslot.values, sslot.nvalues,
                                         sslot.numbers, sslot.nnumbers);
            free_attstatsslot(&mut sslot);
        } else {
            /* No most-common-elements info, so do without */
            selec = tsquery_opr_selec_no_stats(query);
        }

        /*
         * MCE stats count only non-null rows, so adjust for null rows.
         */
        selec *= 1.0 - (*stats).stanullfrac as f64;
    } else {
        /* No stats at all, so do without */
        selec = tsquery_opr_selec_no_stats(query);
        /* we assume no nulls here, so no stanullfrac correction */
    }

    selec
}

/*
 * Extract data from the pg_statistic arrays into useful format.
 */
unsafe fn mcelem_tsquery_selec(query: TSQuery, mcelem: *mut Datum, nmcelem: c_int,
                               numbers: *mut float4, nnumbers: c_int) -> Selectivity {
    let minfreq: float4;
    let lookup: *mut TextFreq;
    let selec: Selectivity;
    let mut i: c_int;

    /*
     * There should be two more Numbers than Values, because the last two
     * cells are taken for minimal and maximal frequency.  Punt if not.
     *
     * (Note: the MCELEM statistics slot definition allows for a third extra
     * number containing the frequency of nulls, but we're not expecting that
     * to appear for a tsvector column.)
     */
    if nnumbers != nmcelem + 2 {
        return tsquery_opr_selec_no_stats(query);
    }

    /*
     * Transpose the data into a single array so we can use bsearch().
     */
    lookup = palloc(core::mem::size_of::<TextFreq>() * nmcelem as usize) as *mut TextFreq;
    i = 0;
    while i < nmcelem {
        /*
         * The text Datums came from an array, so it cannot be compressed or
         * stored out-of-line -- it's safe to use VARSIZE_ANY*.
         */
        Assert!(!VARATT_IS_COMPRESSED(*mcelem.add(i as usize) as *const c_char)
            && !VARATT_IS_EXTERNAL(*mcelem.add(i as usize) as *const c_char));
        (*lookup.add(i as usize)).element = DatumGetPointer(*mcelem.add(i as usize)) as *mut text;
        (*lookup.add(i as usize)).frequency = *numbers.add(i as usize);
        i += 1;
    }

    /*
     * Grab the lowest frequency. compute_tsvector_stats() stored it for us in
     * the one before the last cell of the Numbers array. See ts_typanalyze.c
     */
    minfreq = *numbers.add((nnumbers - 2) as usize);

    selec = tsquery_opr_selec(GETQUERY(query), GETOPERAND(query), lookup,
                              nmcelem, minfreq);

    pfree(lookup as *mut c_void);

    selec
}

/*
 * Traverse the tsquery in preorder, calculating selectivity as:
 *
 *	 selec(left_oper) * selec(right_oper) in AND & PHRASE nodes,
 *
 *	 selec(left_oper) + selec(right_oper) -
 *		selec(left_oper) * selec(right_oper) in OR nodes,
 *
 *	 1 - select(oper) in NOT nodes
 *
 *	 histogram-based estimation in prefix VAL nodes
 *
 *	 freq[val] in exact VAL nodes, if the value is in MCELEM
 *	 min(freq[MCELEM]) / 2 in VAL nodes, if it is not
 *
 * The MCELEM array is already sorted (see ts_typanalyze.c), so we can use
 * binary search for determining freq[MCELEM].
 *
 * If we don't have stats for the tsvector, we still use this logic,
 * except we use default estimates for VAL nodes.  This case is signaled
 * by lookup == NULL.
 */
unsafe fn tsquery_opr_selec(item: *mut QueryItem, operand: *mut c_char,
                            lookup: *mut TextFreq, length: c_int, minfreq: float4) -> Selectivity {
    let mut selec: Selectivity;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    if (*item).r#type == QI_VAL {
        let oper = item as *mut QueryOperand;
        let mut key: LexemeKey = core::mem::zeroed();

        /*
         * Prepare the key for bsearch().
         */
        key.lexeme = operand.add((*oper).distance() as usize);
        key.length = (*oper).length() as c_int;

        if (*oper).prefix {
            /* Prefix match, ie the query item is lexeme:* */
            let mut matched: Selectivity;
            let mut allmces: Selectivity;
            let mut i: c_int;
            let mut n_matched: c_int;

            /*
             * Our strategy is to scan through the MCELEM list and combine the
             * frequencies of the ones that match the prefix.  We then
             * extrapolate the fraction of matching MCELEMs to the remaining
             * rows, assuming that the MCELEMs are representative of the whole
             * lexeme population in this respect.  (Compare
             * histogram_selectivity().)  Note that these are most common
             * elements not most common values, so they're not mutually
             * exclusive.  We treat occurrences as independent events.
             *
             * This is only a good plan if we have a pretty fair number of
             * MCELEMs available; we set the threshold at 100.  If no stats or
             * insufficient stats, arbitrarily use DEFAULT_TS_MATCH_SEL*4.
             */
            if lookup.is_null() || length < 100 {
                return (DEFAULT_TS_MATCH_SEL * 4.0) as Selectivity;
            }

            matched = 0.0;
            allmces = 0.0;
            n_matched = 0;
            i = 0;
            while i < length {
                let t = lookup.add(i as usize);
                let tlen = VARSIZE_ANY_EXHDR((*t).element as *const c_char) as c_int;

                if tlen >= key.length &&
                    strncmp(key.lexeme, VARDATA_ANY((*t).element as *const c_char),
                            key.length as usize) == 0 {
                    matched += (*t).frequency as f64 - matched * (*t).frequency as f64;
                    n_matched += 1;
                }
                allmces += (*t).frequency as f64 - allmces * (*t).frequency as f64;
                i += 1;
            }

            /* Clamp to ensure sanity in the face of roundoff error */
            CLAMP_PROBABILITY(&mut matched);
            CLAMP_PROBABILITY(&mut allmces);

            selec = matched + (1.0 - allmces) * (n_matched as f64 / length as f64);

            /*
             * In any case, never believe that a prefix match has selectivity
             * less than we would assign for a non-MCELEM lexeme.  This
             * preserves the property that "word:*" should be estimated to
             * match at least as many rows as "word" would be.
             */
            selec = f64::max(
                f64::min(DEFAULT_TS_MATCH_SEL, minfreq as f64 / 2.0),
                selec,
            );
        } else {
            /* Regular exact lexeme match */
            let searchres: *mut TextFreq;

            /* If no stats for the variable, use DEFAULT_TS_MATCH_SEL */
            if lookup.is_null() {
                return DEFAULT_TS_MATCH_SEL as Selectivity;
            }

            searchres = bsearch(&key as *const LexemeKey as *const c_void,
                                lookup as *const c_void, length as usize,
                                core::mem::size_of::<TextFreq>(),
                                compare_lexeme_textfreq) as *mut TextFreq;

            if !searchres.is_null() {
                /*
                 * The element is in MCELEM.  Return precise selectivity (or
                 * at least as precise as ANALYZE could find out).
                 */
                selec = (*searchres).frequency as f64;
            } else {
                /*
                 * The element is not in MCELEM.  Punt, but assume that the
                 * selectivity cannot be more than minfreq / 2.
                 */
                selec = f64::min(DEFAULT_TS_MATCH_SEL, minfreq as f64 / 2.0);
            }
        }
    } else {
        /* Current TSQuery node is an operator */
        let s1: Selectivity;
        let s2: Selectivity;

        match (*item).qoperator.oper {
            OP_NOT => {
                selec = 1.0 - tsquery_opr_selec(item.add(1), operand,
                                                lookup, length, minfreq);
            }
            OP_PHRASE | OP_AND => {
                s1 = tsquery_opr_selec(item.add(1), operand,
                                       lookup, length, minfreq);
                s2 = tsquery_opr_selec(item.add((*item).qoperator.left as usize), operand,
                                       lookup, length, minfreq);
                selec = s1 * s2;
            }
            OP_OR => {
                s1 = tsquery_opr_selec(item.add(1), operand,
                                       lookup, length, minfreq);
                s2 = tsquery_opr_selec(item.add((*item).qoperator.left as usize), operand,
                                       lookup, length, minfreq);
                selec = s1 + s2 - s1 * s2;
            }
            _ => {
                elog!(ERROR, "unrecognized operator: {}", (*item).qoperator.oper);
                #[allow(unreachable_code)]
                {
                    selec = 0.0; /* keep compiler quiet */
                }
            }
        }
    }

    /* Clamp intermediate results to stay sane despite roundoff error */
    CLAMP_PROBABILITY(&mut selec);

    selec
}

/*
 * bsearch() comparator for a lexeme (non-NULL terminated string with length)
 * and a TextFreq. Use length, then byte-for-byte comparison, because that's
 * how ANALYZE code sorted data before storing it in a statistic tuple.
 * See ts_typanalyze.c for details.
 */
unsafe extern "C" fn compare_lexeme_textfreq(e1: *const c_void, e2: *const c_void) -> c_int {
    let key = e1 as *const LexemeKey;
    let t = e2 as *const TextFreq;
    let len1: c_int;
    let len2: c_int;

    len1 = (*key).length;
    len2 = VARSIZE_ANY_EXHDR((*t).element as *const c_char) as c_int;

    /* Compare lengths first, possibly avoiding a strncmp call */
    if len1 > len2 {
        return 1;
    } else if len1 < len2 {
        return -1;
    }

    /* Fall back on byte-for-byte comparison */
    strncmp((*key).lexeme, VARDATA_ANY((*t).element as *const c_char), len1 as usize)
}

/* ------------------------------------------------------------------------
 * Stubs for not-yet-ported dependencies.
 * ------------------------------------------------------------------------ */

// "miscadmin.h" InvalidOid lives in postgres_ext via prelude already, but make
// sure STATISTIC_KIND_MCELEM / ATTSTATSSLOT_* and the selfuncs/lsyscache API
// are available.  utils/selfuncs.c, utils/lsyscache.c and catalog/pg_statistic
// slot helpers are NOT yet ported, so stub them locally.

/* catalog/pg_statistic.h: STATISTIC_KIND_MCELEM */
const STATISTIC_KIND_MCELEM: int16 = 4;

/* utils/lsyscache.h: get_attstatsslot() flags */
const ATTSTATSSLOT_VALUES: c_int = 0x01;
const ATTSTATSSLOT_NUMBERS: c_int = 0x02;

/*
 * utils/selfuncs.h: VariableStatData.  Only the fields used here are stubbed;
 * the real definition lives in utils/selfuncs.h once ported.
 */
#[repr(C)]
struct VariableStatData {
    statsTuple: HeapTuple,
    vartype: Oid,
    // ... other selfuncs.h fields elided until utils/selfuncs is ported
}

/*
 * utils/lsyscache.h: AttStatsSlot.  Only the fields used here are stubbed.
 */
#[repr(C)]
struct AttStatsSlot {
    values: *mut Datum,
    nvalues: c_int,
    numbers: *mut float4,
    nnumbers: c_int,
    // ... kind/valuetype/values_arr/numbers_arr elided until lsyscache is ported
}

/* optimizer/planmain.h / nodes/pathnodes.h: PlannerInfo (opaque here) */
#[repr(C)]
struct PlannerInfo {
    _private: [u8; 0],
}

/* utils/selfuncs.c: get_restriction_variable() */
unsafe fn get_restriction_variable(_root: *mut PlannerInfo, _args: *mut List,
                                   _varRelid: c_int, _vardata: *mut VariableStatData,
                                   _other: *mut *mut Node, _varonleft: *mut bool) -> bool {
    unimplemented!()
}

/* utils/selfuncs.h: ReleaseVariableStats() */
unsafe fn ReleaseVariableStats(_vardata: *mut VariableStatData) {
    unimplemented!()
}

/* utils/lsyscache.c: get_attstatsslot() */
unsafe fn get_attstatsslot(_sslot: *mut AttStatsSlot, _statstuple: HeapTuple,
                           _reqkind: int16, _reqop: Oid, _flags: c_int) -> bool {
    unimplemented!()
}

/* utils/lsyscache.c: free_attstatsslot() */
unsafe fn free_attstatsslot(_sslot: *mut AttStatsSlot) {
    unimplemented!()
}

/* libc bsearch / strncmp */
extern "C" {
    fn bsearch(key: *const c_void, base: *const c_void, nmemb: usize, size: usize,
               compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int) -> *mut c_void;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
}

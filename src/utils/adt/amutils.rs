//! utils/adt/amutils.c - SQL-level APIs related to index access methods.

use crate::prelude::*;

use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::builtins::{text_to_cstring, CStringGetTextDatum};
use crate::port::pgstrcasecmp::pg_strcasecmp;

use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::index::amapi::{
    GetIndexAmRoutineByAmId, IndexAmRoutine, IndexAMProperty,
};
use crate::access::index::amapi::IndexAMProperty::*;

use crate::catalog::pg_class::{
    Form_pg_class, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX,
};
use crate::catalog::pg_index::{Form_pg_index, INDOPTION_DESC, INDOPTION_NULLS_FIRST};

use crate::{PG_GETARG_OID, PG_GETARG_INT32, PG_GETARG_TEXT_PP};
use crate::{PG_RETURN_NULL, PG_RETURN_BOOL, PG_RETURN_DATUM};

// ===================================================================
//  Stubs for not-yet-translated subsystems
// ===================================================================

// utils/syscache.h - cache ids (STUB: syscache.c not yet ported).
const RELOID: c_int = 57;
const INDEXRELID: c_int = 34;

// catalog/pg_index - attribute number (STUB: catalog header not fully ported).
const Anum_pg_index_indoption: c_int = 13;

// storage/lockdefs.h - AccessShareLock (STUB).
const AccessShareLock: c_int = 1;

// utils/syscache.h - SearchSysCache1 (STUB: syscache.c not yet ported).
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!("SearchSysCache1: syscache.c not yet ported")
}

// utils/syscache.h - ReleaseSysCache (STUB).
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!("ReleaseSysCache: syscache.c not yet ported")
}

// utils/syscache.h - SysCacheGetAttrNotNull (STUB).
unsafe fn SysCacheGetAttrNotNull(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
) -> Datum {
    unimplemented!("SysCacheGetAttrNotNull: syscache.c not yet ported")
}

// access/index.h (index_open/index_close/index_can_return) - STUB:
// indexam.c / index relation API not yet ported.
type Relation = *mut c_void;

unsafe fn index_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!("index_open: indexam.c not yet ported")
}

unsafe fn index_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!("index_close: indexam.c not yet ported")
}

unsafe fn index_can_return(_indexRelation: Relation, _attno: c_int) -> bool {
    unimplemented!("index_can_return: indexam.c not yet ported")
}

// ===================================================================

/* Convert string property name to enum, for efficiency */
struct am_propname {
    name: *const c_char,
    prop: IndexAMProperty,
}

const am_propnames: &[am_propname] = &[
    am_propname { name: c"asc".as_ptr(), prop: AMPROP_ASC },
    am_propname { name: c"desc".as_ptr(), prop: AMPROP_DESC },
    am_propname { name: c"nulls_first".as_ptr(), prop: AMPROP_NULLS_FIRST },
    am_propname { name: c"nulls_last".as_ptr(), prop: AMPROP_NULLS_LAST },
    am_propname { name: c"orderable".as_ptr(), prop: AMPROP_ORDERABLE },
    am_propname { name: c"distance_orderable".as_ptr(), prop: AMPROP_DISTANCE_ORDERABLE },
    am_propname { name: c"returnable".as_ptr(), prop: AMPROP_RETURNABLE },
    am_propname { name: c"search_array".as_ptr(), prop: AMPROP_SEARCH_ARRAY },
    am_propname { name: c"search_nulls".as_ptr(), prop: AMPROP_SEARCH_NULLS },
    am_propname { name: c"clusterable".as_ptr(), prop: AMPROP_CLUSTERABLE },
    am_propname { name: c"index_scan".as_ptr(), prop: AMPROP_INDEX_SCAN },
    am_propname { name: c"bitmap_scan".as_ptr(), prop: AMPROP_BITMAP_SCAN },
    am_propname { name: c"backward_scan".as_ptr(), prop: AMPROP_BACKWARD_SCAN },
    am_propname { name: c"can_order".as_ptr(), prop: AMPROP_CAN_ORDER },
    am_propname { name: c"can_unique".as_ptr(), prop: AMPROP_CAN_UNIQUE },
    am_propname { name: c"can_multi_col".as_ptr(), prop: AMPROP_CAN_MULTI_COL },
    am_propname { name: c"can_exclude".as_ptr(), prop: AMPROP_CAN_EXCLUDE },
    am_propname { name: c"can_include".as_ptr(), prop: AMPROP_CAN_INCLUDE },
];

unsafe fn lookup_prop_name(name: *const c_char) -> IndexAMProperty {
    for i in 0..am_propnames.len() {
        if pg_strcasecmp(am_propnames[i].name, name) == 0 {
            return am_propnames[i].prop;
        }
    }

    /* We do not throw an error, so that AMs can define their own properties */
    AMPROP_UNKNOWN
}

/*
 * Common code for properties that are just bit tests of indoptions.
 *
 * tuple: the pg_index heaptuple
 * attno: identify the index column to test the indoptions of.
 * guard: if false, a boolean false result is forced (saves code in caller).
 * iopt_mask: mask for interesting indoption bit.
 * iopt_expect: value for a "true" result (should be 0 or iopt_mask).
 *
 * Returns false to indicate a NULL result (for "unknown/inapplicable"),
 * otherwise sets *res to the boolean value to return.
 */
unsafe fn test_indoption(
    tuple: HeapTuple,
    attno: c_int,
    guard: bool,
    iopt_mask: int16,
    iopt_expect: int16,
    res: *mut bool,
) -> bool {
    let datum: Datum;
    let indoption: *mut int2vector;
    let indoption_val: int16;

    if !guard {
        *res = false;
        return true;
    }

    datum = SysCacheGetAttrNotNull(INDEXRELID, tuple, Anum_pg_index_indoption);

    indoption = DatumGetPointer(datum) as *mut int2vector;
    indoption_val = *(*indoption).values.as_ptr().add((attno - 1) as usize);

    *res = (indoption_val & iopt_mask) == iopt_expect;

    true
}

/*
 * Test property of an index AM, index, or index column.
 *
 * This is common code for different SQL-level funcs, so the amoid and
 * index_oid parameters are mutually exclusive; we look up the amoid from the
 * index_oid if needed, or if no index oid is given, we're looking at AM-wide
 * properties.
 */
unsafe fn indexam_property(
    fcinfo: FunctionCallInfo,
    propname: *const c_char,
    mut amoid: Oid,
    index_oid: Oid,
    attno: c_int,
) -> Datum {
    let mut res: bool = false;
    let mut isnull: bool = false;
    let mut natts: c_int = 0;
    let prop: IndexAMProperty;
    let routine: *mut IndexAmRoutine;

    /* Try to convert property name to enum (no error if not known) */
    prop = lookup_prop_name(propname);

    /* If we have an index OID, look up the AM, and get # of columns too */
    if OidIsValid(index_oid) {
        let tuple: HeapTuple;
        let rd_rel: Form_pg_class;

        Assert!(!OidIsValid(amoid));
        tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(index_oid));
        if !HeapTupleIsValid(tuple) {
            PG_RETURN_NULL!(fcinfo);
        }
        rd_rel = GETSTRUCT(tuple) as Form_pg_class;
        if (*rd_rel).relkind != RELKIND_INDEX
            && (*rd_rel).relkind != RELKIND_PARTITIONED_INDEX
        {
            ReleaseSysCache(tuple);
            PG_RETURN_NULL!(fcinfo);
        }
        amoid = (*rd_rel).relam;
        natts = (*rd_rel).relnatts as c_int;
        ReleaseSysCache(tuple);
    }

    /*
     * At this point, either index_oid == InvalidOid or it's a valid index
     * OID. Also, after this test and the one below, either attno == 0 for
     * index-wide or AM-wide tests, or it's a valid column number in a valid
     * index.
     */
    if attno < 0 || attno > natts {
        PG_RETURN_NULL!(fcinfo);
    }

    /*
     * Get AM information.  If we don't have a valid AM OID, return NULL.
     */
    routine = GetIndexAmRoutineByAmId(amoid, true);
    if routine.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    /*
     * If there's an AM property routine, give it a chance to override the
     * generic logic.  Proceed if it returns false.
     */
    if let Some(amproperty) = (*routine).amproperty {
        if amproperty(index_oid, attno, prop, propname, &mut res, &mut isnull) {
            if isnull {
                PG_RETURN_NULL!(fcinfo);
            }
            PG_RETURN_BOOL!(res);
        }
    }

    if attno > 0 {
        let tuple: HeapTuple;
        let rd_index: Form_pg_index;
        let mut iskey: bool = true;

        /*
         * Handle column-level properties. Many of these need the pg_index row
         * (which we also need to use to check for nonkey atts) so we fetch
         * that first.
         */
        tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
        if !HeapTupleIsValid(tuple) {
            PG_RETURN_NULL!(fcinfo);
        }
        rd_index = GETSTRUCT(tuple) as Form_pg_index;

        Assert!(index_oid == (*rd_index).indexrelid);
        Assert!(attno > 0 && attno <= (*rd_index).indnatts as c_int);

        isnull = true;

        /*
         * If amcaninclude, we might be looking at an attno for a nonkey
         * column, for which we (generically) assume that most properties are
         * null.
         */
        if (*routine).amcaninclude && attno > (*rd_index).indnkeyatts as c_int {
            iskey = false;
        }

        match prop {
            AMPROP_ASC => {
                if iskey
                    && test_indoption(
                        tuple,
                        attno,
                        (*routine).amcanorder,
                        INDOPTION_DESC,
                        0,
                        &mut res,
                    )
                {
                    isnull = false;
                }
            }

            AMPROP_DESC => {
                if iskey
                    && test_indoption(
                        tuple,
                        attno,
                        (*routine).amcanorder,
                        INDOPTION_DESC,
                        INDOPTION_DESC,
                        &mut res,
                    )
                {
                    isnull = false;
                }
            }

            AMPROP_NULLS_FIRST => {
                if iskey
                    && test_indoption(
                        tuple,
                        attno,
                        (*routine).amcanorder,
                        INDOPTION_NULLS_FIRST,
                        INDOPTION_NULLS_FIRST,
                        &mut res,
                    )
                {
                    isnull = false;
                }
            }

            AMPROP_NULLS_LAST => {
                if iskey
                    && test_indoption(
                        tuple,
                        attno,
                        (*routine).amcanorder,
                        INDOPTION_NULLS_FIRST,
                        0,
                        &mut res,
                    )
                {
                    isnull = false;
                }
            }

            AMPROP_ORDERABLE => {
                /*
                 * generic assumption is that nonkey columns are not orderable
                 */
                res = if iskey { (*routine).amcanorder } else { false };
                isnull = false;
            }

            AMPROP_DISTANCE_ORDERABLE => {
                /*
                 * The conditions for whether a column is distance-orderable
                 * are really up to the AM (at time of writing, only GiST
                 * supports it at all). The planner has its own idea based on
                 * whether it finds an operator with amoppurpose 'o', but
                 * getting there from just the index column type seems like a
                 * lot of work. So instead we expect the AM to handle this in
                 * its amproperty routine. The generic result is to return
                 * false if the AM says it never supports this, or if this is
                 * a nonkey column, and null otherwise (meaning we don't
                 * know).
                 */
                if !iskey || !(*routine).amcanorderbyop {
                    res = false;
                    isnull = false;
                }
            }

            AMPROP_RETURNABLE => {
                /* note that we ignore iskey for this property */

                isnull = false;
                res = false;

                if (*routine).amcanreturn.is_some() {
                    /*
                     * If possible, the AM should handle this test in its
                     * amproperty function without opening the rel. But this
                     * is the generic fallback if it does not.
                     */
                    let indexrel: Relation = index_open(index_oid, AccessShareLock);

                    res = index_can_return(indexrel, attno);
                    index_close(indexrel, AccessShareLock);
                }
            }

            AMPROP_SEARCH_ARRAY => {
                if iskey {
                    res = (*routine).amsearcharray;
                    isnull = false;
                }
            }

            AMPROP_SEARCH_NULLS => {
                if iskey {
                    res = (*routine).amsearchnulls;
                    isnull = false;
                }
            }

            _ => {}
        }

        ReleaseSysCache(tuple);

        if !isnull {
            PG_RETURN_BOOL!(res);
        }
        PG_RETURN_NULL!(fcinfo);
    }

    if OidIsValid(index_oid) {
        /*
         * Handle index-level properties.  Currently, these only depend on the
         * AM, but that might not be true forever, so we make users name an
         * index not just an AM.
         */
        match prop {
            AMPROP_CLUSTERABLE => {
                PG_RETURN_BOOL!((*routine).amclusterable);
            }

            AMPROP_INDEX_SCAN => {
                PG_RETURN_BOOL!(if (*routine).amgettuple.is_some() { true } else { false });
            }

            AMPROP_BITMAP_SCAN => {
                PG_RETURN_BOOL!(if (*routine).amgetbitmap.is_some() { true } else { false });
            }

            AMPROP_BACKWARD_SCAN => {
                PG_RETURN_BOOL!((*routine).amcanbackward);
            }

            _ => {
                PG_RETURN_NULL!(fcinfo);
            }
        }
    }

    /*
     * Handle AM-level properties (those that control what you can say in
     * CREATE INDEX).
     */
    match prop {
        AMPROP_CAN_ORDER => {
            PG_RETURN_BOOL!((*routine).amcanorder);
        }

        AMPROP_CAN_UNIQUE => {
            PG_RETURN_BOOL!((*routine).amcanunique);
        }

        AMPROP_CAN_MULTI_COL => {
            PG_RETURN_BOOL!((*routine).amcanmulticol);
        }

        AMPROP_CAN_EXCLUDE => {
            PG_RETURN_BOOL!(if (*routine).amgettuple.is_some() { true } else { false });
        }

        AMPROP_CAN_INCLUDE => {
            PG_RETURN_BOOL!((*routine).amcaninclude);
        }

        _ => {
            PG_RETURN_NULL!(fcinfo);
        }
    }
}

/*
 * Test property of an AM specified by AM OID
 */
pub unsafe fn pg_indexam_has_property(fcinfo: FunctionCallInfo) -> Datum {
    let amoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let propname: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 1));

    indexam_property(fcinfo, propname, amoid, InvalidOid, 0)
}

/*
 * Test property of an index specified by index OID
 */
pub unsafe fn pg_index_has_property(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let propname: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 1));

    indexam_property(fcinfo, propname, InvalidOid, relid, 0)
}

/*
 * Test property of an index column specified by index OID and column number
 */
pub unsafe fn pg_index_column_has_property(fcinfo: FunctionCallInfo) -> Datum {
    let relid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let attno: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let propname: *mut c_char = text_to_cstring(PG_GETARG_TEXT_PP!(fcinfo, 2));

    /* Reject attno 0 immediately, so that attno > 0 identifies this case */
    if attno <= 0 {
        PG_RETURN_NULL!(fcinfo);
    }

    indexam_property(fcinfo, propname, InvalidOid, relid, attno)
}

/*
 * Return the name of the given phase, as used for progress reporting by the
 * given AM.
 */
pub unsafe fn pg_indexam_progress_phasename(fcinfo: FunctionCallInfo) -> Datum {
    let amoid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let phasenum: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let routine: *mut IndexAmRoutine;
    let name: *mut c_char;

    routine = GetIndexAmRoutineByAmId(amoid, true);
    if routine.is_null() || (*routine).ambuildphasename.is_none() {
        PG_RETURN_NULL!(fcinfo);
    }

    name = ((*routine).ambuildphasename.unwrap())(phasenum as int64);
    if name.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_DATUM!(CStringGetTextDatum(name));
}

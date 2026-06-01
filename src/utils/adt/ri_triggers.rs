//! ri_triggers.rs
//!   Generic trigger procedures for referential integrity constraint checks.
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/ri_triggers.c
//!
//!  Note about memory management: the private hashtables kept here live
//!  across query and transaction boundaries, in fact they live as long as
//!  the backend does.  This works because the hashtable structures
//!  themselves are allocated by dynahash.c in its permanent DynaHashCxt,
//!  and the SPI plans they point to are saved using SPI_keepplan().
//!  There is not currently any provision for throwing away a no-longer-needed
//!  plan --- consider improving this someday.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//! src/backend/utils/adt/ri_triggers.c

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::pg_config_manual::{INDEX_MAX_KEYS, NAMEDATALEN};
use crate::utils::adt::datum::datum_image_eq; // postgres.h

use std::ffi::{c_char, c_int, c_void};

use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};
use crate::utils::rel::{Relation, RelationData, RelationGetForm};
use crate::catalog::pg_class::Form_pg_class;
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::catalog::pg_constraint::Form_pg_constraint;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::nodes::pg_list::List;
use crate::nodes::bitmapset::Bitmapset;
use crate::lib::stringinfo::{
    initStringInfo, appendStringInfoString, appendStringInfoChar, appendBinaryStringInfo,
    StringInfo, StringInfoData,
};
use crate::commands::trigger::{
    TriggerData,
    TRIGGER_FIRED_AFTER, TRIGGER_FIRED_BY_INSERT, TRIGGER_FIRED_BY_UPDATE,
    TRIGGER_FIRED_BY_DELETE, TRIGGER_FIRED_FOR_ROW, CALLED_AS_TRIGGER,
};
use crate::utils::reltrigger::Trigger;
use crate::appendStringInfo;
/* ERROR comes from the prelude (re-exported from utils::elog). */

/* ----------
 * Local definitions
 * ----------
 */

const RI_MAX_NUMKEYS: usize = INDEX_MAX_KEYS;

const RI_INIT_CONSTRAINTHASHSIZE: c_long = 64;
const RI_INIT_QUERYHASHSIZE: c_long = RI_INIT_CONSTRAINTHASHSIZE * 4;

const RI_KEYS_ALL_NULL: c_int = 0;
const RI_KEYS_SOME_NULL: c_int = 1;
const RI_KEYS_NONE_NULL: c_int = 2;

/* RI query type codes */
/* these queries are executed against the PK (referenced) table: */
const RI_PLAN_CHECK_LOOKUPPK: int32 = 1;
const RI_PLAN_CHECK_LOOKUPPK_FROM_PK: int32 = 2;
const RI_PLAN_LAST_ON_PK: int32 = RI_PLAN_CHECK_LOOKUPPK_FROM_PK;
/* these queries are executed against the FK (referencing) table: */
const RI_PLAN_CASCADE_ONDELETE: int32 = 3;
const RI_PLAN_CASCADE_ONUPDATE: int32 = 4;
const RI_PLAN_NO_ACTION: int32 = 5;
/* For RESTRICT, the same plan can be used for both ON DELETE and ON UPDATE triggers. */
const RI_PLAN_RESTRICT: int32 = 6;
const RI_PLAN_SETNULL_ONDELETE: int32 = 7;
const RI_PLAN_SETNULL_ONUPDATE: int32 = 8;
const RI_PLAN_SETDEFAULT_ONDELETE: int32 = 9;
const RI_PLAN_SETDEFAULT_ONUPDATE: int32 = 10;

const MAX_QUOTED_NAME_LEN: usize = NAMEDATALEN * 2 + 3;
const MAX_QUOTED_REL_NAME_LEN: usize = MAX_QUOTED_NAME_LEN * 2;

/*
 * #define RIAttName(rel, attnum)	NameStr(*attnumAttName(rel, attnum))
 * #define RIAttType(rel, attnum)	attnumTypeId(rel, attnum)
 * #define RIAttCollation(rel, attnum) attnumCollationId(rel, attnum)
 */
#[inline]
unsafe fn RIAttName(rel: Relation, attnum: int16) -> *mut c_char {
    NameStr(attnumAttName(rel, attnum))
}
#[inline]
unsafe fn RIAttType(rel: Relation, attnum: int16) -> Oid {
    attnumTypeId(rel, attnum)
}
#[inline]
unsafe fn RIAttCollation(rel: Relation, attnum: int16) -> Oid {
    attnumCollationId(rel, attnum)
}

const RI_TRIGTYPE_INSERT: c_int = 1;
const RI_TRIGTYPE_UPDATE: c_int = 2;
const RI_TRIGTYPE_DELETE: c_int = 3;


/*
 * RI_ConstraintInfo
 *
 * Information extracted from an FK pg_constraint entry.  This is cached in
 * ri_constraint_cache.
 *
 * Note that pf/pp/ff_eq_oprs may hold the overlaps operator instead of equals
 * for the PERIOD part of a temporal foreign key.
 */
#[repr(C)]
pub struct RI_ConstraintInfo {
    pub constraint_id: Oid,         /* OID of pg_constraint entry (hash key) */
    pub valid: bool,                /* successfully initialized? */
    pub constraint_root_id: Oid,    /* OID of topmost ancestor constraint;
                                     * same as constraint_id if not inherited */
    pub oidHashValue: uint32,       /* hash value of constraint_id */
    pub rootHashValue: uint32,      /* hash value of constraint_root_id */
    pub conname: NameData,          /* name of the FK constraint */
    pub pk_relid: Oid,              /* referenced relation */
    pub fk_relid: Oid,              /* referencing relation */
    pub confupdtype: c_char,        /* foreign key's ON UPDATE action */
    pub confdeltype: c_char,        /* foreign key's ON DELETE action */
    pub ndelsetcols: c_int,         /* number of columns referenced in ON DELETE
                                     * SET clause */
    pub confdelsetcols: [int16; RI_MAX_NUMKEYS], /* attnums of cols to set on
                                                  * delete */
    pub confmatchtype: c_char,      /* foreign key's match type */
    pub hasperiod: bool,            /* if the foreign key uses PERIOD */
    pub nkeys: c_int,               /* number of key columns */
    pub pk_attnums: [int16; RI_MAX_NUMKEYS], /* attnums of referenced cols */
    pub fk_attnums: [int16; RI_MAX_NUMKEYS], /* attnums of referencing cols */
    pub pf_eq_oprs: [Oid; RI_MAX_NUMKEYS], /* equality operators (PK = FK) */
    pub pp_eq_oprs: [Oid; RI_MAX_NUMKEYS], /* equality operators (PK = PK) */
    pub ff_eq_oprs: [Oid; RI_MAX_NUMKEYS], /* equality operators (FK = FK) */
    pub period_contained_by_oper: Oid,         /* anyrange <@ anyrange */
    pub agged_period_contained_by_oper: Oid,   /* fkattr <@ range_agg(pkattr) */
    pub period_intersect_oper: Oid,            /* anyrange * anyrange */
    pub valid_link: dlist_node,     /* Link in list of valid entries */
}

/*
 * RI_QueryKey
 *
 * The key identifying a prepared SPI plan in our query hashtable
 */
#[repr(C)]
pub struct RI_QueryKey {
    pub constr_id: Oid,         /* OID of pg_constraint entry */
    pub constr_queryno: int32,  /* query type ID, see RI_PLAN_XXX above */
}

/*
 * RI_QueryHashEntry
 */
#[repr(C)]
pub struct RI_QueryHashEntry {
    pub key: RI_QueryKey,
    pub plan: SPIPlanPtr,
}

/*
 * RI_CompareKey
 *
 * The key identifying an entry showing how to compare two values
 */
#[repr(C)]
pub struct RI_CompareKey {
    pub eq_opr: Oid,    /* the equality operator to apply */
    pub typeid: Oid,    /* the data type to apply it to */
}

/*
 * RI_CompareHashEntry
 */
#[repr(C)]
pub struct RI_CompareHashEntry {
    pub key: RI_CompareKey,
    pub valid: bool,                /* successfully initialized? */
    pub eq_opr_finfo: FmgrInfo,     /* call info for equality fn */
    pub cast_func_finfo: FmgrInfo,  /* in case we must coerce input */
}


/*
 * Local data
 */
static mut ri_constraint_cache: *mut HTAB = std::ptr::null_mut();
static mut ri_query_cache: *mut HTAB = std::ptr::null_mut();
static mut ri_compare_cache: *mut HTAB = std::ptr::null_mut();
static mut ri_constraint_cache_valid_list: dclist_head = dclist_head { head: dlist_head { head: dlist_node { prev: std::ptr::null_mut(), next: std::ptr::null_mut() } }, count: 0 };


/*
 * RI_FKey_check -
 *
 * Check foreign key existence (combined for INSERT and UPDATE).
 */
unsafe fn RI_FKey_check(trigdata: *mut TriggerData) -> Datum {
    let riinfo: *const RI_ConstraintInfo;
    let fk_rel: Relation;
    let pk_rel: Relation;
    let newslot: *mut TupleTableSlot;
    let mut qkey: RI_QueryKey = std::mem::zeroed();
    let mut qplan: SPIPlanPtr;

    riinfo = ri_FetchConstraintInfo((*trigdata).tg_trigger as *mut Trigger,
                                    (*trigdata).tg_relation, false);

    if TRIGGER_FIRED_BY_UPDATE((*trigdata).tg_event) {
        newslot = (*trigdata).tg_newslot;
    } else {
        newslot = (*trigdata).tg_trigslot;
    }

    /*
     * We should not even consider checking the row if it is no longer valid,
     * since it was either deleted (so the deferred check should be skipped)
     * or updated (in which case only the latest version of the row should be
     * checked).  Test its liveness according to SnapshotSelf.  We need pin
     * and lock on the buffer to call HeapTupleSatisfiesVisibility.  Caller
     * should be holding pin, but not lock.
     */
    if !table_tuple_satisfies_snapshot((*trigdata).tg_relation, newslot, SnapshotSelf) {
        return PointerGetDatum(std::ptr::null());
    }

    /*
     * Get the relation descriptors of the FK and PK tables.
     *
     * pk_rel is opened in RowShareLock mode since that's what our eventual
     * SELECT FOR KEY SHARE will get on it.
     */
    fk_rel = (*trigdata).tg_relation;
    pk_rel = table_open((*riinfo).pk_relid, RowShareLock);

    match ri_NullCheck(RelationGetDescr(fk_rel), newslot, riinfo, false) {
        RI_KEYS_ALL_NULL => {
            /*
             * No further check needed - an all-NULL key passes every type of
             * foreign key constraint.
             */
            table_close(pk_rel, RowShareLock);
            return PointerGetDatum(std::ptr::null());
        }

        RI_KEYS_SOME_NULL => {
            /*
             * This is the only case that differs between the three kinds of
             * MATCH.
             */
            match (*riinfo).confmatchtype {
                FKCONSTR_MATCH_FULL => {
                    /*
                     * Not allowed - MATCH FULL says either all or none of the
                     * attributes can be NULLs
                     */
                    ereport!(ERROR,
                             errmsg!("insert or update on table \"{}\" violates foreign key constraint \"{}\"",
                                     std::ffi::CStr::from_ptr(RelationGetRelationName(fk_rel)).to_string_lossy(),
                                     std::ffi::CStr::from_ptr(NameStr(&raw const (*riinfo).conname as *mut NameData)).to_string_lossy()));
                    table_close(pk_rel, RowShareLock);
                    return PointerGetDatum(std::ptr::null());
                }

                FKCONSTR_MATCH_SIMPLE => {
                    /*
                     * MATCH SIMPLE - if ANY column is null, the key passes
                     * the constraint.
                     */
                    table_close(pk_rel, RowShareLock);
                    return PointerGetDatum(std::ptr::null());
                }

                _ => {}
            }
            /* fall through to RI_KEYS_NONE_NULL */
            RI_FKey_check_continue(trigdata, riinfo, fk_rel, pk_rel, newslot, &mut qkey)
        }

        RI_KEYS_NONE_NULL | _ => {
            /*
             * Have a full qualified key - continue below for all three kinds
             * of MATCH.
             */
            RI_FKey_check_continue(trigdata, riinfo, fk_rel, pk_rel, newslot, &mut qkey)
        }
    }
}

/*
 * Helper carrying the post-NULL-check body of RI_FKey_check.  In C this was
 * straight-line code after the switch; Rust's match arms require us to factor
 * out the shared tail so we don't duplicate it.
 */
unsafe fn RI_FKey_check_continue(
    _trigdata: *mut TriggerData,
    riinfo: *const RI_ConstraintInfo,
    fk_rel: Relation,
    pk_rel: Relation,
    newslot: *mut TupleTableSlot,
    qkey: *mut RI_QueryKey,
) -> Datum {
    let mut qplan: SPIPlanPtr;

    SPI_connect();

    /* Fetch or prepare a saved plan for the real check */
    ri_BuildQueryKey(qkey, riinfo, RI_PLAN_CHECK_LOOKUPPK);

    qplan = ri_FetchPreparedPlan(qkey);
    if qplan.is_null() {
        let mut querybuf: StringInfoData = std::mem::zeroed();
        let mut pkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
        let mut attname: [c_char; MAX_QUOTED_NAME_LEN] = [0; MAX_QUOTED_NAME_LEN];
        let mut paramname: [c_char; 16] = [0; 16];
        let mut querysep: *const c_char;
        let mut queryoids: [Oid; RI_MAX_NUMKEYS] = [0; RI_MAX_NUMKEYS];
        let pk_only: *const c_char;

        /* ----------
         * The query string built is
         *	SELECT 1 FROM [ONLY] <pktable> x WHERE pkatt1 = $1 [AND ...]
         *		   FOR KEY SHARE OF x
         * The type id's for the $ parameters are those of the
         * corresponding FK attributes.
         *
         * But for temporal FKs we need to make sure
         * the FK's range is completely covered.
         * So we use this query instead:
         *  SELECT 1
         *	FROM	(
         *		SELECT pkperiodatt AS r
         *		FROM   [ONLY] pktable x
         *		WHERE  pkatt1 = $1 [AND ...]
         *		AND    pkperiodatt && $n
         *		FOR KEY SHARE OF x
         *	) x1
         *  HAVING $n <@ range_agg(x1.r)
         * Note if FOR KEY SHARE ever allows GROUP BY and HAVING
         * we can make this a bit simpler.
         * ----------
         */
        initStringInfo(&mut querybuf);
        pk_only = if (*(*pk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            c"".as_ptr()
        } else {
            c"ONLY ".as_ptr()
        };
        quoteRelationName(pkrelname.as_mut_ptr(), pk_rel);
        if (*riinfo).hasperiod {
            quoteOneName(attname.as_mut_ptr(),
                         RIAttName(pk_rel, (*riinfo).pk_attnums[((*riinfo).nkeys - 1) as usize]));

            appendStringInfo!(&mut querybuf,
                              "SELECT 1 FROM (SELECT {} AS r FROM {}{} x",
                              cstr(attname.as_ptr()), cstr(pk_only), cstr(pkrelname.as_ptr()));
        } else {
            appendStringInfo!(&mut querybuf, "SELECT 1 FROM {}{} x",
                              cstr(pk_only), cstr(pkrelname.as_ptr()));
        }
        querysep = c"WHERE".as_ptr();
        let mut i = 0;
        while i < (*riinfo).nkeys {
            let pk_type: Oid = RIAttType(pk_rel, (*riinfo).pk_attnums[i as usize]);
            let fk_type: Oid = RIAttType(fk_rel, (*riinfo).fk_attnums[i as usize]);

            quoteOneName(attname.as_mut_ptr(),
                         RIAttName(pk_rel, (*riinfo).pk_attnums[i as usize]));
            sprintf_param(paramname.as_mut_ptr(), i + 1);
            ri_GenerateQual(&mut querybuf, querysep,
                            attname.as_ptr(), pk_type,
                            (*riinfo).pf_eq_oprs[i as usize],
                            paramname.as_ptr(), fk_type);
            querysep = c"AND".as_ptr();
            queryoids[i as usize] = fk_type;
            i += 1;
        }
        appendStringInfoString(&mut querybuf, c" FOR KEY SHARE OF x".as_ptr());
        if (*riinfo).hasperiod {
            let fk_type: Oid = RIAttType(fk_rel, (*riinfo).fk_attnums[((*riinfo).nkeys - 1) as usize]);

            appendStringInfoString(&mut querybuf, c") x1 HAVING ".as_ptr());
            sprintf_param(paramname.as_mut_ptr(), (*riinfo).nkeys);
            ri_GenerateQual(&mut querybuf, c"".as_ptr(),
                            paramname.as_ptr(), fk_type,
                            (*riinfo).agged_period_contained_by_oper,
                            c"pg_catalog.range_agg".as_ptr(), ANYMULTIRANGEOID);
            appendStringInfoString(&mut querybuf, c"(x1.r)".as_ptr());
        }

        /* Prepare and save the plan */
        qplan = ri_PlanCheck(querybuf.data, (*riinfo).nkeys, queryoids.as_mut_ptr(),
                             qkey, fk_rel, pk_rel);
    }

    /*
     * Now check that foreign key exists in PK table
     *
     * XXX detectNewRows must be true when a partitioned table is on the
     * referenced side.  The reason is that our snapshot must be fresh in
     * order for the hack in find_inheritance_children() to work.
     */
    ri_PerformCheck(riinfo, qkey, qplan,
                    fk_rel, pk_rel,
                    std::ptr::null_mut(), newslot,
                    false,
                    (*(*pk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE,
                    SPI_OK_SELECT);

    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }

    table_close(pk_rel, RowShareLock);

    PointerGetDatum(std::ptr::null())
}


/*
 * RI_FKey_check_ins -
 *
 * Check foreign key existence at insert event on FK table.
 */
pub unsafe fn RI_FKey_check_ins(fcinfo: FunctionCallInfo) -> Datum {
    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_check_ins".as_ptr(), RI_TRIGTYPE_INSERT);

    /* Share code with UPDATE case. */
    RI_FKey_check((*fcinfo).context as *mut TriggerData)
}


/*
 * RI_FKey_check_upd -
 *
 * Check foreign key existence at update event on FK table.
 */
pub unsafe fn RI_FKey_check_upd(fcinfo: FunctionCallInfo) -> Datum {
    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_check_upd".as_ptr(), RI_TRIGTYPE_UPDATE);

    /* Share code with INSERT case. */
    RI_FKey_check((*fcinfo).context as *mut TriggerData)
}


/*
 * ri_Check_Pk_Match
 *
 * Check to see if another PK row has been created that provides the same
 * key values as the "oldslot" that's been modified or deleted in our trigger
 * event.  Returns true if a match is found in the PK table.
 *
 * We assume the caller checked that the oldslot contains no NULL key values,
 * since otherwise a match is impossible.
 */
unsafe fn ri_Check_Pk_Match(
    pk_rel: Relation,
    fk_rel: Relation,
    oldslot: *mut TupleTableSlot,
    riinfo: *const RI_ConstraintInfo,
) -> bool {
    let mut qplan: SPIPlanPtr;
    let mut qkey: RI_QueryKey = std::mem::zeroed();
    let result: bool;

    /* Only called for non-null rows */
    Assert!(ri_NullCheck(RelationGetDescr(pk_rel), oldslot, riinfo, true) == RI_KEYS_NONE_NULL);

    SPI_connect();

    /*
     * Fetch or prepare a saved plan for checking PK table with values coming
     * from a PK row
     */
    ri_BuildQueryKey(&mut qkey, riinfo, RI_PLAN_CHECK_LOOKUPPK_FROM_PK);

    qplan = ri_FetchPreparedPlan(&mut qkey);
    if qplan.is_null() {
        let mut querybuf: StringInfoData = std::mem::zeroed();
        let mut pkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
        let mut attname: [c_char; MAX_QUOTED_NAME_LEN] = [0; MAX_QUOTED_NAME_LEN];
        let mut paramname: [c_char; 16] = [0; 16];
        let mut querysep: *const c_char;
        let pk_only: *const c_char;
        let mut queryoids: [Oid; RI_MAX_NUMKEYS] = [0; RI_MAX_NUMKEYS];

        /* ----------
         * The query string built is
         *	SELECT 1 FROM [ONLY] <pktable> x WHERE pkatt1 = $1 [AND ...]
         *		   FOR KEY SHARE OF x
         * The type id's for the $ parameters are those of the
         * PK attributes themselves.
         * (see C source for the temporal-FK variant)
         * ----------
         */
        initStringInfo(&mut querybuf);
        pk_only = if (*(*pk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            c"".as_ptr()
        } else {
            c"ONLY ".as_ptr()
        };
        quoteRelationName(pkrelname.as_mut_ptr(), pk_rel);
        if (*riinfo).hasperiod {
            quoteOneName(attname.as_mut_ptr(), RIAttName(pk_rel, (*riinfo).pk_attnums[((*riinfo).nkeys - 1) as usize]));

            appendStringInfo!(&mut querybuf,
                              "SELECT 1 FROM (SELECT {} AS r FROM {}{} x",
                              cstr(attname.as_ptr()), cstr(pk_only), cstr(pkrelname.as_ptr()));
        } else {
            appendStringInfo!(&mut querybuf, "SELECT 1 FROM {}{} x",
                              cstr(pk_only), cstr(pkrelname.as_ptr()));
        }
        querysep = c"WHERE".as_ptr();
        let mut i = 0;
        while i < (*riinfo).nkeys {
            let pk_type: Oid = RIAttType(pk_rel, (*riinfo).pk_attnums[i as usize]);

            quoteOneName(attname.as_mut_ptr(),
                         RIAttName(pk_rel, (*riinfo).pk_attnums[i as usize]));
            sprintf_param(paramname.as_mut_ptr(), i + 1);
            ri_GenerateQual(&mut querybuf, querysep,
                            attname.as_ptr(), pk_type,
                            (*riinfo).pp_eq_oprs[i as usize],
                            paramname.as_ptr(), pk_type);
            querysep = c"AND".as_ptr();
            queryoids[i as usize] = pk_type;
            i += 1;
        }
        appendStringInfoString(&mut querybuf, c" FOR KEY SHARE OF x".as_ptr());
        if (*riinfo).hasperiod {
            let fk_type: Oid = RIAttType(fk_rel, (*riinfo).fk_attnums[((*riinfo).nkeys - 1) as usize]);

            appendStringInfoString(&mut querybuf, c") x1 HAVING ".as_ptr());
            sprintf_param(paramname.as_mut_ptr(), (*riinfo).nkeys);
            ri_GenerateQual(&mut querybuf, c"".as_ptr(),
                            paramname.as_ptr(), fk_type,
                            (*riinfo).agged_period_contained_by_oper,
                            c"pg_catalog.range_agg".as_ptr(), ANYMULTIRANGEOID);
            appendStringInfoString(&mut querybuf, c"(x1.r)".as_ptr());
        }

        /* Prepare and save the plan */
        qplan = ri_PlanCheck(querybuf.data, (*riinfo).nkeys, queryoids.as_mut_ptr(),
                             &mut qkey, fk_rel, pk_rel);
    }

    /*
     * We have a plan now. Run it.
     */
    result = ri_PerformCheck(riinfo, &mut qkey, qplan,
                             fk_rel, pk_rel,
                             oldslot, std::ptr::null_mut(),
                             false,
                             true,  /* treat like update */
                             SPI_OK_SELECT);

    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }

    result
}


/*
 * RI_FKey_noaction_del -
 *
 * Give an error and roll back the current transaction if the
 * delete has resulted in a violation of the given referential
 * integrity constraint.
 */
pub unsafe fn RI_FKey_noaction_del(fcinfo: FunctionCallInfo) -> Datum {
    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_noaction_del".as_ptr(), RI_TRIGTYPE_DELETE);

    /* Share code with RESTRICT/UPDATE cases. */
    ri_restrict((*fcinfo).context as *mut TriggerData, true)
}

/*
 * RI_FKey_restrict_del -
 *
 * Restrict delete from PK table to rows unreferenced by foreign key.
 *
 * The SQL standard intends that this referential action occur exactly when
 * the delete is performed, rather than after.  This appears to be
 * the only difference between "NO ACTION" and "RESTRICT".  In Postgres
 * we still implement this as an AFTER trigger, but it's non-deferrable.
 */
pub unsafe fn RI_FKey_restrict_del(fcinfo: FunctionCallInfo) -> Datum {
    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_restrict_del".as_ptr(), RI_TRIGTYPE_DELETE);

    /* Share code with NO ACTION/UPDATE cases. */
    ri_restrict((*fcinfo).context as *mut TriggerData, false)
}

/*
 * RI_FKey_noaction_upd -
 *
 * Give an error and roll back the current transaction if the
 * update has resulted in a violation of the given referential
 * integrity constraint.
 */
pub unsafe fn RI_FKey_noaction_upd(fcinfo: FunctionCallInfo) -> Datum {
    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_noaction_upd".as_ptr(), RI_TRIGTYPE_UPDATE);

    /* Share code with RESTRICT/DELETE cases. */
    ri_restrict((*fcinfo).context as *mut TriggerData, true)
}

/*
 * RI_FKey_restrict_upd -
 *
 * Restrict update of PK to rows unreferenced by foreign key.
 *
 * The SQL standard intends that this referential action occur exactly when
 * the update is performed, rather than after.  This appears to be
 * the only difference between "NO ACTION" and "RESTRICT".  In Postgres
 * we still implement this as an AFTER trigger, but it's non-deferrable.
 */
pub unsafe fn RI_FKey_restrict_upd(fcinfo: FunctionCallInfo) -> Datum {
    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_restrict_upd".as_ptr(), RI_TRIGTYPE_UPDATE);

    /* Share code with NO ACTION/DELETE cases. */
    ri_restrict((*fcinfo).context as *mut TriggerData, false)
}

/*
 * ri_restrict -
 *
 * Common code for ON DELETE RESTRICT, ON DELETE NO ACTION,
 * ON UPDATE RESTRICT, and ON UPDATE NO ACTION.
 */
unsafe fn ri_restrict(trigdata: *mut TriggerData, is_no_action: bool) -> Datum {
    let riinfo: *const RI_ConstraintInfo;
    let fk_rel: Relation;
    let pk_rel: Relation;
    let oldslot: *mut TupleTableSlot;
    let mut qkey: RI_QueryKey = std::mem::zeroed();
    let mut qplan: SPIPlanPtr;

    riinfo = ri_FetchConstraintInfo((*trigdata).tg_trigger as *mut Trigger,
                                    (*trigdata).tg_relation, true);

    /*
     * Get the relation descriptors of the FK and PK tables and the old tuple.
     *
     * fk_rel is opened in RowShareLock mode since that's what our eventual
     * SELECT FOR KEY SHARE will get on it.
     */
    fk_rel = table_open((*riinfo).fk_relid, RowShareLock);
    pk_rel = (*trigdata).tg_relation;
    oldslot = (*trigdata).tg_trigslot;

    /*
     * If another PK row now exists providing the old key values, we should
     * not do anything.  However, this check should only be made in the NO
     * ACTION case; in RESTRICT cases we don't wish to allow another row to be
     * substituted.
     *
     * If the foreign key has PERIOD, we incorporate looking for replacement
     * rows in the main SQL query below, so we needn't do it here.
     */
    if is_no_action && !(*riinfo).hasperiod &&
        ri_Check_Pk_Match(pk_rel, fk_rel, oldslot, riinfo) {
        table_close(fk_rel, RowShareLock);
        return PointerGetDatum(std::ptr::null());
    }

    SPI_connect();

    /*
     * Fetch or prepare a saved plan for the restrict lookup (it's the same
     * query for delete and update cases)
     */
    ri_BuildQueryKey(&mut qkey, riinfo, if is_no_action { RI_PLAN_NO_ACTION } else { RI_PLAN_RESTRICT });

    qplan = ri_FetchPreparedPlan(&mut qkey);
    if qplan.is_null() {
        let mut querybuf: StringInfoData = std::mem::zeroed();
        let mut pkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
        let mut fkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
        let mut attname: [c_char; MAX_QUOTED_NAME_LEN] = [0; MAX_QUOTED_NAME_LEN];
        let mut periodattname: [c_char; MAX_QUOTED_NAME_LEN] = [0; MAX_QUOTED_NAME_LEN];
        let mut paramname: [c_char; 16] = [0; 16];
        let mut querysep: *const c_char;
        let mut queryoids: [Oid; RI_MAX_NUMKEYS] = [0; RI_MAX_NUMKEYS];
        let fk_only: *const c_char;

        /* ----------
         * The query string built is
         *	SELECT 1 FROM [ONLY] <fktable> x WHERE $1 = fkatt1 [AND ...]
         *		   FOR KEY SHARE OF x
         * The type id's for the $ parameters are those of the
         * corresponding PK attributes.
         * ----------
         */
        initStringInfo(&mut querybuf);
        fk_only = if (*(*fk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            c"".as_ptr()
        } else {
            c"ONLY ".as_ptr()
        };
        quoteRelationName(fkrelname.as_mut_ptr(), fk_rel);
        appendStringInfo!(&mut querybuf, "SELECT 1 FROM {}{} x",
                          cstr(fk_only), cstr(fkrelname.as_ptr()));
        querysep = c"WHERE".as_ptr();
        let mut i = 0;
        while i < (*riinfo).nkeys {
            let pk_type: Oid = RIAttType(pk_rel, (*riinfo).pk_attnums[i as usize]);
            let fk_type: Oid = RIAttType(fk_rel, (*riinfo).fk_attnums[i as usize]);

            quoteOneName(attname.as_mut_ptr(),
                         RIAttName(fk_rel, (*riinfo).fk_attnums[i as usize]));
            sprintf_param(paramname.as_mut_ptr(), i + 1);
            ri_GenerateQual(&mut querybuf, querysep,
                            paramname.as_ptr(), pk_type,
                            (*riinfo).pf_eq_oprs[i as usize],
                            attname.as_ptr(), fk_type);
            querysep = c"AND".as_ptr();
            queryoids[i as usize] = pk_type;
            i += 1;
        }

        /*----------
         * For temporal foreign keys, a reference could still be valid if the
         * referenced range didn't change too much.  (see C source for full
         * commentary)
         */
        if (*riinfo).hasperiod && is_no_action {
            let pk_period_type: Oid = RIAttType(pk_rel, (*riinfo).pk_attnums[((*riinfo).nkeys - 1) as usize]);
            let fk_period_type: Oid = RIAttType(fk_rel, (*riinfo).fk_attnums[((*riinfo).nkeys - 1) as usize]);
            let mut intersectbuf: StringInfoData = std::mem::zeroed();
            let mut replacementsbuf: StringInfoData = std::mem::zeroed();
            let pk_only: *const c_char = if (*(*pk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
                c"".as_ptr()
            } else {
                c"ONLY ".as_ptr()
            };

            quoteOneName(attname.as_mut_ptr(), RIAttName(fk_rel, (*riinfo).fk_attnums[((*riinfo).nkeys - 1) as usize]));
            sprintf_param(paramname.as_mut_ptr(), (*riinfo).nkeys);

            appendStringInfoString(&mut querybuf, c" AND NOT coalesce(".as_ptr());

            /* Intersect the fk with the old pk range */
            initStringInfo(&mut intersectbuf);
            appendStringInfoChar(&mut intersectbuf, b'(' as c_char);
            ri_GenerateQual(&mut intersectbuf, c"".as_ptr(),
                            attname.as_ptr(), fk_period_type,
                            (*riinfo).period_intersect_oper,
                            paramname.as_ptr(), pk_period_type);
            appendStringInfoChar(&mut intersectbuf, b')' as c_char);

            /* Find the remaining history */
            initStringInfo(&mut replacementsbuf);
            appendStringInfoString(&mut replacementsbuf, c"(SELECT pg_catalog.range_agg(r) FROM ".as_ptr());

            quoteOneName(periodattname.as_mut_ptr(), RIAttName(pk_rel, (*riinfo).pk_attnums[((*riinfo).nkeys - 1) as usize]));
            quoteRelationName(pkrelname.as_mut_ptr(), pk_rel);
            appendStringInfo!(&mut replacementsbuf, "(SELECT y.{} r FROM {}{} y",
                              cstr(periodattname.as_ptr()), cstr(pk_only), cstr(pkrelname.as_ptr()));

            /* Restrict pk rows to what matches */
            querysep = c"WHERE".as_ptr();
            let mut i = 0;
            while i < (*riinfo).nkeys {
                let pk_type: Oid = RIAttType(pk_rel, (*riinfo).pk_attnums[i as usize]);

                quoteOneName(attname.as_mut_ptr(),
                             RIAttName(pk_rel, (*riinfo).pk_attnums[i as usize]));
                sprintf_param(paramname.as_mut_ptr(), i + 1);
                ri_GenerateQual(&mut replacementsbuf, querysep,
                                paramname.as_ptr(), pk_type,
                                (*riinfo).pp_eq_oprs[i as usize],
                                attname.as_ptr(), pk_type);
                querysep = c"AND".as_ptr();
                queryoids[i as usize] = pk_type;
                i += 1;
            }
            appendStringInfoString(&mut replacementsbuf, c" FOR KEY SHARE OF y) y2)".as_ptr());

            ri_GenerateQual(&mut querybuf, c"".as_ptr(),
                            intersectbuf.data, fk_period_type,
                            (*riinfo).agged_period_contained_by_oper,
                            replacementsbuf.data, ANYMULTIRANGEOID);
            /* end of coalesce: */
            appendStringInfoString(&mut querybuf, c", false)".as_ptr());
        }

        appendStringInfoString(&mut querybuf, c" FOR KEY SHARE OF x".as_ptr());

        /* Prepare and save the plan */
        qplan = ri_PlanCheck(querybuf.data, (*riinfo).nkeys, queryoids.as_mut_ptr(),
                             &mut qkey, fk_rel, pk_rel);
    }

    /*
     * We have a plan now. Run it to check for existing references.
     */
    ri_PerformCheck(riinfo, &mut qkey, qplan,
                    fk_rel, pk_rel,
                    oldslot, std::ptr::null_mut(),
                    !is_no_action,
                    true,       /* must detect new rows */
                    SPI_OK_SELECT);

    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }

    table_close(fk_rel, RowShareLock);

    PointerGetDatum(std::ptr::null())
}


/*
 * RI_FKey_cascade_del -
 *
 * Cascaded delete foreign key references at delete event on PK table.
 */
pub unsafe fn RI_FKey_cascade_del(fcinfo: FunctionCallInfo) -> Datum {
    let trigdata: *mut TriggerData = (*fcinfo).context as *mut TriggerData;
    let riinfo: *const RI_ConstraintInfo;
    let fk_rel: Relation;
    let pk_rel: Relation;
    let oldslot: *mut TupleTableSlot;
    let mut qkey: RI_QueryKey = std::mem::zeroed();
    let mut qplan: SPIPlanPtr;

    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_cascade_del".as_ptr(), RI_TRIGTYPE_DELETE);

    riinfo = ri_FetchConstraintInfo((*trigdata).tg_trigger as *mut Trigger,
                                    (*trigdata).tg_relation, true);

    /*
     * Get the relation descriptors of the FK and PK tables and the old tuple.
     *
     * fk_rel is opened in RowExclusiveLock mode since that's what our
     * eventual DELETE will get on it.
     */
    fk_rel = table_open((*riinfo).fk_relid, RowExclusiveLock);
    pk_rel = (*trigdata).tg_relation;
    oldslot = (*trigdata).tg_trigslot;

    SPI_connect();

    /* Fetch or prepare a saved plan for the cascaded delete */
    ri_BuildQueryKey(&mut qkey, riinfo, RI_PLAN_CASCADE_ONDELETE);

    qplan = ri_FetchPreparedPlan(&mut qkey);
    if qplan.is_null() {
        let mut querybuf: StringInfoData = std::mem::zeroed();
        let mut fkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
        let mut attname: [c_char; MAX_QUOTED_NAME_LEN] = [0; MAX_QUOTED_NAME_LEN];
        let mut paramname: [c_char; 16] = [0; 16];
        let mut querysep: *const c_char;
        let mut queryoids: [Oid; RI_MAX_NUMKEYS] = [0; RI_MAX_NUMKEYS];
        let fk_only: *const c_char;

        /* ----------
         * The query string built is
         *	DELETE FROM [ONLY] <fktable> WHERE $1 = fkatt1 [AND ...]
         * The type id's for the $ parameters are those of the
         * corresponding PK attributes.
         * ----------
         */
        initStringInfo(&mut querybuf);
        fk_only = if (*(*fk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            c"".as_ptr()
        } else {
            c"ONLY ".as_ptr()
        };
        quoteRelationName(fkrelname.as_mut_ptr(), fk_rel);
        appendStringInfo!(&mut querybuf, "DELETE FROM {}{}",
                          cstr(fk_only), cstr(fkrelname.as_ptr()));
        querysep = c"WHERE".as_ptr();
        let mut i = 0;
        while i < (*riinfo).nkeys {
            let pk_type: Oid = RIAttType(pk_rel, (*riinfo).pk_attnums[i as usize]);
            let fk_type: Oid = RIAttType(fk_rel, (*riinfo).fk_attnums[i as usize]);

            quoteOneName(attname.as_mut_ptr(),
                         RIAttName(fk_rel, (*riinfo).fk_attnums[i as usize]));
            sprintf_param(paramname.as_mut_ptr(), i + 1);
            ri_GenerateQual(&mut querybuf, querysep,
                            paramname.as_ptr(), pk_type,
                            (*riinfo).pf_eq_oprs[i as usize],
                            attname.as_ptr(), fk_type);
            querysep = c"AND".as_ptr();
            queryoids[i as usize] = pk_type;
            i += 1;
        }

        /* Prepare and save the plan */
        qplan = ri_PlanCheck(querybuf.data, (*riinfo).nkeys, queryoids.as_mut_ptr(),
                             &mut qkey, fk_rel, pk_rel);
    }

    /*
     * We have a plan now. Build up the arguments from the key values in the
     * deleted PK tuple and delete the referencing rows
     */
    ri_PerformCheck(riinfo, &mut qkey, qplan,
                    fk_rel, pk_rel,
                    oldslot, std::ptr::null_mut(),
                    false,
                    true,       /* must detect new rows */
                    SPI_OK_DELETE);

    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }

    table_close(fk_rel, RowExclusiveLock);

    PointerGetDatum(std::ptr::null())
}


/*
 * RI_FKey_cascade_upd -
 *
 * Cascaded update foreign key references at update event on PK table.
 */
pub unsafe fn RI_FKey_cascade_upd(fcinfo: FunctionCallInfo) -> Datum {
    let trigdata: *mut TriggerData = (*fcinfo).context as *mut TriggerData;
    let riinfo: *const RI_ConstraintInfo;
    let fk_rel: Relation;
    let pk_rel: Relation;
    let newslot: *mut TupleTableSlot;
    let oldslot: *mut TupleTableSlot;
    let mut qkey: RI_QueryKey = std::mem::zeroed();
    let mut qplan: SPIPlanPtr;

    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_cascade_upd".as_ptr(), RI_TRIGTYPE_UPDATE);

    riinfo = ri_FetchConstraintInfo((*trigdata).tg_trigger as *mut Trigger,
                                    (*trigdata).tg_relation, true);

    /*
     * Get the relation descriptors of the FK and PK tables and the new and
     * old tuple.
     *
     * fk_rel is opened in RowExclusiveLock mode since that's what our
     * eventual UPDATE will get on it.
     */
    fk_rel = table_open((*riinfo).fk_relid, RowExclusiveLock);
    pk_rel = (*trigdata).tg_relation;
    newslot = (*trigdata).tg_newslot;
    oldslot = (*trigdata).tg_trigslot;

    SPI_connect();

    /* Fetch or prepare a saved plan for the cascaded update */
    ri_BuildQueryKey(&mut qkey, riinfo, RI_PLAN_CASCADE_ONUPDATE);

    qplan = ri_FetchPreparedPlan(&mut qkey);
    if qplan.is_null() {
        let mut querybuf: StringInfoData = std::mem::zeroed();
        let mut qualbuf: StringInfoData = std::mem::zeroed();
        let mut fkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
        let mut attname: [c_char; MAX_QUOTED_NAME_LEN] = [0; MAX_QUOTED_NAME_LEN];
        let mut paramname: [c_char; 16] = [0; 16];
        let mut querysep: *const c_char;
        let mut qualsep: *const c_char;
        let mut queryoids: [Oid; RI_MAX_NUMKEYS * 2] = [0; RI_MAX_NUMKEYS * 2];
        let fk_only: *const c_char;

        /* ----------
         * The query string built is
         *	UPDATE [ONLY] <fktable> SET fkatt1 = $1 [, ...]
         *			WHERE $n = fkatt1 [AND ...]
         * The type id's for the $ parameters are those of the
         * corresponding PK attributes.  Note that we are assuming
         * there is an assignment cast from the PK to the FK type;
         * else the parser will fail.
         * ----------
         */
        initStringInfo(&mut querybuf);
        initStringInfo(&mut qualbuf);
        fk_only = if (*(*fk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            c"".as_ptr()
        } else {
            c"ONLY ".as_ptr()
        };
        quoteRelationName(fkrelname.as_mut_ptr(), fk_rel);
        appendStringInfo!(&mut querybuf, "UPDATE {}{} SET",
                          cstr(fk_only), cstr(fkrelname.as_ptr()));
        querysep = c"".as_ptr();
        qualsep = c"WHERE".as_ptr();
        let mut i = 0;
        let mut j = (*riinfo).nkeys;
        while i < (*riinfo).nkeys {
            let pk_type: Oid = RIAttType(pk_rel, (*riinfo).pk_attnums[i as usize]);
            let fk_type: Oid = RIAttType(fk_rel, (*riinfo).fk_attnums[i as usize]);

            quoteOneName(attname.as_mut_ptr(),
                         RIAttName(fk_rel, (*riinfo).fk_attnums[i as usize]));
            appendStringInfo!(&mut querybuf,
                              "{} {} = ${}",
                              cstr(querysep), cstr(attname.as_ptr()), i + 1);
            sprintf_param(paramname.as_mut_ptr(), j + 1);
            ri_GenerateQual(&mut qualbuf, qualsep,
                            paramname.as_ptr(), pk_type,
                            (*riinfo).pf_eq_oprs[i as usize],
                            attname.as_ptr(), fk_type);
            querysep = c",".as_ptr();
            qualsep = c"AND".as_ptr();
            queryoids[i as usize] = pk_type;
            queryoids[j as usize] = pk_type;
            i += 1;
            j += 1;
        }
        appendBinaryStringInfo(&mut querybuf, qualbuf.data as *const c_void, qualbuf.len);

        /* Prepare and save the plan */
        qplan = ri_PlanCheck(querybuf.data, (*riinfo).nkeys * 2, queryoids.as_mut_ptr(),
                             &mut qkey, fk_rel, pk_rel);
    }

    /*
     * We have a plan now. Run it to update the existing references.
     */
    ri_PerformCheck(riinfo, &mut qkey, qplan,
                    fk_rel, pk_rel,
                    oldslot, newslot,
                    false,
                    true,       /* must detect new rows */
                    SPI_OK_UPDATE);

    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }

    table_close(fk_rel, RowExclusiveLock);

    PointerGetDatum(std::ptr::null())
}


/*
 * RI_FKey_setnull_del -
 *
 * Set foreign key references to NULL values at delete event on PK table.
 */
pub unsafe fn RI_FKey_setnull_del(fcinfo: FunctionCallInfo) -> Datum {
    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_setnull_del".as_ptr(), RI_TRIGTYPE_DELETE);

    /* Share code with UPDATE case */
    ri_set((*fcinfo).context as *mut TriggerData, true, RI_TRIGTYPE_DELETE)
}

/*
 * RI_FKey_setnull_upd -
 *
 * Set foreign key references to NULL at update event on PK table.
 */
pub unsafe fn RI_FKey_setnull_upd(fcinfo: FunctionCallInfo) -> Datum {
    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_setnull_upd".as_ptr(), RI_TRIGTYPE_UPDATE);

    /* Share code with DELETE case */
    ri_set((*fcinfo).context as *mut TriggerData, true, RI_TRIGTYPE_UPDATE)
}

/*
 * RI_FKey_setdefault_del -
 *
 * Set foreign key references to defaults at delete event on PK table.
 */
pub unsafe fn RI_FKey_setdefault_del(fcinfo: FunctionCallInfo) -> Datum {
    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_setdefault_del".as_ptr(), RI_TRIGTYPE_DELETE);

    /* Share code with UPDATE case */
    ri_set((*fcinfo).context as *mut TriggerData, false, RI_TRIGTYPE_DELETE)
}

/*
 * RI_FKey_setdefault_upd -
 *
 * Set foreign key references to defaults at update event on PK table.
 */
pub unsafe fn RI_FKey_setdefault_upd(fcinfo: FunctionCallInfo) -> Datum {
    /* Check that this is a valid trigger call on the right time and event. */
    ri_CheckTrigger(fcinfo, c"RI_FKey_setdefault_upd".as_ptr(), RI_TRIGTYPE_UPDATE);

    /* Share code with DELETE case */
    ri_set((*fcinfo).context as *mut TriggerData, false, RI_TRIGTYPE_UPDATE)
}

/*
 * ri_set -
 *
 * Common code for ON DELETE SET NULL, ON DELETE SET DEFAULT, ON UPDATE SET
 * NULL, and ON UPDATE SET DEFAULT.
 */
unsafe fn ri_set(trigdata: *mut TriggerData, is_set_null: bool, tgkind: c_int) -> Datum {
    let riinfo: *const RI_ConstraintInfo;
    let fk_rel: Relation;
    let pk_rel: Relation;
    let oldslot: *mut TupleTableSlot;
    let mut qkey: RI_QueryKey = std::mem::zeroed();
    let mut qplan: SPIPlanPtr;
    let queryno: int32;

    riinfo = ri_FetchConstraintInfo((*trigdata).tg_trigger as *mut Trigger,
                                    (*trigdata).tg_relation, true);

    /*
     * Get the relation descriptors of the FK and PK tables and the old tuple.
     *
     * fk_rel is opened in RowExclusiveLock mode since that's what our
     * eventual UPDATE will get on it.
     */
    fk_rel = table_open((*riinfo).fk_relid, RowExclusiveLock);
    pk_rel = (*trigdata).tg_relation;
    oldslot = (*trigdata).tg_trigslot;

    SPI_connect();

    /*
     * Fetch or prepare a saved plan for the trigger.
     */
    match tgkind {
        RI_TRIGTYPE_UPDATE => {
            queryno = if is_set_null {
                RI_PLAN_SETNULL_ONUPDATE
            } else {
                RI_PLAN_SETDEFAULT_ONUPDATE
            };
        }
        RI_TRIGTYPE_DELETE => {
            queryno = if is_set_null {
                RI_PLAN_SETNULL_ONDELETE
            } else {
                RI_PLAN_SETDEFAULT_ONDELETE
            };
        }
        _ => {
            elog!(ERROR, "invalid tgkind passed to ri_set");
            unreachable!();
        }
    }

    ri_BuildQueryKey(&mut qkey, riinfo, queryno);

    qplan = ri_FetchPreparedPlan(&mut qkey);
    if qplan.is_null() {
        let mut querybuf: StringInfoData = std::mem::zeroed();
        let mut fkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
        let mut attname: [c_char; MAX_QUOTED_NAME_LEN] = [0; MAX_QUOTED_NAME_LEN];
        let mut paramname: [c_char; 16] = [0; 16];
        let mut querysep: *const c_char;
        let mut qualsep: *const c_char;
        let mut queryoids: [Oid; RI_MAX_NUMKEYS] = [0; RI_MAX_NUMKEYS];
        let fk_only: *const c_char;
        let num_cols_to_set: c_int;
        let set_cols: *const int16;

        match tgkind {
            RI_TRIGTYPE_UPDATE => {
                num_cols_to_set = (*riinfo).nkeys;
                set_cols = (*riinfo).fk_attnums.as_ptr();
            }
            RI_TRIGTYPE_DELETE => {
                /*
                 * If confdelsetcols are present, then we only update the
                 * columns specified in that array, otherwise we update all
                 * the referencing columns.
                 */
                if (*riinfo).ndelsetcols != 0 {
                    num_cols_to_set = (*riinfo).ndelsetcols;
                    set_cols = (*riinfo).confdelsetcols.as_ptr();
                } else {
                    num_cols_to_set = (*riinfo).nkeys;
                    set_cols = (*riinfo).fk_attnums.as_ptr();
                }
            }
            _ => {
                elog!(ERROR, "invalid tgkind passed to ri_set");
                unreachable!();
            }
        }

        /* ----------
         * The query string built is
         *	UPDATE [ONLY] <fktable> SET fkatt1 = {NULL|DEFAULT} [, ...]
         *			WHERE $1 = fkatt1 [AND ...]
         * The type id's for the $ parameters are those of the
         * corresponding PK attributes.
         * ----------
         */
        initStringInfo(&mut querybuf);
        fk_only = if (*(*fk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            c"".as_ptr()
        } else {
            c"ONLY ".as_ptr()
        };
        quoteRelationName(fkrelname.as_mut_ptr(), fk_rel);
        appendStringInfo!(&mut querybuf, "UPDATE {}{} SET",
                          cstr(fk_only), cstr(fkrelname.as_ptr()));

        /*
         * Add assignment clauses
         */
        querysep = c"".as_ptr();
        let mut i = 0;
        while i < num_cols_to_set {
            quoteOneName(attname.as_mut_ptr(), RIAttName(fk_rel, *set_cols.add(i as usize)));
            appendStringInfo!(&mut querybuf,
                              "{} {} = {}",
                              cstr(querysep), cstr(attname.as_ptr()),
                              if is_set_null { "NULL" } else { "DEFAULT" });
            querysep = c",".as_ptr();
            i += 1;
        }

        /*
         * Add WHERE clause
         */
        qualsep = c"WHERE".as_ptr();
        let mut i = 0;
        while i < (*riinfo).nkeys {
            let pk_type: Oid = RIAttType(pk_rel, (*riinfo).pk_attnums[i as usize]);
            let fk_type: Oid = RIAttType(fk_rel, (*riinfo).fk_attnums[i as usize]);

            quoteOneName(attname.as_mut_ptr(),
                         RIAttName(fk_rel, (*riinfo).fk_attnums[i as usize]));

            sprintf_param(paramname.as_mut_ptr(), i + 1);
            ri_GenerateQual(&mut querybuf, qualsep,
                            paramname.as_ptr(), pk_type,
                            (*riinfo).pf_eq_oprs[i as usize],
                            attname.as_ptr(), fk_type);
            qualsep = c"AND".as_ptr();
            queryoids[i as usize] = pk_type;
            i += 1;
        }

        /* Prepare and save the plan */
        qplan = ri_PlanCheck(querybuf.data, (*riinfo).nkeys, queryoids.as_mut_ptr(),
                             &mut qkey, fk_rel, pk_rel);
    }

    /*
     * We have a plan now. Run it to update the existing references.
     */
    ri_PerformCheck(riinfo, &mut qkey, qplan,
                    fk_rel, pk_rel,
                    oldslot, std::ptr::null_mut(),
                    false,
                    true,       /* must detect new rows */
                    SPI_OK_UPDATE);

    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }

    table_close(fk_rel, RowExclusiveLock);

    if is_set_null {
        PointerGetDatum(std::ptr::null())
    } else {
        /*
         * If we just deleted or updated the PK row whose key was equal to the
         * FK columns' default values, and a referencing row exists in the FK
         * table, we would have updated that row to the same values it already
         * had --- and RI_FKey_fk_upd_check_required would hence believe no
         * check is necessary.  So we need to do another lookup now and in
         * case a reference still exists, abort the operation.  That is
         * already implemented in the NO ACTION trigger, so just run it. (This
         * recheck is only needed in the SET DEFAULT case, since CASCADE would
         * remove such rows in case of a DELETE operation or would change the
         * FK key values in case of an UPDATE, while SET NULL is certain to
         * result in rows that satisfy the FK constraint.)
         */
        ri_restrict(trigdata, true)
    }
}


/*
 * RI_FKey_pk_upd_check_required -
 *
 * Check if we really need to fire the RI trigger for an update or delete to a PK
 * relation.  This is called by the AFTER trigger queue manager to see if
 * it can skip queuing an instance of an RI trigger.  Returns true if the
 * trigger must be fired, false if we can prove the constraint will still
 * be satisfied.
 *
 * newslot will be NULL if this is called for a delete.
 */
pub unsafe fn RI_FKey_pk_upd_check_required(
    trigger: *mut Trigger,
    pk_rel: Relation,
    oldslot: *mut TupleTableSlot,
    newslot: *mut TupleTableSlot,
) -> bool {
    let riinfo: *const RI_ConstraintInfo;

    riinfo = ri_FetchConstraintInfo(trigger, pk_rel, true);

    /*
     * If any old key value is NULL, the row could not have been referenced by
     * an FK row, so no check is needed.
     */
    if ri_NullCheck(RelationGetDescr(pk_rel), oldslot, riinfo, true) != RI_KEYS_NONE_NULL {
        return false;
    }

    /* If all old and new key values are equal, no check is needed */
    if !newslot.is_null() && ri_KeysEqual(pk_rel, oldslot, newslot, riinfo, true) {
        return false;
    }

    /* Else we need to fire the trigger. */
    true
}

/*
 * RI_FKey_fk_upd_check_required -
 *
 * Check if we really need to fire the RI trigger for an update to an FK
 * relation.  This is called by the AFTER trigger queue manager to see if
 * it can skip queuing an instance of an RI trigger.  Returns true if the
 * trigger must be fired, false if we can prove the constraint will still
 * be satisfied.
 */
pub unsafe fn RI_FKey_fk_upd_check_required(
    trigger: *mut Trigger,
    fk_rel: Relation,
    oldslot: *mut TupleTableSlot,
    newslot: *mut TupleTableSlot,
) -> bool {
    let riinfo: *const RI_ConstraintInfo;
    let ri_nullcheck: c_int;

    /*
     * AfterTriggerSaveEvent() handles things such that this function is never
     * called for partitioned tables.
     */
    Assert!((*(*fk_rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE);

    riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false);

    ri_nullcheck = ri_NullCheck(RelationGetDescr(fk_rel), newslot, riinfo, false);

    /*
     * If all new key values are NULL, the row satisfies the constraint, so no
     * check is needed.
     */
    if ri_nullcheck == RI_KEYS_ALL_NULL {
        return false;
    }
    /*
     * If some new key values are NULL, the behavior depends on the match
     * type.
     */
    else if ri_nullcheck == RI_KEYS_SOME_NULL {
        match (*riinfo).confmatchtype {
            FKCONSTR_MATCH_SIMPLE => {
                /*
                 * If any new key value is NULL, the row must satisfy the
                 * constraint, so no check is needed.
                 */
                return false;
            }
            FKCONSTR_MATCH_PARTIAL => {
                /*
                 * Don't know, must run full check.
                 */
            }
            FKCONSTR_MATCH_FULL => {
                /*
                 * If some new key values are NULL, the row fails the
                 * constraint.  We must not throw error here, because the row
                 * might get invalidated before the constraint is to be
                 * checked, but we should queue the event to apply the check
                 * later.
                 */
                return true;
            }
            _ => {}
        }
    }

    /*
     * Continues here for no new key values are NULL, or we couldn't decide
     * yet.
     */

    /*
     * If the original row was inserted by our own transaction, we must fire
     * the trigger whether or not the keys are equal.  This is because our
     * UPDATE will invalidate the INSERT so that the INSERT RI trigger will
     * not do anything; so we had better do the UPDATE check.  (We could skip
     * this if we knew the INSERT trigger already fired, but there is no easy
     * way to know that.)
     */
    if slot_is_current_xact_tuple(oldslot) {
        return true;
    }

    /* If all old and new key values are equal, no check is needed */
    if ri_KeysEqual(fk_rel, oldslot, newslot, riinfo, false) {
        return false;
    }

    /* Else we need to fire the trigger. */
    true
}

/*
 * RI_Initial_Check -
 *
 * Check an entire table for non-matching values using a single query.
 * This is not a trigger procedure, but is called during ALTER TABLE
 * ADD FOREIGN KEY to validate the initial table contents.
 *
 * We expect that the caller has made provision to prevent any problems
 * caused by concurrent actions. This could be either by locking rel and
 * pkrel at ShareRowExclusiveLock or higher, or by otherwise ensuring
 * that triggers implementing the checks are already active.
 * Hence, we do not need to lock individual rows for the check.
 *
 * If the check fails because the current user doesn't have permissions
 * to read both tables, return false to let our caller know that they will
 * need to do something else to check the constraint.
 */
#[allow(unreachable_code)] // ri_ReportViolation returns `!` (always ereports ERROR); C keeps cleanup after.
pub unsafe fn RI_Initial_Check(trigger: *mut Trigger, fk_rel: Relation, pk_rel: Relation) -> bool {
    let riinfo: *const RI_ConstraintInfo;
    let mut querybuf: StringInfoData = std::mem::zeroed();
    let mut pkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
    let mut fkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
    let mut pkattname: [c_char; MAX_QUOTED_NAME_LEN + 3] = [0; MAX_QUOTED_NAME_LEN + 3];
    let mut fkattname: [c_char; MAX_QUOTED_NAME_LEN + 3] = [0; MAX_QUOTED_NAME_LEN + 3];
    let mut rte: *mut RangeTblEntry;
    let pk_perminfo: *mut RTEPermissionInfo;
    let fk_perminfo: *mut RTEPermissionInfo;
    let mut rtes: *mut List = NIL;
    let mut perminfos: *mut List = NIL;
    let mut sep: *const c_char;
    let fk_only: *const c_char;
    let pk_only: *const c_char;
    let save_nestlevel: c_int;
    let mut workmembuf: [c_char; 32] = [0; 32];
    let spi_result: c_int;
    let qplan: SPIPlanPtr;

    riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false);

    /*
     * Check to make sure current user has enough permissions to do the test
     * query.  (If not, caller can fall back to the trigger method, which
     * works because it changes user IDs on the fly.)
     *
     * XXX are there any other show-stopper conditions to check?
     */
    pk_perminfo = makeNode_alloc::<RTEPermissionInfo>();
    (*pk_perminfo).relid = RelationGetRelid(pk_rel);
    (*pk_perminfo).requiredPerms = ACL_SELECT;
    perminfos = lappend(perminfos, pk_perminfo as *mut c_void);
    rte = makeNode_alloc::<RangeTblEntry>();
    (*rte).rtekind = RTE_RELATION;
    (*rte).relid = RelationGetRelid(pk_rel);
    (*rte).relkind = (*(*pk_rel).rd_rel).relkind;
    (*rte).rellockmode = AccessShareLock;
    (*rte).perminfoindex = list_length(perminfos) as c_int;
    rtes = lappend(rtes, rte as *mut c_void);

    fk_perminfo = makeNode_alloc::<RTEPermissionInfo>();
    (*fk_perminfo).relid = RelationGetRelid(fk_rel);
    (*fk_perminfo).requiredPerms = ACL_SELECT;
    perminfos = lappend(perminfos, fk_perminfo as *mut c_void);
    rte = makeNode_alloc::<RangeTblEntry>();
    (*rte).rtekind = RTE_RELATION;
    (*rte).relid = RelationGetRelid(fk_rel);
    (*rte).relkind = (*(*fk_rel).rd_rel).relkind;
    (*rte).rellockmode = AccessShareLock;
    (*rte).perminfoindex = list_length(perminfos) as c_int;
    rtes = lappend(rtes, rte as *mut c_void);

    let mut i = 0;
    while i < (*riinfo).nkeys {
        let mut attno: c_int;

        attno = (*riinfo).pk_attnums[i as usize] as c_int - FirstLowInvalidHeapAttributeNumber as c_int;
        (*pk_perminfo).selectedCols = bms_add_member((*pk_perminfo).selectedCols, attno);

        attno = (*riinfo).fk_attnums[i as usize] as c_int - FirstLowInvalidHeapAttributeNumber as c_int;
        (*fk_perminfo).selectedCols = bms_add_member((*fk_perminfo).selectedCols, attno);
        i += 1;
    }

    if !ExecCheckPermissions(rtes, perminfos, false) {
        return false;
    }

    /*
     * Also punt if RLS is enabled on either table unless this role has the
     * bypassrls right or is the table owner of the table(s) involved which
     * have RLS enabled.
     */
    if !has_bypassrls_privilege(GetUserId()) &&
        (((*(*pk_rel).rd_rel).relrowsecurity &&
          !object_ownercheck(RelationRelationId, RelationGetRelid(pk_rel), GetUserId())) ||
         ((*(*fk_rel).rd_rel).relrowsecurity &&
          !object_ownercheck(RelationRelationId, RelationGetRelid(fk_rel), GetUserId()))) {
        return false;
    }

    /*----------
     * The query string built is:
     *	SELECT fk.keycols FROM [ONLY] relname fk
     *	 LEFT OUTER JOIN [ONLY] pkrelname pk
     *	 ON (pk.pkkeycol1=fk.keycol1 [AND ...])
     *	 WHERE pk.pkkeycol1 IS NULL AND
     * For MATCH SIMPLE:
     *	 (fk.keycol1 IS NOT NULL [AND ...])
     * For MATCH FULL:
     *	 (fk.keycol1 IS NOT NULL [OR ...])
     *
     * We attach COLLATE clauses to the operators when comparing columns
     * that have different collations.
     *----------
     */
    initStringInfo(&mut querybuf);
    appendStringInfoString(&mut querybuf, c"SELECT ".as_ptr());
    sep = c"".as_ptr();
    let mut i = 0;
    while i < (*riinfo).nkeys {
        quoteOneName(fkattname.as_mut_ptr(),
                     RIAttName(fk_rel, (*riinfo).fk_attnums[i as usize]));
        appendStringInfo!(&mut querybuf, "{}fk.{}", cstr(sep), cstr(fkattname.as_ptr()));
        sep = c", ".as_ptr();
        i += 1;
    }

    quoteRelationName(pkrelname.as_mut_ptr(), pk_rel);
    quoteRelationName(fkrelname.as_mut_ptr(), fk_rel);
    fk_only = if (*(*fk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        c"".as_ptr()
    } else {
        c"ONLY ".as_ptr()
    };
    pk_only = if (*(*pk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        c"".as_ptr()
    } else {
        c"ONLY ".as_ptr()
    };
    appendStringInfo!(&mut querybuf,
                      " FROM {}{} fk LEFT OUTER JOIN {}{} pk ON",
                      cstr(fk_only), cstr(fkrelname.as_ptr()), cstr(pk_only), cstr(pkrelname.as_ptr()));

    strcpy_lit(pkattname.as_mut_ptr(), c"pk.".as_ptr());
    strcpy_lit(fkattname.as_mut_ptr(), c"fk.".as_ptr());
    sep = c"(".as_ptr();
    let mut i = 0;
    while i < (*riinfo).nkeys {
        let pk_type: Oid = RIAttType(pk_rel, (*riinfo).pk_attnums[i as usize]);
        let fk_type: Oid = RIAttType(fk_rel, (*riinfo).fk_attnums[i as usize]);
        let pk_coll: Oid = RIAttCollation(pk_rel, (*riinfo).pk_attnums[i as usize]);
        let fk_coll: Oid = RIAttCollation(fk_rel, (*riinfo).fk_attnums[i as usize]);

        quoteOneName(pkattname.as_mut_ptr().add(3),
                     RIAttName(pk_rel, (*riinfo).pk_attnums[i as usize]));
        quoteOneName(fkattname.as_mut_ptr().add(3),
                     RIAttName(fk_rel, (*riinfo).fk_attnums[i as usize]));
        ri_GenerateQual(&mut querybuf, sep,
                        pkattname.as_ptr(), pk_type,
                        (*riinfo).pf_eq_oprs[i as usize],
                        fkattname.as_ptr(), fk_type);
        if pk_coll != fk_coll {
            ri_GenerateQualCollation(&mut querybuf, pk_coll);
        }
        sep = c"AND".as_ptr();
        i += 1;
    }

    /*
     * It's sufficient to test any one pk attribute for null to detect a join
     * failure.
     */
    quoteOneName(pkattname.as_mut_ptr(), RIAttName(pk_rel, (*riinfo).pk_attnums[0]));
    appendStringInfo!(&mut querybuf, ") WHERE pk.{} IS NULL AND (", cstr(pkattname.as_ptr()));

    sep = c"".as_ptr();
    let mut i = 0;
    while i < (*riinfo).nkeys {
        quoteOneName(fkattname.as_mut_ptr(), RIAttName(fk_rel, (*riinfo).fk_attnums[i as usize]));
        appendStringInfo!(&mut querybuf,
                          "{}fk.{} IS NOT NULL",
                          cstr(sep), cstr(fkattname.as_ptr()));
        match (*riinfo).confmatchtype {
            FKCONSTR_MATCH_SIMPLE => {
                sep = c" AND ".as_ptr();
            }
            FKCONSTR_MATCH_FULL => {
                sep = c" OR ".as_ptr();
            }
            _ => {}
        }
        i += 1;
    }
    appendStringInfoChar(&mut querybuf, b')' as c_char);

    /*
     * Temporarily increase work_mem so that the check query can be executed
     * more efficiently. (see C source for full commentary)
     */
    save_nestlevel = NewGUCNestLevel();

    snprintf_int(workmembuf.as_mut_ptr(), 32, maintenance_work_mem);
    set_config_option(c"work_mem".as_ptr(), workmembuf.as_ptr(),
                      PGC_USERSET, PGC_S_SESSION,
                      GUC_ACTION_SAVE, true, 0, false);
    set_config_option(c"hash_mem_multiplier".as_ptr(), c"1".as_ptr(),
                      PGC_USERSET, PGC_S_SESSION,
                      GUC_ACTION_SAVE, true, 0, false);

    SPI_connect();

    /*
     * Generate the plan.  We don't need to cache it, and there are no
     * arguments to the plan.
     */
    qplan = SPI_prepare(querybuf.data, 0, std::ptr::null_mut());

    if qplan.is_null() {
        elog!(ERROR, "SPI_prepare returned {} for {}",
              cstr(SPI_result_code_string(SPI_result)), cstr(querybuf.data));
    }

    /*
     * Run the plan.  For safety we force a current snapshot to be used. (see
     * C source for full commentary)
     */
    spi_result = SPI_execute_snapshot(qplan,
                                      std::ptr::null_mut(), std::ptr::null_mut(),
                                      GetLatestSnapshot(),
                                      InvalidSnapshot,
                                      true, false, 1);

    /* Check result */
    if spi_result != SPI_OK_SELECT {
        elog!(ERROR, "SPI_execute_snapshot returned {}", cstr(SPI_result_code_string(spi_result)));
    }

    /* Did we find a tuple violating the constraint? */
    if SPI_processed > 0 {
        let slot: *mut TupleTableSlot;
        let tuple: HeapTuple = *(*SPI_tuptable).vals.add(0);
        let tupdesc: TupleDesc = (*SPI_tuptable).tupdesc;
        let mut fake_riinfo: RI_ConstraintInfo = std::mem::zeroed();

        slot = MakeSingleTupleTableSlot(tupdesc, &raw const TTSOpsVirtual);

        heap_deform_tuple(tuple, tupdesc,
                          (*slot).tts_values, (*slot).tts_isnull);
        ExecStoreVirtualTuple(slot);

        /*
         * The columns to look at in the result tuple are 1..N, not whatever
         * they are in the fk_rel.  Hack up riinfo so that the subroutines
         * called here will behave properly.
         *
         * In addition to this, we have to pass the correct tupdesc to
         * ri_ReportViolation, overriding its normal habit of using the pk_rel
         * or fk_rel's tupdesc.
         */
        std::ptr::copy_nonoverlapping(riinfo, &mut fake_riinfo, 1);
        let mut i = 0;
        while i < fake_riinfo.nkeys {
            fake_riinfo.fk_attnums[i as usize] = (i + 1) as int16;
            i += 1;
        }

        /*
         * If it's MATCH FULL, and there are any nulls in the FK keys,
         * complain about that rather than the lack of a match.  MATCH FULL
         * disallows partially-null FK rows.
         */
        if fake_riinfo.confmatchtype == FKCONSTR_MATCH_FULL &&
            ri_NullCheck(tupdesc, slot, &fake_riinfo, false) != RI_KEYS_NONE_NULL {
            ereport!(ERROR,
                     errmsg!("insert or update on table \"{}\" violates foreign key constraint \"{}\"",
                             cstr(RelationGetRelationName(fk_rel)),
                             cstr(NameStr(&raw mut fake_riinfo.conname))));
        }

        /*
         * We tell ri_ReportViolation we were doing the RI_PLAN_CHECK_LOOKUPPK
         * query, which isn't true, but will cause it to use
         * fake_riinfo.fk_attnums as we need.
         */
        ri_ReportViolation(&fake_riinfo,
                           pk_rel, fk_rel,
                           slot, tupdesc,
                           RI_PLAN_CHECK_LOOKUPPK, false, false);

        ExecDropSingleTupleTableSlot(slot);
    }

    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }

    /*
     * Restore work_mem and hash_mem_multiplier.
     */
    AtEOXact_GUC(true, save_nestlevel);

    true
}

/*
 * RI_PartitionRemove_Check -
 *
 * Verify no referencing values exist, when a partition is detached on
 * the referenced side of a foreign key constraint.
 */
pub unsafe fn RI_PartitionRemove_Check(trigger: *mut Trigger, fk_rel: Relation, pk_rel: Relation) {
    let riinfo: *const RI_ConstraintInfo;
    let mut querybuf: StringInfoData = std::mem::zeroed();
    let constraintDef: *mut c_char;
    let mut pkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
    let mut fkrelname: [c_char; MAX_QUOTED_REL_NAME_LEN] = [0; MAX_QUOTED_REL_NAME_LEN];
    let mut pkattname: [c_char; MAX_QUOTED_NAME_LEN + 3] = [0; MAX_QUOTED_NAME_LEN + 3];
    let mut fkattname: [c_char; MAX_QUOTED_NAME_LEN + 3] = [0; MAX_QUOTED_NAME_LEN + 3];
    let mut sep: *const c_char;
    let fk_only: *const c_char;
    let save_nestlevel: c_int;
    let mut workmembuf: [c_char; 32] = [0; 32];
    let spi_result: c_int;
    let qplan: SPIPlanPtr;
    let mut i: c_int;

    riinfo = ri_FetchConstraintInfo(trigger, fk_rel, false);

    /*
     * We don't check permissions before displaying the error message, on the
     * assumption that the user detaching the partition must have enough
     * privileges to examine the table contents anyhow.
     */

    /*----------
     * The query string built is:
     *  SELECT fk.keycols FROM [ONLY] relname fk
     *    JOIN pkrelname pk
     *    ON (pk.pkkeycol1=fk.keycol1 [AND ...])
     *    WHERE (<partition constraint>) AND
     * For MATCH SIMPLE:
     *   (fk.keycol1 IS NOT NULL [AND ...])
     * For MATCH FULL:
     *   (fk.keycol1 IS NOT NULL [OR ...])
     *
     * We attach COLLATE clauses to the operators when comparing columns
     * that have different collations.
     *----------
     */
    initStringInfo(&mut querybuf);
    appendStringInfoString(&mut querybuf, c"SELECT ".as_ptr());
    sep = c"".as_ptr();
    i = 0;
    while i < (*riinfo).nkeys {
        quoteOneName(fkattname.as_mut_ptr(),
                     RIAttName(fk_rel, (*riinfo).fk_attnums[i as usize]));
        appendStringInfo!(&mut querybuf, "{}fk.{}", cstr(sep), cstr(fkattname.as_ptr()));
        sep = c", ".as_ptr();
        i += 1;
    }

    quoteRelationName(pkrelname.as_mut_ptr(), pk_rel);
    quoteRelationName(fkrelname.as_mut_ptr(), fk_rel);
    fk_only = if (*(*fk_rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        c"".as_ptr()
    } else {
        c"ONLY ".as_ptr()
    };
    appendStringInfo!(&mut querybuf,
                      " FROM {}{} fk JOIN {} pk ON",
                      cstr(fk_only), cstr(fkrelname.as_ptr()), cstr(pkrelname.as_ptr()));
    strcpy_lit(pkattname.as_mut_ptr(), c"pk.".as_ptr());
    strcpy_lit(fkattname.as_mut_ptr(), c"fk.".as_ptr());
    sep = c"(".as_ptr();
    i = 0;
    while i < (*riinfo).nkeys {
        let pk_type: Oid = RIAttType(pk_rel, (*riinfo).pk_attnums[i as usize]);
        let fk_type: Oid = RIAttType(fk_rel, (*riinfo).fk_attnums[i as usize]);
        let pk_coll: Oid = RIAttCollation(pk_rel, (*riinfo).pk_attnums[i as usize]);
        let fk_coll: Oid = RIAttCollation(fk_rel, (*riinfo).fk_attnums[i as usize]);

        quoteOneName(pkattname.as_mut_ptr().add(3),
                     RIAttName(pk_rel, (*riinfo).pk_attnums[i as usize]));
        quoteOneName(fkattname.as_mut_ptr().add(3),
                     RIAttName(fk_rel, (*riinfo).fk_attnums[i as usize]));
        ri_GenerateQual(&mut querybuf, sep,
                        pkattname.as_ptr(), pk_type,
                        (*riinfo).pf_eq_oprs[i as usize],
                        fkattname.as_ptr(), fk_type);
        if pk_coll != fk_coll {
            ri_GenerateQualCollation(&mut querybuf, pk_coll);
        }
        sep = c"AND".as_ptr();
        i += 1;
    }

    /*
     * Start the WHERE clause with the partition constraint (except if this is
     * the default partition and there's no other partition, because the
     * partition constraint is the empty string in that case.)
     */
    constraintDef = pg_get_partconstrdef_string(RelationGetRelid(pk_rel), c"pk".as_ptr());
    if !constraintDef.is_null() && *constraintDef != b'\0' as c_char {
        appendStringInfo!(&mut querybuf, ") WHERE {} AND (",
                          cstr(constraintDef));
    } else {
        appendStringInfoString(&mut querybuf, c") WHERE (".as_ptr());
    }

    sep = c"".as_ptr();
    i = 0;
    while i < (*riinfo).nkeys {
        quoteOneName(fkattname.as_mut_ptr(), RIAttName(fk_rel, (*riinfo).fk_attnums[i as usize]));
        appendStringInfo!(&mut querybuf,
                          "{}fk.{} IS NOT NULL",
                          cstr(sep), cstr(fkattname.as_ptr()));
        match (*riinfo).confmatchtype {
            FKCONSTR_MATCH_SIMPLE => {
                sep = c" AND ".as_ptr();
            }
            FKCONSTR_MATCH_FULL => {
                sep = c" OR ".as_ptr();
            }
            _ => {}
        }
        i += 1;
    }
    appendStringInfoChar(&mut querybuf, b')' as c_char);

    /*
     * Temporarily increase work_mem so that the check query can be executed
     * more efficiently. (see C source for full commentary)
     */
    save_nestlevel = NewGUCNestLevel();

    snprintf_int(workmembuf.as_mut_ptr(), 32, maintenance_work_mem);
    set_config_option(c"work_mem".as_ptr(), workmembuf.as_ptr(),
                      PGC_USERSET, PGC_S_SESSION,
                      GUC_ACTION_SAVE, true, 0, false);
    set_config_option(c"hash_mem_multiplier".as_ptr(), c"1".as_ptr(),
                      PGC_USERSET, PGC_S_SESSION,
                      GUC_ACTION_SAVE, true, 0, false);

    SPI_connect();

    /*
     * Generate the plan.  We don't need to cache it, and there are no
     * arguments to the plan.
     */
    qplan = SPI_prepare(querybuf.data, 0, std::ptr::null_mut());

    if qplan.is_null() {
        elog!(ERROR, "SPI_prepare returned {} for {}",
              cstr(SPI_result_code_string(SPI_result)), cstr(querybuf.data));
    }

    /*
     * Run the plan.  For safety we force a current snapshot to be used. (see
     * C source for full commentary)
     */
    spi_result = SPI_execute_snapshot(qplan,
                                      std::ptr::null_mut(), std::ptr::null_mut(),
                                      GetLatestSnapshot(),
                                      InvalidSnapshot,
                                      true, false, 1);

    /* Check result */
    if spi_result != SPI_OK_SELECT {
        elog!(ERROR, "SPI_execute_snapshot returned {}", cstr(SPI_result_code_string(spi_result)));
    }

    /* Did we find a tuple that would violate the constraint? */
    if SPI_processed > 0 {
        let slot: *mut TupleTableSlot;
        let tuple: HeapTuple = *(*SPI_tuptable).vals.add(0);
        let tupdesc: TupleDesc = (*SPI_tuptable).tupdesc;
        let mut fake_riinfo: RI_ConstraintInfo = std::mem::zeroed();

        slot = MakeSingleTupleTableSlot(tupdesc, &raw const TTSOpsVirtual);

        heap_deform_tuple(tuple, tupdesc,
                          (*slot).tts_values, (*slot).tts_isnull);
        ExecStoreVirtualTuple(slot);

        /*
         * The columns to look at in the result tuple are 1..N, not whatever
         * they are in the fk_rel.  Hack up riinfo so that ri_ReportViolation
         * will behave properly.
         *
         * In addition to this, we have to pass the correct tupdesc to
         * ri_ReportViolation, overriding its normal habit of using the pk_rel
         * or fk_rel's tupdesc.
         */
        std::ptr::copy_nonoverlapping(riinfo, &mut fake_riinfo, 1);
        i = 0;
        while i < fake_riinfo.nkeys {
            fake_riinfo.pk_attnums[i as usize] = (i + 1) as int16;
            i += 1;
        }

        ri_ReportViolation(&fake_riinfo, pk_rel, fk_rel,
                           slot, tupdesc, 0, false, true);
    }

    if SPI_finish() != SPI_OK_FINISH {
        elog!(ERROR, "SPI_finish failed");
    }

    /*
     * Restore work_mem and hash_mem_multiplier.
     */
    AtEOXact_GUC(true, save_nestlevel);
}


/* ----------
 * Local functions below
 * ----------
 */


/*
 * quoteOneName --- safely quote a single SQL name
 *
 * buffer must be MAX_QUOTED_NAME_LEN long (includes room for \0)
 */
unsafe fn quoteOneName(mut buffer: *mut c_char, mut name: *const c_char) {
    /* Rather than trying to be smart, just always quote it. */
    *buffer = b'"' as c_char;
    buffer = buffer.add(1);
    while *name != 0 {
        if *name == b'"' as c_char {
            *buffer = b'"' as c_char;
            buffer = buffer.add(1);
        }
        *buffer = *name;
        buffer = buffer.add(1);
        name = name.add(1);
    }
    *buffer = b'"' as c_char;
    buffer = buffer.add(1);
    *buffer = b'\0' as c_char;
}

/*
 * quoteRelationName --- safely quote a fully qualified relation name
 *
 * buffer must be MAX_QUOTED_REL_NAME_LEN long (includes room for \0)
 */
unsafe fn quoteRelationName(mut buffer: *mut c_char, rel: Relation) {
    quoteOneName(buffer, get_namespace_name(RelationGetNamespace(rel)));
    buffer = buffer.add(strlen(buffer));
    *buffer = b'.' as c_char;
    buffer = buffer.add(1);
    quoteOneName(buffer, RelationGetRelationName(rel));
}

/*
 * ri_GenerateQual --- generate a WHERE clause equating two variables
 *
 * This basically appends " sep leftop op rightop" to buf, adding casts
 * and schema qualification as needed to ensure that the parser will select
 * the operator we specify.  leftop and rightop should be parenthesized
 * if they aren't variables or parameters.
 */
unsafe fn ri_GenerateQual(
    buf: StringInfo,
    sep: *const c_char,
    leftop: *const c_char,
    leftoptype: Oid,
    opoid: Oid,
    rightop: *const c_char,
    rightoptype: Oid,
) {
    appendStringInfo!(buf, " {} ", cstr(sep));
    generate_operator_clause(buf, leftop, leftoptype, opoid,
                             rightop, rightoptype);
}

/*
 * ri_GenerateQualCollation --- add a COLLATE spec to a WHERE clause
 *
 * (see C source for full commentary)
 */
unsafe fn ri_GenerateQualCollation(buf: StringInfo, collation: Oid) {
    let tp: HeapTuple;
    let colltup: Form_pg_collation;
    let collname: *mut c_char;
    let mut onename: [c_char; MAX_QUOTED_NAME_LEN] = [0; MAX_QUOTED_NAME_LEN];

    /* Nothing to do if it's a noncollatable data type */
    if !OidIsValid(collation) {
        return;
    }

    tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for collation {}", collation);
    }
    colltup = GETSTRUCT(tp) as Form_pg_collation;
    collname = NameStr(&raw mut (*colltup).collname);

    /*
     * We qualify the name always, for simplicity and to ensure the query is
     * not search-path-dependent.
     */
    quoteOneName(onename.as_mut_ptr(), get_namespace_name((*colltup).collnamespace));
    appendStringInfo!(buf, " COLLATE {}", cstr(onename.as_ptr()));
    quoteOneName(onename.as_mut_ptr(), collname);
    appendStringInfo!(buf, ".{}", cstr(onename.as_ptr()));

    ReleaseSysCache(tp);
}

/* ----------
 * ri_BuildQueryKey -
 *
 *	Construct a hashtable key for a prepared SPI plan of an FK constraint.
 *
 *		key: output argument, *key is filled in based on the other arguments
 *		riinfo: info derived from pg_constraint entry
 *		constr_queryno: an internal number identifying the query type
 *			(see RI_PLAN_XXX constants at head of file)
 * ----------
 */
unsafe fn ri_BuildQueryKey(key: *mut RI_QueryKey, riinfo: *const RI_ConstraintInfo, constr_queryno: int32) {
    /*
     * Inherited constraints with a common ancestor can share ri_query_cache
     * entries for all query types except RI_PLAN_CHECK_LOOKUPPK_FROM_PK.
     * (see C source for full commentary)
     */
    if constr_queryno != RI_PLAN_CHECK_LOOKUPPK_FROM_PK {
        (*key).constr_id = (*riinfo).constraint_root_id;
    } else {
        (*key).constr_id = (*riinfo).constraint_id;
    }
    (*key).constr_queryno = constr_queryno;
}

/*
 * Check that RI trigger function was called in expected context
 */
unsafe fn ri_CheckTrigger(fcinfo: FunctionCallInfo, funcname: *const c_char, tgkind: c_int) {
    let trigdata: *mut TriggerData = (*fcinfo).context as *mut TriggerData;

    if !CALLED_AS_TRIGGER(fcinfo) {
        ereport!(ERROR,
                 errmsg!("function \"{}\" was not called by trigger manager",
                         cstr(funcname)));
    }

    /*
     * Check proper event
     */
    if !TRIGGER_FIRED_AFTER((*trigdata).tg_event) ||
        !TRIGGER_FIRED_FOR_ROW((*trigdata).tg_event) {
        ereport!(ERROR,
                 errmsg!("function \"{}\" must be fired AFTER ROW",
                         cstr(funcname)));
    }

    match tgkind {
        RI_TRIGTYPE_INSERT => {
            if !TRIGGER_FIRED_BY_INSERT((*trigdata).tg_event) {
                ereport!(ERROR,
                         errmsg!("function \"{}\" must be fired for INSERT",
                                 cstr(funcname)));
            }
        }
        RI_TRIGTYPE_UPDATE => {
            if !TRIGGER_FIRED_BY_UPDATE((*trigdata).tg_event) {
                ereport!(ERROR,
                         errmsg!("function \"{}\" must be fired for UPDATE",
                                 cstr(funcname)));
            }
        }
        RI_TRIGTYPE_DELETE => {
            if !TRIGGER_FIRED_BY_DELETE((*trigdata).tg_event) {
                ereport!(ERROR,
                         errmsg!("function \"{}\" must be fired for DELETE",
                                 cstr(funcname)));
            }
        }
        _ => {}
    }
}


/*
 * Fetch the RI_ConstraintInfo struct for the trigger's FK constraint.
 */
unsafe fn ri_FetchConstraintInfo(trigger: *mut Trigger, trig_rel: Relation, rel_is_pk: bool) -> *const RI_ConstraintInfo {
    let constraintOid: Oid = (*trigger).tgconstraint;
    let riinfo: *const RI_ConstraintInfo;

    /*
     * Check that the FK constraint's OID is available; it might not be if
     * we've been invoked via an ordinary trigger or an old-style "constraint
     * trigger".
     */
    if !OidIsValid(constraintOid) {
        ereport!(ERROR,
                 errmsg!("no pg_constraint entry for trigger \"{}\" on table \"{}\"",
                         cstr((*trigger).tgname), cstr(RelationGetRelationName(trig_rel))));
    }

    /* Find or create a hashtable entry for the constraint */
    riinfo = ri_LoadConstraintInfo(constraintOid);

    /* Do some easy cross-checks against the trigger call data */
    if rel_is_pk {
        if (*riinfo).fk_relid != (*trigger).tgconstrrelid ||
            (*riinfo).pk_relid != RelationGetRelid(trig_rel) {
            elog!(ERROR, "wrong pg_constraint entry for trigger \"{}\" on table \"{}\"",
                  cstr((*trigger).tgname), cstr(RelationGetRelationName(trig_rel)));
        }
    } else {
        if (*riinfo).fk_relid != RelationGetRelid(trig_rel) ||
            (*riinfo).pk_relid != (*trigger).tgconstrrelid {
            elog!(ERROR, "wrong pg_constraint entry for trigger \"{}\" on table \"{}\"",
                  cstr((*trigger).tgname), cstr(RelationGetRelationName(trig_rel)));
        }
    }

    if (*riinfo).confmatchtype != FKCONSTR_MATCH_FULL &&
        (*riinfo).confmatchtype != FKCONSTR_MATCH_PARTIAL &&
        (*riinfo).confmatchtype != FKCONSTR_MATCH_SIMPLE {
        elog!(ERROR, "unrecognized confmatchtype: {}", (*riinfo).confmatchtype as c_int);
    }

    if (*riinfo).confmatchtype == FKCONSTR_MATCH_PARTIAL {
        ereport!(ERROR, errmsg!("MATCH PARTIAL not yet implemented"));
    }

    riinfo
}

/*
 * Fetch or create the RI_ConstraintInfo struct for an FK constraint.
 */
unsafe fn ri_LoadConstraintInfo(constraintOid: Oid) -> *const RI_ConstraintInfo {
    let riinfo: *mut RI_ConstraintInfo;
    let mut found: bool = false;
    let tup: HeapTuple;
    let conForm: Form_pg_constraint;

    /*
     * On the first call initialize the hashtable
     */
    if ri_constraint_cache.is_null() {
        ri_InitHashTables();
    }

    /*
     * Find or create a hash entry.  If we find a valid one, just return it.
     */
    riinfo = hash_search(ri_constraint_cache,
                         &constraintOid as *const Oid as *const c_void,
                         HASH_ENTER, &mut found) as *mut RI_ConstraintInfo;
    if !found {
        (*riinfo).valid = false;
    } else if (*riinfo).valid {
        return riinfo;
    }

    /*
     * Fetch the pg_constraint row so we can fill in the entry.
     */
    tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintOid));
    if !HeapTupleIsValid(tup) {
        /* should not happen */
        elog!(ERROR, "cache lookup failed for constraint {}", constraintOid);
    }
    conForm = GETSTRUCT(tup) as Form_pg_constraint;

    if (*conForm).contype != CONSTRAINT_FOREIGN {
        /* should not happen */
        elog!(ERROR, "constraint {} is not a foreign key constraint", constraintOid);
    }

    /* And extract data */
    Assert!((*riinfo).constraint_id == constraintOid);
    if OidIsValid((*conForm).conparentid) {
        (*riinfo).constraint_root_id = get_ri_constraint_root((*conForm).conparentid);
    } else {
        (*riinfo).constraint_root_id = constraintOid;
    }
    (*riinfo).oidHashValue = GetSysCacheHashValue1(CONSTROID, ObjectIdGetDatum(constraintOid));
    (*riinfo).rootHashValue = GetSysCacheHashValue1(CONSTROID, ObjectIdGetDatum((*riinfo).constraint_root_id));
    std::ptr::copy_nonoverlapping(&raw const (*conForm).conname, &raw mut (*riinfo).conname, 1);
    (*riinfo).pk_relid = (*conForm).confrelid;
    (*riinfo).fk_relid = (*conForm).conrelid;
    (*riinfo).confupdtype = (*conForm).confupdtype;
    (*riinfo).confdeltype = (*conForm).confdeltype;
    (*riinfo).confmatchtype = (*conForm).confmatchtype;
    (*riinfo).hasperiod = (*conForm).conperiod;

    DeconstructFkConstraintRow(tup,
                               &raw mut (*riinfo).nkeys,
                               (*riinfo).fk_attnums.as_mut_ptr(),
                               (*riinfo).pk_attnums.as_mut_ptr(),
                               (*riinfo).pf_eq_oprs.as_mut_ptr(),
                               (*riinfo).pp_eq_oprs.as_mut_ptr(),
                               (*riinfo).ff_eq_oprs.as_mut_ptr(),
                               &raw mut (*riinfo).ndelsetcols,
                               (*riinfo).confdelsetcols.as_mut_ptr());

    /*
     * For temporal FKs, get the operators and functions we need. We ask the
     * opclass of the PK element for these. This all gets cached (as does the
     * generated plan), so there's no performance issue.
     */
    if (*riinfo).hasperiod {
        let opclass: Oid = get_index_column_opclass((*conForm).conindid, (*riinfo).nkeys);

        FindFKPeriodOpers(opclass,
                          &raw mut (*riinfo).period_contained_by_oper,
                          &raw mut (*riinfo).agged_period_contained_by_oper,
                          &raw mut (*riinfo).period_intersect_oper);
    }

    ReleaseSysCache(tup);

    /*
     * For efficient processing of invalidation messages below, we keep a
     * doubly-linked count list of all currently valid entries.
     */
    dclist_push_tail(&raw mut ri_constraint_cache_valid_list, &raw mut (*riinfo).valid_link);

    (*riinfo).valid = true;

    riinfo
}

/*
 * get_ri_constraint_root
 *		Returns the OID of the constraint's root parent
 */
unsafe fn get_ri_constraint_root(mut constrOid: Oid) -> Oid {
    loop {
        let tuple: HeapTuple;
        let constrParentOid: Oid;

        tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constrOid));
        if !HeapTupleIsValid(tuple) {
            elog!(ERROR, "cache lookup failed for constraint {}", constrOid);
        }
        constrParentOid = (*(GETSTRUCT(tuple) as Form_pg_constraint)).conparentid;
        ReleaseSysCache(tuple);
        if !OidIsValid(constrParentOid) {
            break; /* we reached the root constraint */
        }
        constrOid = constrParentOid;
    }
    constrOid
}

/*
 * Callback for pg_constraint inval events
 *
 * (see C source for full commentary)
 */
unsafe fn InvalidateConstraintCacheCallBack(arg: Datum, cacheid: c_int, hashvalue: uint32) {
    let mut iter: dlist_mutable_iter = std::mem::zeroed();
    let mut hashvalue = hashvalue;

    Assert!(!ri_constraint_cache.is_null());

    /*
     * If the list of currently valid entries gets excessively large, we mark
     * them all invalid so we can empty the list.  This arrangement avoids
     * O(N^2) behavior in situations where a session touches many foreign keys
     * and also does many ALTER TABLEs, such as a restore from pg_dump.
     */
    if dclist_count(&raw const ri_constraint_cache_valid_list) > 1000 {
        hashvalue = 0; /* pretend it's a cache reset */
    }

    dclist_foreach_modify(&mut iter, &raw mut ri_constraint_cache_valid_list, |cur| {
        let riinfo: *mut RI_ConstraintInfo = dclist_container_RI_ConstraintInfo_valid_link(cur);

        /*
         * We must invalidate not only entries directly matching the given
         * hash value, but also child entries, in case the invalidation
         * affects a root constraint.
         */
        if hashvalue == 0 ||
            (*riinfo).oidHashValue == hashvalue ||
            (*riinfo).rootHashValue == hashvalue {
            (*riinfo).valid = false;
            /* Remove invalidated entries from the list, too */
            dclist_delete_from(&raw mut ri_constraint_cache_valid_list, cur);
        }
    });
}


/*
 * Prepare execution plan for a query to enforce an RI restriction
 */
unsafe fn ri_PlanCheck(
    querystr: *const c_char,
    nargs: c_int,
    argtypes: *mut Oid,
    qkey: *mut RI_QueryKey,
    fk_rel: Relation,
    pk_rel: Relation,
) -> SPIPlanPtr {
    let qplan: SPIPlanPtr;
    let query_rel: Relation;
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;

    /*
     * Use the query type code to determine whether the query is run against
     * the PK or FK table; we'll do the check as that table's owner
     */
    if (*qkey).constr_queryno <= RI_PLAN_LAST_ON_PK {
        query_rel = pk_rel;
    } else {
        query_rel = fk_rel;
    }

    /* Switch to proper UID to perform check as */
    GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
    SetUserIdAndSecContext((*RelationGetForm(query_rel)).relowner,
                           save_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_NOFORCE_RLS);

    /* Create the plan */
    qplan = SPI_prepare(querystr, nargs, argtypes);

    if qplan.is_null() {
        elog!(ERROR, "SPI_prepare returned {} for {}",
              cstr(SPI_result_code_string(SPI_result)), cstr(querystr));
    }

    /* Restore UID and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    /* Save the plan */
    SPI_keepplan(qplan);
    ri_HashPreparedPlan(qkey, qplan);

    qplan
}

/*
 * Perform a query to enforce an RI restriction
 */
unsafe fn ri_PerformCheck(
    riinfo: *const RI_ConstraintInfo,
    qkey: *mut RI_QueryKey,
    qplan: SPIPlanPtr,
    fk_rel: Relation,
    pk_rel: Relation,
    oldslot: *mut TupleTableSlot,
    newslot: *mut TupleTableSlot,
    is_restrict: bool,
    detectNewRows: bool,
    expect_OK: c_int,
) -> bool {
    let query_rel: Relation;
    let source_rel: Relation;
    let source_is_pk: bool;
    let test_snapshot: Snapshot;
    let crosscheck_snapshot: Snapshot;
    let limit: c_int;
    let spi_result: c_int;
    let mut save_userid: Oid = 0;
    let mut save_sec_context: c_int = 0;
    let mut vals: [Datum; RI_MAX_NUMKEYS * 2] = [0; RI_MAX_NUMKEYS * 2];
    let mut nulls: [c_char; RI_MAX_NUMKEYS * 2] = [0; RI_MAX_NUMKEYS * 2];

    /*
     * Use the query type code to determine whether the query is run against
     * the PK or FK table; we'll do the check as that table's owner
     */
    if (*qkey).constr_queryno <= RI_PLAN_LAST_ON_PK {
        query_rel = pk_rel;
    } else {
        query_rel = fk_rel;
    }

    /*
     * The values for the query are taken from the table on which the trigger
     * is called - it is normally the other one with respect to query_rel. (see
     * C source for full commentary)
     */
    if (*qkey).constr_queryno == RI_PLAN_CHECK_LOOKUPPK {
        source_rel = fk_rel;
        source_is_pk = false;
    } else {
        source_rel = pk_rel;
        source_is_pk = true;
    }

    /* Extract the parameters to be passed into the query */
    if !newslot.is_null() {
        ri_ExtractValues(source_rel, newslot, riinfo, source_is_pk,
                         vals.as_mut_ptr(), nulls.as_mut_ptr());
        if !oldslot.is_null() {
            ri_ExtractValues(source_rel, oldslot, riinfo, source_is_pk,
                             vals.as_mut_ptr().add((*riinfo).nkeys as usize),
                             nulls.as_mut_ptr().add((*riinfo).nkeys as usize));
        }
    } else {
        ri_ExtractValues(source_rel, oldslot, riinfo, source_is_pk,
                         vals.as_mut_ptr(), nulls.as_mut_ptr());
    }

    /*
     * In READ COMMITTED mode, we just need to use an up-to-date regular
     * snapshot, and we will see all rows that could be interesting. (see C
     * source for full commentary)
     */
    if IsolationUsesXactSnapshot() && detectNewRows {
        CommandCounterIncrement(); /* be sure all my own work is visible */
        test_snapshot = GetLatestSnapshot();
        crosscheck_snapshot = GetTransactionSnapshot();
    } else {
        /* the default SPI behavior is okay */
        test_snapshot = InvalidSnapshot;
        crosscheck_snapshot = InvalidSnapshot;
    }

    /*
     * If this is a select query (e.g., for a 'no action' or 'restrict'
     * trigger), we only need to see if there is a single row in the table,
     * matching the key.  Otherwise, limit = 0 - because we want the query to
     * affect ALL the matching rows.
     */
    limit = if expect_OK == SPI_OK_SELECT { 1 } else { 0 };

    /* Switch to proper UID to perform check as */
    GetUserIdAndSecContext(&mut save_userid, &mut save_sec_context);
    SetUserIdAndSecContext((*RelationGetForm(query_rel)).relowner,
                           save_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_NOFORCE_RLS);

    /* Finally we can run the query. */
    spi_result = SPI_execute_snapshot(qplan,
                                      vals.as_mut_ptr(), nulls.as_mut_ptr(),
                                      test_snapshot, crosscheck_snapshot,
                                      false, false, limit);

    /* Restore UID and security context */
    SetUserIdAndSecContext(save_userid, save_sec_context);

    /* Check result */
    if spi_result < 0 {
        elog!(ERROR, "SPI_execute_snapshot returned {}", cstr(SPI_result_code_string(spi_result)));
    }

    if expect_OK >= 0 && spi_result != expect_OK {
        ereport!(ERROR,
                 errmsg!("referential integrity query on \"{}\" from constraint \"{}\" on \"{}\" gave unexpected result",
                         cstr(RelationGetRelationName(pk_rel)),
                         cstr(NameStr(&raw const (*riinfo).conname as *mut NameData)),
                         cstr(RelationGetRelationName(fk_rel))));
    }

    /* XXX wouldn't it be clearer to do this part at the caller? */
    if (*qkey).constr_queryno != RI_PLAN_CHECK_LOOKUPPK_FROM_PK &&
        expect_OK == SPI_OK_SELECT &&
        (SPI_processed == 0) == ((*qkey).constr_queryno == RI_PLAN_CHECK_LOOKUPPK) {
        ri_ReportViolation(riinfo,
                           pk_rel, fk_rel,
                           if !newslot.is_null() { newslot } else { oldslot },
                           std::ptr::null_mut(),
                           (*qkey).constr_queryno, is_restrict, false);
    }

    SPI_processed != 0
}

/*
 * Extract fields from a tuple into Datum/nulls arrays
 */
unsafe fn ri_ExtractValues(
    rel: Relation,
    slot: *mut TupleTableSlot,
    riinfo: *const RI_ConstraintInfo,
    rel_is_pk: bool,
    vals: *mut Datum,
    nulls: *mut c_char,
) {
    let attnums: *const int16;
    let mut isnull: bool = false;

    if rel_is_pk {
        attnums = (*riinfo).pk_attnums.as_ptr();
    } else {
        attnums = (*riinfo).fk_attnums.as_ptr();
    }

    let mut i = 0;
    while i < (*riinfo).nkeys {
        *vals.add(i as usize) = slot_getattr(slot, *attnums.add(i as usize) as c_int, &mut isnull);
        *nulls.add(i as usize) = if isnull { b'n' as c_char } else { b' ' as c_char };
        i += 1;
    }
}

/*
 * Produce an error report
 *
 * If the failed constraint was on insert/update to the FK table,
 * we want the key names and values extracted from there, and the error
 * message to look like 'key blah is not present in PK'.
 * Otherwise, the attr names and values come from the PK table and the
 * message looks like 'key blah is still referenced from FK'.
 */
unsafe fn ri_ReportViolation(
    riinfo: *const RI_ConstraintInfo,
    pk_rel: Relation,
    fk_rel: Relation,
    violatorslot: *mut TupleTableSlot,
    mut tupdesc: TupleDesc,
    queryno: c_int,
    is_restrict: bool,
    partgone: bool,
) -> ! {
    let mut key_names: StringInfoData = std::mem::zeroed();
    let mut key_values: StringInfoData = std::mem::zeroed();
    let onfk: bool;
    let attnums: *const int16;
    let rel_oid: Oid;
    let mut aclresult: AclResult;
    let mut has_perm: bool = true;

    /*
     * Determine which relation to complain about.  If tupdesc wasn't passed
     * by caller, assume the violator tuple came from there.
     */
    onfk = queryno == RI_PLAN_CHECK_LOOKUPPK;
    if onfk {
        attnums = (*riinfo).fk_attnums.as_ptr();
        rel_oid = (*fk_rel).rd_id;
        if tupdesc.is_null() {
            tupdesc = (*fk_rel).rd_att;
        }
    } else {
        attnums = (*riinfo).pk_attnums.as_ptr();
        rel_oid = (*pk_rel).rd_id;
        if tupdesc.is_null() {
            tupdesc = (*pk_rel).rd_att;
        }
    }

    /*
     * Check permissions- if the user does not have access to view the data in
     * any of the key columns then we don't include the errdetail() below.
     * (see C source for full commentary)
     */
    if partgone {
        has_perm = true;
    } else if check_enable_rls(rel_oid, InvalidOid, true) != RLS_ENABLED {
        aclresult = pg_class_aclcheck(rel_oid, GetUserId(), ACL_SELECT);
        if aclresult != ACLCHECK_OK {
            /* Try for column-level permissions */
            let mut idx = 0;
            while idx < (*riinfo).nkeys {
                aclresult = pg_attribute_aclcheck(rel_oid, *attnums.add(idx as usize),
                                                  GetUserId(),
                                                  ACL_SELECT);

                /* No access to the key */
                if aclresult != ACLCHECK_OK {
                    has_perm = false;
                    break;
                }
                idx += 1;
            }
        }
    } else {
        has_perm = false;
    }

    if has_perm {
        /* Get printable versions of the keys involved */
        initStringInfo(&mut key_names);
        initStringInfo(&mut key_values);
        let mut idx = 0;
        while idx < (*riinfo).nkeys {
            let fnum: c_int = *attnums.add(idx as usize) as c_int;
            let att: Form_pg_attribute = TupleDescAttr(tupdesc, (fnum - 1) as usize);
            let name: *mut c_char;
            let val: *mut c_char;
            let datum: Datum;
            let mut isnull: bool = false;

            name = NameStr(&raw mut (*att).attname);

            datum = slot_getattr(violatorslot, fnum, &mut isnull);
            if !isnull {
                let mut foutoid: Oid = 0;
                let mut typisvarlena: bool = false;

                getTypeOutputInfo((*att).atttypid, &mut foutoid, &mut typisvarlena);
                val = OidOutputFunctionCall(foutoid, datum);
            } else {
                val = c"null".as_ptr() as *mut c_char;
            }

            if idx > 0 {
                appendStringInfoString(&mut key_names, c", ".as_ptr());
                appendStringInfoString(&mut key_values, c", ".as_ptr());
            }
            appendStringInfoString(&mut key_names, name);
            appendStringInfoString(&mut key_values, val);
            idx += 1;
        }
    }

    if partgone {
        ereport!(ERROR,
                 errmsg!("removing partition \"{}\" violates foreign key constraint \"{}\"",
                         cstr(RelationGetRelationName(pk_rel)),
                         cstr(NameStr(&raw const (*riinfo).conname as *mut NameData))));
    } else if onfk {
        ereport!(ERROR,
                 errmsg!("insert or update on table \"{}\" violates foreign key constraint \"{}\"",
                         cstr(RelationGetRelationName(fk_rel)),
                         cstr(NameStr(&raw const (*riinfo).conname as *mut NameData))));
    } else if is_restrict {
        ereport!(ERROR,
                 errmsg!("update or delete on table \"{}\" violates RESTRICT setting of foreign key constraint \"{}\" on table \"{}\"",
                         cstr(RelationGetRelationName(pk_rel)),
                         cstr(NameStr(&raw const (*riinfo).conname as *mut NameData)),
                         cstr(RelationGetRelationName(fk_rel))));
    } else {
        ereport!(ERROR,
                 errmsg!("update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"",
                         cstr(RelationGetRelationName(pk_rel)),
                         cstr(NameStr(&raw const (*riinfo).conname as *mut NameData)),
                         cstr(RelationGetRelationName(fk_rel))));
    }

    /* ereport!(ERROR, ...) is no-return; satisfy the `!` return type */
    unreachable!()
}


/*
 * ri_NullCheck -
 *
 * Determine the NULL state of all key values in a tuple
 *
 * Returns one of RI_KEYS_ALL_NULL, RI_KEYS_NONE_NULL or RI_KEYS_SOME_NULL.
 */
unsafe fn ri_NullCheck(
    tupDesc: TupleDesc,
    slot: *mut TupleTableSlot,
    riinfo: *const RI_ConstraintInfo,
    rel_is_pk: bool,
) -> c_int {
    let attnums: *const int16;
    let mut allnull: bool = true;
    let mut nonenull: bool = true;

    if rel_is_pk {
        attnums = (*riinfo).pk_attnums.as_ptr();
    } else {
        attnums = (*riinfo).fk_attnums.as_ptr();
    }

    let mut i = 0;
    while i < (*riinfo).nkeys {
        if slot_attisnull(slot, *attnums.add(i as usize) as c_int) {
            nonenull = false;
        } else {
            allnull = false;
        }
        i += 1;
    }

    if allnull {
        return RI_KEYS_ALL_NULL;
    }

    if nonenull {
        return RI_KEYS_NONE_NULL;
    }

    RI_KEYS_SOME_NULL
}


/*
 * ri_InitHashTables -
 *
 * Initialize our internal hash tables.
 */
unsafe fn ri_InitHashTables() {
    let mut ctl: HASHCTL = std::mem::zeroed();

    ctl.keysize = std::mem::size_of::<Oid>();
    ctl.entrysize = std::mem::size_of::<RI_ConstraintInfo>();
    ri_constraint_cache = hash_create(c"RI constraint cache".as_ptr(),
                                      RI_INIT_CONSTRAINTHASHSIZE,
                                      &mut ctl, HASH_ELEM | HASH_BLOBS);

    /* Arrange to flush cache on pg_constraint changes */
    CacheRegisterSyscacheCallback(CONSTROID,
                                  InvalidateConstraintCacheCallBack,
                                  0 as Datum);

    ctl.keysize = std::mem::size_of::<RI_QueryKey>();
    ctl.entrysize = std::mem::size_of::<RI_QueryHashEntry>();
    ri_query_cache = hash_create(c"RI query cache".as_ptr(),
                                 RI_INIT_QUERYHASHSIZE,
                                 &mut ctl, HASH_ELEM | HASH_BLOBS);

    ctl.keysize = std::mem::size_of::<RI_CompareKey>();
    ctl.entrysize = std::mem::size_of::<RI_CompareHashEntry>();
    ri_compare_cache = hash_create(c"RI compare cache".as_ptr(),
                                   RI_INIT_QUERYHASHSIZE,
                                   &mut ctl, HASH_ELEM | HASH_BLOBS);
}


/*
 * ri_FetchPreparedPlan -
 *
 * Lookup for a query key in our private hash table of prepared
 * and saved SPI execution plans. Return the plan if found or NULL.
 */
unsafe fn ri_FetchPreparedPlan(key: *mut RI_QueryKey) -> SPIPlanPtr {
    let entry: *mut RI_QueryHashEntry;
    let plan: SPIPlanPtr;

    /*
     * On the first call initialize the hashtable
     */
    if ri_query_cache.is_null() {
        ri_InitHashTables();
    }

    /*
     * Lookup for the key
     */
    entry = hash_search(ri_query_cache,
                        key as *const c_void,
                        HASH_FIND, std::ptr::null_mut()) as *mut RI_QueryHashEntry;
    if entry.is_null() {
        return std::ptr::null_mut();
    }

    /*
     * Check whether the plan is still valid.  If it isn't, we don't want to
     * simply rely on plancache.c to regenerate it; rather we should start
     * from scratch and rebuild the query text too.  This is to cover cases
     * such as table/column renames.  We depend on the plancache machinery to
     * detect possible invalidations, though.
     *
     * CAUTION: this check is only trustworthy if the caller has already
     * locked both FK and PK rels.
     */
    plan = (*entry).plan;
    if !plan.is_null() && SPI_plan_is_valid(plan) {
        return plan;
    }

    /*
     * Otherwise we might as well flush the cached plan now, to free a little
     * memory space before we make a new one.
     */
    (*entry).plan = std::ptr::null_mut();
    if !plan.is_null() {
        SPI_freeplan(plan);
    }

    std::ptr::null_mut()
}


/*
 * ri_HashPreparedPlan -
 *
 * Add another plan to our private SPI query plan hashtable.
 */
unsafe fn ri_HashPreparedPlan(key: *mut RI_QueryKey, plan: SPIPlanPtr) {
    let entry: *mut RI_QueryHashEntry;
    let mut found: bool = false;

    /*
     * On the first call initialize the hashtable
     */
    if ri_query_cache.is_null() {
        ri_InitHashTables();
    }

    /*
     * Add the new plan.  We might be overwriting an entry previously found
     * invalid by ri_FetchPreparedPlan.
     */
    entry = hash_search(ri_query_cache,
                        key as *const c_void,
                        HASH_ENTER, &mut found) as *mut RI_QueryHashEntry;
    Assert!(!found || (*entry).plan.is_null());
    (*entry).plan = plan;
}


/*
 * ri_KeysEqual -
 *
 * (see C source for full commentary)
 */
unsafe fn ri_KeysEqual(
    rel: Relation,
    oldslot: *mut TupleTableSlot,
    newslot: *mut TupleTableSlot,
    riinfo: *const RI_ConstraintInfo,
    rel_is_pk: bool,
) -> bool {
    let attnums: *const int16;

    if rel_is_pk {
        attnums = (*riinfo).pk_attnums.as_ptr();
    } else {
        attnums = (*riinfo).fk_attnums.as_ptr();
    }

    /* XXX: could be worthwhile to fetch all necessary attrs at once */
    let mut i = 0;
    while i < (*riinfo).nkeys {
        let oldvalue: Datum;
        let newvalue: Datum;
        let mut isnull: bool = false;

        /*
         * Get one attribute's oldvalue. If it is NULL - they're not equal.
         */
        oldvalue = slot_getattr(oldslot, *attnums.add(i as usize) as c_int, &mut isnull);
        if isnull {
            return false;
        }

        /*
         * Get one attribute's newvalue. If it is NULL - they're not equal.
         */
        newvalue = slot_getattr(newslot, *attnums.add(i as usize) as c_int, &mut isnull);
        if isnull {
            return false;
        }

        if rel_is_pk {
            /*
             * If we are looking at the PK table, then do a bytewise
             * comparison. (see C source for full commentary)
             */
            let att: *mut CompactAttribute = TupleDescCompactAttr((*oldslot).tts_tupleDescriptor, (*attnums.add(i as usize) - 1) as usize);

            if !datum_image_eq(oldvalue, newvalue, (*att).attbyval, (*att).attlen as c_int) {
                return false;
            }
        } else {
            let eq_opr: Oid;

            /*
             * When comparing the PERIOD columns we can skip the check
             * whenever the referencing column stayed equal or shrank, so test
             * with the contained-by operator instead.
             */
            if (*riinfo).hasperiod && i == (*riinfo).nkeys - 1 {
                eq_opr = (*riinfo).period_contained_by_oper;
            } else {
                eq_opr = (*riinfo).ff_eq_oprs[i as usize];
            }

            /*
             * For the FK table, compare with the appropriate equality
             * operator.  Changes that compare equal will still satisfy the
             * constraint after the update.
             */
            if !ri_CompareWithCast(eq_opr, RIAttType(rel, *attnums.add(i as usize)), RIAttCollation(rel, *attnums.add(i as usize)),
                                   newvalue, oldvalue) {
                return false;
            }
        }
        i += 1;
    }

    true
}


/*
 * ri_CompareWithCast -
 *
 * Call the appropriate comparison operator for two values.
 * Normally this is equality, but for the PERIOD part of foreign keys
 * it is ContainedBy, so the order of lhs vs rhs is significant.
 * See below for how the collation is applied.
 *
 * NB: we have already checked that neither value is null.
 */
unsafe fn ri_CompareWithCast(eq_opr: Oid, typeid: Oid, collid: Oid, mut lhs: Datum, mut rhs: Datum) -> bool {
    let entry: *mut RI_CompareHashEntry = ri_HashCompareOp(eq_opr, typeid);

    /* Do we need to cast the values? */
    if OidIsValid((*entry).cast_func_finfo.fn_oid) {
        lhs = FunctionCall3(&mut (*entry).cast_func_finfo,
                            lhs,
                            Int32GetDatum(-1),  /* typmod */
                            BoolGetDatum(false));   /* implicit coercion */
        rhs = FunctionCall3(&mut (*entry).cast_func_finfo,
                            rhs,
                            Int32GetDatum(-1),  /* typmod */
                            BoolGetDatum(false));   /* implicit coercion */
    }

    /*
     * Apply the comparison operator. (see C source for full commentary)
     */
    DatumGetBool(FunctionCall2Coll(&mut (*entry).eq_opr_finfo, collid, lhs, rhs))
}

/*
 * ri_HashCompareOp -
 *
 * See if we know how to compare two values, and create a new hash entry
 * if not.
 */
unsafe fn ri_HashCompareOp(eq_opr: Oid, typeid: Oid) -> *mut RI_CompareHashEntry {
    let mut key: RI_CompareKey = std::mem::zeroed();
    let entry: *mut RI_CompareHashEntry;
    let mut found: bool = false;

    /*
     * On the first call initialize the hashtable
     */
    if ri_compare_cache.is_null() {
        ri_InitHashTables();
    }

    /*
     * Find or create a hash entry.  Note we're assuming RI_CompareKey
     * contains no struct padding.
     */
    key.eq_opr = eq_opr;
    key.typeid = typeid;
    entry = hash_search(ri_compare_cache,
                        &mut key as *mut RI_CompareKey as *const c_void,
                        HASH_ENTER, &mut found) as *mut RI_CompareHashEntry;
    if !found {
        (*entry).valid = false;
    }

    /*
     * If not already initialized, do so.  Since we'll keep this hash entry
     * for the life of the backend, put any subsidiary info for the function
     * cache structs into TopMemoryContext.
     */
    if !(*entry).valid {
        let mut lefttype: Oid = 0;
        let mut righttype: Oid = 0;
        let mut castfunc: Oid = 0;
        let pathtype: CoercionPathType;

        /* We always need to know how to call the equality operator */
        fmgr_info_cxt(get_opcode(eq_opr), &mut (*entry).eq_opr_finfo, TopMemoryContext);

        /*
         * If we chose to use a cast from FK to PK type, we may have to apply
         * the cast function to get to the operator's input type. (see C source
         * for full commentary)
         */
        op_input_types(eq_opr, &mut lefttype, &mut righttype);
        Assert!(lefttype == righttype);
        if typeid == lefttype {
            castfunc = InvalidOid; /* simplest case */
        } else {
            pathtype = find_coercion_pathway(lefttype, typeid,
                                             COERCION_IMPLICIT,
                                             &mut castfunc);
            if pathtype != COERCION_PATH_FUNC &&
                pathtype != COERCION_PATH_RELABELTYPE {
                /*
                 * The declared input type of the eq_opr might be a
                 * polymorphic type such as ANYARRAY or ANYENUM, or other
                 * special cases such as RECORD; find_coercion_pathway
                 * currently doesn't subsume these special cases.
                 */
                if !IsBinaryCoercible(typeid, lefttype) {
                    elog!(ERROR, "no conversion function from {} to {}",
                          cstr(format_type_be(typeid)),
                          cstr(format_type_be(lefttype)));
                }
            }
        }
        if OidIsValid(castfunc) {
            fmgr_info_cxt(castfunc, &mut (*entry).cast_func_finfo, TopMemoryContext);
        } else {
            (*entry).cast_func_finfo.fn_oid = InvalidOid;
        }
        (*entry).valid = true;
    }

    entry
}


/*
 * Given a trigger function OID, determine whether it is an RI trigger,
 * and if so whether it is attached to PK or FK relation.
 */
pub unsafe fn RI_FKey_trigger_type(tgfoid: Oid) -> c_int {
    match tgfoid {
        F_RI_FKEY_CASCADE_DEL |
        F_RI_FKEY_CASCADE_UPD |
        F_RI_FKEY_RESTRICT_DEL |
        F_RI_FKEY_RESTRICT_UPD |
        F_RI_FKEY_SETNULL_DEL |
        F_RI_FKEY_SETNULL_UPD |
        F_RI_FKEY_SETDEFAULT_DEL |
        F_RI_FKEY_SETDEFAULT_UPD |
        F_RI_FKEY_NOACTION_DEL |
        F_RI_FKEY_NOACTION_UPD => RI_TRIGGER_PK,

        F_RI_FKEY_CHECK_INS |
        F_RI_FKEY_CHECK_UPD => RI_TRIGGER_FK,

        _ => RI_TRIGGER_NONE,
    }
}


/* ===========================================================================
 * Local helper shims and stubs for symbols that have no home in the port yet.
 * Each carries a TODO(pg-port) pointing at the real C source.
 * ===========================================================================
 */

/* Helpers that wrap C string pointers for use in format!()-based macros. */
#[inline]
unsafe fn cstr<'a>(p: *const c_char) -> std::borrow::Cow<'a, str> {
    if p.is_null() {
        std::borrow::Cow::Borrowed("(null)")
    } else {
        std::ffi::CStr::from_ptr(p).to_string_lossy()
    }
}

/* sprintf(paramname, "$%d", n) */
#[inline]
unsafe fn sprintf_param(buf: *mut c_char, n: c_int) {
    let s = format!("${}\0", n);
    std::ptr::copy_nonoverlapping(s.as_ptr() as *const c_char, buf, s.len());
}

/* snprintf(buf, size, "%d", v) */
#[inline]
unsafe fn snprintf_int(buf: *mut c_char, _size: usize, v: c_int) {
    let s = format!("{}\0", v);
    std::ptr::copy_nonoverlapping(s.as_ptr() as *const c_char, buf, s.len());
}

/* strcpy of a NUL-terminated literal into buf */
#[inline]
unsafe fn strcpy_lit(buf: *mut c_char, src: *const c_char) {
    let mut i = 0isize;
    loop {
        let ch = *src.offset(i);
        *buf.offset(i) = ch;
        if ch == 0 {
            break;
        }
        i += 1;
    }
}

/* int16/int32/uint32/c_long come from the prelude (crate::c::* / core::ffi). */

/* ----- Catalog constants ----- */
// TODO(pg-port): real values live in catalog/pg_constraint.h
const FKCONSTR_MATCH_FULL: c_char = b'f' as c_char;
const FKCONSTR_MATCH_PARTIAL: c_char = b'p' as c_char;
const FKCONSTR_MATCH_SIMPLE: c_char = b's' as c_char;
const CONSTRAINT_FOREIGN: c_char = b'f' as c_char;

// TODO(pg-port): real value lives in catalog/pg_class.h
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;

// TODO(pg-port): real OIDs live in catalog/pg_type_d.h
const ANYMULTIRANGEOID: Oid = 4537;

// TODO(pg-port): real value lives in access/sysattr.h
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;

/* ----- fmgr OIDs for RI trigger functions (utils/fmgroids.h) ----- */
// TODO(pg-port): real OIDs live in utils/fmgroids.h (generated)
const F_RI_FKEY_CHECK_INS: Oid = 1644;
const F_RI_FKEY_CHECK_UPD: Oid = 1645;
const F_RI_FKEY_CASCADE_DEL: Oid = 1646;
const F_RI_FKEY_CASCADE_UPD: Oid = 1647;
const F_RI_FKEY_RESTRICT_DEL: Oid = 1648;
const F_RI_FKEY_RESTRICT_UPD: Oid = 1649;
const F_RI_FKEY_SETNULL_DEL: Oid = 1650;
const F_RI_FKEY_SETNULL_UPD: Oid = 1651;
const F_RI_FKEY_SETDEFAULT_DEL: Oid = 1652;
const F_RI_FKEY_SETDEFAULT_UPD: Oid = 1653;
const F_RI_FKEY_NOACTION_DEL: Oid = 1654;
const F_RI_FKEY_NOACTION_UPD: Oid = 1655;

/* RI_FKey_trigger_type return codes (commands/trigger.h) */
// TODO(pg-port): real values live in commands/trigger.h
pub const RI_TRIGGER_PK: c_int = 1;
pub const RI_TRIGGER_FK: c_int = 2;
pub const RI_TRIGGER_NONE: c_int = 0;

/* ----- Lock modes (storage/lockdefs.h) ----- */
// TODO(pg-port): real values live in storage/lockdefs.h
const AccessShareLock: c_int = 1;
const RowShareLock: c_int = 2;
const RowExclusiveLock: c_int = 3;

/* ----- ACL bits & results (utils/acl.h / nodes/parsenodes.h) ----- */
// TODO(pg-port): real values live in nodes/parsenodes.h and utils/acl.h
const ACL_SELECT: AclMode = 1 << 1;
type AclMode = u64;
#[repr(C)]
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum AclResult {
    ACLCHECK_OK = 0,
    ACLCHECK_NO_PRIV = 1,
    ACLCHECK_NOT_OWNER = 2,
}
use AclResult::*;

/* ----- Security context flags (miscadmin.h) ----- */
// TODO(pg-port): real values live in miscadmin.h
const SECURITY_LOCAL_USERID_CHANGE: c_int = 0x0001;
const SECURITY_NOFORCE_RLS: c_int = 0x0004;

/* ----- GUC enums (utils/guc.h) ----- */
// TODO(pg-port): real values live in utils/guc.h
const PGC_USERSET: c_int = 7;
const PGC_S_SESSION: c_int = 16;
const GUC_ACTION_SAVE: c_int = 1;

/* ----- RLS result (utils/rls.h) ----- */
// TODO(pg-port): real value lives in utils/rls.h
const RLS_ENABLED: c_int = 2;

/* ----- Coercion (parser/parse_coerce.h) ----- */
// TODO(pg-port): real definitions live in parser/parse_coerce.h
#[repr(C)]
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum CoercionPathType {
    COERCION_PATH_NONE = 0,
    COERCION_PATH_FUNC = 1,
    COERCION_PATH_RELABELTYPE = 2,
    COERCION_PATH_ARRAYCOERCE = 3,
    COERCION_PATH_COERCEVIAIO = 4,
}
use CoercionPathType::*;
const COERCION_IMPLICIT: c_int = 2;

/* ----- RTE kind (nodes/parsenodes.h) ----- */
// TODO(pg-port): real value lives in nodes/parsenodes.h
const RTE_RELATION: c_int = 0;

/* ----- Syscache ids (utils/syscache.h) ----- */
// TODO(pg-port): real values live in utils/syscache.h (generated)
const CONSTROID: c_int = 0;
const COLLOID: c_int = 0;

/* ----- Snapshot constants ----- */
// TODO(pg-port): real values live in utils/snapmgr.h / utils/snapshot.h
const InvalidSnapshot: Snapshot = std::ptr::null_mut();

/* ----- SPI result codes & API (executor/spi.h) ----- */
// TODO(pg-port): real SPI lives in executor/spi.c; src/executor/spi.rs not yet ported.
pub type SPIPlanPtr = *mut c_void;
const SPI_OK_FINISH: c_int = 5;
const SPI_OK_SELECT: c_int = 5;
const SPI_OK_DELETE: c_int = 7;
const SPI_OK_UPDATE: c_int = 8;

#[repr(C)]
pub struct SPITupleTable {
    pub tupdesc: TupleDesc,
    pub vals: *mut HeapTuple,
}

#[allow(non_upper_case_globals)]
static mut SPI_processed: u64 = 0;
#[allow(non_upper_case_globals)]
static mut SPI_result: c_int = 0;
#[allow(non_upper_case_globals)]
static mut SPI_tuptable: *mut SPITupleTable = std::ptr::null_mut();

unsafe fn SPI_connect() -> c_int { 0 }
unsafe fn SPI_finish() -> c_int { SPI_OK_FINISH }
unsafe fn SPI_prepare(_src: *const c_char, _nargs: c_int, _argtypes: *mut Oid) -> SPIPlanPtr { std::ptr::null_mut() }
unsafe fn SPI_keepplan(_plan: SPIPlanPtr) -> c_int { 0 }
unsafe fn SPI_freeplan(_plan: SPIPlanPtr) -> c_int { 0 }
unsafe fn SPI_plan_is_valid(_plan: SPIPlanPtr) -> bool { false }
unsafe fn SPI_result_code_string(_code: c_int) -> *const c_char { c"".as_ptr() }
unsafe fn SPI_execute_snapshot(
    _plan: SPIPlanPtr, _values: *mut Datum, _nulls: *mut c_char,
    _snapshot: Snapshot, _crosscheck_snapshot: Snapshot,
    _read_only: bool, _fire_triggers: bool, _tcount: c_int,
) -> c_int { SPI_OK_SELECT }

/* ----- Snapshot manager (utils/snapmgr.h) ----- */
// TODO(pg-port): real Snapshot/funcs live in utils/snapmgr.{h,c}
pub type Snapshot = *mut c_void;
#[allow(non_upper_case_globals)]
static mut SnapshotSelf_storage: u8 = 0;
#[allow(non_upper_case_globals)]
static mut SnapshotSelf: Snapshot = std::ptr::null_mut();
unsafe fn GetLatestSnapshot() -> Snapshot { std::ptr::null_mut() }
unsafe fn GetTransactionSnapshot() -> Snapshot { std::ptr::null_mut() }
unsafe fn IsolationUsesXactSnapshot() -> bool { false }

/* ----- table access (access/table.h, access/tableam.h) ----- */
// TODO(pg-port): real funcs live in access/table.c & access/tableam.c
unsafe fn table_open(_relid: Oid, _lockmode: c_int) -> Relation { std::ptr::null_mut() }
unsafe fn table_close(_rel: Relation, _lockmode: c_int) {}
unsafe fn table_tuple_satisfies_snapshot(_rel: Relation, _slot: *mut TupleTableSlot, _snapshot: Snapshot) -> bool { true }

/* ----- TupleTableSlot / executor (executor/tuptable.h, executor/executor.h) ----- */
// TODO(pg-port): real types/funcs live in executor/tuptable.{h,c} & executor/execTuples.c
// TupleTableSlot unified to the real type (TriggerData.tg_newslot uses it).
pub use crate::executor::tuptable::TupleTableSlot;
#[repr(C)]
pub struct TupleTableSlotOps {
    _opaque: [u8; 0],
}
#[allow(non_upper_case_globals)]
static TTSOpsVirtual: TupleTableSlotOps = TupleTableSlotOps { _opaque: [] };
unsafe fn slot_getattr(_slot: *mut TupleTableSlot, _attnum: c_int, isnull: *mut bool) -> Datum { *isnull = false; 0 }
unsafe fn slot_attisnull(_slot: *mut TupleTableSlot, _attnum: c_int) -> bool { false }
unsafe fn slot_is_current_xact_tuple(_slot: *mut TupleTableSlot) -> bool { false }
unsafe fn MakeSingleTupleTableSlot(_tupdesc: TupleDesc, _ops: *const TupleTableSlotOps) -> *mut TupleTableSlot { std::ptr::null_mut() }
unsafe fn ExecStoreVirtualTuple(_slot: *mut TupleTableSlot) -> *mut TupleTableSlot { std::ptr::null_mut() }
unsafe fn ExecDropSingleTupleTableSlot(_slot: *mut TupleTableSlot) {}
unsafe fn ExecCheckPermissions(_rtes: *mut List, _perminfos: *mut List, _ereport_on_violation: bool) -> bool { true }

/* ----- HeapTuple / TupleDesc (real types imported at top) ----- */
// TODO(pg-port): heap_deform_tuple lives in access/common/heaptuple.c;
// TupleDescAttr / TupleDescCompactAttr live in access/tupdesc.h
#[repr(C)]
pub struct CompactAttribute {
    pub attbyval: bool,
    pub attlen: int16,
}
unsafe fn heap_deform_tuple(_tuple: HeapTuple, _tupdesc: TupleDesc, _values: *mut Datum, _isnull: *mut bool) {}
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: usize) -> Form_pg_attribute { std::ptr::null_mut() }
unsafe fn TupleDescCompactAttr(_tupdesc: TupleDesc, _i: usize) -> *mut CompactAttribute { std::ptr::null_mut() }

/* ----- Form_pg_collation (catalog/pg_collation.h) ----- */
// TODO(pg-port): real Form_pg_collation lives in catalog/pg_collation.h
#[repr(C)]
pub struct FormData_pg_collation {
    pub collname: NameData,
    pub collnamespace: Oid,
}
pub type Form_pg_collation = *mut FormData_pg_collation;

/* ----- NameData / NameStr (c.h) ----- */
// NameData comes via prelude (crate::c::*).  NameStr returns the first byte's address.
#[inline]
unsafe fn NameStr(name: *mut NameData) -> *mut c_char {
    name as *mut c_char
}

unsafe fn attnumAttName(_rel: Relation, _attnum: int16) -> *mut NameData { std::ptr::null_mut() }
unsafe fn attnumTypeId(_rel: Relation, _attnum: int16) -> Oid { InvalidOid }
unsafe fn attnumCollationId(_rel: Relation, _attnum: int16) -> Oid { InvalidOid }

/* ----- Relation accessors (utils/rel.h) ----- */
// RelationGetForm is imported from utils::rel; the rest are simple field reads
// against the real RelationData / Form_pg_class structs.
// TODO(pg-port): real macros/inlines live in utils/rel.h; mirror them here until exported.
unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc { (*rel).rd_att }
unsafe fn RelationGetRelid(rel: Relation) -> Oid { (*rel).rd_id }
unsafe fn RelationGetRelationName(rel: Relation) -> *const c_char {
    NameStr(&raw mut (*(*rel).rd_rel).relname)
}
unsafe fn RelationGetNamespace(rel: Relation) -> Oid { (*(*rel).rd_rel).relnamespace }

/* ----- List (real type imported at top; helpers stubbed) ----- */
// TODO(pg-port): lappend/list_length live in nodes/list.c
const NIL: *mut List = std::ptr::null_mut();
unsafe fn lappend(list: *mut List, _datum: *mut c_void) -> *mut List { list }
unsafe fn list_length(_list: *mut List) -> c_int { 0 }

/* ----- RangeTblEntry / RTEPermissionInfo (nodes/parsenodes.h) ----- */
// TODO(pg-port): real nodes live in nodes/parsenodes.rs
#[repr(C)]
pub struct RangeTblEntry {
    pub rtekind: c_int,
    pub relid: Oid,
    pub relkind: c_char,
    pub rellockmode: c_int,
    pub perminfoindex: c_int,
}
#[repr(C)]
pub struct RTEPermissionInfo {
    pub relid: Oid,
    pub requiredPerms: AclMode,
    pub selectedCols: *mut Bitmapset,
}

/* makeNode!(T) -> palloc0'd node tagged appropriately. */
// TODO(pg-port): real makeNode lives in nodes/nodes.h; stub allocates zeroed.
// Implemented as a generic fn (not a macro) so it can be called before this
// point in the file without macro-ordering constraints.
unsafe fn makeNode_alloc<T>() -> *mut T {
    palloc0(std::mem::size_of::<T>()) as *mut T
}

/* ----- Bitmapset (real type imported at top) ----- */
// TODO(pg-port): bms_add_member lives in nodes/bitmapset.c
unsafe fn bms_add_member(a: *mut Bitmapset, _x: c_int) -> *mut Bitmapset { a }

/* ----- dlist / dclist (lib/ilist.h) ----- */
// TODO(pg-port): real intrusive lists live in lib/ilist.rs
#[repr(C)]
#[derive(Clone, Copy)]
pub struct dlist_node {
    pub prev: *mut dlist_node,
    pub next: *mut dlist_node,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct dlist_head {
    pub head: dlist_node,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct dclist_head {
    pub head: dlist_head,
    pub count: u32,
}
#[repr(C)]
pub struct dlist_mutable_iter {
    pub cur: *mut dlist_node,
    pub next: *mut dlist_node,
    pub end: *mut dlist_node,
}
unsafe fn dclist_count(head: *const dclist_head) -> u32 { (*head).count }
unsafe fn dclist_push_tail(_head: *mut dclist_head, _node: *mut dlist_node) {}
unsafe fn dclist_delete_from(_head: *mut dclist_head, _node: *mut dlist_node) {}
/* dclist_foreach_modify(iter, head, body): iterate the valid list. */
unsafe fn dclist_foreach_modify<F: FnMut(*mut dlist_node)>(_iter: *mut dlist_mutable_iter, head: *mut dclist_head, mut body: F) {
    let mut cur = (*head).head.head.next;
    while !cur.is_null() && cur != &raw mut (*head).head.head {
        let next = (*cur).next;
        body(cur);
        cur = next;
    }
}
/* dclist_container(RI_ConstraintInfo, valid_link, node) */
unsafe fn dclist_container_RI_ConstraintInfo_valid_link(node: *mut dlist_node) -> *mut RI_ConstraintInfo {
    let offset = std::mem::offset_of!(RI_ConstraintInfo, valid_link);
    (node as *mut u8).sub(offset) as *mut RI_ConstraintInfo
}

/* ----- dynahash (utils/hash/dynahash.c) ----- */
// TODO(pg-port): real HTAB/hash_* live in utils/hash/dynahash.rs
#[repr(C)]
pub struct HTAB {
    _opaque: [u8; 0],
}
#[repr(C)]
pub struct HASHCTL {
    pub keysize: usize,
    pub entrysize: usize,
}
const HASH_ELEM: c_int = 0x0008;
const HASH_BLOBS: c_int = 0x0010;
const HASH_ENTER: c_int = 1;
const HASH_FIND: c_int = 0;
unsafe fn hash_create(_tabname: *const c_char, _nelem: c_long, _info: *mut HASHCTL, _flags: c_int) -> *mut HTAB { std::ptr::null_mut() }
unsafe fn hash_search(_htab: *mut HTAB, _key: *const c_void, _action: c_int, found: *mut bool) -> *mut c_void {
    if !found.is_null() { *found = false; }
    std::ptr::null_mut()
}

/* ----- syscache (utils/cache/syscache.c, utils/cache/inval.c) ----- */
// TODO(pg-port): real syscache lives in utils/cache/syscache.rs
type SyscacheCallbackFunction = unsafe fn(Datum, c_int, uint32);
unsafe fn SearchSysCache1(_cacheid: c_int, _key1: Datum) -> HeapTuple { std::ptr::null_mut() }
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {}
unsafe fn GetSysCacheHashValue1(_cacheid: c_int, _key1: Datum) -> uint32 { 0 }
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool { !tuple.is_null() }
unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void { std::ptr::null_mut() }
unsafe fn CacheRegisterSyscacheCallback(_cacheid: c_int, _func: SyscacheCallbackFunction, _arg: Datum) {}

/* ----- pg_constraint helpers (catalog/pg_constraint.c) ----- */
// TODO(pg-port): DeconstructFkConstraintRow lives in catalog/pg_constraint.c
unsafe fn DeconstructFkConstraintRow(
    _tuple: HeapTuple,
    _numfks: *mut c_int,
    _conkey: *mut int16,
    _confkey: *mut int16,
    _pf_eq_oprs: *mut Oid,
    _pp_eq_oprs: *mut Oid,
    _ff_eq_oprs: *mut Oid,
    _num_delete_set_cols: *mut c_int,
    _delete_set_cols: *mut int16,
) {}

/* ----- index/opclass & temporal-FK opers (catalog/index.c, utils/adt/rangetypes.c) ----- */
// TODO(pg-port): get_index_column_opclass lives in utils/cache/lsyscache.c
unsafe fn get_index_column_opclass(_index_oid: Oid, _attno: c_int) -> Oid { InvalidOid }
// TODO(pg-port): FindFKPeriodOpers lives in utils/adt/rangetypes.c
unsafe fn FindFKPeriodOpers(_opclass: Oid, _contained_by: *mut Oid, _agged_contained_by: *mut Oid, _intersect: *mut Oid) {}

/* ----- lsyscache / type output (utils/cache/lsyscache.c, utils/fmgr.c) ----- */
// TODO(pg-port): these live in utils/cache/lsyscache.c & utils/fmgr.c
unsafe fn get_opcode(_opno: Oid) -> Oid { InvalidOid }
unsafe fn op_input_types(_opno: Oid, lefttype: *mut Oid, righttype: *mut Oid) { *lefttype = InvalidOid; *righttype = InvalidOid; }
unsafe fn getTypeOutputInfo(_type: Oid, typoutput: *mut Oid, typisvarlena: *mut bool) { *typoutput = InvalidOid; *typisvarlena = false; }
unsafe fn OidOutputFunctionCall(_functionId: Oid, _val: Datum) -> *mut c_char { c"".as_ptr() as *mut c_char }
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char { c"".as_ptr() as *mut c_char }
unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char { c"".as_ptr() as *mut c_char }

/* ----- parse_coerce (parser/parse_coerce.c) ----- */
// TODO(pg-port): these live in parser/parse_coerce.c
unsafe fn find_coercion_pathway(_target: Oid, _source: Oid, _ccontext: c_int, funcid: *mut Oid) -> CoercionPathType { *funcid = InvalidOid; COERCION_PATH_NONE }
unsafe fn IsBinaryCoercible(_src: Oid, _target: Oid) -> bool { false }

/* ----- ruleutils (utils/adt/ruleutils.c) ----- */
// TODO(pg-port): these live in utils/adt/ruleutils.c
unsafe fn generate_operator_clause(_buf: StringInfo, _leftop: *const c_char, _leftoptype: Oid, _opoid: Oid, _rightop: *const c_char, _rightoptype: Oid) {}
unsafe fn pg_get_partconstrdef_string(_partitionId: Oid, _aliasname: *const c_char) -> *mut c_char { std::ptr::null_mut() }

/* ----- fmgr call helpers (utils/fmgr.c) ----- */
// TODO(pg-port): FunctionCall* live in utils/fmgr.c
unsafe fn fmgr_info_cxt(_functionId: Oid, _finfo: *mut FmgrInfo, _mcxt: MemoryContext) {}
unsafe fn FunctionCall2Coll(_flinfo: *mut FmgrInfo, _collation: Oid, _arg1: Datum, _arg2: Datum) -> Datum { 0 }
unsafe fn FunctionCall3(_flinfo: *mut FmgrInfo, _arg1: Datum, _arg2: Datum, _arg3: Datum) -> Datum { 0 }

/* ----- acl / rls / misc (utils/acl.c, utils/rls.c, miscadmin.c, access/xact.c) ----- */
// TODO(pg-port): these live across utils/acl.c, utils/rls.c, miscadmin.c, access/xact.c, catalog/aclchk.c
unsafe fn GetUserId() -> Oid { InvalidOid }
unsafe fn GetUserIdAndSecContext(userid: *mut Oid, sec_context: *mut c_int) { *userid = InvalidOid; *sec_context = 0; }
unsafe fn SetUserIdAndSecContext(_userid: Oid, _sec_context: c_int) {}
unsafe fn has_bypassrls_privilege(_roleid: Oid) -> bool { false }
unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool { false }
unsafe fn pg_class_aclcheck(_table_oid: Oid, _roleid: Oid, _mode: AclMode) -> AclResult { ACLCHECK_OK }
unsafe fn pg_attribute_aclcheck(_table_oid: Oid, _attnum: int16, _roleid: Oid, _mode: AclMode) -> AclResult { ACLCHECK_OK }
unsafe fn check_enable_rls(_relid: Oid, _checkAsUser: Oid, _noError: bool) -> c_int { 0 }
unsafe fn CommandCounterIncrement() {}

/* RelationRelationId (catalog/pg_class_d.h) */
// TODO(pg-port): real value lives in catalog/pg_class_d.h
const RelationRelationId: Oid = 1259;

/* ----- GUC (utils/guc.c) ----- */
// TODO(pg-port): these live in utils/guc.c & utils/guc_tables.c
#[allow(non_upper_case_globals)]
static mut maintenance_work_mem: c_int = 65536;
unsafe fn NewGUCNestLevel() -> c_int { 0 }
unsafe fn AtEOXact_GUC(_isCommit: bool, _nestLevel: c_int) {}
unsafe fn set_config_option(_name: *const c_char, _value: *const c_char, _context: c_int, _source: c_int, _action: c_int, _changeVal: bool, _elevel: c_int, _is_reload: bool) -> c_int { 0 }

/* ----- Datum helpers not in postgres.rs ----- */
// Int32GetDatum lives in postgres.h; provide if absent from prelude.
#[inline]
unsafe fn Int32GetDatum(x: int32) -> Datum { x as u32 as Datum }

/* ----- string.h ----- */
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 { n += 1; }
    n
}

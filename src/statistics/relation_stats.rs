//! relation_stats.c - PostgreSQL relation statistics manipulation.

use crate::prelude::*;

use core::ffi::c_short;

use crate::access::common::heaptuple::{heap_freetuple, heap_modify_tuple_by_cols};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::{HeapTuple, HeapTupleData, HeapTupleIsValid, GETSTRUCT};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog_oids::RelationRelationId;
use crate::catalog::pg_class::Form_pg_class;
use crate::nodes::makefuncs::makeRangeVar;
use crate::nodes::primnodes::RangeVar;
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::lockdefs::{RowExclusiveLock, ShareUpdateExclusiveLock, LOCKMODE};
use crate::utils::builtins::TextDatumGetCString;
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::rel::{Relation, RelationGetDescr};

use crate::{
    PG_ARGISNULL, PG_GETARG_DATUM, PG_GETARG_FLOAT4,
    PG_GETARG_UINT32, PG_RETURN_BOOL, PG_RETURN_VOID,
};
use crate::postgres::{Float4GetDatum, ObjectIdGetDatum, UInt32GetDatum};
use crate::{InitFunctionCallInfoData, LOCAL_FCINFO};

/* OID constants from fmgroids / catalog headers. */
const TEXTOID: Oid = 25;
const INT4OID: Oid = 23;
const FLOAT4OID: Oid = 700;

/* Syscache id for pg_class by relid. */
const RELOID: c_int = 57; // TODO: real RELOID once syscache.h ported

/* Attribute numbers from pg_class.h */
const Anum_pg_class_relpages: c_int = 28;
const Anum_pg_class_reltuples: c_int = 29;
const Anum_pg_class_relallvisible: c_int = 30;
const Anum_pg_class_relallfrozen: c_int = 31;

/*
 * struct StatsArgInfo from statistics/stat_utils.h.  Not yet ported there;
 * defined locally so relarginfo can be expressed.
 */
#[repr(C)]
pub struct StatsArgInfo {
    pub argname: *const c_char,
    pub argtype: Oid,
}

/*
 * Positional argument numbers, names, and types for
 * relation_statistics_update().
 */
const RELSCHEMA_ARG: usize = 0;
const RELNAME_ARG: usize = 1;
const RELPAGES_ARG: usize = 2;
const RELTUPLES_ARG: usize = 3;
const RELALLVISIBLE_ARG: usize = 4;
const RELALLFROZEN_ARG: usize = 5;
const NUM_RELATION_STATS_ARGS: usize = 6;

const relarginfo: [StatsArgInfo; NUM_RELATION_STATS_ARGS + 1] = [
    StatsArgInfo { argname: c"schemaname".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"relname".as_ptr(), argtype: TEXTOID },
    StatsArgInfo { argname: c"relpages".as_ptr(), argtype: INT4OID },
    StatsArgInfo { argname: c"reltuples".as_ptr(), argtype: FLOAT4OID },
    StatsArgInfo { argname: c"relallvisible".as_ptr(), argtype: INT4OID },
    StatsArgInfo { argname: c"relallfrozen".as_ptr(), argtype: INT4OID },
    StatsArgInfo { argname: null(), argtype: 0 },
];

/* ---- not-yet-ported callees, stubbed locally ---- */

// TODO: port statistics/stat_utils.c
unsafe fn stats_check_required_arg(
    _fcinfo: FunctionCallInfo,
    _arginfo: *const StatsArgInfo,
    _argnum: c_int,
) {
    unimplemented!()
}

// TODO: port statistics/stat_utils.c
unsafe fn stats_fill_fcinfo_from_arg_pairs(
    _pairs_fcinfo: FunctionCallInfo,
    _positional_fcinfo: FunctionCallInfo,
    _arginfo: *const StatsArgInfo,
) -> bool {
    unimplemented!()
}

// TODO: port statistics/stat_utils.c
unsafe fn RangeVarCallbackForStats(
    _relation: *const RangeVar,
    _rel_id: Oid,
    _old_relid: Oid,
    _arg: *mut c_void,
) {
    unimplemented!()
}

type RangeVarGetRelidCallback =
    unsafe fn(relation: *const RangeVar, relId: Oid, oldRelid: Oid, arg: *mut c_void);

// TODO: port catalog/namespace.c
unsafe fn RangeVarGetRelidExtended(
    _relation: *const RangeVar,
    _lockmode: LOCKMODE,
    _flags: u32,
    _callback: RangeVarGetRelidCallback,
    _callback_arg: *mut c_void,
) -> Oid {
    unimplemented!()
}

// TODO: port access/transam/xlog.c
unsafe fn RecoveryInProgress() -> bool {
    unimplemented!()
}

// TODO: port utils/cache/syscache.c
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!()
}

// TODO: port utils/cache/syscache.c
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!()
}

// TODO: port catalog/indexing.c
unsafe fn CatalogTupleUpdate(_heapRel: Relation, _otid: *mut ItemPointerData, _tup: HeapTuple) {
    unimplemented!()
}

// TODO: port access/transam/xact.c
unsafe fn CommandCounterIncrement() {
    unimplemented!()
}

/*
 * Internal function for modifying statistics for a relation.
 */
unsafe fn relation_statistics_update(fcinfo: FunctionCallInfo) -> bool {
    let mut result: bool = true;
    let nspname: *mut c_char;
    let relname: *mut c_char;
    let reloid: Oid;
    let crel: Relation;
    let mut relpages: BlockNumber = 0;
    let mut update_relpages: bool = false;
    let mut reltuples: f32 = 0.0;
    let mut update_reltuples: bool = false;
    let mut relallvisible: BlockNumber = 0;
    let mut update_relallvisible: bool = false;
    let mut relallfrozen: BlockNumber = 0;
    let mut update_relallfrozen: bool = false;
    let ctup: HeapTuple;
    let pgcform: Form_pg_class;
    let mut replaces: [c_int; 4] = [0; 4];
    let mut values: [Datum; 4] = [0; 4];
    let mut nulls: [bool; 4] = [false; 4];
    let mut nreplaces: c_int = 0;
    let mut locked_table: Oid = InvalidOid;

    stats_check_required_arg(fcinfo, relarginfo.as_ptr(), RELSCHEMA_ARG as c_int);
    stats_check_required_arg(fcinfo, relarginfo.as_ptr(), RELNAME_ARG as c_int);

    nspname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, RELSCHEMA_ARG));
    relname = TextDatumGetCString(PG_GETARG_DATUM!(fcinfo, RELNAME_ARG));

    if RecoveryInProgress() {
        ereport!(ERROR, "recovery is in progress");
    }

    reloid = RangeVarGetRelidExtended(
        makeRangeVar(nspname, relname, -1),
        ShareUpdateExclusiveLock,
        0,
        RangeVarCallbackForStats,
        &mut locked_table as *mut Oid as *mut c_void,
    );

    if !PG_ARGISNULL!(fcinfo, RELPAGES_ARG) {
        relpages = PG_GETARG_UINT32!(fcinfo, RELPAGES_ARG);
        update_relpages = true;
    }

    if !PG_ARGISNULL!(fcinfo, RELTUPLES_ARG) {
        reltuples = PG_GETARG_FLOAT4!(fcinfo, RELTUPLES_ARG);
        if reltuples < -1.0 {
            ereport!(
                WARNING,
                "argument \"reltuples\" must not be less than -1.0"
            );
            result = false;
        } else {
            update_reltuples = true;
        }
    }

    if !PG_ARGISNULL!(fcinfo, RELALLVISIBLE_ARG) {
        relallvisible = PG_GETARG_UINT32!(fcinfo, RELALLVISIBLE_ARG);
        update_relallvisible = true;
    }

    if !PG_ARGISNULL!(fcinfo, RELALLFROZEN_ARG) {
        relallfrozen = PG_GETARG_UINT32!(fcinfo, RELALLFROZEN_ARG);
        update_relallfrozen = true;
    }

    /*
     * Take RowExclusiveLock on pg_class, consistent with
     * vac_update_relstats().
     */
    crel = table_open(RelationRelationId, RowExclusiveLock);

    ctup = SearchSysCache1(RELOID, ObjectIdGetDatum(reloid));
    if !HeapTupleIsValid(ctup) {
        elog!(ERROR, "pg_class entry for relid {} not found", reloid);
    }

    pgcform = GETSTRUCT(ctup) as Form_pg_class;

    if update_relpages && relpages != (*pgcform).relpages as BlockNumber {
        replaces[nreplaces as usize] = Anum_pg_class_relpages;
        values[nreplaces as usize] = UInt32GetDatum(relpages);
        nreplaces += 1;
    }

    if update_reltuples && reltuples != (*pgcform).reltuples {
        replaces[nreplaces as usize] = Anum_pg_class_reltuples;
        values[nreplaces as usize] = Float4GetDatum(reltuples);
        nreplaces += 1;
    }

    if update_relallvisible && relallvisible != (*pgcform).relallvisible as BlockNumber {
        replaces[nreplaces as usize] = Anum_pg_class_relallvisible;
        values[nreplaces as usize] = UInt32GetDatum(relallvisible);
        nreplaces += 1;
    }

    if update_relallfrozen && relallfrozen != (*pgcform).relallfrozen as BlockNumber {
        replaces[nreplaces as usize] = Anum_pg_class_relallfrozen;
        values[nreplaces as usize] = UInt32GetDatum(relallfrozen);
        nreplaces += 1;
    }

    if nreplaces > 0 {
        let tupdesc: TupleDesc = RelationGetDescr(crel);
        let newtup: HeapTuple;

        newtup = heap_modify_tuple_by_cols(
            ctup,
            tupdesc,
            nreplaces,
            replaces.as_mut_ptr(),
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );
        CatalogTupleUpdate(crel, &mut (*newtup).t_self, newtup);
        heap_freetuple(newtup);
    }

    ReleaseSysCache(ctup);

    /* release the lock, consistent with vac_update_relstats() */
    table_close(crel, RowExclusiveLock);

    CommandCounterIncrement();

    result
}

/*
 * Clear statistics for a given pg_class entry; that is, set back to initial
 * stats for a newly-created table.
 */
pub unsafe fn pg_clear_relation_stats(fcinfo: FunctionCallInfo) -> Datum {
    LOCAL_FCINFO!(newfcinfo, 6);

    InitFunctionCallInfoData!(newfcinfo, null_mut(), 6 as c_short, InvalidOid, null_mut(), null_mut());

    (*(*newfcinfo).args.as_mut_ptr().add(0)).value = PG_GETARG_DATUM!(fcinfo, 0);
    (*(*newfcinfo).args.as_mut_ptr().add(0)).isnull = PG_ARGISNULL!(fcinfo, 0);
    (*(*newfcinfo).args.as_mut_ptr().add(1)).value = PG_GETARG_DATUM!(fcinfo, 1);
    (*(*newfcinfo).args.as_mut_ptr().add(1)).isnull = PG_ARGISNULL!(fcinfo, 1);
    (*(*newfcinfo).args.as_mut_ptr().add(2)).value = UInt32GetDatum(0);
    (*(*newfcinfo).args.as_mut_ptr().add(2)).isnull = false;
    (*(*newfcinfo).args.as_mut_ptr().add(3)).value = Float4GetDatum(-1.0);
    (*(*newfcinfo).args.as_mut_ptr().add(3)).isnull = false;
    (*(*newfcinfo).args.as_mut_ptr().add(4)).value = UInt32GetDatum(0);
    (*(*newfcinfo).args.as_mut_ptr().add(4)).isnull = false;
    (*(*newfcinfo).args.as_mut_ptr().add(5)).value = UInt32GetDatum(0);
    (*(*newfcinfo).args.as_mut_ptr().add(5)).isnull = false;

    relation_statistics_update(newfcinfo);
    PG_RETURN_VOID!();
}

pub unsafe fn pg_restore_relation_stats(fcinfo: FunctionCallInfo) -> Datum {
    LOCAL_FCINFO!(positional_fcinfo, NUM_RELATION_STATS_ARGS);
    let mut result: bool = true;

    InitFunctionCallInfoData!(
        positional_fcinfo,
        null_mut(),
        NUM_RELATION_STATS_ARGS as c_short,
        InvalidOid,
        null_mut(),
        null_mut()
    );

    if !stats_fill_fcinfo_from_arg_pairs(fcinfo, positional_fcinfo, relarginfo.as_ptr()) {
        result = false;
    }

    if !relation_statistics_update(positional_fcinfo) {
        result = false;
    }

    PG_RETURN_BOOL!(result);
}

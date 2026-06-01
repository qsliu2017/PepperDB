//! conflict.rs
//!    Support routines for logging conflicts.
//!
//! Translated 1:1 from postgres/src/backend/replication/logical/conflict.c
//!
//! Copyright (c) 2024-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/replication/logical/conflict.c
//!
//! This file contains the code for logging conflicts on the subscriber during
//! logical replication.
//!
//! The matching exports from replication/conflict.h (the ConflictType enum,
//! CONFLICT_NUM_TYPES, ConflictTupleInfo and the function prototypes) are
//! merged here, since Rust has no separate header files.

use crate::prelude::*;

// access/commit_ts.h
use crate::access::transam::commit_ts::{track_commit_timestamp, TransactionIdGetCommitTsData};
// access/tableam.h (table_slot_create)
use crate::access::table::tableam::table_slot_create;
// access/sysattr.h
use crate::access::sysattr::MinTransactionIdAttributeNumber;
// access/index/genam.h, indexam.h
use crate::access::index::genam::BuildIndexValueDescription;
use crate::access::index::indexam::{index_close, index_open};
// executor/executor.h, tuptable.h, execUtils.h
use crate::executor::execUtils::{ExecGetInsertedCols, ExecGetUpdatedCols};
use crate::executor::executor::ExecBuildSlotValueDescription;
use crate::executor::tuptable::{slot_getsysattr, ExecCopySlot, TupleTableSlot, TTS_IS_VIRTUAL};
// lib/stringinfo.h
use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoString, initStringInfo, StringInfo, StringInfoData,
};
// nodes/bitmapset.h
use crate::nodes::bitmapset::{bms_union, Bitmapset};
// nodes/execnodes.h
use crate::nodes::execnodes::{EState, ResultRelInfo};
// nodes/pg_list.h
use crate::nodes::pg_list::{lappend_oid, List};
// postgres.h: TransactionId, Datum helpers.
use crate::postgres::DatumGetTransactionId;
// replication/origin.h: RepOriginId / InvalidRepOriginId.
use crate::access::transam::xlogreader::{InvalidRepOriginId, RepOriginId};
// storage/lockdefs.h
use crate::storage::lockdefs::{NoLock, RowExclusiveLock};
// utils/activity/pgstat.h
use crate::utils::activity::pgstat_subscription::pgstat_report_subscription_conflict;
// utils/rel.h
use crate::utils::rel::{
    Relation, RelationGetDescr, RelationGetNamespace, RelationGetRelationName, RelationGetRelid,
};
// access/tupdesc.h
use crate::access::common::tupdesc::TupleDesc;
// pg_config_manual.h
use crate::pg_config_manual::INDEX_MAX_KEYS;

use crate::{appendStringInfo, ereport, errmsg, foreach_ptr, Assert};

// utils/timestamp.h: TimestampTz (kept local to dedup when datatype/timestamp.h
// is wired the same way sibling replication units do it).
pub type TimestampTz = crate::c::int64;

// --- Locally stubbed referenced-but-not-yet-ported symbols ---

// utils/cache/lsyscache.h: get_namespace_name / get_rel_name. Not yet ported.
// TODO(pg-port): real symbols live in utils/cache/lsyscache.c.
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.c
}
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.c
}

// replication/origin.h: replorigin_by_oid. Not yet ported.
// TODO(pg-port): real symbol lives in replication/logical/origin.c.
unsafe fn replorigin_by_oid(
    _roident: RepOriginId,
    _missing_ok: bool,
    _roname: *mut *mut c_char,
) -> bool {
    unimplemented!() // TODO(pg-port): replication/logical/origin.c
}

// utils/adt/timestamp.h: timestamptz_to_str. The real one lives in
// crate::utils::adt::timestamp, returning *const c_char.
use crate::utils::adt::timestamp::timestamptz_to_str;

// storage/lmgr.h: CheckRelationOidLockedByMe. The real one lives in
// storage/lmgr/lmgr.rs.
use crate::storage::lmgr::lmgr::CheckRelationOidLockedByMe;

// catalog/index.h: BuildIndexInfo. executor/execIndexing.h: FormIndexDatum.
// TODO(pg-port): real BuildIndexInfo lives in catalog/index.c; real
// FormIndexDatum lives in access/index/index.c (declared in execIndexing.h).
unsafe fn BuildIndexInfo(_index: Relation) -> *mut crate::nodes::execnodes::IndexInfo {
    unimplemented!() // TODO(pg-port): catalog/index.c
}
unsafe fn FormIndexDatum(
    _indexInfo: *mut crate::nodes::execnodes::IndexInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!() // TODO(pg-port): executor/execIndexing.c (FormIndexDatum)
}

// replication/logicalrelation.h: GetRelationIdentityOrPK. The real one lives in
// replication/logicalrelation.rs.
use crate::replication::logicalrelation::GetRelationIdentityOrPK;

// executor/executor.h: GetPerTupleExprContext.
// TODO(pg-port): real GetPerTupleExprContext lives in executor/executor.h
// (macro over estate->es_per_tuple_exprcontext).
unsafe fn GetPerTupleExprContext(
    _estate: *mut EState,
) -> *mut crate::nodes::execnodes::ExprContext {
    unimplemented!() // TODO(pg-port): executor/executor.h
}

// replication/worker_internal.h: the subscription object of the current worker.
use crate::replication::worker_internal::MySubscription;

// catalog/pg_subscription.h: Subscription. The shared worker_internal stub types
// it as c_void; we only need the leading `oid` field here, so a local minimal
// repr-C view is used to read it.
// TODO(pg-port): real Subscription lives in catalog/pg_subscription.c.
#[repr(C)]
struct SubscriptionView {
    oid: Oid,
}

// utils/errcodes.h: SQLSTATE classifications used by errcode_apply_conflict.
// TODO(pg-port): real codes live in the generated utils/errcodes.h.
const ERRCODE_UNIQUE_VIOLATION: c_int = 0;
const ERRCODE_T_R_SERIALIZATION_FAILURE: c_int = 0;

/*
 * Conflict types that could occur while applying remote changes.
 *
 * This enum is used in statistics collection (see
 * PgStat_StatSubEntry::conflict_count and
 * PgStat_BackendSubEntry::conflict_count) as well, therefore, when adding new
 * values or reordering existing ones, ensure to review and potentially adjust
 * the corresponding statistics collection codes.
 */
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub enum ConflictType {
    /* The row to be inserted violates unique constraint */
    CT_INSERT_EXISTS = 0,

    /* The row to be updated was modified by a different origin */
    CT_UPDATE_ORIGIN_DIFFERS,

    /* The updated row value violates unique constraint */
    CT_UPDATE_EXISTS,

    /* The row to be updated is missing */
    CT_UPDATE_MISSING,

    /* The row to be deleted was modified by a different origin */
    CT_DELETE_ORIGIN_DIFFERS,

    /* The row to be deleted is missing */
    CT_DELETE_MISSING,

    /* The row to be inserted/updated violates multiple unique constraint */
    CT_MULTIPLE_UNIQUE_CONFLICTS,
    /*
     * Other conflicts, such as exclusion constraint violations, involve more
     * complex rules than simple equality checks. These conflicts are left for
     * future improvements.
     */
}

pub use ConflictType::*;

pub const CONFLICT_NUM_TYPES: usize = CT_MULTIPLE_UNIQUE_CONFLICTS as usize + 1;

/*
 * Information for the existing local row that caused the conflict.
 */
#[repr(C)]
pub struct ConflictTupleInfo {
    /* tuple slot holding the conflicting local tuple */
    pub slot: *mut TupleTableSlot,
    /* OID of the index where the conflict occurred */
    pub indexoid: Oid,
    /* transaction ID of the modification causing the conflict */
    pub xmin: TransactionId,
    /* origin identifier of the modification */
    pub origin: RepOriginId,
    /* timestamp of when the modification on the conflicting local row occurred */
    pub ts: TimestampTz,
}

static CONFLICT_TYPE_NAMES: [&str; CONFLICT_NUM_TYPES] = [
    /* CT_INSERT_EXISTS */ "insert_exists",
    /* CT_UPDATE_ORIGIN_DIFFERS */ "update_origin_differs",
    /* CT_UPDATE_EXISTS */ "update_exists",
    /* CT_UPDATE_MISSING */ "update_missing",
    /* CT_DELETE_ORIGIN_DIFFERS */ "delete_origin_differs",
    /* CT_DELETE_MISSING */ "delete_missing",
    /* CT_MULTIPLE_UNIQUE_CONFLICTS */ "multiple_unique_conflicts",
];

/*
 * Get the xmin and commit timestamp data (origin and timestamp) associated
 * with the provided local row.
 *
 * Return true if the commit timestamp data was found, false otherwise.
 */
pub unsafe fn GetTupleTransactionInfo(
    localslot: *mut TupleTableSlot,
    xmin: *mut TransactionId,
    localorigin: *mut RepOriginId,
    localts: *mut TimestampTz,
) -> bool {
    let xminDatum: Datum;
    let mut isnull: bool = false;

    xminDatum = slot_getsysattr(localslot, MinTransactionIdAttributeNumber as c_int, &mut isnull);
    *xmin = DatumGetTransactionId(xminDatum);
    Assert!(!isnull);

    /*
     * The commit timestamp data is not available if track_commit_timestamp is
     * disabled.
     */
    if !track_commit_timestamp {
        *localorigin = InvalidRepOriginId;
        *localts = 0;
        return false;
    }

    TransactionIdGetCommitTsData(*xmin, localts, localorigin)
}

/*
 * This function is used to report a conflict while applying replication
 * changes.
 *
 * 'searchslot' should contain the tuple used to search the local row to be
 * updated or deleted.
 *
 * 'remoteslot' should contain the remote new tuple, if any.
 *
 * conflicttuples is a list of local rows that caused the conflict and the
 * conflict related information. See ConflictTupleInfo.
 *
 * The caller must ensure that all the indexes passed in ConflictTupleInfo are
 * locked so that we can fetch and display the conflicting key values.
 */
pub unsafe fn ReportApplyConflict(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    elevel: c_int,
    type_: ConflictType,
    searchslot: *mut TupleTableSlot,
    remoteslot: *mut TupleTableSlot,
    conflicttuples: *mut List,
) {
    let localrel: Relation = (*relinfo).ri_RelationDesc;
    let mut err_detail: StringInfoData = std::mem::zeroed();

    initStringInfo(&mut err_detail);

    /* Form errdetail message by combining conflicting tuples information. */
    foreach_ptr!(ConflictTupleInfo, conflicttuple, conflicttuples, {
        errdetail_apply_conflict(
            estate,
            relinfo,
            type_,
            searchslot,
            (*conflicttuple).slot,
            remoteslot,
            (*conflicttuple).indexoid,
            (*conflicttuple).xmin,
            (*conflicttuple).origin,
            (*conflicttuple).ts,
            &mut err_detail,
        );
    });

    pgstat_report_subscription_conflict(
        (*(MySubscription as *mut SubscriptionView)).oid,
        type_ as c_int,
    );

    let _ = errcode_apply_conflict(type_);
    ereport!(
        elevel,
        errmsg!(
            "conflict detected on relation \"{}.{}\": conflict={}\n{}",
            cstr(get_namespace_name(RelationGetNamespace(localrel))),
            cstr(RelationGetRelationName(localrel)),
            CONFLICT_TYPE_NAMES[type_ as usize],
            cstr((err_detail).data)
        )
    );
}

/*
 * Find all unique indexes to check for a conflict and store them into
 * ResultRelInfo.
 */
pub unsafe fn InitConflictIndexes(relInfo: *mut ResultRelInfo) {
    let mut uniqueIndexes: *mut List = crate::nodes::pg_list::NIL;

    let mut i: c_int = 0;
    while i < (*relInfo).ri_NumIndices {
        let indexRelation: Relation = *(*relInfo).ri_IndexRelationDescs.add(i as usize);

        if indexRelation.is_null() {
            i += 1;
            continue;
        }

        /* Detect conflict only for unique indexes */
        if !(*(*(*relInfo).ri_IndexRelationInfo.add(i as usize))).ii_Unique {
            i += 1;
            continue;
        }

        /* Don't support conflict detection for deferrable index */
        if !(*(*indexRelation).rd_index).indimmediate {
            i += 1;
            continue;
        }

        uniqueIndexes = lappend_oid(uniqueIndexes, RelationGetRelid(indexRelation));
        i += 1;
    }

    (*relInfo).ri_onConflictArbiterIndexes = uniqueIndexes;
}

/*
 * Add SQLSTATE error code to the current conflict report.
 */
unsafe fn errcode_apply_conflict(type_: ConflictType) -> c_int {
    match type_ {
        CT_INSERT_EXISTS | CT_UPDATE_EXISTS | CT_MULTIPLE_UNIQUE_CONFLICTS => {
            return ERRCODE_UNIQUE_VIOLATION;
        }
        CT_UPDATE_ORIGIN_DIFFERS | CT_UPDATE_MISSING | CT_DELETE_ORIGIN_DIFFERS
        | CT_DELETE_MISSING => {
            return ERRCODE_T_R_SERIALIZATION_FAILURE;
        }
    }

    #[allow(unreachable_code)]
    {
        Assert!(false);
        0 /* silence compiler warning */
    }
}

/*
 * Add an errdetail() line showing conflict detail.
 *
 * The DETAIL line comprises of two parts:
 * 1. Explanation of the conflict type, including the origin and commit
 *    timestamp of the existing local row.
 * 2. Display of conflicting key, existing local row, remote new row, and
 *    replica identity columns, if any. The remote old row is excluded as its
 *    information is covered in the replica identity columns.
 */
unsafe fn errdetail_apply_conflict(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    type_: ConflictType,
    searchslot: *mut TupleTableSlot,
    localslot: *mut TupleTableSlot,
    remoteslot: *mut TupleTableSlot,
    indexoid: Oid,
    localxmin: TransactionId,
    localorigin: RepOriginId,
    localts: TimestampTz,
    err_msg: StringInfo,
) {
    let mut err_detail: StringInfoData = std::mem::zeroed();
    let val_desc: *mut c_char;
    let mut origin_name: *mut c_char = null_mut();

    initStringInfo(&mut err_detail);

    /* First, construct a detailed message describing the type of conflict */
    match type_ {
        CT_INSERT_EXISTS | CT_UPDATE_EXISTS | CT_MULTIPLE_UNIQUE_CONFLICTS => {
            Assert!(
                OidIsValid(indexoid)
                    && CheckRelationOidLockedByMe(indexoid, RowExclusiveLock, true)
            );

            if localts != 0 {
                if localorigin == InvalidRepOriginId {
                    appendStringInfo!(
                        &mut err_detail,
                        "Key already exists in unique index \"{}\", modified locally in transaction {} at {}.",
                        cstr(get_rel_name(indexoid)),
                        localxmin,
                        cstr(timestamptz_to_str(localts))
                    );
                } else if replorigin_by_oid(localorigin, true, &mut origin_name) {
                    appendStringInfo!(
                        &mut err_detail,
                        "Key already exists in unique index \"{}\", modified by origin \"{}\" in transaction {} at {}.",
                        cstr(get_rel_name(indexoid)),
                        cstr(origin_name),
                        localxmin,
                        cstr(timestamptz_to_str(localts))
                    );
                }
                /*
                 * The origin that modified this row has been removed. This
                 * can happen if the origin was created by a different apply
                 * worker and its associated subscription and origin were
                 * dropped after updating the row, or if the origin was
                 * manually dropped by the user.
                 */
                else {
                    appendStringInfo!(
                        &mut err_detail,
                        "Key already exists in unique index \"{}\", modified by a non-existent origin in transaction {} at {}.",
                        cstr(get_rel_name(indexoid)),
                        localxmin,
                        cstr(timestamptz_to_str(localts))
                    );
                }
            } else {
                appendStringInfo!(
                    &mut err_detail,
                    "Key already exists in unique index \"{}\", modified in transaction {}.",
                    cstr(get_rel_name(indexoid)),
                    localxmin
                );
            }
        }

        CT_UPDATE_ORIGIN_DIFFERS => {
            if localorigin == InvalidRepOriginId {
                appendStringInfo!(
                    &mut err_detail,
                    "Updating the row that was modified locally in transaction {} at {}.",
                    localxmin,
                    cstr(timestamptz_to_str(localts))
                );
            } else if replorigin_by_oid(localorigin, true, &mut origin_name) {
                appendStringInfo!(
                    &mut err_detail,
                    "Updating the row that was modified by a different origin \"{}\" in transaction {} at {}.",
                    cstr(origin_name),
                    localxmin,
                    cstr(timestamptz_to_str(localts))
                );
            }
            /* The origin that modified this row has been removed. */
            else {
                appendStringInfo!(
                    &mut err_detail,
                    "Updating the row that was modified by a non-existent origin in transaction {} at {}.",
                    localxmin,
                    cstr(timestamptz_to_str(localts))
                );
            }
        }

        CT_UPDATE_MISSING => {
            appendStringInfoString(
                &mut err_detail,
                c"Could not find the row to be updated.".as_ptr(),
            );
        }

        CT_DELETE_ORIGIN_DIFFERS => {
            if localorigin == InvalidRepOriginId {
                appendStringInfo!(
                    &mut err_detail,
                    "Deleting the row that was modified locally in transaction {} at {}.",
                    localxmin,
                    cstr(timestamptz_to_str(localts))
                );
            } else if replorigin_by_oid(localorigin, true, &mut origin_name) {
                appendStringInfo!(
                    &mut err_detail,
                    "Deleting the row that was modified by a different origin \"{}\" in transaction {} at {}.",
                    cstr(origin_name),
                    localxmin,
                    cstr(timestamptz_to_str(localts))
                );
            }
            /* The origin that modified this row has been removed. */
            else {
                appendStringInfo!(
                    &mut err_detail,
                    "Deleting the row that was modified by a non-existent origin in transaction {} at {}.",
                    localxmin,
                    cstr(timestamptz_to_str(localts))
                );
            }
        }

        CT_DELETE_MISSING => {
            appendStringInfoString(
                &mut err_detail,
                c"Could not find the row to be deleted.".as_ptr(),
            );
        }
    }

    Assert!(err_detail.len > 0);

    val_desc = build_tuple_value_details(
        estate, relinfo, type_, searchslot, localslot, remoteslot, indexoid,
    );

    /*
     * Next, append the key values, existing local row, remote row, and
     * replica identity columns after the message.
     */
    if !val_desc.is_null() {
        appendStringInfo!(&mut err_detail, "\n{}", cstr(val_desc));
    }

    /*
     * Insert a blank line to visually separate the new detail line from the
     * existing ones.
     */
    if (*err_msg).len > 0 {
        appendStringInfoChar(err_msg, b'\n' as c_char);
    }

    appendStringInfoString(err_msg, (err_detail).data);
}

/*
 * Helper function to build the additional details for conflicting key,
 * existing local row, remote row, and replica identity columns.
 *
 * If the return value is NULL, it indicates that the current user lacks
 * permissions to view the columns involved.
 */
unsafe fn build_tuple_value_details(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    type_: ConflictType,
    searchslot: *mut TupleTableSlot,
    localslot: *mut TupleTableSlot,
    remoteslot: *mut TupleTableSlot,
    indexoid: Oid,
) -> *mut c_char {
    let localrel: Relation = (*relinfo).ri_RelationDesc;
    let relid: Oid = RelationGetRelid(localrel);
    let tupdesc: TupleDesc = RelationGetDescr(localrel);
    let mut tuple_value: StringInfoData = std::mem::zeroed();
    let mut desc: *mut c_char;

    Assert!(!searchslot.is_null() || !localslot.is_null() || !remoteslot.is_null());

    initStringInfo(&mut tuple_value);

    /*
     * Report the conflicting key values in the case of a unique constraint
     * violation.
     */
    if type_ == CT_INSERT_EXISTS || type_ == CT_UPDATE_EXISTS || type_ == CT_MULTIPLE_UNIQUE_CONFLICTS
    {
        Assert!(OidIsValid(indexoid) && !localslot.is_null());

        desc = build_index_value_desc(estate, localrel, localslot, indexoid);

        if !desc.is_null() {
            appendStringInfo!(&mut tuple_value, "Key {}", cstr(desc));
        }
    }

    if !localslot.is_null() {
        /*
         * The 'modifiedCols' only applies to the new tuple, hence we pass
         * NULL for the existing local row.
         */
        desc = ExecBuildSlotValueDescription(relid, localslot, tupdesc, null_mut(), 64);

        if !desc.is_null() {
            if tuple_value.len > 0 {
                appendStringInfoString(&mut tuple_value, c"; ".as_ptr());
                appendStringInfo!(&mut tuple_value, "existing local row {}", cstr(desc));
            } else {
                appendStringInfo!(&mut tuple_value, "Existing local row {}", cstr(desc));
            }
        }
    }

    if !remoteslot.is_null() {
        let modifiedCols: *mut Bitmapset;

        /*
         * Although logical replication doesn't maintain the bitmap for the
         * columns being inserted, we still use it to create 'modifiedCols'
         * for consistency with other calls to ExecBuildSlotValueDescription.
         *
         * Note that generated columns are formed locally on the subscriber.
         */
        modifiedCols = bms_union(
            ExecGetInsertedCols(relinfo, estate),
            ExecGetUpdatedCols(relinfo, estate),
        );
        desc = ExecBuildSlotValueDescription(relid, remoteslot, tupdesc, modifiedCols, 64);

        if !desc.is_null() {
            if tuple_value.len > 0 {
                appendStringInfoString(&mut tuple_value, c"; ".as_ptr());
                appendStringInfo!(&mut tuple_value, "remote row {}", cstr(desc));
            } else {
                appendStringInfo!(&mut tuple_value, "Remote row {}", cstr(desc));
            }
        }
    }

    if !searchslot.is_null() {
        /*
         * Note that while index other than replica identity may be used (see
         * IsIndexUsableForReplicaIdentityFull for details) to find the tuple
         * when applying update or delete, such an index scan may not result
         * in a unique tuple and we still compare the complete tuple in such
         * cases, thus such indexes are not used here.
         */
        let replica_index: Oid = GetRelationIdentityOrPK(localrel);

        Assert!(type_ != CT_INSERT_EXISTS);

        /*
         * If the table has a valid replica identity index, build the index
         * key value string. Otherwise, construct the full tuple value for
         * REPLICA IDENTITY FULL cases.
         */
        if OidIsValid(replica_index) {
            desc = build_index_value_desc(estate, localrel, searchslot, replica_index);
        } else {
            desc = ExecBuildSlotValueDescription(relid, searchslot, tupdesc, null_mut(), 64);
        }

        if !desc.is_null() {
            if tuple_value.len > 0 {
                appendStringInfoString(&mut tuple_value, c"; ".as_ptr());
                if OidIsValid(replica_index) {
                    appendStringInfo!(&mut tuple_value, "replica identity {}", cstr(desc));
                } else {
                    appendStringInfo!(&mut tuple_value, "replica identity full {}", cstr(desc));
                }
            } else if OidIsValid(replica_index) {
                appendStringInfo!(&mut tuple_value, "Replica identity {}", cstr(desc));
            } else {
                appendStringInfo!(&mut tuple_value, "Replica identity full {}", cstr(desc));
            }
        }
    }

    if tuple_value.len == 0 {
        return null_mut();
    }

    appendStringInfoChar(&mut tuple_value, b'.' as c_char);
    tuple_value.data
}

/*
 * Helper functions to construct a string describing the contents of an index
 * entry. See BuildIndexValueDescription for details.
 *
 * The caller must ensure that the index with the OID 'indexoid' is locked so
 * that we can fetch and display the conflicting key value.
 */
unsafe fn build_index_value_desc(
    estate: *mut EState,
    localrel: Relation,
    slot: *mut TupleTableSlot,
    indexoid: Oid,
) -> *mut c_char {
    let index_value: *mut c_char;
    let indexDesc: Relation;
    let mut values: [Datum; INDEX_MAX_KEYS] = [0; INDEX_MAX_KEYS];
    let mut isnull: [bool; INDEX_MAX_KEYS] = [false; INDEX_MAX_KEYS];
    let mut tableslot: *mut TupleTableSlot = slot;

    if tableslot.is_null() {
        return null_mut();
    }

    Assert!(CheckRelationOidLockedByMe(indexoid, RowExclusiveLock, true));

    indexDesc = index_open(indexoid, NoLock);

    /*
     * If the slot is a virtual slot, copy it into a heap tuple slot as
     * FormIndexDatum only works with heap tuple slots.
     */
    if TTS_IS_VIRTUAL(slot) {
        tableslot = table_slot_create(localrel, &mut (*estate).es_tupleTable);
        tableslot = ExecCopySlot(tableslot, slot);
    }

    /*
     * Initialize ecxt_scantuple for potential use in FormIndexDatum when
     * index expressions are present.
     */
    (*GetPerTupleExprContext(estate)).ecxt_scantuple = tableslot;

    /*
     * The values/nulls arrays passed to BuildIndexValueDescription should be
     * the results of FormIndexDatum, which are the "raw" input to the index
     * AM.
     */
    FormIndexDatum(
        BuildIndexInfo(indexDesc),
        tableslot,
        estate,
        values.as_mut_ptr(),
        isnull.as_mut_ptr(),
    );

    index_value = BuildIndexValueDescription(indexDesc, values.as_ptr(), isnull.as_ptr());

    index_close(indexDesc, NoLock);

    index_value
}

// --- Local helper for C-string handling in format strings ---

/// Render a NUL-terminated C string as a Rust `&str` for use in `{}` slots
/// (mirrors the C `%s` of a `char *`). NULL renders as the empty string.
unsafe fn cstr<'a>(p: *const c_char) -> &'a str {
    if p.is_null() {
        return "";
    }
    core::ffi::CStr::from_ptr(p).to_str().unwrap_or("")
}

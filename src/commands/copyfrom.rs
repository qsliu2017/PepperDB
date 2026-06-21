/*-------------------------------------------------------------------------
 *
 * copyfrom.c -> copyfrom.rs
 *		COPY <table> FROM file/program/client
 *
 * This file contains routines needed to efficiently load tuples into a
 * table.  That includes looking up the correct partition, firing triggers,
 * calling the table AM function to insert the data, and updating indexes.
 * Reading data from the input file or client and parsing it into Datums
 * is handled in copyfromparse.c.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copyfrom.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};
use std::mem::size_of;

use crate::c::{int64, uint64, Size};
use crate::access::attnum::AttrNumber;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::heap::hio::{BulkInsertState, BulkInsertStateData};
use crate::access::heap::heapam::{FreeBulkInsertState, GetBulkInsertState, ReleaseBulkInsertStatePin};
use crate::access::common::tupconvert::{execute_attr_map_slot, TupleConversionMap};
use crate::access::table::tableam::{
    table_slot_create, table_tuple_insert, TABLE_INSERT_FROZEN, TABLE_INSERT_SKIP_FSM,
};
use crate::catalog::pg_class::{
    RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_SEQUENCE, RELKIND_VIEW,
};
use crate::commands::copy::{
    CopyFormatOptions, CopyFromState, CopyFromStateData, CopyLogVerbosityChoice,
    CopyOnErrorChoice, COPY_LOG_VERBOSITY_DEFAULT, COPY_ON_ERROR_IGNORE, COPY_ON_ERROR_STOP,
};
use crate::commands::copyapi::{CopyFromRoutine};
use crate::commands::copyfrom_internal::{
    CopyFromBinaryOneRow, CopyFromCSVOneRow, CopyFromTextOneRow, CopyFromStateData as InternalCopyFromStateData,
    CopyInsertMethod, CopySource, EolType, INPUT_BUF_SIZE, RAW_BUF_SIZE, ReceiveCopyBegin,
    ReceiveCopyBinaryHeader, COPY_CALLBACK, COPY_FILE, COPY_FRONTEND, CIM_MULTI,
    CIM_MULTI_CONDITIONAL, CIM_SINGLE, EOL_UNKNOWN,
};
use crate::commands::progress::{
    PROGRESS_COPY_BYTES_TOTAL, PROGRESS_COPY_COMMAND, PROGRESS_COPY_COMMAND_FROM,
    PROGRESS_COPY_TUPLES_EXCLUDED, PROGRESS_COPY_TUPLES_PROCESSED, PROGRESS_COPY_TUPLES_SKIPPED,
    PROGRESS_COPY_TYPE, PROGRESS_COPY_TYPE_CALLBACK, PROGRESS_COPY_TYPE_FILE,
    PROGRESS_COPY_TYPE_PIPE, PROGRESS_COPY_TYPE_PROGRAM,
};
use crate::executor::execIndexing::ExecInsertIndexTuples;
use crate::executor::execTuples::{
    ExecDropSingleTupleTableSlot, ExecResetTupleTable,
    ExecStoreVirtualTuple,
};
use crate::executor::tuptable::{ExecCopySlot, ExecMaterializeSlot, ExecClearTuple};
use crate::executor::execUtils::{
    CreateExecutorState, ExecGetRootToChildMap, ExecInitRangeTable, ExecInitResultRelation,
    FreeExecutorState, GetPerTupleExprContext, GetPerTupleMemoryContext, ResetPerTupleExprContext,
};
use crate::executor::executor::{
    CheckValidResultRel, ExecCloseRangeTableRelations, ExecCloseResultRelations, ExecConstraints,
    ExecInitExpr, ExecInitQual, ExecOpenIndices, ExecPartitionCheck, ExecQual,
};
use crate::executor::tuptable::TupleTableSlot;
use crate::lib::stringinfo::{initStringInfo, StringInfoData};
use crate::mb::pg_wchar::{GetDatabaseEncoding, PG_SQL_ASCII, pg_encoding_to_char, pg_get_client_encoding};
use crate::mb::mbutils::pg_mbcliplen;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::nodes::execnodes::{
    EState, ExprContext, ExprState, ModifyTableState, PartitionTupleRouting, ResultRelInfo,
    TransitionCaptureState,
};
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::nodes::{CmdType, OnConflictAction, Node, NodeTag};
use crate::nodes::nodes::CmdType::*;
use crate::nodes::nodes::OnConflictAction::*;
use crate::nodes::pg_list::{
    lappend, linitial, list_delete_first, list_free, list_length, List, ListCell, NIL,
};
use crate::optimizer::optimizer::{contain_volatile_functions, contain_volatile_functions_not_nextval, expression_planner};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;
use crate::utils::fmgr::{fmgr_info, FmgrInfo};
// MemoryContext comes from crate::prelude (utils::palloc).
// InternalCopyFromStateData.copycontext is memnodes::MemoryContext; cast with as_palloc_ctx
// wherever the two must interoperate.
#[inline]
unsafe fn as_palloc_ctx(ctx: crate::utils::mmgr::memnodes::MemoryContext) -> crate::utils::palloc::MemoryContext {
    ctx as *mut _ as crate::utils::palloc::MemoryContext
}
#[inline]
unsafe fn as_memnodes_ctx(ctx: crate::utils::palloc::MemoryContext) -> crate::utils::mmgr::memnodes::MemoryContext {
    ctx as *mut _ as crate::utils::mmgr::memnodes::MemoryContext
}
use crate::utils::rel::Relation;
use crate::utils::activity::backend_progress::{
    pgstat_progress_end_command, pgstat_progress_start_command, pgstat_progress_update_multi_param,
    pgstat_progress_update_param, ProgressCommandType,
};
use crate::utils::activity::backend_progress::ProgressCommandType::PROGRESS_COMMAND_COPY;
use crate::{makeNode};

/*
 * No more than this many tuples per CopyMultiInsertBuffer
 *
 * Caution: Don't make this too big, as we could end up with this many
 * CopyMultiInsertBuffer items stored in CopyMultiInsertInfo's
 * multiInsertBuffers list.  Increasing this can cause quadratic growth in
 * memory requirements during copies into partitioned tables with a large
 * number of partitions.
 */
const MAX_BUFFERED_TUPLES: usize = 1000;

/*
 * Flush buffers if there are >= this many bytes, as counted by the input
 * size, of tuples stored.
 */
const MAX_BUFFERED_BYTES: c_int = 65535;

/*
 * Trim the list of buffers back down to this number after flushing.  This
 * must be >= 2.
 */
const MAX_PARTITION_BUFFERS: c_int = 32;

/* Stores multi-insert data related to a single relation in CopyFrom. */
#[repr(C)]
pub struct CopyMultiInsertBuffer {
    pub slots: [*mut TupleTableSlot; MAX_BUFFERED_TUPLES], /* Array to store tuples */
    pub resultRelInfo: *mut ResultRelInfo,                 /* ResultRelInfo for 'relid' */
    pub bistate: BulkInsertState, /* BulkInsertState for this rel if plain
                                   * table; NULL if foreign table */
    pub nused: c_int,                        /* number of 'slots' containing tuples */
    pub linenos: [uint64; MAX_BUFFERED_TUPLES], /* Line # of tuple in copy stream */
}

/*
 * Stores one or many CopyMultiInsertBuffers and details about the size and
 * number of tuples which are stored in them.  This allows multiple buffers to
 * exist at once when COPYing into a partitioned table.
 */
#[repr(C)]
pub struct CopyMultiInsertInfo {
    pub multiInsertBuffers: *mut List, /* List of tracked CopyMultiInsertBuffers */
    pub bufferedTuples: c_int,         /* number of tuples buffered over all buffers */
    pub bufferedBytes: c_int,          /* number of bytes from all buffered tuples */
    pub cstate: CopyFromState,         /* Copy state for this CopyMultiInsertInfo */
    pub estate: *mut EState,           /* Executor state used for COPY */
    pub mycid: CommandId,              /* Command Id used for COPY */
    pub ti_options: c_int,             /* table insert options */
}

/* CommandId type (xact.h) */
// TODO(pg-port): real CommandId lives in access/transam/xact.h
pub type CommandId = u32;

/* -------------------------------------------------------------------------
 * Local stubs for unported symbols
 * ------------------------------------------------------------------------- */

/* TODO(pg-port): real RelationGetRelid lives in utils/rel.h */
unsafe fn RelationGetRelid(rel: Relation) -> Oid {
    crate::utils::rel::RelationGetRelid(rel as _) as _
}

/* TODO(pg-port): real RelationGetRelationName lives in utils/rel.h */
unsafe fn RelationGetRelationName(rel: Relation) -> *const c_char {
    crate::utils::rel::RelationGetRelationName(rel as _) as _
}

/* TODO(pg-port): real RelationGetDescr lives in utils/rel.h */
unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc {
    crate::utils::rel::RelationGetDescr(rel as _) as _
}

/* TODO(pg-port): real GetCurrentCommandId lives in access/transam/xact.h */
unsafe fn GetCurrentCommandId(_isWrite: bool) -> CommandId {
    crate::access::transam::xact::GetCurrentCommandId(_isWrite) as _
}

/* TODO(pg-port): real InvalidateCatalogSnapshot lives in utils/snapmgr.h */
unsafe fn InvalidateCatalogSnapshot() {
    crate::utils::time::snapmgr::InvalidateCatalogSnapshot()
}

/* TODO(pg-port): real ThereAreNoPriorRegisteredSnapshots lives in utils/snapmgr.h */
unsafe fn ThereAreNoPriorRegisteredSnapshots() -> bool {
    crate::utils::time::snapmgr::ThereAreNoPriorRegisteredSnapshots()
}

/* TODO(pg-port): real ThereAreNoReadyPortals lives in utils/portal.h */
unsafe fn ThereAreNoReadyPortals() -> bool {
    crate::utils::mmgr::portalmem::ThereAreNoReadyPortals()
}

/* TODO(pg-port): real GetCurrentSubTransactionId lives in access/transam/xact.h */
unsafe fn GetCurrentSubTransactionId() -> SubTransactionId {
    crate::access::transam::xact::GetCurrentSubTransactionId() as _
}

/* TODO(pg-port): SubTransactionId from c.h */
pub type SubTransactionId = u32;
pub const InvalidSubTransactionId: SubTransactionId = 0;

/* TODO(pg-port): real ExecBSInsertTriggers lives in commands/trigger.c */
unsafe fn ExecBSInsertTriggers(_estate: *mut EState, _resultRelInfo: *mut ResultRelInfo) {
    crate::commands::trigger::ExecBSInsertTriggers(_estate as _, _resultRelInfo as _)
}

/* TODO(pg-port): real ExecBRInsertTriggers lives in commands/trigger.c */
unsafe fn ExecBRInsertTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
) -> bool {
    crate::commands::trigger::ExecBRInsertTriggers(_estate as _, _resultRelInfo as _, _slot as _)
}

/* TODO(pg-port): no canonical impl yet; only reached for tables with INSTEAD-OF
 * triggers, which trigger-free COPY never has. Safe no-op. */
unsafe fn ExecIRInsertTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
) {
}

/* TODO(pg-port): real ExecARInsertTriggers lives in commands/trigger.c */
unsafe fn ExecARInsertTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _recheckIndexes: *mut List,
    _transition_capture: *mut TransitionCaptureState,
) {
    crate::commands::trigger::ExecARInsertTriggers(
        _estate as _, _resultRelInfo as _, _slot as _, _recheckIndexes as _,
        _transition_capture as _,
    )
}

/* TODO(pg-port): real ExecASInsertTriggers lives in commands/trigger.c */
unsafe fn ExecASInsertTriggers(
    _estate: *mut EState,
    _resultRelInfo: *mut ResultRelInfo,
    _transition_capture: *mut TransitionCaptureState,
) {
    crate::commands::trigger::ExecASInsertTriggers(
        _estate as _, _resultRelInfo as _, _transition_capture as _,
    )
}

/* TODO(pg-port): no canonical impl yet; query-level trigger bookkeeping is a
 * no-op for trigger-free COPY. */
unsafe fn AfterTriggerBeginQuery() {}

/* TODO(pg-port): no canonical impl yet; see AfterTriggerBeginQuery. */
unsafe fn AfterTriggerEndQuery(_estate: *mut EState) {}

/* TODO(pg-port): no canonical impl yet; transition tables require triggers,
 * which trigger-free COPY never has. Returns NULL (no capture). */
unsafe fn MakeTransitionCaptureState(
    _trigdesc: *mut c_void,
    _relid: Oid,
    _cmdType: CmdType,
) -> *mut TransitionCaptureState {
    core::ptr::null_mut()
}

/* TODO(pg-port): real ExecSetupPartitionTupleRouting lives in executor/execPartition.c */
unsafe fn ExecSetupPartitionTupleRouting(
    _estate: *mut EState,
    _rel: Relation,
) -> *mut PartitionTupleRouting {
    crate::executor::execPartition::ExecSetupPartitionTupleRouting(_estate as _, _rel as _) as _
}

/* TODO(pg-port): real ExecFindPartition lives in executor/execPartition.c */
unsafe fn ExecFindPartition(
    _mtstate: *mut ModifyTableState,
    _rootResultRelInfo: *mut ResultRelInfo,
    _proute: *mut PartitionTupleRouting,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
) -> *mut ResultRelInfo {
    crate::executor::execPartition::ExecFindPartition(
        _mtstate as _, _rootResultRelInfo as _, _proute as _, _slot as _, _estate as _,
    ) as _
}

/* TODO(pg-port): real ExecCleanupTupleRouting lives in executor/execPartition.c */
unsafe fn ExecCleanupTupleRouting(
    _mtstate: *mut ModifyTableState,
    _proute: *mut PartitionTupleRouting,
) {
    crate::executor::execPartition::ExecCleanupTupleRouting(_mtstate as _, _proute as _)
}

/* TODO(pg-port): real ExecComputeStoredGenerated lives in executor/nodeModifyTable.c */
unsafe fn ExecComputeStoredGenerated(
    _resultRelInfo: *mut ResultRelInfo,
    _estate: *mut EState,
    _slot: *mut TupleTableSlot,
    _cmdType: CmdType,
) {
    crate::executor::nodeModifyTable::ExecComputeStoredGenerated(
        _resultRelInfo as _, _estate as _, _slot as _, _cmdType,
    )
}

/* TODO(pg-port): table_multi_insert dispatches the heap AM directly (the AM
 * routine slot is not yet wired). */
unsafe fn table_multi_insert(
    _rel: Relation,
    _slots: *mut *mut TupleTableSlot,
    _nslots: c_int,
    _cid: CommandId,
    _options: c_int,
    _bistate: BulkInsertState,
) {
    crate::access::heap::heapam::heap_multi_insert(
        _rel as _, _slots as _, _nslots as _, _cid as _, _options as _, _bistate as _,
    )
}

/* TODO(pg-port): table_finish_bulk_insert is a no-op for the heap AM. */
unsafe fn table_finish_bulk_insert(_rel: Relation, _options: c_int) {}

/* TODO(pg-port): real NextCopyFrom lives in commands/copyfromparse.c */
unsafe fn NextCopyFrom(
    _cstate: CopyFromState,
    _econtext: *mut ExprContext,
    _values: *mut Datum,
    _nulls: *mut bool,
) -> bool {
    crate::commands::copyfrom_internal::copyfromparse::NextCopyFrom(
        _cstate as _, _econtext as _, _values as _, _nulls as _,
    )
}

/* TODO(pg-port): real bms_make_singleton lives in nodes/bitmapset.c */
unsafe fn bms_make_singleton(_x: c_int) -> *mut c_void {
    crate::nodes::bitmapset::bms_make_singleton(_x as _) as _
}

/* TODO(pg-port): real RELKIND_HAS_STORAGE lives in catalog/pg_class.h */
#[inline]
unsafe fn RELKIND_HAS_STORAGE(relkind: c_char) -> bool {
    relkind == RELKIND_RELATION
        || relkind == b't' as c_char /* RELKIND_TOASTVALUE */
        || relkind == RELKIND_MATVIEW
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_FOREIGN_TABLE
}

/* TODO(pg-port): real getTypeInputInfo lives in utils/lsyscache.c */
unsafe fn getTypeInputInfo(_type_oid: Oid, _func: *mut Oid, _typioparam: *mut Oid) {
    crate::utils::cache::lsyscache::getTypeInputInfo(_type_oid as _, _func as _, _typioparam as _)
}

/* TODO(pg-port): real getTypeBinaryInputInfo lives in utils/lsyscache.c */
unsafe fn getTypeBinaryInputInfo(_type_oid: Oid, _func: *mut Oid, _typioparam: *mut Oid) {
    crate::utils::cache::lsyscache::getTypeBinaryInputInfo(
        _type_oid as _, _func as _, _typioparam as _,
    )
}

/* TODO(pg-port): real FindDefaultConversionProc lives in catalog/namespace.c */
unsafe fn FindDefaultConversionProc(_src_encoding: c_int, _dest_encoding: c_int) -> Oid {
    crate::catalog::namespace::FindDefaultConversionProc(_src_encoding as _, _dest_encoding as _) as _
}

/* TODO(pg-port): real build_column_default lives in rewrite/rewriteHandler.c */
unsafe fn build_column_default(_rel: Relation, _attno: AttrNumber) -> *mut Expr {
    crate::rewrite::rewriteHandler::build_column_default(_rel as _, _attno as _) as _
}

/* Expr type alias (nodes/primnodes.h) */
// TODO(pg-port): real Expr lives in nodes/primnodes.h
pub type Expr = Node;

/* TODO(pg-port): real ProcessCopyOptions lives in commands/copy.c (already ported) */
unsafe fn ProcessCopyOptions(
    _pstate: *mut ParseState,
    _opts: *mut CopyFormatOptions,
    _is_from: bool,
    _options: *mut List,
) {
    crate::commands::copy::ProcessCopyOptions(_pstate as _, _opts as _, _is_from, _options as _)
}

/* TODO(pg-port): real CopyGetAttnums lives in commands/copy.c (already ported) */
unsafe fn CopyGetAttnums(
    _tupDesc: TupleDesc,
    _rel: Relation,
    _attnamelist: *mut List,
) -> *mut List {
    crate::commands::copy::CopyGetAttnums(_tupDesc as _, _rel as _, _attnamelist as _) as _
}

/* TODO(pg-port): real list_member_int lives in nodes/pg_list.h */
unsafe fn list_member_int(_list: *mut List, _datum: c_int) -> bool {
    crate::nodes::pg_list::list_member_int(_list as _, _datum as _)
}

/* TODO(pg-port): real lfirst_int lives in nodes/pg_list.h */
unsafe fn lfirst_int(_lc: *mut ListCell) -> c_int {
    crate::nodes::pg_list::lfirst_int(_lc as _) as _
}

/* TODO(pg-port): real TupleDescAttr lives in access/tupdesc.h */
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> *mut FormData_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(_tupdesc as _, _i as _) as _
}

/* Canonical attribute types (re-exported to keep field layouts correct). */
pub use crate::catalog::pg_attribute::{FormData_pg_attribute, Form_pg_attribute};
pub use crate::c::NameData;

/* TODO(pg-port): real NameStr lives in c.h (already in prelude) */
#[allow(non_snake_case)]
unsafe fn NameStr_attr(name: &NameData) -> *const c_char {
    name.data.as_ptr()
}

/* TODO(pg-port): real OpenPipeStream lives in storage/file/fd.c */
unsafe fn OpenPipeStream(_command: *const c_char, _mode: *const c_char) -> *mut c_void {
    crate::storage::file::fd::OpenPipeStream(_command as _, _mode as _) as _
}

/* TODO(pg-port): real ClosePipeStream lives in storage/file/fd.c */
unsafe fn ClosePipeStream(_file: *mut c_void) -> c_int {
    crate::storage::file::fd::ClosePipeStream(_file as _) as _
}

/* TODO(pg-port): real AllocateFile lives in storage/file/fd.c */
unsafe fn AllocateFile(_filename: *const c_char, _mode: *const c_char) -> *mut c_void {
    crate::storage::file::fd::AllocateFile(_filename as _, _mode as _) as _
}

/* TODO(pg-port): real FreeFile lives in storage/file/fd.c */
unsafe fn FreeFile(_file: *mut c_void) -> c_int {
    crate::storage::file::fd::FreeFile(_file as _) as _
}

/* libc fileno */
unsafe fn fileno(_file: *mut c_void) -> c_int {
    libc::fileno(_file as *mut libc::FILE)
}

/* libc fstat */
unsafe fn fstat(_fd: c_int, _buf: *mut stat) -> c_int {
    libc::fstat(_fd, _buf)
}

pub type stat = libc::stat;
/* TODO(pg-port): S_ISDIR from <sys/stat.h> */
#[inline]
fn S_ISDIR(mode: libc::mode_t) -> bool {
    (mode & libc::S_IFMT) == libc::S_IFDIR
}

/* TODO(pg-port): real wait_result_is_signal lives in port/wait_error.c */
unsafe fn wait_result_is_signal(_exit_status: c_int, _signum: c_int) -> bool {
    crate::common::wait_error::wait_result_is_signal(_exit_status as _, _signum as _)
}

/* TODO(pg-port): real wait_result_to_str lives in port/wait_error.c */
unsafe fn wait_result_to_str(_exitstatus: c_int) -> *mut c_char {
    crate::common::wait_error::wait_result_to_str(_exitstatus as _) as _
}

/* TODO(pg-port): real errcode_for_file_access lives in utils/error/elog.c */
unsafe fn errcode_for_file_access() -> c_int {
    0 // TODO(pg-port): real errcode_for_file_access lives in utils/error/elog.c
}

/* TODO(pg-port): real whereToSendOutput + DestRemote from tcop/dest.h */
static mut whereToSendOutput: c_int = 0;
const DestRemote: c_int = 1;

/* TODO(pg-port): real MemoryContextDelete lives in utils/mmgr/mcxt.c */
unsafe fn MemoryContextDelete(context: MemoryContext) {
    crate::utils::mmgr::mcxt::MemoryContextDelete(context as _)
}

/* TODO(pg-port): AllocSetContextCreate macro: real version in utils/memutils.h */
/* Already available via crate::AllocSetContextCreate from prelude */

/* TODO(pg-port): stdin from <stdio.h> */
const stdin_ptr: *mut c_void = core::ptr::null_mut(); // TODO(pg-port): real stdin pointer

/* TODO(pg-port): PG_BINARY_R from storage/fd.h */
const PG_BINARY_R: *const c_char = c"rb".as_ptr();

/* errno from <errno.h> */
unsafe fn get_errno() -> c_int {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}
const ENOENT: c_int = 2;
const EACCES: c_int = 13;
const SIGPIPE: c_int = 13;

/* errcontext! local macro - same pattern as vacuumparallel.rs */
macro_rules! errcontext {
    ($($a:tt)*) => {{ let _ = format!($($a)*); }};
}

/* errmsg_plural stub */
macro_rules! errmsg_plural {
    ($singular:expr, $plural:expr, $n:expr, $($arg:tt)*) => {
        if $n == 1 {
            format!($singular, $($arg)*)
        } else {
            format!($plural, $($arg)*)
        }
    };
}

/* errdetail_internal stub */
macro_rules! errdetail_internal {
    ($($a:tt)*) => { format!($($a)*) };
}

/* errhint stub */
macro_rules! errhint {
    ($($a:tt)*) => { format!($($a)*) };
}

/* errcode_for_file_access in ereport context */
macro_rules! ereport_file_error {
    ($level:expr, $($msg:tt)*) => {
        ereport!($level, errmsg!($($msg)*))
    };
}

/*
 * COPY FROM routines for built-in formats.
 *
 * CSV and text formats share the same TextLike routines except for the
 * one-row callback.
 */

/* text format */
static COPY_FROM_ROUTINE_TEXT: CopyFromRoutine = CopyFromRoutine {
    CopyFromInFunc: Some(copy_from_text_like_in_func_trampoline),
    CopyFromStart: Some(copy_from_text_like_start_trampoline),
    CopyFromOneRow: Some(copy_from_text_one_row_trampoline),
    CopyFromEnd: Some(copy_from_text_like_end_trampoline),
};

/* CSV format */
static COPY_FROM_ROUTINE_CSV: CopyFromRoutine = CopyFromRoutine {
    CopyFromInFunc: Some(copy_from_text_like_in_func_trampoline),
    CopyFromStart: Some(copy_from_text_like_start_trampoline),
    CopyFromOneRow: Some(copy_from_csv_one_row_trampoline),
    CopyFromEnd: Some(copy_from_text_like_end_trampoline),
};

/* binary format */
static COPY_FROM_ROUTINE_BINARY: CopyFromRoutine = CopyFromRoutine {
    CopyFromInFunc: Some(copy_from_binary_in_func_trampoline),
    CopyFromStart: Some(copy_from_binary_start_trampoline),
    CopyFromOneRow: Some(copy_from_binary_one_row_trampoline),
    CopyFromEnd: Some(copy_from_binary_end_trampoline),
};

/*
 * Trampoline extern "C" wrappers so we can store fn pointers in the static
 * CopyFromRoutine structs above.
 */
unsafe extern "C" fn copy_from_text_like_in_func_trampoline(
    cstate: CopyFromState,
    atttypid: Oid,
    finfo: *mut FmgrInfo,
    typioparam: *mut Oid,
) {
    CopyFromTextLikeInFunc(cstate, atttypid, finfo, typioparam)
}

unsafe extern "C" fn copy_from_text_like_start_trampoline(
    cstate: CopyFromState,
    tupDesc: TupleDesc,
) {
    CopyFromTextLikeStart(cstate, tupDesc)
}

unsafe extern "C" fn copy_from_text_like_end_trampoline(cstate: CopyFromState) {
    CopyFromTextLikeEnd(cstate)
}

unsafe extern "C" fn copy_from_binary_in_func_trampoline(
    cstate: CopyFromState,
    atttypid: Oid,
    finfo: *mut FmgrInfo,
    typioparam: *mut Oid,
) {
    CopyFromBinaryInFunc(cstate, atttypid, finfo, typioparam)
}

unsafe extern "C" fn copy_from_binary_start_trampoline(
    cstate: CopyFromState,
    tupDesc: TupleDesc,
) {
    CopyFromBinaryStart(cstate, tupDesc)
}

unsafe extern "C" fn copy_from_binary_end_trampoline(cstate: CopyFromState) {
    CopyFromBinaryEnd(cstate)
}

/* Trampolines for CopyFromOneRow callbacks */
unsafe extern "C" fn copy_from_text_one_row_trampoline(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
) -> bool {
    CopyFromTextOneRow(cstate, econtext, values, nulls)
}

unsafe extern "C" fn copy_from_csv_one_row_trampoline(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
) -> bool {
    CopyFromCSVOneRow(cstate, econtext, values, nulls)
}

unsafe extern "C" fn copy_from_binary_one_row_trampoline(
    cstate: CopyFromState,
    econtext: *mut ExprContext,
    values: *mut Datum,
    nulls: *mut bool,
) -> bool {
    CopyFromBinaryOneRow(cstate, econtext, values, nulls)
}

/* Return a COPY FROM routine for the given options */
unsafe fn CopyFromGetRoutine(opts: *const CopyFormatOptions) -> *const CopyFromRoutine {
    if (*opts).csv_mode {
        return &raw const COPY_FROM_ROUTINE_CSV;
    } else if (*opts).binary {
        return &raw const COPY_FROM_ROUTINE_BINARY;
    }

    /* default is text */
    &raw const COPY_FROM_ROUTINE_TEXT
}

/* Implementation of the start callback for text and CSV formats */
unsafe fn CopyFromTextLikeStart(cstate: CopyFromState, _tupDesc: TupleDesc) {
    let attr_count: c_int;

    /*
     * If encoding conversion is needed, we need another buffer to hold the
     * converted input data.  Otherwise, we can just point input_buf to the
     * same buffer as raw_buf.
     */
    // NOTE: CopyFromState here points to CopyFromStateData from copy.rs (opaque),
    // but the internal state is from copyfrom_internal.rs.  We cast through the
    // internal type for field access, as the C code does.
    let cs = cstate as *mut InternalCopyFromStateData;
    if (*cs).need_transcoding {
        (*cs).input_buf = palloc((INPUT_BUF_SIZE + 1) as Size) as *mut c_char;
        (*cs).input_buf_index = 0;
        (*cs).input_buf_len = 0;
    } else {
        (*cs).input_buf = (*cs).raw_buf;
    }
    (*cs).input_reached_eof = false;

    initStringInfo(&mut (*cs).line_buf);

    /*
     * Create workspace for CopyReadAttributes results; used by CSV and text
     * format.
     */
    attr_count = list_length((*cs).attnumlist);
    (*cs).max_fields = attr_count;
    (*cs).raw_fields = palloc((attr_count as Size) * size_of::<*mut c_char>()) as *mut *mut c_char;
}

/*
 * Implementation of the infunc callback for text and CSV formats. Assign
 * the input function data to the given *finfo.
 */
unsafe fn CopyFromTextLikeInFunc(
    _cstate: CopyFromState,
    atttypid: Oid,
    finfo: *mut FmgrInfo,
    typioparam: *mut Oid,
) {
    let mut func_oid: Oid = 0;

    getTypeInputInfo(atttypid, &mut func_oid, typioparam);
    fmgr_info(func_oid, finfo);
}

/* Implementation of the end callback for text and CSV formats */
unsafe fn CopyFromTextLikeEnd(_cstate: CopyFromState) {
    /* nothing to do */
}

/* Implementation of the start callback for binary format */
unsafe fn CopyFromBinaryStart(cstate: CopyFromState, _tupDesc: TupleDesc) {
    /* Read and verify binary header */
    ReceiveCopyBinaryHeader(cstate);
}

/*
 * Implementation of the infunc callback for binary format. Assign
 * the binary input function to the given *finfo.
 */
unsafe fn CopyFromBinaryInFunc(
    _cstate: CopyFromState,
    atttypid: Oid,
    finfo: *mut FmgrInfo,
    typioparam: *mut Oid,
) {
    let mut func_oid: Oid = 0;

    getTypeBinaryInputInfo(atttypid, &mut func_oid, typioparam);
    fmgr_info(func_oid, finfo);
}

/* Implementation of the end callback for binary format */
unsafe fn CopyFromBinaryEnd(_cstate: CopyFromState) {
    /* nothing to do */
}

/*
 * error context callback for COPY FROM
 *
 * The argument for the error context must be CopyFromState.
 */
pub unsafe extern "C" fn CopyFromErrorCallback(arg: *mut c_void) {
    let cstate = arg as CopyFromState;
    let cs = cstate as *mut InternalCopyFromStateData;

    if (*cs).relname_only {
        errcontext!("COPY {}", {
            let name = (*cs).cur_relname;
            std::ffi::CStr::from_ptr(name).to_string_lossy()
        });
        return;
    }
    if (*cs).opts.binary {
        /* can't usefully display the data */
        if !(*cs).cur_attname.is_null() {
            errcontext!(
                "COPY {}, line {}, column {}",
                std::ffi::CStr::from_ptr((*cs).cur_relname).to_string_lossy(),
                (*cs).cur_lineno,
                std::ffi::CStr::from_ptr((*cs).cur_attname).to_string_lossy()
            );
        } else {
            errcontext!(
                "COPY {}, line {}",
                std::ffi::CStr::from_ptr((*cs).cur_relname).to_string_lossy(),
                (*cs).cur_lineno
            );
        }
    } else {
        if !(*cs).cur_attname.is_null() && !(*cs).cur_attval.is_null() {
            /* error is relevant to a particular column */
            let attval: *mut c_char = CopyLimitPrintoutLength((*cs).cur_attval);
            errcontext!(
                "COPY {}, line {}, column {}: \"{}\"",
                std::ffi::CStr::from_ptr((*cs).cur_relname).to_string_lossy(),
                (*cs).cur_lineno,
                std::ffi::CStr::from_ptr((*cs).cur_attname).to_string_lossy(),
                std::ffi::CStr::from_ptr(attval).to_string_lossy()
            );
            pfree(attval as *mut c_void);
        } else if !(*cs).cur_attname.is_null() {
            /* error is relevant to a particular column, value is NULL */
            errcontext!(
                "COPY {}, line {}, column {}: null input",
                std::ffi::CStr::from_ptr((*cs).cur_relname).to_string_lossy(),
                (*cs).cur_lineno,
                std::ffi::CStr::from_ptr((*cs).cur_attname).to_string_lossy()
            );
        } else {
            /*
             * Error is relevant to a particular line.
             *
             * If line_buf still contains the correct line, print it.
             */
            if (*cs).line_buf_valid {
                let lineval: *mut c_char = CopyLimitPrintoutLength((*cs).line_buf.data);
                errcontext!(
                    "COPY {}, line {}: \"{}\"",
                    std::ffi::CStr::from_ptr((*cs).cur_relname).to_string_lossy(),
                    (*cs).cur_lineno,
                    std::ffi::CStr::from_ptr(lineval).to_string_lossy()
                );
                pfree(lineval as *mut c_void);
            } else {
                errcontext!(
                    "COPY {}, line {}",
                    std::ffi::CStr::from_ptr((*cs).cur_relname).to_string_lossy(),
                    (*cs).cur_lineno
                );
            }
        }
    }
}

/*
 * Make sure we don't print an unreasonable amount of COPY data in a message.
 *
 * Returns a pstrdup'd copy of the input.
 */
pub unsafe fn CopyLimitPrintoutLength(str_: *const c_char) -> *mut c_char {
    const MAX_COPY_DATA_DISPLAY: c_int = 100;

    let slen: c_int = {
        extern "C" {
            fn strlen(s: *const c_char) -> usize;
        }
        strlen(str_) as c_int
    };
    let len: c_int;
    let res: *mut c_char;

    /* Fast path if definitely okay */
    if slen <= MAX_COPY_DATA_DISPLAY {
        return pstrdup(str_);
    }

    /* Apply encoding-dependent truncation */
    len = pg_mbcliplen(str_, slen, MAX_COPY_DATA_DISPLAY);

    /*
     * Truncate, and add "..." to show we truncated the input.
     */
    res = palloc((len + 4) as Size) as *mut c_char;
    {
        extern "C" {
            fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
            fn strcpy(dest: *mut c_char, src: *const c_char) -> *mut c_char;
        }
        memcpy(res as *mut c_void, str_ as *const c_void, len as usize);
        strcpy(res.offset(len as isize), c"...".as_ptr());
    }

    res
}

/*
 * Allocate memory and initialize a new CopyMultiInsertBuffer for this
 * ResultRelInfo.
 */
unsafe fn CopyMultiInsertBufferInit(rri: *mut ResultRelInfo) -> *mut CopyMultiInsertBuffer {
    let buffer: *mut CopyMultiInsertBuffer;

    buffer = palloc(size_of::<CopyMultiInsertBuffer>() as Size) as *mut CopyMultiInsertBuffer;
    {
        extern "C" {
            fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
        }
        memset(
            (*buffer).slots.as_mut_ptr() as *mut c_void,
            0,
            size_of::<*mut TupleTableSlot>() * MAX_BUFFERED_TUPLES,
        );
    }
    (*buffer).resultRelInfo = rri;
    (*buffer).bistate = if (*rri).ri_FdwRoutine.is_null() {
        GetBulkInsertState()
    } else {
        core::ptr::null_mut()
    };
    (*buffer).nused = 0;

    buffer
}

/*
 * Make a new buffer for this ResultRelInfo.
 */
#[inline]
unsafe fn CopyMultiInsertInfoSetupBuffer(
    miinfo: *mut CopyMultiInsertInfo,
    rri: *mut ResultRelInfo,
) {
    let buffer: *mut CopyMultiInsertBuffer;

    buffer = CopyMultiInsertBufferInit(rri);

    /* Setup back-link so we can easily find this buffer again */
    (*rri).ri_CopyMultiInsertBuffer = buffer as *mut crate::nodes::execnodes::CopyMultiInsertBuffer;
    /* Record that we're tracking this buffer */
    (*miinfo).multiInsertBuffers =
        lappend((*miinfo).multiInsertBuffers, buffer as *mut c_void);
}

/*
 * Initialize an already allocated CopyMultiInsertInfo.
 *
 * If rri is a non-partitioned table then a CopyMultiInsertBuffer is set up
 * for that table.
 */
unsafe fn CopyMultiInsertInfoInit(
    miinfo: *mut CopyMultiInsertInfo,
    rri: *mut ResultRelInfo,
    cstate: CopyFromState,
    estate: *mut EState,
    mycid: CommandId,
    ti_options: c_int,
) {
    (*miinfo).multiInsertBuffers = NIL;
    (*miinfo).bufferedTuples = 0;
    (*miinfo).bufferedBytes = 0;
    (*miinfo).cstate = cstate;
    (*miinfo).estate = estate;
    (*miinfo).mycid = mycid;
    (*miinfo).ti_options = ti_options;

    /*
     * Only setup the buffer when not dealing with a partitioned table.
     * Buffers for partitioned tables will just be setup when we need to send
     * tuples their way for the first time.
     */
    if (*(*(*rri).ri_RelationDesc).rd_rel).relkind != RELKIND_PARTITIONED_TABLE {
        CopyMultiInsertInfoSetupBuffer(miinfo, rri);
    }
}

/*
 * Returns true if the buffers are full
 */
#[inline]
unsafe fn CopyMultiInsertInfoIsFull(miinfo: *const CopyMultiInsertInfo) -> bool {
    if (*miinfo).bufferedTuples >= MAX_BUFFERED_TUPLES as c_int
        || (*miinfo).bufferedBytes >= MAX_BUFFERED_BYTES
    {
        return true;
    }
    false
}

/*
 * Returns true if we have no buffered tuples
 */
#[inline]
unsafe fn CopyMultiInsertInfoIsEmpty(miinfo: *const CopyMultiInsertInfo) -> bool {
    (*miinfo).bufferedTuples == 0
}

/*
 * Write the tuples stored in 'buffer' out to the table.
 */
#[inline]
unsafe fn CopyMultiInsertBufferFlush(
    miinfo: *mut CopyMultiInsertInfo,
    buffer: *mut CopyMultiInsertBuffer,
    processed: *mut int64,
) {
    let cstate: CopyFromState = (*miinfo).cstate;
    let cs = cstate as *mut InternalCopyFromStateData;
    let estate: *mut EState = (*miinfo).estate;
    let nused: c_int = (*buffer).nused;
    let resultRelInfo: *mut ResultRelInfo = (*buffer).resultRelInfo;
    let slots: *mut *mut TupleTableSlot = (*buffer).slots.as_mut_ptr();
    let mut i: c_int;

    if !(*resultRelInfo).ri_FdwRoutine.is_null() {
        let batch_size: c_int = (*resultRelInfo).ri_BatchSize;
        let mut sent: c_int = 0;

        Assert!((*buffer).bistate.is_null());

        /* Ensure that the FDW supports batching and it's enabled */
        Assert!(!(*(*resultRelInfo).ri_FdwRoutine).ExecForeignBatchInsert.is_none());
        Assert!(batch_size > 1);

        /*
         * We suppress error context information other than the relation name,
         * if one of the operations below fails.
         */
        Assert!(!(*cs).relname_only);
        (*cs).relname_only = true;

        while sent < nused {
            let size: c_int = if batch_size < nused - sent {
                batch_size
            } else {
                nused - sent
            };
            let mut inserted: c_int = size;
            let rslots: *mut *mut TupleTableSlot;

            /* insert into foreign table: let the FDW do it */
            rslots = ((*(*resultRelInfo).ri_FdwRoutine).ExecForeignBatchInsert.unwrap())(
                estate,
                resultRelInfo,
                slots.offset(sent as isize),
                core::ptr::null_mut(),
                &mut inserted,
            );

            sent += size;

            /* No need to do anything if there are no inserted rows */
            if inserted <= 0 {
                continue;
            }

            /* Triggers on foreign tables should not have transition tables */
            Assert!(
                (*resultRelInfo).ri_TrigDesc.is_null()
                    || !(*(*resultRelInfo).ri_TrigDesc).trig_insert_new_table
            );

            /* Run AFTER ROW INSERT triggers */
            if !(*resultRelInfo).ri_TrigDesc.is_null()
                && (*(*resultRelInfo).ri_TrigDesc).trig_insert_after_row
            {
                let relid: Oid = RelationGetRelid((*resultRelInfo).ri_RelationDesc);

                i = 0;
                while i < inserted {
                    let slot: *mut TupleTableSlot = *rslots.offset(i as isize);

                    /*
                     * AFTER ROW Triggers might reference the tableoid column,
                     * so (re-)initialize tts_tableOid before evaluating them.
                     */
                    (*slot).tts_tableOid = relid;

                    ExecARInsertTriggers(
                        estate,
                        resultRelInfo,
                        slot,
                        NIL,
                        (*cs).transition_capture,
                    );
                    i += 1;
                }
            }

            /* Update the row counter and progress of the COPY command */
            *processed += inserted as int64;
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, *processed);
        }

        i = 0;
        while i < nused {
            ExecClearTuple(*slots.offset(i as isize));
            i += 1;
        }

        /* reset relname_only */
        (*cs).relname_only = false;
    } else {
        let mycid: CommandId = (*miinfo).mycid;
        let ti_options: c_int = (*miinfo).ti_options;
        let line_buf_valid: bool = (*cs).line_buf_valid;
        let save_cur_lineno: uint64 = (*cs).cur_lineno;
        let oldcontext: MemoryContext;

        Assert!(!(*buffer).bistate.is_null());

        /*
         * Print error context information correctly, if one of the operations
         * below fails.
         */
        (*cs).line_buf_valid = false;

        /*
         * table_multi_insert may leak memory, so switch to short-lived memory
         * context before calling it.
         */
        oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
        table_multi_insert(
            (*resultRelInfo).ri_RelationDesc,
            slots,
            nused,
            mycid,
            ti_options,
            (*buffer).bistate,
        );
        MemoryContextSwitchTo(oldcontext);

        i = 0;
        while i < nused {
            /*
             * If there are any indexes, update them for all the inserted
             * tuples, and run AFTER ROW INSERT triggers.
             */
            if (*resultRelInfo).ri_NumIndices > 0 {
                let recheckIndexes: *mut List;

                (*cs).cur_lineno = (*buffer).linenos[i as usize];
                recheckIndexes = ExecInsertIndexTuples(
                    resultRelInfo,
                    *slots.offset(i as isize),
                    estate,
                    false,
                    false,
                    core::ptr::null_mut(),
                    NIL,
                    false,
                );
                ExecARInsertTriggers(
                    estate,
                    resultRelInfo,
                    *slots.offset(i as isize),
                    recheckIndexes,
                    (*cs).transition_capture,
                );
                list_free(recheckIndexes);
            }
            /*
             * There's no indexes, but see if we need to run AFTER ROW INSERT
             * triggers anyway.
             */
            else if !(*resultRelInfo).ri_TrigDesc.is_null()
                && ((*(*resultRelInfo).ri_TrigDesc).trig_insert_after_row
                    || (*(*resultRelInfo).ri_TrigDesc).trig_insert_new_table)
            {
                (*cs).cur_lineno = (*buffer).linenos[i as usize];
                ExecARInsertTriggers(
                    estate,
                    resultRelInfo,
                    *slots.offset(i as isize),
                    NIL,
                    (*cs).transition_capture,
                );
            }

            ExecClearTuple(*slots.offset(i as isize));
            i += 1;
        }

        /* Update the row counter and progress of the COPY command */
        *processed += nused as int64;
        pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, *processed);

        /* reset cur_lineno and line_buf_valid to what they were */
        (*cs).line_buf_valid = line_buf_valid;
        (*cs).cur_lineno = save_cur_lineno;
    }

    /* Mark that all slots are free */
    (*buffer).nused = 0;
}

/*
 * Drop used slots and free member for this buffer.
 *
 * The buffer must be flushed before cleanup.
 */
#[inline]
unsafe fn CopyMultiInsertBufferCleanup(
    miinfo: *mut CopyMultiInsertInfo,
    buffer: *mut CopyMultiInsertBuffer,
) {
    let resultRelInfo: *mut ResultRelInfo = (*buffer).resultRelInfo;
    let mut i: c_int;

    /* Ensure buffer was flushed */
    Assert!((*buffer).nused == 0);

    /* Remove back-link to ourself */
    (*resultRelInfo).ri_CopyMultiInsertBuffer = core::ptr::null_mut();

    if (*resultRelInfo).ri_FdwRoutine.is_null() {
        Assert!(!(*buffer).bistate.is_null());
        FreeBulkInsertState((*buffer).bistate);
    } else {
        Assert!((*buffer).bistate.is_null());
    }

    /* Since we only create slots on demand, just drop the non-null ones. */
    i = 0;
    while i < MAX_BUFFERED_TUPLES as c_int && !(*buffer).slots[i as usize].is_null() {
        ExecDropSingleTupleTableSlot((*buffer).slots[i as usize]);
        i += 1;
    }

    if (*resultRelInfo).ri_FdwRoutine.is_null() {
        table_finish_bulk_insert((*resultRelInfo).ri_RelationDesc, (*miinfo).ti_options);
    }

    pfree(buffer as *mut c_void);
}

/*
 * Write out all stored tuples in all buffers out to the tables.
 *
 * Once flushed we also trim the tracked buffers list down to size by removing
 * the buffers created earliest first.
 *
 * Callers should pass 'curr_rri' as the ResultRelInfo that's currently being
 * used.  When cleaning up old buffers we'll never remove the one for
 * 'curr_rri'.
 */
#[inline]
unsafe fn CopyMultiInsertInfoFlush(
    miinfo: *mut CopyMultiInsertInfo,
    curr_rri: *mut ResultRelInfo,
    processed: *mut int64,
) {
    let lc: *mut ListCell;

    // foreach loop over multiInsertBuffers
    lc = if !(*miinfo).multiInsertBuffers.is_null() {
        (*(*miinfo).multiInsertBuffers).elements as *mut ListCell
    } else {
        core::ptr::null_mut()
    };
    // Simple manual iteration since foreach! is a macro in this codebase
    {
        let list = (*miinfo).multiInsertBuffers;
        if !list.is_null() {
            let n = list_length(list) as usize;
            for idx in 0..n {
                let buffer = *((*list).elements as *mut *mut CopyMultiInsertBuffer).add(idx);
                CopyMultiInsertBufferFlush(miinfo, buffer, processed);
            }
        }
    }

    (*miinfo).bufferedTuples = 0;
    (*miinfo).bufferedBytes = 0;

    /*
     * Trim the list of tracked buffers down if it exceeds the limit.  Here we
     * remove buffers starting with the ones we created first.  It seems less
     * likely that these older ones will be needed than the ones that were
     * just created.
     */
    while list_length((*miinfo).multiInsertBuffers) > MAX_PARTITION_BUFFERS {
        let mut buffer: *mut CopyMultiInsertBuffer;

        buffer = linitial((*miinfo).multiInsertBuffers) as *mut CopyMultiInsertBuffer;

        /*
         * We never want to remove the buffer that's currently being used, so
         * if we happen to find that then move it to the end of the list.
         */
        if (*buffer).resultRelInfo == curr_rri {
            /*
             * The code below would misbehave if we were trying to reduce the
             * list to less than two items.
             * MAX_PARTITION_BUFFERS >= 2 is asserted via the const declaration
             */
            (*miinfo).multiInsertBuffers =
                list_delete_first((*miinfo).multiInsertBuffers);
            (*miinfo).multiInsertBuffers =
                lappend((*miinfo).multiInsertBuffers, buffer as *mut c_void);
            buffer = linitial((*miinfo).multiInsertBuffers) as *mut CopyMultiInsertBuffer;
        }

        CopyMultiInsertBufferCleanup(miinfo, buffer);
        (*miinfo).multiInsertBuffers = list_delete_first((*miinfo).multiInsertBuffers);
    }
}

/*
 * Cleanup allocated buffers and free memory
 */
#[inline]
unsafe fn CopyMultiInsertInfoCleanup(miinfo: *mut CopyMultiInsertInfo) {
    let list = (*miinfo).multiInsertBuffers;
    if !list.is_null() {
        let n = list_length(list) as usize;
        for idx in 0..n {
            let buffer = *((*list).elements as *mut *mut CopyMultiInsertBuffer).add(idx);
            CopyMultiInsertBufferCleanup(miinfo, buffer);
        }
    }
    list_free((*miinfo).multiInsertBuffers);
}

/*
 * Get the next TupleTableSlot that the next tuple should be stored in.
 *
 * Callers must ensure that the buffer is not full.
 *
 * Note: 'miinfo' is unused but has been included for consistency with the
 * other functions in this area.
 */
#[inline]
unsafe fn CopyMultiInsertInfoNextFreeSlot(
    _miinfo: *mut CopyMultiInsertInfo,
    rri: *mut ResultRelInfo,
) -> *mut TupleTableSlot {
    let buffer: *mut CopyMultiInsertBuffer =
        (*rri).ri_CopyMultiInsertBuffer as *mut CopyMultiInsertBuffer;
    let nused: c_int;

    Assert!(!buffer.is_null());
    Assert!((*buffer).nused < MAX_BUFFERED_TUPLES as c_int);

    nused = (*buffer).nused;

    if (*buffer).slots[nused as usize].is_null() {
        (*buffer).slots[nused as usize] =
            table_slot_create((*rri).ri_RelationDesc, core::ptr::null_mut());
    }
    (*buffer).slots[nused as usize]
}

/*
 * Record the previously reserved TupleTableSlot that was reserved by
 * CopyMultiInsertInfoNextFreeSlot as being consumed.
 */
#[inline]
unsafe fn CopyMultiInsertInfoStore(
    miinfo: *mut CopyMultiInsertInfo,
    rri: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    tuplen: c_int,
    lineno: uint64,
) {
    let buffer: *mut CopyMultiInsertBuffer =
        (*rri).ri_CopyMultiInsertBuffer as *mut CopyMultiInsertBuffer;

    Assert!(!buffer.is_null());
    Assert!(slot == (*buffer).slots[(*buffer).nused as usize]);

    /* Store the line number so we can properly report any errors later */
    (*buffer).linenos[(*buffer).nused as usize] = lineno;

    /* Record this slot as being used */
    (*buffer).nused += 1;

    /* Update how many tuples are stored and their size */
    (*miinfo).bufferedTuples += 1;
    (*miinfo).bufferedBytes += tuplen;
}

/*
 * Copy FROM file to relation.
 */
pub unsafe fn CopyFrom(cstate: CopyFromState) -> uint64 {
    let cs = cstate as *mut InternalCopyFromStateData;
    let mut resultRelInfo: *mut ResultRelInfo;
    let target_resultRelInfo: *mut ResultRelInfo;
    let mut prevResultRelInfo: *mut ResultRelInfo = core::ptr::null_mut();
    let estate: *mut EState = CreateExecutorState(); /* for ExecConstraints() */
    let mtstate: *mut ModifyTableState;
    let econtext: *mut ExprContext;
    let mut singleslot: *mut TupleTableSlot = core::ptr::null_mut();
    let oldcontext: MemoryContext = CurrentMemoryContext;

    let mut proute: *mut PartitionTupleRouting = core::ptr::null_mut();
    let mut errcallback: ErrorContextCallback;
    let mycid: CommandId = GetCurrentCommandId(true);
    let mut ti_options: c_int = 0; /* start with default options for insert */
    let mut bistate: BulkInsertState = core::ptr::null_mut();
    let insertMethod: CopyInsertMethod;
    let mut multiInsertInfo: CopyMultiInsertInfo = core::mem::zeroed(); /* pacify compiler */
    let mut processed: int64 = 0;
    let mut excluded: int64 = 0;
    let mut has_before_insert_row_trig: bool;
    let mut has_instead_insert_row_trig: bool;
    let mut leafpart_use_multi_insert: bool = false;

    Assert!(!(*cs).rel.is_null());
    Assert!(list_length((*cs).range_table) == 1);

    if (*cs).opts.on_error != COPY_ON_ERROR_STOP {
        Assert!(!(*cs).escontext.is_null());
    }

    /*
     * The target must be a plain, foreign, or partitioned relation, or have
     * an INSTEAD OF INSERT row trigger.  (Currently, such triggers are only
     * allowed on views, so we only hint about them in the view case.)
     */
    if (*(*(*cs).rel).rd_rel).relkind != RELKIND_RELATION
        && (*(*(*cs).rel).rd_rel).relkind != RELKIND_FOREIGN_TABLE
        && (*(*(*cs).rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE
        && !(!((*(*cs).rel).trigdesc.is_null())
            && (*((*(*cs).rel).trigdesc as *mut crate::nodes::execnodes::TriggerDesc))
                .trig_insert_instead_row)
    {
        if (*(*(*cs).rel).rd_rel).relkind == RELKIND_VIEW {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot copy to view \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName((*cs).rel))
                        .to_string_lossy()
                )
            );
        } else if (*(*(*cs).rel).rd_rel).relkind == RELKIND_MATVIEW {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot copy to materialized view \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName((*cs).rel))
                        .to_string_lossy()
                )
            );
        } else if (*(*(*cs).rel).rd_rel).relkind == RELKIND_SEQUENCE {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot copy to sequence \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName((*cs).rel))
                        .to_string_lossy()
                )
            );
        } else {
            ereport!(
                ERROR,
                errmsg!(
                    "cannot copy to non-table relation \"{}\"",
                    std::ffi::CStr::from_ptr(RelationGetRelationName((*cs).rel))
                        .to_string_lossy()
                )
            );
        }
    }

    /*
     * If the target file is new-in-transaction, we assume that checking FSM
     * for free space is a waste of time.  This could possibly be wrong, but
     * it's unlikely.
     */
    if RELKIND_HAS_STORAGE((*(*(*cs).rel).rd_rel).relkind)
        && ((*(*cs).rel).rd_createSubid != InvalidSubTransactionId
            || (*(*cs).rel).rd_firstRelfilelocatorSubid != InvalidSubTransactionId)
    {
        ti_options |= TABLE_INSERT_SKIP_FSM;
    }

    /*
     * Optimize if new relation storage was created in this subxact or one of
     * its committed children and we won't see those rows later as part of an
     * earlier scan or command. The subxact test ensures that if this subxact
     * aborts then the frozen rows won't be visible after xact cleanup.  Note
     * that the stronger test of exactly which subtransaction created it is
     * crucial for correctness of this optimization. The test for an earlier
     * scan or command tolerates false negatives. FREEZE causes other sessions
     * to see rows they would not see under MVCC, and a false negative merely
     * spreads that anomaly to the current session.
     */
    if (*cs).opts.freeze {
        /*
         * We currently disallow COPY FREEZE on partitioned tables.  The
         * reason for this is that we've simply not yet opened the partitions
         * to determine if the optimization can be applied to them.  We could
         * go and open them all here, but doing so may be quite a costly
         * overhead for small copies.  In any case, we may just end up routing
         * tuples to a small number of partitions.  It seems better just to
         * raise an ERROR for partitioned tables.
         */
        if (*(*(*cs).rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            ereport!(
                ERROR,
                errmsg!("cannot perform COPY FREEZE on a partitioned table")
            );
        }

        /* There's currently no support for COPY FREEZE on foreign tables. */
        if (*(*(*cs).rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE {
            ereport!(
                ERROR,
                errmsg!("cannot perform COPY FREEZE on a foreign table")
            );
        }

        /*
         * Tolerate one registration for the benefit of FirstXactSnapshot.
         * Scan-bearing queries generally create at least two registrations,
         * though relying on that is fragile, as is ignoring ActiveSnapshot.
         * Clear CatalogSnapshot to avoid counting its registration.  We'll
         * still detect ongoing catalog scans, each of which separately
         * registers the snapshot it uses.
         */
        InvalidateCatalogSnapshot();
        if !ThereAreNoPriorRegisteredSnapshots() || !ThereAreNoReadyPortals() {
            ereport!(
                ERROR,
                errmsg!("cannot perform COPY FREEZE because of prior transaction activity")
            );
        }

        if (*(*cs).rel).rd_createSubid != GetCurrentSubTransactionId()
            && (*(*cs).rel).rd_newRelfilelocatorSubid != GetCurrentSubTransactionId()
        {
            ereport!(
                ERROR,
                errmsg!("cannot perform COPY FREEZE because the table was not created or truncated in the current subtransaction")
            );
        }

        ti_options |= TABLE_INSERT_FROZEN;
    }

    /*
     * We need a ResultRelInfo so we can use the regular executor's
     * index-entry-making machinery.  (There used to be a huge amount of code
     * here that basically duplicated execUtils.c ...)
     */
    ExecInitRangeTable(
        estate,
        (*cs).range_table,
        (*cs).rteperminfos,
        bms_make_singleton(1) as *mut crate::nodes::bitmapset::Bitmapset,
    );
    resultRelInfo = makeNode!(ResultRelInfo, T_ResultRelInfo);
    target_resultRelInfo = resultRelInfo;
    ExecInitResultRelation(estate, resultRelInfo, 1);

    /* Verify the named relation is a valid target for INSERT */
    CheckValidResultRel(resultRelInfo, CMD_INSERT, ONCONFLICT_NONE, NIL);

    ExecOpenIndices(resultRelInfo, false);

    /*
     * Set up a ModifyTableState so we can let FDW(s) init themselves for
     * foreign-table result relation(s).
     */
    mtstate = makeNode!(ModifyTableState, T_ModifyTableState);
    (*mtstate).ps.plan = core::ptr::null_mut();
    (*mtstate).ps.state = estate;
    (*mtstate).operation = CMD_INSERT;
    (*mtstate).mt_nrels = 1;
    (*mtstate).resultRelInfo = resultRelInfo;
    (*mtstate).rootResultRelInfo = resultRelInfo;

    if !(*resultRelInfo).ri_FdwRoutine.is_null()
        && (*(*resultRelInfo).ri_FdwRoutine).BeginForeignInsert.is_some()
    {
        ((*(*resultRelInfo).ri_FdwRoutine).BeginForeignInsert.unwrap())(mtstate, resultRelInfo);
    }

    /*
     * Also, if the named relation is a foreign table, determine if the FDW
     * supports batch insert and determine the batch size (a FDW may support
     * batching, but it may be disabled for the server/table).
     *
     * If the FDW does not support batching, we set the batch size to 1.
     */
    if !(*resultRelInfo).ri_FdwRoutine.is_null()
        && (*(*resultRelInfo).ri_FdwRoutine).GetForeignModifyBatchSize.is_some()
        && (*(*resultRelInfo).ri_FdwRoutine).ExecForeignBatchInsert.is_some()
    {
        (*resultRelInfo).ri_BatchSize = ((*(*resultRelInfo).ri_FdwRoutine)
            .GetForeignModifyBatchSize
            .unwrap())(resultRelInfo);
    } else {
        (*resultRelInfo).ri_BatchSize = 1;
    }

    Assert!((*resultRelInfo).ri_BatchSize >= 1);

    /* Prepare to catch AFTER triggers. */
    AfterTriggerBeginQuery();

    /*
     * If there are any triggers with transition tables on the named relation,
     * we need to be prepared to capture transition tuples.
     *
     * Because partition tuple routing would like to know about whether
     * transition capture is active, we also set it in mtstate, which is
     * passed to ExecFindPartition() below.
     */
    (*cs).transition_capture = MakeTransitionCaptureState(
        (*(*cs).rel).trigdesc as *mut c_void,
        RelationGetRelid((*cs).rel),
        CMD_INSERT,
    );
    (*mtstate).mt_transition_capture = (*cs).transition_capture;

    /*
     * If the named relation is a partitioned table, initialize state for
     * CopyFrom tuple routing.
     */
    if (*(*(*cs).rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        proute = ExecSetupPartitionTupleRouting(estate, (*cs).rel);
    }

    if !(*cs).whereClause.is_null() {
        (*cs).qualexpr = ExecInitQual((*cs).whereClause as *mut List, &mut (*mtstate).ps);
    }

    /*
     * It's generally more efficient to prepare a bunch of tuples for
     * insertion, and insert them in one
     * table_multi_insert()/ExecForeignBatchInsert() call, than call
     * table_tuple_insert()/ExecForeignInsert() separately for every tuple.
     * However, there are a number of reasons why we might not be able to do
     * this.  These are explained below.
     */
    if !(*resultRelInfo).ri_TrigDesc.is_null()
        && ((*(*resultRelInfo).ri_TrigDesc).trig_insert_before_row
            || (*(*resultRelInfo).ri_TrigDesc).trig_insert_instead_row)
    {
        /*
         * Can't support multi-inserts when there are any BEFORE/INSTEAD OF
         * triggers on the table. Such triggers might query the table we're
         * inserting into and act differently if the tuples that have already
         * been processed and prepared for insertion are not there.
         */
        insertMethod = CIM_SINGLE;
    } else if !(*resultRelInfo).ri_FdwRoutine.is_null() && (*resultRelInfo).ri_BatchSize == 1 {
        /*
         * Can't support multi-inserts to a foreign table if the FDW does not
         * support batching, or it's disabled for the server or foreign table.
         */
        insertMethod = CIM_SINGLE;
    } else if !proute.is_null()
        && !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_insert_new_table
    {
        /*
         * For partitioned tables we can't support multi-inserts when there
         * are any statement level insert triggers. It might be possible to
         * allow partitioned tables with such triggers in the future, but for
         * now, CopyMultiInsertInfoFlush expects that any after row insert and
         * statement level insert triggers are on the same relation.
         */
        insertMethod = CIM_SINGLE;
    } else if (*cs).volatile_defexprs {
        /*
         * Can't support multi-inserts if there are any volatile default
         * expressions in the table.  Similarly to the trigger case above,
         * such expressions may query the table we're inserting into.
         *
         * Note: It does not matter if any partitions have any volatile
         * default expressions as we use the defaults from the target of the
         * COPY command.
         */
        insertMethod = CIM_SINGLE;
    } else if contain_volatile_functions((*cs).whereClause) {
        /*
         * Can't support multi-inserts if there are any volatile function
         * expressions in WHERE clause.  Similarly to the trigger case above,
         * such expressions may query the table we're inserting into.
         *
         * Note: the whereClause was already preprocessed in DoCopy(), so it's
         * okay to use contain_volatile_functions() directly.
         */
        insertMethod = CIM_SINGLE;
    } else {
        /*
         * For partitioned tables, we may still be able to perform bulk
         * inserts.  However, the possibility of this depends on which types
         * of triggers exist on the partition.  We must disable bulk inserts
         * if the partition is a foreign table that can't use batching or it
         * has any before row insert or insert instead triggers (same as we
         * checked above for the parent table).  Since the partition's
         * resultRelInfos are initialized only when we actually need to insert
         * the first tuple into them, we must have the intermediate insert
         * method of CIM_MULTI_CONDITIONAL to flag that we must later
         * determine if we can use bulk-inserts for the partition being
         * inserted into.
         */
        if !proute.is_null() {
            insertMethod = CIM_MULTI_CONDITIONAL;
        } else {
            insertMethod = CIM_MULTI;
        }

        CopyMultiInsertInfoInit(
            &mut multiInsertInfo,
            resultRelInfo,
            cstate,
            estate,
            mycid,
            ti_options,
        );
    }

    /*
     * If not using batch mode (which allocates slots as needed) set up a
     * tuple slot too. When inserting into a partitioned table, we also need
     * one, even if we might batch insert, to read the tuple in the root
     * partition's form.
     */
    if insertMethod == CIM_SINGLE || insertMethod == CIM_MULTI_CONDITIONAL {
        singleslot =
            table_slot_create((*resultRelInfo).ri_RelationDesc, &mut (*estate).es_tupleTable);
        bistate = GetBulkInsertState();
    }

    has_before_insert_row_trig = !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_insert_before_row;

    has_instead_insert_row_trig = !(*resultRelInfo).ri_TrigDesc.is_null()
        && (*(*resultRelInfo).ri_TrigDesc).trig_insert_instead_row;

    /*
     * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
     * should do this for COPY, since it's not really an "INSERT" statement as
     * such. However, executing these triggers maintains consistency with the
     * EACH ROW triggers that we already fire on COPY.
     */
    ExecBSInsertTriggers(estate, resultRelInfo);

    econtext = GetPerTupleExprContext(estate);

    /* Set up callback to identify error line number */
    errcallback = ErrorContextCallback {
        callback: Some(CopyFromErrorCallback),
        arg: cstate as *mut c_void,
        previous: error_context_stack,
    };
    error_context_stack = &mut errcallback;

    'main_loop: loop {
        let myslot: *mut TupleTableSlot;
        let mut skip_tuple: bool;

        CHECK_FOR_INTERRUPTS();

        /*
         * Reset the per-tuple exprcontext. We do this after every tuple, to
         * clean-up after expression evaluations etc.
         */
        ResetPerTupleExprContext(estate);

        /* select slot to (initially) load row into */
        if insertMethod == CIM_SINGLE || !proute.is_null() {
            let myslot_ = singleslot;
            Assert!(!myslot_.is_null());
            // assign below after let
            let mut myslot_mut = myslot_;

            /* Switch to per-tuple context before calling NextCopyFrom, which does
             * evaluate default expressions etc. and requires per-tuple context. */
            MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

            ExecClearTuple(myslot_mut);

            /* Directly store the values/nulls array in the slot */
            if !NextCopyFrom(
                cstate,
                econtext,
                (*myslot_mut).tts_values,
                (*myslot_mut).tts_isnull,
            ) {
                break 'main_loop;
            }

            if (*cs).opts.on_error == COPY_ON_ERROR_IGNORE
                && (*(*cs).escontext).error_occurred
            {
                /*
                 * Soft error occurred, skip this tuple and just make
                 * ErrorSaveContext ready for the next NextCopyFrom. Since we
                 * don't set details_wanted and error_data is not to be filled,
                 * just resetting error_occurred is enough.
                 */
                (*(*cs).escontext).error_occurred = false;

                /* Report that this tuple was skipped by the ON_ERROR clause */
                pgstat_progress_update_param(
                    PROGRESS_COPY_TUPLES_SKIPPED,
                    (*cs).num_errors as int64,
                );

                if (*cs).opts.reject_limit > 0
                    && (*cs).num_errors > (*cs).opts.reject_limit as uint64
                {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "skipped more than REJECT_LIMIT ({}) rows due to data type incompatibility",
                            (*cs).opts.reject_limit
                        )
                    );
                }

                /* Repeat NextCopyFrom() until no soft error occurs */
                continue 'main_loop;
            }

            ExecStoreVirtualTuple(myslot_mut);

            /*
             * Constraints and where clause might reference the tableoid column,
             * so (re-)initialize tts_tableOid before evaluating them.
             */
            (*myslot_mut).tts_tableOid =
                RelationGetRelid((*target_resultRelInfo).ri_RelationDesc);

            /* Triggers and stuff need to be invoked in query context. */
            MemoryContextSwitchTo(oldcontext);

            if !(*cs).whereClause.is_null() {
                (*econtext).ecxt_scantuple = myslot_mut;
                /* Skip items that don't match COPY's WHERE clause */
                if !ExecQual((*cs).qualexpr, econtext) {
                    /*
                     * Report that this tuple was filtered out by the WHERE
                     * clause.
                     */
                    excluded += 1;
                    pgstat_progress_update_param(PROGRESS_COPY_TUPLES_EXCLUDED, excluded);
                    continue 'main_loop;
                }
            }

            /* Determine the partition to insert the tuple into */
            if !proute.is_null() {
                let map: *mut TupleConversionMap;

                /*
                 * Attempt to find a partition suitable for this tuple.
                 * ExecFindPartition() will raise an error if none can be found or
                 * if the found partition is not suitable for INSERTs.
                 */
                resultRelInfo =
                    ExecFindPartition(mtstate, target_resultRelInfo, proute, myslot_mut, estate);

                if prevResultRelInfo != resultRelInfo {
                    /* Determine which triggers exist on this partition */
                    has_before_insert_row_trig = !(*resultRelInfo).ri_TrigDesc.is_null()
                        && (*(*resultRelInfo).ri_TrigDesc).trig_insert_before_row;

                    has_instead_insert_row_trig = !(*resultRelInfo).ri_TrigDesc.is_null()
                        && (*(*resultRelInfo).ri_TrigDesc).trig_insert_instead_row;

                    /*
                     * Disable multi-inserts when the partition has BEFORE/INSTEAD
                     * OF triggers, or if the partition is a foreign table that
                     * can't use batching.
                     */
                    leafpart_use_multi_insert = insertMethod == CIM_MULTI_CONDITIONAL
                        && !has_before_insert_row_trig
                        && !has_instead_insert_row_trig
                        && ((*resultRelInfo).ri_FdwRoutine.is_null()
                            || (*resultRelInfo).ri_BatchSize > 1);

                    /* Set the multi-insert buffer to use for this partition. */
                    if leafpart_use_multi_insert {
                        if (*resultRelInfo).ri_CopyMultiInsertBuffer.is_null() {
                            CopyMultiInsertInfoSetupBuffer(&mut multiInsertInfo, resultRelInfo);
                        }
                    } else if insertMethod == CIM_MULTI_CONDITIONAL
                        && !CopyMultiInsertInfoIsEmpty(&multiInsertInfo)
                    {
                        /*
                         * Flush pending inserts if this partition can't use
                         * batching, so rows are visible to triggers etc.
                         */
                        CopyMultiInsertInfoFlush(
                            &mut multiInsertInfo,
                            resultRelInfo,
                            &mut processed,
                        );
                    }

                    if !bistate.is_null() {
                        ReleaseBulkInsertStatePin(bistate);
                    }
                    prevResultRelInfo = resultRelInfo;
                }

                /*
                 * If we're capturing transition tuples, we might need to convert
                 * from the partition rowtype to root rowtype. But if there are no
                 * BEFORE triggers on the partition that could change the tuple,
                 * we can just remember the original unconverted tuple to avoid a
                 * needless round trip conversion.
                 */
                if !(*cs).transition_capture.is_null() {
                    (*(*cs).transition_capture).tcs_original_insert_tuple =
                        if !has_before_insert_row_trig {
                            myslot_mut
                        } else {
                            core::ptr::null_mut()
                        };
                }

                /*
                 * We might need to convert from the root rowtype to the partition
                 * rowtype.
                 */
                map = ExecGetRootToChildMap(resultRelInfo, estate)
                    as *mut TupleConversionMap;
                if insertMethod == CIM_SINGLE || !leafpart_use_multi_insert {
                    /* non batch insert */
                    if !map.is_null() {
                        let new_slot: *mut TupleTableSlot =
                            (*resultRelInfo).ri_PartitionTupleSlot;
                        myslot_mut =
                            execute_attr_map_slot((*map).attrMap, myslot_mut, new_slot);
                    }
                } else {
                    /*
                     * Prepare to queue up tuple for later batch insert into
                     * current partition.
                     */
                    let batchslot: *mut TupleTableSlot;

                    /* no other path available for partitioned table */
                    Assert!(insertMethod == CIM_MULTI_CONDITIONAL);

                    batchslot =
                        CopyMultiInsertInfoNextFreeSlot(&mut multiInsertInfo, resultRelInfo);

                    if !map.is_null() {
                        myslot_mut =
                            execute_attr_map_slot((*map).attrMap, myslot_mut, batchslot);
                    } else {
                        /*
                         * This looks more expensive than it is (Believe me, I
                         * optimized it away. Twice.). The input is in virtual
                         * form, and we'll materialize the slot below - for most
                         * slot types the copy performs the work materialization
                         * would later require anyway.
                         */
                        ExecCopySlot(batchslot, myslot_mut);
                        myslot_mut = batchslot;
                    }
                }

                /* ensure that triggers etc see the right relation  */
                (*myslot_mut).tts_tableOid =
                    RelationGetRelid((*resultRelInfo).ri_RelationDesc);
            }

            skip_tuple = false;

            /* BEFORE ROW INSERT Triggers */
            if has_before_insert_row_trig {
                if !ExecBRInsertTriggers(estate, resultRelInfo, myslot_mut) {
                    skip_tuple = true; /* "do nothing" */
                }
            }

            if !skip_tuple {
                /*
                 * If there is an INSTEAD OF INSERT ROW trigger, let it handle the
                 * tuple.  Otherwise, proceed with inserting the tuple into the
                 * table or foreign table.
                 */
                if has_instead_insert_row_trig {
                    ExecIRInsertTriggers(estate, resultRelInfo, myslot_mut);
                } else {
                    /* Compute stored generated columns */
                    if !(*(*(*resultRelInfo).ri_RelationDesc).rd_att).constr.is_null()
                        && (*(*(*(*resultRelInfo).ri_RelationDesc).rd_att).constr)
                            .has_generated_stored
                    {
                        ExecComputeStoredGenerated(
                            resultRelInfo,
                            estate,
                            myslot_mut,
                            CMD_INSERT,
                        );
                    }

                    /*
                     * If the target is a plain table, check the constraints of
                     * the tuple.
                     */
                    if (*resultRelInfo).ri_FdwRoutine.is_null()
                        && !(*(*(*resultRelInfo).ri_RelationDesc).rd_att).constr.is_null()
                    {
                        ExecConstraints(resultRelInfo, myslot_mut, estate);
                    }

                    /*
                     * Also check the tuple against the partition constraint, if
                     * there is one; except that if we got here via tuple-routing,
                     * we don't need to if there's no BR trigger defined on the
                     * partition.
                     */
                    if (*(*(*resultRelInfo).ri_RelationDesc).rd_rel).relispartition
                        && (proute.is_null() || has_before_insert_row_trig)
                    {
                        ExecPartitionCheck(resultRelInfo, myslot_mut, estate, true);
                    }

                    /* Store the slot in the multi-insert buffer, when enabled. */
                    if insertMethod == CIM_MULTI || leafpart_use_multi_insert {
                        /*
                         * The slot previously might point into the per-tuple
                         * context. For batching it needs to be longer lived.
                         */
                        ExecMaterializeSlot(myslot_mut);

                        /* Add this tuple to the tuple buffer */
                        CopyMultiInsertInfoStore(
                            &mut multiInsertInfo,
                            resultRelInfo,
                            myslot_mut,
                            (*cs).line_buf.len,
                            (*cs).cur_lineno,
                        );

                        /*
                         * If enough inserts have queued up, then flush all
                         * buffers out to their tables.
                         */
                        if CopyMultiInsertInfoIsFull(&multiInsertInfo) {
                            CopyMultiInsertInfoFlush(
                                &mut multiInsertInfo,
                                resultRelInfo,
                                &mut processed,
                            );
                        }

                        /*
                         * We delay updating the row counter and progress of the
                         * COPY command until after writing the tuples stored in
                         * the buffer out to the table, as in single insert mode.
                         * See CopyMultiInsertBufferFlush().
                         */
                        continue 'main_loop; /* next tuple please */
                    } else {
                        let mut recheckIndexes: *mut List = NIL;

                        /* OK, store the tuple */
                        if !(*resultRelInfo).ri_FdwRoutine.is_null() {
                            let new_myslot = ((*(*resultRelInfo).ri_FdwRoutine)
                                .ExecForeignInsert
                                .unwrap())(
                                estate,
                                resultRelInfo,
                                myslot_mut,
                                core::ptr::null_mut(),
                            );

                            if new_myslot.is_null() {
                                /* "do nothing" */
                                continue 'main_loop; /* next tuple please */
                            }
                            myslot_mut = new_myslot;

                            /*
                             * AFTER ROW Triggers might reference the tableoid
                             * column, so (re-)initialize tts_tableOid before
                             * evaluating them.
                             */
                            (*myslot_mut).tts_tableOid =
                                RelationGetRelid((*resultRelInfo).ri_RelationDesc);
                        } else {
                            /* OK, store the tuple and create index entries for it */
                            table_tuple_insert(
                                (*resultRelInfo).ri_RelationDesc,
                                myslot_mut,
                                mycid,
                                ti_options,
                                bistate as *mut crate::access::table::tableam::BulkInsertStateData,
                            );

                            if (*resultRelInfo).ri_NumIndices > 0 {
                                recheckIndexes = ExecInsertIndexTuples(
                                    resultRelInfo,
                                    myslot_mut,
                                    estate,
                                    false,
                                    false,
                                    core::ptr::null_mut(),
                                    NIL,
                                    false,
                                );
                            }
                        }

                        /* AFTER ROW INSERT Triggers */
                        ExecARInsertTriggers(
                            estate,
                            resultRelInfo,
                            myslot_mut,
                            recheckIndexes,
                            (*cs).transition_capture,
                        );

                        list_free(recheckIndexes);
                    }
                }

                /*
                 * We count only tuples not suppressed by a BEFORE INSERT trigger
                 * or FDW; this is the same definition used by nodeModifyTable.c
                 * for counting tuples inserted by an INSERT command.  Update
                 * progress of the COPY command as well.
                 */
                processed += 1;
                pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, processed);
            }
        } else {
            /* CIM_MULTI path - no proute */
            Assert!(resultRelInfo == target_resultRelInfo);
            Assert!(insertMethod == CIM_MULTI);

            let myslot_multi =
                CopyMultiInsertInfoNextFreeSlot(&mut multiInsertInfo, resultRelInfo);
            let myslot_mut = myslot_multi;

            MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

            ExecClearTuple(myslot_mut);

            if !NextCopyFrom(
                cstate,
                econtext,
                (*myslot_mut).tts_values,
                (*myslot_mut).tts_isnull,
            ) {
                break 'main_loop;
            }

            if (*cs).opts.on_error == COPY_ON_ERROR_IGNORE
                && (*(*cs).escontext).error_occurred
            {
                (*(*cs).escontext).error_occurred = false;
                pgstat_progress_update_param(
                    PROGRESS_COPY_TUPLES_SKIPPED,
                    (*cs).num_errors as int64,
                );
                if (*cs).opts.reject_limit > 0
                    && (*cs).num_errors > (*cs).opts.reject_limit as uint64
                {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "skipped more than REJECT_LIMIT ({}) rows due to data type incompatibility",
                            (*cs).opts.reject_limit
                        )
                    );
                }
                continue 'main_loop;
            }

            ExecStoreVirtualTuple(myslot_mut);

            (*myslot_mut).tts_tableOid =
                RelationGetRelid((*target_resultRelInfo).ri_RelationDesc);

            MemoryContextSwitchTo(oldcontext);

            if !(*cs).whereClause.is_null() {
                (*econtext).ecxt_scantuple = myslot_mut;
                if !ExecQual((*cs).qualexpr, econtext) {
                    excluded += 1;
                    pgstat_progress_update_param(PROGRESS_COPY_TUPLES_EXCLUDED, excluded);
                    continue 'main_loop;
                }
            }

            skip_tuple = false;

            /* BEFORE ROW INSERT Triggers */
            if has_before_insert_row_trig {
                if !ExecBRInsertTriggers(estate, resultRelInfo, myslot_mut) {
                    skip_tuple = true;
                }
            }

            if !skip_tuple {
                if has_instead_insert_row_trig {
                    ExecIRInsertTriggers(estate, resultRelInfo, myslot_mut);
                } else {
                    if !(*(*(*resultRelInfo).ri_RelationDesc).rd_att).constr.is_null()
                        && (*(*(*(*resultRelInfo).ri_RelationDesc).rd_att).constr)
                            .has_generated_stored
                    {
                        ExecComputeStoredGenerated(
                            resultRelInfo,
                            estate,
                            myslot_mut,
                            CMD_INSERT,
                        );
                    }
                    if (*resultRelInfo).ri_FdwRoutine.is_null()
                        && !(*(*(*resultRelInfo).ri_RelationDesc).rd_att).constr.is_null()
                    {
                        ExecConstraints(resultRelInfo, myslot_mut, estate);
                    }
                    if (*(*(*resultRelInfo).ri_RelationDesc).rd_rel).relispartition
                        && (proute.is_null() || has_before_insert_row_trig)
                    {
                        ExecPartitionCheck(resultRelInfo, myslot_mut, estate, true);
                    }

                    /* insertMethod == CIM_MULTI */
                    ExecMaterializeSlot(myslot_mut);

                    CopyMultiInsertInfoStore(
                        &mut multiInsertInfo,
                        resultRelInfo,
                        myslot_mut,
                        (*cs).line_buf.len,
                        (*cs).cur_lineno,
                    );

                    if CopyMultiInsertInfoIsFull(&multiInsertInfo) {
                        CopyMultiInsertInfoFlush(
                            &mut multiInsertInfo,
                            resultRelInfo,
                            &mut processed,
                        );
                    }

                    continue 'main_loop;
                }

                processed += 1;
                pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, processed);
            }
        }
    }

    /* Flush any remaining buffered tuples */
    if insertMethod != CIM_SINGLE {
        if !CopyMultiInsertInfoIsEmpty(&multiInsertInfo) {
            CopyMultiInsertInfoFlush(
                &mut multiInsertInfo,
                core::ptr::null_mut(),
                &mut processed,
            );
        }
    }

    /* Done, clean up */
    error_context_stack = errcallback.previous;

    if (*cs).opts.on_error != COPY_ON_ERROR_STOP
        && (*cs).num_errors > 0
        && (*cs).opts.log_verbosity >= COPY_LOG_VERBOSITY_DEFAULT
    {
        ereport!(
            NOTICE,
            errmsg_plural!(
                "{} row was skipped due to data type incompatibility",
                "{} rows were skipped due to data type incompatibility",
                (*cs).num_errors,
                (*cs).num_errors
            )
        );
    }

    if !bistate.is_null() {
        FreeBulkInsertState(bistate);
    }

    MemoryContextSwitchTo(oldcontext);

    /* Execute AFTER STATEMENT insertion triggers */
    ExecASInsertTriggers(estate, target_resultRelInfo, (*cs).transition_capture);

    /* Handle queued AFTER triggers */
    AfterTriggerEndQuery(estate);

    ExecResetTupleTable((*estate).es_tupleTable, false);

    /* Allow the FDW to shut down */
    if !(*target_resultRelInfo).ri_FdwRoutine.is_null()
        && (*(*target_resultRelInfo).ri_FdwRoutine).EndForeignInsert.is_some()
    {
        ((*(*target_resultRelInfo).ri_FdwRoutine).EndForeignInsert.unwrap())(
            estate,
            target_resultRelInfo,
        );
    }

    /* Tear down the multi-insert buffer data */
    if insertMethod != CIM_SINGLE {
        CopyMultiInsertInfoCleanup(&mut multiInsertInfo);
    }

    /* Close all the partitioned tables, leaf partitions, and their indices */
    if !proute.is_null() {
        ExecCleanupTupleRouting(mtstate, proute);
    }

    /* Close the result relations, including any trigger target relations */
    ExecCloseResultRelations(estate);
    ExecCloseRangeTableRelations(estate);

    FreeExecutorState(estate);

    processed as uint64
}

/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'whereClause': WHERE clause from the COPY FROM command
 * 'filename': Name of server-local file to read, NULL for STDIN
 * 'is_program': true if 'filename' is program to execute
 * 'data_source_cb': callback that provides the input data
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyFromState, to be passed to NextCopyFrom and related functions.
 */
pub unsafe fn BeginCopyFrom(
    pstate: *mut ParseState,
    rel: Relation,
    whereClause: *mut Node,
    filename: *const c_char,
    is_program: bool,
    data_source_cb: crate::commands::copy::copy_data_source_cb,
    attnamelist: *mut List,
    options: *mut List,
) -> CopyFromState {
    let cstate: CopyFromState;
    let cs: *mut InternalCopyFromStateData;
    let pipe: bool = filename.is_null();
    let tupDesc: TupleDesc;
    let num_phys_attrs: AttrNumber;
    let mut num_defaults: AttrNumber;
    let in_functions: *mut FmgrInfo;
    let typioparams: *mut Oid;
    let defmap: *mut c_int;
    let defexprs: *mut *mut ExprState;
    let oldcontext: MemoryContext;
    let mut volatile_defexprs: bool;
    let progress_cols: [c_int; 3] = [
        PROGRESS_COPY_COMMAND,
        PROGRESS_COPY_TYPE,
        PROGRESS_COPY_BYTES_TOTAL,
    ];
    let mut progress_vals: [int64; 3] = [PROGRESS_COPY_COMMAND_FROM as int64, 0, 0];

    /* Allocate workspace and zero all fields */
    cstate = palloc0(size_of::<InternalCopyFromStateData>() as Size) as CopyFromState;
    cs = cstate as *mut InternalCopyFromStateData;

    /*
     * We allocate everything used by a cstate in a new memory context. This
     * avoids memory leaks during repeated use of COPY in a query.
     */
    (*cs).copycontext = as_memnodes_ctx(AllocSetContextCreate!(
        CurrentMemoryContext,
        c"COPY".as_ptr() as *const c_char,
        ALLOCSET_DEFAULT_SIZES
    ));

    oldcontext = MemoryContextSwitchTo(as_palloc_ctx((*cs).copycontext));

    /* Extract options from the statement node tree */
    ProcessCopyOptions(pstate, &mut (*cs).opts, true /* is_from */, options);

    /* Set the format routine */
    (*cs).routine = CopyFromGetRoutine(&(*cs).opts);

    /* Process the target relation */
    (*cs).rel = rel;

    tupDesc = RelationGetDescr((*cs).rel);

    /* process common options or initialization */

    /* Generate or convert list of attributes to process */
    (*cs).attnumlist = CopyGetAttnums(tupDesc, (*cs).rel, attnamelist);

    num_phys_attrs = (*tupDesc).natts as AttrNumber;

    /* Convert FORCE_NOT_NULL name list to per-column flags, check validity */
    (*cs).opts.force_notnull_flags =
        palloc0((num_phys_attrs as Size) * size_of::<bool>()) as *mut bool;
    if (*cs).opts.force_notnull_all {
        MemSet(
            (*cs).opts.force_notnull_flags as *mut c_void,
            1, // true
            (num_phys_attrs as Size) * size_of::<bool>(),
        );
    } else if !(*cs).opts.force_notnull.is_null() {
        let attnums: *mut List;
        let mut cur: *mut ListCell;

        attnums = CopyGetAttnums(tupDesc, (*cs).rel, (*cs).opts.force_notnull);

        // foreach(cur, attnums)
        {
            let n = list_length(attnums) as usize;
            for idx in 0..n {
                let attnum: c_int = *((*attnums).elements as *mut c_int).add(idx);
                let attr: Form_pg_attribute = TupleDescAttr(tupDesc, attnum - 1);

                if !list_member_int((*cs).attnumlist, attnum) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "{} column \"{}\" not referenced by COPY",
                            "FORCE_NOT_NULL",
                            std::ffi::CStr::from_ptr(NameStr_attr(&(*attr).attname))
                                .to_string_lossy()
                        )
                    );
                }
                *(*cs).opts.force_notnull_flags.offset((attnum - 1) as isize) = true;
            }
        }
    }

    /* Set up soft error handler for ON_ERROR */
    if (*cs).opts.on_error != COPY_ON_ERROR_STOP {
        (*cs).escontext = makeNode!(ErrorSaveContext, T_ErrorSaveContext);
        (*(*cs).escontext).error_occurred = false;

        /*
         * Currently we only support COPY_ON_ERROR_IGNORE. We'll add other
         * options later
         */
        if (*cs).opts.on_error == COPY_ON_ERROR_IGNORE {
            (*(*cs).escontext).details_wanted = false;
        }
    } else {
        (*cs).escontext = core::ptr::null_mut();
    }

    /* Convert FORCE_NULL name list to per-column flags, check validity */
    (*cs).opts.force_null_flags =
        palloc0((num_phys_attrs as Size) * size_of::<bool>()) as *mut bool;
    if (*cs).opts.force_null_all {
        MemSet(
            (*cs).opts.force_null_flags as *mut c_void,
            1,
            (num_phys_attrs as Size) * size_of::<bool>(),
        );
    } else if !(*cs).opts.force_null.is_null() {
        let attnums: *mut List;

        attnums = CopyGetAttnums(tupDesc, (*cs).rel, (*cs).opts.force_null);

        {
            let n = list_length(attnums) as usize;
            for idx in 0..n {
                let attnum: c_int = *((*attnums).elements as *mut c_int).add(idx);
                let attr: Form_pg_attribute = TupleDescAttr(tupDesc, attnum - 1);

                if !list_member_int((*cs).attnumlist, attnum) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "{} column \"{}\" not referenced by COPY",
                            "FORCE_NULL",
                            std::ffi::CStr::from_ptr(NameStr_attr(&(*attr).attname))
                                .to_string_lossy()
                        )
                    );
                }
                *(*cs).opts.force_null_flags.offset((attnum - 1) as isize) = true;
            }
        }
    }

    /* Convert convert_selectively name list to per-column flags */
    if (*cs).opts.convert_selectively {
        let attnums: *mut List;

        (*cs).convert_select_flags =
            palloc0((num_phys_attrs as Size) * size_of::<bool>()) as *mut bool;

        attnums = CopyGetAttnums(tupDesc, (*cs).rel, (*cs).opts.convert_select);

        {
            let n = list_length(attnums) as usize;
            for idx in 0..n {
                let attnum: c_int = *((*attnums).elements as *mut c_int).add(idx);
                let attr: Form_pg_attribute = TupleDescAttr(tupDesc, attnum - 1);

                if !list_member_int((*cs).attnumlist, attnum) {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "selected column \"{}\" not referenced by COPY",
                            std::ffi::CStr::from_ptr(NameStr_attr(&(*attr).attname))
                                .to_string_lossy()
                        )
                    );
                }
                *(*cs).convert_select_flags.offset((attnum - 1) as isize) = true;
            }
        }
    }

    /* Use client encoding when ENCODING option is not specified. */
    if (*cs).opts.file_encoding < 0 {
        (*cs).file_encoding = pg_get_client_encoding();
    } else {
        (*cs).file_encoding = (*cs).opts.file_encoding;
    }

    /*
     * Look up encoding conversion function.
     */
    if (*cs).file_encoding == GetDatabaseEncoding()
        || (*cs).file_encoding == PG_SQL_ASCII
        || GetDatabaseEncoding() == PG_SQL_ASCII
    {
        (*cs).need_transcoding = false;
    } else {
        (*cs).need_transcoding = true;
        (*cs).conversion_proc = FindDefaultConversionProc(
            (*cs).file_encoding,
            GetDatabaseEncoding(),
        );
        if !OidIsValid((*cs).conversion_proc) {
            ereport!(
                ERROR,
                errmsg!(
                    "default conversion function for encoding \"{}\" to \"{}\" does not exist",
                    std::ffi::CStr::from_ptr(pg_encoding_to_char((*cs).file_encoding))
                        .to_string_lossy(),
                    std::ffi::CStr::from_ptr(pg_encoding_to_char(GetDatabaseEncoding()))
                        .to_string_lossy()
                )
            );
        }
    }

    (*cs).copy_src = COPY_FILE; /* default */

    (*cs).whereClause = whereClause;

    /* Initialize state variables */
    (*cs).eol_type = EOL_UNKNOWN;
    (*cs).cur_relname = RelationGetRelationName((*cs).rel);
    (*cs).cur_lineno = 0;
    (*cs).cur_attname = core::ptr::null();
    (*cs).cur_attval = core::ptr::null();
    (*cs).relname_only = false;

    /*
     * Allocate buffers for the input pipeline.
     *
     * attribute_buf and raw_buf are used in both text and binary modes, but
     * input_buf and line_buf only in text mode.
     */
    (*cs).raw_buf = palloc((RAW_BUF_SIZE + 1) as Size) as *mut c_char;
    (*cs).raw_buf_index = 0;
    (*cs).raw_buf_len = 0;
    (*cs).raw_reached_eof = false;

    initStringInfo(&mut (*cs).attribute_buf);

    /* Assign range table and rteperminfos, we'll need them in CopyFrom. */
    if !pstate.is_null() {
        (*cs).range_table = (*pstate).p_rtable;
        (*cs).rteperminfos = (*pstate).p_rteperminfos;
    }

    num_defaults = 0;
    volatile_defexprs = false;

    /*
     * Pick up the required catalog information for each attribute in the
     * relation, including the input function, the element type (to pass to
     * the input function), and info about defaults and constraints. (Which
     * input function we use depends on text/binary format choice.)
     */
    in_functions = palloc((num_phys_attrs as Size) * size_of::<FmgrInfo>()) as *mut FmgrInfo;
    typioparams = palloc((num_phys_attrs as Size) * size_of::<Oid>()) as *mut Oid;
    defmap = palloc((num_phys_attrs as Size) * size_of::<c_int>()) as *mut c_int;
    defexprs =
        palloc((num_phys_attrs as Size) * size_of::<*mut ExprState>()) as *mut *mut ExprState;

    let mut attnum: c_int = 1;
    while attnum <= num_phys_attrs as c_int {
        let att: Form_pg_attribute = TupleDescAttr(tupDesc, attnum - 1);

        /* We don't need info for dropped attributes */
        if (*att).attisdropped {
            attnum += 1;
            continue;
        }

        /* Fetch the input function and typioparam info */
        ((*(*cs).routine).CopyFromInFunc.unwrap())(
            cstate,
            (*att).atttypid,
            in_functions.offset((attnum - 1) as isize),
            typioparams.offset((attnum - 1) as isize),
        );

        /* Get default info if available */
        *defexprs.offset((attnum - 1) as isize) = core::ptr::null_mut();

        /*
         * We only need the default values for columns that do not appear in
         * the column list, unless the DEFAULT option was given. We never need
         * default values for generated columns.
         */
        if (!(*cs).opts.default_print.is_null()
            || !list_member_int((*cs).attnumlist, attnum))
            && (*att).attgenerated == 0
        {
            let defexpr: *mut Expr = build_column_default((*cs).rel, attnum as AttrNumber);

            if !defexpr.is_null() {
                /* Run the expression through planner */
                let defexpr = expression_planner(defexpr as *mut crate::nodes::primnodes::Expr)
                    as *mut Expr;

                /* Initialize executable expression in copycontext */
                *defexprs.offset((attnum - 1) as isize) =
                    ExecInitExpr(defexpr as *mut crate::nodes::primnodes::Expr, core::ptr::null_mut());

                /* if NOT copied from input */
                /* use default value if one exists */
                if !list_member_int((*cs).attnumlist, attnum) {
                    *defmap.offset(num_defaults as isize) = attnum - 1;
                    num_defaults += 1;
                }

                /*
                 * If a default expression looks at the table being loaded,
                 * then it could give the wrong answer when using
                 * multi-insert. Since database access can be dynamic this is
                 * hard to test for exactly, so we use the much wider test of
                 * whether the default expression is volatile. We allow for
                 * the special case of when the default expression is the
                 * nextval() of a sequence which in this specific case is
                 * known to be safe for use with the multi-insert
                 * optimization. Hence we use this special case function
                 * checker rather than the standard check for
                 * contain_volatile_functions().  Note also that we already
                 * ran the expression through expression_planner().
                 */
                if !volatile_defexprs {
                    volatile_defexprs =
                        contain_volatile_functions_not_nextval(defexpr as *mut Node);
                }
            }
        }

        attnum += 1;
    }

    (*cs).defaults =
        palloc0(((*tupDesc).natts as Size) * size_of::<bool>()) as *mut bool;

    /* initialize progress */
    pgstat_progress_start_command(
        PROGRESS_COMMAND_COPY,
        if !(*cs).rel.is_null() {
            RelationGetRelid((*cs).rel)
        } else {
            InvalidOid
        },
    );
    (*cs).bytes_processed = 0;

    /* We keep those variables in cstate. */
    (*cs).in_functions = in_functions;
    (*cs).typioparams = typioparams;
    (*cs).defmap = defmap;
    (*cs).defexprs = defexprs;
    (*cs).volatile_defexprs = volatile_defexprs;
    (*cs).num_defaults = num_defaults;
    (*cs).is_program = is_program;

    if let Some(_cb) = data_source_cb {
        progress_vals[1] = PROGRESS_COPY_TYPE_CALLBACK as int64;
        (*cs).copy_src = COPY_CALLBACK;
        (*cs).data_source_cb = data_source_cb;
    } else if pipe {
        progress_vals[1] = PROGRESS_COPY_TYPE_PIPE as int64;
        Assert!(!is_program); /* the grammar does not allow this */
        if whereToSendOutput == DestRemote {
            ReceiveCopyBegin(cstate);
        } else {
            (*cs).copy_file = stdin_ptr;
        }
    } else {
        (*cs).filename = pstrdup(filename);

        if (*cs).is_program {
            progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM as int64;
            (*cs).copy_file = OpenPipeStream((*cs).filename, PG_BINARY_R);
            if (*cs).copy_file.is_null() {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not execute command \"{}\": {}",
                        std::ffi::CStr::from_ptr((*cs).filename).to_string_lossy(),
                        "error"
                    )
                );
            }
        } else {
            let mut st: stat = std::mem::zeroed();

            progress_vals[1] = PROGRESS_COPY_TYPE_FILE as int64;
            (*cs).copy_file = AllocateFile((*cs).filename, PG_BINARY_R);
            if (*cs).copy_file.is_null() {
                /* copy errno because ereport subfunctions might change it */
                let save_errno: c_int = get_errno();

                ereport!(
                    ERROR,
                    errmsg!(
                        "could not open file \"{}\" for reading: {}",
                        std::ffi::CStr::from_ptr((*cs).filename).to_string_lossy(),
                        "error"
                    )
                );
            }

            if fstat(fileno((*cs).copy_file), &mut st) != 0 {
                ereport!(
                    ERROR,
                    errmsg!(
                        "could not stat file \"{}\": {}",
                        std::ffi::CStr::from_ptr((*cs).filename).to_string_lossy(),
                        "error"
                    )
                );
            }

            if S_ISDIR(st.st_mode) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "\"{}\" is a directory",
                        std::ffi::CStr::from_ptr((*cs).filename).to_string_lossy()
                    )
                );
            }

            progress_vals[2] = st.st_size;
        }
    }

    pgstat_progress_update_multi_param(3, progress_cols.as_ptr(), progress_vals.as_ptr());

    ((*(*cs).routine).CopyFromStart.unwrap())(cstate, tupDesc);

    MemoryContextSwitchTo(oldcontext);

    cstate
}

/*
 * Clean up storage and release resources for COPY FROM.
 */
pub unsafe fn EndCopyFrom(cstate: CopyFromState) {
    let cs = cstate as *mut InternalCopyFromStateData;

    /* Invoke the end callback */
    ((*(*cs).routine).CopyFromEnd.unwrap())(cstate);

    /* No COPY FROM related resources except memory. */
    if (*cs).is_program {
        ClosePipeFromProgram(cstate);
    } else {
        if !(*cs).filename.is_null() && FreeFile((*cs).copy_file) != 0 {
            ereport!(
                ERROR,
                errmsg!(
                    "could not close file \"{}\": {}",
                    std::ffi::CStr::from_ptr((*cs).filename).to_string_lossy(),
                    "error"
                )
            );
        }
    }

    pgstat_progress_end_command();

    MemoryContextDelete(as_palloc_ctx((*cs).copycontext));
    pfree(cstate as *mut c_void);
}

/*
 * Closes the pipe from an external program, checking the pclose() return code.
 */
unsafe fn ClosePipeFromProgram(cstate: CopyFromState) {
    let cs = cstate as *mut InternalCopyFromStateData;
    let pclose_rc: c_int;

    Assert!((*cs).is_program);

    pclose_rc = ClosePipeStream((*cs).copy_file);
    if pclose_rc == -1 {
        ereport!(
            ERROR,
            errmsg!("could not close pipe to external command: {}", "error")
        );
    } else if pclose_rc != 0 {
        /*
         * If we ended a COPY FROM PROGRAM before reaching EOF, then it's
         * expectable for the called program to fail with SIGPIPE, and we
         * should not report that as an error.  Otherwise, SIGPIPE indicates a
         * problem.
         */
        if !(*cs).raw_reached_eof && wait_result_is_signal(pclose_rc, SIGPIPE) {
            return;
        }

        ereport!(
            ERROR,
            errmsg!(
                "program \"{}\" failed: {}",
                std::ffi::CStr::from_ptr((*cs).filename).to_string_lossy(),
                std::ffi::CStr::from_ptr(wait_result_to_str(pclose_rc)).to_string_lossy()
            )
        );
    }
}

/* -------------------------------------------------------------------------
 * Additional struct field stubs needed above (ri_CopyMultiInsertBuffer,
 * ri_BatchSize, rd_rel, rd_att, trigdesc, etc.) are expected to be present
 * in the real ResultRelInfo / RelationData definitions in their canonical
 * modules once those are fully ported.  The field accesses above will be
 * reconciled at that time.
 * ------------------------------------------------------------------------- */

/* Stub for ErrorContextCallback (local, matching parse_node.rs pattern) */
#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe extern "C" fn(arg: *mut c_void)>,
    pub arg: *mut c_void,
}

/* Local error_context_stack (matches vacuumparallel.rs pattern) */
static mut error_context_stack: *mut ErrorContextCallback = core::ptr::null_mut();

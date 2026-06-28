//! Translated from PostgreSQL src/include/executor/spi.h
//!
//! Server Programming Interface public declarations. The C status-code split
//! (SPI_OK_* success, SPI_ERROR_* negative) maps to `Result<SpiOk, SpiError>`
//! per function-mapping.md 3.2.

#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use bitflags::bitflags;

use crate::access::htup::{HeapTuple, HeapTupleHeaderData};
use crate::access::tupdesc::TupleDesc;
use crate::c::{SubTransactionId, Size};
use crate::commands::trigger::TriggerData;
use crate::nodes::nodes::Node;
use crate::nodes::params::{ParamListInfo, ParserSetupHook};
use crate::nodes::parsenodes::FetchDirection;
use crate::parser::parser::RawParseMode;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::tcop::dest::DestReceiver;
use crate::utils::plancache::CachedPlan;
use crate::utils::portal::Portal;
use crate::utils::queryenvironment::EphemeralNamedRelation;
use crate::utils::rel::Relation;
use crate::utils::resowner::ResourceOwner;
use crate::utils::snapshot::Snapshot;

/// Result table returned by SPI queries. `vals`/`numvals` are the public part;
/// the rest is internal bookkeeping (the slist link collapses under the owned
/// Rust model). `List *` of nodes -> `Vec<Node>`.
pub struct SPITupleTable {
    // Public members:
    pub tupdesc: TupleDesc,
    pub vals: Vec<HeapTuple>,
    pub numvals: u64,
    // Private members:
    pub alloced: u64,
    pub tuptabcxt: crate::utils::palloc::MemoryContext,
    // slist_node next -> intrusive link dropped under owned model
    pub subid: SubTransactionId,
}

/// Optional arguments for SPI_prepare_extended. `void *parserSetupArg` -> a
/// closure capture would replace it; kept as a unit placeholder for now.
pub struct SPIPrepareOptions {
    pub parser_setup: Option<ParserSetupHook>,
    // parserSetupArg (void *) -> closure capture; dropped here. TODO(ptr)
    pub parse_mode: RawParseMode,
    pub cursor_options: i32,
}

/// Optional arguments for SPI_execute[_plan]_extended.
pub struct SPIExecuteOptions {
    pub params: ParamListInfo,
    pub read_only: bool,
    pub allow_nonatomic: bool,
    pub must_return_tuples: bool,
    pub tcount: u64,
    // TODO(ptr): dest is borrowed for the run.
    pub dest: Option<Box<dyn DestReceiver>>,
    pub owner: Option<ResourceOwner>,
}

/// Optional arguments for SPI_cursor_parse_open.
pub struct SPIParseOpenOptions {
    pub params: ParamListInfo,
    pub cursor_options: i32,
    pub read_only: bool,
}

/// Plans are opaque to standard SPI users (`typedef struct _SPI_plan *`).
pub struct _SPI_plan {
    _private: (),
}

/// Opaque plan handle.
pub type SPIPlanPtr = *mut _SPI_plan; // TODO(ptr)

bitflags! {
    /// SPI_OPT_* connect flags (`SPI_connect_ext`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SpiOpt: i32 {
        const NONATOMIC = 1 << 0;
    }
}

/// Success codes (the `SPI_OK_*` constants), the `Ok` arm of SPI results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpiOk {
    Connect = 1,
    Finish = 2,
    Fetch = 3,
    Utility = 4,
    Select = 5,
    SelInto = 6,
    Insert = 7,
    Delete = 8,
    Update = 9,
    Cursor = 10,
    InsertReturning = 11,
    DeleteReturning = 12,
    UpdateReturning = 13,
    Rewritten = 14,
    RelRegister = 15,
    RelUnregister = 16,
    TdRegister = 17,
    Merge = 18,
    MergeReturning = 19,
}

/// Error codes (the `SPI_ERROR_*` constants), the `Err` arm of SPI results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpiError {
    Connect = -1,
    Copy = -2,
    OpUnknown = -3,
    Unconnected = -4,
    Cursor = -5, // not used anymore
    Argument = -6,
    Param = -7,
    Transaction = -8,
    NoAttribute = -9,
    NoOutFunc = -10,
    TypUnknown = -11,
    RelDuplicate = -12,
    RelNotFound = -13,
}

// Process-global SPI result state. TODO(global): move to session/task state.
pub static mut SPI_processed: u64 = 0;
pub static mut SPI_tuptable: Option<Box<SPITupleTable>> = None;
pub static mut SPI_result: i32 = 0;

pub fn SPI_connect() -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_connect_ext(_options: SpiOpt) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_finish() -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_execute(_src: &str, _read_only: bool, _tcount: i64) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_execute_extended(_src: &str, _options: &SPIExecuteOptions) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_execute_plan(
    _plan: SPIPlanPtr,
    _values: &[Datum],
    _nulls: Option<&str>,
    _read_only: bool,
    _tcount: i64,
) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_execute_plan_extended(
    _plan: SPIPlanPtr,
    _options: &SPIExecuteOptions,
) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_execute_plan_with_paramlist(
    _plan: SPIPlanPtr,
    _params: ParamListInfo,
    _read_only: bool,
    _tcount: i64,
) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_exec(_src: &str, _tcount: i64) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_execp(
    _plan: SPIPlanPtr,
    _values: &[Datum],
    _nulls: Option<&str>,
    _tcount: i64,
) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_execute_snapshot(
    _plan: SPIPlanPtr,
    _values: &[Datum],
    _nulls: Option<&str>,
    _snapshot: Snapshot,
    _crosscheck_snapshot: Snapshot,
    _read_only: bool,
    _fire_triggers: bool,
    _tcount: i64,
) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_execute_with_args(
    _src: &str,
    _argtypes: &[Oid],
    _values: &[Datum],
    _nulls: Option<&str>,
    _read_only: bool,
    _tcount: i64,
) -> Result<SpiOk, SpiError> {
    unimplemented!()
}

// Plan preparation. `NULL` plan on failure -> `Option`.
pub fn SPI_prepare(_src: &str, _argtypes: &[Oid]) -> Option<SPIPlanPtr> {
    unimplemented!()
}
pub fn SPI_prepare_cursor(
    _src: &str,
    _argtypes: &[Oid],
    _cursor_options: i32,
) -> Option<SPIPlanPtr> {
    unimplemented!()
}
pub fn SPI_prepare_extended(_src: &str, _options: &SPIPrepareOptions) -> Option<SPIPlanPtr> {
    unimplemented!()
}
pub fn SPI_prepare_params(
    _src: &str,
    _parser_setup: Option<ParserSetupHook>,
    _cursor_options: i32,
) -> Option<SPIPlanPtr> {
    unimplemented!()
}
pub fn SPI_keepplan(_plan: SPIPlanPtr) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_saveplan(_plan: SPIPlanPtr) -> Option<SPIPlanPtr> {
    unimplemented!()
}
pub fn SPI_freeplan(_plan: SPIPlanPtr) -> Result<SpiOk, SpiError> {
    unimplemented!()
}

// InvalidOid sentinel -> Option.
pub fn SPI_getargtypeid(_plan: SPIPlanPtr, _arg_index: i32) -> Option<Oid> {
    unimplemented!()
}
pub fn SPI_getargcount(_plan: SPIPlanPtr) -> i32 {
    unimplemented!()
}
pub fn SPI_is_cursor_plan(_plan: SPIPlanPtr) -> bool {
    unimplemented!()
}
pub fn SPI_plan_is_valid(_plan: SPIPlanPtr) -> bool {
    unimplemented!()
}
pub fn SPI_result_code_string(_code: i32) -> &'static str {
    unimplemented!()
}

pub fn SPI_plan_get_plan_sources(_plan: SPIPlanPtr) -> Vec<Node> {
    unimplemented!()
}
pub fn SPI_plan_get_cached_plan(_plan: SPIPlanPtr) -> Option<Box<CachedPlan>> {
    unimplemented!()
}

pub fn SPI_copytuple(_tuple: HeapTuple) -> Option<HeapTuple> {
    unimplemented!()
}
// C returns HeapTupleHeader (= HeapTupleHeaderData *); NULL -> None. TODO(ptr)
pub fn SPI_returntuple(_tuple: HeapTuple, _tupdesc: TupleDesc) -> Option<*mut HeapTupleHeaderData> {
    unimplemented!()
}
pub fn SPI_modifytuple(
    _rel: Relation,
    _tuple: HeapTuple,
    _attnum: &[i32],
    _values: &[Datum],
    _nulls: Option<&str>,
) -> Option<HeapTuple> {
    unimplemented!()
}
// SPI_ERROR_NOATTRIBUTE sentinel -> Option.
pub fn SPI_fnumber(_tupdesc: TupleDesc, _fname: &str) -> Option<i32> {
    unimplemented!()
}
pub fn SPI_fname(_tupdesc: TupleDesc, _fnumber: i32) -> Option<String> {
    unimplemented!()
}
pub fn SPI_getvalue(_tuple: HeapTuple, _tupdesc: TupleDesc, _fnumber: i32) -> Option<String> {
    unimplemented!()
}
// isnull out-param folds into Option<Datum>.
pub fn SPI_getbinval(_tuple: HeapTuple, _tupdesc: TupleDesc, _fnumber: i32) -> Option<Datum> {
    unimplemented!()
}
pub fn SPI_gettype(_tupdesc: TupleDesc, _fnumber: i32) -> Option<String> {
    unimplemented!()
}
pub fn SPI_gettypeid(_tupdesc: TupleDesc, _fnumber: i32) -> Option<Oid> {
    unimplemented!()
}
pub fn SPI_getrelname(_rel: Relation) -> Option<String> {
    unimplemented!()
}
pub fn SPI_getnspname(_rel: Relation) -> Option<String> {
    unimplemented!()
}

// Memory helpers -> std allocation in Phase 2; kept as raw for now. TODO(ptr)
pub fn SPI_palloc(_size: Size) -> *mut u8 {
    unimplemented!()
}
pub fn SPI_repalloc(_pointer: *mut u8, _size: Size) -> *mut u8 {
    unimplemented!()
}
pub fn SPI_pfree(_pointer: *mut u8) {
    unimplemented!()
}
pub fn SPI_datumTransfer(_value: Datum, _typ_by_val: bool, _typ_len: i32) -> Datum {
    unimplemented!()
}
pub fn SPI_freetuple(_tuple: HeapTuple) {
    unimplemented!()
}
pub fn SPI_freetuptable(_tuptable: Option<Box<SPITupleTable>>) {
    unimplemented!()
}

// NULL portal on failure -> Option.
pub fn SPI_cursor_open(
    _name: Option<&str>,
    _plan: SPIPlanPtr,
    _values: &[Datum],
    _nulls: Option<&str>,
    _read_only: bool,
) -> Option<Portal> {
    unimplemented!()
}
pub fn SPI_cursor_open_with_args(
    _name: &str,
    _src: &str,
    _argtypes: &[Oid],
    _values: &[Datum],
    _nulls: Option<&str>,
    _read_only: bool,
    _cursor_options: i32,
) -> Option<Portal> {
    unimplemented!()
}
pub fn SPI_cursor_open_with_paramlist(
    _name: Option<&str>,
    _plan: SPIPlanPtr,
    _params: ParamListInfo,
    _read_only: bool,
) -> Option<Portal> {
    unimplemented!()
}
pub fn SPI_cursor_parse_open(
    _name: &str,
    _src: &str,
    _options: &SPIParseOpenOptions,
) -> Option<Portal> {
    unimplemented!()
}
pub fn SPI_cursor_find(_name: &str) -> Option<Portal> {
    unimplemented!()
}
pub fn SPI_cursor_fetch(_portal: Portal, _forward: bool, _count: i64) {
    unimplemented!()
}
pub fn SPI_cursor_move(_portal: Portal, _forward: bool, _count: i64) {
    unimplemented!()
}
pub fn SPI_scroll_cursor_fetch(_portal: Portal, _direction: FetchDirection, _count: i64) {
    unimplemented!()
}
pub fn SPI_scroll_cursor_move(_portal: Portal, _direction: FetchDirection, _count: i64) {
    unimplemented!()
}
pub fn SPI_cursor_close(_portal: Portal) {
    unimplemented!()
}

pub fn SPI_register_relation(_enr: EphemeralNamedRelation) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_unregister_relation(_name: &str) -> Result<SpiOk, SpiError> {
    unimplemented!()
}
pub fn SPI_register_trigger_data(_tdata: &mut TriggerData) -> Result<SpiOk, SpiError> {
    unimplemented!()
}

pub fn SPI_start_transaction() {
    unimplemented!()
}
pub fn SPI_commit() {
    unimplemented!()
}
pub fn SPI_commit_and_chain() {
    unimplemented!()
}
pub fn SPI_rollback() {
    unimplemented!()
}
pub fn SPI_rollback_and_chain() {
    unimplemented!()
}

pub fn AtEOXact_SPI(_is_commit: bool) {
    unimplemented!()
}
pub fn AtEOSubXact_SPI(_is_commit: bool, _my_subid: SubTransactionId) {
    unimplemented!()
}
pub fn SPI_inside_nonatomic_context() -> bool {
    unimplemented!()
}

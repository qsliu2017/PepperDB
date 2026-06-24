//! Translated from PostgreSQL src/include/storage/aio.h
//!
//! Main AIO interface. In-memory API (shmem-backed in PG). Public types from
//! `crate::storage::aio_types` are reused. Function signatures kept synchronous
//! (async coloring is a later pass). Routine struct `PgAioHandleCallbacks` ->
//! a trait per routine-struct.md.

use crate::storage::aio_types::{
    PgAioHandle, PgAioResult, PgAioReturn, PgAioTargetData, PgAioWaitRef, PGAIO_RESULT_ID_BITS,
};
use crate::storage::procnumber::ProcNumber;

/// `IoMethod` - enum for the io_method GUC. Sequential ordinal -> enum.
/// io_uring is Linux-only; modeled unconditionally for the skeleton.
#[repr(i32)]
pub enum IoMethod {
    Sync = 0,
    Worker,
    IoUring,
}

/// Default io_method.
pub const DEFAULT_IO_METHOD: IoMethod = IoMethod::Worker;

/// `PgAioHandleFlags` - flags settable with pgaio_io_set_flag(). Independent
/// single bits -> bitflags (the C enum is OR-able). Note the C bit positions:
/// SYNCHRONOUS=1<<0, REFERENCES_LOCAL=1<<1, BUFFERED=1<<2.
use bitflags::bitflags;
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PgAioHandleFlags: u32 {
        /// Hint that IO will be executed synchronously.
        const SYNCHRONOUS = 1 << 0;
        /// IO references backend-local memory (required for correctness).
        const REFERENCES_LOCAL = 1 << 1;
        /// IO is using buffered IO (heuristic hint).
        const BUFFERED = 1 << 2;
    }
}

/// `PgAioOp` - supported IO operations. Sequential ordinal -> enum (zero is
/// INVALID to catch zeroed memory).
#[repr(i32)]
pub enum PgAioOp {
    Invalid = 0,
    Readv,
    Writev,
}

/// `PGAIO_OP_COUNT`.
pub const PGAIO_OP_COUNT: usize = 3;

/// `PgAioTargetID` - what IO is being performed on. Sequential ordinal -> enum.
#[repr(i32)]
pub enum PgAioTargetID {
    Invalid = 0,
    Smgr,
}

/// `PGAIO_TID_COUNT`.
pub const PGAIO_TID_COUNT: usize = 2;

/// Per-op data for a read (`PgAioOpData.read`).
pub struct PgAioOpRead {
    pub fd: i32,
    pub iov_length: u16,
    pub offset: u64,
}

/// Per-op data for a write (`PgAioOpData.write`).
pub struct PgAioOpWrite {
    pub fd: i32,
    pub iov_length: u16,
    pub offset: u64,
}

/// `PgAioOpData` - C union of read/write; modeled as a tagged enum (in-memory).
pub enum PgAioOpData {
    Read(PgAioOpRead),
    Write(PgAioOpWrite),
}

/// `PgAioHandleCallbackID` - IDs for registerable callbacks. Sequential
/// ordinal -> enum.
#[repr(i32)]
pub enum PgAioHandleCallbackID {
    Invalid = 0,
    MdReadv,
    SharedBufferReadv,
    LocalBufferReadv,
}

/// `PGAIO_HCB_MAX`.
pub const PGAIO_HCB_MAX: i32 = PgAioHandleCallbackID::LocalBufferReadv as i32;

const _: () = assert!(PGAIO_HCB_MAX < (1 << PGAIO_RESULT_ID_BITS));

/// `PgAioHandleMaxCallbacks` - max callbacks per IO handle.
pub const PGAIO_HANDLE_MAX_CALLBACKS: usize = 4;

/// `PgAioTargetInfo` - callbacks describing the object IO runs on. Routine
/// struct -> trait. `name` is data (an associated const).
pub trait PgAioTargetInfo {
    /// Name of the target (log messages / views).
    const NAME: &'static str;
    /// Reopen the IO's file descriptor (for worker-process execution).
    fn reopen(ioh: &mut PgAioHandle);
    /// Describe the IO target identity (log messages / views).
    fn describe_identity(sd: &PgAioTargetData) -> String;
}

/// `PgAioHandleCallbacks` - the IO completion callback vtable. Routine struct ->
/// trait per routine-struct.md; all three callbacks are required. `cb_flags` is
/// a raw u8 (caller-defined bitfield). The C `void *arg` model is not present
/// here; report takes an elevel.
pub trait PgAioHandleCallbacks {
    /// Prepare resources affected by the IO for execution.
    fn stage(ioh: &mut PgAioHandle, cb_flags: u8);
    /// Update shared-memory resources to reflect completion (in completing
    /// backend). Returns a possibly-updated result.
    fn complete_shared(
        ioh: &mut PgAioHandle,
        prior_result: PgAioResult,
        cb_flags: u8,
    ) -> PgAioResult;
    /// Like `complete_shared`, but called in the issuing backend.
    fn complete_local(
        ioh: &mut PgAioHandle,
        prior_result: PgAioResult,
        cb_flags: u8,
    ) -> PgAioResult;
    /// Report the result of an IO operation (e.g. raise an error).
    fn report(result: PgAioResult, target_data: &PgAioTargetData, elevel: i32);
}

// --- IO Handles (aio.c) ---

/// Acquire an IO handle (waits for one if none free).
pub fn pgaio_io_acquire(_ret: &mut PgAioReturn) -> &'static mut PgAioHandle {
    unimplemented!()
}

/// Acquire an IO handle without blocking; None if none available.
pub fn pgaio_io_acquire_nb(_ret: &mut PgAioReturn) -> Option<&'static mut PgAioHandle> {
    unimplemented!()
}

pub fn pgaio_io_release(_ioh: &mut PgAioHandle) {
    unimplemented!()
}

/// Release an IO handle owned by a resource owner.
pub fn pgaio_io_release_resowner(_on_error: bool) {
    unimplemented!()
}

pub fn pgaio_io_set_flag(_ioh: &mut PgAioHandle, _flag: PgAioHandleFlags) {
    unimplemented!()
}

pub fn pgaio_io_get_id(_ioh: &mut PgAioHandle) -> i32 {
    unimplemented!()
}

pub fn pgaio_io_get_owner(_ioh: &mut PgAioHandle) -> ProcNumber {
    unimplemented!()
}

pub fn pgaio_io_get_wref(_ioh: &mut PgAioHandle, _iow: &mut PgAioWaitRef) {
    unimplemented!()
}

// --- aio_io.c ---

/// Get the iovec for an IO; returns the vector length.
pub fn pgaio_io_get_iovec(_ioh: &mut PgAioHandle) -> &mut [()] {
    unimplemented!() // TODO(ffi): struct iovec
}

pub fn pgaio_io_get_op(_ioh: &mut PgAioHandle) -> PgAioOp {
    unimplemented!()
}

pub fn pgaio_io_get_op_data(_ioh: &mut PgAioHandle) -> &mut PgAioOpData {
    unimplemented!()
}

pub fn pgaio_io_start_readv(_ioh: &mut PgAioHandle, _fd: i32, _iovcnt: i32, _offset: u64) {
    unimplemented!()
}

pub fn pgaio_io_start_writev(_ioh: &mut PgAioHandle, _fd: i32, _iovcnt: i32, _offset: u64) {
    unimplemented!()
}

// --- aio_target.c ---

pub fn pgaio_io_set_target(_ioh: &mut PgAioHandle, _targetid: PgAioTargetID) {
    unimplemented!()
}

pub fn pgaio_io_has_target(_ioh: &mut PgAioHandle) -> bool {
    unimplemented!()
}

pub fn pgaio_io_get_target_data(_ioh: &mut PgAioHandle) -> &mut PgAioTargetData {
    unimplemented!()
}

pub fn pgaio_io_get_target_description(_ioh: &mut PgAioHandle) -> String {
    unimplemented!()
}

// --- aio_callback.c ---

pub fn pgaio_io_register_callbacks(
    _ioh: &mut PgAioHandle,
    _cb_id: PgAioHandleCallbackID,
    _cb_data: u8,
) {
    unimplemented!()
}

pub fn pgaio_io_set_handle_data_64(_ioh: &mut PgAioHandle, _data: &[u64]) {
    unimplemented!()
}

pub fn pgaio_io_set_handle_data_32(_ioh: &mut PgAioHandle, _data: &[u32]) {
    unimplemented!()
}

/// Get the handle's attached 64-bit data array.
pub fn pgaio_io_get_handle_data(_ioh: &mut PgAioHandle) -> &[u64] {
    unimplemented!()
}

// --- IO Wait References ---

pub fn pgaio_wref_clear(_iow: &mut PgAioWaitRef) {
    unimplemented!()
}

pub fn pgaio_wref_valid(_iow: &mut PgAioWaitRef) -> bool {
    unimplemented!()
}

pub fn pgaio_wref_get_id(_iow: &mut PgAioWaitRef) -> i32 {
    unimplemented!()
}

pub fn pgaio_wref_wait(_iow: &mut PgAioWaitRef) {
    unimplemented!()
}

pub fn pgaio_wref_check_done(_iow: &mut PgAioWaitRef) -> bool {
    unimplemented!()
}

// --- IO Result ---

pub fn pgaio_result_report(_result: PgAioResult, _target_data: &PgAioTargetData, _elevel: i32) {
    unimplemented!()
}

// --- Actions on multiple IOs ---

pub fn pgaio_enter_batchmode() {
    unimplemented!()
}

pub fn pgaio_exit_batchmode() {
    unimplemented!()
}

pub fn pgaio_submit_staged() {
    unimplemented!()
}

pub fn pgaio_have_staged() -> bool {
    unimplemented!()
}

// --- Other ---

pub fn pgaio_closing_fd(_fd: i32) {
    unimplemented!()
}

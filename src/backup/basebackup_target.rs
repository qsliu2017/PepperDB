//! Translated from PostgreSQL src/include/backup/basebackup_target.h
//!
//! Extensibility framework for adding base backup targets. The two registration
//! callbacks are an open (extension-provided) set, so per routine-struct.md they
//! stay `fn` pointers rather than a closed enum/trait.

use crate::backup::basebackup_sink::Bbsink;

/// `check_detail` accepts a target name and target detail; it either raises an
/// error or returns the opaque data needed to create a bbsink for that target.
/// The detail is `None` when TARGET_DETAIL was not specified.
pub type CheckDetailFn = fn(name: &str, detail: Option<&str>) -> *mut ();

/// `get_sink` creates the bbsink. The first argument is the successor sink (the
/// created sink should forward to it); the second is whatever `check_detail`
/// returned.
pub type GetSinkFn = fn(next: &mut Bbsink, detail_data: *mut ()) -> Box<Bbsink>;

/// Opaque handle returned by `BaseBackupGetTargetHandle`, later passed to
/// `BaseBackupGetSink`. Internals live in basebackup_target.c.
pub struct BaseBackupTargetHandle;

/// Extensions call this to register a new backup target.
pub fn BaseBackupAddTarget(_name: &str, _check_detail: CheckDetailFn, _get_sink: GetSinkFn) {
    unimplemented!()
}

/// Returns the handle for `target`/`target_detail`, or raises an error if the
/// target is unrecognized or its check_detail hook rejects the detail.
pub fn BaseBackupGetTargetHandle(
    _target: &str,
    _target_detail: Option<&str>,
) -> Box<BaseBackupTargetHandle> {
    unimplemented!()
}

/// Constructs a bbsink implementing the desired target, forwarding to `next_sink`.
pub fn BaseBackupGetSink(
    _handle: &BaseBackupTargetHandle,
    _next_sink: &mut Bbsink,
) -> Box<Bbsink> {
    unimplemented!()
}

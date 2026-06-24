//! Translated from PostgreSQL src/include/archive/archive_module.h
//
// Archive-module routine struct. Per routine-struct.md (appendix B) this is a
// hook/extension method table whose entry point is an fn-pointer fallback, not
// a trait object: the module-init returns a struct of fn pointers
// (ArchiveModuleCallbacks), mirroring the C shape, with optional callbacks
// modeled as `Option<fn ...>` (runtime NULL-checked; ArchiveFileCB is the only
// required one). The C `ArchiveModuleState.private_data` void* is preserved as
// an opaque per-module state handle threaded through each callback.

// GUC: value of the archive_library GUC.
pub static mut X_LOG_ARCHIVE_LIBRARY: Option<String> = None;

// Support for messages reported from archive module callbacks.
pub static mut ARCH_MODULE_CHECK_ERRDETAIL_STRING: Option<String> = None;

/// C: `ArchiveModuleState` -- per-module state passed to each callback. The
/// `private_data` void* becomes an opaque boxed state handle owned by the core.
pub struct ArchiveModuleState {
    pub private_data: Option<Box<dyn core::any::Any>>,
}

/// ArchiveStartupCB (optional).
pub type ArchiveStartupCb = fn(state: &mut ArchiveModuleState);
/// ArchiveCheckConfiguredCB (optional). Reports whether archiving is configured.
pub type ArchiveCheckConfiguredCb = fn(state: &mut ArchiveModuleState) -> bool;
/// ArchiveFileCB (required): archive `file` found at `path`; returns success.
pub type ArchiveFileCb = fn(state: &mut ArchiveModuleState, file: &str, path: &str) -> bool;
/// ArchiveShutdownCB (optional).
pub type ArchiveShutdownCb = fn(state: &mut ArchiveModuleState);

/// C: `ArchiveModuleCallbacks` -- the module's callback table. Optional
/// callbacks are `None`-checked at call sites; `archive_file` is required.
pub struct ArchiveModuleCallbacks {
    pub startup_cb: Option<ArchiveStartupCb>,
    pub check_configured_cb: Option<ArchiveCheckConfiguredCb>,
    pub archive_file_cb: ArchiveFileCb,
    pub shutdown_cb: Option<ArchiveShutdownCb>,
}

/// C: `const ArchiveModuleCallbacks *_PG_archive_module_init(void)` -- the
/// shared-library entry point that returns the module's callback table. This is
/// the open-extension fn-pointer hook.
pub fn pg_archive_module_init() -> ArchiveModuleCallbacks {
    unimplemented!()
}

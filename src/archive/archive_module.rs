//! archive/archive_module.h - Exports for archive modules.

use std::ffi::{c_char, c_void};

// The value of the archive_library GUC.
// C: extern PGDLLIMPORT char *XLogArchiveLibrary;
#[allow(improper_ctypes)]
extern "C" {
    pub static mut XLogArchiveLibrary: *mut c_char;

    /// Support for messages reported from archive module callbacks.
    /// C: extern PGDLLIMPORT char *arch_module_check_errdetail_string;
    pub static mut arch_module_check_errdetail_string: *mut c_char;
}

#[repr(C)]
pub struct ArchiveModuleState {
    /// Private data pointer for use by an archive module. This can be used to
    /// store state for the module that will be passed to each of its callbacks.
    pub private_data: *mut c_void,
}

/*
 * Archive module callbacks
 *
 * These callback functions should be defined by archive libraries and returned
 * via _PG_archive_module_init(). ArchiveFileCB is the only required callback.
 */
pub type ArchiveStartupCB = Option<unsafe extern "C" fn(state: *mut ArchiveModuleState)>;
pub type ArchiveCheckConfiguredCB =
    Option<unsafe extern "C" fn(state: *mut ArchiveModuleState) -> bool>;
pub type ArchiveFileCB = Option<
    unsafe extern "C" fn(
        state: *mut ArchiveModuleState,
        file: *const c_char,
        path: *const c_char,
    ) -> bool,
>;
pub type ArchiveShutdownCB = Option<unsafe extern "C" fn(state: *mut ArchiveModuleState)>;

#[repr(C)]
pub struct ArchiveModuleCallbacks {
    pub startup_cb: ArchiveStartupCB,
    pub check_configured_cb: ArchiveCheckConfiguredCB,
    pub archive_file_cb: ArchiveFileCB,
    pub shutdown_cb: ArchiveShutdownCB,
}

/// Type of the shared library symbol _PG_archive_module_init that is looked
/// up when loading an archive library.
pub type ArchiveModuleInit =
    Option<unsafe extern "C" fn() -> *const ArchiveModuleCallbacks>;

/// C: extern PGDLLEXPORT const ArchiveModuleCallbacks *_PG_archive_module_init(void);
pub unsafe fn _PG_archive_module_init() -> *const ArchiveModuleCallbacks {
    unimplemented!()
}

/*
 * C function-like macro `arch_module_check_errdetail`:
 *
 *   #define arch_module_check_errdetail \
 *       pre_format_elog_string(errno, TEXTDOMAIN), \
 *       arch_module_check_errdetail_string = format_elog_string
 *
 * This is a comma-expression macro intended to be used inline as a statement;
 * it depends on the elog machinery (pre_format_elog_string / format_elog_string /
 * TEXTDOMAIN / errno) which is not yet ported. It cannot be faithfully expressed
 * as a Rust fn (it has no return value usage and relies on textual substitution
 * at the call site), so it is documented here as a TODO until elog is available.
 *
 * TODO: implement once utils/elog (pre_format_elog_string, format_elog_string)
 * and TEXTDOMAIN are ported.
 */

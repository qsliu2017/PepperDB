//! archive/shell_archive.c - default WAL archiving via the archive_command shell GUC.

use crate::prelude::*;

use crate::archive::archive_module::{ArchiveModuleCallbacks, ArchiveModuleState};
use crate::common::percentrepl::replace_percent_placeholders;
use crate::common::wait_error::wait_result_is_any_signal;
use crate::port::path::make_native_path;
use crate::port::win32_port::{WEXITSTATUS, WIFEXITED, WIFSIGNALED, WTERMSIG};
use crate::utils::mmgr::mcxt::{pfree, pstrdup};

// ----- Locally stubbed (not yet ported) dependencies -----

// The value of the archive_command GUC.  C: extern char XLogArchiveCommand[];
// Not yet ported; modeled as an empty NUL-terminated buffer.
// TODO: replace with the real XLogArchiveCommand GUC storage once ported.
const XLOG_ARCHIVE_COMMAND_LEN: usize = 1024;
static mut XLogArchiveCommand: [c_char; XLOG_ARCHIVE_COMMAND_LEN] = [0; XLOG_ARCHIVE_COMMAND_LEN];

// WAIT_EVENT_ARCHIVE_COMMAND wait-event id (pgstat). Not yet ported.
// TODO: import from the real pgstat wait-event enum once ported.
const WAIT_EVENT_ARCHIVE_COMMAND: u32 = 0;

// C: void pgstat_report_wait_start(uint32 wait_event_info)
// TODO: import from pgstat once ported.
unsafe fn pgstat_report_wait_start(_wait_event_info: u32) {
    unimplemented!()
}

// C: void pgstat_report_wait_end(void)
// TODO: import from pgstat once ported.
unsafe fn pgstat_report_wait_end() {
    unimplemented!()
}

// C: char *pg_strsignal(int signum)
// TODO: import from common/wait_error once exported.
unsafe fn pg_strsignal(_signum: c_int) -> *const c_char {
    unimplemented!()
}

// C: int system(const char *command)
extern "C" {
    fn system(command: *const c_char) -> c_int;
    fn fflush(stream: *mut c_void) -> c_int;
}

// `arch_module_check_errdetail` is a comma-expression macro that is not yet
// portable (see archive_module.rs). Modeled here as a no-op accepting a
// formatted message, to preserve the call site.
// TODO: implement once the elog string-formatting machinery is ported.
unsafe fn arch_module_check_errdetail(_msg: &str) {}

static shell_archive_callbacks: ArchiveModuleCallbacks = ArchiveModuleCallbacks {
    startup_cb: None,
    check_configured_cb: Some(shell_archive_configured),
    archive_file_cb: Some(shell_archive_file),
    shutdown_cb: Some(shell_archive_shutdown),
};

pub unsafe fn shell_archive_init() -> *const ArchiveModuleCallbacks {
    &shell_archive_callbacks
}

unsafe extern "C" fn shell_archive_configured(_state: *mut ArchiveModuleState) -> bool {
    if XLogArchiveCommand[0] != 0 {
        return true;
    }

    arch_module_check_errdetail(&format!("\"{}\" is not set.", "archive_command"));
    false
}

unsafe extern "C" fn shell_archive_file(
    _state: *mut ArchiveModuleState,
    file: *const c_char,
    path: *const c_char,
) -> bool {
    let xlogarchcmd: *mut c_char;
    let mut nativePath: *mut c_char = core::ptr::null_mut();
    let rc: c_int;

    if !path.is_null() {
        nativePath = pstrdup(path);
        make_native_path(nativePath);
    }

    let values: [*const c_char; 2] = [file, nativePath as *const c_char];
    xlogarchcmd = replace_percent_placeholders(
        XLogArchiveCommand.as_ptr(),
        c"archive_command".as_ptr(),
        c"fp".as_ptr(),
        &values,
    );

    ereport!(
        DEBUG3,
        format!(
            "executing archive command \"{}\"",
            cstr_to_string(xlogarchcmd)
        )
    );

    fflush(core::ptr::null_mut());
    pgstat_report_wait_start(WAIT_EVENT_ARCHIVE_COMMAND);
    rc = system(xlogarchcmd);
    pgstat_report_wait_end();

    if rc != 0 {
        /*
         * If either the shell itself, or a called command, died on a signal,
         * abort the archiver.  We do this because system() ignores SIGINT and
         * SIGQUIT while waiting; so a signal is very likely something that
         * should have interrupted us too.  Also die if the shell got a hard
         * "command not found" type of error.  If we overreact it's no big
         * deal, the postmaster will just start the archiver again.
         */
        let lev = if wait_result_is_any_signal(rc, true) {
            FATAL
        } else {
            LOG
        };

        if WIFEXITED(rc) {
            ereport!(
                lev,
                format!(
                    "archive command failed with exit code {}; The failed archive command was: {}",
                    WEXITSTATUS(rc),
                    cstr_to_string(xlogarchcmd)
                )
            );
        } else if WIFSIGNALED(rc) {
            ereport!(
                lev,
                format!(
                    "archive command was terminated by signal {}: {}; The failed archive command was: {}",
                    WTERMSIG(rc),
                    cstr_to_string(pg_strsignal(WTERMSIG(rc))),
                    cstr_to_string(xlogarchcmd)
                )
            );
        } else {
            ereport!(
                lev,
                format!(
                    "archive command exited with unrecognized status {}; The failed archive command was: {}",
                    rc,
                    cstr_to_string(xlogarchcmd)
                )
            );
        }
        pfree(xlogarchcmd as *mut c_void);

        return false;
    }
    pfree(xlogarchcmd as *mut c_void);

    elog!(DEBUG1, "archived write-ahead log file \"{}\"", cstr_to_string(file));
    true
}

unsafe extern "C" fn shell_archive_shutdown(_state: *mut ArchiveModuleState) {
    elog!(DEBUG1, "archiver process shutting down");
}

/// Helper: render a (possibly NULL) NUL-terminated C string for diagnostics.
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::new();
    }
    std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

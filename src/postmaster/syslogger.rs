//! src/backend/postmaster/syslogger.c
//!
//! The system logger (syslogger) appeared in Postgres 8.0. It catches all
//! stderr output from the postmaster, backends, and other subprocesses
//! by redirecting to a pipe, and writes it to a set of logfiles.
//! It's possible to have size and age limits for the logfile configured
//! in postgresql.conf. If these limits are reached or passed, the
//! current logfile is closed and a new one is created (rotated).
//! The logfiles are stored in a subdirectory (configurable in
//! postgresql.conf), using a user-selectable naming scheme.
//!
//! Author: Andreas Pflug <pgadmin@pse-consulting.de>
//!
//! Copyright (c) 2004-2025, PostgreSQL Global Development Group

use crate::prelude::*;

use crate::miscadmin::{B_LOGGER, MyBackendType, MyLatch, MyStartTime};
use crate::nodes::pg_list::{lappend, lfirst, List, ListCell};
use crate::pg_config_manual::MAXPGPATH;
use crate::pgtime::{log_timezone, pg_localtime, pg_strftime, pg_time_t, pg_tm, pg_tz};
use crate::port::pg_bitutils::pg_number_of_ones;
use crate::lib::stringinfo::{
    appendBinaryStringInfo, initStringInfo, StringInfo, StringInfoData,
};
use crate::utils::error::elog_impl::{
    Log_destination, LOG_DESTINATION_CSVLOG, LOG_DESTINATION_JSONLOG, LOG_DESTINATION_STDERR,
};
use core::ffi::CStr;

extern "C" {
    fn close(fd: c_int) -> c_int;
    fn open(path: *const c_char, oflag: c_int, ...) -> c_int;
    fn dup2(oldfd: c_int, newfd: c_int) -> c_int;
    fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize;
    fn pipe(fds: *mut c_int) -> c_int;
    fn umask(mask: mode_t) -> mode_t;
    fn unlink(path: *const c_char) -> c_int;
    fn rename(old: *const c_char, new: *const c_char) -> c_int;
    fn stat(path: *const c_char, buf: *mut libc_stat) -> c_int;
    fn time(tloc: *mut pg_time_t) -> pg_time_t;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memmove(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlcpy(dst: *mut c_char, src: *const c_char, siz: usize) -> usize;
    // stdio
    fn fopen(path: *const c_char, mode: *const c_char) -> *mut FILE;
    fn fclose(stream: *mut FILE) -> c_int;
    fn fwrite(ptr: *const c_void, size: usize, nmemb: usize, stream: *mut FILE) -> usize;
    fn fflush(stream: *mut FILE) -> c_int;
    fn fprintf(stream: *mut FILE, fmt: *const c_char, ...) -> c_int;
    fn fileno(stream: *mut FILE) -> c_int;
    fn fdopen(fd: c_int, mode: *const c_char) -> *mut FILE;
    fn setvbuf(stream: *mut FILE, buf: *mut c_char, mode: c_int, size: usize) -> c_int;
    fn ftello(stream: *mut FILE) -> pgoff_t;
    // Darwin errno
    fn __error() -> *mut c_int;
    // Darwin stdio globals (stdout/stderr)
    static mut __stdoutp: *mut FILE;
    static mut __stderrp: *mut FILE;
}

unsafe fn stdout_file() -> *mut FILE {
    __stdoutp
}
unsafe fn stderr_file() -> *mut FILE {
    __stderrp
}

#[repr(C)]
struct FILE {
    _opaque: [u8; 0],
}

#[repr(C)]
struct libc_stat {
    _opaque: [u8; 256],
}

type pgoff_t = i64;
type sig_atomic_t = c_int;
type mode_t = c_uint;

unsafe fn errno() -> c_int {
    *__error()
}
unsafe fn set_errno(e: c_int) {
    *__error() = e;
}

const STDOUT_FILENO: c_int = 1;
const STDERR_FILENO: c_int = 2;
const O_WRONLY: c_int = 0x0001;
const ENOENT: c_int = 2;
const EINTR: c_int = 4;
const ENFILE: c_int = 23;
const EMFILE: c_int = 24;
const PG_IOLBF: c_int = 1; // _IOLBF on most platforms
const SIG_IGN: usize = 1;
const SIG_DFL: usize = 0;
const SIG_SETMASK: c_int = 3; // Darwin SIG_SETMASK
const SIGHUP: c_int = 1;
const SIGINT: c_int = 2;
const SIGQUIT: c_int = 3;
const SIGPIPE: c_int = 13;
const SIGALRM: c_int = 14;
const SIGTERM: c_int = 15;
const SIGUSR1: c_int = 30;
const SIGUSR2: c_int = 31;
const SIGCHLD: c_int = 20;
const INT_MAX: c_int = 2147483647;
const S_IRUSR: c_int = 0o400;
const S_IWUSR: c_int = 0o200;
const S_IRWXU: c_int = 0o700;
const S_IRWXG: c_int = 0o070;
const S_IRWXO: c_int = 0o007;

// Time-unit constants (from datetime.h / timestamp.h).
const HOURS_PER_DAY: c_int = 24;
const MINS_PER_HOUR: c_int = 60;
const SECS_PER_MINUTE: c_int = 60;

// from libpq/pqsignal.h, declared SIGNAL_ARGS handlers
const DestNone: c_int = 0;

// from postmaster/postmaster.h
const PGINVALID_SOCKET: c_int = -1;

// from src/include/postmaster/syslogger.h ----------------------------------
//
// Primitive protocol structure for writing to syslogger pipe(s).
pub const PIPE_CHUNK_SIZE: c_int = 512;

#[repr(C)]
pub struct PipeProtoHeader {
    pub nuls: [c_char; 2],    // always \0\0
    pub len: uint16,          // size of this chunk (counts data only)
    pub pid: int32,           // writer's pid
    pub flags: bits8,         // bitmask of PIPE_PROTO_*
    pub data: [c_char; 0],    // data payload starts here (FLEXIBLE_ARRAY_MEMBER)
}

#[repr(C)]
pub union PipeProtoChunk {
    pub proto: core::mem::ManuallyDrop<PipeProtoHeader>,
    pub filler: [c_char; PIPE_CHUNK_SIZE as usize],
}

// PIPE_HEADER_SIZE  offsetof(PipeProtoHeader, data)
pub const PIPE_HEADER_SIZE: usize = core::mem::offset_of!(PipeProtoHeader, data);
// PIPE_MAX_PAYLOAD  ((int) (PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE))
pub const PIPE_MAX_PAYLOAD: c_int = PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE as c_int;

// flag bits for PipeProtoHeader->flags
pub const PIPE_PROTO_IS_LAST: bits8 = 0x01; // last chunk of message?
// log destinations
pub const PIPE_PROTO_DEST_STDERR: bits8 = 0x10;
pub const PIPE_PROTO_DEST_CSVLOG: bits8 = 0x20;
pub const PIPE_PROTO_DEST_JSONLOG: bits8 = 0x40;

// Name of files saving meta-data information about the log
// files currently in use by the syslogger
pub const LOG_METAINFO_DATAFILE: &CStr = c"current_logfiles";
pub const LOG_METAINFO_DATAFILE_TMP: &CStr = c"current_logfiles.tmp";
// --------------------------------------------------------------------------

// We read() into a temp buffer twice as big as a chunk, so that any fragment
// left after processing can be moved down to the front and we'll still have
// room to read a full chunk.
const READ_BUF_SIZE: usize = (2 * PIPE_CHUNK_SIZE) as usize;

// Log rotation signal file path, relative to $PGDATA
const LOGROTATE_SIGNAL_FILE: &CStr = c"logrotate";

const DEVNULL: &CStr = c"/dev/null";

//
// GUC parameters.  Logging_collector cannot be changed after postmaster
// start, but the rest can change at SIGHUP.
//
pub static mut Logging_collector: bool = false;
pub static mut Log_RotationAge: c_int = HOURS_PER_DAY * MINS_PER_HOUR;
pub static mut Log_RotationSize: c_int = 10 * 1024;
pub static mut Log_directory: *mut c_char = null_mut();
pub static mut Log_filename: *mut c_char = null_mut();
pub static mut Log_truncate_on_rotation: bool = false;
pub static mut Log_file_mode: c_int = S_IRUSR | S_IWUSR;

//
// Private state
//
static mut next_rotation_time: pg_time_t = 0;
static mut pipe_eof_seen: bool = false;
static mut rotation_disabled: bool = false;
static mut syslogFile: *mut FILE = null_mut();
static mut csvlogFile: *mut FILE = null_mut();
static mut jsonlogFile: *mut FILE = null_mut();
// NON_EXEC_STATIC
pub static mut first_syslogger_file_time: pg_time_t = 0;
static mut last_sys_file_name: *mut c_char = null_mut();
static mut last_csv_file_name: *mut c_char = null_mut();
static mut last_json_file_name: *mut c_char = null_mut();

//
// Buffers for saving partial messages from different backends.
//
// Keep NBUFFER_LISTS lists of these, with the entry for a given source pid
// being in the list numbered (pid % NBUFFER_LISTS), so as to cut down on
// the number of entries we have to examine for any one incoming message.
// There must never be more than one entry for the same source pid.
//
// An inactive buffer is not removed from its list, just held for re-use.
// An inactive buffer has pid == 0 and undefined contents of data.
//
#[repr(C)]
struct save_buffer {
    pid: int32,             // PID of source process
    data: StringInfoData,   // accumulated data, as a StringInfo
}

const NBUFFER_LISTS: usize = 256;
static mut buffer_lists: [*mut List; NBUFFER_LISTS] = [null_mut(); NBUFFER_LISTS];

// These must be exported for EXEC_BACKEND case ... annoying
pub static mut syslogPipe: [c_int; 2] = [-1, -1];

//
// Flags set by interrupt handlers for later service in the main loop.
//
static mut rotation_requested: sig_atomic_t = false as sig_atomic_t;

#[repr(C)]
struct SysloggerStartupData {
    syslogFile: c_int,
    csvlogFile: c_int,
    jsonlogFile: c_int,
}

// ---------------------------------------------------------------------------
// Stubs for unported dependencies (functions/globals living in OTHER .c files).
// Replace each when the owning module is translated.
// ---------------------------------------------------------------------------

// storage/latch.h, storage/waiteventset.h
#[repr(C)]
struct WaitEventSet {
    _opaque: [u8; 0],
}
#[repr(C)]
struct WaitEvent {
    pos: c_int,
    events: u32,
    fd: c_int,
    user_data: *mut c_void,
}
const WL_LATCH_SET: c_int = 1 << 0;
const WL_SOCKET_READABLE: c_int = 1 << 2;
const WAIT_EVENT_SYSLOGGER_MAIN: u32 = 0;

unsafe fn CreateWaitEventSet(_ctx: MemoryContext, _nevents: c_int) -> *mut WaitEventSet {
    todo!("pg-port: storage/waiteventset.c CreateWaitEventSet")
}
unsafe fn AddWaitEventToSet(
    _set: *mut WaitEventSet,
    _events: u32,
    _fd: c_int,
    _latch: *mut Latch,
    _user_data: *mut c_void,
) -> c_int {
    todo!("pg-port: storage/waiteventset.c AddWaitEventToSet")
}
unsafe fn WaitEventSetWait(
    _set: *mut WaitEventSet,
    _timeout: c_long,
    _occurred_events: *mut WaitEvent,
    _nevents: c_int,
    _wait_event_info: u32,
) -> c_int {
    todo!("pg-port: storage/waiteventset.c WaitEventSetWait")
}

use crate::miscadmin::Latch;
unsafe fn SetLatch(_latch: *mut Latch) {
    todo!("pg-port: storage/latch.c SetLatch")
}
unsafe fn ResetLatch(_latch: *mut Latch) {
    todo!("pg-port: storage/latch.c ResetLatch")
}

// storage/ipc.h
unsafe fn proc_exit(_code: c_int) {
    todo!("pg-port: storage/ipc.c proc_exit")
}

// utils/memutils.h
unsafe fn MemoryContextDelete_stub(_ctx: MemoryContext) {
    todo!("pg-port: utils/mmgr/mcxt.c MemoryContextDelete")
}
static mut PostmasterContext: MemoryContext = null_mut();

// postmaster/interrupt.h
static mut ConfigReloadPending: bool = false;
unsafe extern "C" fn SignalHandlerForConfigReload(_sig: c_int) {
    todo!("pg-port: postmaster/interrupt.c SignalHandlerForConfigReload")
}

// utils/guc.h
const PGC_SIGHUP: c_int = 1;
unsafe fn ProcessConfigFile(_context: c_int) {
    todo!("pg-port: utils/misc/guc.c ProcessConfigFile")
}

// libpq/pqsignal.h
unsafe fn pqsignal(_signo: c_int, _func: usize) {
    todo!("pg-port: port/pqsignal.c pqsignal")
}
unsafe fn sigUsr1Handler_ptr() -> usize {
    sigUsr1Handler as usize
}
extern "C" {
    static UnBlockSig: sigset_t;
}
#[repr(C)]
struct sigset_t {
    _opaque: u32,
}
unsafe fn sigprocmask(_how: c_int, _set: *const sigset_t, _oldset: *mut sigset_t) -> c_int {
    todo!("pg-port: signal.h sigprocmask")
}

// utils/ps_status.h
unsafe fn init_ps_display(_fixed_part: *const c_char) {
    todo!("pg-port: utils/misc/ps_status.c init_ps_display")
}

// miscadmin.h
static mut redirection_done: bool = false;
static mut whereToSendOutput: c_int = DestNone;

// storage/fd.h
unsafe fn MakePGDirectory(_directoryName: *const c_char) -> c_int {
    todo!("pg-port: storage/file/fd.c MakePGDirectory")
}

// common/file_perm.h
static mut pg_mode_mask: mode_t = 0o077;

// postmaster/launch_backend.h
unsafe fn postmaster_child_launch(
    _child_type: BackendType,
    _child_slot: c_int,
    _startup_data: *const c_void,
    _startup_data_len: usize,
    _client_sock: *mut c_void,
) -> pid_t {
    todo!("pg-port: postmaster/launch_backend.c postmaster_child_launch")
}

type pid_t = c_int;
type BackendType = crate::miscadmin::BackendType;

// utils/elog.h - write_stderr writes to the postmaster's original stderr,
// never to our input pipe.
unsafe fn write_stderr(fmt: *const c_char) {
    todo!("pg-port: utils/error/elog.c write_stderr")
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

//
// Main entry point for syslogger process
// argc/argv parameters are valid only in EXEC_BACKEND case.
//
pub unsafe fn SysLoggerMain(startup_data: *const c_void, startup_data_len: usize) {
    let mut logbuffer: [c_char; READ_BUF_SIZE] = [0; READ_BUF_SIZE];
    let mut bytes_in_logbuffer: c_int = 0;
    let mut currentLogDir: *mut c_char;
    let mut currentLogFilename: *mut c_char;
    let mut currentLogRotationAge: c_int;
    let mut now: pg_time_t;
    let wes: *mut WaitEventSet;

    //
    // Re-open the error output files that were opened by SysLogger_Start().
    //
    // We expect this will always succeed, which is too optimistic, but if it
    // fails there's not a lot we can do to report the problem anyway.  As
    // coded, we'll just crash on a null pointer dereference after failure...
    //
    // (EXEC_BACKEND only; not built on this platform.)
    Assert!(startup_data_len == 0);
    let _ = startup_data;

    //
    // Now that we're done reading the startup data, release postmaster's
    // working memory context.
    //
    if !PostmasterContext.is_null() {
        MemoryContextDelete_stub(PostmasterContext);
        PostmasterContext = null_mut();
    }

    now = MyStartTime;

    MyBackendType = B_LOGGER;
    init_ps_display(null());

    //
    // If we restarted, our stderr is already redirected into our own input
    // pipe.  This is of course pretty useless, not to mention that it
    // interferes with detecting pipe EOF.  Point stderr to /dev/null. This
    // assumes that all interesting messages generated in the syslogger will
    // come through elog.c and will be sent to write_syslogger_file.
    //
    if redirection_done {
        let fd: c_int = open(DEVNULL.as_ptr(), O_WRONLY, 0);

        //
        // The closes might look redundant, but they are not: we want to be
        // darn sure the pipe gets closed even if the open failed.  We can
        // survive running with stderr pointing nowhere, but we can't afford
        // to have extra pipe input descriptors hanging around.
        //
        // As we're just trying to reset these to go to DEVNULL, there's not
        // much point in checking for failure from the close/dup2 calls here,
        // if they fail then presumably the file descriptors are closed and
        // any writes will go into the bitbucket anyway.
        //
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
        if fd != -1 {
            dup2(fd, STDOUT_FILENO);
            dup2(fd, STDERR_FILENO);
            close(fd);
        }
    }

    //
    // Syslogger's own stderr can't be the syslogPipe, so set it back to text
    // mode if we didn't just close it. (It was set to binary in
    // SubPostmasterMain).  -- WIN32 only.
    //

    //
    // Also close our copy of the write end of the pipe.  This is needed to
    // ensure we can detect pipe EOF correctly.  (But note that in the restart
    // case, the postmaster already did this.)
    //
    if syslogPipe[1] >= 0 {
        close(syslogPipe[1]);
    }
    syslogPipe[1] = -1;

    //
    // Properly accept or ignore signals the postmaster might send us
    //
    // Note: we ignore all termination signals, and instead exit only when all
    // upstream processes are gone, to ensure we don't miss any dying gasps of
    // broken backends...
    //

    pqsignal(SIGHUP, SignalHandlerForConfigReload as usize); // set flag to read config file
    pqsignal(SIGINT, SIG_IGN);
    pqsignal(SIGTERM, SIG_IGN);
    pqsignal(SIGQUIT, SIG_IGN);
    pqsignal(SIGALRM, SIG_IGN);
    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, sigUsr1Handler_ptr()); // request log rotation
    pqsignal(SIGUSR2, SIG_IGN);

    //
    // Reset some signals that are accepted by postmaster but not here
    //
    pqsignal(SIGCHLD, SIG_DFL);

    sigprocmask(SIG_SETMASK, &UnBlockSig, null_mut());

    //
    // Remember active logfiles' name(s).  We recompute 'em from the reference
    // time because passing down just the pg_time_t is a lot cheaper than
    // passing a whole file path in the EXEC_BACKEND case.
    //
    last_sys_file_name = logfile_getname(first_syslogger_file_time, null());
    if !csvlogFile.is_null() {
        last_csv_file_name = logfile_getname(first_syslogger_file_time, c".csv".as_ptr());
    }
    if !jsonlogFile.is_null() {
        last_json_file_name = logfile_getname(first_syslogger_file_time, c".json".as_ptr());
    }

    // remember active logfile parameters
    currentLogDir = pstrdup(Log_directory);
    currentLogFilename = pstrdup(Log_filename);
    currentLogRotationAge = Log_RotationAge;
    // set next planned rotation time
    set_next_rotation_time();
    update_metainfo_datafile();

    //
    // Reset whereToSendOutput, as the postmaster will do (but hasn't yet, at
    // the point where we forked).  This prevents duplicate output of messages
    // from syslogger itself.
    //
    whereToSendOutput = DestNone;

    //
    // Set up a reusable WaitEventSet object we'll use to wait for our latch,
    // and (except on Windows) our socket.
    //
    // Unlike all other postmaster child processes, we'll ignore postmaster
    // death because we want to collect final log output from all backends and
    // then exit last.  We'll do that by running until we see EOF on the
    // syslog pipe, which implies that all other backends have exited
    // (including the postmaster).
    //
    wes = CreateWaitEventSet(null_mut(), 2);
    AddWaitEventToSet(wes, WL_LATCH_SET as u32, PGINVALID_SOCKET, MyLatch, null_mut());
    AddWaitEventToSet(wes, WL_SOCKET_READABLE as u32, syslogPipe[0], null_mut(), null_mut());

    // main worker loop
    loop {
        let mut time_based_rotation: bool = false;
        let mut size_rotation_for: c_int = 0;
        let cur_timeout: c_long;
        let mut event: WaitEvent = WaitEvent {
            pos: 0,
            events: 0,
            fd: 0,
            user_data: null_mut(),
        };

        let rc: c_int;

        // Clear any already-pending wakeups
        ResetLatch(MyLatch);

        //
        // Process any requests or signals received recently.
        //
        if ConfigReloadPending {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);

            //
            // Check if the log directory or filename pattern changed in
            // postgresql.conf. If so, force rotation to make sure we're
            // writing the logfiles in the right place.
            //
            if strcmp(Log_directory, currentLogDir) != 0 {
                pfree(currentLogDir as *mut c_void);
                currentLogDir = pstrdup(Log_directory);
                rotation_requested = true as sig_atomic_t;

                //
                // Also, create new directory if not present; ignore errors
                //
                MakePGDirectory(Log_directory);
            }
            if strcmp(Log_filename, currentLogFilename) != 0 {
                pfree(currentLogFilename as *mut c_void);
                currentLogFilename = pstrdup(Log_filename);
                rotation_requested = true as sig_atomic_t;
            }

            //
            // Force a rotation if CSVLOG output was just turned on or off and
            // we need to open or close csvlogFile accordingly.
            //
            if ((Log_destination & LOG_DESTINATION_CSVLOG) != 0) != (!csvlogFile.is_null()) {
                rotation_requested = true as sig_atomic_t;
            }

            //
            // Force a rotation if JSONLOG output was just turned on or off
            // and we need to open or close jsonlogFile accordingly.
            //
            if ((Log_destination & LOG_DESTINATION_JSONLOG) != 0) != (!jsonlogFile.is_null()) {
                rotation_requested = true as sig_atomic_t;
            }

            //
            // If rotation time parameter changed, reset next rotation time,
            // but don't immediately force a rotation.
            //
            if currentLogRotationAge != Log_RotationAge {
                currentLogRotationAge = Log_RotationAge;
                set_next_rotation_time();
            }

            //
            // If we had a rotation-disabling failure, re-enable rotation
            // attempts after SIGHUP, and force one immediately.
            //
            if rotation_disabled {
                rotation_disabled = false;
                rotation_requested = true as sig_atomic_t;
            }

            //
            // Force rewriting last log filename when reloading configuration.
            // Even if rotation_requested is false, log_destination may have
            // been changed and we don't want to wait the next file rotation.
            //
            update_metainfo_datafile();
        }

        if Log_RotationAge > 0 && !rotation_disabled {
            // Do a logfile rotation if it's time
            now = time(null_mut()) as pg_time_t;
            if now >= next_rotation_time {
                rotation_requested = true as sig_atomic_t;
                time_based_rotation = true;
            }
        }

        if rotation_requested == 0 && Log_RotationSize > 0 && !rotation_disabled {
            // Do a rotation if file is too big
            if ftello(syslogFile) >= Log_RotationSize as pgoff_t * 1024 {
                rotation_requested = true as sig_atomic_t;
                size_rotation_for |= LOG_DESTINATION_STDERR;
            }
            if !csvlogFile.is_null() && ftello(csvlogFile) >= Log_RotationSize as pgoff_t * 1024 {
                rotation_requested = true as sig_atomic_t;
                size_rotation_for |= LOG_DESTINATION_CSVLOG;
            }
            if !jsonlogFile.is_null() && ftello(jsonlogFile) >= Log_RotationSize as pgoff_t * 1024 {
                rotation_requested = true as sig_atomic_t;
                size_rotation_for |= LOG_DESTINATION_JSONLOG;
            }
        }

        if rotation_requested != 0 {
            //
            // Force rotation when both values are zero. It means the request
            // was sent by pg_rotate_logfile() or "pg_ctl logrotate".
            //
            if !time_based_rotation && size_rotation_for == 0 {
                size_rotation_for =
                    LOG_DESTINATION_STDERR | LOG_DESTINATION_CSVLOG | LOG_DESTINATION_JSONLOG;
            }
            logfile_rotate(time_based_rotation, size_rotation_for);
        }

        //
        // Calculate time till next time-based rotation, so that we don't
        // sleep longer than that.  We assume the value of "now" obtained
        // above is still close enough.  Note we can't make this calculation
        // until after calling logfile_rotate(), since it will advance
        // next_rotation_time.
        //
        // Also note that we need to beware of overflow in calculation of the
        // timeout: with large settings of Log_RotationAge, next_rotation_time
        // could be more than INT_MAX msec in the future.  In that case we'll
        // wait no more than INT_MAX msec, and try again.
        //
        if Log_RotationAge > 0 && !rotation_disabled {
            let mut delay: pg_time_t;

            delay = next_rotation_time - now;
            if delay > 0 {
                if delay > (INT_MAX / 1000) as pg_time_t {
                    delay = (INT_MAX / 1000) as pg_time_t;
                }
                cur_timeout = delay as c_long * 1000; // msec
            } else {
                cur_timeout = 0;
            }
        } else {
            cur_timeout = -1;
        }

        //
        // Sleep until there's something to do
        //
        rc = WaitEventSetWait(wes, cur_timeout, &mut event, 1, WAIT_EVENT_SYSLOGGER_MAIN);

        if rc == 1 && event.events == WL_SOCKET_READABLE as u32 {
            let bytesRead: isize;

            bytesRead = read(
                syslogPipe[0],
                logbuffer.as_mut_ptr().add(bytes_in_logbuffer as usize) as *mut c_void,
                core::mem::size_of_val(&logbuffer) - bytes_in_logbuffer as usize,
            );
            if bytesRead < 0 {
                if errno() != EINTR {
                    // C also: errcode_for_socket_access()
                    ereport!(LOG, errmsg!("could not read from logger pipe: %m"));
                }
            } else if bytesRead > 0 {
                bytes_in_logbuffer += bytesRead as c_int;
                process_pipe_input(logbuffer.as_mut_ptr(), &mut bytes_in_logbuffer);
                continue;
            } else {
                //
                // Zero bytes read when select() is saying read-ready means
                // EOF on the pipe: that is, there are no longer any processes
                // with the pipe write end open.  Therefore, the postmaster
                // and all backends are shut down, and we are done.
                //
                pipe_eof_seen = true;

                // if there's any data left then force it out now
                flush_pipe_input(logbuffer.as_mut_ptr(), &mut bytes_in_logbuffer);
            }
        }

        if pipe_eof_seen {
            //
            // seeing this message on the real stderr is annoying - so we make
            // it DEBUG1 to suppress in normal use.
            //
            ereport!(DEBUG1, errmsg!("logger shutting down"));

            //
            // Normal exit from the syslogger is here.  Note that we
            // deliberately do not close syslogFile before exiting; this is to
            // allow for the possibility of elog messages being generated
            // inside proc_exit.  Regular exit() will take care of flushing
            // and closing stdio channels.
            //
            proc_exit(0);
        }
    }
}

//
// Postmaster subroutine to start a syslogger subprocess.
//
pub unsafe fn SysLogger_Start(child_slot: c_int) -> c_int {
    let mut sysloggerPid: pid_t;
    let mut filename: *mut c_char;

    Assert!(Logging_collector);

    //
    // If first time through, create the pipe which will receive stderr
    // output.
    //
    // If the syslogger crashes and needs to be restarted, we continue to use
    // the same pipe (indeed must do so, since extant backends will be writing
    // into that pipe).
    //
    // This means the postmaster must continue to hold the read end of the
    // pipe open, so we can pass it down to the reincarnated syslogger. This
    // is a bit klugy but we have little choice.
    //
    // Also note that we don't bother counting the pipe FDs by calling
    // Reserve/ReleaseExternalFD.  There's no real need to account for them
    // accurately in the postmaster or syslogger process, and both ends of the
    // pipe will wind up closed in all other postmaster children.
    //
    if syslogPipe[0] < 0 {
        if pipe(syslogPipe.as_mut_ptr()) < 0 {
            // C also: errcode_for_socket_access()
            ereport!(FATAL, errmsg!("could not create pipe for syslog: %m"));
        }
    }

    //
    // Create log directory if not present; ignore errors
    //
    MakePGDirectory(Log_directory);

    //
    // The initial logfile is created right in the postmaster, to verify that
    // the Log_directory is writable.  We save the reference time so that the
    // syslogger child process can recompute this file name.
    //
    // It might look a bit strange to re-do this during a syslogger restart,
    // but we must do so since the postmaster closed syslogFile after the
    // previous fork (and remembering that old file wouldn't be right anyway).
    // Note we always append here, we won't overwrite any existing file.  This
    // is consistent with the normal rules, because by definition this is not
    // a time-based rotation.
    //
    first_syslogger_file_time = time(null_mut());

    filename = logfile_getname(first_syslogger_file_time, null());

    syslogFile = logfile_open(filename, c"a".as_ptr(), false);

    pfree(filename as *mut c_void);

    //
    // Likewise for the initial CSV log file, if that's enabled.  (Note that
    // we open syslogFile even when only CSV output is nominally enabled,
    // since some code paths will write to syslogFile anyway.)
    //
    if Log_destination & LOG_DESTINATION_CSVLOG != 0 {
        filename = logfile_getname(first_syslogger_file_time, c".csv".as_ptr());

        csvlogFile = logfile_open(filename, c"a".as_ptr(), false);

        pfree(filename as *mut c_void);
    }

    //
    // Likewise for the initial JSON log file, if that's enabled.  (Note that
    // we open syslogFile even when only JSON output is nominally enabled,
    // since some code paths will write to syslogFile anyway.)
    //
    if Log_destination & LOG_DESTINATION_JSONLOG != 0 {
        filename = logfile_getname(first_syslogger_file_time, c".json".as_ptr());

        jsonlogFile = logfile_open(filename, c"a".as_ptr(), false);

        pfree(filename as *mut c_void);
    }

    sysloggerPid = postmaster_child_launch(B_LOGGER, child_slot, null(), 0, null_mut());

    if sysloggerPid == -1 {
        ereport!(LOG, errmsg!("could not fork system logger: %m"));
        return 0;
    }

    // success, in postmaster

    // now we redirect stderr, if not done already
    if !redirection_done {
        //
        // Leave a breadcrumb trail when redirecting, in case the user forgets
        // that redirection is active and looks only at the original stderr
        // target file.
        //
        // C also: errhint("Future log output will appear in directory \"%s\".", Log_directory)
        ereport!(
            LOG,
            errmsg!("redirecting log output to logging collector process")
        );

        fflush(stdout_file());
        if dup2(syslogPipe[1], STDOUT_FILENO) < 0 {
            // C also: errcode_for_file_access()
            ereport!(FATAL, errmsg!("could not redirect stdout: %m"));
        }
        fflush(stderr_file());
        if dup2(syslogPipe[1], STDERR_FILENO) < 0 {
            // C also: errcode_for_file_access()
            ereport!(FATAL, errmsg!("could not redirect stderr: %m"));
        }
        // Now we are done with the write end of the pipe.
        close(syslogPipe[1]);
        syslogPipe[1] = -1;
        redirection_done = true;
    }

    // postmaster will never write the file(s); close 'em
    fclose(syslogFile);
    syslogFile = null_mut();
    if !csvlogFile.is_null() {
        fclose(csvlogFile);
        csvlogFile = null_mut();
    }
    if !jsonlogFile.is_null() {
        fclose(jsonlogFile);
        jsonlogFile = null_mut();
    }
    sysloggerPid as c_int
}

// --------------------------------
//		pipe protocol handling
// --------------------------------

//
// Process data received through the syslogger pipe.
//
// This routine interprets the log pipe protocol which sends log messages as
// (hopefully atomic) chunks - such chunks are detected and reassembled here.
//
// The protocol has a header that starts with two nul bytes, then has a 16 bit
// length, the pid of the sending process, and a flag to indicate if it is
// the last chunk in a message. Incomplete chunks are saved until we read some
// more, and non-final chunks are accumulated until we get the final chunk.
//
// All of this is to avoid 2 problems:
// . partial messages being written to logfiles (messes rotation), and
// . messages from different backends being interleaved (messages garbled).
//
// Any non-protocol messages are written out directly. These should only come
// from non-PostgreSQL sources, however (e.g. third party libraries writing to
// stderr).
//
// logbuffer is the data input buffer, and *bytes_in_logbuffer is the number
// of bytes present.  On exit, any not-yet-eaten data is left-justified in
// logbuffer, and *bytes_in_logbuffer is updated.
//
unsafe fn process_pipe_input(logbuffer: *mut c_char, bytes_in_logbuffer: *mut c_int) {
    let mut cursor: *mut c_char = logbuffer;
    let mut count: c_int = *bytes_in_logbuffer;
    let mut dest: c_int = LOG_DESTINATION_STDERR;

    // While we have enough for a header, process data...
    while count >= (PIPE_HEADER_SIZE + 1) as c_int {
        let mut p: PipeProtoHeader = core::mem::zeroed();
        let mut chunklen: c_int;
        let dest_flags: bits8;

        // Do we have a valid header?
        memcpy(
            &mut p as *mut PipeProtoHeader as *mut c_void,
            cursor as *const c_void,
            PIPE_HEADER_SIZE,
        );
        dest_flags =
            p.flags & (PIPE_PROTO_DEST_STDERR | PIPE_PROTO_DEST_CSVLOG | PIPE_PROTO_DEST_JSONLOG);
        if p.nuls[0] == b'\0' as c_char
            && p.nuls[1] == b'\0' as c_char
            && p.len > 0
            && p.len <= PIPE_MAX_PAYLOAD as uint16
            && p.pid != 0
            && pg_number_of_ones[dest_flags as usize] == 1
        {
            let mut buffer_list: *mut List;
            let mut existing_slot: *mut save_buffer = null_mut();
            let mut free_slot: *mut save_buffer = null_mut();
            let str: StringInfo;

            chunklen = PIPE_HEADER_SIZE as c_int + p.len as c_int;

            // Fall out of loop if we don't have the whole chunk yet
            if count < chunklen {
                break;
            }

            if (p.flags & PIPE_PROTO_DEST_STDERR) != 0 {
                dest = LOG_DESTINATION_STDERR;
            } else if (p.flags & PIPE_PROTO_DEST_CSVLOG) != 0 {
                dest = LOG_DESTINATION_CSVLOG;
            } else if (p.flags & PIPE_PROTO_DEST_JSONLOG) != 0 {
                dest = LOG_DESTINATION_JSONLOG;
            } else {
                // this should never happen as of the header validation
                Assert!(false);
            }

            // Locate any existing buffer for this source pid
            buffer_list = buffer_lists[(p.pid as usize) % NBUFFER_LISTS];
            foreach!(cell, buffer_list, {
                let buf: *mut save_buffer = lfirst(current_cell!(cell)) as *mut save_buffer;

                if (*buf).pid == p.pid {
                    existing_slot = buf;
                    break;
                }
                if (*buf).pid == 0 && free_slot.is_null() {
                    free_slot = buf;
                }
            });

            if (p.flags & PIPE_PROTO_IS_LAST) == 0 {
                //
                // Save a complete non-final chunk in a per-pid buffer
                //
                if !existing_slot.is_null() {
                    // Add chunk to data from preceding chunks
                    str = &mut (*existing_slot).data;
                    appendBinaryStringInfo(
                        str,
                        cursor.add(PIPE_HEADER_SIZE) as *const c_void,
                        p.len as c_int,
                    );
                } else {
                    // First chunk of message, save in a new buffer
                    if free_slot.is_null() {
                        //
                        // Need a free slot, but there isn't one in the list,
                        // so create a new one and extend the list with it.
                        //
                        free_slot =
                            palloc(core::mem::size_of::<save_buffer>()) as *mut save_buffer;
                        buffer_list = lappend(buffer_list, free_slot as *mut c_void);
                        buffer_lists[(p.pid as usize) % NBUFFER_LISTS] = buffer_list;
                    }
                    (*free_slot).pid = p.pid;
                    str = &mut (*free_slot).data;
                    initStringInfo(str);
                    appendBinaryStringInfo(
                        str,
                        cursor.add(PIPE_HEADER_SIZE) as *const c_void,
                        p.len as c_int,
                    );
                }
            } else {
                //
                // Final chunk --- add it to anything saved for that pid, and
                // either way write the whole thing out.
                //
                if !existing_slot.is_null() {
                    str = &mut (*existing_slot).data;
                    appendBinaryStringInfo(
                        str,
                        cursor.add(PIPE_HEADER_SIZE) as *const c_void,
                        p.len as c_int,
                    );
                    write_syslogger_file((*str).data, (*str).len, dest);
                    // Mark the buffer unused, and reclaim string storage
                    (*existing_slot).pid = 0;
                    pfree((*str).data as *mut c_void);
                } else {
                    // The whole message was one chunk, evidently.
                    write_syslogger_file(cursor.add(PIPE_HEADER_SIZE), p.len as c_int, dest);
                }
            }

            // Finished processing this chunk
            cursor = cursor.add(chunklen as usize);
            count -= chunklen;
        } else {
            // Process non-protocol data

            //
            // Look for the start of a protocol header.  If found, dump data
            // up to there and repeat the loop.  Otherwise, dump it all and
            // fall out of the loop.  (Note: we want to dump it all if at all
            // possible, so as to avoid dividing non-protocol messages across
            // logfiles.  We expect that in many scenarios, a non-protocol
            // message will arrive all in one read(), and we want to respect
            // the read() boundary if possible.)
            //
            chunklen = 1;
            while chunklen < count {
                if *cursor.add(chunklen as usize) == b'\0' as c_char {
                    break;
                }
                chunklen += 1;
            }
            // fall back on the stderr log as the destination
            write_syslogger_file(cursor, chunklen, LOG_DESTINATION_STDERR);
            cursor = cursor.add(chunklen as usize);
            count -= chunklen;
        }
    }

    // We don't have a full chunk, so left-align what remains in the buffer
    if count > 0 && cursor != logbuffer {
        memmove(
            logbuffer as *mut c_void,
            cursor as *const c_void,
            count as usize,
        );
    }
    *bytes_in_logbuffer = count;
}

//
// Force out any buffered data
//
// This is currently used only at syslogger shutdown, but could perhaps be
// useful at other times, so it is careful to leave things in a clean state.
//
unsafe fn flush_pipe_input(logbuffer: *mut c_char, bytes_in_logbuffer: *mut c_int) {
    // Dump any incomplete protocol messages
    for i in 0..NBUFFER_LISTS {
        let list: *mut List = buffer_lists[i];

        foreach!(cell, list, {
            let buf: *mut save_buffer = lfirst(current_cell!(cell)) as *mut save_buffer;

            if (*buf).pid != 0 {
                let str: StringInfo = &mut (*buf).data;

                write_syslogger_file((*str).data, (*str).len, LOG_DESTINATION_STDERR);
                // Mark the buffer unused, and reclaim string storage
                (*buf).pid = 0;
                pfree((*str).data as *mut c_void);
            }
        });
    }

    //
    // Force out any remaining pipe data as-is; we don't bother trying to
    // remove any protocol headers that may exist in it.
    //
    if *bytes_in_logbuffer > 0 {
        write_syslogger_file(logbuffer, *bytes_in_logbuffer, LOG_DESTINATION_STDERR);
    }
    *bytes_in_logbuffer = 0;
}

// --------------------------------
//		logfile routines
// --------------------------------

//
// Write text to the currently open logfile
//
// This is exported so that elog.c can call it when MyBackendType is B_LOGGER.
// This allows the syslogger process to record elog messages of its own,
// even though its stderr does not point at the syslog pipe.
//
pub unsafe fn write_syslogger_file(buffer: *const c_char, count: c_int, destination: c_int) {
    let rc: c_int;
    let logfile: *mut FILE;

    //
    // If we're told to write to a structured log file, but it's not open,
    // dump the data to syslogFile (which is always open) instead.  This can
    // happen if structured output is enabled after postmaster start and we've
    // been unable to open logFile.  There are also race conditions during a
    // parameter change whereby backends might send us structured output
    // before we open the logFile or after we close it.  Writing formatted
    // output to the regular log file isn't great, but it beats dropping log
    // output on the floor.
    //
    // Think not to improve this by trying to open logFile on-the-fly.  Any
    // failure in that would lead to recursion.
    //
    if (destination & LOG_DESTINATION_CSVLOG) != 0 && !csvlogFile.is_null() {
        logfile = csvlogFile;
    } else if (destination & LOG_DESTINATION_JSONLOG) != 0 && !jsonlogFile.is_null() {
        logfile = jsonlogFile;
    } else {
        logfile = syslogFile;
    }

    rc = fwrite(buffer as *const c_void, 1, count as usize, logfile) as c_int;

    //
    // Try to report any failure.  We mustn't use ereport because it would
    // just recurse right back here, but write_stderr is OK: it will write
    // either to the postmaster's original stderr, or to /dev/null, but never
    // to our input pipe which would result in a different sort of looping.
    //
    if rc != count {
        write_stderr(c"could not write to log file: %m\n".as_ptr());
    }
}

//
// Open a new logfile with proper permissions and buffering options.
//
// If allow_errors is true, we just log any open failure and return NULL
// (with errno still correct for the fopen failure).
// Otherwise, errors are treated as fatal.
//
unsafe fn logfile_open(filename: *const c_char, mode: *const c_char, allow_errors: bool) -> *mut FILE {
    let fh: *mut FILE;
    let oumask: mode_t;

    //
    // Note we do not let Log_file_mode disable IWUSR, since we certainly want
    // to be able to write the files ourselves.
    //
    oumask = umask(
        ((!(Log_file_mode | S_IWUSR)) & (S_IRWXU | S_IRWXG | S_IRWXO)) as mode_t,
    );
    fh = fopen(filename, mode);
    umask(oumask);

    if !fh.is_null() {
        setvbuf(fh, null_mut(), PG_IOLBF, 0);
    } else {
        let save_errno: c_int = errno();

        // C also: errcode_for_file_access()
        ereport!(
            if allow_errors { LOG } else { FATAL },
            errmsg!(
                "could not open log file \"{}\": %m",
                CStr::from_ptr(filename).to_string_lossy()
            )
        );
        set_errno(save_errno);
    }

    fh
}

//
// Do logfile rotation for a single destination, as specified by target_dest.
// The information stored in *last_file_name and *logFile is updated on a
// successful file rotation.
//
// Returns false if the rotation has been stopped, or true to move on to
// the processing of other formats.
//
unsafe fn logfile_rotate_dest(
    time_based_rotation: bool,
    size_rotation_for: c_int,
    fntime: pg_time_t,
    target_dest: c_int,
    last_file_name: *mut *mut c_char,
    logFile: *mut *mut FILE,
) -> bool {
    let logFileExt: *const c_char;
    let filename: *mut c_char;
    let fh: *mut FILE;

    //
    // If the target destination was just turned off, close the previous file
    // and unregister its data.  This cannot happen for stderr as syslogFile
    // is assumed to be always opened even if stderr is disabled in
    // log_destination.
    //
    if (Log_destination & target_dest) == 0 && target_dest != LOG_DESTINATION_STDERR {
        if !(*logFile).is_null() {
            fclose(*logFile);
        }
        *logFile = null_mut();
        if !(*last_file_name).is_null() {
            pfree(*last_file_name as *mut c_void);
        }
        *last_file_name = null_mut();
        return true;
    }

    //
    // Leave if it is not time for a rotation or if the target destination has
    // no need to do a rotation based on the size of its file.
    //
    if !time_based_rotation && (size_rotation_for & target_dest) == 0 {
        return true;
    }

    // file extension depends on the destination type
    if target_dest == LOG_DESTINATION_STDERR {
        logFileExt = null();
    } else if target_dest == LOG_DESTINATION_CSVLOG {
        logFileExt = c".csv".as_ptr();
    } else if target_dest == LOG_DESTINATION_JSONLOG {
        logFileExt = c".json".as_ptr();
    } else {
        // cannot happen
        Assert!(false);
        logFileExt = null();
    }

    // build the new file name
    filename = logfile_getname(fntime, logFileExt);

    //
    // Decide whether to overwrite or append.  We can overwrite if (a)
    // Log_truncate_on_rotation is set, (b) the rotation was triggered by
    // elapsed time and not something else, and (c) the computed file name is
    // different from what we were previously logging into.
    //
    if Log_truncate_on_rotation
        && time_based_rotation
        && !(*last_file_name).is_null()
        && strcmp(filename, *last_file_name) != 0
    {
        fh = logfile_open(filename, c"w".as_ptr(), true);
    } else {
        fh = logfile_open(filename, c"a".as_ptr(), true);
    }

    if fh.is_null() {
        //
        // ENFILE/EMFILE are not too surprising on a busy system; just keep
        // using the old file till we manage to get a new one.  Otherwise,
        // assume something's wrong with Log_directory and stop trying to
        // create files.
        //
        if errno() != ENFILE && errno() != EMFILE {
            ereport!(
                LOG,
                errmsg!("disabling automatic rotation (use SIGHUP to re-enable)")
            );
            rotation_disabled = true;
        }

        if !filename.is_null() {
            pfree(filename as *mut c_void);
        }
        return false;
    }

    // fill in the new information
    if !(*logFile).is_null() {
        fclose(*logFile);
    }
    *logFile = fh;

    // instead of pfree'ing filename, remember it for next time
    if !(*last_file_name).is_null() {
        pfree(*last_file_name as *mut c_void);
    }
    *last_file_name = filename;

    true
}

//
// perform logfile rotation
//
unsafe fn logfile_rotate(time_based_rotation: bool, size_rotation_for: c_int) {
    let fntime: pg_time_t;

    rotation_requested = false as sig_atomic_t;

    //
    // When doing a time-based rotation, invent the new logfile name based on
    // the planned rotation time, not current time, to avoid "slippage" in the
    // file name when we don't do the rotation immediately.
    //
    if time_based_rotation {
        fntime = next_rotation_time;
    } else {
        fntime = time(null_mut());
    }

    // file rotation for stderr
    if !logfile_rotate_dest(
        time_based_rotation,
        size_rotation_for,
        fntime,
        LOG_DESTINATION_STDERR,
        &raw mut last_sys_file_name,
        &raw mut syslogFile,
    ) {
        return;
    }

    // file rotation for csvlog
    if !logfile_rotate_dest(
        time_based_rotation,
        size_rotation_for,
        fntime,
        LOG_DESTINATION_CSVLOG,
        &raw mut last_csv_file_name,
        &raw mut csvlogFile,
    ) {
        return;
    }

    // file rotation for jsonlog
    if !logfile_rotate_dest(
        time_based_rotation,
        size_rotation_for,
        fntime,
        LOG_DESTINATION_JSONLOG,
        &raw mut last_json_file_name,
        &raw mut jsonlogFile,
    ) {
        return;
    }

    update_metainfo_datafile();

    set_next_rotation_time();
}

//
// construct logfile name using timestamp information
//
// If suffix isn't NULL, append it to the name, replacing any ".log"
// that may be in the pattern.
//
// Result is palloc'd.
//
unsafe fn logfile_getname(timestamp: pg_time_t, suffix: *const c_char) -> *mut c_char {
    let filename: *mut c_char;
    let mut len: c_int;

    filename = palloc(MAXPGPATH as usize) as *mut c_char;

    snprintf(filename, MAXPGPATH as usize, c"%s/".as_ptr(), Log_directory);

    len = strlen(filename) as c_int;

    // treat Log_filename as a strftime pattern
    pg_strftime(
        filename.add(len as usize),
        MAXPGPATH - len,
        Log_filename,
        pg_localtime(&timestamp, log_timezone),
    );

    if !suffix.is_null() {
        len = strlen(filename) as c_int;
        if len > 4 && strcmp(filename.add((len - 4) as usize), c".log".as_ptr()) == 0 {
            len -= 4;
        }
        strlcpy(filename.add(len as usize), suffix, (MAXPGPATH - len) as usize);
    }

    filename
}

//
// Determine the next planned rotation time, and store in next_rotation_time.
//
unsafe fn set_next_rotation_time() {
    let mut now: pg_time_t;
    let tm: *mut pg_tm;
    let rotinterval: c_int;

    // nothing to do if time-based rotation is disabled
    if Log_RotationAge <= 0 {
        return;
    }

    //
    // The requirements here are to choose the next time > now that is a
    // "multiple" of the log rotation interval.  "Multiple" can be interpreted
    // fairly loosely.  In this version we align to log_timezone rather than
    // GMT.
    //
    rotinterval = Log_RotationAge * SECS_PER_MINUTE; // convert to seconds
    now = time(null_mut()) as pg_time_t;
    tm = pg_localtime(&now, log_timezone);
    now += (*tm).tm_gmtoff;
    now -= now % rotinterval as pg_time_t;
    now += rotinterval as pg_time_t;
    now -= (*tm).tm_gmtoff;
    next_rotation_time = now;
}

//
// Store the name of the file(s) where the log collector, when enabled, writes
// log messages.  Useful for finding the name(s) of the current log file(s)
// when there is time-based logfile rotation.  Filenames are stored in a
// temporary file and which is renamed into the final destination for
// atomicity.  The file is opened with the same permissions as what gets
// created in the data directory and has proper buffering options.
//
unsafe fn update_metainfo_datafile() {
    let fh: *mut FILE;
    let oumask: mode_t;

    if (Log_destination & LOG_DESTINATION_STDERR) == 0
        && (Log_destination & LOG_DESTINATION_CSVLOG) == 0
        && (Log_destination & LOG_DESTINATION_JSONLOG) == 0
    {
        if unlink(LOG_METAINFO_DATAFILE.as_ptr()) < 0 && errno() != ENOENT {
            // C also: errcode_for_file_access()
            ereport!(
                LOG,
                errmsg!(
                    "could not remove file \"{}\": %m",
                    LOG_METAINFO_DATAFILE.to_string_lossy()
                )
            );
        }
        return;
    }

    // use the same permissions as the data directory for the new file
    oumask = umask(pg_mode_mask);
    fh = fopen(LOG_METAINFO_DATAFILE_TMP.as_ptr(), c"w".as_ptr());
    umask(oumask);

    if !fh.is_null() {
        setvbuf(fh, null_mut(), PG_IOLBF, 0);
    } else {
        // C also: errcode_for_file_access()
        ereport!(
            LOG,
            errmsg!(
                "could not open file \"{}\": %m",
                LOG_METAINFO_DATAFILE_TMP.to_string_lossy()
            )
        );
        return;
    }

    if !last_sys_file_name.is_null() && (Log_destination & LOG_DESTINATION_STDERR) != 0 {
        if fprintf(fh, c"stderr %s\n".as_ptr(), last_sys_file_name) < 0 {
            // C also: errcode_for_file_access()
            ereport!(
                LOG,
                errmsg!(
                    "could not write file \"{}\": %m",
                    LOG_METAINFO_DATAFILE_TMP.to_string_lossy()
                )
            );
            fclose(fh);
            return;
        }
    }

    if !last_csv_file_name.is_null() && (Log_destination & LOG_DESTINATION_CSVLOG) != 0 {
        if fprintf(fh, c"csvlog %s\n".as_ptr(), last_csv_file_name) < 0 {
            // C also: errcode_for_file_access()
            ereport!(
                LOG,
                errmsg!(
                    "could not write file \"{}\": %m",
                    LOG_METAINFO_DATAFILE_TMP.to_string_lossy()
                )
            );
            fclose(fh);
            return;
        }
    }

    if !last_json_file_name.is_null() && (Log_destination & LOG_DESTINATION_JSONLOG) != 0 {
        if fprintf(fh, c"jsonlog %s\n".as_ptr(), last_json_file_name) < 0 {
            // C also: errcode_for_file_access()
            ereport!(
                LOG,
                errmsg!(
                    "could not write file \"{}\": %m",
                    LOG_METAINFO_DATAFILE_TMP.to_string_lossy()
                )
            );
            fclose(fh);
            return;
        }
    }
    fclose(fh);

    if rename(LOG_METAINFO_DATAFILE_TMP.as_ptr(), LOG_METAINFO_DATAFILE.as_ptr()) != 0 {
        // C also: errcode_for_file_access()
        ereport!(
            LOG,
            errmsg!(
                "could not rename file \"{}\" to \"{}\": %m",
                LOG_METAINFO_DATAFILE_TMP.to_string_lossy(),
                LOG_METAINFO_DATAFILE.to_string_lossy()
            )
        );
    }
}

// --------------------------------
//		signal handler routines
// --------------------------------

//
// Check to see if a log rotation request has arrived.  Should be
// called by postmaster after receiving SIGUSR1.
//
pub unsafe fn CheckLogrotateSignal() -> bool {
    let mut stat_buf: libc_stat = core::mem::zeroed();

    if stat(LOGROTATE_SIGNAL_FILE.as_ptr(), &mut stat_buf) == 0 {
        return true;
    }

    false
}

//
// Remove the file signaling a log rotation request.
//
pub unsafe fn RemoveLogrotateSignalFiles() {
    unlink(LOGROTATE_SIGNAL_FILE.as_ptr());
}

// SIGUSR1: set flag to rotate logfile
unsafe extern "C" fn sigUsr1Handler(_postgres_signal_arg: c_int) {
    rotation_requested = true as sig_atomic_t;
    SetLatch(MyLatch);
}

// ---------------------------------------------------------------------------
// EXEC_BACKEND-only helpers (translated from #ifdef EXEC_BACKEND block).
// ---------------------------------------------------------------------------

/*
 * syslogger_fdget() -
 *
 * Utility wrapper to grab the file descriptor of an opened error output
 * file.  Used when building the command to fork the logging collector.
 */
#[allow(dead_code)]
unsafe fn syslogger_fdget(file: *mut FILE) -> c_int {
    // #ifndef WIN32
    if !file.is_null() {
        fileno(file)
    } else {
        -1
    }
    // #else: return (int) _get_osfhandle(_fileno(file)) / 0 (WIN32, not built)
}

/*
 * syslogger_fdopen() -
 *
 * Utility wrapper to re-open an error output file, using the given file
 * descriptor.  Used when parsing arguments in a forked logging collector.
 */
#[allow(dead_code)]
unsafe fn syslogger_fdopen(fd: c_int) -> *mut FILE {
    let mut file: *mut FILE = null_mut();

    // #ifndef WIN32
    if fd != -1 {
        file = fdopen(fd, c"a".as_ptr());
        setvbuf(file, null_mut(), PG_IOLBF, 0);
    }
    // #else: _open_osfhandle path (WIN32, not built)

    file
}

// ---------------------------------------------------------------------------
// WIN32-only thread (translated from #ifdef WIN32 block).
// ---------------------------------------------------------------------------

/*
 * Worker thread to transfer data from the pipe to the current logfile.
 *
 * We need this because on Windows, WaitForMultipleObjects does not work on
 * unnamed pipes: it always reports "signaled", so the blocking ReadFile won't
 * block anyway.
 */
#[allow(dead_code)]
unsafe extern "C" fn pipeThread(_arg: *mut c_void) -> c_uint {
    let mut logbuffer: [c_char; READ_BUF_SIZE as usize] = [0; READ_BUF_SIZE as usize];
    let mut bytes_in_logbuffer: c_int = 0;

    loop {
        let mut bytes_read: DWORD = 0;
        let result: BOOL;

        result = ReadFile(
            syslogPipe[0] as HANDLE,
            logbuffer.as_mut_ptr().add(bytes_in_logbuffer as usize) as *mut c_void,
            (logbuffer.len() as DWORD) - (bytes_in_logbuffer as DWORD),
            &raw mut bytes_read,
            null_mut(),
        );

        /*
         * Enter critical section before doing anything that might touch
         * global state shared by the main thread. Anything that uses
         * palloc()/pfree() in particular are not safe outside the critical
         * section.
         */
        EnterCriticalSection(&raw mut sysloggerSection);
        if result == 0 {
            let error: DWORD = GetLastError();

            if error == ERROR_HANDLE_EOF || error == ERROR_BROKEN_PIPE {
                break;
            }
            _dosmaperr(error);
            ereport!(LOG, errmsg!("could not read from logger pipe: %m"));
            /* C also: errcode_for_file_access() */
        } else if bytes_read > 0 {
            bytes_in_logbuffer += bytes_read as c_int;
            process_pipe_input(logbuffer.as_mut_ptr(), &raw mut bytes_in_logbuffer);
        }

        /*
         * If we've filled the current logfile, nudge the main thread to do a
         * log rotation.
         */
        if Log_RotationSize > 0 {
            if ftello(syslogFile) >= (Log_RotationSize as pgoff_t) * 1024
                || (!csvlogFile.is_null()
                    && ftello(csvlogFile) >= (Log_RotationSize as pgoff_t) * 1024)
                || (!jsonlogFile.is_null()
                    && ftello(jsonlogFile) >= (Log_RotationSize as pgoff_t) * 1024)
            {
                SetLatch(MyLatch);
            }
        }
        LeaveCriticalSection(&raw mut sysloggerSection);
    }

    /* We exit the above loop only upon detecting pipe EOF */
    pipe_eof_seen = true;

    /* if there's any data left then force it out now */
    flush_pipe_input(logbuffer.as_mut_ptr(), &raw mut bytes_in_logbuffer);

    /* set the latch to waken the main thread, which will quit */
    SetLatch(MyLatch);

    LeaveCriticalSection(&raw mut sysloggerSection);
    _endthread();
    0
}

// ---- WIN32 primitives (unported; pipeThread is not built on this platform) ----
#[allow(non_camel_case_types)]
type DWORD = u32;
#[allow(non_camel_case_types)]
type BOOL = c_int;
#[allow(non_camel_case_types)]
type HANDLE = *mut c_void;
const ERROR_HANDLE_EOF: DWORD = 38;
const ERROR_BROKEN_PIPE: DWORD = 109;
static mut sysloggerSection: c_int = 0;

#[allow(non_snake_case)]
unsafe fn ReadFile(
    _h: HANDLE,
    _buf: *mut c_void,
    _n: DWORD,
    _read: *mut DWORD,
    _ovl: *mut c_void,
) -> BOOL {
    todo!("pg-port: win32 ReadFile")
}
#[allow(non_snake_case)]
unsafe fn EnterCriticalSection(_s: *mut c_int) {
    todo!("pg-port: win32 EnterCriticalSection")
}
#[allow(non_snake_case)]
unsafe fn LeaveCriticalSection(_s: *mut c_int) {
    todo!("pg-port: win32 LeaveCriticalSection")
}
#[allow(non_snake_case)]
unsafe fn GetLastError() -> DWORD {
    todo!("pg-port: win32 GetLastError")
}
#[allow(non_snake_case)]
unsafe fn _dosmaperr(_e: DWORD) {
    todo!("pg-port: port/win32error.c _dosmaperr")
}
#[allow(non_snake_case)]
unsafe fn _endthread() {
    todo!("pg-port: win32 _endthread")
}

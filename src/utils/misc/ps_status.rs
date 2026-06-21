//--------------------------------------------------------------------
// ps_status.c
//
// Routines to support changing the ps display of PostgreSQL backends
// to contain some useful information. Mechanism differs wildly across
// platforms.
//
// src/backend/utils/misc/ps_status.c
//
// Copyright (c) 2000-2025, PostgreSQL Global Development Group
// various details abducted from various places
//--------------------------------------------------------------------

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::miscadmin::{MyBackendType, IsUnderPostmaster};

// GUCs (utils/misc/guc_tables.c) - kept local until the GUC machinery lands.
const DEFAULT_UPDATE_PROCESS_TITLE: bool = true;
pub static mut cluster_name: *mut c_char = std::ptr::null_mut();

// <unistd.h>
// #if defined(__darwin__) -> <crt_externs.h>

// On macOS (__darwin__) we use PS_USE_CLOBBER_ARGV and PS_PADDING '\0'.
//
// Alternative ways of updating ps display:
//
// PS_USE_SETPROCTITLE_FAST
//	   use the function setproctitle_fast(const char *, ...)
//	   (FreeBSD)
// PS_USE_SETPROCTITLE
//	   use the function setproctitle(const char *, ...)
//	   (other BSDs)
// PS_USE_CLOBBER_ARGV
//	   write over the argv and environment area
//	   (Linux and most SysV-like systems)
// PS_USE_WIN32
//	   push the string out as the name of a Windows event
// PS_USE_NONE
//	   don't update ps display
//	   (This is the default, as it is safest.)

// Different systems want the buffer padded differently.
// On __darwin__ PS_PADDING is '\0'.
const PS_PADDING: c_char = b'\0' as c_char;

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn malloc(size: usize) -> *mut c_void;
    fn strdup(s: *const c_char) -> *mut c_char;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn exit(status: c_int) -> !;
    // macOS: char ***_NSGetArgv(void) from <crt_externs.h>
    fn _NSGetArgv() -> *mut *mut *mut c_char;
    // extern char **environ;
    static mut environ: *mut *mut c_char;
}

// GUC variable
#[no_mangle]
pub static mut update_process_title: bool = DEFAULT_UPDATE_PROCESS_TITLE;

// PS_USE_CLOBBER_ARGV
static mut ps_buffer: *mut c_char = std::ptr::null_mut(); // will point to argv area
static mut ps_buffer_size: usize = 0; // space determined at run time
static mut last_status_len: usize = 0; // use to minimize length of clobber

static mut ps_buffer_cur_len: usize = 0; // nominal strlen(ps_buffer)

static mut ps_buffer_fixed_size: usize = 0; // size of the constant prefix

// Length of ps_buffer before the suffix was appended to the end, or 0 if we
// didn't set a suffix.
static mut ps_buffer_nosuffix_len: usize = 0;

// save the original argv[] location here
static mut save_argc: c_int = 0;
static mut save_argv: *mut *mut c_char = std::ptr::null_mut();

// Call this early in startup to save the original argc/argv values.
// If needed, we make a copy of the original argv[] array to preserve it
// from being clobbered by subsequent ps_display actions.
//
// (The original argv[] will not be overwritten by this routine, but may be
// overwritten during init_ps_display.  Also, the physical location of the
// environment strings may be moved, so this should be called before any code
// that might try to hang onto a getenv() result.  But see hack for musl
// within.)
//
// Note that in case of failure this cannot call elog() as that is not
// initialized yet.  We rely on write_stderr() instead.
#[no_mangle]
pub unsafe extern "C" fn save_ps_display_args(
    mut argc: c_int,
    mut argv: *mut *mut c_char,
) -> *mut *mut c_char {
    save_argc = argc;
    save_argv = argv;

    // PS_USE_CLOBBER_ARGV

    // If we're going to overwrite the argv area, count the available space.
    // Also move the environment strings to make additional room.
    {
        let mut end_of_area: *mut c_char = std::ptr::null_mut();
        let new_environ: *mut *mut c_char;
        let mut i: c_int;

        // check for contiguous argv strings
        i = 0;
        while i < argc {
            let av = *argv.offset(i as isize);
            if i == 0 || end_of_area.offset(1) == av {
                end_of_area = av.add(strlen(av));
            }
            i += 1;
        }

        if end_of_area.is_null() {
            // probably can't happen?
            ps_buffer = std::ptr::null_mut();
            ps_buffer_size = 0;
            return argv;
        }

        // check for contiguous environ strings following argv
        i = 0;
        while !(*environ.offset(i as isize)).is_null() {
            let ev = *environ.offset(i as isize);
            if end_of_area.offset(1) == ev {
                // The musl-related __linux__ special case does not apply on
                // __darwin__; just advance to the end of this string.
                end_of_area = ev.add(strlen(ev));
            }
            i += 1;
        }

        ps_buffer = *argv.offset(0);
        ps_buffer_size = end_of_area.offset_from(*argv.offset(0)) as usize;
        last_status_len = ps_buffer_size;

        // move the environment out of the way
        new_environ =
            malloc(((i + 1) as usize) * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
        if new_environ.is_null() {
            write_stderr(c"out of memory\n".as_ptr());
            exit(1);
        }
        i = 0;
        while !(*environ.offset(i as isize)).is_null() {
            let dup = strdup(*environ.offset(i as isize));
            *new_environ.offset(i as isize) = dup;
            if (*new_environ.offset(i as isize)).is_null() {
                write_stderr(c"out of memory\n".as_ptr());
                exit(1);
            }
            i += 1;
        }
        *new_environ.offset(i as isize) = std::ptr::null_mut();
        environ = new_environ;
    }

    // If we're going to change the original argv[] then make a copy for
    // argument parsing purposes.
    //
    // NB: do NOT think to remove the copying of argv[], even though
    // postmaster.c finishes looking at argv[] long before we ever consider
    // changing the ps display.  On some platforms, getopt() keeps pointers
    // into the argv array, and will get horribly confused when it is
    // re-called to analyze a subprocess' argument string if the argv storage
    // has been clobbered meanwhile.  Other platforms have other dependencies
    // on argv[].
    {
        let new_argv: *mut *mut c_char;
        let mut i: c_int;

        new_argv =
            malloc(((argc + 1) as usize) * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
        if new_argv.is_null() {
            write_stderr(c"out of memory\n".as_ptr());
            exit(1);
        }
        i = 0;
        while i < argc {
            let dup = strdup(*argv.offset(i as isize));
            *new_argv.offset(i as isize) = dup;
            if (*new_argv.offset(i as isize)).is_null() {
                write_stderr(c"out of memory\n".as_ptr());
                exit(1);
            }
            i += 1;
        }
        *new_argv.offset(argc as isize) = std::ptr::null_mut();

        // macOS has a static copy of the argv pointer, which we may fix like
        // so:
        *_NSGetArgv() = new_argv;

        argv = new_argv;
    }

    let _ = &mut argc; // argc not used further after clobber section

    argv
}

// Call this once during subprocess startup to set the identification
// values.
//
// If fixed_part is NULL, a default will be obtained from MyBackendType.
//
// At this point, the original argv[] array may be overwritten.
#[no_mangle]
pub unsafe extern "C" fn init_ps_display(mut fixed_part: *const c_char) {
    let save_update_process_title: bool;

    Assert!(!fixed_part.is_null() || MyBackendType != 0);
    if fixed_part.is_null() {
        fixed_part = GetBackendTypeDesc(MyBackendType);
    }

    // no ps display for stand-alone backend
    if !IsUnderPostmaster {
        return;
    }

    // no ps display if you didn't call save_ps_display_args()
    if save_argv.is_null() {
        return;
    }

    // PS_USE_CLOBBER_ARGV
    // If ps_buffer is a pointer, it might still be null
    if ps_buffer.is_null() {
        return;
    }

    // make extra argv slots point at end_of_area (a NUL)
    let mut i: c_int = 1;
    while i < save_argc {
        *save_argv.offset(i as isize) = ps_buffer.add(ps_buffer_size);
        i += 1;
    }

    // Make fixed prefix of ps display.

    // PS_USE_CLOBBER_ARGV uses the "postgres: " prefix.
    // #define PROGRAM_NAME_PREFIX "postgres: "

    if *cluster_name == 0 {
        snprintf(
            ps_buffer,
            ps_buffer_size,
            c"postgres: %s ".as_ptr(),
            fixed_part,
        );
    } else {
        snprintf(
            ps_buffer,
            ps_buffer_size,
            c"postgres: %s: %s ".as_ptr(),
            cluster_name,
            fixed_part,
        );
    }

    ps_buffer_cur_len = strlen(ps_buffer);
    ps_buffer_fixed_size = ps_buffer_cur_len;

    // On the first run, force the update.
    save_update_process_title = update_process_title;
    update_process_title = true;
    set_ps_display(c"".as_ptr());
    update_process_title = save_update_process_title;
}

// update_ps_display_precheck
//		Helper function to determine if updating the process title is
//		something that we need to do.
unsafe fn update_ps_display_precheck() -> bool {
    // update_process_title=off disables updates
    if !update_process_title {
        return false;
    }

    // no ps display for stand-alone backend
    if !IsUnderPostmaster {
        return false;
    }

    // PS_USE_CLOBBER_ARGV
    // If ps_buffer is a pointer, it might still be null
    if ps_buffer.is_null() {
        return false;
    }

    true
}

// set_ps_display_suffix
//		Adjust the process title to append 'suffix' onto the end with a space
//		between it and the current process title.
#[no_mangle]
pub unsafe extern "C" fn set_ps_display_suffix(suffix: *const c_char) {
    let len: usize;

    // first, check if we need to update the process title
    if !update_ps_display_precheck() {
        return;
    }

    // if there's already a suffix, overwrite it
    if ps_buffer_nosuffix_len > 0 {
        ps_buffer_cur_len = ps_buffer_nosuffix_len;
    } else {
        ps_buffer_nosuffix_len = ps_buffer_cur_len;
    }

    len = strlen(suffix);

    // check if we have enough space to append the suffix
    if ps_buffer_cur_len + len + 1 >= ps_buffer_size {
        // not enough space.  Check the buffer isn't full already
        if ps_buffer_cur_len < ps_buffer_size - 1 {
            // append a space before the suffix
            *ps_buffer.add(ps_buffer_cur_len) = b' ' as c_char;
            ps_buffer_cur_len += 1;

            // just add what we can and fill the ps_buffer
            memcpy(
                ps_buffer.add(ps_buffer_cur_len) as *mut c_void,
                suffix as *const c_void,
                ps_buffer_size - ps_buffer_cur_len - 1,
            );
            *ps_buffer.add(ps_buffer_size - 1) = b'\0' as c_char;
            ps_buffer_cur_len = ps_buffer_size - 1;
        }
    } else {
        *ps_buffer.add(ps_buffer_cur_len) = b' ' as c_char;
        ps_buffer_cur_len += 1;
        memcpy(
            ps_buffer.add(ps_buffer_cur_len) as *mut c_void,
            suffix as *const c_void,
            len + 1,
        );
        ps_buffer_cur_len = ps_buffer_cur_len + len;
    }

    Assert!(strlen(ps_buffer) == ps_buffer_cur_len);

    // and set the new title
    flush_ps_display();
}

// set_ps_display_remove_suffix
//		Remove the process display suffix added by set_ps_display_suffix
#[no_mangle]
pub unsafe extern "C" fn set_ps_display_remove_suffix() {
    // first, check if we need to update the process title
    if !update_ps_display_precheck() {
        return;
    }

    // check we added a suffix
    if ps_buffer_nosuffix_len == 0 {
        return; // no suffix
    }

    // remove the suffix from ps_buffer
    *ps_buffer.add(ps_buffer_nosuffix_len) = b'\0' as c_char;
    ps_buffer_cur_len = ps_buffer_nosuffix_len;
    ps_buffer_nosuffix_len = 0;

    Assert!(ps_buffer_cur_len == strlen(ps_buffer));

    // and set the new title
    flush_ps_display();
}

// Call this to update the ps status display to a fixed prefix plus an
// indication of what you're currently doing passed in the argument.
//
// 'len' must be the same as strlen(activity)
#[no_mangle]
pub unsafe extern "C" fn set_ps_display_with_len(activity: *const c_char, len: usize) {
    Assert!(strlen(activity) == len);

    // first, check if we need to update the process title
    if !update_ps_display_precheck() {
        return;
    }

    // wipe out any suffix when the title is completely changed
    ps_buffer_nosuffix_len = 0;

    // Update ps_buffer to contain both fixed part and activity
    if ps_buffer_fixed_size + len >= ps_buffer_size {
        // handle the case where ps_buffer doesn't have enough space
        memcpy(
            ps_buffer.add(ps_buffer_fixed_size) as *mut c_void,
            activity as *const c_void,
            ps_buffer_size - ps_buffer_fixed_size - 1,
        );
        *ps_buffer.add(ps_buffer_size - 1) = b'\0' as c_char;
        ps_buffer_cur_len = ps_buffer_size - 1;
    } else {
        memcpy(
            ps_buffer.add(ps_buffer_fixed_size) as *mut c_void,
            activity as *const c_void,
            len + 1,
        );
        ps_buffer_cur_len = ps_buffer_fixed_size + len;
    }
    Assert!(strlen(ps_buffer) == ps_buffer_cur_len);

    // Transmit new setting to kernel, if necessary
    flush_ps_display();
}

unsafe fn flush_ps_display() {
    // PS_USE_CLOBBER_ARGV
    // pad unused memory; need only clobber remainder of old status string
    if last_status_len > ps_buffer_cur_len {
        memset(
            ps_buffer.add(ps_buffer_cur_len) as *mut c_void,
            PS_PADDING as c_int,
            last_status_len - ps_buffer_cur_len,
        );
    }
    last_status_len = ps_buffer_cur_len;
}

// Returns what's currently in the ps display, in case someone needs
// it.  Note that only the activity part is returned.  On some platforms
// the string will not be null-terminated, so return the effective
// length into *displen.
#[no_mangle]
pub unsafe extern "C" fn get_ps_display(displen: *mut c_int) -> *const c_char {
    // PS_USE_CLOBBER_ARGV
    // If ps_buffer is a pointer, it might still be null
    if ps_buffer.is_null() {
        *displen = 0;
        return c"".as_ptr();
    }

    *displen = (ps_buffer_cur_len - ps_buffer_fixed_size) as c_int;

    ps_buffer.add(ps_buffer_fixed_size)
}

// set_ps_display
//		inlined to allow strlen to be evaluated during compilation when
//		passing string constants.
//
// (from ps_status.h)
#[inline]
pub unsafe fn set_ps_display(activity: *const c_char) {
    set_ps_display_with_len(activity, strlen(activity));
}

// --- local stubs for unported dependencies ---

unsafe fn write_stderr(_fmt: *const c_char) {
    unimplemented!() // TODO: src/backend/utils/error/elog.c
}

unsafe fn GetBackendTypeDesc(_backend_type: c_int) -> *const c_char {
    crate::miscadmin::GetBackendTypeDesc(_backend_type as _)
}

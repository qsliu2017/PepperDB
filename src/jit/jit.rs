//! Provider independent JIT infrastructure.
//!
//! Code related to loading JIT providers, redirecting calls into JIT providers
//! and error handling.  No code specific to a specific JIT implementation
//! should end up here.
//!
//! Translation of postgres/src/backend/jit/jit.c
//! Merged with postgres/src/include/jit/jit.h
//!
//! Copyright (c) 2016-2025, PostgreSQL Global Development Group

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::c::{Size, FLEXIBLE_ARRAY_MEMBER};
use crate::portability::instr_time::{instr_time, INSTR_TIME_ADD};
use crate::utils::fmgr::FunctionCallInfo;
use crate::pg_config_manual::MAXPGPATH;
use crate::miscadmin::pkglib_path;
use crate::PG_RETURN_BOOL;

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
}

// DLSUFFIX is the platform shared-library suffix (set by configure; ".so" here).
static DLSUFFIX: [u8; 4] = *b".so\0";

// --- Stubs for not-yet-ported deep deps (faithful signatures). ---

unsafe fn pg_file_exists(_name: *const c_char) -> bool {
    unimplemented!() // TODO: common/file_utils.c
}

unsafe fn load_external_function(
    _filename: *const c_char,
    _funcname: *const c_char,
    _signalNotFound: bool,
    _filehandle: *mut *mut c_void,
) -> *mut c_void {
    unimplemented!() // TODO: utils/fmgr/dfmgr.c
}

/* ----------------------------------------------------------------
 * jit.h
 * ----------------------------------------------------------------
 */

/* Flags determining what kind of JIT operations to perform */
pub const PGJIT_NONE: c_int = 0;
pub const PGJIT_PERFORM: c_int = 1 << 0;
pub const PGJIT_OPT3: c_int = 1 << 1;
pub const PGJIT_INLINE: c_int = 1 << 2;
pub const PGJIT_EXPR: c_int = 1 << 3;
pub const PGJIT_DEFORM: c_int = 1 << 4;

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct JitInstrumentation {
    /* number of emitted functions */
    pub created_functions: Size,

    /* accumulated time to generate code */
    pub generation_counter: instr_time,

    /* accumulated time to deform tuples, included into generation_counter */
    pub deform_counter: instr_time,

    /* accumulated time for inlining */
    pub inlining_counter: instr_time,

    /* accumulated time for optimization */
    pub optimization_counter: instr_time,

    /* accumulated time for code emission */
    pub emission_counter: instr_time,
}

/*
 * DSM structure for accumulating jit instrumentation of all workers.
 */
#[repr(C)]
pub struct SharedJitInstrumentation {
    pub num_workers: c_int,
    pub jit_instr: [JitInstrumentation; FLEXIBLE_ARRAY_MEMBER],
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct JitContext {
    /* see PGJIT_* above */
    pub flags: c_int,

    pub instr: JitInstrumentation,
}

/* Forward declaration of ExprState (defined in nodes::execnodes). */
pub use crate::nodes::execnodes::ExprState;

pub type JitProviderInit = Option<unsafe extern "C" fn(cb: *mut JitProviderCallbacks)>;
pub type JitProviderResetAfterErrorCB = Option<unsafe extern "C" fn()>;
pub type JitProviderReleaseContextCB = Option<unsafe extern "C" fn(context: *mut JitContext)>;
pub type JitProviderCompileExprCB = Option<unsafe extern "C" fn(state: *mut ExprState) -> bool>;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct JitProviderCallbacks {
    pub reset_after_error: JitProviderResetAfterErrorCB,
    pub release_context: JitProviderReleaseContextCB,
    pub compile_expr: JitProviderCompileExprCB,
}

impl Default for JitProviderCallbacks {
    fn default() -> Self {
        JitProviderCallbacks {
            reset_after_error: None,
            release_context: None,
            compile_expr: None,
        }
    }
}

/* ----------------------------------------------------------------
 * jit.c
 * ----------------------------------------------------------------
 */

/* GUCs */
#[no_mangle]
pub static mut jit_enabled: bool = true;
#[no_mangle]
pub static mut jit_provider: *mut c_char = std::ptr::null_mut();
#[no_mangle]
pub static mut jit_debugging_support: bool = false;
#[no_mangle]
pub static mut jit_dump_bitcode: bool = false;
#[no_mangle]
pub static mut jit_expressions: bool = true;
#[no_mangle]
pub static mut jit_profiling_support: bool = false;
#[no_mangle]
pub static mut jit_tuple_deforming: bool = true;
#[no_mangle]
pub static mut jit_above_cost: f64 = 100000.0;
#[no_mangle]
pub static mut jit_inline_above_cost: f64 = 500000.0;
#[no_mangle]
pub static mut jit_optimize_above_cost: f64 = 500000.0;

static mut provider: JitProviderCallbacks = JitProviderCallbacks {
    reset_after_error: None,
    release_context: None,
    compile_expr: None,
};
static mut provider_successfully_loaded: bool = false;
static mut provider_failed_loading: bool = false;

/*
 * SQL level function returning whether JIT is available in the current
 * backend. Will attempt to load JIT provider if necessary.
 */
pub unsafe fn pg_jit_available(fcinfo: FunctionCallInfo) -> Datum {
    PG_RETURN_BOOL!(provider_init());
}

/*
 * Return whether a JIT provider has successfully been loaded, caching the
 * result.
 */
unsafe fn provider_init() -> bool {
    let mut path = [0 as c_char; MAXPGPATH as usize];
    let init: JitProviderInit;

    /* don't even try to load if not enabled */
    if !jit_enabled {
        return false;
    }

    /*
     * Don't retry loading after failing - attempting to load JIT provider
     * isn't cheap.
     */
    if provider_failed_loading {
        return false;
    }
    if provider_successfully_loaded {
        return true;
    }

    /*
     * Check whether shared library exists. We do that check before actually
     * attempting to load the shared library (via load_external_function()),
     * because that'd error out in case the shlib isn't available.
     */
    snprintf(
        path.as_mut_ptr(),
        MAXPGPATH as usize,
        c"%s/%s%s".as_ptr(),
        pkglib_path.as_ptr(),
        jit_provider,
        DLSUFFIX.as_ptr() as *const c_char,
    );
    elog!(DEBUG1, "probing availability of JIT provider at {}", "path");
    if !pg_file_exists(path.as_ptr()) {
        elog!(
            DEBUG1,
            "provider not available, disabling JIT for current session"
        );
        provider_failed_loading = true;
        return false;
    }

    /*
     * If loading functions fails, signal failure. We do so because
     * load_external_function() might error out despite the above check if
     * e.g. the library's dependencies aren't installed. We want to signal
     * ERROR in that case, so the user is notified, but we don't want to
     * continually retry.
     */
    provider_failed_loading = true;

    /* and initialize */
    init = std::mem::transmute::<_, JitProviderInit>(load_external_function(
        path.as_ptr(),
        c"_PG_jit_provider_init".as_ptr(),
        true,
        std::ptr::null_mut(),
    ));
    (init.unwrap())(std::ptr::addr_of_mut!(provider));

    provider_successfully_loaded = true;
    provider_failed_loading = false;

    elog!(DEBUG1, "successfully loaded JIT provider in current session");

    true
}

/*
 * Reset JIT provider's error handling. This'll be called after an error has
 * been thrown and the main-loop has re-established control.
 */
pub unsafe fn jit_reset_after_error() {
    if provider_successfully_loaded {
        (provider.reset_after_error.unwrap())();
    }
}

/*
 * Release resources required by one JIT context.
 */
pub unsafe fn jit_release_context(context: *mut JitContext) {
    if provider_successfully_loaded {
        (provider.release_context.unwrap())(context);
    }

    pfree(context as *mut _);
}

/*
 * Ask provider to JIT compile an expression.
 *
 * Returns true if successful, false if not.
 */
pub unsafe fn jit_compile_expr(state: *mut ExprState) -> bool {
    /*
     * We can easily create a one-off context for functions without an
     * associated PlanState (and thus EState). But because there's no executor
     * shutdown callback that could deallocate the created function, they'd
     * live to the end of the transactions, where they'd be cleaned up by the
     * resowner machinery. That can lead to a noticeable amount of memory
     * usage, and worse, trigger some quadratic behaviour in gdb. Therefore,
     * at least for now, don't create a JITed function in those circumstances.
     */
    if (*state).parent.is_null() {
        return false;
    }

    /* if no jitting should be performed at all */
    if ((*(*(*state).parent).state).es_jit_flags & PGJIT_PERFORM) == 0 {
        return false;
    }

    /* or if expressions aren't JITed */
    if ((*(*(*state).parent).state).es_jit_flags & PGJIT_EXPR) == 0 {
        return false;
    }

    /* this also takes !jit_enabled into account */
    if provider_init() {
        return (provider.compile_expr.unwrap())(state);
    }

    false
}

/* Aggregate JIT instrumentation information */
pub unsafe fn InstrJitAgg(dst: *mut JitInstrumentation, add: *mut JitInstrumentation) {
    (*dst).created_functions += (*add).created_functions;
    INSTR_TIME_ADD(&mut (*dst).generation_counter, (*add).generation_counter);
    INSTR_TIME_ADD(&mut (*dst).deform_counter, (*add).deform_counter);
    INSTR_TIME_ADD(&mut (*dst).inlining_counter, (*add).inlining_counter);
    INSTR_TIME_ADD(
        &mut (*dst).optimization_counter,
        (*add).optimization_counter,
    );
    INSTR_TIME_ADD(&mut (*dst).emission_counter, (*add).emission_counter);
}

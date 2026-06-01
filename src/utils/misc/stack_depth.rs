//! Process stack-depth monitoring and limiting.
//!
//! Source: postgres/src/backend/utils/misc/stack_depth.c
//! Merged decls from postgres/src/include/miscadmin.h:
//!   - pg_stack_base_t (typedef char *)
//!   - max_stack_depth (GUC, kB)
//!   - STACK_DEPTH_SLOP constant
//! and the prototypes for set_stack_base/restore_stack_base/check_stack_depth/
//! stack_is_too_deep/get_stack_depth_rlimit.
//!
//! FULLY REAL: the recursion guard (check_stack_depth/stack_is_too_deep) used
//! pervasively across the backend. The pointer-difference logic, the rlimit
//! probe, and the GUC check/assign validation are all faithful translations of
//! the C. The only genuinely platform-specific piece -- the kernel stack limit
//! via getrlimit(RLIMIT_STACK) -- is bound through extern "C" libc per-platform.
//!
//! NOTE on globals: the C file uses `ssize_t` (signed) for
//! `max_stack_depth_bytes`. Per the porting brief we model it with `c_long`
//! (also signed, 64-bit on LP64 targets), matching `ssize_t` in practice.
//!
//! The Itanium "register stack" base pointer (register_stack_base_ptr, guarded
//! by __ia64__ in upstream) is not modeled: PepperDB does not target Itanium.

use crate::prelude::*;

/* ----------------------------------------------------------------
 * miscadmin.h merged declarations
 * ---------------------------------------------------------------- */

/// `typedef char *pg_stack_base_t;` (miscadmin.h).
pub type pg_stack_base_t = *mut c_char;

/// Required daylight between max_stack_depth and the kernel limit, in bytes.
/// `#define STACK_DEPTH_SLOP (512 * 1024)` (miscadmin.h).
pub const STACK_DEPTH_SLOP: c_long = 512 * 1024;

/* ----------------------------------------------------------------
 * GUC hook plumbing (utils/guc_hooks.h)
 *
 * The full GUC machinery (guc.c) is not yet ported. We only need the *type* of
 * the `source` argument to `check_max_stack_depth`; upstream passes a
 * `GucSource` enum that this hook never inspects. We model it with a local stub
 * so the signature is faithful and call-compatible once guc.c lands.
 * ---------------------------------------------------------------- */

/// Stub for `GucSource` (utils/guc.h). The value is unused by this hook.
// TODO(pg-port): replace with the real GucSource enum once utils/guc is ported.
pub type GucSource = c_int;

/// `GUC_check_errdetail(...)`/`GUC_check_errhint(...)` (guc.h) stage a detail/
/// hint string onto the in-flight GUC check error. Until guc.c is ported these
/// are no-op shims that merely format their argument (mirroring how errmsg! is
/// modeled in elog.rs); call sites keep the real format string.
// TODO(pg-port): wire to the real GUC_check_errmsg_string buffer in guc.c.
macro_rules! GUC_check_errdetail {
    ($($arg:tt)*) => {{
        let _detail: String = format!($($arg)*);
        let _ = _detail;
    }};
}
macro_rules! GUC_check_errhint {
    ($($arg:tt)*) => {{
        let _hint: String = format!($($arg)*);
        let _ = _hint;
    }};
}

/* ----------------------------------------------------------------
 * File-static globals
 * ---------------------------------------------------------------- */

/// GUC variable for maximum stack depth (measured in kilobytes).
/// `int max_stack_depth = 100;`
#[no_mangle]
pub static mut max_stack_depth: c_int = 100;

/// max_stack_depth converted to bytes for speed of checking.
/// `static ssize_t max_stack_depth_bytes = 100 * (ssize_t) 1024;`
static mut max_stack_depth_bytes: c_long = 100 * 1024;

/// Stack base pointer -- initialized by set_stack_base(), which should be
/// called from main(). `static char *stack_base_ptr = NULL;`
static mut stack_base_ptr: *mut c_char = null_mut();

/* ----------------------------------------------------------------
 * Platform stack rlimit probe (getrlimit(RLIMIT_STACK))
 * ---------------------------------------------------------------- */

// `struct rlimit` and getrlimit() from <sys/resource.h>. The rlim_t members are
// unsigned 64-bit on the LP64 Unixes we target (Linux/glibc, macOS/BSD).
#[repr(C)]
struct rlimit {
    rlim_cur: u64,
    rlim_max: u64,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
const RLIMIT_STACK: c_int = 3;
#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "dragonfly"
))]
const RLIMIT_STACK: c_int = 3;

// RLIM_INFINITY: ((rlim_t)-1) on these platforms.
const RLIM_INFINITY: u64 = u64::MAX;

// SSIZE_MAX clamp target: largest positive value of the signed result type.
const SSIZE_MAX: c_long = c_long::MAX;

extern "C" {
    fn getrlimit(resource: c_int, rlim: *mut rlimit) -> c_int;
}

/* ----------------------------------------------------------------
 * set_stack_base / restore_stack_base
 * ---------------------------------------------------------------- */

/// set_stack_base: set up reference point for stack depth checking.
/// Returns the old reference point, if any.
///
/// Upstream prefers `__builtin_frame_address(0)` when available to avoid a
/// warning about storing a local's address in a long-lived global. Rust has no
/// portable frame-address intrinsic, so we use the address of a local (the
/// `#else` branch of the C), which is exactly the reference semantics needed.
pub fn set_stack_base() -> pg_stack_base_t {
    let dummy: u8 = 0;
    let old: pg_stack_base_t = unsafe { stack_base_ptr };

    let p = &dummy as *const u8 as *mut c_char;
    unsafe {
        stack_base_ptr = p;
    }

    // Keep `dummy` live until after we have taken its address.
    core::hint::black_box(&dummy);

    old
}

/// restore_stack_base: restore reference point for stack depth checking.
///
/// Used after set_stack_base() to restore the old value (PL/Java thread case).
pub fn restore_stack_base(base: pg_stack_base_t) {
    unsafe {
        stack_base_ptr = base;
    }
}

/* ----------------------------------------------------------------
 * check_stack_depth / stack_is_too_deep
 * ---------------------------------------------------------------- */

/// check_stack_depth: throw an error if recursion is excessively deep.
///
/// Call somewhere in any recursive routine that might overflow the stack. Most
/// Unixen treat stack overflow as an unrecoverable SIGSEGV, so we error out
/// ourselves before hitting the hardware limit.
pub fn check_stack_depth() {
    if stack_is_too_deep() {
        let _ = errcode(ERRCODE_STATEMENT_TOO_COMPLEX);
        ereport!(
            ERROR,
            errmsg!(
                "stack depth limit exceeded; hint: Increase the configuration \
                 parameter \"max_stack_depth\" (currently {}kB), after ensuring \
                 the platform's stack depth limit is adequate.",
                unsafe { max_stack_depth }
            )
        );
    }
}

/// stack_is_too_deep: report whether the current call is too deeply nested.
///
/// Used by code that wants to handle the over-recursion condition itself rather
/// than summarily erroring out.
pub fn stack_is_too_deep() -> bool {
    let stack_top_loc: c_char = 0;

    // Compute distance from reference point to my local variables.
    let here = &stack_top_loc as *const c_char as isize;
    let base = unsafe { stack_base_ptr } as isize;
    let mut stack_depth: c_long = (base - here) as c_long;

    // Take abs value, since stacks grow up on some machines, down on others.
    if stack_depth < 0 {
        stack_depth = -stack_depth;
    }

    // Trouble?
    //
    // The test on stack_base_ptr prevents us from erroring out if called before
    // that's been set. Logically it should be done first, but putting it last
    // avoids wasting cycles during normal cases.
    if stack_depth > unsafe { max_stack_depth_bytes } && unsafe { !stack_base_ptr.is_null() } {
        return true;
    }

    // Keep the probe local live across the pointer arithmetic above.
    core::hint::black_box(&stack_top_loc);

    false
}

/* ----------------------------------------------------------------
 * GUC check / assign hooks for max_stack_depth
 * ---------------------------------------------------------------- */

/// GUC check hook for max_stack_depth.
///
/// In C the signature is `bool check_max_stack_depth(int *newval, void **extra,
/// GucSource source)`. We keep `newval`/`extra` as raw pointers (the GUC
/// machinery passes by reference) and `source` typed as the GucSource stub.
/// `extra`/`source` are unused by the real validation, exactly as upstream.
pub fn check_max_stack_depth(
    newval: *mut c_int,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    let newval_bytes: c_long = unsafe { *newval } as c_long * 1024;
    let stack_rlimit: c_long = get_stack_depth_rlimit();

    if stack_rlimit > 0 && newval_bytes > stack_rlimit - STACK_DEPTH_SLOP {
        GUC_check_errdetail!(
            "\"max_stack_depth\" must not exceed {}kB.",
            (stack_rlimit - STACK_DEPTH_SLOP) / 1024
        );
        GUC_check_errhint!(
            "Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent."
        );
        return false;
    }
    true
}

/// GUC assign hook for max_stack_depth.
pub fn assign_max_stack_depth(newval: c_int, _extra: *mut c_void) {
    let newval_bytes: c_long = newval as c_long * 1024;
    unsafe {
        max_stack_depth_bytes = newval_bytes;
    }
}

/* ----------------------------------------------------------------
 * get_stack_depth_rlimit
 * ---------------------------------------------------------------- */

/// Obtain platform stack depth limit (in bytes). Return -1 if unknown.
///
/// We use a signed result type (c_long ~ ssize_t) because callers compute
/// values that can go negative, e.g. "result - STACK_DEPTH_SLOP".
pub fn get_stack_depth_rlimit() -> c_long {
    // static ssize_t val = 0; -- cached after the first probe, since the kernel
    // limit won't change after process launch.
    static mut VAL: c_long = 0;

    unsafe {
        if VAL == 0 {
            let mut rlim = rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if getrlimit(RLIMIT_STACK, &mut rlim) < 0 {
                VAL = -1;
            } else if rlim.rlim_cur == RLIM_INFINITY {
                VAL = SSIZE_MAX;
            } else if rlim.rlim_cur >= SSIZE_MAX as u64 {
                // rlim_cur is an unsigned type, so guard against overflow.
                VAL = SSIZE_MAX;
            } else {
                VAL = rlim.rlim_cur as c_long;
            }
        }
        VAL
    }
}

/* ----------------------------------------------------------------
 * errcodes.h classification (errcode() shim ignores the value).
 * ---------------------------------------------------------------- */
// TODO(pg-port): ERRCODE_STATEMENT_TOO_COMPLEX from utils/errcodes.h
// (MAKE_SQLSTATE('5','4','0','0','1')).
const ERRCODE_STATEMENT_TOO_COMPLEX: c_int = 0;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shallow_call_is_not_too_deep() {
        // Establish a reference point at this (shallow) frame.
        let old = set_stack_base();

        // A shallow call must not be flagged as too deep, and the guard must
        // not panic.
        assert!(!stack_is_too_deep());
        check_stack_depth(); // must not panic

        restore_stack_base(old);
    }

    #[test]
    fn before_set_stack_base_is_never_too_deep() {
        // With stack_base_ptr NULL the guard must return false regardless of the
        // computed (garbage) depth -- this is the "called before set" guard.
        unsafe {
            stack_base_ptr = null_mut();
        }
        assert!(!stack_is_too_deep());
    }

    #[test]
    fn rlimit_probe_is_sane() {
        // Either unknown (-1) or a positive byte count; never zero on return.
        let r = get_stack_depth_rlimit();
        assert!(r == -1 || r > 0);
    }

    #[test]
    fn assign_hook_updates_bytes_and_triggers_depth() {
        let old = set_stack_base();

        // Force the byte budget to zero; now any nonzero distance from the base
        // is "too deep". Exercises the real comparison path.
        assign_max_stack_depth(0, null_mut());
        assert_eq!(unsafe { max_stack_depth_bytes }, 0);
        assert!(stack_is_too_deep());

        // Restore a generous budget; shallow again.
        assign_max_stack_depth(100, null_mut());
        assert_eq!(unsafe { max_stack_depth_bytes }, 100 * 1024);
        assert!(!stack_is_too_deep());

        restore_stack_base(old);
    }

    #[test]
    fn check_hook_rejects_value_above_rlimit() {
        // If the platform reports a finite rlimit, a value exceeding it (minus
        // slop) must be rejected by the check hook.
        let rlimit = get_stack_depth_rlimit();
        if rlimit > 0 {
            // kB just past the allowed ceiling.
            let mut too_big: c_int =
                (((rlimit - STACK_DEPTH_SLOP) / 1024) as c_int).saturating_add(1024);
            let ok = check_max_stack_depth(&mut too_big, null_mut(), 0);
            assert!(!ok);

            // A clearly safe small value passes.
            let mut small: c_int = 1;
            assert!(check_max_stack_depth(&mut small, null_mut(), 0));
        }
    }
}

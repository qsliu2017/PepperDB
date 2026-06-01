//! Translation of postgres/src/backend/backup/basebackup_target.c
//!                + postgres/src/include/backup/basebackup_target.h (MERGED)
//!
//! Base backups can be "targeted", which means that they can be sent
//! somewhere other than to the client which requested the backup.
//! Furthermore, new targets can be defined by extensions. This file
//! contains code to support that functionality.
//!
//! Portions Copyright (c) 2010-2025, PostgreSQL Global Development Group
//!
//! ---------------------------------------------------------------------------
//! Translation notes:
//!
//! * The C source keeps the registry in a `static List *BaseBackupTargetTypeList`
//!   allocated in TopMemoryContext.  We mirror that with a `static mut` raw List
//!   pointer (NIL until first use) and `lappend` from crate::nodes::pg_list, with
//!   MemoryContextSwitchTo(TopMemoryContext) around allocations exactly as in C.
//!
//! * `bbsink` is the sink type from the sibling basebackup_sink unit
//!   (crate::backup::basebackup_sink::bbsink).  That unit is not yet ported, so
//!   here `bbsink` is an opaque alias for `c_void` and `bbsink_server_new` is a
//!   STUB (unimplemented!()).  TODO: switch to the real bbsink type and the real
//!   bbsink_server_new once basebackup_sink.rs lands.
//!
//! * The builtin target table `builtin_backup_targets[]` is a C static array with
//!   a NULL-name sentinel; we reproduce it as a Rust slice of BaseBackupTargetType
//!   values (no sentinel needed - the slice carries its length) and append each
//!   into the registry list in initialize_target_list().
//!
//! * errcode(...) is the prelude shim (ignores its arg); ereport!(ERROR, ...)
//!   PANICs, so the "keep compiler quiet"/return-NULL tails after an unconditional
//!   ereport are dummy values after an unreachable path.

use crate::prelude::*;
use crate::nodes::pg_list::{lappend, lfirst, List, NIL};
use crate::{current_cell, foreach};

/// bbsink - the base backup sink type.
///
/// STUB: the real type lives in the not-yet-ported basebackup_sink unit
/// (crate::backup::basebackup_sink::bbsink).  Treated as opaque here.
/// TODO: replace with `crate::backup::basebackup_sink::bbsink`.
pub type bbsink = c_void;

/// C: `static bbsink *bbsink_server_new(bbsink *next_sink, void *detail_arg)`
/// from basebackup_server.c.  STUB - not yet ported.
/// TODO: wire to the real bbsink_server_new once basebackup_server.rs lands.
unsafe fn bbsink_server_new(_next_sink: *mut bbsink, _detail_arg: *mut c_void) -> *mut bbsink {
    unimplemented!("bbsink_server_new: basebackup_server.c not yet ported");
}

/// C function-pointer typedefs used by the registry entries.
pub type CheckDetailFn = unsafe extern "C" fn(*mut c_char, *mut c_char) -> *mut c_void;
pub type GetSinkFn = unsafe extern "C" fn(*mut bbsink, *mut c_void) -> *mut bbsink;

/// C:
/// ```c
/// typedef struct BaseBackupTargetType {
///     char *name;
///     void *(*check_detail)(char *, char *);
///     bbsink *(*get_sink)(bbsink *, void *);
/// } BaseBackupTargetType;
/// ```
#[repr(C)]
pub struct BaseBackupTargetType {
    pub name: *mut c_char,
    pub check_detail: Option<CheckDetailFn>,
    pub get_sink: Option<GetSinkFn>,
}

/// C:
/// ```c
/// struct BaseBackupTargetHandle {
///     BaseBackupTargetType *type;
///     void *detail_arg;
/// };
/// ```
#[repr(C)]
pub struct BaseBackupTargetHandle {
    pub r#type: *mut BaseBackupTargetType,
    pub detail_arg: *mut c_void,
}

/// C: `static List *BaseBackupTargetTypeList = NIL;`
static mut BaseBackupTargetTypeList: *mut List = NIL;

/// C: `static BaseBackupTargetType builtin_backup_targets[]`
/// The C array ends with a `{ NULL }` sentinel; the Rust slice's length replaces
/// the sentinel.  Helper that yields the builtin entries (freshly heap-name'd via
/// pstrdup happens at registration; here the names are static C string literals).
unsafe fn builtin_backup_targets() -> [BaseBackupTargetType; 2] {
    [
        BaseBackupTargetType {
            name: c"blackhole".as_ptr() as *mut c_char,
            check_detail: Some(reject_target_detail),
            get_sink: Some(blackhole_get_sink),
        },
        BaseBackupTargetType {
            name: c"server".as_ptr() as *mut c_char,
            check_detail: Some(server_check_detail),
            get_sink: Some(server_get_sink),
        },
    ]
}

/// strcmp helper over two NUL-terminated C strings: true if equal.
unsafe fn c_str_eq(a: *const c_char, b: *const c_char) -> bool {
    if a.is_null() || b.is_null() {
        return a == b;
    }
    let mut i = 0isize;
    loop {
        let ca = *a.offset(i);
        let cb = *b.offset(i);
        if ca != cb {
            return false;
        }
        if ca == 0 {
            return true;
        }
        i += 1;
    }
}

/// Add a new base backup target type.
///
/// This is intended for use by server extensions.
///
/// C: `void BaseBackupAddTarget(char *name, void *(*check_detail)(char *, char *),
///                              bbsink *(*get_sink)(bbsink *, void *))`
pub unsafe fn BaseBackupAddTarget(
    name: *mut c_char,
    check_detail: Option<CheckDetailFn>,
    get_sink: Option<GetSinkFn>,
) {
    // If the target list is not yet initialized, do that first.
    if BaseBackupTargetTypeList == NIL {
        initialize_target_list();
    }

    // Search the target type list for an existing entry with this name.
    foreach!(lc, BaseBackupTargetTypeList, {
        let ttype = lfirst(current_cell!(lc)) as *mut BaseBackupTargetType;

        if c_str_eq((*ttype).name, name) {
            // We found one, so update it.
            //
            // It is probably not a great idea to call BaseBackupAddTarget for
            // the same name multiple times, but if it happens, this seems like
            // the sanest behavior.
            (*ttype).check_detail = check_detail;
            (*ttype).get_sink = get_sink;
            return;
        }
    });

    // We use TopMemoryContext for allocations here to make sure that the data
    // we need doesn't vanish under us; that's also why we copy the target name
    // into a newly-allocated chunk of memory.
    let oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    let newtype = palloc(core::mem::size_of::<BaseBackupTargetType>()) as *mut BaseBackupTargetType;
    (*newtype).name = pstrdup(name);
    (*newtype).check_detail = check_detail;
    (*newtype).get_sink = get_sink;
    BaseBackupTargetTypeList = lappend(BaseBackupTargetTypeList, newtype as *mut c_void);
    MemoryContextSwitchTo(oldcontext);
}

/// Look up a base backup target and validate the target_detail.
///
/// Extensions that define new backup targets will probably define a new type of
/// bbsink to match. Validation of the target_detail can be performed either in
/// the check_detail routine called here, or in the bbsink constructor, which
/// will be called from BaseBackupGetSink. It's mostly a matter of taste, but the
/// check_detail function runs somewhat earlier.
///
/// C: `BaseBackupTargetHandle *BaseBackupGetTargetHandle(char *target,
///                                                        char *target_detail)`
pub unsafe fn BaseBackupGetTargetHandle(
    target: *mut c_char,
    target_detail: *mut c_char,
) -> *mut BaseBackupTargetHandle {
    // If the target list is not yet initialized, do that first.
    if BaseBackupTargetTypeList == NIL {
        initialize_target_list();
    }

    // Search the target type list for a match.
    foreach!(lc, BaseBackupTargetTypeList, {
        let ttype = lfirst(current_cell!(lc)) as *mut BaseBackupTargetType;

        if c_str_eq((*ttype).name, target) {
            // Found the target.
            let handle =
                palloc(core::mem::size_of::<BaseBackupTargetHandle>()) as *mut BaseBackupTargetHandle;
            (*handle).r#type = ttype;
            (*handle).detail_arg = ((*ttype).check_detail.unwrap())(target, target_detail);
            return handle;
        }
    });

    // Did not find the target.
    ereport!(
        ERROR,
        errmsg!("unrecognized target: \"{}\"", c_str_display(target))
    );

    // keep compiler quiet (ereport!(ERROR) PANICs, so this is unreachable).
    #[allow(unreachable_code)]
    null_mut()
}

/// Construct a bbsink that will implement the backup target.
///
/// The get_sink function does all the real work, so all we have to do here is
/// call it with the correct arguments. Whatever the check_detail function
/// returned is here passed through to the get_sink function.
///
/// C: `bbsink *BaseBackupGetSink(BaseBackupTargetHandle *handle, bbsink *next_sink)`
pub unsafe fn BaseBackupGetSink(
    handle: *mut BaseBackupTargetHandle,
    next_sink: *mut bbsink,
) -> *mut bbsink {
    ((*(*handle).r#type).get_sink.unwrap())(next_sink, (*handle).detail_arg)
}

/// Load predefined target types into BaseBackupTargetTypeList.
///
/// C: `static void initialize_target_list(void)`
unsafe fn initialize_target_list() {
    let oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    // The C code appends pointers to the static `builtin_backup_targets` array.
    // We heap-allocate (in TopMemoryContext) one copy of each builtin entry and
    // append the pointers, which preserves the same lifetime guarantees.
    for ttype in builtin_backup_targets() {
        let slot = palloc(core::mem::size_of::<BaseBackupTargetType>()) as *mut BaseBackupTargetType;
        (*slot).name = ttype.name;
        (*slot).check_detail = ttype.check_detail;
        (*slot).get_sink = ttype.get_sink;
        BaseBackupTargetTypeList = lappend(BaseBackupTargetTypeList, slot as *mut c_void);
    }
    MemoryContextSwitchTo(oldcontext);
}

/// Normally, a get_sink function should construct and return a new bbsink that
/// implements the backup target, but the 'blackhole' target just throws the data
/// away. It's cheapest to implement that by not adding a bbsink at all.
///
/// C: `static bbsink *blackhole_get_sink(bbsink *next_sink, void *detail_arg)`
unsafe extern "C" fn blackhole_get_sink(next_sink: *mut bbsink, _detail_arg: *mut c_void) -> *mut bbsink {
    next_sink
}

/// Create a bbsink implementing a server-side backup.
///
/// C: `static bbsink *server_get_sink(bbsink *next_sink, void *detail_arg)`
unsafe extern "C" fn server_get_sink(next_sink: *mut bbsink, detail_arg: *mut c_void) -> *mut bbsink {
    bbsink_server_new(next_sink, detail_arg)
}

/// Implement target-detail checking for a target that does not accept a detail.
///
/// C: `static void *reject_target_detail(char *target, char *target_detail)`
unsafe extern "C" fn reject_target_detail(target: *mut c_char, target_detail: *mut c_char) -> *mut c_void {
    if !target_detail.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "target \"{}\" does not accept a target detail",
                c_str_display(target)
            )
        );
    }

    null_mut()
}

/// Implement target-detail checking for a server-side backup.
///
/// target_detail should be the name of the directory to which the backup should
/// be written, but we don't check that here. Rather, that check, as well as the
/// necessary permissions checking, happens in bbsink_server_new.
///
/// C: `static void *server_check_detail(char *target, char *target_detail)`
unsafe extern "C" fn server_check_detail(target: *mut c_char, target_detail: *mut c_char) -> *mut c_void {
    if target_detail.is_null() {
        ereport!(
            ERROR,
            errmsg!(
                "target \"{}\" requires a target detail",
                c_str_display(target)
            )
        );
    }

    target_detail as *mut c_void
}

/// SQLSTATE shims.  The prelude `errcode()` ignores its argument, so the exact
/// numeric value is irrelevant to control flow; these mirror the C macro names
/// used in the ereport calls above.
/// TODO: replace with the real values from utils/errcodes once ported.
#[allow(non_upper_case_globals)]
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;
#[allow(non_upper_case_globals)]
const ERRCODE_SYNTAX_ERROR: c_int = 0;

/// Lossy helper to render a C string for errmsg! formatting (diagnostics only).
unsafe fn c_str_display(s: *const c_char) -> String {
    if s.is_null() {
        return String::from("(null)");
    }
    let mut bytes = Vec::new();
    let mut i = 0isize;
    loop {
        let ch = *s.offset(i);
        if ch == 0 {
            break;
        }
        bytes.push(ch as u8);
        i += 1;
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    // A trivial check_detail that just echoes the detail back as the detail_arg.
    unsafe extern "C" fn test_check_detail(
        _target: *mut c_char,
        target_detail: *mut c_char,
    ) -> *mut c_void {
        target_detail as *mut c_void
    }

    // A get_sink that forwards (so it does not touch the unported bbsink_server_new).
    unsafe extern "C" fn test_get_sink(next_sink: *mut bbsink, _detail_arg: *mut c_void) -> *mut bbsink {
        next_sink
    }

    // AddTarget then GetTargetHandle by name finds it.
    #[test]
    fn add_then_get_finds_target() {
        unsafe {
            let name = c"mytarget_addfind".as_ptr() as *mut c_char;
            BaseBackupAddTarget(name, Some(test_check_detail), Some(test_get_sink));

            let detail = c"some-detail".as_ptr() as *mut c_char;
            let handle = BaseBackupGetTargetHandle(name, detail);
            assert!(!handle.is_null());
            // check_detail echoed the detail through as detail_arg.
            assert_eq!((*handle).detail_arg, detail as *mut c_void);
            // The looked-up type's name matches.
            assert!(c_str_eq((*(*handle).r#type).name, name));

            // GetSink invokes the (forwarding) get_sink fn ptr.
            let sentinel = 0x1234usize as *mut bbsink;
            let sink = BaseBackupGetSink(handle, sentinel);
            assert_eq!(sink, sentinel);
        }
    }

    // Builtin "blackhole" target is registered and rejects a target detail.
    #[test]
    fn builtin_blackhole_present() {
        unsafe {
            let name = c"blackhole".as_ptr() as *mut c_char;
            let handle = BaseBackupGetTargetHandle(name, null_mut());
            assert!(!handle.is_null());
            assert!(c_str_eq((*(*handle).r#type).name, name));
        }
    }

    // Unknown target name ereports (PANICs).
    #[test]
    #[should_panic]
    fn unknown_target_ereports() {
        unsafe {
            let name = c"no_such_target_xyz".as_ptr() as *mut c_char;
            let _ = BaseBackupGetTargetHandle(name, null_mut());
        }
    }
}

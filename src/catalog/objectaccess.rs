//! Object access hooks: functions for object_access_hook on various events.
//!
//! Source: postgres/src/backend/catalog/objectaccess.c
//! Merged header: postgres/src/include/catalog/objectaccess.h
//! (#include "catalog/pg_class.h", "catalog/pg_namespace.h",
//!  "catalog/pg_proc.h" supplied here as RelationRelationId /
//!  NamespaceRelationId / ProcedureRelationId from catalog_oids).
//!
//! Object access hooks are intended to be called just before or just after
//! performing certain actions on a SQL object.  This is intended as
//! infrastructure for security or logging plugins (e.g. sepgsql).
//!
//! The inline Invoke* wrappers from objectaccess.h are not emitted here; only
//! the Run* entrypoints they call are translated.  Callers should perform the
//! `if (object_access_hook)` null check themselves before calling Run*, exactly
//! as the C macros do.

use crate::prelude::*;
use crate::postgres_ext::Oid;

use crate::catalog::catalog_oids::{
    NamespaceRelationId, ProcedureRelationId, RelationRelationId,
};

// ----------------------------------------------------------------------------
// objectaddress.h is NOT ported yet.  Define a minimal local ObjectAddress so
// that this unit is self-contained.
// TODO: replace with the real catalog/objectaddress.h translation when ready.
// ----------------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAddress {
    pub classId: Oid,    // class Id from pg_class
    pub objectId: Oid,   // OID of the object
    pub objectSubId: int32, // Subitem within object (eg column), or 0
}

/*
 * Object access hooks are intended to be called just before or just after
 * performing certain actions on a SQL object.  This is intended as
 * infrastructure for security or logging plugins.
 *
 * OAT_POST_CREATE should be invoked just after the object is created.
 * OAT_DROP should be invoked just before deletion of objects.
 * OAT_POST_ALTER should be invoked just after the object is altered, but
 *   before the command counter is incremented.
 * OAT_NAMESPACE_SEARCH should be invoked prior to object name lookup under a
 *   particular namespace.
 * OAT_FUNCTION_EXECUTE should be invoked prior to function execution.
 * OAT_TRUNCATE should be invoked just before truncation of objects.
 *
 * Other types may be added in the future.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ObjectAccessType {
    OAT_POST_CREATE,
    OAT_DROP,
    OAT_POST_ALTER,
    OAT_NAMESPACE_SEARCH,
    OAT_FUNCTION_EXECUTE,
    OAT_TRUNCATE,
}
pub use ObjectAccessType::*;

/*
 * Arguments of OAT_POST_CREATE event
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAccessPostCreate {
    /*
     * This flag informs extensions whether the context of this creation is
     * invoked by user's operations, or not. E.g, it shall be dealt as
     * internal stuff on toast tables or indexes due to type changes.
     */
    pub is_internal: bool,
}

/*
 * Arguments of OAT_DROP event
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAccessDrop {
    /*
     * Flags to inform extensions the context of this deletion. Also see
     * PERFORM_DELETION_* in dependency.h
     */
    pub dropflags: c_int,
}

/*
 * Arguments of OAT_POST_ALTER event
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAccessPostAlter {
    /*
     * This identifier is used when system catalog takes two IDs to identify a
     * particular tuple of the catalog. It is only used when the caller want
     * to identify an entry of pg_inherits, pg_db_role_setting or
     * pg_user_mapping. Elsewhere, InvalidOid should be set.
     */
    pub auxiliary_id: Oid,

    /*
     * If this flag is set, the user hasn't requested that the object be
     * altered, but we're doing it anyway for some internal reason.
     */
    pub is_internal: bool,
}

/*
 * Arguments of OAT_NAMESPACE_SEARCH
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAccessNamespaceSearch {
    /*
     * If true, hook should report an error when permission to search this
     * schema is denied.
     */
    pub ereport_on_violation: bool,

    /*
     * This is, in essence, an out parameter.  Core code should initialize
     * this to true, and any extension that wants to deny access should reset
     * it to false.
     */
    pub result: bool,
}

/* Plugin provides a hook function matching one or both of these signatures. */
pub type object_access_hook_type = unsafe extern "C" fn(
    access: ObjectAccessType,
    classId: Oid,
    objectId: Oid,
    subId: c_int,
    arg: *mut c_void,
);

pub type object_access_hook_type_str = unsafe extern "C" fn(
    access: ObjectAccessType,
    classId: Oid,
    objectStr: *const c_char,
    subId: c_int,
    arg: *mut c_void,
);

/*
 * Hook on object accesses.  This is intended as infrastructure for security
 * and logging plugins.  Plugin sets these variables to a suitable hook
 * function.
 */
#[no_mangle]
pub static mut object_access_hook: Option<object_access_hook_type> = None;
pub static mut object_access_hook_str: Option<object_access_hook_type_str> = None;

/*
 * RunObjectPostCreateHook
 *
 * OAT_POST_CREATE object ID based event hook entrypoint
 */
pub unsafe fn RunObjectPostCreateHook(
    classId: Oid,
    objectId: Oid,
    subId: c_int,
    is_internal: bool,
) {
    /* caller should check, but just in case... */
    Assert!(object_access_hook.is_some());

    let pc_arg = ObjectAccessPostCreate { is_internal };

    (object_access_hook.unwrap())(
        OAT_POST_CREATE,
        classId,
        objectId,
        subId,
        &pc_arg as *const _ as *mut c_void,
    );
}

/*
 * RunObjectDropHook
 *
 * OAT_DROP object ID based event hook entrypoint
 */
pub unsafe fn RunObjectDropHook(classId: Oid, objectId: Oid, subId: c_int, dropflags: c_int) {
    /* caller should check, but just in case... */
    Assert!(object_access_hook.is_some());

    let drop_arg = ObjectAccessDrop { dropflags };

    (object_access_hook.unwrap())(
        OAT_DROP,
        classId,
        objectId,
        subId,
        &drop_arg as *const _ as *mut c_void,
    );
}

/*
 * RunObjectTruncateHook
 *
 * OAT_TRUNCATE object ID based event hook entrypoint
 */
pub unsafe fn RunObjectTruncateHook(objectId: Oid) {
    /* caller should check, but just in case... */
    Assert!(object_access_hook.is_some());

    (object_access_hook.unwrap())(OAT_TRUNCATE, RelationRelationId, objectId, 0, null_mut());
}

/*
 * RunObjectPostAlterHook
 *
 * OAT_POST_ALTER object ID based event hook entrypoint
 */
pub unsafe fn RunObjectPostAlterHook(
    classId: Oid,
    objectId: Oid,
    subId: c_int,
    auxiliaryId: Oid,
    is_internal: bool,
) {
    /* caller should check, but just in case... */
    Assert!(object_access_hook.is_some());

    let pa_arg = ObjectAccessPostAlter {
        auxiliary_id: auxiliaryId,
        is_internal,
    };

    (object_access_hook.unwrap())(
        OAT_POST_ALTER,
        classId,
        objectId,
        subId,
        &pa_arg as *const _ as *mut c_void,
    );
}

/*
 * RunNamespaceSearchHook
 *
 * OAT_NAMESPACE_SEARCH object ID based event hook entrypoint
 */
pub unsafe fn RunNamespaceSearchHook(objectId: Oid, ereport_on_violation: bool) -> bool {
    /* caller should check, but just in case... */
    Assert!(object_access_hook.is_some());

    let mut ns_arg = ObjectAccessNamespaceSearch {
        ereport_on_violation,
        result: true,
    };

    (object_access_hook.unwrap())(
        OAT_NAMESPACE_SEARCH,
        NamespaceRelationId,
        objectId,
        0,
        &mut ns_arg as *mut _ as *mut c_void,
    );

    ns_arg.result
}

/*
 * RunFunctionExecuteHook
 *
 * OAT_FUNCTION_EXECUTE object ID based event hook entrypoint
 */
pub unsafe fn RunFunctionExecuteHook(objectId: Oid) {
    /* caller should check, but just in case... */
    Assert!(object_access_hook.is_some());

    (object_access_hook.unwrap())(
        OAT_FUNCTION_EXECUTE,
        ProcedureRelationId,
        objectId,
        0,
        null_mut(),
    );
}

/* String versions */

/*
 * RunObjectPostCreateHookStr
 *
 * OAT_POST_CREATE object name based event hook entrypoint
 */
pub unsafe fn RunObjectPostCreateHookStr(
    classId: Oid,
    objectName: *const c_char,
    subId: c_int,
    is_internal: bool,
) {
    /* caller should check, but just in case... */
    Assert!(object_access_hook_str.is_some());

    let pc_arg = ObjectAccessPostCreate { is_internal };

    (object_access_hook_str.unwrap())(
        OAT_POST_CREATE,
        classId,
        objectName,
        subId,
        &pc_arg as *const _ as *mut c_void,
    );
}

/*
 * RunObjectDropHookStr
 *
 * OAT_DROP object name based event hook entrypoint
 */
pub unsafe fn RunObjectDropHookStr(
    classId: Oid,
    objectName: *const c_char,
    subId: c_int,
    dropflags: c_int,
) {
    /* caller should check, but just in case... */
    Assert!(object_access_hook_str.is_some());

    let drop_arg = ObjectAccessDrop { dropflags };

    (object_access_hook_str.unwrap())(
        OAT_DROP,
        classId,
        objectName,
        subId,
        &drop_arg as *const _ as *mut c_void,
    );
}

/*
 * RunObjectTruncateHookStr
 *
 * OAT_TRUNCATE object name based event hook entrypoint
 */
pub unsafe fn RunObjectTruncateHookStr(objectName: *const c_char) {
    /* caller should check, but just in case... */
    Assert!(object_access_hook_str.is_some());

    (object_access_hook_str.unwrap())(
        OAT_TRUNCATE,
        RelationRelationId,
        objectName,
        0,
        null_mut(),
    );
}

/*
 * RunObjectPostAlterHookStr
 *
 * OAT_POST_ALTER object name based event hook entrypoint
 */
pub unsafe fn RunObjectPostAlterHookStr(
    classId: Oid,
    objectName: *const c_char,
    subId: c_int,
    auxiliaryId: Oid,
    is_internal: bool,
) {
    /* caller should check, but just in case... */
    Assert!(object_access_hook_str.is_some());

    let pa_arg = ObjectAccessPostAlter {
        auxiliary_id: auxiliaryId,
        is_internal,
    };

    (object_access_hook_str.unwrap())(
        OAT_POST_ALTER,
        classId,
        objectName,
        subId,
        &pa_arg as *const _ as *mut c_void,
    );
}

/*
 * RunNamespaceSearchHookStr
 *
 * OAT_NAMESPACE_SEARCH object name based event hook entrypoint
 */
pub unsafe fn RunNamespaceSearchHookStr(
    objectName: *const c_char,
    ereport_on_violation: bool,
) -> bool {
    /* caller should check, but just in case... */
    Assert!(object_access_hook_str.is_some());

    let mut ns_arg = ObjectAccessNamespaceSearch {
        ereport_on_violation,
        result: true,
    };

    (object_access_hook_str.unwrap())(
        OAT_NAMESPACE_SEARCH,
        NamespaceRelationId,
        objectName,
        0,
        &mut ns_arg as *mut _ as *mut c_void,
    );

    ns_arg.result
}

/*
 * RunFunctionExecuteHookStr
 *
 * OAT_FUNCTION_EXECUTE object name based event hook entrypoint
 */
pub unsafe fn RunFunctionExecuteHookStr(objectName: *const c_char) {
    /* caller should check, but just in case... */
    Assert!(object_access_hook_str.is_some());

    (object_access_hook_str.unwrap())(
        OAT_FUNCTION_EXECUTE,
        ProcedureRelationId,
        objectName,
        0,
        null_mut(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::{AtomicU32, AtomicU8, Ordering};
    use std::sync::Mutex;

    // The hook is a process-global `static mut`; serialize the tests that mutate
    // it (cargo runs tests in parallel threads) so they don't race / pollute the
    // shared recording counters below.
    static HOOK_LOCK: Mutex<()> = Mutex::new(());

    // Records the last call's access type (as discriminant+1; 0 == "never
    // fired") and the classId, so the test thread can observe the hook firing.
    static LAST_ACCESS: AtomicU8 = AtomicU8::new(0);
    static LAST_CLASS: AtomicU32 = AtomicU32::new(0);

    unsafe extern "C" fn recording_hook(
        access: ObjectAccessType,
        classId: Oid,
        _objectId: Oid,
        _subId: c_int,
        _arg: *mut c_void,
    ) {
        LAST_ACCESS.store(access as u8 + 1, Ordering::SeqCst);
        LAST_CLASS.store(classId, Ordering::SeqCst);
    }

    // With a null hook installed, callers gate on object_access_hook.is_some();
    // emulate that here and confirm we do not fire / panic.
    #[test]
    fn null_hook_is_noop() {
        let _g = HOOK_LOCK.lock().unwrap();
        LAST_ACCESS.store(0, Ordering::SeqCst);
        unsafe {
            object_access_hook = None;
            // Caller-style guard: nothing runs when the hook is absent.
            if object_access_hook.is_some() {
                RunObjectPostCreateHook(RelationRelationId, 16384, 0, false);
            }
        }
        // No panic, no recorded call from this branch.
        assert_eq!(LAST_ACCESS.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn post_create_hook_fires() {
        let _g = HOOK_LOCK.lock().unwrap();
        unsafe {
            LAST_ACCESS.store(0, Ordering::SeqCst);
            LAST_CLASS.store(0, Ordering::SeqCst);
            object_access_hook = Some(recording_hook);

            RunObjectPostCreateHook(RelationRelationId, 16384, 0, true);

            object_access_hook = None;
        }
        assert_eq!(
            LAST_ACCESS.load(Ordering::SeqCst),
            OAT_POST_CREATE as u8 + 1
        );
        assert_eq!(LAST_CLASS.load(Ordering::SeqCst), RelationRelationId);
    }

    #[test]
    fn namespace_search_returns_result() {
        let _g = HOOK_LOCK.lock().unwrap();
        unsafe {
            object_access_hook = Some(recording_hook);
            // recording_hook leaves ns_arg.result at its initial true.
            let allowed = RunNamespaceSearchHook(2200, true);
            object_access_hook = None;
            assert!(allowed);
        }
    }
}

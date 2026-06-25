//! Translated from PostgreSQL src/include/catalog/objectaccess.h
//! Object access hooks for security / logging plugins.

use crate::postgres_ext::Oid;

/// Type of object access event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectAccessType {
    POST_CREATE,
    DROP,
    POST_ALTER,
    NAMESPACE_SEARCH,
    FUNCTION_EXECUTE,
    TRUNCATE,
}

/// Arguments of POST_CREATE event.
pub struct ObjectAccessPostCreate {
    pub is_internal: bool, // creation invoked by internal stuff vs. user op
}

/// Arguments of DROP event.
pub struct ObjectAccessDrop {
    pub dropflags: i32, // see PERFORM_DELETION_* in dependency.h
}

/// Arguments of POST_ALTER event.
pub struct ObjectAccessPostAlter {
    pub auxiliary_id: Oid, // second ID for catalogs keyed by two IDs, else InvalidOid
    pub is_internal: bool, // internal alter vs. user-requested
}

/// Arguments of NAMESPACE_SEARCH event.
pub struct ObjectAccessNamespaceSearch {
    pub ereport_on_violation: bool, // report an error when access denied
    pub result: bool,               // in/out: false denies access
}

/// C: `typedef void (*object_access_hook_type) (..., int subId, void *arg);`
/// The opaque `arg` is dropped; plugins carry state via the function itself.
pub type object_access_hook_type =
    fn(access: ObjectAccessType, class_id: Oid, object_id: Oid, sub_id: i32);

/// String variant taking an objectStr instead of an objectId.
pub type object_access_hook_type_str =
    fn(access: ObjectAccessType, class_id: Oid, object_str: &str, sub_id: i32);

// Plugin sets these globals to a suitable hook function (None when unset).
pub static mut object_access_hook: Option<object_access_hook_type> = None;
pub static mut object_access_hook_str: Option<object_access_hook_type_str> = None;

// Core code uses these to call the hook.
pub fn RunObjectPostCreateHook(_class_id: Oid, _object_id: Oid, _sub_id: i32, _is_internal: bool) {
    unimplemented!()
}
pub fn RunObjectDropHook(_class_id: Oid, _object_id: Oid, _sub_id: i32, _dropflags: i32) {
    unimplemented!()
}
pub fn RunObjectTruncateHook(_object_id: Oid) {
    unimplemented!()
}
pub fn RunObjectPostAlterHook(_class_id: Oid, _object_id: Oid, _sub_id: i32, _auxiliary_id: Oid, _is_internal: bool) {
    unimplemented!()
}
pub fn RunNamespaceSearchHook(_object_id: Oid, _ereport_on_violation: bool) -> bool {
    unimplemented!()
}
pub fn RunFunctionExecuteHook(_object_id: Oid) {
    unimplemented!()
}

// String versions.
pub fn RunObjectPostCreateHookStr(_class_id: Oid, _object_name: &str, _sub_id: i32, _is_internal: bool) {
    unimplemented!()
}
pub fn RunObjectDropHookStr(_class_id: Oid, _object_name: &str, _sub_id: i32, _dropflags: i32) {
    unimplemented!()
}
pub fn RunObjectTruncateHookStr(_object_name: &str) {
    unimplemented!()
}
pub fn RunObjectPostAlterHookStr(_class_id: Oid, _object_name: &str, _sub_id: i32, _auxiliary_id: Oid, _is_internal: bool) {
    unimplemented!()
}
pub fn RunNamespaceSearchHookStr(_object_name: &str, _ereport_on_violation: bool) -> bool {
    unimplemented!()
}
pub fn RunFunctionExecuteHookStr(_object_name: &str) {
    unimplemented!()
}

// Invoke* wrappers: the C macros guard the Run* call on the hook being set.
pub fn InvokeObjectPostCreateHook(class_id: Oid, object_id: Oid, sub_id: i32) {
    InvokeObjectPostCreateHookArg(class_id, object_id, sub_id, false)
}
pub fn InvokeObjectPostCreateHookArg(class_id: Oid, object_id: Oid, sub_id: i32, is_internal: bool) {
    if unsafe { object_access_hook }.is_some() {
        RunObjectPostCreateHook(class_id, object_id, sub_id, is_internal);
    }
}
pub fn InvokeObjectDropHook(class_id: Oid, object_id: Oid, sub_id: i32) {
    InvokeObjectDropHookArg(class_id, object_id, sub_id, 0)
}
pub fn InvokeObjectDropHookArg(class_id: Oid, object_id: Oid, sub_id: i32, dropflags: i32) {
    if unsafe { object_access_hook }.is_some() {
        RunObjectDropHook(class_id, object_id, sub_id, dropflags);
    }
}
pub fn InvokeObjectTruncateHook(object_id: Oid) {
    if unsafe { object_access_hook }.is_some() {
        RunObjectTruncateHook(object_id);
    }
}
pub fn InvokeObjectPostAlterHook(class_id: Oid, object_id: Oid, sub_id: i32) {
    InvokeObjectPostAlterHookArg(class_id, object_id, sub_id, crate::postgres_ext::InvalidOid, false)
}
pub fn InvokeObjectPostAlterHookArg(class_id: Oid, object_id: Oid, sub_id: i32, auxiliary_id: Oid, is_internal: bool) {
    if unsafe { object_access_hook }.is_some() {
        RunObjectPostAlterHook(class_id, object_id, sub_id, auxiliary_id, is_internal);
    }
}
pub fn InvokeNamespaceSearchHook(object_id: Oid, ereport_on_violation: bool) -> bool {
    if unsafe { object_access_hook }.is_none() {
        true
    } else {
        RunNamespaceSearchHook(object_id, ereport_on_violation)
    }
}
pub fn InvokeFunctionExecuteHook(object_id: Oid) {
    if unsafe { object_access_hook }.is_some() {
        RunFunctionExecuteHook(object_id);
    }
}

// String Invoke* wrappers.
pub fn InvokeObjectPostCreateHookStr(class_id: Oid, object_name: &str, sub_id: i32) {
    InvokeObjectPostCreateHookArgStr(class_id, object_name, sub_id, false)
}
pub fn InvokeObjectPostCreateHookArgStr(class_id: Oid, object_name: &str, sub_id: i32, is_internal: bool) {
    if unsafe { object_access_hook_str }.is_some() {
        RunObjectPostCreateHookStr(class_id, object_name, sub_id, is_internal);
    }
}
pub fn InvokeObjectDropHookStr(class_id: Oid, object_name: &str, sub_id: i32) {
    InvokeObjectDropHookArgStr(class_id, object_name, sub_id, 0)
}
pub fn InvokeObjectDropHookArgStr(class_id: Oid, object_name: &str, sub_id: i32, dropflags: i32) {
    if unsafe { object_access_hook_str }.is_some() {
        RunObjectDropHookStr(class_id, object_name, sub_id, dropflags);
    }
}
pub fn InvokeObjectTruncateHookStr(object_name: &str) {
    if unsafe { object_access_hook_str }.is_some() {
        RunObjectTruncateHookStr(object_name);
    }
}
pub fn InvokeObjectPostAlterHookStr(class_id: Oid, object_name: &str, sub_id: i32) {
    InvokeObjectPostAlterHookArgStr(class_id, object_name, sub_id, crate::postgres_ext::InvalidOid, false)
}
pub fn InvokeObjectPostAlterHookArgStr(class_id: Oid, object_name: &str, sub_id: i32, auxiliary_id: Oid, is_internal: bool) {
    if unsafe { object_access_hook_str }.is_some() {
        RunObjectPostAlterHookStr(class_id, object_name, sub_id, auxiliary_id, is_internal);
    }
}
pub fn InvokeNamespaceSearchHookStr(object_name: &str, ereport_on_violation: bool) -> bool {
    if unsafe { object_access_hook_str }.is_none() {
        true
    } else {
        RunNamespaceSearchHookStr(object_name, ereport_on_violation)
    }
}
pub fn InvokeFunctionExecuteHookStr(object_name: &str) {
    if unsafe { object_access_hook_str }.is_some() {
        RunFunctionExecuteHookStr(object_name);
    }
}

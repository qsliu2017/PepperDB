//! usercontext.c - Convenience functions for running code as a different database user.

use crate::prelude::*;

use crate::miscadmin::{
    GetUserIdAndSecContext, GetUserNameFromId, SetUserIdAndSecContext,
    SECURITY_RESTRICTED_OPERATION,
};

/*
 * When temporarily changing to run as a different user, this structure
 * holds the details needed to restore the original state.
 */
#[repr(C)]
pub struct UserContext {
    pub save_userid: Oid,
    pub save_sec_context: c_int,
    pub save_nestlevel: c_int,
}

/* ERRCODE not yet ported; local stub matching catalog.rs convention. */
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;

/* --- Locally stubbed (not yet ported) callees. --- */

// TODO: port acl.c member_can_set_role
unsafe fn member_can_set_role(_member: Oid, _role: Oid) -> bool { crate::utils::adt::acl::member_can_set_role(_member, _role) }

// TODO: port guc.c NewGUCNestLevel
unsafe fn NewGUCNestLevel() -> c_int {
    unimplemented!()
}

// TODO: port guc.c AtEOXact_GUC
unsafe fn AtEOXact_GUC(_isCommit: bool, _nestLevel: c_int) {
    unimplemented!()
}

/*
 * Temporarily switch to a new user ID.
 *
 * If the current user doesn't have permission to SET ROLE to the new user,
 * an ERROR occurs.
 *
 * If the new user doesn't have permission to SET ROLE to the current user,
 * SECURITY_RESTRICTED_OPERATION is imposed and a new GUC nest level is
 * created so that any settings changes can be rolled back.
 */
#[no_mangle]
pub unsafe fn SwitchToUntrustedUser(userid: Oid, context: *mut UserContext) {
    /* Get the current user ID and security context. */
    GetUserIdAndSecContext(
        &mut (*context).save_userid,
        &mut (*context).save_sec_context,
    );

    /* Check that we have sufficient privileges to assume the target role. */
    if !member_can_set_role((*context).save_userid, userid) {
        let _ = ERRCODE_INSUFFICIENT_PRIVILEGE;
        elog!(
            ERROR,
            "role \"{}\" cannot SET ROLE to \"{}\" (insufficient privilege)",
            CStr_to_str(GetUserNameFromId((*context).save_userid, false)),
            CStr_to_str(GetUserNameFromId(userid, false))
        );
    }

    /*
     * Try to prevent the user to which we're switching from assuming the
     * privileges of the current user, unless they can SET ROLE to that user
     * anyway.
     */
    if member_can_set_role(userid, (*context).save_userid) {
        /*
         * Each user can SET ROLE to the other, so there's no point in
         * imposing any security restrictions. Just let the user do whatever
         * they want.
         */
        SetUserIdAndSecContext(userid, (*context).save_sec_context);
        (*context).save_nestlevel = -1;
    } else {
        let mut sec_context: c_int = (*context).save_sec_context;

        /*
         * This user can SET ROLE to the target user, but not the other way
         * around, so protect ourselves against the target user by setting
         * SECURITY_RESTRICTED_OPERATION to prevent certain changes to the
         * session state. Also set up a new GUC nest level, so that we can
         * roll back any GUC changes that may be made by code running as the
         * target user, inasmuch as they could be malicious.
         */
        sec_context |= SECURITY_RESTRICTED_OPERATION;
        SetUserIdAndSecContext(userid, sec_context);
        (*context).save_nestlevel = NewGUCNestLevel();
    }
}

/*
 * Switch back to the original user ID.
 *
 * If we created a new GUC nest level, also roll back any changes that were
 * made within it.
 */
#[no_mangle]
pub unsafe fn RestoreUserContext(context: *mut UserContext) {
    if (*context).save_nestlevel != -1 {
        AtEOXact_GUC(false, (*context).save_nestlevel);
    }
    SetUserIdAndSecContext((*context).save_userid, (*context).save_sec_context);
}

/* Helper to render a C string pointer for the error message. */
unsafe fn CStr_to_str<'a>(p: *const c_char) -> &'a str {
    if p.is_null() {
        return "(null)";
    }
    core::ffi::CStr::from_ptr(p).to_str().unwrap_or("(invalid)")
}

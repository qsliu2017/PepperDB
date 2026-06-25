//! Translated from PostgreSQL src/backend/utils/init/usercontext.c
//!
//! Run code temporarily as a different database user. The save/restore mechanics
//! operate over [`crate::session::Session`] (current user id + security
//! context). The permission check (`member_can_set_role`) and GUC nest-level
//! roll-back are catalog/acl/guc concerns and call the existing stubs.

use crate::miscadmin::SecurityContext;
use crate::postgres_ext::Oid;
use crate::session;
use crate::utils::acl::member_can_set_role;
use crate::utils::guc::{AtEOXact_GUC, NewGUCNestLevel};

/// Saved state for restoring the original user after a temporary switch (PG
/// `UserContext`). `save_nestlevel == -1` means no GUC nest level was created.
pub struct UserContext {
    pub save_userid: Oid,
    pub save_sec_context: SecurityContext,
    pub save_nestlevel: i32,
}

/// PG `GetUserIdAndSecContext`: read the current effective user id and the
/// security-restriction flags from the session.
pub fn get_user_id_and_sec_context() -> (Oid, SecurityContext) {
    let s = session::current();
    (
        s.current_user_id(),
        SecurityContext::from_bits_truncate(s.sec_context()),
    )
}

/// PG `SetUserIdAndSecContext`: set the current effective user id and the
/// security-restriction flags on the session.
pub fn set_user_id_and_sec_context(userid: Oid, sec_context: SecurityContext) {
    let s = session::current();
    s.set_current_user_id(userid);
    s.set_sec_context(sec_context.bits());
}

/// PG `InLocalUserIdChange`.
pub fn in_local_user_id_change() -> bool {
    sec_context_has(SecurityContext::LOCAL_USERID_CHANGE)
}

/// PG `InSecurityRestrictedOperation`.
pub fn in_security_restricted_operation() -> bool {
    sec_context_has(SecurityContext::RESTRICTED_OPERATION)
}

/// PG `InNoForceRLSOperation`.
pub fn in_no_force_rls_operation() -> bool {
    sec_context_has(SecurityContext::NOFORCE_RLS)
}

fn sec_context_has(flag: SecurityContext) -> bool {
    SecurityContext::from_bits_truncate(session::current().sec_context()).contains(flag)
}

/// PG `SwitchToUntrustedUser`: temporarily switch to `userid`, returning the
/// context needed to restore the prior state. If the caller cannot SET ROLE to
/// the target an error is raised (via the acl stub). If the target cannot SET
/// ROLE back to the caller, a `SECURITY_RESTRICTED_OPERATION` is imposed and a
/// new GUC nest level is opened so any settings changes can be rolled back.
pub fn switch_to_untrusted_user(userid: Oid) -> UserContext {
    let (save_userid, save_sec_context) = get_user_id_and_sec_context();

    // Permission to assume the target role (catalog/acl stub).
    if !member_can_set_role(save_userid, userid) {
        panic!("role cannot SET ROLE to target role"); // TODO(panic): ereport(ERROR)
    }

    if member_can_set_role(userid, save_userid) {
        // Each can SET ROLE to the other: no restriction needed.
        set_user_id_and_sec_context(userid, save_sec_context);
        UserContext { save_userid, save_sec_context, save_nestlevel: -1 }
    } else {
        // Protect ourselves from the target user: restrict + new GUC nest level.
        let sec_context = save_sec_context | SecurityContext::RESTRICTED_OPERATION;
        set_user_id_and_sec_context(userid, sec_context);
        UserContext {
            save_userid,
            save_sec_context,
            save_nestlevel: NewGUCNestLevel(),
        }
    }
}

/// PG `RestoreUserContext`: switch back to the original user id, rolling back any
/// GUC changes made within the nest level created by the switch.
pub fn restore_user_context(context: &UserContext) {
    if context.save_nestlevel != -1 {
        AtEOXact_GUC(false, context.save_nestlevel);
    }
    set_user_id_and_sec_context(context.save_userid, context.save_sec_context);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::miscadmin::BackendType;
    use crate::session::Session;
    use std::sync::Arc;

    #[tokio::test]
    async fn set_get_user_id_and_sec_context_round_trip() {
        let s = Arc::new(Session::new(BackendType::BACKEND));
        session::scope(s, async {
            set_user_id_and_sec_context(Oid(77), SecurityContext::empty());
            let (uid, ctx) = get_user_id_and_sec_context();
            assert_eq!(uid, Oid(77));
            assert!(ctx.is_empty());
            assert!(!in_security_restricted_operation());
        })
        .await;
    }

    #[tokio::test]
    async fn security_restricted_flag_set_and_clear() {
        let s = Arc::new(Session::new(BackendType::BACKEND));
        session::scope(s, async {
            set_user_id_and_sec_context(Oid(1), SecurityContext::RESTRICTED_OPERATION);
            assert!(in_security_restricted_operation());
            assert!(!in_local_user_id_change());

            set_user_id_and_sec_context(Oid(1), SecurityContext::empty());
            assert!(!in_security_restricted_operation());
        })
        .await;
    }
}

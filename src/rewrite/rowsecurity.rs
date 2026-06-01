//! rewrite/rowsecurity.rs
//!   Routines to support policies for row-level security (aka RLS).
//!
//! Translated 1:1 from postgres/src/backend/rewrite/rowsecurity.c
//! Header merged from postgres/src/include/rewrite/rowsecurity.h
//!
//! Policies in PostgreSQL provide a mechanism to limit what records are
//! returned to a user and what records a user is permitted to add to a table.
//!
//! Policies can be defined for specific roles, specific commands, or provided
//! by an extension.  Row security can also be enabled for a table without any
//! policies being explicitly defined, in which case a default-deny policy is
//! applied.
//!
//! Any part of the system which is returning records back to the user, or
//! which is accepting records from the user to add to a table, needs to
//! consider the policies associated with the table (if any).  For normal
//! queries, this is handled by calling get_row_security_policies() during
//! rewrite, for each RTE in the query.  This returns the expressions defined
//! by the table's policies as a list that is prepended to the securityQuals
//! list for the RTE.  For queries which modify the table, any WITH CHECK
//! clauses from the table's policies are also returned and prepended to the
//! list of WithCheckOptions for the Query to check each row that is being
//! added to the table.  Other parts of the system (eg: COPY) simply construct
//! a normal query and use that, if RLS is to be applied.
//!
//! The check to see if RLS should be enabled is provided through
//! check_enable_rls(), which returns an enum (defined in rowsecurity.h) to
//! indicate if RLS should be enabled (RLS_ENABLED), or bypassed (RLS_NONE or
//! RLS_NONE_ENV).  RLS_NONE_ENV indicates that RLS should be bypassed
//! in the current environment, but that may change if the row_security GUC or
//! the current role changes.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::postgres_ext::Oid;
use crate::{foreach, current_cell, makeNode};

use crate::c::OidIsValid;
use crate::miscadmin::GetUserId;
use crate::nodes::nodes::{CmdType, Node, NodeTag, OnConflictAction};
use crate::nodes::pg_list::{lappend, lfirst, linitial, list_length, List, ListCell, NIL};
use crate::nodes::primnodes::{BoolExprType, Const, Expr};
use crate::nodes::parsenodes::{
    Query, RangeTblEntry, RTEKind, RTEPermissionInfo, WCOKind, WithCheckOption, ACL_SELECT,
    ACL_UPDATE,
};
use crate::catalog::pg_class::{RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};
use crate::catalog::pg_type_d::BOOLOID;
use crate::storage::lockdefs::NoLock;
use crate::utils::array::{ArrayType, ARR_DATA_PTR, ARR_DIMS};
use crate::utils::misc::rls::{RLS_NONE, RLS_NONE_ENV};
use crate::utils::rel::Relation;

// Constants from utils/acl.h not yet ported to a Rust module.
const ACL_INSERT_CHR: c_char = b'a' as c_char;
const ACL_SELECT_CHR: c_char = b'r' as c_char;
const ACL_UPDATE_CHR: c_char = b'w' as c_char;
const ACL_DELETE_CHR: c_char = b'd' as c_char;
const ACL_ID_PUBLIC: Oid = 0;

// ---------------------------------------------------------------------------
// Header: rewrite/rowsecurity.h
// ---------------------------------------------------------------------------

/// RowSecurityPolicy
#[repr(C)]
pub struct RowSecurityPolicy {
    pub policy_name: *mut c_char,           /* Name of the policy */
    pub polcmd: c_char,                     /* Type of command policy is for */
    pub roles: *mut ArrayType,              /* Array of roles policy is for */
    pub permissive: bool,                   /* restrictive or permissive policy */
    pub qual: *mut Expr,                    /* Expression to filter rows */
    pub with_check_qual: *mut Expr,         /* Expression to limit rows allowed */
    pub hassublinks: bool,                  /* If either expression has sublinks */
}

/// RowSecurityDesc
#[repr(C)]
pub struct RowSecurityDesc {
    pub rscxt: MemoryContext,               /* row security memory context */
    pub policies: *mut List,                /* list of row security policies */
}

pub type row_security_policy_hook_type =
    Option<unsafe fn(cmdtype: CmdType, relation: Relation) -> *mut List>;

// ---------------------------------------------------------------------------
// hooks to allow extensions to add their own security policies
//
// row_security_policy_hook_permissive can be used to add policies which
// are combined with the other permissive policies, using OR.
//
// row_security_policy_hook_restrictive can be used to add policies which
// are enforced, regardless of other policies (they are combined using AND).
// ---------------------------------------------------------------------------

pub static mut row_security_policy_hook_permissive: row_security_policy_hook_type = None;
pub static mut row_security_policy_hook_restrictive: row_security_policy_hook_type = None;

/*
 * Get any row security quals and WithCheckOption checks that should be
 * applied to the specified RTE.
 *
 * In addition, hasRowSecurity is set to true if row-level security is enabled
 * (even if this RTE doesn't have any row security quals), and hasSubLinks is
 * set to true if any of the quals returned contain sublinks.
 */
pub unsafe fn get_row_security_policies(
    root: *mut Query,
    rte: *mut RangeTblEntry,
    rt_index: c_int,
    securityQuals: *mut *mut List,
    withCheckOptions: *mut *mut List,
    hasRowSecurity: *mut bool,
    hasSubLinks: *mut bool,
) {
    let user_id: Oid;
    let rls_status: c_int;
    let rel: Relation;
    let commandType: CmdType;
    let mut permissive_policies: *mut List = std::ptr::null_mut();
    let mut restrictive_policies: *mut List = std::ptr::null_mut();
    let perminfo: *mut RTEPermissionInfo;

    /* Defaults for the return values */
    *securityQuals = NIL as *mut List;
    *withCheckOptions = NIL as *mut List;
    *hasRowSecurity = false;
    *hasSubLinks = false;

    Assert!((*rte).rtekind == RTEKind::RTE_RELATION);

    /* If this is not a normal relation, just return immediately */
    if (*rte).relkind != RELKIND_RELATION && (*rte).relkind != RELKIND_PARTITIONED_TABLE {
        return;
    }

    perminfo = getRTEPermissionInfo((*root).rteperminfos, rte);

    /* Switch to checkAsUser if it's set */
    user_id = if OidIsValid((*perminfo).checkAsUser) {
        (*perminfo).checkAsUser
    } else {
        GetUserId()
    };

    /* Determine the state of RLS for this, pass checkAsUser explicitly */
    rls_status = check_enable_rls((*rte).relid, (*perminfo).checkAsUser, false);

    /* If there is no RLS on this table at all, nothing to do */
    if rls_status == RLS_NONE {
        return;
    }

    /*
     * RLS_NONE_ENV means we are not doing any RLS now, but that may change
     * with changes to the environment, so we mark it as hasRowSecurity to
     * force a re-plan when the environment changes.
     */
    if rls_status == RLS_NONE_ENV {
        /*
         * Indicate that this query may involve RLS and must therefore be
         * replanned if the environment changes (GUCs, role), but we are not
         * adding anything here.
         */
        *hasRowSecurity = true;

        return;
    }

    /*
     * RLS is enabled for this relation.
     *
     * Get the security policies that should be applied, based on the command
     * type.  Note that if this isn't the target relation, we actually want
     * the relation's SELECT policies, regardless of the query command type,
     * for example in UPDATE t1 ... FROM t2 we need to apply t1's UPDATE
     * policies and t2's SELECT policies.
     */
    rel = table_open((*rte).relid, NoLock);

    commandType = if rt_index == (*root).resultRelation {
        (*root).commandType
    } else {
        CmdType::CMD_SELECT
    };

    /*
     * In some cases, we need to apply USING policies (which control the
     * visibility of records) associated with multiple command types (see
     * specific cases below).
     *
     * When considering the order in which to apply these USING policies, we
     * prefer to apply higher privileged policies, those which allow the user
     * to lock records (UPDATE and DELETE), first, followed by policies which
     * don't (SELECT).
     *
     * Note that the optimizer is free to push down and reorder quals which
     * use leakproof functions.
     *
     * In all cases, if there are no policy clauses allowing access to rows in
     * the table for the specific type of operation, then a single
     * always-false clause (a default-deny policy) will be added (see
     * add_security_quals).
     */

    /*
     * For a SELECT, if UPDATE privileges are required (eg: the user has
     * specified FOR [KEY] UPDATE/SHARE), then add the UPDATE USING quals
     * first.
     *
     * This way, we filter out any records from the SELECT FOR SHARE/UPDATE
     * which the user does not have access to via the UPDATE USING policies,
     * similar to how we require normal UPDATE rights for these queries.
     */
    if commandType == CmdType::CMD_SELECT && ((*perminfo).requiredPerms & ACL_UPDATE) != 0 {
        let mut update_permissive_policies: *mut List = std::ptr::null_mut();
        let mut update_restrictive_policies: *mut List = std::ptr::null_mut();

        get_policies_for_relation(
            rel,
            CmdType::CMD_UPDATE,
            user_id,
            &mut update_permissive_policies,
            &mut update_restrictive_policies,
        );

        add_security_quals(
            rt_index,
            update_permissive_policies,
            update_restrictive_policies,
            securityQuals,
            hasSubLinks,
        );
    }

    /*
     * For SELECT, UPDATE and DELETE, add security quals to enforce the USING
     * policies.  These security quals control access to existing table rows.
     * Restrictive policies are combined together using AND, and permissive
     * policies are combined together using OR.
     */

    get_policies_for_relation(
        rel,
        commandType,
        user_id,
        &mut permissive_policies,
        &mut restrictive_policies,
    );

    if commandType == CmdType::CMD_SELECT
        || commandType == CmdType::CMD_UPDATE
        || commandType == CmdType::CMD_DELETE
    {
        add_security_quals(
            rt_index,
            permissive_policies,
            restrictive_policies,
            securityQuals,
            hasSubLinks,
        );
    }

    /*
     * Similar to above, during an UPDATE, DELETE, or MERGE, if SELECT rights
     * are also required (eg: when a RETURNING clause exists, or the user has
     * provided a WHERE clause which involves columns from the relation), we
     * collect up CMD_SELECT policies and add them via add_security_quals
     * first.
     *
     * This way, we filter out any records which are not visible through an
     * ALL or SELECT USING policy.
     */
    if (commandType == CmdType::CMD_UPDATE
        || commandType == CmdType::CMD_DELETE
        || commandType == CmdType::CMD_MERGE)
        && ((*perminfo).requiredPerms & ACL_SELECT) != 0
    {
        let mut select_permissive_policies: *mut List = std::ptr::null_mut();
        let mut select_restrictive_policies: *mut List = std::ptr::null_mut();

        get_policies_for_relation(
            rel,
            CmdType::CMD_SELECT,
            user_id,
            &mut select_permissive_policies,
            &mut select_restrictive_policies,
        );

        add_security_quals(
            rt_index,
            select_permissive_policies,
            select_restrictive_policies,
            securityQuals,
            hasSubLinks,
        );
    }

    /*
     * For INSERT and UPDATE, add withCheckOptions to verify that any new
     * records added are consistent with the security policies.  This will use
     * each policy's WITH CHECK clause, or its USING clause if no explicit
     * WITH CHECK clause is defined.
     */
    if commandType == CmdType::CMD_INSERT || commandType == CmdType::CMD_UPDATE {
        /* This should be the target relation */
        Assert!(rt_index == (*root).resultRelation);

        add_with_check_options(
            rel,
            rt_index,
            if commandType == CmdType::CMD_INSERT {
                WCOKind::WCO_RLS_INSERT_CHECK
            } else {
                WCOKind::WCO_RLS_UPDATE_CHECK
            },
            permissive_policies,
            restrictive_policies,
            withCheckOptions,
            hasSubLinks,
            false,
        );

        /*
         * Get and add ALL/SELECT policies, if SELECT rights are required for
         * this relation (eg: when RETURNING is used).  These are added as WCO
         * policies rather than security quals to ensure that an error is
         * raised if a policy is violated; otherwise, we might end up silently
         * dropping rows to be added.
         */
        if ((*perminfo).requiredPerms & ACL_SELECT) != 0 {
            let mut select_permissive_policies: *mut List = NIL as *mut List;
            let mut select_restrictive_policies: *mut List = NIL as *mut List;

            get_policies_for_relation(
                rel,
                CmdType::CMD_SELECT,
                user_id,
                &mut select_permissive_policies,
                &mut select_restrictive_policies,
            );
            add_with_check_options(
                rel,
                rt_index,
                if commandType == CmdType::CMD_INSERT {
                    WCOKind::WCO_RLS_INSERT_CHECK
                } else {
                    WCOKind::WCO_RLS_UPDATE_CHECK
                },
                select_permissive_policies,
                select_restrictive_policies,
                withCheckOptions,
                hasSubLinks,
                true,
            );
        }

        /*
         * For INSERT ... ON CONFLICT DO UPDATE we need additional policy
         * checks for the UPDATE which may be applied to the same RTE.
         */
        if commandType == CmdType::CMD_INSERT
            && !(*root).onConflict.is_null()
            && (*(*root).onConflict).action == OnConflictAction::ONCONFLICT_UPDATE
        {
            let mut conflict_permissive_policies: *mut List = std::ptr::null_mut();
            let mut conflict_restrictive_policies: *mut List = std::ptr::null_mut();
            let mut conflict_select_permissive_policies: *mut List = NIL as *mut List;
            let mut conflict_select_restrictive_policies: *mut List = NIL as *mut List;

            /* Get the policies that apply to the auxiliary UPDATE */
            get_policies_for_relation(
                rel,
                CmdType::CMD_UPDATE,
                user_id,
                &mut conflict_permissive_policies,
                &mut conflict_restrictive_policies,
            );

            /*
             * Enforce the USING clauses of the UPDATE policies using WCOs
             * rather than security quals.  This ensures that an error is
             * raised if the conflicting row cannot be updated due to RLS,
             * rather than the change being silently dropped.
             */
            add_with_check_options(
                rel,
                rt_index,
                WCOKind::WCO_RLS_CONFLICT_CHECK,
                conflict_permissive_policies,
                conflict_restrictive_policies,
                withCheckOptions,
                hasSubLinks,
                true,
            );

            /*
             * Get and add ALL/SELECT policies, as WCO_RLS_CONFLICT_CHECK WCOs
             * to ensure they are considered when taking the UPDATE path of an
             * INSERT .. ON CONFLICT DO UPDATE, if SELECT rights are required
             * for this relation, also as WCO policies, again, to avoid
             * silently dropping data.  See above.
             */
            if ((*perminfo).requiredPerms & ACL_SELECT) != 0 {
                get_policies_for_relation(
                    rel,
                    CmdType::CMD_SELECT,
                    user_id,
                    &mut conflict_select_permissive_policies,
                    &mut conflict_select_restrictive_policies,
                );
                add_with_check_options(
                    rel,
                    rt_index,
                    WCOKind::WCO_RLS_CONFLICT_CHECK,
                    conflict_select_permissive_policies,
                    conflict_select_restrictive_policies,
                    withCheckOptions,
                    hasSubLinks,
                    true,
                );
            }

            /* Enforce the WITH CHECK clauses of the UPDATE policies */
            add_with_check_options(
                rel,
                rt_index,
                WCOKind::WCO_RLS_UPDATE_CHECK,
                conflict_permissive_policies,
                conflict_restrictive_policies,
                withCheckOptions,
                hasSubLinks,
                false,
            );

            /*
             * Add ALL/SELECT policies as WCO_RLS_UPDATE_CHECK WCOs, to ensure
             * that the final updated row is visible when taking the UPDATE
             * path of an INSERT .. ON CONFLICT DO UPDATE, if SELECT rights
             * are required for this relation.
             */
            if ((*perminfo).requiredPerms & ACL_SELECT) != 0 {
                add_with_check_options(
                    rel,
                    rt_index,
                    WCOKind::WCO_RLS_UPDATE_CHECK,
                    conflict_select_permissive_policies,
                    conflict_select_restrictive_policies,
                    withCheckOptions,
                    hasSubLinks,
                    true,
                );
            }
        }
    }

    /*
     * FOR MERGE, we fetch policies for UPDATE, DELETE and INSERT (and ALL)
     * and set them up so that we can enforce the appropriate policy depending
     * on the final action we take.
     *
     * We already fetched the SELECT policies above, to check existing rows,
     * but we must also check that new rows created by INSERT/UPDATE actions
     * are visible, if SELECT rights are required. For INSERT actions, we only
     * do this if RETURNING is specified, to be consistent with a plain INSERT
     * command, which can only require SELECT rights when RETURNING is used.
     *
     * We don't push the UPDATE/DELETE USING quals to the RTE because we don't
     * really want to apply them while scanning the relation since we don't
     * know whether we will be doing an UPDATE or a DELETE at the end. We
     * apply the respective policy once we decide the final action on the
     * target tuple.
     *
     * XXX We are setting up USING quals as WITH CHECK. If RLS prohibits
     * UPDATE/DELETE on the target row, we shall throw an error instead of
     * silently ignoring the row. This is different than how normal
     * UPDATE/DELETE works and more in line with INSERT ON CONFLICT DO UPDATE
     * handling.
     */
    if commandType == CmdType::CMD_MERGE {
        let mut merge_update_permissive_policies: *mut List = std::ptr::null_mut();
        let mut merge_update_restrictive_policies: *mut List = std::ptr::null_mut();
        let mut merge_delete_permissive_policies: *mut List = std::ptr::null_mut();
        let mut merge_delete_restrictive_policies: *mut List = std::ptr::null_mut();
        let mut merge_insert_permissive_policies: *mut List = std::ptr::null_mut();
        let mut merge_insert_restrictive_policies: *mut List = std::ptr::null_mut();
        let mut merge_select_permissive_policies: *mut List = NIL as *mut List;
        let mut merge_select_restrictive_policies: *mut List = NIL as *mut List;

        /*
         * Fetch the UPDATE policies and set them up to execute on the
         * existing target row before doing UPDATE.
         */
        get_policies_for_relation(
            rel,
            CmdType::CMD_UPDATE,
            user_id,
            &mut merge_update_permissive_policies,
            &mut merge_update_restrictive_policies,
        );

        /*
         * WCO_RLS_MERGE_UPDATE_CHECK is used to check UPDATE USING quals on
         * the existing target row.
         */
        add_with_check_options(
            rel,
            rt_index,
            WCOKind::WCO_RLS_MERGE_UPDATE_CHECK,
            merge_update_permissive_policies,
            merge_update_restrictive_policies,
            withCheckOptions,
            hasSubLinks,
            true,
        );

        /* Enforce the WITH CHECK clauses of the UPDATE policies */
        add_with_check_options(
            rel,
            rt_index,
            WCOKind::WCO_RLS_UPDATE_CHECK,
            merge_update_permissive_policies,
            merge_update_restrictive_policies,
            withCheckOptions,
            hasSubLinks,
            false,
        );

        /*
         * Add ALL/SELECT policies as WCO_RLS_UPDATE_CHECK WCOs, to ensure
         * that the updated row is visible when executing an UPDATE action, if
         * SELECT rights are required for this relation.
         */
        if ((*perminfo).requiredPerms & ACL_SELECT) != 0 {
            get_policies_for_relation(
                rel,
                CmdType::CMD_SELECT,
                user_id,
                &mut merge_select_permissive_policies,
                &mut merge_select_restrictive_policies,
            );
            add_with_check_options(
                rel,
                rt_index,
                WCOKind::WCO_RLS_UPDATE_CHECK,
                merge_select_permissive_policies,
                merge_select_restrictive_policies,
                withCheckOptions,
                hasSubLinks,
                true,
            );
        }

        /*
         * Fetch the DELETE policies and set them up to execute on the
         * existing target row before doing DELETE.
         */
        get_policies_for_relation(
            rel,
            CmdType::CMD_DELETE,
            user_id,
            &mut merge_delete_permissive_policies,
            &mut merge_delete_restrictive_policies,
        );

        /*
         * WCO_RLS_MERGE_DELETE_CHECK is used to check DELETE USING quals on
         * the existing target row.
         */
        add_with_check_options(
            rel,
            rt_index,
            WCOKind::WCO_RLS_MERGE_DELETE_CHECK,
            merge_delete_permissive_policies,
            merge_delete_restrictive_policies,
            withCheckOptions,
            hasSubLinks,
            true,
        );

        /*
         * No special handling is required for INSERT policies. They will be
         * checked and enforced during ExecInsert(). But we must add them to
         * withCheckOptions.
         */
        get_policies_for_relation(
            rel,
            CmdType::CMD_INSERT,
            user_id,
            &mut merge_insert_permissive_policies,
            &mut merge_insert_restrictive_policies,
        );

        add_with_check_options(
            rel,
            rt_index,
            WCOKind::WCO_RLS_INSERT_CHECK,
            merge_insert_permissive_policies,
            merge_insert_restrictive_policies,
            withCheckOptions,
            hasSubLinks,
            false,
        );

        /*
         * Add ALL/SELECT policies as WCO_RLS_INSERT_CHECK WCOs, to ensure
         * that the inserted row is visible when executing an INSERT action,
         * if RETURNING is specified and SELECT rights are required for this
         * relation.
         */
        if ((*perminfo).requiredPerms & ACL_SELECT) != 0 && !(*root).returningList.is_null() {
            add_with_check_options(
                rel,
                rt_index,
                WCOKind::WCO_RLS_INSERT_CHECK,
                merge_select_permissive_policies,
                merge_select_restrictive_policies,
                withCheckOptions,
                hasSubLinks,
                true,
            );
        }
    }

    table_close(rel, NoLock);

    /*
     * Copy checkAsUser to the row security quals and WithCheckOption checks,
     * in case they contain any subqueries referring to other relations.
     */
    setRuleCheckAsUser(*securityQuals as *mut Node, (*perminfo).checkAsUser);
    setRuleCheckAsUser(*withCheckOptions as *mut Node, (*perminfo).checkAsUser);

    /*
     * Mark this query as having row security, so plancache can invalidate it
     * when necessary (eg: role changes)
     */
    *hasRowSecurity = true;
}

/*
 * get_policies_for_relation
 *
 * Returns lists of permissive and restrictive policies to be applied to the
 * specified relation, based on the command type and role.
 *
 * This includes any policies added by extensions.
 */
unsafe fn get_policies_for_relation(
    relation: Relation,
    cmd: CmdType,
    user_id: Oid,
    permissive_policies: *mut *mut List,
    restrictive_policies: *mut *mut List,
) {
    *permissive_policies = NIL as *mut List;
    *restrictive_policies = NIL as *mut List;

    /* First find all internal policies for the relation. */
    foreach!(item, (*((*relation).rd_rsdesc as *mut RowSecurityDesc)).policies, {
        let mut cmd_matches: bool = false;
        let policy = lfirst(current_cell!(item)) as *mut RowSecurityPolicy;

        /* Always add ALL policies, if they exist. */
        if (*policy).polcmd == b'*' as c_char {
            cmd_matches = true;
        } else {
            /* Check whether the policy applies to the specified command type */
            match cmd {
                CmdType::CMD_SELECT => {
                    if (*policy).polcmd == ACL_SELECT_CHR {
                        cmd_matches = true;
                    }
                }
                CmdType::CMD_INSERT => {
                    if (*policy).polcmd == ACL_INSERT_CHR {
                        cmd_matches = true;
                    }
                }
                CmdType::CMD_UPDATE => {
                    if (*policy).polcmd == ACL_UPDATE_CHR {
                        cmd_matches = true;
                    }
                }
                CmdType::CMD_DELETE => {
                    if (*policy).polcmd == ACL_DELETE_CHR {
                        cmd_matches = true;
                    }
                }
                CmdType::CMD_MERGE => {
                    /*
                     * We do not support a separate policy for MERGE command.
                     * Instead it derives from the policies defined for other
                     * commands.
                     */
                }
                _ => {
                    elog!(ERROR, "unrecognized policy command type {}", cmd as c_int);
                }
            }
        }

        /*
         * Add this policy to the relevant list of policies if it applies to
         * the specified role.
         */
        if cmd_matches && check_role_for_policy((*policy).roles, user_id) {
            if (*policy).permissive {
                *permissive_policies = lappend(*permissive_policies, policy as *mut std::ffi::c_void);
            } else {
                *restrictive_policies = lappend(*restrictive_policies, policy as *mut std::ffi::c_void);
            }
        }
    });

    /*
     * We sort restrictive policies by name so that any WCOs they generate are
     * checked in a well-defined order.
     */
    sort_policies_by_name(*restrictive_policies);

    /*
     * Then add any permissive or restrictive policies defined by extensions.
     * These are simply appended to the lists of internal policies, if they
     * apply to the specified role.
     */
    if let Some(hook) = row_security_policy_hook_restrictive {
        let hook_policies: *mut List = hook(cmd, relation);

        /*
         * As with built-in restrictive policies, we sort any hook-provided
         * restrictive policies by name also.  Note that we also intentionally
         * always check all built-in restrictive policies, in name order,
         * before checking restrictive policies added by hooks, in name order.
         */
        sort_policies_by_name(hook_policies);

        foreach!(item, hook_policies, {
            let policy = lfirst(current_cell!(item)) as *mut RowSecurityPolicy;

            if check_role_for_policy((*policy).roles, user_id) {
                *restrictive_policies =
                    lappend(*restrictive_policies, policy as *mut std::ffi::c_void);
            }
        });
    }

    if let Some(hook) = row_security_policy_hook_permissive {
        let hook_policies: *mut List = hook(cmd, relation);

        foreach!(item, hook_policies, {
            let policy = lfirst(current_cell!(item)) as *mut RowSecurityPolicy;

            if check_role_for_policy((*policy).roles, user_id) {
                *permissive_policies =
                    lappend(*permissive_policies, policy as *mut std::ffi::c_void);
            }
        });
    }
}

/*
 * sort_policies_by_name
 *
 * This is only used for restrictive policies, ensuring that any
 * WithCheckOptions they generate are applied in a well-defined order.
 * This is not necessary for permissive policies, since they are all combined
 * together using OR into a single WithCheckOption check.
 */
unsafe fn sort_policies_by_name(policies: *mut List) {
    list_sort(policies, Some(row_security_policy_cmp));
}

/*
 * list_sort comparator to sort RowSecurityPolicy entries by name
 */
unsafe fn row_security_policy_cmp(a: *const ListCell, b: *const ListCell) -> c_int {
    let pa = lfirst(a as *mut ListCell) as *const RowSecurityPolicy;
    let pb = lfirst(b as *mut ListCell) as *const RowSecurityPolicy;

    /* Guard against NULL policy names from extensions */
    if (*pa).policy_name.is_null() {
        return if (*pb).policy_name.is_null() { 0 } else { 1 };
    }
    if (*pb).policy_name.is_null() {
        return -1;
    }

    strcmp((*pa).policy_name, (*pb).policy_name)
}

/*
 * add_security_quals
 *
 * Add security quals to enforce the specified RLS policies, restricting
 * access to existing data in a table.  If there are no policies controlling
 * access to the table, then all access is prohibited --- i.e., an implicit
 * default-deny policy is used.
 *
 * New security quals are added to securityQuals, and hasSubLinks is set to
 * true if any of the quals added contain sublink subqueries.
 */
unsafe fn add_security_quals(
    rt_index: c_int,
    permissive_policies: *mut List,
    restrictive_policies: *mut List,
    securityQuals: *mut *mut List,
    hasSubLinks: *mut bool,
) {
    let mut permissive_quals: *mut List = NIL as *mut List;
    let rowsec_expr: *mut Expr;

    /*
     * First collect up the permissive quals.  If we do not find any
     * permissive policies then no rows are visible (this is handled below).
     */
    foreach!(item, permissive_policies, {
        let policy = lfirst(current_cell!(item)) as *mut RowSecurityPolicy;

        if !(*policy).qual.is_null() {
            permissive_quals =
                lappend(permissive_quals, copyObject((*policy).qual as *const std::ffi::c_void));
            *hasSubLinks |= (*policy).hassublinks;
        }
    });

    /*
     * We must have permissive quals, always, or no rows are visible.
     *
     * If we do not, then we simply return a single 'false' qual which results
     * in no rows being visible.
     */
    if permissive_quals != NIL as *mut List {
        /*
         * We now know that permissive policies exist, so we can now add
         * security quals based on the USING clauses from the restrictive
         * policies.  Since these need to be combined together using AND, we
         * can just add them one at a time.
         */
        foreach!(item, restrictive_policies, {
            let policy = lfirst(current_cell!(item)) as *mut RowSecurityPolicy;

            if !(*policy).qual.is_null() {
                let qual = copyObject((*policy).qual as *const std::ffi::c_void) as *mut Expr;
                ChangeVarNodes(qual as *mut Node, 1, rt_index, 0);

                *securityQuals =
                    list_append_unique(*securityQuals, qual as *mut std::ffi::c_void);
                *hasSubLinks |= (*policy).hassublinks;
            }
        });

        /*
         * Then add a single security qual combining together the USING
         * clauses from all the permissive policies using OR.
         */
        if list_length(permissive_quals) == 1 {
            rowsec_expr = linitial(permissive_quals) as *mut Expr;
        } else {
            rowsec_expr = makeBoolExpr(BoolExprType::OR_EXPR, permissive_quals, -1) as *mut Expr;
        }

        ChangeVarNodes(rowsec_expr as *mut Node, 1, rt_index, 0);
        *securityQuals = list_append_unique(*securityQuals, rowsec_expr as *mut std::ffi::c_void);
    } else {
        /*
         * A permissive policy must exist for rows to be visible at all.
         * Therefore, if there were no permissive policies found, return a
         * single always-false clause.
         */
        *securityQuals = lappend(
            *securityQuals,
            makeConst(
                BOOLOID,
                -1,
                InvalidOid,
                std::mem::size_of::<bool>() as c_int,
                BoolGetDatum(false),
                false,
                true,
            ) as *mut std::ffi::c_void,
        );
    }
}

/*
 * add_with_check_options
 *
 * Add WithCheckOptions of the specified kind to check that new records
 * added by an INSERT or UPDATE are consistent with the specified RLS
 * policies.  Normally new data must satisfy the WITH CHECK clauses from the
 * policies.  If a policy has no explicit WITH CHECK clause, its USING clause
 * is used instead.  In the special case of an UPDATE arising from an
 * INSERT ... ON CONFLICT DO UPDATE, existing records are first checked using
 * a WCO_RLS_CONFLICT_CHECK WithCheckOption, which always uses the USING
 * clauses from RLS policies.
 *
 * New WCOs are added to withCheckOptions, and hasSubLinks is set to true if
 * any of the check clauses added contain sublink subqueries.
 */
unsafe fn add_with_check_options(
    rel: Relation,
    rt_index: c_int,
    kind: WCOKind,
    permissive_policies: *mut List,
    restrictive_policies: *mut List,
    withCheckOptions: *mut *mut List,
    hasSubLinks: *mut bool,
    force_using: bool,
) {
    let mut permissive_quals: *mut List = NIL as *mut List;

    // #define QUAL_FOR_WCO(policy) \
    //   ( !force_using && (policy)->with_check_qual != NULL ?
    //     (policy)->with_check_qual : (policy)->qual )
    let QUAL_FOR_WCO = |policy: *mut RowSecurityPolicy| -> *mut Expr {
        if !force_using && !(*policy).with_check_qual.is_null() {
            (*policy).with_check_qual
        } else {
            (*policy).qual
        }
    };

    /*
     * First collect up the permissive policy clauses, similar to
     * add_security_quals.
     */
    foreach!(item, permissive_policies, {
        let policy = lfirst(current_cell!(item)) as *mut RowSecurityPolicy;
        let qual = QUAL_FOR_WCO(policy);

        if !qual.is_null() {
            permissive_quals =
                lappend(permissive_quals, copyObject(qual as *const std::ffi::c_void));
            *hasSubLinks |= (*policy).hassublinks;
        }
    });

    /*
     * There must be at least one permissive qual found or no rows are allowed
     * to be added.  This is the same as in add_security_quals.
     *
     * If there are no permissive_quals then we fall through and return a
     * single 'false' WCO, preventing all new rows.
     */
    if permissive_quals != NIL as *mut List {
        /*
         * Add a single WithCheckOption for all the permissive policy clauses,
         * combining them together using OR.  This check has no policy name,
         * since if the check fails it means that no policy granted permission
         * to perform the update, rather than any particular policy being
         * violated.
         */
        let mut wco: *mut WithCheckOption;

        wco = makeNode!(WithCheckOption, T_WithCheckOption);
        (*wco).kind = kind;
        (*wco).relname = pstrdup(RelationGetRelationName(rel));
        (*wco).polname = std::ptr::null_mut();
        (*wco).cascaded = false;

        if list_length(permissive_quals) == 1 {
            (*wco).qual = linitial(permissive_quals) as *mut Node;
        } else {
            (*wco).qual =
                makeBoolExpr(BoolExprType::OR_EXPR, permissive_quals, -1) as *mut Node;
        }

        ChangeVarNodes((*wco).qual, 1, rt_index, 0);

        *withCheckOptions =
            list_append_unique(*withCheckOptions, wco as *mut std::ffi::c_void);

        /*
         * Now add WithCheckOptions for each of the restrictive policy clauses
         * (which will be combined together using AND).  We use a separate
         * WithCheckOption for each restrictive policy to allow the policy
         * name to be included in error reports if the policy is violated.
         */
        foreach!(item, restrictive_policies, {
            let policy = lfirst(current_cell!(item)) as *mut RowSecurityPolicy;
            let qual = QUAL_FOR_WCO(policy);

            if !qual.is_null() {
                let qual = copyObject(qual as *const std::ffi::c_void) as *mut Expr;
                ChangeVarNodes(qual as *mut Node, 1, rt_index, 0);

                wco = makeNode!(WithCheckOption, T_WithCheckOption);
                (*wco).kind = kind;
                (*wco).relname = pstrdup(RelationGetRelationName(rel));
                (*wco).polname = pstrdup((*policy).policy_name);
                (*wco).qual = qual as *mut Node;
                (*wco).cascaded = false;

                *withCheckOptions =
                    list_append_unique(*withCheckOptions, wco as *mut std::ffi::c_void);
                *hasSubLinks |= (*policy).hassublinks;
            }
        });
    } else {
        /*
         * If there were no policy clauses to check new data, add a single
         * always-false WCO (a default-deny policy).
         */
        let wco: *mut WithCheckOption;

        wco = makeNode!(WithCheckOption, T_WithCheckOption);
        (*wco).kind = kind;
        (*wco).relname = pstrdup(RelationGetRelationName(rel));
        (*wco).polname = std::ptr::null_mut();
        (*wco).qual = makeConst(
            BOOLOID,
            -1,
            InvalidOid,
            std::mem::size_of::<bool>() as c_int,
            BoolGetDatum(false),
            false,
            true,
        ) as *mut Node;
        (*wco).cascaded = false;

        *withCheckOptions = lappend(*withCheckOptions, wco as *mut std::ffi::c_void);
    }
}

/*
 * check_role_for_policy -
 *	 determines if the policy should be applied for the current role
 */
unsafe fn check_role_for_policy(policy_roles: *mut ArrayType, user_id: Oid) -> bool {
    let roles = ARR_DATA_PTR(policy_roles) as *mut Oid;

    /* Quick fall-thru for policies applied to all roles */
    if *roles.add(0) == ACL_ID_PUBLIC {
        return true;
    }

    let mut i: c_int = 0;
    while i < *ARR_DIMS(policy_roles).add(0) {
        if has_privs_of_role(user_id, *roles.add(i as usize)) {
            return true;
        }
        i += 1;
    }

    false
}

// ---------------------------------------------------------------------------
// Local stubs for unported dependencies
// ---------------------------------------------------------------------------

unsafe fn getRTEPermissionInfo(
    rteperminfos: *mut List,
    rte: *mut RangeTblEntry,
) -> *mut RTEPermissionInfo {
    unimplemented!() // TODO: parser/parse_relation.c
}

unsafe fn check_enable_rls(relid: Oid, checkAsUser: Oid, noError: bool) -> c_int {
    unimplemented!() // TODO: utils/misc/rls.c
}

unsafe fn table_open(relationId: Oid, lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/table/table.c
}

unsafe fn table_close(relation: Relation, lockmode: c_int) {
    unimplemented!() // TODO: access/table/table.c
}

unsafe fn setRuleCheckAsUser(node: *mut Node, userid: Oid) {
    unimplemented!() // TODO: rewrite/rewriteManip.c
}

unsafe fn ChangeVarNodes(node: *mut Node, rt_index: c_int, new_index: c_int, sublevels_up: c_int) {
    unimplemented!() // TODO: rewrite/rewriteManip.c
}

unsafe fn makeBoolExpr(boolop: BoolExprType, args: *mut List, location: c_int) -> *mut Expr {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn makeConst(
    consttype: Oid,
    consttypmod: i32,
    constcollid: Oid,
    constlen: c_int,
    constvalue: Datum,
    constisnull: bool,
    constbyval: bool,
) -> *mut Const {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn copyObject(from: *const std::ffi::c_void) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: nodes/copyfuncs.c
}

unsafe fn list_sort(
    list: *mut List,
    cmp: Option<unsafe fn(a: *const ListCell, b: *const ListCell) -> c_int>,
) {
    unimplemented!() // TODO: nodes/list.c
}

unsafe fn list_append_unique(list: *mut List, datum: *mut std::ffi::c_void) -> *mut List {
    unimplemented!() // TODO: nodes/list.c
}

unsafe fn has_privs_of_role(member: Oid, role: Oid) -> bool {
    unimplemented!() // TODO: utils/adt/acl.c
}

unsafe fn RelationGetRelationName(relation: Relation) -> *const c_char {
    unimplemented!() // TODO: utils/rel.h
}

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

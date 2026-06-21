//! src/backend/commands/copy.c
//! Implements the COPY utility command
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! Merged companion header: src/include/commands/copy.h

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int64, uint64};
use crate::nodes::pg_list::{
    lappend, lappend_int, lfirst, lfirst_int, list_member_int, List, ListCell, NIL,
};
use crate::nodes::nodes::{nodeTag, Node, NodeTag};
use crate::nodes::nodes::NodeTag::*;
use crate::postgres_ext::Oid;
use crate::access::attnum::AttrNumber;
use crate::access::common::tupdesc::TupleDesc;

// Real node/parse types that the translated body dereferences for named fields.
use crate::nodes::parsenodes::{CopyStmt, DefElem, ACL_INSERT, ACL_SELECT};
use crate::nodes::primnodes::RangeVar;
use crate::parser::parse_node::ParseState;
use crate::parser::parse_node::ParseExprKind;
use crate::parser::parse_node::ParseExprKind::EXPR_KIND_COPY_WHERE;
use crate::utils::rel::Relation;

// Constants pulled from their canonical homes.
use crate::catalog::pg_known_oids::{
    ROLE_PG_EXECUTE_SERVER_PROGRAM, ROLE_PG_READ_SERVER_FILES, ROLE_PG_WRITE_SERVER_FILES,
};
use crate::storage::lockdefs::{AccessShareLock, NoLock, RowExclusiveLock};
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;
use crate::access::attnum::InvalidAttrNumber;
use crate::utils::misc::rls::RLS_ENABLED;

// Function-like macros that live at the crate root (#[macro_export]).
use crate::{
    castNode, current_cell, foreach, intVal, IsA, lfirst_node, list_make1, makeNode, strVal,
};

/* ===================================================================
 * copy.h definitions
 * =================================================================== */

/*
 * Represents whether a header line should be present, and whether it must
 * match the actual names (which implies "true").
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CopyHeaderChoice {
    COPY_HEADER_FALSE = 0,
    COPY_HEADER_TRUE,
    COPY_HEADER_MATCH,
}
pub use CopyHeaderChoice::*;

/*
 * Represents where to save input processing errors.  More values to be added
 * in the future.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CopyOnErrorChoice {
    COPY_ON_ERROR_STOP = 0, /* immediately throw errors, default */
    COPY_ON_ERROR_IGNORE,   /* ignore errors */
}
pub use CopyOnErrorChoice::*;

/*
 * Represents verbosity of logged messages by COPY command.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CopyLogVerbosityChoice {
    COPY_LOG_VERBOSITY_SILENT = -1, /* logs none */
    COPY_LOG_VERBOSITY_DEFAULT = 0, /* logs no additional messages. As this is
                                     * the default, assign 0 */
    COPY_LOG_VERBOSITY_VERBOSE, /* logs additional messages */
}
pub use CopyLogVerbosityChoice::*;

/*
 * A struct to hold COPY options, in a parsed form. All of these are related
 * to formatting, except for 'freeze', which doesn't really belong here, but
 * it's expedient to parse it along with all the other options.
 */
#[repr(C)]
pub struct CopyFormatOptions {
    /* parameters from the COPY command */
    pub file_encoding: c_int, /* file or remote side's character encoding,
                               * -1 if not specified */
    pub binary: bool,         /* binary format? */
    pub freeze: bool,         /* freeze rows on loading? */
    pub csv_mode: bool,       /* Comma Separated Value format? */
    pub header_line: CopyHeaderChoice, /* header line? */
    pub null_print: *mut c_char, /* NULL marker string (server encoding!) */
    pub null_print_len: c_int, /* length of same */
    pub null_print_client: *mut c_char, /* same converted to file encoding */
    pub default_print: *mut c_char, /* DEFAULT marker string */
    pub default_print_len: c_int, /* length of same */
    pub delim: *mut c_char,   /* column delimiter (must be 1 byte) */
    pub quote: *mut c_char,   /* CSV quote char (must be 1 byte) */
    pub escape: *mut c_char,  /* CSV escape char (must be 1 byte) */
    pub force_quote: *mut List, /* list of column names */
    pub force_quote_all: bool, /* FORCE_QUOTE *? */
    pub force_quote_flags: *mut bool, /* per-column CSV FQ flags */
    pub force_notnull: *mut List, /* list of column names */
    pub force_notnull_all: bool, /* FORCE_NOT_NULL *? */
    pub force_notnull_flags: *mut bool, /* per-column CSV FNN flags */
    pub force_null: *mut List, /* list of column names */
    pub force_null_all: bool, /* FORCE_NULL *? */
    pub force_null_flags: *mut bool, /* per-column CSV FN flags */
    pub convert_selectively: bool, /* do selective binary conversion? */
    pub on_error: CopyOnErrorChoice, /* what to do when error happened */
    pub log_verbosity: CopyLogVerbosityChoice, /* verbosity of logged messages */
    pub reject_limit: int64,  /* maximum tolerable number of errors */
    pub convert_select: *mut List, /* list of column names (can be NIL) */
}

/* These are private in commands/copy[from|to].c */
pub type CopyFromState = *mut CopyFromStateData;
pub type CopyToState = *mut CopyToStateData;

pub type copy_data_source_cb =
    Option<unsafe extern "C" fn(outbuf: *mut c_void, minread: c_int, maxread: c_int) -> c_int>;
pub type copy_data_dest_cb = Option<unsafe extern "C" fn(data: *mut c_void, len: c_int)>;

/* ===================================================================
 * Stub types for dependencies not yet ported
 * =================================================================== */

#[repr(C)]
pub struct CopyFromStateData {
    _private: [u8; 0],
}
#[repr(C)]
pub struct CopyToStateData {
    _private: [u8; 0],
}
// Real node types used by the body (imported here, not stubbed).
use crate::nodes::parsenodes::{ColumnRef, RawStmt, ResTarget, SelectStmt, A_Star};
pub type ExprContext = c_void;
pub type DestReceiver = c_void;
pub type Bitmapset = c_void;
pub type LOCKMODE = c_int;

// Global transaction read-only flag (access/transam/xact.c, not yet ported).
static XactReadOnly: bool = false;

/* ===================================================================
 * Local stubs for unported helper functions
 * =================================================================== */

unsafe fn has_privs_of_role(roleid: Oid, role2: Oid) -> bool {
    crate::utils::adt::acl::has_privs_of_role(roleid as _, role2 as _)
}
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn table_openrv(relation: *mut RangeVar, lockmode: LOCKMODE) -> Relation {
    crate::access::table::table::table_openrv(relation as _, lockmode as _) as _
}
unsafe fn table_close(rel: Relation, lockmode: LOCKMODE) {
    crate::access::table::table::table_close(rel as _, lockmode as _)
}
unsafe fn RelationGetRelid(rel: Relation) -> Oid {
    crate::utils::rel::RelationGetRelid(rel as _) as _
}
unsafe fn RelationGetDescr(rel: Relation) -> TupleDesc {
    crate::utils::rel::RelationGetDescr(rel as _) as _
}
unsafe fn RelationGetNamespace(rel: Relation) -> Oid {
    crate::utils::rel::RelationGetNamespace(rel as _) as _
}
unsafe fn RelationGetRelationName(rel: Relation) -> *mut c_char {
    crate::utils::rel::RelationGetRelationName(rel as _) as _
}
unsafe fn RelationGetNumberOfAttributes(rel: Relation) -> c_int {
    crate::utils::rel::RelationGetNumberOfAttributes(rel as _) as _
}
unsafe fn addRangeTableEntryForRelation(
    _pstate: *mut ParseState,
    _rel: Relation,
    _lockmode: LOCKMODE,
    _alias: *mut c_void,
    _inh: bool,
    _inFromCl: bool,
) -> *mut ParseNamespaceItem {
    crate::parser::parse_relation::addRangeTableEntryForRelation(
        _pstate as _, _rel as _, _lockmode as _, _alias as _, _inh, _inFromCl,
    ) as _
}
unsafe fn addNSItemToQuery(
    _pstate: *mut ParseState,
    _nsitem: *mut ParseNamespaceItem,
    _addToJoinList: bool,
    _addToRelNameSpace: bool,
    _addToVarNameSpace: bool,
) {
    crate::parser::parse_relation::addNSItemToQuery(
        _pstate as _, _nsitem as _, _addToJoinList, _addToRelNameSpace, _addToVarNameSpace,
    )
}
unsafe fn transformExpr(
    _pstate: *mut ParseState,
    _expr: *mut Node,
    _exprKind: ParseExprKind,
) -> *mut Node {
    crate::parser::parse_expr::transformExpr(_pstate as _, _expr as _, _exprKind) as _
}
unsafe fn coerce_to_boolean(
    _pstate: *mut ParseState,
    _node: *mut Node,
    _constructName: *const c_char,
) -> *mut Node {
    crate::parser::parse_coerce::coerce_to_boolean(_pstate as _, _node as _, _constructName) as _
}
unsafe fn assign_expr_collations(_pstate: *mut ParseState, _expr: *mut Node) {
    crate::parser::parse_collate::assign_expr_collations(_pstate as _, _expr as _)
}
unsafe fn pull_varattnos(_node: *mut Node, _varno: c_int, _varattnos: *mut *mut Bitmapset) {
    crate::optimizer::util::var::pull_varattnos(_node as _, _varno as _, _varattnos as _)
}
unsafe fn bms_is_member(_x: c_int, _a: *mut Bitmapset) -> bool {
    crate::nodes::bitmapset::bms_is_member(_x as _, _a as _)
}
unsafe fn bms_add_range(_a: *mut Bitmapset, _lower: c_int, _upper: c_int) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_add_range(_a as _, _lower as _, _upper as _) as _
}
unsafe fn bms_del_member(_a: *mut Bitmapset, _x: c_int) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_del_member(_a as _, _x as _) as _
}
unsafe fn bms_add_member(_a: *mut Bitmapset, _x: c_int) -> *mut Bitmapset {
    crate::nodes::bitmapset::bms_add_member(_a as _, _x as _) as _
}
unsafe fn bms_next_member(_a: *mut Bitmapset, _prevbit: c_int) -> c_int {
    crate::nodes::bitmapset::bms_next_member(_a as _, _prevbit as _) as _
}
unsafe fn get_attname(_relid: Oid, _attnum: AttrNumber, _missing_ok: bool) -> *mut c_char {
    crate::utils::cache::lsyscache::get_attname(_relid as _, _attnum as _, _missing_ok) as _
}
unsafe fn eval_const_expressions(_root: *mut c_void, _node: *mut Node) -> *mut Node {
    crate::optimizer::util::clauses::eval_const_expressions(_root as _, _node as _) as _
}
unsafe fn canonicalize_qual(_qual: *mut c_void, _is_check: bool) -> *mut c_void {
    crate::optimizer::prep::prepqual::canonicalize_qual(_qual as _, _is_check) as _
}
unsafe fn make_ands_implicit(_clause: *mut c_void) -> *mut List {
    crate::nodes::makefuncs::make_ands_implicit(_clause as _) as _
}
unsafe fn ExecCheckPermissions(
    _rangeTable: *mut List,
    _rteperminfos: *mut List,
    _ereport_on_violation: bool,
) -> bool {
    crate::executor::execMain::ExecCheckPermissions(
        _rangeTable as _, _rteperminfos as _, _ereport_on_violation,
    )
}
unsafe fn check_enable_rls(_relid: Oid, _checkAsUser: Oid, _noError: bool) -> c_int {
    crate::utils::misc::rls::check_enable_rls(_relid as _, _checkAsUser as _, _noError) as _
}
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    crate::utils::cache::lsyscache::get_namespace_name(_nspid as _) as _
}
unsafe fn makeRangeVar(
    _schemaname: *mut c_char,
    _relname: *mut c_char,
    _location: c_int,
) -> *mut RangeVar {
    crate::nodes::makefuncs::makeRangeVar(_schemaname as _, _relname as _, _location as _) as _
}
unsafe fn PreventCommandIfReadOnly(_cmdname: *const c_char) {
    crate::tcop::utility::PreventCommandIfReadOnly(_cmdname as _)
}
unsafe fn BeginCopyFrom(
    _pstate: *mut ParseState,
    _rel: Relation,
    _whereClause: *mut Node,
    _filename: *const c_char,
    _is_program: bool,
    _data_source_cb: copy_data_source_cb,
    _attnamelist: *mut List,
    _options: *mut List,
) -> CopyFromState {
    crate::commands::copyfrom::BeginCopyFrom(
        _pstate as _, _rel as _, _whereClause as _, _filename as _, _is_program,
        core::mem::transmute(_data_source_cb), _attnamelist as _, _options as _,
    ) as _
}
unsafe fn CopyFrom(_cstate: CopyFromState) -> uint64 {
    crate::commands::copyfrom::CopyFrom(_cstate as _) as _
}
unsafe fn EndCopyFrom(_cstate: CopyFromState) {
    crate::commands::copyfrom::EndCopyFrom(_cstate as _)
}
unsafe fn BeginCopyTo(
    _pstate: *mut ParseState,
    _rel: Relation,
    _raw_query: *mut RawStmt,
    _queryRelId: Oid,
    _filename: *const c_char,
    _is_program: bool,
    _data_dest_cb: copy_data_dest_cb,
    _attnamelist: *mut List,
    _options: *mut List,
) -> CopyToState {
    crate::commands::copyto::BeginCopyTo(
        _pstate as _, _rel as _, _raw_query as _, _queryRelId as _, _filename as _, _is_program,
        core::mem::transmute(_data_dest_cb), _attnamelist as _, _options as _,
    ) as _
}
unsafe fn DoCopyTo(_cstate: CopyToState) -> uint64 {
    crate::commands::copyto::DoCopyTo(_cstate as _) as _
}
unsafe fn EndCopyTo(_cstate: CopyToState) {
    crate::commands::copyto::EndCopyTo(_cstate as _)
}
unsafe fn defGetString(_def: *mut DefElem) -> *mut c_char {
    crate::commands::define::defGetString(_def as _) as _
}
unsafe fn defGetBoolean(_def: *mut DefElem) -> bool {
    crate::commands::define::defGetBoolean(_def as _)
}
unsafe fn defGetInt64(_def: *mut DefElem) -> int64 {
    crate::commands::define::defGetInt64(_def as _) as _
}
unsafe fn errorConflictingDefElem(_defel: *mut DefElem, _pstate: *mut ParseState) {
    crate::commands::define::errorConflictingDefElem(_defel as _, _pstate as _)
}
unsafe fn parser_errposition(_pstate: *mut ParseState, _location: c_int) -> c_int {
    crate::parser::parse_node::parser_errposition(_pstate as _, _location as _) as _
}
unsafe fn pg_char_to_encoding(_name: *const c_char) -> c_int {
    crate::common::encnames::pg_char_to_encoding(_name as _) as _
}
unsafe fn pg_strtoint64(_s: *const c_char) -> int64 {
    crate::utils::adt::numutils::pg_strtoint64(_s as _) as _
}
unsafe fn TupleDescCompactAttr(_tupdesc: TupleDesc, _i: c_int) -> *mut CompactAttribute {
    crate::access::common::tupdesc::TupleDescCompactAttr(_tupdesc as _, _i as _) as _
}
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    crate::access::common::tupdesc::TupleDescAttr(_tupdesc as _, _i as _) as _
}
unsafe fn namestrcmp(_name: *mut NameData, _str: *const c_char) -> c_int {
    crate::utils::adt::name::namestrcmp(_name as _, _str as _) as _
}

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn pg_strcasecmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

/* Canonical node/attribute types (re-exported to keep field layouts correct). */
pub use crate::parser::parse_node::ParseNamespaceItem;
pub use crate::nodes::parsenodes::RTEPermissionInfo;
pub use crate::access::common::tupdesc::CompactAttribute;
pub use crate::catalog::pg_attribute::{FormData_pg_attribute, Form_pg_attribute};
pub use crate::c::NameData;

/* ===================================================================
 * copy.c implementation
 * =================================================================== */

/*
 *	 DoCopy executes the SQL COPY statement
 *
 * Either unload or reload contents of table <relation>, depending on <from>.
 * (<from> = true means we are inserting into the table.)  In the "TO" case
 * we also support copying the output of an arbitrary SELECT, INSERT, UPDATE
 * or DELETE query.
 *
 * If <pipe> is false, transfer is between the table and the file named
 * <filename>.  Otherwise, transfer is between the table and our regular
 * input/output stream. The latter could be either stdin/stdout or a
 * socket, depending on whether we're running under Postmaster control.
 *
 * Do not allow a Postgres user without the 'pg_read_server_files' or
 * 'pg_write_server_files' role to read from or write to a file.
 *
 * Do not allow the copy if user doesn't have proper permission to access
 * the table or the specifically requested columns.
 */
pub unsafe fn DoCopy(
    pstate: *mut ParseState,
    stmt: *const CopyStmt,
    stmt_location: c_int,
    stmt_len: c_int,
    processed: *mut uint64,
) {
    let is_from: bool = (*stmt).is_from;
    let pipe: bool = (*stmt).filename.is_null();
    let mut rel: Relation;
    let relid: Oid;
    let mut query: *mut RawStmt = std::ptr::null_mut();
    let mut whereClause: *mut Node = std::ptr::null_mut();

    /*
     * Disallow COPY to/from file or program except to users with the
     * appropriate role.
     */
    if !pipe {
        if (*stmt).is_program {
            if !has_privs_of_role(GetUserId(), ROLE_PG_EXECUTE_SERVER_PROGRAM) {
                ereport!(
                    ERROR,
                    "permission denied to COPY to or from an external program"
                );
            }
        } else {
            if is_from && !has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES) {
                ereport!(ERROR, "permission denied to COPY from a file");
            }

            if !is_from && !has_privs_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES) {
                ereport!(ERROR, "permission denied to COPY to a file");
            }
        }
    }

    if !(*stmt).relation.is_null() {
        let lockmode: LOCKMODE = if is_from {
            RowExclusiveLock
        } else {
            AccessShareLock
        };
        let nsitem: *mut ParseNamespaceItem;
        let perminfo: *mut RTEPermissionInfo;
        let tupDesc: TupleDesc;
        let attnums: *mut List;
        let cur: *mut ListCell;

        Assert!((*stmt).query.is_null());

        /* Open and lock the relation, using the appropriate lock type. */
        rel = table_openrv((*stmt).relation, lockmode);

        relid = RelationGetRelid(rel);

        nsitem = addRangeTableEntryForRelation(
            pstate,
            rel,
            lockmode,
            std::ptr::null_mut(),
            false,
            false,
        );

        perminfo = (*nsitem).p_perminfo as *mut RTEPermissionInfo;
        (*perminfo).requiredPerms = if is_from { ACL_INSERT } else { ACL_SELECT };

        if !(*stmt).whereClause.is_null() {
            let mut expr_attrs: *mut Bitmapset = std::ptr::null_mut();
            let mut i: c_int;

            /* add nsitem to query namespace */
            addNSItemToQuery(pstate, nsitem, false, true, true);

            /* Transform the raw expression tree */
            whereClause = transformExpr(pstate, (*stmt).whereClause, EXPR_KIND_COPY_WHERE);

            /* Make sure it yields a boolean result. */
            whereClause = coerce_to_boolean(pstate, whereClause, c"WHERE".as_ptr());

            /* we have to fix its collations too */
            assign_expr_collations(pstate, whereClause);

            /*
             * Examine all the columns in the WHERE clause expression.  When
             * the whole-row reference is present, examine all the columns of
             * the table.
             */
            pull_varattnos(whereClause, 1, &mut expr_attrs);
            if bms_is_member(0 - FirstLowInvalidHeapAttributeNumber as i32, expr_attrs) {
                expr_attrs = bms_add_range(
                    expr_attrs,
                    1 - FirstLowInvalidHeapAttributeNumber as i32,
                    RelationGetNumberOfAttributes(rel) - FirstLowInvalidHeapAttributeNumber as i32,
                );
                expr_attrs = bms_del_member(expr_attrs, 0 - FirstLowInvalidHeapAttributeNumber as i32);
            }

            i = -1;
            loop {
                i = bms_next_member(expr_attrs, i);
                if i < 0 {
                    break;
                }
                let attno: AttrNumber = (i + FirstLowInvalidHeapAttributeNumber as i32) as AttrNumber;

                Assert!(attno != 0);

                /*
                 * Prohibit generated columns in the WHERE clause.  Stored
                 * generated columns are not yet computed when the filtering
                 * happens.  Virtual generated columns could probably work (we
                 * would need to expand them somewhere around here), but for
                 * now we keep them consistent with the stored variant.
                 */
                if (*TupleDescAttr(RelationGetDescr(rel), (attno - 1) as c_int)).attgenerated != 0 {
                    elog!(
                        ERROR,
                        "generated columns are not supported in COPY FROM WHERE conditions"
                    );
                }
            }

            whereClause = eval_const_expressions(std::ptr::null_mut(), whereClause);

            whereClause = canonicalize_qual(whereClause as *mut c_void, false) as *mut Node;
            whereClause = make_ands_implicit(whereClause as *mut c_void) as *mut Node;
        }

        tupDesc = RelationGetDescr(rel);
        attnums = CopyGetAttnums(tupDesc, rel, (*stmt).attlist);
        foreach!(cur, attnums, {
            let attno: c_int;
            let bms: *mut *mut Bitmapset;

            attno = lfirst_int(current_cell!(cur)) - FirstLowInvalidHeapAttributeNumber as i32;
            bms = if is_from {
                &mut (*perminfo).insertedCols as *mut _ as *mut *mut Bitmapset
            } else {
                &mut (*perminfo).selectedCols as *mut _ as *mut *mut Bitmapset
            };

            *bms = bms_add_member(*bms, attno);
        });
        ExecCheckPermissions((*pstate).p_rtable, list_make1!(perminfo as *mut c_void), true);

        /*
         * Permission check for row security policies.
         *
         * check_enable_rls will ereport(ERROR) if the user has requested
         * something invalid and will otherwise indicate if we should enable
         * RLS (returns RLS_ENABLED) or not for this COPY statement.
         *
         * If the relation has a row security policy and we are to apply it
         * then perform a "query" copy and allow the normal query processing
         * to handle the policies.
         *
         * If RLS is not enabled for this, then just fall through to the
         * normal non-filtering relation handling.
         */
        if check_enable_rls(relid, InvalidOid, false) == RLS_ENABLED {
            let select: *mut SelectStmt;
            let mut cr: *mut ColumnRef;
            let mut target: *mut ResTarget;
            let from: *mut RangeVar;
            let mut targetList: *mut List = NIL;

            if is_from {
                ereport!(ERROR, "COPY FROM not supported with row-level security");
            }

            /*
             * Build target list
             *
             * If no columns are specified in the attribute list of the COPY
             * command, then the target list is 'all' columns. Therefore, '*'
             * should be used as the target list for the resulting SELECT
             * statement.
             *
             * In the case that columns are specified in the attribute list,
             * create a ColumnRef and ResTarget for each column and add them
             * to the target list for the resulting SELECT statement.
             */
            if (*stmt).attlist.is_null() {
                cr = makeNode!(ColumnRef, T_ColumnRef);
                (*cr).fields = list_make1!(makeNode!(A_Star, T_A_Star) as *mut c_void);
                (*cr).location = -1;

                target = makeNode!(ResTarget, T_ResTarget);
                (*target).name = std::ptr::null_mut();
                (*target).indirection = NIL;
                (*target).val = cr as *mut Node;
                (*target).location = -1;

                targetList = list_make1!(target as *mut c_void);
            } else {
                let lc: *mut ListCell;

                foreach!(lc, (*stmt).attlist, {
                    /*
                     * Build the ColumnRef for each column.  The ColumnRef
                     * 'fields' property is a String node that corresponds to
                     * the column name respectively.
                     */
                    cr = makeNode!(ColumnRef, T_ColumnRef);
                    (*cr).fields = list_make1!(lfirst(current_cell!(lc)));
                    (*cr).location = -1;

                    /* Build the ResTarget and add the ColumnRef to it. */
                    target = makeNode!(ResTarget, T_ResTarget);
                    (*target).name = std::ptr::null_mut();
                    (*target).indirection = NIL;
                    (*target).val = cr as *mut Node;
                    (*target).location = -1;

                    /* Add each column to the SELECT statement's target list */
                    targetList = lappend(targetList, target as *mut c_void);
                });
            }

            /*
             * Build RangeVar for from clause, fully qualified based on the
             * relation which we have opened and locked.  Use "ONLY" so that
             * COPY retrieves rows from only the target table not any
             * inheritance children, the same as when RLS doesn't apply.
             */
            from = makeRangeVar(
                get_namespace_name(RelationGetNamespace(rel)),
                pstrdup(RelationGetRelationName(rel)),
                -1,
            );
            (*from).inh = false; /* apply ONLY */

            /* Build query */
            select = makeNode!(SelectStmt, T_SelectStmt);
            (*select).targetList = targetList;
            (*select).fromClause = list_make1!(from as *mut c_void);

            query = makeNode!(RawStmt, T_RawStmt);
            (*query).stmt = select as *mut Node;
            (*query).stmt_location = stmt_location;
            (*query).stmt_len = stmt_len;

            /*
             * Close the relation for now, but keep the lock on it to prevent
             * changes between now and when we start the query-based COPY.
             *
             * We'll reopen it later as part of the query-based COPY.
             */
            table_close(rel, NoLock);
            rel = std::ptr::null_mut();
        }
    } else {
        Assert!(!(*stmt).query.is_null());

        query = makeNode!(RawStmt, T_RawStmt);
        (*query).stmt = (*stmt).query;
        (*query).stmt_location = stmt_location;
        (*query).stmt_len = stmt_len;

        relid = InvalidOid;
        rel = std::ptr::null_mut();
    }

    if is_from {
        let cstate: CopyFromState;

        Assert!(!rel.is_null());

        /* check read-only transaction and parallel mode */
        if XactReadOnly && !(*rel).rd_islocaltemp {
            PreventCommandIfReadOnly(c"COPY FROM".as_ptr());
        }

        cstate = BeginCopyFrom(
            pstate,
            rel,
            whereClause,
            (*stmt).filename,
            (*stmt).is_program,
            None,
            (*stmt).attlist,
            (*stmt).options,
        );
        *processed = CopyFrom(cstate); /* copy from file to database */
        EndCopyFrom(cstate);
    } else {
        let cstate: CopyToState;

        cstate = BeginCopyTo(
            pstate,
            rel,
            query,
            relid,
            (*stmt).filename,
            (*stmt).is_program,
            None,
            (*stmt).attlist,
            (*stmt).options,
        );
        *processed = DoCopyTo(cstate); /* copy from database to file */
        EndCopyTo(cstate);
    }

    if !rel.is_null() {
        table_close(rel, NoLock);
    }
}

/*
 * Extract a CopyHeaderChoice value from a DefElem.  This is like
 * defGetBoolean() but also accepts the special value "match".
 */
unsafe fn defGetCopyHeaderChoice(def: *mut DefElem, is_from: bool) -> CopyHeaderChoice {
    /*
     * If no parameter value given, assume "true" is meant.
     */
    if (*def).arg.is_null() {
        return COPY_HEADER_TRUE;
    }

    /*
     * Allow 0, 1, "true", "false", "on", "off", or "match".
     */
    match nodeTag((*def).arg as *mut Node) {
        T_Integer => match intVal!((*def).arg) {
            0 => return COPY_HEADER_FALSE,
            1 => return COPY_HEADER_TRUE,
            _ => {
                /* otherwise, error out below */
            }
        },
        _ => {
            let sval: *mut c_char = defGetString(def);

            /*
             * The set of strings accepted here should match up with the
             * grammar's opt_boolean_or_string production.
             */
            if pg_strcasecmp(sval, c"true".as_ptr()) == 0 {
                return COPY_HEADER_TRUE;
            }
            if pg_strcasecmp(sval, c"false".as_ptr()) == 0 {
                return COPY_HEADER_FALSE;
            }
            if pg_strcasecmp(sval, c"on".as_ptr()) == 0 {
                return COPY_HEADER_TRUE;
            }
            if pg_strcasecmp(sval, c"off".as_ptr()) == 0 {
                return COPY_HEADER_FALSE;
            }
            if pg_strcasecmp(sval, c"match".as_ptr()) == 0 {
                if !is_from {
                    elog!(ERROR, "cannot use \"{}\" with HEADER in COPY TO", "match");
                }
                return COPY_HEADER_MATCH;
            }
        }
    }
    elog!(
        ERROR,
        "{} requires a Boolean value or \"match\"",
        "defname"
    );
    #[allow(unreachable_code)]
    COPY_HEADER_FALSE /* keep compiler quiet */
}

/*
 * Extract a CopyOnErrorChoice value from a DefElem.
 */
unsafe fn defGetCopyOnErrorChoice(
    def: *mut DefElem,
    pstate: *mut ParseState,
    is_from: bool,
) -> CopyOnErrorChoice {
    let sval: *mut c_char = defGetString(def);

    if !is_from {
        let _ = parser_errposition(pstate, (*def).location);
        elog!(ERROR, "COPY {} cannot be used with {}", "ON_ERROR", "COPY TO");
    }

    /*
     * Allow "stop", or "ignore" values.
     */
    if pg_strcasecmp(sval, c"stop".as_ptr()) == 0 {
        return COPY_ON_ERROR_STOP;
    }
    if pg_strcasecmp(sval, c"ignore".as_ptr()) == 0 {
        return COPY_ON_ERROR_IGNORE;
    }

    let _ = parser_errposition(pstate, (*def).location);
    elog!(ERROR, "COPY {} \"{}\" not recognized", "ON_ERROR", "sval");
    #[allow(unreachable_code)]
    COPY_ON_ERROR_STOP /* keep compiler quiet */
}

/*
 * Extract REJECT_LIMIT value from a DefElem.
 *
 * REJECT_LIMIT can be specified in two ways: as an int64 for the COPY command
 * option or as a single-quoted string for the foreign table option using
 * file_fdw. Therefore this function needs to handle both formats.
 */
unsafe fn defGetCopyRejectLimitOption(def: *mut DefElem) -> int64 {
    let mut reject_limit: int64 = 0;

    if (*def).arg.is_null() {
        elog!(ERROR, "{} requires a numeric value", "defname");
    } else if nodeTag((*def).arg as *mut Node) == T_String {
        reject_limit = pg_strtoint64(strVal!((*def).arg));
    } else {
        reject_limit = defGetInt64(def);
    }

    if reject_limit <= 0 {
        elog!(
            ERROR,
            "REJECT_LIMIT ({}) must be greater than zero",
            reject_limit
        );
    }

    reject_limit
}

/*
 * Extract a CopyLogVerbosityChoice value from a DefElem.
 */
unsafe fn defGetCopyLogVerbosityChoice(
    def: *mut DefElem,
    pstate: *mut ParseState,
) -> CopyLogVerbosityChoice {
    let sval: *mut c_char;

    /*
     * Allow "silent", "default", or "verbose" values.
     */
    sval = defGetString(def);
    if pg_strcasecmp(sval, c"silent".as_ptr()) == 0 {
        return COPY_LOG_VERBOSITY_SILENT;
    }
    if pg_strcasecmp(sval, c"default".as_ptr()) == 0 {
        return COPY_LOG_VERBOSITY_DEFAULT;
    }
    if pg_strcasecmp(sval, c"verbose".as_ptr()) == 0 {
        return COPY_LOG_VERBOSITY_VERBOSE;
    }

    let _ = parser_errposition(pstate, (*def).location);
    elog!(ERROR, "COPY {} \"{}\" not recognized", "LOG_VERBOSITY", "sval");
    #[allow(unreachable_code)]
    COPY_LOG_VERBOSITY_DEFAULT /* keep compiler quiet */
}

/*
 * Process the statement option list for COPY.
 *
 * Scan the options list (a list of DefElem) and transpose the information
 * into *opts_out, applying appropriate error checking.
 *
 * If 'opts_out' is not NULL, it is assumed to be filled with zeroes initially.
 *
 * This is exported so that external users of the COPY API can sanity-check
 * a list of options.  In that usage, 'opts_out' can be passed as NULL and
 * the collected data is just leaked until CurrentMemoryContext is reset.
 *
 * Note that additional checking, such as whether column names listed in FORCE
 * QUOTE actually exist, has to be applied later.  This just checks for
 * self-consistency of the options list.
 */
pub unsafe fn ProcessCopyOptions(
    pstate: *mut ParseState,
    mut opts_out: *mut CopyFormatOptions,
    is_from: bool,
    options: *mut List,
) {
    let mut format_specified: bool = false;
    let mut freeze_specified: bool = false;
    let mut header_specified: bool = false;
    let mut on_error_specified: bool = false;
    let mut log_verbosity_specified: bool = false;
    let mut reject_limit_specified: bool = false;
    let option: *mut ListCell;

    /* Support external use for option sanity checking */
    if opts_out.is_null() {
        opts_out = palloc0(std::mem::size_of::<CopyFormatOptions>()) as *mut CopyFormatOptions;
    }

    (*opts_out).file_encoding = -1;

    /* Extract options from the statement node tree */
    foreach!(option, options, {
        let defel: *mut DefElem = lfirst_node!(DefElem, T_DefElem, current_cell!(option));

        if strcmp((*defel).defname, c"format".as_ptr()) == 0 {
            let fmt: *mut c_char = defGetString(defel);

            if format_specified {
                errorConflictingDefElem(defel, pstate);
            }
            format_specified = true;
            if strcmp(fmt, c"text".as_ptr()) == 0 {
                /* default format */
            } else if strcmp(fmt, c"csv".as_ptr()) == 0 {
                (*opts_out).csv_mode = true;
            } else if strcmp(fmt, c"binary".as_ptr()) == 0 {
                (*opts_out).binary = true;
            } else {
                let _ = parser_errposition(pstate, (*defel).location);
                elog!(ERROR, "COPY format \"{}\" not recognized", "fmt");
            }
        } else if strcmp((*defel).defname, c"freeze".as_ptr()) == 0 {
            if freeze_specified {
                errorConflictingDefElem(defel, pstate);
            }
            freeze_specified = true;
            (*opts_out).freeze = defGetBoolean(defel);
        } else if strcmp((*defel).defname, c"delimiter".as_ptr()) == 0 {
            if !(*opts_out).delim.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts_out).delim = defGetString(defel);
        } else if strcmp((*defel).defname, c"null".as_ptr()) == 0 {
            if !(*opts_out).null_print.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts_out).null_print = defGetString(defel);
        } else if strcmp((*defel).defname, c"default".as_ptr()) == 0 {
            if !(*opts_out).default_print.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts_out).default_print = defGetString(defel);
        } else if strcmp((*defel).defname, c"header".as_ptr()) == 0 {
            if header_specified {
                errorConflictingDefElem(defel, pstate);
            }
            header_specified = true;
            (*opts_out).header_line = defGetCopyHeaderChoice(defel, is_from);
        } else if strcmp((*defel).defname, c"quote".as_ptr()) == 0 {
            if !(*opts_out).quote.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts_out).quote = defGetString(defel);
        } else if strcmp((*defel).defname, c"escape".as_ptr()) == 0 {
            if !(*opts_out).escape.is_null() {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts_out).escape = defGetString(defel);
        } else if strcmp((*defel).defname, c"force_quote".as_ptr()) == 0 {
            if !(*opts_out).force_quote.is_null() || (*opts_out).force_quote_all {
                errorConflictingDefElem(defel, pstate);
            }
            if !(*defel).arg.is_null() && IsA!((*defel).arg, T_A_Star) {
                (*opts_out).force_quote_all = true;
            } else if !(*defel).arg.is_null() && IsA!((*defel).arg, T_List) {
                (*opts_out).force_quote = castNode!(List, T_List, (*defel).arg);
            } else {
                let _ = parser_errposition(pstate, (*defel).location);
                elog!(
                    ERROR,
                    "argument to option \"{}\" must be a list of column names",
                    "defname"
                );
            }
        } else if strcmp((*defel).defname, c"force_not_null".as_ptr()) == 0 {
            if !(*opts_out).force_notnull.is_null() || (*opts_out).force_notnull_all {
                errorConflictingDefElem(defel, pstate);
            }
            if !(*defel).arg.is_null() && IsA!((*defel).arg, T_A_Star) {
                (*opts_out).force_notnull_all = true;
            } else if !(*defel).arg.is_null() && IsA!((*defel).arg, T_List) {
                (*opts_out).force_notnull = castNode!(List, T_List, (*defel).arg);
            } else {
                let _ = parser_errposition(pstate, (*defel).location);
                elog!(
                    ERROR,
                    "argument to option \"{}\" must be a list of column names",
                    "defname"
                );
            }
        } else if strcmp((*defel).defname, c"force_null".as_ptr()) == 0 {
            if !(*opts_out).force_null.is_null() || (*opts_out).force_null_all {
                errorConflictingDefElem(defel, pstate);
            }
            if !(*defel).arg.is_null() && IsA!((*defel).arg, T_A_Star) {
                (*opts_out).force_null_all = true;
            } else if !(*defel).arg.is_null() && IsA!((*defel).arg, T_List) {
                (*opts_out).force_null = castNode!(List, T_List, (*defel).arg);
            } else {
                let _ = parser_errposition(pstate, (*defel).location);
                elog!(
                    ERROR,
                    "argument to option \"{}\" must be a list of column names",
                    "defname"
                );
            }
        } else if strcmp((*defel).defname, c"convert_selectively".as_ptr()) == 0 {
            /*
             * Undocumented, not-accessible-from-SQL option: convert only the
             * named columns to binary form, storing the rest as NULLs. It's
             * allowed for the column list to be NIL.
             */
            if (*opts_out).convert_selectively {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts_out).convert_selectively = true;
            if (*defel).arg.is_null() || IsA!((*defel).arg, T_List) {
                (*opts_out).convert_select = castNode!(List, T_List, (*defel).arg);
            } else {
                let _ = parser_errposition(pstate, (*defel).location);
                elog!(
                    ERROR,
                    "argument to option \"{}\" must be a list of column names",
                    "defname"
                );
            }
        } else if strcmp((*defel).defname, c"encoding".as_ptr()) == 0 {
            if (*opts_out).file_encoding >= 0 {
                errorConflictingDefElem(defel, pstate);
            }
            (*opts_out).file_encoding = pg_char_to_encoding(defGetString(defel));
            if (*opts_out).file_encoding < 0 {
                let _ = parser_errposition(pstate, (*defel).location);
                elog!(
                    ERROR,
                    "argument to option \"{}\" must be a valid encoding name",
                    "defname"
                );
            }
        } else if strcmp((*defel).defname, c"on_error".as_ptr()) == 0 {
            if on_error_specified {
                errorConflictingDefElem(defel, pstate);
            }
            on_error_specified = true;
            (*opts_out).on_error = defGetCopyOnErrorChoice(defel, pstate, is_from);
        } else if strcmp((*defel).defname, c"log_verbosity".as_ptr()) == 0 {
            if log_verbosity_specified {
                errorConflictingDefElem(defel, pstate);
            }
            log_verbosity_specified = true;
            (*opts_out).log_verbosity = defGetCopyLogVerbosityChoice(defel, pstate);
        } else if strcmp((*defel).defname, c"reject_limit".as_ptr()) == 0 {
            if reject_limit_specified {
                errorConflictingDefElem(defel, pstate);
            }
            reject_limit_specified = true;
            (*opts_out).reject_limit = defGetCopyRejectLimitOption(defel);
        } else {
            let _ = parser_errposition(pstate, (*defel).location);
            elog!(ERROR, "option \"{}\" not recognized", "defname");
        }
    });

    /*
     * Check for incompatible options (must do these three before inserting
     * defaults)
     */
    if (*opts_out).binary && !(*opts_out).delim.is_null() {
        elog!(ERROR, "cannot specify {} in BINARY mode", "DELIMITER");
    }

    if (*opts_out).binary && !(*opts_out).null_print.is_null() {
        elog!(ERROR, "cannot specify {} in BINARY mode", "NULL");
    }

    if (*opts_out).binary && !(*opts_out).default_print.is_null() {
        elog!(ERROR, "cannot specify {} in BINARY mode", "DEFAULT");
    }

    /* Set defaults for omitted options */
    if (*opts_out).delim.is_null() {
        (*opts_out).delim = if (*opts_out).csv_mode {
            c",".as_ptr() as *mut c_char
        } else {
            c"\t".as_ptr() as *mut c_char
        };
    }

    if (*opts_out).null_print.is_null() {
        (*opts_out).null_print = if (*opts_out).csv_mode {
            c"".as_ptr() as *mut c_char
        } else {
            c"\\N".as_ptr() as *mut c_char
        };
    }
    (*opts_out).null_print_len = strlen((*opts_out).null_print) as c_int;

    if (*opts_out).csv_mode {
        if (*opts_out).quote.is_null() {
            (*opts_out).quote = c"\"".as_ptr() as *mut c_char;
        }
        if (*opts_out).escape.is_null() {
            (*opts_out).escape = (*opts_out).quote;
        }
    }

    /* Only single-byte delimiter strings are supported. */
    if strlen((*opts_out).delim) != 1 {
        ereport!(ERROR, "COPY delimiter must be a single one-byte character");
    }

    /* Disallow end-of-line characters */
    if !strchr((*opts_out).delim, '\r' as c_int).is_null()
        || !strchr((*opts_out).delim, '\n' as c_int).is_null()
    {
        ereport!(
            ERROR,
            "COPY delimiter cannot be newline or carriage return"
        );
    }

    if !strchr((*opts_out).null_print, '\r' as c_int).is_null()
        || !strchr((*opts_out).null_print, '\n' as c_int).is_null()
    {
        ereport!(
            ERROR,
            "COPY null representation cannot use newline or carriage return"
        );
    }

    if !(*opts_out).default_print.is_null() {
        (*opts_out).default_print_len = strlen((*opts_out).default_print) as c_int;

        if !strchr((*opts_out).default_print, '\r' as c_int).is_null()
            || !strchr((*opts_out).default_print, '\n' as c_int).is_null()
        {
            ereport!(
                ERROR,
                "COPY default representation cannot use newline or carriage return"
            );
        }
    }

    /*
     * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
     * backslash because it would be ambiguous.  We can't allow the other
     * cases because data characters matching the delimiter must be
     * backslashed, and certain backslash combinations are interpreted
     * non-literally by COPY IN.  Disallowing all lower case ASCII letters is
     * more than strictly necessary, but seems best for consistency and
     * future-proofing.  Likewise we disallow all digits though only octal
     * digits are actually dangerous.
     */
    if !(*opts_out).csv_mode
        && !strchr(
            c"\\.abcdefghijklmnopqrstuvwxyz0123456789".as_ptr(),
            *(*opts_out).delim as c_int,
        )
        .is_null()
    {
        elog!(ERROR, "COPY delimiter cannot be \"{}\"", "delim");
    }

    /* Check header */
    if (*opts_out).binary && (*opts_out).header_line != COPY_HEADER_FALSE {
        elog!(ERROR, "cannot specify {} in BINARY mode", "HEADER");
    }

    /* Check quote */
    if !(*opts_out).csv_mode && !(*opts_out).quote.is_null() {
        elog!(ERROR, "COPY {} requires CSV mode", "QUOTE");
    }

    if (*opts_out).csv_mode && strlen((*opts_out).quote) != 1 {
        ereport!(ERROR, "COPY quote must be a single one-byte character");
    }

    if (*opts_out).csv_mode && *(*opts_out).delim == *(*opts_out).quote {
        ereport!(ERROR, "COPY delimiter and quote must be different");
    }

    /* Check escape */
    if !(*opts_out).csv_mode && !(*opts_out).escape.is_null() {
        elog!(ERROR, "COPY {} requires CSV mode", "ESCAPE");
    }

    if (*opts_out).csv_mode && strlen((*opts_out).escape) != 1 {
        ereport!(ERROR, "COPY escape must be a single one-byte character");
    }

    /* Check force_quote */
    if !(*opts_out).csv_mode && (!(*opts_out).force_quote.is_null() || (*opts_out).force_quote_all) {
        elog!(ERROR, "COPY {} requires CSV mode", "FORCE_QUOTE");
    }
    if (!(*opts_out).force_quote.is_null() || (*opts_out).force_quote_all) && is_from {
        elog!(
            ERROR,
            "COPY {} cannot be used with {}",
            "FORCE_QUOTE",
            "COPY FROM"
        );
    }

    /* Check force_notnull */
    if !(*opts_out).csv_mode
        && ((*opts_out).force_notnull != NIL || (*opts_out).force_notnull_all)
    {
        elog!(ERROR, "COPY {} requires CSV mode", "FORCE_NOT_NULL");
    }
    if ((*opts_out).force_notnull != NIL || (*opts_out).force_notnull_all) && !is_from {
        elog!(
            ERROR,
            "COPY {} cannot be used with {}",
            "FORCE_NOT_NULL",
            "COPY TO"
        );
    }

    /* Check force_null */
    if !(*opts_out).csv_mode && ((*opts_out).force_null != NIL || (*opts_out).force_null_all) {
        elog!(ERROR, "COPY {} requires CSV mode", "FORCE_NULL");
    }

    if ((*opts_out).force_null != NIL || (*opts_out).force_null_all) && !is_from {
        elog!(
            ERROR,
            "COPY {} cannot be used with {}",
            "FORCE_NULL",
            "COPY TO"
        );
    }

    /* Don't allow the delimiter to appear in the null string. */
    if !strchr((*opts_out).null_print, *(*opts_out).delim as c_int).is_null() {
        elog!(
            ERROR,
            "COPY delimiter character must not appear in the {} specification",
            "NULL"
        );
    }

    /* Don't allow the CSV quote char to appear in the null string. */
    if (*opts_out).csv_mode
        && !strchr((*opts_out).null_print, *(*opts_out).quote as c_int).is_null()
    {
        elog!(
            ERROR,
            "CSV quote character must not appear in the {} specification",
            "NULL"
        );
    }

    /* Check freeze */
    if (*opts_out).freeze && !is_from {
        elog!(ERROR, "COPY {} cannot be used with {}", "FREEZE", "COPY TO");
    }

    if !(*opts_out).default_print.is_null() {
        if !is_from {
            elog!(
                ERROR,
                "COPY {} cannot be used with {}",
                "DEFAULT",
                "COPY TO"
            );
        }

        /* Don't allow the delimiter to appear in the default string. */
        if !strchr((*opts_out).default_print, *(*opts_out).delim as c_int).is_null() {
            elog!(
                ERROR,
                "COPY delimiter character must not appear in the {} specification",
                "DEFAULT"
            );
        }

        /* Don't allow the CSV quote char to appear in the default string. */
        if (*opts_out).csv_mode
            && !strchr((*opts_out).default_print, *(*opts_out).quote as c_int).is_null()
        {
            elog!(
                ERROR,
                "CSV quote character must not appear in the {} specification",
                "DEFAULT"
            );
        }

        /* Don't allow the NULL and DEFAULT string to be the same */
        if (*opts_out).null_print_len == (*opts_out).default_print_len
            && strncmp(
                (*opts_out).null_print,
                (*opts_out).default_print,
                (*opts_out).null_print_len as usize,
            ) == 0
        {
            ereport!(
                ERROR,
                "NULL specification and DEFAULT specification cannot be the same"
            );
        }
    }
    /* Check on_error */
    if (*opts_out).binary && (*opts_out).on_error != COPY_ON_ERROR_STOP {
        ereport!(ERROR, "only ON_ERROR STOP is allowed in BINARY mode");
    }

    if (*opts_out).reject_limit != 0 && (*opts_out).on_error == COPY_ON_ERROR_STOP {
        elog!(
            ERROR,
            "COPY {} requires {} to be set to {}",
            "REJECT_LIMIT",
            "ON_ERROR",
            "IGNORE"
        );
    }
}

/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * We don't include generated columns in the generated full list and we don't
 * allow them to be specified explicitly.  They don't make sense for COPY
 * FROM, but we could possibly allow them for COPY TO.  But this way it's at
 * least ensured that whatever we copy out can be copied back in.
 *
 * rel can be NULL ... it's only used for error reports.
 */
pub unsafe fn CopyGetAttnums(
    tupDesc: TupleDesc,
    rel: Relation,
    attnamelist: *mut List,
) -> *mut List {
    let mut attnums: *mut List = NIL;

    if attnamelist == NIL {
        /* Generate default column list */
        let attr_count: c_int = (*tupDesc).natts;
        let mut i: c_int;

        i = 0;
        while i < attr_count {
            let attr: *mut CompactAttribute = TupleDescCompactAttr(tupDesc, i);

            if (*attr).attisdropped || (*attr).attgenerated {
                i += 1;
                continue;
            }
            attnums = lappend_int(attnums, i + 1);
            i += 1;
        }
    } else {
        /* Validate the user-supplied list and extract attnums */
        let l: *mut ListCell;

        foreach!(l, attnamelist, {
            let name: *mut c_char = strVal!(lfirst(current_cell!(l)));
            let mut attnum: c_int;
            let mut i: c_int;

            /* Lookup column name */
            attnum = InvalidAttrNumber as c_int;
            i = 0;
            while i < (*tupDesc).natts {
                let att: Form_pg_attribute = TupleDescAttr(tupDesc, i);

                if (*att).attisdropped {
                    i += 1;
                    continue;
                }
                if namestrcmp(&mut (*att).attname, name) == 0 {
                    if (*att).attgenerated != 0 {
                        elog!(ERROR, "column \"{}\" is a generated column", "name");
                    }
                    attnum = (*att).attnum as c_int;
                    break;
                }
                i += 1;
            }
            if attnum == InvalidAttrNumber as c_int {
                if !rel.is_null() {
                    elog!(
                        ERROR,
                        "column \"{}\" of relation \"{}\" does not exist",
                        "name",
                        "rel"
                    );
                } else {
                    elog!(ERROR, "column \"{}\" does not exist", "name");
                }
            }
            /* Check for duplicates */
            if list_member_int(attnums, attnum) {
                elog!(ERROR, "column \"{}\" specified more than once", "name");
            }
            attnums = lappend_int(attnums, attnum);
        });
    }

    attnums
}

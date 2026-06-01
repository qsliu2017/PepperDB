//! src/backend/commands/view.c
//!
//! use rewrite rules to construct views
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::postgres_ext::Oid;
use crate::nodes::pg_list::*;
use crate::list_make1;
use crate::nodes::nodes::*;
use crate::{foreach, current_cell, makeNode, IsA, lfirst_node, castNode};

// ---------------------------------------------------------------------------
// Local type aliases / stubs for as-yet-unported dependencies.
// ---------------------------------------------------------------------------

type LOCKMODE = c_int;

// ---------------------------------------------------------------------------
// DefineVirtualRelation
//
// Create a view relation and use the rules system to store the query
// for the view.
//
// EventTriggerAlterTableStart must have been called already.
// ---------------------------------------------------------------------------
unsafe fn DefineVirtualRelation(
    relation: *mut RangeVar,
    tlist: *mut List,
    replace: bool,
    options: *mut List,
    viewParse: *mut Query,
) -> ObjectAddress {
    let viewOid: Oid;
    let lockmode: LOCKMODE;
    let mut attrList: *mut List;
    // ListCell *t  -- provided by foreach! below

    /*
     * create a list of ColumnDef nodes based on the names and types of the
     * (non-junk) targetlist items from the view's SELECT list.
     */
    attrList = NIL as *mut List;
    foreach!(t, tlist, {
        let tle = lfirst(current_cell!(t)) as *mut TargetEntry;

        if !(*tle).resjunk {
            let def = makeColumnDef(
                (*tle).resname,
                exprType((*tle).expr as *mut Node),
                exprTypmod((*tle).expr as *mut Node),
                exprCollation((*tle).expr as *mut Node),
            );

            /*
             * It's possible that the column is of a collatable type but the
             * collation could not be resolved, so double-check.
             */
            if type_is_collatable(exprType((*tle).expr as *mut Node)) {
                if !OidIsValid((*def).collOid) {
                    ereport!(
                        ERROR,
                        "could not determine which collation to use for view column"
                    );
                }
            } else {
                Assert!(!OidIsValid((*def).collOid));
            }

            attrList = lappend(attrList, def as *mut std::ffi::c_void);
        }
    });

    /*
     * Look up, check permissions on, and lock the creation namespace; also
     * check for a preexisting view with the same name.  This will also set
     * relation->relpersistence to RELPERSISTENCE_TEMP if the selected
     * namespace is temporary.
     */
    lockmode = if replace { AccessExclusiveLock } else { NoLock };
    let mut viewOid_tmp: Oid = InvalidOid;
    let _ = RangeVarGetAndCheckCreationNamespace(relation, lockmode, &mut viewOid_tmp);
    viewOid = viewOid_tmp;

    if OidIsValid(viewOid) && replace {
        let rel: Relation;
        let descriptor: TupleDesc;
        let mut atcmds: *mut List = NIL as *mut List;
        let mut atcmd: *mut AlterTableCmd;
        let mut address: ObjectAddress = std::mem::zeroed();

        /* Relation is already locked, but we must build a relcache entry. */
        rel = relation_open(viewOid, NoLock);

        /* Make sure it *is* a view. */
        if (*(*rel).rd_rel).relkind != RELKIND_VIEW as c_char {
            ereport!(ERROR, "is not a view");
        }

        /* Also check it's not in use already */
        CheckTableNotInUse(rel, c"CREATE OR REPLACE VIEW".as_ptr());

        /*
         * Due to the namespace visibility rules for temporary objects, we
         * should only end up replacing a temporary view with another
         * temporary view, and similarly for permanent views.
         */
        Assert!((*relation).relpersistence == (*(*rel).rd_rel).relpersistence);

        /*
         * Create a tuple descriptor to compare against the existing view, and
         * verify that the old column list is an initial prefix of the new
         * column list.
         */
        descriptor = BuildDescForRelation(attrList);
        checkViewColumns(descriptor, (*rel).rd_att);

        /*
         * If new attributes have been added, we must add pg_attribute entries
         * for them.  It is convenient (although overkill) to use the ALTER
         * TABLE ADD COLUMN infrastructure for this.
         *
         * Note that we must do this before updating the query for the view,
         * since the rules system requires that the correct view columns be in
         * place when defining the new rules.
         *
         * Also note that ALTER TABLE doesn't run parse transformation on
         * AT_AddColumnToView commands.  The ColumnDef we supply must be ready
         * to execute as-is.
         */
        if list_length(attrList) > (*(*rel).rd_att).natts as c_int {
            let mut skip: c_int = (*(*rel).rd_att).natts as c_int;

            foreach!(c, attrList, {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                atcmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
                (*atcmd).subtype = AT_AddColumnToView;
                (*atcmd).def = lfirst(current_cell!(c)) as *mut Node;
                atcmds = lappend(atcmds, atcmd as *mut std::ffi::c_void);
            });

            /* EventTriggerAlterTableStart called by ProcessUtilitySlow */
            AlterTableInternal(viewOid, atcmds, true);

            /* Make the new view columns visible */
            CommandCounterIncrement();
        }

        /*
         * Update the query for the view.
         *
         * Note that we must do this before updating the view options, because
         * the new options may not be compatible with the old view query (for
         * example if we attempt to add the WITH CHECK OPTION, we require that
         * the new view be automatically updatable, but the old view may not
         * have been).
         */
        StoreViewQuery(viewOid, viewParse, replace);

        /* Make the new view query visible */
        CommandCounterIncrement();

        /*
         * Update the view's options.
         *
         * The new options list replaces the existing options list, even if
         * it's empty.
         */
        atcmd = makeNode!(AlterTableCmd, T_AlterTableCmd);
        (*atcmd).subtype = AT_ReplaceRelOptions;
        (*atcmd).def = options as *mut Node;
        atcmds = list_make1!(atcmd as *mut std::ffi::c_void);

        /* EventTriggerAlterTableStart called by ProcessUtilitySlow */
        AlterTableInternal(viewOid, atcmds, true);

        /*
         * There is very little to do here to update the view's dependencies.
         * Most view-level dependency relationships, such as those on the
         * owner, schema, and associated composite type, aren't changing.
         * Because we don't allow changing type or collation of an existing
         * view column, those dependencies of the existing columns don't
         * change either, while the AT_AddColumnToView machinery took care of
         * adding such dependencies for new view columns.  The dependencies of
         * the view's query could have changed arbitrarily, but that was dealt
         * with inside StoreViewQuery.  What remains is only to check that
         * view replacement is allowed when we're creating an extension.
         */
        ObjectAddressSet(&mut address, RelationRelationId, viewOid);

        recordDependencyOnCurrentExtension(&address, true);

        /*
         * Seems okay, so return the OID of the pre-existing view.
         */
        relation_close(rel, NoLock); /* keep the lock! */

        return address;
    } else {
        let createStmt: *mut CreateStmt = makeNode!(CreateStmt, T_CreateStmt);
        let address: ObjectAddress;

        /*
         * Set the parameters for keys/inheritance etc. All of these are
         * uninteresting for views...
         */
        (*createStmt).relation = relation;
        (*createStmt).tableElts = attrList;
        (*createStmt).inhRelations = NIL as *mut List;
        (*createStmt).constraints = NIL as *mut List;
        (*createStmt).options = options;
        (*createStmt).oncommit = ONCOMMIT_NOOP;
        (*createStmt).tablespacename = std::ptr::null_mut();
        (*createStmt).if_not_exists = false;

        /*
         * Create the relation (this will error out if there's an existing
         * view, so we don't need more code to complain if "replace" is
         * false).
         */
        address = DefineRelation(
            createStmt,
            RELKIND_VIEW as c_char,
            InvalidOid,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        );
        Assert!(address.objectId != InvalidOid);

        /* Make the new view relation visible */
        CommandCounterIncrement();

        /* Store the query for the view */
        StoreViewQuery(address.objectId, viewParse, replace);

        return address;
    }
}

/*
 * Verify that the columns associated with proposed new view definition match
 * the columns of the old view.  This is similar to equalRowTypes(), with code
 * added to generate specific complaints.  Also, we allow the new view to have
 * more columns than the old.
 */
unsafe fn checkViewColumns(newdesc: TupleDesc, olddesc: TupleDesc) {
    let mut i: c_int;

    if (*newdesc).natts < (*olddesc).natts {
        ereport!(ERROR, "cannot drop columns from view");
    }

    i = 0;
    while i < (*olddesc).natts as c_int {
        let newattr: Form_pg_attribute = TupleDescAttr(newdesc, i);
        let oldattr: Form_pg_attribute = TupleDescAttr(olddesc, i);

        /* XXX msg not right, but we don't support DROP COL on view anyway */
        if (*newattr).attisdropped != (*oldattr).attisdropped {
            ereport!(ERROR, "cannot drop columns from view");
        }

        if strcmp(NameStr!((*newattr).attname), NameStr!((*oldattr).attname)) != 0 {
            ereport!(ERROR, "cannot change name of view column");
        }

        /*
         * We cannot allow type, typmod, or collation to change, since these
         * properties may be embedded in Vars of other views/rules referencing
         * this one.  Other column attributes can be ignored.
         */
        if (*newattr).atttypid != (*oldattr).atttypid
            || (*newattr).atttypmod != (*oldattr).atttypmod
        {
            ereport!(ERROR, "cannot change data type of view column");
        }

        /*
         * At this point, attcollations should be both valid or both invalid,
         * so applying get_collation_name unconditionally should be fine.
         */
        if (*newattr).attcollation != (*oldattr).attcollation {
            ereport!(ERROR, "cannot change collation of view column");
        }

        i += 1;
    }

    /*
     * We ignore the constraint fields.  The new view desc can't have any
     * constraints, and the only ones that could be on the old view are
     * defaults, which we are happy to leave in place.
     */
}

unsafe fn DefineViewRules(viewOid: Oid, viewParse: *mut Query, replace: bool) {
    /*
     * Set up the ON SELECT rule.  Since the query has already been through
     * parse analysis, we use DefineQueryRewrite() directly.
     */
    DefineQueryRewrite(
        pstrdup(ViewSelectRuleName),
        viewOid,
        std::ptr::null_mut(),
        CMD_SELECT,
        true,
        replace,
        list_make1!(viewParse as *mut std::ffi::c_void),
    );

    /*
     * Someday: automatic ON INSERT, etc
     */
}

/*
 * DefineView
 *		Execute a CREATE VIEW command.
 */
pub unsafe fn DefineView(
    stmt: *mut ViewStmt,
    queryString: *const c_char,
    stmt_location: c_int,
    stmt_len: c_int,
) -> ObjectAddress {
    let rawstmt: *mut RawStmt;
    let viewParse: *mut Query;
    let view: *mut RangeVar;
    // ListCell *cell -- provided by foreach! below
    let mut check_option: bool;
    let address: ObjectAddress;

    /*
     * Run parse analysis to convert the raw parse tree to a Query.  Note this
     * also acquires sufficient locks on the source table(s).
     */
    rawstmt = makeNode!(RawStmt, T_RawStmt);
    (*rawstmt).stmt = (*stmt).query;
    (*rawstmt).stmt_location = stmt_location;
    (*rawstmt).stmt_len = stmt_len;

    viewParse = parse_analyze_fixedparams(
        rawstmt,
        queryString,
        std::ptr::null_mut(),
        0,
        std::ptr::null_mut(),
    );

    /*
     * The grammar should ensure that the result is a single SELECT Query.
     * However, it doesn't forbid SELECT INTO, so we have to check for that.
     */
    if !IsA!(viewParse, T_Query) {
        elog!(ERROR, "unexpected parse analysis result");
    }
    if (*viewParse).utilityStmt != std::ptr::null_mut()
        && IsA!((*viewParse).utilityStmt, T_CreateTableAsStmt)
    {
        ereport!(ERROR, "views must not contain SELECT INTO");
    }
    if (*viewParse).commandType != CMD_SELECT {
        elog!(ERROR, "unexpected parse analysis result");
    }

    /*
     * Check for unsupported cases.  These tests are redundant with ones in
     * DefineQueryRewrite(), but that function will complain about a bogus ON
     * SELECT rule, and we'd rather the message complain about a view.
     */
    if (*viewParse).hasModifyingCTE {
        ereport!(
            ERROR,
            "views must not contain data-modifying statements in WITH"
        );
    }

    /*
     * If the user specified the WITH CHECK OPTION, add it to the list of
     * reloptions.
     */
    if (*stmt).withCheckOption == LOCAL_CHECK_OPTION {
        (*stmt).options = lappend(
            (*stmt).options,
            makeDefElem(
                c"check_option".as_ptr() as *mut c_char,
                makeString(pstrdup(c"local".as_ptr())) as *mut Node,
                -1,
            ) as *mut std::ffi::c_void,
        );
    } else if (*stmt).withCheckOption == CASCADED_CHECK_OPTION {
        (*stmt).options = lappend(
            (*stmt).options,
            makeDefElem(
                c"check_option".as_ptr() as *mut c_char,
                makeString(pstrdup(c"cascaded".as_ptr())) as *mut Node,
                -1,
            ) as *mut std::ffi::c_void,
        );
    }

    /*
     * Check that the view is auto-updatable if WITH CHECK OPTION was
     * specified.
     */
    check_option = false;

    foreach!(cell, (*stmt).options, {
        let defel = lfirst(current_cell!(cell)) as *mut DefElem;

        if strcmp((*defel).defname, c"check_option".as_ptr()) == 0 {
            check_option = true;
        }
    });

    /*
     * If the check option is specified, look to see if the view is actually
     * auto-updatable or not.
     */
    if check_option {
        let view_updatable_error: *const c_char =
            view_query_is_auto_updatable(viewParse, true);

        if !view_updatable_error.is_null() {
            ereport!(
                ERROR,
                "WITH CHECK OPTION is supported only on automatically updatable views"
            );
        }
    }

    /*
     * If a list of column names was given, run through and insert these into
     * the actual query tree. - thomas 2000-03-08
     */
    if (*stmt).aliases != NIL as *mut List {
        let mut alist_item: *mut ListCell = list_head((*stmt).aliases);
        // ListCell *targetList -- provided by foreach! below

        foreach!(targetList, (*viewParse).targetList, {
            let te = lfirst_node!(TargetEntry, T_TargetEntry, current_cell!(targetList));

            /* junk columns don't get aliases */
            if !(*te).resjunk {
                (*te).resname = pstrdup(strVal(lfirst(alist_item) as *mut Node));
                alist_item = lnext((*stmt).aliases, alist_item);
                if alist_item.is_null() {
                    break; /* done assigning aliases */
                }
            }
        });

        if !alist_item.is_null() {
            ereport!(ERROR, "CREATE VIEW specifies more column names than columns");
        }
    }

    /* Unlogged views are not sensible. */
    if (*(*stmt).view).relpersistence == RELPERSISTENCE_UNLOGGED {
        ereport!(
            ERROR,
            "views cannot be unlogged because they do not have storage"
        );
    }

    /*
     * If the user didn't explicitly ask for a temporary view, check whether
     * we need one implicitly.  We allow TEMP to be inserted automatically as
     * long as the CREATE command is consistent with that --- no explicit
     * schema name.
     */
    view = copyObject((*stmt).view as *mut std::ffi::c_void) as *mut RangeVar; /* don't corrupt original command */
    if (*view).relpersistence == RELPERSISTENCE_PERMANENT
        && isQueryUsingTempRelation(viewParse)
    {
        (*view).relpersistence = RELPERSISTENCE_TEMP;
        ereport!(NOTICE, "view will be a temporary view");
    }

    /*
     * Create the view relation
     *
     * NOTE: if it already exists and replace is false, the xact will be
     * aborted.
     */
    address = DefineVirtualRelation(
        view,
        (*viewParse).targetList,
        (*stmt).replace,
        (*stmt).options,
        viewParse,
    );

    return address;
}

/*
 * Use the rules system to store the query for the view.
 */
pub unsafe fn StoreViewQuery(viewOid: Oid, viewParse: *mut Query, replace: bool) {
    /*
     * Now create the rules associated with the view.
     */
    DefineViewRules(viewOid, viewParse, replace);
}

// ---------------------------------------------------------------------------
// Local stubs for unported helper functions / externs.
// ---------------------------------------------------------------------------

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

unsafe fn makeColumnDef(
    _colname: *mut c_char,
    _typeOid: Oid,
    _typmod: i32,
    _collOid: Oid,
) -> *mut ColumnDef {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn exprType(_expr: *mut Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}

unsafe fn exprTypmod(_expr: *mut Node) -> i32 {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}

unsafe fn exprCollation(_expr: *mut Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}

unsafe fn type_is_collatable(_typid: Oid) -> bool {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

unsafe fn RangeVarGetAndCheckCreationNamespace(
    _relation: *mut RangeVar,
    _lockmode: LOCKMODE,
    _existing_relation_id: *mut Oid,
) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}

unsafe fn relation_open(_relationId: Oid, _lockmode: LOCKMODE) -> Relation {
    unimplemented!() // TODO: access/common/relation.c
}

unsafe fn relation_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!() // TODO: access/common/relation.c
}

unsafe fn CheckTableNotInUse(_rel: Relation, _stmt: *const c_char) {
    unimplemented!() // TODO: commands/tablecmds.c
}

unsafe fn BuildDescForRelation(_columns: *mut List) -> TupleDesc {
    unimplemented!() // TODO: catalog/heap.c
}

unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> Form_pg_attribute {
    unimplemented!() // TODO: access/tupdesc.c
}

unsafe fn AlterTableInternal(_relid: Oid, _cmds: *mut List, _recurse: bool) {
    unimplemented!() // TODO: commands/tablecmds.c
}

unsafe fn CommandCounterIncrement() {
    unimplemented!() // TODO: access/transam/xact.c
}

unsafe fn ObjectAddressSet(_addr: *mut ObjectAddress, _classId: Oid, _objectId: Oid) {
    unimplemented!() // TODO: catalog/objectaddress.h
}

unsafe fn recordDependencyOnCurrentExtension(_object: *const ObjectAddress, _isReplace: bool) {
    unimplemented!() // TODO: catalog/pg_depend.c
}

unsafe fn DefineRelation(
    _stmt: *mut CreateStmt,
    _relkind: c_char,
    _ownerId: Oid,
    _typaddress: *mut ObjectAddress,
    _queryString: *const c_char,
) -> ObjectAddress {
    unimplemented!() // TODO: commands/tablecmds.c
}

unsafe fn DefineQueryRewrite(
    _rulename: *mut c_char,
    _event_relid: Oid,
    _event_qual: *mut Node,
    _event_type: CmdType,
    _is_instead: bool,
    _replace: bool,
    _action: *mut List,
) -> Oid {
    unimplemented!() // TODO: rewrite/rewriteDefine.c
}

unsafe fn parse_analyze_fixedparams(
    _parseTree: *mut RawStmt,
    _sourceText: *const c_char,
    _paramTypes: *const Oid,
    _numParams: c_int,
    _queryEnv: *mut std::ffi::c_void,
) -> *mut Query {
    unimplemented!() // TODO: parser/analyze.c
}

unsafe fn makeDefElem(_name: *mut c_char, _arg: *mut Node, _location: c_int) -> *mut DefElem {
    unimplemented!() // TODO: nodes/makefuncs.c
}

unsafe fn makeString(_str: *mut c_char) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: nodes/value.c
}

unsafe fn view_query_is_auto_updatable(_viewquery: *mut Query, _check_cols: bool) -> *const c_char {
    unimplemented!() // TODO: rewrite/rewriteHandler.c
}

unsafe fn strVal(_v: *mut Node) -> *mut c_char {
    unimplemented!() // TODO: nodes/value.h
}

unsafe fn isQueryUsingTempRelation(_query: *mut Query) -> bool {
    unimplemented!() // TODO: commands/tablecmds.c
}

unsafe fn copyObject(_from: *mut std::ffi::c_void) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: nodes/copyfuncs.c
}

// ---------------------------------------------------------------------------
// Stub types for not-yet-ported node / relcache structures.
// ---------------------------------------------------------------------------

pub type Relation = *mut RelationData;
#[repr(C)]
pub struct RelationData {
    pub rd_rel: *mut FormData_pg_class,
    pub rd_att: TupleDesc,
}

#[repr(C)]
pub struct FormData_pg_class {
    pub relkind: c_char,
    pub relpersistence: c_char,
}

pub type TupleDesc = *mut TupleDescData;
#[repr(C)]
pub struct TupleDescData {
    pub natts: i16,
}

pub type Form_pg_attribute = *mut FormData_pg_attribute;
#[repr(C)]
pub struct FormData_pg_attribute {
    pub attname: NameData,
    pub atttypid: Oid,
    pub atttypmod: i32,
    pub attcollation: Oid,
    pub attisdropped: bool,
}

#[repr(C)]
pub struct NameData {
    pub data: [c_char; 64],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectAddress {
    pub classId: Oid,
    pub objectId: Oid,
    pub objectSubId: i32,
}

#[repr(C)]
pub struct RangeVar {
    pub relpersistence: c_char,
    pub relname: *mut c_char,
}

#[repr(C)]
pub struct Query {
    pub commandType: CmdType,
    pub utilityStmt: *mut Node,
    pub hasModifyingCTE: bool,
    pub targetList: *mut List,
}

#[repr(C)]
pub struct RawStmt {
    pub xpr: Node,
    pub stmt: *mut Node,
    pub stmt_location: c_int,
    pub stmt_len: c_int,
}

#[repr(C)]
pub struct TargetEntry {
    pub xpr: Node,
    pub expr: *mut Node,
    pub resname: *mut c_char,
    pub resjunk: bool,
}

#[repr(C)]
pub struct ColumnDef {
    pub colname: *mut c_char,
    pub collOid: Oid,
}

#[repr(C)]
pub struct DefElem {
    pub defname: *mut c_char,
}

#[repr(C)]
pub struct ViewStmt {
    pub view: *mut RangeVar,
    pub aliases: *mut List,
    pub query: *mut Node,
    pub withCheckOption: ViewCheckOption,
    pub options: *mut List,
    pub replace: bool,
}

#[repr(C)]
pub struct CreateStmt {
    pub relation: *mut RangeVar,
    pub tableElts: *mut List,
    pub inhRelations: *mut List,
    pub constraints: *mut List,
    pub options: *mut List,
    pub oncommit: OnCommitAction,
    pub tablespacename: *mut c_char,
    pub if_not_exists: bool,
}

#[repr(C)]
pub struct AlterTableCmd {
    pub xpr: Node,
    pub subtype: AlterTableType,
    pub def: *mut Node,
}

pub type CmdType = c_int;
pub const CMD_SELECT: CmdType = 1;

pub type ViewCheckOption = c_int;
pub const NO_CHECK_OPTION: ViewCheckOption = 0;
pub const LOCAL_CHECK_OPTION: ViewCheckOption = 1;
pub const CASCADED_CHECK_OPTION: ViewCheckOption = 2;

pub type OnCommitAction = c_int;
pub const ONCOMMIT_NOOP: OnCommitAction = 0;

pub type AlterTableType = c_int;
pub const AT_AddColumnToView: AlterTableType = 1;
pub const AT_ReplaceRelOptions: AlterTableType = 2;

// ---------------------------------------------------------------------------
// Local constants / macros normally provided by other headers.
// ---------------------------------------------------------------------------

pub const NoLock: LOCKMODE = 0;
pub const AccessExclusiveLock: LOCKMODE = 8;

pub const InvalidOid: Oid = 0;

pub const RELKIND_VIEW: u8 = b'v';
pub const RELPERSISTENCE_PERMANENT: c_char = b'p' as c_char;
pub const RELPERSISTENCE_UNLOGGED: c_char = b'u' as c_char;
pub const RELPERSISTENCE_TEMP: c_char = b't' as c_char;

pub const RelationRelationId: Oid = 1259;

#[allow(non_upper_case_globals)]
pub const ViewSelectRuleName: *const c_char = c"_RETURN".as_ptr();

#[inline]
pub fn OidIsValid(objectId: Oid) -> bool {
    objectId != InvalidOid
}

macro_rules! NameStr {
    ($name:expr) => {
        ($name).data.as_ptr()
    };
}
use NameStr;



unsafe fn pstrdup(_s: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}

// elog/ereport levels
pub const ERROR: c_int = 21;
pub const NOTICE: c_int = 19;

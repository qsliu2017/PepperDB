//! src/backend/commands/dropcmds.c
//!
//! dropcmds.c
//!   handle various "DROP" operations
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::{castNode, foreach, current_cell, IsA, makeNode, linitial_node, lsecond_node};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{
    List, ListCell, NIL, lfirst, linitial, lsecond, llast, list_make1, list_length,
    list_copy_head, list_copy_tail, list_free,
};
use crate::parser::parse_node::ParseState;

// gettext_noop(x): i18n no-op marker - identity (#define gettext_noop(x) (x)).
unsafe fn gettext_noop(s: *const c_char) -> *const c_char {
    s
}

// ---------------------------------------------------------------------------
// Local stubs for unported types / helpers.
// ---------------------------------------------------------------------------

type DropStmt = crate::nodes::parsenodes::DropStmt;
type ObjectAddresses = crate::catalog::dependency::ObjectAddresses;
type ObjectAddress = crate::catalog::objectaddress::ObjectAddress;
type ObjectType = crate::nodes::parsenodes::ObjectType;
type RangeVar = crate::nodes::primnodes::RangeVar;
type TypeName = crate::nodes::parsenodes::TypeName;
type ObjectWithArgs = crate::nodes::parsenodes::ObjectWithArgs;

unsafe fn new_object_addresses() -> *mut ObjectAddresses {
    unimplemented!() // TODO: catalog/dependency.c
}
unsafe fn add_exact_object_address(_object: *const ObjectAddress, _addrs: *mut ObjectAddresses) {
    unimplemented!() // TODO: catalog/dependency.c
}
unsafe fn performMultipleDeletions(
    _objects: *const ObjectAddresses,
    _behavior: DropBehavior,
    _flags: c_int,
) {
    unimplemented!() // TODO: catalog/dependency.c
}
unsafe fn free_object_addresses(_addrs: *mut ObjectAddresses) {
    unimplemented!() // TODO: catalog/dependency.c
}
unsafe fn get_object_address(
    _objtype: ObjectType,
    _object: *mut Node,
    _relp: *mut Relation,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> ObjectAddress {
    unimplemented!() // TODO: catalog/objectaddress.c
}
unsafe fn get_object_namespace(_address: *const ObjectAddress) -> Oid {
    unimplemented!() // TODO: catalog/objectaddress.c
}
unsafe fn check_object_ownership(
    _roleid: Oid,
    _objtype: ObjectType,
    _address: ObjectAddress,
    _object: *mut Node,
    _relation: Relation,
) {
    unimplemented!() // TODO: catalog/objectaddress.c
}
unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool {
    unimplemented!() // TODO: utils/adt/acl.c
}
unsafe fn get_func_prokind(_funcid: Oid) -> c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn isTempNamespace(_namespaceId: Oid) -> bool {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn LookupNamespaceNoError(_nspname: *const c_char) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn LookupTypeNameOid(
    _pstate: *mut ParseState,
    _typeName: *const TypeName,
    _missing_ok: bool,
) -> Oid {
    unimplemented!() // TODO: parser/parse_type.c
}
unsafe fn TypeNameToString(_typeName: *const TypeName) -> *mut c_char {
    unimplemented!() // TODO: parser/parse_type.c
}
unsafe fn TypeNameListToString(_typenames: *mut List) -> *mut c_char {
    unimplemented!() // TODO: parser/parse_type.c
}
unsafe fn NameListToString(_names: *mut List) -> *mut c_char {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn makeRangeVarFromNameList(_names: *mut List) -> *mut RangeVar {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn RangeVarGetRelid(
    _relation: *const RangeVar,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn table_close(_relation: Relation, _lockmode: LOCKMODE) {
    unimplemented!() // TODO: access/table/table.c
}
unsafe fn strVal(_v: *mut Node) -> *mut c_char {
    unimplemented!() // TODO: nodes/value.h
}

type Relation = *mut crate::utils::rel::RelationData;
type ParseState = crate::nodes::parsenodes::ParseState;
type DropBehavior = crate::nodes::parsenodes::DropBehavior;
type LOCKMODE = c_int;

// From storage/lockdefs.h
const NoLock: LOCKMODE = 0;
const AccessExclusiveLock: LOCKMODE = 8;

// From access/xact.h
const XACT_FLAGS_ACCESSEDTEMPNAMESPACE: c_int = 1 << 0;

// From catalog/pg_proc.h
const PROKIND_AGGREGATE: c_char = b'a' as c_char;

// From catalog/pg_namespace_d.h
const NamespaceRelationId: Oid = 2615;

extern "C" {
    static mut MyXactFlags: c_int;
}

/*
 * Drop one or more objects.
 *
 * We don't currently handle all object types here.  Relations, for example,
 * require special handling, because (for example) indexes have additional
 * locking requirements.
 *
 * We look up all the objects first, and then delete them in a single
 * performMultipleDeletions() call.  This avoids unnecessary DROP RESTRICT
 * errors if there are dependencies between them.
 */
pub unsafe fn RemoveObjects(stmt: *mut DropStmt) {
    let objects: *mut ObjectAddresses;

    objects = new_object_addresses();

    foreach!(cell1, (*stmt).objects, {
        let mut address: ObjectAddress;
        let object: *mut Node = lfirst(current_cell!(cell1)) as *mut Node;
        let mut relation: Relation = std::ptr::null_mut();
        let namespaceId: Oid;

        /* Get an ObjectAddress for the object. */
        address = get_object_address(
            (*stmt).removeType,
            object,
            &mut relation,
            AccessExclusiveLock,
            (*stmt).missing_ok,
        );

        /*
         * Issue NOTICE if supplied object was not found.  Note this is only
         * relevant in the missing_ok case, because otherwise
         * get_object_address would have thrown an error.
         */
        if !OidIsValid(address.objectId) {
            Assert!((*stmt).missing_ok);
            does_not_exist_skipping((*stmt).removeType, object);
            continue;
        }

        /*
         * Although COMMENT ON FUNCTION, SECURITY LABEL ON FUNCTION, etc. are
         * happy to operate on an aggregate as on any other function, we have
         * historically not allowed this for DROP FUNCTION.
         */
        if (*stmt).removeType == ObjectType::OBJECT_FUNCTION {
            if get_func_prokind(address.objectId) == PROKIND_AGGREGATE {
                ereport!(
                    ERROR,
                    "\"{}\" is an aggregate function"
                    // errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    // NameListToString(castNode(ObjectWithArgs, object)->objname),
                    // errhint("Use DROP AGGREGATE to drop aggregate functions.")
                );
                let _ = NameListToString((*castNode!(ObjectWithArgs, T_ObjectWithArgs, object)).objname);
            }
        }

        /* Check permissions. */
        namespaceId = get_object_namespace(&address);
        if !OidIsValid(namespaceId)
            || !object_ownercheck(NamespaceRelationId, namespaceId, GetUserId())
        {
            check_object_ownership(
                GetUserId(),
                (*stmt).removeType,
                address,
                object,
                relation,
            );
        }

        /*
         * Make note if a temporary namespace has been accessed in this
         * transaction.
         */
        if OidIsValid(namespaceId) && isTempNamespace(namespaceId) {
            MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE;
        }

        /* Release any relcache reference count, but keep lock until commit. */
        if !relation.is_null() {
            table_close(relation, NoLock);
        }

        add_exact_object_address(&address, objects);
    });

    /* Here we really delete them. */
    performMultipleDeletions(objects, (*stmt).behavior, 0);

    free_object_addresses(objects);
}

/*
 * owningrel_does_not_exist_skipping
 *		Subroutine for RemoveObjects
 *
 * After determining that a specification for a rule or trigger returns that
 * the specified object does not exist, test whether its owning relation, and
 * its schema, exist or not; if they do, return false --- the trigger or rule
 * itself is missing instead.  If the owning relation or its schema do not
 * exist, fill the error message format string and name, and return true.
 */
unsafe fn owningrel_does_not_exist_skipping(
    object: *mut List,
    msg: *mut *const c_char,
    name: *mut *mut c_char,
) -> bool {
    let parent_object: *mut List;
    let parent_rel: *mut RangeVar;

    parent_object = list_copy_head(object, list_length(object) - 1);

    if schema_does_not_exist_skipping(parent_object, msg, name) {
        return true;
    }

    parent_rel = makeRangeVarFromNameList(parent_object);

    if !OidIsValid(RangeVarGetRelid(parent_rel, NoLock, true)) {
        *msg = gettext_noop(c"relation \"%s\" does not exist, skipping".as_ptr());
        *name = NameListToString(parent_object);

        return true;
    }

    false
}

/*
 * schema_does_not_exist_skipping
 *		Subroutine for RemoveObjects
 *
 * After determining that a specification for a schema-qualifiable object
 * refers to an object that does not exist, test whether the specified schema
 * exists or not.  If no schema was specified, or if the schema does exist,
 * return false -- the object itself is missing instead.  If the specified
 * schema does not exist, fill the error message format string and the
 * specified schema name, and return true.
 */
unsafe fn schema_does_not_exist_skipping(
    object: *mut List,
    msg: *mut *const c_char,
    name: *mut *mut c_char,
) -> bool {
    let rel: *mut RangeVar;

    rel = makeRangeVarFromNameList(object);

    if !(*rel).schemaname.is_null() && !OidIsValid(LookupNamespaceNoError((*rel).schemaname)) {
        *msg = gettext_noop(c"schema \"%s\" does not exist, skipping".as_ptr());
        *name = (*rel).schemaname;

        return true;
    }

    false
}

/*
 * type_in_list_does_not_exist_skipping
 *		Subroutine for RemoveObjects
 *
 * After determining that a specification for a function, cast, aggregate or
 * operator returns that the specified object does not exist, test whether the
 * involved datatypes, and their schemas, exist or not; if they do, return
 * false --- the original object itself is missing instead.  If the datatypes
 * or schemas do not exist, fill the error message format string and the
 * missing name, and return true.
 *
 * First parameter is a list of TypeNames.
 */
unsafe fn type_in_list_does_not_exist_skipping(
    typenames: *mut List,
    msg: *mut *const c_char,
    name: *mut *mut c_char,
) -> bool {
    foreach!(l, typenames, {
        let typeName: *mut TypeName = lfirst_node!(TypeName, T_TypeName, current_cell!(l));

        if !typeName.is_null() {
            if !OidIsValid(LookupTypeNameOid(std::ptr::null_mut(), typeName, true)) {
                /* type doesn't exist, try to find why */
                if schema_does_not_exist_skipping((*typeName).names, msg, name) {
                    return true;
                }

                *msg = gettext_noop(c"type \"%s\" does not exist, skipping".as_ptr());
                *name = TypeNameToString(typeName);

                return true;
            }
        }
    });

    false
}

/*
 * does_not_exist_skipping
 *		Subroutine for RemoveObjects
 *
 * Generate a NOTICE stating that the named object was not found, and is
 * being skipped.  This is only relevant when "IF EXISTS" is used; otherwise,
 * get_object_address() in RemoveObjects would have thrown an ERROR.
 */
unsafe fn does_not_exist_skipping(objtype: ObjectType, object: *mut Node) {
    let mut msg: *const c_char = std::ptr::null();
    let mut name: *mut c_char = std::ptr::null_mut();
    let mut args: *mut c_char = std::ptr::null_mut();

    match objtype {
        ObjectType::OBJECT_ACCESS_METHOD => {
            msg = gettext_noop(c"access method \"%s\" does not exist, skipping".as_ptr());
            name = strVal(object);
        }
        ObjectType::OBJECT_TYPE | ObjectType::OBJECT_DOMAIN => {
            let typ: *mut TypeName = castNode!(TypeName, T_TypeName, object);

            if !schema_does_not_exist_skipping((*typ).names, &mut msg, &mut name) {
                msg = gettext_noop(c"type \"%s\" does not exist, skipping".as_ptr());
                name = TypeNameToString(typ);
            }
        }
        ObjectType::OBJECT_COLLATION => {
            if !schema_does_not_exist_skipping(
                castNode!(List, T_List, object),
                &mut msg,
                &mut name,
            ) {
                msg = gettext_noop(c"collation \"%s\" does not exist, skipping".as_ptr());
                name = NameListToString(castNode!(List, T_List, object));
            }
        }
        ObjectType::OBJECT_CONVERSION => {
            if !schema_does_not_exist_skipping(
                castNode!(List, T_List, object),
                &mut msg,
                &mut name,
            ) {
                msg = gettext_noop(c"conversion \"%s\" does not exist, skipping".as_ptr());
                name = NameListToString(castNode!(List, T_List, object));
            }
        }
        ObjectType::OBJECT_SCHEMA => {
            msg = gettext_noop(c"schema \"%s\" does not exist, skipping".as_ptr());
            name = strVal(object);
        }
        ObjectType::OBJECT_STATISTIC_EXT => {
            if !schema_does_not_exist_skipping(
                castNode!(List, T_List, object),
                &mut msg,
                &mut name,
            ) {
                msg = gettext_noop(c"statistics object \"%s\" does not exist, skipping".as_ptr());
                name = NameListToString(castNode!(List, T_List, object));
            }
        }
        ObjectType::OBJECT_TSPARSER => {
            if !schema_does_not_exist_skipping(
                castNode!(List, T_List, object),
                &mut msg,
                &mut name,
            ) {
                msg = gettext_noop(c"text search parser \"%s\" does not exist, skipping".as_ptr());
                name = NameListToString(castNode!(List, T_List, object));
            }
        }
        ObjectType::OBJECT_TSDICTIONARY => {
            if !schema_does_not_exist_skipping(
                castNode!(List, T_List, object),
                &mut msg,
                &mut name,
            ) {
                msg = gettext_noop(
                    c"text search dictionary \"%s\" does not exist, skipping".as_ptr(),
                );
                name = NameListToString(castNode!(List, T_List, object));
            }
        }
        ObjectType::OBJECT_TSTEMPLATE => {
            if !schema_does_not_exist_skipping(
                castNode!(List, T_List, object),
                &mut msg,
                &mut name,
            ) {
                msg =
                    gettext_noop(c"text search template \"%s\" does not exist, skipping".as_ptr());
                name = NameListToString(castNode!(List, T_List, object));
            }
        }
        ObjectType::OBJECT_TSCONFIGURATION => {
            if !schema_does_not_exist_skipping(
                castNode!(List, T_List, object),
                &mut msg,
                &mut name,
            ) {
                msg = gettext_noop(
                    c"text search configuration \"%s\" does not exist, skipping".as_ptr(),
                );
                name = NameListToString(castNode!(List, T_List, object));
            }
        }
        ObjectType::OBJECT_EXTENSION => {
            msg = gettext_noop(c"extension \"%s\" does not exist, skipping".as_ptr());
            name = strVal(object);
        }
        ObjectType::OBJECT_FUNCTION => {
            let owa: *mut ObjectWithArgs = castNode!(ObjectWithArgs, T_ObjectWithArgs, object);

            if !schema_does_not_exist_skipping((*owa).objname, &mut msg, &mut name)
                && !type_in_list_does_not_exist_skipping((*owa).objargs, &mut msg, &mut name)
            {
                msg = gettext_noop(c"function %s(%s) does not exist, skipping".as_ptr());
                name = NameListToString((*owa).objname);
                args = TypeNameListToString((*owa).objargs);
            }
        }
        ObjectType::OBJECT_PROCEDURE => {
            let owa: *mut ObjectWithArgs = castNode!(ObjectWithArgs, T_ObjectWithArgs, object);

            if !schema_does_not_exist_skipping((*owa).objname, &mut msg, &mut name)
                && !type_in_list_does_not_exist_skipping((*owa).objargs, &mut msg, &mut name)
            {
                msg = gettext_noop(c"procedure %s(%s) does not exist, skipping".as_ptr());
                name = NameListToString((*owa).objname);
                args = TypeNameListToString((*owa).objargs);
            }
        }
        ObjectType::OBJECT_ROUTINE => {
            let owa: *mut ObjectWithArgs = castNode!(ObjectWithArgs, T_ObjectWithArgs, object);

            if !schema_does_not_exist_skipping((*owa).objname, &mut msg, &mut name)
                && !type_in_list_does_not_exist_skipping((*owa).objargs, &mut msg, &mut name)
            {
                msg = gettext_noop(c"routine %s(%s) does not exist, skipping".as_ptr());
                name = NameListToString((*owa).objname);
                args = TypeNameListToString((*owa).objargs);
            }
        }
        ObjectType::OBJECT_AGGREGATE => {
            let owa: *mut ObjectWithArgs = castNode!(ObjectWithArgs, T_ObjectWithArgs, object);

            if !schema_does_not_exist_skipping((*owa).objname, &mut msg, &mut name)
                && !type_in_list_does_not_exist_skipping((*owa).objargs, &mut msg, &mut name)
            {
                msg = gettext_noop(c"aggregate %s(%s) does not exist, skipping".as_ptr());
                name = NameListToString((*owa).objname);
                args = TypeNameListToString((*owa).objargs);
            }
        }
        ObjectType::OBJECT_OPERATOR => {
            let owa: *mut ObjectWithArgs = castNode!(ObjectWithArgs, T_ObjectWithArgs, object);

            if !schema_does_not_exist_skipping((*owa).objname, &mut msg, &mut name)
                && !type_in_list_does_not_exist_skipping((*owa).objargs, &mut msg, &mut name)
            {
                msg = gettext_noop(c"operator %s does not exist, skipping".as_ptr());
                name = NameListToString((*owa).objname);
            }
        }
        ObjectType::OBJECT_LANGUAGE => {
            msg = gettext_noop(c"language \"%s\" does not exist, skipping".as_ptr());
            name = strVal(object);
        }
        ObjectType::OBJECT_CAST => {
            if !type_in_list_does_not_exist_skipping(
                list_make1(linitial(castNode!(List, T_List, object))),
                &mut msg,
                &mut name,
            ) && !type_in_list_does_not_exist_skipping(
                list_make1(lsecond(castNode!(List, T_List, object))),
                &mut msg,
                &mut name,
            ) {
                /* XXX quote or no quote? */
                msg = gettext_noop(
                    c"cast from type %s to type %s does not exist, skipping".as_ptr(),
                );
                name = TypeNameToString(linitial_node!(
                    TypeName,
                    T_TypeName,
                    castNode!(List, T_List, object)
                ));
                args = TypeNameToString(lsecond_node!(
                    TypeName,
                    T_TypeName,
                    castNode!(List, T_List, object)
                ));
            }
        }
        ObjectType::OBJECT_TRANSFORM => {
            if !type_in_list_does_not_exist_skipping(
                list_make1(linitial(castNode!(List, T_List, object))),
                &mut msg,
                &mut name,
            ) {
                msg = gettext_noop(
                    c"transform for type %s language \"%s\" does not exist, skipping".as_ptr(),
                );
                name = TypeNameToString(linitial_node!(
                    TypeName,
                    T_TypeName,
                    castNode!(List, T_List, object)
                ));
                args = strVal(lsecond(castNode!(List, T_List, object)) as *mut Node);
            }
        }
        ObjectType::OBJECT_TRIGGER => {
            if !owningrel_does_not_exist_skipping(
                castNode!(List, T_List, object),
                &mut msg,
                &mut name,
            ) {
                msg = gettext_noop(
                    c"trigger \"%s\" for relation \"%s\" does not exist, skipping".as_ptr(),
                );
                name = strVal(llast(castNode!(List, T_List, object)) as *mut Node);
                args = NameListToString(list_copy_head(
                    castNode!(List, T_List, object),
                    list_length(castNode!(List, T_List, object)) - 1,
                ));
            }
        }
        ObjectType::OBJECT_POLICY => {
            if !owningrel_does_not_exist_skipping(
                castNode!(List, T_List, object),
                &mut msg,
                &mut name,
            ) {
                msg = gettext_noop(
                    c"policy \"%s\" for relation \"%s\" does not exist, skipping".as_ptr(),
                );
                name = strVal(llast(castNode!(List, T_List, object)) as *mut Node);
                args = NameListToString(list_copy_head(
                    castNode!(List, T_List, object),
                    list_length(castNode!(List, T_List, object)) - 1,
                ));
            }
        }
        ObjectType::OBJECT_EVENT_TRIGGER => {
            msg = gettext_noop(c"event trigger \"%s\" does not exist, skipping".as_ptr());
            name = strVal(object);
        }
        ObjectType::OBJECT_RULE => {
            if !owningrel_does_not_exist_skipping(
                castNode!(List, T_List, object),
                &mut msg,
                &mut name,
            ) {
                msg = gettext_noop(
                    c"rule \"%s\" for relation \"%s\" does not exist, skipping".as_ptr(),
                );
                name = strVal(llast(castNode!(List, T_List, object)) as *mut Node);
                args = NameListToString(list_copy_head(
                    castNode!(List, T_List, object),
                    list_length(castNode!(List, T_List, object)) - 1,
                ));
            }
        }
        ObjectType::OBJECT_FDW => {
            msg = gettext_noop(c"foreign-data wrapper \"%s\" does not exist, skipping".as_ptr());
            name = strVal(object);
        }
        ObjectType::OBJECT_FOREIGN_SERVER => {
            msg = gettext_noop(c"server \"%s\" does not exist, skipping".as_ptr());
            name = strVal(object);
        }
        ObjectType::OBJECT_OPCLASS => {
            let opcname: *mut List = list_copy_tail(castNode!(List, T_List, object), 1);

            if !schema_does_not_exist_skipping(opcname, &mut msg, &mut name) {
                msg = gettext_noop(
                    c"operator class \"%s\" does not exist for access method \"%s\", skipping"
                        .as_ptr(),
                );
                name = NameListToString(opcname);
                args = strVal(linitial(castNode!(List, T_List, object)) as *mut Node);
            }
        }
        ObjectType::OBJECT_OPFAMILY => {
            let opfname: *mut List = list_copy_tail(castNode!(List, T_List, object), 1);

            if !schema_does_not_exist_skipping(opfname, &mut msg, &mut name) {
                msg = gettext_noop(
                    c"operator family \"%s\" does not exist for access method \"%s\", skipping"
                        .as_ptr(),
                );
                name = NameListToString(opfname);
                args = strVal(linitial(castNode!(List, T_List, object)) as *mut Node);
            }
        }
        ObjectType::OBJECT_PUBLICATION => {
            msg = gettext_noop(c"publication \"%s\" does not exist, skipping".as_ptr());
            name = strVal(object);
        }

        ObjectType::OBJECT_COLUMN
        | ObjectType::OBJECT_DATABASE
        | ObjectType::OBJECT_FOREIGN_TABLE
        | ObjectType::OBJECT_INDEX
        | ObjectType::OBJECT_MATVIEW
        | ObjectType::OBJECT_ROLE
        | ObjectType::OBJECT_SEQUENCE
        | ObjectType::OBJECT_SUBSCRIPTION
        | ObjectType::OBJECT_TABLE
        | ObjectType::OBJECT_TABLESPACE
        | ObjectType::OBJECT_VIEW => {
            /*
             * These are handled elsewhere, so if someone gets here the code
             * is probably wrong or should be revisited.
             */
            elog!(ERROR, "unsupported object type: {}", objtype as c_int);
        }

        ObjectType::OBJECT_AMOP
        | ObjectType::OBJECT_AMPROC
        | ObjectType::OBJECT_ATTRIBUTE
        | ObjectType::OBJECT_DEFAULT
        | ObjectType::OBJECT_DEFACL
        | ObjectType::OBJECT_DOMCONSTRAINT
        | ObjectType::OBJECT_LARGEOBJECT
        | ObjectType::OBJECT_PARAMETER_ACL
        | ObjectType::OBJECT_PUBLICATION_NAMESPACE
        | ObjectType::OBJECT_PUBLICATION_REL
        | ObjectType::OBJECT_TABCONSTRAINT
        | ObjectType::OBJECT_USER_MAPPING => {
            /* These are currently not used or needed. */
            elog!(ERROR, "unsupported object type: {}", objtype as c_int);
        }
        /* no default, to let compiler warn about missing case */
    }

    if msg.is_null() {
        elog!(ERROR, "unrecognized object type: {}", objtype as c_int);
    }

    if args.is_null() {
        ereport!(NOTICE, "errmsg(msg, name)");
    } else {
        ereport!(NOTICE, "errmsg(msg, name, args)");
    }
}

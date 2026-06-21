//! src/backend/commands/operatorcmds.c
//!
//!   Routines for operator manipulation commands
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! DESCRIPTION
//!   The "DefineFoo" routines take the parse tree and pick out the
//!   appropriate arguments/flags, passing the results to the
//!   corresponding "FooCreate" routines (in src/backend/catalog) that do
//!   the actual catalog-munging.  These routines also verify permission
//!   of the user to execute the command.
//!
//! NOTES
//!   These things must be defined and committed in the following order:
//!     "create function":
//!             input/output, recv/send functions
//!     "create type":
//!             type
//!     "create operator":
//!             operators

use crate::prelude::*;

use crate::nodes::pg_list::lfirst;
use crate::{current_cell, foreach};

use std::ffi::{c_char, c_int};

// ---------------------------------------------------------------------------
// Local type/const stubs for unported dependencies
// ---------------------------------------------------------------------------

type AclResult = c_int;
type TypeName = crate::nodes::parsenodes::TypeName;
type DefElem = crate::nodes::parsenodes::DefElem;
type AlterOperatorStmt = crate::nodes::parsenodes::AlterOperatorStmt;
type ObjectAddress = crate::catalog::objectaccess::ObjectAddress;
type List = crate::nodes::pg_list::List;
type ListCell = crate::nodes::pg_list::ListCell;
type Relation = crate::utils::rel::Relation;
type HeapTuple = crate::access::htup_details::HeapTuple;
type Form_pg_operator = *mut crate::catalog::pg_operator::FormData_pg_operator;

const NIL: *mut List = std::ptr::null_mut();
const InvalidOid: Oid = 0;

const NamespaceRelationId: Oid = 2615;
const TypeRelationId: Oid = 1247;
const ProcedureRelationId: Oid = 1255;
const OperatorRelationId: Oid = 2617;

const ACL_CREATE: u32 = 1 << 11; // AclMode bit, placeholder
const ACL_USAGE: u32 = 1 << 8;
const ACL_EXECUTE: u32 = 1 << 5;

const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 1;

const OBJECT_SCHEMA: c_int = 0;
const OBJECT_FUNCTION: c_int = 1;
const OBJECT_OPERATOR: c_int = 2;

const INTERNALOID: Oid = 2281;
const OIDOID: Oid = 26;
const INT4OID: Oid = 23;
const INT2OID: Oid = 21;
const FLOAT8OID: Oid = 701;

const FirstGenbkiObjectId: Oid = 10000;

const RowExclusiveLock: c_int = 3;
const NoLock: c_int = 0;

const OPEROID: c_int = 40; // syscache id

const Natts_pg_operator: usize = 14;
const Anum_pg_operator_oprrest: c_int = 12;
const Anum_pg_operator_oprjoin: c_int = 13;
const Anum_pg_operator_oprcom: c_int = 10;
const Anum_pg_operator_oprnegate: c_int = 11;
const Anum_pg_operator_oprcanmerge: c_int = 8;
const Anum_pg_operator_oprcanhash: c_int = 9;

// ---------------------------------------------------------------------------
// Local stubs for unported helper functions
// ---------------------------------------------------------------------------

unsafe fn QualifiedNameGetCreationNamespace(names: *mut List, objname_p: *mut *mut c_char) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn object_aclcheck(classid: Oid, objectid: Oid, roleid: Oid, mode: u32) -> AclResult {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }
unsafe fn aclcheck_error(aclerr: AclResult, objtype: c_int, objname: *const c_char) {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn aclcheck_error_type(aclerr: AclResult, typeOid: Oid) {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn get_namespace_name(nspid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn defGetTypeName(def: *mut DefElem) -> *mut TypeName {
    unimplemented!() // TODO: commands/define.c
}
unsafe fn defGetQualifiedName(def: *mut DefElem) -> *mut List {
    unimplemented!() // TODO: commands/define.c
}
unsafe fn defGetBoolean(def: *mut DefElem) -> bool {
    unimplemented!() // TODO: commands/define.c
}
unsafe fn typenameTypeId(pstate: *mut c_void, typeName: *mut TypeName) -> Oid {
    unimplemented!() // TODO: parser/parse_type.c
}
unsafe fn LookupFuncName(funcname: *mut List, nargs: c_int, argtypes: *const Oid, missing_ok: bool) -> Oid {
    unimplemented!() // TODO: parser/parse_func.c
}
unsafe fn NameListToString(names: *mut List) -> *mut c_char {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn get_func_rettype(funcid: Oid) -> Oid {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn superuser() -> bool {
    unimplemented!() // TODO: utils/misc/superuser.c
}
unsafe fn OperatorCreate(
    operatorName: *const c_char,
    operatorNamespace: Oid,
    leftTypeId: Oid,
    rightTypeId: Oid,
    procedureId: Oid,
    commutatorName: *mut List,
    negatorName: *mut List,
    restrictionId: Oid,
    joinId: Oid,
    canMerge: bool,
    canHash: bool,
) -> ObjectAddress {
    unimplemented!() // TODO: catalog/pg_operator.c
}
unsafe fn OperatorLookup(
    operatorName: *mut List,
    leftTypeId: Oid,
    rightTypeId: Oid,
    defined: *mut bool,
) -> Oid {
    unimplemented!() // TODO: catalog/pg_operator.c
}
unsafe fn op_signature_string(op: *mut List, arg1: Oid, arg2: Oid) -> *mut c_char {
    unimplemented!() // TODO: parser/parse_oper.c
}
unsafe fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> bool {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn table_open(relationId: Oid, lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/table/table.c
}
unsafe fn table_close(relation: Relation, lockmode: c_int) {
    unimplemented!() // TODO: access/table/table.c
}
unsafe fn SearchSysCache1(cacheId: c_int, key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn SearchSysCacheCopy1(cacheId: c_int, key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn ReleaseSysCache(tuple: HeapTuple) {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn HeapTupleIsValid(tuple: HeapTuple) -> bool {
    !tuple.is_null()
}
unsafe fn GETSTRUCT(tuple: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO: access/htup_details.h
}
unsafe fn OperatorUpd(baseId: Oid, commId: Oid, negId: Oid, isDelete: bool) {
    unimplemented!() // TODO: catalog/pg_operator.c
}
unsafe fn CatalogTupleDelete(heapRel: Relation, tid: *mut crate::storage::itemptr::ItemPointerData) {
    unimplemented!() // TODO: catalog/indexing.c
}
unsafe fn CatalogTupleUpdate(heapRel: Relation, otid: *mut crate::storage::itemptr::ItemPointerData, tup: HeapTuple) {
    unimplemented!() // TODO: catalog/indexing.c
}
unsafe fn LookupOperWithArgs(oper: *mut crate::nodes::parsenodes::ObjectWithArgs, noError: bool) -> Oid {
    unimplemented!() // TODO: parser/parse_oper.c
}
unsafe fn OperatorValidateParams(
    leftTypeId: Oid,
    rightTypeId: Oid,
    resultType: Oid,
    hasCommutator: bool,
    hasNegator: bool,
    hasRestrictionSelectivity: bool,
    hasJoinSelectivity: bool,
    canMerge: bool,
    canHash: bool,
) {
    unimplemented!() // TODO: catalog/pg_operator.c
}
unsafe fn heap_modify_tuple(
    tuple: HeapTuple,
    tupleDesc: crate::access::common::tupdesc::TupleDesc,
    replValues: *mut Datum,
    replIsnull: *mut bool,
    doReplace: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn makeOperatorDependencies(tuple: HeapTuple, isUpdate: bool, makeExtensionDep: bool) -> ObjectAddress {
    unimplemented!() // TODO: catalog/pg_operator.c
}
unsafe fn InvokeObjectPostAlterHook(classId: Oid, objectId: Oid, subId: c_int) {
    // TODO: catalog/objectaccess.h (no-op unless object_access_hook set)
}
unsafe fn RelationGetDescr(relation: Relation) -> crate::access::common::tupdesc::TupleDesc {
    unimplemented!() // TODO: utils/rel.h
}

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

/*
 * DefineOperator
 *		this function extracts all the information from the
 *		parameter list generated by the parser and then has
 *		OperatorCreate() do all the actual work.
 *
 * 'parameters' is a list of DefElem
 */
pub unsafe fn DefineOperator(names: *mut List, parameters: *mut List) -> ObjectAddress {
    let mut oprName: *mut c_char = std::ptr::null_mut();
    let oprNamespace: Oid;
    let mut aclresult: AclResult;
    let mut canMerge: bool = false; /* operator merges */
    let mut canHash: bool = false; /* operator hashes */
    let mut functionName: *mut List = NIL; /* function for operator */
    let mut typeName1: *mut TypeName = std::ptr::null_mut(); /* first type name */
    let mut typeName2: *mut TypeName = std::ptr::null_mut(); /* second type name */
    let mut typeId1: Oid = InvalidOid; /* types converted to OID */
    let mut typeId2: Oid = InvalidOid;
    let rettype: Oid;
    let mut commutatorName: *mut List = NIL; /* optional commutator operator name */
    let mut negatorName: *mut List = NIL; /* optional negator operator name */
    let mut restrictionName: *mut List = NIL; /* optional restrict. sel. function */
    let mut joinName: *mut List = NIL; /* optional join sel. function */
    let functionOid: Oid; /* functions converted to OID */
    let restrictionOid: Oid;
    let joinOid: Oid;
    let mut typeId: [Oid; 2] = [0; 2]; /* to hold left and right arg */
    let nargs: c_int;

    /* Convert list of names to a name and namespace */
    oprNamespace = QualifiedNameGetCreationNamespace(names, &mut oprName);

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(NamespaceRelationId, oprNamespace, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(oprNamespace));
    }

    /*
     * loop over the definition list and extract the information we need.
     */
    foreach!(pl, parameters, {
        let defel: *mut DefElem = lfirst(current_cell!(pl)) as *mut DefElem;

        if strcmp((*defel).defname, c"leftarg".as_ptr()) == 0 {
            typeName1 = defGetTypeName(defel);
            if (*typeName1).setof {
                ereport!(ERROR, "SETOF type not allowed for operator argument");
            }
        } else if strcmp((*defel).defname, c"rightarg".as_ptr()) == 0 {
            typeName2 = defGetTypeName(defel);
            if (*typeName2).setof {
                ereport!(ERROR, "SETOF type not allowed for operator argument");
            }
        }
        /* "function" and "procedure" are equivalent here */
        else if strcmp((*defel).defname, c"function".as_ptr()) == 0 {
            functionName = defGetQualifiedName(defel);
        } else if strcmp((*defel).defname, c"procedure".as_ptr()) == 0 {
            functionName = defGetQualifiedName(defel);
        } else if strcmp((*defel).defname, c"commutator".as_ptr()) == 0 {
            commutatorName = defGetQualifiedName(defel);
        } else if strcmp((*defel).defname, c"negator".as_ptr()) == 0 {
            negatorName = defGetQualifiedName(defel);
        } else if strcmp((*defel).defname, c"restrict".as_ptr()) == 0 {
            restrictionName = defGetQualifiedName(defel);
        } else if strcmp((*defel).defname, c"join".as_ptr()) == 0 {
            joinName = defGetQualifiedName(defel);
        } else if strcmp((*defel).defname, c"hashes".as_ptr()) == 0 {
            canHash = defGetBoolean(defel);
        } else if strcmp((*defel).defname, c"merges".as_ptr()) == 0 {
            canMerge = defGetBoolean(defel);
        }
        /* These obsolete options are taken as meaning canMerge */
        else if strcmp((*defel).defname, c"sort1".as_ptr()) == 0 {
            canMerge = true;
        } else if strcmp((*defel).defname, c"sort2".as_ptr()) == 0 {
            canMerge = true;
        } else if strcmp((*defel).defname, c"ltcmp".as_ptr()) == 0 {
            canMerge = true;
        } else if strcmp((*defel).defname, c"gtcmp".as_ptr()) == 0 {
            canMerge = true;
        } else {
            /* WARNING, not ERROR, for historical backwards-compatibility */
            elog!(
                WARNING,
                "operator attribute \"{}\" not recognized",
                CStr_to_str((*defel).defname)
            );
        }
    });

    /*
     * make sure we have our required definitions
     */
    if functionName == NIL {
        ereport!(ERROR, "operator function must be specified");
    }

    /* Transform type names to type OIDs */
    if !typeName1.is_null() {
        typeId1 = typenameTypeId(std::ptr::null_mut(), typeName1);
    }
    if !typeName2.is_null() {
        typeId2 = typenameTypeId(std::ptr::null_mut(), typeName2);
    }

    /*
     * If only the right argument is missing, the user is likely trying to
     * create a postfix operator, so give them a hint about why that does not
     * work.  But if both arguments are missing, do not mention postfix
     * operators, as the user most likely simply neglected to mention the
     * arguments.
     */
    if !OidIsValid(typeId1) && !OidIsValid(typeId2) {
        ereport!(ERROR, "operator argument types must be specified");
    }
    if !OidIsValid(typeId2) {
        ereport!(ERROR, "operator right argument type must be specified");
    }

    if !typeName1.is_null() {
        aclresult = object_aclcheck(TypeRelationId, typeId1, GetUserId(), ACL_USAGE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error_type(aclresult, typeId1);
        }
    }

    if !typeName2.is_null() {
        aclresult = object_aclcheck(TypeRelationId, typeId2, GetUserId(), ACL_USAGE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error_type(aclresult, typeId2);
        }
    }

    /*
     * Look up the operator's underlying function.
     */
    if !OidIsValid(typeId1) {
        typeId[0] = typeId2;
        nargs = 1;
    } else if !OidIsValid(typeId2) {
        typeId[0] = typeId1;
        nargs = 1;
    } else {
        typeId[0] = typeId1;
        typeId[1] = typeId2;
        nargs = 2;
    }
    functionOid = LookupFuncName(functionName, nargs, typeId.as_ptr(), false);

    /*
     * We require EXECUTE rights for the function.  This isn't strictly
     * necessary, since EXECUTE will be checked at any attempted use of the
     * operator, but it seems like a good idea anyway.
     */
    aclresult = object_aclcheck(ProcedureRelationId, functionOid, GetUserId(), ACL_EXECUTE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FUNCTION, NameListToString(functionName));
    }

    rettype = get_func_rettype(functionOid);
    aclresult = object_aclcheck(TypeRelationId, rettype, GetUserId(), ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error_type(aclresult, rettype);
    }

    /*
     * Look up restriction and join estimators if specified
     */
    if !restrictionName.is_null() {
        restrictionOid = ValidateRestrictionEstimator(restrictionName);
    } else {
        restrictionOid = InvalidOid;
    }
    if !joinName.is_null() {
        joinOid = ValidateJoinEstimator(joinName);
    } else {
        joinOid = InvalidOid;
    }

    /*
     * now have OperatorCreate do all the work..
     */
    OperatorCreate(
        oprName,        /* operator name */
        oprNamespace,   /* namespace */
        typeId1,        /* left type id */
        typeId2,        /* right type id */
        functionOid,    /* function for operator */
        commutatorName, /* optional commutator operator name */
        negatorName,    /* optional negator operator name */
        restrictionOid, /* optional restrict. sel. function */
        joinOid,        /* optional join sel. function name */
        canMerge,       /* operator merges */
        canHash,        /* operator hashes */
    )
}

/*
 * Look up a restriction estimator function by name, and verify that it has
 * the correct signature and we have the permissions to attach it to an
 * operator.
 */
unsafe fn ValidateRestrictionEstimator(restrictionName: *mut List) -> Oid {
    let mut typeId: [Oid; 4] = [0; 4];
    let restrictionOid: Oid;

    typeId[0] = INTERNALOID; /* PlannerInfo */
    typeId[1] = OIDOID; /* operator OID */
    typeId[2] = INTERNALOID; /* args list */
    typeId[3] = INT4OID; /* varRelid */

    restrictionOid = LookupFuncName(restrictionName, 4, typeId.as_ptr(), false);

    /* estimators must return float8 */
    if get_func_rettype(restrictionOid) != FLOAT8OID {
        elog!(
            ERROR,
            "restriction estimator function {} must return type {}",
            CStr_to_str(NameListToString(restrictionName)),
            "float8"
        );
    }

    /*
     * If the estimator is not a built-in function, require superuser
     * privilege to install it.  This protects against using something that is
     * not a restriction estimator or has hard-wired assumptions about what
     * data types it is working with.  (Built-in estimators are required to
     * defend themselves adequately against unexpected data type choices, but
     * it seems impractical to expect that of extensions' estimators.)
     *
     * If it is built-in, only require EXECUTE rights.
     */
    if restrictionOid >= FirstGenbkiObjectId {
        if !superuser() {
            ereport!(
                ERROR,
                "must be superuser to specify a non-built-in restriction estimator function"
            );
        }
    } else {
        let aclresult: AclResult =
            object_aclcheck(ProcedureRelationId, restrictionOid, GetUserId(), ACL_EXECUTE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(
                aclresult,
                OBJECT_FUNCTION,
                NameListToString(restrictionName),
            );
        }
    }

    restrictionOid
}

/*
 * Look up a join estimator function by name, and verify that it has the
 * correct signature and we have the permissions to attach it to an
 * operator.
 */
unsafe fn ValidateJoinEstimator(joinName: *mut List) -> Oid {
    let mut typeId: [Oid; 5] = [0; 5];
    let mut joinOid: Oid;
    let joinOid2: Oid;

    typeId[0] = INTERNALOID; /* PlannerInfo */
    typeId[1] = OIDOID; /* operator OID */
    typeId[2] = INTERNALOID; /* args list */
    typeId[3] = INT2OID; /* jointype */
    typeId[4] = INTERNALOID; /* SpecialJoinInfo */

    /*
     * As of Postgres 8.4, the preferred signature for join estimators has 5
     * arguments, but we still allow the old 4-argument form.  Whine about
     * ambiguity if both forms exist.
     */
    joinOid = LookupFuncName(joinName, 5, typeId.as_ptr(), true);
    joinOid2 = LookupFuncName(joinName, 4, typeId.as_ptr(), true);
    if OidIsValid(joinOid) {
        if OidIsValid(joinOid2) {
            elog!(
                ERROR,
                "join estimator function {} has multiple matches",
                CStr_to_str(NameListToString(joinName))
            );
        }
    } else {
        joinOid = joinOid2;
        /* If not found, reference the 5-argument signature in error msg */
        if !OidIsValid(joinOid) {
            joinOid = LookupFuncName(joinName, 5, typeId.as_ptr(), false);
        }
    }

    /* estimators must return float8 */
    if get_func_rettype(joinOid) != FLOAT8OID {
        elog!(
            ERROR,
            "join estimator function {} must return type {}",
            CStr_to_str(NameListToString(joinName)),
            "float8"
        );
    }

    /* privilege checks are the same as in ValidateRestrictionEstimator */
    if joinOid >= FirstGenbkiObjectId {
        if !superuser() {
            ereport!(
                ERROR,
                "must be superuser to specify a non-built-in join estimator function"
            );
        }
    } else {
        let aclresult: AclResult =
            object_aclcheck(ProcedureRelationId, joinOid, GetUserId(), ACL_EXECUTE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, OBJECT_FUNCTION, NameListToString(joinName));
        }
    }

    joinOid
}

/*
 * Look up and return the OID of an operator,
 * given a possibly-qualified name and left and right type IDs.
 *
 * Verifies that the operator is defined (not a shell) and owned by
 * the current user, so that we have permission to associate it with
 * the operator being altered.  Rejecting shell operators is a policy
 * choice to help catch mistakes, rather than something essential.
 */
unsafe fn ValidateOperatorReference(name: *mut List, leftTypeId: Oid, rightTypeId: Oid) -> Oid {
    let oid: Oid;
    let mut defined: bool = false;

    oid = OperatorLookup(name, leftTypeId, rightTypeId, &mut defined);

    /* These message strings are chosen to match parse_oper.c */
    if !OidIsValid(oid) {
        elog!(
            ERROR,
            "operator does not exist: {}",
            CStr_to_str(op_signature_string(name, leftTypeId, rightTypeId))
        );
    }

    if !defined {
        elog!(
            ERROR,
            "operator is only a shell: {}",
            CStr_to_str(op_signature_string(name, leftTypeId, rightTypeId))
        );
    }

    if !object_ownercheck(OperatorRelationId, oid, GetUserId()) {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_OPERATOR, NameListToString(name));
    }

    oid
}

/*
 * Guts of operator deletion.
 */
pub unsafe fn RemoveOperatorById(operOid: Oid) {
    let relation: Relation;
    let mut tup: HeapTuple;
    let op: Form_pg_operator;

    relation = table_open(OperatorRelationId, RowExclusiveLock);

    tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
    if !HeapTupleIsValid(tup) {
        /* should not happen */
        elog!(ERROR, "cache lookup failed for operator {}", operOid);
    }
    op = GETSTRUCT(tup) as Form_pg_operator;

    /*
     * Reset links from commutator and negator, if any.  In case of a
     * self-commutator or self-negator, this means we have to re-fetch the
     * updated tuple.  (We could optimize away updates on the tuple we're
     * about to drop, but it doesn't seem worth convoluting the logic for.)
     */
    if OidIsValid((*op).oprcom) || OidIsValid((*op).oprnegate) {
        OperatorUpd(operOid, (*op).oprcom, (*op).oprnegate, true);
        if operOid == (*op).oprcom || operOid == (*op).oprnegate {
            ReleaseSysCache(tup);
            tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
            if !HeapTupleIsValid(tup) {
                /* should not happen */
                elog!(ERROR, "cache lookup failed for operator {}", operOid);
            }
        }
    }

    CatalogTupleDelete(relation, &mut (*tup).t_self);

    ReleaseSysCache(tup);

    table_close(relation, RowExclusiveLock);
}

/*
 * AlterOperator
 *		routine implementing ALTER OPERATOR <operator> SET (option = ...).
 *
 * Currently, only RESTRICT and JOIN estimator functions can be changed.
 * COMMUTATOR, NEGATOR, MERGES, and HASHES attributes can be set if they
 * have not been set previously.  (Changing or removing one of these
 * attributes could invalidate existing plans, which seems more trouble
 * than it's worth.)
 */
pub unsafe fn AlterOperator(stmt: *mut AlterOperatorStmt) -> ObjectAddress {
    let address: ObjectAddress;
    let oprId: Oid;
    let catalog: Relation;
    let mut tup: HeapTuple;
    let oprForm: Form_pg_operator;
    let mut i: c_int;
    let mut values: [Datum; Natts_pg_operator] = [0; Natts_pg_operator];
    let mut nulls: [bool; Natts_pg_operator] = [false; Natts_pg_operator];
    let mut replaces: [bool; Natts_pg_operator] = [false; Natts_pg_operator];
    let mut restrictionName: *mut List = NIL; /* optional restrict. sel. function */
    let mut updateRestriction: bool = false;
    let restrictionOid: Oid;
    let mut joinName: *mut List = NIL; /* optional join sel. function */
    let mut updateJoin: bool = false;
    let joinOid: Oid;
    let mut commutatorName: *mut List = NIL; /* optional commutator operator name */
    let commutatorOid: Oid;
    let mut negatorName: *mut List = NIL; /* optional negator operator name */
    let negatorOid: Oid;
    let mut canMerge: bool = false;
    let mut updateMerges: bool = false;
    let mut canHash: bool = false;
    let mut updateHashes: bool = false;

    /* Look up the operator */
    oprId = LookupOperWithArgs((*stmt).opername, false);
    catalog = table_open(OperatorRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(OPEROID, ObjectIdGetDatum(oprId));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for operator {}", oprId);
    }
    oprForm = GETSTRUCT(tup) as Form_pg_operator;

    /* Process options */
    foreach!(pl, (*stmt).options, {
        let defel: *mut DefElem = lfirst(current_cell!(pl)) as *mut DefElem;
        let param: *mut List;

        if (*defel).arg.is_null() {
            param = NIL; /* NONE, removes the function */
        } else {
            param = defGetQualifiedName(defel);
        }

        if strcmp((*defel).defname, c"restrict".as_ptr()) == 0 {
            restrictionName = param;
            updateRestriction = true;
        } else if strcmp((*defel).defname, c"join".as_ptr()) == 0 {
            joinName = param;
            updateJoin = true;
        } else if strcmp((*defel).defname, c"commutator".as_ptr()) == 0 {
            commutatorName = defGetQualifiedName(defel);
        } else if strcmp((*defel).defname, c"negator".as_ptr()) == 0 {
            negatorName = defGetQualifiedName(defel);
        } else if strcmp((*defel).defname, c"merges".as_ptr()) == 0 {
            canMerge = defGetBoolean(defel);
            updateMerges = true;
        } else if strcmp((*defel).defname, c"hashes".as_ptr()) == 0 {
            canHash = defGetBoolean(defel);
            updateHashes = true;
        }
        /*
         * The rest of the options that CREATE accepts cannot be changed.
         * Check for them so that we can give a meaningful error message.
         */
        else if strcmp((*defel).defname, c"leftarg".as_ptr()) == 0
            || strcmp((*defel).defname, c"rightarg".as_ptr()) == 0
            || strcmp((*defel).defname, c"function".as_ptr()) == 0
            || strcmp((*defel).defname, c"procedure".as_ptr()) == 0
        {
            elog!(
                ERROR,
                "operator attribute \"{}\" cannot be changed",
                CStr_to_str((*defel).defname)
            );
        } else {
            elog!(
                ERROR,
                "operator attribute \"{}\" not recognized",
                CStr_to_str((*defel).defname)
            );
        }
    });

    /* Check permissions. Must be owner. */
    if !object_ownercheck(OperatorRelationId, oprId, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_OPERATOR,
            NameStr((*oprForm).oprname),
        );
    }

    /*
     * Look up OIDs for any parameters specified
     */
    if !restrictionName.is_null() {
        restrictionOid = ValidateRestrictionEstimator(restrictionName);
    } else {
        restrictionOid = InvalidOid;
    }
    if !joinName.is_null() {
        joinOid = ValidateJoinEstimator(joinName);
    } else {
        joinOid = InvalidOid;
    }

    if !commutatorName.is_null() {
        /* commutator has reversed arg types */
        commutatorOid = ValidateOperatorReference(
            commutatorName,
            (*oprForm).oprright,
            (*oprForm).oprleft,
        );

        /*
         * We don't need to do anything extra for a self commutator as in
         * OperatorCreate, since the operator surely exists already.
         */
    } else {
        commutatorOid = InvalidOid;
    }

    if !negatorName.is_null() {
        negatorOid =
            ValidateOperatorReference(negatorName, (*oprForm).oprleft, (*oprForm).oprright);

        /* Must reject self-negation */
        if negatorOid == (*oprForm).oid {
            ereport!(ERROR, "operator cannot be its own negator");
        }
    } else {
        negatorOid = InvalidOid;
    }

    /*
     * Check that we're not changing any attributes that might be depended on
     * by plans, while allowing no-op updates.
     */
    if OidIsValid(commutatorOid)
        && OidIsValid((*oprForm).oprcom)
        && commutatorOid != (*oprForm).oprcom
    {
        elog!(
            ERROR,
            "operator attribute \"{}\" cannot be changed if it has already been set",
            "commutator"
        );
    }

    if OidIsValid(negatorOid)
        && OidIsValid((*oprForm).oprnegate)
        && negatorOid != (*oprForm).oprnegate
    {
        elog!(
            ERROR,
            "operator attribute \"{}\" cannot be changed if it has already been set",
            "negator"
        );
    }

    if updateMerges && (*oprForm).oprcanmerge && !canMerge {
        elog!(
            ERROR,
            "operator attribute \"{}\" cannot be changed if it has already been set",
            "merges"
        );
    }

    if updateHashes && (*oprForm).oprcanhash && !canHash {
        elog!(
            ERROR,
            "operator attribute \"{}\" cannot be changed if it has already been set",
            "hashes"
        );
    }

    /* Perform additional checks, like OperatorCreate does */
    OperatorValidateParams(
        (*oprForm).oprleft,
        (*oprForm).oprright,
        (*oprForm).oprresult,
        OidIsValid(commutatorOid),
        OidIsValid(negatorOid),
        OidIsValid(restrictionOid),
        OidIsValid(joinOid),
        canMerge,
        canHash,
    );

    /* Update the tuple */
    i = 0;
    while i < Natts_pg_operator as c_int {
        values[i as usize] = 0 as Datum;
        replaces[i as usize] = false;
        nulls[i as usize] = false;
        i += 1;
    }
    if updateRestriction {
        replaces[(Anum_pg_operator_oprrest - 1) as usize] = true;
        values[(Anum_pg_operator_oprrest - 1) as usize] = ObjectIdGetDatum(restrictionOid);
    }
    if updateJoin {
        replaces[(Anum_pg_operator_oprjoin - 1) as usize] = true;
        values[(Anum_pg_operator_oprjoin - 1) as usize] = ObjectIdGetDatum(joinOid);
    }
    if OidIsValid(commutatorOid) {
        replaces[(Anum_pg_operator_oprcom - 1) as usize] = true;
        values[(Anum_pg_operator_oprcom - 1) as usize] = ObjectIdGetDatum(commutatorOid);
    }
    if OidIsValid(negatorOid) {
        replaces[(Anum_pg_operator_oprnegate - 1) as usize] = true;
        values[(Anum_pg_operator_oprnegate - 1) as usize] = ObjectIdGetDatum(negatorOid);
    }
    if updateMerges {
        replaces[(Anum_pg_operator_oprcanmerge - 1) as usize] = true;
        values[(Anum_pg_operator_oprcanmerge - 1) as usize] = BoolGetDatum(canMerge);
    }
    if updateHashes {
        replaces[(Anum_pg_operator_oprcanhash - 1) as usize] = true;
        values[(Anum_pg_operator_oprcanhash - 1) as usize] = BoolGetDatum(canHash);
    }

    tup = heap_modify_tuple(
        tup,
        RelationGetDescr(catalog),
        values.as_mut_ptr(),
        nulls.as_mut_ptr(),
        replaces.as_mut_ptr(),
    );

    CatalogTupleUpdate(catalog, &mut (*tup).t_self, tup);

    address = makeOperatorDependencies(tup, false, true);

    if OidIsValid(commutatorOid) || OidIsValid(negatorOid) {
        OperatorUpd(oprId, commutatorOid, negatorOid, false);
    }

    InvokeObjectPostAlterHook(OperatorRelationId, oprId, 0);

    table_close(catalog, NoLock);

    address
}

// ---------------------------------------------------------------------------
// Local inline helpers
// ---------------------------------------------------------------------------

#[inline]
unsafe fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

#[inline]
unsafe fn NameStr(name: crate::c::NameData) -> *const c_char {
    name.data.as_ptr()
}

#[inline]
unsafe fn CStr_to_str<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "";
    }
    std::ffi::CStr::from_ptr(s).to_str().unwrap_or("")
}

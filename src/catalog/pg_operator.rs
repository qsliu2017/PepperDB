//! Translation of postgres/src/include/catalog/pg_operator.h
//!
//! The `FormData_pg_operator` struct: the fixed-layout, guaranteed-not-null
//! part of a pg_operator catalog row.  The C header has NO `#ifdef
//! CATALOG_VARLEN` section, so every field of the catalog definition (from
//! `oid` through `oprjoin`) is part of this in-memory struct.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::c::NameData;
use crate::postgres_ext::Oid;
use core::ffi::c_char;

/* regproc is a C typedef for Oid (a registered-procedure OID). */
pub type regproc = Oid;

/*
 * FormData_pg_operator - the fixed part of a pg_operator row.
 *
 * #[repr(C)] so the field order/layout/size matches the C struct exactly; for
 * types used in system tables it is critical that the size and alignment
 * defined here agree with the way the compiler lays out the field in a struct
 * representing a table row.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FormData_pg_operator {
    /* oid */
    pub oid: Oid,
    /* name of operator */
    pub oprname: NameData,
    /* OID of namespace containing this oper */
    pub oprnamespace: Oid,
    /* operator owner */
    pub oprowner: Oid,
    /* 'l' for prefix or 'b' for infix */
    pub oprkind: c_char,
    /* can be used in merge join? */
    pub oprcanmerge: bool,
    /* can be used in hash join? */
    pub oprcanhash: bool,
    /* left arg type, or 0 if prefix operator */
    pub oprleft: Oid,
    /* right arg type */
    pub oprright: Oid,
    /* result datatype; can be 0 in a "shell" operator */
    pub oprresult: Oid,
    /* OID of commutator oper, or 0 if none */
    pub oprcom: Oid,
    /* OID of negator oper, or 0 if none */
    pub oprnegate: Oid,
    /* OID of underlying function; can be 0 in a "shell" operator */
    pub oprcode: regproc,
    /* OID of restriction estimator, or 0 */
    pub oprrest: regproc,
    /* OID of join estimator, or 0 */
    pub oprjoin: regproc,
}

/*
 * Form_pg_operator corresponds to a pointer to a row with the format of the
 * pg_operator relation.
 */
pub type Form_pg_operator = *mut FormData_pg_operator;

/* ----------------------------------------------------------------
 * EXPOSE_TO_CLIENT_CODE constants.
 *
 * The pg_operator.h header defines no EXPOSE_TO_CLIENT_CODE macros; the
 * oprkind values ('l' prefix, 'b' infix) are documented inline only.
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 * Translation of postgres/src/backend/catalog/pg_operator.c
 *
 *	  routines to support manipulation of the pg_operator relation
 * ----------------------------------------------------------------
 */

use crate::prelude::*;
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple, heap_modify_tuple};
use crate::access::table::table::{table_open, table_close, LOCKMODE};
use crate::catalog::indexing::{CatalogTupleInsert, CatalogTupleUpdate};
use crate::catalog::catalog::GetNewOidWithIndex;
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::objectaddress_impl::{
    SearchSysCache4, SearchSysCacheCopy1, ReleaseSysCache, NameListToString,
};
use crate::catalog::aclchk::{object_ownercheck, object_aclcheck, aclcheck_error};
use crate::catalog::namespace::QualifiedNameGetCreationNamespace;
use crate::catalog::dependency::{
    ObjectAddresses, new_object_addresses, add_exact_object_address,
    record_object_address_dependencies, free_object_addresses, DEPENDENCY_NORMAL,
};
use crate::catalog::pg_depend::recordDependencyOnCurrentExtension;
use crate::nodes::pg_list::List;
use crate::nodes::parsenodes::{ObjectType, AclMode, ACL_CREATE};
use crate::utils::adt::acl::{AclResult, AclResult::ACLCHECK_OK, AclResult::ACLCHECK_NOT_OWNER};
use crate::optimizer::path::costsize::get_opcode;
use crate::access::transam::xact::CommandCounterIncrement;
use crate::miscadmin::GetUserId;
use crate::parser::parse_oper::LookupOperName;
use crate::utils::cache::lsyscache::{get_func_rettype, get_opname, get_namespace_name};
use crate::utils::builtins::namestrcpy;
use crate::postgres::{
    Datum, ObjectIdGetDatum, PointerGetDatum, CharGetDatum, BoolGetDatum,
};
use crate::c::{OidIsValid, RegProcedureIsValid, RegProcedure, NameStr};
use crate::access::common::tupdesc::TupleDesc;
use crate::utils::rel::Relation;

/* ----------------------------------------------------------------
 * Local consts / type aliases for not-yet-wired dependencies.
 * ----------------------------------------------------------------
 */
const InvalidOid: Oid = 0;
const NIL: *mut List = core::ptr::null_mut();

const NAMEDATALEN: usize = 64;
const BOOLOID: Oid = 16;

const RowExclusiveLock: LOCKMODE = 4;

const OperatorRelationId: Oid = 2617;
const NamespaceRelationId: Oid = 2615;
const TypeRelationId: Oid = 1247;
const ProcedureRelationId: Oid = 1255;

const OperatorOidIndexId: Oid = 2688;

/* syscache ids */
const OPERNAMENSP: c_int = 39;
const OPEROID: c_int = 40;

/* error codes */
const ERRCODE_INVALID_NAME: c_int = 0;
const ERRCODE_DUPLICATE_FUNCTION: c_int = 0;
const ERRCODE_INVALID_FUNCTION_DEFINITION: c_int = 0;

/* attribute numbers (1-based), from pg_operator catalog definition */
const Natts_pg_operator: usize = 15;
const Anum_pg_operator_oid: usize = 1;
const Anum_pg_operator_oprname: usize = 2;
const Anum_pg_operator_oprnamespace: usize = 3;
const Anum_pg_operator_oprowner: usize = 4;
const Anum_pg_operator_oprkind: usize = 5;
const Anum_pg_operator_oprcanmerge: usize = 6;
const Anum_pg_operator_oprcanhash: usize = 7;
const Anum_pg_operator_oprleft: usize = 8;
const Anum_pg_operator_oprright: usize = 9;
const Anum_pg_operator_oprresult: usize = 10;
const Anum_pg_operator_oprcom: usize = 11;
const Anum_pg_operator_oprnegate: usize = 12;
const Anum_pg_operator_oprcode: usize = 13;
const Anum_pg_operator_oprrest: usize = 14;
const Anum_pg_operator_oprjoin: usize = 15;

const OBJECT_OPERATOR: ObjectType = ObjectType::OBJECT_OPERATOR;
const OBJECT_SCHEMA: ObjectType = ObjectType::OBJECT_SCHEMA;

/* ObjectAddressSet: catalog/objectaddress.h convenience setter. */
#[inline]
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}

/* NameGetDatum has no clean home yet; it is just a pointer-to-Datum cast. */
#[inline]
unsafe fn NameGetDatum(name: *const NameData) -> Datum {
    PointerGetDatum(name as *const c_void)
}

/* InvokeObjectPostCreateHook: no-op unless object_access_hook is set. */
unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: c_int) {
    /* TODO(pg-port): catalog/objectaccess.h hook dispatch */
}

/* deleteDependencyRecordsFor: catalog/dependency.c (TODO(pg-port)) */
unsafe fn deleteDependencyRecordsFor(_classId: Oid, _objectId: Oid, _skipExtensionDeps: bool) -> c_long {
    /* TODO(pg-port): catalog/pg_depend.c */
    0
}

/* deleteSharedDependencyRecordsFor: catalog/pg_shdepend.c (TODO(pg-port)) */
unsafe fn deleteSharedDependencyRecordsFor(_classId: Oid, _objectId: Oid, _objectSubId: int32) { crate::catalog::pg_shdepend::deleteSharedDependencyRecordsFor(_classId as _, _objectId as _, _objectSubId as _) }

/* recordDependencyOnOwner: catalog/pg_shdepend.c (TODO(pg-port)) */
unsafe fn recordDependencyOnOwner(_classId: Oid, _objectId: Oid, _owner: Oid) {
    /* TODO(pg-port): catalog/pg_shdepend.c */
}

extern "C" {
    fn strlen(s: *const c_char) -> usize;
    fn strspn(s: *const c_char, accept: *const c_char) -> usize;
    fn strstr(haystack: *const c_char, needle: *const c_char) -> *mut c_char;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

/*
 * Check whether a proposed operator name is legal
 *
 * This had better match the behavior of parser/scan.l!
 *
 * We need this because the parser is not smart enough to check that
 * the arguments of CREATE OPERATOR's COMMUTATOR, NEGATOR, etc clauses
 * are operator names rather than some other lexical entity.
 */
unsafe fn validOperatorName(name: *const c_char) -> bool {
    let len: usize = strlen(name);

    /* Can't be empty or too long */
    if len == 0 || len >= NAMEDATALEN {
        return false;
    }

    /* Can't contain any invalid characters */
    /* Test string here should match op_chars in scan.l */
    if strspn(name, c"~!@#^&|`?+-*/%<>=".as_ptr()) != len {
        return false;
    }

    /* Can't contain slash-star or dash-dash (comment starts) */
    if !strstr(name, c"/*".as_ptr()).is_null() || !strstr(name, c"--".as_ptr()).is_null() {
        return false;
    }

    /*
     * For SQL standard compatibility, '+' and '-' cannot be the last char of
     * a multi-char operator unless the operator contains chars that are not
     * in SQL operators. The idea is to lex '=-' as two operators, but not to
     * forbid operator names like '?-' that could not be sequences of standard
     * SQL operators.
     */
    if len > 1
        && (*name.add(len - 1) == b'+' as c_char || *name.add(len - 1) == b'-' as c_char)
    {
        let mut ic: isize = (len as isize) - 2;
        while ic >= 0 {
            if !strchr(c"~!@#^&|`?%".as_ptr(), *name.add(ic as usize) as c_int).is_null() {
                break;
            }
            ic -= 1;
        }
        if ic < 0 {
            return false; /* nope, not valid */
        }
    }

    /* != isn't valid either, because parser will convert it to <> */
    if strcmp(name, c"!=".as_ptr()) == 0 {
        return false;
    }

    true
}

/*
 * OperatorGet
 *
 *		finds an operator given an exact specification (name, namespace,
 *		left and right type IDs).
 *
 *		*defined is set true if defined (not a shell)
 */
unsafe fn OperatorGet(
    operatorName: *const c_char,
    operatorNamespace: Oid,
    leftObjectId: Oid,
    rightObjectId: Oid,
    defined: *mut bool,
) -> Oid {
    let operatorObjectId: Oid;

    let tup: HeapTuple = SearchSysCache4(
        OPERNAMENSP,
        PointerGetDatum(operatorName as *const c_void),
        ObjectIdGetDatum(leftObjectId),
        ObjectIdGetDatum(rightObjectId),
        ObjectIdGetDatum(operatorNamespace),
    );
    if HeapTupleIsValid(tup) {
        let oprform: Form_pg_operator = GETSTRUCT(tup) as Form_pg_operator;

        operatorObjectId = (*oprform).oid;
        *defined = RegProcedureIsValid((*oprform).oprcode);
        ReleaseSysCache(tup);
    } else {
        operatorObjectId = InvalidOid;
        *defined = false;
    }

    operatorObjectId
}

/*
 * OperatorLookup
 *
 *		looks up an operator given a possibly-qualified name and
 *		left and right type IDs.
 *
 *		*defined is set true if defined (not a shell)
 */
pub unsafe fn OperatorLookup(
    operatorName: *mut List,
    leftObjectId: Oid,
    rightObjectId: Oid,
    defined: *mut bool,
) -> Oid {
    let operatorObjectId: Oid;
    let oprcode: RegProcedure;

    operatorObjectId = LookupOperName(
        core::ptr::null_mut(),
        operatorName,
        leftObjectId,
        rightObjectId,
        true,
        -1,
    );
    if !OidIsValid(operatorObjectId) {
        *defined = false;
        return InvalidOid;
    }

    oprcode = get_opcode(operatorObjectId);
    *defined = RegProcedureIsValid(oprcode);

    operatorObjectId
}

/*
 * OperatorShellMake
 *		Make a "shell" entry for a not-yet-existing operator.
 */
unsafe fn OperatorShellMake(
    operatorName: *const c_char,
    operatorNamespace: Oid,
    leftTypeId: Oid,
    rightTypeId: Oid,
) -> Oid {
    let pg_operator_desc: Relation;
    let operatorObjectId: Oid;
    let mut i: usize;
    let tup: HeapTuple;
    let mut values: [Datum; Natts_pg_operator] = [0; Natts_pg_operator];
    let mut nulls: [bool; Natts_pg_operator] = [false; Natts_pg_operator];
    let mut oname: NameData = core::mem::zeroed();
    let tupDesc: TupleDesc;

    /*
     * validate operator name
     */
    if !validOperatorName(operatorName) {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a valid operator name",
                core::ffi::CStr::from_ptr(operatorName).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_NAME) */
        let _ = ERRCODE_INVALID_NAME;
    }

    /*
     * open pg_operator
     */
    pg_operator_desc = table_open(OperatorRelationId, RowExclusiveLock);
    tupDesc = (*pg_operator_desc).rd_att;

    /*
     * initialize our *nulls and *values arrays
     */
    i = 0;
    while i < Natts_pg_operator {
        nulls[i] = false;
        values[i] = 0 as Datum; /* redundant, but safe */
        i += 1;
    }

    /*
     * initialize values[] with the operator name and input data types. Note
     * that oprcode is set to InvalidOid, indicating it's a shell.
     */
    operatorObjectId =
        GetNewOidWithIndex(pg_operator_desc, OperatorOidIndexId, Anum_pg_operator_oid as i16);
    values[Anum_pg_operator_oid - 1] = ObjectIdGetDatum(operatorObjectId);
    namestrcpy(&mut oname, operatorName);
    values[Anum_pg_operator_oprname - 1] = NameGetDatum(&oname);
    values[Anum_pg_operator_oprnamespace - 1] = ObjectIdGetDatum(operatorNamespace);
    values[Anum_pg_operator_oprowner - 1] = ObjectIdGetDatum(GetUserId());
    values[Anum_pg_operator_oprkind - 1] =
        CharGetDatum(if leftTypeId != 0 { b'b' as c_char } else { b'l' as c_char });
    values[Anum_pg_operator_oprcanmerge - 1] = BoolGetDatum(false);
    values[Anum_pg_operator_oprcanhash - 1] = BoolGetDatum(false);
    values[Anum_pg_operator_oprleft - 1] = ObjectIdGetDatum(leftTypeId);
    values[Anum_pg_operator_oprright - 1] = ObjectIdGetDatum(rightTypeId);
    values[Anum_pg_operator_oprresult - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_operator_oprcom - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_operator_oprnegate - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_operator_oprcode - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_operator_oprrest - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_operator_oprjoin - 1] = ObjectIdGetDatum(InvalidOid);

    /*
     * create a new operator tuple
     */
    tup = heap_form_tuple(tupDesc, values.as_mut_ptr(), nulls.as_mut_ptr());

    /*
     * insert our "shell" operator tuple
     */
    CatalogTupleInsert(pg_operator_desc, tup);

    /* Add dependencies for the entry */
    makeOperatorDependencies(tup, true, false);

    heap_freetuple(tup);

    /* Post creation hook for new shell operator */
    InvokeObjectPostCreateHook(OperatorRelationId, operatorObjectId, 0);

    /*
     * Make sure the tuple is visible for subsequent lookups/updates.
     */
    CommandCounterIncrement();

    /*
     * close the operator relation and return the oid.
     */
    table_close(pg_operator_desc, RowExclusiveLock);

    operatorObjectId
}

/*
 * OperatorCreate
 *
 * The caller should have validated properties and permissions for the
 * objects passed as OID references.  We must handle the commutator and
 * negator operator references specially, however, since those need not
 * exist beforehand.
 */
pub unsafe fn OperatorCreate(
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
    let pg_operator_desc: Relation;
    let mut tup: HeapTuple;
    let isUpdate: bool;
    let mut nulls: [bool; Natts_pg_operator] = [false; Natts_pg_operator];
    let mut replaces: [bool; Natts_pg_operator] = [false; Natts_pg_operator];
    let mut values: [Datum; Natts_pg_operator] = [0; Natts_pg_operator];
    let mut operatorObjectId: Oid;
    let mut operatorAlreadyDefined: bool = false;
    let operResultType: Oid;
    let mut commutatorId: Oid;
    let negatorId: Oid;
    let mut selfCommutator: bool = false;
    let mut oname: NameData = core::mem::zeroed();
    let mut i: usize;
    let address: ObjectAddress;

    /*
     * Sanity checks
     */
    if !validOperatorName(operatorName) {
        ereport!(
            ERROR,
            errmsg!(
                "\"{}\" is not a valid operator name",
                core::ffi::CStr::from_ptr(operatorName).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_INVALID_NAME) */
        let _ = ERRCODE_INVALID_NAME;
    }

    operResultType = get_func_rettype(procedureId);

    OperatorValidateParams(
        leftTypeId,
        rightTypeId,
        operResultType,
        commutatorName != NIL,
        negatorName != NIL,
        OidIsValid(restrictionId),
        OidIsValid(joinId),
        canMerge,
        canHash,
    );

    operatorObjectId = OperatorGet(
        operatorName,
        operatorNamespace,
        leftTypeId,
        rightTypeId,
        &mut operatorAlreadyDefined,
    );

    if operatorAlreadyDefined {
        ereport!(
            ERROR,
            errmsg!(
                "operator {} already exists",
                core::ffi::CStr::from_ptr(operatorName).to_string_lossy()
            )
        );
        /* C also: errcode(ERRCODE_DUPLICATE_FUNCTION) */
        let _ = ERRCODE_DUPLICATE_FUNCTION;
    }

    /*
     * At this point, if operatorObjectId is not InvalidOid then we are
     * filling in a previously-created shell.  Insist that the user own any
     * such shell.
     */
    if OidIsValid(operatorObjectId)
        && !object_ownercheck(OperatorRelationId, operatorObjectId, GetUserId())
    {
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_OPERATOR, operatorName);
    }

    /*
     * Set up the other operators.  If they do not currently exist, create
     * shells in order to get ObjectId's.
     */

    if !commutatorName.is_null() {
        /* commutator has reversed arg types */
        commutatorId = get_other_operator(
            commutatorName,
            rightTypeId,
            leftTypeId,
            operatorName,
            operatorNamespace,
            leftTypeId,
            rightTypeId,
        );

        /* Permission check: must own other operator */
        if OidIsValid(commutatorId)
            && !object_ownercheck(OperatorRelationId, commutatorId, GetUserId())
        {
            aclcheck_error(
                ACLCHECK_NOT_OWNER,
                OBJECT_OPERATOR,
                NameListToString(commutatorName),
            );
        }

        /*
         * If self-linkage to the new operator is requested, we'll fix it
         * below.  (In case of self-linkage to an existing shell operator, we
         * need do nothing special.)
         */
        if !OidIsValid(commutatorId) {
            selfCommutator = true;
        }
    } else {
        commutatorId = InvalidOid;
    }

    if !negatorName.is_null() {
        /* negator has same arg types */
        negatorId = get_other_operator(
            negatorName,
            leftTypeId,
            rightTypeId,
            operatorName,
            operatorNamespace,
            leftTypeId,
            rightTypeId,
        );

        /* Permission check: must own other operator */
        if OidIsValid(negatorId)
            && !object_ownercheck(OperatorRelationId, negatorId, GetUserId())
        {
            aclcheck_error(
                ACLCHECK_NOT_OWNER,
                OBJECT_OPERATOR,
                NameListToString(negatorName),
            );
        }

        /*
         * Prevent self negation, as it doesn't make sense.  It's self
         * negation if result is InvalidOid (negator would be the same
         * operator but it doesn't exist yet) or operatorObjectId (we are
         * replacing a shell that would need to be its own negator).
         */
        if !OidIsValid(negatorId) || negatorId == operatorObjectId {
            ereport!(
                ERROR,
                errmsg!("operator cannot be its own negator")
            );
            /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
            let _ = ERRCODE_INVALID_FUNCTION_DEFINITION;
        }
    } else {
        negatorId = InvalidOid;
    }

    /*
     * set up values in the operator tuple
     */

    i = 0;
    while i < Natts_pg_operator {
        values[i] = 0 as Datum;
        replaces[i] = true;
        nulls[i] = false;
        i += 1;
    }

    namestrcpy(&mut oname, operatorName);
    values[Anum_pg_operator_oprname - 1] = NameGetDatum(&oname);
    values[Anum_pg_operator_oprnamespace - 1] = ObjectIdGetDatum(operatorNamespace);
    values[Anum_pg_operator_oprowner - 1] = ObjectIdGetDatum(GetUserId());
    values[Anum_pg_operator_oprkind - 1] =
        CharGetDatum(if leftTypeId != 0 { b'b' as c_char } else { b'l' as c_char });
    values[Anum_pg_operator_oprcanmerge - 1] = BoolGetDatum(canMerge);
    values[Anum_pg_operator_oprcanhash - 1] = BoolGetDatum(canHash);
    values[Anum_pg_operator_oprleft - 1] = ObjectIdGetDatum(leftTypeId);
    values[Anum_pg_operator_oprright - 1] = ObjectIdGetDatum(rightTypeId);
    values[Anum_pg_operator_oprresult - 1] = ObjectIdGetDatum(operResultType);
    values[Anum_pg_operator_oprcom - 1] = ObjectIdGetDatum(commutatorId);
    values[Anum_pg_operator_oprnegate - 1] = ObjectIdGetDatum(negatorId);
    values[Anum_pg_operator_oprcode - 1] = ObjectIdGetDatum(procedureId);
    values[Anum_pg_operator_oprrest - 1] = ObjectIdGetDatum(restrictionId);
    values[Anum_pg_operator_oprjoin - 1] = ObjectIdGetDatum(joinId);

    pg_operator_desc = table_open(OperatorRelationId, RowExclusiveLock);

    /*
     * If we are replacing an operator shell, update; else insert
     */
    if operatorObjectId != 0 {
        isUpdate = true;

        tup = SearchSysCacheCopy1(OPEROID, ObjectIdGetDatum(operatorObjectId));
        if !HeapTupleIsValid(tup) {
            elog!(ERROR, "cache lookup failed for operator {}", operatorObjectId);
        }

        replaces[Anum_pg_operator_oid - 1] = false;
        tup = heap_modify_tuple(
            tup,
            (*pg_operator_desc).rd_att,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
            replaces.as_mut_ptr(),
        );

        CatalogTupleUpdate(pg_operator_desc, &mut (*tup).t_self, tup);
    } else {
        isUpdate = false;

        operatorObjectId =
            GetNewOidWithIndex(pg_operator_desc, OperatorOidIndexId, Anum_pg_operator_oid as i16);
        values[Anum_pg_operator_oid - 1] = ObjectIdGetDatum(operatorObjectId);

        tup = heap_form_tuple(
            (*pg_operator_desc).rd_att,
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
        );

        CatalogTupleInsert(pg_operator_desc, tup);
    }

    /* Add dependencies for the entry */
    address = makeOperatorDependencies(tup, true, isUpdate);

    /*
     * If a commutator and/or negator link is provided, update the other
     * operator(s) to point at this one, if they don't already have a link.
     */
    if selfCommutator {
        commutatorId = operatorObjectId;
    }

    if OidIsValid(commutatorId) || OidIsValid(negatorId) {
        OperatorUpd(operatorObjectId, commutatorId, negatorId, false);
    }

    /* Post creation hook for new operator */
    InvokeObjectPostCreateHook(OperatorRelationId, operatorObjectId, 0);

    table_close(pg_operator_desc, RowExclusiveLock);

    address
}

/*
 * OperatorValidateParams
 *
 * Check that an operator with argument types leftTypeId and rightTypeId,
 * returning operResultType, can have the attributes that are set to true.
 * Raise an error for any disallowed attribute.
 */
pub unsafe fn OperatorValidateParams(
    leftTypeId: Oid,
    rightTypeId: Oid,
    operResultType: Oid,
    hasCommutator: bool,
    hasNegator: bool,
    hasRestrictionSelectivity: bool,
    hasJoinSelectivity: bool,
    canMerge: bool,
    canHash: bool,
) {
    let _ = ERRCODE_INVALID_FUNCTION_DEFINITION;

    if !(OidIsValid(leftTypeId) && OidIsValid(rightTypeId)) {
        /* If it's not a binary op, these things mustn't be set: */
        if hasCommutator {
            ereport!(ERROR, errmsg!("only binary operators can have commutators"));
        }
        if hasJoinSelectivity {
            ereport!(ERROR, errmsg!("only binary operators can have join selectivity"));
        }
        if canMerge {
            ereport!(ERROR, errmsg!("only binary operators can merge join"));
        }
        if canHash {
            ereport!(ERROR, errmsg!("only binary operators can hash"));
        }
    }

    if operResultType != BOOLOID {
        /* If it's not a boolean op, these things mustn't be set: */
        if hasNegator {
            ereport!(ERROR, errmsg!("only boolean operators can have negators"));
        }
        if hasRestrictionSelectivity {
            ereport!(ERROR, errmsg!("only boolean operators can have restriction selectivity"));
        }
        if hasJoinSelectivity {
            ereport!(ERROR, errmsg!("only boolean operators can have join selectivity"));
        }
        if canMerge {
            ereport!(ERROR, errmsg!("only boolean operators can merge join"));
        }
        if canHash {
            ereport!(ERROR, errmsg!("only boolean operators can hash"));
        }
    }
}

/*
 * Try to lookup another operator (commutator, etc); return its OID
 *
 * If not found, check to see if it would be the same operator we are trying
 * to define; if so, return InvalidOid.  (Caller must decide whether
 * that is sensible.)  If it is not the same operator, create a shell
 * operator.
 */
unsafe fn get_other_operator(
    otherOp: *mut List,
    otherLeftTypeId: Oid,
    otherRightTypeId: Oid,
    operatorName: *const c_char,
    operatorNamespace: Oid,
    leftTypeId: Oid,
    rightTypeId: Oid,
) -> Oid {
    let mut other_oid: Oid;
    let mut otherDefined: bool = false;
    let mut otherName: *mut c_char = core::ptr::null_mut();
    let otherNamespace: Oid;
    let aclresult: AclResult;

    other_oid = OperatorLookup(otherOp, otherLeftTypeId, otherRightTypeId, &mut otherDefined);

    if OidIsValid(other_oid) {
        /* other op already in catalogs */
        return other_oid;
    }

    otherNamespace = QualifiedNameGetCreationNamespace(otherOp as *const List, &mut otherName);

    if strcmp(otherName, operatorName) == 0
        && otherNamespace == operatorNamespace
        && otherLeftTypeId == leftTypeId
        && otherRightTypeId == rightTypeId
    {
        /* self-linkage to new operator; caller must handle this */
        return InvalidOid;
    }

    /* not in catalogs, different from operator, so make shell */

    aclresult = object_aclcheck(NamespaceRelationId, otherNamespace, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(otherNamespace));
    }

    other_oid = OperatorShellMake(otherName, otherNamespace, otherLeftTypeId, otherRightTypeId);
    other_oid
}

/*
 * OperatorUpd
 *
 *	For a given operator, look up its negator and commutator operators.
 *	When isDelete is false, update their negator and commutator fields to
 *	point back to the given operator; when isDelete is true, update those
 *	fields to be InvalidOid.
 */
pub unsafe fn OperatorUpd(baseId: Oid, commId: Oid, negId: Oid, isDelete: bool) {
    let pg_operator_desc: Relation;
    let mut tup: HeapTuple;

    /*
     * If we're making an operator into its own commutator, then we need a
     * command-counter increment here, since we've just inserted the tuple
     * we're about to update.  But when we're dropping an operator, we can
     * skip this because we're at the beginning of the command.
     */
    if !isDelete {
        CommandCounterIncrement();
    }

    /* Open the relation. */
    pg_operator_desc = table_open(OperatorRelationId, RowExclusiveLock);

    /* Get a writable copy of the commutator's tuple. */
    if OidIsValid(commId) {
        tup = SearchSysCacheCopy1(OPEROID, ObjectIdGetDatum(commId));
    } else {
        tup = core::ptr::null_mut();
    }

    /* Update the commutator's tuple if need be. */
    if HeapTupleIsValid(tup) {
        let t: Form_pg_operator = GETSTRUCT(tup) as Form_pg_operator;
        let mut update_commutator: bool = false;

        /*
         * We can skip doing anything if the commutator's oprcom field is
         * already what we want.  While that's not expected in the isDelete
         * case, it's perfectly possible when filling in a shell operator.
         */
        if isDelete && OidIsValid((*t).oprcom) {
            (*t).oprcom = InvalidOid;
            update_commutator = true;
        } else if !isDelete && (*t).oprcom != baseId {
            /*
             * If commutator's oprcom field is already set to point to some
             * third operator, it's an error.  Changing its link would be
             * unsafe, and letting the inconsistency stand would not be good
             * either.  This might be indicative of catalog corruption, so
             * don't assume t->oprcom is necessarily a valid operator.
             */
            if OidIsValid((*t).oprcom) {
                let thirdop: *mut c_char = get_opname((*t).oprcom);

                if !thirdop.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "commutator operator {} is already the commutator of operator {}",
                            core::ffi::CStr::from_ptr(NameStr(&(*t).oprname)).to_string_lossy(),
                            core::ffi::CStr::from_ptr(thirdop).to_string_lossy()
                        )
                    );
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
                } else {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "commutator operator {} is already the commutator of operator {}",
                            core::ffi::CStr::from_ptr(NameStr(&(*t).oprname)).to_string_lossy(),
                            (*t).oprcom
                        )
                    );
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
                }
            }

            (*t).oprcom = baseId;
            update_commutator = true;
        }

        /* If any columns were found to need modification, update tuple. */
        if update_commutator {
            CatalogTupleUpdate(pg_operator_desc, &mut (*tup).t_self, tup);

            /*
             * Do CCI to make the updated tuple visible.  We must do this in
             * case the commutator is also the negator.
             */
            CommandCounterIncrement();
        }
    }

    /*
     * Similarly find and update the negator, if any.
     */
    if OidIsValid(negId) {
        tup = SearchSysCacheCopy1(OPEROID, ObjectIdGetDatum(negId));
    } else {
        tup = core::ptr::null_mut();
    }

    if HeapTupleIsValid(tup) {
        let t: Form_pg_operator = GETSTRUCT(tup) as Form_pg_operator;
        let mut update_negator: bool = false;

        /*
         * We can skip doing anything if the negator's oprnegate field is
         * already what we want.  While that's not expected in the isDelete
         * case, it's perfectly possible when filling in a shell operator.
         */
        if isDelete && OidIsValid((*t).oprnegate) {
            (*t).oprnegate = InvalidOid;
            update_negator = true;
        } else if !isDelete && (*t).oprnegate != baseId {
            /*
             * If negator's oprnegate field is already set to point to some
             * third operator, it's an error.
             */
            if OidIsValid((*t).oprnegate) {
                let thirdop: *mut c_char = get_opname((*t).oprnegate);

                if !thirdop.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "negator operator {} is already the negator of operator {}",
                            core::ffi::CStr::from_ptr(NameStr(&(*t).oprname)).to_string_lossy(),
                            core::ffi::CStr::from_ptr(thirdop).to_string_lossy()
                        )
                    );
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
                } else {
                    ereport!(
                        ERROR,
                        errmsg!(
                            "negator operator {} is already the negator of operator {}",
                            core::ffi::CStr::from_ptr(NameStr(&(*t).oprname)).to_string_lossy(),
                            (*t).oprnegate
                        )
                    );
                    /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION) */
                }
            }

            (*t).oprnegate = baseId;
            update_negator = true;
        }

        /* If any columns were found to need modification, update tuple. */
        if update_negator {
            CatalogTupleUpdate(pg_operator_desc, &mut (*tup).t_self, tup);

            /*
             * In the deletion case, do CCI to make the updated tuple visible.
             */
            if isDelete {
                CommandCounterIncrement();
            }
        }
    }

    /* Close relation and release catalog lock. */
    table_close(pg_operator_desc, RowExclusiveLock);
}

/*
 * Create dependencies for an operator (either a freshly inserted
 * complete operator, a new shell operator, a just-updated shell,
 * or an operator that's being modified by ALTER OPERATOR).
 *
 * makeExtensionDep should be true when making a new operator or
 * replacing a shell, false for ALTER OPERATOR.  Passing false
 * will prevent any change in the operator's extension membership.
 *
 * NB: the OidIsValid tests in this routine are necessary, in case
 * the given operator is a shell.
 */
pub unsafe fn makeOperatorDependencies(
    tuple: HeapTuple,
    makeExtensionDep: bool,
    isUpdate: bool,
) -> ObjectAddress {
    let oper: Form_pg_operator = GETSTRUCT(tuple) as Form_pg_operator;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();
    let addrs: *mut ObjectAddresses;

    ObjectAddressSet(&mut myself, OperatorRelationId, (*oper).oid);

    /*
     * If we are updating the operator, delete any existing entries, except
     * for extension membership which should remain the same.
     */
    if isUpdate {
        deleteDependencyRecordsFor(myself.classId, myself.objectId, true);
        deleteSharedDependencyRecordsFor(myself.classId, myself.objectId, 0);
    }

    addrs = new_object_addresses();

    /* Dependency on namespace */
    if OidIsValid((*oper).oprnamespace) {
        ObjectAddressSet(&mut referenced, NamespaceRelationId, (*oper).oprnamespace);
        add_exact_object_address(&mut referenced, addrs);
    }

    /* Dependency on left type */
    if OidIsValid((*oper).oprleft) {
        ObjectAddressSet(&mut referenced, TypeRelationId, (*oper).oprleft);
        add_exact_object_address(&mut referenced, addrs);
    }

    /* Dependency on right type */
    if OidIsValid((*oper).oprright) {
        ObjectAddressSet(&mut referenced, TypeRelationId, (*oper).oprright);
        add_exact_object_address(&mut referenced, addrs);
    }

    /* Dependency on result type */
    if OidIsValid((*oper).oprresult) {
        ObjectAddressSet(&mut referenced, TypeRelationId, (*oper).oprresult);
        add_exact_object_address(&mut referenced, addrs);
    }

    /*
     * NOTE: we do not consider the operator to depend on the associated
     * operators oprcom and oprnegate.  We do not want to delete this operator
     * if those go away, but only reset the link fields; which is not a
     * function that the dependency logic can handle.  (It's taken care of
     * manually within RemoveOperatorById, instead.)
     */

    /* Dependency on implementation function */
    if OidIsValid((*oper).oprcode) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*oper).oprcode);
        add_exact_object_address(&mut referenced, addrs);
    }

    /* Dependency on restriction selectivity function */
    if OidIsValid((*oper).oprrest) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*oper).oprrest);
        add_exact_object_address(&mut referenced, addrs);
    }

    /* Dependency on join selectivity function */
    if OidIsValid((*oper).oprjoin) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, (*oper).oprjoin);
        add_exact_object_address(&mut referenced, addrs);
    }

    record_object_address_dependencies(&mut myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    /* Dependency on owner */
    recordDependencyOnOwner(OperatorRelationId, (*oper).oid, (*oper).oprowner);

    /* Dependency on extension */
    if makeExtensionDep {
        recordDependencyOnCurrentExtension(&mut myself, isUpdate);
    }

    myself
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_part_layout() {
        // oprname sits right after the 4-byte oid Oid.
        assert_eq!(core::mem::offset_of!(FormData_pg_operator, oprname), 4);
        // oprnamespace follows the NAMEDATALEN-byte oprname (offset 4 + 64 = 68).
        assert_eq!(
            core::mem::offset_of!(FormData_pg_operator, oprnamespace),
            4 + core::mem::size_of::<NameData>()
        );
        // The struct must at least span through its last fixed field.
        assert!(
            core::mem::size_of::<FormData_pg_operator>()
                >= core::mem::offset_of!(FormData_pg_operator, oprjoin)
                    + core::mem::size_of::<regproc>()
        );
    }
}

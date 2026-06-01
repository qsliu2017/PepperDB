//! commands/amcmds.c - routines for SQL commands that manipulate access methods.

use crate::prelude::*;
use crate::{DirectFunctionCall1};

use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog::GetNewOidWithIndex;
use crate::catalog::catalog_oids::{AccessMethodRelationId, ProcedureRelationId};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_am::{Form_pg_am, AMTYPE_INDEX, AMTYPE_TABLE};
use crate::catalog::pg_type_d::{INDEX_AM_HANDLEROID, INTERNALOID, TABLE_AM_HANDLEROID};
use crate::miscadmin::superuser;
use crate::nodes::parsenodes::CreateAmStmt;
use crate::nodes::pg_list::List;
use crate::postgres::{CharGetDatum, CStringGetDatum, ObjectIdGetDatum};
use crate::storage::lockdefs::RowExclusiveLock;
use crate::utils::adt::name::namein;
use crate::utils::builtins::format_type_extended;
use crate::utils::palloc::pstrdup;
use crate::utils::rel::{Relation, RelationGetDescr};
use crate::access::attnum::AttrNumber;
// Name, NameData, NameStr, OidIsValid come via the prelude / crate::c::*.

// ----------------------------------------------------------------------------
// catalog/pg_am_d.h constants (generated header not yet ported).  Attribute
// numbers follow the pg_am column order: oid, amname, amhandler, amtype.
// ----------------------------------------------------------------------------
const Natts_pg_am: usize = 4;
const Anum_pg_am_oid: AttrNumber = 1;
const Anum_pg_am_amname: AttrNumber = 2;
const Anum_pg_am_amhandler: AttrNumber = 3;
const Anum_pg_am_amtype: AttrNumber = 4;

// catalog/indexing.h
const AmOidIndexId: Oid = 2756; // pg_am_oid_index

// utils/syscache.h syscache ids (values not load-bearing for the stubs below)
const AMNAME: c_int = 0;
const AMOID: c_int = 1;

// catalog/dependency.h: DependencyType.DEPENDENCY_NORMAL
const DEPENDENCY_NORMAL: c_char = b'n' as c_char;

// ----------------------------------------------------------------------------
// Local stubs for callees not yet ported.  Each mirrors the C signature.
// TODO: replace with real ports.
// ----------------------------------------------------------------------------

// utils/syscache.h
unsafe fn GetSysCacheOid1(_cache_id: c_int, _oid_col: AttrNumber, _key1: Datum) -> Oid {
    unimplemented!() // TODO
}

unsafe fn SearchSysCache1(_cache_id: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO
}

// access/genam.h / catalog/indexing.h
unsafe fn CatalogTupleInsert(_heap_rel: Relation, _tup: HeapTuple) {
    unimplemented!() // TODO
}

// catalog/dependency.h
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: c_char,
) {
    unimplemented!() // TODO
}

unsafe fn recordDependencyOnCurrentExtension(_object: *const ObjectAddress, _is_update: bool) {
    unimplemented!() // TODO
}

// catalog/objectaccess.h
unsafe fn InvokeObjectPostCreateHook(_class_id: Oid, _object_id: Oid, _sub_id: c_int) {
    // No-op stub: invokes object_access_hook if set.
    // TODO
}

// parser/parse_func.h
unsafe fn LookupFuncName(
    _funcname: *mut List,
    _nargs: c_int,
    _argtypes: *const Oid,
    _missing_ok: bool,
) -> Oid {
    unimplemented!() // TODO
}

// utils/lsyscache.h
unsafe fn get_func_rettype(_funcid: Oid) -> Oid {
    unimplemented!() // TODO
}

unsafe fn get_func_name(_funcid: Oid) -> *mut c_char {
    unimplemented!() // TODO
}

/*
 * CreateAccessMethod
 *		Registers a new access method.
 */
pub unsafe fn CreateAccessMethod(stmt: *mut CreateAmStmt) -> ObjectAddress {
    let rel: Relation;
    let mut myself: ObjectAddress = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    let mut referenced: ObjectAddress = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };
    let mut amoid: Oid;
    let amhandler: Oid;
    let mut nulls: [bool; Natts_pg_am] = [false; Natts_pg_am];
    let mut values: [Datum; Natts_pg_am] = [0; Natts_pg_am];
    let tup: HeapTuple;

    rel = table_open(AccessMethodRelationId, RowExclusiveLock);

    /* Must be superuser */
    if !superuser() {
        ereport!(
            ERROR,
            "permission denied to create access method"
        );
    }

    /* Check if name is used */
    amoid = GetSysCacheOid1(AMNAME, Anum_pg_am_oid, CStringGetDatum((*stmt).amname));
    if OidIsValid(amoid) {
        ereport!(
            ERROR,
            "access method already exists"
        );
    }

    /*
     * Get the handler function oid, verifying the AM type while at it.
     */
    amhandler = lookup_am_handler_func((*stmt).handler_name, (*stmt).amtype);

    /*
     * Insert tuple into pg_am.
     */
    values.iter_mut().for_each(|v| *v = 0);
    nulls.iter_mut().for_each(|n| *n = false);

    amoid = GetNewOidWithIndex(rel, AmOidIndexId, Anum_pg_am_oid);
    values[(Anum_pg_am_oid - 1) as usize] = ObjectIdGetDatum(amoid);
    values[(Anum_pg_am_amname - 1) as usize] =
        DirectFunctionCall1!(namein, CStringGetDatum((*stmt).amname));
    values[(Anum_pg_am_amhandler - 1) as usize] = ObjectIdGetDatum(amhandler);
    values[(Anum_pg_am_amtype - 1) as usize] = CharGetDatum((*stmt).amtype);

    tup = heap_form_tuple(RelationGetDescr(rel), values.as_mut_ptr(), nulls.as_mut_ptr());

    CatalogTupleInsert(rel, tup);
    heap_freetuple(tup);

    myself.classId = AccessMethodRelationId;
    myself.objectId = amoid;
    myself.objectSubId = 0;

    /* Record dependency on handler function */
    referenced.classId = ProcedureRelationId;
    referenced.objectId = amhandler;
    referenced.objectSubId = 0;

    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    recordDependencyOnCurrentExtension(&myself, false);

    InvokeObjectPostCreateHook(AccessMethodRelationId, amoid, 0);

    table_close(rel, RowExclusiveLock);

    myself
}

/*
 * get_am_type_oid
 *		Worker for various get_am_*_oid variants
 *
 * If missing_ok is false, throw an error if access method not found.  If
 * true, just return InvalidOid.
 *
 * If amtype is not '\0', an error is raised if the AM found is not of the
 * given type.
 */
unsafe fn get_am_type_oid(amname: *const c_char, amtype: c_char, missing_ok: bool) -> Oid {
    let tup: HeapTuple;
    let mut oid: Oid = InvalidOid;

    tup = SearchSysCache1(AMNAME, CStringGetDatum(amname));
    if HeapTupleIsValid(tup) {
        let amform: Form_pg_am = GETSTRUCT(tup) as Form_pg_am;

        if amtype != b'\0' as c_char && (*amform).amtype != amtype {
            // C formats NameStr(amform->amname) and get_am_type_string(amtype)
            // into the message; 2-arg ereport! cannot, so the helper is invoked
            // for parity but the message text is fixed.
            let typestr: *const c_char = get_am_type_string(amtype);
            let _ = typestr;
            ereport!(
                ERROR,
                "access method is not of the requested type"
            );
        }

        oid = (*amform).oid;
        ReleaseSysCache(tup);
    }

    if !OidIsValid(oid) && !missing_ok {
        ereport!(
            ERROR,
            "access method does not exist"
        );
    }
    oid
}

/*
 * get_index_am_oid - given an access method name, look up its OID
 *		and verify it corresponds to an index AM.
 */
pub unsafe fn get_index_am_oid(amname: *const c_char, missing_ok: bool) -> Oid {
    get_am_type_oid(amname, AMTYPE_INDEX, missing_ok)
}

/*
 * get_table_am_oid - given an access method name, look up its OID
 *		and verify it corresponds to a table AM.
 */
pub unsafe fn get_table_am_oid(amname: *const c_char, missing_ok: bool) -> Oid {
    get_am_type_oid(amname, AMTYPE_TABLE, missing_ok)
}

/*
 * get_am_oid - given an access method name, look up its OID.
 *		The type is not checked.
 */
pub unsafe fn get_am_oid(amname: *const c_char, missing_ok: bool) -> Oid {
    get_am_type_oid(amname, b'\0' as c_char, missing_ok)
}

/*
 * get_am_name - given an access method OID, look up its name.
 */
pub unsafe fn get_am_name(amOid: Oid) -> *mut c_char {
    let tup: HeapTuple;
    let mut result: *mut c_char = null_mut();

    tup = SearchSysCache1(AMOID, ObjectIdGetDatum(amOid));
    if HeapTupleIsValid(tup) {
        let amform: Form_pg_am = GETSTRUCT(tup) as Form_pg_am;

        result = pstrdup(NameStr(&(*amform).amname));
        ReleaseSysCache(tup);
    }
    result
}

/*
 * Convert single-character access method type into string for error reporting.
 */
unsafe fn get_am_type_string(amtype: c_char) -> *const c_char {
    match amtype {
        x if x == AMTYPE_INDEX => c"INDEX".as_ptr(),
        x if x == AMTYPE_TABLE => c"TABLE".as_ptr(),
        _ => {
            /* shouldn't happen */
            elog!(ERROR, "invalid access method type '{}'", amtype as u8 as char);
            #[allow(unreachable_code)]
            null() /* keep compiler quiet */
        }
    }
}

/*
 * Convert a handler function name to an Oid.  If the return type of the
 * function doesn't match the given AM type, an error is raised.
 *
 * This function either return valid function Oid or throw an error.
 */
unsafe fn lookup_am_handler_func(handler_name: *mut List, amtype: c_char) -> Oid {
    let handlerOid: Oid;
    let funcargtypes: [Oid; 1] = [INTERNALOID];
    let mut expectedType: Oid = InvalidOid;

    if handler_name.is_null() {
        ereport!(
            ERROR,
            "handler function is not specified"
        );
    }

    /* handlers have one argument of type internal */
    handlerOid = LookupFuncName(handler_name, 1, funcargtypes.as_ptr(), false);

    /* check that handler has the correct return type */
    match amtype {
        x if x == AMTYPE_INDEX => {
            expectedType = INDEX_AM_HANDLEROID;
        }
        x if x == AMTYPE_TABLE => {
            expectedType = TABLE_AM_HANDLEROID;
        }
        _ => {
            elog!(ERROR, "unrecognized access method type \"{}\"", amtype as u8 as char);
        }
    }

    if get_func_rettype(handlerOid) != expectedType {
        // The C error message formats get_func_name(handlerOid) and
        // format_type_extended(expectedType, -1, 0); the 2-arg ereport! cannot
        // take runtime args, so the message is fixed but the helpers are still
        // invoked to preserve their side-effect-free intent / keep them linked.
        let funcname: *mut c_char = get_func_name(handlerOid);
        let typname: *mut c_char = format_type_extended(expectedType, -1, 0);
        let _ = funcname;
        let _ = typname;
        ereport!(
            ERROR,
            "function must return the correct handler type"
        );
    }

    handlerOid
}

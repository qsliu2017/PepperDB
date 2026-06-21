//! proclang.c - PostgreSQL LANGUAGE support code (CREATE LANGUAGE).

use crate::prelude::*;

use crate::access::common::heaptuple::{heap_form_tuple, heap_modify_tuple};
use crate::access::htup_details::{HeapTupleIsValid, GETSTRUCT};
use crate::access::table::table::{table_close, table_open};
use crate::catalog::catalog::GetNewOidWithIndex;
use crate::catalog::catalog_oids::{LanguageRelationId, ProcedureRelationId};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_language::Form_pg_language;
use crate::catalog::pg_type_d::{INTERNALOID, LANGUAGE_HANDLEROID, OIDOID};
use crate::miscadmin::{GetUserId, superuser};
use crate::nodes::parsenodes::CreatePLangStmt;
use crate::nodes::pg_list::List;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, ObjectIdGetDatum, PointerGetDatum,
};
use crate::utils::rel::{Relation, RelationGetDescr};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::access::attnum::AttrNumber;
use crate::storage::lockdefs::RowExclusiveLock;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::adt::name::namestrcpy;
// Name, NameData, OidIsValid come from `crate::c::*` via the prelude.

// ----------------------------------------------------------------------------
// catalog/pg_language_d.h constants (generated header not yet ported).
// Attribute numbers follow the pg_language column order.
// ----------------------------------------------------------------------------
const Natts_pg_language: usize = 9;
const Anum_pg_language_oid: usize = 1;
const Anum_pg_language_lanname: usize = 2;
const Anum_pg_language_lanowner: usize = 3;
const Anum_pg_language_lanispl: usize = 4;
const Anum_pg_language_lanpltrusted: usize = 5;
const Anum_pg_language_lanplcallfoid: usize = 6;
const Anum_pg_language_laninline: usize = 7;
const Anum_pg_language_lanvalidator: usize = 8;
const Anum_pg_language_lanacl: usize = 9;

// catalog/indexing.h
const LanguageOidIndexId: Oid = 2681; // pg_language_oid_index

// utils/syscache.h syscache id for LANGNAME (value not load-bearing here)
const LANGNAME: c_int = 35;

// catalog/dependency.h
const DEPENDENCY_NORMAL: c_char = b'n' as c_char;

// ObjectAddresses is an opaque collection type from catalog/dependency.h
type ObjectAddresses = c_void;

/*
 * CREATE LANGUAGE
 */
pub unsafe fn CreateProceduralLanguage(stmt: *mut CreatePLangStmt) -> ObjectAddress {
    let languageName: *const c_char = (*stmt).plname;
    let languageOwner: Oid = GetUserId();
    let handlerOid: Oid;
    let inlineOid: Oid;
    let valOid: Oid;
    let funcrettype: Oid;
    let mut funcargtypes: [Oid; 1] = [0; 1];
    let rel: Relation;
    let tupDesc: TupleDesc;
    let mut values: [Datum; Natts_pg_language] = [0; Natts_pg_language];
    let mut nulls: [bool; Natts_pg_language] = [false; Natts_pg_language];
    let mut replaces: [bool; Natts_pg_language] = [true; Natts_pg_language];
    let mut langname: NameData = core::mem::zeroed();
    let oldtup: HeapTuple;
    let tup: HeapTuple;
    let langoid: Oid;
    let is_update: bool;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();
    let addrs: *mut ObjectAddresses;

    /*
     * Check permission
     */
    if !superuser() {
        ereport!(
            ERROR,
            "must be superuser to create custom procedural language"
        );
    }

    /*
     * Lookup the PL handler function and check that it is of the expected
     * return type
     */
    Assert!(!(*stmt).plhandler.is_null());
    handlerOid = LookupFuncName((*stmt).plhandler, 0, null(), false);
    funcrettype = get_func_rettype(handlerOid);
    if funcrettype != LANGUAGE_HANDLEROID {
        ereport!(
            ERROR,
            "function must return type language_handler"
        );
    }

    /* validate the inline function */
    if !(*stmt).plinline.is_null() {
        funcargtypes[0] = INTERNALOID;
        inlineOid = LookupFuncName((*stmt).plinline, 1, funcargtypes.as_ptr(), false);
        /* return value is ignored, so we don't check the type */
    } else {
        inlineOid = InvalidOid;
    }

    /* validate the validator function */
    if !(*stmt).plvalidator.is_null() {
        funcargtypes[0] = OIDOID;
        valOid = LookupFuncName((*stmt).plvalidator, 1, funcargtypes.as_ptr(), false);
        /* return value is ignored, so we don't check the type */
    } else {
        valOid = InvalidOid;
    }

    /* ok to create it */
    rel = table_open(LanguageRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(rel);

    /* Prepare data to be inserted */
    // memset(values, 0, ...); memset(nulls, false, ...); memset(replaces, true, ...)
    // already done by the initializers above.

    namestrcpy(&mut langname as Name, languageName);
    values[Anum_pg_language_lanname - 1] = NameGetDatum(&langname);
    values[Anum_pg_language_lanowner - 1] = ObjectIdGetDatum(languageOwner);
    values[Anum_pg_language_lanispl - 1] = BoolGetDatum(true);
    values[Anum_pg_language_lanpltrusted - 1] = BoolGetDatum((*stmt).pltrusted);
    values[Anum_pg_language_lanplcallfoid - 1] = ObjectIdGetDatum(handlerOid);
    values[Anum_pg_language_laninline - 1] = ObjectIdGetDatum(inlineOid);
    values[Anum_pg_language_lanvalidator - 1] = ObjectIdGetDatum(valOid);
    nulls[Anum_pg_language_lanacl - 1] = true;

    /* Check for pre-existing definition */
    oldtup = SearchSysCache1(LANGNAME, PointerGetDatum(languageName as *const c_void));

    if HeapTupleIsValid(oldtup) {
        let oldform: Form_pg_language = GETSTRUCT(oldtup) as Form_pg_language;

        /* There is one; okay to replace it? */
        if !(*stmt).replace {
            ereport!(ERROR, "language already exists");
        }

        /* This is currently pointless, since we already checked superuser */
        // #ifdef NOT_USED block omitted (compile-time disabled in C source)

        /*
         * Do not change existing oid, ownership or permissions.  Note
         * dependency-update code below has to agree with this decision.
         */
        replaces[Anum_pg_language_oid - 1] = false;
        replaces[Anum_pg_language_lanowner - 1] = false;
        replaces[Anum_pg_language_lanacl - 1] = false;

        /* Okay, do it... */
        tup = heap_modify_tuple(
            oldtup,
            tupDesc,
            values.as_ptr(),
            nulls.as_ptr(),
            replaces.as_ptr(),
        );
        CatalogTupleUpdate(rel, &mut (*tup).t_self, tup);

        langoid = (*oldform).oid;
        ReleaseSysCache(oldtup);
        is_update = true;
    } else {
        /* Creating a new language */
        langoid = GetNewOidWithIndex(
            rel,
            LanguageOidIndexId,
            Anum_pg_language_oid as AttrNumber,
        );
        values[Anum_pg_language_oid - 1] = ObjectIdGetDatum(langoid);
        tup = heap_form_tuple(tupDesc, values.as_ptr(), nulls.as_ptr());
        CatalogTupleInsert(rel, tup);
        is_update = false;
    }

    /*
     * Create dependencies for the new language.  If we are updating an
     * existing language, first delete any existing pg_depend entries.
     * (However, since we are not changing ownership or permissions, the
     * shared dependencies do *not* need to change, and we leave them alone.)
     */
    myself.classId = LanguageRelationId;
    myself.objectId = langoid;
    myself.objectSubId = 0;

    if is_update {
        deleteDependencyRecordsFor(myself.classId, myself.objectId, true);
    }

    /* dependency on owner of language */
    if !is_update {
        recordDependencyOnOwner(myself.classId, myself.objectId, languageOwner);
    }

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, is_update);

    addrs = new_object_addresses();

    /* dependency on the PL handler function */
    ObjectAddressSet(&mut referenced, ProcedureRelationId, handlerOid);
    add_exact_object_address(&referenced, addrs);

    /* dependency on the inline handler function, if any */
    if OidIsValid(inlineOid) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, inlineOid);
        add_exact_object_address(&referenced, addrs);
    }

    /* dependency on the validator function, if any */
    if OidIsValid(valOid) {
        ObjectAddressSet(&mut referenced, ProcedureRelationId, valOid);
        add_exact_object_address(&referenced, addrs);
    }

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    /* Post creation hook for new procedural language */
    InvokeObjectPostCreateHook(LanguageRelationId, myself.objectId, 0);

    table_close(rel, RowExclusiveLock);

    myself
}

/*
 * get_language_oid - given a language name, look up the OID
 *
 * If missing_ok is false, throw an error if language name not found.  If
 * true, just return InvalidOid.
 */
pub unsafe fn get_language_oid(langname: *const c_char, missing_ok: bool) -> Oid {
    let oid: Oid = GetSysCacheOid1(
        LANGNAME,
        Anum_pg_language_oid as AttrNumber,
        CStringGetDatum(langname),
    );
    if !OidIsValid(oid) && !missing_ok {
        ereport!(ERROR, "language does not exist");
    }
    oid
}

// ----------------------------------------------------------------------------
// Local stubs for not-yet-ported callees.
// ----------------------------------------------------------------------------

// postgres.h: NameGetDatum(const NameData *X) => CStringGetDatum(NameStr(*X))
unsafe fn NameGetDatum(x: *const NameData) -> Datum {
    CStringGetDatum((*x).data.as_ptr())
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

// utils/syscache.h
unsafe fn SearchSysCache1(_cache_id: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO
}

unsafe fn GetSysCacheOid1(_cache_id: c_int, _oidcol: AttrNumber, _key1: Datum) -> Oid {
    unimplemented!() // TODO
}

// catalog/indexing.h
unsafe fn CatalogTupleInsert(_heap_rel: Relation, _tup: HeapTuple) -> Oid {
    unimplemented!() // TODO
}

unsafe fn CatalogTupleUpdate(
    _heap_rel: Relation,
    _otid: *mut ItemPointerData,
    _tup: HeapTuple,
) {
    unimplemented!() // TODO
}

// catalog/dependency.h
unsafe fn deleteDependencyRecordsFor(_class_id: Oid, _object_id: Oid, _skip_extension_deps: bool) -> c_long {
    unimplemented!() // TODO
}

unsafe fn recordDependencyOnOwner(_class_id: Oid, _object_id: Oid, _owner: Oid) {
    unimplemented!() // TODO
}

unsafe fn recordDependencyOnCurrentExtension(_object: *const ObjectAddress, _is_update: bool) {
    unimplemented!() // TODO
}

unsafe fn new_object_addresses() -> *mut ObjectAddresses {
    unimplemented!() // TODO
}

unsafe fn add_exact_object_address(_object: *const ObjectAddress, _addrs: *mut ObjectAddresses) {
    unimplemented!() // TODO
}

unsafe fn record_object_address_dependencies(
    _depender: *const ObjectAddress,
    _referenced: *mut ObjectAddresses,
    _behavior: c_char,
) {
    unimplemented!() // TODO
}

unsafe fn free_object_addresses(_addrs: *mut ObjectAddresses) {
    unimplemented!() // TODO
}

// catalog/dependency.h: ObjectAddressSet(addr, class, object) sets classId/objectId, subId=0
unsafe fn ObjectAddressSet(object: *mut ObjectAddress, class_id: Oid, object_id: Oid) {
    (*object).classId = class_id;
    (*object).objectId = object_id;
    (*object).objectSubId = 0;
}

// catalog/objectaccess.h
unsafe fn InvokeObjectPostCreateHook(_class_id: Oid, _object_id: Oid, _sub_id: c_int) {
    // No-op stub: invokes object_access_hook if set.
    // TODO
}

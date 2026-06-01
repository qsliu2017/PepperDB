//! commands/conversioncmds.c - conversion creation command support code.

use crate::prelude::*;

use crate::catalog::catalog_oids::{NamespaceRelationId, ProcedureRelationId};
use crate::catalog::objectaccess::ObjectAddress;
use crate::catalog::pg_type_d::{BOOLOID, CSTRINGOID, INT4OID, INTERNALOID};
use crate::mb::pg_wchar::{pg_char_to_encoding, PG_SQL_ASCII};
use crate::miscadmin::GetUserId;
use crate::nodes::parsenodes::{
    CreateConversionStmt, ACL_CREATE, ACL_EXECUTE,
};
use crate::nodes::pg_list::List;
use crate::postgres::{
    BoolGetDatum, CStringGetDatum, DatumGetInt32, Int32GetDatum,
};

// ---------------------------------------------------------------------------
// Local stubs for callees not yet ported.  Each mirrors the C signature so the
// real translations can drop in later.  // TODO: replace with real ports.
// ---------------------------------------------------------------------------

// utils/acl.h: AclResult / AclMode constants.
#[allow(non_camel_case_types)]
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;

// nodes/parsenodes.h ObjectType selectors used by aclcheck_error.
#[allow(non_camel_case_types)]
type ObjectType = c_int;
const OBJECT_SCHEMA: ObjectType = 0; // TODO: use real nodes::parsenodes::ObjectType
const OBJECT_FUNCTION: ObjectType = 0;

// utils/acl.h
unsafe fn object_aclcheck(
    _classid: Oid,
    _objectid: Oid,
    _roleid: Oid,
    _mode: c_int,
) -> AclResult {
    unimplemented!() // TODO
}

unsafe fn aclcheck_error(
    _aclerr: AclResult,
    _objtype: ObjectType,
    _objectname: *const c_char,
) {
    unimplemented!() // TODO
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

unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    unimplemented!() // TODO
}

// catalog/namespace.h
unsafe fn QualifiedNameGetCreationNamespace(
    _names: *mut List,
    _objname_p: *mut *mut c_char,
) -> Oid {
    unimplemented!() // TODO
}

// nodes/makefuncs.h
unsafe fn NameListToString(_names: *mut List) -> *mut c_char {
    unimplemented!() // TODO
}

// catalog/pg_conversion.h
unsafe fn ConversionCreate(
    _conname: *const c_char,
    _connamespace: Oid,
    _conowner: Oid,
    _conforencoding: c_int,
    _contoencoding: c_int,
    _conproc: Oid,
    _def: bool,
) -> ObjectAddress {
    unimplemented!() // TODO
}

// fmgr.h: OidFunctionCall6 (non-collation wrapper).
unsafe fn OidFunctionCall6(
    _functionId: Oid,
    _arg1: Datum,
    _arg2: Datum,
    _arg3: Datum,
    _arg4: Datum,
    _arg5: Datum,
    _arg6: Datum,
) -> Datum {
    unimplemented!() // TODO
}

/*
 * CREATE CONVERSION
 */
pub unsafe fn CreateConversionCommand(stmt: *mut CreateConversionStmt) -> ObjectAddress {
    let namespaceId: Oid;
    let mut conversion_name: *mut c_char = null_mut();
    let mut aclresult: AclResult;
    let from_encoding: c_int;
    let to_encoding: c_int;
    let funcoid: Oid;
    let from_encoding_name: *const c_char = (*stmt).for_encoding_name;
    let to_encoding_name: *const c_char = (*stmt).to_encoding_name;
    let func_name: *mut List = (*stmt).func_name;
    static funcargs: [Oid; 6] = [
        INT4OID, INT4OID, CSTRINGOID, INTERNALOID, INT4OID, BOOLOID,
    ];
    let result: [c_char; 1] = [0];
    let funcresult: Datum;

    /* Convert list of names to a name and namespace */
    namespaceId = QualifiedNameGetCreationNamespace(
        (*stmt).conversion_name,
        &mut conversion_name,
    );

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(
        NamespaceRelationId,
        namespaceId,
        GetUserId(),
        ACL_CREATE as c_int,
    );
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(namespaceId));
    }

    /* Check the encoding names */
    from_encoding = pg_char_to_encoding(from_encoding_name);
    if from_encoding < 0 {
        ereport!(
            ERROR,
            "source encoding does not exist"
        );
    }

    to_encoding = pg_char_to_encoding(to_encoding_name);
    if to_encoding < 0 {
        ereport!(
            ERROR,
            "destination encoding does not exist"
        );
    }

    /*
     * We consider conversions to or from SQL_ASCII to be meaningless.  (If
     * you wish to change this, note that pg_do_encoding_conversion() and its
     * sister functions have hard-wired fast paths for any conversion in which
     * the source or target encoding is SQL_ASCII, so that an encoding
     * conversion function declared for such a case will never be used.)
     */
    if from_encoding == PG_SQL_ASCII as c_int || to_encoding == PG_SQL_ASCII as c_int {
        ereport!(
            ERROR,
            "encoding conversion to or from \"SQL_ASCII\" is not supported"
        );
    }

    /*
     * Check the existence of the conversion function. Function name could be
     * a qualified name.
     */
    funcoid = LookupFuncName(
        func_name,
        (core::mem::size_of_val(&funcargs) / core::mem::size_of::<Oid>()) as c_int,
        funcargs.as_ptr(),
        false,
    );

    /* Check it returns int4, else it's probably the wrong function */
    if get_func_rettype(funcoid) != INT4OID {
        ereport!(
            ERROR,
            "encoding conversion function must return type integer"
        );
    }

    /* Check we have EXECUTE rights for the function */
    aclresult = object_aclcheck(
        ProcedureRelationId,
        funcoid,
        GetUserId(),
        ACL_EXECUTE as c_int,
    );
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FUNCTION, NameListToString(func_name));
    }

    /*
     * Check that the conversion function is suitable for the requested source
     * and target encodings. We do that by calling the function with an empty
     * string; the conversion function should throw an error if it can't
     * perform the requested conversion.
     */
    let empty: [c_char; 1] = [0];
    funcresult = OidFunctionCall6(
        funcoid,
        Int32GetDatum(from_encoding),
        Int32GetDatum(to_encoding),
        CStringGetDatum(empty.as_ptr()),
        CStringGetDatum(result.as_ptr()),
        Int32GetDatum(0),
        BoolGetDatum(false),
    );

    /*
     * The function should return 0 for empty input. Might as well check that,
     * too.
     */
    if DatumGetInt32(funcresult) != 0 {
        ereport!(
            ERROR,
            "encoding conversion function returned incorrect result for empty input"
        );
    }

    /*
     * All seem ok, go ahead (possible failure would be a duplicate conversion
     * name)
     */
    ConversionCreate(
        conversion_name,
        namespaceId,
        GetUserId(),
        from_encoding,
        to_encoding,
        funcoid,
        (*stmt).def,
    )
}

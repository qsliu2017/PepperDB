//! src/backend/commands/aggregatecmds.c
//!
//!   Routines for aggregate-manipulation commands
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/commands/aggregatecmds.c
//!
//! DESCRIPTION
//!   The "DefineAggregate" routine takes the parse tree and picks out the
//!   appropriate arguments/flags, passing the results to the
//!   "AggregateCreate" routine (in src/backend/catalog), which does the
//!   actual catalog-munging.  DefineAggregate also verifies the permission of
//!   the user to execute the command.

use crate::prelude::*;
use crate::{foreach, current_cell, lfirst_node};
use crate::parser::parse_node::ParseState;

use std::ffi::{c_char, c_int};

// ---------------------------------------------------------------------------
// Local type aliases / stubs for unported deps
// ---------------------------------------------------------------------------

use crate::nodes::parsenodes::{DefElem, TypeName};
use crate::nodes::pg_list::{List, ListCell};
use crate::catalog::objectaccess::ObjectAddress;
use crate::utils::array::ArrayType;

// utils/acl.h - no canonical port yet
type AclResult = c_int;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const AGGKIND_NORMAL: c_char = b'n' as c_char;
const AGGKIND_ORDERED_SET: c_char = b'o' as c_char;
const AGGKIND_HYPOTHETICAL: c_char = b'h' as c_char;

const AGGMODIFY_READ_ONLY: c_char = b'r' as c_char;
const AGGMODIFY_SHAREABLE: c_char = b's' as c_char;
const AGGMODIFY_READ_WRITE: c_char = b'w' as c_char;

const PROPARALLEL_SAFE: c_char = b's' as c_char;
const PROPARALLEL_RESTRICTED: c_char = b'r' as c_char;
const PROPARALLEL_UNSAFE: c_char = b'u' as c_char;

const TYPTYPE_PSEUDO: c_char = b'p' as c_char;

const NamespaceRelationId: Oid = 2615;
const INTERNALOID: Oid = 2281;

const ACL_CREATE: u32 = 1 << 11;

// Object types (from ObjectType enum)
const OBJECT_SCHEMA: c_int = 0;
const OBJECT_AGGREGATE: c_int = 0;

// ---------------------------------------------------------------------------

/// DefineAggregate
///
/// "oldstyle" signals the old (pre-8.2) style where the aggregate input type
/// is specified by a BASETYPE element in the parameters.  Otherwise,
/// "args" is a pair, whose first element is a list of FunctionParameter structs
/// defining the agg's arguments (both direct and aggregated), and whose second
/// element is an Integer node with the number of direct args, or -1 if this
/// isn't an ordered-set aggregate.
/// "parameters" is a list of DefElem representing the agg's definition clauses.
pub unsafe fn DefineAggregate(
    pstate: *mut ParseState,
    name: *mut List,
    mut args: *mut List,
    oldstyle: bool,
    parameters: *mut List,
    replace: bool,
) -> ObjectAddress {
    let aggName: *mut c_char;
    let aggNamespace: Oid;
    let aclresult: AclResult;
    let mut aggKind: c_char = AGGKIND_NORMAL;
    let mut transfuncName: *mut List = std::ptr::null_mut(); // NIL
    let mut finalfuncName: *mut List = std::ptr::null_mut();
    let mut combinefuncName: *mut List = std::ptr::null_mut();
    let mut serialfuncName: *mut List = std::ptr::null_mut();
    let mut deserialfuncName: *mut List = std::ptr::null_mut();
    let mut mtransfuncName: *mut List = std::ptr::null_mut();
    let mut minvtransfuncName: *mut List = std::ptr::null_mut();
    let mut mfinalfuncName: *mut List = std::ptr::null_mut();
    let mut finalfuncExtraArgs: bool = false;
    let mut mfinalfuncExtraArgs: bool = false;
    let mut finalfuncModify: c_char = 0;
    let mut mfinalfuncModify: c_char = 0;
    let mut sortoperatorName: *mut List = std::ptr::null_mut();
    let mut baseType: *mut TypeName = std::ptr::null_mut();
    let mut transType: *mut TypeName = std::ptr::null_mut();
    let mut mtransType: *mut TypeName = std::ptr::null_mut();
    let mut transSpace: int32 = 0;
    let mut mtransSpace: int32 = 0;
    let mut initval: *mut c_char = std::ptr::null_mut();
    let mut minitval: *mut c_char = std::ptr::null_mut();
    let mut parallel: *mut c_char = std::ptr::null_mut();
    let numArgs: c_int;
    let mut numDirectArgs: c_int = 0;
    let parameterTypes: *mut oidvector;
    let allParameterTypes: *mut ArrayType;
    let parameterModes: *mut ArrayType;
    let parameterNames: *mut ArrayType;
    let parameterDefaults: *mut List;
    let variadicArgType: Oid;
    let transTypeId: Oid;
    let mut mtransTypeId: Oid = InvalidOid;
    let transTypeType: c_char;
    let mut mtransTypeType: c_char = 0;
    let mut proparallel: c_char = PROPARALLEL_UNSAFE;

    /* Convert list of names to a name and namespace */
    let mut aggName_out: *mut c_char = std::ptr::null_mut();
    aggNamespace = QualifiedNameGetCreationNamespace(name, &mut aggName_out);
    aggName = aggName_out;

    /* Check we have creation rights in target namespace */
    aclresult = object_aclcheck(NamespaceRelationId, aggNamespace, GetUserId(), ACL_CREATE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(aggNamespace));
    }

    /* Deconstruct the output of the aggr_args grammar production */
    if !oldstyle {
        assert!(list_length(args) == 2);
        numDirectArgs = intVal(lsecond(args));
        if numDirectArgs >= 0 {
            aggKind = AGGKIND_ORDERED_SET;
        } else {
            numDirectArgs = 0;
        }
        args = linitial_node_List(args);
    }

    /* Examine aggregate's definition clauses */
    foreach!(pl, parameters, {
        let defel: *mut DefElem = lfirst_node!(DefElem, T_DefElem, current_cell!(pl));

        /*
         * sfunc1, stype1, and initcond1 are accepted as obsolete spellings
         * for sfunc, stype, initcond.
         */
        if strcmp_lit((*defel).defname, c"sfunc") == 0 {
            transfuncName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"sfunc1") == 0 {
            transfuncName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"finalfunc") == 0 {
            finalfuncName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"combinefunc") == 0 {
            combinefuncName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"serialfunc") == 0 {
            serialfuncName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"deserialfunc") == 0 {
            deserialfuncName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"msfunc") == 0 {
            mtransfuncName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"minvfunc") == 0 {
            minvtransfuncName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"mfinalfunc") == 0 {
            mfinalfuncName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"finalfunc_extra") == 0 {
            finalfuncExtraArgs = defGetBoolean(defel);
        } else if strcmp_lit((*defel).defname, c"mfinalfunc_extra") == 0 {
            mfinalfuncExtraArgs = defGetBoolean(defel);
        } else if strcmp_lit((*defel).defname, c"finalfunc_modify") == 0 {
            finalfuncModify = extractModify(defel);
        } else if strcmp_lit((*defel).defname, c"mfinalfunc_modify") == 0 {
            mfinalfuncModify = extractModify(defel);
        } else if strcmp_lit((*defel).defname, c"sortop") == 0 {
            sortoperatorName = defGetQualifiedName(defel);
        } else if strcmp_lit((*defel).defname, c"basetype") == 0 {
            baseType = defGetTypeName(defel);
        } else if strcmp_lit((*defel).defname, c"hypothetical") == 0 {
            if defGetBoolean(defel) {
                if aggKind == AGGKIND_NORMAL {
                    ereport!(ERROR, "only ordered-set aggregates can be hypothetical");
                    unreachable!();
                }
                aggKind = AGGKIND_HYPOTHETICAL;
            }
        } else if strcmp_lit((*defel).defname, c"stype") == 0 {
            transType = defGetTypeName(defel);
        } else if strcmp_lit((*defel).defname, c"stype1") == 0 {
            transType = defGetTypeName(defel);
        } else if strcmp_lit((*defel).defname, c"sspace") == 0 {
            transSpace = defGetInt32(defel);
        } else if strcmp_lit((*defel).defname, c"mstype") == 0 {
            mtransType = defGetTypeName(defel);
        } else if strcmp_lit((*defel).defname, c"msspace") == 0 {
            mtransSpace = defGetInt32(defel);
        } else if strcmp_lit((*defel).defname, c"initcond") == 0 {
            initval = defGetString(defel);
        } else if strcmp_lit((*defel).defname, c"initcond1") == 0 {
            initval = defGetString(defel);
        } else if strcmp_lit((*defel).defname, c"minitcond") == 0 {
            minitval = defGetString(defel);
        } else if strcmp_lit((*defel).defname, c"parallel") == 0 {
            parallel = defGetString(defel);
        } else {
            elog!(
                WARNING,
                "aggregate attribute \"{}\" not recognized",
                cstr_to_str((*defel).defname)
            );
        }
    });

    /*
     * make sure we have our required definitions
     */
    if transType.is_null() {
        ereport!(ERROR, "aggregate stype must be specified");
        unreachable!();
    }
    if transfuncName.is_null() {
        ereport!(ERROR, "aggregate sfunc must be specified");
        unreachable!();
    }

    /*
     * if mtransType is given, mtransfuncName and minvtransfuncName must be as
     * well; if not, then none of the moving-aggregate options should have
     * been given.
     */
    if !mtransType.is_null() {
        if mtransfuncName.is_null() {
            ereport!(
                ERROR,
                "aggregate msfunc must be specified when mstype is specified"
            );
            unreachable!();
        }
        if minvtransfuncName.is_null() {
            ereport!(
                ERROR,
                "aggregate minvfunc must be specified when mstype is specified"
            );
            unreachable!();
        }
    } else {
        if !mtransfuncName.is_null() {
            ereport!(ERROR, "aggregate msfunc must not be specified without mstype");
            unreachable!();
        }
        if !minvtransfuncName.is_null() {
            ereport!(
                ERROR,
                "aggregate minvfunc must not be specified without mstype"
            );
            unreachable!();
        }
        if !mfinalfuncName.is_null() {
            ereport!(
                ERROR,
                "aggregate mfinalfunc must not be specified without mstype"
            );
            unreachable!();
        }
        if mtransSpace != 0 {
            ereport!(
                ERROR,
                "aggregate msspace must not be specified without mstype"
            );
            unreachable!();
        }
        if !minitval.is_null() {
            ereport!(
                ERROR,
                "aggregate minitcond must not be specified without mstype"
            );
            unreachable!();
        }
    }

    /*
     * Default values for modify flags can only be determined once we know the
     * aggKind.
     */
    if finalfuncModify == 0 {
        finalfuncModify = if aggKind == AGGKIND_NORMAL {
            AGGMODIFY_READ_ONLY
        } else {
            AGGMODIFY_READ_WRITE
        };
    }
    if mfinalfuncModify == 0 {
        mfinalfuncModify = if aggKind == AGGKIND_NORMAL {
            AGGMODIFY_READ_ONLY
        } else {
            AGGMODIFY_READ_WRITE
        };
    }

    /*
     * look up the aggregate's input datatype(s).
     */
    if oldstyle {
        /*
         * Old style: use basetype parameter.  This supports aggregates of
         * zero or one input, with input type ANY meaning zero inputs.
         *
         * Historically we allowed the command to look like basetype = 'ANY'
         * so we must do a case-insensitive comparison for the name ANY. Ugh.
         */
        let mut aggArgTypes: [Oid; 1] = [InvalidOid];

        if baseType.is_null() {
            ereport!(ERROR, "aggregate input type must be specified");
            unreachable!();
        }

        if pg_strcasecmp(TypeNameToString(baseType), c"ANY".as_ptr()) == 0 {
            numArgs = 0;
            aggArgTypes[0] = InvalidOid;
        } else {
            numArgs = 1;
            aggArgTypes[0] = typenameTypeId(std::ptr::null_mut(), baseType);
        }
        parameterTypes = buildoidvector(aggArgTypes.as_ptr(), numArgs);
        allParameterTypes = std::ptr::null_mut();
        parameterModes = std::ptr::null_mut();
        parameterNames = std::ptr::null_mut();
        parameterDefaults = std::ptr::null_mut(); // NIL
        variadicArgType = InvalidOid;
    } else {
        /*
         * New style: args is a list of FunctionParameters (possibly zero of
         * 'em).  We share functioncmds.c's code for processing them.
         */
        let mut requiredResultType: Oid = InvalidOid;

        if !baseType.is_null() {
            ereport!(
                ERROR,
                "basetype is redundant with aggregate input type specification"
            );
            unreachable!();
        }

        numArgs = list_length(args);
        let mut pt: *mut oidvector = std::ptr::null_mut();
        let mut apt: *mut ArrayType = std::ptr::null_mut();
        let mut pm: *mut ArrayType = std::ptr::null_mut();
        let mut pn: *mut ArrayType = std::ptr::null_mut();
        let mut pd: *mut List = std::ptr::null_mut();
        let mut vat: Oid = InvalidOid;
        interpret_function_parameter_list(
            pstate,
            args,
            InvalidOid,
            OBJECT_AGGREGATE,
            &mut pt,
            std::ptr::null_mut(),
            &mut apt,
            &mut pm,
            &mut pn,
            std::ptr::null_mut(),
            &mut pd,
            &mut vat,
            &mut requiredResultType,
        );
        parameterTypes = pt;
        allParameterTypes = apt;
        parameterModes = pm;
        parameterNames = pn;
        parameterDefaults = pd;
        variadicArgType = vat;
        /* Parameter defaults are not currently allowed by the grammar */
        assert!(parameterDefaults.is_null());
        /* There shouldn't have been any OUT parameters, either */
        assert!(requiredResultType == InvalidOid);
    }

    /*
     * look up the aggregate's transtype.
     *
     * transtype can't be a pseudo-type, since we need to be able to store
     * values of the transtype.  However, we can allow polymorphic transtype
     * in some cases (AggregateCreate will check).  Also, we allow "internal"
     * for functions that want to pass pointers to private data structures;
     * but allow that only to superusers, since you could crash the system (or
     * worse) by connecting up incompatible internal-using functions in an
     * aggregate.
     */
    transTypeId = typenameTypeId(std::ptr::null_mut(), transType);
    transTypeType = get_typtype(transTypeId);
    if transTypeType == TYPTYPE_PSEUDO && !IsPolymorphicType(transTypeId) {
        if transTypeId == INTERNALOID && superuser() {
            /* okay */
        } else {
            elog!(
                ERROR,
                "aggregate transition data type cannot be {}",
                cstr_to_str(format_type_be(transTypeId))
            );
            unreachable!();
        }
    }

    if !serialfuncName.is_null() && !deserialfuncName.is_null() {
        /*
         * Serialization is only needed/allowed for transtype INTERNAL.
         */
        if transTypeId != INTERNALOID {
            elog!(
                ERROR,
                "serialization functions may be specified only when the aggregate transition data type is {}",
                cstr_to_str(format_type_be(INTERNALOID))
            );
            unreachable!();
        }
    } else if !serialfuncName.is_null() || !deserialfuncName.is_null() {
        /*
         * Cannot specify one function without the other.
         */
        ereport!(
            ERROR,
            "must specify both or neither of serialization and deserialization functions"
        );
        unreachable!();
    }

    /*
     * If a moving-aggregate transtype is specified, look that up.  Same
     * restrictions as for transtype.
     */
    if !mtransType.is_null() {
        mtransTypeId = typenameTypeId(std::ptr::null_mut(), mtransType);
        mtransTypeType = get_typtype(mtransTypeId);
        if mtransTypeType == TYPTYPE_PSEUDO && !IsPolymorphicType(mtransTypeId) {
            if mtransTypeId == INTERNALOID && superuser() {
                /* okay */
            } else {
                elog!(
                    ERROR,
                    "aggregate transition data type cannot be {}",
                    cstr_to_str(format_type_be(mtransTypeId))
                );
                unreachable!();
            }
        }
    }

    /*
     * If we have an initval, and it's not for a pseudotype (particularly a
     * polymorphic type), make sure it's acceptable to the type's input
     * function.  We will store the initval as text, because the input
     * function isn't necessarily immutable (consider "now" for timestamp),
     * and we want to use the runtime not creation-time interpretation of the
     * value.  However, if it's an incorrect value it seems much more
     * user-friendly to complain at CREATE AGGREGATE time.
     */
    if !initval.is_null() && transTypeType != TYPTYPE_PSEUDO {
        let mut typinput: Oid = InvalidOid;
        let mut typioparam: Oid = InvalidOid;

        getTypeInputInfo(transTypeId, &mut typinput, &mut typioparam);
        let _ = OidInputFunctionCall(typinput, initval, typioparam, -1);
    }

    /*
     * Likewise for moving-aggregate initval.
     */
    if !minitval.is_null() && mtransTypeType != TYPTYPE_PSEUDO {
        let mut typinput: Oid = InvalidOid;
        let mut typioparam: Oid = InvalidOid;

        getTypeInputInfo(mtransTypeId, &mut typinput, &mut typioparam);
        let _ = OidInputFunctionCall(typinput, minitval, typioparam, -1);
    }

    if !parallel.is_null() {
        if strcmp_lit(parallel, c"safe") == 0 {
            proparallel = PROPARALLEL_SAFE;
        } else if strcmp_lit(parallel, c"restricted") == 0 {
            proparallel = PROPARALLEL_RESTRICTED;
        } else if strcmp_lit(parallel, c"unsafe") == 0 {
            proparallel = PROPARALLEL_UNSAFE;
        } else {
            ereport!(
                ERROR,
                "parameter \"parallel\" must be SAFE, RESTRICTED, or UNSAFE"
            );
            unreachable!();
        }
    }

    /*
     * Most of the argument-checking is done inside of AggregateCreate
     */
    AggregateCreate(
        aggName, /* aggregate name */
        aggNamespace, /* namespace */
        replace,
        aggKind,
        numArgs,
        numDirectArgs,
        parameterTypes,
        PointerGetDatum(allParameterTypes as *const _),
        PointerGetDatum(parameterModes as *const _),
        PointerGetDatum(parameterNames as *const _),
        parameterDefaults,
        variadicArgType,
        transfuncName,    /* step function name */
        finalfuncName,    /* final function name */
        combinefuncName,  /* combine function name */
        serialfuncName,   /* serial function name */
        deserialfuncName, /* deserial function name */
        mtransfuncName,   /* fwd trans function name */
        minvtransfuncName, /* inv trans function name */
        mfinalfuncName,   /* final function name */
        finalfuncExtraArgs,
        mfinalfuncExtraArgs,
        finalfuncModify,
        mfinalfuncModify,
        sortoperatorName, /* sort operator name */
        transTypeId,      /* transition data type */
        transSpace,       /* transition space */
        mtransTypeId,     /* transition data type */
        mtransSpace,      /* transition space */
        initval,          /* initial condition */
        minitval,         /* initial condition */
        proparallel,      /* parallel safe? */
    )
}

/*
 * Convert the string form of [m]finalfunc_modify to the catalog representation
 */
unsafe fn extractModify(defel: *mut DefElem) -> c_char {
    let val: *mut c_char = defGetString(defel);

    if strcmp_lit(val, c"read_only") == 0 {
        return AGGMODIFY_READ_ONLY;
    }
    if strcmp_lit(val, c"shareable") == 0 {
        return AGGMODIFY_SHAREABLE;
    }
    if strcmp_lit(val, c"read_write") == 0 {
        return AGGMODIFY_READ_WRITE;
    }
    elog!(
        ERROR,
        "parameter \"{}\" must be READ_ONLY, SHAREABLE, or READ_WRITE",
        cstr_to_str((*defel).defname)
    );
    0 /* keep compiler quiet */
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

#[inline]
unsafe fn strcmp_lit(a: *const c_char, b: &std::ffi::CStr) -> c_int {
    strcmp(a, b.as_ptr())
}

#[inline]
unsafe fn cstr_to_str(s: *const c_char) -> std::borrow::Cow<'static, str> {
    if s.is_null() {
        std::borrow::Cow::Borrowed("(null)")
    } else {
        std::ffi::CStr::from_ptr(s).to_string_lossy()
    }
}

// ---------------------------------------------------------------------------
// Local stubs for unported helper functions
// ---------------------------------------------------------------------------

const ACLCHECK_OK: AclResult = 0;

unsafe fn QualifiedNameGetCreationNamespace(_names: *mut List, _objname_p: *mut *mut c_char) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}

unsafe fn object_aclcheck(_classid: Oid, _objectid: Oid, _roleid: Oid, _mode: u32) -> AclResult {
    unimplemented!() // TODO: utils/adt/acl.c
}

unsafe fn GetUserId() -> Oid { crate::utils::init::miscinit::GetUserId() }

unsafe fn aclcheck_error(_aclerr: AclResult, _objtype: c_int, _objectname: *const c_char) {
    unimplemented!() // TODO: utils/adt/acl.c
}

unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

unsafe fn intVal(_v: *mut std::ffi::c_void) -> c_int {
    unimplemented!() // TODO: nodes/value.h
}

unsafe fn lsecond(_l: *mut List) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: nodes/pg_list.h
}

unsafe fn linitial_node_List(_l: *mut List) -> *mut List {
    unimplemented!() // TODO: nodes/pg_list.h (linitial_node(List, ...))
}

unsafe fn list_length(_l: *mut List) -> c_int {
    unimplemented!() // TODO: nodes/list.c
}

unsafe fn defGetQualifiedName(_def: *mut DefElem) -> *mut List {
    unimplemented!() // TODO: commands/define.c
}

unsafe fn defGetBoolean(_def: *mut DefElem) -> bool {
    unimplemented!() // TODO: commands/define.c
}

unsafe fn defGetTypeName(_def: *mut DefElem) -> *mut TypeName {
    unimplemented!() // TODO: commands/define.c
}

unsafe fn defGetInt32(_def: *mut DefElem) -> int32 {
    unimplemented!() // TODO: commands/define.c
}

unsafe fn defGetString(_def: *mut DefElem) -> *mut c_char {
    unimplemented!() // TODO: commands/define.c
}

unsafe fn pg_strcasecmp(_s1: *const c_char, _s2: *const c_char) -> c_int {
    unimplemented!() // TODO: port/pgstrcasecmp.c
}

unsafe fn TypeNameToString(_typ: *const TypeName) -> *mut c_char {
    unimplemented!() // TODO: parser/parse_type.c
}

unsafe fn typenameTypeId(_pstate: *mut ParseState, _typeName: *mut TypeName) -> Oid {
    unimplemented!() // TODO: parser/parse_type.c
}

unsafe fn buildoidvector(_oids: *const Oid, _n: c_int) -> *mut oidvector {
    unimplemented!() // TODO: utils/adt/oid.c
}

unsafe fn interpret_function_parameter_list(
    _pstate: *mut ParseState,
    _parameters: *mut List,
    _languageOid: Oid,
    _objtype: c_int,
    _parameterTypes: *mut *mut oidvector,
    _parameterTypes_list: *mut *mut List,
    _allParameterTypes: *mut *mut ArrayType,
    _parameterModes: *mut *mut ArrayType,
    _parameterNames: *mut *mut ArrayType,
    _inParameterNames_list: *mut *mut List,
    _parameterDefaults: *mut *mut List,
    _variadicArgType: *mut Oid,
    _requiredResultType: *mut Oid,
) {
    unimplemented!() // TODO: commands/functioncmds.c
}

unsafe fn get_typtype(_typid: Oid) -> c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

unsafe fn IsPolymorphicType(_typid: Oid) -> bool {
    unimplemented!() // TODO: catalog/pg_type.h
}

unsafe fn superuser() -> bool {
    unimplemented!() // TODO: utils/misc/superuser.c
}

unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/format_type.c
}

unsafe fn getTypeInputInfo(_type: Oid, _typInput: *mut Oid, _typIOParam: *mut Oid) {
    unimplemented!() // TODO: utils/adt/lsyscache.c
}

unsafe fn OidInputFunctionCall(
    _functionId: Oid,
    _str: *mut c_char,
    _typioparam: Oid,
    _typmod: int32,
) -> Datum {
    unimplemented!() // TODO: utils/fmgr/fmgr.c
}

unsafe fn AggregateCreate(
    _aggName: *mut c_char,
    _aggNamespace: Oid,
    _replace: bool,
    _aggKind: c_char,
    _numArgs: c_int,
    _numDirectArgs: c_int,
    _parameterTypes: *mut oidvector,
    _allParameterTypes: Datum,
    _parameterModes: Datum,
    _parameterNames: Datum,
    _parameterDefaults: *mut List,
    _variadicArgType: Oid,
    _aggtransfnName: *mut List,
    _aggfinalfnName: *mut List,
    _aggcombinefnName: *mut List,
    _aggserialfnName: *mut List,
    _aggdeserialfnName: *mut List,
    _aggmtransfnName: *mut List,
    _aggminvtransfnName: *mut List,
    _aggmfinalfnName: *mut List,
    _finalfnExtraArgs: bool,
    _mfinalfnExtraArgs: bool,
    _finalfnModify: c_char,
    _mfinalfnModify: c_char,
    _aggsortopName: *mut List,
    _aggTransType: Oid,
    _aggTransSpace: int32,
    _aggmTransType: Oid,
    _aggmTransSpace: int32,
    _agginitval: *mut c_char,
    _aggminitval: *mut c_char,
    _proparallel: c_char,
) -> ObjectAddress {
    unimplemented!() // TODO: catalog/pg_aggregate.c
}

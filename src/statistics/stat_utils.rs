//! stat_utils.c - PostgreSQL statistics manipulation utilities.

use crate::prelude::*;

use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::catalog::pg_class::{
    Form_pg_class, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
};
use crate::catalog::pg_type_d::TEXTOID;
use crate::c::{NameStr, OidIsValid};
use crate::miscadmin::{GetUserId, MyDatabaseId};
use crate::nodes::parsenodes::{AclMode, ACL_MAINTAIN};
use crate::nodes::primnodes::RangeVar;
use crate::port::pgstrcasecmp::pg_strcasecmp;
use crate::storage::lockdefs::{ShareUpdateExclusiveLock, LOCKMODE};
use crate::utils::array::{ArrayType, ARR_NDIM};
use crate::utils::builtins::{format_type_be, TextDatumGetCString};
use crate::utils::fmgr::FunctionCallInfo;

use crate::postgres::ObjectIdGetDatum;
use crate::{PG_ARGISNULL, PG_GETARG_DATUM};

/*
 * struct StatsArgInfo from statistics/stat_utils.h.
 */
#[repr(C)]
pub struct StatsArgInfo {
    pub argname: *const c_char,
    pub argtype: Oid,
}

/* Syscache id for pg_class by relid (utils/syscache.h not ported). */
const RELOID: c_int = 0;

/* catalog/pg_database.h: pg_database relation OID. */
const DatabaseRelationId: Oid = 1262;

/* utils/acl.h: AclResult constants. */
type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;

/* nodes/parsenodes.h: ObjectType for get_relkind_objtype(). */
type ObjectType = c_int;

/*
 * DatumGetArrayTypeP - utils/array.h.  Detoasts the array; not yet ported, so
 * stubbed as a plain cast.
 */
#[inline]
unsafe fn DatumGetArrayTypeP(d: Datum) -> *mut ArrayType {
    // TODO(pg-port): use real DatumGetArrayTypeP (PG_DETOAST_DATUM) when ported.
    d as *mut ArrayType
}

/* utils/array.h - not ported. */
unsafe fn array_contains_nulls(_array: *mut ArrayType) -> bool {
    // TODO(pg-port): port array_contains_nulls.
    unimplemented!()
}

/* catalog/index.h - not ported. */
unsafe fn IndexGetRelation(_indexId: Oid, _missing_ok: bool) -> Oid {
    // TODO(pg-port): port IndexGetRelation.
    unimplemented!()
}

/* utils/lsyscache.h - not ported. */
unsafe fn get_rel_relkind(_relid: Oid) -> c_char {
    // TODO(pg-port): port get_rel_relkind.
    unimplemented!()
}

/* utils/lsyscache.h - not ported. */
unsafe fn get_relkind_objtype(_relkind: c_char) -> ObjectType {
    // TODO(pg-port): port get_relkind_objtype.
    unimplemented!()
}

/* storage/lmgr.h - not ported. */
unsafe fn LockRelationOid(_relid: Oid, _lockmode: LOCKMODE) {
    // TODO(pg-port): port LockRelationOid.
    unimplemented!()
}

/* storage/lmgr.h - not ported. */
unsafe fn UnlockRelationOid(_relid: Oid, _lockmode: LOCKMODE) {
    // TODO(pg-port): port UnlockRelationOid.
    unimplemented!()
}

/* utils/syscache.h - not ported. */
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    // TODO(pg-port): port SearchSysCache1.
    unimplemented!()
}

/* utils/syscache.h - not ported. */
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    // TODO(pg-port): port ReleaseSysCache.
    unimplemented!()
}

/* utils/acl.h - not ported. */
unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool {
    // TODO(pg-port): port object_ownercheck.
    unimplemented!()
}

/* utils/acl.h - not ported. */
unsafe fn pg_class_aclcheck(_table_oid: Oid, _roleid: Oid, _mode: AclMode) -> AclResult {
    // TODO(pg-port): port pg_class_aclcheck.
    unimplemented!()
}

/* utils/acl.h - not ported. */
unsafe fn aclcheck_error(_aclerr: AclResult, _objtype: ObjectType, _objectname: *const c_char) {
    // TODO(pg-port): port aclcheck_error.
    unimplemented!()
}

/* utils/fmgr.h (funcapi.h) - not ported. */
unsafe fn errdetail_relkind_not_supported(_relkind: c_char) -> c_int {
    // TODO(pg-port): port errdetail_relkind_not_supported.
    unimplemented!()
}

/* utils/fmgr.h - not ported. */
unsafe fn extract_variadic_args(
    _fcinfo: FunctionCallInfo,
    _variadic_start: c_int,
    _convert_unknown: bool,
    _args: *mut *mut Datum,
    _types: *mut *mut Oid,
    _nulls: *mut *mut bool,
) -> c_int {
    // TODO(pg-port): port extract_variadic_args.
    unimplemented!()
}

/*
 * Ensure that a given argument is not null.
 */
pub unsafe fn stats_check_required_arg(
    fcinfo: FunctionCallInfo,
    arginfo: *mut StatsArgInfo,
    argnum: c_int,
) {
    if PG_ARGISNULL!(fcinfo, argnum) {
        elog!(
            ERROR,
            "argument \"{}\" must not be null",
            cstr_to_string((*arginfo.offset(argnum as isize)).argname)
        );
    }
}

/*
 * Check that argument is either NULL or a one dimensional array with no
 * NULLs.
 *
 * If a problem is found, emit a WARNING, and return false. Otherwise return
 * true.
 */
pub unsafe fn stats_check_arg_array(
    fcinfo: FunctionCallInfo,
    arginfo: *mut StatsArgInfo,
    argnum: c_int,
) -> bool {
    let arr: *mut ArrayType;

    if PG_ARGISNULL!(fcinfo, argnum) {
        return true;
    }

    arr = DatumGetArrayTypeP(PG_GETARG_DATUM!(fcinfo, argnum));

    if ARR_NDIM(arr) != 1 {
        elog!(
            WARNING,
            "argument \"{}\" must not be a multidimensional array",
            cstr_to_string((*arginfo.offset(argnum as isize)).argname)
        );
        return false;
    }

    if array_contains_nulls(arr) {
        elog!(
            WARNING,
            "argument \"{}\" array must not contain null values",
            cstr_to_string((*arginfo.offset(argnum as isize)).argname)
        );
        return false;
    }

    true
}

/*
 * Enforce parameter pairs that must be specified together (or not at all) for
 * a particular stakind, such as most_common_vals and most_common_freqs for
 * STATISTIC_KIND_MCV.
 *
 * If a problem is found, emit a WARNING, and return false. Otherwise return
 * true.
 */
pub unsafe fn stats_check_arg_pair(
    fcinfo: FunctionCallInfo,
    arginfo: *mut StatsArgInfo,
    argnum1: c_int,
    argnum2: c_int,
) -> bool {
    if PG_ARGISNULL!(fcinfo, argnum1) && PG_ARGISNULL!(fcinfo, argnum2) {
        return true;
    }

    if PG_ARGISNULL!(fcinfo, argnum1) || PG_ARGISNULL!(fcinfo, argnum2) {
        let nullarg: c_int = if PG_ARGISNULL!(fcinfo, argnum1) {
            argnum1
        } else {
            argnum2
        };
        let otherarg: c_int = if PG_ARGISNULL!(fcinfo, argnum1) {
            argnum2
        } else {
            argnum1
        };

        elog!(
            WARNING,
            "argument \"{}\" must be specified when argument \"{}\" is specified",
            cstr_to_string((*arginfo.offset(nullarg as isize)).argname),
            cstr_to_string((*arginfo.offset(otherarg as isize)).argname)
        );

        return false;
    }

    true
}

/*
 * A role has privileges to set statistics on the relation if any of the
 * following are true:
 *   - the role owns the current database and the relation is not shared
 *   - the role has the MAINTAIN privilege on the relation
 */
pub unsafe fn RangeVarCallbackForStats(
    relation: *const RangeVar,
    relId: Oid,
    oldRelId: Oid,
    arg: *mut c_void,
) {
    let locked_oid: *mut Oid = arg as *mut Oid;
    let mut table_oid: Oid = relId;
    let tuple: HeapTuple;
    let form: Form_pg_class;
    let relkind: c_char;

    /*
     * If we previously locked some other index's heap, and the name we're
     * looking up no longer refers to that relation, release the now-useless
     * lock.
     */
    if relId != oldRelId && OidIsValid(*locked_oid) {
        UnlockRelationOid(*locked_oid, ShareUpdateExclusiveLock);
        *locked_oid = InvalidOid;
    }

    /* If the relation does not exist, there's nothing more to do. */
    if !OidIsValid(relId) {
        return;
    }

    /* If the relation does exist, check whether it's an index. */
    relkind = get_rel_relkind(relId);
    if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
        table_oid = IndexGetRelation(relId, false);
    }

    /*
     * If retrying yields the same OID, there are a couple of extremely
     * unlikely scenarios we need to handle.
     */
    if relId == oldRelId {
        /*
         * If a previous lookup found an index, but the current lookup did
         * not, the index was dropped and the OID was reused for something
         * else between lookups.  In theory, we could simply drop our lock on
         * the index's parent table and proceed, but in the interest of
         * avoiding complexity, we just error.
         */
        if table_oid == relId && OidIsValid(*locked_oid) {
            elog!(
                ERROR,
                "index \"{}\" was concurrently dropped",
                cstr_to_string((*relation).relname)
            );
        }

        /*
         * If the current lookup found an index but a previous lookup either
         * did not find an index or found one with a different parent
         * relation, the relation was dropped and the OID was reused for an
         * index between lookups.  RangeVarGetRelidExtended() will have
         * already locked the index at this point, so we can't just lock the
         * newly discovered parent table OID without risking deadlock.  As
         * above, we just error in this case.
         */
        if table_oid != relId && table_oid != *locked_oid {
            elog!(
                ERROR,
                "index \"{}\" was concurrently created",
                cstr_to_string((*relation).relname)
            );
        }
    }

    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(table_oid));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for OID {}", table_oid);
    }
    form = GETSTRUCT(tuple) as Form_pg_class;

    /* the relkinds that can be used with ANALYZE */
    match (*form).relkind {
        x if x == RELKIND_RELATION
            || x == RELKIND_MATVIEW
            || x == RELKIND_FOREIGN_TABLE
            || x == RELKIND_PARTITIONED_TABLE => {}
        _ => {
            let _ = errdetail_relkind_not_supported((*form).relkind);
            elog!(
                ERROR,
                "cannot modify statistics for relation \"{}\"",
                cstr_to_string(NameStr(&(*form).relname))
            );
        }
    }

    if (*form).relisshared {
        ereport!(ERROR, "cannot modify statistics for shared relation");
    }

    /* Check permissions */
    if !object_ownercheck(DatabaseRelationId, MyDatabaseId, GetUserId()) {
        let aclresult: AclResult =
            pg_class_aclcheck(table_oid, GetUserId(), ACL_MAINTAIN);

        if aclresult != ACLCHECK_OK {
            aclcheck_error(
                aclresult,
                get_relkind_objtype((*form).relkind),
                NameStr(&(*form).relname),
            );
        }
    }

    ReleaseSysCache(tuple);

    /* Lock heap before index to avoid deadlock. */
    if relId != oldRelId && table_oid != relId {
        LockRelationOid(table_oid, ShareUpdateExclusiveLock);
        *locked_oid = table_oid;
    }
}

/*
 * Find the argument number for the given argument name, returning -1 if not
 * found.
 */
unsafe fn get_arg_by_name(argname: *const c_char, arginfo: *mut StatsArgInfo) -> c_int {
    let mut argnum: c_int = 0;

    while !(*arginfo.offset(argnum as isize)).argname.is_null() {
        if pg_strcasecmp(argname, (*arginfo.offset(argnum as isize)).argname) == 0 {
            return argnum;
        }
        argnum += 1;
    }

    elog!(
        WARNING,
        "unrecognized argument name: \"{}\"",
        cstr_to_string(argname)
    );

    -1
}

/*
 * Ensure that a given argument matched the expected type.
 */
unsafe fn stats_check_arg_type(argname: *const c_char, argtype: Oid, expectedtype: Oid) -> bool {
    if argtype != expectedtype {
        elog!(
            WARNING,
            "argument \"{}\" has type {}, expected type {}",
            cstr_to_string(argname),
            cstr_to_string(format_type_be(argtype)),
            cstr_to_string(format_type_be(expectedtype))
        );
        return false;
    }

    true
}

/*
 * Translate variadic argument pairs from 'pairs_fcinfo' into a
 * 'positional_fcinfo' appropriate for calling relation_statistics_update() or
 * attribute_statistics_update() with positional arguments.
 *
 * Caller should have already initialized positional_fcinfo with a size
 * appropriate for calling the intended positional function, and arginfo
 * should also match the intended positional function.
 */
pub unsafe fn stats_fill_fcinfo_from_arg_pairs(
    pairs_fcinfo: FunctionCallInfo,
    positional_fcinfo: FunctionCallInfo,
    arginfo: *mut StatsArgInfo,
) -> bool {
    let mut args: *mut Datum = null_mut();
    let mut argnulls: *mut bool = null_mut();
    let mut types: *mut Oid = null_mut();
    let nargs: c_int;
    let mut result: bool = true;

    /* clear positional args */
    let mut i: isize = 0;
    while !(*arginfo.offset(i)).argname.is_null() {
        (*(*positional_fcinfo).args.as_mut_ptr().offset(i)).value = 0 as Datum;
        (*(*positional_fcinfo).args.as_mut_ptr().offset(i)).isnull = true;
        i += 1;
    }

    nargs = extract_variadic_args(
        pairs_fcinfo,
        0,
        true,
        &mut args,
        &mut types,
        &mut argnulls,
    );

    if nargs % 2 != 0 {
        ereport!(ERROR, "variadic arguments must be name/value pairs");
    }

    /*
     * For each argument name/value pair, find corresponding positional
     * argument for the argument name, and assign the argument value to
     * positional_fcinfo.
     */
    let mut i: c_int = 0;
    while i < nargs {
        let argnum: c_int;
        let argname: *mut c_char;

        if *argnulls.offset(i as isize) {
            elog!(ERROR, "name at variadic position {} is null", i + 1);
        }

        if *types.offset(i as isize) != TEXTOID {
            elog!(
                ERROR,
                "name at variadic position {} has type {}, expected type {}",
                i + 1,
                cstr_to_string(format_type_be(*types.offset(i as isize))),
                cstr_to_string(format_type_be(TEXTOID))
            );
        }

        if *argnulls.offset((i + 1) as isize) {
            i += 2;
            continue;
        }

        argname = TextDatumGetCString(*args.offset(i as isize));

        /*
         * The 'version' argument is a special case, not handled by arginfo
         * because it's not a valid positional argument.
         *
         * For now, 'version' is accepted but ignored. In the future it can be
         * used to interpret older statistics properly.
         */
        if pg_strcasecmp(argname, c"version".as_ptr()) == 0 {
            i += 2;
            continue;
        }

        argnum = get_arg_by_name(argname, arginfo);

        if argnum < 0
            || !stats_check_arg_type(
                argname,
                *types.offset((i + 1) as isize),
                (*arginfo.offset(argnum as isize)).argtype,
            )
        {
            result = false;
            i += 2;
            continue;
        }

        (*(*positional_fcinfo).args.as_mut_ptr().offset(argnum as isize)).value =
            *args.offset((i + 1) as isize);
        (*(*positional_fcinfo).args.as_mut_ptr().offset(argnum as isize)).isnull = false;

        i += 2;
    }

    result
}

/*
 * Helper to render a NUL-terminated C string for runtime-arg error messages.
 */
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::new();
    }
    core::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

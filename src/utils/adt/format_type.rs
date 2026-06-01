//! format_type.c - Display type names "nicely".

use crate::prelude::*;

use crate::{
    PG_ARGISNULL, PG_GETARG_INT32, PG_GETARG_OID, PG_GETARG_POINTER, PG_RETURN_NULL,
    PG_RETURN_TEXT_P, OidFunctionCall1,
};

use crate::postgres::{DatumGetCString, Int32GetDatum, ObjectIdGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::c::{bits16, int32, oidvector, text, VARHDRSZ};

use crate::access::htup_details::{GETSTRUCT, HeapTuple, HeapTupleIsValid};
use crate::catalog::pg_type::{
    Form_pg_type, FormData_pg_type, TYPSTORAGE_PLAIN,
};
use crate::catalog::pg_type_d::{
    BITOID, BOOLOID, BPCHAROID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID, INT8OID, INTERVALOID,
    JSONOID, NUMERICOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID, VARBITOID, VARCHAROID,
};
use crate::utils::builtins::{
    check_valid_oidvector, cstring_to_text, quote_qualified_identifier, FORMAT_TYPE_ALLOW_INVALID,
    FORMAT_TYPE_FORCE_QUALIFY, FORMAT_TYPE_INVALID_AS_NULL, FORMAT_TYPE_TYPEMOD_GIVEN,
};
use crate::pg_config_manual::BITS_PER_BYTE;

use crate::c::{NameStr, NameData};

use crate::utils::fmgr::FunctionCallInfo;

/* ---- Stubs for not-yet-ported callees ---- */

/* utils/fmgroids.h: F_ARRAY_SUBSCRIPT_HANDLER. */
const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 1284;

/* catalog/pg_type.h: IsTrueArrayType(typeForm). */
unsafe fn IsTrueArrayType(typeForm: Form_pg_type) -> bool {
    OidIsValid((*typeForm).typelem) && (*typeForm).typsubscript == F_ARRAY_SUBSCRIPT_HANDLER
}

/* utils/syscache.h: TYPEOID cache id. */
const TYPEOID: c_int = 0;

unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!()
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!()
}

/* catalog/namespace.h */
unsafe fn TypeIsVisible(_typid: Oid) -> bool {
    unimplemented!()
}

/* utils/lsyscache.h */
unsafe fn get_namespace_name_or_temp(_nspid: Oid) -> *mut c_char {
    unimplemented!()
}

/* utils/adt/numeric.c */
unsafe fn numeric_maximum_size(_typemod: int32) -> int32 {
    unimplemented!()
}

/* mb/pg_wchar.h */
unsafe fn pg_encoding_max_length(_encoding: c_int) -> c_int {
    unimplemented!()
}

/* mb/mbutils.c */
unsafe fn GetDatabaseEncoding() -> c_int {
    unimplemented!()
}

/* psprintf("%s(%d)", ...) replacement for printTypmod, default branch. */
unsafe fn psprintf_paren_int(typname: *const c_char, typmod: c_int) -> *mut c_char {
    use core::ffi::CStr;
    let name = CStr::from_ptr(typname).to_string_lossy();
    let s = format!("{}({})\0", name, typmod);
    let bytes = s.into_bytes();
    let p = palloc(bytes.len()) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, p, bytes.len());
    p
}

/* psprintf("%s%s", typname, tmstr) */
unsafe fn psprintf_concat(a: *const c_char, b: *const c_char) -> *mut c_char {
    use core::ffi::CStr;
    let sa = CStr::from_ptr(a).to_bytes();
    let sb = CStr::from_ptr(b).to_bytes();
    let total = sa.len() + sb.len() + 1;
    let p = palloc(total) as *mut c_char;
    core::ptr::copy_nonoverlapping(sa.as_ptr() as *const c_char, p, sa.len());
    core::ptr::copy_nonoverlapping(sb.as_ptr() as *const c_char, p.add(sa.len()), sb.len());
    *p.add(sa.len() + sb.len()) = 0;
    p
}

/* psprintf("%s[]", buf) */
unsafe fn psprintf_array(buf: *const c_char) -> *mut c_char {
    use core::ffi::CStr;
    let s = CStr::from_ptr(buf).to_bytes();
    let total = s.len() + 3;
    let p = palloc(total) as *mut c_char;
    core::ptr::copy_nonoverlapping(s.as_ptr() as *const c_char, p, s.len());
    *p.add(s.len()) = b'[' as c_char;
    *p.add(s.len() + 1) = b']' as c_char;
    *p.add(s.len() + 2) = 0;
    p
}

/* strlen */
unsafe fn c_strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * SQL function: format_type(type_oid, typemod)
 *
 * `type_oid' is from pg_type.oid, `typemod' is from
 * pg_attribute.atttypmod. This function will get the type name and
 * format it and the modifier to canonical SQL format, if the type is
 * a standard type. Otherwise you just get pg_type.typname back,
 * double quoted if it contains funny characters or matches a keyword.
 */
pub unsafe fn format_type(fcinfo: FunctionCallInfo) -> Datum {
    let type_oid: Oid;
    let typemod: int32;
    let result: *mut c_char;
    let mut flags: bits16 = FORMAT_TYPE_ALLOW_INVALID as bits16;

    /* Since this function is not strict, we must test for null args */
    if PG_ARGISNULL!(fcinfo, 0) {
        PG_RETURN_NULL!(fcinfo);
    }

    type_oid = PG_GETARG_OID!(fcinfo, 0);

    if PG_ARGISNULL!(fcinfo, 1) {
        typemod = -1;
    } else {
        typemod = PG_GETARG_INT32!(fcinfo, 1);
        flags |= FORMAT_TYPE_TYPEMOD_GIVEN as bits16;
    }

    result = format_type_extended(type_oid, typemod, flags);

    PG_RETURN_TEXT_P!(cstring_to_text(result));
}

/*
 * format_type_extended
 *		Generate a possibly-qualified type name.
 *
 * Returns a palloc'd string, or NULL.
 */
pub unsafe fn format_type_extended(
    mut type_oid: Oid,
    typemod: int32,
    flags: bits16,
) -> *mut c_char {
    let mut tuple: HeapTuple;
    let mut typeform: Form_pg_type;
    let array_base_type: Oid;
    let is_array: bool;
    let mut buf: *mut c_char;
    let with_typemod: bool;

    if type_oid == InvalidOid {
        if (flags & FORMAT_TYPE_INVALID_AS_NULL as bits16) != 0 {
            return null_mut();
        } else if (flags & FORMAT_TYPE_ALLOW_INVALID as bits16) != 0 {
            return pstrdup(c"-".as_ptr());
        }
    }

    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
    if !HeapTupleIsValid(tuple) {
        if (flags & FORMAT_TYPE_INVALID_AS_NULL as bits16) != 0 {
            return null_mut();
        } else if (flags & FORMAT_TYPE_ALLOW_INVALID as bits16) != 0 {
            return pstrdup(c"???".as_ptr());
        } else {
            elog!(ERROR, "cache lookup failed for type {}", type_oid);
        }
    }
    typeform = GETSTRUCT(tuple) as Form_pg_type;

    /*
     * Check if it's a "true" array type.  Pseudo-array types such as "name"
     * shouldn't get deconstructed.  Also check the toast property, and don't
     * deconstruct "plain storage" array types --- this is because we don't
     * want to show oidvector as oid[].
     */
    array_base_type = (*typeform).typelem;

    if IsTrueArrayType(typeform) && (*typeform).typstorage != TYPSTORAGE_PLAIN {
        /* Switch our attention to the array element type */
        ReleaseSysCache(tuple);
        tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(array_base_type));
        if !HeapTupleIsValid(tuple) {
            if (flags & FORMAT_TYPE_INVALID_AS_NULL as bits16) != 0 {
                return null_mut();
            } else if (flags & FORMAT_TYPE_ALLOW_INVALID as bits16) != 0 {
                return pstrdup(c"???[]".as_ptr());
            } else {
                elog!(ERROR, "cache lookup failed for type {}", type_oid);
            }
        }
        typeform = GETSTRUCT(tuple) as Form_pg_type;
        type_oid = array_base_type;
        is_array = true;
    } else {
        is_array = false;
    }

    with_typemod = (flags & FORMAT_TYPE_TYPEMOD_GIVEN as bits16) != 0 && (typemod >= 0);

    /*
     * See if we want to special-case the output for certain built-in types.
     */
    buf = null_mut(); /* flag for no special case */

    match type_oid {
        BITOID => {
            if with_typemod {
                buf = printTypmod(c"bit".as_ptr(), typemod, (*typeform).typmodout);
            } else if (flags & FORMAT_TYPE_TYPEMOD_GIVEN as bits16) != 0 {
                /*
                 * bit with typmod -1 is not the same as BIT, which means
                 * BIT(1) per SQL spec.  Report it as the quoted typename so
                 * that parser will not assign a bogus typmod.
                 */
            } else {
                buf = pstrdup(c"bit".as_ptr());
            }
        }

        BOOLOID => {
            buf = pstrdup(c"boolean".as_ptr());
        }

        BPCHAROID => {
            if with_typemod {
                buf = printTypmod(c"character".as_ptr(), typemod, (*typeform).typmodout);
            } else if (flags & FORMAT_TYPE_TYPEMOD_GIVEN as bits16) != 0 {
                /*
                 * bpchar with typmod -1 is not the same as CHARACTER, which
                 * means CHARACTER(1) per SQL spec.  Report it as bpchar so
                 * that parser will not assign a bogus typmod.
                 */
            } else {
                buf = pstrdup(c"character".as_ptr());
            }
        }

        FLOAT4OID => {
            buf = pstrdup(c"real".as_ptr());
        }

        FLOAT8OID => {
            buf = pstrdup(c"double precision".as_ptr());
        }

        INT2OID => {
            buf = pstrdup(c"smallint".as_ptr());
        }

        INT4OID => {
            buf = pstrdup(c"integer".as_ptr());
        }

        INT8OID => {
            buf = pstrdup(c"bigint".as_ptr());
        }

        NUMERICOID => {
            if with_typemod {
                buf = printTypmod(c"numeric".as_ptr(), typemod, (*typeform).typmodout);
            } else {
                buf = pstrdup(c"numeric".as_ptr());
            }
        }

        INTERVALOID => {
            if with_typemod {
                buf = printTypmod(c"interval".as_ptr(), typemod, (*typeform).typmodout);
            } else {
                buf = pstrdup(c"interval".as_ptr());
            }
        }

        TIMEOID => {
            if with_typemod {
                buf = printTypmod(c"time".as_ptr(), typemod, (*typeform).typmodout);
            } else {
                buf = pstrdup(c"time without time zone".as_ptr());
            }
        }

        TIMETZOID => {
            if with_typemod {
                buf = printTypmod(c"time".as_ptr(), typemod, (*typeform).typmodout);
            } else {
                buf = pstrdup(c"time with time zone".as_ptr());
            }
        }

        TIMESTAMPOID => {
            if with_typemod {
                buf = printTypmod(c"timestamp".as_ptr(), typemod, (*typeform).typmodout);
            } else {
                buf = pstrdup(c"timestamp without time zone".as_ptr());
            }
        }

        TIMESTAMPTZOID => {
            if with_typemod {
                buf = printTypmod(c"timestamp".as_ptr(), typemod, (*typeform).typmodout);
            } else {
                buf = pstrdup(c"timestamp with time zone".as_ptr());
            }
        }

        VARBITOID => {
            if with_typemod {
                buf = printTypmod(c"bit varying".as_ptr(), typemod, (*typeform).typmodout);
            } else {
                buf = pstrdup(c"bit varying".as_ptr());
            }
        }

        VARCHAROID => {
            if with_typemod {
                buf = printTypmod(c"character varying".as_ptr(), typemod, (*typeform).typmodout);
            } else {
                buf = pstrdup(c"character varying".as_ptr());
            }
        }

        JSONOID => {
            buf = pstrdup(c"json".as_ptr());
        }

        _ => {}
    }

    if buf.is_null() {
        /*
         * Default handling: report the name as it appears in the catalog.
         * Here, we must qualify the name if it is not visible in the search
         * path or if caller requests it; and we must double-quote it if it's
         * not a standard identifier or if it matches any keyword.
         */
        let nspname: *mut c_char;
        let typname: *mut c_char;

        if (flags & FORMAT_TYPE_FORCE_QUALIFY as bits16) == 0 && TypeIsVisible(type_oid) {
            nspname = null_mut();
        } else {
            nspname = get_namespace_name_or_temp((*typeform).typnamespace);
        }

        typname = NameStr(&(*typeform).typname) as *mut c_char;

        buf = quote_qualified_identifier(nspname, typname);

        if with_typemod {
            buf = printTypmod(buf, typemod, (*typeform).typmodout);
        }
    }

    if is_array {
        buf = psprintf_array(buf);
    }

    ReleaseSysCache(tuple);

    buf
}

/*
 * This version is for use within the backend in error messages, etc.
 * One difference is that it will fail for an invalid type.
 *
 * The result is always a palloc'd string.
 */
pub unsafe fn format_type_be(type_oid: Oid) -> *mut c_char {
    format_type_extended(type_oid, -1, 0)
}

/*
 * This version returns a name that is always qualified (unless it's one
 * of the SQL-keyword type names, such as TIMESTAMP WITH TIME ZONE).
 */
pub unsafe fn format_type_be_qualified(type_oid: Oid) -> *mut c_char {
    format_type_extended(type_oid, -1, FORMAT_TYPE_FORCE_QUALIFY as bits16)
}

/*
 * This version allows a nondefault typemod to be specified.
 */
pub unsafe fn format_type_with_typemod(type_oid: Oid, typemod: int32) -> *mut c_char {
    format_type_extended(type_oid, typemod, FORMAT_TYPE_TYPEMOD_GIVEN as bits16)
}

/*
 * Add typmod decoration to the basic type name
 */
unsafe fn printTypmod(typname: *const c_char, typmod: int32, typmodout: Oid) -> *mut c_char {
    let res: *mut c_char;

    /* Shouldn't be called if typmod is -1 */
    Assert!(typmod >= 0);

    if typmodout == InvalidOid {
        /* Default behavior: just print the integer typmod with parens */
        res = psprintf_paren_int(typname, typmod as c_int);
    } else {
        /* Use the type-specific typmodout procedure */
        let tmstr: *mut c_char;

        tmstr = DatumGetCString(OidFunctionCall1!(typmodout, Int32GetDatum(typmod)));
        res = psprintf_concat(typname, tmstr);
    }

    res
}

/*
 * type_maximum_size --- determine maximum width of a variable-width column
 *
 * If the max width is indeterminate, return -1.
 */
pub unsafe fn type_maximum_size(type_oid: Oid, typemod: int32) -> int32 {
    if typemod < 0 {
        return -1;
    }

    match type_oid {
        BPCHAROID | VARCHAROID => {
            /* typemod includes varlena header */
            /* typemod is in characters not bytes */
            (typemod - VARHDRSZ) * pg_encoding_max_length(GetDatabaseEncoding()) + VARHDRSZ
        }

        NUMERICOID => numeric_maximum_size(typemod),

        VARBITOID | BITOID => {
            /* typemod is the (max) number of bits */
            (typemod + (BITS_PER_BYTE as int32 - 1)) / BITS_PER_BYTE as int32
                + 2 * core::mem::size_of::<int32>() as int32
        }

        /* Unknown type, or unlimited-width type such as 'text' */
        _ => -1,
    }
}

/*
 * oidvectortypes			- converts a vector of type OIDs to "typname" list
 */
pub unsafe fn oidvectortypes(fcinfo: FunctionCallInfo) -> Datum {
    let oidArray: *mut oidvector = PG_GETARG_POINTER!(fcinfo, 0) as *mut oidvector;
    let mut result: *mut c_char;
    let numargs: c_int;
    let mut num: c_int;
    let mut total: usize;
    let mut left: usize;

    /* validate input before fetching dim1 */
    check_valid_oidvector(oidArray);
    numargs = (*oidArray).dim1;

    total = 20 * numargs as usize + 1;
    result = palloc(total) as *mut c_char;
    *result.add(0) = 0; /* result[0] = '\0' */
    left = total - 1;

    num = 0;
    while num < numargs {
        let typename: *mut c_char = format_type_extended(
            *(*oidArray).values.as_ptr().add(num as usize),
            -1,
            FORMAT_TYPE_ALLOW_INVALID as bits16,
        );
        let slen: usize = c_strlen(typename);

        if left < (slen + 2) {
            total += slen + 2;
            result = repalloc(result as *mut c_void, total) as *mut c_char;
            left += slen + 2;
        }

        if num > 0 {
            strcat(result, c", ".as_ptr());
            left -= 2;
        }
        strcat(result, typename);
        left -= slen;

        num += 1;
    }

    PG_RETURN_TEXT_P!(cstring_to_text(result));
}

/* C strcat replacement */
unsafe fn strcat(dest: *mut c_char, src: *const c_char) -> *mut c_char {
    let mut d = dest;
    while *d != 0 {
        d = d.add(1);
    }
    let mut s = src;
    while *s != 0 {
        *d = *s;
        d = d.add(1);
        s = s.add(1);
    }
    *d = 0;
    dest
}

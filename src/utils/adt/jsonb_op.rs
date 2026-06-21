//! postgres/src/backend/utils/adt/jsonb_op.c - special operators for jsonb only, used by various index access methods.
//!
//! The jsonb type itself (Jsonb, JsonbValue, JsonbIterator, ...) and its
//! helper routines (findJsonbValueFromContainer, compareJsonbContainers,
//! JsonbDeepContains, JsonbIteratorInit/Next, JsonbHashScalarValue[Extended])
//! live in utils/adt/jsonb_util.c, which is NOT yet ported.  Those types and
//! functions are declared here as local stubs so this file translates 1:1;
//! deconstruct_array_builtin (utils/adt/arrayfuncs.c) is likewise stubbed.

use crate::prelude::*;
use crate::{
    PG_FREE_IF_COPY, PG_GETARG_INT64, PG_GETARG_POINTER, PG_RETURN_BOOL, PG_RETURN_INT32,
    PG_RETURN_UINT64,
};
use crate::utils::fmgr::FunctionCallInfo;
use crate::varatt::{VARDATA_ANY, VARSIZE_ANY_EXHDR};
use crate::catalog::pg_type_d::TEXTOID;

// ---------------------------------------------------------------------------
// Stubbed surface from utils/jsonb.h / jsonb_util.c (NOT yet ported)
// ---------------------------------------------------------------------------

/* flag bits (jsonb.h) */
const JB_FOBJECT: uint32 = 0x20000000;
const JB_FARRAY: uint32 = 0x40000000;

/* JsonbValue type tags (jsonb.h); only jbvString is referenced here */
const jbvString: c_int = 1;

/* JsonbIteratorToken (jsonb.h) */
type JsonbIteratorToken = c_int;
const WJB_DONE: JsonbIteratorToken = 0;
const WJB_KEY: JsonbIteratorToken = 1;
const WJB_VALUE: JsonbIteratorToken = 2;
const WJB_ELEM: JsonbIteratorToken = 3;
const WJB_BEGIN_ARRAY: JsonbIteratorToken = 4;
const WJB_END_ARRAY: JsonbIteratorToken = 5;
const WJB_BEGIN_OBJECT: JsonbIteratorToken = 6;
const WJB_END_OBJECT: JsonbIteratorToken = 7;

#[repr(C)]
struct JsonbContainer {
    _opaque: [u8; 0],
}

#[repr(C)]
struct Jsonb {
    root: JsonbContainer,
}

#[repr(C)]
struct JsonbValueStringVal {
    val: *mut c_char,
    len: c_int,
}

#[repr(C)]
struct JsonbValue {
    type_: c_int,
    val: JsonbValueString,
}

#[repr(C)]
union JsonbValueString {
    string: core::mem::ManuallyDrop<JsonbValueStringVal>,
}

#[repr(C)]
struct JsonbIterator {
    _opaque: [u8; 0],
}

#[repr(C)]
struct ArrayType {
    _opaque: [u8; 0],
}

unsafe fn JB_ROOT_IS_OBJECT(jbp: *mut Jsonb) -> bool {
    (*(VARDATA(jbp as *mut c_void) as *mut uint32) & JB_FOBJECT) != 0
}

unsafe fn JB_ROOT_COUNT(jbp: *mut Jsonb) -> uint32 {
    const JB_CMASK: uint32 = 0x0FFFFFFF;
    *(VARDATA(jbp as *mut c_void) as *mut uint32) & JB_CMASK
}

unsafe fn VARDATA(_ptr: *mut c_void) -> *mut c_char { crate::varatt::VARDATA(_ptr as _) as _ }

unsafe fn findJsonbValueFromContainer(
    _container: *mut JsonbContainer,
    _flags: uint32,
    _key: *mut JsonbValue,
) -> *mut JsonbValue { crate::utils::adt::jsonb_util::findJsonbValueFromContainer(_container as _, _flags as _, _key as _) as _ }

unsafe fn compareJsonbContainers(_a: *mut JsonbContainer, _b: *mut JsonbContainer) -> c_int { crate::utils::adt::jsonb_util::compareJsonbContainers(_a as _, _b as _) as _ }

unsafe fn JsonbDeepContains(_it1: *mut *mut JsonbIterator, _it2: *mut *mut JsonbIterator) -> bool {
    unimplemented!("jsonb_op: JsonbDeepContains not yet translated")
}

unsafe fn JsonbIteratorInit(_container: *mut JsonbContainer) -> *mut JsonbIterator { crate::utils::adt::jsonb_util::JsonbIteratorInit(_container as _) as _ }

unsafe fn JsonbIteratorNext(
    _it: *mut *mut JsonbIterator,
    _val: *mut JsonbValue,
    _skip_nested: bool,
) -> JsonbIteratorToken { crate::utils::adt::jsonb_util::JsonbIteratorNext(_it as _, _val as _, _skip_nested) as _ }

unsafe fn JsonbHashScalarValue(_scalar_val: *const JsonbValue, _hash: *mut uint32) { crate::utils::adt::jsonb_util::JsonbHashScalarValue(_scalar_val as _, _hash as _) }

unsafe fn JsonbHashScalarValueExtended(
    _scalar_val: *const JsonbValue,
    _hash: *mut uint64,
    _seed: uint64,
) { crate::utils::adt::jsonb_util::JsonbHashScalarValueExtended(_scalar_val as _, _hash as _, _seed as _) }

unsafe fn deconstruct_array_builtin(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) {
    unimplemented!("jsonb_op: deconstruct_array_builtin not yet translated")
}

/* PG_GETARG_JSONB_P / PG_GETARG_ARRAYTYPE_P expand through PG_DETOAST_DATUM; the
 * detoast machinery is not wired for jsonb yet, so detoast to the raw pointer. */
unsafe fn PG_GETARG_JSONB_P(fcinfo: FunctionCallInfo, n: c_int) -> *mut Jsonb {
    PG_GETARG_POINTER!(fcinfo, n) as *mut Jsonb
}

unsafe fn PG_GETARG_ARRAYTYPE_P(fcinfo: FunctionCallInfo, n: c_int) -> *mut ArrayType {
    PG_GETARG_POINTER!(fcinfo, n) as *mut ArrayType
}

// ---------------------------------------------------------------------------

#[no_mangle]
pub unsafe fn jsonb_exists(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let key: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let mut kval: JsonbValue = core::mem::zeroed();
    let v: *mut JsonbValue;

    /*
     * We only match Object keys (which are naturally always Strings), or
     * string elements in arrays.  In particular, we do not match non-string
     * scalar elements.  Existence of a key/element is only considered at the
     * top level.  No recursion occurs.
     */
    kval.type_ = jbvString;
    kval.val.string.val = VARDATA_ANY(key as *const c_char);
    kval.val.string.len = VARSIZE_ANY_EXHDR(key as *const c_char) as c_int;

    v = findJsonbValueFromContainer(&mut (*jb).root, JB_FOBJECT | JB_FARRAY, &mut kval);

    PG_RETURN_BOOL!(!v.is_null());
}

#[no_mangle]
pub unsafe fn jsonb_exists_any(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let keys: *mut ArrayType = PG_GETARG_ARRAYTYPE_P(fcinfo, 1);
    let i: c_int;
    let mut key_datums: *mut Datum = null_mut();
    let mut key_nulls: *mut bool = null_mut();
    let mut elem_count: c_int = 0;

    deconstruct_array_builtin(
        keys,
        TEXTOID,
        &mut key_datums,
        &mut key_nulls,
        &mut elem_count,
    );

    let mut i: c_int = 0;
    while i < elem_count {
        let mut strVal: JsonbValue = core::mem::zeroed();

        if *key_nulls.offset(i as isize) {
            i += 1;
            continue;
        }

        strVal.type_ = jbvString;
        /* We rely on the array elements not being toasted */
        strVal.val.string.val = VARDATA_ANY(*key_datums.offset(i as isize) as *const c_char);
        strVal.val.string.len =
            VARSIZE_ANY_EXHDR(*key_datums.offset(i as isize) as *const c_char) as c_int;

        if !findJsonbValueFromContainer(&mut (*jb).root, JB_FOBJECT | JB_FARRAY, &mut strVal)
            .is_null()
        {
            PG_RETURN_BOOL!(true);
        }
        i += 1;
    }
    let _ = i;

    PG_RETURN_BOOL!(false);
}

#[no_mangle]
pub unsafe fn jsonb_exists_all(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let keys: *mut ArrayType = PG_GETARG_ARRAYTYPE_P(fcinfo, 1);
    let mut key_datums: *mut Datum = null_mut();
    let mut key_nulls: *mut bool = null_mut();
    let mut elem_count: c_int = 0;

    deconstruct_array_builtin(
        keys,
        TEXTOID,
        &mut key_datums,
        &mut key_nulls,
        &mut elem_count,
    );

    let mut i: c_int = 0;
    while i < elem_count {
        let mut strVal: JsonbValue = core::mem::zeroed();

        if *key_nulls.offset(i as isize) {
            i += 1;
            continue;
        }

        strVal.type_ = jbvString;
        /* We rely on the array elements not being toasted */
        strVal.val.string.val = VARDATA_ANY(*key_datums.offset(i as isize) as *const c_char);
        strVal.val.string.len =
            VARSIZE_ANY_EXHDR(*key_datums.offset(i as isize) as *const c_char) as c_int;

        if findJsonbValueFromContainer(&mut (*jb).root, JB_FOBJECT | JB_FARRAY, &mut strVal)
            .is_null()
        {
            PG_RETURN_BOOL!(false);
        }
        i += 1;
    }

    PG_RETURN_BOOL!(true);
}

#[no_mangle]
pub unsafe fn jsonb_contains(fcinfo: FunctionCallInfo) -> Datum {
    let val: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let tmpl: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);

    let mut it1: *mut JsonbIterator;
    let mut it2: *mut JsonbIterator;

    if JB_ROOT_IS_OBJECT(val) != JB_ROOT_IS_OBJECT(tmpl) {
        PG_RETURN_BOOL!(false);
    }

    it1 = JsonbIteratorInit(&mut (*val).root);
    it2 = JsonbIteratorInit(&mut (*tmpl).root);

    PG_RETURN_BOOL!(JsonbDeepContains(&mut it1, &mut it2));
}

#[no_mangle]
pub unsafe fn jsonb_contained(fcinfo: FunctionCallInfo) -> Datum {
    /* Commutator of "contains" */
    let tmpl: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let val: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);

    let mut it1: *mut JsonbIterator;
    let mut it2: *mut JsonbIterator;

    if JB_ROOT_IS_OBJECT(val) != JB_ROOT_IS_OBJECT(tmpl) {
        PG_RETURN_BOOL!(false);
    }

    it1 = JsonbIteratorInit(&mut (*val).root);
    it2 = JsonbIteratorInit(&mut (*tmpl).root);

    PG_RETURN_BOOL!(JsonbDeepContains(&mut it1, &mut it2));
}

#[no_mangle]
pub unsafe fn jsonb_ne(fcinfo: FunctionCallInfo) -> Datum {
    let jba: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let jbb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);
    let res: bool;

    res = compareJsonbContainers(&mut (*jba).root, &mut (*jbb).root) != 0;

    PG_FREE_IF_COPY!(fcinfo, jba, 0);
    PG_FREE_IF_COPY!(fcinfo, jbb, 1);
    PG_RETURN_BOOL!(res);
}

/*
 * B-Tree operator class operators, support function
 */
#[no_mangle]
pub unsafe fn jsonb_lt(fcinfo: FunctionCallInfo) -> Datum {
    let jba: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let jbb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);
    let res: bool;

    res = compareJsonbContainers(&mut (*jba).root, &mut (*jbb).root) < 0;

    PG_FREE_IF_COPY!(fcinfo, jba, 0);
    PG_FREE_IF_COPY!(fcinfo, jbb, 1);
    PG_RETURN_BOOL!(res);
}

#[no_mangle]
pub unsafe fn jsonb_gt(fcinfo: FunctionCallInfo) -> Datum {
    let jba: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let jbb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);
    let res: bool;

    res = compareJsonbContainers(&mut (*jba).root, &mut (*jbb).root) > 0;

    PG_FREE_IF_COPY!(fcinfo, jba, 0);
    PG_FREE_IF_COPY!(fcinfo, jbb, 1);
    PG_RETURN_BOOL!(res);
}

#[no_mangle]
pub unsafe fn jsonb_le(fcinfo: FunctionCallInfo) -> Datum {
    let jba: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let jbb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);
    let res: bool;

    res = compareJsonbContainers(&mut (*jba).root, &mut (*jbb).root) <= 0;

    PG_FREE_IF_COPY!(fcinfo, jba, 0);
    PG_FREE_IF_COPY!(fcinfo, jbb, 1);
    PG_RETURN_BOOL!(res);
}

#[no_mangle]
pub unsafe fn jsonb_ge(fcinfo: FunctionCallInfo) -> Datum {
    let jba: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let jbb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);
    let res: bool;

    res = compareJsonbContainers(&mut (*jba).root, &mut (*jbb).root) >= 0;

    PG_FREE_IF_COPY!(fcinfo, jba, 0);
    PG_FREE_IF_COPY!(fcinfo, jbb, 1);
    PG_RETURN_BOOL!(res);
}

#[no_mangle]
pub unsafe fn jsonb_eq(fcinfo: FunctionCallInfo) -> Datum {
    let jba: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let jbb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);
    let res: bool;

    res = compareJsonbContainers(&mut (*jba).root, &mut (*jbb).root) == 0;

    PG_FREE_IF_COPY!(fcinfo, jba, 0);
    PG_FREE_IF_COPY!(fcinfo, jbb, 1);
    PG_RETURN_BOOL!(res);
}

#[no_mangle]
pub unsafe fn jsonb_cmp(fcinfo: FunctionCallInfo) -> Datum {
    let jba: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let jbb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 1);
    let res: c_int;

    res = compareJsonbContainers(&mut (*jba).root, &mut (*jbb).root);

    PG_FREE_IF_COPY!(fcinfo, jba, 0);
    PG_FREE_IF_COPY!(fcinfo, jbb, 1);
    PG_RETURN_INT32!(res);
}

/*
 * Hash operator class jsonb hashing function
 */
#[no_mangle]
pub unsafe fn jsonb_hash(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut r: JsonbIteratorToken;
    let mut hash: uint32 = 0;

    if JB_ROOT_COUNT(jb) == 0 {
        PG_RETURN_INT32!(0);
    }

    it = JsonbIteratorInit(&mut (*jb).root);

    loop {
        r = JsonbIteratorNext(&mut it, &mut v, false);
        if r == WJB_DONE {
            break;
        }
        match r {
            /* Rotation is left to JsonbHashScalarValue() */
            WJB_BEGIN_ARRAY => {
                hash ^= JB_FARRAY;
            }
            WJB_BEGIN_OBJECT => {
                hash ^= JB_FOBJECT;
            }
            WJB_KEY | WJB_VALUE | WJB_ELEM => {
                JsonbHashScalarValue(&v, &mut hash);
            }
            WJB_END_ARRAY | WJB_END_OBJECT => {}
            _ => {
                elog!(ERROR, "invalid JsonbIteratorNext rc: {}", r as c_int);
            }
        }
    }

    PG_FREE_IF_COPY!(fcinfo, jb, 0);
    PG_RETURN_INT32!(hash as int32);
}

#[no_mangle]
pub unsafe fn jsonb_hash_extended(fcinfo: FunctionCallInfo) -> Datum {
    let jb: *mut Jsonb = PG_GETARG_JSONB_P(fcinfo, 0);
    let seed: uint64 = PG_GETARG_INT64!(fcinfo, 1) as uint64;
    let mut it: *mut JsonbIterator;
    let mut v: JsonbValue = core::mem::zeroed();
    let mut r: JsonbIteratorToken;
    let mut hash: uint64 = 0;

    if JB_ROOT_COUNT(jb) == 0 {
        PG_RETURN_UINT64!(seed);
    }

    it = JsonbIteratorInit(&mut (*jb).root);

    loop {
        r = JsonbIteratorNext(&mut it, &mut v, false);
        if r == WJB_DONE {
            break;
        }
        match r {
            /* Rotation is left to JsonbHashScalarValueExtended() */
            WJB_BEGIN_ARRAY => {
                hash ^= ((JB_FARRAY as uint64) << 32) | JB_FARRAY as uint64;
            }
            WJB_BEGIN_OBJECT => {
                hash ^= ((JB_FOBJECT as uint64) << 32) | JB_FOBJECT as uint64;
            }
            WJB_KEY | WJB_VALUE | WJB_ELEM => {
                JsonbHashScalarValueExtended(&v, &mut hash, seed);
            }
            WJB_END_ARRAY | WJB_END_OBJECT => {}
            _ => {
                elog!(ERROR, "invalid JsonbIteratorNext rc: {}", r as c_int);
            }
        }
    }

    PG_FREE_IF_COPY!(fcinfo, jb, 0);
    PG_RETURN_UINT64!(hash);
}

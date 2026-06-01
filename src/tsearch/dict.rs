//! tsearch/dict.c - Standard interface to dictionary.

use crate::prelude::*;

use crate::postgres::{DatumGetPointer, Int32GetDatum, PointerGetDatum};
use crate::postgres_ext::InvalidOid;
use crate::tsearch::ts_public::{DictSubState, TSLexeme};
use crate::utils::array::ArrayType;
use crate::utils::builtins::CStringGetTextDatum;
use crate::utils::fmgr::{FmgrInfo, FunctionCallInfo};
use crate::varatt::{VARDATA_ANY, VARSIZE_ANY_EXHDR};

use crate::{PG_GETARG_OID, PG_GETARG_TEXT_PP, PG_RETURN_NULL, PG_RETURN_POINTER};

use std::ffi::{c_int, c_void};
use std::ptr::null_mut;

// catalog/pg_type.h
use crate::catalog::pg_type_d::TEXTOID;

/*
 * tsearch/ts_cache.h - cache entry for a text search dictionary.  Only the
 * fields used by ts_lexize are modeled here.
 */
// TODO: tsearch/ts_cache.h not yet ported.
#[repr(C)]
pub struct TSDictionaryCacheEntry {
    pub dictId: Oid,
    pub lexize: FmgrInfo,
    pub dictData: *mut c_void,
}

// TODO: tsearch/ts_cache.c lookup_ts_dictionary_cache not yet ported.
unsafe fn lookup_ts_dictionary_cache(dictId: Oid) -> *mut TSDictionaryCacheEntry {
    let _ = dictId;
    unimplemented!()
}

// TODO: utils/fmgr.c FunctionCall4 (collation-less wrapper) not yet ported;
// it is FunctionCall4Coll with InvalidOid collation.
unsafe fn FunctionCall4(
    flinfo: *mut FmgrInfo,
    arg1: Datum,
    arg2: Datum,
    arg3: Datum,
    arg4: Datum,
) -> Datum {
    crate::utils::fmgr::FunctionCall4Coll(flinfo, InvalidOid, arg1, arg2, arg3, arg4)
}

// TODO: utils/array.c construct_array_builtin not yet ported.
unsafe fn construct_array_builtin(
    elems: *mut Datum,
    nelems: c_int,
    elmtype: Oid,
) -> *mut ArrayType {
    let _ = (elems, nelems, elmtype);
    unimplemented!()
}

/*
 * Lexize one word by dictionary, mostly debug function
 */
#[allow(non_snake_case)]
pub unsafe fn ts_lexize(fcinfo: FunctionCallInfo) -> Datum {
    let dictId: Oid = PG_GETARG_OID!(fcinfo, 0);
    let in_: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let a: *mut ArrayType;
    let dict: *mut TSDictionaryCacheEntry;
    let mut res: *mut TSLexeme;
    let mut ptr: *mut TSLexeme;
    let da: *mut Datum;
    let mut dstate = DictSubState {
        isend: false,
        getnext: false,
        private_state: null_mut(),
    };

    dict = lookup_ts_dictionary_cache(dictId);

    res = DatumGetPointer(FunctionCall4(
        &mut (*dict).lexize,
        PointerGetDatum((*dict).dictData),
        PointerGetDatum(VARDATA_ANY(in_ as *const c_char) as *const c_void),
        Int32GetDatum(VARSIZE_ANY_EXHDR(in_ as *const c_char) as int32),
        PointerGetDatum(&mut dstate as *mut DictSubState as *const c_void),
    )) as *mut TSLexeme;

    if dstate.getnext {
        dstate.isend = true;
        ptr = DatumGetPointer(FunctionCall4(
            &mut (*dict).lexize,
            PointerGetDatum((*dict).dictData),
            PointerGetDatum(VARDATA_ANY(in_ as *const c_char) as *const c_void),
            Int32GetDatum(VARSIZE_ANY_EXHDR(in_ as *const c_char) as int32),
            PointerGetDatum(&mut dstate as *mut DictSubState as *const c_void),
        )) as *mut TSLexeme;
        if !ptr.is_null() {
            res = ptr;
        }
    }

    if res.is_null() {
        PG_RETURN_NULL!(fcinfo);
    }

    ptr = res;
    while !(*ptr).lexeme.is_null() {
        ptr = ptr.add(1);
    }
    da = palloc(std::mem::size_of::<Datum>() * (ptr.offset_from(res) as usize)) as *mut Datum;
    ptr = res;
    while !(*ptr).lexeme.is_null() {
        *da.add(ptr.offset_from(res) as usize) = CStringGetTextDatum((*ptr).lexeme);
        ptr = ptr.add(1);
    }

    a = construct_array_builtin(da, ptr.offset_from(res) as c_int, TEXTOID);

    ptr = res;
    while !(*ptr).lexeme.is_null() {
        pfree(DatumGetPointer(*da.add(ptr.offset_from(res) as usize)) as *mut c_void);
        pfree((*ptr).lexeme as *mut c_void);
        ptr = ptr.add(1);
    }
    pfree(res as *mut c_void);
    pfree(da as *mut c_void);

    PG_RETURN_POINTER!(a)
}

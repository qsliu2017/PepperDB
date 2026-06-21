//! src/backend/utils/cache/funccache.c
//!
//! funccache.c
//!   Function cache management.
//!
//! funccache.c manages a cache of function execution data.  The cache
//! is used by SQL-language and PL/pgSQL functions, and could be used by
//! other function languages.  Each cache entry is specific to the execution
//! of a particular function (identified by OID) with specific input data
//! types; so a polymorphic function could have many associated cache entries.
//! Trigger functions similarly have a cache entry per trigger.  These rules
//! allow the cached data to be specific to the particular data types the
//! function call will be dealing with.
//!
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/cache/funccache.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};
use std::ptr;

use crate::c::{uint32, uint64, Size};
use crate::postgres_ext::Oid;
use crate::access::common::tupdesc::TupleDesc;
use crate::nodes::nodes::Node;

// ---------------------------------------------------------------------------
// funccache.h  --  src/include/utils/funccache.h
//
// funccache.h
//   Function cache definitions.
//
// See funccache.c for comments.
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
//
// src/include/utils/funccache.h
// ---------------------------------------------------------------------------

/*
 * Callback that cached_function_compile() invokes when it's necessary to
 * compile a cached function.  The callback must fill in *function (except
 * for the fields of struct CachedFunction), or throw an error if trouble.
 *	fcinfo: current call information
 *	procTup: function's pg_proc row from catcache
 *	hashkey: hash key that will be used for the function
 *	function: pre-zeroed workspace, of size passed to cached_function_compile()
 *	forValidator: passed through from cached_function_compile()
 */
pub type CachedFunctionCompileCallback = unsafe extern "C" fn(
    fcinfo: FunctionCallInfo,
    procTup: HeapTuple,
    hashkey: *const CachedFunctionHashKey,
    function: *mut CachedFunction,
    forValidator: bool,
);

/*
 * Callback called when discarding a cache entry.  Free any free-able
 * subsidiary data of cfunc, but not the struct CachedFunction itself.
 */
pub type CachedFunctionDeleteCallback = unsafe extern "C" fn(cfunc: *mut CachedFunction);

/*
 * Hash lookup key for functions.  This must account for all aspects
 * of a specific call that might lead to different data types or
 * collations being used within the function.
 */
#[repr(C)]
pub struct CachedFunctionHashKey {
    pub funcOid: Oid,

    pub isTrigger: bool,      /* true if called as a DML trigger */
    pub isEventTrigger: bool, /* true if called as an event trigger */

    /* be careful that pad bytes in this struct get zeroed! */

    /*
     * We include the language-specific size of the function's cache entry in
     * the cache key.  This covers the case where CREATE OR REPLACE FUNCTION
     * is used to change the implementation language, and the new language
     * also uses funccache.c but needs a different-sized cache entry.
     */
    pub cacheEntrySize: Size,

    /*
     * For a trigger function, the OID of the trigger is part of the hash key
     * --- we want to compile the trigger function separately for each trigger
     * it is used with, in case the rowtype or transition table names are
     * different.  Zero if not called as a DML trigger.
     */
    pub trigOid: Oid,

    /*
     * We must include the input collation as part of the hash key too,
     * because we have to generate different plans (with different Param
     * collations) for different collation settings.
     */
    pub inputCollation: Oid,

    /* Number of arguments (counting input arguments only, ie pronargs) */
    pub nargs: c_int,

    /* If you change anything below here, fix hashing code in funccache.c! */

    /*
     * If relevant, the result descriptor for a function returning composite.
     */
    pub callResultType: TupleDesc,

    /*
     * Input argument types, with any polymorphic types resolved to actual
     * types.  Only the first nargs entries are valid.
     */
    pub argtypes: [Oid; FUNC_MAX_ARGS as usize],
}

/*
 * Representation of a compiled function.  This struct contains just the
 * fields that funccache.c needs to deal with.  It will typically be
 * embedded in a larger struct containing function-language-specific data.
 */
#[repr(C)]
pub struct CachedFunction {
    /* back-link to hashtable entry, or NULL if not in hash table */
    pub fn_hashkey: *mut CachedFunctionHashKey,
    /* xmin and ctid of function's pg_proc row; used to detect invalidation */
    pub fn_xmin: TransactionId,
    pub fn_tid: ItemPointerData,
    /* deletion callback */
    pub dcallback: Option<CachedFunctionDeleteCallback>,

    /* this field changes when the function is used: */
    pub use_count: uint64,
}

// ---------------------------------------------------------------------------
// funccache.c
// ---------------------------------------------------------------------------

/*
 * Hash table for cached functions
 */
static mut cfunc_hashtable: *mut HTAB = ptr::null_mut();

#[repr(C)]
struct CachedFunctionHashEntry {
    key: CachedFunctionHashKey, /* hash key, must be first */
    function: *mut CachedFunction, /* points to data of language-specific size */
}

const FUNCS_PER_USER: c_long = 128; /* initial table size */

/*
 * Initialize the hash table on first use.
 *
 * The hash table will be in TopMemoryContext regardless of caller's context.
 */
unsafe fn cfunc_hashtable_init() {
    let mut ctl: HASHCTL = std::mem::zeroed();

    /* don't allow double-initialization */
    Assert(cfunc_hashtable.is_null());

    ctl.keysize = std::mem::size_of::<CachedFunctionHashKey>();
    ctl.entrysize = std::mem::size_of::<CachedFunctionHashEntry>();
    ctl.hash = Some(cfunc_hash);
    ctl.match_ = Some(cfunc_match);
    cfunc_hashtable = hash_create(
        c"Cached function hash".as_ptr(),
        FUNCS_PER_USER,
        &mut ctl,
        HASH_ELEM | HASH_FUNCTION | HASH_COMPARE,
    );
}

/*
 * cfunc_hash: hash function for cfunc hash table
 *
 * We need special hash and match functions to deal with the optional
 * presence of a TupleDesc in the hash keys.  As long as we have to do
 * that, we might as well also be smart about not comparing unused
 * elements of the argtypes arrays.
 */
unsafe extern "C" fn cfunc_hash(key: *const c_void, keysize: Size) -> uint32 {
    let k = key as *const CachedFunctionHashKey;
    let mut h: uint32;

    Assert(keysize == std::mem::size_of::<CachedFunctionHashKey>());
    /* Hash all the fixed fields except callResultType */
    h = DatumGetUInt32(hash_any(
        k as *const u8,
        core::mem::offset_of!(CachedFunctionHashKey, callResultType) as c_int,
    ));
    /* Incorporate input argument types */
    if (*k).nargs > 0 {
        h = hash_combine(
            h,
            DatumGetUInt32(hash_any(
                (*k).argtypes.as_ptr() as *const u8,
                ((*k).nargs as usize * std::mem::size_of::<Oid>()) as c_int,
            )),
        );
    }
    /* Incorporate callResultType if present */
    if !(*k).callResultType.is_null() {
        h = hash_combine(h, hashRowType((*k).callResultType));
    }
    h
}

/*
 * cfunc_match: match function to use with cfunc_hash
 */
unsafe extern "C" fn cfunc_match(key1: *const c_void, key2: *const c_void, keysize: Size) -> c_int {
    let k1 = key1 as *const CachedFunctionHashKey;
    let k2 = key2 as *const CachedFunctionHashKey;

    Assert(keysize == std::mem::size_of::<CachedFunctionHashKey>());
    /* Compare all the fixed fields except callResultType */
    if memcmp(
        k1 as *const c_void,
        k2 as *const c_void,
        core::mem::offset_of!(CachedFunctionHashKey, callResultType),
    ) != 0
    {
        return 1; /* not equal */
    }
    /* Compare input argument types (we just verified that nargs matches) */
    if (*k1).nargs > 0
        && memcmp(
            (*k1).argtypes.as_ptr() as *const c_void,
            (*k2).argtypes.as_ptr() as *const c_void,
            (*k1).nargs as usize * std::mem::size_of::<Oid>(),
        ) != 0
    {
        return 1; /* not equal */
    }
    /* Compare callResultType */
    if !(*k1).callResultType.is_null() {
        if !(*k2).callResultType.is_null() {
            if !equalRowTypes((*k1).callResultType, (*k2).callResultType) {
                return 1; /* not equal */
            }
        } else {
            return 1; /* not equal */
        }
    } else {
        if !(*k2).callResultType.is_null() {
            return 1; /* not equal */
        }
    }
    0 /* equal */
}

/*
 * Look up the CachedFunction for the given hash key.
 * Returns NULL if not present.
 */
unsafe fn cfunc_hashtable_lookup(func_key: *mut CachedFunctionHashKey) -> *mut CachedFunction {
    if cfunc_hashtable.is_null() {
        return ptr::null_mut();
    }

    let hentry = hash_search(
        cfunc_hashtable,
        func_key as *mut c_void,
        HASH_FIND,
        ptr::null_mut(),
    ) as *mut CachedFunctionHashEntry;
    if !hentry.is_null() {
        (*hentry).function
    } else {
        ptr::null_mut()
    }
}

/*
 * Insert a hash table entry.
 */
unsafe fn cfunc_hashtable_insert(
    function: *mut CachedFunction,
    func_key: *mut CachedFunctionHashKey,
) {
    let mut found: bool = false;

    if cfunc_hashtable.is_null() {
        cfunc_hashtable_init();
    }

    let hentry = hash_search(
        cfunc_hashtable,
        func_key as *mut c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut CachedFunctionHashEntry;
    if found {
        elog!(WARNING, "trying to insert a function that already exists");
    }

    /*
     * If there's a callResultType, copy it into TopMemoryContext.  If we're
     * unlucky enough for that to fail, leave the entry with null
     * callResultType, which will probably never match anything.
     */
    if !(*func_key).callResultType.is_null() {
        let oldcontext = MemoryContextSwitchTo(TopMemoryContext);

        (*hentry).key.callResultType = ptr::null_mut();
        (*hentry).key.callResultType = CreateTupleDescCopy((*func_key).callResultType);
        MemoryContextSwitchTo(oldcontext);
    }

    (*hentry).function = function;

    /* Set back-link from function to hashtable key */
    (*function).fn_hashkey = &mut (*hentry).key;
}

/*
 * Delete a hash table entry.
 */
unsafe fn cfunc_hashtable_delete(function: *mut CachedFunction) {
    /* do nothing if not in table */
    if (*function).fn_hashkey.is_null() {
        return;
    }

    /*
     * We need to free the callResultType if present, which is slightly tricky
     * because it has to be valid during the hashtable search.  Fortunately,
     * because we have the hashkey back-link, we can grab that pointer before
     * deleting the hashtable entry.
     */
    let tupdesc: TupleDesc = (*(*function).fn_hashkey).callResultType;

    let hentry = hash_search(
        cfunc_hashtable,
        (*function).fn_hashkey as *mut c_void,
        HASH_REMOVE,
        ptr::null_mut(),
    ) as *mut CachedFunctionHashEntry;
    if hentry.is_null() {
        elog!(WARNING, "trying to delete function that does not exist");
    }

    /* Remove back link, which no longer points to allocated storage */
    (*function).fn_hashkey = ptr::null_mut();

    /* Release the callResultType if present */
    if !tupdesc.is_null() {
        FreeTupleDesc(tupdesc);
    }
}

/*
 * Compute the hashkey for a given function invocation
 *
 * The hashkey is returned into the caller-provided storage at *hashkey.
 * Note however that if a callResultType is incorporated, we've not done
 * anything about copying that.
 */
unsafe fn compute_function_hashkey(
    fcinfo: FunctionCallInfo,
    procStruct: Form_pg_proc,
    hashkey: *mut CachedFunctionHashKey,
    cacheEntrySize: Size,
    includeResultType: bool,
    forValidator: bool,
) {
    /* Make sure pad bytes within fixed part of the struct are zero */
    memset(
        hashkey as *mut c_void,
        0,
        core::mem::offset_of!(CachedFunctionHashKey, argtypes),
    );

    /* get function OID */
    (*hashkey).funcOid = (*(*fcinfo).flinfo).fn_oid;

    /* get call context */
    (*hashkey).isTrigger = CALLED_AS_TRIGGER(fcinfo);
    (*hashkey).isEventTrigger = CALLED_AS_EVENT_TRIGGER(fcinfo);

    /* record cacheEntrySize so multiple languages can share hash table */
    (*hashkey).cacheEntrySize = cacheEntrySize;

    /*
     * If DML trigger, include trigger's OID in the hash, so that each trigger
     * usage gets a different hash entry, allowing for e.g. different relation
     * rowtypes or transition table names.  In validation mode we do not know
     * what relation or transition table names are intended to be used, so we
     * leave trigOid zero; the hash entry built in this case will never be
     * used for any actual calls.
     *
     * We don't currently need to distinguish different event trigger usages
     * in the same way, since the special parameter variables don't vary in
     * type in that case.
     */
    if (*hashkey).isTrigger && !forValidator {
        let trigdata = (*fcinfo).context as *mut TriggerData;

        (*hashkey).trigOid = (*(*trigdata).tg_trigger).tgoid;
    }

    /* get input collation, if known */
    (*hashkey).inputCollation = (*fcinfo).fncollation;

    /*
     * We include only input arguments in the hash key, since output argument
     * types can be deduced from those, and it would require extra cycles to
     * include the output arguments.  But we have to resolve any polymorphic
     * argument types to the real types for the call.
     */
    if (*procStruct).pronargs > 0 {
        (*hashkey).nargs = (*procStruct).pronargs as c_int;
        memcpy(
            (*hashkey).argtypes.as_mut_ptr() as *mut c_void,
            (*procStruct).proargtypes.values.as_ptr() as *const c_void,
            (*procStruct).pronargs as usize * std::mem::size_of::<Oid>(),
        );
        cfunc_resolve_polymorphic_argtypes(
            (*procStruct).pronargs as c_int,
            (*hashkey).argtypes.as_mut_ptr(),
            ptr::null_mut(), /* all args are inputs */
            (*(*fcinfo).flinfo).fn_expr,
            forValidator,
            NameStr(&(*procStruct).proname),
        );
    }

    /*
     * While regular OUT arguments are sufficiently represented by the
     * resolved input arguments, a function returning composite has additional
     * variability: ALTER TABLE/ALTER TYPE could affect what it returns. Also,
     * a function returning RECORD may depend on a column definition list to
     * determine its output rowtype.  If the caller needs the exact result
     * type to be part of the hash lookup key, we must run
     * get_call_result_type() to find that out.
     */
    if includeResultType {
        let mut resultTypeId: Oid = 0;
        let mut tupdesc: TupleDesc = ptr::null_mut();

        match get_call_result_type(fcinfo, &mut resultTypeId, &mut tupdesc) {
            TYPEFUNC_COMPOSITE | TYPEFUNC_COMPOSITE_DOMAIN => {
                (*hashkey).callResultType = tupdesc;
            }
            _ => {
                /* scalar result, or indeterminate rowtype */
            }
        }
    }
}

/*
 * This is the same as the standard resolve_polymorphic_argtypes() function,
 * except that:
 * 1. We go ahead and report the error if we can't resolve the types.
 * 2. We treat RECORD-type input arguments (not output arguments) as if
 *    they were polymorphic, replacing their types with the actual input
 *    types if we can determine those.  This allows us to create a separate
 *    function cache entry for each named composite type passed to such an
 *    argument.
 * 3. In validation mode, we have no inputs to look at, so assume that
 *    polymorphic arguments are integer, integer-array or integer-range.
 */
#[no_mangle]
pub unsafe fn cfunc_resolve_polymorphic_argtypes(
    numargs: c_int,
    argtypes: *mut Oid,
    argmodes: *mut c_char,
    call_expr: *mut Node,
    forValidator: bool,
    proname: *const c_char,
) {
    let mut i: c_int;

    if !forValidator {
        let mut inargno: c_int;

        /* normal case, pass to standard routine */
        if !resolve_polymorphic_argtypes(numargs, argtypes, argmodes, call_expr) {
            elog!(
                ERROR,
                "could not determine actual argument type for polymorphic function \"{}\"",
                CStr_to_str(proname)
            );
            unreachable!();
        }
        /* also, treat RECORD inputs (but not outputs) as polymorphic */
        inargno = 0;
        i = 0;
        while i < numargs {
            let argmode: c_char = if !argmodes.is_null() {
                *argmodes.offset(i as isize)
            } else {
                PROARGMODE_IN
            };

            if argmode == PROARGMODE_OUT || argmode == PROARGMODE_TABLE {
                i += 1;
                continue;
            }
            if *argtypes.offset(i as isize) == RECORDOID
                || *argtypes.offset(i as isize) == RECORDARRAYOID
            {
                let resolvedtype: Oid = get_call_expr_argtype(call_expr, inargno);

                if OidIsValid(resolvedtype) {
                    *argtypes.offset(i as isize) = resolvedtype;
                }
            }
            inargno += 1;
            i += 1;
        }
    } else {
        /* special validation case (no need to do anything for RECORD) */
        i = 0;
        while i < numargs {
            match *argtypes.offset(i as isize) {
                ANYELEMENTOID | ANYNONARRAYOID | ANYENUMOID /* XXX dubious */
                | ANYCOMPATIBLEOID | ANYCOMPATIBLENONARRAYOID => {
                    *argtypes.offset(i as isize) = INT4OID;
                }
                ANYARRAYOID | ANYCOMPATIBLEARRAYOID => {
                    *argtypes.offset(i as isize) = INT4ARRAYOID;
                }
                ANYRANGEOID | ANYCOMPATIBLERANGEOID => {
                    *argtypes.offset(i as isize) = INT4RANGEOID;
                }
                ANYMULTIRANGEOID => {
                    *argtypes.offset(i as isize) = INT4MULTIRANGEOID;
                }
                _ => {}
            }
            i += 1;
        }
    }
}

/*
 * delete_function - clean up as much as possible of a stale function cache
 *
 * We can't release the CachedFunction struct itself, because of the
 * possibility that there are fn_extra pointers to it.  We can release
 * the subsidiary storage, but only if there are no active evaluations
 * in progress.  Otherwise we'll just leak that storage.  Since the
 * case would only occur if a pg_proc update is detected during a nested
 * recursive call on the function, a leak seems acceptable.
 *
 * Note that this can be called more than once if there are multiple fn_extra
 * pointers to the same function cache.  Hence be careful not to do things
 * twice.
 */
unsafe fn delete_function(func: *mut CachedFunction) {
    /* remove function from hash table (might be done already) */
    cfunc_hashtable_delete(func);

    /* release the function's storage if safe and not done already */
    if (*func).use_count == 0 && (*func).dcallback.is_some() {
        (((*func).dcallback).unwrap())(func);
        (*func).dcallback = None;
    }
}

/*
 * Compile a cached function, if no existing cache entry is suitable.
 *
 * fcinfo is the current call information.
 *
 * function should be NULL or the result of a previous call of
 * cached_function_compile() for the same fcinfo.  The caller will
 * typically save the result in fcinfo->flinfo->fn_extra, or in a
 * field of a struct pointed to by fn_extra, to re-use in later
 * calls within the same query.
 *
 * ccallback and dcallback are function-language-specific callbacks to
 * compile and delete a cached function entry.  dcallback can be NULL
 * if there's nothing for it to do.
 *
 * cacheEntrySize is the function-language-specific size of the cache entry
 * (which embeds a CachedFunction struct and typically has many more fields
 * after that).
 *
 * If includeResultType is true and the function returns composite,
 * include the actual result descriptor in the cache lookup key.
 *
 * If forValidator is true, we're only compiling for validation purposes,
 * and so some checks are skipped.
 *
 * Note: it's important for this to fall through quickly if the function
 * has already been compiled.
 *
 * Note: this function leaves the "use_count" field as zero.  The caller
 * is expected to increment the use_count and decrement it when done with
 * the cache entry.
 */
#[no_mangle]
pub unsafe fn cached_function_compile(
    fcinfo: FunctionCallInfo,
    mut function: *mut CachedFunction,
    ccallback: CachedFunctionCompileCallback,
    dcallback: Option<CachedFunctionDeleteCallback>,
    cacheEntrySize: Size,
    includeResultType: bool,
    forValidator: bool,
) -> *mut CachedFunction {
    let funcOid: Oid = (*(*fcinfo).flinfo).fn_oid;
    let mut hashkey: CachedFunctionHashKey = std::mem::zeroed();
    let mut function_valid: bool = false;
    let mut hashkey_valid: bool = false;
    let mut new_function: bool = false;

    /*
     * Lookup the pg_proc tuple by Oid; we'll need it in any case
     */
    let procTup: HeapTuple = SearchSysCache1(PROCOID as c_int, ObjectIdGetDatum(funcOid));
    if !HeapTupleIsValid(procTup) {
        elog!(ERROR, "cache lookup failed for function {}", funcOid);
        unreachable!();
    }
    let procStruct: Form_pg_proc = GETSTRUCT(procTup) as Form_pg_proc;

    /*
     * Do we already have a cache entry for the current FmgrInfo?  If not, try
     * to find one in the hash table.
     */
    'recheck: loop {
        if function.is_null() {
            /* Compute hashkey using function signature and actual arg types */
            compute_function_hashkey(
                fcinfo,
                procStruct,
                &mut hashkey,
                cacheEntrySize,
                includeResultType,
                forValidator,
            );
            hashkey_valid = true;

            /* And do the lookup */
            function = cfunc_hashtable_lookup(&mut hashkey);
        }

        if !function.is_null() {
            /* We have a compiled function, but is it still valid? */
            if (*function).fn_xmin == HeapTupleHeaderGetRawXmin((*procTup).t_data)
                && ItemPointerEquals(&mut (*function).fn_tid, &mut (*procTup).t_self)
            {
                function_valid = true;
            } else {
                /*
                 * Nope, so remove it from hashtable and try to drop associated
                 * storage (if not done already).
                 */
                delete_function(function);

                /*
                 * If the function isn't in active use then we can overwrite the
                 * func struct with new data, allowing any other existing fn_extra
                 * pointers to make use of the new definition on their next use.
                 * If it is in use then just leave it alone and make a new one.
                 * (The active invocations will run to completion using the
                 * previous definition, and then the cache entry will just be
                 * leaked; doesn't seem worth adding code to clean it up, given
                 * what a corner case this is.)
                 *
                 * If we found the function struct via fn_extra then it's possible
                 * a replacement has already been made, so go back and recheck the
                 * hashtable.
                 */
                if (*function).use_count != 0 {
                    function = ptr::null_mut();
                    if !hashkey_valid {
                        continue 'recheck;
                    }
                }
            }
        }

        break;
    }

    /*
     * If the function wasn't found or was out-of-date, we have to compile it.
     */
    if !function_valid {
        /*
         * Calculate hashkey if we didn't already; we'll need it to store the
         * completed function.
         */
        if !hashkey_valid {
            compute_function_hashkey(
                fcinfo,
                procStruct,
                &mut hashkey,
                cacheEntrySize,
                includeResultType,
                forValidator,
            );
        }

        /*
         * Create the new function struct, if not done already.  The function
         * cache entry will be kept for the life of the backend, so put it in
         * TopMemoryContext.
         */
        Assert(cacheEntrySize >= std::mem::size_of::<CachedFunction>());
        if function.is_null() {
            function = MemoryContextAllocZero(TopMemoryContext, cacheEntrySize) as *mut CachedFunction;
            new_function = true;
        } else {
            /* re-using a previously existing struct, so clear it out */
            memset(function as *mut c_void, 0, cacheEntrySize);
        }

        /*
         * However, if function compilation fails, we'd like not to leak the
         * function struct, so use a PG_TRY block to prevent that.  (It's up
         * to the compile callback function to avoid its own internal leakage
         * in such cases.)  Unfortunately, freeing the struct is only safe if
         * we just allocated it: otherwise there are probably fn_extra
         * pointers to it.
         */
        PG_TRY!(
            {
                /*
                 * Do the hard, language-specific part.
                 */
                ccallback(fcinfo, procTup, &hashkey, function, forValidator);
            },
            {
                if new_function {
                    pfree(function as *mut c_void);
                }
                PG_RE_THROW();
            }
        );

        /*
         * Fill in the CachedFunction part.  (We do this last to prevent the
         * function from looking valid before it's fully built.)  fn_hashkey
         * will be set by cfunc_hashtable_insert; use_count remains zero.
         */
        (*function).fn_xmin = HeapTupleHeaderGetRawXmin((*procTup).t_data);
        (*function).fn_tid = (*procTup).t_self;
        (*function).dcallback = dcallback;

        /*
         * Add the completed struct to the hash table.
         */
        cfunc_hashtable_insert(function, &mut hashkey);
    }

    ReleaseSysCache(procTup);

    /*
     * Finally return the compiled function
     */
    function
}

// ---------------------------------------------------------------------------
// Local stubs for as-yet-unported dependencies.
// ---------------------------------------------------------------------------

unsafe fn Assert(_cond: bool) {}

unsafe fn CStr_to_str<'a>(_s: *const c_char) -> &'a str {
    unimplemented!() // TODO: helper for elog formatting
}

unsafe fn hash_any(_k: *const u8, _keylen: c_int) -> Datum {
    crate::common::hashfn::hash_any(_k as _, _keylen as _) as _
}

unsafe fn hash_combine(_a: uint32, _b: uint32) -> uint32 {
    crate::common::hashfn::hash_combine(_a as _, _b as _) as _
}

unsafe fn hashRowType(_desc: TupleDesc) -> uint32 {
    crate::access::common::tupdesc::hashRowType(_desc as _) as _
}

unsafe fn equalRowTypes(_tupdesc1: TupleDesc, _tupdesc2: TupleDesc) -> bool {
    crate::access::common::tupdesc::equalRowTypes(_tupdesc1 as _, _tupdesc2 as _)
}

unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> *mut HTAB {
    crate::utils::hash::dynahash::hash_create(_tabname as _, _nelem as _, _info as _, _flags as _) as _
}

unsafe fn hash_search(
    _hashp: *mut HTAB,
    _key_ptr: *mut c_void,
    _action: HASHACTION,
    _found: *mut bool,
) -> *mut c_void {
    crate::utils::hash::dynahash::hash_search(_hashp as _, _key_ptr as _, core::mem::transmute(_action), _found as _) as _
}

unsafe fn CreateTupleDescCopy(_tupdesc: TupleDesc) -> TupleDesc {
    crate::access::common::tupdesc::CreateTupleDescCopy(_tupdesc as _) as _
}

unsafe fn FreeTupleDesc(_tupdesc: TupleDesc) {
    crate::access::common::tupdesc::FreeTupleDesc(_tupdesc as _)
}

unsafe fn CALLED_AS_TRIGGER(_fcinfo: FunctionCallInfo) -> bool {
    crate::commands::trigger::CALLED_AS_TRIGGER(_fcinfo as _)
}

unsafe fn CALLED_AS_EVENT_TRIGGER(_fcinfo: FunctionCallInfo) -> bool {
    unimplemented!() // TODO: src/include/commands/event_trigger.h
}

unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> TypeFuncClass {
    crate::utils::fmgr::funcapi::get_call_result_type(_fcinfo as _, _resultTypeId as _, _resultTupleDesc as _) as _
}

unsafe fn resolve_polymorphic_argtypes(
    _numargs: c_int,
    _argtypes: *mut Oid,
    _argmodes: *mut c_char,
    _call_expr: *mut Node,
) -> bool {
    crate::utils::fmgr::funcapi::resolve_polymorphic_argtypes(_numargs as _, _argtypes as _, _argmodes as _, _call_expr as _)
}

unsafe fn get_call_expr_argtype(_expr: *mut Node, _argnum: c_int) -> Oid {
    crate::utils::fmgr::get_call_expr_argtype(_expr as _, _argnum as _) as _
}

unsafe fn HeapTupleHeaderGetRawXmin(_tup: HeapTupleHeader) -> TransactionId {
    crate::access::htup_details::HeapTupleHeaderGetRawXmin(_tup as _) as _
}

unsafe fn ItemPointerEquals(_pointer1: ItemPointer, _pointer2: ItemPointer) -> bool {
    crate::storage::itemptr::ItemPointerEquals(_pointer1 as _, _pointer2 as _)
}

unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    crate::utils::cache::syscache::SearchSysCache1(_cacheId as _, _key1 as _) as _
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    crate::utils::cache::syscache::ReleaseSysCache(_tuple as _)
}

unsafe fn HeapTupleIsValid(_tuple: HeapTuple) -> bool {
    crate::access::htup_details::HeapTupleIsValid(_tuple as _)
}

unsafe fn GETSTRUCT(_tuple: HeapTuple) -> *mut c_void {
    crate::access::htup_details::GETSTRUCT(_tuple as _) as _
}

unsafe fn memset(_s: *mut c_void, _c: c_int, _n: usize) -> *mut c_void {
    libc::memset(_s as _, _c as _, _n as _) as _
}

unsafe fn memcpy(_dst: *mut c_void, _src: *const c_void, _n: usize) -> *mut c_void {
    libc::memcpy(_dst as _, _src as _, _n as _) as _
}

unsafe fn memcmp(_s1: *const c_void, _s2: *const c_void, _n: usize) -> c_int {
    libc::memcmp(_s1 as _, _s2 as _, _n as _) as _
}

macro_rules! PG_TRY {
    ($try_block:block, $catch_block:block) => {{
        // TODO: src/include/utils/elog.h - faithful PG_TRY/PG_CATCH/PG_END_TRY
        $try_block
    }};
}
use PG_TRY;

unsafe fn PG_RE_THROW() {
    unimplemented!() // TODO: src/include/utils/elog.h
}

// ---------------------------------------------------------------------------
// Local stub types for as-yet-unported dependencies.
// ---------------------------------------------------------------------------

const FUNC_MAX_ARGS: usize = 100; // TODO: src/include/pg_config_manual.h

const PROARGMODE_IN: c_char = b'i' as c_char; // TODO: src/include/catalog/pg_proc.h
const PROARGMODE_OUT: c_char = b'o' as c_char;
const PROARGMODE_TABLE: c_char = b't' as c_char;

const PROCOID: c_int = 47; // TODO: src/include/catalog/pg_proc_d.h (SysCache id)

const HASH_ELEM: c_int = 0x0008; // TODO: src/include/utils/hsearch.h
const HASH_FUNCTION: c_int = 0x0010;
const HASH_COMPARE: c_int = 0x0400;

// TODO: src/include/catalog/pg_type_d.h
const RECORDOID: Oid = 2249;
const RECORDARRAYOID: Oid = 2287;
const ANYELEMENTOID: Oid = 2283;
const ANYNONARRAYOID: Oid = 2776;
const ANYENUMOID: Oid = 3500;
const ANYCOMPATIBLEOID: Oid = 5077;
const ANYCOMPATIBLENONARRAYOID: Oid = 5079;
const ANYARRAYOID: Oid = 2277;
const ANYCOMPATIBLEARRAYOID: Oid = 5078;
const ANYRANGEOID: Oid = 3831;
const ANYCOMPATIBLERANGEOID: Oid = 5080;
const ANYMULTIRANGEOID: Oid = 4537;
const INT4OID: Oid = 23;
const INT4ARRAYOID: Oid = 1007;
const INT4RANGEOID: Oid = 3904;
const INT4MULTIRANGEOID: Oid = 4451;

// TODO: src/include/utils/hsearch.h
#[allow(non_camel_case_types)]
type HASHACTION = c_int;
const HASH_FIND: HASHACTION = 0;
const HASH_ENTER: HASHACTION = 1;
const HASH_REMOVE: HASHACTION = 2;

#[repr(C)]
struct HTAB {
    _private: [u8; 0],
} // TODO: src/backend/utils/hash/dynahash.c

#[repr(C)]
struct HASHCTL {
    keysize: Size,
    entrysize: Size,
    hash: Option<unsafe extern "C" fn(key: *const c_void, keysize: Size) -> uint32>,
    match_: Option<unsafe extern "C" fn(key1: *const c_void, key2: *const c_void, keysize: Size) -> c_int>,
} // TODO: src/include/utils/hsearch.h (partial)

// TODO: src/include/utils/fmgr.h
#[allow(non_camel_case_types)]
type FunctionCallInfo = *mut FunctionCallInfoBaseData;

#[repr(C)]
pub struct FunctionCallInfoBaseData {
    flinfo: *mut FmgrInfo,
    context: *mut Node,
    fncollation: Oid,
}

#[repr(C)]
struct FmgrInfo {
    fn_oid: Oid,
    fn_expr: *mut Node,
}

// TODO: src/include/access/htup.h
#[allow(non_camel_case_types)]
type HeapTuple = *mut HeapTupleData;

#[repr(C)]
pub struct HeapTupleData {
    t_self: ItemPointerData,
    t_data: HeapTupleHeader,
}

#[allow(non_camel_case_types)]
type HeapTupleHeader = *mut c_void; // TODO: src/include/access/htup_details.h

// TODO: src/include/storage/itemptr.h
#[allow(non_camel_case_types)]
type ItemPointer = *mut ItemPointerData;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ItemPointerData {
    _private: [u8; 6],
}

// TODO: src/include/catalog/pg_proc.h
#[allow(non_camel_case_types)]
type Form_pg_proc = *mut FormData_pg_proc;

#[repr(C)]
struct FormData_pg_proc {
    proname: NameData,
    pronargs: i16,
    proargtypes: oidvector,
}

// NameData and oidvector come from crate::prelude (crate::c). FLEXIBLE_ARRAY_MEMBER
// also comes from crate::c.

// TODO: src/include/commands/trigger.h
#[repr(C)]
struct TriggerData {
    tg_trigger: *mut Trigger,
}

#[repr(C)]
struct Trigger {
    tgoid: Oid,
}

// TODO: src/include/nodes/nodes.h (TypeFuncClass)
#[allow(non_camel_case_types)]
type TypeFuncClass = c_int;
const TYPEFUNC_COMPOSITE: TypeFuncClass = 1;
const TYPEFUNC_COMPOSITE_DOMAIN: TypeFuncClass = 2;

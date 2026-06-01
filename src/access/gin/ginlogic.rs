//! Translation of PostgreSQL's GIN binary/ternary consistent-check shims.
//!
//!   IMPL:   postgres/src/backend/access/gin/ginlogic.c
//!   HEADER: a MINIMAL subset of postgres/src/include/access/gin_private.h
//!           (GinScanKeyData) and gin.h (GinTernaryValue + GIN_* consts,
//!           GIN_SEARCH_MODE_EVERYTHING) merged in below.
//!
//! A GIN operator class can provide a boolean or ternary consistent function,
//! or both.  This file provides both boolean and ternary interfaces to the
//! rest of the GIN code, even if only one of them is implemented by the
//! opclass.
//!
//! Providing a boolean interface when the opclass implements only the ternary
//! function is straightforward - just call the ternary function with the
//! check-array as is, and map GIN_TRUE/GIN_FALSE/GIN_MAYBE to TRUE / FALSE /
//! TRUE+recheck respectively.  Providing a ternary interface when the opclass
//! only implements a boolean function is implemented by calling the boolean
//! function many times, with all the MAYBE arguments set to all combinations of
//! TRUE and FALSE (up to MAX_MAYBE_ENTRIES MAYBE arguments).

// prelude gives Datum, c-types (int32/uint16/uint32/Oid/c_char/c_int/c_void),
// null/null_mut, and the Datum get/put helpers (DatumGetBool, PointerGetDatum,
// UInt16GetDatum, UInt32GetDatum, BoolGetDatum) plus OidIsValid.
use crate::prelude::*;
use crate::access::stratnum::StrategyNumber; // uint16
use crate::utils::fmgr::{FmgrInfo, FunctionCall7Coll, FunctionCall8Coll, FunctionCallInfo};

/*
 * gin.h subset: GinTernaryValue and its three encodings.
 *
 * GinTernaryValue is "char" in C; sizeof must equal sizeof(bool).  We mirror
 * the C type as c_char and define our own module-scoped GIN_* constants (a
 * separate local copy already lives in utils/adt/tsginidx.rs).
 */
pub type GinTernaryValue = c_char;

pub const GIN_FALSE: GinTernaryValue = 0; /* item is not present / does not match */
pub const GIN_TRUE: GinTernaryValue = 1; /* item is present / matches */
pub const GIN_MAYBE: GinTernaryValue = 2; /* don't know if item is present */

/* DatumGetGinTernaryValue(X): (GinTernaryValue) X  (gin.h inline) */
#[inline]
pub fn DatumGetGinTernaryValue(X: Datum) -> GinTernaryValue {
    X as GinTernaryValue
}

/* gin.h: for internal use only */
pub const GIN_SEARCH_MODE_EVERYTHING: int32 = 3;

/*
 * gin.h: GinNullCategory is a signed-char category byte stored alongside each
 * entry value.  We only ever pass a pointer to it through fmgr here, so the
 * exact encoding is irrelevant; mirror it as c_char.
 */
pub type GinNullCategory = c_char;

/* gin_private.h: opaque to this file. */
pub type GinScanEntry = *mut c_void; /* struct GinScanEntryData * */

/*
 * gin_private.h: GinScanKey is a pointer to GinScanKeyData.
 *
 * The function-pointer fields below take a GinScanKey in C; we mirror that as
 * *mut GinScanKeyData.
 */
pub type GinScanKey = *mut GinScanKeyData;

/*
 * gin_private.h: MINIMAL mirror of struct GinScanKeyData.
 *
 * This is a partial #[repr(C)] mirror containing ONLY the fields touched by
 * ginlogic.c (plus the leading layout-relevant fields so offsets of the used
 * fields stay correct relative to the real struct).  Fields whose concrete
 * types are not needed here are kept as opaque pointers / ints with the right
 * size.  Fields after `recheckCurItem` in the real struct are omitted because
 * ginlogic.c never touches them; everything ginlogic.c reads/writes appears
 * here in declaration order.
 */
#[repr(C)]
pub struct GinScanKeyData {
    /* Real number of entries in scanEntry[] (always > 0) */
    pub nentries: uint32,
    /* Number of entries that extractQueryFn and consistentFn know about */
    pub nuserentries: uint32,

    /* array of GinScanEntry pointers, one per extracted search condition */
    pub scanEntry: *mut GinScanEntry,

    /* required/additional entry partitioning (opaque to ginlogic.c) */
    pub requiredEntries: *mut GinScanEntry,
    pub nrequired: c_int,
    pub additionalEntries: *mut GinScanEntry,
    pub nadditional: c_int,

    /* array of check flags, reported to consistentFn */
    pub entryRes: *mut GinTernaryValue,
    pub boolConsistentFn: Option<unsafe fn(key: GinScanKey) -> bool>,
    pub triConsistentFn: Option<unsafe fn(key: GinScanKey) -> GinTernaryValue>,
    pub consistentFmgrInfo: *mut FmgrInfo,
    pub triConsistentFmgrInfo: *mut FmgrInfo,
    pub collation: Oid,

    /* other data needed for calling consistentFn */
    pub query: Datum,
    /* NB: these three arrays have only nuserentries elements! */
    pub queryValues: *mut Datum,
    pub queryCategories: *mut GinNullCategory,
    pub extra_data: *mut Pointer,
    pub strategy: StrategyNumber,
    pub searchMode: int32,
    pub attnum: OffsetNumber,

    /* set by consistentFn (or directly by ginlogic shims) to request recheck */
    pub recheckCurItem: bool,
}

/* storage/off.h */
pub type OffsetNumber = uint16;
/* Pointer (= *mut c_char, c.h) comes from crate::c via the prelude glob. */

/*
 * gin_private.h: MINIMAL mirror of struct GinState.
 *
 * ginInitConsistentFunction only reads the per-attribute consistentFn /
 * triConsistentFn FmgrInfo arrays and the supportCollation array, so those are
 * the only fields mirrored.  The real struct has many more leading and
 * trailing fields; INDEX_MAX_KEYS sizes the arrays so the layout matches.
 */
pub const INDEX_MAX_KEYS: usize = 32; /* pg_config_manual.h default */

#[repr(C)]
pub struct GinState {
    /*
     * NB: the real GinState has several leading members before these arrays.
     * ginlogic.c only ever accesses these three by name, and we always
     * allocate/zero whole GinState values in tests, so an exact prefix is not
     * required here; this is a partial mirror.
     */
    pub consistentFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub triConsistentFn: [FmgrInfo; INDEX_MAX_KEYS],
    pub supportCollation: [Oid; INDEX_MAX_KEYS],
}

/*
 * Maximum number of MAYBE inputs that shimTriConsistentFn will try to resolve
 * by calling all combinations.
 */
const MAX_MAYBE_ENTRIES: usize = 4;

/*
 * Dummy consistent functions for an EVERYTHING key.  Just claim it matches.
 */
unsafe fn trueConsistentFn(key: GinScanKey) -> bool {
    (*key).recheckCurItem = false;
    true
}

unsafe fn trueTriConsistentFn(_key: GinScanKey) -> GinTernaryValue {
    GIN_TRUE
}

/*
 * A helper function for calling a regular, binary logic, consistent function.
 */
unsafe fn directBoolConsistentFn(key: GinScanKey) -> bool {
    /*
     * Initialize recheckCurItem in case the consistentFn doesn't know it
     * should set it.  The safe assumption in that case is to force recheck.
     */
    (*key).recheckCurItem = true;

    DatumGetBool(FunctionCall8Coll(
        (*key).consistentFmgrInfo,
        (*key).collation,
        PointerGetDatum((*key).entryRes as *const c_void),
        UInt16GetDatum((*key).strategy),
        (*key).query,
        UInt32GetDatum((*key).nuserentries),
        PointerGetDatum((*key).extra_data as *const c_void),
        PointerGetDatum(&mut (*key).recheckCurItem as *mut bool as *const c_void),
        PointerGetDatum((*key).queryValues as *const c_void),
        PointerGetDatum((*key).queryCategories as *const c_void),
    ))
}

/*
 * A helper function for calling a native ternary logic consistent function.
 */
unsafe fn directTriConsistentFn(key: GinScanKey) -> GinTernaryValue {
    DatumGetGinTernaryValue(FunctionCall7Coll(
        (*key).triConsistentFmgrInfo,
        (*key).collation,
        PointerGetDatum((*key).entryRes as *const c_void),
        UInt16GetDatum((*key).strategy),
        (*key).query,
        UInt32GetDatum((*key).nuserentries),
        PointerGetDatum((*key).extra_data as *const c_void),
        PointerGetDatum((*key).queryValues as *const c_void),
        PointerGetDatum((*key).queryCategories as *const c_void),
    ))
}

/*
 * This function implements a binary logic consistency check, using a ternary
 * logic consistent function provided by the opclass. GIN_MAYBE return value is
 * interpreted as true with recheck flag.
 */
unsafe fn shimBoolConsistentFn(key: GinScanKey) -> bool {
    let result: GinTernaryValue = DatumGetGinTernaryValue(FunctionCall7Coll(
        (*key).triConsistentFmgrInfo,
        (*key).collation,
        PointerGetDatum((*key).entryRes as *const c_void),
        UInt16GetDatum((*key).strategy),
        (*key).query,
        UInt32GetDatum((*key).nuserentries),
        PointerGetDatum((*key).extra_data as *const c_void),
        PointerGetDatum((*key).queryValues as *const c_void),
        PointerGetDatum((*key).queryCategories as *const c_void),
    ));
    if result == GIN_MAYBE {
        (*key).recheckCurItem = true;
        true
    } else {
        (*key).recheckCurItem = false;
        /* result is GIN_TRUE (1) or GIN_FALSE (0) -> bool */
        result != GIN_FALSE
    }
}

/*
 * This function implements a tri-state consistency check, using a boolean
 * consistent function provided by the opclass.
 *
 * Our strategy is to call consistentFn with MAYBE inputs replaced with every
 * combination of TRUE/FALSE. If consistentFn returns the same value for every
 * combination, that's the overall result. Otherwise, return MAYBE. Testing
 * every combination is O(n^2), so this is only feasible for a small number of
 * MAYBE inputs.
 *
 * NB: This function modifies the key->entryRes array.  For now that's okay so
 * long as we restore the entry-time contents before returning.
 */
unsafe fn shimTriConsistentFn(key: GinScanKey) -> GinTernaryValue {
    let mut maybeEntries: [usize; MAX_MAYBE_ENTRIES] = [0; MAX_MAYBE_ENTRIES];
    let mut nmaybe: usize;

    /*
     * Count how many MAYBE inputs there are, and store their indexes in
     * maybeEntries. If there are too many MAYBE inputs, it's not feasible to
     * test all combinations, so give up and return MAYBE.
     */
    nmaybe = 0;
    {
        let mut k = 0usize;
        while k < (*key).nentries as usize {
            if *(*key).entryRes.add(k) == GIN_MAYBE {
                if nmaybe >= MAX_MAYBE_ENTRIES {
                    return GIN_MAYBE;
                }
                maybeEntries[nmaybe] = k;
                nmaybe += 1;
            }
            k += 1;
        }
    }

    /*
     * If none of the inputs were MAYBE, we can just call the consistent
     * function as-is.
     */
    if nmaybe == 0 {
        return directBoolConsistentFn(key) as GinTernaryValue;
    }

    /* First call consistent function with all the maybe-inputs set FALSE */
    for j in 0..nmaybe {
        *(*key).entryRes.add(maybeEntries[j]) = GIN_FALSE;
    }
    let mut curResult: GinTernaryValue = directBoolConsistentFn(key) as GinTernaryValue;
    let mut recheck: bool = (*key).recheckCurItem;

    loop {
        /* Twiddle the entries for next combination. */
        let mut ti = 0usize;
        while ti < nmaybe {
            if *(*key).entryRes.add(maybeEntries[ti]) == GIN_FALSE {
                *(*key).entryRes.add(maybeEntries[ti]) = GIN_TRUE;
                break;
            } else {
                *(*key).entryRes.add(maybeEntries[ti]) = GIN_FALSE;
            }
            ti += 1;
        }
        if ti == nmaybe {
            break;
        }

        let boolResult: bool = directBoolConsistentFn(key);
        recheck |= (*key).recheckCurItem;

        if (curResult != GIN_FALSE) != boolResult {
            curResult = GIN_MAYBE;
            break;
        }
    }

    /* TRUE with recheck is taken to mean MAYBE */
    if curResult == GIN_TRUE && recheck {
        curResult = GIN_MAYBE;
    }

    /* We must restore the original state of the entryRes array */
    for j in 0..nmaybe {
        *(*key).entryRes.add(maybeEntries[j]) = GIN_MAYBE;
    }

    curResult
}

/*
 * Set up the implementation of the consistent functions for a scan key.
 */
pub unsafe fn ginInitConsistentFunction(ginstate: *mut GinState, key: GinScanKey) {
    if (*key).searchMode == GIN_SEARCH_MODE_EVERYTHING {
        (*key).boolConsistentFn = Some(trueConsistentFn);
        (*key).triConsistentFn = Some(trueTriConsistentFn);
    } else {
        let idx = ((*key).attnum - 1) as usize;

        (*key).consistentFmgrInfo = &mut (*ginstate).consistentFn[idx];
        (*key).triConsistentFmgrInfo = &mut (*ginstate).triConsistentFn[idx];
        (*key).collation = (*ginstate).supportCollation[idx];

        if OidIsValid((*ginstate).consistentFn[idx].fn_oid) {
            (*key).boolConsistentFn = Some(directBoolConsistentFn);
        } else {
            (*key).boolConsistentFn = Some(shimBoolConsistentFn);
        }

        if OidIsValid((*ginstate).triConsistentFn[idx].fn_oid) {
            (*key).triConsistentFn = Some(directTriConsistentFn);
        } else {
            (*key).triConsistentFn = Some(shimTriConsistentFn);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::MaybeUninit;

    /*
     * shimTriConsistentFn enumerates the GIN_MAYBE combinations by repeatedly
     * calling directBoolConsistentFn, which invokes the opclass consistent proc
     * through the real fmgr machinery (FunctionCall8Coll).  So our tests must
     * supply a genuine FmgrInfo whose fn_addr points at a fmgr-V1 style proc.
     *
     * directBoolConsistentFn passes, in order:
     *   arg0 = entryRes (*GinTernaryValue), arg1 = strategy, arg2 = query,
     *   arg3 = nuserentries, arg4 = extra_data, arg5 = &recheckCurItem,
     *   arg6 = queryValues, arg7 = queryCategories.
     */

    /* Fake consistent proc: true iff every entryRes[0..nuserentries] is TRUE. */
    unsafe fn fake_all_true_proc(fcinfo: FunctionCallInfo) -> Datum {
        let args = (*fcinfo).args.as_ptr();
        let entry_res = DatumGetPointer((*args.add(0)).value) as *const GinTernaryValue;
        let n = DatumGetUInt32((*args.add(3)).value) as isize;
        /* clear the recheck out-parameter so no recheck is forced */
        let recheck = DatumGetPointer((*args.add(5)).value) as *mut bool;
        *recheck = false;
        (*fcinfo).isnull = false;

        let mut all_true = true;
        let mut i = 0isize;
        while i < n {
            if *entry_res.offset(i) != GIN_TRUE {
                all_true = false;
                break;
            }
            i += 1;
        }
        BoolGetDatum(all_true)
    }

    /* Fake consistent proc: constant TRUE, no recheck. */
    unsafe fn fake_const_true_proc(fcinfo: FunctionCallInfo) -> Datum {
        let args = (*fcinfo).args.as_ptr();
        let recheck = DatumGetPointer((*args.add(5)).value) as *mut bool;
        *recheck = false;
        (*fcinfo).isnull = false;
        BoolGetDatum(true)
    }

    unsafe fn fake_flinfo(proc: crate::utils::fmgr::PGFunction) -> FmgrInfo {
        let mut fi: FmgrInfo = MaybeUninit::zeroed().assume_init();
        fi.fn_addr = Some(proc);
        fi.fn_oid = 1; /* any valid-looking oid; only used in NULL-result elog */
        fi.fn_nargs = 8;
        fi
    }

    /*
     * One GIN_MAYBE + one GIN_TRUE entry with an all-true consistent proc.
     * Enumerating the MAYBE entry yields FALSE (entries [F,T]) for one combo
     * and TRUE (entries [T,T]) for the other; the differing results collapse
     * to GIN_MAYBE.  entryRes must be restored afterward.
     */
    #[test]
    fn shim_tri_returns_maybe_for_split_combination() {
        unsafe {
            let mut entry_res: [GinTernaryValue; 2] = [GIN_MAYBE, GIN_TRUE];
            let mut flinfo = fake_flinfo(fake_all_true_proc);

            let mut key: MaybeUninit<GinScanKeyData> = MaybeUninit::zeroed();
            let kp = key.as_mut_ptr();
            (*kp).nentries = 2;
            (*kp).nuserentries = 2;
            (*kp).entryRes = entry_res.as_mut_ptr();
            (*kp).consistentFmgrInfo = &mut flinfo;

            assert_eq!(shimTriConsistentFn(kp), GIN_MAYBE);
            assert_eq!(entry_res, [GIN_MAYBE, GIN_TRUE]);
        }
    }

    /*
     * Both entries GIN_MAYBE with a constant-true consistent proc: every combo
     * returns TRUE, so they agree and the result is GIN_TRUE (not MAYBE, since
     * recheck is cleared).  entryRes restored to all-MAYBE.
     */
    #[test]
    fn shim_tri_constant_true_collapses_to_true() {
        unsafe {
            let mut entry_res: [GinTernaryValue; 2] = [GIN_MAYBE, GIN_MAYBE];
            let mut flinfo = fake_flinfo(fake_const_true_proc);

            let mut key: MaybeUninit<GinScanKeyData> = MaybeUninit::zeroed();
            let kp = key.as_mut_ptr();
            (*kp).nentries = 2;
            (*kp).nuserentries = 2;
            (*kp).entryRes = entry_res.as_mut_ptr();
            (*kp).consistentFmgrInfo = &mut flinfo;

            assert_eq!(shimTriConsistentFn(kp), GIN_TRUE);
            assert_eq!(entry_res, [GIN_MAYBE, GIN_MAYBE]);
        }
    }
}

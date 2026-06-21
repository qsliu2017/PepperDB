//! Translation of postgres/src/backend/utils/adt/array_expanded.c
//! (merged with the relevant declarations from postgres/src/include/utils/array.h:
//! the EA_MAGIC constant and the ExpandedArrayHeader / AnyArrayType / ArrayMetaState
//! struct definitions).
//!
//! Basic functions for manipulating "expanded" arrays - the read-write,
//! deconstructed in-memory representation of an array value.
//!
//! #include mapping:
//!   - "postgres.h"          -> crate::prelude::* (Datum, DatumGetPointer, palloc,
//!                              elog!/ereport!/errmsg!, Assert!, c-types, Size).
//!   - "access/tupmacs.h"    -> crate::access::tupmacs (att_addlength_datum,
//!                              att_align_nominal).
//!   - "utils/array.h"       -> crate::utils::array (flat ArrayType + ARR_* accessors,
//!                              ARR_OVERHEAD_*, MaxArraySize) PLUS the struct/const
//!                              declarations merged into THIS file (EA_MAGIC,
//!                              ExpandedArrayHeader, AnyArrayType, ArrayMetaState).
//!   - "utils/expandeddatum.h" -> crate::utils::adt::expandeddatum (ExpandedObjectHeader,
//!                              ExpandedObjectMethods, EOH_init_header, EOHPGetRWDatum,
//!                              DatumGetEOHP, the EOM_*_method fn-pointer typedefs).
//!   - "utils/lsyscache.h"   -> get_typlenbyvalalign: NOT PORTED. Stubbed below;
//!                              only the lsyscache lookup path in expand_array uses it.
//!   - "utils/memutils.h"    -> MemoryContext, AllocSetContextCreate!,
//!                              ALLOCSET_START_SMALL_SIZES, MaxAllocSize,
//!                              AllocSizeIsValid (via crate::prelude / crate::utils::memutils).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/array_expanded.c

use crate::prelude::*;
use crate::c::{int16, int32, Size};
use crate::access::tupmacs::{att_addlength_datum, att_align_nominal};
use crate::utils::array::{
    ArrayType, ARR_DATA_PTR, ARR_DIMS, ARR_ELEMTYPE, ARR_HASNULL, ARR_LBOUND, ARR_NDIM, ARR_SIZE,
    ARR_OVERHEAD_NONULLS, ARR_OVERHEAD_WITHNULLS,
};
use crate::utils::adt::arrayutils::ArrayGetNItems;
use crate::utils::adt::expandeddatum::{
    DatumGetEOHP, EOHPGetRWDatum, EOH_init_header, ExpandedObjectHeader, ExpandedObjectMethods,
};
use crate::utils::fmgr::FmgrInfo;
use crate::utils::memutils::AllocSizeIsValid;
use crate::utils::elog::errcode;
use crate::varatt::{SET_VARSIZE, VARATT_IS_EXTERNAL_EXPANDED, VARATT_IS_EXTERNAL_EXPANDED_RW};
use core::ffi::{c_char, c_int, c_void};

// errcodes.h not yet ported; the errcode() shim only logs. See attmap.rs precedent.
// TODO(pg-port): ERRCODE_PROGRAM_LIMIT_EXCEEDED from utils/errcodes.h.
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

// ----------------------------------------------------------------------------
//   utils/array.h declarations merged in (the expanded-array machinery)
// ----------------------------------------------------------------------------

/// `#define EA_MAGIC 689375833` - ID for debugging crosschecks.
pub const EA_MAGIC: c_int = 689375833;

/// `struct ExpandedArrayHeader` (utils/array.h).
///
/// An expanded array lives within a private memory context and is described by
/// this control structure. It may hold a flat ArrayType (`fvalue`), a
/// deconstructed Datum/isnull representation (`dvalues`/`dnulls`), or both.
/// Field order MUST match the C struct exactly.
#[repr(C)]
pub struct ExpandedArrayHeader {
    /// Standard header for expanded objects.
    pub hdr: ExpandedObjectHeader,

    /// Magic value identifying an expanded array (for debugging only).
    pub ea_magic: c_int,

    /* Dimensionality info (always valid) */
    /// # of dimensions.
    pub ndims: c_int,
    /// array dimensions (points into fvalue's header, or palloc'd).
    pub dims: *mut c_int,
    /// index lower bounds for each dimension.
    pub lbound: *mut c_int,

    /* Element type info (always valid) */
    /// element type OID.
    pub element_type: Oid,
    /// element type length.
    pub typlen: int16,
    /// element type pass-by-value flag.
    pub typbyval: bool,
    /// element type alignment.
    pub typalign: c_char,

    /// array of Datums (deconstructed repr), or NULL.
    pub dvalues: *mut Datum,
    /// array of is-null flags for Datums, or NULL.
    pub dnulls: *mut bool,
    /// allocated length of dvalues/dnulls.
    pub dvalueslen: c_int,
    /// number of valid entries in dvalues/dnulls.
    pub nelems: c_int,

    /// cached flat size, or 0 if unknown.
    pub flat_size: Size,

    /// flat representation if valid, else NULL (must be fully detoasted).
    pub fvalue: *mut ArrayType,
    /// start of fvalue's data area.
    pub fstartptr: *mut c_char,
    /// end+1 of fvalue's data area.
    pub fendptr: *mut c_char,
}

/// `union AnyArrayType` (utils/array.h): used by functions that can handle either
/// a flat varlena array or an expanded array. Don't refer to `flt`; cast to
/// ArrayType. Modeled as a repr(C) union of the two header types.
#[repr(C)]
pub union AnyArrayType {
    pub flt: core::mem::ManuallyDrop<ArrayType>,
    pub xpn: core::mem::ManuallyDrop<ExpandedArrayHeader>,
}

/// `struct ArrayMetaState` (utils/array.h): caches type metadata needed for array
/// manipulation, so callers can avoid repeated catalog lookups across calls.
#[repr(C)]
pub struct ArrayMetaState {
    pub element_type: Oid,
    pub typlen: int16,
    pub typbyval: bool,
    pub typalign: c_char,
    pub typdelim: c_char,
    pub typioparam: Oid,
    pub typiofunc: Oid,
    pub proc: FmgrInfo,
}

// ----------------------------------------------------------------------------
//   STUBBED dependencies (not yet ported)
// ----------------------------------------------------------------------------

/// STUB: `get_typlenbyvalalign` (utils/cache/lsyscache.c) - looks up an element
/// type's typlen/typbyval/typalign from the catalog. Only the lsyscache lookup
/// path in expand_array() (i.e. when no valid metacache is supplied) reaches this.
///
/// TODO(pg-port): translate utils/cache/lsyscache.c::get_typlenbyvalalign.
unsafe fn get_typlenbyvalalign(
    _typid: Oid,
    _typlen: *mut int16,
    _typbyval: *mut bool,
    _typalign: *mut c_char,
) { crate::utils::cache::lsyscache::get_typlenbyvalalign(_typid as _, _typlen as _, _typbyval as _, _typalign as _) }

/// STUB: `deconstruct_array` (utils/adt/arrayfuncs.c) - splits a flat ArrayType
/// into freshly-palloc'd Datum and (optionally) isnull arrays. Used only by
/// deconstruct_expanded_array() on the flat-representation path.
///
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::deconstruct_array.
unsafe fn deconstruct_array(
    _array: *mut ArrayType,
    _elmtype: Oid,
    _elmlen: c_int,
    _elmbyval: bool,
    _elmalign: c_char,
    _elemsp: *mut *mut Datum,
    _nullsp: *mut *mut bool,
    _nelemsp: *mut c_int,
) { crate::utils::adt::arrayfuncs::deconstruct_array(_array as _, _elmtype as _, _elmlen as _, _elmbyval, _elmalign as _, _elemsp as _, _nullsp as _, _nelemsp as _) }

/// STUB: `CopyArrayEls` (utils/adt/arrayfuncs.c) - copies element Datums into the
/// data area of a freshly-built flat array, optionally freeing the source data.
/// Used only by EA_flatten_into() on the deconstructed-source path.
///
/// TODO(pg-port): translate utils/adt/arrayfuncs.c::CopyArrayEls.
unsafe fn CopyArrayEls(
    _array: *mut ArrayType,
    _values: *mut Datum,
    _nulls: *mut bool,
    _nitems: c_int,
    _typlen: c_int,
    _typbyval: bool,
    _typalign: c_char,
    _freedata: bool,
) { crate::utils::adt::arrayfuncs::CopyArrayEls(_array as _, _values as _, _nulls as _, _nitems as _, _typlen as _, _typbyval, _typalign as _, _freedata) }

// ----------------------------------------------------------------------------
//   "Methods" required for an expanded object
// ----------------------------------------------------------------------------

/// EA_methods: the ExpandedObjectMethods vtable for expanded arrays.
static EA_methods: ExpandedObjectMethods = ExpandedObjectMethods {
    get_flat_size: EA_get_flat_size,
    flatten_into: EA_flatten_into,
};

// ----------------------------------------------------------------------------
//   array_expanded.c body
// ----------------------------------------------------------------------------

/// expand_array: convert an array Datum into an expanded array.
///
/// The expanded object will be a child of parentcontext.
///
/// Some callers can provide cache space to avoid repeated lookups of element
/// type data across calls; if so, pass a metacache pointer, making sure that
/// metacache->element_type is initialized to InvalidOid before first call. If
/// no cross-call caching is required, pass NULL for metacache.
#[no_mangle]
pub unsafe fn expand_array(
    arraydatum: Datum,
    parentcontext: MemoryContext,
    mut metacache: *mut ArrayMetaState,
) -> Datum {
    let array: *mut ArrayType;
    let eah: *mut ExpandedArrayHeader;
    let objcxt: MemoryContext;
    let oldcxt: MemoryContext;
    let mut fakecache = core::mem::MaybeUninit::<ArrayMetaState>::uninit();

    /*
     * Allocate private context for expanded object.  We start by assuming that
     * the array won't be very large; but if it does grow a lot, don't constrain
     * aset.c's large-context behavior.
     */
    objcxt = AllocSetContextCreate!(
        parentcontext,
        "expanded array\0".as_ptr() as *const c_char,
        ALLOCSET_START_SMALL_SIZES
    );

    /* Set up expanded array header */
    eah = MemoryContextAlloc(objcxt, core::mem::size_of::<ExpandedArrayHeader>())
        as *mut ExpandedArrayHeader;

    // The prelude's MemoryContext (palloc) and expandeddatum's (mmgr::memnodes)
    // are distinct *mut types pending the palloc->mcxt unification; bridge here.
    EOH_init_header(
        &mut (*eah).hdr,
        &EA_methods,
        objcxt as crate::utils::mmgr::memnodes::MemoryContext,
    );
    (*eah).ea_magic = EA_MAGIC;

    /* If the source is an expanded array, we may be able to optimize */
    if VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(arraydatum) as *const c_char) {
        let oldeah = DatumGetEOHP(arraydatum) as *mut ExpandedArrayHeader;

        Assert!((*oldeah).ea_magic == EA_MAGIC);

        /*
         * Update caller's cache if provided; we don't need it this time, but
         * next call might be for a non-expanded source array.  Furthermore, if
         * the caller didn't provide a cache area, use some local storage to
         * cache anyway, thereby avoiding a catalog lookup in the case where we
         * fall through to the flat-copy code path.
         */
        if metacache.is_null() {
            metacache = fakecache.as_mut_ptr();
        }
        (*metacache).element_type = (*oldeah).element_type;
        (*metacache).typlen = (*oldeah).typlen;
        (*metacache).typbyval = (*oldeah).typbyval;
        (*metacache).typalign = (*oldeah).typalign;

        /*
         * If element type is pass-by-value and we have a Datum-array
         * representation, just copy the source's metadata and Datum/isnull
         * arrays.  The original flat array, if present at all, adds no
         * additional information so we need not copy it.
         */
        if (*oldeah).typbyval && !(*oldeah).dvalues.is_null() {
            copy_byval_expanded_array(eah, oldeah);
            /* return a R/W pointer to the expanded array */
            return EOHPGetRWDatum(&(*eah).hdr);
        }

        /*
         * Otherwise, either we have only a flat representation or the elements
         * are pass-by-reference.  In either case, the best thing seems to be to
         * copy the source as a flat representation and then deconstruct that
         * later if necessary.  So, fall through into the flat-source code path.
         */
    }

    /*
     * Detoast and copy source array into private context, as a flat array.
     */
    oldcxt = MemoryContextSwitchTo(objcxt);
    array = crate::PG_DETOAST_DATUM_COPY!(arraydatum) as *mut ArrayType;
    MemoryContextSwitchTo(oldcxt);

    (*eah).ndims = ARR_NDIM(array);
    /* note these pointers point into the fvalue header! */
    (*eah).dims = ARR_DIMS(array);
    (*eah).lbound = ARR_LBOUND(array);

    /* Save array's element-type data for possible use later */
    (*eah).element_type = ARR_ELEMTYPE(array);
    if !metacache.is_null() && (*metacache).element_type == (*eah).element_type {
        /* We have a valid cache of representational data */
        (*eah).typlen = (*metacache).typlen;
        (*eah).typbyval = (*metacache).typbyval;
        (*eah).typalign = (*metacache).typalign;
    } else {
        /* No, so look it up */
        get_typlenbyvalalign(
            (*eah).element_type,
            &mut (*eah).typlen,
            &mut (*eah).typbyval,
            &mut (*eah).typalign,
        );
        /* Update cache if provided */
        if !metacache.is_null() {
            (*metacache).element_type = (*eah).element_type;
            (*metacache).typlen = (*eah).typlen;
            (*metacache).typbyval = (*eah).typbyval;
            (*metacache).typalign = (*eah).typalign;
        }
    }

    /* we don't make a deconstructed representation now */
    (*eah).dvalues = null_mut();
    (*eah).dnulls = null_mut();
    (*eah).dvalueslen = 0;
    (*eah).nelems = 0;
    (*eah).flat_size = 0;

    /* remember we have a flat representation */
    (*eah).fvalue = array;
    (*eah).fstartptr = ARR_DATA_PTR(array);
    (*eah).fendptr = (array as *mut c_char).add(ARR_SIZE(array) as usize);

    /* return a R/W pointer to the expanded array */
    EOHPGetRWDatum(&(*eah).hdr)
}

/// helper for expand_array(): copy pass-by-value Datum-array representation.
unsafe fn copy_byval_expanded_array(
    eah: *mut ExpandedArrayHeader,
    oldeah: *mut ExpandedArrayHeader,
) {
    // eoh_context is mmgr::memnodes-typed; the prelude MemoryContextAlloc takes
    // the palloc MemoryContext - bridge the two *mut types (see expand_array).
    let objcxt = (*eah).hdr.eoh_context as crate::utils::palloc::MemoryContext;
    let ndims = (*oldeah).ndims;
    let dvalueslen = (*oldeah).dvalueslen;

    /* Copy array dimensionality information */
    (*eah).ndims = ndims;
    /* We can alloc both dimensionality arrays with one palloc */
    (*eah).dims = MemoryContextAlloc(
        objcxt,
        ndims as usize * 2 * core::mem::size_of::<c_int>(),
    ) as *mut c_int;
    (*eah).lbound = (*eah).dims.add(ndims as usize);
    /* .. but don't assume the source's arrays are contiguous */
    memcpy(
        (*eah).dims as *mut c_void,
        (*oldeah).dims as *const c_void,
        ndims as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        (*eah).lbound as *mut c_void,
        (*oldeah).lbound as *const c_void,
        ndims as usize * core::mem::size_of::<c_int>(),
    );

    /* Copy element-type data */
    (*eah).element_type = (*oldeah).element_type;
    (*eah).typlen = (*oldeah).typlen;
    (*eah).typbyval = (*oldeah).typbyval;
    (*eah).typalign = (*oldeah).typalign;

    /* Copy the deconstructed representation */
    (*eah).dvalues = MemoryContextAlloc(
        objcxt,
        dvalueslen as usize * core::mem::size_of::<Datum>(),
    ) as *mut Datum;
    memcpy(
        (*eah).dvalues as *mut c_void,
        (*oldeah).dvalues as *const c_void,
        dvalueslen as usize * core::mem::size_of::<Datum>(),
    );
    if !(*oldeah).dnulls.is_null() {
        (*eah).dnulls = MemoryContextAlloc(
            objcxt,
            dvalueslen as usize * core::mem::size_of::<bool>(),
        ) as *mut bool;
        memcpy(
            (*eah).dnulls as *mut c_void,
            (*oldeah).dnulls as *const c_void,
            dvalueslen as usize * core::mem::size_of::<bool>(),
        );
    } else {
        (*eah).dnulls = null_mut();
    }
    (*eah).dvalueslen = dvalueslen;
    (*eah).nelems = (*oldeah).nelems;
    (*eah).flat_size = (*oldeah).flat_size;

    /* we don't make a flat representation */
    (*eah).fvalue = null_mut();
    (*eah).fstartptr = null_mut();
    (*eah).fendptr = null_mut();
}

/// get_flat_size method for expanded arrays.
unsafe extern "C" fn EA_get_flat_size(eohptr: *mut ExpandedObjectHeader) -> Size {
    let eah = eohptr as *mut ExpandedArrayHeader;
    let nelems: c_int;
    let ndims: c_int;
    let dvalues: *mut Datum;
    let dnulls: *mut bool;
    let mut nbytes: Size;

    Assert!((*eah).ea_magic == EA_MAGIC);

    /* Easy if we have a valid flattened value */
    if !(*eah).fvalue.is_null() {
        return ARR_SIZE((*eah).fvalue) as Size;
    }

    /* If we have a cached size value, believe that */
    if (*eah).flat_size != 0 {
        return (*eah).flat_size;
    }

    /*
     * Compute space needed by examining dvalues/dnulls.  Note that the result
     * array will have a nulls bitmap if dnulls isn't NULL, even if the array
     * doesn't actually contain any nulls now.
     */
    nelems = (*eah).nelems;
    ndims = (*eah).ndims;
    Assert!(nelems == ArrayGetNItems(ndims, (*eah).dims));
    dvalues = (*eah).dvalues;
    dnulls = (*eah).dnulls;
    nbytes = 0;
    for i in 0..nelems as isize {
        if !dnulls.is_null() && *dnulls.offset(i) {
            continue;
        }
        nbytes = att_addlength_datum(nbytes, (*eah).typlen as c_int, *dvalues.offset(i));
        nbytes = att_align_nominal(nbytes, (*eah).typalign);
        /* check for overflow of total request */
        if !AllocSizeIsValid(nbytes) {
            let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
            ereport!(
                ERROR,
                errmsg!(
                    "array size exceeds the maximum allowed ({})",
                    crate::utils::memutils::MaxAllocSize as c_int
                )
            );
        }
    }

    if !dnulls.is_null() {
        nbytes += ARR_OVERHEAD_WITHNULLS(ndims, nelems);
    } else {
        nbytes += ARR_OVERHEAD_NONULLS(ndims);
    }

    /* cache for next time */
    (*eah).flat_size = nbytes;

    nbytes
}

/// flatten_into method for expanded arrays.
unsafe extern "C" fn EA_flatten_into(
    eohptr: *mut ExpandedObjectHeader,
    result: *mut c_void,
    allocated_size: Size,
) {
    let eah = eohptr as *mut ExpandedArrayHeader;
    let aresult = result as *mut ArrayType;
    let nelems: c_int;
    let ndims: c_int;
    let dataoffset: int32;

    Assert!((*eah).ea_magic == EA_MAGIC);

    /* Easy if we have a valid flattened value */
    if !(*eah).fvalue.is_null() {
        Assert!(allocated_size == ARR_SIZE((*eah).fvalue) as Size);
        memcpy(result, (*eah).fvalue as *const c_void, allocated_size);
        return;
    }

    /* Else allocation should match previous get_flat_size result */
    Assert!(allocated_size == (*eah).flat_size);

    /* Fill result array from dvalues/dnulls */
    nelems = (*eah).nelems;
    ndims = (*eah).ndims;

    if !(*eah).dnulls.is_null() {
        dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nelems) as int32;
    } else {
        dataoffset = 0; /* marker for no null bitmap */
    }

    /* We must ensure that any pad space is zero-filled */
    memset(aresult as *mut c_void, 0, allocated_size);

    SET_VARSIZE(aresult as *mut c_char, allocated_size as int32);
    (*aresult).ndim = ndims;
    (*aresult).dataoffset = dataoffset;
    (*aresult).elemtype = (*eah).element_type;
    memcpy(
        ARR_DIMS(aresult) as *mut c_void,
        (*eah).dims as *const c_void,
        ndims as usize * core::mem::size_of::<c_int>(),
    );
    memcpy(
        ARR_LBOUND(aresult) as *mut c_void,
        (*eah).lbound as *const c_void,
        ndims as usize * core::mem::size_of::<c_int>(),
    );

    CopyArrayEls(
        aresult,
        (*eah).dvalues,
        (*eah).dnulls,
        nelems,
        (*eah).typlen as c_int,
        (*eah).typbyval,
        (*eah).typalign,
        false,
    );
}

/*
 * Argument fetching support code
 */

/// DatumGetExpandedArray: get a writable expanded array from an input argument.
///
/// Caution: if the input is a read/write pointer, this returns the input
/// argument; so callers must be sure that their changes are "safe", that is they
/// cannot leave the array in a corrupt state.
pub unsafe fn DatumGetExpandedArray(mut d: Datum) -> *mut ExpandedArrayHeader {
    /* If it's a writable expanded array already, just return it */
    if VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d) as *const c_char) {
        let eah = DatumGetEOHP(d) as *mut ExpandedArrayHeader;

        Assert!((*eah).ea_magic == EA_MAGIC);
        return eah;
    }

    /* Else expand the hard way */
    d = expand_array(d, CurrentMemoryContext, null_mut());
    DatumGetEOHP(d) as *mut ExpandedArrayHeader
}

/// As above, when caller has the ability to cache element type info.
pub unsafe fn DatumGetExpandedArrayX(
    mut d: Datum,
    metacache: *mut ArrayMetaState,
) -> *mut ExpandedArrayHeader {
    /* If it's a writable expanded array already, just return it */
    if VARATT_IS_EXTERNAL_EXPANDED_RW(DatumGetPointer(d) as *const c_char) {
        let eah = DatumGetEOHP(d) as *mut ExpandedArrayHeader;

        Assert!((*eah).ea_magic == EA_MAGIC);
        /* Update cache if provided */
        if !metacache.is_null() {
            (*metacache).element_type = (*eah).element_type;
            (*metacache).typlen = (*eah).typlen;
            (*metacache).typbyval = (*eah).typbyval;
            (*metacache).typalign = (*eah).typalign;
        }
        return eah;
    }

    /* Else expand using caller's cache if any */
    d = expand_array(d, CurrentMemoryContext, metacache);
    DatumGetEOHP(d) as *mut ExpandedArrayHeader
}

/// DatumGetAnyArrayP: return either an expanded array or a detoasted varlena
/// array.  The result must not be modified in-place.
pub unsafe fn DatumGetAnyArrayP(d: Datum) -> *mut AnyArrayType {
    let eah: *mut ExpandedArrayHeader;

    /*
     * If it's an expanded array (RW or RO), return the header pointer.
     */
    if VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(d) as *const c_char) {
        eah = DatumGetEOHP(d) as *mut ExpandedArrayHeader;
        Assert!((*eah).ea_magic == EA_MAGIC);
        return eah as *mut AnyArrayType;
    }

    /* Else do regular detoasting as needed */
    crate::PG_DETOAST_DATUM!(d) as *mut AnyArrayType
}

/// Create the Datum/isnull representation of an expanded array object if we
/// didn't do so previously.
pub unsafe fn deconstruct_expanded_array(eah: *mut ExpandedArrayHeader) {
    if (*eah).dvalues.is_null() {
        let oldcxt =
            MemoryContextSwitchTo((*eah).hdr.eoh_context as crate::utils::palloc::MemoryContext);
        let mut dvalues: *mut Datum = null_mut();
        let mut dnulls: *mut bool = null_mut();
        let mut nelems: c_int = 0;

        deconstruct_array(
            (*eah).fvalue,
            (*eah).element_type,
            (*eah).typlen as c_int,
            (*eah).typbyval,
            (*eah).typalign,
            &mut dvalues,
            if ARR_HASNULL((*eah).fvalue) {
                &mut dnulls
            } else {
                null_mut()
            },
            &mut nelems,
        );

        /*
         * Update header only after successful completion of this step.  If
         * deconstruct_array fails partway through, worst consequence is some
         * leaked memory in the object's context.
         */
        (*eah).dvalues = dvalues;
        (*eah).dnulls = dnulls;
        (*eah).nelems = nelems;
        (*eah).dvalueslen = nelems;
        MemoryContextSwitchTo(oldcxt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build an ExpandedArrayHeader by hand for a small pass-by-value int4[]
    // (typlen=4, byval=true, align='i') with two non-null elements, and verify
    // EA_get_flat_size round-trips: it must equal the no-nulls header overhead
    // plus 2 * 4 bytes (int4, INTALIGN keeps it at 4 each).
    #[test]
    fn ea_get_flat_size_byval_int4() {
        unsafe {
            // Allocate the header (palloc0 so private/padding fields are zeroed).
            let eah = palloc0(core::mem::size_of::<ExpandedArrayHeader>())
                as *mut ExpandedArrayHeader;

            // Minimal header init: we only need ea_magic + the fields read by
            // EA_get_flat_size on the deconstructed path. fvalue/flat_size are 0.
            (*eah).ea_magic = EA_MAGIC;
            (*eah).ndims = 1;

            // dims = [2], lbound = [1].
            let mut dims: [c_int; 1] = [2];
            let mut lbound: [c_int; 1] = [1];
            (*eah).dims = dims.as_mut_ptr();
            (*eah).lbound = lbound.as_mut_ptr();

            (*eah).element_type = 23; // INT4OID
            (*eah).typlen = 4;
            (*eah).typbyval = true;
            (*eah).typalign = b'i' as c_char; // TYPALIGN_INT

            // Two Datums; values irrelevant for byval length accounting.
            let mut dvalues: [Datum; 2] = [10, 20];
            (*eah).dvalues = dvalues.as_mut_ptr();
            (*eah).dnulls = null_mut();
            (*eah).dvalueslen = 2;
            (*eah).nelems = 2;
            (*eah).flat_size = 0;
            (*eah).fvalue = null_mut();

            let got = EA_get_flat_size(&mut (*eah).hdr as *mut ExpandedObjectHeader);

            // Expected: 2 elements * (align(4)=4) = 8 data bytes, plus the
            // no-nulls header overhead for ndims=1.
            let expected = ARR_OVERHEAD_NONULLS(1) + 8;
            assert_eq!(got, expected as Size);

            // Second call must hit the cached flat_size and return the same.
            assert_eq!((*eah).flat_size, expected as Size);
            let got2 = EA_get_flat_size(&mut (*eah).hdr as *mut ExpandedObjectHeader);
            assert_eq!(got2, expected as Size);
        }
    }
}

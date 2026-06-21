//! Translation of postgres/src/backend/access/common/heaptuple.c
//!
//! Heap tuple accessor and mutator routines: the form/deform workhorse that is
//! the executor's gateway to tuple data.  heap_form_tuple / heap_deform_tuple /
//! nocachegetattr / heap_fill_tuple / the MinimalTuple <-> HeapTuple conversions
//! all live here.
//!
//! Byte-level correctness matters: the offsets these routines compute must match
//! the on-disk layout and what fastgetattr/heap_getattr in htup_details.rs expect.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping:
//!   postgres.h                  -> crate::prelude
//!   access/heaptoast.h          -> STUB: only toast_flatten_tuple_to_datum is
//!                                  referenced (by heap_copy_tuple_as_datum's
//!                                  HasExternal branch); heaptoast.c not ported, so
//!                                  that one branch is stubbed.
//!   access/sysattr.h            -> the *AttributeNumber consts defined locally
//!                                  below (sysattr.h not ported).
//!   access/tupdesc_details.h    -> AttrMissing via crate::access::common::tupdesc.
//!   common/hashfn.h             -> crate::common::hashfn (hash_bytes).
//!   utils/datum.h               -> crate::utils::adt::datum (datumCopy/datumGetSize).
//!   utils/expandeddatum.h       -> crate::utils::adt::expandeddatum.
//!   utils/hsearch.h             -> crate::utils::hash::dynahash (HTAB/HASHCTL/...).
//!   utils/memutils.h            -> crate::prelude (TopMemoryContext / MemoryContextSwitchTo).
//!   access/htup_details.h       -> crate::access::htup_details (headers + accessors).
//!   access/tupmacs.h            -> crate::access::tupmacs (att_* / fetch/store helpers).
//!   varatt.h                    -> crate::varatt; the four short/external macros that
//!                                  varatt.rs does not yet export (VARATT_CAN_MAKE_SHORT,
//!                                  VARATT_CONVERTED_SHORT_SIZE, VARSIZE_SHORT,
//!                                  VARSIZE_EXTERNAL) are defined locally below.
//!
//! WHAT IS REAL vs STUBBED:
//!   REAL: heap_compute_data_size, fill_val, heap_fill_tuple, heap_attisnull,
//!     nocachegetattr, heap_getsysattr (incl. cmin/cmax via GetRawCommandId - upstream
//!     does NOT resolve combocid here), heap_copytuple, heap_copytuple_with_tuple,
//!     heap_form_tuple, heap_modify_tuple, heap_modify_tuple_by_cols, heap_deform_tuple,
//!     heap_freetuple, heap_form_minimal_tuple, heap_free_minimal_tuple,
//!     heap_copy_minimal_tuple, heap_tuple_from_minimal_tuple,
//!     minimal_tuple_from_heap_tuple, varsize_any, the missing-attr cache
//!     (missing_hash/missing_match/init_missing_cache/getmissingattr - uses real dynahash),
//!     expand_tuple/heap_expand_tuple/minimal_expand_tuple (the missing-value + expanded-
//!     object-flatten paths are real).
//!   STUBBED (one branch each, signature real):
//!     - heap_copy_tuple_as_datum: the HeapTupleHasExternal branch calls
//!       toast_flatten_tuple_to_datum (access/heaptoast.c, not ported).
//!     - fill_val / heap_compute_data_size: the VARATT_IS_EXTERNAL non-expanded branch
//!       uses VARSIZE_EXTERNAL, which needs VARTAG_SIZE (external-TOAST sizing, not yet
//!       in varatt.rs); that single branch is stubbed.  The byval/byref/short-varlena/
//!       expanded-object paths are all real.
//!
//! INTEGRATOR NOTE: htup_details.rs currently has PLACEHOLDER `pub` stubs for
//! `nocachegetattr` and `heap_getsysattr` (and `getmissingattr`).  The REAL ones are
//! defined here.  fastgetattr's slow path in htup_details.rs should be repointed to
//! crate::access::common::heaptuple::nocachegetattr (and heap_getattr to the
//! heap_getsysattr / getmissingattr here).  Do NOT `use` those two names from
//! htup_details (name clash); this module defines them itself.

use crate::prelude::*;

use crate::access::htup_details::{
    BITMAPLEN, HeapTuple, HeapTupleData, HeapTupleHeader, HeapTupleHeaderData,
    HeapTupleHeaderGetNatts, HeapTupleHeaderGetRawCommandId, HeapTupleHeaderGetRawXmax,
    HeapTupleHeaderGetRawXmin, HeapTupleHeaderSetDatumLength, HeapTupleHeaderSetNatts,
    HeapTupleHeaderSetTypMod, HeapTupleHeaderSetTypeId, HeapTupleHasExternal, HeapTupleHasNulls,
    HeapTupleHasVarWidth, HeapTupleIsValid, HeapTupleNoNulls, MaxTupleAttributeNumber,
    MinimalTuple, MinimalTupleData, HEAPTUPLESIZE, HEAP_HASEXTERNAL, HEAP_HASNULL,
    HEAP_HASVARWIDTH, HEAP_NATTS_MASK, MINIMAL_TUPLE_OFFSET, SizeofMinimalTupleHeader,
};
use crate::access::tupmacs::{
    att_addlength_datum, att_addlength_pointer, att_datum_alignby, att_isnull, att_nominal_alignby,
    att_pointer_alignby, fetch_att, store_att_byval,
};
use crate::access::common::tupdesc::{
    AttrMissing, CompactAttribute, TupleDesc, TupleDescCompactAttr,
};
use crate::common::hashfn::hash_bytes;
use crate::storage::itemptr::ItemPointerSetInvalid;
use crate::utils::adt::datum::datumCopy;
use crate::utils::adt::expandeddatum::{DatumGetEOHP, EOH_flatten_into, EOH_get_flat_size};
use crate::utils::hash::dynahash::{
    hash_create, hash_search, HASHCTL, HASH_COMPARE, HASH_CONTEXT, HASH_ELEM, HASH_ENTER,
    HASH_FUNCTION, HTAB,
};
use crate::varatt::{
    SET_VARSIZE_SHORT, VARDATA, VARSIZE, VARSIZE_1B, VARSIZE_ANY, VARATT_IS_EXTERNAL,
    VARATT_IS_EXTERNAL_EXPANDED, VARATT_IS_SHORT, VARHDRSZ_SHORT,
};

use core::ffi::{c_char, c_int, c_void};
use core::mem::{offset_of, size_of};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn strlen(s: *const c_char) -> usize;
}

// ----------------------------------------------------------------------------
//   access/sysattr.h: system attribute numbers (sysattr.h not yet ported).
// ----------------------------------------------------------------------------

pub const SelfItemPointerAttributeNumber: c_int = -1;
pub const MinTransactionIdAttributeNumber: c_int = -2;
pub const MinCommandIdAttributeNumber: c_int = -3;
pub const MaxTransactionIdAttributeNumber: c_int = -4;
pub const MaxCommandIdAttributeNumber: c_int = -5;
pub const TableOidAttributeNumber: c_int = -6;

// ----------------------------------------------------------------------------
//   varatt.h macros not yet exported by varatt.rs.
//
//   VARATT_SHORT_MAX, VARATT_CAN_MAKE_SHORT, VARATT_CONVERTED_SHORT_SIZE,
//   VARSIZE_SHORT.  (VARSIZE_EXTERNAL needs VARTAG_SIZE which is part of the
//   not-yet-ported external-TOAST sizing; the single branch that needs it is
//   stubbed - see fill_val.)
// ----------------------------------------------------------------------------

/*
 * ERRCODE_TOO_MANY_COLUMNS (errcodes.h, not yet ported).  The errcode() shim
 * ignores the value, so any placeholder is fine; kept named for fidelity.
 */
const ERRCODE_TOO_MANY_COLUMNS: c_int = 0;

const VARATT_SHORT_MAX: uint32 = 0x7F;

/*
 * VARATT_CAN_MAKE_SHORT(PTR): true if a 4-byte-header uncompressed varlena is
 * small enough to be re-encoded into the 1-byte short-header form.
 *
 * # Safety
 * `ptr` points to a valid varlena datum.
 */
#[inline]
unsafe fn VARATT_CAN_MAKE_SHORT(ptr: *const c_char) -> bool {
    crate::varatt::VARATT_IS_4B_U(ptr)
        && (VARSIZE(ptr) - VARHDRSZ as uint32 + VARHDRSZ_SHORT as uint32) <= VARATT_SHORT_MAX
}

/*
 * VARATT_CONVERTED_SHORT_SIZE(PTR): the size of the short-header form that a
 * 4-byte-header varlena would convert to.
 *
 * # Safety
 * `ptr` points to a valid 4B uncompressed varlena datum.
 */
#[inline]
unsafe fn VARATT_CONVERTED_SHORT_SIZE(ptr: *const c_char) -> uint32 {
    VARSIZE(ptr) - VARHDRSZ as uint32 + VARHDRSZ_SHORT as uint32
}

/*
 * VARSIZE_SHORT(PTR): total size (incl. 1-byte header) of a short varlena.
 *
 * # Safety
 * `ptr` points to a valid short (1-byte-header) varlena.
 */
#[inline]
unsafe fn VARSIZE_SHORT(ptr: *const c_char) -> uint32 {
    VARSIZE_1B(ptr)
}

// ----------------------------------------------------------------------------
//   ATT_IS_PACKABLE family (local macros from heaptuple.c).
// ----------------------------------------------------------------------------

/*
 * COMPACT_ATTR_IS_PACKABLE(att): attlen == -1 && att->attispackable.
 * (The CompactAttribute equivalent of the FormData ATT_IS_PACKABLE macro.)
 *
 * # Safety
 * `att` references a live CompactAttribute.
 */
#[inline]
unsafe fn COMPACT_ATTR_IS_PACKABLE(att: *const CompactAttribute) -> bool {
    (*att).attlen == -1 && (*att).attispackable
}

// ============================================================================
//   Setup for caching pass-by-ref missing attributes in a way that survives
//   tupleDesc destruction.
// ============================================================================

#[repr(C)]
#[derive(Clone, Copy)]
struct missing_cache_key {
    len: c_int,
    value: Datum,
}

static mut missing_cache: *mut HTAB = null_mut();

/*
 * missing_hash - dynahash HashValueFunc over the (len-byte) datum payload.
 *
 * # Safety
 * `key` points to a missing_cache_key whose value covers `len` readable bytes.
 */
unsafe extern "C" fn missing_hash(key: *const c_void, _keysize: Size) -> uint32 {
    let entry = key as *const missing_cache_key;

    hash_bytes(
        DatumGetPointer((*entry).value) as *const core::ffi::c_uchar,
        (*entry).len,
    )
}

/*
 * missing_match - dynahash HashCompareFunc.
 *
 * # Safety
 * `key1`/`key2` point to missing_cache_keys whose values cover `len` bytes.
 */
unsafe extern "C" fn missing_match(
    key1: *const c_void,
    key2: *const c_void,
    _keysize: Size,
) -> c_int {
    let entry1 = key1 as *const missing_cache_key;
    let entry2 = key2 as *const missing_cache_key;

    if (*entry1).len != (*entry2).len {
        return if (*entry1).len > (*entry2).len { 1 } else { -1 };
    }

    memcmp(
        DatumGetPointer((*entry1).value) as *const c_void,
        DatumGetPointer((*entry2).value) as *const c_void,
        (*entry1).len as usize,
    )
}

unsafe fn init_missing_cache() {
    let mut hash_ctl: HASHCTL = core::mem::zeroed();

    hash_ctl.keysize = size_of::<missing_cache_key>();
    hash_ctl.entrysize = size_of::<missing_cache_key>();
    hash_ctl.hcxt = TopMemoryContext;
    hash_ctl.hash = Some(missing_hash);
    hash_ctl.r#match = Some(missing_match);
    missing_cache = hash_create(
        c"Missing Values Cache".as_ptr(),
        32,
        &hash_ctl,
        HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE,
    );
}

/* memcmp is needed by missing_match. */
extern "C" {
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

/* ----------------------------------------------------------------
 *						misc support routines
 * ----------------------------------------------------------------
 */

/*
 * Return the missing value of an attribute, or NULL if there isn't one.
 *
 * # Safety
 * `tupleDesc` is live, `attnum` in 1..=natts, `isnull` writable.
 */
pub unsafe fn getmissingattr(tupleDesc: TupleDesc, attnum: c_int, isnull: *mut bool) -> Datum {
    Assert!(attnum <= (*tupleDesc).natts);
    Assert!(attnum > 0);

    let att = TupleDescCompactAttr(tupleDesc, attnum - 1);

    if (*att).atthasmissing {
        Assert!(!(*tupleDesc).constr.is_null());
        Assert!(!(*(*tupleDesc).constr).missing.is_null());

        let attrmiss = (*(*tupleDesc).constr).missing.add((attnum - 1) as usize);

        if (*attrmiss).am_present {
            *isnull = false;

            /* no need to cache by-value attributes */
            if (*att).attbyval {
                return (*attrmiss).am_value;
            }

            /* set up cache if required */
            if missing_cache.is_null() {
                init_missing_cache();
            }

            /* check if there's a cache entry */
            Assert!((*att).attlen > 0 || (*att).attlen == -1);

            let mut key: missing_cache_key = core::mem::zeroed();
            if (*att).attlen > 0 {
                key.len = (*att).attlen as c_int;
            } else {
                key.len = VARSIZE_ANY(DatumGetPointer((*attrmiss).am_value)) as c_int;
            }
            key.value = (*attrmiss).am_value;

            let mut found: bool = false;
            let entry = hash_search(
                missing_cache,
                &key as *const missing_cache_key as *const c_void,
                HASH_ENTER,
                &mut found,
            ) as *mut missing_cache_key;

            if !found {
                /* cache miss, so we need a non-transient copy of the datum */
                let oldctx = MemoryContextSwitchTo(TopMemoryContext);
                (*entry).value = datumCopy((*attrmiss).am_value, false, (*att).attlen as c_int);
                MemoryContextSwitchTo(oldctx);
            }

            return (*entry).value;
        }
    }

    *isnull = true;
    PointerGetDatum(null())
}

/*
 * heap_compute_data_size
 *		Determine size of the data area of a tuple to be constructed
 *
 * # Safety
 * `tupleDesc` is live; `values`/`isnull` point to natts elements.
 */
pub unsafe fn heap_compute_data_size(
    tupleDesc: TupleDesc,
    values: *const Datum,
    isnull: *const bool,
) -> Size {
    let mut data_length: Size = 0;
    let numberOfAttributes = (*tupleDesc).natts;

    for i in 0..numberOfAttributes {
        if *isnull.add(i as usize) {
            continue;
        }

        let val = *values.add(i as usize);
        let atti = TupleDescCompactAttr(tupleDesc, i);

        if COMPACT_ATTR_IS_PACKABLE(atti)
            && VARATT_CAN_MAKE_SHORT(DatumGetPointer(val) as *const c_char)
        {
            /*
             * we're anticipating converting to a short varlena header, so
             * adjust length and don't count any alignment
             */
            data_length +=
                VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val) as *const c_char) as Size;
        } else if (*atti).attlen == -1
            && VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val) as *const c_char)
        {
            /*
             * we want to flatten the expanded value so that the constructed
             * tuple doesn't depend on it
             */
            data_length = att_nominal_alignby(data_length, (*atti).attalignby);
            data_length += EOH_get_flat_size(DatumGetEOHP(val));
        } else {
            if std::env::var("PDB_BT").is_ok() && (*atti).attlen < 0 {
                eprintln!("PDB_BT heap_compute_data_size i={} natts={} attlen={} val={:p} isnull={}", i, numberOfAttributes, (*atti).attlen, DatumGetPointer(val), *isnull.add(i as usize));
            }
            data_length = att_datum_alignby(
                data_length,
                (*atti).attalignby,
                (*atti).attlen as c_int,
                val,
            );
            data_length = att_addlength_datum(data_length, (*atti).attlen as c_int, val);
        }
    }

    data_length
}

/*
 * Per-attribute helper for heap_fill_tuple and other routines building tuples.
 *
 * Fill in either a data value or a bit in the null bitmask.
 *
 * `bit` is None when no bitmap is being built (matches the C `bits8 **bit` ==
 * NULL test); otherwise it is `Some(&mut bitP)` where `bitP` is the running
 * `bits8 *`.
 *
 * # Safety
 * Pointers must be valid for the data area / bitmap being written; `datum` is
 * consistent with `att`.
 */
#[inline]
unsafe fn fill_val(
    att: *const CompactAttribute,
    bit: Option<&mut *mut bits8>,
    bitmask: *mut c_int,
    dataP: *mut *mut c_char,
    infomask: *mut uint16,
    datum: Datum,
    isnull: bool,
) {
    let data_length: Size;
    let mut data = *dataP;

    /*
     * If we're building a null bitmap, set the appropriate bit for the
     * current column value here.
     */
    if let Some(bitp) = bit {
        if *bitmask != HIGHBIT as c_int {
            *bitmask <<= 1;
        } else {
            *bitp = (*bitp).add(1);
            **bitp = 0x0;
            *bitmask = 1;
        }

        if isnull {
            *infomask |= HEAP_HASNULL;
            return;
        }

        **bitp |= *bitmask as bits8;
    }

    /*
     * XXX we use the att_nominal_alignby macro on the pointer value itself,
     * not on an offset.  This is a bit of a hack.
     */
    if (*att).attbyval {
        /* pass-by-value */
        data = att_nominal_alignby(data as usize, (*att).attalignby) as *mut c_char;
        store_att_byval(data as *mut c_void, datum, (*att).attlen as c_int);
        data_length = (*att).attlen as Size;
    } else if (*att).attlen == -1 {
        /* varlena */
        let val = DatumGetPointer(datum);

        *infomask |= HEAP_HASVARWIDTH;
        if VARATT_IS_EXTERNAL(val as *const c_char) {
            if VARATT_IS_EXTERNAL_EXPANDED(val as *const c_char) {
                /*
                 * we want to flatten the expanded value so that the
                 * constructed tuple doesn't depend on it
                 */
                let eoh = DatumGetEOHP(datum);

                data = att_nominal_alignby(data as usize, (*att).attalignby) as *mut c_char;
                data_length = EOH_get_flat_size(eoh);
                EOH_flatten_into(eoh, data as *mut c_void, data_length);
            } else {
                *infomask |= HEAP_HASEXTERNAL;
                /* no alignment, since it's short by definition */
                // TODO(pg-port): needs varatt.rs VARSIZE_EXTERNAL / VARTAG_SIZE
                // (external-TOAST sizing not yet ported).  C body:
                //   data_length = VARSIZE_EXTERNAL(val);
                //   memcpy(data, val, data_length);
                unimplemented!(
                    "fill_val: external (non-expanded) TOAST pointer needs VARSIZE_EXTERNAL"
                );
            }
        } else if VARATT_IS_SHORT(val as *const c_char) {
            /* no alignment for short varlenas */
            data_length = VARSIZE_SHORT(val as *const c_char) as Size;
            memcpy(data as *mut c_void, val as *const c_void, data_length);
        } else if (*att).attispackable && VARATT_CAN_MAKE_SHORT(val as *const c_char) {
            /* convert to short varlena -- no alignment */
            data_length = VARATT_CONVERTED_SHORT_SIZE(val as *const c_char) as Size;
            SET_VARSIZE_SHORT(data, data_length as int32);
            memcpy(
                data.add(1) as *mut c_void,
                VARDATA(val as *const c_char) as *const c_void,
                data_length - 1,
            );
        } else {
            /* full 4-byte header varlena */
            data = att_nominal_alignby(data as usize, (*att).attalignby) as *mut c_char;
            data_length = VARSIZE(val as *const c_char) as Size;
            memcpy(data as *mut c_void, val as *const c_void, data_length);
        }
    } else if (*att).attlen == -2 {
        /* cstring ... never needs alignment */
        *infomask |= HEAP_HASVARWIDTH;
        Assert!((*att).attalignby as usize == size_of::<c_char>());
        data_length = strlen(DatumGetCString(datum)) + 1;
        memcpy(
            data as *mut c_void,
            DatumGetPointer(datum) as *const c_void,
            data_length,
        );
    } else {
        /* fixed-length pass-by-reference */
        data = att_nominal_alignby(data as usize, (*att).attalignby) as *mut c_char;
        Assert!((*att).attlen > 0);
        data_length = (*att).attlen as Size;
        memcpy(
            data as *mut c_void,
            DatumGetPointer(datum) as *const c_void,
            data_length,
        );
    }

    data = data.add(data_length);
    *dataP = data;
}

/*
 * heap_fill_tuple
 *		Load data portion of a tuple from values/isnull arrays
 *
 * We also fill the null bitmap (if any) and set the infomask bits
 * that reflect the tuple's data contents.
 *
 * NOTE: it is now REQUIRED that the caller have pre-zeroed the data area.
 *
 * # Safety
 * `tupleDesc` is live; `data` has room for `data_size` bytes; `bit`, if non-null,
 * has room for the null bitmap.
 */
pub unsafe fn heap_fill_tuple(
    tupleDesc: TupleDesc,
    values: *const Datum,
    isnull: *const bool,
    data: *mut c_char,
    data_size: Size,
    infomask: *mut uint16,
    bit: *mut bits8,
) {
    let mut bitP: *mut bits8;
    let mut bitmask: c_int;
    let numberOfAttributes = (*tupleDesc).natts;

    #[cfg(debug_assertions)]
    let start = data;

    let mut data = data;

    if !bit.is_null() {
        /* bitP = &bit[-1]; */
        bitP = bit.offset(-1);
        bitmask = HIGHBIT as c_int;
    } else {
        /* just to keep compiler quiet */
        bitP = null_mut();
        bitmask = 0;
    }

    *infomask &= !(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL);

    for i in 0..numberOfAttributes {
        let attr = TupleDescCompactAttr(tupleDesc, i);

        let bit_arg: Option<&mut *mut bits8> = if !bitP.is_null() {
            Some(&mut bitP)
        } else {
            None
        };

        fill_val(
            attr,
            bit_arg,
            &mut bitmask,
            &mut data,
            infomask,
            if !values.is_null() {
                *values.add(i as usize)
            } else {
                PointerGetDatum(null())
            },
            if !isnull.is_null() {
                *isnull.add(i as usize)
            } else {
                true
            },
        );
    }

    #[cfg(debug_assertions)]
    Assert!((data as usize - start as usize) == data_size);
    let _ = data_size;
}

/* ----------------------------------------------------------------
 *						heap tuple interface
 * ----------------------------------------------------------------
 */

/* ----------------
 *		heap_attisnull	- returns true iff tuple attribute is not present
 * ----------------
 *
 * # Safety
 * `tup` is a valid HeapTuple; `tupleDesc` may be null or live.
 */
pub unsafe fn heap_attisnull(tup: HeapTuple, attnum: c_int, tupleDesc: TupleDesc) -> bool {
    /*
     * We allow a NULL tupledesc for relations not expected to have missing
     * values, such as catalog relations and indexes.
     */
    Assert!(tupleDesc.is_null() || attnum <= (*tupleDesc).natts);
    if attnum > HeapTupleHeaderGetNatts((*tup).t_data) as c_int {
        if !tupleDesc.is_null()
            && (*TupleDescCompactAttr(tupleDesc, attnum - 1)).atthasmissing
        {
            return false;
        } else {
            return true;
        }
    }

    if attnum > 0 {
        if HeapTupleNoNulls(tup) {
            return false;
        }
        return att_isnull(attnum - 1, (*(*tup).t_data).t_bits.as_ptr());
    }

    match attnum {
        TableOidAttributeNumber
        | SelfItemPointerAttributeNumber
        | MinTransactionIdAttributeNumber
        | MinCommandIdAttributeNumber
        | MaxTransactionIdAttributeNumber
        | MaxCommandIdAttributeNumber => {
            /* these are never null */
        }
        _ => {
            elog!(ERROR, "invalid attnum: {}", attnum);
        }
    }

    false
}

/* ----------------
 *		nocachegetattr
 *
 *		This only gets called from fastgetattr(), in cases where we
 *		can't use a cacheoffset and the value is not null.
 *
 *		This caches attribute offsets in the attribute descriptor.
 *
 *		NOTE: if you need to change this code, see also heap_deform_tuple.
 *		Also see nocache_index_getattr, which is the same code for index
 *		tuples.
 * ----------------
 *
 * # Safety
 * `tup` is a valid HeapTuple; `tupleDesc` is live and matches it; `attnum`
 * (1-based) is a non-null user attribute present in the tuple.
 */
pub unsafe fn nocachegetattr(tup: HeapTuple, attnum: c_int, tupleDesc: TupleDesc) -> Datum {
    let td = (*tup).t_data;
    let tp: *mut c_char; /* ptr to data part of tuple */
    let bp = (*td).t_bits.as_ptr(); /* ptr to null bitmap in tuple */
    let mut slow = false; /* do we have to walk attrs? */
    let mut off: c_int; /* current offset within data */

    /* ----------------
     *	 Three cases:
     *
     *	 1: No nulls and no variable-width attributes.
     *	 2: Has a null or a var-width AFTER att.
     *	 3: Has nulls or var-widths BEFORE att.
     * ----------------
     */

    let attnum = attnum - 1;

    if !HeapTupleNoNulls(tup) {
        /*
         * there's a null somewhere in the tuple
         *
         * check to see if any preceding bits are null...
         */
        let byte = attnum >> 3;
        let finalbit = attnum & 0x07;

        /* check for nulls "before" final bit of last byte */
        if ((!*bp.add(byte as usize)) & (((1 << finalbit) - 1) as bits8)) != 0 {
            slow = true;
        } else {
            /* check for nulls in any "earlier" bytes */
            for i in 0..byte {
                if *bp.add(i as usize) != 0xFF {
                    slow = true;
                    break;
                }
            }
        }
    }

    tp = (td as *mut c_char).add((*td).t_hoff as usize);

    if !slow {
        /*
         * If we get here, there are no nulls up to and including the target
         * attribute.  If we have a cached offset, we can use it.
         */
        let att = TupleDescCompactAttr(tupleDesc, attnum);
        if (*att).attcacheoff >= 0 {
            return fetchatt(att, tp.add((*att).attcacheoff as usize));
        }

        /*
         * Otherwise, check for non-fixed-length attrs up to and including
         * target.  If there aren't any, it's safe to cheaply initialize the
         * cached offsets for these attrs.
         */
        if HeapTupleHasVarWidth(tup) {
            for j in 0..=attnum {
                if (*TupleDescCompactAttr(tupleDesc, j)).attlen <= 0 {
                    slow = true;
                    break;
                }
            }
        }
    }

    if !slow {
        let natts = (*tupleDesc).natts;
        let mut j = 1;

        /*
         * If we get here, we have a tuple with no nulls or var-widths up to
         * and including the target attribute, so we can use the cached offset
         * ... only we don't have it yet, or we'd not have got here.  Since
         * it's cheap to compute offsets for fixed-width columns, we take the
         * opportunity to initialize the cached offsets for *all* the leading
         * fixed-width columns, in hope of avoiding future visits to this
         * routine.
         */
        (*TupleDescCompactAttr(tupleDesc, 0)).attcacheoff = 0;

        /* we might have set some offsets in the slow path previously */
        while j < natts && (*TupleDescCompactAttr(tupleDesc, j)).attcacheoff > 0 {
            j += 1;
        }

        off = (*TupleDescCompactAttr(tupleDesc, j - 1)).attcacheoff
            + (*TupleDescCompactAttr(tupleDesc, j - 1)).attlen as c_int;

        while j < natts {
            let att = TupleDescCompactAttr(tupleDesc, j);

            if (*att).attlen <= 0 {
                break;
            }

            off = att_nominal_alignby(off as usize, (*att).attalignby) as c_int;

            (*att).attcacheoff = off;

            off += (*att).attlen as c_int;

            j += 1;
        }

        Assert!(j > attnum);

        off = (*TupleDescCompactAttr(tupleDesc, attnum)).attcacheoff;
    } else {
        let mut usecache = true;
        let mut i = 0;

        /*
         * Now we know that we have to walk the tuple CAREFULLY.  But we still
         * might be able to cache some offsets for next time.
         *
         * Note - This loop is a little tricky.  For each non-null attribute,
         * we have to first account for alignment padding before the attr,
         * then advance over the attr based on its length.  Nulls have no
         * storage and no alignment padding either.  We can use/set
         * attcacheoff until we reach either a null or a var-width attribute.
         */
        off = 0;
        loop {
            /* loop exit is at "break" */
            let att = TupleDescCompactAttr(tupleDesc, i);

            if HeapTupleHasNulls(tup) && att_isnull(i, bp) {
                usecache = false;
                i += 1;
                continue; /* this cannot be the target att */
            }

            /* If we know the next offset, we can skip the rest */
            if usecache && (*att).attcacheoff >= 0 {
                off = (*att).attcacheoff;
            } else if (*att).attlen == -1 {
                /*
                 * We can only cache the offset for a varlena attribute if the
                 * offset is already suitably aligned, so that there would be
                 * no pad bytes in any case: then the offset will be valid for
                 * either an aligned or unaligned value.
                 */
                if usecache
                    && off as usize == att_nominal_alignby(off as usize, (*att).attalignby)
                {
                    (*att).attcacheoff = off;
                } else {
                    off = att_pointer_alignby(
                        off as usize,
                        (*att).attalignby,
                        -1,
                        tp.add(off as usize),
                    ) as c_int;
                    usecache = false;
                }
            } else {
                /* not varlena, so safe to use att_nominal_alignby */
                off = att_nominal_alignby(off as usize, (*att).attalignby) as c_int;

                if usecache {
                    (*att).attcacheoff = off;
                }
            }

            if i == attnum {
                break;
            }

            off = att_addlength_pointer(off as usize, (*att).attlen as c_int, tp.add(off as usize))
                as c_int;

            if usecache && (*att).attlen <= 0 {
                usecache = false;
            }

            i += 1;
        }
    }

    fetchatt(TupleDescCompactAttr(tupleDesc, attnum), tp.add(off as usize))
}

/*
 * fetchatt - fetch_att over a CompactAttribute (tupmacs.h #define rendered local).
 *
 * # Safety
 * `att` is a live CompactAttribute; `T` points to a properly-aligned field of
 * at least attlen readable bytes.
 */
#[inline]
unsafe fn fetchatt(att: *const CompactAttribute, T: *const c_char) -> Datum {
    fetch_att(T as *const c_void, (*att).attbyval, (*att).attlen as c_int)
}

/* ----------------
 *		heap_getsysattr
 *
 *		Fetch the value of a system attribute for a tuple.
 *
 * This is a support routine for heap_getattr().  The function has already
 * determined that the attnum refers to a system attribute.
 *
 * Note: upstream resolves cmin/cmax via HeapTupleHeaderGetRawCommandId here
 * (NOT combocid.c); that XXX is preserved in the comment below.  So this is a
 * fully real translation.
 * ----------------
 *
 * # Safety
 * `tup` is a valid HeapTuple; `isnull` is writable.
 */
pub unsafe fn heap_getsysattr(
    tup: HeapTuple,
    attnum: c_int,
    _tupleDesc: TupleDesc,
    isnull: *mut bool,
) -> Datum {
    let result: Datum;

    Assert!(!tup.is_null());

    /* Currently, no sys attribute ever reads as NULL. */
    *isnull = false;

    match attnum {
        SelfItemPointerAttributeNumber => {
            /* pass-by-reference datatype */
            result = PointerGetDatum(&(*tup).t_self as *const _ as *const c_void);
        }
        MinTransactionIdAttributeNumber => {
            result = TransactionIdGetDatum(HeapTupleHeaderGetRawXmin((*tup).t_data));
        }
        MaxTransactionIdAttributeNumber => {
            result = TransactionIdGetDatum(HeapTupleHeaderGetRawXmax((*tup).t_data));
        }
        MinCommandIdAttributeNumber | MaxCommandIdAttributeNumber => {
            /*
             * cmin and cmax are now both aliases for the same field, which
             * can in fact also be a combo command id.  XXX perhaps we should
             * return the "real" cmin or cmax if possible, that is if we are
             * inside the originating transaction?
             */
            result = CommandIdGetDatum(HeapTupleHeaderGetRawCommandId((*tup).t_data));
        }
        TableOidAttributeNumber => {
            result = ObjectIdGetDatum((*tup).t_tableOid);
        }
        _ => {
            elog!(ERROR, "invalid attnum: {}", attnum);
            #[allow(unreachable_code)]
            {
                result = 0; /* keep compiler quiet */
            }
        }
    }
    result
}

/* ----------------
 *		heap_copytuple
 *
 *		returns a copy of an entire tuple
 *
 * The HeapTuple struct, tuple header, and tuple data are all allocated
 * as a single palloc() block.
 * ----------------
 *
 * # Safety
 * `tuple` is null or a valid HeapTuple.
 */
pub unsafe fn heap_copytuple(tuple: HeapTuple) -> HeapTuple {
    if !HeapTupleIsValid(tuple) || (*tuple).t_data.is_null() {
        return null_mut();
    }

    let newTuple = palloc(HEAPTUPLESIZE + (*tuple).t_len as usize) as HeapTuple;
    (*newTuple).t_len = (*tuple).t_len;
    (*newTuple).t_self = (*tuple).t_self;
    (*newTuple).t_tableOid = (*tuple).t_tableOid;
    (*newTuple).t_data = (newTuple as *mut c_char).add(HEAPTUPLESIZE) as HeapTupleHeader;
    memcpy(
        (*newTuple).t_data as *mut c_void,
        (*tuple).t_data as *const c_void,
        (*tuple).t_len as usize,
    );
    newTuple
}

/* ----------------
 *		heap_copytuple_with_tuple
 *
 *		copy a tuple into a caller-supplied HeapTuple management struct
 * ----------------
 *
 * # Safety
 * `src` is null or valid; `dest` is a writable HeapTupleData.
 */
pub unsafe fn heap_copytuple_with_tuple(src: HeapTuple, dest: HeapTuple) {
    if !HeapTupleIsValid(src) || (*src).t_data.is_null() {
        (*dest).t_data = null_mut();
        return;
    }

    (*dest).t_len = (*src).t_len;
    (*dest).t_self = (*src).t_self;
    (*dest).t_tableOid = (*src).t_tableOid;
    (*dest).t_data = palloc((*src).t_len as usize) as HeapTupleHeader;
    memcpy(
        (*dest).t_data as *mut c_void,
        (*src).t_data as *const c_void,
        (*src).t_len as usize,
    );
}

/*
 * Expand a tuple which has fewer attributes than required. For each attribute
 * not present in the sourceTuple, if there is a missing value that will be
 * used. Otherwise the attribute will be set to NULL.
 *
 * The source tuple must have fewer attributes than the required number.
 *
 * Only one of targetHeapTuple and targetMinimalTuple may be supplied. The
 * other argument must be NULL.
 *
 * # Safety
 * Exactly one of the two target out-params is non-null; `sourceTuple` and
 * `tupleDesc` are live, with sourceNatts < natts.
 */
unsafe fn expand_tuple(
    targetHeapTuple: *mut HeapTuple,
    targetMinimalTuple: *mut MinimalTuple,
    sourceTuple: HeapTuple,
    tupleDesc: TupleDesc,
) {
    let mut attrmiss: *mut AttrMissing = null_mut();
    let mut hasNulls = HeapTupleHasNulls(sourceTuple);
    let targetTHeader: HeapTupleHeader;
    let sourceTHeader = (*sourceTuple).t_data;
    let sourceNatts = HeapTupleHeaderGetNatts(sourceTHeader) as c_int;
    let natts = (*tupleDesc).natts;
    let mut sourceNullLen: c_int;
    let targetNullLen: c_int;
    let sourceDataLen: Size = (*sourceTuple).t_len as Size - (*sourceTHeader).t_hoff as Size;
    let mut targetDataLen: Size;
    let mut len: Size;
    let hoff: c_int;
    let mut nullBits: *mut bits8 = null_mut();
    let mut bitMask: c_int = 0;
    let mut targetData: *mut c_char;
    let infoMask: *mut uint16;

    Assert!(
        (!targetHeapTuple.is_null() && targetMinimalTuple.is_null())
            || (targetHeapTuple.is_null() && !targetMinimalTuple.is_null())
    );

    Assert!(sourceNatts < natts);

    sourceNullLen = if hasNulls { BITMAPLEN(sourceNatts) } else { 0 };

    targetDataLen = sourceDataLen;

    if !(*tupleDesc).constr.is_null() && !(*(*tupleDesc).constr).missing.is_null() {
        /*
         * If there are missing values we want to put them into the tuple.
         * Before that we have to compute the extra length for the values
         * array and the variable length data.
         */
        attrmiss = (*(*tupleDesc).constr).missing;

        /*
         * Find the first item in attrmiss for which we don't have a value in
         * the source. We can ignore all the missing entries before that.
         */
        let mut firstmissingnum = sourceNatts;
        while firstmissingnum < natts {
            if (*attrmiss.add(firstmissingnum as usize)).am_present {
                break;
            } else {
                hasNulls = true;
            }
            firstmissingnum += 1;
        }

        /*
         * Now walk the missing attributes. If there is a missing value make
         * space for it. Otherwise, it's going to be NULL.
         */
        let mut attnum = firstmissingnum;
        while attnum < natts {
            if (*attrmiss.add(attnum as usize)).am_present {
                let att = TupleDescCompactAttr(tupleDesc, attnum);

                targetDataLen = att_datum_alignby(
                    targetDataLen,
                    (*att).attalignby,
                    (*att).attlen as c_int,
                    (*attrmiss.add(attnum as usize)).am_value,
                );

                targetDataLen = att_addlength_pointer(
                    targetDataLen,
                    (*att).attlen as c_int,
                    DatumGetPointer((*attrmiss.add(attnum as usize)).am_value) as *const c_char,
                );
            } else {
                /* no missing value, so it must be null */
                hasNulls = true;
            }
            attnum += 1;
        }
    } else {
        /*
         * If there are no missing values at all then NULLS must be allowed,
         * since some of the attributes are known to be absent.
         */
        hasNulls = true;
    }

    len = 0;

    if hasNulls {
        targetNullLen = BITMAPLEN(natts);
        len += targetNullLen as Size;
    } else {
        targetNullLen = 0;
    }

    /*
     * Allocate and zero the space needed.  Note that the tuple body and
     * HeapTupleData management structure are allocated in one chunk.
     */
    if !targetHeapTuple.is_null() {
        len += offset_of!(HeapTupleHeaderData, t_bits);
        len = MAXALIGN(len); /* align user data safely */
        hoff = len as c_int;
        len += targetDataLen;

        *targetHeapTuple = palloc0(HEAPTUPLESIZE + len) as HeapTuple;
        targetTHeader =
            ((*targetHeapTuple as *mut c_char).add(HEAPTUPLESIZE)) as HeapTupleHeader;
        (**targetHeapTuple).t_data = targetTHeader;
        (**targetHeapTuple).t_len = len as uint32;
        (**targetHeapTuple).t_tableOid = (*sourceTuple).t_tableOid;
        (**targetHeapTuple).t_self = (*sourceTuple).t_self;

        (*targetTHeader).t_infomask = (*sourceTHeader).t_infomask;
        (*targetTHeader).t_hoff = hoff as uint8;
        HeapTupleHeaderSetNatts(targetTHeader, natts as uint16);
        HeapTupleHeaderSetDatumLength(targetTHeader, len as uint32);
        HeapTupleHeaderSetTypeId(targetTHeader, (*tupleDesc).tdtypeid);
        HeapTupleHeaderSetTypMod(targetTHeader, (*tupleDesc).tdtypmod);
        /* We also make sure that t_ctid is invalid unless explicitly set */
        ItemPointerSetInvalid(&mut (*targetTHeader).t_ctid);
        if targetNullLen > 0 {
            nullBits = ((*(*targetHeapTuple)).t_data as *mut c_char)
                .add(offset_of!(HeapTupleHeaderData, t_bits))
                as *mut bits8;
        }
        targetData = ((*(*targetHeapTuple)).t_data as *mut c_char).add(hoff as usize);
        infoMask = &mut (*targetTHeader).t_infomask;
    } else {
        len += SizeofMinimalTupleHeader;
        len = MAXALIGN(len); /* align user data safely */
        hoff = len as c_int;
        len += targetDataLen;

        *targetMinimalTuple = palloc0(len) as MinimalTuple;
        (**targetMinimalTuple).t_len = len as uint32;
        (**targetMinimalTuple).t_hoff = (hoff as usize + MINIMAL_TUPLE_OFFSET) as uint8;
        (**targetMinimalTuple).t_infomask = (*sourceTHeader).t_infomask;
        /* Same macro works for MinimalTuples */
        HeapTupleHeaderSetNatts(
            *targetMinimalTuple as *mut HeapTupleHeaderData,
            natts as uint16,
        );
        if targetNullLen > 0 {
            nullBits = ((*targetMinimalTuple) as *mut c_char)
                .add(offset_of!(MinimalTupleData, t_bits)) as *mut bits8;
        }
        targetData = ((*targetMinimalTuple) as *mut c_char).add(hoff as usize);
        infoMask = &mut (**targetMinimalTuple).t_infomask;
    }

    if targetNullLen > 0 {
        if sourceNullLen > 0 {
            /* if bitmap pre-existed copy in - all is set */
            memcpy(
                nullBits as *mut c_void,
                (sourceTHeader as *const c_char).add(offset_of!(HeapTupleHeaderData, t_bits))
                    as *const c_void,
                sourceNullLen as usize,
            );
            nullBits = nullBits.add((sourceNullLen - 1) as usize);
        } else {
            sourceNullLen = BITMAPLEN(sourceNatts);
            /* Set NOT NULL for all existing attributes */
            memset(nullBits as *mut c_void, 0xff, sourceNullLen as usize);

            nullBits = nullBits.add((sourceNullLen - 1) as usize);

            if (sourceNatts & 0x07) != 0 {
                /* build the mask (inverted!) */
                bitMask = 0xff << (sourceNatts & 0x07);
                /* Voila */
                *nullBits = !bitMask as bits8;
            }
        }

        bitMask = 1 << ((sourceNatts - 1) & 0x07);
    } /* End if have null bitmap */

    memcpy(
        targetData as *mut c_void,
        ((*sourceTuple).t_data as *const c_char).add((*sourceTHeader).t_hoff as usize)
            as *const c_void,
        sourceDataLen,
    );

    targetData = targetData.add(sourceDataLen);

    /* Now fill in the missing values */
    let mut attnum = sourceNatts;
    while attnum < natts {
        let attr = TupleDescCompactAttr(tupleDesc, attnum);

        if !attrmiss.is_null() && (*attrmiss.add(attnum as usize)).am_present {
            let bit_arg: Option<&mut *mut bits8> = if !nullBits.is_null() {
                Some(&mut nullBits)
            } else {
                None
            };
            fill_val(
                attr,
                bit_arg,
                &mut bitMask,
                &mut targetData,
                infoMask,
                (*attrmiss.add(attnum as usize)).am_value,
                false,
            );
        } else {
            fill_val(
                attr,
                Some(&mut nullBits),
                &mut bitMask,
                &mut targetData,
                infoMask,
                0 as Datum,
                true,
            );
        }
        attnum += 1;
    } /* end loop over missing attributes */
}

/*
 * Fill in the missing values for a minimal HeapTuple
 *
 * # Safety
 * `sourceTuple` and `tupleDesc` are live, sourceNatts < natts.
 */
pub unsafe fn minimal_expand_tuple(sourceTuple: HeapTuple, tupleDesc: TupleDesc) -> MinimalTuple {
    let mut minimalTuple: MinimalTuple = null_mut();

    expand_tuple(null_mut(), &mut minimalTuple, sourceTuple, tupleDesc);
    minimalTuple
}

/*
 * Fill in the missing values for an ordinary HeapTuple
 *
 * # Safety
 * `sourceTuple` and `tupleDesc` are live, sourceNatts < natts.
 */
pub unsafe fn heap_expand_tuple(sourceTuple: HeapTuple, tupleDesc: TupleDesc) -> HeapTuple {
    let mut heapTuple: HeapTuple = null_mut();

    expand_tuple(&mut heapTuple, null_mut(), sourceTuple, tupleDesc);
    heapTuple
}

/* ----------------
 *		heap_copy_tuple_as_datum
 *
 *		copy a tuple as a composite-type Datum
 * ----------------
 *
 * # Safety
 * `tuple` and `tupleDesc` are live.
 */
pub unsafe fn heap_copy_tuple_as_datum(tuple: HeapTuple, tupleDesc: TupleDesc) -> Datum {
    let td: HeapTupleHeader;

    /*
     * If the tuple contains any external TOAST pointers, we have to inline
     * those fields to meet the conventions for composite-type Datums.
     */
    if HeapTupleHasExternal(tuple) {
        // TODO(pg-port): needs access/heaptoast.c toast_flatten_tuple_to_datum.
        // C body:
        //   return toast_flatten_tuple_to_datum(tuple->t_data, tuple->t_len, tupleDesc);
        let _ = tupleDesc;
        unimplemented!(
            "heap_copy_tuple_as_datum: HasExternal branch needs toast_flatten_tuple_to_datum (heaptoast.c)"
        );
    }

    /*
     * Fast path for easy case: just make a palloc'd copy and insert the
     * correct composite-Datum header fields (since those may not be set if
     * the given tuple came from disk, rather than from heap_form_tuple).
     */
    td = palloc((*tuple).t_len as usize) as HeapTupleHeader;
    memcpy(
        td as *mut c_void,
        (*tuple).t_data as *const c_void,
        (*tuple).t_len as usize,
    );

    HeapTupleHeaderSetDatumLength(td, (*tuple).t_len);
    HeapTupleHeaderSetTypeId(td, (*tupleDesc).tdtypeid);
    HeapTupleHeaderSetTypMod(td, (*tupleDesc).tdtypmod);

    PointerGetDatum(td as *const c_void)
}

/*
 * heap_form_tuple
 *		construct a tuple from the given values[] and isnull[] arrays,
 *		which are of the length indicated by tupleDescriptor->natts
 *
 * The result is allocated in the current memory context.
 *
 * # Safety
 * `tupleDescriptor` is live; `values`/`isnull` point to natts elements.
 */
pub unsafe fn heap_form_tuple(
    tupleDescriptor: TupleDesc,
    values: *const Datum,
    isnull: *const bool,
) -> HeapTuple {
    let tuple: HeapTuple; /* return tuple */
    let td: HeapTupleHeader; /* tuple data */
    let mut len: Size;
    let data_len: Size;
    let hoff: c_int;
    let mut hasnull = false;
    let numberOfAttributes = (*tupleDescriptor).natts;

    if numberOfAttributes > MaxTupleAttributeNumber {
        let _ = errcode(ERRCODE_TOO_MANY_COLUMNS);
        ereport!(
            ERROR,
            errmsg!(
                "number of columns ({}) exceeds limit ({})",
                numberOfAttributes,
                MaxTupleAttributeNumber
            )
        );
    }

    /*
     * Check for nulls
     */
    for i in 0..numberOfAttributes {
        if *isnull.add(i as usize) {
            hasnull = true;
            break;
        }
    }

    /*
     * Determine total space needed
     */
    len = offset_of!(HeapTupleHeaderData, t_bits);

    if hasnull {
        len += BITMAPLEN(numberOfAttributes) as Size;
    }

    len = MAXALIGN(len); /* align user data safely */
    hoff = len as c_int;

    data_len = heap_compute_data_size(tupleDescriptor, values, isnull);

    len += data_len;

    /*
     * Allocate and zero the space needed.  Note that the tuple body and
     * HeapTupleData management structure are allocated in one chunk.
     */
    tuple = palloc0(HEAPTUPLESIZE + len) as HeapTuple;
    td = (tuple as *mut c_char).add(HEAPTUPLESIZE) as HeapTupleHeader;
    (*tuple).t_data = td;

    /*
     * And fill in the information.  Note we fill the Datum fields even though
     * this tuple may never become a Datum.
     */
    (*tuple).t_len = len as uint32;
    ItemPointerSetInvalid(&mut (*tuple).t_self);
    (*tuple).t_tableOid = InvalidOid;

    HeapTupleHeaderSetDatumLength(td, len as uint32);
    HeapTupleHeaderSetTypeId(td, (*tupleDescriptor).tdtypeid);
    HeapTupleHeaderSetTypMod(td, (*tupleDescriptor).tdtypmod);
    /* We also make sure that t_ctid is invalid unless explicitly set */
    ItemPointerSetInvalid(&mut (*td).t_ctid);

    HeapTupleHeaderSetNatts(td, numberOfAttributes as uint16);
    (*td).t_hoff = hoff as uint8;

    heap_fill_tuple(
        tupleDescriptor,
        values,
        isnull,
        (td as *mut c_char).add(hoff as usize),
        data_len,
        &mut (*td).t_infomask,
        if hasnull {
            (*td).t_bits.as_mut_ptr()
        } else {
            null_mut()
        },
    );

    tuple
}

/*
 * heap_modify_tuple
 *		form a new tuple from an old tuple and a set of replacement values.
 *
 * # Safety
 * `tuple`/`tupleDesc` are live; the three arrays have natts elements.
 */
#[no_mangle]
pub unsafe fn heap_modify_tuple(
    tuple: HeapTuple,
    tupleDesc: TupleDesc,
    replValues: *const Datum,
    replIsnull: *const bool,
    doReplace: *const bool,
) -> HeapTuple {
    let numberOfAttributes = (*tupleDesc).natts;

    /*
     * allocate and fill values and isnull arrays from either the tuple or the
     * repl information, as appropriate.
     */
    let values = palloc(numberOfAttributes as usize * size_of::<Datum>()) as *mut Datum;
    let isnull = palloc(numberOfAttributes as usize * size_of::<bool>()) as *mut bool;

    heap_deform_tuple(tuple, tupleDesc, values, isnull);

    for attoff in 0..numberOfAttributes {
        if *doReplace.add(attoff as usize) {
            *values.add(attoff as usize) = *replValues.add(attoff as usize);
            *isnull.add(attoff as usize) = *replIsnull.add(attoff as usize);
        }
    }

    /*
     * create a new tuple from the values and isnull arrays
     */
    let newTuple = heap_form_tuple(tupleDesc, values, isnull);

    pfree(values as *mut c_void);
    pfree(isnull as *mut c_void);

    /*
     * copy the identification info of the old tuple: t_ctid, t_self
     */
    (*(*newTuple).t_data).t_ctid = (*(*tuple).t_data).t_ctid;
    (*newTuple).t_self = (*tuple).t_self;
    (*newTuple).t_tableOid = (*tuple).t_tableOid;

    newTuple
}

/*
 * heap_modify_tuple_by_cols
 *		form a new tuple from an old tuple and a set of replacement values.
 *
 * Target column numbers are indexed from 1.
 *
 * # Safety
 * `tuple`/`tupleDesc` are live; replCols/replValues/replIsnull have nCols
 * elements.
 */
pub unsafe fn heap_modify_tuple_by_cols(
    tuple: HeapTuple,
    tupleDesc: TupleDesc,
    nCols: c_int,
    replCols: *const c_int,
    replValues: *const Datum,
    replIsnull: *const bool,
) -> HeapTuple {
    let numberOfAttributes = (*tupleDesc).natts;

    /*
     * allocate and fill values and isnull arrays from the tuple, then replace
     * selected columns from the input arrays.
     */
    let values = palloc(numberOfAttributes as usize * size_of::<Datum>()) as *mut Datum;
    let isnull = palloc(numberOfAttributes as usize * size_of::<bool>()) as *mut bool;

    heap_deform_tuple(tuple, tupleDesc, values, isnull);

    for i in 0..nCols {
        let attnum = *replCols.add(i as usize);

        if attnum <= 0 || attnum > numberOfAttributes {
            elog!(ERROR, "invalid column number {}", attnum);
        }
        *values.add((attnum - 1) as usize) = *replValues.add(i as usize);
        *isnull.add((attnum - 1) as usize) = *replIsnull.add(i as usize);
    }

    /*
     * create a new tuple from the values and isnull arrays
     */
    let newTuple = heap_form_tuple(tupleDesc, values, isnull);

    pfree(values as *mut c_void);
    pfree(isnull as *mut c_void);

    /*
     * copy the identification info of the old tuple: t_ctid, t_self
     */
    (*(*newTuple).t_data).t_ctid = (*(*tuple).t_data).t_ctid;
    (*newTuple).t_self = (*tuple).t_self;
    (*newTuple).t_tableOid = (*tuple).t_tableOid;

    newTuple
}

/*
 * heap_deform_tuple
 *		Given a tuple, extract data into values/isnull arrays; this is
 *		the inverse of heap_form_tuple.
 *
 *		Storage for the values/isnull arrays is provided by the caller;
 *		it should be sized according to tupleDesc->natts not
 *		HeapTupleHeaderGetNatts(tuple->t_data).
 *
 *		Note that for pass-by-reference datatypes, the pointer placed
 *		in the Datum will point into the given tuple.
 *
 * # Safety
 * `tuple`/`tupleDesc` are live; `values`/`isnull` point to tdesc_natts elements.
 */
#[no_mangle]
pub unsafe fn heap_deform_tuple(
    tuple: HeapTuple,
    tupleDesc: TupleDesc,
    values: *mut Datum,
    isnull: *mut bool,
) {
    let tup = (*tuple).t_data;
    let hasnulls = HeapTupleHasNulls(tuple);
    let tdesc_natts = (*tupleDesc).natts;
    let mut natts: c_int; /* number of atts to extract */
    let mut attnum: c_int;
    let tp: *mut c_char; /* ptr to tuple data */
    let mut off: u32; /* offset in tuple data */
    let bp = (*tup).t_bits.as_ptr(); /* ptr to null bitmap in tuple */
    let mut slow = false; /* can we use/set attcacheoff? */

    natts = HeapTupleHeaderGetNatts(tup) as c_int;

    /*
     * In inheritance situations, it is possible that the given tuple actually
     * has more fields than the caller is expecting.  Don't run off the end of
     * the caller's arrays.
     */
    natts = natts.min(tdesc_natts);

    tp = (tup as *mut c_char).add((*tup).t_hoff as usize);

    off = 0;

    attnum = 0;
    while attnum < natts {
        let thisatt = TupleDescCompactAttr(tupleDesc, attnum);

        if hasnulls && att_isnull(attnum, bp) {
            *values.add(attnum as usize) = 0 as Datum;
            *isnull.add(attnum as usize) = true;
            slow = true; /* can't use attcacheoff anymore */
            attnum += 1;
            continue;
        }

        *isnull.add(attnum as usize) = false;

        if !slow && (*thisatt).attcacheoff >= 0 {
            off = (*thisatt).attcacheoff as u32;
        } else if (*thisatt).attlen == -1 {
            /*
             * We can only cache the offset for a varlena attribute if the
             * offset is already suitably aligned, so that there would be no
             * pad bytes in any case: then the offset will be valid for either
             * an aligned or unaligned value.
             */
            if !slow && off as usize == att_nominal_alignby(off as usize, (*thisatt).attalignby) {
                (*thisatt).attcacheoff = off as int32;
            } else {
                off = att_pointer_alignby(
                    off as usize,
                    (*thisatt).attalignby,
                    -1,
                    tp.add(off as usize),
                ) as u32;
                slow = true;
            }
        } else {
            /* not varlena, so safe to use att_nominal_alignby */
            off = att_nominal_alignby(off as usize, (*thisatt).attalignby) as u32;

            if !slow {
                (*thisatt).attcacheoff = off as int32;
            }
        }

        *values.add(attnum as usize) = fetchatt(thisatt, tp.add(off as usize));

        off = att_addlength_pointer(off as usize, (*thisatt).attlen as c_int, tp.add(off as usize))
            as u32;

        if (*thisatt).attlen <= 0 {
            slow = true; /* can't use attcacheoff anymore */
        }

        attnum += 1;
    }

    /*
     * If tuple doesn't have all the atts indicated by tupleDesc, read the
     * rest as nulls or missing values as appropriate.
     */
    while attnum < tdesc_natts {
        *values.add(attnum as usize) =
            getmissingattr(tupleDesc, attnum + 1, isnull.add(attnum as usize));
        attnum += 1;
    }
}

/*
 * heap_freetuple
 *
 * # Safety
 * `htup` was allocated as a single palloc block (heap_copytuple/heap_form_tuple).
 */
#[no_mangle]
pub unsafe fn heap_freetuple(htup: HeapTuple) {
    pfree(htup as *mut c_void);
}

/*
 * heap_form_minimal_tuple
 *		construct a MinimalTuple from the given values[] and isnull[] arrays,
 *		which are of the length indicated by tupleDescriptor->natts
 *
 * The result is allocated in the current memory context.
 *
 * # Safety
 * `tupleDescriptor` is live; arrays have natts elements; extra is MAXALIGN'd.
 */
pub unsafe fn heap_form_minimal_tuple(
    tupleDescriptor: TupleDesc,
    values: *const Datum,
    isnull: *const bool,
    extra: Size,
) -> MinimalTuple {
    let tuple: MinimalTuple; /* return tuple */
    let mem: *mut c_char;
    let mut len: Size;
    let data_len: Size;
    let hoff: c_int;
    let mut hasnull = false;
    let numberOfAttributes = (*tupleDescriptor).natts;

    Assert!(extra == MAXALIGN(extra));

    if numberOfAttributes > MaxTupleAttributeNumber {
        let _ = errcode(ERRCODE_TOO_MANY_COLUMNS);
        ereport!(
            ERROR,
            errmsg!(
                "number of columns ({}) exceeds limit ({})",
                numberOfAttributes,
                MaxTupleAttributeNumber
            )
        );
    }

    /*
     * Check for nulls
     */
    for i in 0..numberOfAttributes {
        if *isnull.add(i as usize) {
            hasnull = true;
            break;
        }
    }

    /*
     * Determine total space needed
     */
    len = SizeofMinimalTupleHeader;

    if hasnull {
        len += BITMAPLEN(numberOfAttributes) as Size;
    }

    len = MAXALIGN(len); /* align user data safely */
    hoff = len as c_int;

    data_len = heap_compute_data_size(tupleDescriptor, values, isnull);

    len += data_len;

    /*
     * Allocate and zero the space needed.
     */
    mem = palloc0(len + extra) as *mut c_char;
    memset(mem as *mut c_void, 0, extra);
    tuple = mem.add(extra) as MinimalTuple;

    /*
     * And fill in the information.
     */
    (*tuple).t_len = len as uint32;
    /*
     * HeapTupleHeaderSetNatts(tuple, ...): the C macro is duck-typed and sets
     * the field reached by `tuple->t_infomask2`.  MinimalTupleData has its own
     * t_infomask2 at a different struct offset than HeapTupleHeaderData, so we
     * must set the MinimalTuple's OWN field directly here (casting to
     * HeapTupleHeader would write t_infomask2 at the wrong offset).
     */
    (*tuple).t_infomask2 =
        ((*tuple).t_infomask2 & !HEAP_NATTS_MASK) | (numberOfAttributes as uint16);
    (*tuple).t_hoff = (hoff as usize + MINIMAL_TUPLE_OFFSET) as uint8;

    heap_fill_tuple(
        tupleDescriptor,
        values,
        isnull,
        (tuple as *mut c_char).add(hoff as usize),
        data_len,
        &mut (*tuple).t_infomask,
        if hasnull {
            (*tuple).t_bits.as_mut_ptr()
        } else {
            null_mut()
        },
    );

    tuple
}

/*
 * heap_free_minimal_tuple
 *
 * # Safety
 * `mtup` was allocated by heap_form_minimal_tuple / heap_copy_minimal_tuple.
 */
pub unsafe fn heap_free_minimal_tuple(mtup: MinimalTuple) {
    pfree(mtup as *mut c_void);
}

/*
 * heap_copy_minimal_tuple
 *		copy a MinimalTuple
 *
 * The result is allocated in the current memory context.
 *
 * # Safety
 * `mtup` is a valid MinimalTuple; `extra` is MAXALIGN'd.
 */
pub unsafe fn heap_copy_minimal_tuple(mtup: MinimalTuple, extra: Size) -> MinimalTuple {
    let result: MinimalTuple;
    let mem: *mut c_char;

    Assert!(extra == MAXALIGN(extra));
    mem = palloc((*mtup).t_len as usize + extra) as *mut c_char;
    memset(mem as *mut c_void, 0, extra);
    result = mem.add(extra) as MinimalTuple;
    memcpy(
        result as *mut c_void,
        mtup as *const c_void,
        (*mtup).t_len as usize,
    );
    result
}

/*
 * heap_tuple_from_minimal_tuple
 *		create a HeapTuple by copying from a MinimalTuple;
 *		system columns are filled with zeroes
 *
 * The result is allocated in the current memory context.
 *
 * # Safety
 * `mtup` is a valid MinimalTuple.
 */
pub unsafe fn heap_tuple_from_minimal_tuple(mtup: MinimalTuple) -> HeapTuple {
    let result: HeapTuple;
    let len: u32 = (*mtup).t_len + MINIMAL_TUPLE_OFFSET as u32;

    result = palloc(HEAPTUPLESIZE + len as usize) as HeapTuple;
    (*result).t_len = len;
    ItemPointerSetInvalid(&mut (*result).t_self);
    (*result).t_tableOid = InvalidOid;
    (*result).t_data = (result as *mut c_char).add(HEAPTUPLESIZE) as HeapTupleHeader;
    memcpy(
        ((*result).t_data as *mut c_char).add(MINIMAL_TUPLE_OFFSET) as *mut c_void,
        mtup as *const c_void,
        (*mtup).t_len as usize,
    );
    memset(
        (*result).t_data as *mut c_void,
        0,
        offset_of!(HeapTupleHeaderData, t_infomask2),
    );
    result
}

/*
 * minimal_tuple_from_heap_tuple
 *		create a MinimalTuple by copying from a HeapTuple
 *
 * The result is allocated in the current memory context.
 *
 * # Safety
 * `htup` is a valid HeapTuple with t_len > MINIMAL_TUPLE_OFFSET; extra MAXALIGN'd.
 */
pub unsafe fn minimal_tuple_from_heap_tuple(htup: HeapTuple, extra: Size) -> MinimalTuple {
    let result: MinimalTuple;
    let mem: *mut c_char;
    let len: u32;

    Assert!(extra == MAXALIGN(extra));
    Assert!((*htup).t_len as usize > MINIMAL_TUPLE_OFFSET);
    len = (*htup).t_len - MINIMAL_TUPLE_OFFSET as u32;
    mem = palloc(len as usize + extra) as *mut c_char;
    memset(mem as *mut c_void, 0, extra);
    result = mem.add(extra) as MinimalTuple;
    memcpy(
        result as *mut c_void,
        ((*htup).t_data as *const c_char).add(MINIMAL_TUPLE_OFFSET) as *const c_void,
        len as usize,
    );

    (*result).t_len = len;
    result
}

/*
 * This mainly exists so JIT can inline the definition, but it's also
 * sometimes useful in debugging sessions.
 *
 * # Safety
 * `p` points to a valid varlena datum.
 */
pub unsafe fn varsize_any(p: *mut c_void) -> usize {
    VARSIZE_ANY(p as *const c_char) as usize
}

// ============================================================================
//   Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::common::tupdesc::{CreateTemplateTupleDesc, TupleDescInitBuiltinEntry};
    use crate::catalog::pg_type_d::{INT4OID, INT8OID};
    use crate::postgres::{DatumGetInt32, DatumGetInt64, Int32GetDatum, Int64GetDatum};

    /*
     * Build a 3-column TupleDesc: INT4 "a", INT4 "b", INT8 "c" (all fixed-len byval).
     * This avoids needing real varlena input in the round-trip test.
     */
    unsafe fn make_td() -> TupleDesc {
        let td = CreateTemplateTupleDesc(3);
        TupleDescInitBuiltinEntry(td, 1, c"a".as_ptr(), INT4OID, -1, 0);
        TupleDescInitBuiltinEntry(td, 2, c"b".as_ptr(), INT4OID, -1, 0);
        TupleDescInitBuiltinEntry(td, 3, c"c".as_ptr(), INT8OID, -1, 0);
        td
    }

    #[test]
    fn form_deform_roundtrip() {
        unsafe {
            let td = make_td();

            let values: [Datum; 3] =
                [Int32GetDatum(11), Int32GetDatum(22), Int64GetDatum(333)];
            let isnull: [bool; 3] = [false; 3];

            let t = heap_form_tuple(td, values.as_ptr(), isnull.as_ptr());

            // deform and check the three values come back equal and not null.
            let mut out_values: [Datum; 3] = [0; 3];
            let mut out_isnull: [bool; 3] = [true; 3];
            heap_deform_tuple(t, td, out_values.as_mut_ptr(), out_isnull.as_mut_ptr());

            assert!(!out_isnull[0]);
            assert!(!out_isnull[1]);
            assert!(!out_isnull[2]);
            assert_eq!(DatumGetInt32(out_values[0]), 11);
            assert_eq!(DatumGetInt32(out_values[1]), 22);
            assert_eq!(DatumGetInt64(out_values[2]), 333);

            // heap_attisnull: none of these are null.
            assert!(!heap_attisnull(t, 1, td));
            assert!(!heap_attisnull(t, 2, td));
            assert!(!heap_attisnull(t, 3, td));

            heap_freetuple(t);
        }
    }

    #[test]
    fn null_in_middle_roundtrip() {
        unsafe {
            let td = make_td();

            let values: [Datum; 3] =
                [Int32GetDatum(11), 0 as Datum, Int64GetDatum(333)];
            let isnull: [bool; 3] = [false, true, false];

            let t = heap_form_tuple(td, values.as_ptr(), isnull.as_ptr());

            let mut out_values: [Datum; 3] = [0; 3];
            let mut out_isnull: [bool; 3] = [false; 3];
            heap_deform_tuple(t, td, out_values.as_mut_ptr(), out_isnull.as_mut_ptr());

            assert!(!out_isnull[0]);
            assert!(out_isnull[1]);
            assert!(!out_isnull[2]);
            assert_eq!(DatumGetInt32(out_values[0]), 11);
            assert_eq!(DatumGetInt64(out_values[2]), 333);

            // heap_attisnull on the middle (1-based attnum 2) must be true.
            assert!(heap_attisnull(t, 2, td));
            assert!(!heap_attisnull(t, 1, td));
            assert!(!heap_attisnull(t, 3, td));

            heap_freetuple(t);
        }
    }

    #[test]
    fn copytuple_equal_and_free() {
        unsafe {
            let td = make_td();
            let values: [Datum; 3] =
                [Int32GetDatum(7), Int32GetDatum(8), Int64GetDatum(9)];
            let isnull: [bool; 3] = [false; 3];

            let t = heap_form_tuple(td, values.as_ptr(), isnull.as_ptr());
            let c = heap_copytuple(t);

            // equal length, equal data bytes.
            assert_eq!((*c).t_len, (*t).t_len);
            let a = core::slice::from_raw_parts((*t).t_data as *const u8, (*t).t_len as usize);
            let b = core::slice::from_raw_parts((*c).t_data as *const u8, (*c).t_len as usize);
            assert_eq!(a, b);

            // deform the copy and re-check values.
            let mut ov: [Datum; 3] = [0; 3];
            let mut oi: [bool; 3] = [true; 3];
            heap_deform_tuple(c, td, ov.as_mut_ptr(), oi.as_mut_ptr());
            assert_eq!(DatumGetInt32(ov[0]), 7);
            assert_eq!(DatumGetInt32(ov[1]), 8);
            assert_eq!(DatumGetInt64(ov[2]), 9);

            heap_freetuple(c);
            heap_freetuple(t);
        }
    }

    #[test]
    fn minimal_tuple_roundtrip() {
        unsafe {
            let td = make_td();
            let values: [Datum; 3] =
                [Int32GetDatum(1), Int32GetDatum(2), Int64GetDatum(3)];
            let isnull: [bool; 3] = [false; 3];

            let mt = heap_form_minimal_tuple(td, values.as_ptr(), isnull.as_ptr(), 0);
            // Convert to a HeapTuple and deform it.
            let ht = heap_tuple_from_minimal_tuple(mt);
            let mut ov: [Datum; 3] = [0; 3];
            let mut oi: [bool; 3] = [true; 3];
            heap_deform_tuple(ht, td, ov.as_mut_ptr(), oi.as_mut_ptr());
            assert_eq!(DatumGetInt32(ov[0]), 1);
            assert_eq!(DatumGetInt32(ov[1]), 2);
            assert_eq!(DatumGetInt64(ov[2]), 3);

            heap_free_minimal_tuple(mt);
            heap_freetuple(ht);
        }
    }

    /*
     * Exercise heap_getattr -> fastgetattr -> nocachegetattr (the single-attr
     * walk).  On a freshly-formed tuple the CompactAttributes have attcacheoff
     * == -1 for the columns after a varlena/uncached one, so fetching attr 3
     * forces the slow path that primes the cache (and validates the
     * htup_details fastgetattr slow-path repoint to this module's nocachegetattr).
     */
    #[test]
    fn heap_getattr_via_fastgetattr() {
        unsafe {
            let td = make_td();
            let values: [Datum; 3] =
                [Int32GetDatum(101), Int32GetDatum(202), Int64GetDatum(303)];
            let isnull: [bool; 3] = [false; 3];
            let t = heap_form_tuple(td, values.as_ptr(), isnull.as_ptr());

            let mut n = false;
            let a = crate::access::htup_details::heap_getattr(t, 1, td, &mut n);
            assert_eq!(DatumGetInt32(a), 101);
            assert!(!n);
            let c = crate::access::htup_details::heap_getattr(t, 3, td, &mut n);
            assert_eq!(DatumGetInt64(c), 303);
            assert!(!n);
            // tableoid system column reads back as the (invalid) t_tableOid.
            let b = crate::access::htup_details::heap_getattr(t, 2, td, &mut n);
            assert_eq!(DatumGetInt32(b), 202);

            heap_freetuple(t);
        }
    }
}

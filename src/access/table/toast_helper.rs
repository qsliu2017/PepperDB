//! Translation of postgres/src/backend/access/table/toast_helper.c
//!
//! Helper functions for table AMs implementing compressed or out-of-line
//! storage of varlena attributes.  This is the shared TOAST orchestration
//! layer: it decides which attributes of an over-large heap tuple to compress
//! and/or move out-of-line, but leaves the actual on-disk insert/fetch/delete
//! to the per-AM toast_internals layer.
//!
//! Copyright (c) 2000-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!   src/backend/access/table/toast_helper.c
//!
//! Merged in from access/toast_helper.h: the ToastTupleContext / ToastAttrInfo
//! structs and the TOAST_* / TOASTCOL_* flag consts.
//!
//! `#include`s mapped:
//!   - access/detoast.h        -> crate::access::common::detoast
//!       (detoast_attr / detoast_external_attr) + TOAST_POINTER_SIZE (defined locally)
//!   - access/toast_helper.h   -> the structs + flag consts (merged below)
//!   - access/toast_internals.h-> crate::access::common::toast_internals
//!       (toast_compress_datum / toast_save_datum / toast_delete_datum)
//!   - catalog/pg_type_d.h     -> (TYPSTORAGE_* codes; re-exported from tupdesc)
//!   - varatt.h                -> crate::varatt (VARATT_IS_* / VARSIZE_ANY / struct varlena)
//!       plus VARATT_IS_EXTERNAL_ONDISK / VARSIZE_EXTERNAL / VARTAG_* defined locally
//!       (these are private in detoast.rs, so re-derived here against the same layout).
//!
//! REAL: every function ports faithfully over the ported toast layer.  The two
//! routines that reach into not-yet-ported relation/catalog/snapshot machinery
//! (toast_save_datum in toast_tuple_externalize, toast_delete_datum in
//! toast_tuple_cleanup / toast_delete_external) call the toast_internals stubs,
//! which carry the preserved C bodies and `unimplemented!()`.

use crate::prelude::*;
use crate::varatt::*;

use crate::access::common::detoast::{detoast_attr, detoast_external_attr, varatt_external};
use crate::access::common::toast_internals::{
    toast_compress_datum, toast_delete_datum, toast_save_datum,
};
use crate::access::common::tupdesc::{
    TupleDesc, TupleDescAttr, TupleDescCompactAttr, TYPSTORAGE_EXTENDED, TYPSTORAGE_EXTERNAL,
    TYPSTORAGE_MAIN, TYPSTORAGE_PLAIN,
};
use crate::utils::rel::Relation;

use core::ffi::{c_char, c_int};

extern "C" {
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

// ----------------------------------------------------------------------------
//   access/toast_helper.h: structs
// ----------------------------------------------------------------------------

/*
 * Information about one column of a tuple being toasted.
 *
 * NOTE: tai_size is only made valid for varlena attributes whose column is not
 * marked TOASTCOL_IGNORE.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ToastAttrInfo {
    pub tai_oldexternal: *mut varlena,
    pub tai_size: int32,
    pub tai_colflags: uint8,
    pub tai_compression: c_char,
}

/*
 * Information about one tuple being toasted.
 */
#[repr(C)]
pub struct ToastTupleContext {
    /*
     * Before calling toast_tuple_init, the caller must initialize the
     * following fields.  Each array must have a length equal to
     * ttc_rel->rd_att->natts.  The ttc_oldvalues and ttc_oldisnull fields
     * should be NULL in the case of an insert.
     */
    pub ttc_rel: Relation,        /* the relation that contains the tuple */
    pub ttc_values: *mut Datum,   /* values from the tuple columns */
    pub ttc_isnull: *mut bool,    /* null flags for the tuple columns */
    pub ttc_oldvalues: *mut Datum, /* values from previous tuple */
    pub ttc_oldisnull: *mut bool, /* null flags from previous tuple */

    /*
     * Before calling toast_tuple_init, the caller should set ttc_attr to point
     * to an array of ToastAttrInfo structures of a length equal to
     * ttc_rel->rd_att->natts.  The contents of the array need not be
     * initialized.  ttc_flags also does not need to be initialized.
     */
    pub ttc_flags: uint8,
    pub ttc_attr: *mut ToastAttrInfo,
}

// ----------------------------------------------------------------------------
//   access/toast_helper.h: flag consts
// ----------------------------------------------------------------------------

/* Flags indicating the overall state of a TOAST operation. */
pub const TOAST_NEEDS_DELETE_OLD: uint8 = 0x0001;
pub const TOAST_NEEDS_FREE: uint8 = 0x0002;
pub const TOAST_HAS_NULLS: uint8 = 0x0004;
pub const TOAST_NEEDS_CHANGE: uint8 = 0x0008;

/* Flags indicating the status of a TOAST operation for a particular column. */
pub const TOASTCOL_NEEDS_DELETE_OLD: uint8 = TOAST_NEEDS_DELETE_OLD;
pub const TOASTCOL_NEEDS_FREE: uint8 = TOAST_NEEDS_FREE;
pub const TOASTCOL_IGNORE: uint8 = 0x0010;
pub const TOASTCOL_INCOMPRESSIBLE: uint8 = 0x0020;

// ----------------------------------------------------------------------------
//   Local TOAST-pointer macros
//
//   These mirror varatt.h.  VARATT_IS_EXTERNAL_ONDISK / VARSIZE_EXTERNAL are
//   private (non-pub) inside detoast.rs, so they are re-derived here against
//   the same `struct varatt_external` / `struct varatt_indirect` layout.
//   TOAST_POINTER_SIZE comes from access/detoast.h.
// ----------------------------------------------------------------------------

/* varatt.h: VARTAG_SIZE(tag) -- the size of an external (TOAST) pointer body. */
#[inline]
fn VARTAG_SIZE(tag: uint8) -> usize {
    if tag == VARTAG_INDIRECT {
        core::mem::size_of::<varatt_indirect>()
    } else if VARTAG_IS_EXPANDED(tag) {
        core::mem::size_of::<varatt_expanded>()
    } else if tag == VARTAG_ONDISK {
        core::mem::size_of::<varatt_external>()
    } else {
        /* C: TrapMacro(true, "unrecognized TOAST vartag") -> 0 */
        0
    }
}

/* varatt.h: in-memory indirect TOAST pointer (size only matters here). */
#[repr(C)]
#[derive(Clone, Copy)]
struct varatt_indirect {
    pointer: *mut varlena,
}

/* varatt.h: VARTAG_EXPANDED_* both point at an ExpandedObjectHeader. */
#[repr(C)]
#[derive(Clone, Copy)]
struct varatt_expanded {
    eohptr: *mut c_void,
}

/* access/detoast.h: TOAST_POINTER_SIZE = VARHDRSZ_EXTERNAL + sizeof(varatt_external). */
const TOAST_POINTER_SIZE: int32 =
    VARHDRSZ_EXTERNAL + core::mem::size_of::<varatt_external>() as int32;

/* varatt.h: VARTAG_EXTERNAL(PTR) == VARTAG_1B_E(PTR). */
#[inline]
unsafe fn VARTAG_EXTERNAL(ptr: *const c_char) -> uint8 {
    VARTAG_1B_E(ptr)
}

/* varatt.h: VARATT_IS_EXTERNAL_ONDISK(PTR). */
#[inline]
unsafe fn VARATT_IS_EXTERNAL_ONDISK(ptr: *const c_char) -> bool {
    VARATT_IS_EXTERNAL(ptr) && VARTAG_EXTERNAL(ptr) == VARTAG_ONDISK
}

/* varatt.h: VARSIZE_EXTERNAL(PTR) = VARHDRSZ_EXTERNAL + VARTAG_SIZE(VARTAG_EXTERNAL(PTR)). */
#[inline]
unsafe fn VARSIZE_EXTERNAL(ptr: *const c_char) -> usize {
    VARHDRSZ_EXTERNAL as usize + VARTAG_SIZE(VARTAG_EXTERNAL(ptr))
}

// ----------------------------------------------------------------------------
//   toast_helper.c
// ----------------------------------------------------------------------------

/*
 * Prepare to TOAST a tuple.
 *
 * tupleDesc, toast_values, and toast_isnull are required parameters; they
 * provide the necessary details about the tuple to be toasted.
 *
 * toast_oldvalues and toast_oldisnull should be NULL for a newly-inserted
 * tuple; for an update, they should describe the existing tuple.
 *
 * All of these arrays should have a length equal to tupleDesc->natts.
 *
 * On return, toast_flags and toast_attr will have been initialized.
 * toast_flags is just a single uint8, but toast_attr is a caller-provided
 * array with a length equal to tupleDesc->natts.  The caller need not perform
 * any initialization of the array before calling this function.
 *
 * # Safety
 * `ttc` must be a fully-populated ToastTupleContext as described above; the
 * value/isnull/attr arrays must each have length >= rd_att->natts.
 */
pub unsafe fn toast_tuple_init(ttc: *mut ToastTupleContext) {
    let tupleDesc: TupleDesc = (*(*ttc).ttc_rel).rd_att;
    let numAttrs: c_int = (*tupleDesc).natts;

    (*ttc).ttc_flags = 0;

    let mut i: c_int = 0;
    while i < numAttrs {
        let att = TupleDescAttr(tupleDesc, i);
        let attr = (*ttc).ttc_attr.add(i as usize);
        let value_slot = (*ttc).ttc_values.add(i as usize);

        let old_value: *mut varlena;
        let mut new_value: *mut varlena;

        (*attr).tai_colflags = 0;
        (*attr).tai_oldexternal = null_mut();
        (*attr).tai_compression = (*att).attcompression;

        if !(*ttc).ttc_oldvalues.is_null() {
            /*
             * For UPDATE get the old and new values of this attribute
             */
            old_value =
                DatumGetPointer(*(*ttc).ttc_oldvalues.add(i as usize)) as *mut varlena;
            new_value = DatumGetPointer(*value_slot) as *mut varlena;

            /*
             * If the old value is stored on disk, check if it has changed so
             * we have to delete it later.
             */
            if (*att).attlen == -1
                && !*(*ttc).ttc_oldisnull.add(i as usize)
                && VARATT_IS_EXTERNAL_ONDISK(old_value as *const c_char)
            {
                if *(*ttc).ttc_isnull.add(i as usize)
                    || !VARATT_IS_EXTERNAL_ONDISK(new_value as *const c_char)
                    || memcmp(
                        old_value as *const c_void,
                        new_value as *const c_void,
                        VARSIZE_EXTERNAL(old_value as *const c_char),
                    ) != 0
                {
                    /*
                     * The old external stored value isn't needed any more
                     * after the update
                     */
                    (*attr).tai_colflags |= TOASTCOL_NEEDS_DELETE_OLD;
                    (*ttc).ttc_flags |= TOAST_NEEDS_DELETE_OLD;
                } else {
                    /*
                     * This attribute isn't changed by this update so we reuse
                     * the original reference to the old value in the new
                     * tuple.
                     */
                    (*attr).tai_colflags |= TOASTCOL_IGNORE;
                    i += 1;
                    continue;
                }
            }
        } else {
            /*
             * For INSERT simply get the new value
             */
            new_value = DatumGetPointer(*value_slot) as *mut varlena;
        }

        /*
         * Handle NULL attributes
         */
        if *(*ttc).ttc_isnull.add(i as usize) {
            (*attr).tai_colflags |= TOASTCOL_IGNORE;
            (*ttc).ttc_flags |= TOAST_HAS_NULLS;
            i += 1;
            continue;
        }

        /*
         * Now look at varlena attributes
         */
        if (*att).attlen == -1 {
            /*
             * If the table's attribute says PLAIN always, force it so.
             */
            if (*att).attstorage == TYPSTORAGE_PLAIN {
                (*attr).tai_colflags |= TOASTCOL_IGNORE;
            }

            /*
             * We took care of UPDATE above, so any external value we find
             * still in the tuple must be someone else's that we cannot reuse
             * (this includes the case of an out-of-line in-memory datum).
             * Fetch it back (without decompression, unless we are forcing
             * PLAIN storage).  If necessary, we'll push it out as a new
             * external value below.
             */
            if VARATT_IS_EXTERNAL(new_value as *const c_char) {
                (*attr).tai_oldexternal = new_value;
                if (*att).attstorage == TYPSTORAGE_PLAIN {
                    new_value = detoast_attr(new_value);
                } else {
                    new_value = detoast_external_attr(new_value);
                }
                *value_slot = PointerGetDatum(new_value as *const c_void);
                (*attr).tai_colflags |= TOASTCOL_NEEDS_FREE;
                (*ttc).ttc_flags |= TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE;
            }

            /*
             * Remember the size of this attribute
             */
            (*attr).tai_size = VARSIZE_ANY(new_value as *const c_char) as int32;
        } else {
            /*
             * Not a varlena attribute, plain storage always
             */
            (*attr).tai_colflags |= TOASTCOL_IGNORE;
        }

        i += 1;
    }
}

/*
 * Find the largest varlena attribute that satisfies certain criteria.
 *
 * The relevant column must not be marked TOASTCOL_IGNORE, and if the
 * for_compression flag is passed as true, it must also not be marked
 * TOASTCOL_INCOMPRESSIBLE.
 *
 * The column must have attstorage EXTERNAL or EXTENDED if check_main is false,
 * and must have attstorage MAIN if check_main is true.
 *
 * The column must have a minimum size of MAXALIGN(TOAST_POINTER_SIZE); if not,
 * no benefit is to be expected by compressing it.
 *
 * The return value is the index of the biggest suitable column, or -1 if there
 * is none.
 *
 * # Safety
 * `ttc` must have been initialized by toast_tuple_init.
 */
pub unsafe fn toast_tuple_find_biggest_attribute(
    ttc: *mut ToastTupleContext,
    for_compression: bool,
    check_main: bool,
) -> c_int {
    let tupleDesc: TupleDesc = (*(*ttc).ttc_rel).rd_att;
    let numAttrs: c_int = (*tupleDesc).natts;
    let mut biggest_attno: c_int = -1;
    let mut biggest_size: int32 = MAXALIGN(TOAST_POINTER_SIZE as usize) as int32;
    let mut skip_colflags: int32 = TOASTCOL_IGNORE as int32;

    if for_compression {
        skip_colflags |= TOASTCOL_INCOMPRESSIBLE as int32;
    }

    let mut i: c_int = 0;
    while i < numAttrs {
        let att = TupleDescAttr(tupleDesc, i);
        let attr = (*ttc).ttc_attr.add(i as usize);
        let value = DatumGetPointer(*(*ttc).ttc_values.add(i as usize));

        if ((*attr).tai_colflags as int32 & skip_colflags) != 0 {
            i += 1;
            continue;
        }
        if VARATT_IS_EXTERNAL(value as *const c_char) {
            /* can't happen, toast_action would be PLAIN */
            i += 1;
            continue;
        }
        if for_compression && VARATT_IS_COMPRESSED(value as *const c_char) {
            i += 1;
            continue;
        }
        if check_main && (*att).attstorage != TYPSTORAGE_MAIN {
            i += 1;
            continue;
        }
        if !check_main
            && (*att).attstorage != TYPSTORAGE_EXTENDED
            && (*att).attstorage != TYPSTORAGE_EXTERNAL
        {
            i += 1;
            continue;
        }

        if (*attr).tai_size > biggest_size {
            biggest_attno = i;
            biggest_size = (*attr).tai_size;
        }

        i += 1;
    }

    biggest_attno
}

/*
 * Try compression for an attribute.
 *
 * If we find that the attribute is not compressible, mark it so.
 *
 * # Safety
 * `ttc` initialized by toast_tuple_init; `attribute` in range.
 */
pub unsafe fn toast_tuple_try_compression(ttc: *mut ToastTupleContext, attribute: c_int) {
    let value = (*ttc).ttc_values.add(attribute as usize);
    let attr = (*ttc).ttc_attr.add(attribute as usize);

    let new_value: Datum = toast_compress_datum(*value, (*attr).tai_compression);

    if !DatumGetPointer(new_value).is_null() {
        /* successful compression */
        if ((*attr).tai_colflags & TOASTCOL_NEEDS_FREE) != 0 {
            pfree(DatumGetPointer(*value) as *mut c_void);
        }
        *value = new_value;
        (*attr).tai_colflags |= TOASTCOL_NEEDS_FREE;
        (*attr).tai_size = VARSIZE(DatumGetPointer(*value) as *const c_char) as int32;
        (*ttc).ttc_flags |= TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE;
    } else {
        /* incompressible, ignore on subsequent compression passes */
        (*attr).tai_colflags |= TOASTCOL_INCOMPRESSIBLE;
    }
}

/*
 * Move an attribute to external storage.
 *
 * # Safety
 * `ttc` initialized by toast_tuple_init; `attribute` in range.  Reaches the
 * toast_save_datum stub (heap/relcache path not yet ported).
 */
pub unsafe fn toast_tuple_externalize(
    ttc: *mut ToastTupleContext,
    attribute: c_int,
    options: c_int,
) {
    let value = (*ttc).ttc_values.add(attribute as usize);
    let old_value: Datum = *value;
    let attr = (*ttc).ttc_attr.add(attribute as usize);

    (*attr).tai_colflags |= TOASTCOL_IGNORE;
    *value = toast_save_datum(
        (*ttc).ttc_rel as *mut c_void,
        old_value,
        (*attr).tai_oldexternal as *mut crate::c::varlena,
        options,
    );
    if ((*attr).tai_colflags & TOASTCOL_NEEDS_FREE) != 0 {
        pfree(DatumGetPointer(old_value) as *mut c_void);
    }
    (*attr).tai_colflags |= TOASTCOL_NEEDS_FREE;
    (*ttc).ttc_flags |= TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE;
}

/*
 * Perform appropriate cleanup after one tuple has been subjected to TOAST.
 *
 * # Safety
 * `ttc` initialized by toast_tuple_init.  The delete-old path reaches the
 * toast_delete_datum stub.
 */
pub unsafe fn toast_tuple_cleanup(ttc: *mut ToastTupleContext) {
    let tupleDesc: TupleDesc = (*(*ttc).ttc_rel).rd_att;
    let numAttrs: c_int = (*tupleDesc).natts;

    /*
     * Free allocated temp values
     */
    if ((*ttc).ttc_flags & TOAST_NEEDS_FREE) != 0 {
        let mut i: c_int = 0;
        while i < numAttrs {
            let attr = (*ttc).ttc_attr.add(i as usize);

            if ((*attr).tai_colflags & TOASTCOL_NEEDS_FREE) != 0 {
                pfree(DatumGetPointer(*(*ttc).ttc_values.add(i as usize)) as *mut c_void);
            }
            i += 1;
        }
    }

    /*
     * Delete external values from the old tuple
     */
    if ((*ttc).ttc_flags & TOAST_NEEDS_DELETE_OLD) != 0 {
        let mut i: c_int = 0;
        while i < numAttrs {
            let attr = (*ttc).ttc_attr.add(i as usize);

            if ((*attr).tai_colflags & TOASTCOL_NEEDS_DELETE_OLD) != 0 {
                toast_delete_datum(
                    (*ttc).ttc_rel as *mut c_void,
                    *(*ttc).ttc_oldvalues.add(i as usize),
                    false,
                );
            }
            i += 1;
        }
    }
}

/*
 * Check for external stored attributes and delete them from the secondary
 * relation.
 *
 * # Safety
 * `rel` is a valid relation; `values`/`isnull` have length >= rd_att->natts.
 * Reaches the toast_delete_datum stub for any on-disk external attribute.
 */
pub unsafe fn toast_delete_external(
    rel: Relation,
    values: *const Datum,
    isnull: *const bool,
    is_speculative: bool,
) {
    let tupleDesc: TupleDesc = (*rel).rd_att;
    let numAttrs: c_int = (*tupleDesc).natts;

    let mut i: c_int = 0;
    while i < numAttrs {
        if (*TupleDescCompactAttr(tupleDesc, i)).attlen == -1 {
            let value: Datum = *values.add(i as usize);

            if *isnull.add(i as usize) {
                /* skip */
            } else if VARATT_IS_EXTERNAL_ONDISK(DatumGetPointer(value) as *const c_char) {
                toast_delete_datum(rel as *mut c_void, value, is_speculative);
            }
        }
        i += 1;
    }
}

// ----------------------------------------------------------------------------
//   Tests
//
//   Self-contained: build a ToastAttrInfo array + a minimal ToastTupleContext
//   by hand and exercise the pure selection/classification logic that does not
//   need a live relation (toast_tuple_find_biggest_attribute).  We hand-build a
//   TupleDescData with the few attrs we need so TupleDescAttr resolves.
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::common::tupdesc::CreateTupleDesc;
    use crate::catalog::pg_attribute::FormData_pg_attribute;

    /* Build a default-zeroed Form_pg_attribute with the few fields we set. */
    unsafe fn make_attr(attlen: int16, attstorage: c_char) -> *mut FormData_pg_attribute {
        let att = palloc0(core::mem::size_of::<FormData_pg_attribute>())
            as *mut FormData_pg_attribute;
        (*att).attlen = attlen;
        (*att).attstorage = attstorage;
        (*att).attcompression = 0;
        (*att).attalign = crate::access::common::tupdesc::TYPALIGN_INT;
        att
    }

    /*
     * Build a TupleDesc over `specs` (attlen, attstorage), a RelationData whose
     * rd_att points at it, the values/isnull/attr arrays, and a context.
     * Returns (Box-kept relation, ctx) so callers can mutate attr flags/sizes.
     */
    unsafe fn build_ctx(
        specs: &[(int16, c_char)],
        sizes: &[int32],
        flags: &[uint8],
        is_external: &[bool],
        is_compressed: &[bool],
    ) -> (Box<crate::utils::rel::RelationData>, ToastTupleContext, Vec<ToastAttrInfo>, Vec<Datum>) {
        let n = specs.len();
        let mut attptrs: Vec<*mut FormData_pg_attribute> =
            specs.iter().map(|&(l, s)| make_attr(l, s)).collect();
        let tupdesc = CreateTupleDesc(n as c_int, attptrs.as_mut_ptr() as *mut _);

        // A zeroed RelationData with rd_att wired up is enough for the helpers,
        // which only ever read ttc_rel->rd_att.
        let mut rel: Box<crate::utils::rel::RelationData> =
            Box::new(core::mem::zeroed());
        rel.rd_att = tupdesc;

        // Build per-attr value datums: a tiny varlena (4B header) carrying the
        // external/compressed flag bits the selection code checks.  We allocate
        // a small buffer per attr and stamp a 4B header.
        let mut values: Vec<Datum> = Vec::with_capacity(n);
        for i in 0..n {
            let buf = palloc0(16) as *mut c_char;
            // 4B unaligned header: low 2 bits 00 => 4B; set length to 16.
            SET_VARSIZE(buf, 16);
            if is_compressed[i] {
                // 4B compressed marks the 0x02 bit via SET_VARSIZE_COMPRESSED.
                SET_VARSIZE_COMPRESSED(buf, 16);
            }
            if is_external[i] {
                // 1B external: byte 0 == 0x01.
                *buf = 0x01;
            }
            values.push(PointerGetDatum(buf as *const c_void));
        }

        let mut attrs: Vec<ToastAttrInfo> = (0..n)
            .map(|i| ToastAttrInfo {
                tai_oldexternal: null_mut(),
                tai_size: sizes[i],
                tai_colflags: flags[i],
                tai_compression: 0,
            })
            .collect();

        let ctx = ToastTupleContext {
            ttc_rel: rel.as_mut() as *mut crate::utils::rel::RelationData,
            ttc_values: values.as_mut_ptr(),
            ttc_isnull: null_mut(),
            ttc_oldvalues: null_mut(),
            ttc_oldisnull: null_mut(),
            ttc_flags: 0,
            ttc_attr: attrs.as_mut_ptr(),
        };

        (rel, ctx, attrs, values)
    }

    #[test]
    fn biggest_attribute_picks_largest_eligible_extended() {
        unsafe {
            // 3 varlena EXTENDED attrs, none ignored/external/compressed.
            // Sizes well above MAXALIGN(TOAST_POINTER_SIZE).
            let specs = [
                (-1i16, TYPSTORAGE_EXTENDED),
                (-1i16, TYPSTORAGE_EXTENDED),
                (-1i16, TYPSTORAGE_EXTENDED),
            ];
            let sizes = [100i32, 5000i32, 300i32];
            let flags = [0u8, 0u8, 0u8];
            let (mut rel, mut ctx, mut attrs, mut vals) =
                build_ctx(&specs, &sizes, &flags, &[false; 3], &[false; 3]);
            // Re-point ctx at the live owned arrays (build_ctx returns moved copies).
            ctx.ttc_attr = attrs.as_mut_ptr();
            ctx.ttc_values = vals.as_mut_ptr();
            ctx.ttc_rel = rel.as_mut() as *mut _;

            let idx = toast_tuple_find_biggest_attribute(&mut ctx, false, false);
            assert_eq!(idx, 1, "attr 1 (size 5000) is the largest eligible");
        }
    }

    #[test]
    fn biggest_attribute_skips_ignored_and_wrong_storage() {
        unsafe {
            // attr0: EXTENDED but TOASTCOL_IGNORE -> skipped.
            // attr1: PLAIN storage -> skipped (not EXTENDED/EXTERNAL).
            // attr2: EXTERNAL, eligible -> chosen even though smaller than attr0.
            let specs = [
                (-1i16, TYPSTORAGE_EXTENDED),
                (-1i16, TYPSTORAGE_PLAIN),
                (-1i16, TYPSTORAGE_EXTERNAL),
            ];
            let sizes = [9000i32, 9000i32, 400i32];
            let flags = [TOASTCOL_IGNORE, 0u8, 0u8];
            let (mut rel, mut ctx, mut attrs, mut vals) =
                build_ctx(&specs, &sizes, &flags, &[false; 3], &[false; 3]);
            ctx.ttc_attr = attrs.as_mut_ptr();
            ctx.ttc_values = vals.as_mut_ptr();
            ctx.ttc_rel = rel.as_mut() as *mut _;

            let idx = toast_tuple_find_biggest_attribute(&mut ctx, false, false);
            assert_eq!(idx, 2, "only attr 2 is eligible");
        }
    }

    #[test]
    fn biggest_attribute_for_compression_skips_compressed_and_incompressible() {
        unsafe {
            // attr0: EXTENDED, already COMPRESSED -> skipped under for_compression.
            // attr1: EXTENDED, INCOMPRESSIBLE flag -> skipped under for_compression.
            // attr2: EXTENDED, plain in-line, eligible -> chosen.
            let specs = [
                (-1i16, TYPSTORAGE_EXTENDED),
                (-1i16, TYPSTORAGE_EXTENDED),
                (-1i16, TYPSTORAGE_EXTENDED),
            ];
            let sizes = [9000i32, 8000i32, 1000i32];
            let flags = [0u8, TOASTCOL_INCOMPRESSIBLE, 0u8];
            let (mut rel, mut ctx, mut attrs, mut vals) =
                build_ctx(&specs, &sizes, &flags, &[false; 3], &[true, false, false]);
            ctx.ttc_attr = attrs.as_mut_ptr();
            ctx.ttc_values = vals.as_mut_ptr();
            ctx.ttc_rel = rel.as_mut() as *mut _;

            let idx = toast_tuple_find_biggest_attribute(&mut ctx, true, false);
            assert_eq!(idx, 2, "only the non-compressed, compressible attr qualifies");
        }
    }

    #[test]
    fn biggest_attribute_too_small_returns_minus_one() {
        unsafe {
            // Single eligible attr whose size is below MAXALIGN(TOAST_POINTER_SIZE).
            let specs = [(-1i16, TYPSTORAGE_EXTENDED)];
            let sizes = [4i32];
            let flags = [0u8];
            let (mut rel, mut ctx, mut attrs, mut vals) =
                build_ctx(&specs, &sizes, &flags, &[false], &[false]);
            ctx.ttc_attr = attrs.as_mut_ptr();
            ctx.ttc_values = vals.as_mut_ptr();
            ctx.ttc_rel = rel.as_mut() as *mut _;

            let idx = toast_tuple_find_biggest_attribute(&mut ctx, false, false);
            assert_eq!(idx, -1, "nothing exceeds the MAXALIGN(TOAST_POINTER_SIZE) floor");
        }
    }
}

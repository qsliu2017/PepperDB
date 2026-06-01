//! Translation of postgres/src/backend/access/common/attmap.c
//! (merged with postgres/src/include/access/attmap.h).
//!
//! Attribute mapping support.
//!
//! This file provides utility routines to build and manage attribute
//! mappings by comparing input and output TupleDescs.  Such mappings
//! are typically used by DDL operating on inheritance and partition trees
//! to do a conversion between rowtypes logically equivalent but with
//! columns in a different order, taking into account dropped columns.
//! They are also used by the tuple conversion routines in tupconvert.c.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping:
//!   postgres.h            -> crate::prelude (Datum, c-types, palloc0/pfree,
//!                            elog!/ereport!/errmsg!/Assert!, null_mut, NameStr, ...)
//!   access/attnum.h       -> AttrNumber from crate::nodes::primnodes;
//!                            InvalidAttrNumber defined locally (also lives in
//!                            crate::nodes::makefuncs)
//!   access/tupdesc.h      -> crate::access::common::tupdesc (TupleDesc,
//!                            TupleDescAttr, TupleDescCompactAttr, CompactAttribute,
//!                            TupleDescData.natts)
//!   utils/builtins.h      -> NameStr comparison via strcmp over
//!                            crate::c::NameStr; format_type_with_typemod /
//!                            format_type_be (utils/adt/format_type.c) are NOT yet
//!                            ported - see the error-path note below.
//!   Form_pg_attribute fields (attname/atttypid/atttypmod/attisdropped/attnum) ->
//!                            crate::catalog::pg_attribute::FormData_pg_attribute.
//!
//! WHAT IS REAL vs STUBBED:
//!   REAL: make_attrmap, free_attrmap, build_attrmap_by_position,
//!     build_attrmap_by_name, build_attrmap_by_name_if_req, check_attrmap_match.
//!   The type/typmod-mismatch and "could not find/convert" error paths are fully
//!     real control flow (they ereport!(ERROR), which PANICs).  The C errdetail
//!     text uses format_type_with_typemod / format_type_be to render type names;
//!     those formatters (utils/adt/format_type.c) are not yet ported, so the
//!     detail text below substitutes the raw type OID / typmod in place of the
//!     rendered type name.  No control-flow behavior is stubbed.

use crate::prelude::*;

use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr, TupleDescCompactAttr};
use crate::nodes::primnodes::AttrNumber;

use core::ffi::{c_char, c_int, c_void};
use core::mem::size_of;

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

/* errcodes.h classification (errcode() shim ignores the value). */
// TODO(pg-port): ERRCODE_DATATYPE_MISMATCH from utils/errcodes.h.
const ERRCODE_DATATYPE_MISMATCH: c_int = 0;

/*
 * access/attnum.h: InvalidAttrNumber (zero attribute number).  Mirrors
 * crate::nodes::makefuncs::InvalidAttrNumber; defined locally to keep this
 * module self-contained (attmap.h pulls in access/attnum.h).
 */
pub const InvalidAttrNumber: AttrNumber = 0;

// ============================================================================
//   attmap.h
// ============================================================================

/*
 * Attribute mapping structure
 *
 * This maps attribute numbers between a pair of relations, designated
 * 'input' and 'output' (most typically inheritance parent and child
 * relations), whose common columns may have different attribute numbers.
 * Such difference may arise due to the columns being ordered differently
 * in the two relations or the two relations having dropped columns at
 * different positions.
 *
 * 'maplen' is set to the number of attributes of the 'output' relation,
 * taking into account any of its dropped attributes, with the corresponding
 * elements of the 'attnums' array set to 0.
 */
#[repr(C)]
pub struct AttrMap {
    pub attnums: *mut AttrNumber,
    pub maplen: c_int,
}

// ============================================================================
//   attmap.c
// ============================================================================

/*
 * make_attrmap
 *
 * Utility routine to allocate an attribute map in the current memory
 * context.
 */
pub unsafe fn make_attrmap(maplen: c_int) -> *mut AttrMap {
    let res = palloc0(size_of::<AttrMap>()) as *mut AttrMap;
    (*res).maplen = maplen;
    (*res).attnums = palloc0(size_of::<AttrNumber>() * maplen as usize) as *mut AttrNumber;
    res
}

/*
 * free_attrmap
 *
 * Utility routine to release an attribute map.
 */
pub unsafe fn free_attrmap(map: *mut AttrMap) {
    pfree((*map).attnums as *mut c_void);
    pfree(map as *mut c_void);
}

/*
 * build_attrmap_by_position
 *
 * Return a palloc'd bare attribute map for tuple conversion, matching input
 * and output columns by position.  Dropped columns are ignored in both input
 * and output, marked as 0.  This is normally a subroutine for
 * convert_tuples_by_position in tupconvert.c, but it can be used standalone.
 *
 * Note: the errdetail messages speak of indesc as the "returned" rowtype,
 * outdesc as the "expected" rowtype.  This is okay for current uses but
 * might need generalization in future.
 *
 * `msg` is C's variadic format string used by errmsg_internal("%s", _(msg));
 * here it is rendered as a plain &str message prefix.
 *
 * # Safety
 * `indesc` and `outdesc` must be live TupleDescs.
 */
pub unsafe fn build_attrmap_by_position(
    indesc: TupleDesc,
    outdesc: TupleDesc,
    msg: &str,
) -> *mut AttrMap {
    let n: c_int;
    let mut j: c_int; /* j is next physical input attribute */
    let mut same: bool;

    /*
     * The length is computed as the number of attributes of the expected
     * rowtype as it includes dropped attributes in its count.
     */
    n = (*outdesc).natts;
    let attr_map = make_attrmap(n);

    j = 0; /* j is next physical input attribute */
    let mut nincols: c_int = 0; /* these count non-dropped attributes */
    let mut noutcols: c_int = 0;
    same = true;
    let mut i = 0;
    while i < n {
        let outatt = TupleDescAttr(outdesc, i);

        if (*outatt).attisdropped {
            /* attrMap->attnums[i] is already 0 */
            i += 1;
            continue;
        }
        noutcols += 1;
        while j < (*indesc).natts {
            let inatt = TupleDescAttr(indesc, j);

            if (*inatt).attisdropped {
                j += 1;
                continue;
            }
            nincols += 1;

            /* Found matching column, now check type */
            if (*outatt).atttypid != (*inatt).atttypid
                || ((*outatt).atttypmod != (*inatt).atttypmod && (*outatt).atttypmod >= 0)
            {
                let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
                ereport!(
                    ERROR,
                    errmsg!(
                        "{}: Returned type (oid {}, typmod {}) does not match \
                         expected type (oid {}, typmod {}) in column \"{}\" (position {}).",
                        msg,
                        (*inatt).atttypid,
                        (*inatt).atttypmod,
                        (*outatt).atttypid,
                        (*outatt).atttypmod,
                        cstr_to_string(NameStr(&(*outatt).attname)),
                        noutcols
                    )
                );
            }
            *(*attr_map).attnums.add(i as usize) = (j + 1) as AttrNumber;
            j += 1;
            break;
        }
        if *(*attr_map).attnums.add(i as usize) == 0 {
            same = false; /* we'll complain below */
        }
        i += 1;
    }

    /* Check for unused input columns */
    while j < (*indesc).natts {
        if (*TupleDescCompactAttr(indesc, j)).attisdropped {
            j += 1;
            continue;
        }
        nincols += 1;
        same = false; /* we'll complain below */
        j += 1;
    }

    /* Report column count mismatch using the non-dropped-column counts */
    if !same {
        let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
        ereport!(
            ERROR,
            errmsg!(
                "{}: Number of returned columns ({}) does not match expected column count ({}).",
                msg,
                nincols,
                noutcols
            )
        );
    }

    /* Check if the map has a one-to-one match */
    if check_attrmap_match(indesc, outdesc, attr_map) {
        /* Runtime conversion is not needed */
        free_attrmap(attr_map);
        return null_mut();
    }

    attr_map
}

/*
 * build_attrmap_by_name
 *
 * Return a palloc'd bare attribute map for tuple conversion, matching input
 * and output columns by name.  (Dropped columns are ignored in both input and
 * output.)  This is normally a subroutine for convert_tuples_by_name in
 * tupconvert.c, but can be used standalone.
 *
 * If 'missing_ok' is true, a column from 'outdesc' not being present in
 * 'indesc' is not flagged as an error; AttrMap.attnums[] entry for such an
 * outdesc column will be 0 in that case.
 *
 * # Safety
 * `indesc` and `outdesc` must be live TupleDescs.
 */
pub unsafe fn build_attrmap_by_name(
    indesc: TupleDesc,
    outdesc: TupleDesc,
    missing_ok: bool,
) -> *mut AttrMap {
    let outnatts: c_int;
    let innatts: c_int;
    let mut nextindesc: c_int = -1;

    outnatts = (*outdesc).natts;
    innatts = (*indesc).natts;

    let attr_map = make_attrmap(outnatts);
    let mut i = 0;
    while i < outnatts {
        let outatt = TupleDescAttr(outdesc, i);

        if (*outatt).attisdropped {
            /* attrMap->attnums[i] is already 0 */
            i += 1;
            continue;
        }
        let attname = NameStr(&(*outatt).attname);
        let atttypid = (*outatt).atttypid;
        let atttypmod = (*outatt).atttypmod;

        /*
         * Now search for an attribute with the same name in the indesc. It
         * seems likely that a partitioned table will have the attributes in
         * the same order as the partition, so the search below is optimized
         * for that case.  It is possible that columns are dropped in one of
         * the relations, but not the other, so we use the 'nextindesc'
         * counter to track the starting point of the search.  If the inner
         * loop encounters dropped columns then it will have to skip over
         * them, but it should leave 'nextindesc' at the correct position for
         * the next outer loop.
         */
        let mut j = 0;
        while j < innatts {
            nextindesc += 1;
            if nextindesc >= innatts {
                nextindesc = 0;
            }

            let inatt = TupleDescAttr(indesc, nextindesc);
            if (*inatt).attisdropped {
                j += 1;
                continue;
            }
            if strcmp(attname, NameStr(&(*inatt).attname)) == 0 {
                /* Found it, check type */
                if atttypid != (*inatt).atttypid || atttypmod != (*inatt).atttypmod {
                    let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
                    ereport!(
                        ERROR,
                        errmsg!(
                            "could not convert row type: Attribute \"{}\" of type (oid {}) does \
                             not match corresponding attribute of type (oid {}).",
                            cstr_to_string(attname),
                            (*outdesc).tdtypeid,
                            (*indesc).tdtypeid
                        )
                    );
                }
                *(*attr_map).attnums.add(i as usize) = (*inatt).attnum;
                break;
            }
            j += 1;
        }
        if *(*attr_map).attnums.add(i as usize) == 0 && !missing_ok {
            let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
            ereport!(
                ERROR,
                errmsg!(
                    "could not convert row type: Attribute \"{}\" of type (oid {}) does not \
                     exist in type (oid {}).",
                    cstr_to_string(attname),
                    (*outdesc).tdtypeid,
                    (*indesc).tdtypeid
                )
            );
        }
        i += 1;
    }
    attr_map
}

/*
 * build_attrmap_by_name_if_req
 *
 * Returns mapping created by build_attrmap_by_name, or NULL if no
 * conversion is required.  This is a convenience routine for
 * convert_tuples_by_name() in tupconvert.c and other functions, but it
 * can be used standalone.
 *
 * # Safety
 * `indesc` and `outdesc` must be live TupleDescs.
 */
pub unsafe fn build_attrmap_by_name_if_req(
    indesc: TupleDesc,
    outdesc: TupleDesc,
    missing_ok: bool,
) -> *mut AttrMap {
    /* Verify compatibility and prepare attribute-number map */
    let attr_map = build_attrmap_by_name(indesc, outdesc, missing_ok);

    /* Check if the map has a one-to-one match */
    if check_attrmap_match(indesc, outdesc, attr_map) {
        /* Runtime conversion is not needed */
        free_attrmap(attr_map);
        return null_mut();
    }

    attr_map
}

/*
 * check_attrmap_match
 *
 * Check to see if the map is a one-to-one match, in which case we need
 * not to do a tuple conversion, and the attribute map is not necessary.
 *
 * # Safety
 * `indesc` and `outdesc` must be live TupleDescs and `attr_map` a live AttrMap.
 */
unsafe fn check_attrmap_match(
    indesc: TupleDesc,
    outdesc: TupleDesc,
    attr_map: *mut AttrMap,
) -> bool {
    /* no match if attribute numbers are not the same */
    if (*indesc).natts != (*outdesc).natts {
        return false;
    }

    let mut i = 0;
    while i < (*attr_map).maplen {
        let inatt = TupleDescCompactAttr(indesc, i);

        /*
         * If the input column has a missing attribute, we need a conversion.
         */
        if (*inatt).atthasmissing {
            return false;
        }

        if *(*attr_map).attnums.add(i as usize) == (i + 1) as AttrNumber {
            i += 1;
            continue;
        }

        let outatt = TupleDescCompactAttr(outdesc, i);

        /*
         * If it's a dropped column and the corresponding input column is also
         * dropped, we don't need a conversion.  However, attlen and
         * attalignby must agree.
         */
        if *(*attr_map).attnums.add(i as usize) == 0
            && (*inatt).attisdropped
            && (*inatt).attlen == (*outatt).attlen
            && (*inatt).attalignby == (*outatt).attalignby
        {
            i += 1;
            continue;
        }

        return false;
    }

    true
}

/*
 * Render a NUL-terminated C string (as returned by NameStr) into an owned
 * Rust String, for embedding in the ereport! detail text.  The C code passes
 * the bare `char *` to errdetail's %s; here we materialize it for `{}`.
 *
 * # Safety
 * `p` must be a valid NUL-terminated C string pointer.
 */
unsafe fn cstr_to_string(p: *const c_char) -> std::string::String {
    let mut len = 0usize;
    while *p.add(len) != 0 {
        len += 1;
    }
    let bytes = core::slice::from_raw_parts(p as *const u8, len);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::common::tupdesc::{CreateTemplateTupleDesc, TupleDescInitBuiltinEntry};
    use crate::catalog::pg_type_d::INT4OID;

    // Build a 2-column INT4 TupleDesc with the given column names.
    unsafe fn make_desc(name1: &str, name2: &str) -> TupleDesc {
        let desc = CreateTemplateTupleDesc(2);
        let c1 = std::ffi::CString::new(name1).unwrap();
        let c2 = std::ffi::CString::new(name2).unwrap();
        TupleDescInitBuiltinEntry(desc, 1, c1.as_ptr(), INT4OID, -1, 0);
        TupleDescInitBuiltinEntry(desc, 2, c2.as_ptr(), INT4OID, -1, 0);
        desc
    }

    #[test]
    fn by_name_swaps_columns() {
        unsafe {
            // indesc columns: ("a","b"); outdesc columns: ("b","a").
            // For each output column we find the matching input column by name:
            //   out[0]="b" -> in attno 2; out[1]="a" -> in attno 1.
            // => map [2, 1] (a swap).
            let indesc = make_desc("a", "b");
            let outdesc = make_desc("b", "a");

            let map = build_attrmap_by_name(indesc, outdesc, false);
            assert!(!map.is_null());
            assert_eq!((*map).maplen, 2);
            assert_eq!(*(*map).attnums.add(0), 2);
            assert_eq!(*(*map).attnums.add(1), 1);
            free_attrmap(map);
        }
    }

    #[test]
    fn by_name_identity() {
        unsafe {
            // Identical name order -> identity map [1, 2].
            let indesc = make_desc("a", "b");
            let outdesc = make_desc("a", "b");

            let map = build_attrmap_by_name(indesc, outdesc, false);
            assert!(!map.is_null());
            assert_eq!(*(*map).attnums.add(0), 1);
            assert_eq!(*(*map).attnums.add(1), 2);
            free_attrmap(map);
        }
    }

    #[test]
    fn by_position_identity_returns_null() {
        unsafe {
            // build_attrmap_by_position on two identical descs yields the
            // identity map [1,2]; check_attrmap_match then frees it and returns
            // NULL (no runtime conversion needed).
            let indesc = make_desc("a", "b");
            let outdesc = make_desc("a", "b");

            let map = build_attrmap_by_position(indesc, outdesc, "conversion check");
            // Identity => NULL.
            assert!(map.is_null());
        }
    }

    #[test]
    #[should_panic]
    fn by_position_type_mismatch_errors() {
        unsafe {
            // Same names/positions but a type mismatch in column 2 (INT4 vs INT8)
            // must drive the atttypid check to ereport!(ERROR) (PANIC).
            use crate::catalog::pg_type_d::INT8OID;
            let indesc = CreateTemplateTupleDesc(2);
            let outdesc = CreateTemplateTupleDesc(2);
            TupleDescInitBuiltinEntry(indesc, 1, c"a".as_ptr(), INT4OID, -1, 0);
            TupleDescInitBuiltinEntry(indesc, 2, c"b".as_ptr(), INT8OID, -1, 0);
            TupleDescInitBuiltinEntry(outdesc, 1, c"a".as_ptr(), INT4OID, -1, 0);
            TupleDescInitBuiltinEntry(outdesc, 2, c"b".as_ptr(), INT4OID, -1, 0);
            let _ = build_attrmap_by_position(indesc, outdesc, "conversion check");
        }
    }

    #[test]
    #[should_panic]
    fn by_name_missing_column_errors() {
        unsafe {
            // outdesc has a column ("c") absent from indesc; missing_ok=false
            // must ereport!(ERROR) (which PANICs).
            let indesc = make_desc("a", "b");
            let outdesc = make_desc("a", "c");

            let _ = build_attrmap_by_name(indesc, outdesc, false);
        }
    }

    #[test]
    fn by_name_missing_column_ok_when_missing_ok() {
        unsafe {
            // Same mismatch but missing_ok=true => the absent column maps to 0.
            let indesc = make_desc("a", "b");
            let outdesc = make_desc("a", "c");

            let map = build_attrmap_by_name(indesc, outdesc, true);
            assert!(!map.is_null());
            assert_eq!(*(*map).attnums.add(0), 1); // "a" -> in attno 1
            assert_eq!(*(*map).attnums.add(1), 0); // "c" absent -> 0
            free_attrmap(map);
        }
    }
}

//! Index tuple accessor and mutator routines. Translated from
//! backend/access/common/indextuple.c.
//!
//! An index tuple is `IndexTupleData` (an 8-byte header: heap TID + the packed
//! `t_info` word), an optional fixed-size null bitmap (`IndexAttributeBitMapData`,
//! present when the HasNulls flag is set), MAXALIGN padding to the data offset,
//! then the attribute data laid out exactly like a heap tuple's data area (so
//! [`crate::backend::access::common::heaptuple::heap_fill_tuple`] builds it). The
//! `t_info` word packs HasNulls (bit 15), HasVarwidth (bit 14), an AM-reserved
//! bit (13), and the 13-bit size, mirrored byte-for-byte from C.
//!
//! Soundness: the block is allocated MAXALIGN(8)-aligned, the
//! `*mut u8 -> *mut IndexTupleData` overlay cast is sound (header align 4
//! divides 8), and all field access goes through the unaligned
//! `fetch_att`/varlena accessors (see heaptuple.rs), so no `cast_ptr_alignment`
//! allow is needed.

use core::alloc::Layout;

use crate::access::itup::{
    IndexAttributeBitMapData, IndexInfoFindDataOffset, IndexTuple, IndexTupleData, IndexTupleSize,
    INDEX_NULL_MASK, INDEX_SIZE_MASK, INDEX_VAR_MASK,
};
use crate::access::tupdesc::TupleDescData;
use crate::access::tupmacs::{
    att_addlength_pointer, att_isnull, att_nominal_alignby, att_pointer_alignby, fetch_att,
};
use crate::backend::access::common::heaptuple::heap_compute_data_size;
use crate::c::{bits8, MAXALIGN};
use crate::pg_config_manual::INDEX_MAX_KEYS;
use crate::postgres::Datum;
use crate::utils::elog::ERROR;

// The HEAP_HASVARWIDTH/HEAP_HASEXTERNAL bits returned by heap_fill_tuple's
// tupmask. heap_fill_tuple sets these; index_form_tuple reinterprets them.
use crate::access::htup_details::HEAP_HASVARWIDTH;

/// Allocate a zeroed index-tuple block of `size` bytes (C
/// `MemoryContextAllocZero`). Reclaimed by [`free_index_tuple`].
fn alloc_index_tuple(size: usize) -> *mut u8 {
    let n = size.max(1);
    let layout = Layout::from_size_align(n, crate::pg_config::MAXIMUM_ALIGNOF)
        .unwrap_or_else(|_| Layout::new::<u8>());
    // SAFETY: non-zero size; alloc_zeroed returns zeroed memory or null (OOM).
    let p = unsafe { std::alloc::alloc_zeroed(layout) };
    if p.is_null() {
        crate::elog!(ERROR, "out of memory forming index tuple".to_string());
    }
    p
}

/// Reclaim an index tuple allocated by [`alloc_index_tuple`]. `size` must be the
/// `IndexTupleSize` the tuple was allocated with.
///
/// SAFETY: `tup` came from `alloc_index_tuple(size)` and was not yet freed.
unsafe fn free_index_tuple(tup: IndexTuple, size: usize) {
    if tup.is_null() {
        return;
    }
    let n = size.max(1);
    let layout = Layout::from_size_align(n, crate::pg_config::MAXIMUM_ALIGNOF)
        .unwrap_or_else(|_| Layout::new::<u8>());
    std::alloc::dealloc(tup.cast::<u8>(), layout);
}

/// `index_form_tuple`: construct an index tuple in (conceptually) the current
/// memory context. Delegates to [`index_form_tuple_context`].
pub fn index_form_tuple(
    tuple_descriptor: &TupleDescData,
    values: &[Datum],
    isnull: &[bool],
) -> IndexTuple {
    index_form_tuple_context(tuple_descriptor, values, isnull)
}

/// `index_form_tuple_context`: build an index tuple from `values`/`isnull`.
///
/// The TOAST_INDEX_HACK (detoast/compress external varlenas so the index tuple
/// does not depend on outside storage) is deferred until the detoast/toast
/// subsystem lands; the on-disk header + data layout itself is complete. External
/// varlenas therefore flow through unchanged for now (asserted-against in C).
pub fn index_form_tuple_context(
    tuple_descriptor: &TupleDescData,
    values: &[Datum],
    isnull: &[bool],
) -> IndexTuple {
    let number_of_attributes = tuple_descriptor.natts;

    if number_of_attributes as usize > INDEX_MAX_KEYS {
        crate::ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_TOO_MANY_COLUMNS).errmsg(format!(
                "number of index columns ({number_of_attributes}) exceeds limit ({INDEX_MAX_KEYS})"
            ));
        });
    }

    let natts = number_of_attributes as usize;
    let hasnull = isnull.iter().take(natts).any(|&n| n);

    let mut infomask: u16 = 0;
    if hasnull {
        infomask |= INDEX_NULL_MASK;
    }

    let hoff = IndexInfoFindDataOffset(infomask);
    let data_size = heap_compute_data_size(tuple_descriptor, values, isnull);
    let mut size = hoff + data_size;
    size = MAXALIGN(size); // be conservative

    let tp = alloc_index_tuple(size);
    #[allow(
        clippy::cast_ptr_alignment,
        reason = "sound overlay: tp is alloc_zeroed at MAXIMUM_ALIGNOF (8); IndexTupleData's align (2) divides 8"
    )]
    let tuple = tp.cast::<IndexTupleData>();

    let mut tupmask: u16 = 0;
    // SAFETY: `tp` is a zeroed block of `size` bytes; the data area at `hoff`
    // holds `data_size` bytes and the null bitmap (if any) follows the 8-byte
    // header.
    unsafe {
        let data_ptr = tp.add(hoff);
        if hasnull {
            let bits_ptr = tp.add(core::mem::size_of::<IndexTupleData>());
            let bitmap_len = core::mem::size_of::<IndexAttributeBitMapData>();
            let bits = core::slice::from_raw_parts_mut(bits_ptr, bitmap_len);
            crate::backend::access::common::heaptuple::heap_fill_tuple(
                tuple_descriptor,
                values,
                isnull,
                data_ptr,
                data_size,
                &mut tupmask,
                Some(bits),
            );
        } else {
            crate::backend::access::common::heaptuple::heap_fill_tuple(
                tuple_descriptor,
                values,
                isnull,
                data_ptr,
                data_size,
                &mut tupmask,
                None,
            );
        }
    }

    // heap_fill_tuple set a heap tupmask; map the only relevant bit across.
    if tupmask & HEAP_HASVARWIDTH != 0 {
        infomask |= INDEX_VAR_MASK;
    }

    // Make sure the size fits in t_info's 13-bit size field.
    if (size & INDEX_SIZE_MASK as usize) != size {
        crate::ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_PROGRAM_LIMIT_EXCEEDED).errmsg(format!(
                "index row requires {size} bytes, maximum size is {INDEX_SIZE_MASK}"
            ));
        });
    }

    infomask |= size as u16;

    // SAFETY: `tuple` points at the freshly-allocated header.
    unsafe {
        (*tuple).t_info = infomask;
    }
    tuple
}

/// `nocache_index_getattr`: fetch a non-null attribute (1-based `attnum`) of an
/// index tuple that lacks a usable cached offset. Mirrors
/// [`heaptuple::nocachegetattr`], minus the missing-value path (index tuples
/// never have missing columns).
///
/// As in heaptuple, the shared descriptor's `attcacheoff` cache is not persisted
/// (the descriptor is an immutable `Arc`); offsets are recomputed, which is
/// behaviorally identical.
///
/// SAFETY: `tup` must point at a valid index-tuple block; `attnum` valid and
/// non-null.
pub unsafe fn nocache_index_getattr(
    tup: IndexTuple,
    attnum: i32,
    tuple_desc: &TupleDescData,
) -> Datum {
    let t_info = (*tup).t_info;
    let has_nulls = (t_info & INDEX_NULL_MASK) != 0;
    let has_varwidth = (t_info & INDEX_VAR_MASK) != 0;
    let data_off = IndexInfoFindDataOffset(t_info);

    let attnum = (attnum - 1) as usize;
    let mut slow = false;

    // The null bitmap (if any) is just after the fixed 8-byte header.
    let bp = tup
        .cast::<u8>()
        .add(core::mem::size_of::<IndexTupleData>());

    if has_nulls {
        let byte = attnum >> 3;
        let finalbit = attnum & 0x07;
        let bp_slice = core::slice::from_raw_parts(bp, byte + 1);
        if (!bp_slice[byte]) & (((1u32 << finalbit) - 1) as u8) != 0 {
            slow = true;
        } else {
            for &b in bp_slice.iter().take(byte) {
                if b != 0xFF {
                    slow = true;
                    break;
                }
            }
        }
    }

    let tp = tup.cast::<u8>().add(data_off);

    if !slow && has_varwidth {
        for j in 0..=attnum {
            if tuple_desc.compact_attr(j).attlen <= 0 {
                slow = true;
                break;
            }
        }
    }

    let mut cur = 0usize;
    let off = if slow {
        let mut i = 0usize;
        loop {
            let att_attlen = tuple_desc.compact_attr(i).attlen;
            let att_alignby = tuple_desc.compact_attr(i).attalignby as usize;

            if has_nulls && att_isnull(i as i32, core::slice::from_raw_parts(bp, (i >> 3) + 1)) {
                i += 1;
                continue;
            }

            if att_attlen == -1 {
                cur = att_pointer_alignby(cur, att_alignby, -1, tp.add(cur));
            } else {
                cur = att_nominal_alignby(cur, att_alignby);
            }

            if i == attnum {
                break;
            }

            cur = att_addlength_pointer(cur, i32::from(att_attlen), tp.add(cur));
            i += 1;
        }
        cur
    } else {
        for j in 0..attnum {
            let att = tuple_desc.compact_attr(j);
            cur = att_nominal_alignby(cur, att.attalignby as usize);
            cur += att.attlen as usize;
        }
        att_nominal_alignby(cur, tuple_desc.compact_attr(attnum).attalignby as usize)
    };

    let att = tuple_desc.compact_attr(attnum);
    fetch_att(tp.add(off), att.attbyval, i32::from(att.attlen))
}

/// `index_deform_tuple`: decode an index tuple into `(values, isnull)` slices.
/// Like [`heaptuple::heap_deform_tuple`] but index tuples never have missing
/// columns.
///
/// SAFETY: `tup` must point at a valid index-tuple block; the slices must have
/// at least `tuple_descriptor.natts` entries.
pub unsafe fn index_deform_tuple(
    tup: IndexTuple,
    tuple_descriptor: &TupleDescData,
    values: &mut [Datum],
    isnull: &mut [bool],
) {
    let t_info = (*tup).t_info;
    let hasnulls = (t_info & INDEX_NULL_MASK) != 0;
    let bp = tup
        .cast::<u8>()
        .add(core::mem::size_of::<IndexTupleData>());
    let tp = tup.cast::<u8>().add(IndexInfoFindDataOffset(t_info));

    index_deform_tuple_internal(tuple_descriptor, values, isnull, tp, bp, hasnulls);
}

/// `index_deform_tuple_internal`: decode the data area at `tp` (null bitmap at
/// `bp`) into `(values, isnull)`, without assuming a specific header layout.
///
/// SAFETY: `tp`/`bp` must point at the data area / null bitmap of a valid tuple.
pub unsafe fn index_deform_tuple_internal(
    tuple_descriptor: &TupleDescData,
    values: &mut [Datum],
    isnull: &mut [bool],
    tp: *const u8,
    bp: *const u8,
    hasnulls: bool,
) {
    let natts = tuple_descriptor.natts as usize;
    crate::assert!(natts <= INDEX_MAX_KEYS);

    let mut off = 0usize;

    for attnum in 0..natts {
        let attlen = tuple_descriptor.compact_attr(attnum).attlen;
        let attbyval = tuple_descriptor.compact_attr(attnum).attbyval;
        let attalignby = tuple_descriptor.compact_attr(attnum).attalignby as usize;

        if hasnulls
            && att_isnull(
                attnum as i32,
                core::slice::from_raw_parts(bp, (attnum >> 3) + 1),
            )
        {
            values[attnum] = Datum(0);
            isnull[attnum] = true;
            continue;
        }

        isnull[attnum] = false;

        if attlen == -1 {
            off = att_pointer_alignby(off, attalignby, -1, tp.add(off));
        } else {
            off = att_nominal_alignby(off, attalignby);
        }

        values[attnum] = fetch_att(tp.add(off), attbyval, i32::from(attlen));
        off = att_addlength_pointer(off, i32::from(attlen), tp.add(off));
    }
}

/// `CopyIndexTuple`: return a heap-allocated copy of an index tuple.
///
/// SAFETY: `source` must point at a valid index-tuple block.
pub unsafe fn copy_index_tuple(source: IndexTuple) -> IndexTuple {
    let size = IndexTupleSize(&*source);
    let result = alloc_index_tuple(size);
    core::ptr::copy_nonoverlapping(source.cast::<u8>(), result, size);
    #[allow(
        clippy::cast_ptr_alignment,
        reason = "sound overlay: result is alloc_zeroed at MAXIMUM_ALIGNOF (8); IndexTupleData's align (2) divides 8"
    )]
    result.cast::<IndexTupleData>()
}

/// `index_truncate_tuple`: return a copy of `source` keeping only its first
/// `leavenatts` attributes (used by nbtree suffix truncation). Guaranteed no
/// larger than the original.
///
/// SAFETY: `source` must point at a valid index-tuple block.
pub unsafe fn index_truncate_tuple(
    source_descriptor: &TupleDescData,
    source: IndexTuple,
    leavenatts: i32,
) -> IndexTuple {
    crate::assert!(leavenatts <= source_descriptor.natts);

    if leavenatts == source_descriptor.natts {
        return copy_index_tuple(source);
    }

    let truncdesc = source_descriptor.create_truncated_copy(leavenatts);

    let mut values = vec![Datum(0); INDEX_MAX_KEYS];
    let mut isnull = vec![false; INDEX_MAX_KEYS];
    index_deform_tuple(source, &truncdesc, &mut values, &mut isnull);
    let truncated = index_form_tuple(&truncdesc, &values, &isnull);
    (*truncated).tid = (*source).tid;
    crate::assert!(IndexTupleSize(&*truncated) <= IndexTupleSize(&*source));
    truncated
}

/// `index_getattr`: fetch a user attribute (1-based `attnum`) as `(value,
/// isnull)`, with the fast path for a known cached offset and no nulls.
///
/// SAFETY: `tup` must point at a valid index-tuple block; `attnum` >= 1.
pub unsafe fn index_getattr(
    tup: IndexTuple,
    attnum: i32,
    tuple_desc: &TupleDescData,
) -> (Datum, bool) {
    crate::assert!(attnum > 0);

    let t_info = (*tup).t_info;
    let has_nulls = (t_info & INDEX_NULL_MASK) != 0;

    if !has_nulls {
        return (nocache_index_getattr(tup, attnum, tuple_desc), false);
    }

    let bp = tup
        .cast::<u8>()
        .add(core::mem::size_of::<IndexTupleData>());
    if att_isnull(
        attnum - 1,
        core::slice::from_raw_parts(bp, ((attnum - 1) >> 3) as usize + 1),
    ) {
        (Datum(0), true)
    } else {
        (nocache_index_getattr(tup, attnum, tuple_desc), false)
    }
}

/// `pfree` of an index tuple: reclaim its block. Not a C entry point of its own
/// (callers `pfree` directly), but provided so tests/owners can free a formed
/// tuple. `size` must be the tuple's `IndexTupleSize`.
///
/// SAFETY: `tup` came from a form/copy/truncate in this module.
pub unsafe fn pfree_index_tuple(tup: IndexTuple) {
    if tup.is_null() {
        return;
    }
    let size = IndexTupleSize(&*tup);
    free_index_tuple(tup, size);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::attnum::AttrNumber;
    use crate::catalog::genbki::{INT4OID, INT8OID, TEXTOID};

    fn make_desc(specs: &[(&str, crate::postgres_ext::Oid)]) -> TupleDescData {
        let mut desc = TupleDescData::create_template(specs.len() as i32);
        for (i, (name, oid)) in specs.iter().enumerate() {
            desc.init_builtin_entry((i + 1) as AttrNumber, name, *oid, -1, 0);
        }
        desc
    }

    const VARHDRSZ_TEST: usize = 4;

    fn make_text(s: &str) -> Datum {
        let total = VARHDRSZ_TEST + s.len();
        let mut buf = vec![0u8; total].into_boxed_slice();
        let hdr = (total as u32) << 2;
        buf[0..4].copy_from_slice(&hdr.to_le_bytes());
        buf[4..].copy_from_slice(s.as_bytes());
        crate::postgres::PointerGetDatum(Box::into_raw(buf).cast::<u8>())
    }

    fn read_text(d: Datum) -> String {
        // SAFETY: d is a valid varlena (possibly short-header after packing);
        // decode either header form.
        unsafe {
            let p = crate::postgres::DatumGetPointer(d);
            let (data_off, len) = if (*p & 0x01) == 0x01 {
                (1usize, (crate::varatt::VARSIZE_SHORT(p) as usize) - 1)
            } else {
                (
                    VARHDRSZ_TEST,
                    (crate::varatt::VARSIZE(p) as usize) - VARHDRSZ_TEST,
                )
            };
            let data = core::slice::from_raw_parts(p.add(data_off), len);
            String::from_utf8(data.to_vec()).unwrap()
        }
    }

    #[test]
    fn form_deform_two_int4() {
        let desc = make_desc(&[("a", INT4OID), ("b", INT4OID)]);
        let values = [Datum(5), Datum(6)];
        let isnull = [false, false];

        let itup = index_form_tuple(&desc, &values, &isnull);
        // SAFETY: itup is a freshly-formed valid index tuple.
        unsafe {
            assert!(!(*itup).has_nulls());
            // no nulls -> data offset = MAXALIGN(8) = 8; data = 2 x int4 = 8;
            // total MAXALIGN(16) = 16.
            assert_eq!(IndexTupleSize(&*itup), 16);
        }

        let mut out = vec![Datum(0); 2];
        let mut out_null = vec![false; 2];
        unsafe { index_deform_tuple(itup, &desc, &mut out, &mut out_null) };
        assert_eq!(out[0], Datum(5));
        assert_eq!(out[1], Datum(6));
        assert_eq!(out_null, vec![false, false]);

        unsafe { pfree_index_tuple(itup) };
    }

    #[test]
    fn form_deform_with_null_and_varlena() {
        let desc = make_desc(&[("a", INT4OID), ("t", TEXTOID), ("b", INT8OID)]);
        let t = make_text("idx");
        let values = [Datum(1), t, Datum(0)];
        let isnull = [false, false, true];

        let itup = index_form_tuple(&desc, &values, &isnull);
        // SAFETY: valid formed tuple.
        unsafe {
            assert!((*itup).has_nulls());
            assert!((*itup).has_varwidths());
        }

        let mut out = vec![Datum(0); 3];
        let mut out_null = vec![false; 3];
        unsafe { index_deform_tuple(itup, &desc, &mut out, &mut out_null) };
        assert_eq!(out[0], Datum(1));
        assert!(!out_null[1]);
        assert_eq!(read_text(out[1]), "idx");
        assert!(out_null[2]);

        unsafe { pfree_index_tuple(itup) };
    }

    #[test]
    fn getattr_fast_path() {
        let desc = make_desc(&[("a", INT4OID), ("b", INT4OID)]);
        let values = [Datum(11), Datum(22)];
        let isnull = [false, false];
        let itup = index_form_tuple(&desc, &values, &isnull);

        // SAFETY: valid tuple; attnums in range.
        unsafe {
            let (v1, n1) = index_getattr(itup, 1, &desc);
            assert_eq!(v1, Datum(11));
            assert!(!n1);
            let (v2, n2) = index_getattr(itup, 2, &desc);
            assert_eq!(v2, Datum(22));
            assert!(!n2);
            pfree_index_tuple(itup);
        }
    }
}

//! Heap tuple accessor and mutator routines. Translated from
//! backend/access/common/heaptuple.c.
//!
//! These routines build and decode the PostgreSQL on-disk heap tuple format,
//! which the foundation's heap pages depend on byte-for-byte. The on-disk layout
//! is: the 23-byte `HeapTupleHeaderData` (overlaid via `#[repr(C)]`), an optional
//! null bitmap (one bit per attribute, present only when the tuple has a null),
//! MAXALIGN padding to `t_hoff`, then the user data area where each attribute is
//! laid out at its `attalignby` boundary (short varlenas excepted). `t_hoff`,
//! the null bitmap, the per-attribute alignment padding, and varlena handling
//! are all reproduced exactly as in C.
//!
//! Memory model: C allocates the in-memory `HeapTuple` management struct, its
//! header, and its data as a single `palloc` block. Here the management struct
//! (`HeapTupleData`) is returned by value and the tuple body (header + data) is
//! a separately heap-allocated block whose pointer is stored in `t_data`;
//! `heap_freetuple` reclaims that block. The form/deform byte logic is identical
//! to C either way -- only the allocation bookkeeping differs.
//!
//! Soundness: the body is allocated MAXALIGN(8)-aligned, and the `*const u8` ->
//! `*mut HeapTupleHeaderData` overlay cast is sound because the header's
//! alignment (4) divides 8. Byval field access goes through
//! `fetch_att`/`store_att_byval` (unaligned read/write) and varlena access
//! through the `varatt` accessors (also unaligned), so no aligned access is ever
//! performed on a field that the on-disk layout may place off its boundary --
//! hence no `cast_ptr_alignment` allow is needed.

use core::alloc::Layout;

use crate::access::htup::{alloc_tuple_body, tuple_body_from_raw, HeapTupleData, HeapTupleIsValid};
use crate::access::htup_details::{
    HeapTupleHeaderData, MinimalTupleData, BITMAPLEN, HEAP_HASEXTERNAL, HEAP_HASNULL,
    HEAP_HASVARWIDTH, HEAP_NATTS_MASK, MINIMAL_TUPLE_OFFSET, SizeofHeapTupleHeader,
    SizeofMinimalTupleHeader, MaxTupleAttributeNumber,
};
use crate::access::sysattr::{
    MAX_COMMAND_ID_ATTRIBUTE_NUMBER, MAX_TRANSACTION_ID_ATTRIBUTE_NUMBER,
    MIN_COMMAND_ID_ATTRIBUTE_NUMBER, MIN_TRANSACTION_ID_ATTRIBUTE_NUMBER,
    SELF_ITEM_POINTER_ATTRIBUTE_NUMBER, TABLE_OID_ATTRIBUTE_NUMBER,
};
use crate::access::tupdesc::TupleDescData;
use crate::access::tupmacs::{
    att_addlength_pointer, att_datum_alignby, att_isnull, att_nominal_alignby,
    att_pointer_alignby, fetch_att, store_att_byval,
};
use crate::c::{bits8, HIGHBIT, MAXALIGN, Size};
use crate::postgres::{
    CommandIdGetDatum, Datum, DatumGetPointer, ObjectIdGetDatum, PointerGetDatum,
    TransactionIdGetDatum,
};
use crate::postgres_ext::InvalidOid;
use crate::varatt::{
    SET_VARSIZE_SHORT, VARATT_CAN_MAKE_SHORT, VARATT_CONVERTED_SHORT_SIZE, VARATT_IS_EXTERNAL,
    VARATT_IS_SHORT, VARDATA, VARSIZE, VARSIZE_EXTERNAL, VARSIZE_SHORT,
};
use crate::elog;
use crate::utils::elog::ERROR;

/// `ATT_IS_PACKABLE` is captured on `CompactAttribute.attispackable`; the macro
/// form `COMPACT_ATTR_IS_PACKABLE(att) = attlen == -1 && attispackable`.
#[inline]
fn compact_attr_is_packable(attlen: i16, attispackable: bool) -> bool {
    attlen == -1 && attispackable
}

/// Advance `data` up to its `attalignby` boundary, preserving pointer provenance
/// (offset via `.add`, not an int<->ptr round trip). Equivalent to the C idiom
/// `(char *) att_align_nominal(data, attalign)`.
///
/// SAFETY: the aligned result must stay within the same allocation as `data`,
/// which holds for a tuple data area sized through `heap_compute_data_size`.
#[inline]
unsafe fn align_data_ptr(data: *mut u8, attalignby: usize) -> *mut u8 {
    let pad = att_nominal_alignby(data as usize, attalignby) - data as usize;
    data.add(pad)
}

/// A fresh invalid `ItemPointerData` (C `ItemPointerSetInvalid`).
#[inline]
fn invalid_item_pointer() -> crate::storage::itemptr::ItemPointerData {
    let mut p = crate::storage::itemptr::ItemPointerData {
        blkid: crate::storage::block::BlockIdData { hi: 0, lo: 0 },
        posid: 0,
    };
    p.set_invalid();
    p
}

/// `HeapTupleHeaderSetDatumLength` = `SET_VARSIZE(tup, len)`: write the in-memory
/// composite-Datum varlena length into the first word of the header's choice
/// union. This overlays the on-disk transaction fields (which a Datum tuple does
/// not use), so it never disturbs the on-disk format.
#[inline]
fn set_datum_length(header: &mut HeapTupleHeaderData, len: u32) {
    // SET_VARSIZE writes (len << 2) as the 4-byte varlena header. Writing a
    // (Copy, non-Drop) union field needs no unsafe; both arms are 12-byte POD,
    // so the on-disk xact fields a non-Datum tuple reads are left untouched.
    header.choice.t_datum.len_ = (len << 2) as i32;
}

// `alloc_tuple_body` / `tuple_body_from_raw` (the owned 8-aligned `Box<[u64]>`
// body allocators) are imported from `access::htup`; the body is freed when the
// owning `HeapTupleData` drops -- no manual `free_tuple_body`.

// ----------------------------------------------------------------------------
//                            misc support routines
// ----------------------------------------------------------------------------

/// `getmissingattr`: return the missing value of an attribute (1-based
/// `attnum`), or NULL if there isn't one. Returns `(value, isnull)`.
///
/// The pass-by-ref missing-value cache (`missing_cache`) is a TopMemoryContext
/// optimization; without `MemoryContext` we return the stored datum directly
/// (the descriptor owns it for its lifetime), which is behaviorally identical.
pub fn getmissingattr(tuple_desc: &TupleDescData, attnum: i32) -> (Datum, bool) {
    crate::assert!(attnum <= tuple_desc.natts);
    crate::assert!(attnum > 0);

    let idx = (attnum - 1) as usize;
    let att = tuple_desc.compact_attr(idx);

    if att.atthasmissing
        && let Some(constr) = tuple_desc.constr.as_deref()
            && let Some(missing) = constr.missing.as_ref() {
                let attrmiss = &missing[idx];
                if attrmiss.present {
                    return (attrmiss.value, false);
                }
            }

    (PointerGetDatum(core::ptr::null()), true)
}

/// `heap_compute_data_size`: determine the size of the data area of a tuple to
/// be constructed from `values`/`isnull`.
pub fn heap_compute_data_size(
    tuple_desc: &TupleDescData,
    values: &[Datum],
    isnull: &[bool],
) -> Size {
    let mut data_length: Size = 0;
    let number_of_attributes = tuple_desc.natts as usize;

    for i in 0..number_of_attributes {
        if isnull[i] {
            continue;
        }

        let val = values[i];
        let atti = tuple_desc.compact_attr(i);

        // SAFETY: a non-null varlena/byref datum carries a valid pointer; the
        // packable / short-header probes only read the datum's header byte.
        unsafe {
            if compact_attr_is_packable(atti.attlen, atti.attispackable)
                && VARATT_CAN_MAKE_SHORT(DatumGetPointer(val))
            {
                // Anticipating conversion to a short varlena header; adjust the
                // length and count no alignment.
                data_length += VARATT_CONVERTED_SHORT_SIZE(DatumGetPointer(val));
            } else {
                // The expanded-datum (EOH) flatten path is deferred until the
                // expanded-datum subsystem lands; ordinary fixed/varlena attrs
                // take the aligned path.
                data_length = att_datum_alignby(
                    data_length,
                    atti.attalignby as usize,
                    i32::from(atti.attlen),
                    val,
                );
                data_length =
                    att_addlength_pointer(data_length, i32::from(atti.attlen), DatumGetPointer(val));
            }
        }
    }

    data_length
}

/// Per-attribute helper for [`heap_fill_tuple`]: fill in either a data value or
/// a bit in the null bitmask. Faithful port of C `fill_val`.
///
/// `bit_present` is whether a null bitmap is being built (C's `bit != NULL`);
/// `bit_idx` indexes the current bitmap byte, `bitmask` the current bit. `data`
/// points at the current write cursor in the data area. Returns the number of
/// bytes written for this attribute (the caller advances the cursor).
///
/// SAFETY: `data` must point into the (zeroed) data area with room for the
/// attribute; varlena/byref datums must carry valid pointers.
#[allow(
    clippy::too_many_arguments,
    clippy::fn_params_excessive_bools,
    reason = "faithful port of C fill_val (the bools are the attr flags it dispatches on)"
)]
unsafe fn fill_val(
    attlen: i16,
    attbyval: bool,
    attalignby: u8,
    attispackable: bool,
    bit_present: bool,
    bit_idx: &mut isize,
    bits: &mut [bits8],
    bitmask: &mut i32,
    data: *mut u8,
    infomask: &mut u16,
    datum: Datum,
    isnull: bool,
) -> (usize, *mut u8) {
    let mut data = data;

    if bit_present {
        if *bitmask == i32::from(HIGHBIT) {
            *bit_idx += 1;
            bits[*bit_idx as usize] = 0x0;
            *bitmask = 1;
        } else {
            *bitmask <<= 1;
        }

        if isnull {
            *infomask |= HEAP_HASNULL;
            return (0, data);
        }

        bits[*bit_idx as usize] |= *bitmask as u8;
    }

    let data_length: usize;

    let attalignby = attalignby as usize;
    if attbyval {
        // pass-by-value
        data = align_data_ptr(data, attalignby);
        store_att_byval(data, datum, i32::from(attlen));
        data_length = attlen as usize;
    } else if attlen == -1 {
        // varlena
        let val = DatumGetPointer(datum);
        *infomask |= HEAP_HASVARWIDTH;
        if VARATT_IS_EXTERNAL(val) {
            // The expanded (EOH) flatten path is deferred; an on-disk/indirect
            // external pointer is copied verbatim (no alignment, short by
            // definition).
            *infomask |= HEAP_HASEXTERNAL;
            data_length = VARSIZE_EXTERNAL(val);
            core::ptr::copy_nonoverlapping(val, data, data_length);
        } else if VARATT_IS_SHORT(val) {
            // no alignment for short varlenas
            data_length = VARSIZE_SHORT(val) as usize;
            core::ptr::copy_nonoverlapping(val, data, data_length);
        } else if attispackable && VARATT_CAN_MAKE_SHORT(val) {
            // convert to short varlena -- no alignment
            data_length = VARATT_CONVERTED_SHORT_SIZE(val);
            SET_VARSIZE_SHORT(data, data_length as u8);
            core::ptr::copy_nonoverlapping(VARDATA(val.cast::<u8>()), data.add(1), data_length - 1);
        } else {
            // full 4-byte header varlena
            data = align_data_ptr(data, attalignby);
            data_length = VARSIZE(val) as usize;
            core::ptr::copy_nonoverlapping(val, data, data_length);
        }
    } else if attlen == -2 {
        // cstring ... never needs alignment
        *infomask |= HEAP_HASVARWIDTH;
        crate::assert!(attalignby == core::mem::size_of::<u8>());
        let p = DatumGetPointer(datum);
        let mut len = 0usize;
        while *p.add(len) != 0 {
            len += 1;
        }
        data_length = len + 1;
        core::ptr::copy_nonoverlapping(p, data, data_length);
    } else {
        // fixed-length pass-by-reference
        data = align_data_ptr(data, attalignby);
        crate::assert!(attlen > 0);
        data_length = attlen as usize;
        core::ptr::copy_nonoverlapping(DatumGetPointer(datum), data, data_length);
    }

    data = data.add(data_length);
    (data_length, data)
}

/// `heap_fill_tuple`: load the data portion of a tuple from `values`/`isnull`,
/// fill the null bitmap (if `bit` is provided), and set the infomask bits that
/// reflect the tuple's data contents. The data area MUST be pre-zeroed.
///
/// `data` points at the data area (`t_data + t_hoff`); `bit` is the null bitmap
/// slice (`t_bits`), `None` when the tuple has no nulls. Returns nothing; the
/// data and bitmap are written in place and `infomask` updated.
///
/// SAFETY: `data` must point into a zeroed area with at least `data_size` bytes;
/// `bit`, when present, must be sized `BITMAPLEN(natts)`.
pub unsafe fn heap_fill_tuple(
    tuple_desc: &TupleDescData,
    values: &[Datum],
    isnull: &[bool],
    data: *mut u8,
    data_size: Size,
    infomask: &mut u16,
    bit: Option<&mut [bits8]>,
) {
    let number_of_attributes = tuple_desc.natts as usize;
    let start = data;
    let mut data = data;

    let bit_present = bit.is_some();
    // C primes `bitP = &bit[-1]` and `bitmask = HIGHBIT`; we model the cursor as
    // an isize starting at -1 indexing into the bitmap slice.
    let mut bit_idx: isize = -1;
    let mut bitmask: i32 = i32::from(HIGHBIT);
    let mut empty: [bits8; 0] = [];
    let bits: &mut [bits8] = bit.unwrap_or(&mut empty);

    *infomask &= !(HEAP_HASNULL | HEAP_HASVARWIDTH | HEAP_HASEXTERNAL);

    for i in 0..number_of_attributes {
        let attr = tuple_desc.compact_attr(i);
        let (_n, ndata) = fill_val(
            attr.attlen,
            attr.attbyval,
            attr.attalignby,
            attr.attispackable,
            bit_present,
            &mut bit_idx,
            bits,
            &mut bitmask,
            data,
            infomask,
            values[i],
            isnull[i],
        );
        data = ndata;
    }

    crate::assert!((data as usize - start as usize) == data_size);
}

// ----------------------------------------------------------------------------
//                            heap tuple interface
// ----------------------------------------------------------------------------

/// `heap_attisnull`: true iff the tuple attribute is not present (NULL). A
/// `None` `tuple_desc` is allowed for relations not expected to have missing
/// values (catalogs, indexes).
///
/// SAFETY: `tup.t_data` must point at a valid tuple body with its null bitmap.
pub unsafe fn heap_attisnull(
    tup: &HeapTupleData,
    attnum: i32,
    tuple_desc: Option<&TupleDescData>,
) -> bool {
    crate::assert!(tuple_desc.is_none_or(|d| attnum <= d.natts));
    let td = &*tup.t_data();

    if attnum > i32::from(td.get_natts()) {
        // present (not null) only if the descriptor records a missing value.
        return !matches!(tuple_desc, Some(d) if d.compact_attr((attnum - 1) as usize).atthasmissing);
    }

    if attnum > 0 {
        if (td.t_infomask & HEAP_HASNULL) == 0 {
            return false;
        }
        let natts = td.get_natts() as usize;
        return att_isnull(attnum - 1, td.t_bits(natts));
    }

    match attnum {
        a if a == i32::from(TABLE_OID_ATTRIBUTE_NUMBER)
            || a == i32::from(SELF_ITEM_POINTER_ATTRIBUTE_NUMBER)
            || a == i32::from(MIN_TRANSACTION_ID_ATTRIBUTE_NUMBER)
            || a == i32::from(MIN_COMMAND_ID_ATTRIBUTE_NUMBER)
            || a == i32::from(MAX_TRANSACTION_ID_ATTRIBUTE_NUMBER)
            || a == i32::from(MAX_COMMAND_ID_ATTRIBUTE_NUMBER) =>
        {
            // these are never null
            false
        }
        _ => {
            elog!(ERROR, format!("invalid attnum: {attnum}"));
            unreachable!("elog!(ERROR) raises")
        }
    }
}

/// `nocachegetattr`: fetch a non-null user attribute that lacks a usable cached
/// offset. Called only from [`fastgetattr`]; `attnum` is 1-based and must refer
/// to a non-null user attribute.
///
/// PG caches computed offsets in the shared descriptor's `attcacheoff` as a side
/// effect. Here the descriptor is a shared (`Arc`) immutable value, so the cache
/// is not persisted (it is a pure optimization, not part of the result); the
/// offset is computed afresh, which is behaviorally identical. The `slow`
/// (nulls/var-width-before-target) and fast (leading fixed-width) walks are
/// otherwise faithful to C.
///
/// SAFETY: `tup.t_data` must point at a valid tuple body; `attnum` valid and
/// non-null.
pub unsafe fn nocachegetattr(tup: &HeapTupleData, attnum: i32, tuple_desc: &TupleDescData) -> Datum {
    let td = &*tup.t_data();
    let natts_in_tuple = td.get_natts() as usize;
    let has_nulls = (td.t_infomask & HEAP_HASNULL) != 0;
    let has_varwidth = (td.t_infomask & HEAP_HASVARWIDTH) != 0;
    let bp = td.t_bits(natts_in_tuple);

    let mut slow = false;
    let attnum = (attnum - 1) as usize;

    if has_nulls {
        let byte = attnum >> 3;
        let finalbit = attnum & 0x07;
        if (!bp[byte]) & (((1u32 << finalbit) - 1) as u8) != 0 {
            slow = true;
        } else {
            for &b in bp.iter().take(byte) {
                if b != 0xFF {
                    slow = true;
                    break;
                }
            }
        }
    }

    let tp = (tup.t_data().cast::<u8>()).add(td.t_hoff as usize);

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
        // Walk carefully, accounting for nulls (no storage) and var-widths.
        let mut i = 0usize;
        loop {
            let att_attlen = tuple_desc.compact_attr(i).attlen;
            let att_alignby = tuple_desc.compact_attr(i).attalignby as usize;

            if has_nulls && att_isnull(i as i32, bp) {
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
        // No nulls or var-widths up to and including the target: sum the aligned
        // fixed widths of the leading columns.
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

/// `heap_getsysattr`: fetch the value of a system attribute. Returns
/// `(value, isnull)`; no system attribute ever reads as NULL.
///
/// SAFETY: `tup.t_data` must point at a valid tuple body.
pub unsafe fn heap_getsysattr(
    tup: &HeapTupleData,
    attnum: i32,
    _tuple_desc: &TupleDescData,
) -> (Datum, bool) {
    let td = &*tup.t_data();
    let result = if attnum == i32::from(SELF_ITEM_POINTER_ATTRIBUTE_NUMBER) {
        PointerGetDatum(core::ptr::from_ref(&tup.t_self).cast::<u8>())
    } else if attnum == i32::from(MIN_TRANSACTION_ID_ATTRIBUTE_NUMBER) {
        TransactionIdGetDatum(td.get_raw_xmin())
    } else if attnum == i32::from(MAX_TRANSACTION_ID_ATTRIBUTE_NUMBER) {
        TransactionIdGetDatum(td.get_raw_xmax())
    } else if attnum == i32::from(MIN_COMMAND_ID_ATTRIBUTE_NUMBER)
        || attnum == i32::from(MAX_COMMAND_ID_ATTRIBUTE_NUMBER)
    {
        // cmin/cmax are aliases for the same field (possibly a combo cid).
        CommandIdGetDatum(td.get_raw_command_id())
    } else if attnum == i32::from(TABLE_OID_ATTRIBUTE_NUMBER) {
        ObjectIdGetDatum(tup.t_tableOid)
    } else {
        elog!(ERROR, format!("invalid attnum: {attnum}"));
        unreachable!("elog!(ERROR) raises")
    };
    (result, false)
}

/// `heap_copytuple`: return a copy of an entire tuple. The management struct,
/// header, and data are one block in C; here the body block is copied and a new
/// management struct returned by value. Returns a tuple with a null `t_data`
/// when the input is invalid.
///
/// SAFETY: `tuple.t_data`, when non-null, must point at a `tuple.t_len` body.
pub unsafe fn heap_copytuple(tuple: &HeapTupleData) -> HeapTupleData {
    if !HeapTupleIsValid(Some(tuple)) || tuple.t_data_is_null() {
        return HeapTupleData::null(invalid_item_pointer(), InvalidOid);
    }

    let len = tuple.t_len as usize;
    // SAFETY: input body holds `len` bytes (caller contract); from_raw copies them
    // into a fresh 8-aligned owned body.
    let body = tuple_body_from_raw(tuple.t_data().cast::<u8>(), len);

    HeapTupleData {
        t_len: tuple.t_len,
        t_self: tuple.t_self,
        t_tableOid: tuple.t_tableOid,
        body: Some(body),
    }
}

/// `heap_copytuple_with_tuple`: copy a tuple into a caller-supplied management
/// struct. The destination body is a fresh block (not the single-palloc form).
///
/// SAFETY: see [`heap_copytuple`].
pub unsafe fn heap_copytuple_with_tuple(src: &HeapTupleData, dest: &mut HeapTupleData) {
    if !HeapTupleIsValid(Some(src)) || src.t_data_is_null() {
        dest.body = None;
        return;
    }
    let len = src.t_len as usize;
    dest.t_len = src.t_len;
    dest.t_self = src.t_self;
    dest.t_tableOid = src.t_tableOid;
    // SAFETY: src body holds `len` bytes; copy into a fresh owned body.
    dest.body = Some(tuple_body_from_raw(src.t_data().cast::<u8>(), len));
}

/// `heap_form_tuple`: construct a tuple from `values`/`isnull` arrays of length
/// `tuple_descriptor.natts`. Byte-faithful to C: header at offset 0, optional
/// null bitmap, MAXALIGN to `t_hoff`, then the data area.
pub fn heap_form_tuple(
    tuple_descriptor: &TupleDescData,
    values: &[Datum],
    isnull: &[bool],
) -> HeapTupleData {
    let number_of_attributes = tuple_descriptor.natts;

    if number_of_attributes > MaxTupleAttributeNumber {
        crate::ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_TOO_MANY_COLUMNS).errmsg(format!(
                "number of columns ({number_of_attributes}) exceeds limit ({MaxTupleAttributeNumber})"
            ));
        });
    }

    let natts = number_of_attributes as usize;
    let hasnull = isnull.iter().take(natts).any(|&n| n);

    // Determine total space needed.
    let mut len = SizeofHeapTupleHeader; // offsetof(HeapTupleHeaderData, t_bits)
    if hasnull {
        len += BITMAPLEN(number_of_attributes) as usize;
    }
    len = MAXALIGN(len); // align user data safely
    let hoff = len;

    let data_len = heap_compute_data_size(tuple_descriptor, values, isnull);
    len += data_len;

    // Allocate and zero the owned body block, held by `tuple`.
    let mut tuple = HeapTupleData {
        t_len: len as u32,
        t_self: invalid_item_pointer(),
        t_tableOid: InvalidOid,
        body: Some(alloc_tuple_body(len)),
    };
    let td = tuple.t_data_mut();

    // SAFETY: `td` is the freshly-allocated, zeroed owned body of `len` bytes
    // (kept alive by `tuple.body`). Every write below stays within it; the
    // header overlay is sound (`#[repr(C)]`).
    unsafe {
        let header = &mut *td;
        set_datum_length(header, len as u32);
        header.set_type_id(tuple_descriptor.tdtypeid);
        header.set_typmod(tuple_descriptor.tdtypmod);
        header.ctid.set_invalid();
        header.set_natts(number_of_attributes as u16);
        header.t_hoff = hoff as u8;

        let data_ptr = td.cast::<u8>().add(hoff);
        let mut infomask = header.t_infomask;
        let bitmap_len = BITMAPLEN(number_of_attributes) as usize;
        if hasnull {
            // t_bits begins right after the fixed 23-byte header.
            let bits_ptr = td.cast::<u8>().add(SizeofHeapTupleHeader);
            let bits = core::slice::from_raw_parts_mut(bits_ptr, bitmap_len);
            heap_fill_tuple(
                tuple_descriptor,
                values,
                isnull,
                data_ptr,
                data_len,
                &mut infomask,
                Some(bits),
            );
        } else {
            heap_fill_tuple(
                tuple_descriptor,
                values,
                isnull,
                data_ptr,
                data_len,
                &mut infomask,
                None,
            );
        }
        header.t_infomask = infomask;
    }

    tuple.t_len = len as u32;
    tuple
}

/// `heap_modify_tuple`: form a new tuple from an old tuple and replacement
/// values, taking each column from `repl_*` where `do_replace` is true and from
/// the old tuple otherwise.
///
/// Takes `&mut TupleDescData` because the deform step caches offsets.
///
/// SAFETY: `tuple.t_data` must point at a valid tuple body.
pub unsafe fn heap_modify_tuple(
    tuple: &HeapTupleData,
    tuple_desc: &TupleDescData,
    repl_values: &[Datum],
    repl_isnull: &[bool],
    do_replace: &[bool],
) -> HeapTupleData {
    let number_of_attributes = tuple_desc.natts as usize;

    let (mut values, mut isnull) = heap_deform_tuple(tuple, tuple_desc);

    for attoff in 0..number_of_attributes {
        if do_replace[attoff] {
            values[attoff] = repl_values[attoff];
            isnull[attoff] = repl_isnull[attoff];
        }
    }

    let mut new_tuple = heap_form_tuple(tuple_desc, &values, &isnull);

    // copy the identification info of the old tuple
    (*new_tuple.t_data_mut()).ctid = (*tuple.t_data()).ctid;
    new_tuple.t_self = tuple.t_self;
    new_tuple.t_tableOid = tuple.t_tableOid;
    new_tuple
}

/// `heap_modify_tuple_by_cols`: like [`heap_modify_tuple`] but with an array of
/// 1-based target column numbers instead of a boolean map.
///
/// SAFETY: see [`heap_modify_tuple`].
pub unsafe fn heap_modify_tuple_by_cols(
    tuple: &HeapTupleData,
    tuple_desc: &TupleDescData,
    repl_cols: &[i32],
    repl_values: &[Datum],
    repl_isnull: &[bool],
) -> HeapTupleData {
    let number_of_attributes = tuple_desc.natts;

    let (mut values, mut isnull) = heap_deform_tuple(tuple, tuple_desc);

    for (i, &attnum) in repl_cols.iter().enumerate() {
        if attnum <= 0 || attnum > number_of_attributes {
            elog!(ERROR, format!("invalid column number {attnum}"));
        }
        values[(attnum - 1) as usize] = repl_values[i];
        isnull[(attnum - 1) as usize] = repl_isnull[i];
    }

    let mut new_tuple = heap_form_tuple(tuple_desc, &values, &isnull);

    (*new_tuple.t_data_mut()).ctid = (*tuple.t_data()).ctid;
    new_tuple.t_self = tuple.t_self;
    new_tuple.t_tableOid = tuple.t_tableOid;
    new_tuple
}

/// `heap_deform_tuple`: extract a tuple into `(values, isnull)` vectors of
/// length `tuple_desc.natts`; the inverse of [`heap_form_tuple`]. For
/// pass-by-reference types the Datum points into the tuple body.
///
/// The shared descriptor's `attcacheoff` cache is not persisted (the descriptor
/// is an immutable `Arc`); offsets are computed afresh each call, which is
/// behaviorally identical (the cache is a pure optimization).
///
/// SAFETY: `tuple.t_data` must point at a valid tuple body.
pub unsafe fn heap_deform_tuple(
    tuple: &HeapTupleData,
    tuple_desc: &TupleDescData,
) -> (Vec<Datum>, Vec<bool>) {
    let tup = &*tuple.t_data();
    let has_nulls = (tup.t_infomask & HEAP_HASNULL) != 0;
    let tdesc_natts = tuple_desc.natts as usize;

    let mut natts = tup.get_natts() as usize;
    // Tuple may have more fields than the caller expects (inheritance).
    natts = natts.min(tdesc_natts);

    let mut values = vec![Datum(0); tdesc_natts];
    let mut isnull = vec![false; tdesc_natts];

    let bp = tup.t_bits(tup.get_natts() as usize);
    let tp = tuple.t_data().cast::<u8>().add(tup.t_hoff as usize);

    let mut off = 0usize;
    let mut attnum = 0usize;

    while attnum < natts {
        let attlen = tuple_desc.compact_attr(attnum).attlen;
        let attbyval = tuple_desc.compact_attr(attnum).attbyval;
        let attalignby = tuple_desc.compact_attr(attnum).attalignby as usize;

        if has_nulls && att_isnull(attnum as i32, bp) {
            values[attnum] = Datum(0);
            isnull[attnum] = true;
            attnum += 1;
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

        attnum += 1;
    }

    // Read the rest as nulls or missing values.
    while attnum < tdesc_natts {
        let (v, n) = getmissingattr(tuple_desc, attnum as i32 + 1);
        values[attnum] = v;
        isnull[attnum] = n;
        attnum += 1;
    }

    (values, isnull)
}

/// `heap_freetuple`: free a tuple's body (the C single-palloc block). The owned
/// `TupleStorage` (and its `Box<[u64]>` body) drops here when `htup` is consumed
/// by value -- no manual free.
#[allow(
    clippy::needless_pass_by_value,
    reason = "by-value is the contract: this consumes the tuple (C pfree(htup)) and drops its owned body"
)]
pub fn heap_freetuple(htup: HeapTupleData) {
    drop(htup);
}

/// `fastgetattr`: fetch a non-system user attribute as `(value, isnull)`,
/// delegating offset computation to [`nocachegetattr`]. `attnum` is 1-based and
/// must be a user attribute. (PG has an inlined cached-offset fast path; with the
/// non-persisted cache it collapses to the nocache walk.)
///
/// SAFETY: `tup.t_data` must point at a valid tuple body; `attnum` >= 1.
pub unsafe fn fastgetattr(
    tup: &HeapTupleData,
    attnum: i32,
    tuple_desc: &TupleDescData,
) -> (Datum, bool) {
    crate::assert!(attnum > 0);

    let td = &*tup.t_data();
    let has_nulls = (td.t_infomask & HEAP_HASNULL) != 0;

    if !has_nulls {
        return (nocachegetattr(tup, attnum, tuple_desc), false);
    }

    if att_isnull(attnum - 1, td.t_bits(td.get_natts() as usize)) {
        (Datum(0), true)
    } else {
        (nocachegetattr(tup, attnum, tuple_desc), false)
    }
}

/// `heap_getattr`: extract an attribute (system or user) as `(value, isnull)`,
/// range-checked. An attnum beyond the tuple's columns returns the descriptor's
/// missing value (or NULL).
///
/// SAFETY: `tup.t_data` must point at a valid tuple body.
pub unsafe fn heap_getattr(
    tup: &HeapTupleData,
    attnum: i32,
    tuple_desc: &TupleDescData,
) -> (Datum, bool) {
    if attnum > 0 {
        if attnum > i32::from((*tup.t_data()).get_natts()) {
            return getmissingattr(tuple_desc, attnum);
        }
        fastgetattr(tup, attnum, tuple_desc)
    } else {
        heap_getsysattr(tup, attnum, tuple_desc)
    }
}

// ----------------------------------------------------------------------------
//                        composite-datum and minimal tuples
// ----------------------------------------------------------------------------

/// `heap_copy_tuple_as_datum`: copy a tuple as a composite-type Datum. The
/// external-TOAST-inlining path (`toast_flatten_tuple_to_datum`) is deferred
/// until the TOAST subsystem lands; the common fast path (palloc copy + set the
/// composite-Datum header fields) is complete.
///
/// SAFETY: `tuple.t_data` must point at a valid `tuple.t_len` body.
pub unsafe fn heap_copy_tuple_as_datum(
    tuple: &HeapTupleData,
    tuple_desc: &TupleDescData,
) -> Datum {
    if (*tuple.t_data()).has_external() {
        // toast_flatten_tuple_to_datum(tuple->t_data, tuple->t_len, tupleDesc)
        unimplemented!("toast_flatten_tuple_to_datum deferred (TOAST subsystem)")
    }

    let len = tuple.t_len as usize;
    // A composite Datum is a standalone varlena returned by raw pointer (C
    // palloc'd, not a managed HeapTupleData body); allocate it raw like the
    // minimal-tuple blocks rather than an owned TupleBody.
    let block = alloc_minimal_block(len);
    core::ptr::copy_nonoverlapping(tuple.t_data().cast::<u8>(), block, len);
    #[allow(
        clippy::cast_ptr_alignment,
        reason = "sound overlay: alloc_minimal_block returns 8-aligned memory; HeapTupleHeaderData's align (4) divides 8"
    )]
    let td = block.cast::<HeapTupleHeaderData>();

    let header = &mut *td;
    set_datum_length(header, tuple.t_len);
    header.set_type_id(tuple_desc.tdtypeid);
    header.set_typmod(tuple_desc.tdtypmod);

    PointerGetDatum(td.cast::<u8>())
}

/// `heap_form_minimal_tuple`: build a `MinimalTuple` (a header-less, system-
/// column-less transient executor tuple) from `values`/`isnull`. `extra` bytes
/// (MAXALIGN'd) are reserved before the tuple. Returns the block pointer.
pub fn heap_form_minimal_tuple(
    tuple_descriptor: &TupleDescData,
    values: &[Datum],
    isnull: &[bool],
    extra: Size,
) -> *mut MinimalTupleData {
    crate::assert!(extra == MAXALIGN(extra));

    let number_of_attributes = tuple_descriptor.natts;
    if number_of_attributes > MaxTupleAttributeNumber {
        crate::ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_TOO_MANY_COLUMNS).errmsg(format!(
                "number of columns ({number_of_attributes}) exceeds limit ({MaxTupleAttributeNumber})"
            ));
        });
    }

    let natts = number_of_attributes as usize;
    let hasnull = isnull.iter().take(natts).any(|&n| n);

    let mut len = SizeofMinimalTupleHeader;
    if hasnull {
        len += BITMAPLEN(number_of_attributes) as usize;
    }
    len = MAXALIGN(len);
    let hoff = len;
    let data_len = heap_compute_data_size(tuple_descriptor, values, isnull);
    len += data_len;

    // Allocate `len + extra` zeroed; the tuple begins at `extra`.
    let total = len + extra;
    let block = alloc_minimal_block(total);
    // SAFETY: block has `total` zeroed bytes; the tuple body fits in [extra, total).
    unsafe {
        let tuple_ptr = minimal_at(block, extra);
        let tuple = &mut *tuple_ptr;
        tuple.t_len = len as u32;
        // HeapTupleHeaderSetNatts on a MinimalTuple: t_infomask2's low 11 bits.
        tuple.t_infomask2 =
            (tuple.t_infomask2 & !HEAP_NATTS_MASK) | (number_of_attributes as u16);
        tuple.t_hoff = (hoff + MINIMAL_TUPLE_OFFSET) as u8;

        let data_ptr = block.add(extra + hoff);
        let mut infomask = tuple.t_infomask;
        if hasnull {
            // t_bits begins just after the minimal header.
            let bits_ptr = block.add(extra + SizeofMinimalTupleHeader);
            let bits = core::slice::from_raw_parts_mut(bits_ptr, BITMAPLEN(number_of_attributes) as usize);
            heap_fill_tuple(tuple_descriptor, values, isnull, data_ptr, data_len, &mut infomask, Some(bits));
        } else {
            heap_fill_tuple(tuple_descriptor, values, isnull, data_ptr, data_len, &mut infomask, None);
        }
        tuple.t_infomask = infomask;
        tuple_ptr
    }
}

/// Allocate a zeroed block of `size` bytes for a minimal tuple (the `extra`
/// prefix + the tuple). Reclaimed by [`heap_free_minimal_tuple`] via the stored
/// `t_len` (callers that used `extra` must track it themselves, as in C).
///
/// Stays a raw `alloc_zeroed` (not an owned `Box`): a `MinimalTuple` is returned
/// as a raw `*mut MinimalTupleData` that the caller owns (the executor's
/// transient-tuple ABI), AND the minimal-tuple builders return a pointer OFFSET
/// by `extra` into the middle of the block (`block + extra`), so the returned
/// pointer is not the allocation base -- `Box::from_raw` cannot reconstitute it.
/// Composite-Datum copies likewise leak the block into a byref Datum. This is the
/// output-raw / interior-pointer case, not an owned single allocation.
fn alloc_minimal_block(size: usize) -> *mut u8 {
    let n = size.max(1);
    let layout = Layout::from_size_align(n, crate::pg_config::MAXIMUM_ALIGNOF)
        .unwrap_or_else(|_| Layout::new::<u8>());
    // SAFETY: non-zero size; alloc_zeroed returns zeroed memory or null (OOM).
    let p = unsafe { std::alloc::alloc_zeroed(layout) };
    if p.is_null() {
        elog!(ERROR, "out of memory forming minimal tuple".to_string());
    }
    p
}

/// Overlay the `MinimalTupleData` at offset `extra` of a minimal-tuple block.
///
/// SAFETY: `block` came from [`alloc_minimal_block`] (8-aligned) and `extra` is
/// MAXALIGN'd (a multiple of 8), so `block + extra` meets `MinimalTupleData`'s
/// alignment; `extra` must be within the block.
#[inline]
unsafe fn minimal_at(block: *mut u8, extra: usize) -> *mut MinimalTupleData {
    #[allow(
        clippy::cast_ptr_alignment,
        reason = "sound overlay: block is 8-aligned and extra is MAXALIGN'd, so block+extra meets MinimalTupleData's align"
    )]
    block.add(extra).cast::<MinimalTupleData>()
}

/// `heap_free_minimal_tuple`: free a minimal tuple's block.
///
/// SAFETY: `mtup` came from a minimal-tuple constructor in this module with no
/// `extra` prefix (the no-extra form, matching the common executor usage).
pub unsafe fn heap_free_minimal_tuple(mtup: *mut MinimalTupleData) {
    if mtup.is_null() {
        return;
    }
    let len = (*mtup).t_len as usize;
    let layout = Layout::from_size_align(len.max(1), crate::pg_config::MAXIMUM_ALIGNOF)
        .unwrap_or_else(|_| Layout::new::<u8>());
    std::alloc::dealloc(mtup.cast::<u8>(), layout);
}

/// `heap_copy_minimal_tuple`: copy a minimal tuple, reserving `extra` MAXALIGN'd
/// bytes before it.
///
/// SAFETY: `mtup` must point at a valid minimal tuple.
pub unsafe fn heap_copy_minimal_tuple(mtup: *mut MinimalTupleData, extra: Size) -> *mut MinimalTupleData {
    crate::assert!(extra == MAXALIGN(extra));
    let len = (*mtup).t_len as usize;
    let block = alloc_minimal_block(len + extra);
    let result = minimal_at(block, extra);
    core::ptr::copy_nonoverlapping(mtup.cast::<u8>(), result.cast::<u8>(), len);
    result
}

/// `heap_tuple_from_minimal_tuple`: build a `HeapTuple` by copying from a minimal
/// tuple; the system columns are zeroed.
///
/// SAFETY: `mtup` must point at a valid minimal tuple.
pub unsafe fn heap_tuple_from_minimal_tuple(mtup: *mut MinimalTupleData) -> HeapTupleData {
    let mlen = (*mtup).t_len as usize;
    let len = mlen + MINIMAL_TUPLE_OFFSET;
    let mut tuple = HeapTupleData {
        t_len: len as u32,
        t_self: invalid_item_pointer(),
        t_tableOid: InvalidOid,
        body: Some(alloc_tuple_body(len)),
    };
    let td = tuple.t_data_mut();

    // Copy the minimal tuple into the body starting at MINIMAL_TUPLE_OFFSET, then
    // zero the leading system-column region (up to t_infomask2).
    core::ptr::copy_nonoverlapping(
        mtup.cast::<u8>(),
        td.cast::<u8>().add(MINIMAL_TUPLE_OFFSET),
        mlen,
    );
    let lead = core::mem::offset_of!(HeapTupleHeaderData, t_infomask2);
    core::ptr::write_bytes(td.cast::<u8>(), 0, lead);

    tuple
}

/// `minimal_tuple_from_heap_tuple`: build a minimal tuple by copying from a heap
/// tuple, reserving `extra` MAXALIGN'd bytes before it.
///
/// SAFETY: `htup.t_data` must point at a valid body with `t_len` >
/// `MINIMAL_TUPLE_OFFSET`.
pub unsafe fn minimal_tuple_from_heap_tuple(htup: &HeapTupleData, extra: Size) -> *mut MinimalTupleData {
    crate::assert!(extra == MAXALIGN(extra));
    crate::assert!(htup.t_len as usize > MINIMAL_TUPLE_OFFSET);
    let len = htup.t_len as usize - MINIMAL_TUPLE_OFFSET;
    let block = alloc_minimal_block(len + extra);
    let result = minimal_at(block, extra);
    core::ptr::copy_nonoverlapping(
        htup.t_data().cast::<u8>().add(MINIMAL_TUPLE_OFFSET),
        result.cast::<u8>(),
        len,
    );
    (*result).t_len = len as u32;
    result
}

/// `varsize_any`: total size of any varlena form. Mainly a JIT inline helper.
///
/// SAFETY: `p` must point at a valid varlena.
pub unsafe fn varsize_any(p: *mut u8) -> usize {
    crate::varatt::VARSIZE_ANY(p)
}

/// `heap_expand_tuple` / `minimal_expand_tuple`: expand a tuple that has fewer
/// attributes than the descriptor requires, filling missing attributes from the
/// descriptor's missing values or NULL. Deferred until the missing-values path is
/// exercised by a column-added relation (it needs the same `fill_val` machinery,
/// which is translated, but no caller exists in M2 and it has no test coverage
/// yet); staged to keep the file honest about untested paths.
pub fn heap_expand_tuple(
    _source_tuple: &HeapTupleData,
    _tuple_desc: &TupleDescData,
) -> HeapTupleData {
    unimplemented!("heap_expand_tuple: missing-value expansion deferred (no M2 caller)")
}

/// See [`heap_expand_tuple`].
pub fn minimal_expand_tuple(
    _source_tuple: &HeapTupleData,
    _tuple_desc: &TupleDescData,
) -> *mut MinimalTupleData {
    unimplemented!("minimal_expand_tuple: missing-value expansion deferred (no M2 caller)")
}

// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::attnum::AttrNumber;
    use crate::access::tupdesc::TupleDescData;
    use crate::catalog::genbki::{INT4OID, INT8OID, TEXTOID};

    /// Build a descriptor for `specs` (name, type-oid) without catalog access.
    fn make_desc(specs: &[(&str, crate::postgres_ext::Oid)]) -> TupleDescData {
        let mut desc = TupleDescData::create_template(specs.len() as i32);
        for (i, (name, oid)) in specs.iter().enumerate() {
            desc.init_builtin_entry((i + 1) as AttrNumber, name, *oid, -1, 0);
        }
        desc
    }

    /// Build a 4-byte-header varlena holding `s` on the heap (leaked for the test
    /// duration). Layout: 4-byte length word (len<<2) + bytes.
    fn make_text(s: &str) -> Datum {
        let total = VARHDRSZ_TEST + s.len();
        let mut buf = vec![0u8; total].into_boxed_slice();
        // SET_VARSIZE: (total << 2) little-endian in the first 4 bytes.
        let hdr = (total as u32) << 2;
        buf[0..4].copy_from_slice(&hdr.to_le_bytes());
        buf[4..].copy_from_slice(s.as_bytes());
        let ptr = Box::into_raw(buf).cast::<u8>();
        PointerGetDatum(ptr)
    }

    const VARHDRSZ_TEST: usize = 4;

    fn read_text(d: Datum) -> String {
        // SAFETY: d is a valid varlena (possibly short-header after packing in
        // heap_form_tuple); decode either header form.
        unsafe {
            let p = DatumGetPointer(d);
            let (data_off, len) = if (*p & 0x01) == 0x01 {
                // 1-byte (short) header: size = byte>>1, data follows 1 byte.
                (1usize, (VARSIZE_SHORT(p) as usize) - 1)
            } else {
                // 4-byte header.
                (VARHDRSZ_TEST, (VARSIZE(p) as usize) - VARHDRSZ_TEST)
            };
            let data = core::slice::from_raw_parts(p.add(data_off), len);
            String::from_utf8(data.to_vec()).unwrap()
        }
    }

    #[test]
    fn form_deform_two_int4_known_layout() {
        let desc = make_desc(&[("a", INT4OID), ("b", INT4OID)]);
        let values = [Datum(1), Datum(2)];
        let isnull = [false, false];

        let tuple = heap_form_tuple(&desc, &values, &isnull);

        // No nulls -> t_hoff is MAXALIGN(SizeofHeapTupleHeader=23) = 24.
        // SAFETY: tuple body is valid for t_len bytes.
        unsafe {
            let td = &*tuple.t_data();
            assert_eq!(td.t_hoff, 24);
            assert_eq!(td.get_natts(), 2);
            assert_eq!(td.t_infomask & HEAP_HASNULL, 0);
            // data area: int4=1 then int4=2, each 4-byte aligned with no gap.
            let data = core::slice::from_raw_parts(tuple.t_data().cast::<u8>().add(24), 8);
            assert_eq!(&data[0..4], &1i32.to_le_bytes());
            assert_eq!(&data[4..8], &2i32.to_le_bytes());
            // total length = 24 + 8 = 32.
            assert_eq!(tuple.t_len, 32);
        }

        let (out, out_null) = unsafe { heap_deform_tuple(&tuple, &desc) };
        assert_eq!(out[0], Datum(1));
        assert_eq!(out[1], Datum(2));
        assert_eq!(out_null, vec![false, false]);

        heap_freetuple(tuple);
    }

    #[test]
    fn form_deform_mixed_with_null() {
        // int4, int8 (8-byte aligned), int4; middle column null.
        let desc = make_desc(&[("a", INT4OID), ("b", INT8OID), ("c", INT4OID)]);
        let values = [Datum(7), Datum(0), Datum(9)];
        let isnull = [false, true, false];

        let tuple = heap_form_tuple(&desc, &values, &isnull);

        // SAFETY: valid tuple body.
        unsafe {
            let td = &*tuple.t_data();
            assert_ne!(td.t_infomask & HEAP_HASNULL, 0);
            // bitmap present: t_hoff = MAXALIGN(23 + BITMAPLEN(3)=1) = MAXALIGN(24) = 24.
            assert_eq!(td.t_hoff, 24);
            // null bitmap: bit0=1 (a present), bit1=0 (b null), bit2=1 (c present)
            // -> 0b101 = 5.
            let bits = td.t_bits(3);
            assert_eq!(bits[0] & 0x07, 0b101);
        }

        let (out, out_null) = unsafe { heap_deform_tuple(&tuple, &desc) };
        assert_eq!(out[0], Datum(7));
        assert!(out_null[1]);
        assert_eq!(out[2], Datum(9));

        heap_freetuple(tuple);
    }

    #[test]
    fn form_deform_varlena_roundtrip() {
        // int4, text(varlena), int4 -- exercises alignment + varlena handling.
        let desc = make_desc(&[("a", INT4OID), ("t", TEXTOID), ("b", INT4OID)]);
        let t = make_text("hello world");
        let values = [Datum(42), t, Datum(99)];
        let isnull = [false, false, false];

        let tuple = heap_form_tuple(&desc, &values, &isnull);

        // SAFETY: valid tuple body.
        unsafe {
            let td = &*tuple.t_data();
            assert_ne!(td.t_infomask & HEAP_HASVARWIDTH, 0);
            assert_eq!(td.t_infomask & HEAP_HASNULL, 0);
        }

        let (out, out_null) = unsafe { heap_deform_tuple(&tuple, &desc) };
        assert_eq!(out[0], Datum(42));
        assert!(!out_null[1]);
        assert_eq!(read_text(out[1]), "hello world");
        assert_eq!(out[2], Datum(99));

        heap_freetuple(tuple);
    }

    #[test]
    fn getattr_by_attnum_including_null() {
        let desc = make_desc(&[("a", INT4OID), ("b", INT4OID), ("c", INT4OID)]);
        let values = [Datum(10), Datum(0), Datum(30)];
        let isnull = [false, true, false];
        let tuple = heap_form_tuple(&desc, &values, &isnull);

        // SAFETY: valid tuple body; attnums in range.
        unsafe {
            let (v1, n1) = heap_getattr(&tuple, 1, &desc);
            assert_eq!(v1, Datum(10));
            assert!(!n1);

            let (_v2, n2) = heap_getattr(&tuple, 2, &desc);
            assert!(n2);

            let (v3, n3) = heap_getattr(&tuple, 3, &desc);
            assert_eq!(v3, Datum(30));
            assert!(!n3);
        }

        heap_freetuple(tuple);
    }

    #[test]
    fn deform_int8_with_short_varlena_prefix() {
        // int8 placed AFTER a short (1-byte-header) varlena so its in-tuple
        // offset is not a multiple of 8 relative to the data-area start, yet the
        // tuple base is MAXALIGN(8). The int8 byval read therefore lands on an
        // unaligned address and must round-trip soundly (covers the same class
        // as the SIGBUS, end to end through form/deform). Under
        // `-Zmiri-symbolic-alignment-check` an aligned read here would be flagged.
        let desc = make_desc(&[("t", TEXTOID), ("n", INT8OID)]);
        let t = make_text("ab"); // -> short header (1 + 2 = 3 bytes), no alignment
        let values = [t, Datum(0x0102_0304_0506_0708)];
        let isnull = [false, false];
        let tuple = heap_form_tuple(&desc, &values, &isnull);

        let (out, out_null) = unsafe { heap_deform_tuple(&tuple, &desc) };
        assert_eq!(read_text(out[0]), "ab");
        assert_eq!(out[1], Datum(0x0102_0304_0506_0708));
        assert_eq!(out_null, vec![false, false]);

        heap_freetuple(tuple);
    }

    #[test]
    fn copytuple_roundtrip() {
        let desc = make_desc(&[("a", INT4OID), ("b", INT8OID)]);
        let values = [Datum(123), Datum(456)];
        let isnull = [false, false];
        let tuple = heap_form_tuple(&desc, &values, &isnull);

        // SAFETY: valid tuple body.
        let copy = unsafe { heap_copytuple(&tuple) };
        assert_eq!(copy.t_len, tuple.t_len);
        assert!(!copy.t_data_is_null());
        assert_ne!(copy.t_data(), tuple.t_data());

        let (out, _) = unsafe { heap_deform_tuple(&copy, &desc) };
        assert_eq!(out[0], Datum(123));
        assert_eq!(out[1], Datum(456));

        heap_freetuple(copy);
        heap_freetuple(tuple);
    }
}


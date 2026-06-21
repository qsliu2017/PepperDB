/*
 * brin_tuple.c
 *		Method implementations for tuples in BRIN indexes.
 *
 * Intended usage is that code outside this file only deals with
 * BrinMemTuples, and convert to and from the on-disk representation through
 * functions in this file.
 *
 * NOTES
 *
 * A BRIN tuple is similar to a heap tuple, with a few key differences.  The
 * first interesting difference is that the tuple header is much simpler, only
 * containing its total length and a small area for flags.  Also, the stored
 * data does not match the relation tuple descriptor exactly: for each
 * attribute in the descriptor, the index tuple carries an arbitrary number
 * of values, depending on the opclass.
 *
 * Also, for each column of the index relation there are two null bits: one
 * (hasnulls) stores whether any tuple within the page range has that column
 * set to null; the other one (allnulls) stores whether the column values are
 * all null.  If allnulls is true, then the tuple data area does not contain
 * values for that column at all; whereas it does if the hasnulls is set.
 * Note the size of the null bitmask may not be the same as that of the
 * datum array.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_tuple.c
 */
use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::access::attnum::AttrNumber;
use crate::access::common::tupdesc::TupleDesc;
use crate::c::{bits8, uint16, uint8};
use crate::storage::block::BlockNumber;

/*
 * This enables de-toasting of index entries.  Needed until VACUUM is
 * smart enough to rebuild indexes from scratch.
 */
// #define TOAST_INDEX_HACK

/* ============================================================
 * brin_tuple.h
 *		Declarations for dealing with BRIN-specific tuples.
 *
 * src/include/access/brin_tuple.h
 * ============================================================
 */

/*
 * The BRIN opclasses may register serialization callback, in case the on-disk
 * and in-memory representations differ (e.g. for performance reasons).
 */
pub type brin_serialize_callback_type =
    Option<unsafe fn(bdesc: *mut BrinDesc, src: Datum, dst: *mut Datum)>;

/*
 * A BRIN index stores one index tuple per page range.  Each index tuple
 * has one BrinValues struct for each indexed column; in turn, each BrinValues
 * has (besides the null flags) an array of Datum whose size is determined by
 * the opclass.
 */
#[repr(C)]
pub struct BrinValues {
    pub bv_attno: AttrNumber,    /* index attribute number */
    pub bv_hasnulls: bool,       /* are there any nulls in the page range? */
    pub bv_allnulls: bool,       /* are all values nulls in the page range? */
    pub bv_values: *mut Datum,   /* current accumulated values */
    pub bv_mem_value: Datum,     /* expanded accumulated values */
    pub bv_context: MemoryContext,
    pub bv_serialize: brin_serialize_callback_type,
}

/*
 * This struct is used to represent an in-memory index tuple.  The values can
 * only be meaningfully decoded with an appropriate BrinDesc.
 */
#[repr(C)]
pub struct BrinMemTuple {
    pub bt_placeholder: bool, /* this is a placeholder tuple */
    pub bt_empty_range: bool, /* range represents no tuples */
    pub bt_blkno: BlockNumber, /* heap blkno that the tuple is for */
    pub bt_context: MemoryContext, /* memcxt holding the bt_columns values */
    /* output arrays for brin_deform_tuple: */
    pub bt_values: *mut Datum, /* values array */
    pub bt_allnulls: *mut bool, /* allnulls array */
    pub bt_hasnulls: *mut bool, /* hasnulls array */
    /* not an output array, but must be last */
    pub bt_columns: [BrinValues; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

/*
 * An on-disk BRIN tuple.  This is possibly followed by a nulls bitmask, with
 * room for 2 null bits (two bits for each indexed column); an opclass-defined
 * number of Datum values for each column follow.
 */
#[repr(C)]
pub struct BrinTuple {
    /* heap block number that the tuple is for */
    pub bt_blkno: BlockNumber,

    /* ---------------
     * bt_info is laid out in the following fashion:
     *
     * 7th (high) bit: has nulls
     * 6th bit: is placeholder tuple
     * 5th bit: range is empty
     * 4-0 bit: offset of data
     * ---------------
     */
    pub bt_info: uint8,
}

pub const SizeOfBrinTuple: Size =
    core::mem::offset_of!(BrinTuple, bt_info) + core::mem::size_of::<uint8>();

/*
 * bt_info manipulation macros
 */
pub const BRIN_OFFSET_MASK: uint8 = 0x1F;
pub const BRIN_EMPTY_RANGE_MASK: uint8 = 0x20;
pub const BRIN_PLACEHOLDER_MASK: uint8 = 0x40;
pub const BRIN_NULLS_MASK: uint8 = 0x80;

#[inline]
pub unsafe fn BrinTupleDataOffset(tup: *const BrinTuple) -> Size {
    ((*tup).bt_info & BRIN_OFFSET_MASK) as Size
}

#[inline]
pub unsafe fn BrinTupleHasNulls(tup: *const BrinTuple) -> bool {
    ((*tup).bt_info & BRIN_NULLS_MASK) != 0
}

#[inline]
pub unsafe fn BrinTupleIsPlaceholder(tup: *const BrinTuple) -> bool {
    ((*tup).bt_info & BRIN_PLACEHOLDER_MASK) != 0
}

#[inline]
pub unsafe fn BrinTupleIsEmptyRange(tup: *const BrinTuple) -> bool {
    ((*tup).bt_info & BRIN_EMPTY_RANGE_MASK) != 0
}

/* ============================================================
 * brin_tuple.c implementation
 * ============================================================
 */

/*
 * Return a tuple descriptor used for on-disk storage of BRIN tuples.
 */
unsafe fn brtuple_disk_tupdesc(brdesc: *mut BrinDesc) -> TupleDesc {
    /* We cache these in the BrinDesc */
    if (*brdesc).bd_disktdesc.is_null() {
        let i: c_int;
        let j: c_int;
        let mut attno: AttrNumber = 1;
        let tupdesc: TupleDesc;
        let oldcxt: MemoryContext;

        /* make sure it's in the bdesc's context */
        oldcxt = MemoryContextSwitchTo((*brdesc).bd_context);

        tupdesc = CreateTemplateTupleDesc((*brdesc).bd_totalstored);

        let mut i = 0;
        while i < (*(*brdesc).bd_tupdesc).natts {
            let mut j = 0;
            while j < (*(*(*brdesc).bd_info.as_ptr().add(i as usize))).oi_nstored {
                TupleDescInitEntry(
                    tupdesc,
                    {
                        let cur = attno;
                        attno += 1;
                        cur
                    },
                    std::ptr::null(),
                    (*(*(*(*brdesc).bd_info.as_ptr().add(i as usize)))
                        .oi_typcache[j as usize])
                        .type_id,
                    -1,
                    0,
                );
                j += 1;
            }
            i += 1;
        }

        MemoryContextSwitchTo(oldcxt);

        (*brdesc).bd_disktdesc = tupdesc;
    }

    (*brdesc).bd_disktdesc
}

/*
 * Generate a new on-disk tuple to be inserted in a BRIN index.
 *
 * See brin_form_placeholder_tuple if you touch this.
 */
pub unsafe fn brin_form_tuple(
    brdesc: *mut BrinDesc,
    blkno: BlockNumber,
    tuple: *mut BrinMemTuple,
    size: *mut Size,
) -> *mut BrinTuple {
    let values: *mut Datum;
    let nulls: *mut bool;
    let mut anynulls: bool = false;
    let rettuple: *mut BrinTuple;
    let mut keyno: c_int;
    let mut idxattno: c_int;
    let mut phony_infomask: uint16 = 0;
    let phony_nullbitmap: *mut bits8;
    let mut len: Size;
    let hoff: Size;
    let data_len: Size;
    let i: c_int;

    /* TOAST_INDEX_HACK */
    let untoasted_values: *mut Datum;
    let mut nuntoasted: c_int = 0;

    Assert!((*brdesc).bd_totalstored > 0);

    values = palloc(std::mem::size_of::<Datum>() * (*brdesc).bd_totalstored as usize)
        as *mut Datum;
    nulls = palloc0(std::mem::size_of::<bool>() * (*brdesc).bd_totalstored as usize)
        as *mut bool;
    phony_nullbitmap = palloc(
        std::mem::size_of::<bits8>() * BITMAPLEN((*brdesc).bd_totalstored) as usize,
    ) as *mut bits8;

    /* TOAST_INDEX_HACK */
    untoasted_values =
        palloc(std::mem::size_of::<Datum>() * (*brdesc).bd_totalstored as usize)
            as *mut Datum;

    /*
     * Set up the values/nulls arrays for heap_fill_tuple
     */
    idxattno = 0;
    keyno = 0;
    while keyno < (*(*brdesc).bd_tupdesc).natts {
        let mut datumno: c_int;

        let col = (*tuple).bt_columns.as_mut_ptr().offset(keyno as isize);

        /*
         * "allnulls" is set when there's no nonnull value in any row in the
         * column; when this happens, there is no data to store.  Thus set the
         * nullable bits for all data elements of this column and we're done.
         */
        if (*col).bv_allnulls {
            datumno = 0;
            while datumno < (*(*(*brdesc).bd_info.as_ptr().add(keyno as usize))).oi_nstored as c_int {
                *nulls.offset(idxattno as isize) = true;
                idxattno += 1;
                datumno += 1;
            }
            anynulls = true;
            keyno += 1;
            continue;
        }

        /*
         * The "hasnulls" bit is set when there are some null values in the
         * data.  We still need to store a real value, but the presence of
         * this means we need a null bitmap.
         */
        if (*col).bv_hasnulls {
            anynulls = true;
        }

        /* If needed, serialize the values before forming the on-disk tuple. */
        if let Some(serialize) = (*col).bv_serialize {
            serialize(brdesc, (*col).bv_mem_value, (*col).bv_values);
        }

        /*
         * Now obtain the values of each stored datum.  Note that some values
         * might be toasted, and we cannot rely on the original heap values
         * sticking around forever, so we must detoast them.  Also try to
         * compress them.
         */
        datumno = 0;
        while datumno < (*(*(*brdesc).bd_info.as_ptr().add(keyno as usize))).oi_nstored as c_int {
            let mut value: Datum = *(*col).bv_values.offset(datumno as isize);

            /* TOAST_INDEX_HACK */

            /* We must look at the stored type, not at the index descriptor. */
            let atttype: *mut TypeCacheEntry =
                (*(*(*brdesc).bd_info.as_ptr().add(keyno as usize))).oi_typcache[datumno as usize];

            /* Do we need to free the value at the end? */
            let mut free_value: bool = false;

            /* For non-varlena types we don't need to do anything special */
            if (*atttype).typlen != -1 {
                *values.offset(idxattno as isize) = value;
                idxattno += 1;
                datumno += 1;
                continue;
            }

            /*
             * Do nothing if value is not of varlena type. We don't need to
             * care about NULL values here, thanks to bv_allnulls above.
             *
             * If value is stored EXTERNAL, must fetch it so we are not
             * depending on outside storage.
             *
             * XXX Is this actually true? Could it be that the summary is NULL
             * even for range with non-NULL data? E.g. degenerate bloom filter
             * may be thrown away, etc.
             */
            if VARATT_IS_EXTERNAL(DatumGetPointer(value) as *mut _) {
                value = PointerGetDatum(detoast_external_attr(
                    DatumGetPointer(value) as *mut varlena,
                ) as *const _);
                free_value = true;
            }

            /*
             * If value is above size target, and is of a compressible
             * datatype, try to compress it in-line.
             */
            if !VARATT_IS_EXTENDED(DatumGetPointer(value) as *mut _)
                && VARSIZE(DatumGetPointer(value) as *mut _) > TOAST_INDEX_TARGET
                && ((*atttype).typstorage == TYPSTORAGE_EXTENDED
                    || (*atttype).typstorage == TYPSTORAGE_MAIN)
            {
                let cvalue: Datum;
                let compression: c_char;
                let att: Form_pg_attribute =
                    TupleDescAttr((*brdesc).bd_tupdesc, keyno as usize);

                /*
                 * If the BRIN summary and indexed attribute use the same data
                 * type and it has a valid compression method, we can use the
                 * same compression method. Otherwise we have to use the
                 * default method.
                 */
                if (*att).atttypid == (*atttype).type_id {
                    compression = (*att).attcompression;
                } else {
                    compression = InvalidCompressionMethod;
                }

                cvalue = toast_compress_datum(value, compression);

                if !DatumGetPointer(cvalue).is_null() {
                    /* successful compression */
                    if free_value {
                        pfree(DatumGetPointer(value) as *mut _);
                    }

                    value = cvalue;
                    free_value = true;
                }
            }

            /*
             * If we untoasted / compressed the value, we need to free it
             * after forming the index tuple.
             */
            if free_value {
                *untoasted_values.offset(nuntoasted as isize) = value;
                nuntoasted += 1;
            }

            /* end TOAST_INDEX_HACK */

            *values.offset(idxattno as isize) = value;
            idxattno += 1;
            datumno += 1;
        }

        keyno += 1;
    }

    /* Assert we did not overrun temp arrays */
    Assert!(idxattno <= (*brdesc).bd_totalstored);

    /* compute total space needed */
    len = SizeOfBrinTuple;
    if anynulls {
        /*
         * We need a double-length bitmap on an on-disk BRIN index tuple; the
         * first half stores the "allnulls" bits, the second stores
         * "hasnulls".
         */
        len += BITMAPLEN((*(*brdesc).bd_tupdesc).natts * 2) as Size;
    }

    len = MAXALIGN(len);
    hoff = len;

    data_len = heap_compute_data_size(brtuple_disk_tupdesc(brdesc), values, nulls);
    len += data_len;

    len = MAXALIGN(len);

    rettuple = palloc0(len) as *mut BrinTuple;
    (*rettuple).bt_blkno = blkno;
    (*rettuple).bt_info = hoff as uint8;

    /* Assert that hoff fits in the space available */
    Assert!(((*rettuple).bt_info & BRIN_OFFSET_MASK) as Size == hoff);

    /*
     * The infomask and null bitmap as computed by heap_fill_tuple are useless
     * to us.  However, that function will not accept a null infomask; and we
     * need to pass a valid null bitmap so that it will correctly skip
     * outputting null attributes in the data area.
     */
    heap_fill_tuple(
        brtuple_disk_tupdesc(brdesc),
        values,
        nulls,
        (rettuple as *mut c_char).offset(hoff as isize),
        data_len,
        &mut phony_infomask,
        phony_nullbitmap,
    );

    /* done with these */
    pfree(values as *mut _);
    pfree(nulls as *mut _);
    pfree(phony_nullbitmap as *mut _);

    /* TOAST_INDEX_HACK */
    let mut i = 0;
    while i < nuntoasted {
        pfree(DatumGetPointer(*untoasted_values.offset(i as isize)) as *mut _);
        i += 1;
    }
    let _ = i;

    /*
     * Now fill in the real null bitmasks.  allnulls first.
     */
    if anynulls {
        let mut bitP: *mut bits8;
        let mut bitmask: c_int;

        (*rettuple).bt_info |= BRIN_NULLS_MASK;

        /*
         * Note that we reverse the sense of null bits in this module: we
         * store a 1 for a null attribute rather than a 0.  So we must reverse
         * the sense of the att_isnull test in brin_deconstruct_tuple as well.
         */
        bitP = ((rettuple as *mut c_char).offset(SizeOfBrinTuple as isize) as *mut bits8)
            .offset(-1);
        bitmask = HIGHBIT as c_int;
        keyno = 0;
        while keyno < (*(*brdesc).bd_tupdesc).natts {
            if bitmask != HIGHBIT as c_int {
                bitmask <<= 1;
            } else {
                bitP = bitP.offset(1);
                *bitP = 0x0;
                bitmask = 1;
            }

            if !(*(*tuple).bt_columns.as_ptr().offset(keyno as isize)).bv_allnulls {
                keyno += 1;
                continue;
            }

            *bitP |= bitmask as bits8;
            keyno += 1;
        }
        /* hasnulls bits follow */
        keyno = 0;
        while keyno < (*(*brdesc).bd_tupdesc).natts {
            if bitmask != HIGHBIT as c_int {
                bitmask <<= 1;
            } else {
                bitP = bitP.offset(1);
                *bitP = 0x0;
                bitmask = 1;
            }

            if !(*(*tuple).bt_columns.as_ptr().offset(keyno as isize)).bv_hasnulls {
                keyno += 1;
                continue;
            }

            *bitP |= bitmask as bits8;
            keyno += 1;
        }
    }

    if (*tuple).bt_placeholder {
        (*rettuple).bt_info |= BRIN_PLACEHOLDER_MASK;
    }

    if (*tuple).bt_empty_range {
        (*rettuple).bt_info |= BRIN_EMPTY_RANGE_MASK;
    }

    *size = len;
    rettuple
}

/*
 * Generate a new on-disk tuple with no data values, marked as placeholder.
 *
 * This is a cut-down version of brin_form_tuple.
 */
pub unsafe fn brin_form_placeholder_tuple(
    brdesc: *mut BrinDesc,
    blkno: BlockNumber,
    size: *mut Size,
) -> *mut BrinTuple {
    let mut len: Size;
    let hoff: Size;
    let rettuple: *mut BrinTuple;
    let mut keyno: c_int;
    let mut bitP: *mut bits8;
    let mut bitmask: c_int;

    /* compute total space needed: always add nulls */
    len = SizeOfBrinTuple;
    len += BITMAPLEN((*(*brdesc).bd_tupdesc).natts * 2) as Size;
    len = MAXALIGN(len);
    hoff = len;

    rettuple = palloc0(len) as *mut BrinTuple;
    (*rettuple).bt_blkno = blkno;
    (*rettuple).bt_info = hoff as uint8;
    (*rettuple).bt_info |=
        BRIN_NULLS_MASK | BRIN_PLACEHOLDER_MASK | BRIN_EMPTY_RANGE_MASK;

    bitP =
        ((rettuple as *mut c_char).offset(SizeOfBrinTuple as isize) as *mut bits8).offset(-1);
    bitmask = HIGHBIT as c_int;
    /* set allnulls true for all attributes */
    keyno = 0;
    while keyno < (*(*brdesc).bd_tupdesc).natts {
        if bitmask != HIGHBIT as c_int {
            bitmask <<= 1;
        } else {
            bitP = bitP.offset(1);
            *bitP = 0x0;
            bitmask = 1;
        }

        *bitP |= bitmask as bits8;
        keyno += 1;
    }
    /* no need to set hasnulls */

    *size = len;
    rettuple
}

/*
 * Free a tuple created by brin_form_tuple
 */
pub unsafe fn brin_free_tuple(tuple: *mut BrinTuple) {
    pfree(tuple as *mut _);
}

/*
 * Given a brin tuple of size len, create a copy of it.  If 'dest' is not
 * NULL, its size is destsz, and can be used as output buffer; if the tuple
 * to be copied does not fit, it is enlarged by repalloc, and the size is
 * updated to match.  This avoids palloc/free cycles when many brin tuples
 * are being processed in loops.
 */
pub unsafe fn brin_copy_tuple(
    tuple: *mut BrinTuple,
    len: Size,
    mut dest: *mut BrinTuple,
    destsz: *mut Size,
) -> *mut BrinTuple {
    if destsz.is_null() || *destsz == 0 {
        dest = palloc(len) as *mut BrinTuple;
    } else if len > *destsz {
        dest = repalloc(dest as *mut _, len) as *mut BrinTuple;
        *destsz = len;
    }

    memcpy(dest as *mut _, tuple as *const _, len);

    dest
}

/*
 * Return whether two BrinTuples are bitwise identical.
 */
pub unsafe fn brin_tuples_equal(
    a: *const BrinTuple,
    alen: Size,
    b: *const BrinTuple,
    blen: Size,
) -> bool {
    if alen != blen {
        return false;
    }
    if memcmp(a as *const _, b as *const _, alen) != 0 {
        return false;
    }
    true
}

/*
 * Create a new BrinMemTuple from scratch, and initialize it to an empty
 * state.
 *
 * Note: we don't provide any means to free a deformed tuple, so make sure to
 * use a temporary memory context.
 */
pub unsafe fn brin_new_memtuple(brdesc: *mut BrinDesc) -> *mut BrinMemTuple {
    let dtup: *mut BrinMemTuple;
    let basesize: std::ffi::c_long;

    basesize = MAXALIGN(
        std::mem::size_of::<BrinMemTuple>()
            + std::mem::size_of::<BrinValues>() * (*(*brdesc).bd_tupdesc).natts as usize,
    ) as std::ffi::c_long;
    dtup = palloc0(
        basesize as usize
            + std::mem::size_of::<Datum>() * (*brdesc).bd_totalstored as usize,
    ) as *mut BrinMemTuple;

    (*dtup).bt_values =
        palloc(std::mem::size_of::<Datum>() * (*brdesc).bd_totalstored as usize)
            as *mut Datum;
    (*dtup).bt_allnulls =
        palloc(std::mem::size_of::<bool>() * (*(*brdesc).bd_tupdesc).natts as usize)
            as *mut bool;
    (*dtup).bt_hasnulls =
        palloc(std::mem::size_of::<bool>() * (*(*brdesc).bd_tupdesc).natts as usize)
            as *mut bool;

    (*dtup).bt_empty_range = true;

    (*dtup).bt_context = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"brin dtuple".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );

    brin_memtuple_initialize(dtup, brdesc);

    dtup
}

/*
 * Reset a BrinMemTuple to initial state.  We return the same tuple, for
 * notational convenience.
 */
pub unsafe fn brin_memtuple_initialize(
    dtuple: *mut BrinMemTuple,
    brdesc: *mut BrinDesc,
) -> *mut BrinMemTuple {
    let i: c_int;
    let mut currdatum: *mut c_char;

    MemoryContextReset((*dtuple).bt_context);

    currdatum = (dtuple as *mut c_char).offset(MAXALIGN(
        std::mem::size_of::<BrinMemTuple>()
            + std::mem::size_of::<BrinValues>() * (*(*brdesc).bd_tupdesc).natts as usize,
    ) as isize);
    let mut i = 0;
    while i < (*(*brdesc).bd_tupdesc).natts {
        let col = (*dtuple).bt_columns.as_mut_ptr().offset(i as isize);
        (*col).bv_attno = (i + 1) as AttrNumber;
        (*col).bv_allnulls = true;
        (*col).bv_hasnulls = false;
        (*col).bv_values = currdatum as *mut Datum;

        (*col).bv_mem_value = PointerGetDatum(std::ptr::null());
        (*col).bv_serialize = None;
        (*col).bv_context = (*dtuple).bt_context;

        currdatum = currdatum.offset(
            (std::mem::size_of::<Datum>()
                * (*(*(*brdesc).bd_info.as_ptr().add(i as usize))).oi_nstored as usize)
                as isize,
        );
        i += 1;
    }
    let _ = i;

    (*dtuple).bt_empty_range = true;

    dtuple
}

/*
 * Convert a BrinTuple back to a BrinMemTuple.  This is the reverse of
 * brin_form_tuple.
 *
 * As an optimization, the caller can pass a previously allocated 'dMemtuple'.
 * This avoids having to allocate it here, which can be useful when this
 * function is called many times in a loop.  It is caller's responsibility
 * that the given BrinMemTuple matches what we need here.
 *
 * Note we don't need the "on disk tupdesc" here; we rely on our own routine to
 * deconstruct the tuple from the on-disk format.
 */
pub unsafe fn brin_deform_tuple(
    brdesc: *mut BrinDesc,
    tuple: *mut BrinTuple,
    dMemtuple: *mut BrinMemTuple,
) -> *mut BrinMemTuple {
    let dtup: *mut BrinMemTuple;
    let values: *mut Datum;
    let allnulls: *mut bool;
    let hasnulls: *mut bool;
    let tp: *mut c_char;
    let nullbits: *mut bits8;
    let mut keyno: c_int;
    let mut valueno: c_int;
    let oldcxt: MemoryContext;

    dtup = if !dMemtuple.is_null() {
        brin_memtuple_initialize(dMemtuple, brdesc)
    } else {
        brin_new_memtuple(brdesc)
    };

    if BrinTupleIsPlaceholder(tuple) {
        (*dtup).bt_placeholder = true;
    }

    /* ranges start as empty, depends on the BrinTuple */
    if !BrinTupleIsEmptyRange(tuple) {
        (*dtup).bt_empty_range = false;
    }

    (*dtup).bt_blkno = (*tuple).bt_blkno;

    values = (*dtup).bt_values;
    allnulls = (*dtup).bt_allnulls;
    hasnulls = (*dtup).bt_hasnulls;

    tp = (tuple as *mut c_char).offset(BrinTupleDataOffset(tuple) as isize);

    if BrinTupleHasNulls(tuple) {
        nullbits = (tuple as *mut c_char).offset(SizeOfBrinTuple as isize) as *mut bits8;
    } else {
        nullbits = std::ptr::null_mut();
    }
    brin_deconstruct_tuple(
        brdesc,
        tp,
        nullbits,
        BrinTupleHasNulls(tuple),
        values,
        allnulls,
        hasnulls,
    );

    /*
     * Iterate to assign each of the values to the corresponding item in the
     * values array of each column.  The copies occur in the tuple's context.
     */
    oldcxt = MemoryContextSwitchTo((*dtup).bt_context);
    valueno = 0;
    keyno = 0;
    while keyno < (*(*brdesc).bd_tupdesc).natts {
        let mut i: c_int;

        if *allnulls.offset(keyno as isize) {
            valueno += (*(*(*brdesc).bd_info.as_ptr().add(keyno as usize))).oi_nstored as c_int;
            keyno += 1;
            continue;
        }

        let col = (*dtup).bt_columns.as_mut_ptr().offset(keyno as isize);

        /*
         * We would like to skip datumCopy'ing the values datum in some cases,
         * caller permitting ...
         */
        i = 0;
        while i < (*(*(*brdesc).bd_info.as_ptr().add(keyno as usize))).oi_nstored as c_int {
            *(*col).bv_values.offset(i as isize) = datumCopy(
                *values.offset(valueno as isize),
                (*(*(*(*brdesc).bd_info.as_ptr().add(keyno as usize))).oi_typcache[i as usize])
                    .typbyval,
                (*(*(*(*brdesc).bd_info.as_ptr().add(keyno as usize))).oi_typcache[i as usize])
                    .typlen,
            );
            valueno += 1;
            i += 1;
        }

        (*col).bv_hasnulls = *hasnulls.offset(keyno as isize);
        (*col).bv_allnulls = false;

        (*col).bv_mem_value = PointerGetDatum(std::ptr::null());
        (*col).bv_serialize = None;
        (*col).bv_context = (*dtup).bt_context;

        keyno += 1;
    }

    MemoryContextSwitchTo(oldcxt);

    dtup
}

/*
 * brin_deconstruct_tuple
 *		Guts of attribute extraction from an on-disk BRIN tuple.
 *
 * Its arguments are:
 *	brdesc		BRIN descriptor for the stored tuple
 *	tp			pointer to the tuple data area
 *	nullbits	pointer to the tuple nulls bitmask
 *	nulls		"has nulls" bit in tuple infomask
 *	values		output values, array of size brdesc->bd_totalstored
 *	allnulls	output "allnulls", size brdesc->bd_tupdesc->natts
 *	hasnulls	output "hasnulls", size brdesc->bd_tupdesc->natts
 *
 * Output arrays must have been allocated by caller.
 */
#[inline]
unsafe fn brin_deconstruct_tuple(
    brdesc: *mut BrinDesc,
    tp: *mut c_char,
    nullbits: *mut bits8,
    nulls: bool,
    values: *mut Datum,
    allnulls: *mut bool,
    hasnulls: *mut bool,
) {
    let mut attnum: c_int;
    let mut stored: c_int;
    let diskdsc: TupleDesc;
    let mut off: std::ffi::c_long;

    /*
     * First iterate to natts to obtain both null flags for each attribute.
     * Note that we reverse the sense of the att_isnull test, because we store
     * 1 for a null value (rather than a 1 for a not null value as is the
     * att_isnull convention used elsewhere.)  See brin_form_tuple.
     */
    attnum = 0;
    while attnum < (*(*brdesc).bd_tupdesc).natts {
        /*
         * the "all nulls" bit means that all values in the page range for
         * this column are nulls.  Therefore there are no values in the tuple
         * data area.
         */
        *allnulls.offset(attnum as isize) = nulls && !att_isnull(attnum, nullbits);

        /*
         * the "has nulls" bit means that some tuples have nulls, but others
         * have not-null values.  Therefore we know the tuple contains data
         * for this column.
         *
         * The hasnulls bits follow the allnulls bits in the same bitmask.
         */
        *hasnulls.offset(attnum as isize) =
            nulls && !att_isnull((*(*brdesc).bd_tupdesc).natts + attnum, nullbits);

        attnum += 1;
    }

    /*
     * Iterate to obtain each attribute's stored values.  Note that since we
     * may reuse attribute entries for more than one column, we cannot cache
     * offsets here.
     */
    diskdsc = brtuple_disk_tupdesc(brdesc);
    stored = 0;
    off = 0;
    attnum = 0;
    while attnum < (*(*brdesc).bd_tupdesc).natts {
        let mut datumno: c_int;

        if *allnulls.offset(attnum as isize) {
            stored += (*(*(*brdesc).bd_info.as_ptr().add(attnum as usize))).oi_nstored as c_int;
            attnum += 1;
            continue;
        }

        datumno = 0;
        while datumno < (*(*(*brdesc).bd_info.as_ptr().add(attnum as usize))).oi_nstored as c_int {
            let thisatt: *mut CompactAttribute = TupleDescCompactAttr(diskdsc, stored);

            if (*thisatt).attlen == -1 {
                off = att_pointer_alignby(
                    off,
                    (*thisatt).attalignby,
                    -1,
                    tp.offset(off as isize),
                );
            } else {
                /* not varlena, so safe to use att_nominal_alignby */
                off = att_nominal_alignby(off, (*thisatt).attalignby);
            }

            *values.offset(stored as isize) = fetchatt(thisatt, tp.offset(off as isize));
            stored += 1;

            off = att_addlength_pointer(off, (*thisatt).attlen, tp.offset(off as isize));

            datumno += 1;
        }

        attnum += 1;
    }
}

/* ============================================================
 * Local stubs for unported dependencies
 * ============================================================
 */

pub use crate::access::brin::brin_internal::{BrinDesc, BrinOpcInfo};
pub use crate::utils::cache::typcache::TypeCacheEntry;

#[repr(C)]
pub struct CompactAttribute {
    pub attlen: i16,
    pub attalignby: u8,
}

#[repr(C)]
pub struct FormData_pg_attribute {
    pub atttypid: Oid,
    pub attcompression: c_char,
}
pub type Form_pg_attribute = *mut FormData_pg_attribute;

#[repr(C)]
pub struct varlena {
    pub vl_len_: [c_char; 4],
    pub vl_dat: [c_char; 0],
}

pub const HIGHBIT: uint8 = 0x80;
pub const TOAST_INDEX_TARGET: u32 = (8192 / 16) as u32;
pub const TYPSTORAGE_EXTENDED: c_char = b'x' as c_char;
pub const TYPSTORAGE_MAIN: c_char = b'm' as c_char;
pub const InvalidCompressionMethod: c_char = 0;

#[inline]
unsafe fn BITMAPLEN(natts: c_int) -> c_int {
    (natts + 7) / 8
}

extern "C" {
    fn memcpy(dest: *mut std::ffi::c_void, src: *const std::ffi::c_void, n: usize)
        -> *mut std::ffi::c_void;
    fn memcmp(a: *const std::ffi::c_void, b: *const std::ffi::c_void, n: usize) -> c_int;
}

unsafe fn CreateTemplateTupleDesc(_natts: c_int) -> TupleDesc { crate::access::common::tupdesc::CreateTemplateTupleDesc(_natts) }

unsafe fn TupleDescInitEntry(
    _desc: TupleDesc,
    _attno: AttrNumber,
    _attname: *const c_char,
    _oidtypeid: Oid,
    _typmod: i32,
    _attdim: c_int,
) { crate::access::common::tupdesc::TupleDescInitEntry(_desc, _attno, _attname, _oidtypeid, _typmod as _, _attdim) }

unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: usize) -> Form_pg_attribute {
    unimplemented!() // TODO: access/common/tupdesc.h
}

unsafe fn TupleDescCompactAttr(_tupdesc: TupleDesc, _i: c_int) -> *mut CompactAttribute {
    unimplemented!() // TODO: access/common/tupdesc.h
}

unsafe fn heap_compute_data_size(
    _tupdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> Size { crate::access::common::heaptuple::heap_compute_data_size(_tupdesc, _values as _, _isnull as _) }

unsafe fn heap_fill_tuple(
    _tupleDesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
    _data: *mut c_char,
    _data_size: Size,
    _infomask: *mut uint16,
    _bit: *mut bits8,
) { crate::access::common::heaptuple::heap_fill_tuple(_tupleDesc, _values as _, _isnull as _, _data, _data_size, _infomask, _bit) }

unsafe fn att_isnull(_ATT: c_int, _BITS: *const bits8) -> bool { crate::access::tupmacs::att_isnull(_ATT, _BITS) }

unsafe fn att_pointer_alignby(
    _cur_offset: std::ffi::c_long,
    _attalignby: u8,
    _attlen: c_int,
    _attptr: *const c_char,
) -> std::ffi::c_long { crate::access::tupmacs::att_pointer_alignby(_cur_offset as _, _attalignby, _attlen, _attptr) as _ }

unsafe fn att_nominal_alignby(
    _cur_offset: std::ffi::c_long,
    _attalignby: u8,
) -> std::ffi::c_long { crate::access::tupmacs::att_nominal_alignby(_cur_offset as _, _attalignby) as _ }

unsafe fn att_addlength_pointer(
    _cur_offset: std::ffi::c_long,
    _attlen: i16,
    _attptr: *const c_char,
) -> std::ffi::c_long { crate::access::tupmacs::att_addlength_pointer(_cur_offset as _, _attlen as _, _attptr) as _ }

unsafe fn fetchatt(_A: *const CompactAttribute, _T: *const c_char) -> Datum {
    unimplemented!() // TODO: access/tupmacs.h
}

unsafe fn datumCopy(_value: Datum, _typByVal: bool, _typLen: i16) -> Datum {
    unimplemented!() // TODO: utils/adt/datum.c
}

unsafe fn detoast_external_attr(_attr: *mut varlena) -> *mut varlena { unimplemented!() }

unsafe fn toast_compress_datum(_value: Datum, _cmethod: c_char) -> Datum { crate::access::common::toast_internals::toast_compress_datum(_value, _cmethod) }

unsafe fn VARATT_IS_EXTERNAL(_PTR: *mut varlena) -> bool {
    unimplemented!() // TODO: c.h / postgres.h
}

unsafe fn VARATT_IS_EXTENDED(_PTR: *mut varlena) -> bool {
    unimplemented!() // TODO: c.h / postgres.h
}

unsafe fn VARSIZE(_PTR: *mut varlena) -> u32 {
    unimplemented!() // TODO: c.h / postgres.h
}


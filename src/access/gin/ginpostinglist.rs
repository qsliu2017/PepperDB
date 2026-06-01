//! ginpostinglist.c
//!   routines for dealing with posting lists.
//!
//! 1:1 translation of src/backend/access/gin/ginpostinglist.c, plus the
//! GinPostingList struct + SizeOfGinPostingList / GinNextPostingListSegment
//! from access/ginblock.h.
//!
//! A self-contained varbyte codec that compresses sorted ItemPointers into GIN
//! posting lists. The encoding represents each item pointer as a 64-bit integer
//! (block << 11 | offset), delta-encodes consecutive (sorted) items, then
//! varbyte-encodes the deltas (7 data bits/byte, high bit = continuation,
//! little-endian).
//!
//! #include "postgres.h"          -> use crate::prelude::*
//! #include "access/gin_private.h" -> itemptr / block / off helpers + tidbitmap

use crate::prelude::*;

use crate::storage::block::BlockNumber;
use crate::storage::itemptr::{
    ItemPointer, ItemPointerCompare, ItemPointerData, ItemPointerGetBlockNumber,
    ItemPointerGetOffsetNumber, ItemPointerSetBlockNumber, ItemPointerSetOffsetNumber,
};
use crate::storage::off::{OffsetNumber, OffsetNumberIsValid};

use crate::nodes::tidbitmap::{tbm_add_tuples, TIDBitmap};

// memmove/memcpy are libc primitives; PepperDB forbids the `libc` crate, so we
// declare the one we need locally (matching the convention in heaptuple.rs).
extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

// ---------------------------------------------------------------------------
// ginblock.h: the on-disk struct + size macros.
// ---------------------------------------------------------------------------

/// A compressed posting list.
///
/// Note: This requires 2-byte alignment.
///
/// ```c
/// typedef struct {
///     ItemPointerData first;   /* first item in this posting list (unpacked) */
///     uint16 nbytes;           /* number of bytes that follow */
///     unsigned char bytes[FLEXIBLE_ARRAY_MEMBER]; /* varbyte encoded items */
/// } GinPostingList;
/// ```
#[repr(C)]
pub struct GinPostingList {
    pub first: ItemPointerData,
    pub nbytes: uint16,
    pub bytes: [u8; 0], /* FLEXIBLE_ARRAY_MEMBER: varbyte encoded items */
}

/// `SHORTALIGN(x)` from c.h: round up to the next 2-byte boundary.
#[inline]
const fn shortalign(x: usize) -> usize {
    (x + 1) & !1
}

/// SizeOfGinPostingList(plist) =
///     offsetof(GinPostingList, bytes) + SHORTALIGN(plist->nbytes)
///
/// # Safety
/// `plist` references a valid GinPostingList.
#[inline]
pub unsafe fn SizeOfGinPostingList(plist: *const GinPostingList) -> Size {
    core::mem::offset_of!(GinPostingList, bytes) + shortalign((*plist).nbytes as usize)
}

/// GinNextPostingListSegment(cur) =
///     (GinPostingList *) (((char *) cur) + SizeOfGinPostingList(cur))
///
/// # Safety
/// `cur` references a valid GinPostingList segment.
#[inline]
unsafe fn GinNextPostingListSegment(cur: *mut GinPostingList) -> *mut GinPostingList {
    (cur as *mut u8).add(SizeOfGinPostingList(cur)) as *mut GinPostingList
}

// ---------------------------------------------------------------------------
// Encoding constants (ginpostinglist.c).
// ---------------------------------------------------------------------------

/*
 * For encoding purposes, item pointers are represented as 64-bit unsigned
 * integers. The lowest 11 bits represent the offset number, and the next
 * lowest 32 bits are the block number. That leaves 21 bits unused, i.e.
 * only 43 low bits are used.
 */

/// How many bits do you need to encode offset number? OffsetNumber is a 16-bit
/// integer, but you can't fit that many items on a page. 11 ought to be more
/// than enough.
const MaxHeapTuplesPerPageBits: u32 = 11;

/// Max. number of bytes needed to encode the largest supported integer.
const MaxBytesPerInteger: usize = 7;

/// The Gin* item-pointer accessor macros (gin_private.h) are exact aliases of
/// the regular ItemPointer macros; ginCompareItemPointers == ItemPointerCompare.
#[inline]
unsafe fn ginCompareItemPointers(a: *mut ItemPointerData, b: *mut ItemPointerData) -> int32 {
    ItemPointerCompare(a, b)
}

#[inline]
unsafe fn itemptr_to_uint64(iptr: *const ItemPointerData) -> uint64 {
    // Assert(ItemPointerIsValid(iptr));
    // Assert(GinItemPointerGetOffsetNumber(iptr) < (1 << MaxHeapTuplesPerPageBits));
    let mut val: uint64 = ItemPointerGetBlockNumber(iptr) as uint64;
    val <<= MaxHeapTuplesPerPageBits;
    val |= ItemPointerGetOffsetNumber(iptr) as uint64;
    val
}

#[inline]
unsafe fn uint64_to_itemptr(mut val: uint64, iptr: *mut ItemPointerData) {
    ItemPointerSetOffsetNumber(
        iptr,
        (val & (((1u64) << MaxHeapTuplesPerPageBits) - 1)) as OffsetNumber,
    );
    val >>= MaxHeapTuplesPerPageBits;
    ItemPointerSetBlockNumber(iptr, val as BlockNumber);
    // Assert(ItemPointerIsValid(iptr));
}

/// Varbyte-encode 'val' into *ptr. *ptr is incremented to next integer.
#[inline]
unsafe fn encode_varbyte(mut val: uint64, ptr: *mut *mut u8) {
    let mut p = *ptr;

    while val > 0x7F {
        *p = (0x80 | (val & 0x7F)) as u8;
        p = p.add(1);
        val >>= 7;
    }
    *p = val as u8;
    p = p.add(1);

    *ptr = p;
}

/// Decode varbyte-encoded integer at *ptr. *ptr is incremented to next integer.
#[inline]
unsafe fn decode_varbyte(ptr: *mut *mut u8) -> uint64 {
    let mut val: uint64;
    let mut p = *ptr;
    let mut c: uint64;

    /* 1st byte */
    c = *p as uint64;
    p = p.add(1);
    val = c & 0x7F;
    if c & 0x80 != 0 {
        /* 2nd byte */
        c = *p as uint64;
        p = p.add(1);
        val |= (c & 0x7F) << 7;
        if c & 0x80 != 0 {
            /* 3rd byte */
            c = *p as uint64;
            p = p.add(1);
            val |= (c & 0x7F) << 14;
            if c & 0x80 != 0 {
                /* 4th byte */
                c = *p as uint64;
                p = p.add(1);
                val |= (c & 0x7F) << 21;
                if c & 0x80 != 0 {
                    /* 5th byte */
                    c = *p as uint64;
                    p = p.add(1);
                    val |= (c & 0x7F) << 28;
                    if c & 0x80 != 0 {
                        /* 6th byte */
                        c = *p as uint64;
                        p = p.add(1);
                        val |= (c & 0x7F) << 35;
                        if c & 0x80 != 0 {
                            /* 7th byte, should not have continuation bit */
                            c = *p as uint64;
                            p = p.add(1);
                            val |= c << 42;
                            // Assert((c & 0x80) == 0);
                        }
                    }
                }
            }
        }
    }

    *ptr = p;
    val
}

/// Encode a posting list.
///
/// The encoded list is returned in a palloc'd struct, which will be at most
/// 'maxsize' bytes in size. The number of items in the returned segment is
/// returned in *nwritten. If it's not equal to nipd, not all the items fit in
/// 'maxsize', and only the first *nwritten were encoded.
///
/// The allocated size of the returned struct is short-aligned, and the padding
/// byte at the end, if any, is zero.
///
/// # Safety
/// `ipd` points to `nipd` sorted, valid ItemPointerData. `nwritten` is null or
/// writable.
pub unsafe fn ginCompressPostingList(
    ipd: ItemPointer,
    nipd: c_int,
    mut maxsize: c_int,
    nwritten: *mut c_int,
) -> *mut GinPostingList {
    let mut prev: uint64;
    let mut totalpacked: c_int;
    let maxbytes: c_int;
    let result: *mut GinPostingList;
    let mut ptr: *mut u8;
    let endptr: *mut u8;

    maxsize = SHORTALIGN_DOWN(maxsize as usize) as c_int;

    result = palloc(maxsize as Size) as *mut GinPostingList;

    maxbytes = maxsize - core::mem::offset_of!(GinPostingList, bytes) as c_int;
    Assert!(maxbytes > 0);

    /* Store the first special item */
    (*result).first = *ipd;

    prev = itemptr_to_uint64(&(*result).first);

    ptr = (*result).bytes.as_mut_ptr();
    endptr = (*result).bytes.as_mut_ptr().add(maxbytes as usize);

    totalpacked = 1;
    while totalpacked < nipd {
        let val: uint64 = itemptr_to_uint64(ipd.add(totalpacked as usize));
        let delta: uint64 = val.wrapping_sub(prev);

        Assert!(val > prev);

        if (endptr as isize) - (ptr as isize) >= MaxBytesPerInteger as isize {
            encode_varbyte(delta, &mut ptr);
        } else {
            /*
             * There are less than 7 bytes left. Have to check if the next item
             * fits in that space before writing it out.
             */
            let mut buf: [u8; MaxBytesPerInteger] = [0; MaxBytesPerInteger];
            let mut p: *mut u8 = buf.as_mut_ptr();

            encode_varbyte(delta, &mut p);
            if (p as isize) - (buf.as_ptr() as isize) > (endptr as isize) - (ptr as isize) {
                break; /* output is full */
            }

            let n = (p as usize) - (buf.as_ptr() as usize);
            memcpy(ptr as *mut c_void, buf.as_ptr() as *const c_void, n);
            ptr = ptr.add(n);
        }
        prev = val;
        totalpacked += 1;
    }
    (*result).nbytes = ((ptr as usize) - ((*result).bytes.as_ptr() as usize)) as uint16;

    /*
     * If we wrote an odd number of bytes, zero out the padding byte at the end.
     */
    if (*result).nbytes as usize != shortalign((*result).nbytes as usize) {
        *(*result).bytes.as_mut_ptr().add((*result).nbytes as usize) = 0;
    }

    if !nwritten.is_null() {
        *nwritten = totalpacked;
    }

    Assert!(SizeOfGinPostingList(result) <= maxsize as Size);

    /*
     * Note: the C code optionally checks the encoding round-trips here under
     * CHECK_ENCODING_ROUNDTRIP (USE_ASSERT_CHECKING). Omitted in normal builds.
     */

    result
}

/// Decode a compressed posting list into an array of item pointers. The number
/// of items is returned in *ndecoded.
///
/// # Safety
/// `plist` references a valid GinPostingList. `ndecoded_out` is null or writable.
pub unsafe fn ginPostingListDecode(
    plist: *mut GinPostingList,
    ndecoded_out: *mut c_int,
) -> ItemPointer {
    ginPostingListDecodeAllSegments(plist, SizeOfGinPostingList(plist) as c_int, ndecoded_out)
}

/// Decode multiple posting list segments into an array of item pointers. The
/// number of items is returned in *ndecoded_out. The segments are stored one
/// after each other, with total size 'len' bytes.
///
/// # Safety
/// `segment` references `len` bytes of valid posting-list segments.
pub unsafe fn ginPostingListDecodeAllSegments(
    mut segment: *mut GinPostingList,
    len: c_int,
    ndecoded_out: *mut c_int,
) -> ItemPointer {
    let mut result: ItemPointer;
    let mut nallocated: c_int;
    let mut val: uint64;
    let endseg: *const u8 = (segment as *const u8).add(len as usize);
    let mut ndecoded: c_int;
    let mut ptr: *mut u8;
    let mut endptr: *mut u8;

    /*
     * Guess an initial size of the array.
     */
    nallocated = (*segment).nbytes as c_int * 2 + 1;
    result = palloc(nallocated as Size * core::mem::size_of::<ItemPointerData>()) as ItemPointer;

    ndecoded = 0;
    while (segment as *const u8) < endseg {
        /* enlarge output array if needed */
        if ndecoded >= nallocated {
            nallocated *= 2;
            result = repalloc(
                result as *mut c_void,
                nallocated as Size * core::mem::size_of::<ItemPointerData>(),
            ) as ItemPointer;
        }

        /* copy the first item */
        Assert!(OffsetNumberIsValid(ItemPointerGetOffsetNumber(&(*segment).first)));
        Assert!(
            ndecoded == 0
                || ginCompareItemPointers(&mut (*segment).first, result.add(ndecoded as usize - 1))
                    > 0
        );
        *result.add(ndecoded as usize) = (*segment).first;
        ndecoded += 1;

        val = itemptr_to_uint64(&(*segment).first);
        ptr = (*segment).bytes.as_mut_ptr();
        endptr = (*segment).bytes.as_mut_ptr().add((*segment).nbytes as usize);
        while ptr < endptr {
            /* enlarge output array if needed */
            if ndecoded >= nallocated {
                nallocated *= 2;
                result = repalloc(
                    result as *mut c_void,
                    nallocated as Size * core::mem::size_of::<ItemPointerData>(),
                ) as ItemPointer;
            }

            val = val.wrapping_add(decode_varbyte(&mut ptr));

            uint64_to_itemptr(val, result.add(ndecoded as usize));
            ndecoded += 1;
        }
        segment = GinNextPostingListSegment(segment);
    }

    if !ndecoded_out.is_null() {
        *ndecoded_out = ndecoded;
    }
    result
}

/// Add all item pointers from a bunch of posting lists to a TIDBitmap.
///
/// # Safety
/// `ptr` references `len` bytes of valid posting-list segments; `tbm` is valid.
pub unsafe fn ginPostingListDecodeAllSegmentsToTbm(
    ptr: *mut GinPostingList,
    len: c_int,
    tbm: *mut TIDBitmap,
) -> c_int {
    let mut ndecoded: c_int = 0;
    let items: ItemPointer;

    items = ginPostingListDecodeAllSegments(ptr, len, &mut ndecoded);
    tbm_add_tuples(tbm, items, ndecoded, false);
    pfree(items as *mut c_void);

    ndecoded
}

/// Merge two ordered arrays of itempointers, eliminating any duplicates.
///
/// Returns a palloc'd array, and *nmerged is set to the number of items in the
/// result, after eliminating duplicates.
///
/// # Safety
/// `a`/`b` point to `na`/`nb` sorted ItemPointerData; `nmerged` is writable.
pub unsafe fn ginMergeItemPointers(
    a: *mut ItemPointerData,
    na: uint32,
    b: *mut ItemPointerData,
    nb: uint32,
    nmerged: *mut c_int,
) -> ItemPointer {
    let dst: *mut ItemPointerData;

    dst = palloc((na + nb) as Size * core::mem::size_of::<ItemPointerData>()) as *mut ItemPointerData;

    /*
     * If the argument arrays don't overlap, we can just append them to each
     * other.
     */
    if na == 0
        || nb == 0
        || ginCompareItemPointers(a.add(na as usize - 1), b.add(0)) < 0
    {
        memcpy(
            dst as *mut c_void,
            a as *const c_void,
            na as usize * core::mem::size_of::<ItemPointerData>(),
        );
        memcpy(
            dst.add(na as usize) as *mut c_void,
            b as *const c_void,
            nb as usize * core::mem::size_of::<ItemPointerData>(),
        );
        *nmerged = (na + nb) as c_int;
    } else if ginCompareItemPointers(b.add(nb as usize - 1), a.add(0)) < 0 {
        memcpy(
            dst as *mut c_void,
            b as *const c_void,
            nb as usize * core::mem::size_of::<ItemPointerData>(),
        );
        memcpy(
            dst.add(nb as usize) as *mut c_void,
            a as *const c_void,
            na as usize * core::mem::size_of::<ItemPointerData>(),
        );
        *nmerged = (na + nb) as c_int;
    } else {
        let mut dptr: *mut ItemPointerData = dst;
        let mut aptr: *mut ItemPointerData = a;
        let mut bptr: *mut ItemPointerData = b;

        while ((aptr as usize) - (a as usize)) / core::mem::size_of::<ItemPointerData>() < na as usize
            && ((bptr as usize) - (b as usize)) / core::mem::size_of::<ItemPointerData>()
                < nb as usize
        {
            let cmp: c_int = ginCompareItemPointers(aptr, bptr);

            if cmp > 0 {
                *dptr = *bptr;
                dptr = dptr.add(1);
                bptr = bptr.add(1);
            } else if cmp == 0 {
                /* only keep one copy of the identical items */
                *dptr = *bptr;
                dptr = dptr.add(1);
                bptr = bptr.add(1);
                aptr = aptr.add(1);
            } else {
                *dptr = *aptr;
                dptr = dptr.add(1);
                aptr = aptr.add(1);
            }
        }

        while ((aptr as usize) - (a as usize)) / core::mem::size_of::<ItemPointerData>() < na as usize
        {
            *dptr = *aptr;
            dptr = dptr.add(1);
            aptr = aptr.add(1);
        }

        while ((bptr as usize) - (b as usize)) / core::mem::size_of::<ItemPointerData>() < nb as usize
        {
            *dptr = *bptr;
            dptr = dptr.add(1);
            bptr = bptr.add(1);
        }

        *nmerged = (((dptr as usize) - (dst as usize)) / core::mem::size_of::<ItemPointerData>())
            as c_int;
    }

    dst
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::itemptr::ItemPointerSet;

    unsafe fn mk(block: BlockNumber, off: OffsetNumber) -> ItemPointerData {
        let mut ip = core::mem::zeroed::<ItemPointerData>();
        ItemPointerSet(&mut ip, block, off);
        ip
    }

    // (1) itemptr_to_uint64 / uint64_to_itemptr round-trip a few (block,off) pairs.
    #[test]
    fn itemptr_uint64_roundtrip() {
        unsafe {
            let cases: [(BlockNumber, OffsetNumber); 5] = [
                (0, 1),
                (1, 7),
                (0x0001_2345, 291),
                (0xFFFF_FFFE, 1),
                (1234, 2047), // offset uses all 11 bits
            ];
            for (block, off) in cases {
                let ip = mk(block, off);
                let v = itemptr_to_uint64(&ip);
                let mut out = core::mem::zeroed::<ItemPointerData>();
                uint64_to_itemptr(v, &mut out);
                assert_eq!(ItemPointerGetBlockNumber(&out), block);
                assert_eq!(ItemPointerGetOffsetNumber(&out), off);
            }
        }
    }

    // (2) encode_varbyte then decode_varbyte round-trips values incl >127 and large.
    #[test]
    fn varbyte_roundtrip() {
        unsafe {
            let vals: [uint64; 9] = [
                0,
                1,
                0x7F,
                0x80,
                127,
                128,
                300,
                1_000_000,
                (1u64 << 42) | 0x3FF, // a large 43-bit value
            ];
            for v in vals {
                let mut buf = [0u8; MaxBytesPerInteger];
                let mut p: *mut u8 = buf.as_mut_ptr();
                encode_varbyte(v, &mut p);
                let written = (p as usize) - (buf.as_ptr() as usize);
                assert!(written >= 1 && written <= MaxBytesPerInteger);

                let mut q: *mut u8 = buf.as_mut_ptr();
                let decoded = decode_varbyte(&mut q);
                assert_eq!(decoded, v, "value {} did not round-trip", v);
                // decode consumed exactly the bytes encode produced
                assert_eq!((q as usize) - (buf.as_ptr() as usize), written);
            }
        }
    }

    // (3) ginCompressPostingList of a small sorted array, then ginPostingListDecode
    //     returns the same items in order.
    #[test]
    fn compress_decode_roundtrip() {
        unsafe {
            let mut ipd: [ItemPointerData; 6] = [
                mk(0, 1),
                mk(0, 5),
                mk(0, 291),
                mk(1, 1),
                mk(100, 42),
                mk(0x0001_0000, 7),
            ];
            let nipd = ipd.len() as c_int;

            let mut nwritten: c_int = 0;
            let plist = ginCompressPostingList(ipd.as_mut_ptr(), nipd, 4096, &mut nwritten);
            assert_eq!(nwritten, nipd, "all items should fit");

            let mut ndecoded: c_int = 0;
            let out = ginPostingListDecode(plist, &mut ndecoded);
            assert_eq!(ndecoded, nipd);
            for i in 0..nipd as usize {
                let got = &*out.add(i);
                assert_eq!(ItemPointerGetBlockNumber(got), ItemPointerGetBlockNumber(&ipd[i]));
                assert_eq!(
                    ItemPointerGetOffsetNumber(got),
                    ItemPointerGetOffsetNumber(&ipd[i])
                );
            }
            pfree(out as *mut c_void);
            pfree(plist as *mut c_void);
        }
    }

    // (4) ginMergeItemPointers of two sorted arrays yields the sorted union (dedup).
    #[test]
    fn merge_itempointers_union_dedup() {
        unsafe {
            let mut a: [ItemPointerData; 4] = [mk(0, 1), mk(0, 5), mk(1, 1), mk(2, 9)];
            let mut b: [ItemPointerData; 4] = [mk(0, 5), mk(1, 2), mk(2, 9), mk(3, 1)];
            // expected union: (0,1)(0,5)(1,1)(1,2)(2,9)(3,1) -> 6 items
            let expected: [(BlockNumber, OffsetNumber); 6] =
                [(0, 1), (0, 5), (1, 1), (1, 2), (2, 9), (3, 1)];

            let mut nmerged: c_int = 0;
            let dst = ginMergeItemPointers(
                a.as_mut_ptr(),
                a.len() as uint32,
                b.as_mut_ptr(),
                b.len() as uint32,
                &mut nmerged,
            );
            assert_eq!(nmerged as usize, expected.len());
            for (i, (block, off)) in expected.iter().enumerate() {
                let got = &*dst.add(i);
                assert_eq!(ItemPointerGetBlockNumber(got), *block);
                assert_eq!(ItemPointerGetOffsetNumber(got), *off);
            }
            pfree(dst as *mut c_void);
        }
    }
}

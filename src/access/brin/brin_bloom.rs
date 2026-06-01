/*
 * brin_bloom.c
 *		Implementation of Bloom opclass for BRIN
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * A BRIN opclass summarizing page range into a bloom filter.
 *
 * Bloom filters allow efficient testing whether a given page range contains
 * a particular value. Therefore, if we summarize each page range into a small
 * bloom filter, we can easily (and cheaply) test whether it contains values
 * we get later.
 *
 * The index only supports equality operators, similarly to hash indexes.
 * Bloom indexes are however much smaller, and support only bitmap scans.
 *
 * Note: Don't confuse this with bloom indexes, implemented in a contrib
 * module. That extension implements an entirely new AM, building a bloom
 * filter on multiple columns in a single row. This opclass works with an
 * existing AM (BRIN) and builds bloom filter on a column.
 *
 *
 * values vs. hashes
 * -----------------
 *
 * The original column values are not used directly, but are first hashed
 * using the regular type-specific hash function, producing a uint32 hash.
 * And this hash value is then added to the summary - i.e. it's hashed
 * again and added to the bloom filter.
 *
 * This allows the code to treat all data types (byval/byref/...) the same
 * way, with only minimal space requirements, because we're working with
 * hashes and not the original values. Everything is uint32.
 *
 * Of course, this assumes the built-in hash function is reasonably good,
 * without too many collisions etc. But that does seem to be the case, at
 * least based on past experience. After all, the same hash functions are
 * used for hash indexes, hash partitioning and so on.
 *
 *
 * hashing scheme
 * --------------
 *
 * Bloom filters require a number of independent hash functions. There are
 * different schemes how to construct them - for example we might use
 * hash_uint32_extended with random seeds, but that seems fairly expensive.
 * We use a scheme requiring only two functions described in this paper:
 *
 * Less Hashing, Same Performance:Building a Better Bloom Filter
 * Adam Kirsch, Michael Mitzenmacher, Harvard School of Engineering and
 * Applied Sciences, Cambridge, Massachusetts [DOI 10.1002/rsa.20208]
 *
 * The two hash functions h1 and h2 are calculated using hard-coded seeds,
 * and then combined using (h1 + i * h2) to generate the hash functions.
 *
 *
 * sizing the bloom filter
 * -----------------------
 *
 * Size of a bloom filter depends on the number of distinct values we will
 * store in it, and the desired false positive rate. The higher the number
 * of distinct values and/or the lower the false positive rate, the larger
 * the bloom filter. On the other hand, we want to keep the index as small
 * as possible - that's one of the basic advantages of BRIN indexes.
 *
 * Although the number of distinct elements (in a page range) depends on
 * the data, we can consider it fixed. This simplifies the trade-off to
 * just false positive rate vs. size.
 *
 * At the page range level, false positive rate is a probability the bloom
 * filter matches a random value. For the whole index (with sufficiently
 * many page ranges) it represents the fraction of the index ranges (and
 * thus fraction of the table to be scanned) matching the random value.
 *
 * Furthermore, the size of the bloom filter is subject to implementation
 * limits - it has to fit onto a single index page (8kB by default). As
 * the bitmap is inherently random (when "full" about half the bits is set
 * to 1, randomly), compression can't help very much.
 *
 * To reduce the size of a filter (to fit to a page), we have to either
 * accept higher false positive rate (undesirable), or reduce the number
 * of distinct items to be stored in the filter. We can't alter the input
 * data, of course, but we may make the BRIN page ranges smaller - instead
 * of the default 128 pages (1MB) we may build index with 16-page ranges,
 * or something like that. This should reduce the number of distinct values
 * in the page range, making the filter smaller (with fixed false positive
 * rate). Even for random data sets this should help, as the number of rows
 * per heap page is limited (to ~290 with very narrow tables, likely ~20
 * in practice).
 *
 * Of course, good sizing decisions depend on having the necessary data,
 * i.e. number of distinct values in a page range (of a given size) and
 * table size (to estimate cost change due to change in false positive
 * rate due to having larger index vs. scanning larger indexes). We may
 * not have that data - for example when building an index on empty table
 * it's not really possible. And for some data we only have estimates for
 * the whole table and we can only estimate per-range values (ndistinct).
 *
 * Another challenge is that while the bloom filter is per-column, it's
 * the whole index tuple that has to fit into a page. And for multi-column
 * indexes that may include pieces we have no control over (not necessarily
 * bloom filters, the other columns may use other BRIN opclasses). So it's
 * not entirely clear how to distribute the space between those columns.
 *
 * The current logic, implemented in brin_bloom_get_ndistinct, attempts to
 * make some basic sizing decisions, based on the size of BRIN ranges, and
 * the maximum number of rows per range.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/brin/brin_bloom.c
 */
use crate::prelude::*;
use crate::access::attnum::AttrNumber;
use crate::storage::block::BlockNumber;

use std::ffi::{c_int, c_char};

use crate::c::{int32, uint8, uint16, uint32, uint64, Size};
use crate::utils::fmgr::{FmgrInfo, FunctionCall1Coll, FunctionCallInfo};
use crate::lib::stringinfo::StringInfoData;
use crate::varatt::SET_VARSIZE;
use crate::{
    PG_DETOAST_DATUM, PG_GETARG_DATUM, PG_GETARG_INT32, PG_GETARG_POINTER, PG_GET_COLLATION,
    PG_GET_OPCLASS_OPTIONS, PG_RETURN_BOOL, PG_RETURN_CSTRING, PG_RETURN_POINTER, PG_RETURN_VOID,
};

const BloomEqualStrategyNumber: c_int = 1;

/*
 * Additional SQL level support functions. We only have one, which is
 * used to calculate hash of the input value.
 *
 * Procedure numbers must not use values reserved for BRIN itself; see
 * brin_internal.h.
 */
const BLOOM_MAX_PROCNUMS: usize = 1; /* maximum support procs we need */
const PROCNUM_HASH: uint16 = 11; /* required */

/*
 * Subtract this from procnum to obtain index in BloomOpaque arrays
 * (Must be equal to minimum of private procnums).
 */
const PROCNUM_BASE: uint16 = 11;

/*
 * Storage type for BRIN's reloptions.
 */
#[repr(C)]
pub struct BloomOptions {
    pub vl_len_: int32,             /* varlena header (do not touch directly!) */
    pub nDistinctPerRange: f64,     /* number of distinct values per range */
    pub falsePositiveRate: f64,     /* false positive for bloom filter */
}

/*
 * The current min value (16) is somewhat arbitrary, but it's based
 * on the fact that the filter header is ~20B alone, which is about
 * the same as the filter bitmap for 16 distinct items with 1% false
 * positive rate. So by allowing lower values we'd not gain much. In
 * any case, the min should not be larger than MaxHeapTuplesPerPage
 * (~290), which is the theoretical maximum for single-page ranges.
 */
const BLOOM_MIN_NDISTINCT_PER_RANGE: f64 = 16.0;

/*
 * Used to determine number of distinct items, based on the number of rows
 * in a page range. The 10% is somewhat similar to what estimate_num_groups
 * does, so we use the same factor here.
 */
const BLOOM_DEFAULT_NDISTINCT_PER_RANGE: f64 = -0.1; /* 10% of values */

/*
 * Allowed range and default value for the false positive range. The exact
 * values are somewhat arbitrary, but were chosen considering the various
 * parameters (size of filter vs. page size, etc.).
 *
 * The lower the false-positive rate, the more accurate the filter is, but
 * it also gets larger - at some point this eliminates the main advantage
 * of BRIN indexes, which is the tiny size. At 0.01% the index is about
 * 10% of the table (assuming 290 distinct values per 8kB page).
 *
 * On the other hand, as the false-positive rate increases, larger part of
 * the table has to be scanned due to mismatches - at 25% we're probably
 * close to sequential scan being cheaper.
 */
const BLOOM_MIN_FALSE_POSITIVE_RATE: f64 = 0.0001; /* 0.01% fp rate */
const BLOOM_MAX_FALSE_POSITIVE_RATE: f64 = 0.25; /* 25% fp rate */
const BLOOM_DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.01; /* 1% fp rate */

#[inline]
unsafe fn BloomGetNDistinctPerRange(opts: *const BloomOptions) -> f64 {
    if !opts.is_null() && (*opts).nDistinctPerRange != 0.0 {
        (*opts).nDistinctPerRange
    } else {
        BLOOM_DEFAULT_NDISTINCT_PER_RANGE
    }
}

#[inline]
unsafe fn BloomGetFalsePositiveRate(opts: *const BloomOptions) -> f64 {
    if !opts.is_null() && (*opts).falsePositiveRate != 0.0 {
        (*opts).falsePositiveRate
    } else {
        BLOOM_DEFAULT_FALSE_POSITIVE_RATE
    }
}

/*
 * And estimate of the largest bloom we can fit onto a page. This is not
 * a perfect guarantee, for a couple of reasons. For example, the row may
 * be larger because the index has multiple columns.
 */
#[inline]
fn BloomMaxFilterSize() -> Size {
    MAXALIGN_DOWN(
        BLCKSZ
            - (MAXALIGN(SizeOfPageHeaderData + std::mem::size_of::<ItemIdData>())
                + MAXALIGN(std::mem::size_of::<BrinSpecialSpace>())
                + unsafe { SizeOfBrinTuple() }),
    )
}

/*
 * Seeds used to calculate two hash functions h1 and h2, which are then used
 * to generate k hashes using the (h1 + i * h2) scheme.
 */
const BLOOM_SEED_1: uint64 = 0x71d924af;
const BLOOM_SEED_2: uint64 = 0xba48b314;

/*
 * Bloom Filter
 *
 * Represents a bloom filter, built on hashes of the indexed values. That is,
 * we compute a uint32 hash of the value, and then store this hash into the
 * bloom filter (and compute additional hashes on it).
 *
 * XXX We could implement "sparse" bloom filters, keeping only the bytes that
 * are not entirely 0. But while indexes don't support TOAST, the varlena can
 * still be compressed. So this seems unnecessary, because the compression
 * should do the same job.
 *
 * XXX We can also watch the number of bits set in the bloom filter, and then
 * stop using it (and not store the bitmap, to save space) when the false
 * positive rate gets too high. But even if the false positive rate exceeds the
 * desired value, it still can eliminate some page ranges.
 */
#[repr(C)]
pub struct BloomFilter {
    /* varlena header (do not touch directly!) */
    pub vl_len_: int32,

    /* space for various flags (unused for now) */
    pub flags: uint16,

    /* fields for the HASHED phase */
    pub nhashes: uint8, /* number of hash functions */
    pub nbits: uint32,  /* number of bits in the bitmap (size) */
    pub nbits_set: uint32, /* number of bits set to 1 */

    /* data of the bloom filter */
    pub data: [c_char; FLEXIBLE_ARRAY_MEMBER], /* char data[FLEXIBLE_ARRAY_MEMBER] */
}

/*
 * bloom_filter_size
 *		Calculate Bloom filter parameters (nbits, nbytes, nhashes).
 *
 * Given expected number of distinct values and desired false positive rate,
 * calculates the optimal parameters of the Bloom filter.
 *
 * The resulting parameters are returned through nbytesp (number of bytes),
 * nbitsp (number of bits) and nhashesp (number of hash functions). If a
 * pointer is NULL, the parameter is not returned.
 */
unsafe fn bloom_filter_size(
    ndistinct: c_int,
    false_positive_rate: f64,
    nbytesp: *mut c_int,
    nbitsp: *mut c_int,
    nhashesp: *mut c_int,
) {
    let k: f64;
    let mut nbits: c_int;
    let nbytes: c_int;

    /* sizing bloom filter: -(n * ln(p)) / (ln(2))^2 */
    nbits = f64::ceil(
        -((ndistinct as f64) * f64::ln(false_positive_rate)) / f64::powf(f64::ln(2.0), 2.0),
    ) as c_int;

    /* round m to whole bytes */
    nbytes = (nbits + 7) / 8;
    nbits = nbytes * 8;

    /*
     * round(log(2.0) * m / ndistinct), but assume round() may not be
     * available on Windows
     */
    let mut kk = f64::ln(2.0) * (nbits as f64) / (ndistinct as f64);
    kk = if (kk - f64::floor(kk)) >= 0.5 {
        f64::ceil(kk)
    } else {
        f64::floor(kk)
    };
    k = kk;

    if !nbytesp.is_null() {
        *nbytesp = nbytes;
    }

    if !nbitsp.is_null() {
        *nbitsp = nbits;
    }

    if !nhashesp.is_null() {
        *nhashesp = k as c_int;
    }
}

/*
 * bloom_init
 * 		Initialize the Bloom Filter, allocate all the memory.
 *
 * The filter is initialized with optimal size for ndistinct expected values
 * and the requested false positive rate. The filter is stored as varlena.
 */
unsafe fn bloom_init(ndistinct: c_int, false_positive_rate: f64) -> *mut BloomFilter {
    let len: Size;
    let filter: *mut BloomFilter;

    let mut nbits: c_int = 0; /* size of filter / number of bits */
    let mut nbytes: c_int = 0; /* size of filter / number of bytes */
    let mut nhashes: c_int = 0; /* number of hash functions */

    Assert!(ndistinct > 0);
    Assert!(false_positive_rate > 0.0 && false_positive_rate < 1.0);

    /* calculate bloom filter size / parameters */
    bloom_filter_size(
        ndistinct,
        false_positive_rate,
        &mut nbytes,
        &mut nbits,
        &mut nhashes,
    );

    /*
     * Reject filters that are obviously too large to store on a page.
     *
     * Initially the bloom filter is just zeroes and so very compressible, but
     * as we add values it gets more and more random, and so less and less
     * compressible. So initially everything fits on the page, but we might
     * get surprising failures later - we want to prevent that, so we reject
     * bloom filter that are obviously too large.
     *
     * XXX It's not uncommon to oversize the bloom filter a bit, to defend
     * against unexpected data anomalies (parts of table with more distinct
     * values per range etc.). But we still need to make sure even the
     * oversized filter fits on page, if such need arises.
     *
     * XXX This check is not perfect, because the index may have multiple
     * filters that are small individually, but too large when combined.
     */
    if (nbytes as Size) > BloomMaxFilterSize() {
        elog!(
            ERROR,
            "the bloom filter is too large ({} > {})",
            nbytes,
            BloomMaxFilterSize()
        );
    }

    /*
     * We allocate the whole filter. Most of it is going to be 0 bits, so the
     * varlena is easy to compress.
     */
    len = core::mem::offset_of!(BloomFilter, data) + nbytes as Size;

    filter = palloc0(len) as *mut BloomFilter;

    (*filter).flags = 0;
    (*filter).nhashes = nhashes as uint8;
    (*filter).nbits = nbits as uint32;

    SET_VARSIZE(filter as *mut _, len as c_int);

    filter
}

/*
 * bloom_add_value
 * 		Add value to the bloom filter.
 */
unsafe fn bloom_add_value(
    filter: *mut BloomFilter,
    value: uint32,
    updated: *mut bool,
) -> *mut BloomFilter {
    let h1: uint64;
    let h2: uint64;

    /* compute the hashes, used for the bloom filter */
    h1 = hash_bytes_uint32_extended(value, BLOOM_SEED_1) % (*filter).nbits as uint64;
    h2 = hash_bytes_uint32_extended(value, BLOOM_SEED_2) % (*filter).nbits as uint64;

    /* compute the requested number of hashes */
    let mut i: c_int = 0;
    while i < (*filter).nhashes as c_int {
        /* h1 + h2 + f(i) */
        let h: uint32 = ((h1 + (i as uint64) * h2) % (*filter).nbits as uint64) as uint32;
        let byte: uint32 = h / 8;
        let bit: uint32 = h % 8;

        /* if the bit is not set, set it and remember we did that */
        if ((*filter).data[byte as usize] as uint8 & (0x01u8 << bit)) == 0 {
            (*filter).data[byte as usize] =
                ((*filter).data[byte as usize] as uint8 | (0x01u8 << bit)) as c_char;
            (*filter).nbits_set += 1;
            if !updated.is_null() {
                *updated = true;
            }
        }

        i += 1;
    }

    filter
}

/*
 * bloom_contains_value
 * 		Check if the bloom filter contains a particular value.
 */
unsafe fn bloom_contains_value(filter: *mut BloomFilter, value: uint32) -> bool {
    let h1: uint64;
    let h2: uint64;

    /* calculate the two hashes */
    h1 = hash_bytes_uint32_extended(value, BLOOM_SEED_1) % (*filter).nbits as uint64;
    h2 = hash_bytes_uint32_extended(value, BLOOM_SEED_2) % (*filter).nbits as uint64;

    /* compute the requested number of hashes */
    let mut i: c_int = 0;
    while i < (*filter).nhashes as c_int {
        /* h1 + h2 + f(i) */
        let h: uint32 = ((h1 + (i as uint64) * h2) % (*filter).nbits as uint64) as uint32;
        let byte: uint32 = h / 8;
        let bit: uint32 = h % 8;

        /* if the bit is not set, the value is not there */
        if ((*filter).data[byte as usize] as uint8 & (0x01u8 << bit)) == 0 {
            return false;
        }

        i += 1;
    }

    /* all hashes found in bloom filter */
    true
}

#[repr(C)]
pub struct BloomOpaque {
    /*
     * XXX At this point we only need a single proc (to compute the hash), but
     * let's keep the array just like inclusion and minmax opclasses, for
     * consistency. We may need additional procs in the future.
     */
    pub extra_procinfos: [FmgrInfo; BLOOM_MAX_PROCNUMS],
}

#[no_mangle]
pub unsafe extern "C" fn brin_bloom_opcinfo(fcinfo: FunctionCallInfo) -> Datum {
    let result: *mut BrinOpcInfo;

    /*
     * opaque->strategy_procinfos is initialized lazily; here it is set to
     * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
     *
     * bloom indexes only store the filter as a single BYTEA column
     */

    result = palloc0(MAXALIGN(SizeofBrinOpcInfo(1)) + std::mem::size_of::<BloomOpaque>())
        as *mut BrinOpcInfo;
    (*result).oi_nstored = 1;
    (*result).oi_regular_nulls = true;
    (*result).oi_opaque =
        MAXALIGN((result as *mut c_char).add(SizeofBrinOpcInfo(1)) as usize) as *mut BloomOpaque
            as *mut _;
    (*result).oi_typcache[0] = lookup_type_cache(PG_BRIN_BLOOM_SUMMARYOID, 0);

    PG_RETURN_POINTER!(result as *mut _)
}

/*
 * brin_bloom_get_ndistinct
 *		Determine the ndistinct value used to size bloom filter.
 *
 * Adjust the ndistinct value based on the pagesPerRange value. First,
 * if it's negative, it's assumed to be relative to maximum number of
 * tuples in the range (assuming each page gets MaxHeapTuplesPerPage
 * tuples, which is likely a significant over-estimate). We also clamp
 * the value, not to over-size the bloom filter unnecessarily.
 *
 * XXX We can only do this when the pagesPerRange value was supplied.
 * If it wasn't, it has to be a read-only access to the index, in which
 * case we don't really care. But perhaps we should fall-back to the
 * default pagesPerRange value?
 *
 * XXX We might also fetch info about ndistinct estimate for the column,
 * and compute the expected number of distinct values in a range. But
 * that may be tricky due to data being sorted in various ways, so it
 * seems better to rely on the upper estimate.
 *
 * XXX We might also calculate a better estimate of rows per BRIN range,
 * instead of using MaxHeapTuplesPerPage (which probably produces values
 * much higher than reality).
 */
unsafe fn brin_bloom_get_ndistinct(bdesc: *mut BrinDesc, opts: *mut BloomOptions) -> c_int {
    let mut ndistinct: f64;
    let maxtuples: f64;
    let pagesPerRange: BlockNumber;

    pagesPerRange = BrinGetPagesPerRange((*bdesc).bd_index);
    ndistinct = BloomGetNDistinctPerRange(opts);

    Assert!(BlockNumberIsValid(pagesPerRange));

    maxtuples = (MaxHeapTuplesPerPage * pagesPerRange as usize) as f64;

    /*
     * Similarly to n_distinct, negative values are relative - in this case to
     * maximum number of tuples in the page range (maxtuples).
     */
    if ndistinct < 0.0 {
        ndistinct = (-ndistinct) * maxtuples;
    }

    /*
     * Positive values are to be used directly, but we still apply a couple of
     * safeties to avoid using unreasonably small bloom filters.
     */
    ndistinct = Max(ndistinct, BLOOM_MIN_NDISTINCT_PER_RANGE);

    /*
     * And don't use more than the maximum possible number of tuples, in the
     * range, which would be entirely wasteful.
     */
    ndistinct = Min(ndistinct, maxtuples);

    ndistinct as c_int
}

/*
 * Examine the given index tuple (which contains partial status of a certain
 * page range) by comparing it to the given value that comes from another heap
 * tuple.  If the new value is outside the bloom filter specified by the
 * existing tuple values, update the index tuple and return true.  Otherwise,
 * return false and do not modify in this case.
 */
#[no_mangle]
pub unsafe extern "C" fn brin_bloom_add_value(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc: *mut BrinDesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let column: *mut BrinValues = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let newval: Datum = PG_GETARG_DATUM!(fcinfo, 2);
    let isnull: bool = PG_GETARG_DATUM!(fcinfo, 3) != 0; /* PG_USED_FOR_ASSERTS_ONLY */
    let opts: *mut BloomOptions = PG_GET_OPCLASS_OPTIONS!(fcinfo) as *mut BloomOptions;
    let colloid: Oid = PG_GET_COLLATION!(fcinfo);
    let hashFn: *mut FmgrInfo;
    let mut hashValue: uint32 = 0;
    let mut updated: bool = false;
    let mut attno: AttrNumber = 0;
    let mut filter: *mut BloomFilter;

    Assert!(!isnull);

    attno = (*column).bv_attno;

    /*
     * If this is the first non-null value, we need to initialize the bloom
     * filter. Otherwise just extract the existing bloom filter from
     * BrinValues.
     */
    if (*column).bv_allnulls {
        filter = bloom_init(
            brin_bloom_get_ndistinct(bdesc, opts),
            BloomGetFalsePositiveRate(opts),
        );
        *(*column).bv_values.add(0) = PointerGetDatum(filter as *mut _);
        (*column).bv_allnulls = false;
        updated = true;
    } else {
        filter = PG_DETOAST_DATUM!(*(*column).bv_values.add(0)) as *mut BloomFilter;
    }

    /*
     * Compute the hash of the new value, using the supplied hash function,
     * and then add the hash value to the bloom filter.
     */
    hashFn = bloom_get_procinfo(bdesc, attno as uint16, PROCNUM_HASH);

    hashValue = DatumGetUInt32(FunctionCall1Coll(hashFn, colloid, newval));

    filter = bloom_add_value(filter, hashValue, &mut updated);

    *(*column).bv_values.add(0) = PointerGetDatum(filter as *mut _);

    PG_RETURN_BOOL!(updated)
}

/*
 * Given an index tuple corresponding to a certain page range and a scan key,
 * return whether the scan key is consistent with the index tuple's bloom
 * filter.  Return true if so, false otherwise.
 */
#[no_mangle]
pub unsafe extern "C" fn brin_bloom_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc: *mut BrinDesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let column: *mut BrinValues = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let keys: *mut ScanKey = PG_GETARG_POINTER!(fcinfo, 2) as *mut ScanKey;
    let nkeys: c_int = PG_GETARG_INT32!(fcinfo, 3);
    let colloid: Oid = PG_GET_COLLATION!(fcinfo);
    let attno: AttrNumber = 0;
    let value: Datum = 0;
    let mut matches: bool;
    let finfo: *mut FmgrInfo = std::ptr::null_mut();
    let hashValue: uint32 = 0;
    let filter: *mut BloomFilter;

    filter = PG_DETOAST_DATUM!(*(*column).bv_values.add(0)) as *mut BloomFilter;

    Assert!(!filter.is_null());

    /*
     * Assume all scan keys match. We'll be searching for a scan key
     * eliminating the page range (we can stop on the first such key).
     */
    matches = true;

    let mut keyno: c_int = 0;
    while keyno < nkeys {
        let key: ScanKey = *keys.add(keyno as usize);

        /* NULL keys are handled and filtered-out in bringetbitmap */
        Assert!(((*key).sk_flags & SK_ISNULL) == 0);

        let attno_local: AttrNumber = (*key).sk_attno;
        let value_local: Datum = (*key).sk_argument;
        let _ = attno_local;
        let _ = value_local;

        match (*key).sk_strategy as c_int {
            BloomEqualStrategyNumber => {
                /*
                 * We want to return the current page range if the bloom
                 * filter seems to contain the value.
                 */
                let attno = attno_local;
                let value = value_local;
                let finfo = bloom_get_procinfo(bdesc, attno as uint16, PROCNUM_HASH);

                let hashValue =
                    DatumGetUInt32(FunctionCall1Coll(finfo, colloid, value));
                matches &= bloom_contains_value(filter, hashValue);
                let _ = (finfo, hashValue);
            }
            _ => {
                /* shouldn't happen */
                elog!(ERROR, "invalid strategy number {}", (*key).sk_strategy);
                matches = false;
            }
        }

        let _ = (attno, value, finfo, hashValue);

        if !matches {
            break;
        }

        keyno += 1;
    }

    PG_RETURN_BOOL!(matches)
}

/*
 * Given two BrinValues, update the first of them as a union of the summary
 * values contained in both.  The second one is untouched.
 *
 * XXX We assume the bloom filters have the same parameters for now. In the
 * future we should have 'can union' function, to decide if we can combine
 * two particular bloom filters.
 */
#[no_mangle]
pub unsafe extern "C" fn brin_bloom_union(fcinfo: FunctionCallInfo) -> Datum {
    let nbytes: c_int;
    let col_a: *mut BrinValues = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let col_b: *mut BrinValues = PG_GETARG_POINTER!(fcinfo, 2) as *mut BrinValues;
    let filter_a: *mut BloomFilter;
    let filter_b: *mut BloomFilter;

    Assert!((*col_a).bv_attno == (*col_b).bv_attno);
    Assert!(!(*col_a).bv_allnulls && !(*col_b).bv_allnulls);

    filter_a = PG_DETOAST_DATUM!(*(*col_a).bv_values.add(0)) as *mut BloomFilter;
    filter_b = PG_DETOAST_DATUM!(*(*col_b).bv_values.add(0)) as *mut BloomFilter;

    /* make sure the filters use the same parameters */
    Assert!(!filter_a.is_null() && !filter_b.is_null());
    Assert!((*filter_a).nbits == (*filter_b).nbits);
    Assert!((*filter_a).nhashes == (*filter_b).nhashes);
    Assert!((*filter_a).nbits > 0 && (*filter_a).nbits % 8 == 0);

    nbytes = ((*filter_a).nbits / 8) as c_int;

    /* simply OR the bitmaps */
    let mut i: c_int = 0;
    while i < nbytes {
        (*filter_a).data[i as usize] = ((*filter_a).data[i as usize] as uint8
            | (*filter_b).data[i as usize] as uint8) as c_char;
        i += 1;
    }

    /* update the number of bits set in the filter */
    (*filter_a).nbits_set =
        pg_popcount((*filter_a).data.as_ptr() as *const c_char, nbytes as Size) as uint32;

    /* if we decompressed filter_a, update the summary */
    if PointerGetDatum(filter_a as *mut _) != *(*col_a).bv_values.add(0) {
        pfree(DatumGetPointer(*(*col_a).bv_values.add(0)) as *mut _);
        *(*col_a).bv_values.add(0) = PointerGetDatum(filter_a as *mut _);
    }

    /* also free filter_b, if it was decompressed */
    if PointerGetDatum(filter_b as *mut _) != *(*col_b).bv_values.add(0) {
        pfree(filter_b as *mut _);
    }

    PG_RETURN_VOID!()
}

/*
 * Cache and return inclusion opclass support procedure
 *
 * Return the procedure corresponding to the given function support number
 * or null if it does not exist.
 */
unsafe fn bloom_get_procinfo(
    bdesc: *mut BrinDesc,
    attno: uint16,
    procnum: uint16,
) -> *mut FmgrInfo {
    let opaque: *mut BloomOpaque;
    let basenum: uint16 = procnum - PROCNUM_BASE;

    /*
     * We cache these in the opaque struct, to avoid repetitive syscache
     * lookups.
     */
    opaque = (*(*(*bdesc).bd_info.add((attno - 1) as usize))).oi_opaque as *mut BloomOpaque;

    if (*opaque).extra_procinfos[basenum as usize].fn_oid == InvalidOid {
        if RegProcedureIsValid(index_getprocid((*bdesc).bd_index, attno as AttrNumber, procnum)) {
            fmgr_info_copy(
                &mut (*opaque).extra_procinfos[basenum as usize],
                index_getprocinfo((*bdesc).bd_index, attno as AttrNumber, procnum),
                (*bdesc).bd_context,
            );
        } else {
            ereport!(ERROR, "invalid opclass definition");
        }
    }

    &mut (*opaque).extra_procinfos[basenum as usize]
}

#[no_mangle]
pub unsafe extern "C" fn brin_bloom_options(fcinfo: FunctionCallInfo) -> Datum {
    let relopts: *mut local_relopts = PG_GETARG_POINTER!(fcinfo, 0) as *mut local_relopts;

    init_local_reloptions(relopts, std::mem::size_of::<BloomOptions>());

    add_local_real_reloption(
        relopts,
        c"n_distinct_per_range".as_ptr(),
        c"number of distinct items expected in a BRIN page range".as_ptr(),
        BLOOM_DEFAULT_NDISTINCT_PER_RANGE,
        -1.0,
        INT_MAX as f64,
        core::mem::offset_of!(BloomOptions, nDistinctPerRange) as c_int,
    );

    add_local_real_reloption(
        relopts,
        c"false_positive_rate".as_ptr(),
        c"desired false-positive rate for the bloom filters".as_ptr(),
        BLOOM_DEFAULT_FALSE_POSITIVE_RATE,
        BLOOM_MIN_FALSE_POSITIVE_RATE,
        BLOOM_MAX_FALSE_POSITIVE_RATE,
        core::mem::offset_of!(BloomOptions, falsePositiveRate) as c_int,
    );

    PG_RETURN_VOID!()
}

/*
 * brin_bloom_summary_in
 *		- input routine for type brin_bloom_summary.
 *
 * brin_bloom_summary is only used internally to represent summaries
 * in BRIN bloom indexes, so it has no operations of its own, and we
 * disallow input too.
 */
#[no_mangle]
pub unsafe extern "C" fn brin_bloom_summary_in(fcinfo: FunctionCallInfo) -> Datum {
    /*
     * brin_bloom_summary stores the data in binary form and parsing text
     * input is not needed, so disallow this.
     */
    ereport!(
        ERROR,
        "cannot accept a value of type pg_brin_bloom_summary"
    );

    PG_RETURN_VOID!() /* keep compiler quiet */
}

/*
 * brin_bloom_summary_out
 *		- output routine for type brin_bloom_summary.
 *
 * BRIN bloom summaries are serialized into a bytea value, but we want
 * to output something nicer humans can understand.
 */
#[no_mangle]
pub unsafe extern "C" fn brin_bloom_summary_out(fcinfo: FunctionCallInfo) -> Datum {
    let filter: *mut BloomFilter;
    let mut str: StringInfoData = std::mem::zeroed();

    /* detoast the data to get value with a full 4B header */
    filter = PG_DETOAST_DATUM!(PG_GETARG_DATUM!(fcinfo, 0)) as *mut BloomFilter;

    initStringInfo(&mut str);
    appendStringInfoChar(&mut str, b'{' as c_char);

    appendStringInfo(
        &mut str,
        c"mode: hashed  nhashes: %u  nbits: %u  nbits_set: %u".as_ptr(),
        (*filter).nhashes as uint32,
        (*filter).nbits,
        (*filter).nbits_set,
    );

    appendStringInfoChar(&mut str, b'}' as c_char);

    PG_RETURN_CSTRING!(str.data)
}

/*
 * brin_bloom_summary_recv
 *		- binary input routine for type brin_bloom_summary.
 */
#[no_mangle]
pub unsafe extern "C" fn brin_bloom_summary_recv(fcinfo: FunctionCallInfo) -> Datum {
    ereport!(
        ERROR,
        "cannot accept a value of type pg_brin_bloom_summary"
    );

    PG_RETURN_VOID!() /* keep compiler quiet */
}

/*
 * brin_bloom_summary_send
 *		- binary output routine for type brin_bloom_summary.
 *
 * BRIN bloom summaries are serialized in a bytea value (although the
 * type is named differently), so let's just send that.
 */
#[no_mangle]
pub unsafe extern "C" fn brin_bloom_summary_send(fcinfo: FunctionCallInfo) -> Datum {
    byteasend(fcinfo)
}

/* ---- local stubs for unported dependencies ---- */

const SizeOfPageHeaderData: Size = 24; // TODO: storage/bufpage.h
const BLCKSZ: Size = 8192; // TODO: pg_config.h
const INT_MAX: c_int = 2147483647; // TODO: limits.h
const SK_ISNULL: c_int = 0x0001; // TODO: access/skey.h
const PG_BRIN_BLOOM_SUMMARYOID: Oid = 4601; // TODO: catalog/pg_type.h
static mut MaxHeapTuplesPerPage: usize = 0; // TODO: access/htup_details.h

#[repr(C)]
pub struct ItemIdData {
    _opaque: [u8; 4],
} // TODO: storage/itemid.h
#[repr(C)]
pub struct BrinSpecialSpace {
    _opaque: [u8; 4],
} // TODO: access/brin_page.h
#[repr(C)]
pub struct BrinDesc {
    pub bd_context: MemoryContext,
    pub bd_index: Relation,
    pub bd_info: *mut *mut BrinOpcInfo,
} // TODO: access/brin_internal.h
#[repr(C)]
pub struct BrinOpcInfo {
    pub oi_nstored: uint16,
    pub oi_regular_nulls: bool,
    pub oi_opaque: *mut std::ffi::c_void,
    pub oi_typcache: [*mut TypeCacheEntry; 1],
} // TODO: access/brin_internal.h
#[repr(C)]
pub struct BrinValues {
    pub bv_attno: AttrNumber,
    pub bv_allnulls: bool,
    pub bv_hasnulls: bool,
    pub bv_values: *mut Datum,
} // TODO: access/brin_tuple.h
#[repr(C)]
pub struct ScanKeyData {
    pub sk_flags: c_int,
    pub sk_attno: AttrNumber,
    pub sk_strategy: uint16,
    pub sk_argument: Datum,
} // TODO: access/skey.h
pub type ScanKey = *mut ScanKeyData;
#[repr(C)]
pub struct TypeCacheEntry {
    _opaque: [u8; 0],
} // TODO: utils/typcache.h
#[repr(C)]
pub struct local_relopts {
    _opaque: [u8; 0],
} // TODO: access/reloptions.h
pub type Relation = *mut std::ffi::c_void; // TODO: utils/rel.h
pub type RegProcedure = Oid; // TODO: postgres_ext.h

#[inline]
unsafe fn SizeOfBrinTuple() -> Size {
    unimplemented!() // TODO: access/brin_tuple.h
}
#[inline]
unsafe fn SizeofBrinOpcInfo(_ncols: c_int) -> usize {
    unimplemented!() // TODO: access/brin_internal.h
}
#[inline]
unsafe fn BrinGetPagesPerRange(_index: Relation) -> BlockNumber {
    unimplemented!() // TODO: access/brin.h
}
#[inline]
fn BlockNumberIsValid(_blockNumber: BlockNumber) -> bool {
    unimplemented!() // TODO: storage/block.h
}
#[inline]
fn RegProcedureIsValid(_p: RegProcedure) -> bool {
    unimplemented!() // TODO: c.h
}
unsafe fn hash_bytes_uint32_extended(_k: uint32, _seed: uint64) -> uint64 {
    unimplemented!() // TODO: common/hashfn.h
}
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!() // TODO: utils/typcache.h
}
unsafe fn index_getprocid(_irel: Relation, _attnum: AttrNumber, _procnum: uint16) -> RegProcedure {
    unimplemented!() // TODO: access/genam.h
}
unsafe fn index_getprocinfo(
    _irel: Relation,
    _attnum: AttrNumber,
    _procnum: uint16,
) -> *mut FmgrInfo {
    unimplemented!() // TODO: access/genam.h
}
unsafe fn fmgr_info_copy(_dstinfo: *mut FmgrInfo, _srcinfo: *mut FmgrInfo, _destcxt: MemoryContext) {
    unimplemented!() // TODO: utils/fmgr.h
}
unsafe fn init_local_reloptions(_relopts: *mut local_relopts, _relopt_struct_size: Size) {
    unimplemented!() // TODO: access/reloptions.h
}
unsafe fn add_local_real_reloption(
    _relopts: *mut local_relopts,
    _name: *const c_char,
    _desc: *const c_char,
    _default_val: f64,
    _min_val: f64,
    _max_val: f64,
    _offset: c_int,
) {
    unimplemented!() // TODO: access/reloptions.h
}
unsafe fn pg_popcount(_buf: *const c_char, _bytes: Size) -> uint64 {
    unimplemented!() // TODO: port/pg_bitutils.h
}
unsafe fn byteasend(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: utils/varlena.h
}
unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!() // TODO: lib/stringinfo.h
}
unsafe fn appendStringInfoChar(_str: *mut StringInfoData, _ch: c_char) {
    unimplemented!() // TODO: lib/stringinfo.h
}
unsafe fn appendStringInfo(_str: *mut StringInfoData, _fmt: *const c_char, _a: uint32, _b: uint32, _c: uint32) {
    unimplemented!() // TODO: lib/stringinfo.h
}

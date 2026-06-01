//! src/backend/statistics/mvdistinct.c
//!
//! POSTGRES multivariate ndistinct coefficients
//!
//! Estimating number of groups in a combination of columns (e.g. for GROUP BY)
//! is tricky, and the estimation error is often significant.
//!
//! The multivariate ndistinct coefficients address this by storing ndistinct
//! estimates for combinations of the user-specified columns.  So for example
//! given a statistics object on three columns (a,b,c), this module estimates
//! and stores n-distinct for (a,b), (a,c), (b,c) and (a,b,c).  The per-column
//! estimates are already available in pg_statistic.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/statistics/mvdistinct.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::uint32;
use crate::access::attnum::AttrNumber;
use crate::postgres_ext::Oid;

/* size of the struct header fields (magic, type, nitems) */
const SizeOfHeader: Size = 3 * std::mem::size_of::<uint32>();

/* size of a serialized ndistinct item (coefficient, natts, atts) */
#[inline]
const fn SizeOfItem(natts: usize) -> Size {
    std::mem::size_of::<f64>()
        + std::mem::size_of::<c_int>()
        + natts * std::mem::size_of::<AttrNumber>()
}

/* minimal size of a ndistinct item (with two attributes) */
const MinSizeOfItem: Size = SizeOfItem(2);

/* minimal size of mvndistinct, when all items are minimal */
#[inline]
const fn MinSizeOfItems(nitems: usize) -> Size {
    SizeOfHeader + nitems * MinSizeOfItem
}

/* Combination generator API */

/* internal state for generator of k-combinations of n elements */
#[repr(C)]
struct CombinationGenerator {
    k: c_int,             /* size of the combination */
    n: c_int,             /* total number of elements */
    current: c_int,       /* index of the next combination to return */
    ncombinations: c_int, /* number of combinations (size of array) */
    combinations: *mut c_int, /* array of pre-built combinations */
}

/*
 * statext_ndistinct_build
 *		Compute ndistinct coefficient for the combination of attributes.
 *
 * This computes the ndistinct estimate using the same estimator used
 * in analyze.c and then computes the coefficient.
 *
 * To handle expressions easily, we treat them as system attributes with
 * negative attnums, and offset everything by number of expressions to
 * allow using Bitmapsets.
 */
pub unsafe fn statext_ndistinct_build(
    totalrows: f64,
    data: *mut StatsBuildData,
) -> *mut MVNDistinct {
    let result: *mut MVNDistinct;
    let k: c_int;
    let mut itemcnt: c_int;
    let numattrs: c_int = (*data).nattnums;
    let numcombs: c_int = num_combinations(numattrs);

    result = palloc(
        core::mem::offset_of!(MVNDistinct, items)
            + (numcombs as usize) * std::mem::size_of::<MVNDistinctItem>(),
    ) as *mut MVNDistinct;
    (*result).magic = STATS_NDISTINCT_MAGIC;
    (*result).type_ = STATS_NDISTINCT_TYPE_BASIC;
    (*result).nitems = numcombs as uint32;

    itemcnt = 0;
    let mut k = 2;
    while k <= numattrs {
        let mut combination: *mut c_int;
        let generator: *mut CombinationGenerator;

        /* generate combinations of K out of N elements */
        generator = generator_init(numattrs, k);

        loop {
            combination = generator_next(generator);
            if combination.is_null() {
                break;
            }

            let item: *mut MVNDistinctItem = &mut (*result).items[itemcnt as usize];
            let mut j: c_int;

            (*item).attributes =
                palloc(std::mem::size_of::<AttrNumber>() * k as usize) as *mut AttrNumber;
            (*item).nattributes = k;

            /* translate the indexes to attnums */
            j = 0;
            while j < k {
                *(*item).attributes.offset(j as isize) =
                    *(*data).attnums.offset(*combination.offset(j as isize) as isize);

                debug_assert!(AttributeNumberIsValid(*(*item).attributes.offset(j as isize)));
                j += 1;
            }

            (*item).ndistinct =
                ndistinct_for_combination(totalrows, data, k, combination);

            itemcnt += 1;
            debug_assert!(itemcnt <= (*result).nitems as c_int);
        }

        generator_free(generator);
        k += 1;
    }
    let _ = k;

    /* must consume exactly the whole output array */
    debug_assert!(itemcnt == (*result).nitems as c_int);

    result
}

/*
 * statext_ndistinct_load
 *		Load the ndistinct value for the indicated pg_statistic_ext tuple
 */
pub unsafe fn statext_ndistinct_load(mvoid: Oid, inh: bool) -> *mut MVNDistinct {
    let result: *mut MVNDistinct;
    let mut isnull: bool = false;
    let ndist: Datum;
    let htup: HeapTuple;

    htup = SearchSysCache2(
        STATEXTDATASTXOID,
        ObjectIdGetDatum(mvoid),
        BoolGetDatum(inh),
    );
    if !HeapTupleIsValid(htup) {
        elog!(ERROR, "cache lookup failed for statistics object {}", mvoid);
        unreachable!();
    }

    ndist = SysCacheGetAttr(
        STATEXTDATASTXOID,
        htup,
        Anum_pg_statistic_ext_data_stxdndistinct,
        &mut isnull,
    );
    if isnull {
        elog!(
            ERROR,
            "requested statistics kind \"{}\" is not yet built for statistics object {}",
            STATS_EXT_NDISTINCT as u8 as char,
            mvoid
        );
        unreachable!();
    }

    result = statext_ndistinct_deserialize(DatumGetByteaPP(ndist));

    ReleaseSysCache(htup);

    result
}

/*
 * statext_ndistinct_serialize
 *		serialize ndistinct to the on-disk bytea format
 */
pub unsafe fn statext_ndistinct_serialize(ndistinct: *mut MVNDistinct) -> *mut bytea {
    let mut i: c_int;
    let output: *mut bytea;
    let mut tmp: *mut c_char;
    let mut len: Size;

    debug_assert!((*ndistinct).magic == STATS_NDISTINCT_MAGIC);
    debug_assert!((*ndistinct).type_ == STATS_NDISTINCT_TYPE_BASIC);

    /*
     * Base size is size of scalar fields in the struct, plus one base struct
     * for each item, including number of items for each.
     */
    len = VARHDRSZ + SizeOfHeader;

    /* and also include space for the actual attribute numbers */
    i = 0;
    while i < (*ndistinct).nitems as c_int {
        let nmembers: c_int;

        nmembers = (*ndistinct).items[i as usize].nattributes;
        debug_assert!(nmembers >= 2);

        len += SizeOfItem(nmembers as usize);
        i += 1;
    }

    output = palloc(len) as *mut bytea;
    SET_VARSIZE(output, len as c_int);

    tmp = VARDATA(output as *mut c_void) as *mut c_char;

    /* Store the base struct values (magic, type, nitems) */
    libc_memcpy(
        tmp as *mut c_void,
        &(*ndistinct).magic as *const uint32 as *const c_void,
        std::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(std::mem::size_of::<uint32>());
    libc_memcpy(
        tmp as *mut c_void,
        &(*ndistinct).type_ as *const uint32 as *const c_void,
        std::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(std::mem::size_of::<uint32>());
    libc_memcpy(
        tmp as *mut c_void,
        &(*ndistinct).nitems as *const uint32 as *const c_void,
        std::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(std::mem::size_of::<uint32>());

    /*
     * store number of attributes and attribute numbers for each entry
     */
    i = 0;
    while i < (*ndistinct).nitems as c_int {
        let item: MVNDistinctItem = (*ndistinct).items[i as usize];
        let nmembers: c_int = item.nattributes;

        libc_memcpy(
            tmp as *mut c_void,
            &item.ndistinct as *const f64 as *const c_void,
            std::mem::size_of::<f64>(),
        );
        tmp = tmp.add(std::mem::size_of::<f64>());
        libc_memcpy(
            tmp as *mut c_void,
            &nmembers as *const c_int as *const c_void,
            std::mem::size_of::<c_int>(),
        );
        tmp = tmp.add(std::mem::size_of::<c_int>());

        libc_memcpy(
            tmp as *mut c_void,
            item.attributes as *const c_void,
            std::mem::size_of::<AttrNumber>() * nmembers as usize,
        );
        tmp = tmp.add(nmembers as usize * std::mem::size_of::<AttrNumber>());

        /* protect against overflows */
        debug_assert!(tmp <= (output as *mut c_char).add(len));
        i += 1;
    }

    /* check we used exactly the expected space */
    debug_assert!(tmp == (output as *mut c_char).add(len));

    output
}

/*
 * statext_ndistinct_deserialize
 *		Read an on-disk bytea format MVNDistinct to in-memory format
 */
pub unsafe fn statext_ndistinct_deserialize(data: *mut bytea) -> *mut MVNDistinct {
    let mut i: c_int;
    let minimum_size: Size;
    let mut ndist: MVNDistinct = std::mem::zeroed();
    let ndistinct: *mut MVNDistinct;
    let mut tmp: *mut c_char;

    if data.is_null() {
        return std::ptr::null_mut();
    }

    /* we expect at least the basic fields of MVNDistinct struct */
    if (VARSIZE_ANY_EXHDR(data) as Size) < SizeOfHeader {
        elog!(
            ERROR,
            "invalid MVNDistinct size {} (expected at least {})",
            VARSIZE_ANY_EXHDR(data),
            SizeOfHeader
        );
        unreachable!();
    }

    /* initialize pointer to the data part (skip the varlena header) */
    tmp = VARDATA_ANY(data as *mut c_void) as *mut c_char;

    /* read the header fields and perform basic sanity checks */
    libc_memcpy(
        &mut ndist.magic as *mut uint32 as *mut c_void,
        tmp as *const c_void,
        std::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(std::mem::size_of::<uint32>());
    libc_memcpy(
        &mut ndist.type_ as *mut uint32 as *mut c_void,
        tmp as *const c_void,
        std::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(std::mem::size_of::<uint32>());
    libc_memcpy(
        &mut ndist.nitems as *mut uint32 as *mut c_void,
        tmp as *const c_void,
        std::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(std::mem::size_of::<uint32>());

    if ndist.magic != STATS_NDISTINCT_MAGIC {
        elog!(
            ERROR,
            "invalid ndistinct magic {:08x} (expected {:08x})",
            ndist.magic,
            STATS_NDISTINCT_MAGIC
        );
        unreachable!();
    }
    if ndist.type_ != STATS_NDISTINCT_TYPE_BASIC {
        elog!(
            ERROR,
            "invalid ndistinct type {} (expected {})",
            ndist.type_,
            STATS_NDISTINCT_TYPE_BASIC
        );
        unreachable!();
    }
    if ndist.nitems == 0 {
        elog!(ERROR, "invalid zero-length item array in MVNDistinct");
        unreachable!();
    }

    /* what minimum bytea size do we expect for those parameters */
    minimum_size = MinSizeOfItems(ndist.nitems as usize);
    if (VARSIZE_ANY_EXHDR(data) as Size) < minimum_size {
        elog!(
            ERROR,
            "invalid MVNDistinct size {} (expected at least {})",
            VARSIZE_ANY_EXHDR(data),
            minimum_size
        );
        unreachable!();
    }

    /*
     * Allocate space for the ndistinct items (no space for each item's
     * attnos: those live in bitmapsets allocated separately)
     */
    ndistinct = palloc0(
        MAXALIGN(core::mem::offset_of!(MVNDistinct, items))
            + (ndist.nitems as usize * std::mem::size_of::<MVNDistinctItem>()),
    ) as *mut MVNDistinct;
    (*ndistinct).magic = ndist.magic;
    (*ndistinct).type_ = ndist.type_;
    (*ndistinct).nitems = ndist.nitems;

    i = 0;
    while i < (*ndistinct).nitems as c_int {
        let item: *mut MVNDistinctItem = &mut (*ndistinct).items[i as usize];

        /* ndistinct value */
        libc_memcpy(
            &mut (*item).ndistinct as *mut f64 as *mut c_void,
            tmp as *const c_void,
            std::mem::size_of::<f64>(),
        );
        tmp = tmp.add(std::mem::size_of::<f64>());

        /* number of attributes */
        libc_memcpy(
            &mut (*item).nattributes as *mut c_int as *mut c_void,
            tmp as *const c_void,
            std::mem::size_of::<c_int>(),
        );
        tmp = tmp.add(std::mem::size_of::<c_int>());
        debug_assert!(
            ((*item).nattributes >= 2) && ((*item).nattributes <= STATS_MAX_DIMENSIONS)
        );

        (*item).attributes =
            palloc((*item).nattributes as usize * std::mem::size_of::<AttrNumber>())
                as *mut AttrNumber;

        libc_memcpy(
            (*item).attributes as *mut c_void,
            tmp as *const c_void,
            std::mem::size_of::<AttrNumber>() * (*item).nattributes as usize,
        );
        tmp = tmp.add(std::mem::size_of::<AttrNumber>() * (*item).nattributes as usize);

        /* still within the bytea */
        debug_assert!(tmp <= (data as *mut c_char).add(VARSIZE_ANY(data) as usize));
        i += 1;
    }

    /* we should have consumed the whole bytea exactly */
    debug_assert!(tmp == (data as *mut c_char).add(VARSIZE_ANY(data) as usize));

    ndistinct
}

/*
 * pg_ndistinct_in
 *		input routine for type pg_ndistinct
 *
 * pg_ndistinct is real enough to be a table column, but it has no
 * operations of its own, and disallows input (just like pg_node_tree).
 */
pub unsafe fn pg_ndistinct_in(_fcinfo: FunctionCallInfo) -> Datum {
    ereport!(
        ERROR,
        "cannot accept a value of type pg_ndistinct"
    );

    PG_RETURN_VOID() /* keep compiler quiet */
}

/*
 * pg_ndistinct
 *		output routine for type pg_ndistinct
 *
 * Produces a human-readable representation of the value.
 */
pub unsafe fn pg_ndistinct_out(fcinfo: FunctionCallInfo) -> Datum {
    let data: *mut bytea = PG_GETARG_BYTEA_PP(fcinfo, 0);
    let ndist: *mut MVNDistinct = statext_ndistinct_deserialize(data);
    let mut i: c_int;
    let mut str: StringInfoData = std::mem::zeroed();

    initStringInfo(&mut str);
    appendStringInfoChar(&mut str, b'{' as c_char);

    i = 0;
    while i < (*ndist).nitems as c_int {
        let mut j: c_int;
        let item: MVNDistinctItem = (*ndist).items[i as usize];

        if i > 0 {
            appendStringInfoString(&mut str, c", ".as_ptr());
        }

        j = 0;
        while j < item.nattributes {
            let attnum: AttrNumber = *item.attributes.offset(j as isize);

            appendStringInfo(
                &mut str,
                c"%s%d".as_ptr(),
                if j == 0 { c"\"".as_ptr() } else { c", ".as_ptr() },
                attnum as c_int,
            );
            j += 1;
        }
        appendStringInfo(&mut str, c"\": %d".as_ptr(), item.ndistinct as c_int);
        i += 1;
    }

    appendStringInfoChar(&mut str, b'}' as c_char);

    PG_RETURN_CSTRING(str.data)
}

/*
 * pg_ndistinct_recv
 *		binary input routine for type pg_ndistinct
 */
pub unsafe fn pg_ndistinct_recv(_fcinfo: FunctionCallInfo) -> Datum {
    ereport!(
        ERROR,
        "cannot accept a value of type pg_ndistinct"
    );

    PG_RETURN_VOID() /* keep compiler quiet */
}

/*
 * pg_ndistinct_send
 *		binary output routine for type pg_ndistinct
 *
 * n-distinct is serialized into a bytea value, so let's send that.
 */
pub unsafe fn pg_ndistinct_send(fcinfo: FunctionCallInfo) -> Datum {
    byteasend(fcinfo)
}

/*
 * ndistinct_for_combination
 *		Estimates number of distinct values in a combination of columns.
 *
 * This uses the same ndistinct estimator as compute_scalar_stats() in
 * ANALYZE, i.e.,
 *		n*d / (n - f1 + f1*n/N)
 *
 * except that instead of values in a single column we are dealing with
 * combination of multiple columns.
 */
unsafe fn ndistinct_for_combination(
    totalrows: f64,
    data: *mut StatsBuildData,
    k: c_int,
    combination: *mut c_int,
) -> f64 {
    let mut i: c_int;
    let mut j: c_int;
    let mut f1: c_int;
    let mut cnt: c_int;
    let mut d: c_int;
    let isnull: *mut bool;
    let values: *mut Datum;
    let items: *mut SortItem;
    let mss: MultiSortSupport;
    let numrows: c_int = (*data).numrows;

    mss = multi_sort_init(k);

    /*
     * In order to determine the number of distinct elements, create separate
     * values[]/isnull[] arrays with all the data we have, then sort them
     * using the specified column combination as dimensions.  We could try to
     * sort in place, but it'd probably be more complex and bug-prone.
     */
    items = palloc(numrows as usize * std::mem::size_of::<SortItem>()) as *mut SortItem;
    values = palloc0(std::mem::size_of::<Datum>() * numrows as usize * k as usize) as *mut Datum;
    isnull = palloc0(std::mem::size_of::<bool>() * numrows as usize * k as usize) as *mut bool;

    i = 0;
    while i < numrows {
        (*items.offset(i as isize)).values = values.offset((i * k) as isize);
        (*items.offset(i as isize)).isnull = isnull.offset((i * k) as isize);
        i += 1;
    }

    /*
     * For each dimension, set up sort-support and fill in the values from the
     * sample data.
     *
     * We use the column data types' default sort operators and collations;
     * perhaps at some point it'd be worth using column-specific collations?
     */
    i = 0;
    while i < k {
        let typid: Oid;
        let type_: *mut TypeCacheEntry;
        let collid: Oid;
        let colstat: *mut VacAttrStats =
            *(*data).stats.offset(*combination.offset(i as isize) as isize);

        typid = (*colstat).attrtypid;
        collid = (*colstat).attrcollid;

        type_ = lookup_type_cache(typid, TYPECACHE_LT_OPR);
        if (*type_).lt_opr == InvalidOid
        /* shouldn't happen */
        {
            elog!(
                ERROR,
                "cache lookup failed for ordering operator for type {}",
                typid
            );
            unreachable!();
        }

        /* prepare the sort function for this dimension */
        multi_sort_add_dimension(mss, i, (*type_).lt_opr, collid);

        /* accumulate all the data for this dimension into the arrays */
        j = 0;
        while j < numrows {
            *(*items.offset(j as isize)).values.offset(i as isize) =
                *(*(*data).values.offset(*combination.offset(i as isize) as isize))
                    .offset(j as isize);
            *(*items.offset(j as isize)).isnull.offset(i as isize) =
                *(*(*data).nulls.offset(*combination.offset(i as isize) as isize))
                    .offset(j as isize);
            j += 1;
        }
        i += 1;
    }

    /* We can sort the array now ... */
    qsort_interruptible(
        items as *mut c_void,
        numrows as Size,
        std::mem::size_of::<SortItem>(),
        multi_sort_compare,
        mss as *mut c_void,
    );

    /* ... and count the number of distinct combinations */

    f1 = 0;
    cnt = 1;
    d = 1;
    i = 1;
    while i < numrows {
        if multi_sort_compare(
            items.offset(i as isize) as *const c_void,
            items.offset((i - 1) as isize) as *const c_void,
            mss as *mut c_void,
        ) != 0
        {
            if cnt == 1 {
                f1 += 1;
            }

            d += 1;
            cnt = 0;
        }

        cnt += 1;
        i += 1;
    }

    if cnt == 1 {
        f1 += 1;
    }

    estimate_ndistinct(totalrows, numrows, d, f1)
}

/* The Duj1 estimator (already used in analyze.c). */
unsafe fn estimate_ndistinct(totalrows: f64, numrows: c_int, d: c_int, f1: c_int) -> f64 {
    let numer: f64;
    let denom: f64;
    let mut ndistinct: f64;

    numer = numrows as f64 * d as f64;

    denom = (numrows - f1) as f64 + f1 as f64 * numrows as f64 / totalrows;

    ndistinct = numer / denom;

    /* Clamp to sane range in case of roundoff error */
    if ndistinct < d as f64 {
        ndistinct = d as f64;
    }

    if ndistinct > totalrows {
        ndistinct = totalrows;
    }

    (ndistinct + 0.5).floor()
}

/*
 * n_choose_k
 *		computes binomial coefficients using an algorithm that is both
 *		efficient and prevents overflows
 */
unsafe fn n_choose_k(mut n: c_int, mut k: c_int) -> c_int {
    let mut d: c_int;
    let mut r: c_int;

    debug_assert!((k > 0) && (n >= k));

    /* use symmetry of the binomial coefficients */
    k = Min(k, n - k);

    r = 1;
    d = 1;
    while d <= k {
        r *= n;
        n -= 1;
        r /= d;
        d += 1;
    }

    r
}

/*
 * num_combinations
 *		number of combinations, excluding single-value combinations
 */
unsafe fn num_combinations(n: c_int) -> c_int {
    (1 << n) - (n + 1)
}

/*
 * generator_init
 *		initialize the generator of combinations
 *
 * The generator produces combinations of K elements in the interval (0..N).
 * We prebuild all the combinations in this method, which is simpler than
 * generating them on the fly.
 */
unsafe fn generator_init(n: c_int, k: c_int) -> *mut CombinationGenerator {
    let state: *mut CombinationGenerator;

    debug_assert!((n >= k) && (k > 0));

    /* allocate the generator state as a single chunk of memory */
    state = palloc(std::mem::size_of::<CombinationGenerator>()) as *mut CombinationGenerator;

    (*state).ncombinations = n_choose_k(n, k);

    /* pre-allocate space for all combinations */
    (*state).combinations =
        palloc(std::mem::size_of::<c_int>() * k as usize * (*state).ncombinations as usize)
            as *mut c_int;

    (*state).current = 0;
    (*state).k = k;
    (*state).n = n;

    /* now actually pre-generate all the combinations of K elements */
    generate_combinations(state);

    /* make sure we got the expected number of combinations */
    debug_assert!((*state).current == (*state).ncombinations);

    /* reset the number, so we start with the first one */
    (*state).current = 0;

    state
}

/*
 * generator_next
 *		returns the next combination from the prebuilt list
 *
 * Returns a combination of K array indexes (0 .. N), as specified to
 * generator_init), or NULL when there are no more combination.
 */
unsafe fn generator_next(state: *mut CombinationGenerator) -> *mut c_int {
    if (*state).current == (*state).ncombinations {
        return std::ptr::null_mut();
    }

    let ret = (*state).combinations.offset(((*state).k * (*state).current) as isize);
    (*state).current += 1;
    ret
}

/*
 * generator_free
 *		free the internal state of the generator
 *
 * Releases the generator internal state (pre-built combinations).
 */
unsafe fn generator_free(state: *mut CombinationGenerator) {
    pfree((*state).combinations as *mut c_void);
    pfree(state as *mut c_void);
}

/*
 * generate_combinations_recurse
 *		given a prefix, generate all possible combinations
 *
 * Given a prefix (first few elements of the combination), generate following
 * elements recursively. We generate the combinations in lexicographic order,
 * which eliminates permutations of the same combination.
 */
unsafe fn generate_combinations_recurse(
    state: *mut CombinationGenerator,
    index: c_int,
    start: c_int,
    current: *mut c_int,
) {
    /* If we haven't filled all the elements, simply recurse. */
    if index < (*state).k {
        let mut i: c_int;

        /*
         * The values have to be in ascending order, so make sure we start
         * with the value passed by parameter.
         */

        i = start;
        while i < (*state).n {
            *current.offset(index as isize) = i;
            generate_combinations_recurse(state, index + 1, i + 1, current);
            i += 1;
        }
    } else {
        /* we got a valid combination, add it to the array */
        libc_memcpy(
            (*state).combinations.offset(((*state).k * (*state).current) as isize) as *mut c_void,
            current as *const c_void,
            (*state).k as usize * std::mem::size_of::<c_int>(),
        );
        (*state).current += 1;
    }
}

/*
 * generate_combinations
 *		generate all k-combinations of N elements
 */
unsafe fn generate_combinations(state: *mut CombinationGenerator) {
    let current: *mut c_int =
        palloc0(std::mem::size_of::<c_int>() * (*state).k as usize) as *mut c_int;

    generate_combinations_recurse(state, 0, 0, current);

    pfree(current as *mut c_void);
}

/* ---- memcpy via libc ---- */
extern "C" {
    #[link_name = "memcpy"]
    fn libc_memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/* ===================== local stubs for unported deps ===================== */

const STATS_NDISTINCT_MAGIC: uint32 = 0xA352BFA4;
const STATS_NDISTINCT_TYPE_BASIC: uint32 = 1;
const STATS_MAX_DIMENSIONS: c_int = 8;
const STATS_EXT_NDISTINCT: c_char = b'd' as c_char;

const TYPECACHE_LT_OPR: c_int = 0x0001;

const Anum_pg_statistic_ext_data_stxdndistinct: c_int = 4;
const STATEXTDATASTXOID: c_int = 0;

#[repr(C)]
pub struct MVNDistinctItem {
    pub ndistinct: f64,
    pub nattributes: c_int,
    pub attributes: *mut AttrNumber,
}

impl Copy for MVNDistinctItem {}
impl Clone for MVNDistinctItem {
    fn clone(&self) -> Self {
        *self
    }
}

#[repr(C)]
pub struct MVNDistinct {
    pub magic: uint32,
    pub type_: uint32,
    pub nitems: uint32,
    pub items: [MVNDistinctItem; 0],
}

#[repr(C)]
pub struct StatsBuildData {
    pub nattnums: c_int,
    pub attnums: *mut AttrNumber,
    pub stats: *mut *mut VacAttrStats,
    pub numrows: c_int,
    pub values: *mut *mut Datum,
    pub nulls: *mut *mut bool,
}

#[repr(C)]
pub struct SortItem {
    pub values: *mut Datum,
    pub isnull: *mut bool,
}

#[repr(C)]
pub struct VacAttrStats {
    pub attrtypid: Oid,
    pub attrcollid: Oid,
}

#[repr(C)]
pub struct TypeCacheEntry {
    pub lt_opr: Oid,
}

#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}

pub type MultiSortSupport = *mut c_void;

#[inline]
unsafe fn AttributeNumberIsValid(attno: AttrNumber) -> bool {
    attno != 0
    // TODO: src/include/access/attnum.h
}

#[inline]
unsafe fn Min(a: c_int, b: c_int) -> c_int {
    if a < b {
        a
    } else {
        b
    }
}

unsafe fn multi_sort_init(_ndims: c_int) -> MultiSortSupport {
    unimplemented!() // TODO: src/backend/statistics/extended_stats.c
}

unsafe fn multi_sort_add_dimension(_mss: MultiSortSupport, _sortdim: c_int, _oper: Oid, _collation: Oid) {
    unimplemented!() // TODO: src/backend/statistics/extended_stats.c
}

unsafe extern "C" fn multi_sort_compare(_a: *const c_void, _b: *const c_void, _arg: *mut c_void) -> c_int {
    unimplemented!() // TODO: src/backend/statistics/extended_stats.c
}

unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!() // TODO: src/backend/utils/cache/typcache.c
}

unsafe fn qsort_interruptible(
    _base: *mut c_void,
    _nel: Size,
    _elsize: Size,
    _cmp: unsafe extern "C" fn(*const c_void, *const c_void, *mut c_void) -> c_int,
    _arg: *mut c_void,
) {
    unimplemented!() // TODO: src/port/qsort_interruptible.c
}

unsafe fn SearchSysCache2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    unimplemented!() // TODO: src/backend/utils/cache/syscache.c
}

unsafe fn SysCacheGetAttr(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int, _isNull: *mut bool) -> Datum {
    unimplemented!() // TODO: src/backend/utils/cache/syscache.c
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO: src/backend/utils/cache/syscache.c
}

#[inline]
unsafe fn HeapTupleIsValid(htup: HeapTuple) -> bool {
    !htup.is_null()
}

pub type HeapTuple = *mut c_void;
pub type bytea = c_void;
pub type FunctionCallInfo = *mut c_void;

unsafe fn byteasend(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO: src/backend/utils/adt/varlena.c
}

unsafe fn BoolGetDatum(_b: bool) -> Datum {
    unimplemented!() // TODO: src/include/postgres.h
}

unsafe fn DatumGetByteaPP(_d: Datum) -> *mut bytea {
    unimplemented!() // TODO: src/include/fmgr.h
}

unsafe fn PG_GETARG_BYTEA_PP(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut bytea {
    unimplemented!() // TODO: src/include/fmgr.h
}

unsafe fn PG_RETURN_VOID() -> Datum {
    0
}

unsafe fn PG_RETURN_CSTRING(_c: *mut c_char) -> Datum {
    unimplemented!() // TODO: src/include/fmgr.h
}

const VARHDRSZ: Size = 4;

unsafe fn VARSIZE_ANY(_ptr: *mut bytea) -> u32 {
    unimplemented!() // TODO: src/include/varatt.h
}

unsafe fn VARSIZE_ANY_EXHDR(_ptr: *mut bytea) -> u32 {
    unimplemented!() // TODO: src/include/varatt.h
}

unsafe fn VARDATA_ANY(_ptr: *mut c_void) -> *mut c_char {
    unimplemented!() // TODO: src/include/varatt.h
}

unsafe fn VARDATA(_ptr: *mut c_void) -> *mut c_char {
    unimplemented!() // TODO: src/include/varatt.h
}

unsafe fn SET_VARSIZE(_ptr: *mut bytea, _len: c_int) {
    unimplemented!() // TODO: src/include/varatt.h
}

unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!() // TODO: src/common/stringinfo.c
}

unsafe fn appendStringInfoChar(_str: *mut StringInfoData, _ch: c_char) {
    unimplemented!() // TODO: src/common/stringinfo.c
}

unsafe fn appendStringInfoString(_str: *mut StringInfoData, _s: *const c_char) {
    unimplemented!() // TODO: src/common/stringinfo.c
}

extern "C" {
    fn appendStringInfo(str: *mut StringInfoData, fmt: *const c_char, ...);
}

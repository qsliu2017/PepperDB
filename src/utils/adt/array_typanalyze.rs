//! src/backend/utils/adt/array_typanalyze.c
//!
//! Functions for gathering statistics from array columns
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_POINTER, PG_RETURN_BOOL};
use std::ffi::{c_int, c_void};

/*
 * To avoid consuming too much memory, IO and CPU load during analysis, and/or
 * too much space in the resulting pg_statistic rows, we ignore arrays that
 * are wider than ARRAY_WIDTH_THRESHOLD (after detoasting!).  Note that this
 * number is considerably more than the similar WIDTH_THRESHOLD limit used
 * in analyze.c's standard typanalyze code.
 */
const ARRAY_WIDTH_THRESHOLD: c_int = 0x10000;

/* Extra data for compute_array_stats function */
#[repr(C)]
struct ArrayAnalyzeExtraData {
    /* Information about array element type */
    type_id: Oid,    /* element type's OID */
    eq_opr: Oid,     /* default equality operator's OID */
    coll_id: Oid,    /* collation to use */
    typbyval: bool,  /* physical properties of element type */
    typlen: int16,
    typalign: c_char,

    /*
     * Lookup data for element type's comparison and hash functions (these are
     * in the type's typcache entry, which we expect to remain valid over the
     * lifespan of the ANALYZE run)
     */
    cmp: *mut FmgrInfo,
    hash: *mut FmgrInfo,

    /* Saved state from std_typanalyze() */
    std_compute_stats: AnalyzeAttrComputeStatsFunc,
    std_extra_data: *mut c_void,
}

/*
 * While compute_array_stats is running, we keep a pointer to the extra data
 * here for use by assorted subroutines.  compute_array_stats doesn't
 * currently need to be re-entrant, so avoiding this is not worth the extra
 * notational cruft that would be needed.
 */
static mut array_extra_data: *mut ArrayAnalyzeExtraData = std::ptr::null_mut();

/* A hash table entry for the Lossy Counting algorithm */
#[repr(C)]
struct TrackItem {
    key: Datum,          /* This is 'e' from the LC algorithm. */
    frequency: c_int,    /* This is 'f'. */
    delta: c_int,        /* And this is 'delta'. */
    last_container: c_int, /* For de-duplication of array elements. */
}

/* A hash table entry for distinct-elements counts */
#[repr(C)]
struct DECountItem {
    count: c_int,     /* Count of distinct elements in an array */
    frequency: c_int, /* Number of arrays seen with this count */
}

/*
 * array_typanalyze -- typanalyze function for array columns
 */
#[no_mangle]
pub unsafe extern "C" fn array_typanalyze(fcinfo: FunctionCallInfo) -> Datum {
    let stats = PG_GETARG_POINTER!(fcinfo, 0) as *mut VacAttrStats;
    let element_typeid: Oid;
    let typentry: *mut TypeCacheEntry;
    let extra_data: *mut ArrayAnalyzeExtraData;

    /*
     * Call the standard typanalyze function.  It may fail to find needed
     * operators, in which case we also can't do anything, so just fail.
     */
    if !std_typanalyze(stats) {
        PG_RETURN_BOOL!(false);
    }

    /*
     * Check attribute data type is a varlena array (or a domain over one).
     */
    element_typeid = get_base_element_type((*stats).attrtypid);
    if !OidIsValid(element_typeid) {
        elog!(ERROR, "array_typanalyze was invoked for non-array type {}",
              (*stats).attrtypid);
    }

    /*
     * Gather information about the element type.  If we fail to find
     * something, return leaving the state from std_typanalyze() in place.
     */
    typentry = lookup_type_cache(element_typeid,
                                 TYPECACHE_EQ_OPR |
                                 TYPECACHE_CMP_PROC_FINFO |
                                 TYPECACHE_HASH_PROC_FINFO);

    if !OidIsValid((*typentry).eq_opr) ||
        !OidIsValid((*typentry).cmp_proc_finfo.fn_oid) ||
        !OidIsValid((*typentry).hash_proc_finfo.fn_oid)
    {
        PG_RETURN_BOOL!(true);
    }

    /* Store our findings for use by compute_array_stats() */
    extra_data = palloc(std::mem::size_of::<ArrayAnalyzeExtraData>()) as *mut ArrayAnalyzeExtraData;
    (*extra_data).type_id = (*typentry).type_id;
    (*extra_data).eq_opr = (*typentry).eq_opr;
    (*extra_data).coll_id = (*stats).attrcollid; /* collation we should use */
    (*extra_data).typbyval = (*typentry).typbyval;
    (*extra_data).typlen = (*typentry).typlen;
    (*extra_data).typalign = (*typentry).typalign;
    (*extra_data).cmp = &mut (*typentry).cmp_proc_finfo;
    (*extra_data).hash = &mut (*typentry).hash_proc_finfo;

    /* Save old compute_stats and extra_data for scalar statistics ... */
    (*extra_data).std_compute_stats = (*stats).compute_stats;
    (*extra_data).std_extra_data = (*stats).extra_data;

    /* ... and replace with our info */
    (*stats).compute_stats = Some(compute_array_stats);
    (*stats).extra_data = extra_data as *mut c_void;

    /*
     * Note we leave stats->minrows set as std_typanalyze set it.  Should it
     * be increased for array analysis purposes?
     */

    PG_RETURN_BOOL!(true)
}

/*
 * compute_array_stats() -- compute statistics for an array column
 *
 * This function computes statistics useful for determining selectivity of
 * the array operators <@, &&, and @>.  It is invoked by ANALYZE via the
 * compute_stats hook after sample rows have been collected.
 *
 * We also invoke the standard compute_stats function, which will compute
 * "scalar" statistics relevant to the btree-style array comparison operators.
 * However, exact duplicates of an entire array may be rare despite many
 * arrays sharing individual elements.  This especially afflicts long arrays,
 * which are also liable to lack all scalar statistics due to the low
 * WIDTH_THRESHOLD used in analyze.c.  So, in addition to the standard stats,
 * we find the most common array elements and compute a histogram of distinct
 * element counts.
 *
 * The algorithm used is Lossy Counting, as proposed in the paper "Approximate
 * frequency counts over data streams" by G. S. Manku and R. Motwani, in
 * Proceedings of the 28th International Conference on Very Large Data Bases,
 * Hong Kong, China, August 2002, section 4.2. The paper is available at
 * http://www.vldb.org/conf/2002/S10P03.pdf
 *
 * The Lossy Counting (aka LC) algorithm goes like this:
 * Let s be the threshold frequency for an item (the minimum frequency we
 * are interested in) and epsilon the error margin for the frequency. Let D
 * be a set of triples (e, f, delta), where e is an element value, f is that
 * element's frequency (actually, its current occurrence count) and delta is
 * the maximum error in f. We start with D empty and process the elements in
 * batches of size w. (The batch size is also known as "bucket size" and is
 * equal to 1/epsilon.) Let the current batch number be b_current, starting
 * with 1. For each element e we either increment its f count, if it's
 * already in D, or insert a new triple into D with values (e, 1, b_current
 * - 1). After processing each batch we prune D, by removing from it all
 * elements with f + delta <= b_current.  After the algorithm finishes we
 * suppress all elements from D that do not satisfy f >= (s - epsilon) * N,
 * where N is the total number of elements in the input.  We emit the
 * remaining elements with estimated frequency f/N.  The LC paper proves
 * that this algorithm finds all elements with true frequency at least s,
 * and that no frequency is overestimated or is underestimated by more than
 * epsilon.  Furthermore, given reasonable assumptions about the input
 * distribution, the required table size is no more than about 7 times w.
 *
 * In the absence of a principled basis for other particular values, we
 * follow ts_typanalyze() and use parameters s = 0.07/K, epsilon = s/10.
 * But we leave out the correction for stopwords, which do not apply to
 * arrays.  These parameters give bucket width w = K/0.007 and maximum
 * expected hashtable size of about 1000 * K.
 *
 * Elements may repeat within an array.  Since duplicates do not change the
 * behavior of <@, && or @>, we want to count each element only once per
 * array.  Therefore, we store in the finished pg_statistic entry each
 * element's frequency as the fraction of all non-null rows that contain it.
 * We divide the raw counts by nonnull_cnt to get those figures.
 */
unsafe extern "C" fn compute_array_stats(
    stats: *mut VacAttrStats,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: c_int,
    totalrows: f64,
) {
    let extra_data: *mut ArrayAnalyzeExtraData;
    let mut num_mcelem: c_int;
    let mut null_elem_cnt: c_int = 0;
    let mut analyzed_rows: c_int = 0;

    /* This is D from the LC algorithm. */
    let elements_tab: *mut HTAB;
    let mut elem_hash_ctl: HASHCTL = std::mem::zeroed();
    let mut scan_status: HASH_SEQ_STATUS = std::mem::zeroed();

    /* This is the current bucket number from the LC algorithm */
    let mut b_current: c_int;

    /* This is 'w' from the LC algorithm */
    let bucket_width: c_int;
    let mut array_no: c_int;
    let mut element_no: int64;
    let mut item: *mut TrackItem;
    let mut slot_idx: c_int;
    let count_tab: *mut HTAB;
    let mut count_hash_ctl: HASHCTL = std::mem::zeroed();
    let mut count_item: *mut DECountItem;

    extra_data = (*stats).extra_data as *mut ArrayAnalyzeExtraData;

    /*
     * Invoke analyze.c's standard analysis function to create scalar-style
     * stats for the column.  It will expect its own extra_data pointer, so
     * temporarily install that.
     */
    (*stats).extra_data = (*extra_data).std_extra_data;
    ((*extra_data).std_compute_stats.unwrap())(stats, fetchfunc, samplerows, totalrows);
    (*stats).extra_data = extra_data as *mut c_void;

    /*
     * Set up static pointer for use by subroutines.  We wait till here in
     * case std_compute_stats somehow recursively invokes us (probably not
     * possible, but ...)
     */
    array_extra_data = extra_data;

    /*
     * We want statistics_target * 10 elements in the MCELEM array. This
     * multiplier is pretty arbitrary, but is meant to reflect the fact that
     * the number of individual elements tracked in pg_statistic ought to be
     * more than the number of values for a simple scalar column.
     */
    num_mcelem = (*stats).attstattarget * 10;

    /*
     * We set bucket width equal to num_mcelem / 0.007 as per the comment
     * above.
     */
    bucket_width = num_mcelem * 1000 / 7;

    /*
     * Create the hashtable. It will be in local memory, so we don't need to
     * worry about overflowing the initial size. Also we don't need to pay any
     * attention to locking and memory management.
     */
    elem_hash_ctl.keysize = std::mem::size_of::<Datum>() as Size;
    elem_hash_ctl.entrysize = std::mem::size_of::<TrackItem>() as Size;
    elem_hash_ctl.hash = Some(element_hash);
    elem_hash_ctl.match_ = Some(element_match);
    elem_hash_ctl.hcxt = CurrentMemoryContext;
    elements_tab = hash_create(c"Analyzed elements table".as_ptr(),
                               num_mcelem as c_long,
                               &mut elem_hash_ctl,
                               HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

    /* hashtable for array distinct elements counts */
    count_hash_ctl.keysize = std::mem::size_of::<c_int>() as Size;
    count_hash_ctl.entrysize = std::mem::size_of::<DECountItem>() as Size;
    count_hash_ctl.hcxt = CurrentMemoryContext;
    count_tab = hash_create(c"Array distinct element count table".as_ptr(),
                            64,
                            &mut count_hash_ctl,
                            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    /* Initialize counters. */
    b_current = 1;
    element_no = 0;

    /* Loop over the arrays. */
    array_no = 0;
    while array_no < samplerows {
        let value: Datum;
        let mut isnull: bool = false;
        let array: *mut ArrayType;
        let mut num_elems: c_int = 0;
        let mut elem_values: *mut Datum = std::ptr::null_mut();
        let mut elem_nulls: *mut bool = std::ptr::null_mut();
        let mut null_present: bool;
        let mut j: c_int;
        let prev_element_no: int64 = element_no;
        let distinct_count: c_int;
        let mut count_item_found: bool = false;

        vacuum_delay_point(true);

        value = (fetchfunc.unwrap())(stats, array_no, &mut isnull);
        if isnull {
            /* ignore arrays that are null overall */
            array_no += 1;
            continue;
        }

        /* Skip too-large values. */
        if toast_raw_datum_size(value) > ARRAY_WIDTH_THRESHOLD as Size {
            array_no += 1;
            continue;
        } else {
            analyzed_rows += 1;
        }

        /*
         * Now detoast the array if needed, and deconstruct into datums.
         */
        array = DatumGetArrayTypeP(value);

        Assert!(ARR_ELEMTYPE(array) == (*extra_data).type_id);
        deconstruct_array(array,
                          (*extra_data).type_id,
                          (*extra_data).typlen as c_int,
                          (*extra_data).typbyval,
                          (*extra_data).typalign,
                          &mut elem_values, &mut elem_nulls, &mut num_elems);

        /*
         * We loop through the elements in the array and add them to our
         * tracking hashtable.
         */
        null_present = false;
        j = 0;
        while j < num_elems {
            let elem_value: Datum;
            let mut found: bool = false;

            /* No null element processing other than flag setting here */
            if *elem_nulls.offset(j as isize) {
                null_present = true;
                j += 1;
                continue;
            }

            /* Lookup current element in hashtable, adding it if new */
            elem_value = *elem_values.offset(j as isize);
            item = hash_search(elements_tab,
                               &elem_value as *const Datum as *const c_void,
                               HASH_ENTER, &mut found) as *mut TrackItem;

            if found {
                /* The element value is already on the tracking list */

                /*
                 * The operators we assist ignore duplicate array elements, so
                 * count a given distinct element only once per array.
                 */
                if (*item).last_container == array_no {
                    j += 1;
                    continue;
                }

                (*item).frequency += 1;
                (*item).last_container = array_no;
            } else {
                /* Initialize new tracking list element */

                /*
                 * If element type is pass-by-reference, we must copy it into
                 * palloc'd space, so that we can release the array below. (We
                 * do this so that the space needed for element values is
                 * limited by the size of the hashtable; if we kept all the
                 * array values around, it could be much more.)
                 */
                (*item).key = datumCopy(elem_value,
                                        (*extra_data).typbyval,
                                        (*extra_data).typlen as c_int);

                (*item).frequency = 1;
                (*item).delta = b_current - 1;
                (*item).last_container = array_no;
            }

            /* element_no is the number of elements processed (ie N) */
            element_no += 1;

            /* We prune the D structure after processing each bucket */
            if element_no % bucket_width as int64 == 0 {
                prune_element_hashtable(elements_tab, b_current);
                b_current += 1;
            }

            j += 1;
        }

        /* Count null element presence once per array. */
        if null_present {
            null_elem_cnt += 1;
        }

        /* Update frequency of the particular array distinct element count. */
        distinct_count = (element_no - prev_element_no) as c_int;
        count_item = hash_search(count_tab, &distinct_count as *const c_int as *const c_void,
                                 HASH_ENTER,
                                 &mut count_item_found) as *mut DECountItem;

        if count_item_found {
            (*count_item).frequency += 1;
        } else {
            (*count_item).frequency = 1;
        }

        /* Free memory allocated while detoasting. */
        if PointerGetDatum(array as *const c_void) != value {
            pfree(array as *mut c_void);
        }
        pfree(elem_values as *mut c_void);
        pfree(elem_nulls as *mut c_void);

        array_no += 1;
    }

    /* Skip pg_statistic slots occupied by standard statistics */
    slot_idx = 0;
    while slot_idx < STATISTIC_NUM_SLOTS && (*stats).stakind[slot_idx as usize] != 0 {
        slot_idx += 1;
    }
    if slot_idx > STATISTIC_NUM_SLOTS - 2 {
        elog!(ERROR, "insufficient pg_statistic slots for array stats");
    }

    /* We can only compute real stats if we found some non-null values. */
    if analyzed_rows > 0 {
        let nonnull_cnt: c_int = analyzed_rows;
        let count_items_count: c_int;
        let mut i: c_int;
        let sort_table: *mut *mut TrackItem;
        let mut track_len: c_int;
        let cutoff_freq: int64;
        let mut minfreq: int64;
        let mut maxfreq: int64;

        /*
         * We assume the standard stats code already took care of setting
         * stats_valid, stanullfrac, stawidth, stadistinct.  We'd have to
         * re-compute those values if we wanted to not store the standard
         * stats.
         */

        /*
         * Construct an array of the interesting hashtable items, that is,
         * those meeting the cutoff frequency (s - epsilon)*N.  Also identify
         * the minimum and maximum frequencies among these items.
         *
         * Since epsilon = s/10 and bucket_width = 1/epsilon, the cutoff
         * frequency is 9*N / bucket_width.
         */
        cutoff_freq = 9 * element_no / bucket_width as int64;

        i = hash_get_num_entries(elements_tab) as c_int; /* surely enough space */
        sort_table = palloc((std::mem::size_of::<*mut TrackItem>() as c_int * i) as usize)
            as *mut *mut TrackItem;

        hash_seq_init(&mut scan_status, elements_tab);
        track_len = 0;
        minfreq = element_no;
        maxfreq = 0;
        loop {
            item = hash_seq_search(&mut scan_status) as *mut TrackItem;
            if item.is_null() {
                break;
            }
            if (*item).frequency as int64 > cutoff_freq {
                *sort_table.offset(track_len as isize) = item;
                track_len += 1;
                minfreq = Min(minfreq, (*item).frequency as int64);
                maxfreq = Max(maxfreq, (*item).frequency as int64);
            }
        }
        Assert!(track_len <= i);

        /* emit some statistics for debug purposes */
        elog!(DEBUG3, "compute_array_stats: target # mces = {}, bucket width = {}, # elements = {}, hashtable size = {}, usable entries = {}",
              num_mcelem, bucket_width, element_no, i, track_len);

        /*
         * If we obtained more elements than we really want, get rid of those
         * with least frequencies.  The easiest way is to qsort the array into
         * descending frequency order and truncate the array.
         */
        if num_mcelem < track_len {
            qsort_interruptible(sort_table as *mut c_void, track_len as Size,
                                std::mem::size_of::<*mut TrackItem>() as Size,
                                Some(trackitem_compare_frequencies_desc), std::ptr::null_mut());
            /* reset minfreq to the smallest frequency we're keeping */
            minfreq = (**sort_table.offset((num_mcelem - 1) as isize)).frequency as int64;
        } else {
            num_mcelem = track_len;
        }

        /* Generate MCELEM slot entry */
        if num_mcelem > 0 {
            let old_context: MemoryContext;
            let mcelem_values: *mut Datum;
            let mcelem_freqs: *mut float4;

            /*
             * We want to store statistics sorted on the element value using
             * the element type's default comparison function.  This permits
             * fast binary searches in selectivity estimation functions.
             */
            qsort_interruptible(sort_table as *mut c_void, num_mcelem as Size,
                                std::mem::size_of::<*mut TrackItem>() as Size,
                                Some(trackitem_compare_element), std::ptr::null_mut());

            /* Must copy the target values into anl_context */
            old_context = MemoryContextSwitchTo((*stats).anl_context);

            /*
             * We sorted statistics on the element value, but we want to be
             * able to find the minimal and maximal frequencies without going
             * through all the values.  We also want the frequency of null
             * elements.  Store these three values at the end of mcelem_freqs.
             */
            mcelem_values = palloc(num_mcelem as usize * std::mem::size_of::<Datum>()) as *mut Datum;
            mcelem_freqs = palloc((num_mcelem + 3) as usize * std::mem::size_of::<float4>()) as *mut float4;

            /*
             * See comments above about use of nonnull_cnt as the divisor for
             * the final frequency estimates.
             */
            i = 0;
            while i < num_mcelem {
                let titem: *mut TrackItem = *sort_table.offset(i as isize);

                *mcelem_values.offset(i as isize) = datumCopy((*titem).key,
                                                              (*extra_data).typbyval,
                                                              (*extra_data).typlen as c_int);
                *mcelem_freqs.offset(i as isize) = ((*titem).frequency as f64 /
                    nonnull_cnt as f64) as float4;
                i += 1;
            }
            *mcelem_freqs.offset(i as isize) = (minfreq as f64 / nonnull_cnt as f64) as float4;
            i += 1;
            *mcelem_freqs.offset(i as isize) = (maxfreq as f64 / nonnull_cnt as f64) as float4;
            i += 1;
            *mcelem_freqs.offset(i as isize) = (null_elem_cnt as f64 / nonnull_cnt as f64) as float4;
            i += 1;
            let _ = i;

            MemoryContextSwitchTo(old_context);

            (*stats).stakind[slot_idx as usize] = STATISTIC_KIND_MCELEM as int16;
            (*stats).staop[slot_idx as usize] = (*extra_data).eq_opr;
            (*stats).stacoll[slot_idx as usize] = (*extra_data).coll_id;
            (*stats).stanumbers[slot_idx as usize] = mcelem_freqs;
            /* See above comment about extra stanumber entries */
            (*stats).numnumbers[slot_idx as usize] = num_mcelem + 3;
            (*stats).stavalues[slot_idx as usize] = mcelem_values;
            (*stats).numvalues[slot_idx as usize] = num_mcelem;
            /* We are storing values of element type */
            (*stats).statypid[slot_idx as usize] = (*extra_data).type_id;
            (*stats).statyplen[slot_idx as usize] = (*extra_data).typlen;
            (*stats).statypbyval[slot_idx as usize] = (*extra_data).typbyval;
            (*stats).statypalign[slot_idx as usize] = (*extra_data).typalign;
            slot_idx += 1;
        }

        /* Generate DECHIST slot entry */
        count_items_count = hash_get_num_entries(count_tab) as c_int;
        if count_items_count > 0 {
            let mut num_hist: c_int = (*stats).attstattarget;
            let sorted_count_items: *mut *mut DECountItem;
            let mut j: c_int;
            let delta: c_int;
            let mut frac: int64;
            let hist: *mut float4;

            /* num_hist must be at least 2 for the loop below to work */
            num_hist = Max(num_hist, 2);

            /*
             * Create an array of DECountItem pointers, and sort them into
             * increasing count order.
             */
            sorted_count_items = palloc(std::mem::size_of::<*mut DECountItem>() * count_items_count as usize)
                as *mut *mut DECountItem;
            hash_seq_init(&mut scan_status, count_tab);
            j = 0;
            loop {
                count_item = hash_seq_search(&mut scan_status) as *mut DECountItem;
                if count_item.is_null() {
                    break;
                }
                *sorted_count_items.offset(j as isize) = count_item;
                j += 1;
            }
            qsort_interruptible(sorted_count_items as *mut c_void, count_items_count as Size,
                                std::mem::size_of::<*mut DECountItem>() as Size,
                                Some(countitem_compare_count), std::ptr::null_mut());

            /*
             * Prepare to fill stanumbers with the histogram, followed by the
             * average count.  This array must be stored in anl_context.
             */
            hist = MemoryContextAlloc((*stats).anl_context,
                                      std::mem::size_of::<float4>() * (num_hist + 1) as usize) as *mut float4;
            *hist.offset(num_hist as isize) = (element_no as f64 / nonnull_cnt as f64) as float4;

            /*----------
             * Construct the histogram of distinct-element counts (DECs).
             *
             * The object of this loop is to copy the min and max DECs to
             * hist[0] and hist[num_hist - 1], along with evenly-spaced DECs
             * in between (where "evenly-spaced" is with reference to the
             * whole input population of arrays).  If we had a complete sorted
             * array of DECs, one per analyzed row, the i'th hist value would
             * come from DECs[i * (analyzed_rows - 1) / (num_hist - 1)]
             * (compare the histogram-making loop in compute_scalar_stats()).
             * But instead of that we have the sorted_count_items[] array,
             * which holds unique DEC values with their frequencies (that is,
             * a run-length-compressed version of the full array).  So we
             * control advancing through sorted_count_items[] with the
             * variable "frac", which is defined as (x - y) * (num_hist - 1),
             * where x is the index in the notional DECs array corresponding
             * to the start of the next sorted_count_items[] element's run,
             * and y is the index in DECs from which we should take the next
             * histogram value.  We have to advance whenever x <= y, that is
             * frac <= 0.  The x component is the sum of the frequencies seen
             * so far (up through the current sorted_count_items[] element),
             * and of course y * (num_hist - 1) = i * (analyzed_rows - 1),
             * per the subscript calculation above.  (The subscript calculation
             * implies dropping any fractional part of y; in this formulation
             * that's handled by not advancing until frac reaches 1.)
             *
             * Even though frac has a bounded range, it could overflow int32
             * when working with very large statistics targets, so we do that
             * math in int64.
             *----------
             */
            delta = analyzed_rows - 1;
            j = 0; /* current index in sorted_count_items */
            /* Initialize frac for sorted_count_items[0]; y is initially 0 */
            frac = (**sorted_count_items.offset(0)).frequency as int64 * (num_hist - 1) as int64;
            i = 0;
            while i < num_hist {
                while frac <= 0 {
                    /* Advance, and update x component of frac */
                    j += 1;
                    frac += (**sorted_count_items.offset(j as isize)).frequency as int64 * (num_hist - 1) as int64;
                }
                *hist.offset(i as isize) = (**sorted_count_items.offset(j as isize)).count as float4;
                frac -= delta as int64; /* update y for upcoming i increment */
                i += 1;
            }
            Assert!(j == count_items_count - 1);

            (*stats).stakind[slot_idx as usize] = STATISTIC_KIND_DECHIST as int16;
            (*stats).staop[slot_idx as usize] = (*extra_data).eq_opr;
            (*stats).stacoll[slot_idx as usize] = (*extra_data).coll_id;
            (*stats).stanumbers[slot_idx as usize] = hist;
            (*stats).numnumbers[slot_idx as usize] = num_hist + 1;
            slot_idx += 1;
            let _ = slot_idx;
        }
    }

    /*
     * We don't need to bother cleaning up any of our temporary palloc's. The
     * hashtable should also go away, as it used a child memory context.
     */
}

/*
 * A function to prune the D structure from the Lossy Counting algorithm.
 * Consult compute_tsvector_stats() for wider explanation.
 */
unsafe fn prune_element_hashtable(elements_tab: *mut HTAB, b_current: c_int) {
    let mut scan_status: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut item: *mut TrackItem;

    hash_seq_init(&mut scan_status, elements_tab);
    loop {
        item = hash_seq_search(&mut scan_status) as *mut TrackItem;
        if item.is_null() {
            break;
        }
        if (*item).frequency + (*item).delta <= b_current {
            let value: Datum = (*item).key;

            if hash_search(elements_tab, &mut (*item).key as *mut Datum as *const c_void,
                           HASH_REMOVE, std::ptr::null_mut()).is_null()
            {
                elog!(ERROR, "hash table corrupted");
            }
            /* We should free memory if element is not passed by value */
            if !(*array_extra_data).typbyval {
                pfree(DatumGetPointer(value) as *mut c_void);
            }
        }
    }
}

/*
 * Hash function for elements.
 *
 * We use the element type's default hash opclass, and the column collation
 * if the type is collation-sensitive.
 */
unsafe extern "C" fn element_hash(key: *const c_void, _keysize: Size) -> uint32 {
    let d: Datum = *(key as *const Datum);
    let h: Datum;

    h = FunctionCall1Coll((*array_extra_data).hash,
                          (*array_extra_data).coll_id,
                          d);
    DatumGetUInt32(h)
}

/*
 * Matching function for elements, to be used in hashtable lookups.
 */
unsafe extern "C" fn element_match(key1: *const c_void, key2: *const c_void, _keysize: Size) -> c_int {
    /* The keysize parameter is superfluous here */
    element_compare(key1, key2)
}

/*
 * Comparison function for elements.
 *
 * We use the element type's default btree opclass, and the column collation
 * if the type is collation-sensitive.
 *
 * XXX consider using SortSupport infrastructure
 */
unsafe fn element_compare(key1: *const c_void, key2: *const c_void) -> c_int {
    let d1: Datum = *(key1 as *const Datum);
    let d2: Datum = *(key2 as *const Datum);
    let c: Datum;

    c = FunctionCall2Coll((*array_extra_data).cmp,
                          (*array_extra_data).coll_id,
                          d1, d2);
    DatumGetInt32(c)
}

/*
 * Comparator for sorting TrackItems by frequencies (descending sort)
 */
unsafe extern "C" fn trackitem_compare_frequencies_desc(e1: *const c_void, e2: *const c_void, _arg: *mut c_void) -> c_int {
    let t1 = e1 as *const *const TrackItem;
    let t2 = e2 as *const *const TrackItem;

    (**t2).frequency - (**t1).frequency
}

/*
 * Comparator for sorting TrackItems by element values
 */
unsafe extern "C" fn trackitem_compare_element(e1: *const c_void, e2: *const c_void, _arg: *mut c_void) -> c_int {
    let t1 = e1 as *const *const TrackItem;
    let t2 = e2 as *const *const TrackItem;

    element_compare(&(**t1).key as *const Datum as *const c_void,
                    &(**t2).key as *const Datum as *const c_void)
}

/*
 * Comparator for sorting DECountItems by count
 */
unsafe extern "C" fn countitem_compare_count(e1: *const c_void, e2: *const c_void, _arg: *mut c_void) -> c_int {
    let t1 = e1 as *const *const DECountItem;
    let t2 = e2 as *const *const DECountItem;

    if (**t1).count < (**t2).count {
        -1
    } else if (**t1).count == (**t2).count {
        0
    } else {
        1
    }
}

/* ---- Local stubs for unported dependencies ---- */

unsafe fn std_typanalyze(_stats: *mut VacAttrStats) -> bool { unimplemented!() /* TODO: commands/vacuum.h */ }
unsafe fn get_base_element_type(_typid: Oid) -> Oid { unimplemented!() /* TODO: utils/lsyscache.h */ }
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry { unimplemented!() /* TODO: utils/typcache.h */ }
unsafe fn vacuum_delay_point(_is_analyze: bool) { unimplemented!() /* TODO: commands/vacuum.h */ }
unsafe fn toast_raw_datum_size(_value: Datum) -> Size { unimplemented!() /* TODO: access/detoast.h */ }
unsafe fn DatumGetArrayTypeP(_d: Datum) -> *mut ArrayType { unimplemented!() /* TODO: utils/array.h */ }
unsafe fn ARR_ELEMTYPE(_a: *mut ArrayType) -> Oid { unimplemented!() /* TODO: utils/array.h */ }
unsafe fn deconstruct_array(_array: *mut ArrayType, _elmtype: Oid, _elmlen: c_int, _elmbyval: bool, _elmalign: c_char, _elemsp: *mut *mut Datum, _nullsp: *mut *mut bool, _nelemsp: *mut c_int) { unimplemented!() /* TODO: utils/array.h */ }
unsafe fn datumCopy(_value: Datum, _typByVal: bool, _typLen: c_int) -> Datum { unimplemented!() /* TODO: utils/datum.h */ }
unsafe fn hash_create(_tabname: *const c_char, _nelem: c_long, _info: *mut HASHCTL, _flags: c_int) -> *mut HTAB { unimplemented!() /* TODO: utils/hsearch.h */ }
unsafe fn hash_search(_hashp: *mut HTAB, _keyPtr: *const c_void, _action: HASHACTION, _foundPtr: *mut bool) -> *mut c_void { unimplemented!() /* TODO: utils/hsearch.h */ }
unsafe fn hash_seq_init(_status: *mut HASH_SEQ_STATUS, _hashp: *mut HTAB) { unimplemented!() /* TODO: utils/hsearch.h */ }
unsafe fn hash_seq_search(_status: *mut HASH_SEQ_STATUS) -> *mut c_void { unimplemented!() /* TODO: utils/hsearch.h */ }
unsafe fn hash_get_num_entries(_hashp: *mut HTAB) -> c_long { unimplemented!() /* TODO: utils/hsearch.h */ }
unsafe fn qsort_interruptible(_base: *mut c_void, _nel: Size, _elsize: Size, _cmp: qsort_arg_comparator, _arg: *mut c_void) { unimplemented!() /* TODO: lib/qsort_interruptible */ }
unsafe fn FunctionCall1Coll(_flinfo: *mut FmgrInfo, _collation: Oid, _arg1: Datum) -> Datum { unimplemented!() /* TODO: fmgr.h */ }
unsafe fn FunctionCall2Coll(_flinfo: *mut FmgrInfo, _collation: Oid, _arg1: Datum, _arg2: Datum) -> Datum { unimplemented!() /* TODO: fmgr.h */ }

/* ---- Local stub types for unported dependencies ---- */

#[repr(C)]
pub struct VacAttrStats {
    pub attrtypid: Oid,
    pub attrcollid: Oid,
    pub attstattarget: c_int,
    pub anl_context: MemoryContext,
    pub compute_stats: AnalyzeAttrComputeStatsFunc,
    pub extra_data: *mut c_void,
    pub stakind: [int16; STATISTIC_NUM_SLOTS as usize],
    pub staop: [Oid; STATISTIC_NUM_SLOTS as usize],
    pub stacoll: [Oid; STATISTIC_NUM_SLOTS as usize],
    pub numnumbers: [c_int; STATISTIC_NUM_SLOTS as usize],
    pub stanumbers: [*mut float4; STATISTIC_NUM_SLOTS as usize],
    pub numvalues: [c_int; STATISTIC_NUM_SLOTS as usize],
    pub stavalues: [*mut Datum; STATISTIC_NUM_SLOTS as usize],
    pub statypid: [Oid; STATISTIC_NUM_SLOTS as usize],
    pub statyplen: [int16; STATISTIC_NUM_SLOTS as usize],
    pub statypbyval: [bool; STATISTIC_NUM_SLOTS as usize],
    pub statypalign: [c_char; STATISTIC_NUM_SLOTS as usize],
}

pub type AnalyzeAttrComputeStatsFunc =
    Option<unsafe extern "C" fn(stats: *mut VacAttrStats, fetchfunc: AnalyzeAttrFetchFunc, samplerows: c_int, totalrows: f64)>;
pub type AnalyzeAttrFetchFunc =
    Option<unsafe extern "C" fn(stats: *mut VacAttrStats, rownum: c_int, isNull: *mut bool) -> Datum>;

#[repr(C)]
pub struct TypeCacheEntry {
    pub type_id: Oid,
    pub typlen: int16,
    pub typbyval: bool,
    pub typalign: c_char,
    pub eq_opr: Oid,
    pub cmp_proc_finfo: FmgrInfo,
    pub hash_proc_finfo: FmgrInfo,
}

#[repr(C)]
pub struct FmgrInfo {
    pub fn_oid: Oid,
}

#[repr(C)]
pub struct ArrayType {
    _private: [u8; 0],
}

pub enum HTAB {}

#[repr(C)]
pub struct HASHCTL {
    pub keysize: Size,
    pub entrysize: Size,
    pub hash: HashValueFunc,
    pub match_: HashCompareFunc,
    pub hcxt: MemoryContext,
}

pub type HashValueFunc = Option<unsafe extern "C" fn(key: *const c_void, keysize: Size) -> uint32>;
pub type HashCompareFunc = Option<unsafe extern "C" fn(key1: *const c_void, key2: *const c_void, keysize: Size) -> c_int>;

#[repr(C)]
pub struct HASH_SEQ_STATUS {
    pub hashp: *mut HTAB,
    pub curBucket: u32,
    pub curEntry: *mut c_void,
}

#[repr(C)]
#[allow(dead_code)]
pub enum HASHACTION {
    HASH_FIND = 0,
    HASH_ENTER,
    HASH_REMOVE,
    HASH_ENTER_NULL,
}
pub use HASHACTION::*;

pub type qsort_arg_comparator =
    Option<unsafe extern "C" fn(a: *const c_void, b: *const c_void, arg: *mut c_void) -> c_int>;

/* hash flag bits */
pub const HASH_ELEM: c_int = 0x0008;
pub const HASH_BLOBS: c_int = 0x0010;
pub const HASH_FUNCTION: c_int = 0x0040;
pub const HASH_COMPARE: c_int = 0x0080;
pub const HASH_CONTEXT: c_int = 0x0100;

/* typcache flags */
pub const TYPECACHE_EQ_OPR: c_int = 0x00002;
pub const TYPECACHE_CMP_PROC_FINFO: c_int = 0x00800;
pub const TYPECACHE_HASH_PROC_FINFO: c_int = 0x01000;

/* pg_statistic */
pub const STATISTIC_NUM_SLOTS: c_int = 5;
pub const STATISTIC_KIND_MCELEM: c_int = 4;
pub const STATISTIC_KIND_DECHIST: c_int = 5;

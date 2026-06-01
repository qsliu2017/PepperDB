//! ts_typanalyze.rs
//!   functions for gathering statistics from tsvector columns
//!
//! Translated 1:1 from postgres/src/backend/tsearch/ts_typanalyze.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//!
//!
//! IDENTIFICATION
//!   src/backend/tsearch/ts_typanalyze.c

#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_POINTER, PG_RETURN_BOOL};

use std::ffi::{c_char, c_int, c_void};

use crate::common::hashfn::hash_any;
use crate::varatt::VARSIZE_ANY;
use crate::utils::builtins::cstring_to_text_with_len;
use crate::utils::sort::qsort_interruptible::qsort_interruptible;
use crate::port::qsort::qsort_arg_comparator;
use crate::catalog::pg_statistic::STATISTIC_KIND_MCELEM;
use crate::catalog::pg_type_d::TEXTOID;
use crate::catalog::pg_known_oids::{DEFAULT_COLLATION_OID, TextEqualOperator};
use crate::utils::adt::ts_type::{
    TSVector, WordEntry, STRPTR, ARRPTR, DatumGetTSVector, TSVectorGetDatum,
};
use crate::utils::hash::dynahash::{
    HTAB, HASHCTL, HASH_SEQ_STATUS, hash_create, hash_search, hash_seq_init,
    hash_seq_search, hash_get_num_entries,
    HASH_ELEM, HASH_FUNCTION, HASH_COMPARE, HASH_CONTEXT,
};
use crate::utils::hash::dynahash::HASHACTION::{HASH_ENTER, HASH_REMOVE};

/* A hash key for lexemes */
#[repr(C)]
pub struct LexemeHashKey {
    pub lexeme: *mut c_char,    /* lexeme (not NULL terminated!) */
    pub length: c_int,          /* its length in bytes */
}

/* A hash table entry for the Lossy Counting algorithm */
#[repr(C)]
pub struct TrackItem {
    pub key: LexemeHashKey,     /* This is 'e' from the LC algorithm. */
    pub frequency: c_int,       /* This is 'f'. */
    pub delta: c_int,           /* And this is 'delta'. */
}

/*
 *	ts_typanalyze -- a custom typanalyze function for tsvector columns
 */
pub unsafe fn ts_typanalyze(fcinfo: FunctionCallInfo) -> Datum {
    let stats: *mut VacAttrStats = PG_GETARG_POINTER!(fcinfo, 0) as *mut VacAttrStats;

    /* If the attstattarget column is negative, use the default value */
    if (*stats).attstattarget < 0 {
        (*stats).attstattarget = default_statistics_target;
    }

    (*stats).compute_stats = Some(compute_tsvector_stats);
    /* see comment about the choice of minrows in commands/analyze.c */
    (*stats).minrows = 300 * (*stats).attstattarget;

    PG_RETURN_BOOL!(true)
}

/*
 *	compute_tsvector_stats() -- compute statistics for a tsvector column
 *
 *	This functions computes statistics that are useful for determining @@
 *	operations' selectivity, along with the fraction of non-null rows and
 *	average width.
 *
 *	Instead of finding the most common values, as we do for most datatypes,
 *	we're looking for the most common lexemes. This is more useful, because
 *	there most probably won't be any two rows with the same tsvector and thus
 *	the notion of a MCV is a bit bogus with this datatype. With a list of the
 *	most common lexemes we can do a better job at figuring out @@ selectivity.
 *
 *	For the same reasons we assume that tsvector columns are unique when
 *	determining the number of distinct values.
 *
 *	The algorithm used is Lossy Counting, as proposed in the paper "Approximate
 *	frequency counts over data streams" by G. S. Manku and R. Motwani, in
 *	Proceedings of the 28th International Conference on Very Large Data Bases,
 *	Hong Kong, China, August 2002, section 4.2. The paper is available at
 *	http://www.vldb.org/conf/2002/S10P03.pdf
 *
 *	The Lossy Counting (aka LC) algorithm goes like this:
 *	Let s be the threshold frequency for an item (the minimum frequency we
 *	are interested in) and epsilon the error margin for the frequency. Let D
 *	be a set of triples (e, f, delta), where e is an element value, f is that
 *	element's frequency (actually, its current occurrence count) and delta is
 *	the maximum error in f. We start with D empty and process the elements in
 *	batches of size w. (The batch size is also known as "bucket size" and is
 *	equal to 1/epsilon.) Let the current batch number be b_current, starting
 *	with 1. For each element e we either increment its f count, if it's
 *	already in D, or insert a new triple into D with values (e, 1, b_current
 *	- 1). After processing each batch we prune D, by removing from it all
 *	elements with f + delta <= b_current.  After the algorithm finishes we
 *	suppress all elements from D that do not satisfy f >= (s - epsilon) * N,
 *	where N is the total number of elements in the input.  We emit the
 *	remaining elements with estimated frequency f/N.  The LC paper proves
 *	that this algorithm finds all elements with true frequency at least s,
 *	and that no frequency is overestimated or is underestimated by more than
 *	epsilon.  Furthermore, given reasonable assumptions about the input
 *	distribution, the required table size is no more than about 7 times w.
 *
 *	We set s to be the estimated frequency of the K'th word in a natural
 *	language's frequency table, where K is the target number of entries in
 *	the MCELEM array plus an arbitrary constant, meant to reflect the fact
 *	that the most common words in any language would usually be stopwords
 *	so we will not actually see them in the input.  We assume that the
 *	distribution of word frequencies (including the stopwords) follows Zipf's
 *	law with an exponent of 1.
 *
 *	Assuming Zipfian distribution, the frequency of the K'th word is equal
 *	to 1/(K * H(W)) where H(n) is 1/2 + 1/3 + ... + 1/n and W is the number of
 *	words in the language.  Putting W as one million, we get roughly 0.07/K.
 *	Assuming top 10 words are stopwords gives s = 0.07/(K + 10).  We set
 *	epsilon = s/10, which gives bucket width w = (K + 10)/0.007 and
 *	maximum expected hashtable size of about 1000 * (K + 10).
 *
 *	Note: in the above discussion, s, epsilon, and f/N are in terms of a
 *	lexeme's frequency as a fraction of all lexemes seen in the input.
 *	However, what we actually want to store in the finished pg_statistic
 *	entry is each lexeme's frequency as a fraction of all rows that it occurs
 *	in.  Assuming that the input tsvectors are correctly constructed, no
 *	lexeme occurs more than once per tsvector, so the final count f is a
 *	correct estimate of the number of input tsvectors it occurs in, and we
 *	need only change the divisor from N to nonnull_cnt to get the number we
 *	want.
 */
unsafe fn compute_tsvector_stats(
    stats: *mut VacAttrStats,
    fetchfunc: AnalyzeAttrFetchFunc,
    samplerows: c_int,
    totalrows: f64,
) {
    let mut num_mcelem: c_int;
    let mut null_cnt: c_int = 0;
    let mut total_width: f64 = 0.0;

    /* This is D from the LC algorithm. */
    let lexemes_tab: *mut HTAB;
    let mut hash_ctl: HASHCTL = std::mem::zeroed();
    let mut scan_status: HASH_SEQ_STATUS = std::mem::zeroed();

    /* This is the current bucket number from the LC algorithm */
    let mut b_current: c_int;

    /* This is 'w' from the LC algorithm */
    let bucket_width: c_int;
    let mut vector_no: c_int;
    let mut lexeme_no: c_int;
    let mut hash_key: LexemeHashKey = std::mem::zeroed();

    /*
     * We want statistics_target * 10 lexemes in the MCELEM array.  This
     * multiplier is pretty arbitrary, but is meant to reflect the fact that
     * the number of individual lexeme values tracked in pg_statistic ought to
     * be more than the number of values for a simple scalar column.
     */
    num_mcelem = (*stats).attstattarget * 10;

    /*
     * We set bucket width equal to (num_mcelem + 10) / 0.007 as per the
     * comment above.
     */
    bucket_width = (num_mcelem + 10) * 1000 / 7;

    /*
     * Create the hashtable. It will be in local memory, so we don't need to
     * worry about overflowing the initial size. Also we don't need to pay any
     * attention to locking and memory management.
     */
    hash_ctl.keysize = std::mem::size_of::<LexemeHashKey>();
    hash_ctl.entrysize = std::mem::size_of::<TrackItem>();
    hash_ctl.hash = Some(lexeme_hash);
    hash_ctl.r#match = Some(lexeme_match);
    hash_ctl.hcxt = CurrentMemoryContext;
    lexemes_tab = hash_create(c"Analyzed lexemes table".as_ptr(),
                              num_mcelem as c_long,
                              &hash_ctl,
                              HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

    /* Initialize counters. */
    b_current = 1;
    lexeme_no = 0;

    /* Loop over the tsvectors. */
    vector_no = 0;
    while vector_no < samplerows {
        let value: Datum;
        let mut isnull: bool = false;
        let vector: TSVector;
        let mut curentryptr: *mut WordEntry;
        let lexemesptr: *mut c_char;
        let mut j: c_int;

        vacuum_delay_point(true);

        value = (fetchfunc.unwrap())(stats, vector_no, &mut isnull);

        /*
         * Check for null/nonnull.
         */
        if isnull {
            null_cnt += 1;
            vector_no += 1;
            continue;
        }

        /*
         * Add up widths for average-width calculation.  Since it's a
         * tsvector, we know it's varlena.  As in the regular
         * compute_minimal_stats function, we use the toasted width for this
         * calculation.
         */
        total_width += VARSIZE_ANY(DatumGetPointer(value)) as f64;

        /*
         * Now detoast the tsvector if needed.
         */
        vector = DatumGetTSVector(value);

        /*
         * We loop through the lexemes in the tsvector and add them to our
         * tracking hashtable.
         */
        lexemesptr = STRPTR(vector);
        curentryptr = ARRPTR(vector);
        j = 0;
        while j < (*vector).size {
            let item: *mut TrackItem;
            let mut found: bool = false;

            /*
             * Construct a hash key.  The key points into the (detoasted)
             * tsvector value at this point, but if a new entry is created, we
             * make a copy of it.  This way we can free the tsvector value
             * once we've processed all its lexemes.
             */
            hash_key.lexeme = lexemesptr.add((*curentryptr).pos() as usize);
            hash_key.length = (*curentryptr).len() as c_int;

            /* Lookup current lexeme in hashtable, adding it if new */
            item = hash_search(lexemes_tab,
                               &hash_key as *const LexemeHashKey as *const c_void,
                               HASH_ENTER, &mut found) as *mut TrackItem;

            if found {
                /* The lexeme is already on the tracking list */
                (*item).frequency += 1;
            } else {
                /* Initialize new tracking list element */
                (*item).frequency = 1;
                (*item).delta = b_current - 1;

                (*item).key.lexeme = palloc(hash_key.length as Size) as *mut c_char;
                std::ptr::copy_nonoverlapping(hash_key.lexeme, (*item).key.lexeme,
                                              hash_key.length as usize);
            }

            /* lexeme_no is the number of elements processed (ie N) */
            lexeme_no += 1;

            /* We prune the D structure after processing each bucket */
            if lexeme_no % bucket_width == 0 {
                prune_lexemes_hashtable(lexemes_tab, b_current);
                b_current += 1;
            }

            /* Advance to the next WordEntry in the tsvector */
            curentryptr = curentryptr.add(1);

            j += 1;
        }

        /* If the vector was toasted, free the detoasted copy. */
        if TSVectorGetDatum(vector) != value {
            pfree(vector as *mut c_void);
        }

        vector_no += 1;
    }

    /* We can only compute real stats if we found some non-null values. */
    if null_cnt < samplerows {
        let nonnull_cnt: c_int = samplerows - null_cnt;
        let mut i: c_int;
        let sort_table: *mut *mut TrackItem;
        let mut item: *mut TrackItem;
        let mut track_len: c_int;
        let cutoff_freq: c_int;
        let mut minfreq: c_int;
        let mut maxfreq: c_int;

        (*stats).stats_valid = true;
        /* Do the simple null-frac and average width stats */
        (*stats).stanullfrac = null_cnt as f64 / samplerows as f64;
        (*stats).stawidth = (total_width / nonnull_cnt as f64) as int32;

        /* Assume it's a unique column (see notes above) */
        (*stats).stadistinct = -1.0 * (1.0 - (*stats).stanullfrac);

        /*
         * Construct an array of the interesting hashtable items, that is,
         * those meeting the cutoff frequency (s - epsilon)*N.  Also identify
         * the minimum and maximum frequencies among these items.
         *
         * Since epsilon = s/10 and bucket_width = 1/epsilon, the cutoff
         * frequency is 9*N / bucket_width.
         */
        cutoff_freq = 9 * lexeme_no / bucket_width;

        i = hash_get_num_entries(lexemes_tab) as c_int;	/* surely enough space */
        sort_table = palloc(std::mem::size_of::<*mut TrackItem>() * i as usize) as *mut *mut TrackItem;

        hash_seq_init(&mut scan_status, lexemes_tab);
        track_len = 0;
        minfreq = lexeme_no;
        maxfreq = 0;
        loop {
            item = hash_seq_search(&mut scan_status) as *mut TrackItem;
            if item.is_null() {
                break;
            }
            if (*item).frequency > cutoff_freq {
                *sort_table.add(track_len as usize) = item;
                track_len += 1;
                minfreq = Min(minfreq, (*item).frequency);
                maxfreq = Max(maxfreq, (*item).frequency);
            }
        }
        Assert!(track_len <= i);

        /* emit some statistics for debug purposes */
        elog!(DEBUG3, "tsvector_stats: target # mces = {}, bucket width = {}, # lexemes = {}, hashtable size = {}, usable entries = {}",
              num_mcelem, bucket_width, lexeme_no, i, track_len);

        /*
         * If we obtained more lexemes than we really want, get rid of those
         * with least frequencies.  The easiest way is to qsort the array into
         * descending frequency order and truncate the array.
         */
        if num_mcelem < track_len {
            qsort_interruptible(sort_table as *mut c_void, track_len as usize,
                                std::mem::size_of::<*mut TrackItem>(),
                                trackitem_compare_frequencies_desc, std::ptr::null_mut());
            /* reset minfreq to the smallest frequency we're keeping */
            minfreq = (**sort_table.add((num_mcelem - 1) as usize)).frequency;
        } else {
            num_mcelem = track_len;
        }

        /* Generate MCELEM slot entry */
        if num_mcelem > 0 {
            let old_context: MemoryContext;
            let mcelem_values: *mut Datum;
            let mcelem_freqs: *mut float4;

            /*
             * We want to store statistics sorted on the lexeme value using
             * first length, then byte-for-byte comparison. The reason for
             * doing length comparison first is that we don't care about the
             * ordering so long as it's consistent, and comparing lengths
             * first gives us a chance to avoid a strncmp() call.
             *
             * This is different from what we do with scalar statistics --
             * they get sorted on frequencies. The rationale is that we
             * usually search through most common elements looking for a
             * specific value, so we can grab its frequency.  When values are
             * presorted we can employ binary search for that.  See
             * ts_selfuncs.c for a real usage scenario.
             */
            qsort_interruptible(sort_table as *mut c_void, num_mcelem as usize,
                                std::mem::size_of::<*mut TrackItem>(),
                                trackitem_compare_lexemes, std::ptr::null_mut());

            /* Must copy the target values into anl_context */
            old_context = MemoryContextSwitchTo((*stats).anl_context);

            /*
             * We sorted statistics on the lexeme value, but we want to be
             * able to find out the minimal and maximal frequency without
             * going through all the values.  We keep those two extra
             * frequencies in two extra cells in mcelem_freqs.
             *
             * (Note: the MCELEM statistics slot definition allows for a third
             * extra number containing the frequency of nulls, but we don't
             * create that for a tsvector column, since null elements aren't
             * possible.)
             */
            mcelem_values = palloc(num_mcelem as usize * std::mem::size_of::<Datum>()) as *mut Datum;
            mcelem_freqs = palloc((num_mcelem + 2) as usize * std::mem::size_of::<float4>()) as *mut float4;

            /*
             * See comments above about use of nonnull_cnt as the divisor for
             * the final frequency estimates.
             */
            i = 0;
            while i < num_mcelem {
                let titem: *mut TrackItem = *sort_table.add(i as usize);

                *mcelem_values.add(i as usize) =
                    PointerGetDatum(cstring_to_text_with_len((*titem).key.lexeme,
                                                             (*titem).key.length) as *const c_void);
                *mcelem_freqs.add(i as usize) = ((*titem).frequency as f64 / nonnull_cnt as f64) as float4;

                i += 1;
            }
            *mcelem_freqs.add(i as usize) = (minfreq as f64 / nonnull_cnt as f64) as float4;
            i += 1;
            *mcelem_freqs.add(i as usize) = (maxfreq as f64 / nonnull_cnt as f64) as float4;
            MemoryContextSwitchTo(old_context);

            (*stats).stakind[0] = STATISTIC_KIND_MCELEM;
            (*stats).staop[0] = TextEqualOperator;
            (*stats).stacoll[0] = DEFAULT_COLLATION_OID;
            (*stats).stanumbers[0] = mcelem_freqs;
            /* See above comment about two extra frequency fields */
            (*stats).numnumbers[0] = num_mcelem + 2;
            (*stats).stavalues[0] = mcelem_values;
            (*stats).numvalues[0] = num_mcelem;
            /* We are storing text values */
            (*stats).statypid[0] = TEXTOID;
            (*stats).statyplen[0] = -1;	/* typlen, -1 for varlena */
            (*stats).statypbyval[0] = false;
            (*stats).statypalign[0] = b'i' as c_char;
        }
    } else {
        /* We found only nulls; assume the column is entirely null */
        (*stats).stats_valid = true;
        (*stats).stanullfrac = 1.0;
        (*stats).stawidth = 0;	/* "unknown" */
        (*stats).stadistinct = 0.0;	/* "unknown" */
    }

    /*
     * We don't need to bother cleaning up any of our temporary palloc's. The
     * hashtable should also go away, as it used a child memory context.
     */
}

/*
 *	A function to prune the D structure from the Lossy Counting algorithm.
 *	Consult compute_tsvector_stats() for wider explanation.
 */
unsafe fn prune_lexemes_hashtable(lexemes_tab: *mut HTAB, b_current: c_int) {
    let mut scan_status: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut item: *mut TrackItem;

    hash_seq_init(&mut scan_status, lexemes_tab);
    loop {
        item = hash_seq_search(&mut scan_status) as *mut TrackItem;
        if item.is_null() {
            break;
        }
        if (*item).frequency + (*item).delta <= b_current {
            let lexeme: *mut c_char = (*item).key.lexeme;

            if hash_search(lexemes_tab, &mut (*item).key as *mut LexemeHashKey as *const c_void,
                           HASH_REMOVE, std::ptr::null_mut()).is_null()
            {
                elog!(ERROR, "hash table corrupted");
            }
            pfree(lexeme as *mut c_void);
        }
    }
}

/*
 * Hash functions for lexemes. They are strings, but not NULL terminated,
 * so we need a special hash function.
 */
unsafe extern "C" fn lexeme_hash(key: *const c_void, keysize: Size) -> uint32 {
    let l: *const LexemeHashKey = key as *const LexemeHashKey;

    DatumGetUInt32(hash_any((*l).lexeme as *const core::ffi::c_uchar,
                            (*l).length))
}

/*
 *	Matching function for lexemes, to be used in hashtable lookups.
 */
unsafe extern "C" fn lexeme_match(key1: *const c_void, key2: *const c_void, keysize: Size) -> c_int {
    /* The keysize parameter is superfluous, the keys store their lengths */
    lexeme_compare(key1, key2)
}

/*
 *	Comparison function for lexemes.
 */
unsafe fn lexeme_compare(key1: *const c_void, key2: *const c_void) -> c_int {
    let d1: *const LexemeHashKey = key1 as *const LexemeHashKey;
    let d2: *const LexemeHashKey = key2 as *const LexemeHashKey;

    /* First, compare by length */
    if (*d1).length > (*d2).length {
        return 1;
    } else if (*d1).length < (*d2).length {
        return -1;
    }
    /* Lengths are equal, do a byte-by-byte comparison */
    strncmp((*d1).lexeme, (*d2).lexeme, (*d1).length as usize)
}

/*
 *	Comparator for sorting TrackItems on frequencies (descending sort)
 */
unsafe fn trackitem_compare_frequencies_desc(e1: *const c_void, e2: *const c_void, arg: *mut c_void) -> c_int {
    let t1: *const *const TrackItem = e1 as *const *const TrackItem;
    let t2: *const *const TrackItem = e2 as *const *const TrackItem;

    (**t2).frequency - (**t1).frequency
}

/*
 *	Comparator for sorting TrackItems on lexemes
 */
unsafe fn trackitem_compare_lexemes(e1: *const c_void, e2: *const c_void, arg: *mut c_void) -> c_int {
    let t1: *const *const TrackItem = e1 as *const *const TrackItem;
    let t2: *const *const TrackItem = e2 as *const *const TrackItem;

    lexeme_compare(&(**t1).key as *const LexemeHashKey as *const c_void,
                   &(**t2).key as *const LexemeHashKey as *const c_void)
}

/* ---- Local helpers ---- */

/*
 * strncmp() semantics: compare up to n bytes, returning sign of the first
 * differing byte (unsigned char comparison), or 0 if equal over n bytes.
 */
unsafe fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int {
    let mut i: usize = 0;
    while i < n {
        let c1 = *s1.add(i) as u8;
        let c2 = *s2.add(i) as u8;
        if c1 != c2 {
            return c1 as c_int - c2 as c_int;
        }
        i += 1;
    }
    0
}

/* ---- Local stubs for unported dependencies ---- */

// GUC default_statistics_target (utils/misc/guc_tables.c).
static mut default_statistics_target: c_int = 100; // TODO(pg-port): real default_statistics_target lives in utils/misc/guc.c

unsafe fn vacuum_delay_point(_is_analyze: bool) { /* TODO(pg-port): real vacuum_delay_point lives in commands/vacuum.rs */ }

/* ---- Local stub types for unported dependencies ---- */
/* TODO(pg-port): real VacAttrStats/AnalyzeAttrFetchFunc live in commands/vacuum.rs */

#[repr(C)]
pub struct VacAttrStats {
    pub attrtypid: Oid,
    pub attrcollid: Oid,
    pub attstattarget: c_int,
    pub minrows: c_int,
    pub anl_context: MemoryContext,
    pub compute_stats: AnalyzeAttrComputeStatsFunc,
    pub extra_data: *mut c_void,
    pub stats_valid: bool,
    pub stanullfrac: f64,
    pub stawidth: int32,
    pub stadistinct: f64,
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
    Option<unsafe fn(stats: *mut VacAttrStats, fetchfunc: AnalyzeAttrFetchFunc, samplerows: c_int, totalrows: f64)>;
pub type AnalyzeAttrFetchFunc =
    Option<unsafe fn(stats: *mut VacAttrStats, rownum: c_int, isNull: *mut bool) -> Datum>;

const STATISTIC_NUM_SLOTS: c_int = 5;

//! Translation of postgres/src/backend/access/tablesample/bernoulli.c
//!
//! Support routines for the BERNOULLI tablesample method.
//!
//! To ensure repeatability of samples, it is necessary that selection of a
//! given tuple be history-independent; otherwise syncscanning would break
//! repeatability. To achieve that, we proceed by hashing each candidate TID
//! together with the active seed, and then selecting it if the hash is less
//! than the cutoff value computed from the selection probability by
//! BeginSampleScan.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! #include mapping:
//!   - "postgres.h"             -> crate::prelude::* (REAL)
//!   - <math.h> (rint)          -> extern "C" libm rint (REAL)
//!   - "access/tsmapi.h"        -> crate::access::tsmapi::* (REAL)
//!   - "catalog/pg_type.h"      -> crate::catalog::pg_type_d::FLOAT4OID (REAL)
//!   - "common/hashfn.h"        -> crate::common::hashfn::hash_any (REAL)
//!   - "optimizer/optimizer.h"  -> estimate_expression_value / clamp_row_est:
//!                                 UNPORTED, stubbed (see notes in the fn).
//!   - "utils/fmgrprotos.h"     -> fmgr V1 plumbing via crate::utils::fmgr.

use crate::prelude::*;
use crate::{PG_RETURN_POINTER, list_make1_oid};

use crate::access::tsmapi::TsmRoutine;
use crate::common::hashfn::hash_any;
use crate::nodes::execnodes::SampleScanState;
use crate::storage::block::BlockNumber;
use crate::storage::off::{InvalidOffsetNumber, FirstOffsetNumber, OffsetNumber};
use crate::utils::fmgr::FunctionCallInfo;

// <math.h>: rint() (round to nearest, ties to even) -- bind libm directly so the
// cutoff arithmetic is bit-identical to the C source.
extern "C" {
    fn rint(x: f64) -> f64;
}

// errcode for the "sample percentage" error. The errcode() shim ignores the
// value; ERRCODE_INVALID_TABLESAMPLE_ARGUMENT is not yet defined in this tree,
// so we keep a local 0 placeholder to mirror the C call site. (STUB code)
const ERRCODE_INVALID_TABLESAMPLE_ARGUMENT: c_int = 0;

/* Private state */
#[repr(C)]
#[derive(Clone, Copy)]
struct BernoulliSamplerData {
    cutoff: uint64,       /* select tuples with hash less than this */
    seed: uint32,         /* random seed */
    lt: OffsetNumber,     /* last tuple returned from current block */
}

/*
 * Create a TsmRoutine descriptor for the BERNOULLI method.
 */
pub unsafe fn tsm_bernoulli_handler(_fcinfo: FunctionCallInfo) -> Datum {
    // C: TsmRoutine *tsm = makeNode(TsmRoutine);
    // TODO: T_TsmRoutine is not a NodeTag variant yet, so we palloc0 the struct
    // and leave the node tag at 0 (T_Invalid) instead of makeNode(TsmRoutine).
    let tsm = palloc0(core::mem::size_of::<TsmRoutine>()) as *mut TsmRoutine;

    // C: tsm->parameterTypes = list_make1_oid(FLOAT4OID);
    (*tsm).parameterTypes = list_make1_oid!(crate::catalog::pg_type_d::FLOAT4OID);
    (*tsm).repeatable_across_queries = true;
    (*tsm).repeatable_across_scans = true;
    (*tsm).SampleScanGetSampleSize = Some(bernoulli_samplescangetsamplesize);
    (*tsm).InitSampleScan = Some(bernoulli_initsamplescan);
    (*tsm).BeginSampleScan = Some(bernoulli_beginsamplescan);
    (*tsm).NextSampleBlock = None;
    (*tsm).NextSampleTuple = Some(bernoulli_nextsampletuple);
    (*tsm).EndSampleScan = None;

    PG_RETURN_POINTER!(tsm);
}

/*
 * Sample size estimation.
 *
 * STUB: estimate_expression_value() and clamp_row_est() are unported (planner
 * code, optimizer/optimizer.h), and RelOptInfo (baserel->pages/->tuples) is not
 * yet a ported type. So this keeps the REAL samplefract math but:
 *   - reads paramexprs[0] only as a presence check (linitial),
 *   - cannot fold a Const, so it always takes the 0.1f default branch,
 *   - treats the pages and tuples out-params as TODO since baserel is *mut c_void.
 */
unsafe fn bernoulli_samplescangetsamplesize(
    _root: *mut c_void,    /* PlannerInfo * */
    _baserel: *mut c_void, /* RelOptInfo * */
    _paramexprs: *mut crate::nodes::pg_list::List,
    pages: *mut BlockNumber,
    tuples: *mut f64,
) {
    // C extracts and folds linitial(paramexprs) via estimate_expression_value;
    // both are unported. Default samplefract per the C "bogus / non-Const" path.
    let samplefract: f32 = 0.1f32;

    // C: *pages = baserel->pages;  *tuples = clamp_row_est(baserel->tuples * samplefract);
    // TODO: RelOptInfo unported -> cannot read baserel->pages / baserel->tuples,
    // and clamp_row_est is unported (stubbed as identity below). Write zeros.
    let _ = samplefract;
    if !pages.is_null() {
        *pages = 0;
    }
    if !tuples.is_null() {
        // clamp_row_est(x) stub -> identity. Real input would be
        // baserel->tuples * samplefract.
        *tuples = clamp_row_est(0.0 * samplefract as f64);
    }
}

// STUB: clamp_row_est (optimizer/optimizer.h). Real version clamps to >= 1 and
// rounds; here it is identity until the planner is ported.
#[inline]
unsafe fn clamp_row_est(nrows: f64) -> f64 {
    // TODO: port real clamp_row_est (clamp to [1, MAXINT], rint).
    nrows
}

/*
 * Initialize during executor setup.
 */
unsafe fn bernoulli_initsamplescan(node: *mut SampleScanState, _eflags: c_int) {
    (*node).tsm_state = palloc0(core::mem::size_of::<BernoulliSamplerData>()) as *mut c_void;
}

/*
 * Examine parameters and prepare for a sample scan.
 */
unsafe fn bernoulli_beginsamplescan(
    node: *mut SampleScanState,
    params: *mut Datum,
    _nparams: c_int,
    seed: uint32,
) {
    let sampler = (*node).tsm_state as *mut BernoulliSamplerData;
    let percent: f64 = DatumGetFloat4(*params.add(0)) as f64;

    if percent < 0.0 || percent > 100.0 || percent.is_nan() {
        // C: ereport(ERROR, (errcode(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT),
        //                    errmsg("sample percentage must be between 0 and 100")));
        let _ = errcode(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT);
        ereport!(
            ERROR,
            errmsg!("sample percentage must be between 0 and 100")
        );
    }

    /*
     * The cutoff is sample probability times (PG_UINT32_MAX + 1); we have to
     * store that as a uint64, of course. Note that this gives strictly correct
     * behavior at the limits of zero or one probability.
     */
    let dcutoff: f64 = rint(((PG_UINT32_MAX as f64) + 1.0) * percent / 100.0);
    (*sampler).cutoff = dcutoff as uint64;
    (*sampler).seed = seed;
    (*sampler).lt = InvalidOffsetNumber;

    /*
     * Use bulkread, since we're scanning all pages. But pagemode visibility
     * checking is a win only at larger sampling fractions. The 25% cutoff here
     * is based on very limited experimentation.
     */
    (*node).use_bulkread = true;
    (*node).use_pagemode = percent >= 25.0;
}

/*
 * Select next sampled tuple in current block.
 *
 * It is OK here to return an offset without knowing if the tuple is visible
 * (or even exists). The reason is that we do the coinflip for every tuple
 * offset in the table. Since all tuples have the same probability of being
 * returned, it doesn't matter if we do extra coinflips for invisible tuples.
 *
 * When we reach end of the block, return InvalidOffsetNumber which tells
 * SampleScan to go to next block.
 */
unsafe fn bernoulli_nextsampletuple(
    node: *mut SampleScanState,
    blockno: BlockNumber,
    maxoffset: OffsetNumber,
) -> OffsetNumber {
    let sampler = (*node).tsm_state as *mut BernoulliSamplerData;
    let mut tupoffset: OffsetNumber = (*sampler).lt;
    let mut hashinput: [uint32; 3] = [0; 3];

    /* Advance to first/next tuple in block */
    if tupoffset == InvalidOffsetNumber {
        tupoffset = FirstOffsetNumber;
    } else {
        tupoffset += 1;
    }

    /*
     * We compute the hash by applying hash_any to an array of 3 uint32's
     * containing the block, offset, and seed.
     *
     * These words in the hash input are the same throughout the block:
     */
    hashinput[0] = blockno;
    hashinput[2] = (*sampler).seed;

    /*
     * Loop over tuple offsets until finding suitable TID or reaching end of
     * block.
     */
    while tupoffset <= maxoffset {
        hashinput[1] = tupoffset as uint32;

        let hash: uint32 = DatumGetUInt32(hash_any(
            hashinput.as_ptr() as *const c_uchar,
            core::mem::size_of::<[uint32; 3]>() as c_int,
        ));
        if (hash as uint64) < (*sampler).cutoff {
            break;
        }
        tupoffset += 1;
    }

    if tupoffset > maxoffset {
        tupoffset = InvalidOffsetNumber;
    }

    (*sampler).lt = tupoffset;

    tupoffset
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build a minimal SampleScanState carrying a BernoulliSamplerData in
    // tsm_state. We zero the whole node and only set the fields the sampling
    // method touches (tsm_state).
    unsafe fn make_state(sampler: *mut BernoulliSamplerData) -> *mut SampleScanState {
        let node = Box::into_raw(Box::new(
            core::mem::MaybeUninit::<SampleScanState>::zeroed().assume_init(),
        ));
        (*node).tsm_state = sampler as *mut c_void;
        node
    }

    // Seed a sampler the way beginsamplescan would for a given percent/seed.
    unsafe fn seed_sampler(percent: f64, seed: uint32) -> BernoulliSamplerData {
        let dcutoff = rint(((PG_UINT32_MAX as f64) + 1.0) * percent / 100.0);
        BernoulliSamplerData {
            cutoff: dcutoff as uint64,
            seed,
            lt: InvalidOffsetNumber,
        }
    }

    // Collect the offsets bernoulli selects for one block of `maxoffset` tuples.
    unsafe fn run_block(percent: f64, seed: uint32, maxoffset: OffsetNumber) -> Vec<OffsetNumber> {
        let mut sampler = seed_sampler(percent, seed);
        let node = make_state(&mut sampler as *mut BernoulliSamplerData);
        let mut out = Vec::new();
        loop {
            let off = bernoulli_nextsampletuple(node, 0, maxoffset);
            if off == InvalidOffsetNumber {
                break;
            }
            out.push(off);
        }
        drop(Box::from_raw(node));
        out
    }

    #[test]
    fn offsets_in_range_and_monotonic() {
        unsafe {
            let maxoffset: OffsetNumber = 100;
            let seq = run_block(50.0, 12345, maxoffset);
            // Every returned offset must be a valid tuple offset in this block.
            for (i, &off) in seq.iter().enumerate() {
                assert!(off >= FirstOffsetNumber && off <= maxoffset);
                if i > 0 {
                    // bernoulli advances strictly forward through offsets.
                    assert!(off > seq[i - 1]);
                }
            }
        }
    }

    #[test]
    fn deterministic_for_fixed_seed() {
        unsafe {
            let a = run_block(37.5, 0xC0FFEE, 250);
            let b = run_block(37.5, 0xC0FFEE, 250);
            assert_eq!(a, b);
        }
    }

    #[test]
    fn full_and_zero_probability_limits() {
        unsafe {
            // 100% selects every offset; 0% selects none.
            let all = run_block(100.0, 1, 64);
            assert_eq!(all.len(), 64);
            assert_eq!(all.first().copied(), Some(FirstOffsetNumber));
            assert_eq!(all.last().copied(), Some(64));

            let none = run_block(0.0, 1, 64);
            assert!(none.is_empty());
        }
    }
}

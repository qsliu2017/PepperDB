//! Translation of postgres/src/backend/access/tablesample/system.c
//!
//! Support routines for the SYSTEM tablesample method.
//!
//! To ensure repeatability of samples, it is necessary that selection of a
//! given tuple be history-independent. To achieve that, we proceed by hashing
//! each candidate block number together with the active seed, and then
//! selecting it if the hash is less than the cutoff value computed from the
//! selection probability by BeginSampleScan.
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
use crate::storage::block::{BlockNumber, InvalidBlockNumber};
use crate::storage::off::{InvalidOffsetNumber, FirstOffsetNumber, OffsetNumber};
use crate::utils::fmgr::FunctionCallInfo;

// <math.h>: rint() -- bind libm directly so cutoff arithmetic is bit-identical.
extern "C" {
    fn rint(x: f64) -> f64;
}

// errcode for the "sample percentage" error; see note in bernoulli.rs. (STUB)
const ERRCODE_INVALID_TABLESAMPLE_ARGUMENT: c_int = 0;

/* Private state */
#[repr(C)]
#[derive(Clone, Copy)]
struct SystemSamplerData {
    cutoff: uint64,        /* select blocks with hash less than this */
    seed: uint32,          /* random seed */
    nextblock: BlockNumber, /* next block to consider sampling */
    lt: OffsetNumber,      /* last tuple returned from current block */
}

/*
 * Create a TsmRoutine descriptor for the SYSTEM method.
 */
pub unsafe fn tsm_system_handler(_fcinfo: FunctionCallInfo) -> Datum {
    // C: TsmRoutine *tsm = makeNode(TsmRoutine);
    // TODO: T_TsmRoutine is not a NodeTag variant yet, so we palloc0 the struct
    // and leave the node tag at 0 (T_Invalid) instead of makeNode(TsmRoutine).
    let tsm = palloc0(core::mem::size_of::<TsmRoutine>()) as *mut TsmRoutine;

    // C: tsm->parameterTypes = list_make1_oid(FLOAT4OID);
    (*tsm).parameterTypes = list_make1_oid!(crate::catalog::pg_type_d::FLOAT4OID);
    (*tsm).repeatable_across_queries = true;
    (*tsm).repeatable_across_scans = true;
    (*tsm).SampleScanGetSampleSize = Some(system_samplescangetsamplesize);
    (*tsm).InitSampleScan = Some(system_initsamplescan);
    (*tsm).BeginSampleScan = Some(system_beginsamplescan);
    (*tsm).NextSampleBlock = Some(system_nextsampleblock);
    (*tsm).NextSampleTuple = Some(system_nextsampletuple);
    (*tsm).EndSampleScan = None;

    PG_RETURN_POINTER!(tsm);
}

/*
 * Sample size estimation.
 *
 * STUB: estimate_expression_value() and clamp_row_est() are unported, and
 * RelOptInfo (baserel->pages/->tuples) is not yet a ported type. We keep the
 * REAL samplefract default (0.1f) and clamp_row_est shape but cannot read
 * baserel, so the pages and tuples out-params are computed from a 0 base. See bernoulli.rs.
 */
unsafe fn system_samplescangetsamplesize(
    _root: *mut c_void,    /* PlannerInfo * */
    _baserel: *mut c_void, /* RelOptInfo * */
    _paramexprs: *mut crate::nodes::pg_list::List,
    pages: *mut BlockNumber,
    tuples: *mut f64,
) {
    // estimate_expression_value()/Const folding unported -> default branch.
    let samplefract: f32 = 0.1f32;

    // C: *pages  = clamp_row_est(baserel->pages  * samplefract);
    //    *tuples = clamp_row_est(baserel->tuples * samplefract);
    // TODO: RelOptInfo unported -> baserel->pages / baserel->tuples unavailable.
    if !pages.is_null() {
        *pages = clamp_row_est(0.0 * samplefract as f64) as BlockNumber;
    }
    if !tuples.is_null() {
        *tuples = clamp_row_est(0.0 * samplefract as f64);
    }
}

// STUB: clamp_row_est (optimizer/optimizer.h); identity until planner ported.
#[inline]
unsafe fn clamp_row_est(nrows: f64) -> f64 {
    // TODO: port real clamp_row_est (clamp to [1, MAXINT], rint).
    nrows
}

/*
 * Initialize during executor setup.
 */
unsafe fn system_initsamplescan(node: *mut SampleScanState, _eflags: c_int) {
    (*node).tsm_state = palloc0(core::mem::size_of::<SystemSamplerData>()) as *mut c_void;
}

/*
 * Examine parameters and prepare for a sample scan.
 */
unsafe fn system_beginsamplescan(
    node: *mut SampleScanState,
    params: *mut Datum,
    _nparams: c_int,
    seed: uint32,
) {
    let sampler = (*node).tsm_state as *mut SystemSamplerData;
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
    (*sampler).nextblock = 0;
    (*sampler).lt = InvalidOffsetNumber;

    /*
     * Bulkread buffer access strategy probably makes sense unless we're
     * scanning a very small fraction of the table. The 1% cutoff here is a
     * guess. We should use pagemode visibility checking, since we scan all
     * tuples on each selected page.
     */
    (*node).use_bulkread = percent >= 1.0;
    (*node).use_pagemode = true;
}

/*
 * Select next block to sample.
 */
unsafe fn system_nextsampleblock(node: *mut SampleScanState, nblocks: BlockNumber) -> BlockNumber {
    let sampler = (*node).tsm_state as *mut SystemSamplerData;
    let mut nextblock: BlockNumber = (*sampler).nextblock;
    let mut hashinput: [uint32; 2] = [0; 2];

    /*
     * We compute the hash by applying hash_any to an array of 2 uint32's
     * containing the block number and seed.
     *
     * This word in the hash input is the same throughout:
     */
    hashinput[1] = (*sampler).seed;

    /*
     * Loop over block numbers until finding suitable block or reaching end of
     * relation.
     */
    while nextblock < nblocks {
        hashinput[0] = nextblock;

        let hash: uint32 = DatumGetUInt32(hash_any(
            hashinput.as_ptr() as *const c_uchar,
            core::mem::size_of::<[uint32; 2]>() as c_int,
        ));
        if (hash as uint64) < (*sampler).cutoff {
            break;
        }
        nextblock += 1;
    }

    if nextblock < nblocks {
        /* Found a suitable block; remember where we should start next time */
        (*sampler).nextblock = nextblock + 1;
        return nextblock;
    }

    /* Done, but let's reset nextblock to 0 for safety. */
    (*sampler).nextblock = 0;
    InvalidBlockNumber
}

/*
 * Select next sampled tuple in current block.
 *
 * In block sampling, we just want to sample all the tuples in each selected
 * block.
 *
 * It is OK here to return an offset without knowing if the tuple is visible
 * (or even exists); nodeSamplescan.c will deal with that.
 *
 * When we reach end of the block, return InvalidOffsetNumber which tells
 * SampleScan to go to next block.
 */
unsafe fn system_nextsampletuple(
    node: *mut SampleScanState,
    _blockno: BlockNumber,
    maxoffset: OffsetNumber,
) -> OffsetNumber {
    let sampler = (*node).tsm_state as *mut SystemSamplerData;
    let mut tupoffset: OffsetNumber = (*sampler).lt;

    /* Advance to next possible offset on page */
    if tupoffset == InvalidOffsetNumber {
        tupoffset = FirstOffsetNumber;
    } else {
        tupoffset += 1;
    }

    /* Done? */
    if tupoffset > maxoffset {
        tupoffset = InvalidOffsetNumber;
    }

    (*sampler).lt = tupoffset;

    tupoffset
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn make_state(sampler: *mut SystemSamplerData) -> *mut SampleScanState {
        let node = Box::into_raw(Box::new(
            core::mem::MaybeUninit::<SampleScanState>::zeroed().assume_init(),
        ));
        (*node).tsm_state = sampler as *mut c_void;
        node
    }

    unsafe fn seed_sampler(percent: f64, seed: uint32) -> SystemSamplerData {
        let dcutoff = rint(((PG_UINT32_MAX as f64) + 1.0) * percent / 100.0);
        SystemSamplerData {
            cutoff: dcutoff as uint64,
            seed,
            nextblock: 0,
            lt: InvalidOffsetNumber,
        }
    }

    // Collect the blocks system selects out of nblocks blocks.
    unsafe fn run_blocks(percent: f64, seed: uint32, nblocks: BlockNumber) -> Vec<BlockNumber> {
        let mut sampler = seed_sampler(percent, seed);
        let node = make_state(&mut sampler as *mut SystemSamplerData);
        let mut out = Vec::new();
        loop {
            let blk = system_nextsampleblock(node, nblocks);
            if blk == InvalidBlockNumber {
                break;
            }
            out.push(blk);
        }
        drop(Box::from_raw(node));
        out
    }

    // Collect the offsets system returns for one block of maxoffset tuples.
    unsafe fn run_tuples(maxoffset: OffsetNumber) -> Vec<OffsetNumber> {
        let mut sampler = seed_sampler(50.0, 1);
        let node = make_state(&mut sampler as *mut SystemSamplerData);
        let mut out = Vec::new();
        loop {
            let off = system_nextsampletuple(node, 0, maxoffset);
            if off == InvalidOffsetNumber {
                break;
            }
            out.push(off);
        }
        drop(Box::from_raw(node));
        out
    }

    #[test]
    fn blocks_in_range_and_monotonic() {
        unsafe {
            let nblocks: BlockNumber = 1000;
            let seq = run_blocks(50.0, 9999, nblocks);
            for (i, &blk) in seq.iter().enumerate() {
                assert!(blk < nblocks);
                if i > 0 {
                    // nextsampleblock advances strictly forward.
                    assert!(blk > seq[i - 1]);
                }
            }
        }
    }

    #[test]
    fn deterministic_for_fixed_seed() {
        unsafe {
            let a = run_blocks(20.0, 0xBEEF, 500);
            let b = run_blocks(20.0, 0xBEEF, 500);
            assert_eq!(a, b);
        }
    }

    #[test]
    fn block_probability_limits() {
        unsafe {
            // 100% selects every block; 0% selects none.
            let all = run_blocks(100.0, 7, 128);
            assert_eq!(all.len(), 128);
            assert_eq!(all.first().copied(), Some(0));
            assert_eq!(all.last().copied(), Some(127));

            let none = run_blocks(0.0, 7, 128);
            assert!(none.is_empty());
        }
    }

    #[test]
    fn system_returns_all_tuples_in_block() {
        unsafe {
            // system_nextsampletuple returns every offset 1..=maxoffset in order.
            let seq = run_tuples(50);
            let expect: Vec<OffsetNumber> = (FirstOffsetNumber..=50).collect();
            assert_eq!(seq, expect);
        }
    }
}

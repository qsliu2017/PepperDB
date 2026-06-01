//! Translation of postgres/src/include/access/tsmapi.h
//!         (+ GetTsmRoutine from postgres/src/backend/access/tablesample/tablesample.c)
//!
//! API for tablesample methods.
//!
//! TsmRoutine is the struct returned by a tablesample method's handler
//! function. It provides pointers to the callback functions needed by the
//! planner and executor, as well as additional information about the method.
//!
//! Portions Copyright (c) 2015-2025, PostgreSQL Global Development Group
//!
//! #include mapping:
//!   - "nodes/execnodes.h"  -> crate::nodes::execnodes::SampleScanState (REAL)
//!   - "nodes/pathnodes.h"  -> PlannerInfo / RelOptInfo are NOT yet ported, so
//!                             the SampleScanGetSampleSize callback takes them as
//!                             *mut c_void (root/baserel). (STUB types)
//!   - "access/tsmapi.h" (GetTsmRoutine, from tablesample.c) -> ported below.
//!
//! NOTE: the C `makeNode(TsmRoutine)` tags the node with `T_TsmRoutine`. That
//! variant is NOT yet present in crate::nodes::nodes::NodeTag, so the handlers
//! (bernoulli/system) cannot tag the node and GetTsmRoutine's `IsA(routine,
//! TsmRoutine)` check is degraded to a NULL check. See TODO in `GetTsmRoutine`.

use crate::prelude::*;

use crate::nodes::execnodes::SampleScanState;
use crate::nodes::nodes::NodeTag;
use crate::nodes::pg_list::List;
use crate::storage::block::BlockNumber;
use crate::storage::off::OffsetNumber;
use crate::utils::fmgr::OidFunctionCall1Coll;

/*
 * Callback function signatures --- see tablesample-method.sgml for more info.
 */

// PlannerInfo *root, RelOptInfo *baserel: pathnodes.h is unported, so both are
// passed as *mut c_void. List *paramexprs is real.
pub type SampleScanGetSampleSize_function = Option<
    unsafe fn(
        root: *mut c_void,    /* PlannerInfo * */
        baserel: *mut c_void, /* RelOptInfo * */
        paramexprs: *mut List,
        pages: *mut BlockNumber,
        tuples: *mut f64,
    ),
>;

pub type InitSampleScan_function = Option<unsafe fn(node: *mut SampleScanState, eflags: c_int)>;

pub type BeginSampleScan_function = Option<
    unsafe fn(node: *mut SampleScanState, params: *mut Datum, nparams: c_int, seed: uint32),
>;

pub type NextSampleBlock_function =
    Option<unsafe fn(node: *mut SampleScanState, nblocks: BlockNumber) -> BlockNumber>;

pub type NextSampleTuple_function = Option<
    unsafe fn(
        node: *mut SampleScanState,
        blockno: BlockNumber,
        maxoffset: OffsetNumber,
    ) -> OffsetNumber,
>;

pub type EndSampleScan_function = Option<unsafe fn(node: *mut SampleScanState)>;

/*
 * TsmRoutine is the struct returned by a tablesample method's handler function.
 *
 * It's recommended that the handler initialize the struct with
 * makeNode(TsmRoutine) so that all fields are set to NULL.
 */
#[repr(C)]
pub struct TsmRoutine {
    pub type_: NodeTag,

    /* List of datatype OIDs for the arguments of the TABLESAMPLE clause */
    pub parameterTypes: *mut List,

    /* Can method produce repeatable samples across, or even within, queries? */
    pub repeatable_across_queries: bool,
    pub repeatable_across_scans: bool,

    /* Functions for planning a SampleScan on a physical table */
    pub SampleScanGetSampleSize: SampleScanGetSampleSize_function,

    /* Functions for executing a SampleScan on a physical table */
    pub InitSampleScan: InitSampleScan_function, /* can be NULL */
    pub BeginSampleScan: BeginSampleScan_function,
    pub NextSampleBlock: NextSampleBlock_function, /* can be NULL */
    pub NextSampleTuple: NextSampleTuple_function,
    pub EndSampleScan: EndSampleScan_function, /* can be NULL */
}

/*
 * GetTsmRoutine --- get a TsmRoutine struct by invoking the handler.
 *
 * This is a convenience routine that's just meant to check for errors.
 *
 * (from src/backend/access/tablesample/tablesample.c)
 */
pub unsafe fn GetTsmRoutine(tsmhandler: Oid) -> *mut TsmRoutine {
    // C: datum = OidFunctionCall1(tsmhandler, PointerGetDatum(NULL));
    let datum: Datum = OidFunctionCall1Coll(
        tsmhandler,
        crate::postgres_ext::InvalidOid,
        PointerGetDatum(null()),
    );
    let routine = DatumGetPointer(datum) as *mut TsmRoutine;

    // C: if (routine == NULL || !IsA(routine, TsmRoutine))
    // TODO: T_TsmRoutine is not yet a NodeTag variant, so the IsA() tag check is
    // omitted; only the NULL check is performed here.
    if routine.is_null() {
        elog!(
            ERROR,
            "tablesample handler function {} did not return a TsmRoutine struct",
            tsmhandler
        );
    }

    routine
}

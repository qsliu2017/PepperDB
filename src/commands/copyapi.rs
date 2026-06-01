//! commands/copyapi.h - API for COPY TO/FROM handlers.

use std::ffi::c_void;

use crate::access::common::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::execnodes::ExprContext;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::fmgr::FmgrInfo;

// CopyToState / CopyFromState are defined (as opaque pointers) in copy.h, which
// has not been ported yet. Provide local stubs matching the C opaque-handle
// pattern (both are `typedef struct CopyToStateData *CopyToState`).
// copy.h has landed (src/commands/copy.rs) - point at the real state structs.
pub type CopyToState = *mut crate::commands::copy::CopyToStateData;
pub type CopyFromState = *mut crate::commands::copy::CopyFromStateData;

/*
 * API structure for a COPY TO format implementation. Note this must be
 * allocated in a server-lifetime manner, typically as a static const struct.
 */
#[repr(C)]
pub struct CopyToRoutine {
    /*
     * Set output function information. This callback is called once at the
     * beginning of COPY TO.
     *
     * 'finfo' can be optionally filled to provide the catalog information of
     * the output function.
     *
     * 'atttypid' is the OID of data type used by the relation's attribute.
     */
    pub CopyToOutFunc:
        Option<unsafe extern "C" fn(cstate: CopyToState, atttypid: Oid, finfo: *mut FmgrInfo)>,

    /*
     * Start a COPY TO. This callback is called once at the beginning of COPY
     * TO.
     *
     * 'tupDesc' is the tuple descriptor of the relation from where the data
     * is read.
     */
    pub CopyToStart: Option<unsafe extern "C" fn(cstate: CopyToState, tupDesc: TupleDesc)>,

    /*
     * Write one row stored in 'slot' to the destination.
     */
    pub CopyToOneRow:
        Option<unsafe extern "C" fn(cstate: CopyToState, slot: *mut TupleTableSlot)>,

    /*
     * End a COPY TO. This callback is called once at the end of COPY TO.
     */
    pub CopyToEnd: Option<unsafe extern "C" fn(cstate: CopyToState)>,
}

/*
 * API structure for a COPY FROM format implementation. Note this must be
 * allocated in a server-lifetime manner, typically as a static const struct.
 */
#[repr(C)]
pub struct CopyFromRoutine {
    /*
     * Set input function information. This callback is called once at the
     * beginning of COPY FROM.
     *
     * 'finfo' can be optionally filled to provide the catalog information of
     * the input function.
     *
     * 'typioparam' can be optionally filled to define the OID of the type to
     * pass to the input function. 'atttypid' is the OID of data type used by
     * the relation's attribute.
     */
    pub CopyFromInFunc: Option<
        unsafe extern "C" fn(
            cstate: CopyFromState,
            atttypid: Oid,
            finfo: *mut FmgrInfo,
            typioparam: *mut Oid,
        ),
    >,

    /*
     * Start a COPY FROM. This callback is called once at the beginning of
     * COPY FROM.
     *
     * 'tupDesc' is the tuple descriptor of the relation where the data needs
     * to be copied. This can be used for any initialization steps required by
     * a format.
     */
    pub CopyFromStart: Option<unsafe extern "C" fn(cstate: CopyFromState, tupDesc: TupleDesc)>,

    /*
     * Read one row from the source and fill *values and *nulls.
     *
     * 'econtext' is used to evaluate default expression for each column that
     * is either not read from the file or is using the DEFAULT option of COPY
     * FROM. It is NULL if no default values are used.
     *
     * Returns false if there are no more tuples to read.
     */
    pub CopyFromOneRow: Option<
        unsafe extern "C" fn(
            cstate: CopyFromState,
            econtext: *mut ExprContext,
            values: *mut Datum,
            nulls: *mut bool,
        ) -> bool,
    >,

    /*
     * End a COPY FROM. This callback is called once at the end of COPY FROM.
     */
    pub CopyFromEnd: Option<unsafe extern "C" fn(cstate: CopyFromState)>,
}

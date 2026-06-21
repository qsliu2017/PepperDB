//! execAsync.c - Support routines for asynchronous execution.

use crate::prelude::*;

use std::ffi::c_int;

use crate::executor::executor::ExecReScan;
use crate::executor::instrument::{InstrStartNode, InstrStopNode};
use crate::executor::tuptable::{TupIsNull, TupleTableSlot};
use crate::nodes::execnodes::AsyncRequest;
use crate::nodes::nodes::{nodeTag, NodeTag};

/* ----------------------------------------------------------------
 * Local stubs for node-type specific async callbacks not yet ported.
 * ---------------------------------------------------------------- */

// TODO: from executor/nodeForeignscan.h
unsafe fn ExecAsyncForeignScanRequest(_areq: *mut AsyncRequest) {
    crate::executor::nodeForeignscan::ExecAsyncForeignScanRequest(_areq as _)
}

// TODO: from executor/nodeForeignscan.h
unsafe fn ExecAsyncForeignScanConfigureWait(_areq: *mut AsyncRequest) {
    crate::executor::nodeForeignscan::ExecAsyncForeignScanConfigureWait(_areq as _)
}

// TODO: from executor/nodeForeignscan.h
unsafe fn ExecAsyncForeignScanNotify(_areq: *mut AsyncRequest) {
    crate::executor::nodeForeignscan::ExecAsyncForeignScanNotify(_areq as _)
}

// TODO: from executor/nodeAppend.h
unsafe fn ExecAsyncAppendResponse(_areq: *mut AsyncRequest) {
    unimplemented!()
}

/*
 * Asynchronously request a tuple from a designed async-capable node.
 */
pub unsafe fn ExecAsyncRequest(areq: *mut AsyncRequest) {
    if !(*(*areq).requestee).chgParam.is_null() {
        /* something changed? */
        ExecReScan((*areq).requestee); /* let ReScan handle this */
    }

    /* must provide our own instrumentation support */
    if !(*(*areq).requestee).instrument.is_null() {
        InstrStartNode((*(*areq).requestee).instrument);
    }

    match nodeTag((*areq).requestee) {
        NodeTag::T_ForeignScanState => {
            ExecAsyncForeignScanRequest(areq);
        }
        _ => {
            /* If the node doesn't support async, caller messed up. */
            elog!(
                ERROR,
                "unrecognized node type: {}",
                nodeTag((*areq).requestee) as c_int
            );
        }
    }

    ExecAsyncResponse(areq);

    /* must provide our own instrumentation support */
    if !(*(*areq).requestee).instrument.is_null() {
        InstrStopNode(
            (*(*areq).requestee).instrument,
            if TupIsNull((*areq).result) { 0.0 } else { 1.0 },
        );
    }
}

/*
 * Give the asynchronous node a chance to configure the file descriptor event
 * for which it wishes to wait.  We expect the node-type specific callback to
 * make a single call of the following form:
 *
 * AddWaitEventToSet(set, WL_SOCKET_READABLE, fd, NULL, areq);
 */
pub unsafe fn ExecAsyncConfigureWait(areq: *mut AsyncRequest) {
    /* must provide our own instrumentation support */
    if !(*(*areq).requestee).instrument.is_null() {
        InstrStartNode((*(*areq).requestee).instrument);
    }

    match nodeTag((*areq).requestee) {
        NodeTag::T_ForeignScanState => {
            ExecAsyncForeignScanConfigureWait(areq);
        }
        _ => {
            /* If the node doesn't support async, caller messed up. */
            elog!(
                ERROR,
                "unrecognized node type: {}",
                nodeTag((*areq).requestee) as c_int
            );
        }
    }

    /* must provide our own instrumentation support */
    if !(*(*areq).requestee).instrument.is_null() {
        InstrStopNode((*(*areq).requestee).instrument, 0.0);
    }
}

/*
 * Call the asynchronous node back when a relevant event has occurred.
 */
pub unsafe fn ExecAsyncNotify(areq: *mut AsyncRequest) {
    /* must provide our own instrumentation support */
    if !(*(*areq).requestee).instrument.is_null() {
        InstrStartNode((*(*areq).requestee).instrument);
    }

    match nodeTag((*areq).requestee) {
        NodeTag::T_ForeignScanState => {
            ExecAsyncForeignScanNotify(areq);
        }
        _ => {
            /* If the node doesn't support async, caller messed up. */
            elog!(
                ERROR,
                "unrecognized node type: {}",
                nodeTag((*areq).requestee) as c_int
            );
        }
    }

    ExecAsyncResponse(areq);

    /* must provide our own instrumentation support */
    if !(*(*areq).requestee).instrument.is_null() {
        InstrStopNode(
            (*(*areq).requestee).instrument,
            if TupIsNull((*areq).result) { 0.0 } else { 1.0 },
        );
    }
}

/*
 * Call the requestor back when an asynchronous node has produced a result.
 */
pub unsafe fn ExecAsyncResponse(areq: *mut AsyncRequest) {
    match nodeTag((*areq).requestor) {
        NodeTag::T_AppendState => {
            ExecAsyncAppendResponse(areq);
        }
        _ => {
            /* If the node doesn't support async, caller messed up. */
            elog!(
                ERROR,
                "unrecognized node type: {}",
                nodeTag((*areq).requestor) as c_int
            );
        }
    }
}

/*
 * A requestee node should call this function to deliver the tuple to its
 * requestor node.  The requestee node can call this from its ExecAsyncRequest
 * or ExecAsyncNotify callback.
 */
pub unsafe fn ExecAsyncRequestDone(areq: *mut AsyncRequest, result: *mut TupleTableSlot) {
    (*areq).request_complete = true;
    (*areq).result = result;
}

/*
 * A requestee node should call this function to indicate that it is pending
 * for a callback.  The requestee node can call this from its ExecAsyncRequest
 * or ExecAsyncNotify callback.
 */
pub unsafe fn ExecAsyncRequestPending(areq: *mut AsyncRequest) {
    (*areq).callback_pending = true;
    (*areq).request_complete = false;
    (*areq).result = std::ptr::null_mut();
}

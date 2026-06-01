//! Translation of postgres/src/backend/commands/explain_dr.c
//! (+ the declarations from src/include/commands/explain_dr.h).
//!
//! The DestReceiver used by EXPLAIN (ANALYZE, SERIALIZE ...): it serializes the
//! query's output rows into RowData messages (printtup-style) while measuring the
//! resources expended and total serialized size, but NEVER sends the data to the
//! client.  This is how EXPLAIN measures the overhead of deTOASTing and the
//! datatype out/send functions without actually hitting the network.
//!
//! #include mapping:
//!   commands/explain_state.h -> crate::commands::explain_state {ExplainState,
//!                               ExplainSerializeOption, EXPLAIN_SERIALIZE_*}
//!   tcop/dest.h              -> crate::tcop::dest {DestReceiver, CommandDest,
//!                               DestExplainSerialize}
//!   executor/tuptable.h      -> crate::executor::tuptable {TupleTableSlot,
//!                               slot_getallattrs}
//!   executor/instrument.h    -> crate::executor::instrument {BufferUsage,
//!                               BufferUsageAccumDiff}; instr_time comes from
//!                               crate::portability::instr_time.
//!   access/tupdesc.h         -> crate::access::common::tupdesc {TupleDesc,
//!                               TupleDescAttr}
//!   utils/fmgr.h             -> crate::utils::fmgr {FmgrInfo, fmgr_info,
//!                               OutputFunctionCall, SendFunctionCall}
//!   lib/stringinfo.h         -> crate::lib::stringinfo {StringInfoData, ...}
//!   libpq/pqformat.h         -> crate::libpq::pqformat {pq_beginmessage_reuse,
//!                               pq_sendint16/32, pq_sendcountedtext, pq_sendbytes}
//!
//! STUBS (modules not yet ported):
//!   - getTypeOutputInfo / getTypeBinaryOutputInfo  (utils/lsyscache.h): the
//!     catalog lookups that map a column's atttypid to its output/send function
//!     OID.  Stubbed locally with a TODO; serialize_prepare_info still does the
//!     real palloc0 of FmgrInfo[] and the real fmgr_info() wiring once the OID is
//!     available.
//!   - pgBufferUsage (the pgstat global) is a file-local `static mut` in
//!     crate::executor::instrument and is NOT exported.  A module-local stub
//!     mirrors it here so the BUFFERS accounting (BufferUsageAccumDiff against the
//!     "current" global) is wired exactly as in C; replace with the real global
//!     when it is exported.  The metrics math (bytesSent/timeSpent/bufferUsage) is
//!     REAL.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994-5, Regents of the University of California

use crate::prelude::*;

use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::commands::explain_state::{
    ExplainState, EXPLAIN_SERIALIZE_BINARY, EXPLAIN_SERIALIZE_NONE, EXPLAIN_SERIALIZE_TEXT,
};
use crate::executor::instrument::{BufferUsage, BufferUsageAccumDiff};
use crate::executor::tuptable::{slot_getallattrs, TupleTableSlot};
use crate::lib::stringinfo::{initStringInfo, StringInfo, StringInfoData};
use crate::libpq::pqformat::{
    pq_beginmessage_reuse, pq_sendbytes, pq_sendcountedtext, pq_sendint16, pq_sendint32,
};
use crate::portability::instr_time::{
    instr_time, INSTR_TIME_ACCUM_DIFF, INSTR_TIME_SET_CURRENT, INSTR_TIME_SET_ZERO,
};
use crate::tcop::dest::{CommandDest, DestReceiver};
use crate::utils::fmgr::{fmgr_info, FmgrInfo, OutputFunctionCall, SendFunctionCall};

use crate::varatt::{VARDATA, VARSIZE};

use core::ffi::c_int;

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

// `D` -- the frontend wire-protocol DataRow message type (libpq/protocol.h:
// `#define PqMsg_DataRow 'D'`).  Defined module-locally as in printtup.rs /
// printsimple.rs (libpq/protocol.h is not separately ported).
const PqMsg_DataRow: c_char = b'D' as c_char;

// ----------------------------------------------------------------------------
// STUBS for catalog lookups not yet ported (utils/lsyscache.h).
// ----------------------------------------------------------------------------

// TODO(pg-port): utils/lsyscache.h not yet ported.  getTypeOutputInfo maps a
// type OID to its text-output function OID (and whether it is varlena);
// getTypeBinaryOutputInfo maps to the binary send function OID.  These read
// pg_type, so they need the syscache.  Stubbed; serialize_prepare_info does the
// real FmgrInfo allocation and fmgr_info() wiring around them.
unsafe fn getTypeOutputInfo(_type_oid: Oid, typOutput: *mut Oid, typIsVarlena: *mut bool) {
    unimplemented!("TODO: getTypeOutputInfo (utils/lsyscache.h not yet ported)");
    #[allow(unreachable_code)]
    {
        *typOutput = InvalidOid;
        *typIsVarlena = false;
    }
}

unsafe fn getTypeBinaryOutputInfo(_type_oid: Oid, typSend: *mut Oid, typIsVarlena: *mut bool) {
    unimplemented!("TODO: getTypeBinaryOutputInfo (utils/lsyscache.h not yet ported)");
    #[allow(unreachable_code)]
    {
        *typSend = InvalidOid;
        *typIsVarlena = false;
    }
}

// TODO(pg-port): the real `pgBufferUsage` global lives as a file-local `static
// mut` in crate::executor::instrument and is not exported.  Mirror it here so the
// EXPLAIN (BUFFERS) accounting wires exactly as the C code (diff "now" against a
// snapshot taken before the row).  Replace with the exported global when it lands.
static mut pgBufferUsage: BufferUsage = unsafe { core::mem::zeroed() };

// ----------------------------------------------------------------------------
// SerializeMetrics (explain_dr.h) -- instrumentation collected for SERIALIZE.
// ----------------------------------------------------------------------------

/// Instrumentation data for EXPLAIN's SERIALIZE option.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SerializeMetrics {
    /// # of bytes serialized
    pub bytesSent: uint64,
    /// time spent serializing
    pub timeSpent: instr_time,
    /// buffers accessed during serialization
    pub bufferUsage: BufferUsage,
}

// ----------------------------------------------------------------------------
// SerializeDestReceiver -- the concrete receiver.  `pub` (the C field name) is a
// Rust keyword, so the embedded DestReceiver is named `pub_`.  It MUST be the
// first field for the `(SerializeDestReceiver *) self` downcast to be valid.
// ----------------------------------------------------------------------------

#[repr(C)]
pub struct SerializeDestReceiver {
    /// embedded base receiver (downcast target); C field name is `pub`
    pub pub_: DestReceiver,
    /// this EXPLAIN statement's ExplainState
    pub es: *mut ExplainState,
    /// text or binary, like pq wire protocol
    pub format: int8,
    /// the output tuple desc
    pub attrinfo: TupleDesc,
    /// current number of columns
    pub nattrs: c_int,
    /// precomputed call info for output fns
    pub finfos: *mut FmgrInfo,
    /// per-row temporary memory context
    pub tmpcontext: MemoryContext,
    /// buffer to hold the constructed message
    pub buf: StringInfoData,
    /// collected metrics
    pub metrics: SerializeMetrics,
}

/// Get the function lookup info that we'll need for output.
///
/// This is a subset of what printtup_prepare_info() does.  We don't need to
/// cope with format choices varying across columns, so it's slightly simpler.
unsafe fn serialize_prepare_info(
    receiver: *mut SerializeDestReceiver,
    typeinfo: TupleDesc,
    nattrs: c_int,
) {
    /* get rid of any old data */
    if !(*receiver).finfos.is_null() {
        pfree((*receiver).finfos as *mut c_void);
    }
    (*receiver).finfos = null_mut();

    (*receiver).attrinfo = typeinfo;
    (*receiver).nattrs = nattrs;
    if nattrs <= 0 {
        return;
    }

    (*receiver).finfos =
        palloc0(nattrs as usize * core::mem::size_of::<FmgrInfo>()) as *mut FmgrInfo;

    for i in 0..nattrs {
        let finfo: *mut FmgrInfo = (*receiver).finfos.add(i as usize);
        let attr = TupleDescAttr(typeinfo, i);
        let mut typoutput: Oid = InvalidOid;
        let mut typsend: Oid = InvalidOid;
        let mut typisvarlena: bool = false;

        if (*receiver).format == 0 {
            /* wire protocol format text */
            getTypeOutputInfo((*attr).atttypid, &mut typoutput, &mut typisvarlena);
            fmgr_info(typoutput, finfo);
        } else if (*receiver).format == 1 {
            /* wire protocol format binary */
            getTypeBinaryOutputInfo((*attr).atttypid, &mut typsend, &mut typisvarlena);
            fmgr_info(typsend, finfo);
        } else {
            ereport!(
                ERROR,
                errmsg!("unsupported format code: {}", (*receiver).format)
            );
            unreachable!();
        }
    }
}

/// serializeAnalyzeReceive - collect tuples for EXPLAIN (SERIALIZE)
///
/// This should match printtup() in printtup.c as closely as possible,
/// except for the addition of measurement code.
unsafe fn serializeAnalyzeReceive(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool {
    let typeinfo: TupleDesc = (*slot).tts_tupleDescriptor;
    let myState = self_ as *mut SerializeDestReceiver;
    let buf: StringInfo = &mut (*myState).buf;
    let natts = (*typeinfo).natts;
    let mut start: instr_time = instr_time::default();
    let mut end: instr_time = instr_time::default();
    let mut instr_start: BufferUsage = core::mem::zeroed();

    /* only measure time, buffers if requested */
    if (*(*myState).es).timing {
        INSTR_TIME_SET_CURRENT(&mut start);
    }
    if (*(*myState).es).buffers {
        instr_start = pgBufferUsage;
    }

    /* Set or update my derived attribute info, if needed */
    if (*myState).attrinfo != typeinfo || (*myState).nattrs != natts {
        serialize_prepare_info(myState, typeinfo, natts);
    }

    /* Make sure the tuple is fully deconstructed */
    slot_getallattrs(slot);

    /* Switch into per-row context so we can recover memory below */
    let oldcontext = MemoryContextSwitchTo((*myState).tmpcontext);

    /*
     * Prepare a DataRow message (note buffer is in per-query context)
     *
     * Note that we fill a StringInfo buffer the same as printtup() does, so
     * as to capture the costs of manipulating the strings accurately.
     */
    pq_beginmessage_reuse(buf, PqMsg_DataRow);

    pq_sendint16(buf, natts as uint16);

    /*
     * send the attributes of this tuple
     */
    for i in 0..natts {
        let finfo: *mut FmgrInfo = (*myState).finfos.add(i as usize);
        let attr: Datum = *(*slot).tts_values.add(i as usize);

        if *(*slot).tts_isnull.add(i as usize) {
            pq_sendint32(buf, (-1i32) as uint32);
            continue;
        }

        if (*myState).format == 0 {
            /* Text output */
            let outputstr: *mut c_char = OutputFunctionCall(finfo, attr);
            pq_sendcountedtext(buf, outputstr, strlen(outputstr) as c_int);
        } else {
            /* Binary output */
            let outputbytes: *mut bytea = SendFunctionCall(finfo, attr);
            pq_sendint32(
                buf,
                VARSIZE(outputbytes as *const c_char).wrapping_sub(VARHDRSZ as u32),
            );
            pq_sendbytes(
                buf,
                VARDATA(outputbytes as *const c_char) as *const c_void,
                (VARSIZE(outputbytes as *const c_char).wrapping_sub(VARHDRSZ as u32)) as c_int,
            );
        }
    }

    /*
     * We mustn't call pq_endmessage_reuse(), since that would actually send
     * the data to the client.  Just count the data, instead.  We can leave
     * the buffer alone; it'll be reset on the next iteration (as would also
     * happen in printtup()).
     */
    (*myState).metrics.bytesSent += (*buf).len as uint64;

    /* Return to caller's context, and flush row's temporary memory */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset((*myState).tmpcontext);

    /* Update timing data */
    if (*(*myState).es).timing {
        INSTR_TIME_SET_CURRENT(&mut end);
        INSTR_TIME_ACCUM_DIFF(&mut (*myState).metrics.timeSpent, end, start);
    }

    /* Update buffer metrics */
    if (*(*myState).es).buffers {
        BufferUsageAccumDiff(
            &mut (*myState).metrics.bufferUsage,
            &raw const pgBufferUsage,
            &instr_start,
        );
    }

    true
}

/// serializeAnalyzeStartup - start up the serializeAnalyze receiver
unsafe fn serializeAnalyzeStartup(
    self_: *mut DestReceiver,
    _operation: c_int,
    _typeinfo: TupleDesc,
) {
    let receiver = self_ as *mut SerializeDestReceiver;

    Assert!(!(*receiver).es.is_null());

    match (*(*receiver).es).serialize {
        EXPLAIN_SERIALIZE_NONE => {
            Assert!(false);
        }
        EXPLAIN_SERIALIZE_TEXT => {
            (*receiver).format = 0; /* wire protocol format text */
        }
        EXPLAIN_SERIALIZE_BINARY => {
            (*receiver).format = 1; /* wire protocol format binary */
        }
    }

    /* Create per-row temporary memory context */
    (*receiver).tmpcontext = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"SerializeTupleReceive".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    /* The output buffer is re-used across rows, as in printtup.c */
    initStringInfo(&mut (*receiver).buf);

    /* Initialize results counters */
    core::ptr::write_bytes(
        &mut (*receiver).metrics as *mut SerializeMetrics,
        0,
        1,
    );
    INSTR_TIME_SET_ZERO(&mut (*receiver).metrics.timeSpent);
}

/// serializeAnalyzeShutdown - shut down the serializeAnalyze receiver
unsafe fn serializeAnalyzeShutdown(self_: *mut DestReceiver) {
    let receiver = self_ as *mut SerializeDestReceiver;

    if !(*receiver).finfos.is_null() {
        pfree((*receiver).finfos as *mut c_void);
    }
    (*receiver).finfos = null_mut();

    if !(*receiver).buf.data.is_null() {
        pfree((*receiver).buf.data as *mut c_void);
    }
    (*receiver).buf.data = null_mut();

    if !(*receiver).tmpcontext.is_null() {
        MemoryContextDelete((*receiver).tmpcontext);
    }
    (*receiver).tmpcontext = null_mut();
}

/// serializeAnalyzeDestroy - destroy the serializeAnalyze receiver
unsafe fn serializeAnalyzeDestroy(self_: *mut DestReceiver) {
    pfree(self_ as *mut c_void);
}

/// Build a DestReceiver for EXPLAIN (SERIALIZE) instrumentation.
pub unsafe fn CreateExplainSerializeDestReceiver(es: *mut ExplainState) -> *mut DestReceiver {
    let self_ =
        palloc0(core::mem::size_of::<SerializeDestReceiver>()) as *mut SerializeDestReceiver;

    (*self_).pub_.receiveSlot = Some(serializeAnalyzeReceive);
    (*self_).pub_.rStartup = Some(serializeAnalyzeStartup);
    (*self_).pub_.rShutdown = Some(serializeAnalyzeShutdown);
    (*self_).pub_.rDestroy = Some(serializeAnalyzeDestroy);
    (*self_).pub_.mydest = CommandDest::DestExplainSerialize;

    (*self_).es = es;

    self_ as *mut DestReceiver
}

/// GetSerializationMetrics - collect metrics
///
/// We have to be careful here since the receiver could be an IntoRel
/// receiver if the subject statement is CREATE TABLE AS.  In that
/// case, return all-zeroes stats.
pub unsafe fn GetSerializationMetrics(dest: *mut DestReceiver) -> SerializeMetrics {
    if (*dest).mydest == CommandDest::DestExplainSerialize {
        return (*(dest as *mut SerializeDestReceiver)).metrics;
    }

    let mut empty: SerializeMetrics = core::mem::zeroed();
    INSTR_TIME_SET_ZERO(&mut empty.timeSpent);

    empty
}

#[cfg(test)]
mod tests {
    use super::*;

    // A minimal ExplainState with serialize=TEXT and timing/buffers off, so the
    // receiver callbacks can run without the syscache/printtup output machinery.
    unsafe fn make_es() -> *mut ExplainState {
        let es = palloc0(core::mem::size_of::<ExplainState>()) as *mut ExplainState;
        (*es).serialize = EXPLAIN_SERIALIZE_TEXT;
        (*es).timing = false;
        (*es).buffers = false;
        es
    }

    #[test]
    fn create_wires_four_fn_ptrs_and_dest() {
        unsafe {
            let es = make_es();
            let dr = CreateExplainSerializeDestReceiver(es);

            // The 4 vtable slots are wired (non-None).
            assert!((*dr).receiveSlot.is_some());
            assert!((*dr).rStartup.is_some());
            assert!((*dr).rShutdown.is_some());
            assert!((*dr).rDestroy.is_some());
            assert_eq!((*dr).mydest, CommandDest::DestExplainSerialize);

            // The es pointer is stored in the concrete receiver.
            let recv = dr as *mut SerializeDestReceiver;
            assert_eq!((*recv).es, es);

            pfree(dr as *mut c_void);
            pfree(es as *mut c_void);
        }
    }

    #[test]
    fn metrics_zero_after_startup() {
        unsafe {
            let es = make_es();
            let dr = CreateExplainSerializeDestReceiver(es);

            // Drive startup; metrics must be zero-initialized.
            (*dr).rStartup.unwrap()(dr, 0, null_mut());

            let recv = dr as *mut SerializeDestReceiver;
            assert_eq!((*recv).metrics.bytesSent, 0);
            // format selected from serialize == TEXT.
            assert_eq!((*recv).format, 0);
            // tmpcontext + buffer were set up.
            assert!(!(*recv).tmpcontext.is_null());
            assert!(!(*recv).buf.data.is_null());

            // Shutdown frees finfos/buf/tmpcontext.
            (*dr).rShutdown.unwrap()(dr);
            assert!((*recv).tmpcontext.is_null());
            assert!((*recv).buf.data.is_null());

            (*dr).rDestroy.unwrap()(dr);
            pfree(es as *mut c_void);
        }
    }

    #[test]
    fn get_metrics_zero_for_nonserialize_receiver() {
        unsafe {
            // A receiver with a different mydest returns all-zero metrics.
            let mut fake: DestReceiver = core::mem::zeroed();
            fake.mydest = CommandDest::DestNone;
            let m = GetSerializationMetrics(&mut fake);
            assert_eq!(m.bytesSent, 0);
        }
    }
}

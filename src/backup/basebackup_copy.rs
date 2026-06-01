//! basebackup_copy.c - send basebackup archives using COPY OUT.
//!
//! Source: postgres/src/backend/backup/basebackup_copy.c
//!
//! #include mapping:
//!   "postgres.h"                  -> use crate::prelude::*
//!   "access/tupdesc.h"            -> crate::access::common::tupdesc (PORTED)
//!   "backup/basebackup.h"         -> tablespaceinfo (STUB local; basebackup.c not ported)
//!   "backup/basebackup_sink.h"    -> crate::backup::basebackup_sink (PORTED)
//!   "catalog/pg_type_d.h"         -> crate::catalog::pg_type_d (PORTED: TEXTOID/INT8OID/OIDOID)
//!   "executor/executor.h"         -> crate::executor::executor (PORTED: TupOutputState etc.)
//!   "libpq/libpq.h"               -> crate::libpq::libpq (pq_putmessage / pq_flush_if_writable)
//!   "libpq/pqformat.h"            -> crate::libpq::pqformat (pq_begin/send/end message)
//!   "tcop/dest.h"                 -> crate::tcop::dest (CreateDestReceiver / DestRemoteSimple)
//!   "utils/builtins.h"            -> crate::utils::builtins::CStringGetTextDatum; psprintf (STUB local)
//!   "utils/timestamp.h"           -> GetCurrentTimestamp (PORTED); TimestampDifferenceMilliseconds (STUB)

use crate::prelude::*;

use crate::foreach;
use crate::current_cell;

use crate::access::common::tupdesc::{CreateTemplateTupleDesc, TupleDescInitBuiltinEntry};
use crate::backup::basebackup_sink::{bbsink, bbsink_ops, bbsink_state, TimeLineID, XLogRecPtr};
use crate::catalog::pg_type_d::{INT8OID, OIDOID, TEXTOID};
use crate::executor::executor::{
    begin_tup_output_tupdesc, do_tup_output, end_tup_output, TupOutputState,
};
use crate::executor::execTuples::TTSOpsVirtual;
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::libpq::libpq::{pq_flush_if_writable, pq_putmessage};
use crate::libpq::pqformat::{
    pq_beginmessage, pq_endmessage, pq_puttextmessage, pq_putemptymessage,
    pq_sendint16, pq_sendint64, pq_sendstring,
};
use crate::libpq::protocol::{
    PqMsg_CommandComplete, PqMsg_CopyData, PqMsg_CopyDone, PqMsg_CopyOutResponse,
};
use crate::nodes::pg_list::{lfirst, list_nth, List};
use crate::tcop::dest::{CreateDestReceiver, DestReceiver};
use crate::tcop::dest::CommandDest::DestRemoteSimple;
use crate::access::attnum::AttrNumber;
use crate::access::common::tupdesc::TupleDesc;
use crate::miscadmin::TimestampTz;
use crate::utils::activity::pgstat::GetCurrentTimestamp;
use crate::utils::builtins::CStringGetTextDatum;

use crate::pg_config::MAXIMUM_ALIGNOF;

// ---------------------------------------------------------------------------
// pqformat.h declares pq_sendbyte as a macro forwarding to pq_sendint8. The
// ported pqformat.rs exposes pq_sendint8 but not pq_sendbyte; provide the
// macro-equivalent locally.
// ---------------------------------------------------------------------------
#[inline]
unsafe fn pq_sendbyte_impl(buf: StringInfo, byt: uint8) {
    crate::libpq::pqformat::pq_sendint8(buf, byt)
}

// ---------------------------------------------------------------------------
// backup/basebackup.h: tablespaceinfo. basebackup.c is not yet ported; mirror
// the struct so we can walk the tablespaces list. STUB.
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct tablespaceinfo {
    pub oid: Oid,
    pub path: *mut c_char,
    pub rpath: *mut c_char,
    pub size: int64,
}

// ---------------------------------------------------------------------------
// utils/timestamp.h: TimestampDifferenceMilliseconds. timestamp.c is not yet
// ported. STUB.
// ---------------------------------------------------------------------------
unsafe fn TimestampDifferenceMilliseconds(start_time: TimestampTz, stop_time: TimestampTz) -> c_long {
    let _ = (start_time, stop_time);
    unimplemented!("TimestampDifferenceMilliseconds: utils/adt/timestamp.c not yet translated")
}

// ---------------------------------------------------------------------------
// utils/builtins.h: psprintf. mcxt-backed psprintf is not yet ported. STUB.
// Only the "%X/%X" two-argument form is used here.
// ---------------------------------------------------------------------------
unsafe fn psprintf_XX(hi: uint32, lo: uint32) -> *mut c_char {
    let _ = (hi, lo);
    unimplemented!("psprintf: mcxt-backed psprintf not yet translated")
}

// LSN_FORMAT_ARGS(lsn): (hi32, lo32). Mirrors access/xlogdefs.h.
#[inline]
fn LSN_FORMAT_ARGS(lsn: XLogRecPtr) -> (uint32, uint32) {
    ((lsn >> 32) as uint32, lsn as uint32)
}

#[repr(C)]
pub struct bbsink_copystream {
    /* Common information for all types of sink. */
    pub base: bbsink,

    /* Are we sending the archives to the client, or somewhere else? */
    pub send_to_client: bool,

    /*
     * Protocol message buffer. We assemble CopyData protocol messages by
     * setting the first character of this buffer to 'd' (archive or manifest
     * data) and then making base.bbs_buffer point to the second character so
     * that the rest of the data gets copied into the message just where we
     * want it.
     */
    pub msgbuffer: *mut c_char,

    /*
     * When did we last report progress to the client, and how much progress
     * did we report?
     */
    pub last_progress_report_time: TimestampTz,
    pub bytes_done_at_last_time_check: uint64,
}

/*
 * We don't want to send progress messages to the client excessively
 * frequently. Ideally, we'd like to send a message when the time since the
 * last message reaches PROGRESS_REPORT_MILLISECOND_THRESHOLD, but checking
 * the system time every time we send a tiny bit of data seems too expensive.
 * So we only check it after the number of bytes sine the last check reaches
 * PROGRESS_REPORT_BYTE_INTERVAL.
 */
const PROGRESS_REPORT_BYTE_INTERVAL: uint64 = 65536;
const PROGRESS_REPORT_MILLISECOND_THRESHOLD: c_long = 1000;

static bbsink_copystream_ops: bbsink_ops = bbsink_ops {
    begin_backup: Some(bbsink_copystream_begin_backup),
    begin_archive: Some(bbsink_copystream_begin_archive),
    archive_contents: Some(bbsink_copystream_archive_contents),
    end_archive: Some(bbsink_copystream_end_archive),
    begin_manifest: Some(bbsink_copystream_begin_manifest),
    manifest_contents: Some(bbsink_copystream_manifest_contents),
    end_manifest: Some(bbsink_copystream_end_manifest),
    end_backup: Some(bbsink_copystream_end_backup),
    cleanup: Some(bbsink_copystream_cleanup),
};

/*
 * Create a new 'copystream' bbsink.
 */
pub unsafe fn bbsink_copystream_new(send_to_client: bool) -> *mut bbsink {
    let sink = palloc0(core::mem::size_of::<bbsink_copystream>()) as *mut bbsink_copystream;

    *(&mut (*sink).base.bbs_ops as *mut *const bbsink_ops) = &bbsink_copystream_ops;
    (*sink).send_to_client = send_to_client;

    /* Set up for periodic progress reporting. */
    (*sink).last_progress_report_time = GetCurrentTimestamp();
    (*sink).bytes_done_at_last_time_check = 0u64;

    &mut (*sink).base
}

/*
 * Send start-of-backup wire protocol messages.
 */
unsafe fn bbsink_copystream_begin_backup(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_copystream;
    let state: *mut bbsink_state = (*sink).bbs_state;

    /*
     * Initialize buffer. We ultimately want to send the archive and manifest
     * data by means of CopyData messages where the payload portion of each
     * message begins with a type byte. However, basebackup.c expects the
     * buffer to be aligned, so we can't just allocate one extra byte for the
     * type byte. Instead, allocate enough extra bytes that the portion of the
     * buffer we reveal to our callers can be aligned, while leaving room to
     * slip the type byte in just beforehand.  That will allow us to ship the
     * data with a single call to pq_putmessage and without needing any extra
     * copying.
     */
    let buf = palloc((*mysink).base.bbs_buffer_length + MAXIMUM_ALIGNOF) as *mut c_char;
    (*mysink).msgbuffer = buf.add(MAXIMUM_ALIGNOF - 1);
    (*mysink).base.bbs_buffer = buf.add(MAXIMUM_ALIGNOF);
    *(*mysink).msgbuffer.add(0) = b'd' as c_char; /* archive or manifest data */

    /* Tell client the backup start location. */
    SendXlogRecPtrResult((*state).startptr, (*state).starttli);

    /* Send client a list of tablespaces. */
    SendTablespaceList((*state).tablespaces);

    /* Send a CommandComplete message */
    pq_puttextmessage(PqMsg_CommandComplete as c_char, c"SELECT".as_ptr());

    /* Begin COPY stream. This will be used for all archives + manifest. */
    SendCopyOutResponse();
}

/*
 * Send a CopyData message announcing the beginning of a new archive.
 */
unsafe fn bbsink_copystream_begin_archive(sink: *mut bbsink, archive_name: *const c_char) {
    let state: *mut bbsink_state = (*sink).bbs_state;
    let ti: *mut tablespaceinfo;
    let mut buf: StringInfoData = core::mem::zeroed();

    ti = list_nth((*state).tablespaces, (*state).tablespace_num) as *mut tablespaceinfo;
    pq_beginmessage(&mut buf, PqMsg_CopyData as c_char);
    pq_sendbyte_impl(&mut buf, b'n'); /* New archive */
    pq_sendstring(&mut buf, archive_name);
    pq_sendstring(
        &mut buf,
        if (*ti).path.is_null() {
            c"".as_ptr()
        } else {
            (*ti).path
        },
    );
    pq_endmessage(&mut buf);
}

/*
 * Send a CopyData message containing a chunk of archive content.
 */
unsafe fn bbsink_copystream_archive_contents(sink: *mut bbsink, len: Size) {
    let mysink = sink as *mut bbsink_copystream;
    let state: *mut bbsink_state = (*mysink).base.bbs_state;
    let mut buf: StringInfoData = core::mem::zeroed();
    let targetbytes: uint64;

    /* Send the archive content to the client, if appropriate. */
    if (*mysink).send_to_client {
        /* Add one because we're also sending a leading type byte. */
        pq_putmessage(b'd' as c_char, (*mysink).msgbuffer, len + 1);
    }

    /* Consider whether to send a progress report to the client. */
    targetbytes = (*mysink).bytes_done_at_last_time_check + PROGRESS_REPORT_BYTE_INTERVAL;
    if targetbytes <= (*state).bytes_done {
        let now: TimestampTz = GetCurrentTimestamp();
        let ms: c_long;

        /*
         * OK, we've sent a decent number of bytes, so check the system time
         * to see whether we're due to send a progress report.
         */
        (*mysink).bytes_done_at_last_time_check = (*state).bytes_done;
        ms = TimestampDifferenceMilliseconds((*mysink).last_progress_report_time, now);

        /*
         * Send a progress report if enough time has passed. Also send one if
         * the system clock was set backward, so that such occurrences don't
         * have the effect of suppressing further progress messages.
         */
        if ms >= PROGRESS_REPORT_MILLISECOND_THRESHOLD || now < (*mysink).last_progress_report_time {
            (*mysink).last_progress_report_time = now;

            pq_beginmessage(&mut buf, PqMsg_CopyData as c_char);
            pq_sendbyte_impl(&mut buf, b'p'); /* Progress report */
            pq_sendint64(&mut buf, (*state).bytes_done);
            pq_endmessage(&mut buf);
            pq_flush_if_writable();
        }
    }
}

/*
 * We don't need to explicitly signal the end of the archive; the client
 * will figure out that we've reached the end when we begin the next one,
 * or begin the manifest, or end the COPY stream. However, this seems like
 * a good time to force out a progress report. One reason for that is that
 * if this is the last archive, and we don't force a progress report now,
 * the client will never be told that we sent all the bytes.
 */
unsafe fn bbsink_copystream_end_archive(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_copystream;
    let state: *mut bbsink_state = (*mysink).base.bbs_state;
    let mut buf: StringInfoData = core::mem::zeroed();

    (*mysink).bytes_done_at_last_time_check = (*state).bytes_done;
    (*mysink).last_progress_report_time = GetCurrentTimestamp();
    pq_beginmessage(&mut buf, PqMsg_CopyData as c_char);
    pq_sendbyte_impl(&mut buf, b'p'); /* Progress report */
    pq_sendint64(&mut buf, (*state).bytes_done);
    pq_endmessage(&mut buf);
    pq_flush_if_writable();
}

/*
 * Send a CopyData message announcing the beginning of the backup manifest.
 */
unsafe fn bbsink_copystream_begin_manifest(_sink: *mut bbsink) {
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_beginmessage(&mut buf, PqMsg_CopyData as c_char);
    pq_sendbyte_impl(&mut buf, b'm'); /* Manifest */
    pq_endmessage(&mut buf);
}

/*
 * Each chunk of manifest data is sent using a CopyData message.
 */
unsafe fn bbsink_copystream_manifest_contents(sink: *mut bbsink, len: Size) {
    let mysink = sink as *mut bbsink_copystream;

    if (*mysink).send_to_client {
        /* Add one because we're also sending a leading type byte. */
        pq_putmessage(b'd' as c_char, (*mysink).msgbuffer, len + 1);
    }
}

/*
 * We don't need an explicit terminator for the backup manifest.
 */
unsafe fn bbsink_copystream_end_manifest(_sink: *mut bbsink) {
    /* Do nothing. */
}

/*
 * Send end-of-backup wire protocol messages.
 */
unsafe fn bbsink_copystream_end_backup(_sink: *mut bbsink, endptr: XLogRecPtr, endtli: TimeLineID) {
    SendCopyDone();
    SendXlogRecPtrResult(endptr, endtli);
}

/*
 * Cleanup.
 */
unsafe fn bbsink_copystream_cleanup(_sink: *mut bbsink) {
    /* Nothing to do. */
}

/*
 * Send a CopyOutResponse message.
 */
unsafe fn SendCopyOutResponse() {
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_beginmessage(&mut buf, PqMsg_CopyOutResponse as c_char);
    pq_sendbyte_impl(&mut buf, 0); /* overall format */
    pq_sendint16(&mut buf, 0); /* natts */
    pq_endmessage(&mut buf);
}

/*
 * Send a CopyDone message.
 */
unsafe fn SendCopyDone() {
    pq_putemptymessage(PqMsg_CopyDone as c_char);
}

/*
 * Send a single resultset containing just a single
 * XLogRecPtr record (in text format)
 */
unsafe fn SendXlogRecPtrResult(ptr: XLogRecPtr, tli: TimeLineID) {
    let dest: *mut DestReceiver;
    let tstate: *mut TupOutputState;
    let tupdesc: TupleDesc;
    let mut values: [Datum; 2] = [0; 2];
    let nulls: [bool; 2] = [false; 2];

    dest = CreateDestReceiver(DestRemoteSimple);

    tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitBuiltinEntry(tupdesc, 1 as AttrNumber, c"recptr".as_ptr(), TEXTOID, -1, 0);

    /*
     * int8 may seem like a surprising data type for this, but in theory int4
     * would not be wide enough for this, as TimeLineID is unsigned.
     */
    TupleDescInitBuiltinEntry(tupdesc, 2 as AttrNumber, c"tli".as_ptr(), INT8OID, -1, 0);

    /* send RowDescription */
    tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

    /* Data row */
    let (hi, lo) = LSN_FORMAT_ARGS(ptr);
    values[0] = CStringGetTextDatum(psprintf_XX(hi, lo));
    values[1] = Int64GetDatum(tli as int64);
    do_tup_output(tstate, values.as_ptr(), nulls.as_ptr());

    end_tup_output(tstate);

    /* Send a CommandComplete message */
    pq_puttextmessage(PqMsg_CommandComplete as c_char, c"SELECT".as_ptr());
}

/*
 * Send a result set via libpq describing the tablespace list.
 */
unsafe fn SendTablespaceList(tablespaces: *mut List) {
    let dest: *mut DestReceiver;
    let tstate: *mut TupOutputState;
    let tupdesc: TupleDesc;

    dest = CreateDestReceiver(DestRemoteSimple);

    tupdesc = CreateTemplateTupleDesc(3);
    TupleDescInitBuiltinEntry(tupdesc, 1 as AttrNumber, c"spcoid".as_ptr(), OIDOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 2 as AttrNumber, c"spclocation".as_ptr(), TEXTOID, -1, 0);
    TupleDescInitBuiltinEntry(tupdesc, 3 as AttrNumber, c"size".as_ptr(), INT8OID, -1, 0);

    /* send RowDescription */
    tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

    /* Construct and send the directory information */
    foreach!(lc, tablespaces, {
        let ti = lfirst(current_cell!(lc)) as *mut tablespaceinfo;
        let mut values: [Datum; 3] = [0; 3];
        let mut nulls: [bool; 3] = [false; 3];

        /* Send one datarow message */
        if (*ti).path.is_null() {
            nulls[0] = true;
            nulls[1] = true;
        } else {
            values[0] = ObjectIdGetDatum((*ti).oid);
            values[1] = CStringGetTextDatum((*ti).path);
        }
        if (*ti).size >= 0 {
            values[2] = Int64GetDatum((*ti).size / 1024);
        } else {
            nulls[2] = true;
        }

        do_tup_output(tstate, values.as_ptr(), nulls.as_ptr());
    });

    end_tup_output(tstate);
}

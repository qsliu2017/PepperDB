//! pg_controldata.c - expose contents of the control data file via SQL functions.

use crate::prelude::*;
type pg_time_t = i64;

use crate::access::common::heaptuple::heap_form_tuple;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::access::transam::xlog_internal::{XLByteToSeg, XLogFileName, MAXFNAMELEN};
use crate::access::transam::xlogdefs::XLogSegNo;
use crate::access::transam::{EpochFromFullTransactionId, XidFromFullTransactionId};
use crate::catalog::pg_control::ControlFileData;
use crate::common::controldata_utils::get_controlfile;
use crate::miscadmin::DataDir;
use crate::postgres::{
    BoolGetDatum, Int32GetDatum, Int64GetDatum, ObjectIdGetDatum, TransactionIdGetDatum,
};
use crate::utils::adt::pg_lsn::LSNGetDatum;
use crate::utils::builtins::CStringGetTextDatum;
use crate::utils::fmgr::FunctionCallInfo;
use crate::PG_RETURN_DATUM;

// -------------------------------------------------------------------------
// Locally-stubbed dependencies (not yet ported)
// -------------------------------------------------------------------------

// TODO: port get_call_result_type (src/backend/utils/fmgr/funcapi.c)
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut crate::postgres_ext::Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> c_int {
    unimplemented!()
}
const TYPEFUNC_COMPOSITE: c_int = 1;

// TODO: port HeapTupleGetDatum (src/include/funcapi.h)
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!()
}

// TODO: port LWLockAcquire (src/backend/storage/lmgr/lwlock.c)
unsafe fn LWLockAcquire(_l: *mut c_void, _mode: c_int) -> bool {
    unimplemented!()
}

// TODO: port LWLockRelease (src/backend/storage/lmgr/lwlock.c)
unsafe fn LWLockRelease(_l: *mut c_void) {
    unimplemented!()
}

// TODO: port ControlFileLock (src/include/storage/lwlocknames.h)
static mut ControlFileLock: *mut c_void = core::ptr::null_mut();
const LW_SHARED: c_int = 1;

// TODO: port wal_segment_size GUC (src/backend/access/transam/xlog.c)
static mut wal_segment_size: c_int = 0;

// TODO: port time_t_to_timestamptz (src/backend/utils/adt/timestamp.c)
unsafe fn time_t_to_timestamptz(_tm: pg_time_t) -> TimestampTz {
    unimplemented!()
}

// TODO: port TimestampTzGetDatum (src/include/utils/timestamp.h)
fn TimestampTzGetDatum(x: TimestampTz) -> Datum {
    x as Datum
}
pub type TimestampTz = int64;

// TODO: port psprintf (src/backend/utils/mmgr/mcxt.c)
unsafe fn psprintf_u_u(a: uint32, b: uint32) -> *mut c_char {
    let _ = (a, b);
    unimplemented!()
}

// -------------------------------------------------------------------------

pub unsafe fn pg_control_system(fcinfo: FunctionCallInfo) -> Datum {
    let mut values: [Datum; 4] = [0; 4];
    let mut nulls: [bool; 4] = [false; 4];
    let mut tupdesc: TupleDesc = core::ptr::null_mut();
    let htup: HeapTuple;
    let ControlFile: *mut ControlFileData;
    let mut crc_ok: bool = false;

    if get_call_result_type(fcinfo, core::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    /* read the control file */
    LWLockAcquire(ControlFileLock, LW_SHARED);
    ControlFile = get_controlfile(DataDir, &mut crc_ok) as *mut _;
    LWLockRelease(ControlFileLock);
    if !crc_ok {
        ereport!(
            ERROR,
            "calculated CRC checksum does not match value stored in file"
        );
    }

    values[0] = Int32GetDatum((*ControlFile).pg_control_version as int32);
    nulls[0] = false;

    values[1] = Int32GetDatum((*ControlFile).catalog_version_no as int32);
    nulls[1] = false;

    values[2] = Int64GetDatum((*ControlFile).system_identifier as int64);
    nulls[2] = false;

    values[3] = TimestampTzGetDatum(time_t_to_timestamptz((*ControlFile).time));
    nulls[3] = false;

    htup = heap_form_tuple(tupdesc, values.as_ptr(), nulls.as_ptr());

    PG_RETURN_DATUM!(HeapTupleGetDatum(htup));
}

pub unsafe fn pg_control_checkpoint(fcinfo: FunctionCallInfo) -> Datum {
    let mut values: [Datum; 18] = [0; 18];
    let mut nulls: [bool; 18] = [false; 18];
    let mut tupdesc: TupleDesc = core::ptr::null_mut();
    let htup: HeapTuple;
    let ControlFile: *mut ControlFileData;
    let mut segno: XLogSegNo = 0;
    let mut xlogfilename: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut crc_ok: bool = false;

    if get_call_result_type(fcinfo, core::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    /* Read the control file. */
    LWLockAcquire(ControlFileLock, LW_SHARED);
    ControlFile = get_controlfile(DataDir, &mut crc_ok) as *mut _;
    LWLockRelease(ControlFileLock);
    if !crc_ok {
        ereport!(
            ERROR,
            "calculated CRC checksum does not match value stored in file"
        );
    }

    /*
     * Calculate name of the WAL file containing the latest checkpoint's REDO
     * start point.
     */
    XLByteToSeg(
        (*ControlFile).checkPointCopy.redo,
        &mut segno,
        wal_segment_size,
    );
    XLogFileName(
        xlogfilename.as_mut_ptr(),
        (*ControlFile).checkPointCopy.ThisTimeLineID,
        segno,
        wal_segment_size,
    );

    /* Populate the values and null arrays */
    values[0] = LSNGetDatum((*ControlFile).checkPoint);
    nulls[0] = false;

    values[1] = LSNGetDatum((*ControlFile).checkPointCopy.redo);
    nulls[1] = false;

    values[2] = CStringGetTextDatum(xlogfilename.as_ptr());
    nulls[2] = false;

    values[3] = Int32GetDatum((*ControlFile).checkPointCopy.ThisTimeLineID as int32);
    nulls[3] = false;

    values[4] = Int32GetDatum((*ControlFile).checkPointCopy.PrevTimeLineID as int32);
    nulls[4] = false;

    values[5] = BoolGetDatum((*ControlFile).checkPointCopy.fullPageWrites);
    nulls[5] = false;

    values[6] = CStringGetTextDatum(psprintf_u_u(
        EpochFromFullTransactionId((*ControlFile).checkPointCopy.nextXid),
        XidFromFullTransactionId((*ControlFile).checkPointCopy.nextXid),
    ));
    nulls[6] = false;

    values[7] = ObjectIdGetDatum((*ControlFile).checkPointCopy.nextOid);
    nulls[7] = false;

    values[8] = TransactionIdGetDatum((*ControlFile).checkPointCopy.nextMulti);
    nulls[8] = false;

    values[9] = TransactionIdGetDatum((*ControlFile).checkPointCopy.nextMultiOffset);
    nulls[9] = false;

    values[10] = TransactionIdGetDatum((*ControlFile).checkPointCopy.oldestXid);
    nulls[10] = false;

    values[11] = ObjectIdGetDatum((*ControlFile).checkPointCopy.oldestXidDB);
    nulls[11] = false;

    values[12] = TransactionIdGetDatum((*ControlFile).checkPointCopy.oldestActiveXid);
    nulls[12] = false;

    values[13] = TransactionIdGetDatum((*ControlFile).checkPointCopy.oldestMulti);
    nulls[13] = false;

    values[14] = ObjectIdGetDatum((*ControlFile).checkPointCopy.oldestMultiDB);
    nulls[14] = false;

    values[15] = TransactionIdGetDatum((*ControlFile).checkPointCopy.oldestCommitTsXid);
    nulls[15] = false;

    values[16] = TransactionIdGetDatum((*ControlFile).checkPointCopy.newestCommitTsXid);
    nulls[16] = false;

    values[17] = TimestampTzGetDatum(time_t_to_timestamptz((*ControlFile).checkPointCopy.time));
    nulls[17] = false;

    htup = heap_form_tuple(tupdesc, values.as_ptr(), nulls.as_ptr());

    PG_RETURN_DATUM!(HeapTupleGetDatum(htup));
}

pub unsafe fn pg_control_recovery(fcinfo: FunctionCallInfo) -> Datum {
    let mut values: [Datum; 5] = [0; 5];
    let mut nulls: [bool; 5] = [false; 5];
    let mut tupdesc: TupleDesc = core::ptr::null_mut();
    let htup: HeapTuple;
    let ControlFile: *mut ControlFileData;
    let mut crc_ok: bool = false;

    if get_call_result_type(fcinfo, core::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    /* read the control file */
    LWLockAcquire(ControlFileLock, LW_SHARED);
    ControlFile = get_controlfile(DataDir, &mut crc_ok) as *mut _;
    LWLockRelease(ControlFileLock);
    if !crc_ok {
        ereport!(
            ERROR,
            "calculated CRC checksum does not match value stored in file"
        );
    }

    values[0] = LSNGetDatum((*ControlFile).minRecoveryPoint);
    nulls[0] = false;

    values[1] = Int32GetDatum((*ControlFile).minRecoveryPointTLI as int32);
    nulls[1] = false;

    values[2] = LSNGetDatum((*ControlFile).backupStartPoint);
    nulls[2] = false;

    values[3] = LSNGetDatum((*ControlFile).backupEndPoint);
    nulls[3] = false;

    values[4] = BoolGetDatum((*ControlFile).backupEndRequired);
    nulls[4] = false;

    htup = heap_form_tuple(tupdesc, values.as_ptr(), nulls.as_ptr());

    PG_RETURN_DATUM!(HeapTupleGetDatum(htup));
}

pub unsafe fn pg_control_init(fcinfo: FunctionCallInfo) -> Datum {
    let mut values: [Datum; 12] = [0; 12];
    let mut nulls: [bool; 12] = [false; 12];
    let mut tupdesc: TupleDesc = core::ptr::null_mut();
    let htup: HeapTuple;
    let ControlFile: *mut ControlFileData;
    let mut crc_ok: bool = false;

    if get_call_result_type(fcinfo, core::ptr::null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    /* read the control file */
    LWLockAcquire(ControlFileLock, LW_SHARED);
    ControlFile = get_controlfile(DataDir, &mut crc_ok) as *mut _;
    LWLockRelease(ControlFileLock);
    if !crc_ok {
        ereport!(
            ERROR,
            "calculated CRC checksum does not match value stored in file"
        );
    }

    values[0] = Int32GetDatum((*ControlFile).maxAlign as int32);
    nulls[0] = false;

    values[1] = Int32GetDatum((*ControlFile).blcksz as int32);
    nulls[1] = false;

    values[2] = Int32GetDatum((*ControlFile).relseg_size as int32);
    nulls[2] = false;

    values[3] = Int32GetDatum((*ControlFile).xlog_blcksz as int32);
    nulls[3] = false;

    values[4] = Int32GetDatum((*ControlFile).xlog_seg_size as int32);
    nulls[4] = false;

    values[5] = Int32GetDatum((*ControlFile).nameDataLen as int32);
    nulls[5] = false;

    values[6] = Int32GetDatum((*ControlFile).indexMaxKeys as int32);
    nulls[6] = false;

    values[7] = Int32GetDatum((*ControlFile).toast_max_chunk_size as int32);
    nulls[7] = false;

    values[8] = Int32GetDatum((*ControlFile).loblksize as int32);
    nulls[8] = false;

    values[9] = BoolGetDatum((*ControlFile).float8ByVal);
    nulls[9] = false;

    values[10] = Int32GetDatum((*ControlFile).data_checksum_version as int32);
    nulls[10] = false;

    values[11] = BoolGetDatum((*ControlFile).default_char_signedness);
    nulls[11] = false;

    htup = heap_form_tuple(tupdesc, values.as_ptr(), nulls.as_ptr());

    PG_RETURN_DATUM!(HeapTupleGetDatum(htup));
}

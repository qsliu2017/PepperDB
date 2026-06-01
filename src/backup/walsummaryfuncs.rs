//! walsummaryfuncs.c - SQL-callable functions for accessing WAL summary data.

use crate::prelude::*;

use crate::access::common::heaptuple::heap_form_tuple;
use crate::access::common::tupdesc::TupleDesc;
use crate::access::htup_details::HeapTuple;
use crate::access::transam::xlogdefs::{InvalidXLogRecPtr, TimeLineID, XLogRecPtr};
use crate::common::blkreftable::{
    BlockRefTableReader, BlockRefTableReaderGetBlocks, BlockRefTableReaderNextRelation,
    CreateBlockRefTableReader, DestroyBlockRefTableReader, RelFileLocator,
};
use crate::common::relpath::ForkNumber;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::nodes::execnodes::{ReturnSetInfo, Tuplestorestate};
use crate::nodes::pg_list::{List, ListCell};
use crate::postgres::{
    BoolGetDatum, Int16GetDatum, Int32GetDatum, Int64GetDatum, ObjectIdGetDatum,
};
use crate::storage::block::{BlockNumber, BlockNumberIsValid};
use crate::utils::adt::pg_lsn::LSNGetDatum;
use crate::utils::fmgr::FunctionCallInfo;
use crate::{PG_GETARG_INT64, PG_RETURN_DATUM};

const NUM_WS_ATTS: usize = 3;
const NUM_SUMMARY_ATTS: usize = 6;
const NUM_STATE_ATTS: usize = 4;
const MAX_BLOCKS_PER_CALL: c_int = 256;

/*
 * A WAL summary file, identified by timeline plus start and end LSN.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WalSummaryFile {
    pub tli: TimeLineID,
    pub start_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct WalSummaryIO {
    pub file: File,
    pub filepos: off_t,
}

// File / off_t come from the virtual file descriptor layer (storage/fd.h),
// not yet ported.
pub type File = c_int;
pub type off_t = i64;

// TODO: port GetWalSummaries (src/backend/backup/walsummary.c)
unsafe fn GetWalSummaries(
    _tli: TimeLineID,
    _start_lsn: XLogRecPtr,
    _end_lsn: XLogRecPtr,
) -> *mut List {
    unimplemented!()
}

// TODO: port OpenWalSummaryFile (src/backend/backup/walsummary.c)
unsafe fn OpenWalSummaryFile(_ws: *mut WalSummaryFile, _missing_ok: bool) -> File {
    unimplemented!()
}

// TODO: port ReadWalSummary (src/backend/backup/walsummary.c)
unsafe fn ReadWalSummary(_io: *mut c_void, _data: *mut c_void, _length: c_int) -> c_int {
    unimplemented!()
}

// TODO: port ReportWalSummaryError (src/backend/backup/walsummary.c)
unsafe fn ReportWalSummaryError(_callback_arg: *mut c_void, _msg: &str) -> ! {
    unimplemented!()
}

// TODO: port GetWalSummarizerState (src/backend/postmaster/walsummarizer.c)
unsafe fn GetWalSummarizerState(
    _summarized_tli: *mut TimeLineID,
    _summarized_lsn: *mut XLogRecPtr,
    _pending_lsn: *mut XLogRecPtr,
    _summarizer_pid: *mut c_int,
) {
    unimplemented!()
}

// TODO: port InitMaterializedSRF (src/backend/utils/fmgr/funcapi.c)
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!()
}

// TODO: port get_call_result_type (src/backend/utils/fmgr/funcapi.c)
unsafe fn get_call_result_type(
    _fcinfo: FunctionCallInfo,
    _resultTypeId: *mut crate::postgres_ext::Oid,
    _resultTupleDesc: *mut TupleDesc,
) -> c_int {
    unimplemented!()
}
const TYPEFUNC_COMPOSITE: c_int = 1;

// TODO: port tuplestore_puttuple (src/backend/utils/sort/tuplestore.c)
unsafe fn tuplestore_puttuple(_state: *mut Tuplestorestate, _tuple: HeapTuple) {
    unimplemented!()
}

// TODO: port FilePathName (src/backend/storage/file/fd.c)
unsafe fn FilePathName(_file: File) -> *mut c_char {
    unimplemented!()
}

// TODO: port FileClose (src/backend/storage/file/fd.c)
unsafe fn FileClose(_file: File) {
    unimplemented!()
}

// TODO: port HeapTupleGetDatum (src/include/funcapi.h)
unsafe fn HeapTupleGetDatum(_tuple: HeapTuple) -> Datum {
    unimplemented!()
}

/*
 * List the WAL summary files available in pg_wal/summaries.
 */
pub unsafe fn pg_available_wal_summaries(fcinfo: FunctionCallInfo) -> Datum {
    let rsi: *mut ReturnSetInfo;
    let wslist: *mut List;
    let mut values: [Datum; NUM_WS_ATTS] = [0; NUM_WS_ATTS];
    let mut nulls: [bool; NUM_WS_ATTS] = [false; NUM_WS_ATTS];

    InitMaterializedSRF(fcinfo, 0);
    rsi = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    nulls.iter_mut().for_each(|n| *n = false);

    wslist = GetWalSummaries(0, InvalidXLogRecPtr, InvalidXLogRecPtr);
    // foreach(lc, wslist)
    let length: c_int = if wslist.is_null() { 0 } else { (*wslist).length };
    let elements: *mut ListCell = if wslist.is_null() {
        null_mut()
    } else {
        (*wslist).elements
    };
    let mut idx: c_int = 0;
    while idx < length {
        let lc: *mut ListCell = elements.offset(idx as isize);
        let ws: *mut WalSummaryFile = (*lc).ptr_value as *mut WalSummaryFile;
        let tuple: HeapTuple;

        CHECK_FOR_INTERRUPTS();

        values[0] = Int64GetDatum((*ws).tli as int64);
        values[1] = LSNGetDatum((*ws).start_lsn);
        values[2] = LSNGetDatum((*ws).end_lsn);

        tuple = heap_form_tuple((*rsi).setDesc, values.as_ptr(), nulls.as_ptr());
        tuplestore_puttuple((*rsi).setResult, tuple);

        idx += 1;
    }

    0 as Datum
}

/*
 * List the contents of a WAL summary file identified by TLI, start LSN,
 * and end LSN.
 */
pub unsafe fn pg_wal_summary_contents(fcinfo: FunctionCallInfo) -> Datum {
    let rsi: *mut ReturnSetInfo;
    let mut values: [Datum; NUM_SUMMARY_ATTS] = [0; NUM_SUMMARY_ATTS];
    let mut nulls: [bool; NUM_SUMMARY_ATTS] = [false; NUM_SUMMARY_ATTS];
    let mut ws: WalSummaryFile = core::mem::zeroed();
    let mut io: WalSummaryIO = core::mem::zeroed();
    let reader: *mut BlockRefTableReader;
    let raw_tli: int64;
    let mut rlocator: RelFileLocator = core::mem::zeroed();
    let mut forknum: ForkNumber = 0;
    let mut limit_block: BlockNumber = 0;

    InitMaterializedSRF(fcinfo, 0);
    rsi = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    nulls.iter_mut().for_each(|n| *n = false);

    /*
     * Since the timeline could at least in theory be more than 2^31, and
     * since we don't have unsigned types at the SQL level, it is passed as a
     * 64-bit integer. Test whether it's out of range.
     */
    raw_tli = PG_GETARG_INT64!(fcinfo, 0);
    if raw_tli < 1 || raw_tli > PG_INT32_MAX as int64 {
        ereport!(ERROR, format!("invalid timeline {}", raw_tli));
    }

    /* Prepare to read the specified WAL summary file. */
    ws.tli = raw_tli as TimeLineID;
    ws.start_lsn = PG_GETARG_LSN(fcinfo, 1);
    ws.end_lsn = PG_GETARG_LSN(fcinfo, 2);
    io.filepos = 0;
    io.file = OpenWalSummaryFile(&mut ws, false);
    reader = CreateBlockRefTableReader(
        ReadWalSummary,
        &mut io as *mut WalSummaryIO as *mut c_void,
        FilePathName(io.file),
        ReportWalSummaryError,
        null_mut(),
    );

    /* Loop over relation forks. */
    while BlockRefTableReaderNextRelation(reader, &mut rlocator, &mut forknum, &mut limit_block) {
        let mut blocks: [BlockNumber; MAX_BLOCKS_PER_CALL as usize] =
            [0; MAX_BLOCKS_PER_CALL as usize];
        let mut tuple: HeapTuple;

        CHECK_FOR_INTERRUPTS();

        values[0] = ObjectIdGetDatum(rlocator.relNumber);
        values[1] = ObjectIdGetDatum(rlocator.spcOid);
        values[2] = ObjectIdGetDatum(rlocator.dbOid);
        values[3] = Int16GetDatum(forknum as int16);

        /*
         * If the limit block is not InvalidBlockNumber, emit an extra row
         * with that block number and limit_block = true.
         *
         * There is no point in doing this when the limit_block is
         * InvalidBlockNumber, because no block with that number or any higher
         * number can ever exist.
         */
        if BlockNumberIsValid(limit_block) {
            values[4] = Int64GetDatum(limit_block as int64);
            values[5] = BoolGetDatum(true);

            tuple = heap_form_tuple((*rsi).setDesc, values.as_ptr(), nulls.as_ptr());
            tuplestore_puttuple((*rsi).setResult, tuple);
        }

        /* Loop over blocks within the current relation fork. */
        loop {
            let nblocks: c_uint;

            CHECK_FOR_INTERRUPTS();

            nblocks =
                BlockRefTableReaderGetBlocks(reader, blocks.as_mut_ptr(), MAX_BLOCKS_PER_CALL);
            if nblocks == 0 {
                break;
            }

            /*
             * For each block that we specifically know to have been modified,
             * emit a row with that block number and limit_block = false.
             */
            values[5] = BoolGetDatum(false);
            let mut i: c_uint = 0;
            while i < nblocks {
                values[4] = Int64GetDatum(blocks[i as usize] as int64);

                tuple = heap_form_tuple((*rsi).setDesc, values.as_ptr(), nulls.as_ptr());
                tuplestore_puttuple((*rsi).setResult, tuple);
                i += 1;
            }
        }
    }

    /* Cleanup */
    DestroyBlockRefTableReader(reader);
    FileClose(io.file);

    0 as Datum
}

/*
 * Returns information about the state of the WAL summarizer process.
 */
pub unsafe fn pg_get_wal_summarizer_state(fcinfo: FunctionCallInfo) -> Datum {
    let mut values: [Datum; NUM_STATE_ATTS] = [0; NUM_STATE_ATTS];
    let mut nulls: [bool; NUM_STATE_ATTS] = [false; NUM_STATE_ATTS];
    let mut summarized_tli: TimeLineID = 0;
    let mut summarized_lsn: XLogRecPtr = 0;
    let mut pending_lsn: XLogRecPtr = 0;
    let mut summarizer_pid: c_int = 0;
    let mut tupdesc: TupleDesc = null_mut();
    let htup: HeapTuple;

    GetWalSummarizerState(
        &mut summarized_tli,
        &mut summarized_lsn,
        &mut pending_lsn,
        &mut summarizer_pid,
    );

    if get_call_result_type(fcinfo, null_mut(), &mut tupdesc) != TYPEFUNC_COMPOSITE {
        elog!(ERROR, "return type must be a row type");
    }

    nulls.iter_mut().for_each(|n| *n = false);

    values[0] = Int64GetDatum(summarized_tli as int64);
    values[1] = LSNGetDatum(summarized_lsn);
    values[2] = LSNGetDatum(pending_lsn);

    if summarizer_pid < 0 {
        nulls[3] = true;
    } else {
        values[3] = Int32GetDatum(summarizer_pid);
    }

    htup = heap_form_tuple(tupdesc, values.as_ptr(), nulls.as_ptr());

    PG_RETURN_DATUM!(HeapTupleGetDatum(htup));
}

// TODO: port PG_GETARG_LSN (src/include/utils/pg_lsn.h) - reads a pg_lsn arg.
unsafe fn PG_GETARG_LSN(_fcinfo: FunctionCallInfo, _n: c_int) -> XLogRecPtr {
    unimplemented!()
}

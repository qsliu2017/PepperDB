//! xlogstats.rs - Functions for WAL Statistics.
//!
//! 1:1 translation of src/backend/access/transam/xlogstats.c, MERGED with the
//! XLogStats / XLogRecStats struct definitions and the MAX_XLINFO_TYPES macro
//! from src/include/access/xlogstats.h.
//!
//! Used by pg_waldump --stats to accumulate per-rmgr and per-record-type WAL
//! statistics.  We peek into xlogreader's private decoded backup blocks for the
//! bimg_len indicating the length of FPI data, exactly as the C code does.
//!
//! Copyright (c) 2022-2025, PostgreSQL Global Development Group

use crate::prelude::*;

use crate::access::transam::xlogreader::{
    RmgrId, XLogReaderState, XLogRecGetBlock, XLogRecGetInfo, XLogRecGetRmid, XLogRecGetTotalLen,
    XLogRecHasBlockImage, XLogRecHasBlockRef, XLogRecMaxBlockId, XLogRecPtr,
};

// ---------------------------------------------------------------------------
// Constants
//
// rmgr.h is not ported.  RM_NEXT_ID is currently 25 in PostgreSQL 18
// (RM_MAX_BUILTIN_ID == RM_NEXT_ID - 1 == 24, mirroring xlogreader.rs), so
// RM_MAX_ID == RM_NEXT_ID - 1 == 24.  RM_XACT_ID is the second builtin rmgr in
// rmgrlist.h (id 1).
// ---------------------------------------------------------------------------

/// rmgr.h: one past the last builtin RmgrId.
pub const RM_NEXT_ID: usize = 25;
/// rmgr.h: highest valid builtin RmgrId (RM_NEXT_ID - 1).
pub const RM_MAX_ID: usize = RM_NEXT_ID - 1;
/// rmgr.h: the transaction-commit/abort resource manager (rmgrlist.h id 1).
pub const RM_XACT_ID: RmgrId = 1;

/// xlogstats.h: sixteen possible per-record entries per RmgrId (the four bits
/// of xl_info that are the rmgr's domain).
pub const MAX_XLINFO_TYPES: usize = 16;

// ---------------------------------------------------------------------------
// xlogstats.h struct definitions (MERGED)
// ---------------------------------------------------------------------------

/// Statistics for a single (rmgr, record-type) bucket or a whole rmgr.
#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct XLogRecStats {
    pub count: uint64,
    pub rec_len: uint64,
    pub fpi_len: uint64,
}

/// Accumulated WAL statistics.
///
/// NOTE: the C header guards `startptr`/`endptr` behind `#ifdef FRONTEND`.
/// pg_waldump (a frontend) is the only consumer; we include them unconditionally
/// since this is a translation target rather than a server hot path.  They are
/// not touched by the functions below.
#[repr(C)]
pub struct XLogStats {
    pub count: uint64,
    pub startptr: XLogRecPtr,
    pub endptr: XLogRecPtr,
    pub rmgr_stats: [XLogRecStats; RM_MAX_ID + 1],
    pub record_stats: [[XLogRecStats; MAX_XLINFO_TYPES]; RM_MAX_ID + 1],
}

impl Default for XLogStats {
    fn default() -> Self {
        XLogStats {
            count: 0,
            startptr: 0,
            endptr: 0,
            rmgr_stats: [XLogRecStats::default(); RM_MAX_ID + 1],
            record_stats: [[XLogRecStats::default(); MAX_XLINFO_TYPES]; RM_MAX_ID + 1],
        }
    }
}

// ---------------------------------------------------------------------------
// xlogstats.c functions
// ---------------------------------------------------------------------------

/// Calculate the size of a record, split into !FPI and FPI parts.
///
/// # Safety
/// `record` must be a live reader whose current record is decoded.
pub unsafe fn XLogRecGetLen(
    record: *mut XLogReaderState,
    rec_len: *mut uint32,
    fpi_len: *mut uint32,
) {
    // Calculate the amount of FPI data in the record.
    //
    // XXX: We peek into xlogreader's private decoded backup blocks for the
    // bimg_len indicating the length of FPI data.
    *fpi_len = 0;
    let mut block_id: c_int = 0;
    while block_id <= XLogRecMaxBlockId(record) {
        let bid = block_id as uint8;
        if !XLogRecHasBlockRef(record, bid) {
            block_id += 1;
            continue;
        }

        if XLogRecHasBlockImage(record, bid) {
            *fpi_len += (*XLogRecGetBlock(record, bid)).bimg_len as uint32;
        }
        block_id += 1;
    }

    // Calculate the length of the record as the total length - the length of
    // all the block images.
    *rec_len = XLogRecGetTotalLen(record) - *fpi_len;
}

/// Store per-rmgr and per-record statistics for a given record.
///
/// # Safety
/// `stats` and `record` must be non-null; `record`'s current record decoded.
pub unsafe fn XLogRecStoreStats(stats: *mut XLogStats, record: *mut XLogReaderState) {
    Assert!(!stats.is_null() && !record.is_null());

    (*stats).count += 1;

    let rmid: RmgrId = XLogRecGetRmid(record);

    let mut rec_len: uint32 = 0;
    let mut fpi_len: uint32 = 0;
    XLogRecGetLen(record, &mut rec_len, &mut fpi_len);

    // Update per-rmgr statistics
    let rm = rmid as usize;
    (*stats).rmgr_stats[rm].count += 1;
    (*stats).rmgr_stats[rm].rec_len += rec_len as uint64;
    (*stats).rmgr_stats[rm].fpi_len += fpi_len as uint64;

    // Update per-record statistics, where the record is identified by a
    // combination of the RmgrId and the four bits of the xl_info field that
    // are the rmgr's domain (resulting in sixteen possible entries per RmgrId).
    let mut recid: uint8 = XLogRecGetInfo(record) >> 4;

    // XACT records need to be handled differently. Those records use the first
    // bit of those four bits for an optional flag variable and the following
    // three bits for the opcode. We filter opcode out of xl_info and use it as
    // the identifier of the record.
    if rmid == RM_XACT_ID {
        recid &= 0x07;
    }

    let ri = recid as usize;
    (*stats).record_stats[rm][ri].count += 1;
    (*stats).record_stats[rm][ri].rec_len += rec_len as uint64;
    (*stats).record_stats[rm][ri].fpi_len += fpi_len as uint64;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::transam::xlogreader::{
        DecodedBkpBlock, DecodedXLogRecord, XLogReaderState, XLogRecord,
    };
    use core::mem::MaybeUninit;

    // Build a minimal decoded record with `nblocks` (0 or 1) backup blocks.
    // The decoded struct's flexible blocks[] has one in-line slot, which is
    // enough to exercise a single FPI block.
    unsafe fn make_decoded(
        tot_len: u32,
        rmid: RmgrId,
        info: u8,
        block: Option<(bool, u16)>, // (has_image, bimg_len)
    ) -> Box<DecodedXLogRecord> {
        let mut dec: Box<DecodedXLogRecord> =
            Box::new(MaybeUninit::<DecodedXLogRecord>::zeroed().assume_init());

        let mut hdr: XLogRecord = MaybeUninit::zeroed().assume_init();
        hdr.xl_tot_len = tot_len;
        hdr.xl_info = info;
        hdr.xl_rmid = rmid;
        dec.header = hdr;

        match block {
            Some((has_image, bimg_len)) => {
                dec.max_block_id = 0;
                let blk: &mut DecodedBkpBlock = &mut dec.blocks[0];
                blk.in_use = true;
                blk.has_image = has_image;
                blk.bimg_len = bimg_len;
            }
            None => {
                dec.max_block_id = -1;
            }
        }
        dec
    }

    // A reader whose only meaningful field is `.record`.
    unsafe fn make_reader(dec: *mut DecodedXLogRecord) -> Box<XLogReaderState> {
        let mut st: Box<XLogReaderState> =
            Box::new(MaybeUninit::<XLogReaderState>::zeroed().assume_init());
        st.record = dec;
        st
    }

    #[test]
    fn store_stats_no_fpi_bumps_right_buckets() {
        unsafe {
            // rmid 5, info 0x30 -> recid = 3, total len 100, no block image.
            let mut dec = make_decoded(100, 5, 0x30, None);
            let mut st = make_reader(&mut *dec);
            let mut stats = XLogStats::default();

            XLogRecStoreStats(&mut stats, &mut *st);

            assert_eq!(stats.count, 1);
            assert_eq!(stats.rmgr_stats[5].count, 1);
            assert_eq!(stats.rmgr_stats[5].rec_len, 100);
            assert_eq!(stats.rmgr_stats[5].fpi_len, 0);
            assert_eq!(stats.record_stats[5][3].count, 1);
            assert_eq!(stats.record_stats[5][3].rec_len, 100);
            assert_eq!(stats.record_stats[5][3].fpi_len, 0);
            // nothing else touched
            assert_eq!(stats.record_stats[5][0].count, 0);
            assert_eq!(stats.rmgr_stats[4].count, 0);
        }
    }

    #[test]
    fn store_stats_with_fpi_splits_lengths() {
        unsafe {
            // total 200, one FPI block of 80 -> rec_len 120, fpi_len 80.
            let mut dec = make_decoded(200, 7, 0x10, Some((true, 80)));
            let mut st = make_reader(&mut *dec);
            let mut stats = XLogStats::default();

            XLogRecStoreStats(&mut stats, &mut *st);

            assert_eq!(stats.rmgr_stats[7].rec_len, 120);
            assert_eq!(stats.rmgr_stats[7].fpi_len, 80);
            assert_eq!(stats.record_stats[7][1].rec_len, 120);
            assert_eq!(stats.record_stats[7][1].fpi_len, 80);

            // A block ref present but no image contributes no FPI.
            let mut dec2 = make_decoded(50, 7, 0x10, Some((false, 80)));
            let mut st2 = make_reader(&mut *dec2);
            XLogRecStoreStats(&mut stats, &mut *st2);
            // second record adds rec_len 50, fpi_len 0
            assert_eq!(stats.rmgr_stats[7].count, 2);
            assert_eq!(stats.rmgr_stats[7].rec_len, 170);
            assert_eq!(stats.rmgr_stats[7].fpi_len, 80);
            assert_eq!(stats.record_stats[7][1].count, 2);
        }
    }

    #[test]
    fn xact_recid_masks_opcode_bits() {
        unsafe {
            // For RM_XACT_ID, recid = (info>>4) & 0x07.  info 0xF0 -> 0x0F & 0x07 = 7.
            let mut dec = make_decoded(40, RM_XACT_ID, 0xF0, None);
            let mut st = make_reader(&mut *dec);
            let mut stats = XLogStats::default();

            XLogRecStoreStats(&mut stats, &mut *st);

            let rm = RM_XACT_ID as usize;
            assert_eq!(stats.record_stats[rm][7].count, 1);
            assert_eq!(stats.record_stats[rm][7].rec_len, 40);
            // the un-masked bucket [15] must be untouched
            assert_eq!(stats.record_stats[rm][15].count, 0);
        }
    }

    #[test]
    fn get_len_only() {
        unsafe {
            let mut dec = make_decoded(300, 3, 0, Some((true, 256)));
            let mut st = make_reader(&mut *dec);
            let mut rec_len: uint32 = 0;
            let mut fpi_len: uint32 = 0;
            XLogRecGetLen(&mut *st, &mut rec_len, &mut fpi_len);
            assert_eq!(fpi_len, 256);
            assert_eq!(rec_len, 44);
        }
    }
}

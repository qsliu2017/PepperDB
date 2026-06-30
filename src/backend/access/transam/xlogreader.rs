//! Generic WAL reading facility. Translated from backend/access/transam/xlogreader.c.
//!
//! Given a starting position in the write-ahead log, this facility reads and
//! decodes WAL records one at a time. It validates each record's header and
//! checksum against the same on-disk layout the WAL insertion path produces --
//! the fixed `XLogRecord` header, the per-block headers (with optional full-page
//! image header, `RelFileLocator`, and `BlockNumber`), and the main-data id byte
//! -- and reassembles records that span page or segment boundaries. The decoded
//! result ([`DecodedXLogRecord`]) carries the main data and per-block payloads so
//! callers can replay or inspect a record without re-parsing it.
//!
//! The reader performs no I/O of its own. The caller supplies a page-read routine
//! that fetches a WAL page on demand; the reader drives that routine to obtain the
//! bytes it needs, then validates page headers and record contents. Records are
//! pulled with `read_record`, which returns the next decoded record or `None` at
//! the end of available WAL.
//!
//! In PepperDB the page-read routine is a synchronous closure carried on the
//! reader as a generic type parameter, monomorphized per caller rather than
//! invoked through dynamic dispatch. This keeps the decode loop pure CPU work and
//! leaves any waiting or asynchronous fetching to the caller, in keeping with the
//! single-process async design where the wait side is async and the parse side is
//! not. The recovery-oriented page readers (prefetching, reading from a running
//! server) are provided elsewhere and are not implemented here. `XLogReader` is
//! the concrete reader corresponding to the C `XLogReaderState`; the decoded
//! output is independent of the page-read closure type, so downstream replay code
//! never has to be generic over it.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use crate::access::rmgr::{rmgr_id_is_valid, RmgrId};
use crate::access::xlog_internal::{
    SizeOfXLogLongPHD, SizeOfXLogShortPHD, XLByteToSeg, XLogSegmentOffset, XlpFlags,
    XLOG_PAGE_MAGIC,
};
use crate::access::xlogdefs::{TimeLineID, XLogRecPtr, XLogSegNo, INVALID_XLOG_REC_PTR};
use crate::access::xlogreader::{DecodedBkpBlock, DecodedXLogRecord};
use crate::access::xlogrecord::{
    BkpImage, SizeOfXLogRecord, XLogRecord, BKPBLOCK_FORK_MASK, BKPBLOCK_HAS_DATA,
    BKPBLOCK_HAS_IMAGE, BKPBLOCK_SAME_REL, XLR_BLOCK_ID_DATA_LONG, XLR_BLOCK_ID_DATA_SHORT,
    XLR_BLOCK_ID_ORIGIN, XLR_BLOCK_ID_TOPLEVEL_XID, XLR_INFO_MASK, XLR_MAX_BLOCK_ID,
};
use crate::c::{TransactionId, MAXALIGN};
use crate::catalog::pg_control::XLOG_SWITCH;
use crate::common::relpath::ForkNumber;
use crate::pg_config::XLOG_BLCKSZ;
use crate::port::pg_crc32c::{comp_crc32c, fin_crc32c, init_crc32c};
use crate::storage::block::BlockNumber;
use crate::storage::buf::INVALID_BUFFER;
use crate::storage::relfilelocator::RelFileLocator;

const BLCKSZ: usize = XLOG_BLCKSZ as usize;
const BLCKSZ_U64: u64 = XLOG_BLCKSZ as u64;

/// The page-read callback the caller supplies (PG `XLogReaderRoutine.page_read`).
///
/// Fill `into` (which is `XLOG_BLCKSZ` long) with the WAL page starting at
/// `target_page_ptr` (always page-aligned), making at least `req_len` bytes valid.
/// Returns the number of valid bytes read (`>= req_len`, `<= XLOG_BLCKSZ`), or an
/// error string on failure. Synchronous by design: the reader's loop is CPU-only;
/// the async I/O that backs this in recovery is the caller's concern.
///
/// The reader is generic over the closure type `F: FnMut(XLogRecPtr, usize, &mut
/// [u8]) -> Result<usize, String>` (monomorphized, no `dyn` dispatch). Callers
/// that want to name the routine type without monomorphizing can use the boxed
/// [`PageReadFn`] alias -- `Box<dyn FnMut>: FnMut`, so it satisfies the bound too.
pub type PageReadFn = Box<dyn FnMut(XLogRecPtr, usize, &mut [u8]) -> Result<usize, String> + Send>;

/// The self-contained WAL reader (concrete image of the C `XLogReaderState` for
/// the read+decode path). Holds the caller's page-read routine (generic `F`, no
/// `dyn` dispatch), the current read/decode cursors, a one-page read buffer, an
/// expandable reassembly buffer for boundary-spanning records, and the most
/// recently decoded record.
pub struct XLogReader<F> {
    /// Caller-supplied page fetcher.
    page_read: F,

    /// WAL segment size (bytes); needed for the XLOG_SWITCH end-of-segment skip.
    wal_seg_size: u64,

    /// pg_control system identifier; cross-checked against long page headers when
    /// nonzero (0 = unset, skip the check, matching C `state->system_identifier`).
    system_identifier: u64,

    /// Segment currently cached in `read_buf` (C `state->seg.ws_segno`); used to
    /// trigger per-segment first-page long-header validation.
    seg_no: XLogSegNo,

    // ---- timeline-monotonicity tracking (C latestPagePtr / latestPageTLI) ----
    latest_page_ptr: XLogRecPtr,
    latest_page_tli: TimeLineID,

    /// Set when a contrecord was overwritten (C `overwrittenRecPtr`).
    pub overwritten_rec_ptr: XLogRecPtr,
    /// On a failed multi-page assembly, the aborted record start (C `abortedRecPtr`).
    pub aborted_rec_ptr: XLogRecPtr,
    /// On a failed multi-page assembly, the page missing the contrecord
    /// (C `missingContrecPtr`).
    pub missing_contrec_ptr: XLogRecPtr,

    // ---- read/decode cursors ----
    /// Start of the last record read (C `ReadRecPtr`).
    pub read_rec_ptr: XLogRecPtr,
    /// End+1 of the last record read (C `EndRecPtr`).
    pub end_rec_ptr: XLogRecPtr,
    /// Start of the last record decoded (C `DecodeRecPtr`).
    decode_rec_ptr: XLogRecPtr,
    /// End+1 of the last record decoded (C `NextRecPtr`).
    next_rec_ptr: XLogRecPtr,
    /// Beginning of the WAL record currently being read (C `currRecPtr`).
    curr_rec_ptr: XLogRecPtr,

    // ---- read-state cache (one WAL page) ----
    read_buf: Vec<u8>,
    /// Page address currently cached in `read_buf` (Invalid if none).
    read_buf_origin: XLogRecPtr,
    /// Valid bytes in `read_buf`.
    read_len: usize,

    /// Expandable buffer for a record that crosses page boundaries.
    read_record_buf: Vec<u8>,

    /// Most recently decoded record (C `record`).
    pub record: Option<Box<DecodedXLogRecord>>,

    pub errormsg: Option<String>,
}

impl<F> XLogReader<F>
where
    F: FnMut(XLogRecPtr, usize, &mut [u8]) -> Result<usize, String>,
{
    /// PG `XLogReaderAllocate`: build a reader for the given segment size, taking
    /// the caller's page-read routine (monomorphized; no boxing).
    pub fn new(wal_segment_size: u64, page_read: F) -> Self {
        Self {
            page_read,
            wal_seg_size: wal_segment_size,
            system_identifier: 0,
            seg_no: XLogSegNo(0),
            latest_page_ptr: INVALID_XLOG_REC_PTR,
            latest_page_tli: TimeLineID(0),
            overwritten_rec_ptr: INVALID_XLOG_REC_PTR,
            aborted_rec_ptr: INVALID_XLOG_REC_PTR,
            missing_contrec_ptr: INVALID_XLOG_REC_PTR,
            read_rec_ptr: INVALID_XLOG_REC_PTR,
            end_rec_ptr: INVALID_XLOG_REC_PTR,
            decode_rec_ptr: INVALID_XLOG_REC_PTR,
            next_rec_ptr: INVALID_XLOG_REC_PTR,
            curr_rec_ptr: INVALID_XLOG_REC_PTR,
            read_buf: vec![0u8; BLCKSZ],
            read_buf_origin: INVALID_XLOG_REC_PTR,
            read_len: 0,
            // C keeps space for at least two pages so a boundary-spanning header
            // can always be validated before the full record is reassembled.
            read_record_buf: Vec::with_capacity(BLCKSZ * 2),
            record: None,
            errormsg: None,
        }
    }

    /// PG `XLogBeginRead`: position the reader at `rec_ptr` (must be a valid
    /// record start or a page boundary).
    pub fn begin_read(&mut self, rec_ptr: XLogRecPtr) {
        debug_assert!(rec_ptr.0.is_multiple_of(BLCKSZ_U64) || rec_ptr.0.is_multiple_of(8));
        // Reset the decode cursors; the next read starts here, treated as random
        // access (no prev-link cross-check against a prior decode).
        self.decode_rec_ptr = INVALID_XLOG_REC_PTR;
        self.next_rec_ptr = rec_ptr;
        self.read_rec_ptr = INVALID_XLOG_REC_PTR;
        self.end_rec_ptr = INVALID_XLOG_REC_PTR;
        self.inval_read_state();
    }

    /// Set the pg_control system identifier cross-checked against long page
    /// headers (0 = skip, matching C `state->system_identifier`).
    pub fn set_system_identifier(&mut self, system_identifier: u64) {
        self.system_identifier = system_identifier;
    }

    fn inval_read_state(&mut self) {
        // C XLogReaderInvalReadState resets ws_segno too so we re-validate the
        // first page of the segment after a failure.
        self.read_buf_origin = INVALID_XLOG_REC_PTR;
        self.read_len = 0;
        self.seg_no = XLogSegNo(0);
    }

    fn report(&mut self, msg: String) {
        self.errormsg = Some(msg);
    }

    /// PG `ReadPageInternal`: ensure `read_buf` holds the page at `pageptr` with
    /// at least `req_len` valid bytes, calling the page-read routine on a miss.
    /// Validates the page header before returning. Returns the valid byte count.
    fn read_page_internal(&mut self, pageptr: XLogRecPtr, req_len: usize) -> Result<usize, String> {
        debug_assert!(pageptr.0.is_multiple_of(BLCKSZ_U64));

        // Cache hit: same page, enough bytes already valid.
        if self.read_buf_origin == pageptr && self.read_len >= req_len {
            return Ok(self.read_len);
        }

        let target_seg_no = XLogSegNo(XLByteToSeg(pageptr.0, self.wal_seg_size));
        let target_page_off = XLogSegmentOffset(pageptr.0, self.wal_seg_size) as usize;

        // Whenever we switch to a new WAL segment at a non-zero page offset, read
        // and validate the segment's first page (long header) so the per-segment
        // identification info is always checked once. (C ReadPageInternal.)
        if target_seg_no != self.seg_no && target_page_off != 0 {
            let seg_ptr = XLogRecPtr(pageptr.0 - target_page_off as u64);
            let mut buf = std::mem::take(&mut self.read_buf);
            let read = (self.page_read)(seg_ptr, BLCKSZ, &mut buf);
            self.read_buf = buf;
            let read = read?;
            if read < BLCKSZ {
                return Err(format!("could not read page at {:X}/{:X}", seg_ptr.0 >> 32, seg_ptr.0 as u32));
            }
            self.read_buf_origin = seg_ptr;
            self.read_len = read;
            self.validate_page_header(seg_ptr)?;
        }

        // Read at least a short page header so the header length is parseable.
        let want = req_len.max(SizeOfXLogShortPHD);
        let mut buf = std::mem::take(&mut self.read_buf);
        let read = (self.page_read)(pageptr, want, &mut buf);
        self.read_buf = buf;
        let mut read = read?;
        if read <= SizeOfXLogShortPHD || read < want {
            return Err(format!("could not read page at {:X}/{:X}", pageptr.0 >> 32, pageptr.0 as u32));
        }
        self.read_buf_origin = pageptr;
        self.read_len = read;

        // If the page has a long header and we read fewer bytes, re-read the full
        // header before validating (C ReadPageInternal lines 1090-1099).
        let hdr_size = self.page_header_size();
        if read < hdr_size {
            let mut buf = std::mem::take(&mut self.read_buf);
            let r = (self.page_read)(pageptr, hdr_size, &mut buf);
            self.read_buf = buf;
            read = r?;
            if read < hdr_size {
                return Err(format!("could not read page at {:X}/{:X}", pageptr.0 >> 32, pageptr.0 as u32));
            }
            self.read_len = read;
        }

        // Validate the page header now that the full header is in the buffer.
        self.validate_page_header(pageptr)?;

        self.seg_no = target_seg_no;
        Ok(read)
    }

    /// PG `XLogReaderValidatePageHeader`: magic, flag bits, long-header identity
    /// cross-checks, the offset==0-needs-long-header rule, pageaddr match, and TLI
    /// monotonicity.
    fn validate_page_header(&mut self, recptr: XLogRecPtr) -> Result<(), String> {
        debug_assert!(recptr.0.is_multiple_of(BLCKSZ_U64));
        let offset = XLogSegmentOffset(recptr.0, self.wal_seg_size);
        // Copy out every header field up front so no borrow of `read_buf` is held
        // across the `self.report` (&mut self) calls below.
        let buf = &self.read_buf;
        let magic = u16::from_ne_bytes([buf[0], buf[1]]);
        let info = u16::from_ne_bytes([buf[2], buf[3]]);
        let tli = TimeLineID(u32::from_ne_bytes(buf[4..8].try_into().unwrap()));
        let pageaddr = u64::from_ne_bytes(buf[8..16].try_into().unwrap());
        let sysid = u64::from_ne_bytes(buf[24..32].try_into().unwrap());
        let seg_size = u32::from_ne_bytes(buf[32..36].try_into().unwrap());
        let blcksz = u32::from_ne_bytes(buf[36..40].try_into().unwrap());

        if magic != XLOG_PAGE_MAGIC {
            let m = format!(
                "invalid magic number {magic:04X} in WAL segment, offset {offset}"
            );
            self.report(m.clone());
            return Err(m);
        }
        if info & !XlpFlags::ALL_FLAGS.bits() != 0 {
            let m = format!("invalid info bits {info:04X} in WAL segment, offset {offset}");
            self.report(m.clone());
            return Err(m);
        }

        if info & XlpFlags::LONG_HEADER.bits() != 0 {
            // Long header: cross-check the identification info (xlp_sysid,
            // xlp_seg_size, xlp_xlog_blcksz) read above.
            if self.system_identifier != 0 && sysid != self.system_identifier {
                let m = format!(
                    "WAL file is from different database system: WAL file database system identifier is {sysid}, pg_control database system identifier is {}",
                    self.system_identifier
                );
                self.report(m.clone());
                return Err(m);
            }
            if u64::from(seg_size) != self.wal_seg_size {
                let m = "WAL file is from different database system: incorrect segment size in page header".to_string();
                self.report(m.clone());
                return Err(m);
            }
            if blcksz != XLOG_BLCKSZ {
                let m = "WAL file is from different database system: incorrect XLOG_BLCKSZ in page header".to_string();
                self.report(m.clone());
                return Err(m);
            }
        } else if offset == 0 {
            let m = format!(
                "invalid info bits {info:04X} in WAL segment, offset {offset}"
            );
            self.report(m.clone());
            return Err(m);
        }

        if pageaddr != recptr.0 {
            let m = format!(
                "unexpected pageaddr {:X}/{:X} in WAL segment, offset {offset}",
                pageaddr >> 32,
                pageaddr as u32
            );
            self.report(m.clone());
            return Err(m);
        }

        // A child timeline always has a TLI greater than its parent, so TLI must
        // never go backwards across pages later than the last remembered LSN.
        if recptr > self.latest_page_ptr && tli < self.latest_page_tli {
            let m = format!(
                "out-of-sequence timeline ID {} (after {}) in WAL segment, offset {offset}",
                tli.0, self.latest_page_tli.0
            );
            self.report(m.clone());
            return Err(m);
        }
        self.latest_page_ptr = recptr;
        self.latest_page_tli = tli;
        Ok(())
    }

    /// Header size of the page currently in `read_buf` (long vs short).
    fn page_header_size(&self) -> usize {
        let info = u16::from_ne_bytes([self.read_buf[2], self.read_buf[3]]);
        if info & XlpFlags::LONG_HEADER.bits() != 0 {
            SizeOfXLogLongPHD
        } else {
            SizeOfXLogShortPHD
        }
    }

    fn page_info(&self) -> u16 {
        u16::from_ne_bytes([self.read_buf[2], self.read_buf[3]])
    }

    fn page_rem_len(&self) -> u32 {
        u32::from_ne_bytes(self.read_buf[16..20].try_into().unwrap())
    }

    /// PG `XLogReadRecord` core (`XLogDecodeNextRecord` then decode). Read the
    /// next record from the current position, following continuation across page
    /// boundaries, validating header + CRC, then decoding it. Returns a reference
    /// to the decoded record, `Ok(None)` at clean end-of-WAL, or `Err` with the
    /// error message.
    pub fn read_record(&mut self) -> Result<Option<&DecodedXLogRecord>, String> {
        self.errormsg = None;
        let decoded = self.decode_next_record()?;
        self.record = Some(Box::new(decoded));
        let rec = self.record.as_deref().unwrap();
        self.read_rec_ptr = rec.lsn;
        self.end_rec_ptr = rec.next_lsn;
        Ok(self.record.as_deref())
    }

    /// PG `XLogDecodeNextRecord`: the read+reassemble loop. Produces a fully
    /// decoded record (or an error). Handles the page-boundary continuation case
    /// (XLP_FIRST_IS_CONTRECORD / xlp_rem_len) and validates header + CRC.
    #[allow(clippy::too_many_lines, reason = "1:1 port of C XLogDecodeNextRecord; splitting would diverge from PG structure")]
    fn decode_next_record(&mut self) -> Result<DecodedXLogRecord, String> {
        let mut rec_ptr = self.next_rec_ptr;
        // Random access (no prev decode) verifies prev-link loosely; sequential
        // access (after a prior decode) verifies it exactly.
        let rand_access = self.decode_rec_ptr.is_invalid();

        // The whole read can restart from a new RecPtr when we hit an overwrite
        // contrecord flag mid-reassembly (C `goto restart`).
        'restart: loop {
        self.curr_rec_ptr = rec_ptr;

        let mut target_page_ptr = XLogRecPtr(rec_ptr.0 - rec_ptr.0 % BLCKSZ_U64);
        let mut target_rec_off = (rec_ptr.0 % BLCKSZ_U64) as usize;

        // Read enough to cover the record header (or the part on this page).
        let want = (target_rec_off + SizeOfXLogRecord).min(BLCKSZ);
        self.read_page_internal(target_page_ptr, want)?;

        let page_header_size = self.page_header_size();
        if target_rec_off == 0 {
            // At page start: skip the page header.
            rec_ptr = XLogRecPtr(rec_ptr.0 + page_header_size as u64);
            target_rec_off = page_header_size;
        } else if target_rec_off < page_header_size {
            return Err(format!(
                "invalid record offset at {:X}/{:X}",
                rec_ptr.0 >> 32,
                rec_ptr.0 as u32
            ));
        }

        if (self.page_info() & XlpFlags::FIRST_IS_CONTRECORD.bits()) != 0
            && target_rec_off == page_header_size
        {
            return Err(format!(
                "contrecord is requested by {:X}/{:X}",
                rec_ptr.0 >> 32,
                rec_ptr.0 as u32
            ));
        }

        // xl_tot_len is the first field, always on this page (records MAXALIGNed).
        let rec_off = (rec_ptr.0 % BLCKSZ_U64) as usize;
        let total_len = u32::from_ne_bytes(self.read_buf[rec_off..rec_off + 4].try_into().unwrap());

        let mut got_header = false;
        if target_rec_off <= BLCKSZ - SizeOfXLogRecord {
            // The whole header is on this page; validate it immediately.
            let hdr = self.read_header_at(rec_off);
            self.valid_xlog_record_header(rec_ptr, &hdr, rand_access)?;
            got_header = true;
        } else if (total_len as usize) < SizeOfXLogRecord {
            return Err(format!(
                "invalid record length at {:X}/{:X}: expected at least {}, got {}",
                rec_ptr.0 >> 32,
                rec_ptr.0 as u32,
                SizeOfXLogRecord,
                total_len
            ));
        }

        let len_on_page = (BLCKSZ_U64 - rec_ptr.0 % BLCKSZ_U64) as usize;
        let record_bytes: Vec<u8>;
        let next_rec_ptr: XLogRecPtr;

        let mut assembled = false;
        if total_len as usize > len_on_page {
            // ---- Need to reassemble a boundary-spanning record. ----
            assembled = true;
            let mut buf = Vec::with_capacity(total_len as usize);
            buf.extend_from_slice(&self.read_buf[rec_off..rec_off + len_on_page]);
            let mut got_len = len_on_page;
            let mut last_page_header_size;
            let mut last_rem_len;

            loop {
                target_page_ptr = XLogRecPtr(target_page_ptr.0 + BLCKSZ_U64);

                // Read the short page header first to inspect the contrecord flag.
                self.read_page_internal(target_page_ptr, SizeOfXLogShortPHD)
                    .inspect_err(|m| { self.set_aborted(rec_ptr, target_page_ptr); })?;

                let info = self.page_info();
                // If we expected a continuation but the page carries the overwrite
                // flag, the contrecord was overwritten by a different record after
                // an aborted write. Restart the whole read from this page (C
                // `goto restart`), remembering the record we were reading.
                if (info & XlpFlags::FIRST_IS_OVERWRITE_CONTRECORD.bits()) != 0 {
                    self.overwritten_rec_ptr = rec_ptr;
                    rec_ptr = target_page_ptr;
                    continue 'restart;
                }
                if (info & XlpFlags::FIRST_IS_CONTRECORD.bits()) == 0 {
                    self.set_aborted(rec_ptr, target_page_ptr);
                    return Err(format!(
                        "there is no contrecord flag at {:X}/{:X}",
                        rec_ptr.0 >> 32,
                        rec_ptr.0 as u32
                    ));
                }
                let rem_len = self.page_rem_len();
                if rem_len == 0 || total_len != rem_len + got_len as u32 {
                    self.set_aborted(rec_ptr, target_page_ptr);
                    return Err(format!(
                        "invalid contrecord length {} (expected {}) at {:X}/{:X}",
                        rem_len,
                        i64::from(total_len) - got_len as i64,
                        rec_ptr.0 >> 32,
                        rec_ptr.0 as u32
                    ));
                }

                let page_header_size = self.page_header_size();
                // Bytes of record data taken from this page.
                let mut take = BLCKSZ - page_header_size;
                if (rem_len as usize) < take {
                    take = rem_len as usize;
                }
                self.read_page_internal(target_page_ptr, page_header_size + take)
                    .inspect_err(|m| { self.set_aborted(rec_ptr, target_page_ptr); })?;
                buf.extend_from_slice(
                    &self.read_buf[page_header_size..page_header_size + take],
                );
                got_len += take;
                last_page_header_size = page_header_size;
                last_rem_len = rem_len;

                if !got_header {
                    // The header itself spanned the boundary; validate it now.
                    let hdr = read_header_from(&buf, 0);
                    self.valid_xlog_record_header(rec_ptr, &hdr, rand_access)
                        .inspect_err(|m| { self.set_aborted(rec_ptr, target_page_ptr); })?;
                    got_header = true;
                }

                if got_len >= total_len as usize {
                    next_rec_ptr = XLogRecPtr(
                        target_page_ptr.0
                            + last_page_header_size as u64
                            + MAXALIGN(last_rem_len as usize) as u64,
                    );
                    break;
                }
            }
            record_bytes = buf;
        } else {
            // ---- Record fits on a single page. ----
            let want = (target_rec_off + total_len as usize).min(BLCKSZ);
            self.read_page_internal(target_page_ptr, want)?;
            let rec_off = (rec_ptr.0 % BLCKSZ_U64) as usize;
            record_bytes = self.read_buf[rec_off..rec_off + total_len as usize].to_vec();
            next_rec_ptr = XLogRecPtr(rec_ptr.0 + MAXALIGN(total_len as usize) as u64);
        }

        debug_assert!(got_header);

        // CRC check over the full reassembled record.
        if assembled {
            self.valid_xlog_record(&record_bytes, rec_ptr)
                .inspect_err(|m| { self.set_aborted(rec_ptr, target_page_ptr); })?;
        } else {
            self.valid_xlog_record(&record_bytes, rec_ptr)?;
        }

        let header = read_header_from(&record_bytes, 0);

        // Compute the next-record position, handling XLOG SWITCH.
        let mut next = next_rec_ptr;
        if header.rmid == crate::access::rmgrlist::RmgrId::Xlog as RmgrId
            && (header.info & !XLR_INFO_MASK) == XLOG_SWITCH
        {
            // An XLOG_SWITCH record extends to the end of its segment.
            next = XLogRecPtr(next.0 + self.wal_seg_size - 1);
            next = XLogRecPtr(next.0 - XLogSegmentOffset(next.0, self.wal_seg_size));
        }

        self.decode_rec_ptr = rec_ptr;
        self.next_rec_ptr = next;

        let mut decoded = decode_xlog_record(&record_bytes, &header, rec_ptr)
            .inspect_err(|m| {
                self.report(m.clone());
            })?;
        decoded.next_lsn = next;
        return Ok(decoded);
        } // 'restart
    }

    /// Record the aborted-record / missing-contrecord positions on a failed
    /// multi-page assembly (C `err:` block: abortedRecPtr / missingContrecPtr).
    fn set_aborted(&mut self, rec_ptr: XLogRecPtr, target_page_ptr: XLogRecPtr) {
        self.aborted_rec_ptr = rec_ptr;
        self.missing_contrec_ptr = target_page_ptr;
    }

    /// Read a copy of the `XLogRecord` header out of `read_buf` at byte `off`.
    fn read_header_at(&self, off: usize) -> XLogRecord {
        read_header_from(&self.read_buf, off)
    }

    /// PG `ValidXLogRecordHeader`: length, rmid, and prev-link sanity.
    fn valid_xlog_record_header(
        &mut self,
        rec_ptr: XLogRecPtr,
        record: &XLogRecord,
        rand_access: bool,
    ) -> Result<(), String> {
        if (record.tot_len as usize) < SizeOfXLogRecord {
            let m = format!(
                "invalid record length at {:X}/{:X}: expected at least {}, got {}",
                rec_ptr.0 >> 32,
                rec_ptr.0 as u32,
                SizeOfXLogRecord,
                record.tot_len
            );
            self.report(m.clone());
            return Err(m);
        }
        if !rmgr_id_is_valid(i32::from(record.rmid)) {
            let m = format!(
                "invalid resource manager ID {} at {:X}/{:X}",
                record.rmid,
                rec_ptr.0 >> 32,
                rec_ptr.0 as u32
            );
            self.report(m.clone());
            return Err(m);
        }
        if rand_access {
            if record.prev >= rec_ptr {
                let m = format!(
                    "record with incorrect prev-link {:X}/{:X} at {:X}/{:X}",
                    record.prev.0 >> 32,
                    record.prev.0 as u32,
                    rec_ptr.0 >> 32,
                    rec_ptr.0 as u32
                );
                self.report(m.clone());
                return Err(m);
            }
        } else if record.prev != self.decode_rec_ptr {
            let m = format!(
                "record with incorrect prev-link {:X}/{:X} at {:X}/{:X}",
                record.prev.0 >> 32,
                record.prev.0 as u32,
                rec_ptr.0 >> 32,
                rec_ptr.0 as u32
            );
            self.report(m.clone());
            return Err(m);
        }
        Ok(())
    }

    /// PG `ValidXLogRecord`: CRC-32C over (data area after the fixed header) then
    /// (the header up to xl_crc), matching the assembler's fold order.
    fn valid_xlog_record(&mut self, record: &[u8], rec_ptr: XLogRecPtr) -> Result<(), String> {
        let stored = u32::from_ne_bytes(record[20..24].try_into().unwrap());
        let mut crc = init_crc32c();
        crc = comp_crc32c(crc, &record[SizeOfXLogRecord..]);
        crc = comp_crc32c(crc, &record[..SizeOfXLogRecord - 4]); // up to xl_crc
        crc = fin_crc32c(crc);
        if crc != stored {
            let m = format!(
                "incorrect resource manager data checksum in record at {:X}/{:X}",
                rec_ptr.0 >> 32,
                rec_ptr.0 as u32
            );
            self.report(m.clone());
            return Err(m);
        }
        Ok(())
    }
}

/// Read an `XLogRecord` header out of `bytes` starting at `off` (native-endian,
/// matching the assembler's `to_ne_bytes` on the little-endian target).
fn read_header_from(bytes: &[u8], off: usize) -> XLogRecord {
    XLogRecord {
        tot_len: u32::from_ne_bytes(bytes[off..off + 4].try_into().unwrap()),
        xid: TransactionId(u32::from_ne_bytes(bytes[off + 4..off + 8].try_into().unwrap())),
        prev: XLogRecPtr(u64::from_ne_bytes(bytes[off + 8..off + 16].try_into().unwrap())),
        info: bytes[off + 16],
        rmid: bytes[off + 17],
        // bytes[off+18..20] = padding
        crc: u32::from_ne_bytes(bytes[off + 20..off + 24].try_into().unwrap()),
    }
}

/// PG `DecodeXLogRecord`: parse the complete record bytes into a
/// [`DecodedXLogRecord`]. The exact inverse of 13B's `assemble`: walk the block /
/// data headers, then copy each fragment's payload (block images first, then
/// block data, then the main data) -- in the same payload order the assembler
/// emitted. Native-endian field reads.
#[allow(clippy::too_many_lines, reason = "1:1 port of C DecodeXLogRecord; splitting would diverge from PG structure")]
pub fn decode_xlog_record(
    record: &[u8],
    header: &XLogRecord,
    lsn: XLogRecPtr,
) -> Result<DecodedXLogRecord, String> {
    let mut decoded = DecodedXLogRecord {
        size: 0,
        oversized: false,
        lsn,
        next_lsn: INVALID_XLOG_REC_PTR,
        header: *header,
        record_origin: crate::access::xlogdefs::RepOriginId(0),
        toplevel_xid: crate::c::InvalidTransactionId,
        main_data: None,
        main_data_len: 0,
        max_block_id: -1,
        blocks: (0..=XLR_MAX_BLOCK_ID as usize).map(|_| empty_block()).collect(),
    };

    let remaining_total = record.len();
    let mut datatotal: usize = 0;
    let mut prev_rlocator: Option<RelFileLocator> = None;

    // --- header walk ---
    // `remaining` is the bytes left in the header+data region (after the fixed
    // record header). We stop when the cursor reaches the start of the payload.
    let mut hp = SizeOfXLogRecord; // header-parse cursor
    macro_rules! take {
        ($n:expr) => {{
            if hp + $n > remaining_total {
                return Err("record with invalid length".to_string());
            }
            let s = &record[hp..hp + $n];
            hp += $n;
            s
        }};
    }

    // The header region is [SizeOfXLogRecord, record.len() - datatotal). Because
    // datatotal grows as we parse, we mirror C: loop while `remaining > datatotal`
    // where remaining = record.len() - hp.
    loop {
        let remaining = remaining_total - hp;
        if remaining <= datatotal {
            break;
        }
        let block_id = take!(1)[0];

        if block_id == XLR_BLOCK_ID_DATA_SHORT {
            let n = u32::from(take!(1)[0]);
            decoded.main_data_len = n;
            datatotal += n as usize;
            break; // main data is always last
        } else if block_id == XLR_BLOCK_ID_DATA_LONG {
            let n = u32::from_ne_bytes(take!(4).try_into().unwrap());
            decoded.main_data_len = n;
            datatotal += n as usize;
            break;
        } else if block_id == XLR_BLOCK_ID_ORIGIN {
            let v = u16::from_ne_bytes(take!(2).try_into().unwrap());
            decoded.record_origin = crate::access::xlogdefs::RepOriginId(v);
        } else if block_id == XLR_BLOCK_ID_TOPLEVEL_XID {
            let v = u32::from_ne_bytes(take!(4).try_into().unwrap());
            decoded.toplevel_xid = TransactionId(v);
        } else if block_id <= XLR_MAX_BLOCK_ID {
            // Mark intervening unused block ids.
            for i in (decoded.max_block_id + 1)..i32::from(block_id) {
                decoded.blocks[i as usize].in_use = false;
            }
            if i32::from(block_id) <= decoded.max_block_id {
                return Err(format!("out-of-order block_id {} at {:X}/{:X}", block_id, lsn.0 >> 32, lsn.0 as u32));
            }
            decoded.max_block_id = i32::from(block_id);

            let fork_flags = take!(1)[0];
            let data_len = u16::from_ne_bytes(take!(2).try_into().unwrap());

            let has_image = fork_flags & BKPBLOCK_HAS_IMAGE != 0;
            let has_data = fork_flags & BKPBLOCK_HAS_DATA != 0;

            if has_data && data_len == 0 {
                return Err(format!("BKPBLOCK_HAS_DATA set, but no data included at {:X}/{:X}", lsn.0 >> 32, lsn.0 as u32));
            }
            if !has_data && data_len != 0 {
                return Err(format!("BKPBLOCK_HAS_DATA not set, but data length is {} at {:X}/{:X}", data_len, lsn.0 >> 32, lsn.0 as u32));
            }
            datatotal += data_len as usize;

            let mut bimg_len: u16 = 0;
            let mut hole_offset: u16 = 0;
            let mut hole_length: u16 = 0;
            let mut bimg_info: u8 = 0;
            let mut apply_image = false;

            if has_image {
                bimg_len = u16::from_ne_bytes(take!(2).try_into().unwrap());
                hole_offset = u16::from_ne_bytes(take!(2).try_into().unwrap());
                bimg_info = take!(1)[0];

                let info = BkpImage::from_bits_retain(bimg_info);
                apply_image = info.contains(BkpImage::APPLY);

                if info.is_compressed() {
                    if info.contains(BkpImage::HAS_HOLE) {
                        hole_length = u16::from_ne_bytes(take!(2).try_into().unwrap());
                    }
                } else {
                    hole_length = BLCKSZ as u16 - bimg_len;
                }
                datatotal += bimg_len as usize;

                if info.contains(BkpImage::HAS_HOLE)
                    && (hole_offset == 0 || hole_length == 0 || bimg_len == BLCKSZ as u16)
                {
                    return Err(format!(
                        "BKPIMAGE_HAS_HOLE set, but hole offset {} length {} block image length {} at {:X}/{:X}",
                        hole_offset, hole_length, bimg_len, lsn.0 >> 32, lsn.0 as u32
                    ));
                }
                if !info.contains(BkpImage::HAS_HOLE) && (hole_offset != 0 || hole_length != 0) {
                    return Err(format!(
                        "BKPIMAGE_HAS_HOLE not set, but hole offset {} length {} at {:X}/{:X}",
                        hole_offset, hole_length, lsn.0 >> 32, lsn.0 as u32
                    ));
                }
                if info.is_compressed() && bimg_len == BLCKSZ as u16 {
                    return Err(format!("BKPIMAGE_COMPRESSED set, but block image length {} at {:X}/{:X}", bimg_len, lsn.0 >> 32, lsn.0 as u32));
                }
                if !info.contains(BkpImage::HAS_HOLE) && !info.is_compressed() && bimg_len != BLCKSZ as u16 {
                    return Err(format!(
                        "neither BKPIMAGE_HAS_HOLE nor BKPIMAGE_COMPRESSED set, but block image length is {} at {:X}/{:X}",
                        bimg_len, lsn.0 >> 32, lsn.0 as u32
                    ));
                }
            }

            let rlocator = if fork_flags & BKPBLOCK_SAME_REL == 0 {
                let rl = read_rlocator(take!(12));
                prev_rlocator = Some(rl);
                rl
            } else {
                match prev_rlocator {
                    Some(rl) => rl,
                    None => return Err(format!("BKPBLOCK_SAME_REL set but no previous rel at {:X}/{:X}", lsn.0 >> 32, lsn.0 as u32)),
                }
            };
            let blkno = u32::from_ne_bytes(take!(4).try_into().unwrap());

            let blk = &mut decoded.blocks[block_id as usize];
            blk.in_use = true;
            blk.flags = fork_flags;
            blk.forknum = fork_num_from(fork_flags & BKPBLOCK_FORK_MASK);
            blk.has_image = has_image;
            blk.apply_image = apply_image;
            blk.has_data = has_data;
            blk.data_len = data_len;
            blk.bimg_len = bimg_len;
            blk.hole_offset = hole_offset;
            blk.hole_length = hole_length;
            blk.bimg_info = bimg_info;
            blk.rlocator = rlocator;
            blk.blkno = blkno;
            blk.prefetch_buffer = INVALID_BUFFER;
        } else {
            return Err(format!("invalid block_id {} at {:X}/{:X}", block_id, lsn.0 >> 32, lsn.0 as u32));
        }
    }

    // After the header walk, `hp` sits at the start of the payload region.
    let ptr = hp;
    let remaining = remaining_total - ptr;
    if remaining != datatotal {
        return Err("record with invalid length".to_string());
    }

    // --- payload copy: block images, then block data, then main data ---
    let mut cur = ptr;
    for block_id in 0..=decoded.max_block_id.max(-1) {
        if block_id < 0 {
            break;
        }
        let blk = &mut decoded.blocks[block_id as usize];
        if !blk.in_use {
            continue;
        }
        if blk.has_image {
            let n = blk.bimg_len as usize;
            blk.bkp_image = Some(record[cur..cur + n].to_vec());
            cur += n;
        }
        if blk.has_data {
            let n = blk.data_len as usize;
            blk.data = Some(record[cur..cur + n].to_vec());
            cur += n;
        }
    }
    if decoded.main_data_len > 0 {
        let n = decoded.main_data_len as usize;
        decoded.main_data = Some(record[cur..cur + n].to_vec());
        cur += n;
    }
    debug_assert_eq!(cur, record.len());

    decoded.size = record.len();
    Ok(decoded)
}

fn empty_block() -> DecodedBkpBlock {
    DecodedBkpBlock {
        in_use: false,
        rlocator: RelFileLocator {
            spcOid: crate::postgres_ext::Oid::new(0),
            dbOid: crate::postgres_ext::Oid::new(0),
            relNumber: crate::postgres_ext::Oid::new(0),
        },
        forknum: ForkNumber::MAIN_FORKNUM,
        blkno: 0,
        prefetch_buffer: INVALID_BUFFER,
        flags: 0,
        has_image: false,
        apply_image: false,
        bkp_image: None,
        hole_offset: 0,
        hole_length: 0,
        bimg_len: 0,
        bimg_info: 0,
        has_data: false,
        data: None,
        data_len: 0,
        data_bufsz: 0,
    }
}

/// RelFileLocator from 12 on-disk bytes (spcOid, dbOid, relNumber: 3 x u32, NE).
fn read_rlocator(b: &[u8]) -> RelFileLocator {
    RelFileLocator {
        spcOid: crate::postgres_ext::Oid::new(u32::from_ne_bytes(b[0..4].try_into().unwrap())),
        dbOid: crate::postgres_ext::Oid::new(u32::from_ne_bytes(b[4..8].try_into().unwrap())),
        relNumber: crate::postgres_ext::Oid::new(u32::from_ne_bytes(b[8..12].try_into().unwrap())),
    }
}

/// Fork number from the low-nibble fork value (the assembler writes the enum
/// discriminant as a byte; only the four defined forks occur).
fn fork_num_from(n: u8) -> ForkNumber {
    match n {
        0 => ForkNumber::MAIN_FORKNUM,
        1 => ForkNumber::FSM_FORKNUM,
        2 => ForkNumber::VISIBILITYMAP_FORKNUM,
        3 => ForkNumber::INIT_FORKNUM,
        _ => ForkNumber::InvalidForkNumber,
    }
}

// === accessors on a decoded record (XLogRecGet* / RestoreBlockImage) ===

impl DecodedXLogRecord {
    /// PG `XLogRecGetData`: the record's main data portion.
    pub fn get_data(&self) -> Option<&[u8]> {
        self.main_data.as_deref()
    }

    /// PG `XLogRecGetBlockData`: the rmgr-specific data for `block_id`, if any.
    pub fn get_block_data(&self, block_id: u8) -> Option<&[u8]> {
        if i32::from(block_id) > self.max_block_id || !self.blocks[block_id as usize].in_use {
            return None;
        }
        let blk = &self.blocks[block_id as usize];
        if !blk.has_data {
            return None;
        }
        blk.data.as_deref()
    }

    /// PG `XLogRecGetBlockTagExtended`: (rlocator, fork, blkno, prefetch_buffer)
    /// for `block_id`; None if the record has no such (in-use) block reference.
    pub fn get_block_tag_extended(
        &self,
        block_id: u8,
    ) -> Option<(RelFileLocator, ForkNumber, BlockNumber, crate::storage::buf::Buffer)> {
        if i32::from(block_id) > self.max_block_id || !self.blocks[block_id as usize].in_use {
            return None;
        }
        let blk = &self.blocks[block_id as usize];
        Some((blk.rlocator, blk.forknum, blk.blkno, blk.prefetch_buffer))
    }

    /// PG `XLogRecGetBlockTag`: like [`Self::get_block_tag_extended`] but the block
    /// reference must exist (panics otherwise, matching the C ereport(ERROR)).
    pub fn get_block_tag(&self, block_id: u8) -> (RelFileLocator, ForkNumber, BlockNumber) {
        let (rl, fork, blk, _) = self
            .get_block_tag_extended(block_id)
            .unwrap_or_else(|| panic!("could not locate backup block with ID {block_id} in WAL record"));
        (rl, fork, blk)
    }

    /// PG `XLogRecHasBlockImage`.
    pub fn has_block_image_for(&self, block_id: u8) -> bool {
        i32::from(block_id) <= self.max_block_id && self.blocks[block_id as usize].has_image
    }

    /// PG `RestoreBlockImage`: reconstruct the full `BLCKSZ` page for `block_id`
    /// into `page`, re-inserting the eliminated hole as zeroes. (No compression in
    /// the foundation; a compressed image is rejected.) Returns true on success.
    pub fn restore_block_image(&self, block_id: u8, page: &mut [u8]) -> Result<bool, String> {
        if i32::from(block_id) > self.max_block_id || !self.blocks[block_id as usize].in_use {
            return Err(format!(
                "could not restore image at {:X}/{:X} with invalid block {} specified",
                self.lsn.0 >> 32,
                self.lsn.0 as u32,
                block_id
            ));
        }
        let blk = &self.blocks[block_id as usize];
        if !blk.has_image {
            return Err(format!(
                "could not restore image at {:X}/{:X} with invalid state, block {}",
                self.lsn.0 >> 32,
                self.lsn.0 as u32,
                block_id
            ));
        }
        let info = BkpImage::from_bits_retain(blk.bimg_info);
        if info.is_compressed() {
            return Err(format!(
                "could not restore image at {:X}/{:X} compressed (unsupported in this build), block {}",
                self.lsn.0 >> 32,
                self.lsn.0 as u32,
                block_id
            ));
        }
        let img = blk.bkp_image.as_deref().expect("has_image without bytes");

        if blk.hole_length == 0 {
            page[..BLCKSZ].copy_from_slice(&img[..BLCKSZ]);
        } else {
            let ho = blk.hole_offset as usize;
            let hl = blk.hole_length as usize;
            page[..ho].copy_from_slice(&img[..ho]);
            for b in &mut page[ho..ho + hl] {
                *b = 0;
            }
            page[ho + hl..BLCKSZ].copy_from_slice(&img[ho..BLCKSZ - hl]);
        }
        Ok(true)
    }
}

// === XLogFindNextRecord ===
//
// PG `XLogFindNextRecord`: scan forward from an arbitrary LSN to the first valid
// record boundary. Used by pg_waldump / pg_rewind to start mid-stream. It belongs
// with the recovery/standalone-tooling drivers and is not exercised by the
// foundation read path (begin_read positions at a known record start). The
// scanning logic (skip to a page start, walk pages reading xlp_rem_len to land on
// the first record that begins on/after the target, then re-read it) is deferred.
// TODO(wal): implement when pg_waldump / arbitrary-LSN seeking is ported.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::rmgrlist::RmgrId as BuiltinRmgrId;
    use crate::access::xlog_internal::XLOGDIR;
    use crate::backend::access::transam::xlog::{XLogCtl, INSERT_TLI};
    use crate::backend::access::transam::xloginsert::{
        begin_insert, register_block, register_buf_data, register_data, with_insertion, xlog_insert,
    };
    use crate::access::xloginsert::RegBuf;
    use crate::backend::utils::init::globals::ProcessConfig;
    use crate::catalog::pg_control::XLOG_FPI;
    use crate::postgres_ext::Oid;
    use crate::storage::bufpage::Page;
    use crate::storage::io_backend::IoBackend;
    use std::sync::Arc;

    fn xlog_rmid() -> RmgrId {
        BuiltinRmgrId::Xlog as u8
    }

    async fn fresh_xlog(
        wal_seg_size: u64,
        n_pages: usize,
    ) -> (Arc<XLogCtl>, std::path::PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let uniq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "pepperdb_xlogreader_{}_{}",
            std::process::id(),
            uniq
        ));
        crate::storage::io_backend::mkdir_all(dir.join(XLOGDIR)).await.unwrap();
        let io = Arc::new(IoBackend::with_default_budget());
        let config = Arc::new(ProcessConfig::new());
        config.set_data_dir(dir.to_str().unwrap());
        (XLogCtl::with_config(io, config, wal_seg_size, n_pages), dir)
    }

    /// Build a page-read closure that serves the on-disk WAL segment files for
    /// the given XLogCtl directory. Synchronous: it opens/reads the segment file
    /// for the requested page directly (the reader is pure-CPU; the test's I/O
    /// happens here, mirroring how recovery's async driver supplies pages).
    fn segment_page_reader(dir: std::path::PathBuf, wal_seg_size: u64) -> PageReadFn {
        use std::io::{Read, Seek, SeekFrom};
        let segs_per_id = 0x1_0000_0000u64 / wal_seg_size;
        Box::new(move |page_ptr: XLogRecPtr, _req: usize, into: &mut [u8]| {
            let segno = page_ptr.0 / wal_seg_size;
            let off = page_ptr.0 % wal_seg_size;
            let logid = (segno / segs_per_id) as u32;
            let seg = (segno % segs_per_id) as u32;
            let name = format!("{:08X}{:08X}{:08X}", INSERT_TLI.0, logid, seg);
            let path = dir.join(XLOGDIR).join(name);
            let mut f = std::fs::File::open(&path)
                .map_err(|e| format!("open {path:?}: {e}"))?;
            f.seek(SeekFrom::Start(off)).map_err(|e| e.to_string())?;
            let mut n = 0usize;
            while n < into.len() {
                match f.read(&mut into[n..]).map_err(|e| e.to_string())? {
                    0 => break,
                    k => n += k,
                }
            }
            Ok(n)
        })
    }

    /// Insert a data-only record, flush, then read it back and check the bytes.
    #[tokio::test]
    async fn round_trip_data_only() {
        let (xlog, dir) = fresh_xlog(crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE, 16).await;
        let payload = b"hello WAL record - data only";

        let (start, end) = with_insertion(async {
            begin_insert();
            register_data(payload);
            let start = xlog.get_xlog_insert_rec_ptr();
            let end = xlog_insert(&xlog, xlog_rmid(), 0x00).await;
            (start, end)
        })
        .await;
        xlog.xlog_flush(end).await;

        let mut reader = XLogReader::new(
            crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE,
            segment_page_reader(dir.clone(), crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE),
        );
        reader.begin_read(start);
        let rec = reader.read_record().unwrap().expect("a record");
        assert_eq!(rec.header.rmid, xlog_rmid());
        assert_eq!(rec.get_data().unwrap(), payload);
        assert_eq!(rec.max_block_id, -1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A record with a registered block + per-block data (no image).
    #[tokio::test]
    async fn round_trip_block_and_data() {
        let (xlog, dir) = fresh_xlog(crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE, 16).await;
        let rloc = RelFileLocator { spcOid: Oid::new(1663), dbOid: Oid::new(5), relNumber: Oid::new(16384) };
        let main = b"main data here";
        let blkdata = b"per-block tuple bytes";
        // Use NO_IMAGE so the block contributes data (not an FPI).
        let page = Page::zeroed();

        let (start, end) = with_insertion(async {
            begin_insert();
            register_block(0, &rloc, ForkNumber::MAIN_FORKNUM, 42, &page, RegBuf::NO_IMAGE);
            register_buf_data(0, blkdata);
            register_data(main);
            let start = xlog.get_xlog_insert_rec_ptr();
            let end = xlog_insert(&xlog, xlog_rmid(), 0x00).await;
            (start, end)
        })
        .await;
        xlog.xlog_flush(end).await;

        let mut reader = XLogReader::new(
            crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE,
            segment_page_reader(dir.clone(), crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE),
        );
        reader.begin_read(start);
        let rec = reader.read_record().unwrap().expect("a record");
        assert_eq!(rec.get_data().unwrap(), main);
        assert_eq!(rec.max_block_id, 0);
        let (got_rloc, got_fork, got_blk) = rec.get_block_tag(0);
        assert_eq!(got_rloc, rloc);
        assert_eq!(got_fork, ForkNumber::MAIN_FORKNUM);
        assert_eq!(got_blk, 42);
        assert!(!rec.has_block_image_for(0));
        assert_eq!(rec.get_block_data(0).unwrap(), blkdata);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A full-page image record (log_newpage) -> RestoreBlockImage reproduces the
    /// original page, including the hole.
    #[tokio::test]
    async fn round_trip_fpi_with_hole() {
        let (xlog, dir) = fresh_xlog(crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE, 16).await;
        let rloc = RelFileLocator { spcOid: Oid::new(1663), dbOid: Oid::new(5), relNumber: Oid::new(16390) };
        let mut page = Page::zeroed();
        // Standard page with a hole: pd_lower=40, pd_upper=8000. Fill the non-hole
        // regions with recognizable data.
        page.as_mut_bytes()[12..14].copy_from_slice(&40u16.to_ne_bytes());
        page.as_mut_bytes()[14..16].copy_from_slice(&8000u16.to_ne_bytes());
        for i in 24..40 {
            page.as_mut_bytes()[i] = (i as u8).wrapping_mul(7);
        }
        for i in 8000..BLCKSZ {
            page.as_mut_bytes()[i] = (i as u8).wrapping_add(3);
        }
        let original = page.as_bytes().to_vec();

        let start = xlog.get_xlog_insert_rec_ptr();
        let end = crate::backend::access::transam::xloginsert::log_newpage(
            &xlog,
            &rloc,
            ForkNumber::MAIN_FORKNUM,
            7,
            &page,
            true,
        )
        .await;
        xlog.xlog_flush(end).await;

        let mut reader = XLogReader::new(
            crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE,
            segment_page_reader(dir.clone(), crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE),
        );
        reader.begin_read(start);
        let rec = reader.read_record().unwrap().expect("a record");
        assert_eq!(rec.header.rmid, xlog_rmid());
        assert_eq!(rec.header.info & !XLR_INFO_MASK, XLOG_FPI);
        assert!(rec.has_block_image_for(0));
        let blk = &rec.blocks[0];
        assert!(blk.hole_length > 0, "expected an eliminated hole");

        let mut restored = vec![0u8; BLCKSZ];
        assert!(rec.restore_block_image(0, &mut restored).unwrap());
        assert_eq!(restored, original, "restored FPI must equal original page");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A record large enough to span a WAL page boundary reads back correctly.
    #[tokio::test]
    async fn round_trip_continuation_across_page() {
        // Small segment so we exercise the boundary without writing 16 MB, but
        // big enough to hold the multi-page record.
        let seg = 4 * BLCKSZ_U64;
        let (xlog, dir) = fresh_xlog(seg, 16).await;
        // A payload larger than one page forces continuation.
        let payload: Vec<u8> = (0..(BLCKSZ + 2000)).map(|i| (i % 251) as u8).collect();

        let (start, end) = with_insertion(async {
            begin_insert();
            register_data(&payload);
            let start = xlog.get_xlog_insert_rec_ptr();
            let end = xlog_insert(&xlog, xlog_rmid(), 0x00).await;
            (start, end)
        })
        .await;
        xlog.xlog_flush(end).await;
        // start and end must straddle at least one page boundary.
        assert_ne!(start.0 / BLCKSZ_U64, (end.0 - 1) / BLCKSZ_U64);

        let mut reader = XLogReader::new(seg, segment_page_reader(dir.clone(), seg));
        reader.begin_read(start);
        let rec = reader.read_record().unwrap().expect("a record");
        assert_eq!(rec.get_data().unwrap(), &payload[..]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two records read back, each positioned at its known start (random access).
    /// Covers the page-header skip when the second record's page differs from the
    /// first. (Sequential prev-link replay is covered by
    /// [`sequential_replay_prev_link_chain`].)
    #[tokio::test]
    async fn round_trip_two_records_random_access() {
        let (xlog, dir) = fresh_xlog(crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE, 16).await;
        let p1 = b"first record payload";
        let p2 = b"second record payload, a bit longer than the first";

        let start1 = with_insertion(async {
            begin_insert();
            register_data(p1);
            let s = xlog.get_xlog_insert_rec_ptr();
            let _ = xlog_insert(&xlog, xlog_rmid(), 0x00).await;
            s
        })
        .await;
        let (start2, end2) = with_insertion(async {
            begin_insert();
            register_data(p2);
            let s = xlog.get_xlog_insert_rec_ptr();
            let e = xlog_insert(&xlog, xlog_rmid(), 0x00).await;
            (s, e)
        })
        .await;
        xlog.xlog_flush(end2).await;

        let mut reader = XLogReader::new(
            crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE,
            segment_page_reader(dir.clone(), crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE),
        );
        reader.begin_read(start1);
        let r1 = reader.read_record().unwrap().expect("record 1").get_data().unwrap().to_vec();
        assert_eq!(r1, p1);
        // The reader's next_rec_ptr after record 1 must point exactly at start2.
        assert_eq!(reader.end_rec_ptr, start2);

        reader.begin_read(start2);
        let r2 = reader.read_record().unwrap().expect("record 2").get_data().unwrap().to_vec();
        assert_eq!(r2, p2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// MAJOR (xl_prev/CRC): insert several records sequentially, then replay them
    /// with a SINGLE reader from the first record onward. This exercises exact
    /// prev-link validation (`record.prev == previously-decoded record start`),
    /// which is only possible now that the insert path fills the real `xl_prev`
    /// and finalizes the CRC over it. Asserts each record's prev-link equals the
    /// previous record's start LSN, and all CRCs validate (read_record would Err
    /// otherwise). The first record's prev-link is Invalid (no predecessor).
    #[tokio::test]
    async fn sequential_replay_prev_link_chain() {
        let (xlog, dir) =
            fresh_xlog(crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE, 16).await;
        let payloads: Vec<Vec<u8>> = (0..6)
            .map(|i| format!("sequential record number {i} with some bytes").into_bytes())
            .collect();

        // Insert each record in its own insertion scope; remember its start LSN.
        let mut starts = Vec::new();
        let mut last_end = INVALID_XLOG_REC_PTR;
        for p in &payloads {
            let (s, e) = with_insertion(async {
                begin_insert();
                register_data(p);
                let s = xlog.get_xlog_insert_rec_ptr();
                let e = xlog_insert(&xlog, xlog_rmid(), 0x00).await;
                (s, e)
            })
            .await;
            starts.push(s);
            last_end = e;
        }
        xlog.xlog_flush(last_end).await;

        // Read sequentially from the first record. After the first (random-access)
        // record, every subsequent read is checked with an EXACT prev-link match
        // against the prior decode -- the path that was impossible before the fix.
        let mut reader = XLogReader::new(
            crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE,
            segment_page_reader(dir.clone(), crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE),
        );
        reader.begin_read(starts[0]);

        let mut prev_start = INVALID_XLOG_REC_PTR;
        for (i, p) in payloads.iter().enumerate() {
            let rec = reader.read_record().unwrap().expect("a record");
            assert_eq!(rec.get_data().unwrap(), &p[..], "payload {i}");
            // The decoded prev-link must equal the previous record's start LSN
            // (Invalid for the first record).
            assert_eq!(rec.header.prev, prev_start, "prev-link of record {i}");
            assert_eq!(rec.lsn, starts[i], "lsn of record {i}");
            prev_start = starts[i];
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Corrupting a record byte makes ValidXLogRecord fail (CRC mismatch).
    #[tokio::test]
    async fn crc_corruption_detected() {
        let (xlog, dir) = fresh_xlog(crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE, 16).await;
        let payload = b"payload to be corrupted on disk";

        let (start, end) = with_insertion(async {
            begin_insert();
            register_data(payload);
            let s = xlog.get_xlog_insert_rec_ptr();
            let e = xlog_insert(&xlog, xlog_rmid(), 0x00).await;
            (s, e)
        })
        .await;
        xlog.xlog_flush(end).await;

        // Flip a payload byte in the segment file (well past the page header).
        let segno = start.0 / crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE;
        let segs_per_id = 0x1_0000_0000u64 / crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE;
        let name = format!(
            "{:08X}{:08X}{:08X}",
            INSERT_TLI.0,
            (segno / segs_per_id) as u32,
            (segno % segs_per_id) as u32
        );
        let path = dir.join(XLOGDIR).join(name);
        let mut bytes = std::fs::read(&path).unwrap();
        // The record starts at `start`; corrupt a byte inside its payload region.
        let off = (start.0 % crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE) as usize
            + SizeOfXLogRecord
            + 2;
        bytes[off] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let mut reader = XLogReader::new(
            crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE,
            segment_page_reader(dir.clone(), crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE),
        );
        reader.begin_read(start);
        let err = reader.read_record().err().expect("expected CRC failure");
        assert!(err.contains("checksum"), "unexpected error: {err}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A bad rmid in the header is rejected by ValidXLogRecordHeader.
    #[test]
    fn bad_rmid_header_rejected() {
        // Hand-build a minimal record with an invalid rmid and a self-consistent
        // CRC so the header check (not the CRC) is what fails.
        let mut rec = vec![0u8; SizeOfXLogRecord];
        let tot_len = SizeOfXLogRecord as u32;
        rec[0..4].copy_from_slice(&tot_len.to_ne_bytes());
        // xl_prev = 0 (< rec_ptr, ok for random access), info = 0.
        rec[17] = 100; // rmid: builtin range is 0..=RM_NEXT_ID-1, custom >= 128; 100 is invalid.
        let mut crc = init_crc32c();
        crc = comp_crc32c(crc, &rec[SizeOfXLogRecord..]);
        crc = comp_crc32c(crc, &rec[..SizeOfXLogRecord - 4]);
        crc = fin_crc32c(crc);
        rec[20..24].copy_from_slice(&crc.to_ne_bytes());

        let hdr = read_header_from(&rec, 0);
        let mut reader = XLogReader::new(
            crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE,
            |_: XLogRecPtr, _: usize, _: &mut [u8]| Ok(0usize),
        );
        let res = reader.valid_xlog_record_header(XLogRecPtr(0x1000), &hdr, true);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("resource manager ID"));
    }

    /// A bad xl_tot_len (shorter than the fixed header) is rejected.
    #[test]
    fn bad_tot_len_header_rejected() {
        let mut rec = vec![0u8; SizeOfXLogRecord];
        rec[0..4].copy_from_slice(&((SizeOfXLogRecord as u32) - 1).to_ne_bytes());
        rec[17] = xlog_rmid();
        let hdr = read_header_from(&rec, 0);
        let mut reader = XLogReader::new(
            crate::backend::access::transam::xlog::DEFAULT_WAL_SEGMENT_SIZE,
            |_: XLogRecPtr, _: usize, _: &mut [u8]| Ok(0usize),
        );
        let res = reader.valid_xlog_record_header(XLogRecPtr(0x1000), &hdr, true);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("record length"));
    }
}

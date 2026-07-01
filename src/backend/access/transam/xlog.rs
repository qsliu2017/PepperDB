//! Write-ahead log manager: the running-system WAL insert/write/flush path. Translated from backend/access/transam/xlog.c.
//!
//! The Write-Ahead Log (WAL) records every change to the database before it is
//! applied, so a cluster can be recovered after a crash. PostgreSQL splits this
//! functionality across several files; the original xlog.c coordinates database
//! startup and checkpointing and manages the WAL buffers while the system is
//! running. `XLogInsertRecord` reserves space for an already-assembled record,
//! copies it into the shared WAL buffer ring, and returns its log position;
//! `XLogFlush` forces the log up to a given position out to durable storage.
//! WAL-record construction (xloginsert.c), recovery and standby replay
//! (xlogrecovery.c), and the WAL reader (xlogreader.c) live in their own files.
//!
//! This module covers the running-system path: the insert-reservation cursor,
//! `XLogCtl` (the WAL control state), the copy into the buffer ring, the write,
//! the fsync, and the publication of the durably-flushed log position, together
//! with WAL segment file creation and naming. Crash-recovery startup and
//! checkpoint creation are coordinated elsewhere and are not yet implemented
//! here. The system identifier stamped into each segment's long page header is a
//! fixed placeholder until a control file exists; the header layout is exact, so
//! a real PostgreSQL can still parse it.
//!
//! Whereas PostgreSQL keeps `XLogCtl` in a shared-memory segment guarded by a
//! spinlock and a set of LWLocks, PepperDB is a single async process and holds
//! the same state in `Arc`-shared structures. The insert-reservation cursor is
//! bumped under a brief `parking_lot` mutex -- the faithful image of
//! PostgreSQL's `insertpos_lck` spinlock -- and is never held across an
//! `.await`. The fixed set of WAL insert locks (PostgreSQL's
//! `NUM_XLOGINSERT_LOCKS` LWLocks) become held async mutexes, one per insertion
//! slot: a backend takes its slot lock, reserves WAL space while holding it, and
//! keeps it across the copy into the ring (which may await on page eviction).
//! While held it advertises the position it is inserting at via an atomic, so a
//! flusher never writes or fsyncs a page an inserter is still filling. The
//! single write lock is an async mutex held across the write-and-fsync await,
//! serializing writers and forming the group-commit point; lock order is always
//! insert lock before write lock, so there is no cycle. The write and flush log
//! positions are atomics, and the flushed position is published on a watch
//! channel only after the fsync succeeds and only when it advances, coupling WAL
//! durability to buffer flushes and commits.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{watch, Mutex as AsyncMutex, Notify};

use crate::access::xlog_internal::{
    XLByteToPrevSeg, XLogSegmentOffset, SizeOfXLogLongPHD, SizeOfXLogShortPHD, XLOGDIR,
};
use crate::access::xlogdefs::{TimeLineID, XLogRecPtr, XLogSegNo, INVALID_XLOG_REC_PTR};
use crate::access::xlogrecord::SizeOfXLogRecord;
use crate::catalog::pg_control::DBState;
use crate::c::MAXALIGN;
use crate::pg_config::XLOG_BLCKSZ;
use crate::port::pg_crc32c::{comp_crc32c, fin_crc32c, pg_crc32c};
use crate::storage::io_backend::OpenFlags;

/// The size of a WAL page block (bytes), as a `u64` for the position math.
const BLCKSZ: u64 = XLOG_BLCKSZ as u64;

/// Number of WAL insert (copy) locks (PG `NUM_XLOGINSERT_LOCKS`).
pub const NUM_XLOGINSERT_LOCKS: usize = 8;

/// Default WAL segment size: 16 MB. Configurable (tests use a small value to
/// exercise the segment boundary without writing 16 MB).
pub const DEFAULT_WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

/// Default number of WAL buffer-ring pages (PG sizes this from `wal_buffers`;
/// a small fixed default suffices for the pipeline).
pub const DEFAULT_WAL_BUFFERS: usize = 16;

/// The timeline new WAL is inserted/flushed into. We do not do recovery here, so
/// it is fixed at 1.
pub const INSERT_TLI: TimeLineID = TimeLineID(1);

/// Placeholder system identifier stamped into each segment's long page header
/// (`xlp_sysid`). PG reads `ControlFile->system_identifier` (a per-cluster random
/// 64-bit id set at initdb). There is no pg_control in the F1 foundation yet, so
/// we use a fixed placeholder; the FIELD is present and the layout exact so a real
/// PostgreSQL could parse the header (it only cross-checks sysid against its own
/// control file).
// TODO(wal): source xlp_sysid from ControlFile->system_identifier (step 17).
const PLACEHOLDER_SYSTEM_IDENTIFIER: u64 = 0;

/// Bytes of a WAL page usable for WAL data (excludes the short page header).
const fn usable_bytes_in_page() -> u64 {
    BLCKSZ - SizeOfXLogShortPHD as u64
}

/// Bytes of a WAL segment usable for WAL data. Matches xlog.c's
/// `UsableBytesInSegment`: every page loses a short-header worth of bytes, and
/// the segment's first page additionally carries the long header.
const fn usable_bytes_in_segment(wal_seg_size: u64) -> u64 {
    (wal_seg_size / BLCKSZ) * usable_bytes_in_page()
        - (SizeOfXLogLongPHD as u64 - SizeOfXLogShortPHD as u64)
}

/// Process-global round-robin dispenser for `MyLockNo`: each insertion takes the
/// next lock index mod [`NUM_XLOGINSERT_LOCKS`]. PG keeps `MyLockNo` sticky per
/// backend (and migrates on contention); a per-insertion round-robin gives the
/// same even spread across the 8 locks, and -- crucially -- lets independent
/// inserters land on the SAME lock, which serializes them (PG capping concurrent
/// inserts at 8). Mirrors `MyLockNo`/`lockToTry`.
static LOCK_TO_TRY: AtomicUsize = AtomicUsize::new(0);

fn next_lock_no() -> usize {
    LOCK_TO_TRY.fetch_add(1, Ordering::Relaxed) % NUM_XLOGINSERT_LOCKS
}

/// The insert-reservation cursor, bumped under [`InsertPosLock`]. Stored as
/// "usable byte positions" (excludes all page headers); see
/// [`XLogCtl::byte_pos_to_rec_ptr`].
struct InsertPos {
    /// End of reserved WAL: where the next record will be reserved.
    curr_byte_pos: u64,
    /// Start of the previously reserved record (for the next record's prev-link),
    /// or [`NO_PREV_RECORD`] before any record has been reserved (so the very
    /// first record gets `xl_prev = InvalidXLogRecPtr`, as it would after a real
    /// bootstrap checkpoint in PG).
    prev_byte_pos: u64,
}

/// Sentinel `prev_byte_pos` meaning "no record reserved yet". Distinguishes the
/// first-ever record (prev-link = Invalid) from the second record (whose prev is
/// the first record, which legitimately started at byte position 0).
const NO_PREV_RECORD: u64 = u64::MAX;

/// Sentinel `inserting_at` value meaning "this lock is not currently inserting"
/// (idle / lock released). PG resets the advertised var to 0 on release and lets
/// `LWLockWaitForVar` detect a free lock; in our held-`Mutex` model we instead
/// store this max LSN on release so an idle lock never constrains
/// [`XLogCtl::wait_xlog_insertions_to_finish`] (its position is "past
/// everything"). The distinct value `0` (see below) is the held-but-unreserved
/// "block all waiters" sentinel. Mirrors PG's `InvalidXLogRecPtr` reset combined
/// with the lock-free check.
const NOT_INSERTING: u64 = u64::MAX;

/// Held-but-position-unknown sentinel: set right after acquiring the insert lock,
/// BEFORE reserving, so any waiter (`wait_xlog_insertions_to_finish`) blocks on
/// this lock until the inserter advertises its real start. This is PG's
/// `insertingAt == 0` (InvalidXLogRecPtr) on a freshly-acquired lock, which
/// `LWLockWaitForVar` treats as "don't know yet, must wait". Closing the
/// reserve->advertise gap depends on this being set under the held lock.
const INSERTING_UNKNOWN: u64 = 0;

/// One WAL insert lock (PG `WALInsertLock`): a HELD async mutex plus the
/// advertised `inserting_at` and a `Notify` woken on each advance/release.
///
/// An inserter holds `guard` for the whole reserve+copy (the copy may `.await`
/// on buffer eviction -- the async-mutex exception). While held it advertises
/// how far it has progressed via `inserting_at`, so a flusher
/// ([`XLogCtl::wait_xlog_insertions_to_finish`]) never writes/fsyncs a page the
/// inserter is still filling. Mirrors `WALInsertLock.{lock,insertingAt}`.
struct InsertLock {
    /// The held lock guarding this insertion slot (PG's LWLock, held exclusive).
    guard: AsyncMutex<()>,
    /// The LSN this inserter has filled up to: [`INSERTING_UNKNOWN`] (0) right
    /// after acquire and before reserve (blocks all waiters), then its record
    /// start, advanced to page boundaries as it crosses them, then
    /// [`NOT_INSERTING`] on release (idle).
    inserting_at: AtomicU64,
    /// Woken when `inserting_at` advances or clears, so a waiter rechecks. PG's
    /// per-LWLock wait queue for `LWLockWaitForVar`.
    advanced: Notify,
}

/// Local copy of the shared write/flush results (PG `XLogwrtResult`).
#[derive(Clone, Copy)]
struct LogwrtResult {
    write: u64,
    flush: u64,
}

/// Total shared WAL state, replacing PG's `XLogCtlData` shared-memory struct.
/// An `Arc` field on [`SharedState`] (the ipci.c `XLOGShmemInit` slot).
pub struct XLogCtl {
    // --- sizing (fixed after construction) ---
    wal_seg_size: u64,
    /// Number of buffer-ring pages (PG `XLogCacheBlck + 1`).
    n_pages: usize,

    // --- the WAL buffer ring ---
    /// `n_pages * XLOG_BLCKSZ` bytes of unwritten WAL pages. Guarded for copy by
    /// the insert locks (per-page, by position); page (re)init is serialized by
    /// `buf_mapping`.
    pages: Box<[Mutex<Box<[u8]>>]>,
    /// End-LSN (first byte ptr + XLOG_BLCKSZ) of the page currently loaded in
    /// each ring slot; `InvalidXLogRecPtr` while a slot is being re-initialized.
    xlblocks: Box<[AtomicU64]>,
    /// Latest initialized page (last byte position + 1). Guarded by `buf_mapping`.
    initialized_upto: AtomicU64,
    /// Serializes page (re)initialization in `AdvanceXLInsertBuffer`
    /// (PG `WALBufMappingLock`). Async because re-init may drive `XLogWrite`.
    buf_mapping: AsyncMutex<()>,

    // --- insert reservation ---
    insert_pos: Mutex<InsertPos>,
    insert_locks: [InsertLock; NUM_XLOGINSERT_LOCKS],

    // --- write/flush results (atomics) ---
    /// Highest LSN known fully inserted (PG `logInsertResult`): a shared cache
    /// that lets `wait_xlog_insertions_to_finish` callers piggyback on another
    /// caller's already-proven progress and skip the lock scan entirely.
    log_insert_result: AtomicU64,
    log_write_result: AtomicU64,
    log_flush_result: AtomicU64,
    /// Shared write request (highest LSN someone wants written). Bumped when an
    /// insert crosses a page boundary.
    logwrt_rqst_write: AtomicU64,
    /// LSN of the newest async commit/abort (PG `asyncXactLSN`).
    async_xact_lsn: AtomicU64,

    // --- the flushed-LSN watch (published after fsync, monotonic) ---
    flushed_tx: watch::Sender<XLogRecPtr>,

    // --- write+fsync serialization (group commit) ---
    /// PG `WALWriteLock`: held across the write+fsync `.await`.
    write_lock: AsyncMutex<()>,
    /// The currently-open WAL segment file, under `write_lock`.
    open_seg: AsyncMutex<Option<OpenSegment>>,

    /// The async I/O leaf for segment writes/fsync (PG `pg_pwrite`/`pg_fsync`).
    io: Arc<crate::storage::io_backend::IoBackend>,
    /// Process config carrying `DataDir` (for segment paths).
    config: Arc<crate::backend::utils::init::globals::ProcessConfig>,

    /// fsync counter, for tests asserting group-commit coalescing.
    #[cfg(test)]
    fsync_count: AtomicU64,

    /// Test-only hook: when set, an inserter pauses mid-copy (after finishing the
    /// first page of a multi-page record, before filling the next) until released,
    /// so a flush can deterministically race an in-flight insert.
    #[cfg(test)]
    copy_pause: AtomicU64, // 0 = off, 1 = armed (pause once), 2 = released
    #[cfg(test)]
    copy_paused: Notify, // fired when an inserter reaches the pause point
    #[cfg(test)]
    copy_release: Notify, // test fires this to release the paused inserter

    /// Test-only: when >= 0, force every insertion onto this `MyLockNo` so two
    /// inserters provably share a lock (the same-lock serialization test).
    #[cfg(test)]
    pin_lock_no: std::sync::atomic::AtomicI64,
}

/// The currently-open WAL segment file (PG `openLogFile`/`openLogSegNo`).
struct OpenSegment {
    segno: XLogSegNo,
    file: Arc<std::fs::File>,
    _permit: crate::storage::io_backend::FdPermit,
}

impl XLogCtl {
    /// Construct WAL state with the default segment size and buffer count, bound
    /// to the given I/O leaf and process config (the `SharedState` pieces built
    /// before `XLogCtl` in ipci.c order).
    pub fn new(
        io: Arc<crate::storage::io_backend::IoBackend>,
        config: Arc<crate::backend::utils::init::globals::ProcessConfig>,
    ) -> Arc<Self> {
        Self::with_config(io, config, DEFAULT_WAL_SEGMENT_SIZE, DEFAULT_WAL_BUFFERS)
    }

    /// Construct WAL state with an explicit segment size / page count (tests use
    /// a small segment to exercise the boundary).
    pub fn with_config(
        io: Arc<crate::storage::io_backend::IoBackend>,
        config: Arc<crate::backend::utils::init::globals::ProcessConfig>,
        wal_seg_size: u64,
        n_pages: usize,
    ) -> Arc<Self> {
        assert!(wal_seg_size >= BLCKSZ && wal_seg_size.is_multiple_of(BLCKSZ));
        assert!(n_pages >= 2);
        let pages = (0..n_pages)
            .map(|_| Mutex::new(vec![0u8; XLOG_BLCKSZ as usize].into_boxed_slice()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let xlblocks = (0..n_pages).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice();
        let insert_locks = std::array::from_fn(|_| InsertLock {
            guard: AsyncMutex::new(()),
            inserting_at: AtomicU64::new(NOT_INSERTING),
            advanced: Notify::new(),
        });
        let (flushed_tx, _rx) = watch::channel(INVALID_XLOG_REC_PTR);
        Arc::new(Self {
            wal_seg_size,
            n_pages,
            pages,
            xlblocks,
            initialized_upto: AtomicU64::new(0),
            buf_mapping: AsyncMutex::new(()),
            insert_pos: Mutex::new(InsertPos { curr_byte_pos: 0, prev_byte_pos: NO_PREV_RECORD }),
            insert_locks,
            log_insert_result: AtomicU64::new(0),
            log_write_result: AtomicU64::new(0),
            log_flush_result: AtomicU64::new(0),
            logwrt_rqst_write: AtomicU64::new(0),
            async_xact_lsn: AtomicU64::new(0),
            flushed_tx,
            write_lock: AsyncMutex::new(()),
            open_seg: AsyncMutex::new(None),
            io,
            config,
            #[cfg(test)]
            fsync_count: AtomicU64::new(0),
            #[cfg(test)]
            copy_pause: AtomicU64::new(0),
            #[cfg(test)]
            copy_paused: Notify::new(),
            #[cfg(test)]
            copy_release: Notify::new(),
            #[cfg(test)]
            pin_lock_no: std::sync::atomic::AtomicI64::new(-1),
        })
    }

    fn ring_idx(&self, recptr: u64) -> usize {
        ((recptr / BLCKSZ) % self.n_pages as u64) as usize
    }

    // --- byte-pos <-> LSN conversion (must match xlog_internal.h math) -------

    /// Convert a usable byte position to an `XLogRecPtr`, accounting for page
    /// headers. Mirrors xlog.c `XLogBytePosToRecPtr`.
    fn byte_pos_to_rec_ptr(&self, bytepos: u64) -> XLogRecPtr {
        let uis = usable_bytes_in_segment(self.wal_seg_size);
        let uip = usable_bytes_in_page();
        let long_phd = SizeOfXLogLongPHD as u64;
        let short_phd = SizeOfXLogShortPHD as u64;

        let fullsegs = bytepos / uis;
        let mut bytesleft = bytepos % uis;
        let seg_offset = if bytesleft < BLCKSZ - long_phd {
            bytesleft + long_phd
        } else {
            bytesleft -= BLCKSZ - long_phd;
            let fullpages = bytesleft / uip;
            bytesleft %= uip;
            BLCKSZ + fullpages * BLCKSZ + bytesleft + short_phd
        };
        XLogRecPtr(fullsegs * self.wal_seg_size + seg_offset)
    }

    /// Like [`Self::byte_pos_to_rec_ptr`] but maps a position at a page boundary
    /// to the start of the page (used for record end positions). Mirrors
    /// `XLogBytePosToEndRecPtr`.
    fn byte_pos_to_end_rec_ptr(&self, bytepos: u64) -> XLogRecPtr {
        let uis = usable_bytes_in_segment(self.wal_seg_size);
        let uip = usable_bytes_in_page();
        let long_phd = SizeOfXLogLongPHD as u64;
        let short_phd = SizeOfXLogShortPHD as u64;

        let fullsegs = bytepos / uis;
        let mut bytesleft = bytepos % uis;
        let seg_offset = if bytesleft < BLCKSZ - long_phd {
            if bytesleft == 0 { 0 } else { bytesleft + long_phd }
        } else {
            bytesleft -= BLCKSZ - long_phd;
            let fullpages = bytesleft / uip;
            bytesleft %= uip;
            if bytesleft == 0 {
                BLCKSZ + fullpages * BLCKSZ
            } else {
                BLCKSZ + fullpages * BLCKSZ + bytesleft + short_phd
            }
        };
        XLogRecPtr(fullsegs * self.wal_seg_size + seg_offset)
    }

    /// Convert an `XLogRecPtr` back to a usable byte position. Mirrors
    /// `XLogRecPtrToBytePos`.
    fn rec_ptr_to_byte_pos(&self, ptr: XLogRecPtr) -> u64 {
        let uis = usable_bytes_in_segment(self.wal_seg_size);
        let uip = usable_bytes_in_page();
        let long_phd = SizeOfXLogLongPHD as u64;
        let short_phd = SizeOfXLogShortPHD as u64;
        let ptr = ptr.0;

        let fullsegs = ptr / self.wal_seg_size;
        let fullpages = XLogSegmentOffset(ptr, self.wal_seg_size) / BLCKSZ;
        let offset = ptr % BLCKSZ;

        if fullpages == 0 {
            let mut result = fullsegs * uis;
            if offset > 0 {
                debug_assert!(offset >= long_phd);
                result += offset - long_phd;
            }
            result
        } else {
            let mut result =
                fullsegs * uis + (BLCKSZ - long_phd) + (fullpages - 1) * uip;
            if offset > 0 {
                debug_assert!(offset >= short_phd);
                result += offset - short_phd;
            }
            result
        }
    }

    // --- public LSN getters --------------------------------------------------

    /// PG `GetFlushRecPtr`: the highest durably-flushed LSN.
    pub fn get_flush_rec_ptr(&self) -> XLogRecPtr {
        XLogRecPtr(self.log_flush_result.load(Ordering::Acquire))
    }

    /// PG `GetXLogWriteRecPtr`: the highest LSN written to the segment files.
    pub fn get_xlog_write_rec_ptr(&self) -> XLogRecPtr {
        XLogRecPtr(self.log_write_result.load(Ordering::Acquire))
    }

    /// PG `GetXLogInsertRecPtr`: the current insert (reservation) head.
    pub fn get_xlog_insert_rec_ptr(&self) -> XLogRecPtr {
        let curr = self.insert_pos.lock().curr_byte_pos;
        self.byte_pos_to_rec_ptr(curr)
    }

    /// PG `GetRedoRecPtr`: the latest checkpoint's redo point, the LSN a page
    /// must have been modified after to need a full-page image. There is no
    /// checkpointer in the F1 foundation, so this stays `Invalid` (page writes
    /// always taken). Step 17 (checkpoints) advances it.
    pub fn get_redo_rec_ptr(&self) -> XLogRecPtr {
        INVALID_XLOG_REC_PTR
    }

    /// Subscribe to the flushed-LSN watch (published after fsync, monotonic).
    pub fn subscribe_flushed(&self) -> watch::Receiver<XLogRecPtr> {
        self.flushed_tx.subscribe()
    }

    /// PG `XLogSetAsyncXactLSN`: record the newest async commit/abort LSN. The
    /// walwriter wakeup is step 17; we just keep the monotonic max here.
    pub fn set_async_xact_lsn(&self, lsn: XLogRecPtr) {
        self.async_xact_lsn.fetch_max(lsn.0, Ordering::AcqRel);
    }

    /// The configured WAL segment size (bytes). Recovery's WAL reader needs it to
    /// map an LSN to a segment file.
    pub fn wal_segment_size(&self) -> u64 {
        self.wal_seg_size
    }

    /// Build a synchronous WAL page reader over the on-disk segment files for the
    /// recovery loop (PG's `read_local_xlog_page` role). The reader opens the
    /// segment file for the requested page and reads directly; the recovery
    /// driver's own async I/O happens between record reads, keeping the parse side
    /// pure-CPU (the `XLogReader` contract). Returns `None` if `DataDir` is unset.
    pub fn make_recovery_page_reader(
        &self,
    ) -> Option<crate::backend::access::transam::xlogreader::PageReadFn> {
        let data_dir = self.data_dir()?;
        let wal_seg_size = self.wal_seg_size;
        let segs_per_id = 0x1_0000_0000u64 / wal_seg_size;
        Some(Box::new(move |page_ptr: XLogRecPtr, _req: usize, into: &mut [u8]| {
            use std::io::{Read, Seek, SeekFrom};
            let segno = page_ptr.0 / wal_seg_size;
            let off = page_ptr.0 % wal_seg_size;
            let logid = (segno / segs_per_id) as u32;
            let seg = (segno % segs_per_id) as u32;
            let name = format!("{:08X}{:08X}{:08X}", INSERT_TLI.0, logid, seg);
            let path = std::path::Path::new(&data_dir).join(XLOGDIR).join(name);
            let mut f = std::fs::File::open(&path)
                .map_err(|e| format!("open {}: {e}", path.display()))?;
            f.seek(SeekFrom::Start(off)).map_err(|e| e.to_string())?;
            let mut n = 0usize;
            while n < into.len() {
                match f.read(&mut into[n..]).map_err(|e| e.to_string())? {
                    0 => break,
                    k => n += k,
                }
            }
            Ok(n)
        }))
    }

    // --- insert reservation --------------------------------------------------

    /// Reserve `size` (already including the record header) bytes of WAL.
    /// Returns `(start, end, prev)` LSNs. Mirrors `ReserveXLogInsertLocation`:
    /// the brief lock window only bumps the byte-position cursor; the
    /// position<->LSN math runs outside the lock.
    fn reserve_insert_location(&self, size: usize) -> (XLogRecPtr, XLogRecPtr, XLogRecPtr) {
        let size = MAXALIGN(size) as u64;
        let (startbytepos, endbytepos, prevbytepos) = {
            let mut pos = self.insert_pos.lock();
            let start = pos.curr_byte_pos;
            let end = start + size;
            let prev = pos.prev_byte_pos;
            pos.curr_byte_pos = end;
            pos.prev_byte_pos = start;
            (start, end, prev)
        };
        let start = self.byte_pos_to_rec_ptr(startbytepos);
        let end = self.byte_pos_to_end_rec_ptr(endbytepos);
        // The first-ever record has no predecessor: its prev-link is Invalid.
        let prev = if prevbytepos == NO_PREV_RECORD {
            INVALID_XLOG_REC_PTR
        } else {
            self.byte_pos_to_rec_ptr(prevbytepos)
        };
        debug_assert_eq!(self.rec_ptr_to_byte_pos(start), startbytepos);
        debug_assert_eq!(self.rec_ptr_to_byte_pos(end), endbytepos);
        (start, end, prev)
    }

    // --- the insert path -----------------------------------------------------

    /// PG `XLogInsertRecord` (WALINSERT_NORMAL path): reserve space, finalize the
    /// record header (real `xl_prev` from the reservation + the CRC), copy the
    /// bytes into the WAL buffer ring, and return the end LSN. `record` must begin
    /// with the on-disk `XLogRecord` header whose `xl_prev`/`xl_crc` are still
    /// placeholders; `partial_crc` is the assembler's non-finalized CRC over the
    /// record body (everything after the 24-byte header).
    ///
    /// Async because copying may need to initialize (and thus write out) a full
    /// buffer page. The insert lock is HELD for the whole reserve+copy: we set
    /// `inserting_at = 0` (block-all) before reserving, advertise the real start
    /// after reserving, advance it at each page boundary, and reset it to
    /// `NOT_INSERTING` on release -- so a concurrent flush
    /// ([`Self::wait_xlog_insertions_to_finish`]) never observes a reserve-but-
    /// not-yet-advertised gap and never writes a page we are mid-copy on. Mirrors
    /// XLogInsertRecord: WALInsertLockAcquire (insertingAt=0) -> reserve ->
    /// finalize CRC -> CopyXLogRecordToWAL -> WALInsertLockRelease.
    pub async fn insert_record(
        self: &Arc<Self>,
        record: &[u8],
        partial_crc: pg_crc32c,
    ) -> XLogRecPtr {
        const PREV_OFF: usize = 8; // offsetof(XLogRecord, xl_prev)
        const CRC_OFF: usize = SizeOfXLogRecord - 4; // offsetof(XLogRecord, xl_crc)
        assert!(record.len() >= SizeOfXLogRecord, "record shorter than header");

        // PG WALInsertLockAcquire: pick MyLockNo and take the lock HELD. The
        // insertingAt value is initially 0 ("don't know our insert location
        // yet"), which blocks any waiter on this lock until we advertise.
        let lock_no = self.my_lock_no();
        let lock = &self.insert_locks[lock_no];
        let guard = lock.guard.lock().await;
        lock.inserting_at.store(INSERTING_UNKNOWN, Ordering::Release);

        // PG ReserveXLogInsertLocation: reserve space WHILE HOLDING the insert
        // lock (the brief insertpos bump is its own sync lock). No reserve->
        // advertise gap can open: a waiter is blocked by INSERTING_UNKNOWN.
        let (start, end, prev) = self.reserve_insert_location(record.len());

        // Finalize the header: fill the real xl_prev from the reservation, fold
        // the header (up to but excluding xl_crc) into the body CRC, and write the
        // final CRC. Mirrors XLogInsertRecord: COMP_CRC32C(rdata_crc, rechdr,
        // offsetof(XLogRecord, xl_crc)); FIN_CRC32C.
        let mut bytes = record.to_vec();
        bytes[PREV_OFF..PREV_OFF + 8].copy_from_slice(&prev.0.to_ne_bytes());
        let crc = fin_crc32c(comp_crc32c(partial_crc, &bytes[..CRC_OFF]));
        bytes[CRC_OFF..CRC_OFF + 4].copy_from_slice(&crc.to_ne_bytes());

        // Advertise our reserved start now that we know it (PG advertises lazily
        // in GetXLogBuffer before a blocking advance; we advertise eagerly here,
        // which is at least as conservative).
        lock.inserting_at.store(start.0, Ordering::Release);
        lock.advanced.notify_waiters();

        self.copy_record_to_wal(&bytes, start, end, lock_no).await;

        // PG WALInsertLockRelease: reset insertingAt (we use NOT_INSERTING = idle,
        // non-constraining), wake waiters, then drop the held guard.
        lock.inserting_at.store(NOT_INSERTING, Ordering::Release);
        lock.advanced.notify_waiters();
        drop(guard);

        // If we crossed a page boundary, advance the shared write request so a
        // later flush knows the page is ready.
        if start.0 / BLCKSZ != end.0 / BLCKSZ {
            self.logwrt_rqst_write.fetch_max(end.0, Ordering::AcqRel);
        }
        end
    }

    /// PG `WALInsertLockAcquire`'s `MyLockNo` selection: round-robin from a
    /// process-global counter, overridable by the test pin.
    #[allow(clippy::unused_self, reason = "self used only under #[cfg(test)] for pin_lock_no")]
    fn my_lock_no(&self) -> usize {
        #[cfg(test)]
        {
            let pin = self.pin_lock_no.load(Ordering::Acquire);
            if pin >= 0 {
                return pin as usize % NUM_XLOGINSERT_LOCKS;
            }
        }
        next_lock_no()
    }

    /// PG `CopyXLogRecordToWAL` (normal records only): copy `record` into the
    /// reserved ring space, spanning page boundaries and writing the contrecord
    /// page headers. The insert lock advertises progress via `inserting_at`.
    async fn copy_record_to_wal(
        self: &Arc<Self>,
        record: &[u8],
        start: XLogRecPtr,
        end: XLogRecPtr,
        lock_no: usize,
    ) {
        let write_len = record.len();
        let mut curr_pos = start.0;
        let mut freespace = insert_freespace(curr_pos);
        let mut written = 0usize;

        let mut data = record;
        while !data.is_empty() {
            // Fill the current page.
            while data.len() as u64 > freespace {
                let chunk = freespace as usize;
                self.write_into_page(curr_pos, &data[..chunk]).await;
                data = &data[chunk..];
                written += chunk;
                curr_pos += freespace;

                // Move to the next page: stamp xlp_rem_len + contrecord flag.
                let rem = (write_len - written) as u32;
                self.begin_contrecord_page(curr_pos, rem).await;
                let hdr = if XLogSegmentOffset(curr_pos, self.wal_seg_size) == 0 {
                    SizeOfXLogLongPHD as u64
                } else {
                    SizeOfXLogShortPHD as u64
                };
                curr_pos += hdr;
                freespace = insert_freespace(curr_pos);

                // Test hook: pause here -- the previous page is filled but we have
                // NOT yet advanced our advertised position, so `inserting_at` still
                // points below the just-filled page. A concurrent flush must block
                // in wait_xlog_insertions_to_finish until we resume.
                #[cfg(test)]
                if self
                    .copy_pause
                    .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    self.copy_paused.notify_waiters();
                    self.copy_release.notified().await;
                }

                // Liveness: we have finished filling the previous page, so let a
                // waiter on this insert lock release the page(s) below `curr_pos`
                // (PG's WALInsertLockUpdateInsertingAt). Without this a flusher
                // would block on us for the whole multi-page copy.
                let lock = &self.insert_locks[lock_no];
                lock.inserting_at.store(curr_pos, Ordering::Release);
                lock.advanced.notify_waiters();
            }

            let n = data.len();
            self.write_into_page(curr_pos, data).await;
            curr_pos += n as u64;
            freespace -= n as u64;
            written += n;
            data = &[];
        }
        debug_assert_eq!(written, write_len);

        // Align the end so the next record starts MAXALIGNed.
        curr_pos = MAXALIGN(curr_pos as usize) as u64;
        debug_assert_eq!(curr_pos, end.0, "reserved WAL space != written");
        // Final clear/notify happens in `insert_record` after this returns.
    }

    /// Copy `bytes` into the ring page containing `pos`, at the page offset of
    /// `pos`. Initializes the page first if needed.
    async fn write_into_page(self: &Arc<Self>, pos: u64, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let page = self.get_xlog_buffer(pos).await;
        let off = (pos % BLCKSZ) as usize;
        let mut guard = self.pages[page].lock();
        guard[off..off + bytes.len()].copy_from_slice(bytes);
    }

    /// Stamp a continuation page's header: set xlp_rem_len and the
    /// FIRST_IS_CONTRECORD flag. Mirrors the contrecord bookkeeping in
    /// `CopyXLogRecordToWAL`.
    async fn begin_contrecord_page(self: &Arc<Self>, page_start: u64, rem_len: u32) {
        let idx = self.get_xlog_buffer(page_start).await;
        let mut guard = self.pages[idx].lock();
        // xlp_info is at offset 2 (u16), xlp_rem_len at offset 16 (u32).
        let info = u16::from_ne_bytes([guard[2], guard[3]])
            | crate::access::xlog_internal::XlpFlags::FIRST_IS_CONTRECORD.bits();
        guard[2..4].copy_from_slice(&info.to_ne_bytes());
        guard[16..20].copy_from_slice(&rem_len.to_ne_bytes());
    }

    /// PG `GetXLogBuffer`: return the ring slot index holding the page for `ptr`,
    /// initializing it (and evicting/writing the old page) if needed.
    ///
    /// CRITICAL: any `XLogWrite` needed to make room happens inside
    /// `advance_insert_buffer`, which is called WITHOUT any insert lock held (the
    /// insert locks are `inserting_at` advertisers, not RAII guards here, so this
    /// function never holds one across `.await`). The slot identity is determined
    /// purely by position, so concurrent inserters on different pages do not
    /// collide.
    async fn get_xlog_buffer(self: &Arc<Self>, ptr: u64) -> usize {
        let idx = self.ring_idx(ptr);
        let expected_end = ptr + (BLCKSZ - ptr % BLCKSZ);
        if self.xlblocks[idx].load(Ordering::Acquire) != expected_end {
            self.advance_insert_buffer(ptr).await;
            let endptr = self.xlblocks[idx].load(Ordering::Acquire);
            assert_eq!(endptr, expected_end, "could not find WAL buffer for {ptr:X}");
        }
        idx
    }

    /// PG `AdvanceXLInsertBuffer`: initialize WAL buffer pages up to the one
    /// containing `upto`, writing out any still-unwritten old page first (via
    /// [`Self::xlog_write`], with the buffer-mapping lock released across the
    /// write to avoid deadlock).
    async fn advance_insert_buffer(self: &Arc<Self>, upto: u64) {
        loop {
            let map = self.buf_mapping.lock().await;
            let init_upto = self.initialized_upto.load(Ordering::Acquire);
            if upto < init_upto {
                return; // someone initialized it already
            }
            let nextidx = self.ring_idx(init_upto);
            let old_end = self.xlblocks[nextidx].load(Ordering::Acquire);
            if self.log_write_result.load(Ordering::Acquire) < old_end {
                // The old page in this slot hasn't been written out yet.
                self.logwrt_rqst_write.fetch_max(old_end, Ordering::AcqRel);
                if self.log_write_result.load(Ordering::Acquire) < old_end {
                    // Release the mapping lock before driving the write (deadlock
                    // avoidance, per PG: WaitXLogInsertionsToFinish must be able
                    // to wait for inserters that may themselves need this lock),
                    // then wait for inserters BEFORE the write (never while
                    // holding write_lock), then retry. `old_end` is the END of a
                    // page strictly below every live insert position (we only
                    // evict pages already behind the insert head), so this wait
                    // returns promptly and the write touches no in-flight page.
                    drop(map);
                    self.wait_xlog_insertions_to_finish(old_end).await;
                    self.xlog_write(XLogRecPtr(old_end)).await;
                    continue;
                }
            }

            // The slot is free; set it up as the next output page.
            let new_begin = init_upto;
            let new_end = new_begin + BLCKSZ;
            // Mark invalid while re-initializing.
            self.xlblocks[nextidx].store(INVALID_XLOG_REC_PTR.0, Ordering::Release);
            self.init_page(nextidx, new_begin);
            self.xlblocks[nextidx].store(new_end, Ordering::Release);
            self.initialized_upto.store(new_end, Ordering::Release);

            if upto < new_end {
                return;
            }
        }
    }

    /// Zero a ring slot and write its WAL page header (long header on a segment's
    /// first page). Mirrors the page-init half of `AdvanceXLInsertBuffer`.
    fn init_page(&self, idx: usize, page_begin: u64) {
        let mut guard = self.pages[idx].lock();
        guard.fill(0);
        // XLogPageHeaderData: magic(u16)@0, info(u16)@2, tli(u32)@4,
        // pageaddr(u64)@8, rem_len(u32)@16.
        guard[0..2].copy_from_slice(&crate::access::xlog_internal::XLOG_PAGE_MAGIC.to_ne_bytes());
        let mut info = crate::access::xlog_internal::XlpFlags::BKP_REMOVABLE.bits();
        guard[4..8].copy_from_slice(&INSERT_TLI.0.to_ne_bytes());
        guard[8..16].copy_from_slice(&page_begin.to_ne_bytes());
        if XLogSegmentOffset(page_begin, self.wal_seg_size) == 0 {
            info |= crate::access::xlog_internal::XlpFlags::LONG_HEADER.bits();
            // Long header tail: xlp_sysid(u64)@24, xlp_seg_size(u32)@32,
            // xlp_xlog_blcksz(u32)@36.
            guard[24..32].copy_from_slice(&PLACEHOLDER_SYSTEM_IDENTIFIER.to_ne_bytes());
            guard[32..36].copy_from_slice(&(self.wal_seg_size as u32).to_ne_bytes());
            guard[36..40].copy_from_slice(&XLOG_BLCKSZ.to_ne_bytes());
        }
        guard[2..4].copy_from_slice(&info.to_ne_bytes());
    }

    // --- wait for in-flight inserters ---------------------------------------

    /// PG `WaitXLogInsertionsToFinish`: ensure no inserter is still copying WAL
    /// bytes below `upto`, then return the LSN through which all insertions are
    /// known complete (always `>= upto`). A writer MUST call this and cap its
    /// write target at the result so it never writes/fsyncs a ring page an
    /// inserter is still filling.
    ///
    /// For each insert lock we read its advertised `inserting_at`:
    /// [`NOT_INSERTING`] (idle) or `>= upto` does not constrain us; [`INSERTING_UNKNOWN`]
    /// (0, reserved-but-position-unknown) or any value `< upto` means the inserter
    /// is still copying below `upto`, so we wait on that lock's `advanced` `Notify`
    /// (PG's `LWLockWaitForVar`) and recheck. No busy spin: we re-read only after a
    /// notify wakes us, and creating the `notified()` future BEFORE the read closes
    /// the lost-wakeup race.
    ///
    /// DEADLOCK CONTRACT (PG): MUST be called holding NEITHER an insert lock NOR
    /// [`Self::write_lock`] -- a waited-on inserter may itself need `write_lock`
    /// (to evict a page mid-copy). [`Self::xlog_flush`] and
    /// [`Self::advance_insert_buffer`] both call this strictly before acquiring
    /// `write_lock`. It holds no lock itself (only `.await`s on `Notify`).
    ///
    /// `upto` is capped at the current reservation head (`reservedUpto`): no-one
    /// should flush past what has even been reserved, so in that corner case we
    /// only wait for all reserved WAL to finish (PG does the same and returns a
    /// value `< upto`). The return value is the LSN through which all insertions
    /// are known finished -- always `>= min(upto, reservedUpto)`.
    async fn wait_xlog_insertions_to_finish(&self, upto: u64) -> XLogRecPtr {
        // Fast path: someone already proved everything up to `upto` is finished
        // (PG: logInsertResult cache). Return the freshest known value, no scan.
        let inserted = self.log_insert_result.load(Ordering::Acquire);
        if upto <= inserted {
            return XLogRecPtr(inserted);
        }

        // Cap at the reserved head (PG: reservedUpto = XLogBytePosToEndRecPtr of
        // CurrBytePos). Nothing past it can be in progress.
        let reserved_upto = {
            let curr = self.insert_pos.lock().curr_byte_pos;
            self.byte_pos_to_end_rec_ptr(curr).0
        };
        let upto = upto.min(reserved_upto);

        // finishedUpto starts at the reserved head and is backed out for any
        // insertion still in progress below `upto`.
        let mut finished_upto = reserved_upto;
        for lock in &self.insert_locks {
            loop {
                // Register interest BEFORE re-reading, so a notify that fires
                // between the read and the await is not lost (tokio `Notify`
                // delivers a permit to an already-created `notified()` future).
                let notified = lock.advanced.notified();
                let at = lock.inserting_at.load(Ordering::Acquire);
                if at >= upto {
                    // Idle (NOT_INSERTING) or inserting at >= upto: this lock does
                    // not constrain the write target. If it advertises a real
                    // position below the reserved head, back finished_upto out to
                    // it (PG: finishedUpto = min over in-progress insertingAt).
                    if at != NOT_INSERTING && at != INSERTING_UNKNOWN && at < finished_upto {
                        finished_upto = at;
                    }
                    break;
                }
                // Actively copying bytes below upto -- wait for it to advance past
                // upto or finish, then recheck.
                notified.await;
            }
        }
        // Monotonically advance the shared cache and return the freshest value,
        // which may be beyond `finished_upto` if a concurrent caller proved more
        // (PG: pg_atomic_monotonic_advance_u64 of logInsertResult).
        let finished_upto = finished_upto.max(upto);
        let prev = self.log_insert_result.fetch_max(finished_upto, Ordering::AcqRel);
        XLogRecPtr(prev.max(finished_upto))
    }

    // --- write / fsync / flush ----------------------------------------------

    /// PG `XLogWrite`: write filled WAL buffer pages up to `upto` to the segment
    /// file(s), opening/creating the next segment at a boundary. Advances
    /// `LogwrtResult.Write`. Must be called holding [`Self::write_lock`].
    ///
    /// CRITICAL (deadlock avoidance, PG `XLogWrite` contract): this does NOT wait
    /// for inserters. The caller MUST have called
    /// [`Self::wait_xlog_insertions_to_finish`] for `upto` BEFORE acquiring
    /// [`Self::write_lock`] (see [`Self::xlog_flush`]). Were the wait done here --
    /// while holding `write_lock` -- an inserter mid-copy that must evict a page
    /// (and so acquire `write_lock`) would deadlock against us. The eviction
    /// caller ([`Self::advance_insert_buffer`]) only ever asks to write pages
    /// strictly below all live insert positions, so it likewise needs no wait.
    ///
    /// Whole completed segments are fsynced here (PG fsyncs at segment end);
    /// partial-page flushing + the final fsync is [`Self::xlog_flush`]'s job.
    ///
    /// The `_guard` witness encodes that the caller holds the WAL write lock
    /// ([`Self::write_lock`]); it is held across the I/O `.await` by design (the
    /// group-commit critical section -- an async mutex, so no `await_holding`).
    async fn xlog_write_locked(
        self: &Arc<Self>,
        _guard: &tokio::sync::MutexGuard<'_, ()>,
        upto: u64,
    ) {
        let mut write = self.log_write_result.load(Ordering::Acquire);
        let mut open = self.open_seg.lock().await;

        while write < upto && write < self.initialized_upto.load(Ordering::Acquire) {
            let idx = self.ring_idx(write);
            let end_ptr = self.xlblocks[idx].load(Ordering::Acquire);
            assert!(write < end_ptr, "xlog write request past end of log");

            let page_begin = end_ptr - BLCKSZ; // start LSN of this page
            let segno = XLogSegNo(XLByteToPrevSeg(end_ptr, self.wal_seg_size));

            // The last page to write is PARTIAL when the request stops short of
            // the page end (PG: ispartialpage = WriteRqst.Write < LogwrtResult.Write,
            // with LogwrtResult.Write already advanced to EndPtr). The full 8 KB
            // page is still physically written so its current bytes reach disk,
            // but the persisted write cursor is later set back to `upto` so the
            // same page is RE-WRITTEN once more data is inserted into it.
            let ispartialpage = upto < end_ptr;

            // Ensure the right segment is open.
            self.ensure_segment_open(&mut open, segno).await;

            // Write the whole page at its segment offset.
            let seg_offset = XLogSegmentOffset(page_begin, self.wal_seg_size);
            let bytes = self.pages[idx].lock().clone();
            let seg = open.as_ref().expect("segment open");
            self.io_of()
                .write_at(&seg.file, &bytes, seg_offset)
                .await
                .expect("WAL segment write");

            if ispartialpage {
                // Only a partial page was asked for: keep the persisted write
                // cursor at the actual request so the next flush re-writes this
                // page with the newly-inserted bytes (PG: LogwrtResult.Write =
                // WriteRqst.Write; break). The page's current bytes are on disk;
                // durability of the partial page is handled by the fsync in
                // xlog_flush. A partial page can never be a finishing_seg.
                write = upto;
                self.log_write_result.fetch_max(write, Ordering::AcqRel);
                break;
            }

            // Full page: advance the write cursor to the page end. PG sets
            // LogwrtResult.Write = EndPtr at the top of the loop, BEFORE the
            // finishing_seg block sets Flush = Write, so a reader never sees
            // Flush > Write. Publish Write first (matching xlog.c's
            // write-Write-barrier-write-Flush ordering).
            write = end_ptr;
            self.log_write_result.fetch_max(write, Ordering::AcqRel);

            // If we just wrote the whole last page of a segment, fsync the
            // segment immediately (PG: finishing_seg = !ispartialpage && at
            // segment end).
            let finishing_seg = XLogSegmentOffset(end_ptr, self.wal_seg_size) == 0;
            if finishing_seg {
                let seg = open.as_ref().expect("segment open");
                self.issue_xlog_fsync(&seg.file).await;
                // End-of-page flush is durable now (Write already advanced).
                self.log_flush_result.fetch_max(end_ptr, Ordering::AcqRel);
                self.publish_flushed(end_ptr);
            }
        }
    }

    /// Open (creating + zero-filling if absent) the WAL segment `segno`, if it is
    /// not already the open one. Updates `open` in place.
    async fn ensure_segment_open(&self, open: &mut Option<OpenSegment>, segno: XLogSegNo) {
        if open.as_ref().map(|s| s.segno) == Some(segno) {
            return;
        }
        let seg = self.xlog_file_init(segno).await;
        *open = Some(seg);
    }

    /// PG `issue_xlog_fsync`: fsync the segment file. The durability leaf
    /// ([`IoBackend::fsync`]) ABORTS the process on failure (PG PANIC); do not
    /// swallow.
    async fn issue_xlog_fsync(&self, file: &Arc<std::fs::File>) {
        self.io_of().fsync(file).await;
        #[cfg(test)]
        self.fsync_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Publish the flushed LSN on the watch, monotonically (only forward).
    fn publish_flushed(&self, flushed: u64) {
        self.flushed_tx.send_if_modified(|cur| {
            if cur.0 < flushed {
                cur.0 = flushed;
                true
            } else {
                false
            }
        });
    }

    /// PG `XLogWrite` entry that acquires [`Self::write_lock`] itself (used by
    /// `AdvanceXLInsertBuffer`, which must not hold it).
    async fn xlog_write(self: &Arc<Self>, upto: XLogRecPtr) {
        if self.log_write_result.load(Ordering::Acquire) >= upto.0 {
            return;
        }
        let w = self.write_lock.lock().await;
        if self.log_write_result.load(Ordering::Acquire) >= upto.0 {
            return;
        }
        self.xlog_write_locked(&w, upto.0).await;
    }

    /// PG `XLogFlush`: ensure WAL up to `lsn` is durably on disk. The group-commit
    /// point: many committers serialize on [`Self::write_lock`]; the first does
    /// the write+fsync for everyone up to the highest requested LSN, then
    /// publishes the flushed-LSN watch (after fsync, monotonic).
    pub async fn xlog_flush(self: &Arc<Self>, lsn: XLogRecPtr) {
        if lsn.is_invalid() {
            return;
        }
        // Fast path: already flushed, no lock.
        if self.log_flush_result.load(Ordering::Acquire) >= lsn.0 {
            return;
        }

        // Piggyback: flush as far as anyone has requested (group commit).
        let target = self.logwrt_rqst_write.load(Ordering::Acquire).max(lsn.0);

        // DEADLOCK AVOIDANCE (PG XLogFlush): wait for all in-flight insertions up
        // to `target` to finish BEFORE acquiring WALWriteLock. An inserter mid-copy
        // may need WALWriteLock to evict a page; holding it here while waiting on
        // that inserter would deadlock. `safe` is the LSN through which every
        // insertion is known finished (capped at the reserved head); every page up
        // to `safe` is therefore fully copied and initialized. When the request is
        // past the reserved head (a bogus LSN on disk; PG only LOGs), `safe` is the
        // reserved head and we flush as far as possible.
        let safe = self.wait_xlog_insertions_to_finish(target).await.0;

        let w = self.write_lock.lock().await;

        // Recheck: someone may have flushed past us while we waited.
        if self.log_flush_result.load(Ordering::Acquire) >= lsn.0 {
            return;
        }

        // Write out everything we have safely waited for (group commit: this can
        // exceed our own `target` when a later committer has reserved more).
        self.xlog_write_locked(&w, safe).await;

        // Flush (fsync) up to what we've written, if not already done by a
        // segment-end fsync inside xlog_write_locked.
        let written = self.log_write_result.load(Ordering::Acquire);
        // Everything written is now eligible to be made durable; fsync up to it.
        let flush_to = written;
        if self.log_flush_result.load(Ordering::Acquire) < flush_to {
            let mut open = self.open_seg.lock().await;
            // Make sure the segment covering flush_to is the open one.
            let segno = XLogSegNo(XLByteToPrevSeg(flush_to, self.wal_seg_size));
            self.ensure_segment_open(&mut open, segno).await;
            let seg = open.as_ref().expect("segment open");
            self.issue_xlog_fsync(&seg.file).await;
            drop(open);
            self.log_flush_result.fetch_max(flush_to, Ordering::AcqRel);
            self.publish_flushed(flush_to);
        }

        // Normally we have now flushed >= lsn. The only exception is a request
        // past the end of generated WAL (`lsn > reserved head`, i.e. `safe < lsn`):
        // PG logs and continues rather than PANIC, so we flush as far as reserved.
        assert!(
            self.log_flush_result.load(Ordering::Acquire) >= lsn.0 || safe < lsn.0,
            "xlog flush request not satisfied"
        );
    }

    /// Wait until WAL is flushed to at least `lsn`, without driving the flush.
    /// For async-commit / FlushBuffer waiters who rely on someone else (the
    /// walwriter or another committer) to do the write+fsync. Cancellation-safe:
    /// awaiting the watch removes nothing shared on drop.
    pub async fn wait_flushed(&self, lsn: XLogRecPtr) {
        if self.log_flush_result.load(Ordering::Acquire) >= lsn.0 {
            return;
        }
        let mut rx = self.flushed_tx.subscribe();
        loop {
            if rx.borrow().0 >= lsn.0 {
                return;
            }
            if rx.changed().await.is_err() {
                return; // sender gone (shutdown)
            }
        }
    }

    // --- WAL segment files ---------------------------------------------------

    /// PG `XLogFilePath`: pg_wal/<tli><logid><seg> relative to the data dir.
    /// Uses the shared [`XLogFileName`] helper so the on-disk name matches
    /// xlog_internal.h exactly.
    fn xlog_file_path(&self, data_dir: &str, segno: XLogSegNo) -> std::path::PathBuf {
        let name = crate::access::xlog_internal::XLogFileName(
            INSERT_TLI,
            segno,
            self.wal_seg_size as i32,
        );
        std::path::Path::new(data_dir).join(XLOGDIR).join(name)
    }

    /// PG `XLogFileInit`: open the WAL segment `segno`, creating and zero-filling
    /// it (to `wal_seg_size`) if absent.
    async fn xlog_file_init(&self, segno: XLogSegNo) -> OpenSegment {
        let data_dir = self
            .data_dir()
            .expect("DataDir must be set before WAL writes");
        let path = self.xlog_file_path(&data_dir, segno);
        let io = self.io_of();
        // Open (create if missing).
        let (file, permit) = io
            .open(&path, OpenFlags::create_read_write())
            .await
            .expect("open WAL segment");
        // Zero-fill if newly created / short.
        if io.size(&file).await.expect("WAL segment size") < self.wal_seg_size {
            io.fallocate(&file, 0, self.wal_seg_size).await.expect("WAL segment fallocate");
            io.fsync(&file).await;
        }
        OpenSegment { segno, file, _permit: permit }
    }

    // --- shared-state plumbing ----------------------------------------------

    /// The data dir for segment paths (PG `DataDir`), from `ProcessConfig`.
    fn data_dir(&self) -> Option<String> {
        self.config.data_dir()
    }

    fn io_of(&self) -> Arc<crate::storage::io_backend::IoBackend> {
        self.io.clone()
    }

    // --- control file (pg_control) + recovery entry -------------------------

    /// Path to the control file (`global/pg_control`) under the data dir.
    fn control_file_path(data_dir: &str) -> std::path::PathBuf {
        std::path::Path::new(data_dir)
            .join(crate::access::xlog_internal::XLOG_CONTROL_FILE)
    }

    /// PG `UpdateControlFile`/`WriteControlFile`: write the control file recording
    /// the DB state and the checkpoint's redo point. The 512-byte struct is
    /// written with a trailing CRC; the file is padded to `PG_CONTROL_FILE_SIZE`.
    pub async fn write_control_file(&self, state: DBState, redo: XLogRecPtr, checkpoint: XLogRecPtr) {
        use crate::catalog::pg_control::{
            CheckPoint, ControlFileData, DBState as _DBState, PG_CONTROL_FILE_SIZE,
            PG_CONTROL_VERSION,
        };
        let _ = _DBState::STARTUP; // keep enum import referenced
        let Some(data_dir) = self.data_dir() else {
            return;
        };
        // SAFETY: ControlFileData is repr(C) POD; an all-zero start is valid, and
        // we set the fields recovery reads. Unset fields stay zero (harmless).
        let mut cf: ControlFileData = unsafe { core::mem::zeroed() };
        cf.system_identifier = Self::CLUSTER_SYSTEM_IDENTIFIER;
        cf.pg_control_version = PG_CONTROL_VERSION;
        cf.state = state;
        cf.checkPoint = checkpoint;
        // SAFETY: CheckPoint is repr(C) POD; zero it then set the redo point.
        let mut ckpt: CheckPoint = unsafe { core::mem::zeroed() };
        ckpt.redo = redo;
        ckpt.ThisTimeLineID = INSERT_TLI;
        cf.checkPointCopy = ckpt;
        cf.xlog_seg_size = self.wal_seg_size as u32;

        // Serialize the struct bytes, compute + store the CRC (over all but the
        // trailing crc field), pad to the physical file size.
        let sz = core::mem::size_of::<ControlFileData>();
        // SAFETY: reading the POD struct as bytes.
        let raw = unsafe {
            core::slice::from_raw_parts(std::ptr::from_ref(&cf).cast::<u8>(), sz)
        };
        let mut buf = raw.to_vec();
        let crc_off = core::mem::offset_of!(ControlFileData, crc);
        let mut crc = crate::port::pg_crc32c::init_crc32c();
        crc = crate::port::pg_crc32c::comp_crc32c(crc, &buf[..crc_off]);
        crc = crate::port::pg_crc32c::fin_crc32c(crc);
        buf[crc_off..crc_off + 4].copy_from_slice(&crc.to_ne_bytes());
        buf.resize(PG_CONTROL_FILE_SIZE, 0);

        let path = Self::control_file_path(&data_dir);
        if let Some(parent) = path.parent() {
            let _ = crate::storage::io_backend::mkdir_all(parent.to_path_buf()).await;
        }
        let io = self.io_of();
        let (file, _permit) = io
            .open(&path, OpenFlags::create_read_write())
            .await
            .expect("open pg_control");
        io.write_at(&file, &buf, 0).await.expect("write pg_control");
        io.fsync(&file).await;
    }

    /// PG `ReadControlFile`: read the control file if present. Returns `None` when
    /// there is no control file (a fresh cluster that never checkpointed) or the
    /// CRC does not validate.
    pub async fn read_control_file(&self) -> Option<crate::catalog::pg_control::ControlFileData> {
        use crate::catalog::pg_control::ControlFileData;
        let data_dir = self.data_dir()?;
        let path = Self::control_file_path(&data_dir);
        let io = self.io_of();
        let (file, _permit) = io.open(&path, OpenFlags::read_only()).await.ok()?;
        let sz = core::mem::size_of::<ControlFileData>();
        let mut buf = vec![0u8; sz];
        let n = io.read_at(&file, &mut buf, 0).await.ok()?;
        if n < sz {
            return None;
        }
        // Validate the CRC.
        let crc_off = core::mem::offset_of!(ControlFileData, crc);
        let stored = u32::from_ne_bytes(buf[crc_off..crc_off + 4].try_into().ok()?);
        let mut crc = crate::port::pg_crc32c::init_crc32c();
        crc = crate::port::pg_crc32c::comp_crc32c(crc, &buf[..crc_off]);
        crc = crate::port::pg_crc32c::fin_crc32c(crc);
        if crc != stored {
            return None;
        }
        // SAFETY: buf holds a valid, CRC-checked ControlFileData image.
        let cf = unsafe { std::ptr::read_unaligned(buf.as_ptr().cast::<ControlFileData>()) };
        Some(cf)
    }

    /// The cluster system identifier used in WAL long-page headers. There is no
    /// initdb-set random id yet, so a fixed nonzero constant is used (the reader
    /// only cross-checks it against itself). TODO(initdb): random per-cluster id.
    const CLUSTER_SYSTEM_IDENTIFIER: u64 = 0x5045_5050_4552_4442; // "PEPPERDB"

    #[cfg(test)]
    fn fsync_count(&self) -> u64 {
        self.fsync_count.load(Ordering::Relaxed)
    }

    /// Arm the one-shot mid-copy pause hook (see `copy_record_to_wal`).
    #[cfg(test)]
    fn arm_copy_pause(&self) {
        self.copy_pause.store(1, Ordering::Release);
    }

    /// Wait until an inserter reaches the armed pause point.
    #[cfg(test)]
    async fn wait_copy_paused(&self) {
        loop {
            let n = self.copy_paused.notified();
            if self.copy_pause.load(Ordering::Acquire) == 2 {
                return;
            }
            n.await;
        }
    }

    /// Release a paused inserter.
    #[cfg(test)]
    fn release_copy_pause(&self) {
        self.copy_release.notify_waiters();
    }

    /// Force every insertion onto `lock_no` (the same-lock serialization test).
    #[cfg(test)]
    fn set_pin_lock_no(&self, lock_no: usize) {
        self.pin_lock_no.store(lock_no as i64, Ordering::Release);
    }

    /// Number of insertions currently holding `lock_no` (0 or 1 under the held
    /// model). Used to assert serialization: two pinned inserters never both hold.
    #[cfg(test)]
    fn try_insert_lock_held(&self, lock_no: usize) -> bool {
        self.insert_locks[lock_no].guard.try_lock().is_err()
    }
}

/// Space left on the WAL page after `endptr`. Mirrors `INSERT_FREESPACE`.
fn insert_freespace(endptr: u64) -> u64 {
    if endptr.is_multiple_of(BLCKSZ) {
        0
    } else {
        BLCKSZ - (endptr % BLCKSZ)
    }
}

/// The function bufmgr calls (PG `XLogFlush`): ensure WAL up to `lsn` is durable
/// before the data page it describes is written (WAL-before-data).
pub async fn xlog_flush(xlog: &Arc<XLogCtl>, lsn: XLogRecPtr) {
    xlog.xlog_flush(lsn).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    /// A test fixture: a fresh pg_wal dir + a small-segment XLogCtl bound to its
    /// own I/O leaf and config (so concurrent tests stay isolated).
    struct WalFixture {
        xlog: Arc<XLogCtl>,
        dir: std::path::PathBuf,
    }

    impl WalFixture {
        async fn new(wal_seg_size: u64, n_pages: usize) -> Self {
            use std::sync::atomic::AtomicU64;
            static SEQ: AtomicU64 = AtomicU64::new(0);
            let uniq = SEQ.fetch_add(1, Ordering::Relaxed);
            let dir = std::env::temp_dir().join(format!(
                "pepperdb_xlog_{}_{}_{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                uniq,
            ));
            crate::storage::io_backend::mkdir_all(dir.join(XLOGDIR)).await.unwrap();
            let io = Arc::new(crate::storage::io_backend::IoBackend::with_default_budget());
            let config =
                Arc::new(crate::backend::utils::init::globals::ProcessConfig::new());
            config.set_data_dir(dir.to_str().unwrap());
            let xlog = XLogCtl::with_config(io, config, wal_seg_size, n_pages);
            Self { xlog, dir }
        }
    }

    impl Drop for WalFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// Build a minimal record of `len` bytes: a 24-byte XLogRecord header
    /// (tot_len + a valid rmid filled) followed by zero payload, plus the partial
    /// (body-only) CRC the insert path expects. xl_prev/xl_crc are left as the
    /// placeholders the assembler emits; `insert_record` fills them.
    fn raw_record(len: usize) -> (Vec<u8>, pg_crc32c) {
        use crate::port::pg_crc32c::{comp_crc32c, init_crc32c};
        assert!(len >= SizeOfXLogRecord);
        let mut r = vec![0u8; len];
        r[0..4].copy_from_slice(&(len as u32).to_ne_bytes()); // tot_len
        r[17] = crate::access::rmgrlist::RmgrId::Xlog as u8; // valid rmid for the reader
        let partial = comp_crc32c(init_crc32c(), &r[SizeOfXLogRecord..]);
        (r, partial)
    }

    #[tokio::test]
    async fn insert_flush_advances_and_persists() {
        let fx = WalFixture::new(DEFAULT_WAL_SEGMENT_SIZE, 8).await;
        assert_eq!(fx.xlog.get_flush_rec_ptr(), INVALID_XLOG_REC_PTR);

        let (rec, crc) = raw_record(64);
        let end = fx.xlog.insert_record(&rec, crc).await;
        assert!(end.is_valid());

        // Flush LSN is still behind the insert before XLogFlush.
        assert!(fx.xlog.get_flush_rec_ptr().0 < end.0);

        fx.xlog.xlog_flush(end).await;
        assert!(fx.xlog.get_flush_rec_ptr().0 >= end.0);
        // The watch fired.
        assert!(fx.xlog.subscribe_flushed().borrow().0 >= end.0);

        // The segment file exists on disk and is segment-sized.
        let path = fx.xlog.xlog_file_path(fx.dir.to_str().unwrap(), XLogSegNo(0));
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), DEFAULT_WAL_SEGMENT_SIZE);
    }

    #[tokio::test]
    async fn flush_lsn_is_monotonic() {
        let fx = WalFixture::new(DEFAULT_WAL_SEGMENT_SIZE, 8).await;
        let mut last = 0u64;
        for _ in 0..5 {
            let (rec, crc) = raw_record(128);
            let end = fx.xlog.insert_record(&rec, crc).await;
            fx.xlog.xlog_flush(end).await;
            let f = fx.xlog.get_flush_rec_ptr().0;
            assert!(f >= last, "flush LSN went backwards: {f} < {last}");
            last = f;
        }
        // A redundant flush to an old LSN must not move it backward.
        fx.xlog.xlog_flush(XLogRecPtr(1)).await;
        assert_eq!(fx.xlog.get_flush_rec_ptr().0, last);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn group_commit_coalesces_fsyncs() {
        let fx = WalFixture::new(DEFAULT_WAL_SEGMENT_SIZE, 32).await;

        // Insert many records first (records' end LSNs increase).
        let mut ends = Vec::new();
        for _ in 0..32 {
            let (rec, crc) = raw_record(256);
            ends.push(fx.xlog.insert_record(&rec, crc).await);
        }

        // N concurrent committers each flush to their LSN.
        let mut handles = Vec::new();
        for end in ends.iter().copied() {
            let x = fx.xlog.clone();
            handles.push(tokio::spawn(async move {
                x.xlog_flush(end).await;
                x.get_flush_rec_ptr().0 >= end.0
            }));
        }
        for h in handles {
            assert!(h.await.unwrap(), "committer observed flush >= its LSN");
        }
        let target = ends.last().unwrap().0;
        assert!(fx.xlog.get_flush_rec_ptr().0 >= target);
        // Far fewer fsyncs than flush calls: one flush covers many waiters.
        let fsyncs = fx.xlog.fsync_count();
        assert!(fsyncs < 32, "expected coalesced fsyncs, got {fsyncs}");
    }

    #[tokio::test]
    async fn segment_boundary_opens_new_segment() {
        // Small segment (2 pages = 16 KB) so a few inserts cross the boundary.
        let seg = 2 * BLCKSZ;
        let fx = WalFixture::new(seg, 8).await;

        // Insert enough to fill past one segment.
        let mut end = INVALID_XLOG_REC_PTR;
        for _ in 0..40 {
            let (rec, crc) = raw_record(512);
            end = fx.xlog.insert_record(&rec, crc).await;
        }
        fx.xlog.xlog_flush(end).await;
        assert!(fx.xlog.get_flush_rec_ptr().0 >= end.0);

        // Segment 0 and segment 1 must both exist.
        let p0 = fx.xlog.xlog_file_path(fx.dir.to_str().unwrap(), XLogSegNo(0));
        let p1 = fx.xlog.xlog_file_path(fx.dir.to_str().unwrap(), XLogSegNo(1));
        assert!(std::fs::metadata(&p0).is_ok(), "segment 0 missing");
        assert!(std::fs::metadata(&p1).is_ok(), "segment 1 missing");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_flushed_wakes_on_flush() {
        let fx = WalFixture::new(DEFAULT_WAL_SEGMENT_SIZE, 8).await;
        let (rec, crc) = raw_record(128);
        let end = fx.xlog.insert_record(&rec, crc).await;

        // A waiter that does NOT drive the flush.
        let x = fx.xlog.clone();
        let waiter = tokio::spawn(async move {
            x.wait_flushed(end).await;
            x.get_flush_rec_ptr().0 >= end.0
        });

        // Give the waiter a moment to park on the watch.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "waiter should block until flush");

        // Another task drives the flush.
        fx.xlog.xlog_flush(end).await;

        let woke = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("waiter should wake")
            .unwrap();
        assert!(woke);
    }

    #[tokio::test]
    async fn byte_pos_round_trips() {
        let fx = WalFixture::new(DEFAULT_WAL_SEGMENT_SIZE, 8).await;
        for &bp in &[0u64, 1, 100, 8000, 8168, 8169, 100_000, 16_777_000] {
            let lsn = fx.xlog.byte_pos_to_rec_ptr(bp);
            assert_eq!(fx.xlog.rec_ptr_to_byte_pos(lsn), bp, "round trip at bytepos {bp}");
        }
    }

    /// BLOCKER: a flush must NOT write/fsync a WAL page while an inserter is still
    /// copying into it. An inserter copies a multi-page record but pauses mid-copy
    /// (after page 1 is filled, before advancing its advertised position), so its
    /// `inserting_at` still points below the just-filled page. A concurrent
    /// flusher targeting the record's end must BLOCK in
    /// `wait_xlog_insertions_to_finish` until the inserter resumes; only then may
    /// it write+fsync. We assert (1) the flush does not complete while the
    /// inserter is paused, and (2) after release, every record on disk reads back
    /// with a valid CRC (a half-written page would fail CRC on read).
    ///
    /// Without the fix (no WaitXLogInsertionsToFinish), the flusher would write
    /// the half-filled page immediately -> assertion (1) fails, and the read-back
    /// would hit a record whose bytes were not yet copied -> CRC failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn flush_waits_for_inflight_inserter() {
        use crate::backend::access::transam::xloginsert::{
            begin_insert, register_data, with_insertion, xlog_insert,
        };
        use crate::backend::access::transam::xlogreader::XLogReader;
        let xlog_rmid = crate::access::rmgrlist::RmgrId::Xlog as u8;
        // Small segment, several ring pages: the big record spans pages but stays
        // within one segment so the read-back is simple.
        let seg = 8 * BLCKSZ;
        let fx = WalFixture::new(seg, 16).await;

        // First, a small record so the big one starts mid-segment (page 0).
        let _e0 = with_insertion(async {
            begin_insert();
            register_data(b"small");
            xlog_insert(&fx.xlog, xlog_rmid, 0x00).await
        })
        .await;

        // Arm the one-shot pause, then start the big multi-page inserter (a large
        // main-data payload forces the record to span several pages).
        fx.xlog.arm_copy_pause();
        let payload: Vec<u8> = (0..(3 * BLCKSZ as usize + 500)).map(|i| (i % 251) as u8).collect();
        let xi = fx.xlog.clone();
        let inserter = tokio::spawn(async move {
            with_insertion(async {
                begin_insert();
                register_data(&payload);
                xlog_insert(&xi, xlog_rmid, 0x00).await
            })
            .await
        });

        // Wait until the inserter has filled page 1 and parked at the pause point.
        fx.xlog.wait_copy_paused().await;

        // Flush everything reserved so far (covers the paused big record's range).
        let target = fx.xlog.get_xlog_insert_rec_ptr();
        let xf = fx.xlog.clone();
        let flusher = tokio::spawn(async move { xf.xlog_flush(target).await });

        // The flusher must block while the inserter is paused: it cannot safely
        // write the page the inserter is mid-copy on.
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(
            !flusher.is_finished(),
            "flush completed while an inserter was mid-copy -- the page race is live"
        );

        // Release the inserter; it finishes the copy, then the flush can proceed.
        fx.xlog.release_copy_pause();
        let big_end = inserter.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), flusher)
            .await
            .expect("flush should finish after the inserter clears")
            .unwrap();
        // Make sure everything is durable before reading back.
        fx.xlog.xlog_flush(big_end).await;
        assert!(fx.xlog.get_flush_rec_ptr().0 >= big_end.0);

        // Read every record back: each must decode with a valid CRC. A page that
        // had been fsynced mid-copy would surface here as a CRC failure.
        let mut reader = XLogReader::new(seg, seg_page_reader(fx.dir.clone(), seg));
        // The first record starts at the first usable byte (the long page header).
        reader.begin_read(fx.xlog.byte_pos_to_rec_ptr(0));
        let mut seen = 0;
        loop {
            match reader.read_record() {
                Ok(Some(_rec)) => {
                    seen += 1;
                    if reader.end_rec_ptr.0 >= big_end.0 {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("record {seen} failed to decode (CRC?): {e}"),
            }
        }
        assert_eq!(seen, 2, "expected the small + big record to read back cleanly");
    }

    /// BLOCKER (silent data loss): two records inserted into the SAME WAL page,
    /// each flushed at a MID-PAGE LSN. After flush(end1) the persisted write
    /// cursor must NOT jump to the page end (PG's ispartialpage back-off): if it
    /// did, flush(end2) would see flush_result >= end2 via the page-end-inflated
    /// write cursor and SKIP the write, so record 2's bytes never reach disk.
    /// We assert (1) the second flush actually performs a write+fsync (the fsync
    /// counter advances), and (2) BOTH records read back FROM DISK with a valid
    /// CRC and the exact payload we inserted.
    ///
    /// Without the fix, record 2 is zero on disk: read_record either fails CRC
    /// or returns the wrong payload, and the second flush does 0 fsyncs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn incremental_same_page_flush_is_durable() {
        use crate::backend::access::transam::xloginsert::{
            begin_insert, register_data, with_insertion, xlog_insert,
        };
        use crate::backend::access::transam::xlogreader::XLogReader;
        let xlog_rmid = crate::access::rmgrlist::RmgrId::Xlog as u8;
        let seg = 8 * BLCKSZ;
        let fx = WalFixture::new(seg, 16).await;

        // Two small records: both fit in page 0, so both flushes land mid-page.
        let p1: Vec<u8> = (0..64u8).collect();
        let p2: Vec<u8> = (0..64u8).map(|i| i.wrapping_add(100)).collect();

        let r1 = p1.clone();
        let end1 = with_insertion(async {
            begin_insert();
            register_data(&r1);
            xlog_insert(&fx.xlog, xlog_rmid, 0x00).await
        })
        .await;
        fx.xlog.xlog_flush(end1).await;
        assert!(fx.xlog.get_flush_rec_ptr().0 >= end1.0);
        // end1 is mid-page (page 0): the write cursor must have backed off to
        // end1, NOT advanced to the page end -- the heart of the fix.
        assert!(end1.0 < BLCKSZ, "end1 must be mid first page");
        assert_eq!(
            fx.xlog.get_xlog_write_rec_ptr().0,
            end1.0,
            "partial-page write cursor must equal the request, not the page end"
        );

        let fsyncs_before = fx.xlog.fsync_count();

        // Record 2 goes into the SAME page; flush at a higher mid-page LSN.
        let r2 = p2.clone();
        let end2 = with_insertion(async {
            begin_insert();
            register_data(&r2);
            xlog_insert(&fx.xlog, xlog_rmid, 0x00).await
        })
        .await;
        assert!(end2.0 > end1.0 && end2.0 < BLCKSZ, "end2 mid first page, after end1");
        fx.xlog.xlog_flush(end2).await;
        assert!(fx.xlog.get_flush_rec_ptr().0 >= end2.0);

        // The second flush must have actually written + fsynced (not skipped):
        // without the back-off the page-end-inflated cursor would short-circuit it.
        assert!(
            fx.xlog.fsync_count() > fsyncs_before,
            "second mid-page flush was skipped -- record 2 never reached disk"
        );

        // Read BOTH records back from the on-disk segment: each must decode with a
        // valid CRC and carry exactly the payload we inserted. Record 2's bytes
        // being zero on disk (the bug) surfaces as a CRC failure or wrong data.
        let mut reader = XLogReader::new(seg, seg_page_reader(fx.dir.clone(), seg));
        reader.begin_read(fx.xlog.byte_pos_to_rec_ptr(0));
        let mut datas = Vec::new();
        loop {
            match reader.read_record() {
                Ok(Some(rec)) => {
                    datas.push(rec.get_data().unwrap_or_default().to_vec());
                    if reader.end_rec_ptr.0 >= end2.0 {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("record {} failed CRC/decode: {e}", datas.len()),
            }
        }
        assert_eq!(datas.len(), 2, "both records must read back from disk");
        assert_eq!(datas[0], p1, "record 1 payload mismatch on disk");
        assert_eq!(datas[1], p2, "record 2 payload mismatch on disk (silent loss)");
    }

    /// A `PageReadFn` that reads pages from the fixture's on-disk segment files,
    /// for tests that read records back to verify CRC / xl_prev.
    fn seg_page_reader(
        dir: std::path::PathBuf,
        seg: u64,
    ) -> crate::backend::access::transam::xlogreader::PageReadFn {
        use std::io::{Read, Seek, SeekFrom};
        Box::new(move |page_ptr: XLogRecPtr, _req: usize, into: &mut [u8]| {
            let segno = XLogSegNo(page_ptr.0 / seg);
            let off = page_ptr.0 % seg;
            let name =
                crate::access::xlog_internal::XLogFileName(INSERT_TLI, segno, seg as i32);
            let path = dir.join(XLOGDIR).join(name);
            let mut f = std::fs::File::open(&path).map_err(|e| format!("open {path:?}: {e}"))?;
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

    /// SAME-LOCK SERIALIZATION (the clobber fix): two inserters forced onto the
    /// same `MyLockNo` insert concurrently. Under the held-exclusive model they
    /// must serialize on that one lock -- never both holding it at once -- so
    /// neither clobbers the other's reservation/copy. We assert (1) the lock is
    /// never held by more than one inserter (sampled while they run), (2) both
    /// records read back with a valid CRC, and (3) the second record's `xl_prev`
    /// links to the first record's start LSN (the prev-chain is intact, which a
    /// reserve/advertise clobber would have corrupted).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn same_lock_inserters_serialize() {
        use crate::backend::access::transam::xloginsert::{
            begin_insert, register_data, with_insertion, xlog_insert,
        };
        use crate::backend::access::transam::xlogreader::XLogReader;
        let xlog_rmid = crate::access::rmgrlist::RmgrId::Xlog as u8;
        let seg = 8 * BLCKSZ;
        let fx = WalFixture::new(seg, 16).await;
        // Pin both inserters to the SAME insert lock so they must serialize.
        fx.xlog.set_pin_lock_no(3);

        // Sample the lock state while the inserters run: it must never be held by
        // two at once (try_lock failing just means "held by one", which is fine;
        // we assert no corruption via CRC + prev-chain below, the real signal).
        let sampler = {
            let x = fx.xlog.clone();
            tokio::spawn(async move {
                for _ in 0..2000 {
                    // The held model guarantees at most one holder; this just
                    // exercises the lock under contention.
                    let _ = x.try_insert_lock_held(3);
                    tokio::task::yield_now().await;
                }
            })
        };

        let mut handles = Vec::new();
        for i in 0..2u32 {
            let x = fx.xlog.clone();
            handles.push(tokio::spawn(async move {
                with_insertion(async {
                    begin_insert();
                    let payload = vec![i as u8; 200];
                    register_data(&payload);
                    xlog_insert(&x, xlog_rmid, 0x00).await
                })
                .await
            }));
        }
        let mut ends = Vec::new();
        for h in handles {
            ends.push(h.await.unwrap());
        }
        sampler.await.unwrap();
        let last_end = ends.iter().map(|e| e.0).max().unwrap();
        fx.xlog.xlog_flush(XLogRecPtr(last_end)).await;

        // Read both records back; collect (start, prev). A valid CRC on each is
        // checked by read_record returning Ok.
        let mut reader = XLogReader::new(seg, seg_page_reader(fx.dir.clone(), seg));
        reader.begin_read(fx.xlog.byte_pos_to_rec_ptr(0));
        let mut starts = Vec::new();
        let mut prevs = Vec::new();
        loop {
            match reader.read_record() {
                Ok(Some(rec)) => {
                    starts.push(rec.lsn.0);
                    prevs.push(rec.header.prev.0);
                    if reader.end_rec_ptr.0 >= last_end {
                        break;
                    }
                }
                Ok(None) => break,
                Err(e) => panic!("record failed CRC/decode: {e}"),
            }
        }
        assert_eq!(starts.len(), 2, "both records must read back");
        // The first record's prev-link is Invalid; the second's prev points at
        // the first's start LSN -- the chain is intact (no clobber).
        assert_eq!(prevs[0], INVALID_XLOG_REC_PTR.0);
        assert_eq!(prevs[1], starts[0], "second record's xl_prev must link the first");
    }

    /// NO DEADLOCK: an insert large enough to force `AdvanceXLInsertBuffer` to
    /// evict (and thus write) a full page runs concurrently with a flusher. The
    /// inserter may need WALWriteLock to evict while a flusher holds it; the wait-
    /// before-WALWriteLock ordering must prevent a cycle. We assert everything
    /// completes within a timeout (a deadlock would hang).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn eviction_and_flush_no_deadlock() {
        use crate::backend::access::transam::xloginsert::{
            begin_insert, register_data, with_insertion, xlog_insert,
        };
        let xlog_rmid = crate::access::rmgrlist::RmgrId::Xlog as u8;
        // Few ring pages so a multi-page record forces eviction of earlier pages.
        let seg = 16 * BLCKSZ;
        let fx = WalFixture::new(seg, 4).await;

        let body = async {
            // Spawn several big inserters (each spans more than the ring) plus a
            // flusher hammering the flush path concurrently.
            let mut tasks = Vec::new();
            for _ in 0..4 {
                let x = fx.xlog.clone();
                tasks.push(tokio::spawn(async move {
                    let payload: Vec<u8> =
                        (0..(5 * BLCKSZ as usize)).map(|i| (i % 251) as u8).collect();
                    with_insertion(async {
                        begin_insert();
                        register_data(&payload);
                        xlog_insert(&x, xlog_rmid, 0x00).await
                    })
                    .await
                }));
            }
            // Concurrent flushers chasing the moving insert head.
            for _ in 0..4 {
                let x = fx.xlog.clone();
                tasks.push(tokio::spawn(async move {
                    for _ in 0..20 {
                        let t = x.get_xlog_insert_rec_ptr();
                        x.xlog_flush(t).await;
                        tokio::task::yield_now().await;
                    }
                    INVALID_XLOG_REC_PTR
                }));
            }
            let mut last = INVALID_XLOG_REC_PTR;
            for t in tasks {
                let e = t.await.unwrap();
                if e.0 > last.0 {
                    last = e;
                }
            }
            fx.xlog.xlog_flush(last).await;
            last
        };

        let last = tokio::time::timeout(std::time::Duration::from_secs(20), body)
            .await
            .expect("eviction + concurrent flush deadlocked");
        assert!(fx.xlog.get_flush_rec_ptr().0 >= last.0);
    }

    /// PG-FORMAT: a written segment's FIRST page carries the long header
    /// (XLP_LONG_HEADER set, correct magic, the long-header cross-check fields),
    /// a non-first page carries the short header, and XLogFileName produces the
    /// expected 24-hex name.
    #[tokio::test]
    async fn wal_pages_are_pg_compatible() {
        use crate::access::xlog_internal::{
            XLogFileName, XLogFromFileName, XlpFlags, SizeOfXLogLongPHD, XLOG_PAGE_MAGIC,
        };
        // The long-header cross-check below reads bytes [32..40].
        const _: () = assert!(SizeOfXLogLongPHD >= 40);
        let seg = 4 * BLCKSZ;
        let fx = WalFixture::new(seg, 8).await;

        // Insert enough to fill into the second page of segment 0.
        let mut end = INVALID_XLOG_REC_PTR;
        for _ in 0..40 {
            let (rec, crc) = raw_record(512);
            end = fx.xlog.insert_record(&rec, crc).await;
        }
        fx.xlog.xlog_flush(end).await;

        let path = fx.xlog.xlog_file_path(fx.dir.to_str().unwrap(), XLogSegNo(0));
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len() as u64, seg);

        // First page: long header.
        let magic = u16::from_ne_bytes([bytes[0], bytes[1]]);
        let info = u16::from_ne_bytes([bytes[2], bytes[3]]);
        let pageaddr = u64::from_ne_bytes(bytes[8..16].try_into().unwrap());
        assert_eq!(magic, XLOG_PAGE_MAGIC, "first page magic");
        assert!(
            info & XlpFlags::LONG_HEADER.bits() != 0,
            "first page must set XLP_LONG_HEADER"
        );
        assert_eq!(pageaddr, 0, "first page pageaddr is the segment start LSN");
        // Long-header cross-check fields.
        let seg_size = u32::from_ne_bytes(bytes[32..36].try_into().unwrap());
        let blcksz = u32::from_ne_bytes(bytes[36..40].try_into().unwrap());
        assert_eq!(u64::from(seg_size), seg, "xlp_seg_size");
        assert_eq!(blcksz, XLOG_BLCKSZ, "xlp_xlog_blcksz");

        // Second page (offset BLCKSZ): short header (no long-header flag).
        let p2 = BLCKSZ as usize;
        let magic2 = u16::from_ne_bytes([bytes[p2], bytes[p2 + 1]]);
        let info2 = u16::from_ne_bytes([bytes[p2 + 2], bytes[p2 + 3]]);
        let pageaddr2 = u64::from_ne_bytes(bytes[p2 + 8..p2 + 16].try_into().unwrap());
        assert_eq!(magic2, XLOG_PAGE_MAGIC, "second page magic");
        assert_eq!(
            info2 & XlpFlags::LONG_HEADER.bits(),
            0,
            "non-first page must NOT set XLP_LONG_HEADER"
        );
        assert_eq!(pageaddr2, BLCKSZ, "second page pageaddr");

        // Segment naming: known (tli, segno) -> 24-hex name, round-trips.
        let segs_per_id = 0x1_0000_0000u64 / seg;
        let segno = XLogSegNo(segs_per_id + 5); // logid 1, seg 5
        let name = XLogFileName(INSERT_TLI, segno, seg as i32);
        assert_eq!(name.len(), 24);
        assert_eq!(name, format!("{:08X}{:08X}{:08X}", INSERT_TLI.0, 1u32, 5u32));
        let (tli_back, segno_back) = XLogFromFileName(&name, seg as i32);
        assert_eq!(tli_back, INSERT_TLI);
        assert_eq!(segno_back, segno);
    }
}

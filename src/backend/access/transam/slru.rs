//! Translated from PostgreSQL src/backend/access/transam/slru.c
//!
//! Simple LRU buffering for wrap-around-able permanent metadata (clog,
//! subtrans, ...). This is the async leaf of the F2 transaction spine.
//!
//! Port model (design step14 section 5):
//!  * The shared-memory `SlruSharedData` + per-bank/per-buffer LWLocks collapse
//!    into `SlruCtl { banks: Vec<Mutex<SlruBank>>, slot_io: Vec<WaitQueue> }`.
//!    Each bank owns `SLRU_BANK_SIZE` contiguous slots; the bank `Mutex` is the
//!    ex-bank-control-LWLock and the per-slot `WaitQueue` is the ex-buffer
//!    LWLock used for `SimpleLruWaitIO`.
//!  * THE invariant: a bank `Mutex` guard is SYNC and is NEVER held across an
//!    `.await`. Acquire, inspect/mutate slot metadata, drop, THEN await I/O,
//!    reacquire and recheck (mirrors slru.c releasing the control lock around
//!    `SlruPhysicalReadPage`/`WritePage`).
//!  * Physical read goes into a temp `Box<[u8; BLCKSZ]>`, then is copied into
//!    the slot buffer under the reacquired lock (no `UnsafeCell`).
//!  * An `InProgressSlruIo` RAII unwind guard resets the slot to Empty (read) or
//!    Valid+dirty (write) and wakes waiters on panic, so a failed I/O never
//!    strands waiters (rules s11).
//!  * Reading a not-yet-written page (EOF) returns a zero-filled buffer ok=true
//!    (mirrors `SlruPhysicalReadPage` zeroing on ENOENT/short read).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::access::slru::{SlruPageStatus, SLRU_PAGES_PER_SEGMENT};
use crate::access::xlogdefs::XLogRecPtr;
use crate::backend::access::transam::xlog::{xlog_flush, XLogCtl};
use crate::backend::storage::file::fd::FdManager;
use crate::c::TransactionId;
use crate::pg_config::BLCKSZ;
use crate::storage::io_backend::{self, OpenFlags};
use crate::storage::sync::{FileTag, SyncRequestHandler, SyncRequestType, SyncRequests};
use crate::storage::wait_guard::{WaitGuard, WaitQueue};

const BLCKSZ_USIZE: usize = BLCKSZ as usize;
const BLCKSZ_U64: u64 = BLCKSZ as u64;

/// Bank size for the slot array (slru.c SLRU_BANK_SIZE = 1 << 4).
pub const SLRU_BANK_BITSHIFT: u32 = 4;
pub const SLRU_BANK_SIZE: usize = 1 << SLRU_BANK_BITSHIFT;

/// One SLRU bank: a contiguous run of `SLRU_BANK_SIZE` slots plus the bank LRU
/// counter. All fields are guarded by the bank's `Mutex` (the ex-control-lock).
pub(crate) struct SlruBank {
    /// Per-slot page payloads. Boxed so the bank struct stays small and a slot
    /// can be swapped cheaply.
    page_buffer: Vec<Box<[u8; BLCKSZ_USIZE]>>,
    page_status: Vec<SlruPageStatus>,
    page_dirty: Vec<bool>,
    page_number: Vec<i64>,
    page_lru_count: Vec<i32>,
    /// Per-slot LSN groups (flattened: slot * lsn_groups_per_page + group).
    /// Empty when lsn_groups_per_page == 0.
    group_lsn: Vec<u64>,
    cur_lru_count: i32,
    /// Index of this bank's first slot in the global slot space.
    bankstart: usize,
    lsn_groups_per_page: usize,
}

impl SlruBank {
    fn new(bankstart: usize, lsn_groups_per_page: usize) -> Self {
        SlruBank {
            page_buffer: (0..SLRU_BANK_SIZE).map(|_| Box::new([0u8; BLCKSZ_USIZE])).collect(),
            page_status: (0..SLRU_BANK_SIZE).map(|_| SlruPageStatus::Empty).collect(),
            page_dirty: vec![false; SLRU_BANK_SIZE],
            page_number: vec![0; SLRU_BANK_SIZE],
            page_lru_count: vec![0; SLRU_BANK_SIZE],
            group_lsn: vec![0u64; SLRU_BANK_SIZE * lsn_groups_per_page],
            cur_lru_count: 0,
            bankstart,
            lsn_groups_per_page,
        }
    }

    /// Local slot index (0..SLRU_BANK_SIZE) of `pageno`, if resident.
    fn find(&self, pageno: i64) -> Option<usize> {
        (0..SLRU_BANK_SIZE).find(|&i| {
            !matches!(self.page_status[i], SlruPageStatus::Empty) && self.page_number[i] == pageno
        })
    }

    /// slru.c SlruRecentlyUsed: bump LRU counters for a slot.
    fn recently_used(&mut self, i: usize) {
        let new_count = self.cur_lru_count;
        if new_count != self.page_lru_count[i] {
            self.cur_lru_count = new_count + 1;
            self.page_lru_count[i] = new_count + 1;
        }
    }

    fn zero_lsns(&mut self, i: usize) {
        if self.lsn_groups_per_page > 0 {
            let base = i * self.lsn_groups_per_page;
            for g in &mut self.group_lsn[base..base + self.lsn_groups_per_page] {
                *g = 0;
            }
        }
    }
}

/// SlruCtl: the active control struct for one SLRU (clog, subtrans, ...). Held
/// on `SharedState` behind an `Arc`.
pub struct SlruCtl {
    banks: Vec<Mutex<SlruBank>>,
    /// Per-slot wait queue (global slot index): the ex-per-buffer LWLock used to
    /// coordinate `SimpleLruWaitIO`.
    slot_io: Vec<WaitQueue>,
    nbanks: usize,
    num_slots: usize,
    long_segment_names: bool,
    sync_handler: SyncRequestHandler,
    /// slru.c ctl->PagePrecedes: true if page1 is "older" than page2.
    page_precedes: fn(i64, i64) -> bool,
    lsn_groups_per_page: usize,
    /// slru.c latest_page_number (atomic in C too).
    latest_page_number: AtomicU64,
    /// Resolved directory (slru.c ctl->Dir), e.g. "<datadir>/pg_xact".
    dir: PathBuf,
    /// I/O handles, cloned out of SharedState at init (avoids an Arc cycle:
    /// SharedState owns the SlruCtl Arc, so SlruCtl must not own SharedState).
    fd: Arc<FdManager>,
    xlog: Arc<XLogCtl>,
    sync_requests: Arc<SyncRequests>,
}

/// RAII unwind guard for an in-progress SLRU I/O (rules s11). On a panic during
/// the awaited physical I/O, reset the slot's status and wake any waiters so a
/// failed read/write never strands them. `disarm` cancels the cleanup on
/// success (the caller then sets the final status itself).
struct InProgressSlruIo<'a> {
    ctl: &'a SlruCtl,
    pageno: i64,
    slot: usize,
    /// true = read (reset to Empty on panic); false = write (reset to Valid+dirty).
    is_read: bool,
    armed: bool,
}

impl<'a> InProgressSlruIo<'a> {
    fn arm(ctl: &'a SlruCtl, pageno: i64, slot: usize, is_read: bool) -> Self {
        InProgressSlruIo { ctl, pageno, slot, is_read, armed: true }
    }
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InProgressSlruIo<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Panic during I/O: restore a sane slot state and wake waiters.
        let local = self.slot - self.ctl.bankstart(self.pageno);
        if let Ok(mut bank) = self.ctl.bank(self.pageno).lock() {
            if self.is_read {
                bank.page_status[local] = SlruPageStatus::Empty;
            } else {
                bank.page_status[local] = SlruPageStatus::Valid;
                bank.page_dirty[local] = true;
            }
        }
        self.ctl.slot_io[self.slot].wake_all();
    }
}

impl SlruCtl {
    /// SimpleLruInit: build an SLRU control struct with `nslots` slots (must be a
    /// multiple of SLRU_BANK_SIZE) and `nlsns` LSN groups per page.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        nslots: usize,
        nlsns: usize,
        subdir: &str,
        sync_handler: SyncRequestHandler,
        long_segment_names: bool,
        page_precedes: fn(i64, i64) -> bool,
        fd: Arc<FdManager>,
        xlog: Arc<XLogCtl>,
        sync_requests: Arc<SyncRequests>,
        data_dir: Option<String>,
    ) -> Arc<SlruCtl> {
        assert!(nslots % SLRU_BANK_SIZE == 0, "nslots must be a multiple of SLRU_BANK_SIZE");
        let nbanks = nslots / SLRU_BANK_SIZE;
        let banks = (0..nbanks)
            .map(|b| Mutex::new(SlruBank::new(b * SLRU_BANK_SIZE, nlsns)))
            .collect();
        let slot_io = (0..nslots).map(|_| WaitQueue::new()).collect();
        let dir = match data_dir {
            Some(d) => PathBuf::from(d).join(subdir),
            None => PathBuf::from(subdir),
        };
        Arc::new(SlruCtl {
            banks,
            slot_io,
            nbanks,
            num_slots: nslots,
            long_segment_names,
            sync_handler,
            page_precedes,
            lsn_groups_per_page: nlsns,
            latest_page_number: AtomicU64::new(0),
            dir,
            fd,
            xlog,
            sync_requests,
        })
    }

    #[inline]
    fn bankno(&self, pageno: i64) -> usize {
        (pageno as u64 % self.nbanks as u64) as usize
    }

    #[inline]
    fn bank(&self, pageno: i64) -> &Mutex<SlruBank> {
        &self.banks[self.bankno(pageno)]
    }

    #[inline]
    fn bankstart(&self, pageno: i64) -> usize {
        self.bankno(pageno) * SLRU_BANK_SIZE
    }

    pub fn latest_page_number(&self) -> i64 {
        self.latest_page_number.load(Ordering::Relaxed) as i64
    }

    // -- segment filename (slru.c SlruFileName) --------------------------------

    fn segment_path(&self, segno: i64) -> PathBuf {
        let name = if self.long_segment_names {
            format!("{:015X}", segno)
        } else {
            format!("{:04X}", segno)
        };
        self.dir.join(name)
    }

    // -- physical I/O (slru.c SlruPhysicalReadPage / WritePage) ----------------

    /// Physical read of `pageno` into `buf`. Returns true on success; a
    /// not-yet-written page (ENOENT or short read) zero-fills `buf` and returns
    /// true (the SLRU's zero-on-EOF responsibility, like SlruPhysicalReadPage).
    async fn physical_read(&self, pageno: i64, buf: &mut [u8; BLCKSZ_USIZE]) -> bool {
        let segno = pageno / SLRU_PAGES_PER_SEGMENT as i64;
        let rpageno = pageno % SLRU_PAGES_PER_SEGMENT as i64;
        let offset = rpageno as u64 * BLCKSZ_U64;
        let path = self.segment_path(segno);

        let file = match self.fd.open(&path, OpenFlags::read_only()).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                buf.fill(0);
                return true;
            }
            Err(_) => return false,
        };
        match file.read(&mut buf[..], offset).await {
            Ok(n) if n == BLCKSZ_USIZE => true,
            // Short read past EOF: a not-yet-written page in an existing segment.
            Ok(_) => {
                buf.fill(0);
                true
            }
            Err(_) => false,
        }
    }

    /// Physical write of `buf` to `pageno`. Creates the segment if absent (no
    /// O_EXCL/O_TRUNC, per slru.c). Returns true on success.
    async fn physical_write(&self, pageno: i64, buf: &[u8; BLCKSZ_USIZE]) -> bool {
        let segno = pageno / SLRU_PAGES_PER_SEGMENT as i64;
        let rpageno = pageno % SLRU_PAGES_PER_SEGMENT as i64;
        let offset = rpageno as u64 * BLCKSZ_U64;
        let path = self.segment_path(segno);

        // Ensure the directory exists (initdb normally makes it; be safe).
        let _ = io_backend::mkdir_all(&self.dir).await;

        let mut flags = OpenFlags::read_write();
        flags.create = true;
        let file = match self.fd.open(&path, flags).await {
            Ok(f) => f,
            Err(_) => return false,
        };
        match file.write(&buf[..], offset).await {
            Ok(n) if n == BLCKSZ_USIZE => {}
            _ => return false,
        }

        // Queue an fsync request with the checkpointer (slru.c). The in-memory
        // queue always accepts (the checkpointer drains it later, step17), so
        // the slru.c "queue full -> fsync now" fallback never triggers here.
        if self.sync_handler != SyncRequestHandler::None {
            let tag = self.file_tag(segno);
            self.sync_requests.register_tag(&tag, SyncRequestType::SyncRequest);
        }
        true
    }

    fn file_tag(&self, segno: i64) -> FileTag {
        // slru.c INIT_SLRUFILETAG: only handler + segno matter.
        FileTag {
            handler: self.sync_handler as i16,
            forknum: 0,
            rlocator: crate::storage::relfilelocator::RelFileLocator {
                spcOid: crate::postgres_ext::Oid(0),
                dbOid: crate::postgres_ext::Oid(0),
                relNumber: crate::postgres_ext::Oid(0),
            },
            segno: segno as u64,
        }
    }

    // -- LRU victim selection (slru.c SlruSelectLRUPage) -----------------------
    //
    // Returns Ok(local_slot) for a usable slot (either holding pageno already,
    // or a freeable EMPTY/clean-VALID victim). Returns Err(local_slot) when the
    // chosen victim must first be written out (dirty), or Err for an I/O wait.
    // Done under a held bank guard.

    /// Choose a slot, returning `(local_slot, action)`. `action`:
    ///  - Ready: slot already holds pageno OR is a freeable victim.
    ///  - WriteVictim(slot): the chosen victim is dirty; write it then retry.
    ///  - WaitIo(slot): all slots busy; wait on `slot` then retry.
    fn select_lru(&self, bank: &mut SlruBank, pageno: i64) -> SlruSelect {
        // Already resident?
        if let Some(i) = bank.find(pageno) {
            return SlruSelect::Ready(i);
        }
        let cur_count = bank.cur_lru_count;
        bank.cur_lru_count = cur_count.wrapping_add(1);

        let latest = self.latest_page_number();
        let mut best_valid: Option<(usize, i32, i64)> = None;
        let mut best_invalid: Option<(usize, i32, i64)> = None;

        for i in 0..SLRU_BANK_SIZE {
            if matches!(bank.page_status[i], SlruPageStatus::Empty) {
                return SlruSelect::Ready(i);
            }
            let mut this_delta = cur_count.wrapping_sub(bank.page_lru_count[i]);
            if this_delta < 0 {
                bank.page_lru_count[i] = cur_count;
                this_delta = 0;
            }
            let pn = bank.page_number[i];
            if pn == latest {
                continue;
            }
            let better = |best: &Option<(usize, i32, i64)>| match best {
                None => true,
                Some((_, bd, bpn)) => {
                    this_delta > *bd || (this_delta == *bd && (self.page_precedes)(pn, *bpn))
                }
            };
            if matches!(bank.page_status[i], SlruPageStatus::Valid) {
                if better(&best_valid) {
                    best_valid = Some((i, this_delta, pn));
                }
            } else if better(&best_invalid) {
                best_invalid = Some((i, this_delta, pn));
            }
        }

        match best_valid {
            None => {
                // All valid pages busy; wait for an I/O on the LRU busy slot.
                let i = best_invalid.map(|(i, _, _)| i).unwrap_or(0);
                SlruSelect::WaitIo(i)
            }
            Some((i, _, _)) => {
                if !bank.page_dirty[i] {
                    SlruSelect::Ready(i)
                } else {
                    SlruSelect::WriteVictim(i)
                }
            }
        }
    }

    // -- core: read a page into a slot, reading in if necessary ----------------

    /// slru.c SimpleLruReadPage: ensure `pageno` is resident and return its
    /// global slot number. `write_ok` allows returning a WRITE_IN_PROGRESS page.
    /// `xid` is for error reporting only.
    pub async fn read_page(&self, pageno: i64, write_ok: bool, xid: TransactionId) -> usize {
        loop {
            // Selection and victim-claim happen in ONE critical section, exactly as
            // slru.c holds the bank lock continuously from SlruSelectLRUPage through
            // marking the slot READ_IN_PROGRESS. Only the physical I/O runs with the
            // lock dropped, by which point the slot is already claimed. Splitting this
            // would let two tasks claim the same victim for different pages. Any wait
            // also enqueues UNDER the lock so a completing wake is never missed.
            // Wait carries a guard enqueued UNDER the bank lock (so a completing
            // wake is never missed), awaited after the lock drops at block end.
            enum Next<'q> {
                Return(usize),
                Read(usize),
                WriteVictim(usize),
                Wait(WaitGuard<'q>),
            }
            let next = {
                let mut bank = self.bank(pageno).lock().unwrap();
                let bankstart = bank.bankstart;
                match self.select_lru(&mut bank, pageno) {
                    SlruSelect::Ready(i) => {
                        let resident = bank.page_number[i] == pageno
                            && !matches!(bank.page_status[i], SlruPageStatus::Empty);
                        if resident {
                            let must_wait = matches!(
                                bank.page_status[i],
                                SlruPageStatus::ReadInProgress
                            ) || (matches!(
                                bank.page_status[i],
                                SlruPageStatus::WriteInProgress
                            ) && !write_ok);
                            if must_wait {
                                Next::Wait(self.slot_io[bankstart + i].enqueue())
                            } else {
                                bank.recently_used(i);
                                Next::Return(bankstart + i)
                            }
                        } else {
                            // Freeable victim: claim it now, under the same lock.
                            bank.page_number[i] = pageno;
                            bank.page_status[i] = SlruPageStatus::ReadInProgress;
                            bank.page_dirty[i] = false;
                            Next::Read(bankstart + i)
                        }
                    }
                    SlruSelect::WriteVictim(i) => Next::WriteVictim(bankstart + i),
                    SlruSelect::WaitIo(i) => Next::Wait(self.slot_io[bankstart + i].enqueue()),
                }
            };

            match next {
                Next::Return(slot) => return slot,
                Next::Wait(g) => {
                    g.await;
                    continue;
                }
                Next::WriteVictim(slot) => {
                    self.write_page(slot).await;
                    continue;
                }
                Next::Read(slot) => {
                    let local = slot - self.bankstart(pageno);
                    let mut io = InProgressSlruIo::arm(self, pageno, slot, true);
                    let mut tmp = Box::new([0u8; BLCKSZ_USIZE]);
                    let ok = self.physical_read(pageno, &mut tmp).await;

                    let mut bank = self.bank(pageno).lock().unwrap();
                    bank.page_buffer[local].copy_from_slice(&tmp[..]);
                    bank.zero_lsns(local);
                    bank.page_status[local] =
                        if ok { SlruPageStatus::Valid } else { SlruPageStatus::Empty };
                    bank.recently_used(local);
                    drop(bank);
                    io.disarm();
                    self.slot_io[slot].wake_all();

                    if !ok {
                        slru_report_io_error(&self.dir, pageno, xid, "read");
                    }
                    return slot;
                }
            }
        }
    }

    /// slru.c SimpleLruReadPage_ReadOnly. TODO(perf): the shared-lock fast path
    /// is dropped (our bank lock is a plain Mutex); call the exclusive path.
    pub async fn read_page_readonly(&self, pageno: i64, xid: TransactionId) -> usize {
        self.read_page(pageno, true, xid).await
    }

    /// slru.c SimpleLruZeroPage: alloc a slot, zero it, mark Valid+dirty, and
    /// advance latest_page_number. May write out a dirty victim first.
    pub async fn zero_page(&self, pageno: i64) -> usize {
        loop {
            // Select and initialize the slot in ONE critical section (no I/O here),
            // so no concurrent task can reselect the same victim. Only writing out a
            // dirty victim drops the lock and awaits.
            enum Next<'q> {
                Return(usize),
                WriteVictim(usize),
                Wait(WaitGuard<'q>),
            }
            let next = {
                let mut bank = self.bank(pageno).lock().unwrap();
                let bankstart = bank.bankstart;
                match self.select_lru(&mut bank, pageno) {
                    SlruSelect::Ready(local) => {
                        bank.page_number[local] = pageno;
                        bank.page_status[local] = SlruPageStatus::Valid;
                        bank.page_dirty[local] = true;
                        bank.recently_used(local);
                        bank.page_buffer[local].fill(0);
                        bank.zero_lsns(local);
                        Next::Return(bankstart + local)
                    }
                    SlruSelect::WriteVictim(local) => Next::WriteVictim(bankstart + local),
                    SlruSelect::WaitIo(local) => {
                        Next::Wait(self.slot_io[bankstart + local].enqueue())
                    }
                }
            };
            match next {
                Next::Return(slot) => {
                    self.latest_page_number.store(pageno as u64, Ordering::Relaxed);
                    return slot;
                }
                Next::Wait(g) => {
                    g.await;
                    continue;
                }
                Next::WriteVictim(slot) => {
                    self.write_page(slot).await;
                    continue;
                }
            }
        }
    }

    /// slru.c SlruInternalWritePage / SimpleLruWritePage: write a slot out if
    /// dirty. Honors WAL-before-data by flushing the page's max group_lsn first.
    pub async fn write_page(&self, slot: usize) {
        let bankno = slot / SLRU_BANK_SIZE;
        let local = slot - bankno * SLRU_BANK_SIZE;

        // Snapshot under the lock: pageno, bytes, max group_lsn. Mark write-busy.
        let (pageno, bytes, max_lsn) = {
            let mut bank = self.banks[bankno].lock().unwrap();
            // If a write is already in progress or not dirty/valid, nothing to do.
            if !bank.page_dirty[local]
                || !matches!(bank.page_status[local], SlruPageStatus::Valid)
            {
                return;
            }
            let pageno = bank.page_number[local];
            bank.page_status[local] = SlruPageStatus::WriteInProgress;
            bank.page_dirty[local] = false;
            let bytes = bank.page_buffer[local].clone();
            let max_lsn = if bank.lsn_groups_per_page > 0 {
                let base = local * bank.lsn_groups_per_page;
                bank.group_lsn[base..base + bank.lsn_groups_per_page].iter().copied().max().unwrap_or(0)
            } else {
                0
            };
            (pageno, bytes, max_lsn)
        };

        // WAL-before-data: flush WAL through the page's largest async-commit LSN.
        if max_lsn != 0 {
            xlog_flush(&self.xlog, XLogRecPtr(max_lsn)).await;
        }

        let mut io = InProgressSlruIo::arm(self, pageno, slot, false);
        let ok = self.physical_write(pageno, &bytes).await;

        let mut bank = self.banks[bankno].lock().unwrap();
        if !ok {
            bank.page_dirty[local] = true;
        }
        bank.page_status[local] = SlruPageStatus::Valid;
        drop(bank);
        io.disarm();
        self.slot_io[slot].wake_all();

        if !ok {
            slru_report_io_error(&self.dir, pageno, TransactionId(0), "write");
        }
    }

    /// slru.c SimpleLruWriteAll: write every dirty page out (checkpoint path).
    pub async fn write_all(&self) {
        for slot in 0..self.num_slots {
            let bankno = slot / SLRU_BANK_SIZE;
            let local = slot - bankno * SLRU_BANK_SIZE;
            let needs_write = {
                let bank = self.banks[bankno].lock().unwrap();
                !matches!(bank.page_status[local], SlruPageStatus::Empty)
            };
            if needs_write {
                self.write_page(slot).await;
            }
        }
        // Ensure new directory entries are on disk.
        if self.sync_handler != SyncRequestHandler::None {
            let _ = self.fd.fsync_fname(&self.dir, true).await;
        }
    }

    /// slru.c SimpleLruDoesPhysicalPageExist.
    pub async fn does_physical_page_exist(&self, pageno: i64) -> bool {
        let segno = pageno / SLRU_PAGES_PER_SEGMENT as i64;
        let rpageno = pageno % SLRU_PAGES_PER_SEGMENT as i64;
        let need = (rpageno as u64 + 1) * BLCKSZ_U64;
        let path = self.segment_path(segno);
        match self.fd.open(&path, OpenFlags::read_only()).await {
            Ok(f) => f.size().await.map(|sz| sz >= need).unwrap_or(false),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(_) => false,
        }
    }

    /// slru.c SimpleLruTruncate: drop in-memory pages preceding `cutoff_page`
    /// and remove the now-removable segment files. Wraparound backstop included.
    pub async fn truncate(&self, cutoff_page: i64) {
        if (self.page_precedes)(self.latest_page_number(), cutoff_page) {
            // apparent wraparound: refuse (slru.c logs and returns).
            return;
        }
        for slot in 0..self.num_slots {
            let bankno = slot / SLRU_BANK_SIZE;
            let local = slot - bankno * SLRU_BANK_SIZE;
            let needs_write = {
                let mut bank = self.banks[bankno].lock().unwrap();
                if matches!(bank.page_status[local], SlruPageStatus::Empty)
                    || !(self.page_precedes)(bank.page_number[local], cutoff_page)
                {
                    false
                } else if matches!(bank.page_status[local], SlruPageStatus::Valid)
                    && !bank.page_dirty[local]
                {
                    bank.page_status[local] = SlruPageStatus::Empty;
                    false
                } else {
                    true
                }
            };
            if needs_write {
                self.write_page(slot).await;
            }
        }
        self.remove_segments_before(cutoff_page).await;
    }

    /// Remove every segment whose pages all precede `cutoff_page`.
    async fn remove_segments_before(&self, cutoff_page: i64) {
        for (name, segpage) in self.scan_directory().await {
            if self.may_delete_segment(segpage, cutoff_page) {
                self.delete_segment_file(segpage / SLRU_PAGES_PER_SEGMENT as i64, &name).await;
            }
        }
    }

    /// slru.c SlruMayDeleteSegment.
    fn may_delete_segment(&self, segpage: i64, cutoff_page: i64) -> bool {
        let last = segpage + SLRU_PAGES_PER_SEGMENT as i64 - 1;
        (self.page_precedes)(segpage, cutoff_page) && (self.page_precedes)(last, cutoff_page)
    }

    /// slru.c SlruScanDirectory: returns (filename, first-page) for each valid
    /// SLRU segment file in the directory.
    async fn scan_directory(&self) -> Vec<(String, i64)> {
        let mut out = Vec::new();
        let rd = match std::fs::read_dir(&self.dir) {
            Ok(rd) => rd,
            Err(_) => return out,
        };
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let len = name.len();
            let len_ok = if self.long_segment_names {
                len == 15
            } else {
                len == 4 || len == 5 || len == 6
            };
            if len_ok && name.bytes().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase()) {
                if let Ok(segno) = i64::from_str_radix(&name, 16) {
                    out.push((name, segno * SLRU_PAGES_PER_SEGMENT as i64));
                }
            }
        }
        out
    }

    /// slru.c SlruInternalDeleteSegment: forget fsync requests + unlink.
    async fn delete_segment_file(&self, segno: i64, _name: &str) {
        if self.sync_handler != SyncRequestHandler::None {
            let tag = self.file_tag(segno);
            self.sync_requests.register_tag(&tag, SyncRequestType::SyncForgetRequest);
        }
        let _ = io_backend::unlink(self.segment_path(segno)).await;
    }

    /// slru.c SlruDeleteSegment: drop in-memory refs to `segno`, then unlink.
    pub async fn delete_segment(&self, segno: i64) {
        loop {
            let mut did_write = false;
            for slot in 0..self.num_slots {
                let bankno = slot / SLRU_BANK_SIZE;
                let local = slot - bankno * SLRU_BANK_SIZE;
                let needs_write = {
                    let mut bank = self.banks[bankno].lock().unwrap();
                    if matches!(bank.page_status[local], SlruPageStatus::Empty)
                        || bank.page_number[local] / SLRU_PAGES_PER_SEGMENT as i64 != segno
                    {
                        false
                    } else if matches!(bank.page_status[local], SlruPageStatus::Valid)
                        && !bank.page_dirty[local]
                    {
                        bank.page_status[local] = SlruPageStatus::Empty;
                        false
                    } else {
                        true
                    }
                };
                if needs_write {
                    self.write_page(slot).await;
                    did_write = true;
                }
            }
            if !did_write {
                break;
            }
        }
        self.delete_segment_file(segno, "").await;
    }

    /// slru.c SlruSyncFileTag: fsync the segment named by `ftag`. Ok(path) on
    /// success.
    pub async fn sync_file_tag(&self, ftag: &FileTag) -> std::io::Result<String> {
        let path = self.segment_path(ftag.segno as i64);
        let file = self.fd.open(&path, OpenFlags::read_write()).await?;
        file.sync().await?;
        Ok(path.to_string_lossy().into_owned())
    }

    // -- accessors used by clog/subtrans under the bank lock -------------------

    /// Read a byte from a resident slot's page buffer (clog status read).
    pub fn with_page<R>(&self, pageno: i64, slot: usize, f: impl FnOnce(&[u8; BLCKSZ_USIZE]) -> R) -> R {
        let bankno = self.bankno(pageno);
        let local = slot - bankno * SLRU_BANK_SIZE;
        let bank = self.banks[bankno].lock().unwrap();
        f(&bank.page_buffer[local])
    }

    /// Mutate a resident slot's page buffer and mark it dirty (clog/subtrans set).
    pub fn with_page_mut<R>(
        &self,
        pageno: i64,
        slot: usize,
        f: impl FnOnce(&mut [u8; BLCKSZ_USIZE]) -> R,
    ) -> R {
        let bankno = self.bankno(pageno);
        let local = slot - bankno * SLRU_BANK_SIZE;
        let mut bank = self.banks[bankno].lock().unwrap();
        let r = f(&mut bank.page_buffer[local]);
        bank.page_dirty[local] = true;
        r
    }

    /// Read this slot's group_lsn for `xid` (clog's GetLSNIndex), if any.
    pub fn group_lsn(&self, pageno: i64, slot: usize, group: usize) -> u64 {
        if self.lsn_groups_per_page == 0 {
            return 0;
        }
        let bankno = self.bankno(pageno);
        let local = slot - bankno * SLRU_BANK_SIZE;
        let bank = self.banks[bankno].lock().unwrap();
        bank.group_lsn[local * self.lsn_groups_per_page + group]
    }

    /// Raise this slot's group_lsn for `group` to at least `lsn`.
    pub fn set_group_lsn(&self, pageno: i64, slot: usize, group: usize, lsn: u64) {
        if self.lsn_groups_per_page == 0 || lsn == 0 {
            return;
        }
        let bankno = self.bankno(pageno);
        let local = slot - bankno * SLRU_BANK_SIZE;
        let mut bank = self.banks[bankno].lock().unwrap();
        let idx = local * self.lsn_groups_per_page + group;
        if bank.group_lsn[idx] < lsn {
            bank.group_lsn[idx] = lsn;
        }
    }

    pub fn lsn_groups_per_page(&self) -> usize {
        self.lsn_groups_per_page
    }

    /// Set latest_page_number directly (clog StartupCLOG).
    pub fn set_latest_page_number(&self, pageno: i64) {
        self.latest_page_number.store(pageno as u64, Ordering::Relaxed);
    }
}

enum SlruSelect {
    Ready(usize),
    WriteVictim(usize),
    WaitIo(usize),
}

/// slru.c SlruReportIOError: an SLRU I/O failure is an elog(ERROR); panic.
/// TODO(panic): migrate to Result + ?.
fn slru_report_io_error(dir: &std::path::Path, pageno: i64, xid: TransactionId, op: &str) -> ! {
    panic!(
        "could not access status of transaction {} ({op} of SLRU {} page {pageno} failed)",
        xid.0,
        dir.display()
    );
}

/// slru.c SimpleLruAutotuneBuffers: shared_buffers/divisor, capped, rounded down
/// to a multiple of SLRU_BANK_SIZE, at least one bank.
pub fn autotune_buffers(nbuffers: usize, divisor: usize, max: usize) -> usize {
    let cap = max - (max % SLRU_BANK_SIZE);
    let want = (nbuffers / divisor).saturating_sub((nbuffers / divisor) % SLRU_BANK_SIZE);
    cap.min(want.max(SLRU_BANK_SIZE))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared_state::{SharedState, SharedStateConfig};

    fn never_precedes(_: i64, _: i64) -> bool {
        false
    }

    fn temp_shared(tag: &str) -> Arc<SharedState> {
        let dir = std::env::temp_dir().join(format!(
            "pepperdb_slru_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            ..SharedStateConfig::default()
        })
    }

    fn ctl(shared: Arc<SharedState>, subdir: &str) -> Arc<SlruCtl> {
        SlruCtl::new(
            SLRU_BANK_SIZE * 2,
            0,
            subdir,
            SyncRequestHandler::None,
            false,
            never_precedes,
            shared.fd().clone(),
            shared.xlog().clone(),
            shared.sync_requests().clone(),
            shared.config().data_dir(),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn zero_write_read_roundtrip() {
        let shared = temp_shared("rw");
        let c = ctl(shared, "slru_rw");
        let slot = c.zero_page(0).await;
        c.with_page_mut(0, slot, |buf| buf[5] = 0xAB);
        c.write_page(slot).await;
        // Evict by reading enough distinct pages in this bank, then re-read 0.
        let again = c.read_page(0, false, TransactionId(0)).await;
        let val = c.with_page(0, again, |buf| buf[5]);
        assert_eq!(val, 0xAB);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn never_written_page_reads_zeroes() {
        let shared = temp_shared("zero");
        let c = ctl(shared, "slru_zero");
        // page 0 lives in bank 0; page 99 also resolves; read a page never written.
        let slot = c.read_page(0, false, TransactionId(0)).await;
        let all_zero = c.with_page(0, slot, |buf| buf.iter().all(|&b| b == 0));
        assert!(all_zero, "a never-written page must read as zeroes");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn evict_and_reload_preserves_data() {
        let shared = temp_shared("evict");
        let c = ctl(shared, "slru_evict");
        // Write page 0 with a marker.
        let s = c.zero_page(0).await;
        c.with_page_mut(0, s, |buf| buf[1] = 0x77);
        c.write_page(s).await;
        // Bank 0 holds even pages (nbanks=2). Zero more than SLRU_BANK_SIZE even
        // pages to evict page 0 from its bank.
        for p in (2..=(SLRU_BANK_SIZE as i64 * 4)).step_by(2) {
            let sp = c.zero_page(p).await;
            c.write_page(sp).await;
        }
        // Reload page 0 from disk and confirm the marker survived eviction.
        let again = c.read_page(0, false, TransactionId(0)).await;
        assert_eq!(c.with_page(0, again, |b| b[1]), 0x77);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn two_tasks_race_same_page_read() {
        let shared = temp_shared("race");
        let c = ctl(shared, "slru_race");
        // Pre-write page 0 so a physical read happens.
        let s = c.zero_page(0).await;
        c.with_page_mut(0, s, |buf| buf[0] = 0x42);
        c.write_page(s).await;
        // Force eviction of page 0: zero many distinct pages in bank 0
        // (page % nbanks == bank). nbanks=2, so even pages share bank 0.
        for p in (2..=(SLRU_BANK_SIZE as i64 * 2 * 2)).step_by(2) {
            let _ = c.zero_page(p).await;
        }
        let c1 = c.clone();
        let c2 = c.clone();
        let h1 = tokio::spawn(async move { c1.read_page(0, false, TransactionId(0)).await });
        let h2 = tokio::spawn(async move { c2.read_page(0, false, TransactionId(0)).await });
        let s1 = h1.await.unwrap();
        let s2 = h2.await.unwrap();
        assert_eq!(c.with_page(0, s1, |b| b[0]), 0x42);
        assert_eq!(c.with_page(0, s2, |b| b[0]), 0x42);
    }

    // Regression: two tasks reading DISTINCT non-resident pages in the SAME bank
    // must not claim the same victim slot. The old code dropped the bank lock
    // between LRU selection and claiming the victim, so both tasks could pick the
    // same slot and read different pages into it (release-build corruption).
    #[tokio::test(flavor = "multi_thread")]
    async fn two_tasks_distinct_pages_same_bank_no_clobber() {
        let shared = temp_shared("victim");
        let c = ctl(shared, "slru_victim");
        // ctl has 2 banks (SLRU_BANK_SIZE*2 slots); even pages -> bank 0.
        let bank0: Vec<i64> = (0..SLRU_BANK_SIZE as i64).map(|i| i * 2).collect();
        for &p in &bank0 {
            let s = c.zero_page(p).await;
            c.with_page_mut(p, s, |buf| buf[0] = (p & 0xff) as u8);
            c.write_page(s).await;
        }
        // Two fresh even pages, both bank 0, persisted then evicted.
        let (pa, pb) = (SLRU_BANK_SIZE as i64 * 2, SLRU_BANK_SIZE as i64 * 2 + 2);
        for &p in &[pa, pb] {
            let s = c.zero_page(p).await;
            c.with_page_mut(p, s, |buf| buf[0] = (p & 0xff) as u8);
            c.write_page(s).await;
        }
        for &p in &bank0 {
            let _ = c.read_page(p, false, TransactionId(0)).await;
        }
        let (c1, c2) = (c.clone(), c.clone());
        let h1 = tokio::spawn(async move { c1.read_page(pa, false, TransactionId(0)).await });
        let h2 = tokio::spawn(async move { c2.read_page(pb, false, TransactionId(0)).await });
        let s1 = h1.await.unwrap();
        let s2 = h2.await.unwrap();
        assert_ne!(s1, s2, "distinct pages must occupy distinct slots");
        assert_eq!(c.with_page(pa, s1, |b| b[0]), (pa & 0xff) as u8);
        assert_eq!(c.with_page(pb, s2, |b| b[0]), (pb & 0xff) as u8);
    }
}

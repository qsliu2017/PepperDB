//! Translated from PostgreSQL src/backend/access/transam/slru.c
//!
//! Simple LRU buffering for wrap-around-able permanent metadata (clog,
//! subtrans, ...). This is the async leaf of the F2 transaction spine.
//!
//! Port model (design step14 section 5, refactor R-C):
//!  * The shared-memory `SlruSharedData` + per-bank/per-buffer LWLocks collapse
//!    into `SlruCtl { banks: Vec<RwLock<SlruBank>>, slot_io: Vec<WaitQueue> }`.
//!    Each bank owns `SLRU_BANK_SIZE` contiguous slots; the bank `RwLock` is the
//!    ex-bank-control-LWLock and the per-slot `WaitQueue` is the ex-buffer
//!    LWLock used for `SimpleLruWaitIO`.
//!  * PG's bank control lock is EXCLUSIVE everywhere except the read-only hit
//!    path of `SimpleLruReadPage_ReadOnly` (LW_SHARED). So the read-in / claim /
//!    set-bits paths take the WRITE lock; the status-lookup hit path takes the
//!    READ lock. Under the shared lock ONLY the LRU hint atomics may be mutated
//!    (`SlruRecentlyUsed`); page_status/page_number/page_buffer/page_dirty are
//!    written ONLY under the write lock. The RwLock guarantees no writer runs
//!    concurrently with readers, so reading those non-atomic fields under the
//!    read lock is sound, and we never form `&mut` to a page under it.
//!  * THE invariant: a bank guard is SYNC and is NEVER held across an `.await`.
//!    Acquire, inspect/mutate slot metadata, drop, THEN await I/O, reacquire and
//!    recheck (mirrors slru.c releasing the control lock around
//!    `SlruPhysicalReadPage`/`WritePage`). The closure `f` in the `*_with`
//!    accessors is sync and DOES run under the held lock -- that mirrors PG
//!    holding the bank lock across the buffer access; only physical I/O awaits
//!    with the lock dropped.
//!  * Physical read goes into a temp `Box<[u8; BLCKSZ]>`, then is copied into
//!    the slot buffer under the reacquired lock (no `UnsafeCell`).
//!  * An `InProgressSlruIo` RAII unwind guard resets the slot to Empty (read) or
//!    Valid+dirty (write) and wakes waiters on panic, so a failed I/O never
//!    strands waiters (rules s11).
//!  * Reading a not-yet-written page (EOF) returns a zero-filled buffer ok=true
//!    (mirrors `SlruPhysicalReadPage` zeroing on ENOENT/short read).

use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::access::slru::{SLRU_PAGES_PER_SEGMENT, SlruPageStatus};
use crate::access::xlogdefs::XLogRecPtr;
use crate::backend::access::transam::xlog::{XLogCtl, xlog_flush};
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
/// counter. Non-atomic fields are written only under the bank `RwLock` WRITE
/// lock; the LRU-hint atomics (`page_lru_count`, `cur_lru_count`) may also be
/// updated under the READ lock by `recently_used` (slru.c SlruRecentlyUsed).
pub(crate) struct SlruBank {
    /// Per-slot page payloads. Boxed so the bank struct stays small and a slot
    /// can be swapped cheaply.
    page_buffer: Vec<Box<[u8; BLCKSZ_USIZE]>>,
    page_status: Vec<SlruPageStatus>,
    page_dirty: Vec<bool>,
    page_number: Vec<i64>,
    /// LRU hint: atomic so `recently_used` is sound under the shared read lock.
    page_lru_count: Vec<AtomicI32>,
    /// Per-slot LSN groups (flattened: slot * lsn_groups_per_page + group).
    /// Empty when lsn_groups_per_page == 0.
    group_lsn: Vec<u64>,
    /// Bank LRU hint counter (atomic; see page_lru_count).
    cur_lru_count: AtomicI32,
    /// Index of this bank's first slot in the global slot space.
    bankstart: usize,
    lsn_groups_per_page: usize,
}

impl SlruBank {
    fn new(bankstart: usize, lsn_groups_per_page: usize) -> Self {
        Self {
            page_buffer: (0..SLRU_BANK_SIZE)
                .map(|_| Box::new([0u8; BLCKSZ_USIZE]))
                .collect(),
            page_status: (0..SLRU_BANK_SIZE).map(|_| SlruPageStatus::Empty).collect(),
            page_dirty: vec![false; SLRU_BANK_SIZE],
            page_number: vec![0; SLRU_BANK_SIZE],
            page_lru_count: (0..SLRU_BANK_SIZE).map(|_| AtomicI32::new(0)).collect(),
            group_lsn: vec![0u64; SLRU_BANK_SIZE * lsn_groups_per_page],
            cur_lru_count: AtomicI32::new(0),
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

    /// slru.c SlruRecentlyUsed: bump LRU counters for a slot. Operates only on
    /// the atomics (Relaxed -- it is a hint), so it is sound under a shared lock
    /// even when several readers run it concurrently (slru.c allows this).
    fn recently_used(&self, i: usize) {
        let new_count = self.cur_lru_count.load(Ordering::Relaxed);
        if new_count != self.page_lru_count[i].load(Ordering::Relaxed) {
            self.cur_lru_count.store(new_count + 1, Ordering::Relaxed);
            self.page_lru_count[i].store(new_count + 1, Ordering::Relaxed);
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

/// Mutable handle to one resident page, valid only inside a `read_page_with`
/// closure (the bank WRITE lock is held). Gives the page buffer plus this slot's
/// LSN groups, so a caller can set status bits AND the group LSN in one locked
/// section. Reading or writing the buffer marks the slot dirty.
pub struct SlruPageMut<'a> {
    bank: &'a mut SlruBank,
    local: usize,
    lsn_groups: usize,
}

impl<'a> SlruPageMut<'a> {
    fn new(bank: &'a mut SlruBank, local: usize, lsn_groups: usize) -> Self {
        SlruPageMut {
            bank,
            local,
            lsn_groups,
        }
    }

    /// The page bytes (mutable). Marks the slot dirty (clog/subtrans set path).
    pub fn buf_mut(&mut self) -> &mut [u8; BLCKSZ_USIZE] {
        self.bank.page_dirty[self.local] = true;
        &mut self.bank.page_buffer[self.local]
    }

    /// The page bytes (read-only); does not dirty the slot.
    pub fn buf(&self) -> &[u8; BLCKSZ_USIZE] {
        &self.bank.page_buffer[self.local]
    }

    /// Raise this slot's group_lsn for `group` to at least `lsn` (async commit).
    pub fn set_group_lsn(&mut self, group: usize, lsn: u64) {
        if self.lsn_groups == 0 || lsn == 0 {
            return;
        }
        let idx = self.local * self.lsn_groups + group;
        if self.bank.group_lsn[idx] < lsn {
            self.bank.group_lsn[idx] = lsn;
        }
    }

    /// Demote to a shared view (used when delegating a readonly miss).
    fn as_ref(&self) -> SlruPageRef<'_> {
        SlruPageRef {
            bank: self.bank,
            local: self.local,
            lsn_groups: self.lsn_groups,
        }
    }
}

/// Shared handle to one resident page, valid only inside a
/// `read_page_readonly_with` closure (the bank READ lock is held). Gives only `&`
/// access -- never `&mut` under the shared lock (F1 invariant).
pub struct SlruPageRef<'a> {
    bank: &'a SlruBank,
    local: usize,
    lsn_groups: usize,
}

impl<'a> SlruPageRef<'a> {
    fn new(bank: &'a SlruBank, local: usize, lsn_groups: usize) -> Self {
        SlruPageRef {
            bank,
            local,
            lsn_groups,
        }
    }

    pub fn buf(&self) -> &[u8; BLCKSZ_USIZE] {
        &self.bank.page_buffer[self.local]
    }

    pub fn group_lsn(&self, group: usize) -> u64 {
        if self.lsn_groups == 0 {
            return 0;
        }
        self.bank.group_lsn[self.local * self.lsn_groups + group]
    }
}

/// SlruCtl: the active control struct for one SLRU (clog, subtrans, ...). Held
/// on `SharedState` behind an `Arc`.
pub struct SlruCtl {
    banks: Vec<RwLock<SlruBank>>,
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
        InProgressSlruIo {
            ctl,
            pageno,
            slot,
            is_read,
            armed: true,
        }
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
        if let Ok(mut bank) = self.ctl.bank(self.pageno).write() {
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
    ) -> Arc<Self> {
        assert!(
            nslots.is_multiple_of(SLRU_BANK_SIZE),
            "nslots must be a multiple of SLRU_BANK_SIZE"
        );
        let nbanks = nslots / SLRU_BANK_SIZE;
        let banks = (0..nbanks)
            .map(|b| RwLock::new(SlruBank::new(b * SLRU_BANK_SIZE, nlsns)))
            .collect();
        let slot_io = (0..nslots).map(|_| WaitQueue::new()).collect();
        let dir = data_dir.map_or_else(|| PathBuf::from(subdir), |d| PathBuf::from(d).join(subdir));
        Arc::new(Self {
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
    fn bank(&self, pageno: i64) -> &RwLock<SlruBank> {
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
            format!("{segno:015X}")
        } else {
            format!("{segno:04X}")
        };
        self.dir.join(name)
    }

    // -- physical I/O (slru.c SlruPhysicalReadPage / WritePage) ----------------

    /// Physical read of `pageno` into `buf`. Returns true on success; a wholly
    /// absent segment (ENOENT) zero-fills `buf` and returns true (the SLRU's
    /// zero-on-EOF responsibility, like SlruPhysicalReadPage). The short-read
    /// fallback below is currently unreachable (read_exact_at returns
    /// UnexpectedEof, not a short Ok) -- it matters only for recovery reading a
    /// partially-written segment; TODO(recovery).
    async fn physical_read(&self, pageno: i64, buf: &mut [u8; BLCKSZ_USIZE]) -> bool {
        let segno = pageno / i64::from(SLRU_PAGES_PER_SEGMENT);
        let rpageno = pageno % i64::from(SLRU_PAGES_PER_SEGMENT);
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
        let segno = pageno / i64::from(SLRU_PAGES_PER_SEGMENT);
        let rpageno = pageno % i64::from(SLRU_PAGES_PER_SEGMENT);
        let offset = rpageno as u64 * BLCKSZ_U64;
        let path = self.segment_path(segno);

        // Ensure the directory exists (initdb normally makes it; be safe).
        let _ = io_backend::mkdir_all(&self.dir).await;

        let mut flags = OpenFlags::read_write();
        flags.create = true;
        let Ok(file) = self.fd.open(&path, flags).await else {
            return false;
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
            self.sync_requests
                .register_tag(&tag, SyncRequestType::SyncRequest);
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
    fn select_lru(&self, bank: &SlruBank, pageno: i64) -> SlruSelect {
        // Already resident?
        if let Some(i) = bank.find(pageno) {
            return SlruSelect::Ready(i);
        }
        let cur_count = bank.cur_lru_count.load(Ordering::Relaxed);
        bank.cur_lru_count
            .store(cur_count.wrapping_add(1), Ordering::Relaxed);

        let latest = self.latest_page_number();
        let mut best_valid: Option<(usize, i32, i64)> = None;
        let mut best_invalid: Option<(usize, i32, i64)> = None;

        for i in 0..SLRU_BANK_SIZE {
            if matches!(bank.page_status[i], SlruPageStatus::Empty) {
                return SlruSelect::Ready(i);
            }
            let mut this_delta =
                cur_count.wrapping_sub(bank.page_lru_count[i].load(Ordering::Relaxed));
            if this_delta < 0 {
                bank.page_lru_count[i].store(cur_count, Ordering::Relaxed);
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
                let i = best_invalid.map_or(0, |(i, _, _)| i);
                SlruSelect::WaitIo(i)
            }
            Some((i, _, _)) => {
                if bank.page_dirty[i] {
                    SlruSelect::WriteVictim(i)
                } else {
                    SlruSelect::Ready(i)
                }
            }
        }
    }

    // -- core: read a page into a slot, reading in if necessary ----------------

    /// slru.c SimpleLruReadPage: ensure `pageno` is resident and return its
    /// global slot number. `write_ok` allows returning a WRITE_IN_PROGRESS page.
    /// `xid` is for error reporting only.
    ///
    /// TEST-ONLY: returning the slot drops the bank lock, so a re-lock-by-slot
    /// access (`with_page`) races a concurrent eviction. Production code MUST use
    /// `read_page_with`/`read_page_readonly_with`, which hold the lock across use.
    #[cfg(test)]
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
                let mut bank = self.bank(pageno).write().unwrap();
                let bankstart = bank.bankstart;
                match self.select_lru(&bank, pageno) {
                    SlruSelect::Ready(i) => {
                        let resident = bank.page_number[i] == pageno
                            && !matches!(bank.page_status[i], SlruPageStatus::Empty);
                        if resident {
                            let must_wait =
                                matches!(bank.page_status[i], SlruPageStatus::ReadInProgress)
                                    || (matches!(
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
                }
                Next::WriteVictim(slot) => {
                    self.write_page(slot).await;
                }
                Next::Read(slot) => {
                    let local = slot - self.bankstart(pageno);
                    let mut io = InProgressSlruIo::arm(self, pageno, slot, true);
                    let mut tmp = Box::new([0u8; BLCKSZ_USIZE]);
                    let ok = self.physical_read(pageno, &mut tmp).await;

                    let mut bank = self.bank(pageno).write().unwrap();
                    bank.page_buffer[local].copy_from_slice(&tmp[..]);
                    bank.zero_lsns(local);
                    bank.page_status[local] = if ok {
                        SlruPageStatus::Valid
                    } else {
                        SlruPageStatus::Empty
                    };
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

    /// Closure form of `read_page` (the EXCLUSIVE / read-in path): ensure
    /// `pageno` is resident, then run `f` on the slot buffer UNDER THE HELD WRITE
    /// LOCK in the same critical section that confirmed residency -- closing the
    /// return-slot-then-relock eviction race. Marks the page dirty + recently
    /// used. `write_ok` allows running `f` on a WRITE_IN_PROGRESS page.
    pub async fn read_page_with<R>(
        &self,
        pageno: i64,
        write_ok: bool,
        xid: TransactionId,
        f: impl FnOnce(SlruPageMut<'_>) -> R,
    ) -> R {
        let mut f = Some(f);
        loop {
            // Same single-critical-section select+claim as read_page. The only
            // difference: when the page is resident-and-ready, we run `f` here
            // under the held write lock instead of returning the slot.
            enum Next<'q> {
                Read(usize),
                WriteVictim(usize),
                Wait(WaitGuard<'q>),
            }
            let next = {
                let mut bank = self.bank(pageno).write().unwrap();
                let bankstart = bank.bankstart;
                let lsn_groups = bank.lsn_groups_per_page;
                match self.select_lru(&bank, pageno) {
                    SlruSelect::Ready(i) => {
                        let resident = bank.page_number[i] == pageno
                            && !matches!(bank.page_status[i], SlruPageStatus::Empty);
                        if resident {
                            let must_wait =
                                matches!(bank.page_status[i], SlruPageStatus::ReadInProgress)
                                    || (matches!(
                                        bank.page_status[i],
                                        SlruPageStatus::WriteInProgress
                                    ) && !write_ok);
                            if must_wait {
                                Next::Wait(self.slot_io[bankstart + i].enqueue())
                            } else {
                                bank.recently_used(i);
                                let page = SlruPageMut::new(&mut bank, i, lsn_groups);
                                return (f.take().unwrap())(page);
                            }
                        } else {
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
                Next::Wait(g) => {
                    g.await;
                }
                Next::WriteVictim(slot) => {
                    self.write_page(slot).await;
                }
                Next::Read(slot) => {
                    let local = slot - self.bankstart(pageno);
                    let mut io = InProgressSlruIo::arm(self, pageno, slot, true);
                    let mut tmp = Box::new([0u8; BLCKSZ_USIZE]);
                    let ok = self.physical_read(pageno, &mut tmp).await;

                    let mut bank = self.bank(pageno).write().unwrap();
                    let lsn_groups = bank.lsn_groups_per_page;
                    bank.page_buffer[local].copy_from_slice(&tmp[..]);
                    bank.zero_lsns(local);
                    bank.page_status[local] = if ok {
                        SlruPageStatus::Valid
                    } else {
                        SlruPageStatus::Empty
                    };
                    bank.recently_used(local);
                    if !ok {
                        drop(bank);
                        io.disarm();
                        self.slot_io[slot].wake_all();
                        slru_report_io_error(&self.dir, pageno, xid, "read");
                    }
                    // Run f UNDER the held write lock, in the section that just
                    // made the page resident -- then wake I/O waiters.
                    let page = SlruPageMut::new(&mut bank, local, lsn_groups);
                    let r = (f.take().unwrap())(page);
                    drop(bank);
                    io.disarm();
                    self.slot_io[slot].wake_all();
                    return r;
                }
            }
        }
    }

    /// slru.c SimpleLruReadPage_ReadOnly: the SHARED fast path. Take the READ
    /// lock and scan for a Valid (not *InProgress) slot holding `pageno`; on a
    /// hit, bump the LRU hint (atomic) and run `f` on `&page` UNDER THE HELD READ
    /// LOCK -- never forming `&mut` under the shared lock. On a miss, drop the
    /// read lock and fall back to the exclusive `read_page_with` (`f` is consumed
    /// exactly once in whichever branch runs).
    pub async fn read_page_readonly_with<R>(
        &self,
        pageno: i64,
        xid: TransactionId,
        f: impl FnOnce(SlruPageRef<'_>) -> R,
    ) -> R {
        {
            let bank = self.bank(pageno).read().unwrap();
            if let Some(i) = bank.find(pageno)
                && matches!(
                    bank.page_status[i],
                    SlruPageStatus::Valid | SlruPageStatus::WriteInProgress
                ) {
                    bank.recently_used(i);
                    let page = SlruPageRef::new(&bank, i, bank.lsn_groups_per_page);
                    return f(page);
                }
        }
        // Miss: regular exclusive read. Adapt the &mut wrapper to the & one.
        self.read_page_with(pageno, true, xid, |page| f(page.as_ref()))
            .await
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
                let mut bank = self.bank(pageno).write().unwrap();
                let bankstart = bank.bankstart;
                match self.select_lru(&bank, pageno) {
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
                    self.latest_page_number
                        .store(pageno as u64, Ordering::Relaxed);
                    return slot;
                }
                Next::Wait(g) => {
                    g.await;
                }
                Next::WriteVictim(slot) => {
                    self.write_page(slot).await;
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
            let mut bank = self.banks[bankno].write().unwrap();
            // If a write is already in progress or not dirty/valid, nothing to do.
            if !bank.page_dirty[local] || !matches!(bank.page_status[local], SlruPageStatus::Valid)
            {
                return;
            }
            let pageno = bank.page_number[local];
            bank.page_status[local] = SlruPageStatus::WriteInProgress;
            bank.page_dirty[local] = false;
            let bytes = bank.page_buffer[local].clone();
            let max_lsn = if bank.lsn_groups_per_page > 0 {
                let base = local * bank.lsn_groups_per_page;
                bank.group_lsn[base..base + bank.lsn_groups_per_page]
                    .iter()
                    .copied()
                    .max()
                    .unwrap_or(0)
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

        let mut bank = self.banks[bankno].write().unwrap();
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
                let bank = self.banks[bankno].read().unwrap();
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
        let segno = pageno / i64::from(SLRU_PAGES_PER_SEGMENT);
        let rpageno = pageno % i64::from(SLRU_PAGES_PER_SEGMENT);
        let need = (rpageno as u64 + 1) * BLCKSZ_U64;
        let path = self.segment_path(segno);
        match self.fd.open(&path, OpenFlags::read_only()).await {
            Ok(f) => f.size().await.is_ok_and(|sz| sz >= need),
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
                let mut bank = self.banks[bankno].write().unwrap();
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
        for (name, segpage) in self.scan_directory() {
            if self.may_delete_segment(segpage, cutoff_page) {
                self.delete_segment_file(segpage / i64::from(SLRU_PAGES_PER_SEGMENT), &name)
                    .await;
            }
        }
    }

    /// slru.c SlruMayDeleteSegment.
    fn may_delete_segment(&self, segpage: i64, cutoff_page: i64) -> bool {
        let last = segpage + i64::from(SLRU_PAGES_PER_SEGMENT) - 1;
        (self.page_precedes)(segpage, cutoff_page) && (self.page_precedes)(last, cutoff_page)
    }

    /// slru.c SlruScanDirectory: returns (filename, first-page) for each valid
    /// SLRU segment file in the directory.
    fn scan_directory(&self) -> Vec<(String, i64)> {
        let mut out = Vec::new();
        let Ok(rd) = std::fs::read_dir(&self.dir) else {
            return out;
        };
        for entry in rd.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let len = name.len();
            let len_ok = if self.long_segment_names {
                len == 15
            } else {
                len == 4 || len == 5 || len == 6
            };
            if len_ok
                && name
                    .bytes()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase())
                && let Ok(segno) = i64::from_str_radix(&name, 16) {
                    out.push((name, segno * i64::from(SLRU_PAGES_PER_SEGMENT)));
                }
        }
        out
    }

    /// slru.c SlruInternalDeleteSegment: forget fsync requests + unlink.
    async fn delete_segment_file(&self, segno: i64, _name: &str) {
        if self.sync_handler != SyncRequestHandler::None {
            let tag = self.file_tag(segno);
            self.sync_requests
                .register_tag(&tag, SyncRequestType::SyncForgetRequest);
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
                    let mut bank = self.banks[bankno].write().unwrap();
                    if matches!(bank.page_status[local], SlruPageStatus::Empty)
                        || bank.page_number[local] / i64::from(SLRU_PAGES_PER_SEGMENT) != segno
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

    // -- slot-addressed accessors (TEST ONLY) ---------------------------------
    //
    // These re-lock BY SLOT without re-validating page_number[slot] == pageno, so
    // a concurrent eviction could reuse the slot for a different page between the
    // read_page that returned the slot and the access here. Production code MUST
    // use the closure API (read_page_with / read_page_readonly_with), which holds
    // the lock continuously across find+use. Kept only for single-threaded tests.

    /// Read a resident slot's page buffer (test only; see warning above).
    #[cfg(test)]
    pub fn with_page<R>(
        &self,
        pageno: i64,
        slot: usize,
        f: impl FnOnce(&[u8; BLCKSZ_USIZE]) -> R,
    ) -> R {
        let bankno = self.bankno(pageno);
        let local = slot - bankno * SLRU_BANK_SIZE;
        let bank = self.banks[bankno].read().unwrap();
        f(&bank.page_buffer[local])
    }

    /// Mutate a resident slot's page buffer and mark it dirty (test only).
    #[cfg(test)]
    pub fn with_page_mut<R>(
        &self,
        pageno: i64,
        slot: usize,
        f: impl FnOnce(&mut [u8; BLCKSZ_USIZE]) -> R,
    ) -> R {
        let bankno = self.bankno(pageno);
        let local = slot - bankno * SLRU_BANK_SIZE;
        let mut bank = self.banks[bankno].write().unwrap();
        let r = f(&mut bank.page_buffer[local]);
        bank.page_dirty[local] = true;
        r
    }

    pub fn lsn_groups_per_page(&self) -> usize {
        self.lsn_groups_per_page
    }

    /// Set latest_page_number directly (clog StartupCLOG).
    pub fn set_latest_page_number(&self, pageno: i64) {
        self.latest_page_number
            .store(pageno as u64, Ordering::Relaxed);
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

    fn ctl(shared: &Arc<SharedState>, subdir: &str) -> Arc<SlruCtl> {
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
        let c = ctl(&shared, "slru_rw");
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
        let c = ctl(&shared, "slru_zero");
        // page 0 lives in bank 0; page 99 also resolves; read a page never written.
        let slot = c.read_page(0, false, TransactionId(0)).await;
        let all_zero = c.with_page(0, slot, |buf| buf.iter().all(|&b| b == 0));
        assert!(all_zero, "a never-written page must read as zeroes");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn evict_and_reload_preserves_data() {
        let shared = temp_shared("evict");
        let c = ctl(&shared, "slru_evict");
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
        let c = ctl(&shared, "slru_race");
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
        let c = ctl(&shared, "slru_victim");
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

    // read_page_with mutates under the held write lock; read_page_readonly_with
    // reads it back. Exercises both the shared HIT path (page resident) and the
    // MISS path (page evicted -> exclusive read-in).
    #[tokio::test(flavor = "multi_thread")]
    async fn readonly_with_hit_and_miss() {
        let shared = temp_shared("ro");
        let c = ctl(&shared, "slru_ro");
        // Bring page 0 in and write a marker via the closure write path.
        c.read_page_with(0, true, TransactionId(0), |mut p| p.buf_mut()[7] = 0x5A)
            .await;
        c.write_page(0).await; // make it clean so it can be a victim later

        // HIT: page 0 is resident -> shared fast path returns the marker.
        let hit = c
            .read_page_readonly_with(0, TransactionId(0), |p| p.buf()[7])
            .await;
        assert_eq!(hit, 0x5A, "shared hit path must see the written byte");

        // Evict page 0 from bank 0 (even pages share bank 0; nbanks=2). Persist
        // each filler page so its segment exists for any later read-in.
        for p in (2..=(SLRU_BANK_SIZE as i64 * 2 * 2)).step_by(2) {
            let s = c.zero_page(p).await;
            c.write_page(s).await;
        }
        // MISS: page 0 must be re-read from disk via the exclusive fallback.
        let miss = c
            .read_page_readonly_with(0, TransactionId(0), |p| p.buf()[7])
            .await;
        assert_eq!(
            miss, 0x5A,
            "miss path must re-read the page and see the byte"
        );
    }

    // Concurrent readers of a resident page take the shared lock simultaneously.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_shared_readers() {
        let shared = temp_shared("shared");
        let c = ctl(&shared, "slru_shared");
        c.read_page_with(0, true, TransactionId(0), |mut p| p.buf_mut()[0] = 0x42)
            .await;

        let mut hs = Vec::new();
        for _ in 0..8 {
            let cc = c.clone();
            hs.push(tokio::spawn(async move {
                cc.read_page_readonly_with(0, TransactionId(0), |p| p.buf()[0])
                    .await
            }));
        }
        for h in hs {
            assert_eq!(h.await.unwrap(), 0x42);
        }
    }

    // group_lsn set in the write closure is visible to the readonly closure on
    // the same resident page (clog async-commit LSN path).
    #[tokio::test(flavor = "multi_thread")]
    async fn group_lsn_via_closures() {
        let shared = temp_shared("glsn");
        let c = SlruCtl::new(
            SLRU_BANK_SIZE * 2,
            4, // lsn_groups_per_page > 0
            "slru_glsn",
            SyncRequestHandler::None,
            false,
            never_precedes,
            shared.fd().clone(),
            shared.xlog().clone(),
            shared.sync_requests().clone(),
            shared.config().data_dir(),
        );
        c.read_page_with(0, true, TransactionId(0), |mut p| {
            p.buf_mut()[0] = 1;
            p.set_group_lsn(2, 0xABCD);
        })
        .await;
        let lsn = c
            .read_page_readonly_with(0, TransactionId(0), |p| p.group_lsn(2))
            .await;
        assert_eq!(lsn, 0xABCD);
    }
}

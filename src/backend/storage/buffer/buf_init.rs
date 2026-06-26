//! Translated from PostgreSQL src/backend/storage/buffer/buf_init.c plus the
//! buffer-header / pin / IO-wait state machine from bufmgr.c (the parts that do
//! NOT do smgr I/O -- the actual read/flush is Part B).
//!
//! C shape: `BufferManagerShmemInit` allocates four parallel shmem arrays -- the
//! `BufferDescPadded` descriptors, the `BufferBlocks` page pool (IO-aligned), the
//! per-buffer `BufferIOCVArray` condition variables, and the checkpoint sort
//! array -- then links the descriptors into a freelist and calls
//! `StrategyInitialize`. The buffer-header spinlock (`BM_LOCKED`), pin counts
//! (`refcount`/`usagecount` packed in `state`), and the IO-in-progress handshake
//! (`BM_IO_IN_PROGRESS` + the per-buffer CV) live in bufmgr.c.
//!
//! PepperDB shape (rules.md sections 6.3 / 9): the shmem segment is gone, so the
//! pool is one owned [`BufferPool`] on the heap behind an `Arc` on `SharedState`.
//! It holds an 8-aligned `Box<[Page]>` (step-10's `#[repr(C, align(8))]` Page is
//! what makes the header overlays sound for real buffers), a `Box<[BufferDesc]>`,
//! the sharded [`BufTable`], and the clock-sweep [`StrategyControl`].
//!
//! Concurrency mapping:
//!  * `pg_atomic_uint32 state` -> `AtomicU32`; mutated only via CAS or under the
//!    header lock, exactly as C (buf_internals.h: "updating of state without
//!    holding buffer header lock is restricted to CAS").
//!  * the `BM_LOCKED` spinlock -> [`BufferDesc::lock_hdr`] / [`BufferDesc::unlock_hdr`],
//!    a brief sync `fetch_or`/`store` CAS spin -- NEVER held across an `.await`.
//!  * the content LWLock -> a naked `std::sync::RwLock<()>` guarding the page
//!    bytes in the pool array (parking_lot is not a dependency).
//!  * the per-buffer IO condition variable + `BM_IO_IN_PROGRESS` -> a
//!    [`WaitQueue`] (cancellation-safe `WaitGuard`) + the flag; the waiter awaits
//!    the queue with NO lock held, the IO-doer wakes it on terminate.
//!  * `PrivateRefCount` (per-backend pin cache) -> a per-task refcount map in a
//!    tokio `task_local!` `RefCell`. It is per-task and never shared, so it stays
//!    `Send` (the `Arc<BufferPool>` carried across tasks is `Send + Sync`). A pin
//!    is held across an `.await` (the read/flush I/O), and the multi-thread
//!    runtime migrates the task between threads at that point, so a plain
//!    `thread_local` would split the map across threads and misfire the
//!    shared-refcount-gating 0->1 / 1->0 transitions; the `task_local` follows
//!    the task across migration and keeps the gating exact.

use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::storage::buf::{BufId, Buffer};
use crate::storage::buf_internals::{
    BUF_REFCOUNT_ONE, BUF_USAGECOUNT_ONE, BM_MAX_USAGE_COUNT, BufFlags, BufferTag,
    buf_state_get_refcount, buf_state_get_usagecount,
};
use crate::storage::bufpage::Page;
use crate::storage::procnumber::INVALID_PROC_NUMBER;
use crate::storage::wait_guard::WaitQueue;

use super::buf_table::BufTable;
use super::freelist::StrategyControl;

/// Per-buffer descriptor. C `BufferDesc` (buf_internals.h), minus the shmem
/// layout contract.
///
/// `state` packs flags (high 10 bits) + usagecount (4) + refcount (18); see the
/// `BUF_*` / [`BufFlags`] consts in the header. The header lock is the
/// `BM_LOCKED` bit; the content lock and IO-wait queue are added here for the
/// rewrite (they sit outside the atomic, as in C where they are a separate
/// LWLock and CV).
pub struct BufferDesc {
    /// ID of the page contained in the buffer. C: only valid/stable to read
    /// without the header lock while the buffer is pinned; written only under
    /// the header lock. In an `UnsafeCell` so it can be set through `&self` (the
    /// pool is `Arc`-shared) -- the header lock serializes writes and a pin makes
    /// reads stable, exactly as C. Access via [`tag_copy`](Self::tag_copy) /
    /// [`set_tag`](Self::set_tag) / [`clear_tag`](Self::clear_tag).
    tag: UnsafeCell<BufferTag>,
    /// Buffer index, 0-based; never changes after init (handle = Global(buf_id)).
    pub buf_id: i32,
    /// Packed flags / refcount / usagecount. Mutated ONLY via CAS or under the
    /// header lock (`BM_LOCKED`).
    pub state: AtomicU32,
    /// Backend (task) waiting for sole pin (LockBufferForCleanup). C:
    /// `wait_backend_pgprocno`; written under the header lock.
    pub wait_backend_pgprocno: AtomicU32,
    /// Freelist link, protected by the strategy lock (not the header lock). C:
    /// `freeNext`. Sentinels: [`FREENEXT_END_OF_LIST`] / [`FREENEXT_NOT_IN_LIST`].
    pub free_next: AtomicU32,
    /// Naked content lock guarding the page bytes at `pool.blocks[buf_id]`. C:
    /// the embedded `content_lock` LWLock. NEVER held across an `.await`.
    pub content_lock: RwLock<()>,
    /// Per-buffer IO-wait queue. C: the entry in `BufferIOCVArray`. A waiter
    /// awaits this (no lock held) while `BM_IO_IN_PROGRESS` is set; the IO-doer
    /// wakes it in [`terminate_buffer_io`](BufferPool::terminate_buffer_io).
    pub io_cv: WaitQueue,
}

// SAFETY: the only non-`Sync` field is the `UnsafeCell<BufferTag>`. Writes to it
// happen only under the header lock (`lock_hdr`/`unlock_hdr` serialize them) and
// reads are stable while the buffer is pinned -- the same discipline C uses for
// the plain `BufferTag` field guarded by the header spinlock + the pin.
unsafe impl Sync for BufferDesc {}

/// freeNext sentinels (re-export of buf_internals.h `FREENEXT_*`, stored in the
/// atomic as the bit pattern of the i32). C: `FREENEXT_END_OF_LIST` / `_NOT_IN_LIST`.
pub const FREENEXT_END_OF_LIST: i32 = -1;
pub const FREENEXT_NOT_IN_LIST: i32 = -2;

impl BufferDesc {
    fn new(buf_id: i32) -> Self {
        let mut tag = BufferTag {
            spc_oid: crate::postgres_ext::InvalidOid,
            db_oid: crate::postgres_ext::InvalidOid,
            rel_number: crate::common::relpath::InvalidRelFileNumber,
            fork_num: crate::common::relpath::ForkNumber::InvalidForkNumber,
            block_num: crate::storage::block::INVALID_BLOCK_NUMBER,
        };
        tag.clear();
        Self {
            tag: UnsafeCell::new(tag),
            buf_id,
            state: AtomicU32::new(0),
            wait_backend_pgprocno: AtomicU32::new(INVALID_PROC_NUMBER as u32),
            // Initially every buffer links to the next (freelist), patched up
            // for the last entry in BufferPool::new. C: buf->freeNext = i + 1.
            free_next: AtomicU32::new((buf_id + 1) as u32),
            content_lock: RwLock::new(()),
            io_cv: WaitQueue::new(),
        }
    }

    /// C: `BufferDescriptorGetBuffer` -- the global handle for this descriptor.
    #[inline]
    pub fn buffer(&self) -> Buffer {
        BufId::Global(self.buf_id as u32)
    }

    /// A copy of the buffer's tag. Stable to read while the buffer is pinned
    /// (C reads the tag without the header lock when pinned). SAFETY: the cell is
    /// only written under the header lock; a concurrent write would require the
    /// buffer to be unpinned, which the caller's pin prevents.
    #[inline]
    pub fn tag_copy(&self) -> BufferTag {
        // SAFETY: see method doc; no &mut to the cell is live for a pinned buffer.
        unsafe { *self.tag.get() }
    }

    /// Set the buffer's tag. The caller MUST hold the header lock (it serializes
    /// tag writes). SAFETY: header lock held -> sole writer.
    #[inline]
    pub fn set_tag(&self, tag: BufferTag) {
        // SAFETY: caller holds the header lock; no other access to the cell.
        unsafe { *self.tag.get() = tag };
    }

    /// Clear the buffer's tag to the invalid sentinel. Header lock required.
    #[inline]
    pub fn clear_tag(&self) {
        // SAFETY: caller holds the header lock; no other access to the cell.
        unsafe { (*self.tag.get()).clear() };
    }

    /// C: `LockBufHdr`. Spin-CAS to set `BM_LOCKED`, returning the prior state
    /// word with `BM_LOCKED` OR'd in (so the caller can mutate it and write it
    /// back via [`unlock_hdr`](Self::unlock_hdr)).
    ///
    /// Brief, synchronous, NEVER awaited across. `fetch_or(Acquire)` both sets
    /// the bit and reads the prior value; if `BM_LOCKED` was already set we spin.
    /// Acquire orders subsequent state reads after we observe the lock taken.
    pub fn lock_hdr(&self) -> u32 {
        loop {
            let old = self
                .state
                .fetch_or(BufFlags::LOCKED.bits(), Ordering::Acquire);
            if old & BufFlags::LOCKED.bits() == 0 {
                return old | BufFlags::LOCKED.bits();
            }
            std::hint::spin_loop();
        }
    }

    /// C: `UnlockBufHdr`. Publish `buf_state` with `BM_LOCKED` cleared. The
    /// `Release` store pairs with the next `lock_hdr` `Acquire` and with the CAS
    /// loops, ensuring all mutations made under the lock are visible. C does a
    /// `pg_write_barrier()` then a plain atomic write; `Release` subsumes both.
    pub fn unlock_hdr(&self, buf_state: u32) {
        self.state
            .store(buf_state & !BufFlags::LOCKED.bits(), Ordering::Release);
    }

    /// C: `WaitBufHdrUnlocked`. Spin until `BM_LOCKED` clears, returning the
    /// observed state. Used by the lock-free pin/unpin CAS loops, which must not
    /// CAS while another task holds the header lock (it can do a plain write).
    fn wait_hdr_unlocked(&self) -> u32 {
        loop {
            let s = self.state.load(Ordering::Acquire);
            if s & BufFlags::LOCKED.bits() == 0 {
                return s;
            }
            std::hint::spin_loop();
        }
    }
}

// === Per-task PrivateRefCount (C: the per-backend pin cache in bufmgr.c) ======
//
// C keeps a small fixed array + overflow hash of (Buffer -> local refcount) per
// backend so repeated pins of the same buffer by one backend touch the shared
// refcount only once. We model it per-TASK: a tokio `task_local` map.
//
// Why task_local and not thread_local: a pin is held across the read/flush
// `.await`, and the multi-thread runtime migrates the task across threads at the
// suspension point. A thread_local would leave half the map on the origin thread
// and the rest on the destination thread, so the "first private pin does the
// shared CAS, last private unpin undoes it" gating would misfire and corrupt the
// shared refcount. A task_local follows the task across migration. The map is
// touched only inside synchronous pin/unpin sections (borrow, mutate, drop --
// never held across an `.await`), so the RefCell is Send-safe.
//
// The authoritative shared refcount lives in `state`; this map only gates the
// shared CAS (first pin: shared++, last unpin: shared--), exactly as PG.

tokio::task_local! {
    static PRIVATE_REFCOUNT: RefCell<HashMap<Buffer, u32>>;
}

/// Run `f` with the per-task PrivateRefCount map established, scoping it if not
/// already present. Backends own their task scope; tests that pin/unpin outside
/// `with_private_refcount` use the auto-init via [`with_refcount_map`].
pub async fn with_private_refcount<F, Fut, T>(f: F) -> T
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    if PRIVATE_REFCOUNT.try_with(|_| ()).is_ok() {
        f().await
    } else {
        PRIVATE_REFCOUNT.scope(RefCell::new(HashMap::new()), f()).await
    }
}

/// Access the per-task PrivateRefCount map. If no task-local scope is active
/// (e.g. a synchronous unit test), fall back to a thread-local map so the
/// pin/unpin helpers still work outside an async backend scope. Async callers
/// holding a pin across an `.await` MUST run inside [`with_private_refcount`].
fn with_refcount_map<R>(f: impl FnOnce(&mut HashMap<Buffer, u32>) -> R) -> R {
    thread_local! {
        static FALLBACK: RefCell<HashMap<Buffer, u32>> = RefCell::new(HashMap::new());
    }
    match PRIVATE_REFCOUNT.try_with(std::ptr::from_ref::<RefCell<HashMap<Buffer, u32>>>) {
        Ok(ptr) => {
            // SAFETY: the pointer is valid for the duration of this call -- the
            // task_local outlives the synchronous `f`, which never `.await`s.
            f(&mut unsafe { &*ptr }.borrow_mut())
        }
        Err(_) => FALLBACK.with(|m| f(&mut m.borrow_mut())),
    }
}

/// Test/inspection helper: this task's private (local) pin count for a buffer.
pub fn private_refcount(buffer: Buffer) -> u32 {
    with_refcount_map(|m| m.get(&buffer).copied().unwrap_or(0))
}

/// A page slot in the pool: an `UnsafeCell<Page>` so the `Arc`-shared pool can
/// hand out `&mut Page` from `&self` under the IO/content-lock invariant. The
/// wrapper exists only to assert `Sync` (an `UnsafeCell` is not `Sync`); the
/// safety contract is upheld by the buffer-header state machine, not the type.
struct PageCell(UnsafeCell<Page>);

// SAFETY: aliasing of the inner `Page` is prevented by `BM_IO_IN_PROGRESS` (read
// / flush) and the per-buffer content lock (page mutation); the pool never hands
// out two overlapping `&mut Page` for the same slot concurrently.
unsafe impl Sync for PageCell {}

/// The shared buffer pool: replaces C's `BufferBlocks` + `BufferDescriptors` +
/// `SharedBufHash` + `StrategyControl`, owned on `SharedState`.
pub struct BufferPool {
    /// IO-aligned page pool. C: `BufferBlocks` (TYPEALIGN'd to PG_IO_ALIGN_SIZE).
    /// `Page` is `align(8)`; the base is 8-aligned, enough for the header
    /// overlays. Indexed by `buf_id` (0-based).
    ///
    /// Each page sits in an [`UnsafeCell`] so the pool, shared behind `Arc`, can
    /// still hand out `&mut Page` for an in-place read/write while only holding
    /// `&self`. Safety is the SAME invariant C relies on: exclusive access to a
    /// page's bytes is gated by `BM_IO_IN_PROGRESS` (the read/write doer) or the
    /// per-buffer content lock (page mutation), so at most one writer touches a
    /// page at a time. See [`block`](Self::block) / [`block_mut`](Self::block_mut).
    blocks: Box<[PageCell]>,
    /// Per-buffer descriptors. C: `BufferDescriptors`. Indexed by `buf_id`.
    descriptors: Box<[BufferDesc]>,
    /// Tag -> buf_id map. C: `SharedBufHash` (sharded here).
    pub buf_table: BufTable,
    /// Clock-sweep / freelist control. C: `StrategyControl`.
    pub strategy: StrategyControl,
    nbuffers: usize,
}

impl BufferPool {
    /// C: `BufferManagerShmemInit` + `StrategyInitialize`. Allocate `nbuffers`
    /// aligned page blocks and descriptors, link the freelist, and init the
    /// strategy. `nbuffers` comes from the `NBuffers` GUC (`ProcessConfig`);
    /// callers pass a small value in tests.
    pub fn new(nbuffers: usize) -> Self {
        assert!(nbuffers > 0, "NBuffers must be positive");

        // Heap-allocate the page pool zeroed without an 8 KB-per-page stack move.
        let blocks: Box<[PageCell]> = (0..nbuffers)
            .map(|_| PageCell(UnsafeCell::new(Page::zeroed())))
            .collect();

        let descriptors: Box<[BufferDesc]> = (0..nbuffers)
            .map(|i| BufferDesc::new(i as i32))
            .collect();
        // C: correct last freelist entry to FREENEXT_END_OF_LIST.
        descriptors[nbuffers - 1]
            .free_next
            .store(FREENEXT_END_OF_LIST as u32, Ordering::Relaxed);

        let strategy = StrategyControl::new(nbuffers);

        Self {
            blocks,
            descriptors,
            buf_table: BufTable::new(),
            strategy,
            nbuffers,
        }
    }

    /// C: `NBuffers`. Number of shared buffers in the pool.
    #[inline]
    pub fn nbuffers(&self) -> usize {
        self.nbuffers
    }

    /// C: `GetBufferDescriptor(id)`. Descriptor for a 0-based buf_id.
    #[inline]
    pub fn descriptor(&self, buf_id: i32) -> &BufferDesc {
        &self.descriptors[buf_id as usize]
    }

    /// All descriptors (for the clock sweep / checkpoint scans).
    #[inline]
    pub fn descriptors(&self) -> &[BufferDesc] {
        &self.descriptors
    }

    /// C: `BufHdrGetBlock` / `BufferGetBlock`. A shared view of the page bytes
    /// for a 0-based buf_id. Callers must hold a pin (and, for a consistent
    /// read, the content lock) per the buffer access rules; this just resolves
    /// the storage.
    ///
    /// SAFETY: returns `&Page` from `&self`; sound as long as no `&mut Page` for
    /// the same slot is live (the IO/content-lock invariant). The byte
    /// dereference is read-only.
    #[inline]
    pub fn block(&self, buf_id: i32) -> &Page {
        // SAFETY: no concurrent &mut to this slot under the access invariant.
        unsafe { &*self.blocks[buf_id as usize].0.get() }
    }

    /// Exclusive view of the page bytes for an in-place read or write. The
    /// caller MUST hold exclusive access to this slot: either `BM_IO_IN_PROGRESS`
    /// (the read/flush doer -- only one task wins `start_buffer_io`) or the
    /// content lock in exclusive mode (page mutation). No two overlapping
    /// `&mut Page` for one slot may be live at once.
    ///
    /// SAFETY: the caller upholds the single-writer invariant above. This is the
    /// same contract C's `BufHdrGetBlock` relies on (it returns a raw pointer).
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn block_mut(&self, buf_id: i32) -> &mut Page {
        // SAFETY: caller holds BM_IO_IN_PROGRESS or the exclusive content lock,
        // so this is the only live reference to the slot.
        unsafe { &mut *self.blocks[buf_id as usize].0.get() }
    }

    /// 8-alignment / size assertions for the page pool base (tests rely on this).
    #[inline]
    fn block_base_addr(&self) -> usize {
        self.blocks.as_ptr() as usize
    }

    // === Pin / unpin (C: PinBuffer / PinBuffer_Locked / UnpinBuffer) =========

    /// C: `PinBuffer`. Increment the shared refcount (CAS loop) and bump the
    /// usagecount toward `BM_MAX_USAGE_COUNT` (default strategy), recording a
    /// private pin. Returns whether the buffer is `BM_VALID` (C `result`).
    ///
    /// The first private pin does the shared CAS; subsequent same-task pins only
    /// bump the private count, matching C's PrivateRefCount fast path.
    pub fn pin_buffer(&self, buf_id: i32) -> bool {
        let desc = self.descriptor(buf_id);
        let b = desc.buffer();

        let had_private = private_refcount(b) > 0;
        let valid = if had_private {
            desc.state.load(Ordering::Relaxed) & BufFlags::VALID.bits() != 0
        } else {
            let mut old = desc.state.load(Ordering::Relaxed);
            loop {
                if old & BufFlags::LOCKED.bits() != 0 {
                    old = desc.wait_hdr_unlocked();
                }
                let mut new = old + BUF_REFCOUNT_ONE;
                // Default strategy: bump usagecount unless already at max.
                if buf_state_get_usagecount(new) < BM_MAX_USAGE_COUNT {
                    new += BUF_USAGECOUNT_ONE;
                }
                match desc.state.compare_exchange_weak(
                    old,
                    new,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break new & BufFlags::VALID.bits() != 0,
                    Err(cur) => old = cur,
                }
            }
        };

        with_refcount_map(|m| *m.entry(b).or_insert(0) += 1);
        valid
    }

    /// C: `PinBuffer_Locked`. Pin a buffer whose header lock is already held by
    /// the caller, passing in the locked `buf_state`. Increments refcount and
    /// releases the header lock in one publish, then records the private pin.
    /// The caller must hold the header lock and have no preexisting private pin.
    pub fn pin_buffer_locked(&self, buf_id: i32, buf_state: u32) {
        let desc = self.descriptor(buf_id);
        let b = desc.buffer();
        debug_assert_eq!(private_refcount(b), 0);
        debug_assert!(buf_state & BufFlags::LOCKED.bits() != 0);
        // Update state and clear BM_LOCKED in one write (C does the same).
        desc.unlock_hdr(buf_state + BUF_REFCOUNT_ONE);
        with_refcount_map(|m| *m.entry(b).or_insert(0) += 1);
    }

    /// C: `UnpinBuffer`. Drop one private pin; when the last private pin for this
    /// task goes, decrement the shared refcount via a CAS loop.
    pub fn unpin_buffer(&self, buf_id: i32) {
        let desc = self.descriptor(buf_id);
        let b = desc.buffer();

        let drop_shared = with_refcount_map(|map| {
            let count = map.get_mut(&b).expect("unpin without a private pin");
            debug_assert!(*count > 0);
            *count -= 1;
            if *count == 0 {
                map.remove(&b);
                true
            } else {
                false
            }
        });

        if drop_shared {
            let mut old = desc.state.load(Ordering::Relaxed);
            loop {
                if old & BufFlags::LOCKED.bits() != 0 {
                    old = desc.wait_hdr_unlocked();
                }
                let new = old - BUF_REFCOUNT_ONE;
                match desc.state.compare_exchange_weak(
                    old,
                    new,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(cur) => old = cur,
                }
            }
            // C wakes a BM_PIN_COUNT_WAITER here (LockBufferForCleanup support);
            // the cleanup-lock path is Part B, so nothing to wake yet.
        }
    }

    /// Pin a buffer and return an RAII [`PinGuard`] that unpins exactly once on
    /// Drop (cancellation-safe). Use this instead of bare pin/unpin so a panic
    /// or future cancellation cannot leak a pin.
    pub fn pin_guard(self: &std::sync::Arc<Self>, buf_id: i32) -> PinGuard {
        self.pin_buffer(buf_id);
        PinGuard {
            pool: self.clone(),
            buf_id,
        }
    }

    // === IO-in-progress handshake (C: StartBufferIO / WaitIO / TerminateBufferIO)

    /// C: `StartBufferIO(buf, forInput, nowait=false)`. Returns `true` if THIS
    /// caller must perform the IO; `false` if another task already did it (or is
    /// doing it -- in which case we await its completion first, then return
    /// false). Sets `BM_IO_IN_PROGRESS` under the header lock on success.
    ///
    /// The wait is via the per-buffer [`WaitQueue`] with NO lock held (the header
    /// lock is dropped before awaiting), so this is cancellation-safe and never
    /// holds a sync lock across the `.await`.
    pub async fn start_buffer_io(&self, buf_id: i32, for_input: bool) -> bool {
        let desc = self.descriptor(buf_id);
        loop {
            let buf_state = desc.lock_hdr();
            if buf_state & BufFlags::IO_IN_PROGRESS.bits() == 0 {
                // No IO active. Did someone already complete it?
                let already = if for_input {
                    buf_state & BufFlags::VALID.bits() != 0
                } else {
                    buf_state & BufFlags::DIRTY.bits() == 0
                };
                if already {
                    desc.unlock_hdr(buf_state);
                    return false;
                }
                // Claim the IO. Clear any BM_IO_ERROR left by a prior failed
                // attempt (C: StartBufferIO resets the error state on a fresh
                // claim, so the re-doer starts clean).
                desc.unlock_hdr(
                    (buf_state | BufFlags::IO_IN_PROGRESS.bits()) & !BufFlags::IO_ERROR.bits(),
                );
                return true;
            }
            // Another task holds the IO. Drop the header lock, then wait on the
            // CV (no lock across the await). Re-check after waking.
            desc.unlock_hdr(buf_state);
            self.wait_io(buf_id).await;
        }
    }

    /// C: `StartBufferIO(buf, forInput, nowait=true)`. The non-blocking variant:
    /// returns `Some(true)` if we claimed the IO, `Some(false)` if it is already
    /// done, and `None` if another task is mid-IO (caller should retry/await).
    pub fn start_buffer_io_nowait(&self, buf_id: i32, for_input: bool) -> Option<bool> {
        let desc = self.descriptor(buf_id);
        let buf_state = desc.lock_hdr();
        if buf_state & BufFlags::IO_IN_PROGRESS.bits() != 0 {
            desc.unlock_hdr(buf_state);
            return None;
        }
        let already = if for_input {
            buf_state & BufFlags::VALID.bits() != 0
        } else {
            buf_state & BufFlags::DIRTY.bits() == 0
        };
        if already {
            desc.unlock_hdr(buf_state);
            return Some(false);
        }
        // Claim; clear any prior BM_IO_ERROR (C: StartBufferIO resets it).
        desc.unlock_hdr(
            (buf_state | BufFlags::IO_IN_PROGRESS.bits()) & !BufFlags::IO_ERROR.bits(),
        );
        Some(true)
    }

    /// C: `WaitIO`. Await until `BM_IO_IN_PROGRESS` clears on the buffer. The
    /// header lock is taken only to read the flag, then dropped before awaiting
    /// the per-buffer queue (rules.md section 5: no lock across `.await`). The
    /// `WaitGuard` dequeues on Drop, so a cancelled wait leaves no stale waiter.
    pub async fn wait_io(&self, buf_id: i32) {
        let desc = self.descriptor(buf_id);
        loop {
            // Enqueue BEFORE re-reading the flag so a terminate racing our check
            // cannot wake us before we are queued (the CV protocol: prepare to
            // sleep up front).
            let guard = desc.io_cv.enqueue();
            let buf_state = desc.lock_hdr();
            let in_progress = buf_state & BufFlags::IO_IN_PROGRESS.bits() != 0;
            desc.unlock_hdr(buf_state);
            if !in_progress {
                return; // guard drops here, dequeuing
            }
            guard.await;
        }
    }

    /// C: `TerminateBufferIO`. Clear `BM_IO_IN_PROGRESS` (and `BM_IO_ERROR`)
    /// under the header lock, optionally clearing dirty and OR-ing in
    /// `set_flag_bits` (e.g. `BM_VALID` after a successful read), then wake every
    /// waiter on the per-buffer queue. The actual smgr read/write is Part B; this
    /// is only the state-machine + wakeup so two racers never both do the IO.
    pub fn terminate_buffer_io(&self, buf_id: i32, clear_dirty: bool, set_flag_bits: u32) {
        let desc = self.descriptor(buf_id);
        let mut buf_state = desc.lock_hdr();
        debug_assert!(buf_state & BufFlags::IO_IN_PROGRESS.bits() != 0);
        buf_state &= !BufFlags::IO_IN_PROGRESS.bits();
        buf_state &= !BufFlags::IO_ERROR.bits();
        if clear_dirty && buf_state & BufFlags::JUST_DIRTIED.bits() == 0 {
            buf_state &= !(BufFlags::DIRTY.bits() | BufFlags::CHECKPOINT_NEEDED.bits());
        }
        buf_state |= set_flag_bits;
        desc.unlock_hdr(buf_state);
        // Wake all waiters (C: ConditionVariableBroadcast). Sync, no lock held.
        desc.io_cv.wake_all();
    }
}

/// RAII pin held by a task. Drop unpins exactly once -- safe under panic or
/// future cancellation (C relied on ResourceOwner to release leaked pins at
/// abort; here ownership + Drop does it directly). C analogue: a buffer pin
/// remembered by the current ResourceOwner.
pub struct PinGuard {
    pool: std::sync::Arc<BufferPool>,
    buf_id: i32,
}

impl PinGuard {
    /// The global handle this guard pins.
    pub fn buffer(&self) -> Buffer {
        BufId::Global(self.buf_id as u32)
    }

    /// The 0-based descriptor index.
    pub fn buf_id(&self) -> i32 {
        self.buf_id
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        self.pool.unpin_buffer(self.buf_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    fn pool(n: usize) -> Arc<BufferPool> {
        Arc::new(BufferPool::new(n))
    }

    #[test]
    fn new_allocates_aligned_blocks() {
        let p = BufferPool::new(8);
        assert_eq!(p.nbuffers(), 8);
        // Page pool base is 8-aligned (Page is align(8)).
        assert_eq!(p.block_base_addr() % 8, 0);
        // Each Page is BLCKSZ bytes.
        assert_eq!(
            core::mem::size_of::<Page>(),
            crate::pg_config::BLCKSZ as usize
        );
        // Descriptors are 0-based; buffer() is 1-based.
        assert_eq!(p.descriptor(0).buffer(), BufId::Global(0));
        assert_eq!(p.descriptor(7).buffer(), BufId::Global(7));
    }

    #[test]
    fn freelist_is_linked_with_terminator() {
        let p = BufferPool::new(4);
        for i in 0..3 {
            assert_eq!(
                p.descriptor(i).free_next.load(Ordering::Relaxed) as i32,
                i + 1
            );
        }
        assert_eq!(
            p.descriptor(3).free_next.load(Ordering::Relaxed) as i32,
            FREENEXT_END_OF_LIST
        );
    }

    #[test]
    fn lock_unlock_hdr_round_trip() {
        let p = BufferPool::new(2);
        let d = p.descriptor(0);
        let s = d.lock_hdr();
        assert!(s & BufFlags::LOCKED.bits() != 0, "lock sets BM_LOCKED");
        // mutate under the lock then publish without BM_LOCKED.
        d.unlock_hdr(s | BufFlags::VALID.bits());
        let now = d.state.load(Ordering::Acquire);
        assert!(now & BufFlags::LOCKED.bits() == 0, "unlock clears BM_LOCKED");
        assert!(now & BufFlags::VALID.bits() != 0, "mutation published");
    }

    #[test]
    fn header_lock_serializes() {
        // A pile of tasks each take the header lock, bump a counter under it,
        // and release. The final count must be exact (mutual exclusion).
        let pool = pool(1);
        let counter = Arc::new(AtomicUsize::new(0));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()
            .unwrap();
        rt.block_on(async {
            let mut handles = Vec::new();
            for _ in 0..16 {
                let pool = pool.clone();
                let counter = counter.clone();
                handles.push(tokio::spawn(async move {
                    for _ in 0..1000 {
                        let desc = pool.descriptor(0);
                        let state = desc.lock_hdr();
                        // non-atomic read-modify-write protected by the hdr lock
                        let cur = counter.load(Ordering::Relaxed);
                        counter.store(cur + 1, Ordering::Relaxed);
                        desc.unlock_hdr(state);
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
        });
        assert_eq!(counter.load(Ordering::Relaxed), 16 * 1000);
    }

    #[test]
    fn pin_increments_refcount_unpin_decrements() {
        let p = BufferPool::new(2);
        let d = p.descriptor(0);
        assert_eq!(buf_state_get_refcount(d.state.load(Ordering::Relaxed)), 0);

        p.pin_buffer(0);
        assert_eq!(buf_state_get_refcount(d.state.load(Ordering::Relaxed)), 1);
        assert_eq!(super::private_refcount(d.buffer()), 1);

        // Second pin by the same task: private count rises, shared stays at 1.
        p.pin_buffer(0);
        assert_eq!(buf_state_get_refcount(d.state.load(Ordering::Relaxed)), 1);
        assert_eq!(super::private_refcount(d.buffer()), 2);

        p.unpin_buffer(0);
        assert_eq!(buf_state_get_refcount(d.state.load(Ordering::Relaxed)), 1);
        p.unpin_buffer(0);
        assert_eq!(buf_state_get_refcount(d.state.load(Ordering::Relaxed)), 0);
        assert_eq!(super::private_refcount(d.buffer()), 0);
    }

    #[test]
    fn usagecount_saturates_at_max() {
        let p = BufferPool::new(1);
        let d = p.descriptor(0);
        // Each fresh pin bumps usagecount; after the local cache resets between
        // pins (full unpin), the shared usagecount keeps climbing to the max.
        for _ in 0..(BM_MAX_USAGE_COUNT + 3) {
            p.pin_buffer(0);
            p.unpin_buffer(0);
        }
        let uc = buf_state_get_usagecount(d.state.load(Ordering::Relaxed));
        assert_eq!(uc, BM_MAX_USAGE_COUNT, "usagecount saturates at the max");
    }

    #[test]
    fn pin_guard_unpins_exactly_once_on_drop() {
        let p = pool(2);
        let d = p.descriptor(1);
        let before = buf_state_get_refcount(d.state.load(Ordering::Relaxed));
        {
            let g = p.pin_guard(1);
            assert_eq!(g.buffer(), BufId::Global(1));
            assert_eq!(
                buf_state_get_refcount(d.state.load(Ordering::Relaxed)),
                before + 1
            );
        } // drop -> unpin
        assert_eq!(
            buf_state_get_refcount(d.state.load(Ordering::Relaxed)),
            before,
            "PinGuard Drop unpins exactly once"
        );
        assert_eq!(super::private_refcount(d.buffer()), 0);
    }

    #[test]
    fn pin_buffer_locked_pins_and_releases_header() {
        let p = BufferPool::new(1);
        let d = p.descriptor(0);
        let s = d.lock_hdr();
        p.pin_buffer_locked(0, s);
        let now = d.state.load(Ordering::Acquire);
        assert!(now & BufFlags::LOCKED.bits() == 0, "header released");
        assert_eq!(buf_state_get_refcount(now), 1);
        assert_eq!(super::private_refcount(d.buffer()), 1);
        p.unpin_buffer(0);
        assert_eq!(buf_state_get_refcount(d.state.load(Ordering::Relaxed)), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn private_refcount_survives_thread_migration() {
        // The REQUIRED Part-A fix: a pin held across an `.await` that migrates
        // the task to another thread must still gate the shared refcount exactly
        // (task_local, not thread_local). Pin, force a migration, pin again,
        // unpin twice, and assert the shared refcount returns to its prior value.
        let p = pool(1);
        let d = p.descriptor(0);
        let before = buf_state_get_refcount(d.state.load(Ordering::Relaxed));

        with_private_refcount(|| async {
            let start_thread = std::thread::current().id();
            p.pin_buffer(0); // first private pin -> shared++ (now `before + 1`)
            assert_eq!(
                buf_state_get_refcount(d.state.load(Ordering::Relaxed)),
                before + 1
            );

            // Hop threads: a blocking task forces the runtime to resume us on a
            // different worker. Loop yield_now as a backup until the id changes.
            tokio::task::yield_now().await;
            for _ in 0..10_000 {
                if std::thread::current().id() != start_thread {
                    break;
                }
                let _ = tokio::task::spawn_blocking(|| {}).await;
                tokio::task::yield_now().await;
            }

            // Second same-task pin: private count rises, shared stays put even
            // though we may now be on a different thread. With a thread_local
            // this branch would see private_refcount == 0 and wrongly shared++.
            assert!(private_refcount(d.buffer()) >= 1, "pin tracked across hop");
            p.pin_buffer(0);
            assert_eq!(
                buf_state_get_refcount(d.state.load(Ordering::Relaxed)),
                before + 1,
                "second same-task pin must not bump the shared refcount"
            );

            p.unpin_buffer(0);
            p.unpin_buffer(0); // last private unpin -> shared--
            assert_eq!(private_refcount(d.buffer()), 0, "no private pin leaked");
        })
        .await;

        assert_eq!(
            buf_state_get_refcount(d.state.load(Ordering::Relaxed)),
            before,
            "shared refcount returns to its prior value: no leak, no under-count"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_racers_exactly_one_does_io() {
        let p = pool(1);
        // Buffer is not valid and not dirty -> a read IO is needed.
        let p1 = p.clone();
        let p2 = p.clone();

        // Task 1 claims the IO and holds it briefly.
        let t1 = tokio::spawn(async move {
            let did = p1.start_buffer_io(0, true).await;
            if did {
                tokio::time::sleep(Duration::from_millis(50)).await;
                // Mark valid + finish.
                p1.terminate_buffer_io(0, false, BufFlags::VALID.bits());
            }
            did
        });

        // Give task 1 a head start so it claims first.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Task 2 races: it should NOT get the IO; it awaits then returns false.
        let t2 = tokio::spawn(async move { p2.start_buffer_io(0, true).await });

        let r1 = t1.await.unwrap();
        let r2 = t2.await.unwrap();
        assert!(r1 ^ r2 || (r1 && !r2), "exactly one performs the IO");
        assert!(r1, "the head-start task did the IO");
        assert!(!r2, "the racer saw it completed and returned false");
        // After terminate, the buffer is valid.
        assert!(p.descriptor(0).state.load(Ordering::Acquire) & BufFlags::VALID.bits() != 0);
        // No IO in progress, no lingering waiters.
        assert!(p.descriptor(0).io_cv.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminate_wakes_waiter() {
        let p = pool(1);
        // Claim IO synchronously.
        assert!(p.start_buffer_io(0, true).await);

        let pw = p.clone();
        let waiter = tokio::spawn(async move {
            pw.wait_io(0).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "waiter blocks while IO in progress");

        p.terminate_buffer_io(0, false, BufFlags::VALID.bits());
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter wakes after terminate")
            .unwrap();
        assert!(p.descriptor(0).io_cv.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_wait_io_future_dequeues() {
        let p = pool(1);
        assert!(p.start_buffer_io(0, true).await);

        // Start waiting, then cancel mid-wait by dropping the future.
        {
            let fut = p.wait_io(0);
            tokio::pin!(fut);
            // Poll once so it enqueues, then drop.
            let _ = tokio::time::timeout(Duration::from_millis(20), &mut fut).await;
        } // fut dropped here -> WaitGuard dequeues
        assert!(
            p.descriptor(0).io_cv.is_empty(),
            "cancelled wait dequeues itself"
        );
        // Cleanup.
        p.terminate_buffer_io(0, false, BufFlags::VALID.bits());
    }
}

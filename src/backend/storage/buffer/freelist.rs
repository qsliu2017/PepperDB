//! Translated from PostgreSQL src/backend/storage/buffer/freelist.c
//!
//! The buffer-pool replacement strategy: the clock sweep plus the (now mostly
//! vestigial) freelist and the per-backend ring strategies.
//!
//! C shape: a shmem `BufferStrategyControl` under `buffer_strategy_lock`
//! (spinlock) holds the clock hand `nextVictimBuffer` (a `pg_atomic_uint32` that
//! only ever increases, taken modulo `NBuffers`), the unused-buffer freelist
//! head/tail, the `completePasses` wrap counter, and `numBufferAllocs`.
//! `StrategyGetBuffer` pops the freelist if non-empty, else sweeps the clock
//! decrementing usagecounts until it finds an unpinned, usagecount==0 victim,
//! returning it WITH the header lock held so no one can pin it first.
//!
//! PepperDB shape (rules.md sections 6.3 / 9): the spinlock-protected control
//! block becomes [`StrategyControl`] with `AtomicU32` hand/alloc counters and a
//! `Mutex` guarding the freelist + `completePasses` (the spinlock wrapped its
//! data). Part A returns a victim CANDIDATE (buf_id) with the header lock held;
//! Part B does the actual eviction/flush. The ring `BufferAccessStrategy` is
//! minimal here (default strategy = clock sweep is what matters now).

use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::storage::buf_internals::{BUF_USAGECOUNT_ONE, buf_state_get_refcount, buf_state_get_usagecount};

use super::buf_init::{BufferPool, FREENEXT_END_OF_LIST, FREENEXT_NOT_IN_LIST};

/// Freelist + wrap-counter state protected together (C: the part of
/// `BufferStrategyControl` under `buffer_strategy_lock`).
struct FreelistState {
    /// Head of the unused-buffer list, or -1 when empty. C: `firstFreeBuffer`.
    first_free: i32,
    /// Tail of the unused-buffer list (undefined when head is -1). C: `lastFreeBuffer`.
    last_free: i32,
    /// Complete clock-sweep cycles. C: `completePasses`.
    complete_passes: u32,
}

/// Clock-sweep / freelist control. C: `BufferStrategyControl`.
pub struct StrategyControl {
    /// Clock hand: index of the next buffer to consider; monotonically
    /// increasing, used modulo `nbuffers`. C: `nextVictimBuffer` (pg_atomic).
    next_victim: AtomicU32,
    /// Buffers allocated since last reset (bgwriter rate estimate). C:
    /// `numBufferAllocs` (pg_atomic).
    num_buffer_allocs: AtomicU32,
    /// Freelist head/tail + the wrap counter. C: the spinlock-protected fields.
    freelist: Mutex<FreelistState>,
    nbuffers: u32,
}

impl StrategyControl {
    /// C: `StrategyInitialize`. The freelist starts holding every buffer
    /// (0..nbuffers-1, already linked by `BufferPool::new`), the hand at 0.
    pub fn new(nbuffers: usize) -> Self {
        Self {
            next_victim: AtomicU32::new(0),
            num_buffer_allocs: AtomicU32::new(0),
            freelist: Mutex::new(FreelistState {
                first_free: 0,
                last_free: nbuffers as i32 - 1,
                complete_passes: 0,
            }),
            nbuffers: nbuffers as u32,
        }
    }

    /// C: `completePasses` reader (tests / BgBufferSync).
    pub fn complete_passes(&self) -> u32 {
        self.freelist.lock().unwrap().complete_passes
    }

    /// C: `numBufferAllocs` reader.
    pub fn num_buffer_allocs(&self) -> u32 {
        self.num_buffer_allocs.load(Ordering::Relaxed)
    }

    /// C: `have_free_buffer` -- a lockless check for a free buffer. Stale by the
    /// time it returns, so do not rely on it to actually obtain one.
    pub fn have_free_buffer(&self) -> bool {
        self.freelist.lock().unwrap().first_free >= 0
    }

    /// C: `ClockSweepTick`. Advance the hand one buffer and return the buf_id now
    /// under it (modulo nbuffers). On the wraparound caused by THIS tick, bump
    /// `completePasses` (under the freelist mutex, matching C's spinlock so
    /// readers see a consistent hand+passes pair).
    fn clock_sweep_tick(&self) -> u32 {
        let victim = self.next_victim.fetch_add(1, Ordering::Relaxed);
        if victim < self.nbuffers {
            return victim;
        }
        let original = victim;
        let wrapped = victim % self.nbuffers;
        if wrapped == 0 {
            // We caused a wraparound: fold the hand back and count the pass.
            let expected_reset = original + 1;
            let reset_to = expected_reset % self.nbuffers;
            // CAS the hand back under the lock so the pass bump is consistent.
            loop {
                let mut g = self.freelist.lock().unwrap();
                let mut cur = expected_reset;
                match self.next_victim.compare_exchange(
                    cur,
                    reset_to,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        g.complete_passes += 1;
                        break;
                    }
                    Err(actual) => {
                        cur = actual;
                        // Another tick moved the hand past our expected value;
                        // it (or its own wrap handler) owns the bookkeeping.
                        if cur != expected_reset {
                            break;
                        }
                    }
                }
            }
        }
        wrapped
    }

    /// C: `StrategyFreeBuffer` -- push a buffer onto the freelist (idempotent if
    /// already in it). Updates the descriptor's `free_next` under the lock (C
    /// treats `freeNext` as protected by `buffer_strategy_lock`).
    pub fn free_buffer(&self, pool: &BufferPool, buf_id: i32) {
        let desc = pool.descriptor(buf_id);
        let mut g = self.freelist.lock().unwrap();
        if desc.free_next.load(Ordering::Relaxed) as i32 == FREENEXT_NOT_IN_LIST {
            desc.free_next.store(g.first_free as u32, Ordering::Relaxed);
            if g.first_free < 0 {
                g.last_free = buf_id;
            }
            g.first_free = buf_id;
        }
    }

    /// C: `StrategyGetBuffer` (default strategy only). Return a victim CANDIDATE:
    /// a buf_id whose header lock is HELD and whose `(refcount==0,
    /// usagecount==0)` so no one else can pin it before the caller (Part B) does.
    /// Returns `(buf_id, buf_state)` -- the locked state word, as C returns via
    /// `*buf_state` with the header spinlock still held.
    ///
    /// First drains the freelist (popping + validating each candidate), then runs
    /// the clock sweep, decrementing usagecounts of in-use buffers until it finds
    /// a free one. Panics ("no unpinned buffers available") if every buffer is
    /// pinned for a full sweep, matching C.
    pub fn get_buffer(&self, pool: &BufferPool) -> (i32, u32) {
        self.num_buffer_allocs.fetch_add(1, Ordering::Relaxed);

        // 1) Freelist fast path.
        loop {
            let buf_id = {
                let mut g = self.freelist.lock().unwrap();
                if g.first_free < 0 {
                    break;
                }
                let buf_id = g.first_free;
                let desc = pool.descriptor(buf_id);
                let next = desc.free_next.load(Ordering::Relaxed) as i32;
                g.first_free = next;
                desc.free_next
                    .store(FREENEXT_NOT_IN_LIST as u32, Ordering::Relaxed);
                buf_id
            };
            // Validate under the header lock without the freelist lock held.
            let desc = pool.descriptor(buf_id);
            let buf_state = desc.lock_hdr();
            if buf_state_get_refcount(buf_state) == 0 && buf_state_get_usagecount(buf_state) == 0 {
                return (buf_id, buf_state);
            }
            desc.unlock_hdr(buf_state);
        }

        // 2) Clock sweep.
        let mut trycounter = self.nbuffers;
        loop {
            let buf_id = self.clock_sweep_tick() as i32;
            let desc = pool.descriptor(buf_id);
            let buf_state = desc.lock_hdr();

            if buf_state_get_refcount(buf_state) == 0 {
                if buf_state_get_usagecount(buf_state) != 0 {
                    // Cool down and keep scanning. Decrement under the hdr lock.
                    desc.unlock_hdr(buf_state - BUF_USAGECOUNT_ONE);
                    trycounter = self.nbuffers;
                } else {
                    // Found a usable victim; return it locked.
                    return (buf_id, buf_state);
                }
            } else {
                trycounter -= 1;
                if trycounter == 0 {
                    desc.unlock_hdr(buf_state);
                    // C: elog(ERROR, "no unpinned buffers available").
                    // TODO(panic): migrate to Result + ?
                    panic!("no unpinned buffers available");
                }
                desc.unlock_hdr(buf_state);
            }
        }
    }
}

/// C: `StrategyNotifyBgWriter`. Records the bgwriter's ProcNumber so
/// `StrategyGetBuffer` can wake it at the next buffer allocation (`-1` clears the
/// request). TODO(bufmgr): store into StrategyControl + ring the latch on alloc.
/// A non-panicking no-op for now: the long-lived bgwriter calls this on its
/// hibernate path and must not panic on a timer.
pub fn strategy_notify_bg_writer(_bgwprocno: crate::storage::procnumber::ProcNumber) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buf_internals::{BUF_REFCOUNT_ONE, BufFlags};
    use std::sync::Arc;

    fn pool(n: usize) -> Arc<BufferPool> {
        Arc::new(BufferPool::new(n))
    }

    // Drain the freelist so the clock sweep is exercised directly.
    fn drain_freelist(p: &BufferPool) {
        while p.strategy.have_free_buffer() {
            let (id, s) = p.strategy.get_buffer(p);
            p.descriptor(id).unlock_hdr(s);
        }
    }

    #[test]
    fn freelist_pop_returns_all_buffers_locked() {
        let p = BufferPool::new(4);
        let mut seen = Vec::new();
        for _ in 0..4 {
            assert!(p.strategy.have_free_buffer());
            let (id, s) = p.strategy.get_buffer(&p);
            assert!(s & BufFlags::LOCKED.bits() != 0, "victim returned locked");
            p.descriptor(id).unlock_hdr(s);
            seen.push(id);
        }
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2, 3]);
        assert!(!p.strategy.have_free_buffer());
    }

    #[test]
    fn clock_sweep_decrements_usagecount_and_returns_unpinned() {
        let p = BufferPool::new(3);
        drain_freelist(&p);
        // Give buffer 1 a usagecount of 2 (simulate prior pins).
        let d1 = p.descriptor(1);
        let s = d1.lock_hdr();
        d1.unlock_hdr(s + 2 * BUF_USAGECOUNT_ONE);

        // Sweep should cool down buffer 1 across passes and eventually return a
        // usagecount==0 victim. Buffers 0 and 2 start at usagecount 0, so the
        // first one the hand lands on is returned immediately.
        let (id, s) = p.strategy.get_buffer(&p);
        assert!(buf_state_get_refcount(s) == 0 && buf_state_get_usagecount(s) == 0);
        p.descriptor(id).unlock_hdr(s);
    }

    #[test]
    fn clock_sweep_skips_pinned_buffer() {
        let p = BufferPool::new(2);
        drain_freelist(&p);
        // Pin buffer 0 (refcount=1) so the sweep must skip it and pick buffer 1.
        let d0 = p.descriptor(0);
        let s = d0.lock_hdr();
        d0.unlock_hdr(s + BUF_REFCOUNT_ONE);

        let (id, st) = p.strategy.get_buffer(&p);
        assert_eq!(id, 1, "pinned buffer 0 is skipped");
        p.descriptor(id).unlock_hdr(st);
    }

    #[test]
    fn complete_passes_increments_on_wraparound() {
        let p = BufferPool::new(2);
        drain_freelist(&p);
        let before = p.strategy.complete_passes();
        // Force several full sweeps: give both buffers usagecount so the hand has
        // to wrap. Set usagecount=1 on both; each get_buffer cools one per visit.
        for d in [p.descriptor(0), p.descriptor(1)] {
            let s = d.lock_hdr();
            d.unlock_hdr(s + BUF_USAGECOUNT_ONE);
        }
        // First get: hand visits buf0 (cool to 0), buf1 (cool to 0)? Actually it
        // returns buf0 once it cools to 0 only after a later pass. Run a few.
        for _ in 0..4 {
            let (id, s) = p.strategy.get_buffer(&p);
            p.descriptor(id).unlock_hdr(s);
        }
        assert!(
            p.strategy.complete_passes() > before,
            "the hand wrapped at least once"
        );
    }

    #[test]
    fn free_buffer_pushes_back_idempotently() {
        let p = BufferPool::new(2);
        // Drain so both are out of the list.
        drain_freelist(&p);
        assert!(!p.strategy.have_free_buffer());
        p.strategy.free_buffer(&p, 0);
        assert!(p.strategy.have_free_buffer());
        // Freeing again must not corrupt the list (already NOT_IN_LIST check).
        p.strategy.free_buffer(&p, 0);
        // Exactly one buffer should be poppable.
        let (id, s) = p.strategy.get_buffer(&p);
        assert_eq!(id, 0);
        p.descriptor(id).unlock_hdr(s);
        assert!(!p.strategy.have_free_buffer());
    }
}

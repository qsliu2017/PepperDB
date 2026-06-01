//-------------------------------------------------------------------------
//
// memdebug.rs
//    Declarations used in memory context implementations, not part of the
//    public API of the memory management subsystem.
//
// Ported 1:1 from postgres/src/backend/utils/mmgr/memdebug.c (+ the relevant
// decls of src/include/utils/memdebug.h).
//
// USE_VALGRIND is OFF in this build, so all the VALGRIND_* client-request
// macros are no-ops and are not emitted here. The Valgrind-guarded helper
// fns in memdebug.h (none have non-Valgrind bodies of their own beyond the
// no-op wrappers) and the wipe_mem/set_sentinel/sentinel_ok inlines are
// guarded by CLOBBER_FREED_MEMORY / MEMORY_CONTEXT_CHECKING and are not part
// of this file's mandate.
//
// The only REAL code compiled when USE_VALGRIND is off is `randomize_mem`,
// which in C is guarded by RANDOMIZE_ALLOCATED_MEMORY (defined in assert/
// debug builds). We port it UNCONDITIONALLY here (always available; dead-code
// is fine for this crate) so it is usable regardless of cfg.
//
//-------------------------------------------------------------------------

use crate::prelude::*;
use std::sync::atomic::{AtomicI32, Ordering};

// C: static int save_ctr = 1; (persists across calls, function-local static).
// We model it with a process-wide atomic to preserve the cross-call carry of
// the counter exactly as the C does.
static SAVE_CTR: AtomicI32 = AtomicI32::new(1);

// Fill a just-allocated piece of memory with "random" data. It's not really
// very random, just a repeating sequence with a length that's prime. What we
// mainly want out of it is to have a good probability that two palloc's of the
// same number of bytes start out containing different data.
//
// C body:
//   static int save_ctr = 1;
//   size_t remaining = size;
//   int ctr;
//   ctr = save_ctr;
//   while (remaining-- > 0) {
//       *ptr++ = ctr;
//       if (++ctr > 251) ctr = 1;
//   }
//   save_ctr = ctr;
//
// The VALGRIND_MAKE_MEM_UNDEFINED calls are no-ops here and omitted.
pub fn randomize_mem(ptr: *mut c_char, size: usize) {
    let mut remaining = size;
    let mut ctr: i32 = SAVE_CTR.load(Ordering::Relaxed);

    let mut p = ptr;
    while remaining > 0 {
        remaining -= 1;
        // *ptr++ = ctr;  (truncating int -> char, matching C assignment)
        unsafe {
            *p = ctr as c_char;
            p = p.add(1);
        }
        // if (++ctr > 251) ctr = 1;
        ctr += 1;
        if ctr > 251 {
            ctr = 1;
        }
    }

    SAVE_CTR.store(ctr, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    // randomize_mem mutates the process-global SAVE_CTR; cargo runs tests in
    // parallel threads, so each test must hold this lock across its
    // store-then-assert window or another test's call clobbers the counter.
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    // With save_ctr starting at 1, the first bytes written are exactly the
    // counter values: 1, 2, 3, 4, 5, ... (each written, then incremented,
    // wrapping to 1 only after exceeding 251). For a small buffer these are
    // simply 1..=N. We reset SAVE_CTR explicitly so the test is deterministic
    // regardless of other callers/test ordering.
    #[test]
    fn writes_expected_byte_sequence_from_one() {
        let _g = TEST_LOCK.lock().unwrap();
        SAVE_CTR.store(1, Ordering::Relaxed);
        let mut buf: [c_char; 6] = [0; 6];
        randomize_mem(buf.as_mut_ptr(), buf.len());
        // ctr: write 1 -> 2, write 2 -> 3, ... write 6 -> 7
        let expected: [c_char; 6] = [1, 2, 3, 4, 5, 6];
        assert_eq!(buf, expected);
        // save_ctr should now be 7 for the next call.
        assert_eq!(SAVE_CTR.load(Ordering::Relaxed), 7);
    }

    // Across two calls the counter must carry over (no reset between calls).
    #[test]
    fn counter_carries_across_calls() {
        let _g = TEST_LOCK.lock().unwrap();
        SAVE_CTR.store(1, Ordering::Relaxed);
        let mut a: [c_char; 3] = [0; 3];
        randomize_mem(a.as_mut_ptr(), a.len()); // writes 1,2,3 ; save_ctr=4
        let mut b: [c_char; 3] = [0; 3];
        randomize_mem(b.as_mut_ptr(), b.len()); // writes 4,5,6 ; save_ctr=7
        assert_eq!(a, [1, 2, 3]);
        assert_eq!(b, [4, 5, 6]);
        assert_eq!(SAVE_CTR.load(Ordering::Relaxed), 7);
    }

    // Verify the wrap: at 251 it writes 251 then resets to 1 (skipping 252+).
    #[test]
    fn wraps_after_251() {
        let _g = TEST_LOCK.lock().unwrap();
        // Start the counter at 250 so we cross the wrap boundary.
        SAVE_CTR.store(250, Ordering::Relaxed);
        let mut buf: [c_char; 4] = [0; 4];
        randomize_mem(buf.as_mut_ptr(), buf.len());
        // write 250 ->251, write 251 ->(252>251 -> 1), write 1 ->2, write 2 ->3
        // 250 as i8 == -6, 251 as i8 == -5
        let expected: [c_char; 4] = [250i32 as c_char, 251i32 as c_char, 1, 2];
        assert_eq!(buf, expected);
        assert_eq!(SAVE_CTR.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn zero_size_is_noop() {
        let _g = TEST_LOCK.lock().unwrap();
        SAVE_CTR.store(42, Ordering::Relaxed);
        let mut buf: [c_char; 2] = [9, 9];
        randomize_mem(buf.as_mut_ptr(), 0);
        assert_eq!(buf, [9, 9]);
        assert_eq!(SAVE_CTR.load(Ordering::Relaxed), 42);
    }
}

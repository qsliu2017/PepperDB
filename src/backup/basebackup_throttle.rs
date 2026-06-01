//! Basebackup sink implementing throttling. Data is forwarded to the next base
//! backup sink in the chain at a rate no greater than the configured maximum.
//!
//! Source: postgres/src/backend/backup/basebackup_throttle.c
//!
//! #include mapping:
//!   "postgres.h"                -> use crate::prelude::*
//!   "backup/basebackup_sink.h"  -> crate::backup::basebackup_sink (PORTED)
//!   "miscadmin.h"               -> CHECK_FOR_INTERRUPTS (no-op shim, see below)
//!   "pgstat.h"                  -> WAIT_EVENT_BASE_BACKUP_THROTTLE wait-event id (STUB)
//!   "storage/latch.h"           -> WaitLatch/ResetLatch/MyLatch/WL_* (STUBBED, see below)
//!   "utils/timestamp.h"         -> TimestampTz/TimeOffset, GetCurrentTimestamp,
//!                                  USECS_PER_SEC (TimestampTz/TimeOffset = int64 locally;
//!                                  GetCurrentTimestamp STUBBED)

use crate::prelude::*;

use crate::backup::basebackup_sink::{
    bbsink, bbsink_forward_archive_contents, bbsink_forward_begin_archive,
    bbsink_forward_begin_backup, bbsink_forward_begin_manifest, bbsink_forward_cleanup,
    bbsink_forward_end_archive, bbsink_forward_end_backup, bbsink_forward_end_manifest,
    bbsink_forward_manifest_contents, bbsink_ops,
};

// ---------------------------------------------------------------------------
// Stubs for as-yet-unported dependencies.
// ---------------------------------------------------------------------------

// utils/timestamp.h: a TimestampTz is microseconds since the Postgres epoch; a
// TimeOffset is a signed microsecond difference. Both are int64. Declared
// locally (mirroring the access/rmgrdesc/*.rs convention) until a canonical
// timestamp.rs exists.
type TimestampTz = int64;
type TimeOffset = int64;

// USECS_PER_SEC from datatype/timestamp.h.
const USECS_PER_SEC: int64 = 1_000_000;

// utils/timestamp.h: GetCurrentTimestamp returns the current wall-clock time as
// a TimestampTz. STUB: returns 0 so the byte-accounting arithmetic stays exact
// and deterministic. With a constant clock the throttle never observes elapsed
// time, so `sleep` stays positive and the WaitLatch loop drives the wait; both
// of those are themselves stubbed below. The real clock is TODO.
// TODO: port utils/adt/timestamp.c GetCurrentTimestamp (INSTR_TIME / gettimeofday).
unsafe fn GetCurrentTimestamp() -> TimestampTz {
    0
}

// storage/latch.h wait-event flags. Real bit values from the C header; only
// WL_LATCH_SET / WL_TIMEOUT are inspected by the loop below.
const WL_LATCH_SET: c_int = 1 << 0;
const WL_TIMEOUT: c_int = 1 << 4;
const WL_EXIT_ON_PM_DEATH: c_int = 1 << 5;

// storage/latch.h: MyLatch is the process latch. STUB: an opaque non-null
// pointer placeholder; the stubbed WaitLatch/ResetLatch ignore it.
// TODO: port storage/ipc/latch.c (the real per-process Latch).
type Latch = c_void;
unsafe fn MyLatch() -> *mut Latch {
    // A dangling-but-nonzero sentinel; never dereferenced by the stubs.
    core::ptr::NonNull::<Latch>::dangling().as_ptr()
}

// storage/latch.h: ResetLatch clears a latch's set flag. STUB: no-op.
// TODO: port storage/ipc/latch.c ResetLatch.
unsafe fn ResetLatch(_latch: *mut Latch) {}

// storage/latch.h: WaitLatch blocks until the latch is set, the timeout
// elapses, or postmaster death. STUB: returns WL_TIMEOUT so the throttle loop
// treats every wait as having slept long enough and terminates. Without a real
// clock + latch this is the only way to avoid an infinite spin.
// TODO: port storage/ipc/latch.c WaitLatch (sleeps wait_ms; returns the set of
// triggered WL_* conditions).
unsafe fn WaitLatch(
    _latch: *mut Latch,
    _wakeEvents: c_int,
    _timeout_ms: c_long,
    _wait_event_info: uint32,
) -> c_int {
    WL_TIMEOUT
}

// pgstat.h wait-event id (enum WaitEventTimeout). STUB: an arbitrary nonzero
// placeholder; only passed through to the stubbed WaitLatch.
// TODO: port the pgstat wait-event enums.
const WAIT_EVENT_BASE_BACKUP_THROTTLE: uint32 = 0x0B00_0001;

// miscadmin.h: CHECK_FOR_INTERRUPTS processes any pending query cancel /
// die interrupt. STUB: no-op (matches the shim used by other ported units).
// TODO: port miscadmin.h interrupt handling.
#[inline]
fn CHECK_FOR_INTERRUPTS() {}

// ---------------------------------------------------------------------------
// bbsink_throttle: a throttling sink decorator. Embeds `base: bbsink` as its
// first field so that a *mut bbsink_throttle and a *mut bbsink are
// interconvertible by pointer cast (the C downcast pattern).
// ---------------------------------------------------------------------------
#[repr(C)]
pub struct bbsink_throttle {
    /// Common information for all types of sink.
    pub base: bbsink,

    /// The actual number of bytes, transfer of which may cause sleep.
    pub throttling_sample: uint64,

    /// Amount of data already transferred but not yet throttled.
    pub throttling_counter: int64,

    /// The minimum time required to transfer throttling_sample bytes.
    pub elapsed_min_unit: TimeOffset,

    /// The last check of the transfer rate.
    pub throttled_last: TimestampTz,
}

// ---------------------------------------------------------------------------
// Ops table: begin_backup/archive_contents/manifest_contents are overridden by
// this file; everything else forwards to the successor sink.
// ---------------------------------------------------------------------------
static bbsink_throttle_ops: bbsink_ops = bbsink_ops {
    begin_backup: Some(bbsink_throttle_begin_backup),
    begin_archive: Some(bbsink_forward_begin_archive),
    archive_contents: Some(bbsink_throttle_archive_contents),
    end_archive: Some(bbsink_forward_end_archive),
    begin_manifest: Some(bbsink_forward_begin_manifest),
    manifest_contents: Some(bbsink_throttle_manifest_contents),
    end_manifest: Some(bbsink_forward_end_manifest),
    end_backup: Some(bbsink_forward_end_backup),
    cleanup: Some(bbsink_forward_cleanup),
};

/// How frequently to throttle, as a fraction of the specified rate-second.
const THROTTLING_FREQUENCY: int64 = 8;

/// Create a new basebackup sink that performs throttling and forwards data to a
/// successor sink.
pub unsafe fn bbsink_throttle_new(next: *mut bbsink, maxrate: uint32) -> *mut bbsink {
    Assert!(!next.is_null());
    Assert!(maxrate > 0);

    let sink = palloc0(core::mem::size_of::<bbsink_throttle>()) as *mut bbsink_throttle;
    (*sink).base.bbs_ops = &bbsink_throttle_ops;
    (*sink).base.bbs_next = next;

    (*sink).throttling_sample =
        ((maxrate as int64) * 1024i64 / THROTTLING_FREQUENCY) as uint64;

    // The minimum amount of time for throttling_sample bytes to be transferred.
    (*sink).elapsed_min_unit = USECS_PER_SEC / THROTTLING_FREQUENCY;

    &mut (*sink).base
}

/// There's no real work to do here, but we need to record the current time so
/// that it can be used for future calculations.
unsafe fn bbsink_throttle_begin_backup(sink: *mut bbsink) {
    let mysink = sink as *mut bbsink_throttle;

    bbsink_forward_begin_backup(sink);

    // The 'real data' starts now (header was ignored).
    (*mysink).throttled_last = GetCurrentTimestamp();
}

/// First throttle, and then pass archive contents to next sink.
unsafe fn bbsink_throttle_archive_contents(sink: *mut bbsink, len: Size) {
    throttle(sink as *mut bbsink_throttle, len);

    bbsink_forward_archive_contents(sink, len);
}

/// First throttle, and then pass manifest contents to next sink.
unsafe fn bbsink_throttle_manifest_contents(sink: *mut bbsink, len: Size) {
    throttle(sink as *mut bbsink_throttle, len);

    bbsink_forward_manifest_contents(sink, len);
}

/// Increment the network transfer counter by the given number of bytes, and
/// sleep if necessary to comply with the requested network transfer rate.
unsafe fn throttle(sink: *mut bbsink_throttle, increment: Size) {
    Assert!((*sink).throttling_counter >= 0);

    (*sink).throttling_counter += increment as int64;
    if ((*sink).throttling_counter as uint64) < (*sink).throttling_sample {
        return;
    }

    // How much time should have elapsed at minimum?
    let elapsed_min: TimeOffset = (*sink).elapsed_min_unit
        * ((*sink).throttling_counter / (*sink).throttling_sample as int64);

    // Since the latch could be set repeatedly because of concurrently WAL
    // activity, sleep in a loop to ensure enough time has passed.
    loop {
        // Time elapsed since the last measurement (and possible wake up).
        let elapsed: TimeOffset = GetCurrentTimestamp() - (*sink).throttled_last;

        // sleep if the transfer is faster than it should be
        let sleep: TimeOffset = elapsed_min - elapsed;
        if sleep <= 0 {
            break;
        }

        ResetLatch(MyLatch());

        // We're eating a potentially set latch, so check for interrupts
        CHECK_FOR_INTERRUPTS();

        // (TAR_SEND_SIZE / throttling_sample * elapsed_min_unit) should be the
        // maximum time to sleep. Thus the cast to long is safe.
        let wait_result = WaitLatch(
            MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            (sleep / 1000) as c_long,
            WAIT_EVENT_BASE_BACKUP_THROTTLE,
        );

        if (wait_result & WL_LATCH_SET) != 0 {
            CHECK_FOR_INTERRUPTS();
        }

        // Done waiting?
        if (wait_result & WL_TIMEOUT) != 0 {
            break;
        }
    }

    // As we work with integers, only whole multiple of throttling_sample was
    // processed. The rest will be done during the next call of this function.
    (*sink).throttling_counter %= (*sink).throttling_sample as int64;

    // Time interval for the remaining amount and possible next increments
    // starts now.
    (*sink).throttled_last = GetCurrentTimestamp();
}

#[cfg(test)]
mod tests {
    use super::*;

    // bbsink_throttle_new must compute throttling_sample = maxrate * 1024 /
    // THROTTLING_FREQUENCY and elapsed_min_unit = USECS_PER_SEC /
    // THROTTLING_FREQUENCY. We only need a non-null successor pointer; the
    // constructor never dereferences it.
    #[test]
    fn new_computes_throttling_fields() {
        unsafe {
            // A dummy non-null successor (never dereferenced by the ctor).
            let next = palloc0(core::mem::size_of::<bbsink>()) as *mut bbsink;

            // 1 MB/s -> sample = 1024 * 1024 / 8 = 131072 bytes.
            let maxrate: uint32 = 1024;
            let base = bbsink_throttle_new(next, maxrate);
            let sink = base as *mut bbsink_throttle;

            assert_eq!(
                (*sink).throttling_sample,
                (maxrate as u64) * 1024 / 8
            );
            assert_eq!((*sink).throttling_sample, 131072);
            assert_eq!((*sink).elapsed_min_unit, USECS_PER_SEC / 8);
            assert_eq!((*sink).elapsed_min_unit, 125000);
            assert_eq!((*sink).throttling_counter, 0);
            assert_eq!((*sink).base.bbs_next, next);

            pfree(sink as *mut c_void);
            pfree(next as *mut c_void);
        }
    }

    // The core byte-accounting invariant of throttle(): below one sample the
    // counter just accumulates; on crossing a sample boundary it is reduced
    // modulo throttling_sample. With the stubbed (constant-0) clock the sleep
    // loop is exercised and the stubbed WaitLatch returns WL_TIMEOUT, so the
    // loop terminates and the modulo bookkeeping runs.
    #[test]
    fn throttle_byte_accounting() {
        unsafe {
            let next = palloc0(core::mem::size_of::<bbsink>()) as *mut bbsink;
            let base = bbsink_throttle_new(next, 1024); // sample = 131072
            let sink = base as *mut bbsink_throttle;
            let sample = (*sink).throttling_sample as int64; // 131072

            // Below a full sample: just accumulates, no reset.
            throttle(sink, 1000);
            assert_eq!((*sink).throttling_counter, 1000);

            // Push past 2 full samples plus a remainder of 500.
            let push = (2 * sample + 500 - 1000) as Size;
            throttle(sink, push);
            // counter %= sample leaves the remainder.
            assert_eq!((*sink).throttling_counter, 500);

            pfree(sink as *mut c_void);
            pfree(next as *mut c_void);
        }
    }
}

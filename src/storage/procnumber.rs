//! storage/procnumber.h - definition of process number

use crate::c::uint32;
use std::ffi::c_int;

/*
 * ProcNumber uniquely identifies an active backend or auxiliary process.
 * It's assigned at backend startup after authentication, when the process
 * adds itself to the proc array.  It is an index into the proc array,
 * starting from 0. Note that a ProcNumber can be reused for a different
 * backend immediately after a backend exits.
 */
pub type ProcNumber = c_int;

pub const INVALID_PROC_NUMBER: ProcNumber = -1;

/*
 * Note: MAX_BACKENDS_BITS is 18 as that is the space available for buffer
 * refcounts in buf_internals.h.  This limitation could be lifted by using a
 * 64bit state; but it's unlikely to be worthwhile as 2^18-1 backends exceed
 * currently realistic configurations. Even if that limitation were removed,
 * we still could not a) exceed 2^23-1 because inval.c stores the ProcNumber
 * as a 3-byte signed integer, b) INT_MAX/4 because some places compute
 * 4*MaxBackends without any overflow check.  We check that the configured
 * number of backends does not exceed MAX_BACKENDS in InitializeMaxBackends().
 */
pub const MAX_BACKENDS_BITS: c_int = 18;
pub const MAX_BACKENDS: uint32 = (1u32 << MAX_BACKENDS_BITS) - 1;

/*
 * Proc number of this backend (same as GetNumberFromPGProc(MyProc))
 */
// extern PGDLLIMPORT ProcNumber MyProcNumber;
extern "C" { pub static mut MyProcNumber: ProcNumber; }
/* proc number of our parallel session leader, or INVALID_PROC_NUMBER if none */
// extern PGDLLIMPORT ProcNumber ParallelLeaderProcNumber;
pub static mut ParallelLeaderProcNumber: ProcNumber = INVALID_PROC_NUMBER;

/*
 * The ProcNumber to use for our session's temp relations is normally our own,
 * but parallel workers should use their leader's proc number.
 */
#[inline]
pub unsafe fn ProcNumberForTempRelations() -> ProcNumber {
    if ParallelLeaderProcNumber == INVALID_PROC_NUMBER {
        MyProcNumber
    } else {
        ParallelLeaderProcNumber
    }
}

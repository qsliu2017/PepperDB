//! Translated from PostgreSQL src/include/storage/procnumber.h

/// Uniquely identifies an active backend/auxiliary process; an index into the
/// proc array, starting from 0.
pub type ProcNumber = i32;

pub const INVALID_PROC_NUMBER: ProcNumber = -1;

pub const MAX_BACKENDS_BITS: u32 = 18;
pub const MAX_BACKENDS: u32 = (1u32 << MAX_BACKENDS_BITS) - 1;

/// Proc number of this backend.
pub static mut MY_PROC_NUMBER: ProcNumber = INVALID_PROC_NUMBER;

/// Proc number of our parallel session leader, or INVALID_PROC_NUMBER if none.
pub static mut PARALLEL_LEADER_PROC_NUMBER: ProcNumber = INVALID_PROC_NUMBER;

/// ProcNumber to use for our session's temp relations.
pub fn proc_number_for_temp_relations() -> ProcNumber {
    unsafe {
        if PARALLEL_LEADER_PROC_NUMBER == INVALID_PROC_NUMBER {
            MY_PROC_NUMBER
        } else {
            PARALLEL_LEADER_PROC_NUMBER
        }
    }
}

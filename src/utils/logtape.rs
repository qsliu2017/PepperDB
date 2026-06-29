//! Header for PostgreSQL src/include/utils/logtape.h.
//!
//! The logical-tape abstraction is translated in
//! `crate::backend::utils::sort::logtape` (step 24). This header re-exports the
//! implementation under the C-facing path. `LogicalTapeSet`/`LogicalTape` are now
//! owned structs (the free-block min-heap + per-tape buffer are owned `Vec`s);
//! the BufFile block-I/O leaves stub-call the still-hollow `storage::buffile`.

pub use crate::backend::utils::sort::logtape::{
    logical_tape_create, logical_tape_freeze, logical_tape_read, logical_tape_rewind_for_read,
    logical_tape_set_blocks, logical_tape_set_create, logical_tape_set_forget_free_space,
    logical_tape_write, LogicalTape, LogicalTapeSet, TapeShare,
};

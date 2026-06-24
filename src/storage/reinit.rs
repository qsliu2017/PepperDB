//! Translated from PostgreSQL src/include/storage/reinit.h
//! Reinitialization of unlogged relations.

use bitflags::bitflags;

use crate::common::relpath::{ForkNumber, RelFileNumber};

bitflags! {
    /// `op` argument to ResetUnloggedRelations.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct UnloggedRelationOp: i32 {
        const CLEANUP = 0x0001;
        const INIT    = 0x0002;
    }
}

pub fn ResetUnloggedRelations(_op: UnloggedRelationOp) {
    unimplemented!()
}

/// Parse a relation file name into (relnumber, fork, segno); None if `name` is
/// not a non-temp relation file name.
pub fn parse_filename_for_nontemp_relation(
    _name: &str,
) -> Option<(RelFileNumber, ForkNumber, u32)> {
    unimplemented!()
}

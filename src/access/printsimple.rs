//! Translated from PostgreSQL src/include/access/printsimple.h
//!
//! Print simple tuples without catalog access. These back a `DestReceiver`
//! (see crate::tcop::dest): `printsimple` is the per-tuple receive callback,
//! `printsimple_startup` the per-run startup callback.

use crate::access::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;
use crate::tcop::dest::DestReceiver;

/// Per-tuple callback. Returns true to continue (as DestReceiver::receive_slot).
pub fn printsimple(slot: &mut TupleTableSlot, self_: &mut dyn DestReceiver) -> bool {
    let _ = (slot, self_);
    unimplemented!()
}

/// Startup callback. `operation` is the CmdType code (passed as i32 in C).
pub fn printsimple_startup(self_: &mut dyn DestReceiver, operation: i32, tupdesc: TupleDesc) {
    let _ = (self_, operation, tupdesc);
    unimplemented!()
}

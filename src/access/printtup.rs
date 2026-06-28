//! Translated from PostgreSQL src/include/access/printtup.h
//!
//! The bodies live in `crate::backend::access::common::printtup`; this header
//! re-exports the public API (type-centric: the `DRprinttup` receiver state plus
//! its free-fn constructors) so existing `crate::access::printtup::<name>` call
//! sites keep resolving (rules.md s3).

#![allow(
    clippy::not_unsafe_ptr_arg_deref,
    reason = "SetRemoteDestReceiverParams takes a raw Portal handle per the C API; the deref of a live portal is faithful to C"
)]

use crate::access::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::nodes::CmdType;
use crate::tcop::dest::{CommandDest, DestReceiver};
use crate::utils::portal::Portal;

// DR_printtup is the printtup receiver state (defined in printtup.c). Re-exported
// from the backend body, which holds the concrete struct + DestReceiver impl.
pub use crate::backend::access::common::printtup::DRprinttup;

/// printtup_create_DR: construct a DestReceiver for DestRemote/DestRemoteExecute.
pub use crate::backend::access::common::printtup::printtup_create_DR;

/// SetRemoteDestReceiverParams: bind a printtup receiver to a portal. The C
/// signature takes the `Portal`; M1 only needs the portal's per-column format
/// codes, so the backend body takes `&[i16]` directly. This shim adapts the
/// header signature by reading `portal->formats`.
///
/// SAFETY: `portal` must be a live `PortalData`. On the M1 path the only caller
/// (`exec_simple_query`) holds the owning `PortalData` for the receiver's life.
pub fn SetRemoteDestReceiverParams(self_: &mut dyn DestReceiver, portal: Portal) {
    // SAFETY: live portal per the contract above.
    let formats = unsafe { &(*portal).formats };
    crate::backend::access::common::printtup::set_remote_dest_receiver_params(self_, formats);
}

/// SendRowDescriptionMessage: emit a RowDescription ('T') protocol message. The
/// C `StringInfo buf` and `targetlist` are folded away on the M1 path (the
/// message is appended straight to the send buffer; the targetlist is empty so
/// resorigtbl/resorigcol are zeroes). `formats` empty => text (format 0).
pub fn SendRowDescriptionMessage(typeinfo: &TupleDesc, formats: Option<&[i16]>) {
    crate::backend::access::common::printtup::send_row_description_message(
        typeinfo,
        formats.unwrap_or(&[]),
    );
}

/// debugStartup: rStartup for the DestDebug receiver (standalone backend). Not
/// reachable on the M1 frontend path; grows with the interactive backend.
pub fn debugStartup(_self_: &mut dyn DestReceiver, _operation: CmdType, _typeinfo: TupleDesc) {
    unimplemented!("debugStartup: standalone-backend interactive output deferred")
}

/// debugtup: receiveSlot for the DestDebug receiver. Returns true to continue.
pub fn debugtup(_slot: &mut TupleTableSlot, _self_: &mut dyn DestReceiver) -> bool {
    unimplemented!("debugtup: standalone-backend interactive output deferred")
}

// XXX these are really in executor/spi.c

/// spi_dest_startup: rStartup for the DestSPI receiver. Deferred (SPI).
pub fn spi_dest_startup(_self_: &mut dyn DestReceiver, _operation: CmdType, _typeinfo: TupleDesc) {
    unimplemented!("spi_dest_startup: SPI deferred")
}

/// spi_printtup: receiveSlot for the DestSPI receiver. Deferred (SPI).
pub fn spi_printtup(_slot: &mut TupleTableSlot, _self_: &mut dyn DestReceiver) -> bool {
    unimplemented!("spi_printtup: SPI deferred")
}

// `CommandDest` is referenced by the constructor's signature; keep the import
// meaningful for downstream `use crate::access::printtup::*` consumers.
#[allow(unused_imports)]
use CommandDest as _;

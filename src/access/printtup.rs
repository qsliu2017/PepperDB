//! Translated from PostgreSQL src/include/access/printtup.h

use crate::access::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;
use crate::lib::stringinfo::StringInfo;
use crate::nodes::nodes::{CmdType, Node};
use crate::tcop::dest::{CommandDest, DestReceiver};
use crate::utils::portal::Portal;

// DR_printtup is the printtup receiver state (defined in printtup.c, not the
// header). Modeled as the concrete struct that backs printtup_create_DR and
// implements the DestReceiver trait. Per-attribute print info lives in
// `myinfo`; portal/format state is filled by SetRemoteDestReceiverParams.
pub struct DRprinttup {
    pub portal: Portal, // the Portal we are printing from; TODO(ptr)
    pub sendDescrip: bool, // send RowDescription at startup?
    pub attrinfo: TupleDesc, // tuple descriptor of the tuples; TODO(ptr)
    pub formats: Vec<i16>, // format codes for the columns
    pub myinfo: Vec<PrinttupAttrInfo>, // per-column info
}

// PrinttupAttrInfo: per-column output-function info (defined in printtup.c).
pub struct PrinttupAttrInfo {
    pub format: i16,
    pub typisvarlena: bool,
}

/// printtup_create_DR: construct a DestReceiver for DestRemote/DestRemoteExecute.
// TODO(ptr): runtime-chosen receiver; revisit ownership in Phase 2.
pub fn printtup_create_DR(_dest: CommandDest) -> Box<dyn DestReceiver> {
    unimplemented!()
}

/// SetRemoteDestReceiverParams: bind a printtup receiver to a portal.
pub fn SetRemoteDestReceiverParams(_self_: &mut dyn DestReceiver, _portal: Portal) {
    unimplemented!()
}

/// SendRowDescriptionMessage: emit a RowDescription ('T') protocol message.
/// `formats` is the optional per-column format-code array.
pub fn SendRowDescriptionMessage(
    _buf: &mut StringInfo,
    _typeinfo: TupleDesc,
    _targetlist: &[Box<Node>],
    _formats: Option<&[i16]>,
) {
    unimplemented!()
}

/// debugStartup: rStartup for the DestDebug receiver.
pub fn debugStartup(_self_: &mut dyn DestReceiver, _operation: CmdType, _typeinfo: TupleDesc) {
    unimplemented!()
}

/// debugtup: receiveSlot for the DestDebug receiver. Returns true to continue.
pub fn debugtup(_slot: &mut TupleTableSlot, _self_: &mut dyn DestReceiver) -> bool {
    unimplemented!()
}

// XXX these are really in executor/spi.c

/// spi_dest_startup: rStartup for the DestSPI receiver.
pub fn spi_dest_startup(_self_: &mut dyn DestReceiver, _operation: CmdType, _typeinfo: TupleDesc) {
    unimplemented!()
}

/// spi_printtup: receiveSlot for the DestSPI receiver. Returns true to continue.
pub fn spi_printtup(_slot: &mut TupleTableSlot, _self_: &mut dyn DestReceiver) -> bool {
    unimplemented!()
}

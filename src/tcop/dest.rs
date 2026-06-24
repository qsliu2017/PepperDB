//! Translated from PostgreSQL src/include/tcop/dest.h
//!
//! Support for communication destinations. The `DestReceiver` routine struct
//! (a vtable of receiveSlot/rStartup/rShutdown/rDestroy fn pointers) becomes a
//! Rust trait per routine-struct.md; `rDestroy` maps to `Drop`.

use crate::access::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::nodes::CmdType;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::cmdtaglist::CommandTag;

/// CommandDest identifies the desired destination of query results.
///
/// Note: only DestNone, DestDebug, DestRemote are legal for the global
/// `whereToSendOutput`. The others may be used per-command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandDest {
    DestNone,             // results are discarded
    DestDebug,            // results go to debugging output
    DestRemote,           // results sent to frontend process
    DestRemoteExecute,    // sent to frontend, in Execute command
    DestRemoteSimple,     // sent to frontend, w/no catalog access
    DestSPI,              // results sent to SPI manager
    DestTuplestore,       // results sent to Tuplestore
    DestIntoRel,          // results sent to relation (SELECT INTO)
    DestCopyOut,          // results sent to COPY TO code
    DestSQLFunction,      // results sent to SQL-language func mgr
    DestTransientRel,     // results sent to transient relation
    DestTupleQueue,       // results sent to tuple queue
    DestExplainSerialize, // results are serialized and discarded
}

/// DestReceiver: a destination-specific receiver of query result tuples.
///
/// C is a struct of fn pointers (`_DestReceiver`); here it is a trait. Each
/// executor run calls `r_startup`, then `receive_slot` zero or more times, then
/// `r_shutdown`. The C `rDestroy` callback becomes the implementor's `Drop`.
pub trait DestReceiver {
    /// Called for each tuple to be output. Returns true to continue, false to
    /// stop early (as if the scan had ended).
    fn receive_slot(&mut self, slot: &mut TupleTableSlot) -> bool;

    /// Per-executor-run initialization. `operation` is the CmdType.
    fn r_startup(&mut self, operation: CmdType, typeinfo: TupleDesc);

    /// Per-executor-run shutdown.
    fn r_shutdown(&mut self);

    /// CommandDest code for this receiver.
    fn mydest(&self) -> CommandDest;
}

// `None_Receiver` is the permanent receiver for DestNone. The global instance is
// installed at startup; modeled as task/session state in the single-process port.
// TODO(struct-forward): install the DestNone singleton in Phase 2.

/// CreateDestReceiver: return a receiver appropriate to the destination.
// TODO(ptr): the concrete receiver type is chosen at runtime; revisit ownership.
pub fn create_dest_receiver(_dest: CommandDest) -> Box<dyn DestReceiver> {
    unimplemented!()
}

/// BeginCommand: initialize the destination at start of command.
pub fn begin_command(_command_tag: CommandTag, _dest: CommandDest) {
    unimplemented!()
}

/// EndCommand: clean up the destination at end of command.
pub fn end_command(
    _qc: &QueryCompletion,
    _dest: CommandDest,
    _force_undecorated_output: bool,
) {
    unimplemented!()
}

pub fn end_replication_command(_command_tag: &str) {
    unimplemented!()
}

pub fn null_command(_dest: CommandDest) {
    unimplemented!()
}

pub fn ready_for_query(_dest: CommandDest) {
    unimplemented!()
}

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
///
/// `Send`: a `Box<dyn DestReceiver>` is carried by `QueryDesc` across the
/// executor's awaits; the backend drives that future on a dedicated task, so the
/// receiver must be sendable. The production receivers (DRprinttup, NoneReceiver)
/// hold only Send state.
pub trait DestReceiver: Send {
    /// Called for each tuple to be output. Returns true to continue, false to
    /// stop early (as if the scan had ended).
    fn receive_slot(&mut self, slot: &mut TupleTableSlot) -> bool;

    /// Per-executor-run initialization. `operation` is the CmdType.
    fn r_startup(&mut self, operation: CmdType, typeinfo: TupleDesc);

    /// Per-executor-run shutdown.
    fn r_shutdown(&mut self);

    /// CommandDest code for this receiver.
    fn mydest(&self) -> CommandDest;

    /// Downcast hook for receivers that carry destination-specific state set
    /// after construction (C casts `DestReceiver *` to the concrete struct,
    /// e.g. `SetRemoteDestReceiverParams` -> `DR_printtup`). Default `None`-like
    /// behavior is impossible for `Any`, so the default panics: only receivers
    /// that need post-construction binding override it.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        unimplemented!("this DestReceiver does not support downcast (as_any_mut)")
    }
}

// `None_Receiver` is the permanent receiver for DestNone (C `donothingDR`). The
// concrete do-nothing receiver lives in the backend body; callers construct one
// via `NoneReceiver` or `CreateDestReceiver(DestNone)`.
pub use crate::backend::tcop::dest::NoneReceiver;

// The bodies live in `crate::backend::tcop::dest`; rewire each header decl to a
// `pub use` so existing `crate::tcop::dest::<CName>` call sites keep resolving
// (rules.md s3, non-type-centric file).

/// CreateDestReceiver: return a receiver appropriate to the destination.
pub use crate::backend::tcop::dest::create_dest_receiver as CreateDestReceiver;

/// BeginCommand: initialize the destination at start of command.
pub use crate::backend::tcop::dest::begin_command as BeginCommand;

/// EndCommand: send the CommandComplete tag at end of command.
pub use crate::backend::tcop::dest::end_command as EndCommand;

/// EndReplicationCommand: stripped-down EndCommand for replication (deferred).
pub use crate::backend::tcop::dest::end_replication_command as EndReplicationCommand;

/// NullCommand: tell the dest an empty query string was recognized.
pub use crate::backend::tcop::dest::null_command as NullCommand;

/// ReadyForQuery: announce readiness for a new query and flush (async).
pub use crate::backend::tcop::dest::ready_for_query as ReadyForQuery;

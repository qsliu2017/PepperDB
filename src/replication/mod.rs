//! Logical/physical replication subsystem
//! (postgres/src/backend/replication + postgres/src/include/replication).
//!
//! Header-only type/prototype layer so far; the apply/launcher/walsender
//! implementations are future work.

pub mod walsender;
pub mod slot;
pub mod walreceiver;
pub mod libpqwalreceiver;
pub mod syncrep;
pub mod logical;
pub mod logicallauncher;
pub mod walreceiverfuncs;
pub mod logicalproto;
pub mod logicalrelation;
pub mod logicalworker;
pub mod output_plugin;
pub mod snapbuild_internal;
pub mod walsender_private;
pub mod worker_internal;

pub mod message;
pub mod slotfuncs;
pub mod pgoutput;

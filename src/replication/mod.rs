//! Directory module: src/include/replication

// === scaffold: child modules (Phase 0) ===
pub mod conflict;
pub mod decode;
pub mod logical;
pub mod logicallauncher;
pub mod logicalproto;
pub mod logicalrelation;
pub mod logicalworker;
pub mod message;
pub mod origin;
pub mod output_plugin;
pub mod pgoutput;
pub mod reorderbuffer;
pub mod slot;
pub mod slotsync;
pub mod snapbuild;
pub mod snapbuild_internal;
pub mod syncrep;
pub mod walreceiver;
pub mod walsender;
pub mod walsender_private;
pub mod worker_internal;
// === end scaffold ===

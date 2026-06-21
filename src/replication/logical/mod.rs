//! replication/logical module
pub mod worker;
pub mod tablesync;
pub mod logical;
pub mod reorderbuffer;
pub mod snapbuild;
pub mod origin;
pub mod decode;
pub mod launcher;
pub mod proto;
pub mod conflict;
pub mod slotsync;
// DEFERRED: logicalfuncs needs the logical-decoding subsystem (CreateDecodingContext,
// the XLogReaderRoutine callback ABI, ReplicationSlot) - leave unwired until those land.
// pub mod logicalfuncs;

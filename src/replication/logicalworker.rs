//! replication/logicalworker.h - Exports for logical replication workers.

use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// volatile sig_atomic_t ParallelApplyMessagePending;
// sig_atomic_t is conventionally c_int.
pub static mut ParallelApplyMessagePending: ::std::ffi::c_int = 0;

pub unsafe fn ApplyWorkerMain(main_arg: Datum) {
    unimplemented!()
}

pub unsafe fn ParallelApplyWorkerMain(main_arg: Datum) {
    unimplemented!()
}

pub unsafe fn TablesyncWorkerMain(main_arg: Datum) {
    unimplemented!()
}

pub unsafe fn IsLogicalWorker() -> bool {
    unimplemented!()
}

pub unsafe fn IsLogicalParallelApplyWorker() -> bool {
    unimplemented!()
}

pub unsafe fn HandleParallelApplyMessageInterrupt() {
    unimplemented!()
}

pub unsafe fn ProcessParallelApplyMessages() {
    unimplemented!()
}

pub unsafe fn LogicalRepWorkersWakeupAtCommit(subid: Oid) {
    unimplemented!()
}

pub unsafe fn AtEOXact_LogicalRepWorkers(isCommit: bool) {
    unimplemented!()
}

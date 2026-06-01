//! replication/logicallauncher.h - exports for logical replication launcher.

use std::ffi::c_int;

use crate::c::Size;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// pid_t is the POSIX process id type; no ported module yet.
// TODO: dedup when a proper port of <sys/types.h>/port lands.
pub type pid_t = c_int;

extern "C" {
    pub static mut max_logical_replication_workers: c_int;
    pub static mut max_sync_workers_per_subscription: c_int;
    pub static mut max_parallel_apply_workers_per_subscription: c_int;
}

pub unsafe fn ApplyLauncherRegister() {
    unimplemented!()
}

pub unsafe fn ApplyLauncherMain(main_arg: Datum) {
    unimplemented!()
}

pub unsafe fn ApplyLauncherShmemSize() -> Size {
    unimplemented!()
}

pub unsafe fn ApplyLauncherShmemInit() {
    unimplemented!()
}

pub unsafe fn ApplyLauncherForgetWorkerStartTime(subid: Oid) {
    unimplemented!()
}

pub unsafe fn ApplyLauncherWakeupAtCommit() {
    unimplemented!()
}

pub unsafe fn AtEOXact_ApplyLauncher(isCommit: bool) {
    unimplemented!()
}

pub unsafe fn IsLogicalLauncher() -> bool {
    unimplemented!()
}

pub unsafe fn GetLeaderApplyWorkerPid(pid: pid_t) -> pid_t {
    unimplemented!()
}

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

pub unsafe fn ApplyLauncherRegister() { crate::replication::logical::launcher::ApplyLauncherRegister() }

pub unsafe fn ApplyLauncherMain(main_arg: Datum) { crate::replication::logical::launcher::ApplyLauncherMain(main_arg as _) }

pub unsafe fn ApplyLauncherShmemSize() -> Size { crate::replication::logical::launcher::ApplyLauncherShmemSize() }

pub unsafe fn ApplyLauncherShmemInit() { crate::replication::logical::launcher::ApplyLauncherShmemInit() }

pub unsafe fn ApplyLauncherForgetWorkerStartTime(subid: Oid) { crate::replication::logical::launcher::ApplyLauncherForgetWorkerStartTime(subid as _) }

pub unsafe fn ApplyLauncherWakeupAtCommit() { crate::replication::logical::launcher::ApplyLauncherWakeupAtCommit() }

pub unsafe fn AtEOXact_ApplyLauncher(isCommit: bool) { crate::replication::logical::launcher::AtEOXact_ApplyLauncher(isCommit) }

pub unsafe fn IsLogicalLauncher() -> bool { crate::replication::logical::launcher::IsLogicalLauncher() }

pub unsafe fn GetLeaderApplyWorkerPid(pid: pid_t) -> pid_t { crate::replication::logical::launcher::GetLeaderApplyWorkerPid(pid) }

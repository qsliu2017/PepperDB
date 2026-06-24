//! Translated from PostgreSQL src/include/postmaster/postmaster.h
//
// Exports from postmaster.c. Process-management state; in-memory only. Many
// GUC/process externs -> static mut with TODO(global). EXEC_BACKEND / WIN32
// branches are dropped (non-target).

use crate::lib::ilist::{dlist_head, dlist_node};
use crate::miscadmin::BackendType;
use crate::postmaster::bgworker_internals::RegisteredBgWorker;

// ClientSocket forward reference: real definition in libpq/libpq-be.h.
#[deprecated(note = "TODO(struct-forward): repoint to crate::libpq::libpq_be::ClientSocket in Phase 2")]
// TODO(struct-forward)
pub struct ClientSocket {
    _opaque: (),
}

/// An active postmaster child process. Used to track children and signal them.
pub struct PMChild {
    pub pid: i32,                       // process id of backend (pid_t)
    pub child_slot: i32,                // PMChildSlot for this backend, if any
    pub bkend_type: BackendType,        // child process flavor
    pub rw: *mut RegisteredBgWorker,    // bgworker info; TODO(ptr): Option<&>
    pub bgworker_notify: bool,          // gets bgworker start/stop notifications
    pub elem: dlist_node,               // list link in ActiveChildList
}

// postmaster_alive_fds indices (non-WIN32 path).
pub const POSTMASTER_FD_WATCH: usize = 0; // children check for postmaster death
pub const POSTMASTER_FD_OWN: usize = 1;   // kept open by postmaster only

// GUC options. TODO(global): move into a server-config context.
pub static mut EnableSSL: bool = false;
pub static mut SuperuserReservedConnections: i32 = 0;
pub static mut ReservedConnections: i32 = 0;
pub static mut PostPortNumber: i32 = 0;
pub static mut Unix_socket_permissions: i32 = 0;
pub static mut Unix_socket_group: Option<String> = None;
pub static mut Unix_socket_directories: Option<String> = None;
pub static mut ListenAddresses: Option<String> = None;
pub static mut ClientAuthInProgress: bool = false;
pub static mut PreAuthDelay: i32 = 0;
pub static mut AuthenticationTimeout: i32 = 0;
pub static mut log_hostname: bool = false;
pub static mut enable_bonjour: bool = false;
pub static mut bonjour_name: Option<String> = None;
pub static mut restart_after_crash: bool = false;
pub static mut remove_temp_files_after_crash: bool = false;
pub static mut send_abort_for_crash: bool = false;
pub static mut send_abort_for_kill: bool = false;

pub static mut progname: Option<&'static str> = None;

pub static mut redirection_done: bool = false;
pub static mut LoadedSSL: bool = false;

// defined in globals.c
pub static mut MyClientSocket: *mut ClientSocket = core::ptr::null_mut();

// defined in pmchild.c
pub static mut ActiveChildList: Option<dlist_head> = None;

/// pg_noreturn in C. TODO(panic).
pub fn PostmasterMain(_argc: i32, _argv: &[String]) -> ! {
    unimplemented!()
}

pub fn ClosePostmasterPorts(_am_syslogger: bool) {
    unimplemented!()
}

pub fn InitProcessGlobals() {
    unimplemented!()
}

pub fn MaxLivePostmasterChildren() -> i32 {
    unimplemented!()
}

pub fn PostmasterMarkPIDForWorkerNotify(_pid: i32) -> bool {
    unimplemented!()
}

// prototypes for functions in launch_backend.c
pub fn postmaster_child_launch(
    _child_type: BackendType,
    _child_slot: i32,
    _startup_data: &[u8],
    _client_sock: *mut ClientSocket,
) -> i32 {
    unimplemented!()
}

pub fn PostmasterChildName(_child_type: BackendType) -> &'static str {
    unimplemented!()
}

// prototypes for functions in pmchild.c
pub fn InitPostmasterChildSlots() {
    unimplemented!()
}

pub fn AssignPostmasterChildSlot(_btype: BackendType) -> *mut PMChild {
    unimplemented!()
}

pub fn AllocDeadEndChild() -> *mut PMChild {
    unimplemented!()
}

pub fn ReleasePostmasterChildSlot(_pmchild: &mut PMChild) -> bool {
    unimplemented!()
}

/// Returns the child for a pid, or None if not found.
pub fn FindPostmasterChildByPid(_pid: i32) -> Option<*mut PMChild> {
    unimplemented!()
}

/// Special must-be-first options for dispatching to subprograms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchOption {
    DISPATCH_CHECK,
    DISPATCH_BOOT,
    DISPATCH_FORKCHILD,
    DISPATCH_DESCRIBE_CONFIG,
    DISPATCH_SINGLE,
    DISPATCH_POSTMASTER, // must be last
}

/// Convert an option name to a DispatchOption (None if unrecognized).
pub fn parse_dispatch_option(_name: &str) -> Option<DispatchOption> {
    unimplemented!()
}

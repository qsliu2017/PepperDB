//! port/win32ntdll.h - Dynamically loaded Windows NT functions.

use std::ffi::{c_int, c_void};

use crate::c::uint32;

// --- Windows/NT system types (not ported elsewhere). Stubbed locally. ---
// TODO: dedup - these come from <ntstatus.h>/<winternl.h>/<windows.h>.
pub type NTSTATUS = i32; // LONG
pub type ULONG = uint32;
pub type HANDLE = *mut c_void;
pub type PVOID = *mut c_void;
pub type PIO_STATUS_BLOCK = *mut c_void; // pointer to IO_STATUS_BLOCK

// #ifndef FLUSH_FLAGS_FILE_DATA_SYNC_ONLY
pub const FLUSH_FLAGS_FILE_DATA_SYNC_ONLY: c_int = 0x4;

// typedef NTSTATUS (__stdcall * RtlGetLastNtStatus_t) (void);
pub type RtlGetLastNtStatus_t = Option<unsafe extern "C" fn() -> NTSTATUS>;

// typedef ULONG (__stdcall * RtlNtStatusToDosError_t) (NTSTATUS);
pub type RtlNtStatusToDosError_t = Option<unsafe extern "C" fn(NTSTATUS) -> ULONG>;

// typedef NTSTATUS (__stdcall * NtFlushBuffersFileEx_t)
//     (HANDLE, ULONG, PVOID, ULONG, PIO_STATUS_BLOCK);
pub type NtFlushBuffersFileEx_t =
    Option<unsafe extern "C" fn(HANDLE, ULONG, PVOID, ULONG, PIO_STATUS_BLOCK) -> NTSTATUS>;

extern "C" {
    // extern PGDLLIMPORT RtlGetLastNtStatus_t pg_RtlGetLastNtStatus;
    pub static mut pg_RtlGetLastNtStatus: RtlGetLastNtStatus_t;
    // extern PGDLLIMPORT RtlNtStatusToDosError_t pg_RtlNtStatusToDosError;
    pub static mut pg_RtlNtStatusToDosError: RtlNtStatusToDosError_t;
    // extern PGDLLIMPORT NtFlushBuffersFileEx_t pg_NtFlushBuffersFileEx;
    pub static mut pg_NtFlushBuffersFileEx: NtFlushBuffersFileEx_t;
}

// extern int initialize_ntdll(void);
pub unsafe fn initialize_ntdll() -> c_int {
    unimplemented!()
}

//! storage/io_worker.h - IO worker for implementing AIO "ourselves"

use crate::c::Size;
use std::ffi::c_int;
use std::ffi::c_void;

// pg_noreturn extern void IoWorkerMain(const void *startup_data, size_t startup_data_len);
pub unsafe fn IoWorkerMain(_startup_data: *const c_void, _startup_data_len: Size) -> ! {
    unimplemented!()
}

// extern PGDLLIMPORT int io_workers;
pub static mut io_workers: c_int = 0;

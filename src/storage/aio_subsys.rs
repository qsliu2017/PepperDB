//! storage/aio_subsys.h - Interaction with AIO as a subsystem, rather than actually issuing AIO.
//!
//! This header is for AIO related functionality that's being called by files
//! that don't perform AIO, but interact with the AIO subsystem in some form.

use crate::c::Size;

/* aio_init.c */
pub unsafe fn AioShmemSize() -> Size {
    crate::storage::aio::aio_init::AioShmemSize()
}

pub unsafe fn AioShmemInit() {
    crate::storage::aio::aio_init::AioShmemInit()
}

pub unsafe fn pgaio_init_backend() {
    crate::storage::aio::aio_init::pgaio_init_backend()
}

/* aio.c */
pub unsafe fn pgaio_error_cleanup() {
    crate::storage::aio::aio::pgaio_error_cleanup()
}

pub unsafe fn AtEOXact_Aio(is_commit: bool) {
    crate::storage::aio::aio::AtEOXact_Aio(is_commit)
}

/* method_worker.c */
pub unsafe fn pgaio_workers_enabled() -> bool {
    crate::storage::aio::method_worker::pgaio_workers_enabled()
}

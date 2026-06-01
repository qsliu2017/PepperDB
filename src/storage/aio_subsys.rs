//! storage/aio_subsys.h - Interaction with AIO as a subsystem, rather than actually issuing AIO.
//!
//! This header is for AIO related functionality that's being called by files
//! that don't perform AIO, but interact with the AIO subsystem in some form.

use crate::c::Size;

/* aio_init.c */
pub unsafe fn AioShmemSize() -> Size {
    unimplemented!()
}

pub unsafe fn AioShmemInit() {
    unimplemented!()
}

pub unsafe fn pgaio_init_backend() {
    unimplemented!()
}

/* aio.c */
pub unsafe fn pgaio_error_cleanup() {
    unimplemented!()
}

pub unsafe fn AtEOXact_Aio(is_commit: bool) {
    let _ = is_commit;
    unimplemented!()
}

/* method_worker.c */
pub unsafe fn pgaio_workers_enabled() -> bool {
    unimplemented!()
}

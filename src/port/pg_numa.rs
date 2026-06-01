//! port/pg_numa.h - Basic NUMA portability routines

use crate::c::uint64;
use std::ffi::{c_int, c_ulong, c_void};

extern "C" {
    pub fn pg_numa_init() -> c_int;
    pub fn pg_numa_query_pages(
        pid: c_int,
        count: c_ulong,
        pages: *mut *mut c_void,
        status: *mut c_int,
    ) -> c_int;
    pub fn pg_numa_get_max_node() -> c_int;
}

// In C this is either a `static inline` function (USE_LIBNUMA) that page-faults
// the memory via a volatile read before move_pages(2), or a no-op macro
// `do {} while(0)`. Both forms are translated to a single inline fn; the
// volatile-touch body is kept (it is a harmless read when libnuma is enabled
// and the read still page-faults the memory as required).
// USE_LIBNUMA is a build-config the port doesn't define as a Cargo feature; the
// volatile-touch read is kept unconditionally (harmless, and it page-faults the
// memory as the libnuma path requires).
#[inline]
pub unsafe fn pg_numa_touch_mem_if_required(ptr: *mut c_void) {
    let _touch: uint64 = std::ptr::read_volatile(ptr as *const uint64);
}

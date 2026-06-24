//! Translated from PostgreSQL src/include/port/pg_numa.h

// Basic NUMA portability routines. Bare declarations -> stubs.

/// Initialize NUMA support. Returns 0 on success, -1 on failure.
pub fn pg_numa_init() -> i32 {
    unimplemented!()
}

/// Query the NUMA node of each page; fills `status` per page.
pub fn pg_numa_query_pages(_pid: i32, _pages: &[*mut core::ffi::c_void], _status: &mut [i32]) -> i32 {
    unimplemented!()
}

/// Highest NUMA node number available.
pub fn pg_numa_get_max_node() -> i32 {
    unimplemented!()
}

// Without libnuma (our default), this is a no-op. With libnuma it touches the
// page to fault it in before move_pages(2).
pub fn pg_numa_touch_mem_if_required(_ptr: *const core::ffi::c_void) {}

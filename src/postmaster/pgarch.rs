//! Translated from PostgreSQL src/include/postmaster/pgarch.h

// Archivable WAL file name constraints.
pub const MIN_XFN_CHARS: usize = 16;
pub const MAX_XFN_CHARS: usize = 40;
pub const VALID_XFN_CHARS: &str = "0123456789ABCDEF.history.backup.partial";

pub fn pg_arch_shmem_size() -> usize {
    unimplemented!()
}

pub fn pg_arch_shmem_init() {
    unimplemented!()
}

pub fn pg_arch_can_restart() -> bool {
    unimplemented!()
}

pub fn pg_archiver_main(_startup_data: &[u8]) -> ! {
    unimplemented!()
}

pub fn pg_arch_wakeup() {
    unimplemented!()
}

pub fn pg_arch_force_dir_scan() {
    unimplemented!()
}

//! Translated from PostgreSQL src/include/archive/shell_archive.h
//
// Special initialization for the built-in shell-command archiver: it returns an
// ArchiveModuleCallbacks table directly (no shared library load needed).

use crate::archive::archive_module::ArchiveModuleCallbacks;

/// C: `const ArchiveModuleCallbacks *shell_archive_init(void)`
pub fn shell_archive_init() -> ArchiveModuleCallbacks {
    unimplemented!()
}

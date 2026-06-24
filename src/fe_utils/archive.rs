//! Translated from PostgreSQL src/include/fe_utils/archive.h

/// C: `int RestoreArchivedFile(path, xlogfname, expectedSize, restoreCommand)`.
/// Returns the resulting fd (or status int in the C original).
pub fn restore_archived_file(
    _path: &str,
    _xlogfname: &str,
    _expected_size: i64,
    _restore_command: &str,
) -> i32 {
    unimplemented!()
}

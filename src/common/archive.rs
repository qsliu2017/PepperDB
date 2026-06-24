//! Translated from PostgreSQL src/include/common/archive.h

/// Build a restore command, substituting %p/%f/%r. None on error.
pub fn build_restore_command(
    restore_command: &str,
    xlogpath: &str,
    xlogfname: &str,
    last_restart_point_fname: Option<&str>,
) -> Option<String> {
    let _ = (
        restore_command,
        xlogpath,
        xlogfname,
        last_restart_point_fname,
    );
    unimplemented!()
}

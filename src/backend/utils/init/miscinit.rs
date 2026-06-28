//! Miscellaneous backend initialization support. Translated from backend/utils/init/miscinit.c.
//!
//! Collects the assorted startup helpers that do not belong to any single
//! subsystem: the effective/session/authenticated user-id state and the role
//! machinery behind `SET ROLE` and `SET SESSION AUTHORIZATION`, the OS user-name
//! lookup, validation of a data directory's `PG_VERSION` marker, and the data
//! directory lock file (`postmaster.pid`) that records the owning process and its
//! connection parameters.
//!
//! The user-id values that PostgreSQL keeps as file-static `Oid`s
//! (`AuthenticatedUserId`, `SessionUserId`, `OuterUserId`, `CurrentUserId`, and
//! the security-restriction context) live on the per-task [`crate::session::Session`]
//! instead, so the accessors here read and mutate the current task's session
//! rather than process-wide globals. The lock-file reader and writer preserve the
//! on-disk line format so an existing data directory round-trips, but file access
//! goes through the asynchronous [`IoBackend`].
//!
//! Because the server runs as a single process, the multiprocess and privilege
//! machinery is dropped: there is no `fork`, no `setuid`/group handling, no
//! signal-based interlock that probes a stale pid before overwriting the lock
//! file, and no `on_proc_exit` callback that unlinks it. A pre-existing lock file
//! is therefore treated as a hard error rather than reclaimed. Two helpers are
//! provisional pending deferred subsystems: the OS user name is read from the
//! environment instead of the password database, and mapping a role oid to a name
//! requires a `pg_authid` catalog lookup that is not yet implemented.

use std::io;
use std::sync::Arc;

use crate::backend::utils::init::globals::ProcessConfig;
use crate::pg_config::PG_MAJORVERSION;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::session;
use crate::storage::io_backend::{self, IoBackend, OpenFlags};
use crate::utils::pidfile;

/// PG `DIRECTORY_LOCK_FILE` -- the datadir lock file name.
pub const DIRECTORY_LOCK_FILE: &str = "postmaster.pid";
/// The data-version marker file.
pub const PG_VERSION_FILE: &str = "PG_VERSION";

// ---------------------------------------------------------------------------
// User ID state (over Session)
// ---------------------------------------------------------------------------

/// PG `GetUserId` -- current effective user id.
pub fn get_user_id() -> Oid {
    session::current().current_user_id()
}

/// PG `GetOuterUserId`.
pub fn get_outer_user_id() -> Oid {
    session::current().outer_user_id()
}

/// PG `GetSessionUserId`.
pub fn get_session_user_id() -> Oid {
    session::current().session_user_id()
}

/// PG `GetSessionUserIsSuperuser`.
pub fn get_session_user_is_superuser() -> bool {
    session::current().session_user_is_superuser()
}

/// PG `GetAuthenticatedUserId`.
pub fn get_authenticated_user_id() -> Oid {
    session::current().authenticated_user_id()
}

/// PG `SetAuthenticatedUserId` -- set once at connection start.
pub fn set_authenticated_user_id(userid: Oid) {
    session::current().set_authenticated_user_id(userid);
}

/// PG `SetSessionAuthorization`: set the session user, and (unless a SET ROLE is
/// active) the outer/effective user too.
pub fn set_session_authorization(userid: Oid, is_superuser: bool) {
    let s = session::current();
    s.set_session_user_id(userid, is_superuser);
    if !s.set_role_is_active() {
        s.set_outer_user_id(userid, is_superuser);
    }
}

/// PG `GetCurrentRoleId`: outer-level role, or `InvalidOid` for SET ROLE NONE.
pub fn get_current_role_id() -> Oid {
    let s = session::current();
    if s.set_role_is_active() {
        s.outer_user_id()
    } else {
        InvalidOid
    }
}

/// PG `SetCurrentRoleId` (SET ROLE). `InvalidOid` reverts to the session user.
pub fn set_current_role_id(roleid: Oid, is_superuser: bool) {
    let s = session::current();
    let (roleid, is_superuser) = if roleid == InvalidOid {
        s.set_set_role_is_active(false);
        if s.session_user_id() == InvalidOid {
            return;
        }
        (s.session_user_id(), s.session_user_is_superuser())
    } else {
        s.set_set_role_is_active(true);
        (roleid, is_superuser)
    };
    s.set_outer_user_id(roleid, is_superuser);
}

// ---------------------------------------------------------------------------
// User name (best-effort; privilege/setuid logic deleted by redesign)
// ---------------------------------------------------------------------------

/// PG `GetUserName` -- the OS user running the process. Best-effort over the
/// environment; PROVISIONAL (PG reads the passwd database).
pub fn get_user_name() -> Option<String> {
    std::env::var("USER")
        .or_else(|_| std::env::var("LOGNAME"))
        .ok()
}

/// PG `GetUserNameFromId` -- map a role oid to its name. PROVISIONAL: needs a
/// catalog (`pg_authid`) lookup, which is a deferred subsystem. TODO(catalog).
pub fn get_user_name_from_id(_roleid: Oid, _noerr: bool) -> Option<String> {
    None
}

// ---------------------------------------------------------------------------
// Data-directory lock file (postmaster.pid)
// ---------------------------------------------------------------------------

/// Parsed view of the first lines of a datadir lock file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockFileInfo {
    pub pid: i32,
    pub data_dir: String,
    pub start_time: i64,
    pub port: i32,
    pub socket_dir: String,
}

/// Render the first five lock-file lines (PG's `CreateLockFile` buffer). A
/// standalone backend writes a negated pid; line order matches `pidfile.h`.
fn render_lock_file(info: &LockFileInfo, am_postmaster: bool) -> String {
    let pid = if am_postmaster { info.pid } else { -info.pid };
    let mut s = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        pid, info.data_dir, info.start_time, info.port, info.socket_dir
    );
    // LOCK_FILE_LINE_LISTEN_ADDR is filled empty now only for a standalone backend.
    if !am_postmaster {
        s.push('\n');
    }
    s
}

/// Parse a datadir lock file's first lines back into [`LockFileInfo`]. The pid is
/// stored negated for a standalone backend; the absolute value is returned.
pub fn parse_lock_file(contents: &str) -> Option<LockFileInfo> {
    let mut lines = contents.lines();
    let pid: i32 = lines.next()?.trim().parse().ok()?;
    let data_dir = lines.next()?.to_string();
    let start_time: i64 = lines.next()?.trim().parse().ok()?;
    let port: i32 = lines.next()?.trim().parse().ok()?;
    let socket_dir = lines.next().unwrap_or("").to_string();
    Some(LockFileInfo {
        pid: pid.abs(),
        data_dir,
        start_time,
        port,
        socket_dir,
    })
}

/// PG `CreateDataDirLockFile`: write `DataDir/postmaster.pid` with the pid,
/// datadir, start time, port and socket-dir lines, then fsync. The kill-based
/// stale-pid interlock is dropped (single process); we create exclusively so a
/// pre-existing lock file is an error.
pub async fn create_data_dir_lock_file(
    config: &ProcessConfig,
    io: &Arc<IoBackend>,
    info: &LockFileInfo,
    am_postmaster: bool,
) -> io::Result<()> {
    let data_dir = config
        .data_dir()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "DataDir not set"))?;
    let path = std::path::Path::new(&data_dir).join(DIRECTORY_LOCK_FILE);

    let flags = OpenFlags { create_new: true, ..OpenFlags::read_write() };
    let (file, _permit) = io.open(&path, flags).await?;
    let buf = render_lock_file(info, am_postmaster);
    io.write_at(&file, buf.as_bytes(), 0).await?;
    io.fsync(&file).await;
    Ok(())
}

/// Read and parse `DataDir/postmaster.pid` (PG `RecheckDataDirLockFile` reads it
/// back; here we expose a plain parse for round-tripping/tests).
pub async fn read_data_dir_lock_file(
    config: &ProcessConfig,
    io: &Arc<IoBackend>,
) -> io::Result<LockFileInfo> {
    let data_dir = config
        .data_dir()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "DataDir not set"))?;
    let path = std::path::Path::new(&data_dir).join(DIRECTORY_LOCK_FILE);
    let contents = read_file_to_string(io, &path).await?;
    parse_lock_file(&contents)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed lock file"))
}

// ---------------------------------------------------------------------------
// Data-directory / version validation
// ---------------------------------------------------------------------------

/// PG `ValidatePgVersion`: read `path/PG_VERSION` and require its major version
/// to match this server's. The setuid/permission parts of `checkDataDir` are
/// deleted by the redesign; this is the portable subset.
pub async fn validate_pg_version(io: &Arc<IoBackend>, path: &str) -> io::Result<()> {
    let full = std::path::Path::new(path).join(PG_VERSION_FILE);
    let contents = read_file_to_string(io, &full).await.map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("\"{path}\" is not a valid data directory: PG_VERSION is missing"),
            )
        } else {
            e
        }
    })?;

    let file_major = contents
        .split_whitespace()
        .next()
        .and_then(|tok| tok.split('.').next())
        .and_then(|maj| maj.parse::<u32>().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("\"{path}\" is not a valid data directory: PG_VERSION is invalid"),
            )
        })?;

    #[allow(
        clippy::expect_used,
        reason = "PG_MAJORVERSION is a compile-time numeric constant literal"
    )]
    let my_major: u32 = PG_MAJORVERSION.parse().expect("PG_MAJORVERSION is numeric");
    if file_major != my_major {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "database files are incompatible with server: data directory was initialized by \
                 PostgreSQL major version {file_major}, server is {my_major}"
            ),
        ));
    }
    Ok(())
}

/// Read an entire small file through the [`IoBackend`] into a `String`.
async fn read_file_to_string(io: &Arc<IoBackend>, path: &std::path::Path) -> io::Result<String> {
    let (file, _permit) = io.open(path, OpenFlags::read_only()).await?;
    let len = io.size(&file).await? as usize;
    let mut buf = vec![0u8; len];
    if len > 0 {
        io.read_at(&file, &mut buf, 0).await?;
    }
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let uniq = format!(
            "pepperdb-miscinit-{}-{}-{}",
            tag,
            std::process::id(),
            session::alloc_proc_pid()
        );
        p.push(uniq);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn lock_file_render_parse_round_trips() {
        let info = LockFileInfo {
            pid: 4242,
            data_dir: "/var/lib/pgdata".to_string(),
            start_time: 1_700_000_000,
            port: 5432,
            socket_dir: "/tmp".to_string(),
        };
        let rendered = render_lock_file(&info, true);
        assert_eq!(parse_lock_file(&rendered).unwrap(), info);
        // Standalone backend negates the pid on disk but parses back positive.
        let rendered_sa = render_lock_file(&info, false);
        assert!(rendered_sa.starts_with("-4242\n"));
        assert_eq!(parse_lock_file(&rendered_sa).unwrap(), info);
    }

    #[tokio::test]
    async fn create_and_read_lock_file_round_trips() {
        let dir = temp_dir("lock");
        let config = ProcessConfig::new();
        config.set_data_dir(dir.to_str().unwrap());
        let io = Arc::new(IoBackend::new(io_backend::DEFAULT_FD_BUDGET));

        let info = LockFileInfo {
            pid: 9001,
            data_dir: dir.to_str().unwrap().to_string(),
            start_time: 1_710_000_000,
            port: 5433,
            socket_dir: String::new(),
        };
        create_data_dir_lock_file(&config, &io, &info, true)
            .await
            .unwrap();

        let got = read_data_dir_lock_file(&config, &io).await.unwrap();
        assert_eq!(got.pid, 9001);
        assert_eq!(got.data_dir, info.data_dir);
        assert_eq!(got.port, 5433);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn validate_pg_version_accepts_matching_and_rejects_bad() {
        let dir = temp_dir("ver");
        let io = Arc::new(IoBackend::new(io_backend::DEFAULT_FD_BUDGET));
        let path = dir.to_str().unwrap();

        std::fs::write(dir.join(PG_VERSION_FILE), format!("{PG_MAJORVERSION}\n")).unwrap();
        assert!(validate_pg_version(&io, path).await.is_ok());

        std::fs::write(dir.join(PG_VERSION_FILE), "3\n").unwrap();
        assert!(validate_pg_version(&io, path).await.is_err());

        std::fs::write(dir.join(PG_VERSION_FILE), "garbage\n").unwrap();
        assert!(validate_pg_version(&io, path).await.is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn lock_file_line_constants_match_pidfile() {
        // The renderer relies on pidfile.h's line ordering.
        assert_eq!(pidfile::LOCK_FILE_LINE_PID, 1);
        assert_eq!(pidfile::LOCK_FILE_LINE_DATA_DIR, 2);
        assert_eq!(pidfile::LOCK_FILE_LINE_START_TIME, 3);
        assert_eq!(pidfile::LOCK_FILE_LINE_PORT, 4);
        assert_eq!(pidfile::LOCK_FILE_LINE_SOCKET_DIR, 5);
    }
}

//! utils/pidfile.h - Declarations describing the data directory lock file (postmaster.pid)

/*
 * As of Postgres 10, the contents of the data-directory lock file are:
 *
 * line #
 *		1	postmaster PID (or negative of a standalone backend's PID)
 *		2	data directory path
 *		3	postmaster start timestamp (time_t representation)
 *		4	port number
 *		5	first Unix socket directory path (empty if none)
 *		6	first listen_address (IP address or "*"; empty if no TCP port)
 *		7	shared memory key (empty on Windows)
 *		8	postmaster status (see values below)
 */
pub const LOCK_FILE_LINE_PID: i32 = 1;
pub const LOCK_FILE_LINE_DATA_DIR: i32 = 2;
pub const LOCK_FILE_LINE_START_TIME: i32 = 3;
pub const LOCK_FILE_LINE_PORT: i32 = 4;
pub const LOCK_FILE_LINE_SOCKET_DIR: i32 = 5;
pub const LOCK_FILE_LINE_LISTEN_ADDR: i32 = 6;
pub const LOCK_FILE_LINE_SHMEM_KEY: i32 = 7;
pub const LOCK_FILE_LINE_PM_STATUS: i32 = 8;

/*
 * The PM_STATUS line may contain one of these values.  All these strings
 * must be the same length, per comments for AddToDataDirLockFile().
 * We pad with spaces as needed to make that true.
 */
pub const PM_STATUS_STARTING: &[u8] = b"starting\0"; /* still starting up */
pub const PM_STATUS_STOPPING: &[u8] = b"stopping\0"; /* in shutdown sequence */
pub const PM_STATUS_READY: &[u8] = b"ready   \0"; /* ready for connections */
pub const PM_STATUS_STANDBY: &[u8] = b"standby \0"; /* up, won't accept connections */

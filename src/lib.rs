use std::sync::{Arc, Mutex};

pub mod catalog;
pub mod executor;
pub mod parser;
pub mod server;
pub mod storage;
pub mod txn;
pub mod types;
pub mod wal;

use catalog::bootstrap;
use catalog::Catalog;
use datafusion::execution::context::SessionContext;
use storage::disk::DiskManager;
use storage::pg_control::{self, ControlFileData, DBState};
use txn::TxnManager;
use wal::writer::WalWriter;

mod udfs;

/// Default database OID, matching PostgreSQL's `postgres` database.
const DEFAULT_DB_OID: u32 = 5;

pub struct Database {
    pub catalog: Mutex<Catalog>,
    pub disk: Arc<Mutex<DiskManager>>,
    pub session: SessionContext,
    pub wal: Mutex<WalWriter>,
    pub txn: Arc<Mutex<TxnManager>>,
    control: Mutex<ControlFileData>,
}

impl Database {
    pub fn new(data_dir: &std::path::Path) -> Self {
        let disk = DiskManager::new(data_dir, DEFAULT_DB_OID);
        let control_path = data_dir.join("global").join("pg_control");
        let map_path = data_dir.join("global").join("pg_filenode.map");
        let wal_dir = data_dir.join("pg_wal");
        let clog_dir = data_dir.join("pg_xact");

        // Bootstrap or load catalog
        let catalog = if map_path.exists() {
            bootstrap::load_catalog(&disk).expect("failed to load catalog")
        } else {
            bootstrap::initdb(&disk).expect("failed to run initdb");
            Catalog::new()
        };

        let mut wal_start = 0u64;

        let control = if control_path.exists() {
            let mut ctl =
                pg_control::read_control_file(&control_path).expect("failed to read pg_control");

            // Crash recovery: if not cleanly shut down, replay WAL
            if ctl.state != DBState::Shutdowned {
                let end_lsn = wal::recovery::recover(&disk, &wal_dir, ctl.checkpoint_redo)
                    .expect("WAL recovery failed");
                wal_start = end_lsn;
            } else {
                wal_start = ctl.checkpoint_redo;
            }

            ctl.state = DBState::InProduction;
            pg_control::write_control_file(&control_path, &ctl)
                .expect("failed to update pg_control state");
            ctl
        } else {
            let ctl = ControlFileData {
                system_identifier: rand_system_id(),
                state: DBState::InProduction,
                ..Default::default()
            };
            pg_control::write_control_file(&control_path, &ctl)
                .expect("failed to write initial pg_control");
            ctl
        };

        let wal_writer = WalWriter::new(&wal_dir, wal_start);
        let txn_mgr = TxnManager::new(&clog_dir, txn::FIRST_NORMAL_XID);

        let session = SessionContext::new();
        udfs::register_all(&session);

        Self {
            catalog: Mutex::new(catalog),
            disk: Arc::new(Mutex::new(disk)),
            session,
            wal: Mutex::new(wal_writer),
            txn: Arc::new(Mutex::new(txn_mgr)),
            control: Mutex::new(control),
        }
    }

    /// Clean shutdown: flush WAL, write checkpoint, set state to DB_SHUTDOWNED.
    pub fn shutdown(&self) {
        let mut wal = self.wal.lock().unwrap();
        wal.flush();
        let checkpoint_lsn = wal.current_lsn();

        let disk = self.disk.lock().unwrap();
        let control_path = disk.base_path().join("global").join("pg_control");
        let mut ctl = self.control.lock().unwrap();
        ctl.checkpoint_redo = checkpoint_lsn;
        ctl.state = DBState::Shutdowned;
        pg_control::write_control_file(&control_path, &ctl)
            .expect("failed to write pg_control on shutdown");
    }
}

/// Generate a pseudo-random system identifier from timestamp + pid.
fn rand_system_id() -> u64 {
    use std::time::SystemTime;
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    ts ^ (pid << 48)
}

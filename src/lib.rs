use std::sync::Mutex;

pub mod catalog;
pub mod executor;
pub mod parser;
pub mod server;
pub mod storage;
pub mod types;

use catalog::Catalog;
use storage::disk::DiskManager;

pub struct Database {
    pub catalog: Mutex<Catalog>,
    pub disk: Mutex<DiskManager>,
}

impl Database {
    pub fn new(data_dir: &std::path::Path) -> Self {
        Self {
            catalog: Mutex::new(Catalog::new()),
            disk: Mutex::new(DiskManager::new(data_dir)),
        }
    }
}

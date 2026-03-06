use std::sync::{Arc, Mutex};

pub mod catalog;
pub mod executor;
pub mod parser;
pub mod server;
pub mod storage;
pub mod types;

use catalog::Catalog;
use datafusion::execution::context::SessionContext;
use storage::disk::DiskManager;

pub struct Database {
    pub catalog: Mutex<Catalog>,
    pub disk: Arc<Mutex<DiskManager>>,
    pub session: SessionContext,
}

impl Database {
    pub fn new(data_dir: &std::path::Path) -> Self {
        Self {
            catalog: Mutex::new(Catalog::new()),
            disk: Arc::new(Mutex::new(DiskManager::new(data_dir))),
            session: SessionContext::new(),
        }
    }
}

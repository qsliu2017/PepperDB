//! Backend cache subsystem (postgres/src/backend/utils/cache).
//!
//! So far: the attribute-options cache (`attoptcache`), tablespace cache
//! (`spccache`), and relfilenumber->relation map (`relfilenumbermap`). The
//! relcache/catcache/syscache cores are future work.

pub mod lsyscache;
pub mod relcache;
pub mod typcache;
pub mod attoptcache;
pub mod evtcache;
pub mod partcache;
pub mod relfilenumbermap;
pub mod spccache;
pub mod funccache;
pub mod ts_cache;
pub mod syscache;
pub mod relmapper;
pub mod catcache;

//! Translated from PostgreSQL src/include/storage/sinvaladt.h
//!
//! POSTGRES shared cache invalidation data manager.
//!
//! The function bodies live in `crate::backend::storage::ipc::sinvaladt` (the SI
//! ring transport). These are non-type-centric global-state functions, so the
//! header stubs are rewired to `pub use` re-exports (no deprecated shim).

pub use crate::backend::storage::ipc::sinvaladt::{
    SInvalBuffer, SharedInvalBackendInit, SharedInvalShmemSize, cleanup_invalidation_state,
    current_sinval_buffer, shared_inval_shmem_init,
};

// The header declared these with snake_case names; map to the backend symbols.
pub use crate::backend::storage::ipc::sinvaladt::GetNextLocalTransactionId as get_next_local_transaction_id;
pub use crate::backend::storage::ipc::sinvaladt::SICleanupQueue as si_cleanup_queue;
pub use crate::backend::storage::ipc::sinvaladt::SIGetDataEntries as si_get_data_entries;
pub use crate::backend::storage::ipc::sinvaladt::SIInsertDataEntries as si_insert_data_entries;
pub use crate::backend::storage::ipc::sinvaladt::SharedInvalBackendInit as shared_inval_backend_init;

//! Translated from PostgreSQL src/include/storage/procarray.h
//!
//! POSTGRES process array. Under the single-process port the shmem-resident
//! `ProcArrayStruct` collapses to ordinary `Arc`-shared state (`ProcArray` on
//! `SharedState`, reached via `shared.proc_array()`); `ProcArrayLock` becomes
//! that struct's internal `RwLock`.
//!
//! The header declares; the backend module
//! (`backend::storage::ipc::procarray`) defines (rules s2). Most entry points
//! grew a leading `shared: &Arc<SharedState>` (they need the ProcArray + the
//! VariableCache / clog / subtrans), and the recovery/in-progress paths became
//! `async` (clog/subtrans probes). The re-exports below carry those NEW shapes,
//! mirroring how `access::transam` re-exports varsup/transam. `PGPROC` pointers
//! become references; not-found / invalid-xid sentinels become `Option`;
//! pointer out-params become tuples or owned `Vec`s / structs.

// Snapshot/horizon source + replication slot xmins.
pub use crate::backend::storage::ipc::procarray::{
    backend_pid_get_proc, backend_pid_get_proc_with_lock, backend_xid_get_pid, cancel_db_backends,
    cancel_virtual_transaction, count_db_backends, count_db_connections, count_other_db_backends,
    count_user_backends, expire_all_known_assigned_transaction_ids,
    expire_old_known_assigned_transaction_ids, expire_tree_known_assigned_transaction_ids,
    get_conflicting_virtual_xids, get_current_virtual_xids, get_max_snapshot_subxid_count,
    get_max_snapshot_xid_count, get_oldest_active_transaction_id,
    get_oldest_non_removable_transaction_id, get_oldest_safe_decoding_transaction_id,
    get_oldest_transaction_id_considered_running, get_replication_horizons,
    get_running_transaction_data, get_snapshot_data, get_virtual_xids_delaying_chkpt,
    global_vis_test_for, global_vis_test_is_removable_full_xid, global_vis_test_is_removable_xid,
    have_virtual_xids_delaying_chkpt, is_backend_pid,
    known_assigned_transaction_ids_idle_maintenance, minimum_active_backends, proc_array_add,
    proc_array_apply_recovery_info, proc_array_apply_xid_assignment, proc_array_clear_transaction,
    proc_array_end_transaction, proc_array_get_replication_slot_xmin, proc_array_init_recovery,
    proc_array_install_imported_xmin, proc_array_install_restored_xmin, proc_array_remove,
    proc_array_set_replication_slot_xmin, proc_array_shmem_init, proc_array_shmem_size,
    proc_number_get_proc, proc_number_get_transaction_ids, record_known_assigned_transaction_ids,
    recent_xmin, signal_virtual_transaction, snapshot_globals_scope, terminate_other_db_backends,
    transaction_id_is_active, transaction_id_is_in_progress, transaction_xmin,
    xid_cache_remove_running_xids, ComputeXidHorizonsResult, ProcArray, ProcNumberXids,
};

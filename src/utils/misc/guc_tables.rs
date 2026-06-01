//--------------------------------------------------------------------
//
// guc_tables.c -> guc_tables.rs
//
// Static tables for the Grand Unified Configuration scheme.
//
// Many of these tables are const.  However, ConfigureNamesBool[]
// and so on are not, because the structs in those arrays are actually
// the live per-variable state data that guc.c manipulates.  While many of
// their fields are intended to be constant, some fields change at runtime.
//
//
// Copyright (c) 2000-2025, PostgreSQL Global Development Group
// Written by Peter Eisentraut <peter_e@gmx.net>.
//
// IDENTIFICATION
//   src/backend/utils/misc/guc_tables.c
//
//--------------------------------------------------------------------

use std::ffi::{c_char, c_double, c_int};
use std::ptr;

use crate::utils::misc::guc::{
    config_bool, config_enum, config_enum_entry, config_generic, config_group,
    config_group::*,
    config_int, config_real, config_string, config_type,
    GucContext::*,
    GUC_ALLOW_IN_PARALLEL, GUC_DISALLOW_IN_AUTO_FILE, GUC_DISALLOW_IN_FILE, GUC_EXPLAIN,
    GUC_IS_NAME, GUC_LIST_INPUT, GUC_LIST_QUOTE, GUC_NO_RESET, GUC_NO_RESET_ALL,
    GUC_NO_SHOW_ALL, GUC_NOT_IN_SAMPLE, GUC_NOT_WHILE_SEC_REST, GUC_REPORT,
    GUC_RUNTIME_COMPUTED, GUC_SUPERUSER_ONLY, GUC_UNIT_BLOCKS, GUC_UNIT_BYTE, GUC_UNIT_KB,
    GUC_UNIT_MB, GUC_UNIT_MIN, GUC_UNIT_MS, GUC_UNIT_S, GUC_UNIT_XBLOCKS, MAX_KILOBYTES,
};

// C file-scope enum-option tables are `static const struct config_enum_entry[]`
// holding string literal pointers; they are read-only after init.
unsafe impl Sync for config_enum_entry {}

// ---------------------------------------------------------------------------
// External variable declarations (defined elsewhere, accessed by pointer)
// ---------------------------------------------------------------------------

extern "C" {
    // optimizer/paths.h
    static mut enable_seqscan: bool;
    static mut enable_indexscan: bool;
    static mut enable_indexonlyscan: bool;
    static mut enable_bitmapscan: bool;
    static mut enable_tidscan: bool;
    static mut enable_sort: bool;
    static mut enable_incremental_sort: bool;
    static mut enable_hashagg: bool;
    static mut enable_material: bool;
    static mut enable_memoize: bool;
    static mut enable_nestloop: bool;
    static mut enable_mergejoin: bool;
    static mut enable_hashjoin: bool;
    static mut enable_gathermerge: bool;
    static mut enable_partitionwise_join: bool;
    static mut enable_partitionwise_aggregate: bool;
    static mut enable_parallel_append: bool;
    static mut enable_parallel_hash: bool;
    static mut enable_partition_pruning: bool;
    static mut enable_presorted_aggregate: bool;
    static mut enable_async_append: bool;
    static mut enable_self_join_elimination: bool;
    static mut enable_group_by_reordering: bool;
    static mut enable_distinct_reordering: bool;
    // optimizer/geqo.h
    static mut enable_geqo: bool;
    // optimizer/cost.h
    static mut seq_page_cost: c_double;
    static mut random_page_cost: c_double;
    static mut cpu_tuple_cost: c_double;
    static mut cpu_index_tuple_cost: c_double;
    static mut cpu_operator_cost: c_double;
    static mut parallel_tuple_cost: c_double;
    static mut parallel_setup_cost: c_double;
    static mut effective_cache_size: c_int;
    static mut min_parallel_table_scan_size: c_int;
    static mut min_parallel_index_scan_size: c_int;
    static mut jit_above_cost: c_double;
    static mut jit_optimize_above_cost: c_double;
    static mut jit_inline_above_cost: c_double;
    static mut cursor_tuple_fraction: c_double;
    static mut recursive_worktable_factor: c_double;
    // optimizer/geqo.h
    static mut geqo_threshold: c_int;
    static mut Geqo_effort: c_int;
    static mut Geqo_pool_size: c_int;
    static mut Geqo_generations: c_int;
    static mut Geqo_selection_bias: c_double;
    static mut Geqo_seed: c_double;
    // postmaster/postmaster.h
    static mut MaxConnections: c_int;
    static mut SuperuserReservedConnections: c_int;
    static mut ReservedConnections: c_int;
    static mut PostPortNumber: c_int;
    static mut Unix_socket_permissions: c_int;
    static mut Unix_socket_group: *mut c_char;
    static mut Unix_socket_directories: *mut c_char;
    static mut ListenAddresses: *mut c_char;
    static mut enable_bonjour: bool;
    static mut bonjour_name: *mut c_char;
    // storage/bufmgr.h
    static mut NBuffers: c_int;
    static mut VacuumBufferUsageLimit: c_int;
    static mut shared_memory_size_mb: c_int;
    static mut shared_memory_size_in_huge_pages: c_int;
    static mut num_os_semaphores: c_int;
    // access/slru.h style buffer counts
    static mut commit_timestamp_buffers: c_int;
    static mut multixact_member_buffers: c_int;
    static mut multixact_offset_buffers: c_int;
    static mut notify_buffers: c_int;
    static mut serializable_buffers: c_int;
    static mut subtransaction_buffers: c_int;
    static mut transaction_buffers: c_int;
    static mut num_temp_buffers: c_int;
    static mut min_dynamic_shared_memory: c_int;
    // resources
    static mut work_mem: c_int;
    static mut maintenance_work_mem: c_int;
    static mut logical_decoding_work_mem: c_int;
    static mut max_stack_depth: c_int;
    static mut temp_file_limit: c_int;
    static mut hash_mem_multiplier: c_double;
    // vacuum
    static mut VacuumCostPageHit: c_int;
    static mut VacuumCostPageMiss: c_int;
    static mut VacuumCostPageDirty: c_int;
    static mut VacuumCostLimit: c_int;
    static mut VacuumCostDelay: c_double;
    static mut autovacuum_vac_cost_limit: c_int;
    static mut autovacuum_vac_cost_delay: c_double;
    static mut autovacuum_vac_scale: c_double;
    static mut autovacuum_vac_ins_scale: c_double;
    static mut autovacuum_anl_scale: c_double;
    static mut autovacuum_start_daemon: bool;
    static mut autovacuum_naptime: c_int;
    static mut autovacuum_vac_thresh: c_int;
    static mut autovacuum_vac_max_thresh: c_int;
    static mut autovacuum_vac_ins_thresh: c_int;
    static mut autovacuum_anl_thresh: c_int;
    static mut autovacuum_freeze_max_age: c_int;
    static mut autovacuum_multixact_freeze_max_age: c_int;
    static mut autovacuum_worker_slots: c_int;
    static mut autovacuum_max_workers: c_int;
    static mut autovacuum_work_mem: c_int;
    static mut vacuum_freeze_min_age: c_int;
    static mut vacuum_freeze_table_age: c_int;
    static mut vacuum_multixact_freeze_min_age: c_int;
    static mut vacuum_multixact_freeze_table_age: c_int;
    static mut vacuum_failsafe_age: c_int;
    static mut vacuum_multixact_failsafe_age: c_int;
    static mut vacuum_max_eager_freeze_failure_rate: c_double;
    static mut vacuum_truncate: bool;
    // kernel resources
    static mut max_files_per_process: c_int;
    static mut max_prepared_xacts: c_int;
    // lock management
    static mut DeadlockTimeout: c_int;
    static mut max_locks_per_xact: c_int;
    static mut max_predicate_locks_per_xact: c_int;
    static mut max_predicate_locks_per_relation: c_int;
    static mut max_predicate_locks_per_page: c_int;
    // wal
    static mut enableFsync: bool;
    static mut fullPageWrites: bool;
    static mut wal_log_hints: bool;
    static mut wal_init_zero: bool;
    static mut wal_recycle: bool;
    static mut XLOGbuffers: c_int;
    static mut XLogArchiveTimeout: c_int;
    static mut WalWriterDelay: c_int;
    static mut WalWriterFlushAfter: c_int;
    static mut wal_skip_threshold: c_int;
    static mut max_wal_senders: c_int;
    static mut max_replication_slots: c_int;
    static mut max_slot_wal_keep_size_mb: c_int;
    static mut wal_sender_timeout: c_int;
    static mut idle_replication_slot_timeout_secs: c_int;
    static mut CommitDelay: c_int;
    static mut CommitSiblings: c_int;
    static mut wal_keep_size_mb: c_int;
    static mut min_wal_size_mb: c_int;
    static mut max_wal_size_mb: c_int;
    static mut CheckPointTimeout: c_int;
    static mut CheckPointWarning: c_int;
    static mut checkpoint_flush_after: c_int;
    static mut CheckPointCompletionTarget: c_double;
    static mut wal_decode_buffer_size: c_int;
    static mut wal_retrieve_retry_interval: c_int;
    static mut wal_segment_size: c_int;
    static mut wal_summary_keep_time: c_int;
    static mut wal_compression: c_int;
    static mut wal_level: c_int;
    static mut wal_sync_method: c_int;
    static mut wal_consistency_checking_string: *mut c_char;
    static mut wal_block_size: c_int;
    static mut XLogArchiveMode: c_int;
    static mut XLogArchiveCommand: *mut c_char;
    static mut XLogArchiveLibrary: *mut c_char;
    static mut summarize_wal: bool;
    // replication
    static mut max_standby_archive_delay: c_int;
    static mut max_standby_streaming_delay: c_int;
    static mut recovery_min_apply_delay: c_int;
    static mut wal_receiver_status_interval: c_int;
    static mut wal_receiver_timeout: c_int;
    static mut wal_receiver_create_temp_slot: bool;
    static mut recoveryRestoreCommand: *mut c_char;
    static mut archiveCleanupCommand: *mut c_char;
    static mut recoveryEndCommand: *mut c_char;
    static mut recovery_target_timeline_string: *mut c_char;
    static mut recovery_target_string: *mut c_char;
    static mut recovery_target_xid_string: *mut c_char;
    static mut recovery_target_time_string: *mut c_char;
    static mut recovery_target_name_string: *mut c_char;
    static mut recovery_target_lsn_string: *mut c_char;
    static mut recoveryTargetInclusive: bool;
    static mut recoveryTargetAction: c_int;
    static mut recovery_prefetch: c_int;
    static mut recovery_init_sync_method: c_int;
    static mut PrimaryConnInfo: *mut c_char;
    static mut PrimarySlotName: *mut c_char;
    static mut EnableHotStandby: bool;
    static mut hot_standby_feedback: bool;
    static mut sync_replication_slots: bool;
    static mut synchronized_standby_slots: *mut c_char;
    static mut SyncRepStandbyNames: *mut c_char;
    // bgwriter / io
    static mut BgWriterDelay: c_int;
    static mut bgwriter_lru_maxpages: c_int;
    static mut bgwriter_flush_after: c_int;
    static mut bgwriter_lru_multiplier: c_double;
    static mut effective_io_concurrency: c_int;
    static mut maintenance_io_concurrency: c_int;
    static mut io_max_combine_limit: c_int;
    static mut io_combine_limit_guc: c_int;
    static mut io_max_concurrency: c_int;
    static mut io_workers: c_int;
    static mut backend_flush_after: c_int;
    static mut io_method: c_int;
    // worker processes
    static mut max_worker_processes: c_int;
    static mut max_parallel_maintenance_workers: c_int;
    static mut max_parallel_workers_per_gather: c_int;
    static mut max_parallel_workers: c_int;
    static mut parallel_leader_participation: bool;
    static mut max_logical_replication_workers: c_int;
    static mut max_sync_workers_per_subscription: c_int;
    static mut max_parallel_apply_workers_per_subscription: c_int;
    static mut max_active_replication_origins: c_int;
    // logging
    static mut log_checkpoints: bool;
    static mut Log_disconnections: bool;
    static mut log_replication_commands: bool;
    static mut Debug_print_parse: bool;
    static mut Debug_print_rewritten: bool;
    static mut Debug_print_plan: bool;
    static mut Debug_pretty_print: bool;
    static mut log_parser_stats: bool;
    static mut log_planner_stats: bool;
    static mut log_executor_stats: bool;
    static mut log_statement_stats: bool;
    static mut Trace_connection_negotiation: bool;
    static mut log_lock_waits: bool;
    static mut log_lock_failures: bool;
    static mut log_recovery_conflict_waits: bool;
    static mut log_hostname: bool;
    static mut log_min_error_statement: c_int;
    static mut log_min_messages: c_int;
    static mut client_min_messages: c_int;
    static mut log_min_duration_sample: c_int;
    static mut log_min_duration_statement: c_int;
    static mut Log_autovacuum_min_duration: c_int;
    static mut log_parameter_max_length: c_int;
    static mut log_parameter_max_length_on_error: c_int;
    static mut log_temp_files: c_int;
    static mut log_statement_sample_rate: c_double;
    static mut log_xact_sample_rate: c_double;
    static mut Log_error_verbosity: c_int;
    static mut log_statement: c_int;
    static mut Log_line_prefix: *mut c_char;
    static mut Logging_collector: bool;
    static mut Log_truncate_on_rotation: bool;
    static mut Log_RotationAge: c_int;
    static mut Log_RotationSize: c_int;
    static mut Log_file_mode: c_int;
    static mut Log_directory: *mut c_char;
    static mut Log_filename: *mut c_char;
    static mut Log_destination_string: *mut c_char;
    static mut log_timezone_string: *mut c_char;
    static mut log_startup_progress_interval: c_int;
    static mut log_connections_string: *mut c_char;
    // syslog
    static mut syslog_ident_str: *mut c_char;
    static mut syslog_facility: c_int;
    static mut syslog_sequence_numbers: bool;
    static mut syslog_split_messages: bool;
    static mut event_source: *mut c_char;
    // process title
    static mut update_process_title: bool;
    static mut cluster_name: *mut c_char;
    // stats
    static mut pgstat_track_activities: bool;
    static mut pgstat_track_counts: bool;
    static mut track_cost_delay_timing: bool;
    static mut track_io_timing: bool;
    static mut track_wal_io_timing: bool;
    static mut pgstat_track_functions: c_int;
    static mut pgstat_track_activity_query_size: c_int;
    static mut pgstat_fetch_consistency: c_int;
    static mut compute_query_id: c_int;
    // client connection
    static mut StatementTimeout: c_int;
    static mut LockTimeout: c_int;
    static mut IdleInTransactionSessionTimeout: c_int;
    static mut TransactionTimeout: c_int;
    static mut IdleSessionTimeout: c_int;
    static mut tcp_keepalives_idle: c_int;
    static mut tcp_keepalives_interval: c_int;
    static mut tcp_keepalives_count: c_int;
    static mut tcp_user_timeout: c_int;
    static mut client_connection_check_interval: c_int;
    static mut AuthenticationTimeout: c_int;
    static mut PreAuthDelay: c_int;
    static mut PostAuthDelay: c_int;
    static mut scram_sha_256_iterations: c_int;
    static mut client_encoding_string: *mut c_char;
    static mut datestyle_string: *mut c_char;
    static mut server_encoding_string: *mut c_char;
    static mut server_version_string: *mut c_char;
    static mut server_version_num: c_int;
    static mut timezone_string: *mut c_char;
    static mut log_timezone_string_2: *mut c_char; // alias, same var
    static mut timezone_abbreviations_string: *mut c_char;
    static mut default_statistics_target: c_int;
    static mut from_collapse_limit: c_int;
    static mut join_collapse_limit: c_int;
    static mut extra_float_digits: c_int;
    static mut default_table_access_method: *mut c_char;
    static mut default_tablespace: *mut c_char;
    static mut temp_tablespaces: *mut c_char;
    static mut createrole_self_grant: *mut c_char;
    static mut Dynamic_library_path: *mut c_char;
    static mut Extension_control_path: *mut c_char;
    static mut namespace_search_path: *mut c_char;
    static mut gin_pending_list_limit: c_int;
    static mut GinFuzzySearchLimit: c_int;
    static mut data_directory_mode: c_int;
    static mut debug_io_direct_string: *mut c_char;
    static mut restrict_nonsystem_relation_kind_string: *mut c_char;
    static mut oauth_validator_libraries_string: *mut c_char;
    // locale
    static mut locale_messages: *mut c_char;
    static mut locale_monetary: *mut c_char;
    static mut locale_numeric: *mut c_char;
    static mut locale_time: *mut c_char;
    static mut IntervalStyle: c_int;
    static mut icu_validation_level: c_int;
    // preload libraries
    static mut session_preload_libraries_string: *mut c_char;
    static mut shared_preload_libraries_string: *mut c_char;
    static mut local_preload_libraries_string: *mut c_char;
    // text search
    static mut TSCurrentConfig: *mut c_char;
    // ssl
    static mut EnableSSL: bool;
    static mut ssl_passphrase_command_supports_reload: bool;
    static mut SSLPreferServerCiphers: bool;
    static mut ssl_cert_file: *mut c_char;
    static mut ssl_key_file: *mut c_char;
    static mut ssl_ca_file: *mut c_char;
    static mut ssl_crl_file: *mut c_char;
    static mut ssl_crl_dir: *mut c_char;
    static mut ssl_library: *mut c_char;
    static mut SSLCipherSuites: *mut c_char;
    static mut SSLCipherList: *mut c_char;
    static mut SSLECDHCurve: *mut c_char;
    static mut ssl_dh_params_file: *mut c_char;
    static mut ssl_passphrase_command: *mut c_char;
    static mut ssl_min_protocol_version: c_int;
    static mut ssl_max_protocol_version: c_int;
    // kerberos / gss
    static mut pg_krb_server_keyfile: *mut c_char;
    static mut pg_krb_caseins_users: bool;
    static mut pg_gss_accept_delegation: bool;
    // track commit timestamp
    static mut track_commit_timestamp: bool;
    // compat
    static mut Transform_null_equals: bool;
    static mut DefaultXactReadOnly: bool;
    static mut XactReadOnly: bool;
    static mut DefaultXactDeferrable: bool;
    static mut XactDeferrable: bool;
    static mut check_function_bodies: bool;
    static mut Array_nulls: bool;
    static mut escape_string_warning: bool;
    static mut standard_conforming_strings: bool;
    static mut synchronize_seqscans: bool;
    static mut backslash_quote: c_int;
    static mut quote_all_identifiers: bool;
    static mut lo_compat_privileges: bool;
    // error handling
    static mut ExitOnAnyError: bool;
    static mut restart_after_crash: bool;
    static mut remove_temp_files_after_crash: bool;
    static mut send_abort_for_crash: bool;
    static mut send_abort_for_kill: bool;
    static mut data_sync_retry: bool;
    static mut ignore_checksum_failure: bool;
    static mut zero_damaged_pages: bool;
    static mut ignore_invalid_pages: bool;
    // preset / internal
    static mut assert_enabled: bool;
    static mut data_checksums: bool;
    static mut integer_datetimes: bool;
    static mut max_function_args: c_int;
    static mut max_index_keys: c_int;
    static mut max_identifier_length: c_int;
    static mut block_size: c_int;
    static mut segment_size: c_int;
    static mut in_hot_standby_guc: bool;
    // developer
    static mut allow_in_place_tablespaces: bool;
    static mut allowSystemTableMods: bool;
    static mut IgnoreSystemIndexes: bool;
    static mut debug_discard_caches: c_int;
    static mut trace_sort: bool;
    static mut Trace_notify: bool;
    static mut constraint_exclusion: c_int;
    static mut default_statistics_target_2: c_int; // same var
    static mut default_toast_compression: c_int;
    static mut DefaultXactIsoLevel: c_int;
    static mut XactIsoLevel: c_int;
    static mut SessionReplicationRole: c_int;
    static mut synchronous_commit: c_int;
    static mut dynamic_shared_memory_type: c_int;
    static mut shared_memory_type: c_int;
    static mut huge_pages: c_int;
    static mut huge_page_size: c_int;
    static mut huge_pages_status: c_int;
    static mut file_copy_method: c_int;
    static mut file_extend_method: c_int;
    static mut xmlbinary: c_int;
    static mut xmloption: c_int;
    static mut bytea_output: c_int;
    static mut Password_encryption: c_int;
    static mut plan_cache_mode: c_int;
    static mut debug_parallel_query: c_int;
    static mut debug_logical_replication_streaming: c_int;
    // jit
    static mut jit_enabled: bool;
    static mut jit_debugging_support: bool;
    static mut jit_dump_bitcode: bool;
    static mut jit_expressions: bool;
    static mut jit_profiling_support: bool;
    static mut jit_tuple_deforming: bool;
    static mut jit_provider: *mut c_char;
    // role
    pub static mut role_string: *mut c_char;
    static mut session_authorization_string: *mut c_char;
    static mut current_role_is_superuser: bool;
    // allow alter system
    pub static mut AllowAlterSystem: bool;
    // notify
    static mut max_notify_queue_pages: c_int;
    static mut application_name: *mut c_char;
    static mut event_triggers: bool;
    static mut md5_password_warnings: bool;
    static mut pg_stat_activity_idle_in_transaction_timeout: c_int; // unused alias guard
    // missing but referenced
    static mut log_btree_build_stats: bool;
    static mut ssl_renegotiation_limit: c_int;
    static mut data_directory: *mut c_char;
    static mut ConfigFileName: *mut c_char;
    static mut HbaFileName: *mut c_char;
    static mut IdentFileName: *mut c_char;
    static mut external_pid_file: *mut c_char;
    static mut phony_random_seed: c_double;
    // debug node tests (conditionally compiled)
    #[cfg(feature = "debug_node_tests")]
    static mut Debug_copy_parse_plan_trees: bool;
    #[cfg(feature = "debug_node_tests")]
    static mut Debug_write_read_parse_plan_trees: bool;
    #[cfg(feature = "debug_node_tests")]
    static mut Debug_raw_expression_coverage_test: bool;
    // lock debug
    #[cfg(feature = "lock_debug")]
    static mut Trace_locks: bool;
    #[cfg(feature = "lock_debug")]
    static mut Trace_userlocks: bool;
    #[cfg(feature = "lock_debug")]
    static mut Trace_lwlocks: bool;
    #[cfg(feature = "lock_debug")]
    static mut Debug_deadlocks: bool;
    #[cfg(feature = "lock_debug")]
    static mut Trace_lock_oidmin: c_int;
    #[cfg(feature = "lock_debug")]
    static mut Trace_lock_table: c_int;
    // wal debug
    #[cfg(feature = "wal_debug")]
    static mut XLOG_DEBUG: bool;
    // btree build stats
    #[cfg(feature = "btree_build_stats")]
    static mut btree_build_stats: bool;
    // trace syncscan
    #[cfg(feature = "trace_syncscan")]
    static mut trace_syncscan: bool;
    // debug bounded sort
    #[cfg(feature = "debug_bounded_sort")]
    static mut optimize_bounded_sort: bool;
}

// External enum tables defined in other modules (xlog_internal.h etc.)
extern "C" {
    pub static wal_level_options: config_enum_entry;
    pub static archive_mode_options: config_enum_entry;
    pub static recovery_target_action_options: config_enum_entry;
    pub static wal_sync_method_options: config_enum_entry;
    pub static dynamic_shared_memory_options: config_enum_entry;
    pub static io_method_options: config_enum_entry;
}

// ---------------------------------------------------------------------------
// Hook function stubs (TODO(pg-port): defined in guc_hooks.c / scattered)
// ---------------------------------------------------------------------------
extern "C" {
    fn check_bonjour(newval: *mut bool, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_ssl(newval: *mut bool, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_stage_log_stats(newval: *mut bool, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_log_stats(newval: *mut bool, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_default_with_oids(newval: *mut bool, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_max_stack_depth(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_vacuum_buffer_usage_limit(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_commit_ts_buffers(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_multixact_member_buffers(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_multixact_offset_buffers(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_notify_buffers(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_serial_buffers(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_subtrans_buffers(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_transaction_buffers(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_temp_buffers(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_wal_buffers(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_wal_segment_size(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_autovacuum_work_mem(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_io_max_concurrency(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_huge_page_size(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_client_connection_check_interval(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_random_seed(newval: *mut c_double, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_transaction_read_only(newval: *mut bool, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_transaction_deferrable(newval: *mut bool, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_transaction_isolation(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_recovery_prefetch(newval: *mut c_int, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_recovery_target_timeline(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_recovery_target(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_recovery_target_xid(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_recovery_target_time(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_recovery_target_name(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_recovery_target_lsn(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_primary_slot_name(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_client_encoding(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_datestyle(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_default_table_access_method(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_default_tablespace(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_temp_tablespaces(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_createrole_self_grant(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_search_path(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_timezone(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_log_timezone(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_locale_messages(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_locale_monetary(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_locale_numeric(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_locale_time(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_timezone_abbreviations(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_log_destination(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_canonical_path(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_cluster_name(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_synchronous_standby_names(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_default_text_search_config(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_wal_consistency_checking(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_debug_io_direct(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_synchronized_standby_slots(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_restrict_nonsystem_relation_kind(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_backtrace_functions(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_application_name(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_role(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_session_authorization(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    fn check_log_connections(newval: *mut *mut c_char, extra: *mut *mut std::ffi::c_void, source: crate::utils::misc::guc::GucSource) -> bool;
    // assign hooks
    fn assign_max_stack_depth(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_transaction_timeout(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_max_wal_size(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_io_max_combine_limit(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_io_combine_limit(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_maintenance_io_concurrency(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_recovery_prefetch(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_recovery_target_timeline(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_recovery_target(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_recovery_target_xid(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_recovery_target_time(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_recovery_target_name(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_recovery_target_lsn(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_client_encoding(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_datestyle(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_temp_tablespaces(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_createrole_self_grant(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_search_path(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_timezone(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_log_timezone(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_locale_messages(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_locale_monetary(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_locale_numeric(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_locale_time(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_timezone_abbreviations(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_log_destination(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_synchronous_standby_names(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_default_text_search_config(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_wal_consistency_checking(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_debug_io_direct(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_synchronized_standby_slots(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_restrict_nonsystem_relation_kind(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_backtrace_functions(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_application_name(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_role(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_session_replication_role(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_session_authorization(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_tcp_keepalives_idle(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_tcp_keepalives_interval(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_tcp_keepalives_count(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_tcp_user_timeout(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_random_seed(newval: c_double, extra: *mut std::ffi::c_void);
    fn assign_synchronous_commit(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_wal_sync_method(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_syslog_ident(newval: *const c_char, extra: *mut std::ffi::c_void);
    fn assign_syslog_facility(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_checkpoint_completion_target(newval: c_double, extra: *mut std::ffi::c_void);
    fn assign_stats_fetch_consistency(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_io_method(newval: c_int, extra: *mut std::ffi::c_void);
    fn assign_log_connections(newval: *const c_char, extra: *mut std::ffi::c_void);
    // show hooks
    fn show_in_hot_standby() -> *const c_char;
    fn show_unix_socket_permissions() -> *const c_char;
    fn show_log_file_mode() -> *const c_char;
    fn show_data_directory_mode() -> *const c_char;
    fn show_archive_command() -> *const c_char;
    fn show_log_timezone() -> *const c_char;
    fn show_timezone() -> *const c_char;
    fn show_random_seed() -> *const c_char;
    fn show_tcp_keepalives_idle() -> *const c_char;
    fn show_tcp_keepalives_interval() -> *const c_char;
    fn show_tcp_keepalives_count() -> *const c_char;
    fn show_tcp_user_timeout() -> *const c_char;
    fn show_role() -> *const c_char;
}

// ---------------------------------------------------------------------------
// Compile-time constants that C macros expand to
// ---------------------------------------------------------------------------

// These mirror C constants from various headers; values kept identical.
const BYTEA_OUTPUT_ESCAPE: c_int = 0;
const BYTEA_OUTPUT_HEX: c_int = 1;
const DEBUG5: c_int = 10;
const DEBUG4: c_int = 11;
const DEBUG3: c_int = 12;
const DEBUG2: c_int = 13;
const DEBUG1: c_int = 14;
const LOG: c_int = 15;
const LOG_SERVER_ONLY: c_int = 16;
const INFO: c_int = 17;
const NOTICE: c_int = 18;
const WARNING: c_int = 19;
const ERROR: c_int = 20;
const FATAL: c_int = 21;
const PANIC: c_int = 22;
const INTSTYLE_POSTGRES: c_int = 0;
const INTSTYLE_POSTGRES_VERBOSE: c_int = 1;
const INTSTYLE_SQL_STANDARD: c_int = 2;
const INTSTYLE_ISO_8601: c_int = 3;
const PGERROR_TERSE: c_int = 0;
const PGERROR_DEFAULT: c_int = 1;
const PGERROR_VERBOSE: c_int = 2;
const LOGSTMT_NONE: c_int = 0;
const LOGSTMT_DDL: c_int = 1;
const LOGSTMT_MOD: c_int = 2;
const LOGSTMT_ALL: c_int = 3;
const XACT_SERIALIZABLE: c_int = 3;
const XACT_REPEATABLE_READ: c_int = 2;
const XACT_READ_COMMITTED: c_int = 1;
const XACT_READ_UNCOMMITTED: c_int = 0;
const SESSION_REPLICATION_ROLE_ORIGIN: c_int = 0;
const SESSION_REPLICATION_ROLE_REPLICA: c_int = 1;
const SESSION_REPLICATION_ROLE_LOCAL: c_int = 2;
const TRACK_FUNC_OFF: c_int = 0;
const TRACK_FUNC_PL: c_int = 1;
const TRACK_FUNC_ALL: c_int = 2;
const PGSTAT_FETCH_CONSISTENCY_NONE: c_int = 0;
const PGSTAT_FETCH_CONSISTENCY_CACHE: c_int = 1;
const PGSTAT_FETCH_CONSISTENCY_SNAPSHOT: c_int = 2;
const XMLBINARY_BASE64: c_int = 0;
const XMLBINARY_HEX: c_int = 1;
const XMLOPTION_CONTENT: c_int = 0;
const XMLOPTION_DOCUMENT: c_int = 1;
const BACKSLASH_QUOTE_SAFE_ENCODING: c_int = 0;
const BACKSLASH_QUOTE_ON: c_int = 1;
const BACKSLASH_QUOTE_OFF: c_int = 2;
const COMPUTE_QUERY_ID_AUTO: c_int = 0;
const COMPUTE_QUERY_ID_REGRESS: c_int = 1;
const COMPUTE_QUERY_ID_ON: c_int = 2;
const COMPUTE_QUERY_ID_OFF: c_int = 3;
const CONSTRAINT_EXCLUSION_PARTITION: c_int = 0;
const CONSTRAINT_EXCLUSION_ON: c_int = 1;
const CONSTRAINT_EXCLUSION_OFF: c_int = 2;
const SYNCHRONOUS_COMMIT_LOCAL_FLUSH: c_int = 1;
const SYNCHRONOUS_COMMIT_REMOTE_WRITE: c_int = 2;
const SYNCHRONOUS_COMMIT_REMOTE_APPLY: c_int = 3;
const SYNCHRONOUS_COMMIT_ON: c_int = 4;
const SYNCHRONOUS_COMMIT_OFF: c_int = 0;
const HUGE_PAGES_OFF: c_int = 0;
const HUGE_PAGES_ON: c_int = 1;
const HUGE_PAGES_TRY: c_int = 2;
const HUGE_PAGES_UNKNOWN: c_int = 3;
const RECOVERY_PREFETCH_OFF: c_int = 0;
const RECOVERY_PREFETCH_ON: c_int = 1;
const RECOVERY_PREFETCH_TRY: c_int = 2;
const DEBUG_PARALLEL_OFF: c_int = 0;
const DEBUG_PARALLEL_ON: c_int = 1;
const DEBUG_PARALLEL_REGRESS: c_int = 2;
const PLAN_CACHE_MODE_AUTO: c_int = 0;
const PLAN_CACHE_MODE_FORCE_GENERIC_PLAN: c_int = 1;
const PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN: c_int = 2;
const PASSWORD_TYPE_MD5: c_int = 0;
const PASSWORD_TYPE_SCRAM_SHA_256: c_int = 1;
const PG_TLS_ANY: c_int = 0;
const PG_TLS1_VERSION: c_int = 1;
const PG_TLS1_1_VERSION: c_int = 2;
const PG_TLS1_2_VERSION: c_int = 3;
const PG_TLS1_3_VERSION: c_int = 4;
const DEBUG_LOGICAL_REP_STREAMING_BUFFERED: c_int = 0;
const DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE: c_int = 1;
const DATA_DIR_SYNC_METHOD_FSYNC: c_int = 0;
const DATA_DIR_SYNC_METHOD_SYNCFS: c_int = 1;
const SHMEM_TYPE_SYSV: c_int = 0;
const SHMEM_TYPE_MMAP: c_int = 1;
const SHMEM_TYPE_WINDOWS: c_int = 2;
const TOAST_PGLZ_COMPRESSION: c_int = 0;
const TOAST_LZ4_COMPRESSION: c_int = 1;
const WAL_COMPRESSION_NONE: c_int = 0;
const WAL_COMPRESSION_PGLZ: c_int = 1;
const WAL_COMPRESSION_LZ4: c_int = 2;
const WAL_COMPRESSION_ZSTD: c_int = 3;
const WAL_LEVEL_REPLICA: c_int = 2;
const FILE_COPY_METHOD_COPY: c_int = 0;
const FILE_COPY_METHOD_CLONE: c_int = 1;
const FILE_EXTEND_METHOD_POSIX_FALLOCATE: c_int = 0;
const FILE_EXTEND_METHOD_WRITE_ZEROS: c_int = 1;
// DEFAULT_FILE_EXTEND_METHOD: platform conditional; use write_zeros as conservative default
#[cfg(not(target_os = "linux"))]
const DEFAULT_FILE_EXTEND_METHOD: c_int = FILE_EXTEND_METHOD_WRITE_ZEROS;
#[cfg(target_os = "linux")]
const DEFAULT_FILE_EXTEND_METHOD: c_int = FILE_EXTEND_METHOD_POSIX_FALLOCATE;
// DEFAULT_SYSLOG_FACILITY: platform conditional
#[cfg(target_family = "unix")]
const DEFAULT_SYSLOG_FACILITY: c_int = 128; /* LOG_LOCAL0 on Linux */
#[cfg(not(target_family = "unix"))]
const DEFAULT_SYSLOG_FACILITY: c_int = 0;

const ARCHIVE_MODE_OFF: c_int = 0;
const RECOVERY_TARGET_ACTION_PAUSE: c_int = 0;

// Numeric constants from various headers
const INT_MAX: c_int = c_int::MAX;
const BLCKSZ: c_int = 8192;
const XLOG_BLCKSZ: c_int = 8192;
const RELSEG_SIZE: c_int = 131072; // blocks per segment (default 1GB / BLCKSZ)
const FUNC_MAX_ARGS: c_int = 100;
const INDEX_MAX_KEYS: c_int = 32;
const NAMEDATALEN: c_int = 64;
const MAX_BACKENDS: c_int = 262143; // MaxBackends ceiling
const MAX_PARALLEL_WORKER_LIMIT: c_int = 1024;
const MAX_IO_CONCURRENCY: c_int = 1000;
const MAX_IO_COMBINE_LIMIT: c_int = 128;
const DEFAULT_IO_COMBINE_LIMIT: c_int = 128;
const MAX_IO_WORKERS: c_int = 128;
const MAX_STATISTICS_TARGET: c_int = 10000;
const SLRU_MAX_ALLOWED_BUFFERS: c_int = 131072;
const MAX_BAS_VAC_RING_SIZE_KB: c_int = 131072;
const WRITEBACK_MAX_PENDING_FLUSHES: c_int = 256;
const DEFAULT_BGWRITER_FLUSH_AFTER: c_int = 64;
const DEFAULT_CHECKPOINT_FLUSH_AFTER: c_int = 32;
const DEFAULT_WAL_WRITER_FLUSH_AFTER: c_int = 128;
const DEFAULT_BACKEND_FLUSH_AFTER: c_int = 0;
const DEFAULT_EFFECTIVE_IO_CONCURRENCY: c_int = 1;
const DEFAULT_MAINTENANCE_IO_CONCURRENCY: c_int = 10;
const SECS_PER_MINUTE: c_int = 60;
const MINS_PER_HOUR: c_int = 60;
const HOURS_PER_DAY: c_int = 24;
const DEF_PGPORT: c_int = 5432;
const PG_VERSION_NUM: c_int = 180003;
const DEFAULT_MIN_WAL_SEGS: c_int = 5;
const DEFAULT_MAX_WAL_SEGS: c_int = 64;
const DEFAULT_XLOG_SEG_SIZE: c_int = 16777216; // 16 MB
const WalSegMinSize: c_int = 1048576;
const WalSegMaxSize: c_int = 1073741824;
const MaxAllocSize: c_int = 1073741823; // UINT32_MAX / 2 - 1
const SCRAM_SHA_256_DEFAULT_ITERATIONS: c_int = 4096;
const DEFAULT_GEQO_EFFORT: c_int = 5;
const MIN_GEQO_EFFORT: c_int = 1;
const MAX_GEQO_EFFORT: c_int = 10;
const DEFAULT_GEQO_SELECTION_BIAS: c_double = 2.0;
const MIN_GEQO_SELECTION_BIAS: c_double = 1.5;
const MAX_GEQO_SELECTION_BIAS: c_double = 2.0;
const DEFAULT_EFFECTIVE_CACHE_SIZE: c_int = 524288; // 4GB / BLCKSZ
const DEFAULT_SEQ_PAGE_COST: c_double = 1.0;
const DEFAULT_RANDOM_PAGE_COST: c_double = 4.0;
const DEFAULT_CPU_TUPLE_COST: c_double = 0.01;
const DEFAULT_CPU_INDEX_TUPLE_COST: c_double = 0.005;
const DEFAULT_CPU_OPERATOR_COST: c_double = 0.0025;
const DEFAULT_PARALLEL_TUPLE_COST: c_double = 0.1;
const DEFAULT_PARALLEL_SETUP_COST: c_double = 1000.0;
const DEFAULT_CURSOR_TUPLE_FRACTION: c_double = 0.1;
const DEFAULT_RECURSIVE_WORKTABLE_FACTOR: c_double = 10.0;
const DEFAULT_UPDATE_PROCESS_TITLE: bool = true;
const DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE: c_int = 1; // POSIX mmap = default
const DEFAULT_SHARED_MEMORY_TYPE: c_int = 1;         // SHMEM_TYPE_MMAP
const DEFAULT_WAL_SYNC_METHOD: c_int = 0;            // WAL_SYNC_METHOD_OPEN (platform dep)
const DEFAULT_EVENT_SOURCE: *const c_char = b"PostgreSQL\0".as_ptr() as *const c_char;
const DEFAULT_IO_METHOD: c_int = 0;
const DEFAULT_PGSOCKET_DIR: *const c_char = b"/tmp\0".as_ptr() as *const c_char;
const DEFAULT_TABLE_ACCESS_METHOD: *const c_char = b"heap\0".as_ptr() as *const c_char;
const PG_VERSION: *const c_char = b"18.3\0".as_ptr() as *const c_char;
// PG_KRB_SRVTAB default
const PG_KRB_SRVTAB: *const c_char = b"\0".as_ptr() as *const c_char;
const FirstNormalObjectId: c_int = 16384;

// ---------------------------------------------------------------------------
// Options for enum values (equivalent to static const struct config_enum_entry[])
// ---------------------------------------------------------------------------

// NOTE: arrays must be null-terminated with { NULL, 0, false } sentinel.
// We represent *const c_char fields as pointer casts from &[u8] literals.

macro_rules! cstr {
    ($s:literal) => {
        concat!($s, "\0").as_ptr() as *const c_char
    };
}

macro_rules! cfg_enum_null {
    () => {
        config_enum_entry { name: ptr::null(), val: 0, hidden: false }
    };
}

static bytea_output_options: [config_enum_entry; 3] = [
    config_enum_entry { name: cstr!("escape"), val: BYTEA_OUTPUT_ESCAPE, hidden: false },
    config_enum_entry { name: cstr!("hex"),    val: BYTEA_OUTPUT_HEX,    hidden: false },
    cfg_enum_null!(),
];

/*
 * We have different sets for client and server message level options because
 * they sort slightly different (see "log" level), and because "fatal"/"panic"
 * aren't sensible for client_min_messages.
 */
static client_message_level_options: [config_enum_entry; 12] = [
    config_enum_entry { name: cstr!("debug5"),   val: DEBUG5,   hidden: false },
    config_enum_entry { name: cstr!("debug4"),   val: DEBUG4,   hidden: false },
    config_enum_entry { name: cstr!("debug3"),   val: DEBUG3,   hidden: false },
    config_enum_entry { name: cstr!("debug2"),   val: DEBUG2,   hidden: false },
    config_enum_entry { name: cstr!("debug1"),   val: DEBUG1,   hidden: false },
    config_enum_entry { name: cstr!("debug"),    val: DEBUG2,   hidden: true  },
    config_enum_entry { name: cstr!("log"),      val: LOG,      hidden: false },
    config_enum_entry { name: cstr!("info"),     val: INFO,     hidden: true  },
    config_enum_entry { name: cstr!("notice"),   val: NOTICE,   hidden: false },
    config_enum_entry { name: cstr!("warning"),  val: WARNING,  hidden: false },
    config_enum_entry { name: cstr!("error"),    val: ERROR,    hidden: false },
    cfg_enum_null!(),
];

static server_message_level_options: [config_enum_entry; 14] = [
    config_enum_entry { name: cstr!("debug5"),   val: DEBUG5,   hidden: false },
    config_enum_entry { name: cstr!("debug4"),   val: DEBUG4,   hidden: false },
    config_enum_entry { name: cstr!("debug3"),   val: DEBUG3,   hidden: false },
    config_enum_entry { name: cstr!("debug2"),   val: DEBUG2,   hidden: false },
    config_enum_entry { name: cstr!("debug1"),   val: DEBUG1,   hidden: false },
    config_enum_entry { name: cstr!("debug"),    val: DEBUG2,   hidden: true  },
    config_enum_entry { name: cstr!("info"),     val: INFO,     hidden: false },
    config_enum_entry { name: cstr!("notice"),   val: NOTICE,   hidden: false },
    config_enum_entry { name: cstr!("warning"),  val: WARNING,  hidden: false },
    config_enum_entry { name: cstr!("error"),    val: ERROR,    hidden: false },
    config_enum_entry { name: cstr!("log"),      val: LOG,      hidden: false },
    config_enum_entry { name: cstr!("fatal"),    val: FATAL,    hidden: false },
    config_enum_entry { name: cstr!("panic"),    val: PANIC,    hidden: false },
    cfg_enum_null!(),
];

static intervalstyle_options: [config_enum_entry; 5] = [
    config_enum_entry { name: cstr!("postgres"),          val: INTSTYLE_POSTGRES,         hidden: false },
    config_enum_entry { name: cstr!("postgres_verbose"),   val: INTSTYLE_POSTGRES_VERBOSE, hidden: false },
    config_enum_entry { name: cstr!("sql_standard"),      val: INTSTYLE_SQL_STANDARD,     hidden: false },
    config_enum_entry { name: cstr!("iso_8601"),           val: INTSTYLE_ISO_8601,         hidden: false },
    cfg_enum_null!(),
];

static icu_validation_level_options: [config_enum_entry; 12] = [
    config_enum_entry { name: cstr!("disabled"), val: -1,       hidden: false },
    config_enum_entry { name: cstr!("debug5"),   val: DEBUG5,   hidden: false },
    config_enum_entry { name: cstr!("debug4"),   val: DEBUG4,   hidden: false },
    config_enum_entry { name: cstr!("debug3"),   val: DEBUG3,   hidden: false },
    config_enum_entry { name: cstr!("debug2"),   val: DEBUG2,   hidden: false },
    config_enum_entry { name: cstr!("debug1"),   val: DEBUG1,   hidden: false },
    config_enum_entry { name: cstr!("debug"),    val: DEBUG2,   hidden: true  },
    config_enum_entry { name: cstr!("log"),      val: LOG,      hidden: false },
    config_enum_entry { name: cstr!("info"),     val: INFO,     hidden: true  },
    config_enum_entry { name: cstr!("notice"),   val: NOTICE,   hidden: false },
    config_enum_entry { name: cstr!("warning"),  val: WARNING,  hidden: false },
    cfg_enum_null!(),
];

static log_error_verbosity_options: [config_enum_entry; 4] = [
    config_enum_entry { name: cstr!("terse"),   val: PGERROR_TERSE,   hidden: false },
    config_enum_entry { name: cstr!("default"), val: PGERROR_DEFAULT, hidden: false },
    config_enum_entry { name: cstr!("verbose"), val: PGERROR_VERBOSE, hidden: false },
    cfg_enum_null!(),
];

static log_statement_options: [config_enum_entry; 5] = [
    config_enum_entry { name: cstr!("none"), val: LOGSTMT_NONE, hidden: false },
    config_enum_entry { name: cstr!("ddl"),  val: LOGSTMT_DDL,  hidden: false },
    config_enum_entry { name: cstr!("mod"),  val: LOGSTMT_MOD,  hidden: false },
    config_enum_entry { name: cstr!("all"),  val: LOGSTMT_ALL,  hidden: false },
    cfg_enum_null!(),
];

static isolation_level_options: [config_enum_entry; 5] = [
    config_enum_entry { name: cstr!("serializable"),    val: XACT_SERIALIZABLE,    hidden: false },
    config_enum_entry { name: cstr!("repeatable read"), val: XACT_REPEATABLE_READ, hidden: false },
    config_enum_entry { name: cstr!("read committed"),  val: XACT_READ_COMMITTED,  hidden: false },
    config_enum_entry { name: cstr!("read uncommitted"),val: XACT_READ_UNCOMMITTED, hidden: false },
    cfg_enum_null!(),
];

static session_replication_role_options: [config_enum_entry; 4] = [
    config_enum_entry { name: cstr!("origin"),  val: SESSION_REPLICATION_ROLE_ORIGIN,  hidden: false },
    config_enum_entry { name: cstr!("replica"), val: SESSION_REPLICATION_ROLE_REPLICA, hidden: false },
    config_enum_entry { name: cstr!("local"),   val: SESSION_REPLICATION_ROLE_LOCAL,   hidden: false },
    cfg_enum_null!(),
];

// syslog_facility_options: HAVE_SYSLOG conditional
// On Unix we include local0-7; on non-Unix just "none".
#[cfg(target_family = "unix")]
static syslog_facility_options: [config_enum_entry; 9] = [
    config_enum_entry { name: cstr!("local0"), val: 128, hidden: false },
    config_enum_entry { name: cstr!("local1"), val: 136, hidden: false },
    config_enum_entry { name: cstr!("local2"), val: 144, hidden: false },
    config_enum_entry { name: cstr!("local3"), val: 152, hidden: false },
    config_enum_entry { name: cstr!("local4"), val: 160, hidden: false },
    config_enum_entry { name: cstr!("local5"), val: 168, hidden: false },
    config_enum_entry { name: cstr!("local6"), val: 176, hidden: false },
    config_enum_entry { name: cstr!("local7"), val: 184, hidden: false },
    cfg_enum_null!(),
];
#[cfg(not(target_family = "unix"))]
static syslog_facility_options: [config_enum_entry; 2] = [
    config_enum_entry { name: cstr!("none"), val: 0, hidden: false },
    cfg_enum_null!(),
];

static track_function_options: [config_enum_entry; 4] = [
    config_enum_entry { name: cstr!("none"), val: TRACK_FUNC_OFF, hidden: false },
    config_enum_entry { name: cstr!("pl"),   val: TRACK_FUNC_PL,  hidden: false },
    config_enum_entry { name: cstr!("all"),  val: TRACK_FUNC_ALL, hidden: false },
    cfg_enum_null!(),
];

static stats_fetch_consistency: [config_enum_entry; 4] = [
    config_enum_entry { name: cstr!("none"),     val: PGSTAT_FETCH_CONSISTENCY_NONE,     hidden: false },
    config_enum_entry { name: cstr!("cache"),    val: PGSTAT_FETCH_CONSISTENCY_CACHE,    hidden: false },
    config_enum_entry { name: cstr!("snapshot"), val: PGSTAT_FETCH_CONSISTENCY_SNAPSHOT, hidden: false },
    cfg_enum_null!(),
];

static xmlbinary_options: [config_enum_entry; 3] = [
    config_enum_entry { name: cstr!("base64"), val: XMLBINARY_BASE64, hidden: false },
    config_enum_entry { name: cstr!("hex"),    val: XMLBINARY_HEX,    hidden: false },
    cfg_enum_null!(),
];

static xmloption_options: [config_enum_entry; 3] = [
    config_enum_entry { name: cstr!("content"),  val: XMLOPTION_CONTENT,  hidden: false },
    config_enum_entry { name: cstr!("document"), val: XMLOPTION_DOCUMENT, hidden: false },
    cfg_enum_null!(),
];

/*
 * Although only "on", "off", and "safe_encoding" are documented, we
 * accept all the likely variants of "on" and "off".
 */
static backslash_quote_options: [config_enum_entry; 10] = [
    config_enum_entry { name: cstr!("safe_encoding"), val: BACKSLASH_QUOTE_SAFE_ENCODING, hidden: false },
    config_enum_entry { name: cstr!("on"),    val: BACKSLASH_QUOTE_ON,  hidden: false },
    config_enum_entry { name: cstr!("off"),   val: BACKSLASH_QUOTE_OFF, hidden: false },
    config_enum_entry { name: cstr!("true"),  val: BACKSLASH_QUOTE_ON,  hidden: true  },
    config_enum_entry { name: cstr!("false"), val: BACKSLASH_QUOTE_OFF, hidden: true  },
    config_enum_entry { name: cstr!("yes"),   val: BACKSLASH_QUOTE_ON,  hidden: true  },
    config_enum_entry { name: cstr!("no"),    val: BACKSLASH_QUOTE_OFF, hidden: true  },
    config_enum_entry { name: cstr!("1"),     val: BACKSLASH_QUOTE_ON,  hidden: true  },
    config_enum_entry { name: cstr!("0"),     val: BACKSLASH_QUOTE_OFF, hidden: true  },
    cfg_enum_null!(),
];

/*
 * Although only "on", "off", and "auto" are documented, we accept
 * all the likely variants of "on" and "off".
 */
static compute_query_id_options: [config_enum_entry; 11] = [
    config_enum_entry { name: cstr!("auto"),    val: COMPUTE_QUERY_ID_AUTO,    hidden: false },
    config_enum_entry { name: cstr!("regress"), val: COMPUTE_QUERY_ID_REGRESS, hidden: false },
    config_enum_entry { name: cstr!("on"),      val: COMPUTE_QUERY_ID_ON,      hidden: false },
    config_enum_entry { name: cstr!("off"),     val: COMPUTE_QUERY_ID_OFF,     hidden: false },
    config_enum_entry { name: cstr!("true"),    val: COMPUTE_QUERY_ID_ON,      hidden: true  },
    config_enum_entry { name: cstr!("false"),   val: COMPUTE_QUERY_ID_OFF,     hidden: true  },
    config_enum_entry { name: cstr!("yes"),     val: COMPUTE_QUERY_ID_ON,      hidden: true  },
    config_enum_entry { name: cstr!("no"),      val: COMPUTE_QUERY_ID_OFF,     hidden: true  },
    config_enum_entry { name: cstr!("1"),       val: COMPUTE_QUERY_ID_ON,      hidden: true  },
    config_enum_entry { name: cstr!("0"),       val: COMPUTE_QUERY_ID_OFF,     hidden: true  },
    cfg_enum_null!(),
];

/*
 * Although only "on", "off", and "partition" are documented, we
 * accept all the likely variants of "on" and "off".
 */
static constraint_exclusion_options: [config_enum_entry; 10] = [
    config_enum_entry { name: cstr!("partition"), val: CONSTRAINT_EXCLUSION_PARTITION, hidden: false },
    config_enum_entry { name: cstr!("on"),    val: CONSTRAINT_EXCLUSION_ON,  hidden: false },
    config_enum_entry { name: cstr!("off"),   val: CONSTRAINT_EXCLUSION_OFF, hidden: false },
    config_enum_entry { name: cstr!("true"),  val: CONSTRAINT_EXCLUSION_ON,  hidden: true  },
    config_enum_entry { name: cstr!("false"), val: CONSTRAINT_EXCLUSION_OFF, hidden: true  },
    config_enum_entry { name: cstr!("yes"),   val: CONSTRAINT_EXCLUSION_ON,  hidden: true  },
    config_enum_entry { name: cstr!("no"),    val: CONSTRAINT_EXCLUSION_OFF, hidden: true  },
    config_enum_entry { name: cstr!("1"),     val: CONSTRAINT_EXCLUSION_ON,  hidden: true  },
    config_enum_entry { name: cstr!("0"),     val: CONSTRAINT_EXCLUSION_OFF, hidden: true  },
    cfg_enum_null!(),
];

/*
 * Although only "on", "off", "remote_apply", "remote_write", and "local" are
 * documented, we accept all the likely variants of "on" and "off".
 */
static synchronous_commit_options: [config_enum_entry; 12] = [
    config_enum_entry { name: cstr!("local"),         val: SYNCHRONOUS_COMMIT_LOCAL_FLUSH,  hidden: false },
    config_enum_entry { name: cstr!("remote_write"),  val: SYNCHRONOUS_COMMIT_REMOTE_WRITE, hidden: false },
    config_enum_entry { name: cstr!("remote_apply"),  val: SYNCHRONOUS_COMMIT_REMOTE_APPLY, hidden: false },
    config_enum_entry { name: cstr!("on"),    val: SYNCHRONOUS_COMMIT_ON,  hidden: false },
    config_enum_entry { name: cstr!("off"),   val: SYNCHRONOUS_COMMIT_OFF, hidden: false },
    config_enum_entry { name: cstr!("true"),  val: SYNCHRONOUS_COMMIT_ON,  hidden: true  },
    config_enum_entry { name: cstr!("false"), val: SYNCHRONOUS_COMMIT_OFF, hidden: true  },
    config_enum_entry { name: cstr!("yes"),   val: SYNCHRONOUS_COMMIT_ON,  hidden: true  },
    config_enum_entry { name: cstr!("no"),    val: SYNCHRONOUS_COMMIT_OFF, hidden: true  },
    config_enum_entry { name: cstr!("1"),     val: SYNCHRONOUS_COMMIT_ON,  hidden: true  },
    config_enum_entry { name: cstr!("0"),     val: SYNCHRONOUS_COMMIT_OFF, hidden: true  },
    cfg_enum_null!(),
];

/*
 * Although only "on", "off", "try" are documented, we accept all the likely
 * variants of "on" and "off".
 */
static huge_pages_options: [config_enum_entry; 10] = [
    config_enum_entry { name: cstr!("off"), val: HUGE_PAGES_OFF, hidden: false },
    config_enum_entry { name: cstr!("on"),  val: HUGE_PAGES_ON,  hidden: false },
    config_enum_entry { name: cstr!("try"), val: HUGE_PAGES_TRY, hidden: false },
    config_enum_entry { name: cstr!("true"),  val: HUGE_PAGES_ON,  hidden: true },
    config_enum_entry { name: cstr!("false"), val: HUGE_PAGES_OFF, hidden: true },
    config_enum_entry { name: cstr!("yes"),   val: HUGE_PAGES_ON,  hidden: true },
    config_enum_entry { name: cstr!("no"),    val: HUGE_PAGES_OFF, hidden: true },
    config_enum_entry { name: cstr!("1"),     val: HUGE_PAGES_ON,  hidden: true },
    config_enum_entry { name: cstr!("0"),     val: HUGE_PAGES_OFF, hidden: true },
    cfg_enum_null!(),
];

static huge_pages_status_options: [config_enum_entry; 4] = [
    config_enum_entry { name: cstr!("off"),     val: HUGE_PAGES_OFF,     hidden: false },
    config_enum_entry { name: cstr!("on"),      val: HUGE_PAGES_ON,      hidden: false },
    config_enum_entry { name: cstr!("unknown"), val: HUGE_PAGES_UNKNOWN, hidden: false },
    cfg_enum_null!(),
];

static recovery_prefetch_options: [config_enum_entry; 10] = [
    config_enum_entry { name: cstr!("off"), val: RECOVERY_PREFETCH_OFF, hidden: false },
    config_enum_entry { name: cstr!("on"),  val: RECOVERY_PREFETCH_ON,  hidden: false },
    config_enum_entry { name: cstr!("try"), val: RECOVERY_PREFETCH_TRY, hidden: false },
    config_enum_entry { name: cstr!("true"),  val: RECOVERY_PREFETCH_ON,  hidden: true },
    config_enum_entry { name: cstr!("false"), val: RECOVERY_PREFETCH_OFF, hidden: true },
    config_enum_entry { name: cstr!("yes"),   val: RECOVERY_PREFETCH_ON,  hidden: true },
    config_enum_entry { name: cstr!("no"),    val: RECOVERY_PREFETCH_OFF, hidden: true },
    config_enum_entry { name: cstr!("1"),     val: RECOVERY_PREFETCH_ON,  hidden: true },
    config_enum_entry { name: cstr!("0"),     val: RECOVERY_PREFETCH_OFF, hidden: true },
    cfg_enum_null!(),
];

static debug_parallel_query_options: [config_enum_entry; 10] = [
    config_enum_entry { name: cstr!("off"),     val: DEBUG_PARALLEL_OFF,     hidden: false },
    config_enum_entry { name: cstr!("on"),      val: DEBUG_PARALLEL_ON,      hidden: false },
    config_enum_entry { name: cstr!("regress"), val: DEBUG_PARALLEL_REGRESS, hidden: false },
    config_enum_entry { name: cstr!("true"),  val: DEBUG_PARALLEL_ON,  hidden: true },
    config_enum_entry { name: cstr!("false"), val: DEBUG_PARALLEL_OFF, hidden: true },
    config_enum_entry { name: cstr!("yes"),   val: DEBUG_PARALLEL_ON,  hidden: true },
    config_enum_entry { name: cstr!("no"),    val: DEBUG_PARALLEL_OFF, hidden: true },
    config_enum_entry { name: cstr!("1"),     val: DEBUG_PARALLEL_ON,  hidden: true },
    config_enum_entry { name: cstr!("0"),     val: DEBUG_PARALLEL_OFF, hidden: true },
    cfg_enum_null!(),
];

static plan_cache_mode_options: [config_enum_entry; 4] = [
    config_enum_entry { name: cstr!("auto"),               val: PLAN_CACHE_MODE_AUTO,               hidden: false },
    config_enum_entry { name: cstr!("force_generic_plan"), val: PLAN_CACHE_MODE_FORCE_GENERIC_PLAN, hidden: false },
    config_enum_entry { name: cstr!("force_custom_plan"),  val: PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN,  hidden: false },
    cfg_enum_null!(),
];

static password_encryption_options: [config_enum_entry; 3] = [
    config_enum_entry { name: cstr!("md5"),         val: PASSWORD_TYPE_MD5,          hidden: false },
    config_enum_entry { name: cstr!("scram-sha-256"),val: PASSWORD_TYPE_SCRAM_SHA_256, hidden: false },
    cfg_enum_null!(),
];

static ssl_protocol_versions_info: [config_enum_entry; 6] = [
    config_enum_entry { name: cstr!(""),       val: PG_TLS_ANY,        hidden: false },
    config_enum_entry { name: cstr!("TLSv1"),  val: PG_TLS1_VERSION,   hidden: false },
    config_enum_entry { name: cstr!("TLSv1.1"),val: PG_TLS1_1_VERSION, hidden: false },
    config_enum_entry { name: cstr!("TLSv1.2"),val: PG_TLS1_2_VERSION, hidden: false },
    config_enum_entry { name: cstr!("TLSv1.3"),val: PG_TLS1_3_VERSION, hidden: false },
    cfg_enum_null!(),
];

static debug_logical_replication_streaming_options: [config_enum_entry; 3] = [
    config_enum_entry { name: cstr!("buffered"),  val: DEBUG_LOGICAL_REP_STREAMING_BUFFERED,  hidden: false },
    config_enum_entry { name: cstr!("immediate"), val: DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE, hidden: false },
    cfg_enum_null!(),
];

// recovery_init_sync_method_options: HAVE_SYNCFS conditional (Linux)
#[cfg(target_os = "linux")]
static recovery_init_sync_method_options: [config_enum_entry; 3] = [
    config_enum_entry { name: cstr!("fsync"),  val: DATA_DIR_SYNC_METHOD_FSYNC,  hidden: false },
    config_enum_entry { name: cstr!("syncfs"), val: DATA_DIR_SYNC_METHOD_SYNCFS, hidden: false },
    cfg_enum_null!(),
];
#[cfg(not(target_os = "linux"))]
static recovery_init_sync_method_options: [config_enum_entry; 2] = [
    config_enum_entry { name: cstr!("fsync"), val: DATA_DIR_SYNC_METHOD_FSYNC, hidden: false },
    cfg_enum_null!(),
];

// shared_memory_options: not WIN32 includes sysv; not EXEC_BACKEND includes mmap
// We always include both sysv and mmap (non-Windows, non-exec-backend build).
#[cfg(not(target_os = "windows"))]
static shared_memory_options: [config_enum_entry; 3] = [
    config_enum_entry { name: cstr!("sysv"), val: SHMEM_TYPE_SYSV, hidden: false },
    config_enum_entry { name: cstr!("mmap"), val: SHMEM_TYPE_MMAP, hidden: false },
    cfg_enum_null!(),
];
#[cfg(target_os = "windows")]
static shared_memory_options: [config_enum_entry; 2] = [
    config_enum_entry { name: cstr!("windows"), val: SHMEM_TYPE_WINDOWS, hidden: false },
    cfg_enum_null!(),
];

// default_toast_compression_options: USE_LZ4 conditional
#[cfg(feature = "use_lz4")]
static default_toast_compression_options: [config_enum_entry; 3] = [
    config_enum_entry { name: cstr!("pglz"), val: TOAST_PGLZ_COMPRESSION, hidden: false },
    config_enum_entry { name: cstr!("lz4"),  val: TOAST_LZ4_COMPRESSION,  hidden: false },
    cfg_enum_null!(),
];
#[cfg(not(feature = "use_lz4"))]
static default_toast_compression_options: [config_enum_entry; 2] = [
    config_enum_entry { name: cstr!("pglz"), val: TOAST_PGLZ_COMPRESSION, hidden: false },
    cfg_enum_null!(),
];

// wal_compression_options: USE_LZ4 / USE_ZSTD conditional
#[cfg(all(feature = "use_lz4", feature = "use_zstd"))]
static wal_compression_options: [config_enum_entry; 11] = [
    config_enum_entry { name: cstr!("pglz"),  val: WAL_COMPRESSION_PGLZ, hidden: false },
    config_enum_entry { name: cstr!("lz4"),   val: WAL_COMPRESSION_LZ4,  hidden: false },
    config_enum_entry { name: cstr!("zstd"),  val: WAL_COMPRESSION_ZSTD, hidden: false },
    config_enum_entry { name: cstr!("on"),    val: WAL_COMPRESSION_PGLZ, hidden: false },
    config_enum_entry { name: cstr!("off"),   val: WAL_COMPRESSION_NONE, hidden: false },
    config_enum_entry { name: cstr!("true"),  val: WAL_COMPRESSION_PGLZ, hidden: true  },
    config_enum_entry { name: cstr!("false"), val: WAL_COMPRESSION_NONE, hidden: true  },
    config_enum_entry { name: cstr!("yes"),   val: WAL_COMPRESSION_PGLZ, hidden: true  },
    config_enum_entry { name: cstr!("no"),    val: WAL_COMPRESSION_NONE, hidden: true  },
    config_enum_entry { name: cstr!("1"),     val: WAL_COMPRESSION_PGLZ, hidden: true  },
    cfg_enum_null!(),
];
#[cfg(all(not(feature = "use_lz4"), not(feature = "use_zstd")))]
static wal_compression_options: [config_enum_entry; 9] = [
    config_enum_entry { name: cstr!("pglz"),  val: WAL_COMPRESSION_PGLZ, hidden: false },
    config_enum_entry { name: cstr!("on"),    val: WAL_COMPRESSION_PGLZ, hidden: false },
    config_enum_entry { name: cstr!("off"),   val: WAL_COMPRESSION_NONE, hidden: false },
    config_enum_entry { name: cstr!("true"),  val: WAL_COMPRESSION_PGLZ, hidden: true  },
    config_enum_entry { name: cstr!("false"), val: WAL_COMPRESSION_NONE, hidden: true  },
    config_enum_entry { name: cstr!("yes"),   val: WAL_COMPRESSION_PGLZ, hidden: true  },
    config_enum_entry { name: cstr!("no"),    val: WAL_COMPRESSION_NONE, hidden: true  },
    config_enum_entry { name: cstr!("1"),     val: WAL_COMPRESSION_PGLZ, hidden: true  },
    cfg_enum_null!(),
];

// file_copy_method_options: HAVE_COPYFILE + COPYFILE_CLONE_FORCE || HAVE_COPY_FILE_RANGE conditional
#[cfg(any(target_os = "macos", target_os = "linux"))]
static file_copy_method_options: [config_enum_entry; 3] = [
    config_enum_entry { name: cstr!("copy"),  val: FILE_COPY_METHOD_COPY,  hidden: false },
    config_enum_entry { name: cstr!("clone"), val: FILE_COPY_METHOD_CLONE, hidden: false },
    cfg_enum_null!(),
];
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
static file_copy_method_options: [config_enum_entry; 2] = [
    config_enum_entry { name: cstr!("copy"), val: FILE_COPY_METHOD_COPY, hidden: false },
    cfg_enum_null!(),
];

// file_extend_method_options: HAVE_POSIX_FALLOCATE conditional
#[cfg(target_family = "unix")]
static file_extend_method_options: [config_enum_entry; 3] = [
    config_enum_entry { name: cstr!("posix_fallocate"), val: FILE_EXTEND_METHOD_POSIX_FALLOCATE, hidden: false },
    config_enum_entry { name: cstr!("write_zeros"),     val: FILE_EXTEND_METHOD_WRITE_ZEROS,     hidden: false },
    cfg_enum_null!(),
];
#[cfg(not(target_family = "unix"))]
static file_extend_method_options: [config_enum_entry; 2] = [
    config_enum_entry { name: cstr!("write_zeros"), val: FILE_EXTEND_METHOD_WRITE_ZEROS, hidden: false },
    cfg_enum_null!(),
];


// ---------------------------------------------------------------------------
// GUC option variables exported from this module
// ---------------------------------------------------------------------------
// (These are the non-static global variables that C code declares at file scope.)

#[no_mangle]
pub static mut log_duration: bool = false;
#[no_mangle]
pub static mut Debug_print_plan_2: bool = false; // Debug_print_plan is extern above
#[no_mangle]
pub static mut Debug_print_parse_2: bool = false;
#[no_mangle]
pub static mut Debug_print_rewritten_2: bool = false;
#[no_mangle]
pub static mut Debug_pretty_print_2: bool = true;
#[no_mangle]
pub static mut log_parser_stats_2: bool = false;
#[no_mangle]
pub static mut log_planner_stats_2: bool = false;
#[no_mangle]
pub static mut log_executor_stats_2: bool = false;
#[no_mangle]
pub static mut log_statement_stats_2: bool = false;
#[no_mangle]
pub static mut log_btree_build_stats_2: bool = false;
#[no_mangle]
pub static mut row_security: bool = true; // extern above but needs pub export
#[no_mangle]
pub static mut check_function_bodies_2: bool = true;
#[no_mangle]
pub static mut current_role_is_superuser_2: bool = false;
#[no_mangle]
pub static mut log_min_error_statement_2: c_int = ERROR;
#[no_mangle]
pub static mut log_min_messages_2: c_int = WARNING;
#[no_mangle]
pub static mut client_min_messages_2: c_int = NOTICE;
#[no_mangle]
pub static mut log_min_duration_sample_2: c_int = -1;
#[no_mangle]
pub static mut log_min_duration_statement_2: c_int = -1;
#[no_mangle]
pub static mut log_parameter_max_length_2: c_int = -1;
#[no_mangle]
pub static mut log_parameter_max_length_on_error_2: c_int = 0;
#[no_mangle]
pub static mut log_temp_files_2: c_int = -1;
#[no_mangle]
pub static mut log_statement_sample_rate_2: c_double = 1.0;
#[no_mangle]
pub static mut log_xact_sample_rate_2: c_double = 0.0;
#[no_mangle]
pub static mut temp_file_limit_2: c_int = -1;
#[no_mangle]
pub static mut num_temp_buffers_2: c_int = 1024;
// cluster_name, ConfigFileName, HbaFileName, IdentFileName, etc. are in extern block above
#[no_mangle]
pub static mut tcp_keepalives_idle_2: c_int = 0;
#[no_mangle]
pub static mut tcp_keepalives_interval_2: c_int = 0;
#[no_mangle]
pub static mut tcp_keepalives_count_2: c_int = 0;
#[no_mangle]
pub static mut tcp_user_timeout_2: c_int = 0;
#[no_mangle]
pub static mut huge_pages_2: c_int = HUGE_PAGES_TRY;
#[no_mangle]
pub static mut huge_page_size_2: c_int = 0;
#[no_mangle]
pub static mut huge_pages_status_2: c_int = HUGE_PAGES_UNKNOWN;
#[no_mangle]
pub static mut in_hot_standby_guc_2: bool = false;
// char *backtrace_functions; (guc_tables.c)
#[no_mangle]
pub static mut backtrace_functions: *mut c_char = ptr::null_mut();
// static bool default_with_oids = false; (guc_tables.c)
#[no_mangle]
pub static mut default_with_oids: bool = false;

// ---------------------------------------------------------------------------
// Displayable names for context types (enum GucContext)
//
// Note: these strings are deliberately not localized.
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut GucContext_Names: [*const c_char; 7] = [
    // [PGC_INTERNAL]
    cstr!("internal"),
    // [PGC_POSTMASTER]
    cstr!("postmaster"),
    // [PGC_SIGHUP]
    cstr!("sighup"),
    // [PGC_SU_BACKEND]
    cstr!("superuser-backend"),
    // [PGC_BACKEND]
    cstr!("backend"),
    // [PGC_SUSET]
    cstr!("superuser"),
    // [PGC_USERSET]
    cstr!("user"),
];

// ---------------------------------------------------------------------------
// Displayable names for source types (enum GucSource)
//
// Note: these strings are deliberately not localized.
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut GucSource_Names: [*const c_char; 14] = [
    cstr!("default"),            // PGC_S_DEFAULT
    cstr!("default"),            // PGC_S_DYNAMIC_DEFAULT
    cstr!("environment variable"),// PGC_S_ENV_VAR
    cstr!("configuration file"), // PGC_S_FILE
    cstr!("command line"),       // PGC_S_ARGV
    cstr!("global"),             // PGC_S_GLOBAL
    cstr!("database"),           // PGC_S_DATABASE
    cstr!("user"),               // PGC_S_USER
    cstr!("database user"),      // PGC_S_DATABASE_USER
    cstr!("client"),             // PGC_S_CLIENT
    cstr!("override"),           // PGC_S_OVERRIDE
    cstr!("interactive"),        // PGC_S_INTERACTIVE
    cstr!("test"),               // PGC_S_TEST
    cstr!("session"),            // PGC_S_SESSION
];

// ---------------------------------------------------------------------------
// Displayable names for the groupings defined in enum config_group
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut config_group_names: [*const c_char; 48] = [
    cstr!("Ungrouped"),                                                             // UNGROUPED
    cstr!("File Locations"),                                                        // FILE_LOCATIONS
    cstr!("Connections and Authentication / Connection Settings"),                  // CONN_AUTH_SETTINGS
    cstr!("Connections and Authentication / TCP Settings"),                         // CONN_AUTH_TCP
    cstr!("Connections and Authentication / Authentication"),                       // CONN_AUTH_AUTH
    cstr!("Connections and Authentication / SSL"),                                  // CONN_AUTH_SSL
    cstr!("Resource Usage / Memory"),                                               // RESOURCES_MEM
    cstr!("Resource Usage / Disk"),                                                 // RESOURCES_DISK
    cstr!("Resource Usage / Kernel Resources"),                                     // RESOURCES_KERNEL
    cstr!("Resource Usage / Background Writer"),                                    // RESOURCES_BGWRITER
    cstr!("Resource Usage / I/O"),                                                  // RESOURCES_IO
    cstr!("Resource Usage / Worker Processes"),                                     // RESOURCES_WORKER_PROCESSES
    cstr!("Write-Ahead Log / Settings"),                                            // WAL_SETTINGS
    cstr!("Write-Ahead Log / Checkpoints"),                                         // WAL_CHECKPOINTS
    cstr!("Write-Ahead Log / Archiving"),                                           // WAL_ARCHIVING
    cstr!("Write-Ahead Log / Recovery"),                                            // WAL_RECOVERY
    cstr!("Write-Ahead Log / Archive Recovery"),                                    // WAL_ARCHIVE_RECOVERY
    cstr!("Write-Ahead Log / Recovery Target"),                                     // WAL_RECOVERY_TARGET
    cstr!("Write-Ahead Log / Summarization"),                                       // WAL_SUMMARIZATION
    cstr!("Replication / Sending Servers"),                                         // REPLICATION_SENDING
    cstr!("Replication / Primary Server"),                                          // REPLICATION_PRIMARY
    cstr!("Replication / Standby Servers"),                                         // REPLICATION_STANDBY
    cstr!("Replication / Subscribers"),                                             // REPLICATION_SUBSCRIBERS
    cstr!("Query Tuning / Planner Method Configuration"),                           // QUERY_TUNING_METHOD
    cstr!("Query Tuning / Planner Cost Constants"),                                 // QUERY_TUNING_COST
    cstr!("Query Tuning / Genetic Query Optimizer"),                                // QUERY_TUNING_GEQO
    cstr!("Query Tuning / Other Planner Options"),                                  // QUERY_TUNING_OTHER
    cstr!("Reporting and Logging / Where to Log"),                                  // LOGGING_WHERE
    cstr!("Reporting and Logging / When to Log"),                                   // LOGGING_WHEN
    cstr!("Reporting and Logging / What to Log"),                                   // LOGGING_WHAT
    cstr!("Reporting and Logging / Process Title"),                                 // PROCESS_TITLE
    cstr!("Statistics / Monitoring"),                                               // STATS_MONITORING
    cstr!("Statistics / Cumulative Query and Index Statistics"),                    // STATS_CUMULATIVE
    cstr!("Vacuuming / Automatic Vacuuming"),                                       // VACUUM_AUTOVACUUM
    cstr!("Vacuuming / Cost-Based Vacuum Delay"),                                   // VACUUM_COST_DELAY
    cstr!("Vacuuming / Default Behavior"),                                          // VACUUM_DEFAULT
    cstr!("Vacuuming / Freezing"),                                                  // VACUUM_FREEZING
    cstr!("Client Connection Defaults / Statement Behavior"),                       // CLIENT_CONN_STATEMENT
    cstr!("Client Connection Defaults / Locale and Formatting"),                    // CLIENT_CONN_LOCALE
    cstr!("Client Connection Defaults / Shared Library Preloading"),               // CLIENT_CONN_PRELOAD
    cstr!("Client Connection Defaults / Other Defaults"),                           // CLIENT_CONN_OTHER
    cstr!("Lock Management"),                                                       // LOCK_MANAGEMENT
    cstr!("Version and Platform Compatibility / Previous PostgreSQL Versions"),     // COMPAT_OPTIONS_PREVIOUS
    cstr!("Version and Platform Compatibility / Other Platforms and Clients"),      // COMPAT_OPTIONS_OTHER
    cstr!("Error Handling"),                                                        // ERROR_HANDLING_OPTIONS
    cstr!("Preset Options"),                                                        // PRESET_OPTIONS
    cstr!("Customized Options"),                                                    // CUSTOM_OPTIONS
    cstr!("Developer Options"),                                                     // DEVELOPER_OPTIONS
];

// ---------------------------------------------------------------------------
// Displayable names for GUC variable types (enum config_type)
//
// Note: these strings are deliberately not localized.
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut config_type_names: [*const c_char; 5] = [
    cstr!("bool"),    // PGC_BOOL
    cstr!("integer"), // PGC_INT
    cstr!("real"),    // PGC_REAL
    cstr!("string"),  // PGC_STRING
    cstr!("enum"),    // PGC_ENUM
];


// ---------------------------------------------------------------------------
// Helper macro for zero-initializing the config_generic header of each entry.
// Only the constant fields (name, context, group, short_desc, long_desc, flags)
// are set here; all runtime fields remain zeroed / null and are filled in at
// runtime by InitializeOneGUCOption / build_guc_variables.
// ---------------------------------------------------------------------------

use crate::lib::ilist::{dlist_node, slist_node};
use std::ffi::c_void;

// Inline zero-value helpers for nested structs that cannot be default-constructed
// in a const context without a const Default impl.
const ZERO_DLIST_NODE: dlist_node = dlist_node { prev: std::ptr::null_mut(), next: std::ptr::null_mut() };
const ZERO_SLIST_NODE: slist_node = slist_node { next: std::ptr::null_mut() };

macro_rules! gen_init {
    ($name:expr, $ctx:expr, $grp:expr, $sd:expr, $ld:expr, $fl:expr, $vt:expr) => {
        crate::utils::misc::guc::config_generic {
            name:             cstr!($name),
            context:          $ctx,
            group:            $grp,
            short_desc:       $sd,
            long_desc:        $ld,
            flags:            $fl,
            vartype:          $vt,
            status:           0,
            source:           crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
            reset_source:     crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
            scontext:         crate::utils::misc::guc::GucContext::PGC_INTERNAL,
            reset_scontext:   crate::utils::misc::guc::GucContext::PGC_INTERNAL,
            srole:            0,
            reset_srole:      0,
            stack:            std::ptr::null_mut(),
            extra:            std::ptr::null_mut(),
            nondef_link:      ZERO_DLIST_NODE,
            stack_link:       ZERO_SLIST_NODE,
            report_link:      ZERO_SLIST_NODE,
            last_reported:    std::ptr::null_mut(),
            sourcefile:       std::ptr::null_mut(),
            sourceline:       0,
        }
    };
}

// Null-terminated sentinel entry macros
macro_rules! bool_sentinel {
    () => {
        config_bool {
            gen: crate::utils::misc::guc::config_generic {
                name: std::ptr::null(), context: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                group: config_group::UNGROUPED,
                short_desc: std::ptr::null(), long_desc: std::ptr::null(), flags: 0,
                vartype: config_type::PGC_BOOL, status: 0,
                source: crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
                reset_source: crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
                scontext: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                reset_scontext: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                srole: 0, reset_srole: 0, stack: std::ptr::null_mut(), extra: std::ptr::null_mut(),
                nondef_link: ZERO_DLIST_NODE, stack_link: ZERO_SLIST_NODE, report_link: ZERO_SLIST_NODE,
                last_reported: std::ptr::null_mut(), sourcefile: std::ptr::null_mut(), sourceline: 0,
            },
            variable: std::ptr::null_mut(), boot_val: false,
            check_hook: None, assign_hook: None, show_hook: None,
            reset_val: false, reset_extra: std::ptr::null_mut(),
        }
    };
}

macro_rules! int_sentinel {
    () => {
        config_int {
            gen: crate::utils::misc::guc::config_generic {
                name: std::ptr::null(), context: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                group: config_group::UNGROUPED,
                short_desc: std::ptr::null(), long_desc: std::ptr::null(), flags: 0,
                vartype: config_type::PGC_INT, status: 0,
                source: crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
                reset_source: crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
                scontext: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                reset_scontext: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                srole: 0, reset_srole: 0, stack: std::ptr::null_mut(), extra: std::ptr::null_mut(),
                nondef_link: ZERO_DLIST_NODE, stack_link: ZERO_SLIST_NODE, report_link: ZERO_SLIST_NODE,
                last_reported: std::ptr::null_mut(), sourcefile: std::ptr::null_mut(), sourceline: 0,
            },
            variable: std::ptr::null_mut(), boot_val: 0, min: 0, max: 0,
            check_hook: None, assign_hook: None, show_hook: None,
            reset_val: 0, reset_extra: std::ptr::null_mut(),
        }
    };
}

macro_rules! real_sentinel {
    () => {
        config_real {
            gen: crate::utils::misc::guc::config_generic {
                name: std::ptr::null(), context: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                group: config_group::UNGROUPED,
                short_desc: std::ptr::null(), long_desc: std::ptr::null(), flags: 0,
                vartype: config_type::PGC_REAL, status: 0,
                source: crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
                reset_source: crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
                scontext: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                reset_scontext: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                srole: 0, reset_srole: 0, stack: std::ptr::null_mut(), extra: std::ptr::null_mut(),
                nondef_link: ZERO_DLIST_NODE, stack_link: ZERO_SLIST_NODE, report_link: ZERO_SLIST_NODE,
                last_reported: std::ptr::null_mut(), sourcefile: std::ptr::null_mut(), sourceline: 0,
            },
            variable: std::ptr::null_mut(), boot_val: 0.0, min: 0.0, max: 0.0,
            check_hook: None, assign_hook: None, show_hook: None,
            reset_val: 0.0, reset_extra: std::ptr::null_mut(),
        }
    };
}

macro_rules! string_sentinel {
    () => {
        config_string {
            gen: crate::utils::misc::guc::config_generic {
                name: std::ptr::null(), context: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                group: config_group::UNGROUPED,
                short_desc: std::ptr::null(), long_desc: std::ptr::null(), flags: 0,
                vartype: config_type::PGC_STRING, status: 0,
                source: crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
                reset_source: crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
                scontext: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                reset_scontext: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                srole: 0, reset_srole: 0, stack: std::ptr::null_mut(), extra: std::ptr::null_mut(),
                nondef_link: ZERO_DLIST_NODE, stack_link: ZERO_SLIST_NODE, report_link: ZERO_SLIST_NODE,
                last_reported: std::ptr::null_mut(), sourcefile: std::ptr::null_mut(), sourceline: 0,
            },
            variable: std::ptr::null_mut(), boot_val: std::ptr::null(),
            check_hook: None, assign_hook: None, show_hook: None,
            reset_val: std::ptr::null_mut(), reset_extra: std::ptr::null_mut(),
        }
    };
}

macro_rules! enum_sentinel {
    () => {
        config_enum {
            gen: crate::utils::misc::guc::config_generic {
                name: std::ptr::null(), context: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                group: config_group::UNGROUPED,
                short_desc: std::ptr::null(), long_desc: std::ptr::null(), flags: 0,
                vartype: config_type::PGC_ENUM, status: 0,
                source: crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
                reset_source: crate::utils::misc::guc::GucSource::PGC_S_DEFAULT,
                scontext: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                reset_scontext: crate::utils::misc::guc::GucContext::PGC_INTERNAL,
                srole: 0, reset_srole: 0, stack: std::ptr::null_mut(), extra: std::ptr::null_mut(),
                nondef_link: ZERO_DLIST_NODE, stack_link: ZERO_SLIST_NODE, report_link: ZERO_SLIST_NODE,
                last_reported: std::ptr::null_mut(), sourcefile: std::ptr::null_mut(), sourceline: 0,
            },
            variable: std::ptr::null_mut(), boot_val: 0, options: std::ptr::null(),
            check_hook: None, assign_hook: None, show_hook: None,
            reset_val: 0, reset_extra: std::ptr::null_mut(),
        }
    };
}



// ---------------------------------------------------------------------------
// ConfigureNamesBool[]
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut ConfigureNamesBool: [config_bool; 116] = [
    config_bool {
        gen: gen_init!("enable_seqscan", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of sequential-scan plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_seqscan },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_indexscan", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of index-scan plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_indexscan },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_indexonlyscan", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of index-only-scan plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_indexonlyscan },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_bitmapscan", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of bitmap-scan plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_bitmapscan },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_tidscan", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of TID scan plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_tidscan },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_sort", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of explicit sort steps."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_sort },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_incremental_sort", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of incremental sort steps."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_incremental_sort },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_hashagg", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of hashed aggregation plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_hashagg },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_material", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of materialization."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_material },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_memoize", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of memoization."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_memoize },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_nestloop", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of nested-loop join plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_nestloop },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_mergejoin", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of merge join plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_mergejoin },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_hashjoin", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of hash join plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_hashjoin },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_gathermerge", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of gather merge plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_gathermerge },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_partitionwise_join", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables partitionwise join."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_partitionwise_join },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_partitionwise_aggregate", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables partitionwise aggregation and grouping."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_partitionwise_aggregate },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_parallel_append", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of parallel append plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_parallel_append },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_parallel_hash", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of parallel hash plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_parallel_hash },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_partition_pruning", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables plan-time and execution-time partition pruning."),
            cstr!("Allows the query planner and executor to compare partition bounds to conditions in the query to determine which partitions must be scanned."),
            GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_partition_pruning },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_presorted_aggregate", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's ability to produce plans that provide presorted input for ORDER BY / DISTINCT aggregate functions."),
            cstr!("Allows the query planner to build plans that provide presorted input for aggregate functions with an ORDER BY / DISTINCT clause.  When disabled, implicit sorts are always performed during execution."),
            GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_presorted_aggregate },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_async_append", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables the planner's use of async append plans."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_async_append },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_self_join_elimination", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables removal of unique self-joins."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_self_join_elimination },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_group_by_reordering", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables reordering of GROUP BY keys."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_group_by_reordering },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("enable_distinct_reordering", PGC_USERSET, QUERY_TUNING_METHOD,
            cstr!("Enables reordering of DISTINCT keys."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_distinct_reordering },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("geqo", PGC_USERSET, QUERY_TUNING_GEQO,
            cstr!("Enables genetic query optimization."),
            cstr!("This algorithm attempts to do planning without exhaustive searching."),
            GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_geqo },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    // Not for general use --- used by SET SESSION AUTHORIZATION and SET ROLE
    config_bool {
        gen: gen_init!("is_superuser", PGC_INTERNAL, UNGROUPED,
            cstr!("Shows whether the current user is a superuser."),
            ptr::null(),
            GUC_REPORT | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_ALLOW_IN_PARALLEL,
            config_type::PGC_BOOL),
        variable: unsafe { &raw mut current_role_is_superuser },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    // This setting itself cannot be set by ALTER SYSTEM to avoid an operator turning this off without a way back.
    config_bool {
        gen: gen_init!("allow_alter_system", PGC_SIGHUP, COMPAT_OPTIONS_OTHER,
            cstr!("Allows running the ALTER SYSTEM command."),
            cstr!("Can be set to off for environments where global configuration changes should be made using a different method."),
            GUC_DISALLOW_IN_AUTO_FILE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut AllowAlterSystem },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("bonjour", PGC_POSTMASTER, CONN_AUTH_SETTINGS,
            cstr!("Enables advertising the server via Bonjour."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enable_bonjour },
        boot_val: false,
        check_hook: Some(check_bonjour), assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("track_commit_timestamp", PGC_POSTMASTER, REPLICATION_SENDING,
            cstr!("Collects transaction commit time."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut track_commit_timestamp },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("ssl", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Enables SSL connections."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut EnableSSL },
        boot_val: false,
        check_hook: Some(check_ssl), assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("ssl_passphrase_command_supports_reload", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Controls whether \"ssl_passphrase_command\" is called during server reload."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut ssl_passphrase_command_supports_reload },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("ssl_prefer_server_ciphers", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Give priority to server ciphersuite order."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut SSLPreferServerCiphers },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("fsync", PGC_SIGHUP, WAL_SETTINGS,
            cstr!("Forces synchronization of updates to disk."),
            cstr!("The server will use the fsync() system call in several places to make sure that updates are physically written to disk. This ensures that a database cluster will recover to a consistent state after an operating system or hardware crash."),
            0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut enableFsync },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("ignore_checksum_failure", PGC_SUSET, DEVELOPER_OPTIONS,
            cstr!("Continues processing after a checksum failure."),
            cstr!("Detection of a checksum failure normally causes PostgreSQL to report an error, aborting the current transaction. Setting ignore_checksum_failure to true causes the system to ignore the failure (but still report a warning), and continue processing. This behavior could cause crashes or other serious problems. Only has an effect if checksums are enabled."),
            GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut ignore_checksum_failure },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("zero_damaged_pages", PGC_SUSET, DEVELOPER_OPTIONS,
            cstr!("Continues processing past damaged page headers."),
            cstr!("Detection of a damaged page header normally causes PostgreSQL to report an error, aborting the current transaction. Setting \"zero_damaged_pages\" to true causes the system to instead report a warning, zero out the damaged page, and continue processing. This behavior will destroy data, namely all the rows on the damaged page."),
            GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut zero_damaged_pages },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("ignore_invalid_pages", PGC_POSTMASTER, DEVELOPER_OPTIONS,
            cstr!("Continues recovery after an invalid pages failure."),
            cstr!("Detection of WAL records having references to invalid pages during recovery causes PostgreSQL to raise a PANIC-level error, aborting the recovery. Setting \"ignore_invalid_pages\" to true causes the system to ignore invalid page references in WAL records (but still report a warning), and continue recovery. This behavior may cause crashes, data loss, propagate or hide corruption, or other serious problems. Only has an effect during recovery or in standby mode."),
            GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut ignore_invalid_pages },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("full_page_writes", PGC_SIGHUP, WAL_SETTINGS,
            cstr!("Writes full pages to WAL when first modified after a checkpoint."),
            cstr!("A page write in process during an operating system crash might be only partially written to disk.  During recovery, the row changes stored in WAL are not enough to recover.  This option writes pages when first modified after a checkpoint to WAL so full recovery is possible."),
            0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut fullPageWrites },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("wal_log_hints", PGC_POSTMASTER, WAL_SETTINGS,
            cstr!("Writes full pages to WAL when first modified after a checkpoint, even for a non-critical modification."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut wal_log_hints },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("wal_init_zero", PGC_SUSET, WAL_SETTINGS,
            cstr!("Writes zeroes to new WAL files before first use."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut wal_init_zero },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("wal_recycle", PGC_SUSET, WAL_SETTINGS,
            cstr!("Recycles WAL files by renaming them."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut wal_recycle },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_checkpoints", PGC_SIGHUP, LOGGING_WHAT,
            cstr!("Logs each checkpoint."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_checkpoints },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("trace_connection_negotiation", PGC_POSTMASTER, DEVELOPER_OPTIONS,
            cstr!("Logs details of pre-authentication connection handshake."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Trace_connection_negotiation },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_disconnections", PGC_SU_BACKEND, LOGGING_WHAT,
            cstr!("Logs end of a session, including duration."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Log_disconnections },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_replication_commands", PGC_SUSET, LOGGING_WHAT,
            cstr!("Logs each replication command."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_replication_commands },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("debug_assertions", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows whether the running server has assertion checks enabled."),
            ptr::null(),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut assert_enabled },
        boot_val: false, // DEFAULT_ASSERT_ENABLED = false in non-assert builds
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("exit_on_error", PGC_USERSET, ERROR_HANDLING_OPTIONS,
            cstr!("Terminate session on any error."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut ExitOnAnyError },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("restart_after_crash", PGC_SIGHUP, ERROR_HANDLING_OPTIONS,
            cstr!("Reinitialize server after backend crash."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut restart_after_crash },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("remove_temp_files_after_crash", PGC_SIGHUP, DEVELOPER_OPTIONS,
            cstr!("Remove temporary files after backend crash."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut remove_temp_files_after_crash },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("send_abort_for_crash", PGC_SIGHUP, DEVELOPER_OPTIONS,
            cstr!("Send SIGABRT not SIGQUIT to child processes after backend crash."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut send_abort_for_crash },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("send_abort_for_kill", PGC_SIGHUP, DEVELOPER_OPTIONS,
            cstr!("Send SIGABRT not SIGKILL to stuck child processes."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut send_abort_for_kill },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_duration", PGC_SUSET, LOGGING_WHAT,
            cstr!("Logs the duration of each completed SQL statement."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_duration },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("debug_print_parse", PGC_USERSET, LOGGING_WHAT,
            cstr!("Logs each query's parse tree."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Debug_print_parse },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("debug_print_rewritten", PGC_USERSET, LOGGING_WHAT,
            cstr!("Logs each query's rewritten parse tree."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Debug_print_rewritten },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("debug_print_plan", PGC_USERSET, LOGGING_WHAT,
            cstr!("Logs each query's execution plan."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Debug_print_plan },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("debug_pretty_print", PGC_USERSET, LOGGING_WHAT,
            cstr!("Indents parse and plan tree displays."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Debug_pretty_print },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_parser_stats", PGC_SUSET, STATS_MONITORING,
            cstr!("Writes parser performance statistics to the server log."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_parser_stats },
        boot_val: false,
        check_hook: Some(check_stage_log_stats), assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_planner_stats", PGC_SUSET, STATS_MONITORING,
            cstr!("Writes planner performance statistics to the server log."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_planner_stats },
        boot_val: false,
        check_hook: Some(check_stage_log_stats), assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_executor_stats", PGC_SUSET, STATS_MONITORING,
            cstr!("Writes executor performance statistics to the server log."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_executor_stats },
        boot_val: false,
        check_hook: Some(check_stage_log_stats), assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_statement_stats", PGC_SUSET, STATS_MONITORING,
            cstr!("Writes cumulative performance statistics to the server log."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_statement_stats },
        boot_val: false,
        check_hook: Some(check_log_stats), assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("track_activities", PGC_SUSET, STATS_CUMULATIVE,
            cstr!("Collects information about executing commands."),
            cstr!("Enables the collection of information on the currently executing command of each session, along with the time at which that command began execution."),
            0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut pgstat_track_activities },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("track_counts", PGC_SUSET, STATS_CUMULATIVE,
            cstr!("Collects statistics on database activity."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut pgstat_track_counts },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("track_cost_delay_timing", PGC_SUSET, STATS_CUMULATIVE,
            cstr!("Collects timing statistics for cost-based vacuum delay."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut track_cost_delay_timing },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("track_io_timing", PGC_SUSET, STATS_CUMULATIVE,
            cstr!("Collects timing statistics for database I/O activity."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut track_io_timing },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("track_wal_io_timing", PGC_SUSET, STATS_CUMULATIVE,
            cstr!("Collects timing statistics for WAL I/O activity."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut track_wal_io_timing },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("update_process_title", PGC_SUSET, PROCESS_TITLE,
            cstr!("Updates the process title to show the active SQL command."),
            cstr!("Enables updating of the process title every time a new SQL command is received by the server."),
            0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut update_process_title },
        boot_val: DEFAULT_UPDATE_PROCESS_TITLE,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_UPDATE_PROCESS_TITLE, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("autovacuum", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Starts the autovacuum subprocess."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut autovacuum_start_daemon },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("trace_notify", PGC_USERSET, DEVELOPER_OPTIONS,
            cstr!("Generates debugging output for LISTEN and NOTIFY."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Trace_notify },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_lock_waits", PGC_SUSET, LOGGING_WHAT,
            cstr!("Logs long lock waits."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_lock_waits },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_lock_failures", PGC_SUSET, LOGGING_WHAT,
            cstr!("Logs lock failures."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_lock_failures },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_recovery_conflict_waits", PGC_SIGHUP, LOGGING_WHAT,
            cstr!("Logs standby recovery conflict waits."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_recovery_conflict_waits },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_hostname", PGC_SIGHUP, LOGGING_WHAT,
            cstr!("Logs the host name in the connection logs."),
            cstr!("By default, connection logs only show the IP address of the connecting host. If you want them to show the host name you can turn this on, but depending on your host name resolution setup it might impose a non-negligible performance penalty."),
            0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut log_hostname },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("transform_null_equals", PGC_USERSET, COMPAT_OPTIONS_OTHER,
            cstr!("Treats \"expr=NULL\" as \"expr IS NULL\"."),
            cstr!("When turned on, expressions of the form expr = NULL (or NULL = expr) are treated as expr IS NULL, that is, they return true if expr evaluates to the null value, and false otherwise. The correct behavior of expr = NULL is to always return null (unknown)."),
            0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Transform_null_equals },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("default_transaction_read_only", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the default read-only status of new transactions."),
            ptr::null(), GUC_REPORT, config_type::PGC_BOOL),
        variable: unsafe { &raw mut DefaultXactReadOnly },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("transaction_read_only", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the current transaction's read-only status."),
            ptr::null(),
            GUC_NO_RESET | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE,
            config_type::PGC_BOOL),
        variable: unsafe { &raw mut XactReadOnly },
        boot_val: false,
        check_hook: Some(check_transaction_read_only), assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("default_transaction_deferrable", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the default deferrable status of new transactions."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut DefaultXactDeferrable },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("transaction_deferrable", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Whether to defer a read-only serializable transaction until it can be executed with no possible serialization failures."),
            ptr::null(),
            GUC_NO_RESET | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE,
            config_type::PGC_BOOL),
        variable: unsafe { &raw mut XactDeferrable },
        boot_val: false,
        check_hook: Some(check_transaction_deferrable), assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("row_security", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Enables row security."),
            cstr!("When enabled, row security will be applied to all users."),
            0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut row_security },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("check_function_bodies", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Check routine bodies during CREATE FUNCTION and CREATE PROCEDURE."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut check_function_bodies },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("array_nulls", PGC_USERSET, COMPAT_OPTIONS_PREVIOUS,
            cstr!("Enables input of NULL elements in arrays."),
            cstr!("When turned on, unquoted NULL in an array input value means a null value; otherwise it is taken literally."),
            0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Array_nulls },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    /*
     * WITH OIDS support, and consequently default_with_oids, was removed in
     * PostgreSQL 12, but we tolerate the parameter being set to false to
     * avoid unnecessarily breaking older dump files.
     */
    config_bool {
        gen: gen_init!("default_with_oids", PGC_USERSET, COMPAT_OPTIONS_PREVIOUS,
            cstr!("WITH OIDS is no longer supported; this can only be false."),
            ptr::null(),
            GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut default_with_oids },
        boot_val: false,
        check_hook: Some(check_default_with_oids), assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("logging_collector", PGC_POSTMASTER, LOGGING_WHERE,
            cstr!("Start a subprocess to capture stderr, csvlog and/or jsonlog into log files."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Logging_collector },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("log_truncate_on_rotation", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Truncate existing log files of same name during log rotation."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut Log_truncate_on_rotation },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("trace_sort", PGC_USERSET, DEVELOPER_OPTIONS,
            cstr!("Emit information about resource usage in sorting."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut trace_sort },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("integer_datetimes", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows whether datetimes are integer based."),
            ptr::null(),
            GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut integer_datetimes },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("krb_caseins_users", PGC_SIGHUP, CONN_AUTH_AUTH,
            cstr!("Sets whether Kerberos and GSSAPI user names should be treated as case-insensitive."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut pg_krb_caseins_users },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("gss_accept_delegation", PGC_SIGHUP, CONN_AUTH_AUTH,
            cstr!("Sets whether GSSAPI delegation should be accepted from the client."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut pg_gss_accept_delegation },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("escape_string_warning", PGC_USERSET, COMPAT_OPTIONS_PREVIOUS,
            cstr!("Warn about backslash escapes in ordinary string literals."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut escape_string_warning },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("standard_conforming_strings", PGC_USERSET, COMPAT_OPTIONS_PREVIOUS,
            cstr!("Causes '...' strings to treat backslashes literally."),
            ptr::null(), GUC_REPORT, config_type::PGC_BOOL),
        variable: unsafe { &raw mut standard_conforming_strings },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("synchronize_seqscans", PGC_USERSET, COMPAT_OPTIONS_PREVIOUS,
            cstr!("Enables synchronized sequential scans."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut synchronize_seqscans },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("recovery_target_inclusive", PGC_POSTMASTER, WAL_RECOVERY_TARGET,
            cstr!("Sets whether to include or exclude transaction with recovery target."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut recoveryTargetInclusive },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("summarize_wal", PGC_SIGHUP, WAL_SUMMARIZATION,
            cstr!("Starts the WAL summarizer process to enable incremental backup."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut summarize_wal },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("hot_standby", PGC_POSTMASTER, REPLICATION_STANDBY,
            cstr!("Allows connections and queries during recovery."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut EnableHotStandby },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("hot_standby_feedback", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Allows feedback from a hot standby to the primary that will avoid query conflicts."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut hot_standby_feedback },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("in_hot_standby", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows whether hot standby is currently active."),
            ptr::null(),
            GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut in_hot_standby_guc },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: Some(show_in_hot_standby),
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("allow_system_table_mods", PGC_SUSET, DEVELOPER_OPTIONS,
            cstr!("Allows modifications of the structure of system tables."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut allowSystemTableMods },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("ignore_system_indexes", PGC_BACKEND, DEVELOPER_OPTIONS,
            cstr!("Disables reading from system indexes."),
            cstr!("It does not prevent updating the indexes, so it is safe to use.  The worst consequence is slowness."),
            GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut IgnoreSystemIndexes },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("allow_in_place_tablespaces", PGC_SUSET, DEVELOPER_OPTIONS,
            cstr!("Allows tablespaces directly inside pg_tblspc, for testing."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut allow_in_place_tablespaces },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("lo_compat_privileges", PGC_SUSET, COMPAT_OPTIONS_PREVIOUS,
            cstr!("Enables backward compatibility mode for privilege checks on large objects."),
            cstr!("Skips privilege checks when reading or modifying large objects, for compatibility with PostgreSQL releases prior to 9.0."),
            0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut lo_compat_privileges },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("quote_all_identifiers", PGC_USERSET, COMPAT_OPTIONS_PREVIOUS,
            cstr!("When generating SQL fragments, quote all identifiers."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut quote_all_identifiers },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("data_checksums", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows whether data checksums are turned on for this cluster."),
            ptr::null(),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_RUNTIME_COMPUTED, config_type::PGC_BOOL),
        variable: unsafe { &raw mut data_checksums },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("syslog_sequence_numbers", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Add sequence number to syslog messages to avoid duplicate suppression."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut syslog_sequence_numbers },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("syslog_split_messages", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Split messages sent to syslog by lines and to fit into 1024 bytes."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut syslog_split_messages },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("parallel_leader_participation", PGC_USERSET, RESOURCES_WORKER_PROCESSES,
            cstr!("Controls whether Gather and Gather Merge also run subplans."),
            cstr!("Should gather nodes also run subplans or just gather tuples?"),
            GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut parallel_leader_participation },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("jit", PGC_USERSET, QUERY_TUNING_OTHER,
            cstr!("Allow JIT compilation."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_BOOL),
        variable: unsafe { &raw mut jit_enabled },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("jit_debugging_support", PGC_SU_BACKEND, DEVELOPER_OPTIONS,
            cstr!("Register JIT-compiled functions with debugger."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut jit_debugging_support },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("jit_dump_bitcode", PGC_SUSET, DEVELOPER_OPTIONS,
            cstr!("Write out LLVM bitcode to facilitate JIT debugging."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut jit_dump_bitcode },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("jit_expressions", PGC_USERSET, DEVELOPER_OPTIONS,
            cstr!("Allow JIT compilation of expressions."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut jit_expressions },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("jit_profiling_support", PGC_SU_BACKEND, DEVELOPER_OPTIONS,
            cstr!("Register JIT-compiled functions with perf profiler."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut jit_profiling_support },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("jit_tuple_deforming", PGC_USERSET, DEVELOPER_OPTIONS,
            cstr!("Allow JIT compilation of tuple deforming."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_BOOL),
        variable: unsafe { &raw mut jit_tuple_deforming },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("data_sync_retry", PGC_POSTMASTER, ERROR_HANDLING_OPTIONS,
            cstr!("Whether to continue running after a failure to sync data files."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut data_sync_retry },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("wal_receiver_create_temp_slot", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Sets whether a WAL receiver should create a temporary replication slot if no permanent slot is configured."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut wal_receiver_create_temp_slot },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("event_triggers", PGC_SUSET, CLIENT_CONN_STATEMENT,
            cstr!("Enables event triggers."),
            cstr!("When enabled, event triggers will fire for all applicable statements."),
            0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut event_triggers },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("sync_replication_slots", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Enables a physical standby to synchronize logical failover replication slots from the primary server."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut sync_replication_slots },
        boot_val: false,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: false, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("md5_password_warnings", PGC_USERSET, CONN_AUTH_AUTH,
            cstr!("Enables deprecation warnings for MD5 passwords."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut md5_password_warnings },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    config_bool {
        gen: gen_init!("vacuum_truncate", PGC_USERSET, VACUUM_DEFAULT,
            cstr!("Enables vacuum to truncate empty pages at the end of the table."),
            ptr::null(), 0, config_type::PGC_BOOL),
        variable: unsafe { &raw mut vacuum_truncate },
        boot_val: true,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: true, reset_extra: ptr::null_mut(),
    },
    // End-of-list marker
    bool_sentinel!(),
];

// ---------------------------------------------------------------------------
// ConfigureNamesInt[]
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut ConfigureNamesInt: [config_int; 148] = [
    config_int {
        gen: gen_init!("archive_timeout", PGC_SIGHUP, WAL_ARCHIVING,
            cstr!("Sets the amount of time to wait before forcing a switch to the next WAL file."),
            cstr!("0 disables the timeout."),
            GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut XLogArchiveTimeout },
        boot_val: 0, min: 0, max: INT_MAX / 2,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("post_auth_delay", PGC_BACKEND, DEVELOPER_OPTIONS,
            cstr!("Sets the amount of time to wait after authentication on connection startup."),
            cstr!("This allows attaching a debugger to the process."),
            GUC_NOT_IN_SAMPLE | GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut PostAuthDelay },
        boot_val: 0, min: 0, max: INT_MAX / 1000000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("default_statistics_target", PGC_USERSET, QUERY_TUNING_OTHER,
            cstr!("Sets the default statistics target."),
            cstr!("This applies to table columns that have not had a column-specific target set via ALTER TABLE SET STATISTICS."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut default_statistics_target },
        boot_val: 100, min: 1, max: MAX_STATISTICS_TARGET,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 100, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("from_collapse_limit", PGC_USERSET, QUERY_TUNING_OTHER,
            cstr!("Sets the FROM-list size beyond which subqueries are not collapsed."),
            cstr!("The planner will merge subqueries into upper queries if the resulting FROM list would have no more than this many items."),
            GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut from_collapse_limit },
        boot_val: 8, min: 1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 8, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("join_collapse_limit", PGC_USERSET, QUERY_TUNING_OTHER,
            cstr!("Sets the FROM-list size beyond which JOIN constructs are not flattened."),
            cstr!("The planner will flatten explicit JOIN constructs into lists of FROM items whenever a list of no more than this many items would result."),
            GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut join_collapse_limit },
        boot_val: 8, min: 1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 8, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("geqo_threshold", PGC_USERSET, QUERY_TUNING_GEQO,
            cstr!("Sets the threshold of FROM items beyond which GEQO is used."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut geqo_threshold },
        boot_val: 12, min: 2, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 12, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("geqo_effort", PGC_USERSET, QUERY_TUNING_GEQO,
            cstr!("GEQO: effort is used to set the default for other GEQO parameters."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut Geqo_effort },
        boot_val: DEFAULT_GEQO_EFFORT, min: MIN_GEQO_EFFORT, max: MAX_GEQO_EFFORT,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_GEQO_EFFORT, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("geqo_pool_size", PGC_USERSET, QUERY_TUNING_GEQO,
            cstr!("GEQO: number of individuals in the population."),
            cstr!("0 means use a suitable default value."),
            GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut Geqo_pool_size },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("geqo_generations", PGC_USERSET, QUERY_TUNING_GEQO,
            cstr!("GEQO: number of iterations of the algorithm."),
            cstr!("0 means use a suitable default value."),
            GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut Geqo_generations },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    // This is PGC_SUSET to prevent hiding from log_lock_waits.
    config_int {
        gen: gen_init!("deadlock_timeout", PGC_SUSET, LOCK_MANAGEMENT,
            cstr!("Sets the time to wait on a lock before checking for deadlock."),
            ptr::null(), GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut DeadlockTimeout },
        boot_val: 1000, min: 1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 1000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_standby_archive_delay", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Sets the maximum delay before canceling queries when a hot standby server is processing archived WAL data."),
            cstr!("-1 means wait forever."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut max_standby_archive_delay },
        boot_val: 30 * 1000, min: -1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 30 * 1000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_standby_streaming_delay", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Sets the maximum delay before canceling queries when a hot standby server is processing streamed WAL data."),
            cstr!("-1 means wait forever."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut max_standby_streaming_delay },
        boot_val: 30 * 1000, min: -1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 30 * 1000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("recovery_min_apply_delay", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Sets the minimum delay for applying changes during recovery."),
            ptr::null(), GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut recovery_min_apply_delay },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_receiver_status_interval", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Sets the maximum interval between WAL receiver status reports to the sending server."),
            ptr::null(), GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut wal_receiver_status_interval },
        boot_val: 10, min: 0, max: INT_MAX / 1000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 10, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_receiver_timeout", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Sets the maximum wait time to receive data from the sending server."),
            cstr!("0 disables the timeout."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut wal_receiver_timeout },
        boot_val: 60 * 1000, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 60 * 1000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_connections", PGC_POSTMASTER, CONN_AUTH_SETTINGS,
            cstr!("Sets the maximum number of concurrent connections."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut MaxConnections },
        boot_val: 100, min: 1, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 100, reset_extra: ptr::null_mut(),
    },
    // see max_connections
    config_int {
        gen: gen_init!("superuser_reserved_connections", PGC_POSTMASTER, CONN_AUTH_SETTINGS,
            cstr!("Sets the number of connection slots reserved for superusers."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut SuperuserReservedConnections },
        boot_val: 3, min: 0, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 3, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("reserved_connections", PGC_POSTMASTER, CONN_AUTH_SETTINGS,
            cstr!("Sets the number of connection slots reserved for roles with privileges of pg_use_reserved_connections."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut ReservedConnections },
        boot_val: 0, min: 0, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("min_dynamic_shared_memory", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Amount of dynamic shared memory reserved at startup."),
            ptr::null(), GUC_UNIT_MB, config_type::PGC_INT),
        variable: unsafe { &raw mut min_dynamic_shared_memory },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    /*
     * We sometimes multiply the number of shared buffers by two without
     * checking for overflow, so we mustn't allow more than INT_MAX / 2.
     */
    config_int {
        gen: gen_init!("shared_buffers", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Sets the number of shared memory buffers used by the server."),
            ptr::null(), GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut NBuffers },
        boot_val: 16384, min: 16, max: INT_MAX / 2,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 16384, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_buffer_usage_limit", PGC_USERSET, RESOURCES_MEM,
            cstr!("Sets the buffer pool size for VACUUM, ANALYZE, and autovacuum."),
            ptr::null(), GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut VacuumBufferUsageLimit },
        boot_val: 2048, min: 0, max: MAX_BAS_VAC_RING_SIZE_KB,
        check_hook: Some(check_vacuum_buffer_usage_limit), assign_hook: None, show_hook: None,
        reset_val: 2048, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("shared_memory_size", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the size of the server's main shared memory area (rounded up to the nearest MB)."),
            ptr::null(),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_UNIT_MB | GUC_RUNTIME_COMPUTED,
            config_type::PGC_INT),
        variable: unsafe { &raw mut shared_memory_size_mb },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("shared_memory_size_in_huge_pages", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the number of huge pages needed for the main shared memory area."),
            cstr!("-1 means huge pages are not supported."),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_RUNTIME_COMPUTED,
            config_type::PGC_INT),
        variable: unsafe { &raw mut shared_memory_size_in_huge_pages },
        boot_val: -1, min: -1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("num_os_semaphores", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the number of semaphores required for the server."),
            ptr::null(),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_RUNTIME_COMPUTED,
            config_type::PGC_INT),
        variable: unsafe { &raw mut num_os_semaphores },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("commit_timestamp_buffers", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Sets the size of the dedicated buffer pool used for the commit timestamp cache."),
            cstr!("0 means use a fraction of \"shared_buffers\"."),
            GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut commit_timestamp_buffers },
        boot_val: 0, min: 0, max: SLRU_MAX_ALLOWED_BUFFERS,
        check_hook: Some(check_commit_ts_buffers), assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("multixact_member_buffers", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Sets the size of the dedicated buffer pool used for the MultiXact member cache."),
            ptr::null(), GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut multixact_member_buffers },
        boot_val: 32, min: 16, max: SLRU_MAX_ALLOWED_BUFFERS,
        check_hook: Some(check_multixact_member_buffers), assign_hook: None, show_hook: None,
        reset_val: 32, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("multixact_offset_buffers", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Sets the size of the dedicated buffer pool used for the MultiXact offset cache."),
            ptr::null(), GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut multixact_offset_buffers },
        boot_val: 16, min: 16, max: SLRU_MAX_ALLOWED_BUFFERS,
        check_hook: Some(check_multixact_offset_buffers), assign_hook: None, show_hook: None,
        reset_val: 16, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("notify_buffers", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Sets the size of the dedicated buffer pool used for the LISTEN/NOTIFY message cache."),
            ptr::null(), GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut notify_buffers },
        boot_val: 16, min: 16, max: SLRU_MAX_ALLOWED_BUFFERS,
        check_hook: Some(check_notify_buffers), assign_hook: None, show_hook: None,
        reset_val: 16, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("serializable_buffers", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Sets the size of the dedicated buffer pool used for the serializable transaction cache."),
            ptr::null(), GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut serializable_buffers },
        boot_val: 32, min: 16, max: SLRU_MAX_ALLOWED_BUFFERS,
        check_hook: Some(check_serial_buffers), assign_hook: None, show_hook: None,
        reset_val: 32, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("subtransaction_buffers", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Sets the size of the dedicated buffer pool used for the subtransaction cache."),
            cstr!("0 means use a fraction of \"shared_buffers\"."),
            GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut subtransaction_buffers },
        boot_val: 0, min: 0, max: SLRU_MAX_ALLOWED_BUFFERS,
        check_hook: Some(check_subtrans_buffers), assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("transaction_buffers", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Sets the size of the dedicated buffer pool used for the transaction status cache."),
            cstr!("0 means use a fraction of \"shared_buffers\"."),
            GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut transaction_buffers },
        boot_val: 0, min: 0, max: SLRU_MAX_ALLOWED_BUFFERS,
        check_hook: Some(check_transaction_buffers), assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("temp_buffers", PGC_USERSET, RESOURCES_MEM,
            cstr!("Sets the maximum number of temporary buffers used by each session."),
            ptr::null(), GUC_UNIT_BLOCKS | GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut num_temp_buffers },
        boot_val: 1024, min: 100, max: INT_MAX / 2,
        check_hook: Some(check_temp_buffers), assign_hook: None, show_hook: None,
        reset_val: 1024, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("port", PGC_POSTMASTER, CONN_AUTH_SETTINGS,
            cstr!("Sets the TCP port the server listens on."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut PostPortNumber },
        boot_val: DEF_PGPORT, min: 1, max: 65535,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEF_PGPORT, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("unix_socket_permissions", PGC_POSTMASTER, CONN_AUTH_SETTINGS,
            cstr!("Sets the access permissions of the Unix-domain socket."),
            cstr!("Unix-domain sockets use the usual Unix file system permission set. The parameter value is expected to be a numeric mode specification in the form accepted by the chmod and umask system calls. (To use the customary octal format the number must start with a 0 (zero).)"),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut Unix_socket_permissions },
        boot_val: 0o777, min: 0o000, max: 0o777,
        check_hook: None, assign_hook: None, show_hook: Some(show_unix_socket_permissions),
        reset_val: 0o777, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("log_file_mode", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Sets the file permissions for log files."),
            cstr!("The parameter value is expected to be a numeric mode specification in the form accepted by the chmod and umask system calls. (To use the customary octal format the number must start with a 0 (zero).)"),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut Log_file_mode },
        boot_val: 0o600, min: 0o000, max: 0o777,
        check_hook: None, assign_hook: None, show_hook: Some(show_log_file_mode),
        reset_val: 0o600, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("data_directory_mode", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the mode of the data directory."),
            cstr!("The parameter value is a numeric mode specification in the form accepted by the chmod and umask system calls. (To use the customary octal format the number must start with a 0 (zero).)"),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_RUNTIME_COMPUTED,
            config_type::PGC_INT),
        variable: unsafe { &raw mut data_directory_mode },
        boot_val: 0o700, min: 0o000, max: 0o777,
        check_hook: None, assign_hook: None, show_hook: Some(show_data_directory_mode),
        reset_val: 0o700, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("work_mem", PGC_USERSET, RESOURCES_MEM,
            cstr!("Sets the maximum memory to be used for query workspaces."),
            cstr!("This much memory can be used by each internal sort operation and hash table before switching to temporary disk files."),
            GUC_UNIT_KB | GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut work_mem },
        boot_val: 4096, min: 64, max: MAX_KILOBYTES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 4096, reset_extra: ptr::null_mut(),
    },
    /*
     * Dynamic shared memory has a higher overhead than local memory contexts,
     * so when testing low-memory scenarios that could use shared memory, the
     * recommended minimum is 1MB.
     */
    config_int {
        gen: gen_init!("maintenance_work_mem", PGC_USERSET, RESOURCES_MEM,
            cstr!("Sets the maximum memory to be used for maintenance operations."),
            cstr!("This includes operations such as VACUUM and CREATE INDEX."),
            GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut maintenance_work_mem },
        boot_val: 65536, min: 64, max: MAX_KILOBYTES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 65536, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("logical_decoding_work_mem", PGC_USERSET, RESOURCES_MEM,
            cstr!("Sets the maximum memory to be used for logical decoding."),
            cstr!("This much memory can be used by each internal reorder buffer before spilling to disk."),
            GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut logical_decoding_work_mem },
        boot_val: 65536, min: 64, max: MAX_KILOBYTES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 65536, reset_extra: ptr::null_mut(),
    },
    /*
     * We use the hopefully-safely-small value of 100kB as the compiled-in
     * default for max_stack_depth.  InitializeGUCOptions will increase it if
     * possible, depending on the actual platform-specific stack limit.
     */
    config_int {
        gen: gen_init!("max_stack_depth", PGC_SUSET, RESOURCES_MEM,
            cstr!("Sets the maximum stack depth, in kilobytes."),
            ptr::null(), GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut max_stack_depth },
        boot_val: 100, min: 100, max: MAX_KILOBYTES,
        check_hook: Some(check_max_stack_depth), assign_hook: Some(assign_max_stack_depth), show_hook: None,
        reset_val: 100, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("temp_file_limit", PGC_SUSET, RESOURCES_DISK,
            cstr!("Limits the total size of all temporary files used by each process."),
            cstr!("-1 means no limit."),
            GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut temp_file_limit },
        boot_val: -1, min: -1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_cost_page_hit", PGC_USERSET, VACUUM_COST_DELAY,
            cstr!("Vacuum cost for a page found in the buffer cache."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut VacuumCostPageHit },
        boot_val: 1, min: 0, max: 10000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_cost_page_miss", PGC_USERSET, VACUUM_COST_DELAY,
            cstr!("Vacuum cost for a page not found in the buffer cache."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut VacuumCostPageMiss },
        boot_val: 2, min: 0, max: 10000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 2, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_cost_page_dirty", PGC_USERSET, VACUUM_COST_DELAY,
            cstr!("Vacuum cost for a page dirtied by vacuum."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut VacuumCostPageDirty },
        boot_val: 20, min: 0, max: 10000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 20, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_cost_limit", PGC_USERSET, VACUUM_COST_DELAY,
            cstr!("Vacuum cost amount available before napping."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut VacuumCostLimit },
        boot_val: 200, min: 1, max: 10000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 200, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("autovacuum_vacuum_cost_limit", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Vacuum cost amount available before napping, for autovacuum."),
            cstr!("-1 means use \"vacuum_cost_limit\"."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_vac_cost_limit },
        boot_val: -1, min: -1, max: 10000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_files_per_process", PGC_POSTMASTER, RESOURCES_KERNEL,
            cstr!("Sets the maximum number of files each server process is allowed to open simultaneously."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_files_per_process },
        boot_val: 1000, min: 64, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 1000, reset_extra: ptr::null_mut(),
    },
    /*
     * See also CheckRequiredParameterValues() if this parameter changes
     */
    config_int {
        gen: gen_init!("max_prepared_transactions", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Sets the maximum number of simultaneously prepared transactions."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_prepared_xacts },
        boot_val: 0, min: 0, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("statement_timeout", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the maximum allowed duration of any statement."),
            cstr!("0 disables the timeout."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut StatementTimeout },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("lock_timeout", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the maximum allowed duration of any wait for a lock."),
            cstr!("0 disables the timeout."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut LockTimeout },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("idle_in_transaction_session_timeout", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the maximum allowed idle time between queries, when in a transaction."),
            cstr!("0 disables the timeout."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut IdleInTransactionSessionTimeout },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("transaction_timeout", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the maximum allowed duration of any transaction within a session (not a prepared transaction)."),
            cstr!("0 disables the timeout."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut TransactionTimeout },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: Some(assign_transaction_timeout), show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("idle_session_timeout", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the maximum allowed idle time between queries, when not in a transaction."),
            cstr!("0 disables the timeout."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut IdleSessionTimeout },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_freeze_min_age", PGC_USERSET, VACUUM_FREEZING,
            cstr!("Minimum age at which VACUUM should freeze a table row."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut vacuum_freeze_min_age },
        boot_val: 50000000, min: 0, max: 1000000000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 50000000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_freeze_table_age", PGC_USERSET, VACUUM_FREEZING,
            cstr!("Age at which VACUUM should scan whole table to freeze tuples."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut vacuum_freeze_table_age },
        boot_val: 150000000, min: 0, max: 2000000000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 150000000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_multixact_freeze_min_age", PGC_USERSET, VACUUM_FREEZING,
            cstr!("Minimum age at which VACUUM should freeze a MultiXactId in a table row."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut vacuum_multixact_freeze_min_age },
        boot_val: 5000000, min: 0, max: 1000000000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 5000000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_multixact_freeze_table_age", PGC_USERSET, VACUUM_FREEZING,
            cstr!("Multixact age at which VACUUM should scan whole table to freeze tuples."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut vacuum_multixact_freeze_table_age },
        boot_val: 150000000, min: 0, max: 2000000000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 150000000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_failsafe_age", PGC_USERSET, VACUUM_FREEZING,
            cstr!("Age at which VACUUM should trigger failsafe to avoid a wraparound outage."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut vacuum_failsafe_age },
        boot_val: 1600000000, min: 0, max: 2100000000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 1600000000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("vacuum_multixact_failsafe_age", PGC_USERSET, VACUUM_FREEZING,
            cstr!("Multixact age at which VACUUM should trigger failsafe to avoid a wraparound outage."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut vacuum_multixact_failsafe_age },
        boot_val: 1600000000, min: 0, max: 2100000000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 1600000000, reset_extra: ptr::null_mut(),
    },
    /*
     * See also CheckRequiredParameterValues() if this parameter changes
     */
    config_int {
        gen: gen_init!("max_locks_per_transaction", PGC_POSTMASTER, LOCK_MANAGEMENT,
            cstr!("Sets the maximum number of locks per transaction."),
            cstr!("The shared lock table is sized on the assumption that at most \"max_locks_per_transaction\" objects per server process or prepared transaction will need to be locked at any one time."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_locks_per_xact },
        boot_val: 64, min: 10, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 64, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_pred_locks_per_transaction", PGC_POSTMASTER, LOCK_MANAGEMENT,
            cstr!("Sets the maximum number of predicate locks per transaction."),
            cstr!("The shared predicate lock table is sized on the assumption that at most \"max_pred_locks_per_transaction\" objects per server process or prepared transaction will need to be locked at any one time."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_predicate_locks_per_xact },
        boot_val: 64, min: 10, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 64, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_pred_locks_per_relation", PGC_SIGHUP, LOCK_MANAGEMENT,
            cstr!("Sets the maximum number of predicate-locked pages and tuples per relation."),
            cstr!("If more than this total of pages and tuples in the same relation are locked by a connection, those locks are replaced by a relation-level lock."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_predicate_locks_per_relation },
        boot_val: -2, min: c_int::MIN, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: -2, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_pred_locks_per_page", PGC_SIGHUP, LOCK_MANAGEMENT,
            cstr!("Sets the maximum number of predicate-locked tuples per page."),
            cstr!("If more than this number of tuples on the same page are locked by a connection, those locks are replaced by a page-level lock."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_predicate_locks_per_page },
        boot_val: 2, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 2, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("authentication_timeout", PGC_SIGHUP, CONN_AUTH_AUTH,
            cstr!("Sets the maximum allowed time to complete client authentication."),
            ptr::null(), GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut AuthenticationTimeout },
        boot_val: 60, min: 1, max: 600,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 60, reset_extra: ptr::null_mut(),
    },
    // Not for general use
    config_int {
        gen: gen_init!("pre_auth_delay", PGC_SIGHUP, DEVELOPER_OPTIONS,
            cstr!("Sets the amount of time to wait before authentication on connection startup."),
            cstr!("This allows attaching a debugger to the process."),
            GUC_NOT_IN_SAMPLE | GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut PreAuthDelay },
        boot_val: 0, min: 0, max: 60,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_notify_queue_pages", PGC_POSTMASTER, RESOURCES_DISK,
            cstr!("Sets the maximum number of allocated pages for NOTIFY / LISTEN queue."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_notify_queue_pages },
        boot_val: 1048576, min: 64, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 1048576, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_decode_buffer_size", PGC_POSTMASTER, WAL_RECOVERY,
            cstr!("Buffer size for reading ahead in the WAL during recovery."),
            cstr!("Maximum distance to read ahead in the WAL to prefetch referenced data blocks."),
            GUC_UNIT_BYTE, config_type::PGC_INT),
        variable: unsafe { &raw mut wal_decode_buffer_size },
        boot_val: 512 * 1024, min: 64 * 1024, max: MaxAllocSize,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 512 * 1024, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_keep_size", PGC_SIGHUP, REPLICATION_SENDING,
            cstr!("Sets the size of WAL files held for standby servers."),
            ptr::null(), GUC_UNIT_MB, config_type::PGC_INT),
        variable: unsafe { &raw mut wal_keep_size_mb },
        boot_val: 0, min: 0, max: MAX_KILOBYTES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("min_wal_size", PGC_SIGHUP, WAL_CHECKPOINTS,
            cstr!("Sets the minimum size to shrink the WAL to."),
            ptr::null(), GUC_UNIT_MB, config_type::PGC_INT),
        variable: unsafe { &raw mut min_wal_size_mb },
        boot_val: DEFAULT_MIN_WAL_SEGS * (DEFAULT_XLOG_SEG_SIZE / (1024 * 1024)),
        min: 2, max: MAX_KILOBYTES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_MIN_WAL_SEGS * (DEFAULT_XLOG_SEG_SIZE / (1024 * 1024)),
        reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_wal_size", PGC_SIGHUP, WAL_CHECKPOINTS,
            cstr!("Sets the WAL size that triggers a checkpoint."),
            ptr::null(), GUC_UNIT_MB, config_type::PGC_INT),
        variable: unsafe { &raw mut max_wal_size_mb },
        boot_val: DEFAULT_MAX_WAL_SEGS * (DEFAULT_XLOG_SEG_SIZE / (1024 * 1024)),
        min: 2, max: MAX_KILOBYTES,
        check_hook: None, assign_hook: Some(assign_max_wal_size), show_hook: None,
        reset_val: DEFAULT_MAX_WAL_SEGS * (DEFAULT_XLOG_SEG_SIZE / (1024 * 1024)),
        reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("checkpoint_timeout", PGC_SIGHUP, WAL_CHECKPOINTS,
            cstr!("Sets the maximum time between automatic WAL checkpoints."),
            ptr::null(), GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut CheckPointTimeout },
        boot_val: 300, min: 30, max: 86400,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 300, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("checkpoint_warning", PGC_SIGHUP, WAL_CHECKPOINTS,
            cstr!("Sets the maximum time before warning if checkpoints triggered by WAL volume happen too frequently."),
            cstr!("Write a message to the server log if checkpoints caused by the filling of WAL segment files happen more frequently than this amount of time. 0 disables the warning."),
            GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut CheckPointWarning },
        boot_val: 30, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 30, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("checkpoint_flush_after", PGC_SIGHUP, WAL_CHECKPOINTS,
            cstr!("Number of pages after which previously performed writes are flushed to disk."),
            cstr!("0 disables forced writeback."),
            GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut checkpoint_flush_after },
        boot_val: DEFAULT_CHECKPOINT_FLUSH_AFTER, min: 0, max: WRITEBACK_MAX_PENDING_FLUSHES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_CHECKPOINT_FLUSH_AFTER, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_buffers", PGC_POSTMASTER, WAL_SETTINGS,
            cstr!("Sets the number of disk-page buffers in shared memory for WAL."),
            cstr!("-1 means use a fraction of \"shared_buffers\"."),
            GUC_UNIT_XBLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut XLOGbuffers },
        boot_val: -1, min: -1, max: INT_MAX / XLOG_BLCKSZ,
        check_hook: Some(check_wal_buffers), assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_writer_delay", PGC_SIGHUP, WAL_SETTINGS,
            cstr!("Time between WAL flushes performed in the WAL writer."),
            ptr::null(), GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut WalWriterDelay },
        boot_val: 200, min: 1, max: 10000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 200, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_writer_flush_after", PGC_SIGHUP, WAL_SETTINGS,
            cstr!("Amount of WAL written out by WAL writer that triggers a flush."),
            ptr::null(), GUC_UNIT_XBLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut WalWriterFlushAfter },
        boot_val: DEFAULT_WAL_WRITER_FLUSH_AFTER, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_WAL_WRITER_FLUSH_AFTER, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_skip_threshold", PGC_USERSET, WAL_SETTINGS,
            cstr!("Minimum size of new file to fsync instead of writing WAL."),
            ptr::null(), GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut wal_skip_threshold },
        boot_val: 2048, min: 0, max: MAX_KILOBYTES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 2048, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_wal_senders", PGC_POSTMASTER, REPLICATION_SENDING,
            cstr!("Sets the maximum number of simultaneously running WAL sender processes."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_wal_senders },
        boot_val: 10, min: 0, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 10, reset_extra: ptr::null_mut(),
    },
    // see max_wal_senders
    config_int {
        gen: gen_init!("max_replication_slots", PGC_POSTMASTER, REPLICATION_SENDING,
            cstr!("Sets the maximum number of simultaneously defined replication slots."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_replication_slots },
        boot_val: 10, min: 0, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 10, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_slot_wal_keep_size", PGC_SIGHUP, REPLICATION_SENDING,
            cstr!("Sets the maximum WAL size that can be reserved by replication slots."),
            cstr!("Replication slots will be marked as failed, and segments released for deletion or recycling, if this much space is occupied by WAL on disk. -1 means no maximum."),
            GUC_UNIT_MB, config_type::PGC_INT),
        variable: unsafe { &raw mut max_slot_wal_keep_size_mb },
        boot_val: -1, min: -1, max: MAX_KILOBYTES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_sender_timeout", PGC_USERSET, REPLICATION_SENDING,
            cstr!("Sets the maximum time to wait for WAL replication."),
            ptr::null(), GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut wal_sender_timeout },
        boot_val: 60 * 1000, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 60 * 1000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("idle_replication_slot_timeout", PGC_SIGHUP, REPLICATION_SENDING,
            cstr!("Sets the duration a replication slot can remain idle before it is invalidated."),
            ptr::null(), GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut idle_replication_slot_timeout_secs },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("commit_delay", PGC_SUSET, WAL_SETTINGS,
            cstr!("Sets the delay in microseconds between transaction commit and flushing WAL to disk."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut CommitDelay },
        boot_val: 0, min: 0, max: 100000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("commit_siblings", PGC_USERSET, WAL_SETTINGS,
            cstr!("Sets the minimum number of concurrent open transactions required before performing \"commit_delay\"."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut CommitSiblings },
        boot_val: 5, min: 0, max: 1000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 5, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("extra_float_digits", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Sets the number of digits displayed for floating-point values."),
            cstr!("This affects real, double precision, and geometric data types. A zero or negative parameter value is added to the standard number of digits (FLT_DIG or DBL_DIG as appropriate). Any value greater than zero selects precise output mode."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut extra_float_digits },
        boot_val: 1, min: -15, max: 3,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("log_min_duration_sample", PGC_SUSET, LOGGING_WHEN,
            cstr!("Sets the minimum execution time above which a sample of statements will be logged. Sampling is determined by \"log_statement_sample_rate\"."),
            cstr!("-1 disables sampling. 0 means sample all statements."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut log_min_duration_sample },
        boot_val: -1, min: -1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("log_min_duration_statement", PGC_SUSET, LOGGING_WHEN,
            cstr!("Sets the minimum execution time above which all statements will be logged."),
            cstr!("-1 disables logging statement durations. 0 means log all statement durations."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut log_min_duration_statement },
        boot_val: -1, min: -1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("log_autovacuum_min_duration", PGC_SIGHUP, LOGGING_WHAT,
            cstr!("Sets the minimum execution time above which autovacuum actions will be logged."),
            cstr!("-1 disables logging autovacuum actions. 0 means log all autovacuum actions."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut Log_autovacuum_min_duration },
        boot_val: 600000, min: -1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 600000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("log_parameter_max_length", PGC_SUSET, LOGGING_WHAT,
            cstr!("Sets the maximum length in bytes of data logged for bind parameter values when logging statements."),
            cstr!("-1 means log values in full."),
            GUC_UNIT_BYTE, config_type::PGC_INT),
        variable: unsafe { &raw mut log_parameter_max_length },
        boot_val: -1, min: -1, max: INT_MAX / 2,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("log_parameter_max_length_on_error", PGC_USERSET, LOGGING_WHAT,
            cstr!("Sets the maximum length in bytes of data logged for bind parameter values when logging statements, on error."),
            cstr!("-1 means log values in full."),
            GUC_UNIT_BYTE, config_type::PGC_INT),
        variable: unsafe { &raw mut log_parameter_max_length_on_error },
        boot_val: 0, min: -1, max: INT_MAX / 2,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("bgwriter_delay", PGC_SIGHUP, RESOURCES_BGWRITER,
            cstr!("Background writer sleep time between rounds."),
            ptr::null(), GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut BgWriterDelay },
        boot_val: 200, min: 10, max: 10000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 200, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("bgwriter_lru_maxpages", PGC_SIGHUP, RESOURCES_BGWRITER,
            cstr!("Background writer maximum number of LRU pages to flush per round."),
            cstr!("0 disables background writing."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut bgwriter_lru_maxpages },
        boot_val: 100, min: 0, max: INT_MAX / 2,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 100, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("bgwriter_flush_after", PGC_SIGHUP, RESOURCES_BGWRITER,
            cstr!("Number of pages after which previously performed writes are flushed to disk."),
            cstr!("0 disables forced writeback."),
            GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut bgwriter_flush_after },
        boot_val: DEFAULT_BGWRITER_FLUSH_AFTER, min: 0, max: WRITEBACK_MAX_PENDING_FLUSHES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_BGWRITER_FLUSH_AFTER, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("effective_io_concurrency", PGC_USERSET, RESOURCES_IO,
            cstr!("Number of simultaneous requests that can be handled efficiently by the disk subsystem."),
            cstr!("0 disables simultaneous requests."),
            GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut effective_io_concurrency },
        boot_val: DEFAULT_EFFECTIVE_IO_CONCURRENCY, min: 0, max: MAX_IO_CONCURRENCY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_EFFECTIVE_IO_CONCURRENCY, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("maintenance_io_concurrency", PGC_USERSET, RESOURCES_IO,
            cstr!("A variant of \"effective_io_concurrency\" that is used for maintenance work."),
            cstr!("0 disables simultaneous requests."),
            GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut maintenance_io_concurrency },
        boot_val: DEFAULT_MAINTENANCE_IO_CONCURRENCY, min: 0, max: MAX_IO_CONCURRENCY,
        check_hook: None, assign_hook: Some(assign_maintenance_io_concurrency), show_hook: None,
        reset_val: DEFAULT_MAINTENANCE_IO_CONCURRENCY, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("io_max_combine_limit", PGC_POSTMASTER, RESOURCES_IO,
            cstr!("Server-wide limit that clamps io_combine_limit."),
            ptr::null(), GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut io_max_combine_limit },
        boot_val: DEFAULT_IO_COMBINE_LIMIT, min: 1, max: MAX_IO_COMBINE_LIMIT,
        check_hook: None, assign_hook: Some(assign_io_max_combine_limit), show_hook: None,
        reset_val: DEFAULT_IO_COMBINE_LIMIT, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("io_combine_limit", PGC_USERSET, RESOURCES_IO,
            cstr!("Limit on the size of data reads and writes."),
            ptr::null(), GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut io_combine_limit_guc },
        boot_val: DEFAULT_IO_COMBINE_LIMIT, min: 1, max: MAX_IO_COMBINE_LIMIT,
        check_hook: None, assign_hook: Some(assign_io_combine_limit), show_hook: None,
        reset_val: DEFAULT_IO_COMBINE_LIMIT, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("io_max_concurrency", PGC_POSTMASTER, RESOURCES_IO,
            cstr!("Max number of IOs that one process can execute simultaneously."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut io_max_concurrency },
        boot_val: -1, min: -1, max: 1024,
        check_hook: Some(check_io_max_concurrency), assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("io_workers", PGC_SIGHUP, RESOURCES_IO,
            cstr!("Number of IO worker processes, for io_method=worker."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut io_workers },
        boot_val: 3, min: 1, max: MAX_IO_WORKERS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 3, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("backend_flush_after", PGC_USERSET, RESOURCES_IO,
            cstr!("Number of pages after which previously performed writes are flushed to disk."),
            cstr!("0 disables forced writeback."),
            GUC_UNIT_BLOCKS, config_type::PGC_INT),
        variable: unsafe { &raw mut backend_flush_after },
        boot_val: DEFAULT_BACKEND_FLUSH_AFTER, min: 0, max: WRITEBACK_MAX_PENDING_FLUSHES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_BACKEND_FLUSH_AFTER, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_worker_processes", PGC_POSTMASTER, RESOURCES_WORKER_PROCESSES,
            cstr!("Maximum number of concurrent worker processes."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_worker_processes },
        boot_val: 8, min: 0, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 8, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_logical_replication_workers", PGC_POSTMASTER, REPLICATION_SUBSCRIBERS,
            cstr!("Maximum number of logical replication worker processes."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_logical_replication_workers },
        boot_val: 4, min: 0, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 4, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_sync_workers_per_subscription", PGC_SIGHUP, REPLICATION_SUBSCRIBERS,
            cstr!("Maximum number of table synchronization workers per subscription."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_sync_workers_per_subscription },
        boot_val: 2, min: 0, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 2, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_parallel_apply_workers_per_subscription", PGC_SIGHUP, REPLICATION_SUBSCRIBERS,
            cstr!("Maximum number of parallel apply workers per subscription."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_parallel_apply_workers_per_subscription },
        boot_val: 2, min: 0, max: MAX_PARALLEL_WORKER_LIMIT,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 2, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_active_replication_origins", PGC_POSTMASTER, REPLICATION_SUBSCRIBERS,
            cstr!("Sets the maximum number of active replication origins."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_active_replication_origins },
        boot_val: 10, min: 0, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 10, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("log_rotation_age", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Sets the amount of time to wait before forcing log file rotation."),
            cstr!("0 disables time-based creation of new log files."),
            GUC_UNIT_MIN, config_type::PGC_INT),
        variable: unsafe { &raw mut Log_RotationAge },
        boot_val: HOURS_PER_DAY * MINS_PER_HOUR, min: 0, max: INT_MAX / SECS_PER_MINUTE,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: HOURS_PER_DAY * MINS_PER_HOUR, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("log_rotation_size", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Sets the maximum size a log file can reach before being rotated."),
            cstr!("0 disables size-based creation of new log files."),
            GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut Log_RotationSize },
        boot_val: 10 * 1024, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 10 * 1024, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_function_args", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the maximum number of function arguments."),
            ptr::null(),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_INT),
        variable: unsafe { &raw mut max_function_args },
        boot_val: FUNC_MAX_ARGS, min: FUNC_MAX_ARGS, max: FUNC_MAX_ARGS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: FUNC_MAX_ARGS, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_index_keys", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the maximum number of index keys."),
            ptr::null(),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_INT),
        variable: unsafe { &raw mut max_index_keys },
        boot_val: INDEX_MAX_KEYS, min: INDEX_MAX_KEYS, max: INDEX_MAX_KEYS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: INDEX_MAX_KEYS, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_identifier_length", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the maximum identifier length."),
            ptr::null(),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_INT),
        variable: unsafe { &raw mut max_identifier_length },
        boot_val: NAMEDATALEN - 1, min: NAMEDATALEN - 1, max: NAMEDATALEN - 1,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: NAMEDATALEN - 1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("block_size", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the size of a disk block."),
            ptr::null(),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_INT),
        variable: unsafe { &raw mut block_size },
        boot_val: BLCKSZ, min: BLCKSZ, max: BLCKSZ,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: BLCKSZ, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("segment_size", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the number of pages per disk file."),
            ptr::null(),
            GUC_UNIT_BLOCKS | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_INT),
        variable: unsafe { &raw mut segment_size },
        boot_val: RELSEG_SIZE, min: RELSEG_SIZE, max: RELSEG_SIZE,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: RELSEG_SIZE, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_block_size", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the block size in the write ahead log."),
            ptr::null(),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_INT),
        variable: unsafe { &raw mut wal_block_size },
        boot_val: XLOG_BLCKSZ, min: XLOG_BLCKSZ, max: XLOG_BLCKSZ,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: XLOG_BLCKSZ, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_retrieve_retry_interval", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Sets the time to wait before retrying to retrieve WAL after a failed attempt."),
            ptr::null(), GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut wal_retrieve_retry_interval },
        boot_val: 5000, min: 1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 5000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_segment_size", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the size of write ahead log segments."),
            ptr::null(),
            GUC_UNIT_BYTE | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_RUNTIME_COMPUTED,
            config_type::PGC_INT),
        variable: unsafe { &raw mut wal_segment_size },
        boot_val: DEFAULT_XLOG_SEG_SIZE, min: WalSegMinSize, max: WalSegMaxSize,
        check_hook: Some(check_wal_segment_size), assign_hook: None, show_hook: None,
        reset_val: DEFAULT_XLOG_SEG_SIZE, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("wal_summary_keep_time", PGC_SIGHUP, WAL_SUMMARIZATION,
            cstr!("Time for which WAL summary files should be kept."),
            cstr!("0 disables automatic summary file deletion."),
            GUC_UNIT_MIN, config_type::PGC_INT),
        variable: unsafe { &raw mut wal_summary_keep_time },
        boot_val: 10 * HOURS_PER_DAY * MINS_PER_HOUR,
        min: 0, max: INT_MAX / SECS_PER_MINUTE,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 10 * HOURS_PER_DAY * MINS_PER_HOUR, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("autovacuum_naptime", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Time to sleep between autovacuum runs."),
            ptr::null(), GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_naptime },
        boot_val: 60, min: 1, max: INT_MAX / 1000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 60, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("autovacuum_vacuum_threshold", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Minimum number of tuple updates or deletes prior to vacuum."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_vac_thresh },
        boot_val: 50, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 50, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("autovacuum_vacuum_max_threshold", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Maximum number of tuple updates or deletes prior to vacuum."),
            cstr!("-1 disables the maximum threshold."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_vac_max_thresh },
        boot_val: 100000000, min: -1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 100000000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("autovacuum_vacuum_insert_threshold", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Minimum number of tuple inserts prior to vacuum."),
            cstr!("-1 disables insert vacuums."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_vac_ins_thresh },
        boot_val: 1000, min: -1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 1000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("autovacuum_analyze_threshold", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Minimum number of tuple inserts, updates, or deletes prior to analyze."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_anl_thresh },
        boot_val: 50, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 50, reset_extra: ptr::null_mut(),
    },
    // see varsup.c for why this is PGC_POSTMASTER not PGC_SIGHUP
    config_int {
        gen: gen_init!("autovacuum_freeze_max_age", PGC_POSTMASTER, VACUUM_AUTOVACUUM,
            cstr!("Age at which to autovacuum a table to prevent transaction ID wraparound."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_freeze_max_age },
        // see vacuum_failsafe_age if you change the upper-limit value.
        boot_val: 200000000, min: 100000, max: 2000000000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 200000000, reset_extra: ptr::null_mut(),
    },
    // see multixact.c for why this is PGC_POSTMASTER not PGC_SIGHUP
    config_int {
        gen: gen_init!("autovacuum_multixact_freeze_max_age", PGC_POSTMASTER, VACUUM_AUTOVACUUM,
            cstr!("Multixact age at which to autovacuum a table to prevent multixact wraparound."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_multixact_freeze_max_age },
        boot_val: 400000000, min: 10000, max: 2000000000,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 400000000, reset_extra: ptr::null_mut(),
    },
    // see max_connections
    config_int {
        gen: gen_init!("autovacuum_worker_slots", PGC_POSTMASTER, VACUUM_AUTOVACUUM,
            cstr!("Sets the number of backend slots to allocate for autovacuum workers."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_worker_slots },
        boot_val: 16, min: 1, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 16, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("autovacuum_max_workers", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Sets the maximum number of simultaneously running autovacuum worker processes."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_max_workers },
        boot_val: 3, min: 1, max: MAX_BACKENDS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 3, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_parallel_maintenance_workers", PGC_USERSET, RESOURCES_WORKER_PROCESSES,
            cstr!("Sets the maximum number of parallel processes per maintenance operation."),
            ptr::null(), 0, config_type::PGC_INT),
        variable: unsafe { &raw mut max_parallel_maintenance_workers },
        boot_val: 2, min: 0, max: MAX_PARALLEL_WORKER_LIMIT,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 2, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_parallel_workers_per_gather", PGC_USERSET, RESOURCES_WORKER_PROCESSES,
            cstr!("Sets the maximum number of parallel processes per executor node."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut max_parallel_workers_per_gather },
        boot_val: 2, min: 0, max: MAX_PARALLEL_WORKER_LIMIT,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 2, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("max_parallel_workers", PGC_USERSET, RESOURCES_WORKER_PROCESSES,
            cstr!("Sets the maximum number of parallel workers that can be active at one time."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut max_parallel_workers },
        boot_val: 8, min: 0, max: MAX_PARALLEL_WORKER_LIMIT,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 8, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("autovacuum_work_mem", PGC_SIGHUP, RESOURCES_MEM,
            cstr!("Sets the maximum memory to be used by each autovacuum worker process."),
            cstr!("-1 means use \"maintenance_work_mem\"."),
            GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut autovacuum_work_mem },
        boot_val: -1, min: -1, max: MAX_KILOBYTES,
        check_hook: Some(check_autovacuum_work_mem), assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("tcp_keepalives_idle", PGC_USERSET, CONN_AUTH_TCP,
            cstr!("Time between issuing TCP keepalives."),
            cstr!("0 means use the system default."),
            GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut tcp_keepalives_idle },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: Some(assign_tcp_keepalives_idle), show_hook: Some(show_tcp_keepalives_idle),
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("tcp_keepalives_interval", PGC_USERSET, CONN_AUTH_TCP,
            cstr!("Time between TCP keepalive retransmits."),
            cstr!("0 means use the system default."),
            GUC_UNIT_S, config_type::PGC_INT),
        variable: unsafe { &raw mut tcp_keepalives_interval },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: Some(assign_tcp_keepalives_interval), show_hook: Some(show_tcp_keepalives_interval),
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("ssl_renegotiation_limit", PGC_USERSET, COMPAT_OPTIONS_PREVIOUS,
            cstr!("SSL renegotiation is no longer supported; this can only be 0."),
            ptr::null(),
            GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_INT),
        variable: unsafe { &raw mut ssl_renegotiation_limit },
        boot_val: 0, min: 0, max: 0,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("tcp_keepalives_count", PGC_USERSET, CONN_AUTH_TCP,
            cstr!("Maximum number of TCP keepalive retransmits."),
            cstr!("Number of consecutive keepalive retransmits that can be lost before a connection is considered dead. 0 means use the system default."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut tcp_keepalives_count },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: Some(assign_tcp_keepalives_count), show_hook: Some(show_tcp_keepalives_count),
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("gin_fuzzy_search_limit", PGC_USERSET, CLIENT_CONN_OTHER,
            cstr!("Sets the maximum allowed result for exact search by GIN."),
            cstr!("0 means no limit."),
            0, config_type::PGC_INT),
        variable: unsafe { &raw mut GinFuzzySearchLimit },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("effective_cache_size", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Sets the planner's assumption about the total size of the data caches."),
            cstr!("That is, the total size of the caches (kernel cache and shared buffers) used for PostgreSQL data files. This is measured in disk pages, which are normally 8 kB each."),
            GUC_UNIT_BLOCKS | GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut effective_cache_size },
        boot_val: DEFAULT_EFFECTIVE_CACHE_SIZE, min: 1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_EFFECTIVE_CACHE_SIZE, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("min_parallel_table_scan_size", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Sets the minimum amount of table data for a parallel scan."),
            cstr!("If the planner estimates that it will read a number of table pages too small to reach this limit, a parallel scan will not be considered."),
            GUC_UNIT_BLOCKS | GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut min_parallel_table_scan_size },
        boot_val: (8 * 1024 * 1024) / BLCKSZ, min: 0, max: INT_MAX / 3,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: (8 * 1024 * 1024) / BLCKSZ, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("min_parallel_index_scan_size", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Sets the minimum amount of index data for a parallel scan."),
            cstr!("If the planner estimates that it will read a number of index pages too small to reach this limit, a parallel scan will not be considered."),
            GUC_UNIT_BLOCKS | GUC_EXPLAIN, config_type::PGC_INT),
        variable: unsafe { &raw mut min_parallel_index_scan_size },
        boot_val: (512 * 1024) / BLCKSZ, min: 0, max: INT_MAX / 3,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: (512 * 1024) / BLCKSZ, reset_extra: ptr::null_mut(),
    },
    // Can't be set in postgresql.conf
    config_int {
        gen: gen_init!("server_version_num", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the server version as an integer."),
            ptr::null(),
            GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_INT),
        variable: unsafe { &raw mut server_version_num },
        boot_val: PG_VERSION_NUM, min: PG_VERSION_NUM, max: PG_VERSION_NUM,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: PG_VERSION_NUM, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("log_temp_files", PGC_SUSET, LOGGING_WHAT,
            cstr!("Log the use of temporary files larger than this number of kilobytes."),
            cstr!("-1 disables logging temporary files. 0 means log all temporary files."),
            GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut log_temp_files },
        boot_val: -1, min: -1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: -1, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("track_activity_query_size", PGC_POSTMASTER, STATS_CUMULATIVE,
            cstr!("Sets the size reserved for pg_stat_activity.query, in bytes."),
            ptr::null(), GUC_UNIT_BYTE, config_type::PGC_INT),
        variable: unsafe { &raw mut pgstat_track_activity_query_size },
        boot_val: 1024, min: 100, max: 1048576,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 1024, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("gin_pending_list_limit", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the maximum size of the pending list for GIN index."),
            ptr::null(), GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut gin_pending_list_limit },
        boot_val: 4096, min: 64, max: MAX_KILOBYTES,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 4096, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("tcp_user_timeout", PGC_USERSET, CONN_AUTH_TCP,
            cstr!("TCP user timeout."),
            cstr!("0 means use the system default."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut tcp_user_timeout },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: Some(assign_tcp_user_timeout), show_hook: Some(show_tcp_user_timeout),
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("huge_page_size", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("The size of huge page that should be requested."),
            cstr!("0 means use the system default."),
            GUC_UNIT_KB, config_type::PGC_INT),
        variable: unsafe { &raw mut huge_page_size },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: Some(check_huge_page_size), assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("debug_discard_caches", PGC_SUSET, DEVELOPER_OPTIONS,
            cstr!("Aggressively flush system caches for debugging purposes."),
            cstr!("0 means use normal caching behavior."),
            GUC_NOT_IN_SAMPLE, config_type::PGC_INT),
        variable: unsafe { &raw mut debug_discard_caches },
        boot_val: 0, min: 0, max: 5,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("client_connection_check_interval", PGC_USERSET, CONN_AUTH_TCP,
            cstr!("Sets the time interval between checks for disconnection while running queries."),
            cstr!("0 disables connection checks."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut client_connection_check_interval },
        boot_val: 0, min: 0, max: INT_MAX,
        check_hook: Some(check_client_connection_check_interval), assign_hook: None, show_hook: None,
        reset_val: 0, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("log_startup_progress_interval", PGC_SIGHUP, LOGGING_WHEN,
            cstr!("Time between progress updates for long-running startup operations."),
            cstr!("0 disables progress updates."),
            GUC_UNIT_MS, config_type::PGC_INT),
        variable: unsafe { &raw mut log_startup_progress_interval },
        boot_val: 10000, min: 0, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 10000, reset_extra: ptr::null_mut(),
    },
    config_int {
        gen: gen_init!("scram_iterations", PGC_USERSET, CONN_AUTH_AUTH,
            cstr!("Sets the iteration count for SCRAM secret generation."),
            ptr::null(), GUC_REPORT, config_type::PGC_INT),
        variable: unsafe { &raw mut scram_sha_256_iterations },
        boot_val: SCRAM_SHA_256_DEFAULT_ITERATIONS, min: 1, max: INT_MAX,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: SCRAM_SHA_256_DEFAULT_ITERATIONS, reset_extra: ptr::null_mut(),
    },
    // End-of-list marker
    int_sentinel!(),
];

// ---------------------------------------------------------------------------
// ConfigureNamesReal[]
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut ConfigureNamesReal: [config_real; 20] = [
    config_real {
        gen: gen_init!("seq_page_cost", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Sets the planner's estimate of the cost of a sequentially fetched disk page."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut seq_page_cost },
        boot_val: DEFAULT_SEQ_PAGE_COST, min: 0.0, max: f64::INFINITY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_SEQ_PAGE_COST, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("random_page_cost", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Sets the planner's estimate of the cost of a nonsequentially fetched disk page."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut random_page_cost },
        boot_val: DEFAULT_RANDOM_PAGE_COST, min: 0.0, max: f64::INFINITY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_RANDOM_PAGE_COST, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("cpu_tuple_cost", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Sets the planner's estimate of the cost of processing each tuple (row)."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut cpu_tuple_cost },
        boot_val: DEFAULT_CPU_TUPLE_COST, min: 0.0, max: f64::INFINITY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_CPU_TUPLE_COST, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("cpu_index_tuple_cost", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Sets the planner's estimate of the cost of processing each index entry during an index scan."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut cpu_index_tuple_cost },
        boot_val: DEFAULT_CPU_INDEX_TUPLE_COST, min: 0.0, max: f64::INFINITY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_CPU_INDEX_TUPLE_COST, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("cpu_operator_cost", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Sets the planner's estimate of the cost of processing each operator or function call."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut cpu_operator_cost },
        boot_val: DEFAULT_CPU_OPERATOR_COST, min: 0.0, max: f64::INFINITY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_CPU_OPERATOR_COST, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("parallel_tuple_cost", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Sets the planner's estimate of the cost of passing each tuple (row) from worker to leader backend."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut parallel_tuple_cost },
        boot_val: DEFAULT_PARALLEL_TUPLE_COST, min: 0.0, max: f64::INFINITY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_PARALLEL_TUPLE_COST, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("parallel_setup_cost", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Sets the planner's estimate of the cost of starting up worker processes for parallel query."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut parallel_setup_cost },
        boot_val: DEFAULT_PARALLEL_SETUP_COST, min: 0.0, max: f64::INFINITY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_PARALLEL_SETUP_COST, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("jit_above_cost", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Perform JIT compilation if query is more expensive."),
            cstr!("-1 disables JIT compilation."),
            GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut jit_above_cost },
        boot_val: 100000.0, min: -1.0, max: f64::INFINITY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 100000.0, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("jit_optimize_above_cost", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Optimize JIT-compiled functions if query is more expensive."),
            cstr!("-1 disables optimization."),
            GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut jit_optimize_above_cost },
        boot_val: 500000.0, min: -1.0, max: f64::INFINITY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 500000.0, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("jit_inline_above_cost", PGC_USERSET, QUERY_TUNING_COST,
            cstr!("Perform JIT inlining if query is more expensive."),
            cstr!("-1 disables inlining."),
            GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut jit_inline_above_cost },
        boot_val: 500000.0, min: -1.0, max: f64::INFINITY,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 500000.0, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("geqo_selection_bias", PGC_USERSET, QUERY_TUNING_GEQO,
            cstr!("GEQO: selective pressure within the population."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut Geqo_selection_bias },
        boot_val: DEFAULT_GEQO_SELECTION_BIAS, min: MIN_GEQO_SELECTION_BIAS, max: MAX_GEQO_SELECTION_BIAS,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_GEQO_SELECTION_BIAS, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("geqo_seed", PGC_USERSET, QUERY_TUNING_GEQO,
            cstr!("GEQO: seed for random path selection."),
            ptr::null(), GUC_EXPLAIN, config_type::PGC_REAL),
        variable: unsafe { &raw mut Geqo_seed },
        boot_val: 0.0, min: 0.0, max: 1.0,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0.0, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("vacuum_cost_delay", PGC_USERSET, VACUUM_COST_DELAY,
            cstr!("Vacuum cost delay in milliseconds."),
            ptr::null(), GUC_UNIT_MS, config_type::PGC_REAL),
        variable: unsafe { &raw mut VacuumCostDelay },
        boot_val: 0.0, min: 0.0, max: 100.0,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0.0, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("autovacuum_vacuum_cost_delay", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Vacuum cost delay in milliseconds, for autovacuum."),
            cstr!("-1 means use \"vacuum_cost_delay\"."),
            GUC_UNIT_MS, config_type::PGC_REAL),
        variable: unsafe { &raw mut autovacuum_vac_cost_delay },
        boot_val: 2.0, min: -1.0, max: 100.0,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 2.0, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("autovacuum_vacuum_scale_factor", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Number of tuple updates or deletes prior to vacuum as a fraction of reltuples."),
            ptr::null(), 0, config_type::PGC_REAL),
        variable: unsafe { &raw mut autovacuum_vac_scale },
        boot_val: 0.2, min: 0.0, max: 100.0,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0.2, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("autovacuum_vacuum_insert_scale_factor", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Number of tuple inserts prior to vacuum as a fraction of reltuples."),
            ptr::null(), 0, config_type::PGC_REAL),
        variable: unsafe { &raw mut autovacuum_vac_ins_scale },
        boot_val: 0.2, min: 0.0, max: 100.0,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0.2, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("autovacuum_analyze_scale_factor", PGC_SIGHUP, VACUUM_AUTOVACUUM,
            cstr!("Number of tuple inserts, updates, or deletes prior to analyze as a fraction of reltuples."),
            ptr::null(), 0, config_type::PGC_REAL),
        variable: unsafe { &raw mut autovacuum_anl_scale },
        boot_val: 0.1, min: 0.0, max: 100.0,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0.1, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("checkpoint_completion_target", PGC_SIGHUP, WAL_CHECKPOINTS,
            cstr!("Time spent flushing dirty buffers during checkpoint, as fraction of checkpoint interval."),
            ptr::null(), 0, config_type::PGC_REAL),
        variable: unsafe { &raw mut CheckPointCompletionTarget },
        boot_val: 0.9, min: 0.0, max: 1.0,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 0.9, reset_extra: ptr::null_mut(),
    },
    config_real {
        gen: gen_init!("log_statement_sample_rate", PGC_SUSET, LOGGING_WHEN,
            cstr!("Fraction of statements exceeding \"log_min_duration_sample\" to be logged."),
            cstr!("Use a value between 0.0 (never log) and 1.0 (always log)."),
            0, config_type::PGC_REAL),
        variable: unsafe { &raw mut log_statement_sample_rate },
        boot_val: 1.0, min: 0.0, max: 1.0,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: 1.0, reset_extra: ptr::null_mut(),
    },
    // End-of-list marker
    real_sentinel!(),
];

// ---------------------------------------------------------------------------
// ConfigureNamesString[]
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut ConfigureNamesString: [config_string; 76] = [
    config_string {
        gen: gen_init!("archive_command", PGC_SIGHUP, WAL_ARCHIVING,
            cstr!("Sets the shell command that will be called to archive a WAL file."),
            cstr!("An empty string means use \"archive_library\"."),
            0, config_type::PGC_STRING),
        variable: unsafe { &raw mut XLogArchiveCommand },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: Some(show_archive_command),
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("archive_library", PGC_SIGHUP, WAL_ARCHIVING,
            cstr!("Sets the library that will be called to archive a WAL file."),
            cstr!("An empty string means use \"archive_command\"."),
            0, config_type::PGC_STRING),
        variable: unsafe { &raw mut XLogArchiveLibrary },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("restore_command", PGC_SIGHUP, WAL_ARCHIVE_RECOVERY,
            cstr!("Sets the shell command that will be called to retrieve an archived WAL file."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut recoveryRestoreCommand },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("archive_cleanup_command", PGC_SIGHUP, WAL_ARCHIVE_RECOVERY,
            cstr!("Sets the shell command that will be executed at every restart point."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut archiveCleanupCommand },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("recovery_end_command", PGC_SIGHUP, WAL_ARCHIVE_RECOVERY,
            cstr!("Sets the shell command that will be executed once at the end of recovery."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut recoveryEndCommand },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("recovery_target_timeline", PGC_POSTMASTER, WAL_RECOVERY_TARGET,
            cstr!("Specifies the timeline to recover into."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut recovery_target_timeline_string },
        boot_val: cstr!("latest"),
        check_hook: Some(check_recovery_target_timeline), assign_hook: Some(assign_recovery_target_timeline), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("recovery_target", PGC_POSTMASTER, WAL_RECOVERY_TARGET,
            cstr!("Set to \"immediate\" to end recovery as soon as a consistent state is reached."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut recovery_target_string },
        boot_val: cstr!(""),
        check_hook: Some(check_recovery_target), assign_hook: Some(assign_recovery_target), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("recovery_target_xid", PGC_POSTMASTER, WAL_RECOVERY_TARGET,
            cstr!("Sets the transaction ID up to which recovery will proceed."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut recovery_target_xid_string },
        boot_val: cstr!(""),
        check_hook: Some(check_recovery_target_xid), assign_hook: Some(assign_recovery_target_xid), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("recovery_target_time", PGC_POSTMASTER, WAL_RECOVERY_TARGET,
            cstr!("Sets the time stamp up to which recovery will proceed."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut recovery_target_time_string },
        boot_val: cstr!(""),
        check_hook: Some(check_recovery_target_time), assign_hook: Some(assign_recovery_target_time), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("recovery_target_name", PGC_POSTMASTER, WAL_RECOVERY_TARGET,
            cstr!("Sets the named restore point up to which recovery will proceed."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut recovery_target_name_string },
        boot_val: cstr!(""),
        check_hook: Some(check_recovery_target_name), assign_hook: Some(assign_recovery_target_name), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("recovery_target_lsn", PGC_POSTMASTER, WAL_RECOVERY_TARGET,
            cstr!("Sets the LSN of the write-ahead log location up to which recovery will proceed."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut recovery_target_lsn_string },
        boot_val: cstr!(""),
        check_hook: Some(check_recovery_target_lsn), assign_hook: Some(assign_recovery_target_lsn), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("primary_conninfo", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Sets the connection string to be used to connect to the sending server."),
            ptr::null(), GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut PrimaryConnInfo },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("primary_slot_name", PGC_SIGHUP, REPLICATION_STANDBY,
            cstr!("Sets the name of the replication slot to use on the sending server."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut PrimarySlotName },
        boot_val: cstr!(""),
        check_hook: Some(check_primary_slot_name), assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("client_encoding", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Sets the client's character set encoding."),
            ptr::null(), GUC_IS_NAME | GUC_REPORT, config_type::PGC_STRING),
        variable: unsafe { &raw mut client_encoding_string },
        boot_val: cstr!("SQL_ASCII"),
        check_hook: Some(check_client_encoding), assign_hook: Some(assign_client_encoding), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("log_line_prefix", PGC_SIGHUP, LOGGING_WHAT,
            cstr!("Controls information prefixed to each log line."),
            cstr!("An empty string means no prefix."),
            0, config_type::PGC_STRING),
        variable: unsafe { &raw mut Log_line_prefix },
        boot_val: cstr!("%m [%p] "),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("log_timezone", PGC_SIGHUP, LOGGING_WHAT,
            cstr!("Sets the time zone to use in log messages."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut log_timezone_string },
        boot_val: cstr!("GMT"),
        check_hook: Some(check_log_timezone), assign_hook: Some(assign_log_timezone), show_hook: Some(show_log_timezone),
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("DateStyle", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Sets the display format for date and time values."),
            cstr!("Also controls interpretation of ambiguous date inputs."),
            GUC_LIST_INPUT | GUC_REPORT, config_type::PGC_STRING),
        variable: unsafe { &raw mut datestyle_string },
        boot_val: cstr!("ISO, MDY"),
        check_hook: Some(check_datestyle), assign_hook: Some(assign_datestyle), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("default_table_access_method", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the default table access method for new tables."),
            ptr::null(), GUC_IS_NAME, config_type::PGC_STRING),
        variable: unsafe { &raw mut default_table_access_method },
        boot_val: DEFAULT_TABLE_ACCESS_METHOD,
        check_hook: Some(check_default_table_access_method), assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("default_tablespace", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the default tablespace to create tables and indexes in."),
            cstr!("An empty string means use the database's default tablespace."),
            GUC_IS_NAME, config_type::PGC_STRING),
        variable: unsafe { &raw mut default_tablespace },
        boot_val: cstr!(""),
        check_hook: Some(check_default_tablespace), assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("temp_tablespaces", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the tablespace(s) to use for temporary tables and sort files."),
            cstr!("An empty string means use the database's default tablespace."),
            GUC_LIST_INPUT | GUC_LIST_QUOTE, config_type::PGC_STRING),
        variable: unsafe { &raw mut temp_tablespaces },
        boot_val: cstr!(""),
        check_hook: Some(check_temp_tablespaces), assign_hook: Some(assign_temp_tablespaces), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("createrole_self_grant", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets whether a CREATEROLE user automatically grants the role to themselves, and with which options."),
            cstr!("An empty string disables automatic self grants."),
            GUC_LIST_INPUT, config_type::PGC_STRING),
        variable: unsafe { &raw mut createrole_self_grant },
        boot_val: cstr!(""),
        check_hook: Some(check_createrole_self_grant), assign_hook: Some(assign_createrole_self_grant), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("dynamic_library_path", PGC_SUSET, CLIENT_CONN_OTHER,
            cstr!("Sets the path for dynamically loadable modules."),
            cstr!("If a dynamically loadable module needs to be opened and the specified name does not have a directory component (i.e., the name does not contain a slash), the system will search this path for the specified file."),
            GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut Dynamic_library_path },
        boot_val: cstr!("$libdir"),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("extension_control_path", PGC_SUSET, CLIENT_CONN_OTHER,
            cstr!("Sets the path for extension control files."),
            cstr!("The remaining extension script and secondary control files are then loaded from the same directory where the primary control file was found."),
            GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut Extension_control_path },
        boot_val: cstr!("$system"),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("krb_server_keyfile", PGC_SIGHUP, CONN_AUTH_AUTH,
            cstr!("Sets the location of the Kerberos server key file."),
            ptr::null(), GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut pg_krb_server_keyfile },
        boot_val: PG_KRB_SRVTAB,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("bonjour_name", PGC_POSTMASTER, CONN_AUTH_SETTINGS,
            cstr!("Sets the Bonjour service name."),
            cstr!("An empty string means use the computer name."),
            0, config_type::PGC_STRING),
        variable: unsafe { &raw mut bonjour_name },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("lc_messages", PGC_SUSET, CLIENT_CONN_LOCALE,
            cstr!("Sets the language in which messages are displayed."),
            cstr!("An empty string means use the operating system setting."),
            0, config_type::PGC_STRING),
        variable: unsafe { &raw mut locale_messages },
        boot_val: cstr!(""),
        check_hook: Some(check_locale_messages), assign_hook: Some(assign_locale_messages), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("lc_monetary", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Sets the locale for formatting monetary amounts."),
            cstr!("An empty string means use the operating system setting."),
            0, config_type::PGC_STRING),
        variable: unsafe { &raw mut locale_monetary },
        boot_val: cstr!("C"),
        check_hook: Some(check_locale_monetary), assign_hook: Some(assign_locale_monetary), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("lc_numeric", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Sets the locale for formatting numbers."),
            cstr!("An empty string means use the operating system setting."),
            0, config_type::PGC_STRING),
        variable: unsafe { &raw mut locale_numeric },
        boot_val: cstr!("C"),
        check_hook: Some(check_locale_numeric), assign_hook: Some(assign_locale_numeric), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("lc_time", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Sets the locale for formatting date and time values."),
            cstr!("An empty string means use the operating system setting."),
            0, config_type::PGC_STRING),
        variable: unsafe { &raw mut locale_time },
        boot_val: cstr!("C"),
        check_hook: Some(check_locale_time), assign_hook: Some(assign_locale_time), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("session_preload_libraries", PGC_SUSET, CLIENT_CONN_PRELOAD,
            cstr!("Lists shared libraries to preload into each backend."),
            ptr::null(), GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut session_preload_libraries_string },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("shared_preload_libraries", PGC_POSTMASTER, CLIENT_CONN_PRELOAD,
            cstr!("Lists shared libraries to preload into server."),
            ptr::null(), GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut shared_preload_libraries_string },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("local_preload_libraries", PGC_USERSET, CLIENT_CONN_PRELOAD,
            cstr!("Lists unprivileged shared libraries to preload into each backend."),
            ptr::null(), GUC_LIST_INPUT | GUC_LIST_QUOTE, config_type::PGC_STRING),
        variable: unsafe { &raw mut local_preload_libraries_string },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("search_path", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the schema search order for names that are not schema-qualified."),
            ptr::null(), GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_EXPLAIN | GUC_REPORT, config_type::PGC_STRING),
        variable: unsafe { &raw mut namespace_search_path },
        boot_val: cstr!("\"$user\", public"),
        check_hook: Some(check_search_path), assign_hook: Some(assign_search_path), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    // Can't be set in postgresql.conf
    config_string {
        gen: gen_init!("server_encoding", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the server (database) character set encoding."),
            ptr::null(), GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_STRING),
        variable: unsafe { &raw mut server_encoding_string },
        boot_val: cstr!("SQL_ASCII"),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    // Can't be set in postgresql.conf
    config_string {
        gen: gen_init!("server_version", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the server version."),
            ptr::null(), GUC_REPORT | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_STRING),
        variable: unsafe { &raw mut server_version_string },
        boot_val: PG_VERSION,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    // Not for general use --- used by SET ROLE
    config_string {
        gen: gen_init!("role", PGC_USERSET, UNGROUPED,
            cstr!("Sets the current role."),
            ptr::null(),
            GUC_IS_NAME | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_NOT_WHILE_SEC_REST,
            config_type::PGC_STRING),
        variable: unsafe { &raw mut role_string },
        boot_val: cstr!("none"),
        check_hook: Some(check_role), assign_hook: Some(assign_role), show_hook: Some(show_role),
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    // Not for general use --- used by SET SESSION AUTHORIZATION
    config_string {
        gen: gen_init!("session_authorization", PGC_USERSET, UNGROUPED,
            cstr!("Sets the session user name."),
            ptr::null(),
            GUC_IS_NAME | GUC_REPORT | GUC_NO_SHOW_ALL | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE | GUC_NOT_WHILE_SEC_REST,
            config_type::PGC_STRING),
        variable: unsafe { &raw mut session_authorization_string },
        boot_val: ptr::null(),
        check_hook: Some(check_session_authorization), assign_hook: Some(assign_session_authorization), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("log_destination", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Sets the destination for server log output."),
            cstr!("Valid values are combinations of \"stderr\", \"syslog\", \"csvlog\", \"jsonlog\", and \"eventlog\", depending on the platform."),
            GUC_LIST_INPUT, config_type::PGC_STRING),
        variable: unsafe { &raw mut Log_destination_string },
        boot_val: cstr!("stderr"),
        check_hook: Some(check_log_destination), assign_hook: Some(assign_log_destination), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("log_directory", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Sets the destination directory for log files."),
            cstr!("Can be specified as relative to the data directory or as absolute path."),
            GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut Log_directory },
        boot_val: cstr!("log"),
        check_hook: Some(check_canonical_path), assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("log_filename", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Sets the file name pattern for log files."),
            ptr::null(), GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut Log_filename },
        boot_val: cstr!("postgresql-%Y-%m-%d_%H%M%S.log"),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("syslog_ident", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Sets the program name used to identify PostgreSQL messages in syslog."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut syslog_ident_str },
        boot_val: cstr!("postgres"),
        check_hook: None, assign_hook: Some(assign_syslog_ident), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("event_source", PGC_POSTMASTER, LOGGING_WHERE,
            cstr!("Sets the application name used to identify PostgreSQL messages in the event log."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut event_source },
        boot_val: DEFAULT_EVENT_SOURCE,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("TimeZone", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Sets the time zone for displaying and interpreting time stamps."),
            ptr::null(), GUC_REPORT, config_type::PGC_STRING),
        variable: unsafe { &raw mut timezone_string },
        boot_val: cstr!("GMT"),
        check_hook: Some(check_timezone), assign_hook: Some(assign_timezone), show_hook: Some(show_timezone),
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("timezone_abbreviations", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Selects a file of time zone abbreviations."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut timezone_abbreviations_string },
        boot_val: ptr::null(),
        check_hook: Some(check_timezone_abbreviations), assign_hook: Some(assign_timezone_abbreviations), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("unix_socket_group", PGC_POSTMASTER, CONN_AUTH_SETTINGS,
            cstr!("Sets the owning group of the Unix-domain socket."),
            cstr!("The owning user of the socket is always the user that starts the server. An empty string means use the user's default group."),
            0, config_type::PGC_STRING),
        variable: unsafe { &raw mut Unix_socket_group },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("unix_socket_directories", PGC_POSTMASTER, CONN_AUTH_SETTINGS,
            cstr!("Sets the directories where Unix-domain sockets will be created."),
            ptr::null(), GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut Unix_socket_directories },
        boot_val: DEFAULT_PGSOCKET_DIR,
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("listen_addresses", PGC_POSTMASTER, CONN_AUTH_SETTINGS,
            cstr!("Sets the host name or IP address(es) to listen to."),
            ptr::null(), GUC_LIST_INPUT, config_type::PGC_STRING),
        variable: unsafe { &raw mut ListenAddresses },
        boot_val: cstr!("localhost"),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    /*
     * Can't be set by ALTER SYSTEM as it can lead to recursive definition
     * of data_directory.
     */
    config_string {
        gen: gen_init!("data_directory", PGC_POSTMASTER, FILE_LOCATIONS,
            cstr!("Sets the server's data directory."),
            ptr::null(), GUC_SUPERUSER_ONLY | GUC_DISALLOW_IN_AUTO_FILE, config_type::PGC_STRING),
        variable: unsafe { &raw mut data_directory },
        boot_val: ptr::null(),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("config_file", PGC_POSTMASTER, FILE_LOCATIONS,
            cstr!("Sets the server's main configuration file."),
            ptr::null(), GUC_DISALLOW_IN_FILE | GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut ConfigFileName },
        boot_val: ptr::null(),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("hba_file", PGC_POSTMASTER, FILE_LOCATIONS,
            cstr!("Sets the server's \"hba\" configuration file."),
            ptr::null(), GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut HbaFileName },
        boot_val: ptr::null(),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ident_file", PGC_POSTMASTER, FILE_LOCATIONS,
            cstr!("Sets the server's \"ident\" configuration file."),
            ptr::null(), GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut IdentFileName },
        boot_val: ptr::null(),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("external_pid_file", PGC_POSTMASTER, FILE_LOCATIONS,
            cstr!("Writes the postmaster PID to the specified file."),
            ptr::null(), GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut external_pid_file },
        boot_val: ptr::null(),
        check_hook: Some(check_canonical_path), assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_library", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Shows the name of the SSL library."),
            ptr::null(), GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_STRING),
        variable: unsafe { &raw mut ssl_library },
        // USE_SSL: "OpenSSL", else ""
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_cert_file", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Location of the SSL server certificate file."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut ssl_cert_file },
        boot_val: cstr!("server.crt"),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_key_file", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Location of the SSL server private key file."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut ssl_key_file },
        boot_val: cstr!("server.key"),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_ca_file", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Location of the SSL certificate authority file."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut ssl_ca_file },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_crl_file", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Location of the SSL certificate revocation list file."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut ssl_crl_file },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_crl_dir", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Location of the SSL certificate revocation list directory."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut ssl_crl_dir },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("synchronous_standby_names", PGC_SIGHUP, REPLICATION_PRIMARY,
            cstr!("Number of synchronous standbys and list of names of potential synchronous ones."),
            ptr::null(), GUC_LIST_INPUT, config_type::PGC_STRING),
        variable: unsafe { &raw mut SyncRepStandbyNames },
        boot_val: cstr!(""),
        check_hook: Some(check_synchronous_standby_names), assign_hook: Some(assign_synchronous_standby_names), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("default_text_search_config", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Sets default text search configuration."),
            ptr::null(), 0, config_type::PGC_STRING),
        variable: unsafe { &raw mut TSCurrentConfig },
        boot_val: cstr!("pg_catalog.simple"),
        check_hook: Some(check_default_text_search_config), assign_hook: Some(assign_default_text_search_config), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_tls13_ciphers", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Sets the list of allowed TLSv1.3 cipher suites."),
            cstr!("An empty string means use the default cipher suites."),
            GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut SSLCipherSuites },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_ciphers", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Sets the list of allowed TLSv1.2 (and lower) ciphers."),
            ptr::null(), GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut SSLCipherList },
        // USE_OPENSSL: "HIGH:MEDIUM:+3DES:!aNULL", else "none"
        boot_val: cstr!("none"),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_groups", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Sets the group(s) to use for Diffie-Hellman key exchange."),
            cstr!("Multiple groups can be specified using a colon-separated list."),
            GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut SSLECDHCurve },
        // USE_SSL: "X25519:prime256v1", else "none"
        boot_val: cstr!("none"),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_dh_params_file", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Location of the SSL DH parameters file."),
            cstr!("An empty string means use compiled-in default parameters."),
            GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut ssl_dh_params_file },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("ssl_passphrase_command", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Command to obtain passphrases for SSL."),
            cstr!("An empty string means use the built-in prompting mechanism."),
            GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut ssl_passphrase_command },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("application_name", PGC_USERSET, LOGGING_WHAT,
            cstr!("Sets the application name to be reported in statistics and logs."),
            ptr::null(), GUC_IS_NAME | GUC_REPORT | GUC_NOT_IN_SAMPLE, config_type::PGC_STRING),
        variable: unsafe { &raw mut application_name },
        boot_val: cstr!(""),
        check_hook: Some(check_application_name), assign_hook: Some(assign_application_name), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("cluster_name", PGC_POSTMASTER, PROCESS_TITLE,
            cstr!("Sets the name of the cluster, which is included in the process title."),
            ptr::null(), GUC_IS_NAME, config_type::PGC_STRING),
        variable: unsafe { &raw mut cluster_name },
        boot_val: cstr!(""),
        check_hook: Some(check_cluster_name), assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("wal_consistency_checking", PGC_SUSET, DEVELOPER_OPTIONS,
            cstr!("Sets the WAL resource managers for which WAL consistency checks are done."),
            cstr!("Full-page images will be logged for all data blocks and cross-checked against the results of WAL replay."),
            GUC_LIST_INPUT | GUC_NOT_IN_SAMPLE, config_type::PGC_STRING),
        variable: unsafe { &raw mut wal_consistency_checking_string },
        boot_val: cstr!(""),
        check_hook: Some(check_wal_consistency_checking), assign_hook: Some(assign_wal_consistency_checking), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("jit_provider", PGC_POSTMASTER, CLIENT_CONN_PRELOAD,
            cstr!("JIT provider to use."),
            ptr::null(), GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut jit_provider },
        boot_val: cstr!("llvmjit"),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("backtrace_functions", PGC_SUSET, DEVELOPER_OPTIONS,
            cstr!("Log backtrace for errors in these functions."),
            ptr::null(), GUC_NOT_IN_SAMPLE, config_type::PGC_STRING),
        variable: unsafe { &raw mut backtrace_functions },
        boot_val: cstr!(""),
        check_hook: Some(check_backtrace_functions), assign_hook: Some(assign_backtrace_functions), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("debug_io_direct", PGC_POSTMASTER, DEVELOPER_OPTIONS,
            cstr!("Use direct I/O for file access."),
            cstr!("An empty string disables direct I/O."),
            GUC_LIST_INPUT | GUC_NOT_IN_SAMPLE, config_type::PGC_STRING),
        variable: unsafe { &raw mut debug_io_direct_string },
        boot_val: cstr!(""),
        check_hook: Some(check_debug_io_direct), assign_hook: Some(assign_debug_io_direct), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("synchronized_standby_slots", PGC_SIGHUP, REPLICATION_PRIMARY,
            cstr!("Lists streaming replication standby server replication slot names that logical WAL sender processes will wait for."),
            cstr!("Logical WAL sender processes will send decoded changes to output plugins only after the specified replication slots have confirmed receiving WAL."),
            GUC_LIST_INPUT, config_type::PGC_STRING),
        variable: unsafe { &raw mut synchronized_standby_slots },
        boot_val: cstr!(""),
        check_hook: Some(check_synchronized_standby_slots), assign_hook: Some(assign_synchronized_standby_slots), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("restrict_nonsystem_relation_kind", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Prohibits access to non-system relations of specified kinds."),
            ptr::null(), GUC_LIST_INPUT | GUC_NOT_IN_SAMPLE, config_type::PGC_STRING),
        variable: unsafe { &raw mut restrict_nonsystem_relation_kind_string },
        boot_val: cstr!(""),
        check_hook: Some(check_restrict_nonsystem_relation_kind), assign_hook: Some(assign_restrict_nonsystem_relation_kind), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("oauth_validator_libraries", PGC_SIGHUP, CONN_AUTH_AUTH,
            cstr!("Lists libraries that may be called to validate OAuth v2 bearer tokens."),
            ptr::null(), GUC_LIST_INPUT | GUC_LIST_QUOTE | GUC_SUPERUSER_ONLY, config_type::PGC_STRING),
        variable: unsafe { &raw mut oauth_validator_libraries_string },
        boot_val: cstr!(""),
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    config_string {
        gen: gen_init!("log_connections", PGC_SU_BACKEND, LOGGING_WHAT,
            cstr!("Logs specified aspects of connection establishment and setup."),
            ptr::null(), GUC_LIST_INPUT, config_type::PGC_STRING),
        variable: unsafe { &raw mut log_connections_string },
        boot_val: cstr!(""),
        check_hook: Some(check_log_connections), assign_hook: Some(assign_log_connections), show_hook: None,
        reset_val: ptr::null_mut(), reset_extra: ptr::null_mut(),
    },
    // End-of-list marker
    string_sentinel!(),
];

// ---------------------------------------------------------------------------
// ConfigureNamesEnum[]
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut ConfigureNamesEnum: [config_enum; 42] = [
    config_enum {
        gen: gen_init!("backslash_quote", PGC_USERSET, COMPAT_OPTIONS_PREVIOUS,
            cstr!("Sets whether \"\\'\" is allowed in string literals."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut backslash_quote },
        boot_val: BACKSLASH_QUOTE_SAFE_ENCODING,
        options: unsafe { backslash_quote_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: BACKSLASH_QUOTE_SAFE_ENCODING, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("bytea_output", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the output format for bytea."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut bytea_output },
        boot_val: BYTEA_OUTPUT_HEX,
        options: unsafe { bytea_output_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: BYTEA_OUTPUT_HEX, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("client_min_messages", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the message levels that are sent to the client."),
            cstr!("Each level includes all the levels that follow it. The later the level, the fewer messages are sent."),
            0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut client_min_messages },
        boot_val: NOTICE,
        options: unsafe { client_message_level_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: NOTICE, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("compute_query_id", PGC_SUSET, STATS_MONITORING,
            cstr!("Enables in-core computation of query identifiers."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut compute_query_id },
        boot_val: COMPUTE_QUERY_ID_AUTO,
        options: unsafe { compute_query_id_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: COMPUTE_QUERY_ID_AUTO, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("constraint_exclusion", PGC_USERSET, QUERY_TUNING_OTHER,
            cstr!("Enables the planner to use constraints to optimize queries."),
            cstr!("Table scans will be skipped if their constraints guarantee that no rows match the query."),
            GUC_EXPLAIN, config_type::PGC_ENUM),
        variable: unsafe { &raw mut constraint_exclusion },
        boot_val: CONSTRAINT_EXCLUSION_PARTITION,
        options: unsafe { constraint_exclusion_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: CONSTRAINT_EXCLUSION_PARTITION, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("default_toast_compression", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the default compression method for compressible values."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut default_toast_compression },
        boot_val: TOAST_PGLZ_COMPRESSION,
        options: unsafe { default_toast_compression_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: TOAST_PGLZ_COMPRESSION, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("default_transaction_isolation", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the transaction isolation level of each new transaction."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut DefaultXactIsoLevel },
        boot_val: XACT_READ_COMMITTED,
        options: unsafe { isolation_level_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: XACT_READ_COMMITTED, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("transaction_isolation", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the current transaction's isolation level."),
            ptr::null(),
            GUC_NO_RESET | GUC_NO_RESET_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE,
            config_type::PGC_ENUM),
        variable: unsafe { &raw mut XactIsoLevel },
        boot_val: XACT_READ_COMMITTED,
        options: unsafe { isolation_level_options.as_ptr() },
        check_hook: Some(check_transaction_isolation), assign_hook: None, show_hook: None,
        reset_val: XACT_READ_COMMITTED, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("IntervalStyle", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Sets the display format for interval values."),
            ptr::null(), GUC_REPORT, config_type::PGC_ENUM),
        variable: unsafe { &raw mut IntervalStyle },
        boot_val: INTSTYLE_POSTGRES,
        options: unsafe { intervalstyle_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: INTSTYLE_POSTGRES, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("icu_validation_level", PGC_USERSET, CLIENT_CONN_LOCALE,
            cstr!("Log level for reporting invalid ICU locale strings."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut icu_validation_level },
        boot_val: WARNING,
        options: unsafe { icu_validation_level_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: WARNING, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("log_error_verbosity", PGC_SUSET, LOGGING_WHAT,
            cstr!("Sets the verbosity of logged messages."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut Log_error_verbosity },
        boot_val: PGERROR_DEFAULT,
        options: unsafe { log_error_verbosity_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: PGERROR_DEFAULT, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("log_min_messages", PGC_SUSET, LOGGING_WHEN,
            cstr!("Sets the message levels that are logged."),
            cstr!("Each level includes all the levels that follow it. The later the level, the fewer messages are sent."),
            0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut log_min_messages },
        boot_val: WARNING,
        options: unsafe { server_message_level_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: WARNING, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("log_min_error_statement", PGC_SUSET, LOGGING_WHEN,
            cstr!("Causes all statements generating error at or above this level to be logged."),
            cstr!("Each level includes all the levels that follow it. The later the level, the fewer messages are sent."),
            0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut log_min_error_statement },
        boot_val: ERROR,
        options: unsafe { server_message_level_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ERROR, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("log_statement", PGC_SUSET, LOGGING_WHAT,
            cstr!("Sets the type of statements logged."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut log_statement },
        boot_val: LOGSTMT_NONE,
        options: unsafe { log_statement_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: LOGSTMT_NONE, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("syslog_facility", PGC_SIGHUP, LOGGING_WHERE,
            cstr!("Sets the syslog \"facility\" to be used when syslog enabled."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut syslog_facility },
        boot_val: DEFAULT_SYSLOG_FACILITY,
        options: unsafe { syslog_facility_options.as_ptr() },
        check_hook: None, assign_hook: Some(assign_syslog_facility), show_hook: None,
        reset_val: DEFAULT_SYSLOG_FACILITY, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("session_replication_role", PGC_SUSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets the session's behavior for triggers and rewrite rules."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut SessionReplicationRole },
        boot_val: SESSION_REPLICATION_ROLE_ORIGIN,
        options: unsafe { session_replication_role_options.as_ptr() },
        check_hook: None, assign_hook: Some(assign_session_replication_role), show_hook: None,
        reset_val: SESSION_REPLICATION_ROLE_ORIGIN, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("synchronous_commit", PGC_USERSET, WAL_SETTINGS,
            cstr!("Sets the current transaction's synchronization level."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut synchronous_commit },
        boot_val: SYNCHRONOUS_COMMIT_ON,
        options: unsafe { synchronous_commit_options.as_ptr() },
        check_hook: None, assign_hook: Some(assign_synchronous_commit), show_hook: None,
        reset_val: SYNCHRONOUS_COMMIT_ON, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("archive_mode", PGC_POSTMASTER, WAL_ARCHIVING,
            cstr!("Allows archiving of WAL files using \"archive_command\"."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut XLogArchiveMode },
        boot_val: ARCHIVE_MODE_OFF,
        options: unsafe { &raw const archive_mode_options },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: ARCHIVE_MODE_OFF, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("recovery_target_action", PGC_POSTMASTER, WAL_RECOVERY_TARGET,
            cstr!("Sets the action to perform upon reaching the recovery target."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut recoveryTargetAction },
        boot_val: RECOVERY_TARGET_ACTION_PAUSE,
        options: unsafe { &raw const recovery_target_action_options },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: RECOVERY_TARGET_ACTION_PAUSE, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("track_functions", PGC_SUSET, STATS_CUMULATIVE,
            cstr!("Collects function-level statistics on database activity."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut pgstat_track_functions },
        boot_val: TRACK_FUNC_OFF,
        options: unsafe { track_function_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: TRACK_FUNC_OFF, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("stats_fetch_consistency", PGC_USERSET, STATS_CUMULATIVE,
            cstr!("Sets the consistency of accesses to statistics data."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut pgstat_fetch_consistency },
        boot_val: PGSTAT_FETCH_CONSISTENCY_CACHE,
        options: unsafe { stats_fetch_consistency.as_ptr() },
        check_hook: None, assign_hook: Some(assign_stats_fetch_consistency), show_hook: None,
        reset_val: PGSTAT_FETCH_CONSISTENCY_CACHE, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("wal_compression", PGC_SUSET, WAL_SETTINGS,
            cstr!("Compresses full-page writes written in WAL file with specified method."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut wal_compression },
        boot_val: WAL_COMPRESSION_NONE,
        options: unsafe { wal_compression_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: WAL_COMPRESSION_NONE, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("wal_level", PGC_POSTMASTER, WAL_SETTINGS,
            cstr!("Sets the level of information written to the WAL."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut wal_level },
        boot_val: WAL_LEVEL_REPLICA,
        options: unsafe { &raw const wal_level_options },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: WAL_LEVEL_REPLICA, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("dynamic_shared_memory_type", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Selects the dynamic shared memory implementation used."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut dynamic_shared_memory_type },
        boot_val: DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE,
        options: unsafe { &raw const dynamic_shared_memory_options },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_DYNAMIC_SHARED_MEMORY_TYPE, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("shared_memory_type", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Selects the shared memory implementation used for the main shared memory region."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut shared_memory_type },
        boot_val: DEFAULT_SHARED_MEMORY_TYPE,
        options: unsafe { shared_memory_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_SHARED_MEMORY_TYPE, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("file_copy_method", PGC_USERSET, RESOURCES_DISK,
            cstr!("Selects the file copy method."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut file_copy_method },
        boot_val: FILE_COPY_METHOD_COPY,
        options: unsafe { file_copy_method_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: FILE_COPY_METHOD_COPY, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("file_extend_method", PGC_SIGHUP, RESOURCES_DISK,
            cstr!("Selects the method used for extending data files."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut file_extend_method },
        boot_val: DEFAULT_FILE_EXTEND_METHOD,
        options: unsafe { file_extend_method_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEFAULT_FILE_EXTEND_METHOD, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("wal_sync_method", PGC_SIGHUP, WAL_SETTINGS,
            cstr!("Selects the method used for forcing WAL updates to disk."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut wal_sync_method },
        boot_val: DEFAULT_WAL_SYNC_METHOD,
        options: unsafe { &raw const wal_sync_method_options },
        check_hook: None, assign_hook: Some(assign_wal_sync_method), show_hook: None,
        reset_val: DEFAULT_WAL_SYNC_METHOD, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("xmlbinary", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets how binary values are to be encoded in XML."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut xmlbinary },
        boot_val: XMLBINARY_BASE64,
        options: unsafe { xmlbinary_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: XMLBINARY_BASE64, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("xmloption", PGC_USERSET, CLIENT_CONN_STATEMENT,
            cstr!("Sets whether XML data in implicit parsing and serialization operations is to be considered as documents or content fragments."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut xmloption },
        boot_val: XMLOPTION_CONTENT,
        options: unsafe { xmloption_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: XMLOPTION_CONTENT, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("huge_pages", PGC_POSTMASTER, RESOURCES_MEM,
            cstr!("Use of huge pages on Linux or Windows."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut huge_pages },
        boot_val: HUGE_PAGES_TRY,
        options: unsafe { huge_pages_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: HUGE_PAGES_TRY, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("huge_pages_status", PGC_INTERNAL, PRESET_OPTIONS,
            cstr!("Indicates the status of huge pages."),
            ptr::null(), GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE, config_type::PGC_ENUM),
        variable: unsafe { &raw mut huge_pages_status },
        boot_val: HUGE_PAGES_UNKNOWN,
        options: unsafe { huge_pages_status_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: HUGE_PAGES_UNKNOWN, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("recovery_prefetch", PGC_SIGHUP, WAL_RECOVERY,
            cstr!("Prefetch referenced blocks during recovery."),
            cstr!("Look ahead in the WAL to find references to uncached data."),
            0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut recovery_prefetch },
        boot_val: RECOVERY_PREFETCH_TRY,
        options: unsafe { recovery_prefetch_options.as_ptr() },
        check_hook: Some(check_recovery_prefetch), assign_hook: Some(assign_recovery_prefetch), show_hook: None,
        reset_val: RECOVERY_PREFETCH_TRY, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("debug_parallel_query", PGC_USERSET, DEVELOPER_OPTIONS,
            cstr!("Forces the planner's use parallel query nodes."),
            cstr!("This can be useful for testing the parallel query infrastructure by forcing the planner to generate plans that contain nodes that perform tuple communication between workers and the main process."),
            GUC_NOT_IN_SAMPLE | GUC_EXPLAIN, config_type::PGC_ENUM),
        variable: unsafe { &raw mut debug_parallel_query },
        boot_val: DEBUG_PARALLEL_OFF,
        options: unsafe { debug_parallel_query_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEBUG_PARALLEL_OFF, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("password_encryption", PGC_USERSET, CONN_AUTH_AUTH,
            cstr!("Chooses the algorithm for encrypting passwords."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut Password_encryption },
        boot_val: PASSWORD_TYPE_SCRAM_SHA_256,
        options: unsafe { password_encryption_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: PASSWORD_TYPE_SCRAM_SHA_256, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("plan_cache_mode", PGC_USERSET, QUERY_TUNING_OTHER,
            cstr!("Controls the planner's selection of custom or generic plan."),
            cstr!("Prepared statements can have custom and generic plans, and the planner will attempt to choose which is better.  This can be set to override the default behavior."),
            GUC_EXPLAIN, config_type::PGC_ENUM),
        variable: unsafe { &raw mut plan_cache_mode },
        boot_val: PLAN_CACHE_MODE_AUTO,
        options: unsafe { plan_cache_mode_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: PLAN_CACHE_MODE_AUTO, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("ssl_min_protocol_version", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Sets the minimum SSL/TLS protocol version to use."),
            ptr::null(), GUC_SUPERUSER_ONLY, config_type::PGC_ENUM),
        variable: unsafe { &raw mut ssl_min_protocol_version },
        boot_val: PG_TLS1_2_VERSION,
        // don't allow PG_TLS_ANY
        options: unsafe { ssl_protocol_versions_info.as_ptr().add(1) },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: PG_TLS1_2_VERSION, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("ssl_max_protocol_version", PGC_SIGHUP, CONN_AUTH_SSL,
            cstr!("Sets the maximum SSL/TLS protocol version to use."),
            ptr::null(), GUC_SUPERUSER_ONLY, config_type::PGC_ENUM),
        variable: unsafe { &raw mut ssl_max_protocol_version },
        boot_val: PG_TLS_ANY,
        options: unsafe { ssl_protocol_versions_info.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: PG_TLS_ANY, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("recovery_init_sync_method", PGC_SIGHUP, ERROR_HANDLING_OPTIONS,
            cstr!("Sets the method for synchronizing the data directory before crash recovery."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut recovery_init_sync_method },
        boot_val: DATA_DIR_SYNC_METHOD_FSYNC,
        options: unsafe { recovery_init_sync_method_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DATA_DIR_SYNC_METHOD_FSYNC, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("debug_logical_replication_streaming", PGC_USERSET, DEVELOPER_OPTIONS,
            cstr!("Forces immediate streaming or serialization of changes in large transactions."),
            cstr!("On the publisher, it allows streaming or serializing each change in logical decoding. On the subscriber, it allows serialization of all changes to files and notifies the parallel apply workers to read and apply them at the end of the transaction."),
            GUC_NOT_IN_SAMPLE, config_type::PGC_ENUM),
        variable: unsafe { &raw mut debug_logical_replication_streaming },
        boot_val: DEBUG_LOGICAL_REP_STREAMING_BUFFERED,
        options: unsafe { debug_logical_replication_streaming_options.as_ptr() },
        check_hook: None, assign_hook: None, show_hook: None,
        reset_val: DEBUG_LOGICAL_REP_STREAMING_BUFFERED, reset_extra: ptr::null_mut(),
    },
    config_enum {
        gen: gen_init!("io_method", PGC_POSTMASTER, RESOURCES_IO,
            cstr!("Selects the method for executing asynchronous I/O."),
            ptr::null(), 0, config_type::PGC_ENUM),
        variable: unsafe { &raw mut io_method },
        boot_val: DEFAULT_IO_METHOD,
        options: unsafe { &raw const io_method_options },
        check_hook: None, assign_hook: Some(assign_io_method), show_hook: None,
        reset_val: DEFAULT_IO_METHOD, reset_extra: ptr::null_mut(),
    },
    // End-of-list marker
    enum_sentinel!(),
];

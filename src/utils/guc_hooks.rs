//! Translated from PostgreSQL src/include/utils/guc_hooks.h
//! Per-variable check/assign/show callback functions used by GUC.
//!
//! These match the `Guc{Bool,Int,Real,String,Enum}{Check,Assign}Hook` and
//! `GucShowHook` typedefs in `crate::utils::guc`. Check hooks keep the C shape
//! (`&mut newval`, `&mut extra` opaque slot, source) -> bool ("value accepted");
//! the bool stays rather than becoming Result because it pairs with the in-place
//! newval rewrite + extra production, and these are stored as fn pointers in the
//! config records. Declarations kept in C's by-GUC-name order.

use crate::utils::guc::GucSource;

pub fn check_application_name(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_application_name(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_archive_command() -> &'static str {
    unimplemented!()
}
pub fn check_autovacuum_work_mem(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_vacuum_buffer_usage_limit(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_backtrace_functions(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_backtrace_functions(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_bonjour(_newval: &mut bool, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn check_canonical_path(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_checkpoint_completion_target(_newval: f64, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_client_connection_check_interval(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_client_encoding(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_client_encoding(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_cluster_name(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_commit_ts_buffers(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn show_data_directory_mode() -> &'static str {
    unimplemented!()
}
pub fn check_datestyle(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_datestyle(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_debug_io_direct(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_debug_io_direct(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_log_connections(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_log_connections(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_default_table_access_method(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_default_tablespace(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_default_text_search_config(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_default_text_search_config(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_default_with_oids(
    _newval: &mut bool,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_huge_page_size(_newval: &mut i32, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn assign_io_method(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_io_max_concurrency(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn show_in_hot_standby() -> &'static str {
    unimplemented!()
}
pub fn check_locale_messages(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_locale_messages(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_locale_monetary(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_locale_monetary(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_locale_numeric(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_locale_numeric(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_locale_time(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_locale_time(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_log_destination(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_log_destination(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_log_file_mode() -> &'static str {
    unimplemented!()
}
pub fn check_log_stats(_newval: &mut bool, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn check_log_timezone(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_log_timezone(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_log_timezone() -> &'static str {
    unimplemented!()
}
pub fn assign_maintenance_io_concurrency(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn assign_io_max_combine_limit(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn assign_io_combine_limit(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn assign_max_wal_size(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_max_stack_depth(_newval: &mut i32, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn assign_max_stack_depth(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_multixact_member_buffers(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_multixact_offset_buffers(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_notify_buffers(_newval: &mut i32, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn check_primary_slot_name(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_random_seed(_newval: &mut f64, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn assign_random_seed(_newval: f64, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_random_seed() -> &'static str {
    unimplemented!()
}
pub fn check_recovery_prefetch(
    _new_value: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_recovery_prefetch(_new_value: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_recovery_target(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_recovery_target(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_recovery_target_lsn(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_recovery_target_lsn(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_recovery_target_name(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_recovery_target_name(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_recovery_target_time(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_recovery_target_time(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_recovery_target_timeline(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_recovery_target_timeline(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_recovery_target_xid(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_recovery_target_xid(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_role(_newval: &mut Option<String>, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn assign_role(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_role() -> &'static str {
    unimplemented!()
}
pub fn check_restrict_nonsystem_relation_kind(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_restrict_nonsystem_relation_kind(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_search_path(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_search_path(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_serial_buffers(_newval: &mut i32, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn check_session_authorization(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_session_authorization(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn assign_session_replication_role(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn assign_stats_fetch_consistency(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_ssl(_newval: &mut bool, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn check_stage_log_stats(_newval: &mut bool, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn check_subtrans_buffers(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_synchronous_standby_names(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_synchronous_standby_names(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn assign_synchronous_commit(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn assign_syslog_facility(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn assign_syslog_ident(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn assign_tcp_keepalives_count(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_tcp_keepalives_count() -> &'static str {
    unimplemented!()
}
pub fn assign_tcp_keepalives_idle(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_tcp_keepalives_idle() -> &'static str {
    unimplemented!()
}
pub fn assign_tcp_keepalives_interval(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_tcp_keepalives_interval() -> &'static str {
    unimplemented!()
}
pub fn assign_tcp_user_timeout(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_tcp_user_timeout() -> &'static str {
    unimplemented!()
}
pub fn check_temp_buffers(_newval: &mut i32, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn check_temp_tablespaces(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_temp_tablespaces(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_timezone(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_timezone(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_timezone() -> &'static str {
    unimplemented!()
}
pub fn check_timezone_abbreviations(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_timezone_abbreviations(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_transaction_buffers(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_transaction_deferrable(
    _newval: &mut bool,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_transaction_isolation(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn check_transaction_read_only(
    _newval: &mut bool,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_transaction_timeout(_newval: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn show_unix_socket_permissions() -> &'static str {
    unimplemented!()
}
pub fn check_wal_buffers(_newval: &mut i32, _extra: &mut *mut (), _source: GucSource) -> bool {
    unimplemented!()
}
pub fn check_wal_consistency_checking(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_wal_consistency_checking(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_wal_segment_size(
    _newval: &mut i32,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_wal_sync_method(_new_wal_sync_method: i32, _extra: *mut ()) {
    unimplemented!()
}
pub fn check_synchronized_standby_slots(
    _newval: &mut Option<String>,
    _extra: &mut *mut (),
    _source: GucSource,
) -> bool {
    unimplemented!()
}
pub fn assign_synchronized_standby_slots(_newval: &str, _extra: *mut ()) {
    unimplemented!()
}

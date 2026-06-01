//! utils/guc_hooks.h - per-variable check/assign/show callback declarations used by GUC.

use std::ffi::{c_char, c_double, c_int, c_void};

// GucSource: canonical home is utils/guc.h (not yet translated). Local stub for now.
// TODO: dedup with crate::utils::guc::GucSource when guc.h lands.
pub type GucSource = c_int;

// Hook signatures mirror guc.h typedefs:
//   GucBoolCheckHook:   bool (*)(bool *newval, void **extra, GucSource source)
//   GucIntCheckHook:    bool (*)(int *newval, void **extra, GucSource source)
//   GucRealCheckHook:   bool (*)(double *newval, void **extra, GucSource source)
//   GucStringCheckHook: bool (*)(char **newval, void **extra, GucSource source)
//   GucEnumCheckHook:   bool (*)(int *newval, void **extra, GucSource source)
//   *AssignHook:        void (*)(<scalar> newval, void *extra) / (const char *newval, void *extra)
//   GucShowHook:        const char *(*)(void)

pub unsafe fn check_application_name(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_application_name(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_archive_command() -> *const c_char {
    unimplemented!()
}
pub unsafe fn check_autovacuum_work_mem(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_vacuum_buffer_usage_limit(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_backtrace_functions(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_backtrace_functions(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_bonjour(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_canonical_path(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_checkpoint_completion_target(newval: c_double, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_client_connection_check_interval(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_client_encoding(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_client_encoding(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_cluster_name(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_commit_ts_buffers(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn show_data_directory_mode() -> *const c_char {
    unimplemented!()
}
pub unsafe fn check_datestyle(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_datestyle(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_debug_io_direct(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_debug_io_direct(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_log_connections(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_log_connections(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_default_table_access_method(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_default_tablespace(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_default_text_search_config(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_default_text_search_config(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_default_with_oids(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_huge_page_size(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_io_method(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_io_max_concurrency(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn show_in_hot_standby() -> *const c_char {
    unimplemented!()
}
pub unsafe fn check_locale_messages(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_locale_messages(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_locale_monetary(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_locale_monetary(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_locale_numeric(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_locale_numeric(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_locale_time(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_locale_time(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_log_destination(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_log_destination(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_log_file_mode() -> *const c_char {
    unimplemented!()
}
pub unsafe fn check_log_stats(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_log_timezone(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_log_timezone(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_log_timezone() -> *const c_char {
    unimplemented!()
}
pub unsafe fn assign_maintenance_io_concurrency(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn assign_io_max_combine_limit(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn assign_io_combine_limit(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn assign_max_wal_size(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_max_stack_depth(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_max_stack_depth(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_multixact_member_buffers(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_multixact_offset_buffers(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_notify_buffers(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_primary_slot_name(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_random_seed(
    newval: *mut c_double,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_random_seed(newval: c_double, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_random_seed() -> *const c_char {
    unimplemented!()
}
pub unsafe fn check_recovery_prefetch(
    new_value: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_recovery_prefetch(new_value: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_recovery_target(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_recovery_target(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_recovery_target_lsn(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_recovery_target_lsn(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_recovery_target_name(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_recovery_target_name(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_recovery_target_time(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_recovery_target_time(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_recovery_target_timeline(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_recovery_target_timeline(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_recovery_target_xid(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_recovery_target_xid(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_role(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_role(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_role() -> *const c_char {
    unimplemented!()
}
pub unsafe fn check_restrict_nonsystem_relation_kind(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_restrict_nonsystem_relation_kind(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_search_path(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_search_path(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_serial_buffers(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_session_authorization(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_session_authorization(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn assign_session_replication_role(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn assign_stats_fetch_consistency(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_ssl(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_stage_log_stats(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_subtrans_buffers(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_synchronous_standby_names(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_synchronous_standby_names(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn assign_synchronous_commit(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn assign_syslog_facility(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn assign_syslog_ident(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn assign_tcp_keepalives_count(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_tcp_keepalives_count() -> *const c_char {
    unimplemented!()
}
pub unsafe fn assign_tcp_keepalives_idle(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_tcp_keepalives_idle() -> *const c_char {
    unimplemented!()
}
pub unsafe fn assign_tcp_keepalives_interval(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_tcp_keepalives_interval() -> *const c_char {
    unimplemented!()
}
pub unsafe fn assign_tcp_user_timeout(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_tcp_user_timeout() -> *const c_char {
    unimplemented!()
}
pub unsafe fn check_temp_buffers(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_temp_tablespaces(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_temp_tablespaces(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_timezone(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_timezone(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_timezone() -> *const c_char {
    unimplemented!()
}
pub unsafe fn check_timezone_abbreviations(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_timezone_abbreviations(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_transaction_buffers(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_transaction_deferrable(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_transaction_isolation(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_transaction_read_only(
    newval: *mut bool,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_transaction_timeout(newval: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn show_unix_socket_permissions() -> *const c_char {
    unimplemented!()
}
pub unsafe fn check_wal_buffers(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn check_wal_consistency_checking(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_wal_consistency_checking(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_wal_segment_size(
    newval: *mut c_int,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_wal_sync_method(new_wal_sync_method: c_int, extra: *mut c_void) {
    unimplemented!()
}
pub unsafe fn check_synchronized_standby_slots(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    source: GucSource,
) -> bool {
    unimplemented!()
}
pub unsafe fn assign_synchronized_standby_slots(newval: *const c_char, extra: *mut c_void) {
    unimplemented!()
}

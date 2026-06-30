//! Inline tests for the GUC core: set/show/reset, the context-permission
//! denial, the SET LOCAL transaction-rollback semantics, and clean rejection of
//! invalid bool/int/enum values.

use super::*;

/// Run `body` inside a fresh per-task GUC scope.
async fn in_scope<F, T>(body: F) -> T
where
    F: std::future::Future<Output = T>,
{
    guc_scope(body).await
}

#[tokio::test]
async fn userset_var_round_trips() {
    in_scope(async {
        let rc = set_config_option(
            "work_mem",
            Some("8192"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(rc, 1);
        let (name, val) = GetConfigOptionByName("work_mem", false).unwrap();
        assert_eq!(name, "work_mem");
        assert_eq!(val, "8192");
        assert_eq!(GetConfigOption("work_mem", false, false).unwrap(), "8192");
    })
    .await;
}

#[tokio::test]
async fn set_default_and_reset_restore_boot_value() {
    in_scope(async {
        // boot value of work_mem is 4096.
        set_config_option(
            "work_mem",
            Some("8192"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(GetConfigOption("work_mem", false, false).unwrap(), "8192");

        // RESET (value = None) -> back to reset_val (== boot 4096).
        let rc = set_config_option(
            "work_mem",
            None,
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(rc, 1);
        assert_eq!(GetConfigOption("work_mem", false, false).unwrap(), "4096");
    })
    .await;
}

#[tokio::test]
async fn postmaster_var_rejected_at_runtime_via_userset() {
    in_scope(async {
        let before = GetConfigOption("max_connections", false, false).unwrap();
        // PGC_POSTMASTER variable cannot be changed by a USERSET-context SET.
        let rc = set_config_option(
            "max_connections",
            Some("200"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(rc, 0, "POSTMASTER var must be rejected from USERSET context");
        let after = GetConfigOption("max_connections", false, false).unwrap();
        assert_eq!(before, after, "value must be unchanged after rejection");
    })
    .await;
}

#[tokio::test]
async fn set_local_reverts_at_transaction_end() {
    in_scope(async {
        AtStart_GUC(); // nest level 1 (a transaction)

        set_config_option(
            "work_mem",
            Some("16384"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::LOCAL,
            true,
            0,
            false,
        );
        assert_eq!(GetConfigOption("work_mem", false, false).unwrap(), "16384");

        // Commit: SET LOCAL still reverts at txn end (PG semantics).
        AtEOXact_GUC(true, 1);
        assert_eq!(GetConfigOption("work_mem", false, false).unwrap(), "4096");
    })
    .await;
}

#[tokio::test]
async fn set_then_abort_reverts() {
    in_scope(async {
        AtStart_GUC();
        set_config_option(
            "work_mem",
            Some("32768"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(GetConfigOption("work_mem", false, false).unwrap(), "32768");

        // Abort: even a plain SET reverts to the prior (boot) value.
        AtEOXact_GUC(false, 1);
        assert_eq!(GetConfigOption("work_mem", false, false).unwrap(), "4096");
    })
    .await;
}

#[tokio::test]
async fn set_commits_at_transaction_end() {
    in_scope(async {
        AtStart_GUC();
        set_config_option(
            "work_mem",
            Some("65536"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        // Commit: a plain SET keeps its value.
        AtEOXact_GUC(true, 1);
        assert_eq!(GetConfigOption("work_mem", false, false).unwrap(), "65536");
    })
    .await;
}

#[tokio::test]
async fn invalid_values_rejected_without_panic() {
    in_scope(async {
        // bool: garbage value.
        let rc = set_config_option(
            "enable_seqscan",
            Some("maybe"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(rc, 0);
        assert_eq!(
            GetConfigOption("enable_seqscan", false, false).unwrap(),
            "on"
        );

        // int: not a number.
        let rc = set_config_option(
            "work_mem",
            Some("lots"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(rc, 0);

        // int: out of range (work_mem min is 64).
        let rc = set_config_option(
            "work_mem",
            Some("1"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(rc, 0);

        // enum: not a member.
        let rc = set_config_option(
            "default_transaction_isolation",
            Some("banana"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(rc, 0);
    })
    .await;
}

#[tokio::test]
async fn enum_and_bool_round_trip_show() {
    in_scope(async {
        // bool show formatting.
        set_config_option(
            "enable_seqscan",
            Some("off"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(
            GetConfigOptionByName("enable_seqscan", false).unwrap().1,
            "off"
        );

        // enum: SHOW renders the option name, not the code.
        set_config_option(
            "default_transaction_isolation",
            Some("serializable"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(
            GetConfigOptionByName("default_transaction_isolation", false)
                .unwrap()
                .1,
            "serializable"
        );
    })
    .await;
}

#[tokio::test]
async fn unknown_variable_returns_none() {
    in_scope(async {
        assert!(GetConfigOption("no_such_guc", true, false).is_none());
        assert!(GetConfigOptionByName("no_such_guc", true).is_none());
        let rc = set_config_option(
            "no_such_guc",
            Some("x"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(rc, 0);
    })
    .await;
}

#[tokio::test]
async fn case_insensitive_name_lookup() {
    in_scope(async {
        set_config_option(
            "WORK_MEM",
            Some("2048"),
            GucContext::USERSET,
            GucSource::SESSION,
            GucAction::SET,
            true,
            0,
            false,
        );
        assert_eq!(GetConfigOption("work_mem", false, false).unwrap(), "2048");
        // Canonical name is returned regardless of input case.
        assert_eq!(GetConfigOptionByName("Work_Mem", false).unwrap().0, "work_mem");
    })
    .await;
}

//! Translated from PostgreSQL src/backend/main/main.c
//!
//! The binary entry point. PG's `main` does startup hacks (locale, env), a
//! root-user check, then dispatches on the first `--` argument to one of several
//! subprograms (bootstrap/check/describe-config/single-user/postmaster).
//!
//! Under the single-process async model the postmaster is a supervisor task, so
//! `main` builds a multi-thread tokio runtime and runs the supervisor entry
//! (`postmaster_main`) to completion. The non-postmaster dispatch arms are
//! minimal stubs for now.

use pepperdb::backend::postmaster::postmaster::postmaster_main;
use pepperdb::shared_state::SharedStateConfig;

/// Special must-be-first options for dispatching to subprograms (PG's
/// `DispatchOption`). Default (no recognized `--` option) is the postmaster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dispatch {
    Check,
    Boot,
    DescribeConfig,
    Single,
    Postmaster,
}

fn parse_dispatch(args: &[String]) -> Dispatch {
    // PG only treats a leading `--word` as a dispatch selector.
    match args.get(1).and_then(|a| a.strip_prefix("--")) {
        Some("check") => Dispatch::Check,
        Some("boot") => Dispatch::Boot,
        Some("describe-config") => Dispatch::DescribeConfig,
        Some("single") => Dispatch::Single,
        _ => Dispatch::Postmaster,
    }
}

fn main() {
    // PG's startup_hacks() / set_pglocale_pgservice() / check_root(): minimal.
    // TODO(step09+): locale setup, env scrubbing, and the not-running-as-root
    // check. Single-process: no EXEC_BACKEND forkchild arm.
    let args: Vec<String> = std::env::args().collect();

    // Multi-thread runtime: per-task state is Send (step 08), so backends can run
    // on the multi-thread scheduler. PG's postmaster is the long-lived root; this
    // runtime owns every task.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    match parse_dispatch(&args) {
        Dispatch::Postmaster => {
            runtime.block_on(postmaster_main(SharedStateConfig::default()));
        }
        // TODO(step09+): real implementations. Bootstrap/check/describe-config/
        // single-user modes are not yet ported.
        other => {
            eprintln!("pepperdb: dispatch mode {other:?} is not yet implemented");
            std::process::exit(1);
        }
    }
}

//! Entry point for the pepperdb server executable. Translated from backend/main/main.c.
//!
//! Any server process begins execution here. This performs the essential
//! startup tasks shared by every incarnation of the server and then dispatches
//! to the routine for the chosen incarnation. PostgreSQL recognizes a special
//! must-be-first `--` option that selects the subprogram - bootstrap (`boot`),
//! consistency check (`check`), GUC description (`describe-config`), or a
//! standalone single-user backend (`single`) - and otherwise launches the
//! postmaster.
//!
//! Under the single-process async model there are no separately forked or
//! exec'd children, so the `forkchild` dispatch arm has no analogue and the
//! postmaster is realized as a supervisor task rather than a long-lived parent
//! process. `main` builds a multi-threaded Tokio runtime - backends run as
//! tasks across its worker threads - and runs the postmaster supervisor to
//! completion. The platform startup hacks, locale setup, and not-running-as-root
//! check are minimal here, and the non-postmaster dispatch modes are not yet
//! implemented.

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
    // TODO(startup): locale setup, env scrubbing, and the not-running-as-root
    // check. Single-process: no EXEC_BACKEND forkchild arm.
    let args: Vec<String> = std::env::args().collect();

    // Multi-thread runtime: per-task state is Send (step 08), so backends can run
    // on the multi-thread scheduler. PG's postmaster is the long-lived root; this
    // runtime owns every task.
    #[allow(
        clippy::expect_used,
        reason = "process entry point: a runtime that cannot be built means we cannot run at all"
    )]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    match parse_dispatch(&args) {
        Dispatch::Postmaster => {
            runtime.block_on(postmaster_main(SharedStateConfig::default()));
        }
        // TODO(bootstrap): real implementations. Bootstrap/check/describe-config/
        // single-user modes are not yet ported.
        other => {
            eprintln!("pepperdb: dispatch mode {other:?} is not yet implemented");
            std::process::exit(1);
        }
    }
}

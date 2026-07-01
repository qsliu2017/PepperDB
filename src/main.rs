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

use pepperdb::backend::postmaster::postmaster::{postmaster_main, DEFAULT_PG_PORT};
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

/// The postmaster's startup parameters gathered from the command line and the
/// environment (PG's `PostmasterMain` getopt loop + the `PGDATA`/`PGPORT`
/// fallbacks). The subset step 01 needs: data directory, listen host, and port.
struct ServerOptions {
    /// PG `-D` / `$PGDATA`: the cluster data directory. `None` runs with the
    /// compiled-in relative paths (no on-disk cluster).
    data_dir: Option<String>,
    /// PG `-h` / `listen_addresses`: the single listen address (empty = all).
    host: String,
    /// PG `-p` / `$PGPORT`: the TCP port.
    port: u16,
}

/// Parse the postmaster options (PG `PostmasterMain` getopt: `-D dir`, `-h host`,
/// `-p port`), falling back to `$PGDATA` for the data directory and `$PGPORT`
/// then the compiled-in default for the port -- the fallback order PG's guc.c
/// uses. Only the flags step 01 needs are recognized; both `-D dir` and `-Ddir`
/// spellings are accepted (getopt style).
fn parse_server_options(args: &[String]) -> ServerOptions {
    fn opt_value<'a>(args: &'a [String], i: &mut usize, flag: &str) -> Option<&'a str> {
        let a = &args[*i];
        if a == flag {
            *i += 1;
            args.get(*i).map(String::as_str)
        } else {
            a.strip_prefix(flag)
        }
    }

    let mut data_dir = std::env::var("PGDATA").ok();
    let mut host = String::new();
    let mut port: Option<u16> = std::env::var("PGPORT").ok().and_then(|p| p.parse().ok());

    let mut i = 1;
    while i < args.len() {
        if let Some(v) = opt_value(args, &mut i, "-D") {
            data_dir = Some(v.to_string());
        } else if let Some(v) = opt_value(args, &mut i, "-h") {
            host = v.to_string();
        } else if let Some(v) = opt_value(args, &mut i, "-p") {
            port = v.parse().ok();
        }
        i += 1;
    }

    ServerOptions { data_dir, host, port: port.unwrap_or(DEFAULT_PG_PORT) }
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
            let opts = parse_server_options(&args);
            let config = SharedStateConfig {
                data_dir: opts.data_dir,
                ..SharedStateConfig::default()
            };
            runtime.block_on(postmaster_main(config, &opts.host, opts.port));
        }
        // TODO(bootstrap): real implementations. Bootstrap/check/describe-config/
        // single-user modes are not yet ported.
        other => {
            eprintln!("pepperdb: dispatch mode {other:?} is not yet implemented");
            std::process::exit(1);
        }
    }
}

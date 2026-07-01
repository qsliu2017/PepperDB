#!/usr/bin/env bash
#
# run.sh -- PepperDB pg_regress installcheck harness + double-baseline classifier.
# Plan 004 (pg_regress conformance), step 05.
#
# What it does
# ------------
# 1. Builds (once, cached) PostgreSQL 18.4's own `pg_regress` from the submodule
#    at ref/postgres/src/test/regress, then starts a FRESH PepperDB server on a
#    temp data directory and tears it all down at exit.
# 2. Pins deterministic-output env (PGTZ, PGDATESTYLE, LC_MESSAGES=C, ...) and
#    pre-creates the `regression` database over the wire.
# 3. Runs `pg_regress --use-existing` (installcheck against our running server)
#    with the real Homebrew psql/libpq 18.4, reusing the upstream sql/ and
#    expected/ directories DIRECTLY from the submodule (no copies).
# 4. Classifies each test against a DOUBLE baseline:
#      PASS       -- results/NAME.out == upstream expected/NAME.out
#      KNOWN-DIFF -- results/NAME.out == known_diffs/NAME.out (documented gap)
#      NEW-DIFF   -- matches neither (a regression; fails the run, exit 1)
#
# How pg_regress was obtained (macOS, Homebrew)
# ---------------------------------------------
# pg_regress is not built anywhere under ref/postgres (no configured tree). The
# Homebrew `libpq` formula, however, ships the FULL server headers AND the
# `libpgcommon.a`/`libpgport.a` static libs at exactly version 18.4 -- which is
# all pg_regress.c + pg_regress_main.c need. So instead of configuring the whole
# PG tree, we compile those two files directly against those headers/libs,
# synthesizing the one generated header they need (pg_config_paths.h). See
# build_pg_regress() below. The result is cached at .build/pg_regress and reused.
#
# Why --use-existing (and not pg_regress's own createdb)
# ------------------------------------------------------
# In installcheck mode pg_regress normally runs, over libpq:
#     DROP DATABASE IF EXISTS "regression";
#     CREATE DATABASE "regression" TEMPLATE=template0;
#     ALTER DATABASE "regression" SET lc_messages TO 'C'; (and five more)
# PepperDB today does not parse the quoted identifier / TEMPLATE clause / ALTER
# DATABASE SET forms (it does accept a plain `CREATE DATABASE regression`), so
# pg_regress would bail before running a single test. `--use-existing` skips its
# drop/create/role setup entirely, so the harness creates `regression` itself
# with the supported plain form. (Deferred server gap -- see step report.)
#
# Usage
# -----
#   run.sh                      run the whole pepper_schedule
#   run.sh TEST [TEST...]       run a subset by name (ignores the schedule)
#   run.sh --schedule FILE      run a different schedule file
#   run.sh --update-known       run the schedule, then (re)generate the
#                               known_diffs/*.out snapshots from current output
#                               for every non-PASS test (documents the gap)
#   run.sh --keep               do not delete the per-run work dir (.run/last)
#   run.sh --port N             listen on port N (default: an ephemeral pick)
#   run.sh -h | --help          this help
#
# Exit status: 0 if every test is PASS or KNOWN-DIFF; 1 if any NEW-DIFF; 2 on
# setup failure (build, server start, ...).

set -euo pipefail

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../.." && pwd)"
REGRESS_SRC="$REPO/ref/postgres/src/test/regress"   # upstream sql/ + expected/
KNOWN_DIFFS="$HERE/known_diffs"
SCHEDULE="$HERE/pepper_schedule"
BUILD_DIR="$HERE/.build"                             # cached pg_regress (gitignored)
RUN_DIR="$HERE/.run"                                 # per-run work dirs (gitignored)
PG_REGRESS="$BUILD_DIR/pg_regress"

TEST_SETUP_OVERRIDE="$HERE/test_setup_pepper.sql"   # trimmed shared fixtures

PSQL="/opt/homebrew/bin/psql"
PG_CONFIG="/opt/homebrew/bin/pg_config"
CLANG="/usr/bin/clang"                               # `cc` is shell-aliased here
PEPPER_BIN="$REPO/target/debug/pepperdb"

PGUSER_NAME="postgres"
REGRESS_DB="regression"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log()  { printf '%s\n' "== $*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit 2; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
UPDATE_KNOWN=0
KEEP_RUN=0
PORT=0
declare -a TESTS=()

usage() { sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//; s/^#$//'; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)        usage; exit 0 ;;
        --update-known)   UPDATE_KNOWN=1; shift ;;
        --keep)           KEEP_RUN=1; shift ;;
        --port)           PORT="${2:?--port needs a value}"; shift 2 ;;
        --schedule)       SCHEDULE="${2:?--schedule needs a file}"; shift 2 ;;
        --)               shift; while [[ $# -gt 0 ]]; do TESTS+=("$1"); shift; done ;;
        -*)               die "unknown option: $1" ;;
        *)                TESTS+=("$1"); shift ;;
    esac
done

# ---------------------------------------------------------------------------
# Preflight: required external tools
# ---------------------------------------------------------------------------
[[ -x "$PSQL" ]]      || die "psql not found at $PSQL (need Homebrew PostgreSQL 18.4 client)"
[[ -x "$PG_CONFIG" ]] || die "pg_config not found at $PG_CONFIG"
[[ -x "$CLANG" ]]     || die "clang not found at $CLANG"

# ---------------------------------------------------------------------------
# Build (or reuse cached) pg_regress from the submodule.
# ---------------------------------------------------------------------------
build_pg_regress() {
    if [[ -x "$PG_REGRESS" ]]; then
        log "pg_regress cached: $PG_REGRESS"
        return
    fi
    log "building pg_regress from $REGRESS_SRC (cached at $PG_REGRESS)"
    mkdir -p "$BUILD_DIR"

    local incsrv inccl libdir host libpq_prefix
    incsrv="$("$PG_CONFIG" --includedir-server)"
    inccl="$("$PG_CONFIG" --includedir)"
    libdir="$("$PG_CONFIG" --libdir)"
    host="$("$CLANG" -dumpmachine)"
    libpq_prefix="$(dirname "$libdir")"

    # The one generated header pg_regress.c pulls in. Only PGBINDIR and
    # PKGLIBDIR are actually consulted by pg_regress; the rest just satisfy the
    # header. Point them at the Homebrew libpq prefix.
    cat > "$BUILD_DIR/pg_config_paths.h" <<EOF
#define PGBINDIR "$libpq_prefix/bin"
#define PGSHAREDIR "$libpq_prefix/share/postgresql"
#define SYSCONFDIR "$libpq_prefix/etc"
#define INCLUDEDIR "$inccl"
#define PKGINCLUDEDIR "$inccl/postgresql"
#define INCLUDEDIRSERVER "$incsrv"
#define LIBDIR "$libdir"
#define PKGLIBDIR "$libdir/postgresql"
#define LOCALEDIR "$libpq_prefix/share/locale"
#define DOCDIR "$libpq_prefix/share/doc/postgresql"
#define HTMLDIR "$libpq_prefix/share/doc/postgresql"
#define MANDIR "$libpq_prefix/share/man"
EOF

    # -DHOST_TUPLE / -DSHELLPROG are the makefile's EXTRADEFS.
    "$CLANG" -O2 -I"$BUILD_DIR" -I"$incsrv" -I"$inccl" \
        -DHOST_TUPLE="\"$host\"" -DSHELLPROG="\"/bin/sh\"" \
        -c "$REGRESS_SRC/pg_regress.c" -o "$BUILD_DIR/pg_regress.o"
    "$CLANG" -O2 -I"$BUILD_DIR" -I"$incsrv" -I"$inccl" \
        -c "$REGRESS_SRC/pg_regress_main.c" -o "$BUILD_DIR/pg_regress_main.o"
    "$CLANG" "$BUILD_DIR/pg_regress.o" "$BUILD_DIR/pg_regress_main.o" \
        -L"$libdir" -lpq -lpgcommon -lpgport -o "$PG_REGRESS"
    "$PG_REGRESS" --version >/dev/null || die "built pg_regress is not runnable"
    log "pg_regress built ($("$PG_REGRESS" --version))"
}

# ---------------------------------------------------------------------------
# Build the PepperDB server if needed.
# ---------------------------------------------------------------------------
build_server() {
    if [[ -x "$PEPPER_BIN" ]]; then
        log "pepperdb binary present: $PEPPER_BIN"
        return
    fi
    log "building pepperdb (cargo build)"
    ( cd "$REPO" && cargo build ) || die "cargo build failed"
    [[ -x "$PEPPER_BIN" ]] || die "cargo build did not produce $PEPPER_BIN"
}

# ---------------------------------------------------------------------------
# Pick a free TCP port (unless one was given).
# ---------------------------------------------------------------------------
pick_port() {
    if [[ "$PORT" != 0 ]]; then echo "$PORT"; return; fi
    python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

# ---------------------------------------------------------------------------
# Server lifecycle. A fresh data dir per run; killed at exit via the trap.
# ---------------------------------------------------------------------------
DATADIR=""
SRV_PID=""
SRV_PORT=""

start_server() {
    SRV_PORT="$(pick_port)"
    DATADIR="$(mktemp -d "${TMPDIR:-/tmp}/pepper_regress_data.XXXXXX")"
    log "starting pepperdb: port=$SRV_PORT datadir=$DATADIR"
    # Deterministic-output env, matching PG's regress defaults.
    PGTZ=PST8PDT \
    PGDATESTYLE="Postgres, MDY" \
    LC_MESSAGES=C \
    PGCLIENTENCODING=SQL_ASCII \
        "$PEPPER_BIN" -D "$DATADIR" -h 127.0.0.1 -p "$SRV_PORT" \
        > "$WORK/server.log" 2>&1 &
    SRV_PID=$!

    # Wait until it accepts a connection (bootstrap-on-empty-datadir takes a
    # moment on first start).
    local i
    for i in $(seq 1 120); do
        if ! kill -0 "$SRV_PID" 2>/dev/null; then
            log "server exited during startup; tail of server.log:"
            tail -20 "$WORK/server.log" >&2 || true
            die "pepperdb failed to start"
        fi
        if "$PSQL" -h 127.0.0.1 -p "$SRV_PORT" -U "$PGUSER_NAME" -d postgres \
                -tAc 'SELECT 1' >/dev/null 2>&1; then
            log "server accepting connections after $i probe(s)"
            return
        fi
        sleep 0.5
    done
    tail -20 "$WORK/server.log" >&2 || true
    die "pepperdb did not accept connections in time"
}

stop_server() {
    [[ -n "$SRV_PID" ]] || return 0
    kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
    SRV_PID=""
}

cleanup() {
    stop_server
    [[ -n "$DATADIR" && -d "$DATADIR" ]] && rm -rf "$DATADIR"
    if [[ "$KEEP_RUN" == 0 && -n "${WORK:-}" && -d "$WORK" ]]; then
        rm -rf "$WORK"
    fi
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Create the regression database (plain form; see header note on --use-existing).
# ---------------------------------------------------------------------------
create_regression_db() {
    log "creating database $REGRESS_DB"
    if ! "$PSQL" -h 127.0.0.1 -p "$SRV_PORT" -U "$PGUSER_NAME" -d postgres \
            -v ON_ERROR_STOP=1 -c "CREATE DATABASE $REGRESS_DB" \
            > "$WORK/createdb.log" 2>&1; then
        # Tolerate the DB already existing; anything else is fatal.
        if ! "$PSQL" -h 127.0.0.1 -p "$SRV_PORT" -U "$PGUSER_NAME" \
                -d "$REGRESS_DB" -tAc 'SELECT 1' >/dev/null 2>&1; then
            cat "$WORK/createdb.log" >&2 || true
            die "could not create or reach database $REGRESS_DB"
        fi
        log "database $REGRESS_DB already usable"
    fi
}

# ---------------------------------------------------------------------------
# Assemble the list of tests to run.
# ---------------------------------------------------------------------------
resolve_tests() {
    if [[ ${#TESTS[@]} -gt 0 ]]; then
        printf '%s\n' "${TESTS[@]}"
        return
    fi
    [[ -f "$SCHEDULE" ]] || die "schedule not found: $SCHEDULE"
    # pg_regress schedule format: lines `test: a b c`. Extract the names.
    grep -E '^[[:space:]]*test:' "$SCHEDULE" \
        | sed -E 's/^[[:space:]]*test:[[:space:]]*//' \
        | tr ' ' '\n' | grep -v '^[[:space:]]*$' || true
}

# ---------------------------------------------------------------------------
# Run pg_regress in --use-existing installcheck mode over the given tests.
# Produces $OUTDIR/results/NAME.out for each.
# ---------------------------------------------------------------------------
run_pg_regress() {
    local -a names=("$@")
    OUTDIR="$WORK/out"
    mkdir -p "$OUTDIR"

    # test_setup override: pg_regress resolves each test's SQL from
    # $outputdir/sql/NAME.sql FIRST, then falls back to $inputdir/sql/NAME.sql
    # (see pg_regress_main.c). The upstream sql/test_setup.sql needs geometry,
    # inheritance, range types, SQL/C functions and a C extension the server
    # cannot run yet, so we drop our trimmed fixture into the outputdir slot; all
    # OTHER tests still come from the untouched upstream sql/ via the fallback.
    if [[ -f "$TEST_SETUP_OVERRIDE" ]]; then
        mkdir -p "$OUTDIR/sql"
        cp "$TEST_SETUP_OVERRIDE" "$OUTDIR/sql/test_setup.sql"
        log "test_setup override: $OUTDIR/sql/test_setup.sql <- $(basename "$TEST_SETUP_OVERRIDE")"
    fi

    log "running pg_regress over: ${names[*]}"
    # pg_regress itself diffs vs expected/ (that drives its own ok/not-ok), but
    # our authoritative classification is the double-baseline pass below; we let
    # pg_regress run to completion regardless of its own pass/fail.
    #
    # --bindir=/opt/homebrew/bin so it invokes the real 18.4 psql. Deterministic
    # env is exported for the psql children it spawns.
    PGTZ=PST8PDT \
    PGDATESTYLE="Postgres, MDY" \
    LC_MESSAGES=C \
    PGCLIENTENCODING=SQL_ASCII \
        "$PG_REGRESS" \
            --use-existing \
            --host=127.0.0.1 --port="$SRV_PORT" --user="$PGUSER_NAME" \
            --dbname="$REGRESS_DB" \
            --bindir=/opt/homebrew/bin \
            --inputdir="$REGRESS_SRC" \
            --expecteddir="$REGRESS_SRC" \
            --outputdir="$OUTDIR" \
            --max-connections=1 \
            "${names[@]}" \
        > "$WORK/pg_regress.log" 2>&1 || true   # its exit is not our verdict
}

# ---------------------------------------------------------------------------
# Classify one test given its produced results file.
#   echoes: PASS | KNOWN-DIFF | NEW-DIFF | NORESULT
# ---------------------------------------------------------------------------
classify_one() {
    local name="$1"
    local result="$OUTDIR/results/$name.out"
    local expected="$REGRESS_SRC/expected/$name.out"
    local known="$KNOWN_DIFFS/$name.out"

    [[ -f "$result" ]] || { echo "NORESULT"; return; }

    # test_setup is a trimmed FIXTURE, not a conformance test -- its output
    # deliberately differs from upstream expected/test_setup.out. Judge it by
    # "loaded cleanly": no error / dropped connection in its result. This keeps
    # a broken fixture from silently poisoning every downstream test.
    if [[ "$name" == "test_setup" ]]; then
        if grep -Eq '^(ERROR|FATAL|PANIC):|server closed the connection' "$result"; then
            echo "NEW-DIFF"
        else
            echo "PASS"
        fi
        return
    fi
    if [[ -f "$expected" ]] && diff -q "$expected" "$result" >/dev/null 2>&1; then
        echo "PASS"; return
    fi
    if [[ -f "$known" ]] && diff -q "$known" "$result" >/dev/null 2>&1; then
        echo "KNOWN-DIFF"; return
    fi
    echo "NEW-DIFF"
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
main() {
    WORK="$RUN_DIR/last"
    rm -rf "$WORK"; mkdir -p "$WORK"

    build_pg_regress
    build_server

    # Portable read loop (macOS system bash 3.2 has no `mapfile`).
    local -a names=()
    local line
    while IFS= read -r line; do
        [[ -n "$line" ]] && names+=("$line")
    done < <(resolve_tests)
    [[ ${#names[@]} -gt 0 ]] || die "no tests to run"

    start_server
    create_regression_db
    run_pg_regress "${names[@]}"

    # Classify + report.
    local pass=0 known=0 newdiff=0 noresult=0 total=0
    declare -a new_list=() known_list=() noresult_list=()
    printf '\n'
    printf '%-24s %s\n' "TEST" "STATUS"
    printf '%-24s %s\n' "----" "------"
    local name status
    for name in "${names[@]}"; do
        status="$(classify_one "$name")"
        total=$((total + 1))
        case "$status" in
            PASS)       pass=$((pass + 1)) ;;
            KNOWN-DIFF) known=$((known + 1)); known_list+=("$name") ;;
            NEW-DIFF)   newdiff=$((newdiff + 1)); new_list+=("$name") ;;
            NORESULT)   noresult=$((noresult + 1)); noresult_list+=("$name") ;;
        esac
        printf '%-24s %s\n' "$name" "$status"
    done

    # --update-known: snapshot every non-PASS test's current output.
    if [[ "$UPDATE_KNOWN" == 1 ]]; then
        mkdir -p "$KNOWN_DIFFS"
        local updated=0 nm
        # Snapshot NEW-DIFF + existing KNOWN-DIFF tests. Guard empty arrays for
        # bash 3.2 + `set -u`.
        for nm in ${new_list[@]+"${new_list[@]}"} ${known_list[@]+"${known_list[@]}"}; do
            local result="$OUTDIR/results/$nm.out"
            [[ -f "$result" ]] || continue
            cp "$result" "$KNOWN_DIFFS/$nm.out"
            updated=$((updated + 1))
        done
        log "--update-known: wrote $updated snapshot(s) to $KNOWN_DIFFS"
    fi

    printf '\n'
    log "conformance: $pass PASS / $total total  (KNOWN-DIFF=$known  NEW-DIFF=$newdiff  NORESULT=$noresult)"
    [[ ${#known_list[@]}    -gt 0 ]] && log "KNOWN-DIFF: ${known_list[*]}"
    [[ ${#new_list[@]}      -gt 0 ]] && log "NEW-DIFF:   ${new_list[*]}"
    [[ ${#noresult_list[@]} -gt 0 ]] && log "NORESULT:   ${noresult_list[*]}"
    log "artifacts: $WORK (server.log, pg_regress.log, out/results/, out/regression.diffs)"

    # NEW-DIFF or NORESULT (no output produced) fail the run; PASS and documented
    # KNOWN-DIFF do not. --update-known always exits 0 (it just (re)baselines).
    if [[ "$UPDATE_KNOWN" == 1 ]]; then
        return 0
    fi
    if [[ "$newdiff" -gt 0 || "$noresult" -gt 0 ]]; then
        return 1
    fi
    return 0
}

main "$@"

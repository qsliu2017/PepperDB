#!/bin/zsh
# rt.sh - fast PepperDB regression harness (single entry point).
#
# Why this exists (measured 2026-06-21):
#   - cargo release builds were 37s; `incremental=true` (Cargo.toml) -> ~10s.
#   - runners used `sleep 8` to wait for the server; pg_isready polls in ~0.13s.
#   - runners used `pkill -9 -f release/postgres` which killed *concurrent*
#     servers -> false failures. We track PIDs and kill only our own.
#   - genuine bugs are Rust panics that print "panicked at file:line"; with
#     RUST_BACKTRACE=1 (set here) they print a full backtrace too -> usually no
#     need to add an eprintln probe + rebuild + remove + rebuild.
#   - build and run are ONE command (`build && run`) so a run never uses a
#     stale binary (a recurring time sink).
#
# INFINITE-LOOP SAFETY:
#   A wedged server (tight loop that never checks for interrupts) would hang a
#   psql client forever. Defenses, both active:
#     1. soft: PGOPTIONS statement_timeout -> server aborts the query if it
#        reaches a CHECK_FOR_INTERRUPTS. Catches "polite" loops fast.
#     2. hard: a watchdog that kill -9's the server PID after TIMEOUT seconds.
#        psql then returns (connection lost). Catches non-interruptible loops.
#   Every test runs in its OWN isolated short-lived server, so one wedged test
#   never blocks the others, and the harness always makes forward progress.
#
# Usage:
#   dev/rt.sh build                 # incremental release build (fail-fast)
#   dev/rt.sh test <t1> [t2 ...]    # build, then run each test (isolated)
#   dev/rt.sh sql "<SQL>"           # build, run one query, show output+panic
#   dev/rt.sh board <t1 ...|ALL>    # parallel scoreboard (N workers)
#   dev/rt.sh nobuild <cmd> ...     # skip the build step (use current binary)
# Env: RT_TIMEOUT (per-test hard kill, default 30s), RT_WORKERS (board, def 4)
set -u
cd "${0:A:h}/.."                       # repo root
ROOT=$PWD
PGREF=$ROOT/.pgref/bin
REG=$ROOT/postgres/src/test/regress
BIN=$ROOT/target/release/postgres
export RUST_BACKTRACE=1
TIMEOUT=${RT_TIMEOUT:-30}
WORKERS=${RT_WORKERS:-4}
LIBENV="PG_ABS_SRCDIR=$REG PG_LIBDIR=/tmp/pdblib PG_DLSUFFIX=.dylib PGOPTIONS=-c statement_timeout=$((TIMEOUT*1000 - 5000))ms"

# Stage loadable modules into PG_LIBDIR (/tmp/pdblib). The test SQL resolves
# $libdir / :libdir to this dir; the dylibs are built in-tree but the dir must
# exist and contain them, else CREATE FUNCTION ... AS 'regress'/LANGUAGE plpgsql
# fail with "could not access file". Copy both each run (cheap, keeps them fresh).
setup_libdir() {
  mkdir -p /tmp/pdblib
  cp -f "$REG/regress.dylib" /tmp/pdblib/ 2>/dev/null
  cp -f "$ROOT/postgres/src/pl/plpgsql/src/plpgsql.dylib" /tmp/pdblib/ 2>/dev/null
}
setup_libdir

# macOS SysV shm limit (kern.sysv.shmmni) is ~32 and kill -9 orphans a server's
# segment -> after ~32 runs shmget fails and initdb/startup breaks. Reclaim each
# server's segment precisely (postmaster.pid line 7 = "key id"), plus sweep any
# orphaned (NATTCH=0) segments at startup as a backstop. Safe for concurrent
# board workers (their live segments have NATTCH>0).
ME="$(id -un)"
# PepperDB's postmaster does NOT free its SysV semaphores on exit (no signal
# triggers the cleanup callback - SIGTERM/INT stop the process but leak the sem
# sets; SIGQUIT is ignored). Sem sets thus leak on every shutdown and accumulate
# across runs until SEMMNS is exhausted -> semget ENOSPC -> "server-start-failed"
# for ALL tests. This harness runs one board at a time, so fully reset our SysV
# IPC (shm + sem) at startup: each run starts clean and a single run's sem usage
# stays well under the cap. (shm is also reclaimed per-test by reclaim_shm.)
reset_ipc() {
  ipcs -m | awk -v u="$ME" '$5==u{print $2}' | xargs -n1 ipcrm -m 2>/dev/null
  ipcs -s | awk -v u="$ME" '$5==u{print $2}' | xargs -n1 ipcrm -s 2>/dev/null
}
reclaim_shm() { local id; id=$(awk 'NR==7{print $2}' "$1/postmaster.pid" 2>/dev/null); [[ -n "$id" ]] && ipcrm -m "$id" 2>/dev/null; }
# stop_server <datadir>: kill the postmaster by its UNIQUE datadir in argv, never
# by PID. PIDs are reused by the OS under the board's churn, so a kill -9 <pid> or
# pkill -P <pid> from one worker's teardown/watchdog can hit ANOTHER worker's
# freshly-started server (killed before it logs a byte -> empty log ->
# "server-start-failed"). Matching "-D <datadir>" only ever hits our own server;
# its backends exit when the postmaster dies. Then reclaim the shm seg by id.
# (No graceful signal: none of SIGTERM/INT/QUIT free the SysV sems anyway.)
kill_by_dir() { pkill -9 -f "postgres -D $1 " 2>/dev/null; }
stop_server() { kill_by_dir "$1"; reclaim_shm "$1"; }
reset_ipc

build() {
  echo ">> cargo build --release (incremental)" >&2
  if cargo build --release 2>&1 | grep -qE 'error\[|error:'; then
    cargo build --release 2>&1 | grep -E 'error\[|error:' | head; echo "BUILD FAILED" >&2; return 1
  fi
  [[ -x $BIN ]] || { echo "BUILD FAILED (no binary)" >&2; return 1; }
}

# run_isolated <port> <test>  -> prints "PASS|FAIL|TIMEOUT  <test> [(diff=N)]"
# Owns a fresh server on <port>; kills only its own PID. Loop-safe.
run_isolated() {
  local port=$1 t=$2; local dir=/tmp/rt_$port
  rm -rf "$dir"
  "$PGREF/initdb" -D "$dir" -U postgres -A trust >/dev/null 2>&1
  env PG_LIBDIR=/tmp/pdblib PG_DLSUFFIX=.dylib "$BIN" -D "$dir" -p "$port" -k /tmp >"$dir.log" 2>&1 &
  local spid=$!
  # hard watchdog: kill the server (by datadir, reuse-safe) if anything wedges
  ( sleep $TIMEOUT; kill_by_dir "$dir" ) &
  local wd=$!
  local i=0
  while ! "$PGREF/pg_isready" -h 127.0.0.1 -p "$port" >/dev/null 2>&1; do
    sleep 0.2; i=$((i+1))
    if (( i > TIMEOUT*5 )) || ! kill -0 $spid 2>/dev/null; then
      kill $wd 2>/dev/null; stop_server "$dir"; rm -rf "$dir" "$dir.log"
      echo "FAIL  $t (server-start-failed)"; return
    fi
  done
  "$PGREF/psql" -X -q "host=127.0.0.1 port=$port user=postgres dbname=postgres sslmode=disable connect_timeout=10" \
    -c "CREATE DATABASE regression;" >/dev/null 2>&1
  local C="host=127.0.0.1 port=$port user=postgres dbname=regression sslmode=disable connect_timeout=10"
  # Regression tests depend on test_setup's tables; pre-run it -- but NOT when the
  # test under examination IS test_setup, else it runs twice -> spurious "already exists".
  if [[ "$t" != "test_setup" ]]; then
    env $(echo $LIBENV) "$PGREF/psql" -X -a -q "$C" < "$REG/sql/test_setup.sql" >/dev/null 2>&1
  fi
  env $(echo $LIBENV) "$PGREF/psql" -X -a -q "$C" < "$REG/sql/$t.sql" > "/tmp/rt_$t.out" 2>&1
  local alive=1; kill -0 $spid 2>/dev/null || alive=0
  kill $wd 2>/dev/null
  stop_server "$dir"
  if (( alive == 0 )); then
    echo "TIMEOUT  $t (>${TIMEOUT}s - server wedged/crashed)"
  else
    local d; d=$(diff "$REG/expected/$t.out" "/tmp/rt_$t.out" 2>/dev/null | grep -cE '^[<>]')
    if [[ "$d" == "0" ]]; then echo "PASS  $t"; else echo "FAIL  $t (diff=$d)"; fi
  fi
  # surface a panic location for quick diagnosis (no probe needed)
  grep -iE 'panicked at|internal function with OID|not yet implemented' "$dir.log" 2>/dev/null \
    | grep -vE 'panicking|library' | tail -1 | sed 's/^/      ^ /'
  rm -rf "$dir" "$dir.log"
}

cmd_test() { local p=5444; for t in "$@"; do run_isolated $p "$t"; p=$((p+1)); done; }

cmd_sql() {
  local port=5445 dir=/tmp/rt_sql
  rm -rf "$dir"; "$PGREF/initdb" -D "$dir" -U postgres -A trust >/dev/null 2>&1
  env PG_LIBDIR=/tmp/pdblib PG_DLSUFFIX=.dylib "$BIN" -D "$dir" -p $port -k /tmp >"$dir.log" 2>&1 &
  local spid=$!; ( sleep $TIMEOUT; kill_by_dir "$dir" ) & local wd=$!
  local i=0; while ! "$PGREF/pg_isready" -h 127.0.0.1 -p $port >/dev/null 2>&1; do sleep 0.2; i=$((i+1)); (( i>TIMEOUT*5 )) && break; done
  env $(echo $LIBENV) "$PGREF/psql" -X -a "host=127.0.0.1 port=$port user=postgres dbname=postgres sslmode=disable connect_timeout=10" -c "$1" 2>&1
  kill $wd 2>/dev/null; stop_server "$dir"
  echo "--- panic (if any) ---"
  grep -iE 'panicked at|not implemented|internal function with OID' "$dir.log" 2>/dev/null | grep -vE 'panicking|library' | tail -5
}

cmd_board() {
  local tests
  if [[ "${1:-}" == "ALL" ]]; then
    tests=($(ls $REG/sql/*.sql | xargs -n1 basename | sed 's/\.sql$//'))
  else tests=("$@"); fi
  rm -f /tmp/rt_board.res
  # Bounded-concurrency throttle. NOTE: `$(jobs -r | wc -l)` does NOT work - the
  # command substitution forks a subshell that can't see the parent's background
  # jobs, so it always returns 0 and the throttle never engages (every test
  # launches at once -> dozens of servers -> SysV shm cap (shmmni~32) hit ->
  # mass "server-start-failed"). `wait -n` isn't available either. So track pids
  # and poll: prune finished ones (kill -0) and block only while WORKERS are in
  # flight. Pruning ANY finished job (not FIFO) avoids head-of-line blocking when
  # one slow test would otherwise stall the whole pipeline.
  local i=0; local pids=() p alive
  for t in $tests; do
    ( echo "$(run_isolated $((5460 + i)) "$t")" >> /tmp/rt_board.res ) &
    pids+=($!); i=$((i+1))
    while (( ${#pids} >= WORKERS )); do
      sleep 0.2; alive=(); for p in $pids; do kill -0 $p 2>/dev/null && alive+=($p); done; pids=($alive)
    done
  done
  wait
  echo "=== scoreboard (PASS first, then by diff asc) ==="
  grep '^PASS' /tmp/rt_board.res | sort
  grep -vE '^PASS|^      ' /tmp/rt_board.res \
    | awk '{ n=999999; if (match($0,/diff=[0-9]+/)) n=substr($0,RSTART+5,RLENGTH-5); print n"\t"$0 }' \
    | sort -n | cut -f2-
}

# Run one parallel_schedule group (1-indexed among `test:` lines). Working through
# groups in schedule order focuses on CORE tests first (types before everything,
# and the schedule respects inter-test dependencies) instead of cherry-picking the
# lowest-diff leaf tests.
cmd_group() {
  local n="$1"
  if [[ "$n" == "list" ]]; then
    grep -nE '^test:' "$REG/parallel_schedule" | awk '{c++; print c": "$0}' \
      | sed -E 's/[0-9]+:test: ?//2'; return
  fi
  local line; line=$(grep -E '^test:' "$REG/parallel_schedule" | sed -n "${n}p")
  [[ -z "$line" ]] && { echo "no group $n" >&2; return 1; }
  local rest="${line#test:}"; local tests=(${=rest})
  echo ">> group $n: ${tests[*]}" >&2
  cmd_board "${tests[@]}"
}

NOBUILD=0
[[ "${1:-}" == "nobuild" ]] && { NOBUILD=1; shift; }
SUB="${1:-}"; shift 2>/dev/null
case "$SUB" in
  build) build ;;
  test)  { (( NOBUILD )) || build; } && cmd_test "$@" ;;
  sql)   { (( NOBUILD )) || build; } && cmd_sql "$1" ;;
  board) { (( NOBUILD )) || build; } && cmd_board "$@" ;;
  group) { (( NOBUILD )) || build; } && cmd_group "$1" ;;
  *) echo "usage: rt.sh [nobuild] build|test <t..>|sql <q>|board <t..|ALL>|group <N|list>" >&2; exit 2 ;;
esac

# Plan 004 - pg_regress conformance

Implementation spec for a fresh code agent. Read this, then execute the steps in
`file-list/01-30.txt` in order using the workflow in "Execution" below. Coding
conventions, the error model, and clippy/style are in
`../003-total-translation/rules.md` and `../003-total-translation/error.md`.

## Objective

Make PepperDB pass PostgreSQL 18.4's core regression suite
(`ref/postgres/src/test/regress`) run by a real `psql` client, diffing output
against PostgreSQL's own `expected/*.out`. Translate the modules those tests need,
in leverage order: the in-scope front (steps 07-22) reaches ~71 of ~222 tests, and
the extended engine subsystems (steps 23-29) take it to ~100.

## Ground rules

1. Suite: the core `pg_regress` suite only. Isolation and TAP suites are out of
   scope.
2. Oracle: diff against upstream `expected/NAME.out` (true conformance). A
   snapshot layer (see Harness) absorbs known gaps without hiding regressions.
3. Client: real `psql`/libpq. No in-process client.
4. Driver: PostgreSQL's own `pg_regress` binary in `installcheck` mode. Do not
   reimplement it.
5. Order: dependency-driven. Each step translates one blocking-module cluster,
   then makes its unblocked tests PASS. A test with several blockers is made to
   pass only in the step where its last blocker lands.
6. In scope: the in-scope front (07-22) plus the extended engine subsystems
   (23-29: PL/pgSQL, generated/identity, two-phase commit, RLS, event triggers,
   partitioning, TOAST/compression). Out of scope: the deferred register in
   `file-list/30.txt` -- which now includes parallel query and predicate/SSI,
   deferred because they add ~0 pg_regress conformance (parallel-query tests are
   co-blocked by inheritance/PL; SSI is exercised only by the isolation suite).

## Prerequisites to fix first (Phase 0, steps 01-06)

The server cannot serve a real client today; steps 01-06 fix exactly this:

- No usable server entry: `src/main.rs` hardcodes port 5432, sets no data
  directory, never bootstraps catalogs.
- Incomplete startup handshake: the server sends only an empty `BackendKeyData`
  and `ReadyForQuery` -- no `AuthenticationOk`, no `ParameterStatus` -- so libpq
  aborts before sending a query.
- `ErrorResponse`/`NoticeResponse` are never written to the wire (the message
  text is already correct internally).
- Query-path stubs panic and drop the connection (multi-row `VALUES`,
  multi-statement input); `SHOW` returns nothing over the wire.

Phase 0 exit test: real `psql -c "SELECT 1"` returns the row; a failing query
returns an `ErrorResponse` and the session stays usable; multi-row `VALUES` and
multi-statement input do not drop the connection; the harness runs the initial
READY tests (`bitmapops`, `combocid`, `comments`, `portals_p2`, `sanity_check`,
`select_having`, `misc_sanity`).

## Harness (built in steps 05-06, used by every step after)

`ai/harness/regress/run.sh`:
- initializes a fresh data directory, starts a PepperDB server on a chosen port,
  tears it down at exit;
- pins deterministic-output env (`PGTZ`, `PGDATESTYLE="Postgres, MDY"`,
  `LC_MESSAGES=C`, ...), creates the `regression` database;
- runs PostgreSQL's `pg_regress` in `installcheck` mode against that server,
  reusing upstream `sql/` and `expected/` directly from the submodule (no copies),
  over `ai/harness/regress/pepper_schedule` (grows as steps land);
- classifies each test: **PASS** (matches upstream `expected/NAME.out`),
  **KNOWN-DIFF** (matches a checked-in `ai/harness/regress/known_diffs/NAME.out`
  snapshot -- a documented, accepted gap), **NEW-DIFF** (matches neither -- a
  regression; fails the run);
- `--update-known` regenerates snapshots; sub-commands run one test, a subset, or
  the whole schedule, and print the conformance count (PASS / total).

`test_setup.sql` cannot run upstream as-is (needs geometry, inheritance, range
types, executable SQL functions, a C extension); step 06 ships
`ai/harness/regress/test_setup_pepper.sql` seeding only supported-type fixtures.

## Steps

Definitions are in `file-list/NN.txt` (per step: phase, effort, dependencies,
deliverable, the target files, and the tests it unblocks).

- **01-06** Phase 0: server entry, startup handshake, `ErrorResponse`/
  `NoticeResponse`, kill query-path panics + `SHOW`, the harness, the trimmed
  `test_setup` + starting schedule.
- **07-22** In-scope front: translate one module cluster per step, in leverage
  order, and flip its tests to PASS. Add each newly-passing test to
  `pepper_schedule`. Reaches ~71.
- **23-29** Extended engine subsystems, each a multi-step campaign (internal steps
  and the detailed source/target mapping are in the `file-list` entry and in
  `analysis/<name>.md`): PL/pgSQL (23), generated/identity (24),
  two-phase commit (25), RLS (26), event triggers (27), partitioning (28),
  TOAST/compression (29). Ordered by leverage.
- **30** Deferred register: not implemented in this plan (includes parallel query
  and predicate/SSI, deferred for ~0 pg_regress yield).

Per-step target conformance (the harness reports the actual numbers; treat these
as goals):

| After step | Module cluster | Cumulative PASS / ~222 |
|---|---|---|
| 06 | server gate + harness | ~9 |
| 07 | soft-error input API (`fmgr` `InputFunctionCallSafe`) | ~19 |
| 08 | SRF/funcapi + `generate_series` | ~23 |
| 09 | `CREATE TABLE AS` / `SELECT INTO` | ~26 |
| 10 | string/varlena + regexp + like + crypto | ~32 |
| 11 | datetime (date/time/timetz/timestamp) | ~37 |
| 12 | numeric transcendental + var/stddev aggs | ~40 |
| 13 | reloptions | ~42 |
| 14 | EXPLAIN (text, COSTS OFF) | ~46 |
| 15 | scalar types (pg_lsn/oid/tid/macaddr/dbsize) | ~52 |
| 16 | aggregates + CREATE AGGREGATE | ~54 |
| 17 | arrays | ~56 |
| 18 | roles (pg_authid/pg_auth_members) | ~60 |
| 19 | operator/cast DDL | ~64 |
| 20 | CTE SEARCH/CYCLE + recursive views | ~65 |
| 21 | scrollable cursors / portals | ~67 |
| 22 | misc (money, enum, FK MATCH FULL, functional_deps) | ~71 |
| 23 | PL/pgSQL + SQL-function execution | ~82 |
| 24 | generated / identity columns | ~86 |
| 25 | two-phase commit | ~88 |
| 26 | row-level security | ~89 |
| 27 | event triggers (green after 23) | ~91 |
| 28 | declarative partitioning (+ inheritance prereq) | ~100 |
| 29 | TOAST + compression (compression tests stay KNOWN-DIFF) | ~101 |

Steps 23-28 are the extended engine gains (~71 -> ~100), dominated by PL/pgSQL and
partitioning; step 29 adds TOAST infrastructure and one test (`delete`). The
remaining ~120 tests need the `file-list/30.txt` register (parallel query,
predicate/SSI, geometry, JSON, ranges, inheritance, non-btree AMs, XML, full-text,
per-code SQLSTATE, the type long tail).

## Execution

Run this stage agent-orchestrated. One orchestrator drives; it never edits code
or runs cargo itself. For each step in order:

1. **Implement.** Spawn one implementation agent with the step's `file-list`
   entry. It translates the target files 1:1 from the corresponding
   `ref/postgres` C sources (re-verify exact file paths before editing -- the
   mappings are from static analysis), adds the newly-passing tests to
   `pepper_schedule`, and runs `ai/harness/regress/run.sh` to confirm they turn
   PASS. It must keep `cargo check`/`cargo clippy --all-targets` at 0/0 and
   `cargo test --lib` green. It does not commit.
2. **Commit.** The orchestrator commits the step (one `feat` commit; message
   states the module + the tests it turned green).
3. **Review.** Spawn one independent READ-ONLY agent: confirm cargo + clippy are
   green (authoritatively -- `rm -rf target/*/debug/incremental &&
   CARGO_INCREMENTAL=0 cargo check --all-targets`, because the harness LSP emits
   phantom diagnostics from a stale incremental cache), confirm the step's target
   tests are PASS with zero NEW-DIFF and no earlier test regressed, and audit the
   translated module against its PG source for fidelity.
4. **Fix loop.** Apply review findings; re-verify; then advance.

**Model choice.** Decide per agent at runtime, not in this plan -- match the model
to the step's difficulty: a cheaper model for mechanical 1:1 leaf ports, a
stronger model where correctness is subtle (wire protocol, grammar/planner/
executor spine) and for every review agent.

**Known-diff discipline.** When a target test cannot fully pass yet because it
also depends on a later step's module, snapshot it as KNOWN-DIFF with a one-line
reason rather than forcing it green. NEW-DIFF always fails the run; the KNOWN-DIFF
set only shrinks.

## Standing notes

- The convergence numbers above are targets from static analysis; the harness
  reports the real numbers -- trust the harness.
- Per-test blocking-module analysis is in `analysis/per-test/`; the
  synthesis is in `analysis/regress-phase-plan.md`.
- This plan translates a test-leverage-ordered slice of the breadth backlog plan
  003 left as stubs; it does not start the deferred XL subsystems.

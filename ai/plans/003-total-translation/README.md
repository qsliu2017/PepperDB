# Plan 003 - Total translation

Translate the rest of PostgreSQL 18.4 (`ref/postgres`, tag `REL_18_4`) into the
PepperDB port, on top of the completed foundation (plan 002). The reference for HOW
to translate a file is `rules.md` (primitives, design patterns, async coloring,
lock-precondition-as-types, idiomatic style, clippy); the error model is `error.md`.
This README is the WHAT and the ORDER.

## Precondition: the foundation is done

The single-process async tokio spine is translated end-to-end (~86 backend body
files): storage, buffer manager, smgr/md, WAL, locks, xact/MVCC, procarray/snapshot,
clog, sinval, the aux tasks + supervisor, the libpq startup/auth handshake, the
`PostgresMain` command loop, and `ProcessInterrupts`. Everything above it
(parser, analyzer, rewriter, optimizer, executor, commands, catalog bodies, access
methods, the type/adt library, the libpq wire send/recv) is header stubs ending in
`unimplemented!()` - this plan fills them.

Codegen already in place (`build.rs`): catalog OID symbol constants and the fmgr
builtin table are generated from the upstream `.dat` files. The fmgr table entries
are `func: None` - binding the real Rust functions is part of M1.

## No partial files: full-once leaves, grow-by-case dispatchers

We do NOT translate a subsystem all at once, but we also never leave a file
half-translated and abandoned. Two file kinds, two rules:

- **Leaf file** (self-contained per capability: a `utils/adt/*` type, an executor
  leaf node, most `catalog/*` and `commands/*`, the `rmgrdesc/*`): translated
  **completely on first touch** (`full`) and never revisited. Touch `int.c` -> do all
  of `int.c` (in/out, arithmetic, comparison, aggregate transitions) in that one step.
  Each leaf file is assigned to the earliest milestone whose dependencies make a full
  translation sensible.
- **Dispatcher / spine file** (a `switch (nodeTag)` over statement/plan/node/opcode
  types, or the grammar: `gram.y`, `analyze.c`, `planner.c`, `createplan.c`,
  `setrefs.c`, `execProcnode.c`, `execExpr*.c`, `utility.c`, `heapam.c`, `tableam.c`,
  `nodeModifyTable.c`, `relcache.c`, `lsyscache.c`, `ruleutils.c`, ...): **grows one
  COMPLETE case at a time** (`grow`) as each node/statement type's milestone arrives.
  A case is never left mid-written; the file is correct-for-all-currently-reachable
  tags at every step. These files legitimately appear under several milestones.

Per rules.md s4, a translated function that calls a not-yet-translated subsystem calls
its existing `unimplemented!()` stub - that compiles and is correct staging. Where a
genuine bootstrap cycle would otherwise force a partial translation (e.g. type-output
lookup needs syscache, which needs relcache, which needs bootstrap), a milestone uses
a clearly-marked **`shim`** - a small NON-translation stand-in (like PG's `formrdesc`)
that is DELETED when the real file is translated in full at its milestone. A shim is
never a slice of the real `.c`. This keeps an end-to-end runnable server at every step
without any real source file sitting partially translated.

**Disposition vocabulary** (used in `file-list/NN.txt`): `full` = leaf, entire file
here, never revisited; `grow` = dispatcher/grammar, adds complete case(s) this
milestone; `new` = no `.c` counterpart (codegen / Rust initdb); `shim` = temporary
non-translation stand-in, deleted when the real file lands; `tombstone` =
deleted/replaced by tokio/std/Arc.

## Gating decisions (settled)

1. **Grammar.** Translate `parser/gram.y` rule-for-rule into a Rust parser-generator
   crate's grammar DSL, semantic actions building PG's `RawStmt`/`SelectStmt`/... node
   trees (1:1 node-output contract preserved). Crate: **`lalrpop`** (LR(1)/LALR, the
   structural analog of bison, so productions map across nearly one-to-one). The
   lexer (`parser/scan.l`) becomes a custom token source (hand-written or `logos`)
   reusing `kwlist.rs`. The `.lalrpop` grammar starts at `SELECT <const>` (M1) and is
   extended each milestone. NOT hand-rolled recursive descent; NOT an external SQL
   parser.
2. **Catalog bootstrap.** Extend `build.rs` to emit `FormData_pg_*` seed rows from the
   `.dat` files; a Rust initdb-equivalent writes them into the catalog heaps at first
   boot. No BKI text, no bison bootstrap parser. `formrdesc`/relcache-phase startup
   lets the bootstrap catalogs be read before they exist on disk.
3. **Catalog access.** Build REAL btree catalog indexes in M2 (not seqscan). The
   btree AM, `access/index/*`, `catalog/index.c`, `indextuple.c` land in M2 so
   `SearchSysCache`-via-index is faithful. M6 then narrows to the user-facing index
   surface.

## Cross-cutting substrates - stand up skeletons early

- **fmgr binding** (M1): bind real Rust fns into the generated `fmgr_builtins` table.
- **GUC skeleton** (M1): a defaults-returning settings store. `guc.c`+`guc_tables.c`
  (~340 KB) is a full port deferred to M9; until then callers read defaults.
- **ruleutils.c deparse skeleton** (before M10): error messages / EXPLAIN / views.
- **nodes support** (M1): `makefuncs`, `nodeFuncs`, `list` - pervasive.

## Milestone ladder -> execution steps

The work is broken into dependency-ordered **steps** in `file-list/NN.txt` (paths
relative to `ref/postgres/src/backend/`). Each step is one reviewable unit (translate
-> check/clippy -> review -> gate -> squashed commit; per `rules.md` s12). A step's
header names its milestone, its prerequisite step(s), and whether its files can be
fanned out in parallel. Only the steps that COMPLETE a user-visible capability are
tagged `[MILESTONE]`; the rest are plumbing toward the next milestone.

| Milestone | Goal | steps | milestone step |
|-----------|------|-------|----------------|
| M1 | `SELECT 1;` over the wire (no table) | 01-09 | **09** |
| M2 | `CREATE TABLE t(a int); INSERT; SELECT * FROM t;` | 10-18 | **18** |
| M3 | WHERE + scalar arithmetic & comparison | 19-21 | **21** |
| M4 | date/time, casts, CASE/COALESCE | 22-23 | **23** |
| M5 | ORDER BY / GROUP BY / aggregates / DISTINCT / LIMIT | 24-26 | **26** |
| M6 | user index scans + CREATE INDEX | 27-28 | **28** |
| M7 | multi-table joins + the optimizer proper | 29-32 | **32** |
| M8 | UPDATE / DELETE / MERGE + EvalPlanQual | 33-34 | **34** |
| M9 | SQL txns, cursors, prepared stmts, SPI + full GUC | 35-37 | **37** |
| M10 | DDL breadth (ALTER/DROP/CREATE *) | 38-39 | **39** |
| M11 | views / rules / RI / triggers | 40-41 | **41** |
| M12 | window functions, CTEs, set ops, recursive | 42-44 | **44** |
| M13 | COPY, VACUUM/ANALYZE, CLUSTER | 45-47 | **47** |
| M14 | WAL replay (rmgr redo) for all AMs | 48-49 | **49** |

Within a step, files marked `full` (leaf) with no ordering note can be translated in
parallel (one agent per file); `grow` files and a step's keystone file are done first
or single-threaded as the header notes.

## Deep-defer (off the critical path, not numbered here)

geqo; non-btree AMs (gist/gin/brin/spgist/hash, ~57 files); logical + physical
replication (~23); JIT; FDW; partitioning; tsearch (~15); xml/json/jsonpath;
range/geo types; parallel-query execution nodes (nodeGather); auth methods; encoding
conversions (`utils/mb`, ~30); PL languages (plpgsql).

Of PG 18.4's 876 backend `.c` files, M1+M2 cover ~80; M3-M14 cover the core path;
deep-defer holds the long tail.

## Parallelization hot spots (hard to fan out)

- optimizer path/plan/util triangle (M7) - mutually recursive over the planner graph.
- `execExpr` + `execExprInterp` (M3) - shared opcode enum.
- `tablecmds.c` (M2/M10) - 22k-line maze.
- EvalPlanQual (M8) - executor + MVCC coupling.
- GUC (M9) - huge but isolatable.
- `ruleutils.c` + catalog dependency machinery - pervasive substrates (skeleton early).

## Execution workflow

Steps run STRICTLY IN SEQUENCE (01 -> 49); a step starts only after the previous one is
committed green. The **main agent is a pure orchestrator**: it spawns agents, reads
their summaries, autocommits as it goes, and tracks overall progress on the ladder. It
NEVER reads or edits code, and NEVER runs build / run / test / lint itself - all of
that lives inside spawned agents, so a long run does not bloat the orchestrator's
context.

For each step:

1. **Translate.** Spawn one or more agents to translate THIS step's file list only -
   nothing outside it. Files the step header marks parallel (leaf `full` files) get one
   agent each; the keystone, `grow`, and order-dependent files go to a single agent (or
   in the noted order). Agents follow `rules.md` (dispositions, async coloring, lock
   invariants, idiomatic style) and `error.md`. Translation agents translate; they do
   not gate. The main agent autocommits the result.

2. **Review + verify.** Spawn review agent(s) that run `cargo check` + `cargo clippy
   --all-targets` (must be 0/0) + `cargo test --lib`, and read the diff for correctness
   against the PG source and the rules. Review agents are READ-ONLY: never `git
   checkout / reset / stash / commit`, never edit code, never touch the task list or
   cron jobs. They report findings only.

3. **Fix loop.** If review reports any issue (build/clippy/test failure or correctness
   defect), spawn fix agent(s) to address them, then re-review. Repeat
   review -> fix -> autocommit until a review pass is fully clean (green
   check/clippy/tests, no open findings).

4. **Commit + advance.** A small step squashes to one commit; a large step keeps its
   dependency-ordered sub-commits (each reviewed) and squashes to `feat` + `refactor`
   after the final clean review. Then move to the next step.

The main agent issues only `git` (autocommit) and agent-spawn calls. Tests are inline
`#[cfg(test)]`, use a tempdir (not the repo root), and cover cancellation/race/teardown
paths, not just the happy path. `cargo test --lib` stays green and growing.

# SSI / predicate locking -- step breakdown for plan 004 (candidate addition)

Status: read-only research. No code changed. This is a proposal to fold into
plan 004 (or a plan-004-adjacent register), not an executed step.

## TL;DR

Implementing full SSI (SERIALIZABLE snapshot isolation via predicate locks) is
a large (XL), architecturally-legitimate gap-closer with **near-zero core
pg_regress leverage**. The one test whose name suggests it (`predicate`) tests
something unrelated (qual-reduction / `predtest.c` + inheritance, already
tracked under plan 004 steps 14 and the inheritance entry in step 23's
register). SSI's actual regression coverage lives almost entirely in
`src/test/isolation/specs/*.spec`, which is explicitly out of scope for plan
004. Recommendation: track SSI as a separate, later, architectural-completeness
initiative (a "step 24" register entry, analogous to two-phase commit or
declarative partitioning), not as an in-scope plan 004 step justified by
regress-test count.

## 1. PG source: what SSI actually is

- `ref/postgres/src/backend/storage/lmgr/predicate.c` -- 5063 lines. The SSI
  engine: SIREAD lock acquisition/promotion/cleanup, the `SERIALIZABLEXACT`
  dependency graph (rw-conflict in/out edges), dangerous-structure detection,
  `PreCommit_CheckForSerializationFailure`, safe/deferrable-snapshot logic,
  two-phase-commit predicate-lock carry-over, parallel-worker sharing.
  ~55 non-static entry points (`CreatePredXact`, `PredicateLockAcquire`,
  `CheckForSerializableConflictIn/Out`, `PreCommit_CheckForSerializationFailure`,
  `ReleasePredicateLocks`, `SerialAdd`/`SerialInit` (a dedicated SLRU,
  `pg_serial/`), etc).
- `ref/postgres/src/include/storage/predicate.h` -- 84 lines, the public API
  (already 1:1 mirrored as stub signatures in `src/storage/predicate.rs`).
- `ref/postgres/src/include/storage/predicate_internals.h` -- 478 lines, the
  shared-memory structures: `SERIALIZABLEXACT` (per-serializable-xact SSI
  state, intrusive dlist member), `PredXactListData` (global list + globals
  incl. `SxactGlobalXmin`), `RWConflictData`/`RWConflictPoolHeaderData` (rw
  dependency edges, pooled), `SERIALIZABLEXID`/`SERIALIZABLEXIDTAG` (xid ->
  sxact lookup), `PREDICATELOCKTARGETTAG`/`PREDICATELOCKTARGET` (the lock
  target hierarchy: tuple/page/relation, keyed by db+rel+block+offset),
  `PREDICATELOCK`/`PREDICATELOCKTAG` (sxact x target join), `LOCALPREDICATELOCK`
  (per-backend fast-path cache), two-phase predicate-lock WAL/state records.
- Integration points in the rest of PG (why this is not self-contained):
  - `access/heap/heapam.c`: `PredicateLockTID` on every tuple fetched under a
    SERIALIZABLE snapshot (index and seq scans), `CheckForSerializableConflictIn`
    on every insert/update/delete/lock (7 call sites).
  - `access/nbtree/{nbtinsert,nbtsearch,nbtpage}.c`: `PredicateLockPage`/
    `PredicateLockRelation` on scans, `PredicateLockPageSplit`/
    `PredicateLockPageCombine` on page split/merge (lock-target promotion must
    track physical page changes), `CheckForSerializableConflictIn` on insert.
  - `access/transam/xact.c`: `GetSerializableTransactionSnapshot` at
    `BEGIN ISOLATION LEVEL SERIALIZABLE`, `PreCommit_CheckForSerializationFailure`
    before commit, `ReleasePredicateLocks` at commit/abort,
    `RegisterPredicateLockingXid` lazily on first read.
  - `storage/ipc/procarray.c`: `SxactGlobalXmin` tracked alongside the normal
    xmin horizon (affects vacuum's cutoff -- SSI transactions must not have
    their conflict-detection data vacuumed away).
  - A dedicated SLRU (`pg_serial/`, via `SerialInit`/`SerialAdd`) records
    commit sequence numbers for recently-committed serializable transactions
    so a not-yet-summarized old xact can still be checked for conflicts.
  - `storage/lmgr/lwlock.c`: `SerializablePredicateLockListLock` and per-
    partition predicate lock manager LWLocks (`PredicateLockMgrLock`, an array
    like the regular lock manager's partition locks).
  - GUCs: `default_transaction_isolation`, `max_predicate_locks_per_xact`,
    `max_predicate_locks_per_relation`, `max_predicate_locks_per_page`.

This is a cross-cutting subsystem, not a leaf module: every heap and btree
read/write path needs 1-3 new call sites, plus xact begin/commit/abort hooks,
plus a new SLRU, plus new shared-memory init, plus a new LWLock class.

## 2. PepperDB status (grepped, not assumed)

- `src/storage/predicate.rs` (132 lines) and `src/storage/predicate_internals.rs`
  (282 lines) exist. They are **plan 001 header-translation stubs**: every
  function in `predicate.rs` is `unimplemented!() // TODO(lock-manager)`;
  `predicate_internals.rs` has the struct *shapes* translated (`SerCommitSeqNo`,
  `SERIALIZABLEXACT`, `PredXactListData`, `RWConflictData`, etc.) but as inert
  data definitions with `*mut ...Data` raw-pointer placeholders
  (`// TODO(ptr)`) -- no shared-memory allocation, no hash tables, no
  operations on them. Plan 003 (lock manager, xact/MVCC) never touched this
  file; it was explicitly deferred ("Backed by predicate_internals in Phase 2"
  -- that Phase 2 never happened).
- Zero call sites: `grep -rn "PredicateLock\|CheckForSerializableConflict"` over
  `src/backend/**/*.rs` (heap, nbtree, executor) returns nothing except one
  comment (`src/backend/access/transam/xact.rs:572`, "RegisterPredicateLockingXid
  (predicate.c) is a stub. TODO(predicate-lock)."). PepperDB's heap and nbtree
  code has never called into predicate locking at all -- contrast with PG C
  where `heapam.c` has 7 call sites and `nbtinsert.c`/`nbtsearch.c`/`nbtpage.c`
  have 6 more. This is greenfield integration work in the AM layer, not just
  filling in `predicate.rs` bodies.
- What DOES exist and is usable as a foundation:
  - `access::xact::IsolationIsSerializable()` (`src/access/xact.rs:46`) and
    `XACT_SERIALIZABLE` constant already exist; `BEGIN ISOLATION LEVEL
    SERIALIZABLE` presumably parses and sets the iso level, but today
    SERIALIZABLE behaves identically to REPEATABLE READ (snapshot isolation,
    no predicate locking, no write-skew detection) -- there is no
    serialization-failure path.
  - The lock manager (`src/backend/storage/lmgr/lock.rs`) is complete (plan
    003) and its "sharded hash table = `Vec<Mutex<Shard>>`" pattern
    (rules.md s8) is the direct template for the predicate lock manager's own
    hash table (`PREDICATELOCKTARGET`/`PREDICATELOCK` are separate shared hash
    tables from the regular lock manager in PG, same shape).
  - `src/backend/storage/ipc/procarray.rs` has `recent_xmin()`/
    `transaction_xmin()`/`get_snapshot_data()` -- the xmin-horizon machinery
    SSI's `SxactGlobalXmin` must plug into, but procarray.rs has no
    SSI-specific field or hook today.
  - `src/backend/utils/time/snapmgr.rs` exists (snapshot stack management) --
    the natural home for `GetSerializableTransactionSnapshot`'s override of
    the normal `GetTransactionSnapshot` path.
  - An SLRU framework exists (`transam::slru`, used by clog/commit_ts per
    rules.md s8 "Durability" pattern) -- reusable for the `pg_serial` SLRU
    `SerialAdd`/`SerialInit` needs.

Conclusion: no code today implements or partially implements SSI beyond
inert type/signature stubs. This is a from-scratch subsystem.

## 3. Tests unblocked -- honest count

Cross-referenced `ai/tmp/regress-analysis/*.md` (all 222 core regress test
verdicts) and the raw `ref/postgres/src/test/regress/sql/` tree.

- **`predicate.sql`** (the only test with "predicate" in its name): confirmed
  by reading the SQL file directly -- it is 100% `EXPLAIN (COSTS OFF)` qual-
  reduction tests (`IS NULL`/`IS NOT NULL` provable-true/false folding via
  `predtest.c`-equivalent logic, OR-clause reduction, outer-join nullability,
  table inheritance child-scan pruning) plus two plain `SELECT`s at the end.
  **Zero SSI content** -- no `BEGIN ISOLATION LEVEL SERIALIZABLE`, no
  concurrent sessions, no conflict scenario. Already tracked in
  `ai/tmp/regress-analysis/batch-predicate.md` as `NEEDS_MODULES` blocked on
  (a) `commands/explain.rs` (partially landing in plan 004 step 14), (b) a
  `predtest.c` port (NOT currently in any plan-004 step or step-23 register
  entry -- a gap, but unrelated to SSI), (c) table inheritance (registered in
  step 23's XL backlog). This test needs zero SSI work.
- **Serializability/write-skew/dependency-graph behavior**: grepped
  `ref/postgres/src/test/regress/sql/` for isolation-level tests -- there is
  no core-regress `.sql` file that opens two concurrent sessions and checks
  for a serialization failure (`ERROR: could not serialize access`). Core
  `pg_regress` is single-connection, sequential SQL scripts; SSI's entire
  reason to exist is detecting conflicts *between concurrent* transactions,
  which the core suite's execution model cannot express.
  - The real SSI test suite is `ref/postgres/src/test/isolation/specs/*.spec`
    (permutation-driven multi-session specs: `read-write-unique.spec`,
    `serializable-avoid-fcrc.spec`, `sync-*` and ~40 files with "serializable"
    or "ssi" in scope) -- run by `isolationtester`, a **different driver**
    from `pg_regress`. Plan 004's README section "Ground rules" item 1 states
    explicitly: "Isolation and TAP suites are out of scope."
  - A handful of core regress tests set `default_transaction_isolation` or use
    `BEGIN ISOLATION LEVEL SERIALIZABLE` incidentally (e.g. transaction-control
    tests), but grepping confirms none assert SSI-specific rollback behavior;
    they only need the isolation level to be *accepted and not crash*, which
    already works today (SERIALIZABLE silently behaves as snapshot isolation).
- **Count: 1 test file (`predicate`) superficially named after this feature,
  0 tests that actually require SSI to pass.** Implementing full SSI would
  turn 0 additional core `pg_regress` tests from FAIL to PASS.

This mirrors the task brief's own hint (the `predicate` test is EXPLAIN +
inheritance, not SSI) -- confirmed by direct inspection, not assumption.

## 4. Step breakdown (for a fresh code agent, if this work is greenlit)

Numbered as a self-contained sub-plan; if merged into plan 004 these would
follow step 22 (renumber to not collide with the step-23 register, or become
step-23's first fleshed-out entry). Each step's "target files (verify)" is a
best-effort match against current `src/` layout -- re-verify paths before
editing, per plan 004's own workflow note.

Dependencies common to all steps: plan 003 F2 (lock manager `storage/lmgr/lock.rs`,
`storage/lwlock.rs`), F2 txn/MVCC (`access/xact.rs`, `backend/access/transam/xact.rs`),
F2 snapshot (`backend/utils/time/snapmgr.rs`, `backend/storage/ipc/procarray.rs`),
and the SLRU framework (`backend/access/transam/slru` equivalent used by clog).

### Step A -- Predicate lock target hash table + shared-memory scaffolding
- PG files: `predicate_internals.h` (structs), `predicate.c` lines ~1-1420
  (`PredicateLockShmemInit`, `PredicateLockShmemSize`, `predicatelock_hash`,
  `SerialInit`).
- Target files: `src/storage/predicate_internals.rs` (replace `*mut ...Data`
  placeholders with real owned types per rules.md s8's sharded-hash-table
  pattern: `Vec<Mutex<Shard>>` for `PREDICATELOCKTARGET`/`PREDICATELOCK`
  tables, `Vec<UnsafeCell<SERIALIZABLEXACT>>` fixed arena + free-list for
  `PredXactListData` per rules.md s7 "fixed-size array -> index handle"),
  `src/storage/predicate.rs` (fill `PredicateLockShmemInit`/
  `PredicateLockShmemSize`), a new `backend/access/transam/serial.rs`-style
  SLRU wrapper for `pg_serial` (mirror `clog.rs`'s SLRU usage).
- Deliverable: shared-memory structures exist and initialize at startup; no
  behavioral hookup yet. Unit-testable in isolation (create/lookup/free a
  predicate lock target).
- Deps: plan 003 lock manager (shard pattern), SLRU framework (clog.rs as
  template).
- Tests unblocked: none directly (infrastructure only).
- Effort: L (new subsystem, but mechanical structure translation + one
  reusable pattern application).

### Step B -- SERIALIZABLEXACT lifecycle + SXACT flags + xmin tracking
- PG files: `predicate.c` `CreatePredXact`/`ReleasePredXact`/
  `SetNewSxactGlobalXmin`/`RWConflictExists`/`SetRWConflict`/
  `ReleaseRWConflict`/`FlagSxactUnsafe` (lines ~580-730, ~3251-3320).
- Target files: `src/storage/predicate_internals.rs` (SXACT_FLAG_* ops on the
  arena entries), new integration point in
  `src/backend/storage/ipc/procarray.rs` (a `SxactGlobalXmin`-equivalent field
  alongside the existing xmin horizon so vacuum's cutoff respects live SSI
  state -- this is a real behavioral change to procarray, not additive-only).
- Deliverable: a serializable transaction can be created/torn down and its
  global xmin correctly bounds vacuum; rw-conflict edges (reader/writer pairs)
  can be recorded and queried.
- Deps: Step A; plan 003 procarray xmin-horizon code.
- Tests unblocked: none directly.
- Effort: L.

### Step C -- BEGIN ISOLATION LEVEL SERIALIZABLE snapshot integration
- PG files: `predicate.c` `GetSerializableTransactionSnapshot` (~1682-1940),
  `GetSafeSnapshot`/deferrable-snapshot logic (~1558-1630),
  `RegisterPredicateLockingXid` (~1959).
- Target files: `src/backend/utils/time/snapmgr.rs` (branch
  `GetTransactionSnapshot` to the SSI path when `IsolationIsSerializable()`),
  `src/backend/access/transam/xact.rs` (remove the
  `RegisterPredicateLockingXid` stub comment at line 572, wire it at first
  read), `src/storage/predicate.rs` (fill
  `GetSerializableTransactionSnapshot`/`SetSerializableTransactionSnapshot`).
- Deliverable: `BEGIN ISOLATION LEVEL SERIALIZABLE` creates a tracked
  `SERIALIZABLEXACT`, still functionally equivalent to snapshot isolation
  (no conflict detection wired yet) but the bookkeeping exists.
- Deps: Step B; `IsolationIsSerializable()` (already exists, `src/access/xact.rs:46`).
- Tests unblocked: none (behavior invisible to SQL until Steps D-E land).
- Effort: M.

### Step D -- Predicate lock acquisition on read (heap + nbtree)
- PG files: `predicate.c` `PredicateLockAcquire`/`PredicateLockRelation`/
  `PredicateLockPage`/`PredicateLockTID`/`CoarserLockCovers`/
  `CheckAndPromotePredicateLockRequest` (~2045-2670); call sites in
  `heapam.c:1665,1812` and `nbtsearch.c:1540,1691,2720`.
- Target files: `src/storage/predicate.rs` (fill the four `PredicateLock*`
  functions), `src/backend/access/heap/heapam.rs` (2 new call sites on tuple
  fetch under a SERIALIZABLE snapshot), `src/backend/access/nbtree/nbtsearch.rs`
  (3 new call sites on page-level scan positioning).
- Deliverable: SERIALIZABLE reads acquire SIREAD locks at tuple/page/relation
  granularity with automatic coarsening (many tuple locks on one page ->
  promote to page lock) per PG's `max_predicate_locks_per_*` GUCs.
- Deps: Step C.
- Tests unblocked: none in core regress (see section 3) -- would light up a
  subset of `src/test/isolation/specs/*.spec` if that suite were ever run,
  but it isn't in scope.
- Effort: L (two AM call-site integrations + coarsening logic).

### Step E -- Conflict detection on write + dependency graph + rollback
- PG files: `predicate.c` `CheckForSerializableConflictOut`/
  `CheckTargetForConflictsIn`/`CheckForSerializableConflictIn`/
  `CheckTableForSerializableConflictIn`/`FlagRWConflict` (~3991-4700),
  `PreCommit_CheckForSerializationFailure` (~4703-4790, the dangerous-
  structure/pivot detection that decides who gets the `40001` abort).
- Target files: `src/storage/predicate.rs` (fill all four `CheckFor*`
  functions + `PreCommit_CheckForSerializationFailure`),
  `src/backend/access/heap/heapam.rs` (7 call sites: insert/update/delete/
  lock paths), `src/backend/access/nbtree/nbtinsert.rs` (2 call sites),
  `src/backend/access/transam/xact.rs` (call
  `PreCommit_CheckForSerializationFailure` in the commit path before WAL
  flush, map its failure to a `40001 serialization_failure` `ereport!`).
- Deliverable: concurrent SERIALIZABLE transactions that form a dangerous
  structure (rw-antidependency cycle) get one aborted with SQLSTATE `40001`;
  write skew is now actually prevented. This is the feature's real payoff.
- Deps: Step D; the error model (`ai/plans/003-total-translation/error.md`,
  `ereport!`) for the `40001` abort path.
- Tests unblocked: none in core regress (needs concurrent sessions -- see
  section 3); this is what the isolation-suite specs exercise.
- Effort: XL (this is the algorithmic heart of SSI -- the dependency-graph
  walk and pivot detection is the most subtle part of `predicate.c`).

### Step F -- Cleanup, page split/combine lock transfer, VACUUM integration
- PG files: `predicate.c` `PredicateLockPageSplit`/`PredicateLockPageCombine`
  (nbtree page split/merge must move predicate locks, not silently drop
  them), `DropAllPredicateLocksFromTable`/`TransferPredicateLocksToHeapRelation`
  (index drop / VACUUM FULL / CLUSTER rewrite must transfer locks to the new
  relfilenode), `ReleasePredicateLocks`/`ClearOldPredicateLocks`,
  `CheckPointPredicate` (checkpoint-time summarization).
- Target files: `src/backend/access/nbtree/nbtinsert.rs` (split, line ~1219
  equivalent), `src/backend/access/nbtree/nbtpage.rs` (combine),
  `src/backend/commands/vacuum.rs` / `cluster.rs` (already-implemented per
  plan 003 step 46/47 -- add the transfer call), `src/access/xact.rs`
  (`ReleasePredicateLocks` at commit/abort, filling
  `src/storage/predicate.rs`'s stub).
  Cross-ref: plan 003 step 47 (`VACUUM FULL` / `CLUSTER` table rewrite,
  already landed) needs a follow-up hook here.
  Cross-ref: plan 003 step 48/49 (WAL replay driver) -- if predicate locks
  need WAL records for two-phase commit at all (see Step G caveat below).
- Deliverable: predicate locks survive the structural operations PG allows
  underneath a live SIREAD lock without silently losing conflict-detection
  correctness.
- Deps: Step E; plan 003 steps 46/47 (VACUUM/CLUSTER, already complete).
- Tests unblocked: none in core regress.
- Effort: M.

### Step G -- Two-phase commit + parallel-query sharing (OPTIONAL / DEFER)
- PG files: `predicate.c` `AtPrepare_PredicateLocks`/
  `PostPrepare_PredicateLocks`/`PredicateLockTwoPhaseFinish`/
  `predicatelock_twophase_recover`, `ShareSerializableXact`/
  `AttachSerializableXact`.
- Target files: would touch `src/access/xact.rs`'s
  `prepare_transaction_stub()` (currently `unimplemented!("two-phase
  PrepareTransaction is deferred (twophase.c)")` per
  `ai/tmp/regress-analysis/batch-prepared_xacts.md`) and PepperDB's parallel
  worker task-spawn machinery (rules.md s8 "cross-process transport" pattern
  -- would become an `Arc`-shared handle passed into the spawned task rather
  than a real cross-process handle, since PepperDB's parallel workers are
  in-process tokio tasks, not forked backends).
- Deliverable: n/a -- recommend explicitly deferring. Two-phase commit itself
  is an unimplemented stub in PepperDB (`prepared_xacts` test is
  `OUT_OF_SCOPE` per plan 004's own analysis); building 2PC support for a
  feature (SSI) that has zero core-regress leverage, on top of a 2PC engine
  that doesn't exist, is not justified until/unless 2PC itself is undertaken
  for its own (also currently zero-leverage) reasons.
- Deps: 2PC (`PrepareTransactionBlock`, not started), Step E.
- Tests unblocked: none.
- Effort: XL, and should not be started -- listed only for completeness of
  the PG API surface.

**Total for Steps A-F (the buildable core): XL** (roughly 3-4x a typical
plan-004 "L" step; Step E alone is comparable in algorithmic complexity to
the entire lock manager from plan 003). Step G should be dropped from any
committed plan.

## 5. Architectural notes

- **Low pg_regress leverage -- do not justify this by test count.** Section 3
  establishes 0 core-regress tests need SSI. If this work is undertaken, the
  honest justification is "PostgreSQL correctness/completeness" (PepperDB
  claims SERIALIZABLE isolation but silently downgrades to snapshot
  isolation today -- a correctness gap for any client relying on the
  standard's serializability guarantee), not "unblocks N tests." Recommend:
  (a) do not insert this into plan 004's step 07-22 leverage-ordered
  sequence at all (it would sit at the bottom by leverage, tied with
  two-phase commit and declarative partitioning), (b) if tracked, add it to
  the step-23-style deferred-XL register
  (`ai/plans/004-pg-regress-conformance/file-list/23.txt`) as a new line
  item ("SSI / predicate locking (predicate.c ~5k) # 0 core-regress tests;
  correctness-completeness only, real coverage is src/test/isolation (out of
  scope)"), or (c) treat it as a plan-005-scale initiative if/when the
  project decides to also run the isolation-tester suite (which would be a
  bigger scope change: a new test driver, not just a new module).
- **If ever prioritized, prioritize honestly-later.** Every other plan-004
  step earns its place by moving the PASS count; this one cannot. Sequencing
  it before higher-leverage work (arrays, roles, operator/cast DDL, all still
  pending in steps 17-19) would be a regression in project priorities unless
  the goal changes to correctness-completeness.
- **Single-process implications for predicate-lock shared state.** PepperDB
  is single-process/async-tokio (no fork-per-backend), which actually
  *simplifies* several corners of `predicate.c` that exist purely to survive
  PG's multi-process model:
  - No true shared memory needed -- the predicate lock hash tables become
    ordinary process-local `Vec<Mutex<Shard>>` (rules.md s8 pattern), same
    simplification plan 003 already applied to the regular lock manager. No
    `ShmemInitStruct`/`ShmemAlloc` translation needed at all, just Rust
    allocation at process startup (a `OnceLock`/`process_global`-style init,
    matching the `#[process_global]` macro pattern already used for
    `LockManager`).
  - `ShareSerializableXact`/`AttachSerializableXact` (parallel-query handle
    sharing across forked workers) degenerates to nothing: a parallel worker
    is a tokio task spawned inside the leader's scope (rules.md s8), so it
    can simply hold the same `Arc<SerializableXactState>` the leader created
    -- no serialize/attach protocol needed. This makes Step G's parallel-
    query half nearly free IF 2PC weren't the bigger blocker.
  - Two-phase commit's predicate-lock carryover (`AtPrepare_PredicateLocks`
    et al.) is the one piece that gets *harder*, not easier, in a
    single-process model only insofar as PepperDB has no 2PC at all yet
    (Step G's real dependency, not the process model itself).
  - Caution: the SLRU (`pg_serial`) and the predicate lock hash tables are
    still genuinely concurrent structures (many tokio tasks = many backends
    conceptually), so they still need the sharded-Mutex / atomic-mirror
    discipline from rules.md s7-s9 -- "single process" does not mean "no
    locking," only "no shared-memory/IPC translation."
  - The dependency-graph walk in `PreCommit_CheckForSerializationFailure`
    (Step E) needs to lock across potentially many `SERIALIZABLEXACT`
    entries at once (graph traversal) -- this is the one place where the
    "take all shards in fixed index order" whole-subsystem critical section
    pattern (rules.md s8, used today for deadlock detection in
    `storage/lmgr/lock.rs`) is directly reusable; `deadlock.rs` is a good
    structural template for this step specifically, since both are cycle-
    detection over a live transaction graph.

## 6. Bottom line for the plan-004 owner

Recommend NOT adding this to plan 004's in-scope leverage-ordered sequence.
If the project wants SSI for correctness-completeness independent of
pg_regress, track Steps A-F above as their own XL initiative (plan 005 or a
step-23-register line item), explicitly labeled as test-count-zero and
justified on correctness grounds alone. Step G (two-phase commit + parallel
sharing) should not be scheduled at all until/unless 2PC is separately
undertaken.

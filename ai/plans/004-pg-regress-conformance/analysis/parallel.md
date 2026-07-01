# Parallel query for plan 004 (pg_regress conformance)

Read-only research. No code changed.

## 1. PG source surveyed

| File | Lines | Role |
|---|---|---|
| `ref/postgres/src/backend/access/transam/parallel.c` | 1672 | `ParallelContext`/DSM segment setup, worker launch via bgworker, `shm_mq` tuple queues, error propagation from workers to leader |
| `ref/postgres/src/backend/executor/execParallel.c` | 1531 | Builds the serialized "parallel plan" blob in DSM, `ExecInitParallelPlan`, `ParallelQueryMain` (worker entry point), tuple-queue funnel setup |
| `ref/postgres/src/backend/executor/nodeGather.c` | 476 | Executor node: launches workers, pulls tuples from worker queues + runs a copy of the subplan itself (leader participation) |
| `ref/postgres/src/backend/executor/nodeGatherMerge.c` | 788 | Like Gather but preserves sort order via a merge over per-worker queues |
| `ref/postgres/src/backend/storage/ipc/dsm.c` | 1303 | Dynamic shared memory segment allocation (POSIX/SysV/mmap backends) |
| `ref/postgres/src/backend/postmaster/bgworker.c` | 1398 | Background worker process registration/start, used to launch parallel workers as OS processes |
| `ref/postgres/src/backend/optimizer/path/allpaths.c` | -- | `set_rel_consider_parallel`, `create_plain_partial_paths`, `generate_useful_gather_paths` -- planner-side decision of *whether* a rel/join can be scanned in parallel and adds `GatherPath`/`GatherMergePath` on top |
| `ref/postgres/src/backend/optimizer/path/costsize.c` | -- | `cost_gather`, `cost_gather_merge`, `get_parallel_divisor` -- cost model that makes parallel paths win or lose against sequential ones (driven by `parallel_setup_cost`, `parallel_tuple_cost`, `min_parallel_table_scan_size`, `min_parallel_index_scan_size`) |

All of this is built on PG's **multi-process + shared-memory** architecture: a worker is a full OS process (`bgworker`) attached to a DSM segment (`dsm.c`), communicating with the leader via `shm_mq` (shared-memory message queues). None of that substrate exists in PepperDB, nor should it -- see below.

## 2. PepperDB status: grep results

- **Data-only stubs exist, no logic.** `Gather`/`GatherMerge` plan node structs are fully translated (`src/nodes/plannodes.rs:503-524`) with all PG fields (`num_workers`, `single_copy`, `sort_col_idx`, etc). `GatherPath`/`GatherMergePath` exist in `src/nodes/pathnodes.rs:1031-1048`. `RelOptInfo.rel_parallel_workers`, `Path.parallel_aware/parallel_safe/parallel_workers` all exist as struct fields (`src/nodes/pathnodes.rs:339,392-393,732-735`).
- **Every function body that would populate/consume them is `unimplemented!()`:**
  - `create_gather_path` / `create_gather_merge_path` in `src/backend/optimizer/util/pathnode.rs:190-219` -- both bare `unimplemented!()`.
  - `ExecInitGather` / `ExecEndGather` / `ExecShutdownGather` / `ExecReScanGather` in `src/executor/nodeGather.rs` -- all `unimplemented!()`. Same shape for `nodeGatherMerge.rs`.
  - No `src/backend/executor/nodeGather.rs` or `nodeGatherMerge.rs` **backend body file exists at all** (only the header-stub re-export files under `src/executor/`); there is nothing to dispatch to.
  - `execProcnode.rs`'s node dispatch `match` (in `exec_init_node`/`exec_end_node`/`exec_rescan_node`, `src/backend/executor/execProcnode.rs:254,437,495,515,557`) has no `Gather`/`GatherMerge` arm.
  - Every rel/path site that sets `rel_parallel_workers` sets it to the "unknown/not computed" sentinel `-1` (`plancat.rs:76`, `relnode.rs:114,202`, `initsplan.rs:1244`, `joininfo.rs:250`, `planmain.rs:311`, `equivclass.rs:1466`) -- i.e. `set_rel_consider_parallel` was never ported.
  - `cost.rs` has one guard (`if path.parallel_workers > 0`, `costsize.rs:70`) but no `cost_gather`/`cost_gather_merge` equivalent, and every plan-creation site hardcodes `parallel_workers: 0` (`createplan.rs:1123,1227`, `costsize.rs:673`, `pathnode.rs:134,174,243,312`).
  - No parallel-related GUC is registered in `guc_tables.rs` (`grep -n "\"parallel"` returns nothing) -- `parallel_setup_cost`, `parallel_tuple_cost`, `min_parallel_table_scan_size`, `min_parallel_index_scan_size`, `max_parallel_workers_per_gather`, `force_parallel_mode`/`debug_parallel_query`, `parallel_leader_participation` are all unregistered, so a bare `SET parallel_setup_cost=0` fails with "unrecognized configuration parameter" before any plan-shape question is even reached.
  - `access/parallel.rs` (the `parallel.h` translation) explicitly tombstones `dsm_segment`/`shm_toc`/`shm_mq_handle` as opaque zero-field placeholders and documents the intended single-process mapping: "parallel workers become tokio tasks; the DSM segment + shm_toc keyed regions collapse into Arc-shared state + tokio mpsc channels" -- this note already anticipates the direction of this task but nothing beyond the header shape has been implemented. `src/backend/access/transam/parallel.rs` (the body) exists but was not audited line-by-line here; based on the header's `unimplemented!`-free re-exports it likely still needs the same treatment as the executor nodes -- confirm before relying on it.
  - `pg_stat_database`-style counters `parallel_workers_to_launch`/`parallel_workers_launched` exist as struct fields (`pgstat.rs:260-261`, `execnodes.rs:514-515`) but are dead data, never incremented.
  - Plan 003's own milestone ladder (`ai/harness/translation-milestones.md`) lists "parallel-query execution nodes (nodeGather)" under **Deep-defer (not on the critical path)** alongside PL/pgSQL, GiST/GIN/BRIN/hash AMs, and partitioning -- this was a deliberate, already-recorded deferral, not an oversight.

**Bottom line: there is no Gather node, no parallel path generation, and no worker abstraction of any kind in PepperDB today.** What exists is exactly the shape you'd expect from a mechanical struct/signature translation pass that stopped at the function-body boundary.

## 3. Tests unblocked -- cross-referenced against `ai/tmp/regress-analysis/`

| test | verdict (already recorded) | parallel-specific ask | other blockers noted in same file |
|---|---|---|---|
| `select_parallel` | OUT_OF_SCOPE | real Gather/Gather Merge, `debug_parallel_query`, `max_parallel_workers`, `parallel_leader_participation`, `pg_stat_database` parallel counters, Parallel Append over inheritance children (`a_star`..`f_star`) | table inheritance (STUB), partitioning (missing), custom operator classes (missing), PL/pgSQL function bodies (`sp_parallel_restricted` etc, not translated) |
| `write_parallel` | OUT_OF_SCOPE | parallel-plan GUCs (`parallel_setup_cost`, `parallel_tuple_cost`, `min_parallel_table_scan_size`, `max_parallel_workers_per_gather`) gating a CTAS/matview under a parallel plan | `CREATE MATERIALIZED VIEW`/`REFRESH MATERIALIZED VIEW` entirely missing (bigger blocker than parallel query itself) |
| `vacuum_parallel` | NEEDS_MODULES | `VACUUM (PARALLEL n, ...)`, `max_parallel_maintenance_workers`/`min_parallel_index_scan_size` GUCs | `pg_relation_size`/`pg_size_bytes` missing; `parse_vacuum_options` drops `parallel`/`index_cleanup` silently and hardcodes `nworkers=0` -- this is VACUUM's *own* parallel-worker path (parallel index vacuum), a separate mechanism from Gather/executor parallelism, out of this task's scope but worth flagging as a sibling gap |
| `incremental_sort` | OUT_OF_SCOPE | "parallel query (Gather/GatherMerge, parallel workers)" explicitly called out as a co-blocker | Incremental Sort executor/planner node itself missing entirely (separate from parallel query); GiST AM missing |
| `memoize` | OUT_OF_SCOPE | "parallel Gather plans" mentioned among the GUC/plan surface exercised | EXPLAIN command missing entirely (blocks every query in the file); Memoize executor node missing; PL/pgSQL (`explain_memoize` helper) missing; partitioning missing |
| `explain` | OUT_OF_SCOPE | not parallel-specific, but gates plan-shape assertions for every test above | `src/commands/explain.rs`/`explain_format.rs`/`explain_dr.rs`/`explain_state.rs` are header-stub-only; **no `src/backend/commands/explain*.rs` body file exists at all** -- `EXPLAIN` has zero backend implementation today |
| `expressions` | NEEDS_MODULES | uses `EXPLAIN (verbose, costs off)` for a few queries | same EXPLAIN blocker; unrelated to parallel query otherwise |
| `join_hash` | OUT_OF_SCOPE | parallel-query GUCs (`parallel_workers`, `max_parallel_workers_per_gather`, `min_parallel_table_scan_size`) referenced as load-bearing for hash-join batch/skew assertions | PL/pgSQL helper functions that parse `EXPLAIN (ANALYZE, FORMAT 'json')` output (bigger blocker); JSON type missing |
| `btree_index` | NEEDS_MODULES | "parallel index build with IMMUTABLE SQL function" mentioned in surface, but core btree comparisons are otherwise fine and this is a small fraction of the file | reloptions STUB, partitioning missing -- these dominate, not parallel query |
| `reindex_catalog` | NEEDS_MODULES | "parallel-scan GUC interaction inside a transaction" mentioned as surface | REINDEX command itself entirely unimplemented -- dominant blocker |

**Count: 9 test files reference parallel query or its GUCs** (`select_parallel`, `write_parallel`, `vacuum_parallel`, `incremental_sort`, `memoize`, `expressions`, `join_hash`, `btree_index`, `reindex_catalog`), out of which:
- 2 are pure/near-pure parallel-query tests (`select_parallel`, `write_parallel`).
- 1 is VACUUM's own separate parallel-worker mechanism (`vacuum_parallel`) -- not addressed by this task.
- 6 mention parallel GUCs/plan-shape only incidentally, gated by larger unrelated blockers (EXPLAIN, PL/pgSQL, partitioning, inheritance, JSON, reloptions, REINDEX) that would need to land first or in parallel regardless of parallel-query work.

**Realistic assessment: implementing parallel query, by itself, unblocks approximately 0 full test files to PASS status**, because every one of the 9 has at least one *other* OUT_OF_SCOPE or NEEDS_MODULES blocker recorded independently (EXPLAIN missing outright is the single biggest shared blocker: 3 of the 5 core files need it). The value of doing parallel-query work now is (a) removing it from the "OUT_OF_SCOPE" reason lists so the *other* blockers become the sole gate, and (b) unlocking partial credit -- pg_regress diffs the full output, so even one wrong plan line fails the file, but future work on EXPLAIN/inheritance/PL-pgSQL will need parallel-query support to exist before those files can go green.

## 4. THE ARCHITECTURAL DECISION

### Options

**(A) Real task-parallel Gather.** Spawn tokio tasks as "workers," each executing a clone of the subplan against a shared snapshot/heap state, funneling result tuples back to the leader task via `tokio::sync::mpsc` channels (the natural analogue of `shm_mq`). `ParallelContext` becomes an in-process struct holding `JoinHandle`s instead of PIDs; "DSM" becomes `Arc`-shared read-only plan state.
- Pro: faithful to PG's concurrency semantics; genuine speedup; the `access/parallel.rs` header's own doc comment already sketches exactly this mapping ("parallel workers become tokio tasks ... DSM ... collapse into Arc-shared state + tokio mpsc channels").
- Con: large surface. Needs a plan-tree "worker executor" that can run a subplan independently and safely on borrowed state (transaction snapshot, catalog cache, buffer access) — all of which are currently modeled as per-backend/per-task state (see `per-task-state-must-be-send` memory: no `Rc`/`Cell`, must be `Send`). Needs partial-scan work division (block-range parallel seqscan state machine, `ParallelBlockTableScanDesc` equivalent) rebuilt as an atomic cursor shared across tasks. Real concurrency introduces new classes of bugs (races over shared cursors, cross-task error propagation, cancellation/interrupt semantics) that don't exist in the current single-task-per-query model. Given plan 003 is "single-threaded-per-query" by design, this cuts against the current architecture's grain and is a multi-week effort for a payoff (raw throughput) that pg_regress does not measure at all -- pg_regress checks correctness and (for these tests) plan shape/EXPLAIN text, never wall-clock speedup.

**(B) Sequential-but-plan-faithful Gather.** The planner still produces `GatherPath`/`GatherMergePath` -> `Gather`/`GatherMerge` plan nodes under the same cost-model conditions PG uses (so `EXPLAIN` prints `Gather` / `Workers Planned: N` exactly where PG would), but the executor node runs the subplan on the calling task alone (no actual worker fan-out), simply forwarding tuples through unchanged (or applying the merge-sort pass-through for `GatherMerge`). `num_workers`/`Workers Launched` in `EXPLAIN ANALYZE` output would report the *planned* worker count per PG's costing (tests that only check `costs off` shape are unaffected; tests that check `EXPLAIN ANALYZE` worker-launched counts would need those counts synthesized to match, or such assertions would need row-level normalization the tests may already do via `\gset`/plpgsql helpers).
- Pro: matches PG's plan shape and query *results* (Gather's output must equal the union of worker outputs in the same order-preserving/order-agnostic sense PG guarantees, which is trivially true if there's only one "worker" that is really the leader running the whole subplan). Small, containable surface: mostly `create_gather_path`/`create_gather_merge_path`, `set_rel_consider_parallel`, `cost_gather`/`cost_gather_merge`, the Gather/GatherMerge executor node bodies (trivial pass-through), and the parallel GUCs. No DSM/mpsc/worker-task machinery needed at all in the first cut.
- Con: no real speedup (irrelevant to pg_regress, which is correctness-only and has no timing assertions in these files besides implicit "does it finish" -- confirmed no `\timing` usage found in the surveyed files). `EXPLAIN ANALYZE` worker-count/loop fields need care -- most regress usages here use `EXPLAIN (COSTS OFF)` specifically to dodge cost-number instability, so this is a minor risk, not a blocker.

**(C) Leave parallelism off; emulate only the planner's cost/path shape for EXPLAIN.** Add `consider_parallel`/`GatherPath` generation and costing so `EXPLAIN` shows the right plan, but never actually construct a working `Gather` executor node -- i.e., don't even run option (B)'s pass-through, just enough plan-tree shape to satisfy `EXPLAIN (COSTS OFF)` diffs, then error or fall back to non-parallel execution if the query actually runs.
- Pro: even smaller than (B).
- Con: breaks correctness the moment a test executes the query (not just `EXPLAIN`s it) with a parallel plan chosen -- every one of these test files *runs* the query after `EXPLAIN`ing it (see `select_parallel.sql` excerpt: `explain (costs off) select ...` immediately followed by `select ...` on the next line). (C) would need to also implement (B)'s executor to actually produce correct results, making it strictly a subset of (B) with an artificial extra failure mode. Not a real independent option once you look at how the tests are structured -- discarded.

### Recommendation: (B), sequential-but-plan-faithful Gather

Reasoning: pg_regress is a correctness/output-diffing harness, not a performance benchmark. Every parallel-query test surveyed both `EXPLAIN`s a query and then executes it and diffs the row output. (B) is the minimum-surface option that can make both halves pass: the plan shape comes from real cost-model-driven path generation (so `EXPLAIN` output matches byte-for-byte), and the row output comes from running the exact same subplan PG would run per-worker, just on one task instead of N. This also respects the project's current architecture (`plan 003` executor is single-threaded-per-query by design; `ai/harness/translation-milestones.md` already deep-deferred "parallel-query execution nodes" as a unit, and (B) is the smallest unit that discharges that deferral without violating the single-task model). (A) should be revisited later only if profiling ever calls for it -- it is a genuine architecture change (shared mutable cursor state across tasks, cross-task error/cancellation plumbing) that the current single-process-per-query executor was not built for, and pg_regress gives no signal that would justify taking on that risk now.

What each regress test actually checks, restated for the recommended path:
- `select_parallel`, `write_parallel`: plan shape (`Gather`/`Gather Merge` appears in `EXPLAIN (COSTS OFF)` output) + correct row output after actually running the query. (B) satisfies both; both still blocked on unrelated OUT_OF_SCOPE items (inheritance/partitioning/matviews/PL-pgSQL) regardless.
- `incremental_sort`, `memoize`: parallel query is a co-blocker gating a handful of cases inside otherwise-independent test files; (B) removes that specific gate but the primary node (`IncrementalSort`/`Memoize`) and, for `memoize`, `EXPLAIN` itself, must land too.
- `vacuum_parallel`: unaffected by (B) -- this is VACUUM's independent maintenance-worker parallelism, not Gather-based query parallelism.

## 5. Step breakdown under option (B)

Steps are ordered by dependency. "Our target files" gives the concrete Rust path to fill in (mostly de-stubbing existing header re-export sites); "PG files" is the C source to translate from.

### Step 1 -- Register parallel-query GUCs
- **PG files:** `backend/utils/misc/guc_tables.c` (search `parallel_setup_cost`, `parallel_tuple_cost`, `min_parallel_table_scan_size`, `min_parallel_index_scan_size`, `max_parallel_workers_per_gather`, `max_parallel_workers`, `parallel_leader_participation`, `force_parallel_mode`/`debug_parallel_query`, `enable_parallel_append`, `enable_parallel_hash`, `enable_gathermerge`)
- **Our target files:** `src/backend/utils/misc/guc_tables.rs`
- **Deliverable:** all ~11 GUCs registered with correct defaults/units so `SET`/`RESET`/`SHOW` work; no planner behavior wired yet.
- **Deps:** none.
- **Tests unblocked:** none alone, but removes the immediate "unrecognized configuration parameter" failure that currently aborts `select_parallel`, `write_parallel`, `incremental_sort`'s parallel section, and `join_hash` at the first `SET` line.
- **Effort:** S (small, mechanical; same pattern as any other GUC batch already done in plan 002/003).

### Step 2 -- `consider_parallel` rel-eligibility pass
- **PG files:** `backend/optimizer/path/allpaths.c` (`set_rel_consider_parallel`, `set_rel_size` call site, `create_plain_partial_paths`)
- **Our target files:** `src/backend/optimizer/util/relnode.rs`, `src/backend/optimizer/util/plancat.rs`, `src/backend/optimizer/plan/initsplan.rs`, `src/backend/optimizer/plan/planmain.rs` (replace the `rel_parallel_workers: -1` sentinel sites with a real computed value), plus a new `set_rel_consider_parallel` in `src/backend/optimizer/path/allpaths.rs`
- **Deliverable:** `RelOptInfo.consider_parallel`/`rel_parallel_workers` correctly computed from table size vs `min_parallel_table_scan_size`, parallel-unsafe quals/targetlist detection (reuse the existing `max_parallel_hazard` machinery already stubbed in `pathnodes.rs:127` -- check whether `max_parallel_hazard_walker` exists or needs porting alongside this step).
- **Deps:** Step 1 (needs the GUCs to compare against).
- **Tests unblocked:** none alone (still no Gather path emitted), but this is the first real planner-behavior step.
- **Effort:** M (touches several call sites but each is a small, well-understood check; the tricky part is verifying `max_parallel_hazard` walker parity, worth a sub-check before starting).

### Step 3 -- Partial seq-scan path + `cost_seqscan` parallel divisor
- **PG files:** `backend/optimizer/path/allpaths.c` (`create_plain_partial_paths`), `backend/optimizer/path/costsize.c` (`get_parallel_divisor`, parallel-aware branch of `cost_seqscan`)
- **Our target files:** `src/backend/optimizer/path/allpaths.rs` (new `create_plain_partial_paths`), `src/backend/optimizer/path/costsize.rs` (extend the existing `if path.parallel_workers > 0` guard at line 70 into a real divisor-based cost adjustment)
- **Deliverable:** a `SeqScan` path marked `parallel_aware=true, parallel_workers=N` gets proportionally reduced cost, so the planner can prefer it once Gather's cost is added (Step 4).
- **Deps:** Step 2.
- **Tests unblocked:** none alone.
- **Effort:** M.

### Step 4 -- `create_gather_path`/`create_gather_merge_path` + `cost_gather`/`cost_gather_merge`
- **PG files:** `backend/optimizer/util/pathnode.c` (`create_gather_path`, `create_gather_merge_path`), `backend/optimizer/path/costsize.c` (`cost_gather`, `cost_gather_merge`), `backend/optimizer/path/allpaths.c` (`generate_useful_gather_paths`, call site in `set_plain_rel_pathlist`)
- **Our target files:** `src/backend/optimizer/util/pathnode.rs:190-219` (de-stub both functions), `src/backend/optimizer/path/costsize.rs` (add `cost_gather`/`cost_gather_merge`), `src/backend/optimizer/path/allpaths.rs` (wire `generate_useful_gather_paths` into the per-rel pathlist construction)
- **Deliverable:** planner actually adds a `GatherPath` on top of a cheap partial path when the combined cost beats the sequential plan under `parallel_setup_cost`/`parallel_tuple_cost`; `add_path` competition between Gather and non-Gather paths works correctly.
- **Deps:** Step 3.
- **Tests unblocked:** none alone (still need `createplan.rs` + executor + EXPLAIN to see it).
- **Effort:** M-L (this is the crux of the planner side; `generate_useful_gather_paths` interacts with pathkeys for the Merge variant, worth budgeting extra review time).

### Step 5 -- `create_gather_plan`/`create_gather_merge_plan` (Path -> Plan lowering)
- **PG files:** `backend/optimizer/plan/createplan.c` (`create_gather_plan`, `create_gather_merge_plan`, the `switch` arm in `create_plan_recurse` for `T_GatherPath`/`T_GatherMergePath`)
- **Our target files:** `src/backend/optimizer/plan/createplan.rs` (new functions + dispatch arm; follow the `parallel_workers: 0` hardcode sites at lines 1123/1227 as the insertion points)
- **Deliverable:** `Gather`/`GatherMerge` `Plan` nodes are actually constructed from the corresponding paths, with `num_workers` set from the path.
- **Deps:** Step 4.
- **Tests unblocked:** none alone.
- **Effort:** S-M.

### Step 6 -- Gather/GatherMerge executor nodes (sequential pass-through)
- **PG files:** `backend/executor/nodeGather.c` (skip `ExecParallelSetupTupleQueues`/DSM/worker-launch machinery; keep `ExecInitGather`'s subplan-init and `gather_getnext`'s leader-participation fallback path -- when no workers are available PG already falls back to running the subplan itself, which is exactly the behavior to always take), `backend/executor/nodeGatherMerge.c` (same; keep the heap-based merge logic since with one producer it degenerates to a pass-through but reusing the real merge code costs nothing and is more faithful for future real-worker upgrade)
- **Our target files:** create `src/backend/executor/nodeGather.rs` and `src/backend/executor/nodeGatherMerge.rs` (backend bodies -- currently only header stubs exist under `src/executor/`); wire `ExecInitGather`/`ExecEndGather`/`ExecShutdownGather`/`ExecReScanGather` (`src/executor/nodeGather.rs`) and the GatherMerge equivalents to call into the new backend bodies; add `Gather`/`GatherMerge` arms to `execProcnode.rs`'s `exec_init_node`/`exec_end_node`/`exec_rescan_node` matches (`src/backend/executor/execProcnode.rs:254,437,495,515,557`)
- **Deliverable:** a query with a `Gather` node in its plan executes correctly end-to-end, running the subplan once on the current task and returning its tuples unchanged; `GatherMerge` runs the subplan once and returns tuples already in sorted order (trivial single-input merge).
- **Deps:** Step 5.
- **Tests unblocked:** first point at which any parallel-query test can get past both `EXPLAIN` and execution for the specific queries under test -- but see EXPLAIN dependency below.
- **Effort:** M (executor-node plumbing is well-trodden in this codebase; the only wrinkle is deciding whether to model "0 real workers, 1 leader-runs-everything" as a special case or thread a `num_workers=0` path through the existing PG logic -- recommend the latter for max fidelity with minimal new code).

### Step 7 -- `EXPLAIN` command (cross-cutting prerequisite, not parallel-specific)
- **PG files:** `backend/commands/explain.c`, `explain_format.c`, `explain_dr.c`, `explain_state.c`
- **Our target files:** `src/backend/commands/explain.rs`, `explain_format.rs`, `explain_dr.rs`, `explain_state.rs` (all currently nonexistent as backend bodies; only header stubs at `src/commands/explain*.rs`)
- **Deliverable:** `EXPLAIN [ (COSTS OFF | ANALYZE | ...) ]` produces PG-formatted plan-tree text, including a `Gather`/`Gather Merge` node line with `Workers Planned: N` (and, if executed with `ANALYZE`, `Workers Launched: N` -- recommend hardcoding `Workers Launched` = `Workers Planned` under option (B), since no real workers launch or fail to launch).
- **Deps:** none technically (EXPLAIN is independent machinery), but *for parallel-query tests specifically* this must land before Steps 1-6 have any visible effect, since every test in scope wraps its assertions in `EXPLAIN`.
- **Tests unblocked:** this alone is the single biggest lever for `memoize`, `expressions`, and partially `join_hash`/`btree_index`/`reindex_catalog` -- none of which are primarily about parallel query. Flagging as a **cross-cutting dependency this task surfaces but does not own**; recommend tracking as a separate plan-004 step that parallel-query work depends on, not vice versa.
- **Effort:** L (whole new subsystem; out of scope for a "parallel query" work item per se, called out here because steps 1-6 are invisible to pg_regress without it).

### Step 8 -- Parallel Append (only if `select_parallel`/`write_parallel` are pursued to completion)
- **PG files:** `backend/executor/nodeAppend.c` (parallel-aware Append: `choose_next_subplan_for_leader`/`_worker`, `ExecAppendEstimate`/`InitializeDSM`), `backend/optimizer/path/allpaths.c` (`add_paths_to_append_rel` partial-subpath accumulation)
- **Our target files:** wherever PepperDB's `Append`/`nodeAppend` executor and `AppendPath` planner code live (not audited in this pass -- locate via `grep -rn "AppendPath\|nodeAppend" src/backend/optimizer src/backend/executor`)
- **Deliverable:** Under option (B), Parallel Append degenerates to plain sequential Append-over-partial-children (the "choose next subplan" logic simplifies to a simple counter since there's one worker); still needs the `parallel_workers` reloption (`ALTER TABLE ... SET (parallel_workers = N)`) plumbed through reloptions.
- **Deps:** Steps 1-7, plus table inheritance (`a_star`..`f_star`) which is a separately-tracked STUB in `tablecmds.rs` -- **not owned by this task**.
- **Tests unblocked:** `select_parallel`'s Parallel Append section specifically; the file as a whole remains OUT_OF_SCOPE regardless (PL/pgSQL, partitioning, custom opclasses, `pg_stat_database` counters all still missing).
- **Effort:** M, but low ROI standalone -- recommend deferring until inheritance + PL/pgSQL land, since `select_parallel` cannot pass end-to-end without them anyway.

### Not recommended as steps (out of this task's scope)
- Real DSM/bgworker/shm_mq emulation (option A) -- revisit only if a future milestone explicitly wants measured parallel speedup.
- `vacuum_parallel`'s `VACUUM (PARALLEL n)` worker path -- independent of Gather/executor parallelism; belongs to a VACUUM-focused work item (touches `parse_vacuum_options`, `pg_relation_size`/`pg_size_bytes`).
- `pg_stat_database.parallel_workers_to_launch/launched` counters -- trivial to wire once Step 6 exists (increment `to_launch` by the planned worker count, `launched` by 0 under option B, or by the same count if you want the stats to look "successful"); listed here rather than as its own step since it's a 2-line change once Step 6's data is available, not worth a dedicated step.

## Summary table

| Step | Deliverable | Effort | Blocking deps |
|---|---|---|---|
| 1 | Parallel GUCs registered | S | none |
| 2 | `consider_parallel` rel eligibility | M | 1 |
| 3 | Partial seqscan path + cost divisor | M | 2 |
| 4 | `create_gather_path`/`cost_gather` (+ merge variants) | M-L | 3 |
| 5 | Path -> Plan lowering for Gather/GatherMerge | S-M | 4 |
| 6 | Gather/GatherMerge executor nodes (sequential) | M | 5 |
| 7 | EXPLAIN command (cross-cutting, not owned here) | L | none (but gates visibility of 1-6) |
| 8 | Parallel Append (optional, low standalone ROI) | M | 1-7 + inheritance |

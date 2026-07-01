# Declarative partitioning -- step breakdown for plan 004

Read-only research output. Target: turn `ai/plans/004-pg-regress-conformance/file-list/23.txt`'s
one-line register entry ("declarative partitioning (tablecmds) # ~40 tests") into an
implementable step sequence, in the same format as plan 004's other steps.

## 1. PG source map (sizes, what each file does now)

| File | Lines | Role |
|---|---|---|
| `catalog/partition.c` | 392 | Thin catalog helpers only (post-PG18 refactor moved the core logic out): `get_partition_parent`, `get_partition_ancestors`, `index_get_partition`, `map_partition_varattnos`, `has_partition_attrs`, `get_default_partition_oid`/`update_default_partition_oid`, `get_proposed_default_constraint`. |
| `partitioning/partbounds.c` | 4979 | The core: `PartitionBoundSpec`/`PartitionBoundInfo` construction (`partition_bounds_create` for hash/list/range), overlap validation (`check_new_partition_bound`), qual generation (`get_qual_from_partbound`), plus ~half the file is `partition_bounds_merge` for partition-wise join (defer). |
| `partitioning/partdesc.c` | 508 | `RelationGetPartitionDesc` cache (`PartitionDesc`), `RelationBuildPartitionDesc` (scans pg_inherits + pg_class.relpartbound), `PartitionDirectory` (query-scoped consistent snapshot). Double-caching exists only to support DETACH CONCURRENTLY. |
| `partitioning/partprune.c` | 3831 | Planner/runtime pruning: `gen_partprune_steps`, `get_matching_partitions`, `PartitionPruneStepOp`/`Combine`. Optimization only -- routing correctness never depends on it. |
| `executor/execPartition.c` | 2635 | Tuple routing: `ExecFindPartition`, `PartitionDispatch` tree, `get_partition_for_tuple` (hash/list/range match), two column-remapping schemes (`AttrMap` parent->child, `TupleConversionMap` root->leaf), ON CONFLICT/MERGE re-targeting. |
| `commands/tablecmds.c` | 22126 (whole file) | DDL orchestration: `transformPartitionSpec`/`ComputePartitionAttrs` (parse PARTITION BY), `DefineRelation` (PARTITION BY / PARTITION OF paths), `ATExecAttachPartition`, `ATExecDetachPartition`/`DetachPartitionFinalize` (+ CONCURRENTLY 2-txn protocol). `StorePartitionKey`/`StorePartitionBound` actually live in `catalog/heap.c` (3911, 4067). |
| `catalog/pg_partitioned_table.h` | -- | Schema only: one row per partitioned table (`partrelid`, `partstrat`, `partnatts`, `partdefid`, `partattrs`/`partexprs`/`partclass`/`partcollation`). |
| optimizer partition-wise join/agg | scattered (`path/joinrels.c`, `path/allpaths.c`, `plan/planner.c`) | `try_partitionwise_join`, `generate_partitionwise_join_paths`, `create_partitionwise_grouping_paths`. Pure cost-based optimization; a partitioned table can always be planned as plain per-partition Append + ordinary join/agg. Fully deferrable. |

Complexity ranking: trivial (pg_partitioned_table schema, partition.c helpers) -> moderate
(partdesc.c if CONCURRENTLY is deferred, StorePartitionKey/Bound) -> complex
(transformPartitionSpec/ComputePartitionAttrs, ATExecAttachPartition, ATExecDetachPartition
non-concurrent, execPartition.c tuple routing) -> very-complex (partbounds.c bound
construction + check_new_partition_bound, DETACH CONCURRENTLY, partition-wise join merge).

## 2. PepperDB current state (all read via grep/Read, branch r3)

**Data model exists; nothing downstream is wired.** Specifically:

- AST already fully fielded in `src/nodes/parsenodes.rs:596-666`: `PartitionElem`,
  `PartitionStrategy` (LIST/RANGE/HASH), `PartitionSpec`, `PartitionBoundSpec`,
  `PartitionRangeDatumKind`, `PartitionRangeDatum`, `PartitionCmd`. `CreateStmt` already
  carries `partbound`/`partspec: Option<Box<...>>` fields (lines 1569-1570).
- `pg_class.rs` (113 lines) has real, correct relkind constants: `RELKIND_PARTITIONED_TABLE
  = b'p'`, `RELKIND_PARTITIONED_INDEX = b'I'`, and `RELKIND_HAS_PARTITIONS` correctly checks
  both.
- Header-stub layer already scaffolded (plan 001, all `unimplemented!()`, real bodies never
  written): `src/partitioning/partbounds.rs` (168 lines), `src/partitioning/partdesc.rs` (56),
  `src/partitioning/partprune.rs` (63), `src/executor/execPartition.rs` (120),
  `src/catalog/partition.rs` (53, 7 fns), `src/catalog/pg_partitioned_table.rs` (27, data
  only, no fns), `src/utils/partcache.rs` (77 lines; `PartitionKeyData` struct is real/fielded,
  3 of 8 fns are `unimplemented!()`, the other 5 are trivial field accessors that presuppose a
  `PartitionKeyData` nothing ever constructs).
- **Grammar has zero partition-DDL productions.** `PARTITION` is a keyword only for
  `OVER (PARTITION BY ...)` window syntax (`gram.lalrpop:2135-2138`). No `PARTITION BY` on
  CREATE TABLE, no `PARTITION OF`, no `FOR VALUES`, no `ATTACH`/`DETACH PARTITION`. A comment
  at line 571 explicitly flags this as deferred to "their milestones." SQL with a partition
  clause fails to parse today, before ever reaching DDL execution.
- **`DefineRelation` (`src/backend/commands/tablecmds.rs:57`) explicitly guards and panics**:
  `if stmt.partspec.is_some() || stmt.partbound.is_some() { not_yet_reachable("DefineRelation:
  partitioning"); }` (lines 72-74). Same pattern for `inhRelations` (69-71) and OF-type (75-77).
- **No ATTACH/DETACH PARTITION dispatch anywhere.** `ata_exec_cmd` (533-584) only handles
  AddColumn/DropColumn/ColumnDefault/SetNotNull/DropNotNull/AddConstraint/DropConstraint;
  anything else (including a hypothetical AttachPartition/DetachPartition variant) hits the
  `other => not_yet_reachable` catch-all (line 582).
- **`pg_inherits.rs` (75 lines) is 100% stub** -- all 8 functions (`find_inheritance_children`,
  `find_inheritance_children_extended`, `find_all_inheritors`, `has_subclass`, `has_superclass`,
  `StoreSingleInheritance`, `DeleteInheritsTuple`, `PartitionHasPendingDetach`) are
  `unimplemented!()`. This file is shared infrastructure for BOTH plain table inheritance and
  partitioning parent/child bookkeeping -- it's a hard prerequisite either way.
- **Planner-side relation expansion is a complete stub.** `src/optimizer/inherit.rs` (29
  lines): `expand_inherited_rtentry`, `get_rel_all_updated_cols`, `apply_child_basequals` are
  all `unimplemented!()`. Call sites already guard with `not_yet_reachable` (`initsplan.rs:161`,
  `allpaths.rs:102-103`, noting "M2 has no inheritance"). This is the mechanism that turns one
  range-table entry into N per-partition (or per-inheritance-child) scans -- there is currently
  no code path that does this for any reason, inheritance or partitioning.
- **Reusable base: Append executor node is real and working.** `src/backend/executor/
  nodeAppend.rs` (207 lines) has functional `exec_init_append`/`exec_append`/`exec_end_append`;
  its `unimplemented!()` calls are unreachable-guards (empty-subplan-list), not missing logic.
  This is a legitimate target for partition-scan Append plans once `inherit.rs` exists.
- **CREATE INDEX (btree only) is real, not stub**, for plain tables:
  `src/backend/commands/indexcmds.rs::define_index` (154) and `src/backend/catalog/
  index.rs::index_create`/`index_build` (140/247, 0 `unimplemented!()` in that file) are fully
  implemented. Good foundation for partitioned indexes, but the parent-index/fan-out-to-child
  logic (`RELKIND_PARTITIONED_INDEX` -> attach each leaf's own btree) doesn't exist.
  (Note: there is a second, older `src/catalog/index.rs`, the pre-M2 header-stub layer, still
  full of `unimplemented!()` for concurrent index build / REINDEX -- unrelated to partitioning,
  not touched by this plan.)
- ModifyTable executor (`nodeModifyTable.rs`, 445 lines) is real for INSERT/UPDATE/DELETE with
  RETURNING; its `unimplemented!()` calls gate MERGE and EPQ/concurrent-update recheck, not
  partitioning -- but MERGE's tuple-routing interaction (PG's execPartition.c explicitly notes
  MERGE re-targeting duplicates ON CONFLICT logic) means full MERGE-into-partitioned-table
  support is blocked on both this plan and step-016/whatever lands MERGE.

**Bottom line**: nothing partition-specific compiles or runs today; the AST/relkind scaffolding
exists but every consumer from grammar through executor is a hollow stub, and the two
cross-cutting prerequisites (`pg_inherits.rs` real logic, `optimizer/inherit.rs` relation
expansion) are also currently 100% stub and are needed regardless of whether table inheritance
ships first.

## 3. Tests unblocked (cross-ref `ai/tmp/regress-analysis/` + `ref/postgres/src/test/regress/sql/`)

Primary partition tests (partitioning is THE blocking feature, verdict OUT_OF_SCOPE today):

| Test | Lines | Notes |
|---|---|---|
| `create_table` | (partition section: 133 `--`-comment blocks total in file) | Dominant feature per regress-analysis is PARTITION BY LIST/RANGE/HASH + PARTITION OF with bound checks, multi-column keys, opclasses, collations, column-drop w/ partitions, domain-in-partition-key. Non-partition parts of this file (UNLOGGED/TEMP, AS SELECT, WITH OIDS rejection) are separate plan-003/004 concerns already scoped elsewhere; only the partition sections gate on this plan. |
| `alter_table` | 259 partition-mentioning lines | ATTACH/DETACH PARTITION sections; rest of file (RENAME, OWNER TO, ALTER COLUMN TYPE, identity) is out of scope for other reasons too -- partition is one of several blockers here, not the only one. |
| `insert` | ~187+ (partition section) | "Direct partition inserts should check partition bound constraint" -- tuple routing on INSERT, multi-level partitioning, BEFORE-ROW triggers on partitions. |
| `indexing` | 995 total, 306 partition-mentioning lines | Heaviest partition-adjacent file: partitioned indexes (`ALTER INDEX ... ATTACH PARTITION`), attach/detach, PARTITION BY RANGE/LIST/HASH. Also needs gin/brin/spgist for a few subtests (not partition-blocked, separately out of scope). |
| `hash_part` | 90 | `PARTITION BY HASH`, `FOR VALUES WITH (MODULUS, REMAINDER)`, `satisfies_hash_partition()` builtin, custom opclasses from test_setup. Small, self-contained, good early smoke test. |
| `partition_join` | 1256 | Partitioned-table joins, `enable_partitionwise_join` GUC, partitionwise join planning. Correctness (plain join over Append-scanned partitions) needs only steps 1-4 below; the partitionwise-join *optimization* itself is a separate later step. |
| `partition_prune` | 1520 | Partition pruning at plan and execution time, deeply nested `PARTITION OF ... FOR VALUES IN/FROM/TO/DEFAULT`, `PREPARE`/`EXECUTE force_generic_plan`. Also needs PL/pgSQL function execution (`explain_analyze()`, `list_part_fn()` helpers) -- a compounding non-partition blocker already registered in file-list/23.txt. |
| `partition_aggregate` | 341 | Partitionwise aggregate + join GUCs, GROUP BY/HAVING agg pushdown across partitions. Pure planner optimization test; needs the optimization step, not just base partitioning. |
| `partition_info` | 129 | `pg_partition_tree()`, `pg_partition_ancestors()`, `pg_partition_root()` introspection functions (new, not yet built anywhere in src/backend), multi-level partitioning, partitioned indexes, legacy inheritance interaction. |

Secondary/compounded tests (partitioning is A blocker among several, verdict OUT_OF_SCOPE or
NEEDS_MODULES for other reasons too -- won't fully flip to PASS from this plan alone, but this
plan removes one of their blockers):

- `inherit` -- partitioning listed alongside classic multiple inheritance, `ALTER TABLE
  INHERIT/NO INHERIT`, gist/circle. Needs plan's `pg_inherits.rs` work either way (shared with
  inheritance), but full PASS needs the separate INHERITS grammar/DDL work too (already
  registered under "table inheritance" in file-list/23.txt).
- `identity` -- "identity+partitioning interaction" is one line item among many (mostly
  GENERATED AS IDENTITY grammar, which doesn't exist).
- `create_table_like`, `typed_table`, `predicate` -- inheritance-adjacent, not primarily
  partition-blocked.

**Count**: 9 primary partition tests + partial credit on `inherit`/`identity`/others => matches
the ~40-test estimate in file-list/23.txt only if "indexing" and "alter_table"'s large partition
sections are counted as fractional tests (pg_regress tests are per-file, so literally it's 9
whole files that go OUT_OF_SCOPE-to-PASS plus incremental progress on a few more). Recommend
tracking as **9 file-level targets**, with `alter_table`/`indexing`/`inherit`/`identity` noted
as "partition section will pass, full file may still show unrelated diffs from other gaps."

## 4. Step breakdown

Steps are ordered by dependency. Each can be one plan-004-style `file-list/NN.txt` entry with
sub-steps where a step is large enough to need its own implement/review/commit cycle (per plan
003's convention: a LARGE step splits into agent-reviewed sub-commits, squashed at the end).

---

### Step P1 -- pg_inherits real implementation (shared prerequisite)

- **PG files**: `ref/postgres/src/backend/catalog/pg_inherits.c` (not read in depth this pass;
  small, catalog-scan file), `catalog/heap.c::StoreSingleInheritance`.
- **Target files**: `src/catalog/pg_inherits.rs` (currently 75 lines, 8 `unimplemented!()` fns).
- **Deliverable**: `find_inheritance_children`, `find_inheritance_children_extended`,
  `find_all_inheritors`, `has_subclass`, `has_superclass`, `StoreSingleInheritance`,
  `DeleteInheritsTuple` become real (catalog scan/insert/delete over `pg_inherits`).
  `PartitionHasPendingDetach` can stay stubbed/return-false if DETACH CONCURRENTLY is deferred
  (see P6b).
- **Deps**: none beyond existing catalog/heap infrastructure (plan 002/003 F1 storage core).
- **Tests unblocked**: none directly (infrastructure only), but gates every later step.
- **Effort**: moderate. Straightforward catalog CRUD; the only subtlety is
  `find_all_inheritors`'s transitive closure and lock-ordering (PG sorts by OID to avoid
  deadlock -- replicate that).

### Step P2 -- Grammar: PARTITION BY / PARTITION OF / FOR VALUES

- **PG files**: `ref/postgres/src/backend/parser/gram.y` (grep `PartitionSpec`,
  `PartitionBoundSpec`, `OptPartitionSpec`, `PartitionElem`, `create_generic_options`-adjacent
  `ForValuesClause` productions), `parser/scan.l` for new keywords.
- **Target files**: `src/backend/parser/gram.lalrpop` (add productions; keyword list already
  has `PARTITION` at line 280 for window syntax -- extend its use, do not redefine).
- **Deliverable**: `CREATE TABLE name (...) PARTITION BY {LIST|RANGE|HASH} (col_or_expr, ...)`,
  `CREATE TABLE name PARTITION OF parent FOR VALUES {IN (...) | FROM (...) TO (...) |
  WITH (MODULUS n, REMAINDER r) | DEFAULT}` all parse into the already-existing
  `PartitionSpec`/`PartitionBoundSpec` AST nodes (`src/nodes/parsenodes.rs:596-666` -- no AST
  changes needed, only grammar wiring).
- **Deps**: none (pure grammar; AST already shaped).
- **Tests unblocked**: none alone (parses but `DefineRelation` still panics) -- but is the
  precondition for every DDL test below.
- **Effort**: moderate. lalrpop grammar surgery around an existing CreateStmt production;
  risk of ambiguity with the OVER-clause PARTITION BY needs care (different non-terminal
  context, should not conflict, but verify).

### Step P3 -- pg_partitioned_table catalog + StorePartitionKey/StorePartitionBound

- **PG files**: `catalog/heap.c::StorePartitionKey` (~3911), `StorePartitionBound` (~4067),
  `commands/tablecmds.c::transformPartitionSpec`, `ComputePartitionAttrs`.
- **Target files**: `src/catalog/pg_partitioned_table.rs` (add insert/lookup functions; struct
  already correct), `src/backend/catalog/heap.rs` (add `StorePartitionKey`/`StorePartitionBound`
  -- new functions, file already real/non-stub for the base heap-create path), `src/catalog/
  partition.rs` (implement `get_default_partition_oid`/`update_default_partition_oid` against
  the new catalog).
  Also add `pg_class.relispartition`/`relpartbound` columns if not already present --
  **verify** `src/catalog/pg_class.rs`'s `FormData_pg_class` has these fields before assuming;
  they may need adding.
- **Deliverable**: parsing a partition key (`ComputePartitionAttrs`-equivalent: resolve columns
  vs expressions, reject system columns/mutable functions/subqueries) and writing one
  `pg_partitioned_table` row + `pg_class` relkind flip to `RELKIND_PARTITIONED_TABLE`.
- **Deps**: P1 (pg_inherits, used once a partition is attached), P2 (grammar/AST arrives
  parsed).
- **Tests unblocked**: none fully yet (bound validation/attach still missing) but is the
  DDL-storage half of `create_table`'s partition sections.
- **Effort**: complex. The attribute-vs-expression resolution logic
  (`ComputePartitionAttrs`) has many rejection cases to replicate faithfully.

### Step P4 -- partition bounds: construction + overlap validation (the core, very-complex)

- **PG files**: `partitioning/partbounds.c` -- `partition_bounds_create` (dispatches to
  `create_hash_bounds`/`create_list_bounds`/`create_range_bounds`), `check_new_partition_bound`,
  `get_qual_from_partbound`/`get_qual_for_{hash,list,range}`, comparison primitives
  (`partition_rbound_cmp`, `partition_hbound_cmp`, `partition_{range,list,hash}_bsearch`,
  `compute_partition_hash_value`). Explicitly **exclude** `partition_bounds_merge` and the
  `PartitionMap` machinery (partition-wise join only, see step P9).
- **Target files**: `src/partitioning/partbounds.rs` (currently 168-line stub -- this is where
  `PartitionBoundInfoData`/`PartitionBoundSpec`-consuming logic belongs; verify the existing
  stub's function signatures match before filling bodies).
- **Deliverable**: given a set of sibling `PartitionBoundSpec`s, build the canonical sorted
  `PartitionBoundInfo` for hash/list/range, detect overlaps against a proposed new bound
  (`check_new_partition_bound`), and generate the implicit CHECK-constraint-equivalent qual for
  a bound (used by both DDL validation and, later, constraint-exclusion-style pruning).
- **Deps**: P3 (needs a real partition key + catalog row to validate against).
- **Tests unblocked**: none in isolation, but is the single highest-value step -- unblocks
  `create_table`'s bound-overlap-rejection tests once wired into P5, and `hash_part.sql`'s
  modulus/remainder validation.
- **Effort**: very-complex. This is the algorithmic heart of the feature (sorted bound arrays,
  binary search, hash bucket congruence-class checking). Recommend its own review pass separate
  from the DDL wiring in P5.

### Step P5 -- CREATE TABLE PARTITION BY / PARTITION OF end-to-end DDL

- **PG files**: `commands/tablecmds.c::DefineRelation` (partition branches, ~764-1250),
  `catalog/heap.c::StorePartitionBound`.
- **Target files**: `src/backend/commands/tablecmds.rs::DefineRelation` (remove the
  `not_yet_reachable("DefineRelation: partitioning")` guard at lines 72-74; add real branches).
- **Deliverable**: `CREATE TABLE t (...) PARTITION BY ...` creates a `RELKIND_PARTITIONED_TABLE`
  row + `pg_partitioned_table` row (via P3). `CREATE TABLE p PARTITION OF t FOR VALUES ...`
  validates the bound via P4's `check_new_partition_bound`, creates the child relation, records
  `pg_inherits` (via P1) and `pg_class.relpartbound`/`relispartition`.
- **Deps**: P1, P2, P3, P4.
- **Tests unblocked**: `create_table` (partition sections), `hash_part` (fully -- it's a small,
  self-contained CREATE-only test with no queries/DML against the partitions beyond
  `satisfies_hash_partition()`, which is a one-off builtin function easy to add alongside).
- **Effort**: complex.

### Step P6a -- ALTER TABLE ATTACH PARTITION (non-concurrent)

- **PG files**: `commands/tablecmds.c::ATExecAttachPartition` (~20252-20565).
- **Target files**: `src/backend/commands/tablecmds.rs::ata_exec_cmd` (add
  `AlterTableType::AttachPartition` arm; check whether `AlterTableType`/`AlterTableCmd` AST
  already has this variant in `parsenodes.rs` -- verify before assuming, add if missing).
- **Deliverable**: `ALTER TABLE parent ATTACH PARTITION child FOR VALUES ...` -- lock
  ordering, circularity/compatibility checks (not already a partition, no incompatible
  triggers, matching temp-ness, columns exist in parent), `check_new_partition_bound`,
  `pg_inherits` row via `CreateInheritance`-equivalent, `StorePartitionBound`, index matching
  (deferred to P8 if partitioned-index support isn't ready -- attach can initially require no
  pre-existing indexes, or create matching plain btree indexes per P8).
- **Deps**: P1, P4, P5.
- **Tests unblocked**: `alter_table` (ATTACH sections), contributes to `indexing`,
  `partition_prune`/`partition_join`/`partition_aggregate`/`partition_info` (all use
  `PARTITION OF` syntax primarily, but some also use explicit ATTACH -- verify per-file mix;
  most PG regress partition tests favor inline `PARTITION OF`, so P5 alone may satisfy most of
  their CREATE-time needs, with ATTACH exercised in dedicated sections of `alter_table`/
  `create_table`).
- **Effort**: complex.

### Step P6b -- ALTER TABLE DETACH PARTITION (non-concurrent only; defer CONCURRENTLY)

- **PG files**: `commands/tablecmds.c::ATExecDetachPartition` (~20914-21088),
  `DetachPartitionFinalize` (21096+). Explicitly **defer** the CONCURRENTLY 2-transaction
  protocol (`MarkInheritDetached`, `DetachAddConstraintIfNeeded`, `ATExecDetachPartitionFinalize`
  resumption path) -- PG's own most complex control flow in this feature, and pg_regress core
  suite tests do exercise `DETACH ... CONCURRENTLY` in `alter_table.sql`; if so, snapshot those
  specific sub-cases as KNOWN-DIFF per plan 004's harness discipline rather than blocking the
  step.
- **Target files**: `src/backend/commands/tablecmds.rs::ata_exec_cmd` (add
  `AlterTableType::DetachPartition` arm, non-concurrent path only).
- **Deliverable**: `ALTER TABLE parent DETACH PARTITION child` removes the `pg_inherits` row
  and clears `relpartbound`/`relispartition` in one transaction.
- **Deps**: P1, P6a (shares validation helpers).
- **Tests unblocked**: `alter_table` (DETACH non-concurrent sections).
- **Effort**: complex (non-concurrent path only; CONCURRENTLY is very-complex, deferred).

### Step P7 -- tuple routing on INSERT (execPartition.c)

- **PG files**: `executor/execPartition.c::ExecFindPartition`, `PartitionDispatch` tree walk,
  `get_partition_for_tuple` (hash/list/range match against P4's `PartitionBoundInfo`),
  `ExecInitPartitionInfo` (attribute-map/tuple-conversion setup). Defer the ON CONFLICT/MERGE
  re-targeting sub-logic to a follow-on (or fold into whichever step lands ON CONFLICT/MERGE
  generally -- `insert_conflict.sql` is already a separate stub blocked on
  `nodeModifyTable.rs`'s `on_conflict_action == NONE` assert, tracked outside this plan).
- **Target files**: `src/executor/execPartition.rs` (currently 120-line stub;
  `PartitionDispatchData`/`PartitionTupleRouting` are empty marker structs --  needs real
  fields), `src/backend/executor/nodeModifyTable.rs` (wire routing into the INSERT path before
  `heap_insert`).
- **Deliverable**: `INSERT INTO partitioned_table VALUES (...)` evaluates the partition key,
  descends the dispatch tree (supporting multi-level sub-partitioning), and inserts into the
  correct leaf partition's heap, converting column order via `TupleConversionMap` if parent/leaf
  layouts differ.
- **Deps**: P4 (bound matching), P5 (need a queryable `PartitionDesc` -- see note below on
  P7-prereq), P1.
- **Prereq note**: PG's `RelationGetPartitionDesc` (`partitioning/partdesc.c`) is the runtime
  cache that assembles `PartitionBoundInfo` + child OIDs for a relation; this plan's step P4
  covers bound *construction* but the caching/assembly entry point itself
  (`src/partitioning/partdesc.rs`, 56-line stub) needs its own fill-in, folded into either P5 or
  P7 (recommend P7, since it's first needed at DML time, though DDL-time validation in P4/P5
  also calls it to check for overlaps against existing siblings -- **whichever of P4/P5/P7 lands
  first should implement a non-cached, direct pg_inherits-scan version; caching can be added
  later as a performance pass**).
- **Tests unblocked**: `insert` (partition-routing sections).
- **Effort**: complex.

### Step P8 -- planner: Append-based partition scanning (SELECT correctness, no pruning yet)

- **PG files**: `optimizer/util/inherit.c::expand_inherited_rtentry` (the general
  inheritance/partition RTE-expansion mechanism -- shared with plain table inheritance),
  `apply_child_basequals`.
- **Target files**: `src/optimizer/inherit.rs` (currently 29-line, 3-fn complete stub --
  **shared prerequisite with table inheritance**, not partition-specific logic itself),
  `src/backend/optimizer/plan/initsplan.rs:161` and `src/backend/optimizer/path/allpaths.rs:
  102-103` (remove the `not_yet_reachable` guards once `inherit.rs` is real).
- **Deliverable**: `SELECT * FROM partitioned_table WHERE ...` expands the partitioned table's
  RTE into one child RTE per leaf partition, builds an Append plan over per-partition scans
  (reusing the already-functional `nodeAppend.rs`), with each child's quals adjusted
  (`apply_child_basequals`) for column-order/type differences.
  **Scope decision**: if table inheritance (also in file-list/23.txt) is scheduled first,
  this step should instead be a small addition on top of that work rather than a duplicate;
  flag as a shared dependency when scheduling, not a partitioning-exclusive step.
- **Deps**: P5 (need a real `PartitionDesc`/child list to expand from), P1.
- **Tests unblocked**: contributes to `partition_join`, `partition_aggregate` (base
  correctness -- full join test needs P9 for the partitionwise-join subtests specifically
  gated by `enable_partitionwise_join`, but plain joins over Append-scanned partitions should
  already produce correct, if not optimally planned, results).
- **Effort**: complex, but substantially shared cost with inheritance -- if inheritance lands
  first, this step's effort mostly evaporates into "verify partitioning reuses it."

### Step P9 -- partition pruning (planner + executor)

- **PG files**: `partitioning/partprune.c::gen_partprune_steps`, `get_matching_partitions`,
  `PartitionPruneStepOp`/`Combine`, wiring into `createplan.c`'s Append/MergeAppend builder and
  `execPartition.c`'s pruning consumers.
- **Target files**: `src/partitioning/partprune.rs` (63-line stub), `src/nodes/plannodes.rs`
  (`PartitionPruneInfo`/`PartitionedRelPruneInfo` already fully fielded -- no AST work needed),
  `src/backend/optimizer/plan/createplan.rs` (attach pruning info to Append nodes),
  `src/executor/execPartition.rs` (`ExecDoInitialPruning`/`ExecInitPartitionExecPruning`/
  `ExecFindMatchingSubPlans` -- already stubbed with matching signatures).
- **Deliverable**: static (planning-time, constant-qual) pruning first -- shrink the candidate
  partition list before Append path generation using WHERE-clause constants matched against
  partition-key columns. Runtime/dynamic pruning (parameterized quals, nested-loop rebinding)
  can be a follow-on within this same step or split further if needed.
- **Deps**: P8 (Append-based scanning must exist first -- pruning only narrows it), P4 (bound
  comparison primitives).
- **Tests unblocked**: `partition_prune` (also needs PL/pgSQL execution for its helper
  functions `explain_analyze()`/`list_part_fn()` -- a separate, already-registered blocker in
  file-list/23.txt; this step alone gets partition_prune's core pruning assertions but not a
  full-file PASS until PL/pgSQL lands too), `partition_info` (needs `pg_partition_tree()`/
  `pg_partition_ancestors()`/`pg_partition_root()` -- new introspection functions, small,
  can be added in this step or P5/P1 since they just walk `pg_inherits`/`PartitionDesc`).
- **Effort**: moderate (static-only) to complex (with dynamic/runtime pruning). Explicitly
  **not required for correctness** -- can be staged after P7/P8 land and tests are already
  passing via full-scan Append.

### Step P10 -- partitioned indexes

- **PG files**: `commands/tablecmds.c`/`commands/indexcmds.c` partitioned-index attach logic
  (`RELKIND_PARTITIONED_INDEX` fan-out, `ALTER INDEX ... ATTACH PARTITION`), `catalog/
  partition.c::index_get_partition`.
- **Target files**: `src/backend/commands/indexcmds.rs` (extend `define_index` to detect a
  partitioned target and create a parent `RELKIND_PARTITIONED_INDEX` row fanning out to one
  child btree index per leaf, reusing the already-real `index_create`/`index_build`),
  `src/catalog/partition.rs::index_get_partition` (implement).
- **Deliverable**: `CREATE INDEX ON partitioned_table (col)` creates a partitioned index with
  per-partition child btree indexes; `ALTER INDEX parent ATTACH PARTITION child_idx` links a
  manually-created child index.
- **Deps**: P5, P6a (attach path shares logic), existing btree CREATE INDEX (already real).
- **Tests unblocked**: `indexing` (the large partition+index file -- this is likely its single
  biggest remaining blocker after P5/P6a land, since the file is dominated by partitioned-index
  attach/detach scenarios).
- **Effort**: complex. No new algorithmic content (btree machinery is reused) but real fan-out
  bookkeeping (child index creation timing, dropping a partition's index when the partition is
  detached, `ALTER INDEX ... ATTACH PARTITION` matching childless parent slots).

### Step P11 (optional, can stage indefinitely) -- partition-wise join / aggregate

- **PG files**: `partitioning/partbounds.c::partition_bounds_merge` + `PartitionMap` machinery,
  `optimizer/path/joinrels.c::try_partitionwise_join`, `optimizer/path/allpaths.c::
  generate_partitionwise_join_paths`, `optimizer/plan/planner.c::
  create_partitionwise_grouping_paths`.
- **Target files**: new/extended files under `src/backend/optimizer/path/` and
  `src/backend/optimizer/plan/planner.rs`; extends `RelOptInfo`
  (`consider_partitionwise_join`, `part_scheme`, `part_rels[]`, etc. -- verify current field
  presence in `src/nodes/pathnodes.rs` before assuming absent).
- **Deliverable**: when `enable_partitionwise_join`/`enable_partitionwise_aggregate` GUCs are
  on and both join sides share a compatible partition scheme, plan per-partition joins/
  aggregates instead of a single join/agg over the full Append.
- **Deps**: P8, P9 (bound-merge logic reuses P4's comparison primitives), functioning
  `enable_partitionwise_join`/`enable_partitionwise_aggregate` GUCs (check `src/backend/
  optimizer/cost.rs` -- regress-analysis notes these GUCs are "declared" already).
- **Tests unblocked**: the `enable_partitionwise_join`/`enable_partitionwise_aggregate`-gated
  subtests within `partition_join` and `partition_aggregate` specifically (their EXPLAIN output
  differs based on whether this optimization fires -- without it, correctness is fine but the
  EXPLAIN diff will show a NEW-DIFF against upstream `expected/*.out` unless snapshotted as
  KNOWN-DIFF).
- **Effort**: very-complex. **Purely a cost-based optimization** -- does not affect result
  correctness, only plan shape / EXPLAIN output. Recommend: ship P1-P10 first, snapshot
  `partition_join`/`partition_aggregate`'s partitionwise-specific EXPLAIN assertions as
  KNOWN-DIFF, and treat this step as a stretch goal or separate future plan.

## 5. Architecture notes

**Dependency graph** (arrows = "needs"):

```
P1 (pg_inherits)  ---------------------------\
P2 (grammar)  ---\                            |
                  v                           v
              P3 (pg_partitioned_table) -> P4 (bounds) -> P5 (CREATE TABLE PARTITION BY/OF)
                                                             |         |         |
                                                             v         v         v
                                                    P6a (ATTACH)  P7 (routing) P8 (Append scan, shared w/ inheritance)
                                                       |                          |
                                                       v                          v
                                                    P6b (DETACH)              P9 (pruning) --(optional)--> P11 (partitionwise join/agg)
                                                       |
                                                       v
                                                    P10 (partitioned indexes)
```

**Must-have vs can-stage**:
- Must-have for ANY partitioned table to work at all: P1-P5 (DDL creates the structures), P7
  (INSERT routes correctly -- without this, data written to a partitioned table has nowhere
  correct to go), P8 (SELECT sees all the data -- without this, queries against a partitioned
  table return nothing or error).
- Should-have soon after (most regress tests exercise these): P6a/P6b (ATTACH/DETACH), P10
  (partitioned indexes -- `indexing.sql` is large and index-attach-heavy).
- Can-stage / pure optimization, safe to defer arbitrarily: P9 (pruning -- correctness is
  unaffected, only performance and possibly EXPLAIN-output test diffs), P11 (partitionwise
  join/agg -- same). DETACH CONCURRENTLY within P6b is also stageable independently (snapshot
  as KNOWN-DIFF).

**Interaction with indexes**: PepperDB has only btree (`access/nbtree`). This is not a blocker
for the *base* partitioning feature (P1-P9) since PG's DDL/routing/pruning logic doesn't care
which AM backs an index -- but P10 (partitioned indexes) and any `indexing.sql` subtests that
specifically require gin/brin/spgist partitioned indexes will stay OUT_OF_SCOPE regardless of
how much of this plan lands, gated instead on those AMs (already tracked separately in
file-list/23.txt under "non-btree index AMs"). Scope P10 to btree-only partitioned indexes;
document the gin/brin/spgist subtests of `indexing.sql` as a residual known-gap.

**Interaction with other file-list/23.txt items**:
- **Table inheritance** (separate registered item) shares P1 (`pg_inherits.rs`) and most of P8
  (`optimizer/inherit.rs`'s `expand_inherited_rtentry` is the *general* RTE-expansion mechanism,
  used by both INHERITS and PARTITION BY). Recommend scheduling these two file-list items
  together, or partitioning-first with inheritance reusing P1/P8's real implementations
  (partitioning's needs are a superset: it additionally needs bound matching, which
  plain inheritance does not).
- **PL/pgSQL function execution** (separate registered item) is an independent compounding
  blocker for `partition_prune.sql` specifically (its helper functions are PL/pgSQL). This
  plan's P9 gets partition_prune's SQL-level assertions right, but the file won't fully PASS
  until PL/pgSQL lands too -- track as a joint dependency, not a reason to delay P9.
- **ON CONFLICT / MERGE** (already separately stubbed, see `insert_conflict.sql` analysis) has
  its own re-targeting logic in execPartition.c that this plan explicitly defers out of P7;
  whichever plan lands general ON CONFLICT/MERGE support should re-open execPartition.rs to add
  the partition-aware re-targeting once both sides exist.

## 6. Summary table

| Step | Effort | Deps | Tests unblocked |
|---|---|---|---|
| P1 pg_inherits real impl | moderate | none | (infra only) |
| P2 grammar | moderate | none | (infra only) |
| P3 pg_partitioned_table + StorePartitionKey/Bound | complex | P1, P2 | (infra only) |
| P4 partition bounds construction + validation | very-complex | P3 | (infra only) |
| P5 CREATE TABLE PARTITION BY/OF DDL | complex | P1-P4 | create_table (partition sections), hash_part |
| P6a ATTACH PARTITION | complex | P1, P4, P5 | alter_table (ATTACH sections) |
| P6b DETACH PARTITION (non-concurrent) | complex | P1, P6a | alter_table (DETACH sections) |
| P7 tuple routing (INSERT) | complex | P4, P5, P1 | insert (partition sections) |
| P8 Append-based partition scanning | complex (shared w/ inheritance) | P5, P1 | partition_join, partition_aggregate (base correctness) |
| P9 partition pruning | moderate-complex, OPTIONAL for correctness | P8, P4 | partition_prune (partial -- also needs PL/pgSQL), partition_info |
| P10 partitioned indexes (btree only) | complex | P5, P6a | indexing |
| P11 partitionwise join/agg | very-complex, OPTIONAL/deferrable | P8, P9 | partition_join/partition_aggregate EXPLAIN-output subtests only |

**9 primary test files** targeted: create_table, alter_table, insert, indexing, hash_part,
partition_join, partition_prune, partition_aggregate, partition_info. Full PASS on
partition_prune requires PL/pgSQL (separate item); full PASS on indexing's non-btree subtests
requires other index AMs (separate item); the partitionwise-specific EXPLAIN assertions in
partition_join/partition_aggregate require P11 or KNOWN-DIFF snapshotting.

# Row-Level Security (RLS) -- implementation step breakdown for plan 004

Status: research only, no code changed. Target: extend plan 004
(`ai/plans/004-pg-regress-conformance`) with an RLS phase after the roles step
(current step 18). `rowsecurity` and the RLS portion of `privileges` are
currently in the **deferred XL register** (`file-list/23.txt` lines 11 and 19),
not in the scheduled 07-22 steps. This document turns that one-line register
entry into an implementable, dependency-ordered breakdown.

## 1. PG source inventory

| File | Lines | Role |
|---|---|---|
| `ref/postgres/src/backend/rewrite/rowsecurity.c` | 932 | `get_row_security_policies` -- qual/WCO injection per command type |
| `ref/postgres/src/backend/commands/policy.c` | 1279 | CREATE/ALTER/DROP POLICY, RelationBuildRowSecurity, rename |
| `ref/postgres/src/backend/utils/misc/rls.c` | 168 | `check_enable_rls` (RLS_NONE / RLS_NONE_ENV / RLS_ENABLED), `row_security_active()` SQL funcs |
| `ref/postgres/src/include/rewrite/rowsecurity.h` | 49 | `RowSecurityPolicy`, `RowSecurityDesc`, hook typedefs |
| `ref/postgres/src/include/commands/policy.h` | 38 | prototypes |
| `ref/postgres/src/include/catalog/pg_policy.h` | 58 | `CATALOG(pg_policy,3256,...)`: oid, polname, polrelid, polcmd (char), polpermissive, polroles (Oid[]), polqual/polwithcheck (pg_node_tree) |
| grammar (`gram.y`) | ~90 lines total | `CreatePolicyStmt`/`AlterPolicyStmt` (~5900-5982), `AT_EnableRowSecurity`/`AT_DisableRowSecurity`/`AT_ForceRowSecurity`/`AT_NoForceRowSecurity` in `alter_table_cmd` (~2969-2999), `ALTER POLICY ... RENAME TO` (~9506/9517), `POLICY` keyword -> `OBJECT_POLICY` (~7094) |
| GUC | 1 var | `row_security` (PGC_USERSET, bool, default on) in `guc_tables.c:1696` |

Total core logic to port: **~2,570 lines of C** (932 + 1279 + 168 + headers),
plus a small grammar slice and one GUC. Excludes DROP POLICY (handled through
the generic `dependency.c`/`deletion` object-drop path, not a dedicated
function) and pg_dump/psql `\d` support (out of scope for pg_regress core
semantics).

### Mechanism summary (for the step writer, not just a pointer)

- **Storage**: `pg_class.relrowsecurity`/`relforcerowsecurity` (bools) gate
  whether RLS applies at all; `pg_policy` (OID 3256) holds one row per policy:
  name, target table, `polcmd` (`'*'`/`r`/`a`/`w`/`d` -- ACL_SELECT/INSERT/
  UPDATE/DELETE _CHR), `polpermissive`, `polroles` (Oid array, `0`=PUBLIC),
  `polqual`/`polwithcheck` (serialized `Node*` trees, i.e. `pg_node_tree`,
  parsed with the table already in scope so Vars resolve).
- **check_enable_rls** (`rls.c`): returns an RLS status per relation+user:
  `RLS_NONE` (relrowsecurity off, or catalog/system table), `RLS_NONE_ENV`
  (BYPASSRLS role, or owner without FORCE ROW LEVEL SECURITY, or
  `InNoForceRLSOperation()` true for FK checks) or `RLS_ENABLED`. If enabled
  but the `row_security` GUC is off, raises `ERRCODE_INSUFFICIENT_PRIVILEGE`
  unless `noError`.
- **get_row_security_policies** (`rowsecurity.c`): called once per
  RTE_RELATION in `fireRIRrules` (rewriteHandler.c line ~2253), AFTER the
  main per-RTE rule-walker loop, in a dedicated second loop over
  `parsetree->rtable`. For each RTE: resolve `user_id` from
  `perminfo->checkAsUser` or `GetUserId()`; call `check_enable_rls`; if
  `RLS_NONE` skip; if `RLS_NONE_ENV` just set `hasRowSecurity=true` and
  return (forces replan on env change, e.g. `SET ROLE`); if `RLS_ENABLED`,
  fetch policies via `get_policies_for_relation` (filtered by cmd type +
  `check_role_for_policy`, which checks `has_privs_of_role` against each
  entry in `polroles`, short-circuiting on the PUBLIC sentinel) and inject:
  - **USING quals** (SELECT/UPDATE/DELETE, plus UPDATE quals first if
    `FOR UPDATE/SHARE` requires it, plus SELECT quals appended if
    UPDATE/DELETE/MERGE also needs SELECT rights e.g. for RETURNING) via
    `add_security_quals` -> combined permissive-OR / restrictive-AND, or a
    single `false` qual if there are zero permissive policies (default deny).
    Appended to `rte->securityQuals`.
  - **WITH CHECK options** (INSERT/UPDATE, plus ON CONFLICT DO UPDATE and
    MERGE variants) via `add_with_check_options` -> a `WithCheckOption` node
    per restrictive policy (so violations can name the policy) plus one OR'd
    node for all permissive policies; uses `polwithcheck` if present else
    falls back to `polqual`. Appended to `parsetree->withCheckOptions`.
  - Sets `hasRowSecurity = true` unconditionally once RLS_ENABLED path is
    taken (for plancache invalidation on role/GUC change).
  - Sublinks in injected quals get `ChangeVarNodes` (Var reindex to `rt_index`)
    and are re-walked through `fireRIRonSubLink`-equivalent recursion with an
    `activeRIRs`-style infinite-recursion guard (a policy that queries its
    own table via a sublink).
- **Enforcement of WithCheckOptions** happens at executor time
  (`ExecWithCheckOptions` in `execMain.c`, not in rowsecurity.c itself) --
  each row proposed for INSERT/UPDATE is tested against every WCO qual;
  failure raises `ERRCODE_WITH_CHECK_OPTION_VIOLATION`, naming the policy if
  `wco->polname` is set.
- **CREATE POLICY** (`policy.c`): validates polcmd/qual combination (SELECT
  and DELETE forbid WITH CHECK; INSERT forbids USING), builds a `ParseState`
  with the target table already added as a range table entry (so the policy
  expression's bare column references resolve), transforms `qual`/`with_check`
  as `EXPR_KIND_POLICY`, serializes to `pg_node_tree`, records normal
  dependency on the table and shared dependencies on each named role.
- **ALTER TABLE ENABLE/FORCE ROW LEVEL SECURITY**: sets/clears
  `pg_class.relrowsecurity`/`relforcerowsecurity` (this part lives in
  `tablecmds.c`, not policy.c -- flag as a small addition to whatever ALTER
  TABLE dispatcher already exists).

## 2. PepperDB current state

Everything below already exists as a **Level-1 header stub** (signature +
`unimplemented!()`), i.e. plan 001/002 already carved out the shape; no
grammar or catalog seed rows exist yet.

| File | Status |
|---|---|
| `src/utils/rls.rs` | stub: `CheckEnableRlsResult` enum + `check_enable_rls() -> unimplemented!()`; `row_security: bool` static (not read by any GUC table) |
| `src/rewrite/rowsecurity.rs` | stub: `RowSecurityPolicy`, `RowSecurityDesc`, `RowSecurityPolicies` (bundles the 4 C out-params), hook statics; `get_row_security_policies() -> unimplemented!()` |
| `src/commands/policy.rs` | stub: `RelationBuildRowSecurity`, `RemovePolicyById`, `RemoveRoleFromObjectPolicy`, `CreatePolicy`, `AlterPolicy`, `get_relation_policy_oid`, `rename_policy`, `relation_has_policies` -- all `unimplemented!()` |
| `src/catalog/pg_policy.rs` | stub: `FormData_pg_policy` struct matches the C catalog row 1:1 (oid, polname, polrelid, polcmd, polpermissive, polroles, polqual, polwithcheck as `varlena`); `PolicyRelationId = Oid(3256)`; no seed/bootstrap row, no index OIDs wired (3257/3258 only in comments) |
| `src/nodes/parsenodes.rs` | `CreatePolicyStmt`/`AlterPolicyStmt` structs exist (lines 1807-1827); `AlterTableType::EnableRowSecurity/DisableRowSecurity/ForceRowSecurity/NoForceRowSecurity` variants exist (lines 1392-1395, currently unhandled by any ALTER TABLE dispatcher); `Query.hasRowSecurity`/`withCheckOptions`, `RangeTblEntry.securityQuals`, `WCOKind`, `WithCheckOption` all already modeled (lines 122/157/728/763-777) |
| `src/catalog/pg_class.rs` | `relrowsecurity`/`relforcerowsecurity` bool fields exist (lines 40-41), unused by any code path |
| `src/utils/acl.rs` | `has_bypassrls_privilege(_roleid) -> unimplemented!()` (line 457); `has_privs_of_role() -> unimplemented!()` (line 246); `ACL_ID_PUBLIC = Oid(0)` constant exists |
| `src/miscadmin.rs` | `InNoForceRLSOperation()` stub exists (line 528) |
| `src/utils/rel.rs` | `RelationData.rd_rsdesc: Option<()>` -- explicit placeholder comment: "Row security policies. Unused in this port (the target `RowSecurityDesc` ...)" -- needs retyping to `Option<RowSecurityDesc>` |
| `src/backend/rewrite/rewriteHandler.rs` | `fire_rir_rules` walks `parsetree.rtable` and explicitly falls through `_ => not_yet_reachable("... view / RLS expansion")` for any RTEKind not RELATION/SUBQUERY/RESULT/CTE; **the dedicated second RLS loop (PG's post-loop over `rtable` calling `get_row_security_policies`) does not exist at all yet** -- must be added as a new pass in `query_rewrite`/`fire_rir_rules`, matching PG's placement (after the per-RTE walk, in `fireRIRrules`, right before the `cteList` recursion) |
| `src/nodes/parsenodes.rs` `ObjectType` enum | no `Policy` variant found -- needed for `DROP POLICY`/comment/security-label generic object addressing |
| `src/backend/utils/misc/guc_tables.rs` | no `"row_security"` entry -- the GUC is not registered, so `SET row_security = on/off` and per-session bypass won't parse |
| Executor (`src/backend/executor/`) | no `WithCheckOption`/`WCOKind` handling found -- `ExecWithCheckOptions` enforcement is unimplemented (needed for INSERT/UPDATE policy violations to raise errors) |
| grammar (`src/backend/parser/gram.lalrpop`) | **no** `POLICY`, `CreatePolicyStmt`, `AlterPolicyStmt`, `ROW LEVEL SECURITY`, `CURRENT_USER`, or `ROLE` productions found at all |

Net: the type/struct skeleton for RLS is unusually complete for a
not-yet-started feature (this is a side effect of plan 001/002's exhaustive
header translation), but **zero behavior** exists: no grammar, no catalog
bootstrap, no policy CRUD, no qual injection, no enforcement, and the
role/current_user grammar + GUC registration this feature depends on are
also missing.

## 3. Tests unblocked

Cross-referenced against `ai/tmp/regress-analysis/` (37 batch files, none for
`rowsecurity`/`privileges`/`collate` -- those three were apparently
triaged directly into `file-list/23.txt` without an individual batch writeup)
and `ai/plans/004-pg-regress-conformance/file-list/23.txt`:

| Test | sql/expected lines | Blocked by | Notes |
|---|---|---|---|
| `rowsecurity` | 2434 / 4878 | RLS (this doc) + roles (step 18) + views (M11, DONE) + FK/PK constraint interaction | The single largest regression test file in the suite by expected-output size (4878 lines). Exercises: basic USING/WITH CHECK, PERMISSIVE vs RESTRICTIVE combination and error-policy-naming order, `FORCE ROW LEVEL SECURITY`, BYPASSRLS, views over RLS tables, FK/PK cross-table policy interaction, `INSERT ... ON CONFLICT DO UPDATE`, `MERGE`, `COPY`, materialized views, partitioned tables, `EXPLAIN` plan shape with RLS quals, `pg_policies` catalog view, `ALTER POLICY`/`DROP POLICY`/rename, dependency-driven auto-drop of policies referencing dropped columns/roles. Roughly a full-feature acceptance test, not a narrow unit -- expect it to land LAST after all steps below, and possibly still need KNOWN-DIFF snapshotting on partition/materialized-view sections that hit unrelated deferred subsystems (partitioning is *also* deferred, per `file-list/23.txt` line 6). |
| `privileges` (partial) | 2113 / 3446 | roles (step 18, in-scope) + RLS only for a sub-slice | `file-list/23.txt` line 19 lists `privileges, rowsecurity, security_label` together under "full role-privilege ENFORCEMENT" -- but `privileges` is mostly GRANT/REVOKE on non-table objects (functions, schemas, sequences, types, large objects) and does NOT require RLS as a whole-file gate. Only a minority of its lines touch `row_security`/policies (grep shows 0 direct `CREATE POLICY` references in `privileges.sql`, i.e. RLS is NOT actually a blocker for the bulk of `privileges`; the file-list line grouping the two is a coarse register note, not a precise dependency). Treat `privileges` as primarily a step-18 (roles) beneficiary; do not gate it on this RLS work. |
| `collate*` (5 files) | -- | **not related to RLS** -- collation is a separate, unrelated deferred cluster (locale/ICU); it appears in `ai/tmp/regress-analysis/batch-collate.windows.win1252.md` only because that batch grouping is alphabetically/thematically adjacent, not because of a real RLS dependency. **Excluded from this breakdown.** |

Count: **1 test (`rowsecurity`) is truly gated on this feature**; `privileges`
is gated on roles, not RLS, and should not be counted against this work.

## 4. Architecture notes

- **Role/current_user dependency**: `GetUserId()`, `SetUserIdAndSecContext`,
  session-user tracking are ALREADY implemented (`src/miscadmin.rs`,
  `src/backend/utils/init/usercontext.rs`, `src/backend/utils/init/postinit.rs`)
  from the foundation work -- this is a real head start. What's missing is
  the SQL-visible surface: `CREATE ROLE`/`CREATE USER` (parses to nothing
  today), `SET ROLE`/`SET SESSION AUTHORIZATION` grammar, `CURRENT_USER`/
  `SESSION_USER` as SQL keywords, and `pg_authid.rolbypassrls` actually
  populated by DDL. All of this is plan-004 step 18's job, not this RLS
  work's job -- **RLS's role step must be sequenced strictly after step 18
  lands**, and should reuse step 18's `pg_authid`/`has_privs_of_role`/
  `SET ROLE` machinery rather than re-deriving it.
- **Qual injection hook point**: exactly one new call site, in
  `src/backend/rewrite/rewriteHandler.rs`. PG puts it in `fireRIRrules`
  (rewriteHandler.c:2230-2335) as a *second* pass over `parsetree.rtable`,
  executed after the per-RTE walk that expands views/rules (the same
  function PepperDB already has, `fire_rir_rules`). This ordering matters:
  RLS quals must see the RTE list post-view-expansion (a view over an
  RLS table gets the view's own RLS applied when the view's underlying
  table RTE is walked, not the view's RTE, since views aren't RTE_RELATION
  by the time this loop runs -- they've become RTE_SUBQUERY). Add the pass
  as a new loop at the end of `fire_rir_rules`, gated the same way PG gates
  it (only `RTEKind::RELATION` with `relkind` RELATION/PARTITIONED_TABLE).
- **USING vs WITH CHECK**: USING filters existing rows (SELECT/UPDATE/DELETE
  visibility, becomes an RTE `securityQuals` entry, silently drops
  non-matching rows -- no error). WITH CHECK validates rows being written
  (INSERT/UPDATE, becomes a `Query.withCheckOptions` entry, raises
  `ERRCODE_WITH_CHECK_OPTION_VIOLATION` on violation -- never silently
  drops). A policy lacking an explicit WITH CHECK clause reuses its USING
  qual for the check (`QUAL_FOR_WCO` macro in rowsecurity.c). This means the
  WCO *enforcement* mechanism (execMain.c's `ExecWithCheckOptions`, walking
  `estate->es_result_relation_info`'s WCO list once per proposed tuple) must
  exist in the executor before RLS's WITH CHECK half is meaningful --
  currently absent, confirmed by grep. This mechanism is also needed for
  regular (non-RLS) view WITH CHECK OPTION, so it may already be scheduled
  independently under M11/views; check for overlap before scoping a step to
  "add it."
- **FORCE / permissive / restrictive**: `relforcerowsecurity` only matters
  for the table owner (non-owners always get RLS applied if
  `relrowsecurity` is set); `check_enable_rls`'s `RLS_NONE_ENV` vs
  `RLS_ENABLED` distinction exists so the planner knows to mark the query
  `hasRowSecurity=true` (forcing replan on `SET ROLE`) even when the
  *current* role happens to bypass RLS -- get this branch right or superuser/
  owner sessions will get stale cached plans after a role change.
  Permissive policies for a given command combine with OR (any one grants
  access); restrictive policies combine with AND (all must pass); if zero
  permissive policies exist for a command, the default is deny-all (a
  literal `false` qual/WCO), not "no restriction." Restrictive policies are
  sorted by name for deterministic multi-policy violation-error ordering
  (`rowsecurity.sql` line ~100 explicitly tests this ordering).
- **Bootstrap/catalog gap**: `pg_policy`'s two indexes (`pg_policy_oid_index`
  OID 3257, `pg_policy_polrelid_polname_index` OID 3258) are referenced only
  in comments in `src/catalog/pg_policy.rs` -- they need real seed rows in
  whatever catalog-bootstrap mechanism plan 003/004 uses (the plan's
  "bootstrap = build.rs-seed-rows + Rust-initdb" gating decision), same
  pattern as other catalog tables already seeded.

## 5. Step breakdown

Numbered as a candidate insertion after existing step 18 (roles); renumber
when actually splicing into `file-list/`. Each step assumes the previous
steps in this list are done, plus (for step R1 onward) plan-004 step 18.

### Step R1 -- GUC + grammar: `row_security`, POLICY keyword, CreatePolicyStmt/AlterPolicyStmt, ALTER TABLE ENABLE/FORCE ROW LEVEL SECURITY, DROP POLICY
- **PG files**: `gram.y` lines ~5885-5983 (`CreatePolicyStmt`/`AlterPolicyStmt`/
  `RowSecurityDefaultForCmd`/`RowSecurityOptionalToRole`/etc.), ~2968-2999
  (`AT_EnableRowSecurity` family), ~9506/9517 (`ALTER POLICY ... RENAME`),
  ~7094/17891/18519 (`POLICY` keyword -> `OBJECT_POLICY`); `guc_tables.c:1696`
  (`row_security` GUC entry, `PGC_USERSET`, default true).
- **Our target files**: `src/backend/parser/gram.lalrpop` (grow: add
  `CreatePolicyStmt`, `AlterPolicyStmt`, `DropStmt` POLICY arm, `AlterTableCmd`
  arms for the 4 `AT_*RowSecurity` variants -- the enum variants already exist
  in `parsenodes.rs`, only the grammar production is missing, plus generic
  `RENAME POLICY`); `src/backend/utils/misc/guc_tables.rs` (grow: register
  `row_security`); `src/utils/rls.rs` (wire the static to the GUC registry
  instead of a bare `static mut`, following whatever pattern other GUC-backed
  session bools in this port use).
- **Deliverable**: `CREATE POLICY`, `ALTER POLICY`, `DROP POLICY`,
  `ALTER TABLE t ENABLE/DISABLE/FORCE/NO FORCE ROW LEVEL SECURITY`, and
  `SET row_security = on|off` all parse to the correct AST/GUC-set call
  (semantic actions can be `unimplemented!()` past parsing -- this step is
  grammar-only, matching plan 004's "grow gram.lalrpop" pattern used in step
  18).
- **Deps**: none beyond current tree (does not need step 18, since POLICY
  syntax and the GUC are independent of role syntax; role NAMES used inside
  `TO regress_rls_bob` resolve later at execution, not parse time).
- **Tests unblocked**: none standalone (rowsecurity.sql needs the full
  chain); this step is infrastructure.
- **Effort**: S.

### Step R2 -- pg_policy catalog: bootstrap, seed, indexes, FormData wiring
- **PG files**: `include/catalog/pg_policy.h` (58 lines, already fully read
  across in `src/catalog/pg_policy.rs`).
- **Our target files**: `src/catalog/pg_policy.rs` (grow: wire
  `PolicyOidIndexId`/`PolicyPolrelidPolnameIndexId` as real constants, not
  comments); catalog bootstrap seed list (wherever plan 003/004's
  `build.rs`-seed-rows + Rust-initdb mechanism enumerates system catalogs --
  add `pg_policy` + its 2 indexes + its TOAST table to that list, following
  the exact pattern used for the most recently added catalog, e.g.
  `pg_authid` from step 18).
  - Verify: does `pg_authid`'s step-18 seeding actually complete a
    reusable "add one more system catalog" recipe? If step 18 hasn't landed
    yet, use whichever catalog was seeded most recently as the template.
- **Deliverable**: `pg_policy` exists as a real, empty, indexed system
  catalog reachable via the normal catalog-scan path (`systable_beginscan`
  equivalent) at server startup.
- **Deps**: step 18 (roles) must exist first for `polroles`'s
  `BKI_LOOKUP_OPT(pg_authid)` FK-like relationship to resolve meaningfully,
  though the catalog can technically be created without it -- sequence after
  step 18 to avoid rework.
- **Tests unblocked**: none standalone.
- **Effort**: S.

### Step R3 -- RelationBuildRowSecurity + relcache integration
- **PG files**: `commands/policy.c` lines 192-322 (`RelationBuildRowSecurity`).
- **Our target files**: `src/commands/policy.rs` (implement
  `RelationBuildRowSecurity`); `src/utils/rel.rs` (retype
  `RelationData.rd_rsdesc` from `Option<()>` to `Option<RowSecurityDesc>`,
  and wire it into whatever relcache-build/invalidation path already exists
  for other `rd_*` descriptors, e.g. `rd_att`/`rd_index`).
- **Deliverable**: opening a relation with `relrowsecurity = true` populates
  `rd_rsdesc` with the table's policies scanned from `pg_policy`, ordered by
  `(polrelid, polname)`.
- **Deps**: R2 (catalog exists), relcache invalidation plumbing (verify it
  already exists generically -- this port has `CacheInvalidateRelcache`-style
  hooks per the plan-002 sinval work; if so this step only adds the RLS
  build function, not new invalidation infrastructure).
- **Tests unblocked**: none standalone.
- **Effort**: M (the memory-context dance in the C version can likely
  collapse to a plain `Vec<RowSecurityPolicy>` owned by `RelationData` in
  Rust -- no need to replicate `rscxt`/`MemoryContextAllocZero`; note this
  simplification explicitly in the step so the agent doesn't over-translate).

### Step R4 -- CreatePolicy / AlterPolicy / rename_policy / RemovePolicyById / RemoveRoleFromObjectPolicy / relation_has_policies / get_relation_policy_oid
- **PG files**: `commands/policy.c` in full (1279 lines: this is the bulk of
  the step) -- `RangeVarCallbackForPolicy`, `parse_policy_command`,
  `policy_role_list_to_array`, `CreatePolicy`, `AlterPolicy`, `rename_policy`,
  `get_relation_policy_oid`, `relation_has_policies`, `RemovePolicyById`,
  `RemoveRoleFromObjectPolicy`.
- **Our target files**: `src/commands/policy.rs` (implement all 8 functions,
  replacing every `unimplemented!()`); `src/nodes/parsenodes.rs`
  (`ObjectType` enum: add `Policy` variant, needed for `DROP POLICY`'s
  generic-object-drop path and dependency recording); `src/catalog/
  objectaddress.rs` (grow: handle `ObjectType::Policy` in
  `get_object_type`/address formatting, following whatever pattern nearby
  object types use).
- **Deliverable**: `CREATE POLICY p ON t USING (...) WITH CHECK (...)`,
  `ALTER POLICY`, `DROP POLICY`, `ALTER POLICY ... RENAME TO` all execute
  end-to-end against `pg_policy`; ownership check (`object_ownercheck`),
  system-table guard, and cmd/qual validation (SELECT/DELETE forbid WITH
  CHECK, INSERT forbids USING) all enforced with the exact PG error codes
  (`ERRCODE_SYNTAX_ERROR`, `ERRCODE_DUPLICATE_OBJECT`,
  `ERRCODE_UNDEFINED_OBJECT`).
- **Deps**: R1 (grammar), R2 (catalog), step 18 (`has_privs_of_role`,
  `object_ownercheck` presumably already needed by GRANT/REVOKE from plan
  003 -- verify reuse rather than reimplementing), the expression-parsing
  path used for `transformWhereClause`/`EXPR_KIND_POLICY` (check the
  rewriter/analyzer's existing `ParseState` + range-table-entry-for-relation
  machinery from plan 003's WHERE-clause parsing -- this is the same
  mechanism used elsewhere, not new).
- **Tests unblocked**: none standalone (needs R5 for actual enforcement to
  be observable via SELECT/INSERT).
- **Effort**: L (biggest single-file port in this breakdown: 1279 lines,
  though `policy_role_list_to_array`/`RemoveRoleFromObjectPolicy`'s
  duplicate-role handling can be simplified given Rust's `Vec`/dedup
  idioms -- flag as a simplification opportunity, not a literal transliteration
  target, per rules.md s10).

### Step R5 -- check_enable_rls + has_bypassrls_privilege + InNoForceRLSOperation wiring
- **PG files**: `utils/misc/rls.c` in full (168 lines: `check_enable_rls`,
  `row_security_active`/`row_security_active_name` SQL-callable wrappers).
- **Our target files**: `src/utils/rls.rs` (implement `check_enable_rls`,
  replacing the `CheckEnableRlsResult` enum's `unimplemented!()` producer);
  `src/utils/acl.rs` (implement `has_bypassrls_privilege` by reading
  `pg_authid.rolbypassrls`, and `has_privs_of_role` if not already done by
  step 18's GRANT/REVOKE role-membership work -- check for overlap first);
  `src/miscadmin.rs` (implement `InNoForceRLSOperation`, a simple
  session-local nesting counter set around RI/FK trigger execution --
  find where FK constraint checks currently execute their scan and bracket
  it, or stub the bracket-setting call sites as a follow-up if FK triggers
  aren't yet running real scans).
- **Deliverable**: `check_enable_rls(relid, checkAsUser, noError)` returns
  the correct one of `RLS_NONE`/`RLS_NONE_ENV`/`RLS_ENABLED` for: no RLS on
  table, BYPASSRLS role, table owner without FORCE, table owner with FORCE,
  non-owner with RLS enabled and GUC on, non-owner with RLS enabled and GUC
  off (raises `ERRCODE_INSUFFICIENT_PRIVILEGE` unless `noError`).
- **Deps**: R2/R3 (needs `relrowsecurity`/`relforcerowsecurity` readable from
  a real catalog row, not just the struct field), step 18
  (`pg_authid.rolbypassrls` populated by `CREATE ROLE ... BYPASSRLS`).
- **Tests unblocked**: none standalone.
- **Effort**: S.

### Step R6 -- get_row_security_policies: qual + WCO injection, wired into the rewriter
- **PG files**: `rewrite/rowsecurity.c` in full (932 lines: this is the
  semantic core) -- `get_row_security_policies`, `get_policies_for_relation`,
  `sort_policies_by_name`/`row_security_policy_cmp`, `add_security_quals`,
  `add_with_check_options`, `check_role_for_policy`; plus the call site,
  `rewrite/rewriteHandler.c` lines 2230-2335 (the second RTE loop inside
  `fireRIRrules`, including the sublink-recursion/infinite-recursion-guard
  block at lines 2257-2310).
- **Our target files**: `src/rewrite/rowsecurity.rs` (implement
  `get_row_security_policies` and its private helpers -- can be free
  functions in this file or a sibling module, following whatever
  `grow`/`type-centric` convention rules.md s8 prescribes for this kind of
  multi-helper C file); `src/backend/rewrite/rewriteHandler.rs` (grow:
  add the new post-loop pass to `fire_rir_rules`, calling
  `get_row_security_policies` per `RTEKind::RELATION` RTE and splicing
  results into `rte.securityQuals`/`parsetree.withCheckOptions`/
  `parsetree.hasRowSecurity`, replacing the current
  `_ => not_yet_reachable("... RLS expansion")` fallthrough for this case).
- **Deliverable**: a `SELECT`/`UPDATE`/`DELETE`/`INSERT` against an
  RLS-enabled table gets the correct USING quals appended to its RTE and
  WITH CHECK options appended to the query, exactly matching PG's per-
  command-type logic (including the SELECT-needs-UPDATE-quals-for-FOR-UPDATE
  case, and the UPDATE/DELETE-needs-SELECT-quals-for-RETURNING case). MERGE
  and `INSERT ... ON CONFLICT DO UPDATE` variants can be deferred to a
  follow-up sub-step if MERGE/ON CONFLICT aren't yet supported elsewhere in
  the port -- check current MERGE/ON CONFLICT status before committing to
  full parity here.
- **Deps**: R3 (rd_rsdesc populated), R5 (check_enable_rls real),
  `RTEPermissionInfo.checkAsUser`/`requiredPerms` already exist in
  `parsenodes.rs` (confirmed present) -- verify they're actually populated
  by the current permission-checking path (plan 003's ACL work) before
  relying on them here.
- **Tests unblocked**: none standalone (qual injection without executor
  enforcement is inert for WITH CHECK; USING quals alone are visible via
  SELECT filtering, which may make a narrow slice of `rowsecurity.sql`'s
  early SELECT-only assertions pass, but the file as a whole needs R7 too).
- **Effort**: L (932 lines, non-trivial control flow across 6+ command-type
  branches).

### Step R7 -- WithCheckOption enforcement in the executor
- **PG files**: `executor/execMain.c` (`ExecWithCheckOptions` and call
  sites in `ExecInsert`/`ExecUpdate`) -- locate exact line range at
  implementation time; not yet located in this research pass since it's
  outside the rowsecurity.c/policy.c pair, but it's the standard PG
  `WCOKind`-driven per-tuple qual eval loop, raising
  `ERRCODE_WITH_CHECK_OPTION_VIOLATION` with the relation/policy name from
  `wco->relname`/`wco->polname` on failure.
- **Our target files**: `src/backend/executor/` (wherever `ExecInsert`/
  `ExecUpdate` equivalents live in this port -- locate via the plan 003
  executor module map before starting); may already be partially needed for
  plain (non-RLS) view `WITH CHECK OPTION` -- **check for scheduling overlap
  with any existing/planned executor step before treating this as pure net-
  new RLS work**.
- **Deliverable**: an INSERT/UPDATE whose new row fails a WCO qual raises
  the correct SQLSTATE and error text (including policy name when a named
  restrictive policy fails).
- **Deps**: R6 (WCOs actually populated on the Query).
- **Tests unblocked**: `rowsecurity` moves from "quals visible but checks
  unenforced" to "fully functional" for the non-partition/non-materialized-
  view/non-FK slice of the test.
- **Effort**: M (assuming the executor's row-processing loop already exists
  for plain INSERT/UPDATE from plan 003 DML work -- this step only adds one
  more per-row check, not new row-processing infrastructure).

### Step R8 -- rowsecurity.sql conformance pass (harness integration)
- **PG files**: `test/regress/sql/rowsecurity.sql` (2434 lines) /
  `expected/rowsecurity.out` (4878 lines) -- test-only, no new C to port.
- **Our target files**: `ai/harness/regress/pepper_schedule` (add
  `rowsecurity`); `ai/harness/regress/known_diffs/rowsecurity.out` (snapshot
  any sections gated on partitioning/materialized-views/MERGE if those
  remain unimplemented -- both `partitioning` and `PL/pgSQL` are separately
  listed as deferred XL subsystems in `file-list/23.txt`, so expect this
  test to need real KNOWN-DIFF carve-outs rather than a clean full pass even
  after R1-R7 land).
- **Deliverable**: `rowsecurity` reaches PASS or a documented, narrow
  KNOWN-DIFF via `ai/harness/regress/run.sh`.
- **Deps**: R1-R7, plus whatever partitioning/materialized-view/MERGE
  support exists at the time (may require carving those specific test
  sections into KNOWN-DIFF rather than blocking the whole step on them --
  follow plan 004's "Known-diff discipline" rule).
- **Tests unblocked**: `rowsecurity` (the 1 test this whole feature exists
  to unblock).
- **Effort**: M (mostly diff-driven fixups against real `psql` output, per
  plan 004's standard per-step workflow).

### Summary table

| Step | Title | Effort | Deps | Unblocks |
|---|---|---|---|---|
| R1 | GUC + grammar (POLICY, ROW LEVEL SECURITY, GUC) | S | step 18 not required | infra |
| R2 | pg_policy catalog bootstrap | S | step 18 (sequencing) | infra |
| R3 | RelationBuildRowSecurity + relcache | M | R2 | infra |
| R4 | CreatePolicy/AlterPolicy/rename/remove | L | R1, R2, step 18 | infra |
| R5 | check_enable_rls + bypassrls | S | R2/R3, step 18 | infra |
| R6 | get_row_security_policies qual/WCO injection | L | R3, R5 | infra (partial SELECT filtering) |
| R7 | WithCheckOption executor enforcement | M | R6 | infra (full INSERT/UPDATE enforcement) |
| R8 | rowsecurity.sql conformance | M | R1-R7 | **rowsecurity** |

Total: ~8 steps, roughly 2 L + 2 M(+1M) + 3 S in weight, gating exactly one
regression test (`rowsecurity`) but a foundational one for any future
multi-tenant/security work beyond pg_regress conformance. All steps require
plan-004 step 18 (roles) to have landed first; R1/R2 can technically start
in parallel with step 18 (grammar/catalog shape doesn't need real roles) but
R4 onward is a hard sequential dependency.

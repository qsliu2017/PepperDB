# Event triggers - step breakdown for plan 004

Status: currently a one-line register entry in
`ai/plans/004-pg-regress-conformance/file-list/23.txt` ("event triggers ... #
event_trigger*"). This doc expands that line into implementable steps.

## 1. PG source anatomy

| File | Lines | Role |
|---|---|---|
| `ref/postgres/src/backend/commands/event_trigger.c` | 2412 | all EVENT TRIGGER logic |
| `ref/postgres/src/include/commands/event_trigger.h` | 97 | `EventTriggerData`, `AT_REWRITE_*`, all `EventTrigger*` prototypes |
| `ref/postgres/src/include/catalog/pg_event_trigger.h` | 60 | catalog row (`FormData_pg_event_trigger`), 2 indexes, 2 syscaches |
| `ref/postgres/src/include/tcop/deparse_utility.h` | 108 | `CollectedCommand` (feeds `pg_event_trigger_ddl_commands()`) |
| `ref/postgres/src/backend/tcop/utility.c` | 3770 (37 event-trigger call sites) | `standard_ProcessUtility`/`ProcessUtilitySlow` fire points |
| `ref/postgres/src/backend/tcop/postgres.c` (1 call site, ~line 4373) | -- | `EventTriggerOnLogin()` call in `PostgresMain` |
| `ref/postgres/src/backend/utils/init/postinit.c` (~line 1099) | -- | sets `MyDatabaseHasLoginEventTriggers` from `pg_database.dathasloginevt` |
| `ref/postgres/src/test/regress/sql/event_trigger.sql` | 640 | main test |
| `ref/postgres/src/test/regress/expected/event_trigger.out` | 819 | |
| `ref/postgres/src/test/regress/sql/event_trigger_login.sql` | 24 | login-event test |
| `ref/postgres/src/test/regress/expected/event_trigger_login.out` | 39 | |

`event_trigger.c` function inventory (37 top-level functions/statics): `CreateEventTrigger`, `get_event_trigger_oid`, `AlterEventTrigger`, `AlterEventTriggerOwner[_internal/_oid]`, `insert_event_trigger_tuple`, `filter_list_to_array`, `validate_ddl_tags`, `validate_table_rewrite_tags`, `error_duplicate_filter_variable`, `EventTriggerSupportsObjectType`, `EventTriggerSupportsObject`, `EventTriggerCommonSetup` (implicit, inlined in the three below), `EventTriggerDDLCommandStart`, `EventTriggerDDLCommandEnd`, `EventTriggerSQLDrop`, `EventTriggerTableRewrite`, `EventTriggerOnLogin`, `EventTriggerInvoke` (the fmgr call), `EventTriggerBeginCompleteQuery`/`EndCompleteQuery`, `trackDroppedObjectsNeeded`, `EventTriggerSQLDropAddObject`, `EventTriggerInhibitCommandCollection`/`UndoInhibitCommandCollection`, `EventTriggerCollectSimpleCommand`, `EventTriggerAlterTableStart`/`AlterTableRelid`/`CollectAlterTableSubcmd`/`AlterTableEnd`, `EventTriggerCollectGrant`, `EventTriggerCollectAlterOpFam`, `EventTriggerCollectCreateOpClass`, `EventTriggerCollectAlterTSConfig`, `EventTriggerCollectAlterDefPrivs`, plus the 4 SQL-callable support functions `pg_event_trigger_dropped_objects`, `pg_event_trigger_table_rewrite_oid`, `pg_event_trigger_table_rewrite_reason`, `pg_event_trigger_ddl_commands`, and 2 stringify helpers.

Firing points wired into `standard_ProcessUtility`/`ProcessUtilitySlow` (utility.c, 37 call sites): `ddl_command_start` (once, near top, before dispatch), per-statement `EventTriggerCollectSimpleCommand`/`EventTriggerAlterTableStart/End` (interspersed through nearly every DDL arm), `EventTriggerInhibitCommandCollection` (around DefineOpFamily), `EventTriggerCollectAlterDefPrivs`, then at the end: `EventTriggerSQLDrop` + `ddl_command_end`, wrapped by `EventTriggerBeginCompleteQuery`/`EndCompleteQuery`.

## 2. PepperDB current state (grep-verified)

**Header-stub layer exists, zero logic implemented -- every function body is `unimplemented!()`:**

- `src/commands/event_trigger.rs` (167 lines) -- all 22 `EventTrigger*`/`CreateEventTrigger`/`AlterEventTrigger*` functions are `unimplemented!()`. `EventTriggerData<'a>` struct and `AtRewrite` bitflags (`AT_REWRITE_*`) are correctly translated as types.
- `src/catalog/pg_event_trigger.rs` (28 lines) -- `FormData_pg_event_trigger` struct translated (with `#[derive(pepperdb_derive::Catalog)]`), `EventTriggerRelationId = Oid(3466)` constant present. No index/syscache registration comments resolved (DECLARE_TOAST/DECLARE_UNIQUE_INDEX/MAKE_SYSCACHE left as comments, i.e. not yet wired into the real catalog bootstrap).
- `src/utils/evtcache.rs` (27 lines) -- `EventTriggerEvent` enum (5 variants matching PG's) and `EventTriggerCacheItem` struct translated; `EventCacheLookup()` is `unimplemented!()`. No actual cache (no `OnceLock`/registry, no invalidation hookup).
- `src/tcop/deparse_utility.rs` -- `CollectedCommand`/`CollectedCommandType`/`CollectedCommandData` header-translated (types exist), consumer (`pg_event_trigger_ddl_commands`) not implemented.

**Never called / never wired:**

- `standard_process_utility` / `process_utility_slow` in `src/backend/tcop/utility.rs` (the REAL, working DDL dispatcher, 566 lines) has **zero** event-trigger call sites -- no `EventTriggerDDLCommandStart/End`, no `EventTriggerCollectSimpleCommand`, no `EventTriggerSQLDrop`, no `EventTriggerAlterTableStart/End`. This is the actual gap: even if `commands/event_trigger.rs` were implemented, nothing calls it.
- `src/tcop/utility.rs`'s own `ProcessUtility`/`standard_ProcessUtility` (the header-translated, unused twin) are also bare `unimplemented!()` -- confirms two parallel utility.rs files exist (`src/tcop/utility.rs` = untranslated header stub, `src/backend/tcop/utility.rs` = the real working dispatcher per plan-002's file-mapping invariant); only the latter matters.
- `EventTriggerOnLogin()` is never called anywhere in `src/backend/tcop/postgres.rs`'s `PostgresMain`-equivalent (PG calls it once, right before the main loop, ~postgres.c:4373). No call site exists in PepperDB at all.
- `MyDatabaseHasLoginEventTriggers` IS plumbed as a real field: `Session::database_has_login_event_triggers: AtomicBool` in `src/session.rs`, with an accessor `MyDatabaseHasLoginEventTriggers()` in `src/miscadmin.rs` (marked `#[deprecated]`, points at `Session` accessor). But nothing ever sets it true -- `dathasloginevt` is hardcoded `BoolGetDatum(false)` in both `src/backend/bootstrap/bootstrap.rs:1317` and `src/backend/commands/dbcommands.rs:74`. PG's `postinit.c` sets this from `pg_database.dathasloginevt` at connect-to-database time; that plumbing is absent in PepperDB's connect path.
- **Grammar: `CREATE EVENT TRIGGER` / `ALTER EVENT TRIGGER` cannot be parsed at all.** `src/backend/parser/gram.lalrpop` has zero productions referencing `CreateEventTrigStmt`/`AlterEventTrigStmt` (confirmed by grep - no hits), unlike `CreateTrigStmt` which has a full production (`gram.lalrpop:786-792`) for the (structurally similar but simpler) `CREATE TRIGGER`. The `"event"` keyword is already in `src/parser/kwlist.rs:162` as `UNRESERVED_KEYWORD`, and `"TRIGGER"` token already exists (from CREATE TRIGGER, M11 step 41) -- both reusable.
- **Parse nodes ARE translated** (header layer): `CreateEventTrigStmt { trigname, eventname, whenclause, funcname }` and `AlterEventTrigStmt { trigname, tgenabled }` exist in `src/nodes/parsenodes.rs:1859-1874`, ready for a grammar action to construct.
- **`event_trigger` pseudo-type**: PG declares it in `pg_type.dat` (`typinput => 'event_trigger_in'` etc., a function-only marker type like `trigger`). Grep of `src/catalog/pg_type.rs` and `src/backend/catalog/pg_type.rs` found **no** `event_trigger` entry at all -- the pseudo-type is entirely missing from PepperDB's bootstrap catalog data. This blocks `CREATE FUNCTION ... RETURNS event_trigger`. (Side note: `functioncmds.rs` also has no `RETURNS trigger`/pseudo-type-return validation yet at all, per grep -- so this check may not need to be enforced strictly to get tests running, but the type OID must still resolve for `RETURNS event_trigger` to parse/bind.)
- `EventTriggerInvoke` (the fmgr call that actually invokes the trigger function with an `EventTriggerData` fmgr context) has no PepperDB equivalent -- this needs the function-call/fmgr path to accept a "context" argument analogous to how regular triggers pass `TriggerData`; check how `src/commands/trigger.rs`'s (M11) trigger-firing does this since it's the nearest analog (`FunctionCallInvoke`-with-context pattern), if that exists.

## 3. Tests unblocked

From PG's regress suite (cross-referenced against `ai/tmp/regress-analysis/`, which does not yet have a `batch-event_trigger.md` -- neither `event_trigger` nor `event_trigger_login` appears in any existing batch file's blocking-module list, and the closest related file, `batch-triggers.md`, marks the (row/statement, non-event) `triggers` test `OUT_OF_SCOPE` due to missing PL/pgSQL body execution):

| Test | Lines (sql/expected) | What it needs | PL execution needed? |
|---|---|---|---|
| `event_trigger` | 640/819 | `CREATE EVENT TRIGGER`/`ALTER EVENT TRIGGER`/`DROP EVENT TRIGGER`, `COMMENT ON EVENT TRIGGER`, `pg_event_trigger` catalog + syscaches, WHEN/tag filter validation, enable/disable + `session_replication_role` interaction, `ddl_command_start`/`ddl_command_end`/`sql_drop`/`table_rewrite` firing, `pg_event_trigger_ddl_commands()`/`_dropped_objects()`/`_table_rewrite_oid()`/`_table_rewrite_reason()`, non-top-level firing (nested inside a function call) | YES for the actual trigger function bodies (`test_event_trigger()` is `LANGUAGE plpgsql`, and one variant is `LANGUAGE sql` which must be rejected) -- but the framework/catalog/DDL-parsing/firing-dispatch machinery, the negative-test error paths (bad WHEN, bad tag, args-not-allowed, non-superuser DROP, global-object rejection), and enable/disable state can all be exercised and largely validated even with a no-op/stub function body, IF the fmgr call path can invoke *some* function marked `RETURNS event_trigger` (even a placeholder Rust-native one) and receive an `EventTriggerData` context. Full parity with the `.out` file (NOTICE text, `user_logins`-style side effects) needs real PL/pgSQL. |
| `event_trigger_login` | 24/39 | `CREATE EVENT TRIGGER ... ON login`, `ALTER EVENT TRIGGER ... ENABLE ALWAYS`, `dathasloginevt` catalog flag flip + persistence, `\c` reconnect firing `EventTriggerOnLogin()`, INSERT-from-trigger-body side effect visible after reconnect | YES -- the login trigger's entire observable behavior (`INSERT INTO user_logins`) is a PL/pgSQL body; without PL execution this test cannot produce matching output, though the framework parts (catalog flag set/read, firing call site wired into the connection path, ENABLE ALWAYS semantics under `session_replication_role`) are independently verifiable. |

**Count: 2 tests** (`event_trigger`, `event_trigger_login`). Neither currently appears in any `pepper_schedule`/file-list step 01-22; both are covered only by the one-line register in `file-list/23.txt`.

Note the ai/tmp/regress-analysis/ batch files don't have a dedicated event_trigger entry (it wasn't part of the per-test static analysis batches run so far); this doc is the first structured analysis of it.

## 4. Step breakdown for plan 004

Proposed insertion point: a new step **"24" or "09b"-style slot** (numbering is the plan owner's call) that depends on the CREATE TRIGGER work (step "M11/41", already landed) and on whatever step lands minimal PL/pgSQL execution (plan-004's own numbering doesn't yet have a PL/pgSQL step; this doc treats it as an external dependency, "plan-004 PL/pgSQL"). Steps are ordered so framework+catalog work (A-C) can land and be reviewed **before** PL/pgSQL exists; step D (real firing test parity) is blocked on PL.

---

### Step A -- Catalog + grammar: CREATE/ALTER/DROP EVENT TRIGGER, pg_event_trigger

- **PG files**: `pg_event_trigger.h` (60 lines), `event_trigger.c` lines 123-390 (`CreateEventTrigger`, `get_event_trigger_oid`, `insert_event_trigger_tuple`, `filter_list_to_array`, `validate_ddl_tags`, `error_duplicate_filter_variable`), lines 426-538 (`AlterEventTrigger`, `AlterEventTriggerOwner[_internal/_oid]`), `gram.y`'s `CreateEventTrigStmt`/`AlterEventTrigStmt` productions (grep `ref/postgres/src/backend/parser/gram.y` for exact rule text), `pg_type.dat` `event_trigger` pseudo-type row.
- **Our target files (verify against current stub)**:
  - `src/catalog/pg_event_trigger.rs` -- wire real `DECLARE_UNIQUE_INDEX`/`MAKE_SYSCACHE` (currently commented out) into the real catalog/index/syscache bootstrap machinery (whatever mechanism landed the analogous `pg_trigger` catalog in M11/step 41 -- use as the pattern).
  - `src/commands/event_trigger.rs` -- implement `CreateEventTrigger`, `get_event_trigger_oid`, `AlterEventTrigger`, `AlterEventTriggerOwner[_oid]`, `EventTriggerSupportsObjectType`, `EventTriggerSupportsObject` (currently all `unimplemented!()`).
  - `src/backend/parser/gram.lalrpop` -- add `CreateEventTrigStmt`/`AlterEventTrigStmt`/`DropStmt`(EVENT TRIGGER kind)/`CommentStmt`(EVENT TRIGGER kind) productions, reusing the `"event"`/`"TRIGGER"` tokens already present; pattern off `CreateTrigStmt` at `gram.lalrpop:786-792` (simpler: no target relation, no row/statement spec, no args -- just trigname + ON eventname + optional WHEN filter list + EXECUTE FUNCTION funcname()).
  - `src/backend/tcop/utility.rs` -- add `Node::CreateEventTrigStmt`/`Node::AlterEventTrigStmt` arms to `standard_process_utility`'s match and to `create_command_tag`.
  - `src/backend/catalog/pg_type.rs` (or bootstrap seed data) -- add the `event_trigger` pseudo-type row (typinput/typoutput = `event_trigger_in`/`_out`, byval, len 4, category `P`), needed for `RETURNS event_trigger` to bind.
  - `src/commands/dropcmds.rs` (or wherever `RemoveObjects`/`DropStmt` object-kind dispatch lives) -- `OBJECT_EVENT_TRIGGER` drop support (uses the standard dependency-based drop path once the catalog row + OID class exist).
- **Deliverable**: `CREATE EVENT TRIGGER name ON event [WHEN tag IN (...)] EXECUTE FUNCTION funcname()`, `ALTER EVENT TRIGGER name {ENABLE|ENABLE REPLICA|ENABLE ALWAYS|DISABLE}`, `DROP EVENT TRIGGER`, `COMMENT ON EVENT TRIGGER` all parse and create/mutate/remove a real `pg_event_trigger` row; validation errors (bad WHEN, bad tag, args not allowed, event triggers on event triggers / global objects rejected, non-superuser rejected) match PG's error text. Nothing fires yet.
- **Deps**: plan-003 catalog/index/syscache infra (generic, already used by every other catalog); CREATE TRIGGER's grammar pattern (M11 step 41, landed) as a template; `ObjectType`/dependency/drop machinery (generic, already used).
- **Tests unblocked**: none fully green yet, but unblocks the DDL/negative-test portion (~first half) of `event_trigger.sql` to start matching.
- **Effort**: M (grammar + catalog wiring + validation logic; no firing, no fmgr call plumbing). Comparable to the CREATE TRIGGER step but with a smaller grammar surface and simpler catalog row.

---

### Step B -- Firing framework + ProcessUtility hooks (ddl_command_start/end, sql_drop, per-statement collection)

- **PG files**: `event_trigger.c` lines 578-1067 (`EventTriggerDDLCommandStart/End`, `EventTriggerSQLDrop`, `EventTriggerCommonSetup`-equivalent, `trackDroppedObjectsNeeded`, `EventTriggerSQLDropAddObject`), lines 1067-1439 (`EventTriggerBeginCompleteQuery`/`EndCompleteQuery`, `EventTriggerInhibitCommandCollection`/`Undo...`, `EventTriggerCollectSimpleCommand`, `EventTriggerAlterTable*`, `EventTriggerCollectGrant/AlterOpFam/CreateOpClass/AlterTSConfig/AlterDefPrivs`), `EventTriggerInvoke` (fmgr call, static, search for definition near line 1900-2050 region by function list above), `utils/evtcache.c`/`evtcache.h` (event cache lookup by tag -- confirm exact file location and size), `deparse_utility.h` (`CollectedCommand`).
- **Our target files (verify)**:
  - `src/utils/evtcache.rs` -- implement `EventCacheLookup` for real: build an in-memory cache of enabled event triggers filtered by event + tag (SESSION_REPLICATION_ROLE-aware), invalidated on `pg_event_trigger` changes (use PepperDB's existing syscache/relcache invalidation mechanism, per plan-002's sinval infra).
  - `src/commands/event_trigger.rs` -- implement `EventTriggerDDLCommandStart/End`, `EventTriggerSQLDrop`, `EventTriggerSQLDropAddObject`, `trackDroppedObjectsNeeded`, `EventTriggerInhibitCommandCollection`/`Undo...`, `EventTriggerCollectSimpleCommand`, `EventTriggerAlterTable{Start,Relid,CollectSubcmd,End}`, `EventTriggerCollectGrant/AlterOpFam/CreateOpClass/AlterTSConfig/AlterDefPrivs`, `EventTriggerBeginCompleteQuery`/`EndCompleteQuery`. Needs a per-query `EventTriggerQueryState` stack (currentEventTriggerState in C -- a task-local/Session-scoped `Vec`/stack in PepperDB, not global mutable state, per the Send-safety + task-local conventions already used elsewhere e.g. `Session`).
  - `src/tcop/deparse_utility.rs` -- flesh out `CollectedCommand` consumption for `pg_event_trigger_ddl_commands()` (step D dependency, but the collection side belongs here).
  - `src/backend/tcop/utility.rs` -- **the core gap**: add the ~15-20 call sites into `standard_process_utility`/`process_utility_slow` mirroring utility.c's 37 sites: `EventTriggerDDLCommandStart` near the top of `standard_process_utility`, `EventTriggerCollectSimpleCommand` after each DDL command's `ObjectAddress` is known (in `process_object_ddl`, `process_view_rule_stmt`, the `AlterTableStmt`/`RenameStmt`/`DropStmt` arms, `CreateTrigStmt`, etc.), `EventTriggerAlterTableStart/End` bracketing the `Node::AlterTableStmt` arm, `EventTriggerSQLDrop` + `EventTriggerDDLCommandEnd` at the end, all wrapped in `EventTriggerBeginCompleteQuery`/`EndCompleteQuery` around the top-level dispatch.
  - `src/commands/functioncmds.rs` (or wherever `EventTriggerInvoke`'s analog would call a function) -- fmgr-context call convention for passing `EventTriggerData` similarly to how M11's trigger firing passes `TriggerData` (find and reuse that pattern from `src/backend/commands/trigger.rs`).
- **Deliverable**: DDL commands actually invoke enabled event triggers at `ddl_command_start`/`ddl_command_end`/`sql_drop` with a correctly populated `EventTriggerData` (event name, parsetree, command tag); WHEN-tag filtering and `session_replication_role`-based enable/disable work end-to-end; a Rust-native or minimal stand-in trigger function (not full PL/pgSQL) can observe firing for framework testing.
- **Deps**: Step A (catalog + grammar must exist to create event triggers to fire); plan-003's `standard_process_utility` dispatcher (already the live DDL dispatch backbone -- this step only adds call sites into it, doesn't restructure it); plan-003 trigger-firing fmgr-context-call pattern from `commands/trigger.rs` (M11) as the template for `EventTriggerInvoke`; sinval/syscache invalidation infra (already landed) for `evtcache` invalidation.
- **Tests unblocked**: still not fully green (function bodies are PL/pgSQL), but this is the step that makes firing observably happen; with a stand-in function it can validate ordering/filtering logic that the `.sql` file's negative/enable-disable sections check.
- **Effort**: L (largest step -- touches ~20 call sites across the DDL dispatcher, needs the query-state stack, needs the fmgr-context call convention, needs cache invalidation wiring).

---

### Step C -- table_rewrite event + support functions

- **PG files**: `event_trigger.c` lines 1439-1523 (validate_table_rewrite_tags, EventTriggerTableRewrite), lines 1523-2054 (the four `pg_event_trigger_*` SQL-callable support functions: `pg_event_trigger_dropped_objects`, `pg_event_trigger_table_rewrite_oid`, `pg_event_trigger_table_rewrite_reason`, `pg_event_trigger_ddl_commands`), lines 2054-2412 (`pg_event_trigger_ddl_commands`'s body + the two stringify helpers).
- **Our target files (verify)**:
  - `src/commands/event_trigger.rs` -- `EventTriggerTableRewrite`, `validate_table_rewrite_tags` (currently `unimplemented!()`/absent).
  - `src/backend/tcop/utility.rs` / `src/commands/tablecmds.rs` -- call `EventTriggerTableRewrite` from the ALTER TABLE rewrite path (`AT_REWRITE_*` reasons: ALTER_PERSISTENCE, DEFAULT_VAL, COLUMN_REWRITE, ACCESS_METHOD -- cross-check which of these paths exist yet in PepperDB's `tablecmds.rs`; likely partial since full ALTER TABLE rewrite semantics may not all be landed).
  - New file or extend `src/backend/utils/adt/` module for the four `pg_event_trigger_*()` SQL-callable functions (these need `CALLED_AS_EVENT_TRIGGER`-equivalent context access from within a running event trigger function call -- depends on Step B's fmgr-context plumbing).
- **Deliverable**: `pg_event_trigger_table_rewrite_reason()`/`_oid()`, `pg_event_trigger_ddl_commands()`, `pg_event_trigger_dropped_objects()` are callable from inside a firing event trigger and return correct values.
- **Deps**: Step B (needs the fmgr-context/`EventTriggerData` call convention and the `CollectedCommand`/dropped-objects lists it populates).
- **Tests unblocked**: the middle/later sections of `event_trigger.sql` that call these support functions (still requires Step D's PL/pgSQL for the calling function bodies to actually invoke them and print results, but the Rust-side function implementations are independent, testable via a Rust-native stand-in caller).
- **Effort**: M.

---

### Step D -- login event + full test parity (blocked on plan-004 PL/pgSQL)

- **PG files**: `event_trigger.c` line 359-426 (`EventTriggerOnLogin`, `SetDatabaseHasLoginEventTriggers`), `postinit.c` ~line 1099 (`MyDatabaseHasLoginEventTriggers` set from `dathasloginevt`), `postgres.c` ~line 4373 (the `EventTriggerOnLogin()` call site in `PostgresMain`), `pg_database.h` (`dathasloginevt` column).
- **Our target files (verify)**:
  - `src/commands/event_trigger.rs` -- implement `EventTriggerOnLogin` for real (currently `unimplemented!()`); add `SetDatabaseHasLoginEventTriggers`-equivalent (updates `pg_database.dathasloginevt` when a `login`-event trigger is created/dropped/enabled/disabled -- Step A's `CreateEventTrigger`/`AlterEventTrigger` need to call this too, so this has a soft back-dependency into Step A's implementation, not just an add-on).
  - `src/backend/bootstrap/bootstrap.rs:1317`, `src/backend/commands/dbcommands.rs:74` -- stop hardcoding `dathasloginevt = false`; read/propagate the real flag.
  - Connect-to-database path (wherever PepperDB's equivalent of `InitPostgres`/postinit.c lives -- search `src/backend/utils/init/` or `src/session.rs`'s connect logic) -- set `Session::database_has_login_event_triggers` from `pg_database.dathasloginevt` at connect time.
  - `src/backend/tcop/postgres.rs` -- add the `EventTriggerOnLogin()` call site in the connection-loop setup, right before entering the message loop (near the `MessageContext`/`row_description_context` setup visible around the equivalent of `PostgresMain`, i.e. the block preceding line ~114's "the backend task itself is spawned" comment).
- **Deliverable**: `CREATE EVENT TRIGGER ... ON login` fires on every new connection when enabled; `dathasloginevt` persists correctly and is checked by `\c` reconnects in the test.
- **Deps**: Steps A+B (needs event-trigger creation + the generic firing/invoke mechanism); **plan-004 PL/pgSQL execution** for the actual trigger body (`INSERT INTO user_logins ...`) -- without PL/pgSQL, the framework parts (flag propagation, call-site wiring, ENABLE ALWAYS + session_replication_role gating) can be verified with a Rust-native stand-in function, but the test's literal expected output (which depends on `RAISE NOTICE` + `INSERT` side effects from a real plpgsql body) cannot match until PL/pgSQL lands.
- **Tests unblocked**: `event_trigger_login` reaches framework-verifiable parity here; full byte-for-byte `.out` match requires PL/pgSQL (external dependency, not part of this step).
- **Effort**: S (small, focused; the login-flag propagation is the only genuinely new piece, the firing mechanism is reused from Step B).

---

## 5. Architecture notes

**The PL/pgSQL dependency is real but only affects two of the four steps' full completion, not the framework as a whole.** Every event trigger function in both test files is declared `LANGUAGE plpgsql` (one negative-test variant is `LANGUAGE sql`, expected to be rejected). Steps A, B, and C (catalog, grammar, firing dispatch, table_rewrite, support functions) are pure Rust-native machinery -- they do not require executing a PL/pgSQL function body to be implemented or even to be meaningfully tested, provided the fmgr-context call convention (Step B) can invoke *some* Rust-native function registered with `RETURNS event_trigger` (e.g., a test-only builtin that asserts on its `EventTriggerData` context, analogous to how C-language trigger functions are tested elsewhere before PL/pgSQL trigger bodies exist). Only Step D's exact literal `.out` match, and roughly the back half of `event_trigger.sql` (`test_event_trigger()`'s `RAISE NOTICE` calls, whose text appears verbatim in `expected/event_trigger.out`), truly require plan-004's PL/pgSQL execution subsystem.

**Recommended landing order relative to PL/pgSQL**: Steps A -> B -> C can be fully implemented, reviewed, and clippy/cargo-test verified *before* PL/pgSQL exists, using a Rust-native stand-in event-trigger function for integration testing (not a pg_regress pass, but a `cargo test` integration test). This de-risks the framework work and lets it proceed in parallel with -- or ahead of -- whatever plan schedules PL/pgSQL. Step D's connect-time wiring is similarly independent of PL/pgSQL; only the final `event_trigger`/`event_trigger_login` pg_regress PASS is gated on PL/pgSQL landing.

**Two parallel `utility.rs` files remain a hazard**: `src/tcop/utility.rs` (header-stub twin, `ProcessUtility`/`standard_ProcessUtility` both `unimplemented!()`) is NOT the dispatcher that runs; `src/backend/tcop/utility.rs`'s `process_utility`/`standard_process_utility` is the live one (per plan-002's C-file-mapping invariant: `.c` bodies live under `src/backend/...`, headers under `src/...`). Step B's ~20 call sites all belong in the latter. A future code agent unfamiliar with this split could easily "implement" the dead header-stub file's `ProcessUtility` and see no effect -- worth flagging explicitly in the step's task description to avoid wasted work.

**`EventTriggerQueryState` needs a non-global home.** PG uses a static global (`currentEventTriggerState`, a manually-managed stack via `previous` pointer, pushed/popped around each top-level completed query for nested-command support). Per PepperDB's standing conventions (per-task state must be Send; no global mutable state; follow PG's lock/scope granularity faithfully rather than collapsing it), this should live as a stack (`Vec<EventTriggerQueryState>` or similar) scoped to the `Session`/current async task, pushed in `EventTriggerBeginCompleteQuery` and popped in `EventTriggerEndCompleteQuery`, mirroring the C push/pop discipline without introducing a process-global mutable static.

**`evtcache` invalidation reuses existing infra.** PG's real `evtcache.c` (not fully read in this pass -- verify exact file/line count before Step B) hooks into syscache invalidation callbacks on `EventTriggerRelationId`. PepperDB's plan-002 sinval/syscache-invalidation machinery (landed in F2, steps 14-16) is the generic mechanism to reuse here; this should not require new invalidation infrastructure, only registering a callback.

**Effort summary**: Step A = M, Step B = L (largest single piece: ~20 dispatcher call sites + fmgr-context convention + query-state stack), Step C = M, Step D = S (+ external PL/pgSQL dependency for full test parity). Total framework-only effort (A+B+C+D minus PL/pgSQL) is comparable to one of plan-004's existing "L" steps (e.g. step 10's string/regex/like/crypto cluster) spread across 4 sub-steps for reviewability, consistent with plan-002's practice of splitting a LARGE step into dependency sub-commits each independently agent-reviewed before a final squash.

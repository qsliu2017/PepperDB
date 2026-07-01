# PL/pgSQL + SQL-language function-body execution -- step breakdown

Status: research only, no code changed. This expands plan 004's step-23 register
entry ("PL/pgSQL function-body EXECUTION -- ~45 tests") into an implementable
sequence. It is its own campaign, run after (or interleaved with, see Arch Notes)
the in-scope steps 07-22.

## 1. PG source inventory

| file | lines | role |
|---|---|---|
| `src/pl/plpgsql/src/pl_gram.y` | 4244 | bison grammar for PL/pgSQL statements/expressions |
| `src/pl/plpgsql/src/pl_scanner.c` | 657 | hand-rolled lexer wrapping the core scanner, PL-specific token lookahead |
| `src/pl/plpgsql/src/pl_comp.c` | 2327 | compiler: builds `PLpgSQL_function` from a parsed source, namespace/variable resolution |
| `src/pl/plpgsql/src/pl_exec.c` | 9108 | the interpreter: `exec_stmt_*` tree-walker, expression eval via SPI, cursors, exceptions |
| `src/pl/plpgsql/src/pl_funcs.c` | 1694 | node constructors + `dump_*` debug printers (mechanical, low-value) |
| `src/pl/plpgsql/src/pl_handler.c` | 550 | `plpgsql_call_handler`/`plpgsql_inline_handler`/`plpgsql_validator` -- the fmgr-visible entry points |
| `src/pl/plpgsql/src/plpgsql.h` | 1336 | all `PLpgSQL_*` node types (`PLpgSQL_function`, `_execstate`, `_stmt_*` variants, `_datum`/`_var`/`_row`/`_rec`) |
| `src/pl/plpgsql/src/pl_reserved_kwlist.h` + `pl_unreserved_kwlist.h` | 52 + 112 | PL/pgSQL's own keyword lists (separate from the main SQL grammar's) |
| `src/pl/plpgsql/src/plpgsql--1.0.sql` | 20 | the extension SQL: registers `plpgsql_call_handler`/`_inline_handler`/`_validator` as C functions and `CREATE LANGUAGE plpgsql` binding them |

Total PL/pgSQL: ~19,900 lines (pl_exec.c alone is 9,108 -- roughly half the
subsystem and the highest-risk file: `exec_stmt_*` covers block/assign/perform/
call/getdiag/if/case/loop/while/fori/fors/forc/foreach_a/exit/return/return_next/
return_query/raise/assert/execsql/dynexecute/dynfors/open/fetch/close/commit/
rollback -- 26 statement kinds).

SQL-language functions (the other substrate):

| file | lines | role |
|---|---|---|
| `src/backend/executor/functions.c` | 2695 | `fmgr_sql` (the call handler installed as `fn_addr` for prolang=SQL), `init_sql_fcache`, per-statement `execution_state` list, `postquel_*` helpers, `check_sql_fn_retval` |
| `src/backend/utils/fmgr/fmgr.c` | 2200 | `fmgr_info_cxt_security`'s prolang switch: this is the dispatch point that installs `fmgr_sql` / `fmgr_security_definer` / (for PL languages) the call-handler indirection |

`plpgsql--1.0.sql`'s pattern generalizes: `pg_language.lanplcallfoid` /
`lanvalidator` / `laninline` point at C functions (`fmgr_c_language.c`-style OIDs
for plpgsql's own handler, itself a plain internal/C function) whose `fn_addr`
the fmgr dispatch installs when a call target's `prolang` matches. So the *real*
generic mechanism to build is: fmgr dispatch on `prolang` -> for SQLLANGUAGEID
install `fmgr_sql`, for a plpgsql-registered language OID install
`plpgsql_call_handler` (a Rust fn pointer, since there's no dynamic loader).

## 2. PepperDB current state (grep results, verified by reading each file)

| area | file(s) | status |
|---|---|---|
| fmgr prolang dispatch | `src/backend/utils/fmgr/fmgr.rs` | `fmgr_info_cxt_security` non-builtin path is `unimplemented!()` (line ~153: "the call handler / the dynamic loader, none of which is translated yet"). This is the single chokepoint every function call (SQL or PL/pgSQL) must pass through. |
| SQL-language function executor | `src/executor/functions.rs` | 57-line header stub. `fmgr_sql`, `prepare_sql_fn_parse_info`, `sql_fn_parser_setup`, `check_sql_fn_statements`, `check_sql_fn_retval`, `CreateSQLFunctionDestReceiver` are all `unimplemented!()`. Nothing here executes. |
| CREATE FUNCTION (live path) | `src/backend/commands/functioncmds.rs` (356 lines) | Real, reachable: parses LANGUAGE/AS options, resolves `prolang` to `INTERNALLANGUAGEID`/`SQLLANGUAGEID` (rejects anything else with `ERRCODE_UNDEFINED_OBJECT`, so `LANGUAGE plpgsql` currently errors "language does not exist"), calls `pg_proc::procedure_create`. IN-only parameters; OUT/INOUT/VARIADIC/defaults/3+-part-typenames all hit `not_yet_reachable` (an `unimplemented!` wrapper). Catalog row IS written; body is never executed. |
| CREATE FUNCTION (dead path) | `src/commands/defrem.rs` | Old header-stub `CreateFunction`/`RemoveFunctionById`/`AlterFunction`/`ExecuteDoStmt`/`ExecuteCallStmt`/`CallStmtResultDesc`, ALL `unimplemented!()`. Confirmed by grep: nothing calls into this file's `CreateFunction`/`ExecuteDoStmt`/`ExecuteCallStmt` -- `functioncmds.rs` is the live path for CREATE FUNCTION; DO/CALL have no live path at all (dead stub only). |
| pg_proc catalog | `src/backend/catalog/pg_proc.rs` | `procedure_create` (live) writes `prosrc`/`prolang`/`prokind` etc. into the heap row -- this is real and working. `src/catalog/pg_proc.rs::ProcedureCreate` is the old header-stub twin, unused. Language OID consts: `INTERNALLANGUAGEID = 12`, `SQLLANGUAGEID = 14` (no PLPGSQLLANGUAGEID const yet -- would need bootstrap seeding, see step 1 below). |
| CREATE LANGUAGE | `src/commands/proclang.rs` | 14-line stub: `CreateProceduralLanguage` and `get_language_oid` both `unimplemented!()`. No pg_language row writer exists yet. |
| DO statement | grammar + executor | `Node::DoStmt`/`InlineCodeBlock` types exist in `src/nodes/parsenodes.rs` (lines 2208-2221) and `src/nodes/nodes.rs`, but **no grammar production** references `DoStmt` in `src/backend/parser/gram.lalrpop` (only the bare `"DO"` keyword token exists, used elsewhere for `MERGE ... WHEN ... THEN DO NOTHING`). DO cannot currently be parsed. Dispatch stub is the dead `defrem::ExecuteDoStmt`. |
| CALL statement | grammar + executor | `Node::CallStmt` type exists; no grammar production; dispatch stub is dead `defrem::ExecuteCallStmt`/`CallStmtResultDesc`. |
| SPI | `src/backend/executor/spi.rs` (421 lines, live) + `src/executor/spi_priv.rs` (94 lines, struct defs) | This is the real foundation to build on. `spi_connect`/`spi_finish`/`spi_execute`/`spi_prepare`/`spi_execute_plan` work for the M9-reachable core: single-statement query strings, deformed result rows (`SpiRow { values, isnull }` + `TupleDesc`), a `tokio::task_local!` per-task connection stack. STAGED (not yet done): `SPI_execute_with_args`, subtransaction machinery, cursor advanced options. `SPI_register_trigger_data` stub exists in `src/executor/spi.rs:386` referencing `TriggerData` -- the trigger-context passing convention is already modeled. |
| funcapi (SRF support) | `src/funcapi.rs` (245 lines) | Header stub, all `unimplemented!()`: `get_call_result_type`, `InitMaterializedSRF`, `init_MultiFuncCall`, `BuildTupleFromCStrings`, `TupleDescGetAttInMetadata`, etc. PL/pgSQL's `RETURN NEXT`/`RETURN QUERY` and SQL-language SETOF functions both need this. |
| Trigger firing | `src/backend/commands/trigger.rs` (847 lines) | `exec_ar_insert_triggers`/`exec_ar_delete_triggers`/`exec_ar_update_triggers` are real and queue matching AFTER ROW triggers via `save_ri_event`, but this only records "a trigger should fire" -- there is no code path that then calls the trigger function's body (would need to build a `TriggerData` context, call through fmgr with prolang=plpgsql, and it dead-ends at the fmgr stub above regardless). `exec_bs_insert_triggers`/`exec_as_insert_triggers` (BEFORE/statement) are no-op stubs. `TriggerData` struct itself is defined and real (line 34). |
| Event triggers | `src/commands/event_trigger.rs`, `src/catalog/pg_event_trigger.rs` | `EventTriggerData` struct exists (fmgr-context convention mirrors triggers); firing/dispatch not investigated in depth but almost certainly has the same "queues but never calls" shape, and additionally needs `CREATE EVENT TRIGGER` DDL + `pg_event_trigger` catalog population, which is listed as its own step-23 register line ("event triggers") -- so event-trigger *DDL* is out of scope for this campaign; only PL bodies of already-working triggers matter here. |
| Lexer/parser precedent | `src/backend/parser/scan.rs` (hand-rolled main SQL lexer) + `src/backend/parser/gram.lalrpop` (2649-line lalrpop grammar) | Working precedent for building a second, smaller lexer+grammar pair for PL/pgSQL: reuse the `lalrpop` toolchain and hand-rolled-scanner style rather than introducing a new parser-generator dependency. |

**Conclusion: PL/pgSQL is 100% missing** (no file under `src/` mentions `plpgsql`
except this survey and the catalog function-body TODO comments). SQL-language
function execution is scaffolded (types exist) but not implemented (every
function in the chain is `unimplemented!()`). This is greenfield work end to end.

## 3. Tests unblocked

Cross-referencing `ai/tmp/regress-analysis/*.md` (which already marked
`create_function_sql`, `create_procedure`, and `triggers` OUT_OF_SCOPE citing
"PL/pgSQL body execution MISSING" / "fmgr non-builtin path unimplemented") against
a grep of every `ref/postgres/src/test/regress/sql/*.sql` file in
`parallel_schedule` for `LANGUAGE plpgsql`, `DO $$`, or
`CREATE [OR REPLACE] FUNCTION/PROCEDURE ... LANGUAGE`.

Primary blockers (test cannot pass at all without function-body execution):

| test | hits | notes |
|---|---:|---|
| plpgsql | 257 | the dedicated PL/pgSQL test file -- essentially 100% blocked |
| triggers | 59 | PL/pgSQL trigger functions throughout; already OUT_OF_SCOPE per batch-triggers.md |
| create_function_sql | 37 | SQL-language + plpgsql function DDL/execution; already OUT_OF_SCOPE per batch-create_function_sql.md |
| privileges | 26 | GRANT EXECUTE on SQL/plpgsql functions/procedures; also needs CREATE ROLE (separate blocker, step 23 register) |
| event_trigger | 21 | plpgsql event trigger bodies; also needs CREATE EVENT TRIGGER DDL (separate blocker) |
| merge | 15 | plpgsql helper functions (`merge_func`, `explain_merge`) driving MERGE test scenarios |
| rangefuncs | 13 | SQL/plpgsql functions returning sets, used as range/table functions |
| polymorphism | 9 | polymorphic SQL/plpgsql functions (anyelement/anyarray bodies) |
| domain | 9 | SQL/plpgsql functions in domain CHECK constraints and casts |
| create_procedure | 6 | CALL statement + procedure bodies; already OUT_OF_SCOPE per batch analysis |
| create_table | 6 | function calls in DEFAULT/CHECK expressions (some may be builtin-only; needs per-line triage) |
| alter_generic | 8 | ALTER ... OWNER/RENAME on SQL functions (DDL-only in most cases; needs triage -- likely does NOT require body execution) |
| alter_table | 8 | functions in constraints/defaults; needs triage |
| aggregates | 10 | CREATE AGGREGATE with plpgsql state-transition helpers |
| generated_stored / generated_virtual | 4 + 4 | generated-column expressions calling functions |
| fast_default | 3 | column defaults via functions |
| create_cast | 3 | CAST ... WITH FUNCTION (SQL-language) |
| plancache | 3 | plan invalidation across plpgsql calls |
| create_aggregate | 1 | aggregate transition functions |
| create_function_c | 3 | mostly C-language (regress.so) -- NOT unblocked by this campaign, listed for contrast |
| truncate | (not in count above but noted in batch-triggers.md) | `trunctrigger()`/`tp_ins_data()` are plpgsql; ALSO blocked by TRUNCATE statement + inheritance/partitioning (separate blockers) |
| txid | (noted in batch-triggers.md) | `test_future_xid_status()` is plpgsql; ALSO blocked by the txid module itself (separate) |

Several of the above (`alter_generic`, `alter_table`, `create_table`,
`create_function_c`) contain LANGUAGE-clause hits that are largely DDL-shape or
C-language and must be triaged line-by-line before counting; they are NOT
included in the headline count below because the batch-analysis files don't yet
confirm them as primarily blocked by body execution.

**Headline count**: cross-referencing against the already-classified batch files
(which explicitly cite "PL/pgSQL body execution MISSING" or the fmgr
`unimplemented!` as A blocker, not necessarily the ONLY blocker), the tests
**directly and primarily gated on this campaign** are:

    plpgsql, triggers, create_function_sql, create_procedure,
    privileges, event_trigger, merge, rangefuncs, polymorphism,
    domain, aggregates, create_aggregate, create_cast, plancache,
    generated_stored, generated_virtual, fast_default

= **17 tests**, matching plan 004 file-list/23.txt's own estimate of "~45 tests"
only if each test's several OTHER blockers (CREATE ROLE, CREATE EVENT TRIGGER,
partitioning, generated columns proper) are also resolved by other step-23
campaigns running in parallel or first. **This campaign alone (function-body
execution, nothing else) is necessary but not sufficient for most of them** --
`create_procedure` also needs the CALL statement (covered here, see step 6),
`privileges` also needs CREATE ROLE (separate), `event_trigger` also needs
CREATE EVENT TRIGGER DDL (separate), `triggers`/`truncate` also need TRUNCATE
+ constraint-trigger machinery (separate). Only `plpgsql`,
`create_function_sql`, `rangefuncs`, `polymorphism`, `domain`, `merge`,
`aggregates`, `create_aggregate`, `create_cast`, `plancache` are gated
*solely* (as far as this research found) on function-body execution + the
parameter-mode/DEFAULT gaps already flagged in `functioncmds.rs` -- call this
the **realistic near-term yield: ~10 tests fully unblocked**, with the other
~7-35 becoming reachable once their independent blockers also clear.

## 4. Architecture notes

**Scope decision: build SQL-language functions FIRST, as the smaller substrate,
then PL/pgSQL.** Reasons:
- SQL-language execution is ~150 lines of real logic once fmgr dispatch and
  funcapi exist (`fmgr_sql` is mostly "run each statement via SPI/the planner,
  return the last statement's result"; PG's `functions.c` at 2695 lines includes
  a lot of SRF/lazy-eval machinery that can be deferred to a STAGED subset).
- It exercises the exact fmgr `prolang` dispatch, SPI plan execution, and
  `check_sql_fn_retval` type-coercion machinery that PL/pgSQL's `pl_exec.c`
  ALSO depends on for every `PERFORM`/assignment/`RETURN QUERY` statement (PL/pgSQL
  runs its embedded SQL exactly the same way, through SPI).
  Building SQL-language functions first retires that shared risk cheaply.
- Several regress tests (`create_cast`, `rangefuncs`, parts of `polymorphism`)
  only need SQL-language, not PL/pgSQL, so there is standalone payoff.
- PL/pgSQL is a second language front end (own lexer, own grammar, own
  `PLpgSQL_function`/`_execstate` compiled representation) bolted onto the same
  execution substrate. Doing it second means the substrate (fmgr dispatch, SPI
  call convention, funcapi, TriggerData plumbing) is already proven.

**Do NOT attempt "full PL/pgSQL"** in one step. `pl_exec.c` is 9,108 lines
covering 26 statement kinds plus cursors, exceptions, dynamic SQL, and
GET DIAGNOSTICS. Split the interpreter itself into sub-steps by statement-kind
cluster (see steps 4a-4d below), matching plan 004's existing convention of
"translate a module cluster, flip its tests" rather than one big-bang step.

**Async interaction with SPI.** PepperDB's SPI (`src/backend/executor/spi.rs`)
is already async (`spi_execute`, `spi_execute_plan` are `pub async fn`, backed by
a `tokio::task_local!` stack since a backend is a tokio task). The PL/pgSQL
interpreter's `exec_stmt_execsql`/`exec_stmt_perform`/`exec_stmt_dynexecute`/
cursor open in PG all funnel through `SPI_execute`/`SPI_execute_plan`/
`SPI_cursor_open`. The Rust interpreter's per-statement dispatch (`exec_stmt`,
`exec_stmts`) must therefore itself be `async fn` (or wrap the whole function
call in `spi_scope_async`, per the existing doc comment: "A backend that may
call SPI (PL/pgSQL, RI triggers) wraps its work in this scope"). This is a
straightforward `async`/`.await` propagation through the tree-walker -- no
architectural conflict with the existing async model, but it means the
interpreter cannot be written as a naive recursive-descent function without
`Box::pin` for the recursive async calls (standard Rust async-recursion
boilerplate; note it in the step, don't rediscover it mid-implementation).

**The fmgr language-handler binding is the crux integration point.** In PG,
`prolang` is just another `pg_proc` OID that fmgr resolves to a C function
pointer (`plpgsql_call_handler`), which then re-dispatches internally by
inspecting `fcinfo->flinfo->fn_oid`'s OWN `pg_proc.prosrc`/`prolang`. PepperDB has
no dynamic loader and never will (`fmgr.rs`'s C-language path is explicitly
deferred elsewhere), so the Rust design should SKIP the indirection and give
`fmgr_info_cxt_security` a direct `match prolang` with three arms:
`INTERNALLANGUAGEID`/`SQLLANGUAGEID` (existing paths) plus a new
`PLPGSQLLANGUAGEID` arm that installs a Rust fn pointer equivalent to
`plpgsql_call_handler` (compile-on-first-call + cached execstate, same as PG).
This means `CREATE LANGUAGE plpgsql` (step 1 below) does NOT need to support
arbitrary user-defined languages with dynamically loaded handlers -- it only
needs to seed the ONE known OID PepperDB's own fmgr special-cases, matching how
`INTERNALLANGUAGEID`/`SQLLANGUAGEID` are already special-cased today. Treat
`CREATE LANGUAGE` as catalog bookkeeping (so `\dL`/pg_language queries and
"CREATE FUNCTION ... LANGUAGE plpgsql" name resolution work) rather than a
general extensibility mechanism.

## 5. Step breakdown

Each step lists: PG files, PepperDB target files (verify paths before coding --
this survey's paths are current as of commit c287e6c on branch r3), deliverable,
dependencies, tests unblocked, effort.

---

### Step P1 -- CREATE LANGUAGE + prolang dispatch skeleton

- **PG files**: `src/backend/commands/proclang.c` (not yet read in depth;
  expect ~400-600 lines based on typical PG sizing -- verify), `src/backend/utils/fmgr/fmgr.c` (2200 lines, only the prolang-switch section in `fmgr_info_cxt_security` matters, ~50-100 lines).
- **Target files**: `src/commands/proclang.rs` (currently 14-line stub:
  `CreateProceduralLanguage`, `get_language_oid`), `src/backend/utils/fmgr/fmgr.rs`
  (extend `fmgr_info_cxt_security`'s prolang match), `src/backend/catalog/pg_proc.rs`
  or a new `src/backend/catalog/pg_language.rs` (verify: does a live pg_language
  catalog writer exist anywhere yet? Not found in this survey -- likely needs
  creation), add `PLPGSQLLANGUAGEID` const alongside the existing
  `INTERNALLANGUAGEID = 12` / `SQLLANGUAGEID = 14` in `pg_proc.rs`.
- **Deliverable**: `CREATE LANGUAGE plpgsql` (or a bootstrap-seeded equivalent,
  see Arch Notes -- prefer bootstrap seeding like `INTERNALLANGUAGEID` over a
  real DDL path, since only one hardcoded language OID is needed) writes/seeds a
  pg_language row; `CREATE FUNCTION ... LANGUAGE plpgsql` in `functioncmds.rs`
  stops raising "language does not exist" and resolves to the new OID;
  `fmgr_info_cxt_security` recognizes the OID and installs a placeholder
  `fn_addr` (can panic with a clear "PL/pgSQL body execution not yet
  implemented" `unimplemented!` for now -- this step is plumbing only).
- **Dependencies**: none beyond what exists (`functioncmds.rs`'s existing
  `prolang` match, `pg_proc::procedure_create`).
- **Tests unblocked**: none directly; this is a prerequisite for every later step.
- **Effort**: S.

---

### Step P2 -- SQL-language function execution (`fmgr_sql`)

- **PG files**: `src/backend/executor/functions.c` (2695 lines -- target the
  core ~40% covering `init_sql_fcache`, `postquel_start`/`postquel_getnext`,
  `fmgr_sql`, `check_sql_fn_retval`; defer `SQLFunctionParseInfo` caching
  edge cases, tuplestore-based materialization for large SRF results, and
  `parallel_query`-related fields as STAGED).
- **Target files**: `src/executor/functions.rs` (57-line stub: `fmgr_sql`,
  `prepare_sql_fn_parse_info`, `sql_fn_parser_setup`, `check_sql_fn_statements`,
  `check_sql_fn_retval`, `CreateSQLFunctionDestReceiver` -- fill all in),
  `src/funcapi.rs` (245-line stub -- needs at least `get_call_result_type`,
  `build_function_result_tupdesc_d/t` for SETOF/composite-returning SQL functions),
  `src/backend/utils/fmgr/fmgr.rs` (wire `SQLLANGUAGEID`'s `fn_addr` to the new
  `fmgr_sql`, replacing today's `unimplemented!()` at line ~153).
- **Deliverable**: `SELECT sql_fn(...)` for a scalar-returning, single- or
  multi-statement `LANGUAGE sql` function actually executes (each statement run
  via SPI/the planner in sequence; the last statement's result becomes the
  return value, matching PG's postquel semantics); basic SETOF support via
  funcapi's materialize path.
- **Dependencies**: Step P1 (dispatch skeleton); existing SPI (`spi_execute`/
  `spi_execute_plan`, already live) is the substrate -- no changes needed there
  beyond confirming multi-statement handling (SPI currently has
  `unimplemented!("SPI_execute: multi-statement query strings deferred")` at
  `src/backend/executor/spi.rs:138` -- CHECK whether `fmgr_sql` needs this or
  can call SPI once per statement, which sidesteps it entirely; prefer the
  sidestep to avoid pulling in a second dependency).
- **Tests unblocked**: partial `create_function_sql` (the LANGUAGE SQL half),
  `create_cast` (3 hits, `CAST ... WITH FUNCTION` on SQL functions),
  parts of `rangefuncs` and `polymorphism` that use LANGUAGE SQL only.
- **Effort**: M.

---

### Step P3 -- CREATE FUNCTION parameter-mode + DEFAULT completion

- **PG files**: `src/backend/commands/functioncmds.c` (the
  `interpretFunctionParameters`/`compute_return_type` sections covering OUT/
  INOUT/VARIADIC/TABLE modes and defaults -- this is NOT new PL/pgSQL work, it's
  closing a gap already flagged in PepperDB's own `functioncmds.rs` doc comment).
- **Target files**: `src/backend/commands/functioncmds.rs` (extend
  `interpret_function_parameters`, currently `not_yet_reachable` on non-IN modes
  and `defexpr`; extend `resolve_type` for 3+-part type names if needed by test
  bodies), `src/backend/catalog/pg_proc.rs` (`procedure_create` likely needs new
  parameters for `allParameterTypes`/`parameterModes`/`parameterNames`/
  `parameterDefaults` -- verify current signature).
  a
- **Deliverable**: `CREATE FUNCTION f(IN a int, OUT b int, INOUT c int)` and
  parameter defaults (`f(a int DEFAULT 1)`) both create a valid pg_proc row (NOT
  gated on body execution -- this can and should land independently, possibly
  even before P2, since `create_procedure`/`create_function_sql` exercise it
  heavily regardless of language).
- **Dependencies**: none (independent of P1/P2; can run in parallel or first).
- **Tests unblocked**: none alone (combines with P2/P4 to unblock
  `create_function_sql`/`create_procedure`), but removes a blocker cited
  explicitly in `batch-create_function_sql.md`.
- **Effort**: M.

---

### Step P4 -- CALL statement + procedures

- **PG files**: grammar rule for `CALL` in `gram.y` (search for `CallStmt` --
  small, ~30-50 lines), `src/backend/commands/functioncmds.c`'s the CALL
  execution path is actually in `src/backend/executor/functions.c` or a
  dedicated `src/backend/tcop/utility.c` case -- VERIFY exact PG location for
  `ExecuteCallStmt` before starting (this survey did not pin it down).
- **Target files**: `src/backend/parser/gram.lalrpop` (add a `CallStmt`
  production -- `Node::CallStmt` type already exists in `parsenodes.rs`),
  `src/backend/tcop/utility.rs` (add a `Node::CallStmt` dispatch arm, following
  the existing `Node::CreateFunctionStmt` pattern at lines 149/291/513),
  `src/backend/commands/functioncmds.rs` or a new sibling module for
  `ExecuteCallStmt`/`CallStmtResultDesc` (the dead stubs in `src/commands/defrem.rs`
  are reference-only, do not resurrect that file -- write fresh in the `backend/`
  tree per the plan-002 file-mapping invariant).
- **Deliverable**: `CALL proc(args)` parses and executes a `LANGUAGE sql` or
  `LANGUAGE plpgsql` procedure body (OUT/INOUT parameter passback included).
- **Dependencies**: P1 (dispatch), P2 (if procedure is LANGUAGE sql) or P5-P8
  (if LANGUAGE plpgsql -- most `create_procedure.sql` procedures ARE plpgsql),
  P3 (OUT/INOUT parameters, used throughout `create_procedure.sql`).
- **Tests unblocked**: `create_procedure` (6 hits, but the whole file is
  CALL-shaped, so likely fully unblocked once P1-P8 land), contributes to
  `privileges` (`CALL`-adjacent GRANT EXECUTE ON PROCEDURE cases).
- **Effort**: M.

---

### Step P5 -- PL/pgSQL lexer + grammar (parse only, no execution)

- **PG files**: `pl_scanner.c` (657 lines), `pl_gram.y` (4244 lines --
  the largest single file in this campaign after pl_exec.c),
  `pl_reserved_kwlist.h` + `pl_unreserved_kwlist.h` (164 lines combined),
  `plpgsql.h` (1336 lines -- the `PLpgSQL_*` node type definitions needed as
  the grammar's output AST).
- **Target files**: new `src/pl/plpgsql/` tree (following the
  plan-002 C-file-mapping invariant: `.c`/`.y` -> `src/pl/plpgsql/<name>.rs`),
  specifically: `src/pl/plpgsql/pl_scanner.rs` (hand-rolled, mirroring the
  precedent in `src/backend/parser/scan.rs`), `src/pl/plpgsql/gram.lalrpop`
  (mirroring `src/backend/parser/gram.lalrpop`'s toolchain choice),
  `src/pl/plpgsql/plpgsql.rs` (the `PLpgSQL_*` struct/enum definitions, i.e. the
  header translation of `plpgsql.h`).
- **Deliverable**: given a `prosrc` string for a `LANGUAGE plpgsql` function,
  produce a `PLpgSQL_function` AST (compiled form: variable namespace, statement
  tree) OR a parse error. No execution yet -- this step's exit test is "the
  compiler accepts every plpgsql function body in `plpgsql.sql` without
  panicking," checked by a standalone unit-test harness that iterates the
  regress SQL file's `CREATE FUNCTION` bodies (do not wire into the live
  CREATE FUNCTION path yet -- keep this step reviewable in isolation).
- **Dependencies**: P1 (need `PLPGSQLLANGUAGEID` to exist so `functioncmds.rs`
  accepts the LANGUAGE clause without erroring, though actually parsing the body
  can be deferred to validation time and doesn't strictly need P1 first --
  soft dependency only).
- **Tests unblocked**: none alone (parsing without execution unblocks nothing
  in pg_regress, which only checks output).
- **Effort**: XL. This is the single largest step in the campaign (pl_gram.y
  is bigger than PepperDB's entire existing main SQL grammar). Consider
  splitting further by grammar section (expressions/statements/declarations) if
  the implementing agent's context window struggles with one 4244-line source
  file; the plan-002 precedent of "one step per C file" may need relaxing here
  to "one step per pl_gram.y section" -- flag this to the orchestrator when
  scheduling.

---

### Step P6 -- PL/pgSQL interpreter core (block/assign/if/case/loop/control-flow)

- **PG files**: `pl_exec.c` lines ~1663-2164 (`exec_stmt_block`, `exec_stmts`,
  the dispatch switch) and ~2526-3196 (`exec_stmt_if`, `exec_stmt_case`,
  `exec_stmt_loop`, `exec_stmt_while`, `exec_stmt_fori`, `exec_stmt_exit`) --
  roughly 1000 of the 9108 lines; expression evaluation
  (`exec_eval_expr`/`exec_eval_simple_expr`, elsewhere in the file, ~500 more
  lines) is a hard dependency, pull it in too.
- **Target files**: new `src/pl/plpgsql/pl_exec.rs` (the interpreter/
  tree-walker; this will be the largest Rust file in the campaign, expect
  splitting into `pl_exec/mod.rs` + per-statement-cluster submodules given
  pl_exec.c's 9108-line total).
- **Deliverable**: a `PLpgSQL_function` (from P5) can be executed for the
  control-flow subset: blocks, variable assignment, IF/CASE, LOOP/WHILE/FOR
  (integer range only -- FORS/FORC/FOREACH deferred to P7), EXIT/CONTINUE,
  RETURN (scalar only). Expression evaluation goes through SPI
  (`spi_execute`/`spi_execute_plan`, per Arch Notes) since PL/pgSQL expressions
  are themselves tiny SQL queries (`SELECT <expr>`).
- **Dependencies**: P5 (AST), P1 (dispatch to install the plpgsql call handler
  fn pointer), existing SPI (async, per Arch Notes -- the interpreter's
  `exec_stmt`/`exec_stmts` must be `async fn`).
- **Tests unblocked**: none fully alone; contributes the majority of `plpgsql`
  test coverage once combined with P7/P8.
- **Effort**: XL.

---

### Step P7 -- PL/pgSQL SPI-backed statements (EXECSQL/PERFORM/cursors/RETURN QUERY)

- **PG files**: `pl_exec.c` ~2180-2197 (`exec_stmt_perform`), ~4208-4630
  (`exec_stmt_execsql`, `exec_stmt_dynexecute`, `exec_stmt_dynfors`), ~2839-3164
  (`exec_stmt_fors`, `exec_stmt_forc`, `exec_stmt_foreach_a`), ~3326-3725
  (`exec_stmt_return_next`, `exec_stmt_return_query`), ~4657-4956
  (`exec_stmt_open`, `exec_stmt_fetch`, `exec_stmt_close`) -- roughly 2500 lines,
  the SPI-integration-heavy half of the interpreter.
- **Target files**: extends `src/pl/plpgsql/pl_exec.rs` (or its submodules from
  P6); likely needs SPI extensions: cursor support
  (`SPI_cursor_open`/`_fetch`/`_close` -- check whether `src/backend/executor/spi.rs`
  has any cursor stub yet, not found in this survey, probably needs adding),
  and funcapi's `RETURN NEXT`/`RETURN QUERY` tuplestore materialization
  (extends P2's funcapi work).
- **Deliverable**: dynamic SQL (`EXECUTE`), FOR loops over query results,
  explicit cursors, and set-returning PL/pgSQL functions (`RETURN NEXT`/
  `RETURN QUERY`) all work.
- **Dependencies**: P6 (interpreter core), P2 (funcapi SRF plumbing), SPI cursor
  support (new sub-dependency, not currently staged anywhere -- flag as a likely
  hidden S-M sub-step).
- **Tests unblocked**: combined with P6+P8, most of `plpgsql`; `rangefuncs`
  (13 hits, many are set-returning plpgsql functions).
- **Effort**: L.

---

### Step P8 -- PL/pgSQL exceptions, RAISE, GET DIAGNOSTICS, trigger/COMMIT-ROLLBACK

- **PG files**: `pl_exec.c` ~3725-3936 (`exec_stmt_raise`, `exec_stmt_assert`),
  ~2410-2526 (`exec_stmt_getdiag`), ~4956-5005 (`exec_stmt_commit`/`_rollback`),
  plus the exception-block unwind machinery (`PG_TRY`/`PG_CATCH` equivalent --
  search `pl_exec.c` for `exec_stmt_block`'s exception-handling half, likely
  overlapping the block range already covered in P6, re-verify boundary).
  Also `pl_exec.c`'s trigger entry point (`plpgsql_exec_trigger`, likely near
  the top of the file, not yet located precisely -- verify) and
  `plpgsql_exec_event_trigger`.
- **Target files**: extends `src/pl/plpgsql/pl_exec.rs`; wires into
  `src/backend/commands/trigger.rs` (the currently no-op
  `exec_bs_insert_triggers`/queue-only `exec_ar_insert_triggers` etc. need a new
  call-the-function-body step using the existing `TriggerData` struct at line 34
  as the fmgr context, matching PG's `CALLED_AS_TRIGGER` convention already
  noted in a comment at trigger.rs line 22-23).
- **Deliverable**: `EXCEPTION WHEN ... THEN` blocks catch PepperDB's existing
  `ereport!`/`ERROR`-unwind error model (this is the trickiest integration:
  PL/pgSQL exception handling must map onto PepperDB's Rust-native
  unwind-and-catch error model per `error-model-implemented` -- a PL/pgSQL
  `BEGIN...EXCEPTION...END` block needs to catch the Rust panic/Result carrying
  the `ErrorData`, match its SQLSTATE, and continue -- this is NOT a mechanical
  translation, budget real design time); RAISE statement produces
  `ereport!`-equivalent diagnostics; trigger functions ACTUALLY fire (not just
  queue) by calling into the plpgsql interpreter with a `TriggerData` context.
- **Dependencies**: P6, P7, PepperDB's error model (`ai/plans/003-total-translation/error.md`
  -- read before starting, this step is the one place PL/pgSQL semantics and
  PepperDB's Rust error model must reconcile).
- **Tests unblocked**: the remainder of `plpgsql` (exception tests are a
  significant fraction of the file), `triggers` (59 hits -- but ALSO needs
  TRUNCATE-trigger and constraint-trigger completion per `batch-triggers.md`,
  so triggers may still be partial after this step), `merge` (15 hits, plpgsql
  helper functions), `domain` (9 hits, plpgsql/SQL functions in CHECK
  constraints -- verify domain CHECK constraint evaluation calls through the
  same function-call path).
- **Effort**: XL.

---

### Step P9 -- DO statement (anonymous code blocks)

- **PG files**: `pl_handler.c`'s `plpgsql_inline_handler` (~50-100 lines within
  the 550-line file), the `DO` grammar rule in `gram.y` (small).
- **Target files**: `src/backend/parser/gram.lalrpop` (add the `DoStmt`
  production -- type already exists), `src/backend/tcop/utility.rs` (dispatch
  arm), a new function in `src/pl/plpgsql/pl_handler.rs` or
  `src/backend/commands/functioncmds.rs` implementing the inline-handler
  equivalent (compile + immediately execute a `PLpgSQL_function` without ever
  writing a pg_proc row).
- **Deliverable**: `DO $$ ... $$` (and `DO LANGUAGE plpgsql $$ ... $$`) executes
  a one-shot anonymous block.
- **Dependencies**: P5, P6, P7, P8 (needs the full interpreter -- DO blocks use
  arbitrary PL/pgSQL statements).
- **Tests unblocked**: `merge` (`DO LANGUAGE plpgsql $$ ... $$` at line 722),
  scattered `DO $$` blocks across several already-counted tests (e.g.
  `create_operator`'s privilege checks per `batch-create_function_sql.md`'s
  sibling analysis).
- **Effort**: S (thin wrapper once P6-P8 exist).

---

### Step summary table

| step | title | effort | depends on | key tests |
|---|---|---|---|---|
| P1 | CREATE LANGUAGE + prolang dispatch skeleton | S | -- | (enabler) |
| P2 | SQL-language function execution | M | P1 | create_function_sql (partial), create_cast |
| P3 | Parameter modes (OUT/INOUT/VARIADIC) + defaults | M | -- | (enabler for P4, create_procedure) |
| P4 | CALL statement + procedures | M | P1, P2 or P5-P8, P3 | create_procedure |
| P5 | PL/pgSQL lexer + grammar (parse only) | XL | P1 (soft) | (enabler) |
| P6 | PL/pgSQL interpreter core (control flow) | XL | P5, P1 | (partial plpgsql) |
| P7 | PL/pgSQL SPI-backed statements (cursors/SRF) | L | P6, P2 | rangefuncs, (partial plpgsql) |
| P8 | Exceptions/RAISE/triggers | XL | P6, P7, error model | plpgsql, triggers*, merge, domain |
| P9 | DO statement | S | P5-P8 | merge (DO block), scattered DO $$ |

(* triggers needs additional non-PL work per batch-triggers.md; not fully
green from P8 alone.)

Total distinct tests this campaign directly touches: **plpgsql,
create_function_sql, create_procedure, rangefuncs, polymorphism, domain, merge,
aggregates, create_aggregate, create_cast, plancache, triggers, privileges,
event_trigger, generated_stored, generated_virtual, fast_default = 17 tests**,
of which roughly **10 become fully green from this campaign alone** (plpgsql,
create_function_sql, create_procedure, rangefuncs, polymorphism, domain, merge,
aggregates, create_aggregate, create_cast, plancache -- that's actually 11;
the remaining 6 (triggers, privileges, event_trigger, generated_stored,
generated_virtual, fast_default) need one or more OTHER step-23 campaigns
(CREATE ROLE, CREATE EVENT TRIGGER, generated columns, TRUNCATE/constraint
triggers) to also land before they pass pg_regress end to end.

## 6. Sequencing recommendation

Run P1 -> P3 -> P2 in that order first (P1 and P3 are independent and can run
in parallel; P2 depends only on P1). This alone unblocks `create_cast` and
gets partial credit on `create_function_sql`/`create_procedure` with a modest
S+M+M effort footprint, before committing to the XL grammar/interpreter work.

Then P5 -> P6 -> P7 -> P8 -> P9 sequentially (each genuinely depends on the
previous; do not parallelize these). P4 (CALL) can be slotted in after P3 and
either after P2 (for LANGUAGE sql procedures) or after P8 (for LANGUAGE plpgsql
procedures, the common case in `create_procedure.sql` -- expect to revisit P4
twice: once for a SQL-only partial CALL, once fully after P8).

Given P5/P6/P8 are each XL and pl_exec.c's 9108 lines dwarf everything else in
this campaign, expect this to be plan 004's single largest step-23 sub-campaign
by implementation effort, plausibly larger than all of steps 07-22 combined.

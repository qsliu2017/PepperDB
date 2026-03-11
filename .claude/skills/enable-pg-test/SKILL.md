---
name: enable-pg-test
description: >
  Find and enable the next PostgreSQL regression test with the least
  implementation effort. Use this skill whenever the user asks to "enable
  the next test", "pass another regression test", "find the easiest PG test
  to fix", or anything about making progress on the PG regression test suite.
  Also trigger when the user mentions specific test names from the
  regress_test! macro (e.g. "enable select_having", "fix the boolean test").
---

# Enable Next PG Regression Test

Incrementally enable commented-out PostgreSQL regression tests in PepperDB by
finding the test with the smallest gap between current behavior and expected
output, implementing the minimum fixes, and verifying everything passes.

## Context

- PepperDB mirrors PG 18's regression test layout via symlinks:
  `tests/regress/sql/` and `tests/regress/expected/` point into
  `postgres/src/test/regress/`.
- There are two test harnesses sharing code via `tests/common/mod.rs`:
  - `tests/regress.rs` -- full PG regression tests. Tests declared in
    `regress_test!()` macro; commented-out tests are candidates.
  - `tests/regress_simple.rs` -- simplified subsets of PG tests containing
    only parts PepperDB can handle. Uses `simple_test!()` macro. SQL and
    expected output live in `tests/regress_simple/sql/` and
    `tests/regress_simple/expected/`.
- The shared harness (`tests/common/mod.rs`) runs each .sql file through
  `Database::execute_sql()`, formats output to match psql's aligned display,
  and diffs against the .out file.
- DataFusion handles all SELECT execution; our code handles DDL/DML.
- When a full PG test cannot pass due to too many unsupported features,
  create a simplified version in `tests/regress_simple/` with only the
  passable portions.

## Phase 1: Find the Best Candidate

If the user hasn't already chosen a specific test, analyze candidates to find
the one requiring the least new code.

1. **List candidates** -- read the commented-out entries in `regress_test!()`
   in `tests/regress.rs`.

2. **Read promising .sql files** -- start with small, self-contained tests
   (ones that CREATE/DROP their own tables). Good signals:
   - Small file (< 100 lines)
   - Uses only types PepperDB supports (int, text, char, float, bool)
   - No `\pset`, `SET`, `SHOW`, `BEGIN`/`ROLLBACK`, `CREATE FUNCTION`,
     `CREATE VIEW`, PL/pgSQL, or psql meta-commands
   - Creates and drops its own tables (not dependent on test_setup.sql)

3. **Check the .out file** for blockers:
   - Error position reporting (`LINE 1:` + `^`) -- now supported
   - PG-specific functions (pg_input_is_valid, currval, etc.)
   - Types not yet implemented (OID, NAME, MONEY, UUID, INTERVAL, etc.)
   - Features not in DataFusion or our executor

4. **Prefer storage-layer features** -- tests that exercise storage
   operations (TRUNCATE, COPY, ALTER TABLE, REINDEX, CLUSTER) or fill
   gaps in the existing storage stack are higher priority than tests that
   only require new SQL/DataFusion rewrites. These features strengthen
   the core engine and unblock many downstream tests.

5. **Rank by effort** and recommend the best candidate to the user with a
   brief explanation of what works and what needs fixing.

## Phase 2: Implement Fixes

Once a test is chosen:

### Step 1: Uncomment and run

Uncomment the test name in `regress_test!()` and run it to see the actual
diff. This reveals real gaps rather than guessing.

```bash
cargo test <test_name> -- --nocapture 2>&1 | tail -100
```

### Step 2: Categorize each mismatch

Read the diff carefully. Common categories:

| Category | Where to fix |
|----------|-------------|
| **Error message wording** | `df_to_pg()` in `src/executor/mod.rs` -- translate DataFusion errors to PG-style messages |
| **Error cursor position** | `df_to_pg()` + `find_column_position()` -- set `ErrorInfo.position` |
| **Column naming** | `batches_to_response()` in `src/executor/mod.rs` -- DataFusion names like `lower(t.c)` need PG-style stripping |
| **CHAR(N) trailing spaces** | Function-result columns need `trim_trailing_spaces` in `encode_arrow_value()` |
| **DataFusion SQL limitation** | Add a rewrite in `execute_select_df()` (like `rewrite_constant_having`) |
| **Missing SQL feature** | May need parser/executor changes -- evaluate if it's worth the effort |
| **Output formatting** | `format_table()` / `format_error()` in `tests/regress.rs` |

### Alternative: Create a simplified test

If the full PG test has too many blockers (transactions, PL/pgSQL, unsupported
types, etc.), create a simplified version instead:

1. Copy passable sections of the PG .sql file to
   `tests/regress_simple/sql/<name>.sql`.
2. Avoid: INSERT...SELECT, EXPLAIN, BEGIN/ROLLBACK, SET/SHOW, PREPARE,
   CREATE FUNCTION/VIEW/SCHEMA/DOMAIN/TYPE/OPERATOR, DO $$ blocks,
   binary/hex/octal literals, `float` (use `double precision` instead).
3. Run through PepperDB to generate actual output, verify it matches PG's
   expected output for those queries, then save as
   `tests/regress_simple/expected/<name>.out`.
4. Add the test name to `simple_test!()` in `tests/regress_simple.rs`.

### Step 3: Fix from easiest to hardest

Work through mismatches in order of difficulty. After each fix, re-run the
test to see remaining gaps. Common fix patterns:

**Translating DataFusion errors** -- In `df_to_pg()`, match the DataFusion
error string and rewrite to PG's exact wording. Extract the column/table name
and compute cursor position with `find_column_position()`.

**SQL rewriting** -- When DataFusion rejects valid PG SQL, parse with
sqlparser, modify the AST, and re-serialize before sending to DataFusion.
Add the rewrite function and call it from `execute_select_df()`.

**Type/function support** -- If the test needs a type or function PepperDB
doesn't support, evaluate whether it's a small addition or a large project.
If large, pick a different test instead.

### Step 4: Verify

```bash
cargo test <test_name> -- --nocapture   # The new test passes
cargo test                               # All existing tests still pass
cargo clippy                             # No warnings
cargo fmt -- --check                     # Formatting clean
```

## Phase 3: Common Blockers Reference

These are the most frequent blockers across the PG test suite, ordered by
how many tests they affect:

1. **Pre-existing tables** (test_setup.sql) -- many tests assume tables like
   `INT4_TBL`, `FLOAT8_TBL` exist. Enabling test_setup.sql would unblock
   dozens of tests but is a large effort.

2. **PG-specific functions** -- `pg_input_is_valid()`, `booleq()`,
   `pg_typeof()`, `currval()`, etc. Each would need a custom DataFusion UDF.

3. **Error position reporting** -- `LINE N:` + `^` pointer. Now implemented
   in `format_error_with_position()` (tests/regress.rs) and
   `find_column_position()` (src/executor/mod.rs).

4. **psql meta-commands** -- `\pset`, `\d`, `\copy`. Would need test harness
   changes.

5. **Transactions** -- `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`.

6. **PL/pgSQL** -- `DO $$...$$`, `CREATE FUNCTION ... LANGUAGE plpgsql`.

7. **Missing types** -- OID, NAME, MONEY, UUID, INTERVAL, NUMERIC with
   precision, arrays.

## Key Files

| File | Role |
|------|------|
| `tests/common/mod.rs` | Shared harness: SQL runner, psql output formatting, error formatting |
| `tests/regress.rs` | Full PG test runner (uses `regress_test!()` macro) |
| `tests/regress_simple.rs` | Simplified PG test runner (uses `simple_test!()` macro) |
| `tests/regress_simple/sql/*.sql` | Simplified test inputs (curated subsets) |
| `tests/regress_simple/expected/*.out` | Simplified expected outputs |
| `src/executor/mod.rs` | Query executor: DataFusion bridge, error translation, SQL rewriting |
| `src/parser/mod.rs` | DDL/DML parser (sqlparser-rs AST to PepperDB AST) |
| `postgres/src/test/regress/sql/*.sql` | PG test inputs (via symlink) |
| `postgres/src/test/regress/expected/*.out` | PG expected outputs (via symlink) |

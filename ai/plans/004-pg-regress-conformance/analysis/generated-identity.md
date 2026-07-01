# Generated columns + identity columns -- step breakdown for plan 004 (candidate addition)

Status: read-only research. No code changed. This is a proposal to fold into
plan 004 (currently registered as an unscheduled line item in
`ai/plans/004-pg-regress-conformance/file-list/23.txt`: "generated / identity
columns # generated_*, identity, fast_default"), not an executed step.

## TL;DR

Generated columns (STORED + VIRTUAL) and identity columns are absent at
**every** layer of PepperDB: grammar (no `GENERATED`/`IDENTITY`/`STORED`/
`VIRTUAL` tokens, and `ColumnDef` doesn't even parse inline `DEFAULT`/`CHECK`/
`NOT NULL` yet), catalog (`pg_attrdef` is missing `adgenerated`; `pg_attribute`
has the `attgenerated`/`attidentity`/`atthasmissing`/`attmissingval` fields but
they are always zeroed dead code), commands (`tablecmds.rs` has zero
generated/identity logic and **panics** on `ADD COLUMN ... DEFAULT`), and
executor (`nodeModifyTable.rs` has no stored-generated computation hook).
Sequences and a (very limited, integer-literal-only) DEFAULT-expansion path
are the only working pieces to build on.

Unlike SSI (see `ai/tmp/regress-expansion/ssi.md`), this feature has **real,
substantial pg_regress leverage**: 4 dedicated test files (`generated_stored`,
`generated_virtual`, `identity`, `fast_default`, ~2,893 SQL lines combined) are
in PostgreSQL's `parallel_schedule` and currently blocked entirely. However,
two of those four files (`generated_stored`/`generated_virtual` use
`information_schema.columns`; `fast_default` uses 4 PL/pgSQL helper
functions) have **cross-cutting blockers beyond this feature's own scope**
that must be flagged honestly rather than silently deferred.

## 1. PG source: what generated/identity columns are

### Catalog representation
`src/include/catalog/pg_attribute.h` (`ref/postgres`), fixed-size portion:
- `atthasmissing: bool` (~line 129) -- has a fast-default missing value
- `attidentity: char` (~line 132) -- `'\0'` none, `'a'` ALWAYS, `'d'` BY DEFAULT
- `attgenerated: char` (~line 135) -- `'\0'` none, `'s'` STORED, `'v'` VIRTUAL
- `attmissingval: anyarray` (~line 184, variable-length section) -- one-element
  array holding the fast-default value

Constants: `ATTRIBUTE_IDENTITY_ALWAYS='a'`, `ATTRIBUTE_IDENTITY_BY_DEFAULT='d'`,
`ATTRIBUTE_GENERATED_STORED='s'`, `ATTRIBUTE_GENERATED_VIRTUAL='v'`.

`pg_attrdef.h`: **no `adgenerated` column** in PG 18 -- only `oid`, `adrelid`,
`adnum`, `adbin`. Whether an entry is a plain default, a stored-generation
expression, or a virtual-generation expression is determined purely by
looking up `pg_attribute.attgenerated` for that `(adrelid, adnum)`.

### heap.c (catalog layer)
- `cookDefault()` (`catalog/heap.c:3337-3403`, ~67 lines) -- the choke point
  that transforms a raw default/generation expression. Takes `char
  attgenerated`: non-zero uses `EXPR_KIND_GENERATED_COLUMN`, disallows
  references to other generated columns (`check_nested_generated`), rejects
  mutable functions, and for VIRTUAL additionally calls
  `check_virtual_generated_security`.
- `AddRelationNewConstraints()` (`heap.c:2399-2712`, ~314 lines) -- driver
  called from `DefineRelation` (CREATE TABLE) and `ATExecAddColumn` (ALTER
  TABLE ADD COLUMN). Calls `cookDefault()` then `StoreAttrDefault()` per
  `RawColumnDefault`. Stored-vs-virtual is baked into `attgenerated` already
  set by the parser, not decided here.
- `StoreConstraints()` (`heap.c:2324-2366`, ~43 lines) -- cooked-constraint
  counterpart (LIKE/inheritance).
- `StoreAttrDefault()` (`catalog/pg_attrdef.c:35-142`, ~108 lines) -- inserts
  the `pg_attrdef` row, flips `atthasdef=true`. Line ~121:
  `recordDependencyOn(&defobject, &colobject, attgenerated ?
  DEPENDENCY_INTERNAL : DEPENDENCY_AUTO)` -- generated expressions get an
  INTERNAL dependency (can't drop independently); plain defaults get AUTO.
- `attgenerated`/`attidentity` themselves are set earlier, in
  `BuildDescForRelation`/`transformColumnDefinition` (parse_utilcmd.c) from
  the parsed `ColumnDef->generated`/`ColumnDef->identity` fields.

### tablecmds.c (ALTER TABLE layer)
- **`ATExecAddColumn`** (`tablecmds.c:7217-7636`, ~420 lines total; the
  generated/identity/fast-default logic is concentrated at 7413-7580):
  - Line ~7387: `CheckAttributeType(..., attgenerated == VIRTUAL ?
    CHKATYPE_IS_VIRTUAL : 0)` -- virtual columns get a distinct type-check mode.
  - Lines ~7413-7431: builds a `RawColumnDefault`, calls
    `AddRelationNewConstraints` (uniform across identity/generated/plain).
  - Lines ~7470-7484: **identity special case** -- manually builds a
    `NextValueExpr` pointing at `colDef->identitySequence` (can't use the
    normal default-building path because sequence ownership isn't wired yet).
  - **Fast-default path** (lines ~7526-7569): after computing `defval`, PG
    tries to skip the rewrite by evaluating the default once and calling
    `StoreAttrMissingVal(rel, attnum, missingval)`. Only attempted when:
    `relkind == RELKIND_RELATION`, `!colDef->generated` (generated columns are
    explicitly excluded), no domain constraints, expression not volatile.
    Otherwise falls back to `tab->rewrite |= AT_REWRITE_DEFAULT_VAL` -- except
    virtual generated columns never need a rewrite (nothing stored on disk).
- **`ATExecAddIdentity`** (`tablecmds.c:8240-8363`, ~124 lines) -- validates
  column is NOT NULL, not already identity, has no existing default; sets
  `attidentity = cdef->identity`; recurses to partitions.
- **`ATExecSetIdentity`** (`tablecmds.c:8371-8480`, ~110 lines) -- `SET
  GENERATED {ALWAYS|BY DEFAULT}` / sequence-option changes.
- **`ATExecDropIdentity`** (`tablecmds.c:8488-~8598`, ~110 lines) -- clears
  `attidentity`, drops the internal backing sequence via
  `getIdentitySequence` + `performDeletion`.

### parse_utilcmd.c (identity sequence creation)
**`generateSerialExtraStmts`** (`parse_utilcmd.c:390-583`, ~194 lines),
shared by SERIAL and `GENERATED ... AS IDENTITY` (`for_identity` flag):
1. Picks sequence namespace/name (explicit `SEQUENCE NAME`, or
   `ChooseRelationName(table, column, "seq", ...)`).
2. Builds a `CreateSeqStmt` (`for_identity=true`), prepends to `cxt->blist`
   (runs before the CREATE/ALTER TABLE).
3. Stores the chosen `RangeVar` on `column->identitySequence` so
   `ATExecAddColumn` can build the `NextValueExpr` default before ownership
   exists.
4. Builds an `AlterSeqStmt` with `DefElem("owned_by", [schema, table,
   column])`, appended to `blist` (existing column) or `alist` (new column).
   This is what creates the `pg_depend` internal dependency linking sequence
   to column -- dropping the column drops the sequence.

Called from `transformColumnDefinition` (both the SERIAL path and the
`CONSTR_IDENTITY` case) and from ALTER TABLE ADD COLUMN's pre-transform.

### nodeModifyTable.c (executor -- STORED generated computation)
- **`ExecComputeStoredGenerated`** (`nodeModifyTable.c:544-628`, 85 lines),
  called from `ExecInsert` (line ~935), `ExecUpdate` (line ~1062), MERGE
  (line ~2152). Lazily inits per-command `ExprState*` arrays
  (`ri_GeneratedExprsI`/`U`), early-exits if nothing to compute. Single pass
  over all `natts`: evaluates each generated column's cataloged expression
  against the current slot, copies non-generated attributes through
  unchanged, re-stores via `ExecStoreVirtualTuple` + `ExecMaterializeSlot`.
  Ordering-independence is safe because `check_nested_generated` (heap.c)
  already forbids a generated column from referencing another generated
  column.
- **`ExecInitGenerated`** (`nodeModifyTable.c:430-538`, ~108 lines) -- builds
  one `ExprState` per attribute needing computation via `ExecPrepareExpr`,
  skips virtual-generated columns (nothing to compute at write time), tracks
  a needed-count for the cheap early-exit above.

### Virtual generated columns -- NOT computed in the executor
- On INSERT/UPDATE, **`rewriteTargetListIU`** (`rewriteHandler.c:808-1071`,
  ~264 lines; generated/identity-specific block at 894-1029, ~136 lines)
  rejects any non-DEFAULT value supplied for a generated column (stored or
  virtual), and fills a virtual generated column's targetlist slot with a
  `Const NULL` placeholder -- virtual columns store nothing on disk.
- On SELECT/read, **`expand_virtual_generated_columns`**
  (`optimizer/prep/prepjointree.c:968-~1080`, ~110 lines), called once per
  query from `planner.c:773` during jointree preprocessing, rewrites every
  `Var` reference to a virtual generated column throughout the query tree
  into the actual generation expression (via `build_generation_expression`).
  This is "inline macro expansion at plan time," not materialization.
- **`expand_generated_columns_in_expr`** (`rewriteHandler.c:4546-4578`, ~33
  lines) + **`get_generated_columns`** (`rewriteHandler.c:4507-4538`, ~32
  lines) -- narrower entry point for standalone expressions outside a full
  Query (index predicates, etc.).
- **`build_generation_expression`** (`rewriteHandler.c:4585-4622`, ~38 lines)
  -- shared helper: fetches the cataloged expression, wraps in `CollateExpr`
  if collation differs.

### Identity execution + OVERRIDING
- `NextValueExpr` (built at ADD-COLUMN/CREATE-TABLE time) is evaluated by
  `ExecEvalNextValueExpr` (`execExprInterp.c`), which calls
  `nextval_internal(seqid, false)` directly (bypasses permission checks --
  internal system call).
- `nextval_internal` (`commands/sequence.c:623-~865`, ~240 lines, shared with
  plain sequences) is the real sequence-advance implementation.
- `OVERRIDING SYSTEM VALUE` / `OVERRIDING USER VALUE`: parsed into
  `InsertStmt->override` (an `OverridingKind` enum), threaded through
  `transformInsertStmt` (analyze.c) into `Query->override`. Enforcement lives
  in `rewriteTargetListIU`'s identity/generated block (rewriteHandler.c
  894-1029): `ATTRIBUTE_IDENTITY_ALWAYS` rejects a supplied value unless
  `OVERRIDING SYSTEM VALUE`; `OVERRIDING USER VALUE` forces the identity
  default to apply even for BY DEFAULT columns; UPDATE never allows
  overriding identity/generated columns regardless of OVERRIDING.

### Size summary

| Mechanism | File | Lines | Size |
|---|---|---|---|
| `cookDefault` | heap.c | 3337-3403 | ~67 |
| `AddRelationNewConstraints` | heap.c | 2399-2712 | ~314 (mostly shared w/ plain DEFAULT/CHECK) |
| `StoreConstraints` | heap.c | 2324-2366 | ~43 |
| `StoreAttrDefault` | pg_attrdef.c | 35-142 | ~108 |
| `ATExecAddColumn` (generated/identity/fast-default subset) | tablecmds.c | 7217-7636 (subset ~7387-7580) | ~150 of ~420 |
| `ATExecAddIdentity` | tablecmds.c | 8240-8363 | ~124 |
| `ATExecSetIdentity` | tablecmds.c | 8371-8480 | ~110 |
| `ATExecDropIdentity` | tablecmds.c | 8488-~8598 | ~110 |
| `generateSerialExtraStmts` | parse_utilcmd.c | 390-583 | ~194 |
| `ExecComputeStoredGenerated` | nodeModifyTable.c | 544-628 | ~85 |
| `ExecInitGenerated` | nodeModifyTable.c | 430-538 | ~108 |
| `expand_virtual_generated_columns` | prepjointree.c | 968-~1080 | ~110 |
| `expand_generated_columns_in_expr`+`get_generated_columns` | rewriteHandler.c | 4507-4578 | ~70 |
| `build_generation_expression` | rewriteHandler.c | 4585-4622 | ~38 |
| `rewriteTargetListIU` (identity/generated + OVERRIDING subset) | rewriteHandler.c | 808-1071 (subset 894-1029) | ~136 of ~264 |
| `nextval_internal` | sequence.c | 623-~865 | ~240 (shared, not identity-specific) |

Genuinely novel generated/identity-specific logic (excluding shared plain
DEFAULT/CHECK/sequence machinery): roughly **900-1,000 lines** across 6 files.

## 2. PepperDB status (grepped, not assumed)

- **Catalog structs**: `src/catalog/pg_attribute.rs` (`FormData_pg_attribute`,
  lines 15-42) has all four fields (`atthasmissing: bool` line 29,
  `attidentity: i8` line 30, `attgenerated: i8` line 31, `attmissingval:
  Anyarray` line 41) -- but `FormData_pg_attribute::new()` (lines 57-90)
  hardcodes all four to their empty/zero/false values, and every write site
  (`src/backend/catalog/heap.rs:353-354`, `src/backend/commands/tablecmds.rs:
  681-683,692`) also hardcodes them. The in-memory `CompactAttribute`
  (`src/access/tupdesc.rs:43-61`) mirrors the same always-zero pattern. **The
  fields exist but are fully dead plumbing, never populated.**
- `src/catalog/pg_attrdef.rs` (`FormData_pg_attrdef`, lines 24-30): only
  `oid`, `adrelid`, `adnum`, `adbin` -- **no `adgenerated` field at all**
  (matches PG's actual layout, so this is expected, not a gap vs PG -- but
  the "is this a plain default or a generated expression" distinction still
  needs `attgenerated` populated, which it isn't). All functions
  (`StoreAttrDefault`, `RemoveAttrDefault`, `GetAttrDefaultOid`, etc., lines
  41-65) are `unimplemented!()` stubs.
  `src/catalog/heap.rs`'s `RawColumnDefault` struct (lines 40-44) already
  carries a `generated: u8` field, and `AddRelationNewConstraints`/
  `cookDefault` accept a `_attgenerated: u8` parameter -- but those functions
  are themselves `unimplemented!()` (lines 108-150).
- **tablecmds.rs**: grep for generated/identity/stored/virtual (case
  insensitive) matches only the dead-field writes above -- **zero
  parsing/handling logic**. `ata_exec_add_column` (line 617) at lines 627-628
  calls `not_yet_reachable("ATExecAddColumn: column DEFAULT on add")`
  whenever `coldef.raw_default.is_some()` -- **`ADD COLUMN ... DEFAULT`
  panics today**, it does not fall back to a full rewrite and there is no
  fast-default path to remove or extend; the feature is simply unimplemented.
  Plain ADD COLUMN without a default works (lines 617-700) and always writes
  `atthasmissing=false`/null `attmissingval`. `ata_exec_drop_column` (line
  706) and `ata_exec_column_default` (line 775, `ALTER COLUMN SET/DROP
  DEFAULT`) exist and use `pg_attrdef` for storage but store only deparsed
  text, not cooked expressions, with no generated/identity awareness.
- **Parser/grammar**: `src/backend/parser/gram.lalrpop` and
  `src/parser/kwlist.rs` -- no `GENERATED`/`ALWAYS`/`IDENTITY`/`STORED`/
  `VIRTUAL` tokens anywhere. More fundamentally, `ColumnDef` (gram.lalrpop
  lines ~608-615) only supports `name type` or `name type ColRefConstraint`,
  where `ColRefConstraint` (lines ~620-624) is **REFERENCES only** -- there is
  no `ColQualList`/inline-constraint machinery for `DEFAULT`/`NOT NULL`/
  `CHECK` in CREATE TABLE at all yet (comment at line 606 flags this as
  future growth). `DEFAULT` is currently only reachable via the separate
  `ALTER TABLE ... SET DEFAULT` rule. **GENERATED/IDENTITY have no grammar
  runway to attach to -- this is a bigger prerequisite than adding two
  keywords.**
- **nodeModifyTable.rs**: `exec_insert` (line 355), `exec_update` (line 374),
  driver `exec_modify_table` (line 164). No hits for "generated" anywhere --
  no hook point exists. The natural insertion point is between target-list
  evaluation (currently happens upstream in `preptlist.rs`'s
  `expand_insert_targetlist`, not in the executor) and the heap write inside
  `exec_insert`/`exec_update`.
- **Sequences**: fully implemented, two layers. `src/backend/commands/
  sequence.rs`: `pub async fn nextval(shared: &Arc<SharedState>, seqrelid:
  Oid) -> i64` (line 300), `currval` (line 340), `setval` (line 357).
  `src/commands/sequence.rs`: `pub fn nextval_internal(relid: Oid,
  check_permissions: bool) -> i64` (line 48), SQL-callable `nextval` wrapper
  (line 53). **Ready-made API for identity columns to call.**
- **DEFAULT expansion (plan-003 step 39A)**: `src/backend/optimizer/prep/
  preptlist.rs`, `expand_insert_targetlist` (dispatch line 46, body
  96-134) walks the tupdesc attno-by-attno; an omitted column calls
  `build_insert_default(tupdesc, attno, atttypid)` (line 121, body
  ~142-181). Its doc comment (lines 136-141) is explicit: it only handles a
  **bare integer literal** deparsed default; anything else (any non-trivial
  expression, and by extension any generated/identity default) falls back to
  a **NULL constant**. This mechanism must be extended (to evaluate cooked
  expression trees / call nextval) or bypassed entirely for both features.
- **Regress tests**: no PepperDB-side `tests/regress` directory exists at
  all; the only `generated_stored.sql`/`generated_virtual.sql`/`identity.sql`/
  `fast_default.sql` files are the pristine originals under
  `ref/postgres/src/test/regress/sql/`. `ai/plans/004-pg-regress-conformance/
  file-list/23.txt` line 18 already flags this as a known, unscheduled
  batch. No `ai/tmp/regress-analysis/batch-generated*.md`,
  `batch-identity.md`, or `batch-fast_default.md` exist. `insert.sql` (which
  does contain some bare-DEFAULT INSERT tests, confirmed via
  `ai/tmp/regress-analysis/batch-insert.md`) is already `OUT_OF_SCOPE` for
  unrelated reasons (declarative partitioning dominates the file); its
  DEFAULT-related lines are not currently a distinguished blocker for this
  feature. `alter_table.sql` (3,163 lines) contains only 1 line mentioning
  GENERATED/IDENTITY -- negligible overlap, not a real dependency of this work.

**Bottom line**: generated and identity columns are unimplemented at the
grammar, catalog, command, and executor layers. Sequences and a narrow
DEFAULT-expansion path are the only working foundations. Fast-default
(`attmissingval`) is not merely "missing the fast path" -- `ADD COLUMN ...
DEFAULT` of any kind panics today, so this step must build ADD-COLUMN-with-
DEFAULT from scratch, with the fast (no-rewrite) semantics as the target
behavior rather than a later optimization.

## 3. Tests unblocked -- honest count, with cross-cutting caveats

Cross-referenced `ref/postgres/src/test/regress/parallel_schedule` (all four
files are listed: `identity` + `generated_stored` in one parallel group,
`generated_virtual` in another, `fast_default` in its own late group) and the
raw SQL files.

| Test | Lines | Needs beyond generated/identity itself |
|---|---|---|
| `generated_stored` | 779 | `information_schema.columns` (is_generated/ generation_expression cols), `information_schema.column_column_usage`, `\d` (psql describe), 3 `LANGUAGE plpgsql` helper calls |
| `generated_virtual` | 946 | Same as above (explicitly "keep aligned with generated_stored.sql") |
| `identity` | 540 | `information_schema.columns` (is_identity/identity_* cols), `information_schema.sequences`, `pg_get_serial_sequence()`, `\d` |
| `fast_default` | 628 | 4 `LANGUAGE 'plpgsql'` helper functions (`set`, `comp`, plus 2 more) used pervasively to detect table rewrite via `relfilenode` comparison -- **this file cannot pass at all without PL/pgSQL function-body execution**, which is a separate, already-registered XL blocker (`file-list/23.txt`: "PL/pgSQL function-body EXECUTION # ~45 tests") |

Grepped confirmations: `grep -rln information_schema src/` finds only
incidental hits in `bootstrap.rs`/`vacuum.rs` (comments/unrelated), no actual
`information_schema` view implementation; `grep -rln pg_get_serial_sequence
src/` returns nothing; `grep -c "LANGUAGE 'plpgsql'\|LANGUAGE plpgsql"
fast_default.sql` = 4; same grep on `generated_stored.sql`/
`generated_virtual.sql` = 3 each (used for `check_virtual_generated_security`-
style checks and incidental helper functions, less pervasive than
`fast_default`'s core reliance).

**Count: 4 test files / ~2,893 SQL lines nominally targeted.** Honest
achievable count without also doing PL/pgSQL and `information_schema` work:
- `identity`: blocked on `information_schema.columns`/`.sequences` +
  `pg_get_serial_sequence()` for the introspection queries, but the DDL/DML
  body (CREATE TABLE ... GENERATED AS IDENTITY, ALTER TABLE ADD/SET/DROP
  IDENTITY, OVERRIDING, sequence advancement) is independently testable and
  is the bulk of the file's ~540 lines. Realistic: KNOWN-DIFF on the
  information_schema queries, PASS-able on everything else once
  `information_schema.columns` ships (a small, focused view -- not
  necessarily the whole `information_schema` schema).
  `information_schema.sequences` is currently unimplemented; if kept
  minimal, either add both narrow views as part of this work or KNOWN-DIFF
  those specific queries.
- `generated_stored`/`generated_virtual`: same `information_schema.columns`
  dependency, plus `column_column_usage` (a second, more obscure
  information_schema view --- lower priority, KNOWN-DIFF candidate); the
  bulk of both files (generation-expression semantics, error cases, STORED
  vs VIRTUAL divergence, `\d` output) is independently achievable.
- `fast_default`: **cannot pass end-to-end without PL/pgSQL.** Recommend
  treating this file as KNOWN-DIFF / partially-out-of-scope until PL/pgSQL
  lands, OR writing a PepperDB-side substitute harness query (not touching
  the upstream `.sql` file, since plan 004's ground rule 2 requires diffing
  against upstream `expected/*.out` verbatim) is not viable -- the file
  genuinely needs PL/pgSQL. The `attmissingval`/fast-default mechanism
  itself (ADD COLUMN ... DEFAULT without full rewrite) is still worth
  implementing for its own correctness value and to unblock the DEFAULT
  lines in `insert.sql`/`alter_table.sql`, even if `fast_default.sql` itself
  stays KNOWN-DIFF/blocked pending PL/pgSQL.

Recommend the plan-004 owner treat `information_schema.columns` (narrow, just
the columns these two test files query) as an in-scope micro-dependency of
this step, not a separate step -- it is small and load-bearing for the
majority of assertions in 3 of the 4 files.

## 4. Step breakdown (for a fresh code agent)

Numbered as a self-contained sub-plan; if merged into plan 004 these would
become a new step (after step 22, or as step 23's first fleshed-out entry,
per the project's existing convention for XL registered work -- see
`ai/tmp/regress-expansion/ssi.md` for the sibling precedent). Re-verify exact
line numbers/paths before editing, per plan 004's own workflow note (source
has likely shifted since this research).

Dependencies common to all steps: plan-003 tablecmds (`src/backend/commands/
tablecmds.rs`), nodeModifyTable (`src/backend/executor/nodeModifyTable.rs`),
sequences (`src/backend/commands/sequence.rs` + `src/commands/sequence.rs`),
DEFAULT-expansion (`src/backend/optimizer/prep/preptlist.rs`).

### Step 1 -- Grammar: inline column constraints (DEFAULT/NOT NULL/CHECK/GENERATED/IDENTITY)
- PG files: `gram.y` `ColumnDef`/`ColQualList`/`ColConstraint`/
  `ColConstraintElem` rules (search for `GENERATED`/`STORED`/`b_expr AS`/
  `IDENTITY_P` in gram.y), `kwlist.h` (add `GENERATED`, `IDENTITY`, `STORED`).
  Note: `VIRTUAL` is parsed as an unreserved keyword only in the generated-
  column context (not a true keyword in kwlist.h at all in some PG versions
  -- verify against ref/postgres kwlist.h directly before assuming a new
  token is needed).
- Target files: `src/backend/parser/gram.lalrpop` (replace the current
  `name type` / `name type ColRefConstraint`-only `ColumnDef` rule at
  ~608-624 with a real `ColQualList` that supports repeatable
  `DEFAULT expr`, `NOT NULL`, `NULL`, `CHECK (expr)`, `GENERATED ALWAYS AS
  (expr) {STORED|VIRTUAL}`, `GENERATED {ALWAYS|BY DEFAULT} AS IDENTITY
  [(SequenceOptions)]`, `REFERENCES ...`), `src/parser/kwlist.rs` (add
  `GENERATED`, `ALWAYS`, `IDENTITY`, `STORED` as keywords -- check reserved
  vs unreserved category against PG's kwlist.h), AST node for `ColumnDef`
  (wherever `ColumnDef`/`Constraint` nodes live, likely
  `src/nodes/parsenodes.rs` -- add `generated: u8`, `identity: u8`,
  `identity_sequence: Option<...>` fields mirroring PG's `ColumnDef`).
- Deliverable: `CREATE TABLE t (a int GENERATED ALWAYS AS (expr) STORED, b
  int GENERATED ALWAYS AS IDENTITY)` parses into an AST with the right
  fields populated; does not yet execute (semantic layer untouched).
- Deps: none beyond existing gram.lalrpop infrastructure. This step is a
  hard prerequisite for every step below -- it is also a prerequisite for
  *any* future work needing inline `DEFAULT`/`CHECK`/`NOT NULL` in CREATE
  TABLE, so it has value beyond this feature.
- Tests unblocked: none directly (parse-only).
- Effort: L (grammar surgery on `ColumnDef` is more invasive than it looks --
  PG's real `ColQualList` is one of the larger rule clusters in gram.y;
  budget for this being the single riskiest step in the whole breakdown
  given plan 004's grammar is a lalrpop translation, not a bison one-to-one
  port).

### Step 2 -- Catalog: wire attgenerated/attidentity/atthasmissing through CREATE TABLE
- PG files: `heap.c` `cookDefault` (3337-3403), `AddRelationNewConstraints`
  (2399-2712), `StoreConstraints` (2324-2366), `pg_attrdef.c`
  `StoreAttrDefault` (35-142, esp. the `DEPENDENCY_INTERNAL` vs
  `DEPENDENCY_AUTO` branch at ~121).
- Target files: `src/catalog/heap.rs` (implement `cookDefault`/
  `AddRelationNewConstraints`/`StoreConstraints` -- currently
  `unimplemented!()` at lines 108-150; the `RawColumnDefault.generated: u8`
  field already exists, wire it through instead of discarding), `src/catalog/
  pg_attrdef.rs` (implement `StoreAttrDefault`/`RemoveAttrDefault`/
  `GetAttrDefaultOid`, currently stubs at lines 41-65; record the
  INTERNAL-vs-AUTO pg_depend distinction), `src/catalog/pg_attribute.rs`
  (stop hardcoding `atthasmissing`/`attidentity`/`attgenerated`/
  `attmissingval` to zero in `FormData_pg_attribute::new()` -- thread real
  values from the `ColumnDef` built in Step 1), `src/access/tupdesc.rs`
  (same for `CompactAttribute`, lines 43-61).
- Deliverable: `CREATE TABLE` with a STORED/VIRTUAL generated column or an
  IDENTITY column correctly populates `pg_attribute.attgenerated`/
  `attidentity` and creates the right `pg_attrdef`/`pg_depend` rows; readable
  back via direct catalog queries (`SELECT attgenerated FROM pg_attribute`,
  matching `identity.sql`'s own sanity-check query at line 2).
- Deps: Step 1 (needs the AST fields to read from).
- Tests unblocked: none fully yet (still needs executor + information_schema
  for end-to-end pass), but unblocks the catalog sanity-check line at the
  top of `identity.sql` and is the load-bearing dependency for every
  subsequent step.
- Effort: L (heap.rs/pg_attrdef.rs are currently pure stubs -- this is
  greenfield implementation of ~300-400 lines of real logic, not a small
  patch, even though individual PG functions are modest).

### Step 3 -- Identity: backing sequence creation + ownership link
- PG files: `parse_utilcmd.c` `generateSerialExtraStmts` (390-583), the
  `CONSTR_IDENTITY` dispatch in `transformColumnDefinition` (~line 856).
- Target files: wherever PepperDB's `transformColumnDefinition`-equivalent
  lives (likely `src/backend/parser/parse_utilcmd.rs` -- verify; may need
  the same treatment as `AddRelationNewConstraints` if currently a stub).
  Generate a synthetic `CREATE SEQUENCE` (reuse `src/backend/commands/
  sequence.rs`'s existing create path) plus an `ALTER SEQUENCE ... OWNED
  BY` before/after the CREATE/ALTER TABLE, exactly mirroring PG's
  blist/alist staging. Store the chosen sequence relid/name on the
  column-def structure for Step 4 to consume.
- Deliverable: `CREATE TABLE t (a int GENERATED ALWAYS AS IDENTITY)`
  transactionally creates both the table and an internally-owned sequence,
  linked via `pg_depend` (DEPENDENCY_INTERNAL), discoverable via
  `pg_get_serial_sequence` (Step 6) and dropped automatically when the
  column/table is dropped.
- Deps: Step 2 (attidentity must already be settable);
  existing sequence commands (`src/backend/commands/sequence.rs`).
- Tests unblocked: none alone; feeds Step 4.
- Effort: M.

### Step 4 -- Executor: NextValueExpr evaluation + OVERRIDING + fast-default (attmissingval)
- PG files: `ATExecAddColumn`'s identity/fast-default subset (tablecmds.c
  ~7413-7580), `ExecEvalNextValueExpr` (execExprInterp.c),
  `rewriteTargetListIU`'s identity/generated + OVERRIDING block
  (rewriteHandler.c 894-1029).
- Target files: `src/backend/optimizer/prep/preptlist.rs`
  (`build_insert_default`, currently only handles bare-integer-literal
  defaults at lines 136-181 -- extend to evaluate a `NextValueExpr` node by
  calling `src/commands/sequence.rs::nextval_internal`, and to evaluate
  cooked generation/default expression trees generally rather than falling
  back to NULL), `src/backend/commands/tablecmds.rs`
  (`ata_exec_add_column`, currently panics via `not_yet_reachable` at
  627-628 whenever `raw_default.is_some()` -- replace with: cook the
  default, and if `!generated && !volatile && plain table`, call
  `StoreAttrMissingVal`-equivalent to set `atthasmissing`/`attmissingval`
  instead of rewriting; this is the "fast default" feature itself),
  wherever INSERT's `OVERRIDING SYSTEM/USER VALUE` clause would be threaded
  (parser AST + `src/backend/parser/analyze.rs`'s `transformInsertStmt`
  equivalent) -- enforce ALWAYS-identity/generated-column value rejection
  and OVERRIDING semantics at the same layer PG does (query rewrite /
  targetlist-fixup stage).
- Deliverable: `ALTER TABLE t ADD COLUMN b int DEFAULT 5` no longer panics
  and avoids a full table rewrite (attmissingval fast path);
  `INSERT INTO itest1 VALUES (DEFAULT, 'x')` and `OVERRIDING SYSTEM VALUE`
  work against identity columns; reading a fast-defaulted column via heap
  scan returns the missing value for old rows without rewriting them (heap
  read path must consult `atthasmissing`/`attmissingval` when `natts` on
  disk < `natts` in the current tupdesc -- verify this fallback exists in
  the tuple deform code, likely `src/access/heap/heaptuple.rs`'s
  `heap_deform_tuple`, and add it if missing).
- Deps: Steps 2, 3; existing DEFAULT-expansion path (preptlist.rs).
- Tests unblocked: the DDL/DML bulk of `identity.sql` (~540 lines minus the
  information_schema queries handled in Step 6); the `attmissingval`
  behavioral core of `fast_default.sql` (though the file's PASS/FAIL
  verdict overall stays gated on PL/pgSQL per section 3).
- Effort: L (this is the step with the most genuinely new, non-mechanical
  logic: heap tuple-deform fallback for missing older columns is a
  behavioral change to a hot path, not just new code at the edges).

### Step 5 -- Executor: ExecComputeStoredGenerated (INSERT/UPDATE) + virtual-column read expansion
- PG files: `ExecComputeStoredGenerated`/`ExecInitGenerated`
  (nodeModifyTable.c 430-628), `expand_virtual_generated_columns`
  (prepjointree.c 968-~1080), `build_generation_expression`
  (rewriteHandler.c 4585-4622).
- Target files: `src/backend/executor/nodeModifyTable.rs` (new function
  mirroring `ExecComputeStoredGenerated`, called from `exec_insert` (line
  355) and `exec_update` (line 374) right before the heap write -- lazily
  build one `ExprState` per STORED-generated attribute from its cataloged
  `pg_attrdef` expression, evaluate in a single pass over the slot, copy
  non-generated attributes through, re-store), `src/backend/optimizer/prep/`
  (new file or addition mirroring `expand_virtual_generated_columns` --
  find/add the planner's query-preprocessing entry point equivalent to
  `planner.c:773` and rewrite `Var` references to virtual-generated columns
  into their expressions across the query tree for SELECT).
- Deliverable: `INSERT`/`UPDATE` on a table with STORED generated columns
  correctly computes and stores the generated value; `SELECT` against a
  table with VIRTUAL generated columns transparently substitutes the
  generation expression (column stores nothing on disk, matching PG's
  "nothing physically stored" semantics for VIRTUAL).
- Deps: Step 2 (attgenerated must be set + pg_attrdef expression stored);
  Step 1's grammar for `GENERATED ... AS (...) STORED/VIRTUAL`.
- Tests unblocked: the generation-semantics bulk of `generated_stored.sql`/
  `generated_virtual.sql` (error cases at gtest_err_*, STORED-vs-VIRTUAL
  behavioral divergence, self-reference/nested-generated-column rejection
  via a `check_nested_generated`-equivalent validation added alongside
  `cookDefault` in Step 2).
- Deps note: `check_nested_generated`/`check_virtual_generated_security`
  validation logic belongs in Step 2's `cookDefault` port, not here --
  cross-reference back if Step 2 skipped it for schedule reasons.
- Effort: L.

### Step 6 -- information_schema.columns (+ .sequences) narrow views + pg_get_serial_sequence
- PG files: `src/backend/catalog/information_schema.sql` (the view
  definitions for `columns`, `sequences` -- these are plain SQL views over
  `pg_attribute`/`pg_attrdef`/`pg_class`, not C code), `utils/adt/ruleutils.c`
  `pg_get_serial_sequence` (~a few hundred lines including SQL-callable
  wrapper).
- Target files: wherever PepperDB seeds bootstrap/system views (check
  `src/backend/bootstrap/bootstrap.rs`, since it's the only file the earlier
  grep for "information_schema" touched -- likely just a comment/TODO
  today, verify). Add minimal `information_schema.columns` and
  `information_schema.sequences` views (only the columns
  `identity`/`generated_*` tests actually query: `is_generated`,
  `generation_expression`, `is_identity`, `identity_generation`,
  `identity_start`, `identity_increment`, `identity_maximum`,
  `identity_minimum`, `identity_cycle`, `column_default`, `is_nullable`) --
  do not attempt the full `information_schema` schema. Add
  `pg_get_serial_sequence(regclass, text) -> text` as a new builtin function
  (small, catalog-lookup-only, no C-side complexity) in
  `src/backend/utils/adt/ruleutils.rs` or wherever builtin functions are
  registered.
- Deliverable: `identity.sql`'s and `generated_{stored,virtual}.sql`'s
  `information_schema` introspection queries return correct rows;
  `pg_get_serial_sequence('itest1', 'a')` resolves the identity column's
  backing sequence.
- Deps: Steps 2-5 (the views read the catalog state those steps populate).
- Tests unblocked: this is what flips `identity`/`generated_stored`/
  `generated_virtual` from "DDL/DML passes, introspection queries diff"
  to fully PASS (modulo `\d` / psql-describe output and
  `column_column_usage`, a second, more obscure information_schema view --
  treat as KNOWN-DIFF if out of budget).
- Effort: M (mostly SQL view definitions + one small function; low
  algorithmic risk, but requires finding/using PepperDB's actual builtin
  view/function registration mechanism, which this research did not audit
  in detail -- verify against how other system views are seeded, e.g.
  `pg_stat_*` or similar, before assuming a pattern).

### Step 7 -- fast_default.sql: acknowledge PL/pgSQL blocker (no PepperDB code change)
- Not an implementation step. `fast_default.sql`'s 4 `LANGUAGE 'plpgsql'`
  helper functions (`set`, `comp`, +2 more) are load-bearing for nearly every
  assertion in the file (they compare `relfilenode` before/after ALTER TABLE
  ADD COLUMN to prove no rewrite happened). This file cannot PASS without
  the already-registered PL/pgSQL function-body-execution subsystem
  (`file-list/23.txt`, ~45 tests). Recommend: implement Steps 1-6 anyway
  (the `attmissingval` mechanism has independent correctness value and
  partially unblocks DEFAULT-related lines elsewhere, e.g. any future
  `insert`/`alter_table` rework), but snapshot `fast_default` as KNOWN-DIFF
  with the reason "blocked on PL/pgSQL function execution (file-list/23.txt)"
  rather than attempting to force it green.
- Effort: n/a (documentation/scheduling only).

**Total for Steps 1-6 (the buildable core): XL** (Step 1's grammar surgery
and Step 4's heap tuple-deform fallback are the two highest-risk pieces;
Steps 2/3/5 are more mechanical translation once 1 and 4 land). Step 7 is a
scheduling note, not code.

## 5. Architectural notes

- **Stored vs virtual, the core distinction**: a STORED generated column is
  computed once at write time (INSERT/UPDATE) and physically occupies space
  in the heap tuple like any other column -- `ExecComputeStoredGenerated`
  fills it in before `heap_insert`/`heap_update`, and ordinary tuple-deform
  reads it back like normal. A VIRTUAL generated column stores **nothing** on
  disk (its heap tuple slot is never materialized with a real value; PG's
  rewrite layer even stuffs a `Const NULL` placeholder into the physical
  targetlist since the storage layer still needs *a* slot) and is instead
  expanded as an inline expression substitution at every read site via
  planner-level `Var` rewriting (`expand_virtual_generated_columns`). This
  means: STORED needs executor-side work (Step 5's `ExecComputeStoredGenerated`
  port) but no planner changes; VIRTUAL needs planner-side work (Step 5's
  `expand_virtual_generated_columns` port) but no executor materialization at
  all. Do not conflate the two into one code path -- PG deliberately keeps
  them separate because their performance/storage tradeoffs are opposite
  (STORED costs disk + write-time CPU, VIRTUAL costs read-time CPU on every
  query touching the column, potentially repeated per row per query).
- **Identity = sequence + column-default sugar, not a distinct storage
  concept.** `attidentity` is a marker on `pg_attribute` alone; the actual
  value-generation mechanism is 100% the existing sequence machinery
  (`nextval_internal`) wrapped in a `NextValueExpr` AST node used as that
  column's effective default. This means identity-column work in PepperDB
  should almost entirely reuse Step 3's sequence-creation/ownership pattern
  and the existing `src/backend/commands/sequence.rs`/`src/commands/
  sequence.rs` APIs (already confirmed working) -- there is no new storage
  or MVCC concept to invent, only new DDL surface (grammar + catalog flag +
  auto-generated CREATE SEQUENCE + OWNED BY) and new INSERT-path enforcement
  (OVERRIDING semantics, rejecting user-supplied values for ALWAYS identity).
- **Fast-default (`attmissingval`) is a read-path optimization with a
  write-path catalog trick, not a generated-column feature at all** --
  PG explicitly excludes generated columns from the fast-default path
  (`!colDef->generated` guard in `ATExecAddColumn`) because a generated
  column's value must be *computed*, not filled with a static missing-value
  constant; a newly materialized generated column on ADD COLUMN always needs
  either a full rewrite (STORED, to compute+store the expression for every
  existing row) or nothing at all (VIRTUAL, computed lazily on read same as
  any other row). Do not attempt to combine Step 4 (fast-default) and Step 5
  (generated columns) into a shared "add column with a value" abstraction --
  PG keeps them as separate branches in `ATExecAddColumn` for exactly this
  reason, and PepperDB's port should mirror that separation (per the
  standing rule in `ai/plans/003-total-translation/rules.md` to reproduce
  PG's control-flow splits faithfully rather than collapsing them).
- **The grammar gap (Step 1) is the real hidden cost of this feature.**
  Everything else in this breakdown assumes `ColumnDef` can express inline
  `DEFAULT`/`GENERATED`/`IDENTITY` clauses, but PepperDB's current grammar
  cannot express even a plain inline `DEFAULT` in `CREATE TABLE` (only via a
  separate `ALTER TABLE ... SET DEFAULT`). This means Step 1 is doing double
  duty: it is both a prerequisite for this feature and closes a
  pre-existing, unrelated grammar gap that likely also affects other
  not-yet-attempted plan-004/regress work (inline `CHECK`/`NOT NULL` in
  CREATE TABLE, which several other test files probably also need). Flag
  this to the plan-004 owner as a shared dependency worth landing once,
  not something to special-case only for generated/identity syntax.
- **Cross-cutting blockers are real, not hypothetical.** Unlike a typical
  plan-004 step (translate one module, flip tests green), 3 of 4 target
  test files here also depend on subsystems this step does not own
  (`information_schema` views, PL/pgSQL). Step 6 scopes a minimal
  information_schema slice deliberately rather than the whole schema, and
  Step 7 recommends explicitly not chasing `fast_default.sql`'s PL/pgSQL
  dependency inside this work. This keeps the step honest about what it can
  actually turn green versus what it merely makes closer.

## 6. Bottom line for the plan-004 owner

This feature has real, substantial leverage (4 files, ~2,893 lines, a
plausible 3-of-4 full PASS + 1 partial/KNOWN-DIFF outcome) and reuses working
foundations (sequences, a DEFAULT-expansion skeleton). The blocking chain is
strictly ordered: grammar (Step 1) before catalog (Step 2) before
identity-sequence-linking (Step 3) and fast-default (Step 4) before
generated-column computation (Step 5) before information_schema polish
(Step 6). Recommend scheduling Steps 1-6 as one plan-004 step (or a tight
2-step split at the Step 1/2 boundary, since grammar surgery is the
highest-risk, most independently reviewable piece), sized L-XL, ahead of the
SSI initiative (`ai/tmp/regress-expansion/ssi.md`, zero regress leverage) but
behind whatever step is currently next in the steps 17-22 leverage-ordered
sequence, unless the grammar prerequisite (Step 1) is judged valuable enough
to pull forward independently.

# pg_regress Conformance: Dependency-Driven Phase Plan

Strategy: each phase TRANSLATES one blocking-module cluster, then turns GREEN the
tests that cluster unblocks. A test with N blockers goes green only in the phase
where its LAST blocker lands. Phases are ordered by leverage (tests gated) subject
to dependency order; XL low-leverage subsystems (xml, tsearch, replication, RLS,
non-btree AMs, PL/pgSQL, partitioning) are deferred to the tail.

Total tests analyzed: 222 distinct (233 rows incl. duplicate `cluster` placeholder).

## Convergence curve (cumulative green / ~222)

| After phase | New green | Cumulative | Notes |
|---|---|---|---|
| 0  server/wire gate            | ~9  | ~9   | READY tests: bitmapops, combocid, comments, portals_p2, sanity_check, select_having*, misc_sanity(partial) |
| 1  soft-error input API (fmgr) | ~10 | ~19  | unblocks the tail of many type tests; foundational |
| 2  SRF/funcapi + generate_series| ~4 | ~23  | select_distinct_on, tail of int8/select_distinct |
| 3  CTAS / SELECT INTO          | ~3  | ~26  | select_into, select_distinct, select_implicit(+str fns) |
| 4  string/varlena + regexp + like + crypto | ~6 | ~32 | strings, text, varchar, char, name, md5 |
| 5  datetime cluster            | ~5  | ~37  | date, time, timetz, timestamp (named-zone deferred) |
| 6  numeric transcendental+aggs | ~3  | ~40  | numeric, numeric_big, (aggregates partial) |
| 7  reloptions                  | ~2  | ~42  | reloptions, btree_index(partial) |
| 8  EXPLAIN                     | ~4  | ~46  | expressions, tidrangescan(+tid), case(partial) |
| 9  scalar type modules (pg_lsn, oid, tid, macaddr, dbsize) | ~6 | ~52 | pg_lsn, oid, tid, tidscan, tidrangescan, dbsize, macaddr(+hash dep) |
| 10 aggregates + CREATE AGGREGATE| ~2 | ~54  | aggregates, create_aggregate |
| 11 arrays cluster              | ~2  | ~56  | arrays, array_agg feeds aggregates/tsrf |
| 12 roles/user.rs               | ~4  | ~60  | roleattributes, drop_operator(+op), alter_operator(+op) |
| 13 operator/cast DDL           | ~4  | ~64  | create_cast, drop_operator, alter_operator, create_operator |
| 14 CTE SEARCH/CYCLE + rec view | ~1  | ~65  | with |
| 15 cursors/portals scrollable  | ~2  | ~67  | limit, portals(partial) |
| 16 misc smaller (money, enum, foreign_key MATCH FULL, functional_deps) | ~4 | ~71 | money, enum(partial), foreign_key, functional_deps |
| 17+ DEFERRED XL subsystems     | -   | -    | PL/pgSQL, partitioning, inheritance, geometry, json/jsonb, RLS, non-btree AMs, xml, tsearch, ranges, FDW, replication, roles-enforcement |

Fastest convergence is phases 1-4: a handful of foundational, low-effort modules
(soft-error input API, SRF context, CTAS, string funcs) each unblock the tails of
many otherwise-translated type tests. The critical path for the largest test count
is PL/pgSQL + partitioning, which are intentionally deferred because they are XL and
gate mostly tests that ALSO need other deferred subsystems.

## module_index (blocking module -> gated tests, effort)

See structured output. Highest-leverage single modules:
- `fmgr.rs` InputFunctionCallSafe (soft-error API): ~20 tests, effort S
- `tablecmds.rs` partitioning: ~40 tests, effort XL (deferred)
- PL/pgSQL: ~45 tests, effort XL (deferred)
- `explain.rs`: ~15 tests, effort L
- `createas.rs`: ~6 tests, effort M
- `varlena.rs`+regexp+like: ~6 tests, effort L

## Notes
- Phase 0 (server/wire handshake + ErrorResponse + harness) is a fixed prerequisite,
  not re-derived here; it makes the READY tests actually runnable.
- Geometry (geo_ops.c 5.5k), json/jsonb (jsonb 2.2k + jsonfuncs 6k + jsonapi),
  ranges/multiranges, xml, tsearch, non-btree index AMs (gin/gist/spgist/brin/hash),
  RLS, FDW, event triggers, subscription/replication, generated/identity columns,
  inheritance, partitioning, and full role-privilege ENFORCEMENT are each large,
  self-contained subsystems gating clusters of OUT_OF_SCOPE tests. They belong in a
  second campaign after the ~70-test "in-scope" front is green.

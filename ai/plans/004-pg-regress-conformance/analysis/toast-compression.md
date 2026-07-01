# TOAST + compression (pglz, lz4) -- step breakdown for plan 004

Read-only research. No code edited.

## 1. Current PepperDB status (established first, as requested)

**Bottom line: TOAST is NOT implemented. Only the varlena bit-format layer and
catalog columns exist. Every actual TOAST behavior (store, fetch, decompress,
compress) is `unimplemented!()`.**

### What exists (real, working code)
- `src/varatt.rs` (392 lines) -- the on-disk varlena tag/header bit-format:
  `varatt_external`, `varatt_indirect`, `vartag_external` enum, `VARATT_IS_4B`,
  `VARATT_IS_1B_E`, `VARTAG_EXTERNAL`, `VARATT_IS_COMPRESSED`,
  `VARATT_IS_EXTERNAL_ONDISK/INDIRECT/EXPANDED`, `VARATT_EXTERNAL_GET_EXTSIZE`,
  `VARATT_EXTERNAL_GET_COMPRESS_METHOD`, etc. This is genuinely translated and
  usable -- it is the format layer, not the mechanism.
- `src/access/heaptoast.rs` constants: `TOAST_TUPLE_THRESHOLD`,
  `TOAST_TUPLE_TARGET`, `EXTERN_TUPLE_MAX_SIZE`, `MaximumBytesPerTuple` -- real
  arithmetic, correctly ported.
- `src/catalog/pg_attribute.rs`: `attstorage: i8`, `attcompression: i8` fields
  exist on the struct and are written out in `heap.rs` (`Anum_pg_attribute_*`),
  defaulting to `0` (i.e. always `INVALID_COMPRESSION_METHOD`/unset). Nothing
  reads these to drive behavior.
- `src/catalog/pg_type.rs`: `typstorage` field + `TYPSTORAGE_PLAIN/EXTERNAL/
  EXTENDED/MAIN` constants -- defined but not consulted by any toaster (there
  is no toaster to consult them).

### What is stubbed (`unimplemented!()` or empty bodies)
- `src/access/toast_compression.rs` (68 lines): `pglz_compress_datum`,
  `pglz_decompress_datum`, `pglz_decompress_datum_slice`, `lz4_compress_datum`,
  `lz4_decompress_datum`, `lz4_decompress_datum_slice`,
  `toast_get_compression_id`, `compression_name_to_method`,
  `get_compression_method_name` -- **all 9 functions `unimplemented!()`**.
- `src/common/pg_lzcompress.rs` (67 lines): `pglz_compress`, `pglz_decompress`,
  `pglz_maximum_compressed_size` -- **all `unimplemented!()`**. The strategy
  structs (`PglzStrategy`, `PGLZ_STRATEGY_DEFAULT`, `PGLZ_STRATEGY_ALWAYS`) are
  correctly ported data, but the algorithm itself does not exist.
- `src/access/detoast.rs` (32 lines): `detoast_external_attr`, `detoast_attr`,
  `detoast_attr_slice`, `toast_raw_datum_size`, `toast_datum_size` -- **all 5
  `unimplemented!()`**.
- `src/access/toast_internals.rs` (63 lines): `toast_compress_datum`,
  `toast_get_valid_index`, `toast_delete_datum`, `toast_save_datum`,
  `toast_open_indexes`, `toast_close_indexes`, `get_toast_snapshot` -- **all 7
  `unimplemented!()`**. This is the toast-table store/fetch mechanism itself.
- `src/access/heaptoast.rs` logic fns: `heap_toast_insert_or_update`,
  `heap_toast_delete`, `toast_flatten_tuple`, `toast_flatten_tuple_to_datum`,
  `toast_build_flattened_tuple`, `heap_fetch_toast_slice` -- **all 6
  `unimplemented!()`**.
- `src/access/toast_helper.rs`: `toast_tuple_init`,
  `toast_tuple_find_biggest_attribute`, `toast_tuple_try_compression`,
  `toast_tuple_externalize`, `toast_tuple_cleanup`, `toast_delete_external` --
  **all 6 `unimplemented!()`**. (PG source lives at
  `access/table/toast_helper.c`, not `access/common` -- correct the plan-004
  file-list pointer if it says `common`.)
- `src/catalog/toasting.rs` (30 lines): `NewRelationCreateToastTable`,
  `NewHeapCreateToastTable`, `AlterTableCreateToastTable`,
  `BootstrapToastTable` -- **all 4 `unimplemented!()`, and grep confirms none
  of the first three are called anywhere in `src/catalog/heap.rs` or
  `src/backend/commands/tablecmds.rs`** -- `CREATE TABLE` never creates a
  toast table today.
- `src/common/compression.rs` (66 lines, the generic pg_dump-style
  gzip/lz4/zstd spec parser, distinct from TOAST compression) -- also fully
  stubbed; not required for TOAST but shares the "compression" name, don't
  conflate it in planning.

### Confirmed by existing analysis (`ai/tmp/regress-analysis/`)
- `batch-create_table.md` (`delete` test): *"TOAST -- MISSING, heapam.rs
  comments confirm 'M2: no toast (the tuple fits...)' -- no out-of-line
  storage path exists"*. `src/backend/access/heap/heapam.rs` literally has
  the comment `M2: no toast (the tuple fits...)` at the insert path (line 88,
  112-113, 154) -- heap_insert/heap_update never call
  `heap_toast_insert_or_update`.
- `batch-collate.windows.win1252.md` (`compression` test): confirms stub,
  plus lists the *other*, TOAST-independent blockers for that specific test
  (materialized views, partitioning, LIKE INCLUDING, inheritance).
- `batch-random.md` (`reloptions` test): `toast.*` reloptions namespace is
  blocked because `transformRelOptions`/`default_reloptions` in
  `src/backend/access/common/reloptions.rs` are themselves `unimplemented!()`
  for any non-empty option list -- a prerequisite outside this plan's scope
  but relevant if a future step wants `toast_tuple_target` etc. reloptions.

### Conclusion
Compression (pglz/lz4) sits entirely on top of a TOAST store that does not
exist yet. There is no shortcut: even if pglz/lz4 algorithms were translated
in isolation, there is no code path that calls them, because
`heap_toast_insert_or_update` (the decision point for "compress in place vs.
push external") is itself unimplemented, and there is no toast table to push
external values into (`NewRelationCreateToastTable` is never invoked).
**TOAST infrastructure must land first; compression is the second layer.**

## 2. Tests unblocked (cross-referenced against `ai/tmp/regress-analysis/`)

| Test | Current classification | TOAST/compression role | Other blockers (non-TOAST) |
|---|---|---|---|
| `compression` | OUT_OF_SCOPE | primary subject | materialized views (MISSING), declarative partitioning (MISSING), `INHERITS` (`not_yet_reachable`), `ALTER TABLE ... ALTER COLUMN TYPE` (verify), `LIKE INCLUDING COMPRESSION` |
| `compression_pglz` | OUT_OF_SCOPE | primary subject (direct pglz round-trip + corruption edge cases) | `CREATE FUNCTION ... LANGUAGE C` dynamic library loading (MISSING -- loads `regress.so`'s `test_pglz_compress`/`test_pglz_decompress`), `decode`/`encode` bytea functions (MISSING) |
| `indirect_toast` | OUT_OF_SCOPE | primary subject (external/indirect toast datum handling, `VACUUM FREEZE` interaction) | `CREATE FUNCTION ... LANGUAGE C` (MISSING -- `make_tuple_indirect` from `regress.so`), PL/pgSQL trigger body execution (MISSING -- `update_using_indirect`) |
| `delete` | NEEDS_MODULES | secondary blocker (`repeat('x',10000)` needs TOAST store to hold/delete a >2KB value) | `repeat()` SQL function unbound (`func: None` in `varlena.rs` fmgrtab) |
| `strings` | NEEDS_MODULES | secondary blocker ("TOAST substr fast-path" -- slicing a compressed/external datum without fully detoasting) | LIKE/ILIKE/SIMILAR TO engine, TRIM variants, lpad/rpad, translate, split_part, encode/decode, SHA-2, CRC32 -- all MISSING (large independent surface) |
| `rowtypes` | NEEDS_MODULES | secondary blocker ("TOAST of composite values") | record I/O, record comparison ops, `row_to_json`, `INHERITS` -- all MISSING (TOAST is a minor fraction of this test's blockers) |
| `reloptions` | NEEDS_MODULES | secondary blocker (`toast.*` reloption namespace) | `transformRelOptions`/`default_reloptions` unimplemented for ANY non-empty WITH list -- this is the dominant blocker, unrelated to TOAST itself |
| `cluster` | NEEDS_MODULES | secondary blocker ("TOAST clustering" -- CLUSTER must move toast rows too) | `INHERITS`, `CREATE USER/ROLE`, declarative partitioning, EXPLAIN -- all MISSING (TOAST is minor here) |
| `portals` | NEEDS_MODULES | secondary blocker ("toasted array fetch + default_toast_compression='pglz'") | scrollable/WITH HOLD cursors (`unimplemented!`), `WHERE CURRENT OF`, `INHERITS`, SQL-lang functions -- dominant blockers |
| `encoding` | OUT_OF_SCOPE | secondary blocker ("TOAST slicing of long UTF8 text") | `LANGUAGE C` extension loading, PL/pgSQL, multibyte encoding internals -- dominant blockers |
| `misc_functions` | OUT_OF_SCOPE | secondary blocker (`pg_column_toast_chunk_id`, `STORAGE EXTERNAL`) | huge unrelated surface (introspection SRFs, EXPLAIN, LANGUAGE C/internal) |
| `temp` | NEEDS_MODULES | secondary blocker ("CHECK constraint w/ toasted pg_constraint") | PL/pgSQL body execution -- dominant blocker |

**Count: 3 tests have TOAST/compression as the PRIMARY subject** (`compression`,
`compression_pglz`, `indirect_toast`) **-- all 3 are currently OUT_OF_SCOPE and
will REMAIN out of scope after a TOAST+compression implementation**, because
each also requires `LANGUAGE C` dynamic library loading (to load
`regress.so`'s `test_pglz_compress`/`test_pglz_decompress`/
`make_tuple_indirect`) and/or PL/pgSQL trigger bodies and/or materialized
views/partitioning/inheritance -- none of which this plan addresses. Building
TOAST + compression does NOT flip these 3 tests to PASS on its own.

**9 further tests** (`delete`, `strings`, `rowtypes`, `reloptions`, `cluster`,
`portals`, `encoding`, `misc_functions`, `temp`) list TOAST as ONE of several
blockers, and in most of them (`strings`, `rowtypes`, `reloptions`, `cluster`,
`portals`, `encoding`, `misc_functions`, `temp`) TOAST is a minor fraction of
the total blocker surface -- landing TOAST alone will not flip these to PASS
either. **`delete` is the one test where TOAST is close to the sole remaining
blocker** (the other blocker, `repeat()`, is a ~5-line fmgr binding fix) --
this is the most realistic near-term PASS from this plan.

**Practical implication for plan 004 sequencing:** if the goal is to move the
PASS counter, `delete` is worth doing after landing steps 1-3 below. The 3
compression-specific tests are correctly OUT_OF_SCOPE and should stay
registered in `file-list/23.txt`-style deferred lists (or wherever
LANGUAGE-C-dependent tests are tracked) rather than being treated as this
plan's exit criterion. Recommend that plan 004 frame this work as
"infrastructure for `delete` + general TOAST correctness (any future test
inserting >8KB rows/large arrays will silently rely on this)", not as
"make `compression` pass".

## 3. PG source reference (sizes, so the agent can gauge each step)

| File | Lines | Role |
|---|---|---|
| `access/common/toast_compression.c` | 316 | pglz_compress_datum/decompress_datum(_slice), lz4 equivalents, `toast_get_compression_id`, name<->method mapping |
| `access/common/detoast.c` | 646 | `detoast_attr`, `detoast_attr_slice`, `detoast_external_attr`, chunk-fetch loop from toast table, size helpers |
| `access/heap/heaptoast.c` | 791 | `heap_toast_insert_or_update` (the decision engine: which attrs to compress/externalize), `heap_toast_delete`, `toast_flatten_tuple*`, `heap_fetch_toast_slice` |
| `access/common/toast_internals.c` | 656 | `toast_save_datum`/`toast_delete_datum` (chunked insert/delete into toast table + index), `toast_compress_datum`, `toast_open_indexes` |
| `access/table/toast_helper.c` | (no .c found at `access/common`; actual path `access/table/toast_helper.c`) | per-attribute iteration helper used by `heap_toast_insert_or_update`: `toast_tuple_init`, `_find_biggest_attribute`, `_try_compression`, `_externalize`, `_cleanup` |
| `catalog/toasting.c` | 427 | `NewRelationCreateToastTable`/`AlterTableCreateToastTable`/`BootstrapToastTable` -- creates the `pg_toast.pg_toast_NNNN` table + its index during CREATE TABLE/bootstrap |
| `common/pg_lzcompress.c` | 887 | the pglz algorithm itself (hash-chain LZ77 variant), `pglz_compress`, `pglz_decompress` |
| `include/varatt.h` | 358 | format header (already ported to `src/varatt.rs`) |
| `include/access/toast_internals.h` | 63 | `toast_compress_header` struct: `{vl_len_: i32, tcinfo: u32}` -- `tcinfo` packs 2-bit compression method + 30-bit external size via `VARLENA_EXTSIZE_MASK`/`VARLENA_EXTSIZE_BITS` |

Key on-disk constant (must be byte-faithful): `TOAST_MAX_CHUNK_SIZE` is
computed from `BLCKSZ`/`MaximumBytesPerTuple`, currently correctly ported in
`src/access/heaptoast.rs`. Changing it requires an initdb, per PG's own
comment -- do not deviate from the formula.

`commands/tablecmds.c`: `ATExecSetStorage` and the `COMPRESSION`
clause-handling in `ATExecAddColumn`/`ATExecAlterColumnType`/`transformColumnDefinition`
are the DDL entry points for `SET STORAGE`/`SET COMPRESSION`
(no single small function -- logic is threaded through the generic ALTER
TABLE column-definition pipeline). `src/backend/commands/tablecmds.rs` today
has no `attcompression`/`attstorage`-setting code path at all (confirmed by
grep -- only the catalog write-out in `heap.rs` touches these fields, always
with the zero default).

## 4. Step breakdown

### Step T1 -- Toast table creation (catalog + DDL wiring)
- **PG files:** `catalog/toasting.c` (427L), relevant `DECLARE_TOAST` macros
  in per-catalog headers (e.g. `pg_class.h`), `catalog/pg_type.h` (`TYPSTORAGE_*`
  usage already ported).
- **Our target files (verify/implement):** `src/catalog/toasting.rs` (currently
  4 `unimplemented!()`), wire `NewRelationCreateToastTable` into
  `src/catalog/heap.rs` (`heap_create_with_catalog` equivalent) and
  `src/backend/commands/tablecmds.rs` (`DefineRelation`), plus
  `BootstrapToastTable` into whatever seeds initial catalogs (build.rs /
  Rust-initdb per the plan-003 bootstrap decision).
- **Deliverable:** `CREATE TABLE` for any table with a toastable column
  (varlena, i.e. `attlen == -1`) creates a matching `pg_toast.pg_toast_<oid>`
  table with columns `(chunk_id oid, chunk_seq int4, chunk_data bytea)` and a
  unique btree index on `(chunk_id, chunk_seq)`, and records the toast
  relation's oid in `pg_class.reltoastrelid` of the owning table.
- **Deps:** plan-003 heap/catalog (`heap.rs`, `pg_class.rs`, `index.rs` for
  building the toast index) -- all already complete per plan-003. No
  compression dependency.
- **Tests unblocked:** none directly yet (this alone doesn't let big values
  flow anywhere) -- prerequisite for T2.
- **Effort:** M (table/index DDL is mechanical given existing `heap.rs`/
  `index.rs` primitives; ~427 PG lines to port, but ~half is
  bootstrap-mode special-casing that may be simplified for PepperDB's
  Rust-initdb).

### Step T2 -- External storage: toast_save_datum / toast_delete_datum / toast_open_indexes
- **PG files:** `access/common/toast_internals.c` (656L, minus the
  `toast_compress_datum` function which belongs to T4).
- **Our target files:** `src/access/toast_internals.rs` (7 stubs; implement
  `toast_save_datum`, `toast_delete_datum`, `toast_open_indexes`,
  `toast_close_indexes`, `toast_get_valid_index`, `get_toast_snapshot`; leave
  `toast_compress_datum` for T4).
- **Deliverable:** given a varlena datum too large to fit inline, split it
  into `TOAST_MAX_CHUNK_SIZE`-byte chunks, insert one heap tuple per chunk
  into the target toast table (via the T1 toast relation), return a
  `varatt_external` pointer (`va_valueid`, `va_toastrelid`, `va_rawsize`,
  `va_extinfo` all zero/uncompressed for now); `toast_delete_datum` deletes
  all chunks for a given `chunk_id`.
- **Deps:** T1 (toast table must exist); plan-003 heap insert/delete
  primitives (`heapam.rs`), btree index insert (`nbtree`, complete per M-ladder).
- **Tests unblocked:** none standalone -- prerequisite for T3.
- **Effort:** M-L (656 PG lines; chunking loop + index-scan-based fetch/delete
  path is the meatiest part; no algorithmic novelty, mostly straightforward
  translation using already-existing heap/index primitives).

### Step T3 -- Detoast fetch path + heap_toast_insert_or_update decision engine (no compression yet -- external-only)
- **PG files:** `access/common/detoast.c` (646L), `access/heap/heaptoast.c`
  (791L), `access/table/toast_helper.c` (per-attribute helper, size TBD --
  not found in current checkout path, verify actual line count when
  implementing), `varatt.h` external-representation macros (already ported).
- **Our target files:** `src/access/detoast.rs` (5 stubs),
  `src/access/heaptoast.rs` (6 logic-fn stubs), `src/access/toast_helper.rs`
  (6 stubs); wire `heap_toast_insert_or_update`/`heap_toast_delete` into
  `src/backend/access/heap/heapam.rs` at the exact spots currently marked
  `// M2: no toast (the tuple fits...)` (lines ~88, 112-113, 154).
  **Scope this step to EXTERNALIZE-ONLY** (skip compression attempt; treat
  as if compression is always unavailable) to keep T3 decoupled from T4/T5 --
  i.e. `toast_tuple_try_compression` can no-op/return false in this step and
  get filled in during T4.
  Also implement `SUBSTR`/`substring` fast-path partial fetch
  (`heap_fetch_toast_slice`, `detoast_attr_slice`) since the `strings` test
  and general correctness need slice-without-full-detoast.
- **Deliverable:** inserting/updating a row with a value exceeding
  `TOAST_TUPLE_THRESHOLD` externalizes the largest attribute(s) via T2 until
  the tuple fits; reading such a row transparently detoasts (full or sliced)
  before returning the Datum to the executor; deleting the row deletes the
  toast chunks too.
- **Deps:** T1, T2. NOT dependent on compression (T4/T5) if scoped as above.
- **Tests unblocked:** **`delete`** (once `repeat()` is also bound -- a
  separate ~5-line fix in `varlena.rs` fmgrtab, not part of this plan but
  worth flagging to whoever picks up `delete`). Partial progress toward
  `strings` (TOAST substr fast-path) and `rowtypes` (TOAST of composite
  values), though both need much more unrelated work to fully pass.
- **Effort:** L (this is the biggest step -- 791+646 PG lines, the core
  decision logic of "which attribute to shrink next" with its iterative
  loop over `toast_tuple_find_biggest_attribute`, careful handling of
  self-referential updates (old tuple's toast pointers reused when
  unchanged), and the multi-page chunk-fetch loop for reads).

### Step T4 -- pglz compress/decompress
- **PG files:** `common/pg_lzcompress.c` (887L) for the algorithm,
  `access/common/toast_compression.c` (pglz-specific ~100L subset) for the
  varlena-level wrapper.
- **Our target files:** `src/common/pg_lzcompress.rs` (3 stubs: `pglz_compress`,
  `pglz_decompress`, `pglz_maximum_compressed_size` -- strategy structs
  already correct), `src/access/toast_compression.rs` (fill in
  `pglz_compress_datum`, `pglz_decompress_datum`,
  `pglz_decompress_datum_slice`); wire `toast_compress_datum` in
  `src/access/toast_internals.rs`; wire `toast_tuple_try_compression` in
  `src/access/toast_helper.rs` (previously a no-op stub from T3) to actually
  attempt pglz before falling back to externalize.
- **Deliverable:** inserting a compressible value >32 bytes (min_input_size)
  that shrinks by >=25% (min_comp_rate) gets stored as a 4-byte-header
  "compressed" varlena (`VARATT_IS_4B_C`) inline if it now fits, or
  compressed-then-externalized if still too large; reading transparently
  pglz-decompresses. Must produce **byte-identical** compressed output to
  PG's algorithm is NOT required (compression is not required to be
  bit-reproducible across implementations in general), but the **format**
  (`toast_compress_header { vl_len_, tcinfo }`, 2-bit method + 30-bit
  extsize packing per `VARLENA_EXTSIZE_MASK`/`_BITS`) MUST be byte-exact
  since it's read back by `TOAST_COMPRESS_METHOD`/`_EXTSIZE` macros already
  ported in `varatt.rs`/`toast_internals.rs`.
- **Deps:** T1-T3 (needs the store/fetch path and the try-compression call
  site). This is the actual "compression method" deliverable requested.
- **Tests unblocked:** none flip to full PASS alone (see section 2 --
  `compression_pglz` additionally needs `LANGUAGE C` loading of
  `test_pglz_compress`/`test_pglz_decompress` from `regress.so`, which this
  plan does not provide). Internally testable via Rust unit tests
  round-tripping `pglz_compress`/`pglz_decompress` directly (recommend
  adding these regardless of the regress-suite gap, mirroring PG's own
  `test_pglz_compress` C-extension behavior as a native `#[test]`).
- **Effort:** L (887-line algorithm: hash-chain literal/match LZ77 encoder
  with PG's specific bit-packed control-byte/tag format -- must be
  functionally correct, no shortcuts, since decompression of PG-written
  data and vice versa may matter for on-disk-format tests/back-compat, even
  though PepperDB isn't wire-compatible with real PG files today).

### Step T5 -- lz4 + SET COMPRESSION DDL + attcompression wiring
- **PG files:** `access/common/toast_compression.c` (lz4-specific ~100L
  subset, guarded by `USE_LZ4`/`#ifdef`), `commands/tablecmds.c`
  (`ATExecSetStorage`, column-definition `COMPRESSION` clause handling --
  threaded through `transformColumnDefinition`/`ATExecAddColumn`/
  `ATExecAlterColumnType`, no single isolated function), `catalog/pg_attribute.h`
  (`attcompression` -- already a field in `src/catalog/pg_attribute.rs`),
  `utils/misc/guc_tables.c` (`default_toast_compression` GUC, values `pglz`/`lz4`).
- **Our target files:** `src/access/toast_compression.rs` (fill in
  `lz4_compress_datum`, `lz4_decompress_datum`, `lz4_decompress_datum_slice`,
  `toast_get_compression_id`, `compression_name_to_method`,
  `get_compression_method_name`); add `lz4_flex` as a new Cargo dependency
  (see Arch Notes); `src/backend/commands/tablecmds.rs` (add `COMPRESSION`
  clause parsing in column definitions + `ALTER TABLE ... ALTER COLUMN ...
  SET COMPRESSION {pglz|lz4|default}` + `SET STORAGE {plain|external|
  extended|main}`, writing `attcompression`/`attstorage` instead of the
  current always-zero default); GUC registration for
  `default_toast_compression` (verify GUC table exists/how GUCs are
  registered elsewhere in the port); parser grammar additions for the
  `COMPRESSION` column-constraint keyword and `SET COMPRESSION`/`SET STORAGE`
  ALTER TABLE subcommands (check `src/backend/parser/gram.lalrpop` currently
  has neither).
  Also add `pg_column_compression(anyelement)` builtin (reads
  `VARATT_EXTERNAL_GET_COMPRESS_METHOD`/inline compressed-header method,
  maps to `'pglz'`/`'lz4'`/NULL) since every compression-test query calls it.
- **Deliverable:** `CREATE TABLE t(f1 text COMPRESSION lz4)` and
  `ALTER TABLE t ALTER COLUMN f1 SET COMPRESSION pglz` persist
  `attcompression` correctly; new tuples honor the column's (or the
  `default_toast_compression` GUC's) compression method;
  `pg_column_compression()` reports it back.
- **Deps:** T4 (needs pglz as the fallback/default) + this step's own lz4
  algorithm. `SET STORAGE` does not depend on T4/T5 compression and could be
  split out earlier if useful, but grouping it here since it's part of the
  same DDL surface and both are needed for the `compression` test's `\d+`
  output.
- **Tests unblocked:** still none reach full PASS (materialized views /
  partitioning / inheritance / LANGUAGE C remain independent blockers for
  `compression`; LANGUAGE C remains a blocker for `compression_pglz` and
  `indirect_toast`) -- but this closes the compression-specific gap so that
  IF/WHEN materialized-views + LANGUAGE-C-loading land in a later plan, only
  the SQL surface (not TOAST/compression) is left as a blocker for
  `compression`. Recommend tracking this explicitly as "closes the
  TOAST/compression gap for `compression`; does not itself flip it to PASS".
- **Effort:** M for lz4 algorithm (delegated to `lz4_flex`, so mostly a thin
  wrapper matching PG's header-packing), L for the DDL/grammar plumbing
  (touches parser grammar + tablecmds' already-large ALTER TABLE dispatch,
  which plan-004's own analysis flags as risky/`not_yet_reachable`-laden
  territory).

## 5. Architecture notes

**TOAST-before-compression is a hard dependency, not a sequencing choice.**
Compression in PG is implemented as one clause inside
`heap_toast_insert_or_update`'s per-attribute loop
(`toast_tuple_try_compression`, called from `toast_helper.c`, itself only
reachable from `heaptoast.c`). There is no code path that compresses a datum
except as a step toward either (a) shrinking it enough to store inline, or
(b) shrinking it before externalizing. Without T1-T3, `toast_compress_datum`
has no caller and no toast table to spill into if compression isn't enough
-- confirmed by grep: `NewRelationCreateToastTable` is never invoked in
`src/`. **Do not attempt a "just the algorithm" step in isolation as the
plan's exit criterion** -- it would be untestable via the regress suite and
would leave the store/fetch path (which several OTHER tests need,
independent of compression) still broken.

**On-disk format fidelity requirements (must be byte-exact):**
1. `varatt_external` layout (`va_header`/tag byte, `va_rawsize`,
   `va_extinfo` 2-bit-method+30-bit-size packing, `va_valueid`,
   `va_toastrelid`) -- **already correctly ported** in `src/varatt.rs`. Any
   new code must reuse these accessors (`VARATT_EXTERNAL_GET_EXTSIZE`,
   `_GET_COMPRESS_METHOD`, `_SET_SIZE_AND_COMPRESS_METHOD`), not
   reimplement the bit math.
2. `toast_compress_header { vl_len_: i32, tcinfo: u32 }` -- the inline
   compressed-varlena header (`VARATT_IS_4B_C`), same 2-bit/30-bit packing
   via `VARLENA_EXTSIZE_MASK`/`_BITS`. `src/access/toast_internals.rs`
   already declares `TOAST_COMPRESS_EXTSIZE`/`_METHOD`/
   `_SET_SIZE_AND_COMPRESS_METHOD` signatures (currently unimplemented
   bodies) -- these must match PG's macros exactly since the compressed
   bytes that follow are handed straight to `pglz_decompress`/lz4 decode
   with the size taken from this header.
3. Toast table row shape `(chunk_id oid, chunk_seq int4, chunk_data bytea)`
   with `TOAST_MAX_CHUNK_SIZE` chunking -- affects nothing external, but a
   fixed 4-attribute+ctid layout, must match for `pg_column_toast_chunk_id`
   and any future pg_dump/physical-replication compatibility goals.
4. The compression *algorithm output itself* (pglz's LZ77 byte stream, or
   lz4's block format) does NOT need to be byte-identical to PG's C
   implementation -- compression is round-tripped by PepperDB's own
   writer/reader, and nothing in the regress suite compares raw compressed
   bytes (the `compression_pglz` test round-trips through PG's own
   `pglz_compress`/`pglz_decompress` C functions, which this plan doesn't
   provide access to anyway since it requires `LANGUAGE C`). Do not spend
   effort chasing bit-exact pglz output; spend it on the header/format
   fidelity in points 1-3 instead.

**lz4: external crate `lz4_flex`, not vendored, not a C binding.**
PG's `toast_compression.c` links against system `liblz4` via `<lz4.h>`
(`LZ4_compress_default`/`LZ4_decompress_safe`), which is the **block format**
(single in-memory buffer, no frame headers/checksums) -- this is the
correct format to match, NOT the LZ4 frame format (`.lz4` file format with
magic bytes). Recommend `lz4_flex` (pure Rust, `lz4_flex::block` module,
`compress_prepend_size`/`decompress_size_prepended`-style API, no_std-capable
for the block format, MIT/Apache-2.0, actively maintained, matches PepperDB's
"prefer pure Rust over FFI" pattern seen elsewhere in the port). Do NOT use
the `lz4` crate (C FFI binding to liblz4) or `lz4-sys` -- both reintroduce a
C build dependency that the rest of this Rust port avoids. Add
`lz4_flex = "0.11"` (check latest at implementation time) to
`Cargo.toml [dependencies]` -- currently there is no lz4 crate of any kind
in `Cargo.toml`/`Cargo.lock`.

**Compression method storage on attributes vs. GUC default:** `attcompression`
on `pg_attribute` is per-column and wins when set; `default_toast_compression`
GUC (`'pglz'` or `'lz4'`) applies when a column has no explicit
`COMPRESSION` clause. `INVALID_COMPRESSION_METHOD` (`'\0'`, currently the
always-written default per T5's grep finding) must be distinguished from an
explicit choice -- `compression_name_to_method`/`get_compression_method_name`
round-trip `'pglz'`<->`TOAST_PGLZ_COMPRESSION` (`'p'`) and
`'lz4'`<->`TOAST_LZ4_COMPRESSION` (`'l'`).

**Suggested commit/step grouping for plan 004:** T1+T2 as one dependency
sub-commit (toast table + store/fetch, no behavior change visible to SQL
yet), T3 as its own sub-commit (wires heapam.rs, makes `delete` passable --
worth its own regression-visible milestone), T4 as its own sub-commit (pglz,
add native `#[test]` round-trip coverage since the regress test can't be
reached), T5 as its own sub-commit (lz4 + DDL surface). This matches the
plan-002-style "LARGE step splits into dependency sub-commits, each
agent-reviewed, manual gate after all" convention already used on this repo.

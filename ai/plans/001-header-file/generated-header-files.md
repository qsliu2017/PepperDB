# Generated headers under `src/include`

These headers are **not hand-written C** - they are emitted by Perl generator
scripts from data files (`.dat`, `.txt`) or from BKI-annotated source headers.
Do **not** translate the generated text. Instead translate (or re-implement) the
*generator + its input*, so the Rust side has a single source of truth.

Verification: marker scan (`DO NOT EDIT` / `auto-generated`) over `src/include`,
plus the generator scripts in `src/**/*.pl` and their input files (presence
confirmed). `regex/regex.h` matched on a comment but is hand-written vendored
code - not generated.

## Families

### 1. Catalog headers - `pg_*_d.h`, `schemapg.h`, `syscache_*.h`, `system_fk_info.h`

- **Generator:** `src/backend/catalog/genbki.pl` (+ `src/backend/catalog/Catalog.pm`)
- **Inputs:** the BKI source headers `src/include/catalog/pg_*.h` (those with the
  `CATALOG(...)` macro) plus the `src/include/catalog/pg_*.dat` seed-data files.
- **Outputs under `src/include/catalog/`:** the `pg_*_d.h` files, `schemapg.h`,
  `syscache_ids.h`, `syscache_info.h`, `system_fk_info.h`. (Also emits
  `postgres.bki` used by initdb - not a header.)
- **Approach:** The `pg_*.h` BKI source headers ARE hand-written and must be
  translated normally (they define the C structs). The `_d.h` outputs only
  carry `#define`d OIDs, attribute numbers, and `Anum_*`/`Natts_*` constants.
  Re-implement `genbki` logic (or a build.rs) that reads the `.dat` + struct
  defs and emits the Rust constants. Treat `_d.h` as generated artifacts, never
  translate them by hand.

### 2. fmgr tables - `fmgroids.h`, `fmgrprotos.h`  (NOT under src/include)

- **Generator:** `src/backend/utils/Gen_fmgrtab.pl`
- **Input:** `src/include/catalog/pg_proc.dat`
- **Outputs:** `src/backend/utils/fmgroids.h`, `fmgrprotos.h`, `fmgrtab.c`.
- Live under `src/backend`, so outside the header-translation pass, but the
  Rust port still needs the equivalent OID constants + prototype table.

### 3. Wait events - `utils/wait_event_types.h`

- **Generator:** `src/backend/utils/activity/generate-wait_event_types.pl`
- **Input:** `src/backend/utils/activity/wait_event_names.txt`
- **Output under src/include:** `utils/wait_event_types.h` (the one import cycle
  with `utils/wait_event.h` - merge both into a single `utils::wait_event`
  Rust module fed by the generator).

### 4. LWLock names - `storage/lwlocknames.h`

- **Generator:** `src/backend/storage/lmgr/generate-lwlocknames.pl`
- **Input:** `src/include/storage/lwlocklist.h`
- **Outputs:** `src/include/storage/lwlocknames.h` and a backend copy.

### 5. SQL error codes - `utils/errcodes.h` (NOT under src/include) + PL variants

- **Generator:** `src/backend/utils/generate-errcodes.pl` (+ per-PL
  `generate-plerrcodes.pl`, `generate-spiexceptions.pl`, `generate-pltclerrcodes.pl`)
- **Input:** `src/backend/utils/errcodes.txt`
- **Outputs:** `src/backend/utils/errcodes.h`, `plerrcodes.h`, etc.

### 6. Keyword lists - `*_kwlist_d.h`

- **Generator:** `src/tools/gen_keywordlist.pl`
- **Inputs (hand-written):** `src/include/parser/kwlist.h`,
  `src/interfaces/ecpg/preproc/{c_kwlist,ecpg_kwlist}.h`,
  `src/pl/plpgsql/src/pl_{reserved,unreserved}_kwlist.h`,
  `src/common/kwlist.h`
- **Outputs:** matching `*_kwlist_d.h` (offset/hash tables for keyword lookup).

### 7. Node support - `nodetags.h` (NOT under src/include)

- **Generator:** `src/backend/nodes/gen_node_support.pl`
- **Inputs:** the node-defining headers (`nodes/*.h`, `parsenodes.h`,
  `plannodes.h`, `primnodes.h`, `pathnodes.h`, `execnodes.h`, ...) - parsed for
  `pg_node_attr` annotations.
- **Outputs:** `src/backend/nodes/nodetags.h`, plus `*funcs.c` (copy/equal/out/read).
- **Approach:** In Rust, replace with `#[derive(...)]` macros on the node enums/
  structs rather than a codegen script.

### 8. Unicode tables - `common/unicode_*_table.h`, `unicode_norm_*`

- **Generators:** `src/common/unicode/generate-unicode_*.pl`
- **Inputs:** Unicode Character Database files (`UnicodeData.txt`, etc.),
  downloaded into `src/common/unicode/` at build time (not vendored in-tree).
- **Outputs under src/include/common:** `unicode_case_table.h`,
  `unicode_category_table.h`, `unicode_east_asian_fw_table.h`,
  `unicode_nonspacing_table.h`, `unicode_norm_table.h`,
  `unicode_norm_hashfunc.h`, `unicode_normprops_table.h`.
- **Approach:** Pure lookup tables - regenerate as Rust `static` arrays via a
  port of the generator, or vendor a crate.

## Not generated (false positives from the marker scan)

`regex/regex.h` (vendored Henry Spencer regex), and headers that merely mention
"generated columns" or "automatically generated" in prose
(`access/*.h`, `nodes/*.h`, `catalog/genbki.h`, `catalog/heap.h`, the
`snowball/libstemmer/stem_*.h` which are vendored from the Snowball project).
The `snowball` stemmer headers are generated *upstream* by Snowball, not by our
build - treat as vendored, port or wrap as-is.

## Translation rule of thumb

For each generated header: port the **generator** (as a `build.rs`, a proc-macro,
or a one-time codegen step) reading the same input file, OR replace the mechanism
with idiomatic Rust (derives for node support, a crate for Unicode). Never
hand-translate the generated `.h` text - it will drift from its source.

---

# Rust rewrite strategy per family

The C generators exist because C has no compile-time reflection: a Perl script
parses struct/enum/data text and emits more text. Rust has derive macros, real
tagged enums, and `build.rs`, so most of this machinery either dissolves into a
derive or becomes a small data-parsing `build.rs`. Mechanism chosen per family:

| Family | C mechanism | Rust mechanism | Effort win |
|---|---|---|---|
| Node support | `gen_node_support.pl` -> tags + copy/equal/out/read funcs | `#[derive]` on node types | huge |
| Catalog | `genbki.pl` -> `_d.h` + bki + schemapg | derive macro (struct->attrs) + `build.rs` over `.dat` | high |
| fmgr | `Gen_fmgrtab.pl` -> oids/protos/table | `build.rs` over `pg_proc.dat` + `#[builtin]` registration | medium |
| errcodes | `generate-errcodes.pl` | `build.rs` (or macro) over `errcodes.txt` | trivial |
| wait events | `generate-wait_event_types.pl` | `build.rs` over `wait_event_names.txt` | trivial |
| lwlocks | `generate-lwlocknames.pl` | `build.rs` over `lwlocklist.h` | trivial |
| keywords | `gen_keywordlist.pl` -> offset/hash table | `phf` perfect-hash map | trivial |
| Unicode | `generate-unicode_*.pl` from UCD | `build.rs` over vendored UCD, or a crate | medium (fidelity) |
| Snowball | upstream Snowball compiler | `rust-stemmers` crate / Snowball Rust backend | low (vendor) |

## 1. Node support - the biggest win (replace with derives)

`gen_node_support.pl` parses every node struct (annotated with `pg_node_attr`)
and emits `nodetags.h` plus `copyfuncs.c`/`equalfuncs.c`/`outfuncs.c`/
`readfuncs.c`. In Rust this is entirely free:

- **NodeTag** -> Rust enums are *already* tagged unions; the discriminant is the
  node tag. No generated enum needed.
- **copyfuncs / equalfuncs** -> `#[derive(Clone, PartialEq)]`.
- **outfuncs / readfuncs** (the textual node serializer) -> `#[derive(Serialize,
  Deserialize)]` with a custom format, or a `#[derive(Node)]` that emits PG's
  exact wire format if round-trip compatibility with stored rules/views matters.
- **`pg_node_attr` flags** (`equal_ignore`, `copy_ignore`, `read_write_ignore`,
  `array_size`, ...) -> field attributes: `#[node(equal_ignore)]`, etc., consumed
  by the custom derive.

Net: ~4 large generated `.c` files and one generated header collapse into derive
annotations on the node definitions. This is the single highest-leverage change.

## 2. Catalog (genbki) - derive macro + build.rs

Split by what kind of thing each output is:

- **Struct shape -> derive macro.** A `#[derive(Catalog)]` + `#[catalog(name =
  "pg_type", oid = 71, rowtype_oid = 71, ...)]` on `FormData_pg_type` generates
  what `_d.h` carries: `Anum_pg_type_*` attribute numbers (field position is
  known at derive time), `Natts_pg_type`, the relation/rowtype OID constants, and
  the compiled `TupleDesc` (replacing `schemapg.h`). Per-field BKI hints become
  field attributes: `#[bki(default = "-")]`, `#[bki(lookup = "pg_proc")]`,
  `#[bki(force_not_null)]`. This is the "genbki macro in Rust" idea, and it fits:
  every `_d.h` value is a pure function of field order + the header annotations,
  which is exactly what a derive macro sees.
- **Seed data (`.dat`) -> `build.rs`.** The `.dat` files are data, not code; keep
  them verbatim (so we can re-sync with upstream PG) and parse them in `build.rs`
  to emit (a) the `static` seed-row tables and (b) the symbolic OID constants
  (`TIMESTAMPOID = 1114`, `F_*`, etc. - rows with `oid`/`oid_symbol`).
- **Bootstrap.** PG's textual `postgres.bki` intermediate is unnecessary: initdb
  can insert the seed rows directly from the Rust row tables. One whole artifact
  (`.bki`) and its parser disappear.

A declarative `macro_rules!` cannot count/index named fields - this needs a
**proc-macro derive**. The `.dat` parsing is better as `build.rs` than a macro
(it is bulk data, and proc-macros reading external files are awkward).

## 3. fmgr - build.rs + attribute registration

Same input as catalog (`pg_proc.dat`). `fmgroids.h` (`F_*` oid defines) ->
`build.rs` consts. `fmgrprotos.h` (extern prototypes) is **not needed** - Rust
has no separate declaration step. `fmgrtab.c` (sorted oid->fnptr table with
strict/nargs) -> a `build.rs`-emitted static table keyed by oid (matches PG's
binary-search lookup, no startup cost).

## 4-6. errcodes / wait events / lwlocks - trivial build.rs

All three are "parse a flat list, emit consts + a name lookup":

- `errcodes.txt` -> `ERRCODE_*` SQLSTATE consts (the PL-language variants are just
  alternate emitters reading the same file).
- `wait_event_names.txt` -> wait-event enum + `name()` method. Merge with the
  hand-written `wait_event.h` into one `utils::wait_event` module (this is what
  removed the only include cycle).
- `lwlocklist.h` -> built-in LWLock enum + names array.

## 7. Keywords - use `phf`

`gen_keywordlist.pl` builds a sorted offset table for binary-search keyword
lookup. Replace the whole `ScanKeyword` apparatus with a compile-time perfect
hash via the `phf` crate, built from the keyword list in `build.rs` (or a
`phf_map!` macro). The hand-written `kwlist.h` inputs stay as the source list.

## 8. Unicode - port generators to build.rs (fidelity over convenience)

`unicode_*_table.h` / `unicode_norm_*` are pure lookup tables for case mapping,
normalization, category, and East-Asian width. Crates exist
(`unicode-normalization`, `unicode-properties`), but PG pins a specific Unicode
version and has its own normalization quickcheck/hash layout; mismatches are
correctness/compat bugs. Port the generators to `build.rs` emitting the same
tables, with the UCD input files **vendored** (PG downloads them at build time -
avoid network in `build.rs`).

## 9. Snowball stemmers - vendor, don't translate

The Snowball `stem_*.h`/`.c` are emitted by the upstream Snowball compiler from
`.sbl` sources. Snowball has a Rust backend and the `rust-stemmers` crate already ships
these. Use the crate (or regenerate via Snowball's Rust backend) behind PG's
text-search dictionary interface. Never hand-translate.

## Sequencing note

Two families gate large amounts of downstream code and should land early:
the **catalog derive + `.dat` build.rs** (the many `Anum_*`/OID references) and
**node-support derives** (every planner/parser/executor node). errcodes, wait
events, and lwlocks are quick and also widely referenced, so do them in the same
early pass.

# Plan 001 - Header file translation

Translate PostgreSQL's C headers under `src/include/` into Rust, one module per
header, as the foundation for the wider C-to-Rust port. This phase establishes the
type/signature skeleton of the whole codebase: signatures are translated, and
bodies are stubbed - except functions defined inline in a header, which are
translated in full.

## Goal & mapping

Each `src/include/path/to/module.h` becomes `src/path/to/module.rs`. We make types
and signatures line up, validated with `cargo check`. The full mapping rules are
applied in Phase 1.

## Note

1. Process batches serially - one agent at a time - to avoid concurrent
   `cargo check`. Within a level, batches have no interdependencies, and the rule
   4/5/6 sub-agents report back to the batch agent.

## On-disk format

On-disk-layout headers are translated faithfully and stay mechanical: keep field
order and types exactly and add `#[repr(C)]` (see `translation-rules.md`). This is
why the carve-out is decided per header, not per subsystem - a rewritten subsystem
still has on-disk-format headers (e.g. `bufpage.h`, `itemid.h`, `xlogrecord.h`)
that remain mechanical.

## Workflow

### Phase 0: scaffold every module

Create an empty `.rs` file for each `.h`. Add a `mod.rs` under each directory that
`pub use`s every module in that directory, and one `lib.rs` under `src` that
`pub use`s all modules. This guarantees **module-path resolution**: every
`crate::a::b` path resolves to an existing module, so `mod`/`pub use` lines and any
`use crate::a::b` (importing the module) compile regardless of order.

It does **not** make *item* imports succeed: `use crate::a::b::Foo` still errors
(`E0432`) until `Foo` is actually defined in module `b`. Item imports therefore
depend on the topological order (a header's includes are translated before it),
plus rule 7 below for forward references the include graph doesn't cover.

### Phase 1: translate in topological order

Postgres header files form a DAG; `h-file-list/` holds the topological layering,
where level 1 includes no other in-tree headers and each later level includes only
headers from earlier levels.

#### h-file-list layout

Every header maps to a `.rs` file (Phase 0 scaffolds all of them). `h-file-list/`
records *what to do* with each header, in topological order. Numbering: level N is
file `N00`, zero-padded (`0100`, `0200`, ...), leaving `N01`..`N99` free for
per-level carve-outs that sort right after the level.

- **`XX00.txt`** - the mechanical headers at level XX, one per line. Apply the
  rules below; stub bodies. The large majority.
- **`XXYY.txt`** (`YY` = `01`, `02`, ...) - carve-outs at level XX, each naming a
  header (or small group) plus a short directive. Three kinds:

  - **foundation-rewrite** - types/API are redesigned later, so mechanical
    translation would be thrown away. If other headers depend on it, stub its
    *public* API here (enough for `cargo check`) and defer its implementation; if
    it is internal to its subsystem, defer it wholesale. The directive names the
    target design.
  - **deleted/replaced** - subsumed by tokio/std, no real Rust module. Write a
    **tombstone `.rs`**: a dummy file whose comment says what replaced it (e.g.
    spinlocks -> `std`/`parking_lot`; `palloc`/MemoryContext -> Rust ownership; the
    shmem/IPC segment -> tokio + `Arc`). An agent that reaches a dependent finds the
    tombstone and applies the replacement instead of a `use`.
  - **generated** - produced by a derive macro or `build.rs` from a source file,
    not hand-translated. build.rs-driven outputs (errcodes, wait-event, unicode,
    ...) produce a module that dependents `include!`, and are placed so that module
    exists before its dependents. Derive-driven outputs (catalog/node `_d`) have
    **no separate module** - the derive runs on the translated source struct, and
    are listed beside it. The directive names the generator and its source.

Walk the level files in order. Within a level, process the `XX00.txt` mechanical
batch (one agent per batch) applying the rules below, and handle each `XXYY.txt`
carve-out per its directive. (Same level = no inter-dependencies.)

#### Rule 1: includes become `use`

`#include "other/module.h"` -> `use crate::other::module::{...}`. Import only the
items actually used, growing the list as translation proceeds.

#### Rule 2: definitions become `pub` items

Every definition - type, macro, enum, struct, constant, function signature -
becomes a `pub` Rust item.

#### Rule 3: declarations stubbed, inline definitions translated

A bare function *declaration* (`void f(...);`) becomes a signature with an
`unimplemented!()` body. A function *defined* in the header (`static inline void
f(...) { ... }`) is translated in full - signature and body. Signatures must be
correct either way; bodies of bare declarations come in a later phase.

#### Rule 4: bitflags

If there are bitflags-like `#define`/`struct`/..., use a sub-agent that reads
`bitflags-port.md`.

#### Rule 5: functions

For function declarations and definitions, use a sub-agent that reads
`function-mapping.md`.

#### Rule 6: routine/method structs

If there is a routine/method struct, use a sub-agent that reads
`routine-struct.md`.

#### Rule 7: forward declarations

The header include graph is near-acyclic only because PostgreSQL breaks type
cycles with forward declarations - using a struct by pointer without including
its defining header. These items are not reachable through the include edges, so
the topological order does not place their definition first. When you hit one,
search for its real definition in C and translate it **here**, locally.
**IMPORTANT**: leave a `#[deprecated]` and a `// TODO(struct-forward)` marker so
Phase 2 can repoint it at the canonical module.

Two forms occur, both real in the tree:

- **`typedef struct X *Ptr;`** - a pointer alias, used to avoid a heavy include.
  `src/include/fmgr.h` does this for `Node` (real definition in
  `src/include/nodes/nodes.h`) and for `StringInfoData` (real definition in
  `src/include/lib/stringinfo.h`):

  ```c
  /* We don't want to include primnodes.h here, so make some stub references */
  typedef struct Node *fmNodePtr;
  /* Likewise, avoid including stringinfo.h here */
  typedef struct StringInfoData *fmStringInfo;
  ```

- **bare `struct Foo;`** - an incomplete type. `src/include/access/tableam.h`
  forward-declares a block of them (e.g. `IndexInfo`, real definition in
  `src/include/nodes/execnodes.h`):

  ```c
  struct BulkInsertStateData;
  struct IndexInfo;
  struct SampleScanState;
  ```

Hint for the sub-agent: these forwards almost always carry a comment naming the
header being avoided (`/* We don't want to include primnodes.h here ... */`),
which points straight at the canonical definition for both the local translation
and the Phase 2 fixup.

#### Rule 8: validate with `cargo check`

Validate with `cargo check` only, never `cargo build`. We are checking that types
and signatures line up, not producing a runnable binary.

Construct-level conventions (types, macros, utilities, and the cross-cutting
consequences of the async model) are detailed in
[`translation-rules.md`](./translation-rules.md). Rules 4/5/6 above point at the
per-construct docs (`bitflags-port.md`, `function-mapping.md`,
`routine-struct.md`); `translation-rules.md` is the broader reference.

Self-contained, low-level utilities that sit in early topo levels and depend on
nothing unresolved (`stringinfo`, `bitmapset`, `elog` formatting, simple data
structures) are **implemented inline** during translation rather than stubbed -
they are not carve-outs. The heavy concurrency subsystems are the
*foundation-rewrite* carve-outs above.

All function signatures are translated **synchronous** in this phase. Async
coloring is **not** done here; it happens in a later implementation pass, where the
real `.await` points are known.

Generated and deleted/replaced headers are handled as their carve-out buckets
above (the per-family generated approach is in `generated-header-files.md`).

### Phase 2: resolve all `TODO(struct-forward)`

Walk every `TODO(struct-forward)` marker and replace the locally-translated
forward struct with a `use crate::...` pointing at its real definition, removing
the `#[deprecated]`.

## Crate structure

- **One main crate** mirroring `src/include` as its module tree, to keep
  cross-references frictionless during bulk translation.
- **Proc-macro crate(s)** alongside it (Cargo requires `proc-macro = true` crates
  to be separate): a `#[derive(Catalog)]` for catalog struct shape and a
  `#[derive(Node)]` for node copy/equal/serialize. See `generated-header-files.md`.
- **One root `build.rs`** in the main crate runs the data-driven generators
  (catalog `.dat`, `errcodes.txt`, `wait_event_names.txt`, `lwlocklist.h`,
  Unicode UCD). Cargo allows only one build script per package, at the package
  root; organize it as a `build/` directory of modules. Generated code lands in
  `OUT_DIR` and is pulled into the right module with `include!`.
- Data generators stay in the one build script for this phase; splitting them into
  separate crates is out of scope here.

## References

- [`translation-rules.md`](./translation-rules.md) - construct-level conventions
  (types, macros, utilities, cross-cutting async consequences).
- [`generated-header-files.md`](./generated-header-files.md) - the
  generated/vendored header families, their C generators and inputs, and the
  per-family Rust rewrite strategy.
- `h-file-list/XX00.txt` - mechanical headers per topological level (one per line).
- `h-file-list/XXYY.txt` - per-level carve-outs (foundation-rewrite,
  deleted/replaced, generated), each with a directive.
- `bitflags-port.md`, `function-mapping.md`, `routine-struct.md` - design notes
  for the per-construct sub-agents in Phase 1.

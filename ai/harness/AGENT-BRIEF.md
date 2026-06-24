# Level-1 mechanical translation - agent brief

You translate a fixed list of PostgreSQL C headers into Rust, into files that
**already exist** (Phase 0 scaffolded them). This is the header-skeleton phase:
get **types and signatures** right; stub bodies. Validated later with `cargo check`.

## Inputs
- Your header list: the chunk file path given in your task.
- C sources: `ref/postgres/src/include/<path>.h` (read the real header for each).
- Target Rust file for header `a/b.h`: `src/a/b.rs` (already scaffolded).
- Rule docs (READ THESE FIRST):
  - `ai/plans/001-header-file/README.md` (rules 1-8)
  - `ai/plans/001-header-file/translation-rules.md` (construct conventions)
  - `ai/plans/001-header-file/bitflags-port.md` (rule 4)
  - `ai/plans/001-header-file/function-mapping.md` (rule 5)
  - `ai/plans/001-header-file/routine-struct.md` (rule 6)

## Hard constraints
- **Do NOT run `cargo check` or `cargo build`.** The orchestrator runs a single
  central check after all batches finish (avoids concurrent cargo locks).
- **Edit only the `.rs` files for headers in YOUR list.** Do not touch others.
- **Preserve existing file content markers.** Each target file starts with a
  `//! Translated from ...` doc line - keep it. A few files (directory roots like
  `src/port.rs`) also contain a `// === scaffold: child modules (Phase 0) ===`
  block of `pub mod` lines - **keep that block intact** and add your translated
  items after it.

## Foundational types (these are the ONLY cross-module imports you should need)
Level-1 headers include no other in-tree headers, so you generally need **no**
`use crate::...`. Exceptions:
- `Oid` / `InvalidOid`: `use crate::postgres_ext::{Oid, InvalidOid};`
  (`Oid` is a newtype `Oid(u32)`; `InvalidOid = Oid(0)`). NOTE: if `postgres_ext.h`
  is in YOUR list, you DEFINE these instead of importing.
- `Datum` / `NullableDatum`: `use crate::postgres::{Datum, NullableDatum};`
  (already defined; do not redefine).
- C integer typedefs map directly to Rust primitives - NO import:
  `int16->i16, uint32->u32, int64->i64, Size/Index->usize, bool->bool`.

## Translation rules (condensed)
1. `#include "x.h"` -> normally none at level 1 (they include no in-tree headers).
2. Every C definition (type, enum, struct, `#define` const, macro, fn) -> a `pub`
   Rust item.
3. Bare `void f(...);` -> `pub fn f(...) -> T { unimplemented!() }` (correct
   signature, stub body). `static inline` fn defined in the header -> translate
   **in full** (signature + body).
4. Flag-group `#define`s -> `bitflags!` per bitflags-port.md (check its appendix
   for the GOOD/PARTIAL/OUT-OF-SCOPE/POOR verdict of each group; POOR -> enum,
   on-disk-packed -> raw integer + accessor methods).
5. Function idioms -> Result/Option/tuple/struct/closure per function-mapping.md
   (map by intent: status->Result, not-found-sentinel->Option, out-params->tuple/
   struct, `void*`->generic/enum/closure). Keep signatures **synchronous** (no
   `async fn`).
6. Routine/vtable structs (struct of fn pointers) -> trait + bitflags per
   routine-struct.md.
7. Forward reference to a type the include graph doesn't provide: find its real C
   definition, translate it **locally here**, and mark it:
   `#[deprecated(note = "TODO(struct-forward): repoint to crate::<real::path> in Phase 2")]`
   plus a `// TODO(struct-forward)` comment.

## Type classification (decide per struct)
- **On-disk** (page/line-pointer/tuple/WAL/control-file layouts, per-type binary
  formats): `#[repr(C)]`, exact field order/types, plus layout asserts:
  `const _: () = assert!(core::mem::size_of::<T>() == N);` and key `offset_of!`.
  Bitfields -> a single integer newtype with accessor methods (NOT `#[repr(C)]`
  fields). Target: Linux x86_64 + macOS aarch64 only (64-bit, LE, 8-byte align);
  delete 32-bit/BE paths.
- **In-memory** (everything else): idiomatic Rust (enums, `Vec`, `Option`,
  ownership). No layout contract, no `#[repr(C)]`.

## Platform / replaced shims (applies to the port/ batch especially)
Target platforms are Linux x86_64 + macOS aarch64 only. For headers that are pure
platform/portability shims replaced by Rust std/core, write a **tombstone**: keep
the doc comment, add a short `//` note on the replacement, and translate only what
genuinely carries over:
- `port/atomics/*` (arch-*, generic-*, fallback, generic.h) -> replaced by
  `core::sync::atomic`. Tombstone; do not port asm/intrinsics.
- `port/win32*`, `port/win32_msvc/*`, `port/{cygwin,solaris,freebsd,netbsd,openbsd}.h`,
  `port/{darwin,linux}.h` -> non-target or std-covered. Tombstone (a `//` note).
- `port/pg_bswap.h` -> Rust `.swap_bytes()/.to_be()/.to_le()`; provide thin
  `const fn` wrappers if convenient, else tombstone-note pointing at std.
- `port/pg_bitutils.h` -> Rust integer intrinsics (`leading_zeros`, `count_ones`,
  ...). Translate signatures; bodies `unimplemented!()` or the std one-liner.
- `port/simd.h`, `port/pg_pthread.h`, `portability/mem.h` -> tombstone-note.
- `portability/instr_time.h` -> map to `std::time::Instant`/`Duration` (translate
  the inline ops accordingly, or stub).

## Inline-utility carve-in (lib/ batch)
These low-level data structures are **implemented inline in full** (not stubbed),
mapping to Rust per translation-rules.md's container table:
- `lib/stringinfo.h` -> back with `String`/`Vec<u8>`; translate the append API.
- `lib/binaryheap.h` -> `BinaryHeap`-style; `lib/rbtree.h` -> `BTreeMap`-style;
  `lib/ilist.h` -> `VecDeque`/index-based (intrusive lists are the hard ones -
  model the API, keep it synchronous). `lib/qunique.h` -> slice dedup.
  `lib/sort_template.h` -> a generic `pg_qsort<T>`-style fn. Where a faithful
  inline impl is too large, translate the full public API with correct signatures
  and `unimplemented!()` bodies rather than guessing.

## Output discipline
- Match PG names but make them valid Rust (`non_camel_case_types` etc. are allowed
  crate-wide). Keep comments short (one line). No standalone docs.
- Prefer `const`/`const fn`/`fn` over `macro_rules!` for macros; use `macro_rules!`
  only when a fn cannot express it.
- When done, report: files written, any rule-7 forward decls you added (with the
  target path), and anything you could not classify confidently.

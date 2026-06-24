# Level-2/3 translation addendum (read AFTER AGENT-BRIEF.md)

Levels >=2 differ from level 1: headers now `#include` earlier-level headers, so
you DO add `use crate::...` imports (Rule 1). Everything else in AGENT-BRIEF.md
still applies (stub bodies, inline fns in full, bitflags/enum/Result idioms,
on-disk `#[repr(C)]`+asserts, no `async`, no cargo runs).

## Canonical import paths (earlier levels, already translated)

- `use crate::postgres_ext::{Oid, InvalidOid};`  (Oid is `Oid(u32)` newtype)
- `use crate::postgres::{Datum, NullableDatum};`
- From `c.h` -> `crate::c`: `TransactionId, MultiXactId, MultiXactOffset,
  CommandId, SubTransactionId, LocalTransactionId` (all `u32` type aliases),
  `Pointer, Size (=usize), Index (=usize), Offset (=isize), float4 (=f32),
  float8 (=f64), regproc, RegProcedure, varlena, bytea, text, BpChar, VarChar,
  NameData, Name`, and consts `InvalidTransactionId, InvalidSubTransactionId,
  InvalidCommandId, FirstCommandId, InvalidMultiXactId, NAMEDATALEN`.
  (If `c.h` is in YOUR list you DEFINE these; otherwise import them.)
- C integer typedefs still map straight to Rust primitives (no import).

Map each `#include "a/b.h"` to `use crate::a::b::{the items you use}`. Only import
what you reference; it's fine to under-import in the skeleton.

## Tombstoned includes -> apply the replacement, do NOT import

These headers are tombstones (no real items to import). If a header you translate
includes one, replace the usage with the Rust equivalent instead of a `use`:

- `storage/spin.h`, `storage/s_lock.h` (spinlock) -> `std::sync`/`parking_lot`
- `port/atomics.h` (atomics) -> `core::sync::atomic`
- `storage/latch.h` (Latch) -> `tokio::sync::Notify`
- `storage/lwlock.h` (LWLock) -> `parking_lot`/`std` locks
- `storage/dsm.h`, `utils/dsa.h`, `storage/shmem.h`, `storage/pg_shmem.h`,
  `storage/pg_sema.h`, `storage/ipc.h`, `storage/dsm_impl.h` -> Arc-shared heap
  state / tokio channels (single-process); drop the shmem field or replace with
  the owned Rust type.
- A struct field of type `slock_t`/`LWLock`/`Latch`/`pg_atomic_uint32` etc. ->
  replace with `parking_lot::Mutex<()>` / `AtomicU32` / drop, as fits; leave a
  short `//` note. Keep on-disk structs faithful (those don't embed locks).

## Catalog headers (`catalog/pg_<name>.h`)

These are hand-written BKI source structs. The matching `pg_<name>_d.h`
(generated, no module) normally carries the OID/attnum constants; since the
`#[derive(Catalog)]` macro isn't built yet, HAND-EMIT those constants now and mark
them for replacement. For each `catalog/pg_<name>.h`:

1. `CATALOG(pg_<name>,<oid>,<SymbolRelationId>) ...` -> the struct PLUS
   `pub const <SymbolRelationId>: Oid = Oid(<oid>);`. If it has
   `BKI_ROWTYPE_OID(<roid>,<RowtypeSymbol>)`, also
   `pub const <RowtypeSymbol>: Oid = Oid(<roid>);`.
2. The struct -> `#[repr(C)] pub struct FormData_pg_<name> { <fields in order> }`
   (catalog rows are on-disk). Field C types map as usual (Oid, NameData, bool,
   i32, regproc, text/varlena, `Oid[1]`/`text[1]` arrays). Fields under
   `#ifdef CATALOG_VARLEN` are the variable-length tail - include them with a
   `// CATALOG_VARLEN (not in fixed part)` note. `pub type Form_pg_<name> =
   *mut FormData_pg_<name>;` (add `// TODO(ptr)`).
3. Emit the `_d.h` constants: `pub const Anum_pg_<name>_<field>: i32 = <1-based
   position>;` for every struct field, and `pub const Natts_pg_<name>: i32 =
   <count>;`. Prefix the block with
   `// TODO(catalog-derive): replace hand-emitted _d.h consts with #[derive(Catalog)]`.
4. BKI metadata macros (`DECLARE_UNIQUE_INDEX`, `DECLARE_INDEX`,
   `DECLARE_FOREIGN_KEY`, `MAKE_SYSCACHE`, `DECLARE_TOAST`, `DECLARE_OID_DEFINING`
   ...) -> a one-line `//` note each; do NOT try to model indexes/syscaches now.
   (`DECLARE_OID_DEFINING(Symbol, oid)` -> emit `pub const Symbol: Oid =
   Oid(oid);`.) The seed `.dat` rows are out of scope (build.rs later).

## elog.h (foundation-rewrite, if in your list)

Keep `elog(ERROR)`/`ereport(ERROR)` semantics as a panic (the eventual
`catch_unwind` at task spawn). Translate the macros `ereport/elog/errmsg/errcode/
errdetail/...` as functions/macros that ultimately `panic!` for >=ERROR and `log`
for lower severities. Mark each error-raising path `#[deprecated(note = "TODO(panic):
migrate to Result + ?")]` and add `// TODO(panic)`. Translate the enums
(`PG_DIAG_*` if present, elevel constants WARNING/ERROR/FATAL/PANIC/...) as consts/
enum. `ErrorData`/`ErrorContextCallback` -> structs (the `void *arg` -> closure note).

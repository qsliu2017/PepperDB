# Translation rules (construct-level)

Detailed conventions the Phase 1 sub-agents follow when mapping C constructs to
Rust. These refine the high-level mapping rules in `README.md`. The overriding
constraint is the compatibility invariants (see README "Invariants"): anything
persisted or protocol-visible must stay bit-compatible; everything else is free
to become idiomatic Rust.

## Enabling assumption

We target **Linux x86_64 and macOS aarch64 only** - both 64-bit, little-endian,
8-byte aligned. So `Datum` is 8 bytes, `MAXALIGN` is 8, pointers are 8 bytes.
Delete all 32-bit and big-endian code paths; do not port them.

## Types

### On-disk vs in-memory - decide first

Classify every struct before translating it:

- **On-disk** (pages, line pointers, heap/index tuples, WAL records, control
  file, catalog rows, per-type binary formats): host-native layout, read/written
  as raw bytes. Keep field order and types exactly; add `#[repr(C)]` and static
  layout asserts (below). Never reorder or "optimize". **Only strict on-disk
  structs get `#[repr(C)]`.**
- **In-memory** (everything else - planner/executor state, caches, *and* libpq
  wire messages): no layout contract. Model idiomatically (enums, `Vec`,
  `Option`, ownership). Wire messages still serialize to a byte-exact format, but
  via explicit big-endian (de)serializers (`pq_getmsgint(msg, b)` etc.) on
  ordinary Rust types - they are **not** `#[repr(C)]` and must not be struct-punned.

The on-disk set is small and contained: the storage/access page + tuple + WAL
layer (a few dozen structs), plus catalog rows (handled by `#[derive(Catalog)]`)
and per-type binary formats (localized to each type's module). The
layout-critical unsafe lives here, not spread across the codebase.

### Layout assertions for on-disk structs

```rust
#[repr(C)]
pub struct PageHeaderData { /* fields in C order */ }
const _: () = assert!(core::mem::size_of::<PageHeaderData>() == 24);
const _: () = assert!(core::mem::offset_of!(PageHeaderData, pd_lower) == 12);
```

### Bitfields - `#[repr(C)]` does NOT cover these

C bitfields are on disk (e.g. `ItemIdData { lp_off:15, lp_flags:2, lp_len:15 }`
in every heap page). Rust has no bitfields. Translate to a single integer newtype
with accessor methods replicating the exact bit packing:

```rust
#[repr(transparent)]
pub struct ItemIdData(u32);          // lp_off:15 | lp_flags:2 | lp_len:15
impl ItemIdData {
    pub fn lp_off(self) -> u16 { (self.0 & 0x7fff) as u16 }
    // ...
}
```

### Flexible array members

Rust has no FAM. Split by category:

- **In-memory FAM** (e.g. `List.initial_elements[]`): irrelevant - the struct
  becomes a `Vec`/idiomatic type and the FAM disappears. No unsafe.
- **On-disk FAM** (e.g. `varlena.vl_dat[]`, a page's line-pointer array, a heap
  tuple's null bitmap + user data): the bytes live in a buffer and the header is
  a view over it. Translate the fixed header as a `#[repr(C)]` struct; the
  trailing length is **always derivable from header fields** (varlena `VARSIZE`;
  page `pd_lower`/`pd_upper`; tuple `t_hoff`), so compute it and expose the data
  as a slice behind a safe accessor that contains the unsafe:

```rust
impl HeapTupleHeader {
    /// SAFETY: `self` points into a tuple buffer of its recorded length.
    pub fn user_data(&self) -> &[u8] {
        let off = self.t_hoff as usize;
        unsafe {
            core::slice::from_raw_parts(
                (self as *const Self).cast::<u8>().add(off),
                self.len() - off,
            )
        }
    }
}
```

Hand back a **typed** slice (`&[ItemIdData]`, `&[int16]`) instead of `&[u8]` only
where MAXALIGN guarantees element alignment at that offset; otherwise return
`&[u8]` and parse. Mutation uses `from_raw_parts_mut` with the same length and
alignment rules.

### `<A>Data` / `<A>` pointer pattern

PG names a value struct `FooData` and its pointer `Foo`. Keep the value struct,
named `Foo` (drop the `Data` suffix), and represent pointers as references:

| C | Rust |
|---|---|
| `FooData` (value) | `Foo` |
| `Foo` (non-null pointer) | `&Foo` / `&mut Foo` |
| `Foo` (nullable pointer) | `Option<&Foo>` / `Option<&mut Foo>` |
| `Foo` used as array base | `&[Foo]` / `&mut [Foo]` |

Ownership (borrow vs `Box<Foo>` vs raw `*mut Foo`) often can't be read from the
header. Default to references where the API is clearly borrowing; leave a
`// TODO(ptr)` where the signature does not reveal ownership, to revisit with the
`.c` body.

### Integer and scalar types

Use Rust types; do not use C types in pure-Rust code (`core::ffi::c_*` is a code
smell *except* at genuine OS/libc FFI boundaries).

| C / PG typedef | Rust |
|---|---|
| `int16`/`uint16`, `int32`/`uint32`, `int64`/`uint64` | `i16`/`u16`, `i32`/`u32`, `i64`/`u64` |
| `Size`, `Index` | `usize` |
| `bool` (1 byte) | `bool` (read on-disk bytes as `u8` then convert; raw bytes need not be 0/1) |
| `Oid` | newtype `Oid(u32)` |
| `Datum` = `uintptr_t` | newtype `Datum(usize)` - pointer-width, **not** `u64`; it aliases pointers |
| `char *` C string | `&CStr` / `CString` at boundaries, `&str` / `String` internally |

### Strings and encoding

Server encoding is **UTF-8 only**. Split like other types:

- on-disk `text`/`bytea` = varlena (keep layout); `NameData` = `[u8; NAMEDATALEN]`
  (64) fixed.
- in-memory = `String` / `&str` (text) or `Vec<u8>` / `&[u8]` (bytea), `CStr` at
  C-string boundaries.

### Collections - the bounded set of PG containers

Map PG's generic containers to Rust equivalents. The full set:

| PG type (header) | Rust mapping | Note |
|---|---|---|
| `List`, `ListCell` (`pg_list.h`) | `Vec<T>` | array-based - exact, not approximate; `NIL` = empty; `foreach` = iterator |
| `StringInfoData` (`stringinfo.h`) | `String` / `Vec<u8>` | |
| `HTAB` dynahash (`hsearch.h`) | `HashMap` / `hashbrown` | |
| `simplehash` `SH_*` (`simplehash.h`) | `HashMap` | templated open-addressing |
| `Bitmapset` (`bitmapset.h`) | newtype over `Vec<u64>` or `fixedbitset` | heavy in planner |
| `dlist`/`slist`/`dclist` (`ilist.h`) | `VecDeque` or index-based | intrusive - Rust resists; the hard ones |
| `binaryheap`/`pairingheap` | `BinaryHeap` | |
| `rbtree.h` | `BTreeMap` | |
| `radixtree.h` | `BTreeMap` / crate | |
| `dshash` (shared-mem hash) | `HashMap` behind a lock | shmem is a non-goal; collapses under single-process |
| `Tuplestore`/`Tuplesort` | bespoke | spill-to-disk; keep |

### Enums and bitflags

C enums -> Rust enums when exhaustive. C enums used as OR-able flags, and flag
`#define` groups -> `bitflags`. See `bitflags-port.md`.

## Macros

### Object-like and pure function-like macros

- `#define X 5` -> `pub const X: T = 5;`
- pure function-like macro -> `const fn` or a regular (inline) `fn`; prefer this
  over `macro_rules!`. Use `macro_rules!` only when a function cannot express it
  (e.g. variadic, type-generic over many call sites).

### Layout macros

`sizeof` -> `size_of`, `offsetof` -> `offset_of!`. Needed mainly for on-disk
layout asserts; avoid elsewhere.

### Platform `#ifdef` -> `#[cfg(...)]`

Only Linux x86_64 + macOS aarch64. Most of `src/port/` portability shims are
unnecessary - use Rust std:

- `port/atomics.h` -> `core::sync::atomic` (wholesale replacement)
- file I/O, threads, sockets, time, env -> `std`
- `setjmp`/`longjmp` -> see error model below

Locale/collation/encoding are **not** "just cross-platform": index sort order is
collation-defined and persisted. "English only" means **C (byte) collation +
UTF-8 encoding** explicitly; do not silently drop locale handling.

## Utilities - use std/crates, but guard compatibility

Replace pure-internal utilities with std/crates freely (`qsort` -> `slice::sort`,
`snprintf` -> `format!`, getopt -> `clap`, base64/md5/sha -> crates).

**Do NOT blindly replace** utilities whose output is persisted or
protocol-visible - these must stay bit-compatible:

- hash functions (`hash_any`/`hashfn`) - hash indexes and hash partitioning route
  rows by them
- `pg_crc32c` - WAL integrity (CRC32C/Castagnoli; verify any crate's seed/xorout)
- regex *semantics* - SQL `~` / `SIMILAR TO` differ from the `regex` crate's
- float/numeric text output - client-visible

(An agent sweep can enumerate candidate std-replaceable utilities, but each must
be tagged compat-sensitive or free.)

## Cross-cutting (consequences of the async, single-process model)

### Error model

`elog`/`ereport` use `setjmp`/`longjmp` (`PG_TRY`/`PG_CATCH`). The end goal is
`Result` + `?`. Up front, keep `elog(ERROR)` semantics as a panic contained by one
`catch_unwind` at task spawn, and mark each such path with
`#[deprecated]` and `// TODO(panic)` for later migration. Lower severities
(`WARNING`/`LOG`/...) just log and return.

### Async coloring is deferred

Translate **all** function signatures as synchronous in the header skeleton - no
`async fn`. Async coloring is a later implementation pass that spreads outward from
the I/O leaves (buffer/WAL/lock/network) as those get real implementations, so the
await points are known. Do not guess async-ness during the skeleton.

Two coding invariants apply to translated code:

- **No `.await` while holding a synchronous lock guard** - hot critical sections
  stay await-free; only the wait awaits, after the guard drops.
- A future waiting in a shared queue must remove itself from the queue on `Drop`.

### Global/session state

PG relies on process-global variables (`MyProc`, `CurrentMemoryContext`, GUCs,
...). Under single-process async they cannot stay process-globals: convert to
task-local state or a `Session`/execution context threaded through call paths.
Pervasive - plan for it, don't treat per-file.

### Memory, IPC, locking (simplifications)

- shared memory -> normal heap + `Arc`/locks (single process).
- memory contexts -> arena/`Box` + RAII; the reset-on-abort behavior maps to
  scoped drop.
- LWLocks/spinlocks/latches -> `std`/async locks and wakers (`tokio::sync`).
- blocking work (disk I/O, sorts) in async needs `spawn_blocking`/a thread pool -
  PG calls blocking syscalls freely, so this is the one place async adds cost, not
  just removes it.

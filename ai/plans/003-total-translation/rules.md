# Plan 003 - Total translation: rules for turning a C file into a Rust file

Audience: an agent translating a PostgreSQL `.c` file (in `ref/postgres`) into the
PepperDB Rust port. These rules generalize what Phase F0 (plan 002 steps 01-09)
established. Foundation primitives the rest of the tree depends on now exist - use
them, do not reinvent them.

Background you must read once: `ai/plans/000-foundation-design/README.md` (the
single-process async-tokio model), `ai/plans/001-header-file/translation-rules.md`
(header/type/macro construct rules), and the per-construct notes
`ai/plans/001-header-file/{bitflags-port,function-mapping,routine-struct}.md`.

---

## 0. The mental model

PostgreSQL is multi-process with a shared-memory segment and `longjmp` errors.
PepperDB is one process on the tokio multi-thread runtime: each backend and each
auxiliary process is an async task; shared state lives on the heap behind `Arc`
with each structure owning its own locking; errors are panics caught at the task
boundary. Translation is therefore not mechanical transliteration - it is
re-expressing C intent in that model. Three dispositions per `.c` file:

- **full** - translate the whole file's behavior (the default).
- **rewrite-to-design** - the file's mechanism is replaced (e.g. the VFD pool over
  `IoBackend`, resowner over RAII); translate the *intent*, delete the C mechanism.
- **tombstone** - the file is subsumed by tokio/std/Arc (fork/exec, DSM, shmem
  segment, spinlocks, MemoryContext); write a tombstone note, do not translate.

The file-list under `ai/plans/002-foundation-implement/file-list/NN.txt` records
each file's disposition for the foundation; later phases get their own lists.

---

## 1. Path mapping

A `.c` definition file maps 1:1 by path under `src/backend/`, KEEPING the
`backend/` segment, so the Rust file cross-references its C source:

```
ref/postgres/src/backend/utils/error/elog.c  ->  src/backend/utils/error/elog.rs
ref/postgres/src/backend/storage/file/fd.c   ->  src/backend/storage/file/fd.rs
```

Headers were already translated in plan 001 with `include/` STRIPPED:

```
src/include/utils/elog.h     ->  src/utils/elog.rs
src/include/storage/fd.h     ->  src/storage/fd.rs
```

A genuinely Rust-native abstraction with no C counterpart goes in a sensibly named
module (it is not a translation): `src/shared_state.rs`, `src/session.rs`,
`src/storage/wait_guard.rs`, and `GenSlab` living in `src/storage/procnumber.rs`.

Scaffold the `src/backend/...` tree as you go: add `pub mod <child>;` lines to each
`mod.rs` (same style as the include-tree scaffold) and create only the dirs a step
needs. Validate with `cargo check` (never `cargo build`, except once to confirm a
new bin links). It must stay green.

---

## 2. Declaration / definition split, and updating the header

C splits declaration (header) from definition (`.c`). Preserve that split:

- **Header module (`src/<p>.rs`)** keeps header-origin items: types, consts,
  macros, and `static inline` functions defined in the header. It does NOT keep
  function bodies that come from a `.c`.
- **Backend module (`src/backend/<p>.rs`)** holds the `.c` function bodies as
  `pub`, plus the file-local `static` helpers, file-local statics, and the
  `#[cfg(test)]` tests (all private to that module unless the header declared them).
- Every former `unimplemented!()` stub in the header is replaced - either by a
  `pub use` re-export or a deprecated shim (section 3), so existing
  `use crate::<header>::<name>` call sites keep resolving unchanged.

Globals declared in a header but defined in a `.c` follow the same rule: define in
the backend module, re-export/shim from the header.

---

## 3. Method over function

Prefer idiomatic Rust methods over free functions for self-contained, type-centric
files (a struct with operations: `Latch`, `ConditionVariable`, `WaitEventSet`,
`ResourceOwner`, `ProcSignal`, the VFD `File`).

- **Backend module**: idiomatic methods on the type.

  ```rust
  impl Latch {
      pub fn new() -> Self { ... }
      pub async fn wait(&self) { ... }
      #[inline] pub fn init(&self) { self.reset() }
      pub fn set(&self) { ... }
  }
  ```

- **Header module**: keep each original C-named free function as a thin
  `#[deprecated]` `#[inline]` shim delegating to the method - this preserves
  cross-reference (grep for the C name) and lets mechanical ports compile, while
  the deprecation nudges new code to the method.

  ```rust
  #[deprecated(note = "use `latch.set()`")]
  #[inline]
  pub fn SetLatch(latch: &Latch) { latch.set() }
  ```

  (The attribute key is `note`, not `message`. The inherent `impl` block lives in
  the backend module even though the `struct` is in the header module - same crate,
  allowed. Make struct fields `pub(crate)`.)

For NON-type-centric files (global-state functions with no natural `self`, e.g.
`elog.c`, `interrupt.c`): keep the function form - define `pub fn` in the backend
module and rewire the header stub to `pub use crate::backend::<p>::<name>;` (no
deprecated shim).

Deprecated shims must NOT be called internally (call the method/real fn instead),
so no deprecation warnings appear in `cargo check`.

---

## 4. The full-file principle

Translate the ENTIRE file's behavior; do not leave a half-translated file for a
later stage to "remember". A function that calls a not-yet-implemented subsystem
calls its existing `unimplemented!()` stub - that compiles and is correct staging.
This is different from deleted-by-redesign C (OS portability, fork/exec, Windows,
`sync_file_range`): that is removed entirely with a `// deleted by redesign:` note,
which is the redesign, not a partial translation.

Async coloring is the one thing that legitimately revisits an already-translated
file: when a leaf becomes `async`, the `async` propagates up its callers. That is
expected; the file's *logic* is still translated once.

---

## 5. Async coloring

Async spreads outward from the I/O / wait leaves; everything that transitively
reaches an `.await` becomes `async`.

- **Leaves**: `IoBackend` file I/O, the latch wait, socket I/O, WAL fsync, lock
  waits. These are where `async` originates.
- **Stays synchronous**: flag setters, `SetLatch`/`wake_one`/`Signal`,
  `ProcessInterrupts`, and any hot critical section. These must be callable from
  deep sync code; only the *wait* side is `async`.
- **Positional file I/O** (concurrent reads/writes at offsets on one shared
  handle): `std::os::unix::fs::FileExt::{read_exact_at, write_all_at, ...}` run on
  `tokio::task::spawn_blocking` (this is why `IoBackend` uses `std::fs::File`, not
  `tokio::fs::File` - a single cursor cannot serve concurrent positional access).
- **Sequential / socket I/O** (WAL append, the libpq wire): `tokio::io::AsyncReadExt`
  / `AsyncWriteExt` (`read_exact`, `write_all`). Do not hand-roll the loops.

THE hard invariant: never hold a synchronous lock guard (`std::sync::Mutex`/
`RwLock`) across an `.await`. Take the lock, compute, drop the guard, THEN await. A
future waiting in a shared queue must remove itself on `Drop` (cancellation safety):
that is what `WaitGuard` is for.

---

## 6. Translating common foundation constructs

### 6.1 `static` per-process variables

PostgreSQL's process globals split by who reads/writes them:

- **Process-wide config** (e.g. `DataDir`, the sizing GUCs): a `ProcessConfig`
  reachable from `SharedState` (`src/backend/utils/init/globals.rs`). Set once at
  startup.
- **Per-backend identity / session** (e.g. `MyProcPid`, `MyDatabaseId`, the
  user-id stack): the per-task `Session` (`src/session.rs`), published as a tokio
  `task_local!` `Arc<Session>` with `current()`/`try_current()`/`scope()`.
- **Per-task state that ANOTHER task must set** (interrupt/cancel flags): not a
  `task_local` (only the owner could set it) - put it in a shared per-task slot in
  a generational slab as atomics, settable cross-task by `Key` via a registry
  (`ProcSignal` is the model). The owning task holds a cheap `task_local` handle to
  its own slot for fast reads.
- **Ex-shared-memory state** (the shared segment): typed `Arc` fields on
  `SharedState`, each structure owning its own locking; cloned into tasks by `Arc`.

Per-task state MUST be `Send` - use atomics (`AtomicU32`/`AtomicI32`/`AtomicBool`),
`Mutex`/`RwLock`, and `Arc`, NEVER `Rc`/`Cell`/`RefCell` - because backends run on
the multi-thread runtime via `tokio::spawn` and may migrate threads across an
`.await`. (`Session` uses atomics + a `Mutex<String>` for exactly this reason.)
Replace the header's `pub static mut X` with `#[deprecated]` accessor functions
reading/writing the Session/ProcessConfig/slot - keep a single source of truth.

### 6.2 Error reporting (`elog` / `ereport`)

The end goal is `Result` + `?`; the interim keeps `elog(ERROR)` semantics as a
panic (see `src/utils/elog.rs` / `src/backend/utils/error/elog.rs`).

- `elevel >= ERROR` raises by `std::panic::panic_any(error_data)` carrying the
  structured `ErrorData` value (NOT a bare string), so a `catch_unwind` at the task
  boundary downcasts it back and recovers sqlstate / severity / message / detail /
  context.
- Distinguish severities: `ERROR` is catchable (subtransaction/handler); `FATAL`
  terminates the backend task; `PANIC` (and any critical-section failure) calls
  `std::process::abort()` and must NOT be swallowed by `catch_unwind`.
- Lower severities (`WARNING`/`NOTICE`/`LOG`/`INFO`/`DEBUGx`) format and return;
  they never panic.
- Mark every raising path `#[deprecated(note = "TODO(panic): migrate to Result + ?")]`
  and `// TODO(panic)`.
- The `errordata` stack is per-task (Session-style), not a process global.
  `ErrorData` owns its `String`s (no `palloc`).
- The task spawn wraps the backend future in `catch_unwind` (via
  `futures_util::FutureExt::catch_unwind` + `AssertUnwindSafe`); on a caught panic,
  downcast to `ErrorData`, log it, and end the task without crashing the supervisor.
  RAII `Drop` releases locks/pins/buffers during the unwind.

### 6.3 IPC

The shared segment and its allocators are gone:

- **Shared-memory structs** -> `Arc` fields on `SharedState`; each owns its locking.
- **DSM / DSA / `shm_toc` / `shm_mq`** -> `Arc` + tokio channels; tombstone.
- **`ProcSignal`** (one backend signals another) -> a generational-slab registry of
  per-task slots; each slot has atomic reason/interrupt flags + an `Arc<Latch>` +
  a cancel key. `SendProcSignal` sets the flag (Release) and rings the latch; the
  owner reads (Acquire) at `CHECK_FOR_INTERRUPTS`. Cancel keys are compared in
  constant time. (`src/backend/storage/ipc/procsignal.rs`.)
- **`PMSignal`** (child -> postmaster) -> typed channels to the supervisor task.
- **Latch** (`SetLatch`/`WaitLatch`) -> `tokio::sync::Notify` plus a sticky
  `AtomicBool is_set`: `set` stores the flag and `notify_one` (a stored permit);
  `wait` checks the flag, arms `notified()`, re-checks, then awaits - so a set
  before the wait is never lost. (`src/storage/latch.rs`.)
- **`WaitEventSet`** -> a `tokio::select!` over `AsyncFd` (sockets), `tokio::time`
  (timeout), `Notify` (latch), and a shutdown signal.
- **`ConditionVariable`** -> the reusable `WaitQueue`/`WaitGuard`
  (`src/storage/wait_guard.rs`): a `GenSlab<Waker>` under a `Mutex`; `enqueue`
  returns a guard whose `Drop` dequeues (cancellation-safe); `wake_one`/`wake_all`
  set a sticky `woken` flag. The CV protocol enqueues up front (in
  `prepare_to_sleep`) so a signal racing the predicate check is not lost.
- **`PGSemaphore`** -> `tokio::sync::Semaphore`. **LWLock / spinlock** -> a data-
  owning `parking_lot`/`std` `Mutex`/`RwLock` (wrap the data the lock protects, do
  not reproduce naked locks). **`pg_atomic_*`** -> `core::sync::atomic`.
- **`proc_exit` / `on_shmem_exit` / `before_shmem_exit`** -> RAII `Drop`; the
  callback registry is tombstoned.
- **Interrupts / cancellation**: per-task flags in the shared `ProcSignal` slot;
  `CHECK_FOR_INTERRUPTS` calls the sync, holdoff-gated `ProcessInterrupts`, which
  reads/clears the flags and panics (cancel -> `ERROR`/`ERRCODE_QUERY_CANCELED`,
  terminate -> `FATAL`). Holdoff/crit-section counters are per-task (Session), so
  one backend's holdoff never gates another. A statement/lock timeout is a
  `tokio::time` timer that sets the flag + rings the latch.

### 6.4 Log reporting

Server-log output is a minimal plain-text stderr emitter today
(`send_message_to_server_log` -> `write_console`). The structured/destination
machinery is DEFERRED and its calls land on existing stubs: `log_line_prefix`
grammar, `csvlog`/`jsonlog` formatters, the syslogger pipe, and
`send_message_to_frontend` (the libpq wire send - `pqcomm` is not in the foundation
and is reached only via its stubs). `WARNING`/`LOG`/etc. format and return; only
`>= ERROR` panics (6.2). `emit_log_hook` is a single hook (no dynamic extensions).

---

## 7. Reusable primitives built in F0 - use these, do not reinvent

- `GenSlab<T>` / `Key<T>` (`src/storage/procnumber.rs`) - generational slab; the
  canonical replacement for any fixed slot index (ProcNumber, child slot, VFD,
  proc-signal slot, wait queue, resource registry). A stale `Key` fails lookup, so
  it dedups "released by owner" vs "released by guard" automatically.
- `Latch`, `WaitQueue`/`WaitGuard`, `ConditionVariable` (sections 6.3).
- `IoBackend` (`src/storage/io_backend.rs`) + `FdManager`/`File`
  (`src/backend/storage/file/fd.rs`) - all file I/O goes through these.
- `SharedState` (`src/shared_state.rs`) - the Arc-shared root; add new shared
  subsystems as fields at the position matching ipci.c's `CreateOrAttachShmemStructs`
  order (that order encodes init dependencies; the placeholders mark where).
- `Session` (`src/session.rs`), `ProcSignal`, `ResourceOwner`
  (`src/backend/utils/resowner/resowner.rs`).
- `ErrorData` + the `elog!`/`ereport!` macros (`src/utils/elog.rs`).

---

## 8. Memory management

`MemoryContext` / `palloc` are tombstoned - use Rust ownership and `Drop`. Keep
`work_mem` *accounting* only where spill decisions need it (sort/hash/agg
operators), not a global current-context. Resource cleanup that PG did via
`ResourceOwner` is typed RAII guards plus a phased transaction-abort release order
(`BEFORE_LOCKS` -> `LOCKS` -> `AFTER_LOCKS`), because heterogeneous guard `Drop`
order is not naturally phased; see `ResourceOwner`. Each release runs inside
`catch_unwind` so one bad resource cannot abort the abort.

---

## 9. Concurrency-primitive mapping (from design 000)

| PostgreSQL | PepperDB |
| --- | --- |
| LWLock / spinlock | `parking_lot`/`std` `Mutex`/`RwLock` wrapping the protected data |
| `pg_atomic_*` | `core::sync::atomic` |
| `SetLatch`/`WaitLatch` | `tokio::sync::Notify` + sticky `AtomicBool` |
| `WaitEventSet` | `tokio::select!` |
| `PGSemaphore` | `tokio::sync::Semaphore` |
| ConditionVariable | `WaitQueue`/`WaitGuard` (`GenSlab<Waker>`) |
| shared-memory segment | `Arc` fields on `SharedState`, each owning its lock |
| DSM / DSA / shm_mq / shm_toc | `Arc` + tokio channels |
| `ProcSignal` | generational-slab registry of per-task atomic-flag slots + Latch |
| `PMSignal` | typed channels to the supervisor |
| OS signals | `tokio::signal` driving shutdown/reload/cancel |
| `proc_exit`/`on_shmem_exit` | RAII `Drop` |
| `MemoryContext`/`palloc` | Rust ownership + `Drop` (work_mem accounting in operators) |
| fork/exec, postmaster | `tokio::spawn`; supervisor task |
| `longjmp`/`PG_TRY`/`PG_CATCH` | panic carrying `ErrorData` + `catch_unwind` at the task boundary |

---

## 10. Validation & workflow

- `cargo check` must stay green (lib and, where relevant, bin); never introduce new
  warnings, including deprecation warnings (do not call your own deprecated shims).
- Tests: inline `#[cfg(test)]`; `#[tokio::test]` (often `flavor = "multi_thread"`)
  for anything async or cross-task. Cover the cancellation/race/dedup paths, not
  just the happy path.
- Per file/step: translate, then verify with `cargo check` + tests. In the staged
  foundation work the rhythm was: implement -> autocommit -> independent review ->
  manual gate; keep an equivalent review discipline.

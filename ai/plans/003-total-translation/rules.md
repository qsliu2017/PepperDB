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
module with a **snake_case** name and rewire the header stub to
`pub use crate::backend::<p>::<snake> as <CName>;` so the C-named public API still
resolves. Put the C symbol in the doc comment (`/// PG `AcceptInvalidationMessages``).
Do NOT keep the C PascalCase name on the definition under
`#[allow(non_snake_case, reason = "mirrors the C symbol name")]` - that per-fn allow
is redundant (the crate has a global `#![allow(non_snake_case)]`) and the user
rejected it: snake_case the definition, keep the C name in the doc comment + the
header alias (grep-ability preserved both ways).

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

Shared-state Arc rule: DEREF, do not CLONE. Reaching a `SharedState` subsystem
(`shared.clog()`, `shared.buffers()`) returns `&Arc<T>`; calling a method through it
derefs to `&T` at zero atomic cost. `Arc::clone` is a refcount atomic (and a
cache-line bounce when contended), so clone ONLY when you must own the handle past
the borrow -- i.e. to hold it across an `.await` or move it into a spawned task.
Never `shared.x().clone()` in a hot/per-row path just to call a method; pass `&T`
or `&Arc<T>` and deref.

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

## 7. Reusable primitives - use these, do not reinvent

Built in F0:
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

Built in F1 (storage core):
- `Page` (`src/storage/bufpage.rs`) - `#[repr(C, align(8))]` newtype over
  `[u8; BLCKSZ]`, methods + deprecated C-named shims; the alignment makes the
  `PageHeaderData`/`ItemIdData` overlay casts sound. `Page::checksum`, item ops.
- `BufId` (`src/storage/buf.rs`) - the buffer handle enum
  `{Invalid, Global(u32), Local(u32)}` (not a sign-encoded int); `Buffer = BufId`.
- `BufferPool` (`src/backend/storage/buffer/buf_init.rs`) + `bufmgr.rs`
  (`read_buffer_common`/`flush_buffer`/pin/`LockBuffer`) + `localbuf.rs` + the FSM
  (`src/backend/storage/freespace/`) - all page access goes through the buffer mgr.
- smgr / md (`src/backend/storage/smgr/`) - `SmgrRelation` over `FdManager`;
  `SharedState.sync_requests` is the pending-fsync queue (checkpointer drains it).
- WAL (`src/backend/access/transam/`): `XLogCtl` + `xlog_flush`/`XLogInsert`/
  `log_newpage` (xlog.rs, xloginsert.rs), `XLogReader<F>` (xlogreader.rs),
  `Rmgr`/`GetRmgr` (rmgr.rs); incremental CRC-32C (`src/port/pg_crc32c.rs`).

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

---

## 11. Lessons from F1 (storage core: page, smgr/md, buffer manager, WAL)

These emerged porting steps 10-13; apply them to the rest of the storage/access AMs.

**Types.**
- A value type that gets reinterpreted as a `#[repr(C)]` struct must be ALIGNED:
  make it a `#[repr(C, align(8))]` newtype (`Page`), not a `&[u8]` alias, so the
  pointer-cast to a header struct (`PageHeaderData`/`ItemIdData`) is sound. Prove it
  with `const` size/align asserts.
- Keep genuinely on-disk + arithmetic types as raw scalars (`BlockNumber = u32` +
  sentinel; `ItemPointer`) - an enum would break the on-disk layout and the math.
  Make in-memory HANDLES clear enums (`BufId{Invalid,Global,Local}`), not
  sign-overloaded ints.

**Shared mutable page storage.**
- The buffer pool holds pages in `UnsafeCell` (`PageCell`) with a justified
  `unsafe impl Sync`. It is sound ONLY because mutable page access is exclusive via
  `BM_IO_IN_PROGRESS` (the single IO doer) or the EXCLUSIVE content lock. NEVER form
  `&mut Page` under a SHARED lock (the `fsm_search` bug): a read path under a shared
  lock uses `&Page` only; a write needs the exclusive lock or the IO-in-progress gate.

**Async I/O integration.**
- Positional file I/O = `std::os::unix::fs::FileExt::{read_exact_at, write_all_at}`
  on `spawn_blocking` (the `IoBackend` leaf); read into a page via
  `page.as_mut_bytes()`. EOF / zero-fill (reading past a relation's end) is the
  SMGR layer's responsibility, not the leaf's (the leaf is all-or-`UnexpectedEof`).
- Two-racer coordination: a per-buffer/per-segment IN-PROGRESS flag + a `WaitQueue`
  so exactly one task does the read/write and others await (StartBufferIO/WaitIO).
- A failed async I/O PANICS (elog ERROR/fsync abort). Any in-progress flag set
  before an I/O await MUST be cleared and its waiters woken ON UNWIND - use an RAII
  unwind guard (`InProgressIo`, mirroring PG `AbortBufferIO`), else waiters hang.

**Locks across `.await`.**
- Hot locks are SYNC and dropped before any `.await`: buffer header CAS lock,
  content lock, buf_table shards, the sync-queue/strategy `Mutex`, the WAL insertpos
  bump. Pattern: clone the handle / snapshot the bytes out, drop the guard, then await.
- The ONLY locks held across an I/O await are deliberately-async `tokio::Mutex`es:
  the WAL `WALWriteLock` and the held-exclusive WAL insert locks. Document each.

**WAL durability invariants (load-bearing - silent data loss lives here).**
- The flushed-LSN (an atomic AND a `tokio::watch`) is published ONLY after
  `issue_xlog_fsync`, and MONOTONICALLY (never backward). Group commit: the
  WALWriteLock holder writes+fsyncs for all; waiters await the watch.
- `WaitXLogInsertionsToFinish` is called in `XLogFlush` BEFORE acquiring WALWriteLock
  (deadlock-free: insert-lock then write-lock; eviction writes never wait). WAL
  insert locks are HELD-EXCLUSIVE (PG model): advertise `inserting_at=0` (block-all)
  before reserving, then the real LSN, cleared on release.
- A PARTIAL last page backs the write cursor off to the request LSN (PG
  `ispartialpage`) so a later same-page flush re-writes it - else the second
  record is lost. Test incremental same-page flushes, not just flush-once.
- WAL must be PG-compatible on disk: exact short/long page headers
  (`XLOG_PAGE_MAGIC`, pageaddr, rem_len; long header on segment-first pages),
  24-hex segment naming, and the record with the REAL `xl_prev` folded into the CRC
  (assemble computes a partial CRC over the body; the insert finalizes over the
  header after setting `xl_prev` - mirror `XLogInsertRecord`). CRC-32C is incremental.

**Per-task state held across I/O.**
- A pin/refcount/cache that is held across an I/O `.await` (PrivateRefCount, the
  smgr cache, the local buffer pool, xloginsert staging) MUST be a tokio
  `task_local`, NEVER `std::thread_local` - a thread-local splits across a thread
  migration and corrupts the gated shared count. See [[per-task-state-must-be-send]].

**Idiomatic Rust for dispatch / state machines.**
- A self-contained reader/decoder taking an I/O callback is GENERIC over the
  closure (`XLogReader<F>`), not `Box<dyn>`; keep its produced data type
  (`DecodedXLogRecord`) non-generic so downstream stays simple.
- A C dispatch table of function pointers (`RmgrTable`) becomes a trait + a match
  (`GetRmgr(id) -> &'static dyn Rmgr`, unit-struct impls, inert defaults - NOT
  `unimplemented!()`); keep the per-record arg non-generic so the trait is object-safe.

**Construction order.** Add shared subsystems to `SharedState::new` at the
`TODO(stepNN)` placeholder matching ipci.c's order (xlog before bufmgr; the sync
queue near the checkpointer slot) - it encodes init dependencies.

### Carried-forward TODOs (what F1 deferred; pick up at the named step)
- step 17 checkpointer: drain `SharedState.sync_requests` AND finish the
  fsync-failure / retry / cycle-counter semantics (`sync.rs` `TODO(step17)`);
  `CreateCheckPoint`; source `xlp_sysid` from pg_control (currently a placeholder).
- recovery (out of foundation): `StartupXLOG`/redo (`xlogrecovery.c`) and the async
  WAL page-read driver (`XLogReader` takes a sync callback now).
- `data_checksums_enabled()` is a stub (`PageIsVerified`/`PageSetChecksum*` panic
  until the GUC lands); FSM `fp_next_slot` hint write is dropped (`TODO(perf)`: an
  atomic byte); the buffer victim conditional content-lock is deferred; the smgr EOF
  strict-error path needs the `InRecovery`/`zero_damaged_pages` GUCs.

---

## 12. Lessons from F2 (transaction/MVCC spine: SLRU, clog/subtrans, varsup,
## procarray, snapmgr, combocid, xact)

These emerged porting step 14; apply them to the rest of the access/AM tree.

**SLRU is the second async leaf (after the buffer pool).** `SlruCtl` holds
`banks: Vec<RwLock<SlruBank>>` + a per-slot `WaitQueue`. The bank `RwLock` is the
ex-bank-control-LWLock; PG takes it EXCLUSIVE everywhere except the read-only
status-lookup hit path (`SimpleLruReadPage_ReadOnly`, `LW_SHARED`) - so reads take
`.read()`, read-in/claim/set take `.write()`. LRU-hint counters are atomics so
`SlruRecentlyUsed` is sound under the shared lock (it is a benign-race hint). NEVER
form `&mut` to a page under the shared lock; non-atomic slot fields are write-lock
only. Select-victim and claim (mark ReadInProgress) MUST be ONE critical section
(do not drop the lock between them, or two tasks claim the same slot); only the
physical I/O awaits with the lock dropped, under an `InProgressSlruIo` unwind guard.
The wait path enqueues its `WaitGuard` UNDER the lock (the queue's `woken` flag is
per-slot, so a wake racing an enqueue done after the drop is LOST). Expose access as
a CLOSURE that holds the lock across find+use (`read_page_with`/`read_page_readonly_with`)
- a "return the slot, re-lock by slot" API races eviction (the slot can be repointed
  before the second lock).

**Snapshots are shared, owned values, NOT borrowed.** `Snapshot = Option<Arc<SnapshotData>>`.
Do NOT lend `&'static mut`/`&'static` out of task-local storage (laundering a
task_local borrow to `'static` lets safe code alias `&mut` = UB; the snapshot
manager also mutates the same buffer for `SetCommandId`). Getters return cheap
`Arc::clone`s; `curcid` mutation uses `Arc::make_mut` (copy-on-write). Keep the
shared identity (first-xact snapshot IS the registered Arc, not a second copy).

**Per-task transaction state is `task_local! RefCell<...>`, never `thread_local`.**
The `TransactionState` stack (xact), the active/registered snapshot stacks +
Current/Secondary/Catalog buffers (snapmgr), the combo-cid map (combocid), and the
single-entry `cachedFetchXid` status cache (transam) are all per-backend state held
across `.await`, so they must be `task_local` (thread migration). Never hold a
`RefCell` borrow across an `.await` (borrow, copy/decide, drop, then await).

**Durability ordering (xact RecordTransactionCommit) is load-bearing.** SYNC commit
flushes WAL to disk (`xlog_flush` to >= the commit-record LSN) BEFORE clog is marked
COMMITTED; ASYNC commit (`synchronous_commit=off`) records the commit LSN in clog
(`TransactionIdAsyncCommitTree`) and requests a flush WITHOUT waiting. clog must
never report committed before the WAL is durable in the sync case. The window runs
in a crit section. (`synchronous_commit` is hardcoded ON until GUCs land - `TODO(guc)`.)

**procarray is the one mostly-synchronous subsystem.** `ProcArrayLock` is a
`RwLock` over the procarray data; `GetSnapshotData`/horizons/`IsInProgress` compute
entirely in memory and DROP the guard before any `.await` (clog/subtrans probes
happen after the guard, as PG releases ProcArrayLock before the pg_subtrans probe).
Keep it that way. `GetSnapshotDataReuse` keys on `xactCompletionCount` (init 1, not 0).

**xid comparison is modular, so NOT `Ord`.** `TransactionId::precedes`/`follows` are
METHODS (modular wraparound + permanent-xid special-case); do NOT `impl Ord`/`<` with
precedes semantics (non-transitive -> unsound; breaks sort/min/BTree). A raw derived
`Ord` on `TransactionId` (numeric) is fine for sort/map keys but is NOT transaction
order - document it. `FullTransactionId` (64-bit, monotonic) IS a true total order:
real `Ord`, and `<`-delegating deprecated shims.

**SharedState discipline.** Fields private behind `pub(crate)` accessors, generated
by the `shared_state!` macro (also asserts each field `Send+Sync+'static`). Leaf
routines take the NARROW handle (`&SlruCtl`/`&VariableCache`/`&ProcArray`) or are
methods on it; only genuine multi-subsystem orchestrators (xact, snapmgr) take
`&Arc<SharedState>`. Reaching a subsystem DEREFS (`shared.clog()` -> `&Arc<T>`, free);
`Arc::clone` only to hold across an `.await` or move into a task (section 5). Add new
subsystems to `SharedState::new` at the ipci.c-order marker.

### Carried-forward TODOs (F2 deferred; pick up at the named step)
- step 15 (proc/lock mgr): `proc.c` `InitProcGlobal` populates the real PGPROC array
  that procarray scans (today it runs over the empty `ProcGlobal` stub - snapshots
  are only end-to-end testable after this). Then implement the two group-batching
  perf ops that NEED it: clog `TransactionGroupUpdateXidStatus` (PGPROC.clogGroup* +
  ProcGlobal.clogGroupFirst) and procarray `ProcArrayGroupClearXid`
  (PGPROC.procArrayGroup* + procArrayGroupFirst) - both marked `TODO(step15)`.
- twophase (`PrepareTransaction`), parallel-worker xact state, `xact_redo`/recovery,
  invalidation-message send, and smgr pending-deletes are stubs reached by xact.
- snapmgr `RegisteredSnapshots` is a linear min-xmin scan; PG uses a pairingheap
  keyed by xmin (MODULAR cmp) - needs `lib/pairingheap.c` (a BTreeMap can't
  substitute: modular xid order is not a consistent total order).
- SLRU `physical_read` short-read zero-fill is unreachable (`read_exact_at` ->
  `UnexpectedEof`); only a wholly-absent segment zero-fills. Matters for recovery
  reading a partially-written segment - `TODO(recovery)`.

---

## 13. Lessons from F2 (lock manager: proc, lock, deadlock, lmgr - step 15)

**Shared mutable fixed-size arrays -> index + interior mutability (the BufId/PGPROC
pattern).** A C array of structs shared by all backends and referenced by pointer
(PGPROC, and PG's BUFFER/PROC arrays generally) becomes a fixed `Vec<UnsafeCell<T>>`
(allocated once, NEVER resized/moved), indexed by a Send integer handle (ProcNumber,
like BufId for buffers). Cross-task/shared references MUST be the index, not a raw
`*mut T` (raw pointers are `!Send`; per-task/shared state must be Send, s6.1). Justify
`unsafe impl Send+Sync` on the cell by: the arena never moves, and every mutable field
access is serialized by a documented lock (ProcArrayLock / the partition Mutex /
owner-only). The hot LOCK-FREE-READ fields (the xid/subxid/statusFlags a snapshot
scans) live as PARALLEL mirror arrays of atomics, written under the one lock the
reader also respects - never read a torn compound struct lock-free; mirror the
scalars instead.

**Async grant-wait (ProcSleep) = select! with the wake + timer arms; drop the
partition lock first.** The C "join the wait queue under the partition lock, release
it, then WaitLatch-loop with a deadlock timer" becomes: a SYNC JoinWaitQueue (under
the partition Mutex) that enqueues + sets the proc wait-state, then DROP the Mutex,
then an async `ProcSleep` = `tokio::select!` over the proc's sticky Latch (set by the
waker), a `sleep(DeadlockTimeout)` arm (run the detector), and a `sleep(LockTimeout)`
arm. NEVER hold the partition Mutex across the await. The waker (ProcWakeup, called by
the releaser under the partition lock) sets wait-state THEN `latch.set()` - sticky, so
a set racing the sleeper's arm is not lost.

**EVERY wait must clean up on give-up - one idempotent partition-locked primitive +
an RAII guard.** A waiter can leave the queue four ways: granted (OK), lock-timeout,
hard-deadlock, or FUTURE-DROP (query cancel / select! loser / task abort). All the
non-OK exits must run the SAME cleanup under the awaited lock's partition Mutex:
unlink from the wait queue, undo the request counts (n_requested/requested[mode]),
clear the wait-mask bit, delete the orphan PROCLOCK, GC the LOCK, and ProcLockWakeup
the now-grantable trailing waiters (PG's RemoveFromWaitQueue + CleanUpLock). Make it
IDEMPOTENT (no-op if the proc is no longer WAITING) so the deadlock path (which cleans
under all-partitions) and the wait-site guard don't double-undo. Wrap the
`ProcSleep().await` in an RAII guard that runs this on Drop unless disarmed on OK -
this is the cancellation-safety requirement of s5 applied to the lock wait. The
timeout/deadlock select arms must NOT mutate the shared queue lock-free; they signal
the outcome and let the partition-locked guard do the cleanup.

**Sharded hash tables = `Vec<Mutex<Shard>>`; the shard Mutex IS the partition lock.**
LOCK/PROCLOCK live in NUM_LOCK_PARTITIONS shards (partition = tag hash % N). The shard
Mutex replaces the C partition LWLock - never held across `.await`. Box the entries so
a waiter/holder's raw `*mut LOCK`/`*mut PROCLOCK` stays valid while resident; a LOCK is
GC'd only at n_requested==0, and a queued waiter keeps n_requested>=1, so the pointer a
sleeper holds stays live (don't free an entry anyone still references). The per-task
LOCALLOCK table + fast-path counts are `task_local` (Send: its raw pointers are only
dereferenced under the shard Mutex by the owning task).

**Whole-subsystem critical sections that take ALL partitions (deadlock check) are SYNC
and acquire in a fixed index order, release in reverse** (no `.await` while any is
held; no inter-partition deadlock). The deadlock detector reads the lock graph through
the raw pointers safely precisely because it holds every partition. Per-task deadlock
timers mean two cycle members can both abort (vs PG's single signal-driven victim) -
a fairness/throughput difference, not a soundness bug; record it.

### Carried-forward TODOs (F2 lock-mgr deferred)
- A timer-detected hard deadlock surfaces as `LockAcquireResult::NotAvail`, not the
  "deadlock detected" ERROR (cycle IS detected+broken; only the report is missing) -
  route through `dead_lock_report()` when the panic->Result error model lands.
- `LockRelationOid`/`LockRelation` call `AcceptInvalidationMessages` (sinval, step 16)
  and `IsSharedRelation` (catalog) - translated but PANIC until those land.
- lock groups single-member until F4 (LockCheckConflicts group-subtraction is a no-op);
  2PC `lock_twophase_*`, `pg_locks` (`GetLockStatusData`), resowner->lock accounting
  (a per-task current_owner marker today), autovac SIGINT cancel (step 17).
- the step-14 group-batching ops (clog `TransactionGroupUpdateXidStatus`, procarray
  `ProcArrayGroupClearXid`) are now UNBLOCKED (the PGPROC group fields exist) - still
  `TODO(perf)`, implement when contention warrants.

---

## 14. Idiomatic Rust style (standing conventions)

Apply these everywhere; they make the port read like Rust, not transliterated C.

1. **`let ... else` for the bail-on-None/Err pattern.** Replace
   `let x = match opt { Some(v) => v, None => return };` with
   `let Some(v) = opt else { return; };` (or `continue`/`break`/`?`). Same for the
   common `match get() { Some(g) => g, None => return }` at function tops.

2. **Iterator pipelines over `let mut v = Vec::new(); for ... { v.push() }`.** A
   build-by-push loop with a filter/guard is a code smell; express it as
   `iter.filter(..).filter_map(..).map(..).collect()`. Use `filter_map` + `?` inside
   the closure to flatten nested `if let Some`/conflict guards, and
   `bool::then_some` for the "include this item when cond" tail. Keep an explicit
   loop only when the body has real control flow / side effects that don't map to a
   combinator.

3. **Preallocate filtered collects with `pepperdb_util::PreallocCollect`.** `collect`
   uses the lower size_hint, which `filter` zeroes -> reallocations. Use
   `.prealloc_collect()` (preallocs from the upper hint) for `iter.filter().collect()`
   where most items are kept or the upper bound is a tight small constant
   (MaxBackends, NUM_LOCK_PARTITIONS). For `flat_map`/`flatten` chains (upper hint
   `None`), use `.collect_with_capacity(n)` with a caller-computed bound. Don't use
   it where a filter rejects most of a large input (it would over-allocate).

4. **Enums over sentinel-field pairs that encode a sum type.** When two fields plus a
   sentinel model mutually-exclusive states (e.g. `lock_group_leader: ProcNumber`
   (INVALID if none / self if leader) + `lock_group_members: Vec<ProcNumber>`),
   replace them with an enum that makes invalid states unrepresentable:
   `enum LockGroupRole { None, Leader { members: Vec<ProcNumber> }, Member { leader: ProcNumber } }`.
   Preserve the C semantics (PG's leader points to itself -> the `Leader` variant).

---

## 15. Clippy (enforced; keep it clean)

Clippy is a workspace lint policy in `[workspace.lints.clippy]` (root Cargo.toml;
members opt in via `[lints] workspace = true`). `cargo clippy --all-targets` must be
0 warnings / 0 errors -- do not add lints; a new warning is a regression.

Policy (this is an agent-written port, so we run strict):
- `pedantic` + `nursery` groups = `warn` (priority -1). NOT `clippy::restriction`
  (self-contradictory by design).
- `deny`: `await_holding_lock`, `await_holding_refcell_ref` -- rules s5, the
  load-bearing async invariant. Keep these ALLOW-FREE in production; for genuine
  held-across-await needs use a `tokio::sync::Mutex` (sound), not a std lock + allow.
- `allow` (with the rationale in Cargo.toml): the lints that fight the 1:1 C port
  (too_many_arguments, module_*, unreadable_literal, trailing_empty_array,
  struct_excessive_bools, result_unit_err), the value-cast family
  (cast_possible_truncation/wrap/sign_loss/precision_loss -- intentional C width
  arithmetic in ~900 places), and pure doc/ceremony nags (must_use_candidate,
  doc_markdown, missing_*_doc, missing_const_for_fn, ...). The SOUNDNESS-relevant
  pointer casts stay enforced (cast_ptr_alignment, ptr_as_ptr, ref_as_ptr,
  borrow_as_ptr) plus cast_lossless (widening must use `From`).

Working with it:
- Run `cargo clippy --fix --all-targets` first for the machine-applicable bulk, then
  fix/justify the rest. Hollow `unimplemented!()` stub files: a file-level
  `#![allow(clippy::LINT, reason = "hollow stubs mirror PG sigs; real impl consumes")]`
  is fine (the params aren't consumed yet). Implemented code: prefer the real fix.
- EVERY `#[allow]` needs a `reason = "..."`.
- Clippy is load-bearing, not cosmetic: enabling it caught real bugs -- 8
  `future_not_send` violations (a `!Send` `*mut RelationData` captured across `.await`
  in the relation/page/tuple lock fns, so their futures couldn't be spawned; fixed by
  the sync-outer / `impl Future + Send` pattern) and an unaligned-read UB in
  `RestoreSnapshot`. Treat `future_not_send` / `cast_ptr_alignment` / the await-holding
  denies as bug signals, not style nags.

---

## 16. Lessons from F2 (shared-invalidation transport: sinvaladt/sinval/inval - step 16)

**Follow PG's lock granularity faithfully (the binding decision), even at the cost of
justified `unsafe`.** The SI ring (`SISeg`) keeps PG's two-lock + spinlock scheme:
`SInvalWriteLock` -> `Mutex<SIWriteState>` (writers + register + cleanup), `SInvalReadLock`
-> `RwLock<()>` (readers `.read()` mutate only their OWN ProcState; cleanup `.write()`
does the array-wide pass), and the `maxMsgNum` spinlock -> `AtomicI32` (the spinlock only
ever provided a memory barrier; Acquire/Release gives exactly that). The ring buffer is
`Box<[UnsafeCell<Msg>]>`: a writer fills cell `max % N` under `write` THEN Release-stores
`max_msg_num`; readers Acquire-load `max_msg_num` then read only cells `< max`, so the
pair orders the cell store before the index publish - the SLRU/PGPROC `unsafe impl Sync`
justification (arena never moves; cleanup forces a laggard to reset before the buffer can
wrap a still-needed slot). Per-backend ProcState fields are atomics so two readers under
the shared lock mutate distinct entries soundly. Do NOT collapse this to one Mutex.

**Reuse ProcSignal for the catchup interrupt; send AFTER dropping the lock.** PG's
`SICleanupQueue` signals a laggard via `SendProcSignal(PROCSIG_CATCHUP_INTERRUPT)`; map it
to the existing per-task ProcSignal `CatchupInterrupt` reason bit (s6.1: a per-task flag a
foreign task sets). PG drops both SI locks before the (possibly slow) send, so the cleanup
returns the target `Option<ProcNumber>` and the caller sends only after the `write` guard
is dropped (never hold the lock across the send). Added `ProcSignal::send_by_proc_number`
(PG's `SendProcSignal` targets `psh_slot[procNumber]` by index).

**inval.c's per-backend file-statics -> one `task_local! RefCell<InvalState>`.** The two
dense message arrays + the parent-linked `TransInvalidationInfo` stack (modeled as an owned
`Vec`, "parent" = element below) + the callback lists all live in one per-task RefCell. The
`InvalidationMsgsGroup{firstmsg[2],nextmsg[2]}` index-range bookkeeping is the subtle part -
keep it bit-exact (AddInvalidationMessage appends at `nextmsg`; AppendInvalidationMessageSub
Group asserts `dest.nextmsg==src.firstmsg`; dedup scans the right subgroup). NEVER hold the
RefCell borrow across a callback / `SendSharedInvalidMessages` / `LocalExecuteInvalidationMessage`
(borrow, copy/decide, drop, then call). `ReceiveSharedInvalidMessages`'s recursion-safe
file-static buffer also becomes a per-task RefCell (recursion is within one task).

**Naming: snake_case the backend def, not `#[allow(non_snake_case)]`.** See s3 - a per-fn
`#[allow(non_snake_case, reason = "mirrors the C symbol name")]` is redundant (global allow)
and rejected. Name backend fns snake_case, keep the C symbol in the doc comment + a header
`pub use <snake> as <CName>` alias.

**Staging.** The cache layer (catcache/relcache/syscache/relmapper) is unimplemented;
inval's `LocalExecuteInvalidationMessage`/`CacheInvalidate*` arms call those stubs and only
panic when REAL catalog DDL queues a message (full-file rule s4). `AcceptInvalidationMessages`
on an EMPTY queue is a true no-op, which is what UNBLOCKS the lock manager's relation-lock
wrappers (they no longer panic on the AcceptInvalidationMessages stub).

### Carried-forward TODOs (step 16 deferred)
- The catchup RECEIVE path (`ProcessCatchupInterrupt` / `HandleCatchupInterrupt`) is
  translated but not wired into a main loop (no `ProcessClientReadInterrupt` yet) - dead
  code until step 17+.
- `LocalExecuteInvalidationMessage` smgr arm calls a `smgrreleaserellocator` TODO stub (the
  fn does not exist yet); `LogLogicalInvalidations` leaves the final async `XLogInsert` as a
  `TODO(wal-logical)` (reached only under wal_level=logical); `ProcessCommittedInvalidation
  Messages` DatabasePath-during-recovery is a `TODO(recovery)`.
- All cache-callback dispatch lands on catcache/relcache/relmapper stubs until those land.

---

## 17. Lessons from F3 (auxiliary tasks + sequenced shutdown: checkpointer/bgwriter/
## walwriter/startup/pgarch/autovacuum/bgworker - step 17)

**Every aux process is a long-lived tokio task with ONE shared shape.** `auxiliary_
process_main_common_with_proc` (auxprocess.rs) is the cradle: run inside `my_proc_scope`,
`InitAuxiliaryProcess()` (claims an aux PGPROC + inits its `proc_latch`), then register the
procsignal slot WITH THAT proc_latch so there is a SINGLE unified wakeup latch (a second
sticky latch in the select! busy-spins once anything sets it - the 17a bug). The loop:
`proc_latch.reset()` at top, then `tokio::select!{ biased; proc_latch.wait() | sleep(timeout)
| shutdown.notified() => break }`. A role woken by other backends advertises its ProcNumber
in `ProcGlobal.<role>_proc` (checkpointer/walwriter/autovacuum_launcher); RequestX rings that
proc's latch by number.

**EVERY aux task needs an RAII exit guard (clear advertised proc + deregister slot +
ProcKill) that runs on ALL exits including panic unwind.** The C clears these after the loop;
a panic (e.g. a failed checkpoint re-raises) skips post-loop code and leaves a dangling proc
-> a later RequestX rings a dead latch and hangs. A `Drop` guard covers normal break, early
break, and unwind. PG's sigsetjmp in-loop recovery is NOT reproduced; the task-boundary
`catch_unwind` + the supervisor restart policy replace it.

**Don't let a long-lived task panic on a timer.** A loop that calls an `unimplemented!()`
stub every tick crashes repeatedly. De-fang the SPECIFIC timer-reachable stubs to
non-panicking no-ops with a `// TODO(subsys)` (grep their callers first - only do it if no
caller depends on the panic). Bodies reached only by real work (do_autovacuum, archive a
real .ready file) stay stubbed (s4): they fire only when driven, not on the timer.

**Checkpointer: tombstone the cross-process fsync forwarding; keep the request protocol.**
Backends `RegisterSyncRequest` directly into the shared `SyncRequests` (it dedups), so
ForwardSyncRequest / CompactCheckpointerRequestQueue / the requests[] array / Checkpointer
CommLock are deleted; AbsorbSyncRequests is a no-op. KEEP the ckpt_started/done/failed
counters (modulo compare) + the two ConditionVariables (start_cv/done_cv) - that IS the
backend<->checkpointer handshake. The checkpointer drains the real queue (ProcessSync
Requests + SyncPostCheckpoint).

**Supervisor: spawn aux onto a dedicated JoinSet with a restart policy; on-demand workers
(autovac worker, bgworker) via a process-global OnceLock spawner hook the supervisor
installs.** The respawn arm lives ONLY in the accept loop, never in `drain` (a respawn during
shutdown wedges it). The SEQUENCED SHUTDOWN DRAIN follows PostmasterStateMachine: (1) stop
accepting, (2) terminate+await backends, (3) checkpointer phase-1 (write shutdown checkpoint)
and AWAIT its completion signal (PG PMSIGNAL_XLOG_IS_SHUTDOWN - model as an Arc<Notify> the
checkpointer fires after the write, drain awaits it bounded by the deadline), (4) stop the
other aux tasks (per-role Notify - a SHARED shutdown notify is consumed by the wrong task's
loop-top poll), (5) checkpointer phase-2 (exit). Checkpointer is FIRST-started, LAST-stopped.
Two-phase checkpointer shutdown = two Notifys (phase1=SIGINT, phase2=SIGUSR2).

**Concurrent aux startup is a real concurrency test - it found a latent UB.** Five aux tasks
calling `InitAuxiliaryProcess` at once exposed an unsynchronized slot scan-and-claim (PG holds
ProcStructLock; our port had dropped it) and a raced `static mut Mode`. Symptom: SIGABRT/
SIGTRAP at process/runtime teardown with NO Rust panic (heap corruption), only under multiple
tests in one binary. Fix: claim the aux PGPROC slot under the free-list lock; make Mode an
atomic. Lesson: any shared fixed-array claim must hold the lock across scan+write; verify
with ThreadSanitizer (`-Zsanitizer=thread`) when a teardown abort has no panic message.

### Carried-forward TODOs (F3 deferred)
- The catchup/config-reload RECEIVE path is not main-loop-wired; aux ProcessXInterrupts that
  need the consumed ConfigReloadPending flag (autovacuum rebuild_database_list - currently
  rebuilds every tick -> would starve workers once get_database_list returns real rows) need
  the interrupt layer to EXPOSE the reload flag (TODO(catalog) in autovacuum.rs).
- do_autovacuum / vacuum / catalog scans / bgworker connection-init are stubs until those
  subsystems land; archive copy + pgstat are stubs; StartupXLOG recovery body is xlogrecovery.
- on-demand worker spawn hooks + bgworker entry registry are in-core only (dlopen tombstoned).

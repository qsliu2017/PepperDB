# Translation rules: turning a PostgreSQL `.c` file into a PepperDB Rust file

Audience: an agent translating a PostgreSQL file (`ref/postgres`, pinned at `REL_18_4`)
into the PepperDB port. The foundation (the single-process async spine) is complete; its
primitives exist - use them, do not reinvent them. This is a reference, not a changelog.

---

## 1. The model and the per-file disposition

PostgreSQL is multi-process with a shared-memory segment and `longjmp` errors. PepperDB is
one process on the tokio multi-thread runtime: every backend and auxiliary process is an
async task. Translation re-expresses C intent in that model - three kinds of state in
particular:
- **Ex-shared state** (shmem, DSM, ...) lives on the heap behind `Arc`, each structure
  owning its locking; add the field to `src/shared_state.rs` (section 6.2).
- **Ex-process-private state** (a per-backend `.c` `static`) becomes per-task state: a
  `task_local!` (the per-backend `Session`, `src/session.rs`, is the model), or a
  process-wide `OnceLock` / `SharedState` field for a genuine singleton. Do NOT use
  `std::thread_local` - a task migrates OS threads across `.await` (section 6.1).
- **Errors** are panics carrying `ErrorData`, caught at a task boundary (see `error.md`).

Pick a disposition per file: **full** (translate the whole behavior - the default),
**rewrite-to-design** (the mechanism is replaced - the VFD pool over `IoBackend`, parallel
state over `Arc`; translate the intent, delete the C mechanism with a `// deleted by
redesign:` note), or **tombstone** (subsumed by tokio/std/Arc - fork/exec, DSM, the shmem
segment, spinlocks, `MemoryContext`; write a tombstone note, do not translate). A phase's
file-list (`ai/plans/00N-.../file-list/`) records each file's disposition.

---

## 2. Path mapping and scaffolding

`.c` definitions map 1:1 by path (KEEPING the `backend/` segment); headers were already
translated with `include/` STRIPPED:
```
ref/postgres/src/backend/utils/error/elog.c -> src/backend/utils/error/elog.rs
ref/postgres/src/include/utils/elog.h       -> src/utils/elog.rs
```
A Rust-native abstraction with no C counterpart gets a sensible module name (not a
translation): `src/shared_state.rs`, `src/session.rs`, `src/storage/wait_guard.rs`,
`GenSlab` in `src/storage/procnumber.rs`.

Scaffold the tree as you go (`pub mod <child>;` in each `mod.rs`; create only what a step
needs). Validate with `cargo check` (never `cargo build`, except once to confirm a new bin
links). It must stay green, warning-free.

---

## 3. Translating a definition; updating the header

The C header (declaration) was already translated. When translating a source file (`.c`,
the definition): the backend module (`src/backend/<p>.rs`) holds the `.c` bodies (`pub`),
the file-local `static` helpers/state, and the `#[cfg(test)]` tests (private unless the
header declared them). Replace every former header `unimplemented!()` stub with a `pub use`
re-export or a shim, so existing `use crate::<header>::<name>` keeps resolving. Globals
declared in a header but defined in a `.c` follow the same rule.

**Naming / method-vs-function:**
- Type-centric file (a struct + operations: `Latch`, `ConditionVariable`, `ResourceOwner`,
  the VFD `File`): put idiomatic METHODS on the type in the backend module (the inherent
  `impl` may live there even though the `struct` is in the header - same crate; fields
  `pub(crate)`). The header keeps each C-named free fn as a `#[deprecated(note=..)] #[inline]`
  shim delegating to the method (grep-ability; nudges new code to the method).
- Non-type-centric file (global-state fns, e.g. `elog.c`): define a **snake_case** `pub fn`
  in the backend module, put the C symbol in the doc comment (`/// PG `AcceptInvalidationMessages``),
  and rewire the header as `pub use crate::backend::<p>::<snake> as <CName>;`. Do NOT keep
  the C PascalCase name on the definition behind `#[allow(non_snake_case, reason="mirrors C")]`
  - that per-fn allow is redundant (a crate-global `#![allow(non_snake_case)]` exists) and
  banned; snake-case the def, keep the C name in the doc + the header alias.
- Never call your own `#[deprecated]` shims internally (no deprecation warnings in `check`).

---

## 4. The full-file principle

Translate the ENTIRE file's behavior; do not leave a half-translated file. A function that
calls a not-yet-translated subsystem calls its existing `unimplemented!()` stub - that
compiles and is correct staging. This differs from deleted-by-redesign C (OS portability,
fork/exec, Windows, `sync_file_range`), which is removed with a `// deleted by redesign:`
note. Async coloring is the one thing that legitimately revisits a translated file.

---

## 5. Async coloring and the lock-across-await invariant

Async spreads outward from the I/O and wait leaves; everything transitively reaching an
`.await` becomes `async`.
- **Leaves** (where `async` originates): `IoBackend` file I/O, the latch wait, socket I/O,
  WAL fsync, lock/CV waits.
- **Stays synchronous**: flag setters, `SetLatch`/`wake_one`, `ProcessInterrupts`,
  `CHECK_FOR_INTERRUPTS`, and hot critical sections. Only the *wait* side is async.
- **Positional file I/O** (concurrent offset reads/writes on one handle):
  `std::os::unix::fs::FileExt::{read_exact_at, write_all_at}` on `tokio::task::spawn_blocking`
  (why `IoBackend` uses `std::fs::File`). **Sequential/socket I/O** (WAL append, libpq wire):
  `tokio::io::{AsyncReadExt,AsyncWriteExt}`. Don't hand-roll the loops.

**THE hard invariant: never hold a synchronous lock guard (`parking_lot`/`std` `Mutex`/
`RwLock`) across an `.await`.** Take the lock, compute, snapshot/clone what you need, drop
the guard, THEN await. Enforced by clippy `await_holding_lock` = deny. State legitimately
held across `.await` uses `tokio::sync::{Mutex,RwLock}` (document each). A future waiting in
a shared queue must remove itself on `Drop` (cancellation safety - that is `WaitGuard`).

**Arc rule: DEREF, don't CLONE.** `shared.clog()` returns `&Arc<T>`; calling through it
derefs to `&T` at zero cost. `Arc::clone` is a refcount atomic (+ a cache bounce when
contended) - clone ONLY to own the handle past the borrow (hold across `.await`, move into a
task). Never `shared.x().clone()` on a hot path; pass `&T`/`&Arc<T>` and deref.

---

## 6. Translating common constructs

### 6.1 `static` per-process variables - split by who reads/writes
- **Process-wide config** (`DataDir`, sizing GUCs): `ProcessConfig` on `SharedState`
  (`src/backend/utils/init/globals.rs`), set once at startup.
- **Per-backend identity** (`MyProcPid`, `MyDatabaseId`, the user-id stack): the per-task
  `Session` (`src/session.rs`), a `task_local!` `Arc<Session>` (`current`/`try_current`/`scope`).
- **Per-task state ANOTHER task must set** (interrupt/cancel flags): a shared per-task slot
  in a generational slab as atomics, settable cross-task via a registry (`ProcSignal` is the
  model); the owner keeps a `task_local` handle for fast reads.
- **Ex-shared-memory state**: typed `Arc` fields on `SharedState`, each owning its locking.

Per-task state MUST be `Send`: atomics, `Mutex`/`RwLock`, `Arc` - NEVER `Rc`/`Cell`/`RefCell`
in a Send position (tasks migrate threads across `.await`). Per-task state HELD across an
`.await` is a `task_local!` (`RefCell` for non-`Copy`), NEVER `std::thread_local` (a
thread-local splits on migration and corrupts the state). Replace a header `pub static mut X`
with `#[deprecated]` accessors over the Session/ProcessConfig/slot - one source of truth.

### 6.2 IPC (the shared segment and its allocators are gone)
- **Shared-memory structs** -> `Arc` fields on `SharedState`; each owns its locking.
- **DSM / DSA / shm_toc / shm_mq** -> `Arc` + typed tokio channels; tombstone.
- **Latch** (`SetLatch`/`WaitLatch`) -> `tokio::sync::Notify` + a sticky `AtomicBool is_set`:
  `set` stores the flag + `notify_one`; `wait` checks the flag, arms `notified()`, re-checks,
  awaits - a set before the wait is never lost (`src/storage/latch.rs`).
- **ConditionVariable** -> `WaitQueue`/`WaitGuard` (`src/storage/wait_guard.rs`):
  `GenSlab<Waker>` under a `Mutex`; `enqueue` returns a guard whose `Drop` dequeues; the CV
  protocol enqueues up front so a signal racing the predicate check is not lost.
- **WaitEventSet** -> `tokio::select!` over `AsyncFd`/`tokio::time`/`Notify`/shutdown.
- **ProcSignal** (backend->backend) -> generational-slab registry of per-task slots (atomic
  reason/interrupt flags + `Arc<Latch>` + constant-time-compared cancel key). `SendProcSignal`
  sets the flag (Release) + rings the latch; the owner reads (Acquire) at
  `CHECK_FOR_INTERRUPTS`. (`src/backend/storage/ipc/procsignal.rs`.)
- **PMSignal** (child->postmaster) -> typed channels to the supervisor task.
- **PGSemaphore** -> `tokio::sync::Semaphore`. **`pg_atomic_*`** -> `core::sync::atomic`.
- **`proc_exit`/`on_shmem_exit`/`before_shmem_exit`** -> RAII `Drop` (registry tombstoned).
- **Interrupts/cancellation**: per-task `ProcSignal` slot flags; `CHECK_FOR_INTERRUPTS` calls
  the sync, holdoff-gated `ProcessInterrupts` which reads/clears flags and raises (cancel ->
  `ERROR`, terminate -> `FATAL`). Holdoff/crit-section counters are per-task (Session). A
  statement/lock timeout is a `tokio::time` timer that sets the flag + rings the latch.

### 6.3 Locks - concurrency-primitive mapping
| PostgreSQL | PepperDB |
| --- | --- |
| LWLock / spinlock (sync critical section) | `parking_lot::{Mutex,RwLock}` wrapping the protected data |
| a lock held across `.await` | `tokio::sync::{Mutex,RwLock}` |
| `pg_atomic_*` | `core::sync::atomic` |
| `SetLatch`/`WaitLatch` | `tokio::sync::Notify` + sticky `AtomicBool` |
| `WaitEventSet` | `tokio::select!` |
| `PGSemaphore` | `tokio::sync::Semaphore` |
| ConditionVariable | `WaitQueue`/`WaitGuard` (`GenSlab<Waker>`) |
| shared-memory segment | `Arc` fields on `SharedState`, each owning its lock |
| DSM / DSA / shm_mq / shm_toc | `Arc` + tokio channels |
| ProcSignal / PMSignal | per-task atomic-flag slab + Latch / typed supervisor channels |
| OS signals | `tokio::signal` (shutdown/reload/cancel) |
| `proc_exit`/`on_shmem_exit` | RAII `Drop` |
| `MemoryContext`/`palloc` | Rust ownership + `Drop` |
| fork/exec, postmaster | `tokio::spawn`; supervisor task |
| `longjmp`/`PG_TRY` | panic carrying `ErrorData` + `catch_unwind` (see error.md) |

Sync locks are `parking_lot` (NOT `std`): they do not poison, so an `ERROR`-as-panic raised
while a lock is held cannot poison it and cascade through `.lock()` callers (error.md s2.7).
Wrap the data the lock protects; do not reproduce naked locks. `OnceLock`/`Arc`/atomics stay
`std`.

### 6.4 Memory
`MemoryContext`/`palloc` are tombstoned - use ownership + `Drop`. Keep `work_mem`
*accounting* only where spill decisions need it (sort/hash/agg), not a global current
context. `ResourceOwner` cleanup is typed RAII guards + a phased abort release order
(`BEFORE_LOCKS` -> `LOCKS` -> `AFTER_LOCKS`), each release inside `catch_unwind` so one bad
resource can't abort the abort.

### 6.5 Errors and logging
The error model is normative in `ai/plans/003-total-translation/error.md` - follow it.
Summary: `elog!`/`ereport!` for `elog`/`ereport`; `>= ERROR` raises (`panic_any(ErrorData)`),
`FATAL` ends the backend task, `PANIC` aborts the process (uncatchable), `< ERROR` formats +
returns. Use the `OrElog` trait (`unwrap_or_error/_fatal/_panic[_with]`) instead of bare
`unwrap`/`expect`; `crate::assert!` for `Assert`; `pg_try(..).pg_catch(..).pg_finally(..)`
for `PG_TRY`. Server-log output is a minimal stderr emitter; the structured/destination
machinery (csvlog/jsonlog/syslogger, libpq `send_message_to_frontend`) is deferred to stubs.

---

## 7. Reusable primitives - use these

Foundation:
- `GenSlab<T>`/`Key<T>` (`storage/procnumber.rs`) - generational slab; the replacement for
  any fixed slot index (ProcNumber, child/VFD/proc-signal slot, wait queue). A stale `Key`
  fails lookup, so it auto-dedups "released by owner" vs "released by guard".
- `Latch`, `WaitQueue`/`WaitGuard`, `ConditionVariable` (s6.2).
- `IoBackend` (`storage/io_backend.rs`) + `FdManager`/`File` (`backend/storage/file/fd.rs`) -
  all file I/O.
- `SharedState` (`shared_state.rs`) - the Arc-shared root; add subsystems at the
  `TODO(stepNN)` marker matching ipci.c `CreateOrAttachShmemStructs` order (it encodes init
  dependencies). Fields are private behind `pub(crate)` accessors generated by the
  `shared_state!` macro (which also asserts each field `Send+Sync+'static`).
- `Session` (`session.rs`), `ProcSignal`, `ResourceOwner`, `ErrorData` + `elog!`/`ereport!`.
- `pepperdb_util::PreallocCollect` (`.prealloc_collect()`/`.collect_with_capacity(n)`).

Storage core:
- `Page` (`storage/bufpage.rs`) - `#[repr(C, align(8))]` newtype over `[u8; BLCKSZ]` so the
  `PageHeaderData`/`ItemIdData` overlay casts are sound (prove with `const` size/align asserts).
- `BufId` (`storage/buf.rs`) - `{Invalid, Global(u32), Local(u32)}` (not a sign-encoded int).
- `BufferPool` (`backend/storage/buffer/`) + `bufmgr`/`localbuf` + the FSM - all page access.
- smgr/md (`backend/storage/smgr/`) over `FdManager`; `SharedState.sync_requests` is the
  pending-fsync queue (the checkpointer drains it).
- WAL (`backend/access/transam/`): `XLogCtl` + `xlog_flush`/`XLogInsert`/`log_newpage`,
  `XLogReader<F>`, `Rmgr`/`GetRmgr`; incremental CRC-32C (`port/pg_crc32c.rs`).
- SLRU (`backend/access/transam/slru.rs`) for clog/subtrans; the PGPROC arena + `ProcGlobal`
  (`storage/proc.rs`); `ProcArray`, snapshot manager, the lock manager, sinval, the aux tasks
  + supervisor (`backend/postmaster/`), and the parallel chassis (`access/transam/parallel.rs`).

---

## 8. Proven design patterns (apply across the access/AM/executor tree)

Each is a reusable shape; the named file is the worked example.

- **Shared mutable fixed-size array -> index handle + interior mutability + atomic mirror.**
  A C array of structs shared by all backends and referenced by pointer (PGPROC, buffers)
  becomes a fixed `Vec<UnsafeCell<T>>` (allocated once, never resized/moved), indexed by a
  Send integer handle (ProcNumber/BufId) - NOT a raw `*mut` (which is `!Send`). Justify
  `unsafe impl Send+Sync` by: never moves, and every mutable access is serialized by a
  documented lock. Hot LOCK-FREE-READ scalars (the xid/subxid/statusFlags a snapshot scans)
  live as PARALLEL atomic mirror arrays, written under the one lock the reader respects -
  never read a torn compound struct lock-free. (`storage/proc.rs`, `buffer/buf_init.rs`.)
  A claim into such an array MUST hold the lock across scan+write (scan-then-write races =
  UB); when a teardown abort has no Rust panic message, suspect this and check with
  ThreadSanitizer (`-Zsanitizer=thread`).

- **Async leaf (buffer pool, SLRU, SI ring).** Per-slot IN-PROGRESS flag + a `WaitQueue`:
  exactly one task does the I/O, others await. Select-victim and claim (mark in-progress)
  are ONE critical section (don't drop the lock between them, or two tasks claim the same
  slot). Only the physical I/O awaits, with the lock dropped, under an RAII unwind guard that
  clears the flag + wakes waiters on panic (else waiters hang). Expose access as a CLOSURE
  that holds the lock across find+use; a "return the slot, re-lock by slot" API races
  eviction. Enqueue the `WaitGuard` UNDER the lock (a wake racing an enqueue-after-drop is
  lost). NEVER form `&mut` to a page/slot under a SHARED lock; reads use `&`, writes need the
  exclusive lock or the in-progress gate. (`buffer/bufmgr.rs`, `transam/slru.rs`.)

- **Every blocking wait cleans up on give-up.** A waiter leaves a queue four ways: granted,
  timeout, error, or future-drop (cancel / select! loser / abort). All non-OK exits run ONE
  idempotent locked cleanup primitive (unlink, undo counts, wake now-grantable waiters); wrap
  the `.await` in an RAII guard that runs it on `Drop` unless disarmed on OK. Timeout/deadlock
  select arms signal the outcome; they do not mutate the shared queue lock-free.
  (`storage/lmgr/proc.rs` ProcSleep.)

- **Sharded hash table = `Vec<Mutex<Shard>>`.** Partition = tag-hash % N; the shard Mutex IS
  the partition LWLock (never held across `.await`). Box entries so a holder's raw pointer
  stays valid while resident (GC only when no one references it). A whole-subsystem critical
  section (deadlock check) takes all shards in fixed index order, releases in reverse, no
  `.await` while any is held. (`storage/lmgr/lock.rs`.)

- **Per-task subsystem state = `task_local! RefCell<...>`.** The xact `TransactionState`
  stack, snapmgr's snapshot stacks, the combo-cid map, inval's pending lists, the receive
  buffers - all per-backend state held across `.await`. Never hold the `RefCell` borrow
  across an `.await` or a callback (borrow, copy/decide, drop, then call).

- **A shared owned value is an `Arc`, not a borrow.** `Snapshot = Option<Arc<SnapshotData>>`;
  never lend `&'static mut` out of task-local storage (laundering a task_local borrow to
  `'static` is aliasing UB). Getters return `Arc::clone`; mutate via `Arc::make_mut` (COW).

- **Modular xid order is a METHOD, not `Ord`.** `TransactionId::precedes`/`follows` (modular
  wraparound + permanent-xid case) are non-transitive -> do NOT `impl Ord`/`<` with those
  semantics (breaks sort/min/BTree). A derived numeric `Ord` is fine for map keys but is not
  transaction order (document it). `FullTransactionId` (64-bit) IS a true total order.

- **Auxiliary/background process = long-lived tokio task, one shape.** Cradle:
  `InitAuxiliaryProcess` (claims an aux PGPROC + inits `proc_latch`), register the procsignal
  slot WITH that same `proc_latch` so there is ONE unified wakeup latch (a second sticky
  latch busy-spins). Loop: `proc_latch.reset()`, then `select!{ biased; proc_latch.wait() |
  sleep(timeout) | shutdown.notified() => break }`. Advertise the role's ProcNumber in
  `ProcGlobal.<role>_proc` so others wake it by number. An RAII exit guard (clear advertised
  proc + deregister slot + ProcKill) runs on EVERY exit incl panic. Don't let the loop panic
  on a timer (de-fang the specific timer-reachable stub to a non-panicking no-op). The
  supervisor spawns aux onto a JoinSet with a restart policy (respawn only in the accept
  loop, never during drain) and a sequenced shutdown drain (PostmasterStateMachine order;
  checkpointer first-started, last-stopped). (`backend/postmaster/`.)

- **Cross-process transport -> tombstone, share by Arc.** DSM/shm_mq/shm_toc and the
  parallel-worker state serialization (GUC/snapshot/xact/relmapper into DSM) disappear: a
  worker is a task spawned INSIDE the leader's task-local scopes, inheriting `Arc<Session>` +
  snapshot/xact by scope nesting; messages go over a TYPED tokio mpsc (an enum, not pqmq byte
  framing); keep only genuinely-shared scalars as `Arc<Atomic*>`. Worker errors -> a
  `Message::Error(text)` on the channel; the leader re-raises (no longjmp-to-leader).
  (`access/transam/parallel.rs`.)

- **Durability (only if you touch WAL/clog).** The flushed-LSN (atomic + `tokio::watch`) is
  published only AFTER `issue_xlog_fsync`, monotonically; group commit via the held-exclusive
  WAL insert locks + `WaitXLogInsertionsToFinish` before the write lock; a partial last page
  backs the cursor off so a later same-page flush rewrites it; PG-compatible on-disk format
  with `xl_prev` folded into the CRC. SYNC commit flushes WAL >= the commit LSN BEFORE clog
  is marked committed. (`access/transam/xlog.rs`, `xact.rs`, `clog.rs`.)

---

## 9. Lock preconditions are types, not comments

A function whose contract is "caller holds lock X" (`LWLockHeldByMe`, `callerHasWriteLock`,
the `*_internal`/`*_locked` helpers) must encode that in its signature - never a `// caller
holds X` comment + an implicit assumption. This also removes a deadlock footgun: `parking_lot`
is non-reentrant, so a helper that re-acquires a lock the caller holds HANGS. Two forms:

- **Case A - the helper reads/writes the GUARDED data:** take the dereferenced guard
  (`&mut Inner` / `&Inner`), not `&self`. The only way to call it is to hold the lock, and it
  has no handle to re-lock. Caller: `let mut g = self.inner.write(); Self::do_thing(&mut g, ..)`.
  Compiler-enforced, deadlock-proof, decoupled from the lock type, unit-testable. Prefer this
  whenever the helper dereferences the lock.
- **Case B - the helper touches state CONVENTIONALLY under the lock but not the guarded
  data** (the lock-free mirror atomics, the UnsafeCell arena gated by a partition Mutex):
  take a `&guard` witness as a proof token (may be unused -> `_g`). `fn end_xact_internal(&self,
  _g: &ProcArrayWrite<'_>, ..)`; caller passes `&g`. Weaker (forgeable across instances of the
  same lock type; couples to the lock type) but beats a comment for hot invariant helpers.

Never re-acquire inside the helper a lock the caller holds. `*_locked`/`*_internal` naming may
stay for grep; the SIGNATURE now carries the contract.

---

## 10. Idiomatic Rust style

1. **`let ... else`** for bail-on-None/Err, not `let x = match opt { Some(v)=>v, None=>return };`.
2. **Iterator pipelines** (`filter`/`filter_map`/`map`/`collect`, `?` and `bool::then_some`
   inside the closure) over `let mut v; for { v.push() }` build-loops. Keep an explicit loop
   only for real control flow / side effects.
3. **Preallocate filtered collects** with `pepperdb_util::PreallocCollect` where most items
   are kept or the upper bound is a tight small constant (MaxBackends, NUM_LOCK_PARTITIONS);
   `collect_with_capacity(n)` for `flat_map`/`flatten` (upper hint `None`).
4. **Enums over sentinel-field pairs** that encode a sum type (`enum LockGroupRole { None,
   Leader{members}, Member{leader} }`, preserving the C semantics) - make invalid states
   unrepresentable.

---

## 11. Clippy (enforced; keep it 0/0)

Workspace lint policy in `[workspace.lints.clippy]` (members opt in via `[lints] workspace =
true`); `cargo clippy --all-targets` must be 0 warnings / 0 errors - a new warning is a
regression.
- `pedantic` + `nursery` = `warn` (priority -1); NOT `clippy::restriction`.
- `deny`: `await_holding_lock`/`await_holding_refcell_ref` (s5); the soundness pointer casts
  (`cast_ptr_alignment`, `ptr_as_ptr`, `ref_as_ptr`, `borrow_as_ptr`) + `cast_lossless`;
  `unwrap_used`/`expect_used` (use `OrElog`/`?`/`crate::assert!` - tests are exempt via
  `clippy.toml`).
- `allow` (rationale in Cargo.toml): port-inherent lints (too_many_arguments, module_*,
  unreadable_literal, struct_excessive_bools, trailing_empty_array) and the value-cast family
  (intentional C width arithmetic). EVERY `#[allow]` needs `reason = "..."`. A pre-existing
  unwrap/expect backlog carries a file/site `#[allow(.., reason="TODO(error-migration)")]`;
  migrate per file as subsystems get exercised.
- Clippy is load-bearing, not cosmetic: it has caught `future_not_send` (a `!Send` captured
  across `.await`) and an unaligned-read UB. Treat `future_not_send`/`cast_ptr_alignment`/the
  await-holding denies as bug signals. Run `cargo clippy --fix --all-targets` first, then
  fix/justify the rest.

---

## 12. Validation and workflow

- `cargo check` + `cargo clippy --all-targets` stay green/0; `cargo test --lib` green and
  growing. Inline `#[cfg(test)]`; `#[tokio::test(flavor="multi_thread")]` for async/cross-task
  code. Cover the cancellation/race/dedup/teardown paths, not just the happy path; tests use a
  tempdir, not the repo root.
- Rhythm per file/step: translate -> verify -> independent review -> manual gate. A small step
  is one squashed commit; a large step splits into dependency-ordered sub-commits (each
  reviewed) with one gate after all, then squash. Review agents are READ-ONLY: never
  `git checkout`/`reset`/`stash`/`commit`, never delete cron jobs or edit the task list.

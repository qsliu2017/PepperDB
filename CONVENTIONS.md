# PepperDB Rewrite Conventions

## Invariants

These must stay byte-compatible with PostgreSQL:

- on-disk format
- wire protocol
- catalog layout

`#[repr(C)]` stays on any struct written to disk, stored in the catalog, or crossing
FFI. Assert their layout statically - size and field offsets, plus `Natts` and attribute
order for catalog tuples (a size-only check still passes a rotated-field bug).

The wire protocol is not guarded by `#[repr(C)]`: FE/BE messages are serialized
field-by-field in network byte order, so the (de)serialization routines are the
invariant, not struct memory layout.

## Single source of truth

- Each struct, enum, signature, and constant (`Anum_*`, `*RelationId`, OIDs) has exactly
  one home - the module of its defining `.h`/`.c`. Every other file `use`s it. Never
  locally re-declare a partial/truncated struct or re-inline a constant.
- No forward stubs: cross-module references go through `use`, never a re-declared
  `extern`/`#[no_mangle]` symbol. `#[no_mangle]`/`extern "C"` is reserved for genuine FFI
  and on-disk/catalog exports; its count should trend to zero.

## Breaking changes

- in-memory-only structs: drop `#[repr(C)]`
- functions/statics: drop `#[no_mangle]`/`extern "C"` (kept only per Single source of truth)
- Windows / 32-bit / cross-platform (delegate to std)

### Multiprocess model

Replace the postmaster + `fork()`-per-backend + System V shared memory + Unix
signals/semaphores with ONE process: a multi-threaded tokio runtime, one task per
connection, background workers as tasks, former shared memory as ordinary Rust state.

**Supervisor & tasks**
- Postmaster -> one async supervisor task. Each former child (per-connection backend;
  every aux/bg worker: checkpointer, bgwriter, walwriter, autovacuum, archiver, startup,
  walreceiver, logical/parallel) -> a `tokio::spawn` task in a `JoinSet`.
- Child death = a `JoinHandle` resolving; `JoinError::is_panic()` is the old abnormal
  exit. No `fork`/`waitpid`/`kill`/pids/postmaster-death pipe.
- Accept loop = tokio `TcpListener`/`UnixListener` + `spawn`. The PM state machine is
  owned and mutated only by the supervisor's `select!` loop - no lock.

**Shared state (former shmem)**
- No segment: do not reimplement `ShmemAlloc`/`ShmemInitStruct`/`ShmemInitHash`/
  `PGSharedMemoryCreate`. Former shmem structures are process-lifetime Rust state, built
  once at startup in dependency order BEFORE any task spawns, published per subsystem via
  `OnceLock`/`Arc` (init-once) or `ArcSwap` (reload-swappable). Never lazy first-touch.
- Keep PG lock granularity: one LWLock -> one `Mutex`/`RwLock<T>`; a partitioned hash
  (buffer 128-way, lock 16-way) -> `[Mutex<HashMap>]` indexed by `hashcode % NPARTITIONS`;
  spinlock/`sig_atomic_t`/`pg_atomic` -> `std::sync::atomic` with explicit
  `Acquire`/`Release`. REPLACE hand-rolled `pg_read_barrier`/`pg_write_barrier`/
  `*_ACCESS_ONCE` publish protocols - do not transliterate. `static mut` for shared state
  is forbidden (data race). Delete DSM/DSA pseudo-pointers, `dsa_get_address`, `shm_toc`.
- Prefer parking_lot (non-poisoning) locks for shared state: recovered ERROR panics are
  routine, and std poisoning would turn one task's recovered panic into a lock outage.

**Locking & async**
- Never hold a lock guard, `RefCell`/arena borrow, or buffer pin across `.await`: acquire,
  mutate to a consistent state, drop, THEN await IO/WAL/channel (mirrors PG dropping the
  partition LWLock before page IO). A guard across a suspension point deadlocks the process
  and makes the future `!Send`. Keep snapshot/proc-array/buffer critical sections non-async.
- IPC by primitive: Latch -> `Arc<Notify>`; PGSemaphore -> `Notify` (single-shot) or
  `tokio::sync::Semaphore` (counting); ConditionVariable/Barrier -> `Notify` + a small
  Mutex over the guarded state; `shm_mq` / parallel tuple+error queues -> `mpsc`;
  `WaitEventSet`/epoll -> tokio async IO under `select!`. Register the `Notified` future
  BEFORE testing the predicate; undo queue mutations on cancel via Drop. No spin/busy-wait.
- Keep blocking/CPU-bound work off the reactor: md IO (`FileReadV`/`FileWriteV`/`fsync`),
  sort, hashagg, index build, vacuum, WAL replay, deadlock walk -> `spawn_blocking`/
  dedicated pool/io_uring, or yield via `CHECK_FOR_INTERRUPTS`. Task-locals do NOT cross
  `spawn_blocking` - pass context explicitly.

**Per-connection state**
- Lives in an owned `SessionState`/`BackendCtx` threaded by `&mut` (or `tokio::task_local!`
  for genuinely ambient values). NEVER `static mut`, NEVER `thread_local!`: work-stealing
  migrates a task across OS threads at each `.await`, so a thread_local silently binds to
  another connection (wrong-role ACL, corrupt snapshot/error stacks).
- Holds the former per-backend globals: identity (`MyProc`/`MyProcNumber`/`MyDatabaseId`/
  user-ids), cursors (`CurrentMemoryContext`, `CurrentResourceOwner`, `ActiveSnapshot`),
  xact state, per-session GUC, the error stack, interrupt counters, private buffer
  pins/localbuf, and (default) relcache/catcache/syscache.
- GUC = one `ArcSwap<GucDefaults>` (server-wide, swapped on reload) + per-session overrides
  (value/source/transactional stack); reads consult override then default. Delete the
  `*mut T` variable indirection.
- Caches default to PER-CONNECTION (preserves "my cache matches my snapshot", no hot-path
  lock). A shared cache is a separate decision: it forces MVCC/generation-versioned entries
  checked against the task snapshot, and a single `RwLock` over the whole cache
  self-deadlocks (lookups recurse into catalog reads that drain invalidations).

**Interrupts, cancellation, invalidation**
- No signals for control flow. Query cancel -> cancel a statement-scoped child
  `CancellationToken` + set a per-task atomic; terminate -> connection-root token;
  shutdown -> one global root token every wait `select!`s on. `CHECK_FOR_INTERRUPTS` is an
  async yield point, allowed only with no guard/borrow/pin held and `CritSectionCount == 0`.
- Real OS signals -> one `tokio::signal` listener: SIGTERM/SIGINT -> shutdown token,
  SIGHUP -> config `watch`. Statement timeout -> `tokio::time` in the same `select!` as the
  cancel token (not SIGALRM). The deadlock detector runs synchronously holding all
  partition locks (no await), armed by a `tokio::time` timeout around the lock wait.
- Cross-task wakeups -> typed `mpsc`/`Notify`/atomic pending-reason bits (delete
  `ProcSignal`+SIGUSR1, `pmsignal`, the SI ring). Sinval -> one
  `broadcast<Arc<[SharedInvalidationMessage]>>`; `AcceptInvalidationMessages` is a
  synchronous `try_recv` drain that NEVER awaits; on `Lagged`, reset caches
  (`InvalidateSystemCaches`) and KEEP draining; send only after the commit is visible.
- Query-cancel routing uses a registry keyed by the 32-byte cancel key (constant-time
  compare): all sessions share one OS pid, so the key is the discriminator and the
  client-facing pid is a synthetic per-connection id.

**Buffer pool & IO**
- The pool (page array, `BufferDesc` array, 128-way tag map, strategy) is one `Arc`-shared
  structure. `BM_LOCKED` header spinlock -> a non-blocking `AtomicU32` CAS, never spun
  across a yield; content lock -> a per-buffer `RwLock`, never held across the IO await
  (only `BM_IO_IN_PROGRESS` + a per-buffer `Notify` guard in-flight IO). Bgwriter/cleanup
  wakeups -> `Notify`.
- The in-process IO driver must complete a parked issuer's IO, so a task awaiting another
  task's read cannot deadlock.

**Parallel query**
- Workers are tasks sharing the leader heap. Carry setup as one owned/`Arc` `ParallelSetup`
  (snapshot, GUCs, combocid, relmapper, ...) - delete the `Serialize*`/`Restore*` family
  and the `dsa`/`shm_mq` plumbing. Pass the worker entry as an fn/closure, not a name
  string. Shared parallel structures (hashjoin batch, tuplestore, bitmap, dshash) are
  `Arc<Mutex/RwLock>`/atomics, `Send + Sync`. (Open: keep an explicit lock group for
  leader+workers, or fold into one locking identity.)

**Memory, limits, crash isolation**
- Retire MemoryContext toward ownership/RAII; Drop on unwind subsumes bulk-free-on-error.
  Keep contexts only as typed arenas (per-tuple expr eval, per-query planner graph) with
  `reset(&mut self)`/Drop; everything else is Box/Vec/String. The `catch_unwind` boundary
  sits OUTSIDE the arena scope.
- Per-connection memory accounting via one `#[global_allocator]` attributing live bytes to
  a task-local counter + a server-wide `AtomicUsize`. Enforce the per-connection cap in the
  CALLER path, NOT by returning null: a null `GlobalAlloc::alloc` makes infallible
  `Box`/`Vec`/`format!` call `handle_alloc_error` and ABORT the process (and unwinding out
  of an allocator is UB). Cross the cap by raising a typed ERROR at a checkpoint (or
  `try_reserve` on large paths) -> per-command `catch_unwind` -> Drop reclaims; keep a
  scratch reserve so the OOM can format and unwind.
- Isolation = Rust memory safety + one `catch_unwind` per command. ERROR -> caught, abort
  xact, continue; FATAL -> caught, clean up session, the TASK returns (never
  `process::exit`); PANIC -> uncaught -> `std::process::abort` (optionally re-exec'd by a
  thin outer supervisor; WAL recovery unchanged). The catch boundary must enclose only
  pure-Rust frames (unwinding through `extern "C"` is UB - use `extern "C-unwind"` or keep
  FFI off the path); panic payload is `Send + 'static`. Delete
  `sigsetjmp`/`siglongjmp`/`PG_exception_stack`; `pg_re_throw` -> `resume_unwind`.
- Never re-create shared state to "reset" it while sibling tasks hold references
  (use-after-free). A span that transiently breaks a cross-task invariant runs in a
  critical section (an error there promotes to PANIC). Every pin, lock, IO interlock,
  queued waiter, and permit is released by RAII Drop so a cancelled/panicking task cannot
  wedge shared state; replace `on_proc_exit`/`*_shmem_exit` with Drop + one ordered async
  shutdown.

### Extensions

We will not follow Postgres' extension system, using `dlopen` and C-ABI shared libraries.
Compatibility with existing PG extensions is non-goal.

But built-in extensions are translated to Rust and compiled in.

## Naming
- `snake_case`: fns/locals/modules
- `UpperCamelCase`: types/traits/variants
- `SCREAMING_SNAKE_CASE`: consts
- Verbatim C names only at FFI/catalog boundaries
- Map Enum value to qualified Enum tags.
  For example `PROCSIG_CATCHUP_INTERRUPT` -> `ProcSignalReason::CatchupInterrupt`.

### Cross-reference to C

Renaming costs grep-ability against the C oracle. Preserve it: each
module carries a `// C: src/backend/commands/tablecmds.c` header, and every renamed item
ends its doc comment with the original C name and file:

```rust
/// <other doc>
///
/// DefineRelation (commands/tablecmds.h)
pub fn define_relation(...)
```

Keep the substantive C comments (the *why* / the algorithm); drop the mechanical ones.

## Error model

`ereport`/`elog` map by level, not one-to-one:
- `ERROR`/`FATAL`/`PANIC` -> typed `panic!`, caught by one `catch_unwind` per command.
- `WARNING`/`NOTICE`/`LOG`/`INFO`/`DEBUG*` -> a logging call; never unwinds.
- `PG_TRY`/`PG_CATCH`/`PG_RE_THROW` -> `catch_unwind` or an RAII `Drop` guard.

## Functions, Macros
- Macros:
  - object-like constant macro (`#define FOO 1`) -> `const FOO: T`;
  - function-like macro -> `#[inline] fn`, `const fn` whenever the body allows;
  - `macro_rules!` only for variadic, token-tree, lvalue-producing, or type-taking cases.
- Visibility:
  - Only item declared in the `.h` -> `pub`;
  - item only in the `.c` (C `static`) -> private.
- Pointers: prefer `&`/`&mut` over `*mut`/`*const` (strong default).
  - nullable -> `Option<&T>` / `Option<NonNull<T>>`;
  - owning -> `Box`/`Vec`;
  - keep `*mut` only for intrusive, aliased, shared-memory, or self-referential cases;
  - collapse `<Struct>Data` + `<Struct>` into one `Struct`;
- `goto` -> labeled `break`/`loop`, early return, or `?`.

## Types

- text `char*` -> `&str`/`String`; NUL-terminated C interface -> `&CStr`/`CString`;
  `NameData` -> a `[u8; NAMEDATALEN]` newtype, never conflated with a cstring.
- C integer typedefs -> fixed-width: `int32`->`i32`, `Size`->`usize`,
  `Index`->`usize`/`u32`; `Oid` stays a `u32` alias/newtype.
- Prefer `try_into`/`From`; justify every truncating `as` with a comment.

## Data structure, Collections

Use Rust Collections if possible.

- `List`->`Vec`
- `StringInfo`->`String` (text) / `Bytes` or `Vec<u8>` (binary)
- local `HTAB`/`simplehash`->`HashMap`
- `binaryheap`->`BinaryHeap`
- `rbtree`->`BTreeMap`
- `Datum*`+`bool*`->slices
- `palloc`+`repalloc` growable->`Vec`.
- `ilist`->`Vec`/`VecDeque` (intrusive O(1)-unlink stays)
- `pairingheap`->`BinaryHeap` without decrease-key.

Keep `Bitmapset`, `integerset`, `radixtree` for now.

## `unsafe`

A 1:1 port starts deep in `unsafe`; the direction is to shed it, not to document it in
place. Newly written `unsafe` carries a `// SAFETY:` line.

## `#include` -> imports

`#include "h"` brings names into *this* file's scope - a plain `use` (file-local), not
`pub use` (which re-exports and fans names outward). Map it:
- Universal headers (`postgres.h`/`c.h`) -> `use crate::prelude::*`. Keep this; do not re-glob them.
- Everything else -> *explicit* `use crate::path::module::{A, B, C}` of the symbols the
  file uses.
- Avoid glob `use ...::*`, and never `pub use ...::*`, to model an include.

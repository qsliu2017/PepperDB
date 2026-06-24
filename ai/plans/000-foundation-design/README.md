# Plan 000 - Foundation system design

PostgreSQL is multi-process, with a shared-memory segment and longjmp-based error
handling. We replace that with a single-process, asynchronous Rust design on the
tokio runtime. This document is the design.

## Compatibility invariants

The port preserves everything externally observable and persisted:

1. **On-disk and wire format.** An existing PostgreSQL datadir can be opened, and
   an existing PostgreSQL client can talk to us. This forces page/tuple layout,
   WAL format, the libpq v3 wire protocol, and the bit-exactness of anything that
   lands on disk or the wire (hash functions for hash indexes/partitioning,
   `pg_crc32c` for WAL, collation order, float text output). Wire format is
   preserved by explicit big-endian serialization, not by struct layout.
2. **Catalog layout and pgsql-aware behavior.** System catalog schema and OIDs,
   and SQL semantics, match upstream.

## Non-goals

1. **No existing extensions.** We do not support third-party PostgreSQL extensions
   and will not use `dlopen`-style dynamic loading. A different extension mechanism
   (e.g. trait-based registration) is designed later; the dynamic-loader machinery
   (`fmgr` dynamic load, `dfmgr`, `_PG_init`) is dropped, and core hooks become
   traits.
2. **No multiprocess model.** This design is single-process async (see Model).

## Model

- Single process. Each backend is a spawned async task. The postmaster is a
  supervisor task. The auxiliary processes (checkpointer, background writer, WAL
  writer, autovacuum, archiver, startup/recovery) are long-lived tasks.
- Shared memory is gone. State PostgreSQL kept in the shared segment lives on the
  heap and is shared with `Arc`; each shared structure owns its own locking.
  Dynamic shared memory and the DSA allocator are not needed for cross-process
  sharing; in-process state is shared by `Arc` reference rather than serialized.
  Structures that grew inside DSA (parallel hash tables, shared tuplestores) keep
  their own concurrent locking on the heap.

## I/O

Delegate file and network I/O to tokio. Translate PostgreSQL's I/O routines to
their tokio equivalents:

- file reads/writes (the `smgr`/`md` layer, `pg_pread`/`pg_pwrite`) -> tokio async
  file read/write
- `pg_fsync` / `issue_xlog_fsync` -> tokio async fsync
- frontend socket I/O -> tokio async sockets

## Concurrency primitives

Delegate to the Rust standard library and tokio:

- LWLocks and spinlocks -> Rust locks (`Mutex` / `RwLock`)
- `pg_atomic_*` -> `core::sync::atomic`
- latch (`SetLatch` / `WaitLatch`) -> `tokio::sync::Notify` (with a stored-permit
  pattern so a set before wait is not lost)
- `WaitEventSet` -> `tokio::select!`
- `PGSemaphore` -> `tokio::sync::Semaphore`

## IPC / signaling

- shared-cache invalidation queue (sinval) -> a shared queue with task wakeups
- `ProcSignal` multiplexing -> typed channels between tasks
- OS signals (terminate / reload / cancel) -> tokio signal handling driving
  shutdown, configuration reload, or cancellation

## Memory management

Drop the `MemoryContext` mechanism; Rust ownership handles object lifetime and
cleanup. Keep memory *accounting* at the operators that need it (sorts, hashes,
aggregates) so `work_mem` spill decisions are preserved. There is no global
"current context"; allocation state is per task.

## Error model

The end goal is idiomatic `Result` + `?`.

We do not rewrite every signature to return `Result` up front. We keep
`elog(ERROR)` semantics as a panic and contain it with `catch_unwind` at the task
boundary; cleanup on the error path relies on RAII `Drop` releasing locks, pins,
and buffers during unwind. Errors that previously resumed the backend rather than
terminating it need an explicit recovery boundary, not just the task-level catch.
Mark each such path with `#[deprecated]` and a `// TODO(panic)` so it can later
migrate to `Result`. Lower-severity reports (`WARNING` / `LOG` / ...) just log and
return normally.

## Cancellation & interrupts

Per-task interrupt flags; `CHECK_FOR_INTERRUPTS` checks them and services cancel /
terminate / timeout. Statement and lock timeouts are tokio timers that set the
flag. A client cancel request sets the target task's flag.

## Global / session state

PostgreSQL's per-backend process globals become per-task / session state; nothing
relies on process-global variables.

## Process lifecycle

A supervisor task runs the accept loop and spawns a backend task per connection,
plus the auxiliary tasks. Shutdown is driven by cancellation with a graceful
drain.

## What gets deleted

`fork`/`exec` and postmaster process management; the shared-memory segment setup;
most of the OS-portability layer (`src/port`), including the atomics and semaphore
shims; dynamic shared memory and DSA; and the `longjmp` / `PG_TRY` / `PG_CATCH`
machinery.

## Coding invariants

- Never hold a synchronous lock guard across an `.await`. Hot critical sections
  stay synchronous; only the wait awaits, after the guard is dropped.
- A future that waits in a shared queue must remove itself from that queue on
  `Drop`, because cancellation can drop a task at any await.
- Per-task identity that was a fixed slot index uses a generational key to avoid
  reuse hazards.

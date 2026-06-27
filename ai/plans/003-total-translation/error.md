# Error model (RFC -- final decision)

The single-process async port keeps PostgreSQL's error semantics exactly and realizes
them with Rust unwinding. This document is normative: translation and implementation
must follow it.

---

## 1. Invariants

### 1.1 Severity semantics (match PG's design)

| Severity | Meaning | Scope of effect | On raise |
| --- | --- | --- | --- |
| `DEBUG5..DEBUG1`, `INFO`, `NOTICE`, `LOG`, `WARNING` | informational | none | format + emit (to log and/or client), then **return**. Never unwinds. |
| `ERROR` | the current operation cannot proceed | **this backend's current (sub)transaction / command** | unwind to the nearest recovery point, release all resources acquired since it, report to the client, and **continue the session** (read the next command). |
| `FATAL` | this backend cannot continue | **this backend only** | unwind to the top of the backend task, release this backend's shared resources, report to the client, and **end the backend**. Other backends are unaffected. |
| `PANIC` | shared state may be corrupt | **the whole instance** | crash every backend immediately to protect on-disk data; recovery runs on restart. |

Resource-release guarantee on any unwind (`ERROR`/`FATAL`): heavyweight locks, buffer
pins, LWLock-equivalents, the SLRU/buffer in-progress guards, the lock-wait queue entry,
open files, and the backend's `PGPROC` are released in the correct order. `ERROR` returns
the backend to a clean state; `FATAL` additionally returns the `PGPROC` and deregisters
from the supervisor. `PANIC` does **not** unwind (no cleanup) -- it stops the process so a
half-updated shared structure is never observed or flushed.

`ERROR` "unwinds to the nearest recovery point": the recovery point is whichever is
innermost -- an active subtransaction / `pg_try` block, otherwise the per-command loop. A
subtransaction `ERROR` rolls back to that subtransaction and may be caught; an `ERROR`
with no enclosing subtransaction aborts the whole transaction and returns to the command
loop.

### 1.2 Severity selection (match PG's behavior)

Choose the severity PostgreSQL chooses for the same condition. Anything caused by the
client or by recoverable runtime state is `ERROR`; anything that means this connection is
unusable is `FATAL`; anything that means shared/persistent state may be inconsistent is
`PANIC`.

| Condition | Severity |
| --- | --- |
| Invalid SQL, type/parse error, constraint violation, division by zero, bad function argument, permission denied | `ERROR` |
| Lock timeout, deadlock victim, serialization failure, statement cancelled | `ERROR` |
| Out of memory for a query allocation | `ERROR` |
| Authentication failure, startup against a nonexistent database, protocol violation, admin terminate-backend, idle-session timeout | `FATAL` |
| Corrupted page / tuple / WAL record, failed internal consistency check (`Assert`), unexpected state a backend cannot safely recover from | `PANIC` |
| `fsync`/WAL write failure, failure inside a critical section, control-file corruption | `PANIC` |

When uncertain, match the elevel PostgreSQL uses at the same call site.

---

## 2. Implementation

The semantics above are realized in Rust without `setjmp`/`longjmp` or OS signals for
control flow.

### 2.1 Raise

`elog!`/`ereport!` build one structured `ErrorData` value (sqlstate, severity, message,
detail, hint, context, location).

- `elevel < ERROR`: format, emit, return. No unwind, no allocation of control state.
- `elevel >= ERROR`: `std::panic::panic_any(ErrorData)`. The payload is the value, not a
  string, so a catch can recover the full structured error.
- `elevel == PANIC`: do **not** panic; `std::process::abort()` after emitting the message.
  `PANIC` is never catchable (see 2.5).

### 2.2 Catch boundaries

There are exactly three, innermost first:

1. **Subtransaction / `pg_try`** -- a nested recovery point for code that must catch and
   handle an `ERROR` (e.g. `BEGIN ... EXCEPTION`, `DO`-block cleanup). Implemented by
   `pg_try` (3.4). This is PG's `PG_TRY`/`PG_CATCH`.
2. **Per-command loop** (`PostgresMain`) -- the default `ERROR` recovery point. Wraps the
   processing of each client command in `catch_unwind`; on a caught `ErrorData` it runs
   `AbortCurrentTransaction`, reports the error to the client, and loops to read the next
   command. This is PG's top-level `sigsetjmp`. Without it an `ERROR` would escape to the
   task boundary and wrongly end the backend.
3. **Task boundary** (`admit_and_spawn` / the aux cradles) -- the backstop. Catches
   `FATAL` (and any escaped `ERROR` or non-`ErrorData` bug-panic), reports, runs final
   cleanup, ends the task, and lets the supervisor reap it.

`catch_unwind` downcasts the panic payload to `ErrorData`. A payload that is not
`ErrorData` is an internal bug-panic; treat it as `FATAL` at the task boundary (it never
reaches the per-command catch as a recoverable error).

### 2.3 ERROR is backend-local

`ERROR` recovery happens **inside the backend** at boundary (1) or (2). The supervisor is
not involved -- it neither observes nor handles an `ERROR`. This mirrors PG, where
`elog(ERROR)` `longjmp`s to the backend's own handler.

### 2.4 FATAL is handled inside the task

`FATAL` unwinds past the per-command catch to the task boundary (3), which runs the
backend's shutdown: report to the client, release shared resources, `ProcKill` (return the
`PGPROC`), drop the connection, and end the task. The supervisor only *reaps* the finished
task (deregisters its child slot). The postmaster does **not** abort the backend; the
backend ends itself. (`tokio::task::abort()` is never used to terminate a backend -- it
does not run `Drop`, so it would leak locks/pins/buffers/`PGPROC` and hang waiters. All
termination is cooperative unwinding.)

### 2.5 PANIC crashes the instance

`PANIC` calls `std::process::abort()` directly; it is never wrapped in `panic_any` and
never caught. Aborting (not unwinding) is deliberate: running `Drop`/cleanup during a
corrupt-state crash could flush inconsistent data. The supervisor process dies with it;
restart triggers recovery.

### 2.6 Resource release is RAII

All cleanup on `ERROR`/`FATAL` is `Drop`, not explicit catch code: lock guards, buffer
pins, the `ResourceOwner` phased release (`BEFORE_LOCKS` -> `LOCKS` -> `AFTER_LOCKS`), the
lock-wait `WaitGuard`, the SLRU/buffer in-progress guards, and `PGPROC` reclamation all
release as the stack unwinds. This is why control flow must **unwind** (not abort, not
`tokio` task-abort) for `ERROR`/`FATAL`. Each `ResourceOwner` release phase runs under its
own `catch_unwind` so one failing release cannot abort the rest of the abort.

### 2.7 Locks

LWLocks and spinlocks map to **`parking_lot::{Mutex, RwLock}`** for synchronous
critical sections. `parking_lot` is chosen over `std::sync` because it **does not poison**:
under the panic-based `ERROR` model a `std` lock held when an `ERROR` is raised would be
poisoned, and the prevailing `.lock().unwrap()` idiom would then turn the next acquisition
-- in any backend sharing that lock -- into a panic, cascading. PG locks do not poison;
`parking_lot` matches that and removes the cascade hazard.

- Synchronous, short, never-held-across-`.await` critical sections (the LWLock/spinlock
  translations): `parking_lot::Mutex` / `parking_lot::RwLock`.
- State legitimately held across `.await`: `tokio::sync::{Mutex, RwLock}` (async-aware).
  The `await_holding_lock` lint stays `deny`; a sync lock across `.await` is a bug.
- One-shot publication and atomics: `std::sync::OnceLock`, `core::sync::atomic`.

The tokio `parking_lot` cargo feature is unrelated: it only swaps tokio's *internal* locks
and does not affect our data locks. Using `parking_lot::Mutex` requires the `parking_lot`
crate as a direct dependency.

No-poison soundness depends on the **critical-section discipline**: shared state that an
`ERROR` could leave inconsistent must be mutated inside a critical section, and a failure
inside a critical section escalates to `PANIC` (1.2). This is PG's contract
(`START_CRIT_SECTION`), and it is what makes "release the lock and continue" safe without a
poison flag.

### 2.8 elog internals

- One in-flight `ErrorData` is built per `ereport!` invocation as a value; there is no
  multi-level `errordata[]` recursion stack. The macro builds the value atomically and then
  raises, so there is no open span for a nested `ereport` to interleave into. An error
  raised while building an error (e.g. inside a context callback) is a double fault and
  escalates to `PANIC`.
- The `error_context_stack` (the `errcontext` callback chain that produces `CONTEXT:`
  lines) is per-task state held in a `task_local!`, registered/popped by RAII guards and
  walked at raise time to capture context into the `ErrorData`. It enriches messages only;
  it is independent of `catch_unwind` and may be implemented incrementally.

### 2.9 Cancellation and termination

Query-cancel (`SIGINT`) and terminate (`SIGTERM`) are not task aborts. They set the per-task
`ProcSignal` slot flag and ring the slot `Latch`; the target observes it at the next
`CHECK_FOR_INTERRUPTS` (a synchronous, holdoff-gated poll) and raises `ERROR`
(`ERRCODE_QUERY_CANCELED`) or `FATAL` (terminate) through the normal unwind path. A
statement/lock timeout is a `tokio::time` timer that sets the same flag. Blocking waits
(`select!`) include the latch arm so a cancel wakes the waiter, which then raises through
the unwind path -- never by dropping the task from outside.

---

## 3. Translation reference

How to translate PostgreSQL error constructs. Use these mechanically.

### 3.1 `elog` / `ereport`

```c
elog(ERROR, "cache lookup failed for relation %u", relid);
```
```rust
elog!(ERROR, "cache lookup failed for relation {relid}");
```

`elog!` takes a Rust format string (`{var}`, `{var:format}`) -- collapse C `printf`
varargs into it. Severity is the first argument and uses the same names
(`DEBUG1..DEBUG5`, `INFO`, `NOTICE`, `LOG`, `WARNING`, `ERROR`, `FATAL`, `PANIC`).

```c
ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_TABLE),
         errmsg("relation \"%s\" does not exist", name),
         errdetail("..."), errhint("...")));
```
```rust
ereport!(ERROR,
    errcode(ERRCODE_UNDEFINED_TABLE),
    errmsg("relation \"{name}\" does not exist"),
    errdetail("..."),
    errhint("..."),
);
```

The clauses mirror PG's parenthesized list. `errcode(<sqlstate>)` takes the code constant;
the message-bearing clauses (`errmsg` / `errdetail` / `errhint` / `errcontext`) take a Rust
format string with inline args (`{name}`, `{x:?}`). Clause order is free; `errmsg` is
required for `>= ERROR`. Severity `< ERROR` formats and returns; `>= ERROR` raises
(unwinds); `PANIC` aborts.

### 3.2 `Assert` and internal invariants

```c
Assert(ptr != NULL);
```
```rust
crate::assert!(!ptr.is_null());
```

`crate::assert!` is PG's `Assert`: checked in debug builds only, and on failure it
**aborts** the process (it is `PANIC`, never a catchable panic). Do not use `std::assert!`
/ `std::debug_assert!` for invariants -- a `std` assert is a catchable unwind, which would
let `catch_unwind` swallow a corruption signal. For an invariant that must also hold in
release builds, raise `ereport!(PANIC, ...)` explicitly.

A condition that is recoverable ("shouldn't happen, but the session can continue") is not
an `Assert`; translate it to `ereport!(ERROR, ...)`.

### 3.3 `unwrap` / `expect` / `panic!`

Bare `unwrap` / `expect` / `panic!` are forbidden in non-test code (enforced by clippy
`unwrap_used` / `expect_used` / `panic`). The sanctioned replacement is the `OrElog`
extension trait, implemented for `Option<T>` and `Result<T, E>`, which raises the named
severity (capturing the `Err`, when present, as `errdetail`) instead of a bare panic:

```rust
let rel = lookup(oid).unwrap_or_error_with(|| format!("cache lookup failed for relation {oid}"));
let buf = pin(blk).unwrap_or_fatal("could not pin buffer");
```

Methods (the `_with` variants take a lazy `FnOnce -> impl Into<String>`, so the happy path
allocates nothing):
- `unwrap_or_error(self) -> T` / `unwrap_or_error_with(self, f) -> T`
- `unwrap_or_fatal(self) -> T` / `unwrap_or_fatal_with(self, f) -> T`
- `unwrap_or_panic(self) -> T` / `unwrap_or_panic_with(self, f) -> T` (corruption -> abort)

Translate by intent:
- Invariant / "can't happen": `crate::assert!` or `ereport!(PANIC, ...)`.
- Recoverable / boundary condition: `.unwrap_or_error_with(...)`, `ereport!(ERROR, ...)`, or
  `Result` + `?`.
- Connection-fatal: `.unwrap_or_fatal_with(...)`.
- Not-yet-translated subsystem: `unimplemented!("<subsystem> deferred")` (intentional
  reach-panic staging; unchanged by this model).
- Test code: `unwrap` / `expect` are fine.

### 3.4 `PG_TRY` / `PG_CATCH` / `PG_FINALLY`

`pg_try` is a builder; the two PG forms map directly.

PG_TRY / PG_CATCH:
```rust
pg_try(|| { /* try */ })
    .pg_catch(|error| { /* inspect `error: ErrorData`; re_throw_error(error) to propagate */ });
```
PG_TRY / PG_FINALLY:
```rust
pg_try(|| { /* try */ })
    .pg_finally(|| { /* finally, always runs */ });
```
Both, when needed: `pg_try(try).pg_catch(catch).pg_finally(finally)`.

Semantics: `pg_try` runs the try closure under `catch_unwind`. `.pg_catch(f)` runs
`f(error)` only if the try raised an `ErrorData` -- `f` handles it, or calls
`re_throw_error(error)` to propagate. `.pg_finally(g)` runs `g` on both the normal and error
paths, then returns the value or re-raises an unhandled error. The chain yields the try's
`T`. Non-`ErrorData` payloads and `PANIC` are never caught -- they propagate (`pg_finally`
still runs for an `ErrorData` unwind, never for a `PANIC`/abort).

Prefer RAII (`Drop`) over `pg_try` for resource cleanup; reserve `pg_try` for genuine
catch-and-handle (subtransaction error recovery, `EXCEPTION` blocks).

### 3.5 Severity quick map

`elog(ERROR/FATAL/PANIC, ...)` -> `elog!(ERROR/FATAL/PANIC, ...)` (same name).
`elog(WARNING/NOTICE/LOG/INFO/DEBUGx, ...)` -> `elog!(...)` (formats + returns).
`CHECK_FOR_INTERRUPTS()` -> `check_for_interrupts()` (may raise `ERROR`/`FATAL`).
`START_CRIT_SECTION()` / `END_CRIT_SECTION()` -> the critical-section guard; a failure
inside escalates to `PANIC`.

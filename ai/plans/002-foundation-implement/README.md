# Plan 002 - Foundation system implementation

Implement the foundation subsystems, replacing the `unimplemented!()` stubs in the
foundation modules with real, idiomatic async Rust. **Async coloring happens
here**, driven by the actual `.await` points each implementation introduces.

## Preconditions

The foundation module types and signatures (`BufferDesc`, `Buffer`, `Relation`,
...) already exist as synchronous stubs; this stage fills in their bodies. The
foundational primitives (F0 below) may start as soon as their types exist.

## Invariants

- Never hold a synchronous lock guard across an `.await`; hot critical sections
  stay synchronous, and only the wait awaits, after the guard drops.
- A future waiting in a shared queue removes itself from the queue on `Drop`.
- Per-task identity that was a fixed slot index uses a generational key.
- A panic must not leave shared `Arc` state half-mutated: a critical-section guard
  aborts the process on unwind, bypassing the per-task `catch_unwind`.

## Async-ification

The stub bodies are fully synchronous. Async coloring is applied here as a pass that
spreads outward from the I/O leaves (buffer reads, WAL flush waits, lock waits,
network) as those leaves get their real async implementations. A function becomes
`async fn` once it transitively reaches an `.await`; the change propagates up its
callers. This is interleaved with the phases below, not done up front, because the
await points are only known once each subsystem is implemented.

## Phasing

Build bottom-up; each phase assumes the ones above it.

### Phase F0 - foundational primitives

The shared building blocks the invariants depend on. Small, and getting them right
makes the subsystems mechanical.

- **Runtime skeleton**: the tokio runtime, a supervisor task, and an accept loop
  spawning a `catch_unwind`-wrapped backend task per connection.
- **`SharedState`**: the typed `Arc`-field container replacing the shared segment.
- **Lock conventions**: synchronous locks for hot sections, with the no-`.await`
  rule enforced by API shape.
- **`WaitGuard`**: a wait-future wrapper whose `Drop` runs the dequeue/teardown.
- **Error model**: keep `elog(ERROR)` as a panic with one `catch_unwind` at task
  spawn; mark with `#[deprecated]` and `// TODO(panic)`.
- **Interrupts/cancellation**: per-task interrupt flags, `check_for_interrupts`,
  statement/lock-timeout timers, the client-cancel path, and shutdown cancellation.
- **Identity**: the generational-slab key for per-task identity.
- **`IoBackend`**: async file I/O and fsync, delegated to tokio.
- **Session/global state**: per-task state replacing process globals.

### Phase F1 - storage core

- `smgr` over `IoBackend`; then the **buffer manager** (packed atomic state, a pin
  guard, per-buffer IO-wait via `WaitGuard`).
- **WAL/xlog**: insert path, the flushed-LSN watch, group commit, async fsync.
  Publish the flushed-LSN only after fsync completes, and monotonically. Largely
  parallel to the buffer manager.

### Phase F2 - concurrency control

- **Lock manager**: sharded shared lock tables, the per-task proc struct in the
  generational slab, waits via a oneshot inside a `select!` with the
  deadlock/lock-timeout timers, and the deadlock detector.
- **Invalidation/sinval**: a shared ring with per-task wakeups - with backpressure
  (not a plain broadcast channel) so an overflow/reset backstop (a full ring forces
  a global cache reset) is preserved - ties into the catalog caches.

### Phase F3 - aux tasks & supervision

- Expand the supervisor: checkpointer, background writer, WAL writer, autovacuum,
  and archiver as long-lived tasks; the sequenced shutdown drain.

### Phase F4 - parallel query

- Worker tasks share the plan and runtime state via `Arc` and exchange tuples over
  channels, with error/cancel propagation through join handles and shutdown
  cancellation. No dynamic-shared-memory segment or tuple serialization is needed.

## Open items

- **CPU-loop yield policy** (undecided): whether CPU-bound operators yield at
  `check_for_interrupts` or run on a blocking pool.
- **Deadlock detector**: producing a consistent cross-task waits-for graph snapshot
  and aborting a victim through the wait path.

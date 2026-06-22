# PepperDB Round-2 - main-agent workflow

A 1:1 Rust translation of PostgreSQL 18 exists (1,327 files, round-1), full of stubs
(`TODO(pg-port)`, `unimplemented!()`). Round 2 reviews and rewrites every file to apply
`CONVENTIONS.md` - replacing each stub with a real body or a canonical `use`, and making
the code idiomatic - WITHOUT breaking the build.

This file is the MAIN agent's playbook: enumerate files, order them by dependency, dispatch
one subagent per batch, loop until the queue empties. Each subagent follows `TASKS.md`.

## Roles
- **Main agent (this file):** owns the queue and the loop - ordering, batching, dispatch,
  the build/test gate, progress, commits. Does NOT rewrite files itself.
- **Subagent (`TASKS.md`):** rewrites ONE batch and leaves the build green. One agent
  per batch.
- **`CONVENTIONS.md`:** the rules every rewrite follows. Both roles read it first.

## Queue: every file, dependency-ordered (leaf first)

"Depends less first" = foundational subsystems before their dependents, so a file's
dependencies are already conventional by the time it is reviewed. The core
(`nodes`/`utils`/`access`/`catalog`/`storage`) is one big include cycle, so the order is at
SUBDIRECTORY granularity, not top-level module. Process the layers in order; within a
layer, batches are independent and may run in parallel.

- **L0 primitives/platform** - `src/*.rs` (c, postgres, prelude, ...), `pch`,
  `portability`, `port`, `common`, `lib`
- **L1 memory/error/encoding/storage headers** - `utils/{mmgr,error,hash,resowner,fmgr,init}`,
  `utils/*.rs`, `storage/*.rs`, `mb`, `utils/mb`
- **L2 data types & text** - `utils/{adt,sort,time,misc,activity}`, `regex`, `snowball`,
  `tsearch`, `access/common`
- **L3 storage engine & xlog** - `storage/{page,file,smgr,sync,freespace,buffer,lmgr,ipc,aio,large_object}`,
  `access/transam`
- **L4 nodes/catalog/caches/AMs** - `nodes`, `catalog`, `utils/cache`,
  `access/{table,index,heap,nbtree,hash,gin,gist,brin,spgist,sequence,tablesample,rmgrdesc}`,
  `access/*.rs`, `bootstrap`
- **L5 parse & plan** - `parser`, `rewrite`, `optimizer`, `partitioning`, `statistics`,
  `foreign`, `jit`
- **L6 execute & commands** - `executor`, `commands`, `tcop`
- **L7 server & top** - `libpq`, `postmaster`, `replication`, `backup`, `archive`,
  `fe_utils`, `main`

Materialize the concrete ordered queue (authoritative "list of all files") with the layer
globs below; it prints pending files layer by layer, in order:

```sh
# Pending files in dependency order - run top to bottom; batch each layer's output.
grep -rl 'TODO(pg-port)' src/*.rs src/pch src/portability src/port src/common src/lib 2>/dev/null | sort -u   # L0
grep -rl 'TODO(pg-port)' src/utils/mmgr src/utils/error src/utils/hash src/utils/resowner src/utils/fmgr src/utils/init src/utils/*.rs src/storage/*.rs src/mb src/utils/mb 2>/dev/null | sort -u   # L1
grep -rl 'TODO(pg-port)' src/utils/adt src/utils/sort src/utils/time src/utils/misc src/utils/activity src/regex src/snowball src/tsearch src/access/common 2>/dev/null | sort -u   # L2
grep -rl 'TODO(pg-port)' src/storage/page src/storage/file src/storage/smgr src/storage/sync src/storage/freespace src/storage/buffer src/storage/lmgr src/storage/ipc src/storage/aio src/storage/large_object src/access/transam 2>/dev/null | sort -u   # L3
grep -rl 'TODO(pg-port)' src/nodes src/catalog src/utils/cache src/access/table src/access/index src/access/heap src/access/nbtree src/access/hash src/access/gin src/access/gist src/access/brin src/access/spgist src/access/sequence src/access/tablesample src/access/rmgrdesc src/access/*.rs src/bootstrap 2>/dev/null | sort -u   # L4
grep -rl 'TODO(pg-port)' src/parser src/rewrite src/optimizer src/partitioning src/statistics src/foreign src/jit 2>/dev/null | sort -u   # L5
grep -rl 'TODO(pg-port)' src/executor src/commands src/tcop 2>/dev/null | sort -u   # L6
grep -rl 'TODO(pg-port)' src/libpq src/postmaster src/replication src/backup src/archive src/fe_utils src/main 2>/dev/null | sort -u   # L7
```

## The loop

1. Pick the lowest layer that still has pending files.
2. Split its pending files into batches of ~10-15, grouped by subdirectory - files that
   share a `.h` go in the same batch.
3. Dispatch one subagent per batch (`TASKS.md`, with the batch's file list). One batch at a time;
   do NOT open a higher layer until the current layer's batches are in and the gate passes.
4. Gate after each batch, and before advancing a layer:
   `cargo check --bin postgres` green (no new errors vs the pre-batch baseline);
   `cargo test` `0 failed`.
5. Repeat until `TODO(pg-port)` greps to 0 across `src/` (except relabeled architectural
   markers, below).

Then a **conformance sweep**: walk the same layer order over files that had no stub but
predate `CONVENTIONS.md`, applying the conventions only. Track completion in a `.round2-done`
ledger (append each finished path) so the sweep is resumable; done when every file is listed.

## Batching rules
- Keep a `.c`/`.h` pair and its tightly-coupled siblings together.
- ~10-15 files; fewer for dense modules (`utils/adt`, `optimizer`, `executor`).
- A batch should be cohesive enough to compile on its own.

## Cascading edits (route OUT of a batch)
A change to a SHARED signature/struct/const (a `List` field -> `Vec`, `*mut Foo` -> `&mut
Foo` on a `pub fn`) breaks every user across the tree. Do NOT do these inside a per-batch
task - a batch agent may revert the change to keep green. Make each its OWN
definition-rooted task with an ungameable done-condition: the new shape is present AND the
old access pattern greps to 0 across `src/` AND the build is green. (`CONVENTIONS.md`:
Single source of truth.) A subagent that hits one STOPS and reports it; the main agent
schedules it separately.

## Architectural TODOs (subsystem rewrites, not translations)
Some stubs are whole-subsystem redesigns; subagents RELABEL them, never fake them, and they
drain as their own goals after the per-file queue empties, in this order:
1. error model - `setjmp`/`longjmp`/`PG_TRY` -> typed panic + per-command `catch_unwind`.
2. memory model - `MemoryContext` -> RAII + typed arenas + per-connection cap.
3. node tagged-union -> Rust enum (cross-cutting; staged, not per-file).
4. method-structs -> traits (`TableAmRoutine`/fmgr/`DestReceiver`).
5. multiprocess -> async - `shmem`/`signal`/`fork`/`atomic`; see `CONVENTIONS.md`
   "Multiprocess model".

## Progress / resume
The `TODO(pg-port)` grep count is the meter and the resume point - the loop is idempotent
(re-run the grep, take the next pending batch in layer order). Baseline at start: 500 files
carry a stub; 5,312 `unimplemented!()` calls. Build gate command: `cargo check --bin postgres`.

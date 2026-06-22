# TASKS - rewrite one batch (subagent procedure)

Your job: take the batch of files the main agent assigned, make them conform
to `CONVENTIONS.md`, and leave the build green. Edit only your batch plus the cascade fixes
step 4 forces.

C-source oracle: `postgres/` (PostgreSQL 18.3). Compare each `.rs` to the `.h`/`.c` it was
translated from - `postgres/src/backend/...`, `postgres/src/include/...`.

## Procedure

1. Read `CONVENTIONS.md`. It holds every rule - invariants, naming, includes, pointers,
   types, error model, single source of truth, and the rest. Apply it. This file does not
   repeat the rules; where the two seem to differ, CONVENTIONS wins.

2. Diff each `.rs` against its `.h`/`.c` in the oracle and edit the `.rs` to match. The C is
   the spec for behavior; `CONVENTIONS.md` is the spec for form.

3. Resolve every `TODO(pg-port)` / `unimplemented!()` in the batch by kind:
   - duplicate stub of an item that already exists elsewhere -> delete it, `use` the
     canonical one.
   - default-return stub (`null_mut()` / `false` / `0` / `InvalidOid`) -> translate the
     real body from the `.c`; its dependencies exist now.
   - intentional (`valgrind` no-op, platform-gated) -> leave it.
   - architectural (`setjmp`/`PG_TRY`, `MemoryContext`, `shmem`/`signal`/`fork`/`atomic`)
     -> relabel per `PLANS.md` "Architectural TODOs"; never fake a body.

4. Cascade with the compiler. Build; fix every break your edits caused in other files. If a
   change to a SHARED signature/struct/const ripples widely, STOP - do not force it through
   a batch, and do not revert a correct fix to hide it; hand it back to the main agent as a
   cascade task.

5. End green: `cargo check --bin postgres` with no new errors vs your starting baseline
   (full green is the goal, never regress); `cargo test` `0 failed` for touched modules.

## Done
Every `TODO(pg-port)` in the batch resolved or relabeled; each `.rs` matches its `.h`/`.c`
modulo the conventions; build green; tests pass. Report the files finished, the
`TODO(pg-port)` count delta, and any cascade tasks you handed back.

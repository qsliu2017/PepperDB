# Refactor IndexAmRoutine to a Rust trait

Scope: a concrete refactor of PostgreSQL's `IndexAmRoutine` (`access/amapi.h`) --
a "routine" struct (a struct of function pointers used as a vtable). It is the
best worked example: six built-in implementations, genuinely optional scan
callbacks, and a block of boolean capability flags, so it exercises every part of
the recipe. The recipe in Section 5 is what the other routine structs (appendix)
reuse.

## 1. Approach

`IndexAmRoutine` is a few dozen function pointers plus a block of boolean
capability flags. An
index AM fills a `static const` instance, a handler returns it, and the core
caches it on the relation (`rd_indam`) and dispatches through it.

The refactor:

- **Capability flags -> a `bitflags!` set.** The `amcan*` bools are a capability
  *set*, queried at runtime by the planner. One `AmCaps` value returned by
  `capabilities()` replaces them (Section 2).
- **`IndexAm` is the base trait** -- the required callbacks plus the begin-scan
  factory. `begin_scan` returns a scan **handle** (`IndexScan`), an associated
  type with a lifetime (a GAT) borrowing the relation; `amendscan` is the
  handle's `Drop`.
- **Optional callbacks -> capability supertraits.** Optional scan callbacks
  become supertraits of the scan handle (`PlainScan`, `BitmapScan`,
  `MarkRestore`); optional non-scan groups become supertraits of `IndexAm`
  (`CanReturn`, `ParallelIndexScan`, `Translate`).
- **Static dispatch.** A supertrait bound applies only when the concrete AM type
  is known, so do not hold the AM as `&dyn IndexAm`. The six built-ins are a
  closed `enum` matched per arm (Section 4); extension AMs (e.g. `contrib/bloom`)
  are the open case (`fn`-pointer fallback).
- **Zero-sized impls.** Each AM routine is a process-lifetime singleton; in Rust
  a unit struct -- an `enum` variant or a generic type argument.

Note the state model differs from a table AM: an index AM keeps its per-scan
state in `IndexScanDesc.opaque` (a `void *`) rather than embedding a base
descriptor, so in Rust the handle is simply the AM's concrete scan type owning
that state, with the shared `IndexScanDescData` fields alongside.

## 2. Capability flags -> a `bitflags!` set

The 19 `amcan*`-style bools port directly to one `bitflags!` value -- clean
independent bits, no behaviour:

```rust
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AmCaps: u32 {
        const ORDER             = 1 << 0;   // amcanorder
        const ORDER_BY_OP       = 1 << 1;   // amcanorderbyop
        const HASH              = 1 << 2;   // amcanhash
        const CONSISTENT_EQ     = 1 << 3;   // amconsistentequality
        const CONSISTENT_ORD    = 1 << 4;   // amconsistentordering
        const BACKWARD          = 1 << 5;   // amcanbackward
        const UNIQUE            = 1 << 6;   // amcanunique
        const MULTICOL          = 1 << 7;   // amcanmulticol
        const OPTIONAL_KEY      = 1 << 8;   // amoptionalkey
        const SEARCH_ARRAY      = 1 << 9;   // amsearcharray
        const SEARCH_NULLS      = 1 << 10;  // amsearchnulls
        const STORAGE           = 1 << 11;  // amstorage
        const CLUSTERABLE       = 1 << 12;  // amclusterable
        const PRED_LOCKS        = 1 << 13;  // ampredlocks
        const PARALLEL          = 1 << 14;  // amcanparallel
        const BUILD_PARALLEL    = 1 << 15;  // amcanbuildparallel
        const INCLUDE           = 1 << 16;  // amcaninclude
        const USE_MAINT_WORKMEM = 1 << 17;  // amusemaintenanceworkmem
        const SUMMARIZING       = 1 << 18;  // amsummarizing
    }
}
```

The planner reads `am.capabilities().contains(AmCaps::BACKWARD)` exactly where C
reads `amroutine->amcanbackward`. The non-flag scalar fields (`amstrategies`,
`amsupport`, `amoptsprocnum`, `amkeytype`) are per-AM constants -- associated
consts on the impl. `amparallelvacuumoptions` is itself a flag word and ports as
its own `bitflags!`.

## 3. Section-by-section mapping

| Group (`IndexAmRoutine`) | Members | Required? | Rust target |
|---|---|---|---|
| Properties | `amstrategies`, `amsupport`, `amoptsprocnum`, `amkeytype` | data | associated consts |
| Capability flags | `amcanorder` .. `amsummarizing` (19 bools) | data | `AmCaps` via `capabilities()` |
| | `amparallelvacuumoptions` | data (flag word) | its own `bitflags!` |
| Build & maintenance | `ambuild`, `ambuildempty`, `aminsert`, `ambulkdelete`, `amvacuumcleanup`, `amcostestimate`, `amoptions`, `amvalidate` | required | `IndexAm` methods |
| | `aminsertcleanup`, `amgettreeheight`, `amproperty`, `ambuildphasename`, `amadjustmembers` | optional | default method (or small supertrait) |
| | `amcanreturn` (index-only scan) | optional | `CanReturn: IndexAm` |
| Scan | `ambeginscan` | required | `IndexAm::begin_scan` (factory) |
| | `amrescan`, `amendscan` | required | `IndexScan` handle (`rescan`, `Drop`) |
| | `amgettuple` | optional | `PlainScan: IndexScan` |
| | `amgetbitmap` | optional | `BitmapScan: IndexScan` |
| | `ammarkpos`, `amrestrpos` | optional pair | `MarkRestore: PlainScan` |
| Parallel scan | `amestimateparallelscan`, `aminitparallelscan`, `amparallelrescan` | optional group | `ParallelIndexScan: IndexAm` |
| Planning | `amtranslatestrategy`, `amtranslatecmptype` | optional | `Translate: IndexAm` |

The base trait is the required callbacks plus the scan factory:

```rust
pub trait IndexAm {
    type Scan<'a>: IndexScan where Self: 'a;   // borrows the index relation

    const STRATEGIES: u16;                     // amstrategies
    const SUPPORT: u16;                        // amsupport
    fn capabilities(&self) -> AmCaps;          // the amcan* bools (Section 2)

    // Build & maintenance (required)
    fn build(&self, heap: &Relation, index: &Relation, info: &IndexInfo) -> IndexBuildResult;
    fn build_empty(&self, index: &Relation);
    fn insert(&self, index: &Relation, values: &[Datum], isnull: &[bool],
              heap_tid: ItemPointer, heap: &Relation, check: IndexUniqueCheck,
              unchanged: bool, info: &IndexInfo) -> bool;
    fn bulk_delete(&self, info: &IndexVacuumInfo, stats: Option<IndexBulkDeleteResult>,
                   callback: &mut dyn FnMut(ItemPointer) -> bool) -> IndexBulkDeleteResult;
    fn vacuum_cleanup(&self, info: &IndexVacuumInfo,
                      stats: Option<IndexBulkDeleteResult>) -> Option<IndexBulkDeleteResult>;
    fn cost_estimate(&self, root: &PlannerInfo, path: &mut IndexPath, /* ... */);
    fn options(&self, reloptions: &[Datum], validate: bool) -> Option<Bytea>;
    fn validate(&self, opclassoid: Oid) -> bool;

    // Scan factory (required)
    fn begin_scan<'a>(&'a self, index: &'a Relation, nkeys: i32, norderbys: i32) -> Self::Scan<'a>;

    // Optional non-scan callbacks (aminsertcleanup, amgettreeheight, amproperty,
    // ambuildphasename, amadjustmembers): provided default methods.
}
```

The scan handle carries the live scan operations; `amendscan` is `Drop`:

```rust
pub trait IndexScan {                          // ambeginscan/amrescan/amendscan required
    fn rescan(&mut self, keys: &[ScanKey], orderbys: &[ScanKey]);
}                                              // amendscan -> Drop
```

Optional scan callbacks are supertraits of the handle; optional non-scan groups
are supertraits of `IndexAm`:

```rust
pub trait PlainScan: IndexScan {               // amgettuple -- NULL in BRIN
    fn get_tuple(&mut self, dir: ScanDirection) -> Option<ItemPointer>;
}
pub trait BitmapScan: IndexScan {              // amgetbitmap
    fn get_bitmap(&mut self, tbm: &mut TidBitmap) -> i64;
}
pub trait MarkRestore: PlainScan {             // ammarkpos / amrestrpos (ordered plain scans)
    fn mark_pos(&mut self);
    fn restore_pos(&mut self);
}

pub trait CanReturn: IndexAm {                 // amcanreturn -- supports index-only scans
    fn can_return(&self, index: &Relation, attno: i32) -> bool;
}
pub trait ParallelIndexScan: IndexAm {         // the amestimateparallelscan group (all or none)
    fn estimate_parallel_scan(&self, nkeys: i32, norderbys: i32) -> usize;
    fn init_parallel_scan(&self, target: &mut ParallelIndexScanShared);
    fn parallel_rescan(&self, scan: &mut Self::Scan<'_>);
}
pub trait Translate: IndexAm {                 // amtranslatestrategy / amtranslatecmptype
    fn translate_strategy(&self, strategy: StrategyNumber, opfamily: Oid) -> CompareType;
    fn translate_cmptype(&self, cmptype: CompareType, opfamily: Oid) -> StrategyNumber;
}
```

Notes on the cases that are not a plain required/optional split:

- **At least one of `amgettuple`/`amgetbitmap` is required.** "Implements
  `PlainScan` or `BitmapScan`, but not necessarily both" is not a bound Rust can
  express, so that invariant stays a load-time check, as in C. BRIN implements
  `BitmapScan` only (its handler sets `amgettuple = NULL`); btree implements both.
- **`ammarkpos`/`amrestrpos` only make sense for ordered tuple-at-a-time scans,**
  so `MarkRestore` is a supertrait of `PlainScan`, not of `IndexScan`.
- **The lone optional non-scan callbacks** (`aminsertcleanup`, `amgettreeheight`,
  `amproperty`, `ambuildphasename`, `amadjustmembers`) are skippable callbacks the
  caller NULL-checks; a provided default method (no-op / `None`) is the pragmatic
  mapping rather than a supertrait per callback.

## 4. Dispatch

PG resolves the AM at runtime (`pg_class.relam` -> `pg_am.amhandler` -> handler ->
routine pointer, cached on the relation). To keep the capability supertraits
reachable, the concrete type must be known at the call site, so do not hold the
AM as `&dyn IndexAm` -- a trait object cannot reach a supertrait's methods without
`Any` downcasting, which re-adds the runtime check.

The six built-in index AMs are a closed `enum`, matched per arm:

```rust
enum IndexAmKind { Btree, Hash, Gist, Gin, SpGist, Brin }
```

Within an arm the concrete type is known, so its supertraits are in scope at
compile time: the `Btree` arm can call `MarkRestore`/`PlainScan`/`BitmapScan`
methods; the `Brin` arm only `BitmapScan`. Runtime-registered extension index AMs
(`contrib/bloom`, and out-of-tree AMs) are the open case the closed `enum` does
not cover; handle those with a separate `fn`-pointer fallback, keeping the
built-in path `dyn`-free.

## 5. The recipe (for the other routine structs)

Apply the same steps to any struct in the appendix:

1. **Boolean capability-flag fields** (a struct may carry runtime-queried `bool`
   flags, like the `amcan*` block here) -> one `bitflags!` value returned by a
   `capabilities()` method. Non-flag scalar fields -> associated consts.
2. **One base trait** per routine struct for the required callbacks; **associated
   types** (GATs borrowing the relation) for the handles its `begin`-style
   callbacks return. Per-handle operations move onto a handle trait; the C
   `*_end` callback becomes `Drop`.
3. **Walk the struct's groups.** Required callbacks -> base methods. Each optional
   group -> a capability supertrait (of the base trait, or of the handle trait for
   per-scan modes); group all-or-none callbacks into one supertrait. Lone
   skippable callbacks -> a provided default method.
4. **Static dispatch.** Model the built-in implementations as a closed `enum` (or
   a generic bound); do not use `&dyn`. Extension implementations are the open
   case (`fn`-pointer fallback).

## Appendix: other routine structs in the tree

Small, enumerable set. Grouped by how each marks optional callbacks -- the input
to step 3.

### A. Catalog/handler-resolved AMs (resolved at runtime via `pg_am`)

| Struct | Header | Optional-callback style |
|--------|--------|-------------------------|
| `IndexAmRoutine` | `access/amapi.h` | this RFC: `amcan*` flags -> `bitflags!`; optional scan callbacks (`amgettuple`/`amgetbitmap`/mark-restore) and groups (parallel, translation) -> supertraits |
| `TableAmRoutine` | `access/tableam.h` | comment sections; optional sections (TID-range, bitmap, TOAST, `finish_bulk_insert`) -> supertraits; index *fetch* is required (asserted by `GetTableAmRoutine`) |
| `TsmRoutine` | `access/tsmapi.h` | per-field `/* can be NULL */` (`InitSampleScan`, `NextSampleBlock`) |

### B. Hook/extension method tables (NULL-checked at call sites)

| Struct | Header | Optional-callback style |
|--------|--------|-------------------------|
| `FdwRoutine` | `foreign/fdwapi.h` | block comment "Remaining functions are optional ... NULL"; guarded e.g. `explain.c`, `analyze.c`, `execMain.c` |
| `CustomScanMethods` / `CustomPathMethods` | `nodes/extensible.h` | all required |
| `CustomExecMethods` | `nodes/extensible.h` | grouped `/* Optional methods: needed if mark/restore is supported */`, `/* ... parallel execution ... */` |
| `OutputPluginCallbacks` | `replication/output_plugin.h` | runtime NULL checks (`if (ctx->callbacks.filter_prepare_cb != NULL)`) |
| `ArchiveModuleCallbacks` | `archive/archive_module.h` | "ArchiveFileCB is the only required callback" |
| `RmgrData` | `access/xlog_internal.h` | runtime NULL checks; `rm_mask`, `rm_decode` optional |
| `XLogReaderRoutine` | `access/xlogreader.h` | `page_read` may be NULL; `segment_open`/`segment_close` required |
| `TableFuncRoutine` | `executor/tablefunc.h` | `SetNamespace` may be NULL |
| `SubscriptRoutines` | `nodes/subscripting.h` | assignment-only callbacks omittable |
| `JitProviderCallbacks` | `jit/jit.h` | all required |
| `OAuthValidatorCallbacks` | `libpq/oauth.h` | "ValidatorValidateCB is the only required callback" |
| `IoMethodOps` | `storage/aio_internal.h` | per-field `/* Optional. */` |

### C. Per-instance behaviour tables (selected per object; required-heavy)

| Struct | Header | Optional-callback style |
|--------|--------|-------------------------|
| `TupleTableSlotOps` | `executor/tuptable.h` | `get_heap_tuple`/`get_minimal_tuple` set NULL when slot can't own that form |
| `CopyToRoutine` / `CopyFromRoutine` | `commands/copyapi.h` | all required |
| `MemoryContextMethods` | `nodes/memnodes.h` | "All callbacks are mandatory" (`check` only under `MEMORY_CONTEXT_CHECKING`) |
| `ExtensibleNodeMethods` | `nodes/extensible.h` | "All callbacks are mandatory" |
| `ExpandedObjectMethods` | `utils/expandeddatum.h` | all required |
| `PgAioHandleCallbacks` | `storage/aio.h` | all required |

Verdict by group: A and B map to a base trait plus capability supertraits for the
optional groups (and a `bitflags!` set for any `amcan*`-style flag fields),
dispatched statically over a closed `enum` of the built-ins. C is the same trait
mapping but required-heavy, so few or no supertraits.

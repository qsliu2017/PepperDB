# Map common C function-signature idioms to Rust

Scope: how PostgreSQL's recurring C calling conventions -- status returns,
not-found sentinels, pointer out-params, and `void *` -- should be re-expressed
when ported to Rust.

## 1. Summary

PostgreSQL's C functions encode failure, absence, and extra outputs through a
few recurring conventions: a returned status `int`, a sentinel value
(`InvalidOid`, `-2`, an invalid `HeapTuple`), pointer out-params the callee
fills in, and `void *` for anything generic. Rust has a dedicated construct for
each, so the port is largely a matter of recognising the convention and picking
the matching tool. This RFC catalogs those conventions and their Rust targets.

Porting principles:

- **Map by intent, not by shape.** Classify what a signature *means* -- fallible,
  optional, multi-output, generic -- before rewriting it. The mapping is not 1:1.
- **Prefer the precise type.** `Result` for failure-as-data, `Option` for
  absence, a tuple or named struct for multiple outputs, generics or an `enum`
  for `void *`.
- **Avoid trait objects.** `Box<dyn Trait>` is a code smell; reach for generics,
  an `enum`, or a closure instead.
- **Know the boundaries.** Some conventions are out of scope -- notably the
  pervasive `elog(ERROR)` longjmp error model (Section 3.1).

## 2. Decision rule / mapping table

| C idiom | Rust target | When |
|---------|-------------|------|
| Returns status `int`/`bool` for a genuinely fallible op (parse, lookup, network, SPI) | `Result<T, E>` | The failure is data the caller must handle |
| `elog(ERROR)` / `ereport(ERROR)` longjmp | out of scope | See 3.1; `Result` does not cover it |
| Sentinel for "absent" (`InvalidOid`, invalid tuple, `-2`, NULL ptr) | `Option<T>` | Not-found is normal, not an error |
| One trailing out-param + meaningful return | return a tuple, or fold the flag into the value | e.g. `(Datum, isnull)` -> `Option<Datum>` |
| 2-3 unnamed, hard-to-confuse out-params | tuple | order is obvious |
| 4+ out-params, or same-typed/confusable ones | named struct | order is a footgun |
| Out-param the caller may pass `NULL` to skip | `Option` in the return, or a separate fn | the skip is the signal |
| `void *` homogeneous container/comparator element | generic `<T>` | one real type per instantiation |
| `void *` tagged union dispatched on a discriminant | `enum` | `Node`/`NodeTag`, `Datum` |
| `void *` runtime-pluggable vtable | closed `enum`, or a struct of `fn` pointers -- avoid `Box<dyn Trait>` | AM routines, fmgr, plugins |
| `void *arg` opaque callback context | captured closure | the threaded-through `arg` |

## 3. Errors handling

### 3.1 The longjmp error path

PostgreSQL's dominant error mechanism is not a returned status code -- it is
`ereport`/`elog(ERROR)`, a non-local `longjmp` to the nearest `PG_TRY/PG_CATCH`
frame (`src/include/utils/elog.h`, `src/backend/utils/error/elog.c`). Rust has no
`longjmp`; the replacement is `panic!` contained by `catch_unwind`, with `Result`
as the eventual end goal. Translating that path is not addressed here.

The consequence for this document: only the minority of functions that actually
return a status code map to `Result` (3.2). The pervasive `elog(ERROR)` path keeps
its panic semantics and is not addressed below.

### 3.2 Functions that DO return a status -> `Result`

These are the minority that already hand failure back to the caller. They map
to `Result<T, E>` directly and should.

C (SPI, returns an int status code):

```c
/* src/include/executor/spi.h */
#define SPI_OK_SELECT        5
#define SPI_ERROR_CONNECT  (-1)
#define SPI_ERROR_ARGUMENT (-7)
extern int SPI_execute(const char *src, bool read_only, long tcount);
```

C (parse, returns bool success + out-param):

```c
/* src/backend/utils/adt/bool.c */
bool parse_bool(const char *value, bool *result);
bool parse_bool_with_len(const char *value, size_t len, bool *result);
```

C (libpq, returns a status enum):

```c
/* src/interfaces/libpq/libpq-fe.h */
typedef enum { PGRES_COMMAND_OK, PGRES_TUPLES_OK, PGRES_FATAL_ERROR, ... } ExecStatusType;
extern ExecStatusType PQresultStatus(const PGresult *res);
```

Rust (the `SPI_OK_*` / `SPI_ERROR_*` codes split into a success enum and an error
enum -- shown partially, matching the C constants above):

```rust
enum SpiOk { Select, /* Insert, Update, Delete, ... */ }   // the SPI_OK_* codes
enum SpiError { Connect, Argument, /* ... */ }             // the SPI_ERROR_* codes

fn spi_execute(src: &str, read_only: bool, tcount: i64) -> Result<SpiOk, SpiError>;
fn parse_bool(value: &str) -> Result<bool, ParseError>;  // success+out-param collapses
```

Caveat: `pg_strtoint16_safe(const char *s, Node *escontext)`
(`src/backend/utils/adt/numutils.c`) shows PG's own hybrid -- it returns the
value but routes the error through an optional `escontext` node, behaving like
longjmp when `escontext` is NULL and like a soft error otherwise. In Rust this
is just `Result`; the `escontext` switch disappears.

Caveat: pick `E` per boundary. SPI codes, libpq statuses, and parse failures are
distinct error domains -- do not flatten them into one mega-enum.

## 4. "Not found" -> `Option<T>`

Absence-by-sentinel is a different idiom from failure, and it maps to `Option`,
not `Result`. Returning `Result` for an ordinary miss forces callers to invent
error values for the non-error case.

C:

```c
/* src/include/utils/hsearch.h -- foundPtr out-param signals presence */
extern void *hash_search(HTAB *hashp, const void *keyPtr,
                         HASHACTION action, bool *foundPtr);

/* src/include/utils/syscache.h -- invalid HeapTuple means not found */
extern HeapTuple SearchSysCache1(int cacheId, Datum key1);
extern Oid GetSysCacheOid(int cacheId, AttrNumber oidcol,
                          Datum key1, Datum key2, Datum key3, Datum key4);

/* src/include/utils/lsyscache.h -- InvalidOid sentinel */
extern Oid get_relname_relid(const char *relname, Oid relnamespace);

/* src/include/nodes/bitmapset.h -- returns -2 when exhausted */
extern int bms_next_member(const Bitmapset *a, int prevbit);
```

Rust:

```rust
// hash_search's HASHACTION arg selects the operation; in Rust that is distinct
// methods, not a parameter (cf. std HashMap):
fn get(&self, key: &K) -> Option<&V>;             // HASH_FIND
fn entry(&mut self, key: &K) -> &mut V;           // HASH_ENTER (insert-or-get)
fn remove(&mut self, key: &K) -> Option<V>;       // HASH_REMOVE

fn search_syscache1(cache: CacheId, k: Datum) -> Option<HeapTuple>;
fn get_relname_relid(name: &str, ns: Oid) -> Option<Oid>;  // InvalidOid -> None
fn bms_next_member(&self, prev: i32) -> Option<i32>;       // -2 -> None
```

Note the `foundPtr` out-param vanishes: in C it exists only because the `void *`
return can't distinguish "found NULL-valued entry" from "absent". `Option`
encodes both states in the return. The `HASHACTION` selector disappears too --
find/enter/remove become separate methods rather than one function branching on
an action argument.

Representation: the `Option` wrapper is free. A pointer-like inner type carries a
null niche, so `Option<&T>`, `Option<&mut T>`, `Option<Box<T>>`, and
`Option<NonNull<T>>` are all one word with `None` reusing the null pattern. Pick
by ownership, not size: prefer `Option<&T>` / `Option<Box<T>>` at the safe API
surface (lifetimes and ownership, niche included); reserve `Option<NonNull<T>>`
for the raw FFI/unsafe layer, where it is the right way to encode a nullable
`T*` -- explicit nullability, still one word, but `unsafe` to deref and no
lifetime or aliasing guarantees. A bare `*mut T` is never the porting target.

Caveat: distinguish a *miss* from a *failure*. `SearchSysCacheN` can also
`elog(ERROR)` on a corrupt cache; that path stays whatever Section 3.1 decides.
`Option` is only for the ordinary not-found result. Some PG functions take a
`bool missing_ok` (e.g. `LookupExplicitNamespace(name, missing_ok)`,
`src/backend/catalog/namespace.c`) that toggles between "return `InvalidOid`"
and "`elog(ERROR)`" -- in Rust that is exactly `Option` (caller decides via
`.ok_or(...)` or `.expect(...)`), so drop the flag.

## 5. Out-parameters -> tuple, struct, or Option

Two or three unnamed outputs map to a tuple, but "out-params -> tuple" is too
coarse on its own; the choice depends on count and confusability.

### 5.1 One out-param beside a return -> fold it in

C:

```c
/* src/include/access/htup_details.h */
static inline Datum heap_getattr(HeapTuple tup, int attnum,
                                 TupleDesc tupleDesc, bool *isnull);
```

The `bool *isnull` exists because a `Datum` has no null state. Rust has one:

```rust
fn heap_getattr(tup: &HeapTuple, attnum: i32, desc: &TupleDesc) -> Option<Datum>;
```

Same for `slot_getattr` and the `fcinfo->isnull` flag in `fmgr.h` -- a nullable
`Datum` is an `Option<Datum>`.

### 5.2 Two or three outputs -> tuple

C:

```c
/* src/include/utils/datetime.h */
extern void j2date(int jd, int *year, int *month, int *day);
```

Rust:

```rust
fn j2date(jd: i32) -> (i32, i32, i32);   // or a Date newtype, since the fields are confusable
```

A bare `(i32, i32, i32)` is acceptable here but borderline -- three same-typed
ints invite swaps. A named return type pays off as soon as the fields can be
confused.

### 5.3 Four or more, or confusable -> named struct

C:

```c
/* src/include/utils/lsyscache.h -- 6 outputs of mixed types */
extern void get_type_io_data(Oid typid, IOFuncSelector which_func,
                             int16 *typlen, bool *typbyval, char *typalign,
                             char *typdelim, Oid *typioparam, Oid *func);

/* src/include/optimizer/plancat.h -- 4 outputs */
extern void estimate_rel_size(Relation rel, int32 *attr_widths,
                              BlockNumber *pages, double *tuples,
                              double *allvisfrac);
```

Rust:

```rust
struct TypeIoData { len: i16, by_val: bool, align: u8, delim: u8, io_param: Oid, func: Oid }
fn get_type_io_data(typid: Oid, which: IoFuncSelector) -> TypeIoData;

struct RelSizeEstimate { attr_widths: Vec<i32>, pages: BlockNumber, tuples: f64, allvisfrac: f64 }
fn estimate_rel_size(rel: &Relation) -> RelSizeEstimate;
```

A six-tuple is unreadable at the call site and trivially mis-ordered; the struct
names each field.

### 5.4 Skippable out-param (`NULL` allowed) -> Option in the return

C: `DecodeDateTime(..., int *tzp, ...)` guards `if (tzp != NULL) *tzp = 0;`
(`src/backend/utils/adt/datetime.c`) -- the caller passes `NULL` to opt out of
the timezone output.

Rust: make the optional output an `Option` field of the returned struct (the
callee always computes it; the caller ignores it for free), or provide a
separate function that omits it. Do not port `Option<&mut T>` out-params -- they
re-create the C pattern Rust is trying to remove.

## 6. `void *` -> classify before mapping

"`void *` -> `<T>`" collapses four distinct uses. Classify first. Three have
clean targets and are detailed below (6.1-6.3). The fourth -- a runtime-pluggable
vtable (fmgr, access-method routines, output plugins), where the implementation
is chosen at run time -- has no clean local rewrite. The obvious mapping is
`Box<dyn Trait>`, but trait objects are a code smell we avoid. Where the set of
implementations is closed, use an `enum` and dispatch with `match`; where it is
open, mirror the C with a struct of `fn` pointers. Do not reach for
`Box<dyn Trait>`.

### 6.1 Homogeneous container/element -> generic `<T>`

C:

```c
/* src/include/lib/sort_template.h -- one element type per instantiation */
typedef int (*ST_COMPARATOR_TYPE_NAME)(const ST_ELEMENT_TYPE *,
                                       const ST_ELEMENT_TYPE * ST_SORT_PROTO_ARG);
/* src/port/qsort.c instantiates it with void element type as pg_qsort */
```

`simplehash.h` and dynahash store entries as `void *` for the same reason: one
real entry type per use site. These monomorphize cleanly to `<T>`:

```rust
fn pg_qsort<T>(items: &mut [T], cmp: impl Fn(&T, &T) -> Ordering);
```

### 6.2 Tagged union on a discriminant -> `enum` (not generics, not dyn)

C:

```c
/* src/include/nodes/nodes.h */
typedef struct Node { NodeTag type; } Node;       /* first field tags the variant */
#define nodeTag(nodeptr) (((const Node*)(nodeptr))->type)
```

Every parse/plan node is a `Node *` whose real type is recovered from the
leading `NodeTag`. `Datum` (`src/include/postgres.h`, `typedef uintptr_t Datum`
with the `DatumGetX`/`XGetDatum` family) is the same shape: one machine word
reinterpreted per a type the caller knows from context.

This is a closed, discriminated set -- a Rust `enum`, with `match` replacing
`IsA`/`nodeTag`. Using `<T>` or `dyn` here would be wrong: the variants are
known at compile time and dispatch is on a stored tag, which is exactly what an
`enum` is. Nodes nest (a plan node holds child plans), so the recursive variants
need indirection -- `Box` (or a slice/arena) -- to keep the `enum` sized:

```rust
enum Node { SeqScan(Box<SeqScan>), HashJoin(Box<HashJoin>), /* ... */ }
```

### 6.3 Opaque callback context (`void *arg`) -> captured closure

C:

```c
/* src/include/utils/elog.h */
typedef struct ErrorContextCallback {
    struct ErrorContextCallback *previous;
    void (*callback)(void *arg);
    void *arg;                 /* caller-specific state, threaded through */
} ErrorContextCallback;
```

`MemoryContextCallback` (`src/include/utils/palloc.h`) and the `void *arg` of
`qsort_arg` are identical: the `arg` only exists so a C function pointer can
carry state. A Rust closure captures that state directly, so the `arg` field
disappears:

```rust
fn register_reset_callback(ctx: &MemoryContext, cb: impl FnOnce());
```

## Appendix: concrete instances found in the tree

### A. Status-returning -> `Result` (3.2)

| Function / type | Header | Returns |
|-----------------|--------|---------|
| `SPI_execute`, `SPI_OK_*`/`SPI_ERROR_*` | `executor/spi.h` | int status code |
| `parse_bool`, `parse_bool_with_len` | `utils/adt/bool.c` | bool success + out-param |
| `pg_strtoint16_safe` (and 32/64) | `utils/adt/numutils.c` | value + optional `escontext` |
| `pg_snprintf`, `pg_vsnprintf` | `port.h` | int length |
| `PQresultStatus` / `ExecStatusType`, `PQexec` | `interfaces/libpq/libpq-fe.h` | status enum |

### B. longjmp error path -> out of scope (3.1)

| Symbol | Header / source |
|--------|-----------------|
| `elog`, `ereport`, `ereport_domain` | `utils/elog.h` |
| `PG_TRY`/`PG_CATCH`/`PG_END_TRY`/`PG_RE_THROW` | `utils/elog.h` |
| `PG_exception_stack`, `pg_re_throw`, `errfinish` | `utils/error/elog.c` |
| `ErrorContextCallback`, `error_context_stack` | `utils/elog.h` |

### C. Not-found sentinel -> `Option` (4)

| Function | Header | Sentinel |
|----------|--------|----------|
| `hash_search`, `hash_search_with_hash_value` | `utils/hsearch.h` | NULL + `foundPtr` |
| `SearchSysCache1`..`4` | `utils/syscache.h` | invalid `HeapTuple` |
| `GetSysCacheOid` | `utils/syscache.h` | `InvalidOid` |
| `get_relname_relid` | `utils/lsyscache.h` | `InvalidOid` |
| `LookupExplicitNamespace(.., missing_ok)` | `catalog/namespace.h` | `InvalidOid` when `missing_ok` |
| `bms_next_member` | `nodes/bitmapset.h` | `-2` |
| `bms_get_singleton_member` | `nodes/bitmapset.h` | bool + `*member` |

### D. Out-params -> tuple / struct / Option (5)

| Function | Header | # outs | Target |
|----------|--------|--------|--------|
| `heap_getattr`, `fastgetattr`, `slot_getattr` | `access/htup_details.h`, `executor/tuptable.h` | 1 (`isnull`) | `Option<Datum>` |
| `j2date` | `utils/datetime.h` | 3 | tuple / `Date` |
| `dt2time` | `utils/timestamp.h` | 4 | struct |
| `isoweekdate2date` | `utils/timestamp.h` | 3 | tuple / struct |
| `estimate_rel_size` | `optimizer/plancat.h` | 4 | struct |
| `get_type_io_data` | `utils/lsyscache.h` | 6 | struct |
| `DecodeDateTime` (`int *tzp` skippable) | `utils/datetime.h` | 5 (+ skip) | struct, optional field |

### E. `void *` classified (6)

| Use | Symbol | Header | Target |
|-----|--------|--------|--------|
| Homogeneous element | `qsort_arg` / `sort_template.h`, `pg_qsort` | `lib/sort_template.h`, `port/qsort.c` | `<T>` |
| Homogeneous entries | simplehash entries, dynahash `hash_search` | `lib/simplehash.h`, `utils/hsearch.h` | `<T>` |
| Tagged union | `Node` / `NodeTag` / `nodeTag` / `IsA` | `nodes/nodes.h` | `enum` |
| Tagged any-value | `Datum`, `DatumGetX`/`XGetDatum` | `postgres.h`, `fmgr.h` | `enum` / newtype |
| Runtime vtable | `IndexAmRoutine`, `TableAmRoutine` | `access/amapi.h`, `access/tableam.h` | `enum` / fn ptrs |
| Runtime dispatch | `PGFunction`, `FmgrInfo`, `FunctionCallInvoke` | `fmgr.h` | `enum` / fn ptrs |
| Runtime plugin | `OutputPluginCallbacks` | `replication/output_plugin.h` | `enum` / fn ptrs |
| Opaque context | `ErrorContextCallback.arg`, `MemoryContextCallback.arg`, `qsort_arg` arg | `utils/elog.h`, `utils/palloc.h`, `lib/sort_template.h` | closure |

# Level-1 translation audit rubric

You are a READ-ONLY auditor. Do NOT edit any files. For each `.rs` file in your
assigned list, compare it against its C source (`ref/postgres/src/include/<path>.h`)
and the rule docs, and report deviations where the translation stayed too literal /
mechanical instead of becoming idiomatic Rust.

Rule docs to apply:
- ai/plans/001-header-file/translation-rules.md
- ai/plans/001-header-file/bitflags-port.md  (esp. the appendix verdict tables)
- ai/plans/001-header-file/function-mapping.md
- ai/plans/001-header-file/routine-struct.md

## Anti-patterns to flag

A. **Ordinal group left as flat consts** - a set of related sequential `#define`s
   (0,1,2,3...) or one-of-N codes rendered as a pile of `pub const X: i32 = N;`
   that should be a Rust `enum` (one enum per logical group). Example: the
   per-command PROGRESS_* param/phase/command groups. Report each group's prefix
   and the enum you'd make. (See bitflags-port.md "POOR" category.)

B. **Missed bitflags** - a group of power-of-two flag `#define`s rendered as bare
   consts (or an enum) instead of a `bitflags!` set. Cross-check the bitflags-port
   appendix A (GOOD list) and section 2 rule. Report group + header.
   Also flag the inverse: a `bitflags!` used where the appendix says it should NOT
   be (on-disk word packing a number, one-of-N selector, sequential ordinal).

C. **Tombstone mismatch** (both directions):
   - *Should be tombstone, but literally translated*: the module is subsumed by
     Rust std/core/tokio or is a non-target platform shim (per translation-rules:
     atomics, spinlocks, semaphores, shared memory, threads, win32, etc.) yet got
     a real literal port.
   - *Should be a real type, but over-stubbed*: a type/API that other modules'
     signatures reference was left empty/opaque (`_private: ()`, no fields) when it
     needs a real shape. NOTE: per the README, `stringinfo`, `bitmapset`, simple
     data structures ARE meant to be implemented inline (NOT tombstones) - do not
     flag those as "should be tombstone"; instead judge whether the inline impl is
     adequate.

D. **Trait-object / `Box<dyn Trait>`** - function-mapping.md calls this a code
   smell. Flag `Box<dyn Fn/FnMut/Trait>` and `&dyn`/`dyn` usage; the target is a
   generic `<T>`, a closed `enum`, an `fn` pointer, or a captured closure (generic
   param), per function-mapping section 6 and routine-struct.md.

E. **Out-param / status not folded** - `&mut T` out-params, `bool *ok`, or status
   `int` returns left mechanical instead of `Option`/tuple/named-struct/`Result`
   per function-mapping sections 3-5. (Skeleton signatures should already be in the
   idiomatic shape.)

F. **`#[repr(C)]` misuse** - an on-disk/wire-layout struct missing `#[repr(C)]` or
   its `size_of`/`offset_of!` layout asserts; OR an in-memory struct wrongly given
   `#[repr(C)]`. (translation-rules "on-disk vs in-memory".)

G. **Other literal-C smells** - `*mut T`/`*const T` where `&T`/`&mut T`/`Option<&T>`
   fits and ownership is clear; `core::ffi::c_int`/`c_void` in pure-Rust (non-FFI)
   code; macro_rules! where a `const fn`/`fn` would do; 32-bit/big-endian paths not
   deleted.

## Output format

Return ONLY a findings list, one entry per issue, grouped by file:

```
FILE: src/<path>.rs
- [CATEGORY letter][SEVERITY high|med|low] short description -> suggested fix
```

If a file is clean, do not list it. End with a one-line tally:
`TALLY: <n files audited>, <n findings> (high=<>, med=<>, low=<>)`.
Be precise and conservative - only flag genuine deviations from the rule docs, not
style nitpicks. Quote the C `#define`/decl group when relevant.

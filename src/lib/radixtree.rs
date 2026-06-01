//! lib/radixtree.h - template for an adaptive radix tree (ART), per-caller instantiated.
//!
//! This is a PostgreSQL C *template* header in the same family as
//! `lib/simplehash.h` and `lib/sort_template.h`. The file is `#include`d
//! multiple times, each time with a set of caller-defined macros
//! (`RT_PREFIX`, `RT_VALUE_TYPE`, `RT_SCOPE`, `RT_DECLARE`/`RT_DEFINE`,
//! optionally `RT_SHMEM`, `RT_USE_DELETE`, `RT_DEBUG`,
//! `RT_VARLEN_VALUE_SIZE()`, `RT_RUNTIME_EMBEDDABLE_VALUE`). Including the
//! file `#undef`s every parameter so a fresh radix tree can be generated
//! afterwards.
//!
//! Because *every* type and function name is built with `CppConcat` from
//! `RT_PREFIX` (e.g. `foo_radix_tree`, `foo_create`, `foo_find`), and the
//! struct layouts depend on `RT_VALUE_TYPE` and on whether the tree lives in
//! local memory (`RT_NODE *` child pointers) or in a DSA area (`dsa_pointer`
//! child pointers), the templated body has NO standalone Rust form. It must
//! be re-emitted by whatever Rust generic / macro the eventual caller writes
//! when it instantiates a concrete radix tree. We therefore do NOT attempt to
//! emit a generic `impl` here.
//!
//! What we CAN emit concretely (these symbols are independent of the macro
//! parameters - they are the same numeric values for every instantiation):
//!   - the four node kinds (`RT_NODE_KIND_*`) and their count
//!   - the span / fanout / chunk-mask constants
//!   - the maximum fanout values per node kind that the template uses to lay
//!     out structs
//!   - the size-class enum discriminants
//!   - the "invalid slot index" sentinel and the magic value
//!   - the node-header struct `RT_NODE` (the only struct whose layout is
//!     *not* parameterized by `RT_VALUE_TYPE`/child-pointer type) and the
//!     size-class info struct `RT_SIZE_CLASS_ELEM`
//!   - the small pure-integer inline helpers that do not touch
//!     `RT_VALUE_TYPE` or a concrete node type (`RT_KEY_GET_SHIFT`,
//!     `RT_SHIFT_GET_MAX_VAL`, `RT_GET_KEY_CHUNK`, `RT_BM_IDX`, `RT_BM_BIT`)
//!
//! Everything else (the node4/16/48/256 structs, the radix-tree/control/iter
//! structs, and the create/find/set/delete/iterate/free/stats functions) is
//! documented below as a template instantiated per-caller. See the original
//! C header for the full algorithm; the C is the source of truth.
//!
//! The concept originates from "The Adaptive Radix Tree: ARTful Indexing for
//! Main-Memory Databases" by Leis, Kemper and Neumann, 2013.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]

use crate::c::{int64, uint32, uint64, uint8, Size};
use std::ffi::{c_char, c_int};

// ---------------------------------------------------------------------------
// Concrete, non-parameterized constants
// ---------------------------------------------------------------------------

/// The number of bits encoded in one tree level. `#define RT_SPAN BITS_PER_BYTE`.
pub const RT_SPAN: c_int = 8; // BITS_PER_BYTE

/// The number of possible partial keys (max child pointers) for a node.
/// `#define RT_NODE_MAX_SLOTS (1 << RT_SPAN)`.
pub const RT_NODE_MAX_SLOTS: c_int = 1 << RT_SPAN;

/// Mask for extracting a chunk from a key. `((1 << RT_SPAN) - 1)`.
pub const RT_CHUNK_MASK: c_int = (1 << RT_SPAN) - 1;

/// Maximum level a tree can reach for a key.
/// `((sizeof(uint64) * BITS_PER_BYTE) / RT_SPAN)`.
pub const RT_MAX_LEVEL: c_int = (8 * 8) / RT_SPAN;

/*
 * Node kinds
 *
 * NOTE: There are 4 node kinds, and this should never be increased.
 * The 4 kinds can be represented with 2 bits.
 */
pub const RT_NODE_KIND_4: uint8 = 0x00;
pub const RT_NODE_KIND_16: uint8 = 0x01;
pub const RT_NODE_KIND_48: uint8 = 0x02;
pub const RT_NODE_KIND_256: uint8 = 0x03;
pub const RT_NODE_KIND_COUNT: c_int = 4;

/// Invalid index sentinel for node48's `slot_idxs`. `#define RT_INVALID_SLOT_IDX 0xFF`.
pub const RT_INVALID_SLOT_IDX: uint8 = 0xFF;

/// A magic value used to identify a (shared-memory) radix tree.
/// `#define RT_RADIX_TREE_MAGIC 0x54A48167` (only defined under RT_SHMEM).
pub const RT_RADIX_TREE_MAGIC: uint32 = 0x54A48167;

/*
 * Fanout maximums per node kind. These are independent of macro parameters.
 *
 * RT_FANOUT_4_MAX is `(8 - sizeof(RT_NODE))`; RT_NODE is 3 bytes
 * (kind/fanout/count), so this is 5. The template then hard-codes
 * RT_FANOUT_4 = 4 and StaticAsserts RT_FANOUT_4 <= RT_FANOUT_4_MAX.
 */
/// `#define RT_FANOUT_4_MAX (8 - sizeof(RT_NODE))` -> 8 - 3 == 5.
pub const RT_FANOUT_4_MAX: c_int = 8 - 3;
/// `#define RT_FANOUT_4 4`.
pub const RT_FANOUT_4: c_int = 4;
/// `#define RT_FANOUT_16_MAX 32` (two 128-bit SIMD registers).
pub const RT_FANOUT_16_MAX: c_int = 32;
/// `#define RT_FANOUT_48_MAX 64`.
pub const RT_FANOUT_48_MAX: c_int = 64;
/// `#define RT_FANOUT_256 RT_NODE_MAX_SLOTS` -> 256.
pub const RT_FANOUT_256: c_int = RT_NODE_MAX_SLOTS;

/*
 * Non-shared-memory fanout values (the "! RT_SHMEM" branch). The RT_SHMEM
 * branch derives these from DSA size classes and offsetof(RT_NODE_16/48,
 * children), which depends on sizeof(RT_PTR_ALLOC) == sizeof(dsa_pointer);
 * since that is parameterized per instantiation we emit the local-memory
 * defaults here.
 */
/// `#define RT_FANOUT_16_LO 16` (! RT_SHMEM).
pub const RT_FANOUT_16_LO: c_int = 16;
/// `#define RT_FANOUT_16_HI RT_FANOUT_16_MAX` (! RT_SHMEM).
pub const RT_FANOUT_16_HI: c_int = RT_FANOUT_16_MAX;
/// `#define RT_FANOUT_48 RT_FANOUT_48_MAX` (! RT_SHMEM).
pub const RT_FANOUT_48: c_int = RT_FANOUT_48_MAX;

// ---------------------------------------------------------------------------
// Concrete structs / enums (layout NOT parameterized by RT_VALUE_TYPE)
// ---------------------------------------------------------------------------

/// Common header for all nodes (`typedef struct RT_NODE`). Not parameterized,
/// so it has a concrete Rust form.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RT_NODE {
    /// Node kind, one per search/set algorithm.
    pub kind: uint8,
    /// Max capacity for the current size class.
    pub fanout: uint8,
    /// Number of children.
    pub count: uint8,
}

/*
 * Node size classes (`typedef enum RT_SIZE_CLASS`). Translated as a c_int
 * type plus `pub const` variants, per the C-enum convention.
 */
pub type RT_SIZE_CLASS = c_int;
pub const RT_CLASS_4: RT_SIZE_CLASS = 0;
pub const RT_CLASS_16_LO: RT_SIZE_CLASS = 1;
pub const RT_CLASS_16_HI: RT_SIZE_CLASS = 2;
pub const RT_CLASS_48: RT_SIZE_CLASS = 3;
pub const RT_CLASS_256: RT_SIZE_CLASS = 4;
/// `#define RT_NUM_SIZE_CLASSES lengthof(RT_SIZE_CLASS_INFO)` -> 5.
pub const RT_NUM_SIZE_CLASSES: c_int = 5;

/// Information for each size class (`typedef struct RT_SIZE_CLASS_ELEM`).
/// Layout is not parameterized (name/fanout/allocsize), so concrete.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RT_SIZE_CLASS_ELEM {
    pub name: *const c_char,
    pub fanout: c_int,
    pub allocsize: Size,
}

// ---------------------------------------------------------------------------
// Concrete pure-integer inline helpers
// ---------------------------------------------------------------------------

/// `#define RT_GET_KEY_CHUNK(key, shift) ((uint8)(((key) >> (shift)) & RT_CHUNK_MASK))`.
#[inline]
pub fn RT_GET_KEY_CHUNK(key: uint64, shift: c_int) -> uint8 {
    ((key >> shift) & (RT_CHUNK_MASK as uint64)) as uint8
}

/// `#define RT_BM_IDX(x) ((x) / BITS_PER_BITMAPWORD)`.
/// BITS_PER_BITMAPWORD is parameterized by bitmapword width; the default
/// PostgreSQL build uses 64-bit bitmapword. See nodes/bitmapset.
#[inline]
pub fn RT_BM_IDX(x: c_int) -> c_int {
    // BITS_PER_BITMAPWORD == 64 on the default 64-bit build.
    x / 64
}

/// `#define RT_BM_BIT(x) ((x) % BITS_PER_BITMAPWORD)`.
#[inline]
pub fn RT_BM_BIT(x: c_int) -> c_int {
    x % 64
}

/// Return the smallest shift that will allow storing the given key.
/// `static inline int RT_KEY_GET_SHIFT(uint64 key)`.
#[inline]
pub fn RT_KEY_GET_SHIFT(key: uint64) -> c_int {
    if key == 0 {
        0
    } else {
        // (pg_leftmost_one_pos64(key) / RT_SPAN) * RT_SPAN
        let pos = 63 - (key.leading_zeros() as c_int); // pg_leftmost_one_pos64
        (pos / RT_SPAN) * RT_SPAN
    }
}

/// Maximum shift needed to extract a chunk from a key.
/// `#define RT_MAX_SHIFT RT_KEY_GET_SHIFT(UINT64_MAX)`.
#[inline]
pub fn RT_MAX_SHIFT() -> c_int {
    RT_KEY_GET_SHIFT(u64::MAX as uint64)
}

/// Return the max value that can be stored in the tree with the given shift.
/// `static uint64 RT_SHIFT_GET_MAX_VAL(int shift)`.
#[inline]
pub fn RT_SHIFT_GET_MAX_VAL(shift: c_int) -> uint64 {
    if shift == RT_MAX_SHIFT() {
        u64::MAX as uint64
    } else {
        ((1u64 << (shift + RT_SPAN)) - 1) as uint64
    }
}

// ---------------------------------------------------------------------------
// Templated body - documented, NOT emitted as concrete Rust
// ---------------------------------------------------------------------------

/// Documentation-only module describing the macro-parameterized portion of
/// `radixtree.h`. None of this has a standalone Rust form: every symbol name
/// is `CppConcat(RT_PREFIX_, name)` and the struct layouts depend on
/// `RT_VALUE_TYPE` and on the child-pointer type (`RT_NODE *` for local
/// memory, `dsa_pointer` for shared memory). The eventual Rust caller that
/// instantiates a concrete radix tree must re-emit these per its own
/// type/macro parameters.
///
/// PARAMETERS (caller `#define`s before include):
///   - `RT_PREFIX`                   symbol-name prefix -> `<prefix>_radix_tree`, etc.
///   - `RT_DECLARE`                  emit prototypes + type declarations
///   - `RT_DEFINE`                   emit function definitions
///   - `RT_SCOPE`                    visibility of declarations (extern / static inline)
///   - `RT_VALUE_TYPE`               the value type stored in the tree
///   - `RT_VARLEN_VALUE_SIZE(p)`     size expression for variable-length values
///   - `RT_RUNTIME_EMBEDDABLE_VALUE` allow tagging small varlen values into a slot
///   - `RT_SHMEM`                    place the tree in a DSA area (multi-process)
///   - `RT_USE_DELETE`              emit `RT_DELETE`
///   - `RT_DEBUG`                    emit stats / dump helpers
///
/// PARAMETERIZED CHILD-POINTER TYPE:
///   - local : `RT_PTR_ALLOC = RT_NODE *`, invalid = NULL
///   - shmem : `RT_PTR_ALLOC = dsa_pointer`, invalid = InvalidDsaPointer
///   `RT_CHILD_PTR` is a union (local) / struct (shmem) of { alloc, local }.
///
/// PARAMETERIZED TYPES (re-emitted per instantiation):
///   - `<prefix>_radix_tree`         entry point (ctl + slabs / dsa)
///   - `<prefix>_radix_tree_control` root, max_val, num_keys, start_shift, stats
///   - `<prefix>_iter` / `<prefix>_node_iter`   iteration state
///   - `<prefix>_handle`             dsa_pointer (RT_SHMEM only)
///   - `<prefix>_node_4 / _16 / _48 / _256`     the four node kinds, each
///     embedding `RT_NODE base`, chunk arrays / isset bitmaps / slot_idxs,
///     and a `RT_PTR_ALLOC children[]` flexible array sized by size class.
///   - `RT_SIZE_CLASS_INFO[]`        const table of `RT_SIZE_CLASS_ELEM`,
///     whose `.allocsize` uses `sizeof(RT_NODE_*)` (parameterized).
///
/// PUBLIC INTERFACE (re-emitted per instantiation, scope = RT_SCOPE):
///   - `RT_CREATE(MemoryContext ctx)`  / shmem: `(dsa_area*, int tranche_id)`
///   - `RT_FREE(tree)`
///   - `RT_FIND(tree, uint64 key) -> RT_VALUE_TYPE*`
///   - `RT_SET(tree, uint64 key, RT_VALUE_TYPE* value_p) -> bool`
///   - `RT_DELETE(tree, uint64 key) -> bool`           (RT_USE_DELETE)
///   - `RT_BEGIN_ITERATE(tree) -> RT_ITER*`
///   - `RT_ITERATE_NEXT(iter, uint64* key_p) -> RT_VALUE_TYPE*`
///   - `RT_END_ITERATE(iter)`
///   - `RT_MEMORY_USAGE(tree) -> uint64`
///   - shmem only: `RT_ATTACH`, `RT_DETACH`, `RT_GET_HANDLE`,
///     `RT_LOCK_EXCLUSIVE`, `RT_LOCK_SHARE`, `RT_UNLOCK`
///   - debug only: `RT_STATS(tree)`
///
/// INTERNAL HELPERS (no external prototype; re-emitted per instantiation):
///   value embedding: `RT_CHILDPTR_IS_VALUE`, `RT_VALUE_IS_EMBEDDABLE`,
///     `RT_GET_VALUE_SIZE`;
///   alloc/free: `RT_ALLOC_NODE`, `RT_ALLOC_LEAF`, `RT_FREE_NODE`,
///     `RT_FREE_LEAF`, `RT_FREE_RECURSE`, `RT_COPY_COMMON`, `RT_PTR_SET_LOCAL`;
///   search: `RT_NODE_SEARCH`, `RT_NODE_16_SEARCH_EQ` (SIMD via
///     port/simd Vector8), `RT_NODE_48_GET_CHILD`, `RT_NODE_256_GET_CHILD`,
///     `RT_NODE_48_IS_CHUNK_USED`, `RT_NODE_256_IS_CHUNK_USED`;
///   insert/grow: `RT_NODE_INSERT`, `RT_ADD_CHILD_{4,16,48,256}`,
///     `RT_GROW_NODE_{4,16,48}`, `RT_NODE_{4,16}_GET_INSERTPOS`,
///     `RT_SHIFT_ARRAYS_FOR_INSERT`, `RT_COPY_ARRAYS_FOR_INSERT`,
///     `RT_EXTEND_UP`, `RT_EXTEND_DOWN`, `RT_GET_SLOT_RECURSIVE`,
///     `RT_NODE_MUST_GROW`;
///   delete/shrink (RT_USE_DELETE): `RT_NODE_DELETE`,
///     `RT_REMOVE_CHILD_{4,16,48,256}`, `RT_SHRINK_NODE_{16,48,256}`,
///     `RT_SHIFT_ARRAYS_AND_DELETE`, `RT_COPY_ARRAYS_AND_DELETE`,
///     `RT_DELETE_RECURSIVE`;
///   iterate: `RT_NODE_ITERATE_NEXT`;
///   verify/debug: `RT_VERIFY_NODE`, `RT_DUMP_NODE`.
///
/// ALGORITHM NOTES (see C header for full detail):
///   - span of 8 (byte-addressable chunks), fanout 2^8 = 256 max.
///   - "combined pointer/value slots": values <= sizeof(pointer) are stored
///     directly in the last-level child slot; larger values are single-value
///     leaves. RT_CHILDPTR_IS_VALUE distinguishes them (size compare for
///     fixed-length; low-bit tag for RT_RUNTIME_EMBEDDABLE_VALUE).
///   - "poor man's path compression": the tree height (start_shift)
///     adapts to the key distribution; paths whose high bytes are all zero
///     are not materialized.
///   - adaptive node kinds grow (4 -> 16 -> 48 -> 256) on insertion when
///     full and shrink (256 -> 48 -> 16 -> 4) on deletion below a threshold
///     (~3/4 of the next-lower fanout) to avoid ping-ponging.
///   - concurrency (RT_SHMEM): a single LWLock guards the whole tree;
///     writers take it exclusive, readers shared.
pub mod radixtree_template {}

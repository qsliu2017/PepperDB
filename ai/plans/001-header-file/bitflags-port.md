# Port PostgreSQL bit flags to Rust `bitflags!`

Scope: `src/include/**` flag groups and their `.c` usage

## 1. Summary

PostgreSQL stores most of its boolean option sets as groups of `#define`d
single-bit constants combined with `|` and tested with `&`. When this code is
ported to Rust, those groups map directly onto the `bitflags!` macro, which
gives each set a distinct named type, type-safe combination, and free
`Debug`/iteration instead of a bare integer.

Most of `src/include`'s bitwise-flag groups are clean single-bit sets and port
directly; the rest either pack a number beside the flags or are really an
`enum`/ordinal, and are left in C. This RFC
defines the decision rule, shows conversions for the clean sets (3.1-3.3), and
then the kinds that are deliberately not ported (3.4-3.6).

[`bitflags`]: https://docs.rs/bitflags/latest/bitflags/

## 2. Which pattern should be rewritten this way

Use `bitflags!` when **all** of these hold:

1. The group is a set of related constants, each a distinct power of two
   (`1 << n`, or `0x01 / 0x02 / 0x04 ...`).
2. The value is built by OR-ing members together and read by AND-masking.
3. Every bit means an independent yes/no; bits are not mutually exclusive
   ordinals, and the word does not also carry a packed number.

Composite constants such as `MASK = A | B | C` are fine -- `bitflags!` expresses
them natively and they stay good candidates.

Do **not** use `bitflags!` when:

- The word **packs a number beside the flags** -- a count, length, fork, or
  offset (`BKPBLOCK_*`, `t_infomask2`, jsonb `JEntry`, `WordEntry`). The numeric
  part is not boolean membership, so the word is not a flag set. Leave it in C;
  this matters most for on-disk/WAL words, which must round-trip byte-for-byte.
- A **multi-bit one-of-N selector** shares the word (`GUC_UNIT_*`). Its codes
  are mutually exclusive, not OR-able, so the selector is an `enum`.
- The constants are **sequential ordinals / opcodes / tags** (`0, 1, 2, 3 ...`).
  That is a Rust `enum`.

A clean single-bit flag set in the same header still ports, even if it sits next
to one of these (only the non-flag sub-field is excluded). And a pure flag set
that happens to be on-disk is fine: `bitflags!` over the matching integer type
is byte-identical and round-trips exactly (see 3.2, 3.3).

### Pattern table

| Definition shape | Port to `bitflags!`? |
|------------------|----------------------|
| All members single-bit, OR/AND-combined (composite `MASK = A\|B\|C` allowed) | Yes -- 1:1 |
| Word also carries a packed number (count, length, fork, offset) | No -- not a flag set; leave in C, esp. on-disk/WAL |
| A multi-bit one-of-N selector packed beside flags | No -- the selector is an `enum` |
| Sequential ordinals / opcodes / tags (`0,1,2,3`) | No -- a Rust `enum` |
| Bitfield-packed accessor word (`JEntry`, `WordEntry`) | No -- struct with accessor methods |

## 3. Examples in PostgreSQL

### 3.1 GOOD -- privilege bits (`ACL_*`, `nodes/parsenodes.h`)

`AclMode` is `uint64`; 15 independent privilege bits.

```c
typedef uint64 AclMode;
#define ACL_INSERT      (1<<0)
#define ACL_SELECT      (1<<1)
#define ACL_UPDATE      (1<<2)
/* ... through ACL_MAINTAIN (1<<14) */
#define ACL_NO_RIGHTS   0
#define ACL_SELECT_FOR_UPDATE   ACL_UPDATE   /* alias */
```

```rust
use bitflags::bitflags;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AclMode: u64 {
        const INSERT    = 1 << 0;
        const SELECT    = 1 << 1;
        const UPDATE    = 1 << 2;
        const DELETE    = 1 << 3;
        const TRUNCATE  = 1 << 4;
        const REFERENCES = 1 << 5;
        const TRIGGER   = 1 << 6;
        const EXECUTE   = 1 << 7;
        const USAGE     = 1 << 8;
        const CREATE    = 1 << 9;
        const CREATE_TEMP = 1 << 10;
        const CONNECT   = 1 << 11;
        const SET       = 1 << 12;
        const ALTER_SYSTEM = 1 << 13;
        const MAINTAIN  = 1 << 14;
    }
}

// ACL_NO_RIGHTS and ACL_SELECT_FOR_UPDATE are C #defines, so define them too.
// Use associated consts (not fns): const-evaluable, no parens at the call site,
// and no non_snake_case lint. Keeping the alias out of the macro body avoids
// confusing Debug/iter() output and the zero-flag special-casing of `contains`.
impl AclMode {
    pub const NO_RIGHTS: Self = Self::empty();
    pub const SELECT_FOR_UPDATE: Self = Self::UPDATE;
}

// (acl & ACL_SELECT) -> acl.contains(AclMode::SELECT)
```

### 3.2 GOOD -- WAL record flags (`XLH_INSERT_*`, `access/heapam_xlog.h`)

Stored in a `uint8`; the header even notes "8 bits are available".

```c
#define XLH_INSERT_ALL_VISIBLE_CLEARED  (1<<0)
#define XLH_INSERT_LAST_IN_MULTI        (1<<1)
#define XLH_INSERT_IS_SPECULATIVE       (1<<2)
#define XLH_INSERT_CONTAINS_NEW_TUPLE   (1<<3)
#define XLH_INSERT_ON_TOAST_RELATION    (1<<4)
#define XLH_INSERT_ALL_FROZEN_SET       (1<<5)
```

```rust
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct XlhInsert: u8 {
        const ALL_VISIBLE_CLEARED = 1 << 0;
        const LAST_IN_MULTI       = 1 << 1;
        const IS_SPECULATIVE      = 1 << 2;
        const CONTAINS_NEW_TUPLE  = 1 << 3;
        const ON_TOAST_RELATION   = 1 << 4;
        const ALL_FROZEN_SET      = 1 << 5;
    }
}
```

The sibling `XLH_UPDATE_*`, `XLH_DELETE_*`, `XLHP_*`, `XLHL_*`, and
`XLH_LOCK_*` groups convert the same way.

### 3.3 GOOD + composite mask (`SMGR_TRUNCATE_*`, `catalog/storage_xlog.h`; `COMMAND_OK_IN_*`, `tcop/utility.h`)

The C composite is just an OR; `bitflags!` keeps it as a named constant.

```c
#define SMGR_TRUNCATE_HEAP  0x0001
#define SMGR_TRUNCATE_VM    0x0002
#define SMGR_TRUNCATE_FSM   0x0004
#define SMGR_TRUNCATE_ALL   (SMGR_TRUNCATE_HEAP|SMGR_TRUNCATE_VM|SMGR_TRUNCATE_FSM)
```

```rust
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SmgrTruncate: i32 {
        const HEAP = 0x0001;
        const VM   = 0x0002;
        const FSM  = 0x0004;
        const ALL  = Self::HEAP.bits() | Self::VM.bits() | Self::FSM.bits();
    }
}
```

`COMMAND_OK_IN_READ_ONLY_TXN / _PARALLEL_MODE / _RECOVERY` with
`COMMAND_IS_STRICTLY_READ_ONLY = (... | ... | ...)` is the same shape.

The following three are **not** ported. Each lists its C definition and why
`bitflags!` does not fit; the appendix tables B/C/D collect the rest.

### 3.4 Do not port -- flags packed next to a number (`BKPBLOCK_*`, `access/xlogrecord.h`)

```c
#define BKPBLOCK_FORK_MASK  0x0F      /* fork number 0..15 */
#define BKPBLOCK_HAS_IMAGE  0x10
#define BKPBLOCK_HAS_DATA   0x20
#define BKPBLOCK_WILL_INIT  0x40
#define BKPBLOCK_SAME_REL   0x80
```

The low nibble is a fork *number*, not boolean membership, so the word is not a
flag set. It is also the `fork_flags` byte written verbatim into WAL, so it must
round-trip byte-for-byte -- splitting it would discard the layout. Keep the raw
word. Same shape: `t_infomask`/`t_infomask2`, jsonb `JEntry`, tsvector
`WordEntry`.

### 3.5 Do not port -- a multi-bit selector beside flags (`GUC_*` unit field, `utils/guc.h`)

```c
#define GUC_UNIT_KB     0x01000000
#define GUC_UNIT_BLOCKS 0x02000000
#define GUC_UNIT_MB     0x04000000
#define GUC_UNIT_MEMORY 0x0F000000   /* mask */
#define GUC_UNIT_MS     0x10000000
#define GUC_UNIT_MIN    0x30000000
#define GUC_UNIT_TIME   0x70000000   /* mask */
```

The unit values are mutually exclusive codes packed into a 4-bit field, not
single bits: `GUC_UNIT_KB | GUC_UNIT_MB` is `0x05000000` = `GUC_UNIT_BYTE`, a
*different* unit, so OR-combining them is meaningless. `bitflags!` models set
membership, not a one-of-N choice; that sub-field is an `enum`. (The single-bit
`GUC_*` option flags in the low bits *are* a fine bitflags set -- they are
listed under GOOD; only the unit sub-field is excluded.)

### 3.6 Do not port -- an ordinal packed with flags (`TRIGGER_EVENT_*`, `commands/trigger.h`)

```c
#define TRIGGER_EVENT_INSERT     0x00000000
#define TRIGGER_EVENT_UPDATE     0x00000002
#define TRIGGER_EVENT_TRUNCATE   0x00000003
#define TRIGGER_EVENT_OPMASK     0x00000003   /* 2-bit field */
#define TRIGGER_EVENT_ROW        0x00000004
#define TRIGGER_EVENT_BEFORE     0x00000008
#define TRIGGER_EVENT_TIMINGMASK 0x00000018
```

Bits 0-1 are an INSERT/DELETE/UPDATE/TRUNCATE ordinal and bits 3-4 a timing
value; the header itself notes these "can't be OR'd together". Most of the word
is enum-like, not flags. Same shape: WAL opcodes (`XLOG_GIN_*`, `XLOG_SMGR_*`,
`CLOG_*`), sequential tags (`ReorderBufferChangeType`, `RT_NODE_KIND_*`,
`NodeTag`), and bitfield-packed `JGINFLAG_*` / `GBUF_*`.

## 4. Conversion notes

- Pick the backing integer to match the C field width (`bits8` -> `u8`,
  `bits16`/`int16` -> `u16`, `int`/`bits32` -> `u32`/`i32`, `AclMode` -> `u64`).
- `FLAG_NONE`/`*_NO_RIGHTS`/`*_EMPTY` (value 0) become `T::empty()`.
- `(x & F)` becomes `x.contains(F)`; `x |= F` becomes `x.insert(F)`;
  `x &= ~F` becomes `x.remove(F)`.
- `bitflags!` does **not** enforce semantic invariants. Where C comments say
  "exactly one of these" (e.g. `SO_TYPE_*` in `access/tableam.h`, the page-type
  bits in `access/hash.h`), keep that check in code or split the type bits into
  an `enum`.
- For WAL/on-disk values, the backing type and bit values must stay
  byte-compatible; add a round-trip test against the C constants.

## Appendix: full catalog

Every bitwise-flag group found under `src/include`, with its header, grouped by
verdict. "Group" is the constant prefix unless noted.

### A. GOOD -- direct `bitflags!` (single-bit sets; composites noted)

| Group | Header |
|-------|--------|
| `BRIN_EVACUATE_PAGE` | `access/brin_page.h` |
| `GIN_*` page flags | `access/ginblock.h` |
| `F_*` GiST page flags | `access/gist.h` |
| `XLH_SPLIT_*` | `access/hash_xlog.h` |
| `HEAP_PAGE_PRUNE_*` | `access/heapam.h` |
| `XLH_INSERT_*`, `XLH_UPDATE_*`, `XLH_DELETE_*`, `XLHP_*`, `XLH_FREEZE_*`, `XLHL_*`, `XLH_LOCK_*` | `access/heapam_xlog.h` |
| `BTP_*` page flags | `access/nbtree.h` |
| `SK_BT_*` scan-key flags | `access/nbtree.h` |
| `SPGIST_*` page flags | `access/spgist_private.h` |
| `SK_*` scan-key flags | `access/skey.h` |
| `TOAST_*`, `TOASTCOL_*` | `access/toast_helper.h` |
| `CHECKPOINT_*` | `access/xlog.h` |
| `XLOG_INCLUDE_ORIGIN`, `XLOG_MARK_UNIMPORTANT` | `access/xlog.h` |
| `XLP_*` (composite `XLP_ALL_FLAGS`) | `access/xlog_internal.h` |
| `BKPIMAGE_*` | `access/xlogrecord.h` |
| `GENERIC_XLOG_FULL_IMAGE` | `access/generic_xlog.h` |
| `GIN_INSERT_*`, `GIN_SPLIT_ROOT` | `access/ginxlog.h` |
| `TABLE_INSERT_*` | `access/tableam.h` |
| `XACT_FLAGS_*` | `access/xact.h` |
| `SMGR_TRUNCATE_*` (composite `_ALL`) | `catalog/storage_xlog.h` |
| `PD_*` page flags (composite `PD_VALID_FLAG_BITS`) | `storage/bufpage.h` |
| `IFS_*` | `storage/large_object.h` |
| `EB_*` ExtendBufferedFlags | `storage/bufmgr.h` |
| `IO_DIRECT_*` | `storage/fd.h` |
| `PROC_*` statusFlags (composite masks), `DELAY_CHKPT_*` | `storage/proc.h` |
| `WL_*` wait events (composite `WL_SOCKET_MASK`) | `storage/waiteventset.h` |
| `DSM_CREATE_NULL_IF_MAXSEGMENTS` | `storage/dsm.h` |
| `READ_STREAM_*` | `storage/read_stream.h` |
| `UNLOGGED_RELATION_*` | `storage/reinit.h` |
| `SXACT_FLAG_*` | `storage/predicate_internals.h` |
| `SYNC_STANDBY_*` | `replication/walsender_private.h` |
| `RBTXN_*` | `replication/reorderbuffer.h` |
| `EXEC_FLAG_*` | `executor/executor.h` |
| `EEO_FLAG_*` | `nodes/execnodes.h`, `executor/execExpr.h` |
| `TTS_FLAG_*` | `executor/tuptable.h` |
| `PARAM_FLAG_*` | `nodes/params.h` |
| `ACL_*` (composite alias `ACL_SELECT_FOR_UPDATE`) | `nodes/parsenodes.h` |
| `FRAMEOPTION_*` | `nodes/parsenodes.h` |
| `CURSOR_OPT_*` | `nodes/parsenodes.h` |
| `PVC_*` | `optimizer/optimizer.h` |
| `GROUPING_CAN_*` | `nodes/pathnodes.h` |
| `AMFLAG_*` | `nodes/pathnodes.h` |
| `CUSTOMPATH_SUPPORT_*` | `nodes/extensible.h` |
| `MAT_SRF_*` | `funcapi.h` |
| `REINDEXOPT_*`, `REINDEX_REL_*`, `INDEX_CREATE_*`, `INDEX_CONSTR_CREATE_*` | `catalog/index.h` |
| `PERFORM_DELETION_*` | `catalog/dependency.h` |
| `CHKATYPE_*` | `catalog/heap.h` |
| `INDOPTION_*` | `catalog/pg_index.h` |
| `VACOPT_*` | `commands/vacuum.h` |
| `CLUOPT_*` | `commands/cluster.h` |
| `AT_REWRITE_*` | `commands/event_trigger.h` |
| `RESTRICT_RELKIND_*` | `tcop/tcopprot.h` |
| `COMMAND_OK_IN_*` (composite `COMMAND_IS_STRICTLY_READ_ONLY`) | `tcop/utility.h` |
| `BGWORKER_*`, `BGWORKER_BYPASS_*` | `postmaster/bgworker.h` |
| `PIPE_PROTO_*` | `postmaster/syslogger.h` |
| `FSV_MISSING_OK`, `FDW_MISSING_OK` | `foreign/foreign.h` |
| `SECURITY_*`, `INIT_PG_*` | `miscadmin.h` |
| `GUC_*` option bits (not the unit field) | `utils/guc.h` |
| `HASH_*` | `utils/hsearch.h` |
| `JSP_REGEX_*` | `utils/jsonpath.h` |
| `RANGE_*` | `utils/rangetypes.h` |
| `MCXT_ALLOC_*` | `utils/palloc.h`, `common/fe_memutils.h` |
| `FORMAT_TYPE_*` | `utils/builtins.h` |
| `DSA_ALLOC_*` | `utils/dsa.h` |
| `ATTSTATSSLOT_*` | `utils/lsyscache.h` |
| `RULE_INDEXDEF_*` | `utils/ruleutils.h` |
| `SHARED_TUPLESTORE_SINGLE_PASS` | `utils/sharedtuplestore.h` |
| `TYPECACHE_*` | `utils/typcache.h` |
| `FORMAT_PROC_*`, `FORMAT_OPERATOR_*` | `utils/regproc.h` |
| `PG_COMPRESSION_OPTION_*` | `common/compression.h` |
| `JSONLEX_*` | `common/jsonapi.h` |
| `PGJIT_*` | `jit/jit.h` |
| `TSL_*` | `tsearch/ts_public.h` |
| `P_TSV_*`, `P_TSQ_*`, `TS_EXEC_*`, `QTN_*` | `tsearch/ts_utils.h` |
| `FF_COMPOUND*` and affix flags (composite `FF_COMPOUNDFLAG`) | `tsearch/dicts/spell.h` |
| `PG_U_PROP_*` | `common/unicode_category_table.h` |
| `PG_U_FINAL_SIGMA` | `common/unicode_case_table.h` |
| `FLUSH_FLAGS_*` | `port/win32ntdll.h` |

### B. PARTIAL (in-memory) -- `bitflags!` + an `enum`/field, or composite-mask semantics

| Group | Header | Note |
|-------|--------|------|
| `GUC_UNIT_*` | `utils/guc.h` | 4-bit unit selector beside the flag bits -> flags + `enum` |
| `SO_*` ScanOptions | `access/tableam.h` | `SO_TYPE_*` mutually exclusive + `SO_ALLOW_*` flags |
| `LH_*` hash page | `access/hash.h` | bits 0-3 page-type code + flag bits |
| `TRIGGER_TYPE_*` | `catalog/pg_trigger.h` | single-bit flags + `LEVEL`/`TIMING`/`EVENT` masks |
| `VISIBILITYMAP_*` | `access/visibilitymapdefs.h` | 2 flags + `VALID_BITS` validation masks |
| `REGBUF_*` | `access/xloginsert.h` | composite `REGBUF_WILL_INIT = 0x06` |
| `QTW_*` | `nodes/nodeFuncs.h` | composite `QTW_IGNORE_RC_SUBQUERIES = 0x03` |
| `ER_FLAG_*` | `utils/expandedrecord.h` | composite `ER_FLAGS_NON_DATA` |
| `PGSTAT_BACKEND_FLUSH_*` | `utils/pgstat_internal.h` | composite `_ALL` |
| `VACUUM_OPTION_*` (parallel) | `commands/vacuum.h` | composite `MAX_VALID_VALUE` |

### C. Out of scope -- on-disk/WAL words packing flags next to a number (keep raw)

| Group | Header |
|-------|--------|
| `BKPBLOCK_*` (fork number + flags) | `access/xlogrecord.h` |
| `XLR_INFO` / `xl_info` (flags + rmgr info nibble) | `access/xlogrecord.h` |
| `t_infomask`, `t_infomask2` / `HEAP_NATTS_MASK` (natts count + flags) | `access/htup_details.h` |
| `BT_OFFSET_MASK` / `BT_STATUS_OFFSET_MASK` (offset + 2 status bits) | `access/nbtree.h` |
| `Jsonb` container header / `JENTRY_*` / `JB_*` (count + type + flag) | `utils/jsonb.h` |
| `WordEntry` / `WordEntryPos` (len/pos/weight bitfields) | `tsearch/ts_type.h` |
| `HeadlineWordEntry` (flags + type/len bitfields) | `tsearch/ts_public.h` |

### D. POOR -- not flag sets (use `#[repr(uN)] enum`, or a bitfield-struct with accessors)

| Group | Header | Kind |
|-------|--------|------|
| `TRANSACTION_STATUS_*` | `access/clog.h` | sequential ordinal (0-3) |
| `CLOG_*` record types | `access/clog.h` | WAL opcode (xl_info nibble) |
| `XLOG_GIN_*`, `GIN_SEGMENT_*` | `access/ginxlog.h` | WAL opcode / sequential ordinal |
| `XLOG_SMGR_*` | `catalog/storage_xlog.h` | WAL opcode |
| `ReorderBufferChangeType` | `replication/reorderbuffer.h` | sequential ordinal (0-11) |
| `RT_NODE_KIND_*` | `lib/radixtree.h` | sequential ordinal |
| `NodeTag` | `nodes/nodes.h` | sequential ordinal |
| `TRIGGER_EVENT_*` | `commands/trigger.h` | 2-bit op ordinal + timing + `ROW` flag |
| `GBUF_*` | `access/spgist_private.h` | 2-bit parity field + `NULLS` flag |
| `JGINFLAG_*` | `utils/jsonb.h` | type ordinals (0x01-0x05) + one flag |
| `FF_SUFFIX` / `FF_PREFIX` | `tsearch/dicts/spell.h` | 0/1 type ordinal |

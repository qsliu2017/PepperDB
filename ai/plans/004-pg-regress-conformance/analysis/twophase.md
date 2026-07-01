# Plan 004 scoping: Two-Phase Commit (2PC)

Reference doc for implementing PREPARE TRANSACTION / COMMIT PREPARED / ROLLBACK
PREPARED in PepperDB. PG source: `ref/postgres` (REL_18_4). PepperDB source: `src/`.

---

## 1. PG source summary

### 1.1 Core files

| File | Lines | Role |
|---|---|---|
| `src/backend/access/transam/twophase.c` | 2753 | gxact lifecycle, state file I/O, WAL PREPARE, recovery |
| `src/backend/access/transam/twophase_rmgr.c` | 59 | rmgr callback dispatch tables |
| `src/include/access/twophase.h` | 71 | public API |
| `src/include/access/twophase_rmgr.h` | 41 | `TwoPhaseRmgrId`, callback typedef |
| `src/backend/access/transam/xact.c` | (prepare paths) | `PrepareTransaction`, `PrepareTransactionBlock`, `xact_redo` |
| `src/include/access/xact.h` | (WAL structs) | `GIDSIZE`, `xl_xact_prepare`, `xl_xact_twophase`, parsed-commit/abort twophase fields |
| `src/backend/tcop/utility.c` | dispatch | `TRANS_STMT_PREPARE/COMMIT_PREPARED/ROLLBACK_PREPARED` |
| `src/backend/catalog/system_views.sql:417-422` | view | `pg_prepared_xacts` |
| `src/include/catalog/pg_proc.dat:6560-6565` | catalog fn | `pg_prepared_xact` SRF (oid 1065) |
| `src/backend/utils/misc/guc_tables.c:2706-2713` | GUC | `max_prepared_transactions` |

### 1.2 `GlobalTransactionData` / gxact (twophase.c:147-186)

```c
typedef struct GlobalTransactionData
{
    GlobalTransaction next;         /* free list link */
    int         pgprocno;           /* ID of associated dummy PGPROC */
    TimestampTz prepared_at;
    XLogRecPtr  prepare_start_lsn;  /* start of PREPARE WAL record */
    XLogRecPtr  prepare_end_lsn;    /* end of PREPARE WAL record (wait target) */
    TransactionId xid;
    Oid         owner;
    ProcNumber  locking_backend;    /* backend currently finishing this xact */
    bool        valid;              /* PGPROC entry live in ProcArray */
    bool        ondisk;             /* state file exists on disk */
    bool        inredo;             /* added via xlog redo */
    char        gid[GIDSIZE];       /* GIDSIZE = 200, from xact.h:31 */
} GlobalTransactionData;
```

Shared state (twophase.c:176-188), protected by `TwoPhaseStateLock`:

```c
typedef struct TwoPhaseStateData
{
    GlobalTransaction freeGXacts;    /* free list head */
    int         numPrepXacts;
    GlobalTransaction prepXacts[FLEXIBLE_ARRAY_MEMBER];  /* max_prepared_xacts slots */
} TwoPhaseStateData;
```

Sizing (`TwoPhaseShmemSize`, twophase.c:236-250): fixed struct + `max_prepared_xacts *
sizeof(pointer)` + `max_prepared_xacts * sizeof(GlobalTransactionData)`. Each gxact
gets a **dummy PGPROC** carved out of the PGPROC array (`PreparedXactProcs`, see
1.7) so `TransactionIdIsInProgress` and the lock manager see it as a normal backend.

### 1.3 `PrepareTransaction()` (xact.c:2514-2789, ~275 lines) vs `CommitTransaction()`

Comment at 2510-2513: "NB: if you change this routine, better look at
CommitTransaction too!" -- the two are structurally parallel but diverge sharply:

**Same as commit:** fire deferred triggers, close portals, `AfterTriggerEndXact`,
on-commit actions, `smgrDoPendingSyncs`, close large objects, SSI
`PreCommit_CheckForSerializationFailure`.

**Restrictions unique to prepare** (2613-2626, checked after all pre-commit
hooks so ON COMMIT actions get a chance to run first):
```c
if ((MyXactFlags & XACT_FLAGS_ACCESSEDTEMPNAMESPACE))
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("cannot PREPARE a transaction that has operated on temporary objects")));
...
if (XactHasExportedSnapshots())
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("cannot PREPARE a transaction that has exported snapshots")));
```
(A third real-world restriction, session-level advisory locks conflicting with
xact-level locks on the same object, is enforced inside `AtPrepare_Locks` ->
lock.c, not via an errmsg in xact.c; see regress test `regress_foo6`.)

**Sequence unique to prepare** (2635-2757):
1. `s->state = TRANS_PREPARE` (not `TRANS_COMMIT`).
2. `gxact = MarkAsPreparing(xid, prepareGID, prepared_at, GetUserId(), MyDatabaseId)`
   -- reserves the GID, checks for duplicates, allocates a dummy PGPROC.
3. `StartPrepare(gxact)` -- builds the in-memory state-file header + subxact/rel/inval
   arrays via the *same* helpers as commit (`xactGetCommittedChildren`,
   `smgrGetPendingDeletes`, `xactGetCommittedInvalidationMessages`,
   `pgstat_get_transactional_drops`) but buffers them instead of writing to WAL directly.
4. Resource-manager `AtPrepare_*` callbacks, **in this exact order** (2669-2674):
   `AtPrepare_Notify()`, `AtPrepare_Locks()`, `AtPrepare_PredicateLocks()`,
   `AtPrepare_PgStat()`, `AtPrepare_MultiXact()`, `AtPrepare_RelationMap()`.
   Order matters because replay order at commit/abort must match.
5. `EndPrepare(gxact)` -- writes **one** `XLOG_XACT_PREPARE` WAL record containing the
   whole state-file image, flushes it, calls `MarkAsPrepared` (adds dummy PGPROC to
   ProcArray while the real backend still shows the xid as running -- intentional
   double-entry window), clears `DELAY_CHKPT_START`.
6. `PostPrepare_Locks(xid)` -- **transfers** (not releases) locks from the real
   PGPROC to the dummy PGPROC.
7. `ProcArrayClearTransaction(MyProc)` -- backend stops showing the xid as running
   (must happen after step 6's transfer, per comment at 2699-2704).
8. Backend-local cleanup mirrors commit (`AtEOXact_Buffers`, `RelationCache`,
   `TypeCache`, etc.) but calls `PostPrepare_PgStat`, `PostPrepare_Inval`,
   `PostPrepare_smgr`, `PostPrepare_MultiXact(xid)`, `PostPrepare_PredicateLocks(xid)`
   instead of the `AtEOXact_*` commit equivalents for those subsystems.
9. `PostPrepare_Twophase()` -- unlocks the gxact so another backend can finish it.

**What prepare explicitly does NOT do that commit does:** release locks (transfers
instead), run `AtEOXact_PgStat` (deferred to `FinishPreparedTransaction`), truncate
CLOG/mark the xid committed (that's COMMIT PREPARED's job), do file deletions from
`pendingDeletes` (deferred), do the post-commit resource-owner-release-and-forget
(resources are transferred to the dummy proc instead).

### 1.4 `PrepareTransactionBlock()` (xact.c:3992-4029, 38 lines)

Pure block-state transition, no actual work:
```c
bool PrepareTransactionBlock(const char *gid)
{
    result = EndTransactionBlock(false);
    if (result) {
        ...
        if (s->blockState == TBLOCK_END) {
            prepareGID = MemoryContextStrdup(TopTransactionContext, gid);
            s->blockState = TBLOCK_PREPARE;
        } else { result = false; }
    }
    return result;
}
```
`TBLOCK_PREPARE` (xact.c:172) is consumed by `CommitTransactionCommand()`'s
switch (xact.c:3291, 3352) which calls the static `PrepareTransaction()` for that
state -- the same dispatch shape as `TBLOCK_END` -> `CommitTransaction()`.

### 1.5 WAL record structs (`src/include/access/xact.h`)

```c
#define GIDSIZE 200                                    /* xact.h:31 */

typedef struct xl_xact_twophase {                       /* xact.h:309-312 */
    TransactionId xid;
} xl_xact_twophase;

typedef struct xl_xact_prepare {                         /* xact.h:352-370 */
    uint32      magic;             /* TWOPHASE_MAGIC = 0x57F94534 */
    uint32      total_len;
    TransactionId xid;
    Oid         database;
    TimestampTz prepared_at;
    Oid         owner;
    int32       nsubxacts, ncommitrels, nabortrels, ncommitstats, nabortstats, ninvalmsgs;
    bool        initfileinval;
    uint16      gidlen;             /* GID bytes follow this header */
    XLogRecPtr  origin_lsn;
    TimestampTz origin_timestamp;
} xl_xact_prepare;
```
`xl_xact_commit`/`xl_xact_abort` (xact.h:320-350) are minimal (`{xact_time}`); the
2PC-related payload (`xl_xact_twophase` xid + null-terminated `twophase_gid`) is an
*optional trailer* gated by `XINFO_HAS_TWOPHASE`/`XINFO_HAS_GID` xinfo bits -- i.e.
`XLOG_XACT_COMMIT_PREPARED`/`ABORT_PREPARED` reuse the **same** commit/abort record
shape as a normal commit/abort, just with the twophase trailer populated and a
different `info` opcode. There is no separate `xl_xact_commit_prepared` struct.

`xl_xact_parsed_commit`/`xl_xact_parsed_abort` (xact.h:377-432) are the
deconstructed in-memory forms produced by `ParseCommitRecord`/`ParseAbortRecord`;
both carry `twophase_xid` + `twophase_gid[GIDSIZE]`, and parsed-commit additionally
carries `nabortrels`/`abortlocators`/`nabortstats`/`abortstats` (only meaningful for
2PC commit, since a prepared xact tracks both a commit-time and an abort-time
delete-file list). `xl_xact_parsed_prepare` is a typedef alias of
`xl_xact_parsed_commit` (xact.h:408) -- the 2PC state file header (`TwoPhaseFileHeader`)
is literally `xl_xact_prepare` (twophase.c:976: `typedef xl_xact_prepare
TwoPhaseFileHeader;`).

### 1.6 2PC state-file format (twophase.c:955-989)

```
1. TwoPhaseFileHeader (== xl_xact_prepare)
2. TransactionId[]              (subtransactions)
3. RelFileLocator[]              (files to delete at commit)
4. RelFileLocator[]              (files to delete at abort)
5. xl_xact_stats_item[]          (commit stats drops)
6. xl_xact_stats_item[]          (abort stats drops)
7. SharedInvalidationMessage[]   (inval messages at commit)
8. TwoPhaseRecordOnDisk + rmgr data   (repeated, one per RegisterTwoPhaseRecord call)
9. TwoPhaseRecordOnDisk (rmid == TWOPHASE_RM_END_ID)  -- end sentinel
10. pg_crc32c checksum
```
Each segment MAXALIGN'd except the final CRC. Record header (twophase.c:984-989):
```c
typedef struct TwoPhaseRecordOnDisk {
    uint32 len;              /* rmgr data length, excludes this header */
    TwoPhaseRmgrId rmid;
    uint16 info;
} TwoPhaseRecordOnDisk;
```
Magic: `#define TWOPHASE_MAGIC 0x57F94534` (twophase.c:974). File path:
`pg_twophase/<16-hex-digit-fullxid>` (`TwoPhaseFilePath`, twophase.c:945-953).

**Critical design point (comment block twophase.c:12-69):** the state data is
written to **WAL only** at PREPARE time (`EndPrepare` -> single `XLOG_XACT_PREPARE`
record, gxact remembers `prepare_start_lsn`/`prepare_end_lsn`, `ondisk=false`). The
on-disk `pg_twophase/<xid>` file is created **lazily**, only by
`CheckPointTwoPhase()` (twophase.c:1806-1878) for any gxact whose
`prepare_end_lsn <= redo_horizon` (i.e. the prepare predates the checkpoint) --
this is what lets `COMMIT PREPARED`/`ROLLBACK PREPARED` re-read state directly from
WAL (`XlogReadTwoPhaseData`, twophase.c:1404-1453) in the (expected) common case of a
short-lived prepared xact, without ever touching disk. Comment: "with typical
checkpoint settings this will be about 3 minutes... we expect there will be no
GXACTs that need to be copied to disk." `FinishPreparedTransaction` (twophase.c:1520-1523)
branches on `gxact->ondisk` to decide WAL-vs-file read.

On crash recovery, `restoreTwoPhaseData()` (twophase.c:1889-1919) scans
`pg_twophase/` once at the start of recovery and re-populates `TwoPhaseState`
(`gxact->ondisk=true`, `gxact->inredo=true`); WAL replay of `XLOG_XACT_PREPARE`
records adds any *not yet checkpointed* gxacts via `PrepareRedoAdd` (in-memory
only, `ondisk=false`). So after recovery, in-doubt prepared xacts are known
regardless of whether their state predates or postdates the last checkpoint.

### 1.7 GUC + shared memory sizing

```c
/* guc_tables.c:2706-2713 */
{ {"max_prepared_transactions", PGC_POSTMASTER, RESOURCES_MEM,
   gettext_noop("Sets the maximum number of simultaneously prepared transactions."), NULL},
  &max_prepared_xacts, 0, 0, MAX_BACKENDS, NULL, NULL, NULL },
```
Default **0** (2PC disabled unless explicitly configured), `PGC_POSTMASTER`
(requires restart), max = `MAX_BACKENDS`.

Shmem sizing: `ipci.c:127: size = add_size(size, TwoPhaseShmemSize());` inside
`CreateSharedMemoryAndSemaphores`.

PGPROC pool sizing (`proc.c`):
```c
uint32 TotalProcs = MaxBackends + NUM_AUXILIARY_PROCS + max_prepared_xacts;  /* proc.c:198 */
...
PreparedXactProcs = &procs[FIRST_PREPARED_XACT_PROC_NUMBER];                 /* proc.c:377 */
```
Prepared xacts get **dedicated dummy PGPROC slots** appended after all real
backend + aux-process slots; `TwoPhaseShmemInit` (twophase.c:252-288) associates
each free gxact with one: `gxacts[i].pgprocno = GetNumberFromPGProc(&PreparedXactProcs[i])`.

### 1.8 Resource-manager callback pattern (`twophase_rmgr.h`, `twophase_rmgr.c`)

```c
typedef void (*TwoPhaseCallback)(TransactionId xid, uint16 info, void *recdata, uint32 len);
typedef uint8 TwoPhaseRmgrId;
#define TWOPHASE_RM_END_ID          0
#define TWOPHASE_RM_LOCK_ID         1
#define TWOPHASE_RM_PGSTAT_ID       2
#define TWOPHASE_RM_MULTIXACT_ID    3
#define TWOPHASE_RM_PREDICATELOCK_ID 4
#define TWOPHASE_RM_MAX_ID          TWOPHASE_RM_PREDICATELOCK_ID
```

Four parallel dispatch tables (twophase_rmgr.c:24-58), indexed by rmid:

| rmid | recover (at end of crash recovery) | postcommit | postabort | standby_recover |
|---|---|---|---|---|
| 0 END | NULL | NULL | NULL | NULL |
| 1 LOCK | `lock_twophase_recover` (lock.c:4327) | `lock_twophase_postcommit` (lock.c:4540) | `lock_twophase_postabort` (lock.c:4566) | `lock_twophase_standby_recover` (lock.c:4508) |
| 2 PGSTAT | NULL | `pgstat_twophase_postcommit` (pgstat_relation.c:747) | `pgstat_twophase_postabort` (pgstat_relation.c:783) | NULL |
| 3 MULTIXACT | `multixact_twophase_recover` (multixact.c:2049) | `multixact_twophase_postcommit` (multixact.c:2070) | `multixact_twophase_postabort` (multixact.c:2085) | NULL |
| 4 PREDICATELOCK | `predicatelock_twophase_recover` (predicate.c:4909) | NULL | NULL | NULL |

`AtPrepare_*` / `PostPrepare_*` counterparts (called directly from
`PrepareTransaction`, not through the rmid table): `AtPrepare_Locks`
(lock.c:3446), `PostPrepare_Locks` (lock.c:3542), `AtPrepare_MultiXact`
(multixact.c:1986), `PostPrepare_MultiXact` (multixact.c:2000),
`AtPrepare_PredicateLocks` (predicate.c:4790), `PostPrepare_PredicateLocks`
(predicate.c:4859), `PredicateLockTwoPhaseFinish` (predicate.c:4882),
`AtPrepare_PgStat`/`PostPrepare_PgStat` (pgstat_xact.c:191/211),
`AtPrepare_Notify` (async.c, no postcommit/postabort -- notify replay happens via
the commit record's own path), `AtPrepare_RelationMap` (relmapper.c, no rmgr
callback -- purely file-based).

Each subsystem calls `RegisterTwoPhaseRecord(rmid, info, data, len)`
(twophase.c:1264-1276) during its `AtPrepare_*` to append a
`TwoPhaseRecordOnDisk` + payload into the in-memory chunk chain that
`EndPrepare` later flushes as the WAL record body / state file body.

`ProcessRecords()` (twophase.c:1680-1699) replays the buffer through the right
callback table by scanning `TwoPhaseRecordOnDisk` headers until `rmid ==
TWOPHASE_RM_END_ID`.

### 1.9 `FinishPreparedTransaction()` (twophase.c:1487-1675, ~190 lines)

Used for both COMMIT PREPARED (`isCommit=true`) and ROLLBACK PREPARED
(`isCommit=false`), called from `utility.c:653`/`658`.

Order (matters, see inline comment 1552-1559): `LockGXact` (validates GID,
ownership, same-database, not already locked) -> read state (WAL or file per
`gxact->ondisk`) -> **write COMMIT_PREPARED/ABORT_PREPARED WAL record**
(`RecordTransactionCommitPrepared`/`RecordTransactionAbortPrepared`, twophase.c:2296-2459,
these mirror `RecordTransactionCommit`/`Abort` but always flush since there's no
optimizing-out-the-commit-record shortcut) -> mark clog -> `ProcArrayRemove` (dummy
PGPROC exits the array) -> `gxact->valid = false` -> physically delete
commit/abort-listed relfiles -> stats drops -> cache invalidation sends -> **run
postcommit/postabort rmgr callbacks** (`ProcessRecords` against
`twophase_postcommit_callbacks`/`twophase_postabort_callbacks`, this is what
releases the locks the prepared xact held) -> `PredicateLockTwoPhaseFinish` ->
`RemoveGXact` (frees the gxact struct + dummy PGPROC slot back to freelist) ->
`AtEOXact_PgStat(isCommit, false)` -> physically unlink `pg_twophase/<xid>` file
if `ondisk`.

### 1.10 WAL redo (`xact_redo`, xact.c:6362-6446)

```c
else if (info == XLOG_XACT_COMMIT_PREPARED) {
    ParseCommitRecord(...); xact_redo_commit(&parsed, parsed.twophase_xid, ...);
    LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
    PrepareRedoRemove(parsed.twophase_xid, false);
    LWLockRelease(TwoPhaseStateLock);
}
else if (info == XLOG_XACT_ABORT_PREPARED) { /* symmetric, xact_redo_abort + PrepareRedoRemove */ }
else if (info == XLOG_XACT_PREPARE) {
    LWLockAcquire(TwoPhaseStateLock, LW_EXCLUSIVE);
    PrepareRedoAdd(XLogRecGetData(record), record->ReadRecPtr, record->EndRecPtr, XLogRecGetOrigin(record));
    LWLockRelease(TwoPhaseStateLock);
}
```
So COMMIT_PREPARED/ABORT_PREPARED redo does the **same clog update** as a normal
commit/abort (using the twophase_xid, not the WAL record's own xid, since the
record was written by the finishing backend not the original preparer) **plus**
removes the gxact/state-file. PREPARE redo just re-adds the gxact to
`TwoPhaseState` (in-memory tracking, not yet committed/aborted) via
`PrepareRedoAdd` (twophase.c:2469-2560).

### 1.11 Crash-recovery driver functions

| Function | File:lines | When | Purpose |
|---|---|---|---|
| `restoreTwoPhaseData` | twophase.c:1888-1919 | start of recovery | scan `pg_twophase/` dir, prime `TwoPhaseState` from on-disk files |
| `PrescanPreparedTransactions` | twophase.c:1952-2017 | after WAL replay, before allowing new WAL | find oldest valid prepared-xact XID (for pg_subtrans sizing), discard xacts newer than nextXid (stale PITR target), advance nextXid past subxact XIDs |
| `StandbyRecoverPreparedTransactions` | twophase.c:2032-2055 | hot standby, each replay cycle | make prepared xacts visible to standby snapshots via pg_subtrans, without acquiring locks |
| `RecoverPreparedTransactions` | twophase.c:2073-2163 | end of recovery, before accepting writes | full reconstruction: recreate gxact + dummy PGPROC (`MarkAsPreparingGuts`), reload subxids, **replay rmgr `_recover` callbacks** (`ProcessRecords(..., twophase_recover_callbacks)`) to reacquire locks etc, mark valid, `PostPrepare_Twophase()` |
| `ProcessTwoPhaseBuffer` | twophase.c:2176-2283 | helper for the above three | read state (disk or WAL), reject already-committed/aborted or too-new XIDs, optionally set subxact parent links / advance nextXid |

This is precisely how in-doubt prepared xacts survive a crash without being
treated as aborted: they are never given a COMMIT/ABORT WAL record, so ordinary
crash recovery (which marks all other in-progress xids aborted once redo reaches
the end of WAL) must special-case them via `RecoverPreparedTransactions` running
*after* the main redo loop finishes, restoring their lock state before the server
opens for business, so a later `COMMIT PREPARED`/`ROLLBACK PREPARED` from an
external transaction manager can still find and finish them correctly.

### 1.12 Grammar / AST

`src/backend/parser/gram.y` (rule names confirmed via PG grammar structure,
`TransactionStmt` production family) produces `TransactionStmt` nodes
(`src/include/nodes/parsenodes.h`) with:
```c
typedef enum TransactionStmtKind {
    TRANS_STMT_BEGIN, TRANS_STMT_START, TRANS_STMT_COMMIT, TRANS_STMT_ROLLBACK,
    TRANS_STMT_SAVEPOINT, TRANS_STMT_RELEASE, TRANS_STMT_ROLLBACK_TO,
    TRANS_STMT_PREPARE, TRANS_STMT_COMMIT_PREPARED, TRANS_STMT_ROLLBACK_PREPARED,
    TRANS_STMT_BEGIN_ISOLATION_LEVEL /* not real, illustrative */
} TransactionStmtKind;

typedef struct TransactionStmt {
    NodeTag type;
    TransactionStmtKind kind;
    List *options;
    char *savepoint_name;
    char *gid;          /* GID for two-phase-commit */
    bool chain;
    ...
} TransactionStmt;
```
`PREPARE TRANSACTION '<gid>'`, `COMMIT PREPARED '<gid>'`, `ROLLBACK PREPARED
'<gid>'` all parse the trailing string literal into `stmt->gid`.

### 1.13 `pg_prepared_xacts` view + function

```sql
-- system_views.sql:417-422
CREATE VIEW pg_prepared_xacts AS
    SELECT P.transaction, P.gid, P.prepared, U.rolname AS owner, D.datname AS database
    FROM pg_prepared_xact() AS P
         LEFT JOIN pg_authid U ON P.ownerid = U.oid
         LEFT JOIN pg_database D ON P.dbid = D.oid;
```
```
-- pg_proc.dat:6560-6565, oid 1065
{ oid => '1065', descr => 'view two-phase transactions',
  proname => 'pg_prepared_xact', prorows => '1000', proretset => 't',
  provolatile => 'v', prorettype => 'record', proargtypes => '',
  proallargtypes => '{xid,text,timestamptz,oid,oid}',
  proargmodes => '{o,o,o,o,o}',
  proargnames => '{transaction,gid,prepared,ownerid,dbid}',
  prosrc => 'pg_prepared_xact' },
```
Backing C function `pg_prepared_xact()` (twophase.c:710-789) is a set-returning
function that snapshots `TwoPhaseState` (`GetPreparedTransactionList`,
twophase.c:665-692) under `TwoPhaseStateLock` (shared mode) and yields one row
per **valid** gxact (skips ones mid-prepare). Columns: `transaction` (xid, from
the dummy PGPROC not the gxact struct -- same value), `gid` (text), `prepared`
(timestamptz), `ownerid` (oid), `dbid` (oid, from PGPROC not gxact).

### 1.14 Dispatch in `utility.c` (`standard_ProcessUtility`)

```c
/* utility.c:642-659 */
case TRANS_STMT_PREPARE:
    if (!PrepareTransactionBlock(stmt->gid)) { if (qc) SetQueryCompletion(qc, CMDTAG_ROLLBACK, 0); }
    break;
case TRANS_STMT_COMMIT_PREPARED:
    PreventInTransactionBlock(isTopLevel, "COMMIT PREPARED");
    FinishPreparedTransaction(stmt->gid, true);
    break;
case TRANS_STMT_ROLLBACK_PREPARED:
    PreventInTransactionBlock(isTopLevel, "ROLLBACK PREPARED");
    FinishPreparedTransaction(stmt->gid, false);
    break;
```
`PreventInTransactionBlock` enforces these two commands cannot run inside an
existing `BEGIN` block (they define their own tiny top-level transaction, unlike
`PREPARE TRANSACTION` which finishes the *currently open* block).

---

## 2. PepperDB current status

| Component | Status | File:line |
|---|---|---|
| SQL grammar: `PREPARE TRANSACTION`/`COMMIT PREPARED`/`ROLLBACK PREPARED` | **done** (parses, GID captured, tested) | `src/backend/parser/gram.lalrpop` (`TransactionStmt` production); builder `make_transaction_stmt` in `src/backend/parser/parser.rs` (~1075-1090); test at `parser.rs:2485` |
| `TransactionStmtKind`/`TxKind` enum incl. PREPARE/COMMIT_PREPARED/ROLLBACK_PREPARED | done | `src/nodes/parsenodes.rs` |
| `CommandTag` enum variants (`PrepareTransaction`, `CommitPrepared`, `RollbackPrepared`) | done | `src/tcop/cmdtaglist.rs:65,177,186` |
| `CommandTagBehavior` display metadata for the 3 tags | done | `src/tcop/cmdtaglist.rs:271,383,392` |
| `CreateCommandTag` classification (maps `TxKind` -> `CommandTag`) | done | `src/backend/tcop/utility.rs` (~522-532 per prior audit) |
| WAL structs: `GIDSIZE`, `xl_xact_twophase`, `xl_xact_prepare`, `xl_xact_parsed_commit/abort` w/ twophase fields | **done, struct-identical to PG** | `src/access/xact.rs:16` (`GIDSIZE=200`), `:228-230` (`xl_xact_twophase`), `:254-271` (`xl_xact_prepare`), `:274-306` (parsed commit/abort) |
| `TwoPhaseRmgrId` enum | **done, matches PG IDs exactly** (End=0,Lock=1,Pgstat=2,Multixact=3,Predicatelock=4) | `src/access/twophase_rmgr.rs:11-22` |
| `ProcCounts`/`PROCARRAY_MAXPROCS` sizing formula includes a `max_prepared_xacts` term | done (formula present, fed 0) | `src/storage/proc.rs` (`ProcCounts.max_prepared_xacts: usize`); `src/backend/storage/lmgr/proc.rs:69-100` (`proc_counts()`, line 98 hardcodes `max_prepared_xacts: 0`); `src/shared_state.rs:188-297` (`PROCARRAY_MAXPROCS`/`DEFAULT_PROCARRAY_MAXPROCS=128`) |
| rmgrdesc dispatch pattern (`describe_wal_record`) + Xact-rmgr desc/identify for PREPARE/COMMIT_PREPARED/ABORT_PREPARED | done (desc layer only; no records ever produced) | `src/backend/access/rmgrdesc/mod.rs` (dispatch, `builtin()` maps rmid 0-21 incl. Multixact=6); `src/backend/access/rmgrdesc/xactdesc.rs` (`xact_identify` maps the 3 opcodes to strings; `xact_desc` renders PREPARE's `prepared_at`) |
| `TwoPhaseRmgrId`-specific rmgrdesc module (twophase.c's *own* rmgr, distinct from Xact WAL rmgr) | **absent** | no `twophasedesc.rs`/similar; no arm in `mod.rs`'s `builtin()`/`describe_wal_record()` |
| `prepare_transaction_stub()` | **hard stub, panics** | `src/backend/access/transam/xact.rs:1462-1466`: `unimplemented!("two-phase PrepareTransaction is deferred (twophase.c)")`; called from `Prepare` block-state arm (:1371) and `SubCommit`-into-`Prepare` arm (:1400) |
| `PrepareTransactionBlock` | **partial**: block-state transition works, GID is dropped | `src/backend/access/transam/xact.rs:1704-1714`: sets `TBlockState::Prepare`, but `let _ = gid;` discards the GID string instead of stashing it for the (stubbed) `PrepareTransaction` |
| `xact_redo_async` handling of PREPARE/COMMIT_PREPARED/ABORT_PREPARED | **missing arms** -- falls into an `ERROR` elog, not a panic | `src/backend/access/transam/xact.rs:2692-2732`; only `XLOG_XACT_COMMIT`/`XLOG_XACT_ABORT` opcodes handled; else-branch at :2717-2721 elogs `"xact_redo: unimplemented xact opcode {opcode:#x} (staged: prepared 2PC)"` and returns |
| `src/access/twophase.rs` (twophase.h surface) | **100% stub** -- every fn is `unimplemented!()` | whole file (113 lines): `TwoPhaseShmemSize/Init`, `AtAbort_Twophase`, `PostPrepare_Twophase`, `TwoPhaseGetXidByVirtualXID`, `TwoPhaseGetDummyProc(Number)`, `MarkAsPreparing`, `StartPrepare`, `EndPrepare`, `StandbyTransactionIdIsPrepared`, `PrescanPreparedTransactions`, `StandbyRecoverPreparedTransactions`, `RecoverPreparedTransactions`, `CheckPointTwoPhase`, `FinishPreparedTransaction`, `PrepareRedoAdd/Remove`, `restoreTwoPhaseData`, `LookupGXact(BySubid)`, `TwoPhaseTransactionGid` -- all present as signatures, all bodies `unimplemented!()`. `GlobalTransactionData`/`GlobalTransaction` types exist as an opaque raw-pointer typedef (`*mut GlobalTransactionData`, marked `// TODO(ptr)`) |
| `src/access/twophase_rmgr.rs` callback arrays | **stub** -- empty slices, not populated per PG's 4 dispatch tables | `:24-27`: `TWOPHASE_RECOVER_CALLBACKS`/`POSTCOMMIT`/`POSTABORT`/`STANDBY_RECOVER_CALLBACKS` all `&[]` |
| `register_two_phase_record` | stub | `src/access/twophase_rmgr.rs:29-31`: `unimplemented!()` |
| `AtPrepare_Locks`/`PostPrepare_Locks` | **stub, silent no-op** (not panic) | `src/backend/storage/lmgr/lock.rs:2171-2180`: empty bodies with `TODO(twophase)` comments |
| `lock_twophase_recover`/`postcommit`/`postabort`/`standby_recover` | **stub, silent no-op** | `lock.rs:2182-2191`: `postabort` even forwards to `postcommit` (both no-ops today) |
| `XactLockForVirtualXact` (needed by `VirtualXactLock` for recovered prepared xacts) | **stub**, currently just returns `true` unconditionally | `lock.rs:2109-2111`: `if vxid.is_recovered_prepared_xact() { // TODO(15d/twophase): XactLockForVirtualXact. return true; }` |
| `multixact_twophase_recover`/`postcommit`/`postabort` | **stub, hard panic** | `src/access/multixact.rs` (~195-203): all three `unimplemented!()` |
| `AtPrepare_PredicateLocks`/`PostPrepare_PredicateLocks`/`PredicateLockTwoPhaseFinish`/`predicatelock_twophase_recover` | **stub, hard panic** | `src/storage/predicate.rs` (~112-124): all four `unimplemented!()`, tagged `TODO(lock-manager)` |
| `pgstat_twophase_postcommit`/`postabort` | **stub, hard panic** | `src/pgstat.rs` (~668-673): both `unimplemented!()` (embedded among many other unrelated unimplemented pgstat fns -- this area is broadly unfinished, not specifically 2PC-deferred) |
| `PREPARE TRANSACTION` execution dispatch | reaches the stub (see above) | `src/backend/tcop/utility.rs` `Kind::PREPARE` arm (~379-386): calls `PrepareTransactionBlock`, no immediate panic (panics later at commit-command time) |
| `COMMIT PREPARED`/`ROLLBACK PREPARED` execution dispatch | **hard panic immediately** | `src/backend/tcop/utility.rs` ~387-389: `Kind::COMMIT_PREPARED \| Kind::ROLLBACK_PREPARED => { not_yet_reachable("ProcessUtility: two-phase COMMIT/ROLLBACK PREPARED"); }` |
| `max_prepared_transactions` GUC | **entirely absent** | zero hits anywhere in `src/`; not in `src/backend/utils/misc/guc_tables.rs`'s `CONFIGURE_NAMES_INT` (only `max_connections` pattern exists at line 206) |
| `pg_prepared_xacts` view / `pg_prepared_xact()` function | **entirely absent** | zero hits in `src/catalog/` or `src/backend/catalog/`; only unrelated hit is `ControlFileData.max_prepared_xacts: i32` in `src/catalog/pg_control.rs:106` |
| Shared-memory-equivalent (`SharedState`) reserved field for `TwoPhaseState` | **absent** | `src/backend/storage/ipc/ipci.rs` has no 2PC sizing hook; `src/shared_state.rs` has no `TwoPhaseState`-equivalent `Arc` field (formula-level `max_prepared_xacts` term exists per row above, but no actual gxact array/table type) |
| twophase.c-specific test coverage / regress harness triage | **not yet attempted** | `ai/tmp/regress-analysis/` has no `batch-prepared_xacts.md` or `batch-temp.md` entry (both tests use 2PC) |

**Net picture:** grammar, AST, command-tag, and WAL-struct-shape layers are fully
translated and PG-faithful (including exact `GIDSIZE`/enum-value parity). The
entire *execution* surface -- gxact shared state, state-file I/O, WAL PREPARE
emission, all rmgr `AtPrepare_*`/`PostPrepare_*`/`*_recover`/`*_postcommit`/
`*_postabort` callbacks, the `pg_prepared_xacts` catalog view, the GUC, and
crash-recovery of in-doubt prepared xacts -- is either a hard-panic stub, a
silent no-op stub, or entirely absent.

---

## 3. Tests unblocked

PG regress tests that exercise 2PC (grep across `ref/postgres/src/test/regress/sql/`):

| Test | File | Uses |
|---|---|---|
| `prepared_xacts` | `sql/prepared_xacts.sql` (195 lines) | Primary 2PC test: PREPARE/COMMIT PREPARED/ROLLBACK PREPARED, `pg_prepared_xacts` view, duplicate-GID rejection, SSI serialization failure interaction, subtransactions, shared invalidation (DROP+CREATE across prepare), cross-backend visibility (`\c -` reconnect mid-test), row-level lock / multixact interaction |
| `temp` | `sql/temp.sql` (1 usage, line 313) | `PREPARE TRANSACTION 'twophase_search'` used to test that a temp-namespace `search_path` setting is *not* leaked via a prepared xact (this one should actually raise the "cannot PREPARE a transaction that has operated on temporary objects" error per xact.c:2613-2616 -- worth checking PG's actual `.out` file to confirm expected behavior once this is implemented) |

**Count: 2 test files** reference 2PC statements (`prepared_xacts.sql`,
`temp.sql`). `prepared_xacts` is scheduled standalone in
`parallel_schedule:64` alongside `select_into`, `transactions`, `random`,
`portals`, etc.

**Gating detail confirmed** -- `sql/prepared_xacts.sql:1-4`:
```sql
SELECT current_setting('max_prepared_transactions')::integer < 2 AS skip_test \gset
\if :skip_test
\quit
\endif
```
This is a real, load-bearing gate: the whole test file self-skips via psql's
`\gset`/`\if`/`\quit` meta-commands unless `max_prepared_transactions >= 2` (the
test needs at least 2 concurrently held prepared xacts, e.g. `regress_sub1` +
`regress_sub2` held open simultaneously at lines 111/124-152/160). PG's default
test config sets `max_prepared_transactions` to a nonzero value specifically to
enable this; PepperDB will need the same once the GUC exists.

Neither test appears in `ai/tmp/regress-analysis/` yet (no `batch-prepared_xacts.md`,
no `batch-temp.md`), meaning this pair hasn't been triaged in the prior regress
analysis pass -- plan 004 is the first place they'll be scoped.

---

## 4. Step breakdown for plan 004

Dependencies satisfied by plan 003: xact.c commit/abort/savepoints (done), WAL
writer + record framework (done), clog (done), crash recovery driver + per-AM
redo (M14, done), lock manager core (done). Nothing in plan 004 needs new
infrastructure outside twophase.c's own scope except the GUC registry (pattern
exists) and one new `SharedState` field.

### Step 1 -- GUC + shared "shmem" plumbing (S, ~2-4h)
- Add `max_prepared_transactions` to `CONFIGURE_NAMES_INT` in
  `src/backend/utils/misc/guc_tables.rs`, following the `max_connections` pattern
  at line 206 (`def_generic!`, `PGC_POSTMASTER`-equivalent context, boot=0, min=0,
  max=`MAX_BACKENDS`-equivalent).
- Wire the GUC value into `proc_counts()` (`src/backend/storage/lmgr/proc.rs:98`,
  replacing the hardcoded `max_prepared_xacts: 0`) so `ProcCounts`/`ProcGlobal`
  sizing picks up the real value (formula already exists, this is a one-line
  unblock).
- Add a `TwoPhaseState`-equivalent field to `SharedState`
  (`src/shared_state.rs`) -- see section 5 for the concrete type. No dummy-PGPROC
  array is needed the way PG needs it (see section 5), but the gxact table
  itself needs a home behind a lock.
- **Deliverable:** `max_prepared_transactions` settable, `pg_settings`-equivalent
  reflects it, `PROCARRAY_MAXPROCS`/`ProcCounts` formulas produce nonzero output
  when set >0.
- **Dependencies:** GUC registry (exists), `ProcCounts`/`shared_state.rs` (exist).
- **Tests unblocked:** none standalone; prerequisite for the `\gset`/`\if` gate in
  `prepared_xacts.sql` to evaluate false.

### Step 2 -- gxact core: `MarkAsPreparing`/`StartPrepare`/`EndPrepare`/`PrepareTransaction` (L, ~2-3 days)
- Implement the `GlobalTransactionData` struct for real (currently an opaque raw
  pointer) in `src/access/twophase.rs`: fields per section 1.2, `gid` as `String`
  or `[u8; GIDSIZE]` (prefer PG-exact `[u8; 200]` to match the WAL struct
  reuse in section 1.5).
- Implement `MarkAsPreparing` (duplicate-GID check, freelist/Vec allocation,
  `max_prepared_xacts==0` -> error) against the new `SharedState` field.
- Implement `StartPrepare`/`RegisterTwoPhaseRecord`/`EndPrepare` -- in PepperDB
  these can likely collapse: build the state-file byte buffer in memory (reuse
  `xactGetCommittedChildren`/`smgrGetPendingDeletes`/
  `xactGetCommittedInvalidationMessages`/`pgstat_get_transactional_drops`
  equivalents, which plan 003's commit path already calls -- grep `xact.rs` around
  the commit-record-building code near line 1342-1346 for the exact existing
  helper names) then hand it to the WAL layer as one `XLOG_XACT_PREPARE` record's
  payload (new WAL-emission function analogous to `XactLogCommitRecord`/
  `XactLogAbortRecord`, i.e. `XactLogPrepareRecord` -- does not exist yet, must be added).
- Replace `prepare_transaction_stub()` (`xact.rs:1462-1466`) with a real
  `PrepareTransaction()` following the sequence in section 1.3: restriction
  checks (temp-namespace/exported-snapshots -- check whether `XACT_FLAGS_ACCESSEDTEMPNAMESPACE`-equivalent
  and `XactHasExportedSnapshots`-equivalent already exist in PepperDB's xact
  state, likely yes from plan 003's savepoint/temp-table work), `AtPrepare_*`
  callback calls in PG's exact order, `EndPrepare`, `PostPrepare_Locks`,
  `ProcArrayClearTransaction`, backend cleanup, `PostPrepare_Twophase`.
- Fix `PrepareTransactionBlock` (`xact.rs:1704-1714`) to actually stash the GID
  (currently `let _ = gid;` drops it) into per-task xact state for
  `PrepareTransaction` to consume, mirroring PG's `prepareGID` static.
- Implement the four `AtPrepare_*` callbacks that are currently no-ops/panics:
  `AtPrepare_Locks` (`lock.rs:2173-2175`, needs `RegisterTwoPhaseRecord(Lock, ...)`
  + fast-path lock materialization, mirror `lock.c:3446`), `AtPrepare_PredicateLocks`
  (`predicate.rs`, mirror `predicate.c:4790`), `AtPrepare_MultiXact` (does this fn
  exist in `multixact.rs`? verify -- PG has it at `multixact.c:1986`, not
  mentioned in the PepperDB grep hits so likely also absent, add it),
  `AtPrepare_PgStat` (check `pgstat_xact.c:191` equivalent -- likely needs adding).
  `AtPrepare_Notify`/`AtPrepare_RelationMap` may be lower priority (notify/relmap
  subsystems may not be fully ported yet in plan 003 -- verify before scoping).
- **Deliverable:** `PREPARE TRANSACTION 'gid'` succeeds end-to-end, writes one WAL
  PREPARE record, gxact is live in `SharedState`, backend releases its
  transaction slot, `pg_prepared_xact()`-equivalent (if step 4 lands first) can
  see it.
- **Dependencies:** xact.c commit/abort infra (plan 003, done), WAL writer (done),
  lock manager (done, needs new callback bodies), Step 1 (GUC + shmem field).
- **Tests unblocked:** none pass fully yet (needs step 3 for COMMIT/ROLLBACK
  PREPARED), but `prepared_xacts.sql`'s early PREPARE statements become
  reachable instead of panicking.

### Step 3 -- `FinishPreparedTransaction`: COMMIT PREPARED / ROLLBACK PREPARED (M, ~1-2 days)
- Implement `LockGXact` (GID lookup + ownership/database check + busy check).
- Implement state read: WAL-based (`XlogReadTwoPhaseData` equivalent, read the
  PREPARE record back out of the WAL reader at `prepare_start_lsn`) -- file-based
  read (`ReadTwoPhaseFile`) can be deferred to Step 5/6 if no on-disk state-file
  persistence is implemented yet (see section 5 -- likely fine to implement
  WAL-only for a first cut and defer the on-disk file entirely, since PepperDB's
  crash-recovery replay window is bounded differently than PG's; revisit if
  checkpoint-interval-crossing prepared xacts are a requirement).
- Implement `RecordTransactionCommitPrepared`/`RecordTransactionAbortPrepared`
  (mirror the plan-003 `RecordTransactionCommit`/`Abort`, reuse
  `XactLogCommitRecord`/`XactLogAbortRecord` but populate the twophase trailer
  fields that are currently asserted-invalid -- grep `xact.rs:2504`/`:2576` for
  the `debug_assert!(!twophase_xid.is_valid())`-style guards that must be relaxed).
- Implement `FinishPreparedTransaction` orchestration per section 1.9: WAL
  record -> clog mark -> ProcArray removal -> file deletes -> inval sends ->
  `ProcessRecords` against postcommit/postabort callback tables -> `RemoveGXact`.
- Implement the four missing `_postcommit`/`_postabort` callback bodies (real,
  not stub): `lock_twophase_postcommit/postabort` (`lock.rs:2184-2189`, release
  the transferred locks -- reuse existing `LockRelease`/proc-lock-release
  machinery), `multixact_twophase_postcommit/postabort` (`multixact.rs`, mirror
  `multixact.c:2070/2085`), `pgstat_twophase_postcommit/postabort` (`pgstat.rs`,
  mirror `pgstat_relation.c:747/783`).
- Fix `not_yet_reachable` panics in `utility.rs` (~387-389) to dispatch to the
  new `FinishPreparedTransaction`.
- Add `PreventInTransactionBlock`-equivalent guard for COMMIT/ROLLBACK PREPARED
  (check if this helper already exists from plan 003's BEGIN/COMMIT work).
- **Deliverable:** full PREPARE -> COMMIT PREPARED and PREPARE -> ROLLBACK
  PREPARED round trips work; locks released; data visible/invisible correctly.
- **Dependencies:** Step 2 (gxact + WAL PREPARE), clog (done), lock manager
  release paths (done, need twophase callback wiring).
- **Tests unblocked:** most of `prepared_xacts.sql`'s single-backend-session
  scenarios (duplicate GID rejection, simple commit/rollback, SSI failure)
  become passable. Cross-backend/reconnect scenarios (`\c -` at line 141) need
  Step 2's WAL-based state read to survive past the original backend's lifetime
  -- confirm PepperDB's single-process model still lets a *different* session
  read another session's PREPARE record from the shared WAL reader; should be
  fine since WAL is process-global, not per-connection.

### Step 4 -- `pg_prepared_xacts` view (S, ~2-4h)
- Add `pg_prepared_xact()` SRF-equivalent: snapshot the `SharedState` gxact table
  under its lock, yield `(transaction: xid, gid: text, prepared: timestamptz,
  ownerid: oid, dbid: oid)` rows, skip non-valid (mid-prepare) entries -- mirror
  `GetPreparedTransactionList` (twophase.c:665-692) + `pg_prepared_xact`
  (twophase.c:710-789).
- Register `pg_prepared_xacts` view in whatever PepperDB uses as its
  `system_views.sql`-equivalent bootstrap (check `src/catalog/` for the
  bootstrap-view registration mechanism established in plan 003; the memory
  notes mention "build.rs seeds rows + Rust initdb" as the bootstrap approach --
  find where other system views like `pg_prepared_statements` are registered,
  since `cmdtaglist.rs` confirms tag-level infra exists but view-level does not).
- Register the underlying function in whatever PepperDB uses as its
  `pg_proc.dat`-equivalent catalog seed.
- **Deliverable:** `SELECT * FROM pg_prepared_xacts` returns correct rows.
- **Dependencies:** Step 2 (gxact table must exist and be populated).
- **Tests unblocked:** `prepared_xacts.sql`'s `SELECT gid FROM pg_prepared_xacts
  WHERE gid ~ '^regress_' ORDER BY gid` assertions (used ~7 times throughout the
  test to check prepared-xact visibility before/after operations).

### Step 5 -- Crash recovery of in-doubt prepared xacts (M-L, ~1-2 days)
- Implement `PrepareRedoAdd`/`PrepareRedoRemove` (currently stubs in
  `src/access/twophase.rs`) -- in-memory gxact tracking during WAL replay.
- Add the missing `XLOG_XACT_PREPARE`/`COMMIT_PREPARED`/`ABORT_PREPARED` arms to
  `xact_redo_async` (`xact.rs:2692-2732`), replacing the current
  `elog(ERROR, "unimplemented xact opcode...")` fallthrough. PREPARE arm calls
  `PrepareRedoAdd`; COMMIT_PREPARED/ABORT_PREPARED arms do the normal
  `xact_redo_commit`/`xact_redo_abort`-equivalent clog update using the record's
  `twophase_xid` (not the record header's own xid) plus `PrepareRedoRemove`.
- Implement `PrescanPreparedTransactions`/`RecoverPreparedTransactions` (currently
  stubs) -- wire into plan 003's M14 recovery driver (`xlogrecovery`-equivalent)
  at the appropriate point (after main redo loop, before accepting new
  transactions), per section 1.11's ordering.
- Implement the `*_recover` callback bodies: `lock_twophase_recover` (reacquire
  locks, currently a silent no-op at `lock.rs:2183`), `multixact_twophase_recover`
  (currently panics, `multixact.rs`), `predicatelock_twophase_recover` (currently
  panics, `predicate.rs`).
- Implement `XactLockForVirtualXact` (`lock.rs:2109-2111` TODO) so
  `VirtualXactLock` correctly blocks on a recovered-but-not-yet-committed
  prepared xact instead of unconditionally returning `true`.
- Decide whether on-disk `pg_twophase/`-equivalent state-file persistence +
  `CheckPointTwoPhase` is in scope for this step or deferred entirely (see
  section 5 -- recommend deferring if PepperDB's WAL retention/checkpoint model
  makes the WAL-only path sufficient for the target use cases; flag as an
  explicit scope decision for whoever picks up plan 004).
- **Deliverable:** a prepared-but-unfinished transaction survives a
  simulated crash/restart and can still be `COMMIT PREPARED`/`ROLLBACK
  PREPARED`-ed afterward; its locks are correctly held during the recovery
  window.
- **Dependencies:** Steps 2-3 (gxact + finish path), M14 recovery/redo driver
  (plan 003, done) -- this step is the one most tightly coupled to plan 003's
  xlogrecovery.rs and rmgrdesc dispatch.
- **Tests unblocked:** none of PG's own regress suite exercises actual crash
  recovery of 2PC (comment in `prepared_xacts.sql:9-12`: "We can't readily test
  persistence of prepared xacts within the regression script framework"), so
  this step is validated by new PepperDB-specific integration tests, not by
  unlocking additional `.sql`/`.out` pairs.

### Step ordering rationale
Steps 1-2-3-4 are a strict dependency chain (GUC/shmem -> prepare -> finish ->
view) and should land as sequential sub-commits under one plan-004 gate, matching
the "LARGE step may split into dependency sub-commits" convention. Step 5 can be
developed in parallel with Step 4 once Step 3 lands, since recovery and the
catalog view are independent consumers of the same gxact table.

---

## 5. Architecture notes

### 5.1 State file: format, timing, checkpoint interaction

- **Format:** section 1.6 above (`TwoPhaseFileHeader` == `xl_xact_prepare` +
  subxact/relfile/stats/inval arrays + rmgr records + CRC).
- **When written to WAL:** always, at `PREPARE TRANSACTION` time, exactly once
  per prepared xact (`EndPrepare` -> single `XLOG_XACT_PREPARE` record).
- **When written to disk (`pg_twophase/<xid>`):** only lazily, by
  `CheckPointTwoPhase`, for prepared xacts that have outlived at least one
  checkpoint. Most prepared xacts (held for seconds to minutes by a 2PC
  coordinator) never touch disk at all -- they're read back directly from WAL by
  `COMMIT PREPARED`/`ROLLBACK PREPARED`.
- **Recommendation for PepperDB:** implement the WAL-only path first (Steps 2-3).
  The on-disk file's only purpose is surviving a checkpoint boundary while a
  prepared xact is still open (so WAL segments containing the PREPARE record can
  be recycled) -- if PepperDB's WAL retention model keeps all WAL since the last
  checkpoint anyway (verify against plan 003's checkpoint implementation), the
  disk-file mechanism can be deferred as a pure disk-space optimization rather
  than a correctness requirement, with a follow-up plan once checkpoint-driven
  WAL recycling is confirmed to need it.

### 5.2 WAL PREPARE / COMMIT_PREPARED / ABORT_PREPARED redo, tied to M14

- All three record types are read through the **same rmgr dispatch** as
  heap/btree/etc: PG's `RM_XACT_ID` rmgr, `xact_redo()` switching on the `info`
  opcode bits. PepperDB's equivalent is `xact_redo_async` in
  `src/backend/access/transam/xact.rs:2692`, which plan 003's M14 recovery driver
  (the "step 48 WAL replay driver + per-AM redo" commit, `4bffa70`) already calls
  for `XLOG_XACT_COMMIT`/`ABORT`. Step 5 above adds the three missing opcode arms
  to this *same* function -- no new rmgr registration needed for the WAL-record
  redo path itself, since PREPARE/COMMIT_PREPARED/ABORT_PREPARED are Xact-rmgr
  records, not a separate rmgr.
- **Separately**, twophase.c's *own* rmgr concept (`TwoPhaseRmgrId`,
  `TWOPHASE_RM_LOCK_ID` etc.) is unrelated to the WAL rmgr system -- it's a
  private dispatch table used only for replaying the *state-file's internal
  records* (the `TwoPhaseRecordOnDisk` entries written by `RegisterTwoPhaseRecord`)
  during `RecoverPreparedTransactions`/`FinishPreparedTransaction`, not during
  ordinary WAL redo. Don't conflate the two rmgr concepts when implementing.
- **rmgrdesc (human-readable WAL dumping, "step 49" commit):** the WAL-record
  *description* layer already exists and is correct for these three opcodes --
  `src/backend/access/rmgrdesc/xactdesc.rs`'s `xact_identify`/`xact_desc` already
  map `XLOG_XACT_PREPARE`/`COMMIT_PREPARED`/`ABORT_PREPARED` to display strings
  (this was apparently done speculatively/for completeness during M14, ahead of
  the actual PREPARE-record-emission code existing). No new rmgrdesc work is
  needed for plan 004 on the Xact-rmgr side. If a *state-file* dump/debug tool is
  ever wanted (PG doesn't really have one either, beyond `pg_controldata`-style
  ad hoc tools), that would be new speculative scope, not required for
  correctness.

### 5.3 Single-process implications: what replaces PG's shared memory

PG's `TwoPhaseState` (a `ShmemInitStruct`-allocated fixed-size array of gxact
pointers, sized once at postmaster start by `max_prepared_xacts`) and the
`PreparedXactProcs` dummy-PGPROC carve-out exist to solve two multi-process
problems that **do not apply** to PepperDB's single-process async model:

1. **Cross-process visibility.** PG's gxacts must live in literal shared memory
   because COMMIT PREPARED can run in a different OS process than the one that
   ran PREPARE TRANSACTION. PepperDB has one process; a plain in-process
   collection behind a lock is sufficient and directly matches the established
   `rules.md` convention ("Ex-shared-memory state: typed `Arc` fields on
   `SharedState`, each owning its locking" -- rules.md:121,130,161).
2. **`TransactionIdIsInProgress`/lock-manager integration via a real PGPROC.**
   PG hangs a dummy `PGPROC` off each gxact purely so the existing
   backend-centric ProcArray/lock-manager code paths (`GetLockConflicts`,
   `TransactionIdIsInProgress`, fast-path lock transfer) work unmodified for a
   "backend" that isn't actually connected. PepperDB's `ProcArray`/lock-manager
   equivalents were already translated in plan 003 against the same PGPROC
   abstraction (`src/storage/proc.rs`, `ProcCounts` already has a
   `max_prepared_xacts` field slot, confirmed in section 2) -- so the *cheapest*
   correct translation is to keep this pattern: still allocate a slot in the
   existing PGPROC/`ProcGlobal` arena for each prepared xact (reusing
   `FIRST_PREPARED_XACT_PROC_NUMBER`-equivalent offset math already implied by
   the `ProcCounts.max_prepared_xacts` field), rather than inventing a parallel
   lock-visibility mechanism. This keeps `AtPrepare_Locks`/`PostPrepare_Locks`
   and the recover/postcommit/postabort callbacks structurally identical to PG's,
   which minimizes translation risk.

**Concrete recommendation:**
- Add a new field to `SharedState` (`src/shared_state.rs`), e.g.
  `two_phase_state: Mutex<TwoPhaseState>` (or `RwLock`, matching whatever
  granularity convention plan 003 used for `ProcArray`/`LockManager` -- check
  which lock type those use and mirror it, since `TwoPhaseStateLock` in PG is a
  regular `LWLock` acquired both shared and exclusive, same shape as PG's other
  `LWLock`-guarded structures already translated).
- `TwoPhaseState` (new Rust type, home in `src/access/twophase.rs`) is a plain
  struct: `Vec<GlobalTransactionData>` or `slab`/freelist-style storage (a
  `Vec<Option<GlobalTransactionData>>` or an actual free-list mirroring PG's
  `freeGXacts` linked list both work; a `Vec` with tombstones is simplest given
  Rust's ownership model doesn't need PG's manual freelist trick).
- `GlobalTransactionData` becomes a real struct (not the current opaque
  `*mut GlobalTransactionData` placeholder) with fields per section 1.2, using
  owned `String`/`[u8; GIDSIZE]` for `gid` instead of a C string, and a `usize`
  proc-slot index instead of `pgprocno` if PepperDB's PGPROC arena uses a
  different indexing convention (check `src/storage/proc.rs`'s existing
  `ProcNumber`-equivalent type and reuse it verbatim for consistency).
- No `#[process_global]` macro application needed here specifically (that macro
  is for `OnceLock`-based *singleton* subsystems typically constructed once at
  startup outside `SharedState`, per the memory notes' description of its use on
  `ProcGlobal`/`LockManager`/`VariableCache`) -- `TwoPhaseState` fits more
  naturally as a plain `SharedState` field since its lifetime is tied to the
  database instance the same way `ProcArray` is, unless plan 003 established
  `ProcGlobal` itself via `#[process_global]` (if so, mirror that exact pattern
  instead for consistency -- verify by reading how `ProcGlobal` is declared in
  `src/storage/proc.rs` before deciding).

//! src/backend/utils/cache/inval.c
//!
//! POSTGRES cache invalidation dispatcher code.
//!
//! This is subtle stuff, so pay attention:
//!
//! When a tuple is updated or deleted, our standard visibility rules
//! consider that it is *still valid* so long as we are in the same command,
//! ie, until the next CommandCounterIncrement() or transaction commit.
//! (See access/heap/heapam_visibility.c, and note that system catalogs are
//! generally scanned under the most current snapshot available, rather than
//! the transaction snapshot.)  At the command boundary, the old tuple stops
//! being valid and the new version, if any, becomes valid.  Therefore,
//! we cannot simply flush a tuple from the system caches during heap_update()
//! or heap_delete().  The tuple is still good at that point; what's more,
//! even if we did flush it, it might be reloaded into the caches by a later
//! request in the same command.  So the correct behavior is to keep a list
//! of outdated (updated/deleted) tuples and then do the required cache
//! flushes at the next command boundary.  We must also keep track of
//! inserted tuples so that we can flush "negative" cache entries that match
//! the new tuples; again, that mustn't happen until end of command.
//!
//! See the C source for the full file header commentary.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use core::ffi::CStr;

use crate::access::htup_details::{HeapTuple, HeapTupleData, GETSTRUCT, HeapTupleIsValid};
use crate::access::rmgrdesc::standbydesc::{
    SharedInvalidationMessage, SHAREDINVALCATALOG_ID, SHAREDINVALRELCACHE_ID,
    SHAREDINVALRELMAP_ID, SHAREDINVALRELSYNC_ID, SHAREDINVALSMGR_ID, SHAREDINVALSNAPSHOT_ID,
};
use crate::access::rmgrlist::RM_XACT_ID;
use crate::access::transam::xact::{
    xl_xact_invals, CurTransactionContext, GetCurrentCommandId, GetCurrentTransactionNestLevel,
    IsTransactionState, MinSizeOfXactInvals, XLOG_XACT_INVALIDATIONS,
};
use crate::access::transam::xloginsert::{XLogBeginInsert, XLogInsert, XLogRegisterData};
use crate::catalog::catalog::{IsCatalogRelation, IsSharedRelation, IsToastRelation};
use crate::catalog::catalog_oids::{
    AttributeRelationId, ConstraintRelationId, IndexRelationId, RelationRelationId,
};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::catalog::pg_class::Form_pg_class;
use crate::catalog::pg_constraint::{Form_pg_constraint, CONSTRAINT_FOREIGN};
use crate::catalog::pg_index::Form_pg_index;
use crate::common::relpath::GetDatabasePath;
use crate::miscadmin::{
    CritSectionCount, DatabasePath, IsBootstrapProcessingMode, MyDatabaseId,
};
use crate::storage::ipc::sinval::{ReceiveSharedInvalidMessages, SendSharedInvalidMessages};
use crate::storage::procnumber::MAX_BACKENDS_BITS;
use crate::storage::relfilelocator::RelFileLocatorBackend;
use crate::storage::smgr::smgr::smgrreleaserellocator;
use crate::utils::cache::catcache::{
    CatalogCacheFlushCatalog, PrepareToInvalidateCacheTuple, ResetCatalogCachesExt,
};
use crate::utils::cache::relmapper::RelationMapInvalidate;
use crate::utils::cache::syscache::{
    RelationInvalidatesSnapshotsOnly, SysCacheInvalidate, SysCacheSize,
};
use crate::utils::mmgr::mcxt::TopTransactionContext;
use crate::utils::palloc::{palloc, palloc0, MemoryContextAlloc, MemoryContextAllocZero};
use crate::utils::rel::{Relation, RelationGetRelid};

// errcode is referenced only in folded "C also:" commentary below.

// ----------------------------------------------------------------
// Macro stand-ins (postgres.h family) used by this unit.
// ----------------------------------------------------------------

// INJECTION_POINT(name, arg)  -- utils/injection_point.h
macro_rules! INJECTION_POINT {
    ($name:expr, $arg:expr) => {{
        // TODO(pg-port): utils/injection_point.h
        let _ = ($name, $arg);
    }};
}
use INJECTION_POINT;

// VALGRIND_MAKE_MEM_DEFINED(p, s)  -- valgrind.h (no-op outside valgrind builds)
macro_rules! VALGRIND_MAKE_MEM_DEFINED {
    ($p:expr, $s:expr) => {{
        let _ = ($p, $s);
    }};
}
use VALGRIND_MAKE_MEM_DEFINED;

// StaticAssertStmt(cond, msg)  -- c.h
macro_rules! StaticAssertStmt {
    ($cond:expr, $msg:literal) => {
        const _: () = assert!($cond, $msg);
    };
}
use StaticAssertStmt;

// ObjectIdGetDatum(x)  -- postgres.h
#[inline]
unsafe fn ObjectIdGetDatum(oid: Oid) -> Datum {
    oid as Datum
}

// SysCacheIdentifier value used here (catalog/syscache_ids.h, generated).
const RELOID: c_int = 57;

// ----------------------------------------------------------------
// TODO(pg-port) stubs for dependencies in OTHER .c files.
// ----------------------------------------------------------------

// utils/snapmgr.c
unsafe fn InvalidateCatalogSnapshot() {
    crate::utils::time::snapmgr::InvalidateCatalogSnapshot()
}

// utils/cache/relcache.c
unsafe fn RelationCacheInvalidate(debug_discard: bool) {
    crate::utils::cache::relcache::RelationCacheInvalidate(debug_discard)
}

unsafe fn RelationCacheInvalidateEntry(relationId: Oid) {
    crate::utils::cache::relcache::RelationCacheInvalidateEntry(relationId as _)
}

unsafe fn RelationCacheInitFilePreInvalidate() {
    // TODO(pg-port): utils/cache/relcache.c
}

unsafe fn RelationCacheInitFilePostInvalidate() {
    // TODO(pg-port): utils/cache/relcache.c
}

unsafe fn RelationIdIsInInitFile(_relationId: Oid) -> bool {
    // TODO(pg-port): utils/cache/relcache.c
    false
}

// access/transam/xlog.c
unsafe fn XLogLogicalInfoActive() -> bool {
    // TODO(pg-port): access/transam/xlog.h
    false
}

// utils/cache/relcache.c -- relcache must be available for inval.
unsafe fn AssertCouldGetRelation() {
    // TODO(pg-port): utils/cache/relcache.c
}

// catalog/catalog.c
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    // TODO(pg-port): utils/cache/syscache.c
    null_mut()
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    // TODO(pg-port): utils/cache/syscache.c
}

// ----------------------------------------------------------------
// inval.h public callback function types.
// ----------------------------------------------------------------

pub type SyscacheCallbackFunction = unsafe fn(arg: Datum, cacheid: c_int, hashvalue: uint32);
pub type RelcacheCallbackFunction = unsafe fn(arg: Datum, relid: Oid);
pub type RelSyncCallbackFunction = unsafe fn(arg: Datum, relid: Oid);

/*
 * Pending requests are stored as ready-to-send SharedInvalidationMessages.
 * We keep the messages themselves in arrays in TopTransactionContext (there
 * are separate arrays for catcache and relcache messages).  For transactional
 * messages, control information is kept in a chain of TransInvalidationInfo
 * structs, also allocated in TopTransactionContext.  For inplace update
 * messages, control information appears in an InvalidationInfo, allocated in
 * CurrentMemoryContext.
 */
const CatCacheMsgs: usize = 0;
const RelCacheMsgs: usize = 1;

/* Pointers to main arrays in TopTransactionContext */
#[repr(C)]
#[derive(Clone, Copy)]
struct InvalMessageArray {
    msgs: *mut SharedInvalidationMessage, /* palloc'd array (can be expanded) */
    maxmsgs: c_int,                       /* current allocated size of array */
}

static mut InvalMessageArrays: [InvalMessageArray; 2] = [InvalMessageArray {
    msgs: null_mut(),
    maxmsgs: 0,
}; 2];

/* Control information for one logical group of messages */
#[repr(C)]
#[derive(Clone, Copy)]
struct InvalidationMsgsGroup {
    firstmsg: [c_int; 2], /* first index in relevant array */
    nextmsg: [c_int; 2],  /* last+1 index */
}

impl InvalidationMsgsGroup {
    const fn new() -> Self {
        InvalidationMsgsGroup {
            firstmsg: [0; 2],
            nextmsg: [0; 2],
        }
    }
}

/* Macros to help preserve InvalidationMsgsGroup abstraction */
unsafe fn SetSubGroupToFollow(
    targetgroup: *mut InvalidationMsgsGroup,
    priorgroup: *mut InvalidationMsgsGroup,
    subgroup: usize,
) {
    (*targetgroup).firstmsg[subgroup] = (*priorgroup).nextmsg[subgroup];
    (*targetgroup).nextmsg[subgroup] = (*priorgroup).nextmsg[subgroup];
}

unsafe fn SetGroupToFollow(
    targetgroup: *mut InvalidationMsgsGroup,
    priorgroup: *mut InvalidationMsgsGroup,
) {
    SetSubGroupToFollow(targetgroup, priorgroup, CatCacheMsgs);
    SetSubGroupToFollow(targetgroup, priorgroup, RelCacheMsgs);
}

unsafe fn NumMessagesInSubGroup(group: *const InvalidationMsgsGroup, subgroup: usize) -> c_int {
    (*group).nextmsg[subgroup] - (*group).firstmsg[subgroup]
}

unsafe fn NumMessagesInGroup(group: *const InvalidationMsgsGroup) -> c_int {
    NumMessagesInSubGroup(group, CatCacheMsgs) + NumMessagesInSubGroup(group, RelCacheMsgs)
}

/*----------------
 * Transactional invalidation messages are divided into two groups:
 *	1) events so far in current command, not yet reflected to caches.
 *	2) events in previous commands of current transaction; these have
 *	   been reflected to local caches, and must be either broadcast to
 *	   other backends or rolled back from local cache when we commit
 *	   or abort the transaction.
 *----------------
 */

/* fields common to both transactional and inplace invalidation */
#[repr(C)]
struct InvalidationInfo {
    /* Events emitted by current command */
    CurrentCmdInvalidMsgs: InvalidationMsgsGroup,

    /* init file must be invalidated? */
    RelcacheInitFileInval: bool,
}

/* subclass adding fields specific to transactional invalidation */
#[repr(C)]
struct TransInvalidationInfo {
    /* Base class */
    ii: InvalidationInfo,

    /* Events emitted by previous commands of this (sub)transaction */
    PriorCmdInvalidMsgs: InvalidationMsgsGroup,

    /* Back link to parent transaction's info */
    parent: *mut TransInvalidationInfo,

    /* Subtransaction nesting depth */
    my_level: c_int,
}

static mut transInvalInfo: *mut TransInvalidationInfo = null_mut();

static mut inplaceInvalInfo: *mut InvalidationInfo = null_mut();

/* GUC storage */
pub static mut debug_discard_caches: c_int = 0;

/*
 * Dynamically-registered callback functions.
 */

const MAX_SYSCACHE_CALLBACKS: usize = 64;
const MAX_RELCACHE_CALLBACKS: usize = 10;
const MAX_RELSYNC_CALLBACKS: usize = 10;

#[repr(C)]
#[derive(Clone, Copy)]
struct SYSCACHECALLBACK {
    id: int16,   /* cache number */
    link: int16, /* next callback index+1 for same cache */
    function: Option<SyscacheCallbackFunction>,
    arg: Datum,
}

static mut syscache_callback_list: [SYSCACHECALLBACK; MAX_SYSCACHE_CALLBACKS] = [SYSCACHECALLBACK {
    id: 0,
    link: 0,
    function: None,
    arg: 0,
}; MAX_SYSCACHE_CALLBACKS];

static mut syscache_callback_links: [int16; SysCacheSize as usize] = [0; SysCacheSize as usize];

static mut syscache_callback_count: c_int = 0;

#[repr(C)]
#[derive(Clone, Copy)]
struct RELCACHECALLBACK {
    function: Option<RelcacheCallbackFunction>,
    arg: Datum,
}

static mut relcache_callback_list: [RELCACHECALLBACK; MAX_RELCACHE_CALLBACKS] = [RELCACHECALLBACK {
    function: None,
    arg: 0,
}; MAX_RELCACHE_CALLBACKS];

static mut relcache_callback_count: c_int = 0;

#[repr(C)]
#[derive(Clone, Copy)]
struct RELSYNCCALLBACK {
    function: Option<RelSyncCallbackFunction>,
    arg: Datum,
}

static mut relsync_callback_list: [RELSYNCCALLBACK; MAX_RELSYNC_CALLBACKS] = [RELSYNCCALLBACK {
    function: None,
    arg: 0,
}; MAX_RELSYNC_CALLBACKS];

static mut relsync_callback_count: c_int = 0;

/* ----------------------------------------------------------------
 *				Invalidation subgroup support functions
 * ----------------------------------------------------------------
 */

/*
 * AddInvalidationMessage
 *		Add an invalidation message to a (sub)group.
 *
 * The group must be the last active one, since we assume we can add to the
 * end of the relevant InvalMessageArray.
 *
 * subgroup must be CatCacheMsgs or RelCacheMsgs.
 */
unsafe fn AddInvalidationMessage(
    group: *mut InvalidationMsgsGroup,
    subgroup: usize,
    msg: *const SharedInvalidationMessage,
) {
    let ima: *mut InvalMessageArray = &mut InvalMessageArrays[subgroup];
    let nextindex = (*group).nextmsg[subgroup];

    if nextindex >= (*ima).maxmsgs {
        if (*ima).msgs.is_null() {
            /* Create new storage array in TopTransactionContext */
            let reqsize = 32; /* arbitrary */

            (*ima).msgs = MemoryContextAlloc(
                TopTransactionContext,
                reqsize * core::mem::size_of::<SharedInvalidationMessage>(),
            ) as *mut SharedInvalidationMessage;
            (*ima).maxmsgs = reqsize as c_int;
            Assert!(nextindex == 0);
        } else {
            /* Enlarge storage array */
            let reqsize = (2 * (*ima).maxmsgs) as usize;

            (*ima).msgs = repalloc(
                (*ima).msgs as *mut c_void,
                reqsize * core::mem::size_of::<SharedInvalidationMessage>(),
            ) as *mut SharedInvalidationMessage;
            (*ima).maxmsgs = reqsize as c_int;
        }
    }
    /* Okay, add message to current group */
    *(*ima).msgs.add(nextindex as usize) = *msg;
    (*group).nextmsg[subgroup] += 1;
}

/*
 * Append one subgroup of invalidation messages to another, resetting
 * the source subgroup to empty.
 */
unsafe fn AppendInvalidationMessageSubGroup(
    dest: *mut InvalidationMsgsGroup,
    src: *mut InvalidationMsgsGroup,
    subgroup: usize,
) {
    /* Messages must be adjacent in main array */
    Assert!((*dest).nextmsg[subgroup] == (*src).firstmsg[subgroup]);

    /* ... which makes this easy: */
    (*dest).nextmsg[subgroup] = (*src).nextmsg[subgroup];

    /*
     * This is handy for some callers and irrelevant for others.  But we do it
     * always, reasoning that it's bad to leave different groups pointing at
     * the same fragment of the message array.
     */
    SetSubGroupToFollow(src, dest, subgroup);
}

/*
 * Process a subgroup of invalidation messages.
 *
 * This is a macro that executes the given code fragment for each message in
 * a message subgroup.  The fragment should refer to the message as *msg.
 */
macro_rules! ProcessMessageSubGroup {
    ($group:expr, $subgroup:expr, $msg:ident, $codeFragment:block) => {{
        let mut _msgindex = (*$group).firstmsg[$subgroup];
        let _endmsg = (*$group).nextmsg[$subgroup];
        while _msgindex < _endmsg {
            let $msg: *mut SharedInvalidationMessage =
                InvalMessageArrays[$subgroup].msgs.add(_msgindex as usize);
            $codeFragment;
            _msgindex += 1;
        }
    }};
}
use ProcessMessageSubGroup;

/*
 * Process a subgroup of invalidation messages as an array.
 *
 * As above, but the code fragment can handle an array of messages.
 * The fragment should refer to the messages as msgs[], with n entries.
 */
macro_rules! ProcessMessageSubGroupMulti {
    ($group:expr, $subgroup:expr, $msgs:ident, $n:ident, $codeFragment:block) => {{
        let $n = NumMessagesInSubGroup($group, $subgroup);
        if $n > 0 {
            let $msgs: *mut SharedInvalidationMessage = InvalMessageArrays[$subgroup]
                .msgs
                .add((*$group).firstmsg[$subgroup] as usize);
            $codeFragment;
        }
    }};
}
use ProcessMessageSubGroupMulti;

/* ----------------------------------------------------------------
 *				Invalidation group support functions
 *
 * These routines understand about the division of a logical invalidation
 * group into separate physical arrays for catcache and relcache entries.
 * ----------------------------------------------------------------
 */

/*
 * Add a catcache inval entry
 */
unsafe fn AddCatcacheInvalidationMessage(
    group: *mut InvalidationMsgsGroup,
    id: c_int,
    hashValue: uint32,
    dbId: Oid,
) {
    let mut msg: SharedInvalidationMessage = core::mem::zeroed();

    Assert!(id < c_char::MAX as c_int);
    msg.cc.id = id as int8;
    msg.cc.dbId = dbId;
    msg.cc.hashValue = hashValue;

    /*
     * Define padding bytes in SharedInvalidationMessage structs to be
     * defined. Otherwise the sinvaladt.c ringbuffer, which is accessed by
     * multiple processes, will cause spurious valgrind warnings about
     * undefined memory being used.
     */
    VALGRIND_MAKE_MEM_DEFINED!(&msg, core::mem::size_of_val(&msg));

    AddInvalidationMessage(group, CatCacheMsgs, &msg);
}

/*
 * Add a whole-catalog inval entry
 */
unsafe fn AddCatalogInvalidationMessage(group: *mut InvalidationMsgsGroup, dbId: Oid, catId: Oid) {
    let mut msg: SharedInvalidationMessage = core::mem::zeroed();

    msg.cat.id = SHAREDINVALCATALOG_ID;
    msg.cat.dbId = dbId;
    msg.cat.catId = catId;
    /* check AddCatcacheInvalidationMessage() for an explanation */
    VALGRIND_MAKE_MEM_DEFINED!(&msg, core::mem::size_of_val(&msg));

    AddInvalidationMessage(group, CatCacheMsgs, &msg);
}

/*
 * Add a relcache inval entry
 */
unsafe fn AddRelcacheInvalidationMessage(group: *mut InvalidationMsgsGroup, dbId: Oid, relId: Oid) {
    let mut msg: SharedInvalidationMessage = core::mem::zeroed();

    /*
     * Don't add a duplicate item. We assume dbId need not be checked because
     * it will never change. InvalidOid for relId means all relations so we
     * don't need to add individual ones when it is present.
     */
    ProcessMessageSubGroup!(group, RelCacheMsgs, msg, {
        if (*msg).rc.id == SHAREDINVALRELCACHE_ID
            && ((*msg).rc.relId == relId || (*msg).rc.relId == InvalidOid)
        {
            return;
        }
    });

    /* OK, add the item */
    msg.rc.id = SHAREDINVALRELCACHE_ID;
    msg.rc.dbId = dbId;
    msg.rc.relId = relId;
    /* check AddCatcacheInvalidationMessage() for an explanation */
    VALGRIND_MAKE_MEM_DEFINED!(&msg, core::mem::size_of_val(&msg));

    AddInvalidationMessage(group, RelCacheMsgs, &msg);
}

/*
 * Add a relsync inval entry
 *
 * We put these into the relcache subgroup for simplicity. This message is the
 * same as AddRelcacheInvalidationMessage() except that it is for
 * RelationSyncCache maintained by decoding plugin pgoutput.
 */
unsafe fn AddRelsyncInvalidationMessage(group: *mut InvalidationMsgsGroup, dbId: Oid, relId: Oid) {
    let mut msg: SharedInvalidationMessage = core::mem::zeroed();

    /* Don't add a duplicate item. */
    ProcessMessageSubGroup!(group, RelCacheMsgs, msg, {
        if (*msg).rc.id == SHAREDINVALRELSYNC_ID
            && ((*msg).rc.relId == relId || (*msg).rc.relId == InvalidOid)
        {
            return;
        }
    });

    /* OK, add the item */
    msg.rc.id = SHAREDINVALRELSYNC_ID;
    msg.rc.dbId = dbId;
    msg.rc.relId = relId;
    /* check AddCatcacheInvalidationMessage() for an explanation */
    VALGRIND_MAKE_MEM_DEFINED!(&msg, core::mem::size_of_val(&msg));

    AddInvalidationMessage(group, RelCacheMsgs, &msg);
}

/*
 * Add a snapshot inval entry
 *
 * We put these into the relcache subgroup for simplicity.
 */
unsafe fn AddSnapshotInvalidationMessage(
    group: *mut InvalidationMsgsGroup,
    dbId: Oid,
    relId: Oid,
) {
    let mut msg: SharedInvalidationMessage = core::mem::zeroed();

    /* Don't add a duplicate item */
    /* We assume dbId need not be checked because it will never change */
    ProcessMessageSubGroup!(group, RelCacheMsgs, msg, {
        if (*msg).sn.id == SHAREDINVALSNAPSHOT_ID && (*msg).sn.relId == relId {
            return;
        }
    });

    /* OK, add the item */
    msg.sn.id = SHAREDINVALSNAPSHOT_ID;
    msg.sn.dbId = dbId;
    msg.sn.relId = relId;
    /* check AddCatcacheInvalidationMessage() for an explanation */
    VALGRIND_MAKE_MEM_DEFINED!(&msg, core::mem::size_of_val(&msg));

    AddInvalidationMessage(group, RelCacheMsgs, &msg);
}

/*
 * Append one group of invalidation messages to another, resetting
 * the source group to empty.
 */
unsafe fn AppendInvalidationMessages(
    dest: *mut InvalidationMsgsGroup,
    src: *mut InvalidationMsgsGroup,
) {
    AppendInvalidationMessageSubGroup(dest, src, CatCacheMsgs);
    AppendInvalidationMessageSubGroup(dest, src, RelCacheMsgs);
}

/*
 * Execute the given function for all the messages in an invalidation group.
 * The group is not altered.
 *
 * catcache entries are processed first, for reasons mentioned above.
 */
unsafe fn ProcessInvalidationMessages(
    group: *mut InvalidationMsgsGroup,
    func: unsafe fn(msg: *mut SharedInvalidationMessage),
) {
    ProcessMessageSubGroup!(group, CatCacheMsgs, msg, {
        func(msg);
    });
    ProcessMessageSubGroup!(group, RelCacheMsgs, msg, {
        func(msg);
    });
}

/*
 * As above, but the function is able to process an array of messages
 * rather than just one at a time.
 */
unsafe fn ProcessInvalidationMessagesMulti(
    group: *mut InvalidationMsgsGroup,
    func: unsafe fn(msgs: *const SharedInvalidationMessage, n: c_int),
) {
    ProcessMessageSubGroupMulti!(group, CatCacheMsgs, msgs, n, {
        func(msgs, n);
    });
    ProcessMessageSubGroupMulti!(group, RelCacheMsgs, msgs, n, {
        func(msgs, n);
    });
}

/* ----------------------------------------------------------------
 *					  private support functions
 * ----------------------------------------------------------------
 */

/*
 * RegisterCatcacheInvalidation
 *
 * Register an invalidation event for a catcache tuple entry.
 */
unsafe fn RegisterCatcacheInvalidation(
    cacheId: c_int,
    hashValue: uint32,
    dbId: Oid,
    context: *mut c_void,
) {
    let info = context as *mut InvalidationInfo;

    AddCatcacheInvalidationMessage(
        &mut (*info).CurrentCmdInvalidMsgs,
        cacheId,
        hashValue,
        dbId,
    );
}

/*
 * RegisterCatalogInvalidation
 *
 * Register an invalidation event for all catcache entries from a catalog.
 */
unsafe fn RegisterCatalogInvalidation(info: *mut InvalidationInfo, dbId: Oid, catId: Oid) {
    AddCatalogInvalidationMessage(&mut (*info).CurrentCmdInvalidMsgs, dbId, catId);
}

/*
 * RegisterRelcacheInvalidation
 *
 * As above, but register a relcache invalidation event.
 */
unsafe fn RegisterRelcacheInvalidation(info: *mut InvalidationInfo, dbId: Oid, relId: Oid) {
    AddRelcacheInvalidationMessage(&mut (*info).CurrentCmdInvalidMsgs, dbId, relId);

    /*
     * Most of the time, relcache invalidation is associated with system
     * catalog updates, but there are a few cases where it isn't.  Quick hack
     * to ensure that the next CommandCounterIncrement() will think that we
     * need to do CommandEndInvalidationMessages().
     */
    GetCurrentCommandId(true);

    /*
     * If the relation being invalidated is one of those cached in a relcache
     * init file, mark that we need to zap that file at commit. For simplicity
     * invalidations for a specific database always invalidate the shared file
     * as well.  Also zap when we are invalidating whole relcache.
     */
    if relId == InvalidOid || RelationIdIsInInitFile(relId) {
        (*info).RelcacheInitFileInval = true;
    }
}

/*
 * RegisterRelsyncInvalidation
 *
 * As above, but register a relsynccache invalidation event.
 */
unsafe fn RegisterRelsyncInvalidation(info: *mut InvalidationInfo, dbId: Oid, relId: Oid) {
    AddRelsyncInvalidationMessage(&mut (*info).CurrentCmdInvalidMsgs, dbId, relId);
}

/*
 * RegisterSnapshotInvalidation
 *
 * Register an invalidation event for MVCC scans against a given catalog.
 * Only needed for catalogs that don't have catcaches.
 */
unsafe fn RegisterSnapshotInvalidation(info: *mut InvalidationInfo, dbId: Oid, relId: Oid) {
    AddSnapshotInvalidationMessage(&mut (*info).CurrentCmdInvalidMsgs, dbId, relId);
}

/*
 * PrepareInvalidationState
 *		Initialize inval data for the current (sub)transaction.
 */
unsafe fn PrepareInvalidationState() -> *mut InvalidationInfo {
    let myInfo: *mut TransInvalidationInfo;

    /* PrepareToInvalidateCacheTuple() needs relcache */
    AssertCouldGetRelation();
    /* Can't queue transactional message while collecting inplace messages. */
    Assert!(inplaceInvalInfo.is_null());

    if !transInvalInfo.is_null()
        && (*transInvalInfo).my_level == GetCurrentTransactionNestLevel()
    {
        return transInvalInfo as *mut InvalidationInfo;
    }

    myInfo = MemoryContextAllocZero(
        TopTransactionContext,
        core::mem::size_of::<TransInvalidationInfo>(),
    ) as *mut TransInvalidationInfo;
    (*myInfo).parent = transInvalInfo;
    (*myInfo).my_level = GetCurrentTransactionNestLevel();

    /* Now, do we have a previous stack entry? */
    if !transInvalInfo.is_null() {
        /* Yes; this one should be for a deeper nesting level. */
        Assert!((*myInfo).my_level > (*transInvalInfo).my_level);

        /*
         * The parent (sub)transaction must not have any current (i.e.,
         * not-yet-locally-processed) messages.  If it did, we'd have a
         * semantic problem: the new subtransaction presumably ought not be
         * able to see those events yet, but since the CommandCounter is
         * linear, that can't work once the subtransaction advances the
         * counter.
         */
        if NumMessagesInGroup(&(*transInvalInfo).ii.CurrentCmdInvalidMsgs) != 0 {
            elog!(
                ERROR,
                "cannot start a subtransaction when there are unprocessed inval messages"
            );
        }

        /*
         * MemoryContextAllocZero set firstmsg = nextmsg = 0 in each group,
         * which is fine for the first (sub)transaction, but otherwise we need
         * to update them to follow whatever is already in the arrays.
         */
        SetGroupToFollow(
            &mut (*myInfo).PriorCmdInvalidMsgs,
            &mut (*transInvalInfo).ii.CurrentCmdInvalidMsgs,
        );
        SetGroupToFollow(
            &mut (*myInfo).ii.CurrentCmdInvalidMsgs,
            &mut (*myInfo).PriorCmdInvalidMsgs,
        );
    } else {
        /*
         * Here, we need only clear any array pointers left over from a prior
         * transaction.
         */
        InvalMessageArrays[CatCacheMsgs].msgs = null_mut();
        InvalMessageArrays[CatCacheMsgs].maxmsgs = 0;
        InvalMessageArrays[RelCacheMsgs].msgs = null_mut();
        InvalMessageArrays[RelCacheMsgs].maxmsgs = 0;
    }

    transInvalInfo = myInfo;
    myInfo as *mut InvalidationInfo
}

/*
 * PrepareInplaceInvalidationState
 *		Initialize inval data for an inplace update.
 *
 * See previous function for more background.
 */
unsafe fn PrepareInplaceInvalidationState() -> *mut InvalidationInfo {
    let myInfo: *mut InvalidationInfo;

    AssertCouldGetRelation();
    /* limit of one inplace update under assembly */
    Assert!(inplaceInvalInfo.is_null());

    /* gone after WAL insertion CritSection ends, so use current context */
    myInfo = palloc0(core::mem::size_of::<InvalidationInfo>()) as *mut InvalidationInfo;

    /* Stash our messages past end of the transactional messages, if any. */
    if !transInvalInfo.is_null() {
        SetGroupToFollow(
            &mut (*myInfo).CurrentCmdInvalidMsgs,
            &mut (*transInvalInfo).ii.CurrentCmdInvalidMsgs,
        );
    } else {
        InvalMessageArrays[CatCacheMsgs].msgs = null_mut();
        InvalMessageArrays[CatCacheMsgs].maxmsgs = 0;
        InvalMessageArrays[RelCacheMsgs].msgs = null_mut();
        InvalMessageArrays[RelCacheMsgs].maxmsgs = 0;
    }

    inplaceInvalInfo = myInfo;
    myInfo
}

/* ----------------------------------------------------------------
 *					  public functions
 * ----------------------------------------------------------------
 */

pub unsafe fn InvalidateSystemCachesExtended(debug_discard: bool) {
    let mut i: c_int;

    InvalidateCatalogSnapshot();
    ResetCatalogCachesExt(debug_discard);
    RelationCacheInvalidate(debug_discard); /* gets smgr and relmap too */

    i = 0;
    while i < syscache_callback_count {
        let ccitem: *mut SYSCACHECALLBACK = syscache_callback_list.as_mut_ptr().add(i as usize);

        (*ccitem).function.unwrap()((*ccitem).arg, (*ccitem).id as c_int, 0);
        i += 1;
    }

    i = 0;
    while i < relcache_callback_count {
        let ccitem: *mut RELCACHECALLBACK = relcache_callback_list.as_mut_ptr().add(i as usize);

        (*ccitem).function.unwrap()((*ccitem).arg, InvalidOid);
        i += 1;
    }

    i = 0;
    while i < relsync_callback_count {
        let ccitem: *mut RELSYNCCALLBACK = relsync_callback_list.as_mut_ptr().add(i as usize);

        (*ccitem).function.unwrap()((*ccitem).arg, InvalidOid);
        i += 1;
    }
}

/*
 * LocalExecuteInvalidationMessage
 *
 * Process a single invalidation message (which could be of any type).
 * Only the local caches are flushed; this does not transmit the message
 * to other backends.
 */
pub unsafe fn LocalExecuteInvalidationMessage(msg: *mut SharedInvalidationMessage) {
    if (*msg).id >= 0 {
        if (*msg).cc.dbId == MyDatabaseId || (*msg).cc.dbId == InvalidOid {
            InvalidateCatalogSnapshot();

            SysCacheInvalidate((*msg).cc.id as c_int, (*msg).cc.hashValue);

            CallSyscacheCallbacks((*msg).cc.id as c_int, (*msg).cc.hashValue);
        }
    } else if (*msg).id == SHAREDINVALCATALOG_ID {
        if (*msg).cat.dbId == MyDatabaseId || (*msg).cat.dbId == InvalidOid {
            InvalidateCatalogSnapshot();

            CatalogCacheFlushCatalog((*msg).cat.catId);

            /* CatalogCacheFlushCatalog calls CallSyscacheCallbacks as needed */
        }
    } else if (*msg).id == SHAREDINVALRELCACHE_ID {
        if (*msg).rc.dbId == MyDatabaseId || (*msg).rc.dbId == InvalidOid {
            let mut i: c_int;

            if (*msg).rc.relId == InvalidOid {
                RelationCacheInvalidate(false);
            } else {
                RelationCacheInvalidateEntry((*msg).rc.relId);
            }

            i = 0;
            while i < relcache_callback_count {
                let ccitem: *mut RELCACHECALLBACK =
                    relcache_callback_list.as_mut_ptr().add(i as usize);

                (*ccitem).function.unwrap()((*ccitem).arg, (*msg).rc.relId);
                i += 1;
            }
        }
    } else if (*msg).id == SHAREDINVALSMGR_ID {
        /*
         * We could have smgr entries for relations of other databases, so no
         * short-circuit test is possible here.
         */
        let mut rlocator: RelFileLocatorBackend = core::mem::zeroed();

        rlocator.locator = core::mem::transmute((*msg).sm.rlocator);
        rlocator.backend = (((*msg).sm.backend_hi as c_int) << 16) | ((*msg).sm.backend_lo as c_int);
        smgrreleaserellocator(rlocator);
    } else if (*msg).id == SHAREDINVALRELMAP_ID {
        /* We only care about our own database and shared catalogs */
        if (*msg).rm.dbId == InvalidOid {
            RelationMapInvalidate(true);
        } else if (*msg).rm.dbId == MyDatabaseId {
            RelationMapInvalidate(false);
        }
    } else if (*msg).id == SHAREDINVALSNAPSHOT_ID {
        /* We only care about our own database and shared catalogs */
        if (*msg).sn.dbId == InvalidOid {
            InvalidateCatalogSnapshot();
        } else if (*msg).sn.dbId == MyDatabaseId {
            InvalidateCatalogSnapshot();
        }
    } else if (*msg).id == SHAREDINVALRELSYNC_ID {
        /* We only care about our own database */
        if (*msg).rs.dbId == MyDatabaseId {
            CallRelSyncCallbacks((*msg).rs.relid);
        }
    } else {
        elog!(FATAL, "unrecognized SI message ID: {}", (*msg).id);
    }
}

/*
 *		InvalidateSystemCaches
 *
 *		This blows away all tuples in the system catalog caches and
 *		all the cached relation descriptors and smgr cache entries.
 *		Relation descriptors that have positive refcounts are then rebuilt.
 *
 *		We call this when we see a shared-inval-queue overflow signal,
 *		since that tells us we've lost some shared-inval messages and hence
 *		don't know what needs to be invalidated.
 */
pub unsafe fn InvalidateSystemCaches() {
    InvalidateSystemCachesExtended(false);
}

// extern "C" trampolines for passing the above as function pointers to
// ReceiveSharedInvalidMessages (which takes extern "C" fn pointers).
unsafe extern "C" fn LocalExecuteInvalidationMessage_c(msg: *mut SharedInvalidationMessage) {
    LocalExecuteInvalidationMessage(msg);
}

unsafe extern "C" fn InvalidateSystemCaches_c() {
    InvalidateSystemCaches();
}

/*
 * AcceptInvalidationMessages
 *		Read and process invalidation messages from the shared invalidation
 *		message queue.
 *
 * Note:
 *		This should be called as the first step in processing a transaction.
 */
pub unsafe fn AcceptInvalidationMessages() {
    // #ifdef USE_ASSERT_CHECKING
    /* message handlers shall access catalogs only during transactions */
    if IsTransactionState() {
        AssertCouldGetRelation();
    }
    // #endif

    ReceiveSharedInvalidMessages(LocalExecuteInvalidationMessage_c, InvalidateSystemCaches_c);

    /*----------
     * Test code to force cache flushes anytime a flush could happen.
     *
     * This helps detect intermittent faults caused by code that reads a cache
     * entry and then performs an action that could invalidate the entry, but
     * rarely actually does so.  The default debug_discard_caches = 0 does no
     * forced cache flushes.
     *----------
     */
    // #ifdef DISCARD_CACHES_ENABLED
    {
        static mut recursion_depth: c_int = 0;

        if recursion_depth < debug_discard_caches {
            recursion_depth += 1;
            InvalidateSystemCachesExtended(true);
            recursion_depth -= 1;
        }
    }
    // #endif
}

/*
 * PostPrepare_Inval
 *		Clean up after successful PREPARE.
 *
 * Here, we want to act as though the transaction aborted, so that we will
 * undo any syscache changes it made, thereby bringing us into sync with the
 * outside world, which doesn't believe the transaction committed yet.
 */
pub unsafe fn PostPrepare_Inval() {
    AtEOXact_Inval(false);
}

/*
 * xactGetCommittedInvalidationMessages() is called by
 * RecordTransactionCommit() to collect invalidation messages to add to the
 * commit record. This applies only to commit message types, never to
 * abort records. Must always run before AtEOXact_Inval(), since that
 * removes the data we need to see.
 */
pub unsafe fn xactGetCommittedInvalidationMessages(
    msgs: *mut *mut SharedInvalidationMessage,
    RelcacheInitFileInval: *mut bool,
) -> c_int {
    let msgarray: *mut SharedInvalidationMessage;
    let nummsgs: c_int;
    let mut nmsgs: c_int;

    /* Quick exit if we haven't done anything with invalidation messages. */
    if transInvalInfo.is_null() {
        *RelcacheInitFileInval = false;
        *msgs = null_mut();
        return 0;
    }

    /* Must be at top of stack */
    Assert!((*transInvalInfo).my_level == 1 && (*transInvalInfo).parent.is_null());

    /*
     * Relcache init file invalidation requires processing both before and
     * after we send the SI messages.  However, we need not do anything unless
     * we committed.
     */
    *RelcacheInitFileInval = (*transInvalInfo).ii.RelcacheInitFileInval;

    /*
     * Collect all the pending messages into a single contiguous array of
     * invalidation messages, to simplify what needs to happen while building
     * the commit WAL message.
     */
    nummsgs = NumMessagesInGroup(&(*transInvalInfo).PriorCmdInvalidMsgs)
        + NumMessagesInGroup(&(*transInvalInfo).ii.CurrentCmdInvalidMsgs);

    msgarray = MemoryContextAlloc(
        CurTransactionContext as crate::utils::mmgr::memnodes::MemoryContext,
        (nummsgs as usize) * core::mem::size_of::<SharedInvalidationMessage>(),
    ) as *mut SharedInvalidationMessage;
    *msgs = msgarray;

    nmsgs = 0;
    ProcessMessageSubGroupMulti!(
        &mut (*transInvalInfo).PriorCmdInvalidMsgs,
        CatCacheMsgs,
        msgs,
        n,
        {
            core::ptr::copy_nonoverlapping(msgs, msgarray.add(nmsgs as usize), n as usize);
            nmsgs += n;
        }
    );
    ProcessMessageSubGroupMulti!(
        &mut (*transInvalInfo).ii.CurrentCmdInvalidMsgs,
        CatCacheMsgs,
        msgs,
        n,
        {
            core::ptr::copy_nonoverlapping(msgs, msgarray.add(nmsgs as usize), n as usize);
            nmsgs += n;
        }
    );
    ProcessMessageSubGroupMulti!(
        &mut (*transInvalInfo).PriorCmdInvalidMsgs,
        RelCacheMsgs,
        msgs,
        n,
        {
            core::ptr::copy_nonoverlapping(msgs, msgarray.add(nmsgs as usize), n as usize);
            nmsgs += n;
        }
    );
    ProcessMessageSubGroupMulti!(
        &mut (*transInvalInfo).ii.CurrentCmdInvalidMsgs,
        RelCacheMsgs,
        msgs,
        n,
        {
            core::ptr::copy_nonoverlapping(msgs, msgarray.add(nmsgs as usize), n as usize);
            nmsgs += n;
        }
    );
    Assert!(nmsgs == nummsgs);

    nmsgs
}

/*
 * inplaceGetInvalidationMessages() is called by the inplace update to collect
 * invalidation messages to add to its WAL record.  Like the previous
 * function, we might still fail.
 */
pub unsafe fn inplaceGetInvalidationMessages(
    msgs: *mut *mut SharedInvalidationMessage,
    RelcacheInitFileInval: *mut bool,
) -> c_int {
    let msgarray: *mut SharedInvalidationMessage;
    let nummsgs: c_int;
    let mut nmsgs: c_int;

    /* Quick exit if we haven't done anything with invalidation messages. */
    if inplaceInvalInfo.is_null() {
        *RelcacheInitFileInval = false;
        *msgs = null_mut();
        return 0;
    }

    *RelcacheInitFileInval = (*inplaceInvalInfo).RelcacheInitFileInval;
    nummsgs = NumMessagesInGroup(&(*inplaceInvalInfo).CurrentCmdInvalidMsgs);
    msgarray = palloc((nummsgs as usize) * core::mem::size_of::<SharedInvalidationMessage>())
        as *mut SharedInvalidationMessage;
    *msgs = msgarray;

    nmsgs = 0;
    ProcessMessageSubGroupMulti!(
        &mut (*inplaceInvalInfo).CurrentCmdInvalidMsgs,
        CatCacheMsgs,
        msgs,
        n,
        {
            core::ptr::copy_nonoverlapping(msgs, msgarray.add(nmsgs as usize), n as usize);
            nmsgs += n;
        }
    );
    ProcessMessageSubGroupMulti!(
        &mut (*inplaceInvalInfo).CurrentCmdInvalidMsgs,
        RelCacheMsgs,
        msgs,
        n,
        {
            core::ptr::copy_nonoverlapping(msgs, msgarray.add(nmsgs as usize), n as usize);
            nmsgs += n;
        }
    );
    Assert!(nmsgs == nummsgs);

    nmsgs
}

/*
 * ProcessCommittedInvalidationMessages is executed by xact_redo_commit() or
 * standby_redo() to process invalidation messages. Currently that happens
 * only at end-of-xact.
 *
 * Relcache init file invalidation requires processing both
 * before and after we send the SI messages. See AtEOXact_Inval()
 */
pub unsafe fn ProcessCommittedInvalidationMessages(
    msgs: *mut SharedInvalidationMessage,
    nmsgs: c_int,
    RelcacheInitFileInval: bool,
    dbid: Oid,
    tsid: Oid,
) {
    if nmsgs <= 0 {
        return;
    }

    elog!(
        DEBUG4,
        "replaying commit with {} messages{}",
        nmsgs,
        if RelcacheInitFileInval {
            " and relcache file invalidation"
        } else {
            ""
        }
    );

    if RelcacheInitFileInval {
        elog!(DEBUG4, "removing relcache init files for database {}", dbid);

        /*
         * RelationCacheInitFilePreInvalidate, when the invalidation message
         * is for a specific database, requires DatabasePath to be set, but we
         * should not use SetDatabasePath during recovery, since it is
         * intended to be used only once by normal backends.  Hence, a quick
         * hack: set DatabasePath directly then unset after use.
         */
        if OidIsValid(dbid) {
            DatabasePath = GetDatabasePath(dbid, tsid);
        }

        RelationCacheInitFilePreInvalidate();

        if OidIsValid(dbid) {
            pfree(DatabasePath as *mut c_void);
            DatabasePath = null_mut();
        }
    }

    SendSharedInvalidMessages(msgs, nmsgs);

    if RelcacheInitFileInval {
        RelationCacheInitFilePostInvalidate();
    }
}

/*
 * AtEOXact_Inval
 *		Process queued-up invalidation messages at end of main transaction.
 *
 * If isCommit, we must send out the messages in our PriorCmdInvalidMsgs list
 * to the shared invalidation message queue.  Note that these will be read
 * not only by other backends, but also by our own backend at the next
 * transaction start (via AcceptInvalidationMessages).  This means that
 * we can skip immediate local processing of anything that's still in
 * CurrentCmdInvalidMsgs, and just send that list out too.
 *
 * If not isCommit, we are aborting, and must locally process the messages
 * in PriorCmdInvalidMsgs.  No messages need be sent to other backends,
 * since they'll not have seen our changed tuples anyway.
 *
 * Note:
 *		This should be called as the last step in processing a transaction.
 */
pub unsafe fn AtEOXact_Inval(isCommit: bool) {
    inplaceInvalInfo = null_mut();

    /* Quick exit if no transactional messages */
    if transInvalInfo.is_null() {
        return;
    }

    /* Must be at top of stack */
    Assert!((*transInvalInfo).my_level == 1 && (*transInvalInfo).parent.is_null());

    INJECTION_POINT!("transaction-end-process-inval", null_mut::<c_void>());

    if isCommit {
        /*
         * Relcache init file invalidation requires processing both before and
         * after we send the SI messages.  However, we need not do anything
         * unless we committed.
         */
        if (*transInvalInfo).ii.RelcacheInitFileInval {
            RelationCacheInitFilePreInvalidate();
        }

        AppendInvalidationMessages(
            &mut (*transInvalInfo).PriorCmdInvalidMsgs,
            &mut (*transInvalInfo).ii.CurrentCmdInvalidMsgs,
        );

        ProcessInvalidationMessagesMulti(
            &mut (*transInvalInfo).PriorCmdInvalidMsgs,
            SendSharedInvalidMessages,
        );

        if (*transInvalInfo).ii.RelcacheInitFileInval {
            RelationCacheInitFilePostInvalidate();
        }
    } else {
        ProcessInvalidationMessages(
            &mut (*transInvalInfo).PriorCmdInvalidMsgs,
            LocalExecuteInvalidationMessage,
        );
    }

    /* Need not free anything explicitly */
    transInvalInfo = null_mut();
}

/*
 * PreInplace_Inval
 *		Process queued-up invalidation before inplace update critical section.
 *
 * Tasks belong here if they are safe even if the inplace update does not
 * complete.  Currently, this just unlinks a cache file, which can fail.  The
 * sum of this and AtInplace_Inval() mirrors AtEOXact_Inval(isCommit=true).
 */
pub unsafe fn PreInplace_Inval() {
    Assert!(CritSectionCount == 0);

    if !inplaceInvalInfo.is_null() && (*inplaceInvalInfo).RelcacheInitFileInval {
        RelationCacheInitFilePreInvalidate();
    }
}

/*
 * AtInplace_Inval
 *		Process queued-up invalidations after inplace update buffer mutation.
 */
pub unsafe fn AtInplace_Inval() {
    Assert!(CritSectionCount > 0);

    if inplaceInvalInfo.is_null() {
        return;
    }

    ProcessInvalidationMessagesMulti(
        &mut (*inplaceInvalInfo).CurrentCmdInvalidMsgs,
        SendSharedInvalidMessages,
    );

    if (*inplaceInvalInfo).RelcacheInitFileInval {
        RelationCacheInitFilePostInvalidate();
    }

    inplaceInvalInfo = null_mut();
}

/*
 * ForgetInplace_Inval
 *		Alternative to PreInplace_Inval()+AtInplace_Inval(): discard queued-up
 *		invalidations.  This lets inplace update enumerate invalidations
 *		optimistically, before locking the buffer.
 */
pub unsafe fn ForgetInplace_Inval() {
    inplaceInvalInfo = null_mut();
}

/*
 * AtEOSubXact_Inval
 *		Process queued-up invalidation messages at end of subtransaction.
 *
 * If isCommit, process CurrentCmdInvalidMsgs if any (there probably aren't),
 * and then attach both CurrentCmdInvalidMsgs and PriorCmdInvalidMsgs to the
 * parent's PriorCmdInvalidMsgs list.
 *
 * If not isCommit, we are aborting, and must locally process the messages
 * in PriorCmdInvalidMsgs.  No messages need be sent to other backends.
 */
pub unsafe fn AtEOSubXact_Inval(isCommit: bool) {
    let my_level: c_int;
    let myInfo: *mut TransInvalidationInfo;

    /*
     * Successful inplace update must clear this, but we clear it on abort.
     * Inplace updates allocate this in CurrentMemoryContext, which has
     * lifespan <= subtransaction lifespan.  Hence, don't free it explicitly.
     */
    if isCommit {
        Assert!(inplaceInvalInfo.is_null());
    } else {
        inplaceInvalInfo = null_mut();
    }

    /* Quick exit if no transactional messages. */
    myInfo = transInvalInfo;
    if myInfo.is_null() {
        return;
    }

    /* Also bail out quickly if messages are not for this level. */
    my_level = GetCurrentTransactionNestLevel();
    if (*myInfo).my_level != my_level {
        Assert!((*myInfo).my_level < my_level);
        return;
    }

    if isCommit {
        /* If CurrentCmdInvalidMsgs still has anything, fix it */
        CommandEndInvalidationMessages();

        /*
         * We create invalidation stack entries lazily, so the parent might
         * not have one.  Instead of creating one, moving all the data over,
         * and then freeing our own, we can just adjust the level of our own
         * entry.
         */
        if (*myInfo).parent.is_null() || (*(*myInfo).parent).my_level < my_level - 1 {
            (*myInfo).my_level -= 1;
            return;
        }

        /*
         * Pass up my inval messages to parent.  Notice that we stick them in
         * PriorCmdInvalidMsgs, not CurrentCmdInvalidMsgs, since they've
         * already been locally processed.
         */
        AppendInvalidationMessages(
            &mut (*(*myInfo).parent).PriorCmdInvalidMsgs,
            &mut (*myInfo).PriorCmdInvalidMsgs,
        );

        /* Must readjust parent's CurrentCmdInvalidMsgs indexes now */
        SetGroupToFollow(
            &mut (*(*myInfo).parent).ii.CurrentCmdInvalidMsgs,
            &mut (*(*myInfo).parent).PriorCmdInvalidMsgs,
        );

        /* Pending relcache inval becomes parent's problem too */
        if (*myInfo).ii.RelcacheInitFileInval {
            (*(*myInfo).parent).ii.RelcacheInitFileInval = true;
        }

        /* Pop the transaction state stack */
        transInvalInfo = (*myInfo).parent;

        /* Need not free anything else explicitly */
        pfree(myInfo as *mut c_void);
    } else {
        ProcessInvalidationMessages(
            &mut (*myInfo).PriorCmdInvalidMsgs,
            LocalExecuteInvalidationMessage,
        );

        /* Pop the transaction state stack */
        transInvalInfo = (*myInfo).parent;

        /* Need not free anything else explicitly */
        pfree(myInfo as *mut c_void);
    }
}

/*
 * CommandEndInvalidationMessages
 *		Process queued-up invalidation messages at end of one command
 *		in a transaction.
 *
 * Here, we send no messages to the shared queue, since we don't know yet if
 * we will commit.  We do need to locally process the CurrentCmdInvalidMsgs
 * list, so as to flush our caches of any entries we have outdated in the
 * current command.  We then move the current-cmd list over to become part
 * of the prior-cmds list.
 *
 * Note:
 *		This should be called during CommandCounterIncrement(),
 *		after we have advanced the command ID.
 */
pub unsafe fn CommandEndInvalidationMessages() {
    /*
     * You might think this shouldn't be called outside any transaction, but
     * bootstrap does it, and also ABORT issued when not in a transaction. So
     * just quietly return if no state to work on.
     */
    if std::env::var("PDB_BT").is_ok() {
        eprintln!("PDB_BT CommandEndInvalidationMessages transInvalInfo_null={}", transInvalInfo.is_null());
    }
    if transInvalInfo.is_null() {
        return;
    }

    ProcessInvalidationMessages(
        &mut (*transInvalInfo).ii.CurrentCmdInvalidMsgs,
        LocalExecuteInvalidationMessage,
    );

    /* WAL Log per-command invalidation messages for wal_level=logical */
    if XLogLogicalInfoActive() {
        LogLogicalInvalidations();
    }

    AppendInvalidationMessages(
        &mut (*transInvalInfo).PriorCmdInvalidMsgs,
        &mut (*transInvalInfo).ii.CurrentCmdInvalidMsgs,
    );
}

/*
 * CacheInvalidateHeapTupleCommon
 *		Common logic for end-of-command and inplace variants.
 */
unsafe fn CacheInvalidateHeapTupleCommon(
    relation: Relation,
    tuple: HeapTuple,
    newtuple: HeapTuple,
    prepare_callback: unsafe fn() -> *mut InvalidationInfo,
) {
    let info: *mut InvalidationInfo;
    let tupleRelId: Oid;
    let mut databaseId: Oid;
    let relationId: Oid;

    /* PrepareToInvalidateCacheTuple() needs relcache */
    AssertCouldGetRelation();

    /* Do nothing during bootstrap */
    if IsBootstrapProcessingMode() {
        return;
    }

    /*
     * We only need to worry about invalidation for tuples that are in system
     * catalogs; user-relation tuples are never in catcaches and can't affect
     * the relcache either.
     */
    if !IsCatalogRelation(relation) {
        return;
    }

    /*
     * IsCatalogRelation() will return true for TOAST tables of system
     * catalogs, but we don't care about those, either.
     */
    if IsToastRelation(relation) {
        return;
    }

    /* Allocate any required resources. */
    info = prepare_callback();

    /*
     * First let the catcache do its thing
     */
    tupleRelId = RelationGetRelid(relation);
    if RelationInvalidatesSnapshotsOnly(tupleRelId) {
        databaseId = if IsSharedRelation(tupleRelId) {
            InvalidOid
        } else {
            MyDatabaseId
        };
        RegisterSnapshotInvalidation(info, databaseId, tupleRelId);
    } else {
        PrepareToInvalidateCacheTuple(
            relation as *mut c_void,
            tuple,
            newtuple,
            Some(RegisterCatcacheInvalidation),
            info as *mut c_void,
        );
    }

    /*
     * Now, is this tuple one of the primary definers of a relcache entry? See
     * comments in file header for deeper explanation.
     *
     * Note we ignore newtuple here; we assume an update cannot move a tuple
     * from being part of one relcache entry to being part of another.
     */
    if tupleRelId == RelationRelationId {
        let classtup: Form_pg_class = GETSTRUCT(tuple) as Form_pg_class;

        relationId = (*classtup).oid;
        if (*classtup).relisshared {
            databaseId = InvalidOid;
        } else {
            databaseId = MyDatabaseId;
        }
    } else if tupleRelId == AttributeRelationId {
        let atttup: Form_pg_attribute = GETSTRUCT(tuple) as Form_pg_attribute;

        relationId = (*atttup).attrelid;

        /*
         * KLUGE ALERT: we always send the relcache event with MyDatabaseId,
         * even if the rel in question is shared (which we can't easily tell).
         * This essentially means that only backends in this same database
         * will react to the relcache flush request.  This is in fact
         * appropriate, since only those backends could see our pg_attribute
         * change anyway.
         */
        databaseId = MyDatabaseId;
    } else if tupleRelId == IndexRelationId {
        let indextup: Form_pg_index = GETSTRUCT(tuple) as Form_pg_index;

        /*
         * When a pg_index row is updated, we should send out a relcache inval
         * for the index relation.  As above, we don't know the shared status
         * of the index, but in practice it doesn't matter since indexes of
         * shared catalogs can't have such updates.
         */
        relationId = (*indextup).indexrelid;
        databaseId = MyDatabaseId;
    } else if tupleRelId == ConstraintRelationId {
        let constrtup: Form_pg_constraint = GETSTRUCT(tuple) as Form_pg_constraint;

        /*
         * Foreign keys are part of relcache entries, too, so send out an
         * inval for the table that the FK applies to.
         */
        if (*constrtup).contype == CONSTRAINT_FOREIGN && OidIsValid((*constrtup).conrelid) {
            relationId = (*constrtup).conrelid;
            databaseId = MyDatabaseId;
        } else {
            return;
        }
    } else {
        return;
    }

    /*
     * Yes.  We need to register a relcache invalidation event.
     */
    RegisterRelcacheInvalidation(info, databaseId, relationId);
}

/*
 * CacheInvalidateHeapTuple
 *		Register the given tuple for invalidation at end of command
 *		(ie, current command is creating or outdating this tuple) and end of
 *		transaction.  Also, detect whether a relcache invalidation is implied.
 *
 * For an insert or delete, tuple is the target tuple and newtuple is NULL.
 * For an update, we are called just once, with tuple being the old tuple
 * version and newtuple the new version.  This allows avoidance of duplicate
 * effort during an update.
 */
pub unsafe fn CacheInvalidateHeapTuple(relation: Relation, tuple: HeapTuple, newtuple: HeapTuple) {
    CacheInvalidateHeapTupleCommon(relation, tuple, newtuple, PrepareInvalidationState);
}

/*
 * CacheInvalidateHeapTupleInplace
 *		Register the given tuple for nontransactional invalidation pertaining
 *		to an inplace update.  Also, detect whether a relcache invalidation is
 *		implied.
 *
 * Like CacheInvalidateHeapTuple(), but for inplace updates.
 */
pub unsafe fn CacheInvalidateHeapTupleInplace(relation: Relation, key_equivalent_tuple: HeapTuple) {
    CacheInvalidateHeapTupleCommon(
        relation,
        key_equivalent_tuple,
        null_mut(),
        PrepareInplaceInvalidationState,
    );
}

/*
 * CacheInvalidateCatalog
 *		Register invalidation of the whole content of a system catalog.
 *
 * This is normally used in VACUUM FULL/CLUSTER, where we haven't so much
 * changed any tuples as moved them around.  Some uses of catcache entries
 * expect their TIDs to be correct, so we have to blow away the entries.
 */
pub unsafe fn CacheInvalidateCatalog(catalogId: Oid) {
    let databaseId: Oid;

    if IsSharedRelation(catalogId) {
        databaseId = InvalidOid;
    } else {
        databaseId = MyDatabaseId;
    }

    RegisterCatalogInvalidation(PrepareInvalidationState(), databaseId, catalogId);
}

/*
 * CacheInvalidateRelcache
 *		Register invalidation of the specified relation's relcache entry
 *		at end of command.
 *
 * This is used in places that need to force relcache rebuild but aren't
 * changing any of the tuples recognized as contributors to the relcache
 * entry by CacheInvalidateHeapTuple.  (An example is dropping an index.)
 */
pub unsafe fn CacheInvalidateRelcache(relation: Relation) {
    let databaseId: Oid;
    let relationId: Oid;

    relationId = RelationGetRelid(relation);
    if (*(*relation).rd_rel).relisshared {
        databaseId = InvalidOid;
    } else {
        databaseId = MyDatabaseId;
    }

    RegisterRelcacheInvalidation(PrepareInvalidationState(), databaseId, relationId);
}

/*
 * CacheInvalidateRelcacheAll
 *		Register invalidation of the whole relcache at the end of command.
 *
 * This is used by alter publication as changes in publications may affect
 * large number of tables.
 */
pub unsafe fn CacheInvalidateRelcacheAll() {
    RegisterRelcacheInvalidation(PrepareInvalidationState(), InvalidOid, InvalidOid);
}

/*
 * CacheInvalidateRelcacheByTuple
 *		As above, but relation is identified by passing its pg_class tuple.
 */
pub unsafe fn CacheInvalidateRelcacheByTuple(classTuple: HeapTuple) {
    let classtup: Form_pg_class = GETSTRUCT(classTuple) as Form_pg_class;
    let databaseId: Oid;
    let relationId: Oid;

    relationId = (*classtup).oid;
    if (*classtup).relisshared {
        databaseId = InvalidOid;
    } else {
        databaseId = MyDatabaseId;
    }
    RegisterRelcacheInvalidation(PrepareInvalidationState(), databaseId, relationId);
}

/*
 * CacheInvalidateRelcacheByRelid
 *		As above, but relation is identified by passing its OID.
 *		This is the least efficient of the three options; use one of
 *		the above routines if you have a Relation or pg_class tuple.
 */
pub unsafe fn CacheInvalidateRelcacheByRelid(relid: Oid) {
    let tup: HeapTuple;

    tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for relation {}", relid);
    }
    CacheInvalidateRelcacheByTuple(tup);
    ReleaseSysCache(tup);
}

/*
 * CacheInvalidateRelSync
 *		Register invalidation of the cache in logical decoding output plugin
 *		for a database.
 *
 * This type of invalidation message is used for the specific purpose of output
 * plugins. Processes which do not decode WALs would do nothing even when it
 * receives the message.
 */
pub unsafe fn CacheInvalidateRelSync(relid: Oid) {
    RegisterRelsyncInvalidation(PrepareInvalidationState(), MyDatabaseId, relid);
}

/*
 * CacheInvalidateRelSyncAll
 *		Register invalidation of the whole cache in logical decoding output
 *		plugin.
 */
pub unsafe fn CacheInvalidateRelSyncAll() {
    CacheInvalidateRelSync(InvalidOid);
}

/*
 * CacheInvalidateSmgr
 *		Register invalidation of smgr references to a physical relation.
 *
 * Sending this type of invalidation msg forces other backends to close open
 * smgr entries for the rel.  This should be done to flush dangling open-file
 * references when the physical rel is being dropped or truncated.  Because
 * these are nontransactional (i.e., not-rollback-able) operations, we just
 * send the inval message immediately without any queuing.
 *
 * Note: In order to avoid bloating SharedInvalidationMessage, we store only
 * three bytes of the ProcNumber using what would otherwise be padding space.
 * Thus, the maximum possible ProcNumber is 2^23-1.
 */
pub unsafe fn CacheInvalidateSmgr(rlocator: RelFileLocatorBackend) {
    let mut msg: SharedInvalidationMessage = core::mem::zeroed();

    /* verify optimization stated above stays valid */
    StaticAssertStmt!(MAX_BACKENDS_BITS <= 23, "MAX_BACKENDS_BITS is too big for inval.c");

    msg.sm.id = SHAREDINVALSMGR_ID;
    msg.sm.backend_hi = (rlocator.backend >> 16) as int8;
    msg.sm.backend_lo = (rlocator.backend & 0xffff) as uint16;
    msg.sm.rlocator = core::mem::transmute(rlocator.locator);
    /* check AddCatcacheInvalidationMessage() for an explanation */
    VALGRIND_MAKE_MEM_DEFINED!(&msg, core::mem::size_of_val(&msg));

    SendSharedInvalidMessages(&msg, 1);
}

/*
 * CacheInvalidateRelmap
 *		Register invalidation of the relation mapping for a database,
 *		or for the shared catalogs if databaseId is zero.
 *
 * Sending this type of invalidation msg forces other backends to re-read
 * the indicated relation mapping file.  It is also necessary to send a
 * relcache inval for the specific relations whose mapping has been altered,
 * else the relcache won't get updated with the new filenode data.
 */
pub unsafe fn CacheInvalidateRelmap(databaseId: Oid) {
    let mut msg: SharedInvalidationMessage = core::mem::zeroed();

    msg.rm.id = SHAREDINVALRELMAP_ID;
    msg.rm.dbId = databaseId;
    /* check AddCatcacheInvalidationMessage() for an explanation */
    VALGRIND_MAKE_MEM_DEFINED!(&msg, core::mem::size_of_val(&msg));

    SendSharedInvalidMessages(&msg, 1);
}

/*
 * CacheRegisterSyscacheCallback
 *		Register the specified function to be called for all future
 *		invalidation events in the specified cache.  The cache ID and the
 *		hash value of the tuple being invalidated will be passed to the
 *		function.
 *
 * NOTE: Hash value zero will be passed if a cache reset request is received.
 * In this case the called routines should flush all cached state.
 */
#[no_mangle]
pub unsafe extern "C" fn CacheRegisterSyscacheCallback(
    cacheid: c_int,
    func: SyscacheCallbackFunction,
    arg: Datum,
) {
    if cacheid < 0 || cacheid >= SysCacheSize {
        elog!(FATAL, "invalid cache ID: {}", cacheid);
    }
    if syscache_callback_count >= MAX_SYSCACHE_CALLBACKS as c_int {
        elog!(FATAL, "out of syscache_callback_list slots");
    }

    if syscache_callback_links[cacheid as usize] == 0 {
        /* first callback for this cache */
        syscache_callback_links[cacheid as usize] = (syscache_callback_count + 1) as int16;
    } else {
        /* add to end of chain, so that older callbacks are called first */
        let mut i = (syscache_callback_links[cacheid as usize] - 1) as usize;

        while syscache_callback_list[i].link > 0 {
            i = (syscache_callback_list[i].link - 1) as usize;
        }
        syscache_callback_list[i].link = (syscache_callback_count + 1) as int16;
    }

    syscache_callback_list[syscache_callback_count as usize].id = cacheid as int16;
    syscache_callback_list[syscache_callback_count as usize].link = 0;
    syscache_callback_list[syscache_callback_count as usize].function = Some(func);
    syscache_callback_list[syscache_callback_count as usize].arg = arg;

    syscache_callback_count += 1;
}

/*
 * CacheRegisterRelcacheCallback
 *		Register the specified function to be called for all future
 *		relcache invalidation events.  The OID of the relation being
 *		invalidated will be passed to the function.
 *
 * NOTE: InvalidOid will be passed if a cache reset request is received.
 * In this case the called routines should flush all cached state.
 */
#[no_mangle]
pub unsafe extern "C" fn CacheRegisterRelcacheCallback(func: RelcacheCallbackFunction, arg: Datum) {
    if relcache_callback_count >= MAX_RELCACHE_CALLBACKS as c_int {
        elog!(FATAL, "out of relcache_callback_list slots");
    }

    relcache_callback_list[relcache_callback_count as usize].function = Some(func);
    relcache_callback_list[relcache_callback_count as usize].arg = arg;

    relcache_callback_count += 1;
}

/*
 * CacheRegisterRelSyncCallback
 *		Register the specified function to be called for all future
 *		relsynccache invalidation events.
 *
 * This function is intended to be call from the logical decoding output
 * plugins.
 */
pub unsafe fn CacheRegisterRelSyncCallback(func: RelSyncCallbackFunction, arg: Datum) {
    if relsync_callback_count >= MAX_RELSYNC_CALLBACKS as c_int {
        elog!(FATAL, "out of relsync_callback_list slots");
    }

    relsync_callback_list[relsync_callback_count as usize].function = Some(func);
    relsync_callback_list[relsync_callback_count as usize].arg = arg;

    relsync_callback_count += 1;
}

/*
 * CallSyscacheCallbacks
 *
 * This is exported so that CatalogCacheFlushCatalog can call it, saving
 * this module from knowing which catcache IDs correspond to which catalogs.
 */
pub unsafe fn CallSyscacheCallbacks(cacheid: c_int, hashvalue: uint32) {
    let mut i: c_int;

    if cacheid < 0 || cacheid >= SysCacheSize {
        elog!(ERROR, "invalid cache ID: {}", cacheid);
    }

    i = (syscache_callback_links[cacheid as usize] - 1) as c_int;
    while i >= 0 {
        let ccitem: *mut SYSCACHECALLBACK = syscache_callback_list.as_mut_ptr().add(i as usize);

        Assert!((*ccitem).id as c_int == cacheid);
        (*ccitem).function.unwrap()((*ccitem).arg, cacheid, hashvalue);
        i = ((*ccitem).link - 1) as c_int;
    }
}

/*
 * CallRelSyncCallbacks
 */
pub unsafe fn CallRelSyncCallbacks(relid: Oid) {
    let mut i: c_int = 0;
    while i < relsync_callback_count {
        let ccitem: *mut RELSYNCCALLBACK = relsync_callback_list.as_mut_ptr().add(i as usize);

        (*ccitem).function.unwrap()((*ccitem).arg, relid);
        i += 1;
    }
}

/*
 * LogLogicalInvalidations
 *
 * Emit WAL for invalidations caused by the current command.
 *
 * This is currently only used for logging invalidations at the command end
 * or at commit time if any invalidations are pending.
 */
pub unsafe fn LogLogicalInvalidations() {
    let mut xlrec: xl_xact_invals = core::mem::zeroed();
    let group: *mut InvalidationMsgsGroup;
    let nmsgs: c_int;

    /* Quick exit if we haven't done anything with invalidation messages. */
    if transInvalInfo.is_null() {
        return;
    }

    group = &mut (*transInvalInfo).ii.CurrentCmdInvalidMsgs;
    nmsgs = NumMessagesInGroup(group);

    if nmsgs > 0 {
        /* prepare record */
        core::ptr::write_bytes(&mut xlrec as *mut xl_xact_invals as *mut u8, 0, MinSizeOfXactInvals);
        xlrec.nmsgs = nmsgs;

        /* perform insertion */
        XLogBeginInsert();
        XLogRegisterData(&xlrec as *const xl_xact_invals as *const c_void, MinSizeOfXactInvals as u32);
        ProcessMessageSubGroupMulti!(group, CatCacheMsgs, msgs, n, {
            XLogRegisterData(
                msgs as *const c_void,
                (n as usize * core::mem::size_of::<SharedInvalidationMessage>()) as u32,
            );
        });
        ProcessMessageSubGroupMulti!(group, RelCacheMsgs, msgs, n, {
            XLogRegisterData(
                msgs as *const c_void,
                (n as usize * core::mem::size_of::<SharedInvalidationMessage>()) as u32,
            );
        });
        XLogInsert(RM_XACT_ID, XLOG_XACT_INVALIDATIONS);
    }
}

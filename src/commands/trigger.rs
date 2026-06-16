//! Partial translation of postgres/src/include/commands/trigger.h
//!
//! The trigger-manager call interface: the `TriggerData` node a trigger function
//! receives via `fcinfo->context`, the `TRIGGER_EVENT_*` bits, and the
//! `TRIGGER_FIRED_*` / `CALLED_AS_TRIGGER` test macros.  Only what the builtin
//! trigger support functions (utils/adt/trigfuncs.c) need is translated; the
//! Trigger relcache entry and the trigger-firing machinery are future work.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::access::htup_details::HeapTuple;
use crate::c::uint32;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::execnodes::Relation;
use crate::nodes::nodes::NodeTag;
use crate::executor::tuptable::TupleTableSlot;
use crate::utils::fmgr::FunctionCallInfo;
use core::ffi::c_void;

// --- backend/commands/trigger.c imports ---
use crate::c::{CommandId, Size, MAXALIGN, Min, OidIsValid};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::nodes::nodes::CmdType;
use crate::nodes::pg_list::List;
use crate::nodes::bitmapset::{bms_copy, bms_equal};
use crate::utils::palloc::{pfree, MemoryContext, MemoryContextSwitchTo, MemoryContextAlloc};
use crate::miscadmin::InSecurityRestrictedOperation;

/// The bitmask describing which trigger event fired (see TRIGGER_EVENT_*).
pub type TriggerEvent = uint32;

/// pg_trigger cache entry (catalog/relcache layer).
pub use crate::utils::reltrigger::Trigger;

/// Tuplestore for transition-table rows - opaque stub for now.
/// TODO(pg-port): real Tuplestorestate from utils/tuplestore.c.
#[repr(C)]
pub struct Tuplestorestate {
    _opaque: [u8; 0],
}

/*
 * TriggerData is the node type that is passed as fmgr "context" info when a
 * function is called by the trigger manager.
 */
#[repr(C)]
pub struct TriggerData {
    pub r#type: NodeTag,
    pub tg_event: TriggerEvent,
    pub tg_relation: Relation,
    pub tg_trigtuple: HeapTuple,
    pub tg_newtuple: HeapTuple,
    pub tg_trigger: *mut Trigger,
    pub tg_trigslot: *mut TupleTableSlot,
    pub tg_newslot: *mut TupleTableSlot,
    pub tg_oldtable: *mut Tuplestorestate,
    pub tg_newtable: *mut Tuplestorestate,
    pub tg_updatedcols: *const Bitmapset,
}

/* TriggerEvent bit flags */
pub const TRIGGER_EVENT_INSERT: TriggerEvent = 0x00000000;
pub const TRIGGER_EVENT_DELETE: TriggerEvent = 0x00000001;
pub const TRIGGER_EVENT_UPDATE: TriggerEvent = 0x00000002;
pub const TRIGGER_EVENT_TRUNCATE: TriggerEvent = 0x00000003;
pub const TRIGGER_EVENT_OPMASK: TriggerEvent = 0x00000003;

pub const TRIGGER_EVENT_ROW: TriggerEvent = 0x00000004;

pub const TRIGGER_EVENT_BEFORE: TriggerEvent = 0x00000008;
pub const TRIGGER_EVENT_AFTER: TriggerEvent = 0x00000000;
pub const TRIGGER_EVENT_INSTEAD: TriggerEvent = 0x00000010;
pub const TRIGGER_EVENT_TIMINGMASK: TriggerEvent = 0x00000018;

/* More TriggerEvent flags, used only within trigger.c (not for clients). */
pub const AFTER_TRIGGER_DEFERRABLE: TriggerEvent = 0x00000020;
pub const AFTER_TRIGGER_INITDEFERRED: TriggerEvent = 0x00000040;

#[inline]
pub fn TRIGGER_FIRED_BY_INSERT(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_INSERT
}
#[inline]
pub fn TRIGGER_FIRED_BY_DELETE(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_DELETE
}
#[inline]
pub fn TRIGGER_FIRED_BY_UPDATE(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_UPDATE
}
#[inline]
pub fn TRIGGER_FIRED_BY_TRUNCATE(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_TRUNCATE
}
#[inline]
pub fn TRIGGER_FIRED_FOR_ROW(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_ROW) != 0
}
#[inline]
pub fn TRIGGER_FIRED_FOR_STATEMENT(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_ROW) == 0
}
#[inline]
pub fn TRIGGER_FIRED_BEFORE(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_BEFORE
}
#[inline]
pub fn TRIGGER_FIRED_AFTER(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_AFTER
}
#[inline]
pub fn TRIGGER_FIRED_INSTEAD(event: TriggerEvent) -> bool {
    (event & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_INSTEAD
}

/*
 * Test whether the function is being called as a trigger, i.e. fcinfo->context
 * is a TriggerData node.
 *
 * # Safety
 * `fcinfo` is a valid FunctionCallInfo (or null).
 */
#[inline]
pub unsafe fn CALLED_AS_TRIGGER(fcinfo: FunctionCallInfo) -> bool {
    let ctx = (*fcinfo).context as *const NodeTag;
    !ctx.is_null() && *ctx == NodeTag::T_TriggerData
}

// Silence unused-import warnings for the opaque c_void helper type used by
// downstream trigger code (kept for the module's future expansion).
#[allow(dead_code)]
type _Unused = *mut c_void;

// ===========================================================================
// backend/commands/trigger.c
// ===========================================================================

use crate::prelude::{c_char, c_int};
use crate::nodes::execnodes::EState;
use crate::storage::itemptr::ItemPointerData;
use crate::{AllocSetContextCreate, ereport, elog, errmsg};
use crate::utils::elog::{ERROR, NOTICE};

/// NameStr macro (src/include/c.h)
macro_rules! NameStr {
    ($name:expr) => {
        ($name).data.as_ptr() as *const c_char
    };
}
/// ObjectAddressSet macro (catalog/objectaddress.h)
macro_rules! ObjectAddressSet {
    ($addr:expr, $classId:expr, $objectId:expr) => {{
        $addr.classId = $classId;
        $addr.objectId = $objectId;
        $addr.objectSubId = 0;
    }};
}
/// InvokeObjectPostAlterHook macro (catalog/objectaccess.h) - no-op stub.
macro_rules! InvokeObjectPostAlterHook {
    ($classId:expr, $objectId:expr, $subId:expr) => {{
        let _ = ($classId, $objectId, $subId); // TODO(pg-port): catalog/objectaccess.c
    }};
}

// --- AFTER trigger flag/offset bits (TriggerFlags) ---
pub type TriggerFlags = uint32;

pub const AFTER_TRIGGER_OFFSET: TriggerFlags = 0x07FFFFFF; /* must be low-order bits */
pub const AFTER_TRIGGER_DONE: TriggerFlags = 0x80000000;
pub const AFTER_TRIGGER_IN_PROGRESS: TriggerFlags = 0x40000000;
/* bits describing the size and tuple sources of this event */
pub const AFTER_TRIGGER_FDW_REUSE: TriggerFlags = 0x00000000;
pub const AFTER_TRIGGER_FDW_FETCH: TriggerFlags = 0x20000000;
pub const AFTER_TRIGGER_1CTID: TriggerFlags = 0x10000000;
pub const AFTER_TRIGGER_2CTID: TriggerFlags = 0x30000000;
pub const AFTER_TRIGGER_CP_UPDATE: TriggerFlags = 0x08000000;
pub const AFTER_TRIGGER_TUP_BITS: TriggerFlags = 0x38000000;

pub type AfterTriggerShared = *mut AfterTriggerSharedData;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct AfterTriggerSharedData {
    pub ats_event: TriggerEvent,        /* event type indicator, see trigger.h */
    pub ats_tgoid: Oid,                 /* the trigger's ID */
    pub ats_relid: Oid,                 /* the relation it's on */
    pub ats_rolid: Oid,                 /* role to execute the trigger */
    pub ats_firing_id: CommandId,       /* ID for firing cycle */
    pub ats_table: *mut AfterTriggersTableData, /* transition table access */
    pub ats_modifiedcols: *mut Bitmapset, /* modified columns */
}

pub type AfterTriggerEvent = *mut AfterTriggerEventData;

#[repr(C)]
pub struct AfterTriggerEventData {
    pub ate_flags: TriggerFlags,        /* status bits and offset to shared data */
    pub ate_ctid1: ItemPointerData,     /* inserted, deleted, or old updated tuple */
    pub ate_ctid2: ItemPointerData,     /* new updated tuple */

    /*
     * During a cross-partition update of a partitioned table, we also store
     * the OIDs of source and destination partitions that are needed to fetch
     * the old (ctid1) and the new tuple (ctid2) from, respectively.
     */
    pub ate_src_part: Oid,
    pub ate_dst_part: Oid,
}

/* AfterTriggerEventData, minus ate_src_part, ate_dst_part */
#[repr(C)]
pub struct AfterTriggerEventDataNoOids {
    pub ate_flags: TriggerFlags,
    pub ate_ctid1: ItemPointerData,
    pub ate_ctid2: ItemPointerData,
}

/* AfterTriggerEventData, minus ate_*_part and ate_ctid2 */
#[repr(C)]
pub struct AfterTriggerEventDataOneCtid {
    pub ate_flags: TriggerFlags,
    pub ate_ctid1: ItemPointerData,
}

/* AfterTriggerEventData, minus ate_*_part, ate_ctid1 and ate_ctid2 */
#[repr(C)]
pub struct AfterTriggerEventDataZeroCtids {
    pub ate_flags: TriggerFlags,
}

/// SizeofTriggerEvent(evt)
#[inline]
unsafe fn SizeofTriggerEvent(evt: AfterTriggerEvent) -> Size {
    if ((*evt).ate_flags & AFTER_TRIGGER_TUP_BITS) == AFTER_TRIGGER_CP_UPDATE {
        core::mem::size_of::<AfterTriggerEventData>()
    } else if ((*evt).ate_flags & AFTER_TRIGGER_TUP_BITS) == AFTER_TRIGGER_2CTID {
        core::mem::size_of::<AfterTriggerEventDataNoOids>()
    } else if ((*evt).ate_flags & AFTER_TRIGGER_TUP_BITS) == AFTER_TRIGGER_1CTID {
        core::mem::size_of::<AfterTriggerEventDataOneCtid>()
    } else {
        core::mem::size_of::<AfterTriggerEventDataZeroCtids>()
    }
}

/// GetTriggerSharedData(evt)
#[inline]
unsafe fn GetTriggerSharedData(evt: AfterTriggerEvent) -> AfterTriggerShared {
    ((evt as *mut c_char).add(((*evt).ate_flags & AFTER_TRIGGER_OFFSET) as usize))
        as AfterTriggerShared
}

#[repr(C)]
pub struct AfterTriggerEventChunk {
    pub next: *mut AfterTriggerEventChunk, /* list link */
    pub freeptr: *mut c_char,              /* start of free space in chunk */
    pub endfree: *mut c_char,              /* end of free space in chunk */
    pub endptr: *mut c_char,               /* end of chunk */
    /* event data follows here */
}

/// CHUNK_DATA_START(cptr)
#[inline]
unsafe fn CHUNK_DATA_START(cptr: *mut AfterTriggerEventChunk) -> *mut c_char {
    (cptr as *mut c_char).add(MAXALIGN(core::mem::size_of::<AfterTriggerEventChunk>()))
}

/* A list of events */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AfterTriggerEventList {
    pub head: *mut AfterTriggerEventChunk,
    pub tail: *mut AfterTriggerEventChunk,
    pub tailfree: *mut c_char, /* freeptr of tail chunk */
}

#[repr(C)]
pub struct SetConstraintTriggerData {
    pub sct_tgoid: Oid,
    pub sct_tgisdeferred: bool,
}
pub type SetConstraintTrigger = *mut SetConstraintTriggerData;

#[repr(C)]
pub struct SetConstraintStateData {
    pub all_isset: bool,
    pub all_isdeferred: bool,
    pub numstates: c_int,  /* number of trigstates[] entries in use */
    pub numalloc: c_int,   /* allocated size of trigstates[] */
    pub trigstates: [SetConstraintTriggerData; 0], /* FLEXIBLE_ARRAY_MEMBER */
}
pub type SetConstraintState = *mut SetConstraintStateData;

#[repr(C)]
pub struct AfterTriggersData {
    pub firing_counter: CommandId, /* next firing ID to assign */
    pub state: SetConstraintState, /* the active S C state */
    pub events: AfterTriggerEventList, /* deferred-event list */
    pub event_cxt: MemoryContext,  /* memory context for events, if any */

    /* per-query-level data: */
    pub query_stack: *mut AfterTriggersQueryData, /* array of structs shown below */
    pub query_depth: c_int,        /* current index in above array */
    pub maxquerydepth: c_int,      /* allocated len of above array */

    /* per-subtransaction-level data: */
    pub trans_stack: *mut AfterTriggersTransData, /* array of structs shown below */
    pub maxtransdepth: c_int,      /* allocated len of above array */
}

#[repr(C)]
pub struct AfterTriggersQueryData {
    pub events: AfterTriggerEventList,       /* events pending from this query */
    pub fdw_tuplestore: *mut Tuplestorestate, /* foreign tuples for said events */
    pub tables: *mut List,                   /* list of AfterTriggersTableData */
}

#[repr(C)]
pub struct AfterTriggersTransData {
    /* these fields are just for resetting at subtrans abort: */
    pub state: SetConstraintState,     /* saved S C state, or NULL if not yet saved */
    pub events: AfterTriggerEventList, /* saved list pointer */
    pub query_depth: c_int,            /* saved query_depth */
    pub firing_counter: CommandId,     /* saved firing_counter */
}

#[repr(C)]
pub struct AfterTriggersTableData {
    /* relid + cmdType form the lookup key for these structs: */
    pub relid: Oid,        /* target table's OID */
    pub cmdType: CmdType,  /* event type, CMD_INSERT/UPDATE/DELETE */
    pub closed: bool,      /* true when no longer OK to add tuples */
    pub before_trig_done: bool, /* did we already queue BS triggers? */
    pub after_trig_done: bool,  /* did we already queue AS triggers? */
    pub after_trig_events: AfterTriggerEventList, /* if so, saved list pointer */

    /* "old" transition table for UPDATE/DELETE, if any */
    pub old_tuplestore: *mut Tuplestorestate,
    /* "new" transition table for INSERT/UPDATE, if any */
    pub new_tuplestore: *mut Tuplestorestate,

    pub storeslot: *mut TupleTableSlot, /* for converting to tuplestore's format */
}

static mut afterTriggers: AfterTriggersData = AfterTriggersData {
    firing_counter: 0,
    state: core::ptr::null_mut(),
    events: AfterTriggerEventList {
        head: core::ptr::null_mut(),
        tail: core::ptr::null_mut(),
        tailfree: core::ptr::null_mut(),
    },
    event_cxt: core::ptr::null_mut(),
    query_stack: core::ptr::null_mut(),
    query_depth: 0,
    maxquerydepth: 0,
    trans_stack: core::ptr::null_mut(),
    maxtransdepth: 0,
};

/* ----------
 * afterTriggerCheckState()
 *
 *	Returns true if the trigger event is actually in state DEFERRED.
 * ----------
 */
unsafe fn afterTriggerCheckState(evtshared: AfterTriggerShared) -> bool {
    let tgoid: Oid = (*evtshared).ats_tgoid;
    let state: SetConstraintState = afterTriggers.state;
    let mut i: c_int;

    /*
     * For not-deferrable triggers (i.e. normal AFTER ROW triggers and
     * constraints declared NOT DEFERRABLE), the state is always false.
     */
    if ((*evtshared).ats_event & AFTER_TRIGGER_DEFERRABLE) == 0 {
        return false;
    }

    /*
     * If constraint state exists, SET CONSTRAINTS might have been executed
     * either for this trigger or for all triggers.
     */
    if !state.is_null() {
        /* Check for SET CONSTRAINTS for this specific trigger. */
        i = 0;
        while i < (*state).numstates {
            let trigstate = (*state).trigstates.as_ptr().add(i as usize);
            if (*trigstate).sct_tgoid == tgoid {
                return (*trigstate).sct_tgisdeferred;
            }
            i += 1;
        }

        /* Check for SET CONSTRAINTS ALL. */
        if (*state).all_isset {
            return (*state).all_isdeferred;
        }
    }

    /*
     * Otherwise return the default state for the trigger.
     */
    ((*evtshared).ats_event & AFTER_TRIGGER_INITDEFERRED) != 0
}

/* ----------
 * afterTriggerCopyBitmap()
 *
 * Copy bitmap into AfterTriggerEvents memory context, which is where the after
 * trigger events are kept.
 * ----------
 */
unsafe fn afterTriggerCopyBitmap(src: *mut Bitmapset) -> *mut Bitmapset {
    let dst: *mut Bitmapset;
    let oldcxt: MemoryContext;

    if src.is_null() {
        return core::ptr::null_mut();
    }

    oldcxt = MemoryContextSwitchTo(afterTriggers.event_cxt);

    dst = bms_copy(src);

    MemoryContextSwitchTo(oldcxt);

    dst
}

/* ----------
 * afterTriggerAddEvent()
 *
 *	Add a new trigger event to the specified queue.
 *	The passed-in event data is copied.
 * ----------
 */
unsafe fn afterTriggerAddEvent(
    events: *mut AfterTriggerEventList,
    event: AfterTriggerEvent,
    evtshared: AfterTriggerShared,
) {
    let eventsize: Size = SizeofTriggerEvent(event);
    let needed: Size = eventsize + core::mem::size_of::<AfterTriggerSharedData>();
    let mut chunk: *mut AfterTriggerEventChunk;
    let mut newshared: AfterTriggerShared;
    let newevent: AfterTriggerEvent;

    /*
     * If empty list or not enough room in the tail chunk, make a new chunk.
     * We assume here that a new shared record will always be needed.
     */
    chunk = (*events).tail;
    if chunk.is_null()
        || ((*chunk).endfree as isize - (*chunk).freeptr as isize) < needed as isize
    {
        let mut chunksize: Size;

        /* Create event context if we didn't already */
        if afterTriggers.event_cxt.is_null() {
            afterTriggers.event_cxt = AllocSetContextCreate!(
                TopTransactionContext,
                c"AfterTriggerEvents".as_ptr(),
                ALLOCSET_DEFAULT_SIZES,
            );
        }

        /*
         * Chunk size starts at 1KB and is allowed to increase up to 1MB.
         * These numbers are fairly arbitrary, though there is a hard limit at
         * AFTER_TRIGGER_OFFSET; else we couldn't link event records to their
         * shared records using the available space in ate_flags.  Another
         * constraint is that if the chunk size gets too huge, the search loop
         * below would get slow given a (not too common) usage pattern with
         * many distinct event types in a chunk.  Therefore, we double the
         * preceding chunk size only if there weren't too many shared records
         * in the preceding chunk; otherwise we halve it.  This gives us some
         * ability to adapt to the actual usage pattern of the current query
         * while still having large chunk sizes in typical usage.  All chunk
         * sizes used should be MAXALIGN multiples, to ensure that the shared
         * records will be aligned safely.
         */
        const MIN_CHUNK_SIZE: Size = 1024;
        const MAX_CHUNK_SIZE: Size = 1024 * 1024;

        if chunk.is_null() {
            chunksize = MIN_CHUNK_SIZE;
        } else {
            /* preceding chunk size... */
            chunksize = ((*chunk).endptr as isize - chunk as isize) as Size;
            /* check number of shared records in preceding chunk */
            if ((*chunk).endptr as isize - (*chunk).endfree as isize)
                <= (100 * core::mem::size_of::<AfterTriggerSharedData>()) as isize
            {
                chunksize *= 2; /* okay, double it */
            } else {
                chunksize /= 2; /* too many shared records */
            }
            chunksize = Min(chunksize, MAX_CHUNK_SIZE);
        }
        chunk = MemoryContextAlloc(afterTriggers.event_cxt, chunksize)
            as *mut AfterTriggerEventChunk;
        (*chunk).next = core::ptr::null_mut();
        (*chunk).freeptr = CHUNK_DATA_START(chunk);
        (*chunk).endptr = (chunk as *mut c_char).add(chunksize);
        (*chunk).endfree = (*chunk).endptr;
        Assert!((*chunk).endfree as isize - (*chunk).freeptr as isize >= needed as isize);

        if (*events).tail.is_null() {
            Assert!((*events).head.is_null());
            (*events).head = chunk;
        } else {
            (*(*events).tail).next = chunk;
        }
        (*events).tail = chunk;
        /* events->tailfree is now out of sync, but we'll fix it below */
    }

    /*
     * Try to locate a matching shared-data record already in the chunk. If
     * none, make a new one. The search begins with the most recently added
     * record, since newer ones are most likely to match.
     */
    newshared = (*chunk).endfree as AfterTriggerShared;
    while (newshared as *const c_char) < (*chunk).endptr {
        /* compare fields roughly by probability of them being different */
        if (*newshared).ats_tgoid == (*evtshared).ats_tgoid
            && (*newshared).ats_event == (*evtshared).ats_event
            && (*newshared).ats_firing_id == 0
            && (*newshared).ats_table == (*evtshared).ats_table
            && (*newshared).ats_relid == (*evtshared).ats_relid
            && (*newshared).ats_rolid == (*evtshared).ats_rolid
            && bms_equal((*newshared).ats_modifiedcols, (*evtshared).ats_modifiedcols)
        {
            break;
        }
        newshared = newshared.add(1);
    }
    if (newshared as *const c_char) >= (*chunk).endptr {
        newshared = ((*chunk).endfree as AfterTriggerShared).offset(-1);
        *newshared = *evtshared;
        /* now we must make a suitably-long-lived copy of the bitmap */
        (*newshared).ats_modifiedcols =
            afterTriggerCopyBitmap((*evtshared).ats_modifiedcols);
        (*newshared).ats_firing_id = 0; /* just to be sure */
        (*chunk).endfree = newshared as *mut c_char;
    }

    /* Insert the data */
    newevent = (*chunk).freeptr as AfterTriggerEvent;
    core::ptr::copy_nonoverlapping(
        event as *const u8,
        newevent as *mut u8,
        eventsize,
    );
    /* ... and link the new event to its shared record */
    (*newevent).ate_flags &= !AFTER_TRIGGER_OFFSET;
    (*newevent).ate_flags |= (newshared as *const c_char as isize
        - newevent as *const c_char as isize) as TriggerFlags;

    (*chunk).freeptr = (*chunk).freeptr.add(eventsize);
    (*events).tailfree = (*chunk).freeptr;
}

/* ----------
 * afterTriggerFreeEventList()
 *
 *	Free all the event storage in the given list.
 * ----------
 */
unsafe fn afterTriggerFreeEventList(events: *mut AfterTriggerEventList) {
    let mut chunk: *mut AfterTriggerEventChunk;

    loop {
        chunk = (*events).head;
        if chunk.is_null() {
            break;
        }
        (*events).head = (*chunk).next;
        pfree(chunk as *mut c_void);
    }
    (*events).tail = core::ptr::null_mut();
    (*events).tailfree = core::ptr::null_mut();
}

/* ----------
 * afterTriggerRestoreEventList()
 *
 *	Restore an event list to its prior length, removing all the events
 *	added since it had the value old_events.
 * ----------
 */
unsafe fn afterTriggerRestoreEventList(
    events: *mut AfterTriggerEventList,
    old_events: *const AfterTriggerEventList,
) {
    let mut chunk: *mut AfterTriggerEventChunk;
    let mut next_chunk: *mut AfterTriggerEventChunk;

    if (*old_events).tail.is_null() {
        /* restoring to a completely empty state, so free everything */
        afterTriggerFreeEventList(events);
    } else {
        *events = *old_events;
        /* free any chunks after the last one we want to keep */
        chunk = (*(*events).tail).next;
        while !chunk.is_null() {
            next_chunk = (*chunk).next;
            pfree(chunk as *mut c_void);
            chunk = next_chunk;
        }
        /* and clean up the tail chunk to be the right length */
        (*(*events).tail).next = core::ptr::null_mut();
        (*(*events).tail).freeptr = (*events).tailfree;

        /*
         * We don't make any effort to remove now-unused shared data records.
         * They might still be useful, anyway.
         */
    }
}

/* ----------
 * afterTriggerDeleteHeadEventChunk()
 *
 *	Remove the first chunk of events from the query level's event list.
 *	Keep any event list pointers elsewhere in the query level's data
 *	structures in sync.
 * ----------
 */
unsafe fn afterTriggerDeleteHeadEventChunk(qs: *mut AfterTriggersQueryData) {
    let target: *mut AfterTriggerEventChunk = (*qs).events.head;
    let lc: *mut crate::nodes::pg_list::ListCell;

    Assert!(!target.is_null() && !(*target).next.is_null());

    /*
     * First, update any pointers in the per-table data, so that they won't be
     * dangling.  Resetting obsoleted pointers to NULL will make
     * cancel_prior_stmt_triggers start from the list head, which is fine.
     */
    crate::foreach!(lc, (*qs).tables, {
        let table = lfirst(crate::current_cell!(lc)) as *mut AfterTriggersTableData;

        if (*table).after_trig_done && (*table).after_trig_events.tail == target {
            (*table).after_trig_events.head = core::ptr::null_mut();
            (*table).after_trig_events.tail = core::ptr::null_mut();
            (*table).after_trig_events.tailfree = core::ptr::null_mut();
        }
    });

    /* Now we can flush the head chunk */
    (*qs).events.head = (*target).next;
    pfree(target as *mut c_void);
}

/*
 * afterTriggerMarkEvents()
 *
 *	Scan the given event list for not yet invoked events.  Mark the ones
 *	that can be invoked now with the current firing ID.
 *
 *	If move_list isn't NULL, events that are not to be invoked now are
 *	transferred to move_list.
 *
 *	When immediate_only is true, do not invoke currently-deferred triggers.
 *	(This will be false only at main transaction exit.)
 *
 *	Returns true if any invokable events were found.
 */
unsafe fn afterTriggerMarkEvents(
    events: *mut AfterTriggerEventList,
    move_list: *mut AfterTriggerEventList,
    immediate_only: bool,
) -> bool {
    let mut found = false;
    let mut deferred_found = false;
    let mut event: AfterTriggerEvent;
    let mut chunk: *mut AfterTriggerEventChunk;

    /* for_each_event_chunk(event, chunk, *events) */
    chunk = (*events).head;
    while !chunk.is_null() {
        event = CHUNK_DATA_START(chunk) as AfterTriggerEvent;
        while (event as *const c_char) < (*chunk).freeptr {
            let evtshared: AfterTriggerShared = GetTriggerSharedData(event);
            let mut defer_it = false;

            if ((*event).ate_flags & (AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS)) == 0 {
                /*
                 * This trigger hasn't been called or scheduled yet. Check if we
                 * should call it now.
                 */
                if immediate_only && afterTriggerCheckState(evtshared) {
                    defer_it = true;
                } else {
                    /*
                     * Mark it as to be fired in this firing cycle.
                     */
                    (*evtshared).ats_firing_id = afterTriggers.firing_counter;
                    (*event).ate_flags |= AFTER_TRIGGER_IN_PROGRESS;
                    found = true;
                }
            }

            /*
             * If it's deferred, move it to move_list, if requested.
             */
            if defer_it && !move_list.is_null() {
                deferred_found = true;
                /* add it to move_list */
                afterTriggerAddEvent(move_list, event, evtshared);
                /* mark original copy "done" so we don't do it again */
                (*event).ate_flags |= AFTER_TRIGGER_DONE;
            }

            event = ((event as *mut c_char).add(SizeofTriggerEvent(event))) as AfterTriggerEvent;
        }
        chunk = (*chunk).next;
    }

    /*
     * We could allow deferred triggers if, before the end of the
     * security-restricted operation, we were to verify that a SET CONSTRAINTS
     * ... IMMEDIATE has fired all such triggers.  For now, don't bother.
     */
    if deferred_found && InSecurityRestrictedOperation() {
        ereport!(ERROR, errmsg!("cannot fire deferred trigger within security-restricted operation"));
        /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
    }

    found
}

/*
 * afterTriggerInvokeEvents()
 *
 *	Scan the given event list for events that are marked as to be fired
 *	in the current firing cycle, and fire them.
 *
 *	If estate isn't NULL, we use its result relation info to avoid repeated
 *	openings and closing of trigger target relations.  If it is NULL, we
 *	make one locally to cache the info in case there are multiple trigger
 *	events per rel.
 *
 *	When delete_ok is true, it's safe to delete fully-processed events.
 *
 *	Returns true if no unfired events remain in the list (this allows us
 *	to avoid repeating afterTriggerMarkEvents).
 */
unsafe fn afterTriggerInvokeEvents(
    events: *mut AfterTriggerEventList,
    firing_id: CommandId,
    mut estate: *mut EState,
    delete_ok: bool,
) -> bool {
    let mut all_fired = true;
    let mut chunk: *mut AfterTriggerEventChunk;
    let per_tuple_context: MemoryContext;
    let mut local_estate = false;
    let mut rInfo: *mut ResultRelInfo = core::ptr::null_mut();
    let mut rel: Relation = core::ptr::null_mut();
    let mut trigdesc: *mut TriggerDesc = core::ptr::null_mut();
    let mut finfo: *mut FmgrInfo = core::ptr::null_mut();
    let mut instr: *mut Instrumentation = core::ptr::null_mut();
    let mut slot1: *mut TupleTableSlot = core::ptr::null_mut();
    let mut slot2: *mut TupleTableSlot = core::ptr::null_mut();

    /* Make a local EState if need be */
    if estate.is_null() {
        estate = CreateExecutorState();
        local_estate = true;
    }

    /* Make a per-tuple memory context for trigger function calls */
    per_tuple_context = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"AfterTriggerTupleContext".as_ptr(),
        ALLOCSET_DEFAULT_SIZES,
    );

    /* for_each_chunk(chunk, *events) */
    chunk = (*events).head;
    while !chunk.is_null() {
        let mut event: AfterTriggerEvent;
        let mut all_fired_in_chunk = true;

        /* for_each_event(event, chunk) */
        event = CHUNK_DATA_START(chunk) as AfterTriggerEvent;
        while (event as *const c_char) < (*chunk).freeptr {
            let evtshared: AfterTriggerShared = GetTriggerSharedData(event);

            /*
             * Is it one for me to fire?
             */
            if ((*event).ate_flags & AFTER_TRIGGER_IN_PROGRESS) != 0
                && (*evtshared).ats_firing_id == firing_id
            {
                let src_rInfo: *mut ResultRelInfo;
                let dst_rInfo: *mut ResultRelInfo;

                /*
                 * So let's fire it... but first, find the correct relation if
                 * this is not the same relation as before.
                 */
                if rel.is_null() || RelationGetRelid(rel) != (*evtshared).ats_relid {
                    rInfo = ExecGetTriggerResultRel(estate, (*evtshared).ats_relid, core::ptr::null_mut());
                    rel = (*rInfo).ri_RelationDesc;
                    /* Catch calls with insufficient relcache refcounting */
                    Assert!(!RelationHasReferenceCountZero(rel));
                    trigdesc = (*rInfo).ri_TrigDesc as *mut TriggerDesc;
                    /* caution: trigdesc could be NULL here */
                    finfo = (*rInfo).ri_TrigFunctions;
                    instr = (*rInfo).ri_TrigInstrument;
                    if !slot1.is_null() {
                        ExecDropSingleTupleTableSlot(slot1);
                        ExecDropSingleTupleTableSlot(slot2);
                        slot1 = core::ptr::null_mut();
                        slot2 = core::ptr::null_mut();
                    }
                    if (*(*rel).rd_rel).relkind == RELKIND_FOREIGN_TABLE {
                        slot1 = MakeSingleTupleTableSlot((*rel).rd_att, &raw const TTSOpsMinimalTuple);
                        slot2 = MakeSingleTupleTableSlot((*rel).rd_att, &raw const TTSOpsMinimalTuple);
                    }
                }

                /*
                 * Look up source and destination partition result rels of a
                 * cross-partition update event.
                 */
                if ((*event).ate_flags & AFTER_TRIGGER_TUP_BITS) == AFTER_TRIGGER_CP_UPDATE {
                    Assert!(OidIsValid((*event).ate_src_part) && OidIsValid((*event).ate_dst_part));
                    src_rInfo = ExecGetTriggerResultRel(estate, (*event).ate_src_part, rInfo);
                    dst_rInfo = ExecGetTriggerResultRel(estate, (*event).ate_dst_part, rInfo);
                } else {
                    src_rInfo = rInfo;
                    dst_rInfo = rInfo;
                }

                /*
                 * Fire it.  Note that the AFTER_TRIGGER_IN_PROGRESS flag is
                 * still set, so recursive examinations of the event list
                 * won't try to re-fire it.
                 */
                AfterTriggerExecute(
                    estate, event, rInfo, src_rInfo, dst_rInfo, trigdesc, finfo, instr,
                    per_tuple_context, slot1, slot2,
                );

                /*
                 * Mark the event as done.
                 */
                (*event).ate_flags &= !AFTER_TRIGGER_IN_PROGRESS;
                (*event).ate_flags |= AFTER_TRIGGER_DONE;
            } else if ((*event).ate_flags & AFTER_TRIGGER_DONE) == 0 {
                /* something remains to be done */
                all_fired = false;
                all_fired_in_chunk = false;
            }

            event = ((event as *mut c_char).add(SizeofTriggerEvent(event))) as AfterTriggerEvent;
        }

        /* Clear the chunk if delete_ok and nothing left of interest */
        if delete_ok && all_fired_in_chunk {
            (*chunk).freeptr = CHUNK_DATA_START(chunk);
            (*chunk).endfree = (*chunk).endptr;

            /*
             * If it's last chunk, must sync event list's tailfree too.
             */
            if chunk == (*events).tail {
                (*events).tailfree = (*chunk).freeptr;
            }
        }

        chunk = (*chunk).next;
    }
    if !slot1.is_null() {
        ExecDropSingleTupleTableSlot(slot1);
        ExecDropSingleTupleTableSlot(slot2);
    }

    /* Release working resources */
    MemoryContextDelete(per_tuple_context);

    if local_estate {
        ExecCloseResultRelations(estate);
        ExecResetTupleTable((*estate).es_tupleTable, false);
        FreeExecutorState(estate);
    }

    all_fired
}

/*
 * get_trigger_oid - looks up the trigger OID for the given relid + trigname.
 */
pub unsafe fn get_trigger_oid(relid: Oid, trigname: *const c_char, missing_ok: bool) -> Oid {
    let tgrel: Relation;
    let mut skey: [ScanKeyData; 2] = core::mem::zeroed();
    let tgscan: SysScanDesc;
    let tup: HeapTuple;
    let oid: Oid;

    /*
     * Find the trigger, verify permissions, set up object address
     */
    tgrel = table_open(TriggerRelationId, AccessShareLock);

    ScanKeyInit(
        &mut skey[0],
        Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    ScanKeyInit(
        &mut skey[1],
        Anum_pg_trigger_tgname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        CStringGetDatum(trigname),
    );

    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, core::ptr::null_mut(), 2, skey.as_mut_ptr());

    tup = systable_getnext(tgscan);

    if !HeapTupleIsValid(tup) {
        if !missing_ok {
            ereport!(ERROR, errmsg!("trigger \"{}\" for table \"{}\" does not exist",
                std::ffi::CStr::from_ptr(trigname).to_string_lossy(),
                std::ffi::CStr::from_ptr(get_rel_name(relid)).to_string_lossy()));
            /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
        }
        oid = InvalidOid;
    } else {
        oid = (*(GETSTRUCT(tup) as Form_pg_trigger)).oid;
    }

    systable_endscan(tgscan);
    table_close(tgrel, AccessShareLock);
    oid
}

/*
 *		renametrig		- changes the name of a trigger on a relation
 */
pub unsafe fn renametrig(stmt: *mut RenameStmt) -> ObjectAddress {
    let mut tgoid: Oid = InvalidOid;
    let targetrel: Relation;
    let tgrel: Relation;
    let tuple: HeapTuple;
    let tgscan: SysScanDesc;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let relid: Oid;
    let mut address: ObjectAddress = core::mem::zeroed();

    /*
     * Look up name, check permissions, and acquire lock (which we will NOT
     * release until end of transaction).
     */
    relid = RangeVarGetRelidExtended(
        (*stmt).relation,
        AccessExclusiveLock,
        0,
        Some(RangeVarCallbackForRenameTrigger),
        core::ptr::null_mut(),
    );

    /* Have lock already, so just need to build relcache entry. */
    targetrel = relation_open(relid, NoLock);

    /*
     * On partitioned tables, this operation recurses to partitions.  Lock all
     * tables upfront.
     */
    if (*(*targetrel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        find_all_inheritors(relid, AccessExclusiveLock, core::ptr::null_mut());
    }

    tgrel = table_open(TriggerRelationId, RowExclusiveLock);

    /*
     * Search for the trigger to modify.
     */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(relid),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_trigger_tgname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        PointerGetDatum((*stmt).subname as *mut c_void),
    );
    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, core::ptr::null_mut(), 2, key.as_mut_ptr());
    tuple = systable_getnext(tgscan);
    if HeapTupleIsValid(tuple) {
        let trigform: Form_pg_trigger;

        trigform = GETSTRUCT(tuple) as Form_pg_trigger;
        tgoid = (*trigform).oid;

        /*
         * If the trigger descends from a trigger on a parent partitioned
         * table, reject the rename.
         */
        if OidIsValid((*trigform).tgparentid) {
            ereport!(ERROR, errmsg!("cannot rename trigger \"{}\" on table \"{}\"",
                std::ffi::CStr::from_ptr((*stmt).subname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(targetrel)).to_string_lossy()));
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
            /* C also: errhint("Rename the trigger on the partitioned table \"%s\" instead.", get_rel_name(get_partition_parent(relid, false))) */
        }

        /* Rename the trigger on this relation ... */
        renametrig_internal(tgrel, targetrel, tuple, (*stmt).newname, (*stmt).subname);

        /* ... and if it is partitioned, recurse to its partitions */
        if (*(*targetrel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            let partdesc: PartitionDesc = RelationGetPartitionDesc(targetrel, true);

            let mut i: c_int = 0;
            while i < (*partdesc).nparts {
                let partitionId: Oid = *(*partdesc).oids.add(i as usize);

                renametrig_partition(tgrel, partitionId, (*trigform).oid, (*stmt).newname, (*stmt).subname);
                i += 1;
            }
        }
    } else {
        ereport!(ERROR, errmsg!("trigger \"{}\" for table \"{}\" does not exist",
            std::ffi::CStr::from_ptr((*stmt).subname).to_string_lossy(),
            std::ffi::CStr::from_ptr(RelationGetRelationName(targetrel)).to_string_lossy()));
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    ObjectAddressSet!(address, TriggerRelationId, tgoid);

    systable_endscan(tgscan);

    table_close(tgrel, RowExclusiveLock);

    /*
     * Close rel, but keep exclusive lock!
     */
    relation_close(targetrel, NoLock);

    address
}

/*
 * Subroutine for renametrig -- perform the actual work of renaming one
 * trigger on one table.
 */
unsafe fn renametrig_internal(
    tgrel: Relation,
    targetrel: Relation,
    trigtup: HeapTuple,
    newname: *const c_char,
    expected_name: *const c_char,
) {
    let tuple: HeapTuple;
    let mut tgform: Form_pg_trigger;
    let mut key: [ScanKeyData; 2] = core::mem::zeroed();
    let tgscan: SysScanDesc;

    /* If the trigger already has the new name, nothing to do. */
    tgform = GETSTRUCT(trigtup) as Form_pg_trigger;
    if strcmp(NameStr!((*tgform).tgname), newname) == 0 {
        return;
    }

    /*
     * Before actually trying the rename, search for triggers with the same
     * name.
     */
    ScanKeyInit(
        &mut key[0],
        Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(targetrel)),
    );
    ScanKeyInit(
        &mut key[1],
        Anum_pg_trigger_tgname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        PointerGetDatum(newname as *mut c_void),
    );
    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, core::ptr::null_mut(), 2, key.as_mut_ptr());
    let dup = systable_getnext(tgscan);
    if HeapTupleIsValid(dup) {
        ereport!(ERROR, errmsg!("trigger \"{}\" for relation \"{}\" already exists",
            std::ffi::CStr::from_ptr(newname).to_string_lossy(),
            std::ffi::CStr::from_ptr(RelationGetRelationName(targetrel)).to_string_lossy()));
        /* C also: errcode(ERRCODE_DUPLICATE_OBJECT) */
    }
    systable_endscan(tgscan);

    /*
     * The target name is free; update the existing pg_trigger tuple with it.
     */
    tuple = heap_copytuple(trigtup); /* need a modifiable copy */
    tgform = GETSTRUCT(tuple) as Form_pg_trigger;

    /*
     * If the trigger has a name different from what we expected, let the user
     * know.
     */
    if strcmp(NameStr!((*tgform).tgname), expected_name) != 0 {
        ereport!(NOTICE, errmsg!("renamed trigger \"{}\" on relation \"{}\"",
            std::ffi::CStr::from_ptr(NameStr!((*tgform).tgname)).to_string_lossy(),
            std::ffi::CStr::from_ptr(RelationGetRelationName(targetrel)).to_string_lossy()));
    }

    namestrcpy(&raw mut (*tgform).tgname, newname);

    CatalogTupleUpdate(tgrel, &raw mut (*tuple).t_self, tuple);

    InvokeObjectPostAlterHook!(TriggerRelationId, (*tgform).oid, 0);

    /*
     * Invalidate relation's relcache entry so that other backends (and this
     * one too!) are sent SI message to make them rebuild relcache entries.
     */
    CacheInvalidateRelcache(targetrel);
}

/*
 * Subroutine for renametrig -- Helper for recursing to partitions when
 * renaming triggers on a partitioned table.
 */
unsafe fn renametrig_partition(
    tgrel: Relation,
    partitionId: Oid,
    parentTriggerOid: Oid,
    newname: *const c_char,
    expected_name: *const c_char,
) {
    let tgscan: SysScanDesc;
    let mut key: ScanKeyData = core::mem::zeroed();
    let mut tuple: HeapTuple;

    /*
     * Given a relation and the OID of a trigger on parent relation, find the
     * corresponding trigger in the child and rename that trigger to the given
     * name.
     */
    ScanKeyInit(
        &mut key,
        Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(partitionId),
    );
    tgscan = systable_beginscan(tgrel, TriggerRelidNameIndexId, true, core::ptr::null_mut(), 1, &mut key);
    loop {
        tuple = systable_getnext(tgscan);
        if !HeapTupleIsValid(tuple) {
            break;
        }
        let tgform: Form_pg_trigger = GETSTRUCT(tuple) as Form_pg_trigger;
        let partitionRel: Relation;

        if (*tgform).tgparentid != parentTriggerOid {
            continue; /* not our trigger */
        }

        partitionRel = table_open(partitionId, NoLock);

        /* Rename the trigger on this partition */
        renametrig_internal(tgrel, partitionRel, tuple, newname, expected_name);

        /* And if this relation is partitioned, recurse to its partitions */
        if (*(*partitionRel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
            let partdesc: PartitionDesc = RelationGetPartitionDesc(partitionRel, true);

            let mut i: c_int = 0;
            while i < (*partdesc).nparts {
                let partoid: Oid = *(*partdesc).oids.add(i as usize);

                renametrig_partition(tgrel, partoid, (*tgform).oid, newname, NameStr!((*tgform).tgname));
                i += 1;
            }
        }
        table_close(partitionRel, NoLock);

        /* There should be at most one matching tuple */
        break;
    }
    systable_endscan(tgscan);
}

/*
 * before_stmt_triggers_fired
 *
 * Determine whether we should fire the statement-level BEFORE triggers, and
 * remember that we did.
 */
pub unsafe fn before_stmt_triggers_fired(relid: Oid, cmdType: CmdType) -> bool {
    let result: bool;
    let table: *mut AfterTriggersTableData;

    /* Check state, like AfterTriggerSaveEvent. */
    if afterTriggers.query_depth < 0 {
        elog!(ERROR, "before_stmt_triggers_fired() called outside of query");
    }

    /* Be sure we have enough space to record events at this query depth. */
    if afterTriggers.query_depth >= afterTriggers.maxquerydepth {
        AfterTriggerEnlargeQueryState();
    }

    /*
     * We keep this state in the AfterTriggersTableData that also holds
     * transition tables for the relation + operation.
     */
    table = GetAfterTriggersTableData(relid, cmdType);
    result = (*table).before_trig_done;
    (*table).before_trig_done = true;
    result
}

/*
 * cancel_prior_stmt_triggers
 *
 * If we previously queued a set of AFTER STATEMENT triggers for the given
 * relation + operation, and they've not been fired yet, cancel them.
 */
unsafe fn cancel_prior_stmt_triggers(relid: Oid, cmdType: CmdType, tgevent: c_int) {
    let table: *mut AfterTriggersTableData;
    let qs: *mut AfterTriggersQueryData =
        afterTriggers.query_stack.add(afterTriggers.query_depth as usize);

    /*
     * We keep this state in the AfterTriggersTableData that also holds
     * transition tables for the relation + operation.
     */
    table = GetAfterTriggersTableData(relid, cmdType);

    if (*table).after_trig_done {
        /*
         * We want to start scanning from the tail location that existed just
         * before we inserted any statement triggers.
         */
        let mut event: AfterTriggerEvent;
        let mut chunk: *mut AfterTriggerEventChunk;

        if !(*table).after_trig_events.tail.is_null() {
            chunk = (*table).after_trig_events.tail;
            event = (*table).after_trig_events.tailfree as AfterTriggerEvent;
        } else {
            chunk = (*qs).events.head;
            event = core::ptr::null_mut();
        }

        /* for_each_chunk_from(chunk) */
        'outer: while !chunk.is_null() {
            if event.is_null() {
                event = CHUNK_DATA_START(chunk) as AfterTriggerEvent;
            }
            /* for_each_event_from(event, chunk) */
            while (event as *const c_char) < (*chunk).freeptr {
                let evtshared: AfterTriggerShared = GetTriggerSharedData(event);

                /*
                 * Exit loop when we reach events that aren't AS triggers for
                 * the target relation.
                 */
                if (*evtshared).ats_relid != relid {
                    break 'outer;
                }
                if ((*evtshared).ats_event & TRIGGER_EVENT_OPMASK) != tgevent as TriggerEvent {
                    break 'outer;
                }
                if !TRIGGER_FIRED_FOR_STATEMENT((*evtshared).ats_event) {
                    break 'outer;
                }
                if !TRIGGER_FIRED_AFTER((*evtshared).ats_event) {
                    break 'outer;
                }
                /* OK, mark it DONE */
                (*event).ate_flags &= !AFTER_TRIGGER_IN_PROGRESS;
                (*event).ate_flags |= AFTER_TRIGGER_DONE;

                event = ((event as *mut c_char).add(SizeofTriggerEvent(event))) as AfterTriggerEvent;
            }
            /* signal we must reinitialize event ptr for next chunk */
            event = core::ptr::null_mut();
            chunk = (*chunk).next;
        }
    }
    /* done: */

    /* In any case, save current insertion point for next time */
    (*table).after_trig_done = true;
    (*table).after_trig_events = (*qs).events;
}

/*
 * GUC assign_hook for session_replication_role
 */
pub unsafe fn assign_session_replication_role(newval: c_int, _extra: *mut c_void) {
    /*
     * Must flush the plan cache when changing replication role; but don't
     * flush unnecessarily.
     */
    if SessionReplicationRole != newval {
        ResetPlanCache();
    }
}

/*
 * SQL function pg_trigger_depth()
 */
pub unsafe fn pg_trigger_depth(_fcinfo: FunctionCallInfo) -> Datum {
    crate::PG_RETURN_INT32!(MyTriggerDepth)
}

/*
 * Check whether a trigger modified a virtual generated column and replace the
 * value with null if so.
 */
unsafe fn check_modified_virtual_generated(tupdesc: TupleDesc, mut tuple: HeapTuple) -> HeapTuple {
    if !(!(*tupdesc).constr.is_null() && (*(*tupdesc).constr).has_generated_virtual) {
        return tuple;
    }

    let mut i: c_int = 0;
    while i < (*tupdesc).natts {
        if (*TupleDescAttr(tupdesc, i)).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
            if !heap_attisnull(tuple, i + 1, tupdesc) {
                let replCol: c_int = i + 1;
                let replValue: Datum = 0;
                let replIsnull: bool = true;

                tuple = heap_modify_tuple_by_cols(
                    tuple, tupdesc, 1, &replCol, &replValue, &replIsnull,
                );
            }
        }
        i += 1;
    }

    tuple
}

// ===========================================================================
// Imports for ported helpers and local stubs for genuinely-unported deps.
//
// This file is the trigger-manager layer; the surrounding catalog/executor
// infrastructure that trigger.c depends on lives in many modules whose stub
// vs. real status varies.  Following the prevailing per-file convention in
// this port, we import the macros (which are crate-level) and palloc/Datum
// helpers, and keep the catalog/executor helper functions and the
// trigger.c-internal subroutines as local `TODO(pg-port)` stubs so the file
// is self-consistent.
// ===========================================================================

use crate::{foreach, current_cell, PG_RETURN_INT32};
use crate::nodes::pg_list::lfirst;
use crate::nodes::execnodes::{FmgrInfo, Instrumentation, ResultRelInfo, TupleTableSlotOps};
use crate::utils::reltrigger::TriggerDesc;
use crate::catalog::pg_trigger::{
    TRIGGER_TYPE_ROW, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_AFTER,
    TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_DELETE,
    TRIGGER_TYPE_TRUNCATE, TRIGGER_TYPE_LEVEL_MASK, TRIGGER_TYPE_TIMING_MASK,
};
use crate::access::common::tupdesc::TupleDesc;

/* pg_trigger.h: TRIGGER_TYPE_MATCHES(type, level, timing, event) */
#[inline]
fn TRIGGER_TYPE_MATCHES(tgtype: i16, level: i16, timing: i16, event: i16) -> bool {
    (tgtype & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK | event))
        == (level | timing | event)
}

/* pg_trigger.h: TRIGGER_FOR_{INSERT,UPDATE,DELETE} */
#[inline]
fn TRIGGER_FOR_INSERT(tgtype: i16) -> bool { (tgtype & TRIGGER_TYPE_INSERT) != 0 }
#[inline]
fn TRIGGER_FOR_UPDATE(tgtype: i16) -> bool { (tgtype & TRIGGER_TYPE_UPDATE) != 0 }
#[inline]
fn TRIGGER_FOR_DELETE(tgtype: i16) -> bool { (tgtype & TRIGGER_TYPE_DELETE) != 0 }
#[inline]
fn TRIGGER_FOR_ROW(tgtype: i16) -> bool { (tgtype & TRIGGER_TYPE_ROW) != 0 }

/* reltrigger.h: TRIGGER_USES_TRANSITION_TABLE(namepointer) */
#[inline]
unsafe fn TRIGGER_USES_TRANSITION_TABLE(namepointer: *const core::ffi::c_char) -> bool {
    !namepointer.is_null()
}

/*
 * Set the bits in a TriggerDesc that indicate the trigger types matching the
 * given Trigger.
 */
pub unsafe fn SetTriggerFlags(trigdesc: *mut TriggerDesc, trigger: *const crate::utils::reltrigger::Trigger) {
    let tgtype: i16 = (*trigger).tgtype;

    (*trigdesc).trig_insert_before_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT);
    (*trigdesc).trig_insert_after_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT);
    (*trigdesc).trig_insert_instead_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_INSERT);
    (*trigdesc).trig_insert_before_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_INSERT);
    (*trigdesc).trig_insert_after_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_INSERT);
    (*trigdesc).trig_update_before_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE);
    (*trigdesc).trig_update_after_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE);
    (*trigdesc).trig_update_instead_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_UPDATE);
    (*trigdesc).trig_update_before_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_UPDATE);
    (*trigdesc).trig_update_after_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_UPDATE);
    (*trigdesc).trig_delete_before_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE);
    (*trigdesc).trig_delete_after_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE);
    (*trigdesc).trig_delete_instead_row |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW, TRIGGER_TYPE_INSTEAD, TRIGGER_TYPE_DELETE);
    (*trigdesc).trig_delete_before_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_DELETE);
    (*trigdesc).trig_delete_after_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_DELETE);
    /* there are no row-level truncate triggers */
    (*trigdesc).trig_truncate_before_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_BEFORE, TRIGGER_TYPE_TRUNCATE);
    (*trigdesc).trig_truncate_after_statement |=
        TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER, TRIGGER_TYPE_TRUNCATE);

    (*trigdesc).trig_insert_new_table |=
        TRIGGER_FOR_INSERT(tgtype) && TRIGGER_USES_TRANSITION_TABLE((*trigger).tgnewtable);
    (*trigdesc).trig_update_old_table |=
        TRIGGER_FOR_UPDATE(tgtype) && TRIGGER_USES_TRANSITION_TABLE((*trigger).tgoldtable);
    (*trigdesc).trig_update_new_table |=
        TRIGGER_FOR_UPDATE(tgtype) && TRIGGER_USES_TRANSITION_TABLE((*trigger).tgnewtable);
    (*trigdesc).trig_delete_old_table |=
        TRIGGER_FOR_DELETE(tgtype) && TRIGGER_USES_TRANSITION_TABLE((*trigger).tgoldtable);
}

/*
 * If a trigger is row-level and references transition tables, it is not
 * compatible with being inherited; returns the name of the first such trigger
 * found in trigdesc, or NULL if there is none.
 */
pub unsafe fn FindTriggerIncompatibleWithInheritance(
    trigdesc: *const TriggerDesc,
) -> *const core::ffi::c_char {
    if !trigdesc.is_null() {
        for i in 0..(*trigdesc).numtriggers {
            let trigger: *const crate::utils::reltrigger::Trigger =
                (*trigdesc).triggers.add(i as usize);

            if !TRIGGER_FOR_ROW((*trigger).tgtype) {
                continue;
            }
            if !(*trigger).tgoldtable.is_null() || !(*trigger).tgnewtable.is_null() {
                return (*trigger).tgname;
            }
        }
    }

    core::ptr::null()
}

/*
 * Make a trigger a child of a parent trigger (or detach it).  Updates the
 * child's pg_trigger.tgparentid and the partition dependency records.
 */
pub unsafe fn TriggerSetParentTrigger(
    trigRel: Relation,
    childTrigId: Oid,
    parentTrigId: Oid,
    childTableId: Oid,
) {
    use crate::catalog::pg_depend::{deleteDependencyRecordsForClass, recordDependencyOn};
    use crate::access::common::heaptuple::heap_freetuple;
    use crate::catalog::dependency::{DEPENDENCY_PARTITION_PRI, DEPENDENCY_PARTITION_SEC};
    use crate::catalog::catalog_oids::RelationRelationId;
    /* pg_trigger.h / catalog OIDs (event_trigger.rs is the canonical home but
     * is mid-port; inline here to avoid a cross-file build dependency). */
    const Anum_pg_trigger_oid: i32 = 1;
    const TriggerOidIndexId: Oid = 2696;

    let mut skey: [ScanKeyData; 1] = core::mem::zeroed();
    let tgscan: SysScanDesc;
    let trigForm: Form_pg_trigger;
    let tuple: HeapTuple;
    let newtup: HeapTuple;
    let mut depender: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();

    /* Find the trigger to update. */
    ScanKeyInit(
        &mut skey[0],
        Anum_pg_trigger_oid,
        BTEqualStrategyNumber as u16,
        F_OIDEQ,
        ObjectIdGetDatum(childTrigId),
    );

    tgscan = systable_beginscan(
        trigRel,
        TriggerOidIndexId,
        true,
        core::ptr::null_mut(),
        1,
        skey.as_mut_ptr(),
    );

    tuple = systable_getnext(tgscan);
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "could not find tuple for trigger {}", childTrigId);
    }
    newtup = heap_copytuple(tuple);
    trigForm = GETSTRUCT(newtup) as Form_pg_trigger;
    if OidIsValid(parentTrigId) {
        /* don't allow setting parent for a trigger that already has one */
        if OidIsValid((*trigForm).tgparentid) {
            elog!(ERROR, "trigger {} already has a parent trigger", childTrigId);
        }

        (*trigForm).tgparentid = parentTrigId;

        CatalogTupleUpdate(trigRel, &mut (*tuple).t_self, newtup);

        ObjectAddressSet!(depender, TriggerRelationId, childTrigId);

        ObjectAddressSet!(referenced, TriggerRelationId, parentTrigId);
        recordDependencyOn(&depender, &referenced, DEPENDENCY_PARTITION_PRI);

        ObjectAddressSet!(referenced, RelationRelationId, childTableId);
        recordDependencyOn(&depender, &referenced, DEPENDENCY_PARTITION_SEC);
    } else {
        (*trigForm).tgparentid = InvalidOid;

        CatalogTupleUpdate(trigRel, &mut (*tuple).t_self, newtup);

        deleteDependencyRecordsForClass(
            TriggerRelationId,
            childTrigId,
            TriggerRelationId,
            DEPENDENCY_PARTITION_PRI as c_char,
        );
        deleteDependencyRecordsForClass(
            TriggerRelationId,
            childTrigId,
            RelationRelationId,
            DEPENDENCY_PARTITION_SEC as c_char,
        );
    }

    heap_freetuple(newtup);
    systable_endscan(tgscan);
}

/*
 * EnableDisableTrigger - enable or disable trigger(s) on a relation.
 *
 * If tgname is non-NULL, only the named trigger is affected; otherwise all
 * triggers on the relation are.  For partitioned tables we recurse to the
 * partitions unless ONLY was specified.
 */
pub unsafe fn EnableDisableTrigger(
    rel: Relation,
    tgname: *const core::ffi::c_char,
    tgparent: Oid,
    fires_when: core::ffi::c_char,
    skip_system: bool,
    recurse: bool,
    lockmode: LOCKMODE,
) {
    use crate::miscadmin::superuser;
    use crate::access::common::heaptuple::heap_freetuple;
    const Anum_pg_trigger_tgrelid: i32 = 2;
    const Anum_pg_trigger_tgname: i32 = 4;
    const TriggerRelidNameIndexId: Oid = 2701;

    let tgrel: Relation;
    let nkeys: i32;
    let mut keys: [ScanKeyData; 2] = core::mem::zeroed();
    let tgscan: SysScanDesc;
    let mut tuple: HeapTuple;
    let mut found: bool;
    let mut changed: bool;

    /* Scan the relevant entries in pg_triggers */
    tgrel = table_open(TriggerRelationId, RowExclusiveLock);

    ScanKeyInit(
        &mut keys[0],
        Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber as u16,
        F_OIDEQ,
        ObjectIdGetDatum(RelationGetRelid(rel)),
    );
    if !tgname.is_null() {
        ScanKeyInit(
            &mut keys[1],
            Anum_pg_trigger_tgname,
            BTEqualStrategyNumber as u16,
            F_NAMEEQ,
            CStringGetDatum(tgname),
        );
        nkeys = 2;
    } else {
        nkeys = 1;
    }

    tgscan = systable_beginscan(
        tgrel,
        TriggerRelidNameIndexId,
        true,
        core::ptr::null_mut(),
        nkeys,
        keys.as_mut_ptr(),
    );

    found = false;
    changed = false;

    loop {
        tuple = systable_getnext(tgscan);
        if !HeapTupleIsValid(tuple) {
            break;
        }
        let oldtrig: Form_pg_trigger = GETSTRUCT(tuple) as Form_pg_trigger;

        if OidIsValid(tgparent) && tgparent != (*oldtrig).tgparentid {
            continue;
        }

        if (*oldtrig).tgisinternal {
            /* system trigger ... ok to process? */
            if skip_system {
                continue;
            }
            if !superuser() {
                ereport!(
                    ERROR,
                    errmsg!("permission denied: \"{}\" is a system trigger",
                        std::ffi::CStr::from_ptr((*oldtrig).tgname.data.as_ptr()).to_string_lossy())
                );
                /* C also: errcode(ERRCODE_INSUFFICIENT_PRIVILEGE) */
            }
        }

        found = true;

        if (*oldtrig).tgenabled != fires_when {
            /* need to change this one ... make a copy to scribble on */
            let newtup: HeapTuple = heap_copytuple(tuple);
            let newtrig: Form_pg_trigger = GETSTRUCT(newtup) as Form_pg_trigger;

            (*newtrig).tgenabled = fires_when;

            CatalogTupleUpdate(tgrel, &mut (*newtup).t_self, newtup);

            heap_freetuple(newtup);

            changed = true;
        }

        /*
         * When altering FOR EACH ROW triggers on a partitioned table, do the
         * same on the partitions as well, unless ONLY is specified.
         */
        if recurse
            && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE
            && TRIGGER_FOR_ROW((*oldtrig).tgtype)
        {
            let partdesc: PartitionDesc = RelationGetPartitionDesc(rel, true);

            for i in 0..(*partdesc).nparts {
                let part: Relation = relation_open(*(*partdesc).oids.add(i as usize), lockmode);
                /* Match on child triggers' tgparentid, not their name */
                EnableDisableTrigger(
                    part,
                    core::ptr::null(),
                    (*oldtrig).oid,
                    fires_when,
                    skip_system,
                    recurse,
                    lockmode,
                );
                table_close(part, NoLock); /* keep lock till commit */
            }
        }

        InvokeObjectPostAlterHook!(TriggerRelationId, (*oldtrig).oid, 0);
    }

    systable_endscan(tgscan);

    table_close(tgrel, RowExclusiveLock);

    if !tgname.is_null() && !found {
        ereport!(
            ERROR,
            errmsg!("trigger \"{}\" for table \"{}\" does not exist",
                std::ffi::CStr::from_ptr(tgname).to_string_lossy(),
                std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy())
        );
        /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
    }

    /*
     * If we changed anything, broadcast an SI inval message to force each
     * backend to rebuild the relation's relcache entry.
     */
    if changed {
        CacheInvalidateRelcache(rel);
    }
}
use crate::postgres::Datum;

type LOCKMODE = core::ffi::c_int;
type RenameStmt = crate::nodes::parsenodes::RenameStmt;
type ObjectAddress = crate::catalog::objectaccess::ObjectAddress;
type ScanKeyData = crate::access::common::scankey::ScanKeyData;
type Form_pg_trigger = *mut crate::catalog::pg_trigger::FormData_pg_trigger;
type RangeVar = crate::nodes::primnodes::RangeVar;

use crate::utils::palloc::CurrentMemoryContext;
use crate::utils::memutils::{ALLOCSET_DEFAULT_SIZES, MemoryContextDelete};
/// TODO(pg-port): real TopTransactionContext lives in utils/mmgr/mcxt.c.
static mut TopTransactionContext: MemoryContext = core::ptr::null_mut();

// --- opaque local types ---

/// Sys-catalog scan descriptor (access/genam.h) - opaque stub.
#[repr(C)]
pub struct SysScanDescData {
    _opaque: [u8; 0],
}
type SysScanDesc = *mut SysScanDescData;

/// PartitionDesc (partitioning/partdesc.h) - minimal fields used here.
#[repr(C)]
pub struct PartitionDescData {
    pub nparts: c_int,
    pub oids: *mut Oid,
}
type PartitionDesc = *mut PartitionDescData;

/// TODO(pg-port): real TTSOpsMinimalTuple in executor/execTuples.c.
static TTSOpsMinimalTuple: TupleTableSlotOps = TupleTableSlotOps {
    base_slot_size: 0,
    init: None,
    release: None,
    clear: None,
    getsomeattrs: None,
    getsysattr: None,
    is_current_xact_tuple: None,
    materialize: None,
    copyslot: None,
    get_heap_tuple: None,
    get_minimal_tuple: None,
    copy_heap_tuple: None,
    copy_minimal_tuple: None,
};

// --- constants (TODO(pg-port): real homes in catalog/pg_trigger.h, etc.) ---
const TriggerRelationId: Oid = 2620;
const TriggerRelidNameIndexId: Oid = 2701;
const Anum_pg_trigger_tgrelid: c_int = 2;
const Anum_pg_trigger_tgname: c_int = 3;
const AccessShareLock: c_int = 1;
const RowExclusiveLock: c_int = 3;
const AccessExclusiveLock: c_int = 8;
const NoLock: c_int = 0;
const BTEqualStrategyNumber: u16 = 3;
const F_OIDEQ: Oid = 184;
const F_NAMEEQ: Oid = 60;
const RELKIND_PARTITIONED_TABLE: c_char = b'p' as c_char;
const RELKIND_FOREIGN_TABLE: c_char = b'f' as c_char;
const ATTRIBUTE_GENERATED_VIRTUAL: c_char = b'v' as c_char;
static mut MyTriggerDepth: c_int = 0;
static mut SessionReplicationRole: c_int = 0;

// --- genuinely-unported helper deps: local TODO(pg-port) stubs ---

unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO(pg-port): access/table/table.c
}
unsafe fn table_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): access/table/table.c
}
unsafe fn relation_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO(pg-port): access/common/relation.c
}
unsafe fn relation_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO(pg-port): access/common/relation.c
}
unsafe fn systable_beginscan(
    _heapRelation: Relation,
    _indexId: Oid,
    _indexOK: bool,
    _snapshot: *mut c_void,
    _nkeys: c_int,
    _key: *mut ScanKeyData,
) -> SysScanDesc {
    unimplemented!() // TODO(pg-port): access/index/genam.c
}
unsafe fn systable_getnext(_sysscan: SysScanDesc) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/index/genam.c
}
unsafe fn systable_endscan(_sysscan: SysScanDesc) {
    unimplemented!() // TODO(pg-port): access/index/genam.c
}
unsafe fn ScanKeyInit(
    _entry: *mut ScanKeyData,
    _attributeNumber: c_int,
    _strategy: u16,
    _procedure: Oid,
    _argument: Datum,
) {
    unimplemented!() // TODO(pg-port): access/common/scankey.c
}
unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!() // TODO(pg-port): postgres.h
}
unsafe fn CStringGetDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO(pg-port): postgres.h
}
unsafe fn PointerGetDatum(_p: *mut c_void) -> Datum {
    unimplemented!() // TODO(pg-port): postgres.h
}
unsafe fn GETSTRUCT(_tup: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO(pg-port): access/htup_details.h
}
unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool {
    !tup.is_null()
}
unsafe fn get_rel_name(_relid: Oid) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/cache/lsyscache.c
}
unsafe fn heap_copytuple(_tuple: HeapTuple) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn namestrcpy(_name: *mut crate::c::NameData, _s: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): common/string.c
}
unsafe fn strcmp(_a: *const c_char, _b: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): libc strcmp
}
unsafe fn CatalogTupleUpdate(_heapRel: Relation, _otid: *mut crate::storage::itemptr::ItemPointerData, _tup: HeapTuple) {
    unimplemented!() // TODO(pg-port): catalog/indexing.c
}
unsafe fn CacheInvalidateRelcache(_relation: Relation) {
    unimplemented!() // TODO(pg-port): utils/cache/inval.c
}
unsafe fn RelationGetRelid(_relation: Relation) -> Oid {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
unsafe fn RelationGetRelationName(_relation: Relation) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
unsafe fn RelationHasReferenceCountZero(_relation: Relation) -> bool {
    unimplemented!() // TODO(pg-port): utils/rel.h
}
unsafe fn RelationGetPartitionDesc(_rel: Relation, _omit_detached: bool) -> PartitionDesc {
    unimplemented!() // TODO(pg-port): utils/cache/partcache.c
}
unsafe fn find_all_inheritors(_parentrelId: Oid, _lockmode: c_int, _numparents: *mut c_void) -> *mut List {
    unimplemented!() // TODO(pg-port): catalog/pg_inherits.c
}
type RangeVarGetRelidCallback =
    Option<unsafe extern "C" fn(*const RangeVar, Oid, Oid, *mut c_void)>;
unsafe fn RangeVarGetRelidExtended(
    _relation: *mut RangeVar,
    _lockmode: c_int,
    _flags: u32,
    _callback: RangeVarGetRelidCallback,
    _callback_arg: *mut c_void,
) -> Oid {
    unimplemented!() // TODO(pg-port): catalog/namespace.c
}
unsafe fn ResetPlanCache() {
    unimplemented!() // TODO(pg-port): utils/cache/plancache.c
}
unsafe fn heap_attisnull(_tup: HeapTuple, _attnum: c_int, _tupleDesc: TupleDesc) -> bool {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn heap_modify_tuple_by_cols(
    _tuple: HeapTuple,
    _tupleDesc: TupleDesc,
    _nCols: c_int,
    _replCols: *const c_int,
    _replValues: *const Datum,
    _replIsnull: *const bool,
) -> HeapTuple {
    unimplemented!() // TODO(pg-port): access/common/heaptuple.c
}
unsafe fn TupleDescAttr(_tupdesc: TupleDesc, _i: c_int) -> *mut crate::catalog::pg_attribute::FormData_pg_attribute {
    unimplemented!() // TODO(pg-port): access/tupdesc.h
}

/*
 * Compare two TriggerDesc structures for logical equality.
 */
#[cfg(NOT_USED)]
pub unsafe fn equalTriggerDescs(trigdesc1: *mut TriggerDesc, trigdesc2: *mut TriggerDesc) -> bool {
    let mut i: c_int;
    let mut j: c_int;

    /*
     * We need not examine the hint flags, just the trigger array itself; if
     * we have the same triggers with the same types, the flags should match.
     *
     * As of 7.3 we assume trigger set ordering is significant in the
     * comparison; so we just compare corresponding slots of the two sets.
     *
     * Note: comparing the stringToNode forms of the WHEN clauses means that
     * parse column locations will affect the result.  This is okay as long as
     * this function is only used for detecting exact equality, as for example
     * in checking for staleness of a cache entry.
     */
    if !trigdesc1.is_null() {
        if trigdesc2.is_null() {
            return false;
        }
        if (*trigdesc1).numtriggers != (*trigdesc2).numtriggers {
            return false;
        }
        i = 0;
        while i < (*trigdesc1).numtriggers {
            let trig1: *mut Trigger = (*trigdesc1).triggers.offset(i as isize);
            let trig2: *mut Trigger = (*trigdesc2).triggers.offset(i as isize);

            if (*trig1).tgoid != (*trig2).tgoid {
                return false;
            }
            if strcmp((*trig1).tgname, (*trig2).tgname) != 0 {
                return false;
            }
            if (*trig1).tgfoid != (*trig2).tgfoid {
                return false;
            }
            if (*trig1).tgtype != (*trig2).tgtype {
                return false;
            }
            if (*trig1).tgenabled != (*trig2).tgenabled {
                return false;
            }
            if (*trig1).tgisinternal != (*trig2).tgisinternal {
                return false;
            }
            if (*trig1).tgisclone != (*trig2).tgisclone {
                return false;
            }
            if (*trig1).tgconstrrelid != (*trig2).tgconstrrelid {
                return false;
            }
            if (*trig1).tgconstrindid != (*trig2).tgconstrindid {
                return false;
            }
            if (*trig1).tgconstraint != (*trig2).tgconstraint {
                return false;
            }
            if (*trig1).tgdeferrable != (*trig2).tgdeferrable {
                return false;
            }
            if (*trig1).tginitdeferred != (*trig2).tginitdeferred {
                return false;
            }
            if (*trig1).tgnargs != (*trig2).tgnargs {
                return false;
            }
            if (*trig1).tgnattr != (*trig2).tgnattr {
                return false;
            }
            if (*trig1).tgnattr > 0
                && memcmp(
                    (*trig1).tgattr as *const c_void,
                    (*trig2).tgattr as *const c_void,
                    (*trig1).tgnattr as usize * core::mem::size_of::<i16>(),
                ) != 0
            {
                return false;
            }
            j = 0;
            while j < (*trig1).tgnargs as c_int {
                if strcmp(
                    *(*trig1).tgargs.offset(j as isize),
                    *(*trig2).tgargs.offset(j as isize),
                ) != 0
                {
                    return false;
                }
                j += 1;
            }
            if (*trig1).tgqual.is_null() && (*trig2).tgqual.is_null() {
                /* ok */
            } else if (*trig1).tgqual.is_null() || (*trig2).tgqual.is_null() {
                return false;
            } else if strcmp((*trig1).tgqual, (*trig2).tgqual) != 0 {
                return false;
            }
            if (*trig1).tgoldtable.is_null() && (*trig2).tgoldtable.is_null() {
                /* ok */
            } else if (*trig1).tgoldtable.is_null() || (*trig2).tgoldtable.is_null() {
                return false;
            } else if strcmp((*trig1).tgoldtable, (*trig2).tgoldtable) != 0 {
                return false;
            }
            if (*trig1).tgnewtable.is_null() && (*trig2).tgnewtable.is_null() {
                /* ok */
            } else if (*trig1).tgnewtable.is_null() || (*trig2).tgnewtable.is_null() {
                return false;
            } else if strcmp((*trig1).tgnewtable, (*trig2).tgnewtable) != 0 {
                return false;
            }
            i += 1;
        }
    } else if !trigdesc2.is_null() {
        return false;
    }
    true
}

#[cfg(NOT_USED)]
unsafe fn memcmp(_a: *const c_void, _b: *const c_void, _n: usize) -> c_int {
    unimplemented!() // TODO(pg-port): libc memcmp
}

// AFTER-trigger executor-side helpers.
unsafe fn CreateExecutorState() -> *mut EState {
    unimplemented!() // TODO(pg-port): executor/execUtils.c
}
unsafe fn FreeExecutorState(_estate: *mut EState) {
    unimplemented!() // TODO(pg-port): executor/execUtils.c
}
unsafe fn ExecCloseResultRelations(_estate: *mut EState) {
    unimplemented!() // TODO(pg-port): executor/execMain.c
}
unsafe fn ExecResetTupleTable(_tupleTable: *mut List, _shouldFree: bool) {
    unimplemented!() // TODO(pg-port): executor/execTuples.c
}
unsafe fn ExecGetTriggerResultRel(_estate: *mut EState, _relid: Oid, _rootRelInfo: *mut ResultRelInfo) -> *mut ResultRelInfo {
    unimplemented!() // TODO(pg-port): executor/execMain.c
}
unsafe fn ExecDropSingleTupleTableSlot(_slot: *mut TupleTableSlot) {
    unimplemented!() // TODO(pg-port): executor/execTuples.c
}
unsafe fn MakeSingleTupleTableSlot(_tupdesc: TupleDesc, _tts_ops: *const TupleTableSlotOps) -> *mut TupleTableSlot {
    unimplemented!() // TODO(pg-port): executor/execTuples.c
}

unsafe fn AfterTriggerExecute(
    _estate: *mut EState,
    _event: AfterTriggerEvent,
    _relInfo: *mut ResultRelInfo,
    _src_relInfo: *mut ResultRelInfo,
    _dst_relInfo: *mut ResultRelInfo,
    _trigdesc: *mut TriggerDesc,
    _finfo: *mut FmgrInfo,
    _instr: *mut Instrumentation,
    _per_tuple_context: MemoryContext,
    _trig_tuple_slot1: *mut TupleTableSlot,
    _trig_tuple_slot2: *mut TupleTableSlot,
) {
    unimplemented!() // TODO(pg-port): commands/trigger.c AfterTriggerExecute
}

unsafe fn GetAfterTriggersTableData(relid: Oid, cmdType: CmdType) -> *mut AfterTriggersTableData {
    let mut table: *mut AfterTriggersTableData;
    let qs: *mut AfterTriggersQueryData;
    let oldcxt: MemoryContext;

    /* At this level, cmdType should not be, eg, CMD_MERGE */
    /* Assert(cmdType == CMD_INSERT || cmdType == CMD_UPDATE || cmdType == CMD_DELETE); */

    /* Caller should have ensured query_depth is OK. */
    /* Assert(afterTriggers.query_depth >= 0 && afterTriggers.query_depth < afterTriggers.maxquerydepth); */
    qs = afterTriggers.query_stack.add(afterTriggers.query_depth as usize);

    crate::foreach!(lc, (*qs).tables, {
        table = lfirst(crate::current_cell!(lc)) as *mut AfterTriggersTableData;
        if (*table).relid == relid && (*table).cmdType == cmdType && !(*table).closed {
            return table;
        }
    });

    oldcxt = MemoryContextSwitchTo(CurTransactionContext);

    table = palloc0(core::mem::size_of::<AfterTriggersTableData>()) as *mut AfterTriggersTableData;
    (*table).relid = relid;
    (*table).cmdType = cmdType;
    (*qs).tables = lappend((*qs).tables, table as *mut c_void);

    MemoryContextSwitchTo(oldcxt);

    table
}

/*
 * Returns a TupleTableSlot suitable for holding the tuples to be put
 * into AfterTriggersTableData's transition table tuplestores.
 */
unsafe fn GetAfterTriggersStoreSlot(
    table: *mut AfterTriggersTableData,
    mut tupdesc: TupleDesc,
) -> *mut TupleTableSlot {
    /* Create it if not already done. */
    if (*table).storeslot.is_null() {
        let oldcxt: MemoryContext;

        /*
         * We need this slot only until AfterTriggerEndQuery, but making it
         * last till end-of-subxact is good enough.  It'll be freed by
         * AfterTriggerFreeQuery().  However, the passed-in tupdesc might have
         * a different lifespan, so we'd better make a copy of that.
         */
        oldcxt = MemoryContextSwitchTo(CurTransactionContext);
        tupdesc = CreateTupleDescCopy(tupdesc);
        (*table).storeslot = MakeSingleTupleTableSlot(tupdesc, &raw const TTSOpsVirtual);
        MemoryContextSwitchTo(oldcxt);
    }

    (*table).storeslot
}

/* ----------
 * AfterTriggerEnlargeQueryState()
 * ----------
 */
unsafe fn AfterTriggerEnlargeQueryState() {
    let mut init_depth = afterTriggers.maxquerydepth;

    /* Assert(afterTriggers.query_depth >= afterTriggers.maxquerydepth); */

    if afterTriggers.maxquerydepth == 0 {
        let new_alloc = Max!(afterTriggers.query_depth + 1, 8);

        afterTriggers.query_stack = MemoryContextAlloc(
            TopTransactionContext,
            new_alloc as usize * core::mem::size_of::<AfterTriggersQueryData>(),
        ) as *mut AfterTriggersQueryData;
        afterTriggers.maxquerydepth = new_alloc;
    } else {
        /* repalloc will keep the stack in the same context */
        let old_alloc = afterTriggers.maxquerydepth;
        let new_alloc = Max!(afterTriggers.query_depth + 1, old_alloc * 2);

        afterTriggers.query_stack = repalloc(
            afterTriggers.query_stack as *mut c_void,
            new_alloc as usize * core::mem::size_of::<AfterTriggersQueryData>(),
        ) as *mut AfterTriggersQueryData;
        afterTriggers.maxquerydepth = new_alloc;
    }

    /* Initialize new array entries to empty */
    while init_depth < afterTriggers.maxquerydepth {
        let qs = afterTriggers.query_stack.add(init_depth as usize);

        (*qs).events.head = core::ptr::null_mut();
        (*qs).events.tail = core::ptr::null_mut();
        (*qs).events.tailfree = core::ptr::null_mut();
        (*qs).fdw_tuplestore = core::ptr::null_mut();
        (*qs).tables = NIL;

        init_depth += 1;
    }
}

unsafe extern "C" fn RangeVarCallbackForRenameTrigger(
    _rv: *const RangeVar,
    _relid: Oid,
    _oldrelid: Oid,
    _arg: *mut c_void,
) {
    unimplemented!() // TODO(pg-port): commands/trigger.c RangeVarCallbackForRenameTrigger
}

// ===========================================================================
// AFTER-trigger transition-table / SET CONSTRAINTS machinery (trigger.c)
// ===========================================================================

use crate::nodes::pg_list::{NIL, lappend, lappend_oid, lfirst_oid, list_free, list_free_deep};

/*
 * If this is an UPDATE of a partitioned table root, we'll put the old and new
 * tuples in tuplestores attached to the current query level's FDW area.
 */
unsafe fn GetCurrentFDWTuplestore() -> *mut Tuplestorestate {
    let mut ret: *mut Tuplestorestate;

    ret = (*afterTriggers
        .query_stack
        .add(afterTriggers.query_depth as usize))
    .fdw_tuplestore;
    if ret.is_null() {
        let oldcxt: MemoryContext;
        let saveResourceOwner: ResourceOwner;

        /*
         * Make the tuplestore valid until end of subtransaction.  We really
         * only need it until AfterTriggerEndQuery().
         */
        oldcxt = MemoryContextSwitchTo(CurTransactionContext);
        saveResourceOwner = CurrentResourceOwner;
        CurrentResourceOwner = CurTransactionResourceOwner;

        ret = tuplestore_begin_heap(false, false, work_mem);

        CurrentResourceOwner = saveResourceOwner;
        MemoryContextSwitchTo(oldcxt);

        (*afterTriggers
            .query_stack
            .add(afterTriggers.query_depth as usize))
        .fdw_tuplestore = ret;
    }

    ret
}

/* ----------
 * AfterTriggerFreeQuery()
 *
 *	Release subsidiary storage for a query level.
 * ----------
 */
unsafe fn AfterTriggerFreeQuery(qs: *mut AfterTriggersQueryData) {
    let mut ts: *mut Tuplestorestate;
    let tables: *mut List;

    /* Drop the trigger events */
    afterTriggerFreeEventList(&raw mut (*qs).events);

    /* Drop FDW tuplestore if any */
    ts = (*qs).fdw_tuplestore;
    (*qs).fdw_tuplestore = core::ptr::null_mut();
    if !ts.is_null() {
        tuplestore_end(ts);
    }

    /* Release per-table subsidiary storage */
    tables = (*qs).tables;
    crate::foreach!(lc, tables, {
        let table = lfirst(crate::current_cell!(lc)) as *mut AfterTriggersTableData;

        ts = (*table).old_tuplestore;
        (*table).old_tuplestore = core::ptr::null_mut();
        if !ts.is_null() {
            tuplestore_end(ts);
        }
        ts = (*table).new_tuplestore;
        (*table).new_tuplestore = core::ptr::null_mut();
        if !ts.is_null() {
            tuplestore_end(ts);
        }
        if !(*table).storeslot.is_null() {
            let slot = (*table).storeslot;

            (*table).storeslot = core::ptr::null_mut();
            ExecDropSingleTupleTableSlot(slot);
        }
    });

    /*
     * Now free the AfterTriggersTableData structs and list cells.  Reset list
     * pointer first; if list_free_deep somehow gets an error, better to leak
     * that storage than have an infinite loop.
     */
    (*qs).tables = NIL;
    list_free_deep(tables);
}

/*
 * Returns the transition-table tuplestore for the given event, if any.
 */
unsafe fn GetAfterTriggersTransitionTable(
    event: c_int,
    oldslot: *mut TupleTableSlot,
    newslot: *mut TupleTableSlot,
    transition_capture: *mut TransitionCaptureState,
) -> *mut Tuplestorestate {
    let mut tuplestore: *mut Tuplestorestate = core::ptr::null_mut();
    let delete_old_table = (*transition_capture).tcs_delete_old_table;
    let update_old_table = (*transition_capture).tcs_update_old_table;
    let update_new_table = (*transition_capture).tcs_update_new_table;
    let insert_new_table = (*transition_capture).tcs_insert_new_table;

    /*
     * For INSERT events NEW should be non-NULL, for DELETE events OLD should
     * be non-NULL, whereas for UPDATE events normally both OLD and NEW are
     * non-NULL.
     */
    let _ = (delete_old_table, insert_new_table);

    if !TupIsNull(oldslot) {
        /* Assert(TupIsNull(newslot)); */
        if event as TriggerEvent == TRIGGER_EVENT_DELETE && delete_old_table {
            tuplestore =
                (*((*transition_capture).tcs_delete_private as *mut AfterTriggersTableData))
                    .old_tuplestore;
        } else if event as TriggerEvent == TRIGGER_EVENT_UPDATE && update_old_table {
            tuplestore =
                (*((*transition_capture).tcs_update_private as *mut AfterTriggersTableData))
                    .old_tuplestore;
        }
    } else if !TupIsNull(newslot) {
        /* Assert(TupIsNull(oldslot)); */
        if event as TriggerEvent == TRIGGER_EVENT_INSERT && insert_new_table {
            tuplestore =
                (*((*transition_capture).tcs_insert_private as *mut AfterTriggersTableData))
                    .new_tuplestore;
        } else if event as TriggerEvent == TRIGGER_EVENT_UPDATE && update_new_table {
            tuplestore =
                (*((*transition_capture).tcs_update_private as *mut AfterTriggersTableData))
                    .new_tuplestore;
        }
    }

    tuplestore
}

/*
 * Add the given heap tuple to the given tuplestore, applying the conversion
 * map if necessary.
 *
 * If original_insert_tuple is given, we can add that tuple without conversion.
 */
unsafe fn TransitionTableAddTuple(
    _estate: *mut EState,
    event: c_int,
    transition_capture: *mut TransitionCaptureState,
    relinfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    original_insert_tuple: *mut TupleTableSlot,
    tuplestore: *mut Tuplestorestate,
) {
    let map: *mut TupleConversionMap;

    /*
     * Nothing needs to be done if we don't have a tuplestore.
     */
    if tuplestore.is_null() {
        return;
    }

    if !original_insert_tuple.is_null() {
        tuplestore_puttupleslot(tuplestore, original_insert_tuple);
    } else if {
        map = ExecGetChildToRootMap(relinfo);
        !map.is_null()
    } {
        let table: *mut AfterTriggersTableData;
        let storeslot: *mut TupleTableSlot;

        match event as TriggerEvent {
            TRIGGER_EVENT_INSERT => {
                table = (*transition_capture).tcs_insert_private as *mut AfterTriggersTableData;
            }
            TRIGGER_EVENT_UPDATE => {
                table = (*transition_capture).tcs_update_private as *mut AfterTriggersTableData;
            }
            TRIGGER_EVENT_DELETE => {
                table = (*transition_capture).tcs_delete_private as *mut AfterTriggersTableData;
            }
            _ => {
                elog!(ERROR, "invalid after-trigger event code: {}", event);
                #[allow(unreachable_code)]
                {
                    table = core::ptr::null_mut(); /* keep compiler quiet */
                }
            }
        }

        storeslot = GetAfterTriggersStoreSlot(table, (*map).outdesc);
        execute_attr_map_slot((*map).attrMap, slot, storeslot);
        tuplestore_puttupleslot(tuplestore, storeslot);
    } else {
        tuplestore_puttupleslot(tuplestore, slot);
    }
}

/*
 * Create an empty SetConstraintState with room for numalloc trigstates
 */
unsafe fn SetConstraintStateCreate(mut numalloc: c_int) -> SetConstraintState {
    let state: SetConstraintState;

    /* Behave sanely with numalloc == 0 */
    if numalloc <= 0 {
        numalloc = 1;
    }

    /*
     * We assume that zeroing will correctly initialize the state values.
     */
    state = MemoryContextAllocZero(
        TopTransactionContext,
        offset_of_SetConstraintStateData_trigstates()
            + numalloc as usize * core::mem::size_of::<SetConstraintTriggerData>(),
    ) as SetConstraintState;

    (*state).numalloc = numalloc;

    state
}

/*
 * Copy a SetConstraintState
 */
unsafe fn SetConstraintStateCopy(origstate: SetConstraintState) -> SetConstraintState {
    let state: SetConstraintState;

    state = SetConstraintStateCreate((*origstate).numstates);

    (*state).all_isset = (*origstate).all_isset;
    (*state).all_isdeferred = (*origstate).all_isdeferred;
    (*state).numstates = (*origstate).numstates;
    core::ptr::copy_nonoverlapping(
        (*origstate).trigstates.as_ptr(),
        (*state).trigstates.as_mut_ptr(),
        (*origstate).numstates as usize,
    );

    state
}

/*
 * Add a per-trigger item to a SetConstraintState.  Returns possibly-changed
 * pointer to the state object (it will change if we have to repalloc).
 */
unsafe fn SetConstraintStateAddItem(
    mut state: SetConstraintState,
    tgoid: Oid,
    tgisdeferred: bool,
) -> SetConstraintState {
    if (*state).numstates >= (*state).numalloc {
        let mut newalloc = (*state).numalloc * 2;

        newalloc = Max!(newalloc, 8); /* in case original has size 0 */
        state = repalloc(
            state as *mut c_void,
            offset_of_SetConstraintStateData_trigstates()
                + newalloc as usize * core::mem::size_of::<SetConstraintTriggerData>(),
        ) as SetConstraintState;
        (*state).numalloc = newalloc;
        /* Assert((*state).numstates < (*state).numalloc); */
    }

    let idx = (*state).numstates as usize;
    let slot = (*state).trigstates.as_mut_ptr().add(idx);
    (*slot).sct_tgoid = tgoid;
    (*slot).sct_tgisdeferred = tgisdeferred;
    (*state).numstates += 1;

    state
}

// offsetof(SetConstraintStateData, trigstates)
#[inline]
fn offset_of_SetConstraintStateData_trigstates() -> usize {
    core::mem::offset_of!(SetConstraintStateData, trigstates)
}

// --- imported / locally-stubbed dependencies for the above ---

use crate::nodes::execnodes::{TransitionCaptureState, TupleConversionMap};

/// ResourceOwner (utils/resowner.h) - opaque.
type ResourceOwner = *mut c_void;
/// TODO(pg-port): real CurrentResourceOwner in utils/resowner/resowner.c.
static mut CurrentResourceOwner: ResourceOwner = core::ptr::null_mut();
/// TODO(pg-port): real CurTransactionResourceOwner in access/transam/xact.c.
static mut CurTransactionResourceOwner: ResourceOwner = core::ptr::null_mut();
/// TODO(pg-port): real CurTransactionContext in access/transam/xact.c.
static mut CurTransactionContext: MemoryContext = core::ptr::null_mut();
/// TODO(pg-port): real work_mem GUC in utils/misc/guc_tables.c.
static mut work_mem: c_int = 4096;

use crate::executor::tuptable::TupIsNull;
use crate::access::common::tupdesc::CreateTupleDescCopy;
use crate::access::common::tupconvert::execute_attr_map_slot;
use crate::executor::executor::ExecGetChildToRootMap;

unsafe fn palloc0(size: usize) -> *mut c_void {
    crate::utils::palloc::MemoryContextAllocZero(CurrentMemoryContext, size)
}
unsafe fn repalloc(ptr: *mut c_void, size: usize) -> *mut c_void {
    crate::utils::palloc::repalloc(ptr, size)
}
unsafe fn MemoryContextAllocZero(ctx: MemoryContext, size: usize) -> *mut c_void {
    crate::utils::palloc::MemoryContextAllocZero(ctx, size)
}
// Tuplestore helpers operate on this file's local opaque `Tuplestorestate`
// (the trigger-firing machinery's tuplestores); kept as stubs to stay
// type-consistent until that module is wired up.
unsafe fn tuplestore_begin_heap(
    _randomAccess: bool,
    _interXact: bool,
    _maxKBytes: c_int,
) -> *mut Tuplestorestate {
    unimplemented!() // TODO(pg-port): utils/sort/tuplestore.c
}
unsafe fn tuplestore_end(_state: *mut Tuplestorestate) {
    unimplemented!() // TODO(pg-port): utils/sort/tuplestore.c
}
unsafe fn tuplestore_puttupleslot(_state: *mut Tuplestorestate, _slot: *mut TupleTableSlot) {
    unimplemented!() // TODO(pg-port): utils/sort/tuplestore.c
}

/// TTSOpsVirtual (executor/tuptable.h) - opaque stub ops table.
static TTSOpsVirtual: TupleTableSlotOps = TupleTableSlotOps {
    base_slot_size: 0,
    init: None,
    release: None,
    clear: None,
    getsomeattrs: None,
    getsysattr: None,
    is_current_xact_tuple: None,
    materialize: None,
    copyslot: None,
    get_heap_tuple: None,
    get_minimal_tuple: None,
    copy_heap_tuple: None,
    copy_minimal_tuple: None,
};

macro_rules! Max {
    ($a:expr, $b:expr) => {
        core::cmp::max($a, $b)
    };
}
use Max;


/*
 * ExecCallTriggerFunc and the per-event Exec*Triggers entry points.
 * 1:1 translation of trigger.c.  Per the port's ereport! convention, only
 * errmsg! is kept on each ereport (errcode()/errdetail()/errhint() folded into
 * "C also:" comments).
 */

/*
 * Fetch tuple into "oldslot", dealing with locking and EPQ if necessary.
 * TODO(pg-port): real static GetTupleForTrigger lives in commands/trigger.c.
 */
unsafe fn GetTupleForTrigger(
    _estate: *mut EState,
    _epqstate: *mut crate::nodes::execnodes::EPQState,
    _relinfo: *mut ResultRelInfo,
    _tid: *mut ItemPointerData,
    _lockmode: LockTupleMode,
    _oldslot: *mut TupleTableSlot,
    _do_epq_recheck: bool,
    _epqslot: *mut *mut TupleTableSlot,
    _tmresultp: *mut crate::access::table::tableam::TM_Result,
    _tmfdp: *mut crate::access::table::tableam::TM_FailureData,
) -> bool {
    /* TODO(pg-port): commands/trigger.c GetTupleForTrigger */
    true
}

use crate::executor::execTuples::{ExecFetchSlotHeapTuple, ExecForceStoreHeapTuple};
use crate::executor::executor::GetPerTupleMemoryContext;
use crate::access::common::heaptuple::heap_freetuple;
use crate::executor::execMain::{LockTupleMode, LockTupleExclusive};

/* TODO(pg-port): not-pub in executor/execReplication.rs / execMain.c */
unsafe fn ExecPartitionCheck(
    _resultRelInfo: *mut ResultRelInfo,
    _slot: *mut TupleTableSlot,
    _estate: *mut EState,
    _emitError: bool,
) -> bool {
    /* TODO(pg-port): executor/execMain.c ExecPartitionCheck */
    true
}

/*
 * Call a trigger function.
 *
 *		trigdata: trigger descriptor.
 *		tgindx: trigger's index in finfo and instr arrays.
 *		finfo: array of cached trigger function call information.
 *		instr: optional array of EXPLAIN ANALYZE instrumentation state.
 *		per_tuple_context: memory context to execute the function in.
 *
 * Returns the tuple (or NULL) as returned by the function.
 */
unsafe fn ExecCallTriggerFunc(
    trigdata: *mut TriggerData,
    tgindx: c_int,
    mut finfo: *mut FmgrInfo,
    instr: *mut Instrumentation,
    per_tuple_context: MemoryContext,
) -> HeapTuple {
    use crate::utils::activity::pgstat_function::{
        pgstat_init_function_usage, pgstat_end_function_usage, PgStat_FunctionCallUsage,
    };
    use crate::utils::fmgr::fmgr_info;
    use crate::executor::instrument::{InstrStartNode, InstrStopNode};
    use crate::postgres::DatumGetPointer;
    use crate::{LOCAL_FCINFO, InitFunctionCallInfoData, FunctionCallInvoke};

    LOCAL_FCINFO!(fcinfo, 0);
    let mut fcusage: PgStat_FunctionCallUsage = std::mem::zeroed();
    let result: Datum;
    let oldContext: MemoryContext;

    /*
     * Protect against code paths that may fail to initialize transition table
     * info.
     */
    debug_assert!(
        ((TRIGGER_FIRED_BY_INSERT((*trigdata).tg_event)
            || TRIGGER_FIRED_BY_UPDATE((*trigdata).tg_event)
            || TRIGGER_FIRED_BY_DELETE((*trigdata).tg_event))
            && TRIGGER_FIRED_AFTER((*trigdata).tg_event)
            && ((*trigdata).tg_event & AFTER_TRIGGER_DEFERRABLE) == 0
            && ((*trigdata).tg_event & AFTER_TRIGGER_INITDEFERRED) == 0)
            || ((*trigdata).tg_oldtable.is_null() && (*trigdata).tg_newtable.is_null())
    );

    finfo = finfo.add(tgindx as usize);

    /*
     * We cache fmgr lookup info, to avoid making the lookup again on each
     * call.
     */
    if (*finfo).fn_oid == InvalidOid {
        fmgr_info((*(*trigdata).tg_trigger).tgfoid, finfo);
    }

    debug_assert!((*finfo).fn_oid == (*(*trigdata).tg_trigger).tgfoid);

    /*
     * If doing EXPLAIN ANALYZE, start charging time to this trigger.
     */
    if !instr.is_null() {
        InstrStartNode(instr.add(tgindx as usize));
    }

    /*
     * Do the function evaluation in the per-tuple memory context, so that
     * leaked memory will be reclaimed once per tuple. Note in particular that
     * any new tuple created by the trigger function will live till the end of
     * the tuple cycle.
     */
    oldContext = MemoryContextSwitchTo(per_tuple_context);

    /*
     * Call the function, passing no arguments but setting a context.
     */
    InitFunctionCallInfoData!(
        fcinfo,
        finfo,
        0,
        InvalidOid,
        trigdata as *mut crate::nodes::nodes::Node,
        core::ptr::null_mut()
    );

    pgstat_init_function_usage(fcinfo, &mut fcusage);

    MyTriggerDepth += 1;
    /* PG_TRY/PG_FINALLY: ensure MyTriggerDepth is decremented */
    result = FunctionCallInvoke!(fcinfo);
    MyTriggerDepth -= 1;

    pgstat_end_function_usage(&mut fcusage, true);

    MemoryContextSwitchTo(oldContext);

    /*
     * Trigger protocol allows function to return a null pointer, but NOT to
     * set the isnull result flag.
     */
    if (*fcinfo).isnull {
        ereport!(
            ERROR,
            errmsg!(
                "trigger function {} returned null value",
                (*(*fcinfo).flinfo).fn_oid
            )
        );
        /* C also: errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED) */
    }

    /*
     * If doing EXPLAIN ANALYZE, stop charging time to this trigger, and count
     * one "tuple returned" (really the number of firings).
     */
    if !instr.is_null() {
        InstrStopNode(instr.add(tgindx as usize), 1.0);
    }

    DatumGetPointer(result) as HeapTuple
}

pub unsafe fn ExecBSInsertTriggers(estate: *mut EState, relinfo: *mut ResultRelInfo) {
    let trigdesc: *mut TriggerDesc;
    let mut LocTriggerData: TriggerData = std::mem::zeroed();

    trigdesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    if trigdesc.is_null() {
        return;
    }
    if !(*trigdesc).trig_insert_before_statement {
        return;
    }

    /* no-op if we already fired BS triggers in this context */
    if before_stmt_triggers_fired(
        RelationGetRelid((*relinfo).ri_RelationDesc),
        CmdType::CMD_INSERT,
    ) {
        return;
    }

    LocTriggerData.r#type = NodeTag::T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = (*relinfo).ri_RelationDesc;
    let mut i = 0;
    while i < (*trigdesc).numtriggers {
        let trigger: *mut Trigger = &mut *(*trigdesc).triggers.offset(i as isize);
        let newtuple: HeapTuple;

        if !TRIGGER_TYPE_MATCHES(
            (*trigger).tgtype,
            TRIGGER_TYPE_STATEMENT,
            TRIGGER_TYPE_BEFORE,
            TRIGGER_TYPE_INSERT,
        ) {
            i += 1;
            continue;
        }
        if !TriggerEnabled(
            estate,
            relinfo,
            trigger,
            LocTriggerData.tg_event as c_int,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        ) {
            i += 1;
            continue;
        }

        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(
            &mut LocTriggerData,
            i,
            (*relinfo).ri_TrigFunctions,
            (*relinfo).ri_TrigInstrument,
            GetPerTupleMemoryContext(estate),
        );

        if !newtuple.is_null() {
            ereport!(ERROR, errmsg!("BEFORE STATEMENT trigger cannot return a value"));
            /* C also: errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED) */
        }
        i += 1;
    }
}

pub unsafe fn ExecASInsertTriggers(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    transition_capture: *mut TransitionCaptureState,
) {
    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    if !trigdesc.is_null() && (*trigdesc).trig_insert_after_statement {
        AfterTriggerSaveEvent(
            estate,
            relinfo,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            TRIGGER_EVENT_INSERT as c_int,
            false,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            crate::nodes::pg_list::NIL,
            core::ptr::null_mut(),
            transition_capture,
            false,
        );
    }
}

pub unsafe fn ExecBRInsertTriggers(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
) -> bool {
    use crate::executor::execUtils::ExecGetTriggerOldSlot;
    use crate::utils::rel::{RelationGetDescr, RelationGetNamespace};
    let _ = ExecGetTriggerOldSlot;
    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;
    let mut newtuple: HeapTuple = core::ptr::null_mut();
    let mut should_free: bool = false;
    let mut LocTriggerData: TriggerData = std::mem::zeroed();

    LocTriggerData.r#type = NodeTag::T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = (*relinfo).ri_RelationDesc;
    let mut i = 0;
    while i < (*trigdesc).numtriggers {
        let trigger: *mut Trigger = &mut *(*trigdesc).triggers.offset(i as isize);
        let oldtuple: HeapTuple;

        if !TRIGGER_TYPE_MATCHES(
            (*trigger).tgtype,
            TRIGGER_TYPE_ROW,
            TRIGGER_TYPE_BEFORE,
            TRIGGER_TYPE_INSERT,
        ) {
            i += 1;
            continue;
        }
        if !TriggerEnabled(
            estate,
            relinfo,
            trigger,
            LocTriggerData.tg_event as c_int,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            slot,
        ) {
            i += 1;
            continue;
        }

        if newtuple.is_null() {
            newtuple = ExecFetchSlotHeapTuple(slot, true, &mut should_free);
        }

        LocTriggerData.tg_trigslot = slot;
        oldtuple = newtuple;
        LocTriggerData.tg_trigtuple = oldtuple;
        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(
            &mut LocTriggerData,
            i,
            (*relinfo).ri_TrigFunctions,
            (*relinfo).ri_TrigInstrument,
            GetPerTupleMemoryContext(estate),
        );
        if newtuple.is_null() {
            if should_free {
                heap_freetuple(oldtuple);
            }
            return false; /* "do nothing" */
        } else if newtuple != oldtuple {
            newtuple =
                check_modified_virtual_generated(RelationGetDescr((*relinfo).ri_RelationDesc), newtuple);

            ExecForceStoreHeapTuple(newtuple, slot, false);

            /*
             * After a tuple in a partition goes through a trigger, the user
             * could have changed the partition key enough that the tuple no
             * longer fits the partition.  Verify that.
             */
            if (*trigger).tgisclone && !ExecPartitionCheck(relinfo, slot, estate, false) {
                ereport!(
                    ERROR,
                    errmsg!(
                        "moving row to another partition during a BEFORE FOR EACH ROW trigger is not supported"
                    )
                );
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 * errdetail("Before executing trigger \"%s\", the row was to be in partition \"%s.%s\".",
                 *   trigger->tgname,
                 *   get_namespace_name(RelationGetNamespace(relinfo->ri_RelationDesc)),
                 *   RelationGetRelationName(relinfo->ri_RelationDesc)) */
                let _ = RelationGetNamespace;
            }

            if should_free {
                heap_freetuple(oldtuple);
            }

            /* signal tuple should be re-fetched if used */
            newtuple = core::ptr::null_mut();
        }
        i += 1;
    }

    true
}

pub unsafe fn ExecARInsertTriggers(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    slot: *mut TupleTableSlot,
    recheckIndexes: *mut List,
    transition_capture: *mut TransitionCaptureState,
) {
    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    if !(*relinfo).ri_FdwRoutine.is_null()
        && !transition_capture.is_null()
        && (*transition_capture).tcs_insert_new_table
    {
        debug_assert!(!(*relinfo).ri_RootResultRelInfo.is_null());
        ereport!(
            ERROR,
            errmsg!("cannot collect transition tuples from child foreign tables")
        );
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    if (!trigdesc.is_null() && (*trigdesc).trig_insert_after_row)
        || (!transition_capture.is_null() && (*transition_capture).tcs_insert_new_table)
    {
        AfterTriggerSaveEvent(
            estate,
            relinfo,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            TRIGGER_EVENT_INSERT as c_int,
            true,
            core::ptr::null_mut(),
            slot,
            recheckIndexes,
            core::ptr::null_mut(),
            transition_capture,
            false,
        );
    }
}

pub unsafe fn ExecBSDeleteTriggers(estate: *mut EState, relinfo: *mut ResultRelInfo) {
    let trigdesc: *mut TriggerDesc;
    let mut LocTriggerData: TriggerData = std::mem::zeroed();

    trigdesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    if trigdesc.is_null() {
        return;
    }
    if !(*trigdesc).trig_delete_before_statement {
        return;
    }

    /* no-op if we already fired BS triggers in this context */
    if before_stmt_triggers_fired(
        RelationGetRelid((*relinfo).ri_RelationDesc),
        CmdType::CMD_DELETE,
    ) {
        return;
    }

    LocTriggerData.r#type = NodeTag::T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_DELETE | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = (*relinfo).ri_RelationDesc;
    let mut i = 0;
    while i < (*trigdesc).numtriggers {
        let trigger: *mut Trigger = &mut *(*trigdesc).triggers.offset(i as isize);
        let newtuple: HeapTuple;

        if !TRIGGER_TYPE_MATCHES(
            (*trigger).tgtype,
            TRIGGER_TYPE_STATEMENT,
            TRIGGER_TYPE_BEFORE,
            TRIGGER_TYPE_DELETE,
        ) {
            i += 1;
            continue;
        }
        if !TriggerEnabled(
            estate,
            relinfo,
            trigger,
            LocTriggerData.tg_event as c_int,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        ) {
            i += 1;
            continue;
        }

        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(
            &mut LocTriggerData,
            i,
            (*relinfo).ri_TrigFunctions,
            (*relinfo).ri_TrigInstrument,
            GetPerTupleMemoryContext(estate),
        );

        if !newtuple.is_null() {
            ereport!(ERROR, errmsg!("BEFORE STATEMENT trigger cannot return a value"));
            /* C also: errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED) */
        }
        i += 1;
    }
}

pub unsafe fn ExecASDeleteTriggers(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    transition_capture: *mut TransitionCaptureState,
) {
    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    if !trigdesc.is_null() && (*trigdesc).trig_delete_after_statement {
        AfterTriggerSaveEvent(
            estate,
            relinfo,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            TRIGGER_EVENT_DELETE as c_int,
            false,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            crate::nodes::pg_list::NIL,
            core::ptr::null_mut(),
            transition_capture,
            false,
        );
    }
}

/*
 * Execute BEFORE ROW DELETE triggers.
 *
 * True indicates caller can proceed with the delete.  False indicates caller
 * need to suppress the delete and additionally if requested, we need to pass
 * back the concurrently updated tuple if any.
 */
pub unsafe fn ExecBRDeleteTriggers(
    estate: *mut EState,
    epqstate: *mut crate::nodes::execnodes::EPQState,
    relinfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    fdw_trigtuple: HeapTuple,
    epqslot: *mut *mut TupleTableSlot,
    tmresult: *mut crate::access::table::tableam::TM_Result,
    tmfd: *mut crate::access::table::tableam::TM_FailureData,
    is_merge_delete: bool,
) -> bool {
    use crate::executor::execUtils::ExecGetTriggerOldSlot;
    use crate::storage::itemptr::ItemPointerIsValid;

    let slot: *mut TupleTableSlot = ExecGetTriggerOldSlot(estate, relinfo);
    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;
    let mut result: bool = true;
    let mut LocTriggerData: TriggerData = std::mem::zeroed();
    let trigtuple: HeapTuple;
    let mut should_free: bool = false;

    debug_assert!(!fdw_trigtuple.is_null() ^ ItemPointerIsValid(tupleid));
    if fdw_trigtuple.is_null() {
        let mut epqslot_candidate: *mut TupleTableSlot = core::ptr::null_mut();

        /*
         * Get a copy of the on-disk tuple we are planning to delete.  In
         * general, if the tuple has been concurrently updated, we should
         * recheck it using EPQ.  However, if this is a MERGE DELETE action,
         * we skip this EPQ recheck and leave it to the caller (it must do
         * additional rechecking, and might end up executing a different
         * action entirely).
         */
        if !GetTupleForTrigger(
            estate,
            epqstate,
            relinfo,
            tupleid,
            LockTupleExclusive,
            slot,
            !is_merge_delete,
            &mut epqslot_candidate,
            tmresult,
            tmfd,
        ) {
            return false;
        }

        /*
         * If the tuple was concurrently updated and the caller of this
         * function requested for the updated tuple, skip the trigger
         * execution.
         */
        if !epqslot_candidate.is_null() && !epqslot.is_null() {
            *epqslot = epqslot_candidate;
            return false;
        }

        trigtuple = ExecFetchSlotHeapTuple(slot, true, &mut should_free);
    } else {
        trigtuple = fdw_trigtuple;
        ExecForceStoreHeapTuple(trigtuple, slot, false);
    }

    LocTriggerData.r#type = NodeTag::T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_DELETE | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = (*relinfo).ri_RelationDesc;
    let mut i = 0;
    while i < (*trigdesc).numtriggers {
        let newtuple: HeapTuple;
        let trigger: *mut Trigger = &mut *(*trigdesc).triggers.offset(i as isize);

        if !TRIGGER_TYPE_MATCHES(
            (*trigger).tgtype,
            TRIGGER_TYPE_ROW,
            TRIGGER_TYPE_BEFORE,
            TRIGGER_TYPE_DELETE,
        ) {
            i += 1;
            continue;
        }
        if !TriggerEnabled(
            estate,
            relinfo,
            trigger,
            LocTriggerData.tg_event as c_int,
            core::ptr::null_mut(),
            slot,
            core::ptr::null_mut(),
        ) {
            i += 1;
            continue;
        }

        LocTriggerData.tg_trigslot = slot;
        LocTriggerData.tg_trigtuple = trigtuple;
        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(
            &mut LocTriggerData,
            i,
            (*relinfo).ri_TrigFunctions,
            (*relinfo).ri_TrigInstrument,
            GetPerTupleMemoryContext(estate),
        );
        if newtuple.is_null() {
            result = false; /* tell caller to suppress delete */
            break;
        }
        if newtuple != trigtuple {
            heap_freetuple(newtuple);
        }
        i += 1;
    }
    if should_free {
        heap_freetuple(trigtuple);
    }

    result
}

/*
 * Note: is_crosspart_update must be true if the DELETE is being performed
 * as part of a cross-partition update.
 */
pub unsafe fn ExecARDeleteTriggers(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    fdw_trigtuple: HeapTuple,
    transition_capture: *mut TransitionCaptureState,
    is_crosspart_update: bool,
) {
    use crate::executor::execUtils::ExecGetTriggerOldSlot;
    use crate::storage::itemptr::ItemPointerIsValid;

    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    if !(*relinfo).ri_FdwRoutine.is_null()
        && !transition_capture.is_null()
        && (*transition_capture).tcs_delete_old_table
    {
        debug_assert!(!(*relinfo).ri_RootResultRelInfo.is_null());
        ereport!(
            ERROR,
            errmsg!("cannot collect transition tuples from child foreign tables")
        );
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    if (!trigdesc.is_null() && (*trigdesc).trig_delete_after_row)
        || (!transition_capture.is_null() && (*transition_capture).tcs_delete_old_table)
    {
        let slot: *mut TupleTableSlot = ExecGetTriggerOldSlot(estate, relinfo);

        debug_assert!(!fdw_trigtuple.is_null() ^ ItemPointerIsValid(tupleid));
        if fdw_trigtuple.is_null() {
            GetTupleForTrigger(
                estate,
                core::ptr::null_mut(),
                relinfo,
                tupleid,
                LockTupleExclusive,
                slot,
                false,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            );
        } else {
            ExecForceStoreHeapTuple(fdw_trigtuple, slot, false);
        }

        AfterTriggerSaveEvent(
            estate,
            relinfo,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            TRIGGER_EVENT_DELETE as c_int,
            true,
            slot,
            core::ptr::null_mut(),
            crate::nodes::pg_list::NIL,
            core::ptr::null_mut(),
            transition_capture,
            is_crosspart_update,
        );
    }
}

pub unsafe fn ExecBSUpdateTriggers(estate: *mut EState, relinfo: *mut ResultRelInfo) {
    use crate::executor::execUtils::ExecGetAllUpdatedCols;

    let trigdesc: *mut TriggerDesc;
    let mut LocTriggerData: TriggerData = std::mem::zeroed();
    let updatedCols: *mut Bitmapset;

    trigdesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    if trigdesc.is_null() {
        return;
    }
    if !(*trigdesc).trig_update_before_statement {
        return;
    }

    /* no-op if we already fired BS triggers in this context */
    if before_stmt_triggers_fired(
        RelationGetRelid((*relinfo).ri_RelationDesc),
        CmdType::CMD_UPDATE,
    ) {
        return;
    }

    /* statement-level triggers operate on the parent table */
    debug_assert!((*relinfo).ri_RootResultRelInfo.is_null());

    updatedCols = ExecGetAllUpdatedCols(relinfo, estate);

    LocTriggerData.r#type = NodeTag::T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_UPDATE | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = (*relinfo).ri_RelationDesc;
    LocTriggerData.tg_updatedcols = updatedCols;
    let mut i = 0;
    while i < (*trigdesc).numtriggers {
        let trigger: *mut Trigger = &mut *(*trigdesc).triggers.offset(i as isize);
        let newtuple: HeapTuple;

        if !TRIGGER_TYPE_MATCHES(
            (*trigger).tgtype,
            TRIGGER_TYPE_STATEMENT,
            TRIGGER_TYPE_BEFORE,
            TRIGGER_TYPE_UPDATE,
        ) {
            i += 1;
            continue;
        }
        if !TriggerEnabled(
            estate,
            relinfo,
            trigger,
            LocTriggerData.tg_event as c_int,
            updatedCols,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        ) {
            i += 1;
            continue;
        }

        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(
            &mut LocTriggerData,
            i,
            (*relinfo).ri_TrigFunctions,
            (*relinfo).ri_TrigInstrument,
            GetPerTupleMemoryContext(estate),
        );

        if !newtuple.is_null() {
            ereport!(ERROR, errmsg!("BEFORE STATEMENT trigger cannot return a value"));
            /* C also: errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED) */
        }
        i += 1;
    }
}

pub unsafe fn ExecASUpdateTriggers(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    transition_capture: *mut TransitionCaptureState,
) {
    use crate::executor::execUtils::ExecGetAllUpdatedCols;

    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    /* statement-level triggers operate on the parent table */
    debug_assert!((*relinfo).ri_RootResultRelInfo.is_null());

    if !trigdesc.is_null() && (*trigdesc).trig_update_after_statement {
        AfterTriggerSaveEvent(
            estate,
            relinfo,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            TRIGGER_EVENT_UPDATE as c_int,
            false,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            crate::nodes::pg_list::NIL,
            ExecGetAllUpdatedCols(relinfo, estate),
            transition_capture,
            false,
        );
    }
}

pub unsafe fn ExecBRUpdateTriggers(
    estate: *mut EState,
    epqstate: *mut crate::nodes::execnodes::EPQState,
    relinfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    fdw_trigtuple: HeapTuple,
    newslot: *mut TupleTableSlot,
    tmresult: *mut crate::access::table::tableam::TM_Result,
    tmfd: *mut crate::access::table::tableam::TM_FailureData,
    is_merge_update: bool,
) -> bool {
    use crate::executor::execUtils::{ExecGetAllUpdatedCols, ExecGetTriggerOldSlot};
    use crate::executor::execMain::ExecUpdateLockMode;
    use crate::executor::nodeModifyTable::ExecGetUpdateNewTuple;
    use crate::executor::tuptable::{ExecCopySlot, ExecMaterializeSlot};
    use crate::utils::rel::RelationGetDescr;
    use crate::storage::itemptr::ItemPointerIsValid;
    use crate::c::unlikely;

    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;
    let oldslot: *mut TupleTableSlot = ExecGetTriggerOldSlot(estate, relinfo);
    let mut newtuple: HeapTuple = core::ptr::null_mut();
    let trigtuple: HeapTuple;
    let mut should_free_trig: bool = false;
    let mut should_free_new: bool = false;
    let mut LocTriggerData: TriggerData = std::mem::zeroed();
    let updatedCols: *mut Bitmapset;
    let lockmode: LockTupleMode;

    /* Determine lock mode to use */
    lockmode = ExecUpdateLockMode(estate, relinfo);

    debug_assert!(!fdw_trigtuple.is_null() ^ ItemPointerIsValid(tupleid));
    if fdw_trigtuple.is_null() {
        let mut epqslot_candidate: *mut TupleTableSlot = core::ptr::null_mut();

        /*
         * Get a copy of the on-disk tuple we are planning to update.  In
         * general, if the tuple has been concurrently updated, we should
         * recheck it using EPQ.  However, if this is a MERGE UPDATE action,
         * we skip this EPQ recheck and leave it to the caller (it must do
         * additional rechecking, and might end up executing a different
         * action entirely).
         */
        if !GetTupleForTrigger(
            estate,
            epqstate,
            relinfo,
            tupleid,
            lockmode,
            oldslot,
            !is_merge_update,
            &mut epqslot_candidate,
            tmresult,
            tmfd,
        ) {
            return false; /* cancel the update action */
        }

        /*
         * In READ COMMITTED isolation level it's possible that target tuple
         * was changed due to concurrent update.  In that case we have a raw
         * subplan output tuple in epqslot_candidate, and need to form a new
         * insertable tuple using ExecGetUpdateNewTuple to replace the one we
         * received in newslot.  Neither we nor our callers have any further
         * interest in the passed-in tuple, so it's okay to overwrite newslot
         * with the newer data.
         */
        if !epqslot_candidate.is_null() {
            let epqslot_clean: *mut TupleTableSlot;

            epqslot_clean = ExecGetUpdateNewTuple(relinfo, epqslot_candidate, oldslot);

            /*
             * Typically, the caller's newslot was also generated by
             * ExecGetUpdateNewTuple, so that epqslot_clean will be the same
             * slot and copying is not needed.  But do the right thing if it
             * isn't.
             */
            if unlikely(newslot != epqslot_clean) {
                ExecCopySlot(newslot, epqslot_clean);
            }

            /*
             * At this point newslot contains a virtual tuple that may
             * reference some fields of oldslot's tuple in some disk buffer.
             * If that tuple is in a different page than the original target
             * tuple, then our only pin on that buffer is oldslot's, and we're
             * about to release it.  Hence we'd better materialize newslot to
             * ensure it doesn't contain references into an unpinned buffer.
             * (We'd materialize it below anyway, but too late for safety.)
             */
            ExecMaterializeSlot(newslot);
        }

        /*
         * Here we convert oldslot to a materialized slot holding trigtuple.
         * Neither slot passed to the triggers will hold any buffer pin.
         */
        trigtuple = ExecFetchSlotHeapTuple(oldslot, true, &mut should_free_trig);
    } else {
        /* Put the FDW-supplied tuple into oldslot to unify the cases */
        ExecForceStoreHeapTuple(fdw_trigtuple, oldslot, false);
        trigtuple = fdw_trigtuple;
    }

    LocTriggerData.r#type = NodeTag::T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_UPDATE | TRIGGER_EVENT_ROW | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = (*relinfo).ri_RelationDesc;
    updatedCols = ExecGetAllUpdatedCols(relinfo, estate);
    LocTriggerData.tg_updatedcols = updatedCols;
    let mut i = 0;
    while i < (*trigdesc).numtriggers {
        let trigger: *mut Trigger = &mut *(*trigdesc).triggers.offset(i as isize);
        let oldtuple: HeapTuple;

        if !TRIGGER_TYPE_MATCHES(
            (*trigger).tgtype,
            TRIGGER_TYPE_ROW,
            TRIGGER_TYPE_BEFORE,
            TRIGGER_TYPE_UPDATE,
        ) {
            i += 1;
            continue;
        }
        if !TriggerEnabled(
            estate,
            relinfo,
            trigger,
            LocTriggerData.tg_event as c_int,
            updatedCols,
            oldslot,
            newslot,
        ) {
            i += 1;
            continue;
        }

        if newtuple.is_null() {
            newtuple = ExecFetchSlotHeapTuple(newslot, true, &mut should_free_new);
        }

        LocTriggerData.tg_trigslot = oldslot;
        LocTriggerData.tg_trigtuple = trigtuple;
        oldtuple = newtuple;
        LocTriggerData.tg_newtuple = oldtuple;
        LocTriggerData.tg_newslot = newslot;
        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(
            &mut LocTriggerData,
            i,
            (*relinfo).ri_TrigFunctions,
            (*relinfo).ri_TrigInstrument,
            GetPerTupleMemoryContext(estate),
        );

        if newtuple.is_null() {
            if should_free_trig {
                heap_freetuple(trigtuple);
            }
            if should_free_new {
                heap_freetuple(oldtuple);
            }
            return false; /* "do nothing" */
        } else if newtuple != oldtuple {
            newtuple =
                check_modified_virtual_generated(RelationGetDescr((*relinfo).ri_RelationDesc), newtuple);

            ExecForceStoreHeapTuple(newtuple, newslot, false);

            /*
             * If the tuple returned by the trigger / being stored, is the old
             * row version, and the heap tuple passed to the trigger was
             * allocated locally, materialize the slot. Otherwise we might
             * free it while still referenced by the slot.
             */
            if should_free_trig && newtuple == trigtuple {
                ExecMaterializeSlot(newslot);
            }

            if should_free_new {
                heap_freetuple(oldtuple);
            }

            /* signal tuple should be re-fetched if used */
            newtuple = core::ptr::null_mut();
        }
        i += 1;
    }
    if should_free_trig {
        heap_freetuple(trigtuple);
    }

    true
}

/*
 * Note: 'src_partinfo' and 'dst_partinfo', when non-NULL, refer to the source
 * and destination partitions, respectively, of a cross-partition update of
 * the root partitioned table mentioned in the query, given by 'relinfo'.
 * 'tupleid' in that case refers to the ctid of the "old" tuple in the source
 * partition, and 'newslot' contains the "new" tuple in the destination
 * partition.  This interface allows to support the requirements of
 * ExecCrossPartitionUpdateForeignKey(); is_crosspart_update must be true in
 * that case.
 */
pub unsafe fn ExecARUpdateTriggers(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    src_partinfo: *mut ResultRelInfo,
    dst_partinfo: *mut ResultRelInfo,
    tupleid: *mut ItemPointerData,
    fdw_trigtuple: HeapTuple,
    newslot: *mut TupleTableSlot,
    recheckIndexes: *mut List,
    transition_capture: *mut TransitionCaptureState,
    is_crosspart_update: bool,
) {
    use crate::executor::execUtils::{ExecGetAllUpdatedCols, ExecGetTriggerOldSlot};
    use crate::executor::tuptable::ExecClearTuple;
    use crate::storage::itemptr::ItemPointerIsValid;

    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    if !(*relinfo).ri_FdwRoutine.is_null()
        && !transition_capture.is_null()
        && ((*transition_capture).tcs_update_old_table || (*transition_capture).tcs_update_new_table)
    {
        debug_assert!(!(*relinfo).ri_RootResultRelInfo.is_null());
        ereport!(
            ERROR,
            errmsg!("cannot collect transition tuples from child foreign tables")
        );
        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
    }

    if (!trigdesc.is_null() && (*trigdesc).trig_update_after_row)
        || (!transition_capture.is_null()
            && ((*transition_capture).tcs_update_old_table
                || (*transition_capture).tcs_update_new_table))
    {
        /*
         * Note: if the UPDATE is converted into a DELETE+INSERT as part of
         * update-partition-key operation, then this function is also called
         * separately for DELETE and INSERT to capture transition table rows.
         * In such case, either old tuple or new tuple can be NULL.
         */
        let oldslot: *mut TupleTableSlot;
        let tupsrc: *mut ResultRelInfo;

        debug_assert!(
            (!src_partinfo.is_null() && !dst_partinfo.is_null()) || !is_crosspart_update
        );

        tupsrc = if !src_partinfo.is_null() {
            src_partinfo
        } else {
            relinfo
        };
        oldslot = ExecGetTriggerOldSlot(estate, tupsrc);

        if fdw_trigtuple.is_null() && ItemPointerIsValid(tupleid) {
            GetTupleForTrigger(
                estate,
                core::ptr::null_mut(),
                tupsrc,
                tupleid,
                LockTupleExclusive,
                oldslot,
                false,
                core::ptr::null_mut(),
                core::ptr::null_mut(),
                core::ptr::null_mut(),
            );
        } else if !fdw_trigtuple.is_null() {
            ExecForceStoreHeapTuple(fdw_trigtuple, oldslot, false);
        } else {
            ExecClearTuple(oldslot);
        }

        AfterTriggerSaveEvent(
            estate,
            relinfo,
            src_partinfo,
            dst_partinfo,
            TRIGGER_EVENT_UPDATE as c_int,
            true,
            oldslot,
            newslot,
            recheckIndexes,
            ExecGetAllUpdatedCols(relinfo, estate),
            transition_capture,
            is_crosspart_update,
        );
    }
}

pub unsafe fn ExecBSTruncateTriggers(estate: *mut EState, relinfo: *mut ResultRelInfo) {
    let trigdesc: *mut TriggerDesc;
    let mut LocTriggerData: TriggerData = std::mem::zeroed();

    trigdesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    if trigdesc.is_null() {
        return;
    }
    if !(*trigdesc).trig_truncate_before_statement {
        return;
    }

    LocTriggerData.r#type = NodeTag::T_TriggerData;
    LocTriggerData.tg_event = TRIGGER_EVENT_TRUNCATE | TRIGGER_EVENT_BEFORE;
    LocTriggerData.tg_relation = (*relinfo).ri_RelationDesc;

    let mut i = 0;
    while i < (*trigdesc).numtriggers {
        let trigger: *mut Trigger = &mut *(*trigdesc).triggers.offset(i as isize);
        let newtuple: HeapTuple;

        if !TRIGGER_TYPE_MATCHES(
            (*trigger).tgtype,
            TRIGGER_TYPE_STATEMENT,
            TRIGGER_TYPE_BEFORE,
            TRIGGER_TYPE_TRUNCATE,
        ) {
            i += 1;
            continue;
        }
        if !TriggerEnabled(
            estate,
            relinfo,
            trigger,
            LocTriggerData.tg_event as c_int,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        ) {
            i += 1;
            continue;
        }

        LocTriggerData.tg_trigger = trigger;
        newtuple = ExecCallTriggerFunc(
            &mut LocTriggerData,
            i,
            (*relinfo).ri_TrigFunctions,
            (*relinfo).ri_TrigInstrument,
            GetPerTupleMemoryContext(estate),
        );

        if !newtuple.is_null() {
            ereport!(ERROR, errmsg!("BEFORE STATEMENT trigger cannot return a value"));
            /* C also: errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED) */
        }
        i += 1;
    }
}

pub unsafe fn ExecASTruncateTriggers(estate: *mut EState, relinfo: *mut ResultRelInfo) {
    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;

    if !trigdesc.is_null() && (*trigdesc).trig_truncate_after_statement {
        AfterTriggerSaveEvent(
            estate,
            relinfo,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            TRIGGER_EVENT_TRUNCATE as c_int,
            false,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            crate::nodes::pg_list::NIL,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
            false,
        );
    }
}

/* commands/trigger.h: TRIGGER_FIRES_ON_ORIGIN */
const TRIGGER_FIRES_ON_ORIGIN: core::ffi::c_char = b'O' as core::ffi::c_char;

/*
 * CreateTrigger - create a trigger.
 */
pub unsafe fn CreateTrigger(
    stmt: *mut crate::nodes::parsenodes::CreateTrigStmt,
    queryString: *const core::ffi::c_char,
    relOid: Oid,
    refRelOid: Oid,
    constraintOid: Oid,
    indexOid: Oid,
    funcoid: Oid,
    parentTriggerOid: Oid,
    whenClause: *mut crate::nodes::nodes::Node,
    isInternal: bool,
    in_partition: bool,
) -> ObjectAddress {
    CreateTriggerFiringOn(
        stmt,
        queryString,
        relOid,
        refRelOid,
        constraintOid,
        indexOid,
        funcoid,
        parentTriggerOid,
        whenClause,
        isInternal,
        in_partition,
        TRIGGER_FIRES_ON_ORIGIN,
    )
}


/*
 * CreateTriggerFiringOn - create a trigger with a specified firing-time.
 *
 * 1:1 translation of trigger.c.  Per the port's ereport! convention, only
 * errmsg! is kept on each ereport (errcode()/errdetail()/errhint()/
 * parser_errposition() are folded into "C also:" comments).
 */
pub unsafe fn CreateTriggerFiringOn(
    stmt: *mut crate::nodes::parsenodes::CreateTrigStmt,
    queryString: *const core::ffi::c_char,
    relOid: Oid,
    refRelOid: Oid,
    mut constraintOid: Oid,
    indexOid: Oid,
    mut funcoid: Oid,
    parentTriggerOid: Oid,
    mut whenClause: *mut crate::nodes::nodes::Node,
    isInternal: bool,
    in_partition: bool,
    trigger_fires_when: core::ffi::c_char,
) -> ObjectAddress {
    use crate::catalog::pg_trigger::*;
    use crate::{DirectFunctionCall1, strVal, lfirst_node};
    #[inline] unsafe fn RelationGetNamespace(rel: Relation) -> Oid { (*(*rel).rd_rel).relnamespace }
    use crate::nodes::pg_list::{list_length, list_free, NIL, lfirst};
    use crate::nodes::primnodes::{PRS2_OLD_VARNO, PRS2_NEW_VARNO};
    use crate::c::int2vector;
    use crate::utils::palloc::palloc;
    use crate::utils::cache::inval::CacheInvalidateRelcacheByTuple;
    use crate::catalog::pg_depend::{recordDependencyOn, deleteDependencyRecordsFor};
    use crate::catalog::dependency::recordDependencyOnExpr;
    use crate::access::attnum::InvalidAttrNumber;
    use crate::access::table::table::table_openrv;
    use crate::access::transam::xact::CommandCounterIncrement;
    use crate::catalog::aclchk::aclcheck_error;
    use crate::catalog::aclchk::object_aclcheck;
    use crate::catalog::aclchk::pg_class_aclcheck;
    use crate::catalog::catalog::GetNewOidWithIndex;
    use crate::catalog::catalog::IsSystemRelation;
    use crate::catalog::indexing::CatalogTupleInsert;
    use crate::catalog::namespace::RangeVarGetRelid;
    use crate::catalog::objectaddress_impl::CStringGetTextDatum;
    use crate::catalog::objectaddress_impl::NameListToString;
    use crate::catalog::objectaddress_impl::SearchSysCacheCopy1;
    use crate::catalog::objectaddress_impl::get_rel_relkind;
    use crate::catalog::objectaddress_impl::get_relkind_objtype;
    use crate::catalog::partition::map_partition_varattnos;
    use crate::catalog::pg_class::RELKIND_VIEW;
    use crate::catalog::pg_constraint::CreateConstraintEntry;
    use crate::catalog::pg_inherits::has_superclass;
    use crate::miscadmin::GetUserId;
    use crate::nodes::makefuncs::makeAlias;
    use crate::optimizer::optimizer::pull_var_clause;
    use crate::optimizer::path::allpaths::RELKIND_PARTITIONED_TABLE;
    use crate::optimizer::path::allpaths::copyObject;
    use crate::optimizer::util::plancat::RELKIND_FOREIGN_TABLE;
    use crate::optimizer::util::plancat::RELKIND_RELATION;
    use crate::parser::parse_clause::transformWhereClause;
    use crate::parser::parse_collate::assign_expr_collations;
    use crate::parser::parse_func::LookupFuncName;
    use crate::parser::parse_node::free_parsestate;
    use crate::parser::parse_node::make_parsestate;
    use crate::parser::parse_relation::addNSItemToQuery;
    use crate::parser::parse_relation::addRangeTableEntryForRelation;
    use crate::parser::parse_relation::attnameAttNum;
    use crate::postgres::BoolGetDatum;
    use crate::postgres::DatumGetPointer;
    use crate::postgres::Int16GetDatum;
    use crate::storage::lmgr::lmgr::LockRelationOid;
    use crate::storage::lockdefs::ShareRowExclusiveLock;
    use crate::utils::adt::name::namein;
    use crate::utils::adt::varlena::byteain;
    use crate::utils::adt::xml::RELOID;
    use crate::utils::builtins::buildint2vector;
    use crate::utils::cache::lsyscache::get_func_rettype;
    use crate::utils::memutils::MemoryContextReset;
    use crate::nodes::parsenodes::TriggerTransition;
    use crate::nodes::primnodes::Var;

    use crate::catalog::dependency::{DEPENDENCY_NORMAL, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL,
        DEPENDENCY_PARTITION_PRI, DEPENDENCY_PARTITION_SEC};
    use crate::catalog::catalog_oids::{ProcedureRelationId, ConstraintRelationId, RelationRelationId};
    use crate::access::common::heaptuple::{heap_form_tuple, heap_freetuple};

    const ACL_TRIGGER: u64 = 1 << 5;
    const ACL_EXECUTE: u64 = 1 << 8;
    const TRIGGEROID: Oid = 2279;
    const NAMEDATALEN: usize = 64;

    let mut tgtype: i16;
    let ncolumns: i32;
    let columns: *mut i16;
    let tgattr: *mut int2vector;
    let mut whenRtable: *mut List = NIL as *mut List;
    let qual: *mut core::ffi::c_char;
    let mut values: [Datum; 19] = [0; 19];
    let mut nulls: [bool; 19] = [false; 19];
    let rel: Relation;
    use crate::utils::adt::acl::{AclResult, ACLCHECK_OK};
    use crate::parser::parse_node::EXPR_KIND_TRIGGER_WHEN;
    let mut aclresult: AclResult;
    let tgrel: Relation;
    let pgrel: Relation;
    let mut tuple: HeapTuple = core::ptr::null_mut();
    let funcrettype: Oid;
    let mut trigoid: Oid = InvalidOid;
    let mut internaltrigname: [core::ffi::c_char; 64] = [0; 64];
    let trigname: *mut core::ffi::c_char;
    let mut constrrelid: Oid = InvalidOid;
    let mut myself: ObjectAddress = core::mem::zeroed();
    let mut referenced: ObjectAddress = core::mem::zeroed();
    let mut oldtablename: *mut core::ffi::c_char = core::ptr::null_mut();
    let mut newtablename: *mut core::ffi::c_char = core::ptr::null_mut();
    let partition_recurse: bool;
    let mut trigger_exists: bool = false;
    let mut existing_constraint_oid: Oid = InvalidOid;
    let mut existing_isInternal: bool = false;
    let mut existing_isClone: bool = false;

    if OidIsValid(relOid) {
        rel = table_open(relOid, ShareRowExclusiveLock);
    } else {
        rel = table_openrv((*stmt).relation, ShareRowExclusiveLock);
    }

    let relkind = (*(*rel).rd_rel).relkind;
    if relkind == RELKIND_RELATION {
        if (*stmt).timing != TRIGGER_TYPE_BEFORE as i16 && (*stmt).timing != TRIGGER_TYPE_AFTER as i16 {
            ereport!(ERROR, errmsg!("\"{}\" is a table", rname(rel)));
            /* C also: ERRCODE_WRONG_OBJECT_TYPE; errdetail Tables cannot have INSTEAD OF triggers. */
        }
    } else if relkind == RELKIND_PARTITIONED_TABLE {
        if (*stmt).timing != TRIGGER_TYPE_BEFORE as i16 && (*stmt).timing != TRIGGER_TYPE_AFTER as i16 {
            ereport!(ERROR, errmsg!("\"{}\" is a table", rname(rel)));
        }
        if (*stmt).row && (*stmt).transitionRels != NIL as *mut List {
            ereport!(ERROR, errmsg!("\"{}\" is a partitioned table", rname(rel)));
            /* C also: ROW triggers with transition tables are not supported on partitioned tables. */
        }
    } else if relkind == RELKIND_VIEW {
        if (*stmt).timing != TRIGGER_TYPE_INSTEAD as i16 && (*stmt).row {
            ereport!(ERROR, errmsg!("\"{}\" is a view", rname(rel)));
            /* C also: Views cannot have row-level BEFORE or AFTER triggers. */
        }
        if TRIGGER_FOR_TRUNCATE((*stmt).events as i16) {
            ereport!(ERROR, errmsg!("\"{}\" is a view", rname(rel)));
            /* C also: Views cannot have TRUNCATE triggers. */
        }
    } else if relkind == RELKIND_FOREIGN_TABLE {
        if (*stmt).timing != TRIGGER_TYPE_BEFORE as i16 && (*stmt).timing != TRIGGER_TYPE_AFTER as i16 {
            ereport!(ERROR, errmsg!("\"{}\" is a foreign table", rname(rel)));
        }
        if (*stmt).isconstraint {
            ereport!(ERROR, errmsg!("\"{}\" is a foreign table", rname(rel)));
            /* C also: Foreign tables cannot have constraint triggers. */
        }
    } else {
        ereport!(ERROR, errmsg!("relation \"{}\" cannot have triggers", rname(rel)));
    }

    if !allowSystemTableMods && IsSystemRelation(rel) {
        ereport!(ERROR, errmsg!("permission denied: \"{}\" is a system catalog", rname(rel)));
    }

    if (*stmt).isconstraint {
        if OidIsValid(refRelOid) {
            LockRelationOid(refRelOid, AccessShareLock);
            constrrelid = refRelOid;
        } else if !(*stmt).constrrel.is_null() {
            constrrelid = RangeVarGetRelid((*stmt).constrrel, AccessShareLock, false);
        }
    }

    /* permission checks */
    if !isInternal {
        aclresult = pg_class_aclcheck(RelationGetRelid(rel), GetUserId(), ACL_TRIGGER);
        if aclresult != ACLCHECK_OK {
            aclcheck_error(aclresult, get_relkind_objtype((*(*rel).rd_rel).relkind), RelationGetRelationName(rel));
        }
        if OidIsValid(constrrelid) {
            aclresult = pg_class_aclcheck(constrrelid, GetUserId(), ACL_TRIGGER);
            if aclresult != ACLCHECK_OK {
                aclcheck_error(aclresult, get_relkind_objtype(get_rel_relkind(constrrelid)), get_rel_name(constrrelid));
            }
        }
    }

    partition_recurse = !isInternal && (*stmt).row && relkind == RELKIND_PARTITIONED_TABLE;
    if partition_recurse {
        list_free(find_all_inheritors(RelationGetRelid(rel), ShareRowExclusiveLock, core::ptr::null_mut()));
    }

    /* Compute tgtype */
    tgtype = 0;
    if (*stmt).row {
        tgtype |= TRIGGER_TYPE_ROW;
    }
    tgtype |= (*stmt).timing;
    tgtype |= (*stmt).events as i16;

    if TRIGGER_FOR_ROW(tgtype) && TRIGGER_FOR_TRUNCATE(tgtype) {
        ereport!(ERROR, errmsg!("TRUNCATE FOR EACH ROW triggers are not supported"));
    }
    if TRIGGER_FOR_INSTEAD(tgtype) {
        if !TRIGGER_FOR_ROW(tgtype) {
            ereport!(ERROR, errmsg!("INSTEAD OF triggers must be FOR EACH ROW"));
        }
        if !(*stmt).whenClause.is_null() {
            ereport!(ERROR, errmsg!("INSTEAD OF triggers cannot have WHEN conditions"));
        }
        if (*stmt).columns != NIL as *mut List {
            ereport!(ERROR, errmsg!("INSTEAD OF triggers cannot have column lists"));
        }
    }

    /* transition-table (REFERENCING) validation */
    if (*stmt).transitionRels != NIL as *mut List {
        foreach!(lc, (*stmt).transitionRels, {
            let tt = lfirst_node!(TriggerTransition, T_TriggerTransition, crate::current_cell!(lc));
            if !(*tt).isTable {
                ereport!(ERROR, errmsg!("ROW variable naming in the REFERENCING clause is not supported"));
                /* C also: errhint Use OLD TABLE or NEW TABLE for naming transition tables. */
            }
            if relkind == RELKIND_FOREIGN_TABLE {
                ereport!(ERROR, errmsg!("\"{}\" is a foreign table", rname(rel)));
            }
            if relkind == RELKIND_VIEW {
                ereport!(ERROR, errmsg!("\"{}\" is a view", rname(rel)));
            }
            if TRIGGER_FOR_ROW(tgtype) && has_superclass((*rel).rd_id) {
                ereport!(ERROR, errmsg!("ROW triggers with transition tables are not supported on partitions or inheritance children"));
            }
            if (*stmt).timing != TRIGGER_TYPE_AFTER as i16 {
                ereport!(ERROR, errmsg!("transition table name can only be specified for an AFTER trigger"));
            }
            if TRIGGER_FOR_TRUNCATE(tgtype) {
                ereport!(ERROR, errmsg!("TRUNCATE triggers with transition tables are not supported"));
            }
            let nev = (if TRIGGER_FOR_INSERT(tgtype) {1} else {0})
                + (if TRIGGER_FOR_UPDATE(tgtype) {1} else {0})
                + (if TRIGGER_FOR_DELETE(tgtype) {1} else {0});
            if nev != 1 {
                ereport!(ERROR, errmsg!("transition tables cannot be specified for triggers with more than one event"));
            }
            if (*stmt).columns != NIL as *mut List {
                ereport!(ERROR, errmsg!("transition tables cannot be specified for triggers with column lists"));
            }
            if (*tt).isNew {
                if !(TRIGGER_FOR_INSERT(tgtype) || TRIGGER_FOR_UPDATE(tgtype)) {
                    ereport!(ERROR, errmsg!("NEW TABLE can only be specified for an INSERT or UPDATE trigger"));
                }
                if !newtablename.is_null() {
                    ereport!(ERROR, errmsg!("NEW TABLE cannot be specified multiple times"));
                }
                newtablename = (*tt).name;
            } else {
                if !(TRIGGER_FOR_DELETE(tgtype) || TRIGGER_FOR_UPDATE(tgtype)) {
                    ereport!(ERROR, errmsg!("OLD TABLE can only be specified for a DELETE or UPDATE trigger"));
                }
                if !oldtablename.is_null() {
                    ereport!(ERROR, errmsg!("OLD TABLE cannot be specified multiple times"));
                }
                oldtablename = (*tt).name;
            }
        });
        if !newtablename.is_null() && !oldtablename.is_null()
            && libc_strcmp(newtablename, oldtablename) == 0 {
            ereport!(ERROR, errmsg!("OLD TABLE name and NEW TABLE name cannot be the same"));
        }
    }

    /* Parse WHEN clause if present and not already transformed */
    if whenClause.is_null() && !(*stmt).whenClause.is_null() {
        let pstate = make_parsestate(core::ptr::null_mut());
        (*pstate).p_sourcetext = queryString;
        let mut nsitem = addRangeTableEntryForRelation(pstate, rel as *mut core::ffi::c_void, AccessShareLock,
            makeAlias(c"old".as_ptr(), NIL as *mut List), false, false);
        addNSItemToQuery(pstate, nsitem, false, true, true);
        nsitem = addRangeTableEntryForRelation(pstate, rel as *mut core::ffi::c_void, AccessShareLock,
            makeAlias(c"new".as_ptr(), NIL as *mut List), false, false);
        addNSItemToQuery(pstate, nsitem, false, true, true);
        whenClause = transformWhereClause(pstate, copyObject((*stmt).whenClause as *mut core::ffi::c_void) as *mut crate::nodes::nodes::Node,
            EXPR_KIND_TRIGGER_WHEN, c"WHEN".as_ptr());
        assign_expr_collations(pstate, whenClause);
        let varList = pull_var_clause(whenClause, 0);
        foreach!(lc, varList, {
            let var = lfirst(crate::current_cell!(lc)) as *mut crate::nodes::primnodes::Var;
            if (*var).varno == PRS2_OLD_VARNO as i32 {
                if !TRIGGER_FOR_ROW(tgtype) {
                    ereport!(ERROR, errmsg!("statement trigger's WHEN condition cannot reference column values"));
                }
                if TRIGGER_FOR_INSERT(tgtype) {
                    ereport!(ERROR, errmsg!("INSERT trigger's WHEN condition cannot reference OLD values"));
                }
            } else if (*var).varno == PRS2_NEW_VARNO as i32 {
                if !TRIGGER_FOR_ROW(tgtype) {
                    ereport!(ERROR, errmsg!("statement trigger's WHEN condition cannot reference column values"));
                }
                if TRIGGER_FOR_DELETE(tgtype) {
                    ereport!(ERROR, errmsg!("DELETE trigger's WHEN condition cannot reference NEW values"));
                }
            } else {
                elog!(ERROR, "trigger WHEN condition cannot contain references to other relations");
            }
        });
        whenRtable = (*pstate).p_rtable;
        qual = nodeToString(whenClause as *const core::ffi::c_void);
        free_parsestate(pstate);
    } else if whenClause.is_null() {
        whenRtable = NIL as *mut List;
        qual = core::ptr::null_mut();
    } else {
        qual = nodeToString(whenClause as *const core::ffi::c_void);
        whenRtable = NIL as *mut List;
    }

    /* Find and validate the trigger function. */
    if !OidIsValid(funcoid) {
        funcoid = LookupFuncName((*stmt).funcname, 0, core::ptr::null(), false);
    }
    if !isInternal {
        aclresult = object_aclcheck(ProcedureRelationId, funcoid, GetUserId(), ACL_EXECUTE);
        if aclresult != ACLCHECK_OK {
            aclcheck_error_func(aclresult, funcoid);
        }
    }
    funcrettype = get_func_rettype(funcoid);
    if funcrettype != TRIGGEROID {
        ereport!(ERROR, errmsg!("function {} must return type trigger",
            std::ffi::CStr::from_ptr(NameListToString((*stmt).funcname)).to_string_lossy()));
    }

    /* Scan pg_trigger for an existing trigger of the same name. */
    tgrel = table_open(TriggerRelationId, RowExclusiveLock);
    if !isInternal {
        let mut skeys: [ScanKeyData; 2] = core::mem::zeroed();
        ScanKeyInit(&mut skeys[0], Anum_pg_trigger_tgrelid as i32, BTEqualStrategyNumber as u16, F_OIDEQ,
            ObjectIdGetDatum(RelationGetRelid(rel)));
        ScanKeyInit(&mut skeys[1], Anum_pg_trigger_tgname as i32, BTEqualStrategyNumber as u16, F_NAMEEQ,
            CStringGetDatum((*stmt).trigname));
        let tgscan = systable_beginscan(tgrel, 2701, true, core::ptr::null_mut(), 2, skeys.as_mut_ptr());
        let et = systable_getnext(tgscan);
        if HeapTupleIsValid(et) {
            let oldtrigger: Form_pg_trigger = GETSTRUCT(et) as Form_pg_trigger;
            trigoid = (*oldtrigger).oid;
            existing_constraint_oid = (*oldtrigger).tgconstraint;
            existing_isInternal = (*oldtrigger).tgisinternal;
            existing_isClone = OidIsValid((*oldtrigger).tgparentid);
            trigger_exists = true;
            tuple = heap_copytuple(et);
        }
        systable_endscan(tgscan);
    }

    if !trigger_exists {
        trigoid = GetNewOidWithIndex(tgrel, TriggerOidIndexId_const(), Anum_pg_trigger_oid as i16);
    } else {
        if !(*stmt).replace {
            ereport!(ERROR, errmsg!("trigger \"{}\" for relation \"{}\" already exists",
                cstr((*stmt).trigname), rname(rel)));
        }
        if (existing_isInternal || existing_isClone) && !isInternal && !in_partition {
            ereport!(ERROR, errmsg!("trigger \"{}\" for relation \"{}\" is an internal or a child trigger",
                cstr((*stmt).trigname), rname(rel)));
        }
        if OidIsValid(existing_constraint_oid) {
            ereport!(ERROR, errmsg!("trigger \"{}\" for relation \"{}\" is a constraint trigger",
                cstr((*stmt).trigname), rname(rel)));
        }
    }

    /* CREATE CONSTRAINT TRIGGER: make a pg_constraint entry */
    if (*stmt).isconstraint && !OidIsValid(constraintOid) {
        constraintOid = CreateConstraintEntryForTrigger(stmt, rel, constrrelid, isInternal);
    }

    /* internal triggers get a unique name */
    if isInternal {
        let s = format!("{}_{}\0", cstr((*stmt).trigname), trigoid);
        let b = s.as_bytes();
        let n = core::cmp::min(b.len(), NAMEDATALEN - 1);
        for i in 0..n { internaltrigname[i] = b[i] as core::ffi::c_char; }
        trigname = internaltrigname.as_mut_ptr();
    } else {
        trigname = (*stmt).trigname;
    }

    /* Build the pg_trigger tuple. */
    values[Anum_pg_trigger_oid as usize - 1] = ObjectIdGetDatum(trigoid);
    values[Anum_pg_trigger_tgrelid as usize - 1] = ObjectIdGetDatum(RelationGetRelid(rel));
    values[Anum_pg_trigger_tgparentid as usize - 1] = ObjectIdGetDatum(parentTriggerOid);
    values[Anum_pg_trigger_tgname as usize - 1] = DirectFunctionCall1!(namein, CStringGetDatum(trigname));
    values[Anum_pg_trigger_tgfoid as usize - 1] = ObjectIdGetDatum(funcoid);
    values[Anum_pg_trigger_tgtype as usize - 1] = Int16GetDatum(tgtype);
    values[Anum_pg_trigger_tgenabled as usize - 1] = trigger_fires_when as Datum;
    values[Anum_pg_trigger_tgisinternal as usize - 1] = BoolGetDatum(isInternal);
    values[Anum_pg_trigger_tgconstrrelid as usize - 1] = ObjectIdGetDatum(constrrelid);
    values[Anum_pg_trigger_tgconstrindid as usize - 1] = ObjectIdGetDatum(indexOid);
    values[Anum_pg_trigger_tgconstraint as usize - 1] = ObjectIdGetDatum(constraintOid);
    values[Anum_pg_trigger_tgdeferrable as usize - 1] = BoolGetDatum((*stmt).deferrable);
    values[Anum_pg_trigger_tginitdeferred as usize - 1] = BoolGetDatum((*stmt).initdeferred);

    /* tgargs */
    let nargs = list_length((*stmt).args) as i16;
    let argsbuf = build_trigger_args((*stmt).args);
    values[Anum_pg_trigger_tgnargs as usize - 1] = Int16GetDatum(nargs);
    values[Anum_pg_trigger_tgargs as usize - 1] = DirectFunctionCall1!(byteain, CStringGetDatum(argsbuf));

    /* column number array for column-specific trigger */
    ncolumns = list_length((*stmt).columns);
    if ncolumns == 0 {
        columns = core::ptr::null_mut();
    } else {
        columns = palloc((ncolumns as usize) * core::mem::size_of::<i16>()) as *mut i16;
        let mut i: i32 = 0;
        foreach!(cell, (*stmt).columns, {
            let name = strVal!(lfirst(crate::current_cell!(cell)) as *mut core::ffi::c_void);
            let attnum = attnameAttNum(rel as *mut core::ffi::c_void, name as *const core::ffi::c_char, false);
            if attnum == InvalidAttrNumber as i32 {
                ereport!(ERROR, errmsg!("column \"{}\" of relation \"{}\" does not exist",
                    cstr(name), rname(rel)));
            }
            let mut j = i - 1;
            while j >= 0 {
                if *columns.add(j as usize) == attnum as i16 {
                    ereport!(ERROR, errmsg!("column \"{}\" specified more than once", cstr(name)));
                }
                j -= 1;
            }
            *columns.add(i as usize) = attnum as i16;
            i += 1;
        });
    }
    tgattr = buildint2vector(columns, ncolumns);
    values[Anum_pg_trigger_tgattr as usize - 1] = PointerGetDatum(tgattr as *mut core::ffi::c_void);

    if !qual.is_null() {
        values[Anum_pg_trigger_tgqual as usize - 1] = CStringGetTextDatum(qual);
    } else {
        nulls[Anum_pg_trigger_tgqual as usize - 1] = true;
    }
    if !oldtablename.is_null() {
        values[Anum_pg_trigger_tgoldtable as usize - 1] = DirectFunctionCall1!(namein, CStringGetDatum(oldtablename));
    } else {
        nulls[Anum_pg_trigger_tgoldtable as usize - 1] = true;
    }
    if !newtablename.is_null() {
        values[Anum_pg_trigger_tgnewtable as usize - 1] = DirectFunctionCall1!(namein, CStringGetDatum(newtablename));
    } else {
        nulls[Anum_pg_trigger_tgnewtable as usize - 1] = true;
    }

    /* Insert or replace tuple in pg_trigger. */
    if !trigger_exists {
        tuple = heap_form_tuple((*tgrel).rd_att, values.as_mut_ptr(), nulls.as_mut_ptr());
        CatalogTupleInsert(tgrel, tuple);
    } else {
        let newtup = heap_form_tuple((*tgrel).rd_att, values.as_mut_ptr(), nulls.as_mut_ptr());
        CatalogTupleUpdate(tgrel, &mut (*tuple).t_self, newtup);
        heap_freetuple(newtup);
    }
    heap_freetuple(tuple);
    table_close(tgrel, RowExclusiveLock);

    pfree(DatumGetPointer(values[Anum_pg_trigger_tgname as usize - 1]) as *mut core::ffi::c_void);
    pfree(DatumGetPointer(values[Anum_pg_trigger_tgargs as usize - 1]) as *mut core::ffi::c_void);
    pfree(DatumGetPointer(values[Anum_pg_trigger_tgattr as usize - 1]) as *mut core::ffi::c_void);
    if !oldtablename.is_null() { pfree(DatumGetPointer(values[Anum_pg_trigger_tgoldtable as usize - 1]) as *mut core::ffi::c_void); }
    if !newtablename.is_null() { pfree(DatumGetPointer(values[Anum_pg_trigger_tgnewtable as usize - 1]) as *mut core::ffi::c_void); }

    /* Update pg_class.relhastriggers if needed. */
    pgrel = table_open(RelationRelationId, RowExclusiveLock);
    tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(RelationGetRelid(rel)));
    if !HeapTupleIsValid(tuple) {
        elog!(ERROR, "cache lookup failed for relation {}", RelationGetRelid(rel));
    }
    let classform = GETSTRUCT(tuple) as *mut crate::catalog::pg_class::FormData_pg_class;
    if !(*classform).relhastriggers {
        (*classform).relhastriggers = true;
        CatalogTupleUpdate(pgrel, &mut (*tuple).t_self, tuple);
        CommandCounterIncrement();
    } else {
        CacheInvalidateRelcacheByTuple(tuple);
    }
    heap_freetuple(tuple);
    table_close(pgrel, RowExclusiveLock);

    /* Flush old dependencies if replacing. */
    if trigger_exists {
        deleteDependencyRecordsFor(TriggerRelationId, trigoid, true);
    }

    /* Record dependencies. */
    myself.classId = TriggerRelationId;
    myself.objectId = trigoid;
    myself.objectSubId = 0;
    referenced.classId = ProcedureRelationId;
    referenced.objectId = funcoid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    if isInternal && OidIsValid(constraintOid) {
        referenced.classId = ConstraintRelationId;
        referenced.objectId = constraintOid;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
    } else {
        referenced.classId = RelationRelationId;
        referenced.objectId = RelationGetRelid(rel);
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
        if OidIsValid(constrrelid) {
            referenced.classId = RelationRelationId;
            referenced.objectId = constrrelid;
            referenced.objectSubId = 0;
            recordDependencyOn(&myself, &referenced, DEPENDENCY_AUTO);
        }
        if OidIsValid(constraintOid) {
            referenced.classId = ConstraintRelationId;
            referenced.objectId = constraintOid;
            referenced.objectSubId = 0;
            recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL);
        }
        if OidIsValid(parentTriggerOid) {
            ObjectAddressSet!(referenced, TriggerRelationId, parentTriggerOid);
            recordDependencyOn(&myself, &referenced, DEPENDENCY_PARTITION_PRI);
            ObjectAddressSet!(referenced, RelationRelationId, RelationGetRelid(rel));
            recordDependencyOn(&myself, &referenced, DEPENDENCY_PARTITION_SEC);
        }
    }

    if !columns.is_null() {
        referenced.classId = RelationRelationId;
        referenced.objectId = RelationGetRelid(rel);
        for i in 0..ncolumns {
            referenced.objectSubId = *columns.add(i as usize) as i32;
            recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
        }
    }

    if whenRtable != NIL as *mut List {
        recordDependencyOnExpr(&myself, whenClause, whenRtable, DEPENDENCY_NORMAL);
    }

    InvokeObjectPostCreateHookArg(TriggerRelationId, trigoid, 0, isInternal);

    /* Create the trigger on child partitions if needed. */
    if partition_recurse {
        let partdesc = RelationGetPartitionDesc(rel, true);
        let perChildCxt = AllocSetContextCreate!(CurrentMemoryContext, c"part trig clone".as_ptr());
        let oldcxt = MemoryContextSwitchTo(perChildCxt);
        for i in 0..(*partdesc).nparts {
            let childTbl = table_open(*(*partdesc).oids.add(i as usize), ShareRowExclusiveLock);
            let childStmt = copyObject(stmt as *mut core::ffi::c_void) as *mut crate::nodes::parsenodes::CreateTrigStmt;
            (*childStmt).funcname = NIL as *mut List;
            (*childStmt).whenClause = core::ptr::null_mut();
            let mut q = copyObject(whenClause as *mut core::ffi::c_void) as *mut crate::nodes::nodes::Node;
            q = map_partition_varattnos(q as *mut List, PRS2_OLD_VARNO as i32, childTbl, rel) as *mut crate::nodes::nodes::Node;
            q = map_partition_varattnos(q as *mut List, PRS2_NEW_VARNO as i32, childTbl, rel) as *mut crate::nodes::nodes::Node;
            CreateTriggerFiringOn(childStmt, queryString, *(*partdesc).oids.add(i as usize), refRelOid,
                InvalidOid, InvalidOid, funcoid, trigoid, q, isInternal, true, trigger_fires_when);
            table_close(childTbl, NoLock);
            MemoryContextReset(perChildCxt);
        }
        MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(perChildCxt);
    }

    table_close(rel, NoLock);
    myself
}

/* helpers */
#[inline]
unsafe fn rname(rel: Relation) -> std::borrow::Cow<'static, str> {
    std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
}
#[inline]
unsafe fn cstr(s: *const core::ffi::c_char) -> std::borrow::Cow<'static, str> {
    std::ffi::CStr::from_ptr(s).to_string_lossy()
}
#[inline]
unsafe fn libc_strcmp(a: *const core::ffi::c_char, b: *const core::ffi::c_char) -> core::ffi::c_int { let mut i=0isize; loop { let ca=*a.offset(i); let cb=*b.offset(i); if ca!=cb { return (ca as i32)-(cb as i32); } if ca==0 { return 0; } i+=1; } }
#[inline]
unsafe fn TriggerOidIndexId_const() -> Oid { 2696 }
#[inline]
unsafe fn aclcheck_error_func(_acl: crate::utils::adt::acl::AclResult, _funcoid: Oid) { /* C: aclcheck_error(.., OBJECT_FUNCTION, NameListToString) */ }

/* trigger-type predicates (pg_trigger.h) */
#[inline]
fn TRIGGER_FOR_TRUNCATE(tgtype: i16) -> bool { use crate::catalog::pg_trigger::TRIGGER_TYPE_TRUNCATE; (tgtype & TRIGGER_TYPE_TRUNCATE) != 0 }
#[inline]
fn TRIGGER_FOR_INSTEAD(tgtype: i16) -> bool { use crate::catalog::pg_trigger::TRIGGER_TYPE_INSTEAD; (tgtype & TRIGGER_TYPE_INSTEAD) != 0 }

/* allowSystemTableMods GUC (utils/misc/guc_tables.c) - default false. */
static mut allowSystemTableMods: bool = false;

/* InvokeObjectPostCreateHookArg (catalog/objectaccess.h) - no-op in this port. */
#[inline]
unsafe fn InvokeObjectPostCreateHookArg(_classId: Oid, _objectId: Oid, _subId: i32, _isInternal: bool) {}

/* byte-serialize trigger args into "a1\000a2\000..." (trigger.c inline block). */
unsafe fn build_trigger_args(args: *mut List) -> *mut core::ffi::c_char {
    use crate::nodes::pg_list::{list_length, lfirst};
    use crate::strVal;
    use crate::utils::palloc::palloc;
    if list_length(args) == 0 {
        let p = palloc(1) as *mut core::ffi::c_char;
        *p = 0;
        return p;
    }
    let mut len: usize = 0;
    foreach!(le, args, {
        let ar0 = strVal!(lfirst(crate::current_cell!(le)));
        let mut ar = ar0;
        len += libc_strlen(ar) + 4;
        while *ar != 0 {
            if *ar == b'\\' as core::ffi::c_char { len += 1; }
            ar = ar.add(1);
        }
    });
    let buf = palloc(len + 1) as *mut core::ffi::c_char;
    *buf = 0;
    foreach!(le, args, {
        let mut sptr = strVal!(lfirst(crate::current_cell!(le)));
        let mut d = buf.add(libc_strlen(buf));
        while *sptr != 0 {
            if *sptr == b'\\' as core::ffi::c_char { *d = b'\\' as core::ffi::c_char; d = d.add(1); }
            *d = *sptr; d = d.add(1); sptr = sptr.add(1);
        }
        *d = b'\\' as core::ffi::c_char; *d.add(1) = b'0' as core::ffi::c_char;
        *d.add(2) = b'0' as core::ffi::c_char; *d.add(3) = b'0' as core::ffi::c_char; *d.add(4) = 0;
    });
    buf
}
#[inline]
unsafe fn libc_strlen(s: *const core::ffi::c_char) -> usize { let mut n=0usize; while *s.add(n)!=0 {n+=1;} n }

/* CreateConstraintEntry wrapper for a CREATE CONSTRAINT TRIGGER (trigger.c). */
unsafe fn CreateConstraintEntryForTrigger(
    stmt: *mut crate::nodes::parsenodes::CreateTrigStmt,
    rel: Relation,
    _constrrelid: Oid,
    isInternal: bool,
) -> Oid {
    use crate::catalog::pg_constraint::{CreateConstraintEntry, CONSTRAINT_TRIGGER};
    let sp = b' ' as core::ffi::c_char;
    CreateConstraintEntry(
        (*stmt).trigname, (*(*rel).rd_rel).relnamespace, CONSTRAINT_TRIGGER,
        (*stmt).deferrable, (*stmt).initdeferred, true, true, InvalidOid,
        RelationGetRelid(rel), core::ptr::null(), 0, 0, InvalidOid, InvalidOid, InvalidOid,
        core::ptr::null(), core::ptr::null(), core::ptr::null(), core::ptr::null(), 0,
        sp, sp, core::ptr::null(), 0, sp, core::ptr::null(), core::ptr::null_mut(),
        core::ptr::null(), true, 0, true, false, isInternal,
    )
}

/* nodeToString (nodes/outfuncs.c) - outfuncs not yet ported; dependency stub. */
unsafe fn nodeToString(_obj: *const core::ffi::c_void) -> *mut core::ffi::c_char {
    /* TODO(pg-port): nodes/outfuncs.c nodeToString */
    core::ptr::null_mut()
}

// ============================================================================
// backend/commands/trigger.c: after-trigger transaction/subxact lifecycle,
// SET CONSTRAINTS, and event-queue insertion (AfterTriggerSaveEvent).
// ============================================================================

/* ----------
 * AfterTriggerBeginXact()
 *
 *	Called at transaction start (either BEGIN or implicit for single
 *	statement).
 * ----------
 */
pub unsafe fn AfterTriggerBeginXact() {
    /*
     * Initialize after-trigger state structure to empty
     */
    afterTriggers.firing_counter = 1 as CommandId; /* mustn't be 0 */
    afterTriggers.query_depth = -1;

    /*
     * Verify that there is no leftover state remaining.  If these assertions
     * trip, it means that AfterTriggerEndXact wasn't called or didn't clean
     * up properly.
     */
    /* Assert(afterTriggers.state == NULL); */
    /* Assert(afterTriggers.query_stack == NULL); */
    /* Assert(afterTriggers.maxquerydepth == 0); */
    /* Assert(afterTriggers.event_cxt == NULL); */
    /* Assert(afterTriggers.events.head == NULL); */
    /* Assert(afterTriggers.trans_stack == NULL); */
    /* Assert(afterTriggers.maxtransdepth == 0); */
}

/* ----------
 * AfterTriggerFireDeferred()
 *
 *	Called just before the current transaction is committed. At this
 *	point we invoke all pending DEFERRED triggers.
 *
 *	It is possible for other modules to queue additional deferred triggers
 *	during pre-commit processing; therefore xact.c may have to call this
 *	multiple times.
 * ----------
 */
pub unsafe fn AfterTriggerFireDeferred() {
    let events: *mut AfterTriggerEventList;
    let mut snap_pushed = false;

    /* Must not be inside a query */
    /* Assert(afterTriggers.query_depth == -1); */

    /*
     * If there are any triggers to fire, make sure we have set a snapshot for
     * them to use.  (Since PortalRunUtility doesn't set a snap for COMMIT, we
     * can't assume ActiveSnapshot is valid on entry.)
     */
    events = &raw mut afterTriggers.events;
    if !(*events).head.is_null() {
        PushActiveSnapshot(GetTransactionSnapshot());
        snap_pushed = true;
    }

    /*
     * Run all the remaining triggers.  Loop until they are all gone, in case
     * some trigger queues more for us to do.
     */
    while afterTriggerMarkEvents(events, core::ptr::null_mut(), false) {
        let firing_id: CommandId = afterTriggers.firing_counter;
        afterTriggers.firing_counter += 1;

        if afterTriggerInvokeEvents(events, firing_id, core::ptr::null_mut(), true) {
            break; /* all fired */
        }
    }

    /*
     * We don't bother freeing the event list, since it will go away anyway
     * (and more efficiently than via pfree) in AfterTriggerEndXact.
     */

    if snap_pushed {
        PopActiveSnapshot();
    }
}

/* ----------
 * AfterTriggerEndXact()
 *
 *	The current transaction is finishing.
 *
 *	Any unfired triggers are canceled so we simply throw
 *	away anything we know.
 * ----------
 */
pub unsafe fn AfterTriggerEndXact(_isCommit: bool) {
    /*
     * Forget the pending-events list.
     *
     * Since all the info is in TopTransactionContext or children thereof, we
     * don't really need to do anything to reclaim memory.  However, the
     * pending-events list could be large, and so it's useful to discard it as
     * soon as possible --- especially if we are aborting because we ran out
     * of memory for the list!
     */
    if !afterTriggers.event_cxt.is_null() {
        MemoryContextDelete(afterTriggers.event_cxt);
        afterTriggers.event_cxt = core::ptr::null_mut();
        afterTriggers.events.head = core::ptr::null_mut();
        afterTriggers.events.tail = core::ptr::null_mut();
        afterTriggers.events.tailfree = core::ptr::null_mut();
    }

    /*
     * Forget any subtransaction state as well.  Since this can't be very
     * large, we let the eventual reset of TopTransactionContext free the
     * memory instead of doing it here.
     */
    afterTriggers.trans_stack = core::ptr::null_mut();
    afterTriggers.maxtransdepth = 0;

    /*
     * Forget the query stack and constraint-related state information.  As
     * with the subtransaction state information, we don't bother freeing the
     * memory here.
     */
    afterTriggers.query_stack = core::ptr::null_mut();
    afterTriggers.maxquerydepth = 0;
    afterTriggers.state = core::ptr::null_mut();

    /* No more afterTriggers manipulation until next transaction starts. */
    afterTriggers.query_depth = -1;
}

/*
 * AfterTriggerBeginSubXact()
 *
 *	Start a subtransaction.
 */
pub unsafe fn AfterTriggerBeginSubXact() {
    let my_level: c_int = GetCurrentTransactionNestLevel();

    /*
     * Allocate more space in the trans_stack if needed.  (Note: because the
     * minimum nest level of a subtransaction is 2, we waste the first couple
     * entries of the array; not worth the notational effort to avoid it.)
     */
    while my_level >= afterTriggers.maxtransdepth {
        if afterTriggers.maxtransdepth == 0 {
            /* Arbitrarily initialize for max of 8 subtransaction levels */
            afterTriggers.trans_stack = MemoryContextAlloc(
                TopTransactionContext,
                8 * core::mem::size_of::<AfterTriggersTransData>(),
            ) as *mut AfterTriggersTransData;
            afterTriggers.maxtransdepth = 8;
        } else {
            /* repalloc will keep the stack in the same context */
            let new_alloc: c_int = afterTriggers.maxtransdepth * 2;

            afterTriggers.trans_stack = repalloc(
                afterTriggers.trans_stack as *mut c_void,
                new_alloc as usize * core::mem::size_of::<AfterTriggersTransData>(),
            ) as *mut AfterTriggersTransData;
            afterTriggers.maxtransdepth = new_alloc;
        }
    }

    /*
     * Push the current information into the stack.  The SET CONSTRAINTS state
     * is not saved until/unless changed.  Likewise, we don't make a
     * per-subtransaction event context until needed.
     */
    let slot = afterTriggers.trans_stack.add(my_level as usize);
    (*slot).state = core::ptr::null_mut();
    (*slot).events = afterTriggers.events;
    (*slot).query_depth = afterTriggers.query_depth;
    (*slot).firing_counter = afterTriggers.firing_counter;
}

/*
 * AfterTriggerEndSubXact()
 *
 *	The current subtransaction is ending.
 */
pub unsafe fn AfterTriggerEndSubXact(isCommit: bool) {
    let my_level: c_int = GetCurrentTransactionNestLevel();
    let state: SetConstraintState;
    let mut event: AfterTriggerEvent;
    let mut chunk: *mut AfterTriggerEventChunk;
    let subxact_firing_id: CommandId;

    /*
     * Pop the prior state if needed.
     */
    if isCommit {
        /* Assert(my_level < afterTriggers.maxtransdepth); */
        /* If we saved a prior state, we don't need it anymore */
        let slot = afterTriggers.trans_stack.add(my_level as usize);
        state = (*slot).state;
        if !state.is_null() {
            pfree(state as *mut c_void);
        }
        /* this avoids double pfree if error later: */
        (*slot).state = core::ptr::null_mut();
        /* Assert(afterTriggers.query_depth == ...trans_stack[my_level].query_depth); */
    } else {
        /*
         * Aborting.  It is possible subxact start failed before calling
         * AfterTriggerBeginSubXact, in which case we mustn't risk touching
         * trans_stack levels that aren't there.
         */
        if my_level >= afterTriggers.maxtransdepth {
            return;
        }

        let slot = afterTriggers.trans_stack.add(my_level as usize);

        /*
         * Release query-level storage for queries being aborted, and restore
         * query_depth to its pre-subxact value.  This assumes that a
         * subtransaction will not add events to query levels started in a
         * earlier transaction state.
         */
        while afterTriggers.query_depth > (*slot).query_depth {
            if afterTriggers.query_depth < afterTriggers.maxquerydepth {
                AfterTriggerFreeQuery(
                    afterTriggers.query_stack.add(afterTriggers.query_depth as usize),
                );
            }
            afterTriggers.query_depth -= 1;
        }
        /* Assert(afterTriggers.query_depth == (*slot).query_depth); */

        /*
         * Restore the global deferred-event list to its former length,
         * discarding any events queued by the subxact.
         */
        afterTriggerRestoreEventList(&raw mut afterTriggers.events, &raw const (*slot).events);

        /*
         * Restore the trigger state.  If the saved state is NULL, then this
         * subxact didn't save it, so it doesn't need restoring.
         */
        state = (*slot).state;
        if !state.is_null() {
            pfree(afterTriggers.state as *mut c_void);
            afterTriggers.state = state;
        }
        /* this avoids double pfree if error later: */
        (*slot).state = core::ptr::null_mut();

        /*
         * Scan for any remaining deferred events that were marked DONE or IN
         * PROGRESS by this subxact or a child, and un-mark them. We can
         * recognize such events because they have a firing ID greater than or
         * equal to the firing_counter value we saved at subtransaction start.
         * (This essentially assumes that the current subxact includes all
         * subxacts started after it.)
         */
        subxact_firing_id = (*slot).firing_counter;
        /* for_each_event_chunk(event, chunk, afterTriggers.events) */
        chunk = afterTriggers.events.head;
        while !chunk.is_null() {
            event = CHUNK_DATA_START(chunk) as AfterTriggerEvent;
            while (event as *const c_char) < (*chunk).freeptr {
                let evtshared: AfterTriggerShared = GetTriggerSharedData(event);

                if ((*event).ate_flags & (AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS)) != 0 {
                    if (*evtshared).ats_firing_id >= subxact_firing_id {
                        (*event).ate_flags &= !(AFTER_TRIGGER_DONE | AFTER_TRIGGER_IN_PROGRESS);
                    }
                }

                event = ((event as *mut c_char).add(SizeofTriggerEvent(event))) as AfterTriggerEvent;
            }
            chunk = (*chunk).next;
        }
    }
}

/* ----------
 * AfterTriggerSetState()
 *
 *	Execute the SET CONSTRAINTS ... utility command.
 * ----------
 */
pub unsafe fn AfterTriggerSetState(stmt: *mut crate::nodes::parsenodes::ConstraintsSetStmt) {
    use crate::nodes::pg_list::{NIL, lappend_oid, list_free, lfirst, list_length};
    use crate::list_make1_oid;
    use crate::catalog::namespace::{LookupExplicitNamespace, fetch_search_path};
    use crate::catalog::objectaddress_impl::get_database_name;
    use crate::miscadmin::MyDatabaseId;
    use crate::catalog::pg_constraint::Form_pg_constraint;
    use crate::catalog::pg_trigger::Form_pg_trigger;

    /* Local catalog index OIDs not yet centralized in the port. */
    const ConstraintRelationId: Oid = 2606;
    const ConstraintNameNspIndexId: Oid = 2664;
    const ConstraintParentIndexId: Oid = 2579;
    const TriggerConstraintIndexId: Oid = 2699;
    const Anum_pg_constraint_conname: c_int = 2;
    const Anum_pg_constraint_connamespace: c_int = 3;
    const Anum_pg_constraint_conparentid: c_int = 12;
    const Anum_pg_trigger_tgconstraint: c_int = 11;

    let my_level: c_int = GetCurrentTransactionNestLevel();

    /* If we haven't already done so, initialize our state. */
    if afterTriggers.state.is_null() {
        afterTriggers.state = SetConstraintStateCreate(8);
    }

    /*
     * If in a subtransaction, and we didn't save the current state already,
     * save it so it can be restored if the subtransaction aborts.
     */
    if my_level > 1 && (*afterTriggers.trans_stack.add(my_level as usize)).state.is_null() {
        (*afterTriggers.trans_stack.add(my_level as usize)).state =
            SetConstraintStateCopy(afterTriggers.state);
    }

    /*
     * Handle SET CONSTRAINTS ALL ...
     */
    if (*stmt).constraints == NIL {
        /*
         * Forget any previous SET CONSTRAINTS commands in this transaction.
         */
        (*afterTriggers.state).numstates = 0;

        /*
         * Set the per-transaction ALL state to known.
         */
        (*afterTriggers.state).all_isset = true;
        (*afterTriggers.state).all_isdeferred = (*stmt).deferred;
    } else {
        let conrel: Relation;
        let tgrel: Relation;
        let mut conoidlist: *mut List = NIL;
        let mut tgoidlist: *mut List = NIL;

        /*
         * Handle SET CONSTRAINTS constraint-name [, ...]
         *
         * First, identify all the named constraints and make a list of their
         * OIDs.  A constraint in a partitioned table may have corresponding
         * constraints in the partitions.  Grab those too.
         */
        conrel = table_open(ConstraintRelationId, AccessShareLock);

        crate::foreach!(lc, (*stmt).constraints, {
            let constraint = lfirst(crate::current_cell!(lc)) as *mut crate::nodes::primnodes::RangeVar;
            let mut found: bool;
            let namespacelist: *mut List;

            if !(*constraint).catalogname.is_null() {
                if strcmp((*constraint).catalogname, get_database_name(MyDatabaseId)) != 0 {
                    ereport!(ERROR, errmsg!(
                        "cross-database references are not implemented: \"{}.{}.{}\"",
                        cstr((*constraint).catalogname), cstr((*constraint).schemaname),
                        cstr((*constraint).relname)));
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED) */
                }
            }

            /*
             * If we're given the schema name with the constraint, look only
             * in that schema.  If given a bare constraint name, use the
             * search path to find the first matching constraint.
             */
            if !(*constraint).schemaname.is_null() {
                let namespaceId: Oid = LookupExplicitNamespace((*constraint).schemaname, false);
                namespacelist = list_make1_oid!(namespaceId);
            } else {
                namespacelist = fetch_search_path(true);
            }

            found = false;
            crate::foreach!(nslc, namespacelist, {
                let namespaceId: Oid = crate::nodes::pg_list::lfirst_oid(crate::current_cell!(nslc));
                let conscan: SysScanDesc;
                let mut skey: [ScanKeyData; 2] = core::mem::zeroed();
                let mut tup: HeapTuple;

                ScanKeyInit(&raw mut skey[0], Anum_pg_constraint_conname,
                    BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum((*constraint).relname));
                ScanKeyInit(&raw mut skey[1], Anum_pg_constraint_connamespace,
                    BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(namespaceId));

                conscan = systable_beginscan(conrel, ConstraintNameNspIndexId,
                    true, core::ptr::null_mut(), 2, skey.as_mut_ptr());

                loop {
                    tup = systable_getnext(conscan);
                    if !HeapTupleIsValid(tup) { break; }
                    let con = GETSTRUCT(tup) as Form_pg_constraint;

                    if (*con).condeferrable {
                        conoidlist = lappend_oid(conoidlist, (*con).oid);
                    } else if (*stmt).deferred {
                        ereport!(ERROR, errmsg!(
                            "constraint \"{}\" is not deferrable", cstr((*constraint).relname)));
                        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                    }
                    found = true;
                }

                systable_endscan(conscan);

                /*
                 * Once we've found a matching constraint we do not search
                 * later parts of the search path.
                 */
                if found { break; }
            });

            list_free(namespacelist);

            /*
             * Not found ?
             */
            if !found {
                ereport!(ERROR, errmsg!(
                    "constraint \"{}\" does not exist", cstr((*constraint).relname)));
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT) */
            }
        });

        /*
         * Scan for any possible descendants of the constraints.  We append
         * whatever we find to the same list that we're scanning; this has the
         * effect that we create new scans for those, too, so if there are
         * further descendents, we'll also catch them.
         */
        crate::foreach!(lc, conoidlist, {
            let parent: Oid = crate::nodes::pg_list::lfirst_oid(crate::current_cell!(lc));
            let mut key: ScanKeyData = core::mem::zeroed();
            let scan: SysScanDesc;
            let mut tuple: HeapTuple;

            ScanKeyInit(&raw mut key, Anum_pg_constraint_conparentid,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(parent));

            scan = systable_beginscan(conrel, ConstraintParentIndexId, true,
                core::ptr::null_mut(), 1, &raw mut key);

            loop {
                tuple = systable_getnext(scan);
                if !HeapTupleIsValid(tuple) { break; }
                let con = GETSTRUCT(tuple) as Form_pg_constraint;
                conoidlist = lappend_oid(conoidlist, (*con).oid);
            }

            systable_endscan(scan);
        });

        table_close(conrel, AccessShareLock);

        /*
         * Now, locate the trigger(s) implementing each of these constraints,
         * and make a list of their OIDs.
         */
        tgrel = table_open(TriggerRelationId, AccessShareLock);

        crate::foreach!(lc, conoidlist, {
            let conoid: Oid = crate::nodes::pg_list::lfirst_oid(crate::current_cell!(lc));
            let mut skey: ScanKeyData = core::mem::zeroed();
            let tgscan: SysScanDesc;
            let mut htup: HeapTuple;

            ScanKeyInit(&raw mut skey, Anum_pg_trigger_tgconstraint,
                BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(conoid));

            tgscan = systable_beginscan(tgrel, TriggerConstraintIndexId, true,
                core::ptr::null_mut(), 1, &raw mut skey);

            loop {
                htup = systable_getnext(tgscan);
                if !HeapTupleIsValid(htup) { break; }
                let pg_trigger = GETSTRUCT(htup) as Form_pg_trigger;

                /*
                 * Silently skip triggers that are marked as non-deferrable in
                 * pg_trigger.  This is not an error condition, since a
                 * deferrable RI constraint may have some non-deferrable
                 * actions.
                 */
                if (*pg_trigger).tgdeferrable {
                    tgoidlist = lappend_oid(tgoidlist, (*pg_trigger).oid);
                }
            }

            systable_endscan(tgscan);
        });

        table_close(tgrel, AccessShareLock);

        /*
         * Now we can set the trigger states of individual triggers for this
         * xact.
         */
        crate::foreach!(lc, tgoidlist, {
            let tgoid: Oid = crate::nodes::pg_list::lfirst_oid(crate::current_cell!(lc));
            let state: SetConstraintState = afterTriggers.state;
            let mut found = false;
            let mut i = 0;

            while i < (*state).numstates {
                let ts = (*state).trigstates.as_mut_ptr().add(i as usize);
                if (*ts).sct_tgoid == tgoid {
                    (*ts).sct_tgisdeferred = (*stmt).deferred;
                    found = true;
                    break;
                }
                i += 1;
            }
            if !found {
                afterTriggers.state =
                    SetConstraintStateAddItem(state, tgoid, (*stmt).deferred);
            }
        });
        let _ = list_length;
    }

    /*
     * SQL99 requires that when a constraint is set to IMMEDIATE, any deferred
     * checks against that constraint must be made when the SET CONSTRAINTS
     * command is executed -- i.e. the effects of the SET CONSTRAINTS command
     * apply retroactively.
     */
    if !(*stmt).deferred {
        let events: *mut AfterTriggerEventList = &raw mut afterTriggers.events;
        let mut snapshot_set = false;

        while afterTriggerMarkEvents(events, core::ptr::null_mut(), true) {
            let firing_id: CommandId = afterTriggers.firing_counter;
            afterTriggers.firing_counter += 1;

            /*
             * Make sure a snapshot has been established in case trigger
             * functions need one.
             */
            if !snapshot_set {
                PushActiveSnapshot(GetTransactionSnapshot());
                snapshot_set = true;
            }

            /*
             * We can delete fired events if we are at top transaction level,
             * but we'd better not if inside a subtransaction, since the
             * subtransaction could later get rolled back.
             */
            if afterTriggerInvokeEvents(events, firing_id, core::ptr::null_mut(),
                !IsSubTransaction()) {
                break; /* all fired */
            }
        }

        if snapshot_set {
            PopActiveSnapshot();
        }
    }
}

/* ----------
 * AfterTriggerSaveEvent()
 *
 *	Called by ExecA[RS]...Triggers() to queue up the triggers that should
 *	be fired for an event.
 * ----------
 */
pub unsafe fn AfterTriggerSaveEvent(
    estate: *mut EState,
    relinfo: *mut ResultRelInfo,
    src_partinfo: *mut ResultRelInfo,
    dst_partinfo: *mut ResultRelInfo,
    event: c_int,
    row_trigger: bool,
    mut oldslot: *mut TupleTableSlot,
    mut newslot: *mut TupleTableSlot,
    recheckIndexes: *mut List,
    modifiedCols: *mut Bitmapset,
    transition_capture: *mut TransitionCaptureState,
    is_crosspart_update: bool,
) {
    use crate::catalog::pg_trigger::{
        TRIGGER_TYPE_INSERT, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_TRUNCATE,
        TRIGGER_TYPE_ROW, TRIGGER_TYPE_STATEMENT, TRIGGER_TYPE_AFTER,
    };
    use crate::utils::adt::ri_triggers::{
        RI_FKey_trigger_type, RI_FKey_pk_upd_check_required, RI_FKey_fk_upd_check_required,
        RI_TRIGGER_PK, RI_TRIGGER_FK, RI_TRIGGER_NONE,
    };
    use crate::storage::itemptr::{ItemPointerCopy, ItemPointerSetInvalid};
    use crate::nodes::pg_list::list_member_oid;
    use crate::miscadmin::GetUserId;
    use crate::executor::execUtils::{ExecGetTriggerOldSlot, ExecGetTriggerNewSlot, ExecGetChildToRootMap};
    use crate::access::common::tupconvert::execute_attr_map_slot;
    use crate::executor::tuptable::ExecCopySlot;

    /* pg_proc OID of unique_key_recheck (fmgroids.h). */
    const F_UNIQUE_KEY_RECHECK: Oid = 1250;

    let rel: Relation = (*relinfo).ri_RelationDesc;
    let trigdesc: *mut TriggerDesc = (*relinfo).ri_TrigDesc as *mut TriggerDesc;
    let mut new_event: AfterTriggerEventData = core::mem::zeroed();
    let mut new_shared: AfterTriggerSharedData = core::mem::zeroed();
    let relkind: c_char = (*(*rel).rd_rel).relkind;
    let tgtype_event: c_int;
    let tgtype_level: c_int;
    let mut i: c_int;
    let mut fdw_tuplestore: *mut Tuplestorestate = core::ptr::null_mut();

    /*
     * Check state.  We use a normal test not Assert because it is possible to
     * reach here in the wrong state given misconfigured RI triggers.
     */
    if afterTriggers.query_depth < 0 {
        elog!(ERROR, "AfterTriggerSaveEvent() called outside of query");
    }

    /* Be sure we have enough space to record events at this query depth. */
    if afterTriggers.query_depth >= afterTriggers.maxquerydepth {
        AfterTriggerEnlargeQueryState();
    }

    /*
     * If the directly named relation has any triggers with transition tables,
     * then we need to capture transition tuples.
     */
    if row_trigger && !transition_capture.is_null() {
        let original_insert_tuple: *mut TupleTableSlot =
            (*transition_capture).tcs_original_insert_tuple;

        /*
         * Capture the old tuple in the appropriate transition table based on
         * the event.
         */
        if !TupIsNull(oldslot) {
            let old_tuplestore: *mut Tuplestorestate = GetAfterTriggersTransitionTable(
                event, oldslot, core::ptr::null_mut(), transition_capture);
            TransitionTableAddTuple(estate, event, transition_capture, relinfo,
                oldslot, core::ptr::null_mut(), old_tuplestore);
        }

        /*
         * Capture the new tuple in the appropriate transition table based on
         * the event.
         */
        if !TupIsNull(newslot) {
            let new_tuplestore: *mut Tuplestorestate = GetAfterTriggersTransitionTable(
                event, core::ptr::null_mut(), newslot, transition_capture);
            TransitionTableAddTuple(estate, event, transition_capture, relinfo,
                newslot, original_insert_tuple, new_tuplestore);
        }

        /*
         * If transition tables are the only reason we're here, return.
         */
        if trigdesc.is_null()
            || (event == TRIGGER_EVENT_DELETE as c_int && !(*trigdesc).trig_delete_after_row)
            || (event == TRIGGER_EVENT_INSERT as c_int && !(*trigdesc).trig_insert_after_row)
            || (event == TRIGGER_EVENT_UPDATE as c_int && !(*trigdesc).trig_update_after_row)
            || (event == TRIGGER_EVENT_UPDATE as c_int
                && (TupIsNull(oldslot) ^ TupIsNull(newslot)))
        {
            return;
        }
    }

    /*
     * We normally don't see partitioned tables here for row level triggers
     * except in the special case of a cross-partition update.
     */
    /* Assert(!row_trigger || relkind != RELKIND_PARTITIONED_TABLE || (is_crosspart_update && ...)); */

    /*
     * Validate the event code and collect the associated tuple CTIDs.
     */
    match event as TriggerEvent {
        TRIGGER_EVENT_INSERT => {
            tgtype_event = TRIGGER_TYPE_INSERT as c_int;
            if row_trigger {
                ItemPointerCopy(&raw const (*newslot).tts_tid, &raw mut new_event.ate_ctid1);
                ItemPointerSetInvalid(&raw mut new_event.ate_ctid2);
            } else {
                ItemPointerSetInvalid(&raw mut new_event.ate_ctid1);
                ItemPointerSetInvalid(&raw mut new_event.ate_ctid2);
                cancel_prior_stmt_triggers(RelationGetRelid(rel), CmdType::CMD_INSERT, event);
            }
        }
        TRIGGER_EVENT_DELETE => {
            tgtype_event = TRIGGER_TYPE_DELETE as c_int;
            if row_trigger {
                ItemPointerCopy(&raw const (*oldslot).tts_tid, &raw mut new_event.ate_ctid1);
                ItemPointerSetInvalid(&raw mut new_event.ate_ctid2);
            } else {
                ItemPointerSetInvalid(&raw mut new_event.ate_ctid1);
                ItemPointerSetInvalid(&raw mut new_event.ate_ctid2);
                cancel_prior_stmt_triggers(RelationGetRelid(rel), CmdType::CMD_DELETE, event);
            }
        }
        TRIGGER_EVENT_UPDATE => {
            tgtype_event = TRIGGER_TYPE_UPDATE as c_int;
            if row_trigger {
                ItemPointerCopy(&raw const (*oldslot).tts_tid, &raw mut new_event.ate_ctid1);
                ItemPointerCopy(&raw const (*newslot).tts_tid, &raw mut new_event.ate_ctid2);

                /*
                 * Also remember the OIDs of partitions to fetch these tuples
                 * out of later in AfterTriggerExecute().
                 */
                if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
                    new_event.ate_src_part =
                        RelationGetRelid((*src_partinfo).ri_RelationDesc);
                    new_event.ate_dst_part =
                        RelationGetRelid((*dst_partinfo).ri_RelationDesc);
                }
            } else {
                ItemPointerSetInvalid(&raw mut new_event.ate_ctid1);
                ItemPointerSetInvalid(&raw mut new_event.ate_ctid2);
                cancel_prior_stmt_triggers(RelationGetRelid(rel), CmdType::CMD_UPDATE, event);
            }
        }
        TRIGGER_EVENT_TRUNCATE => {
            tgtype_event = TRIGGER_TYPE_TRUNCATE as c_int;
            ItemPointerSetInvalid(&raw mut new_event.ate_ctid1);
            ItemPointerSetInvalid(&raw mut new_event.ate_ctid2);
        }
        _ => {
            elog!(ERROR, "invalid after-trigger event code: {}", event);
            #[allow(unreachable_code)]
            { tgtype_event = 0; }
        }
    }

    /* Determine flags */
    if !(relkind == RELKIND_FOREIGN_TABLE && row_trigger) {
        if row_trigger && event == TRIGGER_EVENT_UPDATE as c_int {
            if relkind == RELKIND_PARTITIONED_TABLE {
                new_event.ate_flags = AFTER_TRIGGER_CP_UPDATE;
            } else {
                new_event.ate_flags = AFTER_TRIGGER_2CTID;
            }
        } else {
            new_event.ate_flags = AFTER_TRIGGER_1CTID;
        }
    }
    /* else, we'll initialize ate_flags for each trigger */

    tgtype_level = if row_trigger { TRIGGER_TYPE_ROW as c_int } else { TRIGGER_TYPE_STATEMENT as c_int };

    /*
     * Must convert/copy the source and destination partition tuples into the
     * root partitioned table's format/slot.
     */
    if row_trigger && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
        let mut rootslot: *mut TupleTableSlot;
        let mut map: *mut crate::access::common::tupconvert::TupleConversionMap;

        rootslot = ExecGetTriggerOldSlot(estate, relinfo);
        map = ExecGetChildToRootMap(src_partinfo);
        if !map.is_null() {
            oldslot = execute_attr_map_slot((*map).attrMap, oldslot, rootslot);
        } else {
            oldslot = ExecCopySlot(rootslot, oldslot);
        }

        rootslot = ExecGetTriggerNewSlot(estate, relinfo);
        map = ExecGetChildToRootMap(dst_partinfo);
        if !map.is_null() {
            newslot = execute_attr_map_slot((*map).attrMap, newslot, rootslot);
        } else {
            newslot = ExecCopySlot(rootslot, newslot);
        }
    }

    i = 0;
    while i < (*trigdesc).numtriggers {
        let trigger: *mut Trigger = (*trigdesc).triggers.add(i as usize);

        if !TRIGGER_TYPE_MATCHES((*trigger).tgtype, tgtype_level as i16,
            TRIGGER_TYPE_AFTER as i16, tgtype_event as i16) {
            i += 1;
            continue;
        }
        if !TriggerEnabled(estate, relinfo, trigger, event, modifiedCols, oldslot, newslot) {
            i += 1;
            continue;
        }

        if relkind == RELKIND_FOREIGN_TABLE && row_trigger {
            if fdw_tuplestore.is_null() {
                fdw_tuplestore = GetCurrentFDWTuplestore();
                new_event.ate_flags = AFTER_TRIGGER_FDW_FETCH;
            } else {
                /* subsequent event for the same tuple */
                new_event.ate_flags = AFTER_TRIGGER_FDW_REUSE;
            }
        }

        /*
         * If the trigger is a foreign key enforcement trigger, there are
         * certain cases where we can skip queueing the event.
         */
        if TRIGGER_FIRED_BY_UPDATE(event as TriggerEvent) || TRIGGER_FIRED_BY_DELETE(event as TriggerEvent) {
            match RI_FKey_trigger_type((*trigger).tgfoid) {
                x if x == RI_TRIGGER_PK => {
                    /*
                     * For cross-partitioned updates of partitioned PK table,
                     * skip the event fired by the component delete on the
                     * source leaf partition unless the constraint originates
                     * in the partition itself (!tgisclone).
                     */
                    if is_crosspart_update
                        && TRIGGER_FIRED_BY_DELETE(event as TriggerEvent)
                        && (*trigger).tgisclone
                    {
                        i += 1;
                        continue;
                    }

                    /* Update or delete on trigger's PK table */
                    if !RI_FKey_pk_upd_check_required(trigger, rel, oldslot, newslot) {
                        /* skip queuing this event */
                        i += 1;
                        continue;
                    }
                }
                x if x == RI_TRIGGER_FK => {
                    /*
                     * Update on trigger's FK table.
                     */
                    if (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE
                        || !RI_FKey_fk_upd_check_required(trigger, rel, oldslot, newslot)
                    {
                        /* skip queuing this event */
                        i += 1;
                        continue;
                    }
                }
                x if x == RI_TRIGGER_NONE => {
                    /*
                     * Not an FK trigger.  No need to queue the update event
                     * fired during a cross-partitioned update of a
                     * partitioned table.
                     */
                    if row_trigger && (*(*rel).rd_rel).relkind == RELKIND_PARTITIONED_TABLE {
                        i += 1;
                        continue;
                    }
                }
                _ => {}
            }
        }

        /*
         * If the trigger is a deferred unique constraint check trigger, only
         * queue it if the unique constraint was potentially violated.
         */
        if (*trigger).tgfoid == F_UNIQUE_KEY_RECHECK {
            if !list_member_oid(recheckIndexes, (*trigger).tgconstrindid) {
                i += 1;
                continue; /* Uniqueness definitely not violated */
            }
        }

        /*
         * Fill in event structure and add it to the current query's queue.
         */
        new_shared.ats_event = (event as TriggerEvent & TRIGGER_EVENT_OPMASK)
            | (if row_trigger { TRIGGER_EVENT_ROW } else { 0 })
            | (if (*trigger).tgdeferrable { AFTER_TRIGGER_DEFERRABLE } else { 0 })
            | (if (*trigger).tginitdeferred { AFTER_TRIGGER_INITDEFERRED } else { 0 });
        new_shared.ats_tgoid = (*trigger).tgoid;
        new_shared.ats_relid = RelationGetRelid(rel);
        new_shared.ats_rolid = GetUserId();
        new_shared.ats_firing_id = 0;
        if (!(*trigger).tgoldtable.is_null() || !(*trigger).tgnewtable.is_null())
            && !transition_capture.is_null()
        {
            match event as TriggerEvent {
                TRIGGER_EVENT_INSERT => {
                    new_shared.ats_table = (*transition_capture).tcs_insert_private as *mut AfterTriggersTableData;
                }
                TRIGGER_EVENT_UPDATE => {
                    new_shared.ats_table = (*transition_capture).tcs_update_private as *mut AfterTriggersTableData;
                }
                TRIGGER_EVENT_DELETE => {
                    new_shared.ats_table = (*transition_capture).tcs_delete_private as *mut AfterTriggersTableData;
                }
                _ => {
                    /* Must be TRUNCATE, see switch above */
                    new_shared.ats_table = core::ptr::null_mut();
                }
            }
        } else {
            new_shared.ats_table = core::ptr::null_mut();
        }
        new_shared.ats_modifiedcols = modifiedCols;

        afterTriggerAddEvent(
            &raw mut (*afterTriggers.query_stack.add(afterTriggers.query_depth as usize)).events,
            &raw mut new_event, &raw mut new_shared);

        i += 1;
    }

    /*
     * Finally, spool any foreign tuple(s).
     */
    if !fdw_tuplestore.is_null() {
        if !oldslot.is_null() {
            tuplestore_puttupleslot(fdw_tuplestore, oldslot);
        }
        if !newslot.is_null() {
            tuplestore_puttupleslot(fdw_tuplestore, newslot);
        }
    }
}

// --- genuinely-unported helper deps for after-trigger lifecycle: local stubs ---

use crate::access::transam::xact::{GetCurrentTransactionNestLevel, IsSubTransaction};

/* snapmgr.rs exists but utils/time/mod.rs does not declare it; local stubs as in parallel.rs. */
type Snapshot = *mut core::ffi::c_void;
unsafe fn GetTransactionSnapshot() -> Snapshot {
    /* TODO(pg-port): utils/time/snapmgr.c GetTransactionSnapshot */
    core::ptr::null_mut()
}
unsafe fn PushActiveSnapshot(_snapshot: Snapshot) {
    /* TODO(pg-port): utils/time/snapmgr.c PushActiveSnapshot */
}
unsafe fn PopActiveSnapshot() {
    /* TODO(pg-port): utils/time/snapmgr.c PopActiveSnapshot */
}

unsafe fn TriggerEnabled(
    _estate: *mut EState,
    _relinfo: *mut ResultRelInfo,
    _trigger: *mut Trigger,
    _event: c_int,
    _modifiedCols: *mut Bitmapset,
    _oldslot: *mut TupleTableSlot,
    _newslot: *mut TupleTableSlot,
) -> bool {
    /* TODO(pg-port): commands/trigger.c TriggerEnabled */
    true
}

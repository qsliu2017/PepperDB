//! utils/time/combocid.c - Combo command ID support routines.
//!
//! Before version 8.3, HeapTupleHeaderData had separate fields for cmin and
//! cmax.  cmin and cmax are now overlaid in the same header field; when the
//! inserting transaction also deletes the tuple we create a "combo" command ID
//! and store it instead.  The combo CID maps back to the real cmin/cmax via a
//! backend-private array (and a cmin,cmax -> combocid hash table for reuse),
//! both kept in TopTransactionContext and destroyed at end of transaction.

use crate::prelude::*;

use crate::access::htup_details::{
    HeapTupleHeaderData, HeapTupleHeaderGetRawCommandId, HeapTupleHeaderGetRawXmin,
    HeapTupleHeaderGetUpdateXid, HeapTupleHeaderGetXmin, HeapTupleHeaderXminCommitted,
    HEAP_COMBOCID, HEAP_MOVED,
};
use crate::utils::hash::dynahash::{
    hash_create, hash_search, HASHACTION, HASHCTL, HASH_BLOBS, HASH_CONTEXT, HASH_ELEM, HTAB,
};
use crate::utils::mmgr::mcxt::TopTransactionContext;

use crate::miscadmin::CritSectionCount;

/* ---- locally-stubbed, not-yet-ported dependencies ---- */

/// transam/xact.c (not yet ported)
unsafe fn TransactionIdIsCurrentTransactionId(_xid: TransactionId) -> bool {
    unimplemented!()
}

/// shmem.c add_size (not yet exported there); faithful overflow-checked add.
#[inline]
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    let result = s1.wrapping_add(s2);
    /* We are assuming Size is an unsigned type here... */
    if result < s1 || result < s2 {
        ereport!(ERROR, "requested shared memory size overflows size_t");
    }
    result
}

/// shmem.c mul_size (not yet exported there); faithful overflow-checked mul.
#[inline]
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    if s1 == 0 || s2 == 0 {
        return 0;
    }
    let result = s1.wrapping_mul(s2);
    if result / s2 != s1 {
        ereport!(ERROR, "requested shared memory size overflows size_t");
    }
    result
}

/* Hash table to lookup combo CIDs by cmin and cmax */
static mut comboHash: *mut HTAB = null_mut();

/* Key and entry structures for the hash table */
#[repr(C)]
#[derive(Clone, Copy)]
struct ComboCidKeyData {
    cmin: CommandId,
    cmax: CommandId,
}

type ComboCidKey = *mut ComboCidKeyData;

#[repr(C)]
struct ComboCidEntryData {
    key: ComboCidKeyData,
    combocid: CommandId,
}

type ComboCidEntry = *mut ComboCidEntryData;

/* Initial size of the hash table */
const CCID_HASH_SIZE: c_long = 100;

/*
 * An array of cmin,cmax pairs, indexed by combo command id.
 * To convert a combo CID to cmin and cmax, you do a simple array lookup.
 */
static mut comboCids: ComboCidKey = null_mut();
static mut usedComboCids: c_int = 0; /* number of elements in comboCids */
static mut sizeComboCids: c_int = 0; /* allocated size of array */

/* Initial size of the array */
const CCID_ARRAY_SIZE: c_int = 100;

/**** External API ****/

/*
 * GetCmin and GetCmax assert that they are only called in situations where
 * they make sense, that is, can deliver a useful answer.  If you have
 * reason to examine a tuple's t_cid field from a transaction other than
 * the originating one, use HeapTupleHeaderGetRawCommandId() directly.
 */

pub unsafe fn HeapTupleHeaderGetCmin(tup: *const HeapTupleHeaderData) -> CommandId {
    let cid: CommandId = HeapTupleHeaderGetRawCommandId(tup);

    Assert!(((*tup).t_infomask & HEAP_MOVED) == 0);
    Assert!(TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tup)));

    if ((*tup).t_infomask & HEAP_COMBOCID) != 0 {
        GetRealCmin(cid)
    } else {
        cid
    }
}

pub unsafe fn HeapTupleHeaderGetCmax(tup: *const HeapTupleHeaderData) -> CommandId {
    let cid: CommandId = HeapTupleHeaderGetRawCommandId(tup);

    Assert!(((*tup).t_infomask & HEAP_MOVED) == 0);

    /*
     * Because GetUpdateXid() performs memory allocations if xmax is a
     * multixact we can't Assert() if we're inside a critical section. This
     * weakens the check, but not using GetCmax() inside one would complicate
     * things too much.
     */
    Assert!(
        CritSectionCount > 0
            || TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(tup))
    );

    if ((*tup).t_infomask & HEAP_COMBOCID) != 0 {
        GetRealCmax(cid)
    } else {
        cid
    }
}

/*
 * Given a tuple we are about to delete, determine the correct value to store
 * into its t_cid field.
 *
 * If we don't need a combo CID, *cmax is unchanged and *iscombo is set to
 * false.  If we do need one, *cmax is replaced by a combo CID and *iscombo
 * is set to true.
 *
 * The reason this is separate from the actual HeapTupleHeaderSetCmax()
 * operation is that this could fail due to out-of-memory conditions.  Hence
 * we need to do this before entering the critical section that actually
 * changes the tuple in shared buffers.
 */
pub unsafe fn HeapTupleHeaderAdjustCmax(
    tup: *const HeapTupleHeaderData,
    cmax: *mut CommandId,
    iscombo: *mut bool,
) {
    /*
     * If we're marking a tuple deleted that was inserted by (any
     * subtransaction of) our transaction, we need to use a combo command id.
     * Test for HeapTupleHeaderXminCommitted() first, because it's cheaper
     * than a TransactionIdIsCurrentTransactionId call.
     */
    if !HeapTupleHeaderXminCommitted(tup)
        && TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetRawXmin(tup))
    {
        let cmin: CommandId = HeapTupleHeaderGetCmin(tup);

        *cmax = GetComboCommandId(cmin, *cmax);
        *iscombo = true;
    } else {
        *iscombo = false;
    }
}

/*
 * Combo command ids are only interesting to the inserting and deleting
 * transaction, so we can forget about them at the end of transaction.
 */
pub unsafe fn AtEOXact_ComboCid() {
    /*
     * Don't bother to pfree. These are allocated in TopTransactionContext, so
     * they're going to go away at the end of transaction anyway.
     */
    comboHash = null_mut();

    comboCids = null_mut();
    usedComboCids = 0;
    sizeComboCids = 0;
}

/**** Internal routines ****/

/*
 * Get a combo command id that maps to cmin and cmax.
 *
 * We try to reuse old combo command ids when possible.
 */
unsafe fn GetComboCommandId(cmin: CommandId, cmax: CommandId) -> CommandId {
    let combocid: CommandId;
    let mut key: ComboCidKeyData = std::mem::zeroed();
    let entry: ComboCidEntry;
    let mut found: bool = false;

    /*
     * Create the hash table and array the first time we need to use combo
     * cids in the transaction.
     */
    if comboHash.is_null() {
        let mut hash_ctl: HASHCTL = std::mem::zeroed();

        /* Make array first; existence of hash table asserts array exists */
        comboCids = MemoryContextAlloc(
            TopTransactionContext as *mut _,
            std::mem::size_of::<ComboCidKeyData>() * CCID_ARRAY_SIZE as usize,
        ) as ComboCidKey;
        sizeComboCids = CCID_ARRAY_SIZE;
        usedComboCids = 0;

        hash_ctl.keysize = std::mem::size_of::<ComboCidKeyData>();
        hash_ctl.entrysize = std::mem::size_of::<ComboCidEntryData>();
        hash_ctl.hcxt = TopTransactionContext as *mut _;

        comboHash = hash_create(
            b"Combo CIDs\0".as_ptr() as *const c_char,
            CCID_HASH_SIZE,
            &hash_ctl,
            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
        );
    }

    /*
     * Grow the array if there's not at least one free slot.  We must do this
     * before possibly entering a new hashtable entry, else failure to
     * repalloc would leave a corrupt hashtable entry behind.
     */
    if usedComboCids >= sizeComboCids {
        let newsize: c_int = sizeComboCids * 2;

        comboCids = repalloc(
            comboCids as *mut c_void,
            std::mem::size_of::<ComboCidKeyData>() * newsize as usize,
        ) as ComboCidKey;
        sizeComboCids = newsize;
    }

    /* Lookup or create a hash entry with the desired cmin/cmax */

    /* We assume there is no struct padding in ComboCidKeyData! */
    key.cmin = cmin;
    key.cmax = cmax;
    entry = hash_search(
        comboHash,
        &key as *const ComboCidKeyData as *const c_void,
        HASHACTION::HASH_ENTER,
        &mut found,
    ) as ComboCidEntry;

    if found {
        /* Reuse an existing combo CID */
        return (*entry).combocid;
    }

    /* We have to create a new combo CID; we already made room in the array */
    combocid = usedComboCids as CommandId;

    (*comboCids.add(combocid as usize)).cmin = cmin;
    (*comboCids.add(combocid as usize)).cmax = cmax;
    usedComboCids += 1;

    (*entry).combocid = combocid;

    combocid
}

unsafe fn GetRealCmin(combocid: CommandId) -> CommandId {
    Assert!((combocid as c_int) < usedComboCids);
    (*comboCids.add(combocid as usize)).cmin
}

unsafe fn GetRealCmax(combocid: CommandId) -> CommandId {
    Assert!((combocid as c_int) < usedComboCids);
    (*comboCids.add(combocid as usize)).cmax
}

/*
 * Estimate the amount of space required to serialize the current combo CID
 * state.
 */
pub unsafe fn EstimateComboCIDStateSpace() -> Size {
    let mut size: Size;

    /* Add space required for saving usedComboCids */
    size = std::mem::size_of::<c_int>();

    /* Add space required for saving ComboCidKeyData */
    size = add_size(
        size,
        mul_size(std::mem::size_of::<ComboCidKeyData>(), usedComboCids as Size),
    );

    size
}

/*
 * Serialize the combo CID state into the memory, beginning at start_address.
 * maxsize should be at least as large as the value returned by
 * EstimateComboCIDStateSpace.
 */
pub unsafe fn SerializeComboCIDState(maxsize: Size, start_address: *mut c_char) {
    let endptr: *mut c_char;

    /* First, we store the number of currently-existing combo CIDs. */
    *(start_address as *mut c_int) = usedComboCids;

    /* If maxsize is too small, throw an error. */
    endptr = start_address.add(
        std::mem::size_of::<c_int>()
            + std::mem::size_of::<ComboCidKeyData>() * usedComboCids as usize,
    );
    if endptr < start_address || endptr > start_address.add(maxsize) {
        elog!(ERROR, "not enough space to serialize ComboCID state");
    }

    /* Now, copy the actual cmin/cmax pairs. */
    if usedComboCids > 0 {
        std::ptr::copy_nonoverlapping(
            comboCids as *const c_char,
            start_address.add(std::mem::size_of::<c_int>()),
            std::mem::size_of::<ComboCidKeyData>() * usedComboCids as usize,
        );
    }
}

/*
 * Read the combo CID state at the specified address and initialize this
 * backend with the same combo CIDs.  This is only valid in a backend that
 * currently has no combo CIDs (and only makes sense if the transaction state
 * is serialized and restored as well).
 */
pub unsafe fn RestoreComboCIDState(comboCIDstate: *mut c_char) {
    let num_elements: c_int;
    let keydata: *mut ComboCidKeyData;
    let mut i: c_int;

    Assert!(comboCids.is_null() && comboHash.is_null());

    /* First, we retrieve the number of combo CIDs that were serialized. */
    num_elements = *(comboCIDstate as *mut c_int);
    keydata = comboCIDstate.add(std::mem::size_of::<c_int>()) as *mut ComboCidKeyData;

    /* Use GetComboCommandId to restore each combo CID. */
    i = 0;
    while i < num_elements {
        let cid = GetComboCommandId(
            (*keydata.add(i as usize)).cmin,
            (*keydata.add(i as usize)).cmax,
        );

        /* Verify that we got the expected answer. */
        if cid != i as CommandId {
            elog!(ERROR, "unexpected command ID while restoring combo CIDs");
        }
        i += 1;
    }
}

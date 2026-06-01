//! subtrans.c - PostgreSQL subtransaction-log manager.

use crate::prelude::*;

use crate::access::transam::{
    FirstNormalTransactionId, FullTransactionId, InvalidTransactionId, MaxTransactionId,
    TransactionIdEquals, TransactionIdIsNormal, TransactionIdIsValid, TransactionIdRetreat,
    XidFromFullTransactionId,
};
use crate::access::transam::transam::{
    TransactionIdFollows, TransactionIdFollowsOrEquals, TransactionIdPrecedes,
};
use crate::utils::guc_hooks::GucSource;

// ---------------------------------------------------------------------------
// Locally stubbed dependencies (not yet ported).
// ---------------------------------------------------------------------------

// BLCKSZ from pg_config.h.  Standard PostgreSQL default block size.
const BLCKSZ: usize = 8192;

// LWLock and its acquisition modes live in storage/lwlock.h (not ported yet).
#[allow(non_camel_case_types)]
pub type LWLock = c_void;
const LW_EXCLUSIVE: c_int = 1;

unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    // TODO: port storage/lwlock.c
    unimplemented!()
}
unsafe fn LWLockRelease(_lock: *mut LWLock) {
    // TODO: port storage/lwlock.c
    unimplemented!()
}

// SLRU control structures from access/slru.h (not ported yet).
#[allow(non_snake_case)]
#[repr(C)]
pub struct SlruSharedData {
    pub page_buffer: *mut *mut c_char,
    pub page_dirty: *mut bool,
}
#[allow(non_camel_case_types)]
pub type SlruShared = *mut SlruSharedData;

#[allow(non_snake_case)]
#[repr(C)]
pub struct SlruCtlData {
    pub shared: SlruShared,
    pub PagePrecedes: Option<unsafe fn(i64, i64) -> bool>,
}
#[allow(non_camel_case_types)]
pub type SlruCtl = *mut SlruCtlData;

// Tranche ids from lwlock.h / lwlocknames.
const LWTRANCHE_SUBTRANS_BUFFER: c_int = 0;
const LWTRANCHE_SUBTRANS_SLRU: c_int = 0;

// SyncRequestHandler from storage/sync.h.
const SYNC_HANDLER_NONE: c_int = -1;

// Maximum allowed SLRU buffers, from slru.h.
const SLRU_MAX_ALLOWED_BUFFERS: c_int = 131072;

unsafe fn SimpleLruGetBankLock(_ctl: SlruCtl, _pageno: i64) -> *mut LWLock {
    // TODO: port access/slru.c
    unimplemented!()
}
unsafe fn SimpleLruReadPage(
    _ctl: SlruCtl,
    _pageno: i64,
    _write_ok: bool,
    _xid: TransactionId,
) -> c_int {
    // TODO: port access/slru.c
    unimplemented!()
}
unsafe fn SimpleLruReadPage_ReadOnly(_ctl: SlruCtl, _pageno: i64, _xid: TransactionId) -> c_int {
    // TODO: port access/slru.c
    unimplemented!()
}
unsafe fn SimpleLruZeroPage(_ctl: SlruCtl, _pageno: i64) -> c_int {
    // TODO: port access/slru.c
    unimplemented!()
}
unsafe fn SimpleLruWritePage(_ctl: SlruCtl, _slotno: c_int) {
    // TODO: port access/slru.c
    unimplemented!()
}
unsafe fn SimpleLruWriteAll(_ctl: SlruCtl, _allow_redirtied: bool) {
    // TODO: port access/slru.c
    unimplemented!()
}
unsafe fn SimpleLruTruncate(_ctl: SlruCtl, _cutoffPage: i64) {
    // TODO: port access/slru.c
    unimplemented!()
}
unsafe fn SimpleLruInit(
    _ctl: SlruCtl,
    _name: *const c_char,
    _nslots: c_int,
    _nlsns: c_int,
    _subdir: *const c_char,
    _buffer_tranche_id: c_int,
    _bank_tranche_id: c_int,
    _sync_handler: c_int,
    _long_segment_names: bool,
) {
    // TODO: port access/slru.c
    unimplemented!()
}
unsafe fn SimpleLruShmemSize(_nslots: c_int, _nlsns: c_int) -> Size {
    // TODO: port access/slru.c
    unimplemented!()
}
unsafe fn SimpleLruAutotuneBuffers(_divisor: c_int, _max: c_int) -> c_int {
    // TODO: port access/slru.c
    unimplemented!()
}
unsafe fn SlruPagePrecedesUnitTests(_ctl: SlruCtl, _per_page: c_int) {
    // TODO: port access/slru.c
    unimplemented!()
}

// GUC machinery from utils/guc.c (not ported yet).
#[allow(non_upper_case_globals)]
static mut subtransaction_buffers: c_int = 0;

const PGC_POSTMASTER: c_int = 1;
const PGC_S_DYNAMIC_DEFAULT: c_int = 1;
const PGC_S_OVERRIDE: c_int = 17;

unsafe fn SetConfigOption(
    _name: *const c_char,
    _value: *const c_char,
    _context: c_int,
    _source: c_int,
) {
    // TODO: port utils/misc/guc.c
    unimplemented!()
}
unsafe fn check_slru_buffers(_name: *const c_char, _newval: *mut c_int) -> bool {
    // TODO: port access/slru.c
    unimplemented!()
}

// TransamVariables->nextXid from access/transam/varsup.c (not ported yet).
#[allow(non_snake_case)]
#[repr(C)]
pub struct TransamVariablesData {
    pub nextXid: FullTransactionId,
}
#[allow(non_upper_case_globals)]
static mut TransamVariables: *mut TransamVariablesData = null_mut();

// TransactionXmin lives in utils/time/snapmgr.c (not ported yet).
#[allow(non_upper_case_globals)]
static mut TransactionXmin: TransactionId = InvalidTransactionId;

// DTrace probes compile to no-ops when not built with --enable-dtrace.
fn TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_START(_shutdown: bool) {}
fn TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_DONE(_shutdown: bool) {}

// ---------------------------------------------------------------------------
// SubTrans page sizing.
// ---------------------------------------------------------------------------

// We need four bytes per xact.
const SUBTRANS_XACTS_PER_PAGE: usize = BLCKSZ / core::mem::size_of::<TransactionId>();

/*
 * Although we return an int64 the actual value can't currently exceed
 * 0xFFFFFFFF/SUBTRANS_XACTS_PER_PAGE.
 */
#[inline]
fn TransactionIdToPage(xid: TransactionId) -> i64 {
    (xid as i64) / (SUBTRANS_XACTS_PER_PAGE as i64)
}

#[allow(non_snake_case)]
#[inline]
fn TransactionIdToEntry(xid: TransactionId) -> TransactionId {
    xid % (SUBTRANS_XACTS_PER_PAGE as TransactionId)
}

// Link to shared-memory data structures for SUBTRANS control.
#[allow(non_upper_case_globals)]
static mut SubTransCtlData: SlruCtlData = SlruCtlData {
    shared: null_mut(),
    PagePrecedes: None,
};

#[inline]
#[allow(non_snake_case)]
fn SubTransCtl() -> SlruCtl {
    &raw mut SubTransCtlData
}

/*
 * Record the parent of a subtransaction in the subtrans log.
 */
#[allow(non_snake_case)]
pub unsafe fn SubTransSetParent(xid: TransactionId, parent: TransactionId) {
    let pageno: i64 = TransactionIdToPage(xid);
    let entryno: c_int = TransactionIdToEntry(xid) as c_int;
    let slotno: c_int;
    let lock: *mut LWLock;
    let mut ptr: *mut TransactionId;

    Assert!(TransactionIdIsValid(parent));
    Assert!(TransactionIdFollows(xid, parent));

    lock = SimpleLruGetBankLock(SubTransCtl(), pageno);
    LWLockAcquire(lock, LW_EXCLUSIVE);

    slotno = SimpleLruReadPage(SubTransCtl(), pageno, true, xid);
    ptr = (*(*SubTransCtl()).shared).page_buffer.offset(slotno as isize) as *mut TransactionId;
    ptr = ptr.offset(entryno as isize);

    /*
     * It's possible we'll try to set the parent xid multiple times but we
     * shouldn't ever be changing the xid from one valid xid to another valid
     * xid, which would corrupt the data structure.
     */
    if *ptr != parent {
        Assert!(*ptr == InvalidTransactionId);
        *ptr = parent;
        *(*(*SubTransCtl()).shared).page_dirty.offset(slotno as isize) = true;
    }

    LWLockRelease(lock);
}

/*
 * Interrogate the parent of a transaction in the subtrans log.
 */
#[allow(non_snake_case)]
pub unsafe fn SubTransGetParent(xid: TransactionId) -> TransactionId {
    let pageno: i64 = TransactionIdToPage(xid);
    let entryno: c_int = TransactionIdToEntry(xid) as c_int;
    let slotno: c_int;
    let mut ptr: *mut TransactionId;
    let parent: TransactionId;

    /* Can't ask about stuff that might not be around anymore */
    Assert!(TransactionIdFollowsOrEquals(xid, TransactionXmin));

    /* Bootstrap and frozen XIDs have no parent */
    if !TransactionIdIsNormal(xid) {
        return InvalidTransactionId;
    }

    /* lock is acquired by SimpleLruReadPage_ReadOnly */

    slotno = SimpleLruReadPage_ReadOnly(SubTransCtl(), pageno, xid);
    ptr = (*(*SubTransCtl()).shared).page_buffer.offset(slotno as isize) as *mut TransactionId;
    ptr = ptr.offset(entryno as isize);

    parent = *ptr;

    LWLockRelease(SimpleLruGetBankLock(SubTransCtl(), pageno));

    parent
}

/*
 * SubTransGetTopmostTransaction
 *
 * Returns the topmost transaction of the given transaction id.
 *
 * Because we cannot look back further than TransactionXmin, it is possible
 * that this function will lie and return an intermediate subtransaction ID
 * instead of the true topmost parent ID.  This is OK, because in practice
 * we only care about detecting whether the topmost parent is still running
 * or is part of a current snapshot's list of still-running transactions.
 * Therefore, any XID before TransactionXmin is as good as any other.
 */
#[allow(non_snake_case)]
pub unsafe fn SubTransGetTopmostTransaction(xid: TransactionId) -> TransactionId {
    let mut parentXid: TransactionId = xid;
    let mut previousXid: TransactionId = xid;

    /* Can't ask about stuff that might not be around anymore */
    Assert!(TransactionIdFollowsOrEquals(xid, TransactionXmin));

    while TransactionIdIsValid(parentXid) {
        previousXid = parentXid;
        if TransactionIdPrecedes(parentXid, TransactionXmin) {
            break;
        }
        parentXid = SubTransGetParent(parentXid);

        /*
         * By convention the parent xid gets allocated first, so should always
         * precede the child xid. Anything else points to a corrupted data
         * structure that could lead to an infinite loop, so exit.
         */
        if !TransactionIdPrecedes(parentXid, previousXid) {
            elog!(
                ERROR,
                "pg_subtrans contains invalid entry: xid {} points to parent xid {}",
                previousXid,
                parentXid
            );
        }
    }

    Assert!(TransactionIdIsValid(previousXid));

    previousXid
}

/*
 * Number of shared SUBTRANS buffers.
 *
 * If asked to autotune, use 2MB for every 1GB of shared buffers, up to 8MB.
 * Otherwise just cap the configured amount to be between 16 and the maximum
 * allowed.
 */
#[allow(non_snake_case)]
unsafe fn SUBTRANSShmemBuffers() -> c_int {
    /* auto-tune based on shared buffers */
    if subtransaction_buffers == 0 {
        return SimpleLruAutotuneBuffers(512, 1024);
    }

    (16.max(subtransaction_buffers)).min(SLRU_MAX_ALLOWED_BUFFERS)
}

/*
 * Initialization of shared memory for SUBTRANS
 */
#[allow(non_snake_case)]
pub unsafe fn SUBTRANSShmemSize() -> Size {
    SimpleLruShmemSize(SUBTRANSShmemBuffers(), 0)
}

#[allow(non_snake_case)]
pub unsafe fn SUBTRANSShmemInit() {
    /* If auto-tuning is requested, now is the time to do it */
    if subtransaction_buffers == 0 {
        let mut buf: [c_char; 32] = [0; 32];

        // snprintf(buf, sizeof(buf), "%d", SUBTRANSShmemBuffers());
        let s = format!("{}\0", SUBTRANSShmemBuffers());
        let bytes = s.as_bytes();
        let n = bytes.len().min(buf.len());
        for i in 0..n {
            buf[i] = bytes[i] as c_char;
        }
        let buf_ptr = buf.as_ptr();

        SetConfigOption(
            c"subtransaction_buffers".as_ptr(),
            buf_ptr,
            PGC_POSTMASTER,
            PGC_S_DYNAMIC_DEFAULT,
        );

        /*
         * We prefer to report this value's source as PGC_S_DYNAMIC_DEFAULT.
         * However, if the DBA explicitly set subtransaction_buffers = 0 in
         * the config file, then PGC_S_DYNAMIC_DEFAULT will fail to override
         * that and we must force the matter with PGC_S_OVERRIDE.
         */
        if subtransaction_buffers == 0 {
            /* failed to apply it? */
            SetConfigOption(
                c"subtransaction_buffers".as_ptr(),
                buf_ptr,
                PGC_POSTMASTER,
                PGC_S_OVERRIDE,
            );
        }
    }
    Assert!(subtransaction_buffers != 0);

    (*SubTransCtl()).PagePrecedes = Some(SubTransPagePrecedes);
    SimpleLruInit(
        SubTransCtl(),
        c"subtransaction".as_ptr(),
        SUBTRANSShmemBuffers(),
        0,
        c"pg_subtrans".as_ptr(),
        LWTRANCHE_SUBTRANS_BUFFER,
        LWTRANCHE_SUBTRANS_SLRU,
        SYNC_HANDLER_NONE,
        false,
    );
    SlruPagePrecedesUnitTests(SubTransCtl(), SUBTRANS_XACTS_PER_PAGE as c_int);
}

/*
 * GUC check_hook for subtransaction_buffers
 */
#[allow(non_snake_case)]
pub unsafe fn check_subtrans_buffers(
    newval: *mut c_int,
    _extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    check_slru_buffers(c"subtransaction_buffers".as_ptr(), newval)
}

/*
 * This func must be called ONCE on system install.  It creates
 * the initial SUBTRANS segment.  (The SUBTRANS directory is assumed to
 * have been created by the initdb shell script, and SUBTRANSShmemInit
 * must have been called already.)
 *
 * Note: it's not really necessary to create the initial segment now,
 * since slru.c would create it on first write anyway.  But we may as well
 * do it to be sure the directory is set up correctly.
 */
#[allow(non_snake_case)]
pub unsafe fn BootStrapSUBTRANS() {
    let slotno: c_int;
    let lock: *mut LWLock = SimpleLruGetBankLock(SubTransCtl(), 0);

    LWLockAcquire(lock, LW_EXCLUSIVE);

    /* Create and zero the first page of the subtrans log */
    slotno = ZeroSUBTRANSPage(0);

    /* Make sure it's written out */
    SimpleLruWritePage(SubTransCtl(), slotno);
    Assert!(!*(*(*SubTransCtl()).shared).page_dirty.offset(slotno as isize));

    LWLockRelease(lock);
}

/*
 * Initialize (or reinitialize) a page of SUBTRANS to zeroes.
 *
 * The page is not actually written, just set up in shared memory.
 * The slot number of the new page is returned.
 *
 * Control lock must be held at entry, and will be held at exit.
 */
#[allow(non_snake_case)]
unsafe fn ZeroSUBTRANSPage(pageno: i64) -> c_int {
    SimpleLruZeroPage(SubTransCtl(), pageno)
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup,
 * after StartupXLOG has initialized TransamVariables->nextXid.
 *
 * oldestActiveXID is the oldest XID of any prepared transaction, or nextXid
 * if there are none.
 */
#[allow(non_snake_case)]
pub unsafe fn StartupSUBTRANS(oldestActiveXID: TransactionId) {
    let nextXid: FullTransactionId;
    let mut startPage: i64;
    let endPage: i64;
    let mut prevlock: *mut LWLock = null_mut();
    let mut lock: *mut LWLock;

    /*
     * Since we don't expect pg_subtrans to be valid across crashes, we
     * initialize the currently-active page(s) to zeroes during startup.
     * Whenever we advance into a new page, ExtendSUBTRANS will likewise zero
     * the new page without regard to whatever was previously on disk.
     */
    startPage = TransactionIdToPage(oldestActiveXID);
    nextXid = (*TransamVariables).nextXid;
    endPage = TransactionIdToPage(XidFromFullTransactionId(nextXid));

    loop {
        lock = SimpleLruGetBankLock(SubTransCtl(), startPage);
        if prevlock != lock {
            if !prevlock.is_null() {
                LWLockRelease(prevlock);
            }
            LWLockAcquire(lock, LW_EXCLUSIVE);
            prevlock = lock;
        }

        let _ = ZeroSUBTRANSPage(startPage);
        if startPage == endPage {
            break;
        }

        startPage += 1;
        /* must account for wraparound */
        if startPage > TransactionIdToPage(MaxTransactionId) {
            startPage = 0;
        }
    }

    LWLockRelease(lock);
}

/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 */
#[allow(non_snake_case)]
pub unsafe fn CheckPointSUBTRANS() {
    /*
     * Write dirty SUBTRANS pages to disk
     *
     * This is not actually necessary from a correctness point of view. We do
     * it merely to improve the odds that writing of dirty pages is done by
     * the checkpoint process and not by backends.
     */
    TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_START(true);
    SimpleLruWriteAll(SubTransCtl(), true);
    TRACE_POSTGRESQL_SUBTRANS_CHECKPOINT_DONE(true);
}

/*
 * Make sure that SUBTRANS has room for a newly-allocated XID.
 *
 * NB: this is called while holding XidGenLock.  We want it to be very fast
 * most of the time; even when it's not so fast, no actual I/O need happen
 * unless we're forced to write out a dirty subtrans page to make room
 * in shared memory.
 */
#[allow(non_snake_case)]
pub unsafe fn ExtendSUBTRANS(newestXact: TransactionId) {
    let pageno: i64;
    let lock: *mut LWLock;

    /*
     * No work except at first XID of a page.  But beware: just after
     * wraparound, the first XID of page zero is FirstNormalTransactionId.
     */
    if TransactionIdToEntry(newestXact) != 0
        && !TransactionIdEquals(newestXact, FirstNormalTransactionId)
    {
        return;
    }

    pageno = TransactionIdToPage(newestXact);

    lock = SimpleLruGetBankLock(SubTransCtl(), pageno);
    LWLockAcquire(lock, LW_EXCLUSIVE);

    /* Zero the page */
    ZeroSUBTRANSPage(pageno);

    LWLockRelease(lock);
}

/*
 * Remove all SUBTRANS segments before the one holding the passed transaction ID
 *
 * oldestXact is the oldest TransactionXmin of any running transaction.  This
 * is called only during checkpoint.
 */
#[allow(non_snake_case)]
pub unsafe fn TruncateSUBTRANS(oldestXact: TransactionId) {
    let cutoffPage: i64;
    let mut oldestXact = oldestXact;

    /*
     * The cutoff point is the start of the segment containing oldestXact. We
     * pass the *page* containing oldestXact to SimpleLruTruncate.  We step
     * back one transaction to avoid passing a cutoff page that hasn't been
     * created yet in the rare case that oldestXact would be the first item on
     * a page and oldestXact == next XID.  In that case, if we didn't subtract
     * one, we'd trigger SimpleLruTruncate's wraparound detection.
     */
    TransactionIdRetreat(&mut oldestXact);
    cutoffPage = TransactionIdToPage(oldestXact);

    SimpleLruTruncate(SubTransCtl(), cutoffPage);
}

/*
 * Decide whether a SUBTRANS page number is "older" for truncation purposes.
 * Analogous to CLOGPagePrecedes().
 */
#[allow(non_snake_case)]
fn SubTransPagePrecedes(page1: i64, page2: i64) -> bool {
    let mut xid1: TransactionId;
    let mut xid2: TransactionId;

    xid1 = (page1 as TransactionId).wrapping_mul(SUBTRANS_XACTS_PER_PAGE as TransactionId);
    xid1 = xid1.wrapping_add(FirstNormalTransactionId + 1);
    xid2 = (page2 as TransactionId).wrapping_mul(SUBTRANS_XACTS_PER_PAGE as TransactionId);
    xid2 = xid2.wrapping_add(FirstNormalTransactionId + 1);

    TransactionIdPrecedes(xid1, xid2)
        && TransactionIdPrecedes(
            xid1,
            xid2.wrapping_add(SUBTRANS_XACTS_PER_PAGE as TransactionId - 1),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_id_to_page_and_entry() {
        // SUBTRANS_XACTS_PER_PAGE = 8192/4 = 2048.
        assert_eq!(SUBTRANS_XACTS_PER_PAGE, 2048);
        assert_eq!(TransactionIdToPage(0), 0);
        assert_eq!(TransactionIdToPage(2047), 0);
        assert_eq!(TransactionIdToPage(2048), 1);
        assert_eq!(TransactionIdToEntry(2048), 0);
        assert_eq!(TransactionIdToEntry(2049), 1);
        assert_eq!(TransactionIdToEntry(4095), 2047);
    }

    #[test]
    fn test_subtrans_page_precedes() {
        // A lower page precedes a higher one (no wraparound).
        assert!(SubTransPagePrecedes(1, 2));
        assert!(!SubTransPagePrecedes(2, 1));
        assert!(!SubTransPagePrecedes(5, 5));
    }
}
